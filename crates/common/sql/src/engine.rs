//! The SQL sink engine: everything a SQL sink decides, written once.
//!
//! [`SqlSinkEngine`] owns the planning every SQL sink used to carry its own
//! copy of: mapping records onto the live columns, page-ordered upsert/delete
//! groups, the multi-row statements chunked under the parameter limit, the
//! create-table and evolution DDL, the scoped-cleanup sequence, the
//! commit-token write, the rollback-journal ordering and the row report. A
//! backend supplies a [`SqlDialect`] and a [`SqlExecutor`] — its driver's
//! connection, binding each planned parameter and running each statement —
//! and keeps only what is genuinely its own: transactions, the bulk-load
//! paths, overwrite swaps and catalog queries.

use crate::dialect::{SqlDialect, TableTarget};
use crate::mapping::{
    ColumnMatching, MatchedRow, TableColumn, group_by_present, match_record, union_columns,
    unmatched_fields,
};
use crate::statement::{Param, Statement, StatementKind};
use faucet_core::write_mode::{KeyTuple, WritePlan, WriteSpec};
use faucet_core::{FaucetError, RowOutcome, SeenKeys, SqlBaseType, WriteMode, WriteReport};
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};

/// A backend's connection, as the engine drives it: catalog reads and
/// statement execution. Implemented over a driver connection or open
/// transaction, so every statement of one engine call shares it.
#[faucet_core::async_trait]
pub trait SqlExecutor: Send {
    /// The target's columns in declared order (with their types when the
    /// backend binds through them). An error when the table is missing.
    async fn columns(&mut self, target: &TableTarget) -> Result<Vec<TableColumn>, FaucetError>;

    /// The type of each `key` column, for backends whose placeholders cast.
    /// The default binds untyped.
    async fn key_types(
        &mut self,
        target: &TableTarget,
        key: &[String],
    ) -> Result<Vec<Option<String>>, FaucetError> {
        let _ = target;
        Ok(vec![None; key.len()])
    }

    /// The columns a scoped cleanup validates against and types its key
    /// table from; an empty list when the table is missing. Defaults to
    /// [`columns`](Self::columns).
    async fn cleanup_columns(
        &mut self,
        target: &TableTarget,
    ) -> Result<Vec<TableColumn>, FaucetError> {
        self.columns(target).await
    }

    /// Bind `stmt`'s parameters and run it, returning the rows it affected.
    async fn execute(&mut self, stmt: &Statement<'_>) -> Result<u64, FaucetError>;

    /// Run `stmt` (a [`StatementKind::CleanupJournal`] read) and return its
    /// one text column per row. Only called when the sink's write spec
    /// journals.
    async fn fetch_text(&mut self, stmt: &Statement<'_>) -> Result<Vec<String>, FaucetError> {
        let _ = stmt;
        Err(FaucetError::Sink(
            "rollback journaling of a scoped cleanup is not supported by this sink".into(),
        ))
    }

    /// Journal the before-image of every key `plan` touches, on this
    /// connection, before the plan is applied (#706). Only called when the
    /// sink's write spec journals.
    async fn journal_plan(&mut self, plan: &WritePlan, run_id: &str) -> Result<(), FaucetError> {
        let _ = (plan, run_id);
        Err(FaucetError::Sink(
            "rollback journaling is not supported by this sink".into(),
        ))
    }
}

/// Engine behaviour that is a backend's choice rather than its dialect's.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct EngineOptions {
    /// Log, once per engine, record fields that match no column.
    pub warn_unmatched_fields: bool,
}

impl EngineOptions {
    /// Defaults: no unmatched-field warning.
    pub fn new() -> Self {
        Self::default()
    }

    /// Log record fields that match no column, once per engine.
    pub fn warn_unmatched_fields(mut self, on: bool) -> Self {
        self.warn_unmatched_fields = on;
        self
    }
}

/// One schema-evolution step, in the order to apply them.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum EvolutionStep {
    /// Add `column` of base type `base`.
    Add {
        /// The column to add.
        column: String,
        /// Its base type.
        base: SqlBaseType,
    },
    /// Widen `column` to `type_name`.
    Widen {
        /// The column to widen.
        column: String,
        /// Its base type before the change, when known.
        from: Option<SqlBaseType>,
        /// The base type its values now need.
        to: SqlBaseType,
        /// The dialect type it widens to.
        type_name: &'static str,
    },
    /// Drop `NOT NULL` from `column`.
    Relax {
        /// The column to relax.
        column: String,
    },
}

/// Per-row outcomes of a page split before it is written: the rows to write,
/// and each one's position in the page.
#[derive(Debug)]
#[non_exhaustive]
pub struct SplitPage {
    /// One outcome per offered row; failed rows are already `Err`.
    pub outcomes: Vec<RowOutcome>,
    /// The rows to write.
    pub writable: Vec<Value>,
    /// `positions[i]` is `writable[i]`'s index in the offered page.
    pub positions: Vec<usize>,
}

/// The SQL sink engine. See the module docs.
pub struct SqlSinkEngine<D> {
    dialect: D,
    write: WriteSpec,
    options: EngineOptions,
    unmatched_warned: AtomicBool,
}

impl<D: std::fmt::Debug> std::fmt::Debug for SqlSinkEngine<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlSinkEngine")
            .field("dialect", &self.dialect)
            .field("write_mode", &self.write.write_mode)
            .field("key", &self.write.key)
            .finish()
    }
}

fn not_object() -> FaucetError {
    FaucetError::Sink("AutoMap requires JSON object records".into())
}

impl<D: SqlDialect> SqlSinkEngine<D> {
    /// An engine for `dialect` writing under `write`.
    pub fn new(dialect: D, write: WriteSpec) -> Self {
        Self {
            dialect,
            write,
            options: EngineOptions::default(),
            unmatched_warned: AtomicBool::new(false),
        }
    }

    /// The same engine with `options`.
    pub fn with_options(mut self, options: EngineOptions) -> Self {
        self.options = options;
        self
    }

    /// The dialect.
    pub fn dialect(&self) -> &D {
        &self.dialect
    }

    /// The write spec the engine plans under.
    pub fn write_spec(&self) -> &WriteSpec {
        &self.write
    }

    /// The key columns.
    pub fn key(&self) -> &[String] {
        &self.write.key
    }

    /// Whether the write mode is keyed (`upsert` / `delete`).
    pub fn is_keyed(&self) -> bool {
        matches!(self.write.write_mode, WriteMode::Upsert | WriteMode::Delete)
    }

    /// The key a created table carries a primary key over: the write key
    /// when the writes dedup on it, else none.
    pub fn table_key(&self) -> &[String] {
        if self.write.dedups_by_key() {
            &self.write.key
        } else {
            &[]
        }
    }

    // ── planning ────────────────────────────────────────────────────────

    /// The page's keyed write groups in page order, failing on the first row
    /// whose key could not be read.
    pub fn plan_keyed(&self, records: &[Value]) -> Result<Vec<WritePlan>, FaucetError> {
        let plan = faucet_core::plan_writes(records, &self.write);
        if let Some((idx, msg)) = plan.failed.first() {
            return Err(FaucetError::Sink(format!(
                "{} {}: row {idx}: {msg}",
                self.dialect.name(),
                self.write.write_mode.as_str()
            )));
        }
        Ok(faucet_core::plan_writes_ordered(records, &self.write))
    }

    /// The page's keyed write groups in page order, plus a per-row failure
    /// for every row whose key could not be read (for per-row outcomes).
    pub fn plan_keyed_partial(
        &self,
        records: &[Value],
    ) -> (Vec<WritePlan>, Vec<(usize, FaucetError)>) {
        let plan = faucet_core::plan_writes(records, &self.write);
        let failed = plan
            .failed
            .iter()
            .map(|(idx, msg)| {
                (
                    *idx,
                    FaucetError::Sink(format!(
                        "{} {}: {msg}",
                        self.dialect.name(),
                        self.write.write_mode.as_str()
                    )),
                )
            })
            .collect();
        (
            faucet_core::plan_writes_ordered(records, &self.write),
            failed,
        )
    }

    /// The `CREATE TABLE` for a target inferred from `records`, or `None`
    /// when a column-mapped page has nothing to infer from. `json_column`
    /// creates the fixed whole-record shape instead.
    pub fn create_table_sql(
        &self,
        target: &TableTarget,
        records: &[Value],
        json_column: Option<&str>,
    ) -> Result<Option<String>, FaucetError> {
        if let Some(column) = json_column {
            return self.dialect.create_json_table_sql(target, column).map(Some);
        }
        let key = self.table_key();
        let planned = if key.is_empty() {
            faucet_core::plan_columns(records)
        } else {
            faucet_core::plan_keyed_columns(records, key)
        };
        match planned {
            Some(columns) => self
                .dialect
                .create_table_sql(target, &columns, key)
                .map(Some),
            None => Ok(None),
        }
    }

    /// Match every record of a page, warning once about unmatched fields
    /// when configured to.
    pub fn map_page<'a>(
        &self,
        target: &TableTarget,
        columns: &[TableColumn],
        records: &'a [Value],
    ) -> Result<Vec<MatchedRow<'a>>, FaucetError> {
        let matching = self.dialect.column_matching();
        let mut rows = Vec::with_capacity(records.len());
        for (index, record) in records.iter().enumerate() {
            let obj = record.as_object().ok_or_else(not_object)?;
            let cells = match_record(columns, obj, matching);
            if cells.is_empty() {
                return Err(self.no_match_error(index, obj, columns, &target.name));
            }
            self.warn_unmatched(obj, columns, matching, &target.name);
            rows.push(MatchedRow { index, cells });
        }
        Ok(rows)
    }

    fn no_match_error(
        &self,
        index: usize,
        obj: &serde_json::Map<String, Value>,
        columns: &[TableColumn],
        table: &str,
    ) -> FaucetError {
        FaucetError::Sink(no_match_message(&self.dialect, index, obj, columns, table))
    }

    fn warn_unmatched(
        &self,
        obj: &serde_json::Map<String, Value>,
        columns: &[TableColumn],
        matching: ColumnMatching,
        table: &str,
    ) {
        if !self.options.warn_unmatched_fields || self.unmatched_warned.load(Ordering::Relaxed) {
            return;
        }
        let unmatched = unmatched_fields(obj, columns, matching);
        if !unmatched.is_empty() && !self.unmatched_warned.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                table = %table,
                fields = ?unmatched,
                "{} sink: record fields match no table column and are not written \
                 (configure a `schema:` drift policy to add or quarantine them)",
                self.dialect.name()
            );
        }
    }

    /// The statements that write `records` over `columns`: one union-column
    /// `INSERT` per chunk for an append, or one upsert per set of carried
    /// columns (so an absent column keeps its stored value) for a keyed
    /// write. Returns the rows written and the statements.
    pub fn insert_statements<'a>(
        &self,
        target: &TableTarget,
        columns: &'a [TableColumn],
        records: &'a [Value],
        upsert: bool,
    ) -> Result<(usize, Vec<Statement<'a>>), FaucetError> {
        if records.is_empty() {
            return Ok((0, Vec::new()));
        }
        let rows = self.map_page(target, columns, records)?;
        let mut out = Vec::new();
        if upsert {
            for (present, members) in group_by_present(&rows, columns.len()) {
                let members: Vec<&MatchedRow<'a>> = members.into_iter().collect();
                self.push_row_statements(target, columns, &present, &members, true, &mut out)?;
            }
        } else {
            let present = union_columns(&rows, columns.len());
            let members: Vec<&MatchedRow<'a>> = rows.iter().collect();
            self.push_row_statements(target, columns, &present, &members, false, &mut out)?;
        }
        Ok((rows.len(), out))
    }

    fn push_row_statements<'a>(
        &self,
        target: &TableTarget,
        columns: &'a [TableColumn],
        present: &[usize],
        rows: &[&MatchedRow<'a>],
        upsert: bool,
        out: &mut Vec<Statement<'a>>,
    ) -> Result<(), FaucetError> {
        let names: Vec<String> = present.iter().map(|i| columns[*i].name.clone()).collect();
        let types: Vec<Option<&str>> = present.iter().map(|i| columns[*i].sql_type()).collect();
        let per = self.dialect.rows_per_statement(names.len());
        for sub in rows.chunks(per) {
            let sql = if upsert {
                self.dialect.upsert_sql(
                    &target.reference,
                    &self.write.key,
                    &names,
                    &types,
                    sub.len(),
                )?
            } else {
                self.dialect
                    .insert_sql(&target.reference, &names, &types, sub.len())?
            };
            let mut params = Vec::with_capacity(sub.len() * present.len());
            for row in sub {
                for i in present {
                    let col = &columns[*i];
                    params.push(match row.value(*i) {
                        Some(v) => Param::borrowed(&col.name, col.sql_type(), v),
                        None => Param::null(&col.name, col.sql_type()),
                    });
                }
            }
            let kind = if upsert {
                StatementKind::Upsert
            } else {
                StatementKind::Insert
            };
            out.push(Statement::new(kind, sql, params));
        }
        Ok(())
    }

    /// The statements that write each record whole, as JSON text, into
    /// `column`.
    pub fn json_insert_statements<'a>(
        &self,
        target: &TableTarget,
        column: &'a str,
        records: &[Value],
    ) -> Result<Vec<Statement<'a>>, FaucetError> {
        let columns = [column.to_string()];
        records
            .chunks(self.dialect.rows_per_statement(1))
            .map(|chunk| {
                let sql = self
                    .dialect
                    .insert_sql(&target.reference, &columns, &[], chunk.len())?;
                let params = chunk
                    .iter()
                    .map(|r| {
                        serde_json::to_string(r)
                            .map(|s| Param::owned(column, None, Value::String(s)))
                            .map_err(|e| {
                                FaucetError::Sink(format!("failed to serialize record: {e}"))
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Statement::new(StatementKind::InsertJson, sql, params))
            })
            .collect()
    }

    /// The keyed deletes of `deletes`, chunked under the parameter limit.
    pub fn delete_statements<'a>(
        &self,
        target: &TableTarget,
        key_types: &'a [Option<String>],
        deletes: &'a [KeyTuple],
    ) -> Result<Vec<Statement<'a>>, FaucetError> {
        let key = &self.write.key;
        let types: Vec<Option<&str>> = key_types.iter().map(|t| t.as_deref()).collect();
        deletes
            .chunks(self.dialect.rows_per_statement(key.len()))
            .map(|chunk| {
                let sql =
                    self.dialect
                        .delete_keys_sql(&target.reference, key, &types, chunk.len())?;
                Ok(Statement::new(
                    StatementKind::Delete,
                    sql,
                    key_params(chunk, key_types),
                ))
            })
            .collect()
    }

    /// Split a page into the rows to write and per-row failures: a record
    /// matching no column (when `require_match`) or one `validate` refuses
    /// fails on its own row instead of failing the page. `validate` sees
    /// `(row index, column, value)` for each carried cell and returns the
    /// failure text.
    pub fn split_writable(
        &self,
        target: &TableTarget,
        records: &[Value],
        columns: &[TableColumn],
        require_match: bool,
        validate: impl Fn(usize, &TableColumn, &Value) -> Option<String>,
    ) -> SplitPage {
        split_page(
            &self.dialect,
            &target.name,
            records,
            columns,
            require_match,
            validate,
        )
    }

    /// The DDL steps of a schema evolution. `existing` (the live column
    /// names) skips additions already present, for dialects without `ADD
    /// COLUMN IF NOT EXISTS`.
    pub fn evolution_steps(
        &self,
        evolution: &faucet_core::SchemaEvolution,
        existing: Option<&HashSet<String>>,
    ) -> Vec<EvolutionStep> {
        let base = |v: &Value| faucet_core::json_schema_base_type(v).unwrap_or(SqlBaseType::Text);
        let mut steps = Vec::new();
        for c in &evolution.additions {
            if existing.is_some_and(|e| e.contains(&c.name)) {
                continue;
            }
            steps.push(EvolutionStep::Add {
                column: c.name.clone(),
                base: base(&c.to),
            });
        }
        for c in &evolution.widenings {
            let to = base(&c.to);
            let from = c.from.as_ref().and_then(faucet_core::json_schema_base_type);
            steps.push(EvolutionStep::Widen {
                column: c.name.clone(),
                from,
                to,
                type_name: self.dialect.widened_type_name(from, to),
            });
        }
        for col in &evolution.relax_nullability {
            steps.push(EvolutionStep::Relax {
                column: col.clone(),
            });
        }
        steps
    }

    /// What a page write did, for row accounting: a keyed write accepts one
    /// row per planned upsert or delete and coalesces the rest (same key,
    /// last write wins); an append accepts what it wrote.
    pub fn report(&self, offered: usize, written: usize, plans: &[WritePlan]) -> WriteReport {
        if self.is_keyed() {
            let planned: usize = plans
                .iter()
                .map(|p| p.upserts.len() + p.deletes.len())
                .sum();
            WriteReport::keyed(offered, planned)
        } else {
            WriteReport::accepted(written)
        }
    }

    // ── execution ───────────────────────────────────────────────────────

    /// Map `records` onto the target's live columns and write them
    /// (`upsert` adds the keyed conflict handling). Returns the rows written.
    pub async fn insert<E: SqlExecutor + ?Sized>(
        &self,
        exec: &mut E,
        target: &TableTarget,
        records: &[Value],
        upsert: bool,
    ) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        let columns = exec.columns(target).await?;
        let (written, statements) = self.insert_statements(target, &columns, records, upsert)?;
        for stmt in &statements {
            exec.execute(stmt).await?;
        }
        Ok(written)
    }

    /// Write each record whole into `column`.
    pub async fn insert_json<E: SqlExecutor + ?Sized>(
        &self,
        exec: &mut E,
        target: &TableTarget,
        column: &str,
        records: &[Value],
    ) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        for stmt in &self.json_insert_statements(target, column, records)? {
            exec.execute(stmt).await?;
        }
        Ok(records.len())
    }

    /// Delete the rows keyed by `deletes`. Returns the rows affected.
    pub async fn delete<E: SqlExecutor + ?Sized>(
        &self,
        exec: &mut E,
        target: &TableTarget,
        deletes: &[KeyTuple],
    ) -> Result<usize, FaucetError> {
        if deletes.is_empty() {
            return Ok(0);
        }
        let key_types = exec.key_types(target, &self.write.key).await?;
        let mut total = 0usize;
        for stmt in &self.delete_statements(target, &key_types, deletes)? {
            total += exec.execute(stmt).await? as usize;
        }
        Ok(total)
    }

    /// Apply keyed write groups in page order on one connection: each
    /// group's journal (when journaling), then its upserts, then its deletes.
    pub async fn apply_plans<E: SqlExecutor + ?Sized>(
        &self,
        exec: &mut E,
        target: &TableTarget,
        plans: &[WritePlan],
    ) -> Result<usize, FaucetError> {
        // The groups share one table: read its columns and key types once,
        // not once per group (a CDC page alternating upserts and deletes is
        // one group per row).
        let mut columns: Option<Vec<TableColumn>> = None;
        let mut key_types: Option<Vec<Option<String>>> = None;
        let mut affected = 0usize;
        for plan in plans {
            if self.write.journals()
                && let Some(run_id) = self.write.rollback_run_id()
            {
                exec.journal_plan(plan, run_id).await?;
            }
            if !plan.upserts.is_empty() {
                if columns.is_none() {
                    columns = Some(exec.columns(target).await?);
                }
                let columns = columns.as_deref().unwrap_or_default();
                let (written, statements) =
                    self.insert_statements(target, columns, &plan.upserts, true)?;
                for stmt in &statements {
                    exec.execute(stmt).await?;
                }
                affected += written;
            }
            if !plan.deletes.is_empty() {
                if key_types.is_none() {
                    key_types = Some(exec.key_types(target, &self.write.key).await?);
                }
                let key_types = key_types.as_deref().unwrap_or_default();
                for stmt in &self.delete_statements(target, key_types, &plan.deletes)? {
                    affected += exec.execute(stmt).await? as usize;
                }
            }
        }
        Ok(affected)
    }

    /// Refuse a scoped cleanup without a key.
    pub fn check_cleanup_key(&self) -> Result<(), FaucetError> {
        if self.write.key.is_empty() {
            return Err(FaucetError::Sink(
                "cleanup requires a non-empty `key`".to_string(),
            ));
        }
        Ok(())
    }

    /// Delete the rows in `scope` whose key this run did not write (#478),
    /// on one connection the caller holds a transaction on: validate the
    /// scope and key against the live columns, load the written keys into
    /// the cleanup key table, run the `NOT EXISTS` delete. Returns the rows
    /// deleted. An empty `seen` set deletes the whole scope (the source
    /// reported it empty).
    pub async fn cleanup<E: SqlExecutor + ?Sized>(
        &self,
        exec: &mut E,
        target: &TableTarget,
        scope: &BTreeMap<String, Value>,
        seen: &SeenKeys,
    ) -> Result<u64, FaucetError> {
        self.check_cleanup_key()?;
        let key = &self.write.key;
        let live = exec.cleanup_columns(target).await?;
        let scope_cols: Vec<String> = scope.keys().cloned().collect();
        check_cleanup_columns(
            |c| live.iter().any(|l| l.name == c),
            &scope_cols,
            key,
            &self.dialect.cleanup_table_display(target),
        )?;
        let type_of = |c: &str| {
            live.iter()
                .find(|l| l.name == c)
                .and_then(|l| l.sql_type().map(str::to_string))
        };
        let key_types: Vec<Option<String>> = key.iter().map(|k| type_of(k)).collect();
        let key_types_ref: Vec<Option<&str>> = key_types.iter().map(|t| t.as_deref()).collect();
        let scope_types: Vec<Option<String>> = scope_cols.iter().map(|c| type_of(c)).collect();
        let scope_types_ref: Vec<Option<&str>> = scope_types.iter().map(|t| t.as_deref()).collect();

        let drop = self.dialect.cleanup_drop_sql();
        if let Some(sql) = &drop {
            exec.execute(&Statement::bare(StatementKind::CleanupDrop, sql.clone()))
                .await?;
        }
        let create = self
            .dialect
            .cleanup_create_sql(target, key, &key_types_ref)?;
        exec.execute(&Statement::bare(StatementKind::CleanupCreate, create))
            .await?;

        for chunk in seen
            .keys()
            .chunks(self.dialect.rows_per_statement(key.len()))
        {
            let sql = self
                .dialect
                .cleanup_insert_sql(key, &key_types_ref, chunk.len())?;
            exec.execute(&Statement::new(
                StatementKind::CleanupLoad,
                sql,
                key_params(chunk, &key_types),
            ))
            .await?;
        }

        let sql = self
            .dialect
            .cleanup_delete_sql(target, &scope_cols, &scope_types_ref, key)?;
        let params: Vec<Param<'_>> = scope
            .iter()
            .zip(&scope_types)
            .map(|((c, v), t)| Param::borrowed(c, t.as_deref(), v))
            .collect();
        if self.write.journals()
            && let Some(run_id) = self.write.rollback_run_id()
        {
            self.journal_cleanup(exec, target, &scope_cols, &scope_types_ref, &params, run_id)
                .await?;
        }
        let deleted = exec
            .execute(&Statement::new(StatementKind::CleanupDelete, sql, params))
            .await?;

        if let Some(sql) = drop {
            exec.execute(&Statement::bare(StatementKind::CleanupDrop, sql))
                .await?;
        }
        Ok(deleted)
    }

    /// Journal the before-image of every row the cleanup delete is about to
    /// remove, so a rollback of the run restores them (#706): the delete runs
    /// on the same transaction right after.
    async fn journal_cleanup<E: SqlExecutor + ?Sized>(
        &self,
        exec: &mut E,
        target: &TableTarget,
        scope_cols: &[String],
        scope_types: &[Option<&str>],
        params: &[Param<'_>],
        run_id: &str,
    ) -> Result<(), FaucetError> {
        let key = &self.write.key;
        let sql = self
            .dialect
            .cleanup_absent_keys_sql(target, scope_cols, scope_types, key)?
            .ok_or_else(|| {
                FaucetError::Sink(format!(
                    "{}: a scoped cleanup cannot be journaled for rollback",
                    self.dialect.name()
                ))
            })?;
        let rows = exec
            .fetch_text(&Statement::new(
                StatementKind::CleanupJournal,
                sql,
                params.to_vec(),
            ))
            .await?;
        let deletes = rows
            .iter()
            .map(|text| {
                let row = faucet_core::fidelity::parse_json(text.as_bytes()).map_err(|e| {
                    FaucetError::Sink(format!("cleanup: reading a key to journal: {e}"))
                })?;
                Ok(KeyTuple(
                    key.iter()
                        .map(|k| (k.clone(), row.get(k).cloned().unwrap_or(Value::Null)))
                        .collect(),
                ))
            })
            .collect::<Result<Vec<_>, FaucetError>>()?;
        if deletes.is_empty() {
            return Ok(());
        }
        let plan = WritePlan {
            deletes,
            ..WritePlan::default()
        };
        exec.journal_plan(&plan, run_id).await
    }

    /// Upsert `scope`'s commit token on the caller's transaction, so it
    /// commits with the page it watermarks.
    pub async fn write_token<E: SqlExecutor + ?Sized>(
        &self,
        exec: &mut E,
        scope: &str,
        token: &str,
    ) -> Result<(), FaucetError> {
        let params = vec![
            Param::owned(
                faucet_core::idempotency::COMMIT_TOKEN_SCOPE_COL,
                None,
                Value::String(scope.to_string()),
            ),
            Param::owned(
                faucet_core::idempotency::COMMIT_TOKEN_TOKEN_COL,
                None,
                Value::String(token.to_string()),
            ),
        ];
        exec.execute(&Statement::new(
            StatementKind::TokenUpsert,
            self.dialect.commit_token_upsert_sql(),
            params,
        ))
        .await?;
        Ok(())
    }
}

/// The dialect's error text for the record at `index` matching no column of
/// `table`.
pub fn no_match_message<D: SqlDialect + ?Sized>(
    dialect: &D,
    index: usize,
    obj: &serde_json::Map<String, Value>,
    columns: &[TableColumn],
    table: &str,
) -> String {
    let fields: Vec<&String> = obj.keys().collect();
    let names: Vec<&String> = columns.iter().map(|c| &c.name).collect();
    dialect.no_matching_column_message(index, &fields, &names, table)
}

/// [`SqlSinkEngine::split_writable`] for callers holding only a dialect:
/// split a page into the rows to write and per-row failures (a record
/// matching no column of `table` when `require_match`, or one `validate`
/// refuses).
pub fn split_page<D: SqlDialect + ?Sized>(
    dialect: &D,
    table: &str,
    records: &[Value],
    columns: &[TableColumn],
    require_match: bool,
    validate: impl Fn(usize, &TableColumn, &Value) -> Option<String>,
) -> SplitPage {
    let matching = dialect.column_matching();
    let mut outcomes = Vec::with_capacity(records.len());
    let mut writable = Vec::with_capacity(records.len());
    let mut positions = Vec::with_capacity(records.len());
    for (idx, record) in records.iter().enumerate() {
        let failure = record.as_object().and_then(|obj| {
            let cells = match_record(columns, obj, matching);
            if require_match && cells.is_empty() {
                return Some(no_match_message(dialect, idx, obj, columns, table));
            }
            cells
                .iter()
                .find_map(|(i, v)| validate(idx, &columns[*i], v))
        });
        match failure {
            Some(msg) => outcomes.push(Err(FaucetError::Sink(msg))),
            None => {
                outcomes.push(Ok(()));
                writable.push(record.clone());
                positions.push(idx);
            }
        }
    }
    SplitPage {
        outcomes,
        writable,
        positions,
    }
}

/// Refuse a scoped cleanup whose scope or key names a column the destination
/// lacks: both are in destination terms, so a missing one is a configuration
/// error worth naming before any `DELETE` runs. `table` is how the error names
/// the destination ([`SqlDialect::cleanup_table_display`]).
pub fn check_cleanup_columns(
    exists: impl Fn(&str) -> bool,
    scope: &[String],
    key: &[String],
    table: &str,
) -> Result<(), FaucetError> {
    for col in scope.iter().chain(key.iter()) {
        if !exists(col) {
            return Err(FaucetError::Sink(format!(
                "cleanup: column '{col}' does not exist on {table} — the \
                 completeness claim and `key` are in destination column terms"
            )));
        }
    }
    Ok(())
}

/// Each key tuple's values in key order, row-major, typed per key column.
fn key_params<'a>(keys: &'a [KeyTuple], types: &'a [Option<String>]) -> Vec<Param<'a>> {
    keys.iter()
        .flat_map(|kt| {
            kt.0.iter().enumerate().map(|(i, (col, v))| {
                Param::borrowed(col, types.get(i).and_then(|t| t.as_deref()), v)
            })
        })
        .collect()
}
