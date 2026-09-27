//! The Oracle [`Sink`] — the only module that talks to the driver. Every call
//! runs on a blocking thread; a page is written in one transaction.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use faucet_common_oracle::oracle::sql_type::{OracleType, ToSql};
use faucet_common_oracle::oracle::{self, Connection};
use faucet_common_oracle::{
    OraclePool, Side, blocking, call_timeout, checkout, connect_pool, ora_err, quote_table_oracle,
};
use faucet_core::check::{CheckContext, CheckReport, Probe};
use faucet_core::idempotency::OVERWRITE_STAGING_SUFFIX;
use faucet_core::{FaucetError, RowOutcome, Sink, WriteMode, WritePlan};
use serde_json::Value;

use crate::config::{IdentifierCase, OracleColumnMapping, OracleSinkConfig};
use crate::plan::{
    BindKind, COLUMNS_SQL, ColumnInfo, ORA_NAME_IN_USE, TABLE_EXISTS_SQL, add_column_sql,
    case_records, clone_table_sql, column_from_row, create_json_table_sql, create_table_sql,
    delete_sql, dictionary_binds, drop_table_sql, encode_row, ignoring, insert_sql, merge_sql,
    relax_null_sql, rename_sql, resolve_insert_columns, schema_from_columns, swap_sql,
    token_merge_sql, token_select_sql, token_table, token_table_ddl, widen_column_sql,
};

/// Oracle Database sink.
pub struct OracleSink {
    inner: Arc<Inner>,
}

struct Inner {
    config: OracleSinkConfig,
    pool: OraclePool,
    timeout: Option<Duration>,
    target: String,
    target_quoted: String,
    staging: String,
    staging_quoted: String,
    token_table: String,
    columns: Mutex<Option<Vec<ColumnInfo>>>,
    ready: AtomicBool,
}

fn sink_err(context: &str, e: &oracle::Error) -> FaucetError {
    ora_err(Side::Sink, context, e)
}

fn exec(conn: &Connection, sql: &str, context: &str) -> Result<(), FaucetError> {
    conn.execute(sql, &[])
        .map(|_| ())
        .map_err(|e| sink_err(context, &e))
}

/// Execute one array-DML statement over `rows`. With `collect_errors`, rows the
/// server rejects are returned as `(row index, message)` and the rest apply.
fn run_batch(
    conn: &Connection,
    sql: &str,
    kinds: &[BindKind],
    rows: &[Vec<Option<String>>],
    collect_errors: bool,
) -> Result<Vec<(usize, String)>, FaucetError> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let mut builder = conn.batch(sql, rows.len());
    if collect_errors {
        builder.with_batch_errors();
    }
    let mut batch = builder.build().map_err(|e| sink_err("prepare", &e))?;
    for (i, k) in kinds.iter().enumerate() {
        batch
            .set_type(i + 1, &k.oracle_type())
            .map_err(|e| sink_err("bind", &e))?;
    }
    for row in rows {
        let refs: Vec<&dyn ToSql> = row.iter().map(|v| v as &dyn ToSql).collect();
        batch.append_row(&refs).map_err(|e| sink_err("bind", &e))?;
    }
    match batch.execute() {
        Ok(()) => Ok(Vec::new()),
        Err(e) => match e.batch_errors() {
            Some(errs) if collect_errors => Ok(errs
                .iter()
                .map(|d| (d.offset() as usize, d.message().to_string()))
                .collect()),
            _ => Err(sink_err("write", &e)),
        },
    }
}

/// Commit `body`'s work, or roll it back when it fails.
fn in_transaction<T>(
    conn: &Connection,
    body: impl FnOnce(&Connection) -> Result<T, FaucetError>,
) -> Result<T, FaucetError> {
    match body(conn) {
        Ok(v) => {
            conn.commit().map_err(|e| sink_err("commit", &e))?;
            Ok(v)
        }
        Err(e) => {
            let _ = conn.rollback();
            Err(e)
        }
    }
}

impl Inner {
    fn conn(&self) -> Result<Connection, FaucetError> {
        checkout(&self.pool, Side::Sink, self.timeout)
    }

    fn is_keyed(&self) -> bool {
        matches!(
            self.config.write.write_mode,
            WriteMode::Upsert | WriteMode::Delete
        )
    }

    /// The table appends land in: staging during an overwrite.
    fn effective(&self) -> (&str, &str) {
        if self.config.write.is_overwrite() {
            (&self.staging, &self.staging_quoted)
        } else {
            (&self.target, &self.target_quoted)
        }
    }

    fn table_exists(&self, conn: &Connection, table: &str) -> Result<bool, FaucetError> {
        let (owner, name) = dictionary_binds(table)?;
        let n: i64 = conn
            .query_row_as(TABLE_EXISTS_SQL, &[&owner, &name])
            .map_err(|e| sink_err("table probe", &e))?;
        Ok(n > 0)
    }

    fn load_columns(&self, conn: &Connection, table: &str) -> Result<Vec<ColumnInfo>, FaucetError> {
        let (owner, name) = dictionary_binds(table)?;
        let rows = conn
            .query_as::<(
                String,
                String,
                Option<i64>,
                Option<i64>,
                String,
                String,
                String,
            )>(COLUMNS_SQL, &[&owner, &name])
            .map_err(|e| sink_err("column discovery", &e))?;
        let mut out = Vec::new();
        for r in rows {
            let (n, t, p, s, nullable, virt, generation) =
                r.map_err(|e| sink_err("column discovery", &e))?;
            out.push(column_from_row(n, t, p, s, &nullable, &virt, &generation));
        }
        Ok(out)
    }

    /// Writable columns of the effective table, discovered once.
    fn columns(&self, conn: &Connection) -> Result<Vec<ColumnInfo>, FaucetError> {
        if let Some(c) = self.columns.lock().expect("columns mutex").clone() {
            return Ok(c);
        }
        let (table, _) = self.effective();
        let cols = self.load_columns(conn, table)?;
        if cols.is_empty() {
            return Err(FaucetError::Sink(format!(
                "oracle table {table:?} does not exist or has no columns"
            )));
        }
        *self.columns.lock().expect("columns mutex") = Some(cols.clone());
        Ok(cols)
    }

    fn forget_columns(&self) {
        *self.columns.lock().expect("columns mutex") = None;
    }

    /// Create the `json_column` table up front (its shape is fixed).
    fn create_json_table(&self, conn: &Connection) -> Result<(), FaucetError> {
        let OracleColumnMapping::JsonColumn { column } = &self.config.column_mapping else {
            return Ok(());
        };
        if !self.config.create_table || self.table_exists(conn, &self.target)? {
            return Ok(());
        }
        let ddl = create_json_table_sql(&self.target_quoted, column)?;
        exec(conn, &ignoring(&ddl, &[ORA_NAME_IN_USE]), "create table")
    }

    /// Make sure the effective table exists before the first write, creating
    /// it from the page's inferred columns in `auto_columns` mode.
    fn ensure_table_ready(&self, conn: &Connection, records: &[Value]) -> Result<(), FaucetError> {
        if self.ready.load(Ordering::Relaxed) {
            return Ok(());
        }
        let (table, quoted) = self.effective();
        if self.table_exists(conn, table)? {
            self.ready.store(true, Ordering::Relaxed);
            return Ok(());
        }
        if !self.config.create_table {
            return Err(faucet_core::missing_target_error("oracle sink", table));
        }
        let planned = match &self.config.column_mapping {
            OracleColumnMapping::JsonColumn { column } => {
                Some(create_json_table_sql(quoted, column)?)
            }
            OracleColumnMapping::AutoColumns { .. } => {
                let key: &[String] = if self.config.write.dedups_by_key() {
                    &self.config.write.key
                } else {
                    &[]
                };
                let cols = if key.is_empty() {
                    faucet_core::plan_columns(records)
                } else {
                    faucet_core::plan_keyed_columns(records, key)
                };
                cols.map(|c| create_table_sql(quoted, &c, key))
                    .transpose()?
            }
        };
        let Some(ddl) = planned else {
            return Ok(());
        };
        exec(conn, &ignoring(&ddl, &[ORA_NAME_IN_USE]), "create table")?;
        self.forget_columns();
        self.ready.store(true, Ordering::Relaxed);
        Ok(())
    }

    /// Resolve the column list, bind kinds and encoded rows for a chunk.
    /// Per-row encoding failures are returned alongside, by chunk index.
    #[allow(clippy::type_complexity)]
    fn prepare(
        &self,
        conn: &Connection,
        chunk: &[Value],
    ) -> Result<
        (
            Vec<String>,
            Vec<BindKind>,
            Vec<(usize, Result<Vec<Option<String>>, String>)>,
        ),
        FaucetError,
    > {
        let (cols, kinds) = match &self.config.column_mapping {
            OracleColumnMapping::JsonColumn { column } => {
                (vec![column.clone()], vec![BindKind::Clob])
            }
            OracleColumnMapping::AutoColumns { on_unknown_field } => {
                let info = self.columns(conn)?;
                let insertable: Vec<String> = info
                    .iter()
                    .filter(|c| c.insertable)
                    .map(|c| c.name.clone())
                    .collect();
                let cols = resolve_insert_columns(&insertable, chunk, *on_unknown_field)?;
                let kinds = cols
                    .iter()
                    .map(|c| {
                        info.iter()
                            .find(|i| &i.name == c)
                            .map(BindKind::for_column)
                            .unwrap_or(BindKind::Text)
                    })
                    .collect();
                (cols, kinds)
            }
        };
        let rows = chunk
            .iter()
            .enumerate()
            .map(|(i, r)| {
                let encoded = match &self.config.column_mapping {
                    OracleColumnMapping::JsonColumn { .. } => Ok(vec![Some(r.to_string())]),
                    OracleColumnMapping::AutoColumns { .. } => encode_row(r, &cols, &kinds),
                };
                (i, encoded)
            })
            .collect();
        Ok((cols, kinds, rows))
    }

    fn chunks<'a>(&self, records: &'a [Value]) -> Vec<&'a [Value]> {
        match self.config.batch_size {
            0 => vec![records],
            n => records.chunks(n).collect(),
        }
    }

    /// Insert `records` into the effective table (no commit). In `partial`
    /// mode, returns a per-record error message instead of failing.
    fn append(
        &self,
        conn: &Connection,
        records: &[Value],
        partial: bool,
    ) -> Result<Vec<Option<String>>, FaucetError> {
        let mut outcomes = vec![None; records.len()];
        let mut base = 0;
        for chunk in self.chunks(records) {
            let (cols, kinds, rows) = self.prepare(conn, chunk)?;
            if cols.is_empty() {
                base += chunk.len();
                continue;
            }
            let mut positions = Vec::with_capacity(rows.len());
            let mut good = Vec::with_capacity(rows.len());
            for (i, encoded) in rows {
                match encoded {
                    Ok(v) => {
                        positions.push(i);
                        good.push(v);
                    }
                    Err(msg) if partial => outcomes[base + i] = Some(msg),
                    Err(msg) => {
                        return Err(FaucetError::Sink(format!("oracle row {}: {msg}", base + i)));
                    }
                }
            }
            let sql = insert_sql(self.effective().1, &cols)?;
            for (offset, msg) in run_batch(conn, &sql, &kinds, &good, partial)? {
                if let Some(&pos) = positions.get(offset) {
                    outcomes[base + pos] = Some(msg);
                }
            }
            base += chunk.len();
        }
        Ok(outcomes)
    }

    /// Apply a planned upsert/delete batch (no commit).
    fn apply_plan(&self, conn: &Connection, plan: &WritePlan) -> Result<usize, FaucetError> {
        let key = &self.config.write.key;
        let mut affected = 0;
        for chunk in self.chunks(&plan.upserts) {
            let (cols, kinds, rows) = self.prepare(conn, chunk)?;
            if cols.is_empty() {
                continue;
            }
            let rows = rows
                .into_iter()
                .map(|(i, r)| {
                    r.map_err(|m| FaucetError::Sink(format!("oracle upsert row {i}: {m}")))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let sql = merge_sql(&self.target_quoted, key, &cols)?;
            run_batch(conn, &sql, &kinds, &rows, false)?;
            affected += rows.len();
        }
        if !plan.deletes.is_empty() {
            let info = self.columns(conn)?;
            let kinds: Vec<BindKind> = key
                .iter()
                .map(|k| {
                    info.iter()
                        .find(|c| &c.name == k)
                        .map(BindKind::for_column)
                        .ok_or_else(|| {
                            FaucetError::Sink(format!(
                                "oracle delete: key column {k:?} does not exist"
                            ))
                        })
                })
                .collect::<Result<_, _>>()?;
            let sql = delete_sql(&self.target_quoted, key)?;
            let per = if self.config.batch_size == 0 {
                plan.deletes.len()
            } else {
                self.config.batch_size
            };
            for chunk in plan.deletes.chunks(per) {
                let rows = chunk
                    .iter()
                    .map(|kt| {
                        kt.0.iter()
                            .zip(&kinds)
                            .map(|((_, v), k)| crate::plan::value_to_text(v, *k))
                            .collect::<Result<Vec<_>, _>>()
                            .map_err(|m| FaucetError::Sink(format!("oracle delete key: {m}")))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                run_batch(conn, &sql, &kinds, &rows, false)?;
                affected += rows.len();
            }
        }
        Ok(affected)
    }

    fn plan(&self, records: &[Value]) -> Result<WritePlan, FaucetError> {
        let plan = faucet_core::plan_writes(records, &self.config.write);
        if let Some((idx, msg)) = plan.failed.first() {
            return Err(FaucetError::Sink(format!(
                "oracle {}: row {idx}: {msg}",
                self.config.write.write_mode.as_str()
            )));
        }
        Ok(plan)
    }

    fn ensure_token_table(&self, conn: &Connection) -> Result<(), FaucetError> {
        exec(
            conn,
            &ignoring(&token_table_ddl(&self.token_table), &[ORA_NAME_IN_USE]),
            "create watermark table",
        )
    }

    fn read_token(&self, conn: &Connection, scope: &str) -> Result<Option<String>, FaucetError> {
        let scope = faucet_core::idempotency::scope_key(scope, crate::plan::SCOPE_COL_WIDTH);
        let rows = conn
            .query_as::<String>(&token_select_sql(&self.token_table), &[&scope])
            .map_err(|e| sink_err("watermark read", &e))?;
        rows.into_iter()
            .next()
            .transpose()
            .map_err(|e| sink_err("watermark read", &e))
    }

    fn write_token(&self, conn: &Connection, scope: &str, token: &str) -> Result<(), FaucetError> {
        let scope = faucet_core::idempotency::scope_key(scope, crate::plan::SCOPE_COL_WIDTH);
        let token = token.to_string();
        let clob = OracleType::CLOB;
        conn.execute(
            &token_merge_sql(&self.token_table),
            &[&scope, &(&token, &clob)],
        )
        .map_err(|e| sink_err("watermark write", &e))?;
        Ok(())
    }
}

impl OracleSink {
    /// Validate, connect, and (in `json_column` mode) create the table.
    pub async fn new(config: OracleSinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let config = config.normalized();
        let target = config.table.clone();
        let staging = format!("{target}{OVERWRITE_STAGING_SUFFIX}");
        let inner = Inner {
            target_quoted: quote_table_oracle(&target)?,
            staging_quoted: quote_table_oracle(&staging)?,
            token_table: token_table(&target)?,
            pool: connect_pool(&config.connection, config.max_connections).await?,
            timeout: call_timeout(config.statement_timeout_secs),
            target,
            staging,
            config,
            columns: Mutex::new(None),
            ready: AtomicBool::new(false),
        };
        let sink = Self {
            inner: Arc::new(inner),
        };
        sink.run(|i| {
            let conn = i.conn()?;
            i.create_json_table(&conn)
        })
        .await?;
        Ok(sink)
    }

    async fn run<T, F>(&self, f: F) -> Result<T, FaucetError>
    where
        F: FnOnce(&Inner) -> Result<T, FaucetError> + Send + 'static,
        T: Send + 'static,
    {
        let inner = self.inner.clone();
        blocking(move || f(&inner)).await
    }

    /// Record keys are identifiers only in `auto_columns` mode; a JSON payload
    /// is stored verbatim.
    fn cased(&self, records: &[Value]) -> Vec<Value> {
        let cfg = &self.inner.config;
        let upper = cfg.identifier_case == IdentifierCase::Upper
            && matches!(cfg.column_mapping, OracleColumnMapping::AutoColumns { .. });
        case_records(records, upper).into_owned()
    }
}

#[async_trait]
impl Sink for OracleSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        let records = self.cased(records);
        self.run(move |i| {
            let conn = i.conn()?;
            i.ensure_table_ready(&conn, &records)?;
            if i.is_keyed() {
                let plan = i.plan(&records)?;
                return in_transaction(&conn, |c| i.apply_plan(c, &plan));
            }
            in_transaction(&conn, |c| {
                i.append(c, &records, false).map(|_| records.len())
            })
        })
        .await
    }

    async fn write_batch_partial(&self, records: &[Value]) -> Result<Vec<RowOutcome>, FaucetError> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        let records = self.cased(records);
        let mode = self.inner.config.write.write_mode.as_str();
        let messages = self
            .run(move |i| {
                let conn = i.conn()?;
                i.ensure_table_ready(&conn, &records)?;
                if i.is_keyed() {
                    let plan = faucet_core::plan_writes(&records, &i.config.write);
                    in_transaction(&conn, |c| i.apply_plan(c, &plan))?;
                    let mut out = vec![None; records.len()];
                    for (idx, msg) in plan.failed {
                        out[idx] = Some(msg);
                    }
                    return Ok(out);
                }
                in_transaction(&conn, |c| i.append(c, &records, true))
            })
            .await?;
        Ok(messages
            .into_iter()
            .map(|m| match m {
                None => Ok(()),
                Some(msg) => Err(FaucetError::Sink(format!("oracle {mode}: {msg}"))),
            })
            .collect())
    }

    fn supports_idempotent_writes(&self) -> bool {
        true
    }

    async fn write_batch_idempotent(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<usize, FaucetError> {
        let records = self.cased(records);
        let (scope, token) = (scope.to_string(), token.to_string());
        self.run(move |i| {
            let conn = i.conn()?;
            if !records.is_empty() {
                i.ensure_table_ready(&conn, &records)?;
            }
            i.ensure_token_table(&conn)?;
            let plan = if i.is_keyed() {
                Some(i.plan(&records)?)
            } else {
                None
            };
            in_transaction(&conn, |c| {
                let n = match &plan {
                    Some(p) => i.apply_plan(c, p)?,
                    None if records.is_empty() => 0,
                    None => i.append(c, &records, false).map(|_| records.len())?,
                };
                i.write_token(c, &scope, &token)?;
                Ok(n)
            })
        })
        .await
    }

    async fn last_committed_token(&self, scope: &str) -> Result<Option<String>, FaucetError> {
        let scope = scope.to_string();
        self.run(move |i| {
            let conn = i.conn()?;
            i.ensure_token_table(&conn)?;
            i.read_token(&conn, &scope)
        })
        .await
    }

    fn dedups_by_key(&self) -> bool {
        self.inner.config.write.dedups_by_key()
    }

    fn supported_write_modes(&self) -> &'static [WriteMode] {
        &[
            WriteMode::Append,
            WriteMode::Upsert,
            WriteMode::Delete,
            WriteMode::Overwrite,
        ]
    }

    fn is_overwrite(&self) -> bool {
        self.inner.config.write.is_overwrite()
    }

    /// Drop any leftover staging, then clone the target's structure into a
    /// fresh staging table. A first run (no target, `create_table: true`)
    /// leaves staging to be created from the first page.
    async fn begin_overwrite(&self) -> Result<(), FaucetError> {
        self.run(|i| {
            let conn = i.conn()?;
            exec(&conn, &drop_table_sql(&i.staging_quoted), "drop staging")?;
            i.forget_columns();
            i.ready.store(false, Ordering::Relaxed);
            let exists = i.table_exists(&conn, &i.target)?;
            if !exists && i.config.create_table {
                return Ok(());
            }
            if !exists {
                return Err(faucet_core::missing_target_error("oracle sink", &i.target));
            }
            exec(
                &conn,
                &clone_table_sql(&i.staging_quoted, &i.target_quoted),
                "create staging",
            )
        })
        .await
    }

    /// Replace the target's rows with staging's in one transaction (readers see
    /// the old rows until commit), then drop staging. A first run renames
    /// staging into place.
    async fn commit_overwrite(&self) -> Result<(), FaucetError> {
        self.run(|i| {
            let conn = i.conn()?;
            if !i.table_exists(&conn, &i.target)? {
                if i.table_exists(&conn, &i.staging)? {
                    exec(
                        &conn,
                        &rename_sql(&i.staging_quoted, &i.target)?,
                        "publish staging",
                    )?;
                }
                return Ok(());
            }
            let cols: Vec<String> = i
                .load_columns(&conn, &i.target)?
                .into_iter()
                .filter(|c| c.insertable)
                .map(|c| c.name)
                .collect();
            let [delete, insert] = swap_sql(&i.target_quoted, &i.staging_quoted, &cols)?;
            in_transaction(&conn, |c| {
                exec(c, &delete, "overwrite swap")?;
                exec(c, &insert, "overwrite swap")
            })?;
            exec(&conn, &drop_table_sql(&i.staging_quoted), "drop staging")
        })
        .await
    }

    async fn abort_overwrite(&self) -> Result<(), FaucetError> {
        self.run(|i| {
            let conn = i.conn()?;
            exec(&conn, &drop_table_sql(&i.staging_quoted), "drop staging")
        })
        .await
    }

    fn supports_schema_evolution(&self) -> bool {
        true
    }

    async fn current_schema(&self) -> Result<Option<Value>, FaucetError> {
        self.run(|i| {
            let conn = i.conn()?;
            let cols = i.load_columns(&conn, &i.target)?;
            Ok((!cols.is_empty()).then(|| schema_from_columns(&cols)))
        })
        .await
    }

    async fn evolve_schema(
        &self,
        evolution: &faucet_core::SchemaEvolution,
    ) -> Result<(), FaucetError> {
        let evolution = evolution.clone();
        self.run(move |i| {
            let conn = i.conn()?;
            let base = |c: &faucet_core::ColumnChange| {
                faucet_core::json_schema_base_type(&c.to).unwrap_or(faucet_core::SqlBaseType::Text)
            };
            for c in &evolution.additions {
                exec(
                    &conn,
                    &add_column_sql(&i.target_quoted, &c.name, base(c))?,
                    "add column",
                )?;
            }
            for c in &evolution.widenings {
                exec(
                    &conn,
                    &widen_column_sql(&i.target_quoted, &c.name, base(c))?,
                    "widen column",
                )?;
            }
            for col in &evolution.relax_nullability {
                exec(
                    &conn,
                    &relax_null_sql(&i.target_quoted, col)?,
                    "relax not null",
                )?;
            }
            i.forget_columns();
            Ok(())
        })
        .await
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(OracleSinkConfig))
            .expect("schema serialization")
    }

    fn connector_name(&self) -> &'static str {
        "oracle"
    }

    fn dataset_uri(&self) -> String {
        format!(
            "{}?table={}",
            self.inner.config.connection.display_uri(),
            self.inner.config.table
        )
    }

    async fn check(&self, ctx: &CheckContext) -> Result<CheckReport, FaucetError> {
        let started = std::time::Instant::now();
        let probe = self.run(|i| {
            let conn = i.conn()?;
            let exists = i.table_exists(&conn, &i.target)?;
            Ok(exists || i.config.create_table)
        });
        let hint = "check the connection settings / credentials and that the listener is reachable";
        let probes = match tokio::time::timeout(ctx.timeout, probe).await {
            Ok(Ok(ready)) => {
                let target = if ready {
                    Probe::pass("target", started.elapsed())
                } else {
                    Probe::fail_hint(
                        "target",
                        started.elapsed(),
                        format!("table {:?} does not exist", self.inner.target),
                        "create it, or set `create_table: true`",
                    )
                };
                vec![Probe::pass("connect", started.elapsed()), target]
            }
            Ok(Err(e)) => vec![Probe::fail_hint(
                "connect",
                started.elapsed(),
                e.to_string(),
                hint,
            )],
            Err(_) => vec![Probe::fail_hint(
                "connect",
                started.elapsed(),
                "timed out",
                hint,
            )],
        };
        Ok(CheckReport { probes })
    }
}
