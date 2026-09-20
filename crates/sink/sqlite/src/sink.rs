//! SQLite sink implementation.

use crate::config::{SqliteColumnMapping, SqliteSinkConfig};
use async_trait::async_trait;
use faucet_core::util::quote_ident;
use faucet_core::{FaucetError, SchemaEvolution, SqlBaseType, json_schema_base_type};
use serde_json::Value;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};
use std::str::FromStr;
use std::time::Duration;

/// Quote a SQLite identifier with backticks.
///
/// Deliberately NOT ANSI double quotes for the scoped-cleanup path: SQLite's
/// double-quoted-string misfeature silently reinterprets a double-quoted
/// identifier that does not resolve to a column as a **string literal**. In a
/// cleanup that is unacceptable — a typo'd scope column would make
/// `t."typo" = ?` a constant comparison instead of erroring, and the DELETE
/// would then match the wrong rows (or every row in the table). Backtick-quoted
/// identifiers are always identifiers, so an unknown column surfaces as a
/// proper "no such column" error. Embedded backticks are doubled, preventing
/// identifier injection. Mirrors `quote_ident_sqlite` in `faucet-source-sqlite`.
fn quote_ident_sqlite(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Transient table holding the key tuples this run wrote, joined against by the
/// scoped-cleanup DELETE (#478).
///
/// Always created in — and referenced through — the `temp` schema, so it can
/// never be confused with (or, on the defensive `DROP`, destroy) a real table of
/// the same name in the main database.
const CLEANUP_KEYS_TABLE: &str = "faucet_cleanup_keys";

/// Schema-qualified, quoted reference to [`CLEANUP_KEYS_TABLE`].
fn cleanup_keys_ref() -> String {
    format!("temp.{}", quote_ident_sqlite(CLEANUP_KEYS_TABLE))
}

/// A declared SQLite column type (`PRAGMA table_info.type`) that is safe to
/// re-emit verbatim in the cleanup temp table's DDL, or `None`.
///
/// The declared type comes from the database's own catalog, but SQLite lets a
/// column be declared with *arbitrary quoted text*, so it is filtered to the
/// shape real type specs take (`VARCHAR(255)`, `DOUBLE PRECISION`,
/// `DECIMAL(10, 2)`) rather than pasted in blind. `None` means "declare the temp
/// column without a type" — legal in SQLite, and only costs the column its type
/// affinity.
fn safe_type_spec(declared: &str) -> Option<&str> {
    let t = declared.trim();
    if t.is_empty() {
        return None;
    }
    t.chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, ' ' | '_' | '(' | ')' | ',' | '.'))
        .then_some(t)
}

/// `CREATE TEMP TABLE temp.`faucet_cleanup_keys` (…)` — one column per key
/// column, mirroring the destination column's declared type so the join
/// comparison sees matching type affinities.
fn build_cleanup_temp_table_sql(key_types: &[(String, String)]) -> String {
    let cols = key_types
        .iter()
        .map(|(col, declared)| match safe_type_spec(declared) {
            Some(t) => format!("{} {t}", quote_ident_sqlite(col)),
            None => quote_ident_sqlite(col),
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("CREATE TEMP TABLE {} ({cols})", cleanup_keys_ref())
}

/// `INSERT INTO temp.`faucet_cleanup_keys` (…) VALUES (?, …), …` for `rows`
/// key tuples. The caller chunks `rows` to stay under SQLite's bind-variable
/// cap.
fn build_cleanup_insert_sql(key: &[String], rows: usize) -> String {
    let col_list = key
        .iter()
        .map(|k| quote_ident_sqlite(k))
        .collect::<Vec<_>>()
        .join(", ");
    let tuple = format!("({})", vec!["?"; key.len()].join(", "));
    let tuples = vec![tuple; rows].join(", ");
    format!(
        "INSERT INTO {} ({col_list}) VALUES {tuples}",
        cleanup_keys_ref()
    )
}

/// The cleanup DELETE: every row matching the scope (equality predicates,
/// AND-ed, one bind each) whose key is absent from the written-key table.
///
/// The target table is referenced by name rather than an alias — a single-table
/// `DELETE … AS alias` is a newer SQLite grammar, and the correlated reference
/// works identically through the table name.
fn build_cleanup_delete_sql(table: &str, scope_cols: &[String], key: &[String]) -> String {
    let t = quote_ident_sqlite(table);
    let scope_pred = scope_cols
        .iter()
        .map(|c| format!("{t}.{} = ?", quote_ident_sqlite(c)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let join_pred = key
        .iter()
        .map(|k| {
            let q = quote_ident_sqlite(k);
            format!("c.{q} = {t}.{q}")
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    format!(
        "DELETE FROM {t} WHERE {scope_pred} AND NOT EXISTS (SELECT 1 FROM {} c WHERE {join_pred})",
        cleanup_keys_ref()
    )
}

/// Check that every scope and key column exists on the destination table.
///
/// Fails with a clear message rather than letting SQLite reject an unknown
/// column mid-DELETE. The scope is written in *destination* terms, so a name
/// that isn't a real column is a config error worth naming.
fn validate_cleanup_columns(
    existing: &std::collections::HashSet<String>,
    scope_cols: &[String],
    key: &[String],
    table: &str,
) -> Result<(), FaucetError> {
    for col in scope_cols.iter().chain(key.iter()) {
        if !existing.contains(col) {
            return Err(FaucetError::Sink(format!(
                "cleanup: column '{col}' does not exist on table '{table}' — the \
                 completeness claim and `key` are in destination column terms"
            )));
        }
    }
    Ok(())
}

/// Bind one JSON value to a SQLite query as its native type.
///
/// Shared by the delete-by-key and scoped-cleanup paths so the two never drift:
/// a key bound as a JSON string (`"7"` instead of `7`) would silently match
/// nothing and turn a delete into a no-op.
fn bind_value<'q>(
    q: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>,
    v: &Value,
) -> sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>> {
    match v {
        Value::Null => q.bind(None::<String>),
        Value::Bool(b) => q.bind(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                q.bind(i)
            } else if let Some(f) = n.as_f64() {
                q.bind(f)
            } else {
                // u64 above i64::MAX — preserve exact text.
                q.bind(n.to_string())
            }
        }
        Value::String(s) => q.bind(s.clone()),
        // Arrays/objects have no scalar SQL representation — bind their JSON
        // text (suitable for TEXT columns).
        other => q.bind(other.to_string()),
    }
}

/// `CREATE TABLE IF NOT EXISTS` for an auto-created target (#580).
///
/// `IF NOT EXISTS` rather than probe-then-create: two matrix rows writing the
/// same table would otherwise race between the probe and the DDL.
fn build_create_table_sql(
    table: &str,
    columns: &[faucet_core::PlannedColumn],
    json_column: Option<&str>,
) -> String {
    let cols = match json_column {
        // JSON mode stores the whole record in one column, so the page's own
        // shape is irrelevant — the table is the same whatever arrives.
        Some(col) => format!(
            "{} INTEGER PRIMARY KEY AUTOINCREMENT, {} TEXT NOT NULL",
            quote_ident_sqlite("id"),
            quote_ident_sqlite(col)
        ),
        None => faucet_core::render_columns(columns, |n| quote_ident_sqlite(n), sqlite_keyword),
    };
    format!(
        "CREATE TABLE IF NOT EXISTS {} ({cols})",
        quote_ident_sqlite(table)
    )
}

/// Map a [`SqlBaseType`] to the SQLite column-type keyword used when adding a
/// column during schema evolution (issue #194). SQLite uses dynamic typing
/// (type affinity), so these are advisory affinities rather than strict types:
/// `Boolean` maps to `INTEGER` (SQLite has no native boolean) and `Json` to
/// `TEXT` (JSON is stored as text).
fn sqlite_keyword(t: SqlBaseType) -> &'static str {
    match t {
        SqlBaseType::Integer => "INTEGER",
        SqlBaseType::Double => "REAL",
        SqlBaseType::Boolean => "INTEGER",
        SqlBaseType::Text => "TEXT",
        SqlBaseType::Json => "TEXT",
    }
}

/// `ALTER TABLE <table> ADD COLUMN "<col>" <kw>` — SQLite has no
/// `ADD COLUMN IF NOT EXISTS`, so [`SqliteSink::evolve_schema`] only emits this
/// for columns it has already verified are absent (idempotency by pre-check).
/// `table` is the unquoted table name; it is quoted here via [`quote_ident`].
fn build_add_column_sql(table: &str, col: &str, t: SqlBaseType) -> String {
    format!(
        "ALTER TABLE {} ADD COLUMN {} {}",
        quote_ident(table),
        quote_ident(col),
        sqlite_keyword(t)
    )
}

/// Map a SQLite column affinity string (`PRAGMA table_info.type`, e.g. `INTEGER`,
/// `REAL`, `VARCHAR(255)`, `TEXT`) to a JSON-Schema type fragment so
/// [`SqliteSink::current_schema`] round-trips with [`faucet_core::diff_schema`].
///
/// SQLite determines affinity by a tolerant, case-insensitive substring match on
/// the declared type (the rules in <https://www.sqlite.org/datatype3.html>), so
/// this mirrors that: contains `INT` → integer; `CHAR`/`CLOB`/`TEXT` → string;
/// `REAL`/`FLOA`/`DOUB` (and the loose `NUMERIC`/`DECIMAL`) → number; everything
/// else falls back to string. `nullable` reflects `PRAGMA table_info.notnull == 0`.
fn sqlite_affinity_to_json_schema(declared: &str, nullable: bool) -> serde_json::Value {
    let up = declared.to_ascii_uppercase();
    let contains = |needle: &str| up.contains(needle);
    let base = if contains("INT") {
        "integer"
    } else if contains("CHAR") || contains("CLOB") || contains("TEXT") {
        "string"
    } else if contains("REAL")
        || contains("FLOA")
        || contains("DOUB")
        || contains("NUMERIC")
        || contains("DECIMAL")
    {
        "number"
    } else {
        "string"
    };
    if nullable {
        serde_json::json!({ "type": [base, "null"] })
    } else {
        serde_json::json!({ "type": base })
    }
}

/// Build the `ON CONFLICT(key) DO UPDATE …` tail for an upsert INSERT.
/// Non-key columns are SET from `excluded`. If every column is a key column,
/// emit `DO NOTHING`.
fn on_conflict_clause(key: &[String], all_cols: &[String]) -> String {
    let key_list = key
        .iter()
        .map(|k| quote_ident(k))
        .collect::<Vec<_>>()
        .join(", ");
    let updates: Vec<String> = all_cols
        .iter()
        .filter(|c| !key.iter().any(|k| k == *c))
        .map(|c| format!("{q} = excluded.{q}", q = quote_ident(c)))
        .collect();
    if updates.is_empty() {
        format!("ON CONFLICT({key_list}) DO NOTHING")
    } else {
        format!(
            "ON CONFLICT({key_list}) DO UPDATE SET {}",
            updates.join(", ")
        )
    }
}

/// A sink that writes JSON records to a SQLite table.
pub struct SqliteSink {
    config: SqliteSinkConfig,
    pool: SqlitePool,
    /// Whether the target has been confirmed present for this sink instance
    /// (#580). One check per run, not per page.
    table_ready: std::sync::atomic::AtomicBool,
}

impl SqliteSink {
    /// Make sure the target table exists before the first write (#580).
    async fn ensure_table_ready(&self, records: &[Value]) -> Result<(), FaucetError> {
        use std::sync::atomic::Ordering;
        if self.table_ready.load(Ordering::Relaxed) {
            return Ok(());
        }
        if !self.config.create_table {
            let exists: Option<String> = sqlx::query_scalar(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
            )
            .bind(&self.config.table_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite table probe failed: {e}")))?;
            if exists.is_none() {
                return Err(faucet_core::missing_target_error(
                    "sqlite sink",
                    &self.config.table_name,
                ));
            }
            self.table_ready.store(true, Ordering::Relaxed);
            return Ok(());
        }

        let json_column = match &self.config.column_mapping {
            SqliteColumnMapping::AutoMap => None,
            SqliteColumnMapping::Json { column } => Some(column.as_str()),
        };
        let columns = match (json_column, faucet_core::plan_columns(records)) {
            (Some(_), _) => Vec::new(),
            (None, Some(c)) => c,
            (None, None) => return Ok(()),
        };
        let sql = build_create_table_sql(&self.config.table_name, &columns, json_column);
        sqlx::query(&sql)
            .execute(&self.pool)
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite CREATE TABLE failed: {e}")))?;
        self.table_ready.store(true, Ordering::Relaxed);
        Ok(())
    }

    /// Create a new SQLite sink. Establishes a connection pool.
    ///
    /// The pool opens each connection with `journal_mode = WAL` and a 5-second
    /// `busy_timeout`. WAL lets a writer and readers proceed concurrently
    /// instead of locking each other out, and the busy timeout makes a
    /// connection wait-and-retry for the write lock rather than failing
    /// immediately with `SQLITE_BUSY` under contention. `create_if_missing`
    /// preserves the previous behaviour of creating the database file on first
    /// open. WAL on a `sqlite::memory:` database is a harmless no-op.
    pub async fn new(config: SqliteSinkConfig) -> Result<Self, FaucetError> {
        config.write.validate()?;
        if matches!(
            config.write.write_mode,
            faucet_core::WriteMode::Upsert | faucet_core::WriteMode::Delete
        ) && !matches!(config.column_mapping, SqliteColumnMapping::AutoMap)
        {
            return Err(FaucetError::Config(
                "sqlite sink: write_mode upsert/delete requires column_mapping: auto_map \
                 (key columns must be real columns, not inside a JSON blob)"
                    .into(),
            ));
        }

        let options = SqliteConnectOptions::from_str(&config.database_url)
            .map_err(|e| FaucetError::Sink(format!("invalid SQLite database_url: {e}")))?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(5));

        let pool = SqlitePoolOptions::new()
            .max_connections(config.max_connections)
            .connect_with(options)
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite connection failed: {e}")))?;

        Ok(Self {
            config,
            pool,
            table_ready: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// The staging table name used while an overwrite run is in flight.
    fn staging_table(&self) -> String {
        format!(
            "{}{}",
            self.config.table_name,
            faucet_core::idempotency::OVERWRITE_STAGING_SUFFIX
        )
    }

    /// The table the data-write path targets. For `write_mode: overwrite` every
    /// write in this sink's lifetime lands in the staging table (created by
    /// [`begin_overwrite`], swapped into the real table by
    /// [`commit_overwrite`]); otherwise it is the configured table.
    fn effective_table(&self) -> String {
        if self.config.write.is_overwrite() {
            self.staging_table()
        } else {
            self.config.table_name.clone()
        }
    }

    /// Insert JSON-column records within an existing transaction, sub-chunking
    /// at SQLite's bind-variable cap. JSON mode binds one variable per row.
    async fn insert_json_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        records: &[Value],
        column: &str,
    ) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        // SQLite caps bind params per statement at 32766 (>=3.32). JSON mode
        // binds one variable per row, so chunk at that cap.
        const MAX_SQLITE_VARS: usize = 32766;
        for chunk in records.chunks(MAX_SQLITE_VARS) {
            let placeholders: Vec<&str> = chunk.iter().map(|_| "(?)").collect();
            let insert_sql = format!(
                "INSERT INTO {} ({}) VALUES {}",
                quote_ident(&self.effective_table()),
                quote_ident(column),
                placeholders.join(", ")
            );
            let mut q = sqlx::query(&insert_sql);
            for record in chunk {
                let json_str = serde_json::to_string(record)
                    .map_err(|e| FaucetError::Sink(format!("failed to serialize record: {e}")))?;
                q = q.bind(json_str);
            }
            q.execute(&mut **tx)
                .await
                .map_err(|e| FaucetError::Sink(format!("SQLite insert failed: {e}")))?;
        }
        Ok(records.len())
    }

    /// Insert a batch of records using JSON column mode.
    /// Opens its own `BEGIN`/`COMMIT` transaction and delegates to
    /// [`Self::insert_json_tx`], which sub-chunks at SQLite's bind-variable cap.
    async fn insert_json(&self, records: &[Value], column: &str) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite transaction begin failed: {e}")))?;
        let n = self.insert_json_tx(&mut tx, records, column).await?;
        tx.commit()
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite transaction commit failed: {e}")))?;
        Ok(n)
    }

    /// Insert a batch of records using auto-mapped columns.
    ///
    /// Discovers column names from `pragma_table_info` and maps
    /// top-level JSON fields to columns. Uses a single multi-row INSERT
    /// wrapped in a transaction.
    async fn insert_auto_map(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite transaction begin failed: {e}")))?;

        let written = self.insert_auto_map_tx(&mut tx, records).await?;

        tx.commit()
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite transaction commit failed: {e}")))?;

        Ok(written)
    }

    /// Auto-map insert against an in-progress transaction.
    ///
    /// This is the reusable core shared by [`Self::insert_auto_map`] (which
    /// opens its own `BEGIN`/`COMMIT`) and [`faucet_core::Sink::write_batch_idempotent`]
    /// (which folds the insert and the commit-token upsert into one
    /// transaction). The read-only `PRAGMA table_info` column-discovery query
    /// runs on the transaction's own connection (`&mut **tx`), not on
    /// `&self.pool` — otherwise, with the default single-connection pool, it
    /// would deadlock waiting for a connection the open transaction is holding.
    ///
    /// When `conflict_key` is `Some(key)`, each sub-chunk's INSERT is given an
    /// `ON CONFLICT(key) DO UPDATE …` tail so it upserts by the key columns
    /// (last-write-wins within the batch is handled by the planner's dedup,
    /// so a single sub-chunk never double-hits the same conflict target).
    async fn insert_auto_map_with_conflict_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        records: &[Value],
        conflict_key: Option<&[String]>,
    ) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        // Get column names from the table using pragma_table_info. Use the
        // transaction's connection so a single-connection pool doesn't deadlock.
        let effective_table = self.effective_table();
        let columns: Vec<String> = sqlx::query(&format!(
            "PRAGMA table_info({})",
            quote_ident(&effective_table)
        ))
        .fetch_all(&mut **tx)
        .await
        .map_err(|e| FaucetError::Sink(format!("failed to query table columns: {e}")))?
        .iter()
        .map(|row| row.get::<String, _>("name"))
        .collect();

        if columns.is_empty() {
            return Err(FaucetError::Sink(format!(
                "table '{effective_table}' has no columns or does not exist"
            )));
        }

        // Pre-validate all records and collect matched column values. The
        // INSERT column set is the UNION of table columns present in ANY record
        // (in declared table order), not just the first record's keys —
        // otherwise a field present only in a later record of the batch would be
        // silently dropped (audit #146 H1). A row missing a unioned column binds
        // SQL NULL.
        let mut matched_rows: Vec<Vec<(&String, &Value)>> = Vec::with_capacity(records.len());
        let mut used: std::collections::HashSet<&str> = std::collections::HashSet::new();

        for record in records {
            let obj = record
                .as_object()
                .ok_or_else(|| FaucetError::Sink("AutoMap requires JSON object records".into()))?;

            let matching: Vec<(&String, &Value)> = columns
                .iter()
                .filter_map(|col| obj.get(col).map(|v| (col, v)))
                .collect();

            if matching.is_empty() {
                tracing::warn!(
                    record_keys = ?obj.keys().collect::<Vec<_>>(),
                    table_columns = ?columns,
                    "record has no keys matching table columns, skipping"
                );
                continue;
            }

            for (c, _) in &matching {
                used.insert(c.as_str());
            }
            matched_rows.push(matching);
        }

        if matched_rows.is_empty() {
            return Ok(0);
        }

        // Table columns (in declared order) that appear in at least one record.
        let insert_columns: Vec<String> = columns
            .iter()
            .filter(|c| used.contains(c.as_str()))
            .cloned()
            .collect();

        let num_cols = insert_columns.len();
        let num_rows = matched_rows.len();
        let col_names: Vec<String> = insert_columns.iter().map(|c| quote_ident(c)).collect();

        // SQLite caps bind parameters per statement at SQLITE_MAX_VARIABLE_NUMBER
        // (32766 since 3.32). A multi-row INSERT binds `rows × num_cols`
        // parameters, so a wide table at a large batch_size can exceed it and
        // fail at runtime with "too many SQL variables" (#78/#21). Split into
        // sub-INSERTs of at most floor(MAX_VARS / num_cols) rows.
        const MAX_SQLITE_VARS: usize = 32766;
        let max_rows_per_insert = (MAX_SQLITE_VARS / num_cols).max(1);

        for sub in matched_rows.chunks(max_rows_per_insert) {
            // Build multi-row VALUES clause: (?, ?), (?, ?), ...
            let row_placeholder = format!("({})", vec!["?"; num_cols].join(", "));
            let value_tuples: Vec<&str> =
                (0..sub.len()).map(|_| row_placeholder.as_str()).collect();
            let base_query = format!(
                "INSERT INTO {} ({}) VALUES {}",
                quote_ident(&effective_table),
                col_names.join(", "),
                value_tuples.join(", ")
            );
            let query = match conflict_key {
                Some(key) => format!("{base_query} {}", on_conflict_clause(key, &insert_columns)),
                None => base_query,
            };

            let mut q = sqlx::query(&query);
            for matched in sub {
                for col in &insert_columns {
                    let val = matched.iter().find(|(c, _)| *c == col).map(|(_, v)| *v);
                    // Bind native SQLite types so column affinity and typed reads
                    // round-trip correctly. Binding every value as a JSON string
                    // (the old behaviour) stored `"Bob"` with embedded quotes,
                    // turned `true` into the text "true", and bound the literal
                    // text "null" for absent columns instead of SQL NULL (#78/#4).
                    q = match val {
                        None | Some(Value::Null) => q.bind(None::<String>),
                        Some(Value::Bool(b)) => q.bind(*b),
                        Some(Value::Number(n)) => {
                            if let Some(i) = n.as_i64() {
                                q.bind(i)
                            } else if let Some(f) = n.as_f64() {
                                q.bind(f)
                            } else {
                                // u64 above i64::MAX — preserve exact text.
                                q.bind(n.to_string())
                            }
                        }
                        Some(Value::String(s)) => q.bind(s.clone()),
                        // Arrays/objects have no scalar SQL representation — store
                        // their JSON text (suitable for TEXT / JSON columns).
                        Some(v) => q.bind(v.to_string()),
                    };
                }
            }

            q.execute(&mut **tx)
                .await
                .map_err(|e| FaucetError::Sink(format!("SQLite insert failed: {e}")))?;
        }

        Ok(num_rows)
    }

    /// Auto-map insert against an in-progress transaction with plain append
    /// semantics (no `ON CONFLICT` tail).
    ///
    /// Thin wrapper over
    /// [`insert_auto_map_with_conflict_tx`](Self::insert_auto_map_with_conflict_tx)
    /// so the append path and the idempotent-write path keep their original
    /// signature.
    async fn insert_auto_map_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        records: &[Value],
    ) -> Result<usize, FaucetError> {
        self.insert_auto_map_with_conflict_tx(tx, records, None)
            .await
    }

    /// Delete rows whose key columns match any of `deletes`, using
    /// `DELETE FROM t WHERE (k1, …) IN ((?, …), …)`, chunked at
    /// SQLite's bind-variable cap. Runs inside the caller's transaction.
    async fn delete_by_keys(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        deletes: &[faucet_core::KeyTuple],
    ) -> Result<usize, FaucetError> {
        if deletes.is_empty() {
            return Ok(0);
        }
        let key = &self.config.write.key;
        let table_ref = quote_ident(&self.config.table_name);
        let col_list = key
            .iter()
            .map(|k| quote_ident(k))
            .collect::<Vec<_>>()
            .join(", ");

        const MAX_SQLITE_VARS: usize = 32766;
        let per = (MAX_SQLITE_VARS / key.len().max(1)).max(1);
        let mut total = 0usize;

        for chunk in deletes.chunks(per) {
            let tuples: Vec<String> = chunk
                .iter()
                .map(|_| format!("({})", vec!["?"; key.len()].join(", ")))
                .collect();
            let sql = format!(
                "DELETE FROM {table_ref} WHERE ({col_list}) IN ({})",
                tuples.join(", ")
            );
            let mut q = sqlx::query(&sql);
            for kt in chunk {
                for (_, v) in &kt.0 {
                    // Bind native SQLite types — same logic as in the INSERT path.
                    q = bind_value(q, v);
                }
            }
            let res = q
                .execute(&mut **tx)
                .await
                .map_err(|e| FaucetError::Sink(format!("SQLite delete failed: {e}")))?;
            total += res.rows_affected() as usize;
        }
        Ok(total)
    }

    /// Apply a planned upsert/delete batch inside one `BEGIN`/`COMMIT`
    /// transaction. Upserts and deletes are wrapped together so they commit
    /// atomically.
    async fn apply_plan(&self, plan: &faucet_core::WritePlan) -> Result<usize, FaucetError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite transaction begin failed: {e}")))?;

        let mut affected = 0usize;
        if !plan.upserts.is_empty() {
            affected += self
                .insert_auto_map_with_conflict_tx(
                    &mut tx,
                    &plan.upserts,
                    Some(&self.config.write.key),
                )
                .await?;
        }
        if !plan.deletes.is_empty() {
            affected += self.delete_by_keys(&mut tx, &plan.deletes).await?;
        }

        tx.commit()
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite transaction commit failed: {e}")))?;
        Ok(affected)
    }

    /// Delete rows in `scope` whose key was not written by this run (#478).
    ///
    /// Uses a temp table + `NOT EXISTS` rather than `key NOT IN (…)` because the
    /// written-key set routinely exceeds SQLite's 32766 bind-variable limit (the
    /// cleanup ceiling defaults to 100k rows). It also makes the whole thing one
    /// transaction, so the delete is all-or-nothing: a partial delete would
    /// remove rows the run actually wrote.
    ///
    /// An empty `seen` set is meaningful, not a no-op — it means the source
    /// reported the scope as empty, so every row in it is stale and must go. That
    /// is the case this feature exists for, and `NOT EXISTS` against an empty
    /// table handles it without a special branch.
    ///
    /// Every statement runs on the transaction's own connection: the temp table
    /// lives in that connection's `temp` schema, and with the default
    /// single-connection pool any query sent to `&self.pool` instead would
    /// deadlock waiting for the connection the open transaction is holding.
    async fn cleanup_scope_impl(
        &self,
        scope: &std::collections::BTreeMap<String, Value>,
        seen: &faucet_core::SeenKeys,
    ) -> Result<u64, FaucetError> {
        let key = &self.config.write.key;
        if key.is_empty() {
            return Err(FaucetError::Sink(
                "cleanup requires a non-empty `key`".to_string(),
            ));
        }
        let table = &self.config.table_name;

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite transaction begin failed: {e}")))?;

        // Live column set + declared types, from the table this DELETE targets.
        let declared: std::collections::HashMap<String, String> =
            sqlx::query(&format!("PRAGMA table_info({})", quote_ident_sqlite(table)))
                .fetch_all(&mut *tx)
                .await
                .map_err(|e| FaucetError::Sink(format!("cleanup: table_info query failed: {e}")))?
                .iter()
                .map(|row| (row.get::<String, _>("name"), row.get::<String, _>("type")))
                .collect();

        let scope_cols: Vec<String> = scope.keys().cloned().collect();
        let existing: std::collections::HashSet<String> = declared.keys().cloned().collect();
        validate_cleanup_columns(&existing, &scope_cols, key, table)?;

        // A previous cleanup that failed *after* its COMMIT-less DROP could not
        // leave the table behind (SQLite rolls DDL back), but the pooled
        // connection is shared, so drop defensively before creating.
        let keys_ref = cleanup_keys_ref();
        sqlx::query(&format!("DROP TABLE IF EXISTS {keys_ref}"))
            .execute(&mut *tx)
            .await
            .map_err(|e| FaucetError::Sink(format!("cleanup: temp table drop failed: {e}")))?;

        let key_types: Vec<(String, String)> = key
            .iter()
            .map(|k| (k.clone(), declared.get(k).cloned().unwrap_or_default()))
            .collect();
        sqlx::query(&build_cleanup_temp_table_sql(&key_types))
            .execute(&mut *tx)
            .await
            .map_err(|e| FaucetError::Sink(format!("cleanup: temp table creation failed: {e}")))?;

        // Load the written keys, chunked at SQLite's bind-variable cap.
        const MAX_SQLITE_VARS: usize = 32766;
        let per = (MAX_SQLITE_VARS / key.len()).max(1);
        for chunk in seen.keys().chunks(per) {
            let sql = build_cleanup_insert_sql(key, chunk.len());
            let mut q = sqlx::query(&sql);
            for kt in chunk {
                for (_, v) in &kt.0 {
                    q = bind_value(q, v);
                }
            }
            q.execute(&mut *tx)
                .await
                .map_err(|e| FaucetError::Sink(format!("cleanup: loading keys failed: {e}")))?;
        }

        // DELETE everything in scope that isn't in the written-key set.
        let sql = build_cleanup_delete_sql(table, &scope_cols, key);
        let mut q = sqlx::query(&sql);
        for v in scope.values() {
            q = bind_value(q, v);
        }
        let res = q
            .execute(&mut *tx)
            .await
            .map_err(|e| FaucetError::Sink(format!("cleanup: delete failed: {e}")))?;

        // Drop inside the transaction so the pooled connection goes back clean.
        sqlx::query(&format!("DROP TABLE IF EXISTS {keys_ref}"))
            .execute(&mut *tx)
            .await
            .map_err(|e| FaucetError::Sink(format!("cleanup: temp table drop failed: {e}")))?;

        tx.commit()
            .await
            .map_err(|e| FaucetError::Sink(format!("cleanup: commit failed: {e}")))?;
        Ok(res.rows_affected())
    }

    /// Ensure the commit-token watermark table exists.
    async fn ensure_commit_table(&self) -> Result<(), FaucetError> {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {t} ({s} TEXT PRIMARY KEY, {k} TEXT NOT NULL, updated_at TEXT DEFAULT (datetime('now')))",
            t = quote_ident(faucet_core::idempotency::COMMIT_TOKEN_TABLE),
            s = quote_ident(faucet_core::idempotency::COMMIT_TOKEN_SCOPE_COL),
            k = quote_ident(faucet_core::idempotency::COMMIT_TOKEN_TOKEN_COL),
        );
        sqlx::query(&sql)
            .execute(&self.pool)
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite commit-table create failed: {e}")))?;
        Ok(())
    }
}

#[async_trait]
impl faucet_core::Sink for SqliteSink {
    fn connector_name(&self) -> &'static str {
        "sqlite"
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(SqliteSinkConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        let path = self
            .config
            .database_url
            .trim_start_matches("sqlite://")
            .trim_start_matches("sqlite:");
        format!("sqlite://{}?table={}", path, self.config.table_name)
    }

    /// Preflight connectivity probe (`faucet doctor`).
    ///
    /// Acquires a connection from the existing pool and runs `SELECT 1`. This
    /// is non-mutating and idempotent — it validates that the database file /
    /// connection opens without writing anything.
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};

        let started = std::time::Instant::now();
        let probe =
            match tokio::time::timeout(ctx.timeout, sqlx::query("SELECT 1").execute(&self.pool))
                .await
            {
                Ok(Ok(_)) => Probe::pass("auth", started.elapsed()),
                Ok(Err(e)) => Probe::fail_hint(
                    "auth",
                    started.elapsed(),
                    e.to_string(),
                    "check database_url / that the database file is reachable and openable",
                ),
                Err(_) => Probe::fail_hint(
                    "auth",
                    started.elapsed(),
                    "timed out",
                    "check database_url / that the database file is reachable and openable",
                ),
            };
        Ok(CheckReport::single(probe))
    }

    fn supports_cleanup(&self) -> bool {
        // Column-mapping mode only: the scope + key predicates address real
        // columns, which a single JSON payload column does not have.
        matches!(self.config.column_mapping, SqliteColumnMapping::AutoMap)
    }

    async fn cleanup_scope(
        &self,
        scope: &std::collections::BTreeMap<String, Value>,
        seen: &faucet_core::SeenKeys,
    ) -> Result<u64, FaucetError> {
        self.cleanup_scope_impl(scope, seen).await
    }

    fn supported_write_modes(&self) -> &'static [faucet_core::WriteMode] {
        &[
            faucet_core::WriteMode::Append,
            faucet_core::WriteMode::Upsert,
            faucet_core::WriteMode::Delete,
            faucet_core::WriteMode::Overwrite,
        ]
    }

    fn dedups_by_key(&self) -> bool {
        self.config.write.dedups_by_key()
    }

    fn is_overwrite(&self) -> bool {
        self.config.write.is_overwrite()
    }

    /// Create the staging table as an empty clone of the target's shape
    /// (`CREATE TABLE staging AS SELECT * FROM target WHERE 0`), dropping any
    /// leftover staging table from a previously-crashed run first. The target
    /// table must already exist (the sink never auto-creates it) — an overwrite
    /// replaces its rows, not its definition.
    async fn begin_overwrite(&self) -> Result<(), FaucetError> {
        let staging = quote_ident(&self.staging_table());
        let target = quote_ident(&self.config.table_name);
        sqlx::query(&format!("DROP TABLE IF EXISTS {staging}"))
            .execute(&self.pool)
            .await
            .map_err(|e| FaucetError::Sink(format!("sqlite overwrite: drop stale staging: {e}")))?;
        sqlx::query(&format!(
            "CREATE TABLE {staging} AS SELECT * FROM {target} WHERE 0"
        ))
        .execute(&self.pool)
        .await
        .map_err(|e| {
            FaucetError::Sink(format!(
                "sqlite overwrite: create staging from '{}' (does the table exist?): {e}",
                self.config.table_name
            ))
        })?;
        Ok(())
    }

    /// Atomically replace the destination with the staged rows in one
    /// transaction: `DELETE FROM target; INSERT INTO target SELECT * FROM
    /// staging; DROP TABLE staging`. SQLite DDL is transactional, so a failure
    /// anywhere rolls the whole swap back and the prior rows survive.
    async fn commit_overwrite(&self) -> Result<(), FaucetError> {
        let staging = quote_ident(&self.staging_table());
        let target = quote_ident(&self.config.table_name);
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| FaucetError::Sink(format!("sqlite overwrite: begin swap: {e}")))?;
        for stmt in [
            format!("DELETE FROM {target}"),
            format!("INSERT INTO {target} SELECT * FROM {staging}"),
            format!("DROP TABLE {staging}"),
        ] {
            sqlx::query(&stmt)
                .execute(&mut *tx)
                .await
                .map_err(|e| FaucetError::Sink(format!("sqlite overwrite swap failed: {e}")))?;
        }
        tx.commit()
            .await
            .map_err(|e| FaucetError::Sink(format!("sqlite overwrite: commit swap: {e}")))?;
        Ok(())
    }

    /// Drop the staging table so a failed/cancelled overwrite leaves nothing
    /// behind. Best-effort — the destination was never touched.
    async fn abort_overwrite(&self) -> Result<(), FaucetError> {
        sqlx::query(&format!(
            "DROP TABLE IF EXISTS {}",
            quote_ident(&self.staging_table())
        ))
        .execute(&self.pool)
        .await
        .map_err(|e| FaucetError::Sink(format!("sqlite overwrite: drop staging: {e}")))?;
        Ok(())
    }

    fn supports_schema_evolution(&self) -> bool {
        true
    }

    /// Read the live destination schema via `PRAGMA table_info`, shaped as an
    /// `infer_schema`-compatible object (`{"type":"object","properties":{…}}`),
    /// or `None` when the target table does not exist yet (issue #194).
    ///
    /// `PRAGMA table_info` returns one row per column with `name`, `type` (the
    /// declared affinity string), and `notnull`. The affinity string is mapped
    /// to a JSON-Schema base type via `sqlite_affinity_to_json_schema`, and
    /// `notnull == 0` surfaces the column as nullable. The PRAGMA runs on a
    /// connection acquired from the pool (a standalone read — not inside an open
    /// transaction).
    async fn current_schema(&self) -> Result<Option<serde_json::Value>, FaucetError> {
        let rows = sqlx::query(&format!(
            "PRAGMA table_info({})",
            quote_ident(&self.config.table_name)
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| FaucetError::Sink(format!("sqlite current_schema query failed: {e}")))?;

        if rows.is_empty() {
            return Ok(None); // table does not exist yet (or has no columns)
        }

        let mut props = serde_json::Map::new();
        for row in &rows {
            let name: String = row.get("name");
            let declared: String = row.get("type");
            let notnull: i64 = row.get("notnull");
            props.insert(
                name,
                sqlite_affinity_to_json_schema(&declared, notnull == 0),
            );
        }
        Ok(Some(
            serde_json::json!({ "type": "object", "properties": props }),
        ))
    }

    /// Apply an additive schema evolution to the destination table (issue #194).
    ///
    /// - **Additions** — `ALTER TABLE … ADD COLUMN`. SQLite has no
    ///   `ADD COLUMN IF NOT EXISTS`, so the current columns are read first and a
    ///   column already present is silently skipped (idempotency by pre-check).
    /// - **Widenings** — a no-op under SQLite's dynamic typing: a column already
    ///   accepts a value of any type, so there is nothing to ALTER. Logged once
    ///   at `debug`.
    /// - **Nullability relaxations** — a no-op: SQLite cannot drop a `NOT NULL`
    ///   constraint in place (it requires a full table rebuild, which is out of
    ///   scope here). Logged once at `debug`; the column is left as-is.
    async fn evolve_schema(&self, evolution: &SchemaEvolution) -> Result<(), FaucetError> {
        // Read the current column set so additions are idempotent (no
        // `ADD COLUMN IF NOT EXISTS` in SQLite).
        let existing: std::collections::HashSet<String> = sqlx::query(&format!(
            "PRAGMA table_info({})",
            quote_ident(&self.config.table_name)
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| FaucetError::Sink(format!("sqlite evolve table_info failed: {e}")))?
        .iter()
        .map(|row| row.get::<String, _>("name"))
        .collect();

        for c in &evolution.additions {
            if existing.contains(&c.name) {
                continue; // already present — ADD COLUMN would error
            }
            let t = json_schema_base_type(&c.to).unwrap_or(SqlBaseType::Text);
            sqlx::query(&build_add_column_sql(&self.config.table_name, &c.name, t))
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    FaucetError::Sink(format!("sqlite ADD COLUMN {} failed: {e}", c.name))
                })?;
        }

        if !evolution.widenings.is_empty() {
            tracing::debug!("sqlite: type widening is a no-op under dynamic typing");
        }
        for col in &evolution.relax_nullability {
            tracing::debug!("sqlite cannot relax NOT NULL in place; column {col} left as-is");
        }

        Ok(())
    }

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        self.ensure_table_ready(records).await?;

        // Upsert/delete modes: plan the writes and apply atomically. Append and
        // overwrite are insert-shaped and fall through (overwrite lands in the
        // staging table via `effective_table`).
        if matches!(
            self.config.write.write_mode,
            faucet_core::WriteMode::Upsert | faucet_core::WriteMode::Delete
        ) {
            let plan = faucet_core::plan_writes(records, &self.config.write);
            if let Some((idx, msg)) = plan.failed.first() {
                return Err(FaucetError::Sink(format!(
                    "sqlite {}: row {idx}: {msg}",
                    self.config.write.write_mode.as_str()
                )));
            }
            return self.apply_plan(&plan).await;
        }

        // `batch_size = 0` is the "no batching" sentinel: write the entire
        // upstream slice as a single multi-row INSERT inside one
        // `BEGIN`/`COMMIT` transaction, preserving `StreamPage` framing.
        // Otherwise re-chunk into `batch_size` slices so each transaction
        // stays near SQLite's sweet spot (~1000 rows per multi-row INSERT).
        let effective_chunk = if self.config.batch_size == 0 {
            records.len()
        } else {
            self.config.batch_size
        };

        let mut total = 0;
        for chunk in records.chunks(effective_chunk) {
            total += match &self.config.column_mapping {
                SqliteColumnMapping::Json { column } => self.insert_json(chunk, column).await?,
                SqliteColumnMapping::AutoMap => self.insert_auto_map(chunk).await?,
            };
        }

        tracing::info!(
            table = %self.config.table_name,
            rows = total,
            "SQLite write complete"
        );
        Ok(total)
    }

    /// Write a batch and report per-row outcomes.
    ///
    /// In append mode this delegates to [`write_batch`](faucet_core::Sink::write_batch) and
    /// maps a single success onto an all-`Ok(())` vector (the trait default).
    /// In upsert/delete mode the good rows are applied (upserts + deletes), and
    /// only the rows whose key could not be extracted (missing / null key) are
    /// reported as `Err` so the pipeline routes them to the DLQ per-row instead
    /// of sending the whole page.
    async fn write_batch_partial(
        &self,
        records: &[Value],
    ) -> Result<Vec<faucet_core::RowOutcome>, FaucetError> {
        if !matches!(
            self.config.write.write_mode,
            faucet_core::WriteMode::Upsert | faucet_core::WriteMode::Delete
        ) {
            // Append and overwrite: insert-shaped, no per-row key failures.
            self.write_batch(records).await?;
            return Ok(records.iter().map(|_| Ok(())).collect());
        }

        let plan = faucet_core::plan_writes(records, &self.config.write);
        self.apply_plan(&plan).await?;

        let mut outcomes: Vec<faucet_core::RowOutcome> = records.iter().map(|_| Ok(())).collect();
        for (idx, msg) in &plan.failed {
            outcomes[*idx] = Err(FaucetError::Sink(format!(
                "sqlite {}: {msg}",
                self.config.write.write_mode.as_str()
            )));
        }
        Ok(outcomes)
    }

    fn supports_idempotent_writes(&self) -> bool {
        true
    }

    async fn last_committed_token(&self, scope: &str) -> Result<Option<String>, FaucetError> {
        self.ensure_commit_table().await?;
        let sql = format!(
            "SELECT {k} FROM {t} WHERE {s} = ?",
            t = quote_ident(faucet_core::idempotency::COMMIT_TOKEN_TABLE),
            k = quote_ident(faucet_core::idempotency::COMMIT_TOKEN_TOKEN_COL),
            s = quote_ident(faucet_core::idempotency::COMMIT_TOKEN_SCOPE_COL),
        );
        let row = sqlx::query(&sql)
            .bind(scope)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite token read failed: {e}")))?;
        Ok(row.map(|r| r.get::<String, _>(0)))
    }

    async fn write_batch_idempotent(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<usize, FaucetError> {
        self.ensure_commit_table().await?;

        // For upsert/delete modes, plan the page before opening the transaction
        // so a key-extraction failure aborts without leaving an open tx.
        let plan = if matches!(self.config.write.write_mode, faucet_core::WriteMode::Append) {
            None
        } else {
            let plan = faucet_core::plan_writes(records, &self.config.write);
            if let Some((idx, msg)) = plan.failed.first() {
                return Err(FaucetError::Sink(format!(
                    "sqlite {}: row {idx}: {msg}",
                    self.config.write.write_mode.as_str()
                )));
            }
            Some(plan)
        };

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite transaction begin failed: {e}")))?;

        // Data write and the commit-token upsert share ONE transaction so the
        // page is committed atomically with its watermark: on crash either both
        // land or neither does, which is what makes a replay skip-on-resume
        // produce zero duplicates. For upsert/delete the planned upserts/deletes
        // commit together with the watermark in this same tx (no nested tx —
        // we reuse `apply_plan`'s helpers directly on this transaction).
        let written = match &plan {
            Some(plan) => {
                let mut affected = 0usize;
                if !plan.upserts.is_empty() {
                    affected += self
                        .insert_auto_map_with_conflict_tx(
                            &mut tx,
                            &plan.upserts,
                            Some(&self.config.write.key),
                        )
                        .await?;
                }
                if !plan.deletes.is_empty() {
                    affected += self.delete_by_keys(&mut tx, &plan.deletes).await?;
                }
                affected
            }
            None => match &self.config.column_mapping {
                SqliteColumnMapping::Json { column } => {
                    self.insert_json_tx(&mut tx, records, column).await?
                }
                SqliteColumnMapping::AutoMap => self.insert_auto_map_tx(&mut tx, records).await?,
            },
        };

        let upsert = format!(
            "INSERT INTO {t} ({s}, {k}) VALUES (?, ?) ON CONFLICT({s}) DO UPDATE SET {k} = excluded.{k}, updated_at = datetime('now')",
            t = quote_ident(faucet_core::idempotency::COMMIT_TOKEN_TABLE),
            s = quote_ident(faucet_core::idempotency::COMMIT_TOKEN_SCOPE_COL),
            k = quote_ident(faucet_core::idempotency::COMMIT_TOKEN_TOKEN_COL),
        );
        sqlx::query(&upsert)
            .bind(scope)
            .bind(token)
            .execute(&mut *tx)
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite token upsert failed: {e}")))?;

        tx.commit()
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite transaction commit failed: {e}")))?;
        Ok(written)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SqliteSinkConfig;
    use faucet_core::Sink as _;

    #[tokio::test]
    async fn dataset_uri_strips_sqlite_prefix_and_includes_table() {
        let config = SqliteSinkConfig::new("sqlite:///tmp/test.db", "events");
        let sink = SqliteSink::new(config).await.unwrap();
        assert_eq!(sink.dataset_uri(), "sqlite:///tmp/test.db?table=events");
    }

    #[tokio::test]
    async fn dataset_uri_with_memory_db() {
        let config = SqliteSinkConfig::new("sqlite::memory:", "logs");
        let sink = SqliteSink::new(config).await.unwrap();
        assert_eq!(sink.dataset_uri(), "sqlite://:memory:?table=logs");
    }

    #[test]
    fn sqlite_on_conflict_clause() {
        let clause =
            on_conflict_clause(&["id".to_string()], &["id".to_string(), "name".to_string()]);
        assert_eq!(
            clause,
            r#"ON CONFLICT("id") DO UPDATE SET "name" = excluded."name""#
        );
    }

    #[test]
    fn sqlite_on_conflict_all_keys_does_nothing() {
        let clause = on_conflict_clause(&["id".to_string()], &["id".to_string()]);
        assert_eq!(clause, r#"ON CONFLICT("id") DO NOTHING"#);
    }

    #[test]
    fn sqlite_on_conflict_composite_key() {
        let clause = on_conflict_clause(
            &["a".to_string(), "b".to_string()],
            &["a".to_string(), "b".to_string(), "v".to_string()],
        );
        assert_eq!(
            clause,
            r#"ON CONFLICT("a", "b") DO UPDATE SET "v" = excluded."v""#
        );
    }

    #[test]
    fn sqlite_add_column_ddl() {
        assert_eq!(
            build_add_column_sql("t", "email", SqlBaseType::Text),
            r#"ALTER TABLE "t" ADD COLUMN "email" TEXT"#
        );
        assert_eq!(
            build_add_column_sql("t", "age", SqlBaseType::Integer),
            r#"ALTER TABLE "t" ADD COLUMN "age" INTEGER"#
        );
        assert_eq!(
            build_add_column_sql("t", "score", SqlBaseType::Double),
            r#"ALTER TABLE "t" ADD COLUMN "score" REAL"#
        );
        // Boolean has no native SQLite type → INTEGER affinity; JSON → TEXT.
        assert_eq!(
            build_add_column_sql("t", "ok", SqlBaseType::Boolean),
            r#"ALTER TABLE "t" ADD COLUMN "ok" INTEGER"#
        );
        assert_eq!(
            build_add_column_sql("t", "meta", SqlBaseType::Json),
            r#"ALTER TABLE "t" ADD COLUMN "meta" TEXT"#
        );
    }

    #[test]
    fn sqlite_keyword_mapping() {
        assert_eq!(sqlite_keyword(SqlBaseType::Integer), "INTEGER");
        assert_eq!(sqlite_keyword(SqlBaseType::Double), "REAL");
        assert_eq!(sqlite_keyword(SqlBaseType::Boolean), "INTEGER");
        assert_eq!(sqlite_keyword(SqlBaseType::Text), "TEXT");
        assert_eq!(sqlite_keyword(SqlBaseType::Json), "TEXT");
    }

    // ---------------------------------------------------------------------
    // Scoped cleanup (#478) — SQL generation and column validation
    // ---------------------------------------------------------------------

    fn cols(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn cleanup_quotes_identifiers_with_backticks() {
        // Not double quotes: SQLite's double-quoted-string misfeature would turn
        // a typo'd column into a string literal instead of an error.
        assert_eq!(quote_ident_sqlite("id"), "`id`");
        assert_eq!(quote_ident_sqlite("ev`il"), "`ev``il`");
    }

    #[test]
    fn cleanup_temp_table_mirrors_declared_types() {
        let sql = build_cleanup_temp_table_sql(&[
            ("id".to_string(), "INTEGER".to_string()),
            ("slug".to_string(), "VARCHAR(255)".to_string()),
        ]);
        assert_eq!(
            sql,
            "CREATE TEMP TABLE temp.`faucet_cleanup_keys` (`id` INTEGER, `slug` VARCHAR(255))"
        );
    }

    #[test]
    fn cleanup_temp_table_omits_an_unusable_type() {
        // A typeless column is legal in SQLite; it only loses type affinity.
        let sql = build_cleanup_temp_table_sql(&[("id".to_string(), String::new())]);
        assert_eq!(sql, "CREATE TEMP TABLE temp.`faucet_cleanup_keys` (`id`)");
        // A declared type that isn't type-spec-shaped is dropped rather than
        // pasted into DDL.
        let sql = build_cleanup_temp_table_sql(&[("id".to_string(), "INT); DROP".to_string())]);
        assert_eq!(sql, "CREATE TEMP TABLE temp.`faucet_cleanup_keys` (`id`)");
    }

    #[test]
    fn safe_type_spec_accepts_real_types_and_rejects_the_rest() {
        assert_eq!(safe_type_spec("DOUBLE PRECISION"), Some("DOUBLE PRECISION"));
        assert_eq!(safe_type_spec("DECIMAL(10, 2)"), Some("DECIMAL(10, 2)"));
        assert_eq!(safe_type_spec("  TEXT  "), Some("TEXT"));
        assert_eq!(safe_type_spec(""), None);
        assert_eq!(safe_type_spec("   "), None);
        assert_eq!(safe_type_spec("TEXT`"), None);
        assert_eq!(safe_type_spec("TEXT'"), None);
    }

    #[test]
    fn cleanup_insert_emits_one_tuple_per_row() {
        let sql = build_cleanup_insert_sql(&cols(&["a", "b"]), 3);
        assert_eq!(
            sql,
            "INSERT INTO temp.`faucet_cleanup_keys` (`a`, `b`) VALUES (?, ?), (?, ?), (?, ?)"
        );
    }

    #[test]
    fn cleanup_delete_ands_the_scope_and_excludes_written_keys() {
        let sql = build_cleanup_delete_sql("assoc", &cols(&["contact_id"]), &cols(&["id"]));
        assert_eq!(
            sql,
            "DELETE FROM `assoc` WHERE `assoc`.`contact_id` = ? \
             AND NOT EXISTS (SELECT 1 FROM temp.`faucet_cleanup_keys` c \
             WHERE c.`id` = `assoc`.`id`)"
        );
    }

    #[test]
    fn cleanup_delete_composite_scope_and_key() {
        let sql =
            build_cleanup_delete_sql("t", &cols(&["tenant", "contact_id"]), &cols(&["a", "b"]));
        assert_eq!(
            sql,
            "DELETE FROM `t` WHERE `t`.`tenant` = ? AND `t`.`contact_id` = ? \
             AND NOT EXISTS (SELECT 1 FROM temp.`faucet_cleanup_keys` c \
             WHERE c.`a` = `t`.`a` AND c.`b` = `t`.`b`)"
        );
    }

    #[test]
    fn cleanup_validation_names_a_missing_scope_column() {
        let existing: std::collections::HashSet<String> =
            cols(&["id", "name"]).into_iter().collect();
        let err = validate_cleanup_columns(&existing, &cols(&["contact_id"]), &cols(&["id"]), "t")
            .expect_err("unknown scope column must be refused");
        let msg = err.to_string();
        assert!(msg.contains("contact_id"), "{msg}");
        assert!(msg.contains("'t'"), "{msg}");
    }

    #[test]
    fn cleanup_validation_names_a_missing_key_column() {
        let existing: std::collections::HashSet<String> =
            cols(&["contact_id"]).into_iter().collect();
        let err = validate_cleanup_columns(&existing, &cols(&["contact_id"]), &cols(&["id"]), "t")
            .expect_err("unknown key column must be refused");
        assert!(err.to_string().contains("id"), "{err}");
    }

    #[test]
    fn cleanup_validation_passes_when_every_column_exists() {
        let existing: std::collections::HashSet<String> =
            cols(&["id", "contact_id"]).into_iter().collect();
        assert!(
            validate_cleanup_columns(&existing, &cols(&["contact_id"]), &cols(&["id"]), "t")
                .is_ok()
        );
    }

    #[tokio::test]
    async fn supports_cleanup_only_in_auto_map_mode() {
        let config = SqliteSinkConfig::new("sqlite::memory:", "t")
            .column_mapping(SqliteColumnMapping::AutoMap);
        let sink = SqliteSink::new(config).await.unwrap();
        assert!(sink.supports_cleanup());

        // The default mapping is a single JSON payload column — no real columns
        // for the scope/key predicates to address.
        let config = SqliteSinkConfig::new("sqlite::memory:", "t");
        let sink = SqliteSink::new(config).await.unwrap();
        assert!(!sink.supports_cleanup());
    }

    #[test]
    fn sqlite_affinity_round_trips_to_json_schema() {
        use serde_json::json;
        // Tolerant case-insensitive substring matching, SQLite affinity rules.
        assert_eq!(
            sqlite_affinity_to_json_schema("INTEGER", false),
            json!({"type":"integer"})
        );
        assert_eq!(
            sqlite_affinity_to_json_schema("BIGINT", false),
            json!({"type":"integer"})
        );
        assert_eq!(
            sqlite_affinity_to_json_schema("REAL", false),
            json!({"type":"number"})
        );
        assert_eq!(
            sqlite_affinity_to_json_schema("DOUBLE PRECISION", false),
            json!({"type":"number"})
        );
        assert_eq!(
            sqlite_affinity_to_json_schema("DECIMAL(10,2)", false),
            json!({"type":"number"})
        );
        assert_eq!(
            sqlite_affinity_to_json_schema("TEXT", false),
            json!({"type":"string"})
        );
        assert_eq!(
            sqlite_affinity_to_json_schema("VARCHAR(255)", false),
            json!({"type":"string"})
        );
        // Unknown / empty affinity falls back to string.
        assert_eq!(
            sqlite_affinity_to_json_schema("BLOB", false),
            json!({"type":"string"})
        );
        assert_eq!(
            sqlite_affinity_to_json_schema("", false),
            json!({"type":"string"})
        );
        // Nullable columns widen the type array.
        assert_eq!(
            sqlite_affinity_to_json_schema("integer", true),
            json!({"type":["integer","null"]})
        );
    }
}
