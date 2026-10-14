//! DuckDB sink implementation — the one module that performs I/O.
//!
//! `duckdb` is synchronous, so every write runs inside
//! [`tokio::task::spawn_blocking`]. Each `write_batch` is applied as one
//! `BEGIN`/`COMMIT` transaction of `batch_size`-row multi-row `INSERT`s
//! (rolled back on error). The sink is append-only; keyed upsert and an
//! Arrow-native columnar fast path are tracked as follow-ups.

use crate::config::{DuckdbColumnMapping, DuckdbSinkConfig};
use async_trait::async_trait;
use duckdb::types::Value as DuckValue;
use duckdb::{AccessMode, Config, Connection};
use faucet_common_sql::dialects::Duckdb;
use faucet_common_sql::{SqlDialect, SqlSinkEngine, Statement, TableColumn, TableTarget};
use faucet_core::driver::{DriverSink, PageWrite, RowPolicy, SinkDriver};
use faucet_core::{CallSlot, FaucetError};

use serde_json::Value;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

/// Quote a possibly schema-qualified table name, one segment at a time, so
/// `analytics.events` becomes `"analytics"."events"` rather than the single
/// identifier `"analytics.events"` (which names a table with a dot in it and can
/// never resolve). Mirrors the ClickHouse sink's `quote_table` (#456 L3).
fn quote_table(table: &str) -> String {
    faucet_common_sql::dialects::duckdb::quote_table(table)
}

/// The engine target of `table`.
fn target(table: &str) -> TableTarget {
    TableTarget::new(table, quote_table(table))
}

/// The shared SQL planning engine (#884) for an append-only DuckDB table.
fn engine() -> SqlSinkEngine<Duckdb> {
    SqlSinkEngine::new(Duckdb, faucet_core::WriteSpec::default())
}

/// The DuckDB type keyword for a [`faucet_core::SqlBaseType`] (#580): integers
/// land as `BIGINT` and floats as `DOUBLE` so a later, wider value never
/// overflows a narrower column; nested values land as JSON text.
#[cfg(test)]
fn duckdb_keyword(t: faucet_core::SqlBaseType) -> &'static str {
    Duckdb.type_name(t)
}

/// `CREATE TABLE IF NOT EXISTS` for an auto-created target (#580). JSON mode
/// stores the whole record in one column, so the page's own shape is
/// irrelevant there.
fn build_create_table_sql(
    table: &str,
    columns: &[faucet_core::PlannedColumn],
    json_column: Option<&str>,
) -> Result<String, FaucetError> {
    match json_column {
        Some(col) => Duckdb.create_json_table_sql(&target(table), col),
        None => Duckdb.create_table_sql(&target(table), columns, &[]),
    }
}

/// The bare table name of a possibly schema-qualified target, plus its schema —
/// `information_schema.columns` stores the two separately.
fn split_table(table: &str) -> (Option<&str>, &str) {
    match table.rsplit_once('.') {
        Some((schema, name)) => (Some(schema), name),
        None => (None, table),
    }
}

/// `(catalog, schema, name)` of a target written as `name`, `schema.name` or
/// `catalog.schema.name`. A missing part means the connection's current
/// database / schema, which is where an unqualified INSERT lands.
fn split_qualified(table: &str) -> (Option<&str>, Option<&str>, &str) {
    let (rest, name) = split_table(table);
    match rest {
        None => (None, None, name),
        Some(rest) => match rest.rsplit_once('.') {
            Some((catalog, schema)) => (Some(catalog), Some(schema), name),
            None => (None, Some(rest), name),
        },
    }
}

fn opt_param(v: Option<&str>) -> DuckValue {
    v.map_or(DuckValue::Null, |s| DuckValue::Text(s.to_string()))
}

/// The target's columns in declared order, read from the one table an INSERT
/// into `table` resolves to — never a same-named table or view in another
/// schema or attached database.
///
/// Also returns the tz-less timestamp columns, whose offset-bearing ISO
/// strings are converted to UTC before binding.
fn table_columns(
    conn: &Connection,
    table: &str,
) -> Result<(Vec<String>, HashSet<String>), FaucetError> {
    let (catalog, schema, name) = split_qualified(table);
    let mut stmt = conn
        .prepare(
            "SELECT column_name, data_type FROM information_schema.columns \
             WHERE table_catalog = coalesce(?, current_database()) \
               AND table_schema = coalesce(?, current_schema()) \
               AND table_name = ? \
             ORDER BY ordinal_position",
        )
        .map_err(|e| FaucetError::Sink(format!("failed to query table columns: {e}")))?;
    let rows = stmt
        .query_map(
            duckdb::params_from_iter([
                opt_param(catalog),
                opt_param(schema),
                DuckValue::Text(name.to_string()),
            ]),
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .map_err(|e| FaucetError::Sink(format!("failed to query table columns: {e}")))?
        .collect::<Result<Vec<(String, String)>, _>>()
        .map_err(|e| FaucetError::Sink(format!("failed to decode table columns: {e}")))?;
    let naive_ts = rows
        .iter()
        .filter(|(_, ty)| is_naive_timestamp(ty))
        .map(|(c, _)| c.clone())
        .collect();
    Ok((rows.into_iter().map(|(c, _)| c).collect(), naive_ts))
}

/// Whether the (possibly qualified) target table exists.
fn table_exists(conn: &Connection, table: &str) -> Result<bool, FaucetError> {
    let (catalog, schema, name) = split_qualified(table);
    let found: i64 = conn
        .query_row(
            "SELECT count(*) FROM duckdb_tables() \
             WHERE database_name = coalesce(?, current_database()) \
               AND schema_name = coalesce(?, current_schema()) \
               AND table_name = ?",
            duckdb::params_from_iter([
                opt_param(catalog),
                opt_param(schema),
                DuckValue::Text(name.to_string()),
            ]),
            |r| r.get(0),
        )
        .map_err(|e| FaucetError::Sink(format!("duckdb table probe failed: {e}")))?;
    Ok(found > 0)
}

/// The error for a record that matches no column of the target.
fn no_matching_column_message(
    idx: usize,
    obj: &serde_json::Map<String, Value>,
    columns: &[String],
) -> String {
    let columns: Vec<TableColumn> = columns.iter().map(TableColumn::untyped).collect();
    faucet_common_sql::no_match_message(&Duckdb, idx, obj, &columns, "")
}

/// The error for fields the target has no column for (`create_table` off).
fn missing_columns_message(table: &str, missing: &[String]) -> String {
    format!(
        "duckdb: table '{table}' has no column for field(s) {}; add them, drop the \
         fields with a transform, or set `create_table: true`",
        missing.join(", ")
    )
}

/// A sink that writes JSON records to a DuckDB table.
///
/// Built on the core sink driver (RFC 0014): `DuckdbDriver` holds the
/// blocking DuckDB I/O, and a [`DriverSink`] serves the plain, reported and
/// per-row write paths from it.
pub struct DuckdbSink {
    /// The connection (shared with the driver), for setup and inspection.
    pub(crate) conn: Arc<Mutex<Connection>>,
    inner: DriverSink<DuckdbDriver>,
}

/// The DuckDB I/O the sink driver runs on.
pub(crate) struct DuckdbDriver {
    config: DuckdbSinkConfig,
    conn: Arc<Mutex<Connection>>,
    /// The process-wide database instance this sink's connection belongs to,
    /// kept alive for as long as the sink is.
    _instance: Option<SharedInstance>,
    /// Whether the target has been confirmed present for this sink instance
    /// (#580). One check per run, not per page.
    table_ready: std::sync::atomic::AtomicBool,
    /// Interrupts a write the call context cuts short, so the blocking task
    /// stops (and its transaction rolls back) instead of running on.
    interrupt: Arc<duckdb::InterruptHandle>,
}

/// One DuckDB database instance per file in this process (#789 SQL-07).
///
/// Every `Connection::open` creates a separate instance, and DuckDB's file
/// lock never conflicts inside one process, so two sinks on one file used to
/// write through two instances; whichever closed last checkpointed its own
/// view and the other's committed data disappeared. Sinks on the same file now
/// take `try_clone()`d connections of one shared instance.
static INSTANCES: std::sync::LazyLock<
    Mutex<std::collections::HashMap<std::path::PathBuf, std::sync::Weak<Mutex<Connection>>>>,
> = std::sync::LazyLock::new(|| Mutex::new(std::collections::HashMap::new()));

/// The identity of a database file, stable whether or not it exists yet.
fn instance_key(path: &str) -> std::path::PathBuf {
    let p = std::path::Path::new(path);
    if let Ok(c) = std::fs::canonicalize(p) {
        return c;
    }
    let parent = p
        .parent()
        .filter(|d| !d.as_os_str().is_empty())
        .unwrap_or(std::path::Path::new("."));
    match (std::fs::canonicalize(parent), p.file_name()) {
        (Ok(dir), Some(name)) => dir.join(name),
        _ => p.to_path_buf(),
    }
}

/// A connection to `path`, sharing the process's instance for that file.
type SharedInstance = Arc<Mutex<Connection>>;

fn connect(path: &str) -> Result<(Connection, Option<SharedInstance>), FaucetError> {
    if path == ":memory:" {
        return Ok((open(path)?, None));
    }
    let key = instance_key(path);
    let mut instances = INSTANCES
        .lock()
        .map_err(|_| FaucetError::Sink("duckdb instance registry poisoned".into()))?;
    let instance = match instances.get(&key).and_then(std::sync::Weak::upgrade) {
        Some(existing) => existing,
        None => {
            let fresh = Arc::new(Mutex::new(open(path)?));
            instances.insert(key, Arc::downgrade(&fresh));
            fresh
        }
    };
    instances.retain(|_, w| w.strong_count() > 0);
    let conn = instance
        .lock()
        .map_err(|_| FaucetError::Sink("duckdb instance mutex poisoned".into()))?
        .try_clone()
        .map_err(|e| FaucetError::Sink(format!("DuckDB connect failed ({path}): {e}")))?;
    Ok((conn, Some(instance)))
}

fn open(path: &str) -> Result<Connection, FaucetError> {
    let flags = Config::default()
        .access_mode(AccessMode::ReadWrite)
        .map_err(|e| FaucetError::Config(format!("duckdb config: {e}")))?;
    let conn = if path == ":memory:" {
        Connection::open_in_memory_with_flags(flags)
    } else {
        Connection::open_with_flags(path, flags)
    };
    conn.map_err(|e| FaucetError::Sink(format!("DuckDB open failed ({path}): {e}")))
}

/// Lock the database instance down to its own storage (#862 H-04).
fn sandbox(conn: &Connection) -> Result<(), FaucetError> {
    let setting = |name: &str| -> Result<bool, FaucetError> {
        conn.query_row(
            &format!("SELECT current_setting('{name}')::BOOLEAN"),
            [],
            |r| r.get::<_, bool>(0),
        )
        .map_err(|e| FaucetError::Config(format!("duckdb sandbox: reading {name}: {e}")))
    };
    if setting("lock_configuration")? {
        return if setting("enable_external_access")? {
            Err(FaucetError::Config(
                "duckdb sandbox: the database's configuration is locked with external access \
                 enabled"
                    .into(),
            ))
        } else {
            Ok(())
        };
    }
    conn.execute_batch(
        "SET autoinstall_known_extensions=false; SET autoload_known_extensions=false; \
         SET enable_external_access=false; SET lock_configuration=true;",
    )
    .map_err(|e| FaucetError::Config(format!("duckdb sandbox: {e}")))
}

/// Convert a JSON value into an owned DuckDB parameter value.
fn json_to_duck(v: &Value) -> DuckValue {
    match v {
        Value::Null => DuckValue::Null,
        Value::Bool(b) => DuckValue::Boolean(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                DuckValue::BigInt(i)
            } else if let Some(u) = n.as_u64() {
                DuckValue::UBigInt(u)
            } else if let Some(f) = n.as_f64() {
                DuckValue::Double(f)
            } else {
                DuckValue::Null
            }
        }
        Value::String(s) => DuckValue::Text(s.clone()),
        // Arrays/objects have no scalar SQL form — store their JSON text.
        other => DuckValue::Text(other.to_string()),
    }
}

/// Insert records as a single JSON text column via one multi-row INSERT.
fn insert_json(
    conn: &Connection,
    table: &str,
    column: &str,
    records: &[Value],
) -> Result<usize, FaucetError> {
    if records.is_empty() {
        return Ok(0);
    }
    for stmt in &engine().json_insert_statements(&target(table), column, records)? {
        execute(conn, stmt)?;
    }
    Ok(records.len())
}

/// Insert records mapping top-level JSON keys onto existing table columns.
///
/// A field the table has no column for is added (typed like an auto-created
/// column) when `create_table` is on, and fails the write otherwise — never
/// dropped. Offset-bearing ISO strings bound for a tz-less `TIMESTAMP` column
/// are converted to UTC (DuckDB's text cast would drop the offset).
fn insert_auto_map(
    conn: &Connection,
    table: &str,
    records: &[Value],
    create_table: bool,
) -> Result<usize, FaucetError> {
    if records.is_empty() {
        return Ok(0);
    }
    let (cols, naive_ts) = table_columns(conn, table)?;
    if cols.is_empty() {
        return Err(FaucetError::Sink(format!(
            "table '{table}' has no columns or does not exist"
        )));
    }
    let missing = missing_fields(records, &cols);
    if !missing.is_empty() {
        if !create_table {
            return Err(FaucetError::Sink(missing_columns_message(table, &missing)));
        }
        let planned = faucet_core::plan_columns(records).unwrap_or_default();
        for column in planned.iter().filter(|c| missing.contains(&c.name)) {
            conn.execute_batch(&add_column_sql(table, column))
                .map_err(|e| FaucetError::Sink(format!("duckdb ADD COLUMN failed: {e}")))?;
        }
        return insert_auto_map(conn, table, records, false);
    }
    insert_rows(conn, table, records, &cols, &naive_ts)
}

/// A `TIMESTAMP` type without a time zone (`TIMESTAMP`, `TIMESTAMP_NS`, …).
fn is_naive_timestamp(ty: &str) -> bool {
    let t = ty.to_ascii_uppercase();
    (t.starts_with("TIMESTAMP") || t == "DATETIME")
        && !t.contains("TIME ZONE")
        && t != "TIMESTAMPTZ"
}

/// Record fields (first-seen order) that match no column of the table, exactly
/// or ignoring ASCII case.
fn missing_fields(records: &[Value], cols: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for key in records
        .iter()
        .filter_map(Value::as_object)
        .flat_map(|o| o.keys())
    {
        if !cols.iter().any(|c| c.eq_ignore_ascii_case(key)) && !out.contains(key) {
            out.push(key.clone());
        }
    }
    out
}

/// `ALTER TABLE … ADD COLUMN IF NOT EXISTS`, typed like an auto-created column.
fn add_column_sql(table: &str, column: &faucet_core::PlannedColumn) -> String {
    Duckdb
        .add_column_sql(&target(table), &column.name, column.base_type)
        .expect("duckdb ADD COLUMN")
}

/// An ISO 8601 timestamp carrying an offset as UTC wall time; `None` otherwise.
fn utc_wall_time(s: &str) -> Option<String> {
    let dt = chrono::DateTime::parse_from_rfc3339(s.trim()).ok()?;
    Some(dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.f").to_string())
}

/// Append `records` over the table's columns: the engine maps each record
/// onto them (exactly, else ignoring ASCII case), refuses a record with no
/// matching column (skipping it would count it written while nothing landed),
/// and writes the union of carried columns — a row missing one binds NULL.
fn insert_rows(
    conn: &Connection,
    table: &str,
    records: &[Value],
    cols: &[String],
    naive_ts: &HashSet<String>,
) -> Result<usize, FaucetError> {
    // A tz-less timestamp column is marked so its offset-bearing strings are
    // converted to UTC wall time at bind.
    let columns: Vec<TableColumn> = cols
        .iter()
        .map(|c| {
            if naive_ts.contains(c) {
                TableColumn::new(c.clone(), "TIMESTAMP")
            } else {
                TableColumn::untyped(c.clone())
            }
        })
        .collect();
    let (written, statements) =
        engine().insert_statements(&target(table), &columns, records, false)?;
    for stmt in &statements {
        execute(conn, stmt)?;
    }
    Ok(written)
}

/// Bind and run one planned statement.
fn execute(conn: &Connection, stmt: &Statement<'_>) -> Result<(), FaucetError> {
    let params: Vec<DuckValue> = stmt
        .params
        .iter()
        .map(|p| match p.value.as_ref() {
            Value::String(v) if p.sql_type.is_some_and(is_naive_timestamp) => {
                DuckValue::Text(utc_wall_time(v).unwrap_or_else(|| v.clone()))
            }
            // Its shortest text, which DuckDB casts exactly into a DECIMAL or
            // DOUBLE column, where a bound double converts from binary (#885).
            _ if p.non_integer_text().is_some() => {
                DuckValue::Text(p.non_integer_text().expect("checked"))
            }
            v => json_to_duck(v),
        })
        .collect();
    let mut prepared = conn
        .prepare(&stmt.sql)
        .map_err(|e| FaucetError::Sink(format!("DuckDB prepare failed: {e}")))?;
    prepared
        .execute(duckdb::params_from_iter(params))
        .map_err(|e| FaucetError::Sink(format!("DuckDB insert failed: {e}")))?;
    Ok(())
}

/// Apply the whole batch inside one `BEGIN`/`COMMIT` transaction, re-chunking
/// into `batch_size` multi-row INSERTs. Any error rolls the transaction back.
fn write_all_blocking(
    conn: &Arc<Mutex<Connection>>,
    config: &DuckdbSinkConfig,
    records: &[Value],
) -> Result<usize, FaucetError> {
    let guard = conn
        .lock()
        .map_err(|_| FaucetError::Sink("duckdb connection mutex poisoned".into()))?;

    let chunk = if config.batch_size == 0 {
        records.len().max(1)
    } else {
        config.batch_size
    };

    guard
        .execute_batch("BEGIN TRANSACTION")
        .map_err(|e| FaucetError::Sink(format!("DuckDB begin failed: {e}")))?;

    let applied = (|| -> Result<usize, FaucetError> {
        let mut total = 0usize;
        for c in records.chunks(chunk) {
            total += match &config.column_mapping {
                DuckdbColumnMapping::Json { column } => {
                    insert_json(&guard, &config.table_name, column, c)?
                }
                DuckdbColumnMapping::AutoMap => {
                    insert_auto_map(&guard, &config.table_name, c, config.create_table)?
                }
            };
        }
        Ok(total)
    })();

    match applied {
        Ok(total) => {
            guard
                .execute_batch("COMMIT")
                .map_err(|e| FaucetError::Sink(format!("DuckDB commit failed: {e}")))?;
            Ok(total)
        }
        Err(e) => {
            let _ = guard.execute_batch("ROLLBACK");
            Err(e)
        }
    }
}

impl DuckdbDriver {
    /// Make sure the target table exists before the first write (#580).
    ///
    /// Synchronous: the DuckDB connection is already behind a `Mutex` and the
    /// DDL is one statement, so hopping to `spawn_blocking` for it would cost
    /// more than it saves.
    fn ensure_table_ready(&self, records: &[Value]) -> Result<(), FaucetError> {
        use std::sync::atomic::Ordering;
        if self.table_ready.load(Ordering::Relaxed) {
            return Ok(());
        }
        let json_column = match &self.config.column_mapping {
            DuckdbColumnMapping::AutoMap => None,
            DuckdbColumnMapping::Json { column } => Some(column.as_str()),
        };
        let guard = self
            .conn
            .lock()
            .map_err(|e| FaucetError::Sink(format!("duckdb connection mutex poisoned: {e}")))?;

        if !self.config.create_table {
            if !table_exists(&guard, &self.config.table_name)? {
                return Err(faucet_core::missing_target_error(
                    "duckdb sink",
                    &self.config.table_name,
                ));
            }
            self.table_ready.store(true, Ordering::Relaxed);
            return Ok(());
        }

        // AutoMap needs a page to infer from; a page with nothing inferable
        // leaves the table uncreated so the next page can try.
        let mut columns = match (json_column, faucet_core::plan_columns(records)) {
            (Some(_), _) => Vec::new(),
            (None, Some(c)) => c,
            (None, None) => return Ok(()),
        };
        let mut seen = HashSet::new();
        columns.retain(|c| seen.insert(c.name.to_ascii_lowercase()));
        let sql = build_create_table_sql(&self.config.table_name, &columns, json_column)?;
        guard
            .execute_batch(&sql)
            .map_err(|e| FaucetError::Sink(format!("duckdb CREATE TABLE failed: {e}")))?;
        drop(guard);
        self.table_ready.store(true, Ordering::Relaxed);
        Ok(())
    }

    /// Write the whole page in one transaction on the blocking pool, as one
    /// backend call named `write`. A call the context cuts short interrupts
    /// the connection so the blocking task stops and rolls back.
    async fn write_all(&self, calls: &CallSlot, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        self.ensure_table_ready(records)?;
        let conn = self.conn.clone();
        let config = self.config.clone();
        let owned = records.to_vec();
        let task = tokio::task::spawn_blocking(move || write_all_blocking(&conn, &config, &owned));
        let n = calls
            .call_once("write", async {
                task.await
                    .map_err(|e| FaucetError::Sink(format!("duckdb write task panicked: {e}")))?
            })
            .await
            .inspect_err(|e| {
                if matches!(
                    e,
                    FaucetError::Timeout { .. } | FaucetError::Cancelled { .. }
                ) {
                    self.interrupt.interrupt();
                }
            })?;
        tracing::info!(
            table = %self.config.table_name,
            rows = n,
            "DuckDB write complete"
        );
        Ok(n)
    }

    /// In `auto_map` mode a record that matches no column of the target fails
    /// on its own row (DLQ-routable) and the rest of the page is written.
    async fn write_partial(
        &self,
        calls: &CallSlot,
        records: &[Value],
    ) -> Result<Vec<faucet_core::RowOutcome>, FaucetError> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        if !matches!(self.config.column_mapping, DuckdbColumnMapping::AutoMap) {
            self.write_all(calls, records).await?;
            return Ok(records.iter().map(|_| Ok(())).collect());
        }
        self.ensure_table_ready(records)?;
        let cols = {
            let guard = self
                .conn
                .lock()
                .map_err(|_| FaucetError::Sink("duckdb connection mutex poisoned".into()))?;
            table_columns(&guard, &self.config.table_name)?.0
        };
        let (outcomes, writable) = split_unmatched(
            records,
            &self.config.table_name,
            &cols,
            self.config.create_table,
        );
        if !writable.is_empty() {
            self.write_all(calls, &writable).await?;
        }
        Ok(outcomes)
    }
}

impl DuckdbSink {
    /// Create a new DuckDB sink, opening (and reusing) one read-write connection.
    pub async fn new(config: DuckdbSinkConfig) -> Result<Self, FaucetError> {
        faucet_core::validate_batch_size(config.batch_size)?;
        let path = config.resolved_path().to_string();
        let confine = config.sandbox;
        let (conn, instance) = tokio::task::spawn_blocking(move || {
            let (conn, instance) = connect(&path)?;
            if confine {
                sandbox(&conn)?;
            }
            Ok::<_, FaucetError>((conn, instance))
        })
        .await
        .map_err(|e| FaucetError::Sink(format!("duckdb open task panicked: {e}")))??;
        let interrupt = conn.interrupt_handle();
        let conn = Arc::new(Mutex::new(conn));
        Ok(Self {
            conn: conn.clone(),
            inner: DriverSink::new(DuckdbDriver {
                config,
                conn,
                _instance: instance,
                table_ready: std::sync::atomic::AtomicBool::new(false),
                interrupt,
            }),
        })
    }

    /// Run an arbitrary SQL statement (e.g. DDL) on the sink's connection.
    ///
    /// Exposed for setup/introspection in tests and tooling — DuckDB permits a
    /// single read-write handle per database, so callers that need to create a
    /// table or inspect state must go through the sink's own connection.
    #[doc(hidden)]
    pub async fn run_sql(&self, sql: &str) -> Result<(), FaucetError> {
        let conn = self.conn.clone();
        let sql = sql.to_string();
        tokio::task::spawn_blocking(move || {
            let guard = conn
                .lock()
                .map_err(|_| FaucetError::Sink("duckdb connection mutex poisoned".into()))?;
            guard
                .execute_batch(&sql)
                .map_err(|e| FaucetError::Sink(format!("DuckDB statement failed: {e}")))
        })
        .await
        .map_err(|e| FaucetError::Sink(format!("duckdb task panicked: {e}")))?
    }

    /// `SELECT count(*)` over `table` on the sink's connection.
    #[doc(hidden)]
    pub async fn scalar_count(&self, table: &str) -> Result<i64, FaucetError> {
        let conn = self.conn.clone();
        let sql = format!("SELECT count(*) FROM {}", quote_table(table));
        tokio::task::spawn_blocking(move || {
            let guard = conn
                .lock()
                .map_err(|_| FaucetError::Sink("duckdb connection mutex poisoned".into()))?;
            guard
                .query_row(&sql, [], |r| r.get::<_, i64>(0))
                .map_err(|e| FaucetError::Sink(format!("DuckDB count failed: {e}")))
        })
        .await
        .map_err(|e| FaucetError::Sink(format!("duckdb task panicked: {e}")))?
    }
}

#[async_trait]
impl faucet_core::Sink for DuckdbSink {
    faucet_core::forward_sink!(inner);
}

#[async_trait]
impl SinkDriver for DuckdbDriver {
    fn connector_name(&self) -> &'static str {
        "duckdb"
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(DuckdbSinkConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        format!(
            "duckdb://{}?table={}",
            self.config.resolved_path(),
            self.config.table_name
        )
    }

    fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        self.config.batch_atomicity()
    }

    async fn write(
        &self,
        calls: &CallSlot,
        rows: &[Value],
        policy: RowPolicy,
    ) -> Result<PageWrite, FaucetError> {
        match policy {
            RowPolicy::PerRow => Ok(PageWrite::from_outcomes(
                self.write_partial(calls, rows).await?,
            )),
            _ => Ok(PageWrite::accepted(self.write_all(calls, rows).await?)),
        }
    }
}

/// Per-row outcomes plus the records to write, applying per row the rules the
/// writer applies to the whole page: a field with no column fails its row
/// unless `create_table` will add it, and a record left with no column at all
/// fails its row. Non-object records are left to the writer, which refuses
/// them.
fn split_unmatched(
    records: &[Value],
    table: &str,
    columns: &[String],
    create_table: bool,
) -> (Vec<faucet_core::RowOutcome>, Vec<Value>) {
    let mut outcomes = Vec::with_capacity(records.len());
    let mut writable = Vec::with_capacity(records.len());
    for (idx, record) in records.iter().enumerate() {
        let refusal = record.as_object().and_then(|obj| {
            let missing = missing_fields(std::slice::from_ref(record), columns);
            if !create_table && !missing.is_empty() {
                Some(missing_columns_message(table, &missing))
            } else if obj.is_empty() {
                Some(no_matching_column_message(idx, obj, columns))
            } else {
                None
            }
        });
        match refusal {
            Some(msg) => outcomes.push(Err(FaucetError::Sink(msg))),
            None => {
                outcomes.push(Ok(()));
                writable.push(record.clone());
            }
        }
    }
    (outcomes, writable)
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::Sink as _;
    use serde_json::json;

    /// A connection another holder keeps locked stands in for a hung backend
    /// (#880): the write's blocking task waits on the lock while the call ends
    /// at the deadline and on cancel.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_write_ends_at_the_call_deadline_and_on_cancel() {
        let mut built = Vec::new();
        let mut held = Vec::new();
        for _ in 0..2 {
            let sink = sink_with_table(
                "CREATE TABLE t (id BIGINT)",
                "t",
                DuckdbColumnMapping::AutoMap,
            )
            .await;
            // Confirm the table first, so the write under test goes straight
            // to its blocking task.
            sink.write_batch(&[json!({"id": 0})]).await.unwrap();
            held.push(Arc::clone(&sink.conn));
            built.push(sink);
        }
        let guards: Vec<_> = held.iter().map(|c| c.lock().unwrap()).collect();
        let built = std::sync::Mutex::new(built);
        faucet_conformance::calls::assert_sink_call_deadline(
            || built.lock().unwrap().pop().unwrap(),
            &[json!({"id": 1})],
        )
        .await;
        drop(guards);
    }

    async fn sink_with_table(ddl: &str, table: &str, mapping: DuckdbColumnMapping) -> DuckdbSink {
        let sink =
            DuckdbSink::new(DuckdbSinkConfig::new(":memory:", table).column_mapping(mapping))
                .await
                .unwrap();
        sink.conn.lock().unwrap().execute_batch(ddl).expect("ddl");
        sink
    }

    fn count(sink: &DuckdbSink, table: &str) -> i64 {
        let guard = sink.conn.lock().unwrap();
        guard
            .query_row(
                &format!("SELECT count(*) FROM {}", quote_table(table)),
                [],
                |r| r.get::<_, i64>(0),
            )
            .unwrap()
    }

    #[tokio::test]
    async fn writes_json_column() {
        let sink = sink_with_table(
            "CREATE TABLE events (data TEXT)",
            "events",
            DuckdbColumnMapping::Json {
                column: "data".into(),
            },
        )
        .await;
        let n = sink
            .write_batch(&[json!({"a": 1}), json!({"a": 2})])
            .await
            .unwrap();
        assert_eq!(n, 2);
        assert_eq!(count(&sink, "events"), 2);
        assert_eq!(sink.connector_name(), "duckdb");
        // A recorder installed mid-run meters the next write.
        sink.set_roundtrip_recorder(Arc::new(
            faucet_core::observability::RoundtripRecorder::new(
                faucet_core::observability::RoundtripSide::Sink,
                "p",
                "r",
                "duckdb",
            ),
        ));
        assert_eq!(sink.write_batch(&[json!({"a": 3})]).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn writes_auto_mapped_columns() {
        let sink = sink_with_table(
            "CREATE TABLE t (id INTEGER, name TEXT)",
            "t",
            DuckdbColumnMapping::AutoMap,
        )
        .await;
        let n = sink
            .write_batch(&[
                json!({"id": 1, "name": "a", "extra": "ignored"}),
                json!({"id": 2, "name": "b"}),
            ])
            .await
            .unwrap();
        assert_eq!(n, 2);
        assert_eq!(count(&sink, "t"), 2);
    }

    #[tokio::test]
    async fn fields_match_columns_ignoring_case() {
        let sink = sink_with_table(
            "CREATE TABLE t (id INTEGER, name TEXT)",
            "t",
            DuckdbColumnMapping::AutoMap,
        )
        .await;
        let n = sink
            .write_batch(&[json!({"ID": 1, "Name": "a"}), json!({"id": 2, "name": "b"})])
            .await
            .unwrap();
        assert_eq!(n, 2);
        let conn = sink.conn.lock().unwrap();
        let names: Vec<String> = conn
            .prepare("SELECT name FROM t ORDER BY id")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(names, vec!["a".to_string(), "b".to_string()]);
        let cols: i64 = conn
            .query_row(
                "SELECT count(*) FROM information_schema.columns WHERE table_name = 't'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(cols, 2, "no column was added for a case variant");
        assert!(missing_fields(&[json!({"ID": 1})], &["id".to_string()]).is_empty());
    }

    #[tokio::test]
    async fn a_record_matching_no_column_is_refused_not_skipped() {
        let sink = sink_with_table(
            "CREATE TABLE t (id INTEGER, name TEXT)",
            "t",
            DuckdbColumnMapping::AutoMap,
        )
        .await;
        // With `create_table` on, an unknown field becomes a column, so the
        // record left with no column at all is the empty one.
        let page = [json!({"id": 1}), json!({})];
        let err = sink.write_batch(&page).await.expect_err("unmatched record");
        assert!(err.to_string().contains("record 1 has no field"), "{err}");
        assert_eq!(count(&sink, "t"), 0, "the page is all-or-nothing");

        let outcomes = sink.write_batch_partial(&page).await.unwrap();
        assert!(outcomes[0].is_ok());
        assert!(
            outcomes[1]
                .as_ref()
                .unwrap_err()
                .to_string()
                .contains("record 1")
        );
        assert_eq!(count(&sink, "t"), 1);

        // With `create_table` off, a field with no column fails its own row.
        let fixed = DuckdbSink::new(
            DuckdbSinkConfig::new(":memory:", "f")
                .column_mapping(DuckdbColumnMapping::AutoMap)
                .with_create_table(false),
        )
        .await
        .unwrap();
        fixed
            .conn
            .lock()
            .unwrap()
            .execute_batch("CREATE TABLE f (id INTEGER)")
            .unwrap();
        let outcomes = fixed
            .write_batch_partial(&[json!({"id": 1}), json!({"id": 2, "other": "x"})])
            .await
            .unwrap();
        assert!(outcomes[0].is_ok());
        let err = outcomes[1].as_ref().unwrap_err().to_string();
        assert!(
            err.contains("other") && err.contains("create_table"),
            "{err}"
        );
        assert_eq!(count(&fixed, "f"), 1);

        let json_sink = sink_with_table(
            "CREATE TABLE j (data TEXT)",
            "j",
            DuckdbColumnMapping::Json {
                column: "data".into(),
            },
        )
        .await;
        let outcomes = json_sink.write_batch_partial(&page).await.unwrap();
        assert!(outcomes.iter().all(Result::is_ok));
        assert!(json_sink.write_batch_partial(&[]).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn schema_qualified_and_shadowed_targets_resolve_to_one_table() {
        let sink = DuckdbSink::new(
            DuckdbSinkConfig::new(":memory:", "analytics.events")
                .column_mapping(DuckdbColumnMapping::AutoMap)
                .with_create_table(false),
        )
        .await
        .unwrap();
        sink.run_sql(
            "CREATE SCHEMA analytics; CREATE TABLE analytics.events (id INTEGER); \
             CREATE TABLE main.events (id INTEGER, extra TEXT);",
        )
        .await
        .unwrap();
        // `extra` is a column of the shadowing `main.events` only, so the
        // resolved target refuses it rather than borrowing the other table's.
        let err = sink
            .write_batch(&[json!({"id": 1, "extra": "x"})])
            .await
            .expect_err("analytics.events has no `extra`");
        assert!(err.to_string().contains("extra"), "{err}");
        assert_eq!(sink.write_batch(&[json!({"id": 1})]).await.unwrap(), 1);
        assert_eq!(count(&sink, "analytics.events"), 1);
        assert_eq!(count(&sink, "main.events"), 0);

        let plain = sink_with_table(
            "CREATE SCHEMA other; CREATE TABLE other.t (id INTEGER, ghost TEXT); \
             CREATE TABLE t (id INTEGER);",
            "t",
            DuckdbColumnMapping::AutoMap,
        )
        .await;
        assert_eq!(
            plain
                .write_batch(&[json!({"id": 1, "ghost": "g"})])
                .await
                .unwrap(),
            1
        );
        assert_eq!(count(&plain, "t"), 1);
        let ghost: i64 = plain
            .conn
            .lock()
            .unwrap()
            .query_row("SELECT count(*) FROM main.t WHERE ghost = 'g'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(ghost, 1, "the field is added to the resolved table");
        assert_eq!(split_qualified("db.s.t"), (Some("db"), Some("s"), "t"));
    }

    #[tokio::test]
    async fn empty_batch_is_noop() {
        let sink = sink_with_table(
            "CREATE TABLE t (data TEXT)",
            "t",
            DuckdbColumnMapping::default(),
        )
        .await;
        assert_eq!(sink.write_batch(&[]).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn a_missing_table_errors_not_panics_when_create_is_off() {
        let sink = DuckdbSink::new(
            DuckdbSinkConfig::new(":memory:", "nope")
                .column_mapping(DuckdbColumnMapping::AutoMap)
                .with_create_table(false),
        )
        .await
        .unwrap();
        let err = sink
            .write_batch(&[json!({"a": 1})])
            .await
            .expect_err("a missing table with create_table: false must fail");
        assert!(
            err.to_string().contains("create_table: true"),
            "the error must name the way out: {err}"
        );
    }

    /// The other arm of `create_table: false`: the table *does* exist, so the
    /// probe must accept it and write. Only the failure arm was covered, and a
    /// probe that rejected a healthy table would break every opted-out config.
    #[tokio::test]
    async fn an_existing_table_is_accepted_when_create_is_off() {
        let sink = DuckdbSink::new(
            DuckdbSinkConfig::new(":memory:", "present")
                .column_mapping(DuckdbColumnMapping::AutoMap)
                .with_create_table(false),
        )
        .await
        .unwrap();
        sink.conn
            .lock()
            .unwrap()
            .execute_batch("CREATE TABLE present (a BIGINT)")
            .expect("ddl");

        assert_eq!(sink.write_batch(&[json!({"a": 1})]).await.unwrap(), 1);
        assert_eq!(count(&sink, "present"), 1);
        // The probe runs once and latches, so a second page does not re-probe.
        assert_eq!(sink.write_batch(&[json!({"a": 2})]).await.unwrap(), 1);
        assert_eq!(count(&sink, "present"), 2);
    }

    /// A page with nothing inferable must leave the table uncreated so the
    /// next page can try — emitting a zero-column CREATE would poison the
    /// destination for every later page.
    #[tokio::test]
    async fn a_page_with_nothing_inferable_creates_no_table() {
        let sink = DuckdbSink::new(
            DuckdbSinkConfig::new(":memory:", "later").column_mapping(DuckdbColumnMapping::AutoMap),
        )
        .await
        .unwrap();

        // Records with no inferable columns: nothing to build a schema from,
        // so the page fails rather than creating a zero-column table.
        let err = sink
            .write_batch(&[json!({})])
            .await
            .expect_err("an empty shape cannot be written");
        assert!(
            err.to_string().contains("no columns or does not exist"),
            "got: {err}"
        );
        let exists: i64 = sink
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT count(*) FROM duckdb_tables() WHERE table_name = 'later'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(exists, 0, "no table may be created from an empty shape");

        // The point of leaving it uncreated: a later page with real columns
        // still creates it, so one shapeless page does not poison the run.
        assert_eq!(sink.write_batch(&[json!({"a": 1})]).await.unwrap(), 1);
        assert_eq!(count(&sink, "later"), 1);
    }

    #[tokio::test]
    async fn a_missing_table_is_created_from_the_first_page_by_default() {
        // #580: a first-ever sync cannot assume the destination exists.
        let sink = DuckdbSink::new(
            DuckdbSinkConfig::new(":memory:", "fresh").column_mapping(DuckdbColumnMapping::AutoMap),
        )
        .await
        .unwrap();
        let n = sink
            .write_batch(&[json!({"id": 1, "name": "a"}), json!({"id": 2, "name": "b"})])
            .await
            .expect("the table is created, then written");
        assert_eq!(n, 2);
        assert_eq!(count(&sink, "fresh"), 2);

        // The created columns take the inferred types, and a second page with
        // a null in a column that was non-null in page 1 still writes — every
        // inferred column is nullable on purpose.
        sink.write_batch(&[json!({"id": 3, "name": Value::Null})])
            .await
            .expect("a later null must not violate an inferred NOT NULL");
        assert_eq!(count(&sink, "fresh"), 3);
    }
}

#[cfg(test)]
mod schema_qualified_tests {
    use super::*;

    /// #456 L3: a schema-qualified target must quote each segment, or it names a
    /// table with a literal dot in it and can never resolve. The ClickHouse sink
    /// already did this; DuckDB used a single `quote_ident`.
    #[test]
    fn column_helpers() {
        assert!(is_naive_timestamp("TIMESTAMP"));
        assert!(is_naive_timestamp("timestamp_ns"));
        assert!(is_naive_timestamp("DATETIME"));
        assert!(!is_naive_timestamp("TIMESTAMP WITH TIME ZONE"));
        assert!(!is_naive_timestamp("TIMESTAMPTZ"));
        assert!(!is_naive_timestamp("VARCHAR"));
        let cols = vec!["id".to_string()];
        use serde_json::json;
        let recs = vec![
            json!({"id": 1, "a": 1}),
            json!({"a": 2, "b": 1.5}),
            json!(3),
        ];
        assert_eq!(missing_fields(&recs, &cols), vec!["a", "b"]);
        let planned = faucet_core::plan_columns(&recs[1..2]).unwrap();
        let b = planned.iter().find(|c| c.name == "b").unwrap();
        assert_eq!(
            add_column_sql("s.t", b),
            "ALTER TABLE \"s\".\"t\" ADD COLUMN IF NOT EXISTS \"b\" DOUBLE"
        );
        assert_eq!(
            utc_wall_time("2024-01-02T03:04:05.5Z").as_deref(),
            Some("2024-01-02 03:04:05.500")
        );
        assert_eq!(utc_wall_time("2024-01-02 03:04:05"), None);
    }

    #[test]
    fn quote_table_quotes_each_segment() {
        assert_eq!(quote_table("events"), "\"events\"");
        assert_eq!(quote_table("analytics.events"), "\"analytics\".\"events\"");
    }

    #[test]
    fn split_table_separates_schema_from_name() {
        assert_eq!(split_table("events"), (None, "events"));
        assert_eq!(
            split_table("analytics.events"),
            (Some("analytics"), "events")
        );
        // Deepest qualifier wins (catalog.schema.table → schema is the prefix).
        assert_eq!(
            split_table("db.analytics.events"),
            (Some("db.analytics"), "events")
        );
    }

    /// Every inferred base type must have a DuckDB keyword. A wrong keyword
    /// here silently creates a column of the wrong type on first write, and
    /// the data is only found to be mistyped much later.
    #[test]
    fn every_base_type_maps_to_a_duckdb_keyword() {
        use faucet_core::SqlBaseType;
        assert_eq!(duckdb_keyword(SqlBaseType::Integer), "BIGINT");
        assert_eq!(duckdb_keyword(SqlBaseType::Double), "DOUBLE");
        assert_eq!(duckdb_keyword(SqlBaseType::Boolean), "BOOLEAN");
        assert_eq!(duckdb_keyword(SqlBaseType::Text), "TEXT");
        // Nested values are serialised as JSON text by the writer, so the
        // column must be TEXT and not a DuckDB JSON type.
        assert_eq!(duckdb_keyword(SqlBaseType::Json), "TEXT");
    }
}
