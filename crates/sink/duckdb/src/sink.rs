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
use faucet_core::FaucetError;
use faucet_core::util::quote_ident;

use serde_json::Value;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

/// Quote a possibly schema-qualified table name, one segment at a time, so
/// `analytics.events` becomes `"analytics"."events"` rather than the single
/// identifier `"analytics.events"` (which names a table with a dot in it and can
/// never resolve). Mirrors the ClickHouse sink's `quote_table` (#456 L3).
fn quote_table(table: &str) -> String {
    table
        .split('.')
        .map(quote_ident)
        .collect::<Vec<_>>()
        .join(".")
}

/// Map a [`faucet_core::SqlBaseType`] to the DuckDB type keyword used when
/// auto-creating a table (#580). Integers land as `BIGINT` and floats as
/// `DOUBLE` so a later, wider value never overflows a narrower column;
/// nested values land as `JSON` text, matching how the writer serialises them.
fn duckdb_keyword(t: faucet_core::SqlBaseType) -> &'static str {
    use faucet_core::SqlBaseType::*;
    match t {
        Integer => "BIGINT",
        Double => "DOUBLE",
        Boolean => "BOOLEAN",
        Text => "TEXT",
        Json => "TEXT",
    }
}

/// `CREATE TABLE IF NOT EXISTS` for an auto-created target (#580).
fn build_create_table_sql(
    table: &str,
    columns: &[faucet_core::PlannedColumn],
    json_column: Option<&str>,
) -> String {
    let cols = match json_column {
        // JSON mode stores the whole record in one column, so the page's own
        // shape is irrelevant — the table is the same whatever arrives.
        Some(col) => format!("{} TEXT NOT NULL", quote_ident(col)),
        None => faucet_core::render_columns(columns, quote_ident, duckdb_keyword),
    };
    format!("CREATE TABLE IF NOT EXISTS {} ({cols})", quote_table(table))
}

/// The bare table name of a possibly schema-qualified target, plus its schema —
/// `information_schema.columns` stores the two separately.
fn split_table(table: &str) -> (Option<&str>, &str) {
    match table.rsplit_once('.') {
        Some((schema, name)) => (Some(schema), name),
        None => (None, table),
    }
}

/// A sink that writes JSON records to a DuckDB table.
pub struct DuckdbSink {
    config: DuckdbSinkConfig,
    conn: Arc<Mutex<Connection>>,
    /// The process-wide database instance this sink's connection belongs to,
    /// kept alive for as long as the sink is.
    _instance: Option<SharedInstance>,
    /// Whether the target has been confirmed present for this sink instance
    /// (#580). One check per run, not per page.
    table_ready: std::sync::atomic::AtomicBool,
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
    let placeholders = vec!["(?)"; records.len()].join(", ");
    let sql = format!(
        "INSERT INTO {} ({}) VALUES {}",
        quote_table(table),
        quote_ident(column),
        placeholders
    );
    let mut params: Vec<DuckValue> = Vec::with_capacity(records.len());
    for r in records {
        let text = serde_json::to_string(r)
            .map_err(|e| FaucetError::Sink(format!("failed to serialize record: {e}")))?;
        params.push(DuckValue::Text(text));
    }
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| FaucetError::Sink(format!("DuckDB prepare failed: {e}")))?;
    stmt.execute(duckdb::params_from_iter(params))
        .map_err(|e| FaucetError::Sink(format!("DuckDB insert failed: {e}")))?;
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
            return Err(FaucetError::Sink(format!(
                "duckdb: table '{table}' has no column for field(s) {}; add them, drop the \
                 fields with a transform, or set `create_table: true`",
                missing.join(", ")
            )));
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

/// The table's columns (declared order) and its tz-less timestamp columns.
fn table_columns(
    conn: &Connection,
    table: &str,
) -> Result<(Vec<String>, HashSet<String>), FaucetError> {
    // `table` may be schema-qualified, and information_schema keeps the schema and
    // the name in separate columns (#456 L3).
    let (schema, name) = split_table(table);
    let (sql, args): (&str, Vec<&str>) = match schema {
        Some(schema) => (
            "SELECT column_name, data_type FROM information_schema.columns \
             WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
            vec![schema, name],
        ),
        None => (
            "SELECT column_name, data_type FROM information_schema.columns \
             WHERE table_name = ? ORDER BY ordinal_position",
            vec![name],
        ),
    };
    let mut cstmt = conn
        .prepare(sql)
        .map_err(|e| FaucetError::Sink(format!("failed to query table columns: {e}")))?;
    let rows = cstmt
        .query_map(duckdb::params_from_iter(args), |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
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

/// A `TIMESTAMP` type without a time zone (`TIMESTAMP`, `TIMESTAMP_NS`, …).
fn is_naive_timestamp(ty: &str) -> bool {
    let t = ty.to_ascii_uppercase();
    (t.starts_with("TIMESTAMP") || t == "DATETIME")
        && !t.contains("TIME ZONE")
        && t != "TIMESTAMPTZ"
}

/// Record fields (first-seen order) that are not columns of the table.
fn missing_fields(records: &[Value], cols: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for key in records
        .iter()
        .filter_map(Value::as_object)
        .flat_map(|o| o.keys())
    {
        if !cols.contains(key) && !out.contains(key) {
            out.push(key.clone());
        }
    }
    out
}

/// `ALTER TABLE … ADD COLUMN IF NOT EXISTS`, typed like an auto-created column.
fn add_column_sql(table: &str, column: &faucet_core::PlannedColumn) -> String {
    let def =
        faucet_core::render_columns(std::slice::from_ref(column), quote_ident, duckdb_keyword);
    format!(
        "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {def}",
        quote_table(table)
    )
}

/// An ISO 8601 timestamp carrying an offset as UTC wall time; `None` otherwise.
fn utc_wall_time(s: &str) -> Option<String> {
    let dt = chrono::DateTime::parse_from_rfc3339(s.trim()).ok()?;
    Some(dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.f").to_string())
}

fn insert_rows(
    conn: &Connection,
    table: &str,
    records: &[Value],
    cols: &[String],
    naive_ts: &HashSet<String>,
) -> Result<usize, FaucetError> {
    // Records with at least one matching column; the INSERT column set is the
    // union of table columns present in any such record (declared order). A row
    // missing a unioned column binds SQL NULL. Records with no matching key are
    // skipped (mirrors the SQLite sink).
    let mut used: HashSet<&str> = HashSet::new();
    let mut rows: Vec<&serde_json::Map<String, Value>> = Vec::with_capacity(records.len());
    for rec in records {
        let obj = rec
            .as_object()
            .ok_or_else(|| FaucetError::Sink("AutoMap requires JSON object records".into()))?;
        if !cols.iter().any(|c| obj.contains_key(c)) {
            tracing::warn!(
                record_keys = ?obj.keys().collect::<Vec<_>>(),
                "record has no keys matching table columns, skipping"
            );
            continue;
        }
        for c in cols {
            if obj.contains_key(c) {
                used.insert(c.as_str());
            }
        }
        rows.push(obj);
    }
    if rows.is_empty() {
        return Ok(0);
    }

    let insert_cols: Vec<&String> = cols.iter().filter(|c| used.contains(c.as_str())).collect();
    let num_cols = insert_cols.len();
    let col_list = insert_cols
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    let row_ph = format!("({})", vec!["?"; num_cols].join(", "));
    let values = vec![row_ph.as_str(); rows.len()].join(", ");
    let sql = format!(
        "INSERT INTO {} ({}) VALUES {}",
        quote_table(table),
        col_list,
        values
    );

    let mut params: Vec<DuckValue> = Vec::with_capacity(rows.len() * num_cols);
    for obj in &rows {
        for c in &insert_cols {
            let value = match obj.get(*c) {
                Some(Value::String(v)) if naive_ts.contains(c.as_str()) => {
                    DuckValue::Text(utc_wall_time(v).unwrap_or_else(|| v.clone()))
                }
                Some(v) => json_to_duck(v),
                None => DuckValue::Null,
            };
            params.push(value);
        }
    }

    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| FaucetError::Sink(format!("DuckDB prepare failed: {e}")))?;
    stmt.execute(duckdb::params_from_iter(params))
        .map_err(|e| FaucetError::Sink(format!("DuckDB insert failed: {e}")))?;
    Ok(rows.len())
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

impl DuckdbSink {
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
            let found: i64 = guard
                .query_row(
                    "SELECT count(*) FROM duckdb_tables() WHERE table_name = ?",
                    [&self.config.table_name],
                    |r| r.get(0),
                )
                .map_err(|e| FaucetError::Sink(format!("duckdb table probe failed: {e}")))?;
            if found == 0 {
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
        let columns = match (json_column, faucet_core::plan_columns(records)) {
            (Some(_), _) => Vec::new(),
            (None, Some(c)) => c,
            (None, None) => return Ok(()),
        };
        let sql = build_create_table_sql(&self.config.table_name, &columns, json_column);
        guard
            .execute_batch(&sql)
            .map_err(|e| FaucetError::Sink(format!("duckdb CREATE TABLE failed: {e}")))?;
        drop(guard);
        self.table_ready.store(true, Ordering::Relaxed);
        Ok(())
    }

    /// Create a new DuckDB sink, opening (and reusing) one read-write connection.
    pub async fn new(config: DuckdbSinkConfig) -> Result<Self, FaucetError> {
        faucet_core::validate_batch_size(config.batch_size)?;
        let path = config.resolved_path().to_string();
        let (conn, instance) = tokio::task::spawn_blocking(move || connect(&path))
            .await
            .map_err(|e| FaucetError::Sink(format!("duckdb open task panicked: {e}")))??;
        Ok(Self {
            config,
            conn: Arc::new(Mutex::new(conn)),
            _instance: instance,
            table_ready: std::sync::atomic::AtomicBool::new(false),
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
    fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        self.config.batch_atomicity()
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(DuckdbSinkConfig))
            .expect("schema serialization")
    }

    fn connector_name(&self) -> &'static str {
        "duckdb"
    }

    fn dataset_uri(&self) -> String {
        format!(
            "duckdb://{}?table={}",
            self.config.resolved_path(),
            self.config.table_name
        )
    }

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        self.ensure_table_ready(records)?;
        let conn = self.conn.clone();
        let config = self.config.clone();
        let owned = records.to_vec();
        let n = tokio::task::spawn_blocking(move || write_all_blocking(&conn, &config, &owned))
            .await
            .map_err(|e| FaucetError::Sink(format!("duckdb write task panicked: {e}")))??;
        tracing::info!(
            table = %self.config.table_name,
            rows = n,
            "DuckDB write complete"
        );
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::Sink as _;
    use serde_json::json;

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
