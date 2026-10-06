//! DuckDB source implementation — the one module that performs I/O.
//!
//! `duckdb` is a synchronous, embedded engine, so every database call runs
//! inside [`tokio::task::spawn_blocking`]. Streaming stays bounded-memory: a
//! dedicated blocking task holds the connection, iterates the result row by
//! row, and hands finished [`StreamPage`]s to the async side over a small
//! bounded channel — never buffering the whole result set.

use crate::config::DuckdbSourceConfig;
use async_trait::async_trait;
use base64::Engine as _;
use duckdb::types::{Value as DuckValue, ValueRef};
use duckdb::{AccessMode, Config, Connection};
use faucet_core::{FaucetError, Stream, StreamPage};
use serde_json::Value;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

/// A source that executes a SQL query against DuckDB and returns rows as JSON.
///
/// The connection is opened once in [`DuckdbSource::new`] and reused for every
/// fetch/stream, wrapped in `Arc<Mutex<_>>` so it can move into the blocking
/// task that runs each query.
pub struct DuckdbSource {
    config: DuckdbSourceConfig,
    conn: Arc<Mutex<Connection>>,
}

/// Open a DuckDB connection honouring the config's path and access mode.
fn open(config: &DuckdbSourceConfig) -> Result<Connection, FaucetError> {
    let path = config.resolved_path();
    let mode = if config.read_only {
        AccessMode::ReadOnly
    } else {
        AccessMode::ReadWrite
    };
    let flags = Config::default()
        .access_mode(mode)
        .map_err(|e| FaucetError::Config(format!("duckdb config: {e}")))?;
    let conn = if path == ":memory:" {
        Connection::open_in_memory_with_flags(flags)
    } else {
        Connection::open_with_flags(path, flags)
    };
    conn.map_err(|e| FaucetError::Config(format!("DuckDB open failed ({path}): {e}")))
}

impl DuckdbSource {
    /// Create a new DuckDB source, opening (and reusing) one connection.
    pub async fn new(config: DuckdbSourceConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let cfg = config.clone();
        let conn = tokio::task::spawn_blocking(move || open(&cfg))
            .await
            .map_err(|e| FaucetError::Source(format!("duckdb open task panicked: {e}")))??;
        Ok(Self {
            config,
            conn: Arc::new(Mutex::new(conn)),
        })
    }
}

/// Build the effective SQL query and ordered context-bind values for a given
/// parent context. Returns the literal query when there is no context.
///
/// DuckDB accepts positional `?` placeholders, so the bind-marker formatter
/// ignores the index (mirrors the SQLite source).
fn resolve_query(
    config: &DuckdbSourceConfig,
    context: &HashMap<String, Value>,
) -> (String, Vec<Value>) {
    if context.is_empty() {
        (config.query.clone(), Vec::new())
    } else {
        faucet_core::util::substitute_context_bind_params(&config.query, context, 1, |_| {
            "?".to_string()
        })
    }
}

/// Convert a JSON context value into an owned DuckDB parameter value.
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
        // Arrays/objects have no scalar SQL form — bind their JSON text.
        other => DuckValue::Text(other.to_string()),
    }
}

/// Convert a DuckDB column value to a `serde_json::Value`.
///
/// Scalar types map exactly. `Text` becomes a UTF-8 (lossy) string and `Blob`
/// becomes base64 so binary survives the JSON round-trip, and `Decimal` its
/// exact text. Temporal and nested (LIST / STRUCT / MAP / …) types are best-effort: temporal values
/// surface their raw integer, and everything else falls back to a stable
/// debug string — documented in the crate README.
fn value_ref_to_json(v: ValueRef<'_>) -> Value {
    use serde_json::json;
    match v {
        ValueRef::Null => Value::Null,
        ValueRef::Boolean(b) => Value::Bool(b),
        ValueRef::TinyInt(n) => json!(n),
        ValueRef::SmallInt(n) => json!(n),
        ValueRef::Int(n) => json!(n),
        ValueRef::BigInt(n) => json!(n),
        ValueRef::HugeInt(n) => i64::try_from(n)
            .map(|x| json!(x))
            .unwrap_or_else(|_| Value::String(n.to_string())),
        ValueRef::UTinyInt(n) => json!(n),
        ValueRef::USmallInt(n) => json!(n),
        ValueRef::UInt(n) => json!(n),
        ValueRef::UBigInt(n) => json!(n),
        ValueRef::Float(f) => serde_json::Number::from_f64(f as f64)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        ValueRef::Double(f) => serde_json::Number::from_f64(f)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        ValueRef::Text(bytes) => Value::String(String::from_utf8_lossy(bytes).into_owned()),
        ValueRef::Blob(bytes) => {
            Value::String(base64::engine::general_purpose::STANDARD.encode(bytes))
        }
        // Exact decimal text, as the postgres/mysql sources emit: parsing it
        // into a JSON number rounds through `f64` (#789 SQL-15).
        ValueRef::Decimal(d) => Value::String(d.to_string()),
        ValueRef::Timestamp(_, n) => json!(n),
        ValueRef::Date32(n) => json!(n),
        ValueRef::Time64(_, n) => json!(n),
        ValueRef::Interval {
            months,
            days,
            nanos,
        } => json!({ "months": months, "days": days, "nanos": nanos }),
        // List, Struct, Map, Array, Enum, Union — best-effort.
        other => Value::String(format!("{other:?}")),
    }
}

/// Build a JSON object from the current row using the pre-fetched column names.
fn row_to_json(row: &duckdb::Row<'_>, col_names: &[String]) -> Result<Value, FaucetError> {
    let mut map = serde_json::Map::with_capacity(col_names.len());
    for (i, name) in col_names.iter().enumerate() {
        // duckdb-rs panics on some valid values (a DECIMAL wider than 28
        // significant digits, a nanosecond TIME); fail the read instead of
        // letting the panic end the stream as if it were complete.
        let read = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            row.get_ref(i).map(value_ref_to_json)
        }));
        let value = match read {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => {
                return Err(FaucetError::Source(format!(
                    "DuckDB column {name} read failed: {e}"
                )));
            }
            Err(_) => {
                return Err(FaucetError::Source(format!(
                    "DuckDB column {name} holds a value the driver cannot decode (a DECIMAL \
                     wider than 28 significant digits or a nanosecond TIME); CAST it to \
                     VARCHAR in the query"
                )));
            }
        };
        map.insert(name.clone(), value);
    }
    Ok(Value::Object(map))
}

/// Run the query on the blocking thread and drain every row into a `Vec`
/// (used by `fetch_with_context`).
fn collect_blocking(
    conn: &Arc<Mutex<Connection>>,
    query: &str,
    binds: &[Value],
) -> Result<Vec<Value>, FaucetError> {
    let guard = conn
        .lock()
        .map_err(|_| FaucetError::Source("duckdb connection mutex poisoned".into()))?;
    let mut stmt = guard
        .prepare(query)
        .map_err(|e| FaucetError::Source(format!("DuckDB prepare failed: {e}")))?;
    let params: Vec<DuckValue> = binds.iter().map(json_to_duck).collect();
    let mut rows = stmt
        .query(duckdb::params_from_iter(params))
        .map_err(|e| FaucetError::Source(format!("DuckDB query failed: {e}")))?;
    // DuckDB populates column metadata only after execution, so column names are
    // read from the first row (via `Row: AsRef<Statement>`), not the prepared
    // statement.
    let mut col_names: Vec<String> = Vec::new();
    let mut out = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| FaucetError::Source(format!("DuckDB row read failed: {e}")))?
    {
        if col_names.is_empty() {
            col_names = row.as_ref().column_names();
        }
        out.push(row_to_json(row, &col_names)?);
    }
    Ok(out)
}

/// Run the query on the blocking thread, sending bounded pages over `tx`.
fn stream_blocking(
    conn: &Arc<Mutex<Connection>>,
    query: &str,
    binds: &[Value],
    batch_size: usize,
    tx: &mpsc::Sender<Result<StreamPage, FaucetError>>,
) -> Result<(), FaucetError> {
    let guard = conn
        .lock()
        .map_err(|_| FaucetError::Source("duckdb connection mutex poisoned".into()))?;
    let mut stmt = guard
        .prepare(query)
        .map_err(|e| FaucetError::Source(format!("DuckDB prepare failed: {e}")))?;
    let params: Vec<DuckValue> = binds.iter().map(json_to_duck).collect();
    let mut rows = stmt
        .query(duckdb::params_from_iter(params))
        .map_err(|e| FaucetError::Source(format!("DuckDB query failed: {e}")))?;

    let chunk = if batch_size == 0 {
        usize::MAX
    } else {
        batch_size
    };
    let cap = if batch_size == 0 { 1024 } else { batch_size };
    let mut buffer: Vec<Value> = Vec::with_capacity(cap);
    // Column names come from the first row (see `collect_blocking`).
    let mut col_names: Vec<String> = Vec::new();

    while let Some(row) = rows
        .next()
        .map_err(|e| FaucetError::Source(format!("DuckDB row read failed: {e}")))?
    {
        if col_names.is_empty() {
            col_names = row.as_ref().column_names();
        }
        buffer.push(row_to_json(row, &col_names)?);
        if buffer.len() >= chunk {
            let page = std::mem::replace(&mut buffer, Vec::with_capacity(cap));
            // Receiver dropped (stream cancelled) → stop cleanly.
            if tx
                .blocking_send(Ok(StreamPage {
                    records: page,
                    bookmark: None,
                }))
                .is_err()
            {
                return Ok(());
            }
        }
    }
    if !buffer.is_empty() {
        let _ = tx.blocking_send(Ok(StreamPage {
            records: buffer,
            bookmark: None,
        }));
    }
    Ok(())
}

#[async_trait]
impl faucet_core::Source for DuckdbSource {
    async fn fetch_with_context(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let conn = self.conn.clone();
        let (query_str, binds) = resolve_query(&self.config, context);
        let query_label = self.config.query.clone();
        let records =
            tokio::task::spawn_blocking(move || collect_blocking(&conn, &query_str, &binds))
                .await
                .map_err(|e| FaucetError::Source(format!("duckdb query task panicked: {e}")))??;
        tracing::info!(
            rows = records.len(),
            query = %query_label,
            "DuckDB source fetch complete"
        );
        Ok(records)
    }

    /// Stream rows in bounded-memory pages. A blocking task holds the
    /// connection and pushes each finished page over a small channel; the async
    /// side never holds more than a couple of pages at once.
    ///
    /// The trait-level `batch_size` argument is ignored in favour of the config
    /// field (the user-facing knob). `batch_size = 0` drains the whole result
    /// into a single page. This is a full-query source with no incremental
    /// mode, so every page carries `bookmark: None`.
    fn stream_pages<'a>(
        &'a self,
        context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let conn = self.conn.clone();
        let (query_str, binds) = resolve_query(&self.config, context);
        let batch_size = self.config.batch_size;
        let query_label = self.config.query.clone();
        let (tx, mut rx) = mpsc::channel::<Result<StreamPage, FaucetError>>(4);

        let reader = tokio::task::spawn_blocking(move || {
            if let Err(e) = stream_blocking(&conn, &query_str, &binds, batch_size, &tx) {
                let _ = tx.blocking_send(Err(e));
            }
        });

        Box::pin(async_stream::try_stream! {
            let mut total = 0usize;
            while let Some(item) = rx.recv().await {
                let page = item?;
                total += page.records.len();
                yield page;
            }
            // The channel also closes when the reader panics; only a reader
            // that returned is a complete result (#789 SQL-42).
            reader.await.map_err(|e| {
                FaucetError::Source(format!("DuckDB reader stopped before the end of the result: {e}"))
            })?;
            tracing::info!(
                rows = total,
                batch_size,
                query = %query_label,
                "DuckDB source stream complete",
            );
        })
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(DuckdbSourceConfig))
            .expect("schema serialization")
    }

    fn connector_name(&self) -> &'static str {
        "duckdb"
    }

    fn dataset_uri(&self) -> String {
        format!(
            "duckdb://{}?query={}",
            self.config.resolved_path(),
            self.config.query
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::Source;
    use serde_json::json;

    async fn memory_source(setup: &str, query: &str) -> DuckdbSource {
        let source = DuckdbSource::new(DuckdbSourceConfig::new(":memory:", query))
            .await
            .unwrap();
        source
            .conn
            .lock()
            .unwrap()
            .execute_batch(setup)
            .expect("seed");
        source
    }

    #[tokio::test]
    async fn fetch_scalar_row() {
        let source = DuckdbSource::new(DuckdbSourceConfig::new(
            ":memory:",
            "SELECT 1 AS val, 'hello' AS msg, true AS flag, 2.5 AS score",
        ))
        .await
        .unwrap();
        let records = source.fetch_all().await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["val"], 1);
        assert_eq!(records[0]["msg"], "hello");
        assert_eq!(records[0]["flag"], true);
        assert_eq!(records[0]["score"], "2.5", "a DECIMAL literal stays exact");
        assert_eq!(source.connector_name(), "duckdb");
    }

    #[tokio::test]
    async fn fetch_from_table() {
        let source = memory_source(
            "CREATE TABLE items (id INTEGER, name TEXT); \
             INSERT INTO items VALUES (1, 'Alice'), (2, 'Bob');",
            "SELECT * FROM items ORDER BY id",
        )
        .await;
        let records = source.fetch_all().await.unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["id"], 1);
        assert_eq!(records[1]["name"], "Bob");
    }

    #[tokio::test]
    async fn blob_column_decodes_to_base64() {
        let source = DuckdbSource::new(DuckdbSourceConfig::new(
            ":memory:",
            "SELECT '\\x00\\xFF'::BLOB AS data",
        ))
        .await
        .unwrap();
        let records = source.fetch_all().await.unwrap();
        assert_eq!(records[0]["data"], "AP8=");
    }

    #[tokio::test]
    async fn streaming_pages_are_bounded() {
        let source = {
            let s = memory_source(
                "CREATE TABLE t (id INTEGER); \
                 INSERT INTO t SELECT * FROM range(0, 250);",
                "SELECT id FROM t ORDER BY id",
            )
            .await;
            DuckdbSource {
                config: s.config.with_batch_size(100),
                conn: s.conn,
            }
        };
        let ctx = HashMap::new();
        let mut stream = source.stream_pages(&ctx, 100);
        let mut seen = 0usize;
        let mut peak = 0usize;
        while let Some(page) = futures::StreamExt::next(&mut stream).await {
            let page = page.unwrap();
            peak = peak.max(page.records.len());
            seen += page.records.len();
        }
        assert_eq!(seen, 250);
        assert!(peak <= 100, "peak page {peak} exceeds batch_size");
        assert!(peak < 250, "buffered everything into one page");
    }

    #[tokio::test]
    async fn decimals_are_exact_strings() {
        let source = DuckdbSource::new(DuckdbSourceConfig::new(
            ":memory:",
            "SELECT CAST('12345678901234567.0123456789' AS DECIMAL(38, 10)) AS d",
        ))
        .await
        .unwrap();
        let records = source.fetch_all().await.unwrap();
        assert_eq!(records[0]["d"], json!("12345678901234567.0123456789"));
    }

    #[tokio::test]
    async fn an_undecodable_value_fails_the_stream_instead_of_ending_it() {
        let source = DuckdbSource::new(DuckdbSourceConfig::new(
            ":memory:",
            "SELECT * FROM (VALUES (1, CAST(1 AS DECIMAL(38, 2))), \
             (2, CAST('123456789012345678901234567890.12' AS DECIMAL(38, 2)))) t(id, d)",
        ))
        .await
        .unwrap();
        let ctx = HashMap::new();
        let mut stream = source.stream_pages(&ctx, 100);
        let mut error = None;
        while let Some(page) = futures::StreamExt::next(&mut stream).await {
            if let Err(e) = page {
                error = Some(e.to_string());
                break;
            }
        }
        let error = error.expect("the stream must not end cleanly");
        assert!(error.contains("column d"), "{error}");
        assert!(source.fetch_all().await.is_err());
    }

    #[tokio::test]
    async fn empty_result() {
        let source = DuckdbSource::new(DuckdbSourceConfig::new(
            ":memory:",
            "SELECT 1 AS x WHERE 1 = 0",
        ))
        .await
        .unwrap();
        assert!(source.fetch_all().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn invalid_query_returns_error() {
        let source = DuckdbSource::new(DuckdbSourceConfig::new(":memory:", "NOT VALID SQL"))
            .await
            .unwrap();
        assert!(source.fetch_all().await.is_err());
    }

    #[tokio::test]
    async fn fetch_with_context_binds_params_safely() {
        let source = DuckdbSource::new(DuckdbSourceConfig::new(
            ":memory:",
            "SELECT {val} AS result",
        ))
        .await
        .unwrap();
        let mut context = HashMap::new();
        context.insert("val".to_string(), serde_json::json!("1; DROP TABLE x; --"));
        let records = source.fetch_with_context(&context).await.unwrap();
        assert_eq!(records[0]["result"], "1; DROP TABLE x; --");
    }

    #[tokio::test]
    async fn new_rejects_out_of_range_batch_size() {
        let config = DuckdbSourceConfig::new(":memory:", "SELECT 1")
            .with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(matches!(
            DuckdbSource::new(config).await,
            Err(FaucetError::Config(_))
        ));
    }

    #[test]
    fn value_ref_json_scalars() {
        assert_eq!(value_ref_to_json(ValueRef::Null), Value::Null);
        assert_eq!(
            value_ref_to_json(ValueRef::Boolean(true)),
            Value::Bool(true)
        );
        assert_eq!(value_ref_to_json(ValueRef::Int(7)), serde_json::json!(7));
        assert_eq!(
            value_ref_to_json(ValueRef::Text(b"hi")),
            Value::String("hi".into())
        );
    }
}
