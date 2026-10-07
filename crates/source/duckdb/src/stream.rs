//! DuckDB source implementation — the one module that performs I/O.
//!
//! `duckdb` is a synchronous, embedded engine, so every database call runs
//! inside [`tokio::task::spawn_blocking`]. Streaming stays bounded-memory: a
//! dedicated blocking task holds the connection, pulls the result chunk by
//! chunk through DuckDB's streaming execution, and hands finished
//! [`StreamPage`]s to the async side over a small bounded channel — never
//! materializing the whole result set.

use crate::config::DuckdbSourceConfig;
use crate::convert;
use async_trait::async_trait;
use duckdb::arrow::array::{Array as _, StringArray};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::types::Value as DuckValue;
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

/// DuckDB types whose Arrow export loses information, so the source casts them
/// to `VARCHAR`: `UHUGEINT` arrives as a signed 128-bit decimal (values from
/// 2^127 up turn negative), `BIT` / `BIGNUM` as DuckDB's internal bytes and
/// `TIMETZ` without its offset.
fn needs_text_cast(duck_type: &str) -> bool {
    matches!(
        duck_type.to_ascii_uppercase().as_str(),
        "UHUGEINT" | "BIT" | "BITSTRING" | "BIGNUM" | "VARINT" | "TIMETZ" | "TIME WITH TIME ZONE"
    )
}

/// The query as it will run: the user's text with trailing `;` removed, and,
/// when a column needs a text cast, wrapped in a projection applying it.
#[derive(Debug, PartialEq)]
struct Plan {
    sql: String,
    duck_types: Vec<String>,
}

fn trimmed(query: &str) -> &str {
    query.trim_end().trim_end_matches(';').trim_end()
}

/// Build the plan from `DESCRIBE`'s `(name, type)` rows. Two result columns
/// sharing a name are refused: a JSON row holds one value per name.
fn plan_from_description(query: &str, columns: &[(String, String)]) -> Result<Plan, FaucetError> {
    let mut seen = std::collections::HashSet::new();
    for (name, _) in columns {
        if !seen.insert(name.as_str()) {
            return Err(duplicate_column(name));
        }
    }
    let query = trimmed(query);
    let duck_types = columns.iter().map(|(_, t)| t.clone()).collect();
    if !columns.iter().any(|(_, t)| needs_text_cast(t)) {
        return Ok(Plan {
            sql: query.to_string(),
            duck_types,
        });
    }
    let projection = columns
        .iter()
        .map(|(name, t)| {
            let q = faucet_core::util::quote_ident(name);
            if needs_text_cast(t) {
                format!("CAST({q} AS VARCHAR) AS {q}")
            } else {
                q
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    Ok(Plan {
        sql: format!("SELECT {projection} FROM (\n{query}\n) AS __faucet_q"),
        duck_types: columns
            .iter()
            .map(|(_, t)| {
                if needs_text_cast(t) {
                    "VARCHAR".to_string()
                } else {
                    t.clone()
                }
            })
            .collect(),
    })
}

fn duplicate_column(name: &str) -> FaucetError {
    FaucetError::Source(format!(
        "DuckDB query returns two columns named {name:?}; alias them \
         (`SELECT a.{name} AS a_{name}, b.{name} AS b_{name} …`) so neither is lost"
    ))
}

fn source_err(what: &str) -> impl Fn(duckdb::Error) -> FaucetError + '_ {
    move |e| FaucetError::Source(format!("DuckDB {what} failed: {e}"))
}

/// `DESCRIBE` the query to learn each column's DuckDB type. `None` when the
/// statement cannot be described (a `PRAGMA`, `CALL`, …).
fn describe(conn: &Connection, query: &str, params: &[DuckValue]) -> Option<Vec<(String, String)>> {
    let mut stmt = conn.prepare(&format!("DESCRIBE {}", trimmed(query))).ok()?;
    let batches: Vec<_> = stmt
        .query_arrow(duckdb::params_from_iter(params.iter().cloned()))
        .ok()?
        .collect();
    let mut out = Vec::new();
    for batch in batches {
        let names = batch.column(0).as_any().downcast_ref::<StringArray>()?;
        let types = batch.column(1).as_any().downcast_ref::<StringArray>()?;
        for i in 0..batch.num_rows() {
            out.push((names.value(i).to_string(), types.value(i).to_string()));
        }
    }
    Some(out)
}

/// Run `query`, handing each converted Arrow batch to `sink`. Results stream
/// chunk by chunk (`duckdb_execute_prepared_streaming`), so memory stays
/// bounded; only a statement that cannot be wrapped for the schema probe (a
/// `PRAGMA`, `SHOW`, …) falls back to a materialized result. `sink` returns
/// `false` to stop early.
fn run_query(
    conn: &Connection,
    query: &str,
    binds: &[Value],
    mut sink: impl FnMut(Vec<Value>) -> Result<bool, FaucetError>,
) -> Result<(), FaucetError> {
    let params: Vec<DuckValue> = binds.iter().map(json_to_duck).collect();
    let plan = match describe(conn, query, &params) {
        Some(columns) => plan_from_description(query, &columns)?,
        None => Plan {
            sql: trimmed(query).to_string(),
            duck_types: Vec::new(),
        },
    };
    let bound = || duckdb::params_from_iter(params.iter().cloned());

    let schema = conn
        .prepare(&format!(
            "SELECT * FROM (\n{}\n) AS __faucet_probe LIMIT 0",
            plan.sql
        ))
        .and_then(|mut probe| {
            let _ = probe.query_arrow(bound())?.count();
            Ok(probe.schema())
        });

    let mut stmt = conn.prepare(&plan.sql).map_err(source_err("prepare"))?;
    match schema {
        Ok(schema) => {
            check_unique(schema.fields().iter().map(|f| f.name().as_str()))?;
            let stream = stmt
                .stream_arrow(bound(), schema)
                .map_err(source_err("query"))?;
            for batch in guarded(stream) {
                if !sink(convert::batch_to_values(&batch?, &plan.duck_types)?)? {
                    break;
                }
            }
        }
        Err(_) => {
            let arrow = stmt.query_arrow(bound()).map_err(source_err("query"))?;
            let mut checked = false;
            for batch in guarded(arrow) {
                let batch = batch?;
                if !checked {
                    check_unique(batch.schema().fields().iter().map(|f| f.name().as_str()))?;
                    checked = true;
                }
                if !sink(convert::batch_to_values(&batch, &plan.duck_types)?)? {
                    break;
                }
            }
        }
    }
    Ok(())
}

fn check_unique<'a>(names: impl Iterator<Item = &'a str>) -> Result<(), FaucetError> {
    let mut seen = std::collections::HashSet::new();
    for name in names {
        if !seen.insert(name) {
            return Err(duplicate_column(name));
        }
    }
    Ok(())
}

/// Iterate batches, turning a driver panic (it `expect`s inside the FFI
/// import) into an error so a failed read never looks like the end of the
/// result.
fn guarded<I: Iterator<Item = RecordBatch>>(
    mut iter: I,
) -> impl Iterator<Item = Result<RecordBatch, FaucetError>> {
    let mut failed = false;
    std::iter::from_fn(move || {
        if failed {
            return None;
        }
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| iter.next())) {
            Ok(next) => next.map(Ok),
            Err(_) => {
                failed = true;
                Some(Err(FaucetError::Source(
                    "DuckDB result chunk could not be decoded; CAST unusual columns to \
                     VARCHAR in the query"
                        .into(),
                )))
            }
        }
    })
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
    let mut out = Vec::new();
    run_query(&guard, query, binds, |rows| {
        out.extend(rows);
        Ok(true)
    })?;
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
    let chunk = if batch_size == 0 {
        usize::MAX
    } else {
        batch_size
    };
    let cap = if batch_size == 0 { 1024 } else { batch_size };
    let mut buffer: Vec<Value> = Vec::with_capacity(cap);
    let mut open = true;
    run_query(&guard, query, binds, |rows| {
        for row in rows {
            buffer.push(row);
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
                    open = false;
                    return Ok(false);
                }
            }
        }
        Ok(true)
    })?;
    if open && !buffer.is_empty() {
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
    async fn wide_decimals_decode_exactly() {
        let source = DuckdbSource::new(DuckdbSourceConfig::new(
            ":memory:",
            "SELECT * FROM (VALUES (1, CAST(1 AS DECIMAL(38, 2))), \
             (2, CAST('123456789012345678901234567890.12' AS DECIMAL(38, 2)))) t(id, d)",
        ))
        .await
        .unwrap();
        let rows = source.fetch_all().await.unwrap();
        assert_eq!(rows[1]["d"], json!("123456789012345678901234567890.12"));
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

    async fn one_row(query: &str) -> Value {
        let source = DuckdbSource::new(DuckdbSourceConfig::new(":memory:", query))
            .await
            .unwrap();
        let mut rows = source.fetch_all().await.unwrap();
        assert_eq!(rows.len(), 1);
        rows.remove(0)
    }

    #[tokio::test]
    async fn temporal_values_are_iso_at_their_precision() {
        let row = one_row(
            "SELECT TIMESTAMP_S '2024-01-01 10:00:00' AS ts_s, \
             TIMESTAMP_NS '2024-01-01 10:00:00.123456789' AS ts_ns, \
             TIMESTAMP '2024-01-01 10:00:00.5' AS ts, \
             TIMESTAMPTZ '2024-01-01 10:00:00+05:30' AS tz, \
             DATE '2024-01-02' AS d, TIME '10:11:12.5' AS t, \
             TIMETZ '10:00:00+05' AS ttz, INTERVAL 3 DAY AS i, \
             'infinity'::TIMESTAMP AS inf, '-infinity'::DATE AS ninf",
        )
        .await;
        assert_eq!(row["ts_s"], json!("2024-01-01T10:00:00"));
        assert_eq!(row["ts_ns"], json!("2024-01-01T10:00:00.123456789"));
        assert_eq!(row["ts"], json!("2024-01-01T10:00:00.500"));
        assert_eq!(row["tz"], json!("2024-01-01T04:30:00Z"));
        assert_eq!(row["d"], json!("2024-01-02"));
        assert_eq!(row["t"], json!("10:11:12.500"));
        assert_eq!(row["ttz"], json!("10:00:00+05"));
        assert_eq!(row["i"], json!({"months": 0, "days": 3, "nanos": 0}));
        assert_eq!(row["inf"], json!("infinity"));
        assert_eq!(row["ninf"], json!("-infinity"));
    }

    #[tokio::test]
    async fn nested_values_are_json_of_this_row_only() {
        let source = memory_source(
            "CREATE TABLE n AS SELECT * FROM (VALUES \
               (1, [1, 2], {'k': 'a'}, MAP {'x': 1}, MAP {1: 'one'}, 'x'::ENUM('x', 'y')), \
               (2, [3], {'k': 'b'}, MAP {'y': 2}, MAP {2: 'two'}, 'y'::ENUM('x', 'y'))) \
             t(id, l, s, m, mi, e)",
            "SELECT id, l, s, m, mi, e, [1, 2]::INT[2] AS arr, union_value(n := 7) AS un \
             FROM n ORDER BY id",
        )
        .await;
        let rows = source.fetch_all().await.unwrap();
        assert_eq!(rows[1]["l"], json!([3]));
        assert_eq!(rows[1]["s"], json!({"k": "b"}));
        assert_eq!(rows[1]["m"], json!({"y": 2}));
        assert_eq!(rows[1]["mi"], json!([{"key": 2, "value": "two"}]));
        assert_eq!(rows[1]["e"], json!("y"));
        assert_eq!(rows[0]["arr"], json!([1, 2]));
        assert_eq!(rows[0]["un"], json!(7));
    }

    #[tokio::test]
    async fn wide_and_opaque_types_come_out_exact() {
        let row = one_row(
            "SELECT 340282366920938463463374607431768211455::UHUGEINT AS u, \
             170141183460469231731687303715884105728::UHUGEINT AS u127, \
             '101'::BIT AS b, 12345678901234567890123456789::BIGNUM AS g, \
             42::HUGEINT AS h, 170141183460469231731687303715884105727::HUGEINT AS hmax, \
             18446744073709551615::UBIGINT AS ub, \
             '11111111-1111-1111-1111-111111111111'::UUID AS uu, \
             0.1::REAL AS r, 'nan'::DOUBLE AS nd, '-inf'::REAL AS nr, NULL::INT AS z",
        )
        .await;
        assert_eq!(row["u"], json!("340282366920938463463374607431768211455"));
        assert_eq!(
            row["u127"],
            json!("170141183460469231731687303715884105728")
        );
        assert_eq!(row["b"], json!("101"));
        assert_eq!(row["g"], json!("12345678901234567890123456789"));
        assert_eq!(row["h"], json!(42));
        assert_eq!(
            row["hmax"],
            json!("170141183460469231731687303715884105727")
        );
        assert_eq!(row["ub"], json!(18446744073709551615u64));
        assert_eq!(row["uu"], json!("11111111-1111-1111-1111-111111111111"));
        assert_eq!(row["r"], json!(0.1));
        assert_eq!(row["nd"], json!("NaN"));
        assert_eq!(row["nr"], json!("-Infinity"));
        assert_eq!(row["z"], Value::Null);
    }

    #[tokio::test]
    async fn duplicate_column_names_are_refused() {
        let source = DuckdbSource::new(DuckdbSourceConfig::new(
            ":memory:",
            "SELECT 1 AS id, 2 AS id;",
        ))
        .await
        .unwrap();
        let err = source.fetch_all().await.expect_err("duplicate names");
        assert!(
            err.to_string().contains("two columns named \"id\""),
            "{err}"
        );
        assert!(check_unique(["a", "b", "a"].into_iter()).is_err());
    }

    #[tokio::test]
    async fn statements_that_cannot_be_wrapped_still_run() {
        let row = one_row("PRAGMA version").await;
        assert!(row.get("library_version").is_some(), "{row}");
    }

    #[test]
    fn plans_cast_only_lossy_columns() {
        let cols = vec![
            ("a".to_string(), "INTEGER".to_string()),
            ("b".to_string(), "UHUGEINT".to_string()),
        ];
        let plan = plan_from_description("SELECT a, b FROM t -- note\n;", &cols).unwrap();
        assert_eq!(
            plan.sql,
            "SELECT \"a\", CAST(\"b\" AS VARCHAR) AS \"b\" FROM (\nSELECT a, b FROM t -- note\n) AS __faucet_q"
        );
        assert_eq!(plan.duck_types, vec!["INTEGER", "VARCHAR"]);
        let plain = plan_from_description("SELECT 1 AS a;", &cols[..1]).unwrap();
        assert_eq!(plain.sql, "SELECT 1 AS a");
    }
}
