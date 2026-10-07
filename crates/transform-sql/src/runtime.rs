//! The compiled SQL transform: owns the DuckDB connection and runs each page.

use crate::compile::{Reloadable, build_connection, sql_escape, validate_query};
use crate::config::SqlTransformConfig;
use crate::shovel::{
    ABSENT_MARKER, infer_schema, json_to_record_batch, mark_absent_fields, record_batches_to_json,
    schema_eq, strip_absent_fields,
};
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use duckdb::Connection;
use duckdb::vtab::arrow::arrow_recordbatch_to_query_params;
use faucet_core::FaucetError;
use faucet_core::stage::TransformStage;
use serde_json::Value;
use std::sync::{Arc, Mutex};

struct State {
    conn: Connection,
    query: String,
    reloadables: Vec<Reloadable>,
    cached_schema: Option<SchemaRef>,
    pages_seen: u64,
    aggregates: Option<bool>,
    warned: bool,
}

/// A compiled SQL transform. One DuckDB connection, reused across the row's pages.
pub struct SqlTransform {
    state: Arc<Mutex<State>>,
}

impl std::fmt::Debug for SqlTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("SqlTransform");
        match self.state.lock() {
            Ok(st) => d.field("query", &st.query),
            Err(e) => d.field("query", &e.into_inner().query),
        };
        d.finish_non_exhaustive()
    }
}

impl SqlTransform {
    /// Build the connection, load reference relations, and validate the query.
    pub fn compile(cfg: &SqlTransformConfig) -> Result<Self, FaucetError> {
        let (conn, reloadables) = build_connection(cfg)?;
        validate_query(&conn, &cfg.query)?;
        Ok(Self {
            state: Arc::new(Mutex::new(State {
                conn,
                query: cfg.query.clone(),
                reloadables,
                cached_schema: None,
                pages_seen: 0,
                aggregates: None,
                warned: false,
            })),
        })
    }

    /// Consume into a page-level `Value` transform stage.
    pub fn into_page_stage(self) -> TransformStage {
        let state = self.state;
        TransformStage::PageFn(Arc::new(move |records: Vec<Value>| {
            let mut st = state.lock().unwrap_or_else(|e| e.into_inner());
            execute_page(&mut st, records)
        }))
    }

    /// Consume into a `Value` page stage **plus** an Arrow `RecordBatch → Record
    /// Batch` form that shares the same DuckDB connection (#375). The CLI passes
    /// the batch fn to `TransformingSource::new_with_batches`, so a `parquet →
    /// sql → parquet` chain runs Arrow end-to-end (no `Value` materialization)
    /// while any other chain keeps using the `Value` page stage.
    pub fn into_columnar_stage(self) -> (TransformStage, faucet_core::stage::PageFnBatchBox) {
        let rows_state = self.state.clone();
        let batch_state = self.state;
        let stage = TransformStage::PageFn(Arc::new(move |records: Vec<Value>| {
            let mut st = rows_state.lock().unwrap_or_else(|e| e.into_inner());
            execute_page(&mut st, records)
        }));
        let batch: faucet_core::stage::PageFnBatchBox = Arc::new(move |batch: RecordBatch| {
            let mut st = batch_state.lock().unwrap_or_else(|e| e.into_inner());
            execute_batch(&mut st, batch)
        });
        (stage, batch)
    }
}

/// Register `batch` into DuckDB and run the query, returning the raw Arrow
/// result batches. Shared by the `Value` path ([`execute_page`]) and the
/// columnar path ([`execute_batch`]) so both use the same #372 chunked
/// registration and the same per-page aggregation-warning state.
fn run_query_batches(st: &mut State, batch: RecordBatch) -> Result<Vec<RecordBatch>, FaucetError> {
    reload_relations(st)?;
    // DuckDB's arrow vtab copies each arrow array into a `DataChunk` whose
    // capacity is `STANDARD_VECTOR_SIZE` (2048); handing it a single batch with
    // more rows than that aborts the process (#372). `register_batch_chunked`
    // slices to <=2048 rows into one `batch` relation, so query semantics are
    // unchanged.
    register_batch_chunked(st, batch)?;

    // First-page aggregation detection (now that `batch` exists).
    if st.aggregates.is_none() {
        st.aggregates = Some(plan_has_aggregate(&st.conn, &st.query));
    }
    st.pages_seen += 1;
    if st.pages_seen >= 2 && st.aggregates == Some(true) && !st.warned {
        st.warned = true;
        tracing::warn!(
            target: "faucet::transform::sql",
            "sql transform with aggregation received multiple pages; aggregation is \
             per-page — set batch_size: 0 for global aggregation"
        );
    }

    let mut stmt = st
        .conn
        .prepare(&st.query)
        .map_err(|e| FaucetError::Transform(format!("sql transform: prepare: {e}")))?;
    let batches: Vec<RecordBatch> = stmt
        .query_arrow([])
        .map_err(|e| FaucetError::Transform(format!("sql transform: execute: {e}")))?
        .collect();
    Ok(batches)
}

fn execute_page(st: &mut State, mut records: Vec<Value>) -> Result<Vec<Value>, FaucetError> {
    if records.is_empty() {
        return Ok(Vec::new());
    }
    // Schema cache: infer once per page, reuse the cached schema on a match,
    // otherwise adopt the freshly inferred one (first page or drift).
    let mut fresh = infer_schema(&records)?;
    if mark_absent_fields(&mut records, &fresh) {
        let mut fields: Vec<_> = fresh.fields().iter().cloned().collect();
        fields.push(Arc::new(Field::new(ABSENT_MARKER, DataType::Utf8, true)));
        fresh = Arc::new(Schema::new(fields));
    }
    let schema = match &st.cached_schema {
        Some(s) if schema_eq(s, &fresh) => s.clone(),
        _ => {
            st.cached_schema = Some(fresh.clone());
            fresh
        }
    };
    let batch = json_to_record_batch(&records, schema)?;
    let batches = run_query_batches(st, batch)?;
    let mut rows = record_batches_to_json(&batches)?;
    strip_absent_fields(&mut rows);
    Ok(rows)
}

/// The columnar analogue of [`execute_page`]: feed an Arrow `RecordBatch`
/// straight to DuckDB and return the result as one `RecordBatch`, with no
/// `Value` materialization on either side.
fn execute_batch(st: &mut State, batch: RecordBatch) -> Result<RecordBatch, FaucetError> {
    if batch.num_rows() == 0 {
        // Empty page: pass through unchanged (the sink skips 0-row batches).
        return Ok(batch);
    }
    let batches = run_query_batches(st, batch)?;
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(std::sync::Arc::new(
            arrow::datatypes::Schema::empty(),
        )));
    }
    let schema = batches[0].schema();
    arrow::compute::concat_batches(&schema, &batches)
        .map_err(|e| FaucetError::Transform(format!("sql transform: concat result batches: {e}")))
}

/// DuckDB's fixed vector size — the maximum rows a single arrow array may carry
/// into the arrow vtab without tripping the capacity assertion (#372).
const DUCKDB_VECTOR_SIZE: usize = 2048;

/// Register `batch` as the temp table `batch`, splitting it into <=2048-row
/// slices so an oversized page never aborts the process (#372). The first slice
/// CREATEs the table, the rest INSERT into it — all landing in one relation, so
/// the downstream query is unaffected. `RecordBatch::slice` is zero-copy.
fn register_batch_chunked(st: &mut State, batch: RecordBatch) -> Result<(), FaucetError> {
    let total = batch.num_rows();
    let mut offset = 0;
    let mut first = true;
    // `execute_page` already returned early on empty input, but guard anyway so
    // a zero-row batch still CREATEs an empty `batch` table (matching the prior
    // single-CREATE behaviour) rather than leaving a stale one.
    loop {
        let len = (total - offset).min(DUCKDB_VECTOR_SIZE);
        let slice = batch.slice(offset, len);
        let params = arrow_recordbatch_to_query_params(slice);
        let sql = if first {
            "CREATE OR REPLACE TEMP TABLE batch AS SELECT * FROM arrow(?, ?)"
        } else {
            "INSERT INTO batch SELECT * FROM arrow(?, ?)"
        };
        st.conn
            .execute(sql, params)
            .map_err(|e| FaucetError::Transform(format!("sql transform: register batch: {e}")))?;
        first = false;
        offset += len;
        if offset >= total {
            break;
        }
    }
    Ok(())
}

fn reload_relations(st: &mut State) -> Result<(), FaucetError> {
    for r in st.reloadables.iter_mut() {
        let cur = std::fs::metadata(&r.path).and_then(|m| m.modified()).ok();
        if cur != r.last_mtime {
            let stmt = if r.is_csv {
                format!(
                    "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM read_csv_auto('{}', header={});",
                    r.name,
                    sql_escape(&r.path),
                    r.has_header
                )
            } else {
                format!(
                    "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM read_json_auto('{}', format='newline_delimited');",
                    r.name,
                    sql_escape(&r.path)
                )
            };
            st.conn.execute_batch(&stmt).map_err(|e| {
                FaucetError::Transform(format!("sql transform: reload '{}': {e}", r.name))
            })?;
            r.last_mtime = cur;
        }
    }
    Ok(())
}

fn plan_has_aggregate(conn: &Connection, query: &str) -> bool {
    let explain = format!("EXPLAIN {query}");
    let mut found = false;
    if let Ok(mut stmt) = conn.prepare(&explain)
        && let Ok(rows) = stmt.query_map([], |row| row.get::<_, String>(1))
    {
        for r in rows.flatten() {
            let u = r.to_uppercase();
            if u.contains("AGGREGATE") || u.contains("WINDOW") {
                found = true;
                break;
            }
        }
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SqlTransformConfig;
    use faucet_core::stage::{apply_stages_to_page, compile_stage};
    use serde_json::json;

    fn run(query: &str, rows: Vec<Value>) -> Vec<Value> {
        let cfg = SqlTransformConfig {
            query: query.into(),
            relations: vec![],
            memory_limit: None,
            threads: Some(1),
        };
        let stage = compile_stage(&SqlTransform::compile(&cfg).unwrap().into_page_stage()).unwrap();
        apply_stages_to_page(rows, std::slice::from_ref(&stage)).unwrap()
    }

    // #372: a page larger than DuckDB's 2048-row vector size used to abort the
    // whole process inside the arrow vtab. After chunked registration it must
    // pass through cleanly.
    #[test]
    fn large_page_passthrough_does_not_abort() {
        let rows: Vec<Value> = (0..10_000).map(|i| json!({"id": i, "v": i * 2})).collect();
        let out = run("SELECT * FROM batch", rows);
        assert_eq!(out.len(), 10_000, "every row of a >2048-row page survives");
        // Spot-check a row past the first vector boundary.
        assert_eq!(out[5_000]["id"], json!(5_000));
    }

    // Chunked registration lands every slice in one `batch` relation, so a
    // GROUP BY still aggregates over the whole page, not per-2048-chunk.
    #[test]
    fn large_page_aggregate_is_global_over_the_whole_page() {
        let rows: Vec<Value> = (0..5_000).map(|i| json!({"k": i % 4, "v": 1})).collect();
        let out = run(
            "SELECT k, COUNT(*) AS n FROM batch GROUP BY k ORDER BY k",
            rows,
        );
        assert_eq!(out.len(), 4, "one group per key");
        let total: i64 = out.iter().map(|r| r["n"].as_i64().unwrap()).sum();
        assert_eq!(total, 5_000, "every row counted exactly once across chunks");
        for r in &out {
            assert_eq!(r["n"], json!(1_250), "5000 rows / 4 keys = 1250 each");
        }
    }

    // The <=2048 fast path (single CREATE) is unchanged.
    #[test]
    fn small_page_still_works() {
        let out = run(
            "SELECT id FROM batch WHERE id >= 1 ORDER BY id",
            vec![json!({"id": 0}), json!({"id": 1}), json!({"id": 2})],
        );
        assert_eq!(out.len(), 2);
        assert_eq!(out[0]["id"], json!(1));
    }

    /// The columnar (#375) form: `into_columnar_stage` yields a `RecordBatch →
    /// RecordBatch` fn; feed a batch straight in and get one back, no `Value`.
    fn batch_fn(query: &str) -> faucet_core::stage::PageFnBatchBox {
        let cfg = SqlTransformConfig {
            query: query.into(),
            relations: vec![],
            memory_limit: None,
            threads: Some(1),
        };
        let (stage, batch) = SqlTransform::compile(&cfg).unwrap().into_columnar_stage();
        // The row form is a plain page stage; the batch form is what we test.
        assert!(matches!(stage, TransformStage::PageFn(_)));
        batch
    }

    #[test]
    fn columnar_batch_fn_transforms_record_batch() {
        let bf = batch_fn("SELECT id, v * 2 AS doubled FROM batch WHERE id >= 1 ORDER BY id");
        let input = faucet_core::columnar::values_to_record_batch_inferred(&[
            json!({"id": 0, "v": 5}),
            json!({"id": 1, "v": 10}),
            json!({"id": 2, "v": 20}),
        ])
        .unwrap();
        let out = bf(input).unwrap();
        let rows = faucet_core::columnar::record_batch_to_values(&out).unwrap();
        assert_eq!(rows.len(), 2, "WHERE id >= 1 keeps two rows");
        assert_eq!(rows[0]["id"], json!(1));
        assert_eq!(rows[0]["doubled"], json!(20));
        assert_eq!(rows[1]["doubled"], json!(40));
    }

    // #372 on the columnar path: a >2048-row inbound batch must not abort.
    #[test]
    fn columnar_batch_fn_handles_large_batch() {
        let bf = batch_fn("SELECT * FROM batch");
        let recs: Vec<Value> = (0..5_000).map(|i| json!({"id": i})).collect();
        let input = faucet_core::columnar::values_to_record_batch_inferred(&recs).unwrap();
        let out = bf(input).unwrap();
        assert_eq!(out.num_rows(), 5_000);
    }
}
