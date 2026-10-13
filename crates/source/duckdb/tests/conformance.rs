//! Runs the reusable `faucet-conformance` battery against the real DuckDB query
//! source, seeded from a tempfile database — no Docker required.
//!
//! - check 1 `assert_config_schema_valid`
//! - check 2 `assert_bounded_memory` (real paging over a seeded table)
//! - check 6 `assert_errors_not_panics` (query against a missing table → typed
//!   error, no panic)
//!
//! DuckDB query is full-table (no bookmark), so check 3 does not apply; checks
//! 4/5 are sink-only.

use duckdb::Connection;
use faucet_source_duckdb::{DuckdbSource, DuckdbSourceConfig};
use tempfile::TempDir;

fn seed(rows: usize) -> (TempDir, String) {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("t.duckdb");
    let path_str = path.to_string_lossy().into_owned();
    {
        let conn = Connection::open(&path).expect("open");
        conn.execute_batch("CREATE TABLE t (id INTEGER, name TEXT)")
            .expect("create");
        // range(0, rows) → one row per id; append the name column.
        conn.execute_batch(&format!(
            "INSERT INTO t SELECT i AS id, 'row-' || i AS name FROM range(0, {rows}) t(i)"
        ))
        .expect("insert");
    } // connection dropped → file released for the source to open
    (dir, path_str)
}

#[tokio::test]
async fn conformance_config_schema_valid() {
    let (_dir, path) = seed(1);
    let source = DuckdbSource::new(DuckdbSourceConfig::new(path, "SELECT * FROM t"))
        .await
        .expect("source");
    faucet_conformance::assert_config_schema_valid(&source);
    // Check 10: connector_name() is non-empty (reuses this offline instance).
    faucet_conformance::assert_connector_name_nonempty(&source);
}

#[tokio::test]
async fn conformance_batch_size_zero_single_page() {
    // Check 9: a source built with `batch_size = 0` yields the whole result set
    // as a single page (the DuckDB "no batching" sentinel).
    let total = 200;
    let (_dir, path) = seed(total);
    let source = DuckdbSource::new(
        DuckdbSourceConfig::new(path, "SELECT id, name FROM t ORDER BY id").with_batch_size(0),
    )
    .await
    .expect("source");
    faucet_conformance::assert_batch_size_zero_single_page(&source).await;
}

#[tokio::test]
async fn conformance_bounded_memory() {
    let total = 500;
    let batch = 100;
    let (_dir, path) = seed(total);
    let source = DuckdbSource::new(
        DuckdbSourceConfig::new(path, "SELECT id, name FROM t ORDER BY id").with_batch_size(batch),
    )
    .await
    .expect("source");
    faucet_conformance::assert_bounded_memory(&source, batch, total).await;
}

#[tokio::test]
async fn conformance_errors_not_panics() {
    // Valid database, but the query references a table that does not exist — the
    // read path must surface a typed FaucetError, never a panic.
    let (_dir, path) = seed(1);
    let source = DuckdbSource::new(DuckdbSourceConfig::new(path, "SELECT * FROM missing_table"))
        .await
        .expect("source builds; the query only fails at read time");
    faucet_conformance::assert_errors_not_panics(&source).await;
}

#[tokio::test]
async fn descriptor_matches_the_built_source() {
    for v in [
        serde_json::json!({"database": ":memory:", "query": "SELECT 1"}),
        serde_json::json!({"database": ":memory:", "query": "SELECT 1", "batch_size": 0}),
    ] {
        let cfg: DuckdbSourceConfig = serde_json::from_value(v.clone()).unwrap();
        let built = DuckdbSource::new(cfg).await.expect("source");
        faucet_conformance::assert_descriptor_matches_source(
            &faucet_source_duckdb::DESCRIPTOR,
            "duckdb",
            &v,
            &built,
        );
    }
}

/// A query that would run for hours stands in for a hung backend (#880): the
/// call ends at the deadline and on cancel, and interrupts the query.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_call_deadline() {
    let mut built = Vec::new();
    for _ in 0..2 {
        built.push(
            DuckdbSource::new(DuckdbSourceConfig::new(
                ":memory:",
                "SELECT count(*) AS n FROM range(1000000000000) a",
            ))
            .await
            .expect("source"),
        );
    }
    let built = std::sync::Mutex::new(built);
    faucet_conformance::calls::assert_source_call_deadline(|| built.lock().unwrap().pop().unwrap())
        .await;
}
