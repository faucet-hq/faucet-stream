//! Temporal columns render at the column's precision, whatever the value.

use faucet_core::Source as _;
use faucet_source_duckdb::{DuckdbSource, DuckdbSourceConfig};
use serde_json::json;
use tempfile::TempDir;

#[tokio::test]
async fn timestamps_and_times_keep_a_fixed_fraction_width() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("t.duckdb");
    {
        let conn = duckdb::Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE t (id INT, ts TIMESTAMP, tz TIMESTAMPTZ, t TIME); \
             INSERT INTO t VALUES \
               (1, '2024-01-01 10:00:00', '2024-01-01 10:00:00+00', '10:00:00'), \
               (2, '2024-01-01 10:00:00.000001', '2024-01-01 10:00:00.5+00', '10:00:00.5')",
        )
        .unwrap();
    }
    let rows = DuckdbSource::new(DuckdbSourceConfig::new(
        path.to_string_lossy(),
        "SELECT ts, tz, t FROM t ORDER BY id",
    ))
    .await
    .unwrap()
    .fetch_all()
    .await
    .unwrap();
    assert_eq!(
        rows,
        vec![
            json!({"ts": "2024-01-01T10:00:00.000000", "tz": "2024-01-01T10:00:00.000000Z", "t": "10:00:00.000000"}),
            json!({"ts": "2024-01-01T10:00:00.000001", "tz": "2024-01-01T10:00:00.500000Z", "t": "10:00:00.500000"}),
        ]
    );
}

/// A parameterised query runs once: the column-type probe does not execute it.
#[tokio::test]
async fn a_bound_query_is_executed_once() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("s.duckdb");
    {
        let conn = duckdb::Connection::open(&path).unwrap();
        conn.execute_batch("CREATE SEQUENCE seq").unwrap();
    }
    let source = DuckdbSource::new(DuckdbSourceConfig::new(
        path.to_string_lossy(),
        "SELECT nextval('seq') AS n, {tag} AS tag",
    ))
    .await
    .unwrap();
    let context = std::collections::HashMap::from([("tag".to_string(), json!("a"))]);
    let rows = source.fetch_with_context(&context).await.unwrap();
    assert_eq!(rows, vec![json!({"n": 1, "tag": "a"})]);
}
