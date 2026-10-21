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
