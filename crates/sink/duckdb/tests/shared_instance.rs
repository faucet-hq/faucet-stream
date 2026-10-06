//! #789 SQL-07: two sinks on one database file in one process share a DuckDB
//! instance, so neither loses the other's committed data when they close.

use faucet_core::Sink;
use faucet_sink_duckdb::{DuckdbColumnMapping, DuckdbSink, DuckdbSinkConfig};
use serde_json::json;

fn cfg(path: &str, table: &str) -> DuckdbSinkConfig {
    DuckdbSinkConfig::new(path, table).column_mapping(DuckdbColumnMapping::AutoMap)
}

fn count(path: &str, table: &str) -> i64 {
    let conn = duckdb::Connection::open(path).expect("reopen");
    conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
        .expect("count")
}

#[tokio::test(flavor = "multi_thread")]
async fn two_sinks_on_one_file_keep_each_others_tables() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("shared.duckdb");
    let path = path.to_str().expect("utf-8 path");

    let a = DuckdbSink::new(cfg(path, "a")).await.expect("sink a");
    let b = DuckdbSink::new(cfg(path, "b")).await.expect("sink b");
    a.write_batch(&[json!({"id": 1}), json!({"id": 2})])
        .await
        .expect("write a");
    b.write_batch(&[json!({"id": 3})]).await.expect("write b");
    a.flush().await.expect("flush a");
    b.flush().await.expect("flush b");
    drop(a);
    let late = DuckdbSink::new(cfg(path, "c")).await.expect("sink c");
    late.write_batch(&[json!({"id": 4})])
        .await
        .expect("write c");
    drop(b);
    drop(late);

    assert_eq!(count(path, "a"), 2);
    assert_eq!(count(path, "b"), 1);
    assert_eq!(count(path, "c"), 1);

    let relative = DuckdbSink::new(cfg("./target-relative-does-not-exist/x.duckdb", "t")).await;
    assert!(
        relative.is_err(),
        "a missing directory is still an open error"
    );
}
