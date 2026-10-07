//! #789 SQL-82 / SQL-91: later fields become columns (or fail loudly) and
//! offset-bearing timestamps land in UTC in tz-less TIMESTAMP columns.

use faucet_core::Sink;
use faucet_sink_duckdb::{DuckdbColumnMapping, DuckdbSink, DuckdbSinkConfig};
use serde_json::json;

fn cfg(path: &str, table: &str) -> DuckdbSinkConfig {
    DuckdbSinkConfig::new(path, table).column_mapping(DuckdbColumnMapping::AutoMap)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_later_field_becomes_a_column() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("cols.duckdb");
    let path = path.to_str().expect("utf-8 path");
    let sink = DuckdbSink::new(cfg(path, "t")).await.expect("sink");
    sink.write_batch(&[json!({"id": 1})]).await.expect("page 1");
    sink.write_batch(&[json!({"id": 2, "extra": "x"})])
        .await
        .expect("page 2");
    sink.flush().await.expect("flush");
    drop(sink);
    let conn = duckdb::Connection::open(path).expect("reopen");
    let extra: String = conn
        .query_row("SELECT extra FROM t WHERE id = 2", [], |r| r.get(0))
        .expect("extra landed");
    assert_eq!(extra, "x");
}

#[tokio::test(flavor = "multi_thread")]
async fn without_create_table_an_unknown_field_fails() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("fixed.duckdb");
    let path = path.to_str().expect("utf-8 path");
    {
        let conn = duckdb::Connection::open(path).expect("open");
        conn.execute_batch("CREATE TABLE t (id BIGINT, ts TIMESTAMP)")
            .expect("create");
    }
    let sink = DuckdbSink::new(cfg(path, "t").with_create_table(false))
        .await
        .expect("sink");
    let err = sink
        .write_batch(&[json!({"id": 1, "surprise": true})])
        .await
        .expect_err("no column");
    assert!(err.to_string().contains("surprise"), "{err}");

    sink.write_batch(&[
        json!({"id": 2, "ts": "2024-01-02T03:04:05-08:00"}),
        json!({"id": 3, "ts": "2024-01-02 03:04:05"}),
    ])
    .await
    .expect("timestamps");
    sink.flush().await.expect("flush");
    drop(sink);
    let conn = duckdb::Connection::open(path).expect("reopen");
    let ts: Vec<String> = conn
        .prepare("SELECT CAST(ts AS VARCHAR) FROM t ORDER BY id")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(ts, vec!["2024-01-02 11:04:05", "2024-01-02 03:04:05"]);
}
