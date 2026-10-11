//! #862 H-04: a sandboxed sink writes its own database file but its instance
//! can no longer reach the host.

use faucet_core::Sink;
use faucet_sink_duckdb::{DuckdbColumnMapping, DuckdbSink, DuckdbSinkConfig};
use serde_json::json;

fn cfg(path: &str, table: &str) -> DuckdbSinkConfig {
    DuckdbSinkConfig::new(path, table)
        .column_mapping(DuckdbColumnMapping::AutoMap)
        .with_sandbox(true)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_sandboxed_sink_writes_its_file_and_nothing_else() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("out.duckdb");
    let path = path.to_str().expect("utf-8 path");
    let outside = dir.path().join("leak.csv");

    let sink = DuckdbSink::new(cfg(path, "t")).await.expect("sink");
    let again = DuckdbSink::new(cfg(path, "u")).await.expect("second sink");
    sink.write_batch(&[json!({"id": 1}), json!({"id": 2})])
        .await
        .expect("write");
    again
        .write_batch(&[json!({"id": 3})])
        .await
        .expect("write u");
    sink.flush().await.expect("flush");
    assert_eq!(sink.scalar_count("t").await.expect("count"), 2);

    let err = sink
        .run_sql(&format!("COPY t TO '{}'", outside.display()))
        .await
        .expect_err("COPY TO a host file is refused");
    assert!(err.to_string().contains("disabled"), "{err}");
    assert!(!outside.exists());
    let err = sink
        .run_sql("SET enable_external_access=true")
        .await
        .expect_err("the sandbox cannot be lifted");
    assert!(err.to_string().contains("lock"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn sandboxing_an_instance_locked_open_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("locked.duckdb");
    let path = path.to_str().expect("utf-8 path");
    let open = DuckdbSink::new(DuckdbSinkConfig::new(path, "t"))
        .await
        .expect("open sink");
    open.run_sql("SET lock_configuration=true")
        .await
        .expect("lock");
    let err = DuckdbSink::new(cfg(path, "t"))
        .await
        .err()
        .expect("refused");
    assert!(err.to_string().contains("locked"), "{err}");
}
