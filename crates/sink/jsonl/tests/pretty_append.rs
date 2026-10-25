#![allow(deprecated)]
//! A pretty append run repairs a torn multi-line record left by an earlier
//! interrupted run, reading only the end of the file.

use faucet_core::Sink;
use faucet_sink_jsonl::{JsonlSink, JsonlSinkConfig};
use serde_json::json;

#[tokio::test]
async fn a_pretty_append_run_cuts_a_torn_record_larger_than_a_read_chunk() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out.jsonl");
    let torn = format!("{{\n  \"blob\": \"{}\",\n", "z".repeat(40_000));
    std::fs::write(&path, format!("{{\n  \"id\": 1\n}}\n{torn}")).unwrap();

    let sink = JsonlSink::new(JsonlSinkConfig::new(&path).append(true).pretty(true));
    sink.write_batch(&[json!({"id": 2})]).await.unwrap();
    sink.flush().await.unwrap();
    sink.write_batch(&[json!({"id": 3})]).await.unwrap();
    sink.flush().await.unwrap();

    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        "{\n  \"id\": 1\n}\n{\n  \"id\": 2\n}\n{\n  \"id\": 3\n}\n"
    );
}
