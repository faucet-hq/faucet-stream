//! `faucet-conformance` battery for the Iceberg source, run fully offline
//! against a SQLite SQL catalog + local warehouse seeded by the Iceberg sink.
//!
//! Checks: 1 (config schema), 2 (bounded memory), 3 (bookmark round-trip in
//! `mode: incremental`), 6 (errors, not panics), 9 (`batch_size = 0` single
//! page), 10 (`connector_name`), 11 (`check()` well-formed), 12 (discover
//! round-trip).

mod common;

use common::Lake;
use faucet_conformance::{
    assert_batch_size_zero_single_page, assert_bookmark_roundtrip, assert_bounded_memory,
    assert_config_schema_valid_value, assert_connector_name_nonempty, assert_discover_roundtrips,
    assert_errors_not_panics, assert_preflight_check_wellformed,
};
use faucet_core::Source;
use faucet_source_iceberg::{IcebergSource, IcebergSourceConfig};
use serde_json::json;

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(IcebergSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "iceberg");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conformance_bounded_memory() {
    let lake = Lake::new();
    lake.append("events", 0..250).await;
    let source = lake.source("events", json!({ "batch_size": 50 })).await;
    assert_bounded_memory(&source, 50, 250).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conformance_bookmark_roundtrip() {
    let lake = Lake::new();
    lake.append("events", 0..20).await;
    let source = lake
        .source("events", json!({ "mode": "incremental" }))
        .await;
    assert_bookmark_roundtrip(&source).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conformance_errors_not_panics() {
    let lake = Lake::new();
    let source = lake.source("missing", json!({})).await;
    assert_errors_not_panics(&source).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conformance_batch_size_zero_single_page() {
    let lake = Lake::new();
    lake.append("events", 0..6).await;
    lake.append("events", 6..9).await;
    let source = lake.source("events", json!({ "batch_size": 0 })).await;
    assert_batch_size_zero_single_page(&source).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conformance_connector_name_and_preflight() {
    let lake = Lake::new();
    lake.append("events", 0..3).await;
    let source = lake.source("events", json!({})).await;
    assert_connector_name_nonempty(&source);
    assert_preflight_check_wellformed(&source, &faucet_core::check::CheckContext::default()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conformance_discover_roundtrips() {
    let lake = Lake::new();
    lake.append("a", 0..2).await;
    lake.append("b", 0..2).await;
    let source = lake.source("a", json!({})).await;
    let lake = &lake;
    assert_discover_roundtrips(&source, |patch| async move {
        let mut v = lake.source_config("a", json!({}));
        v.table = patch["table"].as_str().unwrap().to_string();
        let rebuilt: Box<dyn Source> = Box::new(IcebergSource::new(v).await.unwrap());
        rebuilt
    })
    .await;
}
