//! `faucet-conformance` checks for the Iceberg sink that count the table back
//! through the Iceberg source, run fully offline against a SQLite SQL catalog
//! + local warehouse (#878).
//!
//! Checks: 4 (idempotent replay — the snapshot-property watermark), 8 (schema
//! evolution), 13 (cancellation flushes) and 15 (per-row outcomes).

#[path = "iceberg_common/mod.rs"]
mod common;

use common::Lake;
use faucet_core::{Sink, Source};
use faucet_sink_iceberg::{IcebergSink, IcebergSinkConfig};
use faucet_source_iceberg::IcebergSource;
use serde_json::json;

async fn sink(lake: &Lake, table: &str) -> IcebergSink {
    let cfg: IcebergSinkConfig = serde_json::from_value(json!({
        "catalog": lake.catalog_json(),
        "namespace": ["db"],
        "table": table,
        "create_if_missing": true,
        "batch_size": 0
    }))
    .unwrap();
    IcebergSink::new(cfg).await.unwrap()
}

/// Rows committed in `table` (0 before the table exists).
async fn count(lake: &Lake, table: &str) -> usize {
    match IcebergSource::new(lake.source_config(table, json!({}))).await {
        Ok(source) => source.fetch_all().await.map(|r| r.len()).unwrap_or(0),
        Err(_) => 0,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conformance_idempotent_replay() {
    let lake = Lake::new();
    let sink = sink(&lake, "eo").await;
    let lake = &lake;
    faucet_conformance::assert_idempotent_replay(&sink, || count(lake, "eo")).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conformance_partial_outcomes_and_schema_evolution() {
    let lake = Lake::new();
    let sink = sink(&lake, "partial").await;
    let lake_ref = &lake;
    faucet_conformance::assert_partial_outcomes_truthful(&sink, || count(lake_ref, "partial"))
        .await;
    faucet_conformance::assert_schema_evolution_effective(&sink).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conformance_cancellation_flushes() {
    let lake = Lake::new();
    let sink = sink(&lake, "cancel").await;
    let lake = &lake;
    faucet_conformance::assert_cancellation_flushes(&sink, || count(lake, "cancel")).await;
    assert!(sink.supports_idempotent_writes());
}
