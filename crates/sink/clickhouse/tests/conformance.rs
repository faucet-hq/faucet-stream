//! `faucet-conformance` battery for the ClickHouse sink.
//!
//! Check 1 (config schema) is pure/offline and always runs. Check 5
//! (capabilities truthful) **auto-starts** a real `clickhouse/clickhouse-server`
//! container via `testcontainers`; it skips cleanly when Docker is unavailable
//! and runs for real in CI. The ClickHouse sink is append-only (no
//! idempotent-watermark / keyed-upsert mechanism), so check 5 takes the
//! honest-`false` branch: Append works and no phantom commit token is recorded.
//! Passing this battery in CI is the Tier-1 (supported) criterion.

mod common;

use faucet_conformance::{
    assert_capabilities_truthful, assert_config_schema_valid_value,
    assert_connector_name_nonempty_value,
};
use faucet_core::Sink as _;
use faucet_sink_clickhouse::{ClickHouseSink, ClickHouseSinkConfig};
use serde_json::Value;
use testcontainers_modules::clickhouse::ClickHouse;
use testcontainers_modules::testcontainers::ContainerAsync;

/// Start a ClickHouse container, or `None` when Docker is unavailable.
/// A missing test backend: a skip locally, a failure when CI requires the
/// backends (`FAUCET_REQUIRE_BACKENDS`).
fn backend_missing(why: &str) {
    if std::env::var("FAUCET_REQUIRE_BACKENDS").is_ok() {
        panic!("{why} (FAUCET_REQUIRE_BACKENDS is set)");
    }
    eprintln!("skipping: {why}");
}

async fn start_clickhouse() -> Option<(ContainerAsync<ClickHouse>, String)> {
    match common::start_clickhouse().await {
        Ok(started) => Some(started),
        // CI sets FAUCET_REQUIRE_BACKENDS so a missing backend fails, not skips.
        Err(e) if std::env::var("FAUCET_REQUIRE_BACKENDS").is_ok() => {
            panic!("ClickHouse container did not start and FAUCET_REQUIRE_BACKENDS is set: {e}")
        }
        Err(_) => None,
    }
}

async fn http_exec(base: &str, sql: &str) {
    let resp = reqwest::Client::new()
        .post(base)
        .body(sql.to_string())
        .send()
        .await
        .expect("http exec send");
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    assert!(
        status.is_success(),
        "statement failed ({status}): {sql}\n{body}"
    );
}

/// Distinct rows currently in `cap` (independent read path).
async fn count_rows(base: &str) -> usize {
    let body = reqwest::Client::new()
        .post(base)
        .body("SELECT count() AS c FROM cap FORMAT JSONEachRow".to_string())
        .send()
        .await
        .expect("count send")
        .text()
        .await
        .expect("count body");
    body.lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .filter_map(|v| {
            v["c"]
                .as_str()
                .and_then(|s| s.parse::<usize>().ok())
                .or_else(|| v["c"].as_u64().map(|n| n as usize))
        })
        .next()
        .unwrap_or(0)
}

// ── Check 1: config schema validity (pure, offline) ──────────────────────────
#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(ClickHouseSinkConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "clickhouse");
}

// ── Check 10: connector_name is non-empty (offline, lazy sink) ───────────────
#[test]
fn conformance_connector_name_nonempty() {
    let sink =
        ClickHouseSink::new(ClickHouseSinkConfig::new("http://127.0.0.1:1", "t")).expect("sink");
    faucet_conformance::assert_batch_atomicity_declared(&sink);
    assert_connector_name_nonempty_value(sink.connector_name(), sink.connector_name());
}

// ── Check 5: capabilities are truthful (real backend, skip if no Docker) ─────
#[tokio::test(flavor = "multi_thread")]
async fn conformance_capabilities_truthful() {
    let Some((_c, base)) = start_clickhouse().await else {
        backend_missing("clickhouse conformance_capabilities_truthful: Docker unavailable");
        return;
    };

    // The battery's records are keyed `{id, v}`; a plain MergeTree append table
    // matches them (no upsert semantics — the sink is append-only).
    http_exec(
        &base,
        "CREATE TABLE cap (id Int64, v String) ENGINE = MergeTree ORDER BY id",
    )
    .await;

    let sink = ClickHouseSink::new(ClickHouseSinkConfig::new(&base, "cap")).expect("sink");
    let base_ref = base.as_str();
    assert_capabilities_truthful(&sink, move || async move {
        // Default (non-async) INSERT is durable on ack, so rows are countable
        // immediately after write_batch.
        count_rows(base_ref).await
    })
    .await;

    assert!(!sink.supports_idempotent_writes());
    assert!(!sink.dedups_by_key());
}
