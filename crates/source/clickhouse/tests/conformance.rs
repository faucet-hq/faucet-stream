//! `faucet-conformance` battery for the ClickHouse source.
//!
//! Checks 1 (config schema) and 6 (errors, not panics) are pure/offline and
//! always run. Check 2 (bounded-memory streaming) **auto-starts** a real
//! `clickhouse/clickhouse-server` container via `testcontainers`; it skips
//! cleanly when Docker is unavailable (so `cargo test` passes on a Docker-less
//! host) and runs for real in CI, where Docker is present. Passing this battery
//! in CI is the Tier-1 (supported) criterion.

mod common;

use faucet_conformance::{
    assert_bounded_memory, assert_config_schema_valid_value, assert_connector_name_nonempty,
    assert_errors_not_panics,
};
use faucet_source_clickhouse::{ClickHouseSource, ClickHouseSourceConfig};
use testcontainers_modules::clickhouse::ClickHouse;
use testcontainers_modules::testcontainers::ContainerAsync;

/// Start a ClickHouse container, or `None` (skip) when Docker is unavailable.
async fn start_clickhouse() -> Option<(ContainerAsync<ClickHouse>, String)> {
    common::start_clickhouse()
        .await
        .map_err(|e| faucet_conformance::containers::backend_missing(&e.to_string()))
        .ok()
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

// ── Check 1: config schema validity (pure, offline) ──────────────────────────
#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(ClickHouseSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "clickhouse");
}

// ── Check 6: errors, not panics (offline, unreachable endpoint) ──────────────
#[tokio::test(flavor = "multi_thread")]
async fn conformance_errors_not_panics() {
    // `new()` is lazy (builds a reqwest client, no I/O); the query surfaces a
    // typed `FaucetError` against a refused connection, never a panic.
    let source = ClickHouseSource::new(ClickHouseSourceConfig::new(
        "http://127.0.0.1:1",
        "SELECT 1",
    ))
    .expect("source builds lazily");
    // Check 10: connector_name() is non-empty (reuses this offline instance).
    assert_connector_name_nonempty(&source);
    assert_errors_not_panics(&source).await;
}

// ── Check 2: bounded-memory streaming (real backend, skip if no Docker) ──────
#[tokio::test(flavor = "multi_thread")]
async fn conformance_bounded_memory() {
    let Some((_c, base)) = start_clickhouse().await else {
        return;
    };

    http_exec(
        &base,
        "CREATE TABLE nums (n UInt32) ENGINE = MergeTree ORDER BY n",
    )
    .await;
    // Seed 250 rows in one JSONEachRow insert.
    let vals: Vec<String> = (1..=250).map(|n| format!("{{\"n\":{n}}}")).collect();
    http_exec(
        &base,
        &format!("INSERT INTO nums FORMAT JSONEachRow {}", vals.join(" ")),
    )
    .await;

    // ClickHouse pages via its config `batch_size` (the stream_pages arg is a
    // hint), so set it to 50: 250 rows → pages of 50, bounded.
    let source = ClickHouseSource::new(
        ClickHouseSourceConfig::new(&base, "SELECT n FROM nums ORDER BY n").with_batch_size(50),
    )
    .expect("source");
    assert_bounded_memory(&source, 50, 250).await;
}
