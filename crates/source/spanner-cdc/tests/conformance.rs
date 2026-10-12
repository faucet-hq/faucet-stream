//! `faucet-conformance` battery for the Spanner change-streams source.
//!
//! Check 1 (config-schema validity) is pure and offline. Checks 2 (bounded
//! memory), 3 (bookmark round-trip), 6 (errors, not panics) and 11 (preflight
//! `check()`) run against the Cloud Spanner emulator via testcontainers and
//! skip cleanly when Docker is unavailable.

mod support;

use faucet_conformance::{
    assert_bookmark_roundtrip, assert_bounded_memory, assert_config_schema_valid_value,
    assert_connector_name_nonempty, assert_errors_not_panics, assert_preflight_check_wellformed,
};
use faucet_source_spanner_cdc::{SpannerCdcSource, SpannerCdcSourceConfig};
use serde_json::json;

const TABLE_DDL: &str = "CREATE TABLE nums (id INT64 NOT NULL, v STRING(MAX)) PRIMARY KEY (id)";
const STREAM_DDL: &str = "CREATE CHANGE STREAM cs FOR nums";

// ── Check 1: config schema ──────────────────────────────────────────────────

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(SpannerCdcSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-spanner-cdc");
}

/// Insert `n` rows in transactions of 100 rows each (so a transaction always
/// fits inside the page cap used by the bounded-memory check).
async fn seed(client: &gcloud_spanner::client::Client, n: usize) {
    for start in (0..n).step_by(100) {
        let values: Vec<String> = (start..(start + 100).min(n))
            .map(|i| format!("({i}, 'v{i}')"))
            .collect();
        support::execute_dml(
            client,
            &format!("INSERT INTO nums (id, v) VALUES {}", values.join(", ")),
        )
        .await;
    }
}

// ── Check 2: bounded-memory streaming (Docker) ──────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn conformance_bounded_memory() {
    let Some(emu) = support::start_emulator().await else {
        return;
    };
    support::create_database(&emu.host, "bounded", &[TABLE_DDL, STREAM_DDL]).await;
    let client = support::raw_client(&emu.host, "bounded").await;
    let ts0 = support::emulator_now(&client, false).await;
    seed(&client, 2_000).await;

    let mut cfg = support::cdc_config(
        &emu.host,
        "bounded",
        "cs",
        json!({"type": "timestamp", "timestamp": ts0}),
    );
    cfg.batch_size = 500;
    let source = SpannerCdcSource::new(cfg).await.expect("source");
    assert_connector_name_nonempty(&source);

    // Wait until the whole set is readable in one run (host/VM clock headroom),
    // then let the battery drive a fresh run from the same start.
    let all = support::collect(&source, 2_000, std::time::Duration::from_secs(120)).await;
    assert_eq!(all.len(), 2_000);
    let fresh = SpannerCdcSource::new({
        let mut c = support::cdc_config(
            &emu.host,
            "bounded",
            "cs",
            json!({"type": "timestamp", "timestamp": ts0}),
        );
        c.batch_size = 500;
        c
    })
    .await
    .expect("source");
    assert_bounded_memory(&fresh, 500, 2_000).await;
}

// ── Check 3: bookmark round-trip (Docker) ───────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn conformance_bookmark_roundtrip() {
    let Some(emu) = support::start_emulator().await else {
        return;
    };
    support::create_database(&emu.host, "resume", &[TABLE_DDL, STREAM_DDL]).await;
    let client = support::raw_client(&emu.host, "resume").await;
    let ts0 = support::emulator_now(&client, false).await;
    seed(&client, 300).await;
    // Make sure the changes are inside the first run's window.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    let source = SpannerCdcSource::new(support::cdc_config(
        &emu.host,
        "resume",
        "cs",
        json!({"type": "timestamp", "timestamp": ts0}),
    ))
    .await
    .expect("source");
    assert_bookmark_roundtrip(&source).await;
}

// ── Check 11: preflight check() (Docker) ────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn conformance_preflight_check_is_wellformed() {
    let Some(emu) = support::start_emulator().await else {
        return;
    };
    support::create_database(&emu.host, "preflight", &[TABLE_DDL, STREAM_DDL]).await;
    let source = SpannerCdcSource::new(support::cdc_config(
        &emu.host,
        "preflight",
        "cs",
        json!({"type": "now"}),
    ))
    .await
    .expect("source");
    assert_preflight_check_wellformed(&source, &faucet_core::check::CheckContext::default()).await;
}

// ── Check 6: errors are typed, not panics (Docker) ──────────────────────────

/// The source builds against a live emulator, then fails at read: a start
/// position older than the stream is rejected by Spanner with `OutOfRange`.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_errors_not_panics() {
    let Some(emu) = support::start_emulator().await else {
        return;
    };
    support::create_database(&emu.host, "errors", &[TABLE_DDL, STREAM_DDL]).await;
    let source = SpannerCdcSource::new(support::cdc_config(
        &emu.host,
        "errors",
        "cs",
        json!({"type": "timestamp", "timestamp": "2020-01-01T00:00:00Z"}),
    ))
    .await
    .expect("builds against a live emulator");
    assert_errors_not_panics(&source).await;
}
