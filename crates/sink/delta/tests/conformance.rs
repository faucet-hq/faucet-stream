//! `faucet-conformance` battery for the Delta Lake sink.
//!
//! Runs **entirely on the local filesystem** (no Docker). Passing this battery
//! in CI is the Tier-1 (supported) criterion — see the connector catalog's
//! "Support tiers" note.
//!
//! Checks exercised: 1 (config schema, offline), 10 (`connector_name()`
//! non-empty), and 11 (`check()` well-formed). Check 5 (capabilities truthful)
//! counts the table back through the Delta source, so it lives in
//! `crates/interop-tests/tests/delta_conformance.rs`.

use faucet_conformance::assert_config_schema_valid_value;
use faucet_core::Sink as _;
use faucet_sink_delta::{DeltaSink, DeltaSinkConfig};

fn table_uri(dir: &tempfile::TempDir, name: &str) -> String {
    dir.path().join(name).to_string_lossy().into_owned()
}

// ── Check 1: config schema validity (pure, offline) ──────────────────────────
#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(DeltaSinkConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "delta");
}

// ── Check 10: connector_name is non-empty ─────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
async fn conformance_connector_name_nonempty() {
    let dir = tempfile::tempdir().unwrap();
    let uri = table_uri(&dir, "name");
    let sink = DeltaSink::new(DeltaSinkConfig::new(&uri))
        .await
        .expect("sink");
    faucet_conformance::assert_batch_atomicity_declared(&sink);
    faucet_conformance::assert_connector_name_nonempty_value(
        sink.connector_name(),
        sink.connector_name(),
    );
}

// ── Check 11: preflight check() is well-formed ────────────────────────────────
/// A reachable local warehouse makes the metadata-open probe pass (the table
/// need not exist yet); the check must return Ok(report) with a well-formed
/// probe.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_preflight_check_wellformed() {
    let dir = tempfile::tempdir().unwrap();
    let uri = table_uri(&dir, "preflight");
    let sink = DeltaSink::new(DeltaSinkConfig::new(&uri))
        .await
        .expect("sink");
    faucet_conformance::assert_sink_preflight_check_wellformed(
        &sink,
        &faucet_core::check::CheckContext::default(),
    )
    .await;
}
