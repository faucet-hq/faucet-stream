//! `faucet-conformance` battery — Check 1 (config schema validity) and
//! Check 10 (connector_name non-empty). Passing this battery in CI is the
//! Tier-1 (supported) criterion.
use faucet_conformance::assert_config_schema_valid_value;
use faucet_core::Sink as _;
use faucet_sink_gcs::{GcsCredentials, GcsSink, GcsSinkConfig};

#[test]
fn conformance_config_schema_valid() {
    let schema =
        serde_json::to_value(schemars::schema_for!(faucet_sink_gcs::GcsSinkConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "gcs");
}

// ── Check 10: connector_name is non-empty (offline, lazy build) ──────────────
/// Building the GCS storage client with `Anonymous` creds + an endpoint
/// override performs no I/O, so this runs unconditionally (no emulator needed).
#[tokio::test(flavor = "multi_thread")]
async fn conformance_connector_name_nonempty() {
    let config = GcsSinkConfig::new("does-not-exist")
        .auth(GcsCredentials::Anonymous)
        .storage_host("http://127.0.0.1:1");
    let sink = GcsSink::new(config).await.expect("sink builds lazily");
    faucet_conformance::assert_batch_atomicity_declared(&sink);
    faucet_conformance::assert_connector_name_nonempty_value(
        sink.connector_name(),
        sink.connector_name(),
    );
}
