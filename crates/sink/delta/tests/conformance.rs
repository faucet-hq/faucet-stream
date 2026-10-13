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

// ── Descriptor agreement (RFC 0013) ───────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
async fn conformance_descriptor_matches_built_sink() {
    let dir = tempfile::tempdir().unwrap();
    let v = serde_json::json!({ "table_uri": table_uri(&dir, "descriptor") });
    let cfg: DeltaSinkConfig = serde_json::from_value(v.clone()).unwrap();
    let built = DeltaSink::new(cfg).await.expect("sink");
    faucet_conformance::assert_descriptor_matches_sink(
        &faucet_sink_delta::DESCRIPTOR,
        "delta",
        &v,
        &built,
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

// ── Check: backend calls honour the call deadline and cancel (#880) ──────────

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(f))
}

fn hung_table(endpoint: &str) -> serde_json::Value {
    serde_json::json!({
        "table_uri": "s3://hung/table",
        "storage_options": {
            "AWS_ENDPOINT_URL": endpoint,
            "AWS_ACCESS_KEY_ID": "k",
            "AWS_SECRET_ACCESS_KEY": "s",
            "AWS_REGION": "us-east-1",
            "AWS_ALLOW_HTTP": "true",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
        }
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_call_deadline() {
    let hung = faucet_conformance::hung::SilentListener::start()
        .await
        .expect("listener");
    let v = hung_table(&hung.http_url());
    faucet_conformance::calls::assert_sink_call_deadline(
        || {
            let cfg: faucet_sink_delta::DeltaSinkConfig =
                serde_json::from_value(v.clone()).expect("config");
            block_on(faucet_sink_delta::DeltaSink::new(cfg)).expect("sink")
        },
        &[serde_json::json!({"id": 1})],
    )
    .await;
}
