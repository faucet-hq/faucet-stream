//! `faucet-conformance` battery for the Delta Lake sink.
//!
//! Runs **entirely on the local filesystem** (no Docker): the sink writes to a
//! temp-dir Delta table and the destination is counted back through
//! [`DeltaSource`]. Passing this battery in CI is the Tier-1 (supported)
//! criterion — see the connector catalog's "Support tiers" note.
//!
//! Checks exercised: 1 (config schema, offline), 5 (capabilities truthful), 10
//! (`connector_name()` non-empty), and 11 (`check()` well-formed). The Delta
//! sink is append-only (no idempotent-watermark / keyed-upsert mechanism), so
//! check 5 takes the honest-`false` branch: Append is advertised and works, and
//! no phantom commit token is recorded. Checks 7/8 (upsert/evolution) do not
//! apply — it advertises neither.

use faucet_conformance::{assert_capabilities_truthful, assert_config_schema_valid_value};
use faucet_core::{Sink as _, Source as _};
use faucet_sink_delta::{DeltaSink, DeltaSinkConfig};
use faucet_source_delta::{DeltaSource, DeltaSourceConfig};

fn table_uri(dir: &tempfile::TempDir, name: &str) -> String {
    dir.path().join(name).to_string_lossy().into_owned()
}

/// Distinct rows currently committed in the table (0 before the first commit).
async fn count_committed(uri: &str) -> usize {
    match DeltaSource::new(DeltaSourceConfig::new(uri)).await {
        Ok(src) => src.fetch_all().await.map(|r| r.len()).unwrap_or(0),
        Err(_) => 0,
    }
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

// ── Check 5: capabilities are truthful ───────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
async fn conformance_capabilities_truthful() {
    let dir = tempfile::tempdir().unwrap();
    let uri = table_uri(&dir, "cap");

    let sink = DeltaSink::new(DeltaSinkConfig::new(&uri))
        .await
        .expect("sink");

    // The sink buffers via delta-rs's RecordBatchWriter and only commits on
    // flush(), so the count closure flushes before reading the table back. A
    // repeated flush on an already-committed writer is a clean no-op.
    let sink_ref = &sink;
    let uri_ref = uri.as_str();
    assert_capabilities_truthful(&sink, move || async move {
        sink_ref.flush().await.expect("flush");
        count_committed(uri_ref).await
    })
    .await;

    // The honest branch must have left the append-only sink non-idempotent.
    assert!(!sink.supports_idempotent_writes());
    assert!(!sink.dedups_by_key());
}
