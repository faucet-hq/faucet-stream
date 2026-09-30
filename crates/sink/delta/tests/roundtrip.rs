//! Delta sink tests against a real table on the local filesystem. The
//! round trips that read the table back with the Delta source live in
//! `crates/interop-tests/tests/delta_roundtrip.rs`.

use faucet_core::Sink;
use faucet_sink_delta::{DeltaSink, DeltaSinkConfig};
use serde_json::json;

fn table_uri(dir: &tempfile::TempDir, name: &str) -> String {
    // Bare absolute path — `ensure_table_uri` promotes it to file://.
    dir.path().join(name).to_string_lossy().into_owned()
}

#[tokio::test]
async fn missing_table_without_create_errors() {
    let dir = tempfile::tempdir().unwrap();
    let uri = table_uri(&dir, "nope");

    let mut cfg = DeltaSinkConfig::new(&uri);
    cfg.create_if_not_missing = false;
    let sink = DeltaSink::new(cfg).await.unwrap();
    let err = sink.write_batch(&[json!({"id": 1})]).await.unwrap_err();
    assert!(
        err.to_string().contains("does not exist"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn empty_batch_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let uri = table_uri(&dir, "empty");
    let sink = DeltaSink::new(DeltaSinkConfig::new(&uri)).await.unwrap();
    assert_eq!(sink.write_batch(&[]).await.unwrap(), 0);
    // Nothing buffered → flush is a clean no-op.
    sink.flush().await.unwrap();
}

#[tokio::test]
async fn sink_check_passes_on_reachable_store() {
    use faucet_core::check::{CheckContext, ProbeStatus};
    let dir = tempfile::tempdir().unwrap();
    let uri = table_uri(&dir, "chk");
    // Table need not exist — create_if_not_missing handles that at write time,
    // so a reachable (local) store passes.
    let sink = DeltaSink::new(DeltaSinkConfig::new(&uri)).await.unwrap();
    let report = sink.check(&CheckContext::default()).await.unwrap();
    assert!(
        report
            .probes
            .iter()
            .all(|p| matches!(p.status, ProbeStatus::Pass)),
        "sink check should pass: {report:?}"
    );
}
