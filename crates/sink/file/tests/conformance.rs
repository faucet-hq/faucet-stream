//! Runs the reusable `faucet-conformance` battery against the file sink.
//!
//! The sink is append/overwrite only, so the battery exercises the honest
//! branch: no idempotent-write or keyed-dedup claim.

use faucet_core::Sink;
use faucet_sink_file::{FileSink, FileSinkConfig};

fn count_lines(path: &std::path::Path) -> usize {
    std::fs::read_to_string(path)
        .map(|s| s.lines().filter(|l| !l.trim().is_empty()).count())
        .unwrap_or(0)
}

fn sink(path: &std::path::Path) -> FileSink {
    FileSink::new(FileSinkConfig::new(path.to_string_lossy())).unwrap()
}

#[test]
fn conformance_config_schema_and_atomicity() {
    let s = FileSink::new(FileSinkConfig::new("/tmp/does-not-matter.jsonl")).unwrap();
    faucet_conformance::assert_batch_atomicity_declared(&s);
    faucet_conformance::assert_config_schema_valid_value(&s.config_schema(), s.connector_name());
    faucet_conformance::assert_connector_name_nonempty_value(
        s.connector_name(),
        s.connector_name(),
    );
}

#[tokio::test]
async fn conformance_preflight_check_wellformed() {
    let dir = tempfile::tempdir().unwrap();
    faucet_conformance::assert_sink_preflight_check_wellformed(
        &sink(&dir.path().join("out.jsonl")),
        &faucet_core::check::CheckContext::default(),
    )
    .await;
}

#[tokio::test]
async fn conformance_capabilities_truthful() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out.jsonl");
    let s = sink(&path);
    let s_ref = &s;
    faucet_conformance::assert_capabilities_truthful(&s, || {
        let path = path.clone();
        async move {
            s_ref.flush().await.expect("flush");
            count_lines(&path)
        }
    })
    .await;
    faucet_conformance::assert_write_modes_truthful(&s, || async { 0 }).await;
    assert!(!s.supports_idempotent_writes());
    assert!(!s.dedups_by_key());
}

#[tokio::test]
async fn conformance_cancellation_flushes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out.jsonl");
    let s = sink(&path);
    faucet_conformance::assert_cancellation_flushes(&s, || {
        let path = path.clone();
        async move { count_lines(&path) }
    })
    .await;
}
