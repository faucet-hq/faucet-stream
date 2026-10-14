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

/// `write_mode: overwrite` stages until commit and an abort leaves the
/// previous output in place (#878).
#[tokio::test]
async fn conformance_overwrite_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out.jsonl");
    std::fs::write(&path, "{\"id\":-1}\n{\"id\":-2}\n{\"id\":-3}\n").unwrap();
    let s = FileSink::new(
        FileSinkConfig::new(path.to_string_lossy())
            .write_mode(faucet_sink_file::FileWriteMode::Overwrite),
    )
    .unwrap();
    faucet_conformance::assert_overwrite_lifecycle(&s, || {
        let path = path.clone();
        async move { count_lines(&path) }
    })
    .await;
}

// ── Descriptor (RFC 0013): agrees with the built sink ───────────────────────

#[test]
fn descriptor_matches_built_sink() {
    let dir = tempfile::tempdir().unwrap();
    let mut paths = vec![
        ("a.jsonl", serde_json::json!({})),
        ("b.json", serde_json::json!({})),
    ];
    #[cfg(feature = "file-format-csv")]
    paths.extend([
        ("c.csv", serde_json::json!({"on_unknown_field": "warn"})),
        ("d.csv", serde_json::json!({"on_unknown_field": "widen"})),
    ]);
    #[cfg(feature = "file-format-parquet")]
    paths.push(("e-{part}.parquet", serde_json::json!({})));
    for (name, csv) in paths {
        let v = serde_json::json!({
            "path": dir.path().join(name).to_string_lossy(),
            "csv": csv,
        });
        let cfg: FileSinkConfig = serde_json::from_value(v.clone()).unwrap();
        let built = FileSink::new(cfg).expect("sink builds");
        faucet_conformance::assert_descriptor_matches_sink(
            &faucet_sink_file::DESCRIPTOR,
            "file",
            &v,
            &built,
        );
    }
}

/// RFC 0014: the sink is a `FileSink` over the local object-store driver, so
/// its capabilities follow the driver (overwrite through the swap area, no
/// exactly-once, cleanup or rollback) and its four write paths agree.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_driver_declarations_and_row_paths() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out.jsonl");
    let s = sink(&path);
    faucet_conformance::assert_driver_declarations_truthful(&s).await;
    faucet_conformance::assert_driver_row_paths_agree(&s).await;
    assert_eq!(count_lines(&path), 12);
}
