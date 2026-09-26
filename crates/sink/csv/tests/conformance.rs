//! `faucet-conformance` battery against the real CSV sink.
//!
//! CSV is an append-only file sink — it advertises no idempotency mechanism,
//! so the battery exercises the **honest branch**:
//! - check 1 `assert_config_schema_valid_value` (value form, for sinks);
//! - check 5 `assert_capabilities_truthful` — Append works, and the sink does
//!   *not* claim idempotent/keyed dedup (so the pipeline correctly refuses
//!   `delivery: exactly_once` for it).
use faucet_conformance::assert_config_schema_valid_value;
use faucet_core::Sink;
use faucet_sink_csv::{CsvSink, CsvSinkConfig};

/// Count durable data rows in the CSV file. Headers are disabled in the test
/// config, so every non-empty line is one written record. A missing file
/// (before the first write) counts as zero.
fn count_rows(path: &std::path::Path) -> usize {
    std::fs::read_to_string(path)
        .map(|s| s.lines().filter(|l| !l.trim().is_empty()).count())
        .unwrap_or(0)
}

#[test]
fn conformance_config_schema_valid() {
    let schema =
        serde_json::to_value(schemars::schema_for!(faucet_sink_csv::CsvSinkConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "csv");
}

// ── Check 10: connector_name is non-empty ─────────────────────────────────────
#[test]
fn conformance_connector_name_nonempty() {
    let sink = CsvSink::new(CsvSinkConfig::new("/tmp/does-not-matter.csv"));
    faucet_conformance::assert_batch_atomicity_declared(&sink);
    faucet_conformance::assert_connector_name_nonempty_value(
        sink.connector_name(),
        sink.connector_name(),
    );
    assert_eq!(sink.connector_name(), "csv");
}

// ── Check 11: preflight check() is well-formed ────────────────────────────────
#[tokio::test]
async fn conformance_preflight_check_wellformed() {
    // A writable parent directory makes the filesystem probe pass; the check
    // must return Ok(report) with a well-formed probe.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out.csv");
    let sink = CsvSink::new(CsvSinkConfig::new(path.to_str().unwrap()));
    faucet_conformance::assert_sink_preflight_check_wellformed(
        &sink,
        &faucet_core::check::CheckContext::default(),
    )
    .await;
}

#[tokio::test]
async fn conformance_capabilities_truthful() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out.csv");
    // Disable headers so the durable line count equals the written-row count
    // (the battery expects the count to grow by exactly 1 per appended record).
    let sink = CsvSink::new(CsvSinkConfig::new(path.to_str().unwrap()).write_headers(false));
    let sink_ref = &sink;

    faucet_conformance::assert_capabilities_truthful(&sink, || {
        let path = path.clone();
        async move {
            // The sink buffers; flush so the durable row count reflects writes.
            sink_ref.flush().await.expect("flush");
            count_rows(&path)
        }
    })
    .await;

    // The honest branch must have left the append-only sink non-idempotent.
    assert!(!sink.supports_idempotent_writes());
    assert!(!sink.dedups_by_key());
}
