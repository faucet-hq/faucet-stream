//! #620 — `target_file_size` must actually control output file size and bound
//! peak memory.
//!
//! The knob was declared in the config and validated (a zero was rejected), and
//! then **never read**: `RecordBatchWriter::for_table` was built with no sizing,
//! and the only `flush_and_commit` was at end-of-run. Since a bulk source emits
//! no bookmarks, that meant one commit per run and a parquet buffer holding the
//! *whole dataset* — an OOM on a large table, and a documented knob that did
//! nothing.
//!
//! delta-rs offers no target-size parameter: `RecordBatchWriter` writes exactly
//! one file per commit, so file size is governed by flush cadence. Wiring the
//! knob and bounding memory are therefore the same fix, and these tests assert
//! it through the observable consequence — the number of Delta versions and
//! parquet files the table ends up with.

use faucet_sink_delta::DeltaSinkConfig;

fn table_uri(dir: &tempfile::TempDir, name: &str) -> String {
    dir.path().join(name).to_string_lossy().into_owned()
}

#[tokio::test]
async fn a_zero_target_is_still_rejected_at_config_load() {
    // Pre-existing validation, pinned so wiring the knob did not loosen it — a
    // zero would otherwise commit on every single row.
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = DeltaSinkConfig::new(table_uri(&dir, "zero"));
    cfg.target_file_size = Some(0);
    assert!(
        cfg.validate().is_err(),
        "a zero target_file_size must remain a config error"
    );
}
