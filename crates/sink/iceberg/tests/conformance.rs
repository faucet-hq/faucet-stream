//! `faucet-conformance` battery for the Iceberg sink.
//!
//! **Check 1** (`assert_config_schema_valid_value`) and **check 10**
//! (`connector_name()` non-empty, gated on `catalog-sql`) run here, so the
//! Iceberg sink stays **Tier-2** for now. The effectively-once check (check 4)
//! does not fit this sink cleanly on `iceberg-rust` 0.9.1: the sink is
//! append-only and only materialises rows on `flush()` (it buffers in
//! `write_batch_idempotent`), while the battery drives an interleaved
//! write→count→write→count sequence and never flushes. Bridging that with a
//! flush-inside-count shim makes the cumulative `total-records` snapshot summary
//! non-monotonic across the two commits, so the "forward progress after a new
//! token" assertion cannot be satisfied deterministically. Promoting Iceberg to
//! Tier-1 is tracked separately (needs an upstream append/commit model that the
//! watermark-replay check can observe without a terminal flush).
use faucet_conformance::assert_config_schema_valid_value;

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(
        faucet_sink_iceberg::IcebergSinkConfig
    ))
    .unwrap();
    assert_config_schema_valid_value(&schema, "iceberg");
}

// ── Check 10: connector_name is non-empty ─────────────────────────────────────
/// Builds the sink fully offline against a SQLite catalog + local-FS warehouse
/// (no Docker), so this runs under `--all-features` in CI. Gated on
/// `catalog-sql` (the same feature the offline `integration.rs` uses).
#[cfg(feature = "catalog-sql")]
#[tokio::test(flavor = "multi_thread")]
async fn conformance_connector_name_nonempty() {
    use faucet_core::Sink as _;
    use faucet_sink_iceberg::{IcebergSink, IcebergSinkConfig};

    let dir = tempfile::TempDir::new().expect("tempdir");
    let db_path = dir.path().join("catalog.db");
    let warehouse_path = dir.path().join("warehouse");
    std::fs::create_dir_all(&warehouse_path).expect("create warehouse dir");
    let sqlite_uri = format!("sqlite:{}?mode=rwc", db_path.display());
    let warehouse_uri = format!("file://{}", warehouse_path.display());

    let cfg: IcebergSinkConfig = serde_json::from_value(serde_json::json!({
        "catalog": { "type": "sql", "uri": sqlite_uri, "warehouse": warehouse_uri },
        "namespace": ["db"],
        "table": "conformance_name",
        "create_if_missing": true,
        "batch_size": 0
    }))
    .expect("sink config parse");

    let sink = IcebergSink::new(cfg).await.expect("IcebergSink::new");
    faucet_conformance::assert_batch_atomicity_declared(&sink);
    faucet_conformance::assert_connector_name_nonempty_value(
        sink.connector_name(),
        sink.connector_name(),
    );
}
