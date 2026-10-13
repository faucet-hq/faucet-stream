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

// ── Descriptor agreement (RFC 0013) ───────────────────────────────────────────
/// Offline against a SQLite catalog, like check 10.
#[cfg(feature = "catalog-sql")]
#[tokio::test(flavor = "multi_thread")]
async fn conformance_descriptor_matches_built_sink() {
    use faucet_sink_iceberg::{IcebergSink, IcebergSinkConfig};

    let dir = tempfile::TempDir::new().expect("tempdir");
    let warehouse_path = dir.path().join("warehouse");
    std::fs::create_dir_all(&warehouse_path).expect("create warehouse dir");
    let v = serde_json::json!({
        "catalog": {
            "type": "sql",
            "uri": format!("sqlite:{}?mode=rwc", dir.path().join("catalog.db").display()),
            "warehouse": format!("file://{}", warehouse_path.display()),
        },
        "namespace": ["db"],
        "table": "descriptor",
    });
    let cfg: IcebergSinkConfig = serde_json::from_value(v.clone()).expect("sink config parse");
    let built = IcebergSink::new(cfg).await.expect("IcebergSink::new");
    faucet_conformance::assert_descriptor_matches_sink(
        &faucet_sink_iceberg::DESCRIPTOR,
        "iceberg",
        &v,
        &built,
    );
}

// ── Check: backend calls honour the call deadline and cancel (#880) ──────────

/// A local SQLite catalog over an `s3://` warehouse whose endpoint never
/// answers: the table's metadata write is the hung backend call.
#[cfg(feature = "catalog-sql")]
#[tokio::test(flavor = "multi_thread")]
async fn conformance_call_deadline() {
    use faucet_sink_iceberg::{IcebergSink, IcebergSinkConfig};

    let hung = faucet_conformance::hung::SilentListener::start()
        .await
        .expect("listener");
    let endpoint = hung.http_url();
    let dir = tempfile::TempDir::new().expect("tempdir");
    let n = std::sync::atomic::AtomicUsize::new(0);
    let make = || {
        let db = dir.path().join(format!(
            "catalog-{}.db",
            n.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        ));
        let cfg: IcebergSinkConfig = serde_json::from_value(serde_json::json!({
            "catalog": {
                "type": "sql",
                "uri": format!("sqlite:{}?mode=rwc", db.display()),
                "warehouse": "s3://hung/warehouse",
                "properties": {
                    "s3.endpoint": endpoint,
                    "s3.access-key-id": "k",
                    "s3.secret-access-key": "s",
                    "s3.region": "us-east-1",
                    "s3.path-style-access": "true",
                    "s3.disable-config-load": "true",
                    "s3.disable-ec2-metadata": "true"
                }
            },
            "namespace": ["db"],
            "table": "hung",
            "create_if_missing": true,
            "batch_size": 0
        }))
        .expect("sink config parse");
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(IcebergSink::new(cfg))
        })
        .expect("IcebergSink::new")
    };
    faucet_conformance::calls::assert_sink_call_deadline(make, &[serde_json::json!({"id": 1})])
        .await;
}
