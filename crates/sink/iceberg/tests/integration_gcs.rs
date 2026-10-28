//! Integration test: `IcebergSink` with a SQLite SQL catalog writing to a
//! `gs://` warehouse on fake-gcs-server, #967.
//!
//! Requires Docker and the `catalog-sql` feature:
//!   cargo test -p faucet-sink-iceberg --all-features --test integration_gcs

#![cfg(feature = "catalog-sql")]

mod catalog_support;

use catalog_support::assert_round_trip;
use faucet_conformance::stores;
use serde_json::json;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_catalog_gcs_warehouse_creates_and_appends() {
    let gcs = stores::fake_gcs(&["faucet-iceberg-gcs"]).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let warehouse = "gs://faucet-iceberg-gcs/warehouse".to_string();
    let catalog = json!({
        "type": "sql",
        "uri": format!("sqlite:{}?mode=rwc", dir.path().join("catalog.db").display()),
        "warehouse": warehouse,
        "properties": {
            "gcs.service.path": gcs.endpoint,
            "gcs.no-auth": "true",
            "gcs.project-id": "faucet-tests"
        }
    });
    assert_round_trip(catalog, "events", &warehouse).await;
}
