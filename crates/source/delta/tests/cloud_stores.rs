//! The Delta source against real object stores: `s3://` on MinIO and `gs://` on
//! fake-gcs-server, each through its `DeltaCredentials` variant (#967; Azure
//! is in `azure_store.rs`). Every test commits a table with delta-rs, then
//! reads it with the source at the latest version and at version 1.
//!
//! Requires Docker and the store features:
//!   cargo test -p faucet-source-delta --all-features --test cloud_stores

#![cfg(any(feature = "s3", feature = "gcs"))]

mod store_support;

use faucet_conformance::stores;
use serde_json::json;
use store_support::{assert_round_trip, connection};

#[cfg(feature = "s3")]
async fn s3_bucket(endpoint: &str, bucket: &str) {
    use aws_sdk_s3::config::{Credentials, Region};
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
        .region(Region::new(stores::MINIO_REGION))
        .endpoint_url(endpoint)
        .credentials_provider(Credentials::new(
            stores::MINIO_ACCESS_KEY,
            stores::MINIO_SECRET_KEY,
            None,
            None,
            "test",
        ))
        .force_path_style(true)
        .build();
    aws_sdk_s3::Client::from_conf(config)
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("create bucket");
}

#[cfg(feature = "s3")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_table_on_minio_with_aws_credentials() {
    let minio = stores::minio().await;
    s3_bucket(&minio.endpoint, "delta-source").await;
    let cfg = connection(
        "s3://delta-source/tables/events",
        json!({ "type": "aws", "config": {
            "access_key_id": stores::MINIO_ACCESS_KEY,
            "secret_access_key": stores::MINIO_SECRET_KEY,
            "region": stores::MINIO_REGION,
            "endpoint_url": minio.endpoint,
            "allow_http": true
        }}),
        json!({}),
    );
    assert_round_trip(cfg).await;
}

#[cfg(feature = "gcs")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gs_table_on_fake_gcs_with_an_inline_service_account_key() {
    let gcs = stores::fake_gcs_xml(&["delta-src-gs"]).await;
    let cfg = connection(
        "gs://delta-src-gs/tables/events",
        json!({ "type": "gcp", "config": {
            "service_account_key": stores::fake_gcs_service_account_key(&gcs.endpoint)
        }}),
        json!({}),
    );
    assert_round_trip(cfg).await;
}

#[cfg(feature = "gcs")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gs_table_on_fake_gcs_with_a_service_account_file() {
    let gcs = stores::fake_gcs_xml(&["delta-src-gs-file"]).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let key = dir.path().join("sa.json");
    std::fs::write(&key, stores::fake_gcs_service_account_key(&gcs.endpoint)).expect("key file");
    let cfg = connection(
        "gs://delta-src-gs-file/tables/events",
        json!({ "type": "gcp", "config": {
            "service_account_path": key.to_string_lossy()
        }}),
        json!({}),
    );
    assert_round_trip(cfg).await;
}
