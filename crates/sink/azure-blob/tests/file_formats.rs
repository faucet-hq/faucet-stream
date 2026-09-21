//! Round-trip the shared file formats through a real Azure Blob endpoint
//! (Azurite, #604): the sink encodes a whole-object format, and the blob that
//! lands decodes back to the records that went in.
//!
//! Unit tests already pin `faucet_core::file_format` in isolation; what only
//! an integration test shows is that the sink's buffered-group path — the one
//! every format except JSON Lines takes — produces bytes the shared decoder
//! reads back.
#![cfg(all(not(target_os = "windows"), feature = "file-format-csv"))]

use std::sync::Arc;

use faucet_core::Sink;
use faucet_core::file_format::{FileFormat, FormatOptions, decode};
use faucet_sink_azure_blob::{
    AzureBlobSink, AzureBlobSinkConfig, AzureCredentials, AzureSinkFormat,
};
use futures::StreamExt;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt};
use serde_json::{Value, json};
use testcontainers_modules::azurite::{Azurite, BLOB_PORT};
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const AZURITE_ACCOUNT: &str = "devstoreaccount1";
const AZURITE_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
const CONTAINER: &str = "faucet-format-test";

async fn start_azurite() -> (ContainerAsync<Azurite>, u16) {
    let container = Azurite::default()
        .start()
        .await
        .expect("start azurite container");
    let port = container
        .get_host_port_ipv4(BLOB_PORT)
        .await
        .expect("azurite blob host port");
    (container, port)
}

fn endpoint(port: u16) -> String {
    format!("http://127.0.0.1:{port}/{AZURITE_ACCOUNT}")
}

async fn create_container(port: u16) {
    use azure_storage::{CloudLocation, prelude::*};
    use azure_storage_blobs::prelude::*;

    ClientBuilder::with_location(
        CloudLocation::Emulator {
            address: "127.0.0.1".to_owned(),
            port,
        },
        StorageCredentials::emulator(),
    )
    .container_client(CONTAINER)
    .create()
    .await
    .expect("create azurite blob container");
}

fn verify_store(port: u16) -> Arc<dyn ObjectStore> {
    Arc::new(
        MicrosoftAzureBuilder::new()
            .with_account(AZURITE_ACCOUNT)
            .with_access_key(AZURITE_KEY)
            .with_container_name(CONTAINER)
            .with_endpoint(endpoint(port))
            .with_allow_http(true)
            .build()
            .expect("build verifying object store"),
    )
}

fn sink_config(port: u16) -> AzureBlobSinkConfig {
    AzureBlobSinkConfig::new(CONTAINER)
        .account(AZURITE_ACCOUNT)
        .auth(AzureCredentials::AccountKey {
            account_key: AZURITE_KEY.into(),
        })
        .endpoint(endpoint(port))
        .allow_http(true)
}

/// The sole object under `prefix`, as raw bytes.
async fn sole_object(store: &Arc<dyn ObjectStore>, prefix: &str) -> Vec<u8> {
    let mut listing = store.list(Some(&ObjPath::from(prefix)));
    let mut keys = Vec::new();
    while let Some(meta) = listing.next().await {
        keys.push(meta.expect("list meta").location);
    }
    assert_eq!(keys.len(), 1, "expected one blob, got {keys:?}");
    store
        .get(&keys[0])
        .await
        .expect("get blob")
        .bytes()
        .await
        .expect("read blob")
        .to_vec()
}

fn records() -> Vec<Value> {
    vec![
        json!({"id": "1", "name": "ada"}),
        json!({"id": "2", "name": "grace"}),
    ]
}

async fn round_trip(format: AzureSinkFormat, shared: FileFormat, ext: &str) {
    let (_c, port) = start_azurite().await;
    create_container(port).await;
    let prefix = format!("{}/", shared.as_str());

    let sink = AzureBlobSink::new(
        sink_config(port)
            .prefix(prefix.clone())
            .format(format)
            .file_extension(ext),
    )
    .await
    .expect("sink");

    let expected = records();
    assert_eq!(
        sink.write_batch(&expected).await.expect("write"),
        expected.len()
    );
    // A whole-object format cannot know the group is complete before flush.
    sink.flush().await.expect("flush");

    let bytes = sole_object(&verify_store(port), &prefix).await;
    let back = decode(&bytes, shared, &FormatOptions::default())
        .await
        .expect("decode the landed blob");
    assert_eq!(
        back, expected,
        "{shared:?} did not round-trip through Azure"
    );
}

#[tokio::test]
async fn csv_blobs_round_trip() {
    round_trip(AzureSinkFormat::Csv, FileFormat::Csv, ".csv").await;
}

#[tokio::test]
async fn json_array_blobs_round_trip() {
    round_trip(AzureSinkFormat::JsonArray, FileFormat::JsonArray, ".json").await;
}

#[cfg(feature = "file-format-xml")]
#[tokio::test]
async fn xml_blobs_round_trip() {
    round_trip(AzureSinkFormat::Xml, FileFormat::Xml, ".xml").await;
}

#[cfg(feature = "file-format-excel")]
#[tokio::test]
async fn xlsx_blobs_round_trip() {
    round_trip(AzureSinkFormat::Xlsx, FileFormat::Xlsx, ".xlsx").await;
}

/// An empty page must not leave a stray zero-record blob behind.
#[tokio::test]
async fn an_empty_write_leaves_no_blob() {
    let (_c, port) = start_azurite().await;
    create_container(port).await;
    let sink = AzureBlobSink::new(
        sink_config(port)
            .prefix("empty/".to_string())
            .format(AzureSinkFormat::Csv)
            .file_extension(".csv"),
    )
    .await
    .expect("sink");

    assert_eq!(sink.write_batch(&[]).await.expect("write"), 0);
    sink.flush().await.expect("flush");

    let store = verify_store(port);
    let mut listing = store.list(Some(&ObjPath::from("empty/")));
    assert!(listing.next().await.is_none(), "no blob for an empty page");
}
