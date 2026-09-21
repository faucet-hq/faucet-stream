//! The multipart (block) upload path (#618), against Azurite.
//!
//! This is the memory bound the cross-page accumulator exists for: once the
//! open object passes `PART_BYTES` (8 MiB) the sink hands each full part to
//! `upload_part` and drops the buffer, so peak stays at O(part size) rather
//! than O(object size). Before #618 the sink held the whole object and
//! issued a single `put`, capping output size at process memory.
//!
//! Every other test writes blobs far below the threshold, so the lazy
//! `put_multipart`, the part sequence, and `complete()` were all unrun — and
//! a wrong part sequence does not fail loudly, it produces a torn blob.
#![cfg(not(target_os = "windows"))]

use std::sync::Arc;

use faucet_core::Sink;
use faucet_sink_azure_blob::{AzureBlobSink, AzureBlobSinkConfig, AzureCredentials};
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
const CONTAINER: &str = "faucet-multipart";

/// ~1 KiB per record, so 16k records is ~16 MiB — past the 8 MiB part floor
/// with a tail, forcing at least two parts plus a remainder.
const RECORDS: usize = 16_000;

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

fn wide_record(i: usize) -> Value {
    json!({ "id": i, "payload": "x".repeat(1000) })
}

#[tokio::test]
async fn a_blob_past_the_part_floor_uploads_in_parts_and_reads_back_whole() {
    let (_c, port) = start_azurite().await;
    create_container(port).await;

    // No caps: the whole run is one blob, which is the shape that must go
    // multipart rather than buffer.
    let sink = AzureBlobSink::new(
        sink_config(port)
            .prefix("big/".to_string())
            .file_extension(".jsonl")
            .with_batch_size(0),
    )
    .await
    .expect("sink");

    let page: Vec<Value> = (0..RECORDS).map(wide_record).collect();
    assert_eq!(sink.write_batch(&page).await.expect("write"), RECORDS);
    sink.flush().await.expect("flush completes the upload");

    let store = verify_store(port);
    let mut listing = store.list(Some(&ObjPath::from("big/")));
    let mut keys = Vec::new();
    while let Some(meta) = listing.next().await {
        keys.push(meta.expect("list meta").location);
    }
    assert_eq!(keys.len(), 1, "one blob, uploaded in parts: {keys:?}");

    let bytes = store
        .get(&keys[0])
        .await
        .expect("get blob")
        .bytes()
        .await
        .expect("read blob");

    // Every record present, in order, and nothing torn at a part boundary —
    // the failure a wrong part sequence actually produces.
    let text = String::from_utf8(bytes.to_vec()).expect("utf-8");
    let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(lines.len(), RECORDS, "every record survived the part split");
    for (i, line) in lines.iter().enumerate() {
        let v: Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("line {i} is not whole JSON ({e}): {line:.80}"));
        assert_eq!(v["id"], json!(i), "records kept their order across parts");
    }
}

/// A byte cap below the part floor rolls whole blobs instead, and the tail
/// that closes each one must not be lost.
#[tokio::test]
async fn a_byte_cap_rolls_blobs_and_keeps_every_record() {
    let (_c, port) = start_azurite().await;
    create_container(port).await;
    let sink = AzureBlobSink::new(
        sink_config(port)
            .prefix("rolled/".to_string())
            .file_extension(".jsonl")
            .with_batch_size(0)
            .max_bytes_per_file(2 * 1024 * 1024),
    )
    .await
    .expect("sink");

    let total = 6_000;
    let page: Vec<Value> = (0..total).map(wide_record).collect();
    sink.write_batch(&page).await.expect("write");
    sink.flush().await.expect("flush");

    let store = verify_store(port);
    let mut listing = store.list(Some(&ObjPath::from("rolled/")));
    let mut keys = Vec::new();
    while let Some(meta) = listing.next().await {
        keys.push(meta.expect("list meta").location);
    }
    assert!(
        keys.len() >= 2,
        "~6 MiB at a 2 MiB cap is several blobs, got {}",
        keys.len()
    );

    // Rolling must not drop the remainder that closes each blob.
    let mut seen = 0usize;
    for k in &keys {
        let bytes = store
            .get(k)
            .await
            .expect("get blob")
            .bytes()
            .await
            .expect("read blob");
        seen += String::from_utf8_lossy(&bytes)
            .lines()
            .filter(|l| !l.trim().is_empty())
            .count();
    }
    assert_eq!(seen, total, "no records lost across the rollover boundary");
}
