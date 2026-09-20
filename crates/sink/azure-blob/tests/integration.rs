//! Integration tests for `faucet-sink-azure-blob` against an auto-started
//! Azurite emulator (via `testcontainers-modules`).
//!
//! These boot a real Azurite container, create a destination blob container,
//! drive the connector's real write path (`write_batch` + `flush`), then read
//! the written objects back through an `object_store` Azure client and assert
//! the JSONL bodies. They are **not** `#[ignore]`d and **not** env-gated — CI's
//! `--all-features` test/coverage jobs run them with Docker present, which is
//! what exercises the serialize / key-generation / upload / rollover lines in
//! `src/sink.rs`.
//!
//! Container creation goes through the Azure SDK (`object_store` cannot create a
//! container); every other blob operation goes through `object_store`.

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

/// Well-known Azurite dev account + key (`devstoreaccount1`).
const AZURITE_ACCOUNT: &str = "devstoreaccount1";
const AZURITE_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
const CONTAINER: &str = "faucet-test";

// Azurite is lightweight, but `cargo test` runs a binary's tests concurrently;
// serialize so at most one emulator container runs at a time (steadier on CI).
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Start Azurite; return the container handle plus its mapped blob port. The
/// handle must be held for the duration of the test to keep the container up.
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

/// Create the destination blob container via the Azure SDK (`object_store` has
/// no container-management API).
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

/// Build an `object_store` Azure client for reading the written objects back.
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

/// List every object key under `prefix`, sorted (UUIDv7 keys sort by write order).
async fn list_keys(store: &Arc<dyn ObjectStore>, prefix: &str) -> Vec<String> {
    let mut listing = store.list(Some(&ObjPath::from(prefix)));
    let mut keys = Vec::new();
    while let Some(meta) = listing.next().await {
        keys.push(meta.expect("list meta").location.to_string());
    }
    keys.sort();
    keys
}

/// Download an object body and parse it as JSON Lines records.
async fn read_jsonl(store: &Arc<dyn ObjectStore>, key: &str) -> Vec<Value> {
    let bytes = store
        .get(&ObjPath::from(key))
        .await
        .expect("get object")
        .bytes()
        .await
        .expect("read object bytes");
    let text = String::from_utf8(bytes.to_vec()).expect("utf8 body");
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str::<Value>(l).expect("parse jsonl line"))
        .collect()
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

// ── Happy path: write a batch as one object and read the rows back ───────────

#[tokio::test(flavor = "multi_thread")]
async fn sink_writes_and_reads_back() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_azurite().await;
    create_container(port).await;

    // batch_size = 0 → the whole page is written as a single object.
    let sink = AzureBlobSink::new(sink_config(port).prefix("out/").with_batch_size(0))
        .await
        .expect("sink new");

    let records = vec![json!({"id": 1}), json!({"id": 2}), json!({"id": 3})];
    let written = sink.write_batch(&records).await.expect("write_batch");
    assert_eq!(written, 3);
    sink.flush().await.expect("flush");

    let store = verify_store(port);
    let keys = list_keys(&store, "out").await;
    assert_eq!(keys.len(), 1, "batch_size=0 writes a single object");
    assert!(keys[0].starts_with("out/") && keys[0].ends_with(".jsonl"));

    let rows = read_jsonl(&store, &keys[0]).await;
    let mut ids: Vec<i64> = rows.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 3]);
}

// ── Rollover: max_records_per_file splits a batch across multiple objects ────

#[tokio::test(flavor = "multi_thread")]
async fn sink_rolls_over_multiple_files() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_azurite().await;
    create_container(port).await;

    let sink = AzureBlobSink::new(
        sink_config(port)
            .prefix("roll/")
            .with_batch_size(0)
            .max_records_per_file(2)
            .concurrency(4),
    )
    .await
    .expect("sink new");

    let records: Vec<Value> = (1..=5).map(|i| json!({ "id": i })).collect();
    let written = sink.write_batch(&records).await.expect("write_batch");
    assert_eq!(written, 5);
    // Since #618 the remainder of the open object is closed at `flush`, which
    // the pipeline calls at every bookmark-carrying page and at the end.
    sink.flush().await.expect("flush");

    let store = verify_store(port);
    let keys = list_keys(&store, "roll").await;
    assert_eq!(keys.len(), 3, "5 records at 2/file → 3 objects");

    // Every record must land exactly once across the rolled-over objects.
    let mut all_ids = Vec::new();
    for key in &keys {
        for row in read_jsonl(&store, key).await {
            all_ids.push(row["id"].as_i64().unwrap());
        }
    }
    all_ids.sort_unstable();
    assert_eq!(all_ids, vec![1, 2, 3, 4, 5]);
}

// ── Empty batch is a no-op (no object written) ───────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn sink_write_batch_empty_is_noop() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_azurite().await;
    create_container(port).await;

    let sink = AzureBlobSink::new(sink_config(port).prefix("empty/"))
        .await
        .expect("sink new");

    let written = sink.write_batch(&[]).await.expect("write_batch");
    assert_eq!(written, 0);

    let store = verify_store(port);
    assert!(list_keys(&store, "empty").await.is_empty());
}

// ── Preflight check succeeds against a reachable, credentialed container ──────

#[tokio::test(flavor = "multi_thread")]
async fn sink_check_passes() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_azurite().await;
    create_container(port).await;

    let sink = AzureBlobSink::new(sink_config(port).prefix("chk/"))
        .await
        .expect("sink new");

    let ctx = faucet_core::check::CheckContext {
        timeout: std::time::Duration::from_secs(10),
    };
    let report = sink.check(&ctx).await.expect("check");
    assert!(
        report
            .probes
            .iter()
            .all(|p| matches!(p.status, faucet_core::check::ProbeStatus::Pass)),
        "expected all probes to pass, got {:?}",
        report.probes
    );
}

// ── Compressed (.gz) upload (requires the `compression` feature) ─────────────

#[cfg(feature = "compression")]
#[tokio::test(flavor = "multi_thread")]
async fn sink_writes_gzip_when_compression_enabled() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_azurite().await;
    create_container(port).await;

    // `.jsonl.gz` extension + Compression::Auto → the body is gzip-compressed.
    let sink = AzureBlobSink::new(
        sink_config(port)
            .prefix("gz/")
            .file_extension(".jsonl.gz")
            .with_batch_size(0),
    )
    .await
    .expect("sink new");

    let written = sink
        .write_batch(&[json!({"id": 1}), json!({"id": 2})])
        .await
        .expect("write_batch");
    assert_eq!(written, 2);
    sink.flush().await.expect("flush");

    let store = verify_store(port);
    let keys = list_keys(&store, "gz").await;
    assert_eq!(keys.len(), 1);
    assert!(keys[0].ends_with(".jsonl.gz"));

    let bytes = store
        .get(&ObjPath::from(keys[0].as_str()))
        .await
        .expect("get object")
        .bytes()
        .await
        .expect("read bytes");
    // Gzip magic header.
    assert_eq!(
        &bytes[..2],
        b"\x1f\x8b",
        "object body must be gzip-compressed"
    );

    // And it must round-trip back to the original records once decompressed.
    use std::io::Read as _;
    let mut reader =
        faucet_core::compression::wrap_sync_reader(&bytes[..], faucet_core::Compression::Gzip);
    let mut decompressed = Vec::new();
    reader.read_to_end(&mut decompressed).expect("gunzip");
    let text = String::from_utf8(decompressed).expect("utf8");
    let ids: Vec<i64> = text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            serde_json::from_str::<Value>(l).unwrap()["id"]
                .as_i64()
                .unwrap()
        })
        .collect();
    assert_eq!(ids, vec![1, 2]);
}
