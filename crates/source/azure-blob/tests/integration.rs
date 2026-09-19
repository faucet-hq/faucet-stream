//! Integration tests for `faucet-source-azure-blob` against an auto-started
//! Azurite emulator (via `testcontainers-modules`).
//!
//! These boot a real Azurite container, create a blob container, seed objects
//! through the same `object_store` Azure client the connector uses, then drive
//! the connector's real read path (`fetch_all` + `stream_pages`) and assert the
//! decoded records. They are **not** `#[ignore]`d and **not** env-gated — CI's
//! `--all-features` test/coverage jobs run them with Docker present, which is
//! what exercises the object-listing / download / decode / batching lines in
//! `src/stream.rs`.
//!
//! Container creation goes through the Azure SDK (`object_store` cannot create a
//! container); every other blob operation goes through `object_store` so the
//! seed path mirrors the connector's own client.

#![cfg(not(target_os = "windows"))]

use std::collections::HashMap;
use std::sync::Arc;

use faucet_core::Source;
use faucet_source_azure_blob::{
    AzureBlobSource, AzureBlobSourceConfig, AzureCredentials, AzureFileFormat,
};
use futures::StreamExt;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
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

/// Start Azurite and return the container handle plus its mapped blob port. The
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

/// The blob endpoint object_store / the connector point at for `devstoreaccount1`.
fn endpoint(port: u16) -> String {
    format!("http://127.0.0.1:{port}/{AZURITE_ACCOUNT}")
}

/// Create the destination blob container via the Azure SDK (`object_store` has
/// no container-management API). Uses the emulator credentials, so it targets
/// the same `devstoreaccount1` container the connector reads.
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

/// Build an `object_store` Azure client (the same client type the connector
/// uses) for seeding blobs into the running Azurite container.
fn seed_store(port: u16) -> Arc<dyn ObjectStore> {
    Arc::new(
        MicrosoftAzureBuilder::new()
            .with_account(AZURITE_ACCOUNT)
            .with_access_key(AZURITE_KEY)
            .with_container_name(CONTAINER)
            .with_endpoint(endpoint(port))
            .with_allow_http(true)
            .build()
            .expect("build seeding object store"),
    )
}

/// Put one object with the given raw bytes.
async fn put_object(store: &Arc<dyn ObjectStore>, key: &str, body: Vec<u8>) {
    store
        .put(&ObjPath::from(key), PutPayload::from(body))
        .await
        .expect("seed object");
}

/// The connector config pointed at the running emulator with an explicit
/// endpoint + shared-key credentials.
fn source_config(port: u16) -> AzureBlobSourceConfig {
    AzureBlobSourceConfig::new(CONTAINER)
        .account(AZURITE_ACCOUNT)
        .auth(AzureCredentials::AccountKey {
            account_key: AZURITE_KEY.into(),
        })
        .endpoint(endpoint(port))
        .allow_http(true)
}

/// Collect all records from a source's streaming path, returning per-page sizes.
async fn drain_pages(source: &AzureBlobSource) -> (Vec<Value>, Vec<usize>) {
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 0);
    let mut records = Vec::new();
    let mut sizes = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.expect("stream page ok");
        sizes.push(page.records.len());
        records.extend(page.records);
    }
    (records, sizes)
}

// ── JSON Lines: fetch_all over a prefix listing of multiple objects ──────────

#[tokio::test(flavor = "multi_thread")]
async fn source_reads_json_lines_via_fetch() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_azurite().await;
    create_container(port).await;
    let store = seed_store(port);

    put_object(
        &store,
        "data/a.jsonl",
        b"{\"id\":1}\n{\"id\":2}\n{\"id\":3}\n".to_vec(),
    )
    .await;
    put_object(&store, "data/b.jsonl", b"{\"id\":4}\n{\"id\":5}\n".to_vec()).await;
    // An object outside the prefix must be excluded by the listing filter.
    put_object(&store, "other/c.jsonl", b"{\"id\":99}\n".to_vec()).await;

    let source = AzureBlobSource::new(source_config(port).prefix("data/"))
        .await
        .expect("source new");

    let records = source
        .fetch_with_context(&HashMap::new())
        .await
        .expect("fetch");

    assert_eq!(
        records.len(),
        5,
        "prefix listing should read both data/ objects"
    );
    assert!(records.iter().all(|r| r.is_object()));
    let mut ids: Vec<i64> = records.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 3, 4, 5]);
}

// ── JSON Lines: stream_pages splits into batch_size pages ────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn source_streams_json_lines_in_batch_sized_pages() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_azurite().await;
    create_container(port).await;
    let store = seed_store(port);

    put_object(
        &store,
        "page/data.jsonl",
        b"{\"n\":1}\n{\"n\":2}\n{\"n\":3}\n{\"n\":4}\n{\"n\":5}\n".to_vec(),
    )
    .await;

    let source = AzureBlobSource::new(source_config(port).prefix("page/").with_batch_size(2))
        .await
        .expect("source new");

    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 2);
    let mut sizes = Vec::new();
    let mut total = 0;
    while let Some(page) = pages.next().await {
        let page = page.expect("stream page ok");
        total += page.records.len();
        sizes.push(page.records.len());
    }
    assert_eq!(total, 5);
    assert_eq!(
        sizes,
        vec![2, 2, 1],
        "batch_size=2 over 5 records → [2,2,1]"
    );
}

// ── JSON array + raw text formats (fetch + stream) ───────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn source_reads_json_array_and_raw_text() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_azurite().await;
    create_container(port).await;
    let store = seed_store(port);

    put_object(&store, "arr/x.json", b"[{\"id\":1},{\"id\":2}]".to_vec()).await;
    put_object(&store, "raw/f.txt", b"hello\nworld".to_vec()).await;

    // JSON array: fetch_all (parse_content) + stream_pages (batch_size=0 → one
    // page per object carrying the whole array).
    let arr_source = AzureBlobSource::new(
        source_config(port)
            .prefix("arr/")
            .file_format(AzureFileFormat::JsonArray)
            .with_batch_size(0),
    )
    .await
    .expect("array source new");

    let fetched = arr_source
        .fetch_with_context(&HashMap::new())
        .await
        .expect("fetch array");
    assert_eq!(fetched.len(), 2);
    assert_eq!(fetched[0]["id"], 1);

    let (streamed, sizes) = drain_pages(&arr_source).await;
    assert_eq!(streamed.len(), 2);
    assert_eq!(
        sizes,
        vec![2],
        "batch_size=0 emits the whole array as one page"
    );

    // Raw text: each object becomes one {key, content} record.
    let raw_source = AzureBlobSource::new(
        source_config(port)
            .prefix("raw/")
            .file_format(AzureFileFormat::RawText),
    )
    .await
    .expect("raw source new");

    let raw = raw_source
        .fetch_with_context(&HashMap::new())
        .await
        .expect("fetch raw");
    assert_eq!(
        raw,
        vec![json!({"key": "raw/f.txt", "content": "hello\nworld"})]
    );

    let (raw_streamed, _sizes) = drain_pages(&raw_source).await;
    assert_eq!(
        raw_streamed,
        vec![json!({"key": "raw/f.txt", "content": "hello\nworld"})]
    );
}

// ── Preflight check succeeds against a reachable, credentialed container ──────

#[tokio::test(flavor = "multi_thread")]
async fn source_check_passes() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_azurite().await;
    create_container(port).await;

    let source = AzureBlobSource::new(source_config(port))
        .await
        .expect("source new");

    let ctx = faucet_core::check::CheckContext {
        timeout: std::time::Duration::from_secs(10),
    };
    let report = source.check(&ctx).await.expect("check");
    assert!(
        report
            .probes
            .iter()
            .all(|p| matches!(p.status, faucet_core::check::ProbeStatus::Pass)),
        "expected all probes to pass, got {:?}",
        report.probes
    );
}

// ── Compressed (.gz) object decoding (requires the `compression` feature) ────

#[cfg(feature = "compression")]
#[tokio::test(flavor = "multi_thread")]
async fn source_reads_gzip_object() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_azurite().await;
    create_container(port).await;
    let store = seed_store(port);

    let jsonl = b"{\"id\":10}\n{\"id\":11}\n";
    let gz = faucet_core::compression::compress_buf(jsonl, faucet_core::Compression::Gzip)
        .expect("gzip compress");
    put_object(&store, "gz/c.jsonl.gz", gz).await;

    // Compression::Auto resolves per object key, so `.gz` triggers gunzip.
    let source = AzureBlobSource::new(source_config(port).prefix("gz/"))
        .await
        .expect("source new");

    let records = source
        .fetch_with_context(&HashMap::new())
        .await
        .expect("fetch gzip");
    let mut ids: Vec<i64> = records.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![10, 11]);
}

// ── #619: `concurrency` is real on the streaming path ────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn source_streams_blobs_concurrently_in_listing_order() {
    // `stream_pages` used to read blobs strictly one at a time, so the
    // `concurrency` field was accepted and ignored. The prefetch that fixed
    // that is *ordered*, so records must still arrive in listing order — an
    // unordered look-ahead would interleave blobs by completion time and
    // change the sequence a downstream sink writes.
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_azurite().await;
    create_container(port).await;
    let store = seed_store(port);

    // Zero-padded names so lexicographic listing order is numeric order.
    for i in 1..=12i64 {
        put_object(
            &store,
            &format!("data/part-{i:04}.jsonl"),
            format!("{{\"id\":{i}}}\n").into_bytes(),
        )
        .await;
    }

    let source = AzureBlobSource::new(source_config(port).prefix("data/").concurrency(6))
        .await
        .expect("source new");

    let (records, _sizes) = drain_pages(&source).await;
    let ids: Vec<i64> = records.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    assert_eq!(
        ids,
        (1..=12).collect::<Vec<i64>>(),
        "a concurrent read must preserve listing order"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn source_streams_raw_text_concurrently_without_losing_blobs() {
    // The whole-blob path: `concurrency` bodies really are resident at once
    // here, which is the memory bound the knob advertises. One record per
    // blob makes a dropped or duplicated prefetch immediately visible.
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_azurite().await;
    create_container(port).await;
    let store = seed_store(port);

    for i in 1..=10i64 {
        put_object(
            &store,
            &format!("raw/f-{i:04}.txt"),
            format!("body-{i}").into_bytes(),
        )
        .await;
    }

    let source = AzureBlobSource::new(
        source_config(port)
            .prefix("raw/")
            .file_format(AzureFileFormat::RawText)
            .concurrency(5),
    )
    .await
    .expect("source new");

    let (records, _sizes) = drain_pages(&source).await;
    let bodies: Vec<&str> = records
        .iter()
        .map(|r| r["content"].as_str().unwrap())
        .collect();
    assert_eq!(
        bodies,
        (1..=10).map(|i| format!("body-{i}")).collect::<Vec<_>>(),
        "every blob must appear exactly once, in order"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn source_concurrency_zero_reads_serially_rather_than_stalling() {
    // A `buffered(0)` stream yields nothing forever, so a config of `0` has to
    // be clamped. Pinned here so the clamp is never refactored into a hang.
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_azurite().await;
    create_container(port).await;
    let store = seed_store(port);
    put_object(&store, "z/a.jsonl", b"{\"id\":1}\n{\"id\":2}\n".to_vec()).await;

    let source = AzureBlobSource::new(source_config(port).prefix("z/").concurrency(0))
        .await
        .expect("source new");

    let (records, _sizes) =
        tokio::time::timeout(std::time::Duration::from_secs(60), drain_pages(&source))
            .await
            .expect("concurrency = 0 must read serially, not hang");
    assert_eq!(records.len(), 2);
}
