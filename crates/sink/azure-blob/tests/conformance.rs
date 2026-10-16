//! `faucet-conformance` battery for the Azure Blob / ADLS Gen2 sink.
//!
//! Check 1 (config schema) is pure/offline and always runs. Check 5
//! (capabilities truthful) **auto-starts** an Azurite emulator in Docker via
//! `testcontainers`; it skips cleanly when Docker is unavailable and runs for
//! real in CI. The Azure Blob sink is append-only (JSONL objects, no
//! idempotent-watermark / keyed-upsert mechanism), so check 5 takes the
//! honest-`false` branch: Append works and no phantom commit token is recorded.
//! The destination count is read back through an `object_store` Azure client.
//! Passing this battery in CI is the Tier-1 (supported) criterion.

#![cfg(not(target_os = "windows"))]

use std::sync::Arc;

use faucet_conformance::{assert_capabilities_truthful, assert_config_schema_valid_value};
use faucet_core::Sink as _;
use faucet_sink_azure_blob::{AzureBlobSink, AzureBlobSinkConfig, AzureCredentials};
use futures::StreamExt;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt};
use testcontainers_modules::azurite::{Azurite, BLOB_PORT};
use testcontainers_modules::testcontainers::ContainerAsync;

const AZURITE_ACCOUNT: &str = "devstoreaccount1";
const AZURITE_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
const CONTAINER: &str = "faucet-test";

/// Start Azurite, or `None` when Docker is unavailable.
async fn start_azurite() -> Option<(ContainerAsync<Azurite>, u16)> {
    let container = faucet_conformance::containers::start_or_skip(
        || {
            // In-memory storage: a nearly full runner disk must not fail the emulator.
            testcontainers::ImageExt::with_cmd(
                Azurite::default(),
                [
                    "azurite",
                    "--blobHost",
                    "0.0.0.0",
                    "--queueHost",
                    "0.0.0.0",
                    "--tableHost",
                    "0.0.0.0",
                    "--inMemoryPersistence",
                ],
            )
        },
        &Default::default(),
    )
    .await?;
    let port = container.get_host_port_ipv4(BLOB_PORT).await.ok()?;
    Some((container, port))
}

fn endpoint(port: u16) -> String {
    format!("http://127.0.0.1:{port}/{AZURITE_ACCOUNT}")
}

/// Create the destination blob container (`object_store` has no
/// container-management API).
async fn create_container(port: u16) {
    faucet_common_azure::testing::create_container(
        &format!("http://127.0.0.1:{port}/{AZURITE_ACCOUNT}"),
        AZURITE_ACCOUNT,
        AZURITE_KEY,
        CONTAINER,
    )
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

/// Total JSONL rows across every object under `prefix` (the destination count).
async fn count_rows(port: u16, prefix: &str) -> usize {
    let store = verify_store(port);
    let mut listing = store.list(Some(&ObjPath::from(prefix)));
    let mut total = 0usize;
    while let Some(meta) = listing.next().await {
        let key = meta.expect("list meta").location;
        let bytes = store
            .get(&key)
            .await
            .expect("get object")
            .bytes()
            .await
            .expect("read bytes");
        let text = String::from_utf8(bytes.to_vec()).expect("utf8 body");
        total += text.lines().filter(|l| !l.trim().is_empty()).count();
    }
    total
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

// ── Check 1: config schema validity (pure, offline) ──────────────────────────
#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(AzureBlobSinkConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "azure-blob");
}

// ── Check 10: connector_name is non-empty (offline, lazy build) ──────────────
/// The `object_store` Azure builder is lazy, so this runs unconditionally with
/// no container.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_connector_name_nonempty() {
    let config = AzureBlobSinkConfig::new(CONTAINER)
        .account(AZURITE_ACCOUNT)
        .auth(AzureCredentials::AccountKey {
            account_key: AZURITE_KEY.into(),
        })
        .endpoint("http://127.0.0.1:1")
        .allow_http(true);
    let sink = AzureBlobSink::new(config)
        .await
        .expect("sink builds lazily");
    faucet_conformance::assert_batch_atomicity_declared(&sink);
    faucet_conformance::assert_connector_name_nonempty_value(
        sink.connector_name(),
        sink.connector_name(),
    );
}

// ── Check 5: capabilities are truthful (Azurite, skip if no Docker) ──────────
#[tokio::test(flavor = "multi_thread")]
async fn conformance_capabilities_truthful() {
    let Some((_c, port)) = start_azurite().await else {
        faucet_conformance::containers::backend_missing("Docker unavailable");
        return;
    };
    create_container(port).await;

    // batch_size 0 → each write_batch flushes its page as a self-contained
    // object, so the destination count reflects every write immediately.
    let sink = AzureBlobSink::new(sink_config(port).prefix("conf/").with_batch_size(0))
        .await
        .expect("sink new");

    let sink_ref = &sink;
    assert_capabilities_truthful(&sink, move || async move {
        sink_ref.flush().await.expect("flush");
        count_rows(port, "conf").await
    })
    .await;

    assert!(!sink.supports_idempotent_writes());
    assert!(!sink.dedups_by_key());
    // RFC 0014: the sink runs on the object-store family driver.
    faucet_conformance::assert_driver_declarations_truthful(&sink).await;
    faucet_conformance::assert_driver_row_paths_agree(&sink).await;
}

// ── Descriptor (RFC 0013): agrees with the built sink ───────────────────────

fn descriptor_config(base: serde_json::Value, extra: serde_json::Value) -> serde_json::Value {
    let mut v = base;
    for (k, x) in extra.as_object().unwrap() {
        v[k] = x.clone();
    }
    v
}

#[tokio::test]
async fn descriptor_matches_built_sink() {
    let base = serde_json::to_value(
        AzureBlobSinkConfig::new("c")
            .account("devstoreaccount1")
            .auth(AzureCredentials::AccountKey {
                account_key: "a2V5".into(),
            })
            .endpoint("http://127.0.0.1:1/devstoreaccount1")
            .allow_http(true),
    )
    .unwrap();
    let mut cases = vec![
        serde_json::json!({}),
        serde_json::json!({"format": "raw_text"}),
        serde_json::json!({"format": "auto", "path": "x.json"}),
        serde_json::json!({"format": "auto", "file_extension": ".txt"}),
    ];
    #[cfg(feature = "file-format-csv")]
    cases.extend([
        serde_json::json!({"format": "csv", "csv": {"on_unknown_field": "warn"}}),
        serde_json::json!({"format": "csv", "csv": {"on_unknown_field": "error"}}),
    ]);
    #[cfg(feature = "arrow")]
    cases.push(serde_json::json!({"format": "auto", "path": "x-{part}.parquet"}));
    for extra in cases {
        let v = descriptor_config(base.clone(), extra);
        let cfg: AzureBlobSinkConfig = serde_json::from_value(v.clone()).unwrap();
        let built = AzureBlobSink::new(cfg).await.expect("sink builds offline");
        faucet_conformance::assert_descriptor_matches_sink(
            &faucet_sink_azure_blob::DESCRIPTOR,
            "azure-blob",
            &v,
            &built,
        );
    }
}

// ── Check: backend calls honour the call deadline and cancel (#880) ──────────

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(f))
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_call_deadline() {
    let hung = faucet_conformance::hung::SilentListener::start()
        .await
        .expect("listener");
    let endpoint = hung.http_url();
    faucet_conformance::calls::assert_sink_call_deadline(
        || {
            let config = AzureBlobSinkConfig::new("hung")
                .account(AZURITE_ACCOUNT)
                .auth(AzureCredentials::AccountKey {
                    account_key: AZURITE_KEY.into(),
                })
                .endpoint(&endpoint)
                .allow_http(true);
            block_on(AzureBlobSink::new(config)).expect("sink builds offline")
        },
        &[serde_json::json!({"id": 1})],
    )
    .await;
}

// ── #878: declared capabilities against Azurite ───────────────────────────────

/// Rows under `prefix` a reader of the destination sees: the overwrite swap
/// area (`.faucet-overwrite-…/`) and its markers are not part of it.
async fn visible_rows(port: u16, prefix: &str) -> usize {
    let store = verify_store(port);
    let mut listing = store.list(Some(&ObjPath::from(prefix)));
    let mut total = 0usize;
    while let Some(meta) = listing.next().await {
        let key = meta.expect("list meta").location;
        if key.as_ref().contains("/.faucet-") {
            continue;
        }
        let bytes = store
            .get(&key)
            .await
            .expect("get object")
            .bytes()
            .await
            .expect("read bytes");
        total += String::from_utf8_lossy(&bytes)
            .lines()
            .filter(|l| !l.trim().is_empty())
            .count();
    }
    total
}

/// The blob is committed when the sink flushes, so a cancelled run must still
/// flush; and `write_mode: overwrite` stages in a swap area until commit.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_declared_capabilities() {
    let Some((_c, port)) = start_azurite().await else {
        faucet_conformance::containers::backend_missing("Docker unavailable");
        return;
    };
    create_container(port).await;

    let sink = AzureBlobSink::new(sink_config(port).prefix("cancel/"))
        .await
        .expect("sink new");
    faucet_conformance::assert_cancellation_flushes(&sink, move || async move {
        visible_rows(port, "cancel").await
    })
    .await;

    verify_store(port)
        .put(
            &ObjPath::from("ovw/out-00001.jsonl"),
            "{\"id\":-1}\n{\"id\":-2}\n{\"id\":-3}\n".into(),
        )
        .await
        .expect("seed blob");
    let mut cfg = sink_config(port);
    cfg.path = Some("ovw/out-{part}.jsonl".into());
    cfg.write_mode = faucet_common_file::write::FileWriteMode::Overwrite;
    let overwrite = AzureBlobSink::new(cfg).await.expect("overwrite sink");
    faucet_conformance::assert_overwrite_lifecycle(&overwrite, move || async move {
        visible_rows(port, "ovw").await
    })
    .await;
}
