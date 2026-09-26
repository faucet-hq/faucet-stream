//! `faucet-conformance` Tier-1 battery for the GCS source.
//!
//! Check 1 (config-schema validity) is pure and offline and always runs.
//!
//! Checks 2, 9 and 12 run against `fake-gcs-server`: a plaintext storage host
//! lists over the JSON API (see `faucet-common-gcs`), so the emulator serves
//! the full list + read path. They skip when Docker is unavailable.

#![cfg(not(target_os = "windows"))]

use faucet_conformance::{assert_config_schema_valid_value, assert_errors_not_panics};
use faucet_source_gcs::{GcsCredentials, GcsSource, GcsSourceConfig};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};

// ── Check 1: config schema ──────────────────────────────────────────────────

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(GcsSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-gcs");
}

// ── Check 10: connector_name is non-empty (offline, lazy build) ──────────────
/// Building the storage clients with `Anonymous` creds + an endpoint
/// override performs no I/O, so this runs unconditionally (no emulator needed).
#[tokio::test(flavor = "multi_thread")]
async fn conformance_connector_name_nonempty() {
    let config = GcsSourceConfig::new("does-not-exist")
        .auth(GcsCredentials::Anonymous)
        .storage_host("http://127.0.0.1:1");
    let source = GcsSource::new(config).await.expect("source builds lazily");
    faucet_conformance::assert_connector_name_nonempty(&source);
}

// ── Check 2: bounded-memory streaming (emulator) ────────────────────────────

/// Spawn `fake-gcs-server` and return `(host_url, bucket_name)`.
/// Returns `None` when Docker is unavailable so tests skip cleanly.
async fn spawn_fake_gcs() -> Option<(ContainerAsync<GenericImage>, String, String)> {
    let image = GenericImage::new("fsouza/fake-gcs-server", "latest")
        .with_exposed_port(4443.tcp())
        .with_wait_for(WaitFor::message_on_stderr("server started at"))
        .with_cmd(vec![
            "-scheme=http".to_string(),
            "-public-host=0.0.0.0:4443".to_string(),
        ]);
    let container = match image.start().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping: Docker not available ({e})");
            return None;
        }
    };
    let port = container.get_host_port_ipv4(4443).await.ok()?;
    let host = format!("http://127.0.0.1:{port}");
    let bucket = "faucet-conformance".to_string();

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{host}/storage/v1/b"))
        .json(&serde_json::json!({"name": bucket}))
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() && resp.status() != reqwest::StatusCode::CONFLICT {
        eprintln!("Skipping: could not create bucket ({})", resp.status());
        return None;
    }

    // The handle is returned so the container lives exactly as long as the
    // test that owns it: dropping it stops and removes the container.
    // testcontainers-rs has no reaper — a forgotten handle is a leaked
    // container, one per test, forever.
    Some((container, host, bucket))
}

/// Upload an object via fake-gcs-server's REST surface.
async fn seed_object(host: &str, bucket: &str, name: &str, body: &str, content_type: &str) {
    let client = reqwest::Client::new();
    let url = format!(
        "{host}/upload/storage/v1/b/{bucket}/o?uploadType=media&name={}",
        urlencoding::encode(name)
    );
    client
        .post(url)
        .header("Content-Type", content_type)
        .body(body.to_string())
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
}

/// Build a JSONL body with records `{"id": i}` for `i = 1..=n`.
fn jsonl_body(n: i64) -> String {
    let mut out = String::new();
    for i in 1..=n {
        out.push_str(&format!("{{\"id\":{i}}}\n"));
    }
    out
}

#[tokio::test]
async fn conformance_bounded_memory() {
    let Some((_gcs, host, bucket)) = spawn_fake_gcs().await else {
        return;
    };
    seed_object(
        &host,
        &bucket,
        "data/events.jsonl",
        &jsonl_body(5_000),
        "application/x-ndjson",
    )
    .await;

    let config = GcsSourceConfig::new(&bucket)
        .prefix("data/")
        .auth(GcsCredentials::Anonymous)
        .storage_host(&host)
        .with_batch_size(250);
    let source = GcsSource::new(config).await.unwrap();

    faucet_conformance::assert_bounded_memory(&source, 250, 5_000).await;

    // Check 9: the same seeded object read with `batch_size = 0` is one page.
    let zero = GcsSource::new(
        GcsSourceConfig::new(&bucket)
            .prefix("data/")
            .auth(GcsCredentials::Anonymous)
            .storage_host(&host)
            .with_batch_size(0),
    )
    .await
    .unwrap();
    faucet_conformance::assert_batch_size_zero_single_page(&zero).await;
}

// ── Check 12: discovery round-trips (emulator) ───────────────────────────────

/// Every dataset `discover()` reports must be genuinely selectable: take its
/// config_patch (a `{"prefix": …}` override) and rebuild the source pointed at
/// that prefix, then read it.
#[tokio::test]
async fn conformance_discover_roundtrips() {
    let Some((_gcs, host, bucket)) = spawn_fake_gcs().await else {
        return;
    };
    seed_object(
        &host,
        &bucket,
        "orders/data.jsonl",
        &jsonl_body(3),
        "application/x-ndjson",
    )
    .await;
    seed_object(
        &host,
        &bucket,
        "customers/data.jsonl",
        &jsonl_body(3),
        "application/x-ndjson",
    )
    .await;

    let source = GcsSource::new(
        GcsSourceConfig::new(&bucket)
            .auth(GcsCredentials::Anonymous)
            .storage_host(&host),
    )
    .await
    .unwrap();

    faucet_conformance::assert_discover_roundtrips(&source, |patch| {
        let host = host.clone();
        let bucket = bucket.clone();
        async move {
            let prefix = patch["prefix"].as_str().expect("prefix patch").to_string();
            let cfg = GcsSourceConfig::new(&bucket)
                .prefix(&prefix)
                .auth(GcsCredentials::Anonymous)
                .storage_host(&host);
            Box::new(GcsSource::new(cfg).await.expect("rebuilt source"))
                as Box<dyn faucet_core::Source>
        }
    })
    .await;
}

// ── Check 6: errors, not panics (no container) ──────────────────────────────

/// Point the source at an unreachable GCS storage host (`http://127.0.0.1:1`,
/// which refuses connections immediately) with anonymous credentials. `new()`
/// stays lazy — building the storage clients with `Anonymous` creds + an
/// endpoint override does not perform I/O (see `faucet-common-gcs` tests) — so
/// no container is needed. The first list/get RPC fails with a typed
/// `FaucetError` on both the `fetch_all` and `stream_pages` paths, never a
/// panic.
///
/// Check 6 only needs the *failure* path, which an unreachable host reproduces
/// deterministically — so it needs no container.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_errors_not_panics() {
    let config = GcsSourceConfig::new("does-not-exist")
        .prefix("data/")
        .auth(GcsCredentials::Anonymous)
        .storage_host("http://127.0.0.1:1")
        .with_batch_size(250);
    let source = GcsSource::new(config).await.expect("source builds lazily");
    assert_errors_not_panics(&source).await;
}
