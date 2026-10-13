//! `faucet-conformance` battery for the Azure Blob source.
//!
//! Check 1 (config-schema validity) is pure and offline and always runs.
//!
//! Check 6 (errors, not panics) points the source at an unreachable endpoint
//! and verifies the read paths surface typed `FaucetError`s rather than
//! panicking. The `object_store` builder is lazy (no I/O at build time), so no
//! container is needed — only the failure path is exercised, which an
//! unreachable host reproduces deterministically.

use faucet_conformance::{assert_config_schema_valid_value, assert_errors_not_panics};
use faucet_source_azure_blob::{AzureBlobSource, AzureBlobSourceConfig, AzureCredentials};

// ── Check 1: config schema ──────────────────────────────────────────────────

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(AzureBlobSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-azure-blob");
}

// ── Check 10: connector_name is non-empty (offline, lazy build) ──────────────
/// The `object_store` Azure builder is lazy, so this runs unconditionally with
/// no container.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_connector_name_nonempty() {
    let config = AzureBlobSourceConfig::new("does-not-exist")
        .account("devstoreaccount1")
        .auth(AzureCredentials::AccountKey {
            account_key: "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==".into(),
        })
        .endpoint("http://127.0.0.1:1")
        .allow_http(true);
    let source = AzureBlobSource::new(config)
        .await
        .expect("source builds lazily");
    faucet_conformance::assert_connector_name_nonempty(&source);
}

// ── Check 6: errors, not panics (no container) ──────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn conformance_errors_not_panics() {
    // `http://127.0.0.1:1` refuses connections immediately. Anonymous-style
    // build stays lazy, so `new()` succeeds; the first list/get RPC fails with
    // a typed `FaucetError` on both `fetch_all` and `stream_pages`.
    let config = AzureBlobSourceConfig::new("does-not-exist")
        .account("devstoreaccount1")
        .prefix("data/")
        .auth(AzureCredentials::AccountKey {
            account_key: "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==".into(),
        })
        .endpoint("http://127.0.0.1:1")
        .allow_http(true)
        .with_batch_size(250);
    let source = AzureBlobSource::new(config)
        .await
        .expect("source builds lazily");
    assert_errors_not_panics(&source).await;
}

// ── Descriptor (RFC 0013): agrees with the built source ─────────────────────

fn descriptor_config(base: serde_json::Value, extra: serde_json::Value) -> serde_json::Value {
    let mut v = base;
    for (k, x) in extra.as_object().unwrap() {
        v[k] = x.clone();
    }
    v
}

#[tokio::test(flavor = "multi_thread")]
async fn descriptor_matches_built_source() {
    let base = serde_json::to_value(
        AzureBlobSourceConfig::new("c")
            .account("devstoreaccount1")
            .auth(AzureCredentials::AccountKey {
                account_key: "a2V5".into(),
            })
            .endpoint("http://127.0.0.1:1")
            .allow_http(true),
    )
    .unwrap();
    let mut cases = vec![
        serde_json::json!({}),
        serde_json::json!({"file_format": "json_array"}),
        serde_json::json!({"file_format": "raw_text"}),
    ];
    #[cfg(feature = "arrow")]
    cases.push(serde_json::json!({"file_format": "parquet"}));
    #[cfg(feature = "file-format-avro")]
    cases.push(serde_json::json!({"file_format": "avro"}));
    #[cfg(feature = "file-format-orc")]
    cases.push(serde_json::json!({"file_format": "orc"}));
    for extra in cases {
        let v = descriptor_config(base.clone(), extra);
        let cfg: AzureBlobSourceConfig = serde_json::from_value(v.clone()).unwrap();
        let built = AzureBlobSource::new(cfg)
            .await
            .expect("source builds lazily");
        faucet_conformance::assert_descriptor_matches_source(
            &faucet_source_azure_blob::DESCRIPTOR,
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
    faucet_conformance::calls::assert_source_call_deadline(|| {
        let config = AzureBlobSourceConfig::new("hung")
            .account("devstoreaccount1")
            .auth(AzureCredentials::AccountKey {
                account_key: "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==".into(),
            })
            .endpoint(&endpoint)
            .allow_http(true);
        block_on(AzureBlobSource::new(config)).expect("source builds offline")
    })
    .await;
}

/// The blob's headers arrive, then its body stops mid-stream: the read must
/// still end at the deadline and on cancel, because the body is read under the
/// call context too (#880), not only the request.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_call_deadline_bounds_a_stalled_body() {
    use faucet_conformance::hung::{Reply, StallingBodyServer};
    let server = StallingBodyServer::start(|_method, _target| {
        Reply::stall_body("application/octet-stream", "{\"id\":1}\n", 4096)
            .header("last-modified", "Thu, 01 Jan 2026 00:00:00 GMT")
            .header("etag", "\"0x1\"")
            .header("x-ms-blob-type", "BlockBlob")
    })
    .await
    .expect("server");
    let endpoint = server.http_url();
    faucet_conformance::calls::assert_source_call_deadline(|| {
        let config = AzureBlobSourceConfig::new("stalled")
            .account("devstoreaccount1")
            .auth(AzureCredentials::AccountKey {
                account_key: "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==".into(),
            })
            .endpoint(&endpoint)
            .allow_http(true)
            .object_keys(vec!["a.jsonl".into()]);
        block_on(AzureBlobSource::new(config)).expect("source builds offline")
    })
    .await;
}
