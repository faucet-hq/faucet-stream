//! The Azure Blob sink's failure paths (#777), against a mock Blob endpoint:
//! a refused listing fails the overwrite abort and the preflight, and a slow
//! endpoint times the preflight out.

use faucet_core::Sink;
use faucet_core::check::{CheckContext, ProbeStatus};
use faucet_sink_azure_blob::{AzureBlobSink, AzureBlobSinkConfig, AzureCredentials};
use serde_json::{Value, json};
use std::time::Duration;
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

const KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

fn config(server: &MockServer) -> AzureBlobSinkConfig {
    AzureBlobSinkConfig::new("c")
        .account("devstoreaccount1")
        .auth(AzureCredentials::AccountKey {
            account_key: KEY.into(),
        })
        .endpoint(server.uri())
        .allow_http(true)
}

async fn sink(server: &MockServer, fields: Value) -> AzureBlobSink {
    let mut cfg = serde_json::to_value(config(server)).unwrap();
    for (k, v) in fields.as_object().unwrap() {
        cfg[k] = v.clone();
    }
    AzureBlobSink::new(serde_json::from_value(cfg).unwrap())
        .await
        .expect("AzureBlobSink::new")
}

async fn refuse_everything(server: &MockServer) {
    Mock::given(|_: &Request| true)
        .respond_with(ResponseTemplate::new(403))
        .mount(server)
        .await;
}

fn ctx() -> CheckContext {
    CheckContext {
        timeout: Duration::from_millis(300),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_listing_fails_the_overwrite_abort() {
    let server = MockServer::start().await;
    refuse_everything(&server).await;
    let s = sink(
        &server,
        json!({"path": "d/", "if_exists": "replace", "write_mode": "overwrite"}),
    )
    .await;
    assert!(s.is_overwrite());
    let e = s.abort_overwrite().await.unwrap_err().to_string();
    assert!(
        e.contains("azure head error for key 'd/.faucet-overwrite-"),
        "{e}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_listing_fails_the_preflight() {
    let server = MockServer::start().await;
    refuse_everything(&server).await;
    let report = sink(&server, json!({})).await.check(&ctx()).await.unwrap();
    let probe = &report.probes[0];
    assert!(
        matches!(probe.status, ProbeStatus::Fail { .. }),
        "{probe:?}"
    );
    assert_eq!(
        probe.hint.as_deref(),
        Some("check account, container, credentials, and network")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_slow_endpoint_times_the_preflight_out() {
    let server = MockServer::start().await;
    Mock::given(|_: &Request| true)
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(5)))
        .mount(&server)
        .await;
    let report = sink(&server, json!({})).await.check(&ctx()).await.unwrap();
    let probe = &report.probes[0];
    assert!(
        matches!(&probe.status, ProbeStatus::Fail { reason } if reason == "timed out"),
        "{probe:?}"
    );
    assert_eq!(probe.name, "network");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_part_token_in_the_directory_is_a_config_error() {
    let server = MockServer::start().await;
    let mut v = serde_json::to_value(config(&server)).unwrap();
    v["path"] = json!("a-{part}/x.jsonl");
    let e = AzureBlobSink::new(serde_json::from_value(v).unwrap())
        .await
        .err()
        .expect("refused")
        .to_string();
    assert!(e.contains("azure-blob sink: "), "{e}");
    assert!(e.contains("may appear only in the file name"), "{e}");
}

#[cfg(feature = "arrow")]
#[tokio::test(flavor = "multi_thread")]
async fn a_parquet_sink_encodes_batches_before_any_upload() {
    let server = MockServer::start().await;
    refuse_everything(&server).await;
    let s = sink(&server, json!({"format": "parquet"})).await;
    assert!(s.supports_columnar());
    let rows = [json!({"a": 1}), json!({"a": 2})];
    let batch = faucet_core::columnar::values_to_record_batch(
        &rows,
        faucet_core::columnar::infer_arrow_schema(&rows).unwrap(),
    )
    .unwrap();
    assert_eq!(s.write_batch_columnar(&batch).await.unwrap(), 2);
    assert!(
        s.flush().await.is_err(),
        "the refused upload fails the flush"
    );
    let lines = sink(&server, json!({"path": "x.jsonl"})).await;
    assert!(!lines.supports_columnar());
}
