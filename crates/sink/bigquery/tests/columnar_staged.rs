//! GCS-staged columnar Parquet load (#380), driven against wiremock GCS +
//! BigQuery.
//!
//! #803: google-cloud-storage >= 1.19 sends a single-shot upload's CRC32C as a
//! third multipart part unless the checksum is declared up front. A server
//! that reads only metadata + media (fake-gcs-server) stores that part as
//! object content, so the staging upload must declare the checksum in its
//! metadata part.
#![cfg(feature = "arrow")]

use arrow::array::{ArrayRef, StringArray};
use arrow::record_batch::RecordBatch;
use base64::Engine as _;
use faucet_core::Sink;
use faucet_sink_bigquery::{BigQueryCredentials, BigQuerySink, BigQuerySinkConfig};
use gcp_bigquery_client::client_builder::ClientBuilder;
use serde::Serialize;
use serde_json::json;
use std::sync::Arc;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

const PROJECT_ID: &str = "p";
const DATASET_ID: &str = "d";
const TABLE_ID: &str = "t";
const BUCKET: &str = "stage-bucket";
const AUTH_TOKEN_PATH: &str = "/:o/oauth2/token";
const AUTH_SCOPE_BASE: &str = "/auth/bigquery";
const JOB_ID: &str = "job-staged-1";

#[derive(Serialize)]
struct FakeToken {
    access_token: &'static str,
    token_type: &'static str,
    expires_in: u32,
}

fn dummy_service_account_json(oauth_server: &str) -> serde_json::Value {
    let token_uri = format!("{oauth_server}{AUTH_TOKEN_PATH}");
    json!({
        "type": "service_account",
        "project_id": "dummy",
        "private_key_id": "dummy",
        "private_key": include_str!("fixtures/bigquery_test_key.pem"),
        "client_email": "dummy@developer.gserviceaccount.com",
        "client_id": "dummy",
        "auth_uri": "https://example.invalid/o/oauth2/auth",
        "token_uri": token_uri,
    })
}

/// Answers a GCS upload the way a server that keeps the declared checksum
/// does: the object resource echoes the metadata's `crc32c`.
struct EchoDeclaredCrc;

impl Respond for EchoDeclaredCrc {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let body = String::from_utf8_lossy(&request.body);
        let mut object = json!({ "bucket": BUCKET, "name": "staged.parquet" });
        if let Some(rest) = body.split("\"crc32c\":\"").nth(1) {
            let crc = rest.split('"').next().unwrap_or_default();
            object["crc32c"] = json!(crc);
        }
        ResponseTemplate::new(200).set_body_json(object)
    }
}

async fn mount(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path(AUTH_TOKEN_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(FakeToken {
            access_token: "fake-token",
            token_type: "bearer",
            expires_in: 9_999_999,
        }))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path(format!("/upload/storage/v1/b/{BUCKET}/o")))
        .respond_with(EchoDeclaredCrc)
        .expect(1)
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path(format!("/projects/{PROJECT_ID}/jobs")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobReference": { "projectId": PROJECT_ID, "jobId": JOB_ID },
            "status": { "state": "RUNNING" }
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/projects/{PROJECT_ID}/jobs/{JOB_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobReference": { "projectId": PROJECT_ID, "jobId": JOB_ID },
            "status": { "state": "DONE" }
        })))
        .mount(server)
        .await;
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(204))
        .mount(server)
        .await;
}

async fn build_sink(server: &MockServer) -> BigQuerySink {
    let sa_file = tempfile::NamedTempFile::new().expect("sa tempfile");
    std::fs::write(
        sa_file.path(),
        serde_json::to_string_pretty(&dummy_service_account_json(&server.uri())).unwrap(),
    )
    .expect("write sa");
    let client = ClientBuilder::new()
        .with_auth_base_url(format!("{}{AUTH_SCOPE_BASE}", server.uri()))
        .with_v2_base_url(server.uri())
        .build_from_service_account_key_file(sa_file.path().to_str().unwrap())
        .await
        .expect("build client");
    std::mem::forget(sa_file);
    let mut config = BigQuerySinkConfig::new(
        PROJECT_ID,
        DATASET_ID,
        TABLE_ID,
        BigQueryCredentials::ServiceAccountKey {
            json: serde_json::to_string(&dummy_service_account_json(&server.uri())).unwrap(),
        },
    );
    config.upload_base_url = Some(server.uri());
    config.bulk_load = Some(
        serde_json::from_value(json!({
            "staging_bucket": BUCKET,
            "gcs_auth": { "type": "anonymous" },
            "storage_host": server.uri(),
        }))
        .expect("bulk_load config"),
    );
    BigQuerySink::from_parts(config, client)
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

#[tokio::test]
async fn the_staging_upload_declares_its_crc32c_in_the_metadata_part() {
    let server = MockServer::start().await;
    mount(&server).await;
    let sink = build_sink(&server).await;

    let batch = RecordBatch::try_from_iter(vec![(
        "id",
        Arc::new(StringArray::from(vec!["1", "2", "3"])) as ArrayRef,
    )])
    .expect("batch");
    let n = sink
        .write_batch_columnar(&batch)
        .await
        .expect("staged columnar write");
    assert_eq!(n, 3);

    let requests = server.received_requests().await.unwrap();
    let upload = requests
        .iter()
        .find(|r| r.url.path().starts_with("/upload/storage"))
        .expect("a staging upload");
    let content_type = upload.headers["content-type"].to_str().unwrap();
    let boundary = content_type
        .split("boundary=")
        .nth(1)
        .unwrap()
        .trim_matches('"');
    let delimiter = format!("--{boundary}\r\n");
    let body = &upload.body;
    let text = String::from_utf8_lossy(body);
    assert_eq!(
        text.matches(&delimiter).count(),
        2,
        "metadata + media only, no trailing checksum part"
    );

    // The media part is the Parquet file; its CRC32C is the one declared.
    let media_start = find(body, delimiter.as_bytes())
        .and_then(|first| {
            let after = first + delimiter.len();
            find(&body[after..], delimiter.as_bytes()).map(|second| after + second)
        })
        .and_then(|second| find(&body[second..], b"\r\n\r\n").map(|h| second + h + 4))
        .expect("media part");
    let media_end = media_start
        + find(&body[media_start..], format!("\r\n--{boundary}").as_bytes())
            .expect("closing delimiter");
    let media = &body[media_start..media_end];
    assert!(media.starts_with(b"PAR1"), "the media part is Parquet");
    let expected = base64::engine::general_purpose::STANDARD.encode(crc32c_of(media).to_be_bytes());
    assert!(
        text.contains(&format!("\"crc32c\":\"{expected}\"")),
        "the checksum travels in the metadata part: {text:.600}"
    );
    assert!(
        requests
            .iter()
            .any(|r| r.url.path() == format!("/projects/{PROJECT_ID}/jobs")),
        "the load job is inserted after the upload"
    );
}

/// CRC-32C (Castagnoli), bitwise; independent of the crate under test.
fn crc32c_of(bytes: &[u8]) -> u32 {
    let mut crc = !0_u32;
    for &b in bytes {
        crc ^= u32::from(b);
        for _ in 0..8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ 0x82F6_3B78
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

async fn mount_token(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path(AUTH_TOKEN_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(FakeToken {
            access_token: "fake-token",
            token_type: "bearer",
            expires_in: 9_999_999,
        }))
        .mount(server)
        .await;
}

fn one_column_batch(values: Vec<&str>) -> RecordBatch {
    RecordBatch::try_from_iter(vec![(
        "id",
        Arc::new(StringArray::from(values)) as ArrayRef,
    )])
    .expect("batch")
}

/// A staging upload the storage backend refuses fails the write before any
/// load job is inserted.
#[tokio::test]
async fn a_refused_staging_upload_fails_the_columnar_write() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    Mock::given(method("POST"))
        .and(path(format!("/upload/storage/v1/b/{BUCKET}/o")))
        .respond_with(ResponseTemplate::new(403).set_body_json(json!({
            "error": {"code": 403, "message": "forbidden"}
        })))
        .mount(&server)
        .await;
    let sink = build_sink(&server).await;
    let err = sink
        .write_batch_columnar(&one_column_batch(vec!["1"]))
        .await
        .expect_err("a refused upload fails the write");
    assert!(err.to_string().contains("staging upload failed"), "{err}");
    let requests = server.received_requests().await.unwrap();
    assert!(
        !requests
            .iter()
            .any(|r| r.url.path() == format!("/projects/{PROJECT_ID}/jobs")),
        "no load job after a failed upload"
    );
}

/// A load job still running past `job_timeout` is polled, then cancelled; a
/// cancel that lands fails the write as cancelled.
#[tokio::test]
async fn a_load_job_past_job_timeout_is_cancelled() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    Mock::given(method("POST"))
        .and(path(format!("/upload/storage/v1/b/{BUCKET}/o")))
        .respond_with(EchoDeclaredCrc)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(format!("/projects/{PROJECT_ID}/jobs")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobReference": { "projectId": PROJECT_ID, "jobId": JOB_ID },
            "status": { "state": "RUNNING" }
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/projects/{PROJECT_ID}/jobs/{JOB_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobReference": { "projectId": PROJECT_ID, "jobId": JOB_ID },
            "status": { "state": "RUNNING" }
        })))
        .up_to_n_times(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/projects/{PROJECT_ID}/jobs/{JOB_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobReference": { "projectId": PROJECT_ID, "jobId": JOB_ID },
            "status": {
                "state": "DONE",
                "errorResult": {"reason": "stopped", "message": "Job cancelled"}
            }
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(format!("/projects/{PROJECT_ID}/jobs/{JOB_ID}/cancel")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "kind": "bigquery#jobCancelResponse",
            "job": {"jobReference": {"projectId": PROJECT_ID, "jobId": JOB_ID}}
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;
    let sink = build_sink_with(&server, |c| {
        c.job_timeout = std::time::Duration::from_millis(300);
    })
    .await;
    let err = sink
        .write_batch_columnar(&one_column_batch(vec!["1", "2"]))
        .await
        .expect_err("a load past job_timeout fails");
    let msg = err.to_string();
    assert!(
        msg.contains("job_timeout") && msg.contains("cancelled"),
        "{msg}"
    );
}

/// The public `write_columnar` entry point stages nothing for an empty batch.
#[tokio::test]
async fn write_columnar_with_an_empty_batch_is_a_no_op() {
    let server = MockServer::start().await;
    let (client, config) = build_parts(&server, |_| {}).await;
    let n = faucet_sink_bigquery::load::write_columnar(
        &client,
        &config,
        &tokio::sync::OnceCell::new(),
        &one_column_batch(vec![]),
    )
    .await
    .expect("empty batch");
    assert_eq!(n, 0);
    assert!(server.received_requests().await.unwrap().is_empty());
}

async fn build_parts(
    server: &MockServer,
    f: impl FnOnce(&mut BigQuerySinkConfig),
) -> (gcp_bigquery_client::Client, BigQuerySinkConfig) {
    let sa_file = tempfile::NamedTempFile::new().expect("sa tempfile");
    std::fs::write(
        sa_file.path(),
        serde_json::to_string_pretty(&dummy_service_account_json(&server.uri())).unwrap(),
    )
    .expect("write sa");
    let client = ClientBuilder::new()
        .with_auth_base_url(format!("{}{AUTH_SCOPE_BASE}", server.uri()))
        .with_v2_base_url(server.uri())
        .build_from_service_account_key_file(sa_file.path().to_str().unwrap())
        .await
        .expect("build client");
    let mut config = BigQuerySinkConfig::new(
        PROJECT_ID,
        DATASET_ID,
        TABLE_ID,
        BigQueryCredentials::ServiceAccountKey {
            json: serde_json::to_string(&dummy_service_account_json(&server.uri())).unwrap(),
        },
    );
    config.upload_base_url = Some(server.uri());
    config.bulk_load = Some(
        serde_json::from_value(json!({
            "staging_bucket": BUCKET,
            "gcs_auth": { "type": "anonymous" },
            "storage_host": server.uri(),
        }))
        .expect("bulk_load config"),
    );
    f(&mut config);
    (client, config)
}

async fn build_sink_with(
    server: &MockServer,
    f: impl FnOnce(&mut BigQuerySinkConfig),
) -> BigQuerySink {
    let (client, config) = build_parts(server, f).await;
    BigQuerySink::from_parts(config, client)
}
