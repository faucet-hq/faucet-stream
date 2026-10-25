//! Bucket-free columnar Parquet load (#635), driven against a wiremock
//! BigQuery.
//!
//! The GCS-staged route needs a bucket to create, grant and garbage-collect
//! for a file read exactly once by the job that follows it. This route posts
//! the Parquet with the job instead. What only an integration test can show is
//! the part that is silently wrong otherwise: that the request really is a
//! `multipart/related` upload whose metadata says `PARQUET`, whose media part
//! is a real Parquet file, and whose resulting job is polled to `DONE` rather
//! than assumed successful from the upload's 200.
#![cfg(feature = "arrow")]

use arrow::array::{ArrayRef, StringArray};
use arrow::record_batch::RecordBatch;
use faucet_core::Sink;
use faucet_sink_bigquery::{BigQueryCredentials, BigQuerySink, BigQuerySinkConfig};
use gcp_bigquery_client::client_builder::ClientBuilder;
use serde::Serialize;
use serde_json::json;
use std::sync::Arc;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

const PROJECT_ID: &str = "p";
const DATASET_ID: &str = "d";
const TABLE_ID: &str = "t";
const AUTH_TOKEN_PATH: &str = "/:o/oauth2/token";
const AUTH_SCOPE_BASE: &str = "/auth/bigquery";
const JOB_ID: &str = "job-columnar-1";

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

/// Token endpoint, the multipart upload endpoint, and the job poll.
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
        .and(path(format!(
            "/upload/bigquery/v2/projects/{PROJECT_ID}/jobs"
        )))
        .and(query_param("uploadType", "multipart"))
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
}

async fn build_sink(server: &MockServer, config: BigQuerySinkConfig) -> BigQuerySink {
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
    // The temp file must outlive the client's lazy auth; leak it for the test.
    std::mem::forget(sa_file);
    BigQuerySink::from_parts(config, client)
}

fn config(server: &MockServer) -> BigQuerySinkConfig {
    let mut c = BigQuerySinkConfig::new(
        PROJECT_ID,
        DATASET_ID,
        TABLE_ID,
        BigQueryCredentials::ServiceAccountKey {
            json: serde_json::to_string(&dummy_service_account_json(&server.uri())).unwrap(),
        },
    );
    c.upload_base_url = Some(server.uri());
    c
}

fn batch(ids: &[&str]) -> RecordBatch {
    RecordBatch::try_from_iter(vec![(
        "id",
        Arc::new(StringArray::from(ids.to_vec())) as ArrayRef,
    )])
    .expect("batch")
}

/// Raw bodies of every multipart upload POST, in order.
async fn upload_bodies(server: &MockServer) -> Vec<Vec<u8>> {
    server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.url.path().contains("/upload/bigquery"))
        .map(|r| r.body.clone())
        .collect()
}

#[tokio::test]
async fn a_bucket_free_batch_uploads_parquet_and_polls_the_job() {
    let server = MockServer::start().await;
    mount(&server).await;
    let sink = build_sink(&server, config(&server)).await;

    let n = sink
        .write_batch_columnar(&batch(&["1", "2", "3"]))
        .await
        .expect("bucket-free columnar write");
    assert_eq!(n, 3, "the row count is the batch's");

    let bodies = upload_bodies(&server).await;
    assert_eq!(bodies.len(), 1, "one multipart upload per batch");
    let body = &bodies[0];
    let text = String::from_utf8_lossy(body);

    // Metadata part: a PARQUET load into the configured table.
    assert!(text.contains("\"sourceFormat\":\"PARQUET\""), "{text:.400}");
    assert!(text.contains("\"writeDisposition\":\"WRITE_APPEND\""));
    assert!(text.contains("\"tableId\":\"t\""));
    assert!(text.contains("Content-Type: application/octet-stream"));

    // Media part: a real Parquet file. `PAR1` both opens and closes the
    // format, so finding it proves the batch was encoded, not just framed.
    assert!(
        body.windows(4).any(|w| w == b"PAR1"),
        "the media part must be actual Parquet"
    );

    // The job is polled: a load job returns 200 and can still fail later, so
    // trusting the upload's status code would report success on a failed load.
    let polled = server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|r| r.url.path().ends_with(&format!("/jobs/{JOB_ID}")))
        .count();
    assert!(polled >= 1, "the load job must be polled to DONE");
}

/// An empty batch must not open a load job at all — an empty source leaves the
/// destination untouched, and a zero-row `WRITE_TRUNCATE` would wipe it.
#[tokio::test]
async fn an_empty_batch_uploads_nothing() {
    let server = MockServer::start().await;
    mount(&server).await;
    let sink = build_sink(&server, config(&server)).await;

    let n = sink
        .write_batch_columnar(&batch(&[]))
        .await
        .expect("empty write");
    assert_eq!(n, 0);
    assert!(
        upload_bodies(&server).await.is_empty(),
        "an empty batch must issue no upload"
    );
}

/// Overwrite truncates on the **first** batch and appends after. Truncating on
/// every batch would leave only the last one in the table — a silent,
/// green-run data loss.
/// The resumable-upload pair later batches feed: initiate (`Location` back at
/// the mock) and the finalize PUT returning a DONE job.
async fn mount_resumable(server: &MockServer) {
    let session = format!("{}/session/col", server.uri());
    Mock::given(method("POST"))
        .and(path(format!(
            "/upload/bigquery/v2/projects/{PROJECT_ID}/jobs"
        )))
        .and(query_param("uploadType", "resumable"))
        .respond_with(ResponseTemplate::new(200).insert_header("location", session.as_str()))
        .mount(server)
        .await;
    Mock::given(method("PUT"))
        .and(path("/session/col"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobReference": { "projectId": PROJECT_ID, "jobId": JOB_ID },
            "status": { "state": "DONE" }
        })))
        .mount(server)
        .await;
}

/// Upload POSTs in order, each as (`uploadType`, body text).
async fn uploads(server: &MockServer) -> Vec<(String, String)> {
    server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.method.as_str() == "POST" && r.url.path().contains("/upload/bigquery"))
        .map(|r| {
            let kind = r
                .url
                .query_pairs()
                .find(|(k, _)| k == "uploadType")
                .map(|(_, v)| v.to_string())
                .unwrap_or_default();
            (kind, String::from_utf8_lossy(&r.body).to_string())
        })
        .collect()
}

/// An overwrite run's batches all join one `WRITE_TRUNCATE` load finalized on
/// flush, so nothing replaces the table before the run completes. This used
/// to assert a truncating Parquet load committed by the first batch — the
/// non-atomic replace being removed.
#[tokio::test]
async fn overwrite_batches_join_one_truncating_load_at_flush() {
    let server = MockServer::start().await;
    mount(&server).await;
    mount_resumable(&server).await;
    // The target exists with a designed schema; the truncating load must carry
    // it, or BigQuery replaces it with the Parquet file's own (SQL-35).
    Mock::given(method("GET"))
        .and(path(format!(
            "/projects/{PROJECT_ID}/datasets/{DATASET_ID}/tables/{TABLE_ID}"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tableReference": {
                "projectId": PROJECT_ID, "datasetId": DATASET_ID, "tableId": TABLE_ID
            },
            "schema": { "fields": [
                { "name": "id", "type": "STRING", "mode": "REQUIRED", "description": "the key" }
            ] }
        })))
        .mount(&server)
        .await;
    let mut cfg = config(&server);
    cfg.write.write_mode = faucet_core::WriteMode::Overwrite;
    let sink = build_sink(&server, cfg).await;

    assert_eq!(
        sink.write_batch_columnar(&batch(&["1"])).await.expect("b1"),
        1
    );
    assert_eq!(
        sink.write_batch_columnar(&batch(&["2"])).await.expect("b2"),
        1
    );
    assert_eq!(
        sink.write_batch_columnar(&batch(&["3"])).await.expect("b3"),
        1
    );
    sink.flush().await.expect("flush");

    let ups = uploads(&server).await;
    assert!(
        ups.iter().all(|(kind, _)| kind == "resumable"),
        "no discrete load commits mid-run: {ups:?}"
    );
    let opening = &ups[0].1;
    assert!(opening.contains("WRITE_TRUNCATE"), "{opening}");
    assert!(
        !ups.iter().any(|(_, body)| body.contains("WRITE_APPEND")),
        "one truncating session: {ups:?}"
    );
}

/// Append runs load the first columnar batch as Parquet; later batches go
/// through the row path's single append load (SQL-36).
#[tokio::test]
async fn append_loads_parquet_once_per_run() {
    let server = MockServer::start().await;
    mount(&server).await;
    mount_resumable(&server).await;
    let sink = build_sink(&server, config(&server)).await;

    sink.write_batch_columnar(&batch(&["1"])).await.expect("b1");
    sink.write_batch_columnar(&batch(&["2", "3"]))
        .await
        .expect("b2");
    sink.flush().await.expect("flush");

    let kinds: Vec<String> = uploads(&server).await.into_iter().map(|u| u.0).collect();
    assert_eq!(kinds, vec!["multipart", "resumable"]);
}

/// A non-2xx upload must surface as an error, not a silent success — the load
/// never happened.
#[tokio::test]
async fn a_failed_upload_is_an_error() {
    let server = MockServer::start().await;
    // Token endpoint only; the upload endpoint returns 500.
    Mock::given(method("POST"))
        .and(path(AUTH_TOKEN_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(FakeToken {
            access_token: "fake-token",
            token_type: "bearer",
            expires_in: 9_999_999,
        }))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(format!(
            "/upload/bigquery/v2/projects/{PROJECT_ID}/jobs"
        )))
        .and(query_param("uploadType", "multipart"))
        .respond_with(ResponseTemplate::new(500).set_body_string("quota exceeded"))
        .mount(&server)
        .await;
    let sink = build_sink(&server, config(&server)).await;
    let err = sink
        .write_batch_columnar(&batch(&["1"]))
        .await
        .expect_err("a 500 upload must error");
    assert!(err.to_string().contains("500"), "{err}");
    assert!(err.to_string().contains("quota exceeded"), "{err}");
}

/// A 200 upload whose body carries no `jobReference.jobId` must error — there
/// is no job to poll, so trusting it would report a load that never ran.
#[tokio::test]
async fn a_200_upload_without_a_job_id_is_an_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(AUTH_TOKEN_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(FakeToken {
            access_token: "fake-token",
            token_type: "bearer",
            expires_in: 9_999_999,
        }))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(format!(
            "/upload/bigquery/v2/projects/{PROJECT_ID}/jobs"
        )))
        .and(query_param("uploadType", "multipart"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "kind": "bigquery#job" })))
        .mount(&server)
        .await;
    let sink = build_sink(&server, config(&server)).await;
    let err = sink
        .write_batch_columnar(&batch(&["1"]))
        .await
        .expect_err("a job with no jobId must error");
    assert!(err.to_string().contains("jobId"), "{err}");
}

/// The original seven-argument `write_columnar_media` stays callable and loads
/// without an explicit schema.
#[tokio::test]
async fn write_columnar_media_without_a_schema_still_loads() {
    let server = MockServer::start().await;
    mount(&server).await;
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
    let cfg = config(&server);
    let n = faucet_sink_bigquery::load::write_columnar_media(
        &client,
        &cfg,
        &server.uri(),
        "fake-token",
        TABLE_ID,
        "WRITE_APPEND",
        &batch(&["1"]),
    )
    .await
    .expect("load");
    assert_eq!(n, 1);
    let ups = uploads(&server).await;
    assert_eq!(ups.len(), 1);
    assert!(!ups[0].1.contains("\"schema\""), "{}", ups[0].1);
}
