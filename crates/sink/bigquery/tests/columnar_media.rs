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
        "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDNk6cKkWP/4NMu\nWb3s24YHfM639IXzPtTev06PUVVQnyHmT1bZgQ/XB6BvIRaReqAqnQd61PAGtX3e\n8XocTw+u/ZfiPJOf+jrXMkRBpiBh9mbyEIqBy8BC20OmsUc+O/YYh/qRccvRfPI7\n3XMabQ8eFWhI6z/t35oRpvEVFJnSIgyV4JR/L/cjtoKnxaFwjBzEnxPiwtdy4olU\nKO/1maklXexvlO7onC7CNmPAjuEZKzdMLzFszikCDnoKJC8k6+2GZh0/JDMAcAF4\nwxlKNQ89MpHVRXZ566uKZg0MqZqkq5RXPn6u7yvNHwZ0oahHT+8ixPPrAEjuPEKM\nUPzVRz71AgMBAAECggEAfdbVWLW5Befkvam3hea2+5xdmeN3n3elrJhkiXxbAhf3\nE1kbq9bCEHmdrokNnI34vz0SWBFCwIiWfUNJ4UxQKGkZcSZto270V8hwWdNMXUsM\npz6S2nMTxJkdp0s7dhAUS93o9uE2x4x5Z0XecJ2ztFGcXY6Lupu2XvnW93V9109h\nkY3uICLdbovJq7wS/fO/AL97QStfEVRWW2agIXGvoQG5jOwfPh86GZZRYP9b8VNw\ntkAUJe4qpzNbWs9AItXOzL+50/wsFkD/iWMGWFuU8DY5ZwsL434N+uzFlaD13wtZ\n63D+tNAxCSRBfZGQbd7WxJVFfZe/2vgjykKWsdyNAQKBgQDnEBgSI836HGSRk0Ub\nDwiEtdfh2TosV+z6xtyU7j/NwjugTOJEGj1VO/TMlZCEfpkYPLZt3ek2LdNL66n8\nDyxwzTT5Q3D/D0n5yE3mmxy13Qyya6qBYvqqyeWNwyotGM7hNNOix1v9lEMtH5Rd\nUT0gkThvJhtrV663bcAWCALmtQKBgQDjw2rYlMUp2TUIa2/E7904WOnSEG85d+nc\norhzthX8EWmPgw1Bbfo6NzH4HhebTw03j3NjZdW2a8TG/uEmZFWhK4eDvkx+rxAa\n6EwamS6cmQ4+vdep2Ac4QCSaTZj02YjHb06Be3gptvpFaFrotH2jnpXxggdiv8ul\n6x+ooCffQQKBgQCR3ykzGoOI6K/c75prELyR+7MEk/0TzZaAY1cSdq61GXBHLQKT\nd/VMgAN1vN51pu7DzGBnT/dRCvEgNvEjffjSZdqRmrAVdfN/y6LSeQ5RCfJgGXSV\nJoWVmMxhCNrxiX3h01Xgp/c9SYJ3VD54AzeR/dwg32/j/oEAsDraLciXGQKBgQDF\nMNc8k/DvfmJv27R06Ma6liA6AoiJVMxgfXD8nVUDW3/tBCVh1HmkFU1p54PArvxe\nchAQqoYQ3dUMBHeh6ZRJaYp2ATfxJlfnM99P1/eHFOxEXdBt996oUMBf53bZ5cyJ\n/lAVwnQSiZy8otCyUDHGivJ+mXkTgcIq8BoEwERFAQKBgQDmImBaFqoMSVihqHIf\nDa4WZqwM7ODqOx0JnBKrKO8UOc51J5e1vpwP/qRpNhUipoILvIWJzu4efZY7GN5C\nImF9sN3PP6Sy044fkVPyw4SYEisxbvp9tfw8Xmpj/pbmugkB2ut6lz5frmEBoJSN\n3osZlZTgx+pM3sO6ITV6U4ID2Q==\n-----END PRIVATE KEY-----\n",
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
#[tokio::test]
async fn overwrite_truncates_once_then_appends() {
    let server = MockServer::start().await;
    mount(&server).await;
    let mut cfg = config(&server);
    cfg.write.write_mode = faucet_core::WriteMode::Overwrite;
    let sink = build_sink(&server, cfg).await;

    sink.write_batch_columnar(&batch(&["1"])).await.expect("b1");
    sink.write_batch_columnar(&batch(&["2"])).await.expect("b2");
    sink.write_batch_columnar(&batch(&["3"])).await.expect("b3");

    let dispositions: Vec<String> = upload_bodies(&server)
        .await
        .iter()
        .map(|b| {
            let t = String::from_utf8_lossy(b);
            if t.contains("WRITE_TRUNCATE") {
                "TRUNCATE".to_string()
            } else {
                "APPEND".to_string()
            }
        })
        .collect();
    assert_eq!(
        dispositions,
        vec!["TRUNCATE", "APPEND", "APPEND"],
        "only the first batch of an overwrite run may truncate"
    );
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
