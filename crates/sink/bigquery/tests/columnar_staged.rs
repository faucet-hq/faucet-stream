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
        "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDNk6cKkWP/4NMu\nWb3s24YHfM639IXzPtTev06PUVVQnyHmT1bZgQ/XB6BvIRaReqAqnQd61PAGtX3e\n8XocTw+u/ZfiPJOf+jrXMkRBpiBh9mbyEIqBy8BC20OmsUc+O/YYh/qRccvRfPI7\n3XMabQ8eFWhI6z/t35oRpvEVFJnSIgyV4JR/L/cjtoKnxaFwjBzEnxPiwtdy4olU\nKO/1maklXexvlO7onC7CNmPAjuEZKzdMLzFszikCDnoKJC8k6+2GZh0/JDMAcAF4\nwxlKNQ89MpHVRXZ566uKZg0MqZqkq5RXPn6u7yvNHwZ0oahHT+8ixPPrAEjuPEKM\nUPzVRz71AgMBAAECggEAfdbVWLW5Befkvam3hea2+5xdmeN3n3elrJhkiXxbAhf3\nE1kbq9bCEHmdrokNnI34vz0SWBFCwIiWfUNJ4UxQKGkZcSZto270V8hwWdNMXUsM\npz6S2nMTxJkdp0s7dhAUS93o9uE2x4x5Z0XecJ2ztFGcXY6Lupu2XvnW93V9109h\nkY3uICLdbovJq7wS/fO/AL97QStfEVRWW2agIXGvoQG5jOwfPh86GZZRYP9b8VNw\ntkAUJe4qpzNbWs9AItXOzL+50/wsFkD/iWMGWFuU8DY5ZwsL434N+uzFlaD13wtZ\n63D+tNAxCSRBfZGQbd7WxJVFfZe/2vgjykKWsdyNAQKBgQDnEBgSI836HGSRk0Ub\nDwiEtdfh2TosV+z6xtyU7j/NwjugTOJEGj1VO/TMlZCEfpkYPLZt3ek2LdNL66n8\nDyxwzTT5Q3D/D0n5yE3mmxy13Qyya6qBYvqqyeWNwyotGM7hNNOix1v9lEMtH5Rd\nUT0gkThvJhtrV663bcAWCALmtQKBgQDjw2rYlMUp2TUIa2/E7904WOnSEG85d+nc\norhzthX8EWmPgw1Bbfo6NzH4HhebTw03j3NjZdW2a8TG/uEmZFWhK4eDvkx+rxAa\n6EwamS6cmQ4+vdep2Ac4QCSaTZj02YjHb06Be3gptvpFaFrotH2jnpXxggdiv8ul\n6x+ooCffQQKBgQCR3ykzGoOI6K/c75prELyR+7MEk/0TzZaAY1cSdq61GXBHLQKT\nd/VMgAN1vN51pu7DzGBnT/dRCvEgNvEjffjSZdqRmrAVdfN/y6LSeQ5RCfJgGXSV\nJoWVmMxhCNrxiX3h01Xgp/c9SYJ3VD54AzeR/dwg32/j/oEAsDraLciXGQKBgQDF\nMNc8k/DvfmJv27R06Ma6liA6AoiJVMxgfXD8nVUDW3/tBCVh1HmkFU1p54PArvxe\nchAQqoYQ3dUMBHeh6ZRJaYp2ATfxJlfnM99P1/eHFOxEXdBt996oUMBf53bZ5cyJ\n/lAVwnQSiZy8otCyUDHGivJ+mXkTgcIq8BoEwERFAQKBgQDmImBaFqoMSVihqHIf\nDa4WZqwM7ODqOx0JnBKrKO8UOc51J5e1vpwP/qRpNhUipoILvIWJzu4efZY7GN5C\nImF9sN3PP6Sy044fkVPyw4SYEisxbvp9tfw8Xmpj/pbmugkB2ut6lz5frmEBoJSN\n3osZlZTgx+pM3sO6ITV6U4ID2Q==\n-----END PRIVATE KEY-----\n",
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
