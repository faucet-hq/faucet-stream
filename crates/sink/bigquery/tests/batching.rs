//! Integration tests for [`BigQuerySink`]'s `batch_size` chunking.
//!
//! The tests stand up a wiremock server in front of both the OAuth token
//! endpoint and the BigQuery `tabledata.insertAll` endpoint, then drive the
//! sink with a known record count and assert on the number of `insertAll`
//! HTTP calls observed.

use faucet_core::Sink;
use faucet_sink_bigquery::{BigQueryCredentials, BigQuerySink, BigQuerySinkConfig};
use gcp_bigquery_client::client_builder::ClientBuilder;
use serde::Serialize;
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const PROJECT_ID: &str = "test-project";
const DATASET_ID: &str = "test_dataset";
const TABLE_ID: &str = "test_table";
const AUTH_TOKEN_PATH: &str = "/:o/oauth2/token";
const AUTH_SCOPE_BASE: &str = "/auth/bigquery";

#[derive(Serialize)]
struct FakeToken {
    access_token: &'static str,
    token_type: &'static str,
    expires_in: u32,
}

fn fake_token() -> FakeToken {
    FakeToken {
        access_token: "fake-token",
        token_type: "bearer",
        expires_in: 9_999_999,
    }
}

/// A throwaway service account JSON suitable only for offline auth setup —
/// `yup_oauth2` needs a valid-looking private key to assemble the JWT it
/// posts to the (mocked) token endpoint.
fn dummy_service_account_json(oauth_server: &str) -> serde_json::Value {
    let token_uri = format!("{oauth_server}{AUTH_TOKEN_PATH}");
    json!({
        "type": "service_account",
        "project_id": "dummy",
        "private_key_id": "dummy",
        // Throwaway 2048-bit RSA key — never used to sign anything we care
        // about; the token endpoint we POST to is the mock above.
        "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDNk6cKkWP/4NMu\nWb3s24YHfM639IXzPtTev06PUVVQnyHmT1bZgQ/XB6BvIRaReqAqnQd61PAGtX3e\n8XocTw+u/ZfiPJOf+jrXMkRBpiBh9mbyEIqBy8BC20OmsUc+O/YYh/qRccvRfPI7\n3XMabQ8eFWhI6z/t35oRpvEVFJnSIgyV4JR/L/cjtoKnxaFwjBzEnxPiwtdy4olU\nKO/1maklXexvlO7onC7CNmPAjuEZKzdMLzFszikCDnoKJC8k6+2GZh0/JDMAcAF4\nwxlKNQ89MpHVRXZ566uKZg0MqZqkq5RXPn6u7yvNHwZ0oahHT+8ixPPrAEjuPEKM\nUPzVRz71AgMBAAECggEAfdbVWLW5Befkvam3hea2+5xdmeN3n3elrJhkiXxbAhf3\nE1kbq9bCEHmdrokNnI34vz0SWBFCwIiWfUNJ4UxQKGkZcSZto270V8hwWdNMXUsM\npz6S2nMTxJkdp0s7dhAUS93o9uE2x4x5Z0XecJ2ztFGcXY6Lupu2XvnW93V9109h\nkY3uICLdbovJq7wS/fO/AL97QStfEVRWW2agIXGvoQG5jOwfPh86GZZRYP9b8VNw\ntkAUJe4qpzNbWs9AItXOzL+50/wsFkD/iWMGWFuU8DY5ZwsL434N+uzFlaD13wtZ\n63D+tNAxCSRBfZGQbd7WxJVFfZe/2vgjykKWsdyNAQKBgQDnEBgSI836HGSRk0Ub\nDwiEtdfh2TosV+z6xtyU7j/NwjugTOJEGj1VO/TMlZCEfpkYPLZt3ek2LdNL66n8\nDyxwzTT5Q3D/D0n5yE3mmxy13Qyya6qBYvqqyeWNwyotGM7hNNOix1v9lEMtH5Rd\nUT0gkThvJhtrV663bcAWCALmtQKBgQDjw2rYlMUp2TUIa2/E7904WOnSEG85d+nc\norhzthX8EWmPgw1Bbfo6NzH4HhebTw03j3NjZdW2a8TG/uEmZFWhK4eDvkx+rxAa\n6EwamS6cmQ4+vdep2Ac4QCSaTZj02YjHb06Be3gptvpFaFrotH2jnpXxggdiv8ul\n6x+ooCffQQKBgQCR3ykzGoOI6K/c75prELyR+7MEk/0TzZaAY1cSdq61GXBHLQKT\nd/VMgAN1vN51pu7DzGBnT/dRCvEgNvEjffjSZdqRmrAVdfN/y6LSeQ5RCfJgGXSV\nJoWVmMxhCNrxiX3h01Xgp/c9SYJ3VD54AzeR/dwg32/j/oEAsDraLciXGQKBgQDF\nMNc8k/DvfmJv27R06Ma6liA6AoiJVMxgfXD8nVUDW3/tBCVh1HmkFU1p54PArvxe\nchAQqoYQ3dUMBHeh6ZRJaYp2ATfxJlfnM99P1/eHFOxEXdBt996oUMBf53bZ5cyJ\n/lAVwnQSiZy8otCyUDHGivJ+mXkTgcIq8BoEwERFAQKBgQDmImBaFqoMSVihqHIf\nDa4WZqwM7ODqOx0JnBKrKO8UOc51J5e1vpwP/qRpNhUipoILvIWJzu4efZY7GN5C\nImF9sN3PP6Sy044fkVPyw4SYEisxbvp9tfw8Xmpj/pbmugkB2ut6lz5frmEBoJSN\n3osZlZTgx+pM3sO6ITV6U4ID2Q==\n-----END PRIVATE KEY-----\n",
        "client_email": "dummy@developer.gserviceaccount.com",
        "client_id": "dummy",
        "auth_uri": format!("{oauth_server}/o/oauth2/auth"),
        "token_uri": token_uri,
        "auth_provider_x509_cert_url": format!("{oauth_server}/oauth2/v1/certs"),
        "client_x509_cert_url": format!("{oauth_server}/robot/v1/metadata/x509/dummy"),
    })
}

/// Mount a successful token endpoint and a single `insertAll` endpoint that
/// always returns `{}` (no per-row errors).
async fn mount_happy_path(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path(AUTH_TOKEN_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(fake_token()))
        .mount(server)
        .await;

    // The table exists: the append path probes `tables.get` before writing
    // (create_table defaults on), so stub a 200 schema response.
    Mock::given(method("GET"))
        .and(path(format!(
            "/projects/{PROJECT_ID}/datasets/{DATASET_ID}/tables/{TABLE_ID}"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tableReference": {"projectId": PROJECT_ID, "datasetId": DATASET_ID, "tableId": TABLE_ID},
            "schema": {"fields": [
                {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "name", "type": "STRING", "mode": "NULLABLE"}
            ]}
        })))
        .mount(server)
        .await;

    let insert_path =
        format!("/projects/{PROJECT_ID}/datasets/{DATASET_ID}/tables/{TABLE_ID}/insertAll");
    Mock::given(method("POST"))
        .and(path(insert_path))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(server)
        .await;
}

/// Stand up a sink wired to the given mock server. Returns the sink plus the
/// tempfile holding the dummy service account JSON (must outlive the sink so
/// the path stays valid).
async fn build_sink(
    server: &MockServer,
    batch_size: usize,
) -> (BigQuerySink, tempfile::NamedTempFile) {
    let sa_json = dummy_service_account_json(&server.uri());
    let sa_file = tempfile::NamedTempFile::new().expect("create sa tempfile");
    std::fs::write(
        sa_file.path(),
        serde_json::to_string_pretty(&sa_json).unwrap(),
    )
    .expect("write sa tempfile");

    let client = ClientBuilder::new()
        // Point both the auth scope (used as the OAuth scope URL) and the
        // BigQuery v2 base URL at our mock server.
        .with_auth_base_url(format!("{}{AUTH_SCOPE_BASE}", server.uri()))
        .with_v2_base_url(server.uri())
        .build_from_service_account_key_file(sa_file.path().to_str().unwrap())
        .await
        .expect("build bigquery client against mock");

    let config = BigQuerySinkConfig::new(
        PROJECT_ID,
        DATASET_ID,
        TABLE_ID,
        BigQueryCredentials::ApplicationDefault, // unused: from_parts bypasses auth
    )
    .with_batch_size(batch_size)
    // These tests are about the **per-page `jobs.query` path's** chunking, so
    // they pin it explicitly: since #612 the bulk load path is the default, and
    // it feeds a whole page into one resumable session (chunked by bytes
    // internally) rather than issuing one insert per `batch_size` slice.
    .with_media_load(false);

    (BigQuerySink::from_parts(config, client), sa_file)
}

fn make_records(n: usize) -> Vec<serde_json::Value> {
    (0..n)
        .map(|i| json!({"id": i, "name": format!("row-{i}")}))
        .collect()
}

fn count_insert_calls(received: &[wiremock::Request]) -> usize {
    let suffix =
        format!("/projects/{PROJECT_ID}/datasets/{DATASET_ID}/tables/{TABLE_ID}/insertAll");
    received
        .iter()
        .filter(|req| req.url.path().ends_with(&suffix))
        .count()
}

#[tokio::test]
async fn write_batch_chunks_at_configured_batch_size() {
    let server = MockServer::start().await;
    mount_happy_path(&server).await;
    let (sink, _sa_file) = build_sink(&server, 500).await;

    let records = make_records(1_500);
    let written = sink.write_batch(&records).await.expect("write succeeds");
    assert_eq!(written, 1_500);

    // 1500 records split into 500-row chunks → 3 insertAll calls.
    let calls = count_insert_calls(&server.received_requests().await.unwrap());
    assert_eq!(
        calls, 3,
        "expected 3 insertAll calls for 1500 records with batch_size=500"
    );
}

#[tokio::test]
async fn write_batch_emits_single_call_when_under_batch_size() {
    let server = MockServer::start().await;
    mount_happy_path(&server).await;
    let (sink, _sa_file) = build_sink(&server, 500).await;

    let records = make_records(100);
    let written = sink.write_batch(&records).await.expect("write succeeds");
    assert_eq!(written, 100);

    let calls = count_insert_calls(&server.received_requests().await.unwrap());
    assert_eq!(
        calls, 1,
        "expected 1 insertAll call when records.len() < batch_size"
    );
}

#[tokio::test]
async fn write_batch_partial_final_chunk() {
    let server = MockServer::start().await;
    mount_happy_path(&server).await;
    let (sink, _sa_file) = build_sink(&server, 500).await;

    let records = make_records(1_200);
    let written = sink.write_batch(&records).await.expect("write succeeds");
    assert_eq!(written, 1_200);

    // 1200 → 500 + 500 + 200 → 3 calls.
    let calls = count_insert_calls(&server.received_requests().await.unwrap());
    assert_eq!(
        calls, 3,
        "expected 3 insertAll calls for 1200 records (last chunk = 200)"
    );
}

#[tokio::test]
async fn batch_size_zero_sends_single_request() {
    let server = MockServer::start().await;
    mount_happy_path(&server).await;
    let (sink, _sa_file) = build_sink(&server, 0).await;

    // Far above the default batch size — the sentinel should still collapse
    // the whole slice into one insertAll call.
    let records = make_records(2_500);
    let written = sink.write_batch(&records).await.expect("write succeeds");
    assert_eq!(written, 2_500);

    let calls = count_insert_calls(&server.received_requests().await.unwrap());
    assert_eq!(
        calls, 1,
        "batch_size=0 sentinel must forward the entire slice in one call"
    );
}

#[tokio::test]
async fn write_batch_empty_input_makes_no_http_call() {
    let server = MockServer::start().await;
    mount_happy_path(&server).await;
    let (sink, _sa_file) = build_sink(&server, 500).await;

    let written = sink.write_batch(&[]).await.expect("write succeeds");
    assert_eq!(written, 0);

    let calls = count_insert_calls(&server.received_requests().await.unwrap());
    assert_eq!(calls, 0, "empty batch must not hit the wire");
}
