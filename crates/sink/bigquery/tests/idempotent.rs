//! Integration tests for the BigQuery exactly-once write path (#215): the
//! `write_batch_idempotent` / `last_committed_token` hooks driving `jobs.query`
//! and verifying success via `get_job`.

use faucet_core::Sink;
use faucet_sink_bigquery::BigQuerySink;
use faucet_sink_bigquery::{BigQueryCredentials, BigQuerySinkConfig};
use gcp_bigquery_client::client_builder::ClientBuilder;
use serde::Serialize;
use serde_json::json;
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const PROJECT_ID: &str = "p";
const DATASET_ID: &str = "d";
const TABLE_ID: &str = "t";
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

fn dummy_service_account_json(oauth_server: &str) -> serde_json::Value {
    let token_uri = format!("{oauth_server}{AUTH_TOKEN_PATH}");
    json!({
        "type": "service_account",
        "project_id": "dummy",
        "private_key_id": "dummy",
        "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDNk6cKkWP/4NMu\nWb3s24YHfM639IXzPtTev06PUVVQnyHmT1bZgQ/XB6BvIRaReqAqnQd61PAGtX3e\n8XocTw+u/ZfiPJOf+jrXMkRBpiBh9mbyEIqBy8BC20OmsUc+O/YYh/qRccvRfPI7\n3XMabQ8eFWhI6z/t35oRpvEVFJnSIgyV4JR/L/cjtoKnxaFwjBzEnxPiwtdy4olU\nKO/1maklXexvlO7onC7CNmPAjuEZKzdMLzFszikCDnoKJC8k6+2GZh0/JDMAcAF4\nwxlKNQ89MpHVRXZ566uKZg0MqZqkq5RXPn6u7yvNHwZ0oahHT+8ixPPrAEjuPEKM\nUPzVRz71AgMBAAECggEAfdbVWLW5Befkvam3hea2+5xdmeN3n3elrJhkiXxbAhf3\nE1kbq9bCEHmdrokNnI34vz0SWBFCwIiWfUNJ4UxQKGkZcSZto270V8hwWdNMXUsM\npz6S2nMTxJkdp0s7dhAUS93o9uE2x4x5Z0XecJ2ztFGcXY6Lupu2XvnW93V9109h\nkY3uICLdbovJq7wS/fO/AL97QStfEVRWW2agIXGvoQG5jOwfPh86GZZRYP9b8VNw\ntkAUJe4qpzNbWs9AItXOzL+50/wsFkD/iWMGWFuU8DY5ZwsL434N+uzFlaD13wtZ\n63D+tNAxCSRBfZGQbd7WxJVFfZe/2vgjykKWsdyNAQKBgQDnEBgSI836HGSRk0Ub\nDwiEtdfh2TosV+z6xtyU7j/NwjugTOJEGj1VO/TMlZCEfpkYPLZt3ek2LdNL66n8\nDyxwzTT5Q3D/D0n5yE3mmxy13Qyya6qBYvqqyeWNwyotGM7hNNOix1v9lEMtH5Rd\nUT0gkThvJhtrV663bcAWCALmtQKBgQDjw2rYlMUp2TUIa2/E7904WOnSEG85d+nc\norhzthX8EWmPgw1Bbfo6NzH4HhebTw03j3NjZdW2a8TG/uEmZFWhK4eDvkx+rxAa\n6EwamS6cmQ4+vdep2Ac4QCSaTZj02YjHb06Be3gptvpFaFrotH2jnpXxggdiv8ul\n6x+ooCffQQKBgQCR3ykzGoOI6K/c75prELyR+7MEk/0TzZaAY1cSdq61GXBHLQKT\nd/VMgAN1vN51pu7DzGBnT/dRCvEgNvEjffjSZdqRmrAVdfN/y6LSeQ5RCfJgGXSV\nJoWVmMxhCNrxiX3h01Xgp/c9SYJ3VD54AzeR/dwg32/j/oEAsDraLciXGQKBgQDF\nMNc8k/DvfmJv27R06Ma6liA6AoiJVMxgfXD8nVUDW3/tBCVh1HmkFU1p54PArvxe\nchAQqoYQ3dUMBHeh6ZRJaYp2ATfxJlfnM99P1/eHFOxEXdBt996oUMBf53bZ5cyJ\n/lAVwnQSiZy8otCyUDHGivJ+mXkTgcIq8BoEwERFAQKBgQDmImBaFqoMSVihqHIf\nDa4WZqwM7ODqOx0JnBKrKO8UOc51J5e1vpwP/qRpNhUipoILvIWJzu4efZY7GN5C\nImF9sN3PP6Sy044fkVPyw4SYEisxbvp9tfw8Xmpj/pbmugkB2ut6lz5frmEBoJSN\n3osZlZTgx+pM3sO6ITV6U4ID2Q==\n-----END PRIVATE KEY-----\n",
        "client_email": "dummy@developer.gserviceaccount.com",
        "client_id": "dummy",
        "auth_uri": format!("{oauth_server}/o/oauth2/auth"),
        "token_uri": token_uri,
        "auth_provider_x509_cert_url": format!("{oauth_server}/oauth2/v1/certs"),
        "client_x509_cert_url": format!("{oauth_server}/robot/v1/metadata/x509/dummy"),
    })
}

async fn mount_token_endpoint(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path(AUTH_TOKEN_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(fake_token()))
        .mount(server)
        .await;
}

async fn build_sink(server: &MockServer) -> (BigQuerySink, tempfile::NamedTempFile) {
    let sa_json = dummy_service_account_json(&server.uri());
    let sa_file = tempfile::NamedTempFile::new().expect("create sa tempfile");
    std::fs::write(
        sa_file.path(),
        serde_json::to_string_pretty(&sa_json).unwrap(),
    )
    .expect("write sa tempfile");
    let client = ClientBuilder::new()
        .with_auth_base_url(format!("{}{AUTH_SCOPE_BASE}", server.uri()))
        .with_v2_base_url(server.uri())
        .build_from_service_account_key_file(sa_file.path().to_str().unwrap())
        .await
        .expect("build bigquery client against mock");
    let config = BigQuerySinkConfig::new(
        PROJECT_ID,
        DATASET_ID,
        TABLE_ID,
        BigQueryCredentials::ApplicationDefault,
    );
    (BigQuerySink::from_parts(config, client), sa_file)
}

fn queries_path() -> String {
    format!("/projects/{PROJECT_ID}/queries")
}
fn tables_get_path() -> String {
    format!("/projects/{PROJECT_ID}/datasets/{DATASET_ID}/tables/{TABLE_ID}")
}

/// A completed `jobs.query` response with no rows, carrying the given jobId.
fn done_query(job_id: &str) -> serde_json::Value {
    json!({
        "kind": "bigquery#queryResponse",
        "jobComplete": true,
        "jobReference": {"projectId": PROJECT_ID, "jobId": job_id}
    })
}

/// Mount the target table's schema (id INTEGER REQUIRED, name STRING NULLABLE).
async fn mount_table_schema(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path(tables_get_path()))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tableReference": {"projectId": PROJECT_ID, "datasetId": DATASET_ID, "tableId": TABLE_ID},
            "schema": {"fields": [
                {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
                {"name": "name", "type": "STRING", "mode": "NULLABLE"}
            ]}
        })))
        .mount(server)
        .await;
}

/// Mount a `get_job` that reports DONE with no error.
async fn mount_job_done(server: &MockServer, job_id: &str) {
    Mock::given(method("GET"))
        .and(path(format!("/projects/{PROJECT_ID}/jobs/{job_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobReference": {"projectId": PROJECT_ID, "jobId": job_id},
            "status": {"state": "DONE"},
            "statistics": {"query": {"totalBytesBilled": "10485760"}}
        })))
        .mount(server)
        .await;
}

/// Mount a `get_job` that reports DONE with a terminal errorResult.
async fn mount_job_failed(server: &MockServer, job_id: &str, message: &str) {
    Mock::given(method("GET"))
        .and(path(format!("/projects/{PROJECT_ID}/jobs/{job_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobReference": {"projectId": PROJECT_ID, "jobId": job_id},
            "status": {"state": "DONE", "errorResult": {"reason": "invalidQuery", "message": message}}
        })))
        .mount(server)
        .await;
}

/// Capture all POST bodies sent to the queries endpoint as parsed JSON.
async fn captured_query_bodies(server: &MockServer) -> Vec<serde_json::Value> {
    server
        .received_requests()
        .await
        .expect("recording enabled")
        .into_iter()
        .filter(|r| r.url.path().ends_with("/queries"))
        .map(|r| serde_json::from_slice(&r.body).expect("query body is JSON"))
        .collect()
}

#[tokio::test]
async fn sink_advertises_idempotent_support() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    let (sink, _sa) = build_sink(&server).await;
    assert!(sink.supports_idempotent_writes());
}

#[tokio::test]
async fn write_batch_idempotent_posts_transaction_with_params() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    // CREATE and the transaction both resolve to job-x; one get_job(DONE) covers both.
    Mock::given(method("POST"))
        .and(path(queries_path()))
        .respond_with(ResponseTemplate::new(200).set_body_json(done_query("job-x")))
        .mount(&server)
        .await;
    mount_job_done(&server, "job-x").await;

    let (sink, _sa) = build_sink(&server).await;
    let meter = std::sync::Arc::new(faucet_core::UsageMeter::new());
    sink.set_roundtrip_recorder(std::sync::Arc::new(
        faucet_core::observability::RoundtripRecorder::new(
            faucet_core::observability::RoundtripSide::Sink,
            "p",
            "r",
            "bigquery",
        )
        .with_meter(meter.clone()),
    ));
    let records = vec![json!({"id": 1, "name": "a"}), json!({"id": 2, "name": "b"})];
    let written = sink
        .write_batch_idempotent(&records, "pipe::row1", "00000000000000000003")
        .await
        .expect("idempotent write");
    assert_eq!(written, 2);
    let usage = meter.snapshot();
    assert!(usage.sink_roundtrips.get("job").is_some_and(|n| *n >= 1));
    assert!(
        usage
            .signals
            .iter()
            .any(|s| s.kind == "bytes_billed" && s.quantity == 10_485_760.0),
        "{:?}",
        usage.signals
    );

    let bodies = captured_query_bodies(&server).await;
    let tx = bodies
        .iter()
        .find(|b| {
            b["query"]
                .as_str()
                .unwrap_or("")
                .contains("BEGIN TRANSACTION")
        })
        .expect("a transaction query was sent");
    let q = tx["query"].as_str().unwrap();
    assert!(q.contains("INSERT INTO `p.d.t` (`id`, `name`)"), "got: {q}");
    assert!(
        q.contains("FROM UNNEST(JSON_QUERY_ARRAY(@payload)) AS r"),
        "got: {q}"
    );
    assert!(q.contains("MERGE `p.d._faucet_commit_token`"), "got: {q}");
    assert_eq!(tx["parameterMode"], "NAMED");
    let names: Vec<&str> = tx["queryParameters"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["name"].as_str().unwrap())
        .collect();
    assert!(
        names.contains(&"payload") && names.contains(&"scope") && names.contains(&"token"),
        "params: {names:?}"
    );
    let payload = tx["queryParameters"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["name"] == "payload")
        .unwrap()["parameterValue"]["value"]
        .as_str()
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(payload).unwrap(),
        json!(records)
    );
    assert!(tx.get("requestId").is_some(), "requestId must be set");
}

#[tokio::test]
async fn ensure_commit_table_issues_create() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    Mock::given(method("POST"))
        .and(path(queries_path()))
        .and(body_string_contains("CREATE TABLE IF NOT EXISTS"))
        .respond_with(ResponseTemplate::new(200).set_body_json(done_query("job-c")))
        .expect(1..)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(queries_path()))
        .and(body_string_contains("BEGIN TRANSACTION"))
        .respond_with(ResponseTemplate::new(200).set_body_json(done_query("job-t")))
        .mount(&server)
        .await;
    mount_job_done(&server, "job-c").await;
    mount_job_done(&server, "job-t").await;

    let (sink, _sa) = build_sink(&server).await;
    sink.write_batch_idempotent(
        &[json!({"id": 1, "name": "x"})],
        "s",
        "00000000000000000001",
    )
    .await
    .expect("write");
    // The .expect(1..) on the CREATE mock asserts it was issued.
}

#[tokio::test]
async fn last_committed_token_reads_watermark_row() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    Mock::given(method("POST"))
        .and(path(queries_path()))
        .and(body_string_contains("CREATE TABLE IF NOT EXISTS"))
        .respond_with(ResponseTemplate::new(200).set_body_json(done_query("job-c")))
        .mount(&server)
        .await;
    mount_job_done(&server, "job-c").await;
    Mock::given(method("POST"))
        .and(path(queries_path()))
        .and(body_string_contains("SELECT token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "kind": "bigquery#queryResponse",
            "jobComplete": true,
            "jobReference": {"projectId": PROJECT_ID, "jobId": "job-sel"},
            "schema": {"fields": [{"name": "token", "type": "STRING", "mode": "NULLABLE"}]},
            "rows": [{"f": [{"v": "00000000000000000009"}]}],
            "totalRows": "1"
        })))
        .mount(&server)
        .await;

    let (sink, _sa) = build_sink(&server).await;
    let got = sink
        .last_committed_token("pipe::row1")
        .await
        .expect("read token");
    assert_eq!(got.as_deref(), Some("00000000000000000009"));
}

#[tokio::test]
async fn last_committed_token_none_when_no_row() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    Mock::given(method("POST"))
        .and(path(queries_path()))
        .and(body_string_contains("CREATE TABLE IF NOT EXISTS"))
        .respond_with(ResponseTemplate::new(200).set_body_json(done_query("job-c")))
        .mount(&server)
        .await;
    mount_job_done(&server, "job-c").await;
    // `.expect(1..)`: None must come from actually running the SELECT against an
    // empty watermark, not from short-circuiting before issuing the query.
    Mock::given(method("POST"))
        .and(path(queries_path()))
        .and(body_string_contains("SELECT token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "kind": "bigquery#queryResponse",
            "jobComplete": true,
            "jobReference": {"projectId": PROJECT_ID, "jobId": "job-sel"},
            "schema": {"fields": [{"name": "token", "type": "STRING", "mode": "NULLABLE"}]},
            "totalRows": "0"
        })))
        .expect(1..)
        .mount(&server)
        .await;

    let (sink, _sa) = build_sink(&server).await;
    let got = sink
        .last_committed_token("never-seen")
        .await
        .expect("read token");
    assert_eq!(got, None);
}

#[tokio::test]
async fn write_batch_idempotent_bubbles_job_error() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    // CREATE succeeds; the transaction's job reports a terminal errorResult.
    Mock::given(method("POST"))
        .and(path(queries_path()))
        .and(body_string_contains("CREATE TABLE IF NOT EXISTS"))
        .respond_with(ResponseTemplate::new(200).set_body_json(done_query("job-c")))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(queries_path()))
        .and(body_string_contains("BEGIN TRANSACTION"))
        .respond_with(ResponseTemplate::new(200).set_body_json(done_query("job-t")))
        .mount(&server)
        .await;
    mount_job_done(&server, "job-c").await;
    mount_job_failed(&server, "job-t", "type mismatch on column id").await;

    let (sink, _sa) = build_sink(&server).await;
    match sink
        .write_batch_idempotent(
            &[json!({"id": "notint", "name": "x"})],
            "s",
            "00000000000000000001",
        )
        .await
    {
        Err(faucet_core::FaucetError::Sink(m)) => assert!(m.contains("type mismatch"), "got: {m}"),
        other => panic!("expected Sink error, got: {other:?}"),
    }
}

#[tokio::test]
async fn empty_page_still_commits_token() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    Mock::given(method("POST"))
        .and(path(queries_path()))
        .respond_with(ResponseTemplate::new(200).set_body_json(done_query("job-x")))
        .mount(&server)
        .await;
    mount_job_done(&server, "job-x").await;

    let (sink, _sa) = build_sink(&server).await;
    let written = sink
        .write_batch_idempotent(&[], "pipe::row1", "00000000000000000004")
        .await
        .expect("empty idempotent write");
    assert_eq!(written, 0);

    let bodies = captured_query_bodies(&server).await;
    let tx = bodies
        .iter()
        .find(|b| {
            b["query"]
                .as_str()
                .unwrap_or("")
                .contains("BEGIN TRANSACTION")
        })
        .unwrap();
    let payload = tx["queryParameters"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["name"] == "payload")
        .unwrap()["parameterValue"]["value"]
        .as_str()
        .unwrap();
    assert_eq!(payload, "[]");
}

#[tokio::test]
async fn write_batch_idempotent_errors_on_schemaless_table() {
    // The typed INSERT is generated from the target schema; a table with no
    // field definitions cannot be written idempotently, so the sink must fail
    // with a clear error rather than emit an empty column list.
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    // ensure_commit_table's CREATE succeeds first.
    Mock::given(method("POST"))
        .and(path(queries_path()))
        .respond_with(ResponseTemplate::new(200).set_body_json(done_query("job-x")))
        .mount(&server)
        .await;
    mount_job_done(&server, "job-x").await;
    // The target table reports an empty schema.
    Mock::given(method("GET"))
        .and(path(tables_get_path()))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tableReference": {"projectId": PROJECT_ID, "datasetId": DATASET_ID, "tableId": TABLE_ID},
            "schema": {"fields": []}
        })))
        .mount(&server)
        .await;

    let (sink, _sa) = build_sink(&server).await;
    match sink
        .write_batch_idempotent(&[json!({"id": 1})], "s", "00000000000000000001")
        .await
    {
        Err(faucet_core::FaucetError::Sink(m)) => {
            assert!(
                m.contains("schema fields"),
                "expected a schema-fields error, got: {m}"
            )
        }
        other => panic!("expected Sink error for a schemaless table, got: {other:?}"),
    }
}

#[tokio::test]
async fn last_committed_token_errors_when_select_not_complete() {
    // Fail-safe guard: a non-synchronous watermark read must NOT be read as
    // `None` (which would replay committed pages → duplicates) — it must error.
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    Mock::given(method("POST"))
        .and(path(queries_path()))
        .and(body_string_contains("CREATE TABLE IF NOT EXISTS"))
        .respond_with(ResponseTemplate::new(200).set_body_json(done_query("job-c")))
        .mount(&server)
        .await;
    mount_job_done(&server, "job-c").await;
    // The SELECT comes back still running.
    Mock::given(method("POST"))
        .and(path(queries_path()))
        .and(body_string_contains("SELECT token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "kind": "bigquery#queryResponse",
            "jobComplete": false,
            "jobReference": {"projectId": PROJECT_ID, "jobId": "job-sel"}
        })))
        .mount(&server)
        .await;

    let (sink, _sa) = build_sink(&server).await;
    match sink.last_committed_token("pipe::row1").await {
        Err(faucet_core::FaucetError::Sink(m)) => {
            assert!(m.contains("did not complete"), "got: {m}")
        }
        other => panic!("expected a fail-safe Sink error, got: {other:?}"),
    }
}

#[tokio::test]
async fn last_committed_token_errors_when_select_has_no_schema() {
    // Fail-safe guard: a completed read with no schema cannot be trusted to
    // distinguish "no token" from "row present but unreadable" — it must error
    // rather than return a wrong `None`.
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    Mock::given(method("POST"))
        .and(path(queries_path()))
        .and(body_string_contains("CREATE TABLE IF NOT EXISTS"))
        .respond_with(ResponseTemplate::new(200).set_body_json(done_query("job-c")))
        .mount(&server)
        .await;
    mount_job_done(&server, "job-c").await;
    // Completed, but the response carries no schema.
    Mock::given(method("POST"))
        .and(path(queries_path()))
        .and(body_string_contains("SELECT token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "kind": "bigquery#queryResponse",
            "jobComplete": true,
            "jobReference": {"projectId": PROJECT_ID, "jobId": "job-sel"},
            "totalRows": "0"
        })))
        .mount(&server)
        .await;

    let (sink, _sa) = build_sink(&server).await;
    match sink.last_committed_token("pipe::row1").await {
        Err(faucet_core::FaucetError::Sink(m)) => assert!(m.contains("no schema"), "got: {m}"),
        other => panic!("expected a fail-safe Sink error, got: {other:?}"),
    }
}
