//! Integration tests for the BigQuery source.
//!
//! Drives [`BigQuerySource`] against a wiremock server mocking the BigQuery
//! REST API. The same OAuth-token + service-account dance the sink tests
//! use lets us run the real `gcp_bigquery_client` client end-to-end without
//! reaching out to GCP — we just point both the auth and `v2` base URLs at
//! the local mock.

use std::collections::HashMap;

use faucet_core::Source;
use faucet_source_bigquery::{BigQueryCredentials, BigQuerySource, BigQuerySourceConfig};
use futures::StreamExt;
use gcp_bigquery_client::client_builder::ClientBuilder;
use serde::Serialize;
use serde_json::{Value, json};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const PROJECT_ID: &str = "test-project";
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

fn dummy_service_account_json(oauth_server: &str) -> Value {
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

async fn mount_token(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path(AUTH_TOKEN_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(fake_token()))
        .mount(server)
        .await;
}

async fn build_source(
    server: &MockServer,
    config: BigQuerySourceConfig,
) -> (BigQuerySource, tempfile::NamedTempFile) {
    let sa_json = dummy_service_account_json(&server.uri());
    let sa_file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(
        sa_file.path(),
        serde_json::to_string_pretty(&sa_json).unwrap(),
    )
    .unwrap();

    let client = ClientBuilder::new()
        .with_auth_base_url(format!("{}{AUTH_SCOPE_BASE}", server.uri()))
        .with_v2_base_url(server.uri())
        .build_from_service_account_key_file(sa_file.path().to_str().unwrap())
        .await
        .expect("build bigquery client against mock");

    (BigQuerySource::from_parts(config, client), sa_file)
}

fn schema_two_cols() -> Value {
    json!({
        "fields": [
            {"name": "id", "type": "INTEGER"},
            {"name": "name", "type": "STRING"}
        ]
    })
}

fn rows(start: i64, count: i64) -> Vec<Value> {
    (start..start + count)
        .map(|i| json!({"f": [{"v": i.to_string()}, {"v": format!("name-{i}")}]}))
        .collect()
}

fn default_config() -> BigQuerySourceConfig {
    BigQuerySourceConfig::new(
        PROJECT_ID,
        BigQueryCredentials::ApplicationDefault,
        "SELECT id, name FROM events",
    )
    .with_batch_size(10)
}

#[tokio::test]
async fn fetch_all_returns_rows_when_first_response_is_complete() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    Mock::given(method("POST"))
        .and(path(format!("/projects/{PROJECT_ID}/queries")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": true,
            "schema": schema_two_cols(),
            "jobReference": {"projectId": PROJECT_ID, "jobId": "job-1"},
            "totalBytesProcessed": "2048",
            "rows": rows(0, 3),
        })))
        .mount(&server)
        .await;

    let (src, _f) = build_source(&server, default_config()).await;
    let meter = std::sync::Arc::new(faucet_core::UsageMeter::new());
    src.set_roundtrip_recorder(std::sync::Arc::new(
        faucet_core::observability::RoundtripRecorder::new(
            faucet_core::observability::RoundtripSide::Source,
            "p",
            "r",
            "bigquery",
        )
        .with_meter(meter.clone()),
    ));
    let rows = src.fetch_all().await.unwrap();
    let usage = meter.snapshot();
    assert_eq!(usage.source_roundtrips.get("query"), Some(&1));
    assert_eq!(usage.signals.len(), 1);
    assert_eq!(usage.signals[0].kind, "bytes_processed");
    assert_eq!(usage.signals[0].quantity, 2048.0);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], json!({"id": 0, "name": "name-0"}));
    assert_eq!(rows[2], json!({"id": 2, "name": "name-2"}));
}

#[tokio::test]
async fn fetch_all_paginates_via_page_token() {
    let server = MockServer::start().await;
    mount_token(&server).await;

    // jobs.query: first page (rows 0..5) + pageToken.
    Mock::given(method("POST"))
        .and(path(format!("/projects/{PROJECT_ID}/queries")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": true,
            "schema": schema_two_cols(),
            "jobReference": {"projectId": PROJECT_ID, "jobId": "job-page"},
            "rows": rows(0, 5),
            "pageToken": "tok-1",
        })))
        .mount(&server)
        .await;

    // getQueryResults: second page (rows 5..10) — final page (no further token).
    Mock::given(method("GET"))
        .and(path(format!("/projects/{PROJECT_ID}/queries/job-page")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": true,
            "schema": schema_two_cols(),
            "rows": rows(5, 5),
        })))
        .mount(&server)
        .await;

    let (src, _f) = build_source(&server, default_config()).await;
    let rows = src.fetch_all().await.unwrap();
    assert_eq!(rows.len(), 10);
    assert_eq!(rows[0]["id"], 0);
    assert_eq!(rows[9]["id"], 9);
}

#[tokio::test]
async fn stream_pages_chunks_by_batch_size() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    Mock::given(method("POST"))
        .and(path(format!("/projects/{PROJECT_ID}/queries")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": true,
            "schema": schema_two_cols(),
            "jobReference": {"projectId": PROJECT_ID, "jobId": "job-stream"},
            "rows": rows(0, 13),
        })))
        .mount(&server)
        .await;

    // batch_size = 5 → 13 rows yield 3 pages of [5, 5, 3].
    let (src, _f) = build_source(&server, default_config().with_batch_size(5)).await;
    let ctx = HashMap::new();
    let mut sizes = Vec::new();
    let mut pages = src.stream_pages(&ctx, 0);
    while let Some(page) = pages.next().await {
        let page = page.unwrap();
        assert!(page.bookmark.is_none());
        sizes.push(page.records.len());
    }
    assert_eq!(sizes, vec![5, 5, 3]);
}

#[tokio::test]
async fn stream_pages_batch_size_zero_emits_one_page() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    Mock::given(method("POST"))
        .and(path(format!("/projects/{PROJECT_ID}/queries")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": true,
            "schema": schema_two_cols(),
            "jobReference": {"projectId": PROJECT_ID, "jobId": "job-flat"},
            "rows": rows(0, 7),
        })))
        .mount(&server)
        .await;

    let (src, _f) = build_source(&server, default_config().with_batch_size(0)).await;
    let ctx = HashMap::new();
    let pages: Vec<_> = src.stream_pages(&ctx, 0).collect().await;
    assert_eq!(pages.len(), 1);
    assert_eq!(pages[0].as_ref().unwrap().records.len(), 7);
}

#[tokio::test]
async fn polls_until_job_complete_when_first_response_is_incomplete() {
    let server = MockServer::start().await;
    mount_token(&server).await;

    // jobs.query returns jobComplete=false (statement still running).
    Mock::given(method("POST"))
        .and(path(format!("/projects/{PROJECT_ID}/queries")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": false,
            "jobReference": {"projectId": PROJECT_ID, "jobId": "job-poll"},
        })))
        .mount(&server)
        .await;

    // First poll: still not complete.
    Mock::given(method("GET"))
        .and(path(format!("/projects/{PROJECT_ID}/queries/job-poll")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": false,
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    // Subsequent poll: complete with rows.
    Mock::given(method("GET"))
        .and(path(format!("/projects/{PROJECT_ID}/queries/job-poll")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": true,
            "schema": schema_two_cols(),
            "rows": rows(0, 2),
        })))
        .mount(&server)
        .await;

    let (src, _f) = build_source(&server, default_config()).await;
    let rows = src.fetch_all().await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["id"], 0);
}

#[tokio::test]
async fn submits_positional_bindings_for_params_and_context() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    Mock::given(method("POST"))
        .and(path(format!("/projects/{PROJECT_ID}/queries")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": true,
            "schema": schema_two_cols(),
            "jobReference": {"projectId": PROJECT_ID, "jobId": "job-binds"},
            "rows": rows(0, 1),
        })))
        .mount(&server)
        .await;

    let cfg = default_config()
        .with_params(vec![json!("us-east")])
        .with_location("EU");
    let mut cfg = cfg;
    cfg.query = "SELECT * FROM events WHERE region = ? AND id > {parent.min_id}".into();
    let (src, _f) = build_source(&server, cfg).await;

    let mut ctx = HashMap::new();
    ctx.insert("parent.min_id".to_string(), json!(100));
    src.fetch_with_context(&ctx).await.unwrap();

    let reqs = server.received_requests().await.unwrap();
    let query_req = reqs
        .iter()
        .find(|r| r.url.path().ends_with("/queries"))
        .expect("captured jobs.query request");
    let body: Value = serde_json::from_slice(&query_req.body).unwrap();
    assert_eq!(
        body["query"],
        "SELECT * FROM events WHERE region = ? AND id > ?"
    );
    assert_eq!(body["parameterMode"], "POSITIONAL");
    let params = body["queryParameters"].as_array().unwrap();
    assert_eq!(params.len(), 2);
    assert_eq!(params[0]["parameterValue"]["value"], "us-east");
    assert_eq!(params[1]["parameterValue"]["value"], "100");
    assert_eq!(body["location"], "EU");
}

#[tokio::test]
async fn fetch_all_decodes_all_bigquery_types_to_json() {
    // Type-aware decoding end-to-end through `jobs.query`: INTEGER, FLOAT,
    // BOOL, STRING, TIMESTAMP, a nested RECORD, and a REPEATED column all
    // round-trip into the expected JSON shapes.
    let server = MockServer::start().await;
    mount_token(&server).await;

    let schema = json!({
        "fields": [
            {"name": "id", "type": "INTEGER"},
            {"name": "ratio", "type": "FLOAT"},
            {"name": "active", "type": "BOOLEAN"},
            {"name": "label", "type": "STRING"},
            {"name": "ts", "type": "TIMESTAMP"},
            {"name": "tags", "type": "STRING", "mode": "REPEATED"},
            {
                "name": "owner",
                "type": "RECORD",
                "fields": [
                    {"name": "uid", "type": "INTEGER"},
                    {"name": "email", "type": "STRING"}
                ]
            }
        ]
    });
    // A single row exercising every cell shape BigQuery returns.
    let row = json!({"f": [
        {"v": "42"},
        {"v": "2.5"},
        {"v": "true"},
        {"v": "hello"},
        {"v": "1.7e9"},
        {"v": [{"v": "a"}, {"v": "b"}]},
        {"v": {"f": [{"v": "7"}, {"v": "x@y.z"}]}}
    ]});

    Mock::given(method("POST"))
        .and(path(format!("/projects/{PROJECT_ID}/queries")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": true,
            "schema": schema,
            "jobReference": {"projectId": PROJECT_ID, "jobId": "job-types"},
            "rows": [row],
        })))
        .mount(&server)
        .await;

    let (src, _f) = build_source(&server, default_config()).await;
    let rows = src.fetch_all().await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0],
        json!({
            "id": 42,
            "ratio": 2.5,
            "active": true,
            "label": "hello",
            "ts": "1.7e9",
            "tags": ["a", "b"],
            "owner": {"uid": 7, "email": "x@y.z"}
        })
    );
}

#[tokio::test]
async fn fetch_all_paginates_across_three_pages_via_get_query_results() {
    // jobs.query → page 1 (+tok-1); getQueryResults → page 2 (+tok-2);
    // getQueryResults → page 3 (no token, final). All three pages flow
    // through the getQueryResults polling/paging loop.
    let server = MockServer::start().await;
    mount_token(&server).await;

    Mock::given(method("POST"))
        .and(path(format!("/projects/{PROJECT_ID}/queries")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": true,
            "schema": schema_two_cols(),
            "jobReference": {"projectId": PROJECT_ID, "jobId": "job-3pages"},
            "rows": rows(0, 4),
            "pageToken": "tok-1",
        })))
        .mount(&server)
        .await;

    // Second page carries a further pageToken so the loop iterates again.
    Mock::given(method("GET"))
        .and(path(format!("/projects/{PROJECT_ID}/queries/job-3pages")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": true,
            "schema": schema_two_cols(),
            "rows": rows(4, 4),
            "pageToken": "tok-2",
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    // Final page: no pageToken.
    Mock::given(method("GET"))
        .and(path(format!("/projects/{PROJECT_ID}/queries/job-3pages")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": true,
            "schema": schema_two_cols(),
            "rows": rows(8, 2),
        })))
        .mount(&server)
        .await;

    let (src, _f) = build_source(&server, default_config()).await;
    let rows = src.fetch_all().await.unwrap();
    assert_eq!(rows.len(), 10);
    assert_eq!(rows[0]["id"], 0);
    assert_eq!(rows[4]["id"], 4);
    assert_eq!(rows[9]["id"], 9);
    assert_eq!(rows[9]["name"], "name-9");
}

#[tokio::test]
async fn stream_pages_initial_response_without_rows_then_paginates() {
    // jobs.query returns a complete-but-rowless first response carrying only a
    // pageToken (exercises `rows_from_response_owned`'s None branch); the
    // actual rows arrive via getQueryResults.
    let server = MockServer::start().await;
    mount_token(&server).await;

    Mock::given(method("POST"))
        .and(path(format!("/projects/{PROJECT_ID}/queries")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": true,
            "schema": schema_two_cols(),
            "jobReference": {"projectId": PROJECT_ID, "jobId": "job-norows"},
            "pageToken": "tok-1",
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!("/projects/{PROJECT_ID}/queries/job-norows")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": true,
            "schema": schema_two_cols(),
            "rows": rows(0, 3),
        })))
        .mount(&server)
        .await;

    let (src, _f) = build_source(&server, default_config().with_batch_size(0)).await;
    let ctx = HashMap::new();
    let pages: Vec<_> = src.stream_pages(&ctx, 0).collect().await;
    assert_eq!(pages.len(), 1);
    let page = pages[0].as_ref().unwrap();
    assert_eq!(page.records.len(), 3);
    assert_eq!(page.records[0], json!({"id": 0, "name": "name-0"}));
}

#[tokio::test]
async fn fetch_all_errors_when_job_query_returns_non_2xx() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    Mock::given(method("POST"))
        .and(path(format!("/projects/{PROJECT_ID}/queries")))
        .respond_with(ResponseTemplate::new(403).set_body_json(json!({
            "error": {"code": 403, "message": "Access Denied"}
        })))
        .mount(&server)
        .await;

    let (src, _f) = build_source(&server, default_config()).await;
    match src.fetch_all().await {
        Err(faucet_core::FaucetError::Source(m)) => {
            assert!(m.contains("jobs.query failed"), "got: {m}");
        }
        other => panic!("expected Source error, got: {other:?}"),
    }
}

#[tokio::test]
async fn fetch_all_errors_when_get_query_results_returns_non_2xx() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    // First page completes with a token, forcing a getQueryResults call.
    Mock::given(method("POST"))
        .and(path(format!("/projects/{PROJECT_ID}/queries")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": true,
            "schema": schema_two_cols(),
            "jobReference": {"projectId": PROJECT_ID, "jobId": "job-getfail"},
            "rows": rows(0, 1),
            "pageToken": "tok-1",
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/projects/{PROJECT_ID}/queries/job-getfail")))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .mount(&server)
        .await;

    let (src, _f) = build_source(&server, default_config()).await;
    match src.fetch_all().await {
        Err(faucet_core::FaucetError::Source(m)) => {
            assert!(m.contains("getQueryResults failed"), "got: {m}");
        }
        other => panic!("expected Source error, got: {other:?}"),
    }
}

#[tokio::test]
async fn fetch_all_errors_when_job_reference_missing() {
    // A complete 200 response with rows but no jobReference must surface a
    // typed Source error rather than silently proceeding.
    let server = MockServer::start().await;
    mount_token(&server).await;
    Mock::given(method("POST"))
        .and(path(format!("/projects/{PROJECT_ID}/queries")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": true,
            "schema": schema_two_cols(),
            "rows": rows(0, 1),
        })))
        .mount(&server)
        .await;

    let (src, _f) = build_source(&server, default_config()).await;
    match src.fetch_all().await {
        Err(faucet_core::FaucetError::Source(m)) => {
            assert!(m.contains("missing jobReference"), "got: {m}");
        }
        other => panic!("expected Source error, got: {other:?}"),
    }
}

#[tokio::test]
async fn submits_typed_bool_and_float_param_types() {
    // `bq_param_type` infers BOOL / FLOAT64 / INT64 from the JSON value so a
    // numeric/boolean bind isn't forced to STRING. A JSON null becomes a typed
    // NULL with the value omitted.
    let server = MockServer::start().await;
    mount_token(&server).await;
    Mock::given(method("POST"))
        .and(path(format!("/projects/{PROJECT_ID}/queries")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": true,
            "schema": schema_two_cols(),
            "jobReference": {"projectId": PROJECT_ID, "jobId": "job-typed"},
            "rows": rows(0, 1),
        })))
        .mount(&server)
        .await;

    let cfg = default_config().with_params(vec![json!(true), json!(2.5), json!(7), json!(null)]);
    let (src, _f) = build_source(&server, cfg).await;
    src.fetch_all().await.unwrap();

    let reqs = server.received_requests().await.unwrap();
    let query_req = reqs
        .iter()
        .find(|r| r.url.path().ends_with("/queries"))
        .expect("captured jobs.query request");
    let body: Value = serde_json::from_slice(&query_req.body).unwrap();
    let params = body["queryParameters"].as_array().unwrap();
    assert_eq!(params.len(), 4);
    assert_eq!(params[0]["parameterType"]["type"], "BOOL");
    assert_eq!(params[0]["parameterValue"]["value"], "true");
    assert_eq!(params[1]["parameterType"]["type"], "FLOAT64");
    assert_eq!(params[1]["parameterValue"]["value"], "2.5");
    assert_eq!(params[2]["parameterType"]["type"], "INT64");
    assert_eq!(params[2]["parameterValue"]["value"], "7");
    // JSON null → typed STRING NULL: value omitted from the value object.
    assert_eq!(params[3]["parameterType"]["type"], "STRING");
    assert!(
        params[3]["parameterValue"]["value"].is_null(),
        "null bind must omit the value: {:?}",
        params[3]
    );
}

#[tokio::test]
async fn connector_name_schema_and_dataset_uri_are_exposed() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    let mut cfg = default_config();
    cfg.query = "SELECT 1".into();
    let (src, _f) = build_source(&server, cfg).await;

    assert_eq!(src.connector_name(), "bigquery");
    assert_eq!(src.dataset_uri(), "bigquery://test-project?query=SELECT 1");
    let schema = src.config_schema();
    // schema_for! emits an object schema describing the config struct.
    assert!(
        schema.get("properties").is_some() || schema.get("$ref").is_some(),
        "config_schema should be a JSON Schema object: {schema}"
    );
}

#[tokio::test]
async fn check_probe_passes_on_successful_dry_run() {
    use faucet_core::check::{CheckContext, ProbeStatus};
    let server = MockServer::start().await;
    mount_token(&server).await;
    // The doctor probe submits the query with dryRun=true to the same
    // jobs.query endpoint; a 200 → a passing probe.
    Mock::given(method("POST"))
        .and(path(format!("/projects/{PROJECT_ID}/queries")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": true,
            "jobReference": {"projectId": PROJECT_ID, "jobId": "dry"},
        })))
        .mount(&server)
        .await;

    let (src, _f) = build_source(&server, default_config()).await;
    let report = src.check(&CheckContext::default()).await.unwrap();
    assert_eq!(report.probes.len(), 1);
    assert_eq!(report.probes[0].name, "query");
    assert!(
        matches!(report.probes[0].status, ProbeStatus::Pass),
        "expected Pass, got: {:?}",
        report.probes[0].status
    );

    // And the submitted body really did carry dryRun=true.
    let reqs = server.received_requests().await.unwrap();
    let query_req = reqs
        .iter()
        .find(|r| r.url.path().ends_with("/queries"))
        .expect("captured dry-run request");
    let body: Value = serde_json::from_slice(&query_req.body).unwrap();
    assert_eq!(body["dryRun"], true);
}

/// Mount the dataset/table catalog mocks used by the discover tests:
/// two datasets (`sales`, `ops`); `sales` holds a table + a view (the view
/// must be skipped), `ops` holds one table. `tables.get` returns a schema for
/// both tables and a `numRows` for `orders` only.
async fn mount_discover_catalog(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path(format!("/projects/{PROJECT_ID}/datasets")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "kind": "bigquery#datasetList",
            "etag": "e",
            "datasets": [
                {"datasetReference": {"projectId": PROJECT_ID, "datasetId": "sales"}},
                {"datasetReference": {"projectId": PROJECT_ID, "datasetId": "ops"}}
            ]
        })))
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!("/projects/{PROJECT_ID}/datasets/sales/tables")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tables": [
                {
                    "tableReference": {"projectId": PROJECT_ID, "datasetId": "sales", "tableId": "orders"},
                    "type": "TABLE"
                },
                {
                    "tableReference": {"projectId": PROJECT_ID, "datasetId": "sales", "tableId": "orders_view"},
                    "type": "VIEW"
                }
            ]
        })))
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!("/projects/{PROJECT_ID}/datasets/ops/tables")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tables": [
                {
                    "tableReference": {"projectId": PROJECT_ID, "datasetId": "ops", "tableId": "events"},
                    "type": "TABLE"
                }
            ]
        })))
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!(
            "/projects/{PROJECT_ID}/datasets/sales/tables/orders"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tableReference": {"projectId": PROJECT_ID, "datasetId": "sales", "tableId": "orders"},
            "schema": {
                "fields": [
                    {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
                    {"name": "note", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "tags", "type": "STRING", "mode": "REPEATED"}
                ]
            },
            "numRows": "120"
        })))
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!(
            "/projects/{PROJECT_ID}/datasets/ops/tables/events"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tableReference": {"projectId": PROJECT_ID, "datasetId": "ops", "tableId": "events"},
            "schema": {
                "fields": [
                    {"name": "payload", "type": "JSON"}
                ]
            }
        })))
        .mount(server)
        .await;
}

#[tokio::test]
async fn discover_enumerates_tables_with_schemas_and_estimates() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    mount_discover_catalog(&server).await;

    let (src, _f) = build_source(&server, default_config()).await;
    assert!(src.supports_discover());
    let datasets = src.discover().await.unwrap();

    assert_eq!(datasets.len(), 2, "the view must be skipped: {datasets:?}");

    assert_eq!(datasets[0].name, "sales.orders");
    assert_eq!(datasets[0].kind, "table");
    assert_eq!(datasets[0].estimated_rows, Some(120));
    assert_eq!(
        datasets[0].config_patch,
        json!({"query": "SELECT * FROM `test-project.sales.orders`"})
    );
    let schema = datasets[0].schema.as_ref().unwrap();
    assert_eq!(schema["properties"]["id"]["type"], "integer");
    assert_eq!(
        schema["properties"]["note"]["type"],
        json!(["string", "null"])
    );
    assert_eq!(schema["properties"]["tags"]["type"], "array");

    assert_eq!(datasets[1].name, "ops.events");
    assert_eq!(datasets[1].estimated_rows, None, "no numRows = no estimate");
    let schema = datasets[1].schema.as_ref().unwrap();
    assert_eq!(
        schema["properties"]["payload"]["type"],
        json!(["object", "null"])
    );
}

#[tokio::test]
async fn discover_caps_table_count_and_schema_fetches() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    mount_discover_catalog(&server).await;

    // max_tables = 1 → only `sales.orders` is enumerated (truncation branch);
    // max_schema_fetches = 0 → it is emitted name-only (no tables.get at all).
    let (src, _f) = build_source(&server, default_config()).await;
    let datasets = src.discover_with_caps(1, 0).await.unwrap();
    assert_eq!(datasets.len(), 1);
    assert_eq!(datasets[0].name, "sales.orders");
    assert!(datasets[0].schema.is_none(), "past the schema-fetch cap");
    assert_eq!(datasets[0].estimated_rows, None);
    assert_eq!(
        datasets[0].config_patch["query"],
        "SELECT * FROM `test-project.sales.orders`"
    );

    // No tables.get request may have been issued (schema fetches capped at 0).
    let reqs = server.received_requests().await.unwrap();
    assert!(
        !reqs.iter().any(|r| r.url.path().contains("/tables/orders")),
        "schema fetch must be skipped when past the cap"
    );
}

#[tokio::test]
async fn discover_paginates_dataset_and_table_lists() {
    let server = MockServer::start().await;
    mount_token(&server).await;

    // Dataset page 2 (matched only when pageToken=ds-2 is present).
    Mock::given(method("GET"))
        .and(path(format!("/projects/{PROJECT_ID}/datasets")))
        .and(wiremock::matchers::query_param("pageToken", "ds-2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "kind": "bigquery#datasetList",
            "etag": "e",
            "datasets": [
                {"datasetReference": {"projectId": PROJECT_ID, "datasetId": "b"}}
            ]
        })))
        .mount(&server)
        .await;
    // Dataset page 1 (no pageToken) — carries the next-page token.
    Mock::given(method("GET"))
        .and(path(format!("/projects/{PROJECT_ID}/datasets")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "kind": "bigquery#datasetList",
            "etag": "e",
            "datasets": [
                {"datasetReference": {"projectId": PROJECT_ID, "datasetId": "a"}}
            ],
            "nextPageToken": "ds-2"
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;

    // Dataset `a`: two table-list pages.
    Mock::given(method("GET"))
        .and(path(format!("/projects/{PROJECT_ID}/datasets/a/tables")))
        .and(wiremock::matchers::query_param("pageToken", "t-2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tables": [
                {"tableReference": {"projectId": PROJECT_ID, "datasetId": "a", "tableId": "t2"}, "type": "TABLE"}
            ]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/projects/{PROJECT_ID}/datasets/a/tables")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tables": [
                {"tableReference": {"projectId": PROJECT_ID, "datasetId": "a", "tableId": "t1"}, "type": "TABLE"}
            ],
            "nextPageToken": "t-2"
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;

    // Dataset `b`: an empty dataset (no `tables` key at all).
    Mock::given(method("GET"))
        .and(path(format!("/projects/{PROJECT_ID}/datasets/b/tables")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;

    let (src, _f) = build_source(&server, default_config()).await;
    // Schema fetches capped at 0 so the catalog paging is what's under test.
    let datasets = src.discover_with_caps(500, 0).await.unwrap();
    let names: Vec<&str> = datasets.iter().map(|d| d.name.as_str()).collect();
    assert_eq!(names, vec!["a.t1", "a.t2"]);
}

#[tokio::test]
async fn discover_surfaces_typed_error_on_catalog_failure() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    Mock::given(method("GET"))
        .and(path(format!("/projects/{PROJECT_ID}/datasets")))
        .respond_with(ResponseTemplate::new(403).set_body_json(json!({
            "error": {"code": 403, "message": "Access Denied"}
        })))
        .mount(&server)
        .await;

    let (src, _f) = build_source(&server, default_config()).await;
    match src.discover().await {
        Err(faucet_core::FaucetError::Source(m)) => {
            assert!(m.contains("catalog discovery failed"), "got: {m}");
        }
        other => panic!("expected Source error, got: {other:?}"),
    }
}

#[tokio::test]
async fn check_probe_fails_on_dry_run_error() {
    use faucet_core::check::{CheckContext, ProbeStatus};
    let server = MockServer::start().await;
    mount_token(&server).await;
    Mock::given(method("POST"))
        .and(path(format!("/projects/{PROJECT_ID}/queries")))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "error": {"code": 400, "message": "Syntax error"}
        })))
        .mount(&server)
        .await;

    let (src, _f) = build_source(&server, default_config()).await;
    let report = src.check(&CheckContext::default()).await.unwrap();
    assert_eq!(report.probes.len(), 1);
    match &report.probes[0].status {
        ProbeStatus::Fail { reason } => {
            assert!(reason.contains("dry-run failed"), "got: {reason}");
        }
        other => panic!("expected Fail, got: {other:?}"),
    }
    assert!(
        report.probes[0].hint.is_some(),
        "a failing probe should carry a remediation hint"
    );
}

/// #380: the source advertises the Arrow columnar fast path exactly when
/// `read_api` is enabled — the "columnar path is taken" acceptance criterion.
#[cfg(feature = "arrow")]
#[tokio::test]
async fn columnar_taken_only_in_read_api_mode() {
    use faucet_core::Source as _;
    let server = MockServer::start().await;

    // Query mode → row path only.
    let (query_src, _sa1) = build_source(
        &server,
        BigQuerySourceConfig::new(
            PROJECT_ID,
            BigQueryCredentials::ApplicationDefault,
            "SELECT 1",
        ),
    )
    .await;
    assert!(!query_src.supports_columnar());

    // read_api mode → columnar fast path advertised.
    let (read_src, _sa2) = build_source(
        &server,
        BigQuerySourceConfig::new(PROJECT_ID, BigQueryCredentials::ApplicationDefault, "")
            .with_read_api("my_dataset.my_table"),
    )
    .await;
    assert!(read_src.supports_columnar());

    // Both entry points route to the Storage Read path when `read_api` is set.
    // Building the streams performs no gRPC (they are lazy), so we can exercise
    // the dispatch without a live BigQuery: just confirm a stream is returned.
    let ctx = HashMap::new();
    let _batches = read_src.stream_batches(&ctx, 0);
    let _pages = read_src.stream_pages(&ctx, 0);
}
