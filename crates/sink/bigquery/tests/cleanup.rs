//! Integration tests for the BigQuery scoped-cleanup path (#478): the
//! `cleanup_scope` hook driving a single `DELETE … WHERE <scope> AND NOT
//! EXISTS (… UNNEST(JSON_QUERY_ARRAY(@keys)) …)` job and reporting the row
//! count from the job's DML statistics.

#![allow(deprecated)] // the tests drive the 1.x lifecycle hooks directly (RFC 0014)
use faucet_core::Sink;
use faucet_sink_bigquery::{BigQueryCredentials, BigQuerySink, BigQuerySinkConfig};
use gcp_bigquery_client::client_builder::ClientBuilder;
use serde::Serialize;
use serde_json::json;
use wiremock::matchers::{method, path};
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

fn config_with<F: FnOnce(&mut BigQuerySinkConfig)>(f: F) -> BigQuerySinkConfig {
    let mut c = BigQuerySinkConfig::new(
        PROJECT_ID,
        DATASET_ID,
        TABLE_ID,
        BigQueryCredentials::ApplicationDefault,
    );
    f(&mut c);
    c
}

async fn build_sink(
    server: &MockServer,
    config: BigQuerySinkConfig,
) -> (BigQuerySink, tempfile::NamedTempFile) {
    let sa_json = dummy_service_account_json(&server.uri());
    let sa_file = tempfile::NamedTempFile::new().expect("create sa tempfile");
    std::fs::write(
        sa_file.path(),
        serde_json::to_string_pretty(&sa_json).unwrap(),
    )
    .expect("write sa");
    let client = ClientBuilder::new()
        .with_auth_base_url(format!("{}{AUTH_SCOPE_BASE}", server.uri()))
        .with_v2_base_url(server.uri())
        .build_from_service_account_key_file(sa_file.path().to_str().unwrap())
        .await
        .expect("build client");
    (BigQuerySink::from_parts(config, client), sa_file)
}

fn tables_get_path() -> String {
    format!("/projects/{PROJECT_ID}/datasets/{DATASET_ID}/tables/{TABLE_ID}")
}
fn queries_path() -> String {
    format!("/projects/{PROJECT_ID}/queries")
}

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

fn done_query(job_id: &str) -> serde_json::Value {
    json!({
        "kind": "bigquery#queryResponse",
        "jobComplete": true,
        "jobReference": {"projectId": PROJECT_ID, "jobId": job_id}
    })
}

async fn mount_query_done(server: &MockServer, job_id: &str) {
    Mock::given(method("POST"))
        .and(path(queries_path()))
        .respond_with(ResponseTemplate::new(200).set_body_json(done_query(job_id)))
        .mount(server)
        .await;
}

async fn mount_job_done(server: &MockServer, job_id: &str, deleted: Option<&str>) {
    let mut body = json!({
        "jobReference": {"projectId": PROJECT_ID, "jobId": job_id},
        "status": {"state": "DONE"}
    });
    if let Some(n) = deleted {
        body["statistics"] = json!({"query": {"numDmlAffectedRows": n}});
    }
    Mock::given(method("GET"))
        .and(path(format!("/projects/{PROJECT_ID}/jobs/{job_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(server)
        .await;
}

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

/// The written-key set the pipeline hands to `cleanup_scope`.
fn seen_with(ids: &[i64]) -> faucet_core::SeenKeys {
    let page: Vec<serde_json::Value> = ids.iter().map(|i| json!({"id": i, "name": "x"})).collect();
    let mut seen = faucet_core::SeenKeys::new();
    seen.record_page(&page, &["id".to_string()], 1000);
    seen
}

fn scope_of(
    col: &str,
    v: serde_json::Value,
) -> std::collections::BTreeMap<String, serde_json::Value> {
    std::collections::BTreeMap::from([(col.to_string(), v)])
}

fn cleanup_config() -> BigQuerySinkConfig {
    config_with(|c| {
        c.write.write_mode = faucet_core::WriteMode::Upsert;
        c.write.key = vec!["id".into()];
    })
}

/// Pull the one `DELETE` query body the sink posted.
async fn captured_delete(server: &MockServer) -> serde_json::Value {
    captured_query_bodies(server)
        .await
        .into_iter()
        .find(|b| {
            b["query"]
                .as_str()
                .unwrap_or("")
                .starts_with("DELETE FROM `p.d.t`")
        })
        .expect("cleanup DELETE query")
}

#[tokio::test]
async fn sink_advertises_cleanup() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    let (sink, _sa) = build_sink(&server, cleanup_config()).await;
    assert!(sink.supports_cleanup());
}

#[tokio::test]
async fn cleanup_posts_one_scoped_delete_with_two_json_params() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    mount_query_done(&server, "job-c").await;
    mount_job_done(&server, "job-c", Some("3")).await;

    let (sink, _sa) = build_sink(&server, cleanup_config()).await;
    let deleted = sink
        .cleanup_scope(&scope_of("name", json!("acme")), &seen_with(&[1, 2]))
        .await
        .expect("cleanup");
    // The delete count comes from the job's DML statistics.
    assert_eq!(deleted, 3);

    let body = captured_delete(&server).await;
    let q = body["query"].as_str().unwrap();
    assert!(q.contains("T.`name` = JSON_VALUE(@scope, '$.name')"), "{q}");
    assert!(
        q.contains("NOT EXISTS (SELECT 1 FROM UNNEST(JSON_QUERY_ARRAY(@keys)) AS k WHERE T.`id` = CAST(JSON_VALUE(k, '$.id') AS INT64))"),
        "{q}"
    );
    assert_eq!(body["parameterMode"], "NAMED");

    // Exactly two bound params — the whole key set rides in one JSON STRING.
    let params = body["queryParameters"].as_array().expect("params");
    assert_eq!(params.len(), 2, "{params:?}");
    let by_name = |n: &str| -> serde_json::Value {
        let raw = params
            .iter()
            .find(|p| p["name"] == n)
            .unwrap_or_else(|| panic!("missing param {n}"))["parameterValue"]["value"]
            .as_str()
            .expect("string value");
        serde_json::from_str(raw).expect("param is JSON text")
    };
    assert_eq!(by_name("scope"), json!({"name": "acme"}));
    assert_eq!(by_name("keys"), json!([{"id": 1}, {"id": 2}]));
}

#[tokio::test]
async fn cleanup_with_no_written_keys_still_deletes_the_scope() {
    // The motivating case: the source reported the scope empty, so every row in
    // it is stale. An empty `seen` set must NOT short-circuit.
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    mount_query_done(&server, "job-e").await;
    mount_job_done(&server, "job-e", Some("5")).await;

    let (sink, _sa) = build_sink(&server, cleanup_config()).await;
    let deleted = sink
        .cleanup_scope(
            &scope_of("name", json!("acme")),
            &faucet_core::SeenKeys::new(),
        )
        .await
        .expect("cleanup");
    assert_eq!(deleted, 5);

    let body = captured_delete(&server).await;
    let keys = body["queryParameters"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["name"] == "keys")
        .expect("keys param")["parameterValue"]["value"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(keys, "[]", "an empty key set is `[]`, not a skipped run");
}

#[tokio::test]
async fn cleanup_reports_zero_when_the_job_carries_no_dml_stats() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    mount_query_done(&server, "job-n").await;
    mount_job_done(&server, "job-n", None).await;

    let (sink, _sa) = build_sink(&server, cleanup_config()).await;
    let deleted = sink
        .cleanup_scope(&scope_of("name", json!("acme")), &seen_with(&[1]))
        .await
        .expect("cleanup");
    assert_eq!(deleted, 0);
}

#[tokio::test]
async fn cleanup_rejects_a_scope_column_that_is_not_in_the_table() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;

    let (sink, _sa) = build_sink(&server, cleanup_config()).await;
    let err = sink
        .cleanup_scope(&scope_of("nope", json!(1)), &seen_with(&[1]))
        .await
        .expect_err("unknown scope column must be refused");
    assert!(err.to_string().contains("scope column 'nope'"), "{err}");
    // Nothing was sent: the config error is caught before any DML runs.
    assert!(
        captured_query_bodies(&server).await.is_empty(),
        "no query may be issued for an invalid scope"
    );
}

#[tokio::test]
async fn cleanup_requires_a_non_empty_key() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    let cfg = config_with(|c| {
        c.write.write_mode = faucet_core::WriteMode::Upsert;
        c.write.key = vec![];
    });
    let (sink, _sa) = build_sink(&server, cfg).await;
    let err = sink
        .cleanup_scope(&scope_of("name", json!("acme")), &seen_with(&[1]))
        .await
        .expect_err("cleanup without a key must be refused");
    assert!(err.to_string().contains("non-empty `key`"), "{err}");
}

#[tokio::test]
async fn cleanup_fails_when_the_delete_job_fails() {
    // A runtime job failure comes back as HTTP 200 with an errorResult, so the
    // authoritative `get_job` check must turn it into an `Err` — reporting a
    // successful cleanup here would claim stale rows were removed when they
    // were not.
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    mount_query_done(&server, "job-f").await;
    Mock::given(method("GET"))
        .and(path(format!("/projects/{PROJECT_ID}/jobs/job-f")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobReference": {"projectId": PROJECT_ID, "jobId": "job-f"},
            "status": {"state": "DONE", "errorResult": {"reason": "invalidQuery", "message": "boom"}}
        })))
        .mount(&server)
        .await;

    let (sink, _sa) = build_sink(&server, cleanup_config()).await;
    let err = sink
        .cleanup_scope(&scope_of("name", json!("acme")), &seen_with(&[1]))
        .await
        .expect_err("a failed delete job must not report success");
    assert!(err.to_string().contains("failed"), "{err}");
}
