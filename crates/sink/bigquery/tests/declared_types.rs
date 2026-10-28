//! Integration tests for creating the BigQuery target from a source's declared
//! column types (#955), against a wiremock fake of `tables.get` and
//! `jobs.query`.

#![allow(deprecated)] // the tests drive the 1.x lifecycle hooks directly (RFC 0014)
use faucet_core::Sink;
use faucet_sink_bigquery::BigQuerySink;
use faucet_sink_bigquery::{BigQueryCredentials, BigQuerySinkConfig};
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
        "private_key": include_str!("fixtures/bigquery_test_key.pem"),
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

/// A completed `jobs.query` response carrying the given jobId.
fn done_query(job_id: &str) -> serde_json::Value {
    json!({
        "kind": "bigquery#queryResponse",
        "jobComplete": true,
        "jobReference": {"projectId": PROJECT_ID, "jobId": job_id}
    })
}

/// Mount a `get_job` that reports DONE with no error.
async fn mount_job_done(server: &MockServer, job_id: &str) {
    Mock::given(method("GET"))
        .and(path(format!("/projects/{PROJECT_ID}/jobs/{job_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobReference": {"projectId": PROJECT_ID, "jobId": job_id},
            "status": {"state": "DONE"}
        })))
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

/// What the postgres source declares for `id integer, amount numeric(12,2),
/// total numeric, day date, at timestamptz, local timestamp, uid uuid`.
fn declared() -> faucet_core::DeclaredColumns {
    use faucet_core::{DeclaredColumn, LogicalType as L};
    faucet_core::DeclaredColumns::new([
        DeclaredColumn::new("id", L::Int64, false),
        DeclaredColumn::new(
            "amount",
            L::Decimal {
                precision: Some(12),
                scale: Some(2),
            },
            true,
        ),
        DeclaredColumn::new(
            "total",
            L::Decimal {
                precision: None,
                scale: None,
            },
            true,
        ),
        DeclaredColumn::new("day", L::Date, true),
        DeclaredColumn::new("at", L::Timestamp { tz: true }, true),
        DeclaredColumn::new("local", L::Timestamp { tz: false }, true),
        DeclaredColumn::new("uid", L::Uuid, true),
    ])
}

async fn missing_target(server: &MockServer) {
    mount_token_endpoint(server).await;
    Mock::given(method("GET"))
        .and(path(tables_get_path()))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "error": {"code": 404, "message": "Not found: Table p:d.t", "errors": []}
        })))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path(queries_path()))
        .respond_with(ResponseTemplate::new(200).set_body_json(done_query("job-ddl")))
        .mount(server)
        .await;
    mount_job_done(server, "job-ddl").await;
}

async fn create_ddl(server: &MockServer) -> Vec<String> {
    captured_query_bodies(server)
        .await
        .iter()
        .filter_map(|b| b["query"].as_str().map(str::to_string))
        .filter(|q| q.contains("CREATE TABLE"))
        .collect()
}

#[tokio::test]
async fn an_empty_run_creates_the_declared_table_typed() {
    let server = MockServer::start().await;
    missing_target(&server).await;
    let (sink, _sa) = build_sink(&server).await;
    assert!(sink.capabilities().declared_types());
    sink.set_declared_columns(declared());
    sink.complete_run().await.expect("complete_run");
    let ddl = create_ddl(&server).await;
    assert_eq!(
        ddl,
        [
            "CREATE TABLE IF NOT EXISTS `p.d.t` (`id` INT64, `amount` NUMERIC, `total` BIGNUMERIC, \
          `day` DATE, `at` TIMESTAMP, `local` DATETIME, `uid` STRING)"
        ],
    );
}

#[tokio::test]
async fn a_reshaped_or_absent_declaration_creates_nothing_for_an_empty_run() {
    let server = MockServer::start().await;
    missing_target(&server).await;
    let (sink, _sa) = build_sink(&server).await;
    sink.complete_run().await.expect("complete_run");
    sink.set_declared_columns(declared().reshaped());
    sink.complete_run().await.expect("complete_run");
    assert!(create_ddl(&server).await.is_empty());
}
