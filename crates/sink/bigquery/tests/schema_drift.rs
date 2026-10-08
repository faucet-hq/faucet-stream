//! Integration tests for the BigQuery sink's schema-drift hooks (#194):
//! `current_schema` (via a schema-only `tables.get`) and `evolve_schema` (via
//! `ALTER TABLE` DDL run through `jobs.query`).

use faucet_core::Sink;
use faucet_core::drift::{ColumnChange, SchemaEvolution};
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

#[tokio::test]
async fn advertises_schema_evolution_support() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    let (sink, _sa) = build_sink(&server).await;
    assert!(sink.supports_schema_evolution());
}

#[tokio::test]
async fn current_schema_maps_table_fields() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    let (sink, _sa) = build_sink(&server).await;

    let schema = sink
        .current_schema()
        .await
        .expect("current_schema")
        .unwrap();
    assert_eq!(schema["type"], "object");
    // Every column reported nullable (safe default for drift).
    assert_eq!(
        schema["properties"]["id"]["type"],
        json!(["integer", "null"])
    );
    assert_eq!(
        schema["properties"]["name"]["type"],
        json!(["string", "null"])
    );
}

/// A table holding a column type the REST client cannot decode (`RANGE`)
/// still yields its schema: the typed `tables.get` fails to decode, the raw
/// fallback reads the same resource and types the column as a string (SQL-172).
#[tokio::test]
async fn current_schema_tolerates_a_range_column() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    let table = json!({
        "tableReference": {"projectId": PROJECT_ID, "datasetId": DATASET_ID, "tableId": TABLE_ID},
        "schema": {"fields": [
            {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "span", "type": "RANGE", "rangeElementType": {"type": "DATE"}}
        ]}
    });
    for p in [
        tables_get_path(),
        format!("/bigquery/v2{}", tables_get_path()),
    ] {
        Mock::given(method("GET"))
            .and(path(p))
            .respond_with(ResponseTemplate::new(200).set_body_json(table.clone()))
            .mount(&server)
            .await;
    }
    let sa_json = dummy_service_account_json(&server.uri());
    let sa_file = tempfile::NamedTempFile::new().expect("sa tempfile");
    std::fs::write(sa_file.path(), sa_json.to_string()).expect("write sa");
    let client = ClientBuilder::new()
        .with_auth_base_url(format!("{}{AUTH_SCOPE_BASE}", server.uri()))
        .with_v2_base_url(server.uri())
        .build_from_service_account_key_file(sa_file.path().to_str().unwrap())
        .await
        .expect("client");
    let mut config = BigQuerySinkConfig::new(
        PROJECT_ID,
        DATASET_ID,
        TABLE_ID,
        BigQueryCredentials::ServiceAccountKeyPath {
            path: sa_file.path().to_str().unwrap().into(),
        },
    );
    config.upload_base_url = Some(server.uri());
    let sink = BigQuerySink::from_parts(config, client);

    let schema = sink
        .current_schema()
        .await
        .expect("a RANGE column must not fail the schema read")
        .unwrap();
    assert_eq!(
        schema["properties"]["span"]["type"],
        json!(["string", "null"])
    );
    assert_eq!(
        schema["properties"]["id"]["type"],
        json!(["integer", "null"])
    );
}

#[tokio::test]
async fn current_schema_none_for_missing_table() {
    // A 404 on tables.get → Ok(None), so the drift pass treats the target as
    // not-yet-created rather than erroring.
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    Mock::given(method("GET"))
        .and(path(tables_get_path()))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "error": {"code": 404, "message": "Not found: Table p:d.t", "errors": []}
        })))
        .mount(&server)
        .await;
    let (sink, _sa) = build_sink(&server).await;
    assert_eq!(sink.current_schema().await.expect("current_schema"), None);
}

#[tokio::test]
async fn current_schema_none_for_schemaless_table() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    Mock::given(method("GET"))
        .and(path(tables_get_path()))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tableReference": {"projectId": PROJECT_ID, "datasetId": DATASET_ID, "tableId": TABLE_ID},
            "schema": {"fields": []}
        })))
        .mount(&server)
        .await;
    let (sink, _sa) = build_sink(&server).await;
    assert_eq!(sink.current_schema().await.expect("current_schema"), None);
}

#[tokio::test]
async fn evolve_schema_issues_alter_table_ddl() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    // Every DDL statement resolves to job-ddl; one get_job(DONE) covers all.
    Mock::given(method("POST"))
        .and(path(queries_path()))
        .respond_with(ResponseTemplate::new(200).set_body_json(done_query("job-ddl")))
        .mount(&server)
        .await;
    mount_job_done(&server, "job-ddl").await;

    let (sink, _sa) = build_sink(&server).await;
    let evo = SchemaEvolution {
        additions: vec![ColumnChange {
            name: "email".into(),
            from: None,
            to: json!({"type": ["string", "null"]}),
        }],
        widenings: vec![ColumnChange {
            name: "id".into(),
            from: Some(json!({"type": "integer"})),
            to: json!({"type": "number"}),
        }],
        relax_nullability: vec!["name".into()],
    };
    sink.evolve_schema(&evo).await.expect("evolve_schema");

    let bodies = captured_query_bodies(&server).await;
    let queries: Vec<&str> = bodies.iter().filter_map(|b| b["query"].as_str()).collect();
    assert!(
        queries
            .iter()
            .any(|q| q.contains("ALTER TABLE `p.d.t` ADD COLUMN IF NOT EXISTS `email` STRING")),
        "missing ADD COLUMN DDL; got {queries:?}"
    );
    assert!(
        queries
            .iter()
            .any(|q| q.contains("ALTER TABLE `p.d.t` ALTER COLUMN `id` SET DATA TYPE FLOAT64")),
        "missing widening DDL; got {queries:?}"
    );
    assert!(
        queries
            .iter()
            .any(|q| q.contains("ALTER TABLE `p.d.t` ALTER COLUMN `name` DROP NOT NULL")),
        "missing DROP NOT NULL DDL; got {queries:?}"
    );
}

#[tokio::test]
async fn evolve_schema_invalidates_cached_schema() {
    // After evolve_schema, the next exactly-once write must re-fetch the table
    // schema (the cache was reset). We assert a second tables.get is issued by
    // demanding the schema mock fire at least twice.
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;

    // tables.get must be hit more than once: once on the first idempotent write,
    // again after evolve_schema invalidates the cache.
    Mock::given(method("GET"))
        .and(path(tables_get_path()))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tableReference": {"projectId": PROJECT_ID, "datasetId": DATASET_ID, "tableId": TABLE_ID},
            "schema": {"fields": [
                {"name": "id", "type": "INTEGER", "mode": "REQUIRED"}
            ]}
        })))
        .expect(2..)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(queries_path()))
        .respond_with(ResponseTemplate::new(200).set_body_json(done_query("job-x")))
        .mount(&server)
        .await;
    mount_job_done(&server, "job-x").await;

    let (sink, _sa) = build_sink(&server).await;
    // 1st write fills the cache.
    sink.write_batch_idempotent(&[json!({"id": 1})], "s", "00000000000000000001")
        .await
        .expect("first write");
    // Evolve resets the cache.
    let evo = SchemaEvolution {
        additions: vec![ColumnChange {
            name: "email".into(),
            from: None,
            to: json!({"type": ["string", "null"]}),
        }],
        ..Default::default()
    };
    sink.evolve_schema(&evo).await.expect("evolve");
    // 2nd write must re-fetch (asserted by the .expect(2..) on the schema mock).
    sink.write_batch_idempotent(&[json!({"id": 2})], "s", "00000000000000000002")
        .await
        .expect("second write");
}
