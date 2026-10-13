//! `faucet-conformance` Tier-1 battery for the BigQuery source.
//!
//! Check 1 — the connector's config JSON Schema is a valid, well-formed value.
//! Check 2 — `stream_pages` pages under a bounded batch size (every record
//! streamed; peak page ≤ batch_size and < total), i.e. memory is O(batch_size)
//! regardless of total volume.
//!
//! The bounded-memory check drives the source against a wiremock fake of the
//! BigQuery REST API (the same OAuth-token + service-account dance the other
//! source tests use). The result set is 6 000 rows spread across three
//! `jobs.query` / `jobs.getQueryResults` pages (2 000 rows each) linked by
//! `pageToken`. The source re-frames those into pages of the configured
//! `batch_size` (250), so peak page is 250 < 6 000.

use faucet_conformance::{
    assert_bounded_memory, assert_config_schema_valid_value, assert_connector_name_nonempty,
    assert_errors_not_panics,
};
use faucet_source_bigquery::{BigQueryCredentials, BigQuerySource, BigQuerySourceConfig};
use gcp_bigquery_client::client_builder::ClientBuilder;
use serde::Serialize;
use serde_json::{Value, json};
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

const PROJECT_ID: &str = "test-project";
const AUTH_TOKEN_PATH: &str = "/:o/oauth2/token";
const AUTH_SCOPE_BASE: &str = "/auth/bigquery";
const JOB_ID: &str = "conformance-job";
const HTTP_PAGE_ROWS: i64 = 2_000;
const TOTAL_ROWS: usize = 6_000;

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

    // One token provider shared by the typed client and the result-page
    // reader, which sends `jobs.getQueryResults` to `{api_host}/bigquery/v2`.
    let tokens =
        faucet_common_bigquery::token_provider(&BigQueryCredentials::ServiceAccountKeyPath {
            path: sa_file.path().to_str().unwrap().into(),
        })
        .await
        .expect("token provider against mock");
    let client = ClientBuilder::new()
        .with_v2_base_url(server.uri())
        .build_from_authenticator(tokens.clone())
        .await
        .expect("build bigquery client against mock");
    let mut config = config;
    if config.api_host.is_none() {
        config.api_host = Some(server.uri());
    }

    (
        BigQuerySource::from_parts_with_tokens(config, client, tokens),
        sa_file,
    )
}

/// Path of a `jobs.getQueryResults` request for `job`.
fn results_path(job: &str) -> String {
    format!("/bigquery/v2/projects/{PROJECT_ID}/queries/{job}")
}

fn schema_one_col() -> Value {
    json!({"fields": [{"name": "id", "type": "INTEGER"}]})
}

fn rows(start: i64, count: i64) -> Vec<Value> {
    (start..start + count)
        .map(|i| json!({"f": [{"v": i.to_string()}]}))
        .collect()
}

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(BigQuerySourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-bigquery");
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_bounded_memory() {
    let server = MockServer::start().await;
    mount_token(&server).await;

    // jobs.query: first HTTP page (rows 0..2000) + pageToken "tok-1".
    Mock::given(method("POST"))
        .and(path(format!("/projects/{PROJECT_ID}/queries")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": true,
            "schema": schema_one_col(),
            "jobReference": {"projectId": PROJECT_ID, "jobId": JOB_ID},
            "rows": rows(0, HTTP_PAGE_ROWS),
            "pageToken": "tok-1",
        })))
        .mount(&server)
        .await;

    // getQueryResults page 2 (rows 2000..4000) + pageToken "tok-2".
    Mock::given(method("GET"))
        .and(path(results_path(JOB_ID)))
        .and(query_param("pageToken", "tok-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": true,
            "schema": schema_one_col(),
            "rows": rows(HTTP_PAGE_ROWS, HTTP_PAGE_ROWS),
            "pageToken": "tok-2",
        })))
        .mount(&server)
        .await;

    // getQueryResults page 3 (rows 4000..6000) — final page, no further token.
    Mock::given(method("GET"))
        .and(path(results_path(JOB_ID)))
        .and(query_param("pageToken", "tok-2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": true,
            "schema": schema_one_col(),
            "rows": rows(2 * HTTP_PAGE_ROWS, HTTP_PAGE_ROWS),
        })))
        .mount(&server)
        .await;

    // Config batch_size must equal the batch passed to the battery — this
    // overriding source treats its config batch_size as authoritative.
    let config = BigQuerySourceConfig::new(
        PROJECT_ID,
        BigQueryCredentials::ApplicationDefault,
        "SELECT id FROM events",
    )
    .with_batch_size(250);
    let (source, _sa_file) = build_source(&server, config).await;

    assert_bounded_memory(&source, 250, TOTAL_ROWS).await;
}

// ── Check 12: discovery round-trips ───────────────────────────────────────────

/// Every table `discover()` reports (datasets.list → tables.list → tables.get)
/// must be genuinely selectable: deep-merge its config_patch (`{"query": …}`)
/// onto the base config, rebuild the source, and read it via `jobs.query`. A
/// realistic mock of the BigQuery catalog + query REST API.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_discover_roundtrips() {
    let server = MockServer::start().await;
    mount_token(&server).await;

    // Catalog: one dataset `sales` with one physical table `orders`.
    Mock::given(method("GET"))
        .and(path(format!("/projects/{PROJECT_ID}/datasets")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "kind": "bigquery#datasetList",
            "etag": "etag-0",
            "datasets": [{
                "datasetReference": { "projectId": PROJECT_ID, "datasetId": "sales" }
            }]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!(
            "/projects/{PROJECT_ID}/datasets/sales/tables"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tables": [{
                "type": "TABLE",
                "tableReference": {
                    "projectId": PROJECT_ID, "datasetId": "sales", "tableId": "orders"
                }
            }]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!(
            "/projects/{PROJECT_ID}/datasets/sales/tables/orders"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tableReference": {
                "projectId": PROJECT_ID, "datasetId": "sales", "tableId": "orders"
            },
            "schema": schema_one_col(),
            "numRows": "3",
        })))
        .mount(&server)
        .await;
    // Read path for the rebuilt source: a single jobs.query page of 3 rows.
    Mock::given(method("POST"))
        .and(path(format!("/projects/{PROJECT_ID}/queries")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": true,
            "schema": schema_one_col(),
            "jobReference": {"projectId": PROJECT_ID, "jobId": JOB_ID},
            "rows": rows(0, 3),
        })))
        .mount(&server)
        .await;

    let base_cfg = BigQuerySourceConfig::new(
        PROJECT_ID,
        BigQueryCredentials::ApplicationDefault,
        "SELECT 1",
    );
    let (source, _sa_file) = build_source(&server, base_cfg).await;

    faucet_conformance::assert_discover_roundtrips(&source, |patch| {
        let server_uri = server.uri();
        async move {
            // Rebuild against the same mock: a fresh client + config carrying
            // the discovered query. `build_source` needs a &MockServer, so
            // reconstruct the client the same way here.
            let query = patch["query"].as_str().expect("query patch").to_string();
            let cfg = BigQuerySourceConfig::new(
                PROJECT_ID,
                BigQueryCredentials::ApplicationDefault,
                query,
            );
            let sa_json = dummy_service_account_json(&server_uri);
            let sa_file = tempfile::NamedTempFile::new().unwrap();
            std::fs::write(
                sa_file.path(),
                serde_json::to_string_pretty(&sa_json).unwrap(),
            )
            .unwrap();
            let client = ClientBuilder::new()
                .with_auth_base_url(format!("{server_uri}{AUTH_SCOPE_BASE}"))
                .with_v2_base_url(server_uri)
                .build_from_service_account_key_file(sa_file.path().to_str().unwrap())
                .await
                .expect("build bigquery client against mock");
            Box::new(BigQuerySource::from_parts(cfg, client)) as Box<dyn faucet_core::Source>
        }
    })
    .await;
}

// ── Check 6: errors, not panics ──────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn conformance_errors_not_panics() {
    // Unreachable endpoint: the client is built (lazily — no network yet) with
    // both its OAuth-token and v2 base URLs pointed at port 1, which refuses
    // connections immediately on all platforms. The first read (OAuth token
    // fetch → query) surfaces a typed `FaucetError` without panicking.
    const UNREACHABLE: &str = "http://127.0.0.1:1";

    let sa_json = dummy_service_account_json(UNREACHABLE);
    let sa_file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(
        sa_file.path(),
        serde_json::to_string_pretty(&sa_json).unwrap(),
    )
    .unwrap();

    let client = ClientBuilder::new()
        .with_auth_base_url(format!("{UNREACHABLE}{AUTH_SCOPE_BASE}"))
        .with_v2_base_url(UNREACHABLE.to_string())
        .build_from_service_account_key_file(sa_file.path().to_str().unwrap())
        .await
        .expect("build bigquery client against unreachable base");

    let config = BigQuerySourceConfig::new(
        PROJECT_ID,
        BigQueryCredentials::ApplicationDefault,
        "SELECT id FROM events",
    );
    let source = BigQuerySource::from_parts(config, client);

    // Check 10: connector_name() is non-empty (reuses this offline instance).
    assert_connector_name_nonempty(&source);

    assert_errors_not_panics(&source).await;
}

// ── Descriptor agreement (RFC 0013) ───────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn conformance_descriptor_matches_built_source() {
    const UNREACHABLE: &str = "http://127.0.0.1:1";
    let sa_file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(
        sa_file.path(),
        serde_json::to_string_pretty(&dummy_service_account_json(UNREACHABLE)).unwrap(),
    )
    .unwrap();
    let base = json!({
        "project_id": PROJECT_ID,
        "auth": {"type": "application_default"},
        "query": "SELECT 1",
    });
    let mut read_api = base.clone();
    read_api["read_api"] = json!(true);
    read_api["read_table"] = json!("d.t");
    for v in [base, read_api] {
        let cfg: BigQuerySourceConfig = serde_json::from_value(v.clone()).unwrap();
        let client = ClientBuilder::new()
            .with_auth_base_url(format!("{UNREACHABLE}{AUTH_SCOPE_BASE}"))
            .with_v2_base_url(UNREACHABLE.to_string())
            .build_from_service_account_key_file(sa_file.path().to_str().unwrap())
            .await
            .expect("offline client");
        let built = BigQuerySource::from_parts(cfg, client);
        faucet_conformance::assert_descriptor_matches_source(
            &faucet_source_bigquery::DESCRIPTOR,
            "bigquery",
            &v,
            &built,
        );
    }
}

// ── Check: backend calls honour the call deadline and cancel (#880) ──────────

#[tokio::test(flavor = "multi_thread")]
async fn conformance_call_deadline() {
    let server = MockServer::start().await;
    mount_token(&server).await;
    Mock::given(wiremock::matchers::any())
        .respond_with(ResponseTemplate::new(200).set_delay(std::time::Duration::from_secs(60)))
        .mount(&server)
        .await;
    let make = || {
        let config = BigQuerySourceConfig::new(
            PROJECT_ID,
            BigQueryCredentials::ApplicationDefault,
            "SELECT id FROM events",
        );
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let (source, file) = build_source(&server, config).await;
                std::mem::forget(file);
                source
            })
        })
    };
    faucet_conformance::calls::assert_source_call_deadline(make).await;
}
