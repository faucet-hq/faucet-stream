//! Integration tests for [`BigQuerySink::write_batch_partial`] against a
//! wiremock-driven `tabledata.insertAll` endpoint.
//!
//! The test harness reuses the same `build_sink` / `dummy_service_account_json`
//! pattern from `batching.rs` so both test files exercise the same code paths.

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

/// A throwaway service account JSON — `yup_oauth2` needs a valid-looking
/// private key to assemble the JWT it posts to the (mocked) token endpoint.
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

/// Mount a token endpoint on the mock server (required by `yup_oauth2` during
/// client initialisation).
async fn mount_token_endpoint(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path(AUTH_TOKEN_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(fake_token()))
        .mount(server)
        .await;
}

/// Build a [`BigQuerySink`] wired to the given mock server with the specified
/// `batch_size`. Returns the sink and the tempfile holding the dummy service
/// account JSON (the tempfile must outlive the sink to keep the path valid).
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
        .with_auth_base_url(format!("{}{AUTH_SCOPE_BASE}", server.uri()))
        .with_v2_base_url(server.uri())
        .build_from_service_account_key_file(sa_file.path().to_str().unwrap())
        .await
        .expect("build bigquery client against mock");

    // The append path probes `tables.get` before writing (create_table defaults
    // on); stub a 200 so the existing table is found and no create is attempted.
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

    let config = BigQuerySinkConfig::new(
        PROJECT_ID,
        DATASET_ID,
        TABLE_ID,
        BigQueryCredentials::ApplicationDefault, // unused: from_parts bypasses auth
    )
    .with_batch_size(batch_size)
    // This file asserts the `insertAll` envelope and its per-row error
    // handling. `write_batch_partial` takes that path regardless (a DLQ needs
    // row isolation), but the plain `write_batch` cases here would otherwise
    // take the load path that became the default in #612.
    .with_media_load(false);

    (BigQuerySink::from_parts(config, client), sa_file)
}

fn insert_path() -> String {
    format!("/projects/{PROJECT_ID}/datasets/{DATASET_ID}/tables/{TABLE_ID}/insertAll")
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn all_rows_succeed_returns_all_ok() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    Mock::given(method("POST"))
        .and(path(insert_path()))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;

    let (sink, _sa_file) = build_sink(&server, 0).await;
    let records = vec![json!({"a": 1}), json!({"a": 2})];
    let outcomes = sink.write_batch_partial(&records).await.unwrap();
    assert_eq!(outcomes.len(), 2);
    assert!(outcomes.iter().all(|o| o.is_ok()));
}

#[tokio::test]
async fn sparse_insert_errors_routed_by_index() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    let body = json!({
        "insertErrors": [
            { "index": 1, "errors": [{ "reason": "invalid", "message": "bad ts" }] },
            { "index": 3, "errors": [{ "reason": "invalid", "message": "bad ts" }] }
        ]
    });
    Mock::given(method("POST"))
        .and(path(insert_path()))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&server)
        .await;

    let (sink, _sa_file) = build_sink(&server, 0).await;
    let records: Vec<_> = (0..5).map(|i| json!({"i": i})).collect();
    let outcomes = sink.write_batch_partial(&records).await.unwrap();
    assert_eq!(outcomes.len(), 5);
    assert!(outcomes[0].is_ok());
    assert!(outcomes[1].is_err());
    assert!(outcomes[2].is_ok());
    assert!(outcomes[3].is_err());
    assert!(outcomes[4].is_ok());
    let msg = outcomes[1].as_ref().unwrap_err().to_string();
    assert!(msg.contains("bad ts"), "got: {msg}");
}

#[tokio::test]
async fn http_failure_bubbles_outer_err() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    Mock::given(method("POST"))
        .and(path(insert_path()))
        .respond_with(ResponseTemplate::new(401).set_body_string("Unauthorized"))
        .mount(&server)
        .await;

    let (sink, _sa_file) = build_sink(&server, 0).await;
    let records = vec![json!({"a": 1})];
    let result = sink.write_batch_partial(&records).await;
    assert!(
        matches!(result, Err(faucet_core::FaucetError::Sink(_))),
        "expected Err(FaucetError::Sink), got: {result:?}"
    );
}

#[tokio::test]
async fn chunked_outcomes_concatenate_in_order() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    // Both insertAll calls return row 0 (within each chunk) as failed.
    // For the first chunk (records 0+1), chunk-local index 0 → global index 0.
    // For the second chunk (records 2+3), chunk-local index 0 → global index 2.
    Mock::given(method("POST"))
        .and(path(insert_path()))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "insertErrors": [{ "index": 0, "errors": [{ "message": "x" }] }]
        })))
        .expect(2)
        .mount(&server)
        .await;

    let (sink, _sa_file) = build_sink(&server, 2).await;

    let records: Vec<_> = (0..4).map(|i| json!({"i": i})).collect();
    let outcomes = sink.write_batch_partial(&records).await.unwrap();
    assert_eq!(outcomes.len(), 4);
    // Chunk 1: index 0 fails, index 1 ok → global [0]=err, [1]=ok
    assert!(outcomes[0].is_err(), "record 0 should be err");
    assert!(outcomes[1].is_ok(), "record 1 should be ok");
    // Chunk 2: index 0 fails, index 1 ok → global [2]=err, [3]=ok
    assert!(outcomes[2].is_err(), "record 2 should be err");
    assert!(outcomes[3].is_ok(), "record 3 should be ok");
}

/// Capture the JSON body of every `insertAll` request the client sent,
/// transparently gunzipping it (gcp-bigquery-client's default `gzip` feature
/// gzips the insertAll body with `Content-Encoding: gzip`).
async fn captured_insert_bodies(server: &MockServer) -> Vec<serde_json::Value> {
    use flate2::read::GzDecoder;
    use std::io::Read;

    server
        .received_requests()
        .await
        .expect("request recording is enabled")
        .into_iter()
        .filter(|r| r.url.path().ends_with("/insertAll"))
        .map(|r| {
            let gzipped = r
                .headers
                .get("content-encoding")
                .and_then(|v| v.to_str().ok())
                .map(|v| v.contains("gzip"))
                .unwrap_or(false);
            let bytes = if gzipped {
                let mut decoder = GzDecoder::new(&r.body[..]);
                let mut out = Vec::new();
                decoder
                    .read_to_end(&mut out)
                    .expect("gunzip insertAll body");
                out
            } else {
                r.body.clone()
            };
            serde_json::from_slice(&bytes).expect("insertAll body is JSON")
        })
        .collect()
}

#[tokio::test]
async fn write_batch_partial_sends_skip_invalid_rows_true() {
    // C3 regression (audit #146): the DLQ/partial path MUST set
    // skipInvalidRows=true. Otherwise, under the default skipInvalidRows=false,
    // BigQuery commits *nothing* for a chunk that contains a bad row while
    // `write_batch_partial` maps the unflagged siblings to `Ok(())` — the
    // pipeline then advances the bookmark over rows that were never persisted
    // (silent data loss).
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    Mock::given(method("POST"))
        .and(path(insert_path()))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;

    let (sink, _sa_file) = build_sink(&server, 0).await;
    let records = vec![json!({"a": 1}), json!({"a": 2})];
    let outcomes = sink
        .write_batch_partial(&records)
        .await
        .expect("partial write");
    assert_eq!(outcomes.len(), 2);
    assert!(outcomes.iter().all(|o| o.is_ok()));

    let bodies = captured_insert_bodies(&server).await;
    assert_eq!(bodies.len(), 1, "expected one insertAll call");
    assert_eq!(
        bodies[0]["skipInvalidRows"],
        json!(true),
        "the DLQ/partial path must send skipInvalidRows=true (C3)"
    );
}

#[tokio::test]
async fn insert_request_body_carries_rows_in_json_envelope() {
    // Assert the exact `tabledata.insertAll` request body shape: a `rows`
    // array where each entry wraps the record under `json`.
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    Mock::given(method("POST"))
        .and(path(insert_path()))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;

    let (sink, _sa_file) = build_sink(&server, 0).await;
    let records = vec![json!({"a": 1, "b": "x"}), json!({"a": 2, "b": "y"})];
    let written = sink.write_batch(&records).await.expect("write_batch");
    assert_eq!(written, 2);

    let bodies = captured_insert_bodies(&server).await;
    assert_eq!(bodies.len(), 1);
    let rows = bodies[0]["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["json"], json!({"a": 1, "b": "x"}));
    assert_eq!(rows[1]["json"], json!({"a": 2, "b": "y"}));
    // No insert_id_field configured → no insertId on any row.
    assert!(rows[0].get("insertId").is_none());
    assert!(rows[1].get("insertId").is_none());
}

#[tokio::test]
async fn insert_id_field_populates_per_row_insert_id() {
    // With `insert_id_field` set, each row's `insertId` is the field's value.
    // A row missing the field is sent without an insertId.
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    // Existing table (append probes tables.get before writing).
    Mock::given(method("GET"))
        .and(path(format!(
            "/projects/{PROJECT_ID}/datasets/{DATASET_ID}/tables/{TABLE_ID}"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tableReference": {"projectId": PROJECT_ID, "datasetId": DATASET_ID, "tableId": TABLE_ID},
            "schema": {"fields": [{"name": "v", "type": "INTEGER", "mode": "NULLABLE"}]}
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(insert_path()))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;

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
        .unwrap();
    let config = BigQuerySinkConfig::new(
        PROJECT_ID,
        DATASET_ID,
        TABLE_ID,
        BigQueryCredentials::ApplicationDefault,
    )
    .with_batch_size(0)
    .with_insert_id_field("event_id");
    let sink = BigQuerySink::from_parts(config, client);

    // First row has a string event_id, second a numeric one (stringified),
    // third lacks the field entirely.
    let records = vec![
        json!({"event_id": "evt-1", "v": 1}),
        json!({"event_id": 99, "v": 2}),
        json!({"v": 3}),
    ];
    let written = sink.write_batch(&records).await.expect("write_batch");
    assert_eq!(written, 3);

    let bodies = captured_insert_bodies(&server).await;
    assert_eq!(bodies.len(), 1);
    let rows = bodies[0]["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["insertId"], "evt-1");
    // A non-string field value is stringified.
    assert_eq!(rows[1]["insertId"], "99");
    // Missing field → no insertId for that row.
    assert!(rows[2].get("insertId").is_none(), "row 2: {:?}", rows[2]);
}

#[tokio::test]
async fn write_batch_collapses_insert_errors_into_single_err() {
    // The all-or-nothing `write_batch` path turns any `insertErrors` in the
    // 200 body into a single FaucetError::Sink (aborting before the bookmark
    // advances), naming the failing-row count and the first message.
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    Mock::given(method("POST"))
        .and(path(insert_path()))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "insertErrors": [
                { "index": 0, "errors": [{ "reason": "invalid", "message": "missing required field foo" }] },
                { "index": 2, "errors": [{ "reason": "invalid", "message": "bad value" }] }
            ]
        })))
        .mount(&server)
        .await;

    let (sink, _sa_file) = build_sink(&server, 0).await;
    let records: Vec<_> = (0..3).map(|i| json!({"i": i})).collect();
    match sink.write_batch(&records).await {
        Err(faucet_core::FaucetError::Sink(m)) => {
            assert!(m.contains("2 row(s) failed"), "got: {m}");
            assert!(m.contains("missing required field foo"), "got: {m}");
        }
        other => panic!("expected Sink error, got: {other:?}"),
    }
}

#[tokio::test]
async fn write_batch_http_failure_bubbles_outer_err() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    Mock::given(method("POST"))
        .and(path(insert_path()))
        .respond_with(ResponseTemplate::new(503).set_body_string("unavailable"))
        .mount(&server)
        .await;

    let (sink, _sa_file) = build_sink(&server, 0).await;
    match sink.write_batch(&[json!({"a": 1})]).await {
        Err(faucet_core::FaucetError::Sink(m)) => {
            assert!(m.contains("insertAll failed"), "got: {m}");
        }
        other => panic!("expected Sink error, got: {other:?}"),
    }
}

#[tokio::test]
async fn connector_name_schema_and_dataset_uri_are_exposed() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    let (sink, _sa_file) = build_sink(&server, 0).await;

    assert_eq!(sink.connector_name(), "bigquery");
    assert_eq!(
        sink.dataset_uri(),
        format!("bigquery://{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
    );
    let schema = sink.config_schema();
    assert!(
        schema.get("properties").is_some() || schema.get("$ref").is_some(),
        "config_schema should be a JSON Schema object: {schema}"
    );
}

fn tables_get_path() -> String {
    format!("/projects/{PROJECT_ID}/datasets/{DATASET_ID}/tables/{TABLE_ID}")
}

#[tokio::test]
async fn check_probe_passes_on_successful_tables_get() {
    use faucet_core::check::{CheckContext, ProbeStatus};
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    // The doctor probe runs a read-only tables.get for table metadata.
    Mock::given(method("GET"))
        .and(path(tables_get_path()))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "schema": {},
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": DATASET_ID,
                "tableId": TABLE_ID
            }
        })))
        .mount(&server)
        .await;

    let (sink, _sa_file) = build_sink(&server, 0).await;
    let report = sink.check(&CheckContext::default()).await.unwrap();
    assert_eq!(report.probes.len(), 1);
    assert_eq!(report.probes[0].name, "auth");
    assert!(
        matches!(report.probes[0].status, ProbeStatus::Pass),
        "expected Pass, got: {:?}",
        report.probes[0].status
    );
}

#[tokio::test]
async fn check_probe_fails_on_tables_get_error() {
    use faucet_core::check::{CheckContext, ProbeStatus};
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    Mock::given(method("GET"))
        .and(path(tables_get_path()))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "error": {"code": 404, "message": "Not found: Table"}
        })))
        .mount(&server)
        .await;

    let (sink, _sa_file) = build_sink(&server, 0).await;
    let report = sink.check(&CheckContext::default()).await.unwrap();
    assert_eq!(report.probes.len(), 1);
    match &report.probes[0].status {
        ProbeStatus::Fail { reason } => {
            assert!(reason.contains("tables.get"), "got: {reason}");
        }
        other => panic!("expected Fail, got: {other:?}"),
    }
    assert!(
        report.probes[0].hint.is_some(),
        "a failing probe must carry a remediation hint"
    );
}

#[tokio::test]
async fn write_batch_keeps_all_or_nothing_skip_invalid_rows_false() {
    // The non-DLQ `write_batch` path stays all-or-nothing: skipInvalidRows=false
    // so a single bad row fails the whole request (no partial commit to resume
    // past, no duplicate-on-retry window).
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    Mock::given(method("POST"))
        .and(path(insert_path()))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;

    let (sink, _sa_file) = build_sink(&server, 0).await;
    let written = sink
        .write_batch(&[json!({"a": 1})])
        .await
        .expect("write_batch");
    assert_eq!(written, 1);

    let bodies = captured_insert_bodies(&server).await;
    assert_eq!(bodies.len(), 1, "expected one insertAll call");
    assert_eq!(
        bodies[0]["skipInvalidRows"],
        json!(false),
        "the non-DLQ write_batch path must stay all-or-nothing (skipInvalidRows=false)"
    );
}
