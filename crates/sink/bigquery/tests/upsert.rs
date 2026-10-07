//! Integration tests for the BigQuery upsert/delete write path (#224): the
//! in-place `MERGE … USING (SELECT … FROM UNNEST(@payload))` driven by
//! `write_batch` / `write_batch_partial` / `write_batch_idempotent`.

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
async fn sink_advertises_upsert_and_delete() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    let (sink, _sa) = build_sink(&server, config_with(|_| {})).await;
    let modes = sink.supported_write_modes();
    assert!(modes.contains(&faucet_core::WriteMode::Upsert));
    assert!(modes.contains(&faucet_core::WriteMode::Delete));
}

#[tokio::test]
async fn write_batch_upsert_posts_merge() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    mount_query_done(&server, "job-u").await;
    mount_job_done(&server, "job-u").await;

    let cfg = config_with(|c| {
        c.write.write_mode = faucet_core::WriteMode::Upsert;
        c.write.key = vec!["id".into()];
    });
    let (sink, _sa) = build_sink(&server, cfg).await;
    let n = sink
        .write_batch(&[json!({"id": 1, "name": "a"}), json!({"id": 2, "name": "b"})])
        .await
        .expect("upsert write");
    assert_eq!(n, 2);

    let bodies = captured_query_bodies(&server).await;
    let tx = bodies
        .iter()
        .find(|b| {
            b["query"]
                .as_str()
                .unwrap_or("")
                .contains("MERGE INTO `p.d.t`")
        })
        .expect("merge query");
    let q = tx["query"].as_str().unwrap();
    assert!(
        q.contains("USING (SELECT CAST(JSON_VALUE(r, '$.id') AS INT64) AS `id`"),
        "got: {q}"
    );
    assert!(q.contains("ON T.`id` = S.`id`"), "got: {q}");
    assert!(
        q.contains(
            "WHEN MATCHED THEN UPDATE SET `name` = IF(S.`__faucet_has_1`, S.`name`, T.`name`)"
        ),
        "got: {q}"
    );
    assert_eq!(tx["parameterMode"], "NAMED");
    let pnames: Vec<&str> = tx["queryParameters"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["name"].as_str().unwrap())
        .collect();
    assert!(pnames.contains(&"payload"), "got: {pnames:?}");
}

#[tokio::test]
async fn write_batch_delete_marker_routes_to_delete() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    mount_query_done(&server, "job-d").await;
    mount_job_done(&server, "job-d").await;

    let cfg = config_with(|c| {
        c.write.write_mode = faucet_core::WriteMode::Upsert;
        c.write.key = vec!["id".into()];
        c.write.delete_marker = Some(faucet_core::DeleteMarker {
            field: "__op".into(),
            values: vec!["d".into()],
        });
    });
    let (sink, _sa) = build_sink(&server, cfg).await;
    // One upsert (id=1), one delete (id=2 marked __op=d).
    let n = sink
        .write_batch(&[
            json!({"id": 1, "name": "a", "__op": "u"}),
            json!({"id": 2, "__op": "d"}),
        ])
        .await
        .expect("upsert+delete write");
    assert_eq!(n, 2);

    let bodies = captured_query_bodies(&server).await;
    let tx = bodies
        .iter()
        .find(|b| {
            b["query"]
                .as_str()
                .unwrap_or("")
                .contains("BEGIN TRANSACTION")
        })
        .expect("tx");
    let q = tx["query"].as_str().unwrap();
    assert!(q.contains("MERGE INTO `p.d.t`"), "got: {q}");
    assert!(q.contains("DELETE FROM `p.d.t` T WHERE EXISTS"), "got: {q}");
    let pnames: Vec<&str> = tx["queryParameters"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["name"].as_str().unwrap())
        .collect();
    assert!(
        pnames.contains(&"payload") && pnames.contains(&"deletes"),
        "got: {pnames:?}"
    );
}

#[tokio::test]
async fn write_batch_upsert_missing_key_errors() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    mount_query_done(&server, "job-x").await;
    mount_job_done(&server, "job-x").await;

    let cfg = config_with(|c| {
        c.write.write_mode = faucet_core::WriteMode::Upsert;
        c.write.key = vec!["id".into()];
    });
    let (sink, _sa) = build_sink(&server, cfg).await;
    // Second row has no `id` → planner routes it to `failed` → write_batch errors.
    let err = sink
        .write_batch(&[json!({"id": 1, "name": "a"}), json!({"name": "no key"})])
        .await
        .unwrap_err();
    assert!(format!("{err}").contains("bigquery upsert"), "got: {err}");
}

#[tokio::test]
async fn write_batch_idempotent_upsert_composes_merge_and_watermark() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    // CREATE commit-table + the transaction both resolve to job-eo.
    mount_query_done(&server, "job-eo").await;
    mount_job_done(&server, "job-eo").await;

    let cfg = config_with(|c| {
        c.write.write_mode = faucet_core::WriteMode::Upsert;
        c.write.key = vec!["id".into()];
    });
    let (sink, _sa) = build_sink(&server, cfg).await;
    let n = sink
        .write_batch_idempotent(
            &[json!({"id": 1, "name": "a"})],
            "pipe::row1",
            "00000000000000000005",
        )
        .await
        .expect("eo upsert");
    assert_eq!(n, 1);

    let bodies = captured_query_bodies(&server).await;
    let tx = bodies
        .iter()
        .find(|b| {
            let q = b["query"].as_str().unwrap_or("");
            q.contains("MERGE INTO `p.d.t`") && q.contains("MERGE `p.d._faucet_commit_token`")
        })
        .expect("transaction with data MERGE + watermark MERGE");
    let q = tx["query"].as_str().unwrap();
    let data = q.find("MERGE INTO `p.d.t`").unwrap();
    let wm = q.find("MERGE `p.d._faucet_commit_token`").unwrap();
    assert!(data < wm, "data merge must precede watermark: {q}");
    let pnames: Vec<&str> = tx["queryParameters"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["name"].as_str().unwrap())
        .collect();
    assert!(
        pnames.contains(&"payload") && pnames.contains(&"scope") && pnames.contains(&"token"),
        "got: {pnames:?}"
    );
}

#[tokio::test]
async fn write_batch_partial_upsert_routes_missing_key_to_dlq() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    mount_query_done(&server, "job-p").await;
    mount_job_done(&server, "job-p").await;

    let cfg = config_with(|c| {
        c.write.write_mode = faucet_core::WriteMode::Upsert;
        c.write.key = vec!["id".into()];
    });
    let (sink, _sa) = build_sink(&server, cfg).await;
    // Row 0 good, row 1 missing key → row 1 must come back Err, row 0 Ok.
    let outcomes = sink
        .write_batch_partial(&[json!({"id": 1, "name": "a"}), json!({"name": "no key"})])
        .await
        .expect("partial write");
    assert_eq!(outcomes.len(), 2);
    assert!(outcomes[0].is_ok(), "row 0 should be Ok");
    assert!(outcomes[1].is_err(), "row 1 (missing key) should be Err");
}

#[tokio::test]
async fn write_batch_delete_only_posts_delete_without_merge() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    mount_query_done(&server, "job-del").await;
    mount_job_done(&server, "job-del").await;

    // write_mode: delete routes every keyed row to a DELETE — no upserts, so
    // the transaction must bind only @deletes and emit no data MERGE.
    let cfg = config_with(|c| {
        c.write.write_mode = faucet_core::WriteMode::Delete;
        c.write.key = vec!["id".into()];
    });
    let (sink, _sa) = build_sink(&server, cfg).await;
    let n = sink
        .write_batch(&[json!({"id": 1}), json!({"id": 2})])
        .await
        .expect("delete write");
    assert_eq!(n, 2);

    let bodies = captured_query_bodies(&server).await;
    let tx = bodies
        .iter()
        .find(|b| {
            b["query"]
                .as_str()
                .unwrap_or("")
                .contains("DELETE FROM `p.d.t`")
        })
        .expect("delete query");
    let q = tx["query"].as_str().unwrap();
    assert!(q.contains("DELETE FROM `p.d.t` T WHERE EXISTS"), "got: {q}");
    assert!(
        !q.contains("MERGE INTO `p.d.t`"),
        "delete-only must not emit a data MERGE: {q}"
    );
    let pnames: Vec<&str> = tx["queryParameters"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["name"].as_str().unwrap())
        .collect();
    assert!(pnames.contains(&"deletes"), "got: {pnames:?}");
    assert!(
        !pnames.contains(&"payload"),
        "delete-only must not bind @payload: {pnames:?}"
    );
}

#[tokio::test]
async fn write_batch_idempotent_zero_rows_posts_watermark_only() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    mount_query_done(&server, "job-wm").await;
    mount_job_done(&server, "job-wm").await;

    let cfg = config_with(|c| {
        c.write.write_mode = faucet_core::WriteMode::Upsert;
        c.write.key = vec!["id".into()];
    });
    let (sink, _sa) = build_sink(&server, cfg).await;
    // An empty exactly-once page must still advance the watermark: the
    // transaction posts the token MERGE alone, with no data MERGE/DELETE.
    let n = sink
        .write_batch_idempotent(&[], "pipe::row1", "00000000000000000009")
        .await
        .expect("zero-row eo upsert");
    assert_eq!(n, 0);

    let bodies = captured_query_bodies(&server).await;
    let tx = bodies
        .iter()
        .find(|b| {
            b["query"]
                .as_str()
                .unwrap_or("")
                .contains("MERGE `p.d._faucet_commit_token`")
        })
        .expect("watermark merge query");
    let q = tx["query"].as_str().unwrap();
    assert!(
        !q.contains("MERGE INTO `p.d.t`"),
        "zero-row page must not emit a data MERGE: {q}"
    );
    assert!(
        !q.contains("DELETE FROM `p.d.t`"),
        "zero-row page must not emit a DELETE: {q}"
    );
    let pnames: Vec<&str> = tx["queryParameters"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["name"].as_str().unwrap())
        .collect();
    assert!(
        pnames.contains(&"scope") && pnames.contains(&"token"),
        "got: {pnames:?}"
    );
    assert!(
        !pnames.contains(&"payload") && !pnames.contains(&"deletes"),
        "no data params for a zero-row page: {pnames:?}"
    );
}

/// A MERGE that outlives `job_timeout` is cancelled before the write fails,
/// so it cannot commit after the run was reported failed (SQL-96).
#[tokio::test]
async fn a_job_past_job_timeout_is_cancelled() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    let running = json!({
        "kind": "bigquery#queryResponse",
        "jobComplete": false,
        "jobReference": {"projectId": PROJECT_ID, "jobId": "job-slow"}
    });
    Mock::given(method("POST"))
        .and(path(queries_path()))
        .respond_with(ResponseTemplate::new(200).set_body_json(running.clone()))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/projects/{PROJECT_ID}/queries/job-slow")))
        .respond_with(ResponseTemplate::new(200).set_body_json(running))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(format!("/projects/{PROJECT_ID}/jobs/job-slow/cancel")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "kind": "bigquery#jobCancelResponse",
            "job": {"jobReference": {"projectId": PROJECT_ID, "jobId": "job-slow"}}
        })))
        .expect(1)
        .mount(&server)
        .await;

    let cfg = config_with(|c| {
        c.write.write_mode = faucet_core::WriteMode::Upsert;
        c.write.key = vec!["id".into()];
        c.job_timeout = std::time::Duration::from_millis(300);
    });
    let (sink, _sa) = build_sink(&server, cfg).await;
    let err = sink
        .write_batch(&[json!({"id": 1, "name": "a"})])
        .await
        .expect_err("a job past job_timeout must fail the write");
    let msg = err.to_string();
    assert!(
        msg.contains("job_timeout") && msg.contains("cancelled"),
        "{msg}"
    );
}

/// A transaction BigQuery aborts for a concurrent update on the same table is
/// re-submitted (it committed nothing) with a fresh `requestId`, so a second
/// writer in the dataset does not fail the run (SQL-95).
#[tokio::test]
async fn a_transaction_aborted_by_a_concurrent_update_is_retried() {
    use wiremock::matchers::body_string_contains;
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    Mock::given(method("POST"))
        .and(path(queries_path()))
        .and(body_string_contains("MERGE INTO"))
        .respond_with(ResponseTemplate::new(200).set_body_json(done_query("job-a")))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(queries_path()))
        .and(body_string_contains("MERGE INTO"))
        .respond_with(ResponseTemplate::new(200).set_body_json(done_query("job-b")))
        .mount(&server)
        .await;
    mount_query_done(&server, "job-eo").await;
    Mock::given(method("GET"))
        .and(path(format!("/projects/{PROJECT_ID}/jobs/job-a")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobReference": {"projectId": PROJECT_ID, "jobId": "job-a"},
            "status": {"state": "DONE", "errorResult": {
                "reason": "invalidQuery",
                "message": "Transaction is aborted due to concurrent update against table p:d.t."
            }}
        })))
        .mount(&server)
        .await;
    mount_job_done(&server, "job-b").await;
    mount_job_done(&server, "job-eo").await;

    let cfg = config_with(|c| {
        c.write.write_mode = faucet_core::WriteMode::Upsert;
        c.write.key = vec!["id".into()];
    });
    let (sink, _sa) = build_sink(&server, cfg).await;
    let n = sink
        .write_batch_idempotent(
            &[json!({"id": 1, "name": "a"})],
            "pipe::row1",
            "00000000000000000009",
        )
        .await
        .expect("the conflicting transaction is retried");
    assert_eq!(n, 1);

    let ids: Vec<String> = captured_query_bodies(&server)
        .await
        .into_iter()
        .filter(|b| b["query"].as_str().unwrap_or("").contains("MERGE INTO"))
        .map(|b| b["requestId"].as_str().unwrap_or_default().to_string())
        .collect();
    assert_eq!(ids.len(), 2, "first attempt aborted, second committed");
    assert_ne!(ids[0], ids[1], "the retry needs a fresh requestId");
    assert_eq!(ids[1], format!("{}-r1", ids[0]));
}

/// Any other job failure is not retried.
#[tokio::test]
async fn a_failed_transaction_that_was_not_a_conflict_is_not_retried() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    mount_query_done(&server, "job-x").await;
    Mock::given(method("GET"))
        .and(path(format!("/projects/{PROJECT_ID}/jobs/job-x")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobReference": {"projectId": PROJECT_ID, "jobId": "job-x"},
            "status": {"state": "DONE", "errorResult": {
                "reason": "invalidQuery", "message": "Bad cast"
            }}
        })))
        .mount(&server)
        .await;
    let cfg = config_with(|c| {
        c.write.write_mode = faucet_core::WriteMode::Upsert;
        c.write.key = vec!["id".into()];
    });
    let (sink, _sa) = build_sink(&server, cfg).await;
    let err = sink
        .write_batch(&[json!({"id": 1, "name": "a"})])
        .await
        .expect_err("a runtime failure fails the write");
    assert!(err.to_string().contains("Bad cast"), "{err}");
    assert_eq!(captured_query_bodies(&server).await.len(), 1);
}

/// A record field the table has no column for fails the write instead of
/// being projected away by the MERGE (SQL-94).
#[tokio::test]
async fn a_field_missing_from_the_table_fails_instead_of_vanishing() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    mount_query_done(&server, "job-u").await;
    mount_job_done(&server, "job-u").await;
    let cfg = config_with(|c| {
        c.write.write_mode = faucet_core::WriteMode::Upsert;
        c.write.key = vec!["id".into()];
    });
    let (sink, _sa) = build_sink(&server, cfg).await;
    let err = sink
        .write_batch(&[json!({"id": 1, "name": "a", "nickname": "x"})])
        .await
        .expect_err("an unknown field must not be dropped");
    let msg = err.to_string();
    assert!(
        msg.contains("`nickname`") && msg.contains("on_drift"),
        "{msg}"
    );
    assert!(
        captured_query_bodies(&server)
            .await
            .iter()
            .all(|b| !b["query"].as_str().unwrap_or("").contains("MERGE INTO")),
        "nothing is written"
    );
}
