//! Integration tests for the BigQuery `write_mode: overwrite` lifecycle (#492):
//! bucket-free staging via the query API + the atomic `TRUNCATE`+`INSERT … SELECT`
//! swap. Driven against a wiremock BigQuery so the DDL/query bodies can be
//! asserted without a real project.

use faucet_core::{OverwriteScope, Sink, WriteMode};
use faucet_sink_bigquery::{BigQueryCredentials, BigQuerySink, BigQuerySinkConfig};
use gcp_bigquery_client::client_builder::ClientBuilder;
use serde::Serialize;
use serde_json::json;
use wiremock::matchers::{method, path, query_param};
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

fn config_overwrite() -> BigQuerySinkConfig {
    let mut c = BigQuerySinkConfig::new(
        PROJECT_ID,
        DATASET_ID,
        TABLE_ID,
        BigQueryCredentials::ApplicationDefault,
    );
    c.write.write_mode = WriteMode::Overwrite;
    // Since #612 the bulk load path is the default. The tests below assert the
    // **`jobs.query` staging path's** mechanics (LIKE clone, INSERT … SELECT
    // swap, insertAll), so they pin it; the load-path tests opt back in with
    // `config.media_load = true`.
    c.media_load = false;
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

async fn mount_table_schema(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path(format!(
            "/projects/{PROJECT_ID}/datasets/{DATASET_ID}/tables/{TABLE_ID}"
        )))
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

/// `tables.get` returns 404 (table not found) — the shape the BigQuery client
/// maps to a not-found error. `up_to` bounds how many times it answers before a
/// later, higher-numbered-priority mock (e.g. a 200 schema) takes over.
async fn mount_table_missing(server: &MockServer, up_to: Option<u64>) {
    let mut m = Mock::given(method("GET"))
        .and(path(format!(
            "/projects/{PROJECT_ID}/datasets/{DATASET_ID}/tables/{TABLE_ID}"
        )))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "error": {"code": 404, "message": "Not found: Table",
                      "errors": [{"reason": "notFound", "message": "Not found: Table"}]}
        })))
        .with_priority(1);
    if let Some(n) = up_to {
        m = m.up_to_n_times(n);
    }
    m.mount(server).await;
}

/// `tables.get` on the overwrite staging table returns 200 — the shape
/// `commit_overwrite`'s existence probe expects when a page was staged. Keyed on
/// the `<table>__faucet_ovw` path so it never collides with the target mocks.
async fn mount_staging_present(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path(format!(
            "/projects/{PROJECT_ID}/datasets/{DATASET_ID}/tables/{TABLE_ID}__faucet_ovw"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tableReference": {"projectId": PROJECT_ID, "datasetId": DATASET_ID,
                               "tableId": format!("{TABLE_ID}__faucet_ovw")},
            "schema": {"fields": [
                {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
                {"name": "name", "type": "STRING", "mode": "NULLABLE"}
            ]}
        })))
        .mount(server)
        .await;
}

async fn mount_query_and_job(server: &MockServer, job_id: &str) {
    Mock::given(method("POST"))
        .and(path(format!("/projects/{PROJECT_ID}/queries")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "kind": "bigquery#queryResponse",
            "jobComplete": true,
            "jobReference": {"projectId": PROJECT_ID, "jobId": job_id}
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/projects/{PROJECT_ID}/jobs/{job_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobReference": {"projectId": PROJECT_ID, "jobId": job_id},
            "status": {"state": "DONE"}
        })))
        .mount(server)
        .await;
}

async fn queries(server: &MockServer) -> Vec<String> {
    server
        .received_requests()
        .await
        .expect("recording enabled")
        .into_iter()
        .filter(|r| r.url.path().ends_with("/queries"))
        .filter_map(|r| {
            serde_json::from_slice::<serde_json::Value>(&r.body)
                .ok()
                .and_then(|b| b["query"].as_str().map(str::to_string))
        })
        .collect()
}

#[tokio::test]
async fn overwrite_advertised_and_flagged() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    let (sink, _sa) = build_sink(&server, config_overwrite()).await;
    assert!(sink.is_overwrite());
    assert!(sink.supported_write_modes().contains(&WriteMode::Overwrite));
}

#[tokio::test]
async fn overwrite_lifecycle_posts_bucket_free_swap() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    mount_query_and_job(&server, "job-o").await;
    mount_staging_present(&server).await;

    let (sink, _sa) = build_sink(&server, config_overwrite()).await;

    // begin → CREATE OR REPLACE TABLE temp LIKE target
    sink.begin_overwrite().await.expect("begin");
    // write → the page is loaded into the staging table via the query API
    let n = sink
        .write_batch(&[json!({"id": 1, "name": "a"}), json!({"id": 2, "name": "b"})])
        .await
        .expect("write");
    assert_eq!(n, 2);
    // also exercise the partial path (overwrite is insert-shaped, all Ok)
    let outcomes = sink
        .write_batch_partial(&[json!({"id": 3, "name": "c"})])
        .await
        .expect("partial");
    assert_eq!(outcomes.len(), 1);
    assert!(outcomes[0].is_ok());
    // commit → transactional TRUNCATE + INSERT … SELECT swap, then DROP temp
    sink.commit_overwrite().await.expect("commit");

    let qs = queries(&server).await;
    assert!(
        qs.iter()
            .any(|q| q.contains("CREATE OR REPLACE TABLE `p.d.t__faucet_ovw` LIKE `p.d.t`")),
        "begin DDL missing: {qs:?}"
    );
    assert!(
        qs.iter()
            .any(|q| q.contains("INSERT INTO `p.d.t__faucet_ovw`")),
        "staging load missing: {qs:?}"
    );
    assert!(
        qs.iter().any(|q| q.contains("BEGIN TRANSACTION")
            && q.contains("TRUNCATE TABLE `p.d.t`")
            && q.contains("INSERT INTO `p.d.t` SELECT * FROM `p.d.t__faucet_ovw`")),
        "commit swap missing: {qs:?}"
    );
    assert!(
        qs.iter()
            .any(|q| q.contains("DROP TABLE IF EXISTS `p.d.t__faucet_ovw`")),
        "drop temp missing: {qs:?}"
    );
}

#[tokio::test]
async fn scoped_overwrite_commit_deletes_in_window_not_truncate() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    mount_query_and_job(&server, "job-s").await;
    mount_staging_present(&server).await;

    let mut config = config_overwrite();
    config.scope = Some(OverwriteScope::Window {
        column: "posting_date".into(),
        from: json!("2024-06-01"),
        to: json!("2024-07-01"),
    });
    let (sink, _sa) = build_sink(&server, config).await;

    sink.begin_overwrite().await.expect("begin");
    sink.write_batch(&[json!({"id": 1, "name": "a"})])
        .await
        .expect("write");
    sink.commit_overwrite().await.expect("commit");

    let qs = queries(&server).await;
    assert!(
        qs.iter().any(|q| q.contains("BEGIN TRANSACTION")
            && q.contains("DELETE FROM `p.d.t` WHERE `posting_date` >= '2024-06-01' AND `posting_date` < '2024-07-01'")
            && q.contains("INSERT INTO `p.d.t` SELECT * FROM `p.d.t__faucet_ovw`")),
        "scoped commit swap missing: {qs:?}"
    );
    assert!(
        !qs.iter().any(|q| q.contains("TRUNCATE TABLE `p.d.t`")),
        "scoped overwrite must not truncate: {qs:?}"
    );
}

#[tokio::test]
async fn overwrite_abort_drops_staging_without_swap() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    // `begin_overwrite` now probes table existence first; the target exists, so
    // it clones staging via `LIKE` and abort drops that staging.
    mount_table_schema(&server).await;
    mount_query_and_job(&server, "job-a").await;

    let (sink, _sa) = build_sink(&server, config_overwrite()).await;
    sink.begin_overwrite().await.expect("begin");
    sink.abort_overwrite().await.expect("abort");

    let qs = queries(&server).await;
    assert!(
        qs.iter()
            .any(|q| q.contains("DROP TABLE IF EXISTS `p.d.t__faucet_ovw`")),
        "abort must drop staging: {qs:?}"
    );
    assert!(
        !qs.iter().any(|q| q.contains("TRUNCATE TABLE")),
        "abort must not swap into the target: {qs:?}"
    );
}

#[tokio::test]
async fn overwrite_staging_insert_failure_surfaces_error() {
    // A rejected staging-page load must surface as an error, not silently drop
    // the page — otherwise the commit swap would replace the destination with an
    // incomplete dataset. Covers the `insert_overwrite_page` query-failure path.
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    Mock::given(method("POST"))
        .and(path(format!("/projects/{PROJECT_ID}/queries")))
        .respond_with(ResponseTemplate::new(500).set_body_json(json!({
            "error": {"code": 500, "message": "backend error"}
        })))
        .mount(&server)
        .await;

    let (sink, _sa) = build_sink(&server, config_overwrite()).await;
    let err = sink
        .write_batch(&[json!({"id": 1, "name": "a"})])
        .await
        .expect_err("a failed staging load must surface as an error");
    assert!(
        err.to_string().contains("overwrite page insert"),
        "error should name the overwrite page insert: {err}"
    );
}

#[tokio::test]
async fn overwrite_creates_dataset_table_and_staging_when_missing() {
    // create_table defaults to true: a first-ever overwrite sync must create the
    // dataset + target table (schema inferred from the first page) and the
    // staging clone, then swap — instead of 404ing on the missing target.
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    // tables.get on the target returns 404 for the two existence probes
    // (begin_overwrite + the write path's self-heal), then 200 once the write
    // path has created it (target_schema re-reads it to build the staging load).
    mount_table_missing(&server, Some(2)).await;
    mount_table_schema(&server).await;
    mount_query_and_job(&server, "job-c").await;
    mount_staging_present(&server).await;

    let (sink, _sa) = build_sink(&server, config_overwrite()).await;

    sink.begin_overwrite().await.expect("begin");
    let n = sink
        .write_batch(&[json!({"id": 1, "name": "a"}), json!({"id": 2, "name": "b"})])
        .await
        .expect("write");
    assert_eq!(n, 2);
    sink.commit_overwrite().await.expect("commit");

    let qs = queries(&server).await;
    assert!(
        qs.iter()
            .any(|q| q.contains("CREATE SCHEMA IF NOT EXISTS `p`.`d`")),
        "dataset create missing: {qs:?}"
    );
    assert!(
        qs.iter()
            .any(|q| q.starts_with("CREATE OR REPLACE TABLE `p.d.t` (")
                && q.contains("`id` INT64")
                && q.contains("`name` STRING")),
        "target table create missing: {qs:?}"
    );
    assert!(
        qs.iter()
            .any(|q| q.contains("CREATE OR REPLACE TABLE `p.d.t__faucet_ovw` LIKE `p.d.t`")),
        "staging clone missing: {qs:?}"
    );
    assert!(
        qs.iter()
            .any(|q| q.contains("INSERT INTO `p.d.t__faucet_ovw`")),
        "staging load missing: {qs:?}"
    );
    assert!(
        qs.iter().any(|q| q.contains("TRUNCATE TABLE `p.d.t`")
            && q.contains("INSERT INTO `p.d.t` SELECT * FROM `p.d.t__faucet_ovw`")),
        "commit swap missing: {qs:?}"
    );
}

#[tokio::test]
async fn overwrite_creates_missing_target_across_sink_instances() {
    // Regression: the executor drives begin_overwrite on a short-lived throwaway
    // sink, the page write on the invocation's sink, and commit_overwrite on a
    // third sink — three *different* instances. A missing target must still be
    // created and swapped, because the write path self-heals off the real BQ
    // objects rather than an in-memory flag set on the (dropped) begin sink.
    // Before the fix this 404'd at `tables.get (schema)` on the never-created
    // target.
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    // 404 for begin's probe + the write sink's self-heal probe, then 200 once
    // the write sink has created the target.
    mount_table_missing(&server, Some(2)).await;
    mount_table_schema(&server).await;
    mount_query_and_job(&server, "job-xi").await;
    mount_staging_present(&server).await;

    let config = config_overwrite();
    // Three independent sinks over the same mock backend — the crux of the bug.
    let (begin_sink, _b) = build_sink(&server, config.clone()).await;
    let (write_sink, _w) = build_sink(&server, config.clone()).await;
    let (commit_sink, _c) = build_sink(&server, config).await;

    begin_sink.begin_overwrite().await.expect("begin");
    let n = write_sink
        .write_batch(&[json!({"id": 1, "name": "a"})])
        .await
        .expect("write must self-heal the missing target, not 404");
    assert_eq!(n, 1);
    commit_sink.commit_overwrite().await.expect("commit");

    let qs = queries(&server).await;
    assert!(
        qs.iter()
            .any(|q| q.contains("CREATE SCHEMA IF NOT EXISTS `p`.`d`")),
        "dataset create missing: {qs:?}"
    );
    assert!(
        qs.iter()
            .any(|q| q.starts_with("CREATE OR REPLACE TABLE `p.d.t` (")),
        "write sink must create the missing target: {qs:?}"
    );
    assert!(
        qs.iter()
            .any(|q| q.contains("CREATE OR REPLACE TABLE `p.d.t__faucet_ovw` LIKE `p.d.t`")),
        "write sink must create the staging clone: {qs:?}"
    );
    assert!(
        qs.iter().any(|q| q.contains("TRUNCATE TABLE `p.d.t`")
            && q.contains("INSERT INTO `p.d.t` SELECT * FROM `p.d.t__faucet_ovw`")),
        "commit swap missing: {qs:?}"
    );
}

#[tokio::test]
async fn overwrite_begin_errors_when_target_missing_and_create_disabled() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_missing(&server, None).await;

    let mut config = config_overwrite();
    config.create_table = false;
    let (sink, _sa) = build_sink(&server, config).await;

    let err = sink
        .begin_overwrite()
        .await
        .expect_err("missing target + create_table disabled must error");
    assert!(
        err.to_string().contains("create_table` is disabled"),
        "error should name the disabled create_table: {err}"
    );
}

#[tokio::test]
async fn append_creates_table_when_missing_by_default() {
    // The append path also honors create_table: a missing table is created from
    // the first page's inferred schema before the streaming insert.
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_missing(&server, None).await;
    mount_query_and_job(&server, "job-ap").await;
    Mock::given(method("POST"))
        .and(path(format!(
            "/projects/{PROJECT_ID}/datasets/{DATASET_ID}/tables/{TABLE_ID}/insertAll"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;

    // Append mode, `create_table` defaults true. `media_load` is pinned off so
    // this exercises the `insertAll` write; the load-path counterpart is
    // `append_creates_table_when_missing_under_the_default_load_path`.
    let config = BigQuerySinkConfig::new(
        PROJECT_ID,
        DATASET_ID,
        TABLE_ID,
        BigQueryCredentials::ApplicationDefault,
    )
    .with_media_load(false);
    let (sink, _sa) = build_sink(&server, config).await;

    let n = sink
        .write_batch(&[json!({"id": 1, "name": "a"})])
        .await
        .expect("write");
    assert_eq!(n, 1);

    let qs = queries(&server).await;
    assert!(
        qs.iter()
            .any(|q| q.starts_with("CREATE OR REPLACE TABLE `p.d.t` (")),
        "append path must create the missing table: {qs:?}"
    );
}

#[tokio::test]
async fn overwrite_recreates_schemaless_target() {
    // A table created by a bare `bq mk` (no schema) exists but has no fields, so
    // `CREATE … LIKE` / typed inserts fail against it. create_table (default on)
    // must (re)create it from the first page's schema, then swap.
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    // schema probe: empty-schema 200 for the two existence checks (begin_overwrite
    // + the write path's self-heal), then a real schema once the table has been
    // (re)created.
    Mock::given(method("GET"))
        .and(path(format!(
            "/projects/{PROJECT_ID}/datasets/{DATASET_ID}/tables/{TABLE_ID}"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tableReference": {"projectId": PROJECT_ID, "datasetId": DATASET_ID, "tableId": TABLE_ID},
            "schema": {"fields": []}
        })))
        .up_to_n_times(2)
        .with_priority(1)
        .mount(&server)
        .await;
    mount_table_schema(&server).await;
    mount_query_and_job(&server, "job-sl").await;
    mount_staging_present(&server).await;

    let (sink, _sa) = build_sink(&server, config_overwrite()).await;
    sink.begin_overwrite().await.expect("begin");
    let n = sink
        .write_batch(&[json!({"id": 1, "name": "a"})])
        .await
        .expect("write");
    assert_eq!(n, 1);
    sink.commit_overwrite().await.expect("commit");

    let qs = queries(&server).await;
    assert!(
        qs.iter()
            .any(|q| q.starts_with("CREATE OR REPLACE TABLE `p.d.t` (")),
        "schemaless target must be re-created: {qs:?}"
    );
    assert!(
        qs.iter().any(|q| q.contains("TRUNCATE TABLE `p.d.t`")),
        "commit swap missing: {qs:?}"
    );
}

#[tokio::test]
async fn create_table_surfaces_non_404_schema_probe_error() {
    // A non-404 tables.get error (e.g. 500) during the readiness probe must
    // surface, not be treated as "missing" (#578).
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    Mock::given(method("GET"))
        .and(path(format!(
            "/projects/{PROJECT_ID}/datasets/{DATASET_ID}/tables/{TABLE_ID}"
        )))
        .respond_with(ResponseTemplate::new(500).set_body_json(json!({
            "error": {"code": 500, "message": "backend error"}
        })))
        .mount(&server)
        .await;

    let config = BigQuerySinkConfig::new(
        PROJECT_ID,
        DATASET_ID,
        TABLE_ID,
        BigQueryCredentials::ApplicationDefault,
    );
    let (sink, _sa) = build_sink(&server, config).await;
    let err = sink
        .write_batch(&[json!({"id": 1})])
        .await
        .expect_err("a non-404 probe error must surface");
    assert!(
        err.to_string().contains("schema probe"),
        "error should name the schema probe: {err}"
    );
}

#[tokio::test]
async fn create_table_errors_when_schema_uninferable() {
    // create_table on + missing table + a page with no object fields → the
    // schema can't be inferred, so it errors rather than creating an empty table.
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_missing(&server, None).await;

    let config = BigQuerySinkConfig::new(
        PROJECT_ID,
        DATASET_ID,
        TABLE_ID,
        BigQueryCredentials::ApplicationDefault,
    );
    let (sink, _sa) = build_sink(&server, config).await;
    let err = sink
        .write_batch(&[json!(1), json!("x")])
        .await
        .expect_err("uninferable schema must error");
    assert!(
        err.to_string().contains("cannot infer a schema"),
        "error should explain the schema could not be inferred: {err}"
    );
}

#[tokio::test]
async fn append_errors_when_missing_and_create_disabled() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_missing(&server, None).await;

    let config = BigQuerySinkConfig::new(
        PROJECT_ID,
        DATASET_ID,
        TABLE_ID,
        BigQueryCredentials::ApplicationDefault,
    )
    .with_create_table(false);
    let (sink, _sa) = build_sink(&server, config).await;

    let err = sink
        .write_batch(&[json!({"id": 1, "name": "a"})])
        .await
        .expect_err("missing table + create_table disabled must error");
    assert!(
        err.to_string().contains("create_table` is disabled"),
        "error should name the disabled create_table: {err}"
    );
}

/// Direct (solo) media-load overwrite: `begin_overwrite` and `commit_overwrite`
/// must issue NO staging DDL (no `CREATE … LIKE`, no `TRUNCATE`/`INSERT … SELECT`
/// swap) — the page is loaded straight into the target via a WRITE_TRUNCATE load
/// job, so there is nothing to stage or swap. `create_table` is off so `begin`
/// does no dataset HTTP at all; the assertion is simply "zero query jobs".
#[tokio::test]
async fn direct_overwrite_begin_and_commit_skip_staging() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    let mut config = config_overwrite();
    config.media_load = true;
    config.create_table = false; // solo, media load, no `_overwrite_staging`
    let (sink, _sa) = build_sink(&server, config).await;

    sink.begin_overwrite()
        .await
        .expect("direct begin is a no-op");
    sink.commit_overwrite()
        .await
        .expect("direct commit is a no-op");
    sink.abort_overwrite()
        .await
        .expect("direct abort is a no-op");

    let qs = queries(&server).await;
    assert!(
        qs.is_empty(),
        "direct overwrite must issue no staging/swap DDL, got: {qs:?}"
    );
}

/// Grouped overwrite (executor injects `_overwrite_staging`): even with
/// `media_load`, `begin_overwrite` must still create the staging clone — the
/// independent writer instances of a fan-out can only coordinate through a shared
/// staging table, so the direct WRITE_TRUNCATE path is off.
#[tokio::test]
async fn grouped_overwrite_still_stages_under_media_load() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    mount_query_and_job(&server, "job-create-staging").await;

    let mut config = config_overwrite();
    config.media_load = true;
    config.overwrite_staging = true; // what the executor sets for a >1 fan-out
    let (sink, _sa) = build_sink(&server, config).await;

    sink.begin_overwrite().await.expect("grouped begin stages");

    let qs = queries(&server).await;
    assert!(
        qs.iter().any(|q| q.contains("CREATE OR REPLACE TABLE")
            && q.contains(&format!("{TABLE_ID}__faucet_ovw"))),
        "grouped overwrite must create the staging clone, got: {qs:?}"
    );
}

// ── Streaming resumable-upload load (media_load) ────────────────────────────

/// Mount the resumable-upload trio against the mock: initiate (200 + a
/// `Location` header pointing back at the mock), the finalize PUT (200 + a DONE
/// `Job`), and the `jobs.get` poll (DONE). Small test payloads stay under the
/// 8-MiB chunk threshold, so the whole stream is one finalize PUT.
async fn mount_resumable(server: &MockServer, session_path: &str, job_id: &str) {
    let session_uri = format!("{}{session_path}", server.uri());
    Mock::given(method("POST"))
        .and(path(format!(
            "/upload/bigquery/v2/projects/{PROJECT_ID}/jobs"
        )))
        .and(query_param("uploadType", "resumable"))
        .respond_with(ResponseTemplate::new(200).insert_header("location", session_uri.as_str()))
        .mount(server)
        .await;
    Mock::given(method("PUT"))
        .and(path(session_path.to_string()))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobReference": {"projectId": PROJECT_ID, "jobId": job_id},
            "status": {"state": "DONE"}
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/projects/{PROJECT_ID}/jobs/{job_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobReference": {"projectId": PROJECT_ID, "jobId": job_id},
            "status": {"state": "DONE"}
        })))
        .mount(server)
        .await;
}

/// Point `config.auth` at the mock's OAuth token endpoint so the streaming
/// upload's `access_token()` mint succeeds (the `media_load` upload path
/// authenticates itself, separately from the `gcp_bigquery_client` client).
fn with_sa_auth(mut config: BigQuerySinkConfig, server: &MockServer) -> BigQuerySinkConfig {
    config.auth = BigQueryCredentials::ServiceAccountKey {
        json: serde_json::to_string(&dummy_service_account_json(&server.uri())).unwrap(),
    };
    config
}

/// The gzipped bodies of every PUT to the resumable session URI.
async fn session_puts(server: &MockServer, session_path: &str) -> Vec<Vec<u8>> {
    server
        .received_requests()
        .await
        .expect("recording enabled")
        .into_iter()
        .filter(|r| r.method.as_str() == "PUT" && r.url.path() == session_path)
        .map(|r| r.body.clone())
        .collect()
}

fn gunzip(bytes: &[u8]) -> String {
    use std::io::Read;
    let mut d = flate2::read::GzDecoder::new(bytes);
    let mut s = String::new();
    d.read_to_string(&mut s).expect("gunzip");
    s
}

/// Solo direct overwrite streams **all** pages into ONE `WRITE_TRUNCATE`
/// resumable load, finalized on `flush` (not per page): two pages ⇒ one finalize
/// PUT whose gunzipped body carries both rows' NDJSON, and `commit_overwrite` is
/// a no-op.
#[tokio::test]
async fn direct_overwrite_streams_one_load_across_pages() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await; // target already exists (no create)
    mount_resumable(&server, "/resumable/ovw-1", "load-ovw-1").await;

    let mut config = config_overwrite();
    config.media_load = true;
    config.create_table = false;
    config.upload_base_url = Some(server.uri());
    let config = with_sa_auth(config, &server);
    let (sink, _sa) = build_sink(&server, config).await;

    sink.begin_overwrite().await.expect("direct begin");
    sink.write_batch(&[json!({"id": 1, "name": "a"})])
        .await
        .expect("page 1");
    // No upload has been finalized yet — deferred to flush.
    assert!(
        session_puts(&server, "/resumable/ovw-1").await.is_empty(),
        "no finalize PUT before flush"
    );
    sink.write_batch(&[json!({"id": 2, "name": "b"})])
        .await
        .expect("page 2");
    sink.flush().await.expect("flush finalizes the load");
    sink.commit_overwrite()
        .await
        .expect("direct commit is a no-op");

    let puts = session_puts(&server, "/resumable/ovw-1").await;
    assert_eq!(
        puts.len(),
        1,
        "one finalize PUT for the whole stream, got {}",
        puts.len()
    );
    let body = gunzip(&puts[0]);
    assert!(
        body.contains("\"id\":1") && body.contains("\"id\":2"),
        "both pages in one atomic load, got: {body}"
    );
}

/// Append + `media_load` streams pages into one `WRITE_APPEND` resumable load,
/// finalized on `flush`.
#[tokio::test]
async fn append_media_load_streams_one_load_on_flush() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    mount_resumable(&server, "/resumable/app-1", "load-app-1").await;

    let mut config = BigQuerySinkConfig::new(
        PROJECT_ID,
        DATASET_ID,
        TABLE_ID,
        BigQueryCredentials::ApplicationDefault,
    );
    config.media_load = true;
    config.upload_base_url = Some(server.uri());
    let config = with_sa_auth(config, &server);
    let (sink, _sa) = build_sink(&server, config).await;

    sink.write_batch(&[json!({"id": 1, "name": "a"})])
        .await
        .expect("page 1");
    sink.write_batch(&[json!({"id": 2, "name": "b"})])
        .await
        .expect("page 2");
    sink.flush().await.expect("flush finalizes");

    let puts = session_puts(&server, "/resumable/app-1").await;
    assert_eq!(puts.len(), 1, "one finalize PUT for the appended stream");
    let body = gunzip(&puts[0]);
    assert!(
        body.contains("\"id\":1") && body.contains("\"id\":2"),
        "got: {body}"
    );
}

/// An empty overwrite source opens no session and finalizes cleanly — no upload
/// initiate, no PUT — leaving the destination untouched.
#[tokio::test]
async fn empty_media_load_stream_finalizes_cleanly() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;

    let mut config = config_overwrite();
    config.media_load = true;
    config.create_table = false;
    config.upload_base_url = Some(server.uri());
    let config = with_sa_auth(config, &server);
    let (sink, _sa) = build_sink(&server, config).await;

    sink.begin_overwrite().await.expect("begin");
    sink.flush()
        .await
        .expect("flush on an empty stream is a no-op");
    sink.commit_overwrite().await.expect("commit no-op");

    let reqs = server.received_requests().await.expect("recording");
    assert!(
        !reqs.iter().any(|r| r.url.path().starts_with("/upload/")),
        "empty stream must not initiate an upload"
    );
}

/// Aborting a direct overwrite cancels the un-finalized resumable session
/// (DELETE the session URI); no finalize PUT is sent.
#[tokio::test]
async fn direct_overwrite_abort_cancels_session() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    mount_resumable(&server, "/resumable/abort-1", "load-abort-1").await;
    Mock::given(method("DELETE"))
        .and(path("/resumable/abort-1"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;

    let mut config = config_overwrite();
    config.media_load = true;
    config.create_table = false;
    config.upload_base_url = Some(server.uri());
    let config = with_sa_auth(config, &server);
    let (sink, _sa) = build_sink(&server, config).await;

    sink.begin_overwrite().await.expect("begin");
    sink.write_batch(&[json!({"id": 1, "name": "a"})])
        .await
        .expect("page opens the session");
    sink.abort_overwrite()
        .await
        .expect("abort cancels the session");

    let reqs = server.received_requests().await.expect("recording");
    let deletes = reqs
        .iter()
        .filter(|r| r.method.as_str() == "DELETE" && r.url.path() == "/resumable/abort-1")
        .count();
    assert_eq!(deletes, 1, "abort must DELETE the session URI");
    assert!(
        session_puts(&server, "/resumable/abort-1").await.is_empty(),
        "abort must not finalize (no PUT)"
    );
}

// ── Coverage: streaming session branches, load_ndjson, access_token ─────────

/// Matches a resumable PUT whose `Content-Range` upper bound is `*` (a
/// mid-stream chunk) vs the finalize PUT (a concrete total).
/// Rows whose values are deliberately high-entropy (hex of index products) so
/// gzip can't collapse them — the compressed stream then exceeds a small chunk
/// threshold mid-`feed`, exercising the resumable chunk-PUT loop.
fn bulky_rows(n: u64) -> Vec<serde_json::Value> {
    (0..n)
        .map(|i| {
            let a = i.wrapping_mul(0x9E37_79B9_7F4A_7C15);
            let b = i.wrapping_mul(0xC2B2_AE3D_27D4_EB4F);
            json!({"id": i, "name": format!("{a:016x}{b:016x}{i:016x}")})
        })
        .collect()
}

struct MidChunk;
impl wiremock::Match for MidChunk {
    fn matches(&self, req: &wiremock::Request) -> bool {
        req.headers
            .get("content-range")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.ends_with("/*"))
            .unwrap_or(false)
    }
}
struct FinalChunk;
impl wiremock::Match for FinalChunk {
    fn matches(&self, req: &wiremock::Request) -> bool {
        req.headers
            .get("content-range")
            .and_then(|v| v.to_str().ok())
            .map(|s| !s.ends_with("/*"))
            .unwrap_or(false)
    }
}

fn done_job(job_id: &str) -> serde_json::Value {
    json!({"jobReference": {"projectId": PROJECT_ID, "jobId": job_id},
           "status": {"state": "DONE"}})
}

async fn mount_init(server: &MockServer, session_path: &str) {
    let session_uri = format!("{}{session_path}", server.uri());
    Mock::given(method("POST"))
        .and(path(format!(
            "/upload/bigquery/v2/projects/{PROJECT_ID}/jobs"
        )))
        .and(query_param("uploadType", "resumable"))
        .respond_with(ResponseTemplate::new(200).insert_header("location", session_uri.as_str()))
        .mount(server)
        .await;
}
async fn mount_jobs_get_done(server: &MockServer, job_id: &str) {
    Mock::given(method("GET"))
        .and(path(format!("/projects/{PROJECT_ID}/jobs/{job_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(done_job(job_id)))
        .mount(server)
        .await;
}

fn direct_media_config(server: &MockServer) -> BigQuerySinkConfig {
    let mut c = config_overwrite();
    c.media_load = true;
    c.create_table = false;
    c.upload_base_url = Some(server.uri());
    with_sa_auth(c, server)
}

/// The mid-stream chunk-PUT loop: a small `resumable_chunk` threshold makes a
/// modest page cross it, so `feed` PUTs at least one 308 mid chunk before the
/// finalize PUT; the reassembled bytes gunzip to all rows.
#[tokio::test]
async fn resumable_multi_chunk_streams_and_reassembles() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    mount_init(&server, "/rz/mc").await;
    Mock::given(method("PUT"))
        .and(path("/rz/mc"))
        .and(MidChunk)
        .respond_with(ResponseTemplate::new(308))
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(path("/rz/mc"))
        .and(FinalChunk)
        .respond_with(ResponseTemplate::new(200).set_body_json(done_job("load-mc")))
        .mount(&server)
        .await;
    mount_jobs_get_done(&server, "load-mc").await;

    let mut config = direct_media_config(&server);
    config.resumable_chunk = Some(512); // tiny threshold → mid-chunk PUTs fire
    let (sink, _sa) = build_sink(&server, config).await;

    sink.begin_overwrite().await.expect("begin");
    let rows = bulky_rows(20_000);
    sink.write_batch(&rows).await.expect("write");
    sink.flush().await.expect("flush");

    let puts = session_puts(&server, "/rz/mc").await;
    assert!(
        puts.len() >= 2,
        "expected >=1 mid chunk + finalize, got {}",
        puts.len()
    );
    let all: Vec<u8> = puts.concat();
    let body = gunzip(&all);
    assert!(
        body.contains("\"id\":0") && body.contains("\"id\":19999"),
        "reassembled stream missing rows"
    );
}

/// A mid-stream chunk PUT that returns something other than 308 surfaces the
/// "expected 308 Resume Incomplete" error.
#[tokio::test]
async fn resumable_chunk_non_308_errors() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    mount_init(&server, "/rz/bad").await;
    Mock::given(method("PUT"))
        .and(path("/rz/bad"))
        .and(MidChunk)
        .respond_with(ResponseTemplate::new(400).set_body_string("nope"))
        .mount(&server)
        .await;

    let mut config = direct_media_config(&server);
    config.resumable_chunk = Some(512);
    let (sink, _sa) = build_sink(&server, config).await;
    sink.begin_overwrite().await.expect("begin");
    let rows = bulky_rows(20_000);
    let err = sink
        .write_batch(&rows)
        .await
        .expect_err("non-308 must error");
    assert!(err.to_string().contains("expected 308"), "got: {err}");
}

#[tokio::test]
async fn resumable_init_http_error_surfaces() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    Mock::given(method("POST"))
        .and(path(format!(
            "/upload/bigquery/v2/projects/{PROJECT_ID}/jobs"
        )))
        .and(query_param("uploadType", "resumable"))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .mount(&server)
        .await;
    let (sink, _sa) = build_sink(&server, direct_media_config(&server)).await;
    sink.begin_overwrite().await.expect("begin");
    let err = sink
        .write_batch(&[json!({"id": 1})])
        .await
        .expect_err("init 500 must error");
    assert!(
        err.to_string().contains("resumable init returned HTTP"),
        "got: {err}"
    );
}

#[tokio::test]
async fn resumable_init_missing_location_errors() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    Mock::given(method("POST"))
        .and(path(format!(
            "/upload/bigquery/v2/projects/{PROJECT_ID}/jobs"
        )))
        .and(query_param("uploadType", "resumable"))
        .respond_with(ResponseTemplate::new(200)) // no Location header
        .mount(&server)
        .await;
    let (sink, _sa) = build_sink(&server, direct_media_config(&server)).await;
    sink.begin_overwrite().await.expect("begin");
    let err = sink
        .write_batch(&[json!({"id": 1})])
        .await
        .expect_err("missing Location must error");
    assert!(err.to_string().contains("no Location header"), "got: {err}");
}

#[tokio::test]
async fn resumable_finalize_http_error_surfaces() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    mount_init(&server, "/rz/ff").await;
    Mock::given(method("PUT"))
        .and(path("/rz/ff"))
        .respond_with(ResponseTemplate::new(500).set_body_string("bad finalize"))
        .mount(&server)
        .await;
    let (sink, _sa) = build_sink(&server, direct_media_config(&server)).await;
    sink.begin_overwrite().await.expect("begin");
    sink.write_batch(&[json!({"id": 1})])
        .await
        .expect("write buffers");
    let err = sink.flush().await.expect_err("finalize 500 must error");
    assert!(
        err.to_string().contains("resumable finalize returned HTTP"),
        "got: {err}"
    );
}

#[tokio::test]
async fn resumable_finalize_missing_job_ref_errors() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    mount_init(&server, "/rz/nj").await;
    Mock::given(method("PUT"))
        .and(path("/rz/nj"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"status": {"state": "DONE"}})),
        )
        .mount(&server)
        .await;
    let (sink, _sa) = build_sink(&server, direct_media_config(&server)).await;
    sink.begin_overwrite().await.expect("begin");
    sink.write_batch(&[json!({"id": 1})]).await.expect("write");
    let err = sink
        .flush()
        .await
        .expect_err("missing jobReference must error");
    assert!(
        err.to_string().contains("missing jobReference"),
        "got: {err}"
    );
}

/// A second `flush` (or `commit`) after the load is finalized is a no-op — one
/// finalize PUT total.
#[tokio::test]
async fn flush_twice_finalizes_once() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    mount_resumable(&server, "/rz/once", "load-once").await;
    let (sink, _sa) = build_sink(&server, direct_media_config(&server)).await;
    sink.begin_overwrite().await.expect("begin");
    sink.write_batch(&[json!({"id": 1})]).await.expect("write");
    sink.flush().await.expect("flush 1 finalizes");
    sink.flush().await.expect("flush 2 is a no-op");
    assert_eq!(
        session_puts(&server, "/rz/once").await.len(),
        1,
        "one finalize PUT"
    );
}

/// `await_load_job` maps a DONE job carrying an `errorResult` to an error.
#[tokio::test]
async fn await_load_job_reports_job_error() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    mount_init(&server, "/rz/je").await;
    Mock::given(method("PUT"))
        .and(path("/rz/je"))
        .respond_with(ResponseTemplate::new(200).set_body_json(done_job("load-je")))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/projects/{PROJECT_ID}/jobs/load-je")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobReference": {"projectId": PROJECT_ID, "jobId": "load-je"},
            "status": {"state": "DONE", "errorResult": {"reason": "invalid", "message": "bad load"}}
        })))
        .mount(&server)
        .await;
    let (sink, _sa) = build_sink(&server, direct_media_config(&server)).await;
    sink.begin_overwrite().await.expect("begin");
    sink.write_batch(&[json!({"id": 1})]).await.expect("write");
    let err = sink
        .flush()
        .await
        .expect_err("job errorResult must surface");
    assert!(err.to_string().contains("failed"), "got: {err}");
}

/// `await_load_job` errors when the polled job has no `status` at all.
#[tokio::test]
async fn await_load_job_missing_status_errors() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    mount_init(&server, "/rz/ns").await;
    Mock::given(method("PUT"))
        .and(path("/rz/ns"))
        .respond_with(ResponseTemplate::new(200).set_body_json(done_job("load-ns")))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/projects/{PROJECT_ID}/jobs/load-ns")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobReference": {"projectId": PROJECT_ID, "jobId": "load-ns"}
        })))
        .mount(&server)
        .await;
    let (sink, _sa) = build_sink(&server, direct_media_config(&server)).await;
    sink.begin_overwrite().await.expect("begin");
    sink.write_batch(&[json!({"id": 1})]).await.expect("write");
    let err = sink.flush().await.expect_err("no status must error");
    assert!(err.to_string().contains("no status"), "got: {err}");
}

/// `await_load_job` polls past a non-DONE state (exercises the RUNNING→DONE loop).
#[tokio::test]
async fn await_load_job_polls_until_done() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    mount_init(&server, "/rz/poll").await;
    Mock::given(method("PUT"))
        .and(path("/rz/poll"))
        .respond_with(ResponseTemplate::new(200).set_body_json(done_job("load-poll")))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/projects/{PROJECT_ID}/jobs/load-poll")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobReference": {"projectId": PROJECT_ID, "jobId": "load-poll"},
            "status": {"state": "RUNNING"}
        })))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    mount_jobs_get_done(&server, "load-poll").await;
    let (sink, _sa) = build_sink(&server, direct_media_config(&server)).await;
    sink.begin_overwrite().await.expect("begin");
    sink.write_batch(&[json!({"id": 1})]).await.expect("write");
    sink.flush().await.expect("polls RUNNING then DONE");
}

/// Grouped staging + `media_load`: the staged page load goes through the
/// single-shot multipart `load_ndjson` (not the streaming session).
#[tokio::test]
async fn grouped_media_load_write_uses_multipart_load() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    mount_query_and_job(&server, "job-grp").await;
    mount_staging_present(&server).await;
    Mock::given(method("POST"))
        .and(path(format!(
            "/upload/bigquery/v2/projects/{PROJECT_ID}/jobs"
        )))
        .and(query_param("uploadType", "multipart"))
        .respond_with(ResponseTemplate::new(200).set_body_json(done_job("load-grp")))
        .mount(&server)
        .await;
    mount_jobs_get_done(&server, "load-grp").await;

    let mut config = config_overwrite();
    config.media_load = true;
    config.overwrite_staging = true;
    config.upload_base_url = Some(server.uri());
    let config = with_sa_auth(config, &server);
    let (sink, _sa) = build_sink(&server, config).await;
    sink.begin_overwrite().await.expect("begin stages");
    let n = sink
        .write_batch(&[json!({"id": 1, "name": "a"})])
        .await
        .expect("staged load");
    assert_eq!(n, 1);
    let reqs = server.received_requests().await.expect("recording");
    assert!(
        reqs.iter().any(|r| r.method.as_str() == "POST"
            && r.url.path().starts_with("/upload/")
            && r.url.query().unwrap_or("").contains("uploadType=multipart")),
        "grouped media staging must use the multipart load endpoint"
    );
}

/// A failed multipart `load_ndjson` upload surfaces as an error.
#[tokio::test]
async fn multipart_load_http_error_surfaces() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    mount_query_and_job(&server, "job-grp2").await;
    Mock::given(method("POST"))
        .and(path(format!(
            "/upload/bigquery/v2/projects/{PROJECT_ID}/jobs"
        )))
        .and(query_param("uploadType", "multipart"))
        .respond_with(ResponseTemplate::new(500).set_body_string("upload boom"))
        .mount(&server)
        .await;

    let mut config = config_overwrite();
    config.media_load = true;
    config.overwrite_staging = true;
    config.upload_base_url = Some(server.uri());
    let config = with_sa_auth(config, &server);
    let (sink, _sa) = build_sink(&server, config).await;
    sink.begin_overwrite().await.expect("begin");
    let err = sink
        .write_batch(&[json!({"id": 1})])
        .await
        .expect_err("upload 500 must error");
    assert!(
        err.to_string().contains("media load upload returned HTTP"),
        "got: {err}"
    );
}

/// Direct overwrite with a missing target + create_table on: creates the target
/// from the first page, then streams.
#[tokio::test]
async fn direct_overwrite_creates_missing_target_then_streams() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_missing(&server, Some(1)).await; // one self-heal probe, then present
    mount_table_schema(&server).await;
    mount_query_and_job(&server, "job-dc").await;
    mount_resumable(&server, "/rz/dc", "load-dc").await;

    let mut config = config_overwrite();
    config.media_load = true; // create_table defaults on
    config.upload_base_url = Some(server.uri());
    let config = with_sa_auth(config, &server);
    let (sink, _sa) = build_sink(&server, config).await;
    sink.begin_overwrite().await.expect("begin");
    sink.write_batch(&[json!({"id": 1, "name": "a"})])
        .await
        .expect("write creates + streams");
    sink.flush().await.expect("flush");

    let qs = queries(&server).await;
    assert!(
        qs.iter()
            .any(|q| q.starts_with("CREATE OR REPLACE TABLE `p.d.t` (")),
        "direct overwrite must create the missing target: {qs:?}"
    );
    assert_eq!(
        session_puts(&server, "/rz/dc").await.len(),
        1,
        "one finalize PUT"
    );
}

/// Direct overwrite, missing target, create_table disabled → typed error.
#[tokio::test]
async fn direct_overwrite_missing_target_create_disabled_errors() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_missing(&server, None).await;
    let (sink, _sa) = build_sink(&server, direct_media_config(&server)).await;
    sink.begin_overwrite().await.expect("begin");
    let err = sink
        .write_batch(&[json!({"id": 1})])
        .await
        .expect_err("missing + create off must error");
    assert!(
        err.to_string().contains("create_table` is disabled"),
        "got: {err}"
    );
}

/// Grouped staging overwrite, missing target, create_table disabled → typed error.
#[tokio::test]
async fn staging_overwrite_missing_target_create_disabled_errors() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_missing(&server, None).await;
    let mut config = config_overwrite();
    config.media_load = true;
    config.overwrite_staging = true;
    config.create_table = false;
    config.upload_base_url = Some(server.uri());
    let config = with_sa_auth(config, &server);
    let (sink, _sa) = build_sink(&server, config).await;
    // begin_overwrite on a grouped staging sink with a missing target + create
    // off errors at the staging create; drive the write path's staging branch
    // directly by skipping begin.
    let err = sink
        .write_batch(&[json!({"id": 1})])
        .await
        .expect_err("staging missing + create off must error");
    assert!(
        err.to_string().contains("create_table` is disabled"),
        "got: {err}"
    );
}

/// `access_token` on a malformed service-account JSON surfaces an Auth error.
#[tokio::test]
async fn access_token_invalid_json_errors() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    mount_init(&server, "/rz/bt").await;
    let mut config = BigQuerySinkConfig::new(
        PROJECT_ID,
        DATASET_ID,
        TABLE_ID,
        BigQueryCredentials::ApplicationDefault,
    );
    config.media_load = true;
    config.upload_base_url = Some(server.uri());
    config.auth = BigQueryCredentials::ServiceAccountKey {
        json: "{ not valid json".into(),
    };
    let (sink, _sa) = build_sink(&server, config).await;
    let err = sink
        .write_batch(&[json!({"id": 1})])
        .await
        .expect_err("bad SA json must error");
    assert!(
        err.to_string().contains("invalid service account JSON"),
        "got: {err}"
    );
}

/// `access_token` via the `ServiceAccountKeyPath` credential variant mints
/// against the mock token endpoint and streams successfully.
#[tokio::test]
async fn access_token_service_account_key_path_streams() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    mount_resumable(&server, "/rz/kp", "load-kp").await;

    let sa_json = dummy_service_account_json(&server.uri());
    let sa_file = tempfile::NamedTempFile::new().expect("sa tempfile");
    std::fs::write(sa_file.path(), serde_json::to_string(&sa_json).unwrap()).expect("write sa");

    let mut config = BigQuerySinkConfig::new(
        PROJECT_ID,
        DATASET_ID,
        TABLE_ID,
        BigQueryCredentials::ApplicationDefault,
    );
    config.media_load = true;
    config.upload_base_url = Some(server.uri());
    config.auth = BigQueryCredentials::ServiceAccountKeyPath {
        path: sa_file.path().to_str().unwrap().to_string(),
    };
    let (sink, _sa) = build_sink(&server, config).await;
    sink.write_batch(&[json!({"id": 1, "name": "a"})])
        .await
        .expect("write");
    sink.flush().await.expect("flush");
    assert_eq!(
        session_puts(&server, "/rz/kp").await.len(),
        1,
        "one finalize PUT"
    );
}

/// A **DLQ run keeps per-row isolation even under `media_load`** (#612).
///
/// `write_batch_partial` is only called when a DLQ is configured, and a
/// BigQuery load job is all-or-nothing — it cannot say which rows were
/// rejected. This method therefore takes the per-row `insertAll` path whatever
/// `media_load` says: the operator asked for row isolation by configuring a
/// DLQ, and silently turning "three bad rows quarantined, the rest written"
/// into "the whole page failed" would be a data-routing change they never see.
///
/// This test previously asserted the opposite (the load path being taken here,
/// returning a blanket `Ok` per row). Making `media_load` the default made that
/// behaviour reachable by everyone rather than only by opt-in, which is what
/// turned it from a documented quirk into a regression worth fixing.
#[tokio::test]
async fn write_batch_partial_keeps_row_isolation_under_media_load() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    // A resumable session is mounted but must go *unused*.
    mount_resumable(&server, "/rz/pa", "load-pa").await;
    Mock::given(method("POST"))
        .and(path(format!(
            "/projects/{PROJECT_ID}/datasets/{DATASET_ID}/tables/{TABLE_ID}/insertAll"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;
    let mut config = BigQuerySinkConfig::new(
        PROJECT_ID,
        DATASET_ID,
        TABLE_ID,
        BigQueryCredentials::ApplicationDefault,
    );
    config.media_load = true;
    config.upload_base_url = Some(server.uri());
    let config = with_sa_auth(config, &server);
    let (sink, _sa) = build_sink(&server, config).await;

    let outcomes = sink
        .write_batch_partial(&[json!({"id": 1}), json!({"id": 2})])
        .await
        .expect("partial append");
    assert_eq!(outcomes.len(), 2);
    assert!(outcomes.iter().all(|o| o.is_ok()));

    sink.flush().await.expect("flush");
    assert!(
        session_puts(&server, "/rz/pa").await.is_empty(),
        "the DLQ path must not route through a load session — per-row outcomes \
         are impossible there"
    );
}

// ── Coverage: malformed load/finalize responses + non-404 probe ─────────────

fn grouped_media_config(server: &MockServer) -> BigQuerySinkConfig {
    let mut c = config_overwrite();
    c.media_load = true;
    c.overwrite_staging = true;
    c.upload_base_url = Some(server.uri());
    with_sa_auth(c, server)
}

/// Grouped staging multipart load whose response is missing `jobReference`.
#[tokio::test]
async fn grouped_multipart_missing_job_ref_errors() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    mount_query_and_job(&server, "job-mjr").await;
    Mock::given(method("POST"))
        .and(path(format!(
            "/upload/bigquery/v2/projects/{PROJECT_ID}/jobs"
        )))
        .and(query_param("uploadType", "multipart"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"status": {"state": "DONE"}})),
        )
        .mount(&server)
        .await;
    let (sink, _sa) = build_sink(&server, grouped_media_config(&server)).await;
    sink.begin_overwrite().await.expect("begin");
    let err = sink
        .write_batch(&[json!({"id": 1})])
        .await
        .expect_err("missing jobReference");
    assert!(
        err.to_string().contains("missing jobReference"),
        "got: {err}"
    );
}

/// Grouped staging multipart load whose response is not valid JSON.
#[tokio::test]
async fn grouped_multipart_invalid_json_errors() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    mount_query_and_job(&server, "job-mij").await;
    Mock::given(method("POST"))
        .and(path(format!(
            "/upload/bigquery/v2/projects/{PROJECT_ID}/jobs"
        )))
        .and(query_param("uploadType", "multipart"))
        .respond_with(ResponseTemplate::new(200).set_body_string("<<<not json>>>"))
        .mount(&server)
        .await;
    let (sink, _sa) = build_sink(&server, grouped_media_config(&server)).await;
    sink.begin_overwrite().await.expect("begin");
    let err = sink
        .write_batch(&[json!({"id": 1})])
        .await
        .expect_err("bad json");
    assert!(err.to_string().contains("parse job response"), "got: {err}");
}

/// Grouped staging multipart load whose `jobReference` has no `jobId`.
#[tokio::test]
async fn grouped_multipart_missing_job_id_errors() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    mount_query_and_job(&server, "job-mid2").await;
    Mock::given(method("POST"))
        .and(path(format!(
            "/upload/bigquery/v2/projects/{PROJECT_ID}/jobs"
        )))
        .and(query_param("uploadType", "multipart"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobReference": {"projectId": PROJECT_ID}, "status": {"state": "DONE"}
        })))
        .mount(&server)
        .await;
    let (sink, _sa) = build_sink(&server, grouped_media_config(&server)).await;
    sink.begin_overwrite().await.expect("begin");
    let err = sink
        .write_batch(&[json!({"id": 1})])
        .await
        .expect_err("missing jobId");
    assert!(err.to_string().contains("missing jobId"), "got: {err}");
}

/// Streaming finalize whose response is not valid JSON.
#[tokio::test]
async fn resumable_finalize_invalid_json_errors() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    mount_init(&server, "/rz/fj").await;
    Mock::given(method("PUT"))
        .and(path("/rz/fj"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not-json"))
        .mount(&server)
        .await;
    let (sink, _sa) = build_sink(&server, direct_media_config(&server)).await;
    sink.begin_overwrite().await.expect("begin");
    sink.write_batch(&[json!({"id": 1})]).await.expect("write");
    let err = sink.flush().await.expect_err("bad finalize json");
    assert!(err.to_string().contains("parse job response"), "got: {err}");
}

/// Streaming finalize whose `jobReference` has no `jobId`.
#[tokio::test]
async fn resumable_finalize_missing_job_id_errors() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    mount_init(&server, "/rz/fi").await;
    Mock::given(method("PUT"))
        .and(path("/rz/fi"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobReference": {"projectId": PROJECT_ID}, "status": {"state": "DONE"}
        })))
        .mount(&server)
        .await;
    let (sink, _sa) = build_sink(&server, direct_media_config(&server)).await;
    sink.begin_overwrite().await.expect("begin");
    sink.write_batch(&[json!({"id": 1})]).await.expect("write");
    let err = sink.flush().await.expect_err("missing jobId");
    assert!(err.to_string().contains("missing jobId"), "got: {err}");
}

/// A non-404 `tables.get` during the overwrite target probe surfaces (not
/// treated as "missing").
#[tokio::test]
async fn overwrite_target_probe_non_404_errors() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    Mock::given(method("GET"))
        .and(path(format!(
            "/projects/{PROJECT_ID}/datasets/{DATASET_ID}/tables/{TABLE_ID}"
        )))
        .respond_with(ResponseTemplate::new(500).set_body_json(json!({
            "error": {"code": 500, "message": "backend error"}
        })))
        .mount(&server)
        .await;
    let (sink, _sa) = build_sink(&server, direct_media_config(&server)).await;
    sink.begin_overwrite().await.expect("begin");
    let err = sink
        .write_batch(&[json!({"id": 1})])
        .await
        .expect_err("non-404 probe must surface");
    assert!(err.to_string().contains("schema probe"), "got: {err}");
}

/// The append counterpart of `append_creates_table_when_missing`, on the load
/// path that #612 made the default: a missing table is still created from the
/// first page's inferred schema, and the rows then go through one resumable
/// load rather than `insertAll`.
#[tokio::test]
async fn append_creates_table_when_missing_under_the_default_load_path() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_missing(&server, None).await;
    mount_query_and_job(&server, "job-create").await;
    mount_resumable(&server, "/resumable/append-default", "load-append-default").await;

    // No `with_media_load(false)` — this is the shipped default.
    let mut config = BigQuerySinkConfig::new(
        PROJECT_ID,
        DATASET_ID,
        TABLE_ID,
        BigQueryCredentials::ApplicationDefault,
    );
    config.upload_base_url = Some(server.uri());
    let config = with_sa_auth(config, &server);
    let (sink, _sa) = build_sink(&server, config).await;

    let n = sink
        .write_batch(&[json!({"id": 1, "name": "a"})])
        .await
        .expect("write");
    assert_eq!(n, 1);
    sink.flush().await.expect("flush finalizes the load");

    assert!(
        queries(&server)
            .await
            .iter()
            .any(|q| q.starts_with("CREATE OR REPLACE TABLE `p.d.t` (")),
        "create_table must still run before the load: {:?}",
        queries(&server).await
    );

    let puts = session_puts(&server, "/resumable/append-default").await;
    assert_eq!(puts.len(), 1, "one finalize PUT for the page");
    assert!(
        gunzip(&puts[0]).contains("\"id\":1"),
        "the row must ride the load job"
    );

    let insert_alls = server
        .received_requests()
        .await
        .expect("recording enabled")
        .into_iter()
        .filter(|r| r.url.path().ends_with("/insertAll"))
        .count();
    assert_eq!(
        insert_alls, 0,
        "the default append must not fall back to streaming inserts"
    );
}
