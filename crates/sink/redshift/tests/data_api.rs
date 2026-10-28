//! The Redshift sink over `credentials: { type: redshift_data_api }` and
//! `{ type: iam }`, end to end.
//!
//! The Data API is a stateful contract fake (`support::DataApiFake`) that runs
//! each statement on a real PostgreSQL container; what landed is read back
//! through the same Data API. `iam` logs in to the container with credentials
//! from a fake `GetClusterCredentials`.
#![allow(clippy::disallowed_methods)]

#[path = "../../../common/redshift/tests/support/mod.rs"]
mod support;

use faucet_common_redshift::DataApiClient;
use faucet_common_redshift::data_api::Statement;
use faucet_conformance::fidelity::{self, Tolerance};
use faucet_core::{FaucetError, Sink};
use faucet_sink_redshift::{RedshiftSink, RedshiftSinkConfig};
use serde_json::{Value, json};
use support::{DataApiFake, REGION, access_key, admin_url};
use wiremock::MockServer;

struct Env {
    _pg: testcontainers::ContainerAsync<testcontainers_modules::postgres::Postgres>,
    admin: sqlx::PgPool,
    server: MockServer,
    fake: DataApiFake,
}

async fn env() -> Env {
    let (pg, port) = support::start_postgres().await;
    let admin = sqlx::PgPool::connect(&admin_url(port)).await.unwrap();
    let server = MockServer::start().await;
    let fake = DataApiFake::new(admin_url(port), 1000);
    fake.mount(&server).await;
    Env {
        _pg: pg,
        admin,
        server,
        fake,
    }
}

fn credentials(env: &Env) -> Value {
    json!({"type": "redshift_data_api", "config": {
        "workgroup_name": "faucet-wg",
        "region": REGION,
        "aws_credentials": access_key(),
        "endpoint_url": env.server.uri(),
    }})
}

fn config(env: &Env, table: &str, extra: Value) -> RedshiftSinkConfig {
    let mut cfg = json!({
        "database": "postgres",
        "credentials": credentials(env),
        "table_name": table,
        "write_strategy": "insert",
        "create_table": false,
    });
    for (k, v) in extra.as_object().unwrap() {
        cfg[k] = v.clone();
    }
    serde_json::from_value(cfg).unwrap()
}

/// Every row of `table` read back through the Data API.
async fn read_back(env: &Env, table: &str) -> Vec<Value> {
    let conn: faucet_common_redshift::RedshiftConnection = serde_json::from_value(json!({
        "database": "postgres", "credentials": credentials(env)
    }))
    .unwrap();
    let api = DataApiClient::from_connection(&conn, FaucetError::Source)
        .unwrap()
        .unwrap();
    api.query(
        &faucet_core::CallContext::new(),
        &Statement::new(format!("SELECT * FROM {table} ORDER BY 1")),
    )
    .await
    .unwrap()
    .1
    .into_iter()
    .map(Value::Object)
    .collect()
}

fn columns(row: &Value) -> String {
    row.as_object()
        .unwrap()
        .iter()
        .map(|(k, v)| {
            let ty = match v {
                Value::Number(n) if n.is_i64() || n.is_u64() => "BIGINT",
                Value::Number(_) => "DOUBLE PRECISION",
                Value::Bool(_) => "BOOLEAN",
                _ => "TEXT",
            };
            format!("\"{k}\" {ty}")
        })
        .collect::<Vec<_>>()
        .join(", ")
}

#[tokio::test(flavor = "multi_thread")]
async fn the_fidelity_corpus_lands_exactly_through_data_api_inserts() {
    let env = env().await;
    let mut wide = fidelity::flat_corpus();
    for f in [
        "nested_object",
        "nested_array",
        "nested_empty_object",
        "nested_empty_array",
    ] {
        wide.as_object_mut().unwrap().remove(f);
    }
    let wide = vec![wide];
    sqlx::raw_sql(&format!(
        "CREATE TABLE corpus ({}); CREATE TABLE typed (__fidelity_id TEXT, dec NUMERIC(38, 9), \
         dec_scale NUMERIC(10, 2), big NUMERIC(38, 0), at TIMESTAMPTZ);",
        columns(&wide[0])
    ))
    .execute(&env.admin)
    .await
    .unwrap();
    let sink = RedshiftSink::new(config(&env, "corpus", json!({})))
        .await
        .unwrap();
    fidelity::assert_corpus_written(
        &sink,
        &wide,
        || read_back(&env, "corpus"),
        Tolerance::exact(),
    )
    .await;

    let typed = vec![json!({
        "__fidelity_id": "typed",
        "dec": "12345678901234567890.123456789",
        "dec_scale": "1.50",
        "big": "123456789012345678901234567890",
        "at": "2026-09-18T12:34:56.123456Z",
    })];
    let sink = RedshiftSink::new(config(&env, "typed", json!({})))
        .await
        .unwrap();
    fidelity::assert_corpus_written(
        &sink,
        &typed,
        || read_back(&env, "typed"),
        Tolerance::exact(),
    )
    .await;

    let insert = env
        .fake
        .submitted()
        .into_iter()
        .map(|s| s.body)
        .find(|b| {
            b["Sql"]
                .as_str()
                .is_some_and(|s| s.starts_with("INSERT INTO \"typed\""))
        })
        .unwrap();
    assert_eq!(insert["WorkgroupName"], "faucet-wg");
    let sql = insert["Sql"].as_str().unwrap();
    assert!(sql.contains("CAST(:p2 AS numeric(38,9))"), "{sql}");
    assert!(sql.contains("AS timestamp with time zone)"), "{sql}");
    assert!(
        insert["Parameters"]
            .as_array()
            .unwrap()
            .contains(&json!({"name": "p2", "value": "12345678901234567890.123456789"})),
        "the decimal travels as its exact text"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_sink_creates_and_widens_its_table_through_the_data_api() {
    let env = env().await;
    let sink = RedshiftSink::new(config(
        &env,
        "events",
        json!({"create_table": true, "schema": "analytics"}),
    ))
    .await
    .unwrap();
    sink.write_batch(&[
        json!({"id": 1, "name": "a"}),
        json!({"id": 2, "name": null}),
    ])
    .await
    .unwrap();
    sink.flush().await.unwrap();
    sink.write_batch(&[json!({"id": 3, "name": "c", "score": 1.5})])
        .await
        .unwrap();
    sink.flush().await.unwrap();
    assert_eq!(
        read_back(&env, "analytics.events").await,
        [
            json!({"id": 1, "name": "a", "score": null}),
            json!({"id": 2, "name": null, "score": null}),
            json!({"id": 3, "name": "c", "score": 1.5}),
        ]
    );
    let sqls: Vec<String> = env
        .fake
        .submitted()
        .into_iter()
        .map(|s| s.body["Sql"].as_str().unwrap().to_string())
        .collect();
    assert!(
        sqls.iter()
            .any(|s| s == "CREATE SCHEMA IF NOT EXISTS \"analytics\""),
        "{sqls:?}"
    );
    assert!(
        sqls.iter()
            .any(|s| s.starts_with("CREATE TABLE IF NOT EXISTS \"analytics\".\"events\"")),
        "{sqls:?}"
    );
    assert!(
        sqls.iter()
            .any(|s| s.contains("ADD COLUMN") && s.contains("\"score\"")),
        "{sqls:?}"
    );
    assert_eq!(
        sink.dataset_uri(),
        "redshift://workgroup:faucet-wg/postgres?table=analytics.events"
    );
    let report = sink
        .check(&faucet_core::check::CheckContext::default())
        .await
        .unwrap();
    assert_eq!(report.failed_count(), 0, "{report:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_statement_fails_the_write_and_a_missing_table_is_named() {
    let env = env().await;
    sqlx::query("CREATE TABLE typed (id BIGINT)")
        .execute(&env.admin)
        .await
        .unwrap();
    let sink = RedshiftSink::new(config(&env, "typed", json!({})))
        .await
        .unwrap();
    sink.write_batch(&[json!({"id": "not a number"})])
        .await
        .unwrap();
    let err = sink.flush().await.unwrap_err().to_string();
    assert!(err.contains("redshift: INSERT failed"), "{err}");
    assert!(err.contains("statement failed"), "{err}");
    assert!(err.contains("bigint"), "{err}");

    let missing = RedshiftSink::new(config(&env, "absent", json!({})))
        .await
        .unwrap();
    let err = missing
        .write_batch(&[json!({"id": 1})])
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("absent"), "{err}");

    // The failed row stays buffered for a retry; a fresh sink takes the DLQ path.
    let sink = RedshiftSink::new(config(&env, "typed", json!({})))
        .await
        .unwrap();
    let outcomes = sink
        .write_batch_partial(&[json!({"id": 5}), json!("not an object")])
        .await
        .unwrap();
    assert!(outcomes[0].is_ok() && outcomes[1].is_err(), "{outcomes:?}");
    assert_eq!(read_back(&env, "typed").await, [json!({"id": 5})]);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_iam_sink_writes_with_temporary_workgroup_credentials() {
    let (_pg, port) = support::start_postgres().await;
    let admin = sqlx::PgPool::connect(&admin_url(port)).await.unwrap();
    sqlx::raw_sql(
        r#"CREATE ROLE "IAMR:loader" LOGIN PASSWORD 'wg-pw';
           CREATE TABLE t (id INT, name TEXT);
           GRANT SELECT, INSERT ON t TO "IAMR:loader";"#,
    )
    .execute(&admin)
    .await
    .unwrap();
    let server = MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::header(
        "x-amz-target",
        "RedshiftServerless.GetCredentials",
    ))
    .respond_with(support::serverless_response(
        "IAMR:loader",
        "wg-pw",
        std::time::Duration::from_secs(900),
    ))
    .expect(1)
    .mount(&server)
    .await;
    let cfg: RedshiftSinkConfig = serde_json::from_value(json!({
        "host": "127.0.0.1",
        "port": port,
        "database": "postgres",
        "tls": false,
        "credentials": {"type": "iam", "config": {
            "workgroup_name": "wg", "region": REGION,
            "aws_credentials": access_key(), "endpoint_url": server.uri()}},
        "table_name": "t",
        "write_strategy": "insert",
        "create_table": false,
    }))
    .unwrap();
    let sink = RedshiftSink::new(cfg).await.unwrap();
    sink.write_batch(&[json!({"id": 1, "name": "x"})])
        .await
        .unwrap();
    sink.flush().await.unwrap();
    let got: Vec<(i32, String)> = sqlx::query_as("SELECT id, name FROM t")
        .fetch_all(&admin)
        .await
        .unwrap();
    assert_eq!(got, [(1, "x".to_string())]);
}
