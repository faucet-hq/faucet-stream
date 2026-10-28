//! The Redshift source over `credentials: { type: redshift_data_api }` and
//! `{ type: iam }`, end to end.
//!
//! The Data API is a stateful contract fake (`support::DataApiFake`) that runs
//! each statement on a real PostgreSQL container and renders its rows as the
//! service's typed fields, paged; `iam` logs in to the same container with
//! credentials from a fake `GetClusterCredentials`.
#![allow(clippy::disallowed_methods)]

#[path = "../../../common/redshift/tests/support/mod.rs"]
mod support;

use std::collections::HashMap;

use faucet_conformance::fidelity::{self, Tolerance};
use faucet_core::Source;
use faucet_source_redshift::{RedshiftSource, RedshiftSourceConfig};
use futures::StreamExt;
use serde_json::{Value, json};
use support::{DataApiFake, REGION, access_key, admin_url};
use wiremock::MockServer;

struct Env {
    _pg: testcontainers::ContainerAsync<testcontainers_modules::postgres::Postgres>,
    admin: sqlx::PgPool,
    server: MockServer,
    fake: DataApiFake,
}

async fn env(page_size: usize) -> Env {
    let (pg, port) = support::start_postgres().await;
    let admin = sqlx::PgPool::connect(&admin_url(port)).await.unwrap();
    let server = MockServer::start().await;
    let fake = DataApiFake::new(admin_url(port), page_size);
    fake.mount(&server).await;
    Env {
        _pg: pg,
        admin,
        server,
        fake,
    }
}

fn source(env: &Env, query: &str, extra: Value) -> RedshiftSource {
    let mut cfg = json!({
        "database": "postgres",
        "credentials": {"type": "redshift_data_api", "config": {
            "cluster_identifier": "faucet-cluster",
            "db_user": "reader",
            "region": REGION,
            "aws_credentials": access_key(),
            "endpoint_url": env.server.uri(),
        }},
        "query": query,
    });
    for (k, v) in extra.as_object().unwrap() {
        cfg[k] = v.clone();
    }
    let cfg: RedshiftSourceConfig = serde_json::from_value(cfg).unwrap();
    RedshiftSource::new(cfg).unwrap()
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

/// A SQL literal for one corpus value.
fn literal(v: &Value) -> String {
    match v {
        Value::Null => "NULL".into(),
        Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        Value::Number(n) if n.as_f64() == Some(0.0) && n.to_string().starts_with('-') => {
            "'-0'::float8".into()
        }
        other => other.to_string(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_fidelity_corpus_reads_back_exactly_through_the_data_api() {
    let env = env(1).await;
    let mut wide = fidelity::flat_corpus();
    for f in [
        "nested_object",
        "nested_array",
        "nested_empty_object",
        "nested_empty_array",
    ] {
        wide.as_object_mut().unwrap().remove(f);
    }
    let obj = wide.as_object().unwrap();
    let names: Vec<String> = obj.keys().map(|k| format!("\"{k}\"")).collect();
    let values: Vec<String> = obj.values().map(literal).collect();
    sqlx::raw_sql(&format!(
        "CREATE TABLE corpus ({}); INSERT INTO corpus ({}) VALUES ({});
         CREATE TABLE typed (__fidelity_id TEXT, dec NUMERIC(38, 9), dec_scale NUMERIC(10, 2),
             big NUMERIC(38, 0), bin BYTEA, at TIMESTAMPTZ, local TIMESTAMP, f4 REAL);
         INSERT INTO typed VALUES ('typed', 12345678901234567890.123456789, 1.50,
             123456789012345678901234567890, '\\x000102fdfeff', '2026-09-18 12:34:56.123456+00',
             '1969-07-20 20:17:40', 0.1), ('second', NULL, NULL, NULL, NULL, NULL, NULL, NULL);",
        columns(&wide),
        names.join(", "),
        values.join(", "),
    ))
    .execute(&env.admin)
    .await
    .unwrap();

    let s = source(&env, "SELECT * FROM corpus", json!({}));
    fidelity::assert_corpus_read(&s, &[wide], Tolerance::exact()).await;

    let typed = vec![
        json!({
            "__fidelity_id": "typed",
            "dec": "12345678901234567890.123456789",
            "dec_scale": "1.50",
            "big": "123456789012345678901234567890",
            "bin": "AAEC/f7/",
            "at": "2026-09-18T12:34:56.123456Z",
            "local": "1969-07-20T20:17:40.000000",
            "f4": 0.1,
        }),
        json!({"__fidelity_id": "second", "dec": null, "dec_scale": null, "big": null,
               "bin": null, "at": null, "local": null, "f4": null}),
    ];
    let s = source(&env, "SELECT * FROM typed ORDER BY 1 DESC", json!({}));
    fidelity::assert_corpus_read(&s, &typed, Tolerance::exact()).await;
    let pages = env
        .fake
        .targets()
        .iter()
        .filter(|t| t.as_str() == "RedshiftData.GetStatementResult")
        .count();
    assert_eq!(
        pages, 3,
        "one page for the wide row, two for the typed rows"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn incremental_reads_page_resume_and_bind_the_bookmark_as_a_parameter() {
    let env = env(2).await;
    sqlx::raw_sql(
        "CREATE TABLE events (id INT, v TEXT);
         INSERT INTO events VALUES (1, 'a'), (2, 'b'), (2, 'b2'), (3, 'c'), (4, 'd');",
    )
    .execute(&env.admin)
    .await
    .unwrap();
    let s = source(
        &env,
        "SELECT id, v FROM events WHERE id >= ${bookmark}",
        json!({
            "batch_size": 2,
            "replication": {"type": "incremental", "column": "id", "initial_value": 2},
        }),
    );
    let ctx = HashMap::new();
    let pages: Vec<_> = s.stream_pages(&ctx, 2).map(|p| p.unwrap()).collect().await;
    let ids: Vec<Vec<i64>> = pages
        .iter()
        .map(|p| {
            p.records
                .iter()
                .map(|r| r["id"].as_i64().unwrap())
                .collect()
        })
        .collect();
    assert_eq!(ids, [vec![2, 2], vec![3, 4], vec![]]);
    let last = pages.last().unwrap().bookmark.clone().unwrap();

    let submitted = env.fake.submitted();
    assert_eq!(
        submitted[0].body["Sql"],
        "SELECT * FROM (SELECT id, v FROM events WHERE id >= NULL\n) AS q LIMIT 0",
        "the cursor type comes from a LIMIT 0 run"
    );
    assert_eq!(
        submitted[1].body["Sql"],
        "SELECT * FROM (SELECT id, v FROM events WHERE id >= CAST(:p1 AS int4)\n) AS q \
         WHERE q.\"id\" >= CAST(:p2 AS int4) ORDER BY q.\"id\""
    );
    assert_eq!(
        submitted[1].body["Parameters"],
        json!([{"name": "p1", "value": "2"}, {"name": "p2", "value": "2"}])
    );
    assert_eq!(submitted[1].body["DbUser"], "reader");

    sqlx::query("INSERT INTO events VALUES (4, 'd2'), (5, 'e')")
        .execute(&env.admin)
        .await
        .unwrap();
    s.apply_start_bookmark(last).await.unwrap();
    let (rows, _) = s.fetch_with_context_incremental(&ctx).await.unwrap();
    let got: Vec<&str> = rows.iter().map(|r| r["v"].as_str().unwrap()).collect();
    assert_eq!(
        got,
        ["d2", "e"],
        "rows already emitted at the bookmark are dropped"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn failures_columns_and_the_preflight_go_through_the_data_api() {
    let env = env(10).await;
    sqlx::query("CREATE TABLE t (id BIGINT NOT NULL, at TIMESTAMPTZ)")
        .execute(&env.admin)
        .await
        .unwrap();

    let err = source(&env, "SELECT * FROM missing", json!({}))
        .fetch_all()
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("statement failed"), "{err}");
    assert!(err.contains("\"missing\" does not exist"), "{err}");

    let s = source(&env, "SELECT * FROM t", json!({}));
    assert_eq!(
        s.dataset_uri(),
        "redshift://cluster:faucet-cluster/postgres?query=SELECT * FROM t"
    );
    let declared = s.declared_columns(&HashMap::new()).await.unwrap().unwrap();
    let rendered = format!("{declared:?}");
    assert!(
        rendered.contains("\"id\"") && rendered.contains("Int64"),
        "{rendered}"
    );
    assert!(rendered.contains("Timestamp"), "{rendered}");
    assert!(s.fetch_all().await.unwrap().is_empty());

    let report = s
        .check(&faucet_core::check::CheckContext::default())
        .await
        .unwrap();
    assert!(report.failed_count() == 0, "{report:?}");
    assert!(
        env.fake
            .submitted()
            .iter()
            .any(|st| st.body["Sql"] == "SELECT 1")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_iam_source_reads_with_temporary_cluster_credentials() {
    let (_pg, port) = support::start_postgres().await;
    let admin = sqlx::PgPool::connect(&admin_url(port)).await.unwrap();
    sqlx::raw_sql(
        r#"CREATE ROLE "IAM:reader" LOGIN PASSWORD 'tmp-pw';
           CREATE TABLE t (id INT); INSERT INTO t VALUES (1), (2);
           GRANT SELECT ON t TO "IAM:reader";"#,
    )
    .execute(&admin)
    .await
    .unwrap();
    let server = MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::body_string_contains(
        "Action=GetClusterCredentials",
    ))
    .and(wiremock::matchers::body_string_contains("DbUser=reader"))
    .respond_with(support::credentials_response(
        "IAM:reader",
        "tmp-pw",
        std::time::Duration::from_secs(900),
    ))
    .expect(1)
    .mount(&server)
    .await;
    let cfg: RedshiftSourceConfig = serde_json::from_value(json!({
        "host": "127.0.0.1",
        "port": port,
        "database": "postgres",
        "user": "reader",
        "tls": false,
        "credentials": {"type": "iam", "config": {
            "cluster_identifier": "c", "region": REGION,
            "aws_credentials": access_key(), "endpoint_url": server.uri()}},
        "query": "SELECT id FROM t ORDER BY id",
    }))
    .unwrap();
    let s = RedshiftSource::new(cfg).unwrap();
    assert!(
        s.check(&faucet_core::check::CheckContext::default())
            .await
            .unwrap()
            .failed_count()
            == 0
    );
    assert_eq!(
        s.fetch_all().await.unwrap(),
        [json!({"id": 1}), json!({"id": 2})]
    );
}
