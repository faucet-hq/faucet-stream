//! Integration tests for [`PostgresSink`]'s schema-drift introspection +
//! evolution path (`current_schema` / `evolve_schema`, issue #194) against a
//! real Postgres instance via testcontainers.
//!
//! These tests require Docker. Each test boots its own container so they are
//! fully isolated and safe to run in parallel.

use faucet_core::{ColumnChange, SchemaEvolution, Sink};
use faucet_sink_postgres::{PostgresColumnMapping, PostgresSink, PostgresSinkConfig};
use serde_json::json;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;

/// Start a Postgres container and return both the container handle and a
/// connection URL.
async fn start_postgres() -> (ContainerAsync<Postgres>, String) {
    let container: ContainerAsync<Postgres> =
        faucet_conformance::containers::start(|| Postgres::default().with_tag("16-alpine")).await;
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("postgres port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    (container, url)
}

#[tokio::test(flavor = "multi_thread")]
async fn current_schema_then_evolve_add_and_widen_is_idempotent() {
    let (_container, url) = start_postgres().await;

    // 1. Create a one-column table: id bigint (int8, NOT NULL is not implied,
    //    so it is nullable).
    let pool = sqlx::PgPool::connect(&url).await.expect("pool connect");
    sqlx::query("CREATE TABLE t (id bigint)")
        .execute(&pool)
        .await
        .expect("create table");
    pool.close().await;

    // 2. Build the sink (AutoMap) and read the live schema.
    let sink = PostgresSink::new(
        PostgresSinkConfig::new(&url, "t").column_mapping(PostgresColumnMapping::AutoMap),
    )
    .await
    .expect("sink new");

    let schema = sink
        .current_schema()
        .await
        .expect("current_schema")
        .expect("table exists");
    let props = schema
        .get("properties")
        .and_then(|p| p.as_object())
        .expect("properties object");
    assert_eq!(
        props.get("id"),
        Some(&json!({ "type": ["integer", "null"] })),
        "id bigint must surface as a nullable integer; got {schema:?}"
    );
    assert!(!props.contains_key("email"), "email must not exist yet");

    // 3. Evolve: add `email: text`, widen `id` integer→number.
    let evolution = SchemaEvolution {
        additions: vec![ColumnChange {
            name: "email".into(),
            from: None,
            to: json!({ "type": "string" }),
        }],
        widenings: vec![ColumnChange {
            name: "id".into(),
            from: Some(json!({ "type": ["integer", "null"] })),
            to: json!({ "type": ["number", "null"] }),
        }],
        relax_nullability: vec![],
    };
    sink.evolve_schema(&evolution).await.expect("evolve_schema");

    // 4. Re-query: email present, id widened to a numeric type.
    let schema2 = sink
        .current_schema()
        .await
        .expect("current_schema 2")
        .expect("table still exists");
    let props2 = schema2
        .get("properties")
        .and_then(|p| p.as_object())
        .expect("properties object");
    assert_eq!(
        props2.get("email"),
        Some(&json!({ "type": ["string", "null"] })),
        "email text must now exist as a nullable string; got {schema2:?}"
    );
    assert_eq!(
        props2.get("id"),
        Some(&json!({ "type": ["number", "null"] })),
        "id must have widened to a numeric type (double precision); got {schema2:?}"
    );

    // 5. Re-run the SAME evolution → idempotent, no error.
    sink.evolve_schema(&evolution)
        .await
        .expect("re-running the same evolution must be idempotent");

    let schema3 = sink
        .current_schema()
        .await
        .expect("current_schema 3")
        .expect("table still exists");
    let props3 = schema3
        .get("properties")
        .and_then(|p| p.as_object())
        .expect("properties object");
    assert_eq!(
        props3.get("id"),
        Some(&json!({ "type": ["number", "null"] })),
        "id type must be stable after a repeated evolution"
    );
    assert_eq!(
        props3.get("email"),
        Some(&json!({ "type": ["string", "null"] })),
        "email must still exist after a repeated evolution"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn current_schema_is_none_for_missing_table() {
    let (_container, url) = start_postgres().await;

    let sink = PostgresSink::new(
        PostgresSinkConfig::new(&url, "does_not_exist")
            .column_mapping(PostgresColumnMapping::AutoMap),
    )
    .await
    .expect("sink new");

    assert_eq!(
        sink.current_schema().await.expect("current_schema"),
        None,
        "a missing table must report no schema"
    );
}
