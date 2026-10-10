//! Integration tests for [`MysqlSink`]'s schema-drift introspection +
//! evolution path (`current_schema` / `evolve_schema`, issue #194) against a
//! real MySQL instance via testcontainers.
//!
//! These tests require Docker. Each test boots its own container so they are
//! fully isolated and safe to run in parallel.

use faucet_core::{ColumnChange, SchemaEvolution, Sink};
use faucet_sink_mysql::{MysqlColumnMapping, MysqlSink, MysqlSinkConfig};
use serde_json::json;
use std::sync::OnceLock;
use testcontainers::ContainerAsync;
use testcontainers_modules::mysql::Mysql;
use tokio::sync::Semaphore;

/// Bounds concurrent MySQL container startups across all tests in this binary
/// (MySQL 8.x init is memory-heavy — mirrors the cap in `upsert.rs`).
fn startup_limit() -> &'static Semaphore {
    static SEM: OnceLock<Semaphore> = OnceLock::new();
    SEM.get_or_init(|| Semaphore::new(2))
}

/// Start a MySQL container and return both the container handle and a
/// connection URL.
async fn start_mysql() -> (ContainerAsync<Mysql>, String) {
    let _permit = startup_limit()
        .acquire()
        .await
        .expect("startup semaphore closed");
    let container: ContainerAsync<Mysql> =
        faucet_conformance::containers::start(Mysql::default).await;
    let port = container
        .get_host_port_ipv4(3306)
        .await
        .expect("mysql port");
    let url = format!("mysql://root@127.0.0.1:{port}/test");
    (container, url)
}

#[tokio::test(flavor = "multi_thread")]
async fn current_schema_then_evolve_add_and_widen_is_idempotent() {
    let (_container, url) = start_mysql().await;

    // 1. Create a one-column table: id BIGINT (nullable — no NOT NULL).
    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool connect");
    sqlx::query("CREATE TABLE t (id BIGINT)")
        .execute(&pool)
        .await
        .expect("create table");
    pool.close().await;

    // 2. Build the sink (AutoMap) and read the live schema.
    let sink =
        MysqlSink::new(MysqlSinkConfig::new(&url, "t").column_mapping(MysqlColumnMapping::AutoMap))
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
        "id BIGINT must surface as a nullable integer; got {schema:?}"
    );
    assert!(!props.contains_key("email"), "email must not exist yet");

    // 3. Evolve: add `email: text`, widen `id` integer→number (DOUBLE).
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
        "email must now exist as a nullable string; got {schema2:?}"
    );
    assert_eq!(
        props2.get("id"),
        Some(&json!({ "type": ["number", "null"] })),
        "id must have widened to a numeric type (DOUBLE); got {schema2:?}"
    );

    // 5. Re-run the SAME evolution → idempotent, no error, no duplicate column.
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
        props3.len(),
        2,
        "exactly two columns must exist (id, email) after a repeated evolution; got {props3:?}"
    );
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
    let (_container, url) = start_mysql().await;

    let sink = MysqlSink::new(
        MysqlSinkConfig::new(&url, "does_not_exist").column_mapping(MysqlColumnMapping::AutoMap),
    )
    .await
    .expect("sink new");

    assert_eq!(
        sink.current_schema().await.expect("current_schema"),
        None,
        "a missing table must report no schema"
    );
}
