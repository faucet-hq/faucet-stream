//! Composition test (#190): exactly-once delivery + `write_mode: upsert` for
//! the PostgreSQL sink, against a real Postgres instance via testcontainers.
//!
//! Verifies that `write_batch_idempotent` routes through the upsert planner so
//! the data write AND the commit-token watermark commit atomically in one
//! transaction. Re-writing the same key in a later page (with a higher token)
//! must UPDATE the row in place — not duplicate it — and advance the token.
//!
//! Requires Docker. Boots its own container so it is isolated.

use faucet_core::{Sink, WriteMode, WriteSpec, format_token};
use faucet_sink_postgres::{PostgresColumnMapping, PostgresSink, PostgresSinkConfig};
use serde_json::json;
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;

async fn start_postgres() -> (ContainerAsync<Postgres>, String) {
    let image = Postgres::default().with_tag("16-alpine");
    let container: ContainerAsync<Postgres> =
        image.start().await.expect("postgres container start");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("postgres port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    (container, url)
}

async fn create_kv_table(url: &str) {
    let pool = sqlx::PgPool::connect(url).await.expect("pool connect");
    sqlx::query("CREATE TABLE kv (id INT PRIMARY KEY, name TEXT)")
        .execute(&pool)
        .await
        .expect("create table");
    pool.close().await;
}

async fn row_count(url: &str) -> i64 {
    let pool = sqlx::PgPool::connect(url).await.expect("pool connect");
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*)::BIGINT FROM kv")
        .fetch_one(&pool)
        .await
        .expect("count");
    pool.close().await;
    count
}

async fn name_for_id(url: &str, id: i32) -> Option<String> {
    let pool = sqlx::PgPool::connect(url).await.expect("pool connect");
    let name: Option<String> = sqlx::query_scalar("SELECT name FROM kv WHERE id = $1")
        .bind(id)
        .fetch_optional(&pool)
        .await
        .expect("read back");
    pool.close().await;
    name
}

fn upsert_sink_config(url: &str) -> PostgresSinkConfig {
    let mut config = PostgresSinkConfig::new(url, "kv")
        .column_mapping(PostgresColumnMapping::AutoMap)
        .with_batch_size(0);
    config.write = WriteSpec {
        write_mode: WriteMode::Upsert,
        key: vec!["id".into()],
        delete_marker: None,
        rollback: None,
    };
    config
}

#[tokio::test(flavor = "multi_thread")]
async fn idempotent_upsert_updates_in_place_and_advances_token() {
    let (_container, url) = start_postgres().await;
    create_kv_table(&url).await;

    let sink = PostgresSink::new(upsert_sink_config(&url))
        .await
        .expect("sink new");

    let scope = "kv::r1";
    let t1 = format_token(1);
    let t2 = format_token(2);

    // Page 1, token ...0001: upsert id=1 -> "a" in the idempotent path.
    let written = sink
        .write_batch_idempotent(&[json!({"id": 1, "name": "a"})], scope, &t1)
        .await
        .expect("idempotent upsert 1");
    assert_eq!(written, 1, "one upsert applied");
    assert_eq!(
        sink.last_committed_token(scope).await.expect("token read"),
        Some(t1.clone()),
        "token advanced to t1"
    );
    assert_eq!(row_count(&url).await, 1);
    assert_eq!(name_for_id(&url, 1).await.as_deref(), Some("a"));

    // Page 2, token ...0002: upsert id=1 -> "b".
    let written = sink
        .write_batch_idempotent(&[json!({"id": 1, "name": "b"})], scope, &t2)
        .await
        .expect("idempotent upsert 2");
    assert_eq!(written, 1, "one upsert applied (update in place)");

    // Exactly ONE row id=1, now "b" — upserted in the idempotent path, not duplicated.
    assert_eq!(
        row_count(&url).await,
        1,
        "upsert in the idempotent path must update, not duplicate"
    );
    assert_eq!(
        name_for_id(&url, 1).await.as_deref(),
        Some("b"),
        "row must reflect the latest value 'b'"
    );

    // Token advanced to t2 — committed atomically with the upsert.
    assert_eq!(
        sink.last_committed_token(scope).await.expect("token read"),
        Some(t2),
        "token advanced to t2 alongside the upsert"
    );
}
