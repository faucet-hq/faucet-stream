//! Integration tests for [`PostgresSink`]'s exactly-once / idempotent write
//! path and its AutoMap edge/error branches, against a real Postgres instance
//! via testcontainers.
//!
//! These tests require Docker. Each test boots its own container so they are
//! fully isolated and safe to run in parallel.

use faucet_core::Sink;
use faucet_core::check::ProbeStatus;
use faucet_core::idempotency::{
    COMMIT_TOKEN_SCOPE_COL, COMMIT_TOKEN_TABLE, COMMIT_TOKEN_TOKEN_COL, format_token,
};
use faucet_sink_postgres::{PostgresColumnMapping, PostgresSink, PostgresSinkConfig};
use serde_json::json;
use sqlx::Row;
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;

/// Start a Postgres container and return both the container handle and a
/// connection URL.
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

/// Create a single-JSONB-column `events` table.
async fn create_jsonb_table(url: &str) {
    let pool = sqlx::PgPool::connect(url).await.expect("pool connect");
    sqlx::query("CREATE TABLE events (data JSONB NOT NULL)")
        .execute(&pool)
        .await
        .expect("create table");
    pool.close().await;
}

async fn jsonb_row_count(url: &str) -> i64 {
    let pool = sqlx::PgPool::connect(url).await.expect("pool connect");
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*)::BIGINT FROM events")
        .fetch_one(&pool)
        .await
        .expect("count");
    pool.close().await;
    count
}

/// Read the stored commit token for `scope` directly from the watermark table.
async fn stored_token(url: &str, scope: &str) -> Option<String> {
    let pool = sqlx::PgPool::connect(url).await.expect("pool connect");
    let sql = format!(
        "SELECT \"{COMMIT_TOKEN_TOKEN_COL}\" FROM \"{COMMIT_TOKEN_TABLE}\" \
         WHERE \"{COMMIT_TOKEN_SCOPE_COL}\" = $1"
    );
    let row = sqlx::query(&sql)
        .bind(scope)
        .fetch_optional(&pool)
        .await
        .expect("token read");
    pool.close().await;
    row.map(|r| r.get::<String, _>(0))
}

#[tokio::test(flavor = "multi_thread")]
async fn idempotent_write_persists_data_and_token_atomically() {
    let (_container, url) = start_postgres().await;
    create_jsonb_table(&url).await;

    let config = PostgresSinkConfig::new(&url, "events").with_batch_size(0);
    let sink = PostgresSink::new(config).await.expect("sink new");

    assert!(
        sink.supports_idempotent_writes(),
        "postgres sink advertises idempotent writes"
    );

    // No token committed yet for a fresh scope.
    assert_eq!(
        sink.last_committed_token("scope-a")
            .await
            .expect("token read"),
        None,
        "fresh scope has no committed token"
    );

    let token = format_token(1);
    let records = vec![json!({"k": "v1"}), json!({"k": "v2"})];
    let written = sink
        .write_batch_idempotent(&records, "scope-a", &token)
        .await
        .expect("idempotent write");
    assert_eq!(written, 2);

    // Data landed.
    assert_eq!(jsonb_row_count(&url).await, 2);
    // Token landed in the same commit and is readable both via the trait and
    // directly from the watermark table.
    assert_eq!(
        sink.last_committed_token("scope-a")
            .await
            .expect("token read")
            .as_deref(),
        Some(token.as_str())
    );
    assert_eq!(
        stored_token(&url, "scope-a").await.as_deref(),
        Some(token.as_str())
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn idempotent_write_upserts_token_on_repeat_scope() {
    let (_container, url) = start_postgres().await;
    create_jsonb_table(&url).await;

    let config = PostgresSinkConfig::new(&url, "events").with_batch_size(0);
    let sink = PostgresSink::new(config).await.expect("sink new");

    let t1 = format_token(1);
    let t2 = format_token(2);
    sink.write_batch_idempotent(&[json!({"n": 1})], "s", &t1)
        .await
        .expect("write 1");
    sink.write_batch_idempotent(&[json!({"n": 2})], "s", &t2)
        .await
        .expect("write 2");

    // Both inserts landed; the watermark advanced to the newer token (ON
    // CONFLICT DO UPDATE), not duplicated.
    assert_eq!(jsonb_row_count(&url).await, 2);
    assert_eq!(
        sink.last_committed_token("s")
            .await
            .expect("read")
            .as_deref(),
        Some(t2.as_str()),
        "watermark must advance to the latest token"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn idempotent_write_auto_map_mode_commits_data_and_token() {
    // Exercises the AutoMap branch inside write_batch_idempotent's transaction.
    let (_container, url) = start_postgres().await;
    let pool = sqlx::PgPool::connect(&url).await.expect("pool connect");
    sqlx::query("CREATE TABLE rows_t (id BIGINT, name TEXT)")
        .execute(&pool)
        .await
        .expect("create table");
    pool.close().await;

    let config = PostgresSinkConfig::new(&url, "rows_t")
        .column_mapping(PostgresColumnMapping::AutoMap)
        .with_batch_size(0);
    let sink = PostgresSink::new(config).await.expect("sink new");

    let token = format_token(5);
    let written = sink
        .write_batch_idempotent(&[json!({"id": 9, "name": "z"})], "auto-scope", &token)
        .await
        .expect("idempotent automap write");
    assert_eq!(written, 1);

    let pool = sqlx::PgPool::connect(&url).await.expect("pool connect");
    let name: String = sqlx::query_scalar("SELECT name FROM rows_t WHERE id = 9")
        .fetch_one(&pool)
        .await
        .expect("read back");
    pool.close().await;
    assert_eq!(name, "z");
    assert_eq!(
        sink.last_committed_token("auto-scope")
            .await
            .expect("read")
            .as_deref(),
        Some(token.as_str())
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn auto_map_into_missing_table_errors_with_no_columns() {
    // AutoMap discovery against a non-existent relation returns zero columns,
    // surfacing the typed "no columns or does not exist" error.
    let (_container, url) = start_postgres().await;

    // `create_table` defaults on since #580, so the sink would create the
    // table rather than fail. Pinned off here because the missing-table error
    // path is exactly what this test covers.
    let config = PostgresSinkConfig::new(&url, "does_not_exist")
        .column_mapping(PostgresColumnMapping::AutoMap)
        .with_create_table(false)
        .with_batch_size(0);
    let sink = PostgresSink::new(config).await.expect("sink new");

    let err = sink
        .write_batch(&[json!({"a": 1})])
        .await
        .expect_err("missing table must error");
    let msg = err.to_string();
    assert!(
        msg.contains("does not exist") && msg.contains("create_table"),
        "the refusal must name the missing target and the knob that would \
         create it (#580); got: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn auto_map_skips_records_with_no_matching_columns_then_noop() {
    // A record whose keys match no table column is skipped (logged warn). When
    // EVERY record is skipped, matched_rows is empty and write_batch returns 0
    // without issuing an INSERT.
    let (_container, url) = start_postgres().await;
    let pool = sqlx::PgPool::connect(&url).await.expect("pool connect");
    sqlx::query("CREATE TABLE t (id BIGINT, name TEXT)")
        .execute(&pool)
        .await
        .expect("create table");
    pool.close().await;

    let config = PostgresSinkConfig::new(&url, "t")
        .column_mapping(PostgresColumnMapping::AutoMap)
        .with_batch_size(0);
    let sink = PostgresSink::new(config).await.expect("sink new");

    // Neither key matches `id`/`name`.
    let written = sink
        .write_batch(&[json!({"nope": 1}), json!({"other": 2})])
        .await
        .expect("write");
    assert_eq!(written, 0, "all records skipped → zero written");

    let pool = sqlx::PgPool::connect(&url).await.expect("pool connect");
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*)::BIGINT FROM t")
        .fetch_one(&pool)
        .await
        .expect("count");
    pool.close().await;
    assert_eq!(count, 0, "no INSERT issued when nothing matches");
}

#[tokio::test(flavor = "multi_thread")]
async fn auto_map_non_object_record_errors() {
    // AutoMap requires JSON object records; a scalar surfaces a typed error.
    let (_container, url) = start_postgres().await;
    let pool = sqlx::PgPool::connect(&url).await.expect("pool connect");
    sqlx::query("CREATE TABLE t (id BIGINT)")
        .execute(&pool)
        .await
        .expect("create table");
    pool.close().await;

    let config = PostgresSinkConfig::new(&url, "t")
        .column_mapping(PostgresColumnMapping::AutoMap)
        .with_batch_size(0);
    let sink = PostgresSink::new(config).await.expect("sink new");

    let err = sink
        .write_batch(&[json!(42)])
        .await
        .expect_err("non-object must error");
    assert!(
        err.to_string()
            .contains("AutoMap requires JSON object records"),
        "got: {err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn check_passes_against_live_server_and_metadata_methods() {
    let (_container, url) = start_postgres().await;
    create_jsonb_table(&url).await;

    let config = PostgresSinkConfig::new(&url, "events").with_schema("public");
    let sink = PostgresSink::new(config).await.expect("sink new");

    // config_schema is a JSON object schema.
    let schema = sink.config_schema();
    assert!(schema.get("properties").is_some(), "schema: {schema}");

    // dataset_uri redacts credentials and includes the schema-qualified table.
    let uri = sink.dataset_uri();
    assert!(
        !uri.contains("postgres:postgres"),
        "credentials leaked: {uri}"
    );
    assert!(
        uri.contains("table=public.events"),
        "dataset_uri must carry the schema-qualified table: {uri}"
    );

    // check(): SELECT 1 against a reachable DB → single passing "auth" probe.
    let report = sink
        .check(&faucet_core::check::CheckContext::default())
        .await
        .expect("check");
    assert_eq!(report.probes.len(), 1);
    assert_eq!(report.probes[0].name, "auth");
    assert!(
        matches!(report.probes[0].status, ProbeStatus::Pass),
        "auth probe must pass: {:?}",
        report.probes[0].status
    );

    // write_batch with empty input is a no-op returning 0.
    assert_eq!(sink.write_batch(&[]).await.expect("empty"), 0);
    // A Value::Null record contributes nothing under JSONB unnest semantics but
    // the call still succeeds; assert the empty fast-path explicitly above.
}
