//! Integration tests for [`MysqlSink`]'s exactly-once / idempotent write path,
//! its AutoMap edge/error branches, native-type binding edge cases, and the
//! preflight `check()`, against a real MySQL instance via testcontainers.
//!
//! These tests require Docker. Each test boots its own container and seeds its
//! own table so they are fully isolated and safe to run in parallel.

use faucet_core::Sink;
use faucet_core::check::ProbeStatus;
use faucet_core::idempotency::{
    COMMIT_TOKEN_SCOPE_COL, COMMIT_TOKEN_TABLE, COMMIT_TOKEN_TOKEN_COL, format_token,
};
use faucet_sink_mysql::{MysqlColumnMapping, MysqlSink, MysqlSinkConfig};
use serde_json::{Value, json};
use sqlx::Row;
use std::sync::OnceLock;
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::mysql::Mysql;
use tokio::sync::Semaphore;

/// Bounds concurrent MySQL container startups (see `batching.rs` for rationale).
fn startup_limit() -> &'static Semaphore {
    static SEM: OnceLock<Semaphore> = OnceLock::new();
    SEM.get_or_init(|| Semaphore::new(2))
}

async fn start_mysql() -> (ContainerAsync<Mysql>, String) {
    let _permit = startup_limit()
        .acquire()
        .await
        .expect("startup semaphore closed");
    let image = Mysql::default();
    let container: ContainerAsync<Mysql> = image.start().await.expect("mysql container start");
    let port = container
        .get_host_port_ipv4(3306)
        .await
        .expect("mysql port");
    let url = format!("mysql://root@127.0.0.1:{port}/test");
    (container, url)
}

async fn create_json_table(url: &str) {
    let pool = sqlx::MySqlPool::connect(url).await.expect("pool connect");
    sqlx::query("CREATE TABLE events (id BIGINT AUTO_INCREMENT PRIMARY KEY, data JSON NOT NULL)")
        .execute(&pool)
        .await
        .expect("create table");
    pool.close().await;
}

async fn count_events(url: &str) -> i64 {
    let pool = sqlx::MySqlPool::connect(url).await.expect("verify pool");
    let count: i64 = sqlx::query("SELECT COUNT(*) FROM events")
        .fetch_one(&pool)
        .await
        .expect("select count")
        .get(0);
    pool.close().await;
    count
}

/// Read the stored commit token for `scope` directly from the watermark table.
async fn stored_token(url: &str, scope: &str) -> Option<String> {
    let pool = sqlx::MySqlPool::connect(url).await.expect("pool connect");
    let sql = format!(
        "SELECT `{COMMIT_TOKEN_TOKEN_COL}` FROM `{COMMIT_TOKEN_TABLE}` \
         WHERE `{COMMIT_TOKEN_SCOPE_COL}` = ?"
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
    let (_container, url) = start_mysql().await;
    create_json_table(&url).await;

    let config = MysqlSinkConfig::new(&url, "events")
        .column_mapping(MysqlColumnMapping::Json {
            column: "data".into(),
        })
        .with_batch_size(0);
    let sink = MysqlSink::new(config).await.expect("sink new");

    assert!(
        sink.supports_idempotent_writes(),
        "mysql sink advertises idempotent writes"
    );
    assert_eq!(
        sink.last_committed_token("scope-a").await.expect("read"),
        None,
        "fresh scope has no committed token"
    );

    let token = format_token(1);
    let written = sink
        .write_batch_idempotent(&[json!({"k": 1}), json!({"k": 2})], "scope-a", &token)
        .await
        .expect("idempotent write");
    assert_eq!(written, 2);
    assert_eq!(count_events(&url).await, 2);
    assert_eq!(
        sink.last_committed_token("scope-a")
            .await
            .expect("read")
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
    let (_container, url) = start_mysql().await;
    create_json_table(&url).await;

    let config = MysqlSinkConfig::new(&url, "events")
        .column_mapping(MysqlColumnMapping::Json {
            column: "data".into(),
        })
        .with_batch_size(0);
    let sink = MysqlSink::new(config).await.expect("sink new");

    let t1 = format_token(1);
    let t2 = format_token(2);
    sink.write_batch_idempotent(&[json!({"n": 1})], "s", &t1)
        .await
        .expect("write 1");
    sink.write_batch_idempotent(&[json!({"n": 2})], "s", &t2)
        .await
        .expect("write 2");

    assert_eq!(count_events(&url).await, 2);
    assert_eq!(
        sink.last_committed_token("s")
            .await
            .expect("read")
            .as_deref(),
        Some(t2.as_str()),
        "ON DUPLICATE KEY UPDATE must advance the watermark"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn idempotent_write_auto_map_mode_commits_data_and_token() {
    let (_container, url) = start_mysql().await;
    {
        let pool = sqlx::MySqlPool::connect(&url).await.expect("pool");
        sqlx::query("CREATE TABLE rows_t (id BIGINT, name VARCHAR(32))")
            .execute(&pool)
            .await
            .expect("create table");
        pool.close().await;
    }

    let config = MysqlSinkConfig::new(&url, "rows_t")
        .column_mapping(MysqlColumnMapping::AutoMap)
        .with_batch_size(0);
    let sink = MysqlSink::new(config).await.expect("sink new");

    let token = format_token(7);
    let written = sink
        .write_batch_idempotent(&[json!({"id": 3, "name": "q"})], "auto", &token)
        .await
        .expect("idempotent automap write");
    assert_eq!(written, 1);

    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool");
    let name: String = sqlx::query("SELECT name FROM rows_t WHERE id = 3")
        .fetch_one(&pool)
        .await
        .expect("read back")
        .get("name");
    pool.close().await;
    assert_eq!(name, "q");
    assert_eq!(
        sink.last_committed_token("auto")
            .await
            .expect("read")
            .as_deref(),
        Some(token.as_str())
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn auto_map_into_missing_table_errors_with_no_columns() {
    let (_container, url) = start_mysql().await;

    // `create_table` defaults on since #580, so the sink would create the
    // table rather than fail. Pinned off here because the missing-table error
    // path is exactly what this test covers.
    let config = MysqlSinkConfig::new(&url, "does_not_exist")
        .column_mapping(MysqlColumnMapping::AutoMap)
        .with_create_table(false);
    let sink = MysqlSink::new(config).await.expect("sink new");

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
    let (_container, url) = start_mysql().await;
    {
        let pool = sqlx::MySqlPool::connect(&url).await.expect("pool");
        sqlx::query("CREATE TABLE t (id BIGINT, name VARCHAR(32))")
            .execute(&pool)
            .await
            .expect("create table");
        pool.close().await;
    }

    let config = MysqlSinkConfig::new(&url, "t").column_mapping(MysqlColumnMapping::AutoMap);
    let sink = MysqlSink::new(config).await.expect("sink new");

    let written = sink
        .write_batch(&[json!({"nope": 1}), json!({"other": 2})])
        .await
        .expect("write");
    assert_eq!(written, 0, "all records skipped → zero written");

    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool");
    let count: i64 = sqlx::query("SELECT COUNT(*) FROM t")
        .fetch_one(&pool)
        .await
        .expect("count")
        .get(0);
    pool.close().await;
    assert_eq!(count, 0, "no INSERT issued when nothing matches");
}

#[tokio::test(flavor = "multi_thread")]
async fn auto_map_non_object_record_errors() {
    let (_container, url) = start_mysql().await;
    {
        let pool = sqlx::MySqlPool::connect(&url).await.expect("pool");
        sqlx::query("CREATE TABLE t (id BIGINT)")
            .execute(&pool)
            .await
            .expect("create table");
        pool.close().await;
    }

    let config = MysqlSinkConfig::new(&url, "t").column_mapping(MysqlColumnMapping::AutoMap);
    let sink = MysqlSink::new(config).await.expect("sink new");

    let err = sink
        .write_batch(&[json!("scalar")])
        .await
        .expect_err("non-object must error");
    assert!(
        err.to_string()
            .contains("AutoMap requires JSON object records"),
        "got: {err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn auto_map_binds_float_and_nested_json_as_text() {
    // Covers the Number-as-f64 binding branch and the array/object branch:
    //  - a fractional number binds via the native f64 path,
    //  - an object/array has no scalar SQL form so its JSON text is bound.
    let (_container, url) = start_mysql().await;
    {
        let pool = sqlx::MySqlPool::connect(&url).await.expect("pool");
        sqlx::query("CREATE TABLE t (amount DOUBLE, nested JSON, list JSON)")
            .execute(&pool)
            .await
            .expect("create table");
        pool.close().await;
    }

    let config = MysqlSinkConfig::new(&url, "t").column_mapping(MysqlColumnMapping::AutoMap);
    let sink = MysqlSink::new(config).await.expect("sink new");

    let records = vec![json!({
        "amount": 1234.5,
        "nested": {"a": [1, 2]},
        "list": [1, 2, 3]
    })];
    assert_eq!(sink.write_batch(&records).await.expect("write"), 1);

    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool");
    let row = sqlx::query("SELECT amount, nested, list FROM t")
        .fetch_one(&pool)
        .await
        .expect("read back");
    let amount: f64 = row.get("amount");
    // The JSON columns round-trip the object/array as parsed JSON.
    let nested: Value = row.get("nested");
    let list: Value = row.get("list");
    pool.close().await;

    assert!(
        (amount - 1234.5).abs() < 1e-9,
        "fractional number must bind via the native f64 path; got {amount}"
    );
    assert_eq!(
        nested,
        json!({"a": [1, 2]}),
        "nested object must be stored as JSON text and parse back"
    );
    assert_eq!(
        list,
        json!([1, 2, 3]),
        "array must be stored as JSON text and parse back"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn check_passes_against_live_server_and_metadata_methods() {
    let (_container, url) = start_mysql().await;
    create_json_table(&url).await;

    let config = MysqlSinkConfig::new(&url, "events");
    let sink = MysqlSink::new(config).await.expect("sink new");

    let schema = sink.config_schema();
    assert!(schema.get("properties").is_some(), "schema: {schema}");

    let uri = sink.dataset_uri();
    assert!(
        uri.contains("table=events"),
        "dataset_uri must carry the table: {uri}"
    );

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

    assert_eq!(sink.write_batch(&[]).await.expect("empty"), 0);
}
