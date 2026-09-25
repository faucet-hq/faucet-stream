//! Integration tests for the MySQL sink's scoped cleanup (#478), exercised
//! against a real MySQL instance via testcontainers.
//!
//! These tests require Docker. Each test boots its own container and seeds its
//! own table so they are fully isolated and safe to run in parallel.
//!
//! Each test seeds rows in two scopes, runs `cleanup_scope` for one of them, and
//! asserts that only the stale rows *inside* that scope were removed — the rows
//! of the other scope are the canary for an over-broad predicate.

use faucet_core::{SeenKeys, Sink, WriteMode, WriteSpec};
use faucet_sink_mysql::{MysqlColumnMapping, MysqlSink, MysqlSinkConfig};
use serde_json::{Value, json};
use sqlx::Row;
use std::collections::BTreeMap;
use std::sync::OnceLock;
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::mysql::Mysql;
use tokio::sync::Semaphore;

/// Bounds concurrent MySQL container startups across all tests in this binary —
/// see the same helper in `upsert.rs` for why.
fn startup_limit() -> &'static Semaphore {
    static SEM: OnceLock<Semaphore> = OnceLock::new();
    SEM.get_or_init(|| Semaphore::new(2))
}

/// Start a MySQL container and return both the container handle and a
/// connection URL. Drop the handle to stop the container.
async fn start_mysql() -> (ContainerAsync<Mysql>, String) {
    let _permit = startup_limit()
        .acquire()
        .await
        .expect("startup semaphore closed");
    let container: ContainerAsync<Mysql> = Mysql::default()
        .start()
        .await
        .expect("mysql container start");
    let port = container
        .get_host_port_ipv4(3306)
        .await
        .expect("mysql port");
    let url = format!("mysql://root@127.0.0.1:{port}/test");
    (container, url)
}

/// Create `assoc` with a composite primary key and seed it.
async fn create_and_seed(url: &str, rows: &[(i64, i64)]) {
    let pool = sqlx::MySqlPool::connect(url).await.expect("pool connect");
    sqlx::query(
        "CREATE TABLE assoc (
             contact_id INT NOT NULL,
             id         INT NOT NULL,
             label      VARCHAR(255),
             PRIMARY KEY (contact_id, id)
         )",
    )
    .execute(&pool)
    .await
    .expect("create table");
    for (contact_id, id) in rows {
        sqlx::query("INSERT INTO assoc (contact_id, id, label) VALUES (?, ?, 'x')")
            .bind(contact_id)
            .bind(id)
            .execute(&pool)
            .await
            .expect("seed");
    }
    pool.close().await;
}

/// All `(contact_id, id)` pairs currently in the table, sorted.
async fn remaining(url: &str) -> Vec<(i32, i32)> {
    let pool = sqlx::MySqlPool::connect(url).await.expect("pool connect");
    let rows = sqlx::query("SELECT contact_id, id FROM assoc ORDER BY contact_id, id")
        .fetch_all(&pool)
        .await
        .expect("select");
    pool.close().await;
    rows.iter()
        .map(|r| (r.get::<i32, _>("contact_id"), r.get::<i32, _>("id")))
        .collect()
}

fn cleanup_sink_config(url: &str) -> MysqlSinkConfig {
    let mut config = MysqlSinkConfig::new(url, "assoc").column_mapping(MysqlColumnMapping::AutoMap);
    config.write = WriteSpec {
        write_mode: WriteMode::Upsert,
        key: vec!["contact_id".to_string(), "id".to_string()],
        delete_marker: None,
        rollback: None,
    };
    config
}

/// A `SeenKeys` holding the keys of `page`, as the pipeline would accumulate them.
fn seen(page: &[Value]) -> SeenKeys {
    let key = vec!["contact_id".to_string(), "id".to_string()];
    let mut seen = SeenKeys::new();
    seen.record_page(page, &key, 100_000);
    seen
}

fn scope(contact_id: i64) -> BTreeMap<String, Value> {
    BTreeMap::from([("contact_id".to_string(), json!(contact_id))])
}

// ---------------------------------------------------------------------------
// Test 1: rows in the scope that this run did not write are deleted; the
// written ones and every other scope survive
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn cleanup_deletes_only_unwritten_rows_inside_the_scope() {
    let (_container, url) = start_mysql().await;
    create_and_seed(&url, &[(1, 10), (1, 11), (1, 12), (2, 20)]).await;

    let sink = MysqlSink::new(cleanup_sink_config(&url))
        .await
        .expect("sink new");

    let written = [json!({"contact_id": 1, "id": 10})];
    let deleted = sink
        .cleanup_scope(&scope(1), &seen(&written))
        .await
        .expect("cleanup");

    assert_eq!(deleted, 2, "the two stale rows of contact 1");
    assert_eq!(
        remaining(&url).await,
        vec![(1, 10), (2, 20)],
        "contact 2 is outside the claimed scope and must be untouched"
    );
}

// ---------------------------------------------------------------------------
// Test 2: an empty written-key set is NOT a no-op — it means the source
// reported the scope as empty, so the whole scope is stale. This is the
// motivating case for the feature.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn cleanup_with_empty_seen_set_deletes_the_whole_scope() {
    let (_container, url) = start_mysql().await;
    create_and_seed(&url, &[(1, 10), (1, 11), (2, 20)]).await;

    let sink = MysqlSink::new(cleanup_sink_config(&url))
        .await
        .expect("sink new");

    let deleted = sink
        .cleanup_scope(&scope(1), &SeenKeys::new())
        .await
        .expect("cleanup");

    assert_eq!(deleted, 2);
    assert_eq!(remaining(&url).await, vec![(2, 20)]);

    // A second cleanup on the same sink must work too — the session-scoped
    // temporary key table must not leak across invocations.
    let deleted = sink
        .cleanup_scope(&scope(2), &seen(&[json!({"contact_id": 2, "id": 20})]))
        .await
        .expect("second cleanup");
    assert_eq!(deleted, 0);
    assert_eq!(remaining(&url).await, vec![(2, 20)]);
}

// ---------------------------------------------------------------------------
// Test 3: an unknown scope column is a clear error, and nothing is deleted
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn cleanup_with_unknown_scope_column_errors_and_deletes_nothing() {
    let (_container, url) = start_mysql().await;
    create_and_seed(&url, &[(1, 10), (2, 20)]).await;

    let sink = MysqlSink::new(cleanup_sink_config(&url))
        .await
        .expect("sink new");

    let bad = BTreeMap::from([("owner_id".to_string(), json!(1))]);
    let err = sink
        .cleanup_scope(&bad, &SeenKeys::new())
        .await
        .expect_err("unknown column must be refused");
    let msg = err.to_string();
    assert!(msg.contains("owner_id"), "{msg}");
    assert!(msg.contains("assoc"), "{msg}");

    assert_eq!(
        remaining(&url).await,
        vec![(1, 10), (2, 20)],
        "a refused cleanup must not delete anything"
    );
}
