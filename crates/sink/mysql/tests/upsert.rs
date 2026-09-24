//! Integration tests for `MysqlSink` upsert / delete write modes,
//! exercised against a real MySQL instance via testcontainers.
//!
//! These tests require Docker. Each test boots its own container and seeds
//! its own table so they are fully isolated and safe to run in parallel.
//!
//! Each test creates `CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(255))`
//! and exercises a write-mode scenario end-to-end via `write_batch`.

use faucet_core::{Sink, WriteMode, WriteSpec};
use faucet_sink_mysql::{MysqlColumnMapping, MysqlSink, MysqlSinkConfig};
use serde_json::json;
use sqlx::Row;
use std::sync::OnceLock;
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::mysql::Mysql;
use tokio::sync::Semaphore;

/// Bounds concurrent MySQL container startups across all tests in this
/// binary. MySQL 8.x init is heavy (~2-3 GB RSS per container during
/// startup) and starting too many in parallel exhausts memory on
/// Colima/Docker Desktop, surfacing as random "Failed to start mysqld
/// daemon" errors. We allow at most two simultaneous startups; once a
/// container is running it is steady-state cheap, so the cap only
/// serialises the spin-up window.
fn startup_limit() -> &'static Semaphore {
    static SEM: OnceLock<Semaphore> = OnceLock::new();
    SEM.get_or_init(|| Semaphore::new(2))
}

/// Start a MySQL container and return both the container handle and a
/// connection URL. The container is kept alive by the returned handle; drop
/// it to stop the container.
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

/// Create `CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(255))`.
async fn create_upsert_table(url: &str) {
    let pool = sqlx::MySqlPool::connect(url).await.expect("pool connect");
    sqlx::query("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(255))")
        .execute(&pool)
        .await
        .expect("create table");
    pool.close().await;
}

/// Build an upsert sink for the given URL targeting table `t`.
fn make_upsert_sink_config(url: &str, key: Vec<String>) -> MysqlSinkConfig {
    let mut config = MysqlSinkConfig::new(url, "t").column_mapping(MysqlColumnMapping::AutoMap);
    config.write = WriteSpec {
        write_mode: WriteMode::Upsert,
        key,
        delete_marker: None,
    };
    config
}

// ---------------------------------------------------------------------------
// Test 1: second upsert with same key updates the row (last-write-wins)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn upsert_second_write_updates_existing_row() {
    let (_container, url) = start_mysql().await;
    create_upsert_table(&url).await;

    let config = make_upsert_sink_config(&url, vec!["id".to_string()]);
    let sink = MysqlSink::new(config).await.expect("sink new");

    // Insert {id:1, name:"alice"}.
    sink.write_batch(&[json!({"id": 1, "name": "alice"})])
        .await
        .expect("first write");

    // Upsert {id:1, name:"alice2"} — must update the row.
    sink.write_batch(&[json!({"id": 1, "name": "alice2"})])
        .await
        .expect("second write");

    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool");
    let count: i64 = sqlx::query("SELECT COUNT(*) FROM t")
        .fetch_one(&pool)
        .await
        .expect("count")
        .get(0);
    let name: String = sqlx::query("SELECT name FROM t WHERE id = 1")
        .fetch_one(&pool)
        .await
        .expect("row")
        .get("name");
    pool.close().await;

    assert_eq!(count, 1, "upsert must not duplicate the row");
    assert_eq!(name, "alice2", "upsert must update the name to 'alice2'");
}

// ---------------------------------------------------------------------------
// Test 2: delete_marker routes a row to delete; the table ends up empty
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn upsert_with_delete_marker_removes_row() {
    let (_container, url) = start_mysql().await;
    create_upsert_table(&url).await;

    // Upsert sink with a delete_marker on __op = "d".
    let mut config = MysqlSinkConfig::new(&url, "t").column_mapping(MysqlColumnMapping::AutoMap);
    config.write = WriteSpec {
        write_mode: WriteMode::Upsert,
        key: vec!["id".to_string()],
        delete_marker: Some(faucet_core::DeleteMarker {
            field: "__op".to_string(),
            values: vec!["d".to_string()],
        }),
    };
    let sink = MysqlSink::new(config).await.expect("sink new");

    // Insert the row first (marker field stripped from upsert).
    sink.write_batch(&[json!({"id": 1, "name": "x", "__op": "u"})])
        .await
        .expect("insert");

    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool");
    let count_after_insert: i64 = sqlx::query("SELECT COUNT(*) FROM t")
        .fetch_one(&pool)
        .await
        .expect("count")
        .get(0);
    pool.close().await;
    assert_eq!(count_after_insert, 1, "row must be present after upsert");

    // Delete via the marker.
    sink.write_batch(&[json!({"id": 1, "__op": "d"})])
        .await
        .expect("delete");

    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool");
    let count_after_delete: i64 = sqlx::query("SELECT COUNT(*) FROM t")
        .fetch_one(&pool)
        .await
        .expect("count")
        .get(0);
    pool.close().await;

    assert_eq!(
        count_after_delete, 0,
        "row must be deleted after delete-marker write"
    );
}

// ---------------------------------------------------------------------------
// Test 3: same key twice in one batch → last-write-wins (count == 1, name == "new")
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn upsert_same_key_twice_in_one_batch_last_write_wins() {
    let (_container, url) = start_mysql().await;
    create_upsert_table(&url).await;

    let config = make_upsert_sink_config(&url, vec!["id".to_string()]);
    let sink = MysqlSink::new(config).await.expect("sink new");

    // Two records with the same id in a single batch; the planner deduplicates
    // them so MySQL sees only the last one.
    sink.write_batch(&[
        json!({"id": 1, "name": "old"}),
        json!({"id": 1, "name": "new"}),
    ])
    .await
    .expect("batch write");

    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool");
    let count: i64 = sqlx::query("SELECT COUNT(*) FROM t")
        .fetch_one(&pool)
        .await
        .expect("count")
        .get(0);
    let name: String = sqlx::query("SELECT name FROM t WHERE id = 1")
        .fetch_one(&pool)
        .await
        .expect("row")
        .get("name");
    pool.close().await;

    assert_eq!(
        count, 1,
        "dedup must collapse two records with the same key to one row"
    );
    assert_eq!(name, "new", "last-write-wins: name must be 'new'");
}

// ---------------------------------------------------------------------------
// Test 4: supported_write_modes includes Append, Upsert, Delete
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn supported_write_modes_includes_upsert_and_delete() {
    let (_container, url) = start_mysql().await;
    create_upsert_table(&url).await;

    let config = MysqlSinkConfig::new(&url, "t");
    let sink = MysqlSink::new(config).await.expect("sink new");

    let modes = sink.supported_write_modes();
    assert!(modes.contains(&WriteMode::Append));
    assert!(modes.contains(&WriteMode::Upsert));
    assert!(modes.contains(&WriteMode::Delete));
}

// ---------------------------------------------------------------------------
// Test 5: config validation rejects upsert without a key (no container needed;
//         the error fires before the connection attempt).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn new_rejects_upsert_without_key() {
    let mut config = MysqlSinkConfig::new("mysql://root@127.0.0.1:13306/test", "t")
        .column_mapping(MysqlColumnMapping::AutoMap);
    config.write = WriteSpec {
        write_mode: WriteMode::Upsert,
        key: vec![], // missing key → rejected before any connection attempt
        delete_marker: None,
    };

    let err = MysqlSink::new(config)
        .await
        .err()
        .expect("must fail without key");
    assert!(err.to_string().contains("non-empty `key`"), "got: {err}");
}

// ---------------------------------------------------------------------------
// Test 6: config validation rejects upsert with json column_mapping (no
//         container needed; the error fires before the connection attempt).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn new_rejects_upsert_with_json_column_mapping() {
    // Default column_mapping is Json — upsert must be rejected.
    let mut config = MysqlSinkConfig::new("mysql://root@127.0.0.1:13306/test", "t");
    config.write = WriteSpec {
        write_mode: WriteMode::Upsert,
        key: vec!["id".to_string()],
        delete_marker: None,
    };

    let err = MysqlSink::new(config)
        .await
        .err()
        .expect("must fail with json column_mapping");
    assert!(
        err.to_string().contains("auto_map"),
        "error must mention auto_map; got: {err}"
    );
}

// ---------------------------------------------------------------------------
// Test 7: write_batch_partial routes missing-key rows to the DLQ per-row
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn write_batch_partial_routes_missing_key_per_row() {
    let (_container, url) = start_mysql().await;
    create_upsert_table(&url).await;

    let config = make_upsert_sink_config(&url, vec!["id".to_string()]);
    let sink = MysqlSink::new(config).await.expect("sink new");

    let records = [
        json!({"id": 1, "name": "ok"}),
        json!({"name": "missing-id"}),
    ];
    let outcomes = sink
        .write_batch_partial(&records)
        .await
        .expect("partial write");

    assert_eq!(outcomes.len(), 2, "one outcome per input row");
    assert!(outcomes[0].is_ok(), "the good row must be Ok");
    assert!(
        outcomes[1].is_err(),
        "the missing-key row must be Err (routed to the DLQ)"
    );

    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool");
    let count: i64 = sqlx::query("SELECT COUNT(*) FROM t")
        .fetch_one(&pool)
        .await
        .expect("count")
        .get(0);
    let name: String = sqlx::query("SELECT name FROM t WHERE id = 1")
        .fetch_one(&pool)
        .await
        .expect("row")
        .get("name");
    pool.close().await;

    assert_eq!(count, 1, "only the good row should be written");
    assert_eq!(name, "ok", "id=1 must be present with name 'ok'");
}

#[tokio::test(flavor = "multi_thread")]
async fn upsert_on_a_fresh_database_creates_a_keyed_table_and_dedups() {
    // #676: the auto-created table carries PRIMARY KEY on a text key (typed
    // VARCHAR(191), since MySQL cannot index LONGTEXT), and the second sink's
    // unique-index check passes against it.
    let (_container, url) = start_mysql().await;
    let first = MysqlSink::new(make_upsert_sink_config(&url, vec!["sku".into()]))
        .await
        .unwrap();
    first
        .write_batch(&[json!({"sku": "a", "qty": 1}), json!({"sku": "b", "qty": 2})])
        .await
        .unwrap();
    let second = MysqlSink::new(make_upsert_sink_config(&url, vec!["sku".into()]))
        .await
        .unwrap();
    second
        .write_batch(&[
            json!({"sku": "a", "qty": 10}),
            json!({"sku": "c", "qty": 3}),
        ])
        .await
        .unwrap();
    let pool = sqlx::MySqlPool::connect(&url).await.unwrap();
    let rows: Vec<(String, i64)> = sqlx::query_as("SELECT sku, qty FROM t ORDER BY sku")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![("a".into(), 10), ("b".into(), 2), ("c".into(), 3)]
    );
}
