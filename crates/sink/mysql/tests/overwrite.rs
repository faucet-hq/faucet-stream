//! Integration tests for `MysqlSink`'s `write_mode: overwrite` (#492) against a
//! real MySQL instance via testcontainers.
//!
//! These tests require Docker. Each test boots its own container and seeds its
//! own table so they are fully isolated. The lifecycle under test is
//! `begin_overwrite` → N × `write_batch` → `commit_overwrite` (success) or
//! `abort_overwrite` (failure/cancel). Writes during the run land in a staging
//! table (`t__faucet_ovw`); the destination keeps its old rows until the atomic
//! `RENAME TABLE` swap, and an aborted run leaves the previous rows intact.

use faucet_core::{Sink, WriteMode, WriteSpec};
use faucet_sink_mysql::{MysqlColumnMapping, MysqlSink, MysqlSinkConfig};
use serde_json::json;
use std::sync::OnceLock;
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::mysql::Mysql;
use tokio::sync::Semaphore;

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

async fn create_table(url: &str) {
    let pool = sqlx::MySqlPool::connect(url).await.expect("pool connect");
    sqlx::query("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(255))")
        .execute(&pool)
        .await
        .expect("create table");
    pool.close().await;
}

async fn seed(url: &str, sql: &str) {
    let pool = sqlx::MySqlPool::connect(url).await.expect("pool connect");
    sqlx::query(sql).execute(&pool).await.expect("seed");
    pool.close().await;
}

async fn names_ordered(url: &str) -> Vec<String> {
    let pool = sqlx::MySqlPool::connect(url).await.expect("pool connect");
    let names: Vec<String> = sqlx::query_scalar("SELECT name FROM t ORDER BY id")
        .fetch_all(&pool)
        .await
        .expect("read back");
    pool.close().await;
    names
}

async fn table_exists(url: &str, table: &str) -> bool {
    let pool = sqlx::MySqlPool::connect(url).await.expect("pool connect");
    let n: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.tables \
         WHERE table_schema = DATABASE() AND table_name = ?",
    )
    .bind(table)
    .fetch_one(&pool)
    .await
    .expect("count");
    pool.close().await;
    n > 0
}

fn overwrite_config(url: &str) -> MysqlSinkConfig {
    let mut config = MysqlSinkConfig::new(url, "t").column_mapping(MysqlColumnMapping::AutoMap);
    config.write = WriteSpec {
        write_mode: WriteMode::Overwrite,
        key: vec![],
        delete_marker: None,
    };
    config
}

#[tokio::test(flavor = "multi_thread")]
async fn overwrite_replaces_all_rows_on_commit() {
    let (_container, url) = start_mysql().await;
    create_table(&url).await;
    seed(
        &url,
        "INSERT INTO t (id, name) VALUES (1, 'old_a'), (2, 'old_b')",
    )
    .await;

    let sink = MysqlSink::new(overwrite_config(&url))
        .await
        .expect("sink new");

    sink.begin_overwrite().await.expect("begin");
    sink.write_batch(&[json!({"id": 10, "name": "new_x"})])
        .await
        .expect("write 1");
    sink.write_batch(&[json!({"id": 11, "name": "new_y"})])
        .await
        .expect("write 2");

    // Before commit the destination still holds the OLD rows — writes staged.
    assert_eq!(names_ordered(&url).await, vec!["old_a", "old_b"]);

    sink.commit_overwrite().await.expect("commit");

    assert_eq!(names_ordered(&url).await, vec!["new_x", "new_y"]);
    assert!(
        !table_exists(&url, "t__faucet_ovw").await,
        "staging table must be gone after commit"
    );
    assert!(
        !table_exists(&url, "t__faucet_ovw_old").await,
        "old table must be dropped after commit"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn overwrite_abort_leaves_previous_data_intact() {
    let (_container, url) = start_mysql().await;
    create_table(&url).await;
    seed(
        &url,
        "INSERT INTO t (id, name) VALUES (1, 'old_a'), (2, 'old_b')",
    )
    .await;

    let sink = MysqlSink::new(overwrite_config(&url))
        .await
        .expect("sink new");

    sink.begin_overwrite().await.expect("begin");
    sink.write_batch(&[json!({"id": 99, "name": "doomed"})])
        .await
        .expect("write");
    sink.abort_overwrite().await.expect("abort");

    assert_eq!(names_ordered(&url).await, vec!["old_a", "old_b"]);
    assert!(
        !table_exists(&url, "t__faucet_ovw").await,
        "staging table must be gone after abort"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn overwrite_reported_in_supported_write_modes() {
    let (_container, url) = start_mysql().await;
    create_table(&url).await;
    let sink = MysqlSink::new(overwrite_config(&url))
        .await
        .expect("sink new");
    assert!(sink.supported_write_modes().contains(&WriteMode::Overwrite));
    assert!(sink.is_overwrite());
}

async fn target_present(url: &str) -> bool {
    let url = url.to_string();
    table_exists(&url, "t").await
}

#[tokio::test(flavor = "multi_thread")]
async fn overwrite_on_a_fresh_database_creates_then_swaps() {
    // #676. The CLI runs begin, the writes and the commit on *different* sink
    // instances, so each step gets its own sink.
    let (_container, url) = start_mysql().await;
    let sink = || MysqlSink::new(overwrite_config(&url));

    sink().await.unwrap().begin_overwrite().await.unwrap();
    sink()
        .await
        .unwrap()
        .write_batch(&[json!({"id": 1, "name": "a"}), json!({"id": 2, "name": "b"})])
        .await
        .unwrap();
    assert!(
        !target_present(&url).await,
        "a first run stages; the target appears at commit"
    );
    sink().await.unwrap().commit_overwrite().await.unwrap();
    assert_eq!(names_ordered(&url).await, vec!["a", "b"]);

    sink().await.unwrap().begin_overwrite().await.unwrap();
    sink()
        .await
        .unwrap()
        .write_batch(&[json!({"id": 3, "name": "c"})])
        .await
        .unwrap();
    assert_eq!(
        names_ordered(&url).await,
        vec!["a", "b"],
        "second run is staged"
    );
    sink().await.unwrap().commit_overwrite().await.unwrap();
    assert_eq!(names_ordered(&url).await, vec!["c"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn aborted_first_overwrite_leaves_no_table_behind() {
    let (_container, url) = start_mysql().await;
    let sink = || MysqlSink::new(overwrite_config(&url));
    sink().await.unwrap().begin_overwrite().await.unwrap();
    sink()
        .await
        .unwrap()
        .write_batch(&[json!({"id": 1, "name": "a"})])
        .await
        .unwrap();
    sink().await.unwrap().abort_overwrite().await.unwrap();
    assert!(!target_present(&url).await);
}
