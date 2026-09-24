//! Integration tests for [`PostgresSink`]'s `write_mode: overwrite` (#492)
//! against a real Postgres instance via testcontainers.
//!
//! These tests require Docker. Each test boots its own container so they are
//! fully isolated and safe to run in parallel. The lifecycle under test is
//! `begin_overwrite` → N × `write_batch` → `commit_overwrite` (success) or
//! `abort_overwrite` (failure/cancel). The central guarantee: writes during the
//! run land in a staging table, so the destination keeps its old rows until the
//! atomic swap — and an aborted run leaves the previous rows fully intact.

use faucet_core::{OverwriteScope, Sink, WriteMode, WriteSpec};
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

async fn seed(url: &str, sql: &str) {
    let pool = sqlx::PgPool::connect(url).await.expect("pool connect");
    sqlx::query(sql).execute(&pool).await.expect("seed");
    pool.close().await;
}

async fn names_ordered(url: &str) -> Vec<String> {
    let pool = sqlx::PgPool::connect(url).await.expect("pool connect");
    let names: Vec<String> = sqlx::query_scalar("SELECT name FROM kv ORDER BY id")
        .fetch_all(&pool)
        .await
        .expect("read back");
    pool.close().await;
    names
}

async fn staging_exists(url: &str) -> bool {
    let pool = sqlx::PgPool::connect(url).await.expect("pool connect");
    let present: Option<String> = sqlx::query_scalar("SELECT to_regclass('kv__faucet_ovw')::text")
        .fetch_one(&pool)
        .await
        .expect("regclass");
    pool.close().await;
    present.is_some()
}

fn overwrite_config(url: &str) -> PostgresSinkConfig {
    let mut config = PostgresSinkConfig::new(url, "kv")
        .column_mapping(PostgresColumnMapping::AutoMap)
        .with_batch_size(0);
    config.write = WriteSpec {
        write_mode: WriteMode::Overwrite,
        key: vec![],
        delete_marker: None,
    };
    config
}

#[tokio::test(flavor = "multi_thread")]
async fn overwrite_replaces_all_rows_on_commit() {
    let (_container, url) = start_postgres().await;
    create_kv_table(&url).await;
    seed(
        &url,
        "INSERT INTO kv (id, name) VALUES (1, 'old_a'), (2, 'old_b')",
    )
    .await;

    let sink = PostgresSink::new(overwrite_config(&url))
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
        !staging_exists(&url).await,
        "staging table must be dropped after commit"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn scoped_overwrite_replaces_only_in_window_rows() {
    let (_container, url) = start_postgres().await;
    create_kv_table(&url).await;
    // id 1 is out of the [10, 20) window (kept); id 15 is in-window (replaced).
    seed(
        &url,
        "INSERT INTO kv (id, name) VALUES (1, 'keep'), (15, 'old_in')",
    )
    .await;

    let mut config = overwrite_config(&url);
    config.scope = Some(OverwriteScope::Window {
        column: "id".into(),
        from: json!(10),
        to: json!(20),
    });
    let sink = PostgresSink::new(config).await.expect("sink new");

    sink.begin_overwrite().await.expect("begin");
    sink.write_batch(&[
        json!({"id": 15, "name": "new_in"}),
        json!({"id": 16, "name": "new_in2"}),
    ])
    .await
    .expect("write");

    // Before commit the destination still holds the OLD rows.
    assert_eq!(names_ordered(&url).await, vec!["keep", "old_in"]);

    sink.commit_overwrite().await.expect("commit");

    // Out-of-window id 1 preserved; in-window id 15 replaced; id 16 added.
    assert_eq!(names_ordered(&url).await, vec!["keep", "new_in", "new_in2"]);
    assert!(!staging_exists(&url).await, "staging dropped after commit");
}

#[tokio::test(flavor = "multi_thread")]
async fn overwrite_abort_leaves_previous_data_intact() {
    let (_container, url) = start_postgres().await;
    create_kv_table(&url).await;
    seed(
        &url,
        "INSERT INTO kv (id, name) VALUES (1, 'old_a'), (2, 'old_b')",
    )
    .await;

    let sink = PostgresSink::new(overwrite_config(&url))
        .await
        .expect("sink new");

    sink.begin_overwrite().await.expect("begin");
    sink.write_batch(&[json!({"id": 99, "name": "doomed"})])
        .await
        .expect("write");
    sink.abort_overwrite().await.expect("abort");

    assert_eq!(names_ordered(&url).await, vec!["old_a", "old_b"]);
    assert!(
        !staging_exists(&url).await,
        "staging table must be dropped after abort"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn overwrite_reported_in_supported_write_modes() {
    let (_container, url) = start_postgres().await;
    create_kv_table(&url).await;
    let sink = PostgresSink::new(overwrite_config(&url))
        .await
        .expect("sink new");
    assert!(sink.supported_write_modes().contains(&WriteMode::Overwrite));
    assert!(sink.is_overwrite());
}

async fn target_present(url: &str) -> bool {
    let url = url.to_string();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    let present: Option<String> = sqlx::query_scalar("SELECT to_regclass('kv')::text")
        .fetch_one(&pool)
        .await
        .unwrap();
    pool.close().await;
    present.is_some()
}

#[tokio::test(flavor = "multi_thread")]
async fn overwrite_on_a_fresh_database_creates_then_swaps() {
    // #676. The CLI runs begin, the writes and the commit on *different* sink
    // instances, so each step gets its own sink.
    let (_container, url) = start_postgres().await;
    let sink = || PostgresSink::new(overwrite_config(&url));

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
    let (_container, url) = start_postgres().await;
    let sink = || PostgresSink::new(overwrite_config(&url));
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
