//! Integration tests for the SQLite sink's `write_mode: overwrite` (#492).
//!
//! Exercised end-to-end against a tempfile-backed SQLite database (no Docker).
//! The lifecycle the pipeline drives is `begin_overwrite` → N × `write_batch`
//! → `commit_overwrite` (success) or `abort_overwrite` (failure/cancel). The
//! central guarantees under test:
//!   * writes during the run land in staging, so the destination keeps its old
//!     rows until the swap;
//!   * a successful commit replaces the destination with exactly this run's rows;
//!   * an aborted run leaves the previous destination completely intact.

use faucet_core::{Sink, WriteMode, WriteSpec};
use faucet_sink_sqlite::{SqliteColumnMapping, SqliteSink, SqliteSinkConfig};
use serde_json::{Value, json};
use sqlx::Row;
use sqlx::sqlite::SqlitePoolOptions;
use tempfile::TempDir;

async fn fresh_db(create_sql: &str) -> (TempDir, String) {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("test.db");
    let url = format!("sqlite://{}?mode=rwc", path.display());
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("connect");
    sqlx::query(create_sql)
        .execute(&pool)
        .await
        .expect("create table");
    pool.close().await;
    (dir, url)
}

async fn seed(url: &str, sql: &str) {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .expect("connect");
    sqlx::query(sql).execute(&pool).await.expect("seed");
    pool.close().await;
}

async fn names(url: &str, table: &str) -> Vec<String> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .expect("connect");
    let rows = sqlx::query(&format!("SELECT name FROM {table} ORDER BY id"))
        .fetch_all(&pool)
        .await
        .expect("select");
    pool.close().await;
    rows.iter().map(|r| r.get::<String, _>("name")).collect()
}

async fn table_exists(url: &str, table: &str) -> bool {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .expect("connect");
    let row =
        sqlx::query("SELECT COUNT(*) AS n FROM sqlite_master WHERE type='table' AND name = ?")
            .bind(table)
            .fetch_one(&pool)
            .await
            .expect("query");
    let n: i64 = row.get("n");
    pool.close().await;
    n > 0
}

fn overwrite_config(url: &str, mapping: SqliteColumnMapping) -> SqliteSinkConfig {
    SqliteSinkConfig {
        database_url: url.to_string(),
        table_name: "users".to_string(),
        column_mapping: mapping,
        batch_size: 1000,
        max_connections: 1,
        create_table: true,
        write: WriteSpec {
            write_mode: WriteMode::Overwrite,
            key: vec![],
            delete_marker: None,
        },
    }
}

#[tokio::test]
async fn overwrite_replaces_all_rows_on_commit() {
    let (_dir, url) = fresh_db("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").await;
    seed(
        &url,
        "INSERT INTO users (id, name) VALUES (1, 'old_a'), (2, 'old_b')",
    )
    .await;

    let sink = SqliteSink::new(overwrite_config(&url, SqliteColumnMapping::AutoMap))
        .await
        .unwrap();

    sink.begin_overwrite().await.unwrap();
    // Two pages worth of new data go into staging.
    sink.write_batch(&[json!({"id": 10, "name": "new_x"})])
        .await
        .unwrap();
    sink.write_batch(&[json!({"id": 11, "name": "new_y"})])
        .await
        .unwrap();

    // Before commit, the destination still holds the OLD rows — writes were staged.
    assert_eq!(names(&url, "users").await, vec!["old_a", "old_b"]);

    sink.commit_overwrite().await.unwrap();

    // After commit, the destination is exactly this run's rows.
    assert_eq!(names(&url, "users").await, vec!["new_x", "new_y"]);
    assert!(
        !table_exists(&url, "users__faucet_ovw").await,
        "staging table must be dropped after commit"
    );
}

#[tokio::test]
async fn overwrite_abort_leaves_previous_data_intact() {
    let (_dir, url) = fresh_db("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").await;
    seed(
        &url,
        "INSERT INTO users (id, name) VALUES (1, 'old_a'), (2, 'old_b')",
    )
    .await;

    let sink = SqliteSink::new(overwrite_config(&url, SqliteColumnMapping::AutoMap))
        .await
        .unwrap();

    sink.begin_overwrite().await.unwrap();
    sink.write_batch(&[json!({"id": 99, "name": "doomed"})])
        .await
        .unwrap();
    // Simulate a mid-run failure/cancel: abort instead of commit.
    sink.abort_overwrite().await.unwrap();

    // The prior destination is completely untouched.
    assert_eq!(names(&url, "users").await, vec!["old_a", "old_b"]);
    assert!(
        !table_exists(&url, "users__faucet_ovw").await,
        "staging table must be dropped after abort"
    );
}

#[tokio::test]
async fn overwrite_works_in_json_column_mode() {
    let (_dir, url) =
        fresh_db("CREATE TABLE users (id INTEGER PRIMARY KEY, data TEXT NOT NULL)").await;
    seed(
        &url,
        r#"INSERT INTO users (id, data) VALUES (1, '{"v":"old"}')"#,
    )
    .await;

    let mapping = SqliteColumnMapping::Json {
        column: "data".into(),
    };
    let sink = SqliteSink::new(overwrite_config(&url, mapping))
        .await
        .unwrap();

    sink.begin_overwrite().await.unwrap();
    sink.write_batch(&[json!({"v": "fresh1"}), json!({"v": "fresh2"})])
        .await
        .unwrap();
    sink.commit_overwrite().await.unwrap();

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    let rows = sqlx::query("SELECT data FROM users ORDER BY id")
        .fetch_all(&pool)
        .await
        .unwrap();
    let payloads: Vec<Value> = rows
        .iter()
        .map(|r| serde_json::from_str(&r.get::<String, _>("data")).unwrap())
        .collect();
    pool.close().await;
    assert_eq!(
        payloads,
        vec![json!({"v": "fresh1"}), json!({"v": "fresh2"})]
    );
}

#[tokio::test]
async fn overwrite_begin_errors_when_target_missing() {
    // Overwrite never auto-creates the target — begin must fail clearly if it
    // does not exist, rather than silently creating an empty staging clone.
    let (_dir, url) = fresh_db("CREATE TABLE unrelated (id INTEGER)").await;
    let sink = SqliteSink::new(overwrite_config(&url, SqliteColumnMapping::AutoMap))
        .await
        .unwrap();
    let err = sink.begin_overwrite().await.unwrap_err();
    assert!(
        err.to_string().contains("staging"),
        "expected a staging-creation error, got: {err}"
    );
}

#[tokio::test]
async fn overwrite_reported_in_supported_write_modes() {
    let (_dir, url) = fresh_db("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").await;
    let sink = SqliteSink::new(overwrite_config(&url, SqliteColumnMapping::AutoMap))
        .await
        .unwrap();
    assert!(sink.supported_write_modes().contains(&WriteMode::Overwrite));
    assert!(sink.is_overwrite());
}
