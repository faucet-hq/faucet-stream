//! Integration tests for the SQLite sink's upsert / delete write modes.
//!
//! All tests use a tempfile-backed SQLite database — no Docker required.

use faucet_core::{DeleteMarker, Sink, WriteMode, WriteSpec};
use faucet_sink_sqlite::{SqliteColumnMapping, SqliteSink, SqliteSinkConfig};
use serde_json::{Value, json};
use sqlx::Row;
use sqlx::sqlite::SqlitePoolOptions;
use tempfile::TempDir;

/// Create a fresh tempfile SQLite DB with the given CREATE TABLE SQL.
/// Returns `(TempDir, url)` — keep `TempDir` alive for the test duration.
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

async fn count_rows(url: &str, table: &str) -> i64 {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .expect("connect");
    let row = sqlx::query(&format!("SELECT COUNT(*) AS n FROM \"{table}\""))
        .fetch_one(&pool)
        .await
        .expect("count");
    let n: i64 = row.get("n");
    pool.close().await;
    n
}

async fn fetch_name(url: &str, id: i64) -> Option<String> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .expect("connect");
    let row = sqlx::query("SELECT name FROM users WHERE id = ?")
        .bind(id)
        .fetch_optional(&pool)
        .await
        .expect("fetch");
    pool.close().await;
    row.map(|r| r.get::<String, _>("name"))
}

fn upsert_config(url: &str) -> SqliteSinkConfig {
    SqliteSinkConfig {
        database_url: url.to_string(),
        table_name: "users".to_string(),
        column_mapping: SqliteColumnMapping::AutoMap,
        batch_size: 1000,
        max_connections: 1,
        create_table: true,
        write: WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["id".to_string()],
            delete_marker: None,
            rollback: None,
        },
    }
}

// ---------------------------------------------------------------------------
// Test 1: upsert updates an existing row (last-write-wins)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn upsert_updates_existing_row() {
    let (_dir, url) = fresh_db("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").await;

    let sink = SqliteSink::new(upsert_config(&url)).await.unwrap();

    // Insert initial row.
    sink.write_batch(&[json!({"id": 1, "name": "alice"})])
        .await
        .unwrap();
    assert_eq!(count_rows(&url, "users").await, 1);
    assert_eq!(fetch_name(&url, 1).await.as_deref(), Some("alice"));

    // Upsert same key with a different name — must update in place.
    sink.write_batch(&[json!({"id": 1, "name": "alice2"})])
        .await
        .unwrap();
    assert_eq!(
        count_rows(&url, "users").await,
        1,
        "upsert must not create a duplicate row"
    );
    assert_eq!(
        fetch_name(&url, 1).await.as_deref(),
        Some("alice2"),
        "name must be updated to 'alice2'"
    );
}

// ---------------------------------------------------------------------------
// Test 2: delete_marker routes delete-flagged rows to deletes
// ---------------------------------------------------------------------------
#[tokio::test]
async fn delete_marker_removes_row() {
    let (_dir, url) = fresh_db("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").await;

    let config = SqliteSinkConfig {
        database_url: url.clone(),
        table_name: "users".to_string(),
        column_mapping: SqliteColumnMapping::AutoMap,
        batch_size: 1000,
        max_connections: 1,
        create_table: true,
        write: WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["id".to_string()],
            delete_marker: Some(DeleteMarker {
                field: "__op".to_string(),
                values: vec!["d".to_string()],
            }),
            rollback: None,
        },
    };
    let sink = SqliteSink::new(config).await.unwrap();

    // First write: upsert a row (marker present but not "d").
    sink.write_batch(&[json!({"id": 1, "name": "x", "__op": "u"})])
        .await
        .unwrap();
    assert_eq!(count_rows(&url, "users").await, 1);

    // Second write: delete the row via marker.
    sink.write_batch(&[json!({"id": 1, "__op": "d"})])
        .await
        .unwrap();
    assert_eq!(
        count_rows(&url, "users").await,
        0,
        "delete-marked row must be removed from the table"
    );
}

// ---------------------------------------------------------------------------
// Test 3: within a single batch, last-write-wins per key
// ---------------------------------------------------------------------------
#[tokio::test]
async fn single_batch_last_write_wins() {
    let (_dir, url) = fresh_db("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").await;

    let sink = SqliteSink::new(upsert_config(&url)).await.unwrap();

    // One batch containing two records with the same key; the last one wins.
    let records: Vec<Value> = vec![
        json!({"id": 1, "name": "old"}),
        json!({"id": 1, "name": "new"}),
    ];
    sink.write_batch(&records).await.unwrap();

    assert_eq!(
        count_rows(&url, "users").await,
        1,
        "duplicate key within one batch must produce exactly one row"
    );
    assert_eq!(
        fetch_name(&url, 1).await.as_deref(),
        Some("new"),
        "last-write-wins: final value must be 'new'"
    );
}

// ---------------------------------------------------------------------------
// Test 4: write_batch_partial routes missing-key rows to the DLQ per-row
// ---------------------------------------------------------------------------
#[tokio::test]
async fn write_batch_partial_routes_missing_key_per_row() {
    let (_dir, url) = fresh_db("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").await;

    let sink = SqliteSink::new(upsert_config(&url)).await.unwrap();

    let records: Vec<Value> = vec![
        json!({"id": 1, "name": "ok"}),
        json!({"name": "missing-id"}),
    ];
    let outcomes = sink.write_batch_partial(&records).await.unwrap();

    assert_eq!(outcomes.len(), 2, "one outcome per input row");
    assert!(outcomes[0].is_ok(), "the good row must be Ok");
    assert!(
        outcomes[1].is_err(),
        "the missing-key row must be Err (routed to the DLQ)"
    );

    // The good row must have been written even though the page had a bad row.
    assert_eq!(
        count_rows(&url, "users").await,
        1,
        "only the good row should be written"
    );
    assert_eq!(
        fetch_name(&url, 1).await.as_deref(),
        Some("ok"),
        "id=1 must be present with name 'ok'"
    );
}

#[tokio::test]
async fn upsert_on_a_fresh_database_creates_a_keyed_table_and_dedups() {
    // #676: `create_table` builds the table with a PRIMARY KEY on `key`, so the
    // first run's ON CONFLICT has a target and a re-run updates in place.
    let (_dir, url) = fresh_db("CREATE TABLE unrelated (id INTEGER)").await;
    let cfg = || SqliteSinkConfig {
        database_url: url.clone(),
        table_name: "users".into(),
        column_mapping: SqliteColumnMapping::AutoMap,
        batch_size: 1000,
        max_connections: 1,
        create_table: true,
        write: WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["id".into()],
            delete_marker: None,
            rollback: None,
        },
    };
    let first = SqliteSink::new(cfg()).await.unwrap();
    first
        .write_batch(&[json!({"id": 1, "name": "a"}), json!({"id": 2, "name": "b"})])
        .await
        .unwrap();
    let second = SqliteSink::new(cfg()).await.unwrap();
    second
        .write_batch(&[
            json!({"id": 1, "name": "a2"}),
            json!({"id": 3, "name": "c"}),
        ])
        .await
        .unwrap();
    assert_eq!(count_rows(&url, "users").await, 3);
    assert_eq!(fetch_name(&url, 1).await.as_deref(), Some("a2"));
}

#[tokio::test]
async fn dlq_and_exactly_once_paths_create_a_fresh_keyed_table() {
    // #676: `write_batch_partial` (DLQ attached) and `write_batch_idempotent`
    // (exactly-once) never went through the create path before.
    for idempotent in [false, true] {
        let (_dir, url) = fresh_db("CREATE TABLE unrelated (id INTEGER)").await;
        let sink = SqliteSink::new(SqliteSinkConfig {
            database_url: url.clone(),
            table_name: "users".into(),
            column_mapping: SqliteColumnMapping::AutoMap,
            batch_size: 1000,
            max_connections: 1,
            create_table: true,
            write: WriteSpec {
                write_mode: WriteMode::Upsert,
                key: vec!["id".into()],
                delete_marker: None,
                rollback: None,
            },
        })
        .await
        .unwrap();
        let page = [
            json!({"id": 1, "name": "a"}),
            json!({"id": 1, "name": "a2"}),
        ];
        if idempotent {
            sink.write_batch_idempotent(&page, "scope", "0000000000000001")
                .await
                .unwrap();
        } else {
            let outcomes = sink.write_batch_partial(&page).await.unwrap();
            assert!(outcomes.iter().all(|o| o.is_ok()));
        }
        assert_eq!(
            count_rows(&url, "users").await,
            1,
            "idempotent={idempotent}"
        );
        assert_eq!(fetch_name(&url, 1).await.as_deref(), Some("a2"));
    }
}
