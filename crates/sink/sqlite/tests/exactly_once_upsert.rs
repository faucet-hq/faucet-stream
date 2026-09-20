//! Composition test (#190): exactly-once delivery + `write_mode: upsert`.
//!
//! Verifies that `write_batch_idempotent` routes through the upsert planner so
//! the data write AND the commit-token watermark commit atomically in one
//! transaction. Re-writing the same key in a later page (with a higher token)
//! must UPDATE the row in place — not duplicate it — and advance the token.
//!
//! Runs against a tempfile SQLite database — no Docker required.

use faucet_core::{Sink, WriteMode, WriteSpec, format_token};
use faucet_sink_sqlite::{SqliteColumnMapping, SqliteSink, SqliteSinkConfig};
use serde_json::json;
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

#[tokio::test]
async fn idempotent_upsert_updates_in_place_and_advances_token() {
    let (_dir, url) = fresh_db("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").await;

    let cfg = SqliteSinkConfig {
        database_url: url.clone(),
        table_name: "users".to_string(),
        column_mapping: SqliteColumnMapping::AutoMap,
        batch_size: 1000,
        max_connections: 1,
        create_table: true,
        write: WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["id".to_string()],
            delete_marker: None,
        },
    };
    let sink = SqliteSink::new(cfg).await.unwrap();

    let scope = "users::r1";
    let t1 = format_token(1);
    let t2 = format_token(2);

    // Page 1, token ...0001: upsert id=1 -> "a".
    let written = sink
        .write_batch_idempotent(&[json!({"id": 1, "name": "a"})], scope, &t1)
        .await
        .unwrap();
    assert_eq!(written, 1, "one upsert applied");
    assert_eq!(
        sink.last_committed_token(scope).await.unwrap(),
        Some(t1.clone()),
        "token advanced to t1"
    );
    assert_eq!(count_rows(&url, "users").await, 1);
    assert_eq!(fetch_name(&url, 1).await.as_deref(), Some("a"));

    // Page 2, token ...0002: upsert id=1 -> "b" in the idempotent path.
    let written = sink
        .write_batch_idempotent(&[json!({"id": 1, "name": "b"})], scope, &t2)
        .await
        .unwrap();
    assert_eq!(written, 1, "one upsert applied (update in place)");

    // Exactly ONE row id=1, now named "b" — upserted, not duplicated.
    assert_eq!(
        count_rows(&url, "users").await,
        1,
        "upsert in the idempotent path must update, not duplicate"
    );
    assert_eq!(
        fetch_name(&url, 1).await.as_deref(),
        Some("b"),
        "row must reflect the latest value 'b'"
    );

    // Token advanced to t2 — committed atomically with the upsert.
    assert_eq!(
        sink.last_committed_token(scope).await.unwrap(),
        Some(t2),
        "token advanced to t2 alongside the upsert"
    );
}
