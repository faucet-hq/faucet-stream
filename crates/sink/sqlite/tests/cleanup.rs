//! Integration tests for the SQLite sink's scoped cleanup (#478).
//!
//! All tests use a tempfile-backed SQLite database — no Docker required.
//!
//! Each test seeds a table with rows in two scopes, runs `cleanup_scope` for
//! one of them, and asserts that only the stale rows *inside* that scope were
//! removed. The rows of the other scope are the canary for an over-broad
//! predicate.

use faucet_core::{SeenKeys, Sink, WriteMode, WriteSpec};
use faucet_sink_sqlite::{SqliteColumnMapping, SqliteSink, SqliteSinkConfig};
use serde_json::{Value, json};
use sqlx::Row;
use sqlx::sqlite::SqlitePoolOptions;
use std::collections::BTreeMap;
use tempfile::TempDir;

const CREATE: &str = "CREATE TABLE assoc (
     contact_id INTEGER NOT NULL,
     id         INTEGER NOT NULL,
     label      TEXT,
     PRIMARY KEY (contact_id, id)
 )";

/// Create a fresh tempfile SQLite DB holding `assoc` seeded with `rows`.
/// Returns `(TempDir, url)` — keep `TempDir` alive for the test duration.
async fn fresh_db(rows: &[(i64, i64, &str)]) -> (TempDir, String) {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("test.db");
    let url = format!("sqlite://{}?mode=rwc", path.display());
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("connect");
    sqlx::query(CREATE).execute(&pool).await.expect("create");
    for (contact_id, id, label) in rows {
        sqlx::query("INSERT INTO assoc (contact_id, id, label) VALUES (?, ?, ?)")
            .bind(contact_id)
            .bind(id)
            .bind(label)
            .execute(&pool)
            .await
            .expect("seed");
    }
    pool.close().await;
    (dir, url)
}

/// All `(contact_id, id)` pairs currently in the table, sorted.
async fn remaining(url: &str) -> Vec<(i64, i64)> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .expect("connect");
    let rows = sqlx::query("SELECT contact_id, id FROM assoc ORDER BY contact_id, id")
        .fetch_all(&pool)
        .await
        .expect("select");
    pool.close().await;
    rows.iter()
        .map(|r| (r.get::<i64, _>("contact_id"), r.get::<i64, _>("id")))
        .collect()
}

fn sink_config(url: &str, key: &[&str]) -> SqliteSinkConfig {
    let mut config =
        SqliteSinkConfig::new(url, "assoc").column_mapping(SqliteColumnMapping::AutoMap);
    config.write = WriteSpec {
        write_mode: WriteMode::Upsert,
        key: key.iter().map(|s| s.to_string()).collect(),
        delete_marker: None,
        rollback: None,
    };
    config
}

/// A `SeenKeys` holding the keys of `page`, as the pipeline would accumulate them.
fn seen(page: &[Value], key: &[&str]) -> SeenKeys {
    let key: Vec<String> = key.iter().map(|s| s.to_string()).collect();
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
#[tokio::test]
async fn deletes_only_unwritten_rows_inside_the_scope() {
    let (_dir, url) = fresh_db(&[
        (1, 10, "kept"),
        (1, 11, "stale"),
        (1, 12, "stale"),
        (2, 20, "other scope"),
    ])
    .await;
    let sink = SqliteSink::new(sink_config(&url, &["contact_id", "id"]))
        .await
        .unwrap();

    let written = [json!({"contact_id": 1, "id": 10, "label": "kept"})];
    let deleted = sink
        .cleanup_scope(&scope(1), &seen(&written, &["contact_id", "id"]))
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
#[tokio::test]
async fn empty_seen_set_deletes_the_whole_scope() {
    let (_dir, url) = fresh_db(&[(1, 10, "a"), (1, 11, "b"), (2, 20, "other scope")]).await;
    let sink = SqliteSink::new(sink_config(&url, &["contact_id", "id"]))
        .await
        .unwrap();

    let deleted = sink
        .cleanup_scope(&scope(1), &SeenKeys::new())
        .await
        .expect("cleanup");

    assert_eq!(deleted, 2);
    assert_eq!(remaining(&url).await, vec![(2, 20)]);
}

// ---------------------------------------------------------------------------
// Test 3: a scope that matches nothing deletes nothing (and does not error)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn scope_matching_no_rows_deletes_nothing() {
    let (_dir, url) = fresh_db(&[(1, 10, "a")]).await;
    let sink = SqliteSink::new(sink_config(&url, &["contact_id", "id"]))
        .await
        .unwrap();

    let deleted = sink
        .cleanup_scope(&scope(99), &SeenKeys::new())
        .await
        .expect("cleanup");

    assert_eq!(deleted, 0);
    assert_eq!(remaining(&url).await, vec![(1, 10)]);
}

// ---------------------------------------------------------------------------
// Test 4: a single-column key still scopes correctly (the key need not include
// the scope column)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn single_column_key_is_scoped_by_the_claim() {
    let (_dir, url) = fresh_db(&[(1, 10, "kept"), (1, 11, "stale"), (2, 20, "other")]).await;
    let sink = SqliteSink::new(sink_config(&url, &["id"])).await.unwrap();

    let written = [json!({"contact_id": 1, "id": 10})];
    let deleted = sink
        .cleanup_scope(&scope(1), &seen(&written, &["id"]))
        .await
        .expect("cleanup");

    assert_eq!(deleted, 1);
    assert_eq!(remaining(&url).await, vec![(1, 10), (2, 20)]);
}

// ---------------------------------------------------------------------------
// Test 5: an unknown scope column is a clear error, and nothing is deleted
// ---------------------------------------------------------------------------
#[tokio::test]
async fn unknown_scope_column_errors_and_deletes_nothing() {
    let (_dir, url) = fresh_db(&[(1, 10, "a"), (2, 20, "b")]).await;
    let sink = SqliteSink::new(sink_config(&url, &["contact_id", "id"]))
        .await
        .unwrap();

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

// ---------------------------------------------------------------------------
// Test 6: the same sink can clean up repeatedly — the temp key table must not
// leak across invocations on the pooled connection
// ---------------------------------------------------------------------------
#[tokio::test]
async fn repeated_cleanups_reuse_the_connection() {
    let (_dir, url) = fresh_db(&[(1, 10, "a"), (1, 11, "b"), (2, 20, "c"), (2, 21, "d")]).await;
    let sink = SqliteSink::new(sink_config(&url, &["contact_id", "id"]))
        .await
        .unwrap();

    let first = [json!({"contact_id": 1, "id": 10})];
    assert_eq!(
        sink.cleanup_scope(&scope(1), &seen(&first, &["contact_id", "id"]))
            .await
            .expect("first cleanup"),
        1
    );

    let second = [json!({"contact_id": 2, "id": 21})];
    assert_eq!(
        sink.cleanup_scope(&scope(2), &seen(&second, &["contact_id", "id"]))
            .await
            .expect("second cleanup"),
        1
    );

    assert_eq!(remaining(&url).await, vec![(1, 10), (2, 21)]);
}

// ---------------------------------------------------------------------------
// Test 7: a text key round-trips — keys are bound as native SQLite types, so a
// string key must match the stored TEXT value rather than its JSON form
// ---------------------------------------------------------------------------
#[tokio::test]
async fn text_keys_match_natively() {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("test.db");
    let url = format!("sqlite://{}?mode=rwc", path.display());
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("connect");
    sqlx::query("CREATE TABLE assoc (contact_id INTEGER, id TEXT, label TEXT)")
        .execute(&pool)
        .await
        .expect("create");
    for (contact, id) in [(1, "a"), (1, "b"), (2, "c")] {
        sqlx::query("INSERT INTO assoc (contact_id, id, label) VALUES (?, ?, 'x')")
            .bind(contact)
            .bind(id)
            .execute(&pool)
            .await
            .expect("seed");
    }
    pool.close().await;

    let sink = SqliteSink::new(sink_config(&url, &["id"])).await.unwrap();
    let written = [json!({"contact_id": 1, "id": "a"})];
    let deleted = sink
        .cleanup_scope(&scope(1), &seen(&written, &["id"]))
        .await
        .expect("cleanup");

    assert_eq!(deleted, 1, "only 'b' is stale — 'a' was written");
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("connect");
    let ids: Vec<String> = sqlx::query("SELECT id FROM assoc ORDER BY id")
        .fetch_all(&pool)
        .await
        .expect("select")
        .iter()
        .map(|r| r.get::<String, _>("id"))
        .collect();
    pool.close().await;
    assert_eq!(ids, vec!["a".to_string(), "c".to_string()]);
}
