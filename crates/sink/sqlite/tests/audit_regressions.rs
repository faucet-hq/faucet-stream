//! Regressions for the #789 SQLite sink findings. Tempfile databases only.

use faucet_core::rollback::{RollbackMode, RollbackOptions, RollbackWriteSpec};
use faucet_core::{SeenKeys, Sink, WriteMode, WriteSpec};
use faucet_sink_sqlite::{SqliteColumnMapping, SqliteSink, SqliteSinkConfig};
use serde_json::{Value, json};
use sqlx::Row;
use sqlx::sqlite::SqlitePoolOptions;
use std::collections::BTreeMap;
use tempfile::TempDir;

async fn fresh_db(setup: &[&str]) -> (TempDir, String) {
    let dir = TempDir::new().expect("tempdir");
    let url = format!("sqlite://{}?mode=rwc", dir.path().join("t.db").display());
    for sql in setup {
        exec(&url, sql).await;
    }
    (dir, url)
}

async fn exec(url: &str, sql: &str) {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .expect("connect");
    sqlx::query(sql).execute(&pool).await.expect("exec");
    pool.close().await;
}

async fn query(url: &str, sql: &str) -> Vec<sqlx::sqlite::SqliteRow> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .expect("connect");
    let rows = sqlx::query(sql).fetch_all(&pool).await.expect("query");
    pool.close().await;
    rows
}

fn config(url: &str, table: &str, mode: WriteMode, key: &[&str]) -> SqliteSinkConfig {
    let mut c = SqliteSinkConfig::new(url, table).column_mapping(SqliteColumnMapping::AutoMap);
    c.write = WriteSpec {
        write_mode: mode,
        key: key.iter().map(|s| s.to_string()).collect(),
        delete_marker: None,
        rollback: None,
    };
    c
}

fn with_rollback(mut c: SqliteSinkConfig, run_id: &str, keep_previous: bool) -> SqliteSinkConfig {
    c.write.rollback = Some(RollbackWriteSpec {
        run_id: run_id.into(),
        run_id_column: "_faucet_run_id".into(),
        journal: true,
        keep_previous,
    });
    c
}

fn opts(mode: RollbackMode) -> RollbackOptions {
    RollbackOptions {
        run_id_column: "_faucet_run_id".into(),
        mode,
        force: true,
        dry_run: false,
        later_runs: false,
    }
}

#[tokio::test]
async fn upsert_keeps_columns_a_record_omits() {
    let (_dir, url) = fresh_db(&[
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)",
        "INSERT INTO users VALUES (1, 'a', 'a@x'), (2, 'b', 'b@x')",
    ])
    .await;
    let sink = SqliteSink::new(config(&url, "users", WriteMode::Upsert, &["id"]))
        .await
        .unwrap();
    let n = sink
        .write_batch(&[
            json!({"id": 1, "name": "a2"}),
            json!({"id": 2, "name": "b2", "email": "b2@x"}),
            json!({"id": 3, "email": "c@x"}),
        ])
        .await
        .unwrap();
    assert_eq!(n, 3);
    let rows = query(&url, "SELECT id, name, email FROM users ORDER BY id").await;
    let got: Vec<(i64, Option<String>, Option<String>)> = rows
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    assert_eq!(
        got,
        vec![
            (1, Some("a2".into()), Some("a@x".into())),
            (2, Some("b2".into()), Some("b2@x".into())),
            (3, None, Some("c@x".into())),
        ]
    );
}

#[tokio::test]
async fn append_refuses_a_record_matching_no_column() {
    let (_dir, url) = fresh_db(&["CREATE TABLE ev (user_id INTEGER, kind TEXT)"]).await;
    let sink = SqliteSink::new(config(&url, "ev", WriteMode::Append, &[]))
        .await
        .unwrap();
    let err = sink
        .write_batch(&[json!({"user_id": 1, "kind": "a"}), json!({"userId": 2})])
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("record 1 has no field matching"),
        "{err}"
    );
    let count = query(&url, "SELECT count(*) FROM ev").await;
    assert_eq!(count[0].get::<i64, _>(0), 0, "the chunk is one transaction");

    let outcomes = sink
        .write_batch_partial(&[
            json!({"USER_ID": 1, "Kind": "a"}),
            json!({"userId": 2}),
            json!({"user_id": 3}),
        ])
        .await
        .unwrap();
    assert!(outcomes[0].is_ok());
    assert!(
        outcomes[1].is_err(),
        "unmatched record is a per-row failure"
    );
    assert!(outcomes[2].is_ok());
    let rows = query(&url, "SELECT user_id, kind FROM ev ORDER BY user_id").await;
    let got: Vec<(i64, Option<String>)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
    assert_eq!(got, vec![(1, Some("a".into())), (3, None)]);
}

#[tokio::test]
async fn delete_with_a_key_that_is_not_a_column_fails() {
    let (_dir, url) = fresh_db(&[
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
        "INSERT INTO users VALUES (1, 'a')",
    ])
    .await;
    let sink = SqliteSink::new(config(&url, "users", WriteMode::Delete, &["idd"]))
        .await
        .unwrap();
    let err = sink.write_batch(&[json!({"idd": 1})]).await.unwrap_err();
    assert!(err.to_string().contains("no such column"), "{err}");
}

#[tokio::test]
async fn cleanup_keeps_rows_matched_through_a_nocase_key() {
    let (_dir, url) = fresh_db(&[
        "CREATE TABLE tags (scope INTEGER, code TEXT COLLATE NOCASE, PRIMARY KEY (scope, code))",
        "INSERT INTO tags VALUES (1, 'ABC'), (1, 'old')",
    ])
    .await;
    let sink = SqliteSink::new(config(&url, "tags", WriteMode::Upsert, &["scope", "code"]))
        .await
        .unwrap();
    let page = [json!({"scope": 1, "code": "abc"})];
    sink.write_batch(&page).await.unwrap();
    let key = vec!["scope".to_string(), "code".to_string()];
    let mut seen = SeenKeys::new();
    seen.record_page(&page, &key, 100);
    let deleted = sink
        .cleanup_scope(&BTreeMap::from([("scope".into(), json!(1))]), &seen)
        .await
        .unwrap();
    assert_eq!(deleted, 1, "only the stale row");
    let rows = query(&url, "SELECT code FROM tags").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>(0), "ABC");
}

#[tokio::test]
async fn rollback_restores_full_precision_reals_and_blobs() {
    let (_dir, url) = fresh_db(&[
        "CREATE TABLE m (id INTEGER PRIMARY KEY, r REAL, b BLOB, t TEXT, _faucet_run_id TEXT)",
        "INSERT INTO m VALUES (1, 0.30000000000000004, x'00ff10', '{\"a\":1}', 'r0')",
    ])
    .await;
    let sink = SqliteSink::new(with_rollback(
        config(&url, "m", WriteMode::Upsert, &["id"]),
        "r1",
        false,
    ))
    .await
    .unwrap();
    sink.write_batch(&[json!({"id": 1, "r": 1.5, "t": "x", "_faucet_run_id": "r1"})])
        .await
        .unwrap();
    let out = sink
        .rollback_run("r1", &opts(RollbackMode::Upsert))
        .await
        .unwrap();
    assert!(out.applied);
    assert_eq!(out.restored, 1);
    let rows = query(&url, "SELECT r, b, t, _faucet_run_id FROM m WHERE id = 1").await;
    assert_eq!(rows[0].get::<f64, _>(0), 0.30000000000000004);
    assert_eq!(rows[0].get::<Vec<u8>, _>(1), vec![0x00, 0xff, 0x10]);
    assert_eq!(rows[0].get::<String, _>(2), "{\"a\":1}");
    assert_eq!(rows[0].get::<String, _>(3), "r0");
}

#[tokio::test]
async fn first_overwrite_run_is_undoable() {
    let (_dir, url) = fresh_db(&[]).await;
    let sink_cfg = || with_rollback(config(&url, "fresh", WriteMode::Overwrite, &[]), "r1", true);
    let sink = SqliteSink::new(sink_cfg()).await.unwrap();
    sink.begin_overwrite().await.unwrap();
    sink.write_batch(&[json!({"id": 1, "_faucet_run_id": "r1"})])
        .await
        .unwrap();
    sink.commit_overwrite().await.unwrap();
    let n = query(&url, "SELECT count(*) FROM fresh").await;
    assert_eq!(n[0].get::<i64, _>(0), 1);

    let out = SqliteSink::new(sink_cfg())
        .await
        .unwrap()
        .rollback_run("r1", &opts(RollbackMode::Overwrite))
        .await
        .unwrap();
    assert!(out.applied);
    let n = query(&url, "SELECT count(*) FROM fresh").await;
    assert_eq!(n[0].get::<i64, _>(0), 0, "the first run's rows are gone");
}

#[tokio::test]
async fn auto_created_boolean_and_nested_columns_do_not_drift() {
    let (_dir, url) = fresh_db(&[]).await;
    let sink = SqliteSink::new(config(&url, "auto", WriteMode::Append, &[]))
        .await
        .unwrap();
    let page: Vec<Value> = vec![json!({"id": 1, "ok": true, "doc": {"a": 1}, "tags": ["x"]})];
    sink.write_batch(&page).await.unwrap();
    let dest = sink.current_schema().await.unwrap().expect("schema");
    let inferred = faucet_core::schema::infer_schema(&page);
    let diff = faucet_core::drift::diff_schema(&dest, &inferred, true);
    assert!(diff.is_empty(), "{diff:?}");
}

#[test]
fn busy_timeout_defaults_to_a_minute_and_is_configurable() {
    let c = SqliteSinkConfig::new("sqlite::memory:", "t");
    assert_eq!(c.busy_timeout_secs, 60);
    assert_eq!(c.with_busy_timeout_secs(5).busy_timeout_secs, 5);
    let parsed: SqliteSinkConfig = serde_json::from_value(json!({
        "database_url": "sqlite::memory:",
        "table_name": "t",
        "column_mapping": "auto_map",
        "busy_timeout_secs": 120
    }))
    .unwrap();
    assert_eq!(parsed.busy_timeout_secs, 120);
}

#[tokio::test]
async fn json_column_partial_writes_and_large_unsigned_values() {
    let (_d, url) = fresh_db(&[
        "CREATE TABLE docs (data TEXT)",
        "CREATE TABLE nums (n TEXT)",
    ])
    .await;
    let json_sink = SqliteSink::new(SqliteSinkConfig::new(&url, "docs").column_mapping(
        SqliteColumnMapping::Json {
            column: "data".into(),
        },
    ))
    .await
    .unwrap();
    let out = json_sink
        .write_batch_partial(&[json!({"a": 1}), json!({"b": 2})])
        .await
        .unwrap();
    assert_eq!(out.len(), 2);
    assert!(out.iter().all(Result::is_ok));
    assert_eq!(query(&url, "SELECT data FROM docs").await.len(), 2);

    let nums = SqliteSink::new(config(&url, "nums", WriteMode::Append, &[]))
        .await
        .unwrap();
    nums.write_batch(&[json!({"n": u64::MAX})]).await.unwrap();
    let rows = query(&url, "SELECT n FROM nums").await;
    assert_eq!(rows[0].get::<String, _>(0), u64::MAX.to_string());
}
