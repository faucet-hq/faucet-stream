//! Integration tests for run rollback on the SQLite sink (#706): the
//! before-image journal, the kept previous table, and the per-mode undo.
//!
//! All tests use a tempfile-backed SQLite database — no Docker required.

use faucet_core::rollback::{RollbackMode, RollbackOptions, RollbackWriteSpec};
use faucet_core::{DeleteMarker, Sink, WriteMode, WriteSpec};
use faucet_sink_sqlite::{SqliteColumnMapping, SqliteSink, SqliteSinkConfig};
use serde_json::{Value, json};
use sqlx::Row;
use sqlx::sqlite::SqlitePoolOptions;
use tempfile::TempDir;

const RUN_COL: &str = "_faucet_run_id";

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

async fn exec(url: &str, sql: &str) {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .expect("connect");
    sqlx::query(sql).execute(&pool).await.expect("exec");
    pool.close().await;
}

/// `(id, name, run)` rows of `users`, ordered by id.
async fn rows(url: &str) -> Vec<(i64, String, Option<String>)> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .expect("connect");
    let out = sqlx::query(&format!(
        "SELECT id, name, \"{RUN_COL}\" AS run FROM users ORDER BY id"
    ))
    .fetch_all(&pool)
    .await
    .expect("rows")
    .iter()
    .map(|r| (r.get("id"), r.get("name"), r.get("run")))
    .collect();
    pool.close().await;
    out
}

async fn table_exists(url: &str, table: &str) -> bool {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .expect("connect");
    let n: i64 =
        sqlx::query_scalar("SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?")
            .bind(table)
            .fetch_one(&pool)
            .await
            .expect("probe");
    pool.close().await;
    n == 1
}

async fn journal_rows(url: &str) -> i64 {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .expect("connect");
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM _faucet_run_journal")
        .fetch_one(&pool)
        .await
        .expect("journal count");
    pool.close().await;
    n
}

fn rollback_spec(run_id: &str, journal: bool, keep_previous: bool) -> RollbackWriteSpec {
    RollbackWriteSpec {
        run_id: run_id.into(),
        run_id_column: RUN_COL.into(),
        journal,
        keep_previous,
    }
}

fn config(url: &str, write: WriteSpec) -> SqliteSinkConfig {
    SqliteSinkConfig {
        database_url: url.to_string(),
        table_name: "users".to_string(),
        column_mapping: SqliteColumnMapping::AutoMap,
        batch_size: 1000,
        max_connections: 1,
        create_table: true,
        write,
    }
}

fn upsert_config(url: &str, run_id: &str) -> SqliteSinkConfig {
    config(
        url,
        WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["id".to_string()],
            delete_marker: Some(DeleteMarker {
                field: "__op".into(),
                values: vec!["d".into()],
            }),
            rollback: Some(rollback_spec(run_id, true, false)),
        },
    )
}

fn opts(mode: RollbackMode) -> RollbackOptions {
    RollbackOptions {
        run_id_column: RUN_COL.into(),
        mode,
        force: false,
        dry_run: false,
    }
}

const CREATE_USERS: &str =
    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, _faucet_run_id TEXT)";

fn user(id: i64, name: &str, run: &str) -> Value {
    json!({"id": id, "name": name, RUN_COL: run})
}

// ---------------------------------------------------------------------------
// Append: delete by run id
// ---------------------------------------------------------------------------

#[tokio::test]
async fn append_rollback_deletes_only_the_runs_rows() {
    let (_dir, url) = fresh_db(CREATE_USERS).await;
    let sink = SqliteSink::new(config(&url, WriteSpec::default()))
        .await
        .unwrap();
    sink.write_batch(&[user(1, "a", "r1"), user(2, "b", "r1"), user(3, "c", "r0")])
        .await
        .unwrap();

    let mut dry = opts(RollbackMode::Append);
    dry.dry_run = true;
    let out = sink.rollback_run("r1", &dry).await.unwrap();
    assert_eq!((out.deleted, out.applied), (2, false), "{out:?}");
    assert_eq!(rows(&url).await.len(), 3, "dry run changed nothing");

    let out = sink
        .rollback_run("r1", &opts(RollbackMode::Append))
        .await
        .unwrap();
    assert_eq!((out.deleted, out.applied), (2, true), "{out:?}");
    assert_eq!(
        rows(&url).await,
        vec![(3, "c".to_string(), Some("r0".to_string()))]
    );

    let again = sink
        .rollback_run("r1", &opts(RollbackMode::Append))
        .await
        .unwrap();
    assert_eq!(again.deleted, 0);
    assert!(again.note.unwrap().contains("no rows"));
}

#[tokio::test]
async fn append_rollback_needs_the_run_id_column() {
    let (_dir, url) = fresh_db("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").await;
    let sink = SqliteSink::new(config(&url, WriteSpec::default()))
        .await
        .unwrap();
    sink.write_batch(&[json!({"id": 1, "name": "a"})])
        .await
        .unwrap();
    let err = sink
        .rollback_run("r1", &opts(RollbackMode::Append))
        .await
        .unwrap_err();
    assert!(err.to_string().contains(RUN_COL), "{err}");
}

#[tokio::test]
async fn missing_table_is_nothing_to_undo() {
    let (_dir, url) = fresh_db("CREATE TABLE other (id INTEGER)").await;
    let sink = SqliteSink::new(config(&url, WriteSpec::default()))
        .await
        .unwrap();
    let out = sink
        .rollback_run("r1", &opts(RollbackMode::Append))
        .await
        .unwrap();
    assert!(out.applied && out.deleted == 0);
    assert!(out.note.unwrap().contains("does not exist"));
}

// ---------------------------------------------------------------------------
// Upsert: journal + restore
// ---------------------------------------------------------------------------

#[tokio::test]
async fn upsert_rollback_restores_before_images() {
    let (_dir, url) = fresh_db(CREATE_USERS).await;
    exec(
        &url,
        "INSERT INTO users VALUES (1, 'old-1', 'r0'), (2, 'old-2', 'r0')",
    )
    .await;
    let sink = SqliteSink::new(upsert_config(&url, "r1")).await.unwrap();
    // Changes 1, deletes 2, creates 3 — across two pages, and touches 1 twice
    // so the journal must keep the *first* image.
    sink.write_batch(&[
        user(1, "new-1", "r1"),
        json!({"id": 2, "__op": "d"}),
        user(3, "new-3", "r1"),
    ])
    .await
    .unwrap();
    sink.write_batch(&[user(1, "newer-1", "r1")]).await.unwrap();
    assert_eq!(journal_rows(&url).await, 3, "one journal row per key");
    assert_eq!(
        rows(&url).await,
        vec![
            (1, "newer-1".into(), Some("r1".into())),
            (3, "new-3".into(), Some("r1".into())),
        ]
    );

    let mut dry = opts(RollbackMode::Upsert);
    dry.dry_run = true;
    let out = sink.rollback_run("r1", &dry).await.unwrap();
    assert_eq!(
        (out.deleted, out.restored, out.applied),
        (1, 2, false),
        "{out:?}"
    );
    assert_eq!(rows(&url).await.len(), 2, "dry run changed nothing");
    assert_eq!(journal_rows(&url).await, 3, "dry run keeps the journal");

    let out = sink
        .rollback_run("r1", &opts(RollbackMode::Upsert))
        .await
        .unwrap();
    assert_eq!(
        (out.deleted, out.restored, out.conflicts, out.applied),
        (1, 2, 0, true),
        "{out:?}"
    );
    assert_eq!(
        rows(&url).await,
        vec![
            (1, "old-1".into(), Some("r0".into())),
            (2, "old-2".into(), Some("r0".into())),
        ]
    );
    assert_eq!(journal_rows(&url).await, 0, "journal cleared after restore");

    let again = sink
        .rollback_run("r1", &opts(RollbackMode::Upsert))
        .await
        .unwrap();
    assert!(again.applied && again.restored == 0);
    assert!(again.note.unwrap().contains("no journal rows"));
}

#[tokio::test]
async fn upsert_rollback_is_blocked_by_a_later_run_unless_forced() {
    let (_dir, url) = fresh_db(CREATE_USERS).await;
    exec(&url, "INSERT INTO users VALUES (1, 'old-1', 'r0')").await;
    let sink = SqliteSink::new(upsert_config(&url, "r1")).await.unwrap();
    sink.write_batch(&[user(1, "new-1", "r1"), user(2, "new-2", "r1")])
        .await
        .unwrap();
    // A later run changed key 1.
    exec(
        &url,
        "UPDATE users SET name = 'later-1', _faucet_run_id = 'r2' WHERE id = 1",
    )
    .await;

    let out = sink
        .rollback_run("r1", &opts(RollbackMode::Upsert))
        .await
        .unwrap();
    assert_eq!((out.conflicts, out.applied), (1, false), "{out:?}");
    assert!(out.note.unwrap().contains("--force"));
    assert_eq!(rows(&url).await.len(), 2, "blocked: nothing changed");
    assert_eq!(journal_rows(&url).await, 2);

    let mut force = opts(RollbackMode::Upsert);
    force.force = true;
    let out = sink.rollback_run("r1", &force).await.unwrap();
    assert_eq!(
        (out.deleted, out.restored, out.conflicts, out.applied),
        (1, 1, 1, true),
        "{out:?}"
    );
    assert_eq!(
        rows(&url).await,
        vec![(1, "old-1".into(), Some("r0".into()))]
    );
}

#[tokio::test]
async fn idempotent_writes_journal_too() {
    let (_dir, url) = fresh_db(CREATE_USERS).await;
    exec(&url, "INSERT INTO users VALUES (1, 'old-1', 'r0')").await;
    let sink = SqliteSink::new(upsert_config(&url, "r1")).await.unwrap();
    sink.write_batch_idempotent(&[user(1, "new-1", "r1")], "scope", "t1")
        .await
        .unwrap();
    assert_eq!(journal_rows(&url).await, 1);
    let out = sink
        .rollback_run("r1", &opts(RollbackMode::Upsert))
        .await
        .unwrap();
    assert_eq!((out.restored, out.applied), (1, true));
    assert_eq!(rows(&url).await[0].1, "old-1");
}

#[tokio::test]
async fn partial_writes_journal_and_forget_run_clears_it() {
    let (_dir, url) = fresh_db(CREATE_USERS).await;
    let sink = SqliteSink::new(upsert_config(&url, "r1")).await.unwrap();
    let outcomes = sink
        .write_batch_partial(&[user(1, "a", "r1"), json!({"name": "no key"})])
        .await
        .unwrap();
    assert!(outcomes[0].is_ok() && outcomes[1].is_err());
    assert_eq!(journal_rows(&url).await, 1);

    sink.forget_run("r1").await.unwrap();
    assert_eq!(journal_rows(&url).await, 0);
    let out = sink
        .rollback_run("r1", &opts(RollbackMode::Upsert))
        .await
        .unwrap();
    assert!(out.note.unwrap().contains("no journal rows"));
    assert_eq!(rows(&url).await.len(), 1, "forgotten: the row stays");
}

#[tokio::test]
async fn unjournaled_upserts_leave_no_journal() {
    let (_dir, url) = fresh_db(CREATE_USERS).await;
    let mut cfg = upsert_config(&url, "r1");
    cfg.write.rollback = Some(rollback_spec("r1", false, false));
    let sink = SqliteSink::new(cfg).await.unwrap();
    sink.write_batch(&[user(1, "a", "r1")]).await.unwrap();
    assert!(!table_exists(&url, "_faucet_run_journal").await);
}

#[tokio::test]
async fn upsert_rollback_without_a_key_is_an_error() {
    let (_dir, url) = fresh_db(CREATE_USERS).await;
    let sink = SqliteSink::new(config(&url, WriteSpec::default()))
        .await
        .unwrap();
    let err = sink
        .rollback_run("r1", &opts(RollbackMode::Upsert))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("no `key`"), "{err}");
}

// ---------------------------------------------------------------------------
// Overwrite: kept previous table
// ---------------------------------------------------------------------------

async fn overwrite_run(url: &str, run_id: &str, keep_previous: bool, page: &[Value]) {
    let cfg = config(
        url,
        WriteSpec {
            write_mode: WriteMode::Overwrite,
            key: vec![],
            delete_marker: None,
            rollback: Some(rollback_spec(run_id, false, keep_previous)),
        },
    );
    let sink = SqliteSink::new(cfg).await.unwrap();
    sink.begin_overwrite().await.unwrap();
    sink.write_batch(page).await.unwrap();
    sink.commit_overwrite().await.unwrap();
}

#[tokio::test]
async fn overwrite_rollback_swaps_the_previous_table_back() {
    let (_dir, url) = fresh_db(CREATE_USERS).await;
    exec(
        &url,
        "INSERT INTO users VALUES (1, 'old-1', 'r0'), (2, 'old-2', 'r0')",
    )
    .await;
    overwrite_run(&url, "r1", true, &[user(9, "new-9", "r1")]).await;
    assert!(table_exists(&url, "users__faucet_prev").await);
    assert_eq!(rows(&url).await.len(), 1);

    let sink = SqliteSink::new(config(&url, WriteSpec::default()))
        .await
        .unwrap();
    let mut dry = opts(RollbackMode::Overwrite);
    dry.dry_run = true;
    let out = sink.rollback_run("r1", &dry).await.unwrap();
    assert_eq!((out.restored, out.applied), (2, false), "{out:?}");
    assert!(table_exists(&url, "users__faucet_prev").await);

    let out = sink
        .rollback_run("r1", &opts(RollbackMode::Overwrite))
        .await
        .unwrap();
    assert_eq!((out.restored, out.applied), (2, true), "{out:?}");
    assert_eq!(
        rows(&url).await,
        vec![
            (1, "old-1".into(), Some("r0".into())),
            (2, "old-2".into(), Some("r0".into())),
        ]
    );
    assert!(!table_exists(&url, "users__faucet_prev").await);

    let err = sink
        .rollback_run("r1", &opts(RollbackMode::Overwrite))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("no previous copy"), "{err}");
}

#[tokio::test]
async fn overwrite_rollback_is_blocked_when_a_later_run_overwrote_again() {
    let (_dir, url) = fresh_db(CREATE_USERS).await;
    exec(&url, "INSERT INTO users VALUES (1, 'old-1', 'r0')").await;
    overwrite_run(&url, "r1", true, &[user(2, "r1-row", "r1")]).await;
    overwrite_run(&url, "r2", true, &[user(3, "r2-row", "r2")]).await;
    let sink = SqliteSink::new(config(&url, WriteSpec::default()))
        .await
        .unwrap();
    let out = sink
        .rollback_run("r1", &opts(RollbackMode::Overwrite))
        .await
        .unwrap();
    assert_eq!((out.conflicts, out.applied), (1, false), "{out:?}");
    assert_eq!(rows(&url).await[0].1, "r2-row", "blocked: untouched");

    // Forcing restores the kept copy — which is r1's output, the image before
    // the latest overwrite.
    let mut force = opts(RollbackMode::Overwrite);
    force.force = true;
    let out = sink.rollback_run("r1", &force).await.unwrap();
    assert!(out.applied);
    assert_eq!(rows(&url).await[0].1, "r1-row");
}

#[tokio::test]
async fn overwrite_without_keep_previous_keeps_nothing() {
    let (_dir, url) = fresh_db(CREATE_USERS).await;
    exec(&url, "INSERT INTO users VALUES (1, 'old-1', 'r0')").await;
    overwrite_run(&url, "r1", false, &[user(2, "new", "r1")]).await;
    assert!(!table_exists(&url, "users__faucet_prev").await);
}

// ---------------------------------------------------------------------------
// Capability surface
// ---------------------------------------------------------------------------

#[tokio::test]
async fn capability_and_readback_follow_the_column_mapping() {
    let (_dir, url) = fresh_db(CREATE_USERS).await;
    let sink = SqliteSink::new(config(&url, WriteSpec::default()))
        .await
        .unwrap();
    assert!(sink.supports_rollback());
    let (kind, cfg) = sink.readback_source().expect("readback");
    assert_eq!(kind, "sqlite");
    assert_eq!(cfg["query"], "SELECT * FROM \"users\"");
    assert_eq!(cfg["database_url"], url);

    let mut json_cfg = config(&url, WriteSpec::default());
    json_cfg.column_mapping = SqliteColumnMapping::Json {
        column: "data".into(),
    };
    json_cfg.table_name = "blobs".into();
    let json_sink = SqliteSink::new(json_cfg).await.unwrap();
    assert!(!json_sink.supports_rollback());
    assert!(json_sink.readback_source().is_none());
    let err = json_sink
        .rollback_run("r1", &opts(RollbackMode::Append))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("auto_map"), "{err}");
}

#[tokio::test]
async fn rewind_commit_token_sets_and_clears_the_watermark() {
    let (_dir, url) = fresh_db(CREATE_USERS).await;
    let sink = SqliteSink::new(config(&url, WriteSpec::default()))
        .await
        .unwrap();
    sink.rewind_commit_token("s", Some("000000000000000000005"))
        .await
        .unwrap();
    assert_eq!(
        sink.last_committed_token("s").await.unwrap().as_deref(),
        Some("000000000000000000005")
    );
    sink.rewind_commit_token("s", Some("000000000000000000002"))
        .await
        .unwrap();
    assert_eq!(
        sink.last_committed_token("s").await.unwrap().as_deref(),
        Some("000000000000000000002")
    );
    sink.rewind_commit_token("s", None).await.unwrap();
    assert_eq!(sink.last_committed_token("s").await.unwrap(), None);
}
