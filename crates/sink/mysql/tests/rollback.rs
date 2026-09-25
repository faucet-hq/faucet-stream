//! Integration tests for run rollback on the MySQL sink (#706): the
//! before-image journal written in the upsert transaction, the previous table
//! an overwrite keeps (by rename), the per-mode undo, the later-run conflict
//! guard, and the exactly-once watermark rewind.
//!
//! Requires Docker; each test boots its own container (startups are bounded by
//! a semaphore, like the sibling suites).

use faucet_core::rollback::{RollbackMode, RollbackOptions, RollbackWriteSpec};
use faucet_core::{DeleteMarker, Sink, WriteMode, WriteSpec};
use faucet_sink_mysql::{MysqlColumnMapping, MysqlSink, MysqlSinkConfig};
use serde_json::{Value, json};
use sqlx::Row;
use std::sync::OnceLock;
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::mysql::Mysql;
use tokio::sync::Semaphore;

const RUN_COL: &str = "_faucet_run_id";

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

async fn exec(url: &str, sql: &str) {
    let pool = sqlx::MySqlPool::connect(url).await.expect("pool");
    sqlx::query(sql).execute(&pool).await.expect("exec");
    pool.close().await;
}

/// `(id, name, run)` rows of `users`, ordered by id.
async fn rows(url: &str) -> Vec<(i32, String, Option<String>)> {
    let pool = sqlx::MySqlPool::connect(url).await.expect("pool");
    let out = sqlx::query(&format!(
        "SELECT id, name, `{RUN_COL}` AS run FROM users ORDER BY id"
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

async fn scalar(url: &str, sql: &str) -> i64 {
    let pool = sqlx::MySqlPool::connect(url).await.expect("pool");
    let n: i64 = sqlx::query_scalar(sql)
        .fetch_one(&pool)
        .await
        .expect("scalar");
    pool.close().await;
    n
}

async fn table_exists(url: &str, table: &str) -> bool {
    scalar(
        url,
        &format!(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = DATABASE() \
             AND table_name = '{table}'"
        ),
    )
    .await
        == 1
}

fn rollback_spec(run_id: &str, journal: bool, keep_previous: bool) -> RollbackWriteSpec {
    RollbackWriteSpec {
        run_id: run_id.into(),
        run_id_column: RUN_COL.into(),
        journal,
        keep_previous,
    }
}

fn config(url: &str, table: &str, write: WriteSpec) -> MysqlSinkConfig {
    let mut c = MysqlSinkConfig::new(url, table).column_mapping(MysqlColumnMapping::AutoMap);
    c.write = write;
    c
}

fn upsert_config(url: &str, run_id: &str) -> MysqlSinkConfig {
    config(
        url,
        "users",
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
    "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255), _faucet_run_id VARCHAR(64))";

fn user(id: i32, name: &str, run: &str) -> Value {
    json!({"id": id, "name": name, RUN_COL: run})
}

#[tokio::test(flavor = "multi_thread")]
async fn append_rollback_deletes_only_the_runs_rows() {
    let (_c, url) = start_mysql().await;
    exec(
        &url,
        "CREATE TABLE users (id INT, name VARCHAR(255), _faucet_run_id VARCHAR(64))",
    )
    .await;
    let sink = MysqlSink::new(config(&url, "users", WriteSpec::default()))
        .await
        .unwrap();
    sink.write_batch(&[user(1, "a", "r1"), user(2, "b", "r1"), user(3, "c", "r0")])
        .await
        .unwrap();
    let mut dry = opts(RollbackMode::Append);
    dry.dry_run = true;
    let out = sink.rollback_run("r1", &dry).await.unwrap();
    assert_eq!((out.deleted, out.applied), (2, false), "{out:?}");
    assert_eq!(rows(&url).await.len(), 3);
    let out = sink
        .rollback_run("r1", &opts(RollbackMode::Append))
        .await
        .unwrap();
    assert_eq!((out.deleted, out.applied), (2, true), "{out:?}");
    assert_eq!(rows(&url).await, vec![(3, "c".into(), Some("r0".into()))]);
    let again = sink
        .rollback_run("r1", &opts(RollbackMode::Append))
        .await
        .unwrap();
    assert!(again.note.unwrap().contains("no rows"));
}

#[tokio::test(flavor = "multi_thread")]
async fn upsert_rollback_restores_before_images_and_guards_later_runs() {
    let (_c, url) = start_mysql().await;
    exec(
        &url,
        "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255), _faucet_run_id VARCHAR(64), \
         score DECIMAL(6,2), joined DATETIME)",
    )
    .await;
    exec(
        &url,
        "INSERT INTO users VALUES (1, 'old-1', 'r0', 1.50, '2026-01-01 00:00:00'), \
         (2, 'old-2', 'r0', 2.25, '2026-01-02 00:00:00')",
    )
    .await;
    let sink = MysqlSink::new(upsert_config(&url, "r1")).await.unwrap();
    sink.write_batch(&[
        json!({"id": 1, "name": "new-1", RUN_COL: "r1", "score": 9.99}),
        json!({"id": 2, "__op": "d"}),
        json!({"id": 3, "name": "new-3", RUN_COL: "r1"}),
    ])
    .await
    .unwrap();
    sink.write_batch_idempotent(
        &[json!({"id": 1, "name": "newer-1", RUN_COL: "r1"})],
        "scope",
        "t1",
    )
    .await
    .unwrap();
    assert_eq!(
        scalar(&url, "SELECT count(*) FROM _faucet_run_journal").await,
        3
    );

    let mut dry = opts(RollbackMode::Upsert);
    dry.dry_run = true;
    let out = sink.rollback_run("r1", &dry).await.unwrap();
    assert_eq!(
        (out.deleted, out.restored, out.applied),
        (1, 2, false),
        "{out:?}"
    );
    assert_eq!(rows(&url).await.len(), 2);

    // A later run touches key 1 → blocked, then forced.
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
    assert_eq!(rows(&url).await.len(), 2, "blocked: untouched");
    let mut force = opts(RollbackMode::Upsert);
    force.force = true;
    let out = sink.rollback_run("r1", &force).await.unwrap();
    assert_eq!(
        (out.deleted, out.restored, out.conflicts, out.applied),
        (1, 2, 1, true),
        "{out:?}"
    );
    assert_eq!(
        rows(&url).await,
        vec![
            (1, "old-1".into(), Some("r0".into())),
            (2, "old-2".into(), Some("r0".into())),
        ]
    );
    let pool = sqlx::MySqlPool::connect(&url).await.unwrap();
    let r = sqlx::query(
        "SELECT CAST(score AS CHAR) AS s, CAST(joined AS CHAR) AS j FROM users WHERE id = 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(r.get::<String, _>("s"), "1.50");
    assert!(r.get::<String, _>("j").starts_with("2026-01-01"));
    pool.close().await;
    assert_eq!(
        scalar(&url, "SELECT count(*) FROM _faucet_run_journal").await,
        0
    );

    // forget_run drops a run's journal.
    sink.write_batch(&[user(5, "x", "r1")]).await.unwrap();
    assert_eq!(
        scalar(&url, "SELECT count(*) FROM _faucet_run_journal").await,
        1
    );
    sink.forget_run("r1").await.unwrap();
    assert_eq!(
        scalar(&url, "SELECT count(*) FROM _faucet_run_journal").await,
        0
    );
}

async fn overwrite_run(url: &str, run_id: &str, keep_previous: bool, page: &[Value]) {
    let cfg = config(
        url,
        "users",
        WriteSpec {
            write_mode: WriteMode::Overwrite,
            key: vec![],
            delete_marker: None,
            rollback: Some(rollback_spec(run_id, false, keep_previous)),
        },
    );
    let sink = MysqlSink::new(cfg).await.unwrap();
    sink.begin_overwrite().await.unwrap();
    sink.write_batch(page).await.unwrap();
    sink.commit_overwrite().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn overwrite_rollback_renames_the_previous_table_back() {
    let (_c, url) = start_mysql().await;
    exec(&url, CREATE_USERS).await;
    exec(
        &url,
        "INSERT INTO users VALUES (1, 'old-1', 'r0'), (2, 'old-2', 'r0')",
    )
    .await;
    overwrite_run(&url, "r1", true, &[user(9, "new-9", "r1")]).await;
    assert!(table_exists(&url, "users__faucet_prev").await);
    assert_eq!(rows(&url).await.len(), 1);
    let sink = MysqlSink::new(config(&url, "users", WriteSpec::default()))
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

    overwrite_run(&url, "r1", true, &[user(5, "r1-row", "r1")]).await;
    overwrite_run(&url, "r2", true, &[user(6, "r2-row", "r2")]).await;
    let out = sink
        .rollback_run("r1", &opts(RollbackMode::Overwrite))
        .await
        .unwrap();
    assert_eq!((out.conflicts, out.applied), (1, false), "{out:?}");
    let mut force = opts(RollbackMode::Overwrite);
    force.force = true;
    let out = sink.rollback_run("r1", &force).await.unwrap();
    assert!(out.applied);
    assert_eq!(rows(&url).await[0].1, "r1-row");
    overwrite_run(&url, "r3", false, &[user(7, "x", "r3")]).await;
    assert!(!table_exists(&url, "users__faucet_prev").await);
}

#[tokio::test(flavor = "multi_thread")]
async fn capability_readback_and_token_rewind() {
    let (_c, url) = start_mysql().await;
    exec(&url, CREATE_USERS).await;
    let sink = MysqlSink::new(config(&url, "users", WriteSpec::default()))
        .await
        .unwrap();
    assert!(sink.supports_rollback());
    let (kind, cfg) = sink.readback_source().expect("readback");
    assert_eq!(kind, "mysql");
    assert_eq!(cfg["query"], "SELECT * FROM `users`");

    let mut json_cfg = config(&url, "blobs", WriteSpec::default());
    json_cfg.column_mapping = MysqlColumnMapping::Json {
        column: "data".into(),
    };
    let json_sink = MysqlSink::new(json_cfg).await.unwrap();
    assert!(!json_sink.supports_rollback());
    assert!(json_sink.readback_source().is_none());
    let err = json_sink
        .rollback_run("r1", &opts(RollbackMode::Append))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("auto_map"), "{err}");

    sink.rewind_commit_token("s", Some("000000000000000000005"))
        .await
        .unwrap();
    assert_eq!(
        sink.last_committed_token("s").await.unwrap().as_deref(),
        Some("000000000000000000005")
    );
    sink.rewind_commit_token("s", None).await.unwrap();
    assert_eq!(sink.last_committed_token("s").await.unwrap(), None);

    let missing = MysqlSink::new(config(&url, "nope", WriteSpec::default()))
        .await
        .unwrap();
    let out = missing
        .rollback_run("r1", &opts(RollbackMode::Append))
        .await
        .unwrap();
    assert!(out.note.unwrap().contains("does not exist"));
    exec(&url, "CREATE TABLE plain (id INT, name VARCHAR(20))").await;
    let plain = MysqlSink::new(config(&url, "plain", WriteSpec::default()))
        .await
        .unwrap();
    let err = plain
        .rollback_run("r1", &opts(RollbackMode::Append))
        .await
        .unwrap_err();
    assert!(err.to_string().contains(RUN_COL), "{err}");
}
