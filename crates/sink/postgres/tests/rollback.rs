//! Integration tests for run rollback on the PostgreSQL sink (#706): the
//! before-image journal written in the upsert transaction, the kept previous
//! table of an overwrite, the per-mode undo, the later-run conflict guard,
//! and the exactly-once watermark rewind.
//!
//! Requires Docker; each test boots its own container.

use faucet_core::rollback::{RollbackMode, RollbackOptions, RollbackWriteSpec};
use faucet_core::{DeleteMarker, Sink, WriteMode, WriteSpec};
use faucet_sink_postgres::{PostgresColumnMapping, PostgresSink, PostgresSinkConfig};
use serde_json::{Value, json};
use sqlx::Row;
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;

const RUN_COL: &str = "_faucet_run_id";

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

async fn exec(url: &str, sql: &str) {
    let pool = sqlx::PgPool::connect(url).await.expect("pool");
    sqlx::query(sql).execute(&pool).await.expect("exec");
    pool.close().await;
}

/// `(id, name, run)` rows of `users`, ordered by id.
async fn rows(url: &str) -> Vec<(i32, String, Option<String>)> {
    let pool = sqlx::PgPool::connect(url).await.expect("pool");
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

async fn scalar(url: &str, sql: &str) -> i64 {
    let pool = sqlx::PgPool::connect(url).await.expect("pool");
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
        &format!("SELECT count(*) FROM pg_tables WHERE tablename = '{table}'"),
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

fn config(url: &str, write: WriteSpec) -> PostgresSinkConfig {
    let mut c =
        PostgresSinkConfig::new(url, "users").column_mapping(PostgresColumnMapping::AutoMap);
    c.write = write;
    c
}

fn upsert_config(url: &str, run_id: &str) -> PostgresSinkConfig {
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
    "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, _faucet_run_id TEXT)";

fn user(id: i32, name: &str, run: &str) -> Value {
    json!({"id": id, "name": name, RUN_COL: run})
}

#[tokio::test]
async fn append_rollback_deletes_only_the_runs_rows() {
    let (_c, url) = start_postgres().await;
    exec(
        &url,
        "CREATE TABLE users (id INT, name TEXT, _faucet_run_id TEXT)",
    )
    .await;
    let sink = PostgresSink::new(config(&url, WriteSpec::default()))
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

#[tokio::test]
async fn upsert_rollback_restores_before_images_with_typed_columns() {
    let (_c, url) = start_postgres().await;
    exec(
        &url,
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, _faucet_run_id TEXT, \
         score NUMERIC(6,2), joined TIMESTAMPTZ, tags JSONB)",
    )
    .await;
    exec(
        &url,
        "INSERT INTO users VALUES (1, 'old-1', 'r0', 1.50, '2026-01-01T00:00:00Z', '{\"a\":1}'), \
         (2, 'old-2', 'r0', 2.25, '2026-01-02T00:00:00Z', '[1,2]')",
    )
    .await;
    let sink = PostgresSink::new(upsert_config(&url, "r1")).await.unwrap();
    // Change 1, delete 2, create 3; touch 1 again on a second page so the
    // journal must keep the *first* image.
    sink.write_batch(&[
        json!({"id": 1, "name": "new-1", RUN_COL: "r1", "score": 9.99}),
        json!({"id": 2, "__op": "d"}),
        json!({"id": 3, "name": "new-3", RUN_COL: "r1"}),
    ])
    .await
    .unwrap();
    sink.write_batch(&[json!({"id": 1, "name": "newer-1", RUN_COL: "r1"})])
        .await
        .unwrap();
    assert_eq!(
        scalar(&url, "SELECT count(*) FROM _faucet_run_journal").await,
        3
    );
    assert_eq!(
        scalar(
            &url,
            "SELECT count(*) FROM _faucet_run_journal WHERE before_json IS NULL"
        )
        .await,
        1,
        "the created key journals a null image"
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
    // Typed columns round-trip through the JSON before-image.
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    let r = sqlx::query(
        "SELECT score::text AS s, joined::text AS j, tags::text AS t FROM users WHERE id = 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(r.get::<String, _>("s"), "1.50");
    assert!(r.get::<String, _>("j").starts_with("2026-01-01"));
    assert_eq!(r.get::<String, _>("t"), "{\"a\": 1}");
    pool.close().await;
    assert_eq!(
        scalar(&url, "SELECT count(*) FROM _faucet_run_journal").await,
        0
    );
}

#[tokio::test]
async fn upsert_rollback_is_blocked_by_a_later_run_unless_forced() {
    let (_c, url) = start_postgres().await;
    exec(&url, CREATE_USERS).await;
    exec(&url, "INSERT INTO users VALUES (1, 'old-1', 'r0')").await;
    let sink = PostgresSink::new(upsert_config(&url, "r1")).await.unwrap();
    sink.write_batch(&[user(1, "new-1", "r1"), user(2, "new-2", "r1")])
        .await
        .unwrap();
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
        (1, 1, 1, true),
        "{out:?}"
    );
    assert_eq!(
        rows(&url).await,
        vec![(1, "old-1".into(), Some("r0".into()))]
    );
}

#[tokio::test]
async fn idempotent_and_partial_writes_journal_and_forget_clears() {
    let (_c, url) = start_postgres().await;
    exec(&url, CREATE_USERS).await;
    exec(&url, "INSERT INTO users VALUES (1, 'old-1', 'r0')").await;
    let sink = PostgresSink::new(upsert_config(&url, "r1")).await.unwrap();
    sink.write_batch_idempotent(&[user(1, "new-1", "r1")], "scope", "t1")
        .await
        .unwrap();
    let outcomes = sink
        .write_batch_partial(&[user(2, "b", "r1"), json!({"name": "no key"})])
        .await
        .unwrap();
    assert!(outcomes[0].is_ok() && outcomes[1].is_err());
    assert_eq!(
        scalar(&url, "SELECT count(*) FROM _faucet_run_journal").await,
        2
    );
    sink.forget_run("r1").await.unwrap();
    assert_eq!(
        scalar(&url, "SELECT count(*) FROM _faucet_run_journal").await,
        0
    );
    let out = sink
        .rollback_run("r1", &opts(RollbackMode::Upsert))
        .await
        .unwrap();
    assert!(out.note.unwrap().contains("no journal rows"));
    assert_eq!(rows(&url).await.len(), 2, "forgotten: the rows stay");
}

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
    let sink = PostgresSink::new(cfg).await.unwrap();
    sink.begin_overwrite().await.unwrap();
    sink.write_batch(page).await.unwrap();
    sink.commit_overwrite().await.unwrap();
}

#[tokio::test]
async fn overwrite_rollback_swaps_the_previous_table_back() {
    let (_c, url) = start_postgres().await;
    exec(&url, CREATE_USERS).await;
    exec(
        &url,
        "INSERT INTO users VALUES (1, 'old-1', 'r0'), (2, 'old-2', 'r0')",
    )
    .await;
    overwrite_run(&url, "r1", true, &[user(9, "new-9", "r1")]).await;
    assert!(table_exists(&url, "users__faucet_prev").await);
    assert_eq!(rows(&url).await.len(), 1);
    let sink = PostgresSink::new(config(&url, WriteSpec::default()))
        .await
        .unwrap();
    let mut dry = opts(RollbackMode::Overwrite);
    dry.dry_run = true;
    let out = sink.rollback_run("r1", &dry).await.unwrap();
    assert_eq!((out.restored, out.applied), (2, false), "{out:?}");
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

    // A second overwrite by another run blocks r1's rollback (the kept copy is
    // now r1's output, not its input) unless forced.
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
    // Without keep_previous nothing is kept.
    overwrite_run(&url, "r3", false, &[user(7, "x", "r3")]).await;
    assert!(!table_exists(&url, "users__faucet_prev").await);
}

#[tokio::test]
async fn capability_readback_and_token_rewind() {
    let (_c, url) = start_postgres().await;
    exec(&url, CREATE_USERS).await;
    let sink = PostgresSink::new(config(&url, WriteSpec::default()))
        .await
        .unwrap();
    assert!(sink.supports_rollback());
    let (kind, cfg) = sink.readback_source().expect("readback");
    assert_eq!(kind, "postgres");
    assert_eq!(cfg["query"], "SELECT * FROM \"users\"");
    assert_eq!(cfg["connection_url"], url);

    let mut jsonb = config(&url, WriteSpec::default());
    jsonb.column_mapping = PostgresColumnMapping::Jsonb {
        column: "data".into(),
    };
    jsonb.table_name = "blobs".into();
    let jsonb_sink = PostgresSink::new(jsonb).await.unwrap();
    assert!(!jsonb_sink.supports_rollback());
    assert!(jsonb_sink.readback_source().is_none());
    let err = jsonb_sink
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
    sink.rewind_commit_token("s", Some("000000000000000000002"))
        .await
        .unwrap();
    assert_eq!(
        sink.last_committed_token("s").await.unwrap().as_deref(),
        Some("000000000000000000002")
    );
    sink.rewind_commit_token("s", None).await.unwrap();
    assert_eq!(sink.last_committed_token("s").await.unwrap(), None);

    // A missing table is nothing to undo; an append rollback needs the column.
    let missing = PostgresSink::new(
        PostgresSinkConfig::new(&url, "nope").column_mapping(PostgresColumnMapping::AutoMap),
    )
    .await
    .unwrap();
    let out = missing
        .rollback_run("r1", &opts(RollbackMode::Append))
        .await
        .unwrap();
    assert!(out.note.unwrap().contains("does not exist"));
    exec(&url, "CREATE TABLE plain (id INT, name TEXT)").await;
    let plain = PostgresSink::new(
        PostgresSinkConfig::new(&url, "plain").column_mapping(PostgresColumnMapping::AutoMap),
    )
    .await
    .unwrap();
    let err = plain
        .rollback_run("r1", &opts(RollbackMode::Append))
        .await
        .unwrap_err();
    assert!(err.to_string().contains(RUN_COL), "{err}");
}
