//! #737 — `on_batch_error: dlq_all` end to end: refused on a sink that can land
//! part of a failed batch, accepted on an all-or-nothing one, and on that one a
//! failed batch goes to the DLQ whole, lands nothing, and a DLQ replay then
//! writes every row exactly once. The batch outcome reaches `faucet status`.
#![cfg(all(
    feature = "source-csv",
    feature = "sink-sqlite",
    feature = "sink-jsonl"
))]

use clap::Parser;
use faucet_cli::cli::{Cli, StateLoadArgs, StatusArgs};
use faucet_cli::error::CliError;
use sqlx::Row;
use std::path::{Path, PathBuf};

async fn run(args: &[&str]) -> Result<(), CliError> {
    let mut argv = vec!["faucet"];
    argv.extend_from_slice(args);
    let cli = Cli::try_parse_from(argv).expect("argv parses");
    Box::pin(faucet_cli::run_command(cli)).await
}

fn s(p: &Path) -> String {
    p.display().to_string()
}

fn write_config(dir: &Path, sink: &str, dlq_extra: &str) -> PathBuf {
    let text = format!(
        r#"version: 1
name: ledger
pipeline:
  source: {{ type: csv, config: {{ path: {input} }} }}
  sink: {sink}
  state: {{ type: file, config: {{ path: {state} }} }}
  dlq:
    sink: {{ type: jsonl, config: {{ path: {dlq} }} }}
    on_batch_error: dlq_all
{dlq_extra}"#,
        input = s(&dir.join("in.csv")),
        state = s(&dir.join("state")),
        dlq = s(&dir.join("dlq.jsonl")),
    );
    let path = dir.join("ledger.yaml");
    std::fs::write(&path, text).unwrap();
    path
}

async fn exec(db: &str, sql: &str) {
    let pool = sqlx::SqlitePool::connect(db).await.unwrap();
    sqlx::query(sql).execute(&pool).await.unwrap();
    pool.close().await;
}

async fn ids(db: &str) -> Vec<i64> {
    let pool = sqlx::SqlitePool::connect(db).await.unwrap();
    let rows = sqlx::query("SELECT id FROM t ORDER BY id")
        .fetch_all(&pool)
        .await
        .unwrap();
    pool.close().await;
    rows.iter().map(|r| r.get::<i64, _>("id")).collect()
}

#[tokio::test]
async fn dlq_all_on_a_best_effort_sink_is_refused_before_anything_runs() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("in.csv"), "id\n1\n").unwrap();
    let out = dir.path().join("out.jsonl");
    let cfg = write_config(
        dir.path(),
        &format!("{{ type: jsonl, config: {{ path: {} }} }}", s(&out)),
        "",
    );
    let cfgs = s(&cfg);

    let err = run(&["validate", &cfgs]).await.unwrap_err().to_string();
    assert!(
        err.contains("dlq_all") && err.contains("best_effort"),
        "{err}"
    );
    assert!(run(&["run", &cfgs]).await.is_err());
    assert!(!out.exists(), "a refused run writes nothing");
    let report = faucet_cli::commands::status::build(&StatusArgs {
        config: Some(cfg.clone()),
        row: None,
        probe: false,
        load: StateLoadArgs {
            json: true,
            env_file: None,
            no_env_file: true,
            profile: None,
        },
    })
    .await
    .unwrap();
    let failure = report.rows[0]
        .last_failure
        .as_ref()
        .expect("the refusal is recorded");
    assert!(
        failure
            .error
            .as_deref()
            .is_some_and(|e| e.contains("allow_duplicates_on_dlq_all")),
        "{failure:?}"
    );

    let cfg = write_config(
        dir.path(),
        &format!("{{ type: jsonl, config: {{ path: {} }} }}", s(&out)),
        "    allow_duplicates_on_dlq_all: true\n",
    );
    run(&["validate", &s(&cfg)]).await.unwrap();
    run(&["run", &s(&cfg)]).await.unwrap();
    assert_eq!(std::fs::read_to_string(&out).unwrap().lines().count(), 1);
}

#[tokio::test]
async fn a_failed_batch_on_an_atomic_sink_replays_without_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("in.csv"), "id,amount\n1,10\n2,-5\n3,7\n").unwrap();
    let db = format!("sqlite://{}?mode=rwc", s(&dir.path().join("out.db")));
    exec(
        &db,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, amount INTEGER CHECK (amount >= 0))",
    )
    .await;
    let cfg = write_config(
        dir.path(),
        &format!(
            "{{ type: sqlite, config: {{ database_url: \"{db}\", table_name: t, column_mapping: auto_map, batch_size: 0 }} }}"
        ),
        "",
    );
    let cfgs = s(&cfg);

    run(&["validate", &cfgs]).await.unwrap();
    run(&["run", &cfgs]).await.unwrap();
    assert!(
        ids(&db).await.is_empty(),
        "the CHECK violation rolls the whole batch back"
    );
    let dlq = std::fs::read_to_string(dir.path().join("dlq.jsonl")).unwrap();
    let envelopes: Vec<serde_json::Value> = dlq
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(envelopes.len(), 3);
    assert!(envelopes.iter().all(|e| e["reason"] == "dlq_all"));

    let report = faucet_cli::commands::status::build(&StatusArgs {
        config: Some(cfg.clone()),
        row: None,
        probe: false,
        load: StateLoadArgs {
            json: true,
            env_file: None,
            no_env_file: true,
            profile: None,
        },
    })
    .await
    .unwrap();
    let batches = report.rows[0]
        .batches
        .expect("the run recorded its batches");
    assert_eq!((batches.attempted, batches.dlq_all), (1, 1));
    assert!(
        report.rows[0]
            .reasons
            .iter()
            .any(|r| r.contains("went to the DLQ whole")),
        "{:?}",
        report.rows[0].reasons
    );

    exec(&db, "DROP TABLE t").await;
    exec(
        &db,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, amount INTEGER)",
    )
    .await;
    run(&[
        "dlq",
        "replay",
        &cfgs,
        "--from",
        &s(&dir.path().join("dlq.jsonl")),
    ])
    .await
    .unwrap();
    assert_eq!(ids(&db).await, vec![1, 2, 3], "every row exactly once");
}
