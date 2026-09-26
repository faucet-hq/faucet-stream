#![cfg(all(feature = "catalog", feature = "serve-history-sqlite"))]
//! End-to-end for `faucet history` (#391): seed run records into a real sqlite
//! catalog store, then drive `history::run` over the table, `--json`, and
//! `--row` paths. Exercises the command's connect → fetch → filter → render
//! wiring that the pure unit tests can't reach.

use chrono::Utc;
use faucet_cli::cli::HistoryArgs;
use faucet_cli::serve::history::{InvocationRecord, RunRecord, RunStatus};
use std::path::PathBuf;

fn args(config: PathBuf, row: Option<String>, json: bool) -> HistoryArgs {
    HistoryArgs {
        config: Some(config),
        env_file: None,
        no_env_file: true,
        profile: None,
        limit: 20,
        row,
        json,
    }
}

fn record(id: &str, row_id: &str) -> RunRecord {
    let t = Utc::now();
    RunRecord {
        concurrency: None,
        run_id: id.into(),
        name: Some("demo".into()),
        labels: Default::default(),
        status: RunStatus::Completed,
        submitted_at: t,
        started_at: Some(t),
        finished_at: Some(t),
        elapsed_secs: Some(1.0),
        records_written: 10,
        invocations: vec![InvocationRecord {
            row_id: row_id.into(),
            parent_record_key: None,
            run_id: None,
            records_written: 10,
            duration_ms: 0,
            error: None,
        }],
        error: None,
        idempotency_key: None,
        doctor_report: None,
        config_body: None,
        config_format: None,
        timeout_secs: None,
        clock: None,
        attempt: 0,
        replay_of: None,
        callback: None,
    }
}

#[tokio::test]
async fn history_reads_seeded_sqlite_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("cat.db");
    let cfg = dir.path().join("faucet.yaml");
    std::fs::write(
        &cfg,
        format!(
            "version: 1\nname: demo\ncatalog:\n  url: \"sqlite:{db}\"\npipeline:\n  source: {{ type: rest, config: {{ path: /x }} }}\n  sink: {{ type: jsonl, config: {{ path: o }} }}\n",
            db = db.display(),
        ),
    )
    .unwrap();

    // Seed two run records under different matrix rows.
    let handle = faucet_cli::catalog::connect_from_spec(&faucet_cli::catalog::CatalogSpec {
        url: format!("sqlite:{}", db.display()),
        sample_records: 10,
        datasets: Vec::new(),
    })
    .await
    .unwrap();
    handle.store.upsert(&record("run-a", "us")).await.unwrap();
    handle.store.upsert(&record("run-b", "eu")).await.unwrap();

    // Table, JSON, and --row filter all drive `run()` to completion.
    faucet_cli::commands::history::run(args(cfg.clone(), None, false))
        .await
        .expect("table render");
    faucet_cli::commands::history::run(args(cfg.clone(), None, true))
        .await
        .expect("json render");
    faucet_cli::commands::history::run(args(cfg.clone(), Some("eu".into()), false))
        .await
        .expect("row filter");
}
