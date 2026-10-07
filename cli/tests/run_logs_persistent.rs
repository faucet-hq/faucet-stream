//! Persistent run logs (#529): the SQLite `RunHistory` log methods and the
//! `LogHub` persistence writer (batching, per-run cap, retention). SQLite is
//! embedded, so the SQL tests exercise the shared machinery used by Postgres too.
#![cfg(feature = "serve-history-sqlite")]

use faucet_cli::serve::history::sqlite::SqliteHistory;
use faucet_cli::serve::history::{RUN_LOG_TRUNCATED_SEQ, RunHistory, RunLogLine};
use std::sync::Arc;
use std::time::Duration;

async fn store(dir: &tempfile::TempDir, file: &str) -> SqliteHistory {
    let path = dir.path().join(file);
    SqliteHistory::connect(
        &format!("sqlite:{}", path.display()),
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        "test-instance".to_string(),
    )
    .await
    .expect("connect sqlite history")
}

fn log_line(seq: u64, ts: &str, line: &str) -> RunLogLine {
    RunLogLine {
        seq,
        ts: ts.into(),
        level: "INFO".into(),
        line: line.into(),
    }
}

fn now_ts() -> String {
    faucet_cli::serve::history::sql::fmt_ts(chrono::Utc::now())
}

#[tokio::test]
async fn record_list_and_paginate() {
    let dir = tempfile::tempdir().unwrap();
    let h = store(&dir, "logs1.db").await;
    let now = now_ts();
    let lines: Vec<RunLogLine> = (0..5)
        .map(|i| log_line(i, &now, &format!("l{i}")))
        .collect();
    h.record_run_logs("run-a", &lines).await.unwrap();

    let page = h.list_run_logs("run-a", None, 10).await.unwrap();
    assert!(!page.truncated);
    assert_eq!(page.lines.len(), 5);
    assert_eq!(page.lines[0].seq, 0);
    assert_eq!(page.lines[4].line, "l4");
    assert!(
        page.lines.windows(2).all(|w| w[0].seq < w[1].seq),
        "oldest-first by seq"
    );

    // after seq 1, limit 2 → seq 2,3.
    let page = h.list_run_logs("run-a", Some(1), 2).await.unwrap();
    assert_eq!(
        page.lines.iter().map(|l| l.seq).collect::<Vec<_>>(),
        vec![2, 3]
    );

    // Unknown run → empty.
    let page = h.list_run_logs("nope", None, 10).await.unwrap();
    assert!(page.lines.is_empty() && !page.truncated);
}

#[tokio::test]
async fn truncation_sentinel_sets_flag_and_is_excluded() {
    let dir = tempfile::tempdir().unwrap();
    let h = store(&dir, "logs2.db").await;
    let now = now_ts();
    h.record_run_logs("run-b", &[log_line(0, &now, "a"), log_line(1, &now, "b")])
        .await
        .unwrap();
    h.record_run_logs(
        "run-b",
        &[RunLogLine {
            seq: RUN_LOG_TRUNCATED_SEQ,
            ts: String::new(),
            level: "WARN".into(),
            line: "truncated".into(),
        }],
    )
    .await
    .unwrap();

    let page = h.list_run_logs("run-b", None, 100).await.unwrap();
    assert!(page.truncated, "sentinel must set truncated");
    assert_eq!(page.lines.len(), 2, "sentinel excluded from lines");
    assert!(page.lines.iter().all(|l| l.seq != RUN_LOG_TRUNCATED_SEQ));
}

#[tokio::test]
async fn purge_by_retention() {
    let dir = tempfile::tempdir().unwrap();
    let h = store(&dir, "logs3.db").await;
    let old = "2000-01-01T00:00:00.000000000Z";
    let fresh = now_ts();
    h.record_run_logs(
        "run-c",
        &[log_line(0, old, "ancient"), log_line(1, &fresh, "recent")],
    )
    .await
    .unwrap();

    let removed = h.purge_run_logs(Duration::from_secs(86_400)).await.unwrap();
    assert_eq!(
        removed, 1,
        "only the year-2000 line is past the 1-day window"
    );
    let page = h.list_run_logs("run-c", None, 10).await.unwrap();
    assert_eq!(page.lines.len(), 1);
    assert_eq!(page.lines[0].line, "recent");
}

// ── LogHub persistence writer ───────────────────────────────────────────────

/// Poll `list_run_logs` until it has at least `want` lines or the deadline hits
/// (the writer task drains the channel asynchronously).
async fn wait_for_lines(
    h: &Arc<dyn RunHistory>,
    run: &str,
    want: usize,
) -> faucet_cli::serve::history::RunLogPage {
    for _ in 0..100 {
        let page = h.list_run_logs(run, None, 10_000).await.unwrap();
        if page.lines.len() >= want {
            return page;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    h.list_run_logs(run, None, 10_000).await.unwrap()
}

#[tokio::test]
async fn loghub_persists_captured_lines_on_finish() {
    let dir = tempfile::tempdir().unwrap();
    let h: Arc<dyn RunHistory> = Arc::new(store(&dir, "hub1.db").await);
    let hub = faucet_cli::serve::logs::LogHub::new();
    hub.enable_persistence(Arc::clone(&h), 1000);

    for i in 0..3 {
        hub.capture("run-x", "INFO", now_ts(), format!("line {i}"));
    }
    hub.finish("run-x");

    let page = wait_for_lines(&h, "run-x", 3).await;
    assert_eq!(page.lines.len(), 3);
    assert!(!page.truncated);
    assert_eq!(page.lines[0].line, "line 0");
}

#[tokio::test]
async fn loghub_enforces_per_run_cap_and_marks_truncated() {
    let dir = tempfile::tempdir().unwrap();
    let h: Arc<dyn RunHistory> = Arc::new(store(&dir, "hub2.db").await);
    let hub = faucet_cli::serve::logs::LogHub::new();
    hub.enable_persistence(Arc::clone(&h), 2); // cap at 2 lines

    for i in 0..6 {
        hub.capture("run-y", "INFO", now_ts(), format!("line {i}"));
    }
    hub.finish("run-y");

    // Only the first 2 persist; a truncation marker sets the flag.
    let page = wait_for_lines(&h, "run-y", 2).await;
    assert_eq!(page.lines.len(), 2, "capped at max_lines_per_run");
    // The marker is written at End; poll a little for it.
    let mut truncated = page.truncated;
    for _ in 0..50 {
        if truncated {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        truncated = h.list_run_logs("run-y", None, 10).await.unwrap().truncated;
    }
    assert!(truncated, "hitting the cap must set truncated");
}

#[tokio::test]
async fn loghub_without_persistence_is_ephemeral() {
    // No enable_persistence → capture still feeds the ring but persists nothing.
    let dir = tempfile::tempdir().unwrap();
    let h: Arc<dyn RunHistory> = Arc::new(store(&dir, "hub3.db").await);
    let hub = faucet_cli::serve::logs::LogHub::new();
    hub.capture("run-z", "INFO", now_ts(), "ephemeral".into());
    hub.finish("run-z");
    tokio::time::sleep(Duration::from_millis(50)).await;
    let page = h.list_run_logs("run-z", None, 10).await.unwrap();
    assert!(
        page.lines.is_empty(),
        "nothing persisted without enable_persistence"
    );
}

/// #789 SERVE-49: lines of a run that has not ended are written when the
/// server shuts the writer down, instead of being lost with the task.
#[tokio::test]
async fn shutdown_persists_lines_of_runs_still_open() {
    let dir = tempfile::tempdir().unwrap();
    let h: Arc<dyn RunHistory> = Arc::new(store(&dir, "hub4.db").await);
    let hub = faucet_cli::serve::logs::LogHub::new();
    hub.enable_persistence(Arc::clone(&h), 2);
    for i in 0..3 {
        hub.capture("open-run", "INFO", now_ts(), format!("tail {i}"));
    }
    hub.shutdown_persistence(Duration::from_secs(5)).await;
    let page = h.list_run_logs("open-run", None, 100).await.unwrap();
    assert_eq!(page.lines.len(), 2, "{page:?}");
    assert!(page.truncated, "the cap marker is written at shutdown too");
    // A second shutdown, and one on a hub without persistence, are no-ops.
    hub.shutdown_persistence(Duration::from_secs(1)).await;
    faucet_cli::serve::logs::LogHub::new()
        .shutdown_persistence(Duration::from_secs(1))
        .await;
}

/// #789 SERVE-35: two instances writing the same run's logs with the same
/// key both keep their line, and the time-based key orders them.
#[tokio::test]
async fn colliding_log_keys_keep_both_lines() {
    let dir = tempfile::tempdir().unwrap();
    let h = store(&dir, "collide.db").await;
    let a = faucet_cli::serve::logs::persist_seq(1);
    h.record_run_logs("r", &[log_line(a, "t", "from instance a")])
        .await
        .unwrap();
    h.record_run_logs("r", &[log_line(a, "t", "from instance b")])
        .await
        .unwrap();
    let page = h.list_run_logs("r", None, 10).await.unwrap();
    assert_eq!(page.lines.len(), 2, "{page:?}");
    let later = faucet_cli::serve::logs::persist_seq(0);
    assert!(later > a >> 12 << 12, "keys grow with time");
}
