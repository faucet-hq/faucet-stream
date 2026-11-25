//! A bounded run-history call (#1100) against a real SQLite backend: a lease
//! renewal stuck behind another writer's lock fails as a transient timeout,
//! is counted, and leaves the wrapper healthy.
#![cfg(feature = "serve")]

use chrono::Utc;
use faucet_cli::serve::history::fallback::{FallbackHistory, with_call_bound};
use faucet_cli::serve::history::sqlite::SqliteHistory;
use faucet_cli::serve::history::{RunHistory, RunRecord, RunStatus};
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use std::collections::BTreeMap;
use std::time::Duration;

const TIMEOUTS: &str = "faucet_serve_history_call_timeouts_total";

#[test]
fn a_renewal_stuck_behind_a_writer_times_out_and_is_counted() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(renewal_behind_a_writer())
    });
    let counted: Vec<u64> = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, ..)| key.key().name() == TIMEOUTS)
        .map(|(.., value)| match value {
            DebugValue::Counter(n) => n,
            other => panic!("{TIMEOUTS} is a counter, got {other:?}"),
        })
        .collect();
    assert_eq!(counted, vec![1]);
}

async fn renewal_behind_a_writer() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite:{}", dir.path().join("history.db").display());
    let primary = SqliteHistory::connect(
        &url,
        Duration::from_secs(3600),
        Duration::from_secs(30),
        "bounded".to_string(),
    )
    .await
    .expect("connect sqlite history");
    let history = FallbackHistory::healthy(Box::new(primary), Duration::from_secs(3600), "sqlite");
    let mut run = RunRecord::queued("run-1".into(), None, BTreeMap::new(), None, Utc::now());
    run.status = RunStatus::Running;
    history.upsert(&run).await.unwrap();
    assert_eq!(history.renew_leases().await.unwrap(), 1);

    let writer = sqlx::SqlitePool::connect(&url).await.unwrap();
    let mut blocker = writer.begin().await.unwrap();
    sqlx::query("UPDATE faucet_serve_runs SET name = name WHERE run_id = 'run-1'")
        .execute(&mut *blocker)
        .await
        .unwrap();

    let started = std::time::Instant::now();
    let err = with_call_bound(Duration::from_millis(200), history.renew_leases())
        .await
        .expect_err("a renewal behind the writer's lock is cut off");
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "cut at the bound, not the busy timeout"
    );
    assert!(err.is_transient(), "{err}");
    assert!(
        err.to_string()
            .contains("the run-history backend did not answer within 200ms"),
        "{err}"
    );
    assert!(
        !history.degraded(),
        "a cut call does not degrade the wrapper"
    );

    blocker.rollback().await.unwrap();
    assert_eq!(
        with_call_bound(Duration::from_secs(10), history.renew_leases())
            .await
            .unwrap(),
        1
    );
}
