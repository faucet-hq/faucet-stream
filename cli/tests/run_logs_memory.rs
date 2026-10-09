//! Persistent run logs (#529) — the in-memory `RunHistory` backend impl.
#![cfg(feature = "serve")]

use faucet_cli::serve::history::memory::MemoryHistory;
use faucet_cli::serve::history::{RUN_LOG_TRUNCATED_SEQ, RunHistory, RunLogLine};
use std::time::Duration;

fn line(seq: u64, ts: &str, l: &str) -> RunLogLine {
    RunLogLine {
        seq,
        ts: ts.into(),
        level: "INFO".into(),
        line: l.into(),
        attrs: Default::default(),
    }
}

fn now() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
}

#[tokio::test]
async fn memory_record_list_paginate_and_truncate() {
    let h = MemoryHistory::new(Duration::from_secs(3600));
    let ts = now();
    let batch: Vec<RunLogLine> = (0..4).map(|i| line(i, &ts, &format!("l{i}"))).collect();
    h.record_run_logs("r", &batch).await.unwrap();
    h.record_run_logs("r", &[line(RUN_LOG_TRUNCATED_SEQ, "", "trunc")])
        .await
        .unwrap();

    let page = h.list_run_logs("r", None, 10).await.unwrap();
    assert_eq!(page.lines.len(), 4);
    assert!(page.truncated);
    assert!(page.lines.windows(2).all(|w| w[0].seq < w[1].seq));

    let page = h.list_run_logs("r", Some(1), 2).await.unwrap();
    assert_eq!(
        page.lines.iter().map(|l| l.seq).collect::<Vec<_>>(),
        vec![2, 3]
    );

    assert!(
        h.list_run_logs("absent", None, 10)
            .await
            .unwrap()
            .lines
            .is_empty()
    );
}

#[tokio::test]
async fn memory_purge_by_retention() {
    let h = MemoryHistory::new(Duration::from_secs(3600));
    h.record_run_logs(
        "r",
        &[
            line(0, "2000-01-01T00:00:00.000000000Z", "old"),
            line(1, &now(), "fresh"),
        ],
    )
    .await
    .unwrap();
    let removed = h.purge_run_logs(Duration::from_secs(86_400)).await.unwrap();
    assert_eq!(removed, 1);
    let page = h.list_run_logs("r", None, 10).await.unwrap();
    assert_eq!(page.lines.len(), 1);
    assert_eq!(page.lines[0].line, "fresh");
}
