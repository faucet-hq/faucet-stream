//! #651 Category E1 — read-side faults, and the "never hangs" property.
//!
//! Everything in Category A injects a *write*-side failure. This suite injects
//! the other side: the source's read fails partway through — a dropped
//! connection, an expired cursor, a revoked token. The engine's obligation is
//! narrow but absolute:
//!
//! 1. The run either completes or **fails cleanly**. It must never report
//!    success having read half the data.
//! 2. Whatever bookmark is durable afterwards is backed by confirmed writes, so
//!    a resumed run picks up without a gap.
//! 3. It **terminates**. A retry loop that never gives up looks identical to a
//!    hang from the outside, and an operator cannot tell a slow pipeline from a
//!    wedged one — so every test here runs under a hard timeout rather than
//!    trusting the run to return.
//!
//! The container-backed version of these (toxiproxy in front of a real
//! database) is a nightly tier; this suite covers the engine contract with no
//! Docker so it can gate every PR.

use std::sync::Arc;
use std::time::Duration;

use faucet_conformance::scripted::{
    Boundary, Event, EventLog, PagedSource, ScriptedSink, assert_bookmarks_backed_by_writes,
};
use faucet_core::{CancellationToken, Pipeline};

/// Hard ceiling for every run in this suite. Generous enough that a slow
/// machine does not flake, tight enough that an unbounded retry loop fails the
/// test instead of hanging the job.
const MUST_FINISH_WITHIN: Duration = Duration::from_secs(20);

/// Run a future under the ceiling, failing loudly on a hang rather than letting
/// the harness time the whole suite out with no attribution.
async fn within<F: std::future::Future>(label: &str, fut: F) -> F::Output {
    match tokio::time::timeout(MUST_FINISH_WITHIN, fut).await {
        Ok(v) => v,
        Err(_) => panic!(
            "{label} did not finish within {MUST_FINISH_WITHIN:?} — a run that never \
             terminates is indistinguishable from a hang, which is the regression this \
             ceiling exists to catch"
        ),
    }
}

fn records_through_page(p: usize, per_page: usize) -> usize {
    (p + 1) * per_page
}

#[tokio::test]
async fn a_mid_stream_read_failure_fails_the_run_rather_than_truncating_it() {
    // The silent-truncation trap: three pages were read fine, then the
    // connection dropped. Reporting success here would hand the caller a
    // partial dataset that looks complete.
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone());
    let store = Arc::new(sink.state_store());
    let source = PagedSource::new(8, 4).failing_after(3);

    let err = within(
        "mid-stream read failure",
        Pipeline::new(&source, &sink)
            .with_state_store(store.clone())
            .run(),
    )
    .await
    .expect_err("a failed read must fail the run, not truncate it silently");

    assert!(
        matches!(err, faucet_core::FaucetError::Source(_)),
        "the source's own typed error must surface: {err:?}"
    );
    // The pages that *were* read are kept — they were genuinely delivered, and
    // discarding them would turn a recoverable partial run into a full replay.
    assert_eq!(
        log.records_written(),
        12,
        "the three pages read before the failure must still have been written: {:?}",
        log.events()
    );
}

#[tokio::test]
async fn the_bookmark_after_a_read_failure_is_still_consistent() {
    let per_page = 3;
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone());
    let store = Arc::new(sink.state_store());
    let source = PagedSource::new(10, per_page).failing_after(4);

    let _ = within(
        "read failure bookmark",
        Pipeline::new(&source, &sink)
            .with_state_store(store.clone())
            .run(),
    )
    .await;

    assert_bookmarks_backed_by_writes(&log, |b| {
        records_through_page(b["page"].as_u64().expect("page") as usize, per_page)
    });

    // And resume must pick up exactly where it stopped — no gap across the
    // failure boundary.
    let durable = store.stored("scripted:paged").expect("partial progress");
    let resumed_from = durable["page"].as_u64().expect("page") as usize;
    let resume_source = PagedSource::new(10, per_page);
    let resume_sink = ScriptedSink::new(EventLog::new());
    within(
        "resume after read failure",
        Pipeline::new(&resume_source, &resume_sink)
            .with_state_store(store.clone())
            .run(),
    )
    .await
    .expect("the resumed run completes");

    assert_eq!(
        resume_source.emitted_pages().first().copied(),
        Some(resumed_from + 1),
        "resume must continue from the page after the durable bookmark, leaving no \
         gap across the read failure; emitted {:?}",
        resume_source.emitted_pages()
    );
}

#[tokio::test]
async fn a_read_failure_on_the_very_first_page_leaves_no_bookmark() {
    // Nothing was read, so nothing may be claimed. A bookmark here would make
    // the next run skip the first page forever.
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone());
    let store = Arc::new(sink.state_store());
    let source = PagedSource::new(5, 3).failing_after(0);

    within(
        "first-page read failure",
        Pipeline::new(&source, &sink)
            .with_state_store(store.clone())
            .run(),
    )
    .await
    .expect_err("the run fails");

    assert_eq!(store.stored("scripted:paged"), None);
    assert_eq!(log.records_written(), 0);
    assert!(
        !log.any(|e| matches!(e, Event::StatePut { .. })),
        "{:?}",
        log.events()
    );
}

#[tokio::test]
async fn a_read_failure_never_publishes_an_overwrite() {
    // Combines the two most destructive paths: a full-refresh run whose *read*
    // fails. Publishing the staging target would replace the destination with
    // whatever fraction of the source arrived.
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone()).overwrite();
    let source = PagedSource::new(8, 3).failing_after(2);

    within(
        "overwrite with a failed read",
        Pipeline::new(&source, &sink).run(),
    )
    .await
    .expect_err("the run fails");

    assert!(
        !log.any(|e| matches!(e, Event::CommitOverwrite)),
        "a partial read must never be published as a full refresh: {:?}",
        log.events()
    );
    assert!(log.any(|e| matches!(e, Event::AbortOverwrite)));
}

#[tokio::test]
async fn a_read_failure_never_triggers_a_cleanup_delete() {
    // Same shape for the delete path: the source stopped early, so the
    // written-key set is incomplete and every unwritten row looks stale.
    let mut scope = std::collections::BTreeMap::new();
    scope.insert("tenant".to_string(), serde_json::json!("acme"));
    let policy = Arc::new(
        faucet_core::cleanup::CleanupPolicy::new(scope, vec!["n".to_string()], 1000)
            .expect("valid policy"),
    );

    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone()).cleanup();
    let source = PagedSource::new(8, 3).failing_after(2);

    within(
        "cleanup with a failed read",
        Pipeline::new(&source, &sink).with_cleanup(policy).run(),
    )
    .await
    .expect_err("the run fails");

    assert!(
        !log.any(|e| matches!(e, Event::Cleanup(_))),
        "a partial read must not license a delete: {:?}",
        log.events()
    );
}

#[tokio::test]
async fn a_slow_sink_under_cancel_terminates_promptly() {
    // The observed-hang regression, in the abstract: a destination that has
    // become very slow, and an operator who cancels. The run must stop at the
    // next page boundary, not ride out the full backlog.
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone()).with_write_delay(Duration::from_millis(40));
    let store = Arc::new(sink.state_store());
    // 400 pages at 40ms each would be ~16s if cancellation were ignored, which
    // the ceiling below would catch.
    let source = PagedSource::new(400, 1);
    let cancel = CancellationToken::new();

    let canceller = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        canceller.cancel();
    });

    let started = std::time::Instant::now();
    let result = within(
        "cancelled slow sink",
        Pipeline::new(&source, &sink)
            .with_state_store(store.clone())
            .with_cancel(cancel)
            .run(),
    )
    .await
    .expect("a cancelled run stops cleanly");
    let elapsed = started.elapsed();

    assert!(
        result.records_written < source.total_records(),
        "the run must have been cut short: wrote {} of {}",
        result.records_written,
        source.total_records()
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "cancellation took {elapsed:?} — it must take effect at the next page \
         boundary, not after draining the backlog"
    );
    assert!(
        log.any(|e| matches!(e, Event::Flush)),
        "and it must still flush what it accepted"
    );
}

#[tokio::test]
async fn a_write_failure_during_a_slow_run_still_terminates() {
    // Belt and braces on the termination property: a failure arriving while
    // writes are slow must not leave the run spinning.
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone())
        .with_write_delay(Duration::from_millis(20))
        .failing_at(Boundary::Write(2));
    let store = Arc::new(sink.state_store());
    let source = PagedSource::new(100, 1);

    within(
        "slow run with a write failure",
        Pipeline::new(&source, &sink)
            .with_state_store(store.clone())
            .run(),
    )
    .await
    .expect_err("the write failure fails the run");

    assert_bookmarks_backed_by_writes(&log, |b| {
        records_through_page(b["page"].as_u64().expect("page") as usize, 1)
    });
}

#[tokio::test]
async fn a_healthy_run_is_the_control_and_reads_everything() {
    // Without this, a bug that made every run fail early would leave the suite
    // above entirely green.
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone());
    let source = PagedSource::new(6, 3);

    let result = within("healthy control run", Pipeline::new(&source, &sink).run())
        .await
        .expect("a healthy run completes");

    assert_eq!(result.records_written, source.total_records());
    assert_eq!(source.emitted_pages(), (0..6).collect::<Vec<_>>());
}
