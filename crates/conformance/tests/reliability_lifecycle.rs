//! #651 Category A3 + A8 — run-completion lifecycle.
//!
//! Two guarantees about what the engine does at the *end* of a run, both of
//! which are invisible to a test that only checks the happy path:
//!
//! - **A3, overwrite atomicity.** `write_mode: overwrite` stages the new data
//!   and swaps it in. The swap must happen only after a fully successful,
//!   uncancelled run — otherwise a mid-run failure truncates the destination
//!   and then fails, which is the single most destructive thing a data tool can
//!   do.
//! - **A8, cancel still flushes.** A cancelled run must stop at a page boundary
//!   and *flush*, so a buffered sink (a Parquet footer, an S3 multipart) commits
//!   what it accepted instead of orphaning it. The difference between this and
//!   a dropped future is the whole point.

use std::sync::Arc;
use std::time::Duration;

use faucet_conformance::scripted::{Boundary, Event, EventLog, PagedSource, ScriptedSink};
use faucet_core::{CancellationToken, Pipeline};

// ─── A3: overwrite atomicity ─────────────────────────────────────────────────

#[tokio::test]
async fn a_successful_overwrite_stages_then_commits_exactly_once() {
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone()).overwrite();
    let source = PagedSource::new(3, 4);

    Pipeline::new(&source, &sink)
        .run()
        .await
        .expect("a clean overwrite run succeeds");

    let events = log.events();
    let begin = events
        .iter()
        .position(|e| matches!(e, Event::BeginOverwrite))
        .expect("the staging target must be prepared before any write");
    let commit = events
        .iter()
        .position(|e| matches!(e, Event::CommitOverwrite))
        .expect("a successful run must commit the swap");
    let first_write = events
        .iter()
        .position(|e| matches!(e, Event::Write(_)))
        .expect("the run wrote data");
    let last_write = events
        .iter()
        .rposition(|e| matches!(e, Event::Write(_)))
        .expect("the run wrote data");

    assert!(
        begin < first_write,
        "begin_overwrite must precede the first write, else the first rows land in \
         the live destination: {events:?}"
    );
    assert!(
        last_write < commit,
        "the swap must come after every write, else it publishes a partial dataset: {events:?}"
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Event::CommitOverwrite))
            .count(),
        1,
        "exactly one swap per run: {events:?}"
    );
    assert!(
        !log.any(|e| matches!(e, Event::AbortOverwrite)),
        "a successful run must not also abort: {events:?}"
    );
}

#[tokio::test]
async fn a_failed_write_aborts_the_overwrite_and_never_swaps() {
    // The destructive case: if the engine swapped here, the live destination
    // would be replaced by a partial load.
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone())
        .overwrite()
        .failing_at(Boundary::Write(1));
    let source = PagedSource::new(4, 3);

    Pipeline::new(&source, &sink)
        .run()
        .await
        .expect_err("a failed write must fail the run");

    assert!(
        !log.any(|e| matches!(e, Event::CommitOverwrite)),
        "a failed run must NEVER commit the swap — the prior destination would be \
         replaced by a partial load: {:?}",
        log.events()
    );
    assert!(
        log.any(|e| matches!(e, Event::AbortOverwrite)),
        "a failed run must discard the staged data: {:?}",
        log.events()
    );
}

#[tokio::test]
async fn a_failed_flush_aborts_the_overwrite_and_never_swaps() {
    // Subtler: every write succeeded, so the staged data *looks* complete. But
    // flush is what makes it durable, and it failed — publishing it would
    // expose a partially-written staging target as the live destination.
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone())
        .overwrite()
        .failing_at(Boundary::Flush);
    let source = PagedSource::new(3, 3);

    Pipeline::new(&source, &sink)
        .run()
        .await
        .expect_err("a failed flush must fail the run");

    assert!(
        !log.any(|e| matches!(e, Event::CommitOverwrite)),
        "unflushed staged data must not be published: {:?}",
        log.events()
    );
    assert!(log.any(|e| matches!(e, Event::AbortOverwrite)));
}

#[tokio::test]
async fn a_failing_commit_surfaces_rather_than_reporting_success() {
    // If the swap itself fails, the run must not report success: the operator
    // would believe the refresh landed when the destination still holds the old
    // data.
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone())
        .overwrite()
        .failing_at(Boundary::CommitOverwrite);
    let source = PagedSource::new(2, 3);

    let err = Pipeline::new(&source, &sink)
        .run()
        .await
        .expect_err("a failed swap must fail the run");
    assert!(
        matches!(err, faucet_core::FaucetError::Sink(_)),
        "got {err:?}"
    );
    assert!(
        !log.any(|e| matches!(e, Event::CommitOverwrite)),
        "the swap did not happen, so it must not be logged as having happened"
    );
}

#[tokio::test]
async fn a_cancelled_overwrite_does_not_swap() {
    // A cancel is not a success. Publishing a partial refresh because the
    // operator pressed Ctrl-C would be the same data loss as the failure case,
    // just harder to notice.
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone())
        .overwrite()
        .with_write_delay(Duration::from_millis(30));
    let source = PagedSource::new(50, 2);
    let cancel = CancellationToken::new();

    let canceller = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(45)).await;
        canceller.cancel();
    });

    let _ = Pipeline::new(&source, &sink)
        .with_cancel(cancel)
        .run()
        .await;

    assert!(
        !log.any(|e| matches!(e, Event::CommitOverwrite)),
        "a cancelled overwrite must not publish its partial staging target: {:?}",
        log.events()
    );
}

#[tokio::test]
async fn suppressing_the_lifecycle_leaves_the_swap_to_the_caller() {
    // The fan-out case (#552): an external orchestrator owns the swap so it can
    // run it once across many invocations. The engine must then drive neither
    // half — a `begin` here would stage twice, a `commit` would publish after
    // the first invocation of the group.
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone()).overwrite();
    let source = PagedSource::new(2, 3);

    Pipeline::new(&source, &sink)
        .with_suppress_overwrite(true)
        .run()
        .await
        .expect("the run itself still succeeds");

    assert!(
        !log.any(|e| matches!(
            e,
            Event::BeginOverwrite | Event::CommitOverwrite | Event::AbortOverwrite
        )),
        "a suppressed lifecycle must leave every phase to the caller: {:?}",
        log.events()
    );
    assert!(
        log.any(|e| matches!(e, Event::Write(_))),
        "but the data must still be written to the staging target"
    );
}

// ─── A8: cancel still flushes ────────────────────────────────────────────────

#[tokio::test]
async fn a_cancelled_run_flushes_what_it_accepted() {
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone()).with_write_delay(Duration::from_millis(25));
    let store = Arc::new(sink.state_store());
    let source = PagedSource::new(50, 2);
    let cancel = CancellationToken::new();

    let canceller = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(60)).await;
        canceller.cancel();
    });

    let result = Pipeline::new(&source, &sink)
        .with_state_store(store.clone())
        .with_cancel(cancel)
        .run()
        .await
        .expect("a cancelled run stops cleanly rather than erroring");

    // It stopped early — otherwise this test is not exercising cancellation.
    assert!(
        result.records_written < source.total_records(),
        "the run was not actually cancelled mid-stream: wrote {} of {}",
        result.records_written,
        source.total_records()
    );
    assert!(
        result.records_written > 0,
        "the cancel fired before any page landed, so the flush path is untested"
    );

    // The guarantee: a flush happened, so a buffered sink committed rather than
    // orphaning its partial output.
    assert!(
        log.any(|e| matches!(e, Event::Flush)),
        "a cancelled run must flush — this is the difference between a cooperative \
         cancel and a dropped future: {:?}",
        log.events()
    );

    // And the bookmark reflects only flushed data.
    let events = log.events();
    let last_flush = events
        .iter()
        .rposition(|e| matches!(e, Event::Flush))
        .expect("a flush occurred");
    let bookmarks_after_flush = events[last_flush..]
        .iter()
        .filter(|e| matches!(e, Event::StatePut { .. }))
        .count();
    assert!(
        bookmarks_after_flush <= 1,
        "at most the final bookmark may follow the last flush: {events:?}"
    );
}

#[tokio::test]
async fn cancelling_before_the_first_page_writes_nothing_and_claims_nothing() {
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone()).with_write_delay(Duration::from_millis(50));
    let store = Arc::new(sink.state_store());
    let source = PagedSource::new(20, 2);
    let cancel = CancellationToken::new();
    cancel.cancel(); // already cancelled before the run starts

    let result = Pipeline::new(&source, &sink)
        .with_state_store(store.clone())
        .with_cancel(cancel)
        .run()
        .await
        .expect("an immediately-cancelled run is not an error");

    assert_eq!(result.records_written, 0);
    assert_eq!(
        store.stored("scripted:paged"),
        None,
        "nothing was written, so nothing may be claimed durable: {:?}",
        log.events()
    );
}

#[tokio::test]
async fn an_uncancelled_run_is_the_control_and_consumes_everything() {
    // Guards the two tests above: if the cancel token were ignored entirely,
    // they would need to still detect it. This pins what "not cancelled" looks
    // like so the partial-consumption assertion above means something.
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone());
    let source = PagedSource::new(6, 2);

    let result = Pipeline::new(&source, &sink)
        .with_cancel(CancellationToken::new())
        .run()
        .await
        .expect("an uncancelled run completes");

    assert_eq!(result.records_written, source.total_records());
    assert!(log.any(|e| matches!(e, Event::Flush)));
}
