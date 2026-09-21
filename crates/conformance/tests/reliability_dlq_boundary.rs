//! #651 — the dead-letter path's ordering guarantees.
//!
//! Every other reliability suite injects a failure that stops the run. The
//! DLQ is the one error path where the run **keeps going and stays green**,
//! which is exactly why getting it wrong is so expensive: rows are quietly
//! dropped, or a bookmark advances past rows that were never made durable
//! anywhere, and nothing in the exit code says so.
//!
//! Three orderings matter, and none of them is a property of a single
//! function — only of the sequence the engine calls its collaborators in:
//!
//! 1. A quarantined row reaches the DLQ **before** the bookmark covering its
//!    page is persisted. Otherwise a crash in between loses the row from both
//!    the destination and the dead-letter file.
//! 2. `on_batch_error: propagate` must **not** advance the bookmark.
//! 3. A DLQ sink that itself fails must fail the run, not swallow the row.
//!
//! So these run the real `Pipeline::run` against the scripted doubles and
//! assert against one interleaved event log shared by the main sink, the DLQ
//! sink and the state store.

use std::sync::Arc;

use faucet_conformance::scripted::{Boundary, Event, EventLog, PagedSource, RowMask, ScriptedSink};
use faucet_core::Pipeline;
use faucet_core::dlq::{DlqConfig, OnBatchError};

fn dlq_envelopes(log: &EventLog) -> usize {
    log.events()
        .iter()
        .map(|e| match e {
            Event::DlqWrite(n) => *n,
            _ => 0,
        })
        .sum()
}

/// Index of the first DLQ write, and of the first persisted bookmark.
fn first_dlq_and_first_bookmark(log: &EventLog) -> (Option<usize>, Option<usize>) {
    (
        log.position(|e| matches!(e, Event::DlqWrite(_))),
        log.position(|e| matches!(e, Event::StatePut { .. })),
    )
}

/// The control arm. With nothing failing, the DLQ must stay completely
/// untouched — a harness that wrote to it unconditionally would make every
/// other test in this file vacuous.
#[tokio::test]
async fn a_clean_run_writes_nothing_to_the_dlq() {
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone());
    let dlq = Arc::new(ScriptedSink::new(log.clone()).as_dlq());
    let store = Arc::new(sink.state_store());
    let source = PagedSource::new(3, 4);

    let result = Pipeline::new(&source, &sink)
        .with_state_store(store)
        .with_dlq(DlqConfig::new(dlq))
        .run()
        .await
        .expect("a clean run succeeds");

    assert_eq!(result.records_written, 12);
    assert_eq!(dlq_envelopes(&log), 0, "log: {:?}", log.events());
}

/// The core ordering guarantee: a row the sink rejected is durable in the DLQ
/// **before** the bookmark covering its page is persisted. Reverse the two and
/// a crash in the window loses the row entirely — it is neither in the
/// destination nor in the dead-letter file, and the resumed run skips its page.
#[tokio::test]
async fn a_quarantined_row_reaches_the_dlq_before_its_page_bookmark() {
    let log = EventLog::new();
    // Row 1 of the first page fails; the other three rows land normally.
    let sink = ScriptedSink::new(log.clone()).failing_at(Boundary::RowsInWrite {
        batch: 0,
        rows: RowMask::just(1),
    });
    let dlq = Arc::new(ScriptedSink::new(log.clone()).as_dlq());
    let store = Arc::new(sink.state_store());
    let source = PagedSource::new(2, 4);

    Pipeline::new(&source, &sink)
        .with_state_store(store)
        .with_dlq(DlqConfig {
            on_batch_error: OnBatchError::DlqAll,
            ..DlqConfig::new(dlq)
        })
        .run()
        .await
        .expect("a row-level failure must not fail the run when a DLQ is configured");

    assert_eq!(
        dlq_envelopes(&log),
        1,
        "exactly the failed row is dead-lettered: {:?}",
        log.events()
    );

    let (first_dlq, first_bookmark) = first_dlq_and_first_bookmark(&log);
    let first_dlq = first_dlq.expect("the failed row must be dead-lettered");
    let first_bookmark = first_bookmark.expect("the surviving rows must still bookmark");
    assert!(
        first_dlq < first_bookmark,
        "the bookmark for a page was persisted before that page's rejected row \
         was durable in the DLQ — a crash in that window loses the row from \
         both destinations.\nlog: {:?}",
        log.events()
    );
}

/// The surviving rows of a partially-failed page are still written, and the
/// bookmark still advances. A pipeline that failed the whole page here would
/// turn one bad row into a stalled stream.
#[tokio::test]
async fn the_surviving_rows_of_a_partial_page_are_written_and_bookmarked() {
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone()).failing_at(Boundary::RowsInWrite {
        batch: 1,
        rows: RowMask::of(&[0, 2]),
    });
    let dlq = Arc::new(ScriptedSink::new(log.clone()).as_dlq());
    let store = Arc::new(sink.state_store());
    let source = PagedSource::new(3, 4);

    Pipeline::new(&source, &sink)
        .with_state_store(store)
        .with_dlq(DlqConfig {
            on_batch_error: OnBatchError::DlqAll,
            ..DlqConfig::new(dlq)
        })
        .run()
        .await
        .expect("a partial page must not fail the run");

    assert_eq!(dlq_envelopes(&log), 2, "log: {:?}", log.events());
    // 12 records, 2 dead-lettered → 10 confirmed by the main sink.
    assert_eq!(
        log.records_written(),
        10,
        "the rows that did land must still be written: {:?}",
        log.events()
    );
    // And the run still reaches the final bookmark rather than stalling.
    let bookmarks = log.bookmarks();
    assert_eq!(
        bookmarks.last().and_then(|b| b["page"].as_u64()),
        Some(2),
        "the stream must reach its last page: {bookmarks:?}"
    );
}

/// `on_batch_error: propagate` is the opposite contract: a wholesale sink
/// failure aborts the run. The bookmark for the failed page must not advance,
/// or the resumed run skips data that was never written anywhere.
#[tokio::test]
async fn propagate_aborts_and_never_bookmarks_the_failed_page() {
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone()).failing_at(Boundary::Write(1));
    let dlq = Arc::new(ScriptedSink::new(log.clone()).as_dlq());
    let store = Arc::new(sink.state_store());
    let source = PagedSource::new(4, 3);

    let err = Pipeline::new(&source, &sink)
        .with_state_store(store)
        .with_dlq(DlqConfig::new(dlq)) // Propagate is the default.
        .run()
        .await
        .expect_err("propagate must surface the sink failure");
    assert!(
        err.to_string().contains("scripted sink"),
        "the original sink error must survive, not be replaced: {err}"
    );

    // Page 0 succeeded, so its bookmark is legitimate; page 1's must not exist.
    let pages: Vec<u64> = log
        .bookmarks()
        .iter()
        .filter_map(|b| b["page"].as_u64())
        .collect();
    assert!(
        !pages.contains(&1),
        "the failed page must never be bookmarked: {pages:?}"
    );
}

/// A DLQ that cannot accept the row must fail the run. Swallowing the error
/// here is the worst outcome available: the row is in neither destination and
/// the run still reports success.
#[tokio::test]
async fn a_failing_dlq_sink_fails_the_run_rather_than_dropping_the_row() {
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone()).failing_at(Boundary::RowsInWrite {
        batch: 0,
        rows: RowMask::just(0),
    });
    // The DLQ itself rejects its very first write.
    let dlq = Arc::new(
        ScriptedSink::new(log.clone())
            .as_dlq()
            .failing_at(Boundary::Write(0)),
    );
    let store = Arc::new(sink.state_store());
    let source = PagedSource::new(2, 3);

    let result = Pipeline::new(&source, &sink)
        .with_state_store(store)
        .with_dlq(DlqConfig {
            on_batch_error: OnBatchError::DlqAll,
            ..DlqConfig::new(dlq)
        })
        .run()
        .await;

    assert!(
        result.is_err(),
        "a DLQ write failure must not be swallowed — the row would be lost \
         from both destinations while the run reported success.\nlog: {:?}",
        log.events()
    );
    assert_eq!(
        dlq_envelopes(&log),
        0,
        "nothing was durably dead-lettered: {:?}",
        log.events()
    );
}

/// The per-page failure budget is an abort, not a silent cap: exceeding it
/// must fail the run rather than quietly dead-lettering fewer rows than
/// failed.
#[tokio::test]
async fn exceeding_the_page_failure_budget_fails_the_run() {
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone()).failing_at(Boundary::RowsInWrite {
        batch: 0,
        rows: RowMask::of(&[0, 1, 2]),
    });
    let dlq = Arc::new(ScriptedSink::new(log.clone()).as_dlq());
    let store = Arc::new(sink.state_store());
    let source = PagedSource::new(1, 4);

    let result = Pipeline::new(&source, &sink)
        .with_state_store(store)
        .with_dlq(DlqConfig {
            on_batch_error: OnBatchError::DlqAll,
            max_failures_per_page: Some(2),
            ..DlqConfig::new(dlq)
        })
        .run()
        .await;

    assert!(
        result.is_err(),
        "3 failures against a budget of 2 must abort: {:?}",
        log.events()
    );
}

/// Whether a rejected row is dead-lettered or fails the run is decided by one
/// thing: **is a DLQ configured?** Without one the engine takes the plain
/// `write_batch` path, so a sink's per-row error reporting is never consulted
/// — there would be nowhere to route the row. Pinning both arms of that
/// decision with the *same* scripted sink is what shows the routing is the
/// engine's choice and not the sink's.
#[tokio::test]
async fn the_dlq_presence_decides_whether_row_outcomes_are_consulted_at_all() {
    let rows = Boundary::RowsInWrite {
        batch: 0,
        rows: RowMask::just(1),
    };

    // No DLQ: the plain write path, so nothing is dead-lettered and the whole
    // page lands.
    let plain_log = EventLog::new();
    let plain = ScriptedSink::new(plain_log.clone()).failing_at(rows);
    let store = Arc::new(plain.state_store());
    let result = Pipeline::new(&PagedSource::new(2, 4), &plain)
        .with_state_store(store)
        .run()
        .await
        .expect("the plain write path is not row-scripted");
    assert_eq!(result.records_written, 8);
    assert_eq!(dlq_envelopes(&plain_log), 0);

    // Same sink, same boundary, with a DLQ: now the row is routed.
    let dlq_log = EventLog::new();
    let scripted = ScriptedSink::new(dlq_log.clone()).failing_at(rows);
    let dlq = Arc::new(ScriptedSink::new(dlq_log.clone()).as_dlq());
    let store = Arc::new(scripted.state_store());
    Pipeline::new(&PagedSource::new(2, 4), &scripted)
        .with_state_store(store)
        .with_dlq(DlqConfig {
            on_batch_error: OnBatchError::DlqAll,
            ..DlqConfig::new(dlq)
        })
        .run()
        .await
        .expect("the row-level failure is absorbed by the DLQ");
    assert_eq!(
        dlq_envelopes(&dlq_log),
        1,
        "the same rejected row must be dead-lettered once a DLQ exists: {:?}",
        dlq_log.events()
    );
}
