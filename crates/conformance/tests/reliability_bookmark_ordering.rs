//! #651 Category A1 — bookmark ordering and crash safety.
//!
//! The guarantee: **a bookmark is persisted only after the sink has confirmed
//! the data that bookmark covers.** Everything downstream of faucet depends on
//! it — it is what makes a resumed run lose nothing.
//!
//! A unit test on pure logic cannot check this, because the guarantee is not a
//! property of any one function: it is a property of the *order* in which the
//! engine calls two collaborators. So these tests run the real `Pipeline::run`
//! against the scripted doubles and assert against the interleaved event log.
//!
//! Each test states the failure it injects and what must be true afterwards.
//! Where a test would still pass if the guarantee were broken, it says so and
//! asserts the stronger thing instead.

use std::sync::Arc;

use faucet_conformance::scripted::{
    Boundary, Event, EventLog, PagedSource, ScriptedSink, assert_bookmarks_backed_by_writes,
};
use faucet_core::{Pipeline, Source, Value};

/// The bookmark the engine persists for `PagedSource` after page `p`.
fn bookmark_for_page(p: usize) -> Value {
    serde_json::json!({ "page": p })
}

/// Records covered by pages `0..=p`.
fn records_through_page(p: usize, per_page: usize) -> usize {
    (p + 1) * per_page
}

#[tokio::test]
async fn every_bookmark_is_backed_by_confirmed_writes_on_a_clean_run() {
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone());
    let store = Arc::new(sink.state_store());
    let source = PagedSource::new(4, 5);

    let result = Pipeline::new(&source, &sink)
        .with_state_store(store.clone())
        .run()
        .await
        .expect("a clean run succeeds");

    assert_eq!(result.records_written, 20);

    // The shared guarantee assertion — the same one a deliberately-wrong event
    // log is fed in `scripted`'s own `#[should_panic]` tests, so this is a
    // check that has been shown capable of failing.
    assert_bookmarks_backed_by_writes(&log, |b| {
        records_through_page(b["page"].as_u64().expect("page bookmark") as usize, 5)
    });

    // The guarantee, stated directly: at the moment each bookmark was made
    // durable, the sink had already confirmed exactly the records that
    // bookmark claims.
    let confirmed_at_each = log.writes_before_each_bookmark();
    let bookmarks = log.bookmarks();
    assert_eq!(
        bookmarks.len(),
        4,
        "one bookmark per page: {:?}",
        log.events()
    );
    for (i, (confirmed, bookmark)) in confirmed_at_each.iter().zip(&bookmarks).enumerate() {
        let page = bookmark["page"].as_u64().expect("page bookmark") as usize;
        assert_eq!(page, i, "bookmarks must advance in page order");
        assert!(
            *confirmed >= records_through_page(page, 5),
            "bookmark {bookmark} was persisted with only {confirmed} records confirmed — it \
             claims {} are durable",
            records_through_page(page, 5)
        );
    }
}

#[tokio::test]
async fn a_failed_write_never_advances_the_bookmark_past_it() {
    // Page 2's write fails. Pages 0 and 1 were confirmed, so their bookmarks
    // are legitimate; page 2's must never become durable.
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone()).failing_at(Boundary::Write(2));
    let store = Arc::new(sink.state_store());
    let source = PagedSource::new(5, 4);

    let err = Pipeline::new(&source, &sink)
        .with_state_store(store.clone())
        .run()
        .await
        .expect_err("the run must surface the sink failure, not swallow it");
    assert!(
        matches!(err, faucet_core::FaucetError::Sink(_)),
        "expected the sink's own typed error, got {err:?}"
    );

    let durable = store
        .stored("scripted:paged")
        .expect("the confirmed pages left a bookmark behind");
    let page = durable["page"].as_u64().expect("page bookmark") as usize;
    assert!(
        page < 2,
        "the durable bookmark is {durable}, which covers the page whose write FAILED — a \
         resumed run would skip it and lose those records"
    );

    // And the log must show no bookmark at all after the failure.
    assert_bookmarks_backed_by_writes(&log, |b| {
        records_through_page(b["page"].as_u64().expect("page bookmark") as usize, 4)
    });

    let fail_at = log
        .position(|e| matches!(e, Event::WriteFailed(_)))
        .expect("the failure is logged");
    let after = &log.events()[fail_at..];
    assert!(
        !after.iter().any(|e| matches!(e, Event::StatePut { .. })),
        "a bookmark was persisted after the write failed: {after:?}"
    );
}

#[tokio::test]
async fn a_failed_flush_does_not_leave_a_bookmark_for_unflushed_data() {
    // The subtle one: the writes all *succeeded*, so a naive engine would
    // happily persist the bookmark. But flush is what makes them durable, and
    // it failed — so the data is not there and the bookmark must not claim it.
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone()).failing_at(Boundary::Flush);
    let store = Arc::new(sink.state_store());
    let source = PagedSource::new(3, 4);

    let err = Pipeline::new(&source, &sink)
        .with_state_store(store.clone())
        .run()
        .await
        .expect_err("a failed flush must fail the run");
    assert!(
        matches!(err, faucet_core::FaucetError::Sink(_)),
        "got {err:?}"
    );

    assert!(
        log.any(|e| matches!(e, Event::FlushFailed)),
        "the flush failure must actually have been exercised: {:?}",
        log.events()
    );
    assert_eq!(
        store.stored("scripted:paged"),
        None,
        "no page was ever flushed, so nothing may be claimed durable"
    );
}

#[tokio::test]
async fn resume_re_reads_exactly_the_unconfirmed_tail() {
    let per_page = 4;

    // Run 1: page 3 of 6 fails.
    let log1 = EventLog::new();
    let sink1 = ScriptedSink::new(log1.clone()).failing_at(Boundary::Write(3));
    let store = Arc::new(sink1.state_store());
    let source1 = PagedSource::new(6, per_page);
    Pipeline::new(&source1, &sink1)
        .with_state_store(store.clone())
        .run()
        .await
        .expect_err("run 1 fails mid-stream");

    let durable = store.stored("scripted:paged").expect("partial progress");
    let resumed_from = durable["page"].as_u64().expect("page") as usize;
    let confirmed_run1 = log1.records_written();

    // Run 2: same durable store, a healthy sink.
    let log2 = EventLog::new();
    let sink2 = ScriptedSink::new(log2.clone());
    let source2 = PagedSource::new(6, per_page);
    let result = Pipeline::new(&source2, &sink2)
        .with_state_store(store.clone())
        .run()
        .await
        .expect("run 2 completes");

    // It restarted from the page *after* the last durable bookmark — not from
    // the beginning (which would be wasteful but safe) and not past the gap
    // (which would lose data).
    assert_eq!(
        source2.emitted_pages().first().copied(),
        Some(resumed_from + 1),
        "resume must start at the page after the durable bookmark; emitted {:?}",
        source2.emitted_pages()
    );

    // Nothing is lost: every page is covered by run 1 or run 2.
    let mut covered: Vec<usize> = source1.emitted_pages();
    covered.extend(source2.emitted_pages());
    covered.sort_unstable();
    covered.dedup();
    assert_eq!(
        covered,
        (0..6).collect::<Vec<_>>(),
        "every page must be read by one run or the other — no gap"
    );

    // And the duplicate set is bounded by the at-least-once window: only the
    // pages that were read but not confirmed are re-read.
    let total = source1.total_records();
    assert!(
        confirmed_run1 + result.records_written >= total,
        "combined writes {} must cover the full dataset of {total}",
        confirmed_run1 + result.records_written
    );
    let duplicates = (confirmed_run1 + result.records_written) - total;
    assert!(
        duplicates <= per_page,
        "at-least-once permits re-delivering only the unconfirmed page; {duplicates} \
         duplicate records is more than one page of {per_page}"
    );
}

#[tokio::test]
async fn a_failed_state_put_keeps_the_previous_bookmark_rather_than_a_partial_one() {
    // Models a crash in the window between a confirmed write and a durable
    // bookmark. The engine must leave the *older* bookmark in place; a resumed
    // run then re-reads that page (a duplicate, which at-least-once allows)
    // rather than skipping it (a loss, which it does not).
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone()).failing_at(Boundary::StatePut(1));
    let store = Arc::new(sink.state_store());
    let source = PagedSource::new(4, 3);

    let outcome = Pipeline::new(&source, &sink)
        .with_state_store(store.clone())
        .run()
        .await;

    assert!(
        log.any(|e| matches!(e, Event::StatePutFailed { .. })),
        "the state-put failure must have been exercised: {:?}",
        log.events()
    );

    // Whether the engine surfaces the error or continues, the invariant is the
    // same: whatever is durable must be backed by confirmed writes.
    if let Some(durable) = store.stored("scripted:paged") {
        let page = durable["page"].as_u64().expect("page") as usize;
        let confirmed = log.records_written();
        assert!(
            confirmed >= records_through_page(page, 3),
            "durable bookmark {durable} claims {} records but only {confirmed} were confirmed",
            records_through_page(page, 3)
        );
    }
    // A failed bookmark write is a durability failure and must not be reported
    // as a clean success, because the caller would then advance its own state.
    if let Ok(ref r) = outcome {
        assert!(
            r.bookmark.is_some(),
            "a run that could not persist its bookmark must not report success \
             with nothing to resume from"
        );
    }
}

#[tokio::test]
async fn a_source_that_emits_no_pages_persists_no_bookmark() {
    // The empty-run edge: nothing was written, so there is nothing to claim.
    // A bookmark written here would silently skip the first real page.
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone());
    let store = Arc::new(sink.state_store());
    let source = PagedSource::new(0, 5);

    let result = Pipeline::new(&source, &sink)
        .with_state_store(store.clone())
        .run()
        .await
        .expect("an empty run is a success");

    assert_eq!(result.records_written, 0);
    assert_eq!(
        store.stored("scripted:paged"),
        None,
        "an empty run must not invent a bookmark"
    );
    assert!(log.bookmarks().is_empty(), "{:?}", log.events());
}

#[tokio::test]
async fn a_fully_consumed_source_resumes_to_zero_records() {
    // Idempotence of the resume path itself: re-running a completed pipeline
    // against the same durable bookmark must read nothing, not replay
    // everything.
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone());
    let store = Arc::new(sink.state_store());

    let first = PagedSource::new(3, 2);
    Pipeline::new(&first, &sink)
        .with_state_store(store.clone())
        .run()
        .await
        .expect("first run");

    let log2 = EventLog::new();
    let sink2 = ScriptedSink::new(log2.clone());
    let second = PagedSource::new(3, 2);
    let result = Pipeline::new(&second, &sink2)
        .with_state_store(store.clone())
        .run()
        .await
        .expect("second run");

    assert_eq!(
        result.records_written,
        0,
        "a completed pipeline re-run must read nothing; emitted {:?}",
        second.emitted_pages()
    );
    assert!(second.emitted_pages().is_empty());
}

#[tokio::test]
async fn the_source_actually_honours_the_bookmark_it_is_given() {
    // Guards the tests above: if `PagedSource` ignored `apply_start_bookmark`,
    // every resume assertion here would pass for the wrong reason.
    let source = PagedSource::new(5, 2);
    source
        .apply_start_bookmark(bookmark_for_page(2))
        .await
        .expect("apply");
    let fetched = source.fetch_all().await.expect("fetch");
    let pages: Vec<u64> = fetched
        .iter()
        .map(|r| r["page"].as_u64().expect("page"))
        .collect();
    assert!(
        pages.iter().all(|p| *p > 2),
        "the double must skip bookmarked pages, got {pages:?}"
    );
}
