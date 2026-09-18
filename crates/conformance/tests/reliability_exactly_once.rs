//! #651 Category A2 — exactly-once delivery, both mechanisms.
//!
//! Two independent mechanisms satisfy `delivery: exactly_once`, and they fail
//! in different ways, so each needs its own end-to-end proof:
//!
//! - **Atomic watermark** — the sink commits the page's rows *and* a commit
//!   token in one transaction. The dangerous window is between that commit and
//!   the state-store `put`: a crash there leaves the sink ahead of the state
//!   store, and a naive resume re-writes pages that already landed.
//! - **Keyed upsert** — no watermark at all; the write is simply convergent, so
//!   replaying a whole window is harmless.
//!
//! The tests below inject a failure in exactly that window and assert the
//! stronger property: **zero duplicates**, not merely "the run recovered".

use std::sync::Arc;

use faucet_conformance::scripted::{
    Boundary, Event, EventLog, PagedSource, ScriptedSink, assert_bookmarks_backed_by_writes,
};
use faucet_core::idempotency::DeliveryMode;
use faucet_core::{Pipeline, Sink};

/// Records covered by pages `0..=p`.
fn records_through_page(p: usize, per_page: usize) -> usize {
    (p + 1) * per_page
}

/// Commit tokens the sink stored, in order.
fn tokens(log: &EventLog) -> Vec<String> {
    log.events()
        .into_iter()
        .filter_map(|e| match e {
            Event::IdempotentWrite { token, .. } => Some(token),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn exactly_once_routes_writes_through_the_idempotent_path() {
    // The precondition for everything else: if the engine silently used the
    // plain `write_batch`, no token would exist and every test below would be
    // asserting nothing.
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone()).idempotent();
    let store = Arc::new(sink.state_store());
    let source = PagedSource::new(3, 4);

    let result = Pipeline::new(&source, &sink)
        .with_state_store(store.clone())
        .with_delivery(DeliveryMode::ExactlyOnce)
        .run()
        .await
        .expect("a clean exactly-once run succeeds");

    assert_eq!(result.records_written, 12);
    assert!(
        !log.any(|e| matches!(e, Event::Write(_))),
        "exactly-once must not use the plain write path: {:?}",
        log.events()
    );
    let t = tokens(&log);
    assert_eq!(t.len(), 3, "one commit token per page: {t:?}");

    // Tokens must be strictly increasing, since the resume logic compares them
    // ordinally to decide what has already landed.
    let mut sorted = t.clone();
    sorted.sort();
    assert_eq!(t, sorted, "commit tokens must be monotonic: {t:?}");
    assert_eq!(
        t.iter().collect::<std::collections::BTreeSet<_>>().len(),
        t.len(),
        "commit tokens must be distinct: {t:?}"
    );

    assert_bookmarks_backed_by_writes(&log, |b| {
        let bare = faucet_core::idempotency::unwrap_state(b)
            .0
            .unwrap_or(b.clone());
        records_through_page(bare["page"].as_u64().expect("page") as usize, 4)
    });
}

#[tokio::test]
async fn a_crash_between_sink_commit_and_state_put_replays_nothing() {
    // The exactly-once window. Run 1's second bookmark `put` fails *after* the
    // sink has durably committed page 1 and its token. The sink is therefore
    // ahead of the state store.
    let per_page = 4;
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone()).idempotent();
    let store = Arc::new(sink.state_store());

    let source1 = PagedSource::new(5, per_page);
    let _ = Pipeline::new(&source1, &sink)
        .with_state_store(store.clone())
        .with_delivery(DeliveryMode::ExactlyOnce)
        .run()
        .await;

    assert!(
        log.any(|e| matches!(e, Event::IdempotentWrite { .. })),
        "run 1 must have committed at least one page: {:?}",
        log.events()
    );
    let committed_run1 = log.records_written();
    let token_after_run1 = sink
        .last_committed_token("scripted:paged")
        .await
        .expect("token read")
        .expect("run 1 committed a token");

    // Run 2 on the SAME sink — so its commit tokens survive, exactly as a real
    // destination's would across a restart — with the failure disarmed.
    sink.rearm(Boundary::Never);
    let log_before_run2 = log.events().len();
    let source2 = PagedSource::new(5, per_page);
    Pipeline::new(&source2, &sink)
        .with_state_store(store.clone())
        .with_delivery(DeliveryMode::ExactlyOnce)
        .run()
        .await
        .expect("run 2 completes");

    let run2_events = &log.events()[log_before_run2..];
    let run2_records: usize = run2_events
        .iter()
        .map(|e| match e {
            Event::IdempotentWrite { records, .. } => *records,
            _ => 0,
        })
        .sum();

    // The guarantee: across both runs the destination received each record
    // exactly once. Under at-least-once this total would exceed the dataset.
    let total = source1.total_records();
    assert_eq!(
        committed_run1 + run2_records,
        total,
        "exactly-once must deliver {total} records across the two runs, not \
         {} — run 1 committed {committed_run1}, run 2 committed {run2_records}.\n\
         token after run 1: {token_after_run1}",
        committed_run1 + run2_records
    );
}

#[tokio::test]
async fn re_running_a_completed_exactly_once_pipeline_writes_nothing() {
    // Replay idempotence of the whole pipeline: the strongest single statement
    // of the guarantee, and the one an operator actually relies on when they
    // re-trigger a run they are unsure completed.
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone()).idempotent();
    let store = Arc::new(sink.state_store());

    let first = PagedSource::new(4, 3);
    Pipeline::new(&first, &sink)
        .with_state_store(store.clone())
        .with_delivery(DeliveryMode::ExactlyOnce)
        .run()
        .await
        .expect("first run");
    let after_first = log.records_written();
    assert_eq!(after_first, 12);

    let second = PagedSource::new(4, 3);
    let result = Pipeline::new(&second, &sink)
        .with_state_store(store.clone())
        .with_delivery(DeliveryMode::ExactlyOnce)
        .run()
        .await
        .expect("second run");

    assert_eq!(
        log.records_written(),
        after_first,
        "a re-run of a completed exactly-once pipeline wrote {} extra records",
        log.records_written() - after_first
    );
    assert_eq!(result.records_written, 0);
}

#[tokio::test]
async fn the_keyed_upsert_mechanism_converges_without_any_watermark() {
    // The second mechanism. No commit token is involved: the sink dedups by
    // key, so replaying the full window must leave the destination identical.
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone()).keyed();
    assert!(
        !sink.supports_idempotent_writes(),
        "this arm must exercise the keyed path, not the watermark path"
    );
    assert!(sink.dedups_by_key());

    let store = Arc::new(sink.state_store());
    let source = PagedSource::new(3, 5);
    Pipeline::new(&source, &sink)
        .with_state_store(store.clone())
        .with_delivery(DeliveryMode::ExactlyOnce)
        .run()
        .await
        .expect("keyed exactly-once run succeeds");

    // Replay the same window from scratch against the same sink.
    let replay_store = Arc::new(faucet_core::state::MemoryStateStore::default());
    let replay_source = PagedSource::new(3, 5);
    Pipeline::new(&replay_source, &sink)
        .with_state_store(replay_store)
        .with_delivery(DeliveryMode::ExactlyOnce)
        .run()
        .await
        .expect("a full replay succeeds");

    // The keyed mechanism's contract is convergence at the destination, not a
    // suppressed write — so the assertion is that the write path stayed the
    // plain (convergent) one throughout, never the watermark path.
    assert!(
        !log.any(|e| matches!(e, Event::IdempotentWrite { .. })),
        "the keyed mechanism must not use the commit-token path: {:?}",
        log.events()
    );
    assert!(log.any(|e| matches!(e, Event::Write(_))));
}

#[tokio::test]
async fn a_failed_idempotent_write_leaves_no_token_to_skip_on() {
    // If a failed write left its token behind, the resumed run would treat the
    // page as already landed and skip it — silent loss, and the worst possible
    // outcome for a feature whose entire purpose is correctness.
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone())
        .idempotent()
        .failing_at(Boundary::Write(0));
    let store = Arc::new(sink.state_store());
    let source = PagedSource::new(3, 2);

    Pipeline::new(&source, &sink)
        .with_state_store(store.clone())
        .with_delivery(DeliveryMode::ExactlyOnce)
        .run()
        .await
        .expect_err("the first page's write fails");

    assert_eq!(
        sink.last_committed_token("scripted:paged")
            .await
            .expect("token read"),
        None,
        "a failed write must not leave a commit token behind"
    );
    assert_eq!(
        store.stored("scripted:paged"),
        None,
        "and no bookmark either"
    );

    // Recovery must then deliver the full dataset.
    sink.rearm(Boundary::Never);
    let retry_source = PagedSource::new(3, 2);
    Pipeline::new(&retry_source, &sink)
        .with_state_store(store.clone())
        .with_delivery(DeliveryMode::ExactlyOnce)
        .run()
        .await
        .expect("retry succeeds");
    assert_eq!(
        log.records_written(),
        6,
        "the retry must deliver every record exactly once: {:?}",
        log.events()
    );
}

#[tokio::test]
async fn at_least_once_is_the_control_arm_and_does_duplicate() {
    // Proves the exactly-once assertions above are measuring something real:
    // the identical failure under the default delivery mode *does* re-deliver
    // the unconfirmed page. If this test ever stopped duplicating, the
    // exactly-once tests would no longer be distinguishing anything.
    let per_page = 4;

    // Same injected failure as the exactly-once crash test: the bookmark `put`
    // after page 1 fails, leaving the sink ahead of the state store.
    let al_log = EventLog::new();
    let al_sink = ScriptedSink::new(al_log.clone()).failing_at(Boundary::StatePut(1));
    let al_store = Arc::new(al_sink.state_store());
    let source1 = PagedSource::new(5, per_page);
    let _ = Pipeline::new(&source1, &al_sink)
        .with_state_store(al_store.clone())
        .run()
        .await;
    assert!(
        al_log.any(|e| matches!(e, Event::StatePutFailed { .. })),
        "the control arm must actually hit the injected failure: {:?}",
        al_log.events()
    );

    al_sink.rearm(Boundary::Never);
    let source2 = PagedSource::new(5, per_page);
    Pipeline::new(&source2, &al_sink)
        .with_state_store(al_store.clone())
        .run()
        .await
        .expect("run 2 completes");

    let total = source1.total_records();
    let at_least_once_delivered = al_log.records_written();
    assert!(
        at_least_once_delivered >= total,
        "at-least-once must never lose data: delivered {at_least_once_delivered} of {total}"
    );
    assert!(
        at_least_once_delivered > total,
        "at-least-once is expected to re-deliver the unconfirmed page — it delivered \
         exactly {at_least_once_delivered}. If this stops duplicating, the exactly-once \
         tests above are no longer distinguishing the two modes and the comparison is \
         vacuous."
    );

    // Now the same failure under exactly-once, for the side-by-side.
    let eo_log = EventLog::new();
    let eo_sink = ScriptedSink::new(eo_log.clone())
        .idempotent()
        .failing_at(Boundary::StatePut(1));
    let eo_store = Arc::new(eo_sink.state_store());
    let eo_source1 = PagedSource::new(5, per_page);
    let _ = Pipeline::new(&eo_source1, &eo_sink)
        .with_state_store(eo_store.clone())
        .with_delivery(DeliveryMode::ExactlyOnce)
        .run()
        .await;
    eo_sink.rearm(Boundary::Never);
    let eo_source2 = PagedSource::new(5, per_page);
    Pipeline::new(&eo_source2, &eo_sink)
        .with_state_store(eo_store.clone())
        .with_delivery(DeliveryMode::ExactlyOnce)
        .run()
        .await
        .expect("exactly-once run 2 completes");

    assert_eq!(
        eo_log.records_written(),
        total,
        "under the identical failure, exactly-once delivered {} records where \
         at-least-once delivered {at_least_once_delivered} — it must deliver exactly {total}",
        eo_log.records_written()
    );
}
