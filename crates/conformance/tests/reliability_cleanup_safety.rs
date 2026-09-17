//! #651 Category A4 — scoped cleanup safety.
//!
//! Scoped cleanup is the only feature that **deletes destination rows**, so its
//! failure mode is the worst one faucet has: deleting data the source simply
//! had not got round to returning yet. The safety argument is entirely about
//! *when* it fires — after every page is written and flushed, and only when the
//! run finished uncancelled — because at any earlier point the set of written
//! keys is incomplete and a delete would remove rows this run itself would have
//! written.
//!
//! The pure parts (key accumulation, the sticky overflow, the error text) are
//! unit-tested in `faucet_core::cleanup`. What could not be tested there is the
//! gate itself: these tests drive the real `Pipeline::run` and assert that
//! `cleanup_scope` is *not even called* in each unsafe situation.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use faucet_conformance::scripted::{Boundary, Event, EventLog, PagedSource, ScriptedSink};
use faucet_core::cleanup::CleanupPolicy;
use faucet_core::{CancellationToken, Pipeline};

/// A policy scoped to one tenant, keyed by the record's `n` field.
fn policy(max_keys: usize) -> Arc<CleanupPolicy> {
    let mut scope = BTreeMap::new();
    scope.insert("tenant".to_string(), serde_json::json!("acme"));
    Arc::new(
        CleanupPolicy::new(scope, vec!["n".to_string()], max_keys)
            .expect("a scoped, keyed policy is valid"),
    )
}

/// Whether the engine called `cleanup_scope` at all.
fn cleanup_fired(log: &EventLog) -> bool {
    log.any(|e| matches!(e, Event::Cleanup(_)))
}

#[tokio::test]
async fn a_complete_run_fires_cleanup_once_after_every_write() {
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone()).cleanup();
    let source = PagedSource::new(3, 4);

    Pipeline::new(&source, &sink)
        .with_cleanup(policy(1000))
        .run()
        .await
        .expect("a complete run succeeds");

    let events = log.events();
    let cleanup_at = events
        .iter()
        .position(|e| matches!(e, Event::Cleanup(_)))
        .expect("a complete run must fire cleanup, else stale rows are left behind");
    let last_write = events
        .iter()
        .rposition(|e| matches!(e, Event::Write(_)))
        .expect("the run wrote data");

    assert!(
        last_write < cleanup_at,
        "cleanup must run after every write — firing between pages would delete rows a \
         later page of this same run goes on to write: {events:?}"
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Event::Cleanup(_)))
            .count(),
        1,
        "cleanup fires once per run, not once per page: {events:?}"
    );

    // It saw every written key — that completeness is what licenses the delete.
    let tracked = events
        .iter()
        .find_map(|e| match e {
            Event::Cleanup(n) => Some(*n),
            _ => None,
        })
        .expect("cleanup fired");
    assert_eq!(
        tracked,
        source.total_records(),
        "cleanup must see every written key before deciding what is stale"
    );
}

#[tokio::test]
async fn a_failed_run_never_deletes() {
    // The catastrophic case. Pages 0 and 1 landed, page 2 failed. The keys from
    // pages 3+ were never written, so every destination row they correspond to
    // looks "stale" — deleting them would destroy live data on the strength of
    // a run that did not finish.
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone())
        .cleanup()
        .failing_at(Boundary::Write(2));
    let source = PagedSource::new(6, 3);

    Pipeline::new(&source, &sink)
        .with_cleanup(policy(1000))
        .run()
        .await
        .expect_err("the run fails");

    assert!(
        !cleanup_fired(&log),
        "a failed run must not delete anything — the written-key set is partial, so \
         every unwritten row would be mistaken for stale: {:?}",
        log.events()
    );
}

#[tokio::test]
async fn a_failed_flush_never_deletes() {
    // Every write was accepted but none is durable. Deleting on the strength of
    // keys that may not have landed is the same loss, one step removed.
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone())
        .cleanup()
        .failing_at(Boundary::Flush);
    let source = PagedSource::new(3, 3);

    Pipeline::new(&source, &sink)
        .with_cleanup(policy(1000))
        .run()
        .await
        .expect_err("a failed flush fails the run");

    assert!(
        !cleanup_fired(&log),
        "unflushed writes must not license a delete: {:?}",
        log.events()
    );
}

#[tokio::test]
async fn a_cancelled_run_never_deletes() {
    // A cancel produces a *successful* `Ok` result by design (the run stopped
    // cleanly and flushed), which is exactly why this case needs its own test:
    // the naive check "did the run return Ok?" would let the delete through.
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone())
        .cleanup()
        .with_write_delay(Duration::from_millis(25));
    let source = PagedSource::new(50, 2);
    let cancel = CancellationToken::new();

    let canceller = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(55)).await;
        canceller.cancel();
    });

    let result = Pipeline::new(&source, &sink)
        .with_cleanup(policy(1000))
        .with_cancel(cancel)
        .run()
        .await
        .expect("a cancelled run returns Ok — which is the trap this test guards");

    assert!(
        result.records_written < source.total_records(),
        "the run must actually have been cut short for this test to mean anything: \
         wrote {} of {}",
        result.records_written,
        source.total_records()
    );
    assert!(
        !cleanup_fired(&log),
        "a cancelled run returned Ok but read only part of the source — deleting here \
         would remove rows the rest of the run would have written: {:?}",
        log.events()
    );
}

#[tokio::test]
async fn exceeding_max_keys_fails_the_run_and_deletes_nothing() {
    // Above the ceiling the tracked set is knowingly incomplete. Both
    // alternatives to failing are wrong: deleting would remove rows this run
    // wrote, and silently skipping would leave the stale rows the feature
    // exists to remove while reporting success.
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone()).cleanup();
    let source = PagedSource::new(5, 4); // 20 keys, ceiling of 3

    let err = Pipeline::new(&source, &sink)
        .with_cleanup(policy(3))
        .run()
        .await
        .expect_err("an overflowed key set must fail the run rather than guess");

    let msg = err.to_string();
    assert!(
        msg.contains("cleanup"),
        "the error must name the feature that refused: {msg}"
    );
    assert!(
        !cleanup_fired(&log),
        "nothing may be deleted when the tracked set overflowed: {:?}",
        log.events()
    );
}

#[tokio::test]
async fn a_sink_without_cleanup_support_is_rejected_before_any_write() {
    // Fail at configuration time, not after moving data: a run that writes and
    // *then* discovers it cannot clean up has already left the destination in
    // the half-updated state the claim promised to resolve.
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone()); // no .cleanup()
    let source = PagedSource::new(3, 3);

    let err = Pipeline::new(&source, &sink)
        .with_cleanup(policy(1000))
        .run()
        .await
        .expect_err("an incapable sink must be refused");
    assert!(
        matches!(err, faucet_core::FaucetError::Config(_)),
        "this is a configuration error, not a runtime one: {err:?}"
    );
    assert!(
        !log.any(|e| matches!(e, Event::Write(_))),
        "the refusal must come before any data moves: {:?}",
        log.events()
    );
}

#[tokio::test]
async fn an_empty_scope_is_refused_at_policy_construction() {
    // An empty scope is an every-row predicate: the difference between
    // "delete this tenant's stale rows" and "truncate the table".
    let err = CleanupPolicy::new(BTreeMap::new(), vec!["n".into()], 100)
        .expect_err("an unscoped cleanup must be impossible to construct");
    assert!(
        matches!(err, faucet_core::FaucetError::Config(_)),
        "got {err:?}"
    );

    // And a scope with no key cannot tell a written row from a stale one.
    let mut scope = BTreeMap::new();
    scope.insert("tenant".to_string(), serde_json::json!("acme"));
    CleanupPolicy::new(scope, vec![], 100).expect_err("a keyless cleanup must be refused");
}

#[tokio::test]
async fn a_run_that_reads_nothing_still_deletes_the_whole_scope() {
    // The feature's raison d'être, and the case that separates it from "delete
    // nothing when in doubt": the source legitimately returned zero rows for
    // this scope, which is a *complete* answer meaning everything there is
    // stale. Cleanup must fire with an empty key set.
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone()).cleanup();
    let source = PagedSource::new(0, 5);

    Pipeline::new(&source, &sink)
        .with_cleanup(policy(1000))
        .run()
        .await
        .expect("an empty complete run succeeds");

    assert!(
        log.contains(&Event::Cleanup(0)),
        "a complete run that read nothing must still clean the scope — otherwise a \
         fully-deleted upstream scope never converges: {:?}",
        log.events()
    );
}
