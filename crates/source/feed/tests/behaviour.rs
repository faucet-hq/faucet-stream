//! The feed source through a real `Pipeline` against a SQLite feed store.

mod support;

use faucet_common_feed::{FeedExecution, Move, NewEvent, OpenError, PayloadOpener, Seq, scope};
use faucet_conformance::scripted::{Boundary, Event, EventLog, ScriptedSink, ScriptedStateStore};
use faucet_core::idempotency::DeliveryMode;
use faucet_core::{CancellationToken, FaucetError, Pipeline, Source, json};
use faucet_source_feed::{FeedSource, FeedSourceConfig, FeedSourceError, FeedStoreSpec};
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use support::{Fixture, RecordingSink, STATE_KEY, ctx};

fn source_error(err: &FaucetError) -> &FeedSourceError {
    FeedSourceError::of(err).unwrap_or_else(|| panic!("not a feed source error: {err}"))
}

fn bound() -> FeedSource {
    FeedSource::new(FeedSourceConfig::new())
}

#[tokio::test]
async fn one_run_is_one_page_and_the_cursor_moves_only_after_the_sink_wrote() {
    let fx = Fixture::with_events(5).await;
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (store, feed, delivery, spy) = (
        Arc::clone(&fx.store),
        fx.feed.clone(),
        fx.delivery.clone(),
        Arc::clone(&seen),
    );
    let sink = RecordingSink::new().on_write(move |_| {
        let (store, feed, delivery, spy) = (
            Arc::clone(&store),
            feed.clone(),
            delivery.clone(),
            Arc::clone(&spy),
        );
        async move {
            let heads = store.heads(&ctx(), &[feed]).await.unwrap();
            let d = heads[0]
                .deliveries
                .iter()
                .find(|d| d.delivery == delivery)
                .unwrap();
            spy.lock()
                .unwrap()
                .push((d.cursor.get(), d.claim.is_some()));
        }
    });
    let log = EventLog::new();
    let state = Arc::new(ScriptedStateStore::new(log.clone()));
    let source = fx.source();
    let result = Pipeline::new(&source, &sink)
        .with_state_store(state.clone())
        .run()
        .await
        .unwrap();

    assert_eq!(result.records_written, 5);
    assert_eq!(sink.writes(), vec![5], "one page");
    assert_eq!(sink.ns(), vec![1, 2, 3, 4, 5]);
    assert_eq!(
        *seen.lock().unwrap(),
        vec![(1, true)],
        "claimed, not yet moved"
    );
    assert_eq!(fx.position().await, (6, 0, None), "moved and released");
    assert_eq!(state.stored(STATE_KEY), Some(fx.bookmark(6, 0)));
    assert_eq!(result.bookmark, Some(fx.bookmark(6, 0)));
    let row = &sink.rows()[4];
    assert_eq!(row["_feed"]["seq"], json!(5));
    assert_eq!(row["_feed"]["id"], json!(fx.id(5)));
    assert_eq!(row["_feed"]["headers"], json!({}));
    assert!(row["_feed"]["received_at"].as_str().unwrap().ends_with('Z'));

    let again = Pipeline::new(&source, &sink)
        .with_state_store(state)
        .run()
        .await
        .unwrap();
    assert_eq!(again.records_written, 0, "nothing new");
    assert_eq!(fx.cursor().await, 6);
}

#[tokio::test]
async fn a_sink_failure_leaves_the_cursor_and_the_next_run_delivers() {
    let fx = Fixture::with_events(4).await;
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone()).failing_at(Boundary::Write(0));
    let source = fx.source();
    let err = Pipeline::new(&source, &sink).run().await.unwrap_err();
    assert!(matches!(err, FaucetError::Sink(_)), "{err}");
    assert_eq!(fx.cursor().await, 1);
    fx.released().await;

    sink.rearm(Boundary::Never);
    let result = Pipeline::new(&source, &sink).run().await.unwrap();
    assert_eq!(result.records_written, 4);
    assert_eq!(fx.cursor().await, 5);
    assert!(log.contains(&Event::WriteFailed(4)) && log.contains(&Event::Write(4)));
}

#[tokio::test]
async fn a_flush_failure_leaves_the_cursor() {
    let fx = Fixture::with_events(2).await;
    let sink = ScriptedSink::new(EventLog::new()).failing_at(Boundary::Flush);
    let source = fx.source();
    Pipeline::new(&source, &sink).run().await.unwrap_err();
    assert_eq!(fx.cursor().await, 1);
}

#[tokio::test]
async fn exactly_once_a_crash_after_the_sink_commit_never_rewrites_the_page() {
    let fx = Fixture::with_events(5).await;
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone())
        .idempotent()
        .failing_at(Boundary::StatePut(0));
    let state = Arc::new(sink.state_store());
    let source = fx.source();
    let run = || async {
        Pipeline::new(&source, &sink)
            .with_state_store(state.clone())
            .with_delivery(DeliveryMode::ExactlyOnce)
            .run()
            .await
    };
    run().await.unwrap_err();
    assert_eq!(
        fx.cursor().await,
        1,
        "the cursor commit was lost with the crash"
    );
    assert_eq!(log.records_written(), 5, "the sink holds the page");
    fx.released().await;

    sink.rearm(Boundary::Never);
    let result = run().await.unwrap();
    assert_eq!(result.records_written, 0, "nothing is written twice");
    assert_eq!(log.records_written(), 5);
    assert_eq!(
        fx.position().await,
        (6, 0, None),
        "the cursor caught up from the sink's token"
    );

    let mut fx = fx;
    fx.append(vec![NewEvent::new("application/json", r#"{"n": 6}"#)])
        .await;
    let result = run().await.unwrap();
    assert_eq!(result.records_written, 1);
    assert_eq!(log.records_written(), 6);
    assert_eq!(fx.cursor().await, 7);
    assert_eq!(
        state.stored(STATE_KEY).unwrap()["bookmark"],
        fx.bookmark(7, 0)
    );
}

#[tokio::test]
async fn at_least_once_the_same_crash_repeats_the_page() {
    let fx = Fixture::with_events(3).await;
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone()).failing_at(Boundary::StatePut(0));
    let state = Arc::new(sink.state_store());
    let source = fx.source();
    let run = || async {
        Pipeline::new(&source, &sink)
            .with_state_store(state.clone())
            .run()
            .await
    };
    run().await.unwrap_err();
    assert_eq!(fx.cursor().await, 1);
    fx.released().await;
    sink.rearm(Boundary::Never);
    run().await.unwrap();
    assert_eq!(log.records_written(), 6, "the page was written again");
    assert_eq!(fx.cursor().await, 4);
}

async fn run_with_bookmark(
    fx: &Fixture,
    source: &FeedSource,
    bookmark: faucet_core::Value,
) -> (Result<usize, FaucetError>, RecordingSink) {
    let sink = RecordingSink::new();
    let state = Arc::new(ScriptedStateStore::new(EventLog::new()).seed(STATE_KEY, bookmark));
    let result = Pipeline::new(source, &sink)
        .with_state_store(state)
        .run()
        .await
        .map(|r| r.records_written);
    if result.is_err() {
        fx.released().await;
    }
    (result, sink)
}

#[tokio::test]
async fn a_bookmark_of_another_feed_or_delivery_is_refused() {
    let fx = Fixture::with_events(3).await;
    for other in [
        json!({"feed": "billing", "delivery": "warehouse", "epoch": 0, "next": 2, "id": "x"}),
        json!({"feed": "orders", "delivery": "lake", "epoch": 0, "next": 2, "id": "x"}),
        json!({"cursor": "2026-01-01"}),
    ] {
        let (result, sink) = run_with_bookmark(&fx, &fx.source(), other).await;
        let err = result.unwrap_err();
        assert!(
            matches!(source_error(&err), FeedSourceError::SharedState { expected, .. } if expected == "orders/warehouse"),
            "{err}"
        );
        assert!(err.to_string().contains("its own `name`"), "{err}");
        assert!(sink.rows().is_empty());
        assert_eq!(fx.position().await, (1, 0, None));
    }
}

#[tokio::test]
async fn a_bookmark_of_another_epoch_is_ignored() {
    let fx = Fixture::with_events(3).await;
    fx.store
        .move_cursor(
            &ctx(),
            &fx.delivery,
            Seq(1),
            Seq(2),
            Move::Skip {
                reason: "test".into(),
                by: "test".into(),
            },
        )
        .await
        .unwrap();
    let (result, sink) = run_with_bookmark(&fx, &fx.source(), fx.bookmark(4, 0)).await;
    assert_eq!(result.unwrap(), 2);
    assert_eq!(
        sink.ns(),
        vec![2, 3],
        "read from the cursor, not from the old bookmark"
    );
    assert_eq!(fx.position().await, (4, 1, None));
}

#[tokio::test]
async fn a_bookmark_at_or_behind_the_cursor_reads_from_the_cursor() {
    for next in [3, 2] {
        let fx = Fixture::with_events(4).await;
        let first = fx.bind(3).await;
        scope(first.clone(), async {
            Pipeline::new(&bound(), &RecordingSink::new())
                .run()
                .await
                .unwrap();
        })
        .await;
        fx.store
            .release(&ctx(), &first.binding.claim)
            .await
            .unwrap();
        assert_eq!(fx.cursor().await, 3);
        let (result, sink) = run_with_bookmark(&fx, &fx.source(), fx.bookmark(next, 0)).await;
        assert_eq!(result.unwrap(), 2);
        assert_eq!(sink.ns(), vec![3, 4]);
        assert_eq!(fx.cursor().await, 5);
    }
}

#[tokio::test]
async fn a_bookmark_ahead_of_the_cursor_moves_it_without_reading_the_page_again() {
    let fx = Fixture::with_events(5).await;
    let (result, sink) = run_with_bookmark(&fx, &fx.source(), fx.bookmark(4, 0)).await;
    assert_eq!(result.unwrap(), 2);
    assert_eq!(
        sink.ns(),
        vec![4, 5],
        "events 1 to 3 are in the sink already"
    );
    assert_eq!(fx.position().await, (6, 0, None));
}

#[tokio::test]
async fn a_bookmark_that_names_another_event_holds_the_delivery() {
    let fx = Fixture::with_events(5).await;
    let mut wrong = fx.bookmark(4, 0);
    wrong["id"] = json!("an-id-from-before-the-restore");
    let (result, sink) = run_with_bookmark(&fx, &fx.source(), wrong).await;
    let err = result.unwrap_err();
    assert!(
        matches!(source_error(&err), FeedSourceError::Held { seq: 3, id, .. } if id == "an-id-from-before-the-restore"),
        "{err}"
    );
    let text = err.to_string();
    assert!(
        text.contains("is held") && text.contains("restored"),
        "{text}"
    );
    assert!(sink.rows().is_empty(), "nothing is read");
    assert_eq!(fx.position().await, (1, 0, None), "nothing is skipped");
}

#[tokio::test]
async fn a_bookmark_past_the_watermark_holds_the_delivery() {
    let fx = Fixture::with_events(3).await;
    let past =
        json!({"feed": "orders", "delivery": "warehouse", "epoch": 0, "next": 9, "id": "gone"});
    let (result, sink) = run_with_bookmark(&fx, &fx.source(), past.clone()).await;
    let err = result.unwrap_err();
    assert!(
        matches!(source_error(&err), FeedSourceError::Held { seq: 8, .. }),
        "{err}"
    );
    assert!(sink.rows().is_empty());
    assert_eq!(fx.position().await, (1, 0, None));

    let drained = Fixture::with_events(2).await;
    Pipeline::new(&drained.source(), &RecordingSink::new())
        .run()
        .await
        .unwrap();
    assert_eq!(drained.cursor().await, 3);
    let (result, _) = run_with_bookmark(&drained, &drained.source(), past).await;
    let err = result.unwrap_err();
    assert!(
        matches!(source_error(&err), FeedSourceError::Held { seq: 8, reason, .. } if reason.contains("ends at event 2")),
        "{err}"
    );
    assert_eq!(drained.position().await, (3, 0, None));
}

#[tokio::test]
async fn a_bookmark_past_the_claim_ends_the_run_with_nothing_to_do() {
    let fx = Fixture::with_events(5).await;
    let execution = fx.bind(3).await;
    let sink = RecordingSink::new();
    let state =
        Arc::new(ScriptedStateStore::new(EventLog::new()).seed(STATE_KEY, fx.bookmark(5, 0)));
    let source = bound();
    let result = scope(
        execution.clone(),
        Pipeline::new(&source, &sink).with_state_store(state).run(),
    )
    .await
    .unwrap();
    assert_eq!(result.records_written, 0);
    assert!(sink.rows().is_empty());
    let (cursor, _, owner) = fx.position().await;
    assert_eq!(cursor, 5, "the cursor passed the claim's end");
    assert_eq!(
        owner.as_deref(),
        Some("serve:test"),
        "a bound source leaves the claim to its scheduler"
    );

    let limited = Fixture::with_events(5).await;
    let source = FeedSource::new(limited.config().max_events(2));
    let (result, sink) = run_with_bookmark(&limited, &source, limited.bookmark(5, 0)).await;
    assert_eq!(result.unwrap(), 0);
    assert!(sink.rows().is_empty());
    assert_eq!(limited.position().await, (5, 0, None));
}

#[tokio::test]
async fn a_bound_source_reads_its_claimed_range_and_keeps_the_claim() {
    let fx = Fixture::with_events(5).await;
    let execution = fx.bind(4).await.dataset("acme/orders");
    let sink = RecordingSink::new();
    let source = bound();
    let (result, uri, key) = scope(execution.clone(), async {
        let result = Pipeline::new(&source, &sink).run().await;
        (result, source.dataset_uri(), source.state_key())
    })
    .await;
    assert_eq!(result.unwrap().records_written, 3);
    assert_eq!(sink.ns(), vec![1, 2, 3]);
    assert_eq!(uri, "feed://acme/orders");
    assert_eq!(key.as_deref(), Some(STATE_KEY));
    let (cursor, _, owner) = fx.position().await;
    assert_eq!((cursor, owner.as_deref()), (4, Some("serve:test")));

    let again = scope(execution, Pipeline::new(&source, &sink).run())
        .await
        .unwrap();
    assert_eq!(
        again.records_written, 0,
        "the cursor is already at the claim's end"
    );
    assert_eq!(fx.cursor().await, 4);
}

#[tokio::test]
async fn a_bound_source_refuses_its_own_feed_delivery_and_store() {
    let fx = Fixture::with_events(2).await;
    let execution = fx.bind(3).await;
    for (cfg, named) in [
        (fx.config(), "`feed`, `delivery`, `store`"),
        (
            serde_json::from_value(json!({"delivery": "warehouse"})).unwrap(),
            "`delivery`",
        ),
    ] {
        let source = FeedSource::new(cfg);
        let err = scope(
            execution.clone(),
            Pipeline::new(&source, &RecordingSink::new()).run(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, FaucetError::Config(_)), "{err}");
        assert!(err.to_string().contains(named), "{err}");
    }
    let injected = FeedSource::with_store(FeedSourceConfig::new(), Arc::clone(&fx.store));
    let err = scope(
        execution,
        Pipeline::new(&injected, &RecordingSink::new()).run(),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("injected store"), "{err}");
    assert_eq!(fx.cursor().await, 1);
}

#[tokio::test]
async fn an_unbound_source_needs_feed_delivery_and_store() {
    let err = Pipeline::new(&bound(), &RecordingSink::new())
        .run()
        .await
        .unwrap_err();
    assert!(matches!(err, FaucetError::Config(_)), "{err}");
    assert!(
        err.to_string().contains("missing: feed, delivery, store"),
        "{err}"
    );

    let fx = Fixture::with_events(1).await;
    let no_file = FeedSource::new(FeedSourceConfig::standalone(
        "orders",
        "warehouse",
        FeedStoreSpec::sqlite(format!("{}.missing", fx.path)),
    ));
    let err = Pipeline::new(&no_file, &RecordingSink::new())
        .run()
        .await
        .unwrap_err();
    assert!(err.to_string().contains("there is no feed store"), "{err}");
    assert!(!std::path::Path::new(&format!("{}.missing", fx.path)).exists());

    for (feed, delivery, what) in [
        ("billing", "warehouse", "feed billing"),
        ("orders", "lake", "delivery lake of feed orders"),
    ] {
        let source = FeedSource::with_store(
            serde_json::from_value(json!({"feed": feed, "delivery": delivery})).unwrap(),
            Arc::clone(&fx.store),
        );
        let err = Pipeline::new(&source, &RecordingSink::new())
            .run()
            .await
            .unwrap_err();
        assert!(
            matches!(source_error(&err), FeedSourceError::Missing { what: w } if w == what),
            "{err}"
        );
        assert!(err.to_string().contains("faucet feed create"), "{err}");
    }
    let bad = FeedSource::with_store(
        serde_json::from_value(json!({"feed": "no spaces", "delivery": "d"})).unwrap(),
        Arc::clone(&fx.store),
    );
    let err = Pipeline::new(&bad, &RecordingSink::new())
        .run()
        .await
        .unwrap_err();
    assert!(matches!(err, FaucetError::Config(_)), "{err}");
}

#[tokio::test]
async fn a_lost_claim_aborts_the_run_before_the_cursor_moves() {
    let fx = Fixture::with_events(3).await;
    let execution = fx.bind(4).await;
    let (store, claim) = (Arc::clone(&fx.store), execution.binding.claim.clone());
    let sink = RecordingSink::new().on_write(move |_| {
        let (store, claim) = (Arc::clone(&store), claim.clone());
        async move { store.release(&ctx(), &claim).await.unwrap() }
    });
    let source = bound();
    let err = scope(execution, Pipeline::new(&source, &sink).run())
        .await
        .unwrap_err();
    assert!(
        matches!(source_error(&err), FeedSourceError::ClaimLost { .. }),
        "{err}"
    );
    assert_eq!(fx.position().await, (1, 0, None));
}

#[tokio::test]
async fn the_renewal_task_reports_a_claim_that_was_replaced() {
    let fx = Fixture::with_events(2).await;
    let execution = fx.bind(3).await;
    let (store, claim) = (Arc::clone(&fx.store), execution.binding.claim.clone());
    let sink = RecordingSink::new().on_write(move |_| {
        let (store, claim) = (Arc::clone(&store), claim.clone());
        async move {
            store.attach(&ctx(), &claim).await.unwrap();
            ctx().wait(Duration::from_millis(400)).await.unwrap();
        }
    });
    let source = FeedSource::new(FeedSourceConfig::new().claim_ttl_secs(1));
    let err = scope(execution, Pipeline::new(&source, &sink).run())
        .await
        .unwrap_err();
    assert!(
        matches!(source_error(&err), FeedSourceError::ClaimLost { .. }),
        "{err}"
    );
    assert_eq!(fx.cursor().await, 1);
}

#[tokio::test]
async fn the_claim_is_renewed_while_the_sink_writes() {
    let fx = Fixture::with_events(2).await;
    let sink = RecordingSink::new().on_write(|_| async {
        ctx().wait(Duration::from_millis(1600)).await.unwrap();
    });
    let source = FeedSource::new(fx.config().claim_ttl_secs(1));
    let result = Pipeline::new(&source, &sink).run().await.unwrap();
    assert_eq!(
        result.records_written, 2,
        "a one second claim outlived a slower write"
    );
    assert_eq!(fx.position().await, (3, 0, None));
}

#[tokio::test]
async fn an_earlier_attempt_is_fenced_once_a_newer_one_attaches() {
    let fx = Fixture::with_events(4).await;
    let execution = fx.bind(5).await;
    let empty = HashMap::new();
    let earlier = bound();
    let outcome = scope(execution.clone(), async {
        let mut pages = earlier.stream_pages(&empty, 0);
        let first = pages.next().await.unwrap().unwrap();
        assert_eq!(first.records.len(), 4);

        let newer = bound();
        let sink = RecordingSink::new();
        let result = Pipeline::new(&newer, &sink).run().await.unwrap();
        assert_eq!(
            result.records_written, 4,
            "the newer attempt owns the range"
        );

        pages.next().await.unwrap()
    })
    .await;
    let err = outcome.unwrap_err();
    assert!(
        matches!(source_error(&err), FeedSourceError::ClaimLost { .. }),
        "{err}"
    );
    assert_eq!(fx.cursor().await, 5);
}

#[tokio::test]
async fn a_cancel_commits_what_was_written_and_ends() {
    let fx = Fixture::with_events(6).await;
    let cancel = CancellationToken::new();
    let token = cancel.clone();
    let sink = RecordingSink::new().on_write(move |_| {
        let token = token.clone();
        async move { token.cancel() }
    });
    let source = FeedSource::new(fx.config().page_events(2));
    let result = Pipeline::new(&source, &sink)
        .with_cancel(cancel.clone())
        .run()
        .await
        .unwrap();
    assert_eq!(result.records_written, 2);
    assert_eq!(sink.ns(), vec![1, 2]);
    assert_eq!(
        fx.position().await,
        (3, 0, None),
        "the written page is committed"
    );

    let result = Pipeline::new(&source, &RecordingSink::new())
        .with_cancel(cancel)
        .run()
        .await
        .unwrap();
    assert_eq!(result.records_written, 0, "a cancelled run reads nothing");
    assert_eq!(fx.position().await, (3, 0, None));
}

#[tokio::test]
async fn a_second_reader_of_the_same_delivery_fails_on_the_claim() {
    let fx = Fixture::with_events(3).await;
    let empty = HashMap::new();
    let first = fx.source();
    let mut pages = first.stream_pages(&empty, 0);
    assert_eq!(pages.next().await.unwrap().unwrap().records.len(), 3);

    let second = fx.source();
    let err = Pipeline::new(&second, &RecordingSink::new())
        .run()
        .await
        .unwrap_err();
    assert!(
        matches!(source_error(&err), FeedSourceError::ClaimBusy { owner, .. } if owner.starts_with("cli:")),
        "{err}"
    );
    assert_eq!(fx.cursor().await, 1);

    assert!(pages.next().await.is_none());
    assert_eq!(fx.position().await, (4, 0, None));
}

#[tokio::test]
async fn a_dropped_read_releases_its_own_claim() {
    let fx = Fixture::with_events(2).await;
    let empty = HashMap::new();
    let source = fx.source();
    {
        let mut pages = source.stream_pages(&empty, 0);
        pages.next().await.unwrap().unwrap();
        assert!(fx.position().await.2.is_some());
    }
    fx.released().await;
    assert_eq!(
        fx.cursor().await,
        1,
        "a page nobody confirmed is not committed"
    );
}

fn reversing_opener() -> PayloadOpener {
    Arc::new(|sealed, context| {
        if context == "feed:orders:tenant:-:event" {
            Ok(sealed.iter().rev().copied().collect())
        } else {
            Err(OpenError(format!("unexpected context {context}")))
        }
    })
}

async fn sealed_fixture() -> Fixture {
    let mut fx = Fixture::empty().await;
    let sealed: Vec<u8> = br#"{"n": 1}"#.iter().rev().copied().collect();
    fx.append(vec![
        NewEvent::new("application/json", sealed).sealed(true),
        NewEvent::new("application/json", r#"{"n": 2}"#),
    ])
    .await;
    fx
}

#[tokio::test]
async fn a_sealed_event_is_opened_through_the_run_handle() {
    let fx = sealed_fixture().await;
    let execution: FeedExecution = fx.bind(3).await.opener(reversing_opener());
    let sink = RecordingSink::new();
    let result = scope(execution, Pipeline::new(&bound(), &sink).run())
        .await
        .unwrap();
    assert_eq!(result.records_written, 2);
    assert_eq!(sink.ns(), vec![1, 2]);
}

#[tokio::test]
async fn a_sealed_event_without_a_key_fails_the_run_and_skips_nothing() {
    let fx = sealed_fixture().await;
    let execution = fx.bind(3).await;
    let sink = RecordingSink::new();
    let err = scope(execution.clone(), Pipeline::new(&bound(), &sink).run())
        .await
        .unwrap_err();
    assert!(
        matches!(source_error(&err), FeedSourceError::Sealed { seq: 1, reason, .. } if reason.contains("no key")),
        "{err}"
    );
    assert!(sink.rows().is_empty());
    assert_eq!(fx.cursor().await, 1);

    let refusing: PayloadOpener = Arc::new(|_, _| Err(OpenError("the key is gone".into())));
    let err = scope(
        execution.clone().opener(refusing),
        Pipeline::new(&bound(), &sink).run(),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("the key is gone"), "{err}");
    assert_eq!(fx.cursor().await, 1);

    fx.store
        .release(&ctx(), &execution.binding.claim)
        .await
        .unwrap();
    let err = Pipeline::new(&fx.source(), &sink).run().await.unwrap_err();
    assert!(
        matches!(source_error(&err), FeedSourceError::Sealed { .. }),
        "{err}"
    );
    fx.released().await;
    assert_eq!(fx.cursor().await, 1);
}

#[tokio::test]
async fn every_content_type_becomes_a_record() {
    let mut fx = Fixture::empty().await;
    fx.append(vec![
        NewEvent::new("application/json", r#"{"id": 1, "amount": 12.50}"#)
            .header("x-kind", "order"),
        NewEvent::new("application/json", "[1, 2, 3]"),
        NewEvent::new("application/vnd.thing+json; charset=utf-8", "\"plain\""),
        NewEvent::new("text/plain", "hello"),
        NewEvent::new("application/x-www-form-urlencoded", "a=1&b=two+words"),
        NewEvent::new("application/octet-stream", vec![0u8, 1, 2, 255]),
    ])
    .await;
    let sink = RecordingSink::new();
    let source = FeedSource::new(fx.config().include_metadata(false));
    Pipeline::new(&source, &sink).run().await.unwrap();
    assert_eq!(
        sink.rows(),
        vec![
            json!({"id": 1, "amount": 12.5}),
            json!({"value": [1, 2, 3]}),
            json!({"value": "plain"}),
            json!({"value": "hello"}),
            json!({"a": "1", "b": "two words"}),
            json!({"value": "AAEC/w=="}),
        ]
    );

    let with_metadata = Fixture::empty().await;
    let mut with_metadata = with_metadata;
    with_metadata
        .append(vec![
            NewEvent::new("text/plain", "hi").header("x-kind", "note"),
        ])
        .await;
    let sink = RecordingSink::new();
    Pipeline::new(&with_metadata.source(), &sink)
        .run()
        .await
        .unwrap();
    let row = &sink.rows()[0];
    assert_eq!(row["value"], json!("hi"));
    assert_eq!(row["_feed"]["headers"], json!({"x-kind": "note"}));
    assert_eq!(row["_feed"]["seq"], json!(1));
}

#[tokio::test]
async fn an_event_that_is_not_what_its_type_says_fails_the_run() {
    let mut fx = Fixture::empty().await;
    fx.append(vec![
        NewEvent::new("application/json", r#"{"n": 1}"#),
        NewEvent::new("application/json", "{broken"),
    ])
    .await;
    let sink = RecordingSink::new();
    let err = Pipeline::new(&fx.source(), &sink).run().await.unwrap_err();
    assert!(
        matches!(
            source_error(&err),
            FeedSourceError::Undecodable { seq: 2, .. }
        ),
        "{err}"
    );
    assert!(sink.rows().is_empty());
    assert_eq!(
        fx.position().await,
        (1, 0, None),
        "released by the source, nothing committed"
    );
}

#[tokio::test]
async fn page_events_splits_a_run_and_commits_each_page() {
    let fx = Fixture::with_events(5).await;
    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone());
    let state = Arc::new(sink.state_store());
    let source = FeedSource::new(fx.config().page_events(2));
    let result = Pipeline::new(&source, &sink)
        .with_state_store(state)
        .run()
        .await
        .unwrap();
    assert_eq!(result.records_written, 5);
    assert_eq!(
        log.bookmarks(),
        vec![fx.bookmark(3, 0), fx.bookmark(5, 0), fx.bookmark(6, 0)]
    );
    assert_eq!(log.writes_before_each_bookmark(), vec![2, 4, 5]);
    assert_eq!(fx.position().await, (6, 0, None));

    let failing = Fixture::with_events(5).await;
    let sink = ScriptedSink::new(EventLog::new()).failing_at(Boundary::Write(1));
    let source = FeedSource::new(failing.config().page_events(2));
    Pipeline::new(&source, &sink).run().await.unwrap_err();
    failing.released().await;
    assert_eq!(
        failing.cursor().await,
        3,
        "the first page is committed, the failed one is not"
    );
}

#[tokio::test]
async fn max_events_bounds_one_drain() {
    let fx = Fixture::with_events(5).await;
    let source = FeedSource::new(fx.config().max_events(2));
    let sink = RecordingSink::new();
    for expected in [
        vec![1, 2],
        vec![1, 2, 3, 4],
        vec![1, 2, 3, 4, 5],
        vec![1, 2, 3, 4, 5],
    ] {
        Pipeline::new(&source, &sink).run().await.unwrap();
        assert_eq!(sink.ns(), expected);
    }
    assert_eq!(fx.cursor().await, 6);
}

#[tokio::test]
async fn the_buffered_fetch_is_refused() {
    let fx = Fixture::with_events(2).await;
    let source = fx.source();
    for err in [
        source.fetch_all().await.unwrap_err(),
        source.fetch_all_incremental().await.unwrap_err(),
        source
            .fetch_with_context(&HashMap::new())
            .await
            .unwrap_err(),
    ] {
        assert_eq!(source_error(&err), &FeedSourceError::BufferedFetch);
    }
    assert_eq!(fx.position().await, (1, 0, None));
}

#[tokio::test]
async fn the_source_declares_what_it_is() {
    let fx = Fixture::with_events(1).await;
    let source = fx.source();
    assert_eq!(source.connector_name(), "feed");
    assert_eq!(source.dataset_uri(), "feed://orders");
    assert_eq!(source.state_key().as_deref(), Some(STATE_KEY));
    assert!(source.supports_exactly_once());
    assert!(source.consumes_destructively());
    assert!(source.capabilities().read_side_effects());
    assert!(source.set_cancel(CancellationToken::new()));
    assert_eq!(source.config(), &fx.config());
    assert_eq!(bound().dataset_uri(), "feed://unbound");
    assert_eq!(bound().state_key(), None);
    let slashed = FeedSource::new(FeedSourceConfig::standalone(
        "acme/orders",
        "main",
        FeedStoreSpec::sqlite("x"),
    ));
    assert_eq!(
        slashed.state_key().as_deref(),
        Some("feed:acme.orders:main")
    );
}

/// The bookmark is state other releases read back: its shape is frozen. A
/// new shape needs a new fixture and a reader for this one.
#[tokio::test]
async fn the_bookmark_shape_is_frozen() {
    let fixture: faucet_core::Value =
        serde_json::from_str(include_str!("fixtures/bookmark-v1.json")).unwrap();
    let parsed: faucet_source_feed::FeedBookmark = serde_json::from_value(fixture.clone()).unwrap();
    assert_eq!(
        parsed.to_value(),
        fixture,
        "the fixture reads and writes back unchanged"
    );

    let fx = Fixture::with_events(5).await;
    let result = Pipeline::new(&fx.source(), &RecordingSink::new())
        .run()
        .await
        .unwrap();
    let mut expected = fixture;
    expected["id"] = json!(fx.id(5));
    assert_eq!(result.bookmark, Some(expected));
}
