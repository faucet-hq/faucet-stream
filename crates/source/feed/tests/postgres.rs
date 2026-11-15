//! The feed source against a PostgreSQL feed store. Needs Docker.

mod support;

use faucet_common_feed::{
    DeliveryUid, FeedStore, FeedUid, NewEvent, PostgresFeedStore, PostgresOptions, Start,
    StoreLimits, scope,
};
use faucet_conformance::scripted::{Boundary, EventLog, ScriptedSink};
use faucet_core::Pipeline;
use faucet_core::idempotency::DeliveryMode;
use faucet_source_feed::{FeedSource, FeedSourceConfig, FeedSourceError, FeedStoreSpec};
use std::sync::Arc;
use support::{RecordingSink, ctx};
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;

#[tokio::test(flavor = "multi_thread")]
async fn the_source_drains_resumes_and_fences_on_postgres() {
    let container =
        faucet_conformance::containers::start(|| Postgres::default().with_tag("16-alpine")).await;
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("postgres port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let store: Arc<dyn FeedStore> = Arc::new(
        PostgresFeedStore::connect(&url, &PostgresOptions::new())
            .await
            .expect("connect"),
    );
    let feed = FeedUid::new("orders").unwrap();
    let delivery = DeliveryUid::new(feed.clone(), "warehouse").unwrap();
    store
        .ensure_feed(&ctx(), &feed, None, &StoreLimits::new())
        .await
        .unwrap();
    store
        .ensure_delivery(&ctx(), &delivery, Start::Earliest)
        .await
        .unwrap();
    let events = (1..=6)
        .map(|n| NewEvent::new("application/json", format!(r#"{{"n": {n}}}"#)))
        .collect();
    store.append(&ctx(), &feed, events).await.unwrap();
    let cursor = || async {
        let heads = store
            .heads(&ctx(), std::slice::from_ref(&feed))
            .await
            .unwrap();
        let d = &heads[0].deliveries[0];
        (d.cursor.get(), d.claim.is_some())
    };
    let config = || {
        FeedSourceConfig::standalone("orders", "warehouse", FeedStoreSpec::postgres(url.clone()))
    };

    let log = EventLog::new();
    let sink = ScriptedSink::new(log.clone())
        .idempotent()
        .failing_at(Boundary::StatePut(0));
    let state = Arc::new(sink.state_store());
    let source = FeedSource::new(config().max_events(4));
    let run = || async {
        Pipeline::new(&source, &sink)
            .with_state_store(state.clone())
            .with_delivery(DeliveryMode::ExactlyOnce)
            .run()
            .await
    };
    run().await.unwrap_err();
    assert_eq!(cursor().await.0, 1, "the crash lost the cursor commit");
    for _ in 0..200 {
        if !cursor().await.1 {
            break;
        }
        ctx()
            .wait(std::time::Duration::from_millis(10))
            .await
            .unwrap();
    }
    sink.rearm(Boundary::Never);
    assert_eq!(run().await.unwrap().records_written, 0);
    assert_eq!(
        cursor().await,
        (5, false),
        "caught up from the sink's token"
    );
    assert_eq!(run().await.unwrap().records_written, 2);
    assert_eq!(log.records_written(), 6, "every event exactly once");
    assert_eq!(cursor().await, (7, false));

    store
        .append(
            &ctx(),
            &feed,
            vec![NewEvent::new("application/json", r#"{"n": 7}"#)],
        )
        .await
        .unwrap();
    let claim = store
        .claim(
            &ctx(),
            &delivery,
            faucet_common_feed::Seq(7),
            faucet_common_feed::Seq(8),
            &faucet_common_feed::ClaimOwner::new("serve:test").unwrap(),
            std::time::Duration::from_secs(60),
        )
        .await
        .unwrap()
        .expect("the claim");
    let busy = Pipeline::new(&FeedSource::new(config()), &RecordingSink::new())
        .run()
        .await
        .unwrap_err();
    assert!(
        matches!(
            FeedSourceError::of(&busy),
            Some(FeedSourceError::ClaimBusy { owner, .. }) if owner == "serve:test"
        ),
        "{busy}"
    );
    let binding = faucet_common_feed::FeedBinding::new(
        delivery.clone(),
        faucet_common_feed::Epoch(0),
        claim.id.clone(),
        claim.from,
        claim.to,
        "default",
    );
    let execution = faucet_common_feed::FeedExecution::new(binding, Arc::clone(&store));
    let bound_sink = RecordingSink::new();
    let bound = FeedSource::new(FeedSourceConfig::new());
    let result = scope(execution, Pipeline::new(&bound, &bound_sink).run())
        .await
        .unwrap();
    assert_eq!(result.records_written, 1);
    assert_eq!(bound_sink.ns(), vec![7]);
    assert_eq!(cursor().await, (8, true), "a bound source keeps the claim");
}
