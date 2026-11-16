//! `faucet-conformance` battery for the feed source.

mod support;

use faucet_common_feed::{
    ClaimId, DeliveryUid, Epoch, FeedBinding, FeedExecution, FeedUid, Seq, StalledStore, scope,
};
use faucet_conformance::{
    assert_bookmark_roundtrip, assert_config_schema_valid_value, assert_connector_name_nonempty,
    assert_descriptor_matches_source, assert_errors_not_panics, assert_preflight_check_wellformed,
};
use faucet_core::Source;
use faucet_core::check::{CheckContext, ProbeStatus};
use faucet_core::json;
use faucet_source_feed::{DESCRIPTOR, FeedSource, FeedSourceConfig};
use std::sync::Arc;
use support::Fixture;

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(FeedSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-feed");
    assert_eq!(
        FeedSource::new(FeedSourceConfig::new()).config_schema(),
        schema
    );
}

#[test]
fn conformance_connector_name_nonempty() {
    assert_connector_name_nonempty(&FeedSource::new(FeedSourceConfig::new()));
}

#[test]
fn conformance_descriptor_matches_built_source() {
    for config in [
        json!({}),
        json!({"page_events": 100, "include_metadata": false}),
        json!({
            "feed": "orders",
            "delivery": "warehouse",
            "store": {"type": "sqlite", "config": {"path": "./feeds.db"}}
        }),
        json!({
            "feed": "orders",
            "delivery": "warehouse",
            "store": {"type": "postgres", "config": {"url": "postgres://localhost/feeds"}},
            "claim_ttl_secs": 60
        }),
    ] {
        let cfg: FeedSourceConfig = serde_json::from_value(config.clone()).unwrap();
        assert_descriptor_matches_source(&DESCRIPTOR, "feed", &config, &FeedSource::new(cfg));
    }
    let kind = DESCRIPTOR.kind_capabilities();
    assert_eq!(
        kind,
        FeedSource::new(FeedSourceConfig::new()).capabilities()
    );
    assert!(kind.exactly_once() && kind.consumes_destructively() && kind.read_side_effects());
}

/// The assertion drains the stream with no sink, so the store sees every
/// page as written: the cursor moves, and the resumed read finds nothing.
#[tokio::test]
async fn conformance_bookmark_roundtrip() {
    let fx = Fixture::with_events(7).await;
    let source = FeedSource::new(fx.config().page_events(3));
    assert_bookmark_roundtrip(&source).await;
    assert_eq!(fx.position().await, (8, 0, None));
}

#[tokio::test]
async fn conformance_errors_not_panics() {
    let fx = Fixture::with_events(1).await;
    let source = FeedSource::with_store(
        serde_json::from_value(json!({"feed": "absent", "delivery": "warehouse"})).unwrap(),
        Arc::clone(&fx.store),
    );
    assert_errors_not_panics(&source).await;
}

fn stalled_execution() -> FeedExecution {
    let feed = FeedUid::new("orders").unwrap();
    let delivery = DeliveryUid::new(feed, "warehouse").unwrap();
    let binding = FeedBinding::new(
        delivery,
        Epoch(0),
        ClaimId::generate(),
        Seq(1),
        Seq(2),
        "default",
    );
    FeedExecution::new(binding, Arc::new(StalledStore::new()))
}

/// A store that never answers: a bound read ends at the deadline and on
/// cancel.
#[tokio::test]
async fn conformance_call_deadline() {
    scope(
        stalled_execution(),
        faucet_conformance::calls::assert_source_call_deadline(|| {
            FeedSource::new(FeedSourceConfig::new())
        }),
    )
    .await;
}

#[tokio::test]
async fn conformance_call_deadline_standalone() {
    faucet_conformance::calls::assert_source_call_deadline(|| {
        FeedSource::with_store(
            serde_json::from_value(json!({"feed": "orders", "delivery": "warehouse"})).unwrap(),
            Arc::new(StalledStore::new()),
        )
    })
    .await;
}

#[tokio::test]
async fn conformance_preflight_check_wellformed() {
    let fx = Fixture::with_events(2).await;
    let source = fx.source();
    let ctx = CheckContext::default();
    assert_preflight_check_wellformed(&source, &ctx).await;
    let report = source.check(&ctx).await.unwrap();
    assert!(
        report
            .probes
            .iter()
            .all(|p| matches!(p.status, ProbeStatus::Pass))
    );
    assert_eq!(
        fx.position().await,
        (1, 0, None),
        "the probe reads and claims nothing"
    );
}

#[tokio::test]
async fn the_preflight_check_reports_a_missing_feed_or_delivery_and_skips_an_unbound_source() {
    let fx = Fixture::with_events(1).await;
    let ctx = CheckContext::default();
    for (feed, delivery, text) in [
        ("absent", "warehouse", "feed absent"),
        ("orders", "lake", "delivery lake of feed orders"),
    ] {
        let source = FeedSource::with_store(
            serde_json::from_value(json!({"feed": feed, "delivery": delivery})).unwrap(),
            Arc::clone(&fx.store),
        );
        let report = source.check(&ctx).await.unwrap();
        assert!(
            matches!(&report.probes[0].status, ProbeStatus::Fail { reason } if reason.contains(text)),
            "{report:?}"
        );
    }
    let unbound = FeedSource::new(FeedSourceConfig::new());
    let report = unbound.check(&ctx).await.unwrap();
    assert!(
        matches!(report.probes[0].status, ProbeStatus::Skip { .. }),
        "{report:?}"
    );

    let execution = fx.bind(2).await;
    let report = scope(execution, unbound.check(&ctx)).await.unwrap();
    assert!(matches!(report.probes[0].status, ProbeStatus::Pass));
    assert_eq!(fx.cursor().await, 1);
}
