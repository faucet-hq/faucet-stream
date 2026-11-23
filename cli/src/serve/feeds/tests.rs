//! Unit tests of the feeds runtime's pure parts; the end-to-end behaviour is
//! in `cli/tests/serve_feeds_core.rs`.

use super::runner::{self, Decision, Refusal};
use super::status::{DeliveryStatus, Health, LiveClaim, Outcome, Pause};
use super::*;
use crate::serve::error::ServeError;
use crate::serve::history::feeds::{DeliverySpec, RunFeedBinding, Span};
use crate::serve::history::{RunRecord, RunStatus};
use chrono::Utc;
use faucet_common_feed::{ClaimId, ClaimInfo, DeliveryHead, Epoch, Head, Seq};
use serde_json::json;

fn delivery(v: serde_json::Value) -> DeliverySpec {
    serde_json::from_value(v).unwrap()
}

fn uid() -> DeliveryUid {
    DeliveryUid::new(FeedUid::new("f1").unwrap(), "0123456789abcdef0123").unwrap()
}

fn head(watermark: u64, bytes: u64, cursor: u64, age_secs: i64) -> (Head, DeliveryHead) {
    let now = Utc::now();
    let dh = DeliveryHead {
        delivery: uid(),
        cursor: Seq(cursor),
        epoch: Epoch(1),
        claim: None,
        oldest_undelivered_at: (cursor <= watermark)
            .then(|| now - chrono::Duration::seconds(age_secs)),
    };
    let head = Head {
        feed: FeedUid::new("f1").unwrap(),
        tenant: None,
        watermark: Seq(watermark),
        bytes,
        oldest: Seq(1),
        now,
        deliveries: vec![dh.clone()],
    };
    (head, dh)
}

#[test]
fn a_delivery_is_claimed_only_when_its_flush_policy_is_met() {
    let d = delivery(json!({
        "name": "lake", "template": {"id": "t"},
        "flush": {"max_events": 10, "max_bytes": 1000, "max_age": "60s", "min_interval": "30s"}
    }));
    let now = Utc::now();
    let gap = std::time::Duration::from_secs(30);
    let idle = DeliveryStatus::default();
    let at = |h: &(Head, DeliveryHead), s: &DeliveryStatus| {
        runner::decide(&d, &h.0, &h.1, h.0.bytes, s, gap, now)
    };

    assert_eq!(at(&head(5, 10, 6, 0), &idle), Decision::Idle);
    assert_eq!(at(&head(5, 10, 3, 1), &idle), Decision::Idle);
    assert_eq!(
        at(&head(20, 10, 3, 1), &idle),
        Decision::Claim { bytes_only: false },
        "max_events"
    );
    assert_eq!(
        at(&head(5, 10, 3, 61), &idle),
        Decision::Claim { bytes_only: false },
        "max_age"
    );
    assert_eq!(
        at(&head(5, 5000, 3, 1), &idle),
        Decision::Claim { bytes_only: true },
        "the store has to confirm the byte limit"
    );
    // Delivered events kept for replay are not waiting bytes.
    let kept = head(5, 5000, 3, 1);
    assert_eq!(
        runner::decide(&d, &kept.0, &kept.1, 40, &idle, gap, now),
        Decision::Idle
    );

    // The shortest gap between two runs.
    let mut recent = DeliveryStatus {
        last_submit_at: Some(now - chrono::Duration::seconds(5)),
        ..Default::default()
    };
    assert_eq!(at(&head(20, 10, 3, 1), &recent), Decision::Idle);
    recent.last_submit_at = Some(now - chrono::Duration::seconds(31));
    assert!(matches!(
        at(&head(20, 10, 3, 1), &recent),
        Decision::Claim { .. }
    ));
    // A run asked for now waits for neither the policy nor the gap.
    recent.last_submit_at = Some(now);
    recent.deliver_now = true;
    assert_eq!(
        at(&head(5, 10, 3, 1), &recent),
        Decision::Claim { bytes_only: false }
    );

    // Paused, stalled, held or backing off: blocked, to be looked at again.
    let paused = DeliveryStatus {
        paused: Some(Pause {
            at: now,
            by: "ops".into(),
            reason: None,
        }),
        ..Default::default()
    };
    assert_eq!(at(&head(20, 10, 3, 1), &paused), Decision::Blocked);
    for health in [Health::Stalled, Health::Held] {
        let s = DeliveryStatus {
            health,
            ..Default::default()
        };
        assert_eq!(at(&head(20, 10, 3, 1), &s), Decision::Blocked);
    }
    let mut failing = DeliveryStatus {
        health: Health::Failing,
        ..Default::default()
    };
    assert!(matches!(
        at(&head(20, 10, 3, 1), &failing),
        Decision::Claim { .. }
    ));
    failing.backoff_until = Some(now + chrono::Duration::seconds(10));
    assert_eq!(at(&head(20, 10, 3, 1), &failing), Decision::Blocked);
    let in_spec = delivery(json!({"name": "lake", "template": {"id": "t"}, "paused": true}));
    let h = head(9000, 10, 3, 1);
    assert_eq!(
        runner::decide(&in_spec, &h.0, &h.1, h.0.bytes, &idle, gap, now),
        Decision::Blocked
    );

    // A claim in the store, or one the status still records, is looked at first.
    let mut claimed = head(20, 10, 3, 1);
    claimed.1.claim = Some(ClaimInfo {
        id: ClaimId::new("c1").unwrap(),
        to: Seq(9),
        owner: faucet_common_feed::ClaimOwner::new("serve:x").unwrap(),
        expires_at: now,
        attached: false,
    });
    assert_eq!(at(&claimed, &idle), Decision::CheckClaim);
    let mut recorded = DeliveryStatus::default();
    recorded.claimed(LiveClaim {
        id: "c1".into(),
        from: 3,
        to: 9,
        run_id: None,
        claimed_at: now,
        blocking: false,
    });
    assert_eq!(at(&head(20, 10, 3, 1), &recorded), Decision::CheckClaim);
}

#[test]
fn the_gap_and_the_batch_respect_the_tenant() {
    let d = delivery(json!({
        "name": "lake", "template": {"id": "t"},
        "flush": {"min_interval": "3s"}, "max_batch": {"events": 1000, "bytes": 4000}
    }));
    let floor = std::time::Duration::from_secs(10);
    assert_eq!(runner::min_interval(&d, false, floor), Span(3).duration());
    assert_eq!(runner::min_interval(&d, true, floor), floor);
    let slow =
        delivery(json!({"name": "l", "template": {"id": "t"}, "flush": {"min_interval": "1m"}}));
    assert_eq!(
        runner::min_interval(&slow, true, floor),
        Span(60).duration()
    );

    assert_eq!(runner::effective_batch(&d, None), (1000, 4000));
    let limits = |records, bytes| TenantLimits {
        max_records_per_run: records,
        max_bytes_per_run: bytes,
        ..Default::default()
    };
    assert_eq!(
        runner::effective_batch(&d, Some(&limits(Some(100), Some(2000)))),
        (100, 1500),
        "three quarters of the tenant's byte ceiling"
    );
    assert_eq!(
        runner::effective_batch(&d, Some(&limits(Some(5000), Some(1_000_000)))),
        (1000, 4000)
    );
    assert_eq!(
        runner::effective_batch(&d, Some(&limits(None, Some(1)))),
        (1000, 1)
    );
}

#[test]
fn a_refusal_that_passes_with_time_is_not_counted() {
    for e in [
        ServeError::Draining,
        ServeError::TooManyRequests("tenant at its limit".into()),
        ServeError::Unavailable("history unavailable".into()),
    ] {
        assert!(matches!(runner::classify(e), Refusal::Transient(_)));
    }
    for e in [
        ServeError::NotFound,
        ServeError::Forbidden("params screen".into()),
        ServeError::Conflict("the connection needs re-authorization".into()),
        ServeError::Unprocessable {
            message: "policy refusal".into(),
            details: None,
        },
        ServeError::BadConfig("bad".into()),
    ] {
        assert!(matches!(runner::classify(e), Refusal::Permanent(_)));
    }
    assert_eq!(
        runner::classify(ServeError::NotFound),
        Refusal::Permanent("the template was not found".into())
    );
}

fn run(status: RunStatus, error: Option<&str>) -> RunRecord {
    let mut r = RunRecord::queued("r1".into(), None, Default::default(), None, Utc::now());
    r.status = status;
    r.error = error.map(str::to_string);
    r
}

#[test]
fn a_run_counts_by_where_it_left_the_cursor() {
    // The cursor moved: delivered, whatever the run said afterwards.
    assert_eq!(
        runner::outcome_of(&run(RunStatus::Failed, Some("profile drift")), true),
        Outcome::Delivered
    );
    assert_eq!(
        runner::outcome_of(&run(RunStatus::Cancelled, None), true),
        Outcome::Delivered
    );
    assert_eq!(
        runner::outcome_of(&run(RunStatus::Cancelled, None), false),
        Outcome::Cancelled
    );
    // Only the source's own typed failure holds a delivery or reads as a
    // lost claim: an error that merely says so is an ordinary failure.
    use crate::serve::history::{FeedFailure, InvocationRecord};
    let said = |text: &str, kind: Option<FeedFailure>| {
        let mut r = run(RunStatus::Failed, Some(text));
        let mut inv: InvocationRecord = serde_json::from_value(
            serde_json::json!({"row_id": "row", "records_written": 0, "error": text}),
        )
        .unwrap();
        inv.feed_error = kind;
        r.invocations.push(inv);
        r
    };
    let lock = "IO Error: Conflicting lock is held in another process";
    assert!(matches!(
        runner::outcome_of(&said(lock, None), false),
        Outcome::Failed(_)
    ));
    assert!(matches!(
        runner::outcome_of(&said("claim c1 is no longer held by this run", None), false),
        Outcome::Failed(_)
    ));
    assert_eq!(
        runner::outcome_of(&said("stopped", Some(FeedFailure::ClaimLost)), false),
        Outcome::ClaimLost
    );
    assert!(matches!(
        runner::outcome_of(
            &said("delivery f1/d is held: event 9", Some(FeedFailure::Held)),
            false
        ),
        Outcome::Held(_)
    ));
    assert_eq!(
        runner::outcome_of(&run(RunStatus::Failed, Some("sink down")), false),
        Outcome::Failed("sink down".into())
    );
    assert_eq!(
        runner::outcome_of(&run(RunStatus::Failed, None), false),
        Outcome::Failed("the run failed".into())
    );
    assert!(matches!(
        runner::outcome_of(&run(RunStatus::Completed, None), false),
        Outcome::Failed(m) if m.contains("without delivering")
    ));
}

#[test]
fn ids_keys_and_limits_follow_the_definition() {
    assert_eq!(short_id("0123456789abcdef0123"), "0123456789ab");
    assert_eq!(short_id("ABCDEF0123456789"), "abcdef012345");
    let hashed = short_id("to-lake");
    assert_eq!(hashed.len(), 12);
    assert!(hashed.chars().all(|c| c.is_ascii_hexdigit()));
    assert_eq!(hashed, short_id("to-lake"));
    assert_ne!(hashed, short_id("to-audit"));
    assert_eq!(file_ids_key("-/orders"), "feed_uid:-/orders");

    let binding = FeedBinding::new(
        uid(),
        Epoch(2),
        ClaimId::new("c9").unwrap(),
        Seq(4),
        Seq(9),
        "default",
    );
    assert_eq!(
        runner::idempotency_key(&binding),
        "feed:f1:0123456789abcdef0123:c9"
    );
    let stored = RunFeedBinding::of(&binding);
    assert_eq!((stored.from, stored.to, stored.epoch), (4, 9, 2));
    assert_eq!(stored.binding().unwrap(), binding);
    let mut bad = stored.clone();
    bad.feed = "not a uid".into();
    assert!(bad.binding().is_err());
    assert_eq!(
        crate::runtime::server::delivery_pipeline_name(Some("orders"), "0123456789ab"),
        "orders.0123456789ab"
    );
    assert_eq!(
        crate::runtime::server::delivery_pipeline_name(None, "0123456789ab"),
        "serve.0123456789ab"
    );

    let spec: FeedSpec = serde_json::from_value(json!({
        "name": "orders", "tenant": "acme",
        "ingest": {"auth": {"tokens": "managed"}, "dedupe": {"header": "X-Id", "window": "2h"}},
        "retention": {"max_age": "1d", "undelivered": {"max_age": "2d", "max_bytes": 4096}}
    }))
    .unwrap();
    let tenant = TenantLimits {
        max_feed_bytes: Some(1 << 20),
        ..Default::default()
    };
    let limits = store_limits(&spec, Some(&tenant));
    assert_eq!(limits.max_age, Duration::from_secs(86_400));
    assert_eq!(limits.dedupe_window, Duration::from_secs(7_200));
    assert_eq!(limits.tenant_max_bytes, Some(1 << 20));
    assert_eq!(limits.undelivered_max_bytes, Some(4096));
    assert_eq!(
        limits.undelivered_max_age,
        Some(Duration::from_secs(172_800))
    );
    assert_eq!(store_limits(&spec, None).tenant_max_bytes, None);

    use crate::serve::history::feeds::StartSpec;
    assert_eq!(start_of(StartSpec::Earliest), Start::Earliest);
    assert_eq!(start_of(StartSpec::Latest), Start::Latest);
    assert_eq!(start_of(StartSpec::Seq(7)), Start::Seq(Seq(7)));
    let t = Utc::now();
    assert_eq!(start_of(StartSpec::Time(t)), Start::Time(t));
}

fn feed(tenant: Option<&str>) -> Feed {
    let spec: FeedSpec = serde_json::from_value(json!({
        "name": "orders", "tenant": tenant,
        "ingest": {"auth": {"signature": {"header": "X-Sig", "secret": "s3cret"}}},
        "deliveries": [
            {"name": "lake", "template": {"id": "t", "params": {"k": {"$sealed": "ct"}}}},
            {"name": "audit", "template": {"id": "t"}, "paused": true}
        ]
    }))
    .unwrap();
    let uid = FeedUid::new("f1").unwrap();
    let deliveries = [
        ("lake", "0123456789abcdef0123"),
        ("audit", "fedcba9876543210"),
    ]
    .into_iter()
    .map(|(name, id)| {
        (
            name.to_string(),
            DeliveryRef {
                uid: DeliveryUid::new(uid.clone(), id).unwrap(),
                meta: Default::default(),
            },
        )
    })
    .collect();
    Feed {
        uid,
        spec,
        source: Source::Console,
        deliveries,
        approved: false,
        revision: 3,
        ensured: true,
        derived: Default::default(),
    }
}

#[test]
fn a_snapshot_finds_feeds_by_tenant_and_name_and_tokens_by_hash() {
    let plain = Arc::new(feed(None));
    let mut other = feed(Some("acme"));
    other.uid = FeedUid::new("f2").unwrap();
    let other = Arc::new(other);
    assert_eq!(
        (plain.key(), plain.label()),
        ("-/orders".into(), "orders".into())
    );
    assert_eq!(
        (other.key(), other.label()),
        ("acme/orders".into(), "acme/orders".into())
    );
    assert_eq!(plain.delivery("lake").unwrap().short_id(), "0123456789ab");
    assert_eq!(plain.delivery_by_uid(&uid()).unwrap().0, "lake");
    assert!(plain.delivery("nope").is_none());

    let token = FeedTokenRecord {
        id: "t1".into(),
        feed: "f1".into(),
        name: "ci".into(),
        hash: service::token_hash("fdt_abc"),
        prefix: "fdt_abc".into(),
        created_at: Utc::now(),
        created_by: "alice".into(),
        expires_at: None,
        revoked_at: None,
        revoked_by: None,
        last_used_at: None,
    };
    let snapshot = Snapshot::new(
        vec![plain.clone(), other.clone()],
        vec![token],
        HashMap::new(),
    );
    assert_eq!(snapshot.feed(None, "orders").unwrap().uid, plain.uid);
    assert_eq!(
        snapshot.feed(Some("acme"), "orders").unwrap().uid,
        other.uid
    );
    assert!(snapshot.feed(Some("globex"), "orders").is_none());
    assert_eq!(snapshot.by_uid("f2").unwrap().tenant(), Some("acme"));
    assert!(snapshot.by_uid("f9").is_none());
    assert_eq!(
        snapshot.token(&service::token_hash("fdt_abc")).unwrap().id,
        "t1"
    );
    assert!(snapshot.token(&service::token_hash("fdt_other")).is_none());
    assert!(snapshot.fresh() && snapshot.age() < MAX_SNAPSHOT_AGE);
    assert_eq!(service::token_hash("a").len(), 64);
    assert!(Snapshot::default().feeds.is_empty());
}

#[test]
fn a_view_masks_secrets_and_shows_each_deliverys_position() {
    let f = feed(None);
    let (mut h, dh) = head(20, 512, 7, 30);
    h.deliveries = vec![dh];
    let mut statuses = BTreeMap::new();
    statuses.insert(
        "lake".to_string(),
        DeliveryStatus {
            batch_events: Some(250),
            consecutive_failures: 3,
            health: Health::Failing,
            last_error: Some("sink down".into()),
            ..Default::default()
        },
    );
    let v = service::view(&f, Some(&h), Some(100), &statuses);
    assert_eq!(
        (v.name.as_str(), v.revision, v.watermark, v.retained_bytes),
        ("orders", 3, Some(20), Some(512))
    );
    assert_eq!(
        v.spec.ingest.auth.signature.as_ref().unwrap().secret,
        "$sealed"
    );
    let lake = &v.deliveries[0];
    assert_eq!(
        (
            lake.status.as_str(),
            lake.position,
            lake.lag,
            lake.batch_events
        ),
        ("failing", Some(7), Some(14), 250)
    );
    assert_eq!(
        lake.spec.template.as_ref().unwrap().params["k"],
        json!({"$sealed": true})
    );
    assert_eq!(lake.last_error.as_deref(), Some("sink down"));
    let audit = &v.deliveries[1];
    assert_eq!(
        (audit.status.as_str(), audit.position, audit.batch_events),
        ("paused", None, 10_000)
    );
    let text = serde_json::to_string(&v).unwrap();
    assert!(
        !text.contains("s3cret") && !text.contains("\"ct\""),
        "{text}"
    );
    assert!(service::view(&f, None, None, &statuses).watermark.is_none());

    let d = &f.spec.deliveries[0];
    let mut s = DeliveryStatus::default();
    assert_eq!(service::status_word(d, &s), "ok");
    s.needs_approval = Some("why".into());
    assert_eq!(service::status_word(d, &s), "needs_approval");
    s.health = Health::Stalled;
    assert_eq!(service::status_word(d, &s), "stalled");
    s.paused = Some(Pause {
        at: Utc::now(),
        by: "x".into(),
        reason: None,
    });
    assert_eq!(service::status_word(d, &s), "paused");
}

#[test]
fn an_action_is_scoped_to_the_callers_tenant() {
    let ctx = |tenant: Option<&str>| crate::serve::rbac::AuthContext {
        principal: "p".into(),
        role: crate::serve::rbac::Role::Operator,
        source_ip: None,
        tenant: tenant.map(str::to_string),
    };
    assert_eq!(service::scope(&ctx(None), None).unwrap(), None);
    assert_eq!(
        service::scope(&ctx(None), Some("acme")).unwrap().as_deref(),
        Some("acme")
    );
    assert_eq!(
        service::scope(&ctx(Some("acme")), None).unwrap().as_deref(),
        Some("acme")
    );
    assert_eq!(
        service::scope(&ctx(Some("acme")), Some("acme"))
            .unwrap()
            .as_deref(),
        Some("acme")
    );
    assert!(matches!(
        service::scope(&ctx(Some("acme")), Some("globex")),
        Err(ServeError::NotFound)
    ));
}

#[test]
fn a_rollback_note_says_whether_a_replay_is_needed() {
    let b = RunFeedBinding {
        feed: "f1".into(),
        delivery: "d1".into(),
        epoch: 1,
        claim: "c1".into(),
        from: 11,
        to: 21,
        store: "default".into(),
    };
    let lost = service::rollback_note("orders", "lake", &b, Some(11));
    assert!(
        lost.contains("is paused")
            && lost.contains("resuming the delivery redelivers these events"),
        "{lost}"
    );
    assert!(lost.contains("11..20") && lost.contains("no replay is needed"));
    let moved = service::rollback_note("orders", "lake", &b, Some(21));
    assert!(
        moved.contains("replay the delivery from event 11"),
        "{moved}"
    );
    assert!(moved.contains("every later run's events again"));
    let later = service::rollback_note("orders", "lake", &b, Some(500));
    assert!(later.contains("replay the delivery from event 11"));
    let unknown = service::rollback_note("orders", "lake", &b, None);
    assert!(
        unknown.contains("could not be read") && unknown.contains("replay from event 11"),
        "{unknown}"
    );
}

fn degraded_state() -> ServerState {
    let config = crate::serve::test_support::test_config();
    let history = Arc::new(
        crate::serve::history::fallback::FallbackHistory::degraded_at_startup(
            Duration::from_secs(60),
            "test",
        ),
    );
    ServerState::new(
        &config,
        None,
        tokio_util::sync::CancellationToken::new(),
        history,
        crate::serve::logs::LogHub::new(),
        None,
        #[cfg(feature = "triggers")]
        crate::serve::triggers::health::TriggersHandle::empty(),
    )
}

#[tokio::test]
async fn while_the_history_is_unreachable_the_scheduler_holds_and_nothing_is_made() {
    let state = degraded_state();
    let rt = state.feeds();
    let mut scheduler = runner::Scheduler::default();
    let report = scheduler.tick(&state).await;
    assert!(report.held);
    assert_eq!((report.claims, report.checks), (0, 0));
    assert!(
        rt.stores().opened().is_empty(),
        "no store is opened while holding"
    );
    assert!(
        rt.refresh(&state)
            .await
            .unwrap_err()
            .contains("unreachable")
    );
    assert!(rt.refresh_now(&state).await.is_err());
    rt.lease_tick(&state).await;
    assert!(!rt.started(), "nothing starts without a feed");
    let actor = crate::serve::rbac::AuthContext::system("test");
    let spec: FeedSpec = serde_json::from_value(
        json!({"name": "orders", "ingest": {"auth": {"tokens": "managed"}}}),
    )
    .unwrap();
    assert!(matches!(
        service::create(&state, &actor, None, spec).await,
        Err(ServeError::Unavailable(_))
    ));
    assert!(matches!(
        service::list(&state, &actor, None).await,
        Err(ServeError::Unavailable(_))
    ));
}

#[tokio::test]
async fn a_memory_history_takes_no_feed() {
    let state = crate::serve::test_support::test_state();
    let rt = state.feeds();
    assert!(format!("{rt:?}").contains("started: false"));
    assert_eq!(rt.readiness(), Readiness::default());
    assert!(rt.file().feeds.is_empty() && rt.file_feed(None, "orders").is_none());
    assert!(rt.owner().as_str().starts_with("serve:"));
    assert_eq!(rt.tenant_min_interval(), Duration::from_secs(10));
    let actor = crate::serve::rbac::AuthContext::system("test");
    let spec: FeedSpec = serde_json::from_value(
        json!({"name": "orders", "ingest": {"auth": {"tokens": "managed"}}}),
    )
    .unwrap();
    let err = service::create(&state, &actor, None, spec)
        .await
        .unwrap_err();
    assert!(
        err.api_error().error.message.contains("--history memory"),
        "{err}"
    );
    assert!(
        service::list(&state, &actor, None)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        service::get(&state, &actor, None, "orders").await,
        Err(ServeError::NotFound)
    ));
    // A tick with no feeds touches no store.
    let report = runner::Scheduler::default().tick(&state).await;
    assert_eq!(
        report,
        runner::TickReport {
            store_ok: true,
            ..Default::default()
        }
    );
    assert!(rt.stores().opened().is_empty());
    rt.notify();
}

#[test]
fn a_store_failure_waiting_may_fix_is_told_from_a_refusal() {
    use faucet_common_feed::FeedError;
    let down = EnsureError::store(&FeedError::Unavailable("down".into()), None);
    assert!(down.is_transient());
    let timed_out = EnsureError::store(
        &FeedError::Call(faucet_core::FaucetError::Cancelled {
            operation: "ensure".into(),
        }),
        None,
    );
    assert!(timed_out.is_transient());
    let refused = EnsureError::store(
        &FeedError::Backend("permission denied".into()),
        Some("delivery 'lake'"),
    );
    assert!(!refused.is_transient());
    assert_eq!(
        String::from(refused),
        "delivery 'lake': feed store error: permission denied"
    );
}

#[test]
fn a_moved_store_is_not_transient_and_says_where_the_events_are() {
    let moved = EnsureError::Moved(StoreMoved {
        recorded_store: "a".into(),
        recorded_id: 1,
        now_store: "b".into(),
        now_id: 2,
    });
    assert!(!moved.is_transient());
    assert!(!moved.to_string().is_empty());
    let busy = EnsureError::Store {
        transient: true,
        message: "busy".into(),
    };
    assert!(busy.is_transient());
    assert_eq!(busy.to_string(), "busy");
}
