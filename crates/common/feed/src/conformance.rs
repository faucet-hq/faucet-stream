//! The store conformance suite: the semantics every [`FeedStore`] backend
//! must hold. Implement [`Backend`] for a store and run [`run_all`], or one
//! entry of [`CHECKS`] per test. A check panics when the store breaks a rule.
//!
//! Checks use fresh feed uids, so they can share one database. They need a
//! multi-thread Tokio runtime.

use crate::error::{FeedError, Result};
use crate::store::FeedStore;
use crate::types::{
    AppendAck, BatchLimits, ClaimOwner, DeliveryUid, Epoch, FeedUid, FullLimit, Head, Move,
    NewEvent, Seq, Start, StoreLimits, TenantId,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use faucet_core::io::CallContext;
use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// The store under test.
#[async_trait]
pub trait Backend: Send + Sync {
    /// Open the store. Every call opens the **same** underlying storage, so a
    /// second call observes what the first one stored.
    async fn open(&self) -> Result<Arc<dyn FeedStore>>;

    /// The format version the storage records.
    async fn schema_version(&self) -> Result<i64>;

    /// Overwrite the format version the storage records.
    async fn set_schema_version(&self, version: i64) -> Result<()>;
}

type CheckFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

/// One conformance check.
pub struct Check {
    /// The check's name.
    pub name: &'static str,
    run: for<'a> fn(&'a dyn Backend) -> CheckFuture<'a>,
}

impl Check {
    /// Run the check against `backend`; panics when the store breaks the rule.
    pub async fn run(&self, backend: &dyn Backend) {
        (self.run)(backend).await
    }
}

macro_rules! checks {
    ($($name:ident),* $(,)?) => {
        /// Every check, in the order [`run_all`] runs them.
        pub const CHECKS: &[Check] = &[$(
            Check {
                name: stringify!($name),
                run: {
                    fn run(b: &dyn Backend) -> CheckFuture<'_> {
                        Box::pin($name(b))
                    }
                    run
                },
            },
        )*];
    };
}

checks!(
    append_is_durable_ordered_and_gap_free,
    concurrent_appends_never_expose_a_gap,
    dedupe_within_the_window,
    dedupe_replaces_an_expired_row,
    dedupe_under_concurrent_retries,
    limits_refuse_and_store_nothing,
    one_claimer_wins,
    an_expired_claim_is_gone,
    attach_fences_the_earlier_holder,
    commit_is_fenced_and_bounded,
    cursor_moves_are_guarded_and_recorded,
    trim_respects_age_cursors_and_claims,
    bound_seq_at_and_heads,
    delivery_start_delete_and_purge,
    state_survives_a_restart,
    a_newer_schema_is_refused,
    one_maintenance_holder,
);

/// Run every check in [`CHECKS`] against `backend`.
pub async fn run_all(backend: &dyn Backend) {
    for check in CHECKS {
        check.run(backend).await;
    }
}

/// Run the check called `name`; panics when there is none.
pub async fn run(backend: &dyn Backend, name: &str) {
    match CHECKS.iter().find(|c| c.name == name) {
        Some(check) => check.run(backend).await,
        None => panic!("no conformance check named {name}"),
    }
}

const TTL: Duration = Duration::from_secs(60);
const SHORT: Duration = Duration::from_millis(300);
const PAST_SHORT: Duration = Duration::from_millis(700);

struct Fixture {
    store: Arc<dyn FeedStore>,
    calls: CallContext,
}

impl Fixture {
    async fn open(backend: &dyn Backend) -> Self {
        Self {
            store: backend.open().await.expect("open the store"),
            calls: CallContext::new(),
        }
    }

    async fn feed(&self, limits: StoreLimits) -> FeedUid {
        let feed = FeedUid::generate();
        self.store
            .ensure_feed(&self.calls, &feed, None, &limits)
            .await
            .expect("ensure_feed");
        feed
    }

    async fn delivery(&self, feed: &FeedUid, start: Start) -> DeliveryUid {
        let delivery = DeliveryUid::generate(feed.clone());
        self.store
            .ensure_delivery(&self.calls, &delivery, start)
            .await
            .expect("ensure_delivery");
        delivery
    }

    async fn append(&self, feed: &FeedUid, events: Vec<NewEvent>) -> AppendAck {
        self.store
            .append(&self.calls, feed, events)
            .await
            .expect("append")
    }

    async fn fill(&self, feed: &FeedUid, n: usize) -> AppendAck {
        self.append(feed, (0..n).map(|i| event(&format!("e{i}"))).collect())
            .await
    }

    async fn head(&self, feed: &FeedUid) -> Head {
        let mut heads = self
            .store
            .heads(&self.calls, std::slice::from_ref(feed))
            .await
            .expect("heads");
        assert_eq!(heads.len(), 1, "heads returns the one feed asked for");
        heads.remove(0)
    }

    async fn cursor(&self, delivery: &DeliveryUid) -> Seq {
        let head = self.head(delivery.feed()).await;
        head.deliveries
            .iter()
            .find(|d| &d.delivery == delivery)
            .expect("the delivery is listed")
            .cursor
    }

    async fn advance(&self, delivery: &DeliveryUid, to: Seq) {
        let from = self.cursor(delivery).await;
        let claim = self
            .store
            .claim(&self.calls, delivery, from, to, &owner("advance"), TTL)
            .await
            .expect("claim")
            .expect("the claim is granted");
        let holder = self
            .store
            .attach(&self.calls, &claim.id)
            .await
            .expect("attach");
        self.store
            .commit(&self.calls, &holder, from, to)
            .await
            .expect("commit");
        self.store
            .release(&self.calls, &claim.id)
            .await
            .expect("release");
    }

    async fn wait(&self, d: Duration) {
        self.calls.wait(d).await.expect("wait");
    }
}

fn event(text: &str) -> NewEvent {
    NewEvent::new(
        "application/json",
        format!("{{\"v\":\"{text}\"}}").into_bytes(),
    )
}

fn sized(bytes: usize) -> NewEvent {
    NewEvent::new("application/octet-stream", vec![b'x'; bytes])
}

fn owner(name: &str) -> ClaimOwner {
    ClaimOwner::new(format!("test:{name}")).expect("a valid owner")
}

fn millis(ms: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(ms).expect("a valid timestamp")
}

fn full_limit(result: Result<AppendAck>) -> FullLimit {
    match result {
        Err(FeedError::Full { limit, .. }) => limit,
        other => panic!("expected a full refusal, got {other:?}"),
    }
}

/// An append returns after its events are stored, with consecutive sequence
/// numbers in request order, and a read returns exactly what was sent.
pub async fn append_is_durable_ordered_and_gap_free(backend: &dyn Backend) {
    let f = Fixture::open(backend).await;
    let feed = f.feed(StoreLimits::new()).await;

    let empty = f.head(&feed).await;
    assert_eq!(empty.watermark, Seq(0));
    assert_eq!(empty.end(), Seq(1));
    assert_eq!(empty.oldest, Seq(1));
    assert_eq!(empty.bytes, 0);

    let received = millis(1_700_000_000_123);
    let sent = vec![
        NewEvent::new("application/json", br#"{"a":1}"#.to_vec())
            .received_at(received)
            .header("x-event-id", "evt-1")
            .header("content-type", "application/json")
            .dedupe_key("evt-1"),
        NewEvent::new("text/plain", "plain \u{00e9}".as_bytes().to_vec()).received_at(received),
        NewEvent::new("application/octet-stream", vec![0u8, 159, 255])
            .received_at(received)
            .sealed(true),
    ];
    let ack = f.append(&feed, sent.clone()).await;
    assert_eq!(ack.events.len(), 3);
    assert_eq!(ack.first_seq, Some(Seq(1)));
    assert_eq!(ack.last_seq, Some(Seq(3)));
    assert_eq!(ack.duplicates, 0);
    assert_eq!(ack.stored(), 3);
    let ids: BTreeSet<&str> = ack.ids().into_iter().collect();
    assert_eq!(ids.len(), 3, "every event has its own id");
    for (i, appended) in ack.events.iter().enumerate() {
        assert_eq!(appended.seq, Seq(i as u64 + 1));
        assert!(!appended.duplicate);
    }

    let second = f.fill(&feed, 2).await;
    assert_eq!(second.first_seq, Some(Seq(4)));
    assert_eq!(second.last_seq, Some(Seq(5)));

    let head = f.head(&feed).await;
    assert_eq!(head.watermark, Seq(5));
    assert_eq!(head.oldest, Seq(1));
    let total: u64 = sent.iter().map(NewEvent::size).sum::<u64>() + 2 * event("e0").size();
    assert_eq!(head.bytes, total);

    let batch = f
        .store
        .read(&f.calls, &feed, Seq(1), Seq(100))
        .await
        .expect("read");
    assert_eq!(batch.from, Seq(1));
    assert_eq!(batch.to, Seq(6), "a read stops at the watermark");
    assert_eq!(batch.events.len(), 5);
    assert_eq!(batch.bytes(), total);
    for (i, (stored, new)) in batch.events.iter().zip(&sent).enumerate() {
        assert_eq!(stored.seq, Seq(i as u64 + 1));
        assert_eq!(stored.id, ack.events[i].id);
        assert_eq!(stored.received_at, new.received_at);
        assert_eq!(stored.content_type, new.content_type);
        assert_eq!(stored.headers, new.headers);
        assert_eq!(stored.payload, new.payload);
        assert_eq!(stored.size, new.size());
        assert_eq!(stored.sealed, new.sealed);
        assert_eq!(stored.dedupe_key, new.dedupe_key);
    }

    let part = f
        .store
        .read(&f.calls, &feed, Seq(2), Seq(4))
        .await
        .expect("read");
    assert_eq!((part.from, part.to, part.events.len()), (Seq(2), Seq(4), 2));
    let none = f
        .store
        .read(&f.calls, &feed, Seq(6), Seq(9))
        .await
        .expect("read at the end");
    assert!(none.events.is_empty());
    assert_eq!(none.to, Seq(6));
    assert!(matches!(
        f.store.read(&f.calls, &feed, Seq(7), Seq(9)).await,
        Err(FeedError::PastWatermark { .. })
    ));

    let nothing = f.append(&feed, Vec::new()).await;
    assert!(nothing.events.is_empty());
    assert_eq!(nothing.first_seq, None);
    assert_eq!(f.head(&feed).await.watermark, Seq(5));

    let missing = FeedUid::generate();
    assert!(matches!(
        f.store.append(&f.calls, &missing, vec![event("x")]).await,
        Err(FeedError::FeedNotFound(_))
    ));
    assert!(matches!(
        f.store.read(&f.calls, &missing, Seq(1), Seq(2)).await,
        Err(FeedError::FeedNotFound(_))
    ));
}

/// With several appenders committing at once, a reader that follows the
/// watermark never sees sequence N+1 before N (edge case E1).
pub async fn concurrent_appends_never_expose_a_gap(backend: &dyn Backend) {
    const WRITERS: usize = 8;
    const ROUNDS: usize = 20;
    let f = Fixture::open(backend).await;
    let feed = f.feed(StoreLimits::new()).await;
    let done = Arc::new(AtomicBool::new(false));

    let reader = {
        let store = f.store.clone();
        let feed = feed.clone();
        let done = done.clone();
        tokio::spawn(async move {
            let calls = CallContext::new();
            let mut cursor = Seq(1);
            let mut seen: Vec<Seq> = Vec::new();
            loop {
                let finished = done.load(Ordering::SeqCst);
                let heads = store
                    .heads(&calls, std::slice::from_ref(&feed))
                    .await
                    .expect("heads");
                let end = heads[0].end();
                if end > cursor {
                    let batch = store
                        .read(&calls, &feed, cursor, end)
                        .await
                        .expect("every event below the watermark is readable");
                    assert_eq!(
                        batch.to, end,
                        "the watermark never runs ahead of the events"
                    );
                    for (i, e) in batch.events.iter().enumerate() {
                        assert_eq!(e.seq, cursor.plus(i as u64), "sequence numbers have no gap");
                    }
                    seen.extend(batch.events.iter().map(|e| e.seq));
                    cursor = end;
                } else if finished {
                    return seen;
                }
                tokio::task::yield_now().await;
            }
        })
    };

    let mut writers = Vec::new();
    for w in 0..WRITERS {
        let store = f.store.clone();
        let feed = feed.clone();
        writers.push(tokio::spawn(async move {
            let calls = CallContext::new();
            let mut stored = 0u64;
            for r in 0..ROUNDS {
                let n = 1 + (w + r) % 3;
                let events = (0..n).map(|i| event(&format!("w{w}r{r}i{i}"))).collect();
                let ack = store.append(&calls, &feed, events).await.expect("append");
                let (first, last) = (ack.first_seq.expect("first"), ack.last_seq.expect("last"));
                assert_eq!(
                    first.distance_to(last) + 1,
                    n as u64,
                    "one append is contiguous"
                );
                stored += n as u64;
            }
            stored
        }));
    }
    let mut total = 0u64;
    for writer in writers {
        total += writer.await.expect("a writer finished");
    }
    done.store(true, Ordering::SeqCst);
    let seen = reader.await.expect("the reader finished");

    assert_eq!(seen.len() as u64, total, "the reader saw every event once");
    for (i, seq) in seen.iter().enumerate() {
        assert_eq!(*seq, Seq(i as u64 + 1));
    }
    assert_eq!(f.head(&feed).await.watermark, Seq(total));
}

/// A retry with the same key inside the window is not stored again, within a
/// request or across requests.
pub async fn dedupe_within_the_window(backend: &dyn Backend) {
    let f = Fixture::open(backend).await;
    let feed = f.feed(StoreLimits::new()).await;

    let first = f
        .append(
            &feed,
            vec![
                event("a").dedupe_key("k1"),
                event("b"),
                event("a again").dedupe_key("k1"),
                event("c").dedupe_key("k2"),
            ],
        )
        .await;
    assert_eq!(first.duplicates, 1);
    assert_eq!(first.stored(), 3);
    assert_eq!(
        (first.first_seq, first.last_seq),
        (Some(Seq(1)), Some(Seq(3)))
    );
    assert!(first.events[2].duplicate);
    assert_eq!(first.events[2].seq, first.events[0].seq);
    assert_eq!(first.events[2].id, first.events[0].id);
    assert_eq!(first.events[3].seq, Seq(3));

    let retry = f
        .append(
            &feed,
            vec![
                event("a retry").dedupe_key("k1"),
                event("d").dedupe_key("k3"),
            ],
        )
        .await;
    assert_eq!(retry.duplicates, 1);
    assert!(retry.events[0].duplicate);
    assert_eq!(retry.events[0].id, first.events[0].id);
    assert_eq!(retry.events[0].seq, Seq(1));
    assert_eq!(retry.events[1].seq, Seq(4));
    assert_eq!(
        (retry.first_seq, retry.last_seq),
        (Some(Seq(4)), Some(Seq(4)))
    );

    let all_known = f.append(&feed, vec![event("x").dedupe_key("k2")]).await;
    assert_eq!(all_known.duplicates, 1);
    assert_eq!(all_known.first_seq, None);
    assert_eq!(f.head(&feed).await.watermark, Seq(4));

    let other = f.feed(StoreLimits::new()).await;
    let ack = f.append(&other, vec![event("a").dedupe_key("k1")]).await;
    assert_eq!(ack.duplicates, 0, "keys are per feed");

    let off = f
        .feed(StoreLimits::new().dedupe_window(Duration::ZERO))
        .await;
    f.append(&off, vec![event("a").dedupe_key("k1")]).await;
    let again = f.append(&off, vec![event("a").dedupe_key("k1")]).await;
    assert_eq!(again.duplicates, 0, "a zero window turns dedupe off");
}

/// A dedupe row past its expiry, by the store's clock, is replaced: it never
/// swallows a new event.
pub async fn dedupe_replaces_an_expired_row(backend: &dyn Backend) {
    let f = Fixture::open(backend).await;
    let feed = f.feed(StoreLimits::new().dedupe_window(SHORT)).await;

    f.append(
        &feed,
        vec![event("a").dedupe_key("a"), event("b").dedupe_key("b")],
    )
    .await;
    let inside = f.append(&feed, vec![event("a").dedupe_key("a")]).await;
    assert_eq!(inside.duplicates, 1);
    assert_eq!(
        f.store
            .purge_dedupe(&f.calls, &feed, 100)
            .await
            .expect("purge"),
        0,
        "live rows are kept"
    );

    f.wait(PAST_SHORT).await;
    let after = f.append(&feed, vec![event("a new").dedupe_key("a")]).await;
    assert_eq!(after.duplicates, 0, "an expired key stores a new event");
    assert_eq!(after.first_seq, Some(Seq(3)));

    let replaced = f
        .append(&feed, vec![event("a retry").dedupe_key("a")])
        .await;
    assert_eq!(replaced.duplicates, 1);
    assert_eq!(
        replaced.events[0].seq,
        Seq(3),
        "the row now names the new event"
    );

    assert_eq!(
        f.store
            .purge_dedupe(&f.calls, &feed, 100)
            .await
            .expect("purge"),
        1,
        "only the expired row goes"
    );
    assert_eq!(
        f.store
            .purge_dedupe(&f.calls, &feed, 100)
            .await
            .expect("purge"),
        0
    );
}

/// Concurrent retries carrying one key store one event.
pub async fn dedupe_under_concurrent_retries(backend: &dyn Backend) {
    const RETRIES: usize = 16;
    let f = Fixture::open(backend).await;
    let feed = f.feed(StoreLimits::new()).await;
    let mut tasks = Vec::new();
    for i in 0..RETRIES {
        let store = f.store.clone();
        let feed = feed.clone();
        tasks.push(tokio::spawn(async move {
            store
                .append(
                    &CallContext::new(),
                    &feed,
                    vec![event(&format!("retry {i}")).dedupe_key("same")],
                )
                .await
                .expect("append")
        }));
    }
    let mut stored = 0;
    let mut ids = BTreeSet::new();
    for task in tasks {
        let ack = task.await.expect("a retry finished");
        stored += ack.stored();
        assert_eq!(ack.events[0].seq, Seq(1));
        ids.insert(ack.events[0].id.clone());
    }
    assert_eq!(stored, 1, "one retry stored the event");
    assert_eq!(ids.len(), 1, "every retry is told the same id");
    assert_eq!(f.head(&feed).await.watermark, Seq(1));
}

/// The tenant quota and the undelivered limits are checked inside the append:
/// a refusal is typed and stores nothing.
pub async fn limits_refuse_and_store_nothing(backend: &dyn Backend) {
    let f = Fixture::open(backend).await;

    let tenant = TenantId::new(format!("t-{}", FeedUid::generate())).expect("a tenant id");
    let quota = StoreLimits::new().tenant_max_bytes(100);
    let (one, two) = (FeedUid::generate(), FeedUid::generate());
    for feed in [&one, &two] {
        f.store
            .ensure_feed(&f.calls, feed, Some(&tenant), &quota)
            .await
            .expect("ensure_feed");
    }
    f.append(&one, vec![sized(60)]).await;
    let refused = f
        .store
        .append(&f.calls, &two, vec![sized(30), sized(30)])
        .await;
    assert_eq!(full_limit(refused), FullLimit::TenantBytes);
    assert_eq!(
        f.head(&two).await.watermark,
        Seq(0),
        "a refused append stores nothing"
    );
    f.append(&two, vec![sized(40)]).await;
    let refused = f.store.append(&f.calls, &one, vec![sized(1)]).await;
    assert_eq!(full_limit(refused), FullLimit::TenantBytes);
    let other_tenant = f.feed(quota.clone()).await;
    f.append(&other_tenant, vec![sized(90)]).await;
    if let Some(held) = f
        .store
        .tenant_bytes(&f.calls, &tenant)
        .await
        .expect("tenant_bytes")
    {
        assert_eq!(held, 100, "the bytes the quota is checked against");
    }

    let by_events = f.feed(StoreLimits::new().undelivered_max_events(3)).await;
    f.fill(&by_events, 5).await;
    let reader = f.delivery(&by_events, Start::Earliest).await;
    let refused = f.store.append(&f.calls, &by_events, vec![event("x")]).await;
    assert_eq!(full_limit(refused), FullLimit::UndeliveredEvents);
    f.advance(&reader, Seq(6)).await;
    f.fill(&by_events, 3).await;
    let refused = f.store.append(&f.calls, &by_events, vec![event("x")]).await;
    assert_eq!(full_limit(refused), FullLimit::UndeliveredEvents);
    assert_eq!(f.head(&by_events).await.watermark, Seq(8));

    let by_bytes = f.feed(StoreLimits::new().undelivered_max_bytes(10)).await;
    let reader = f.delivery(&by_bytes, Start::Earliest).await;
    f.append(&by_bytes, vec![sized(8)]).await;
    let refused = f
        .store
        .append(&f.calls, &by_bytes, vec![sized(1), sized(8)])
        .await;
    assert_eq!(full_limit(refused), FullLimit::UndeliveredBytes);
    assert_eq!(f.head(&by_bytes).await.watermark, Seq(1));
    f.advance(&reader, Seq(2)).await;
    f.append(&by_bytes, vec![sized(9)]).await;

    let by_age = f.feed(StoreLimits::new().undelivered_max_age(SHORT)).await;
    f.delivery(&by_age, Start::Earliest).await;
    f.fill(&by_age, 1).await;
    f.fill(&by_age, 1).await;
    f.wait(PAST_SHORT).await;
    let refused = f.store.append(&f.calls, &by_age, vec![event("x")]).await;
    assert_eq!(full_limit(refused), FullLimit::UndeliveredAge);
}

/// Of several concurrent claimers of one range, exactly one wins.
pub async fn one_claimer_wins(backend: &dyn Backend) {
    const CLAIMERS: usize = 16;
    let f = Fixture::open(backend).await;
    let feed = f.feed(StoreLimits::new()).await;
    f.fill(&feed, 10).await;
    let delivery = f.delivery(&feed, Start::Earliest).await;

    let mut tasks = Vec::new();
    for i in 0..CLAIMERS {
        let store = f.store.clone();
        let delivery = delivery.clone();
        tasks.push(tokio::spawn(async move {
            store
                .claim(
                    &CallContext::new(),
                    &delivery,
                    Seq(1),
                    Seq(11),
                    &owner(&format!("c{i}")),
                    TTL,
                )
                .await
                .expect("claim")
        }));
    }
    let mut winners = Vec::new();
    for task in tasks {
        if let Some(claim) = task.await.expect("a claimer finished") {
            winners.push(claim);
        }
    }
    assert_eq!(winners.len(), 1, "exactly one claimer wins");
    let claim = &winners[0];
    assert_eq!(
        (claim.from, claim.to, claim.epoch),
        (Seq(1), Seq(11), Epoch(0))
    );

    let head = f.head(&feed).await;
    let listed = head.deliveries[0]
        .claim
        .as_ref()
        .expect("the claim is listed");
    assert_eq!(listed.id, claim.id);
    assert_eq!(listed.to, Seq(11));
    assert_eq!(listed.owner, claim.owner);
    assert!(!listed.attached);
    assert!(listed.expires_at > head.now);

    let again = f
        .store
        .claim(&f.calls, &delivery, Seq(1), Seq(11), &owner("late"), TTL)
        .await
        .expect("claim");
    assert!(again.is_none(), "a live claim excludes another");
    f.store.release(&f.calls, &claim.id).await.expect("release");

    let moved = f
        .store
        .claim(&f.calls, &delivery, Seq(2), Seq(5), &owner("stale"), TTL)
        .await
        .expect("claim");
    assert!(moved.is_none(), "the cursor must match");
    let past = f
        .store
        .claim(&f.calls, &delivery, Seq(1), Seq(12), &owner("far"), TTL)
        .await
        .expect("claim");
    assert!(past.is_none(), "a claim never passes the watermark");
    assert!(matches!(
        f.store
            .claim(&f.calls, &delivery, Seq(1), Seq(1), &owner("empty"), TTL)
            .await,
        Err(FeedError::Invalid(_))
    ));
    let next = f
        .store
        .claim(&f.calls, &delivery, Seq(1), Seq(4), &owner("next"), TTL)
        .await
        .expect("claim");
    assert!(next.is_some(), "a released range can be claimed again");
}

/// A claim past its TTL, by the store's clock, is gone even when nobody
/// replaced it.
pub async fn an_expired_claim_is_gone(backend: &dyn Backend) {
    let f = Fixture::open(backend).await;
    let feed = f.feed(StoreLimits::new()).await;
    f.fill(&feed, 5).await;
    let delivery = f.delivery(&feed, Start::Earliest).await;
    let claim = f
        .store
        .claim(&f.calls, &delivery, Seq(1), Seq(6), &owner("a"), SHORT)
        .await
        .expect("claim")
        .expect("granted");
    let holder = f.store.attach(&f.calls, &claim.id).await.expect("attach");
    assert_eq!((holder.cursor, holder.to), (Seq(1), Seq(6)));
    assert_eq!(holder.delivery, delivery);

    f.wait(PAST_SHORT).await;
    assert!(!f.store.renew(&f.calls, &holder, TTL).await.expect("renew"));
    assert!(
        !f.store
            .renew_claim(&f.calls, &claim.id, TTL)
            .await
            .expect("renew_claim")
    );
    assert!(matches!(
        f.store.commit(&f.calls, &holder, Seq(1), Seq(6)).await,
        Err(FeedError::ClaimLost { .. })
    ));
    assert!(matches!(
        f.store.attach(&f.calls, &claim.id).await,
        Err(FeedError::ClaimLost { .. })
    ));
    assert_eq!(f.cursor(&delivery).await, Seq(1), "the cursor did not move");
    assert!(f.head(&feed).await.deliveries[0].claim.is_none());

    let next = f
        .store
        .claim(&f.calls, &delivery, Seq(1), Seq(6), &owner("b"), TTL)
        .await
        .expect("claim")
        .expect("an expired claim does not block the next");
    assert!(!f.store.renew(&f.calls, &holder, TTL).await.expect("renew"));
    f.store.release(&f.calls, &claim.id).await.expect("release");
    assert!(
        f.store
            .renew_claim(&f.calls, &next.id, TTL)
            .await
            .expect("renew_claim"),
        "releasing a dead claim leaves its successor alone"
    );
}

/// Attaching rotates the holder token; renewing by claim id does not.
pub async fn attach_fences_the_earlier_holder(backend: &dyn Backend) {
    let f = Fixture::open(backend).await;
    let feed = f.feed(StoreLimits::new()).await;
    f.fill(&feed, 5).await;
    let delivery = f.delivery(&feed, Start::Earliest).await;
    let claim = f
        .store
        .claim(&f.calls, &delivery, Seq(1), Seq(6), &owner("a"), TTL)
        .await
        .expect("claim")
        .expect("granted");

    let first = f.store.attach(&f.calls, &claim.id).await.expect("attach");
    assert!(f.store.renew(&f.calls, &first, TTL).await.expect("renew"));
    assert!(
        f.head(&feed).await.deliveries[0]
            .claim
            .as_ref()
            .is_some_and(|c| c.attached)
    );
    assert!(
        f.store
            .renew_claim(&f.calls, &claim.id, TTL)
            .await
            .expect("renew_claim")
    );
    assert!(
        f.store.renew(&f.calls, &first, TTL).await.expect("renew"),
        "renewing by claim id rotates nothing"
    );

    let second = f.store.attach(&f.calls, &claim.id).await.expect("attach");
    assert_ne!(first.token, second.token);
    assert!(!f.store.renew(&f.calls, &first, TTL).await.expect("renew"));
    assert!(matches!(
        f.store.commit(&f.calls, &first, Seq(1), Seq(3)).await,
        Err(FeedError::ClaimLost { .. })
    ));
    assert_eq!(f.cursor(&delivery).await, Seq(1));
    assert!(f.store.renew(&f.calls, &second, TTL).await.expect("renew"));
    f.store
        .commit(&f.calls, &second, Seq(1), Seq(3))
        .await
        .expect("the current holder commits");
    assert_eq!(f.cursor(&delivery).await, Seq(3));

    f.store.release(&f.calls, &claim.id).await.expect("release");
    assert!(!f.store.renew(&f.calls, &second, TTL).await.expect("renew"));
    assert!(matches!(
        f.store.commit(&f.calls, &second, Seq(3), Seq(4)).await,
        Err(FeedError::ClaimLost { .. })
    ));
    f.store
        .release(&f.calls, &claim.id)
        .await
        .expect("releasing twice is not an error");
}

/// A commit is a compare-and-set on the cursor: it may pass the claim's end
/// but never the watermark, and it never goes back.
pub async fn commit_is_fenced_and_bounded(backend: &dyn Backend) {
    let f = Fixture::open(backend).await;
    let feed = f.feed(StoreLimits::new()).await;
    f.fill(&feed, 10).await;
    let delivery = f.delivery(&feed, Start::Earliest).await;
    let claim = f
        .store
        .claim(&f.calls, &delivery, Seq(1), Seq(6), &owner("a"), TTL)
        .await
        .expect("claim")
        .expect("granted");
    let holder = f.store.attach(&f.calls, &claim.id).await.expect("attach");

    match f.store.commit(&f.calls, &holder, Seq(2), Seq(3)).await {
        Err(FeedError::CursorConflict {
            expected, actual, ..
        }) => {
            assert_eq!((expected, actual), (Seq(2), Seq(1)));
        }
        other => panic!("expected a cursor conflict, got {other:?}"),
    }
    assert!(matches!(
        f.store.commit(&f.calls, &holder, Seq(1), Seq(12)).await,
        Err(FeedError::PastWatermark { .. })
    ));
    assert_eq!(f.cursor(&delivery).await, Seq(1));

    f.store
        .commit(&f.calls, &holder, Seq(1), Seq(8))
        .await
        .expect("a commit may pass the claim's end");
    assert_eq!(f.cursor(&delivery).await, Seq(8));
    assert!(matches!(
        f.store.commit(&f.calls, &holder, Seq(8), Seq(5)).await,
        Err(FeedError::Invalid(_))
    ));
    f.store
        .commit(&f.calls, &holder, Seq(8), Seq(11))
        .await
        .expect("a commit may reach the feed's end");
    f.store
        .commit(&f.calls, &holder, Seq(11), Seq(11))
        .await
        .expect("a commit that moves nothing is allowed");
    let head = f.head(&feed).await;
    assert_eq!(head.deliveries[0].cursor, Seq(11));
    assert_eq!(
        head.deliveries[0].epoch,
        Epoch(0),
        "a commit keeps the epoch"
    );
    assert_eq!(head.deliveries[0].oldest_undelivered_at, None);

    f.store.release(&f.calls, &claim.id).await.expect("release");
    let replaced = f
        .store
        .claim(
            &f.calls,
            &delivery,
            Seq(11),
            Seq(11).next(),
            &owner("b"),
            TTL,
        )
        .await
        .expect("claim");
    assert!(replaced.is_none(), "nothing left to claim");
    f.fill(&feed, 1).await;
    let replaced = f
        .store
        .claim(&f.calls, &delivery, Seq(11), Seq(12), &owner("b"), TTL)
        .await
        .expect("claim")
        .expect("granted");
    assert!(matches!(
        f.store.commit(&f.calls, &holder, Seq(11), Seq(12)).await,
        Err(FeedError::ClaimLost { .. })
    ));
    f.store
        .release(&f.calls, &replaced.id)
        .await
        .expect("release");
}

/// Outside a commit the cursor moves only through `move_cursor`: refused while
/// a claim is live, bumping the epoch, a skip always leaving a gap, a replay
/// going back no further than the oldest retained event.
pub async fn cursor_moves_are_guarded_and_recorded(backend: &dyn Backend) {
    let f = Fixture::open(backend).await;
    let feed = f.feed(StoreLimits::new().max_age(Duration::ZERO)).await;
    f.fill(&feed, 10).await;
    let delivery = f.delivery(&feed, Start::Earliest).await;
    let skip = |reason: &str| Move::Skip {
        reason: reason.to_string(),
        by: "admin".to_string(),
    };

    let claim = f
        .store
        .claim(&f.calls, &delivery, Seq(1), Seq(6), &owner("a"), TTL)
        .await
        .expect("claim")
        .expect("granted");
    assert!(matches!(
        f.store
            .move_cursor(&f.calls, &delivery, Seq(1), Seq(4), skip("poison"))
            .await,
        Err(FeedError::ClaimLive { .. })
    ));
    assert_eq!(f.cursor(&delivery).await, Seq(1));
    f.store.release(&f.calls, &claim.id).await.expect("release");

    let epoch = f
        .store
        .move_cursor(&f.calls, &delivery, Seq(1), Seq(4), skip("poison"))
        .await
        .expect("skip");
    assert_eq!(epoch, Epoch(1));
    assert_eq!(f.cursor(&delivery).await, Seq(4));
    let gaps = f
        .store
        .gaps(&f.calls, &feed, Some(&delivery))
        .await
        .expect("gaps");
    assert_eq!(gaps.len(), 1, "a skip always records a gap");
    assert_eq!((gaps[0].from, gaps[0].to), (Seq(1), Seq(4)));
    assert_eq!(gaps[0].reason, "poison");
    assert_eq!(gaps[0].by, "admin");
    assert_eq!(gaps[0].delivery.as_ref(), Some(&delivery));

    match f
        .store
        .move_cursor(&f.calls, &delivery, Seq(1), Seq(5), skip("stale"))
        .await
    {
        Err(FeedError::CursorConflict { actual, .. }) => assert_eq!(actual, Seq(4)),
        other => panic!("expected a cursor conflict, got {other:?}"),
    }
    assert!(matches!(
        f.store
            .move_cursor(&f.calls, &delivery, Seq(4), Seq(12), skip("far"))
            .await,
        Err(FeedError::PastWatermark { .. })
    ));
    assert!(matches!(
        f.store
            .move_cursor(&f.calls, &delivery, Seq(4), Seq(3), skip("back"))
            .await,
        Err(FeedError::Invalid(_))
    ));
    assert!(matches!(
        f.store
            .move_cursor(&f.calls, &delivery, Seq(4), Seq(6), Move::Replay)
            .await,
        Err(FeedError::Invalid(_))
    ));

    let epoch = f
        .store
        .move_cursor(&f.calls, &delivery, Seq(4), Seq(2), Move::Replay)
        .await
        .expect("replay");
    assert_eq!(epoch, Epoch(2));
    assert_eq!(f.cursor(&delivery).await, Seq(2));
    assert_eq!(
        f.store
            .gaps(&f.calls, &feed, Some(&delivery))
            .await
            .expect("gaps")
            .len(),
        1,
        "a replay records no gap"
    );

    f.store
        .move_cursor(&f.calls, &delivery, Seq(2), Seq(6), skip("more"))
        .await
        .expect("skip");
    let trimmed = f.store.trim(&f.calls, &feed, 100).await.expect("trim");
    assert_eq!(trimmed.events, 5);
    assert!(matches!(
        f.store
            .move_cursor(&f.calls, &delivery, Seq(6), Seq(3), Move::Replay)
            .await,
        Err(FeedError::OutOfRange { .. })
    ));
    assert_eq!(f.cursor(&delivery).await, Seq(6));

    assert!(matches!(
        f.store
            .move_cursor(&f.calls, &delivery, Seq(6), Seq(12), Move::Recover)
            .await,
        Err(FeedError::OutOfRange { .. })
    ));
    let epoch = f
        .store
        .move_cursor(&f.calls, &delivery, Seq(6), Seq(11), Move::Recover)
        .await
        .expect("recover");
    assert_eq!(epoch, Epoch(4));
    let all = f.store.gaps(&f.calls, &feed, None).await.expect("gaps");
    assert_eq!(all.len(), 3);
    assert!(
        all.iter()
            .any(|g| g.reason == "recover" && (g.from, g.to) == (Seq(6), Seq(11)))
    );
    assert_eq!(f.head(&feed).await.deliveries[0].epoch, Epoch(4));

    let unknown = DeliveryUid::generate(feed.clone());
    assert!(matches!(
        f.store
            .move_cursor(&f.calls, &unknown, Seq(1), Seq(2), skip("none"))
            .await,
        Err(FeedError::DeliveryNotFound(_))
    ));

    let expired = f
        .store
        .claim(&f.calls, &delivery, Seq(11), Seq(11), &owner("x"), SHORT)
        .await;
    assert!(matches!(expired, Err(FeedError::Invalid(_))));
    f.fill(&feed, 2).await;
    f.store
        .claim(&f.calls, &delivery, Seq(11), Seq(13), &owner("x"), SHORT)
        .await
        .expect("claim")
        .expect("granted");
    f.wait(PAST_SHORT).await;
    f.store
        .move_cursor(&f.calls, &delivery, Seq(11), Seq(12), skip("after expiry"))
        .await
        .expect("an expired claim does not block a move");
}

/// A trim removes only events older than the feed's `max_age` and below every
/// delivery cursor, never inside a claimed range, at most `max_rows` a call.
pub async fn trim_respects_age_cursors_and_claims(backend: &dyn Backend) {
    let f = Fixture::open(backend).await;
    let now = StoreLimits::new().max_age(Duration::ZERO);

    let free = f.feed(now.clone()).await;
    f.append(&free, (0..10).map(|_| sized(7)).collect()).await;
    let first = f.store.trim(&f.calls, &free, 4).await.expect("trim");
    assert_eq!((first.events, first.bytes, first.more), (4, 28, true));
    let head = f.head(&free).await;
    assert_eq!(
        (head.oldest, head.bytes, head.watermark),
        (Seq(5), 42, Seq(10))
    );
    let rest = f.store.trim(&f.calls, &free, 100).await.expect("trim");
    assert_eq!((rest.events, rest.more), (6, false));
    let head = f.head(&free).await;
    assert_eq!(
        (head.oldest, head.bytes, head.watermark),
        (Seq(11), 0, Seq(10))
    );
    assert_eq!(
        f.fill(&free, 1).await.first_seq,
        Some(Seq(11)),
        "sequence numbers are never reused after a trim"
    );
    assert_eq!(
        f.store.trim(&f.calls, &free, 0).await.expect("trim").events,
        0
    );

    let read = f.feed(now.clone()).await;
    f.fill(&read, 10).await;
    let slow = f.delivery(&read, Start::Earliest).await;
    let fast = f.delivery(&read, Start::Earliest).await;
    f.advance(&slow, Seq(4)).await;
    f.advance(&fast, Seq(7)).await;
    let held = f
        .store
        .claim(&f.calls, &fast, Seq(7), Seq(10), &owner("fast"), TTL)
        .await
        .expect("claim")
        .expect("granted");
    let trimmed = f.store.trim(&f.calls, &read, 100).await.expect("trim");
    assert_eq!(trimmed.events, 3, "a trim stops at the lowest cursor");
    assert_eq!(f.head(&read).await.oldest, Seq(4));
    f.store
        .read(&f.calls, &read, Seq(4), Seq(11))
        .await
        .expect("undelivered events are intact");
    assert!(matches!(
        f.store.read(&f.calls, &read, Seq(1), Seq(5)).await,
        Err(FeedError::RangeUnavailable { .. })
    ));
    assert!(matches!(
        f.store
            .bound(&f.calls, &read, Seq(2), BatchLimits::default())
            .await,
        Err(FeedError::RangeUnavailable { .. })
    ));

    f.advance(&slow, Seq(9)).await;
    let trimmed = f.store.trim(&f.calls, &read, 100).await.expect("trim");
    assert_eq!(trimmed.events, 3, "a trim never enters a claimed range");
    assert_eq!(f.head(&read).await.oldest, Seq(7));
    let claimed = f
        .store
        .read(&f.calls, &read, Seq(7), Seq(10))
        .await
        .expect("the claimed range is intact");
    assert_eq!(claimed.events.len(), 3);
    f.store.release(&f.calls, &held.id).await.expect("release");

    let young = f
        .feed(StoreLimits::new().max_age(Duration::from_secs(3600)))
        .await;
    f.fill(&young, 5).await;
    let trimmed = f.store.trim(&f.calls, &young, 100).await.expect("trim");
    assert_eq!(trimmed.events, 0, "events younger than max_age stay");

    let aging = f.feed(StoreLimits::new().max_age(SHORT)).await;
    f.fill(&aging, 3).await;
    f.wait(PAST_SHORT).await;
    f.fill(&aging, 2).await;
    let trimmed = f.store.trim(&f.calls, &aging, 100).await.expect("trim");
    assert_eq!(trimmed.events, 3, "only events past max_age go");
    assert_eq!(f.head(&aging).await.oldest, Seq(4));

    assert!(matches!(
        f.store.trim(&f.calls, &FeedUid::generate(), 10).await,
        Err(FeedError::FeedNotFound(_))
    ));
}

/// `bound` fits events and bytes, `bytes_from` sums what is retained from a
/// position on, `seq_at` finds a position by store time, and `heads` reports
/// every listed feed at once.
pub async fn bound_seq_at_and_heads(backend: &dyn Backend) {
    let f = Fixture::open(backend).await;
    let feed = f.feed(StoreLimits::new()).await;
    f.append(
        &feed,
        vec![sized(10), sized(10), sized(10), sized(50), sized(10)],
    )
    .await;
    let bound = |from: u64, events: u64, bytes: u64| {
        let (store, calls, feed) = (f.store.clone(), f.calls.clone(), feed.clone());
        async move {
            store
                .bound(&calls, &feed, Seq(from), BatchLimits::new(events, bytes))
                .await
        }
    };
    assert_eq!(bound(1, 3, 1000).await.expect("bound"), Seq(4));
    assert_eq!(bound(1, 100, 25).await.expect("bound"), Seq(3));
    assert_eq!(bound(1, 100, 30).await.expect("bound"), Seq(4));
    assert_eq!(bound(1, 100, 1000).await.expect("bound"), Seq(6));
    assert_eq!(
        bound(4, 100, 20).await.expect("bound"),
        Seq(5),
        "one oversized event still forms a batch"
    );
    assert_eq!(bound(6, 100, 1000).await.expect("bound"), Seq(6));
    assert_eq!(bound(1, 0, 1000).await.expect("bound"), Seq(1));
    assert!(matches!(
        bound(7, 100, 1000).await,
        Err(FeedError::PastWatermark { .. })
    ));

    let bytes_from = |from: u64| {
        let (store, calls, feed) = (f.store.clone(), f.calls.clone(), feed.clone());
        async move {
            store
                .bytes_from(&calls, &feed, Seq(from))
                .await
                .expect("bytes_from")
        }
    };
    if let Some(all) = bytes_from(1).await {
        assert_eq!(all, 90, "every retained payload byte");
        assert_eq!(bytes_from(0).await, Some(90));
        assert_eq!(bytes_from(4).await, Some(60));
        assert_eq!(bytes_from(5).await, Some(10));
        assert_eq!(bytes_from(6).await, Some(0), "nothing at the feed's end");
        assert_eq!(bytes_from(1000).await, Some(0), "nor past it");
        let other = f.feed(StoreLimits::new()).await;
        f.append(&other, vec![sized(7)]).await;
        assert_eq!(
            bytes_from(1).await,
            Some(90),
            "another feed's bytes are not counted"
        );
        assert_eq!(
            f.store
                .bytes_from(&f.calls, &other, Seq(1))
                .await
                .expect("bytes_from"),
            Some(7)
        );
    } else {
        assert_eq!(
            bytes_from(4).await,
            None,
            "a backend answers every range or none"
        );
    }

    if let Some(hourly) = f
        .store
        .hourly_counts(&f.calls, &feed, 24)
        .await
        .expect("hourly_counts")
    {
        assert_eq!(hourly.len(), 24, "one count per hour, oldest first");
        assert_eq!(hourly[23], 5, "everything was stored in the last hour");
        assert_eq!(hourly.iter().sum::<u64>(), 5);
    }
    let empty = f.feed(StoreLimits::new()).await;
    if let Some(newest) = f
        .store
        .newest_received(&f.calls, &[feed.clone(), empty.clone()])
        .await
        .expect("newest_received")
    {
        let last = f
            .store
            .read(&f.calls, &feed, Seq(5), Seq(6))
            .await
            .expect("read")
            .events[0]
            .received_at;
        assert_eq!(
            newest,
            vec![(feed.clone(), last)],
            "a feed with no event is left out"
        );
    }

    let before = f.head(&feed).await.now;
    f.wait(Duration::from_millis(50)).await;
    let between = f.head(&feed).await.now;
    f.wait(Duration::from_millis(50)).await;
    f.fill(&feed, 2).await;
    let at = |time| {
        let (store, calls, feed) = (f.store.clone(), f.calls.clone(), feed.clone());
        async move { store.seq_at(&calls, &feed, time).await.expect("seq_at") }
    };
    assert_eq!(at(millis(0)).await, Seq(1));
    assert_eq!(at(between).await, Seq(6));
    assert_eq!(at(before + chrono::Duration::days(1)).await, Seq(8));
    let timed = DeliveryUid::generate(feed.clone());
    let state = f
        .store
        .ensure_delivery(&f.calls, &timed, Start::Time(between))
        .await
        .expect("ensure_delivery");
    assert_eq!(state.cursor, Seq(6));

    let tenant = TenantId::new(format!("t-{}", FeedUid::generate())).expect("a tenant id");
    let owned = FeedUid::generate();
    f.store
        .ensure_feed(&f.calls, &owned, Some(&tenant), &StoreLimits::new())
        .await
        .expect("ensure_feed");
    let empty = f.feed(StoreLimits::new()).await;
    let lagging = f.delivery(&feed, Start::Seq(Seq(3))).await;
    let caught_up = f.delivery(&feed, Start::Latest).await;

    let asked = vec![
        feed.clone(),
        owned.clone(),
        FeedUid::generate(),
        empty.clone(),
    ];
    let heads = f.store.heads(&f.calls, &asked).await.expect("heads");
    assert_eq!(heads.len(), 3, "an unknown feed is left out");
    let of = |uid: &FeedUid| heads.iter().find(|h| &h.feed == uid).expect("listed");
    assert_eq!(of(&owned).tenant.as_ref(), Some(&tenant));
    assert_eq!(of(&empty).watermark, Seq(0));
    assert!(of(&empty).deliveries.is_empty());
    let head = of(&feed);
    assert_eq!(
        (head.watermark, head.oldest, head.bytes),
        (Seq(7), Seq(1), 90 + 2 * event("e0").size())
    );
    assert_eq!(head.tenant, None);
    assert_eq!(head.deliveries.len(), 3);
    let position = |d: &DeliveryUid| {
        head.deliveries
            .iter()
            .find(|h| &h.delivery == d)
            .expect("the delivery is listed")
    };
    assert_eq!(position(&lagging).cursor, Seq(3));
    let oldest = position(&lagging)
        .oldest_undelivered_at
        .expect("a lagging delivery has an oldest undelivered event");
    assert!(oldest <= head.now);
    assert!(
        oldest <= before,
        "it is the store time of the event at the cursor"
    );
    assert_eq!(position(&caught_up).cursor, Seq(8));
    assert_eq!(position(&caught_up).oldest_undelivered_at, None);
    assert_eq!(position(&timed).cursor, Seq(6));

    assert!(
        f.store
            .heads(&f.calls, &[])
            .await
            .expect("heads")
            .is_empty()
    );
}

/// A delivery starts where it is told, keeps its place when ensured again,
/// and goes away with its feed.
pub async fn delivery_start_delete_and_purge(backend: &dyn Backend) {
    let f = Fixture::open(backend).await;
    let feed = f.feed(StoreLimits::new().max_age(Duration::ZERO)).await;
    f.append(&feed, vec![event("a").dedupe_key("k"), event("b")])
        .await;
    f.fill(&feed, 3).await;

    let start = |start: Start| {
        let (store, calls, d) = (
            f.store.clone(),
            f.calls.clone(),
            DeliveryUid::generate(feed.clone()),
        );
        async move { (store.ensure_delivery(&calls, &d, start).await, d) }
    };
    let (earliest, first) = start(Start::Earliest).await;
    let earliest = earliest.expect("earliest");
    assert_eq!(
        (earliest.cursor, earliest.epoch, earliest.created),
        (Seq(1), Epoch(0), true)
    );
    assert_eq!(start(Start::Latest).await.0.expect("latest").cursor, Seq(6));
    assert_eq!(
        start(Start::Seq(Seq(3))).await.0.expect("seq").cursor,
        Seq(3)
    );
    assert_eq!(
        start(Start::Seq(Seq(6))).await.0.expect("end").cursor,
        Seq(6)
    );
    assert!(matches!(
        start(Start::Seq(Seq(9))).await.0,
        Err(FeedError::OutOfRange { .. })
    ));

    f.advance(&first, Seq(3)).await;
    let again = f
        .store
        .ensure_delivery(&f.calls, &first, Start::Latest)
        .await
        .expect("ensure_delivery");
    assert_eq!(
        (again.cursor, again.created),
        (Seq(3), false),
        "an existing delivery keeps its position"
    );
    assert!(matches!(
        f.store
            .ensure_delivery(
                &f.calls,
                &DeliveryUid::generate(FeedUid::generate()),
                Start::Earliest
            )
            .await,
        Err(FeedError::FeedNotFound(_))
    ));

    let tenant = TenantId::new("someone").expect("a tenant id");
    assert!(matches!(
        f.store
            .ensure_feed(&f.calls, &feed, Some(&tenant), &StoreLimits::new())
            .await,
        Err(FeedError::TenantMismatch { .. })
    ));
    f.store
        .ensure_feed(
            &f.calls,
            &feed,
            None,
            &StoreLimits::new()
                .max_age(Duration::ZERO)
                .undelivered_max_events(1),
        )
        .await
        .expect("ensuring again updates the limits");
    assert_eq!(
        full_limit(f.store.append(&f.calls, &feed, vec![event("x")]).await),
        FullLimit::UndeliveredEvents
    );
    assert_eq!(
        f.head(&feed).await.watermark,
        Seq(5),
        "ensuring again keeps the events"
    );

    let trimmed = f.feed(StoreLimits::new().max_age(Duration::ZERO)).await;
    f.fill(&trimmed, 5).await;
    f.store.trim(&f.calls, &trimmed, 100).await.expect("trim");
    let late = f.delivery(&trimmed, Start::Earliest).await;
    assert_eq!(f.cursor(&late).await, Seq(6));
    let gaps = f
        .store
        .gaps(&f.calls, &trimmed, Some(&late))
        .await
        .expect("gaps");
    assert_eq!(
        gaps.len(),
        1,
        "starting after a trim records what was missed"
    );
    assert_eq!((gaps[0].from, gaps[0].to), (Seq(1), Seq(6)));
    assert!(matches!(
        f.store
            .ensure_delivery(
                &f.calls,
                &DeliveryUid::generate(trimmed.clone()),
                Start::Seq(Seq(2))
            )
            .await,
        Err(FeedError::OutOfRange { .. })
    ));

    f.store
        .delete_delivery(&f.calls, &late)
        .await
        .expect("delete");
    assert!(f.head(&trimmed).await.deliveries.is_empty());
    assert!(
        f.store
            .gaps(&f.calls, &trimmed, None)
            .await
            .expect("gaps")
            .is_empty()
    );
    f.store
        .delete_delivery(&f.calls, &late)
        .await
        .expect("deleting twice is not an error");
    let reborn = f
        .store
        .ensure_delivery(&f.calls, &late, Start::Latest)
        .await
        .expect("ensure_delivery");
    assert!(reborn.created);

    f.store
        .move_cursor(
            &f.calls,
            &first,
            Seq(3),
            Seq(4),
            Move::Skip {
                reason: "r".into(),
                by: "b".into(),
            },
        )
        .await
        .expect("skip");
    let stats = f.store.purge_feed(&f.calls, &feed).await.expect("purge");
    assert_eq!(
        (stats.events, stats.deliveries, stats.dedupe, stats.gaps),
        (5, 4, 1, 1)
    );
    assert!(
        f.store
            .heads(&f.calls, std::slice::from_ref(&feed))
            .await
            .expect("heads")
            .is_empty()
    );
    assert!(matches!(
        f.store.append(&f.calls, &feed, vec![event("x")]).await,
        Err(FeedError::FeedNotFound(_))
    ));
    let none = f.store.purge_feed(&f.calls, &feed).await.expect("purge");
    assert_eq!(
        (none.events, none.deliveries, none.dedupe, none.gaps),
        (0, 0, 0, 0)
    );
    assert_eq!(
        f.head(&trimmed).await.watermark,
        Seq(5),
        "another feed is untouched"
    );
}

/// Events, cursors, claims, dedupe keys and gaps are all there after the
/// store is opened again.
pub async fn state_survives_a_restart(backend: &dyn Backend) {
    let before = Fixture::open(backend).await;
    let feed = before.feed(StoreLimits::new()).await;
    let ack = before
        .append(
            &feed,
            vec![
                event("a").dedupe_key("k").header("x-event-id", "1"),
                event("b").sealed(true),
                event("c"),
            ],
        )
        .await;
    let sent = before
        .store
        .read(&before.calls, &feed, Seq(1), Seq(4))
        .await
        .expect("read");
    let delivery = before.delivery(&feed, Start::Earliest).await;
    before.advance(&delivery, Seq(2)).await;
    before
        .store
        .move_cursor(
            &before.calls,
            &delivery,
            Seq(2),
            Seq(3),
            Move::Skip {
                reason: "poison".into(),
                by: "admin".into(),
            },
        )
        .await
        .expect("skip");
    let claim = before
        .store
        .claim(&before.calls, &delivery, Seq(3), Seq(4), &owner("a"), TTL)
        .await
        .expect("claim")
        .expect("granted");
    let holder = before
        .store
        .attach(&before.calls, &claim.id)
        .await
        .expect("attach");
    drop(before);

    let after = Fixture::open(backend).await;
    let head = after.head(&feed).await;
    assert_eq!((head.watermark, head.oldest), (Seq(3), Seq(1)));
    assert_eq!(head.bytes, sent.bytes());
    let position = &head.deliveries[0];
    assert_eq!((position.cursor, position.epoch), (Seq(3), Epoch(1)));
    assert_eq!(position.claim.as_ref().map(|c| &c.id), Some(&claim.id));
    let read = after
        .store
        .read(&after.calls, &feed, Seq(1), Seq(4))
        .await
        .expect("read");
    assert_eq!(read, sent, "events read back exactly as stored");

    let retry = after.append(&feed, vec![event("a").dedupe_key("k")]).await;
    assert_eq!(retry.duplicates, 1);
    assert_eq!(retry.events[0].id, ack.events[0].id);
    assert_eq!(after.fill(&feed, 1).await.first_seq, Some(Seq(4)));
    assert!(
        after
            .store
            .renew(&after.calls, &holder, TTL)
            .await
            .expect("renew"),
        "a holder outlives the process that attached it"
    );
    after
        .store
        .commit(&after.calls, &holder, Seq(3), Seq(4))
        .await
        .expect("commit");
    let gaps = after
        .store
        .gaps(&after.calls, &feed, None)
        .await
        .expect("gaps");
    assert_eq!(gaps.len(), 1);
    assert_eq!((gaps[0].from, gaps[0].to), (Seq(2), Seq(3)));
}

/// A store written by a newer release is refused with a typed error.
pub async fn a_newer_schema_is_refused(backend: &dyn Backend) {
    drop(backend.open().await.expect("open the store"));
    let version = backend.schema_version().await.expect("schema_version");
    assert!(version >= 1, "the store records its format version");
    backend
        .set_schema_version(version + 1)
        .await
        .expect("set_schema_version");
    let refused = backend.open().await;
    backend
        .set_schema_version(version)
        .await
        .expect("set_schema_version");
    match refused {
        Err(FeedError::NewerSchema { found, supported }) => {
            assert_eq!((found, supported), (version + 1, version));
        }
        Err(other) => panic!("expected a newer-schema refusal, got {other:?}"),
        Ok(_) => panic!("a newer store must be refused"),
    }
    let f = Fixture::open(backend).await;
    let feed = f.feed(StoreLimits::new()).await;
    f.fill(&feed, 1).await;
}

/// One owner holds a feed's maintenance claim at a time, for its TTL.
pub async fn one_maintenance_holder(backend: &dyn Backend) {
    let f = Fixture::open(backend).await;
    let feed = f.feed(StoreLimits::new()).await;
    let other = f.feed(StoreLimits::new()).await;
    let (a, b) = (owner("a"), owner("b"));
    let take = |feed: &FeedUid, who: &ClaimOwner, ttl: Duration| {
        let (store, calls, feed, who) =
            (f.store.clone(), f.calls.clone(), feed.clone(), who.clone());
        async move {
            store
                .maintenance(&calls, &feed, &who, ttl)
                .await
                .expect("maintenance")
        }
    };
    assert!(take(&feed, &a, SHORT).await);
    assert!(!take(&feed, &b, TTL).await, "one holder at a time");
    assert!(take(&feed, &a, SHORT).await, "the holder may renew");
    assert!(take(&other, &b, TTL).await, "the claim is per feed");
    f.wait(PAST_SHORT).await;
    assert!(take(&feed, &b, TTL).await, "the claim ends with its TTL");
    assert!(!take(&feed, &a, TTL).await);

    let mut racers = Vec::new();
    let raced = f.feed(StoreLimits::new()).await;
    for i in 0..8 {
        let (store, raced) = (f.store.clone(), raced.clone());
        racers.push(tokio::spawn(async move {
            store
                .maintenance(&CallContext::new(), &raced, &owner(&format!("r{i}")), TTL)
                .await
                .expect("maintenance")
        }));
    }
    let mut winners = 0;
    for racer in racers {
        winners += usize::from(racer.await.expect("a racer finished"));
    }
    assert_eq!(winners, 1, "exactly one racer takes the claim");

    assert!(matches!(
        f.store
            .maintenance(&f.calls, &FeedUid::generate(), &a, TTL)
            .await,
        Err(FeedError::FeedNotFound(_))
    ));
}
