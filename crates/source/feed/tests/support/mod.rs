//! Shared fixtures: a SQLite feed store in a temp dir and a recording sink.
#![allow(dead_code)]

use faucet_common_feed::{
    ClaimOwner, DeliveryUid, Epoch, FeedBinding, FeedExecution, FeedStore, FeedUid, NewEvent, Seq,
    SqliteFeedStore, SqliteOptions, Start, StoreLimits,
};
use faucet_core::io::CallContext;
use faucet_core::{FaucetError, Sink, Value, async_trait};
use faucet_source_feed::{FeedSource, FeedSourceConfig, FeedStoreSpec};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub const FEED: &str = "orders";
pub const DELIVERY: &str = "warehouse";
pub const STATE_KEY: &str = "feed:orders:warehouse";

pub struct Fixture {
    _dir: tempfile::TempDir,
    pub path: String,
    pub store: Arc<dyn FeedStore>,
    pub feed: FeedUid,
    pub delivery: DeliveryUid,
    pub ids: Vec<String>,
}

pub fn ctx() -> CallContext {
    CallContext::new()
}

impl Fixture {
    /// A store with the feed, its delivery at the first event, and `events`
    /// JSON events `{"n": 1}`, `{"n": 2}`, ….
    pub async fn with_events(events: usize) -> Self {
        let mut fixture = Self::empty().await;
        let batch = (1..=events)
            .map(|n| NewEvent::new("application/json", format!(r#"{{"n": {n}}}"#)))
            .collect();
        fixture.append(batch).await;
        fixture
    }

    pub async fn empty() -> Self {
        let dir = tempfile::tempdir().expect("a temp dir");
        let path = dir.path().join("feeds.db");
        let store: Arc<dyn FeedStore> = Arc::new(
            SqliteFeedStore::open(&path, &SqliteOptions::new())
                .await
                .expect("open the store"),
        );
        let feed = FeedUid::new(FEED).unwrap();
        let delivery = DeliveryUid::new(feed.clone(), DELIVERY).unwrap();
        store
            .ensure_feed(&ctx(), &feed, None, &StoreLimits::new())
            .await
            .unwrap();
        store
            .ensure_delivery(&ctx(), &delivery, Start::Earliest)
            .await
            .unwrap();
        Self {
            _dir: dir,
            path: path.to_string_lossy().into_owned(),
            store,
            feed,
            delivery,
            ids: Vec::new(),
        }
    }

    pub async fn append(&mut self, events: Vec<NewEvent>) {
        let ack = self
            .store
            .append(&ctx(), &self.feed, events)
            .await
            .expect("append");
        self.ids.extend(ack.events.into_iter().map(|e| e.id));
    }

    /// The id of event `seq`.
    pub fn id(&self, seq: u64) -> &str {
        &self.ids[seq as usize - 1]
    }

    pub fn config(&self) -> FeedSourceConfig {
        FeedSourceConfig::standalone(FEED, DELIVERY, FeedStoreSpec::sqlite(self.path.clone()))
    }

    /// A `faucet run`-shaped source that opens the store file itself.
    pub fn source(&self) -> FeedSource {
        FeedSource::new(self.config())
    }

    pub async fn cursor(&self) -> u64 {
        self.position().await.0
    }

    /// `(cursor, epoch, claim owner)`.
    pub async fn position(&self) -> (u64, u64, Option<String>) {
        let heads = self
            .store
            .heads(&ctx(), std::slice::from_ref(&self.feed))
            .await
            .unwrap();
        let delivery = heads[0]
            .deliveries
            .iter()
            .find(|d| d.delivery == self.delivery)
            .expect("the delivery");
        (
            delivery.cursor.get(),
            delivery.epoch.0,
            delivery.claim.as_ref().map(|c| c.owner.to_string()),
        )
    }

    /// Wait until no claim is live (a failed read releases its claim on a
    /// spawned task).
    pub async fn released(&self) {
        for _ in 0..200 {
            if self.position().await.2.is_none() {
                return;
            }
            ctx().wait(Duration::from_millis(10)).await.unwrap();
        }
        panic!("the claim was never released");
    }

    /// Claim `[cursor, to)` the way a server's scheduler does and return the
    /// handle a bound source reads through.
    pub async fn bind(&self, to: u64) -> FeedExecution {
        let (cursor, epoch, _) = self.position().await;
        let claim = self
            .store
            .claim(
                &ctx(),
                &self.delivery,
                Seq(cursor),
                Seq(to),
                &ClaimOwner::new("serve:test").unwrap(),
                Duration::from_secs(60),
            )
            .await
            .unwrap()
            .expect("the claim");
        let binding = FeedBinding::new(
            self.delivery.clone(),
            Epoch(epoch),
            claim.id,
            Seq(cursor),
            Seq(to),
            "default",
        );
        FeedExecution::new(binding, Arc::clone(&self.store))
    }

    pub fn bookmark(&self, next: u64, epoch: u64) -> Value {
        serde_json::json!({
            "feed": FEED,
            "delivery": DELIVERY,
            "epoch": epoch,
            "next": next,
            "id": self.id(next - 1),
        })
    }
}

type Hook = Box<dyn Fn(usize) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// A sink that keeps what it was given and runs a hook inside each write
/// (given the 0-based write number).
#[derive(Default)]
pub struct RecordingSink {
    pub rows: Mutex<Vec<Value>>,
    pub writes: Mutex<Vec<usize>>,
    hook: Option<Hook>,
}

impl RecordingSink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn on_write<F, Fut>(mut self, hook: F) -> Self
    where
        F: Fn(usize) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.hook = Some(Box::new(move |n| Box::pin(hook(n))));
        self
    }

    pub fn rows(&self) -> Vec<Value> {
        self.rows.lock().unwrap().clone()
    }

    pub fn writes(&self) -> Vec<usize> {
        self.writes.lock().unwrap().clone()
    }

    /// The `n` field of every row written, in order.
    pub fn ns(&self) -> Vec<u64> {
        self.rows()
            .iter()
            .map(|r| r["n"].as_u64().expect("an n"))
            .collect()
    }
}

#[async_trait]
impl Sink for RecordingSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        let nth = self.writes.lock().unwrap().len();
        if let Some(hook) = &self.hook {
            hook(nth).await;
        }
        self.rows.lock().unwrap().extend(records.iter().cloned());
        self.writes.lock().unwrap().push(records.len());
        Ok(records.len())
    }
}
