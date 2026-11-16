//! Feed source stream executor.

use crate::bookmark::{FeedBookmark, Resume, resume};
use crate::config::{DEFAULT_MAX_BYTES, DEFAULT_MAX_EVENTS, FeedSourceConfig, FeedStoreSpec};
use crate::decode;
use crate::error::FeedSourceError;
use async_trait::async_trait;
use faucet_common_feed::{
    BatchLimits, ClaimOwner, DeliveryUid, FeedError, FeedExecution, FeedStore, FeedUid, Holder,
    PayloadOpener, Seq, StoredEvent, TenantId, sealing_context,
};
use faucet_core::check::{CheckContext, CheckReport, Probe};
use faucet_core::driver::{DriverSource, PageStream, SourceDriver};
use faucet_core::io::CallContext;
use faucet_core::{CallSlot, CancellationToken, Capabilities, FaucetError, StreamPage};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const RELEASE_TIMEOUT: Duration = Duration::from_secs(10);
const MIN_RENEW_INTERVAL: Duration = Duration::from_millis(50);

/// Reads one delivery's events from a feed store, in order, and moves the
/// delivery's cursor only after the pipeline wrote them.
///
/// Under a [`FeedExecution`] scoped over the run's task (a delivery run on a
/// server) the store, the delivery and the claimed range come from that
/// handle. Otherwise (`faucet run`, or a library caller) the config names the
/// feed, the delivery and the store, and the source takes its own claim.
pub struct FeedSource {
    inner: DriverSource<FeedDriver>,
}

/// The connector's I/O primitives (RFC 0014).
pub(crate) struct FeedDriver {
    config: FeedSourceConfig,
    bookmark: Mutex<Option<Value>>,
    cancel: Mutex<Option<CancellationToken>>,
    given: Option<Arc<dyn FeedStore>>,
    opened: tokio::sync::OnceCell<Arc<dyn FeedStore>>,
}

impl FeedSource {
    /// A feed source for `config`. Nothing is opened until the first read.
    pub fn new(config: FeedSourceConfig) -> Self {
        Self::build(config, None)
    }

    /// A feed source that reads `config.feed` / `config.delivery` from
    /// `store` instead of opening `config.store`.
    pub fn with_store(config: FeedSourceConfig, store: Arc<dyn FeedStore>) -> Self {
        Self::build(config, Some(store))
    }

    fn build(config: FeedSourceConfig, given: Option<Arc<dyn FeedStore>>) -> Self {
        Self {
            inner: DriverSource::new(FeedDriver {
                config,
                bookmark: Mutex::new(None),
                cancel: Mutex::new(None),
                given,
                opened: tokio::sync::OnceCell::new(),
            }),
        }
    }

    /// The source's configuration.
    pub fn config(&self) -> &FeedSourceConfig {
        &self.inner.driver().config
    }
}

#[async_trait]
impl faucet_core::Source for FeedSource {
    faucet_core::forward_source!(inner);
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// What a read works against.
struct Target {
    store: Arc<dyn FeedStore>,
    delivery: DeliveryUid,
    opener: Option<PayloadOpener>,
    bound: Option<FeedExecution>,
}

impl FeedDriver {
    fn configured(&self) -> Vec<&'static str> {
        [
            ("feed", self.config.feed.is_some()),
            ("delivery", self.config.delivery.is_some()),
            ("store", self.config.store.is_some()),
        ]
        .into_iter()
        .filter_map(|(name, set)| set.then_some(name))
        .collect()
    }

    fn standalone_delivery(&self) -> Result<DeliveryUid, FaucetError> {
        let mut missing = Vec::new();
        if self.config.feed.is_none() {
            missing.push("feed");
        }
        if self.config.delivery.is_none() {
            missing.push("delivery");
        }
        if self.config.store.is_none() && self.given.is_none() {
            missing.push("store");
        }
        match (&self.config.feed, &self.config.delivery) {
            (Some(feed), Some(delivery)) if missing.is_empty() => {
                let feed = FeedUid::new(feed.clone()).map_err(config_error)?;
                DeliveryUid::new(feed, delivery.clone()).map_err(config_error)
            }
            _ => Err(FaucetError::Config(format!(
                "feed source: `feed`, `delivery` and `store` are required when the source runs \
                 outside a server delivery (missing: {})",
                missing.join(", ")
            ))),
        }
    }

    async fn target(&self) -> Result<Target, FaucetError> {
        self.config.validate()?;
        if let Some(execution) = faucet_common_feed::current() {
            let set = self.configured();
            if !set.is_empty() || self.given.is_some() {
                return Err(FaucetError::Config(format!(
                    "feed source: a delivery run reads the feed, delivery and store it was \
                     started for; remove {} from the source config",
                    if set.is_empty() {
                        "the injected store".to_string()
                    } else {
                        set.iter()
                            .map(|n| format!("`{n}`"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    }
                )));
            }
            return Ok(Target {
                store: Arc::clone(&execution.store),
                delivery: execution.binding.delivery.clone(),
                opener: execution.opener.clone(),
                bound: Some(execution),
            });
        }
        let delivery = self.standalone_delivery()?;
        Ok(Target {
            store: self.store().await?,
            delivery,
            opener: None,
            bound: None,
        })
    }

    async fn store(&self) -> Result<Arc<dyn FeedStore>, FaucetError> {
        if let Some(store) = &self.given {
            return Ok(Arc::clone(store));
        }
        let spec = self.config.store.as_ref().ok_or_else(|| {
            FaucetError::Config("feed source: `store` is not configured".to_string())
        })?;
        self.opened
            .get_or_try_init(|| open_store(spec))
            .await
            .map(Arc::clone)
    }

    async fn start(&self, calls: &CallSlot) -> Result<Option<Run>, FaucetError> {
        let ctx = calls.get();
        let target = self.target().await?;
        let cancel = lock(&self.cancel).take();
        let stored = lock(&self.bookmark).clone();
        let feed = target.delivery.feed().clone();
        let head = target
            .store
            .heads(&ctx, std::slice::from_ref(&feed))
            .await?
            .into_iter()
            .find(|h| h.feed == feed)
            .ok_or_else(|| FeedSourceError::Missing {
                what: format!("feed {feed}"),
            })?;
        let ttl = match &target.bound {
            Some(_) => self.config.claim_ttl(),
            None => self.config.standalone_claim_ttl(),
        };
        let holder = match &target.bound {
            Some(execution) => {
                let holder = target
                    .store
                    .attach(&ctx, &execution.binding.claim)
                    .await
                    .map_err(|e| lost_or(e, &target.delivery))?;
                if holder.delivery != target.delivery {
                    return Err(FaucetError::Config(format!(
                        "feed source: claim {} belongs to delivery {}, not {}",
                        execution.binding.claim, holder.delivery, target.delivery
                    )));
                }
                holder
            }
            None => {
                let position = head
                    .deliveries
                    .iter()
                    .find(|d| d.delivery == target.delivery)
                    .ok_or_else(|| FeedSourceError::Missing {
                        what: format!("delivery {} of feed {feed}", target.delivery.id()),
                    })?;
                if let Some(claim) = &position.claim {
                    return Err(FeedSourceError::ClaimBusy {
                        delivery: target.delivery.to_string(),
                        owner: claim.owner.to_string(),
                    }
                    .into());
                }
                if position.cursor >= head.end() {
                    return match resume(
                        stored.as_ref(),
                        &target.delivery,
                        position.epoch,
                        position.cursor,
                    )? {
                        Resume::FromCursor => Ok(None),
                        Resume::Ahead { next, id } => Err(FeedSourceError::Held {
                            delivery: target.delivery.to_string(),
                            seq: next.get().saturating_sub(1),
                            id,
                            reason: format!("ends at event {}", head.watermark),
                        }
                        .into()),
                    };
                }
                let limits = BatchLimits::new(
                    self.config.max_events.unwrap_or(DEFAULT_MAX_EVENTS),
                    self.config.max_bytes.unwrap_or(DEFAULT_MAX_BYTES),
                );
                let to = target
                    .store
                    .bound(&ctx, &feed, position.cursor, limits)
                    .await?;
                if to <= position.cursor {
                    return Ok(None);
                }
                let claim = target
                    .store
                    .claim(
                        &ctx,
                        &target.delivery,
                        position.cursor,
                        to,
                        &cli_owner()?,
                        ttl,
                    )
                    .await?
                    .ok_or_else(|| FeedSourceError::ClaimBusy {
                        delivery: target.delivery.to_string(),
                        owner: "another reader".to_string(),
                    })?;
                match target.store.attach(&ctx, &claim.id).await {
                    Ok(holder) => holder,
                    Err(e) => {
                        release(&target.store, &claim.id).await;
                        return Err(lost_or(e, &target.delivery));
                    }
                }
            }
        };
        let mut run = Run::new(
            target,
            ctx,
            holder,
            head.tenant,
            cancel,
            ttl,
            self.config.page_events,
            self.config.include_metadata,
        );
        match run.reconcile(stored.as_ref()).await {
            Ok(true) => Ok(Some(run)),
            Ok(false) => {
                run.finish().await;
                Ok(None)
            }
            Err(e) => {
                run.finish().await;
                Err(e)
            }
        }
    }
}

fn config_error(err: FeedError) -> FaucetError {
    FaucetError::Config(format!("feed source: {err}"))
}

fn lost_or(err: FeedError, delivery: &DeliveryUid) -> FaucetError {
    match err {
        FeedError::ClaimLost { .. } => FeedSourceError::ClaimLost {
            delivery: delivery.to_string(),
        }
        .into(),
        other => other.into(),
    }
}

fn cli_owner() -> Result<ClaimOwner, FaucetError> {
    let host: String = std::env::var("HOSTNAME")
        .unwrap_or_default()
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
        .take(64)
        .collect();
    let host = if host.is_empty() { "local" } else { &host };
    ClaimOwner::new(format!("cli:{host}:{}", std::process::id())).map_err(config_error)
}

async fn open_store(spec: &FeedStoreSpec) -> Result<Arc<dyn FeedStore>, FaucetError> {
    match spec {
        FeedStoreSpec::Sqlite(cfg) => open_sqlite(&cfg.path).await,
        FeedStoreSpec::Postgres(cfg) => open_postgres(&cfg.url).await,
    }
}

#[cfg(feature = "sqlite")]
async fn open_sqlite(path: &str) -> Result<Arc<dyn FeedStore>, FaucetError> {
    let file = std::path::Path::new(path);
    faucet_core::path_guard::check(file)?;
    if !file.is_file() {
        return Err(FaucetError::Config(format!(
            "feed source: there is no feed store at '{path}'; create the feed with `faucet feed \
             create`"
        )));
    }
    let store =
        faucet_common_feed::SqliteFeedStore::open(file, &faucet_common_feed::SqliteOptions::new())
            .await?;
    Ok(Arc::new(store))
}

#[cfg(not(feature = "sqlite"))]
async fn open_sqlite(_path: &str) -> Result<Arc<dyn FeedStore>, FaucetError> {
    Err(FaucetError::Config(
        "feed source: this build has no SQLite feed store (crate feature `sqlite`)".to_string(),
    ))
}

#[cfg(feature = "postgres")]
async fn open_postgres(url: &str) -> Result<Arc<dyn FeedStore>, FaucetError> {
    let options = faucet_common_feed::PostgresOptions::new().max_connections(2);
    let store = faucet_common_feed::PostgresFeedStore::connect(url, &options).await?;
    Ok(Arc::new(store))
}

#[cfg(not(feature = "postgres"))]
async fn open_postgres(_url: &str) -> Result<Arc<dyn FeedStore>, FaucetError> {
    Err(FaucetError::Config(
        "feed source: this build has no PostgreSQL feed store (crate feature `postgres`)"
            .to_string(),
    ))
}

async fn release(store: &Arc<dyn FeedStore>, claim: &faucet_common_feed::ClaimId) {
    let ctx = CallContext::new().with_timeout(RELEASE_TIMEOUT);
    if let Err(e) = store.release(&ctx, claim).await {
        tracing::warn!(claim = %claim, error = %e, "feed: releasing the claim failed; it ends by its TTL");
    }
}

/// One read: a held claim, the events not yet emitted, and the page waiting
/// for its commit.
struct Run {
    store: Arc<dyn FeedStore>,
    ctx: CallContext,
    holder: Holder,
    tenant: Option<TenantId>,
    opener: Option<PayloadOpener>,
    cancel: Option<CancellationToken>,
    cursor: Seq,
    pending: Option<Seq>,
    events: Option<VecDeque<StoredEvent>>,
    page_events: Option<u64>,
    include_metadata: bool,
    owns_claim: bool,
    released: bool,
    lost: Arc<AtomicBool>,
    renewal: tokio::task::JoinHandle<()>,
}

impl Run {
    #[allow(clippy::too_many_arguments)]
    fn new(
        target: Target,
        ctx: CallContext,
        holder: Holder,
        tenant: Option<TenantId>,
        cancel: Option<CancellationToken>,
        ttl: Duration,
        page_events: Option<u64>,
        include_metadata: bool,
    ) -> Self {
        let lost = Arc::new(AtomicBool::new(false));
        let renewal = tokio::spawn(faucet_common_feed::inherit(renew(
            Arc::clone(&target.store),
            ctx.clone(),
            holder.clone(),
            ttl,
            Arc::clone(&lost),
        )));
        Self {
            store: target.store,
            ctx,
            cursor: holder.cursor,
            holder,
            tenant,
            opener: target.opener,
            cancel,
            pending: None,
            events: None,
            page_events,
            include_metadata,
            owns_claim: target.bound.is_none(),
            released: false,
            lost,
            renewal,
        }
    }

    fn delivery(&self) -> &DeliveryUid {
        &self.holder.delivery
    }

    fn feed(&self) -> &FeedUid {
        self.holder.delivery.feed()
    }

    fn claim_lost(&self) -> FaucetError {
        FeedSourceError::ClaimLost {
            delivery: self.delivery().to_string(),
        }
        .into()
    }

    async fn commit(&mut self, new: Seq) -> Result<(), FaucetError> {
        if self.lost.load(Ordering::SeqCst) {
            return Err(self.claim_lost());
        }
        self.store
            .commit(&self.ctx, &self.holder, self.cursor, new)
            .await
            .map_err(|e| lost_or(e, self.delivery()))?;
        self.cursor = new;
        Ok(())
    }

    /// Apply the resume rules. `false` when nothing is left to read.
    async fn reconcile(&mut self, stored: Option<&Value>) -> Result<bool, FaucetError> {
        if let Resume::Ahead { next, id } =
            resume(stored, self.delivery(), self.holder.epoch, self.cursor)?
        {
            self.prove(next, &id).await?;
            self.commit(next).await?;
            tracing::info!(
                delivery = %self.delivery(),
                cursor = %next,
                "feed: the destination already holds a page; moved the cursor without reading it again"
            );
        }
        Ok(self.cursor < self.holder.to)
    }

    async fn prove(&self, next: Seq, id: &str) -> Result<(), FaucetError> {
        let seq = Seq(next.get().saturating_sub(1));
        let held = |reason: String| -> FaucetError {
            FeedSourceError::Held {
                delivery: self.delivery().to_string(),
                seq: seq.get(),
                id: id.to_string(),
                reason,
            }
            .into()
        };
        match self.store.read(&self.ctx, self.feed(), seq, next).await {
            Ok(batch) => match batch.events.first() {
                Some(event) if event.seq == seq && event.id == id => Ok(()),
                Some(event) => Err(held(format!(
                    "holds another event at that position ({})",
                    event.id
                ))),
                None => Err(held("ends before it".to_string())),
            },
            Err(
                e @ (FeedError::PastWatermark { .. }
                | FeedError::RangeUnavailable { .. }
                | FeedError::OutOfRange { .. }),
            ) => Err(held(format!("does not hold it ({e})"))),
            Err(e) => Err(e.into()),
        }
    }

    fn cancelled(&self) -> bool {
        self.cancel.as_ref().is_some_and(|c| c.is_cancelled())
    }

    /// Commit the page the pipeline just wrote, then hand out the next one.
    async fn next_page(&mut self) -> Result<Option<StreamPage>, FaucetError> {
        if let Some(next) = self.pending.take() {
            self.commit(next).await?;
        }
        if self.lost.load(Ordering::SeqCst) {
            return Err(self.claim_lost());
        }
        if self.cancelled() || self.cursor >= self.holder.to {
            return Ok(None);
        }
        if self.events.is_none() {
            let batch = self
                .store
                .read(&self.ctx, self.feed(), self.cursor, self.holder.to)
                .await?;
            self.events = Some(batch.events.into());
        }
        let Some(events) = self.events.as_mut() else {
            return Ok(None);
        };
        let take = self
            .page_events
            .map_or(usize::MAX, |n| usize::try_from(n).unwrap_or(usize::MAX))
            .min(events.len());
        if take == 0 {
            return Ok(None);
        }
        let page: Vec<StoredEvent> = events.drain(..take).collect();
        let Some(last) = page.last() else {
            return Ok(None);
        };
        let bookmark =
            FeedBookmark::after(self.delivery(), self.holder.epoch, last.seq, &last.id).to_value();
        let next = last.seq.next();
        let mut records = Vec::with_capacity(page.len());
        for event in &page {
            records.push(self.record(event)?);
        }
        self.pending = Some(next);
        Ok(Some(StreamPage {
            records,
            bookmark: Some(bookmark),
        }))
    }

    fn record(&self, event: &StoredEvent) -> Result<Value, FaucetError> {
        let sealed = |reason: String| -> FaucetError {
            FeedSourceError::Sealed {
                feed: self.feed().to_string(),
                seq: event.seq.get(),
                id: event.id.clone(),
                reason,
            }
            .into()
        };
        let record = if event.sealed {
            let opener = self
                .opener
                .as_ref()
                .ok_or_else(|| sealed("this run has no key to open sealed payloads".to_string()))?;
            let context = sealing_context(self.feed(), self.tenant.as_ref());
            let plain = opener(&event.payload, &context).map_err(|e| sealed(e.to_string()))?;
            decode::record(self.feed(), event, &plain, self.include_metadata)
        } else {
            decode::record(self.feed(), event, &event.payload, self.include_metadata)
        };
        record.map_err(FaucetError::from)
    }

    async fn finish(&mut self) {
        self.renewal.abort();
        if self.owns_claim && !self.released {
            self.released = true;
            release(&self.store, &self.holder.claim).await;
        }
    }
}

impl Drop for Run {
    fn drop(&mut self) {
        self.renewal.abort();
        if self.owns_claim
            && !self.released
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            let store = Arc::clone(&self.store);
            let claim = self.holder.claim.clone();
            runtime.spawn(async move { release(&store, &claim).await });
        }
    }
}

async fn renew(
    store: Arc<dyn FeedStore>,
    ctx: CallContext,
    holder: Holder,
    ttl: Duration,
    lost: Arc<AtomicBool>,
) {
    let interval = (ttl / 10).max(MIN_RENEW_INTERVAL);
    loop {
        if ctx.wait(interval).await.is_err() {
            return;
        }
        match store.renew(&ctx, &holder, ttl).await {
            Ok(true) => {}
            Ok(false) => {
                lost.store(true, Ordering::SeqCst);
                tracing::warn!(delivery = %holder.delivery, claim = %holder.claim, "feed: the range claim is no longer held");
                return;
            }
            Err(e) => {
                tracing::warn!(delivery = %holder.delivery, error = %e, "feed: renewing the range claim failed; retrying");
            }
        }
    }
}

#[async_trait]
impl SourceDriver for FeedDriver {
    fn connector_name(&self) -> &'static str {
        "feed"
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(FeedSourceConfig)).unwrap_or(Value::Null)
    }

    fn dataset_uri(&self) -> String {
        let name = faucet_common_feed::current()
            .map(|e| e.dataset.unwrap_or_else(|| e.binding.feed.to_string()))
            .or_else(|| self.config.feed.clone())
            .unwrap_or_else(|| "unbound".to_string());
        format!("feed://{name}")
    }

    fn state_key(&self) -> Option<String> {
        let (feed, delivery) = match faucet_common_feed::current() {
            Some(e) => (
                e.binding.feed.to_string(),
                e.binding.delivery.id().to_string(),
            ),
            None => (self.config.feed.clone()?, self.config.delivery.clone()?),
        };
        Some(format!("feed:{feed}:{delivery}").replace('/', "."))
    }

    /// Only stores the bookmark; the first read checks it against the store.
    async fn resume_from(&self, bookmark: Value) -> Result<(), FaucetError> {
        *lock(&self.bookmark) = Some(bookmark);
        Ok(())
    }

    fn deterministic_replay(&self) -> bool {
        true
    }

    /// A read moves the delivery's cursor as the pipeline writes each page.
    fn consumes_destructively(&self) -> bool {
        true
    }

    /// On cancel the source commits what was written and ends at the next
    /// poll.
    fn set_cancel(&self, cancel: CancellationToken) -> bool {
        *lock(&self.cancel) = Some(cancel);
        true
    }

    fn capabilities(&self, derived: Capabilities) -> Capabilities {
        derived.to_builder().read_side_effects(true).build()
    }

    /// Each page carries a bookmark, so the pipeline writes, flushes and
    /// stores it before polling again; that next poll commits the cursor.
    fn read<'a>(
        &'a self,
        calls: &'a CallSlot,
        _context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> PageStream<'a> {
        Box::pin(faucet_core::async_stream::stream! {
            let mut run = match self.start(calls).await {
                Ok(Some(run)) => run,
                Ok(None) => return,
                Err(e) => {
                    yield Err(e);
                    return;
                }
            };
            loop {
                match run.next_page().await {
                    Ok(Some(page)) => yield Ok(page),
                    Ok(None) => break,
                    Err(e) => {
                        run.finish().await;
                        yield Err(e);
                        return;
                    }
                }
            }
            run.finish().await;
        })
    }

    async fn fetch(
        &self,
        _calls: &CallSlot,
        _context: &HashMap<String, Value>,
    ) -> Option<Result<(Vec<Value>, Option<Value>), FaucetError>> {
        Some(Err(FeedSourceError::BufferedFetch.into()))
    }

    /// Looks the feed up in the store; reads and claims nothing.
    async fn check(
        &self,
        calls: &CallSlot,
        _ctx: &CheckContext,
    ) -> Result<Option<CheckReport>, FaucetError> {
        let start = std::time::Instant::now();
        if faucet_common_feed::current().is_none()
            && self.configured().is_empty()
            && self.given.is_none()
        {
            return Ok(Some(CheckReport::single(Probe::skip(
                "store",
                "the source is not bound to a feed; a delivery run supplies it",
            ))));
        }
        let probe = match self.probe(calls).await {
            Ok(()) => Probe::pass("store", start.elapsed()),
            Err(e) => Probe::fail("store", start.elapsed(), e.to_string()),
        };
        Ok(Some(CheckReport::single(probe)))
    }
}

impl FeedDriver {
    async fn probe(&self, calls: &CallSlot) -> Result<(), FaucetError> {
        let target = self.target().await?;
        let feed = target.delivery.feed().clone();
        let heads = target
            .store
            .heads(&calls.get(), std::slice::from_ref(&feed))
            .await?;
        let head =
            heads
                .iter()
                .find(|h| h.feed == feed)
                .ok_or_else(|| FeedSourceError::Missing {
                    what: format!("feed {feed}"),
                })?;
        if !head
            .deliveries
            .iter()
            .any(|d| d.delivery == target.delivery)
        {
            return Err(FeedSourceError::Missing {
                what: format!("delivery {} of feed {feed}", target.delivery.id()),
            }
            .into());
        }
        Ok(())
    }
}
