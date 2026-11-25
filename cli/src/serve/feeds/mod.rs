//! Feeds for `faucet serve` (#1034): durable event logs senders push into,
//! delivered by ordinary runs.
//!
//! Feeds come from two places, merged by tenant and name:
//! - the `--feeds <file>` (read-only at runtime; it wins over a stored feed
//!   of the same name), and
//! - the API / console, stored in the run history so every cluster member
//!   sees the same set.
//!
//! Nothing here runs until a feed exists: the stores, their lock files, the
//! scheduler loop and the definitions refresh start on a server when it
//! first sees one ([`FeedsRuntime::ensure_started`]).
//!
//! [`spec`] holds the definition's rules and the file, [`stores`] the
//! server-defined stores, [`runner`] the scheduler every server runs,
//! [`status`] the shared per-delivery record, [`service`] every rule the API
//! adapters call, [`secrets`] the sealed values and [`metrics`] the series.

pub mod ingest;
pub mod metrics;
pub mod notify;
pub mod runner;
pub mod secrets;
pub mod service;
pub mod spec;
pub mod status;
pub mod stores;

use crate::serve::config::FeedSettings;
use crate::serve::history::feeds::{DeliveryMeta, FeedRecord, FeedSpec, FeedTokenRecord, feed_key};
use crate::serve::history::tenants::TenantLimits;
use crate::serve::state::ServerState;
use faucet_common_feed::{
    ClaimOwner, DeliveryUid, FeedBinding, FeedExecution, FeedStore, FeedUid, Start, StoreLimits,
    TenantId,
};
use faucet_core::io::CallContext;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

/// How often a server reloads the definitions.
pub const REFRESH_INTERVAL: Duration = Duration::from_secs(5);
/// The oldest definitions snapshot ingest may answer from.
pub const MAX_SNAPSHOT_AGE: Duration = Duration::from_secs(60);
/// The shortest gap between two forced refreshes.
pub const REFRESH_NOW_GAP: Duration = Duration::from_secs(1);
/// How long a store call may take.
pub const STORE_CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// A step of removing a deleted feed or delivery. A test stops the removal
/// after one, as a crash there would.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopAfter {
    /// The definition says what is to be removed.
    Marked = 1,
    /// The runs' state keys are gone.
    State = 2,
    /// The store rows are gone.
    Store = 3,
    /// The statuses and the warning count are gone.
    Statuses = 4,
}

/// Where a feed is defined.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    /// Made through the API or the console; stored in the run history.
    Console,
    /// Listed in the `--feeds` file; read-only at runtime.
    File,
}

/// The call context every store call of the feed layer runs under.
pub fn store_calls() -> CallContext {
    CallContext::default().with_timeout(STORE_CALL_TIMEOUT)
}

/// One delivery of a feed as the server runs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryRef {
    pub uid: DeliveryUid,
    pub meta: DeliveryMeta,
}

impl DeliveryRef {
    /// The 12 lowercase hex characters a delivery run's pipeline name ends in.
    pub fn short_id(&self) -> String {
        short_id(self.uid.id())
    }
}

/// The suffix of a delivery run's pipeline name: 12 lowercase hex characters
/// of the delivery's id.
pub fn short_id(delivery_id: &str) -> String {
    let hex: String = delivery_id
        .chars()
        .filter(char::is_ascii_hexdigit)
        .map(|c| c.to_ascii_lowercase())
        .take(12)
        .collect();
    if hex.len() == 12 {
        return hex;
    }
    use sha2::Digest;
    let digest = sha2::Sha256::digest(delivery_id.as_bytes());
    digest.iter().take(6).map(|b| format!("{b:02x}")).collect()
}

/// One feed as the server runs it: its definition with secrets opened, its
/// uid and its deliveries' uids.
#[derive(Debug, Clone, PartialEq)]
pub struct Feed {
    pub uid: FeedUid,
    pub spec: FeedSpec,
    pub source: Source,
    pub deliveries: BTreeMap<String, DeliveryRef>,
    /// The deliveries came through an approved change request.
    pub approved: bool,
    pub revision: u64,
    /// The feed and its deliveries exist in the store.
    pub ensured: bool,
    /// What this server learned about the feed when it loaded it.
    pub derived: Derived,
}

/// What a server learned about a feed when it loaded its definition, beside
/// the definition itself.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Derived {
    /// The sealed signature secret does not open with this server's vault
    /// key; the spec's secret is then empty and ingest refuses every request.
    pub signature_unreadable: Option<String>,
    /// A `--feeds` file feed's strings that held a `${…}` reference: the
    /// resolved value, and the text views show in its place.
    pub references: BTreeMap<String, String>,
    /// A `--feeds` file feed's template params each delivery's template
    /// declares secret (shown as the keep marker); `None` when the template
    /// could not be read, so every param is shown as the marker.
    pub secret_params: BTreeMap<String, Option<Vec<String>>>,
    /// The feed's events are in another physical store than the one its
    /// definition now names: nothing is received, delivered or created
    /// for it until an operator adopts the new store.
    pub store_moved: Option<StoreMoved>,
}

/// How long a reload waits for a feed store to open before checking its
/// feeds on a later pass instead.
const STORE_OPEN_WAIT: Duration = Duration::from_secs(2);

/// The settings key recording which physical store holds a feed's rows.
pub fn store_binding_key(feed_uid: &str) -> String {
    format!("feed_store:{feed_uid}")
}

/// Which physical store holds a feed's rows: the store's name when the
/// binding was written, and its identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreBinding {
    pub store: String,
    pub id: i64,
}

/// A feed whose events are in another store than the one it now names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreMoved {
    /// The store holding its events, by the name it had then, and its identity.
    pub recorded_store: String,
    pub recorded_id: i64,
    /// The store its definition names now, and that store's identity.
    pub now_store: String,
    pub now_id: i64,
}

impl std::fmt::Display for StoreMoved {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "its events are in feed store '{}' (id {}) but its definition now names store '{}', \
             which is a different store (id {})",
            self.recorded_store, self.recorded_id, self.now_store, self.now_id
        )
    }
}

impl Feed {
    pub fn name(&self) -> &str {
        &self.spec.name
    }

    pub fn tenant(&self) -> Option<&str> {
        self.spec.tenant.as_deref()
    }

    pub fn key(&self) -> String {
        feed_key(self.tenant(), self.name())
    }

    /// `<tenant>/<name>` for a tenant feed, else the name: the `feed` metric
    /// label and the dataset name.
    pub fn label(&self) -> String {
        match self.tenant() {
            Some(t) => format!("{t}/{}", self.name()),
            None => self.name().to_string(),
        }
    }

    pub fn delivery(&self, name: &str) -> Option<&DeliveryRef> {
        self.deliveries.get(name)
    }

    /// The delivery with this uid.
    pub fn delivery_by_uid(&self, uid: &DeliveryUid) -> Option<(&str, &DeliveryRef)> {
        self.deliveries
            .iter()
            .find(|(_, d)| d.uid == *uid)
            .map(|(n, d)| (n.as_str(), d))
    }
}

/// The definitions a server works from: every feed, the usable tokens by
/// hash and the tenants' limits.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub feeds: Vec<Arc<Feed>>,
    by_key: HashMap<String, Arc<Feed>>,
    by_uid: HashMap<String, Arc<Feed>>,
    tokens: HashMap<String, FeedTokenRecord>,
    pub tenant_limits: HashMap<String, TenantLimits>,
    /// Stored feeds with a removal to finish: deleted ones, and ones that
    /// dropped a delivery.
    pub unfinished: Vec<FeedRecord>,
    pub loaded_at: Instant,
    loaded: bool,
}

impl Default for Snapshot {
    fn default() -> Self {
        let mut never_read = Self::new(Vec::new(), Vec::new(), HashMap::new());
        never_read.loaded = false;
        never_read
    }
}

impl Snapshot {
    pub fn new(
        feeds: Vec<Arc<Feed>>,
        tokens: Vec<FeedTokenRecord>,
        tenant_limits: HashMap<String, TenantLimits>,
    ) -> Self {
        Self {
            by_key: feeds.iter().map(|f| (f.key(), f.clone())).collect(),
            by_uid: feeds
                .iter()
                .map(|f| (f.uid.as_str().to_string(), f.clone()))
                .collect(),
            tokens: tokens.into_iter().map(|t| (t.hash.clone(), t)).collect(),
            feeds,
            tenant_limits,
            unfinished: Vec::new(),
            loaded_at: Instant::now(),
            loaded: true,
        }
    }

    /// Whether the definitions were ever read; a server that started while
    /// the run history was unreachable holds an empty snapshot that was not.
    pub fn loaded(&self) -> bool {
        self.loaded
    }

    /// The feed named `name` of `tenant`.
    pub fn feed(&self, tenant: Option<&str>, name: &str) -> Option<Arc<Feed>> {
        self.by_key.get(&feed_key(tenant, name)).cloned()
    }

    pub fn by_uid(&self, uid: &str) -> Option<Arc<Feed>> {
        self.by_uid.get(uid).cloned()
    }

    /// The token whose SHA-256 (lowercase hex) is `hash`.
    pub fn token(&self, hash: &str) -> Option<&FeedTokenRecord> {
        self.tokens.get(hash)
    }

    pub fn age(&self) -> Duration {
        self.loaded_at.elapsed()
    }

    /// Whether ingest may still answer from this snapshot.
    pub fn fresh(&self) -> bool {
        self.age() <= MAX_SNAPSHOT_AGE
    }
}

/// The retained bytes below a feed's lowest delivery position (delivered,
/// kept for replay) as the scheduler last counted them. They stay the same
/// while that position and the oldest retained event do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeedBytes {
    pub lowest: faucet_common_feed::Seq,
    pub oldest: faucet_common_feed::Seq,
    pub delivered: u64,
}

/// How long a published [`FeedBytes`] answers for the feed.
const BYTES_MAX_AGE: Duration = Duration::from_secs(60);

/// What `/readyz` reports about feeds.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Readiness {
    pub started: bool,
    pub store_ok: bool,
    pub feeds: usize,
    pub stalled: Vec<String>,
    pub held: Vec<String>,
    /// Feeds whose signature secret this server cannot open: they refuse
    /// every ingest request with `503`.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub secret_unreadable: Vec<String>,
    /// Feeds whose events are in another feed store than the one they name:
    /// they receive and deliver nothing.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub store_moved: Vec<String>,
}

/// The ids of a `--feeds` file feed, kept in the run history's settings so
/// they survive restarts and every server uses the same ones.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileIds {
    pub uid: String,
    #[serde(default)]
    pub deliveries: BTreeMap<String, String>,
}

/// The settings key of a file feed's ids.
pub fn file_ids_key(key: &str) -> String {
    format!("feed_uid:{key}")
}

/// The store limits a feed's definition asks for.
pub fn store_limits(spec: &FeedSpec, tenant: Option<&TenantLimits>) -> StoreLimits {
    let mut limits = StoreLimits::new()
        .max_age(spec.retention.max_age.duration())
        .undelivered_max_age(spec.retention.undelivered.max_age.duration())
        .undelivered_max_bytes(spec.retention.undelivered.max_bytes.bytes());
    if let Some(d) = &spec.ingest.dedupe {
        limits = limits.dedupe_window(d.window.duration());
    }
    if let Some(window) = replay_window(spec) {
        let longer = limits.dedupe_window.max(window);
        limits = limits.dedupe_window(longer);
    }
    if let Some(bytes) = tenant.and_then(|t| t.max_feed_bytes) {
        limits = limits.tenant_max_bytes(bytes);
    }
    limits
}

/// How long the store remembers a signed request so a replay is not stored
/// again: as long as its timestamp can still be accepted. `None` when the
/// feed's signature carries no timestamp.
pub fn replay_window(spec: &FeedSpec) -> Option<Duration> {
    let sig = spec.ingest.auth.signature.as_ref()?;
    sig.timestamp_header.as_ref()?;
    Some(Duration::from_secs(
        sig.tolerance_secs.saturating_mul(2).saturating_add(60),
    ))
}

/// What a reader of `spec` should know before relying on it.
pub fn warnings(spec: &FeedSpec) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(sig) = &spec.ingest.auth.signature
        && sig.timestamp_header.is_none()
        && spec.ingest.dedupe.is_none()
    {
        out.push(
            "the signature has no timestamp_header, so a captured signed request can be sent \
             again at any time and is stored again; set timestamp_header (replays inside the \
             tolerance are then stored once) or a dedupe key"
                .to_string(),
        );
    }
    out
}

/// Where a new delivery starts in the store.
pub fn start_of(start: crate::serve::history::feeds::StartSpec) -> Start {
    use crate::serve::history::feeds::StartSpec;
    match start {
        StartSpec::Earliest => Start::Earliest,
        StartSpec::Latest => Start::Latest,
        StartSpec::Seq(n) => Start::Seq(faucet_common_feed::Seq(n)),
        StartSpec::Time(t) => Start::Time(t.into()),
    }
}

/// The feeds runtime a server carries.
pub struct FeedsRuntime {
    settings: FeedSettings,
    file: spec::LoadedFile,
    stores: stores::Stores,
    ingest: ingest::Ingest,
    owner: ClaimOwner,
    snapshot: RwLock<Arc<Snapshot>>,
    file_ids: Mutex<HashMap<String, FileIds>>,
    ensured: Mutex<HashSet<String>>,
    started: AtomicBool,
    scheduling: AtomicBool,
    stop_after: AtomicU8,
    changed: Arc<tokio::sync::Notify>,
    last_refresh: tokio::sync::Mutex<Option<Instant>>,
    refreshing: tokio::sync::Mutex<()>,
    readiness: Mutex<Readiness>,
    bytes: Mutex<HashMap<String, (FeedBytes, Instant)>>,
    guarantees: Mutex<HashMap<String, (String, Instant)>>,
    /// Feeds whose store binding matched, by uid: the store identity seen.
    bound: Mutex<HashMap<String, i64>>,
    /// Feeds whose events are in another store, by uid.
    moved: Mutex<HashMap<String, StoreMoved>>,
    /// Stores already reported as keeping no identity.
    unidentified: Mutex<HashSet<String>>,
}

impl std::fmt::Debug for FeedsRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FeedsRuntime")
            .field("file", &self.file.feeds.len())
            .field("stores", &self.stores)
            .field("started", &self.started())
            .finish()
    }
}

impl FeedsRuntime {
    pub fn new(settings: FeedSettings, file: spec::LoadedFile, stores: stores::Stores) -> Self {
        let owner = ClaimOwner::new(format!("serve:{}", uuid::Uuid::new_v4().simple()))
            .expect("`serve:` and 32 hex characters form a valid claim owner");
        Self {
            ingest: ingest::Ingest::new(&settings),
            settings,
            file,
            stores,
            owner,
            snapshot: RwLock::new(Arc::new(Snapshot::default())),
            file_ids: Mutex::new(HashMap::new()),
            ensured: Mutex::new(HashSet::new()),
            started: AtomicBool::new(false),
            scheduling: AtomicBool::new(true),
            stop_after: AtomicU8::new(0),
            changed: Arc::new(tokio::sync::Notify::new()),
            last_refresh: tokio::sync::Mutex::new(None),
            refreshing: tokio::sync::Mutex::new(()),
            readiness: Mutex::new(Readiness::default()),
            bytes: Mutex::new(HashMap::new()),
            guarantees: Mutex::new(HashMap::new()),
            bound: Mutex::new(HashMap::new()),
            moved: Mutex::new(HashMap::new()),
            unidentified: Mutex::new(HashSet::new()),
        }
    }

    /// The runtime of a server with no `--feeds` file.
    pub fn from_config(config: &crate::serve::config::ServeConfig) -> Self {
        Self::new(
            config.feeds.clone(),
            spec::LoadedFile::default(),
            stores::Stores::from_config(config),
        )
    }

    /// A runtime that never starts the scheduler: the `faucet feed` verbs
    /// change definitions and positions and leave delivering to a server.
    pub fn offline(self) -> Self {
        self.stop_scheduling();
        self
    }

    /// Never start the scheduler on this runtime from here on; one already
    /// running keeps running.
    pub fn stop_scheduling(&self) {
        self.scheduling.store(false, Ordering::Release);
    }

    /// Stop every removal after `step` (`None`: run them through). For
    /// tests of a server that crashes mid-removal.
    #[doc(hidden)]
    pub fn stop_removals_after(&self, step: Option<StopAfter>) {
        self.stop_after
            .store(step.map_or(0, |s| s as u8), Ordering::Release);
    }

    pub(crate) fn stopped(&self, step: StopAfter) -> Result<(), String> {
        if self.stop_after.load(Ordering::Acquire) == step as u8 {
            return Err(format!("stopped after {step:?}"));
        }
        Ok(())
    }

    /// The `--feeds` file's feeds.
    pub fn file(&self) -> &spec::LoadedFile {
        &self.file
    }

    pub fn file_feed(&self, tenant: Option<&str>, name: &str) -> Option<&FeedSpec> {
        self.file
            .feeds
            .iter()
            .find(|f| f.name == name && f.tenant.as_deref() == tenant)
    }

    pub fn stores(&self) -> &stores::Stores {
        &self.stores
    }

    /// What this server keeps for ingest.
    pub fn ingest(&self) -> &ingest::Ingest {
        &self.ingest
    }

    /// The feed settings the server was started with.
    pub fn settings(&self) -> &FeedSettings {
        &self.settings
    }

    /// Who this server's claims belong to: `serve:<instance>`.
    pub fn owner(&self) -> &ClaimOwner {
        &self.owner
    }

    /// The shortest gap between two runs of a tenant feed's delivery.
    pub fn tenant_min_interval(&self) -> Duration {
        self.settings.tenant_delivery_min_interval
    }

    /// Whether the API listener serves the ingest routes too.
    pub fn ingest_on_api(&self) -> bool {
        self.settings.listen.is_none()
    }

    /// The current definitions.
    pub fn snapshot(&self) -> Arc<Snapshot> {
        self.snapshot
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Make the definitions snapshot look `by` older. For tests of what a
    /// server answers from a stale snapshot.
    #[doc(hidden)]
    pub fn backdate_snapshot(&self, by: Duration) {
        let mut held = self
            .snapshot
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut aged = (**held).clone();
        if let Some(earlier) = aged.loaded_at.checked_sub(by) {
            aged.loaded_at = earlier;
        }
        *held = Arc::new(aged);
    }

    /// Drop the definitions, as a server that has not read them yet holds
    /// them. For tests of what such a server answers.
    #[doc(hidden)]
    pub fn forget_snapshot(&self) {
        *self
            .snapshot
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::new(Snapshot::default());
    }

    pub fn started(&self) -> bool {
        self.started.load(Ordering::Acquire)
    }

    /// The signal the scheduler waits on.
    pub fn changed(&self) -> Arc<tokio::sync::Notify> {
        self.changed.clone()
    }

    /// Wake the scheduler: a feed changed on this server.
    pub fn notify(&self) {
        self.changed.notify_one();
    }

    pub fn readiness(&self) -> Readiness {
        let mut r = self
            .readiness
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        r.started = self.started();
        r.secret_unreadable = self
            .snapshot()
            .feeds
            .iter()
            .filter(|f| f.derived.signature_unreadable.is_some())
            .map(|f| f.label())
            .collect();
        r.secret_unreadable.sort();
        r.store_moved = self
            .snapshot()
            .feeds
            .iter()
            .filter(|f| self.moved_of(f.uid.as_str()).is_some())
            .map(|f| f.label())
            .collect();
        r.store_moved.sort();
        r
    }

    pub(crate) fn set_readiness(&self, readiness: Readiness) {
        *self
            .readiness
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = readiness;
    }

    pub(crate) fn publish_bytes(&self, feed_uid: &str, bytes: FeedBytes) {
        let mut held = self
            .bytes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        held.retain(|_, (_, at)| at.elapsed() < BYTES_MAX_AGE);
        held.insert(feed_uid.to_string(), (bytes, Instant::now()));
    }

    /// What the scheduler counted for the feed within the last minute.
    pub fn bytes(&self, feed_uid: &str) -> Option<FeedBytes> {
        self.bytes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(feed_uid)
            .filter(|(_, at)| at.elapsed() < BYTES_MAX_AGE)
            .map(|(b, _)| *b)
    }

    pub(crate) fn remember_guarantee(&self, key: String, word: String) {
        let mut held = self
            .guarantees
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        held.retain(|_, (_, at)| at.elapsed() < BYTES_MAX_AGE);
        held.insert(key, (word, Instant::now()));
    }

    pub(crate) fn guarantee(&self, key: &str) -> Option<String> {
        self.guarantees
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(key)
            .filter(|(_, at)| at.elapsed() < BYTES_MAX_AGE)
            .map(|(w, _)| w.clone())
    }

    /// The store that holds `feed`, opened on first use. Handed out only
    /// once this server has checked that the store is the one holding the
    /// feed's rows ([`check_store`](Self::check_store), done by every reload
    /// and scheduler pass): nothing is read from or written to another
    /// store for it.
    pub async fn store_for(&self, feed: &Feed) -> Result<Arc<dyn FeedStore>, String> {
        if let Some(moved) = self.moved_of(feed.uid.as_str()) {
            return Err(format!("feed '{}': {moved}", feed.label()));
        }
        let store = self.stores.get(&feed.spec.store).await?;
        if let Some(id) = store.store_id()
            && self
                .bound
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(feed.uid.as_str())
                != Some(&id)
        {
            return Err(format!(
                "feed '{}': its store has not been checked yet; retry shortly",
                feed.label()
            ));
        }
        Ok(store)
    }

    /// Why `feed_uid`'s events are not in the store it names, when they are not.
    pub fn moved_of(&self, feed_uid: &str) -> Option<StoreMoved> {
        self.moved
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(feed_uid)
            .cloned()
    }

    fn set_moved(&self, feed: &Feed, moved: Option<StoreMoved>) {
        let mut held = self
            .moved
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match moved {
            Some(m) => {
                if held
                    .insert(feed.uid.as_str().to_string(), m.clone())
                    .is_none()
                {
                    tracing::error!(feed = %feed.label(), "{m}; nothing is received or delivered for it until the store is pointed back or adopted (faucet feed adopt-store)");
                }
                metrics::store_moved(&feed.label(), true);
            }
            None => {
                if held.remove(feed.uid.as_str()).is_some() {
                    metrics::store_moved(&feed.label(), false);
                }
            }
        }
    }

    /// Check that `store` is the physical store holding `feed`'s rows,
    /// recording it the first time. A store without an identity is not
    /// checked (reported once). Nothing creates rows for a feed before this
    /// passes.
    pub async fn check_store(
        &self,
        state: &ServerState,
        feed: &Feed,
        store: &dyn FeedStore,
    ) -> Result<(), EnsureError> {
        let Some(id) = store.store_id() else {
            if self
                .unidentified
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(feed.spec.store.clone())
            {
                tracing::info!(store = %feed.spec.store, "this feed store keeps no identity: a change of the store a feed names cannot be detected");
            }
            return Ok(());
        };
        let uid = feed.uid.as_str();
        if self
            .bound
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(uid)
            == Some(&id)
        {
            self.set_moved(feed, None);
            return Ok(());
        }
        let history = state.history();
        let key = store_binding_key(uid);
        let unavailable = |e: crate::serve::history::HistoryError| EnsureError::Store {
            transient: true,
            message: format!("reading which store holds the feed: {e}"),
        };
        for _ in 0..3 {
            let found = history.meta_get(&key).await.map_err(unavailable)?;
            let recorded = match found.as_deref().filter(|s| !s.is_empty()) {
                None => {
                    let binding = StoreBinding {
                        store: feed.spec.store.clone(),
                        id,
                    };
                    let text = serde_json::to_string(&binding).unwrap_or_default();
                    if !history
                        .meta_compare_and_set(&key, found.as_deref(), &text)
                        .await
                        .map_err(unavailable)?
                    {
                        continue;
                    }
                    binding
                }
                Some(text) => {
                    serde_json::from_str::<StoreBinding>(text).map_err(|e| EnsureError::Store {
                        transient: false,
                        message: format!("the record of the feed's store cannot be read: {e}"),
                    })?
                }
            };
            if recorded.id == id {
                self.bound
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(uid.to_string(), id);
                self.set_moved(feed, None);
                return Ok(());
            }
            let moved = StoreMoved {
                recorded_store: recorded.store,
                recorded_id: recorded.id,
                now_store: feed.spec.store.clone(),
                now_id: id,
            };
            self.set_moved(feed, Some(moved.clone()));
            return Err(EnsureError::Moved(moved));
        }
        Err(EnsureError::Store {
            transient: true,
            message: "the record of the feed's store kept changing".into(),
        })
    }

    /// Record `store` as the one holding `feed`'s rows from now on, whatever
    /// held them before.
    pub(crate) async fn rebind(
        &self,
        state: &ServerState,
        feed: &Feed,
        store: &dyn FeedStore,
    ) -> Result<(), String> {
        let Some(id) = store.store_id() else {
            self.set_moved(feed, None);
            return Ok(());
        };
        let binding = StoreBinding {
            store: feed.spec.store.clone(),
            id,
        };
        let text = serde_json::to_string(&binding).map_err(|e| e.to_string())?;
        state
            .history()
            .meta_put(&store_binding_key(feed.uid.as_str()), &text)
            .await
            .map_err(|e| e.to_string())?;
        self.bound
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(feed.uid.as_str().to_string(), id);
        self.set_moved(feed, None);
        Ok(())
    }

    /// Forget a deleted feed's store record.
    pub(crate) async fn unbind(&self, state: &ServerState, feed_uid: &str) -> Result<(), String> {
        state
            .history()
            .meta_delete(&store_binding_key(feed_uid))
            .await
            .map_err(|e| e.to_string())?;
        self.bound
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(feed_uid);
        self.moved
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(feed_uid);
        Ok(())
    }

    /// [`check_store`](Self::check_store), then create the feed's rows.
    pub async fn ensure(
        &self,
        state: &ServerState,
        feed: &Feed,
        store: &dyn FeedStore,
        tenant_limits: Option<&TenantLimits>,
    ) -> Result<(), EnsureError> {
        self.check_store(state, feed, store).await?;
        ensure_in_store(store, feed, tenant_limits).await
    }

    /// Start the scheduler on this server if it is not running: called when
    /// the server first sees a feed.
    pub fn ensure_started(&self, state: &ServerState) {
        if !self.scheduling.load(Ordering::Acquire) || self.started.swap(true, Ordering::AcqRel) {
            return;
        }
        tracing::info!("the first feed exists: starting the feed scheduler");
        metrics::describe();
        let shutdown = state.shutdown_token();
        let state = state.clone();
        tokio::spawn(runner::supervise(state, shutdown, Duration::from_secs(1)));
    }

    /// The lease tick's check: start when a feed exists somewhere.
    pub async fn lease_tick(&self, state: &ServerState) {
        if self.started() {
            return;
        }
        let exists = !self.file.feeds.is_empty()
            || matches!(state.history().feed_count().await, Ok(n) if n > 0);
        if exists {
            self.ensure_started(state);
        }
    }

    /// Reload the definitions unless they were reloaded within the last
    /// second; concurrent callers share one reload. `Err` while the run
    /// history is unreachable: the caller answers as if it knew nothing.
    pub async fn refresh_now(&self, state: &ServerState) -> Result<Arc<Snapshot>, String> {
        let mut last = self.last_refresh.lock().await;
        if last.is_some_and(|t| t.elapsed() < REFRESH_NOW_GAP) {
            return Ok(self.snapshot());
        }
        let snapshot = self.refresh(state).await?;
        *last = Some(Instant::now());
        if !snapshot.feeds.is_empty() || !snapshot.unfinished.is_empty() {
            self.ensure_started(state);
        }
        Ok(snapshot)
    }

    /// Reload the definitions from the run history and the file. Reloads
    /// run one at a time, so an older read never replaces a newer one.
    pub async fn refresh(&self, state: &ServerState) -> Result<Arc<Snapshot>, String> {
        let _one_at_a_time = self.refreshing.lock().await;
        let history = state.history();
        if history.degraded() {
            return Err("the run history is unreachable".into());
        }
        let stored = history.feed_list().await.map_err(|e| e.to_string())?;
        let tokens = history
            .feed_token_list(None)
            .await
            .map_err(|e| e.to_string())?;
        let mut tenants: HashSet<String> = HashSet::new();
        tenants.extend(stored.iter().filter_map(|r| r.tenant().map(String::from)));
        tenants.extend(self.file.feeds.iter().filter_map(|f| f.tenant.clone()));
        let mut tenant_limits = HashMap::new();
        for t in tenants {
            if let Some(rec) = history.tenant_get(&t).await.map_err(|e| e.to_string())? {
                tenant_limits.insert(t, rec.limits);
            }
        }
        let mut feeds: Vec<Arc<Feed>> = Vec::new();
        let mut taken: HashSet<String> = HashSet::new();
        for spec in &self.file.feeds {
            let key = feed_key(spec.tenant.as_deref(), &spec.name);
            let limits = spec.tenant.as_ref().and_then(|t| tenant_limits.get(t));
            match self.file_feed_resolved(state, spec, &key, limits).await {
                Ok(feed) => {
                    taken.insert(key);
                    feeds.push(Arc::new(feed));
                }
                Err(e) => {
                    tracing::warn!(feed = %spec.name, error = %e, "could not prepare a --feeds file feed; retrying")
                }
            }
        }
        let mut unfinished = Vec::new();
        let mut deleted: HashSet<String> = HashSet::new();
        for record in stored {
            if !record.live() {
                deleted.insert(record.uid.clone());
                unfinished.push(record);
                continue;
            }
            if !record.removed.is_empty() {
                unfinished.push(record.clone());
            }
            if taken.contains(&record.key()) {
                continue;
            }
            match feed_of_record(state, &record) {
                Ok(feed) => feeds.push(Arc::new(feed)),
                Err(e) => {
                    tracing::warn!(feed = %record.name(), error = %e, "skipping a stored feed that cannot be read")
                }
            }
        }
        let now = chrono::Utc::now();
        let tokens = tokens
            .into_iter()
            .filter(|t| t.usable(now) && !deleted.contains(&t.feed))
            .collect();
        // Check that each feed's store holds its rows (a store that cannot
        // be opened now is checked on a later pass; until then nothing hands
        // it out for the feed).
        let mut opened: HashMap<String, Option<Arc<dyn FeedStore>>> = HashMap::new();
        for feed in &mut feeds {
            if feed.derived.store_moved.is_some() {
                continue;
            }
            let name = feed.spec.store.clone();
            if !opened.contains_key(&name) {
                let store = tokio::time::timeout(STORE_OPEN_WAIT, self.stores.get(&name))
                    .await
                    .ok()
                    .and_then(Result::ok);
                opened.insert(name.clone(), store);
            }
            let Some(Some(store)) = opened.get(&name) else {
                continue;
            };
            if let Err(EnsureError::Moved(moved)) = self.check_store(state, feed, &**store).await {
                Arc::make_mut(feed).derived.store_moved = Some(moved);
            }
        }
        let mut snapshot = Snapshot::new(feeds, tokens, tenant_limits);
        snapshot.unfinished = unfinished;
        let snapshot = Arc::new(snapshot);
        *self
            .snapshot
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = snapshot.clone();
        Ok(snapshot)
    }

    async fn file_feed_resolved(
        &self,
        state: &ServerState,
        spec: &FeedSpec,
        key: &str,
        limits: Option<&TenantLimits>,
    ) -> Result<Feed, String> {
        let cached = self
            .file_ids
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(key)
            .cloned();
        let (ids, gone) = match cached {
            Some(ids) => (ids, Vec::new()),
            None => resolve_file_ids(state, key, spec).await?,
        };
        let uid = FeedUid::new(ids.uid.clone()).map_err(|e| e.to_string())?;
        let mut deliveries = BTreeMap::new();
        for d in &spec.deliveries {
            let id = ids
                .deliveries
                .get(&d.name)
                .ok_or_else(|| format!("delivery '{}' has no id yet", d.name))?;
            deliveries.insert(
                d.name.clone(),
                DeliveryRef {
                    uid: DeliveryUid::new(uid.clone(), id.clone()).map_err(|e| e.to_string())?,
                    meta: DeliveryMeta {
                        uid: id.clone(),
                        ..Default::default()
                    },
                },
            );
        }
        let mut derived = Derived {
            references: self.file.references.get(key).cloned().unwrap_or_default(),
            ..Derived::default()
        };
        for d in &spec.deliveries {
            let Some(t) = &d.template else {
                continue;
            };
            let secret = crate::serve::schedules::secrets::declared_secret_params(
                state,
                &t.id,
                t.version.as_deref(),
            )
            .await
            .ok();
            crate::serve::schedules::secrets::mask_declared(
                &mut t.params.clone(),
                secret.as_deref(),
            );
            derived.secret_params.insert(d.name.clone(), secret);
        }
        let mut feed = Feed {
            uid,
            spec: spec.clone(),
            source: Source::File,
            deliveries,
            approved: true,
            revision: 0,
            ensured: false,
            derived,
        };
        let already = self
            .ensured
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(key);
        if !already {
            let store = self.stores.get(&feed.spec.store).await?;
            match self.ensure(state, &feed, &*store, limits).await {
                Ok(()) => {}
                Err(EnsureError::Moved(moved)) => {
                    feed.derived.store_moved = Some(moved);
                    return Ok(feed);
                }
                Err(e) => return Err(e.into()),
            }
            let calls = store_calls();
            for id in &gone {
                let dropped =
                    DeliveryUid::new(feed.uid.clone(), id.clone()).map_err(|e| e.to_string())?;
                store
                    .delete_delivery(&calls, &dropped)
                    .await
                    .map_err(|e| e.to_string())?;
                status::forget(&*state.history(), feed.uid.as_str(), id).await;
            }
            self.ensured
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(key.to_string());
            self.file_ids
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(key.to_string(), ids);
        }
        feed.ensured = true;
        Ok(feed)
    }

    /// The handle a delivery run reads through: the store its binding names,
    /// opened on this server, the payload opener and the feed's name.
    pub async fn execution(
        &self,
        state: &ServerState,
        binding: &FeedBinding,
    ) -> Result<FeedExecution, String> {
        let store = self.stores.get(&binding.store).await?;
        let mut execution = FeedExecution::new(binding.clone(), store);
        if let Some(opener) = secrets::payload_opener(state) {
            execution = execution.opener(opener);
        }
        if let Some(feed) = self.snapshot().by_uid(binding.feed.as_str()) {
            execution = execution.dataset(feed.label());
        }
        Ok(execution)
    }
}

/// Why a feed's rows could not be created in its store.
#[derive(Debug)]
pub enum EnsureError {
    /// The store failed or refused.
    Store {
        /// Waiting may fix it (the store is down, busy or timed out).
        transient: bool,
        message: String,
    },
    /// The feed's events are in another store than the one it names.
    Moved(StoreMoved),
}

impl EnsureError {
    fn store(e: &faucet_common_feed::FeedError, context: Option<&str>) -> Self {
        EnsureError::Store {
            transient: e.is_transient() || matches!(e, faucet_common_feed::FeedError::Call(_)),
            message: match context {
                Some(c) => format!("{c}: {e}"),
                None => e.to_string(),
            },
        }
    }

    /// Whether waiting may fix it.
    pub fn is_transient(&self) -> bool {
        match self {
            EnsureError::Store { transient, .. } => *transient,
            EnsureError::Moved(_) => false,
        }
    }
}

impl std::fmt::Display for EnsureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EnsureError::Store { message, .. } => f.write_str(message),
            EnsureError::Moved(m) => m.fmt(f),
        }
    }
}

impl From<EnsureError> for String {
    fn from(e: EnsureError) -> Self {
        e.to_string()
    }
}

/// Create `feed` and its deliveries in `store` when missing, and store its
/// limits.
pub async fn ensure_in_store(
    store: &dyn FeedStore,
    feed: &Feed,
    tenant_limits: Option<&TenantLimits>,
) -> Result<(), EnsureError> {
    let calls = store_calls();
    let tenant = feed
        .tenant()
        .map(TenantId::new)
        .transpose()
        .map_err(|e| EnsureError::store(&e, None))?;
    store
        .ensure_feed(
            &calls,
            &feed.uid,
            tenant.as_ref(),
            &store_limits(&feed.spec, tenant_limits),
        )
        .await
        .map_err(|e| EnsureError::store(&e, None))?;
    for d in &feed.spec.deliveries {
        if let Some(r) = feed.delivery(&d.name) {
            store
                .ensure_delivery(&calls, &r.uid, start_of(d.start))
                .await
                .map_err(|e| EnsureError::store(&e, Some(&format!("delivery '{}'", d.name))))?;
        }
    }
    Ok(())
}

/// The runnable form of a stored feed: its signature secret opened. A secret
/// that does not open leaves the feed usable for everything but ingest: its
/// secret is emptied (never the sealed text, which is no secret) and
/// [`Derived::signature_unreadable`] says why.
pub fn feed_of_record(state: &ServerState, record: &FeedRecord) -> Result<Feed, String> {
    let uid = FeedUid::new(record.uid.clone()).map_err(|e| e.to_string())?;
    let mut spec = record.spec.clone();
    let mut derived = Derived::default();
    if let Err(e) = secrets::open_signature(state, &mut spec, &record.uid) {
        if let Some(sig) = spec.ingest.auth.signature.as_mut() {
            sig.secret.clear();
        }
        derived.signature_unreadable = Some(e);
    }
    let mut deliveries = BTreeMap::new();
    for (name, meta) in &record.deliveries {
        deliveries.insert(
            name.clone(),
            DeliveryRef {
                uid: DeliveryUid::new(uid.clone(), meta.uid.clone()).map_err(|e| e.to_string())?,
                meta: meta.clone(),
            },
        );
    }
    Ok(Feed {
        uid,
        spec,
        source: Source::Console,
        deliveries,
        approved: record.approved,
        revision: record.revision,
        ensured: true,
        derived,
    })
}

/// Read or create a file feed's ids, adding an id for each new delivery and
/// dropping the ids of deliveries the file no longer lists (returned).
async fn resolve_file_ids(
    state: &ServerState,
    key: &str,
    spec: &FeedSpec,
) -> Result<(FileIds, Vec<String>), String> {
    let history = state.history();
    let meta = file_ids_key(key);
    for _ in 0..5 {
        let current = history.meta_get(&meta).await.map_err(|e| e.to_string())?;
        let stored: Option<FileIds> = current
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok());
        let mut ids = stored.clone().unwrap_or_else(|| FileIds {
            uid: FeedUid::generate().as_str().to_string(),
            deliveries: BTreeMap::new(),
        });
        let wanted: HashSet<&str> = spec.deliveries.iter().map(|d| d.name.as_str()).collect();
        let gone: Vec<String> = ids
            .deliveries
            .iter()
            .filter(|(n, _)| !wanted.contains(n.as_str()))
            .map(|(_, id)| id.clone())
            .collect();
        ids.deliveries.retain(|n, _| wanted.contains(n.as_str()));
        for d in &spec.deliveries {
            ids.deliveries
                .entry(d.name.clone())
                .or_insert_with(|| uuid::Uuid::new_v4().simple().to_string());
        }
        if stored.as_ref() == Some(&ids) {
            return Ok((ids, gone));
        }
        let encoded = serde_json::to_string(&ids).map_err(|e| e.to_string())?;
        if history
            .meta_compare_and_set(&meta, current.as_deref(), &encoded)
            .await
            .map_err(|e| e.to_string())?
        {
            return Ok((ids, gone));
        }
    }
    Err("the feed's ids kept changing".into())
}

#[cfg(test)]
mod tests;
