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
use std::sync::atomic::{AtomicBool, Ordering};
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
    pub loaded_at: Instant,
}

impl Default for Snapshot {
    fn default() -> Self {
        Self::new(Vec::new(), Vec::new(), HashMap::new())
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
            loaded_at: Instant::now(),
        }
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

/// What `/readyz` reports about feeds.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Readiness {
    pub started: bool,
    pub store_ok: bool,
    pub feeds: usize,
    pub stalled: Vec<String>,
    pub held: Vec<String>,
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
    if let Some(bytes) = tenant.and_then(|t| t.max_feed_bytes) {
        limits = limits.tenant_max_bytes(bytes);
    }
    limits
}

/// Where a new delivery starts in the store.
pub fn start_of(start: crate::serve::history::feeds::StartSpec) -> Start {
    use crate::serve::history::feeds::StartSpec;
    match start {
        StartSpec::Earliest => Start::Earliest,
        StartSpec::Latest => Start::Latest,
        StartSpec::Seq(n) => Start::Seq(faucet_common_feed::Seq(n)),
        StartSpec::Time(t) => Start::Time(t),
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
    changed: Arc<tokio::sync::Notify>,
    last_refresh: tokio::sync::Mutex<Option<Instant>>,
    readiness: Mutex<Readiness>,
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
            changed: Arc::new(tokio::sync::Notify::new()),
            last_refresh: tokio::sync::Mutex::new(None),
            readiness: Mutex::new(Readiness::default()),
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
        self.scheduling.store(false, Ordering::Release);
        self
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
        r
    }

    pub(crate) fn set_readiness(&self, readiness: Readiness) {
        *self
            .readiness
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = readiness;
    }

    /// The store that holds `feed`, opened on first use.
    pub async fn store_for(&self, feed: &Feed) -> Result<Arc<dyn FeedStore>, String> {
        self.stores.get(&feed.spec.store).await
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
        if !snapshot.feeds.is_empty() {
            self.ensure_started(state);
        }
        Ok(snapshot)
    }

    /// Reload the definitions from the run history and the file.
    pub async fn refresh(&self, state: &ServerState) -> Result<Arc<Snapshot>, String> {
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
        for record in stored {
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
        let tokens = tokens.into_iter().filter(|t| t.usable(now)).collect();
        let snapshot = Arc::new(Snapshot::new(feeds, tokens, tenant_limits));
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
        let mut feed = Feed {
            uid,
            spec: spec.clone(),
            source: Source::File,
            deliveries,
            approved: true,
            revision: 0,
            ensured: false,
        };
        let already = self
            .ensured
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(key);
        if !already {
            let store = self.store_for(&feed).await?;
            ensure_in_store(&*store, &feed, limits).await?;
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

/// Create `feed` and its deliveries in `store` when missing, and store its
/// limits.
pub async fn ensure_in_store(
    store: &dyn FeedStore,
    feed: &Feed,
    tenant_limits: Option<&TenantLimits>,
) -> Result<(), String> {
    let calls = store_calls();
    let tenant = feed
        .tenant()
        .map(TenantId::new)
        .transpose()
        .map_err(|e| e.to_string())?;
    store
        .ensure_feed(
            &calls,
            &feed.uid,
            tenant.as_ref(),
            &store_limits(&feed.spec, tenant_limits),
        )
        .await
        .map_err(|e| e.to_string())?;
    for d in &feed.spec.deliveries {
        if let Some(r) = feed.delivery(&d.name) {
            store
                .ensure_delivery(&calls, &r.uid, start_of(d.start))
                .await
                .map_err(|e| format!("delivery '{}': {e}", d.name))?;
        }
    }
    Ok(())
}

/// The runnable form of a stored feed: its signature secret opened.
pub fn feed_of_record(state: &ServerState, record: &FeedRecord) -> Result<Feed, String> {
    let uid = FeedUid::new(record.uid.clone()).map_err(|e| e.to_string())?;
    let mut spec = record.spec.clone();
    secrets::open_signature(state, &mut spec, &record.uid)?;
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
