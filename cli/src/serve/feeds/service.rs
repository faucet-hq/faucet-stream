//! Every rule of the feeds API (#1034), so the HTTP, CLI and MCP adapters
//! stay thin: who sees which feed, what a definition may say, quotas, how a
//! feed, a delivery and a token are made, changed and removed, and the
//! operator's moves on a delivery (pause, resume, deliver now, replay, skip,
//! recover).

use super::status::{self, DeliveryStatus, Health};
use super::{
    DeliveryRef, Feed, Source, StopAfter, ensure_in_store, feed_of_record, metrics, secrets, spec,
    store_calls,
};
use crate::serve::error::ServeError;
use crate::serve::history::feeds::{
    DeliverySpec, FeedChange, FeedRecord, FeedSpec, FeedTokenRecord, RemovedDelivery,
    RunFeedBinding, feed_key, spec_diff,
};
use crate::serve::rbac::{AuthContext, Role};
use crate::serve::state::ServerState;
use chrono::{DateTime, Utc};
use faucet_common_feed::{ClaimOwner, DeliveryUid, FeedStore, FeedUid, Head, Move, Seq};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

const REPLACE_ATTEMPTS: usize = 5;

fn unprocessable(message: impl Into<String>) -> ServeError {
    ServeError::Unprocessable {
        message: message.into(),
        details: None,
    }
}

fn store_error(e: impl std::fmt::Display) -> ServeError {
    ServeError::Unavailable(format!("feed store: {e}"))
}

/// The tenant an action is for: a tenant principal acts on its own tenant
/// only; anyone else on the tenant the route names.
pub fn scope(actor: &AuthContext, tenant: Option<&str>) -> Result<Option<String>, ServeError> {
    match (&actor.tenant, tenant) {
        (Some(own), Some(asked)) if own != asked => Err(ServeError::NotFound),
        (Some(own), _) => Ok(Some(own.clone())),
        (None, t) => Ok(t.map(str::to_string)),
    }
}

/// One delivery as a response shows it.
#[derive(Debug, Clone, Serialize)]
pub struct DeliveryView {
    pub name: String,
    pub uid: String,
    #[serde(flatten)]
    pub spec: DeliverySpec,
    /// `ok`, `failing`, `stalled`, `held`, `paused`, `needs_approval`, or
    /// `setting_up` while the store has no position for it yet.
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub paused_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pause_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub paused_at: Option<DateTime<Utc>>,
    /// The next event the delivery reads.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub epoch: Option<u64>,
    /// Events not delivered yet.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lag: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oldest_undelivered_at: Option<DateTime<Utc>>,
    /// The size of the next run, in events.
    pub batch_events: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub claim: Option<status::LiveClaim>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_run: Option<status::LastRun>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub consecutive_failures: u32,
    /// Events stored in the last 24 hours that this delivery has delivered.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivered_24h: Option<u64>,
    /// The delivery guarantee its pipeline gives, derived from the template
    /// as `faucet validate` derives it; `unknown` when it cannot be derived.
    /// Only on a feed's own page.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub guarantee: Option<String>,
    /// No run starts before this: the back-off after a failed or refused
    /// run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_run_at: Option<DateTime<Utc>>,
    /// When a run last moved the delivery's position.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_delivered_at: Option<DateTime<Utc>>,
    /// Payload bytes from the delivery's position on. Only on a feed's own
    /// page, and only when the store can count them.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub undelivered_bytes: Option<u64>,
}

/// One feed as a response shows it.
#[derive(Debug, Clone, Serialize)]
pub struct FeedView {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    pub uid: String,
    pub source: Source,
    pub store: String,
    pub revision: u64,
    pub approved: bool,
    /// The definition, with secrets replaced by markers.
    pub spec: FeedSpec,
    /// The last stored event's number.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub watermark: Option<u64>,
    /// The oldest retained event's number.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oldest: Option<u64>,
    /// Payload bytes of every retained event: the undelivered ones and the
    /// delivered ones kept for replay.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retained_bytes: Option<u64>,
    /// Payload bytes the slowest delivery has not delivered yet: what the
    /// feed's undelivered limit counts.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub undelivered_bytes: Option<u64>,
    /// Retained events and their payload bytes stored in the last 24 hours.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub events_24h: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_24h: Option<u64>,
    /// When the newest retained event was received.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_event_at: Option<DateTime<Utc>>,
    /// Worst first: `held`, `secret_unreadable` (this server cannot open the
    /// signature secret, so it refuses senders), `stalled`, `full` (an undelivered limit is
    /// reached), `over_quota` (the tenant's feeds in this store hold its
    /// whole byte quota, delivered events kept for replay included),
    /// `paused` or `receiving`.
    pub status: String,
    /// The path senders post events to.
    pub ingest_path: String,
    /// Undelivered bytes or age as a fraction of the feed's undelivered
    /// limits (the larger); at 1 the feed refuses new events.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub undelivered_ratio: Option<f64>,
    /// Requests this server refused lately (see the refusals listing).
    pub refused_recent: u64,
    /// What to know before relying on the definition as it is.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    pub deliveries: Vec<DeliveryView>,
}

/// The path senders post a feed's events to.
pub fn ingest_path(tenant: Option<&str>, name: &str) -> String {
    match tenant {
        Some(t) => format!("/v1/tenants/{t}/feeds/{name}/events"),
        None => format!("/v1/feeds/{name}/events"),
    }
}

/// How full a feed is: its undelivered bytes or age against its limits.
/// `undelivered` is the bytes from the lowest cursor on.
fn undelivered_ratio(feed: &Feed, head: &Head, undelivered: u64) -> f64 {
    let limits = &feed.spec.retention.undelivered;
    let by_bytes = undelivered as f64 / limits.max_bytes.bytes().max(1) as f64;
    let oldest = head
        .deliveries
        .iter()
        .filter_map(|d| d.oldest_undelivered_at)
        .map(|t| (head.now - t).num_milliseconds().max(0) as f64 / 1000.0)
        .fold(0.0, f64::max);
    by_bytes.max(oldest / limits.max_age.secs().max(1) as f64)
}

/// The bytes of `head` no delivery has delivered yet, when no count is
/// needed to know: none pending, or nothing delivered is still retained.
fn undelivered_known(head: &Head) -> Option<u64> {
    let lowest = head.deliveries.iter().map(|d| d.cursor).min()?;
    if lowest >= head.end() {
        Some(0)
    } else if lowest <= head.oldest {
        Some(head.bytes)
    } else {
        None
    }
}

/// The status word a feed shows, worst first.
fn feed_status(deliveries: &[DeliveryView], ratio: Option<f64>) -> &'static str {
    let any = |word: &str| deliveries.iter().any(|d| d.status == word);
    if any("held") {
        "held"
    } else if any("stalled") {
        "stalled"
    } else if ratio.is_some_and(|r| r >= 1.0) {
        "full"
    } else if !deliveries.is_empty() && deliveries.iter().all(|d| d.status == "paused") {
        "paused"
    } else {
        "receiving"
    }
}

/// A gap in what a delivery read.
#[derive(Debug, Clone, Serialize)]
pub struct GapView {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivery: Option<String>,
    pub from: u64,
    pub to: u64,
    pub reason: String,
    pub at: DateTime<Utc>,
    pub by: String,
}

/// A feed token as a response shows it: never the token or its hash.
#[derive(Debug, Clone, Serialize)]
pub struct TokenView {
    pub id: String,
    pub name: String,
    pub prefix: String,
    pub created_at: DateTime<Utc>,
    pub created_by: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revoked_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_used_at: Option<DateTime<Utc>>,
}

impl From<&FeedTokenRecord> for TokenView {
    fn from(t: &FeedTokenRecord) -> Self {
        Self {
            id: t.id.clone(),
            name: t.name.clone(),
            prefix: t.prefix.clone(),
            created_at: t.created_at,
            created_by: t.created_by.clone(),
            expires_at: t.expires_at,
            revoked_at: t.revoked_at,
            last_used_at: t.last_used_at,
        }
    }
}

/// One feed with what only its own page shows.
#[derive(Debug, Clone, Serialize)]
pub struct FeedDetail {
    #[serde(flatten)]
    pub feed: FeedView,
    pub gaps: Vec<GapView>,
    pub tokens: Vec<TokenView>,
    pub changes: Vec<FeedChange>,
    /// Who made the feed and when; absent for a `--feeds` file feed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_by: Option<String>,
    /// Retained events stored in each of the last 24 hours, oldest first.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub hourly_24h: Vec<u64>,
}

/// The status word a delivery shows.
pub fn status_word(d: &DeliverySpec, s: &DeliveryStatus) -> &'static str {
    if d.paused || s.paused.is_some() {
        "paused"
    } else if s.health != Health::Ok {
        s.health.as_str()
    } else if s.needs_approval.is_some() {
        "needs_approval"
    } else {
        "ok"
    }
}

/// Build a feed's view from its definition, the store's head and each
/// delivery's status. Pure.
pub fn view(
    feed: &Feed,
    head: Option<&Head>,
    undelivered: Option<u64>,
    statuses: &std::collections::BTreeMap<String, DeliveryStatus>,
) -> FeedView {
    let masked = secrets::masked_feed(feed);
    let deliveries = masked
        .deliveries
        .iter()
        .filter_map(|d| {
            let dref = feed.delivery(&d.name)?;
            let s = statuses.get(&d.name).cloned().unwrap_or_default();
            let dh = head.and_then(|h| h.deliveries.iter().find(|x| x.delivery == dref.uid));
            let (max_events, _) = super::runner::effective_batch(d, None);
            Some(DeliveryView {
                name: d.name.clone(),
                uid: dref.uid.id().to_string(),
                status: match (head, dh, status_word(d, &s)) {
                    (Some(_), None, "ok") => "setting_up",
                    (_, _, word) => word,
                }
                .to_string(),
                paused_by: s
                    .paused
                    .as_ref()
                    .map(|p| p.by.clone())
                    .or_else(|| dref.meta.paused_by.clone()),
                pause_reason: s
                    .paused
                    .as_ref()
                    .and_then(|p| p.reason.clone())
                    .or_else(|| dref.meta.pause_reason.clone()),
                paused_at: s.paused.as_ref().map(|p| p.at).or(dref.meta.paused_at),
                position: dh.map(|x| x.cursor.get()),
                epoch: dh.map(|x| x.epoch.0),
                lag: match (dh, head) {
                    (Some(x), Some(h)) => Some(x.cursor.distance_to(h.end())),
                    _ => None,
                },
                oldest_undelivered_at: dh.and_then(|x| x.oldest_undelivered_at),
                batch_events: s.batch(max_events),
                claim: s.claim.clone(),
                last_run: s.last_run.clone(),
                last_error: s.last_error.clone().or(s.needs_approval.clone()),
                consecutive_failures: s.consecutive_failures,
                delivered_24h: None,
                guarantee: None,
                next_run_at: s.backoff_until,
                last_delivered_at: s.last_delivered_at,
                undelivered_bytes: None,
                spec: d.clone(),
            })
        })
        .collect::<Vec<DeliveryView>>();
    let no_delivery = head.is_some_and(|h| h.deliveries.is_empty());
    let undelivered = head
        .and_then(undelivered_known)
        .or(undelivered)
        .or(no_delivery.then_some(0));
    let ratio = head
        .zip(undelivered)
        .map(|(h, bytes)| undelivered_ratio(feed, h, bytes));
    FeedView {
        status: feed_status(&deliveries, ratio).to_string(),
        ingest_path: ingest_path(feed.tenant(), feed.name()),
        undelivered_ratio: ratio,
        refused_recent: 0,
        warnings: super::warnings(&feed.spec),
        name: feed.name().to_string(),
        tenant: feed.tenant().map(str::to_string),
        uid: feed.uid.as_str().to_string(),
        source: feed.source,
        store: feed.spec.store.clone(),
        revision: feed.revision,
        approved: feed.approved,
        spec: masked,
        watermark: head.map(|h| h.watermark.get()),
        oldest: head.map(|h| h.oldest.get()),
        retained_bytes: head.map(|h| h.bytes),
        undelivered_bytes: undelivered,
        events_24h: None,
        bytes_24h: None,
        last_event_at: None,
        deliveries,
    }
}

/// A feed found for an action.
#[derive(Debug, Clone)]
pub struct Found {
    pub feed: Arc<Feed>,
    /// The stored record; `None` for a `--feeds` file feed.
    pub record: Option<FeedRecord>,
}

/// The feed `name` of `tenant`: the file's, else the stored one, read from
/// the history so a change made on another server is seen at once.
pub async fn find(
    state: &ServerState,
    tenant: Option<&str>,
    name: &str,
) -> Result<Found, ServeError> {
    let rt = state.feeds();
    if rt.file_feed(tenant, name).is_some() {
        let snapshot = match rt.snapshot().feed(tenant, name) {
            Some(f) => Some(f),
            None => rt
                .refresh(state)
                .await
                .map_err(ServeError::Unavailable)?
                .feed(tenant, name),
        };
        return snapshot
            .map(|feed| Found { feed, record: None })
            .ok_or_else(|| ServeError::Unavailable("the feed is not ready yet; retry".into()));
    }
    let record = state
        .history()
        .feed_get(&feed_key(tenant, name))
        .await
        .map_err(ServeError::from)?
        .filter(FeedRecord::live)
        .ok_or(ServeError::NotFound)?;
    let feed = feed_of_record(state, &record).map_err(ServeError::Internal)?;
    Ok(Found {
        feed: Arc::new(feed),
        record: Some(record),
    })
}

fn read_only(found: &Found) -> Result<&FeedRecord, ServeError> {
    found.record.as_ref().ok_or_else(|| {
        ServeError::Conflict(format!(
            "feed '{}' is defined in the --feeds file: edit the file and restart the server",
            found.feed.name()
        ))
    })
}

async fn head_of(store: &dyn FeedStore, feed: &Feed) -> Result<Option<Head>, ServeError> {
    Ok(store
        .heads(&store_calls(), std::slice::from_ref(&feed.uid))
        .await
        .map_err(store_error)?
        .into_iter()
        .next())
}

async fn statuses(
    state: &ServerState,
    feed: &Feed,
) -> Result<std::collections::BTreeMap<String, DeliveryStatus>, ServeError> {
    let history = state.history();
    let mut out = std::collections::BTreeMap::new();
    for (name, dref) in &feed.deliveries {
        let s = status::read(&*history, feed.uid.as_str(), dref.uid.id())
            .await
            .map_err(ServeError::Unavailable)?;
        out.insert(name.clone(), s);
    }
    Ok(out)
}

/// The bytes no delivery of `feed` has delivered yet: what the scheduler
/// counted a moment ago, else counted now from the lowest cursor.
async fn undelivered_of(
    state: &ServerState,
    store: &dyn FeedStore,
    feed: &Feed,
    head: &Head,
) -> Option<u64> {
    if let Some(known) = undelivered_known(head) {
        return Some(known);
    }
    let lowest = head.deliveries.iter().map(|d| d.cursor).min()?;
    if let Some(counted) = state.feeds().bytes(feed.uid.as_str())
        && counted.lowest == lowest
        && counted.oldest == head.oldest
    {
        return Some(head.bytes.saturating_sub(counted.delivered));
    }
    store
        .bytes_from(&store_calls(), &feed.uid, lowest)
        .await
        .ok()
        .flatten()
}

/// Per delivery of `feed`: the payload bytes from its position on. A
/// delivery the store cannot count for is left out.
async fn delivery_bytes(state: &ServerState, feed: &Feed) -> HashMap<String, u64> {
    let mut out = HashMap::new();
    let Ok(store) = state.feeds().store_for(feed).await else {
        return out;
    };
    let Ok(Some(head)) = head_of(&*store, feed).await else {
        return out;
    };
    for (name, dref) in &feed.deliveries {
        let Some(dh) = head.deliveries.iter().find(|x| x.delivery == dref.uid) else {
            continue;
        };
        let bytes = if dh.cursor >= head.end() {
            Some(0)
        } else if dh.cursor <= head.oldest {
            Some(head.bytes)
        } else {
            store
                .bytes_from(&store_calls(), &feed.uid, dh.cursor)
                .await
                .ok()
                .flatten()
        };
        if let Some(b) = bytes {
            out.insert(name.clone(), b);
        }
    }
    out
}

/// Feeds viewed side by side.
const VIEWS_AT_ONCE: usize = 8;

/// The views of `feeds`: one `heads` and one newest-event query per store,
/// then each feed's statuses and last-24-hours figures side by side.
/// The payload bytes all of `tenant`'s feeds hold in each store they are
/// in: what its byte quota is checked against in that store (delivered
/// events kept for replay included). A store that cannot be read is left
/// out.
async fn tenant_store_bytes(
    state: &ServerState,
    snapshot: &super::Snapshot,
    tenant: &str,
) -> BTreeMap<String, u64> {
    let mut by_store: BTreeMap<&str, Vec<FeedUid>> = BTreeMap::new();
    for f in snapshot.feeds.iter().filter(|f| f.tenant() == Some(tenant)) {
        by_store
            .entry(f.spec.store.as_str())
            .or_default()
            .push(f.uid.clone());
    }
    let Ok(id) = faucet_common_feed::TenantId::new(tenant) else {
        return BTreeMap::new();
    };
    let calls = store_calls();
    let mut out = BTreeMap::new();
    for (name, uids) in by_store {
        let Ok(store) = state.feeds().stores().get(name).await else {
            continue;
        };
        let held = match store.tenant_bytes(&calls, &id).await {
            Ok(Some(held)) => held,
            Ok(None) => match store.heads(&calls, &uids).await {
                Ok(heads) => heads.iter().map(|h| h.bytes).sum(),
                Err(_) => continue,
            },
            Err(_) => continue,
        };
        out.insert(name.to_string(), held);
    }
    out
}

/// The `(tenant, store)` pairs whose feeds hold the tenant's whole byte
/// quota there, so they refuse new events.
async fn over_quota(state: &ServerState, feeds: &[Arc<Feed>]) -> HashSet<(String, String)> {
    let tenants: std::collections::BTreeSet<&str> =
        feeds.iter().filter_map(|f| f.tenant()).collect();
    let mut out = HashSet::new();
    if tenants.is_empty() {
        return out;
    }
    let snapshot = state.feeds().snapshot();
    for t in tenants {
        let Ok(Some(record)) = state.history().tenant_get(t).await else {
            continue;
        };
        let Some(max) = record.limits.max_feed_bytes else {
            continue;
        };
        for (store, held) in tenant_store_bytes(state, &snapshot, t).await {
            if held >= max {
                out.insert((t.to_string(), store));
            }
        }
    }
    out
}

async fn views(state: &ServerState, feeds: &[Arc<Feed>]) -> Result<Vec<FeedView>, ServeError> {
    use futures::StreamExt;
    let rt = state.feeds();
    let over = &over_quota(state, feeds).await;
    let mut by_store: std::collections::BTreeMap<&str, Vec<FeedUid>> = Default::default();
    for f in feeds {
        by_store
            .entry(f.spec.store.as_str())
            .or_default()
            .push(f.uid.clone());
    }
    let mut stores: HashMap<&str, Arc<dyn FeedStore>> = HashMap::new();
    let mut heads: HashMap<FeedUid, Head> = HashMap::new();
    let mut newest: HashMap<FeedUid, DateTime<Utc>> = HashMap::new();
    let mut unbatched: HashSet<&str> = HashSet::new();
    let calls = store_calls();
    for (name, uids) in &by_store {
        let Ok(store) = rt.stores().get(name).await else {
            continue;
        };
        if let Ok(found) = store.heads(&calls, uids).await {
            heads.extend(found.into_iter().map(|h| (h.feed.clone(), h)));
        }
        match store.newest_received(&calls, uids).await {
            Ok(Some(found)) => newest.extend(found),
            _ => {
                unbatched.insert(name);
            }
        }
        stores.insert(name, store);
    }
    let (stores, heads, newest, unbatched) = (&stores, &heads, &newest, &unbatched);
    let one = |feed: &Arc<Feed>| {
        let feed = feed.clone();
        async move {
            let store = stores.get(feed.spec.store.as_str());
            let head = heads.get(&feed.uid);
            let undelivered = match (store, head) {
                (Some(store), Some(head)) => undelivered_of(state, &**store, &feed, head).await,
                _ => None,
            };
            let mut out = view(&feed, head, undelivered, &statuses(state, &feed).await?);
            if let Some(why) = &feed.derived.signature_unreadable {
                if out.status != "held" {
                    out.status = "secret_unreadable".into();
                }
                out.warnings.push(format!(
                    "{why}; this server refuses every request to the feed with 503 until it \
                     runs with the vault key the feed was made under or the secret is set again"
                ));
            }
            if matches!(out.status.as_str(), "receiving" | "paused")
                && let Some(t) = feed.tenant()
                && over.contains(&(t.to_string(), feed.spec.store.clone()))
            {
                out.status = "over_quota".into();
            }
            out.refused_recent = state.feeds().ingest().refusals(feed.uid.as_str()).len() as u64;
            let (Some(store), Some(head)) = (store, head) else {
                return Ok(out);
            };
            // What the last 24 hours brought, by the store's own clock.
            let calls = store_calls();
            let since = head.now - chrono::Duration::hours(24);
            if let Ok(from) = store.seq_at(&calls, &feed.uid, since).await {
                out.events_24h = Some(from.distance_to(head.end()));
                out.bytes_24h = store
                    .bytes_from(&calls, &feed.uid, from)
                    .await
                    .ok()
                    .flatten();
                for d in &mut out.deliveries {
                    d.delivered_24h = d.position.map(|p| from.distance_to(Seq(p).max(from)));
                }
            }
            out.last_event_at = newest.get(&feed.uid).copied();
            if unbatched.contains(feed.spec.store.as_str())
                && head.end() > head.oldest
                && let Ok(batch) = store
                    .read(&calls, &feed.uid, head.watermark, head.end())
                    .await
            {
                out.last_event_at = batch.events.last().map(|e| e.received_at);
            }
            Ok::<FeedView, ServeError>(out)
        }
    };
    let viewed: Vec<Result<FeedView, ServeError>> = futures::stream::iter(feeds.iter().map(one))
        .buffered(VIEWS_AT_ONCE)
        .collect()
        .await;
    viewed.into_iter().collect()
}

async fn view_of(state: &ServerState, feed: &Feed) -> Result<FeedView, ServeError> {
    let mut one = views(state, &[Arc::new(feed.clone())]).await?;
    one.pop()
        .ok_or_else(|| ServeError::Internal("a feed has no view".into()))
}

/// Retained events stored in each of the last 24 hours, oldest first; empty
/// when the store cannot say.
async fn hourly_24h(state: &ServerState, feed: &Feed) -> Vec<u64> {
    let Ok(store) = state.feeds().store_for(feed).await else {
        return Vec::new();
    };
    let calls = store_calls();
    if let Ok(Some(counts)) = store.hourly_counts(&calls, &feed.uid, 24).await {
        return counts;
    }
    let Ok(Some(head)) = head_of(&*store, feed).await else {
        return Vec::new();
    };
    let mut marks = Vec::with_capacity(25);
    for hours in (1..=24).rev() {
        match store
            .seq_at(&calls, &feed.uid, head.now - chrono::Duration::hours(hours))
            .await
        {
            Ok(seq) => marks.push(seq),
            Err(_) => return Vec::new(),
        }
    }
    marks.push(head.end());
    marks.windows(2).map(|w| w[0].distance_to(w[1])).collect()
}

/// A tenant's feed quotas and what it uses of them.
#[derive(Debug, Clone, Serialize)]
pub struct Quotas {
    pub tenant: String,
    /// Every feed of the tenant, not only the listed page's.
    pub feeds: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_feeds: Option<u32>,
    /// The bytes the tenant's feeds hold in the store where they hold the
    /// most, delivered events kept for replay included: what
    /// `max_feed_bytes` is checked against, per store.
    pub stored_bytes: u64,
    /// The same, for each store, when the tenant's feeds are in more than one.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub stored_bytes_by_store: BTreeMap<String, u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_feed_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_feed_events_per_sec: Option<u32>,
}

/// What the feeds page lists.
#[derive(Debug, Clone, Serialize)]
pub struct FeedList {
    pub feeds: Vec<FeedView>,
    /// The tenant's quotas, when the listing is for one tenant.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quotas: Option<Quotas>,
    /// The stores a new feed may name.
    pub stores: Vec<String>,
    /// Whether this listener serves the ingest routes; `false` with
    /// `--feeds-listen`, where only that listener does.
    pub ingest_on_api: bool,
    /// Whether creating or changing a feed here needs an approved change
    /// request (`always`, `deliveries` or `never`).
    pub approval: &'static str,
    /// Pass as `after` for the next page; absent on the last one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
}

/// The `tenant` value that lists the server's feeds and every tenant's.
pub const ALL_TENANTS: &str = "*";
/// Feeds on one page of the list, by default and at most.
pub const DEFAULT_PAGE: usize = 200;
pub const MAX_PAGE: usize = 1000;

/// Where a page of the list starts and how long it is.
#[derive(Debug, Clone, Default)]
pub struct Page {
    /// The `next` of the page before.
    pub after: Option<String>,
    pub limit: Option<usize>,
}

/// The feeds `actor` sees for `tenant`, with the tenant's quotas.
pub async fn overview(
    state: &ServerState,
    actor: &AuthContext,
    tenant: Option<&str>,
    page: Page,
) -> Result<FeedList, ServeError> {
    use crate::serve::changes::ChangeKind;
    let (feeds, next) = list_page(state, actor, tenant, &page).await?;
    let everyone = tenant == Some(ALL_TENANTS) && actor.tenant.is_none();
    let scoped = if everyone {
        None
    } else {
        scope(actor, tenant.filter(|t| *t != ALL_TENANTS))?
    };
    let quotas = match &scoped {
        Some(t) => {
            let limits = state
                .history()
                .tenant_get(t)
                .await
                .map_err(ServeError::from)?
                .map(|r| r.limits)
                .unwrap_or_default();
            let snapshot = listed(state).await?;
            let by_store = tenant_store_bytes(state, &snapshot, t).await;
            Some(Quotas {
                tenant: t.clone(),
                feeds: snapshot
                    .feeds
                    .iter()
                    .filter(|f| f.tenant() == Some(t.as_str()))
                    .count() as u64,
                max_feeds: limits.max_feeds,
                stored_bytes: by_store.values().copied().max().unwrap_or(0),
                stored_bytes_by_store: if by_store.len() > 1 {
                    by_store
                } else {
                    BTreeMap::new()
                },
                max_feed_bytes: limits.max_feed_bytes,
                max_feed_events_per_sec: limits.max_feed_events_per_sec,
            })
        }
        None => None,
    };
    let stores = if actor.role == Role::Admin && actor.tenant.is_none() {
        state.feeds().stores().names()
    } else {
        vec![spec::DEFAULT_STORE.to_string()]
    };
    Ok(FeedList {
        feeds,
        quotas,
        stores,
        ingest_on_api: state.feeds().ingest_on_api(),
        approval: if state.requires_approval(ChangeKind::Feed) {
            "always"
        } else if state.requires_approval(ChangeKind::Run) {
            "deliveries"
        } else {
            "never"
        },
        next,
    })
}

/// Most events one page of the event browser holds.
pub const MAX_EVENTS_PAGE: u64 = 200;

/// One stored event as the browser shows it.
#[derive(Debug, Clone, Serialize)]
pub struct EventView {
    pub seq: u64,
    pub id: String,
    pub received_at: DateTime<Utc>,
    pub stored_at: DateTime<Utc>,
    pub content_type: String,
    pub size: u64,
    pub sealed: bool,
    pub headers: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dedupe_key: Option<String>,
    /// The deliveries that have delivered it.
    pub delivered_by: Vec<String>,
    /// The payload: text, or base64 when it is not UTF-8. Only on the
    /// single-event read, and never for a sealed event.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<String>,
    /// `utf8` or `base64`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_encoding: Option<&'static str>,
    /// Why the payload is not shown (`sealed`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_withheld: Option<&'static str>,
}

/// A page of the event browser, newest first.
#[derive(Debug, Clone, Serialize)]
pub struct EventsPage {
    pub events: Vec<EventView>,
    /// The oldest retained event's number.
    pub oldest: u64,
    /// The last stored event's number.
    pub watermark: u64,
    /// Pass as `before` for the next (older) page.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_before: Option<u64>,
}

fn event_view(
    e: faucet_common_feed::StoredEvent,
    head: &Head,
    feed: &Feed,
    payload: bool,
) -> EventView {
    let delivered_by = feed
        .deliveries
        .iter()
        .filter(|(_, dref)| {
            head.deliveries
                .iter()
                .any(|d| d.delivery == dref.uid && d.cursor > e.seq)
        })
        .map(|(name, _)| name.clone())
        .collect();
    let (mut text, mut encoding, mut withheld) = (None, None, None);
    if payload && e.sealed {
        withheld = Some("sealed");
    } else if payload {
        match String::from_utf8(e.payload) {
            Ok(s) => (text, encoding) = (Some(s), Some("utf8")),
            Err(raw) => {
                let b64 = faucet_core::fidelity::binary(raw.as_bytes());
                (text, encoding) = (b64.as_str().map(str::to_string), Some("base64"));
            }
        }
    }
    EventView {
        seq: e.seq.get(),
        id: e.id,
        received_at: e.received_at,
        stored_at: e.stored_at,
        content_type: e.content_type,
        size: e.size,
        sealed: e.sealed,
        headers: e.headers,
        dedupe_key: e.dedupe_key,
        delivered_by,
        payload: text,
        payload_encoding: encoding,
        payload_withheld: withheld,
    }
}

/// Which events a page of the browser holds.
#[derive(Debug, Clone, Copy, Default)]
pub struct EventsWindow {
    /// The events before this one (default: the newest).
    pub before: Option<u64>,
    /// The events from this one on instead.
    pub from: Option<u64>,
    /// The events from the first one stored at or after this time on.
    pub at: Option<DateTime<Utc>>,
    /// At most this many (default 50, at most [`MAX_EVENTS_PAGE`]).
    pub limit: Option<u64>,
}

/// A page of a feed's retained events without their payloads, newest first:
/// the `limit` events before `before` (default: the newest), or, with
/// `from` (or `at`, the first event stored at or after that time), the
/// `limit` events from there on.
pub async fn events(
    state: &ServerState,
    actor: &AuthContext,
    tenant: Option<&str>,
    name: &str,
    window: EventsWindow,
) -> Result<EventsPage, ServeError> {
    let EventsWindow {
        before,
        from,
        at,
        limit,
    } = window;
    let tenant = scope(actor, tenant)?;
    let found = find(state, tenant.as_deref(), name).await?;
    let feed = &found.feed;
    let store = state.feeds().store_for(feed).await.map_err(store_error)?;
    let head = head_of(&*store, feed).await?.ok_or(ServeError::NotFound)?;
    let from = match (from, at) {
        (Some(from), _) => Some(from),
        (None, Some(at)) => Some(
            store
                .seq_at(&store_calls(), &feed.uid, at)
                .await
                .map_err(store_error)?
                .get(),
        ),
        (None, None) => None,
    };
    let limit = limit.unwrap_or(50).clamp(1, MAX_EVENTS_PAGE);
    let (oldest, end) = (head.oldest.get(), head.end().get());
    let (lo, hi) = match from {
        Some(from) => {
            let lo = from.clamp(oldest, end);
            (lo, lo.saturating_add(limit).min(end))
        }
        None => {
            let hi = before.unwrap_or(end).clamp(oldest, end);
            (hi.saturating_sub(limit).max(oldest), hi)
        }
    };
    let mut events = Vec::new();
    if hi > lo {
        let batch = store
            .read(&store_calls(), &feed.uid, Seq(lo), Seq(hi))
            .await
            .map_err(store_error)?;
        events = batch
            .events
            .into_iter()
            .rev()
            .map(|e| event_view(e, &head, feed, false))
            .collect();
    }
    Ok(EventsPage {
        events,
        oldest,
        watermark: head.watermark.get(),
        next_before: (lo > oldest).then_some(lo),
    })
}

/// One retained event with its payload (never a sealed one's).
pub async fn event(
    state: &ServerState,
    actor: &AuthContext,
    tenant: Option<&str>,
    name: &str,
    seq: u64,
) -> Result<EventView, ServeError> {
    let tenant = scope(actor, tenant)?;
    let found = find(state, tenant.as_deref(), name).await?;
    let feed = &found.feed;
    let store = state.feeds().store_for(feed).await.map_err(store_error)?;
    let head = head_of(&*store, feed).await?.ok_or(ServeError::NotFound)?;
    if seq < head.oldest.get() || seq > head.watermark.get() {
        return Err(ServeError::NotFound);
    }
    let batch = store
        .read(&store_calls(), &feed.uid, Seq(seq), Seq(seq + 1))
        .await
        .map_err(store_error)?;
    let e = batch
        .events
        .into_iter()
        .next()
        .ok_or(ServeError::NotFound)?;
    Ok(event_view(e, &head, feed, true))
}

/// The requests of a feed this server refused lately, newest first. Kept in
/// memory per server; payloads and credentials are never kept.
pub async fn refusals(
    state: &ServerState,
    actor: &AuthContext,
    tenant: Option<&str>,
    name: &str,
    limit: Option<usize>,
) -> Result<Vec<super::ingest::RefusalRecord>, ServeError> {
    let tenant = scope(actor, tenant)?;
    let found = find(state, tenant.as_deref(), name).await?;
    let mut all = state.feeds().ingest().refusals(found.feed.uid.as_str());
    all.truncate(limit.unwrap_or(super::ingest::REFUSALS_KEPT));
    Ok(all)
}

/// The definitions a listing reads: the server's snapshot, reloaded only
/// when it is older than the scheduler would let it get.
async fn listed(state: &ServerState) -> Result<Arc<super::Snapshot>, ServeError> {
    let rt = state.feeds();
    let held = rt.snapshot();
    if held.loaded() && held.age() <= super::REFRESH_INTERVAL {
        return Ok(held);
    }
    rt.refresh_now(state).await.map_err(ServeError::Unavailable)
}

/// The feeds `actor` sees for `tenant`.
pub async fn list(
    state: &ServerState,
    actor: &AuthContext,
    tenant: Option<&str>,
) -> Result<Vec<FeedView>, ServeError> {
    let all = Page {
        after: None,
        limit: Some(usize::MAX),
    };
    Ok(list_page(state, actor, tenant, &all).await?.0)
}

/// One page of the feeds `actor` sees for `tenant`, ordered by tenant and
/// name, and where the next page starts. `tenant` [`ALL_TENANTS`] lists the
/// server's feeds and every tenant's for a principal that is not confined
/// to one tenant.
pub async fn list_page(
    state: &ServerState,
    actor: &AuthContext,
    tenant: Option<&str>,
    page: &Page,
) -> Result<(Vec<FeedView>, Option<String>), ServeError> {
    let everyone = tenant == Some(ALL_TENANTS) && actor.tenant.is_none();
    let tenant = scope(actor, tenant.filter(|t| *t != ALL_TENANTS))?;
    let snapshot = listed(state).await?;
    let mut seen: Vec<Arc<Feed>> = snapshot
        .feeds
        .iter()
        .filter(|f| everyone || f.tenant() == tenant.as_deref())
        .filter(|f| page.after.as_ref().is_none_or(|after| f.key() > *after))
        .cloned()
        .collect();
    seen.sort_by_key(|f| f.key());
    let limit = page.limit.unwrap_or(DEFAULT_PAGE).max(1);
    let limit = if limit == usize::MAX {
        limit
    } else {
        limit.min(MAX_PAGE)
    };
    let more = seen.len() > limit;
    seen.truncate(limit);
    let next = more.then(|| seen.last().map(|f| f.key())).flatten();
    Ok((views(state, &seen).await?, next))
}

/// The delivery guarantee of each of `feed`'s deliveries, by name: its
/// pipeline loaded as a run would load it, with placeholders for sealed
/// params. Remembered per delivery and feed revision for a minute.
async fn guarantees(state: &ServerState, feed: &Feed) -> BTreeMap<String, String> {
    let rt = state.feeds();
    let mut out = BTreeMap::new();
    for d in &feed.spec.deliveries {
        let Some(dref) = feed.delivery(&d.name) else {
            continue;
        };
        let key = format!("{}@{}", dref.uid, feed.revision);
        let word = match rt.guarantee(&key) {
            Some(word) => word,
            None => {
                let word = match delivery_config(state, feed, d, dref).await {
                    Ok(loaded) => loaded
                        .nodes
                        .first()
                        .map(|n| n.delivery_guarantee.to_string())
                        .unwrap_or_else(|| "unknown".into()),
                    Err(_) => "unknown".into(),
                };
                rt.remember_guarantee(key, word.clone());
                word
            }
        };
        out.insert(d.name.clone(), word);
    }
    out
}

/// The delivery actions the audit log records and the feed's history shows.
const AUDITED_MOVES: &[&str] = &[
    "feed.pause",
    "feed.resume",
    "feed.deliver_now",
    "feed.replay",
    "feed.skip",
    "feed.recover",
];
/// Audit entries read for one feed's history.
const ACTIVITY_SCAN: usize = 2000;

/// What was done to `feed`'s deliveries (pause, resume, deliver now, replay,
/// skip, recover), newest first, from the audit log's most recent entries.
async fn activity(state: &ServerState, feed: &Feed) -> Vec<FeedChange> {
    let filter = crate::serve::history::AuditFilter {
        tenant: feed.tenant().map(str::to_string),
        result: Some("ok".into()),
        limit: ACTIVITY_SCAN,
        ..Default::default()
    };
    let Ok(entries) = state.history().list_audit(&filter).await else {
        return Vec::new();
    };
    let label = feed.label();
    let own = |target: &str| -> Option<String> {
        let target = target.strip_prefix("feed:").unwrap_or(target);
        let delivery = target.strip_prefix(label.as_str())?.strip_prefix('/')?;
        feed.delivery(delivery).map(|_| delivery.to_string())
    };
    entries
        .into_iter()
        .filter(|e| AUDITED_MOVES.contains(&e.action.as_str()))
        .filter(|e| e.tenant.as_deref() == feed.tenant())
        .filter_map(|e| {
            let delivery = e.target.as_deref().and_then(own)?;
            let mut detail = vec![format!("delivery {delivery}")];
            detail.extend(e.detail.filter(|d| !d.is_empty()));
            Some(FeedChange {
                at: e.timestamp,
                by: e.principal,
                action: e.action.trim_start_matches("feed.").to_string(),
                detail,
            })
        })
        .collect()
}

/// One feed with its gaps, tokens and change log.
pub async fn get(
    state: &ServerState,
    actor: &AuthContext,
    tenant: Option<&str>,
    name: &str,
) -> Result<FeedDetail, ServeError> {
    let tenant = scope(actor, tenant)?;
    let found = find(state, tenant.as_deref(), name).await?;
    let feed = &found.feed;
    let gaps = match state.feeds().store_for(feed).await {
        Ok(store) => store
            .gaps(&store_calls(), &feed.uid, None)
            .await
            .unwrap_or_default(),
        Err(_) => Vec::new(),
    };
    let gaps = gaps
        .into_iter()
        .map(|g| GapView {
            delivery: g
                .delivery
                .as_ref()
                .and_then(|d| feed.delivery_by_uid(d))
                .map(|(n, _)| n.to_string()),
            from: g.from.get(),
            to: g.to.get(),
            reason: g.reason,
            at: g.at,
            by: g.by,
        })
        .collect();
    let tokens = state
        .history()
        .feed_token_list(Some(feed.uid.as_str()))
        .await
        .map_err(ServeError::from)?;
    let mut view = view_of(state, feed).await?;
    let derived = guarantees(state, feed).await;
    let waiting = delivery_bytes(state, feed).await;
    for d in &mut view.deliveries {
        d.guarantee = derived.get(&d.name).cloned();
        d.undelivered_bytes = waiting.get(&d.name).copied();
    }
    Ok(FeedDetail {
        feed: view,
        gaps,
        tokens: tokens.iter().map(TokenView::from).collect(),
        hourly_24h: hourly_24h(state, feed).await,
        created_at: found.record.as_ref().map(|r| r.created_at),
        created_by: found.record.as_ref().map(|r| r.created_by.clone()),
        changes: {
            let mut all = found.record.map(|r| r.changes).unwrap_or_default();
            all.extend(activity(state, feed).await);
            all.sort_by_key(|a| std::cmp::Reverse(a.at));
            all
        },
    })
}

/// The config a delivery's runs load, for a check or to find its state.
pub(crate) async fn delivery_config(
    state: &ServerState,
    feed: &Feed,
    d: &DeliverySpec,
    dref: &DeliveryRef,
) -> Result<crate::serve::load::LoadedSubmission, ServeError> {
    use crate::runtime::server::{Submission, load};
    let actor = AuthContext {
        principal: format!("feed:{}", feed.name()),
        role: Role::Operator,
        source_ip: None,
        tenant: feed.tenant().map(str::to_string),
    };
    let suffix = dref.short_id();
    if let Some(t) = &d.template {
        use crate::serve::history::templates::VersionSelector;
        let history = state.history();
        let selector = |v: &Option<String>| -> Result<VersionSelector, ServeError> {
            match v {
                None => Ok(VersionSelector::default()),
                Some(s) => serde_json::from_value(serde_json::Value::String(s.clone()))
                    .map_err(|e| unprocessable(format!("template version '{s}': {e}"))),
            }
        };
        let map = crate::serve::handlers::templates::map_err;
        let want = crate::templates::resolve_version(&history, &t.id, selector(&t.version)?)
            .await
            .map_err(map)?;
        let declared = history
            .template_get(&t.id, Some(want))
            .await
            .map_err(ServeError::from)?
            .map(|r| r.params)
            .unwrap_or_default();
        let supplied: crate::params::SuppliedParams = t
            .params
            .iter()
            .map(|(k, v)| {
                let hidden = crate::serve::schedules::secrets::is_sealed(v)
                    || crate::serve::schedules::secrets::is_keep_marker(v);
                match declared.get(k).filter(|_| hidden) {
                    Some(p) => (k.clone(), p.placeholder()),
                    None => (k.clone(), v.clone()),
                }
            })
            .collect();
        let sink = crate::templates::SinkChoice {
            id: t.sink.clone(),
            version: selector(&t.sink_version)?,
            overlay: t
                .overlay
                .as_ref()
                .map(|o| crate::serve::handlers::templates::TriggerBody {
                    overlay: Some(crate::serve::handlers::templates::OverlayRef::Id(o.clone())),
                    overlay_version: selector(&t.overlay_version).ok(),
                    ..Default::default()
                })
                .and_then(|b| b.overlay_choice()),
            allow_untested: false,
        };
        let materialized = crate::templates::materialize_for_run_selected(
            &history,
            &t.id,
            want,
            &sink,
            &supplied,
            &Default::default(),
            crate::templates::Materialize::Persisted,
            None,
            None,
        )
        .await
        .map_err(map)?;
        let principal = crate::runtime::Principal::new(actor, crate::runtime::Origin::Trusted);
        return load(
            state,
            &principal,
            Submission::new(&materialized.body, crate::serve::load::ConfigFormat::Json)
                .delivering(Some(&suffix), true),
        )
        .await;
    }
    let pipeline = d
        .pipeline
        .as_ref()
        .ok_or_else(|| unprocessable("the delivery names no template and no pipeline"))?;
    let path = state.feeds().file().pipeline_path(pipeline);
    let text = tokio::fs::read_to_string(&path)
        .await
        .map_err(|e| unprocessable(format!("reading pipeline file {}: {e}", path.display())))?;
    let format = if path
        .extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("json"))
    {
        crate::serve::load::ConfigFormat::Json
    } else {
        crate::serve::load::ConfigFormat::Yaml
    };
    let principal = crate::runtime::Principal::new(actor, crate::runtime::Origin::ServerFile);
    load(
        state,
        &principal,
        Submission::new(&text, format).delivering(Some(&suffix), true),
    )
    .await
}

/// A delivery's pipeline must be exactly one root row that reads the feed.
pub fn check_rows(loaded: &crate::serve::load::LoadedSubmission) -> Result<(), String> {
    use crate::expand::NodeRole;
    if loaded.is_topology() {
        return Err("a delivery's pipeline cannot be a node graph (`pipeline.nodes`)".into());
    }
    let [node] = loaded.nodes.as_slice() else {
        return Err(format!(
            "a delivery's pipeline must have exactly one row, and this one has {}; a `feed` \
             source cannot be split by `partition`, `shard`, `fan_out` or a matrix",
            loaded.nodes.len()
        ));
    };
    if node.source.kind != crate::executor::FEED_KIND {
        return Err(format!(
            "a delivery's pipeline must read the feed (`source: {{type: feed}}`); row '{}' \
             reads `{}`",
            node.id, node.source.kind
        ));
    }
    if !matches!(node.role, NodeRole::Root) {
        return Err(format!(
            "row '{}' must be a root row: a `feed` source takes no `parent` and no `fan_out`",
            node.id
        ));
    }
    if node.cleanup_scope.is_some() {
        return Err(format!(
            "row '{}': a `feed` source cannot declare `complete_for`",
            node.id
        ));
    }
    if loaded.cfg.shard.is_some() {
        return Err("a delivery's pipeline cannot be sharded (`shard:`)".into());
    }
    for field in ["feed", "delivery", "store"] {
        if node.source.config.get(field).is_some_and(|v| !v.is_null()) {
            return Err(format!(
                "row '{}' sets the source's `{field}`; the server supplies it for a delivery",
                node.id
            ));
        }
    }
    Ok(())
}

/// Check that a delivery's template (or pipeline file) resolves and has the
/// one row a delivery runs.
pub async fn check_delivery(
    state: &ServerState,
    feed: &Feed,
    d: &DeliverySpec,
) -> Result<(), ServeError> {
    let dref = feed
        .delivery(&d.name)
        .ok_or_else(|| ServeError::Internal(format!("delivery '{}' has no id", d.name)))?;
    let loaded = delivery_config(state, feed, d, dref).await.map_err(|e| {
        unprocessable(format!(
            "delivery '{}': {}",
            d.name,
            e.api_error().error.message
        ))
    })?;
    check_rows(&loaded).map_err(|e| unprocessable(format!("delivery '{}': {e}", d.name)))
}

/// Check every `pipeline:` delivery of the `--feeds` file as a delivery made
/// through the API is checked: the file loads, and it is one root row that
/// reads the feed. `Err` names the feed and the delivery. A tenant feed
/// whose tenant does not exist yet is left to its first run.
pub async fn check_file_deliveries(state: &ServerState) -> Result<(), String> {
    let rt = state.feeds();
    for spec in &rt.file().feeds {
        if let Some(t) = &spec.tenant
            && !matches!(state.history().tenant_get(t).await, Ok(Some(_)))
        {
            tracing::warn!(feed = %spec.name, tenant = %t, "the feed's tenant does not exist yet: its pipeline files are checked when they first run");
            continue;
        }
        let uid = faucet_common_feed::FeedUid::generate();
        let feed = Feed {
            deliveries: spec
                .deliveries
                .iter()
                .map(|d| {
                    let uid = DeliveryUid::generate(uid.clone());
                    let meta = crate::serve::history::feeds::DeliveryMeta {
                        uid: uid.id().to_string(),
                        ..Default::default()
                    };
                    (d.name.clone(), DeliveryRef { uid, meta })
                })
                .collect(),
            uid,
            spec: spec.clone(),
            source: Source::File,
            approved: true,
            revision: 0,
            ensured: false,
            derived: Default::default(),
        };
        for d in spec.deliveries.iter().filter(|d| d.pipeline.is_some()) {
            check_delivery(state, &feed, d).await.map_err(|e| {
                format!("feeds: '{}': {}", feed.label(), e.api_error().error.message)
            })?;
        }
    }
    Ok(())
}

/// Screen a delivery's params as its creator's and seal the secret ones.
async fn prepare_delivery(
    state: &ServerState,
    actor: &AuthContext,
    record: &FeedRecord,
    d: &mut DeliverySpec,
    current: Option<&DeliverySpec>,
    presealed: bool,
) -> Result<(), ServeError> {
    let Some(t) = d.template.as_mut() else {
        return Ok(());
    };
    let uid = record
        .deliveries
        .get(&d.name)
        .map(|m| m.uid.clone())
        .ok_or_else(|| ServeError::Internal(format!("delivery '{}' has no id", d.name)))?;
    let history = state.history();
    let selector: Option<crate::serve::history::templates::VersionSelector> = t
        .version
        .as_ref()
        .and_then(|s| serde_json::from_value(serde_json::Value::String(s.clone())).ok());
    let secret: Vec<String> = match crate::templates::resolve_version(
        &history,
        &t.id,
        selector.unwrap_or_default(),
    )
    .await
    {
        Ok(v) => history
            .template_get(&t.id, Some(v))
            .await
            .map_err(ServeError::from)?
            .map(|r| {
                r.params
                    .iter()
                    .filter(|(_, p)| p.secret)
                    .map(|(k, _)| k.clone())
                    .collect()
            })
            .unwrap_or_default(),
        Err(e) => {
            return Err(unprocessable(format!(
                "delivery '{}': {}",
                d.name,
                crate::serve::handlers::templates::map_err(e)
                    .api_error()
                    .error
                    .message
            )));
        }
    };
    let body = crate::serve::handlers::templates::TriggerBody {
        params: t.params.clone(),
        version: selector,
        sink: t.sink.clone(),
        ..Default::default()
    };
    let principal = crate::runtime::Principal::caller(state, actor.clone())
        .on_behalf_of(record.tenant().map(str::to_string));
    crate::runtime::server::screen_template_params(state, &principal, &t.id, body).await?;
    let scope = secrets::Scope {
        feed: &record.uid,
        tenant: record.tenant(),
    };
    if presealed
        && let Some(k) = secret.iter().find(|k| {
            t.params.get(*k).is_some_and(|v| {
                !v.is_null()
                    && !crate::serve::schedules::secrets::is_keep_marker(v)
                    && !crate::serve::schedules::secrets::is_sealed(v)
            })
        })
    {
        return Err(unprocessable(format!(
            "delivery '{}': template.params.{k}: a change request carries secret values \
             sealed, never as sent",
            d.name
        )));
    }
    secrets::seal_params(
        state,
        t,
        &uid,
        &secret,
        current.and_then(|c| c.template.as_ref()),
        scope,
        presealed,
    )
    .map_err(|e| unprocessable(format!("delivery '{}': {e}", d.name)))
}

async fn tenant_limits(
    state: &ServerState,
    tenant: Option<&str>,
) -> Result<Option<crate::serve::history::tenants::TenantLimits>, ServeError> {
    let Some(t) = tenant else {
        return Ok(None);
    };
    let record = state
        .history()
        .tenant_get(t)
        .await
        .map_err(ServeError::from)?
        .ok_or_else(|| unprocessable(format!("tenant '{t}' does not exist")))?;
    Ok(Some(record.limits))
}

fn check_store(state: &ServerState, actor: &AuthContext, store: &str) -> Result<(), ServeError> {
    if store != spec::DEFAULT_STORE && (actor.role != Role::Admin || actor.tenant.is_some()) {
        return Err(ServeError::Forbidden(format!(
            "feed store '{store}' is a server resource: only a server admin may choose a store \
             other than `default`"
        )));
    }
    match state.feeds().stores().refusal(store) {
        Some(why) => Err(unprocessable(why)),
        None => Ok(()),
    }
}

async fn after_change(state: &ServerState) {
    let rt = state.feeds();
    rt.ensure_started(state);
    if let Err(e) = rt.refresh(state).await {
        tracing::warn!(error = %e, "could not reload feeds after a change");
    }
    rt.notify();
}

/// The ids a change request fixed when it was proposed: the feed's, and one
/// per delivery it adds. Secret values are sealed to them.
#[derive(Debug, Clone, Copy)]
struct Ids<'a> {
    feed: Option<&'a str>,
    deliveries: Option<&'a BTreeMap<String, String>>,
}

impl Ids<'_> {
    const FRESH: Ids<'static> = Ids {
        feed: None,
        deliveries: None,
    };

    fn delivery(&self, name: &str) -> Option<&str> {
        self.deliveries?.get(name).map(String::as_str)
    }
}

/// A feed ready to be created: validated, its secrets sealed.
struct BuiltCreate {
    record: FeedRecord,
    feed: Feed,
    limits: Option<crate::serve::history::tenants::TenantLimits>,
}

async fn build_create(
    state: &ServerState,
    actor: &AuthContext,
    tenant: Option<&str>,
    mut new: FeedSpec,
    ids: Ids<'_>,
    presealed: bool,
) -> Result<BuiltCreate, ServeError> {
    crate::runtime::server::refuse_while_unavailable(state)?;
    if !state.persistent_history() {
        return Err(unprocessable(super::stores::NO_DEFAULT_STORE));
    }
    let tenant = scope(actor, tenant)?;
    match (&new.tenant, &tenant) {
        (Some(a), Some(b)) if a != b => {
            return Err(ServeError::Forbidden(format!(
                "the feed names tenant '{a}', and this request is for tenant '{b}'"
            )));
        }
        (Some(a), None) => {
            return Err(unprocessable(format!(
                "create a tenant's feed under the tenant ('{a}'), not as a server feed"
            )));
        }
        _ => new.tenant = tenant.clone(),
    }
    spec::validate(&new, Source::Console).map_err(unprocessable)?;
    let rt = state.feeds();
    if rt.file_feed(tenant.as_deref(), &new.name).is_some() {
        return Err(ServeError::Conflict(format!(
            "the --feeds file already defines feed '{}'",
            new.name
        )));
    }
    check_store(state, actor, &new.store)?;
    let limits = tenant_limits(state, tenant.as_deref()).await?;
    if let (Some(t), Some(max)) = (&tenant, limits.as_ref().and_then(|l| l.max_feeds)) {
        let owned = state
            .history()
            .feed_list()
            .await
            .map_err(ServeError::from)?
            .iter()
            .filter(|r| r.live() && r.tenant() == Some(t.as_str()))
            .count()
            + rt.file()
                .feeds
                .iter()
                .filter(|f| f.tenant.as_deref() == Some(t.as_str()))
                .count();
        if owned as u64 >= u64::from(max) {
            return Err(unprocessable(format!(
                "tenant '{t}' already has {owned} feed(s), its limit (max_feeds: {max})"
            )));
        }
    }
    crate::runtime::server::require_member_capability(
        state,
        crate::serve::history::CAP_FEEDS,
        super::runner::FEEDS_REFUSAL,
    )
    .await?;
    let now = Utc::now();
    let mut record = FeedRecord::new(new, &actor.principal, now);
    if let Some(uid) = ids.feed {
        record.uid = uid.to_string();
    }
    for (name, meta) in record.deliveries.iter_mut() {
        if let Some(uid) = ids.delivery(name) {
            meta.uid = uid.to_string();
        }
    }
    let feed_scope = secrets::Scope {
        feed: &record.uid.clone(),
        tenant: tenant.as_deref(),
    };
    secrets::seal_signature(state, &mut record.spec, None, feed_scope, presealed)
        .map_err(unprocessable)?;
    let mut deliveries = record.spec.deliveries.clone();
    for d in &mut deliveries {
        prepare_delivery(state, actor, &record, d, None, presealed).await?;
    }
    record.spec.deliveries = deliveries;
    let feed = feed_of_record(state, &record).map_err(unprocessable)?;
    for d in &feed.spec.deliveries {
        check_delivery(state, &feed, d).await?;
    }
    Ok(BuiltCreate {
        record,
        feed,
        limits,
    })
}

async fn commit_create(
    state: &ServerState,
    built: BuiltCreate,
    approved: bool,
) -> Result<FeedView, ServeError> {
    let BuiltCreate {
        mut record,
        feed,
        limits,
    } = built;
    record.approved = approved;
    let feed = Feed { approved, ..feed };
    let store = state.feeds().store_for(&feed).await.map_err(store_error)?;
    let history = state.history();
    if let Some(old) = history
        .feed_get(&record.key())
        .await
        .map_err(ServeError::from)?
        && !old.live()
        && let Err(e) = finish_delete(state, &old).await
    {
        return Err(ServeError::Conflict(format!(
            "a deleted feed named '{}' is still being removed ({e}); retry",
            record.name()
        )));
    }
    if !history
        .feed_insert(&record)
        .await
        .map_err(ServeError::from)?
    {
        return Err(ServeError::Conflict(format!(
            "a feed named '{}' already exists",
            record.name()
        )));
    }
    if let Err(e) = ensure_in_store(&*store, &feed, limits.as_ref()).await {
        if let Err(undo) = state.history().feed_delete(&record.key()).await {
            tracing::error!(feed = %record.name(), error = %undo, "could not remove a feed whose store rows were not created");
        }
        return Err(store_error(e));
    }
    after_change(state).await;
    view_of(state, &feed).await
}

/// Create a feed. A tenant principal's feed belongs to its tenant.
pub async fn create(
    state: &ServerState,
    actor: &AuthContext,
    tenant: Option<&str>,
    new: FeedSpec,
) -> Result<FeedView, ServeError> {
    let built = build_create(state, actor, tenant, new, Ids::FRESH, false).await?;
    commit_create(state, built, false).await
}

/// Whether `detail` (a [`spec_diff`]) adds or changes a delivery: what makes
/// a feed start runs it did not start before.
fn touches_deliveries(detail: &[String]) -> bool {
    detail
        .iter()
        .any(|l| l.starts_with("delivery ") && (l.ends_with(" added") || l.ends_with(" changed")))
}

/// A stored feed's next revision: validated, its secrets sealed.
struct BuiltUpdate {
    current: FeedRecord,
    next: FeedRecord,
    dropped: Vec<(String, String)>,
    feed: Feed,
    old: Arc<Feed>,
    limits: Option<crate::serve::history::tenants::TenantLimits>,
    touched: bool,
    detail: Vec<String>,
}

async fn build_update(
    state: &ServerState,
    actor: &AuthContext,
    tenant: Option<&str>,
    name: &str,
    mut new: FeedSpec,
    ids: Ids<'_>,
    presealed: bool,
) -> Result<BuiltUpdate, ServeError> {
    crate::runtime::server::refuse_while_unavailable(state)?;
    let tenant = scope(actor, tenant)?;
    let found = find(state, tenant.as_deref(), name).await?;
    let current = read_only(&found)?.clone();
    if ids.feed.is_some_and(|uid| uid != current.uid) {
        return Err(ServeError::Conflict(format!(
            "feed '{name}' was replaced since this change was proposed"
        )));
    }
    new.tenant.clone_from(&current.spec.tenant);
    if new.name != current.spec.name {
        return Err(unprocessable(
            "a feed cannot be renamed; create a new feed instead",
        ));
    }
    if new.store != current.spec.store {
        return Err(unprocessable(
            "a feed cannot move to another store; create a new feed instead",
        ));
    }
    spec::validate(&new, Source::Console).map_err(unprocessable)?;
    let limits = tenant_limits(state, tenant.as_deref()).await?;
    let now = Utc::now();
    let (mut next, dropped) = current.revised(new, &actor.principal, now, "updated", Vec::new());
    for (dname, meta) in next.deliveries.iter_mut() {
        if !current.deliveries.contains_key(dname)
            && let Some(uid) = ids.delivery(dname)
        {
            meta.uid = uid.to_string();
        }
    }
    let feed_scope = secrets::Scope {
        feed: &current.uid,
        tenant: tenant.as_deref(),
    };
    secrets::seal_signature(
        state,
        &mut next.spec,
        Some(&current.spec),
        feed_scope,
        presealed,
    )
    .map_err(unprocessable)?;
    let mut deliveries = next.spec.deliveries.clone();
    for d in &mut deliveries {
        let before = current.spec.delivery(&d.name);
        prepare_delivery(state, actor, &next, d, before, presealed).await?;
    }
    next.spec.deliveries = deliveries;
    let detail = spec_diff(
        &secrets::masked(&current.spec),
        &secrets::masked(&next.spec),
    );
    let touched = touches_deliveries(&detail);
    if let Some(last) = next.changes.last_mut() {
        last.detail.clone_from(&detail);
    }
    let feed = feed_of_record(state, &next).map_err(unprocessable)?;
    for d in &feed.spec.deliveries {
        if current.spec.delivery(&d.name) != Some(d) {
            check_delivery(state, &feed, d).await?;
        }
    }
    Ok(BuiltUpdate {
        current,
        next,
        dropped,
        feed,
        old: found.feed,
        limits,
        touched,
        detail,
    })
}

/// Store a built update. `Ok(None)`: the feed changed meanwhile.
async fn commit_update(
    state: &ServerState,
    built: BuiltUpdate,
    approved: bool,
) -> Result<Option<FeedView>, ServeError> {
    let BuiltUpdate {
        current,
        mut next,
        dropped,
        feed,
        old,
        limits,
        touched,
        ..
    } = built;
    if touched {
        next.approved = approved;
    }
    let feed = Feed {
        approved: next.approved,
        ..feed
    };
    let store = state.feeds().store_for(&old).await.map_err(store_error)?;
    for (dropped_name, uid) in &dropped {
        refuse_live_claim(&*store, &old, dropped_name, uid).await?;
    }
    if !state
        .history()
        .feed_replace(&current, &next)
        .await
        .map_err(ServeError::from)?
    {
        return Ok(None);
    }
    ensure_in_store(&*store, &feed, limits.as_ref())
        .await
        .map_err(store_error)?;
    if let Err(e) = finish_removed(state, &next).await {
        tracing::warn!(feed = %feed.label(), error = %e, "the delivery is removed; removing what it left will be retried");
    }
    after_change(state).await;
    view_of(state, &feed).await.map(Some)
}

/// Replace a stored feed's definition. The name, the tenant and the store
/// stay; a delivery keeps its position by its name.
pub async fn update(
    state: &ServerState,
    actor: &AuthContext,
    tenant: Option<&str>,
    name: &str,
    new: FeedSpec,
) -> Result<FeedView, ServeError> {
    for _ in 0..REPLACE_ATTEMPTS {
        let built =
            build_update(state, actor, tenant, name, new.clone(), Ids::FRESH, false).await?;
        if let Some(view) = commit_update(state, built, false).await? {
            return Ok(view);
        }
    }
    Err(ServeError::Conflict(
        "the feed kept changing; retry the update".into(),
    ))
}

async fn refuse_live_claim(
    store: &dyn FeedStore,
    feed: &Feed,
    delivery: &str,
    uid: &str,
) -> Result<(), ServeError> {
    let head = head_of(store, feed).await?;
    let live = head
        .iter()
        .flat_map(|h| h.deliveries.iter())
        .find(|d| d.delivery.id() == uid)
        .and_then(|d| d.claim.as_ref());
    match live {
        Some(c) => Err(ServeError::Conflict(format!(
            "delivery '{delivery}' has a run in flight (claim {} held by {}); pause the \
             delivery and wait for its run to end, or cancel the run, then retry",
            c.id, c.owner
        ))),
        None => Ok(()),
    }
}

/// The deliveries of the feed (or just `delivery`) with a run in flight, for
/// a delete to refuse by name. Empty for a `--feeds` file feed, which a
/// delete refuses for its own reason.
pub async fn in_flight(
    state: &ServerState,
    actor: &AuthContext,
    tenant: Option<&str>,
    name: &str,
    delivery: Option<&str>,
) -> Result<Vec<(String, Option<String>)>, ServeError> {
    let tenant = scope(actor, tenant)?;
    let found = find(state, tenant.as_deref(), name).await?;
    if found.record.is_none() {
        return Ok(Vec::new());
    }
    let mut runs = runs_in_flight(state, &found.feed).await?;
    runs.retain(|(d, _)| delivery.is_none_or(|want| want == d));
    Ok(runs)
}

/// The deliveries of `feed` with a run in flight: `(delivery, run id)`. The
/// run id is `None` between a claim and its submit, and for a `faucet run`
/// drain.
pub async fn runs_in_flight(
    state: &ServerState,
    feed: &Feed,
) -> Result<Vec<(String, Option<String>)>, ServeError> {
    let store = state.feeds().store_for(feed).await.map_err(store_error)?;
    let head = head_of(&*store, feed).await?;
    let mut out = Vec::new();
    for (name, dref) in &feed.deliveries {
        let claimed = head
            .iter()
            .flat_map(|h| h.deliveries.iter())
            .any(|d| d.delivery == dref.uid && d.claim.is_some());
        if claimed {
            let run = status::read(&*state.history(), feed.uid.as_str(), dref.uid.id())
                .await
                .ok()
                .and_then(|s| s.claim.and_then(|c| c.run_id));
            out.push((name.clone(), run));
        }
    }
    Ok(out)
}

/// Why a delivery's state keys could not be located.
enum StateLookup {
    /// A dependency is down: the same lookup can succeed later.
    Retry(String),
    /// The delivery's config no longer loads.
    Unknown(String),
}

impl StateLookup {
    fn message(self) -> String {
        match self {
            Self::Retry(m) | Self::Unknown(m) => m,
        }
    }
}

/// Where a delivery's run keeps its bookmark: the state store and the key.
async fn state_target(
    state: &ServerState,
    feed: &Feed,
    d: &DeliverySpec,
    dref: &DeliveryRef,
) -> Result<Option<(Arc<dyn faucet_core::StateStore>, faucet_core::StateKey)>, StateLookup> {
    let loaded = delivery_config(state, feed, d, dref)
        .await
        .map_err(|e| match e {
            ServeError::Unavailable(m) => StateLookup::Retry(m),
            other => StateLookup::Unknown(other.api_error().error.message),
        })?;
    let Some(node) = loaded.nodes.first() else {
        return Ok(None);
    };
    let Some(spec) = &node.state else {
        return Ok(None);
    };
    let store = crate::state::build_state_store(spec)
        .await
        .map_err(|e| StateLookup::Retry(e.to_string()))?;
    let scope = loaded
        .tenant
        .as_ref()
        .map(|t| t.state_scope())
        .unwrap_or_default();
    let name = loaded.cfg.name.clone().unwrap_or_else(|| "serve".into());
    Ok(Some((store, scope.key(&name).with_row(node.id.as_str()))))
}

/// Remove the state keys of delivery `d`'s runs. `Err`: try again later.
/// A delivery whose config no longer loads has no findable keys: logged
/// and left.
async fn remove_state(
    state: &ServerState,
    feed: &Feed,
    d: &DeliverySpec,
    dref: &DeliveryRef,
) -> Result<(), String> {
    use faucet_core::KeySuffix;
    let target = match state_target(state, feed, d, dref).await {
        Ok(t) => t,
        Err(StateLookup::Retry(e)) => return Err(e),
        Err(StateLookup::Unknown(e)) => {
            tracing::warn!(feed = %feed.label(), delivery = %d.name, error = %e, "a removed delivery's config no longer loads; its state keys are left");
            return Ok(());
        }
    };
    let Some((store, key)) = target else {
        return Ok(());
    };
    let mut keys = vec![key.render()];
    for suffix in [
        KeySuffix::Sla,
        KeySuffix::Profiling,
        KeySuffix::RollbackIndex,
        KeySuffix::Status,
        KeySuffix::Lease,
    ] {
        keys.push(key.clone().with_suffix(suffix).render());
    }
    for k in keys {
        store.delete(&k).await.map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// The feed of `record` for a cleanup: without its signature when the
/// sealed secret no longer opens.
fn cleanup_feed(state: &ServerState, record: &FeedRecord) -> Result<Feed, String> {
    feed_of_record(state, record).or_else(|_| {
        let mut plain = record.clone();
        plain.spec.ingest.auth.signature = None;
        feed_of_record(state, &plain)
    })
}

/// `feed` as it was while it still had delivery `gone`.
fn feed_with(feed: &Feed, gone: &RemovedDelivery) -> Result<(Feed, DeliveryRef), String> {
    let dref = DeliveryRef {
        uid: DeliveryUid::new(feed.uid.clone(), gone.uid.clone()).map_err(|e| e.to_string())?,
        meta: crate::serve::history::feeds::DeliveryMeta {
            uid: gone.uid.clone(),
            ..Default::default()
        },
    };
    let mut was = feed.clone();
    was.spec.deliveries = vec![gone.spec.clone()];
    was.deliveries = BTreeMap::from([(gone.spec.name.clone(), dref.clone())]);
    Ok((was, dref))
}

/// Remove what one dropped delivery left: its runs' state keys, its store
/// position and its status.
async fn clear_removed(
    state: &ServerState,
    store: &dyn FeedStore,
    feed: &Feed,
    gone: &RemovedDelivery,
) -> Result<(), String> {
    let rt = state.feeds();
    let (was, dref) = feed_with(feed, gone)?;
    remove_state(state, &was, &gone.spec, &dref).await?;
    rt.stopped(StopAfter::State)?;
    store
        .delete_delivery(&store_calls(), &dref.uid)
        .await
        .map_err(|e| e.to_string())?;
    rt.stopped(StopAfter::Store)?;
    status::clear(&*state.history(), feed.uid.as_str(), &gone.uid).await?;
    if feed.delivery(&gone.spec.name).is_none() {
        metrics::forget_delivery(&feed.label(), &gone.spec.name);
    }
    Ok(())
}

/// Finish removing the deliveries `record` dropped, then clear the list.
/// Safe to repeat and to run on any server.
pub async fn finish_removed(state: &ServerState, record: &FeedRecord) -> Result<(), String> {
    if record.removed.is_empty() || !record.live() {
        return Ok(());
    }
    let rt = state.feeds();
    rt.stopped(StopAfter::Marked)?;
    let feed = cleanup_feed(state, record)?;
    let store = rt.store_for(&feed).await?;
    for gone in &record.removed {
        clear_removed(state, &*store, &feed, gone).await?;
    }
    rt.stopped(StopAfter::Statuses)?;
    let mut done = record.clone();
    done.removed.clear();
    state
        .history()
        .feed_replace(record, &done)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Finish deleting a feed marked `deleting`: its runs' state keys, its
/// events and positions, its statuses and warning count, then the record
/// with its tokens. Safe to repeat and to run on any server.
pub async fn finish_delete(state: &ServerState, record: &FeedRecord) -> Result<(), String> {
    if record.live() {
        return Ok(());
    }
    let rt = state.feeds();
    let history = state.history();
    rt.stopped(StopAfter::Marked)?;
    let feed = cleanup_feed(state, record)?;
    let store = rt.store_for(&feed).await?;
    for d in &feed.spec.deliveries {
        if let Some(dref) = feed.delivery(&d.name) {
            remove_state(state, &feed, d, dref).await?;
        }
    }
    for gone in &record.removed {
        let (was, dref) = feed_with(&feed, gone)?;
        remove_state(state, &was, &gone.spec, &dref).await?;
    }
    rt.stopped(StopAfter::State)?;
    store
        .purge_feed(&store_calls(), &feed.uid)
        .await
        .map_err(|e| e.to_string())?;
    rt.stopped(StopAfter::Store)?;
    let uids = record
        .deliveries
        .values()
        .map(|m| m.uid.as_str())
        .chain(record.removed.iter().map(|g| g.uid.as_str()));
    for uid in uids {
        status::clear(&*history, &record.uid, uid).await?;
    }
    super::notify::clear(&*history, &record.uid).await?;
    rt.stopped(StopAfter::Statuses)?;
    for token in history
        .feed_token_list(Some(&record.uid))
        .await
        .map_err(|e| e.to_string())?
    {
        rt.ingest().forget_token(&token.id);
    }
    history
        .feed_remove(record)
        .await
        .map_err(|e| e.to_string())?;
    let names: Vec<String> = feed.deliveries.keys().cloned().collect();
    metrics::forget_feed(&feed.label(), &names);
    rt.ingest().forget_feed(feed.uid.as_str());
    Ok(())
}

/// Mark `record` deleted. `Ok(None)`: it changed meanwhile.
async fn mark_deleting(
    state: &ServerState,
    record: &FeedRecord,
    by: &str,
) -> Result<Option<FeedRecord>, ServeError> {
    let marked = record.tombstoned(by, Utc::now());
    Ok(state
        .history()
        .feed_replace(record, &marked)
        .await
        .map_err(ServeError::from)?
        .then_some(marked))
}

async fn delete_inner(
    state: &ServerState,
    actor: &AuthContext,
    tenant: Option<&str>,
    name: &str,
    expect_uid: Option<&str>,
) -> Result<(), ServeError> {
    crate::runtime::server::refuse_while_unavailable(state)?;
    let tenant = scope(actor, tenant)?;
    let found = find(state, tenant.as_deref(), name).await?;
    let record = read_only(&found)?.clone();
    if expect_uid.is_some_and(|uid| uid != record.uid) {
        return Err(ServeError::Conflict(format!(
            "feed '{name}' was replaced since this change was proposed"
        )));
    }
    let feed = found.feed.clone();
    let store = state.feeds().store_for(&feed).await.map_err(store_error)?;
    for (dname, meta) in &record.deliveries {
        refuse_live_claim(&*store, &feed, dname, &meta.uid).await?;
    }
    let Some(marked) = mark_deleting(state, &record, &actor.principal).await? else {
        return Err(ServeError::Conflict(format!(
            "feed '{name}' changed while it was being deleted; retry"
        )));
    };
    if let Err(e) = finish_delete(state, &marked).await {
        tracing::warn!(feed = %feed.label(), error = %e, "the feed is deleted; removing what it left will be retried");
    }
    after_change(state).await;
    Ok(())
}

/// Delete a stored feed: its definition and tokens, its events, cursors and
/// gaps, and each delivery's status, metric series and state keys. Refused
/// while a delivery has a run in flight. The feed is gone for its users
/// once this returns; a removal that could not finish is finished by the
/// scheduler of any server.
pub async fn delete(
    state: &ServerState,
    actor: &AuthContext,
    tenant: Option<&str>,
    name: &str,
) -> Result<(), ServeError> {
    delete_inner(state, actor, tenant, name, None).await
}

// ── Changes ─────────────────────────────────────────────────────────────────

pub use crate::serve::schedules::service::ChangeAction;

/// What a feed change request (`kind: feed`) proposes.
#[derive(Debug, Clone, PartialEq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChangePayload {
    pub action: ChangeAction,
    pub name: String,
    /// The tenant the feed belongs to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    /// The definition, for `create` and `update`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec: Option<FeedSpec>,
    /// The feed's uid: set by the server when the change is proposed, never
    /// taken from a client. Secret values in `spec` are sealed to it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feed_uid: Option<String>,
    /// The id of each delivery the change adds; set by the server.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub delivery_uids: BTreeMap<String, String>,
    /// The change adds or edits a delivery, so the feed starts runs it did
    /// not start before; set by the server.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub starts_runs: bool,
}

impl ChangePayload {
    pub fn new(
        action: ChangeAction,
        tenant: Option<&str>,
        name: &str,
        spec: Option<FeedSpec>,
    ) -> Self {
        Self {
            action,
            name: name.to_string(),
            tenant: tenant.map(str::to_string),
            spec,
            feed_uid: None,
            delivery_uids: BTreeMap::new(),
            starts_runs: false,
        }
    }

    fn ids(&self) -> Ids<'_> {
        Ids {
            feed: self.feed_uid.as_deref(),
            deliveries: Some(&self.delivery_uids),
        }
    }
}

/// A planned change: what will be stored, and the row a reviewer sees.
enum Planned {
    Create(Box<BuiltCreate>),
    Update(Box<BuiltUpdate>),
    Delete,
}

fn spec_of(p: &ChangePayload) -> Result<&FeedSpec, ServeError> {
    let spec = p
        .spec
        .as_ref()
        .ok_or_else(|| unprocessable(format!("{} needs `spec`", p.action.as_str())))?;
    if spec.name != p.name {
        return Err(unprocessable("spec.name must equal name"));
    }
    Ok(spec)
}

async fn plan_inner(
    state: &ServerState,
    actor: &AuthContext,
    p: &ChangePayload,
    presealed: bool,
) -> Result<(Planned, serde_json::Value), ServeError> {
    let tenant = scope(actor, p.tenant.as_deref())?;
    let mut row = serde_json::json!({
        "feed": p.name,
        "tenant": tenant,
        "action": p.action.as_str(),
    });
    let planned = match p.action {
        ChangeAction::Create => {
            let spec = spec_of(p)?;
            if state
                .history()
                .feed_get(&feed_key(tenant.as_deref(), &p.name))
                .await
                .map_err(ServeError::from)?
                .is_some_and(|r| r.live())
            {
                return Err(ServeError::Conflict(format!(
                    "a feed named '{}' already exists",
                    p.name
                )));
            }
            let built = build_create(
                state,
                actor,
                tenant.as_deref(),
                spec.clone(),
                p.ids(),
                presealed,
            )
            .await?;
            row["spec"] = serde_json::to_value(secrets::masked(&built.record.spec))
                .map_err(|e| ServeError::Internal(e.to_string()))?;
            row["starts_runs"] = (!built.record.spec.deliveries.is_empty()).into();
            Planned::Create(Box::new(built))
        }
        ChangeAction::Update => {
            let spec = spec_of(p)?;
            let built = build_update(
                state,
                actor,
                tenant.as_deref(),
                &p.name,
                spec.clone(),
                p.ids(),
                presealed,
            )
            .await?;
            row["revision"] = built.current.revision.into();
            row["diff"] = built.detail.clone().into();
            row["spec"] = serde_json::to_value(secrets::masked(&built.next.spec))
                .map_err(|e| ServeError::Internal(e.to_string()))?;
            row["starts_runs"] = built.touched.into();
            Planned::Update(Box::new(built))
        }
        ChangeAction::Delete => {
            let found = find(state, tenant.as_deref(), &p.name).await?;
            let record = read_only(&found)?;
            if p.feed_uid.as_deref().is_some_and(|uid| uid != record.uid) {
                return Err(ServeError::Conflict(format!(
                    "feed '{}' was replaced since this change was proposed",
                    p.name
                )));
            }
            row["revision"] = record.revision.into();
            Planned::Delete
        }
    };
    Ok((planned, row))
}

/// Plan a change: validate it as `actor`, return the rows a reviewer sees.
/// The payload is the one [`prepare_change`] stored: its secret values are
/// sealed to the feed; a plain secret value is refused.
pub async fn plan(
    state: &ServerState,
    actor: &AuthContext,
    p: &ChangePayload,
) -> Result<Vec<serde_json::Value>, ServeError> {
    if let Some(sig) = p
        .spec
        .as_ref()
        .and_then(|s| s.ingest.auth.signature.as_ref())
        && !sig.secret.starts_with(secrets::SEALED_TEXT)
    {
        return Err(unprocessable(
            "ingest.auth.signature.secret: a change request carries secret values sealed, \
             never as sent",
        ));
    }
    let (_, row) = plan_inner(state, actor, p, true).await?;
    Ok(vec![row])
}

/// Whether a change makes the feed start runs it did not start before.
async fn starts_runs(
    state: &ServerState,
    actor: &AuthContext,
    p: &ChangePayload,
) -> Result<bool, ServeError> {
    let Some(spec) = &p.spec else {
        return Ok(false);
    };
    match p.action {
        ChangeAction::Create => Ok(!spec.deliveries.is_empty()),
        ChangeAction::Delete => Ok(false),
        ChangeAction::Update => {
            let tenant = scope(actor, p.tenant.as_deref())?;
            let found = find(state, tenant.as_deref(), &p.name).await?;
            let before = secrets::masked(&found.feed.spec);
            Ok(touches_deliveries(&spec_diff(
                &before,
                &secrets::masked(spec),
            )))
        }
    }
}

/// Prepare a feed change request's payload before it is stored: the server
/// names the feed and the deliveries it adds (whatever the client sent) and
/// seals its secret values to them, so the stored request never holds a
/// secret in the clear.
pub async fn prepare_change(
    state: &ServerState,
    actor: &AuthContext,
    payload: serde_json::Value,
) -> Result<serde_json::Value, ServeError> {
    let mut p: ChangePayload = serde_json::from_value(payload)
        .map_err(|e| ServeError::BadConfig(format!("feed change payload: {e}")))?;
    let tenant = scope(actor, p.tenant.as_deref())?;
    p.tenant.clone_from(&tenant);
    let stored = match p.action {
        ChangeAction::Create => None,
        ChangeAction::Update | ChangeAction::Delete => {
            let found = find(state, tenant.as_deref(), &p.name).await?;
            Some(read_only(&found)?.clone())
        }
    };
    p.feed_uid = Some(match &stored {
        Some(r) => r.uid.clone(),
        None => uuid::Uuid::new_v4().simple().to_string(),
    });
    p.delivery_uids = p
        .spec
        .iter()
        .flat_map(|s| s.deliveries.iter())
        .filter(|d| {
            stored
                .as_ref()
                .is_none_or(|r| !r.deliveries.contains_key(&d.name))
        })
        .map(|d| (d.name.clone(), uuid::Uuid::new_v4().simple().to_string()))
        .collect();
    p.starts_runs = starts_runs(state, actor, &p).await?;
    if p.spec.is_some() {
        p.spec = match plan_inner(state, actor, &p, false).await?.0 {
            Planned::Create(b) => Some(b.record.spec),
            Planned::Update(b) => Some(b.next.spec),
            Planned::Delete => None,
        };
    }
    serde_json::to_value(&p).map_err(|e| ServeError::Internal(e.to_string()))
}

/// The tenant that owns the feed a change payload is for: the requester's
/// own, else the one the payload names (an unscoped principal's route).
pub fn change_tenant(
    actor: &AuthContext,
    payload: &serde_json::Value,
) -> Result<Option<String>, ServeError> {
    let named = payload.get("tenant").and_then(serde_json::Value::as_str);
    scope(actor, named)
}

/// Apply a change as `actor`. Returns the feed's view after it (`None` for
/// a delete).
///
/// `approved` is true when an approved change request applies it: such a
/// feed's delivery runs need no further approval under
/// `--require-approval run`.
pub async fn apply(
    state: &ServerState,
    actor: &AuthContext,
    p: ChangePayload,
    approved: bool,
) -> Result<Option<FeedView>, ServeError> {
    if approved && p.feed_uid.is_none() {
        return Err(ServeError::Conflict(
            "the change names no feed; propose it again".into(),
        ));
    }
    let out = match plan_inner(state, actor, &p, approved).await?.0 {
        Planned::Create(b) => Some(commit_create(state, *b, approved).await?),
        Planned::Update(b) => {
            Some(commit_update(state, *b, approved).await?.ok_or_else(|| {
                ServeError::Conflict(format!("feed '{}' changed meanwhile", p.name))
            })?)
        }
        Planned::Delete => {
            delete_inner(
                state,
                actor,
                p.tenant.as_deref(),
                &p.name,
                p.feed_uid.as_deref(),
            )
            .await?;
            None
        }
    };
    if approved {
        crate::serve::audit::write_target_detail(
            state,
            actor,
            &format!("feed.{}", p.action.as_str()),
            format!("feed:{}", label_of(p.tenant.as_deref(), &p.name)),
            "an approved change request".into(),
            "ok",
        )
        .await;
    }
    Ok(out)
}

/// `<tenant>/<name>` for a tenant feed, else the name.
pub fn label_of(tenant: Option<&str>, name: &str) -> String {
    match tenant {
        Some(t) => format!("{t}/{name}"),
        None => name.to_string(),
    }
}

/// What a create / update / delete did.
#[derive(Debug)]
pub enum Changed {
    /// Applied now (`None` for a delete).
    Applied(Option<Box<FeedView>>),
    /// The server gates it: proposed as a change request instead.
    Pending(Box<crate::serve::changes::ChangeRequest>),
}

/// Whether `p` needs an approved change request on this server: always
/// under `--require-approval feed`, and under `--require-approval run` when
/// it adds or edits a delivery.
pub async fn gated(
    state: &ServerState,
    actor: &AuthContext,
    p: &ChangePayload,
) -> Result<bool, ServeError> {
    use crate::serve::changes::ChangeKind;
    if state.requires_approval(ChangeKind::Feed) {
        return Ok(true);
    }
    Ok(state.requires_approval(ChangeKind::Run) && starts_runs(state, actor, p).await?)
}

/// Create, update or delete a feed: applied now, or proposed as a change
/// request that applies it once approved.
pub async fn change(
    state: &ServerState,
    actor: &AuthContext,
    mut p: ChangePayload,
    reason: Option<String>,
) -> Result<Changed, ServeError> {
    p.tenant = scope(actor, p.tenant.as_deref())?;
    if gated(state, actor, &p).await? {
        let payload = serde_json::to_value(&p).map_err(|e| ServeError::Internal(e.to_string()))?;
        let change = crate::serve::changes::create(
            state,
            actor,
            crate::serve::changes::NewChange {
                kind: crate::serve::changes::ChangeKind::Feed,
                payload,
                reason,
                budget: None,
                trusted_config: false,
                caller_params: Default::default(),
            },
        )
        .await?;
        return Ok(Changed::Pending(Box::new(change)));
    }
    if p.action == ChangeAction::Update
        && let Some(spec) = p.spec.clone()
    {
        return update(state, actor, p.tenant.as_deref(), &p.name, spec)
            .await
            .map(|v| Changed::Applied(Some(Box::new(v))));
    }
    apply(state, actor, p, false)
        .await
        .map(|v| Changed::Applied(v.map(Box::new)))
}

/// A stored feed's definition with one delivery added or replaced, as an
/// update would send it.
pub async fn with_delivery(
    state: &ServerState,
    actor: &AuthContext,
    tenant: Option<&str>,
    name: &str,
    delivery: DeliverySpec,
) -> Result<FeedSpec, ServeError> {
    let scoped = scope(actor, tenant)?;
    let found = find(state, scoped.as_deref(), name).await?;
    let mut next = secrets::masked(&read_only(&found)?.spec);
    match next.deliveries.iter_mut().find(|d| d.name == delivery.name) {
        Some(slot) => *slot = delivery,
        None => next.deliveries.push(delivery),
    }
    Ok(next)
}

/// A stored feed's definition without one delivery.
pub async fn without_delivery(
    state: &ServerState,
    actor: &AuthContext,
    tenant: Option<&str>,
    name: &str,
    delivery: &str,
) -> Result<FeedSpec, ServeError> {
    let scoped = scope(actor, tenant)?;
    let found = find(state, scoped.as_deref(), name).await?;
    let mut next = secrets::masked(&read_only(&found)?.spec);
    let before = next.deliveries.len();
    next.deliveries.retain(|d| d.name != delivery);
    if next.deliveries.len() == before {
        return Err(ServeError::NotFound);
    }
    Ok(next)
}

/// Add a delivery to a stored feed, or replace the one with its name.
pub async fn set_delivery(
    state: &ServerState,
    actor: &AuthContext,
    tenant: Option<&str>,
    name: &str,
    delivery: DeliverySpec,
) -> Result<FeedView, ServeError> {
    let next = with_delivery(state, actor, tenant, name, delivery).await?;
    update(state, actor, tenant, name, next).await
}

/// Remove a delivery from a stored feed.
pub async fn delete_delivery(
    state: &ServerState,
    actor: &AuthContext,
    tenant: Option<&str>,
    name: &str,
    delivery: &str,
) -> Result<FeedView, ServeError> {
    let next = without_delivery(state, actor, tenant, name, delivery).await?;
    update(state, actor, tenant, name, next).await
}

/// A delivery of a found feed.
struct Target {
    feed: Arc<Feed>,
    delivery: DeliverySpec,
    dref: DeliveryRef,
    store: Arc<dyn FeedStore>,
}

async fn target(
    state: &ServerState,
    actor: &AuthContext,
    tenant: Option<&str>,
    name: &str,
    delivery: &str,
) -> Result<Target, ServeError> {
    let tenant = scope(actor, tenant)?;
    let found = find(state, tenant.as_deref(), name).await?;
    let feed = found.feed;
    let d = feed
        .spec
        .delivery(delivery)
        .cloned()
        .ok_or(ServeError::NotFound)?;
    let dref = feed
        .delivery(delivery)
        .cloned()
        .ok_or(ServeError::NotFound)?;
    let store = state.feeds().store_for(&feed).await.map_err(store_error)?;
    Ok(Target {
        feed,
        delivery: d,
        dref,
        store,
    })
}

impl Target {
    async fn update_status(
        &self,
        state: &ServerState,
        change: impl Fn(&mut DeliveryStatus) -> bool,
    ) -> Result<DeliveryStatus, ServeError> {
        let s = status::update(
            &*state.history(),
            self.feed.uid.as_str(),
            self.dref.uid.id(),
            change,
        )
        .await
        .map_err(ServeError::Unavailable)?;
        state.feeds().notify();
        Ok(s)
    }

    async fn head(&self) -> Result<(Head, faucet_common_feed::DeliveryHead), ServeError> {
        let head = head_of(&*self.store, &self.feed)
            .await?
            .ok_or(ServeError::NotFound)?;
        let dh = head
            .deliveries
            .iter()
            .find(|d| d.delivery == self.dref.uid)
            .cloned()
            .ok_or(ServeError::NotFound)?;
        Ok((head, dh))
    }
}

/// Pause a delivery: it keeps its position and starts no runs.
pub async fn pause(
    state: &ServerState,
    actor: &AuthContext,
    tenant: Option<&str>,
    name: &str,
    delivery: &str,
    reason: Option<String>,
) -> Result<(), ServeError> {
    crate::runtime::server::refuse_while_unavailable(state)?;
    let t = target(state, actor, tenant, name, delivery).await?;
    let now = Utc::now();
    let by = actor.principal.clone();
    t.update_status(state, |s| {
        s.paused = Some(status::Pause {
            at: now,
            by: by.clone(),
            reason: reason.clone(),
        });
        true
    })
    .await
    .map(|_| ())
}

/// Resume a delivery and clear what kept it from running: a pause, a stall
/// and its failure counts. A `paused: true` in the definition has to be
/// changed there.
pub async fn resume(
    state: &ServerState,
    actor: &AuthContext,
    tenant: Option<&str>,
    name: &str,
    delivery: &str,
) -> Result<(), ServeError> {
    crate::runtime::server::refuse_while_unavailable(state)?;
    let t = target(state, actor, tenant, name, delivery).await?;
    if t.delivery.paused {
        return Err(ServeError::Conflict(format!(
            "delivery '{delivery}' is paused in its definition (`paused: true`); change the \
             definition to resume it"
        )));
    }
    if let Some(why) = super::runner::approval_missing(state, &t.feed, &t.delivery) {
        return Err(ServeError::Conflict(format!(
            "delivery '{delivery}' cannot be resumed: {why}"
        )));
    }
    t.update_status(state, |s| {
        if s.health == Health::Held {
            s.paused = None;
            return true;
        }
        s.paused = None;
        s.reset();
        true
    })
    .await
    .map(|_| ())
}

/// Start a run for whatever is undelivered, without waiting for the flush
/// policy.
pub async fn deliver_now(
    state: &ServerState,
    actor: &AuthContext,
    tenant: Option<&str>,
    name: &str,
    delivery: &str,
) -> Result<(), ServeError> {
    crate::runtime::server::refuse_while_unavailable(state)?;
    let t = target(state, actor, tenant, name, delivery).await?;
    t.update_status(state, |s| {
        s.deliver_now = true;
        s.backoff_until = None;
        true
    })
    .await
    .map(|_| ())
}

/// Where a replay starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Position {
    Seq(u64),
    Time(DateTime<Utc>),
}

/// What a cursor move did.
#[derive(Debug, Clone, Serialize)]
pub struct Moved {
    pub from: u64,
    pub to: u64,
    pub epoch: u64,
}

fn claim_live(e: faucet_common_feed::FeedError) -> ServeError {
    match e {
        faucet_common_feed::FeedError::ClaimLive { .. } => ServeError::Conflict(
            "the delivery has a run in flight; pause it and wait for the run to end, then retry"
                .into(),
        ),
        faucet_common_feed::FeedError::CursorConflict { .. } => {
            ServeError::Conflict("the delivery moved meanwhile; retry".into())
        }
        e @ (faucet_common_feed::FeedError::OutOfRange { .. }
        | faucet_common_feed::FeedError::PastWatermark { .. }) => unprocessable(e.to_string()),
        other => store_error(other),
    }
}

/// Move a delivery back to deliver retained events again.
pub async fn replay(
    state: &ServerState,
    actor: &AuthContext,
    tenant: Option<&str>,
    name: &str,
    delivery: &str,
    from: Position,
) -> Result<Moved, ServeError> {
    crate::runtime::server::refuse_while_unavailable(state)?;
    let t = target(state, actor, tenant, name, delivery).await?;
    let (_, dh) = t.head().await?;
    let calls = store_calls();
    let to = match from {
        Position::Seq(n) => Seq(n),
        Position::Time(at) => t
            .store
            .seq_at(&calls, &t.feed.uid, at)
            .await
            .map_err(store_error)?,
    };
    if to > dh.cursor {
        return Err(unprocessable(format!(
            "a replay goes back: event {to} is after the delivery's position ({}); use skip to \
             move forward",
            dh.cursor
        )));
    }
    let epoch = t
        .store
        .move_cursor(&calls, &t.dref.uid, dh.cursor, to, Move::Replay)
        .await
        .map_err(claim_live)?;
    t.update_status(state, |s| {
        s.reset();
        true
    })
    .await?;
    Ok(Moved {
        from: dh.cursor.get(),
        to: to.get(),
        epoch: epoch.0,
    })
}

/// If the delivery's stored bookmark says the sink already holds a page past
/// the cursor, commit the cursor to it, so a skip or a recover starts from
/// what was really delivered and that page is never read again.
async fn settle_bookmark(state: &ServerState, t: &Target) -> Result<(), ServeError> {
    let Some((states, key)) = state_target(state, &t.feed, &t.delivery, &t.dref)
        .await
        .map_err(|e| unprocessable(e.message()))?
    else {
        return Ok(());
    };
    let Some(value) = states
        .get(&key.render())
        .await
        .map_err(|e| ServeError::Unavailable(format!("reading the delivery's state: {e}")))?
    else {
        return Ok(());
    };
    let (Some(bookmark), _) = crate::pipeline_state::ops::decode_bookmark(&value) else {
        return Ok(());
    };
    let Ok(b) = serde_json::from_value::<faucet_source_feed::FeedBookmark>(bookmark) else {
        return Ok(());
    };
    let (head, dh) = t.head().await?;
    if b.feed != t.feed.uid.as_str()
        || b.delivery != t.dref.uid.id()
        || b.epoch != dh.epoch.0
        || b.next <= dh.cursor.get()
        || Seq(b.next) > head.end()
    {
        return Ok(());
    }
    let calls = store_calls();
    let last = t
        .store
        .read(&calls, &t.feed.uid, Seq(b.next - 1), Seq(b.next))
        .await
        .map_err(store_error)?;
    if last.events.first().map(|e| e.id.as_str()) != Some(b.id.as_str()) {
        return Ok(());
    }
    let owner = ClaimOwner::new(format!("serve:settle:{}", uuid::Uuid::new_v4().simple()))
        .map_err(store_error)?;
    let claim = t
        .store
        .claim(
            &calls,
            &t.dref.uid,
            dh.cursor,
            Seq(b.next),
            &owner,
            std::time::Duration::from_secs(60),
        )
        .await
        .map_err(store_error)?
        .ok_or_else(|| {
            ServeError::Conflict(
                "the delivery has a run in flight; pause it and wait for the run to end".into(),
            )
        })?;
    let holder = t.store.attach(&calls, &claim.id).await.map_err(store_error);
    let committed = match holder {
        Ok(h) => t
            .store
            .commit(&calls, &h, dh.cursor, Seq(b.next))
            .await
            .map_err(store_error),
        Err(e) => Err(e),
    };
    if let Err(e) = t.store.release(&calls, &claim.id).await {
        tracing::warn!(error = %e, "releasing the bookmark-settling claim failed; it ends by its time limit");
    }
    committed
}

/// Move a delivery forward past events that will not be delivered, leaving
/// a gap that names them. `to` is the next event to read; default: one
/// event past the position.
pub async fn skip(
    state: &ServerState,
    actor: &AuthContext,
    tenant: Option<&str>,
    name: &str,
    delivery: &str,
    to: Option<u64>,
    reason: &str,
) -> Result<Moved, ServeError> {
    crate::runtime::server::refuse_while_unavailable(state)?;
    if reason.trim().is_empty() {
        return Err(unprocessable(
            "a skip needs a reason: it is kept with the gap it leaves",
        ));
    }
    let t = target(state, actor, tenant, name, delivery).await?;
    settle_bookmark(state, &t).await?;
    let (head, dh) = t.head().await?;
    let to = Seq(to.unwrap_or(dh.cursor.get() + 1));
    if to <= dh.cursor {
        return Err(unprocessable(format!(
            "a skip goes forward: event {to} is not after the delivery's position ({}); use \
             replay to go back",
            dh.cursor
        )));
    }
    if to > head.end() {
        return Err(unprocessable(format!(
            "event {to} is past the feed's end ({})",
            head.end()
        )));
    }
    let epoch = t
        .store
        .move_cursor(
            &store_calls(),
            &t.dref.uid,
            dh.cursor,
            to,
            Move::Skip {
                reason: reason.to_string(),
                by: actor.principal.clone(),
            },
        )
        .await
        .map_err(claim_live)?;
    metrics::gap(&t.feed.label());
    t.update_status(state, |s| {
        s.reset();
        true
    })
    .await?;
    Ok(Moved {
        from: dh.cursor.get(),
        to: to.get(),
        epoch: epoch.0,
    })
}

/// Put a held delivery back on a valid position after its store was
/// restored: the cursor is kept within the feed, the epoch moves on and a
/// gap is recorded.
pub async fn recover(
    state: &ServerState,
    actor: &AuthContext,
    tenant: Option<&str>,
    name: &str,
    delivery: &str,
) -> Result<Moved, ServeError> {
    crate::runtime::server::refuse_while_unavailable(state)?;
    let t = target(state, actor, tenant, name, delivery).await?;
    settle_bookmark(state, &t).await?;
    let (head, dh) = t.head().await?;
    let to = dh.cursor.min(head.end()).max(head.oldest);
    let epoch = t
        .store
        .move_cursor(&store_calls(), &t.dref.uid, dh.cursor, to, Move::Recover)
        .await
        .map_err(claim_live)?;
    metrics::gap(&t.feed.label());
    t.update_status(state, |s| {
        s.reset();
        true
    })
    .await?;
    let _ = actor;
    Ok(Moved {
        from: dh.cursor.get(),
        to: to.get(),
        epoch: epoch.0,
    })
}

/// The SHA-256 of a token, lowercase hex: what the history stores and ingest
/// looks up.
pub fn token_hash(token: &str) -> String {
    use sha2::Digest;
    sha2::Sha256::digest(token.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// A new token's text: `fdt_` and 64 hex characters.
fn new_token() -> String {
    format!(
        "fdt_{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}

/// Issue a feed token. The text is returned once and never stored.
pub async fn token_create(
    state: &ServerState,
    actor: &AuthContext,
    tenant: Option<&str>,
    name: &str,
    token_name: &str,
    expires_at: Option<DateTime<Utc>>,
) -> Result<(TokenView, String), ServeError> {
    crate::runtime::server::refuse_while_unavailable(state)?;
    let tenant = scope(actor, tenant)?;
    let found = find(state, tenant.as_deref(), name).await?;
    if found.feed.spec.ingest.auth.tokens.is_none() {
        return Err(unprocessable(format!(
            "feed '{name}' does not use tokens (`ingest.auth.tokens: managed`)"
        )));
    }
    crate::serve::history::tenants::validate_slug(token_name, "token name", spec::MAX_NAME_LEN)
        .map_err(unprocessable)?;
    let now = Utc::now();
    if expires_at.is_some_and(|t| t <= now) {
        return Err(unprocessable("expires_at is in the past"));
    }
    let text = new_token();
    crate::secrets::registry::register_server(&text);
    let record = FeedTokenRecord {
        id: uuid::Uuid::new_v4().simple().to_string(),
        feed: found.feed.uid.as_str().to_string(),
        name: token_name.to_string(),
        hash: token_hash(&text),
        prefix: text.chars().take(10).collect(),
        created_at: now,
        created_by: actor.principal.clone(),
        expires_at,
        revoked_at: None,
        revoked_by: None,
        last_used_at: None,
    };
    if !state
        .history()
        .feed_token_insert(&record)
        .await
        .map_err(ServeError::from)?
    {
        return Err(ServeError::Conflict("the token id is taken; retry".into()));
    }
    after_change(state).await;
    Ok((TokenView::from(&record), text))
}

/// A feed's tokens.
pub async fn token_list(
    state: &ServerState,
    actor: &AuthContext,
    tenant: Option<&str>,
    name: &str,
) -> Result<Vec<TokenView>, ServeError> {
    let tenant = scope(actor, tenant)?;
    let found = find(state, tenant.as_deref(), name).await?;
    Ok(state
        .history()
        .feed_token_list(Some(found.feed.uid.as_str()))
        .await
        .map_err(ServeError::from)?
        .iter()
        .map(TokenView::from)
        .collect())
}

/// Revoke a token: every server stops accepting it within one refresh.
pub async fn token_revoke(
    state: &ServerState,
    actor: &AuthContext,
    tenant: Option<&str>,
    name: &str,
    id: &str,
) -> Result<TokenView, ServeError> {
    crate::runtime::server::refuse_while_unavailable(state)?;
    let tenant = scope(actor, tenant)?;
    let found = find(state, tenant.as_deref(), name).await?;
    let history = state.history();
    for _ in 0..REPLACE_ATTEMPTS {
        let current = history
            .feed_token_list(Some(found.feed.uid.as_str()))
            .await
            .map_err(ServeError::from)?
            .into_iter()
            .find(|t| t.id == id)
            .ok_or(ServeError::NotFound)?;
        if current.revoked_at.is_some() {
            return Ok(TokenView::from(&current));
        }
        let mut next = current.clone();
        next.revoked_at = Some(Utc::now());
        next.revoked_by = Some(actor.principal.clone());
        if history
            .feed_token_replace(&current, &next)
            .await
            .map_err(ServeError::from)?
        {
            after_change(state).await;
            return Ok(TokenView::from(&next));
        }
    }
    Err(ServeError::Conflict(
        "the token kept changing; retry".into(),
    ))
}

/// Pause the delivery a run delivered for (a rollback of that run), and say
/// what resuming it will do. `None` when the feed or delivery is gone.
pub async fn pause_for_rollback(
    state: &ServerState,
    actor: &AuthContext,
    run_id: &str,
    binding: &RunFeedBinding,
) -> Option<String> {
    let rt = state.feeds();
    let feed = match rt.snapshot().by_uid(&binding.feed) {
        Some(f) => f,
        None => rt.refresh(state).await.ok()?.by_uid(&binding.feed)?,
    };
    let (name, dref) = feed
        .deliveries
        .iter()
        .find(|(_, d)| d.uid.id() == binding.delivery)
        .map(|(n, d)| (n.clone(), d.clone()))?;
    let now = Utc::now();
    let by = actor.principal.clone();
    let reason = format!("its run {run_id} was rolled back");
    let paused = status::update(&*state.history(), feed.uid.as_str(), dref.uid.id(), |s| {
        s.paused = Some(status::Pause {
            at: now,
            by: by.clone(),
            reason: Some(reason.clone()),
        });
        true
    })
    .await;
    if let Err(e) = paused {
        return Some(format!(
            "the delivery '{name}' of feed '{}' could NOT be paused ({e}); pause it before \
             anything else is delivered",
            feed.label()
        ));
    }
    rt.notify();
    crate::serve::audit::write_target_detail(
        state,
        actor,
        "feed.pause",
        format!("feed:{}/{name}", feed.label()),
        reason,
        "ok",
    )
    .await;
    let cursor = match rt.store_for(&feed).await {
        Ok(store) => head_of(&*store, &feed)
            .await
            .ok()
            .flatten()
            .and_then(|h| h.deliveries.into_iter().find(|d| d.delivery == dref.uid))
            .map(|d| d.cursor.get()),
        Err(_) => None,
    };
    Some(rollback_note(&feed.label(), &name, binding, cursor))
}

/// What a rollback report says about the paused delivery.
pub fn rollback_note(
    feed: &str,
    delivery: &str,
    b: &RunFeedBinding,
    cursor: Option<u64>,
) -> String {
    let paused = format!("delivery '{delivery}' of feed '{feed}' is paused");
    match cursor {
        Some(c) if c < b.to => format!(
            "{paused}. The run did not move the delivery past its events ({}..{}), so resuming \
             the delivery redelivers these events; no replay is needed",
            b.from,
            b.to.saturating_sub(1)
        ),
        Some(_) => format!(
            "{paused}. The feed still holds the run's events: replay the delivery from event \
             {} to deliver them again, then resume it. A replay delivers every later run's \
             events again too",
            b.from
        ),
        None => format!(
            "{paused}. The feed store could not be read: check the delivery's position before \
             resuming; if it is past event {}, replay from event {}",
            b.to.saturating_sub(1),
            b.from
        ),
    }
}

/// Delete every feed of a tenant that is being deleted: each is marked
/// deleted first, so no server sets it up again, then removed as a deleted
/// feed is. Returns how many were removed; `Err` leaves the rest marked for
/// any server's scheduler, and a repeat of the call, to finish.
pub async fn purge_tenant(state: &ServerState, tenant: &str) -> Result<usize, String> {
    let history = state.history();
    let records = history.feed_list().await.map_err(|e| e.to_string())?;
    let mut marked = Vec::new();
    for record in records.into_iter().filter(|r| r.tenant() == Some(tenant)) {
        if !record.live() {
            marked.push(record);
            continue;
        }
        let by = format!("tenant:{tenant}:deleted");
        match mark_deleting(state, &record, &by).await {
            Ok(Some(m)) => marked.push(m),
            Ok(None) => return Err(format!("feed '{}' changed; retry", record.name())),
            Err(e) => return Err(e.api_error().error.message),
        }
    }
    let mut purged = 0;
    let mut failed = None;
    for record in &marked {
        match finish_delete(state, record).await {
            Ok(()) => purged += 1,
            Err(e) => failed = Some(format!("feed '{}': {e}", record.name())),
        }
    }
    if !marked.is_empty() {
        after_change(state).await;
    }
    match failed {
        Some(e) => Err(e),
        None => Ok(purged),
    }
}
