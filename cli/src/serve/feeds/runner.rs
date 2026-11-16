//! The feed scheduler (#1034). Every server runs one loop; the feed store's
//! range claim picks one winner per range.
//!
//! Each second: one `heads` query per store covers every feed. A delivery
//! whose flush policy is met and that is not paused, stalled, held or backing
//! off gets a claim over `[cursor, to)`, and the winner submits a run bound
//! to it through `crate::runtime::server`. While a claim is live any server
//! renews it every 5 seconds as long as its run is queued or running, and
//! releases it and records the outcome once the run is over. A claim nobody
//! submitted a run for, and every claim a `faucet run` drain owns, ends only
//! by its own release or its time limit. While the run history is
//! unreachable the scheduler holds: it takes and releases nothing.

use super::status::{self, DeliveryStatus, Health, LiveClaim, Outcome};
use super::{Feed, FeedsRuntime, Readiness, Snapshot, Source, metrics, store_calls};
use crate::serve::error::ServeError;
use crate::serve::history::feeds::{DeliverySpec, RunFeedBinding};
use crate::serve::history::tenants::TenantLimits;
use crate::serve::history::{ListFilter, RunRecord, RunStatus, labels::LabelFilter};
use crate::serve::state::ServerState;
use chrono::{DateTime, Utc};
use faucet_common_feed::{
    BatchLimits, ClaimId, DeliveryHead, DeliveryUid, FeedBinding, FeedStore, Head, Seq,
    StoreLimits,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

/// How often the loop looks at every feed.
pub const TICK: Duration = Duration::from_secs(1);
/// How long a range claim lasts without a renewal.
pub const CLAIM_TTL: Duration = Duration::from_secs(300);
/// How often a live claim's run is looked at.
pub const CLAIM_CHECK: Duration = Duration::from_secs(5);
/// How often retention runs.
pub const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(60);
/// Most rows one retention call removes.
pub const MAINTENANCE_ROWS: u32 = 1_000;
/// Most retention calls per feed in one pass.
const MAINTENANCE_PASSES: usize = 10;
/// Deliveries stepped side by side.
pub const CONCURRENT_STEPS: usize = 16;
/// The longest one delivery's step may take.
pub const STEP_TIMEOUT: Duration = Duration::from_secs(120);

/// The run label naming the feed a run delivers for.
pub const LABEL_FEED: &str = "feed";
/// The run label naming the delivery.
pub const LABEL_DELIVERY: &str = "feed_delivery";
/// The run label carrying the range claim a run reads under.
pub const LABEL_CLAIM: &str = "feed_claim";

/// The refusal while a cluster member does not run feeds.
pub const FEEDS_REFUSAL: &str = "feeds unavailable: a cluster member is built without feeds or \
     predates them; finish the rolling upgrade, then retry";

/// The principal a delivery's cancelled or rolled-back run pauses it as.
pub const SCHEDULER: &str = "feed-scheduler";

/// What the loop does with one delivery this tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// Nothing to do.
    Idle,
    /// Something in the shared status keeps it from running; look again.
    Blocked,
    /// A claim is live (or recorded): look at its run.
    CheckClaim,
    /// The flush policy is met. `bytes_only`: only the byte limit may be,
    /// which the store has to confirm.
    Claim { bytes_only: bool },
}

/// The shortest gap between two runs of `d`.
pub fn min_interval(d: &DeliverySpec, tenant_feed: bool, tenant_floor: Duration) -> Duration {
    let own = d.flush.min_interval.duration();
    if tenant_feed {
        own.max(tenant_floor)
    } else {
        own
    }
}

/// The most one run of `d` may read: its `max_batch` within the tenant's
/// per-run ceilings (three quarters of the byte ceiling, which leaves room
/// for the `_feed` metadata each record carries).
pub fn effective_batch(d: &DeliverySpec, tenant: Option<&TenantLimits>) -> (u64, u64) {
    let mut events = d.max_batch.events;
    let mut bytes = d.max_batch.bytes.bytes();
    if let Some(t) = tenant {
        if let Some(r) = t.max_records_per_run {
            events = events.min(r);
        }
        if let Some(b) = t.max_bytes_per_run {
            bytes = bytes.min((b / 4).saturating_mul(3).max(1));
        }
    }
    (events.max(1), bytes.max(1))
}

/// Decide what to do with a delivery from the store's view (`head`, `dh`)
/// and the last status this server read. Pure.
pub fn decide(
    d: &DeliverySpec,
    head: &Head,
    dh: &DeliveryHead,
    status: &DeliveryStatus,
    gap: Duration,
    now: DateTime<Utc>,
) -> Decision {
    if dh.claim.is_some() || status.claim.is_some() {
        return Decision::CheckClaim;
    }
    let pending = dh.cursor.distance_to(head.end());
    if pending == 0 {
        return Decision::Idle;
    }
    if d.paused || status.paused.is_some() || !status.health.runs() || status.backing_off(now) {
        return Decision::Blocked;
    }
    if status.deliver_now {
        return Decision::Claim { bytes_only: false };
    }
    let by_events = pending >= d.flush.max_events;
    let by_age = dh.oldest_undelivered_at.is_some_and(|t| {
        (head.now - t).num_seconds() >= d.flush.max_age.secs().min(i64::MAX as u64) as i64
    });
    let maybe_bytes = head.bytes >= d.flush.max_bytes.bytes();
    if !(by_events || by_age || maybe_bytes) {
        return Decision::Idle;
    }
    let gap = chrono::Duration::from_std(gap).unwrap_or_default();
    if status.last_submit_at.is_some_and(|t| t + gap > now) {
        return Decision::Idle;
    }
    Decision::Claim {
        bytes_only: !(by_events || by_age),
    }
}

/// Why a submit was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// Passes with time: back off, do not count.
    Transient(String),
    /// A retry cannot fix it: counts like a failed run.
    Permanent(String),
}

/// Sort a submission error.
pub fn classify(e: ServeError) -> Refusal {
    match e {
        ServeError::QueueFull { .. } => Refusal::Transient("the run queue is full".into()),
        ServeError::Draining => Refusal::Transient("the server is shutting down".into()),
        ServeError::TooManyRequests(m) => Refusal::Transient(m),
        e @ ServeError::Unavailable(_) => Refusal::Transient(e.api_error().error.message),
        ServeError::NotFound => Refusal::Permanent("the template was not found".into()),
        other => Refusal::Permanent(other.api_error().error.message),
    }
}

/// How a finished run ended for its delivery. `moved`: the cursor is past
/// the claim's start.
pub fn outcome_of(rec: &RunRecord, moved: bool) -> Outcome {
    if moved {
        return Outcome::Delivered;
    }
    let mut text = rec.error.clone().unwrap_or_default();
    for inv in &rec.invocations {
        if let Some(e) = &inv.error {
            text.push_str("; ");
            text.push_str(e);
        }
    }
    let text = text.trim_start_matches("; ").to_string();
    if rec.status == RunStatus::Cancelled {
        return Outcome::Cancelled;
    }
    use crate::serve::history::FeedFailure;
    let said = |kind: FeedFailure| rec.invocations.iter().any(|i| i.feed_error == Some(kind));
    if said(FeedFailure::ClaimLost) {
        return Outcome::ClaimLost;
    }
    if said(FeedFailure::Held) {
        return Outcome::Held(text);
    }
    match rec.status {
        RunStatus::Completed => {
            Outcome::Failed("the run completed without delivering its events".into())
        }
        _ if text.is_empty() => Outcome::Failed(format!("the run {}", rec.status.as_str())),
        _ => Outcome::Failed(text),
    }
}

/// The refusal of a delivery whose runs would start without an approval
/// this server requires: `--require-approval run` covers a feed made through
/// the API unless an approved change request made its deliveries, and then
/// only while they name pinned template versions.
pub fn approval_missing(state: &ServerState, feed: &Feed, d: &DeliverySpec) -> Option<String> {
    if !state.requires_approval(crate::serve::changes::ChangeKind::Run)
        || feed.source == Source::File
    {
        return None;
    }
    if !feed.approved {
        return Some(
            "this server requires approval for runs (--require-approval run), and this feed's \
             deliveries were not made through an approved change request; propose an update \
             with a change request (kind `feed`) to approve them"
                .to_string(),
        );
    }
    d.template
        .as_ref()
        .and_then(|t| t.follows_a_channel())
        .map(|field| {
            format!(
                "this server requires approval for runs (--require-approval run), and this \
                 delivery follows a channel ({field}) that a later launch can move without an \
                 approval; pin a version and approve the change"
            )
        })
}

/// The idempotency key of the run for one claim.
pub fn idempotency_key(binding: &FeedBinding) -> String {
    format!(
        "feed:{}:{}:{}",
        binding.feed,
        binding.delivery.id(),
        binding.claim
    )
}

fn actor(feed: &Feed) -> crate::serve::rbac::AuthContext {
    crate::serve::rbac::AuthContext {
        principal: format!("feed:{}", feed.name()),
        role: crate::serve::rbac::Role::Operator,
        source_ip: None,
        tenant: feed.tenant().map(str::to_string),
    }
}

fn run_labels(feed: &Feed, d: &DeliverySpec, claim: &ClaimId) -> BTreeMap<String, String> {
    let mut labels = d.labels.clone();
    labels.insert(LABEL_FEED.into(), feed.label());
    labels.insert(LABEL_DELIVERY.into(), d.name.clone());
    labels.insert(LABEL_CLAIM.into(), claim.as_str().to_string());
    labels
}

/// Submit the run for `binding`. `Ok` is the run's id.
pub async fn submit(
    state: &ServerState,
    rt: &FeedsRuntime,
    feed: &Feed,
    d: &DeliverySpec,
    binding: &FeedBinding,
) -> Result<String, Refusal> {
    crate::runtime::server::require_member_capability(
        state,
        crate::serve::history::CAP_FEEDS,
        FEEDS_REFUSAL,
    )
    .await
    .map_err(classify)?;
    let suffix = super::short_id(binding.delivery.id());
    let name = format!("{}:{}", feed.name(), d.name);
    let labels = run_labels(feed, d, &binding.claim);
    let key = idempotency_key(binding);
    let from_file = feed.source == Source::File;
    if let Some(template) = &d.template {
        use crate::serve::history::templates::VersionSelector;
        let scope = super::secrets::Scope {
            feed: feed.uid.as_str(),
            tenant: feed.tenant(),
        };
        let template = super::secrets::opened_params(state, template, binding.delivery.id(), scope)
            .map_err(Refusal::Permanent)?;
        let selector =
            |field: &str, v: &Option<String>| -> Result<Option<VersionSelector>, Refusal> {
                v.as_ref()
                    .map(|s| {
                        serde_json::from_value(serde_json::Value::String(s.clone()))
                            .map_err(|e| Refusal::Permanent(format!("template {field} '{s}': {e}")))
                    })
                    .transpose()
            };
        let body = crate::serve::handlers::templates::TriggerBody {
            params: template.params.clone(),
            version: selector("version", &template.version)?,
            sink: template.sink.clone(),
            sink_version: selector("sink_version", &template.sink_version)?,
            overlay: template
                .overlay
                .as_ref()
                .map(|o| crate::serve::handlers::templates::OverlayRef::Id(o.clone())),
            overlay_version: selector("overlay_version", &template.overlay_version)?,
            name: Some(name),
            labels,
            timeout_secs: Some(d.timeout_secs),
            idempotency_key: Some(key),
            pool: d.pool.clone(),
            standing_approval: true,
            operator_params: from_file,
            server_key: true,
            pool_by_operator: feed.tenant().is_none(),
            feed: Some(RunFeedBinding::of(binding)),
            pipeline_name_suffix: Some(suffix),
            ..Default::default()
        };
        let principal =
            crate::runtime::Principal::new(actor(feed), crate::runtime::Origin::Trusted);
        use crate::runtime::server::{TriggerOutcome, trigger_template};
        return match trigger_template(state.clone(), principal, template.id.clone(), body).await {
            Ok(TriggerOutcome::Run(r)) => Ok(r.run.run_id),
            Ok(TriggerOutcome::PendingApproval(c)) => Err(Refusal::Permanent(format!(
                "the run waits for approval (change request {})",
                c.id
            ))),
            Err(e) => Err(classify(e)),
        };
    }
    let Some(pipeline) = &d.pipeline else {
        return Err(Refusal::Permanent(
            "the delivery names no template and no pipeline".into(),
        ));
    };
    let path = rt.file().pipeline_path(pipeline);
    let config = tokio::fs::read_to_string(&path).await.map_err(|e| {
        Refusal::Permanent(format!("reading pipeline file {}: {e}", path.display()))
    })?;
    let json = path
        .extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("json"));
    let req = crate::serve::runner::SubmitRequest {
        config,
        config_format: if json {
            crate::serve::runner::ConfigFormatWire::Json
        } else {
            crate::serve::runner::ConfigFormatWire::Yaml
        },
        name: Some(name),
        labels,
        timeout_secs: Some(d.timeout_secs),
        doctor_first: false,
        idempotency_key: Some(key),
        clock: None,
        concurrency: None,
        callback: None,
        require_approval: false,
        reason: None,
        budget: None,
        approved_change: None,
        selection: None,
        depends_on: None,
        pool: d.pool.clone(),
        trusted_config: true,
        server_file: true,
        sealed_params: None,
        caller_fields: Default::default(),
        server_key: true,
        feed: Some(RunFeedBinding::of(binding)),
        pipeline_name_suffix: Some(suffix),
    };
    let principal = crate::runtime::Principal::new(actor(feed), crate::runtime::Origin::ServerFile);
    match crate::runtime::server::submit(
        state,
        &principal,
        req,
        crate::runtime::server::Approval::Standing,
    )
    .await
    {
        Ok(crate::serve::runner::SubmitOutcome::Accepted(run)) => Ok(run.run_id),
        Ok(crate::serve::runner::SubmitOutcome::PendingApproval(c)) => Err(Refusal::Permanent(
            format!("the run waits for approval (change request {})", c.id),
        )),
        Err(e) => Err(classify(e)),
    }
}

/// The run that carries `claim`'s label, if one was submitted.
async fn run_of_claim(
    state: &ServerState,
    feed: &Feed,
    claim: &str,
) -> Result<Option<RunRecord>, String> {
    let filter = ListFilter {
        tenant: feed.tenant().map(str::to_string),
        labels: vec![LabelFilter {
            key: LABEL_CLAIM.into(),
            value: Some(claim.to_string()),
        }],
        limit: 1,
        ..Default::default()
    };
    let history = state.history();
    let page = history.list(&filter).await.map_err(|e| e.to_string())?;
    if history.degraded() {
        return Err("the run history is unreachable".into());
    }
    Ok(page.runs.into_iter().next())
}

/// The delivery ids `feed`'s stored definition lists right now; `None`
/// when it cannot be read or names another feed.
async fn defined_ids(state: &ServerState, feed: &Feed) -> Option<HashSet<String>> {
    let history = state.history();
    let ids: HashSet<String> = match feed.source {
        Source::File => {
            let raw = history
                .meta_get(&super::file_ids_key(&feed.key()))
                .await
                .ok()??;
            let ids: super::FileIds = serde_json::from_str(&raw).ok()?;
            if ids.uid != feed.uid.as_str() {
                return None;
            }
            ids.deliveries.into_values().collect()
        }
        Source::Console => {
            let record = history.feed_get(&feed.key()).await.ok()??;
            if record.uid != feed.uid.as_str() {
                return None;
            }
            record.deliveries.values().map(|m| m.uid.clone()).collect()
        }
    };
    (!history.degraded()).then_some(ids)
}

/// Delete the store's delivery rows that no definition lists and no claim
/// holds: what a change leaves when it stops between dropping a delivery's
/// id and deleting its row. Returns how many were deleted.
pub async fn drop_orphans(
    state: &ServerState,
    store: &dyn FeedStore,
    feed: &Feed,
    head: &Head,
) -> usize {
    let known: HashSet<&str> = feed.deliveries.values().map(|d| d.uid.id()).collect();
    let orphans: Vec<&DeliveryHead> = head
        .deliveries
        .iter()
        .filter(|d| d.claim.is_none() && !known.contains(d.delivery.id()))
        .collect();
    if orphans.is_empty() {
        return 0;
    }
    let Some(defined) = defined_ids(state, feed).await else {
        return 0;
    };
    let mut deleted = 0;
    for orphan in orphans {
        if defined.contains(orphan.delivery.id()) {
            continue;
        }
        match store.delete_delivery(&store_calls(), &orphan.delivery).await {
            Ok(()) => {
                status::forget(&*state.history(), feed.uid.as_str(), orphan.delivery.id()).await;
                metrics::orphan_delivery(&feed.label());
                tracing::warn!(feed = %feed.label(), delivery_uid = orphan.delivery.id(), cursor = orphan.cursor.get(), "deleted a delivery position no definition lists");
                deleted += 1;
            }
            Err(e) => {
                tracing::warn!(feed = %feed.label(), delivery_uid = orphan.delivery.id(), error = %e, "could not delete a delivery position no definition lists; retrying")
            }
        }
    }
    deleted
}

/// One delivery as a step sees it.
struct Work {
    feed: Arc<Feed>,
    delivery: DeliverySpec,
    uid: DeliveryUid,
    head: Head,
    dh: DeliveryHead,
    store: Arc<dyn FeedStore>,
    tenant_limits: Option<TenantLimits>,
    gap: Duration,
}

impl Work {
    fn feed_uid(&self) -> &str {
        self.feed.uid.as_str()
    }

    async fn update(
        &self,
        state: &ServerState,
        change: impl Fn(&mut DeliveryStatus) -> bool,
    ) -> Result<DeliveryStatus, String> {
        status::update(&*state.history(), self.feed_uid(), self.uid.id(), change).await
    }

    async fn release(&self, claim: &str) {
        let Ok(id) = ClaimId::new(claim) else {
            return;
        };
        if let Err(e) = self.store.release(&store_calls(), &id).await {
            tracing::warn!(feed = %self.feed.label(), delivery = %self.delivery.name, claim, error = %e, "releasing a feed claim failed; it ends by its time limit");
        }
    }
}

/// Take a claim and submit its run. Returns the delivery's status as last
/// stored, or `None` when the history could not be read (hold).
async fn claim_step(
    state: &ServerState,
    rt: &FeedsRuntime,
    w: &Work,
    bytes_only: bool,
) -> Option<DeliveryStatus> {
    let now = Utc::now();
    let fresh = status::read(&*state.history(), w.feed_uid(), w.uid.id())
        .await
        .ok()?;
    let Decision::Claim { .. } = decide(&w.delivery, &w.head, &w.dh, &fresh, w.gap, now) else {
        return Some(fresh);
    };
    if let Some(why) = approval_missing(state, &w.feed, &w.delivery) {
        if fresh.needs_approval.as_deref() != Some(why.as_str()) {
            metrics::run(&w.feed.label(), &w.delivery.name, "needs_approval");
        }
        return w
            .update(state, |s| {
                s.needs_approval = Some(why.clone());
                true
            })
            .await
            .ok();
    }
    let calls = store_calls();
    let cursor = w.dh.cursor;
    if bytes_only {
        let fits = w
            .store
            .bound(
                &calls,
                &w.feed.uid,
                cursor,
                BatchLimits::new(u64::MAX, w.delivery.flush.max_bytes.bytes()),
            )
            .await
            .ok()?;
        if fits >= w.head.end() {
            return Some(fresh);
        }
    }
    let (max_events, max_bytes) = effective_batch(&w.delivery, w.tenant_limits.as_ref());
    let limits = BatchLimits::new(fresh.batch(max_events), max_bytes);
    let to = w
        .store
        .bound(&calls, &w.feed.uid, cursor, limits)
        .await
        .ok()?;
    if to <= cursor {
        return Some(fresh);
    }
    let claim = match w
        .store
        .claim(&calls, &w.uid, cursor, to, rt.owner(), CLAIM_TTL)
        .await
    {
        Ok(Some(c)) => c,
        Ok(None) => return Some(fresh),
        Err(e) => {
            tracing::warn!(feed = %w.feed.label(), delivery = %w.delivery.name, error = %e, "claiming a feed range failed");
            return Some(fresh);
        }
    };
    let live = LiveClaim {
        id: claim.id.as_str().to_string(),
        from: claim.from.get(),
        to: claim.to.get(),
        run_id: None,
        claimed_at: now,
    };
    let recorded = w
        .update(state, |s| {
            s.claimed(live.clone());
            s.needs_approval = None;
            true
        })
        .await;
    if recorded.is_err() {
        w.release(&live.id).await;
        return None;
    }
    let binding = FeedBinding::new(
        w.uid.clone(),
        claim.epoch,
        claim.id.clone(),
        claim.from,
        claim.to,
        w.feed.spec.store.clone(),
    );
    match submit(state, rt, &w.feed, &w.delivery, &binding).await {
        Ok(run_id) => {
            tracing::info!(feed = %w.feed.label(), delivery = %w.delivery.name, run_id = %run_id, from = live.from, to = live.to, "feed delivery run submitted");
            w.update(state, |s| {
                s.submitted(&live.id, &run_id, now);
                true
            })
            .await
            .ok()
        }
        Err(refusal) => {
            w.release(&live.id).await;
            let outcome = match refusal {
                Refusal::Transient(m) => Outcome::Transient(m),
                Refusal::Permanent(m) => Outcome::Failed(m),
            };
            finish(state, w, &live, &outcome).await
        }
    }
}

/// Record how `live` ended, once across every server, and act on it.
async fn finish(
    state: &ServerState,
    w: &Work,
    live: &LiveClaim,
    outcome: &Outcome,
) -> Option<DeliveryStatus> {
    let now = Utc::now();
    let (max_events, _) = effective_batch(&w.delivery, w.tenant_limits.as_ref());
    let first = std::sync::atomic::AtomicBool::new(false);
    let stalled_now = std::sync::atomic::AtomicBool::new(false);
    let stored = w
        .update(state, |s| {
            let already = s.last_run.as_ref().is_some_and(|r| r.claim == live.id);
            let was = s.health;
            let changed = s.finish(live, outcome, max_events, w.delivery.retries, now);
            first.store(changed && !already, std::sync::atomic::Ordering::Release);
            stalled_now.store(
                changed && !already && was != Health::Stalled && s.health == Health::Stalled,
                std::sync::atomic::Ordering::Release,
            );
            if changed && !already && matches!(outcome, Outcome::Cancelled) {
                s.paused = Some(status::Pause {
                    at: now,
                    by: SCHEDULER.to_string(),
                    reason: Some(match &live.run_id {
                        Some(id) => format!("its run {id} was cancelled"),
                        None => "its run was cancelled".to_string(),
                    }),
                });
            }
            changed
        })
        .await
        .ok()?;
    if stalled_now.load(std::sync::atomic::Ordering::Acquire) {
        super::notify::spawn(
            state,
            w.feed.clone(),
            super::notify::FeedEvent::Stalled {
                delivery: w.delivery.name.clone(),
                seq: live.from,
                error: stored.last_error.clone().unwrap_or_default(),
            },
        );
    }
    if first.load(std::sync::atomic::Ordering::Acquire) {
        let (feed, delivery) = (w.feed.label(), w.delivery.name.as_str());
        metrics::run(&feed, delivery, outcome.as_str());
        match outcome {
            Outcome::Failed(e) | Outcome::Held(e) => {
                tracing::warn!(feed = %feed, delivery, health = stored.health.as_str(), batch = stored.batch(max_events), error = %e, "feed delivery run did not deliver")
            }
            Outcome::Cancelled => {
                crate::serve::audit::write_target_detail(
                    state,
                    &crate::serve::rbac::AuthContext::system(SCHEDULER),
                    "feed.pause",
                    format!("feed:{feed}/{delivery}"),
                    "its run was cancelled".into(),
                    "ok",
                )
                .await;
            }
            _ => {}
        }
    }
    Some(stored)
}

/// Look at the run of a live (or recorded) claim: renew the claim while the
/// run is going, release it and record the outcome once it is over.
async fn check_step(state: &ServerState, rt: &FeedsRuntime, w: &Work) -> Option<DeliveryStatus> {
    let history = state.history();
    let stored = status::read(&*history, w.feed_uid(), w.uid.id())
        .await
        .ok()?;
    let in_store = w.dh.claim.as_ref();
    let live = match (&stored.claim, in_store) {
        (Some(sc), Some(c)) if sc.id == c.id.as_str() => sc.clone(),
        (_, Some(c)) => {
            if !c.owner.as_str().starts_with("serve:") {
                return Some(stored);
            }
            LiveClaim {
                id: c.id.as_str().to_string(),
                from: w.dh.cursor.get(),
                to: c.to.get(),
                run_id: None,
                claimed_at: Utc::now(),
            }
        }
        (Some(sc), None) => sc.clone(),
        (None, None) => return Some(stored),
    };
    let claim_live = in_store.is_some_and(|c| c.id.as_str() == live.id);
    let rec = match &live.run_id {
        Some(id) => match history.get(id).await {
            Ok(r) => {
                if history.degraded() {
                    return None;
                }
                Some(r)
            }
            Err(_) => return None,
        },
        None => run_of_claim(state, &w.feed, &live.id).await.ok()?.map(Some),
    };
    let Some(rec) = rec else {
        // No run was ever submitted for it. A live claim ends by its own
        // release or its time limit; a recorded one that is gone is cleared.
        if claim_live {
            return Some(stored);
        }
        return finish(state, w, &live, &Outcome::ClaimLost).await;
    };
    if let Some(r) = rec.as_ref().filter(|r| !r.status.is_terminal()) {
        if claim_live
            && let Ok(id) = ClaimId::new(live.id.clone())
            && let Err(e) = w.store.renew_claim(&store_calls(), &id, CLAIM_TTL).await
        {
            tracing::warn!(feed = %w.feed.label(), delivery = %w.delivery.name, error = %e, "renewing a feed claim failed");
        }
        if stored
            .claim
            .as_ref()
            .is_none_or(|c| c.id != live.id || c.run_id.is_none())
        {
            let (run_id, at) = (r.run_id.clone(), r.submitted_at);
            return w
                .update(state, |s| {
                    if s.claim.as_ref().is_none_or(|c| c.id != live.id) {
                        s.claimed(live.clone());
                    }
                    s.submitted(&live.id, &run_id, at);
                    true
                })
                .await
                .ok();
        }
        return Some(stored);
    }
    // The run is over (or its record is gone): where is the cursor now?
    let head = w
        .store
        .heads(&store_calls(), std::slice::from_ref(&w.feed.uid))
        .await
        .ok()?;
    let cursor = head
        .iter()
        .flat_map(|h| h.deliveries.iter())
        .find(|d| d.delivery == w.uid)
        .map(|d| d.cursor)?;
    let moved = cursor > Seq(live.from);
    let mut live = live;
    let outcome = match &rec {
        Some(r) => {
            live.run_id = Some(r.run_id.clone());
            outcome_of(r, moved)
        }
        None if moved => Outcome::Delivered,
        None => Outcome::ClaimLost,
    };
    let after = finish(state, w, &live, &outcome).await?;
    if claim_live {
        w.release(&live.id).await;
    }
    let _ = rt;
    Some(after)
}

/// What this server remembers of a delivery between ticks.
#[derive(Debug, Clone, Default)]
struct Local {
    status: DeliveryStatus,
    read_at: Option<Instant>,
    checked_at: Option<Instant>,
}

/// The scheduler's state on one server.
#[derive(Default)]
pub struct Scheduler {
    local: HashMap<String, Local>,
    reload_at: Option<Instant>,
    maintain_at: Option<Instant>,
    warned: HashMap<String, usize>,
    /// Feeds and deliveries this server is creating in the store.
    setting_up: HashSet<String>,
    /// The limits this server last wrote to each feed's store row.
    limits: HashMap<String, StoreLimits>,
}

/// What one tick did, for tests and logs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TickReport {
    pub held: bool,
    pub store_ok: bool,
    pub feeds: usize,
    pub claims: usize,
    pub checks: usize,
}

enum Step {
    Claim { bytes_only: bool },
    Check,
    Read,
}

impl Scheduler {
    /// Forget what this server cached: a feed changed here.
    pub fn invalidate(&mut self) {
        self.reload_at = None;
        for l in self.local.values_mut() {
            l.read_at = None;
            l.checked_at = None;
        }
    }

    /// One pass over every feed.
    pub async fn tick(&mut self, state: &ServerState) -> TickReport {
        let rt = state.feeds();
        let mut report = TickReport::default();
        if state.history().degraded() {
            report.held = true;
            return report;
        }
        if self.reload_at.is_none_or(|t| t <= Instant::now()) {
            match rt.refresh(state).await {
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(error = %e, "could not reload feeds; keeping the current set");
                    if state.history().degraded() {
                        report.held = true;
                        return report;
                    }
                }
            }
            self.reload_at = Some(Instant::now() + super::REFRESH_INTERVAL);
        }
        let snapshot = rt.snapshot();
        report.feeds = snapshot.feeds.len();
        report.store_ok = true;
        let mut by_store: BTreeMap<&str, Vec<&Arc<Feed>>> = BTreeMap::new();
        for f in snapshot.feeds.iter().filter(|f| f.ensured) {
            by_store.entry(f.spec.store.as_str()).or_default().push(f);
        }
        let now = Utc::now();
        let mut work: Vec<(Work, Step)> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        let mut maintain: Vec<(Arc<dyn FeedStore>, Arc<Feed>)> = Vec::new();
        let mut crossings: Vec<(Arc<Feed>, usize, f64)> = Vec::new();
        for (name, feeds) in by_store {
            let store = match rt.stores().get(name).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(store = name, error = %e, "feed store unavailable");
                    report.store_ok = false;
                    continue;
                }
            };
            let uids: Vec<_> = feeds.iter().map(|f| f.uid.clone()).collect();
            let heads = match store.heads(&store_calls(), &uids).await {
                Ok(h) => h,
                Err(e) => {
                    tracing::warn!(store = name, error = %e, "reading feed heads failed");
                    report.store_ok = false;
                    continue;
                }
            };
            let heads: HashMap<_, _> = heads.into_iter().map(|h| (h.feed.clone(), h)).collect();
            for feed in feeds {
                let Some(head) = heads.get(&feed.uid) else {
                    let limits = feed.tenant().and_then(|t| snapshot.tenant_limits.get(t));
                    self.set_up(&*store, feed, None, limits).await;
                    continue;
                };
                maintain.push((store.clone(), (*feed).clone()));
                drop_orphans(state, &*store, feed, head).await;
                if let Some((crossed, ratio)) = self.observe(feed, head) {
                    crossings.push(((*feed).clone(), crossed, ratio));
                }
                let limits = feed
                    .tenant()
                    .and_then(|t| snapshot.tenant_limits.get(t))
                    .cloned();
                self.apply_limits(&*store, feed, limits.as_ref()).await;
                for d in &feed.spec.deliveries {
                    let Some(dref) = feed.delivery(&d.name) else {
                        continue;
                    };
                    let Some(dh) = head.deliveries.iter().find(|x| x.delivery == dref.uid) else {
                        self.set_up(&*store, feed, Some(d), limits.as_ref()).await;
                        continue;
                    };
                    let key = dref.uid.to_string();
                    seen.insert(key.clone());
                    let local = self.local.entry(key).or_default();
                    let gap = min_interval(d, feed.tenant().is_some(), rt.tenant_min_interval());
                    let never_read = local.read_at.is_none();
                    let decision = decide(d, head, dh, &local.status, gap, now);
                    let recent = |t: Option<Instant>| t.is_some_and(|t| t.elapsed() < CLAIM_CHECK);
                    let step = match decision {
                        Decision::Claim { bytes_only } => Some(Step::Claim { bytes_only }),
                        Decision::CheckClaim if !recent(local.checked_at) => Some(Step::Check),
                        Decision::Blocked if !recent(local.read_at) => Some(Step::Read),
                        Decision::Idle if never_read && dh.cursor < head.end() => Some(Step::Read),
                        _ => None,
                    };
                    let Some(step) = step else {
                        continue;
                    };
                    match step {
                        Step::Claim { .. } => report.claims += 1,
                        Step::Check => report.checks += 1,
                        Step::Read => {}
                    }
                    work.push((
                        Work {
                            feed: (*feed).clone(),
                            delivery: d.clone(),
                            uid: dref.uid.clone(),
                            head: head.clone(),
                            dh: dh.clone(),
                            store: store.clone(),
                            tenant_limits: limits.clone(),
                            gap,
                        },
                        step,
                    ));
                }
            }
        }
        self.local.retain(|k, _| seen.contains(k));
        self.limits.retain(|uid, _| snapshot.by_uid(uid).is_some());
        use futures::StreamExt;
        let rt_ref = &rt;
        let results: Vec<(String, bool, Option<DeliveryStatus>)> = futures::stream::iter(work)
            .map(|(w, step)| async move {
                let key = w.uid.to_string();
                let checked = matches!(step, Step::Check);
                let run = async {
                    match step {
                        Step::Claim { bytes_only } => {
                            claim_step(state, rt_ref, &w, bytes_only).await
                        }
                        Step::Check => check_step(state, rt_ref, &w).await,
                        Step::Read => status::read(&*state.history(), w.feed_uid(), w.uid.id())
                            .await
                            .ok(),
                    }
                };
                let out = match tokio::time::timeout(STEP_TIMEOUT, run).await {
                    Ok(s) => s,
                    Err(_) => {
                        tracing::warn!(delivery = %key, "a feed delivery step timed out");
                        None
                    }
                };
                (key, checked, out)
            })
            .buffer_unordered(CONCURRENT_STEPS)
            .collect()
            .await;
        for (key, checked, status) in results {
            if let (Some(local), Some(status)) = (self.local.get_mut(&key), status) {
                local.status = status;
                local.read_at = Some(Instant::now());
                if checked {
                    local.checked_at = Some(Instant::now());
                }
            }
        }
        for (feed, crossed, ratio) in crossings {
            self.crossed(state, feed, crossed, ratio).await;
        }
        self.readiness(&rt, &snapshot, report.store_ok);
        if self.maintain_at.is_none_or(|t| t <= Instant::now()) {
            for (store, feed) in maintain {
                maintain_feed(&rt, &*store, &feed).await;
            }
            self.maintain_at = Some(Instant::now() + MAINTENANCE_INTERVAL);
        }
        report
    }

    /// Create what the store lacks of a defined feed: its row, or the
    /// position of delivery `d`. A change that stored the definition and
    /// stopped before the store call leaves this.
    async fn set_up(
        &mut self,
        store: &dyn FeedStore,
        feed: &Feed,
        d: Option<&DeliverySpec>,
        tenant_limits: Option<&TenantLimits>,
    ) {
        let label = feed.label();
        let name = d.map(|d| d.name.as_str()).unwrap_or_default();
        let key = format!("{}/{name}", feed.uid);
        if self.setting_up.insert(key.clone()) {
            metrics::delivery_setup(&label, name);
            tracing::warn!(feed = %label, delivery = name, "a defined feed or delivery is missing from the feed store; creating it");
        }
        let made = match d.and_then(|d| feed.delivery(&d.name).map(|r| (d, r))) {
            Some((d, dref)) => store
                .ensure_delivery(&store_calls(), &dref.uid, super::start_of(d.start))
                .await
                .map(|_| ())
                .map_err(|e| e.to_string()),
            None => super::ensure_in_store(store, feed, tenant_limits).await,
        };
        match made {
            Ok(()) => {
                self.setting_up.remove(&key);
            }
            Err(e) => {
                tracing::debug!(feed = %label, delivery = name, error = %e, "could not create it yet; retrying")
            }
        }
    }

    /// Write `feed`'s limits (its own and its tenant's) to its store row
    /// when they differ from what this server last wrote there.
    async fn apply_limits(
        &mut self,
        store: &dyn FeedStore,
        feed: &Feed,
        tenant_limits: Option<&TenantLimits>,
    ) -> bool {
        let want = super::store_limits(&feed.spec, tenant_limits);
        let uid = feed.uid.as_str();
        if self.limits.get(uid) == Some(&want) {
            return false;
        }
        let tenant = match feed.tenant().map(faucet_common_feed::TenantId::new) {
            Some(Ok(t)) => Some(t),
            Some(Err(_)) => return false,
            None => None,
        };
        match store
            .ensure_feed(&store_calls(), &feed.uid, tenant.as_ref(), &want)
            .await
        {
            Ok(()) => {
                self.limits.insert(uid.to_string(), want);
                true
            }
            Err(e) => {
                tracing::warn!(feed = %feed.label(), error = %e, "could not write a feed's limits to its store; retrying");
                false
            }
        }
    }

    /// Record a change in how many `warn_at` thresholds a feed is past; the
    /// server that raises the shared count sends `feed_near_full`. While the
    /// history cannot be written the change is tried again next tick.
    async fn crossed(&mut self, state: &ServerState, feed: Arc<Feed>, crossed: usize, ratio: f64) {
        let label = feed.label();
        let before = self.warned.get(&label).copied().unwrap_or(0);
        match super::notify::crossing(&*state.history(), feed.uid.as_str(), crossed).await {
            Ok(rose) => {
                self.warned.insert(label.clone(), crossed);
                if crossed > before {
                    tracing::warn!(
                        feed = %label,
                        event = "feed_near_full",
                        ratio,
                        "feed is nearing its undelivered limits; past them it refuses new events"
                    );
                }
                let threshold = crossed
                    .checked_sub(1)
                    .and_then(|i| feed.spec.retention.warn_at.get(i).copied());
                if let (true, Some(threshold)) = (rose, threshold) {
                    super::notify::spawn(
                        state,
                        feed,
                        super::notify::FeedEvent::NearFull { ratio, threshold },
                    );
                }
            }
            Err(e) => {
                tracing::warn!(feed = %label, error = %e, "could not record the feed's warning state; retrying")
            }
        }
    }

    /// Emit a feed's gauges; returns how many `warn_at` thresholds it is
    /// past, with the ratio, when that differs from what this server last
    /// recorded.
    fn observe(&mut self, feed: &Feed, head: &Head) -> Option<(usize, f64)> {
        let label = feed.label();
        let mut depth = 0u64;
        let mut oldest = 0f64;
        for (name, dref) in &feed.deliveries {
            let Some(dh) = head.deliveries.iter().find(|x| x.delivery == dref.uid) else {
                continue;
            };
            let lag = dh.cursor.distance_to(head.end());
            depth = depth.max(lag);
            let age = dh
                .oldest_undelivered_at
                .map(|t| (head.now - t).num_milliseconds().max(0) as f64 / 1000.0)
                .unwrap_or(0.0);
            oldest = oldest.max(age);
            metrics::lag(&label, name, lag);
            metrics::oldest_undelivered(&label, name, age);
        }
        metrics::depth(&label, depth);
        let limits = &feed.spec.retention.undelivered;
        let by_age = oldest / limits.max_age.secs().max(1) as f64;
        // The feed's stored bytes bound its undelivered bytes from above.
        let by_bytes = if depth > 0 {
            head.bytes as f64 / limits.max_bytes.bytes().max(1) as f64
        } else {
            0.0
        };
        let ratio = by_age.max(by_bytes);
        metrics::undelivered_ratio(&label, ratio);
        let crossed = feed
            .spec
            .retention
            .warn_at
            .iter()
            .filter(|w| ratio >= **w)
            .count();
        let before = self.warned.get(&label).copied().unwrap_or(0);
        (crossed != before).then_some((crossed, ratio))
    }

    fn readiness(&self, rt: &FeedsRuntime, snapshot: &Snapshot, store_ok: bool) {
        let mut stalled = Vec::new();
        let mut held = Vec::new();
        for feed in &snapshot.feeds {
            let mut held_here = 0;
            for (name, dref) in &feed.deliveries {
                let health = self
                    .local
                    .get(&dref.uid.to_string())
                    .map(|l| l.status.health)
                    .unwrap_or_default();
                let at = format!("{}/{name}", feed.label());
                metrics::stalled(&feed.label(), name, health == Health::Stalled);
                match health {
                    Health::Stalled => stalled.push(at),
                    Health::Held => {
                        held_here += 1;
                        held.push(at)
                    }
                    _ => {}
                }
            }
            metrics::held(&feed.label(), held_here);
        }
        rt.set_readiness(Readiness {
            started: true,
            store_ok,
            feeds: snapshot.feeds.len(),
            stalled,
            held,
        });
    }
}

/// Retention for one feed, under its maintenance claim: at most
/// [`MAINTENANCE_ROWS`] rows per call.
async fn maintain_feed(rt: &FeedsRuntime, store: &dyn FeedStore, feed: &Feed) {
    let calls = store_calls();
    match store
        .maintenance(&calls, &feed.uid, rt.owner(), MAINTENANCE_INTERVAL * 2)
        .await
    {
        Ok(true) => {}
        Ok(false) => return,
        Err(e) => {
            tracing::warn!(feed = %feed.label(), error = %e, "feed maintenance claim failed");
            return;
        }
    }
    let mut trimmed = 0;
    for _ in 0..MAINTENANCE_PASSES {
        match store.trim(&calls, &feed.uid, MAINTENANCE_ROWS).await {
            Ok(t) => {
                trimmed += t.events;
                if !t.more {
                    break;
                }
            }
            Err(e) => {
                tracing::warn!(feed = %feed.label(), error = %e, "trimming a feed failed");
                break;
            }
        }
    }
    if trimmed > 0 {
        metrics::trimmed(&feed.label(), trimmed);
    }
    if let Err(e) = store
        .purge_dedupe(&calls, &feed.uid, MAINTENANCE_ROWS)
        .await
    {
        tracing::warn!(feed = %feed.label(), error = %e, "purging a feed's dedupe keys failed");
    }
}

/// Run the loop until `shutdown`.
#[allow(clippy::disallowed_methods)] // a runtime timer, not a connector backend call
pub async fn run(state: ServerState, shutdown: CancellationToken) {
    let changed = state.feeds().changed();
    let mut scheduler = Scheduler::default();
    loop {
        if shutdown.is_cancelled() {
            return;
        }
        scheduler.tick(&state).await;
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = changed.notified() => scheduler.invalidate(),
            _ = tokio::time::sleep(TICK) => {}
        }
    }
}

struct AbortOnDrop(tokio::task::AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Run the loop supervised: a panic restarts it after `pause` instead of
/// silently ending every delivery.
#[allow(clippy::disallowed_methods)] // a restart back-off, not a connector backend call
pub async fn supervise(state: ServerState, shutdown: CancellationToken, pause: Duration) {
    loop {
        let task = tokio::spawn(run(state.clone(), shutdown.clone()));
        let _guard = AbortOnDrop(task.abort_handle());
        match task.await {
            Err(e) if e.is_panic() && !shutdown.is_cancelled() => {
                tracing::error!("the feed scheduler panicked; restarting it");
                metrics::scheduler_restarted();
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    _ = tokio::time::sleep(pause) => {}
                }
            }
            _ => return,
        }
    }
}
