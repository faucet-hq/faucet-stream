//! Every rule of the feeds API (#1034), so the HTTP, CLI and MCP adapters
//! stay thin: who sees which feed, what a definition may say, quotas, how a
//! feed, a delivery and a token are made, changed and removed, and the
//! operator's moves on a delivery (pause, resume, deliver now, replay, skip,
//! recover).

use super::status::{self, DeliveryStatus, Health};
use super::{
    DeliveryRef, Feed, Source, ensure_in_store, feed_of_record, metrics, secrets, spec, store_calls,
};
use crate::serve::error::ServeError;
use crate::serve::history::feeds::{
    DeliverySpec, FeedChange, FeedRecord, FeedSpec, FeedTokenRecord, RunFeedBinding, feed_key,
    spec_diff,
};
use crate::serve::rbac::{AuthContext, Role};
use crate::serve::state::ServerState;
use chrono::{DateTime, Utc};
use faucet_common_feed::{ClaimOwner, DeliveryUid, FeedStore, Head, Move, Seq};
use serde::Serialize;
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
    /// `ok`, `failing`, `stalled`, `held`, `paused` or `needs_approval`.
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub paused_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pause_reason: Option<String>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stored_bytes: Option<u64>,
    /// Events and bytes accepted in the last 24 hours, once ingest counts them.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub events_24h: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_24h: Option<u64>,
    pub deliveries: Vec<DeliveryView>,
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
    statuses: &std::collections::BTreeMap<String, DeliveryStatus>,
) -> FeedView {
    let masked = secrets::masked(&feed.spec);
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
                status: status_word(d, &s).to_string(),
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
                spec: d.clone(),
            })
        })
        .collect();
    FeedView {
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
        stored_bytes: head.map(|h| h.bytes),
        events_24h: None,
        bytes_24h: None,
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

async fn view_of(state: &ServerState, feed: &Feed) -> Result<FeedView, ServeError> {
    let head = match state.feeds().store_for(feed).await {
        Ok(store) => head_of(&*store, feed).await.ok().flatten(),
        Err(_) => None,
    };
    Ok(view(feed, head.as_ref(), &statuses(state, feed).await?))
}

/// The feeds `actor` sees for `tenant`.
pub async fn list(
    state: &ServerState,
    actor: &AuthContext,
    tenant: Option<&str>,
) -> Result<Vec<FeedView>, ServeError> {
    let tenant = scope(actor, tenant)?;
    let rt = state.feeds();
    let snapshot = rt.refresh(state).await.map_err(ServeError::Unavailable)?;
    let mut out = Vec::new();
    for feed in snapshot
        .feeds
        .iter()
        .filter(|f| f.tenant() == tenant.as_deref())
    {
        out.push(view_of(state, feed).await?);
    }
    Ok(out)
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
    Ok(FeedDetail {
        feed: view_of(state, feed).await?,
        gaps,
        tokens: tokens.iter().map(TokenView::from).collect(),
        changes: found.record.map(|r| r.changes).unwrap_or_default(),
    })
}

/// The config a delivery's runs load, for a check or to find its state.
async fn delivery_config(
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
    if node.source.kind != crate::runtime::server::FEED_SOURCE_KIND {
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

/// Screen a delivery's params as its creator's and seal the secret ones.
async fn prepare_delivery(
    state: &ServerState,
    actor: &AuthContext,
    record: &FeedRecord,
    d: &mut DeliverySpec,
    current: Option<&DeliverySpec>,
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
    secrets::seal_params(
        state,
        t,
        &uid,
        &secret,
        current.and_then(|c| c.template.as_ref()),
        scope,
        false,
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

/// Create a feed. A tenant principal's feed belongs to its tenant.
pub async fn create(
    state: &ServerState,
    actor: &AuthContext,
    tenant: Option<&str>,
    mut new: FeedSpec,
) -> Result<FeedView, ServeError> {
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
            .filter(|r| r.tenant() == Some(t.as_str()))
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
    let feed_scope = secrets::Scope {
        feed: &record.uid.clone(),
        tenant: tenant.as_deref(),
    };
    secrets::seal_signature(state, &mut record.spec, None, feed_scope).map_err(unprocessable)?;
    let mut deliveries = record.spec.deliveries.clone();
    for d in &mut deliveries {
        prepare_delivery(state, actor, &record, d, None).await?;
    }
    record.spec.deliveries = deliveries;
    let feed = feed_of_record(state, &record).map_err(unprocessable)?;
    for d in &feed.spec.deliveries {
        check_delivery(state, &feed, d).await?;
    }
    let store = rt.store_for(&feed).await.map_err(store_error)?;
    if !state
        .history()
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

/// Replace a stored feed's definition. The name, the tenant and the store
/// stay; a delivery keeps its position by its name.
pub async fn update(
    state: &ServerState,
    actor: &AuthContext,
    tenant: Option<&str>,
    name: &str,
    mut new: FeedSpec,
) -> Result<FeedView, ServeError> {
    crate::runtime::server::refuse_while_unavailable(state)?;
    let tenant = scope(actor, tenant)?;
    let found = find(state, tenant.as_deref(), name).await?;
    let current = read_only(&found)?.clone();
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
    let rt = state.feeds();
    let store = rt.store_for(&found.feed).await.map_err(store_error)?;
    let now = Utc::now();
    let detail = spec_diff(&secrets::masked(&current.spec), &secrets::masked(&new));
    let deliveries_changed = detail.iter().any(|l| l.starts_with("delivery "));
    let mut current = current;
    for _ in 0..REPLACE_ATTEMPTS {
        let (mut next, dropped) = current.revised(
            new.clone(),
            &actor.principal,
            now,
            "updated",
            detail.clone(),
        );
        let feed_scope = secrets::Scope {
            feed: &current.uid,
            tenant: tenant.as_deref(),
        };
        secrets::seal_signature(state, &mut next.spec, Some(&current.spec), feed_scope)
            .map_err(unprocessable)?;
        let mut deliveries = next.spec.deliveries.clone();
        for d in &mut deliveries {
            let before = current.spec.delivery(&d.name);
            prepare_delivery(state, actor, &next, d, before).await?;
        }
        next.spec.deliveries = deliveries;
        if deliveries_changed {
            next.approved = false;
        }
        let feed = feed_of_record(state, &next).map_err(unprocessable)?;
        for d in &feed.spec.deliveries {
            if current.spec.delivery(&d.name) != Some(d) {
                check_delivery(state, &feed, d).await?;
            }
        }
        for (dropped_name, uid) in &dropped {
            refuse_live_claim(&*store, &found.feed, dropped_name, uid).await?;
        }
        if state
            .history()
            .feed_replace(&current, &next)
            .await
            .map_err(ServeError::from)?
        {
            ensure_in_store(&*store, &feed, limits.as_ref())
                .await
                .map_err(store_error)?;
            for (dropped_name, uid) in &dropped {
                if let Some(old) = current.spec.delivery(dropped_name) {
                    forget_delivery(state, &*store, &found.feed, old, uid).await;
                }
            }
            after_change(state).await;
            return view_of(state, &feed).await;
        }
        current = state
            .history()
            .feed_get(&current.key())
            .await
            .map_err(ServeError::from)?
            .ok_or(ServeError::NotFound)?;
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

/// Remove a delivery from the store with its status, metric series and
/// state keys. Rows a sink keeps for it are left behind.
async fn forget_delivery(
    state: &ServerState,
    store: &dyn FeedStore,
    feed: &Feed,
    d: &DeliverySpec,
    uid: &str,
) {
    let label = feed.label();
    if let Some(dref) = feed.delivery(&d.name)
        && let Err(e) = remove_state(state, feed, d, dref).await
    {
        tracing::warn!(feed = %label, delivery = %d.name, error = %e, "could not remove a deleted delivery's state keys");
    }
    if let Ok(duid) = DeliveryUid::new(feed.uid.clone(), uid)
        && let Err(e) = store.delete_delivery(&store_calls(), &duid).await
    {
        tracing::warn!(feed = %label, delivery = %d.name, error = %e, "could not remove a deleted delivery from the feed store");
    }
    status::forget(&*state.history(), feed.uid.as_str(), uid).await;
    metrics::forget_delivery(&label, &d.name);
}

/// Where a delivery's run keeps its bookmark: the state store and the key.
async fn state_target(
    state: &ServerState,
    feed: &Feed,
    d: &DeliverySpec,
    dref: &DeliveryRef,
) -> Result<Option<(Arc<dyn faucet_core::StateStore>, faucet_core::StateKey)>, String> {
    let loaded = delivery_config(state, feed, d, dref)
        .await
        .map_err(|e| e.api_error().error.message)?;
    let Some(node) = loaded.nodes.first() else {
        return Ok(None);
    };
    let Some(spec) = &node.state else {
        return Ok(None);
    };
    let store = crate::state::build_state_store(spec)
        .await
        .map_err(|e| e.to_string())?;
    let scope = loaded
        .tenant
        .as_ref()
        .map(|t| t.state_scope())
        .unwrap_or_default();
    let name = loaded.cfg.name.clone().unwrap_or_else(|| "serve".into());
    Ok(Some((store, scope.key(&name).with_row(node.id.as_str()))))
}

async fn remove_state(
    state: &ServerState,
    feed: &Feed,
    d: &DeliverySpec,
    dref: &DeliveryRef,
) -> Result<(), String> {
    use faucet_core::KeySuffix;
    let Some((store, key)) = state_target(state, feed, d, dref).await? else {
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

/// Delete a stored feed: its definition and tokens, its events, cursors and
/// gaps, and each delivery's status, metric series and state keys. Refused
/// while a delivery has a run in flight.
pub async fn delete(
    state: &ServerState,
    actor: &AuthContext,
    tenant: Option<&str>,
    name: &str,
) -> Result<(), ServeError> {
    crate::runtime::server::refuse_while_unavailable(state)?;
    let tenant = scope(actor, tenant)?;
    let found = find(state, tenant.as_deref(), name).await?;
    let record = read_only(&found)?.clone();
    let feed = found.feed.clone();
    let store = state.feeds().store_for(&feed).await.map_err(store_error)?;
    for (dname, meta) in &record.deliveries {
        refuse_live_claim(&*store, &feed, dname, &meta.uid).await?;
    }
    for d in &feed.spec.deliveries {
        if let Some(meta) = record.deliveries.get(&d.name) {
            forget_delivery(state, &*store, &feed, d, &meta.uid).await;
        }
    }
    if !state
        .history()
        .feed_delete(&record.key())
        .await
        .map_err(ServeError::from)?
    {
        return Err(ServeError::NotFound);
    }
    store
        .purge_feed(&store_calls(), &feed.uid)
        .await
        .map_err(store_error)?;
    let names: Vec<String> = feed.deliveries.keys().cloned().collect();
    metrics::forget_feed(&feed.label(), &names);
    after_change(state).await;
    Ok(())
}

/// Add a delivery to a stored feed, or replace the one with its name.
pub async fn set_delivery(
    state: &ServerState,
    actor: &AuthContext,
    tenant: Option<&str>,
    name: &str,
    delivery: DeliverySpec,
) -> Result<FeedView, ServeError> {
    let scoped = scope(actor, tenant)?;
    let found = find(state, scoped.as_deref(), name).await?;
    let mut next = secrets::masked(&read_only(&found)?.spec);
    match next.deliveries.iter_mut().find(|d| d.name == delivery.name) {
        Some(slot) => *slot = delivery,
        None => next.deliveries.push(delivery),
    }
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
    let scoped = scope(actor, tenant)?;
    let found = find(state, scoped.as_deref(), name).await?;
    let mut next = secrets::masked(&read_only(&found)?.spec);
    let before = next.deliveries.len();
    next.deliveries.retain(|d| d.name != delivery);
    if next.deliveries.len() == before {
        return Err(ServeError::NotFound);
    }
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
        .map_err(unprocessable)?
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

/// Remove a deleted tenant's feeds from their stores, with their statuses
/// and metric series. The definitions and tokens go with the tenant's
/// history rows.
pub async fn purge_tenant(state: &ServerState, tenant: &str) -> Result<usize, String> {
    let records = state
        .history()
        .feed_list()
        .await
        .map_err(|e| e.to_string())?;
    let mut purged = 0;
    for record in records.iter().filter(|r| r.tenant() == Some(tenant)) {
        let feed = match feed_of_record(state, record) {
            Ok(f) => f,
            Err(_) => {
                let mut plain = record.clone();
                plain.spec.ingest.auth.signature = None;
                feed_of_record(state, &plain)?
            }
        };
        let store = state.feeds().store_for(&feed).await?;
        store
            .purge_feed(&store_calls(), &feed.uid)
            .await
            .map_err(|e| e.to_string())?;
        for meta in record.deliveries.values() {
            status::forget(&*state.history(), &record.uid, &meta.uid).await;
        }
        let names: Vec<String> = record.deliveries.keys().cloned().collect();
        metrics::forget_feed(&feed.label(), &names);
        purged += 1;
    }
    if purged > 0 {
        state.feeds().notify();
    }
    Ok(purged)
}
