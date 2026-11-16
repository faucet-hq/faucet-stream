//! `/v1/feeds` (#1034): HTTP adapters over [`crate::serve::feeds::service`],
//! which holds every rule. Each handler names the tenant (`?tenant=`, or the
//! caller's own), calls the service and writes the audit entry.
//!
//! | Route | Permission | Audit action |
//! |---|---|---|
//! | `GET /v1/feeds`, `GET /v1/feeds/{name}` | `FeedRead` | |
//! | `GET /v1/feeds/{name}/{tokens,events,refusals}` | `FeedRead` | |
//! | `POST /v1/feeds`, `PUT /v1/feeds/{name}` | `FeedWrite` | `feed.create`, `feed.update` |
//! | `POST /v1/feeds/{name}/deliveries`, `PUT`/`DELETE …/deliveries/{delivery}` | `FeedWrite` | `feed.delivery_set`, `feed.delivery_delete` |
//! | `POST …/deliveries/{delivery}/{pause,resume,deliver-now}` | `FeedWrite` | `feed.pause`, `feed.resume`, `feed.deliver_now` |
//! | `POST /v1/feeds/{name}/tokens`, `DELETE …/tokens/{id}` | `FeedWrite` | `feed.token_create`, `feed.token_revoke` |
//! | `DELETE /v1/feeds/{name}` | `FeedAdmin` | `feed.delete` |
//! | `POST …/deliveries/{delivery}/{replay,skip,recover}` | `FeedAdmin` | `feed.replay`, `feed.skip`, `feed.recover` |
//! | `GET /v1/feeds/{name}/events/{seq}` (the payload) | `FeedAdmin` | `feed.events_read` |
//!
//! The ingest routes (`POST /v1/feeds/{name}/events` and the tenant form)
//! are not here: they authenticate themselves (`feeds::ingest`).

use crate::serve::error::ServeError;
use crate::serve::feeds::ingest::RefusalRecord;
use crate::serve::feeds::service::{
    self, ChangeAction, ChangePayload, Changed, EventView, EventsPage, FeedDetail, FeedList,
    FeedView, Moved, Position, TokenView,
};
use crate::serve::history::feeds::{DeliverySpec, FeedSpec};
use crate::serve::rbac::AuthContext;
use crate::serve::state::ServerState;
use axum::Json;
use axum::extract::{Extension, Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::json;

/// `?tenant=` names the tenant whose feeds are meant (a tenant principal's
/// own is implied); `?reason=` is kept on the change request when the
/// server gates the change.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scope {
    #[serde(default)]
    pub tenant: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
}

fn label(actor: &AuthContext, q: &Scope, name: &str) -> String {
    service::label_of(actor.tenant.as_deref().or(q.tenant.as_deref()), name)
}

/// Write the audit entry of a change: who, what, which feed, how it ended.
async fn audit<T>(
    state: &ServerState,
    actor: &AuthContext,
    q: &Scope,
    action: &str,
    target: String,
    detail: String,
    outcome: &Result<T, ServeError>,
) {
    let mut who = actor.clone();
    if who.tenant.is_none() {
        who.tenant.clone_from(&q.tenant);
    }
    let (result, detail) = match outcome {
        Ok(_) => ("ok", detail),
        Err(e) => (
            "error",
            format!(
                "{detail}{}{}",
                if detail.is_empty() { "" } else { ": " },
                e.api_error().error.message
            ),
        ),
    };
    crate::serve::audit::write_target_detail(
        state,
        &who,
        action,
        format!("feed:{target}"),
        detail,
        result,
    )
    .await;
}

/// The response for a change: `ok` with the view, 204 for a delete, or 202
/// with the pending change request.
fn respond(changed: Changed, ok: StatusCode) -> Response {
    match changed {
        Changed::Applied(Some(v)) => (ok, Json(v)).into_response(),
        Changed::Applied(None) => StatusCode::NO_CONTENT.into_response(),
        Changed::Pending(c) => (
            StatusCode::ACCEPTED,
            Json(crate::serve::runner::pending_approval_body(&c)),
        )
            .into_response(),
    }
}

fn detail_of(changed: &Result<Changed, ServeError>) -> String {
    match changed {
        Ok(Changed::Pending(c)) => format!("proposed as change request {}", c.id),
        _ => String::new(),
    }
}

/// 409 naming the runs in flight, so a client can cancel them and retry.
fn in_flight(runs: Vec<(String, Option<String>)>) -> Response {
    let named: Vec<String> = runs
        .iter()
        .map(|(d, run)| match run {
            Some(id) => format!("'{d}' (run {id})"),
            None => format!("'{d}' (its run is starting)"),
        })
        .collect();
    let body = json!({"error": {
        "code": "conflict",
        "message": format!(
            "a run is in flight for delivery {}; pause the delivery and wait for the run to \
             end, or cancel the run, then retry",
            named.join(", ")
        ),
        "details": {"runs": runs.iter().map(|(d, run)| json!({"delivery": d, "run_id": run})).collect::<Vec<_>>()},
    }});
    (StatusCode::CONFLICT, Json(body)).into_response()
}

pub async fn list(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Query(q): Query<Scope>,
) -> Result<Json<FeedList>, ServeError> {
    service::overview(&state, &actor, q.tenant.as_deref())
        .await
        .map(Json)
}

pub async fn get(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(q): Query<Scope>,
) -> Result<Json<FeedDetail>, ServeError> {
    service::get(&state, &actor, q.tenant.as_deref(), &name)
        .await
        .map(Json)
}

pub async fn create(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Query(q): Query<Scope>,
    Json(spec): Json<FeedSpec>,
) -> Result<Response, ServeError> {
    let name = spec.name.clone();
    let p = ChangePayload::new(ChangeAction::Create, q.tenant.as_deref(), &name, Some(spec));
    let changed = service::change(&state, &actor, p, q.reason.clone()).await;
    let target = label(&actor, &q, &name);
    audit(
        &state,
        &actor,
        &q,
        "feed.create",
        target,
        detail_of(&changed),
        &changed,
    )
    .await;
    Ok(respond(changed?, StatusCode::CREATED))
}

pub async fn update(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(q): Query<Scope>,
    Json(spec): Json<FeedSpec>,
) -> Result<Response, ServeError> {
    let p = ChangePayload::new(ChangeAction::Update, q.tenant.as_deref(), &name, Some(spec));
    let changed = service::change(&state, &actor, p, q.reason.clone()).await;
    let target = label(&actor, &q, &name);
    audit(
        &state,
        &actor,
        &q,
        "feed.update",
        target,
        detail_of(&changed),
        &changed,
    )
    .await;
    Ok(respond(changed?, StatusCode::OK))
}

pub async fn delete(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(q): Query<Scope>,
) -> Result<Response, ServeError> {
    let tenant = q.tenant.as_deref();
    let runs = service::in_flight(&state, &actor, tenant, &name, None).await?;
    if !runs.is_empty() {
        return Ok(in_flight(runs));
    }
    let p = ChangePayload::new(ChangeAction::Delete, tenant, &name, None);
    let changed = service::change(&state, &actor, p, q.reason.clone()).await;
    let target = label(&actor, &q, &name);
    audit(
        &state,
        &actor,
        &q,
        "feed.delete",
        target,
        detail_of(&changed),
        &changed,
    )
    .await;
    Ok(respond(changed?, StatusCode::NO_CONTENT))
}

async fn set_delivery(
    state: ServerState,
    actor: AuthContext,
    name: String,
    q: Scope,
    delivery: DeliverySpec,
    ok: StatusCode,
) -> Result<Response, ServeError> {
    let tenant = q.tenant.as_deref();
    let target = format!("{}/{}", label(&actor, &q, &name), delivery.name);
    let changed = async {
        let next = service::with_delivery(&state, &actor, tenant, &name, delivery).await?;
        let p = ChangePayload::new(ChangeAction::Update, tenant, &name, Some(next));
        service::change(&state, &actor, p, q.reason.clone()).await
    }
    .await;
    audit(
        &state,
        &actor,
        &q,
        "feed.delivery_set",
        target,
        detail_of(&changed),
        &changed,
    )
    .await;
    Ok(respond(changed?, ok))
}

pub async fn delivery_create(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(q): Query<Scope>,
    Json(delivery): Json<DeliverySpec>,
) -> Result<Response, ServeError> {
    let existing = service::get(&state, &actor, q.tenant.as_deref(), &name).await?;
    if existing
        .feed
        .deliveries
        .iter()
        .any(|d| d.name == delivery.name)
    {
        return Err(ServeError::Conflict(format!(
            "feed '{name}' already has a delivery named '{}'",
            delivery.name
        )));
    }
    set_delivery(state, actor, name, q, delivery, StatusCode::CREATED).await
}

pub async fn delivery_update(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path((name, delivery_name)): Path<(String, String)>,
    Query(q): Query<Scope>,
    Json(delivery): Json<DeliverySpec>,
) -> Result<Response, ServeError> {
    if delivery.name != delivery_name {
        return Err(ServeError::Unprocessable {
            message: "a delivery cannot be renamed; add a new delivery instead".into(),
            details: None,
        });
    }
    let existing = service::get(&state, &actor, q.tenant.as_deref(), &name).await?;
    if !existing
        .feed
        .deliveries
        .iter()
        .any(|d| d.name == delivery.name)
    {
        return Err(ServeError::NotFound);
    }
    set_delivery(state, actor, name, q, delivery, StatusCode::OK).await
}

pub async fn delivery_delete(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path((name, delivery)): Path<(String, String)>,
    Query(q): Query<Scope>,
) -> Result<Response, ServeError> {
    let tenant = q.tenant.as_deref();
    let runs = service::in_flight(&state, &actor, tenant, &name, Some(&delivery)).await?;
    if !runs.is_empty() {
        return Ok(in_flight(runs));
    }
    let target = format!("{}/{delivery}", label(&actor, &q, &name));
    let changed = async {
        let next = service::without_delivery(&state, &actor, tenant, &name, &delivery).await?;
        let p = ChangePayload::new(ChangeAction::Update, tenant, &name, Some(next));
        service::change(&state, &actor, p, q.reason.clone()).await
    }
    .await;
    audit(
        &state,
        &actor,
        &q,
        "feed.delivery_delete",
        target,
        detail_of(&changed),
        &changed,
    )
    .await;
    Ok(respond(changed?, StatusCode::OK))
}

/// An optional `{reason}` body.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReasonBody {
    #[serde(default)]
    pub reason: Option<String>,
}

async fn feed_view(
    state: &ServerState,
    actor: &AuthContext,
    q: &Scope,
    name: &str,
) -> Result<Json<FeedView>, ServeError> {
    service::get(state, actor, q.tenant.as_deref(), name)
        .await
        .map(|d| Json(d.feed))
}

pub async fn pause(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path((name, delivery)): Path<(String, String)>,
    Query(q): Query<Scope>,
    body: Option<Json<ReasonBody>>,
) -> Result<Json<FeedView>, ServeError> {
    let reason = body.and_then(|Json(b)| b.reason);
    let done = service::pause(
        &state,
        &actor,
        q.tenant.as_deref(),
        &name,
        &delivery,
        reason.clone(),
    )
    .await;
    let target = format!("{}/{delivery}", label(&actor, &q, &name));
    audit(
        &state,
        &actor,
        &q,
        "feed.pause",
        target,
        reason.unwrap_or_default(),
        &done,
    )
    .await;
    done?;
    feed_view(&state, &actor, &q, &name).await
}

pub async fn resume(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path((name, delivery)): Path<(String, String)>,
    Query(q): Query<Scope>,
) -> Result<Json<FeedView>, ServeError> {
    let done = service::resume(&state, &actor, q.tenant.as_deref(), &name, &delivery).await;
    let target = format!("{}/{delivery}", label(&actor, &q, &name));
    audit(
        &state,
        &actor,
        &q,
        "feed.resume",
        target,
        String::new(),
        &done,
    )
    .await;
    done?;
    feed_view(&state, &actor, &q, &name).await
}

pub async fn deliver_now(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path((name, delivery)): Path<(String, String)>,
    Query(q): Query<Scope>,
) -> Result<Json<FeedView>, ServeError> {
    let done = service::deliver_now(&state, &actor, q.tenant.as_deref(), &name, &delivery).await;
    let target = format!("{}/{delivery}", label(&actor, &q, &name));
    audit(
        &state,
        &actor,
        &q,
        "feed.deliver_now",
        target,
        String::new(),
        &done,
    )
    .await;
    done?;
    feed_view(&state, &actor, &q, &name).await
}

/// `POST …/replay` body: where the delivery goes back to (exactly one).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayBody {
    /// The event to deliver next.
    #[serde(default)]
    pub seq: Option<u64>,
    /// The first event stored at or after this time.
    #[serde(default)]
    pub time: Option<DateTime<Utc>>,
}

fn moved_detail(moved: &Result<Moved, ServeError>, extra: &str) -> String {
    match moved {
        Ok(m) => format!("from event {} to {}{extra}", m.from, m.to),
        Err(_) => extra.trim_start_matches("; ").to_string(),
    }
}

pub async fn replay(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path((name, delivery)): Path<(String, String)>,
    Query(q): Query<Scope>,
    Json(body): Json<ReplayBody>,
) -> Result<Json<Moved>, ServeError> {
    let from = match (body.seq, body.time) {
        (Some(seq), None) => Position::Seq(seq),
        (None, Some(time)) => Position::Time(time),
        _ => {
            return Err(ServeError::Unprocessable {
                message: "a replay names where it starts: exactly one of `seq` and `time`".into(),
                details: None,
            });
        }
    };
    let moved = service::replay(&state, &actor, q.tenant.as_deref(), &name, &delivery, from).await;
    let target = format!("{}/{delivery}", label(&actor, &q, &name));
    audit(
        &state,
        &actor,
        &q,
        "feed.replay",
        target,
        moved_detail(&moved, ""),
        &moved,
    )
    .await;
    moved.map(Json)
}

/// `POST …/skip` body.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkipBody {
    /// The next event to read; default: one event past the position.
    #[serde(default)]
    pub to: Option<u64>,
    /// Why; kept with the gap the skip leaves.
    pub reason: String,
}

pub async fn skip(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path((name, delivery)): Path<(String, String)>,
    Query(q): Query<Scope>,
    Json(body): Json<SkipBody>,
) -> Result<Json<Moved>, ServeError> {
    let moved = service::skip(
        &state,
        &actor,
        q.tenant.as_deref(),
        &name,
        &delivery,
        body.to,
        &body.reason,
    )
    .await;
    let target = format!("{}/{delivery}", label(&actor, &q, &name));
    let detail = moved_detail(&moved, &format!("; {}", body.reason.trim()));
    audit(&state, &actor, &q, "feed.skip", target, detail, &moved).await;
    moved.map(Json)
}

pub async fn recover(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path((name, delivery)): Path<(String, String)>,
    Query(q): Query<Scope>,
) -> Result<Json<Moved>, ServeError> {
    let moved = service::recover(&state, &actor, q.tenant.as_deref(), &name, &delivery).await;
    let target = format!("{}/{delivery}", label(&actor, &q, &name));
    audit(
        &state,
        &actor,
        &q,
        "feed.recover",
        target,
        moved_detail(&moved, ""),
        &moved,
    )
    .await;
    moved.map(Json)
}

pub async fn tokens(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(q): Query<Scope>,
) -> Result<Json<Vec<TokenView>>, ServeError> {
    service::token_list(&state, &actor, q.tenant.as_deref(), &name)
        .await
        .map(Json)
}

/// `POST /v1/feeds/{name}/tokens` body.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TokenBody {
    pub name: String,
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,
}

pub async fn token_create(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(q): Query<Scope>,
    Json(body): Json<TokenBody>,
) -> Result<Response, ServeError> {
    let made = service::token_create(
        &state,
        &actor,
        q.tenant.as_deref(),
        &name,
        &body.name,
        body.expires_at,
    )
    .await;
    let detail = match &made {
        Ok((view, _)) => format!("token {} ({})", view.name, view.id),
        Err(_) => format!("token {}", body.name),
    };
    audit(
        &state,
        &actor,
        &q,
        "feed.token_create",
        label(&actor, &q, &name),
        detail,
        &made,
    )
    .await;
    let (view, token) = made?;
    let mut out = serde_json::to_value(view).map_err(|e| ServeError::Internal(e.to_string()))?;
    out["token"] = json!(token);
    Ok((StatusCode::CREATED, Json(out)).into_response())
}

pub async fn token_revoke(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path((name, id)): Path<(String, String)>,
    Query(q): Query<Scope>,
) -> Result<Json<TokenView>, ServeError> {
    let revoked = service::token_revoke(&state, &actor, q.tenant.as_deref(), &name, &id).await;
    let target = label(&actor, &q, &name);
    audit(
        &state,
        &actor,
        &q,
        "feed.token_revoke",
        target,
        format!("token {id}"),
        &revoked,
    )
    .await;
    revoked.map(Json)
}

/// `GET /v1/feeds/{name}/events` query.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventsQuery {
    #[serde(default)]
    pub tenant: Option<String>,
    /// List the events before this one, newest first (default: the newest).
    #[serde(default)]
    pub before: Option<u64>,
    /// List the events from this one on instead.
    #[serde(default)]
    pub from: Option<u64>,
    /// At most this many (default 50, at most 200).
    #[serde(default)]
    pub limit: Option<u64>,
}

pub async fn events(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(q): Query<EventsQuery>,
) -> Result<Json<EventsPage>, ServeError> {
    service::events(
        &state,
        &actor,
        q.tenant.as_deref(),
        &name,
        q.before,
        q.from,
        q.limit,
    )
    .await
    .map(Json)
}

pub async fn event(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path((name, seq)): Path<(String, u64)>,
    Query(q): Query<Scope>,
) -> Result<Json<EventView>, ServeError> {
    let read = service::event(&state, &actor, q.tenant.as_deref(), &name, seq).await;
    let target = label(&actor, &q, &name);
    audit(
        &state,
        &actor,
        &q,
        "feed.events_read",
        target,
        format!("event {seq}"),
        &read,
    )
    .await;
    read.map(Json)
}

/// `GET /v1/feeds/{name}/refusals` query.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RefusalsQuery {
    #[serde(default)]
    pub tenant: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

pub async fn refusals(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path(name): Path<String>,
    Query(q): Query<RefusalsQuery>,
) -> Result<Json<Vec<RefusalRecord>>, ServeError> {
    service::refusals(&state, &actor, q.tenant.as_deref(), &name, q.limit)
        .await
        .map(Json)
}
