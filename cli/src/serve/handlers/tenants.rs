//! `/v1/tenants*`, `/v1/connect/*` and `POST /v1/templates/{id}/fanout` —
//! multi-tenant embedded integrations (#709).
//!
//! | route | permission |
//! |---|---|
//! | `GET /v1/tenants`, `GET /v1/tenants/{tenant}` | `TenantRead` (viewer+) |
//! | `POST /v1/tenants`, `PATCH`/`DELETE /v1/tenants/{tenant}` | `TenantAdmin` (admin) |
//! | `GET /v1/tenants/{tenant}/connections[/{name}]`, `GET /v1/connect/providers` | `TenantRead` |
//! | `POST /v1/tenants/{tenant}/connections`, `PUT`/`DELETE …/connections/{name}`, `POST …/connect/{provider}` | `ConnectionManage` (operator+) |
//! | `POST /v1/tenants/{tenant}/runs`, `POST …/templates/{id}/runs`, `POST /v1/templates/{id}/fanout` | `RunWrite` (operator+) |
//! | `GET /v1/connect/callback` | public — the OAuth `state` is the credential |
//!
//! A tenant-scoped principal reaches only its own tenant's routes (the auth
//! middleware answers `404` for another tenant's, so existence does not leak).

use crate::serve::error::ServeError;
use crate::serve::handlers::templates::{TriggerBody, TriggerOutcome, trigger_template_outcome};
use crate::serve::history::tenants::{
    ConnectionRecord, ConnectionStatus, TenantLimits, TenantRecord, TenantSelector,
    validate_connection_name, validate_tenant_id,
};
use crate::serve::rbac::AuthContext;
use crate::serve::runner::{self, SubmitOutcome, SubmitRequest};
use crate::serve::state::ServerState;
use crate::serve::tenants::{self, connect};
use axum::Json;
use axum::extract::{Extension, Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Redirect, Response};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

const DEFAULT_FANOUT_CONCURRENCY: usize = 4;
const MAX_FANOUT_CONCURRENCY: usize = 64;

fn store_err(e: impl std::fmt::Display) -> ServeError {
    ServeError::Internal(format!("tenant store: {e}"))
}

/// The actor, acting for `tenant`.
fn for_tenant(actor: &AuthContext, tenant: &str) -> AuthContext {
    AuthContext {
        tenant: Some(tenant.to_string()),
        ..actor.clone()
    }
}

/// A connection as the API shows it — never the credentials.
#[derive(Debug, Clone, Serialize)]
pub struct ConnectionView {
    pub name: String,
    pub provider_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connect_provider: Option<String>,
    pub status: ConnectionStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reauth_reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub updated_by: String,
}

impl From<&ConnectionRecord> for ConnectionView {
    fn from(c: &ConnectionRecord) -> Self {
        Self {
            name: c.name.clone(),
            provider_type: c.provider_type.clone(),
            connect_provider: c.connect_provider.clone(),
            status: c.status,
            reauth_reason: c.reauth_reason.clone(),
            created_at: c.created_at,
            updated_at: c.updated_at,
            updated_by: c.updated_by.clone(),
        }
    }
}

/// A tenant with its connections' state.
#[derive(Debug, Clone, Serialize)]
pub struct TenantView {
    #[serde(flatten)]
    pub tenant: TenantRecord,
    pub connections: Vec<ConnectionView>,
    /// Runs queued, pending or running for the tenant.
    pub active_runs: usize,
}

async fn view(state: &ServerState, tenant: TenantRecord) -> Result<TenantView, ServeError> {
    let connections = state
        .history()
        .connection_list(&tenant.id)
        .await
        .map_err(store_err)?
        .iter()
        .map(ConnectionView::from)
        .collect();
    let active_runs = tenants::active_runs(state, &tenant.id, 1000).await?;
    Ok(TenantView {
        tenant,
        connections,
        active_runs,
    })
}

// ── Tenants CRUD ─────────────────────────────────────────────────────────────

/// `POST /v1/tenants` body.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateTenant {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub limits: TenantLimits,
    #[serde(default)]
    pub notifications: Vec<Value>,
}

fn check_fields(limits: &TenantLimits, notifications: &[Value]) -> Result<(), ServeError> {
    limits.validate().map_err(ServeError::BadConfig)?;
    tenants::validate_notifications(notifications).map_err(ServeError::BadConfig)
}

pub async fn create_tenant(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Json(body): Json<CreateTenant>,
) -> Result<(StatusCode, Json<TenantView>), ServeError> {
    validate_tenant_id(&body.id).map_err(ServeError::BadConfig)?;
    check_fields(&body.limits, &body.notifications)?;
    let history = state.history();
    if history
        .tenant_get(&body.id)
        .await
        .map_err(store_err)?
        .is_some()
    {
        return Err(ServeError::Conflict(format!(
            "tenant '{}' already exists",
            body.id
        )));
    }
    let now = Utc::now();
    let rec = TenantRecord {
        id: body.id,
        name: body.name,
        labels: body.labels,
        limits: body.limits,
        notifications: body.notifications,
        suspended: false,
        created_at: now,
        updated_at: now,
        created_by: actor.principal.clone(),
    };
    history.tenant_upsert(&rec).await.map_err(store_err)?;
    crate::serve::audit::write(
        &state,
        &for_tenant(&actor, &rec.id),
        "tenant.create",
        None,
        None,
        "ok",
    )
    .await;
    Ok((StatusCode::CREATED, Json(view(&state, rec).await?)))
}

pub async fn list_tenants(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
) -> Result<Json<Vec<TenantView>>, ServeError> {
    let mut out = Vec::new();
    for t in state.history().tenant_list().await.map_err(store_err)? {
        if actor.sees_tenant(Some(&t.id)) {
            out.push(view(&state, t).await?);
        }
    }
    Ok(Json(out))
}

pub async fn get_tenant(
    State(state): State<ServerState>,
    Path(tenant): Path<String>,
) -> Result<Json<TenantView>, ServeError> {
    let rec = tenants::get_tenant(&state, &tenant).await?;
    Ok(Json(view(&state, rec).await?))
}

/// `PATCH /v1/tenants/{tenant}` body: every field optional; a present field
/// replaces the stored one.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PatchTenant {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub labels: Option<BTreeMap<String, String>>,
    #[serde(default)]
    pub limits: Option<TenantLimits>,
    #[serde(default)]
    pub notifications: Option<Vec<Value>>,
    #[serde(default)]
    pub suspended: Option<bool>,
}

pub async fn patch_tenant(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path(tenant): Path<String>,
    Json(body): Json<PatchTenant>,
) -> Result<Json<TenantView>, ServeError> {
    let mut rec = tenants::get_tenant(&state, &tenant).await?;
    if let Some(n) = body.name {
        rec.name = Some(n).filter(|n| !n.trim().is_empty());
    }
    if let Some(l) = body.labels {
        rec.labels = l;
    }
    if let Some(l) = body.limits {
        rec.limits = l;
    }
    if let Some(n) = body.notifications {
        rec.notifications = n;
    }
    let action = match body.suspended {
        Some(true) if !rec.suspended => "tenant.suspend",
        Some(false) if rec.suspended => "tenant.resume",
        _ => "tenant.update",
    };
    if let Some(s) = body.suspended {
        rec.suspended = s;
    }
    check_fields(&rec.limits, &rec.notifications)?;
    rec.updated_at = Utc::now();
    state
        .history()
        .tenant_upsert(&rec)
        .await
        .map_err(store_err)?;
    crate::serve::audit::write(
        &state,
        &for_tenant(&actor, &tenant),
        action,
        None,
        None,
        "ok",
    )
    .await;
    Ok(Json(view(&state, rec).await?))
}

pub async fn delete_tenant(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path(tenant): Path<String>,
) -> Result<Json<tenants::DeleteReport>, ServeError> {
    let report = tenants::delete_tenant(&state, &tenant).await?;
    crate::serve::audit::write(
        &state,
        &for_tenant(&actor, &tenant),
        "tenant.delete",
        None,
        None,
        "ok",
    )
    .await;
    Ok(Json(report))
}

// ── Connections ──────────────────────────────────────────────────────────────

/// `POST /v1/tenants/{tenant}/connections` body.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateConnection {
    pub name: String,
    /// `{ type, config }` — the `auth:` catalog shape.
    pub provider: Value,
}

/// `PUT /v1/tenants/{tenant}/connections/{name}` body.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PutConnection {
    pub provider: Value,
}

/// Validate, seal and store a connection (create or replace). A replaced
/// connection is active again: storing new credentials is how a tenant
/// reconnects.
async fn store_connection(
    state: &ServerState,
    actor: &AuthContext,
    tenant: &str,
    name: &str,
    provider: Value,
    must_be_new: bool,
) -> Result<ConnectionRecord, ServeError> {
    tenants::get_tenant(state, tenant).await?;
    validate_connection_name(name).map_err(ServeError::BadConfig)?;
    let rt = state.tenants();
    let vault = rt.require_vault()?;
    let kind = provider
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| ServeError::BadConfig("provider: missing `type`".into()))?
        .to_string();
    faucet_auth::build_provider(&provider)
        .map_err(|e| ServeError::BadConfig(format!("provider: {e}")))?;
    tenants::register_secrets(&provider);
    let history = state.history();
    let existing = history
        .connection_get(tenant, name)
        .await
        .map_err(store_err)?;
    if must_be_new && existing.is_some() {
        return Err(ServeError::Conflict(format!(
            "tenant '{tenant}' already has a connection '{name}' (use PUT to replace it)"
        )));
    }
    let now = Utc::now();
    let rec = ConnectionRecord {
        tenant: tenant.to_string(),
        name: name.to_string(),
        provider_type: kind,
        connect_provider: None,
        sealed: vault.seal(&provider),
        status: ConnectionStatus::Active,
        reauth_reason: None,
        created_at: existing.map(|e| e.created_at).unwrap_or(now),
        updated_at: now,
        updated_by: actor.principal.clone(),
    };
    history.connection_upsert(&rec).await.map_err(store_err)?;
    tenants::metrics::refresh_connection_gauges(state).await;
    crate::serve::audit::write(
        state,
        &for_tenant(actor, tenant),
        "connection.upsert",
        None,
        None,
        "ok",
    )
    .await;
    Ok(rec)
}

pub async fn create_connection(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path(tenant): Path<String>,
    Json(body): Json<CreateConnection>,
) -> Result<(StatusCode, Json<ConnectionView>), ServeError> {
    let rec = store_connection(&state, &actor, &tenant, &body.name, body.provider, true).await?;
    Ok((StatusCode::CREATED, Json(ConnectionView::from(&rec))))
}

pub async fn put_connection(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path((tenant, name)): Path<(String, String)>,
    Json(body): Json<PutConnection>,
) -> Result<Json<ConnectionView>, ServeError> {
    let rec = store_connection(&state, &actor, &tenant, &name, body.provider, false).await?;
    Ok(Json(ConnectionView::from(&rec)))
}

pub async fn list_connections(
    State(state): State<ServerState>,
    Path(tenant): Path<String>,
) -> Result<Json<Vec<ConnectionView>>, ServeError> {
    tenants::get_tenant(&state, &tenant).await?;
    Ok(Json(
        state
            .history()
            .connection_list(&tenant)
            .await
            .map_err(store_err)?
            .iter()
            .map(ConnectionView::from)
            .collect(),
    ))
}

pub async fn get_connection(
    State(state): State<ServerState>,
    Path((tenant, name)): Path<(String, String)>,
) -> Result<Json<ConnectionView>, ServeError> {
    let rec = state
        .history()
        .connection_get(&tenant, &name)
        .await
        .map_err(store_err)?
        .ok_or(ServeError::NotFound)?;
    Ok(Json(ConnectionView::from(&rec)))
}

pub async fn delete_connection(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path((tenant, name)): Path<(String, String)>,
) -> Result<StatusCode, ServeError> {
    if !state
        .history()
        .connection_delete(&tenant, &name)
        .await
        .map_err(store_err)?
    {
        return Err(ServeError::NotFound);
    }
    tenants::metrics::refresh_connection_gauges(&state).await;
    crate::serve::audit::write(
        &state,
        &for_tenant(&actor, &tenant),
        "connection.delete",
        None,
        None,
        "ok",
    )
    .await;
    Ok(StatusCode::NO_CONTENT)
}

// ── Hosted OAuth connect ─────────────────────────────────────────────────────

pub async fn start_connect(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path((tenant, provider)): Path<(String, String)>,
    Json(body): Json<connect::StartRequest>,
) -> Result<Json<connect::StartResponse>, ServeError> {
    let actor = for_tenant(&actor, &tenant);
    Ok(Json(
        connect::start(&state, &actor, &tenant, &provider, body).await?,
    ))
}

/// `GET /v1/connect/callback` — public; redirects the browser back to the
/// embedding product.
pub async fn connect_callback(
    State(state): State<ServerState>,
    Query(q): Query<connect::CallbackQuery>,
) -> Result<Response, ServeError> {
    let to = connect::callback(&state, q).await?;
    Ok(Redirect::to(&to).into_response())
}

/// `GET /v1/connect/providers` response element.
#[derive(Debug, Serialize)]
pub struct ProviderView {
    pub name: String,
    pub scopes: Vec<String>,
}

pub async fn list_connect_providers(State(state): State<ServerState>) -> Json<Vec<ProviderView>> {
    let rt = state.tenants();
    Json(
        rt.providers
            .names()
            .into_iter()
            .filter_map(|n| {
                rt.providers.get(&n).map(|p| ProviderView {
                    name: n.clone(),
                    scopes: p.scopes.clone(),
                })
            })
            .collect(),
    )
}

// ── Tenant runs ──────────────────────────────────────────────────────────────

pub async fn submit_tenant_run(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path(tenant): Path<String>,
    Json(req): Json<SubmitRequest>,
) -> Result<Response, ServeError> {
    let actor = for_tenant(&actor, &tenant);
    match runner::submit_gated(state, req, actor).await? {
        SubmitOutcome::Accepted(resp) => Ok((StatusCode::ACCEPTED, Json(resp)).into_response()),
        SubmitOutcome::PendingApproval(change) => Ok((
            StatusCode::ACCEPTED,
            Json(runner::pending_approval_body(&change)),
        )
            .into_response()),
    }
}

pub async fn trigger_tenant_template(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path((tenant, id)): Path<(String, String)>,
    Json(body): Json<TriggerBody>,
) -> Result<Response, ServeError> {
    let actor = for_tenant(&actor, &tenant);
    match trigger_template_outcome(state, actor, id, body).await? {
        TriggerOutcome::Run(resp) => Ok((StatusCode::ACCEPTED, Json(resp)).into_response()),
        TriggerOutcome::PendingApproval(change) => Ok((
            StatusCode::ACCEPTED,
            Json(runner::pending_approval_body(&change)),
        )
            .into_response()),
    }
}

// ── Fan-out ──────────────────────────────────────────────────────────────────

/// Resolve a selector to tenant ids: `all` skips suspended tenants; a named
/// tenant that is suspended or does not exist is reported, not silently
/// dropped.
pub async fn resolve_tenants(
    state: &ServerState,
    selector: &TenantSelector,
) -> Result<(Vec<String>, Vec<FanoutResult>), ServeError> {
    let all = state.history().tenant_list().await.map_err(store_err)?;
    match selector {
        TenantSelector::All(_) => Ok((
            all.into_iter()
                .filter(|t| !t.suspended)
                .map(|t| t.id)
                .collect(),
            Vec::new(),
        )),
        TenantSelector::Named(ids) => {
            let known: BTreeMap<&str, &TenantRecord> =
                all.iter().map(|t| (t.id.as_str(), t)).collect();
            let mut run = Vec::new();
            let mut skipped = Vec::new();
            for id in ids {
                match known.get(id.as_str()) {
                    Some(t) if t.suspended => {
                        skipped.push(FanoutResult::skipped(id, "tenant is suspended"))
                    }
                    Some(_) => run.push(id.clone()),
                    None => skipped.push(FanoutResult::skipped(id, "no such tenant")),
                }
            }
            Ok((run, skipped))
        }
    }
}

/// `POST /v1/templates/{id}/fanout` body: a trigger body plus the tenants.
#[derive(Debug, Clone, Deserialize)]
pub struct FanoutBody {
    pub tenants: TenantSelector,
    /// Submissions in flight at once. Default 4.
    #[serde(default)]
    pub concurrency: Option<usize>,
    #[serde(flatten)]
    pub trigger: TriggerBody,
}

/// One tenant's outcome.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct FanoutResult {
    pub tenant: String,
    /// `submitted`, `pending_approval`, `skipped` or `failed`.
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub change_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl FanoutResult {
    pub fn skipped(tenant: &str, reason: impl Into<String>) -> Self {
        Self {
            tenant: tenant.to_string(),
            status: "skipped".into(),
            run_id: None,
            change_id: None,
            reason: Some(reason.into()),
        }
    }
}

/// `POST /v1/templates/{id}/fanout` response.
#[derive(Debug, Serialize)]
pub struct FanoutResponse {
    pub fanout_id: String,
    pub template_id: String,
    pub results: Vec<FanoutResult>,
}

/// Whether a trigger failure means "this tenant is not ready" (skip) rather
/// than a real failure: a missing or revoked connection, a suspended tenant,
/// or a tenant at its concurrency limit.
fn is_skip(e: &ServeError) -> bool {
    matches!(e, ServeError::Conflict(_) | ServeError::TooManyRequests(_))
}

/// Trigger a template once per tenant, at most `concurrency` at a time.
pub async fn fan_out(
    state: &ServerState,
    actor: &AuthContext,
    template_id: &str,
    body: FanoutBody,
    fanout_id: &str,
) -> Result<Vec<FanoutResult>, ServeError> {
    body.tenants.validate().map_err(ServeError::BadConfig)?;
    let concurrency = body
        .concurrency
        .unwrap_or(DEFAULT_FANOUT_CONCURRENCY)
        .clamp(1, MAX_FANOUT_CONCURRENCY);
    let (targets, mut results) = resolve_tenants(state, &body.tenants).await?;
    let sem = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let mut set = tokio::task::JoinSet::new();
    for tenant in targets {
        let permit = sem
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore not closed");
        let state = state.clone();
        let actor = for_tenant(actor, &tenant);
        let id = template_id.to_string();
        let mut trigger = body.trigger.clone();
        trigger
            .labels
            .insert("fanout".to_string(), fanout_id.to_string());
        if let Some(k) = &trigger.idempotency_key {
            trigger.idempotency_key = Some(format!("{k}:{tenant}"));
        }
        set.spawn(async move {
            let _permit = permit;
            let outcome = trigger_template_outcome(state, actor, id, trigger).await;
            match outcome {
                Ok(TriggerOutcome::Run(r)) => FanoutResult {
                    tenant,
                    status: "submitted".into(),
                    run_id: Some(r.run.run_id),
                    change_id: None,
                    reason: None,
                },
                Ok(TriggerOutcome::PendingApproval(c)) => FanoutResult {
                    tenant,
                    status: "pending_approval".into(),
                    run_id: None,
                    change_id: Some(c.id.clone()),
                    reason: None,
                },
                Err(e) if is_skip(&e) => {
                    FanoutResult::skipped(&tenant, e.api_error().error.message)
                }
                Err(e) => FanoutResult {
                    tenant,
                    status: "failed".into(),
                    run_id: None,
                    change_id: None,
                    reason: Some(e.api_error().error.message),
                },
            }
        });
    }
    while let Some(r) = set.join_next().await {
        match r {
            Ok(res) => results.push(res),
            Err(e) => tracing::error!(error = %e, "fan-out task panicked"),
        }
    }
    results.sort_by(|a, b| a.tenant.cmp(&b.tenant));
    Ok(results)
}

pub async fn fanout_template(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path(id): Path<String>,
    Json(body): Json<FanoutBody>,
) -> Result<Json<FanoutResponse>, ServeError> {
    let fanout_id = uuid::Uuid::now_v7().to_string();
    let results = fan_out(&state, &actor, &id, body, &fanout_id).await?;
    crate::serve::audit::write(
        &state,
        &actor,
        "template.fanout",
        None,
        Some(fanout_id.clone()),
        "ok",
    )
    .await;
    Ok(Json(FanoutResponse {
        fanout_id,
        template_id: id,
        results,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skips_are_readiness_errors() {
        assert!(is_skip(&ServeError::Conflict("x".into())));
        assert!(is_skip(&ServeError::TooManyRequests("x".into())));
        assert!(!is_skip(&ServeError::BadConfig("x".into())));
        let s = FanoutResult::skipped("a", "why");
        assert_eq!(
            (s.status.as_str(), s.reason.as_deref()),
            ("skipped", Some("why"))
        );
    }
}
