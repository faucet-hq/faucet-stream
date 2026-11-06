//! `/v1/policy*` and `/v1/tenants/{tenant}/policy*` — the data-flow policy
//! in the console (#1043).
//!
//! | route | permission |
//! |---|---|
//! | `GET /v1/policy`, `GET /v1/policy/refusals` | `PolicyRead` (viewer+; a tenant principal sees the server rules that apply to it and its own refusals) |
//! | `GET /v1/policy/versions` | `PolicyRead` (unscoped principals) |
//! | `POST /v1/policy/check` | `Plan` (operator+; not for tenant principals) |
//! | `POST /v1/policy/preview`, `POST /v1/policy`, `POST /v1/policy/rollback` | `PolicyAdmin` (admin, unscoped) |
//! | `GET /v1/tenants/{tenant}/policy` | `PolicyRead` |
//! | `POST /v1/tenants/{tenant}/policy[/preview\|/rollback]` | `PolicyAdmin` (an admin of that tenant, or a server admin) |
//!
//! A change is never applied directly: `POST` creates a change request
//! (`policy_update` / `tenant_policy_update`) that another admin approves
//! through `/v1/changes/{id}/approve`. A server started with `--policy
//! <file>` shows that file read-only and refuses edits with a 409.

use crate::serve::changes::policy_update::{
    PolicyPayload, require_persistent, require_server_managed,
};
use crate::serve::changes::{self, ChangeKind, ChangeListFilter, ChangeRequest, ChangeStatus};
use crate::serve::error::ServeError;
use crate::serve::history::policies::{
    DEFAULT_VERSION_LIMIT, PolicyVersionRecord, SERVER_SCOPE, tenant_scope,
};
use crate::serve::policies::{self, Layer, PolicySource};
use crate::serve::rbac::{AuthContext, Role};
use crate::serve::runner::ConfigFormatWire;
use crate::serve::state::ServerState;
use axum::Json;
use axum::extract::{Extension, Path, Query, State};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Why a tenant principal cannot use a server-wide policy route.
const TENANT_SERVER_POLICY: &str = "a tenant-scoped principal cannot change or dry-run the \
     server's policy; its own tenant policy is at /v1/tenants/{tenant}/policy";

fn store_err(e: crate::serve::history::HistoryError) -> ServeError {
    ServeError::from(e)
}

fn unscoped(actor: &AuthContext) -> Result<(), ServeError> {
    if actor.tenant.is_some() {
        return Err(ServeError::Forbidden(TENANT_SERVER_POLICY.into()));
    }
    Ok(())
}

/// A stored version without its spec, for listings.
#[derive(Debug, Serialize)]
pub struct VersionView {
    pub version: u32,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub created_by: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approved_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub change_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restores: Option<u32>,
    pub rules: usize,
    pub classifications: usize,
    /// The version's document as canonical YAML (what a rollback restores).
    pub yaml: String,
}

impl From<&PolicyVersionRecord> for VersionView {
    fn from(r: &PolicyVersionRecord) -> Self {
        let count = |k: &str| r.spec.get(k).and_then(Value::as_array).map_or(0, Vec::len);
        Self {
            yaml: policies::spec_of(r)
                .map(|s| policies::canonical_yaml(&s))
                .unwrap_or_default(),
            version: r.version,
            created_at: r.created_at,
            created_by: r.created_by.clone(),
            approved_by: r.approved_by.clone(),
            reason: r.reason.clone(),
            change_id: r.change_id.clone(),
            restores: r.restores,
            rules: count("rules"),
            classifications: count("classifications"),
        }
    }
}

/// The pending policy change for `scope`, if any (newest first).
async fn pending_change(
    state: &ServerState,
    kind: ChangeKind,
    tenant: Option<&str>,
) -> Result<Option<Value>, ServeError> {
    let pending = changes::list(
        state,
        &ChangeListFilter {
            status: Some(ChangeStatus::Pending),
            kind: Some(kind),
            tenant: tenant.map(str::to_string),
            limit: 50,
            ..Default::default()
        },
    )
    .await?;
    Ok(pending
        .into_iter()
        .find(|c| c.tenant.as_deref() == tenant)
        .map(|c| {
            json!({
                "id": c.id,
                "requester": c.requester,
                "created_at": c.created_at,
                "next_version": c.plan.as_ref().and_then(|p| p.summary.get("next_version")).cloned(),
            })
        }))
}

/// `GET /v1/policy` → the server policy in force.
pub async fn get_policy(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
) -> Result<Json<Value>, ServeError> {
    let layer = policies::server_layer(&state).await?;
    let rules = layer
        .spec
        .as_ref()
        .map(policies::outline)
        .unwrap_or_default();
    let refusals = policies::refusal_counts(&state, actor.tenant.as_deref()).await;
    let source = match layer.source {
        PolicySource::File => "file",
        PolicySource::Server => "server",
    };
    if let Some(tenant) = &actor.tenant {
        // A tenant sees which server rules apply to it and what they require,
        // not the classifications behind them or the rest of the policy.
        return Ok(Json(json!({
            "source": source,
            "loaded": layer.spec.is_some(),
            "scoped_tenant": tenant,
            "rules": rules,
            "refusals_7d": refusals,
            "editable": false,
        })));
    }
    let persistent = state.persistent_history();
    let editable = actor.role == Role::Admin && layer.source == PolicySource::Server && persistent;
    let summary = match &layer.spec {
        Some(spec) => json!({
            "rules": spec.rules.len(),
            "labels": spec.classifications.iter().map(|c| c.label.as_str()).collect::<std::collections::BTreeSet<_>>().len(),
            "value_detectors": spec.has_value_detectors(),
            "sinks_with_attributes": policies::sink_attributes(&state).await,
            "columns_seen": policies::columns_seen(&state, spec).await,
        }),
        None => Value::Null,
    };
    Ok(Json(json!({
        "source": source,
        "loaded": layer.spec.is_some(),
        "file": layer.file,
        "version": layer.version.as_ref().map(VersionView::from),
        "policy": layer.spec,
        "yaml": layer.spec.as_ref().map(policies::canonical_yaml),
        "rules": rules,
        "summary": summary,
        "refusals_7d": refusals,
        "editable": editable,
        "store": state.policy_store(),
        "persistent_history": persistent,
        "file_drift": policies::file_drift(&state).await?,
        "pending_change": pending_change(&state, ChangeKind::PolicyUpdate, None).await?,
    })))
}

/// `?limit=` for version listings.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VersionsQuery {
    pub limit: Option<usize>,
}

/// `GET /v1/policy/versions` → the server-managed policy's versions,
/// newest first.
pub async fn list_versions(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Query(q): Query<VersionsQuery>,
) -> Result<Json<Vec<VersionView>>, ServeError> {
    unscoped(&actor)?;
    let versions = state
        .history()
        .policy_versions(SERVER_SCOPE, q.limit.unwrap_or(DEFAULT_VERSION_LIMIT))
        .await
        .map_err(store_err)?;
    Ok(Json(versions.iter().map(VersionView::from).collect()))
}

/// `GET /v1/policy/refusals` query.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RefusalsQuery {
    pub since: Option<chrono::DateTime<chrono::Utc>>,
    pub tenant: Option<String>,
    pub limit: Option<usize>,
}

/// `GET /v1/policy/refusals` → submit-time and run-time refusals, newest
/// first; a tenant principal sees its own only.
pub async fn list_refusals(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Query(q): Query<RefusalsQuery>,
) -> Result<Json<Vec<policies::Refusal>>, ServeError> {
    let tenant = actor.tenant_filter(q.tenant)?;
    Ok(Json(
        policies::refusals(
            &state,
            tenant.as_deref(),
            q.since,
            q.limit.unwrap_or(200).clamp(1, 2000),
        )
        .await?,
    ))
}

/// `POST /v1/policy/check` body: a config (or a registered template) to
/// check against the policy in force, without running it.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckRequest {
    /// The config to check.
    #[serde(default)]
    pub config: String,
    #[serde(default)]
    pub config_format: ConfigFormatWire,
    /// Check a registered pipeline template instead of `config`.
    #[serde(default)]
    pub template: Option<String>,
    /// The template version (default: stable, else newest).
    #[serde(default)]
    pub version: Option<u32>,
    /// Only this row.
    #[serde(default)]
    pub row: Option<String>,
    /// Sample records for `row`: column labels come from real values.
    #[serde(default)]
    pub sample: Option<Vec<Value>>,
}

/// `POST /v1/policy/check` → the policy report for a config or template.
pub async fn check(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Json(req): Json<CheckRequest>,
) -> Result<Json<Value>, ServeError> {
    unscoped(&actor)?;
    if let Some(id) = req.template.clone() {
        #[cfg(feature = "templates")]
        {
            let report = policies::check_template(&state, None, &id, req.version).await?;
            crate::serve::audit::write_target(
                &state,
                &actor,
                "policy.check",
                format!("template:{id}"),
                "ok",
            )
            .await;
            return Ok(Json(report));
        }
        #[cfg(not(feature = "templates"))]
        {
            let _ = id;
            return Err(ServeError::Unprocessable {
                message: "checking a template requires the `templates` feature".into(),
                details: None,
            });
        }
    }
    if req.config.trim().is_empty() {
        return Err(ServeError::BadConfig("set `config` or `template`".into()));
    }
    let principal = crate::runtime::Principal::caller(&state, actor);
    crate::runtime::server::policy_check(&state, &principal, req)
        .await
        .map(Json)
}

/// A draft for `POST …/preview`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreviewRequest {
    /// The policy document (YAML or JSON) as written.
    pub policy: String,
}

/// What a preview answers: the draft's problems, or its diff and impact.
#[derive(Debug, Serialize)]
pub struct PreviewResponse {
    pub valid: bool,
    pub problems: Vec<policies::Problem>,
    pub current_version: Option<u32>,
    pub next_version: u32,
    pub diff: Vec<policies::DiffLine>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub impact: Option<policies::Impact>,
}

async fn preview_layer(
    state: &ServerState,
    layer: Layer,
    text: &str,
) -> Result<PreviewResponse, ServeError> {
    require_server_managed(state, &layer)?;
    let scope = layer.scope();
    let current = policies::latest(state, &scope).await?;
    let current_version = current.as_ref().map(|r| r.version);
    let next_version = current_version.unwrap_or(0) + 1;
    let (draft, mut problems) = policies::parse_draft_all(text, &layer);
    let Some(draft) = draft else {
        return Ok(PreviewResponse {
            valid: false,
            problems,
            current_version,
            next_version,
            diff: Vec::new(),
            impact: None,
        });
    };
    let mut checked = policies::check_draft(state, &layer, &draft, text).await?;
    checked.append(&mut problems);
    let mut problems = checked;
    problems.sort_by_key(|p| p.line.unwrap_or(usize::MAX));
    let before_text = match &current {
        Some(r) => policies::canonical_yaml(&policies::spec_of(r)?),
        None => String::new(),
    };
    let diff = policies::line_diff(&before_text, &policies::canonical_yaml(&draft));
    if !problems.is_empty() {
        return Ok(PreviewResponse {
            valid: false,
            problems,
            current_version,
            next_version,
            diff,
            impact: None,
        });
    }
    let impact = policies::draft_impact(state, &layer, draft).await?;
    Ok(PreviewResponse {
        valid: true,
        problems: Vec::new(),
        current_version,
        next_version,
        diff,
        impact: Some(impact),
    })
}

/// `POST /v1/policy/preview` → validate a draft of the server policy and
/// show what it changes. Never stores anything.
pub async fn preview(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Json(req): Json<PreviewRequest>,
) -> Result<Json<PreviewResponse>, ServeError> {
    unscoped(&actor)?;
    Ok(Json(
        preview_layer(&state, Layer::Server, &req.policy).await?,
    ))
}

/// A proposed policy change.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProposeRequest {
    /// The policy document (YAML or JSON) as written.
    pub policy: String,
    /// Why — shown to the approver and kept with the version. Required.
    pub reason: String,
    /// The version the draft was edited from (`None` when there was none);
    /// refused with 409 when another version is in force by now.
    #[serde(default)]
    pub base_version: Option<u32>,
}

/// A rollback to an earlier version.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RollbackRequest {
    pub version: u32,
    pub reason: String,
}

async fn propose(
    state: &ServerState,
    actor: &AuthContext,
    layer: Layer,
    policy: String,
    reason: String,
    base_version: Option<u32>,
    restores: Option<u32>,
) -> Result<(StatusCode, Json<ChangeRequest>), ServeError> {
    require_persistent(state)?;
    require_server_managed(state, &layer)?;
    if reason.trim().is_empty() {
        return Err(ServeError::BadConfig(
            "a policy change needs a reason; it is shown to the approver and kept with the \
             version"
                .into(),
        ));
    }
    let current = policies::latest(state, &layer.scope())
        .await?
        .map(|r| r.version);
    if base_version != current {
        return Err(ServeError::Conflict(format!(
            "the policy changed since this draft was started: {} is in force now; reload it and \
             re-apply your edit",
            current.map_or_else(|| "no version".to_string(), |v| format!("version {v}"))
        )));
    }
    let kind = match layer {
        Layer::Server => ChangeKind::PolicyUpdate,
        Layer::Tenant(_) => ChangeKind::TenantPolicyUpdate,
    };
    let payload = PolicyPayload {
        tenant: layer.tenant().map(str::to_string),
        policy,
        base_version,
        restores,
    };
    let change = changes::create(
        state,
        actor,
        changes::NewChange {
            kind,
            payload: serde_json::to_value(&payload)
                .map_err(|e| ServeError::Internal(e.to_string()))?,
            reason: Some(reason),
            budget: None,
            trusted_config: false,
            caller_params: Default::default(),
        },
    )
    .await?;
    Ok((StatusCode::CREATED, Json(change.redacted_for(actor))))
}

/// `POST /v1/policy` → 201 with the `policy_update` change request.
pub async fn propose_server(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Json(req): Json<ProposeRequest>,
) -> Result<(StatusCode, Json<ChangeRequest>), ServeError> {
    unscoped(&actor)?;
    propose(
        &state,
        &actor,
        Layer::Server,
        req.policy,
        req.reason,
        req.base_version,
        None,
    )
    .await
}

async fn rollback_layer(
    state: &ServerState,
    actor: &AuthContext,
    layer: Layer,
    req: RollbackRequest,
) -> Result<(StatusCode, Json<ChangeRequest>), ServeError> {
    require_persistent(state)?;
    require_server_managed(state, &layer)?;
    let scope = layer.scope();
    let target = state
        .history()
        .policy_version_get(&scope, req.version)
        .await
        .map_err(store_err)?
        .ok_or(ServeError::NotFound)?;
    let current = policies::latest(state, &scope).await?.map(|r| r.version);
    if current == Some(req.version) {
        return Err(ServeError::Conflict(format!(
            "version {} is already in force",
            req.version
        )));
    }
    let text = policies::canonical_yaml(&policies::spec_of(&target)?);
    propose(
        state,
        actor,
        layer,
        text,
        req.reason,
        current,
        Some(req.version),
    )
    .await
}

/// `POST /v1/policy/rollback` → 201 with a `policy_update` change request
/// re-enacting an earlier version.
pub async fn rollback_server(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Json(req): Json<RollbackRequest>,
) -> Result<(StatusCode, Json<ChangeRequest>), ServeError> {
    unscoped(&actor)?;
    rollback_layer(&state, &actor, Layer::Server, req).await
}

// ── Tenant policies ──────────────────────────────────────────────────────────

/// The tenant a `/v1/tenants/{tenant}/policy*` route names, checked: it
/// exists, and a tenant principal reaches only its own (else 404).
#[cfg(feature = "tenants")]
async fn tenant_in_scope(
    state: &ServerState,
    actor: &AuthContext,
    tenant: &str,
) -> Result<(), ServeError> {
    actor.tenant_filter(Some(tenant.to_string()))?;
    state
        .history()
        .tenant_get(tenant)
        .await
        .map_err(ServeError::from)?
        .ok_or(ServeError::NotFound)?;
    Ok(())
}

/// `GET /v1/tenants/{tenant}/policy` → the server rules that apply to the
/// tenant, its own add-only policy and its versions.
#[cfg(feature = "tenants")]
pub async fn get_tenant_policy(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path(tenant): Path<String>,
) -> Result<Json<Value>, ServeError> {
    tenant_in_scope(&state, &actor, &tenant).await?;
    let server = policies::server_layer(&state).await?;
    let versions = state
        .history()
        .policy_versions(&tenant_scope(&tenant), DEFAULT_VERSION_LIMIT)
        .await
        .map_err(store_err)?;
    let current = versions.first();
    let spec = current.map(policies::spec_of).transpose()?;
    Ok(Json(json!({
        "tenant": tenant,
        "server": {
            "source": match server.source {
                PolicySource::File => "file",
                PolicySource::Server => "server",
            },
            "loaded": server.spec.is_some(),
            "version": server.version.as_ref().map(|r| r.version),
            "rules": server.spec.as_ref().map(policies::outline).unwrap_or_default(),
            "labels": server.spec.as_ref().map(|s| s.classifications.iter().map(|c| c.label.clone()).collect::<std::collections::BTreeSet<_>>()).unwrap_or_default(),
        },
        "loaded": spec.is_some(),
        "policy": spec,
        "yaml": spec.as_ref().map(policies::canonical_yaml),
        "rules": spec.as_ref().map(policies::outline).unwrap_or_default(),
        "version": current.map(VersionView::from),
        "versions": versions.iter().map(VersionView::from).collect::<Vec<_>>(),
        "refusals_7d": policies::refusal_counts(&state, Some(&tenant)).await,
        "editable": actor.role == Role::Admin && state.persistent_history(),
        "persistent_history": state.persistent_history(),
        "pending_change": pending_change(&state, ChangeKind::TenantPolicyUpdate, Some(&tenant)).await?,
    })))
}

/// `POST /v1/tenants/{tenant}/policy/preview`.
#[cfg(feature = "tenants")]
pub async fn preview_tenant(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path(tenant): Path<String>,
    Json(req): Json<PreviewRequest>,
) -> Result<Json<PreviewResponse>, ServeError> {
    tenant_in_scope(&state, &actor, &tenant).await?;
    Ok(Json(
        preview_layer(&state, Layer::Tenant(tenant), &req.policy).await?,
    ))
}

/// `POST /v1/tenants/{tenant}/policy` → 201 with a `tenant_policy_update`
/// change request.
#[cfg(feature = "tenants")]
pub async fn propose_tenant(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path(tenant): Path<String>,
    Json(req): Json<ProposeRequest>,
) -> Result<(StatusCode, Json<ChangeRequest>), ServeError> {
    tenant_in_scope(&state, &actor, &tenant).await?;
    propose(
        &state,
        &actor,
        Layer::Tenant(tenant),
        req.policy,
        req.reason,
        req.base_version,
        None,
    )
    .await
}

/// `POST /v1/tenants/{tenant}/policy/rollback`.
#[cfg(feature = "tenants")]
pub async fn rollback_tenant(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Path(tenant): Path<String>,
    Json(req): Json<RollbackRequest>,
) -> Result<(StatusCode, Json<ChangeRequest>), ServeError> {
    tenant_in_scope(&state, &actor, &tenant).await?;
    rollback_layer(&state, &actor, Layer::Tenant(tenant), req).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_policy_routes_refuse_a_tenant_principal() {
        let mut actor = AuthContext {
            principal: "a".into(),
            role: Role::Admin,
            source_ip: None,
            tenant: None,
        };
        assert!(unscoped(&actor).is_ok());
        actor.tenant = Some("acme".into());
        assert!(matches!(unscoped(&actor), Err(ServeError::Forbidden(_))));
    }
}
