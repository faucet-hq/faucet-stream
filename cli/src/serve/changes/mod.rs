//! Change requests (#703): plan → approve → run.
//!
//! A **change request** is a proposed action — run a config, register a
//! template version, launch a template version — stored with the review
//! material `faucet plan` produces (the resolved rows, delivery guarantees,
//! policy verdicts, impact) and the requester, and executed only once the
//! approvers the [`policy`] names have approved it. Humans propose through
//! `POST /v1/changes` or the console; agents through the MCP `propose_run` /
//! `propose_template` tools. `faucet serve --require-approval <kinds>` makes
//! the gate mandatory for those kinds (`POST /v1/runs` then answers with a
//! pending request instead of a run); otherwise a request is opt-in.
//!
//! **The world may change between approval and execution.** Every request
//! stores a *material* fingerprint of its plan (per row: source, sink, write
//! mode, delivery guarantee, transform chain; for a template launch: the
//! target and the version it replaces). Execution re-plans and compares; a
//! difference marks the request `invalidated` and runs nothing — the approver
//! reviewed something else. A rotated secret or a re-ordered comment is not
//! material and does not invalidate.
//!
//! **Budgets** travel with the request: the approved [`BudgetSpec`] is merged
//! into the run (the stricter of it and the config's own `budget:`), so an
//! approved change can never exceed what was agreed — enforced by the
//! executor's `BudgetSink` (a crossing page is refused whole).
//!
//! Storage rides the run-history backends (`RunHistory::change_*`); a request
//! lives in shared history, so in a cluster any instance may execute it. Every
//! transition is audited (`change.requested` / `approved` / `rejected` /
//! `executed` / `invalidated` / `expired` / `failed`).

pub mod policy;

pub use policy::{ApprovalPolicy, ApprovalRule, EffectiveRule};

use crate::serve::error::ServeError;
use crate::serve::history::HistoryError;
use crate::serve::rbac::{AuthContext, Role};
use crate::serve::runner::{self, SubmitRequest};
use crate::serve::state::ServerState;
use chrono::{DateTime, Utc};
use faucet_core::{BudgetSpec, JsonSchema};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::str::FromStr;

/// What a change request proposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ChangeKind {
    /// Run a config (`POST /v1/runs` body).
    Run,
    /// Register a template version (`POST /v1/templates` body).
    TemplateRegister,
    /// Launch a template version (`POST /v1/templates/{id}/launch`).
    TemplateLaunch,
}

impl ChangeKind {
    pub const ALL: [ChangeKind; 3] = [
        ChangeKind::Run,
        ChangeKind::TemplateRegister,
        ChangeKind::TemplateLaunch,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            ChangeKind::Run => "run",
            ChangeKind::TemplateRegister => "template_register",
            ChangeKind::TemplateLaunch => "template_launch",
        }
    }
}

impl FromStr for ChangeKind {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s.trim() {
            "run" => Ok(ChangeKind::Run),
            "template_register" | "template-register" => Ok(ChangeKind::TemplateRegister),
            "template_launch" | "template-launch" => Ok(ChangeKind::TemplateLaunch),
            other => Err(format!(
                "unknown change kind `{other}` (expected run, template_register or template_launch)"
            )),
        }
    }
}

impl std::fmt::Display for ChangeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Where a request is in its life.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ChangeStatus {
    /// Waiting for approvals (possibly some already given, below quorum).
    Pending,
    /// The quorum approved; execution is in progress.
    Approved,
    /// An approver rejected it (or the requester withdrew it).
    Rejected,
    /// Executed: `run_id` / `template` name the result.
    Executed,
    /// Lapsed before the quorum approved.
    Expired,
    /// The quorum approved, but the plan had materially changed since the
    /// request was made; nothing ran.
    Invalidated,
    /// Execution was attempted and failed; `error` says why.
    Failed,
}

impl ChangeStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            ChangeStatus::Pending => "pending",
            ChangeStatus::Approved => "approved",
            ChangeStatus::Rejected => "rejected",
            ChangeStatus::Executed => "executed",
            ChangeStatus::Expired => "expired",
            ChangeStatus::Invalidated => "invalidated",
            ChangeStatus::Failed => "failed",
        }
    }

    pub fn is_terminal(self) -> bool {
        !matches!(self, ChangeStatus::Pending | ChangeStatus::Approved)
    }
}

impl FromStr for ChangeStatus {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        Ok(match s {
            "pending" => ChangeStatus::Pending,
            "approved" => ChangeStatus::Approved,
            "rejected" => ChangeStatus::Rejected,
            "executed" => ChangeStatus::Executed,
            "expired" => ChangeStatus::Expired,
            "invalidated" => ChangeStatus::Invalidated,
            "failed" => ChangeStatus::Failed,
            other => return Err(format!("unknown change status `{other}`")),
        })
    }
}

/// One approval.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Approval {
    pub principal: String,
    pub role: Role,
    #[schemars(with = "String")]
    pub at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// The rejection, when there is one.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Rejection {
    pub principal: String,
    #[schemars(with = "String")]
    pub at: DateTime<Utc>,
    pub reason: String,
}

/// The review material, computed when the request is made and again at
/// execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ChangePlan {
    /// Hash of the *material* part of the plan — what an approver reviewed.
    /// Execution recomputes it; a difference invalidates the approval.
    pub material: String,
    /// The plan itself: one `faucet plan` report per root row for a run, the
    /// registered document's rows for a pipeline template, the launch
    /// summary for a launch.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rows: Vec<Value>,
    /// A short structured summary (pipeline name, row count, the template
    /// and versions involved, …) the console renders at a glance.
    pub summary: Value,
}

/// What a `template_*` execution produced.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct TemplateOutcome {
    pub id: String,
    pub version: u32,
}

/// A stored change request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ChangeRequest {
    pub id: String,
    pub kind: ChangeKind,
    pub status: ChangeStatus,
    pub requester: String,
    pub requester_role: Role,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// The proposed action, verbatim: a `POST /v1/runs` body, a
    /// `POST /v1/templates` body, or `{ id, version }` for a launch. Secrets
    /// are never resolved into it.
    pub payload: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan: Option<ChangePlan>,
    /// Ceilings the approved run must honour (merged with the config's own
    /// `budget:`, the stricter wins).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget: Option<BudgetSpec>,
    /// Distinct approvals needed before execution.
    pub required_approvals: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub approvals: Vec<Approval>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rejection: Option<Rejection>,
    #[schemars(with = "String")]
    pub created_at: DateTime<Utc>,
    #[schemars(with = "String")]
    pub updated_at: DateTime<Utc>,
    /// When a pending request lapses.
    #[schemars(with = "String")]
    pub expires_at: DateTime<Utc>,
    /// The run an executed `run` request started.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// The version an executed `template_*` request produced / launched.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template: Option<TemplateOutcome>,
    /// Why execution failed or the approval was invalidated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// The tenant the proposed run is for (#709): the requester's own tenant
    /// or the tenant route it was proposed through. Execution runs it as that
    /// tenant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
}

impl ChangeRequest {
    /// The record without the plan rows — what a listing carries.
    pub fn without_rows(mut self) -> Self {
        if let Some(p) = &mut self.plan {
            p.rows = Vec::new();
        }
        self
    }

    fn touch(&mut self) {
        self.updated_at = Utc::now();
    }
}

/// `GET /v1/changes` filter.
#[derive(Debug, Clone, Default)]
pub struct ChangeListFilter {
    pub status: Option<ChangeStatus>,
    pub kind: Option<ChangeKind>,
    pub requester: Option<String>,
    /// Only requests for this tenant (#709).
    pub tenant: Option<String>,
    /// Newest first; `0` = backend default.
    pub limit: usize,
}

impl ChangeListFilter {
    pub fn matches(&self, c: &ChangeRequest) -> bool {
        self.status.is_none_or(|s| c.status == s)
            && self.kind.is_none_or(|k| c.kind == k)
            && self.requester.as_deref().is_none_or(|r| c.requester == r)
            && self
                .tenant
                .as_deref()
                .is_none_or(|t| c.tenant.as_deref() == Some(t))
    }
}

/// Records a listing returns when the caller sets no limit.
pub const DEFAULT_LIST_LIMIT: usize = 200;

/// A new request as the caller phrases it.
#[derive(Debug, Clone, Deserialize)]
pub struct NewChange {
    pub kind: ChangeKind,
    pub payload: Value,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub budget: Option<BudgetSpec>,
}

/// Executing a `template_launch` request needs this much of the launch body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaunchPayload {
    pub id: String,
    #[cfg(feature = "templates")]
    #[serde(default)]
    pub version: Option<crate::serve::history::templates::VersionSelector>,
}

fn store_err(e: HistoryError) -> ServeError {
    ServeError::Internal(format!("change store: {e}"))
}

fn sha_hex(canonical: &str) -> String {
    let mut h = Sha256::new();
    h.update(canonical.as_bytes());
    format!("{:x}", h.finalize())
}

/// The material summary of a run's plan rows: what changes the *meaning* of
/// what was approved. `sink_probe` (a live probe), `impact` and `policy`
/// (derived) are deliberately left out — they describe the world, not the
/// proposal.
pub fn material_of_rows(rows: &[Value]) -> String {
    let parts: Vec<Value> = rows
        .iter()
        .map(|r| {
            json!({
                "row": r.get("row"),
                "source": r.get("source"),
                "sink": r.get("sink"),
                "write_mode": r.get("write_mode"),
                "delivery_guarantee": r.get("delivery_guarantee"),
                "transforms": r.get("transforms"),
                "quality": r.get("quality"),
                "contract": r.get("contract"),
                "masking": r.get("masking"),
                "schema_drift": r.get("schema_drift"),
            })
        })
        .collect();
    sha_hex(&serde_json::to_string(&parts).unwrap_or_default())
}

/// Which row-level facts differ between two plans (for the invalidation
/// message). Empty when nothing material moved.
pub fn material_diff(before: &[Value], after: &[Value]) -> Vec<String> {
    let mut out = Vec::new();
    let key = |r: &Value| {
        r.get("row")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string()
    };
    let fields = [
        "source",
        "sink",
        "write_mode",
        "delivery_guarantee",
        "transforms",
        "quality",
        "contract",
        "masking",
        "schema_drift",
    ];
    for b in before {
        let k = key(b);
        match after.iter().find(|a| key(a) == k) {
            None => out.push(format!("row `{k}` is gone")),
            Some(a) => {
                for f in fields {
                    if b.get(f) != a.get(f) {
                        out.push(format!(
                            "row `{k}`: {f} was {} and is now {}",
                            b.get(f).map(Value::to_string).unwrap_or_default(),
                            a.get(f).map(Value::to_string).unwrap_or_default()
                        ));
                    }
                }
            }
        }
    }
    for a in after {
        let k = key(a);
        if !before.iter().any(|b| key(b) == k) {
            out.push(format!("row `{k}` is new"));
        }
    }
    out
}

/// Plan a `run` payload: load the submission the way `POST /v1/runs` would,
/// refuse a policy violation, and produce one plan report per root row.
async fn plan_run(
    state: &ServerState,
    actor: &AuthContext,
    req: &SubmitRequest,
) -> Result<ChangePlan, ServeError> {
    let format: crate::serve::load::ConfigFormat = req.config_format.into();
    let loaded =
        runner::load_for(state, &req.config, format, actor.tenant.as_deref()).await?;
    runner::policy_gate(state, actor, &loaded).await?;
    let auth = loaded
        .auth_catalog()
        .map_err(|e| ServeError::BadConfig(e.to_string()))?;
    let mut rows = Vec::new();
    for node in loaded
        .nodes
        .iter()
        .filter(|n| matches!(n.role, crate::expand::NodeRole::Root))
    {
        #[cfg(feature = "catalog")]
        let history = state.history();
        let report = crate::commands::plan::plan_node(
            &loaded.cfg,
            node,
            &auth,
            crate::commands::plan::PlanOptions {
                sample: None,
                #[cfg(feature = "catalog")]
                impact: Some(crate::commands::plan::ImpactOptions {
                    store: history.as_ref(),
                    pipeline: loaded
                        .cfg
                        .name
                        .clone()
                        .unwrap_or_else(|| "serve".to_string()),
                    depth: crate::impact::DEFAULT_DEPTH,
                }),
                #[cfg(not(feature = "catalog"))]
                _marker: std::marker::PhantomData,
            },
        )
        .await
        .map_err(|e| ServeError::BadConfig(e.to_string()))?;
        rows.push(serde_json::to_value(&report).map_err(|e| ServeError::Internal(e.to_string()))?);
    }
    let sinks: Vec<String> = rows
        .iter()
        .filter_map(|r| r.get("sink").and_then(Value::as_str).map(str::to_string))
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    Ok(ChangePlan {
        material: material_of_rows(&rows),
        summary: json!({
            "pipeline": loaded.cfg.name,
            "rows": rows.len(),
            "sinks": sinks,
            "delivery": loaded.cfg.delivery,
            "budget": loaded.cfg.budget,
        }),
        rows,
    })
}

#[cfg(feature = "templates")]
async fn plan_template_register(
    state: &ServerState,
    body: &crate::serve::handlers::templates::RegisterBody,
) -> Result<ChangePlan, ServeError> {
    let preview = crate::templates::preview_register(
        &state.history(),
        &crate::templates::RegisterRequest {
            id: body.id.clone(),
            body: body.config.clone(),
            format: body.config_format.into(),
            description: body.description.clone(),
            tags: body.tags.clone(),
            launch: body.launch,
            created_by: None,
        },
    )
    .await
    .map_err(|e| ServeError::Unprocessable {
        message: e.to_string(),
        details: None,
    })?;
    let rows: Vec<Value> = preview
        .rows
        .iter()
        .map(|r| serde_json::to_value(r).unwrap_or(Value::Null))
        .collect();
    let body_hash = sha_hex(&body.config);
    Ok(ChangePlan {
        material: sha_hex(&format!(
            "{}\n{}\n{}\n{}\n{}",
            preview.id,
            preview.kind,
            body_hash,
            body.launch,
            material_of_rows(&rows)
        )),
        summary: json!({
            "template": preview.id,
            "template_kind": preview.kind.to_string(),
            "previous_version": preview.previous_version,
            "launch": body.launch,
            "tags": body.tags,
            "rows": rows.len(),
        }),
        rows,
    })
}

#[cfg(feature = "templates")]
async fn plan_template_launch(
    state: &ServerState,
    payload: &LaunchPayload,
) -> Result<ChangePlan, ServeError> {
    use crate::serve::history::templates::VersionSelector;
    let store = state.history();
    let target = payload.version.unwrap_or_else(VersionSelector::newest);
    let version = crate::templates::resolve_version(&store, &payload.id, target)
        .await
        .map_err(template_err)?;
    let st = crate::templates::template_state(&store, &payload.id)
        .await
        .map_err(template_err)?;
    Ok(ChangePlan {
        material: sha_hex(&format!("{}\n{}\n{:?}", payload.id, version, st.stable)),
        summary: json!({
            "template": payload.id,
            "target_version": version,
            "selector": target,
            "stable_before": st.stable,
            "status_before": st.status,
        }),
        rows: Vec::new(),
    })
}

#[cfg(feature = "templates")]
fn template_err(e: crate::error::CliError) -> ServeError {
    use crate::error::CliError;
    match e {
        CliError::UnknownPipelineTemplate { .. } => ServeError::NotFound,
        CliError::Internal(m) => ServeError::Internal(m),
        other => ServeError::Unprocessable {
            message: other.to_string(),
            details: None,
        },
    }
}

/// Validate the payload for its kind and compute the plan.
async fn plan_for(
    state: &ServerState,
    actor: &AuthContext,
    kind: ChangeKind,
    payload: &Value,
) -> Result<ChangePlan, ServeError> {
    match kind {
        ChangeKind::Run => {
            let req: SubmitRequest = serde_json::from_value(payload.clone())
                .map_err(|e| ServeError::BadConfig(format!("run payload: {e}")))?;
            plan_run(state, actor, &req).await
        }
        #[cfg(feature = "templates")]
        ChangeKind::TemplateRegister => {
            let body: crate::serve::handlers::templates::RegisterBody =
                serde_json::from_value(payload.clone()).map_err(|e| {
                    ServeError::BadConfig(format!("template_register payload: {e}"))
                })?;
            plan_template_register(state, &body).await
        }
        #[cfg(feature = "templates")]
        ChangeKind::TemplateLaunch => {
            let body: LaunchPayload = serde_json::from_value(payload.clone())
                .map_err(|e| ServeError::BadConfig(format!("template_launch payload: {e}")))?;
            plan_template_launch(state, &body).await
        }
        #[cfg(not(feature = "templates"))]
        ChangeKind::TemplateRegister | ChangeKind::TemplateLaunch => {
            let _ = (state, actor);
            Err(ServeError::Unprocessable {
                message: "template changes require a server built with the `templates` feature"
                    .into(),
                details: None,
            })
        }
    }
}

fn record_metric(kind: ChangeKind, outcome: &'static str) {
    metrics::counter!(
        "faucet_serve_changes_total",
        "kind" => kind.as_str(),
        "outcome" => outcome
    )
    .increment(1);
}

/// Refresh the pending-requests gauge from the store (best-effort).
pub async fn refresh_pending_gauge(state: &ServerState) {
    if let Ok(pending) = state
        .history()
        .change_list(&ChangeListFilter {
            status: Some(ChangeStatus::Pending),
            limit: 10_000,
            ..Default::default()
        })
        .await
    {
        metrics::gauge!("faucet_serve_changes_pending").set(pending.len() as f64);
    }
}

async fn save(state: &ServerState, c: &ChangeRequest) -> Result<(), ServeError> {
    state.history().change_upsert(c).await.map_err(store_err)
}

/// Create a request: validate + plan the payload, work out the quorum, store
/// it, audit `change.requested`, and (for a run whose config declares
/// `notifications:`) ping the approvers with a `change_requested` event.
pub async fn create(
    state: &ServerState,
    actor: &AuthContext,
    new: NewChange,
) -> Result<ChangeRequest, ServeError> {
    if let Some(b) = &new.budget {
        b.validate().map_err(ServeError::BadConfig)?;
    }
    let mut payload = new.payload;
    if new.kind == ChangeKind::Run {
        // The stored payload is the run as it will be submitted: the gate
        // flag has done its job, and the reason lives on the request itself.
        if let Some(obj) = payload.as_object_mut() {
            obj.remove("require_approval");
            obj.remove("reason");
        }
    }
    let plan = plan_for(state, actor, new.kind, &payload).await?;
    let policy = state.approvals();
    let rule = policy.effective(new.kind);
    let now = Utc::now();
    let change = ChangeRequest {
        id: uuid::Uuid::now_v7().to_string(),
        kind: new.kind,
        status: ChangeStatus::Pending,
        requester: actor.principal.clone(),
        requester_role: actor.role,
        reason: new.reason.filter(|r| !r.trim().is_empty()),
        payload,
        plan: Some(plan),
        budget: new.budget,
        required_approvals: rule.min_approvers,
        approvals: Vec::new(),
        rejection: None,
        created_at: now,
        updated_at: now,
        expires_at: now + chrono::Duration::seconds(state.approval_expiry().as_secs() as i64),
        run_id: None,
        template: None,
        error: None,
        tenant: actor.tenant.clone(),
    };
    save(state, &change).await?;
    record_metric(change.kind, "requested");
    crate::serve::audit::write(
        state,
        actor,
        "change.requested",
        None,
        Some(change.id.clone()),
        "ok",
    )
    .await;
    refresh_pending_gauge(state).await;
    #[cfg(feature = "notify")]
    notify_requested(&change).await;
    Ok(change)
}

/// Ping the approvers through the proposed config's own `notifications:`
/// block (a run request only — that is the config that names the channels).
/// Best-effort: a malformed block is logged, never an error.
#[cfg(feature = "notify")]
async fn notify_requested(change: &ChangeRequest) {
    if change.kind != ChangeKind::Run {
        return;
    }
    let Some(config) = change.payload.get("config").and_then(Value::as_str) else {
        return;
    };
    let doc: Value = match serde_yaml::from_str(config) {
        Ok(v) => v,
        Err(_) => return,
    };
    let specs: Vec<crate::notify::NotificationSpec> = doc
        .get("notifications")
        .cloned()
        .and_then(|v| serde_json::from_value(v).ok())
        .unwrap_or_default();
    let pipeline = doc
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("serve")
        .to_string();
    match crate::notify::Notifier::from_specs(&specs) {
        Ok(Some(n)) => {
            n.emit(crate::notify::NotifyEvent::change_requested(
                pipeline,
                &change.id,
                change.kind.as_str(),
                &change.requester,
                change.reason.as_deref().unwrap_or(""),
            ))
            .await;
        }
        Ok(None) => {}
        Err(e) => tracing::warn!(change = %change.id, "change_requested notification skipped: {e}"),
    }
}

async fn load(state: &ServerState, id: &str) -> Result<ChangeRequest, ServeError> {
    state
        .history()
        .change_get(id)
        .await
        .map_err(store_err)?
        .ok_or(ServeError::NotFound)
}

/// Mark a pending request expired when its window has passed. Returns the
/// (possibly updated) record.
async fn expire_if_due(
    state: &ServerState,
    mut change: ChangeRequest,
) -> Result<ChangeRequest, ServeError> {
    if change.status == ChangeStatus::Pending && change.expires_at <= Utc::now() {
        change.status = ChangeStatus::Expired;
        change.touch();
        save(state, &change).await?;
        record_metric(change.kind, "expired");
        crate::serve::audit::write(
            state,
            &AuthContext::system("expiry"),
            "change.expired",
            None,
            Some(change.id.clone()),
            "ok",
        )
        .await;
    }
    Ok(change)
}

/// One request, expiry applied.
pub async fn get(state: &ServerState, id: &str) -> Result<ChangeRequest, ServeError> {
    let change = load(state, id).await?;
    expire_if_due(state, change).await
}

/// Approve. Below quorum the request stays `pending` with the approval
/// recorded; at quorum it is re-planned and executed (or invalidated).
pub async fn approve(
    state: &ServerState,
    actor: &AuthContext,
    id: &str,
    comment: Option<String>,
) -> Result<ChangeRequest, ServeError> {
    let mut change = get(state, id).await?;
    if change.status != ChangeStatus::Pending {
        return Err(ServeError::Conflict(format!(
            "change {id} is {}, not pending",
            change.status.as_str()
        )));
    }
    state
        .approvals()
        .may_approve(change.kind, actor, &change.requester)
        .map_err(|reason| {
            record_metric(change.kind, "denied");
            ServeError::Forbidden(reason)
        })?;
    if change
        .approvals
        .iter()
        .any(|a| a.principal == actor.principal)
    {
        return Err(ServeError::Conflict(format!(
            "'{}' already approved change {id}",
            actor.principal
        )));
    }
    change.approvals.push(Approval {
        principal: actor.principal.clone(),
        role: actor.role,
        at: Utc::now(),
        comment: comment.filter(|c| !c.trim().is_empty()),
    });
    change.touch();
    record_metric(change.kind, "approved");
    crate::serve::audit::write(
        state,
        actor,
        "change.approved",
        None,
        Some(change.id.clone()),
        "ok",
    )
    .await;
    if (change.approvals.len() as u32) < change.required_approvals {
        save(state, &change).await?;
        return Ok(change);
    }
    change.status = ChangeStatus::Approved;
    save(state, &change).await?;
    let change = execute(state, actor, change).await?;
    refresh_pending_gauge(state).await;
    Ok(change)
}

/// Reject a pending request. The requester may withdraw their own.
pub async fn reject(
    state: &ServerState,
    actor: &AuthContext,
    id: &str,
    reason: String,
) -> Result<ChangeRequest, ServeError> {
    let mut change = get(state, id).await?;
    if change.status != ChangeStatus::Pending {
        return Err(ServeError::Conflict(format!(
            "change {id} is {}, not pending",
            change.status.as_str()
        )));
    }
    if actor.principal != change.requester {
        state
            .approvals()
            .may_approve(change.kind, actor, "")
            .map_err(ServeError::Forbidden)?;
    }
    change.status = ChangeStatus::Rejected;
    change.rejection = Some(Rejection {
        principal: actor.principal.clone(),
        at: Utc::now(),
        reason,
    });
    change.touch();
    save(state, &change).await?;
    record_metric(change.kind, "rejected");
    crate::serve::audit::write(
        state,
        actor,
        "change.rejected",
        None,
        Some(change.id.clone()),
        "ok",
    )
    .await;
    refresh_pending_gauge(state).await;
    Ok(change)
}

/// Re-plan, compare, and carry out an approved request. `actor` is the
/// approver who completed the quorum (for audit); the run itself is submitted
/// as the requester.
async fn execute(
    state: &ServerState,
    actor: &AuthContext,
    mut change: ChangeRequest,
) -> Result<ChangeRequest, ServeError> {
    let requester = AuthContext {
        principal: change.requester.clone(),
        role: change.requester_role,
        source_ip: None,
        tenant: change.tenant.clone(),
    };
    // Re-plan against the world as it is now. A planning failure (the
    // template is gone, the config no longer loads) fails the request.
    let fresh = match plan_for(state, &requester, change.kind, &change.payload).await {
        Ok(p) => p,
        Err(e) => return finish_failed(state, actor, change, e.to_string()).await,
    };
    if let Some(before) = &change.plan
        && before.material != fresh.material
    {
        let diff = material_diff(&before.rows, &fresh.rows);
        let why = if diff.is_empty() {
            format!(
                "the plan changed since approval ({} → {})",
                serde_json::to_string(&before.summary).unwrap_or_default(),
                serde_json::to_string(&fresh.summary).unwrap_or_default()
            )
        } else {
            format!("the plan changed since approval: {}", diff.join("; "))
        };
        change.status = ChangeStatus::Invalidated;
        change.error = Some(why);
        change.plan = Some(fresh);
        change.touch();
        save(state, &change).await?;
        record_metric(change.kind, "invalidated");
        crate::serve::audit::write(
            state,
            actor,
            "change.invalidated",
            None,
            Some(change.id.clone()),
            "denied",
        )
        .await;
        return Ok(change);
    }
    let outcome: Result<(Option<String>, Option<TemplateOutcome>), ServeError> = match change.kind {
        ChangeKind::Run => {
            let mut req: SubmitRequest = match serde_json::from_value(change.payload.clone()) {
                Ok(r) => r,
                Err(e) => return finish_failed(state, actor, change, e.to_string()).await,
            };
            req.require_approval = false;
            req.reason = None;
            req.approved_change = Some(change.id.clone());
            req.budget = match (req.budget.take(), change.budget.clone()) {
                (Some(a), Some(b)) => Some(a.merge(&b)),
                (a, b) => a.or(b),
            };
            req.labels.insert("change".to_string(), change.id.clone());
            runner::submit(state.clone(), req, requester.clone())
                .await
                .map(|r| (Some(r.run_id), None))
        }
        #[cfg(feature = "templates")]
        ChangeKind::TemplateRegister => {
            let body: crate::serve::handlers::templates::RegisterBody =
                match serde_json::from_value(change.payload.clone()) {
                    Ok(b) => b,
                    Err(e) => return finish_failed(state, actor, change, e.to_string()).await,
                };
            crate::templates::register(
                &state.history(),
                crate::templates::RegisterRequest {
                    id: body.id,
                    body: body.config,
                    format: body.config_format.into(),
                    description: body.description,
                    tags: body.tags,
                    launch: body.launch,
                    created_by: Some(change.requester.clone()),
                },
            )
            .await
            .map(|rec| {
                (
                    None,
                    Some(TemplateOutcome {
                        id: rec.id,
                        version: rec.version,
                    }),
                )
            })
            .map_err(template_err)
        }
        #[cfg(feature = "templates")]
        ChangeKind::TemplateLaunch => {
            let body: LaunchPayload = match serde_json::from_value(change.payload.clone()) {
                Ok(b) => b,
                Err(e) => return finish_failed(state, actor, change, e.to_string()).await,
            };
            let target = body
                .version
                .unwrap_or_else(crate::serve::history::templates::VersionSelector::newest);
            crate::templates::launch(&state.history(), &body.id, target, Some(&change.requester))
                .await
                .map(|o| {
                    (
                        None,
                        Some(TemplateOutcome {
                            id: body.id.clone(),
                            version: o.version,
                        }),
                    )
                })
                .map_err(template_err)
        }
        #[cfg(not(feature = "templates"))]
        ChangeKind::TemplateRegister | ChangeKind::TemplateLaunch => {
            Err(ServeError::Unprocessable {
                message: "template changes require the `templates` feature".into(),
                details: None,
            })
        }
    };
    match outcome {
        Ok((run_id, template)) => {
            change.status = ChangeStatus::Executed;
            change.run_id = run_id.clone();
            change.template = template;
            change.plan = Some(fresh);
            change.touch();
            save(state, &change).await?;
            record_metric(change.kind, "executed");
            crate::serve::audit::write(
                state,
                actor,
                "change.executed",
                run_id,
                Some(change.id.clone()),
                "ok",
            )
            .await;
            Ok(change)
        }
        Err(e) => finish_failed(state, actor, change, e.to_string()).await,
    }
}

async fn finish_failed(
    state: &ServerState,
    actor: &AuthContext,
    mut change: ChangeRequest,
    error: String,
) -> Result<ChangeRequest, ServeError> {
    change.status = ChangeStatus::Failed;
    change.error = Some(crate::secrets::registry::redact(&error).into_owned());
    change.touch();
    save(state, &change).await?;
    record_metric(change.kind, "failed");
    crate::serve::audit::write(
        state,
        actor,
        "change.failed",
        None,
        Some(change.id.clone()),
        "error",
    )
    .await;
    Ok(change)
}

/// List requests (newest first), plan rows stripped.
pub async fn list(
    state: &ServerState,
    filter: &ChangeListFilter,
) -> Result<Vec<ChangeRequest>, ServeError> {
    let rows = state
        .history()
        .change_list(filter)
        .await
        .map_err(store_err)?;
    let mut out = Vec::with_capacity(rows.len());
    for c in rows {
        out.push(expire_if_due(state, c).await?.without_rows());
    }
    Ok(out)
}

/// Expire every pending request past its window. Returns how many lapsed.
pub async fn expire_due(state: &ServerState) -> usize {
    let pending = match state
        .history()
        .change_list(&ChangeListFilter {
            status: Some(ChangeStatus::Pending),
            limit: 10_000,
            ..Default::default()
        })
        .await
    {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!("change expiry sweep: {e}");
            return 0;
        }
    };
    let mut n = 0;
    for c in pending {
        match expire_if_due(state, c).await {
            Ok(c) if c.status == ChangeStatus::Expired => n += 1,
            Ok(_) => {}
            Err(e) => tracing::warn!("change expiry sweep: {e}"),
        }
    }
    if n > 0 {
        refresh_pending_gauge(state).await;
    }
    n
}

/// The background sweep: expire due requests every `period` until shutdown.
pub async fn expiry_loop(
    state: ServerState,
    period: std::time::Duration,
    shutdown: tokio_util::sync::CancellationToken,
) {
    let mut tick = tokio::time::interval(period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tick.tick() => {
                let n = expire_due(&state).await;
                if n > 0 {
                    tracing::info!(expired = n, "change requests lapsed");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kinds_and_statuses_round_trip() {
        for k in ChangeKind::ALL {
            assert_eq!(k.as_str().parse::<ChangeKind>().unwrap(), k);
            assert_eq!(
                serde_json::from_value::<ChangeKind>(json!(k.as_str())).unwrap(),
                k
            );
        }
        assert_eq!(
            "template-launch".parse::<ChangeKind>().unwrap(),
            ChangeKind::TemplateLaunch
        );
        assert!("nope".parse::<ChangeKind>().unwrap_err().contains("nope"));
        assert_eq!(ChangeKind::Run.to_string(), "run");
        for s in [
            ChangeStatus::Pending,
            ChangeStatus::Approved,
            ChangeStatus::Rejected,
            ChangeStatus::Executed,
            ChangeStatus::Expired,
            ChangeStatus::Invalidated,
            ChangeStatus::Failed,
        ] {
            assert_eq!(s.as_str().parse::<ChangeStatus>().unwrap(), s);
        }
        assert!(!ChangeStatus::Pending.is_terminal());
        assert!(!ChangeStatus::Approved.is_terminal());
        assert!(ChangeStatus::Executed.is_terminal());
        assert!("later".parse::<ChangeStatus>().is_err());
    }

    #[test]
    fn material_ignores_probes_and_impact_but_not_sinks() {
        let a = vec![
            json!({"row": "r", "source": "csv", "sink": "jsonl", "write_mode": "append",
            "delivery_guarantee": "at-least-once", "transforms": ["flatten"], "sink_probe": "ok"}),
        ];
        let b = vec![
            json!({"row": "r", "source": "csv", "sink": "jsonl", "write_mode": "append",
            "delivery_guarantee": "at-least-once", "transforms": ["flatten"], "sink_probe": "failed",
            "impact": {"severity": "breaking"}}),
        ];
        assert_eq!(material_of_rows(&a), material_of_rows(&b));
        assert!(material_diff(&a, &b).is_empty());
        let c = vec![
            json!({"row": "r", "source": "csv", "sink": "postgres", "write_mode": "upsert",
            "delivery_guarantee": "at-least-once", "transforms": ["flatten"]}),
        ];
        assert_ne!(material_of_rows(&a), material_of_rows(&c));
        let diff = material_diff(&a, &c);
        assert_eq!(diff.len(), 2, "{diff:?}");
        assert!(
            diff[0].contains("sink was \"jsonl\" and is now \"postgres\""),
            "{diff:?}"
        );
        let d = vec![c[0].clone(), json!({"row": "extra", "sink": "x"})];
        let diff = material_diff(&a, &d);
        assert!(
            diff.iter().any(|l| l.contains("`extra` is new")),
            "{diff:?}"
        );
        let diff = material_diff(&d, &a);
        assert!(
            diff.iter().any(|l| l.contains("`extra` is gone")),
            "{diff:?}"
        );
    }

    #[test]
    fn filter_and_listing_shape() {
        let now = Utc::now();
        let c = ChangeRequest {
            id: "c1".into(),
            kind: ChangeKind::Run,
            status: ChangeStatus::Pending,
            requester: "bob".into(),
            requester_role: Role::Operator,
            reason: None,
            payload: json!({}),
            plan: Some(ChangePlan {
                material: "m".into(),
                rows: vec![json!({"row": "r"})],
                summary: json!({}),
            }),
            budget: None,
            required_approvals: 1,
            approvals: Vec::new(),
            rejection: None,
            created_at: now,
            updated_at: now,
            expires_at: now,
            run_id: None,
            template: None,
            error: None,
        };
        assert!(ChangeListFilter::default().matches(&c));
        assert!(
            ChangeListFilter {
                status: Some(ChangeStatus::Pending),
                kind: Some(ChangeKind::Run),
                requester: Some("bob".into()),
                limit: 0
            }
            .matches(&c)
        );
        assert!(
            !ChangeListFilter {
                status: Some(ChangeStatus::Executed),
                ..Default::default()
            }
            .matches(&c)
        );
        assert!(
            !ChangeListFilter {
                requester: Some("alice".into()),
                ..Default::default()
            }
            .matches(&c)
        );
        let stripped = c.clone().without_rows();
        assert!(stripped.plan.as_ref().unwrap().rows.is_empty());
        let json = serde_json::to_value(&c).unwrap();
        assert_eq!(json["kind"], "run");
        assert_eq!(json["requester_role"], "operator");
        let back: ChangeRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back, c);
    }

    fn csv_yaml(dir: &std::path::Path, name: &str, notify: &str) -> String {
        let input = dir.join("in.csv");
        std::fs::write(&input, "id,name\n1,a\n").unwrap();
        format!(
            "version: 1\nname: {name}\npipeline:\n  source:\n    type: csv\n    config:\n      path: {}\n  sink:\n    type: jsonl\n    config:\n      path: {}\n{notify}",
            input.display(),
            dir.join("out.jsonl").display()
        )
    }

    fn actor(name: &str) -> AuthContext {
        AuthContext {
            principal: name.into(),
            role: Role::Admin,
            source_ip: None,
        }
    }

    fn stored(kind: ChangeKind, payload: Value, material: &str, quorum: u32) -> ChangeRequest {
        let now = Utc::now();
        ChangeRequest {
            id: uuid::Uuid::now_v7().to_string(),
            kind,
            status: ChangeStatus::Pending,
            requester: "bob".into(),
            requester_role: Role::Operator,
            reason: None,
            payload,
            plan: Some(ChangePlan {
                material: material.into(),
                rows: vec![json!({"row": "r", "sink": "old"})],
                summary: json!({"v": 1}),
            }),
            budget: None,
            required_approvals: quorum,
            approvals: Vec::new(),
            rejection: None,
            created_at: now,
            updated_at: now,
            expires_at: now + chrono::Duration::hours(1),
            run_id: None,
            template: None,
            error: None,
        }
    }

    #[tokio::test]
    async fn quorum_duplicates_rejection_and_expiry() {
        let state = crate::serve::test_support::test_state();
        let c = stored(ChangeKind::Run, json!({"config": "x"}), "m", 2);
        save(&state, &c).await.unwrap();

        let once = approve(&state, &actor("alice"), &c.id, Some("  ".into()))
            .await
            .unwrap();
        assert_eq!(once.status, ChangeStatus::Pending);
        assert_eq!(once.approvals.len(), 1);
        assert!(once.approvals[0].comment.is_none());
        let dup = approve(&state, &actor("alice"), &c.id, None)
            .await
            .unwrap_err();
        assert!(matches!(dup, ServeError::Conflict(m) if m.contains("already approved")));

        let rejected = reject(&state, &actor("bob"), &c.id, "withdrawn".into())
            .await
            .unwrap();
        assert_eq!(rejected.status, ChangeStatus::Rejected);
        let again = reject(&state, &actor("bob"), &c.id, "x".into())
            .await
            .unwrap_err();
        assert!(matches!(again, ServeError::Conflict(m) if m.contains("not pending")));
        let late = approve(&state, &actor("carol"), &c.id, None)
            .await
            .unwrap_err();
        assert!(matches!(late, ServeError::Conflict(_)));

        let mut old = stored(ChangeKind::Run, json!({}), "m", 1);
        old.expires_at = Utc::now() - chrono::Duration::seconds(5);
        save(&state, &old).await.unwrap();
        let fresh = stored(ChangeKind::Run, json!({}), "m", 1);
        save(&state, &fresh).await.unwrap();
        assert_eq!(expire_due(&state).await, 1);
        assert_eq!(
            get(&state, &old.id).await.unwrap().status,
            ChangeStatus::Expired
        );
        assert_eq!(expire_due(&state).await, 0);
        assert!(matches!(
            get(&state, "nope").await,
            Err(ServeError::NotFound)
        ));

        let listed = list(&state, &ChangeListFilter::default()).await.unwrap();
        assert_eq!(listed.len(), 3);
        assert!(
            listed
                .iter()
                .all(|c| c.plan.as_ref().unwrap().rows.is_empty())
        );

        let shutdown = tokio_util::sync::CancellationToken::new();
        let task = tokio::spawn(expiry_loop(
            state.clone(),
            std::time::Duration::from_millis(10),
            shutdown.clone(),
        ));
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        shutdown.cancel();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn bad_payloads_fail_planning_and_execution() {
        let state = crate::serve::test_support::test_state();
        let err = create(
            &state,
            &actor("bob"),
            NewChange {
                kind: ChangeKind::Run,
                payload: json!({"nope": 1}),
                reason: None,
                budget: None,
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ServeError::BadConfig(m) if m.contains("run payload")));
        let err = create(
            &state,
            &actor("bob"),
            NewChange {
                kind: ChangeKind::Run,
                payload: json!({"config": "x"}),
                reason: None,
                budget: Some(BudgetSpec {
                    max_records: Some(0),
                    ..Default::default()
                }),
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ServeError::BadConfig(_)));

        // A stored request whose payload no longer plans fails on approval.
        let c = stored(ChangeKind::Run, json!({"nope": 1}), "m", 1);
        save(&state, &c).await.unwrap();
        let done = approve(&state, &actor("alice"), &c.id, None).await.unwrap();
        assert_eq!(done.status, ChangeStatus::Failed);
        assert!(done.error.unwrap().contains("run payload"));
    }

    #[tokio::test]
    async fn a_changed_plan_invalidates_and_an_unchanged_one_executes() {
        let dir = tempfile::tempdir().unwrap();
        let state = crate::serve::test_support::test_state();
        let notify = "notifications:\n  - name: log\n    on: [change_requested]\n    channel:\n      type: webhook\n      config:\n        url: http://127.0.0.1:9/hook\n";
        let created = create(
            &state,
            &actor("bob"),
            NewChange {
                kind: ChangeKind::Run,
                payload: json!({
                    "config": csv_yaml(dir.path(), "chg", notify),
                    "require_approval": true,
                    "reason": "gone",
                    "budget": {"max_records": 100}
                }),
                reason: Some("nightly".into()),
                budget: Some(BudgetSpec {
                    max_records: Some(50),
                    ..Default::default()
                }),
            },
        )
        .await
        .unwrap();
        assert!(created.payload.get("require_approval").is_none());
        let executed = approve(&state, &actor("alice"), &created.id, None)
            .await
            .unwrap();
        assert_eq!(
            executed.status,
            ChangeStatus::Executed,
            "{:?}",
            executed.error
        );
        assert!(executed.run_id.is_some());

        let payload = json!({"config": csv_yaml(dir.path(), "chg2", "")});
        let mut c = stored(ChangeKind::Run, payload.clone(), "stale", 1);
        save(&state, &c).await.unwrap();
        let inv = approve(&state, &actor("alice"), &c.id, None).await.unwrap();
        assert_eq!(inv.status, ChangeStatus::Invalidated);
        assert!(inv.error.unwrap().contains("plan changed"));

        // Same material, no row diff: the summary explains the change.
        c = stored(ChangeKind::Run, payload, "stale", 1);
        c.plan.as_mut().unwrap().rows.clear();
        save(&state, &c).await.unwrap();
        let fresh_rows = plan_for(&state, &actor("bob"), ChangeKind::Run, &c.payload)
            .await
            .unwrap()
            .rows;
        c.plan.as_mut().unwrap().rows = fresh_rows;
        save(&state, &c).await.unwrap();
        let inv = approve(&state, &actor("alice"), &c.id, None).await.unwrap();
        assert_eq!(inv.status, ChangeStatus::Invalidated);
        assert!(inv.error.unwrap().contains("→"));
    }

    #[cfg(feature = "notify")]
    #[tokio::test]
    async fn requested_notifications_skip_what_they_cannot_read() {
        let mut c = stored(ChangeKind::TemplateLaunch, json!({"config": "x"}), "m", 1);
        notify_requested(&c).await;
        c.kind = ChangeKind::Run;
        c.payload = json!({});
        notify_requested(&c).await;
        c.payload = json!({"config": "{ nope"});
        notify_requested(&c).await;
        c.payload = json!({"config": "notifications:\n  - name: x\n    on: [change_requested]\n    channel:\n      type: webhook\n      config:\n        url: \"\"\n"});
        notify_requested(&c).await;
        c.payload = json!({"config": "name: p\n"});
        notify_requested(&c).await;
    }

    #[test]
    fn store_errors_are_internal() {
        let e = store_err(HistoryError::Backend("down".into()));
        assert!(matches!(e, ServeError::Internal(m) if m.contains("change store")));
    }

    #[cfg(feature = "templates")]
    #[tokio::test]
    async fn template_changes_plan_execute_and_fail() {
        let dir = tempfile::tempdir().unwrap();
        let state = crate::serve::test_support::test_state();
        let new = |kind, payload| NewChange {
            kind,
            payload,
            reason: None,
            budget: None,
        };
        for (kind, needle) in [
            (ChangeKind::TemplateRegister, "template_register payload"),
            (ChangeKind::TemplateLaunch, "template_launch payload"),
        ] {
            let err = create(&state, &actor("bob"), new(kind, json!({"x": 1})))
                .await
                .unwrap_err();
            assert!(matches!(err, ServeError::BadConfig(m) if m.contains(needle)));
        }
        let err = create(
            &state,
            &actor("bob"),
            new(ChangeKind::TemplateLaunch, json!({"id": "ghost"})),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ServeError::NotFound), "{err:?}");
        let err = create(
            &state,
            &actor("bob"),
            new(ChangeKind::TemplateRegister, json!({"config": "{ nope"})),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ServeError::Unprocessable { .. }), "{err:?}");

        let reg = create(
            &state,
            &actor("bob"),
            new(
                ChangeKind::TemplateRegister,
                json!({"id": "tpl", "config": csv_yaml(dir.path(), "tpl", "")}),
            ),
        )
        .await
        .unwrap();
        let reg = approve(&state, &actor("alice"), &reg.id, None)
            .await
            .unwrap();
        assert_eq!(reg.status, ChangeStatus::Executed, "{:?}", reg.error);
        assert_eq!(reg.template.as_ref().unwrap().version, 1);

        let launch = create(
            &state,
            &actor("bob"),
            new(ChangeKind::TemplateLaunch, json!({"id": "tpl"})),
        )
        .await
        .unwrap();
        let launch = approve(&state, &actor("alice"), &launch.id, None)
            .await
            .unwrap();
        assert_eq!(launch.status, ChangeStatus::Executed, "{:?}", launch.error);

        for kind in [ChangeKind::TemplateRegister, ChangeKind::TemplateLaunch] {
            let c = stored(kind, json!({"x": 1}), "m", 1);
            save(&state, &c).await.unwrap();
            let done = approve(&state, &actor("alice"), &c.id, None).await.unwrap();
            assert_eq!(done.status, ChangeStatus::Failed);
        }
        assert!(matches!(
            template_err(crate::error::CliError::Internal("x".into())),
            ServeError::Internal(_)
        ));
    }
}
