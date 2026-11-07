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
#[cfg(feature = "policy")]
pub mod policy_update;

pub use policy::{ApprovalPolicy, ApprovalRule, EffectiveRule};

use crate::serve::error::ServeError;
use crate::serve::history::HistoryError;
use crate::serve::rbac::{AuthContext, Role};
use crate::serve::runner::SubmitRequest;
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
    /// Delete a template or one of its versions
    /// (`DELETE /v1/templates/{id}[?version=]`).
    TemplateDelete,
    /// Retire or revive one template version
    /// (`POST /v1/templates/{id}/versions/{version}/deprecate`).
    TemplateDeprecate,
    /// Create, edit or delete a schedule (#1044): payload
    /// `{action: create|update|delete, name, spec?}`.
    Schedule,
    /// Put a new version of the server-managed data-flow policy in force
    /// (`POST /v1/policy`, `POST /v1/policy/rollback`, #1043).
    PolicyUpdate,
    /// Put a new version of one tenant's add-only policy in force
    /// (`POST /v1/tenants/{tenant}/policy[/rollback]`, #1043).
    TenantPolicyUpdate,
}

impl ChangeKind {
    pub const ALL: [ChangeKind; 8] = [
        ChangeKind::Run,
        ChangeKind::TemplateRegister,
        ChangeKind::TemplateLaunch,
        ChangeKind::TemplateDelete,
        ChangeKind::TemplateDeprecate,
        ChangeKind::Schedule,
        ChangeKind::PolicyUpdate,
        ChangeKind::TenantPolicyUpdate,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            ChangeKind::Run => "run",
            ChangeKind::TemplateRegister => "template_register",
            ChangeKind::TemplateLaunch => "template_launch",
            ChangeKind::TemplateDelete => "template_delete",
            ChangeKind::TemplateDeprecate => "template_deprecate",
            ChangeKind::Schedule => "schedule",
            ChangeKind::PolicyUpdate => "policy_update",
            ChangeKind::TenantPolicyUpdate => "tenant_policy_update",
        }
    }

    /// A data-flow policy change (#1043): proposed only through the policy
    /// routes, never through `POST /v1/changes`.
    pub fn is_policy(self) -> bool {
        matches!(
            self,
            ChangeKind::PolicyUpdate | ChangeKind::TenantPolicyUpdate
        )
    }

    /// The kind whose `--require-approval` and approval rule also govern this
    /// one: deleting or retiring a version moves what callers resolve to, as a
    /// launch does.
    pub fn governed_by(self) -> Option<ChangeKind> {
        matches!(
            self,
            ChangeKind::TemplateDelete | ChangeKind::TemplateDeprecate
        )
        .then_some(ChangeKind::TemplateLaunch)
    }
}

impl FromStr for ChangeKind {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s.trim() {
            "run" => Ok(ChangeKind::Run),
            "template_register" | "template-register" => Ok(ChangeKind::TemplateRegister),
            "template_launch" | "template-launch" => Ok(ChangeKind::TemplateLaunch),
            "template_delete" | "template-delete" => Ok(ChangeKind::TemplateDelete),
            "template_deprecate" | "template-deprecate" => Ok(ChangeKind::TemplateDeprecate),
            "schedule" => Ok(ChangeKind::Schedule),
            "policy_update" | "policy-update" => Ok(ChangeKind::PolicyUpdate),
            "tenant_policy_update" | "tenant-policy-update" => Ok(ChangeKind::TenantPolicyUpdate),
            other => Err(format!(
                "unknown change kind `{other}` (expected run, template_register, \
                 template_launch, template_delete, template_deprecate, schedule, policy_update \
                 or tenant_policy_update)"
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
    /// The proposed run's config was written by the server's operator (a
    /// registered template), not by the requester. Set by the server only.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub trusted_config: bool,
    /// Where that trusted config holds values the requester supplied for the
    /// template's params (#989): planned and executed under the caller
    /// reference policy. Set by the server only.
    #[serde(default, skip_serializing_if = "crate::params::CallerFields::is_empty")]
    pub caller_params: crate::params::CallerFields,
    /// Approved, and its execution was refused for a reason that passes on
    /// its own (#1050): the reconcile sweep runs it again. Set by the server
    /// only.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub deferred: bool,
}

impl ChangeRequest {
    /// The record without the plan rows — what a listing carries.
    pub fn without_rows(mut self) -> Self {
        if let Some(p) = &mut self.plan {
            p.rows = Vec::new();
        }
        self
    }

    /// The record as `actor` may read it: a callback's headers are masked for
    /// everyone but an admin, the proposed config for anyone who cannot
    /// approve a change, and a proposed template body for anyone but an
    /// unscoped admin (the rule `GET /v1/templates/{id}` applies to the
    /// registered body).
    pub fn redacted_for(mut self, actor: &AuthContext) -> Self {
        // Sealed schedule params never leave the server, not even as
        // ciphertext (#1051).
        if self.kind == ChangeKind::Schedule
            && let Some(params) = self
                .payload
                .pointer_mut("/spec/template/params")
                .and_then(Value::as_object_mut)
        {
            for v in params.values_mut() {
                if v.as_object()
                    .is_some_and(|m| m.len() == 1 && m.get("$sealed").is_some_and(Value::is_string))
                {
                    *v = serde_json::json!({ "$sealed": true });
                }
            }
        }
        let template_body = self.kind == ChangeKind::TemplateRegister
            || (self.kind == ChangeKind::Run && self.trusted_config);
        if template_body
            && !actor.reads_template_bodies()
            && let Some(config) = self.payload.get_mut("config")
        {
            *config = Value::String(REDACTED.to_string());
        }
        if actor.role == Role::Admin {
            return self;
        }
        if let Some(url) = self.payload.pointer_mut("/callback/url")
            && let Some(text) = url.as_str()
        {
            *url = Value::String(faucet_core::redact_uri_credentials(text));
        }
        if let Some(headers) = self
            .payload
            .pointer_mut("/callback/headers")
            .and_then(Value::as_object_mut)
        {
            for v in headers.values_mut() {
                *v = Value::String(REDACTED.to_string());
            }
        }
        if !actor
            .role
            .grants(crate::serve::rbac::Permission::ChangeApprove)
            && let Some(config) = self.payload.get_mut("config")
        {
            *config = Value::String(REDACTED.to_string());
        }
        self
    }

    fn touch(&mut self) {
        self.updated_at = Utc::now();
    }

    /// A pending request of `kind` carrying `payload`, for tests elsewhere.
    #[cfg(test)]
    pub(crate) fn for_test(kind: ChangeKind, payload: Value) -> Self {
        let now = Utc::now();
        Self {
            id: uuid::Uuid::now_v7().to_string(),
            kind,
            status: ChangeStatus::Pending,
            requester: "bob".into(),
            requester_role: Role::Admin,
            reason: Some("test".into()),
            payload,
            plan: None,
            budget: None,
            required_approvals: 1,
            approvals: Vec::new(),
            rejection: None,
            created_at: now,
            updated_at: now,
            expires_at: now + chrono::Duration::hours(1),
            run_id: None,
            template: None,
            error: None,
            tenant: None,
            trusted_config: false,
            caller_params: Default::default(),
            deferred: false,
        }
    }
}

/// What a masked value reads as.
pub const REDACTED: &str = "***";

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
    /// Set in-process for a run proposed from a registered template; never
    /// accepted from a client.
    #[serde(skip)]
    pub trusted_config: bool,
    /// Set in-process with `trusted_config`: where the config holds values
    /// the requester supplied for the template's params (#989).
    #[serde(skip)]
    pub caller_params: crate::params::CallerFields,
}

/// Executing a `template_launch` request needs this much of the launch body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaunchPayload {
    pub id: String,
    #[cfg(feature = "templates")]
    #[serde(default)]
    pub version: Option<crate::serve::history::templates::VersionSelector>,
}

/// Executing a `template_delete` request needs this: the template, and the
/// version to remove (every version when absent).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeletePayload {
    pub id: String,
    #[cfg(feature = "templates")]
    #[serde(default)]
    pub version: Option<crate::serve::history::templates::VersionSelector>,
}

/// Executing a `template_deprecate` request needs this: the version to
/// retire (or, with `undo`, revive) and why.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeprecatePayload {
    pub id: String,
    #[cfg(feature = "templates")]
    pub version: crate::serve::history::templates::VersionSelector,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub undo: bool,
}

fn store_err(e: HistoryError) -> ServeError {
    ServeError::from(e)
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

/// The material fingerprint of a run: its rows, plus the selection that
/// chose them (#741) — a different subset is a different change even when the
/// rows it keeps are unchanged.
pub fn run_material(rows: &[Value], selection: Option<&str>) -> String {
    match selection {
        None => material_of_rows(rows),
        Some(sel) => sha_hex(&format!("{}\nselection={sel}", material_of_rows(rows))),
    }
}

/// [`run_material`] with the completion dependencies the request added
/// (#960, canonical form): a different order is a different change.
pub fn run_material_ordered(
    rows: &[Value],
    selection: Option<&str>,
    depends_on: Option<&str>,
) -> String {
    match depends_on {
        None => run_material(rows, selection),
        Some(d) => sha_hex(&format!(
            "{}\ndepends_on={d}",
            run_material(rows, selection)
        )),
    }
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
    // Load, trust and the policy gate, as the run would pass them; the
    // budget and tenant admission apply when the approved run is submitted.
    let principal = crate::runtime::Principal::for_body(state, actor.clone(), req.trusted_config);
    let loaded = crate::runtime::server::admit(
        state,
        &principal,
        crate::runtime::server::Submission::new(&req.config, req.config_format.into())
            .selected(req.selection.as_ref())
            .with_caller_fields(&req.caller_fields),
        crate::runtime::server::Gates {
            policy: true,
            budget: None,
            admit: false,
        },
    )
    .await?
    .loaded;
    loaded.require_matrix()?;
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
    let selection = req.selection.as_ref().map(|s| s.canonical());
    let depends_on = req
        .depends_on
        .as_ref()
        .filter(|d| !d.is_empty())
        .map(|d| d.canonical());
    Ok(ChangePlan {
        material: run_material_ordered(&rows, selection.as_deref(), depends_on.as_deref()),
        summary: json!({
            "pipeline": loaded.cfg.name,
            "rows": rows.len(),
            "selection": selection,
            "depends_on": req.depends_on.as_ref().filter(|d| !d.is_empty()),
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
    let tests = crate::templates::bundle::version_tests(
        &store,
        &payload.id,
        version,
        crate::serve::history::templates::RESULTS_RETAIN,
    )
    .await
    .map_err(template_err)?;
    // The body is part of the material: approval covers the document the
    // reviewer saw, not just its version number.
    let body = store
        .template_get(&payload.id, Some(version))
        .await
        .map_err(ServeError::from)?
        .map(|r| sha_hex(&r.body))
        .unwrap_or_default();
    Ok(ChangePlan {
        material: sha_hex(&format!(
            "{}\n{}\n{:?}\n{}",
            payload.id, version, st.stable, body
        )),
        summary: json!({
            "template": payload.id,
            "target_version": version,
            "selector": target,
            "stable_before": st.stable,
            "status_before": st.status,
            "tests": tests.status,
        }),
        rows: Vec::new(),
    })
}

#[cfg(feature = "templates")]
async fn existing_state(
    state: &ServerState,
    id: &str,
) -> Result<crate::serve::history::templates::TemplateState, ServeError> {
    let st = crate::templates::template_state(&state.history(), id)
        .await
        .map_err(template_err)?;
    if st.versions.is_empty() {
        return Err(ServeError::NotFound);
    }
    Ok(st)
}

#[cfg(feature = "templates")]
async fn resolve_existing(
    state: &ServerState,
    id: &str,
    st: &crate::serve::history::templates::TemplateState,
    selector: crate::serve::history::templates::VersionSelector,
) -> Result<u32, ServeError> {
    let version = crate::templates::resolve_version(&state.history(), id, selector)
        .await
        .map_err(template_err)?;
    if !st.versions.contains(&version) {
        return Err(ServeError::Unprocessable {
            message: format!("template '{id}' has no version {version}"),
            details: None,
        });
    }
    Ok(version)
}

#[cfg(feature = "templates")]
async fn plan_template_delete(
    state: &ServerState,
    payload: &DeletePayload,
) -> Result<ChangePlan, ServeError> {
    let st = existing_state(state, &payload.id).await?;
    let target = match payload.version {
        Some(sel) => Some(resolve_existing(state, &payload.id, &st, sel).await?),
        None => None,
    };
    let removes: Vec<u32> = match target {
        Some(v) => vec![v],
        None => st.versions.clone(),
    };
    Ok(ChangePlan {
        material: sha_hex(&format!(
            "delete\n{}\n{:?}\n{:?}",
            payload.id, removes, st.stable
        )),
        summary: json!({
            "template": payload.id,
            "delete": match target {
                Some(v) => json!(v),
                None => json!("all"),
            },
            "versions_removed": removes,
            "stable_before": st.stable,
            "newest_before": st.newest,
        }),
        rows: Vec::new(),
    })
}

#[cfg(feature = "templates")]
async fn plan_template_deprecate(
    state: &ServerState,
    payload: &DeprecatePayload,
) -> Result<ChangePlan, ServeError> {
    let st = existing_state(state, &payload.id).await?;
    let version = resolve_existing(state, &payload.id, &st, payload.version).await?;
    let retired = st.version_deprecation(version).is_some();
    Ok(ChangePlan {
        material: sha_hex(&format!(
            "deprecate\n{}\n{version}\n{}\n{retired}",
            payload.id, payload.undo
        )),
        summary: json!({
            "template": payload.id,
            "target_version": version,
            "selector": payload.version,
            "action": if payload.undo { "revive" } else { "retire" },
            "deprecated_before": retired,
            "newest_before": st.newest,
            "reason": payload.reason,
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
        refused @ CliError::VersionNotPassing { .. } => {
            crate::serve::handlers::templates::not_passing(refused)
        }
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
    trusted: bool,
    caller_params: &crate::params::CallerFields,
) -> Result<ChangePlan, ServeError> {
    match kind {
        ChangeKind::Run => {
            let mut req: SubmitRequest = serde_json::from_value(payload.clone())
                .map_err(|e| ServeError::BadConfig(format!("run payload: {e}")))?;
            req.trusted_config = trusted;
            req.caller_fields = caller_params.clone();
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
        #[cfg(feature = "templates")]
        ChangeKind::TemplateDelete => {
            let body: DeletePayload = serde_json::from_value(payload.clone())
                .map_err(|e| ServeError::BadConfig(format!("template_delete payload: {e}")))?;
            plan_template_delete(state, &body).await
        }
        #[cfg(feature = "templates")]
        ChangeKind::TemplateDeprecate => {
            let body: DeprecatePayload = serde_json::from_value(payload.clone())
                .map_err(|e| ServeError::BadConfig(format!("template_deprecate payload: {e}")))?;
            plan_template_deprecate(state, &body).await
        }
        #[cfg(not(feature = "templates"))]
        ChangeKind::TemplateRegister
        | ChangeKind::TemplateLaunch
        | ChangeKind::TemplateDelete
        | ChangeKind::TemplateDeprecate => {
            let _ = (state, actor, trusted);
            Err(ServeError::Unprocessable {
                message: "template changes require a server built with the `templates` feature"
                    .into(),
                details: None,
            })
        }
        ChangeKind::Schedule => plan_schedule(state, actor, payload).await,
        #[cfg(feature = "policy")]
        ChangeKind::PolicyUpdate | ChangeKind::TenantPolicyUpdate => {
            policy_update::plan(state, kind, payload).await
        }
        #[cfg(not(feature = "policy"))]
        ChangeKind::PolicyUpdate | ChangeKind::TenantPolicyUpdate => {
            Err(ServeError::Unprocessable {
                message: "policy changes require a server built with the `policy` feature".into(),
                details: None,
            })
        }
    }
}

/// Plan a schedule change (#1044): validate it as the requester; the rows
/// (which carry the stored revision it edits) are the material, so an edit
/// made meanwhile invalidates the approval.
#[cfg(all(feature = "schedule", feature = "templates"))]
async fn plan_schedule(
    state: &ServerState,
    actor: &AuthContext,
    payload: &Value,
) -> Result<ChangePlan, ServeError> {
    let p: crate::serve::schedules::service::ChangePayload =
        serde_json::from_value(payload.clone())
            .map_err(|e| ServeError::BadConfig(format!("schedule payload: {e}")))?;
    let rows = crate::serve::schedules::service::plan(state, actor, &p).await?;
    Ok(ChangePlan {
        material: material_of_rows(&rows),
        summary: json!({"schedule": p.name, "action": p.action.as_str()}),
        rows,
    })
}

#[cfg(not(all(feature = "schedule", feature = "templates")))]
async fn plan_schedule(
    state: &ServerState,
    actor: &AuthContext,
    payload: &Value,
) -> Result<ChangePlan, ServeError> {
    let _ = (state, actor, payload);
    Err(ServeError::Unprocessable {
        message: "schedule changes require a server built with the `schedule` and `templates` \
                  features"
            .into(),
        details: None,
    })
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

/// Write `c` only if the stored request is still `from`: two approvals, or an
/// approval racing a rejection or the expiry sweep, cannot both win.
async fn transition(
    state: &ServerState,
    c: &ChangeRequest,
    from: ChangeStatus,
) -> Result<(), ServeError> {
    if state
        .history()
        .change_transition(c, from)
        .await
        .map_err(store_err)?
    {
        Ok(())
    } else {
        Err(ServeError::Conflict(format!(
            "change {} was changed concurrently; reload it",
            c.id
        )))
    }
}

/// Write `c` only if the stored request is still exactly `before` (what this
/// call read): a concurrent approval elsewhere makes this one reload.
async fn transition_from(
    state: &ServerState,
    c: &ChangeRequest,
    before: &ChangeRequest,
) -> Result<(), ServeError> {
    if state
        .history()
        .change_transition_from(c, before)
        .await
        .map_err(store_err)?
    {
        Ok(())
    } else {
        Err(ServeError::Conflict(format!(
            "change {} was changed concurrently; reload it",
            c.id
        )))
    }
}

/// The change kinds whose approval rules a request must meet: its own, plus a
/// launch's when a registration also launches the template or moves a channel.
fn approval_kinds(state: &ServerState, kind: ChangeKind, payload: &Value) -> Vec<ChangeKind> {
    let launches = payload.get("launch").and_then(Value::as_bool) == Some(true)
        || payload
            .get("tags")
            .and_then(Value::as_array)
            .is_some_and(|t| !t.is_empty());
    if kind == ChangeKind::TemplateRegister && launches {
        vec![kind, ChangeKind::TemplateLaunch]
    } else if kind == ChangeKind::Schedule
        && state.requires_approval(ChangeKind::Run)
        && payload.get("action").and_then(Value::as_str) != Some("delete")
    {
        // A schedule starts runs: its change needs the run rule too (#1051).
        vec![kind, ChangeKind::Run]
    } else {
        vec![kind]
    }
}

/// Whether each rule of a multi-rule change is met by its own approvers,
/// rather than every approver meeting every rule (#1051): a schedule change
/// under run approval.
fn rules_met_independently(kind: ChangeKind) -> bool {
    kind == ChangeKind::Schedule
}

/// Serializes every transition of one change request within this process.
fn change_lock(id: &str) -> std::sync::Arc<tokio::sync::Mutex<()>> {
    static LOCKS: std::sync::LazyLock<
        dashmap::DashMap<String, std::sync::Arc<tokio::sync::Mutex<()>>>,
    > = std::sync::LazyLock::new(dashmap::DashMap::new);
    LOCKS.entry(id.to_string()).or_default().clone()
}

/// How long an `approved` request may sit unexecuted before the sweep treats
/// its execution as interrupted.
const STALE_APPROVED: chrono::TimeDelta = chrono::TimeDelta::minutes(5);

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
        let labels: std::collections::BTreeMap<String, String> = payload
            .get("labels")
            .filter(|l| !l.is_null())
            .map(|l| serde_json::from_value(l.clone()))
            .transpose()
            .map_err(|e| ServeError::BadConfig(format!("labels: {e}")))?
            .unwrap_or_default();
        crate::serve::history::labels::check_submitted(&labels, new.trusted_config).map_err(
            |message| ServeError::Unprocessable {
                message,
                details: None,
            },
        )?;
        // The request is stored before it runs, so its callback is held to
        // the rules a submitted run's is, now.
        if let Some(cb) = payload.get("callback").filter(|c| !c.is_null()) {
            let cb: crate::serve::callback::CallbackSpec = serde_json::from_value(cb.clone())
                .map_err(|e| ServeError::BadConfig(format!("callback: {e}")))?;
            cb.validate_for(state.callback_allow_hosts(), actor.tenant.is_some())
                .and_then(|()| cb.reject_persisted_headers(state.persistent_history()))
                .map_err(|message| ServeError::Unprocessable {
                    message,
                    details: None,
                })?;
        }
    }
    // A schedule change names its schedule and carries its secrets sealed
    // before it is stored (#1051).
    #[cfg(all(feature = "schedule", feature = "templates"))]
    if new.kind == ChangeKind::Schedule {
        payload = crate::serve::schedules::service::prepare_change(state, actor, payload).await?;
    }
    let plan = plan_for(
        state,
        actor,
        new.kind,
        &payload,
        new.trusted_config,
        &new.caller_params,
    )
    .await?;
    let policy = state.approvals();
    let required_approvals = approval_kinds(state, new.kind, &payload)
        .into_iter()
        .map(|k| policy.effective(k).min_approvers)
        .max()
        .unwrap_or(1);
    let now = Utc::now();
    // A tenant policy change belongs to the tenant whose policy it changes,
    // whoever proposed it (a server admin through the tenant's route).
    let tenant = if new.kind == ChangeKind::TenantPolicyUpdate {
        payload
            .get("tenant")
            .and_then(Value::as_str)
            .map(str::to_string)
    } else {
        actor.tenant.clone()
    };
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
        required_approvals,
        approvals: Vec::new(),
        rejection: None,
        created_at: now,
        updated_at: now,
        expires_at: now + chrono::Duration::seconds(state.approval_expiry().as_secs() as i64),
        run_id: None,
        template: None,
        error: None,
        tenant,
        trusted_config: new.trusted_config,
        caller_params: new.caller_params,
        deferred: false,
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
    notify_requested(state, &change).await;
    Ok(change)
}

/// Ping the approvers through the proposed config's own `notifications:`
/// block (a run request only — that is the config that names the channels).
/// Best-effort: a malformed block is logged, never an error.
#[cfg(feature = "notify")]
async fn notify_requested(state: &ServerState, change: &ChangeRequest) {
    if change.kind != ChangeKind::Run {
        return;
    }
    let Ok(req) = serde_json::from_value::<SubmitRequest>(change.payload.clone()) else {
        return;
    };
    // Load the config the way the plan did, so `${env:…}` webhook URLs and the
    // `--default-config` base resolve before the notifier is built.
    let mut requester = AuthContext::system("change");
    requester.tenant = change.tenant.clone();
    let principal = crate::runtime::Principal::for_body(state, requester, change.trusted_config);
    let loaded = match crate::runtime::server::load(
        state,
        &principal,
        crate::runtime::server::Submission::new(&req.config, req.config_format.into())
            .selected(req.selection.as_ref())
            .with_caller_fields(&change.caller_params),
    )
    .await
    {
        Ok(l) => l,
        Err(e) => {
            tracing::warn!(change = %change.id, "change_requested notification skipped: {e}");
            return;
        }
    };
    let pipeline = loaded
        .cfg
        .name
        .clone()
        .unwrap_or_else(|| "serve".to_string());
    match crate::notify::Notifier::from_specs(&loaded.cfg.notifications) {
        Ok(Some(n)) => {
            let event = crate::notify::NotifyEvent::change_requested(
                pipeline,
                &change.id,
                change.kind.as_str(),
                &change.requester,
                change.reason.as_deref().unwrap_or(""),
            );
            crate::runtime::under(&principal, n.emit(event)).await;
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
        if !state
            .history()
            .change_transition(&change, ChangeStatus::Pending)
            .await
            .map_err(store_err)?
        {
            // Approved or rejected in the meantime: report what is stored.
            return load(state, &change.id).await;
        }
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
    let lock = change_lock(id);
    let _held = lock.lock().await;
    let mut change = get(state, id).await?;
    if change.status != ChangeStatus::Pending {
        return Err(ServeError::Conflict(format!(
            "change {id} is {}, not pending",
            change.status.as_str()
        )));
    }
    let before = change.clone();
    let kinds = approval_kinds(state, change.kind, &change.payload);
    let policy = state.approvals();
    if rules_met_independently(change.kind) {
        let reasons: Vec<String> = kinds
            .iter()
            .filter_map(|k| policy.may_approve(*k, actor, &change.requester).err())
            .collect();
        if reasons.len() == kinds.len() {
            record_metric(change.kind, "denied");
            return Err(ServeError::Forbidden(reasons.join("; ")));
        }
    } else {
        for kind in &kinds {
            policy
                .may_approve(*kind, actor, &change.requester)
                .map_err(|reason| {
                    record_metric(change.kind, "denied");
                    ServeError::Forbidden(reason)
                })?;
        }
    }
    #[cfg(feature = "policy")]
    if change.kind.is_policy() {
        policy_update::approver_in_scope(actor, change.tenant.as_deref()).map_err(|reason| {
            record_metric(change.kind, "denied");
            ServeError::Forbidden(reason)
        })?;
    }
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
    let quorum = if rules_met_independently(change.kind) {
        kinds.iter().all(|k| {
            let met = change
                .approvals
                .iter()
                .filter(|a| {
                    let who = AuthContext {
                        principal: a.principal.clone(),
                        role: a.role,
                        source_ip: None,
                        tenant: None,
                    };
                    policy.may_approve(*k, &who, &change.requester).is_ok()
                })
                .count() as u32;
            met >= policy.effective(*k).min_approvers
        })
    } else {
        change.approvals.len() as u32 >= change.required_approvals
    };
    if quorum {
        change.status = ChangeStatus::Approved;
    }
    transition_from(state, &change, &before).await?;
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
    if !quorum {
        return Ok(change);
    }
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
    let lock = change_lock(id);
    let _held = lock.lock().await;
    let mut change = get(state, id).await?;
    if change.status != ChangeStatus::Pending {
        return Err(ServeError::Conflict(format!(
            "change {id} is {}, not pending",
            change.status.as_str()
        )));
    }
    if actor.principal != change.requester {
        let kinds = approval_kinds(state, change.kind, &change.payload);
        let policy = state.approvals();
        let refusals: Vec<String> = kinds
            .iter()
            .filter_map(|k| policy.may_approve(*k, actor, "").err())
            .collect();
        let refused = if rules_met_independently(change.kind) {
            refusals.len() == kinds.len()
        } else {
            !refusals.is_empty()
        };
        if refused {
            return Err(ServeError::Forbidden(refusals.join("; ")));
        }
        #[cfg(feature = "policy")]
        if change.kind.is_policy() {
            policy_update::approver_in_scope(actor, change.tenant.as_deref())
                .map_err(ServeError::Forbidden)?;
        }
    }
    change.status = ChangeStatus::Rejected;
    change.rejection = Some(Rejection {
        principal: actor.principal.clone(),
        at: Utc::now(),
        reason,
    });
    change.touch();
    transition(state, &change, ChangeStatus::Pending).await?;
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
    if let Err(why) = requester_still_authorized(state.auth_mode(), &change) {
        return finish_failed(state, actor, change, why).await;
    }
    let requester = AuthContext {
        principal: change.requester.clone(),
        role: change.requester_role,
        source_ip: None,
        tenant: change.tenant.clone(),
    };
    // A policy change whose version was stored before its execution was
    // interrupted is done: re-planning would see its own version and call it
    // invalidated.
    #[cfg(feature = "policy")]
    if change.kind.is_policy()
        && let Some(version) = policy_update::already_applied(state, &change).await?
    {
        if let Some(plan) = &mut change.plan {
            plan.summary["version"] = json!(version);
        }
        change.status = ChangeStatus::Executed;
        change.touch();
        save(state, &change).await?;
        record_metric(change.kind, "executed");
        return Ok(change);
    }
    // Re-plan against the world as it is now. A planning failure (the
    // template is gone, the config no longer loads) fails the request.
    let fresh = match plan_for(
        state,
        &requester,
        change.kind,
        &change.payload,
        change.trusted_config,
        &change.caller_params,
    )
    .await
    {
        Ok(p) => p,
        Err(e) => return finish_refused(state, actor, change, e).await,
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
        return finish_invalidated(state, actor, change, why, Some(fresh)).await;
    }
    // The policy version a policy change stored (#1043), kept in the plan.
    #[allow(unused_mut)]
    let mut created_version: Option<u32> = None;
    let outcome: Result<(Option<String>, Option<TemplateOutcome>), ServeError> = match change.kind {
        ChangeKind::Run => {
            let mut req: SubmitRequest = match serde_json::from_value(change.payload.clone()) {
                Ok(r) => r,
                Err(e) => return finish_failed(state, actor, change, e.to_string()).await,
            };
            req.require_approval = false;
            req.reason = None;
            req.approved_change = Some(change.id.clone());
            req.trusted_config = change.trusted_config;
            req.caller_fields = change.caller_params.clone();
            req.budget = match (req.budget.take(), change.budget.clone()) {
                (Some(a), Some(b)) => Some(a.merge(&b)),
                (a, b) => a.or(b),
            };
            req.labels.insert("change".to_string(), change.id.clone());
            // A retried execution (a double approval, the sweep reconciling an
            // interrupted one) replays this run instead of starting another.
            req.idempotency_key
                .get_or_insert_with(|| format!("change:{}", change.id));
            // The approved run, as its requester: every stage runs again
            // at submission, and the approval was granted (`approved_change`).
            let principal =
                crate::runtime::Principal::for_body(state, requester.clone(), req.trusted_config);
            crate::runtime::server::submit(
                state,
                &principal,
                req,
                crate::runtime::server::Approval::Required,
            )
            .await
            .and_then(|o| o.accepted())
            .map(|r| (Some(r.run_id), None))
        }
        #[cfg(feature = "templates")]
        ChangeKind::TemplateRegister => {
            let body: crate::serve::handlers::templates::RegisterBody =
                match serde_json::from_value(change.payload.clone()) {
                    Ok(b) => b,
                    Err(e) => return finish_failed(state, actor, change, e.to_string()).await,
                };
            // The version is stored even when its test run, a channel or the
            // launch is then refused; the request records it either way.
            match crate::templates::register_staged(
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
            {
                Ok(mut r) => {
                    let registered = TemplateOutcome {
                        id: r.record.id.clone(),
                        version: r.record.version,
                    };
                    match r.after.take() {
                        None => Ok((None, Some(registered))),
                        Some(e) => {
                            change.template = Some(registered);
                            Err(template_err(e))
                        }
                    }
                }
                Err(e) => Err(template_err(e)),
            }
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
        #[cfg(feature = "templates")]
        ChangeKind::TemplateDelete => {
            let body: DeletePayload = match serde_json::from_value(change.payload.clone()) {
                Ok(b) => b,
                Err(e) => return finish_failed(state, actor, change, e.to_string()).await,
            };
            execute_template_delete(state, &body, &fresh).await
        }
        #[cfg(feature = "templates")]
        ChangeKind::TemplateDeprecate => {
            let body: DeprecatePayload = match serde_json::from_value(change.payload.clone()) {
                Ok(b) => b,
                Err(e) => return finish_failed(state, actor, change, e.to_string()).await,
            };
            execute_template_deprecate(state, &body, &fresh, &change.requester).await
        }
        #[cfg(not(feature = "templates"))]
        ChangeKind::TemplateRegister
        | ChangeKind::TemplateLaunch
        | ChangeKind::TemplateDelete
        | ChangeKind::TemplateDeprecate => Err(ServeError::Unprocessable {
            message: "template changes require the `templates` feature".into(),
            details: None,
        }),
        #[cfg(all(feature = "schedule", feature = "templates"))]
        ChangeKind::Schedule => {
            let p: crate::serve::schedules::service::ChangePayload =
                match serde_json::from_value(change.payload.clone()) {
                    Ok(p) => p,
                    Err(e) => return finish_failed(state, actor, change, e.to_string()).await,
                };
            crate::serve::schedules::service::apply(state, &requester, p, true)
                .await
                .map(|_| (None, None))
        }
        #[cfg(not(all(feature = "schedule", feature = "templates")))]
        ChangeKind::Schedule => Err(ServeError::Unprocessable {
            message: "schedule changes require the `schedule` and `templates` features".into(),
            details: None,
        }),
        #[cfg(feature = "policy")]
        ChangeKind::PolicyUpdate | ChangeKind::TenantPolicyUpdate => {
            policy_update::execute(state, &change, &fresh, actor)
                .await
                .map(|v| {
                    created_version = Some(v);
                    (None, None)
                })
        }
        #[cfg(not(feature = "policy"))]
        ChangeKind::PolicyUpdate | ChangeKind::TenantPolicyUpdate => {
            Err(ServeError::Unprocessable {
                message: "policy changes require the `policy` feature".into(),
                details: None,
            })
        }
    };
    match outcome {
        Ok((run_id, template)) => {
            let mut fresh = fresh;
            if let Some(v) = created_version {
                fresh.summary["version"] = json!(v);
            }
            change.status = ChangeStatus::Executed;
            change.deferred = false;
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
        // A policy or schedule change that another landed change has made
        // stale.
        Err(ServeError::Conflict(why))
            if change.kind.is_policy() || change.kind == ChangeKind::Schedule =>
        {
            finish_invalidated(state, actor, change, why, None).await
        }
        Err(e) => finish_refused(state, actor, change, e).await,
    }
}

/// Mark a change invalidated with `why` (and the plan it was checked
/// against, when re-planned).
async fn finish_invalidated(
    state: &ServerState,
    actor: &AuthContext,
    mut change: ChangeRequest,
    why: String,
    plan: Option<ChangePlan>,
) -> Result<ChangeRequest, ServeError> {
    change.status = ChangeStatus::Invalidated;
    change.deferred = false;
    change.error = Some(crate::secrets::registry::redact_message(&why));
    if plan.is_some() {
        change.plan = plan;
    }
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
    Ok(change)
}

/// Whether an execution refusal may pass on its own: the server could not
/// take the work now, nothing about the change is wrong (#1050).
fn temporary_refusal(e: &ServeError) -> bool {
    matches!(
        e,
        ServeError::Unavailable(_)
            | ServeError::QueueFull { .. }
            | ServeError::TooManyRequests(_)
            | ServeError::Draining
    )
}

/// An execution the server refused: a temporary refusal keeps the change
/// `approved` and marks it deferred, and [`reconcile_stale_approved`] runs it
/// again later; any other refusal fails it. A registration that already
/// stored its version is never run again (it would add another version).
async fn finish_refused(
    state: &ServerState,
    actor: &AuthContext,
    mut change: ChangeRequest,
    e: ServeError,
) -> Result<ChangeRequest, ServeError> {
    if !temporary_refusal(&e) {
        return finish_failed(state, actor, change, e.to_string()).await;
    }
    if change.kind == ChangeKind::TemplateRegister
        && let Some(t) = &change.template
    {
        let why = format!(
            "version {} of '{}' was registered, then {e}; finish its launch or channels \
             with a new change",
            t.version, t.id
        );
        return finish_failed(state, actor, change, why).await;
    }
    change.deferred = true;
    change.error = Some(crate::secrets::registry::redact_message(&format!(
        "{e}; the approved change runs again once the server can take it"
    )));
    change.touch();
    if let Err(save_err) = save(state, &change).await {
        tracing::warn!(change = %change.id, error = %save_err, "could not record a deferred change execution");
    }
    record_metric(change.kind, "deferred");
    crate::serve::audit::write(
        state,
        actor,
        "change.deferred",
        None,
        Some(change.id.clone()),
        "error",
    )
    .await;
    Ok(change)
}

/// The version a planned `template_*` change targets, from its summary.
#[cfg(feature = "templates")]
fn planned_version(plan: &ChangePlan, key: &str) -> Option<u32> {
    plan.summary
        .get(key)
        .and_then(Value::as_u64)
        .and_then(|v| u32::try_from(v).ok())
}

#[cfg(feature = "templates")]
async fn execute_template_delete(
    state: &ServerState,
    body: &DeletePayload,
    plan: &ChangePlan,
) -> Result<(Option<String>, Option<TemplateOutcome>), ServeError> {
    let target = planned_version(plan, "delete");
    let removed = state
        .history()
        .template_delete(&body.id, target)
        .await
        .map_err(ServeError::from)?;
    if removed == 0 {
        return Err(ServeError::NotFound);
    }
    Ok((
        None,
        target.map(|version| TemplateOutcome {
            id: body.id.clone(),
            version,
        }),
    ))
}

#[cfg(feature = "templates")]
async fn execute_template_deprecate(
    state: &ServerState,
    body: &DeprecatePayload,
    plan: &ChangePlan,
    requester: &str,
) -> Result<(Option<String>, Option<TemplateOutcome>), ServeError> {
    let version = planned_version(plan, "target_version").ok_or_else(|| {
        ServeError::Internal("template_deprecate plan carries no target version".into())
    })?;
    crate::templates::set_version_deprecated(
        &state.history(),
        &body.id,
        version,
        body.reason.clone(),
        Some(requester),
        !body.undo,
    )
    .await
    .map_err(template_err)?;
    Ok((
        None,
        Some(TemplateOutcome {
            id: body.id.clone(),
            version,
        }),
    ))
}

/// Under `--auth-config`, the requester must still be a configured principal
/// with at least the role and the same tenant scope it held when it asked.
/// Server-originated requesters (`system:`, `trigger:`, `sync:`, `runtime`)
/// are not in the file and are not checked.
fn requester_still_authorized(
    mode: &crate::serve::config::AuthMode,
    change: &ChangeRequest,
) -> Result<(), String> {
    let crate::serve::config::AuthMode::Rbac(rbac) = mode else {
        return Ok(());
    };
    let name = change.requester.as_str();
    let Some((role, tenant)) = rbac.principal(name) else {
        if crate::serve::rbac::is_server_identity(name) {
            return Ok(());
        }
        return Err(format!(
            "requester '{name}' is no longer a principal in --auth-config, so the approved \
             change cannot run on its behalf"
        ));
    };
    let downgraded = change
        .requester_role
        .permissions()
        .into_iter()
        .any(|p| !role.grants(p));
    if downgraded {
        return Err(format!(
            "requester '{name}' now holds role {role:?}, below the {:?} it held when it \
             requested this change, so the approved change cannot run on its behalf",
            change.requester_role
        ));
    }
    // `change.tenant` is the tenant the change runs under: the requester's own
    // scope or the tenant route an unscoped principal proposed it through. An
    // unscoped principal may act for any tenant, so only a principal that is
    // now scoped to a different tenant (or scoped at all, for an untenanted
    // change) has lost the right to run it.
    if let Some(scope) = tenant
        && Some(scope) != change.tenant.as_deref()
    {
        return Err(format!(
            "requester '{name}' is no longer scoped to the tenant this change was \
             requested for, so the approved change cannot run on its behalf"
        ));
    }
    Ok(())
}

async fn finish_failed(
    state: &ServerState,
    actor: &AuthContext,
    mut change: ChangeRequest,
    error: String,
) -> Result<ChangeRequest, ServeError> {
    change.status = ChangeStatus::Failed;
    change.deferred = false;
    change.error = Some(crate::secrets::registry::redact_message(&error));
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

/// Change requests per status matching `filter`, ignoring its limit (#998).
/// Pending requests past their window are expired first, so a lapsed one
/// counts as `expired` here exactly as the listing shows it.
pub async fn counts(
    state: &ServerState,
    filter: &ChangeListFilter,
) -> Result<crate::serve::history::StatusCounts, ServeError> {
    expire_due(state).await;
    state
        .history()
        .change_counts(filter)
        .await
        .map_err(store_err)
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
    reconcile_stale_approved(state).await;
    n
}

/// Finish requests left `approved`: one whose execution was deferred (a
/// temporary refusal) is executed again, whatever its kind; one whose
/// execution never recorded its outcome (a crash or a failed save between
/// approval and `executed`) is executed again too — the run's `change:<id>`
/// idempotency key replays the original submission, a launch of the live
/// version is a no-op — except a registration, which would add a second
/// version and fails with an explanation instead.
pub async fn reconcile_stale_approved(state: &ServerState) -> usize {
    let approved = match state
        .history()
        .change_list(&ChangeListFilter {
            status: Some(ChangeStatus::Approved),
            limit: 10_000,
            ..Default::default()
        })
        .await
    {
        Ok(a) => a,
        Err(e) => {
            tracing::warn!("change reconcile sweep: {e}");
            return 0;
        }
    };
    let cutoff = Utc::now() - STALE_APPROVED;
    let mut n = 0;
    for c in approved.into_iter().filter(|c| c.updated_at <= cutoff) {
        let lock = change_lock(&c.id);
        let _held = lock.lock().await;
        let Ok(c) = load(state, &c.id).await else {
            continue;
        };
        if c.status != ChangeStatus::Approved {
            continue;
        }
        let actor = AuthContext::system("reconcile");
        let done = if c.kind == ChangeKind::TemplateRegister && c.deferred {
            // Cleared before the attempt: one interrupted from here on is
            // not known to be safe to run again.
            let mut c = c;
            c.deferred = false;
            c.touch();
            match save(state, &c).await {
                Ok(()) => execute(state, &actor, c).await,
                Err(e) => Err(e),
            }
        } else if c.kind == ChangeKind::TemplateRegister {
            finish_failed(
                state,
                &actor,
                c,
                "execution was interrupted after approval and its outcome is unknown; check \
                 the template's versions before proposing it again"
                    .into(),
            )
            .await
        } else {
            execute(state, &actor, c).await
        };
        match done {
            Ok(_) => n += 1,
            Err(e) => tracing::warn!("change reconcile sweep: {e}"),
        }
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
#[allow(clippy::disallowed_methods)] // tests drive time and raw clients directly
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
        assert_eq!(
            ChangeKind::TemplateDeprecate.governed_by(),
            Some(ChangeKind::TemplateLaunch)
        );
        assert_eq!(ChangeKind::TemplateLaunch.governed_by(), None);
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
            tenant: Some("acme".into()),
            trusted_config: false,
            caller_params: Default::default(),
            deferred: false,
        };
        assert!(ChangeListFilter::default().matches(&c));
        assert!(
            !ChangeListFilter {
                tenant: Some("globex".into()),
                ..Default::default()
            }
            .matches(&c)
        );
        assert!(
            ChangeListFilter {
                status: Some(ChangeStatus::Pending),
                kind: Some(ChangeKind::Run),
                requester: Some("bob".into()),
                tenant: Some("acme".into()),
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
            tenant: None,
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
            tenant: None,
            trusted_config: false,
            caller_params: Default::default(),
            deferred: false,
        }
    }

    #[test]
    fn execution_requires_the_requester_to_still_be_authorized() {
        use crate::serve::config::AuthMode;
        use crate::serve::rbac::{PrincipalSpec, RbacConfig};
        let spec = |name: &str, role, tenant: Option<&str>| PrincipalSpec {
            name: name.into(),
            token: format!("tok-{name}"),
            role,
            tenant: tenant.map(Into::into),
        };
        let mode = |ps| AuthMode::Rbac(std::sync::Arc::new(RbacConfig::new(ps).unwrap()));
        let c = stored(ChangeKind::Run, json!({}), "m", 1);

        assert!(requester_still_authorized(&AuthMode::None, &c).is_ok());
        assert!(
            requester_still_authorized(&mode(vec![spec("bob", Role::Operator, None)]), &c).is_ok()
        );
        assert!(
            requester_still_authorized(&mode(vec![spec("bob", Role::Admin, None)]), &c).is_ok()
        );
        let gone = requester_still_authorized(&mode(vec![spec("amy", Role::Admin, None)]), &c)
            .unwrap_err();
        assert!(gone.contains("no longer a principal"), "{gone}");
        let down = requester_still_authorized(&mode(vec![spec("bob", Role::Viewer, None)]), &c)
            .unwrap_err();
        assert!(down.contains("Viewer"), "{down}");
        let scoped =
            requester_still_authorized(&mode(vec![spec("bob", Role::Operator, Some("acme"))]), &c)
                .unwrap_err();
        assert!(scoped.contains("tenant"), "{scoped}");

        // An unscoped principal that proposed through a tenant route.
        let mut routed = c.clone();
        routed.tenant = Some("acme".into());
        assert!(
            requester_still_authorized(&mode(vec![spec("bob", Role::Operator, None)]), &routed)
                .is_ok()
        );
        assert!(
            requester_still_authorized(
                &mode(vec![spec("bob", Role::Operator, Some("acme"))]),
                &routed
            )
            .is_ok()
        );
        let moved = requester_still_authorized(
            &mode(vec![spec("bob", Role::Operator, Some("globex"))]),
            &routed,
        )
        .unwrap_err();
        assert!(moved.contains("tenant"), "{moved}");

        let mut sys = c.clone();
        sys.requester = "sync:origin".into();
        assert!(
            requester_still_authorized(&mode(vec![spec("amy", Role::Admin, None)]), &sys).is_ok()
        );
        // A removed principal whose name merely contains a colon is not a
        // server actor.
        let mut colon = c.clone();
        colon.requester = "ci:deploy".into();
        let gone = requester_still_authorized(&mode(vec![spec("amy", Role::Admin, None)]), &colon)
            .unwrap_err();
        assert!(gone.contains("no longer a principal"), "{gone}");
    }

    #[test]
    fn responses_mask_what_the_reader_may_not_see() {
        let mut c = stored(
            ChangeKind::Run,
            json!({"config": "pipeline: {}", "callback": {"url": "u", "headers": {"authorization": "Bearer x"}}}),
            "m",
            1,
        );
        let who = |role| AuthContext {
            principal: "p".into(),
            role,
            source_ip: None,
            tenant: None,
        };
        let admin = c.clone().redacted_for(&who(Role::Admin));
        assert_eq!(admin.payload, c.payload);
        let op = c.clone().redacted_for(&who(Role::Operator));
        assert_eq!(op.payload["config"], "pipeline: {}");
        assert_eq!(op.payload["callback"]["headers"]["authorization"], REDACTED);
        let viewer = c.clone().redacted_for(&who(Role::Viewer));
        assert_eq!(viewer.payload["config"], REDACTED);
        c.payload = json!({"id": "t"});
        assert_eq!(
            c.clone().redacted_for(&who(Role::Viewer)).payload,
            c.payload
        );

        let register = stored(
            ChangeKind::TemplateRegister,
            json!({"config": "auth: {token: inline}", "tags": ["dev"]}),
            "m",
            1,
        );
        assert_eq!(
            register.clone().redacted_for(&who(Role::Admin)).payload,
            register.payload,
            "an unscoped admin reads the proposed body"
        );
        let op = register.clone().redacted_for(&who(Role::Operator));
        assert_eq!(
            op.payload["config"], REDACTED,
            "approvers below admin do not"
        );
        assert_eq!(op.payload["tags"], json!(["dev"]));
        assert!(op.plan.is_some(), "the plan stays visible for review");
        let scoped = register.clone().redacted_for(&AuthContext {
            tenant: Some("acme".into()),
            ..who(Role::Admin)
        });
        assert_eq!(scoped.payload["config"], REDACTED);

        let mut from_template = stored(
            ChangeKind::Run,
            json!({"config": "pipeline: {token: bound}", "callback": {"url": "https://u:dummy-secret@hooks.example/x?token=dummy-secret&job=7"}}),
            "m",
            1,
        );
        from_template.trusted_config = true;
        let op = from_template.clone().redacted_for(&who(Role::Operator));
        assert_eq!(
            op.payload["config"], REDACTED,
            "a template body stays the admin's"
        );
        let url = op.payload["callback"]["url"].as_str().unwrap();
        assert!(
            !url.contains("dummy-secret") && url.contains("job=7"),
            "{url}"
        );
        let scoped = from_template.clone().redacted_for(&AuthContext {
            tenant: Some("acme".into()),
            ..who(Role::Admin)
        });
        assert_eq!(scoped.payload["config"], REDACTED);
        assert_eq!(
            from_template
                .clone()
                .redacted_for(&who(Role::Admin))
                .payload,
            from_template.payload
        );
    }

    #[test]
    fn a_registration_that_launches_needs_the_launch_rule_too() {
        let state = crate::serve::test_support::test_state();
        let approval_kinds = |k, p: &Value| approval_kinds(&state, k, p);
        assert_eq!(
            approval_kinds(ChangeKind::Run, &json!({})),
            vec![ChangeKind::Run]
        );
        assert_eq!(
            approval_kinds(ChangeKind::TemplateRegister, &json!({"launch": false})),
            vec![ChangeKind::TemplateRegister]
        );
        let launches = vec![ChangeKind::TemplateRegister, ChangeKind::TemplateLaunch];
        assert_eq!(
            approval_kinds(ChangeKind::TemplateRegister, &json!({"launch": true})),
            launches
        );
        assert_eq!(
            approval_kinds(ChangeKind::TemplateRegister, &json!({"tags": ["prod"]})),
            launches
        );

        let policy: ApprovalPolicy = serde_json::from_value(json!({"rules": [
            {"kinds": ["template_register"], "roles": ["operator", "admin"]},
            {"kinds": ["template_launch"], "roles": ["admin"], "min_approvers": 2},
        ]}))
        .unwrap();
        let op = AuthContext {
            principal: "op".into(),
            role: Role::Operator,
            source_ip: None,
            tenant: None,
        };
        let refused = launches
            .iter()
            .find_map(|k| policy.may_approve(*k, &op, "bob").err())
            .expect("the launch rule refuses an operator");
        assert!(refused.contains("template_launch"), "{refused}");
        let quorum = launches
            .iter()
            .map(|k| policy.effective(*k).min_approvers)
            .max();
        assert_eq!(quorum, Some(2));
    }

    #[test]
    fn a_schedule_change_needs_the_run_rule_while_runs_need_approval() {
        let state = crate::serve::test_support::test_state();
        let update = json!({"action": "update", "name": "n"});
        assert_eq!(
            approval_kinds(&state, ChangeKind::Schedule, &update),
            vec![ChangeKind::Schedule]
        );
        let mut config = crate::serve::test_support::test_config();
        config.require_approval = vec![ChangeKind::Run, ChangeKind::Schedule];
        let gated = crate::serve::test_support::state_from(&config);
        assert_eq!(
            approval_kinds(&gated, ChangeKind::Schedule, &update),
            vec![ChangeKind::Schedule, ChangeKind::Run]
        );
        assert_eq!(
            approval_kinds(&gated, ChangeKind::Schedule, &json!({"action": "delete"})),
            vec![ChangeKind::Schedule]
        );
        assert!(rules_met_independently(ChangeKind::Schedule));
        assert!(!rules_met_independently(ChangeKind::TemplateRegister));
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
                trusted_config: false,
                caller_params: Default::default(),
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
                trusted_config: false,
                caller_params: Default::default(),
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ServeError::BadConfig(_)));
        let run = |labels: serde_json::Value, trusted_config| NewChange {
            kind: ChangeKind::Run,
            payload: json!({"config": "x", "labels": labels}),
            reason: None,
            budget: None,
            trusted_config,
            caller_params: Default::default(),
        };
        let err = create(&state, &actor("bob"), run(json!({"template": "t"}), false))
            .await
            .unwrap_err();
        assert!(
            matches!(err, ServeError::Unprocessable { ref message, .. } if message.contains("set by the server")),
            "{err:?}"
        );
        let err = create(&state, &actor("bob"), run(json!({"k": 1}), false))
            .await
            .unwrap_err();
        assert!(
            matches!(err, ServeError::BadConfig(ref m) if m.starts_with("labels:")),
            "{err:?}"
        );
        let err = create(&state, &actor("bob"), run(json!({"template": "t"}), true))
            .await
            .unwrap_err();
        assert!(
            !format!("{err:?}").contains("set by the server"),
            "a server-written request may carry provenance labels: {err:?}"
        );

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
        // The notifications block only parses with the notify feature.
        let notify = if cfg!(feature = "notify") {
            "notifications:\n  - name: log\n    on: [change_requested]\n    channel:\n      type: webhook\n      config:\n        url: http://127.0.0.1:9/hook\n"
        } else {
            ""
        };
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
                trusted_config: false,
                caller_params: Default::default(),
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
        let fresh_rows = plan_for(
            &state,
            &actor("bob"),
            ChangeKind::Run,
            &c.payload,
            false,
            &Default::default(),
        )
        .await
        .unwrap()
        .rows;
        c.plan.as_mut().unwrap().rows = fresh_rows;
        save(&state, &c).await.unwrap();
        let inv = approve(&state, &actor("alice"), &c.id, None).await.unwrap();
        assert_eq!(inv.status, ChangeStatus::Invalidated);
        assert!(inv.error.unwrap().contains("→"));
    }

    #[tokio::test]
    async fn concurrent_approvals_execute_once_and_a_rejection_cannot_be_overwritten() {
        let dir = tempfile::tempdir().unwrap();
        let state = crate::serve::test_support::test_state();
        let mk = || async {
            create(
                &state,
                &actor("bob"),
                NewChange {
                    kind: ChangeKind::Run,
                    payload: json!({ "config": csv_yaml(dir.path(), "race", "") }),
                    reason: Some("x".into()),
                    budget: None,
                    trusted_config: false,
                    caller_params: Default::default(),
                },
            )
            .await
            .unwrap()
        };
        let c = mk().await;
        let (alice, carol) = (actor("alice"), actor("carol"));
        let (a, b) = tokio::join!(
            approve(&state, &alice, &c.id, None),
            approve(&state, &carol, &c.id, None)
        );
        let executed: Vec<_> = [&a, &b]
            .into_iter()
            .filter_map(|r| r.as_ref().ok())
            .filter(|c| c.status == ChangeStatus::Executed)
            .collect();
        assert_eq!(executed.len(), 1, "{a:?} {b:?}");
        assert!(
            matches!(
                a.as_ref().err().or(b.as_ref().err()),
                Some(ServeError::Conflict(_))
            ),
            "{a:?} {b:?}"
        );

        let c = mk().await;
        reject(&state, &actor("alice"), &c.id, "no".into())
            .await
            .unwrap();
        let late = approve(&state, &actor("carol"), &c.id, None)
            .await
            .unwrap_err();
        assert!(matches!(late, ServeError::Conflict(_)), "{late:?}");
        assert_eq!(
            get(&state, &c.id).await.unwrap().status,
            ChangeStatus::Rejected
        );

        // The store-level compare-and-set refuses a stale writer.
        let mut stale = get(&state, &c.id).await.unwrap();
        stale.status = ChangeStatus::Approved;
        assert!(
            !state
                .history()
                .change_transition(&stale, ChangeStatus::Pending)
                .await
                .unwrap()
        );
        let err = transition(&state, &stale, ChangeStatus::Pending)
            .await
            .unwrap_err();
        assert!(matches!(err, ServeError::Conflict(_)));
    }

    #[tokio::test]
    async fn a_partial_approval_from_a_stale_read_does_not_drop_another() {
        use crate::serve::history::RunHistory as _;
        let store =
            crate::serve::history::memory::MemoryHistory::new(std::time::Duration::from_secs(60));
        let mut c = ChangeRequest {
            id: "chg-x".into(),
            kind: ChangeKind::Run,
            status: ChangeStatus::Pending,
            requester: "bob".into(),
            requester_role: Role::Operator,
            reason: None,
            payload: json!({}),
            plan: None,
            budget: None,
            required_approvals: 3,
            approvals: Vec::new(),
            rejection: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::hours(1),
            run_id: None,
            template: None,
            error: None,
            tenant: None,
            trusted_config: false,
            caller_params: Default::default(),
            deferred: false,
        };
        store.change_upsert(&c).await.unwrap();
        let read = c.clone();
        let approval = |who: &str| Approval {
            principal: who.into(),
            role: Role::Admin,
            at: Utc::now(),
            comment: None,
        };
        let mut first = read.clone();
        first.approvals.push(approval("alice"));
        first.updated_at = read.updated_at + chrono::Duration::seconds(1);
        assert!(store.change_transition_from(&first, &read).await.unwrap());
        c.approvals.push(approval("carol"));
        c.updated_at = read.updated_at + chrono::Duration::seconds(2);
        assert!(
            !store.change_transition_from(&c, &read).await.unwrap(),
            "carol's write from the stale read would drop alice's approval"
        );
        let stored = store.change_get("chg-x").await.unwrap().unwrap();
        assert_eq!(stored.approvals.len(), 1);
        assert_eq!(stored.approvals[0].principal, "alice");
    }

    #[tokio::test]
    async fn the_sweep_finishes_requests_stranded_in_approved() {
        let dir = tempfile::tempdir().unwrap();
        let state = crate::serve::test_support::test_state();
        let created = create(
            &state,
            &actor("bob"),
            NewChange {
                kind: ChangeKind::Run,
                payload: json!({ "config": csv_yaml(dir.path(), "stranded", "") }),
                reason: None,
                budget: None,
                trusted_config: false,
                caller_params: Default::default(),
            },
        )
        .await
        .unwrap();
        let mut c = get(&state, &created.id).await.unwrap();
        c.status = ChangeStatus::Approved;
        save(&state, &c).await.unwrap();
        // Fresh: left alone.
        assert_eq!(reconcile_stale_approved(&state).await, 0);
        c.updated_at = Utc::now() - chrono::TimeDelta::minutes(10);
        save(&state, &c).await.unwrap();
        let mut reg = stored(ChangeKind::TemplateRegister, json!({"config": "x"}), "m", 1);
        reg.status = ChangeStatus::Approved;
        reg.updated_at = c.updated_at;
        save(&state, &reg).await.unwrap();
        assert_eq!(expire_due(&state).await, 0);
        let done = get(&state, &c.id).await.unwrap();
        assert_eq!(done.status, ChangeStatus::Executed, "{:?}", done.error);
        let run_id = done.run_id.clone().unwrap();
        let rec = state.history().get(&run_id).await.unwrap().unwrap();
        assert_eq!(
            rec.idempotency_key.as_deref(),
            Some(format!("change:{}", c.id).as_str())
        );
        let reg = get(&state, &reg.id).await.unwrap();
        assert_eq!(reg.status, ChangeStatus::Failed);
        assert!(reg.error.unwrap().contains("interrupted"));
    }

    #[tokio::test]
    async fn a_temporary_refusal_keeps_an_approved_change_for_the_sweep() {
        let dir = tempfile::tempdir().unwrap();
        let state = crate::serve::test_support::test_state();
        let created = create(
            &state,
            &actor("bob"),
            NewChange {
                kind: ChangeKind::Run,
                payload: json!({ "config": csv_yaml(dir.path(), "later", "") }),
                reason: None,
                budget: None,
                trusted_config: false,
                caller_params: Default::default(),
            },
        )
        .await
        .unwrap();
        state.set_draining();
        let deferred = approve(&state, &actor("alice"), &created.id, None)
            .await
            .unwrap();
        assert_eq!(deferred.status, ChangeStatus::Approved);
        assert!(deferred.error.unwrap().contains("runs again"));
        assert!(deferred.run_id.is_none());
        assert!(!temporary_refusal(&ServeError::BadConfig("x".into())));
        for e in [
            ServeError::Unavailable("down".into()),
            ServeError::QueueFull {
                retry_after_secs: 1,
            },
            ServeError::TooManyRequests("limit".into()),
            ServeError::Draining,
        ] {
            assert!(temporary_refusal(&e), "{e}");
        }
    }

    /// A deferred registration is run again by the sweep like any deferred
    /// change; one whose version was already stored is never run again.
    #[cfg(feature = "templates")]
    #[tokio::test]
    async fn the_sweep_runs_a_deferred_registration_again() {
        let dir = tempfile::tempdir().unwrap();
        let state = crate::serve::test_support::test_state();
        let created = create(
            &state,
            &actor("bob"),
            NewChange {
                kind: ChangeKind::TemplateRegister,
                payload: json!({"id": "tpl", "config": crate::templates::bundle::smoke(&csv_yaml(dir.path(), "tpl", ""))}),
                reason: None,
                budget: None,
                trusted_config: false,
                caller_params: Default::default(),
            },
        )
        .await
        .unwrap();
        let c = get(&state, &created.id).await.unwrap();
        let deferred = finish_refused(
            &state,
            &actor("alice"),
            c,
            ServeError::Unavailable("down".into()),
        )
        .await
        .unwrap();
        assert!(deferred.deferred);
        let mut c = get(&state, &created.id).await.unwrap();
        assert!(c.deferred, "the deferral is recorded");
        c.status = ChangeStatus::Approved;
        c.updated_at = Utc::now() - chrono::TimeDelta::minutes(10);
        save(&state, &c).await.unwrap();
        assert_eq!(reconcile_stale_approved(&state).await, 1);
        let done = get(&state, &c.id).await.unwrap();
        assert_eq!(done.status, ChangeStatus::Executed, "{:?}", done.error);
        assert_eq!(done.template.as_ref().unwrap().version, 1);
        assert!(!done.deferred);

        // Registered, then a later step refused: failed, never run again.
        let mut again = stored(ChangeKind::TemplateRegister, json!({"config": "x"}), "m", 1);
        again.status = ChangeStatus::Approved;
        again.template = Some(TemplateOutcome {
            id: "tpl".into(),
            version: 2,
        });
        save(&state, &again).await.unwrap();
        let failed = finish_refused(
            &state,
            &actor("alice"),
            again,
            ServeError::Unavailable("down".into()),
        )
        .await
        .unwrap();
        assert_eq!(failed.status, ChangeStatus::Failed);
        assert!(!failed.deferred);
        assert!(failed.error.unwrap().contains("version 2"));
    }

    #[cfg(feature = "notify")]
    #[tokio::test]
    async fn requested_notifications_skip_what_they_cannot_read() {
        let state = crate::serve::test_support::test_state();
        let mut c = stored(ChangeKind::TemplateLaunch, json!({"config": "x"}), "m", 1);
        notify_requested(&state, &c).await;
        c.kind = ChangeKind::Run;
        c.payload = json!({});
        notify_requested(&state, &c).await;
        c.payload = json!({"config": "{ nope"});
        notify_requested(&state, &c).await;
        c.payload = json!({"config": "notifications:\n  - name: x\n    on: [change_requested]\n    channel:\n      type: webhook\n      config:\n        url: \"\"\n"});
        notify_requested(&state, &c).await;
        c.payload = json!({"config": "name: p\n"});
        notify_requested(&state, &c).await;
    }

    #[test]
    fn store_errors_are_internal() {
        let e = store_err(HistoryError::Backend("down".into()));
        assert!(matches!(e, ServeError::Internal(_)));
        let e = store_err(HistoryError::Unreachable("refused".into()));
        assert!(matches!(e, ServeError::Unavailable(_)));
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
            trusted_config: false,
            caller_params: Default::default(),
        };
        for (kind, needle) in [
            (ChangeKind::TemplateRegister, "template_register payload"),
            (ChangeKind::TemplateLaunch, "template_launch payload"),
            (ChangeKind::TemplateDelete, "template_delete payload"),
            (ChangeKind::TemplateDeprecate, "template_deprecate payload"),
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
                json!({"id": "tpl", "config": crate::templates::bundle::smoke(&csv_yaml(dir.path(), "tpl", ""))}),
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

        let retire = |undo: bool| {
            new(
                ChangeKind::TemplateDeprecate,
                json!({"id": "tpl", "version": 1, "reason": "r", "undo": undo}),
            )
        };
        let dep = create(&state, &actor("bob"), retire(false)).await.unwrap();
        crate::templates::set_version_deprecated(&state.history(), "tpl", 1, None, None, true)
            .await
            .unwrap();
        let dep = approve(&state, &actor("alice"), &dep.id, None)
            .await
            .unwrap();
        assert_eq!(dep.status, ChangeStatus::Invalidated, "retired meanwhile");
        let revive = create(&state, &actor("bob"), retire(true)).await.unwrap();
        let revive = approve(&state, &actor("alice"), &revive.id, None)
            .await
            .unwrap();
        assert_eq!(revive.status, ChangeStatus::Executed, "{:?}", revive.error);
        assert_eq!(revive.template.as_ref().unwrap().version, 1);
        let st = state.history().template_state("tpl").await.unwrap();
        assert!(st.version_deprecation(1).is_none());
        let err = create(
            &state,
            &actor("bob"),
            new(
                ChangeKind::TemplateDeprecate,
                json!({"id": "tpl", "version": 9}),
            ),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ServeError::Unprocessable { .. }), "{err:?}");
        let gone = create(
            &state,
            &actor("bob"),
            new(
                ChangeKind::TemplateDelete,
                json!({"id": "tpl", "version": 1}),
            ),
        )
        .await
        .unwrap();
        assert_eq!(gone.plan.as_ref().unwrap().summary["delete"], 1);
        state.history().template_delete("tpl", None).await.unwrap();
        let gone = approve(&state, &actor("alice"), &gone.id, None)
            .await
            .unwrap();
        assert_eq!(gone.status, ChangeStatus::Failed, "the template is gone");

        let bare = create(
            &state,
            &actor("bob"),
            new(
                ChangeKind::TemplateRegister,
                json!({"id": "bare", "config": csv_yaml(dir.path(), "bare", "")}),
            ),
        )
        .await
        .unwrap();
        let bare = approve(&state, &actor("alice"), &bare.id, None)
            .await
            .unwrap();
        assert_eq!(bare.status, ChangeStatus::Executed, "{:?}", bare.error);
        let refused = create(
            &state,
            &actor("bob"),
            new(ChangeKind::TemplateLaunch, json!({"id": "bare"})),
        )
        .await
        .unwrap();
        let refused = approve(&state, &actor("alice"), &refused.id, None)
            .await
            .unwrap();
        assert_eq!(refused.status, ChangeStatus::Failed);
        assert!(
            refused.error.as_deref().unwrap_or("").contains("no_tests"),
            "{:?}",
            refused.error
        );

        for kind in [
            ChangeKind::TemplateRegister,
            ChangeKind::TemplateLaunch,
            ChangeKind::TemplateDelete,
            ChangeKind::TemplateDeprecate,
        ] {
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

    #[cfg(feature = "policy")]
    fn policy_version(
        scope: String,
        change_id: Option<String>,
    ) -> crate::serve::history::policies::PolicyVersionRecord {
        crate::serve::history::policies::PolicyVersionRecord {
            scope,
            version: 1,
            spec: json!({ "version": 1 }),
            created_at: Utc::now(),
            created_by: "bob".into(),
            approved_by: None,
            reason: None,
            change_id,
            restores: None,
        }
    }

    #[cfg(feature = "policy")]
    #[tokio::test]
    async fn a_policy_change_checks_the_approvers_tenant_and_finishes_an_applied_version() {
        use crate::serve::history::policies::tenant_scope;
        let state = crate::serve::test_support::test_state();
        let mut c = stored(
            ChangeKind::TenantPolicyUpdate,
            json!({ "tenant": "acme", "policy": "version: 1\n" }),
            "m",
            1,
        );
        c.tenant = Some("acme".into());
        save(&state, &c).await.unwrap();

        let mut globex = actor("gina");
        globex.tenant = Some("globex".into());
        assert!(matches!(
            approve(&state, &globex, &c.id, None).await,
            Err(ServeError::Forbidden(_))
        ));
        assert!(matches!(
            reject(&state, &globex, &c.id, "no".into()).await,
            Err(ServeError::Forbidden(_))
        ));

        // The version was stored before the execution was interrupted.
        let applied = policy_version(tenant_scope("acme"), Some(c.id.clone()));
        assert!(
            state
                .history()
                .policy_version_insert(&applied)
                .await
                .unwrap()
        );
        let done = approve(&state, &actor("alice"), &c.id, None).await.unwrap();
        assert_eq!(done.status, ChangeStatus::Executed);
        assert_eq!(done.plan.unwrap().summary["version"], 1);
    }

    #[cfg(feature = "policy")]
    #[tokio::test]
    async fn a_policy_change_that_lost_the_race_or_does_not_parse() {
        use crate::serve::history::policies::SERVER_SCOPE;
        let state = crate::serve::test_support::test_state();
        let garbled = stored(ChangeKind::PolicyUpdate, json!("not a payload"), "m", 1);
        assert_eq!(
            policy_update::already_applied(&state, &garbled)
                .await
                .unwrap(),
            None
        );
        let bad_draft = json!({ "policy": "version: 1\nclassifications:\n  - label: a\n    field_pattern: \"(\"\n" });
        assert!(matches!(
            policy_update::plan(&state, ChangeKind::PolicyUpdate, &bad_draft).await,
            Err(ServeError::Unprocessable { .. })
        ));

        // Another change stored the version this one planned to write.
        let c = stored(
            ChangeKind::PolicyUpdate,
            json!({ "policy": "version: 1\n" }),
            "m",
            1,
        );
        let first = policy_version(SERVER_SCOPE.into(), None);
        assert!(state.history().policy_version_insert(&first).await.unwrap());
        let fresh = ChangePlan {
            material: "m".into(),
            rows: Vec::new(),
            summary: json!({ "next_version": 1 }),
        };
        assert!(matches!(
            policy_update::execute(&state, &c, &fresh, &actor("alice")).await,
            Err(ServeError::Conflict(_))
        ));
    }
}
