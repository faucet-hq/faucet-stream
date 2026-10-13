//! The runtime operations of `faucet serve` (#887). The HTTP handlers, the
//! MCP tools, the triggers and change-request execution call these and
//! nothing below them: a config is loaded, trusted, gated, budgeted and
//! admitted here, in [`Stage`](super::Stage) order, and executed through
//! [`super::run`].

use super::{Origin, Principal};
use crate::config::PipelineConfig;
use crate::serve::error::ServeError;
use crate::serve::load::{ConfigFormat, LoadedSubmission};
use crate::serve::rbac::AuthContext;
use crate::serve::runner::{self, SubmitOutcome, SubmitRequest, SubmitResponse};
use crate::serve::state::ServerState;
use serde_json::Value;

// ── Stages ───────────────────────────────────────────────────────────────────

/// The config an operation loads.
#[derive(Debug, Clone, Copy)]
pub struct Submission<'a> {
    pub body: &'a str,
    pub format: ConfigFormat,
    /// Run only some of the config's rows (#741).
    pub selection: Option<&'a crate::select::SelectionRequest>,
}

impl<'a> Submission<'a> {
    /// The whole config `body`.
    pub fn new(body: &'a str, format: ConfigFormat) -> Self {
        Self {
            body,
            format,
            selection: None,
        }
    }

    /// Narrowed to `selection`'s rows.
    pub fn selected(mut self, selection: Option<&'a crate::select::SelectionRequest>) -> Self {
        self.selection = selection;
        self
    }
}

/// Which gates an operation passes after its config is loaded and trusted.
#[derive(Debug, Default, Clone)]
pub struct Gates {
    /// Refuse a config whose data-flow policy is violated (#702). Every
    /// operation that writes sets it.
    pub policy: bool,
    /// The caller's ceilings, merged into the config's own `budget:` (#703).
    pub budget: Option<faucet_core::BudgetSpec>,
    /// Admit the operation against its tenant's limits (#709); a no-op for
    /// a principal without a tenant.
    pub admit: bool,
}

impl Gates {
    /// The gates of an operation that writes: policy and tenant admission.
    pub fn writing() -> Self {
        Self {
            policy: true,
            budget: None,
            admit: true,
        }
    }
}

/// A tenant's admission guard: held until the operation is recorded.
#[cfg(feature = "tenants")]
pub type Admission = crate::serve::tenants::Admission;
/// No tenants in this build.
#[cfg(not(feature = "tenants"))]
pub type Admission = ();

/// A config past every gate.
pub struct Admitted {
    pub loaded: LoadedSubmission,
    /// The tenant's admission, while the caller records the operation.
    pub admission: Option<Admission>,
    /// The caller's and the tenant's ceilings (already merged into
    /// `loaded.cfg.budget`), for a body the caller stores.
    pub budget: Option<faucet_core::BudgetSpec>,
}

impl std::fmt::Debug for Admitted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Admitted")
            .field("rows", &self.loaded.nodes.len())
            .field("admitted", &self.admission.is_some())
            .field("budget", &self.budget)
            .finish()
    }
}

/// Load and trust a config for `principal`: parse it under the server's
/// default base and data-flow policy, screen it for the principal's origin,
/// bind its tenant scope and check its references (#881). No gate runs —
/// for a read (a plan, a status, a notification) only.
pub async fn load(
    state: &ServerState,
    principal: &Principal,
    sub: Submission<'_>,
) -> Result<LoadedSubmission, ServeError> {
    if sub.selection.is_some() && is_topology_body(sub.body, sub.format) {
        return Err(ServeError::BadConfig(
            crate::select::TOPOLOGY_REFUSAL.to_string(),
        ));
    }
    let scope = match principal.tenant() {
        None => None,
        Some(t) => Some(tenant_scope(state, t).await?),
    };
    let mut loaded = crate::serve::load::load_submission_with(
        sub.body,
        sub.format,
        state.default_base().as_ref(),
        server_policy(state).as_deref(),
        principal.body_origin(),
        scope,
        &state.references(),
    )
    .await?;
    if let Some(sel) = sub.selection {
        let nodes = std::mem::take(&mut loaded.nodes);
        let bad = |e: crate::error::CliError| {
            ServeError::BadConfig(crate::secrets::registry::redact(&e.to_string()).into_owned())
        };
        let auth = loaded.auth_catalog().map_err(bad)?;
        loaded.nodes = sel
            .apply_resolved(&loaded.cfg, nodes, &auth)
            .await
            .map_err(bad)?;
    }
    Ok(loaded)
}

/// Every stage up to execution: [`load`] (load + trust), then the policy
/// gate, the budget merge and tenant admission that `gates` names.
pub async fn admit(
    state: &ServerState,
    principal: &Principal,
    sub: Submission<'_>,
    gates: Gates,
) -> Result<Admitted, ServeError> {
    let mut loaded = load(state, principal, sub).await?;
    if gates.policy {
        policy_gate(state, &principal.actor(), &loaded).await?;
    }
    let mut budget = gates.budget;
    if let Some(b) = &budget {
        b.validate().map_err(ServeError::BadConfig)?;
    }
    let admission = match principal.tenant().filter(|_| gates.admit) {
        Some(t) => {
            let (admission, ceilings) = admit_tenant(state, t).await?;
            budget = match (budget, ceilings) {
                (Some(own), Some(t)) => Some(own.merge(&t)),
                (a, b) => a.or(b),
            };
            Some(admission)
        }
        None => None,
    };
    if let Some(extra) = &budget {
        merge_budget(&mut loaded.cfg, extra);
    }
    Ok(Admitted {
        loaded,
        admission,
        budget,
    })
}

/// The stricter of the config's `budget:` and `extra`.
fn merge_budget(cfg: &mut PipelineConfig, extra: &faucet_core::BudgetSpec) {
    let kinds = crate::budget::sink_template_kinds(cfg);
    cfg.budget = Some(match cfg.budget.take() {
        Some(own) => own.merge_with(extra, &|name: &str| kinds.get(name).cloned()),
        None => extra.clone(),
    });
}

/// Admit work for `tenant`: it exists, is not suspended and is under its
/// concurrency limit; its per-run ceilings are returned.
#[cfg(feature = "tenants")]
pub(crate) async fn admit_tenant(
    state: &ServerState,
    tenant: &str,
) -> Result<(Admission, Option<faucet_core::BudgetSpec>), ServeError> {
    crate::serve::tenants::admit_inline(state, tenant).await
}

#[cfg(not(feature = "tenants"))]
pub(crate) async fn admit_tenant(
    _state: &ServerState,
    tenant: &str,
) -> Result<(Admission, Option<faucet_core::BudgetSpec>), ServeError> {
    Err(ServeError::BadConfig(format!(
        "work for tenant '{tenant}' refused: this server was built without the `tenants` feature"
    )))
}

#[cfg(feature = "tenants")]
async fn tenant_scope(
    state: &ServerState,
    tenant: &str,
) -> Result<std::sync::Arc<crate::serve::load::TenantScope>, ServeError> {
    crate::serve::tenants::scope(state, tenant)
        .await
        .map(std::sync::Arc::new)
}

#[cfg(not(feature = "tenants"))]
async fn tenant_scope(
    _state: &ServerState,
    tenant: &str,
) -> Result<std::sync::Arc<crate::serve::load::TenantScope>, ServeError> {
    Err(ServeError::BadConfig(format!(
        "run for tenant '{tenant}' refused: this server was built without the `tenants` feature"
    )))
}

/// Whether a submitted body is a topology config (`pipeline.nodes`).
fn is_topology_body(body: &str, format: ConfigFormat) -> bool {
    let doc: Option<Value> = match format {
        ConfigFormat::Yaml => serde_yaml::from_str(body).ok(),
        ConfigFormat::Json => serde_json::from_str(body).ok(),
    };
    doc.and_then(|d| d.pointer("/pipeline/nodes").cloned())
        .is_some_and(|n| match n {
            Value::Object(o) => !o.is_empty(),
            Value::Array(a) => !a.is_empty(),
            _ => false,
        })
}

/// Data-flow policy gate (#702): refuse a config that would move a labelled
/// column somewhere its rules forbid, audited as `policy.denied`.
async fn policy_gate(
    state: &ServerState,
    actor: &AuthContext,
    loaded: &LoadedSubmission,
) -> Result<(), ServeError> {
    #[cfg(feature = "policy")]
    if let Some(spec) = loaded.cfg.policy.as_ref() {
        let report = if loaded.is_topology() {
            crate::policy::evaluate_topology(spec, &loaded.cfg)
        } else {
            crate::policy::evaluate_nodes(spec, &loaded.nodes, &Default::default())
        }
        .map_err(|e| ServeError::BadConfig(e.to_string()))?;
        if report.violated() {
            let merged = serde_json::to_value(&loaded.cfg).unwrap_or(Value::Null);
            let fp = crate::serve::idempotency::fingerprint(&merged, loaded.cfg.name.as_deref());
            crate::policy::record_metrics(loaded.cfg.name.as_deref().unwrap_or("serve"), &report);
            crate::serve::audit::write(state, actor, "policy.denied", None, Some(fp), "denied")
                .await;
            return Err(ServeError::Unprocessable {
                message: format!(
                    "policy: {} violation(s) — {}",
                    report.violations,
                    report
                        .all_violations()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join("; ")
                ),
                details: serde_json::to_value(&report).ok(),
            });
        }
    }
    #[cfg(not(feature = "policy"))]
    let _ = (state, actor, loaded);
    Ok(())
}

/// The server-wide data-flow policy handed to the loader (#702).
#[cfg(feature = "policy")]
fn server_policy(state: &ServerState) -> Option<std::sync::Arc<faucet_core::PolicySpec>> {
    state.policy()
}
#[cfg(not(feature = "policy"))]
fn server_policy(_state: &ServerState) -> Option<std::sync::Arc<()>> {
    None
}

/// The config a stored run re-executes (a cluster claim, a shard): its own
/// body, tenant and selection, trusted, with no gate — it passed them when
/// it was submitted.
pub async fn reload(
    state: &ServerState,
    rec: &crate::serve::history::RunRecord,
    body: &str,
) -> Result<LoadedSubmission, ServeError> {
    let principal = Principal::stored(AuthContext::runtime(), rec);
    load(
        state,
        &principal,
        Submission::new(body, rec.config_format.unwrap_or_default())
            .selected(rec.selection.as_ref()),
    )
    .await
}

// ── Governance ───────────────────────────────────────────────────────────────

/// What one execution of an admitted run needs beyond its config.
pub struct GovernedRun {
    pub run_id: String,
    pub auth: crate::auth_catalog::AuthCatalog,
    pub clock: chrono::DateTime<chrono::FixedOffset>,
    pub cancel: tokio_util::sync::CancellationToken,
    pub concurrency: Option<usize>,
    /// One shard of a source-sharded run (#230).
    pub shard: Option<faucet_core::ShardSpec>,
    #[cfg(feature = "notify")]
    pub notifier: Option<std::sync::Arc<crate::notify::Notifier>>,
    /// Record the run in the Data Movement Catalog (whole runs only).
    pub catalog: bool,
}

/// The governed execution options of a server run: its budget (the
/// config's merged with its tenant's limits), state namespace, resilience,
/// lineage, pricing and catalog — the governance stage. Every run and shard
/// the server executes takes its options from here.
pub fn governed_options(
    state: &ServerState,
    cfg: &PipelineConfig,
    tenant: Option<&crate::serve::load::TenantScope>,
    run: GovernedRun,
) -> Result<crate::executor::ExecuteOptions, String> {
    let resilience = match &cfg.resilience {
        Some(spec) => Some(spec.to_policy().map_err(|e| format!("resilience: {e}"))?),
        None => None,
    };
    #[cfg(feature = "lineage")]
    let lineage = crate::lineage_glue::build_emitter(cfg.lineage.as_ref())
        .map_err(|e| format!("lineage: {e}"))?;
    let usage = runner::usage_options(cfg).map_err(|e| format!("usage: {e}"))?;
    #[cfg(not(feature = "catalog"))]
    let _ = run.catalog;
    Ok(crate::executor::ExecuteOptions {
        legacy_state_writes: state.cluster().legacy_state_writes(),
        force_lease: false,
        pipeline_name: cfg.name.clone().unwrap_or_else(|| "serve".to_string()),
        // Correlates notifications to the submitted run (#480).
        run_id: Some(run.run_id.clone()),
        execution: cfg.execution.clone(),
        concurrency: run.concurrency,
        dry_run: false,
        limit: None,
        state_path_override: None,
        state_scope: tenant.map(|t| t.state_scope()).unwrap_or_default(),
        shard: run.shard,
        auth: run.auth,
        clock: run.clock,
        cancel: Some(run.cancel),
        resilience,
        sla: cfg.sla.clone(),
        reconcile: cfg.reconcile.clone(),
        verify: cfg.verify.clone(),
        rollback: cfg.rollback.clone(),
        #[cfg(feature = "lineage")]
        lineage,
        #[cfg(feature = "lineage")]
        lineage_cfg: cfg.lineage.clone(),
        #[cfg(feature = "notify")]
        notifier: run.notifier,
        #[cfg(feature = "catalog")]
        catalog: run.catalog.then(|| crate::catalog::CatalogHandle {
            store: state.history(),
            run_id: Some(run.run_id.clone()),
            sample_records: crate::catalog::DEFAULT_SAMPLE_RECORDS,
            annotations: Vec::new(),
            tenant: tenant.map(|t| t.values.id.clone()),
        }),
        usage,
        budget: run_budget(cfg, tenant),
    })
}

/// The per-run budget: the config's own `budget:` merged with the tenant's
/// limits (the stricter of each ceiling).
fn run_budget(
    cfg: &PipelineConfig,
    tenant: Option<&crate::serve::load::TenantScope>,
) -> Option<faucet_core::BudgetSpec> {
    match (cfg.budget.clone(), tenant.and_then(|t| t.budget.clone())) {
        (Some(a), Some(b)) => {
            let kinds = crate::budget::sink_template_kinds(cfg);
            Some(a.merge_with(&b, &|name: &str| kinds.get(name).cloned()))
        }
        (a, b) => a.or(b),
    }
}

// ── submit / run / cancel ────────────────────────────────────────────────────

/// How a submission meets `--require-approval run` (#703).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Approval {
    /// The server's requirement and the request's own `require_approval`.
    Required,
    /// A standing approval (a trigger declared in the server's triggers
    /// file): only the request's own `require_approval` asks for one.
    Standing,
}

/// Submit a run: approval first (a change request instead, when one is
/// required), then every stage, then the run is queued — or stored pending
/// for a cluster peer — and executed through [`super::run`].
pub async fn submit(
    state: &ServerState,
    principal: &Principal,
    mut req: SubmitRequest,
    approval: Approval,
) -> Result<SubmitOutcome, ServeError> {
    let gated = req.approved_change.is_none()
        && (req.require_approval
            || (approval == Approval::Required
                && state.requires_approval(crate::serve::changes::ChangeKind::Run)));
    if gated {
        let reason = req.reason.clone();
        let budget = req.budget.clone();
        let payload =
            serde_json::to_value(&req).map_err(|e| ServeError::Internal(e.to_string()))?;
        let change = crate::serve::changes::create(
            state,
            &principal.actor(),
            crate::serve::changes::NewChange {
                kind: crate::serve::changes::ChangeKind::Run,
                payload,
                reason,
                budget,
                trusted_config: req.trusted_config,
            },
        )
        .await?;
        return Ok(SubmitOutcome::PendingApproval(Box::new(change)));
    }
    // A draining server takes no new work (#789 SERVE-25).
    if state.is_draining() {
        return Err(ServeError::Draining);
    }
    // `0` is rejected rather than clamped: it would mean "no connections"
    // (#610).
    if req.concurrency == Some(0) {
        return Err(ServeError::Unprocessable {
            message: "concurrency must be greater than 0 — it is a connection/fetch count, not a `0 = unlimited` sentinel"
                .into(),
            details: None,
        });
    }
    // The selection names the run (#741): a label, so part of the
    // idempotency fingerprint.
    if let Some(sel) = &req.selection {
        req.labels
            .entry(crate::select::LABEL_SELECTION.to_string())
            .or_insert_with(|| sel.canonical());
    }
    let admitted = admit(
        state,
        principal,
        Submission::new(&req.config, req.config_format.into()).selected(req.selection.as_ref()),
        Gates {
            policy: true,
            budget: req.budget.take(),
            admit: true,
        },
    )
    .await?;
    // The stored body (what a cluster peer re-runs) carries the ceilings too.
    req.budget = admitted.budget.clone();
    let req = runner::apply_request_budget(req)?;
    runner::enqueue(state, principal, admitted, req)
        .await
        .map(SubmitOutcome::Accepted)
}

impl SubmitOutcome {
    /// The accepted run; an unexpected pending change is an internal error
    /// for a caller that never asks for approval.
    pub fn accepted(self) -> Result<SubmitResponse, ServeError> {
        match self {
            SubmitOutcome::Accepted(r) => Ok(r),
            SubmitOutcome::PendingApproval(c) => Err(ServeError::Internal(format!(
                "submission became change request {} where no approval was asked for",
                c.id
            ))),
        }
    }
}

/// What [`cancel`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelOutcome {
    /// A cancel was requested (or a pending run cancelled outright).
    Requested,
    /// The run had already finished, or is finishing: nothing to do.
    Finished,
}

/// Cancel a run (or a DLQ replay) the principal may see: its local token
/// when this instance runs it, else — in a cluster — the pending run itself
/// or a flag its owner picks up.
pub async fn cancel(
    state: &ServerState,
    principal: &Principal,
    id: &str,
) -> Result<CancelOutcome, ServeError> {
    let actor = principal.actor();
    if actor.tenant.is_some() {
        let rec = state
            .history()
            .get(id)
            .await
            .map_err(|e| ServeError::Internal(e.to_string()))?
            .ok_or(ServeError::NotFound)?;
        if !actor.sees_tenant(rec.tenant.as_deref()) {
            return Err(ServeError::NotFound);
        }
    }
    if state.registry().cancel(id) {
        crate::serve::audit::write(
            state,
            &actor,
            "run.cancel",
            Some(id.to_string()),
            None,
            "ok",
        )
        .await;
        return Ok(CancelOutcome::Requested);
    }
    let rec = state
        .history()
        .get(id)
        .await
        .map_err(|e| ServeError::Internal(e.to_string()))?
        .ok_or(ServeError::NotFound)?;
    if rec.status.is_terminal() {
        return Ok(CancelOutcome::Finished);
    }
    if state.cluster().enabled() {
        if state
            .history()
            .cancel_pending(id)
            .await
            .map_err(|e| ServeError::Internal(e.to_string()))?
        {
            crate::serve::audit::write(
                state,
                &actor,
                "run.cancel",
                Some(id.to_string()),
                None,
                "ok",
            )
            .await;
            // A terminal transition that bypasses `runner::finalize`, so it
            // fires the completion callback itself (#481).
            match state.history().get(id).await {
                Ok(Some(rec)) => crate::serve::callback::fire(&rec).await,
                Ok(None) => {}
                Err(e) => tracing::warn!(
                    run_id = %id,
                    error = %e,
                    "could not read cancelled run for its completion callback"
                ),
            }
            return Ok(CancelOutcome::Requested);
        }
        // Running on a peer: flag it; the peer cancels on its next tick.
        state
            .history()
            .request_cancel(id)
            .await
            .map_err(|e| ServeError::Internal(e.to_string()))?;
        if rec.status == crate::serve::history::RunStatus::Sharded {
            runner::maybe_finalize_parent(state, id).await;
        }
        crate::serve::audit::write(
            state,
            &actor,
            "run.cancel",
            Some(id.to_string()),
            None,
            "ok",
        )
        .await;
        return Ok(CancelOutcome::Requested);
    }
    // Single instance, no local token: the run is finishing or just finished.
    Ok(CancelOutcome::Finished)
}

// ── Read operations ──────────────────────────────────────────────────────────

fn cli_to_serve(e: crate::error::CliError) -> ServeError {
    match e {
        crate::error::CliError::Config(m) => ServeError::BadConfig(m),
        other => ServeError::Internal(other.to_string()),
    }
}

/// Plan one row of a config (#283 / #707): load and trust it, then plan the
/// row — nothing is written, so no gate runs. Audited as `plan`.
pub async fn plan(
    state: &ServerState,
    principal: &Principal,
    req: crate::serve::handlers::plan::PlanRequest,
) -> Result<Value, ServeError> {
    let loaded = load(
        state,
        principal,
        Submission::new(&req.config, req.config_format.into()),
    )
    .await?;
    loaded.require_matrix()?;
    let auth = loaded.auth_catalog().map_err(cli_to_serve)?;
    let node = crate::commands::plan::select_root(&loaded.nodes, req.row.as_deref())
        .map_err(cli_to_serve)?;
    let sample = req.sample.map(|records| {
        let label = format!("request ({} record(s))", records.len());
        (records, label)
    });
    #[cfg(feature = "catalog")]
    let history = state.history();
    #[cfg(feature = "catalog")]
    let impact = req.impact.then(|| crate::commands::plan::ImpactOptions {
        store: history.as_ref(),
        // The name the executor records a serve run's catalog edges under.
        pipeline: loaded
            .cfg
            .name
            .clone()
            .unwrap_or_else(|| "serve".to_string()),
        depth: req.depth.unwrap_or(crate::impact::DEFAULT_DEPTH),
    });
    #[cfg(not(feature = "catalog"))]
    if req.impact {
        return Err(ServeError::Unprocessable {
            message: "impact analysis requires a server built with the `catalog` feature".into(),
            details: None,
        });
    }
    let report = super::local::plan(
        principal,
        &loaded.cfg,
        node,
        &auth,
        crate::commands::plan::PlanOptions {
            sample,
            #[cfg(feature = "catalog")]
            impact,
            #[cfg(not(feature = "catalog"))]
            _marker: std::marker::PhantomData,
        },
    )
    .await
    .map_err(cli_to_serve)?;
    let value = serde_json::to_value(&report).map_err(|e| ServeError::Internal(e.to_string()))?;
    crate::serve::audit::write(state, &principal.actor(), "plan", None, None, "ok").await;
    Ok(value)
}

/// Validate and probe a config without running it: 200 with the report when
/// every probe passes, 422 with it as `details` when one fails.
pub async fn doctor(
    state: &ServerState,
    principal: &Principal,
    sub: Submission<'_>,
) -> Result<Value, ServeError> {
    let loaded = load(state, principal, sub).await?;
    runner::run_doctor_first(state, &loaded).await
}

// ── Writing operations ───────────────────────────────────────────────────────

/// Verify a config's destination against its source by content (#701); with
/// `repair` (and not `dry_run`), every writing gate runs first and the
/// repair writes through the sink. Audited as `verify`.
pub async fn verify(
    state: &ServerState,
    principal: &Principal,
    req: crate::serve::handlers::verify::VerifyRequest,
) -> Result<Value, ServeError> {
    let writes = req.repair && !req.dry_run;
    if writes {
        runner::refuse_unapproved_write(state, "verify repair")?;
    }
    let admitted = admit(
        state,
        principal,
        Submission::new(&req.config, req.config_format.into()),
        if writes {
            Gates::writing()
        } else {
            Gates::default()
        },
    )
    .await?;
    let loaded = &admitted.loaded;
    let _permit = runner::inline_permit(state)?;
    let auth = loaded.auth_catalog().map_err(cli_to_serve)?;
    let pipeline_name = loaded
        .cfg
        .name
        .clone()
        .unwrap_or_else(|| "verify".to_string());
    let mut spec = loaded.cfg.verify.clone().unwrap_or_default();
    if let Some(n) = req.max_differences {
        spec.max_differences = n;
    }
    let outcome = super::local::verify(
        principal,
        &loaded.cfg,
        &spec,
        crate::verify::VerifyInputs {
            row: req.row,
            repair: req.repair,
            allow_delete: req.allow_delete,
            dry_run: req.dry_run,
            pipeline_name,
            execution: loaded.cfg.execution.clone(),
            auth,
            clock: chrono::Utc::now().fixed_offset(),
        },
    )
    .await
    .map_err(cli_to_serve)?;
    crate::serve::audit::write(
        state,
        &principal.actor(),
        "verify",
        None,
        None,
        if outcome.report.equal() {
            "equal"
        } else {
            "different"
        },
    )
    .await;
    Ok(serde_json::to_value(outcome).unwrap_or(Value::Null))
}

/// Roll back one invocation of a finished run (#706), loaded as it was
/// executed (its tenant, its stored config unless the request carries one).
/// A real rollback passes every writing gate. Audited as `run.rollback`.
pub async fn rollback(
    state: &ServerState,
    principal: &Principal,
    id: &str,
    req: crate::serve::handlers::runs::RollbackRequest,
) -> Result<crate::rollback::RollbackReport, ServeError> {
    let rec = state
        .history()
        .get(id)
        .await
        .map_err(|e| ServeError::Internal(e.to_string()))?
        .ok_or(ServeError::NotFound)?;
    if !rec.status.is_terminal() {
        return Err(ServeError::Conflict(format!(
            "run {id} is {} — a run can only be rolled back once it has finished",
            rec.status.as_str()
        )));
    }
    let known: Vec<String> = rec
        .invocations
        .iter()
        .filter_map(|i| i.run_id.clone())
        .collect();
    let invocation_id = match req.invocation_id {
        Some(inv) => {
            if !known.is_empty() && !known.contains(&inv) {
                return Err(ServeError::BadConfig(format!(
                    "invocation_id {inv} is not one of run {id}'s invocations ({})",
                    known.join(", ")
                )));
            }
            inv
        }
        None if known.len() == 1 => known[0].clone(),
        None if known.is_empty() => {
            return Err(ServeError::Unprocessable {
                message: format!(
                    "run {id} recorded no invocation ids (it predates rollback support, or \
                     no pipeline ran); pass `invocation_id` explicitly"
                ),
                details: None,
            });
        }
        None => {
            return Err(ServeError::BadConfig(format!(
                "run {id} has {} invocations — pass `invocation_id` (one of: {})",
                known.len(),
                known.join(", ")
            )));
        }
    };
    // The caller's config, else the stored one (cluster mode), loaded under
    // the run's tenant (#789 SERVE-27).
    let actor = principal.actor();
    let (config, format, run_as) = match (req.config, rec.config_body.as_deref()) {
        (Some(c), _) => (
            c,
            req.config_format.into(),
            Principal::caller(state, actor).on_behalf_of(rec.tenant.clone()),
        ),
        (None, Some(stored)) => (
            stored.to_string(),
            rec.config_format.unwrap_or_default(),
            Principal::stored(actor, &rec),
        ),
        (None, None) => {
            return Err(ServeError::Unprocessable {
                message: format!(
                    "run {id}'s config is not stored on this server; pass `config` (the \
                     pipeline config the run was made with) in the request body"
                ),
                details: None,
            });
        }
    };
    if !req.dry_run {
        runner::refuse_unapproved_write(state, "run rollback")?;
    }
    let admitted = admit(
        state,
        &run_as,
        Submission::new(&config, format),
        if req.dry_run {
            Gates::default()
        } else {
            Gates::writing()
        },
    )
    .await?;
    let loaded = &admitted.loaded;
    loaded.require_matrix()?;
    let _permit = runner::inline_permit(state)?;
    let auth = loaded
        .auth_catalog()
        .map_err(|e| ServeError::BadConfig(e.to_string()))?;
    let pipeline_name = loaded
        .tenant
        .as_ref()
        .map(|t| t.state_scope())
        .unwrap_or_default()
        .prefix(loaded.cfg.name.as_deref().unwrap_or("serve"));
    let report = super::local::rollback_nodes(
        &run_as,
        &loaded.nodes,
        &crate::rollback::RollbackInputs {
            run_id: invocation_id,
            row: req.row,
            dry_run: req.dry_run,
            force: req.force,
            pipeline_name,
            auth,
        },
    )
    .await
    .map_err(cli_to_serve)?;
    let result = if req.dry_run {
        "dry_run"
    } else if report.outcome.applied {
        "ok"
    } else {
        "blocked"
    };
    crate::serve::audit::write(
        state,
        &principal.actor(),
        "run.rollback",
        Some(id.to_string()),
        None,
        result,
    )
    .await;
    Ok(report)
}

/// Replay DLQ records through a config's sink (#281). A real replay passes
/// every writing gate, runs under its tenant's ceilings, is recorded as a
/// run while it runs (so it holds a tenant slot and can be cancelled) and
/// is audited as `dlq.replay`. `tenant` is the request's on-behalf-of
/// tenant; a scoped principal may only name its own.
pub async fn replay_dlq(
    state: &ServerState,
    principal: &Principal,
    req: crate::serve::handlers::dlq::DlqReplayRequest,
) -> Result<Value, ServeError> {
    use crate::dlq_replay::reader::DlqDecryptor;
    use crate::serve::load::refuse_request_path;
    let principal = match (principal.actor().tenant, &req.tenant) {
        (Some(own), Some(asked)) if &own != asked => return Err(ServeError::NotFound),
        _ => principal.clone().on_behalf_of(req.tenant.clone()),
    };
    let tenant = principal.tenant().map(str::to_string);
    refuse_request_path("from", &req.from, false, tenant.as_deref())?;
    if let Some(failed) = &req.failed_dlq {
        refuse_request_path("failed_dlq", failed, true, tenant.as_deref())?;
    }
    if !req.dry_run {
        runner::refuse_unapproved_write(state, "DLQ replay")?;
    }
    // A tenant's replay is admitted even when it is a dry run (#871): it
    // reads the tenant's connections either way.
    let admitted = admit(
        state,
        &principal,
        Submission::new(&req.config, req.config_format.into()),
        Gates {
            policy: !req.dry_run,
            budget: None,
            admit: true,
        },
    )
    .await?;
    let Admitted {
        loaded,
        admission,
        budget,
    } = admitted;
    let _permit = runner::inline_permit(state)?;
    let auth = loaded.auth_catalog().map_err(cli_to_serve)?;
    let pipeline_name = loaded
        .cfg
        .name
        .clone()
        .unwrap_or_else(|| "dlq-replay".to_string());
    let id = req
        .id
        .clone()
        .unwrap_or_else(|| format!("dlq-replay-{}", uuid::Uuid::now_v7()));
    let cancel = state.shutdown_token().child_token();
    let Some(claim) = state.registry().try_register(id.clone(), cancel.clone()) else {
        return Err(ServeError::Conflict(format!(
            "a run or replay with id '{id}' is already in flight"
        )));
    };
    let _token = replay::Token {
        state,
        id: id.clone(),
        claim,
    };
    let decryptor = DlqDecryptor::from_keys(&req.encryption_keys)
        .map_err(crate::error::CliError::from)
        .map_err(cli_to_serve)?;
    let started = if req.dry_run {
        drop(admission);
        None
    } else {
        Some(replay::start(state, &id, &req.from, tenant.as_deref(), admission).await?)
    };
    let outcome = super::local::replay_dlq(
        &principal,
        &loaded.cfg,
        &req.from,
        crate::dlq_replay::ReplayInputs {
            reason: req.reason.as_deref(),
            failed_dlq: req.failed_dlq.as_deref(),
            row: req.row.as_deref(),
            dry_run: req.dry_run,
            pipeline_name,
            execution: loaded.cfg.execution.clone(),
            auth,
            clock: chrono::Utc::now().fixed_offset(),
            decryptor,
            budget,
            cancel: Some(cancel.clone()),
        },
    )
    .await;
    if let Some(rec) = started {
        rec.finish(&outcome, cancel.is_cancelled()).await;
    }
    let result_label = if outcome.is_ok() { "ok" } else { "error" };
    crate::serve::audit::write(
        state,
        &principal.actor(),
        "dlq.replay",
        None,
        None,
        result_label,
    )
    .await;
    let outcome = outcome.map_err(cli_to_serve)?;
    let mut body = serde_json::to_value(outcome).unwrap_or(Value::Null);
    if let Some(obj) = body.as_object_mut() {
        obj.insert("id".into(), Value::String(id));
        if let Some(t) = tenant {
            obj.insert("tenant".into(), Value::String(t));
        }
    }
    Ok(body)
}

/// The run record of a DLQ replay while it runs.
pub(crate) mod replay {
    use super::Admission;
    use crate::error::CliError;
    use crate::serve::error::ServeError;
    use crate::serve::history::{RunRecord, RunStatus};
    use crate::serve::state::ServerState;
    use chrono::Utc;

    /// Removes a replay's cancel token from the registry when it ends.
    pub(crate) struct Token<'a> {
        pub state: &'a ServerState,
        pub id: String,
        pub claim: u64,
    }

    impl Drop for Token<'_> {
        fn drop(&mut self) {
            self.state.registry().release_claim(&self.id, self.claim);
        }
    }

    /// Record a replay as a running run under its id, owned by `tenant`,
    /// then release the admission. In a cluster the tenant's concurrency is
    /// re-counted across instances and an over-limit replay is refused.
    pub(crate) async fn start(
        state: &ServerState,
        id: &str,
        from: &str,
        tenant: Option<&str>,
        admission: Option<Admission>,
    ) -> Result<Record, ServeError> {
        let history = state.history();
        let now = Utc::now();
        let mut labels = std::collections::BTreeMap::new();
        labels.insert(
            crate::serve::handlers::dlq::LABEL_DLQ_REPLAY.to_string(),
            "true".to_string(),
        );
        let mut rec = RunRecord::queued(id.to_string(), None, labels, None, now);
        rec.status = RunStatus::Running;
        rec.started_at = Some(now);
        rec.replay_of = Some(from.to_string());
        rec.tenant = tenant.map(str::to_string);
        if !history
            .insert_new(&rec)
            .await
            .map_err(|e| ServeError::from_history("run history", e))?
        {
            return Err(ServeError::Conflict(format!(
                "a run with id '{id}' already exists"
            )));
        }
        let guard = Record {
            state: state.clone(),
            rec: Some(rec),
        };
        if let Some(t) = tenant {
            history
                .tenant_run_link(id, t)
                .await
                .map_err(|e| ServeError::Internal(format!("recording the replay's tenant: {e}")))?;
        }
        drop(admission);
        #[cfg(feature = "tenants")]
        if state.cluster().enabled()
            && let Some(t) = tenant
            && let Some(msg) = crate::serve::tenants::over_limit(state, t).await?
        {
            return Err(ServeError::TooManyRequests(msg));
        }
        Ok(guard)
    }

    /// A replay's run record. Finished explicitly; dropped unfinished (a
    /// refused or abandoned request), it is closed as cancelled so it never
    /// holds a tenant slot.
    pub(crate) struct Record {
        state: ServerState,
        rec: Option<RunRecord>,
    }

    impl Record {
        /// Write the replay's terminal state.
        pub(crate) async fn finish(
            mut self,
            outcome: &Result<crate::dlq_replay::ReplayOutcome, CliError>,
            cancelled: bool,
        ) {
            let Some(mut rec) = self.rec.take() else {
                return;
            };
            rec.status = match (outcome, cancelled) {
                (_, true) => RunStatus::Cancelled,
                (Ok(_), false) => RunStatus::Completed,
                (Err(_), false) => RunStatus::Failed,
            };
            match outcome {
                Ok(o) => rec.records_written = o.records_written as u64,
                Err(e) => {
                    rec.error = Some(crate::secrets::registry::redact_message(&e.to_string()))
                }
            }
            close(&self.state, rec).await;
        }
    }

    impl Drop for Record {
        fn drop(&mut self) {
            let Some(mut rec) = self.rec.take() else {
                return;
            };
            rec.status = RunStatus::Cancelled;
            rec.error = Some("the replay request ended before the replay finished".into());
            let state = self.state.clone();
            if let Ok(rt) = tokio::runtime::Handle::try_current() {
                rt.spawn(async move { close(&state, rec).await });
            }
        }
    }

    /// Stamp `rec`'s finish time and store it.
    async fn close(state: &ServerState, mut rec: RunRecord) {
        let now = Utc::now();
        rec.finished_at = Some(now);
        rec.elapsed_secs = rec
            .started_at
            .map(|s| (now - s).num_milliseconds().max(0) as f64 / 1000.0);
        if let Err(e) = state.history().upsert(&rec).await {
            tracing::warn!(run_id = %rec.run_id, error = %e, "could not record a replay's outcome");
        }
    }
}

// ── trigger_template ─────────────────────────────────────────────────────────

#[cfg(feature = "templates")]
pub use templates::{TriggerOutcome, trigger_template};

#[cfg(feature = "templates")]
mod templates {
    use super::{Approval, Origin, Principal};
    use crate::params::SuppliedParams;
    use crate::serve::error::ServeError;
    use crate::serve::handlers::templates::{
        LABEL_ALLOW_UNTESTED, LABEL_OVERLAY, LABEL_OVERLAY_VERSION, LABEL_SINK_TEMPLATE,
        LABEL_SINK_TEMPLATE_VERSION, LABEL_TEMPLATE, LABEL_TEMPLATE_VERSION, RESERVED_LABELS,
        TriggerBody, TriggerResponse, map_err,
    };
    use crate::serve::runner::{ConfigFormatWire, SubmitOutcome, SubmitRequest};
    use crate::serve::state::ServerState;

    /// What a trigger produced: a run, or (under `--require-approval run` / an
    /// explicit `require_approval`) a pending change request (#703).
    #[derive(Debug)]
    #[allow(clippy::large_enum_variant)]
    pub enum TriggerOutcome {
        Run(TriggerResponse),
        PendingApproval(Box<crate::serve::changes::ChangeRequest>),
    }

    /// Run a registered template (#444): resolve the version, materialize it
    /// with the caller's params (composing a sink template, applying an
    /// overlay), label the run with its provenance and [`super::submit`] it.
    pub async fn trigger_template(
        state: ServerState,
        principal: Principal,
        id: String,
        mut body: TriggerBody,
    ) -> Result<TriggerOutcome, ServeError> {
        let actor = principal.actor();
        let tenant = principal.tenant().map(str::to_string);
        if tenant.is_some() && !body.env.is_empty() {
            return Err(ServeError::Forbidden(
                "a tenant-scoped principal may not override a template's environment (`env`)"
                    .into(),
            ));
        }
        // A run for a tenant never runs a version whose tests are not passing
        // (#856): `allow_untested` is an operator's escape hatch.
        if tenant.is_some() && body.allow_untested {
            crate::serve::audit::write_target_detail(
                &state,
                &actor,
                "template.run",
                format!("template:{id}"),
                "allow_untested refused for a tenant".into(),
                "denied",
            )
            .await;
            return Err(ServeError::Forbidden(
                "`allow_untested` is never allowed for a tenant-scoped principal or a run made for a \
                 tenant — run a version that holds a label"
                    .into(),
            ));
        }
        let overlay = body.overlay_choice();
        let supplied: SuppliedParams = std::mem::take(&mut body.params).into_iter().collect();
        // An unpinned request means `stable` — the launched version.
        let s = state.history();
        let want = crate::templates::resolve_version(&s, &id, body.version.unwrap_or_default())
            .await
            .map_err(map_err)?;
        // A deprecated template still runs, loudly.
        let tstate = crate::templates::template_state(&s, &id)
            .await
            .map_err(map_err)?;
        let deprecated = crate::templates::deprecation_warning(&tstate, want);
        if let Some(reason) = &deprecated {
            tracing::warn!(
                template = %id,
                version = want,
                reason = %reason,
                "triggering a DEPRECATED pipeline template"
            );
        }
        // A clustered or gated submit persists the materialized config, so
        // nothing secret may be baked into it: `${env:}` / `${file:}` /
        // `${secret:}` stay as tokens for the executing instance (#456 C5).
        let clustered = state.cluster().enabled();
        let gated = body.require_approval
            || (!body.standing_approval
                && state.requires_approval(crate::serve::changes::ChangeKind::Run));
        let mode = if clustered || gated {
            crate::templates::Materialize::Persisted
        } else {
            crate::templates::Materialize::Local
        };
        let sink = crate::templates::SinkChoice {
            id: body.sink.clone(),
            version: body.sink_version.unwrap_or_default(),
            overlay,
            allow_untested: body.allow_untested,
        };
        if let Some(sink_id) = &sink.id {
            let sink_state = crate::templates::template_state(&s, sink_id)
                .await
                .map_err(map_err)?;
            if sink_state.status == crate::serve::history::templates::TemplateStatus::Deprecated {
                tracing::warn!(sink_template = %sink_id, "composing a DEPRECATED sink template");
            }
        }
        let materialized = crate::templates::materialize_for_run_selected(
            &s,
            &id,
            want,
            &sink,
            &supplied,
            &body.env,
            mode,
            body.selection.as_ref(),
        )
        .await
        .map_err(map_err)?;

        // Caller-supplied values that would land in the persisted body (a
        // `secret: true` param, an `env:` override) are refused (#456 M4).
        if (clustered || gated) && (materialized.used_secret_params || !body.env.is_empty()) {
            let what = if materialized.used_secret_params {
                "declares `secret: true` param(s)"
            } else {
                "was triggered with `env` overrides"
            };
            let why = if gated {
                "this trigger needs an approved change request, which stores the materialized \
                 config for the approvers to review — and would store the value with it"
            } else {
                "a clustered server persists the materialized config so a peer can execute it — \
                 which would store the value in the shared run-history database"
            };
            let alternative = if gated {
                "trigger it on a server that does not require approval for runs"
            } else {
                "trigger it on a non-clustered server"
            };
            return Err(ServeError::Unprocessable {
                message: format!(
                    "this template {what}, and {why}. Reference the secret from the template body \
                     instead (`${{env:VAR}}`, `${{vault:…}}`, `${{aws-sm:…}}`, … — all resolved on \
                     the executing instance, never persisted), or {alternative}"
                ),
                details: None,
            });
        }

        let mut labels = body.labels;
        // Provenance labels are the server's to set.
        labels.retain(|k, _| !RESERVED_LABELS.contains(&k.as_str()));
        labels.insert(LABEL_TEMPLATE.into(), materialized.template_id.clone());
        if let Some(status) = materialized.untested {
            labels.insert(LABEL_ALLOW_UNTESTED.into(), "true".into());
            crate::serve::audit::write_target_detail(
                &state,
                &actor,
                "template.run",
                format!(
                    "template:{}@{}",
                    materialized.template_id, materialized.version
                ),
                format!("allow_untested: v{} is {status}", materialized.version),
                "ok",
            )
            .await;
        }
        labels.insert(
            LABEL_TEMPLATE_VERSION.into(),
            materialized.version.to_string(),
        );
        if let (Some(sid), Some(sv)) = (&materialized.sink_id, materialized.sink_version) {
            labels.insert(LABEL_SINK_TEMPLATE.into(), sid.clone());
            labels.insert(LABEL_SINK_TEMPLATE_VERSION.into(), sv.to_string());
        }
        if let Some(oid) = &materialized.overlay_id {
            labels.insert(LABEL_OVERLAY.into(), oid.clone());
            if let Some(ov) = materialized.overlay_version {
                labels.insert(LABEL_OVERLAY_VERSION.into(), ov.to_string());
            }
        }
        // The caller's selection names the run (#741).
        if let Some(sel) = &body.selection {
            labels.insert(crate::select::LABEL_SELECTION.into(), sel.canonical());
        }

        let req = SubmitRequest {
            config: materialized.body.clone(),
            config_format: ConfigFormatWire::Json,
            name: body.name.or_else(|| materialized.name.clone()),
            labels,
            timeout_secs: body.timeout_secs,
            doctor_first: body.doctor_first,
            idempotency_key: body.idempotency_key,
            clock: body.clock,
            concurrency: body.concurrency,
            callback: body.callback,
            require_approval: body.require_approval,
            reason: body.reason,
            budget: body.budget,
            approved_change: None,
            selection: materialized.selection.clone(),
            trusted_config: true,
            server_file: false,
        };
        // The template is the operator's: the run loads it trusted.
        let run_as = Principal::new(actor.clone(), Origin::Trusted).on_behalf_of(tenant);
        let approval = if body.standing_approval {
            Approval::Standing
        } else {
            Approval::Required
        };
        let run = match super::submit(&state, &run_as, req, approval).await? {
            SubmitOutcome::Accepted(run) => run,
            SubmitOutcome::PendingApproval(change) => {
                return Ok(TriggerOutcome::PendingApproval(change));
            }
        };
        // `submit` recorded `run.submit`; this entry attributes the trigger.
        crate::serve::audit::write(
            &state,
            &actor,
            "template.run",
            Some(run.run_id.clone()),
            None,
            "ok",
        )
        .await;
        Ok(TriggerOutcome::Run(TriggerResponse {
            run,
            template_id: materialized.template_id,
            template_version: materialized.version,
            sink_template: materialized.sink_id,
            sink_template_version: materialized.sink_version,
            streams: materialized.streams,
            overlay: materialized.overlay_id,
            overlay_version: materialized.overlay_version,
            overlay_contributes: materialized.overlay_contributes,
            warnings: materialized.warnings,
            params: materialized.params_redacted,
            deprecated,
            selection: body.selection,
        }))
    }
}
