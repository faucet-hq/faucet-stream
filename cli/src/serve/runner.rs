//! The run lifecycle: validate + queue a submission (`submit`), then run it under
//! a permit. A cancel / timeout / shutdown trigger cooperatively cancels the
//! pipeline (so a buffered sink flushes at its next page boundary, #146 H16) and
//! grants a bounded flush grace before hard-dropping it; the task then finalizes
//! an authoritative terminal status. See spec §7 + §20.

use crate::executor::RunSummary;
use crate::registry::build_source;
use crate::serve::error::ServeError;
use crate::serve::history::{Claim, InvocationRecord, RunRecord, RunStatus};
use crate::serve::history::{ClaimedShard, ShardInsert, ShardOutcome};
use crate::serve::load::{BodyOrigin, ConfigFormat, LoadedSubmission};
use crate::serve::rbac::AuthContext;
use crate::serve::state::ServerState;
use crate::serve::{idempotency, metrics};
use chrono::{DateTime, FixedOffset, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

/// `Retry-After` advertised when the queue is full.
const QUEUE_FULL_RETRY_AFTER_SECS: u64 = 5;

/// Grace granted to a cancelled / timed-out / shutting-down run to flush
/// buffered sink output cooperatively before its future is hard-dropped (which
/// aborts the pipeline's task set, the backstop for a run stuck mid-write so a
/// hung run can't wedge shutdown). Generous enough for an S3 multipart
/// completion (#146 H16).
pub(crate) const RUN_FLUSH_GRACE: Duration = Duration::from_secs(30);

/// `POST /v1/runs` request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitRequest {
    pub config: String,
    #[serde(default)]
    pub config_format: ConfigFormatWire,
    pub name: Option<String>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    pub timeout_secs: Option<u64>,
    #[serde(default)]
    pub doctor_first: bool,
    pub idempotency_key: Option<String>,
    /// Override this run's **connector** concurrency (#610): how many
    /// concurrent connections/fetches the source and sink may use, whatever
    /// the config says.
    ///
    /// The multi-tenant knob — the same template driving a customer with beefy
    /// read replicas and one with a small instance, without per-customer
    /// copies of the config or the template author having to pre-declare a
    /// `${param.*}` for it. Mapped onto whichever knob the connector declares
    /// (`max_connections` / `partition_concurrency` / `shard_concurrency` /
    /// `concurrency`); a connector with none ignores it. Does **not** change
    /// matrix parallelism or the server's own `--max-concurrent` slots, and it
    /// caps only the *client* side — it cannot exceed what the upstream will
    /// actually accept. Must be > 0. Per-shard for a sharded run.
    #[serde(default)]
    pub concurrency: Option<usize>,
    pub clock: Option<String>,
    /// Optional completion callback fired when this run reaches a terminal
    /// state (#481). Validated at submit time so a bad destination is a 422 on
    /// the submission rather than a silent no-op when the run finishes.
    #[serde(default)]
    pub callback: Option<crate::serve::callback::CallbackSpec>,
    /// Ask for an approved change request (#703) instead of a run: the
    /// response is the pending request, and the run starts once the
    /// approvers the server's policy names approve it. Implied by
    /// `--require-approval run`.
    #[serde(default)]
    pub require_approval: bool,
    /// Why, for the approvers (with `require_approval`).
    #[serde(default)]
    pub reason: Option<String>,
    /// Run budget (#703): ceilings merged into the config's own `budget:`
    /// block (the stricter of each wins), so an approved change never exceeds
    /// what was agreed.
    #[serde(default)]
    pub budget: Option<faucet_core::BudgetSpec>,
    /// Set in-process by an executing change request, never accepted from a
    /// client (`skip`): lets the run through the approval gate.
    #[serde(skip)]
    pub approved_change: Option<String>,
    /// Run only some of the config's matrix rows (#741) — the CLI's
    /// `--select` / `--only` / `--skip` / `--tag` / `--status` /
    /// `--include-parents`. Stored with the run so a clustered, claimed or
    /// sharded run applies the same subset. Omitted: every row runs.
    #[serde(default)]
    pub selection: Option<crate::select::SelectionRequest>,
    /// Set in-process when the server's operator wrote the config (a
    /// registered template, the triggers file), never accepted from a client.
    #[serde(skip)]
    pub trusted_config: bool,
    /// Set in-process when the config text came from a file on the server
    /// (the triggers file), never accepted from a client: its SQL connectors
    /// keep host access (#862 H-04). Implies `trusted_config`.
    #[serde(skip)]
    pub server_file: bool,
}

impl SubmitRequest {
    /// How the server loads this request's config.
    fn origin(&self, state: &ServerState) -> BodyOrigin {
        if self.server_file {
            BodyOrigin::ServerFile
        } else {
            state.origin(self.trusted_config)
        }
    }
}

/// How a stored run's config is re-loaded on claim / resume / rollback.
pub fn stored_origin(rec: &RunRecord) -> BodyOrigin {
    if rec.host_access {
        BodyOrigin::ServerFile
    } else {
        BodyOrigin::Trusted
    }
}

/// What a gated submission produced (#703).
#[derive(Debug)]
pub enum SubmitOutcome {
    /// The run was accepted (queued / pending in a cluster).
    Accepted(SubmitResponse),
    /// The server (or the request) wants approval first: this is the pending
    /// change request; nothing ran.
    PendingApproval(Box<crate::serve::changes::ChangeRequest>),
}

/// The JSON body `POST /v1/runs` / a template trigger answer with when the
/// submission became a change request instead of a run.
pub fn pending_approval_body(change: &crate::serve::changes::ChangeRequest) -> serde_json::Value {
    serde_json::json!({
        "status": "pending_approval",
        "change_id": change.id,
        "change": change,
    })
}

/// [`submit`] behind the approval gate (#703): the runtime's `submit`
/// (`crate::runtime::server::submit`) for `actor` with a config the request
/// carried (or the operator's, when `trusted_config`).
pub async fn submit_gated(
    state: ServerState,
    req: SubmitRequest,
    actor: AuthContext,
) -> Result<SubmitOutcome, ServeError> {
    let principal = submit_principal(&state, &req, actor);
    crate::runtime::server::submit(
        &state,
        &principal,
        req,
        crate::runtime::server::Approval::Required,
    )
    .await
}

/// The principal a submission runs as: its config's origin decides how it is
/// screened.
fn submit_principal(
    state: &ServerState,
    req: &SubmitRequest,
    actor: AuthContext,
) -> crate::runtime::Principal {
    crate::runtime::Principal::new(actor, crate::runtime::Origin::from(req.origin(state)))
}

/// The 409 message of an idempotency key reused with a different payload.
pub(crate) const IDEMPOTENCY_CONFLICT: &str = "idempotency key reused with a different payload";

/// Refuse a write that would bypass `--require-approval run` (#789
/// SERVE-18): verify repair, DLQ replay and run rollback write to a sink
/// outside the run queue, and no change-request kind expresses them, so under
/// approval they are disabled (a dry run still works).
pub(crate) fn refuse_unapproved_write(state: &ServerState, what: &str) -> Result<(), ServeError> {
    if state.requires_approval(crate::serve::changes::ChangeKind::Run) {
        return Err(ServeError::Forbidden(format!(
            "this server requires an approved change request for runs (--require-approval run); \
             {what} writes to the destination outside the run queue and cannot be proposed as a \
             change request, so it is disabled on this server — use dry_run to see what it \
             would do, or run it from the CLI or a server without --require-approval run"
        )));
    }
    Ok(())
}

/// A run slot for work that executes inline in the request (verify, DLQ
/// replay), so it counts against `--max-concurrent-runs` like a queued run.
pub(crate) fn inline_permit(
    state: &ServerState,
) -> Result<tokio::sync::OwnedSemaphorePermit, ServeError> {
    state.semaphore().try_acquire_owned().map_err(|_| {
        ServeError::TooManyRequests(
            "every run slot is busy (--max-concurrent-runs); retry when a run finishes".into(),
        )
    })
}

/// Fold a request-level budget (#703) into the config document, so the
/// stored body (what a cluster peer re-runs) carries it too. The config's own
/// `budget:` and the request's merge, the stricter of each ceiling winning.
pub(crate) fn apply_request_budget(mut req: SubmitRequest) -> Result<SubmitRequest, ServeError> {
    let Some(extra) = req.budget.take() else {
        return Ok(req);
    };
    extra.validate().map_err(ServeError::BadConfig)?;
    let mut doc: serde_json::Value = match req.config_format {
        ConfigFormatWire::Yaml => serde_yaml::from_str(&req.config)
            .map_err(|e| ServeError::BadConfig(format!("invalid YAML: {e}")))?,
        ConfigFormatWire::Json => serde_json::from_str(&req.config)
            .map_err(|e| ServeError::BadConfig(format!("invalid JSON: {e}")))?,
    };
    let kinds = crate::budget::sink_template_kinds_in_doc(&doc);
    let Some(obj) = doc.as_object_mut() else {
        return Err(ServeError::BadConfig("config must be a mapping".into()));
    };
    let merged = match obj.get("budget") {
        Some(existing) => {
            let own: faucet_core::BudgetSpec = serde_json::from_value(existing.clone())
                .map_err(|e| ServeError::BadConfig(format!("budget: {e}")))?;
            own.merge_with(&extra, &|name: &str| kinds.get(name).cloned())
        }
        None => extra,
    };
    obj.insert(
        "budget".to_string(),
        serde_json::to_value(&merged).map_err(|e| ServeError::Internal(e.to_string()))?,
    );
    req.config = serde_json::to_string(&doc).map_err(|e| ServeError::Internal(e.to_string()))?;
    req.config_format = ConfigFormatWire::Json;
    Ok(req)
}

/// Resolve a submitted config's `usage:` pricing (#704). A submitted config
/// has no filesystem: `pricing_file` is refused (it would read a file on the
/// server), inline `pricing` overrides apply.
pub fn usage_options(
    cfg: &crate::config::PipelineConfig,
) -> Result<crate::usage::UsageOptions, String> {
    if cfg.usage.as_ref().is_some_and(|u| u.pricing_file.is_some()) {
        return Err(
            "usage.pricing_file is not allowed in a submitted config (it would read a file \
             on the server); set the rates inline under usage.pricing"
                .to_string(),
        );
    }
    crate::usage::UsageOptions::from_spec(cfg.usage.as_ref(), None)
}

/// Run label naming the pipeline (`cfg.name`, else `serve`) a run belongs to.
pub const LABEL_PIPELINE: &str = "pipeline";

/// Wire enum mirroring `load::ConfigFormat` with serde rename.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConfigFormatWire {
    #[default]
    Yaml,
    Json,
}

impl From<ConfigFormatWire> for ConfigFormat {
    fn from(w: ConfigFormatWire) -> Self {
        match w {
            ConfigFormatWire::Yaml => ConfigFormat::Yaml,
            ConfigFormatWire::Json => ConfigFormat::Json,
        }
    }
}

/// `POST /v1/runs` success body (202).
#[derive(Debug, Serialize)]
pub struct SubmitResponse {
    pub run_id: String,
    pub status: RunStatus,
    pub submitted_at: DateTime<Utc>,
}

/// Re-run a claimed Pending run on this instance (cluster mode). Reconstructs the
/// execution inputs from the persisted record (re-resolving config with this
/// instance's own env/credentials), acquires a permit, and runs the shared tail.
pub fn resume_claimed_run(state: ServerState, rec: RunRecord) {
    resume_claimed(state, rec, None);
}

/// [`resume_claimed_run`] with the run slot the claim loop already took, so a
/// claimed run never waits on (or is over-claimed past) the semaphore
/// (#789 SERVE-46).
pub fn resume_claimed_run_with_permit(
    state: ServerState,
    rec: RunRecord,
    permit: tokio::sync::OwnedSemaphorePermit,
) {
    resume_claimed(state, rec, Some(permit));
}

fn resume_claimed(
    state: ServerState,
    rec: RunRecord,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
) {
    // In flight from the moment it is claimed, so a shutdown drain waits for
    // it while it is still loading its config (#789 SERVE-48).
    state.registry().mark_running_unqueued();
    metrics::set_run_gauges(&state);
    let guard = InFlightGuard {
        state: state.clone(),
        run_id: rec.run_id.clone(),
    };
    tokio::spawn(async move {
        let guard = guard;
        let run_id = rec.run_id.clone();
        let Some(body) = rec.config_body.as_deref() else {
            tracing::error!(run_id, "claimed run has no stored config; failing it");
            finalize(
                &state,
                &run_id,
                rec.submitted_at,
                Terminal::Failed {
                    reason: "claimed run record missing config_body".into(),
                    records: 0,
                    invs: Vec::new(),
                },
            )
            .await;
            return;
        };
        let loaded = match crate::runtime::server::reload(&state, &rec, body).await {
            Ok(l) => l,
            Err(e) => {
                finalize(
                    &state,
                    &run_id,
                    rec.submitted_at,
                    Terminal::Failed {
                        reason: format!(
                            "re-loading claimed config: {}",
                            e.api_error().error.message
                        ),
                        records: 0,
                        invs: Vec::new(),
                    },
                )
                .await;
                return;
            }
        };

        // Mode B (#230): a sharded run is expanded into shard rows here — the
        // claiming instance acts as the (ephemeral) coordinator — and is NOT
        // executed as a whole. Enumeration + insert is idempotent, so two
        // instances both claiming + coordinating converge on the same shard set.
        if let Some(sh) = loaded.cfg.shard.clone()
            && sh.count >= 2
        {
            match coordinate_sharded_run(&state, &run_id, &loaded, sh.count).await {
                Ok(true) => return, // expanded into shards — shard loop runs them
                Ok(false) => {}     // not shardable → fall through, run the whole run
                Err(e) => {
                    finalize(
                        &state,
                        &run_id,
                        rec.submitted_at,
                        Terminal::Failed {
                            reason: format!("sharding: {e}"),
                            records: 0,
                            invs: Vec::new(),
                        },
                    )
                    .await;
                    return;
                }
            }
        }

        let _permit = match permit {
            Some(p) => p,
            None => state
                .semaphore()
                .acquire_owned()
                .await
                .expect("semaphore not closed"),
        };
        // Register a local cancel token so a cross-instance cancel (the claim loop
        // calling registry.cancel) reaches this run.
        let run_token = CancellationToken::new();
        state.registry().register(run_id.clone(), run_token.clone());
        let principal = crate::runtime::Principal::stored(AuthContext::runtime(), &rec);
        crate::runtime::under(
            &principal.clone(),
            execute_run(
                state.clone(),
                principal,
                loaded,
                run_id,
                run_token,
                rec.submitted_at,
                rec.timeout_secs,
                rec.clock.clone(),
                rec.concurrency,
                Some(guard),
            ),
        )
        .await;
    });
}

/// Coordinator step (Mode B): expand a sharded run into `faucet_serve_shards`
/// rows. Returns `Ok(true)` when the run was sharded (caller must not execute it
/// as a whole), `Ok(false)` when it isn't shardable (caller runs it whole).
///
/// Idempotent: enumeration is deterministic and the insert is
/// `ON CONFLICT DO NOTHING`, so a re-coordinated run (e.g. after the coordinator
/// crashed and the Pending run was requeued) converges on the same shard set.
async fn coordinate_sharded_run(
    state: &ServerState,
    run_id: &str,
    loaded: &LoadedSubmission,
    count: usize,
) -> crate::error::CliResult<bool> {
    use crate::error::CliError;

    // Sharding applies to a single-source pipeline; a matrix fan-out is not
    // shardable (each row is already an independent unit — use Mode A).
    if loaded.nodes.len() != 1 {
        tracing::warn!(
            run_id,
            nodes = loaded.nodes.len(),
            "shard requested but the run is not a single-node pipeline; running it whole"
        );
        return Ok(false);
    }
    let node = &loaded.nodes[0];
    let auth = loaded
        .auth_catalog()
        .map_err(|e| CliError::Internal(format!("auth catalog: {e}")))?;
    let source = build_source(&node.source.kind, node.source.config.clone(), &auth, None).await?;
    if !source.is_shardable() {
        tracing::warn!(
            run_id,
            kind = %node.source.kind,
            "source is not shardable; running the run whole"
        );
        return Ok(false);
    }

    // Mark the parent run Sharded BEFORE inserting any shard rows (F27).
    // Orphan recovery (`recover_orphans` / `reclaim_orphans`) only reclaims
    // `running` runs; once the parent is `Sharded` it is immune to a false
    // fail. Doing this first guarantees the invariant that **no shard row ever
    // exists while the parent is still `running`** — otherwise a coordinator
    // crash between `insert_shards` and the status flip would let orphan
    // recovery mark the run Failed while its already-inserted shards are
    // claimed, execute, and write data (a run the user believes failed and may
    // resubmit → duplicate writes). If the flip itself fails, the run stays
    // `running` + owned by us with no shards inserted, so a crash here is
    // safely *requeued* by reclaim, not falsely failed. (A crash after the flip
    // but before `insert_shards` leaves a 0-shard `Sharded` run, which
    // `all_terminal()` never finalizes — an availability leak, not a
    // correctness/duplication bug; the desired trade.)
    {
        let mut r = state
            .history()
            .get(run_id)
            .await
            .map_err(|e| CliError::Internal(e.to_string()))?
            .ok_or_else(|| CliError::Internal(format!("run {run_id} vanished before sharding")))?;
        r.status = RunStatus::Sharded;
        state
            .history()
            .upsert(&r)
            .await
            .map_err(|e| CliError::Internal(e.to_string()))?;
    }

    let shards = source
        .enumerate_shards(count)
        .await
        .map_err(|e| CliError::Internal(format!("enumerate_shards: {e}")))?;
    let inserts: Vec<ShardInsert> = shards
        .iter()
        .map(|s| ShardInsert {
            shard_id: s.id.clone(),
            descriptor: s.descriptor.clone(),
            size_estimate: s.size_estimate,
        })
        .collect();
    let inserted = state
        .history()
        .insert_shards(run_id, &inserts)
        .await
        .map_err(|e| CliError::Internal(e.to_string()))?;
    tracing::info!(
        run_id,
        shards = inserts.len(),
        inserted,
        "expanded run into shards (Mode B)"
    );

    // Wake the local claim loop so it picks up the freshly-inserted shards.
    state.cluster().kick();
    Ok(true)
}

/// Execute one claimed shard (Mode B): rebuild + narrow the source to the shard,
/// run it under a permit, owner-fenced-finalize the shard, then finalize the
/// parent run once every shard is terminal.
pub fn resume_claimed_shard(state: ServerState, claimed: ClaimedShard) {
    tokio::spawn(async move {
        let ClaimedShard {
            run_id,
            shard_id,
            descriptor,
            run,
        } = claimed;

        let Some(body) = run.config_body.clone() else {
            tracing::error!(run_id, shard_id, "claimed shard's run has no stored config");
            let _ = state
                .history()
                .finalize_shard(&run_id, &shard_id, ShardOutcome::Failed)
                .await;
            maybe_finalize_parent(&state, &run_id).await;
            return;
        };
        let loaded = match crate::runtime::server::reload(&state, &run, &body).await {
            Ok(l) => l,
            Err(e) => {
                tracing::error!(
                    run_id,
                    shard_id,
                    error = %e.api_error().error.message,
                    "re-loading shard config failed"
                );
                let _ = state
                    .history()
                    .finalize_shard(&run_id, &shard_id, ShardOutcome::Failed)
                    .await;
                maybe_finalize_parent(&state, &run_id).await;
                return;
            }
        };

        let _permit = state
            .semaphore()
            .acquire_owned()
            .await
            .expect("semaphore not closed");

        let shard = faucet_core::ShardSpec {
            id: shard_id.clone(),
            descriptor,
            size_estimate: None,
        };
        // Register a per-shard cooperative-cancel token BEFORE running so a
        // cross-instance cancel reaches this shard: `cancel_run` flags the parent
        // → `pending_shard_cancellations` → the claim loop calls
        // `registry().cancel_run_shards(run_id)`, which fires this token. Removed
        // on return so a terminated shard never leaks a token (shard accounting is
        // separate from the run's `in_flight`, so a plain `deregister` is used).
        let coop = CancellationToken::new();
        state
            .registry()
            .register_shard(&run_id, &shard_id, coop.clone());
        // Count the shard in `in_flight` so the shutdown drain waits for it and
        // fires the cooperative-flush cancel (audit #321 H5). The guard releases
        // the slot + token on EVERY return path (including panic), so a shard can
        // never leak the counter and wedge graceful shutdown.
        state.registry().mark_shard_running();
        let _shard_guard = ShardInFlightGuard {
            state: state.clone(),
            run_id: run_id.clone(),
            shard_id: shard_id.clone(),
        };
        let principal = crate::runtime::Principal::stored(AuthContext::runtime(), &run);
        let (outcome, shutdown) = crate::runtime::under(
            &principal,
            execute_shard(
                &state,
                &principal,
                loaded,
                &run_id,
                &shard_id,
                shard,
                coop,
                run.timeout_secs,
                run.clock.clone(),
                run.concurrency,
                run.submitted_at,
            ),
        )
        .await;

        // A clustered shutdown hands the shard back to the cluster instead of
        // failing it, as it does a whole run (#789 SERVE-25): a peer resumes it
        // from its per-shard bookmark.
        if shutdown && state.cluster().enabled() {
            match state.history().release_shard(&run_id, &shard_id).await {
                Ok(true) => {
                    tracing::info!(
                        run_id,
                        shard_id,
                        "shutdown: shard handed back to the cluster as pending"
                    );
                    drop(_shard_guard);
                    if !state.registry().has_run_shards(&run_id) {
                        state.log_hub().finish(&run_id);
                        schedule_log_drop(state.clone(), run_id.clone());
                    }
                    return;
                }
                Ok(false) => {}
                Err(e) => tracing::warn!(
                    run_id,
                    shard_id,
                    error = %e,
                    "shutdown: could not requeue the shard"
                ),
            }
        }

        match state
            .history()
            .finalize_shard(&run_id, &shard_id, outcome)
            .await
        {
            Ok(true) => {}
            Ok(false) => tracing::warn!(
                run_id,
                shard_id,
                "shard was reclaimed by another instance; discarding result"
            ),
            Err(e) => tracing::error!(run_id, shard_id, error = %e, "finalize_shard failed"),
        }
        maybe_finalize_parent(&state, &run_id).await;
        // The last of this run's shards on this instance closes its log
        // buffer: SSE readers get `End`, the tail is persisted and the
        // buffer is dropped after the drain window (#789 SERVE-35).
        drop(_shard_guard);
        if !state.registry().has_run_shards(&run_id) {
            state.log_hub().finish(&run_id);
            schedule_log_drop(state.clone(), run_id.clone());
        }
    });
}

/// Run one shard's pipeline (single node, source narrowed via `opts.shard`).
/// Does not touch the parent run record — the caller finalizes the shard and
/// the parent. The flag is `true` when a server shutdown interrupted it.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::disallowed_methods)] // a runtime timer, not a connector backend call
async fn execute_shard(
    state: &ServerState,
    principal: &crate::runtime::Principal,
    loaded: LoadedSubmission,
    run_id: &str,
    shard_id: &str,
    shard: faucet_core::ShardSpec,
    coop: CancellationToken,
    timeout_secs: Option<u64>,
    clock_flag: Option<String>,
    concurrency: Option<usize>,
    submitted_at: DateTime<Utc>,
) -> (ShardOutcome, bool) {
    let auth = match loaded.auth_catalog() {
        Ok(a) => a,
        Err(e) => {
            tracing::error!(run_id, shard_id, "shard auth catalog: {e}");
            return (ShardOutcome::Failed, false);
        }
    };
    let LoadedSubmission { cfg, nodes, tenant } = loaded;
    let clock = match resolve_clock(clock_flag.as_deref(), submitted_at) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(
                run_id,
                shard_id,
                "shard clock: {}",
                e.api_error().error.message
            );
            return (ShardOutcome::Failed, false);
        }
    };
    // `coop` is the per-shard cancel token registered by `resume_claimed_shard`;
    // a cross-instance cancel (F10), a server shutdown, or a timeout all fire it
    // so the shard's pipeline flushes at its next page boundary. A shard is not
    // recorded in the catalog: its records and URIs are a slice of the row's.
    let opts = match crate::runtime::server::governed_options(
        state,
        &cfg,
        tenant.as_deref(),
        crate::runtime::server::GovernedRun {
            run_id: run_id.to_string(),
            auth,
            clock,
            cancel: coop.clone(),
            concurrency,
            shard: Some(shard),
            #[cfg(feature = "notify")]
            notifier: None,
            catalog: false,
        },
    ) {
        Ok(o) => o,
        Err(e) => {
            tracing::error!(run_id, shard_id, "shard {e}");
            return (ShardOutcome::Failed, false);
        }
    };

    let server_shutdown = state.shutdown_token();
    let span = tracing::info_span!(parent: None, "faucet.serve.shard", serve_run_id = %run_id, shard = %shard_id);
    let audit_state = state.clone();
    let audit_run_id = run_id.to_string();
    let principal = principal.clone();
    let work = async move {
        let result = crate::runtime::run(&principal, nodes, opts).await;
        audit_runtime_policy_denials(&audit_state, &audit_run_id, &result).await;
        classify_run(result)
    }
    .instrument(span);
    tokio::pin!(work);
    let timeout_fut = async {
        match timeout_secs {
            Some(s) => tokio::time::sleep(Duration::from_secs(s)).await,
            None => std::future::pending::<()>().await,
        }
    };
    tokio::pin!(timeout_fut);

    // Cancel triggers (shutdown / timeout) cooperatively cancel + flush, like
    // execute_run. A failed shard simply returns false → its lease eventually
    // reassigns it (or it poisons after max_attempts).
    // A cross-instance cancel (F10) flows in via the shard's registered coop
    // token: `cancel_run` flags the parent → `pending_cancellations` →
    // `claim_loop` fires `registry().cancel(run_id)`. The token is registered
    // under the run id by `resume_claimed_shard` before this runs.
    let terminal = tokio::select! {
        biased;
        t = &mut work => t,
        _ = coop.cancelled() => {
            // Fired by the claim loop for a remote cancel. Flush within the grace
            // window; a cooperative cancel returns Ok(partial), so the shard is
            // Cancelled — but a flush that FAILS must surface, not be masked.
            Terminal::cancelled()
                .after_interrupt(tokio::time::timeout(RUN_FLUSH_GRACE, &mut work).await.ok())
        }
        _ = server_shutdown.cancelled() => {
            coop.cancel();
            Terminal::shutdown_failed()
                .after_interrupt(tokio::time::timeout(RUN_FLUSH_GRACE, &mut work).await.ok())
        }
        _ = &mut timeout_fut => {
            coop.cancel();
            Terminal::timeout(timeout_secs.unwrap_or(0))
                .after_interrupt(tokio::time::timeout(RUN_FLUSH_GRACE, &mut work).await.ok())
        }
    };
    let shutdown = matches!(terminal, Terminal::ShutdownFailed { .. });
    let outcome = match terminal {
        Terminal::Completed { .. } => ShardOutcome::Completed,
        Terminal::Cancelled { .. } => ShardOutcome::Cancelled,
        _ => ShardOutcome::Failed,
    };
    (outcome, shutdown)
}

/// Finalize a `Sharded` parent run once all its shards are terminal. The last
/// shard to finish always observes `all_terminal` (its own `finalize_shard`
/// committed first), so the run never lingers `Sharded`. A benign double-finalize
/// (two shards finishing simultaneously) writes the same terminal status twice.
pub(crate) async fn maybe_finalize_parent(state: &ServerState, run_id: &str) {
    let progress = match state.history().shard_progress(run_id).await {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(run_id, error = %e, "shard_progress failed");
            return;
        }
    };
    if !progress.all_terminal() {
        return;
    }
    let (status, error) = progress.parent_outcome();
    // Status-fenced finalize: transitions the parent only while it is still
    // `Sharded`, and does NOT re-stamp owner/lease on the terminal record, so a
    // near-simultaneous double-finalize from two instances has a single winner
    // (F45). The loser (and any non-`Sharded` parent) is a no-op.
    match state
        .history()
        .finalize_sharded_parent(run_id, status, Utc::now(), error)
        .await
    {
        Ok(true) => {
            metrics::record_run_finished(status, status.finished_reason());
            tracing::info!(
                run_id,
                shards = progress.total,
                failed = progress.failed,
                "sharded run finalized"
            );
            // Fire the parent's completion callback exactly once — `Ok(true)` is
            // the status-fenced winner, so a near-simultaneous double-finalize
            // from two instances delivers a single callback (#481). Re-read the
            // record: `finalize_sharded_parent` takes fields, not the record, so
            // this is the only way to get the persisted terminal state.
            match state.history().get(run_id).await {
                Ok(Some(rec)) => crate::serve::callback::fire(&rec).await,
                Ok(None) => tracing::warn!(
                    run_id,
                    "sharded parent vanished before its callback could fire"
                ),
                Err(e) => tracing::warn!(
                    run_id,
                    error = %e,
                    "could not read sharded parent for its completion callback"
                ),
            }
        }
        Ok(false) => {} // already finalized by another shard/instance — nothing to do
        Err(e) => {
            tracing::error!(run_id, error = %e, "finalizing sharded parent run failed");
        }
    }
}

/// Warn at submit when a clustered or source-sharded run is at-least-once with
/// a non-idempotent destination (F26/F39). Cluster failover and shard reclaim
/// are at-least-once by construction: an owner whose lease lapses while it is
/// still alive (slow, paused), or a reassigned shard whose source re-reads from
/// the start (postgres PK-range sharding yields no resumable bookmark), can
/// re-execute work and write **duplicate** rows to an append-mode sink. The
/// safe configuration is `write_mode: upsert`/`delete` (keyed, so re-writes are
/// idempotent) or `delivery: exactly_once`. We surface the risk loudly rather
/// than silently duplicating — the repo's #1 worst bug class.
/// Pure decision for [`warn_if_cluster_at_least_once`]: the sink kinds that
/// would write non-idempotently under cluster failover / shard reclaim. Empty
/// when the run is neither clustered nor sharded, is `exactly_once`, or every
/// sink is keyed (`upsert`/`delete`).
fn at_least_once_risky_sinks(
    loaded: &LoadedSubmission,
    clustered: bool,
    sharded: bool,
) -> Vec<&str> {
    if !(clustered || sharded) || loaded.cfg.delivery == faucet_core::DeliveryMode::ExactlyOnce {
        return Vec::new();
    }
    loaded
        .nodes
        .iter()
        .filter(|n| {
            !matches!(
                n.sink
                    .config
                    .get("write_mode")
                    .and_then(|v| v.as_str())
                    .unwrap_or("append"),
                "upsert" | "delete"
            )
        })
        .map(|n| n.sink.kind.as_str())
        .collect()
}

fn warn_if_cluster_at_least_once(loaded: &LoadedSubmission, clustered: bool, sharded: bool) {
    let risky = at_least_once_risky_sinks(loaded, clustered, sharded);
    if !risky.is_empty() {
        let scope = if sharded {
            "source-sharded (Mode B)"
        } else {
            "clustered"
        };
        tracing::warn!(
            sinks = ?risky,
            "{scope} execution is at-least-once: a failover or shard reclaim can re-run work \
             and write duplicate rows to an append-mode sink. Set `write_mode: upsert` (or \
             `delivery: exactly_once`) on the destination to make re-execution idempotent (F26/F39)."
        );
    }
}

/// Best-effort release of an idempotency claim whose paired run-record upsert
/// failed (F21). No-op when the request carried no key; the release itself is
/// scoped by `run_id` (a newer run that re-claimed the key keeps its claim).
async fn release_orphaned_claim(state: &ServerState, req: &SubmitRequest, run_id: &str) {
    if req.idempotency_key.is_some()
        && let Err(e) = state.history().release_idempotency(run_id).await
    {
        tracing::warn!(
            run_id,
            error = %e,
            "failed to release idempotency claim after a run-record write error; \
             a replay of the key may 404 until the claim self-expires"
        );
    }
}

/// Submit a run without the approval gate: the runtime's `submit`
/// (`crate::runtime::server::submit`) under a standing approval, for `actor`.
pub async fn submit(
    state: ServerState,
    req: SubmitRequest,
    actor: AuthContext,
) -> Result<SubmitResponse, ServeError> {
    let principal = submit_principal(&state, &req, actor);
    crate::runtime::server::submit(
        &state,
        &principal,
        req,
        crate::runtime::server::Approval::Standing,
    )
    .await?
    .accepted()
}

/// The execution stage of an admitted submission: validate what only a
/// queued run needs (sharding, the callback), reserve a queue slot, run the
/// `doctor_first` preflight, claim the idempotency key, record the run and
/// spawn it — or store it pending for a cluster peer. Called only by the
/// runtime, after every gate.
pub(crate) async fn enqueue(
    state: &ServerState,
    principal: &crate::runtime::Principal,
    admitted: crate::runtime::server::Admitted,
    req: SubmitRequest,
) -> Result<SubmitResponse, ServeError> {
    let state = state.clone();
    let actor = principal.actor();
    let crate::runtime::server::Admitted {
        loaded,
        admission: _admission,
        ..
    } = admitted;

    // At-least-once duplicate-write warning for clustered / source-sharded runs
    // with an append-mode destination (F26/F39).
    let sharded = loaded.cfg.shard.as_ref().is_some_and(|s| s.count >= 2);
    if sharded && state.cluster().enabled() {
        crate::expand::check_sharding(&loaded.cfg, &loaded.nodes).map_err(|e| {
            ServeError::Unprocessable {
                message: e.to_string(),
                details: None,
            }
        })?;
    }
    warn_if_cluster_at_least_once(&loaded, state.cluster().enabled(), sharded);

    // Completion-callback validation (#481) — before the queue reservation, so a
    // malformed or refused destination costs nothing and surfaces as a 422 on
    // the submission rather than as a silent no-op when the run finishes.
    if let Some(cb) = req.callback.as_ref() {
        cb.validate_for(state.callback_allow_hosts(), principal.tenant().is_some())
            .map_err(|message| ServeError::Unprocessable {
                message,
                details: None,
            })?;
        cb.reject_secrets_in_cluster(state.cluster().enabled())
            .map_err(|message| ServeError::Unprocessable {
                message,
                details: None,
            })?;
    }

    // Reserve a queue slot first, so a Fresh idempotency claim is always followed
    // by a spawned run (no orphaned claims — spec §20.2).
    if !state.registry().try_reserve() {
        return Err(ServeError::QueueFull {
            retry_after_secs: QUEUE_FULL_RETRY_AFTER_SECS,
        });
    }
    // Releases the reservation on ANY early return below (doctor_first 422 /
    // replay / conflict / claim or upsert error). Defused just before spawn.
    let reservation = ReservationGuard::new(state.clone());

    // doctor_first preflight — run BEHIND the reservation so concurrent preflight
    // probing is bounded by `max_queued_runs` rather than running unthrottled
    // before any limit applies (#146 R). On failure the guard releases the slot
    // via the early `?`. The (redacted) report is stored on the run record below
    // so `GET /v1/runs/{id}` exposes it (#146 R: doctor_report was never set).
    let doctor_report = if req.doctor_first {
        Some(run_doctor_first(&state, &loaded).await?)
    } else {
        None
    };

    let run_id = uuid::Uuid::now_v7().to_string();

    // Config fingerprint (sha256) — the idempotency identity AND the value
    // recorded on the `run.submit` audit entry (#205).
    let merged = serde_json::to_value(&loaded.cfg).unwrap_or(serde_json::Value::Null);
    let fp_config = idempotency::fingerprint(&merged, loaded.cfg.name.as_deref());

    // Idempotency claim (if a key was supplied).
    if let Some(key) = &req.idempotency_key {
        // Fold the run-affecting request fields (clock / timeout_secs / labels)
        // into the fingerprint, not just the config — so a key replayed with a
        // different backfill `clock` is a 409, not a replay of the original
        // run's window (#146 M7).
        let fp = idempotency::request_fingerprint(
            &fp_config,
            req.clock.as_deref(),
            req.timeout_secs,
            req.concurrency,
            &req.labels,
        );
        let key = idempotency_scope(principal.tenant(), key);
        match state
            .history()
            .claim_idempotency(&key, &fp, &run_id, state.idempotency_retention())
            .await
            .map_err(|e| match e {
                // Degraded backend can't safely honor idempotency → 503, retry.
                crate::serve::history::HistoryError::Degraded(m) => ServeError::Unavailable(m),
                other => ServeError::Internal(other.to_string()),
            })? {
            Claim::Fresh => {}
            Claim::Replay(existing) => {
                metrics::record_idempotency_hit();
                return replay_response(&state, &actor, &existing).await;
            }
            Claim::Conflict => {
                return Err(ServeError::Conflict(IDEMPOTENCY_CONFLICT.into()));
            }
        }
        // A `Fresh` claim is recorded BEFORE the record upsert below. The memory
        // backend's `upsert` is infallible, but a SQL backend's can fail — so if
        // the upsert fails, `release_orphaned_claim` drops the claim (F21) rather
        // than leaving a replay 404-ing until the claim self-expires.
    }

    let submitted_at = Utc::now();
    // The pipeline a run belongs to, whatever the run is named — what
    // `faucet status` / `/v1/state` look runs up by (#732, #735).
    let mut labels = req.labels.clone();
    labels.entry(LABEL_PIPELINE.to_string()).or_insert_with(|| {
        loaded
            .cfg
            .name
            .clone()
            .unwrap_or_else(|| "serve".to_string())
    });
    let mut rec = RunRecord::queued(
        run_id.clone(),
        req.name.clone(),
        labels,
        req.idempotency_key.clone(),
        submitted_at,
    );
    rec.doctor_report = doctor_report;
    rec.callback = req.callback.clone();
    rec.tenant = principal.tenant().map(str::to_string);
    rec.selection = req.selection.clone();
    rec.host_access = req.server_file;
    rec.caller_config = principal.caller_body();

    if state.cluster().enabled() {
        // A degraded (DB-unreachable) backend can't coordinate a cluster: the
        // claim loop's claim_pending is a no-op on the in-memory fallback, so a
        // Pending run would never be claimed. Fail closed with a retryable 503
        // rather than silently orphaning the run (#197 spec §9).
        if state.history().degraded() {
            return Err(ServeError::Unavailable(
                "clustered run-history backend is degraded; runs cannot be claimed \
                 by any instance — retry once it recovers"
                    .into(),
            ));
        }
        // Cluster mode: persist the RAW config so any instance can re-resolve +
        // run it, mark the run Pending, and wake the local claim loop. No local
        // queue slot / spawn — the claim loop owns execution.
        rec.status = RunStatus::Pending;
        rec.config_body = Some(req.config.clone());
        rec.config_format = Some(req.config_format.into());
        rec.timeout_secs = req.timeout_secs;
        rec.clock = req.clock.clone();
        rec.concurrency = req.concurrency;
        // The tenant link goes first (#789 SERVE-29): once the record exists
        // a peer may claim and run it, so nothing may fail after that.
        if let Err(e) = link_tenant_run(&state, &rec).await {
            release_orphaned_claim(&state, &req, &run_id).await;
            return Err(e);
        }
        if let Err(e) = state.history().upsert(&rec).await {
            // The record write that should follow a `Fresh` claim failed — release
            // the orphaned claim so a replay starts fresh instead of 404-ing for
            // the whole retention window (F21). Best-effort.
            release_orphaned_claim(&state, &req, &run_id).await;
            return Err(ServeError::Internal(e.to_string()));
        }
        // The admission lock is per instance, so two instances can both have
        // admitted the last slot: re-count with the record in place and back
        // out (#789 SERVE-43). Racing submitters may both back out — the safe
        // direction; either retries.
        #[cfg(feature = "tenants")]
        if let Some(t) = rec.tenant.as_deref()
            && let Some(msg) =
                recount_refusal(t, crate::serve::tenants::over_limit(&state, t).await)
            && state
                .history()
                .cancel_pending(&run_id)
                .await
                .unwrap_or(false)
        {
            release_orphaned_claim(&state, &req, &run_id).await;
            return Err(ServeError::TooManyRequests(msg));
        }
        // Release the local queue reservation (cluster runs are bounded by the
        // claim loop + semaphore, not the submit-side queue).
        drop(reservation);
        state.cluster().kick();
        crate::serve::audit::write(
            &state,
            &actor,
            "run.submit",
            Some(run_id.clone()),
            Some(fp_config.clone()),
            "ok",
        )
        .await;
        return Ok(SubmitResponse {
            run_id,
            status: RunStatus::Pending,
            submitted_at,
        });
    }

    if let Err(e) = link_tenant_run(&state, &rec).await {
        release_orphaned_claim(&state, &req, &run_id).await;
        return Err(e);
    }
    if let Err(e) = state.history().upsert(&rec).await {
        // See the cluster path above: release the orphaned claim (F21).
        release_orphaned_claim(&state, &req, &run_id).await;
        return Err(ServeError::Internal(e.to_string()));
    }

    let run_token = CancellationToken::new();
    state.registry().register(run_id.clone(), run_token.clone());
    metrics::set_run_gauges(&state);

    // The spawned task now owns the queued→running→finished lifecycle.
    reservation.defuse();
    spawn_run(
        state.clone(),
        principal.clone(),
        loaded,
        req,
        run_id.clone(),
        run_token,
        submitted_at,
    );

    crate::serve::audit::write(
        &state,
        &actor,
        "run.submit",
        Some(run_id.clone()),
        Some(fp_config.clone()),
        "ok",
    )
    .await;

    Ok(SubmitResponse {
        run_id,
        status: RunStatus::Queued,
        submitted_at,
    })
}

/// Record a tenant run in the run → tenant mapping (#709) so a listing can
/// filter by tenant. A failure fails the submission: an unmapped tenant run
/// would be invisible to the tenant's own listing.
async fn link_tenant_run(state: &ServerState, rec: &RunRecord) -> Result<(), ServeError> {
    if let Some(t) = &rec.tenant {
        state
            .history()
            .tenant_run_link(&rec.run_id, t)
            .await
            .map_err(|e| ServeError::Internal(format!("recording the run's tenant: {e}")))?;
    }
    Ok(())
}

/// Run the `doctor_first` probes; on any failure return 422 with the report.
/// Run the `doctor_first` probes. On success returns the (redacted) report so
/// the caller can store it on the run record (`doctor_report`); on any probe
/// failure returns 422 with the same redacted report as `details`.
pub(crate) async fn run_doctor_first(
    state: &ServerState,
    loaded: &LoadedSubmission,
) -> Result<serde_json::Value, ServeError> {
    use faucet_core::check::CheckContext;
    loaded.require_matrix()?;
    let auth = loaded
        .auth_catalog()
        .map_err(|e| ServeError::Unprocessable {
            message: e.to_string(),
            details: None,
        })?;
    let ctx = CheckContext {
        timeout: state.probe_timeout(),
    };
    // Same pipeline-name derivation as the run path above, so SLA probes read
    // the state keys the executor writes.
    let pipeline_name = loaded
        .tenant
        .as_ref()
        .map(|t| t.state_scope())
        .unwrap_or_default()
        .prefix(loaded.cfg.name.as_deref().unwrap_or("serve"));
    let mut invs = crate::commands::doctor::probe_roots(
        &loaded.nodes,
        &auth,
        &ctx,
        loaded.cfg.sla.as_ref(),
        loaded.cfg.profiling.as_ref(),
        &pipeline_name,
    )
    .await;
    let failed = crate::commands::doctor::count_failures(&invs);
    // Redact regardless of outcome — the report is surfaced either way (as the
    // 422 `details` on failure, or stored on the run record on success).
    crate::commands::doctor::redact_invocations(&mut invs);
    let report = serde_json::json!({ "invocations": invs });
    if failed > 0 {
        return Err(ServeError::Unprocessable {
            message: format!("doctor_first preflight failed: {failed} probe(s) failed"),
            details: Some(report),
        });
    }
    Ok(report)
}

/// Build the replay response for an idempotency hit (the existing run's status).
async fn replay_response(
    state: &ServerState,
    actor: &AuthContext,
    run_id: &str,
) -> Result<SubmitResponse, ServeError> {
    let rec = state
        .history()
        .get(run_id)
        .await
        .map_err(|e| ServeError::Internal(e.to_string()))?
        .ok_or(ServeError::NotFound)?;
    if !actor.sees_tenant(rec.tenant.as_deref()) {
        return Err(ServeError::Conflict(IDEMPOTENCY_CONFLICT.into()));
    }
    Ok(SubmitResponse {
        run_id: rec.run_id,
        status: rec.status,
        submitted_at: rec.submitted_at,
    })
}

/// Prefixes of the idempotency keys the server derives itself (tenant
/// namespaces, trigger fires, backfill units, approved changes).
pub(crate) const RESERVED_IDEMPOTENCY_PREFIXES: &[&str] =
    &["tenant:", "trig:", "backfill:", "change:"];

/// Refuse a caller's idempotency key in a namespace the server derives keys
/// in: an unscoped caller could otherwise occupy a tenant's key or a
/// trigger's fire and turn the real one into a replay of its run.
pub(crate) fn refuse_reserved_idempotency_key(key: Option<&str>) -> Result<(), ServeError> {
    match key {
        Some(k)
            if RESERVED_IDEMPOTENCY_PREFIXES
                .iter()
                .any(|p| k.starts_with(p)) =>
        {
            Err(ServeError::BadConfig(format!(
                "idempotency_key '{k}' uses a prefix the server reserves for its own keys \
                 ({}); choose another key",
                RESERVED_IDEMPOTENCY_PREFIXES.join(", ")
            )))
        }
        _ => Ok(()),
    }
}

/// The key an idempotency claim is stored under: a tenant's keys live in
/// their own namespace, so two tenants sending the same key never meet.
pub(crate) fn idempotency_scope(tenant: Option<&str>, key: &str) -> String {
    match tenant {
        Some(t) => format!("tenant:{t}:{key}"),
        None => key.to_string(),
    }
}

/// Releases a queue reservation on drop unless [`Self::defuse`]d. Guarantees the
/// `queued` counter is balanced on every early-return path (replay / conflict /
/// claim-or-upsert error) without a manual `release_reservation` at each site.
/// Defused once the run is handed to the spawned task, which then owns the
/// queued→running transition via `mark_running`.
struct ReservationGuard {
    state: Option<ServerState>,
}

impl ReservationGuard {
    fn new(state: ServerState) -> Self {
        Self { state: Some(state) }
    }

    /// Hand the reservation off to the spawned task (no release on drop).
    fn defuse(mut self) {
        self.state = None;
    }
}

impl Drop for ReservationGuard {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            state.registry().release_reservation();
            metrics::set_run_gauges(&state);
        }
    }
}

/// Releases the in-flight slot (decrement `in_flight`, drop the cancel token, wake
/// the shutdown drain) on drop — on EVERY path including panic. Without this, a
/// panic between `mark_running` and a manual `mark_finished` would leak the
/// counter and hang graceful shutdown forever.
struct InFlightGuard {
    state: ServerState,
    run_id: String,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.state.registry().mark_finished(&self.run_id);
        metrics::set_run_gauges(&self.state);
    }
}

/// Releases a claimed shard's in-flight slot (decrement `in_flight`, drop the
/// shard cancel token, wake the shutdown drain) on drop — on EVERY path
/// including panic. Mirrors [`InFlightGuard`] but keys on the per-shard token so
/// a Mode-B shard participates in the graceful-shutdown drain (audit #321 H5).
struct ShardInFlightGuard {
    state: ServerState,
    run_id: String,
    shard_id: String,
}

impl Drop for ShardInFlightGuard {
    fn drop(&mut self) {
        self.state
            .registry()
            .mark_shard_finished(&self.run_id, &self.shard_id);
        metrics::set_run_gauges(&self.state);
    }
}

/// Terminal classification of a run task.
enum Terminal {
    Completed {
        records: u64,
        invs: Vec<InvocationRecord>,
    },
    Failed {
        reason: String,
        records: u64,
        invs: Vec<InvocationRecord>,
    },
    /// The interrupted variants keep what the pipeline wrote before it
    /// stopped (#789 SERVE-28): rows landed even though the run did not finish.
    Timeout {
        secs: u64,
        records: u64,
        invs: Vec<InvocationRecord>,
    },
    Cancelled {
        records: u64,
        invs: Vec<InvocationRecord>,
    },
    ShutdownFailed {
        records: u64,
        invs: Vec<InvocationRecord>,
    },
}

impl Terminal {
    fn cancelled() -> Self {
        Terminal::Cancelled {
            records: 0,
            invs: Vec::new(),
        }
    }

    fn shutdown_failed() -> Self {
        Terminal::ShutdownFailed {
            records: 0,
            invs: Vec::new(),
        }
    }

    fn timeout(secs: u64) -> Self {
        Terminal::Timeout {
            secs,
            records: 0,
            invs: Vec::new(),
        }
    }

    /// An interrupted run's terminal state, carrying what the pipeline wrote
    /// before it stopped at a page boundary.
    fn with_partial(self, written: u64, done: Vec<InvocationRecord>) -> Self {
        match self {
            Terminal::Timeout { secs, .. } => Terminal::Timeout {
                secs,
                records: written,
                invs: done,
            },
            Terminal::Cancelled { .. } => Terminal::Cancelled {
                records: written,
                invs: done,
            },
            Terminal::ShutdownFailed { .. } => Terminal::ShutdownFailed {
                records: written,
                invs: done,
            },
            other => other,
        }
    }

    /// After a cancel trigger: a flush failure surfaces as itself, a
    /// cooperative stop keeps its partial counts under the trigger's status,
    /// and a run that never answered within the grace keeps nothing.
    fn after_interrupt(self, finished: Option<Terminal>) -> Self {
        match finished {
            Some(failed @ Terminal::Failed { .. }) => failed,
            Some(Terminal::Completed { records, invs }) => self.with_partial(records, invs),
            _ => self,
        }
    }

    /// (status, metric reason label, records, invocations, error message)
    fn into_parts(
        self,
    ) -> (
        RunStatus,
        &'static str,
        u64,
        Vec<InvocationRecord>,
        Option<String>,
    ) {
        match self {
            Terminal::Completed { records, invs } => {
                (RunStatus::Completed, "ok", records, invs, None)
            }
            Terminal::Failed {
                reason,
                records,
                invs,
            } => (RunStatus::Failed, "error", records, invs, Some(reason)),
            Terminal::Timeout {
                secs,
                records,
                invs,
            } => (
                RunStatus::Failed,
                "timeout",
                records,
                invs,
                Some(format!("run exceeded timeout_secs ({secs}s)")),
            ),
            Terminal::Cancelled { records, invs } => {
                (RunStatus::Cancelled, "cancelled", records, invs, None)
            }
            Terminal::ShutdownFailed { records, invs } => (
                RunStatus::Failed,
                "server_shutdown",
                records,
                invs,
                Some("server shutdown before the run finished".into()),
            ),
        }
    }
}

/// Audit every invocation a data-flow policy's runtime backstop denied (#702)
/// as `policy.denied` attributed to `runtime`, so the audit log carries both
/// the submit-time refusals and the ones only the real records revealed.
async fn audit_runtime_policy_denials(
    state: &ServerState,
    run_id: &str,
    result: &crate::error::CliResult<RunSummary>,
) {
    let denied: Vec<String> = match result {
        Ok(summary) => summary
            .invocations
            .iter()
            .filter(|i| {
                matches!(
                    i.error_kind,
                    Some(crate::executor::InvocationErrorKind::Policy)
                )
            })
            .map(|i| i.row_id.clone())
            .collect(),
        Err(crate::error::CliError::Faucet(faucet_core::FaucetError::PolicyViolation {
            ..
        }))
        | Err(crate::error::CliError::PolicyViolations { .. }) => vec![String::new()],
        Err(_) => Vec::new(),
    };
    for row in denied {
        let result = if row.is_empty() {
            "denied".to_string()
        } else {
            format!("denied:{row}")
        };
        crate::serve::audit::write(
            state,
            &AuthContext::runtime(),
            "policy.denied",
            Some(run_id.to_string()),
            None,
            &result,
        )
        .await;
    }
}

/// Classify a finished `run_expanded` result into a `Terminal`.
fn classify_run(result: crate::error::CliResult<RunSummary>) -> Terminal {
    match result {
        Ok(summary) => {
            let records: u64 = summary
                .invocations
                .iter()
                .map(|i| i.records_written as u64)
                .sum();
            let invs: Vec<InvocationRecord> = summary
                .invocations
                .iter()
                .map(InvocationRecord::from)
                .collect();
            if summary.had_failures() {
                Terminal::Failed {
                    reason: format!("{} invocation(s) failed", summary.failure_count()),
                    records,
                    invs,
                }
            } else {
                Terminal::Completed { records, invs }
            }
        }
        Err(e) => Terminal::Failed {
            reason: e.to_string(),
            records: 0,
            invs: Vec::new(),
        },
    }
}

/// Parse the optional request `clock` (RFC3339), defaulting to `submitted_at`.
fn resolve_clock(
    flag: Option<&str>,
    default: DateTime<Utc>,
) -> Result<DateTime<FixedOffset>, ServeError> {
    match flag {
        None => Ok(default.fixed_offset()),
        Some(s) => DateTime::parse_from_rfc3339(s)
            .map_err(|_| ServeError::BadConfig(format!("clock '{s}' is not RFC3339"))),
    }
}

/// Spawn the detached run task: acquire a permit, run under the 3-arm select,
/// finalize the terminal status.
fn spawn_run(
    state: ServerState,
    principal: crate::runtime::Principal,
    loaded: LoadedSubmission,
    req: SubmitRequest,
    run_id: String,
    run_token: CancellationToken,
    submitted_at: DateTime<Utc>,
) {
    let server_shutdown = state.shutdown_token();
    tokio::spawn(async move {
        // Race the permit acquisition against cancel / shutdown so a run
        // cancelled while STILL QUEUED (before any permit frees) is finalized
        // immediately, instead of only after it eventually acquires a permit
        // (#146 R). `biased` prefers the cancel/shutdown signals over a
        // simultaneously-available permit.
        let _permit = tokio::select! {
            biased;
            _ = run_token.cancelled() => {
                finalize_queued_cancel(&state, &run_id, submitted_at, Terminal::cancelled()).await;
                return;
            }
            _ = server_shutdown.cancelled() => {
                finalize_queued_cancel(&state, &run_id, submitted_at, Terminal::shutdown_failed())
                    .await;
                return;
            }
            permit = state.semaphore().acquire_owned() => permit.expect("semaphore not closed"),
        };
        crate::runtime::under(
            &principal.clone(),
            execute_run(
                state,
                principal,
                loaded,
                run_id,
                run_token,
                submitted_at,
                req.timeout_secs,
                req.clock,
                req.concurrency,
                None,
            ),
        )
        .await;
        // `_permit` drops here.
    });
}

/// The queued→running→finalize execution tail, shared by the submit path
/// (`spawn_run`) and the cluster claim path (`resume_claimed_run`). Assumes the
/// caller already holds an execution permit and has registered `run_token`.
/// `from_queue` is `true` when the run consumed a local queue slot (submit path)
/// and `false` for a cluster claim-path run that never reserved one (#228).
#[allow(clippy::too_many_arguments)]
#[allow(clippy::disallowed_methods)] // a runtime timer, not a connector backend call
async fn execute_run(
    state: ServerState,
    principal: crate::runtime::Principal,
    loaded: LoadedSubmission,
    run_id: String,
    run_token: CancellationToken,
    submitted_at: DateTime<Utc>,
    timeout_secs: Option<u64>,
    clock_flag: Option<String>,
    concurrency: Option<usize>,
    claimed: Option<InFlightGuard>,
) {
    let server_shutdown = state.shutdown_token();
    let auth_result = loaded.auth_catalog();
    let LoadedSubmission { cfg, nodes, tenant } = loaded;
    #[cfg(feature = "catalog")]
    let catalog_tenant = tenant.as_ref().map(|t| t.values.id.clone());

    // Queued → running. From here the guard guarantees `mark_finished` (and a
    // gauge refresh) on EVERY exit, including early returns and panics.
    // A submit-path run consumed a local queue slot (Queued→Running); a cluster
    // claim-path run never did (#228) — only bump in_flight for it.
    let _guard = match claimed {
        Some(g) => g,
        None => {
            state.registry().mark_running();
            InFlightGuard {
                state: state.clone(),
                run_id: run_id.clone(),
            }
        }
    };
    let started = Utc::now();
    if let Ok(Some(mut rec)) = state.history().get(&run_id).await {
        rec.status = RunStatus::Running;
        rec.started_at = Some(started);
        let _ = state.history().upsert(&rec).await;
    }
    metrics::set_run_gauges(&state);

    // Build execution options (auth/clock failures finalize as Failed).
    let auth = match auth_result {
        Ok(a) => a,
        Err(e) => {
            finalize(
                &state,
                &run_id,
                started,
                Terminal::Failed {
                    reason: format!("auth catalog: {e}"),
                    records: 0,
                    invs: Vec::new(),
                },
            )
            .await;
            return;
        }
    };
    let clock = match resolve_clock(clock_flag.as_deref(), submitted_at) {
        Ok(c) => c,
        Err(e) => {
            finalize(
                &state,
                &run_id,
                started,
                Terminal::Failed {
                    reason: e.api_error().error.message,
                    records: 0,
                    invs: Vec::new(),
                },
            )
            .await;
            return;
        }
    };

    // Cooperative-cancel token the pipeline observes so it flushes buffered
    // output (e.g. a Parquet footer, an S3 multipart upload) at its next
    // page boundary on cancel / timeout / shutdown — instead of having its
    // future hard-dropped, which flushes nothing (#146 H16).
    let coop = CancellationToken::new();
    // Notifier from the submitted config's `notifications:` block. A malformed
    // block disables notifications for this run (logged) rather than failing an
    // already-accepted run.
    #[cfg(feature = "notify")]
    let notifier = crate::notify::Notifier::from_specs(&cfg.notifications).unwrap_or_else(|e| {
        tracing::error!(%run_id, "notifications config invalid, disabling: {e}");
        None
    });
    #[cfg(feature = "notify")]
    if let Some(n) = &notifier {
        state.log_export().register_notifier(&run_id, n.clone());
    }
    // The governed options (budget, state namespace, resilience, lineage,
    // pricing, catalog); a malformed block finalizes the run as Failed,
    // mirroring the auth/clock failure handling above. Every serve run is
    // recorded in the Data Movement Catalog (#279) under its run id.
    let opts = match crate::runtime::server::governed_options(
        &state,
        &cfg,
        tenant.as_deref(),
        crate::runtime::server::GovernedRun {
            run_id: run_id.clone(),
            auth,
            clock,
            cancel: coop.clone(),
            concurrency,
            shard: None,
            #[cfg(feature = "notify")]
            notifier,
            catalog: true,
        },
    ) {
        Ok(o) => o,
        Err(reason) => {
            finalize(
                &state,
                &run_id,
                started,
                Terminal::Failed {
                    reason,
                    records: 0,
                    invs: Vec::new(),
                },
            )
            .await;
            return;
        }
    };

    let span = tracing::info_span!(parent: None, "faucet.serve.run", serve_run_id = %run_id);
    let audit_state = state.clone();
    let audit_run_id = run_id.clone();
    // The resolved+expanded config snapshot (#374) a successful run leaves in
    // the catalog — what `plan --diff` diffs against and what impact analysis
    // (#707) reads a downstream row's contract from. Same never-fails-the-run
    // contract as the CLI runtimes.
    #[cfg(feature = "catalog")]
    let snapshot_inputs = (
        crate::catalog::CatalogHandle {
            store: state.history(),
            run_id: Some(run_id.clone()),
            sample_records: crate::catalog::DEFAULT_SAMPLE_RECORDS,
            annotations: Vec::new(),
            tenant: catalog_tenant.clone(),
        },
        cfg.name.clone().unwrap_or_else(|| "serve".to_string()),
        crate::catalog::snapshot::on_error_str(&cfg.execution).to_string(),
        nodes.clone(),
    );
    // A topology config runs as its node graph (#789 CLI-17), with the same
    // cancel token, clock, budget and run id a matrix run gets.
    let topology = crate::topology::is_topology(&cfg).then(|| {
        (
            cfg.clone(),
            opts.auth.clone(),
            crate::topology::TopologyRunOptions {
                cancel: opts.cancel.clone(),
                dry_run: false,
                limit: None,
                clock: Some(opts.clock),
                budget: opts.budget.clone(),
                run_id: opts.run_id.clone(),
                force_lease: opts.force_lease,
            },
        )
    });
    let work = async move {
        // Emitted inside the run span so it is captured by the SSE log layer
        // (and gives every `/logs` reader at least one line to anchor on).
        tracing::info!("pipeline run starting");
        let result = match &topology {
            Some((cfg, auth, run)) => crate::topology::run_topology(cfg, auth, run.clone()).await,
            None => crate::runtime::run(&principal, nodes, opts).await,
        };
        audit_runtime_policy_denials(&audit_state, &audit_run_id, &result).await;
        #[cfg(feature = "catalog")]
        {
            let (handle, pipeline, on_error, nodes) = &snapshot_inputs;
            let succeeded = matches!(&result, Ok(s) if !s.had_failures());
            crate::catalog::snapshot::record_if_ok(
                Some(handle),
                pipeline,
                on_error,
                nodes,
                succeeded,
                chrono::Utc::now(),
            )
            .await;
        }
        classify_run(result)
    }
    .instrument(span);
    tokio::pin!(work);

    // The run timeout is modelled as a cancel trigger (not a hard
    // `tokio::time::timeout` drop) so a timed-out run still flushes.
    let timeout_fut = async {
        match timeout_secs {
            Some(s) => tokio::time::sleep(Duration::from_secs(s)).await,
            None => std::future::pending::<()>().await,
        }
    };
    tokio::pin!(timeout_fut);

    enum Trigger {
        Done(Terminal),
        Cancel,
        Shutdown,
        Timeout(u64),
    }

    // Phase 1: run to natural completion, or until a cancel trigger fires.
    // `biased` prefers a just-completed run over a simultaneous trigger.
    let trigger = tokio::select! {
        biased;
        t = &mut work => Trigger::Done(t),
        _ = run_token.cancelled() => Trigger::Cancel,
        _ = server_shutdown.cancelled() => Trigger::Shutdown,
        _ = &mut timeout_fut => Trigger::Timeout(timeout_secs.unwrap_or(0)),
    };

    let shutting_down = matches!(trigger, Trigger::Shutdown);
    let terminal = match trigger {
        Trigger::Done(t) => t,
        triggered => {
            // Phase 2: a trigger fired. Cancel cooperatively and give the
            // pipeline a bounded grace to flush at its next page boundary,
            // then hard-drop it (drops the JoinSet, aborting any pipeline
            // genuinely stuck mid-write) so a hung run can't wedge shutdown.
            coop.cancel();
            let trigger_terminal = match triggered {
                Trigger::Cancel => Terminal::cancelled(),
                Trigger::Shutdown => Terminal::shutdown_failed(),
                Trigger::Timeout(secs) => Terminal::timeout(secs),
                Trigger::Done(_) => unreachable!("matched in the outer arm"),
            };
            // A cooperative cancel makes `run_stream` flush and return Ok(partial),
            // so the trigger label (Cancelled/Timeout/ShutdownFailed) is the
            // correct status for that path. But if the flush itself FAILS within
            // the grace window, surface that real failure — never mask it behind
            // the trigger label, which would hide a data error / partial write.
            trigger_terminal
                .after_interrupt(tokio::time::timeout(RUN_FLUSH_GRACE, &mut work).await.ok())
        }
    };

    // A clustered instance shutting down hands an unfinished run back to the
    // cluster rather than failing it (#789 SERVE-25): a peer claims it and
    // resumes from its bookmark (at-least-once, as any failover is).
    if shutting_down
        && state.cluster().enabled()
        && matches!(terminal, Terminal::ShutdownFailed { .. })
        && release_for_peer(&state, &run_id).await
    {
        state.log_hub().finish(&run_id);
        schedule_log_drop(state.clone(), run_id.clone());
        return;
    }
    finalize(&state, &run_id, started, terminal).await;
    // Signal `/logs` readers the run is done, then drop the buffer after a
    // drain window so a late fetcher can still replay it (spec §12).
    state.log_hub().finish(&run_id);
    schedule_log_drop(state.clone(), run_id.clone());
    // `_guard` drops here → mark_finished + gauge refresh.
}

/// Requeue a run this instance owns as `pending` for a peer. `false` when it
/// could not be released (the caller then fails it as before).
async fn release_for_peer(state: &ServerState, run_id: &str) -> bool {
    let rec = match state.history().get(run_id).await {
        Ok(Some(r)) => r,
        _ => return false,
    };
    match state.history().release_owned(&rec).await {
        Ok(true) => {
            tracing::info!(%run_id, "shutdown: run handed back to the cluster as pending");
            true
        }
        Ok(false) => false,
        Err(e) => {
            tracing::warn!(%run_id, error = %e, "shutdown: could not requeue the run");
            false
        }
    }
}

/// Write the authoritative terminal record + the run-finished metric.
async fn finalize(state: &ServerState, run_id: &str, started: DateTime<Utc>, term: Terminal) {
    let finished = Utc::now();
    let elapsed = (finished - started).to_std().ok().map(|d| d.as_secs_f64());
    let (status, reason, records, invs, error) = term.into_parts();
    // Read-modify-write the existing record to preserve its metadata
    // (name / labels / idempotency_key / submitted_at). If it can't be read —
    // the backend errored, or the record was purged / landed in another store
    // under degraded fallback — DON'T silently drop the terminal status (#146
    // M6): reconstruct a minimal terminal record and upsert it, so the run
    // never lingers non-terminal while `record_run_finished` has already fired.
    let mut rec = match state.history().get(run_id).await {
        Ok(Some(rec)) => rec,
        Ok(None) => {
            tracing::warn!(
                run_id,
                "finalize: run record not found; writing a fresh terminal record"
            );
            RunRecord::queued(run_id.to_string(), None, BTreeMap::new(), None, started)
        }
        Err(e) => {
            tracing::warn!(
                run_id,
                error = %e,
                "finalize: failed to read run record; writing a fresh terminal record"
            );
            RunRecord::queued(run_id.to_string(), None, BTreeMap::new(), None, started)
        }
    };
    rec.status = status;
    rec.started_at.get_or_insert(started);
    rec.finished_at = Some(finished);
    rec.elapsed_secs = elapsed;
    rec.records_written = records;
    rec.invocations = invs;
    rec.error = error.map(|e| crate::secrets::registry::redact_message(&e));
    if state.cluster().enabled() {
        // Owner-fenced: if another instance reclaimed this run (lease expired),
        // our write is a no-op and we discard our result — the reclaimer is now
        // authoritative (#197).
        match state.history().finalize_owned(&rec).await {
            Ok(true) => {
                metrics::record_run_finished(status, reason);
                record_tenant_run(&rec);
                // Only the instance that won the owner-fenced write fires the
                // callback, so a reclaimed run cannot deliver twice (#481).
                crate::serve::callback::fire(&rec).await;
            }
            Ok(false) => tracing::warn!(
                run_id,
                "finalize: run was reclaimed by another instance; discarding result"
            ),
            Err(e) => {
                tracing::error!(run_id, error = %e, "finalize: owner-fenced write failed")
            }
        }
    } else {
        if let Err(e) = state.history().upsert(&rec).await {
            tracing::error!(
                run_id,
                error = %e,
                "finalize: failed to persist terminal run record"
            );
        }
        metrics::record_run_finished(status, reason);
        record_tenant_run(&rec);
        crate::serve::callback::fire(&rec).await;
    }
}

/// Count a finished tenant run (#709).
fn record_tenant_run(rec: &RunRecord) {
    #[cfg(feature = "tenants")]
    if let Some(t) = &rec.tenant {
        crate::serve::tenants::metrics::record_run(t, rec.status.as_str());
    }
    #[cfg(not(feature = "tenants"))]
    let _ = rec;
}

/// Finalize a run that was cancelled / hit shutdown while still QUEUED (before
/// it acquired an execution permit, so it never became in-flight and has no
/// `InFlightGuard`). Releases the queue slot, writes the terminal record, closes
/// the log buffer, and refreshes the gauges — the queued-path analogue of the
/// normal `finalize` + `InFlightGuard`-drop cleanup.
async fn finalize_queued_cancel(
    state: &ServerState,
    run_id: &str,
    submitted_at: DateTime<Utc>,
    term: Terminal,
) {
    state.registry().mark_queued_cancelled(run_id);
    finalize(state, run_id, submitted_at, term).await;
    state.log_hub().finish(run_id);
    schedule_log_drop(state.clone(), run_id.to_string());
    metrics::set_run_gauges(state);
}

/// Spawn a detached timer that drops a finished run's log buffer after the drain
/// window, freeing its ring once late `/logs` fetchers have had a chance to read.
#[allow(clippy::disallowed_methods)] // a runtime timer, not a connector backend call
fn schedule_log_drop(state: ServerState, run_id: String) {
    tokio::spawn(async move {
        tokio::time::sleep(crate::serve::logs::LOG_DRAIN).await;
        state.log_hub().drop_run(&run_id);
    });
}

/// The cluster re-count's verdict once the run is recorded: the refusal
/// message when the tenant is over its limit. A failed re-count admits the run
/// (logged) — the record already exists and a peer may have claimed it, so an
/// error here must not report a failure for a run that will still execute.
#[cfg(feature = "tenants")]
fn recount_refusal(tenant: &str, recount: Result<Option<String>, ServeError>) -> Option<String> {
    match recount {
        Ok(refusal) => refusal,
        Err(e) => {
            tracing::warn!(
                tenant,
                error = %e,
                "could not re-count the tenant's runs after recording a clustered run; admitting it"
            );
            None
        }
    }
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)] // tests drive time and raw clients directly
mod tests {
    use super::*;

    /// A caller's config runs with the caller reference policy scoped over
    /// it, so its HTTP clients check every request and redirect hop (#881).
    #[tokio::test]
    async fn a_caller_run_executes_under_the_caller_reference_policy() {
        use crate::runtime::{Origin, Principal};
        let trusted = Principal::new(AuthContext::system("t"), Origin::Trusted);
        assert!(trusted.reference_policy().is_none());
        let caller = Principal::new(
            AuthContext::system("t"),
            Origin::Caller {
                allow_subprocess: false,
            },
        );
        let policy = caller.reference_policy().expect("a caller run is scoped");
        assert!(policy.check_url("http://169.254.169.254/").is_err());
        assert!(!policy.allows_subprocess());
        let seen = crate::runtime::under(&caller, async {
            faucet_core::http::current_reference_policy()
        })
        .await;
        assert!(seen.is_some());
        assert!(
            crate::runtime::under(&trusted, async {
                faucet_core::http::current_reference_policy()
            })
            .await
            .is_none()
        );
    }

    #[test]
    fn caller_idempotency_keys_stay_out_of_the_server_namespaces() {
        assert!(refuse_reserved_idempotency_key(None).is_ok());
        assert!(refuse_reserved_idempotency_key(Some("nightly-2026-10-12")).is_ok());
        for k in ["tenant:acme:k", "trig:t:1", "backfill:h:u", "change:c1"] {
            let err = refuse_reserved_idempotency_key(Some(k)).unwrap_err();
            assert!(
                matches!(err, ServeError::BadConfig(ref m) if m.contains(k)),
                "{err:?}"
            );
        }
    }

    #[cfg(feature = "tenants")]
    #[test]
    fn a_failed_cluster_recount_admits_the_recorded_run() {
        assert_eq!(recount_refusal("acme", Ok(None)), None);
        assert_eq!(
            recount_refusal("acme", Ok(Some("over".into()))).as_deref(),
            Some("over")
        );
        assert_eq!(
            recount_refusal("acme", Err(ServeError::Internal("db".into()))),
            None
        );
    }

    /// A stand-in admin actor for the submit() tests.
    fn admin_actor() -> AuthContext {
        AuthContext {
            principal: "test".into(),
            role: crate::serve::rbac::Role::Admin,
            source_ip: None,
            tenant: None,
        }
    }

    #[tokio::test]
    async fn a_replay_never_reveals_another_tenants_run() {
        assert_eq!(idempotency_scope(None, "k"), "k");
        assert_eq!(idempotency_scope(Some("acme"), "k"), "tenant:acme:k");
        let state = crate::serve::test_support::test_state();
        let mut rec = RunRecord::queued("r1".into(), None, BTreeMap::new(), None, Utc::now());
        rec.tenant = Some("acme".into());
        state.history().upsert(&rec).await.unwrap();
        let globex = AuthContext {
            tenant: Some("globex".into()),
            ..admin_actor()
        };
        assert!(matches!(
            replay_response(&state, &globex, "r1").await,
            Err(ServeError::Conflict(_))
        ));
        assert_eq!(
            replay_response(&state, &admin_actor(), "r1")
                .await
                .unwrap()
                .run_id,
            "r1"
        );
    }

    #[test]
    fn classify_ok_no_failures_is_completed() {
        let summary = RunSummary {
            invocations: vec![crate::executor::InvocationOutcome {
                row_id: "r".into(),
                parent_record_key: None,
                run_id: None,
                records_written: 3,
                error: None,
                error_kind: None,
                metrics: None,
                usage: None,
            }],
        };
        let (status, reason, records, _, error) = classify_run(Ok(summary)).into_parts();
        assert_eq!(status, RunStatus::Completed);
        assert_eq!(reason, "ok");
        assert_eq!(records, 3);
        assert!(error.is_none());
    }

    #[test]
    fn classify_ok_with_failures_is_failed() {
        let summary = RunSummary {
            invocations: vec![crate::executor::InvocationOutcome {
                row_id: "r".into(),
                parent_record_key: None,
                run_id: None,
                records_written: 0,
                error: Some("boom".into()),
                error_kind: None,
                metrics: None,
                usage: None,
            }],
        };
        let (status, reason, _, _, error) = classify_run(Ok(summary)).into_parts();
        assert_eq!(status, RunStatus::Failed);
        assert_eq!(reason, "error");
        assert!(error.unwrap().contains("invocation(s) failed"));
    }

    #[test]
    fn interrupted_runs_keep_what_they_wrote() {
        let inv: InvocationRecord = serde_json::from_value(serde_json::json!({
            "row_id": "r", "parent_record_key": null, "records_written": 7, "error": null
        }))
        .unwrap();
        let done = || Terminal::Completed {
            records: 7,
            invs: vec![inv.clone()],
        };
        for (t, status) in [
            (Terminal::cancelled(), RunStatus::Cancelled),
            (Terminal::shutdown_failed(), RunStatus::Failed),
            (Terminal::timeout(3), RunStatus::Failed),
        ] {
            let (s, _, records, invs, _) = t.after_interrupt(Some(done())).into_parts();
            assert_eq!((s, records, invs.len()), (status, 7, 1));
        }
        let (_, _, records, _, _) = Terminal::cancelled().after_interrupt(None).into_parts();
        assert_eq!(records, 0, "a run that never answered keeps nothing");
        let failed = Terminal::Failed {
            reason: "flush".into(),
            records: 1,
            invs: Vec::new(),
        };
        let (s, reason, ..) = Terminal::cancelled()
            .after_interrupt(Some(failed))
            .into_parts();
        assert_eq!((s, reason), (RunStatus::Failed, "error"));
        let (_, _, records, ..) = done().with_partial(1, Vec::new()).into_parts();
        assert_eq!(records, 7, "only interrupted variants take partial counts");
    }

    #[tokio::test]
    async fn a_draining_server_refuses_submissions() {
        let state = crate::serve::test_support::test_state();
        assert!(!state.is_draining());
        state.set_draining();
        let req: SubmitRequest =
            serde_json::from_value(serde_json::json!({ "config": "version: 1\n" })).unwrap();
        let err = submit(state, req, AuthContext::system("t"))
            .await
            .unwrap_err();
        assert!(matches!(err, ServeError::Draining), "{err:?}");
    }

    async fn wait_terminal(state: &ServerState, run_id: &str) -> RunRecord {
        for _ in 0..200 {
            if let Some(r) = state.history().get(run_id).await.unwrap()
                && r.status.is_terminal()
            {
                return r;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("run {run_id} never finished");
    }

    #[tokio::test]
    async fn an_inline_action_is_refused_while_every_run_slot_is_busy() {
        let state = crate::serve::test_support::test_state();
        let held = state.semaphore().acquire_many_owned(4).await.unwrap();
        assert!(matches!(
            inline_permit(&state),
            Err(ServeError::TooManyRequests(_))
        ));
        drop(held);
        assert!(inline_permit(&state).is_ok());
    }

    #[tokio::test]
    async fn a_queued_run_is_failed_by_a_shutdown_before_it_starts() {
        let state = crate::serve::test_support::test_state();
        let _held = state.semaphore().acquire_many_owned(4).await.unwrap();
        let req: SubmitRequest = serde_json::from_value(serde_json::json!({
            "config": "version: 1\npipeline:\n  source: { type: csv, config: { path: x.csv } }\n  sink: { type: jsonl, config: { path: out.jsonl } }\n"
        }))
        .unwrap();
        let resp = submit(state.clone(), req, admin_actor()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        state.shutdown_token().cancel();
        let rec = wait_terminal(&state, &resp.run_id).await;
        assert_eq!(rec.status, RunStatus::Failed, "{rec:?}");
    }

    #[cfg(feature = "source-webhook")]
    #[tokio::test]
    async fn a_clustered_run_the_store_cannot_hand_back_is_failed_at_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let state = crate::serve::test_support::test_state_clustered();
        let mut rec = RunRecord::queued("r".into(), None, BTreeMap::new(), None, Utc::now());
        rec.status = RunStatus::Running;
        rec.config_body = Some(format!(
            "version: 1\npipeline:\n  source: {{ type: webhook, config: {{ listen_addr: \"127.0.0.1:0\", timeout_secs: 60 }} }}\n  sink: {{ type: jsonl, config: {{ path: \"{}\" }} }}\n",
            dir.path().join("out.jsonl").display()
        ));
        state.history().upsert(&rec).await.unwrap();
        resume_claimed_run(state.clone(), rec);
        tokio::time::sleep(Duration::from_millis(500)).await;
        state.shutdown_token().cancel();
        let rec = wait_terminal(&state, "r").await;
        assert_eq!(rec.status, RunStatus::Failed, "{rec:?}");
    }

    #[cfg(feature = "source-webhook")]
    #[tokio::test]
    async fn a_run_that_outlives_its_timeout_is_stopped_and_failed() {
        let dir = tempfile::tempdir().unwrap();
        let state = crate::serve::test_support::test_state();
        let req: SubmitRequest = serde_json::from_value(serde_json::json!({
            "config": format!(
                "version: 1\npipeline:\n  source: {{ type: webhook, config: {{ listen_addr: \"127.0.0.1:0\", timeout_secs: 60 }} }}\n  sink: {{ type: jsonl, config: {{ path: \"{}\" }} }}\n",
                dir.path().join("out.jsonl").display()
            ),
            "timeout_secs": 1,
        }))
        .unwrap();
        let resp = submit(state.clone(), req, admin_actor()).await.unwrap();
        let rec = wait_terminal(&state, &resp.run_id).await;
        assert_eq!(rec.status, RunStatus::Failed, "{rec:?}");
        assert!(
            rec.error.as_deref().unwrap_or_default().contains("1s"),
            "{rec:?}"
        );
    }

    #[test]
    fn timeout_maps_to_failed_with_timeout_reason() {
        let (status, reason, _, _, error) = Terminal::timeout(30).into_parts();
        assert_eq!(status, RunStatus::Failed);
        assert_eq!(reason, "timeout");
        assert!(error.unwrap().contains("30s"));
    }

    #[test]
    fn resolve_clock_defaults_and_parses() {
        let default = Utc::now();
        assert_eq!(
            resolve_clock(None, default).unwrap(),
            default.fixed_offset()
        );
        assert!(resolve_clock(Some("2026-01-31T00:00:00Z"), default).is_ok());
        assert!(resolve_clock(Some("not-a-time"), default).is_err());
    }

    #[tokio::test]
    async fn conflict_releases_reservation() {
        use crate::serve::config::{AuthMode, HistoryBackendSpec, ServeConfig};
        use crate::serve::history::RunHistory;
        use crate::serve::history::memory::MemoryHistory;
        use crate::serve::state::ServerState;
        use std::sync::Arc;
        use tokio_util::sync::CancellationToken;

        let cfg = ServeConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            log_format: crate::cli::LogFormat::Text,
            auth: AuthMode::None,
            max_concurrent_runs: 4,
            max_queued_runs: 4,
            default_config_path: None,
            history: HistoryBackendSpec::Memory,
            cors_origins: vec![],
            body_limit_bytes: 1_048_576,
            shutdown_grace: Duration::from_secs(60),
            retain_terminal_runs: Duration::from_secs(60),
            idempotency_retention: Duration::from_secs(60),
            log_retention: std::time::Duration::from_secs(0),
            log_max_lines_per_run: 100_000,
            log_buffer: Default::default(),
            local_output_retention_days: 7,
            local_output_in_flight_grace: Duration::from_secs(60),
            preview: crate::serve::preview::PreviewConfig::default(),
            lease_ttl: Duration::from_secs(30),
            probe_timeout: Duration::from_secs(10),
            env_file: None,
            no_env_file: false,
            log_level: "info".into(),
            ui_enabled: true,
            cluster: crate::serve::cluster::ClusterConfig::disabled(),
            triggers_path: None,
            templates_sync_path: None,
            policy_path: None,
            otel: None,
            callback_allow_hosts: Vec::new(),
            require_approval: Vec::new(),
            approval_expiry: std::time::Duration::from_secs(86_400),
            vault: None,
            connect_providers_path: None,
            allow_subprocess_connectors: false,
            references: Default::default(),
        };
        let history = Arc::new(MemoryHistory::new(Duration::from_secs(60))) as Arc<dyn RunHistory>;
        let state = ServerState::new(
            &cfg,
            None,
            CancellationToken::new(),
            history,
            crate::serve::logs::LogHub::new(),
            None,
            #[cfg(feature = "triggers")]
            crate::serve::triggers::health::TriggersHandle::empty(),
        );

        // Pre-claim the key with a DIFFERENT fingerprint so submit() hits Conflict.
        state
            .history()
            .claim_idempotency("k", "different-fp", "prior", Duration::from_secs(60))
            .await
            .unwrap();

        let req = SubmitRequest {
            config: "version: 1\npipeline:\n  source: { type: csv, config: { path: x.csv } }\n  sink: { type: jsonl, config: { path: out.jsonl } }\n".into(),
            config_format: ConfigFormatWire::Yaml,
            name: None,
            labels: BTreeMap::new(),
            timeout_secs: None,
            doctor_first: false,
            callback: None,
            idempotency_key: Some("k".into()),
            clock: None,
            concurrency: None,
            require_approval: false,
            reason: None,
            budget: None,
            approved_change: None,
            selection: None,
            trusted_config: false,
            server_file: false,
        };

        let err = submit(state.clone(), req, admin_actor()).await.unwrap_err();
        assert!(
            matches!(err, ServeError::Conflict(_)),
            "expected Conflict, got {err:?}"
        );
        // The reservation taken before the claim must have been released by the guard.
        assert_eq!(state.registry().queued(), 0);
    }

    #[tokio::test]
    async fn cluster_submit_writes_pending_with_config_and_does_not_spawn() {
        use crate::serve::cluster::ClusterConfig;
        use crate::serve::config::{AuthMode, HistoryBackendSpec, ServeConfig};
        use crate::serve::history::RunHistory;
        use crate::serve::history::memory::MemoryHistory;
        use crate::serve::state::ServerState;
        use std::sync::Arc;
        use tokio_util::sync::CancellationToken;

        let mut cluster = ClusterConfig::disabled();
        cluster.enabled = true;
        let cfg = ServeConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            log_format: crate::cli::LogFormat::Text,
            auth: AuthMode::None,
            max_concurrent_runs: 4,
            max_queued_runs: 4,
            default_config_path: None,
            history: HistoryBackendSpec::Memory,
            cors_origins: vec![],
            body_limit_bytes: 1_048_576,
            shutdown_grace: Duration::from_secs(60),
            retain_terminal_runs: Duration::from_secs(60),
            idempotency_retention: Duration::from_secs(60),
            log_retention: std::time::Duration::from_secs(0),
            log_max_lines_per_run: 100_000,
            log_buffer: Default::default(),
            local_output_retention_days: 7,
            local_output_in_flight_grace: Duration::from_secs(60),
            preview: crate::serve::preview::PreviewConfig::default(),
            lease_ttl: Duration::from_secs(30),
            probe_timeout: Duration::from_secs(10),
            env_file: None,
            no_env_file: false,
            log_level: "info".into(),
            ui_enabled: true,
            cluster,
            triggers_path: None,
            templates_sync_path: None,
            policy_path: None,
            otel: None,
            callback_allow_hosts: Vec::new(),
            require_approval: Vec::new(),
            approval_expiry: std::time::Duration::from_secs(86_400),
            vault: None,
            connect_providers_path: None,
            allow_subprocess_connectors: false,
            references: Default::default(),
        };
        let history = Arc::new(MemoryHistory::new(Duration::from_secs(60))) as Arc<dyn RunHistory>;
        let state = ServerState::new(
            &cfg,
            None,
            CancellationToken::new(),
            history,
            crate::serve::logs::LogHub::new(),
            None,
            #[cfg(feature = "triggers")]
            crate::serve::triggers::health::TriggersHandle::empty(),
        );

        let req = SubmitRequest {
            config: "version: 1\npipeline:\n  source: { type: csv, config: { path: x.csv } }\n  sink: { type: jsonl, config: { path: out.jsonl } }\n".into(),
            config_format: ConfigFormatWire::Yaml,
            name: Some("n".into()),
            labels: BTreeMap::new(),
            timeout_secs: Some(99),
            doctor_first: false,
            callback: None,
            idempotency_key: None,
            clock: None,
            concurrency: None,
            require_approval: false,
            reason: None,
            budget: None,
            approved_change: None,
            selection: None,
            trusted_config: false,
            server_file: false,
        };
        let resp = submit(state.clone(), req, admin_actor()).await.unwrap();
        assert_eq!(resp.status, RunStatus::Pending);
        // No local queue slot was consumed (cluster runs don't queue locally).
        assert_eq!(state.registry().queued(), 0);
        let rec = state.history().get(&resp.run_id).await.unwrap().unwrap();
        assert_eq!(rec.status, RunStatus::Pending);
        assert!(rec.config_body.as_deref().unwrap().contains("version: 1"));
        assert_eq!(rec.timeout_secs, Some(99));
    }

    /// A `ServerState` backed by an in-memory history, for finalize tests.
    fn memory_state() -> crate::serve::state::ServerState {
        use crate::serve::config::{AuthMode, HistoryBackendSpec, ServeConfig};
        use crate::serve::history::RunHistory;
        use crate::serve::history::memory::MemoryHistory;
        use crate::serve::state::ServerState;
        use std::sync::Arc;
        use tokio_util::sync::CancellationToken;

        let cfg = ServeConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            log_format: crate::cli::LogFormat::Text,
            auth: AuthMode::None,
            max_concurrent_runs: 4,
            max_queued_runs: 4,
            default_config_path: None,
            history: HistoryBackendSpec::Memory,
            cors_origins: vec![],
            body_limit_bytes: 1_048_576,
            shutdown_grace: Duration::from_secs(60),
            retain_terminal_runs: Duration::from_secs(60),
            idempotency_retention: Duration::from_secs(60),
            log_retention: std::time::Duration::from_secs(0),
            log_max_lines_per_run: 100_000,
            log_buffer: Default::default(),
            local_output_retention_days: 7,
            local_output_in_flight_grace: Duration::from_secs(60),
            preview: crate::serve::preview::PreviewConfig::default(),
            lease_ttl: Duration::from_secs(30),
            probe_timeout: Duration::from_secs(10),
            env_file: None,
            no_env_file: false,
            log_level: "info".into(),
            ui_enabled: true,
            cluster: crate::serve::cluster::ClusterConfig::disabled(),
            triggers_path: None,
            templates_sync_path: None,
            policy_path: None,
            otel: None,
            callback_allow_hosts: Vec::new(),
            require_approval: Vec::new(),
            approval_expiry: std::time::Duration::from_secs(86_400),
            vault: None,
            connect_providers_path: None,
            allow_subprocess_connectors: false,
            references: Default::default(),
        };
        let history = Arc::new(MemoryHistory::new(Duration::from_secs(60))) as Arc<dyn RunHistory>;
        ServerState::new(
            &cfg,
            None,
            CancellationToken::new(),
            history,
            crate::serve::logs::LogHub::new(),
            None,
            #[cfg(feature = "triggers")]
            crate::serve::triggers::health::TriggersHandle::empty(),
        )
    }

    #[tokio::test]
    async fn finalize_writes_terminal_record_when_record_is_missing() {
        // M6 (#146): if the run record can't be read at finalize time (purged,
        // or split to another store under degraded fallback), the terminal
        // status must NOT be silently dropped — a fresh terminal record is
        // written so the run never lingers non-terminal while the run-finished
        // metric has already fired.
        let state = memory_state();
        let started = Utc::now();
        finalize(
            &state,
            "ghost",
            started,
            Terminal::Failed {
                reason: "boom".into(),
                records: 0,
                invs: Vec::new(),
            },
        )
        .await;
        let rec = state
            .history()
            .get("ghost")
            .await
            .unwrap()
            .expect("finalize must create a terminal record even when none existed");
        assert_eq!(rec.status, RunStatus::Failed);
        assert!(rec.finished_at.is_some());
        assert!(rec.started_at.is_some());
        assert_eq!(rec.error.as_deref(), Some("boom"));
    }

    #[tokio::test]
    async fn finalize_preserves_metadata_of_existing_record() {
        // The happy path still read-modify-writes, preserving name/labels/key.
        let state = memory_state();
        let started = Utc::now();
        let mut rec = RunRecord::queued(
            "r1".into(),
            Some("nightly".into()),
            BTreeMap::new(),
            Some("idem-k".into()),
            started,
        );
        rec.status = RunStatus::Running;
        rec.started_at = Some(started);
        state.history().upsert(&rec).await.unwrap();

        finalize(
            &state,
            "r1",
            started,
            Terminal::Completed {
                records: 5,
                invs: Vec::new(),
            },
        )
        .await;
        let got = state.history().get("r1").await.unwrap().unwrap();
        assert_eq!(got.status, RunStatus::Completed);
        assert_eq!(got.records_written, 5);
        assert_eq!(got.name.as_deref(), Some("nightly"));
        assert_eq!(got.idempotency_key.as_deref(), Some("idem-k"));
    }

    #[cfg(any(feature = "serve-history-sqlite", feature = "serve-history-postgres"))]
    #[tokio::test]
    async fn cluster_submit_503s_when_history_degraded() {
        // #197 spec §9: a degraded backend can't coordinate a cluster, so submit
        // must fail closed with 503 rather than orphan a never-claimable Pending run.
        use crate::serve::cluster::ClusterConfig;
        use crate::serve::config::{AuthMode, HistoryBackendSpec, ServeConfig};
        use crate::serve::history::RunHistory;
        use crate::serve::history::fallback::FallbackHistory;
        use crate::serve::state::ServerState;
        use std::sync::Arc;
        use tokio_util::sync::CancellationToken;

        let mut cluster = ClusterConfig::disabled();
        cluster.enabled = true;
        let cfg = ServeConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            log_format: crate::cli::LogFormat::Text,
            auth: AuthMode::None,
            max_concurrent_runs: 4,
            max_queued_runs: 4,
            default_config_path: None,
            history: HistoryBackendSpec::Memory,
            cors_origins: vec![],
            body_limit_bytes: 1_048_576,
            shutdown_grace: Duration::from_secs(60),
            retain_terminal_runs: Duration::from_secs(60),
            idempotency_retention: Duration::from_secs(60),
            log_retention: std::time::Duration::from_secs(0),
            log_max_lines_per_run: 100_000,
            log_buffer: Default::default(),
            local_output_retention_days: 7,
            local_output_in_flight_grace: Duration::from_secs(60),
            preview: crate::serve::preview::PreviewConfig::default(),
            lease_ttl: Duration::from_secs(30),
            probe_timeout: Duration::from_secs(10),
            env_file: None,
            no_env_file: false,
            log_level: "info".into(),
            ui_enabled: true,
            cluster,
            triggers_path: None,
            templates_sync_path: None,
            policy_path: None,
            otel: None,
            callback_allow_hosts: Vec::new(),
            require_approval: Vec::new(),
            approval_expiry: std::time::Duration::from_secs(86_400),
            vault: None,
            connect_providers_path: None,
            allow_subprocess_connectors: false,
            references: Default::default(),
        };
        // A backend that is degraded from startup (primary unreachable).
        let history = Arc::new(FallbackHistory::degraded_at_startup(
            Duration::from_secs(60),
            "test",
        )) as Arc<dyn RunHistory>;
        assert!(history.degraded());
        let state = ServerState::new(
            &cfg,
            None,
            CancellationToken::new(),
            history,
            crate::serve::logs::LogHub::new(),
            None,
            #[cfg(feature = "triggers")]
            crate::serve::triggers::health::TriggersHandle::empty(),
        );
        let req = SubmitRequest {
            config: "version: 1\npipeline:\n  source: { type: csv, config: { path: x.csv } }\n  sink: { type: jsonl, config: { path: out.jsonl } }\n".into(),
            config_format: ConfigFormatWire::Yaml,
            name: None,
            labels: BTreeMap::new(),
            timeout_secs: None,
            doctor_first: false,
            callback: None,
            idempotency_key: None,
            clock: None,
            concurrency: None,
            require_approval: false,
            reason: None,
            budget: None,
            approved_change: None,
            selection: None,
            trusted_config: false,
            server_file: false,
        };
        let err = submit(state.clone(), req, admin_actor()).await.unwrap_err();
        assert!(
            matches!(err, ServeError::Unavailable(_)),
            "expected 503 Unavailable, got {err:?}"
        );
        // The queue reservation must have been released (no leak).
        assert_eq!(state.registry().queued(), 0);
    }

    // ── Mode B coverage: coordinator / parent finalize / shard execution ─────
    // SQLite-backed so the shard RunHistory methods are live (the memory backend
    // is inert for shards). No Docker: the S3 source builds offline and its
    // enumerate_shards is pure (hash-modulo); csv→jsonl runs entirely on temp
    // files.
    #[cfg(feature = "serve-history-sqlite")]
    mod shards {
        use super::*;
        use crate::serve::config::{AuthMode, HistoryBackendSpec, ServeConfig};
        use crate::serve::history::RunHistory;
        use crate::serve::history::sqlite::SqliteHistory;
        use crate::serve::load::ConfigFormat;
        use crate::serve::state::ServerState;
        use faucet_core::ShardSpec;
        use std::collections::BTreeMap;
        use std::sync::Arc;
        use tokio_util::sync::CancellationToken;

        async fn sqlite_state(dir: &std::path::Path) -> ServerState {
            sqlite_state_with(dir, crate::serve::cluster::ClusterConfig::disabled()).await
        }

        async fn sqlite_state_with(
            dir: &std::path::Path,
            cluster: crate::serve::cluster::ClusterConfig,
        ) -> ServerState {
            let url = format!("sqlite://{}/h.db", dir.display());
            let history = Arc::new(
                SqliteHistory::connect(
                    &url,
                    Duration::from_secs(300),
                    Duration::from_secs(300),
                    "inst-test".into(),
                )
                .await
                .expect("sqlite history"),
            ) as Arc<dyn RunHistory>;
            let cfg = ServeConfig {
                listen: "127.0.0.1:0".parse().unwrap(),
                log_format: crate::cli::LogFormat::Text,
                auth: AuthMode::None,
                max_concurrent_runs: 4,
                max_queued_runs: 4,
                default_config_path: None,
                history: HistoryBackendSpec::Memory,
                cors_origins: vec![],
                body_limit_bytes: 1_048_576,
                shutdown_grace: Duration::from_secs(60),
                retain_terminal_runs: Duration::from_secs(60),
                idempotency_retention: Duration::from_secs(60),
                log_retention: std::time::Duration::from_secs(0),
                log_max_lines_per_run: 100_000,
                log_buffer: Default::default(),
                local_output_retention_days: 7,
                local_output_in_flight_grace: Duration::from_secs(60),
                preview: crate::serve::preview::PreviewConfig::default(),
                lease_ttl: Duration::from_secs(30),
                probe_timeout: Duration::from_secs(10),
                env_file: None,
                no_env_file: false,
                log_level: "info".into(),
                ui_enabled: true,
                cluster,
                triggers_path: None,
                templates_sync_path: None,
                policy_path: None,
                otel: None,
                callback_allow_hosts: Vec::new(),
                require_approval: Vec::new(),
                approval_expiry: std::time::Duration::from_secs(86_400),
                vault: None,
                connect_providers_path: None,
                allow_subprocess_connectors: false,
                references: Default::default(),
            };
            ServerState::new(
                &cfg,
                None,
                CancellationToken::new(),
                history,
                crate::serve::logs::LogHub::new(),
                None,
                #[cfg(feature = "triggers")]
                crate::serve::triggers::health::TriggersHandle::empty(),
            )
        }

        async fn loaded(yaml: &str) -> LoadedSubmission {
            crate::serve::load::load_submission(
                yaml,
                ConfigFormat::Yaml,
                None,
                None,
                BodyOrigin::Trusted,
            )
            .await
            .expect("load submission")
        }

        async fn seed_run(state: &ServerState, run_id: &str, status: RunStatus) {
            let mut rec = RunRecord::queued(run_id.into(), None, BTreeMap::new(), None, Utc::now());
            rec.status = status;
            rec.config_body = Some("version: 1".into());
            state.history().upsert(&rec).await.expect("seed run");
        }

        #[tokio::test]
        async fn coordinate_matrix_run_is_not_shardable() {
            // A matrix expands to >1 node → not shardable → Ok(false), no build.
            let dir = tempfile::tempdir().unwrap();
            let state = sqlite_state(dir.path()).await;
            let l = loaded(
                "version: 1\nname: m\nmatrix:\n  - id: a\n  - id: b\npipeline:\n  \
                 source: { type: rest, config: { url: \"http://localhost/x\" } }\n  \
                 sink: { type: stdout, config: {} }\n",
            )
            .await;
            assert!(!coordinate_sharded_run(&state, "r", &l, 4).await.unwrap());
        }

        #[tokio::test]
        async fn coordinate_non_shardable_source_runs_whole() {
            // A csv source is not shardable → Ok(false) (built offline).
            let dir = tempfile::tempdir().unwrap();
            let state = sqlite_state(dir.path()).await;
            let input = dir.path().join("in.csv");
            std::fs::write(&input, "id\n1\n").unwrap();
            let l = loaded(&format!(
                "version: 1\npipeline:\n  \
                 source: {{ type: csv, config: {{ path: \"{}\" }} }}\n  \
                 sink: {{ type: stdout, config: {{}} }}\n",
                input.display()
            ))
            .await;
            assert!(!coordinate_sharded_run(&state, "r", &l, 4).await.unwrap());
        }

        #[tokio::test]
        async fn coordinate_s3_source_inserts_shards_and_marks_sharded() {
            let dir = tempfile::tempdir().unwrap();
            let state = sqlite_state(dir.path()).await;
            seed_run(&state, "r", RunStatus::Running).await;
            let l = loaded(
                "version: 1\npipeline:\n  \
                 source: { type: s3, config: { bucket: my-bucket, prefix: null, \
                 region: null, endpoint_url: null, file_format: json_lines, \
                 max_objects: null, concurrency: 10 } }\n  \
                 sink: { type: stdout, config: {} }\n",
            )
            .await;
            assert!(coordinate_sharded_run(&state, "r", &l, 4).await.unwrap());
            // 4 shards inserted; parent flipped to Sharded.
            let prog = state.history().shard_progress("r").await.unwrap();
            assert_eq!(prog.total, 4);
            assert_eq!(prog.pending, 4);
            assert_eq!(
                state.history().get("r").await.unwrap().unwrap().status,
                RunStatus::Sharded
            );
        }

        #[tokio::test]
        async fn at_least_once_risky_sinks_flags_append_cluster_and_shard() {
            // F26/F39: a clustered or sharded run with an append-mode sink is
            // at-least-once → flagged. Neither flag → not flagged.
            let append = loaded(
                "version: 1\npipeline:\n  \
                 source: { type: rest, config: { url: \"http://localhost/x\" } }\n  \
                 sink: { type: stdout, config: {} }\n",
            )
            .await;
            // Not clustered, not sharded → no warning.
            assert!(at_least_once_risky_sinks(&append, false, false).is_empty());
            // Clustered append → flagged.
            assert_eq!(
                at_least_once_risky_sinks(&append, true, false),
                vec!["stdout"]
            );
            // Sharded append → flagged.
            assert_eq!(
                at_least_once_risky_sinks(&append, false, true),
                vec!["stdout"]
            );
        }

        #[tokio::test]
        async fn at_least_once_risky_sinks_safe_for_upsert_and_exactly_once() {
            // Upsert sink → keyed re-writes are idempotent → not flagged.
            let upsert = loaded(
                "version: 1\npipeline:\n  \
                 source: { type: postgres, config: { connection_url: \"postgres://x\", \
                 query: \"select 1\" } }\n  \
                 sink: { type: postgres, config: { connection_url: \"postgres://y\", \
                 table_name: t, column_mapping: auto_map, write_mode: upsert, key: [id] } }\n",
            )
            .await;
            assert!(at_least_once_risky_sinks(&upsert, true, true).is_empty());

            // exactly_once delivery → not flagged even with an append sink.
            let eo = loaded(
                "version: 1\ndelivery: exactly_once\npipeline:\n  \
                 source: { type: postgres-cdc, config: {} }\n  \
                 sink: { type: sqlite, config: {} }\n  \
                 state: { type: file, config: { path: \"/tmp/x.json\" } }\n",
            )
            .await;
            assert!(at_least_once_risky_sinks(&eo, true, false).is_empty());
        }

        async fn seed_sharded_with_shards(state: &ServerState, run_id: &str, n: usize) {
            use crate::serve::history::ShardInsert;
            seed_run(state, run_id, RunStatus::Sharded).await;
            let shards: Vec<ShardInsert> = (0..n)
                .map(|i| ShardInsert {
                    shard_id: i.to_string(),
                    descriptor: serde_json::json!({ "i": i }),
                    size_estimate: None,
                })
                .collect();
            state
                .history()
                .insert_shards(run_id, &shards)
                .await
                .unwrap();
            // Claim them so they are 'running' and finalizable by this instance.
            let claimed = state.history().claim_shards(n).await.unwrap();
            assert_eq!(claimed.len(), n);
        }

        #[tokio::test]
        async fn maybe_finalize_parent_completes_when_all_shards_succeed() {
            let dir = tempfile::tempdir().unwrap();
            let state = sqlite_state(dir.path()).await;
            seed_sharded_with_shards(&state, "r", 3).await;
            for i in 0..3 {
                state
                    .history()
                    .finalize_shard(
                        "r",
                        &i.to_string(),
                        crate::serve::history::ShardOutcome::Completed,
                    )
                    .await
                    .unwrap();
            }
            maybe_finalize_parent(&state, "r").await;
            assert_eq!(
                state.history().get("r").await.unwrap().unwrap().status,
                RunStatus::Completed
            );
        }

        #[tokio::test]
        async fn maybe_finalize_parent_fails_when_a_shard_fails() {
            let dir = tempfile::tempdir().unwrap();
            let state = sqlite_state(dir.path()).await;
            seed_sharded_with_shards(&state, "r", 2).await;
            state
                .history()
                .finalize_shard("r", "0", crate::serve::history::ShardOutcome::Completed)
                .await
                .unwrap();
            state
                .history()
                .finalize_shard("r", "1", crate::serve::history::ShardOutcome::Failed)
                .await
                .unwrap();
            maybe_finalize_parent(&state, "r").await;
            assert_eq!(
                state.history().get("r").await.unwrap().unwrap().status,
                RunStatus::Failed
            );
        }

        #[tokio::test]
        async fn maybe_finalize_parent_keeps_sharded_until_all_terminal() {
            let dir = tempfile::tempdir().unwrap();
            let state = sqlite_state(dir.path()).await;
            seed_sharded_with_shards(&state, "r", 2).await;
            // Only one shard finalized → run stays Sharded.
            state
                .history()
                .finalize_shard("r", "0", crate::serve::history::ShardOutcome::Completed)
                .await
                .unwrap();
            maybe_finalize_parent(&state, "r").await;
            assert_eq!(
                state.history().get("r").await.unwrap().unwrap().status,
                RunStatus::Sharded
            );
        }

        #[tokio::test]
        async fn finalize_sharded_parent_is_status_fenced_and_idempotent() {
            // F45: the status-fenced finalize transitions the parent exactly
            // once. A concurrent second finalize (e.g. two shards finishing on
            // two instances) is a no-op, and a non-Sharded run is never touched.
            let dir = tempfile::tempdir().unwrap();
            let state = sqlite_state(dir.path()).await;
            seed_sharded_with_shards(&state, "r", 2).await;

            let first = state
                .history()
                .finalize_sharded_parent("r", RunStatus::Completed, Utc::now(), None)
                .await
                .unwrap();
            assert!(first, "first finalize wins");
            assert_eq!(
                state.history().get("r").await.unwrap().unwrap().status,
                RunStatus::Completed
            );

            // Second finalize sees a non-Sharded parent → no-op, status unchanged.
            let second = state
                .history()
                .finalize_sharded_parent("r", RunStatus::Failed, Utc::now(), Some("late".into()))
                .await
                .unwrap();
            assert!(!second, "second finalize is a no-op");
            let r = state.history().get("r").await.unwrap().unwrap();
            assert_eq!(r.status, RunStatus::Completed, "status not overwritten");
            assert!(r.error.is_none(), "late error must not be stamped");

            // An unknown / never-sharded run id is not finalized.
            let missing = state
                .history()
                .finalize_sharded_parent("does-not-exist", RunStatus::Completed, Utc::now(), None)
                .await
                .unwrap();
            assert!(!missing);
        }

        #[tokio::test]
        async fn execute_shard_runs_a_csv_to_jsonl_shard() {
            let dir = tempfile::tempdir().unwrap();
            let state = sqlite_state(dir.path()).await;
            let input = dir.path().join("in.csv");
            std::fs::write(&input, "id,name\n1,alice\n2,bob\n").unwrap();
            let output = dir.path().join("out.jsonl");
            let yaml = format!(
                "version: 1\npipeline:\n  \
                 source: {{ type: csv, config: {{ path: \"{}\" }} }}\n  \
                 sink: {{ type: jsonl, config: {{ path: \"{}\" }} }}\n",
                input.display(),
                output.display()
            );
            let l = loaded(&yaml).await;
            // The whole-dataset shard is a no-op for the (non-shardable) csv source,
            // exercising the apply_shard call + per-shard state-key path end-to-end.
            let ok = execute_shard(
                &state,
                &crate::runtime::Principal::local(),
                l,
                "r",
                "0",
                ShardSpec::whole(),
                CancellationToken::new(),
                None,
                None,
                None,
                Utc::now(),
            )
            .await
            .0;
            assert_eq!(
                ok,
                ShardOutcome::Completed,
                "csv→jsonl shard should complete"
            );
            let written = std::fs::read_to_string(&output).unwrap();
            assert_eq!(written.lines().count(), 2, "both rows written");
            assert!(written.contains("alice") && written.contains("bob"));
        }

        #[tokio::test]
        async fn resume_claimed_shard_executes_and_finalizes_parent() {
            // End-to-end per-shard entry point: claim a shard whose run is a
            // csv→jsonl pipeline, dispatch it, and confirm the shard runs, is
            // finalized, and the parent run flips to Completed.
            let dir = tempfile::tempdir().unwrap();
            let state = sqlite_state(dir.path()).await;
            let input = dir.path().join("in.csv");
            std::fs::write(&input, "id,name\n1,alice\n").unwrap();
            let output = dir.path().join("out.jsonl");
            let yaml = format!(
                "version: 1\npipeline:\n  \
                 source: {{ type: csv, config: {{ path: \"{}\" }} }}\n  \
                 sink: {{ type: jsonl, config: {{ path: \"{}\" }} }}\n",
                input.display(),
                output.display()
            );
            // Seed the parent Sharded run carrying the pipeline config, + one shard.
            let mut rec = RunRecord::queued("r".into(), None, BTreeMap::new(), None, Utc::now());
            rec.status = RunStatus::Sharded;
            rec.config_body = Some(yaml);
            state.history().upsert(&rec).await.unwrap();
            use crate::serve::history::ShardInsert;
            state
                .history()
                .insert_shards(
                    "r",
                    &[ShardInsert {
                        shard_id: "0".into(),
                        descriptor: serde_json::Value::Null,
                        size_estimate: None,
                    }],
                )
                .await
                .unwrap();
            let claimed = state.history().claim_shards(1).await.unwrap();
            assert_eq!(claimed.len(), 1);

            resume_claimed_shard(state.clone(), claimed.into_iter().next().unwrap());

            // Poll until the parent run reaches a terminal state (the spawned
            // task runs the shard, finalizes it, then finalizes the parent).
            let mut status = RunStatus::Sharded;
            for _ in 0..100 {
                tokio::time::sleep(Duration::from_millis(50)).await;
                status = state.history().get("r").await.unwrap().unwrap().status;
                if status.is_terminal() {
                    break;
                }
            }
            assert_eq!(status, RunStatus::Completed, "shard ran → parent completed");
            assert!(output.exists(), "shard wrote its output");
        }

        #[cfg(feature = "source-webhook")]
        fn waiting_yaml(dir: &std::path::Path) -> String {
            format!(
                "version: 1\npipeline:\n  \
                 source: {{ type: webhook, config: {{ listen_addr: \"127.0.0.1:0\", timeout_secs: 60 }} }}\n  \
                 sink: {{ type: jsonl, config: {{ path: \"{}\" }} }}\n",
                dir.join("out.jsonl").display()
            )
        }

        #[cfg(feature = "source-webhook")]
        #[tokio::test]
        async fn a_shard_stops_cleanly_on_a_remote_cancel_a_timeout_or_a_shutdown() {
            let dir = tempfile::tempdir().unwrap();
            let state = sqlite_state(dir.path()).await;
            let yaml = waiting_yaml(dir.path());
            let run = |coop: CancellationToken, timeout: Option<u64>| {
                let state = state.clone();
                let yaml = yaml.clone();
                async move {
                    execute_shard(
                        &state,
                        &crate::runtime::Principal::local(),
                        loaded(&yaml).await,
                        "r",
                        "0",
                        ShardSpec::whole(),
                        coop,
                        timeout,
                        None,
                        None,
                        Utc::now(),
                    )
                    .await
                }
            };
            let coop = CancellationToken::new();
            let fire = coop.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(300)).await;
                fire.cancel();
            });
            assert_ne!(run(coop, None).await, (ShardOutcome::Failed, false));
            assert_eq!(
                run(CancellationToken::new(), Some(1)).await,
                (ShardOutcome::Failed, false)
            );
            let shutdown = state.shutdown_token();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(300)).await;
                shutdown.cancel();
            });
            assert_eq!(
                run(CancellationToken::new(), None).await,
                (ShardOutcome::Failed, true)
            );
        }

        /// #862 M-003: a clustered shutdown hands a claimed shard back as
        /// pending instead of failing it (and with it the parent run).
        #[cfg(feature = "source-webhook")]
        #[tokio::test]
        async fn a_clustered_shutdown_hands_a_claimed_shard_back_as_pending() {
            use crate::serve::history::ShardInsert;
            let dir = tempfile::tempdir().unwrap();
            let mut cluster = crate::serve::cluster::ClusterConfig::disabled();
            cluster.enabled = true;
            let state = sqlite_state_with(dir.path(), cluster).await;
            let mut rec = RunRecord::queued("r".into(), None, BTreeMap::new(), None, Utc::now());
            rec.status = RunStatus::Sharded;
            rec.config_body = Some(waiting_yaml(dir.path()));
            state.history().upsert(&rec).await.unwrap();
            let shards = ["0", "1"].map(|id| ShardInsert {
                shard_id: id.into(),
                descriptor: serde_json::Value::Null,
                size_estimate: None,
            });
            state.history().insert_shards("r", &shards).await.unwrap();
            let claimed = state.history().claim_shards(1).await.unwrap();
            assert_eq!(claimed.len(), 1);
            resume_claimed_shard(state.clone(), claimed.into_iter().next().unwrap());
            tokio::time::sleep(Duration::from_millis(500)).await;
            state.shutdown_token().cancel();
            let mut progress = state.history().shard_progress("r").await.unwrap();
            for _ in 0..200 {
                tokio::time::sleep(Duration::from_millis(50)).await;
                progress = state.history().shard_progress("r").await.unwrap();
                if progress.running == 0 {
                    break;
                }
            }
            assert_eq!(progress.pending, 2, "{progress:?}");
            assert_eq!(progress.failed, 0, "{progress:?}");
            let parent = state.history().get("r").await.unwrap().unwrap();
            assert_eq!(parent.status, RunStatus::Sharded, "the parent stays open");
            assert_eq!(state.history().claim_shards(2).await.unwrap().len(), 2);
            assert!(!state.history().release_shard("r", "nope").await.unwrap());
        }

        #[cfg(feature = "source-webhook")]
        #[tokio::test]
        async fn a_clustered_shutdown_hands_a_claimed_run_back_as_pending() {
            let dir = tempfile::tempdir().unwrap();
            let mut cluster = crate::serve::cluster::ClusterConfig::disabled();
            cluster.enabled = true;
            let state = sqlite_state_with(dir.path(), cluster).await;
            let mut rec = RunRecord::queued("r".into(), None, BTreeMap::new(), None, Utc::now());
            rec.status = RunStatus::Pending;
            rec.config_body = Some(waiting_yaml(dir.path()));
            state.history().upsert(&rec).await.unwrap();
            let claimed = state.history().claim_pending(1).await.unwrap();
            assert_eq!(claimed.len(), 1);
            resume_claimed_run(state.clone(), claimed.into_iter().next().unwrap());
            let mut status = RunStatus::Pending;
            for _ in 0..100 {
                tokio::time::sleep(Duration::from_millis(50)).await;
                status = state.history().get("r").await.unwrap().unwrap().status;
                if status == RunStatus::Running {
                    break;
                }
            }
            assert_eq!(status, RunStatus::Running);
            tokio::time::sleep(Duration::from_millis(300)).await;
            state.shutdown_token().cancel();
            for _ in 0..200 {
                tokio::time::sleep(Duration::from_millis(50)).await;
                status = state.history().get("r").await.unwrap().unwrap().status;
                if status == RunStatus::Pending {
                    break;
                }
            }
            assert_eq!(status, RunStatus::Pending, "handed back to the cluster");
        }

        #[tokio::test]
        async fn resume_claimed_shard_with_no_config_fails_the_shard() {
            // A claimed shard whose parent run has no config_body can't run →
            // the shard is finalized failed and the parent run fails.
            let dir = tempfile::tempdir().unwrap();
            let state = sqlite_state(dir.path()).await;
            let mut rec = RunRecord::queued("r".into(), None, BTreeMap::new(), None, Utc::now());
            rec.status = RunStatus::Sharded; // config_body intentionally None
            state.history().upsert(&rec).await.unwrap();
            use crate::serve::history::ShardInsert;
            state
                .history()
                .insert_shards(
                    "r",
                    &[ShardInsert {
                        shard_id: "0".into(),
                        descriptor: serde_json::Value::Null,
                        size_estimate: None,
                    }],
                )
                .await
                .unwrap();
            let claimed = state.history().claim_shards(1).await.unwrap();
            resume_claimed_shard(state.clone(), claimed.into_iter().next().unwrap());

            let mut status = RunStatus::Sharded;
            for _ in 0..100 {
                tokio::time::sleep(Duration::from_millis(50)).await;
                status = state.history().get("r").await.unwrap().unwrap().status;
                if status.is_terminal() {
                    break;
                }
            }
            assert_eq!(status, RunStatus::Failed, "no-config shard → parent failed");
        }

        #[tokio::test]
        async fn coordinate_returns_err_when_source_build_fails() {
            // A shardable-looking source with an invalid config fails to build →
            // coordinate_sharded_run surfaces the error (the caller fails the run).
            let dir = tempfile::tempdir().unwrap();
            let state = sqlite_state(dir.path()).await;
            // s3 missing the required `file_format` field → build_source errors.
            let l = loaded(
                "version: 1\npipeline:\n  \
                 source: { type: s3, config: { bucket: b } }\n  \
                 sink: { type: stdout, config: {} }\n",
            )
            .await;
            assert!(coordinate_sharded_run(&state, "r", &l, 4).await.is_err());
        }

        #[tokio::test]
        async fn execute_shard_returns_false_on_malformed_resilience() {
            // A resilience block that parses but fails to compile makes
            // execute_shard fail fast (false) before running the pipeline.
            let dir = tempfile::tempdir().unwrap();
            let state = sqlite_state(dir.path()).await;
            let input = dir.path().join("in.csv");
            std::fs::write(&input, "id\n1\n").unwrap();
            let yaml = format!(
                "version: 1\nresilience:\n  retry:\n    max_attempts: 0\npipeline:\n  \
                 source: {{ type: csv, config: {{ path: \"{}\" }} }}\n  \
                 sink: {{ type: stdout, config: {{}} }}\n",
                input.display()
            );
            let l = loaded(&yaml).await;
            let ok = execute_shard(
                &state,
                &crate::runtime::Principal::local(),
                l,
                "r",
                "0",
                ShardSpec::whole(),
                CancellationToken::new(),
                None,
                None,
                None,
                Utc::now(),
            )
            .await
            .0;
            assert_eq!(
                ok,
                ShardOutcome::Failed,
                "malformed resilience → shard fails fast"
            );
        }

        #[tokio::test]
        async fn resume_claimed_shard_with_unloadable_config_fails_the_shard() {
            // A claimed shard whose run config fails to re-load (malformed body)
            // exercises resume_claimed_shard's load-error branch → shard failed.
            let dir = tempfile::tempdir().unwrap();
            let state = sqlite_state(dir.path()).await;
            let mut rec = RunRecord::queued("r".into(), None, BTreeMap::new(), None, Utc::now());
            rec.status = RunStatus::Sharded;
            rec.config_body = Some("this: is: not: valid: yaml: [".into());
            state.history().upsert(&rec).await.unwrap();
            use crate::serve::history::ShardInsert;
            state
                .history()
                .insert_shards(
                    "r",
                    &[ShardInsert {
                        shard_id: "0".into(),
                        descriptor: serde_json::Value::Null,
                        size_estimate: None,
                    }],
                )
                .await
                .unwrap();
            let claimed = state.history().claim_shards(1).await.unwrap();
            resume_claimed_shard(state.clone(), claimed.into_iter().next().unwrap());

            let mut status = RunStatus::Sharded;
            for _ in 0..100 {
                tokio::time::sleep(Duration::from_millis(50)).await;
                status = state.history().get("r").await.unwrap().unwrap().status;
                if status.is_terminal() {
                    break;
                }
            }
            assert_eq!(
                status,
                RunStatus::Failed,
                "unloadable config → parent failed"
            );
        }

        // ── F10: cross-instance cancel of a Sharded run ──────────────────────

        #[tokio::test]
        async fn request_cancel_flags_a_sharded_parent() {
            // F10: a Sharded parent (status 'sharded') must accept request_cancel
            // (the guard was broadened from 'running' to ('running','sharded')).
            let dir = tempfile::tempdir().unwrap();
            let state = sqlite_state(dir.path()).await;
            seed_run(&state, "r", RunStatus::Sharded).await;
            // Insert + claim a shard so this instance owns a running shard under r,
            // then flag the parent (a cancel-requested parent's shards are never
            // claimed, so the claim has to come first).
            use crate::serve::history::ShardInsert;
            state
                .history()
                .insert_shards(
                    "r",
                    &[ShardInsert {
                        shard_id: "0".into(),
                        descriptor: serde_json::Value::Null,
                        size_estimate: None,
                    }],
                )
                .await
                .unwrap();
            let claimed = state.history().claim_shards(1).await.unwrap();
            assert_eq!(claimed.len(), 1, "shard claimed (running, owned)");
            state.history().request_cancel("r").await.unwrap();

            let flagged = state.history().pending_shard_cancellations().await.unwrap();
            assert_eq!(
                flagged,
                vec!["r".to_string()],
                "the flagged sharded parent's run id is returned for its running shard"
            );
        }

        #[tokio::test]
        async fn pending_shard_cancellations_filters_unflagged_and_pending_shards() {
            let dir = tempfile::tempdir().unwrap();
            let state = sqlite_state(dir.path()).await;
            use crate::serve::history::ShardInsert;
            let one = |id: &str| {
                vec![ShardInsert {
                    shard_id: id.into(),
                    descriptor: serde_json::Value::Null,
                    size_estimate: None,
                }]
            };

            // Runs A (flagged) and C (NOT flagged) each get one shard; claim both
            // so they are running+owned here.
            seed_run(&state, "A", RunStatus::Sharded).await;
            state.history().insert_shards("A", &one("0")).await.unwrap();
            seed_run(&state, "C", RunStatus::Sharded).await;
            state.history().insert_shards("C", &one("0")).await.unwrap();
            let claimed = state.history().claim_shards(8).await.unwrap();
            assert_eq!(claimed.len(), 2, "A and C shards claimed (running)");
            state.history().request_cancel("A").await.unwrap();

            // Run B: flagged, but its shard stays PENDING (inserted after the
            // claim, never claimed) → must be excluded (the join requires a
            // 'running' shard owned by this instance).
            seed_run(&state, "B", RunStatus::Sharded).await;
            state.history().request_cancel("B").await.unwrap();
            state.history().insert_shards("B", &one("0")).await.unwrap();

            let flagged = state.history().pending_shard_cancellations().await.unwrap();
            assert_eq!(
                flagged,
                vec!["A".to_string()],
                "only A (flagged + a running owned shard); B pending-shard, C unflagged"
            );
        }

        // ── F11: orphaned-Sharded-parent sweep ───────────────────────────────

        #[tokio::test]
        async fn finalize_sweep_completes_an_all_success_sharded_parent() {
            let dir = tempfile::tempdir().unwrap();
            let state = sqlite_state(dir.path()).await;
            seed_sharded_with_shards(&state, "r", 3).await;
            for i in 0..3 {
                state
                    .history()
                    .finalize_shard(
                        "r",
                        &i.to_string(),
                        crate::serve::history::ShardOutcome::Completed,
                    )
                    .await
                    .unwrap();
            }
            // No inline maybe_finalize_parent — the sweep must finalize it.
            let n = state
                .history()
                .finalize_completed_sharded_parents()
                .await
                .unwrap();
            assert_eq!(n, 1, "one sharded parent finalized");
            let rec = state.history().get("r").await.unwrap().unwrap();
            assert_eq!(rec.status, RunStatus::Completed);
            assert!(rec.finished_at.is_some());
            assert!(rec.error.is_none());

            // Idempotent: a second sweep finalizes nothing.
            assert_eq!(
                state
                    .history()
                    .finalize_completed_sharded_parents()
                    .await
                    .unwrap(),
                0,
                "already-terminal parent is not re-finalized"
            );
        }

        #[tokio::test]
        async fn finalize_sweep_fails_a_parent_with_a_failed_shard() {
            let dir = tempfile::tempdir().unwrap();
            let state = sqlite_state(dir.path()).await;
            seed_sharded_with_shards(&state, "r", 3).await;
            state
                .history()
                .finalize_shard("r", "0", crate::serve::history::ShardOutcome::Completed)
                .await
                .unwrap();
            state
                .history()
                .finalize_shard("r", "1", crate::serve::history::ShardOutcome::Failed)
                .await
                .unwrap();
            state
                .history()
                .finalize_shard("r", "2", crate::serve::history::ShardOutcome::Completed)
                .await
                .unwrap();
            let n = state
                .history()
                .finalize_completed_sharded_parents()
                .await
                .unwrap();
            assert_eq!(n, 1);
            let rec = state.history().get("r").await.unwrap().unwrap();
            assert_eq!(rec.status, RunStatus::Failed);
            assert!(rec.finished_at.is_some());
            assert_eq!(rec.error.as_deref(), Some("1/3 shard(s) failed"));
        }

        #[tokio::test]
        async fn finalize_sweep_leaves_a_not_all_terminal_parent_sharded() {
            let dir = tempfile::tempdir().unwrap();
            let state = sqlite_state(dir.path()).await;
            seed_sharded_with_shards(&state, "r", 2).await;
            // Only one shard terminal → the parent stays sharded.
            state
                .history()
                .finalize_shard("r", "0", crate::serve::history::ShardOutcome::Completed)
                .await
                .unwrap();
            let n = state
                .history()
                .finalize_completed_sharded_parents()
                .await
                .unwrap();
            assert_eq!(n, 0, "parent with a still-running shard is not finalized");
            assert_eq!(
                state.history().get("r").await.unwrap().unwrap().status,
                RunStatus::Sharded
            );
        }
    }

    #[test]
    fn request_budgets_fold_into_the_config() {
        let req = |config: &str, fmt: &str, budget: serde_json::Value| -> SubmitRequest {
            serde_json::from_value(serde_json::json!({
                "config": config, "config_format": fmt, "budget": budget
            }))
            .unwrap()
        };
        let none: SubmitRequest =
            serde_json::from_value(serde_json::json!({"config": "a: 1"})).unwrap();
        assert_eq!(apply_request_budget(none).unwrap().config, "a: 1");

        let out = apply_request_budget(req(
            "budget:\n  max_records: 5\n",
            "yaml",
            serde_json::json!({"max_records": 50, "max_bytes": 10}),
        ))
        .unwrap();
        let doc: serde_json::Value = serde_json::from_str(&out.config).unwrap();
        assert_eq!(doc["budget"]["max_records"], 5);
        assert_eq!(doc["budget"]["max_bytes"], 10);

        let out =
            apply_request_budget(req("{}", "json", serde_json::json!({"max_records": 3}))).unwrap();
        assert!(out.config.contains("\"max_records\":3"));

        for (config, fmt, budget, needle) in [
            (
                "{",
                "yaml",
                serde_json::json!({"max_records": 1}),
                "invalid YAML",
            ),
            (
                "{",
                "json",
                serde_json::json!({"max_records": 1}),
                "invalid JSON",
            ),
            (
                "[1]",
                "json",
                serde_json::json!({"max_records": 1}),
                "mapping",
            ),
            (
                "budget: 7",
                "yaml",
                serde_json::json!({"max_records": 1}),
                "budget",
            ),
            ("{}", "json", serde_json::json!({"max_records": 0}), ""),
        ] {
            let e = apply_request_budget(req(config, fmt, budget)).unwrap_err();
            assert!(
                matches!(&e, ServeError::BadConfig(m) if m.contains(needle)),
                "{e:?}"
            );
        }
    }
}
