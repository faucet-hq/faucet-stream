//! `/v1/dlq/*` HTTP handlers — inspect, replay, and discard dead-letter-queue
//! envelopes over the control plane. Thin glue over [`crate::dlq_replay`]:
//! deserialize, call the orchestration, map errors to status codes.
//!
//! The DLQ location is a **server-local** path, so `inspect` needs `DlqRead`
//! (viewer) and `replay` / `discard` need `DlqManage` (operator) — see
//! [`crate::serve::rbac::required_permission`].

use crate::auth_catalog::build_auth_catalog;
use crate::dlq_replay::{self, ReplayInputs, reader::DlqDecryptor};
use crate::error::CliError;
use crate::serve::error::ServeError;
use crate::serve::history::{RunRecord, RunStatus};
use crate::serve::load::refuse_request_path;
use crate::serve::rbac::AuthContext;
use crate::serve::runner::ConfigFormatWire;
use crate::serve::state::ServerState;
use axum::Json;
use axum::extract::{Extension, State};
use chrono::Utc;
use serde::Deserialize;
use serde_json::Value;

/// Map a CLI-layer error to an HTTP-facing one: a bad reason / path / config is
/// a client error (400); anything else is internal (500).
fn cli_to_serve(e: CliError) -> ServeError {
    match e {
        CliError::Config(m) => ServeError::BadConfig(m),
        other => ServeError::Internal(other.to_string()),
    }
}

fn default_sample_limit() -> usize {
    5
}

/// `POST /v1/dlq/inspect` request body.
#[derive(Debug, Deserialize)]
pub struct DlqInspectRequest {
    pub location: String,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default = "default_sample_limit")]
    pub limit: usize,
    /// Keys for a DLQ sealed at rest (first = current, rest = rotated).
    #[serde(default)]
    pub encryption_keys: Vec<String>,
}

/// `POST /v1/dlq/inspect` → 200 with the grouped
/// [`InspectSummary`](crate::dlq_replay::InspectSummary). Read-only.
pub async fn inspect(
    State(_state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Json(req): Json<DlqInspectRequest>,
) -> Result<Json<Value>, ServeError> {
    refuse_request_path("location", &req.location, false, actor.tenant.as_deref())?;
    let location = req.location;
    let reason = req.reason;
    let limit = req.limit;
    let dec = DlqDecryptor::from_keys(&req.encryption_keys)
        .map_err(CliError::from)
        .map_err(cli_to_serve)?;
    let summary = tokio::task::spawn_blocking(move || {
        dlq_replay::inspect(&location, reason.as_deref(), limit, &dec)
    })
    .await
    .map_err(|e| ServeError::Internal(format!("dlq inspect task: {e}")))?
    .map_err(cli_to_serve)?;
    Ok(Json(serde_json::to_value(summary).unwrap_or(Value::Null)))
}

/// `POST /v1/dlq/replay` request body.
#[derive(Debug, Deserialize)]
pub struct DlqReplayRequest {
    /// The pipeline config text whose sink / transforms / quality / contract
    /// replayed records flow through.
    pub config: String,
    #[serde(default)]
    pub config_format: ConfigFormatWire,
    /// DLQ location to replay from.
    pub from: String,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub failed_dlq: Option<String>,
    #[serde(default)]
    pub row: Option<String>,
    #[serde(default)]
    pub dry_run: bool,
    /// Keys for a DLQ sealed at rest (first = current, rest = rotated). When
    /// empty, the config's own dlq jsonl `encryption` block is used.
    #[serde(default)]
    pub encryption_keys: Vec<String>,
    /// An id for this replay, so `POST /v1/runs/{id}/cancel` can stop it
    /// while it runs (#871). Generated (`dlq-replay-<uuid>`) when omitted and
    /// returned as `id`.
    #[serde(default)]
    pub id: Option<String>,
    /// Replay on behalf of this tenant (#709, #871): the config loads under
    /// the tenant's scope (its connections, `${tenant.*}`) and the replay is
    /// admitted against the tenant's limits, whose per-run ceilings bound it.
    #[serde(default)]
    pub tenant: Option<String>,
}

/// Removes a replay's cancel token from the registry when the request ends.
struct ReplayToken<'a> {
    state: &'a ServerState,
    id: String,
    claim: u64,
}

impl Drop for ReplayToken<'_> {
    fn drop(&mut self) {
        self.state.registry().release_claim(&self.id, self.claim);
    }
}

/// `POST /v1/dlq/replay` → 200 with the
/// [`ReplayOutcome`](crate::dlq_replay::ReplayOutcome). Operator-only;
/// audited as `dlq.replay`.
pub async fn replay(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Json(req): Json<DlqReplayRequest>,
) -> Result<Json<Value>, ServeError> {
    let tenant = match (&actor.tenant, &req.tenant) {
        (Some(own), Some(asked)) if own != asked => return Err(ServeError::NotFound),
        (own, asked) => own.clone().or_else(|| asked.clone()),
    };
    refuse_request_path("from", &req.from, false, tenant.as_deref())?;
    if let Some(failed) = &req.failed_dlq {
        refuse_request_path("failed_dlq", failed, true, tenant.as_deref())?;
    }
    // A tenant's replay loads under its scope and is admitted against its
    // limits, whose per-run ceilings join the config's budget (#871).
    let (admission, caller_budget) = match tenant.as_deref() {
        Some(t) => {
            let (a, b) = admit_replay(&state, t).await?;
            (Some(a), b)
        }
        None => (None, None),
    };
    let loaded = crate::serve::runner::load_for(
        &state,
        &req.config,
        req.config_format.into(),
        tenant.as_deref(),
        state.caller_origin(),
    )
    .await?;
    if !req.dry_run {
        crate::serve::runner::refuse_unapproved_write(&state, "DLQ replay")?;
        crate::serve::runner::policy_gate(&state, &actor, &loaded).await?;
    }
    let _permit = crate::serve::runner::inline_permit(&state)?;
    let auth = build_auth_catalog(loaded.cfg.auth.as_ref()).map_err(cli_to_serve)?;
    let pipeline_name = loaded
        .cfg
        .name
        .clone()
        .unwrap_or_else(|| "dlq-replay".to_string());

    // Cancellable like a run (#871): the token is registered under the
    // replay's id and stops with the server.
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
    let _token = ReplayToken {
        state: &state,
        id: id.clone(),
        claim,
    };
    let decryptor = DlqDecryptor::from_keys(&req.encryption_keys)
        .map_err(CliError::from)
        .map_err(cli_to_serve)?;
    // A replay that writes is a run (#871): recorded while it runs, so it
    // holds one of its tenant's `max_concurrent_runs` slots on every instance.
    let started = if req.dry_run {
        drop(admission);
        None
    } else {
        Some(start_replay_record(&state, &id, &req.from, tenant.as_deref(), admission).await?)
    };

    let outcome = dlq_replay::replay(
        &loaded.cfg,
        &req.from,
        ReplayInputs {
            reason: req.reason.as_deref(),
            failed_dlq: req.failed_dlq.as_deref(),
            row: req.row.as_deref(),
            dry_run: req.dry_run,
            pipeline_name,
            execution: loaded.cfg.execution.clone(),
            auth,
            clock: Utc::now().fixed_offset(),
            decryptor,
            budget: caller_budget,
            cancel: Some(cancel.clone()),
        },
    )
    .await;

    if let Some(rec) = started {
        rec.finish(&outcome, cancel.is_cancelled()).await;
    }
    let result_label = if outcome.is_ok() { "ok" } else { "error" };
    crate::serve::audit::write(&state, &actor, "dlq.replay", None, None, result_label).await;

    let outcome = outcome.map_err(cli_to_serve)?;
    let mut body = serde_json::to_value(outcome).unwrap_or(Value::Null);
    if let Some(obj) = body.as_object_mut() {
        obj.insert("id".into(), Value::String(id));
        if let Some(t) = tenant {
            obj.insert("tenant".into(), Value::String(t));
        }
    }
    Ok(Json(body))
}

/// A tenant's admission guard; nothing without the `tenants` feature.
#[cfg(feature = "tenants")]
type ReplayAdmission = crate::serve::tenants::Admission;
#[cfg(not(feature = "tenants"))]
type ReplayAdmission = ();

/// Admit a replay for `tenant`: it exists, is not suspended and is under its
/// concurrency limit; its per-run ceilings are returned as the replay's budget.
/// The guard serializes the tenant's admissions until the replay is recorded.
#[cfg(feature = "tenants")]
async fn admit_replay(
    state: &ServerState,
    tenant: &str,
) -> Result<(ReplayAdmission, Option<faucet_core::BudgetSpec>), ServeError> {
    crate::serve::tenants::admit_inline(state, tenant).await
}

#[cfg(not(feature = "tenants"))]
async fn admit_replay(
    _state: &ServerState,
    tenant: &str,
) -> Result<(ReplayAdmission, Option<faucet_core::BudgetSpec>), ServeError> {
    Err(ServeError::BadConfig(format!(
        "replay for tenant '{tenant}' refused: this server was built without the `tenants` feature"
    )))
}

/// Label on the run record of a DLQ replay.
pub const LABEL_DLQ_REPLAY: &str = "dlq_replay";

/// Record a replay as a running run under its id, owned by `tenant`, then
/// release the admission guard. In cluster mode the tenant's concurrency is
/// re-counted across instances and an over-limit replay is refused (429).
async fn start_replay_record(
    state: &ServerState,
    id: &str,
    from: &str,
    tenant: Option<&str>,
    admission: Option<ReplayAdmission>,
) -> Result<ReplayRecord, ServeError> {
    let history = state.history();
    let now = Utc::now();
    let mut labels = std::collections::BTreeMap::new();
    labels.insert(LABEL_DLQ_REPLAY.to_string(), "true".to_string());
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
    let guard = ReplayRecord {
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

/// A replay's run record while it runs. Finished explicitly; dropped
/// unfinished (a refused or abandoned request), it is closed as cancelled so
/// it never holds a tenant slot.
struct ReplayRecord {
    state: ServerState,
    rec: Option<RunRecord>,
}

impl ReplayRecord {
    /// Write the replay's terminal state. A failed write is logged: the
    /// replay itself already finished.
    async fn finish(
        mut self,
        outcome: &Result<dlq_replay::ReplayOutcome, CliError>,
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
            Err(e) => rec.error = Some(crate::secrets::registry::redact_message(&e.to_string())),
        }
        close(&self.state, rec).await;
    }
}

impl Drop for ReplayRecord {
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

/// `POST /v1/dlq/discard` request body.
#[derive(Debug, Deserialize)]
pub struct DlqDiscardRequest {
    pub location: String,
    #[serde(default)]
    pub reason: Option<String>,
    /// Only discard envelopes strictly older than this epoch-millis timestamp.
    #[serde(default)]
    pub before_ms: Option<i64>,
    /// Permanently delete instead of archiving to a `<file>.archived` sibling.
    #[serde(default)]
    pub delete: bool,
    /// Keys for a DLQ sealed at rest (first = current, rest = rotated).
    #[serde(default)]
    pub encryption_keys: Vec<String>,
}

/// `POST /v1/dlq/discard` → 200 with the
/// [`DiscardOutcome`](crate::dlq_replay::DiscardOutcome). Operator-only;
/// audited as `dlq.discard`.
pub async fn discard(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Json(req): Json<DlqDiscardRequest>,
) -> Result<Json<Value>, ServeError> {
    refuse_request_path("location", &req.location, true, actor.tenant.as_deref())?;
    let location = req.location;
    let reason = req.reason;
    let before_ms = req.before_ms;
    let delete = req.delete;
    let dec = DlqDecryptor::from_keys(&req.encryption_keys)
        .map_err(CliError::from)
        .map_err(cli_to_serve)?;
    let outcome = tokio::task::spawn_blocking(move || {
        dlq_replay::discard(&location, reason.as_deref(), before_ms, delete, &dec)
    })
    .await
    .map_err(|e| ServeError::Internal(format!("dlq discard task: {e}")));

    let result_label = if matches!(&outcome, Ok(Ok(_))) {
        "ok"
    } else {
        "error"
    };
    crate::serve::audit::write(&state, &actor, "dlq.discard", None, None, result_label).await;

    let outcome = outcome?.map_err(cli_to_serve)?;
    Ok(Json(serde_json::to_value(outcome).unwrap_or(Value::Null)))
}

#[cfg(all(test, feature = "source-csv", feature = "sink-jsonl"))]
mod tests {
    use super::*;

    fn actor(tenant: Option<&str>) -> AuthContext {
        AuthContext {
            principal: "alice".into(),
            role: crate::serve::rbac::Role::Admin,
            source_ip: None,
            tenant: tenant.map(str::to_string),
        }
    }

    /// A DLQ file with `n` envelopes and a config replaying it into `out`.
    fn fixture(dir: &std::path::Path, n: usize, budget: &str) -> (String, String) {
        let dlq = dir.join("dlq.jsonl");
        let lines: Vec<String> = (0..n)
            .map(|i| {
                serde_json::json!({
                    "error": {"kind": "Sink", "message": "boom"},
                    "reason": "partial",
                    "payload": {"id": i},
                    "ts_ms": 1, "sink": "jsonl", "pipeline": "p", "row": "", "record_index": i,
                })
                .to_string()
            })
            .collect();
        std::fs::write(&dlq, lines.join("\n") + "\n").unwrap();
        let cfg = format!(
            "version: 1\n{budget}pipeline:\n  source: {{ type: csv, config: {{ path: /dev/null }} }}\n  sink: {{ type: jsonl, config: {{ path: \"{}\" }} }}\n",
            dir.join("out.jsonl").display()
        );
        (cfg, dlq.display().to_string())
    }

    fn request(cfg: String, from: String) -> DlqReplayRequest {
        serde_json::from_value(serde_json::json!({
            "config": cfg, "from": from,
            "failed_dlq": format!("{from}.failed.jsonl"),
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn a_replay_reports_its_id_and_the_id_cannot_be_reused_while_it_runs() {
        let dir = tempfile::tempdir().unwrap();
        let (cfg, from) = fixture(dir.path(), 2, "");
        let state = crate::serve::test_support::test_state();
        let out = replay(
            State(state.clone()),
            Extension(actor(None)),
            Json(request(cfg.clone(), from.clone())),
        )
        .await
        .unwrap();
        assert!(out.0["id"].as_str().unwrap().starts_with("dlq-replay-"));
        assert_eq!(out.0["records_written"], 2);
        assert!(
            !state
                .registry()
                .is_registered(out.0["id"].as_str().unwrap())
        );

        state
            .registry()
            .register("busy".into(), tokio_util::sync::CancellationToken::new());
        let mut req = request(cfg, from);
        req.id = Some("busy".into());
        let err = replay(State(state.clone()), Extension(actor(None)), Json(req))
            .await
            .unwrap_err();
        assert!(matches!(err, ServeError::Conflict(_)), "{err:?}");
        state.registry().deregister("busy");
        assert!(!state.registry().is_registered("busy"));
    }

    /// #871: the replay runs on a token registered under its id, a child of
    /// the server's: cancelled, it stops at its first page boundary.
    #[tokio::test]
    async fn a_cancelled_replay_stops_before_writing() {
        let dir = tempfile::tempdir().unwrap();
        let (cfg, from) = fixture(dir.path(), 3, "");
        let state = crate::serve::test_support::test_state();
        state.shutdown_token().cancel();
        let out = replay(
            State(state),
            Extension(actor(None)),
            Json(request(cfg, from)),
        )
        .await
        .unwrap();
        assert_eq!(out.0["records_written"], 0);
    }

    /// #871: the config's own `budget:` bounds a replay.
    #[tokio::test]
    async fn a_replay_stops_at_the_config_budget() {
        let dir = tempfile::tempdir().unwrap();
        let (cfg, from) = fixture(dir.path(), 3, "budget: { max_records: 2 }\n");
        let state = crate::serve::test_support::test_state();
        let err = replay(
            State(state),
            Extension(actor(None)),
            Json(request(cfg, from)),
        )
        .await
        .unwrap_err();
        assert!(format!("{err:?}").contains("max_records"), "{err:?}");
    }

    /// #871: a tenant's replay is admitted against its limits, whose per-run
    /// ceiling bounds it; a suspended tenant's replay is refused.
    #[cfg(feature = "tenants")]
    #[tokio::test]
    async fn a_tenant_replay_counts_against_the_tenants_limits() {
        use crate::serve::history::tenants::TenantRecord;
        let dir = tempfile::tempdir().unwrap();
        let (cfg, from) = fixture(dir.path(), 3, "");
        let state = crate::serve::test_support::test_state();
        state.set_tenants(crate::serve::tenants::TenantsRuntime::new(
            Some(
                crate::serve::tenants::vault::Vault::new("0123456789abcdef0123456789abcdef", &[])
                    .unwrap(),
            ),
            Default::default(),
        ));
        let now = chrono::Utc::now();
        let mut t = TenantRecord {
            id: "acme".into(),
            name: None,
            labels: Default::default(),
            limits: Default::default(),
            notifications: Vec::new(),
            notifications_sealed: None,
            suspended: false,
            created_at: now,
            updated_at: now,
            created_by: "t".into(),
        };
        t.limits.max_records_per_run = Some(2);
        state.history().tenant_upsert(&t).await.unwrap();

        let mut req = request(cfg.clone(), from.clone());
        req.tenant = Some("acme".into());
        let err = replay(State(state.clone()), Extension(actor(None)), Json(req))
            .await
            .unwrap_err();
        assert!(format!("{err:?}").contains("max_records"), "{err:?}");

        // A principal confined to another tenant cannot name this one.
        let mut req = request(cfg.clone(), from.clone());
        req.tenant = Some("acme".into());
        let err = replay(
            State(state.clone()),
            Extension(actor(Some("other"))),
            Json(req),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ServeError::NotFound), "{err:?}");

        t.suspended = true;
        state.history().tenant_upsert(&t).await.unwrap();
        let mut req = request(cfg, from);
        req.tenant = Some("acme".into());
        let err = replay(State(state), Extension(actor(None)), Json(req))
            .await
            .unwrap_err();
        assert!(matches!(err, ServeError::Conflict(_)), "{err:?}");
    }

    #[cfg(feature = "tenants")]
    async fn tenant_state(max_concurrent_runs: Option<u32>) -> ServerState {
        use crate::serve::history::tenants::TenantRecord;
        let state = crate::serve::test_support::test_state();
        state.set_tenants(crate::serve::tenants::TenantsRuntime::new(
            Some(
                crate::serve::tenants::vault::Vault::new("0123456789abcdef0123456789abcdef", &[])
                    .unwrap(),
            ),
            Default::default(),
        ));
        let now = chrono::Utc::now();
        let mut t = TenantRecord {
            id: "acme".into(),
            name: None,
            labels: Default::default(),
            limits: Default::default(),
            notifications: Vec::new(),
            notifications_sealed: None,
            suspended: false,
            created_at: now,
            updated_at: now,
            created_by: "t".into(),
        };
        t.limits.max_concurrent_runs = max_concurrent_runs;
        state.history().tenant_upsert(&t).await.unwrap();
        state
    }

    /// #871: a running replay holds one of its tenant's `max_concurrent_runs`
    /// slots — a run submitted meanwhile is refused with 429 — and gives it
    /// back when it finishes.
    #[cfg(feature = "tenants")]
    #[tokio::test]
    async fn a_running_tenant_replay_holds_a_concurrency_slot() {
        let state = tenant_state(Some(1)).await;
        let (admission, _) = admit_replay(&state, "acme").await.unwrap();
        let running = start_replay_record(&state, "rp-1", "/dlq", Some("acme"), Some(admission))
            .await
            .unwrap();
        let rec = state.history().get("rp-1").await.unwrap().unwrap();
        assert_eq!(rec.status, RunStatus::Running);
        assert_eq!(rec.replay_of.as_deref(), Some("/dlq"));
        assert_eq!(rec.tenant.as_deref(), Some("acme"));
        assert_eq!(
            rec.labels.get(LABEL_DLQ_REPLAY).map(String::as_str),
            Some("true")
        );

        let req: crate::serve::runner::SubmitRequest =
            serde_json::from_value(serde_json::json!({"config": "x"})).unwrap();
        let err = crate::serve::runner::submit(state.clone(), req, actor(Some("acme")))
            .await
            .unwrap_err();
        assert!(matches!(err, ServeError::TooManyRequests(_)), "{err:?}");
        assert!(matches!(
            admit_replay(&state, "acme").await,
            Err(ServeError::TooManyRequests(_))
        ));

        let outcome = Ok(dlq_replay::ReplayOutcome {
            candidates: 2,
            records_written: 2,
            dry_run: false,
            failed_dlq: "/dlq.failed".into(),
        });
        running.finish(&outcome, false).await;
        let rec = state.history().get("rp-1").await.unwrap().unwrap();
        assert_eq!(rec.status, RunStatus::Completed);
        assert_eq!(rec.records_written, 2);
        assert!(rec.finished_at.is_some());
        drop(admit_replay(&state, "acme").await.unwrap());
    }

    /// #871: an end-to-end tenant replay leaves a completed run record.
    #[cfg(feature = "tenants")]
    #[tokio::test]
    async fn a_tenant_replay_is_recorded_as_a_run() {
        let dir = tempfile::tempdir().unwrap();
        let (cfg, from) = fixture(dir.path(), 2, "");
        let state = tenant_state(Some(1)).await;
        let mut req = request(cfg, from.clone());
        req.id = Some("rp-2".into());
        let out = replay(
            State(state.clone()),
            Extension(actor(Some("acme"))),
            Json(req),
        )
        .await
        .unwrap();
        assert_eq!(out.0["tenant"], "acme");
        let rec = state.history().get("rp-2").await.unwrap().unwrap();
        assert_eq!(rec.status, RunStatus::Completed);
        assert_eq!(rec.records_written, 2);
        assert_eq!(rec.replay_of.as_deref(), Some(from.as_str()));
        drop(admit_replay(&state, "acme").await.unwrap());

        // An id already in history is refused rather than overwritten.
        let (cfg, from) = fixture(dir.path(), 1, "");
        let mut req = request(cfg, from);
        req.id = Some("rp-2".into());
        let err = replay(State(state), Extension(actor(Some("acme"))), Json(req))
            .await
            .unwrap_err();
        assert!(matches!(err, ServeError::Conflict(_)), "{err:?}");
    }

    /// #871: a replay that fails, is cancelled, or whose request is abandoned
    /// closes its record, so it never keeps holding a slot.
    #[tokio::test]
    #[allow(clippy::disallowed_methods)]
    async fn an_unfinished_replay_record_is_closed() {
        let state = crate::serve::test_support::test_state();
        let failed = start_replay_record(&state, "rp-f", "/d", None, None)
            .await
            .unwrap();
        failed
            .finish(&Err(CliError::Config("bad".into())), false)
            .await;
        let rec = state.history().get("rp-f").await.unwrap().unwrap();
        assert_eq!(rec.status, RunStatus::Failed);
        assert!(rec.error.unwrap().contains("bad"));

        let cancelled = start_replay_record(&state, "rp-c", "/d", None, None)
            .await
            .unwrap();
        cancelled
            .finish(&Err(CliError::Config("stopped".into())), true)
            .await;
        let rec = state.history().get("rp-c").await.unwrap().unwrap();
        assert_eq!(rec.status, RunStatus::Cancelled);

        let abandoned = start_replay_record(&state, "rp-a", "/d", None, None)
            .await
            .unwrap();
        drop(abandoned);
        let mut status = RunStatus::Running;
        for _ in 0..200 {
            status = state.history().get("rp-a").await.unwrap().unwrap().status;
            if status != RunStatus::Running {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert_eq!(status, RunStatus::Cancelled);
    }

    /// The DLQ locations a request names are screened like connector paths:
    /// a system tree or a path outside the server's roots is refused before
    /// anything is read, archived or deleted.
    #[tokio::test]
    async fn dlq_locations_are_screened_by_the_reference_policy() {
        let state = crate::serve::test_support::test_state();
        let inspect_req: DlqInspectRequest =
            serde_json::from_value(serde_json::json!({"location": "/etc/passwd"})).unwrap();
        let err = inspect(
            State(state.clone()),
            Extension(actor(None)),
            Json(inspect_req),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ServeError::Forbidden(_)), "{err:?}");

        let victim = "/var/faucet-not-a-root/dlq.jsonl";
        let discard_req: DlqDiscardRequest =
            serde_json::from_value(serde_json::json!({"location": victim, "delete": true}))
                .unwrap();
        let err = discard(
            State(state.clone()),
            Extension(actor(None)),
            Json(discard_req),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(&err, ServeError::Forbidden(m) if m.contains("--allow-path-root")),
            "{err:?}"
        );

        let dir = tempfile::tempdir().unwrap();
        let (cfg, from) = fixture(dir.path(), 1, "");
        let mut req = request(cfg.clone(), "/etc/hosts".into());
        req.failed_dlq = None;
        let err = replay(State(state.clone()), Extension(actor(None)), Json(req))
            .await
            .unwrap_err();
        assert!(matches!(err, ServeError::Forbidden(_)), "{err:?}");
        let mut req = request(cfg, from);
        req.failed_dlq = Some("/etc/faucet-failed.jsonl".into());
        let err = replay(State(state.clone()), Extension(actor(None)), Json(req))
            .await
            .unwrap_err();
        assert!(
            matches!(&err, ServeError::Forbidden(m) if m.contains("failed_dlq")),
            "{err:?}"
        );
    }

    /// A replay id is claimed atomically: a replay whose id another holds is
    /// refused without releasing the holder's registration, and a replay whose
    /// id names a recorded run is refused and leaves nothing registered.
    #[tokio::test]
    async fn a_replay_id_is_claimed_atomically() {
        let dir = tempfile::tempdir().unwrap();
        let (cfg, from) = fixture(dir.path(), 1, "");
        let state = crate::serve::test_support::test_state();
        let held = state
            .registry()
            .try_register("dup".into(), tokio_util::sync::CancellationToken::new())
            .unwrap();
        let mut req = request(cfg.clone(), from.clone());
        req.id = Some("dup".into());
        let err = replay(State(state.clone()), Extension(actor(None)), Json(req))
            .await
            .unwrap_err();
        assert!(matches!(err, ServeError::Conflict(_)), "{err:?}");
        assert!(
            state.registry().is_registered("dup"),
            "the holder keeps its claim"
        );
        state.registry().release_claim("dup", held);

        let existing =
            RunRecord::queued("taken".into(), None, Default::default(), None, Utc::now());
        state.history().upsert(&existing).await.unwrap();
        let mut req = request(cfg, from);
        req.id = Some("taken".into());
        let err = replay(State(state.clone()), Extension(actor(None)), Json(req))
            .await
            .unwrap_err();
        assert!(matches!(err, ServeError::Conflict(_)), "{err:?}");
        assert!(!state.registry().is_registered("taken"));
        let rec = state.history().get("taken").await.unwrap().unwrap();
        assert_eq!(
            rec.status,
            RunStatus::Queued,
            "the existing record is untouched"
        );
        assert!(!state.history().insert_new(&existing).await.unwrap());
    }

    /// A dry-run replay writes nothing, so it is not recorded as a run.
    #[tokio::test]
    async fn a_dry_run_replay_is_not_recorded() {
        let dir = tempfile::tempdir().unwrap();
        let (cfg, from) = fixture(dir.path(), 1, "");
        let state = crate::serve::test_support::test_state();
        let mut req = request(cfg, from);
        req.dry_run = true;
        req.id = Some("rp-d".into());
        let _ = replay(State(state.clone()), Extension(actor(None)), Json(req))
            .await
            .unwrap();
        assert!(state.history().get("rp-d").await.unwrap().is_none());
    }
}
