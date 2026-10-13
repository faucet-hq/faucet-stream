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
    Json(req): Json<DlqInspectRequest>,
) -> Result<Json<Value>, ServeError> {
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
}

impl Drop for ReplayToken<'_> {
    fn drop(&mut self) {
        self.state.registry().deregister(&self.id);
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
    // A tenant's replay loads under its scope and is admitted against its
    // limits, whose per-run ceilings join the config's budget (#871).
    let caller_budget = match tenant.as_deref() {
        Some(t) => admit_replay(&state, t).await?,
        None => None,
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
    if state.registry().is_registered(&id) {
        return Err(ServeError::Conflict(format!(
            "a run or replay with id '{id}' is already in flight"
        )));
    }
    let cancel = state.shutdown_token().child_token();
    state.registry().register(id.clone(), cancel.clone());
    let _token = ReplayToken {
        state: &state,
        id: id.clone(),
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
            decryptor: DlqDecryptor::from_keys(&req.encryption_keys)
                .map_err(CliError::from)
                .map_err(cli_to_serve)?,
            budget: caller_budget,
            cancel: Some(cancel),
        },
    )
    .await;

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

/// Admit a replay for `tenant`: it exists, is not suspended and is under its
/// concurrency limit; its per-run ceilings are returned as the replay's budget.
#[cfg(feature = "tenants")]
async fn admit_replay(
    state: &ServerState,
    tenant: &str,
) -> Result<Option<faucet_core::BudgetSpec>, ServeError> {
    crate::serve::tenants::admit_inline(state, tenant)
        .await
        .map(|(_admission, budget)| budget)
}

#[cfg(not(feature = "tenants"))]
async fn admit_replay(
    _state: &ServerState,
    tenant: &str,
) -> Result<Option<faucet_core::BudgetSpec>, ServeError> {
    Err(ServeError::BadConfig(format!(
        "replay for tenant '{tenant}' refused: this server was built without the `tenants` feature"
    )))
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
        assert!(!state.registry().is_registered(out.0["id"].as_str().unwrap()));

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
        let out = replay(State(state), Extension(actor(None)), Json(request(cfg, from)))
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
        let err = replay(State(state), Extension(actor(None)), Json(request(cfg, from)))
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
        let err = replay(State(state.clone()), Extension(actor(Some("other"))), Json(req))
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
}
