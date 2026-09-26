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
use crate::serve::load::load_submission;
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
}

/// `POST /v1/dlq/replay` → 200 with the
/// [`ReplayOutcome`](crate::dlq_replay::ReplayOutcome). Operator-only;
/// audited as `dlq.replay`.
pub async fn replay(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Json(req): Json<DlqReplayRequest>,
) -> Result<Json<Value>, ServeError> {
    let loaded = load_submission(
        &req.config,
        req.config_format.into(),
        state.default_base().as_ref(),
        crate::serve::runner::server_policy(&state).as_deref(),
    )
    .await?;
    let auth = build_auth_catalog(loaded.cfg.auth.as_ref()).map_err(cli_to_serve)?;
    let pipeline_name = loaded
        .cfg
        .name
        .clone()
        .unwrap_or_else(|| "dlq-replay".to_string());

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
        },
    )
    .await;

    let result_label = if outcome.is_ok() { "ok" } else { "error" };
    crate::serve::audit::write(&state, &actor, "dlq.replay", None, None, result_label).await;

    let outcome = outcome.map_err(cli_to_serve)?;
    Ok(Json(serde_json::to_value(outcome).unwrap_or(Value::Null)))
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
    /// Permanently delete instead of archiving to a `<file>.archived.jsonl` sibling.
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
