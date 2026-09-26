//! `POST /v1/verify` — content verification over the control plane (#701).
//! Thin glue over [`crate::verify`]: load the submitted config, verify one
//! root row, return the [`VerifyOutcome`](crate::verify::VerifyOutcome).
//!
//! A repair writes through the sink, so the whole endpoint is `RunWrite`
//! (operator+) — see [`crate::serve::rbac::required_permission`]. Audited as
//! `verify`.

use crate::auth_catalog::build_auth_catalog;
use crate::error::CliError;
use crate::serve::error::ServeError;
use crate::serve::load::load_submission;
use crate::serve::rbac::AuthContext;
use crate::serve::runner::ConfigFormatWire;
use crate::serve::state::ServerState;
use crate::verify::VerifyInputs;
use axum::Json;
use axum::extract::{Extension, State};
use chrono::Utc;
use serde::Deserialize;
use serde_json::Value;

fn cli_to_serve(e: CliError) -> ServeError {
    match e {
        CliError::Config(m) => ServeError::BadConfig(m),
        other => ServeError::Internal(other.to_string()),
    }
}

/// `POST /v1/verify` request body.
#[derive(Debug, Deserialize)]
pub struct VerifyRequest {
    /// The pipeline config whose source and sink to compare.
    pub config: String,
    #[serde(default)]
    pub config_format: ConfigFormatWire,
    /// Which root row to verify (default: the first root).
    #[serde(default)]
    pub row: Option<String>,
    /// Re-sync differing keys through the sink.
    #[serde(default)]
    pub repair: bool,
    /// With `repair`, also delete destination-only rows.
    #[serde(default)]
    pub allow_delete: bool,
    /// With `repair`, plan without writing.
    #[serde(default)]
    pub dry_run: bool,
    /// Report at most this many differences.
    #[serde(default)]
    pub max_differences: Option<usize>,
}

/// `POST /v1/verify` → 200 with the verification outcome (`differences` may be
/// non-empty — a mismatch is a result, not an error).
pub async fn verify(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Json(req): Json<VerifyRequest>,
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
        .unwrap_or_else(|| "verify".to_string());
    let mut spec = loaded.cfg.verify.clone().unwrap_or_default();
    if let Some(n) = req.max_differences {
        spec.max_differences = n;
    }
    let outcome = crate::verify::verify(
        &loaded.cfg,
        &spec,
        VerifyInputs {
            row: req.row,
            repair: req.repair,
            allow_delete: req.allow_delete,
            dry_run: req.dry_run,
            pipeline_name,
            execution: loaded.cfg.execution.clone(),
            auth,
            clock: Utc::now().fixed_offset(),
        },
    )
    .await
    .map_err(cli_to_serve)?;
    crate::serve::audit::write(
        &state,
        &actor,
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
    Ok(Json(serde_json::to_value(outcome).unwrap_or(Value::Null)))
}
