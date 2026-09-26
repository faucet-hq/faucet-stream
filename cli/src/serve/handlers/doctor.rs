//! `POST /v1/doctor` — validate + probe a submitted config WITHOUT running it.
//! Reuses the same preflight as `doctor_first`: 200 with the report when all
//! probes pass, 422 with the report as `details` when any fail.

use crate::serve::error::ServeError;
use crate::serve::load::load_submission;
use crate::serve::runner::{ConfigFormatWire, run_doctor_first};
use crate::serve::state::ServerState;
use axum::Json;
use axum::extract::State;
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
pub struct DoctorRequest {
    pub config: String,
    #[serde(default)]
    pub config_format: ConfigFormatWire,
}

/// `POST /v1/doctor` → 200 report (all pass) / 422 report (any fail) / 400 (bad config).
pub async fn doctor(
    State(state): State<ServerState>,
    Json(req): Json<DoctorRequest>,
) -> Result<Json<Value>, ServeError> {
    let loaded = load_submission(
        &req.config,
        req.config_format.into(),
        state.default_base().as_ref(),
        crate::serve::runner::server_policy(&state).as_deref(),
    )
    .await?;
    let report = run_doctor_first(&state, &loaded).await?;
    Ok(Json(report))
}
