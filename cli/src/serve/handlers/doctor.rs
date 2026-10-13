//! `POST /v1/doctor` — validate + probe a submitted config WITHOUT running it.
//! Reuses the same preflight as `doctor_first`: 200 with the report when all
//! probes pass, 422 with the report as `details` when any fail. An adapter
//! over [`crate::runtime::server::doctor`].

use crate::runtime::Principal;
use crate::runtime::server::Submission;
use crate::serve::error::ServeError;
use crate::serve::rbac::AuthContext;
use crate::serve::runner::ConfigFormatWire;
use crate::serve::state::ServerState;
use axum::Json;
use axum::extract::{Extension, State};
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
    actor: Option<Extension<AuthContext>>,
    Json(req): Json<DoctorRequest>,
) -> Result<Json<Value>, ServeError> {
    let actor = actor
        .map(|Extension(a)| a)
        .unwrap_or_else(|| AuthContext::system("doctor"));
    let principal = Principal::caller(&state, actor);
    let sub = Submission::new(&req.config, req.config_format.into());
    crate::runtime::server::doctor(&state, &principal, sub)
        .await
        .map(Json)
}
