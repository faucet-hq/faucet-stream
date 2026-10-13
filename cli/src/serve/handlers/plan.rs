//! `POST /v1/plan` — plan a config over the control plane (#283 / #707).
//! Thin glue over [`crate::commands::plan::plan_node`]: load the submitted
//! config exactly like a run would (merge base, params, secrets, the server's
//! data-flow policy), pick one root row, run the optional sample through the
//! offline harness, and — with `impact: true` — walk the server's catalog
//! downstream of the row's sink. Nothing is written and no run starts, so the
//! endpoint is `Plan` (viewer+) — see [`crate::serve::rbac::required_permission`].
//! Audited as `plan`.

use crate::serve::error::ServeError;
use crate::serve::rbac::AuthContext;
use crate::serve::runner::ConfigFormatWire;
use crate::serve::state::ServerState;
use axum::Json;
use axum::extract::{Extension, State};
use serde::Deserialize;
use serde_json::Value;

/// `POST /v1/plan` request body.
#[derive(Debug, Deserialize)]
pub struct PlanRequest {
    /// The pipeline config to plan.
    pub config: String,
    #[serde(default)]
    pub config_format: ConfigFormatWire,
    /// Which root row to plan (default: the first root).
    #[serde(default)]
    pub row: Option<String>,
    /// Sample input records to run through the row's transforms offline
    /// (the planned output schema, volume, and sink delta). Nothing is read
    /// from the real source.
    #[serde(default)]
    pub sample: Option<Vec<Value>>,
    /// Change impact analysis (#707) against this server's catalog.
    #[serde(default)]
    pub impact: bool,
    /// Downstream hop bound for `impact`.
    #[serde(default)]
    pub depth: Option<u32>,
}

/// `POST /v1/plan` → 200 with the plan report: the runtime's `plan` for a
/// config the request carried.
pub async fn plan(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Json(req): Json<PlanRequest>,
) -> Result<Json<Value>, ServeError> {
    let principal = crate::runtime::Principal::caller(&state, actor);
    crate::runtime::server::plan(&state, &principal, req)
        .await
        .map(Json)
}
