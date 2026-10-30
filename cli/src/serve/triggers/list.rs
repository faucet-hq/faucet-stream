//! `GET /v1/triggers` (#998): every trigger in the `--triggers` file with its
//! live health and fire / error tallies. `StatusRead` (viewer+), enforced by
//! the auth middleware; a server without `--triggers` lists none.

use super::health::TriggerInfo;
use crate::serve::state::ServerState;
use axum::Json;
use axum::extract::State;
use serde::Serialize;

/// `GET /v1/triggers` response body.
#[derive(Debug, Serialize)]
pub struct TriggerListResponse {
    pub triggers: Vec<TriggerInfo>,
}

/// `GET /v1/triggers` → 200.
pub async fn list_triggers(State(state): State<ServerState>) -> Json<TriggerListResponse> {
    Json(TriggerListResponse {
        triggers: state.triggers().list(),
    })
}
