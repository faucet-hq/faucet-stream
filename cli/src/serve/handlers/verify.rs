//! `POST /v1/verify` — content verification over the control plane (#701).
//! Thin glue over [`crate::verify`]: load the submitted config, verify one
//! root row, return the [`VerifyOutcome`](crate::verify::VerifyOutcome).
//!
//! A repair writes through the sink, so the whole endpoint is `RunWrite`
//! (operator+) — see [`crate::serve::rbac::required_permission`]. Audited as
//! `verify`.

use crate::serve::error::ServeError;
use crate::serve::rbac::AuthContext;
use crate::serve::runner::ConfigFormatWire;
use crate::serve::state::ServerState;
use axum::Json;
use axum::extract::{Extension, State};
use serde::Deserialize;
use serde_json::Value;

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
/// non-empty — a mismatch is a result, not an error): the runtime's `verify`
/// for a config the request carried.
pub async fn verify(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Json(req): Json<VerifyRequest>,
) -> Result<Json<Value>, ServeError> {
    let principal = crate::runtime::Principal::caller(&state, actor);
    crate::runtime::server::verify(&state, &principal, req)
        .await
        .map(Json)
}
