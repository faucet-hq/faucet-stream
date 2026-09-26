//! `POST /v1/plan` — plan a config over the control plane (#283 / #707).
//! Thin glue over [`crate::commands::plan::plan_node`]: load the submitted
//! config exactly like a run would (merge base, params, secrets, the server's
//! data-flow policy), pick one root row, run the optional sample through the
//! offline harness, and — with `impact: true` — walk the server's catalog
//! downstream of the row's sink. Nothing is written and no run starts, so the
//! endpoint is `Plan` (viewer+) — see [`crate::serve::rbac::required_permission`].
//! Audited as `plan`.

use crate::auth_catalog::build_auth_catalog;
use crate::commands::plan::{PlanOptions, plan_node, select_root};
use crate::error::CliError;
use crate::serve::error::ServeError;
use crate::serve::load::load_submission;
use crate::serve::rbac::AuthContext;
use crate::serve::runner::ConfigFormatWire;
use crate::serve::state::ServerState;
use axum::Json;
use axum::extract::{Extension, State};
use serde::Deserialize;
use serde_json::Value;

fn cli_to_serve(e: CliError) -> ServeError {
    match e {
        CliError::Config(m) => ServeError::BadConfig(m),
        other => ServeError::Internal(other.to_string()),
    }
}

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

/// `POST /v1/plan` → 200 with the plan report.
pub async fn plan(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
    Json(req): Json<PlanRequest>,
) -> Result<Json<Value>, ServeError> {
    let loaded = load_submission(
        &req.config,
        req.config_format.into(),
        state.default_base().as_ref(),
        crate::serve::runner::server_policy(&state).as_deref(),
    )
    .await?;
    let auth = build_auth_catalog(loaded.cfg.auth.as_ref()).map_err(cli_to_serve)?;
    let node = select_root(&loaded.nodes, req.row.as_deref()).map_err(cli_to_serve)?;
    let sample = req.sample.map(|records| {
        let label = format!("request ({} record(s))", records.len());
        (records, label)
    });
    #[cfg(feature = "catalog")]
    let history = state.history();
    #[cfg(feature = "catalog")]
    let impact = req.impact.then(|| crate::commands::plan::ImpactOptions {
        store: history.as_ref(),
        // The same name the executor records catalog edges under for a serve
        // run, so the row's own edge is found.
        pipeline: loaded
            .cfg
            .name
            .clone()
            .unwrap_or_else(|| "serve".to_string()),
        depth: req.depth.unwrap_or(crate::impact::DEFAULT_DEPTH),
    });
    #[cfg(not(feature = "catalog"))]
    if req.impact {
        return Err(ServeError::Unprocessable {
            message: "impact analysis requires a server built with the `catalog` feature".into(),
            details: None,
        });
    }
    let report = plan_node(
        &loaded.cfg,
        node,
        &auth,
        PlanOptions {
            sample,
            #[cfg(feature = "catalog")]
            impact,
            #[cfg(not(feature = "catalog"))]
            _marker: std::marker::PhantomData,
        },
    )
    .await
    .map_err(cli_to_serve)?;
    let value = serde_json::to_value(&report).map_err(|e| ServeError::Internal(e.to_string()))?;
    crate::serve::audit::write(&state, &actor, "plan", None, None, "ok").await;
    Ok(Json(value))
}
