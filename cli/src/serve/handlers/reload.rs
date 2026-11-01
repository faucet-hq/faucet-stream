//! `POST /v1/reload` — hot-reload the server's `--default-config` merge base
//! (#198) without restarting or interrupting in-flight runs.
//!
//! Re-reads and re-validates the `--default-config` file and atomically swaps
//! the in-memory merge base. In-flight runs already captured their config, so
//! they are unaffected; subsequent submissions merge onto the new base. An
//! invalid new config is rejected (422) and the previous base is kept. Admin-only
//! (RBAC [`Permission::Reload`](crate::serve::rbac::Permission::Reload)).

use crate::serve::error::ServeError;
use crate::serve::rbac::AuthContext;
use crate::serve::state::ServerState;
use axum::Json;
use axum::extract::{Extension, State};
use serde_json::{Value, json};

/// `POST /v1/reload` → 200 `{reloaded, path?, loaded_at?}` / 422 (invalid new
/// config). `loaded_at` (#998) is when the merge base now in use was loaded.
///
/// Writes a `config.reload` audit record on every outcome — this is a
/// server-wide privileged mutation (it swaps the config merge base every later
/// run inherits), so it must be attributable in `GET /v1/audit` like the other
/// privileged mutations (audit #321 M6).
pub async fn reload(
    State(state): State<ServerState>,
    Extension(actor): Extension<AuthContext>,
) -> Result<Json<Value>, ServeError> {
    let Some(path) = state.default_config_path().cloned() else {
        crate::serve::audit::write(&state, &actor, "config.reload", None, None, "noop").await;
        return Ok(Json(json!({
            "reloaded": false,
            "reason": "no --default-config configured; nothing to reload",
        })));
    };

    // `serve` has no --profile flag; honour FAUCET_PROFILE like startup loading.
    let profile = std::env::var("FAUCET_PROFILE").ok();
    match crate::config::PipelineConfig::from_path_async(&path, profile.as_deref()).await {
        Ok(cfg) => {
            let value = serde_json::to_value(&cfg).map_err(|e| {
                ServeError::Internal(format!("serializing reloaded --default-config: {e}"))
            })?;
            state.set_default_base(Some(value));
            tracing::info!(path = %path.display(), "reloaded --default-config (POST /v1/reload)");
            crate::serve::audit::write(&state, &actor, "config.reload", None, None, "ok").await;
            Ok(Json(json!({
                "reloaded": true,
                "path": path.display().to_string(),
                "loaded_at": state.default_loaded_at(),
            })))
        }
        Err(e) => {
            crate::serve::audit::write(&state, &actor, "config.reload", None, None, "rejected")
                .await;
            Err(ServeError::Unprocessable {
                message: format!("reload rejected — keeping previous config: {e}"),
                details: None,
            })
        }
    }
}
