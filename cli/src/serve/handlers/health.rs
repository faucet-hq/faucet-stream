//! Liveness (`/healthz`), readiness (`/readyz`), and Prometheus (`/metrics`)
//! endpoints. All three are unauthenticated (probes / scrapers). Phase 1
//! `/readyz` is always-ready; it gains history-degraded / queue-full checks in
//! later phases.

use crate::serve::state::ServerState;
use axum::Json;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde_json::json;

/// Liveness: a responding handler means the process is alive.
pub async fn healthz() -> impl IntoResponse {
    StatusCode::OK
}

/// Readiness: 503 if the history backend is degraded or the queue is full,
/// else 200. The JSON body surfaces history/queue health and cluster membership.
pub async fn readyz(State(state): State<ServerState>) -> impl IntoResponse {
    let history_ok = !state.history().degraded();
    let queue_ok = !state.registry().is_full();
    let code = if history_ok && queue_ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    #[cfg(feature = "triggers")]
    let triggers =
        serde_json::to_value(state.triggers().snapshot()).unwrap_or(serde_json::Value::Null);
    #[cfg(not(feature = "triggers"))]
    let triggers = serde_json::Value::Array(vec![]);
    let body = json!({
        "status": if code == StatusCode::OK { "ready" } else { "not_ready" },
        "history_ok": history_ok,
        "queue_ok": queue_ok,
        "cluster": {
            "enabled": state.cluster().enabled(),
            "instances": state.cluster().members(),
        },
        "triggers": triggers,
    });
    (code, Json(body))
}

/// Prometheus exposition rendered from serve's own recorder handle.
pub async fn metrics(State(state): State<ServerState>) -> Response {
    match state.render_metrics() {
        Some(body) => ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body).into_response(),
        // 503 (not 200) so a Prometheus scraper marks the target down rather
        // than silently recording a successful scrape with zero metrics. This
        // only fires if the recorder failed to install (e.g. a second server in
        // one process); in normal operation `render_metrics()` is `Some`.
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
            "# metrics recorder not installed in this process\n",
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::cluster::ClusterConfig;
    use crate::serve::config::{AuthMode, HistoryBackendSpec, ServeConfig};
    use crate::serve::history::RunHistory;
    use crate::serve::history::memory::MemoryHistory;
    use crate::serve::state::ServerState;
    use axum::response::IntoResponse;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    fn state(cluster_enabled: bool) -> ServerState {
        let mut cluster = ClusterConfig::disabled();
        cluster.enabled = cluster_enabled;
        let cfg = ServeConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            log_format: crate::cli::LogFormat::Text,
            auth: AuthMode::None,
            max_concurrent_runs: 4,
            max_queued_runs: 4,
            default_config_path: None,
            history: HistoryBackendSpec::Memory,
            cors_origins: vec![],
            body_limit_bytes: 1_048_576,
            shutdown_grace: Duration::from_secs(60),
            retain_terminal_runs: Duration::from_secs(60),
            idempotency_retention: Duration::from_secs(60),
            log_retention: std::time::Duration::from_secs(0),
            log_max_lines_per_run: 100_000,
            local_output_retention_days: 7,
            local_output_in_flight_grace: Duration::from_secs(60),
            preview: crate::serve::preview::PreviewConfig::default(),
            lease_ttl: Duration::from_secs(30),
            probe_timeout: Duration::from_secs(10),
            env_file: None,
            no_env_file: false,
            log_level: "info".into(),
            ui_enabled: true,
            cluster,
            triggers_path: None,
            callback_allow_hosts: Vec::new(),
        };
        let history = Arc::new(MemoryHistory::new(Duration::from_secs(60))) as Arc<dyn RunHistory>;
        ServerState::new(
            &cfg,
            None,
            CancellationToken::new(),
            history,
            crate::serve::logs::LogHub::new(),
            None,
            #[cfg(feature = "triggers")]
            crate::serve::triggers::health::TriggersHandle::empty(),
        )
    }

    #[tokio::test]
    async fn readyz_reports_cluster_membership() {
        let st = state(true);
        st.cluster().set_members(3);
        let resp = readyz(axum::extract::State(st)).await.into_response();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["cluster"]["enabled"], true);
        assert_eq!(v["cluster"]["instances"], 3);
        assert_eq!(v["status"], "ready");
        assert_eq!(v["history_ok"], true);
        assert_eq!(v["queue_ok"], true);
    }

    #[tokio::test]
    async fn readyz_ok_single_instance() {
        let resp = readyz(axum::extract::State(state(false)))
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
