//! `faucet_serve_*` request metrics. The `path` label is the *matched route
//! template* (`/v1/runs/{id}`), never the raw path — cardinality safety.

use crate::serve::history::RunStatus;
use crate::serve::state::ServerState;
use axum::extract::{MatchedPath, Request};
use axum::middleware::Next;
use axum::response::Response;
use std::time::Instant;

/// The matched route template for a request, or `<unmatched>` for a 404.
pub fn matched_path_label(req: &Request) -> String {
    req.extensions()
        .get::<MatchedPath>()
        .map(|m| m.as_str().to_owned())
        .unwrap_or_else(|| "<unmatched>".to_string())
}

/// Middleware: count every request + record its duration, labelled by method,
/// matched-path template, and response status.
pub async fn track_metrics(req: Request, next: Next) -> Response {
    let method = req.method().as_str().to_owned();
    let path = matched_path_label(&req);
    let start = Instant::now();
    let resp = next.run(req).await;
    let status = resp.status().as_u16().to_string();

    metrics::counter!(
        "faucet_serve_requests_total",
        "method" => method.clone(), "path" => path.clone(), "status" => status
    )
    .increment(1);
    // `status` is intentionally omitted from the histogram to cap series count
    // (a duration histogram per status code multiplies time-series); join with
    // the counter when a status breakdown is needed.
    metrics::histogram!(
        "faucet_serve_request_duration_seconds",
        "method" => method, "path" => path
    )
    .record(start.elapsed().as_secs_f64());

    resp
}

/// Refresh the queue-depth + in-flight gauges from the registry. Called on every
/// queue transition (submit, queued→running, finish).
pub fn set_run_gauges(state: &ServerState) {
    metrics::gauge!("faucet_serve_runs_queued").set(state.registry().queued() as f64);
    metrics::gauge!("faucet_serve_runs_in_flight").set(state.registry().in_flight() as f64);
}

/// Count a run reaching a terminal state, labelled by status + a finer reason.
pub fn record_run_finished(status: RunStatus, reason: &'static str) {
    metrics::counter!(
        "faucet_serve_runs_total",
        "status" => status.as_str(), "reason" => reason
    )
    .increment(1);
}

/// Count an idempotency-key replay hit.
pub fn record_idempotency_hit() {
    metrics::counter!("faucet_serve_idempotency_hits_total").increment(1);
}

/// Set the run-history degraded gauge (`1` once the persistent backend has
/// fallen back to in-memory; drives alerting alongside `/readyz`).
pub fn set_history_degraded(degraded: bool) {
    metrics::gauge!("faucet_serve_history_degraded").set(if degraded { 1.0 } else { 0.0 });
}

/// `1` while the run-history backend did not answer the last lease renewal
/// (#905): the signal for a strict backend that never falls back to memory.
pub fn set_history_unreachable(unreachable: bool) {
    metrics::gauge!("faucet_serve_history_unreachable").set(if unreachable { 1.0 } else { 0.0 });
}

/// Count runs claimed by this instance's claim loop.
pub fn record_runs_claimed(n: usize) {
    metrics::counter!("faucet_serve_runs_claimed_total").increment(n as u64);
}

/// Count source shards claimed for execution by this instance (Mode B, #230).
pub fn record_shards_claimed(n: usize) {
    metrics::counter!("faucet_serve_shards_claimed_total").increment(n as u64);
}

/// Count shard reclaims (rebalance), split by outcome (Mode B, #230).
pub fn record_shards_reclaimed(requeued: usize, failed: usize) {
    metrics::counter!("faucet_serve_shards_reclaimed_total", "outcome" => "requeued")
        .increment(requeued as u64);
    metrics::counter!("faucet_serve_shards_reclaimed_total", "outcome" => "failed")
        .increment(failed as u64);
}

/// `1` when clustered execution is enabled for this instance, else `0`.
pub fn set_cluster_enabled(on: bool) {
    metrics::gauge!("faucet_serve_cluster_enabled").set(if on { 1.0 } else { 0.0 });
}

/// Live cluster member count (refreshed each lease-loop tick).
pub fn set_cluster_instances(n: usize) {
    metrics::gauge!("faucet_serve_cluster_instances").set(n as f64);
}

/// Count failover reclaims, split by outcome.
pub fn record_runs_reclaimed(requeued: usize, failed: usize) {
    // Emit both label series unconditionally (increment(0) is a no-op that still
    // registers the series), so `outcome="requeued"`/`"failed"` exist from the
    // first call — consistent with the other counters here and friendlier to
    // `absent()`/`rate()` alerting.
    metrics::counter!("faucet_serve_runs_reclaimed_total", "outcome" => "requeued")
        .increment(requeued as u64);
    metrics::counter!("faucet_serve_runs_reclaimed_total", "outcome" => "failed")
        .increment(failed as u64);
}

/// Count persisted run-log lines purged by the retention loop (#529).
pub fn inc_run_logs_purged(n: usize) {
    metrics::counter!("faucet_serve_run_logs_purged_total").increment(n as u64);
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::Request;

    #[test]
    fn unmatched_path_falls_back_to_sentinel() {
        let req = Request::builder()
            .uri("/whatever")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(matched_path_label(&req), "<unmatched>");
    }

    #[test]
    fn run_finished_label_strings_are_stable() {
        // Guards against accidental relabeling (which would split Prometheus series).
        use crate::serve::history::RunStatus;
        assert_eq!(RunStatus::Completed.as_str(), "completed");
        assert_eq!(RunStatus::Cancelled.as_str(), "cancelled");
    }

    /// Regression guard: axum 0.8 populates `MatchedPath` *before* middleware
    /// fires even for outer `.layer()` middleware (axum 0.8 changed this from
    /// 0.7 where outer layers ran before routing). Verifies the cardinality-safe
    /// matched-path design works correctly in `build_router`'s outer-layer
    /// position and that the `path` label is never stuck at `"<unmatched>"` for
    /// routed requests.
    #[tokio::test]
    async fn matched_path_captured_in_outer_layer_position() {
        use axum::routing::get;
        use std::sync::{Arc, Mutex};
        use tower::util::ServiceExt;

        // Capture the path label that track_metrics sees from an outer .layer()
        // — the same position used by build_router.
        let captured: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let captured2 = captured.clone();

        let capture_middleware = axum::middleware::from_fn(move |req: Request, next: Next| {
            let captured = captured2.clone();
            async move {
                let label = matched_path_label(&req);
                *captured.lock().unwrap() = Some(label);
                next.run(req).await
            }
        });

        // Mirrors build_router: outer .layer() on the merged router.
        let router = axum::Router::new()
            .route("/v1/runs/{id}", get(|| async { "ok" }))
            .layer(capture_middleware);

        let req = Request::builder()
            .uri("/v1/runs/abc-123")
            .body(axum::body::Body::empty())
            .unwrap();
        let _resp: axum::response::Response = router.oneshot(req).await.unwrap();

        let label = captured.lock().unwrap().clone().unwrap();
        // axum 0.8: MatchedPath is populated before the outer layer runs.
        assert_eq!(
            label, "/v1/runs/{id}",
            "MatchedPath must be the route template, not '<unmatched>' — \
             axum 0.8 outer .layer() correctly sees MatchedPath"
        );
    }

    #[test]
    fn shard_metrics_emit_without_a_recorder() {
        // With no global recorder installed these are no-ops; assert they don't
        // panic and exercise the emit helpers (Mode B, #230).
        super::record_shards_claimed(3);
        super::record_shards_reclaimed(2, 1);
    }
}
