//! axum router assembly and the bind / graceful-shutdown serve loop.

use crate::error::{CliError, CliResult};
use crate::serve::config::ServeConfig;
use crate::serve::handlers::{
    audit, backfill, dlq, doctor, health, logs, plan, reload, runs, schemas, verify, whoami,
};
use crate::serve::history::RunHistory;
use crate::serve::state::ServerState;
use crate::serve::{auth, metrics};
use axum::Router;
use axum::routing::{get, post};
use serde_json::Value;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::limit::RequestBodyLimitLayer;

/// Build the full router: unauthenticated probes + the bearer-guarded `/v1` API.
pub fn build_router(
    state: ServerState,
    config: &ServeConfig,
    #[cfg_attr(not(feature = "mcp"), allow(unused_variables))] mcp: &crate::serve::McpServeSettings,
) -> Router {
    let public = Router::new()
        .route("/healthz", get(health::healthz))
        .route("/readyz", get(health::readyz))
        .route("/metrics", get(health::metrics));

    // `/v1` routes guarded by the bearer middleware via `route_layer` (only runs
    // for matched routes; OPTIONS preflight is allowed through inside the layer).
    #[cfg_attr(
        not(any(feature = "triggers", feature = "catalog", feature = "templates")),
        allow(unused_mut)
    )]
    let mut api = Router::new()
        .route("/v1/runs", post(runs::submit_run).get(runs::list_runs))
        .route("/v1/runs/{id}", get(runs::get_run).delete(runs::delete_run))
        .route("/v1/runs/{id}/cancel", post(runs::cancel_run))
        .route("/v1/runs/{id}/rollback", post(runs::rollback_run))
        .route("/v1/runs/{id}/logs", get(logs::stream_logs))
        .route("/v1/schemas", get(schemas::list_schemas))
        .route("/v1/schemas/{kind}/{name}", get(schemas::get_schema))
        .route("/v1/doctor", post(doctor::doctor))
        .route("/v1/backfill", post(backfill::submit_backfill))
        .route("/v1/verify", post(verify::verify))
        .route("/v1/plan", post(plan::plan))
        .route("/v1/dlq/inspect", post(dlq::inspect))
        .route("/v1/dlq/replay", post(dlq::replay))
        .route("/v1/dlq/discard", post(dlq::discard))
        .route("/v1/audit", get(audit::list_audit))
        .route("/v1/reload", post(reload::reload))
        .route("/v1/whoami", get(whoami::whoami));
    #[cfg(feature = "triggers")]
    {
        api = api.route(
            "/v1/triggers/{name}",
            post(crate::serve::triggers::webhook::handle)
                .put(crate::serve::triggers::webhook::handle),
        );
    }
    #[cfg(feature = "catalog")]
    {
        use crate::serve::handlers::{catalog, local_outputs, preview};
        api = api
            .route("/v1/catalog/datasets", get(catalog::list_datasets))
            .route("/v1/catalog/datasets/{id}", get(catalog::get_dataset))
            .route(
                "/v1/catalog/datasets/{id}/consumers",
                post(catalog::annotate_dataset),
            )
            .route("/v1/catalog/lineage", get(catalog::lineage))
            // Local sink output retention (#587) — the control surface behind the
            // Datasets page's cleanup controls (#588).
            .route("/v1/local-outputs", get(local_outputs::list_outputs))
            .route(
                "/v1/local-outputs/{id}",
                axum::routing::delete(local_outputs::delete_output),
            )
            .route("/v1/local-outputs/cleanup", post(local_outputs::cleanup))
            // Dataset preview of a tracked local output (#586). Always routed —
            // the opt-in gate is enforced in the handler so a disabled server
            // answers "not enabled, here is the flag" instead of a bare 404.
            .route(
                "/v1/local-outputs/{id}/preview",
                get(preview::preview_output),
            );
    }
    // Pipeline template registry + parameterized trigger API (#444).
    #[cfg(feature = "templates")]
    {
        use crate::serve::handlers::templates;
        api = api
            .route(
                "/v1/templates",
                post(templates::register_template).get(templates::list_templates),
            )
            .route(
                "/v1/templates/{id}",
                get(templates::get_template).delete(templates::delete_template),
            )
            // Static `/matrix` is matched ahead of `{id}` (same reservation as `/sync`).
            .route("/v1/templates/matrix", get(templates::template_matrix))
            .route("/v1/templates/{id}/runs", post(templates::trigger_template))
            .route("/v1/templates/{id}/tags", post(templates::promote_template))
            .route(
                "/v1/templates/{id}/launch",
                post(templates::launch_template),
            )
            .route(
                "/v1/templates/{id}/rollback",
                post(templates::rollback_template),
            )
            .route(
                "/v1/templates/{id}/deprecate",
                post(templates::deprecate_template),
            )
            .route(
                "/v1/templates/{id}/versions/{version}/deprecate",
                post(templates::deprecate_version),
            );
        // Template hosting + sync (RFC 0006 / #589). Static `/sync` is matched
        // ahead of the `{id}` parameter by the router, so a template can never
        // be named `sync` on the wire (the id charset allows it; the route
        // wins, which is the documented reservation).
        #[cfg(feature = "templates-sync")]
        {
            api = api
                .route("/v1/templates/sync", post(templates::sync_templates))
                .route(
                    "/v1/templates/{id}/publish",
                    post(templates::publish_template),
                );
        }
    }
    // MCP endpoint (#420): mounted only with `--mcp`. Placed on `api` so it
    // inherits the bearer-auth + RBAC route-layer below; the per-request
    // mutation gate additionally requires the caller's `RunWrite` scope.
    #[cfg(feature = "mcp")]
    if mcp.enabled {
        api = api
            .route("/mcp", post(crate::serve::mcp_route::handle))
            .layer(axum::Extension(crate::serve::mcp_route::McpRouteFlags {
                allow_mutations: mcp.allow_mutations,
            }));
    }

    let api = api.route_layer(axum::middleware::from_fn_with_state(
        state.clone(),
        auth::require_auth,
    ));

    let cors = if config.cors_origins.is_empty() {
        CorsLayer::new()
    } else {
        let origins: Vec<axum::http::HeaderValue> = config
            .cors_origins
            .iter()
            .filter_map(|o| match o.parse() {
                Ok(v) => Some(v),
                Err(e) => {
                    tracing::warn!(origin = %o, error = %e, "ignoring invalid --cors-origin");
                    None
                }
            })
            .collect();
        CorsLayer::new().allow_origin(AllowOrigin::list(origins))
    };

    #[cfg_attr(not(feature = "serve-ui"), allow(unused_mut))]
    let mut router = public.merge(api);

    #[cfg(feature = "serve-ui")]
    if config.ui_enabled {
        use crate::serve::ui_assets;
        router = router
            .route("/", axum::routing::get(ui_assets::index))
            .route("/assets/{*path}", axum::routing::get(ui_assets::asset))
            .fallback(ui_assets::spa_fallback);
    }

    router
        .layer(RequestBodyLimitLayer::new(config.body_limit_bytes))
        .layer(axum::middleware::from_fn(metrics::track_metrics))
        .layer(cors)
        .with_state(state)
}

/// Load the optional `--default-config` once at startup, fully resolved, as a
/// merge base `Value`.
async fn load_default_base(config: &ServeConfig) -> CliResult<Option<Value>> {
    match &config.default_config_path {
        None => Ok(None),
        Some(path) => {
            // `serve` has no --profile flag; honour FAUCET_PROFILE from the environment directly.
            let profile = std::env::var("FAUCET_PROFILE").ok();
            let cfg =
                crate::config::PipelineConfig::from_path_async(path, profile.as_deref()).await?;
            Ok(Some(serde_json::to_value(&cfg).map_err(|e| {
                CliError::Serve(format!("serializing --default-config: {e}"))
            })?))
        }
    }
}

/// How often the background maintenance task purges expired history.
///
/// A quarter of the shorter of the two retention windows, clamped to
/// `[60s, 1h]`: frequent enough to bound store growth (and to honour a short
/// `idempotency_retention`) without churning on the multi-day default
/// `retain_terminal_runs`.
fn purge_interval(retain_terminal: Duration, idem_retention: Duration) -> Duration {
    (retain_terminal.min(idem_retention) / 4)
        .clamp(Duration::from_secs(60), Duration::from_secs(3600))
}

/// Background history-maintenance loop: every `period`, drop terminal run
/// records older than `retain` and expired idempotency claims, until `shutdown`
/// fires. Without this, the history store (in-memory `DashMap`s or the SQL
/// `faucet_serve_runs` / `faucet_serve_idem` tables) grows without bound for the
/// life of the process and the `--retain-terminal-runs-secs` /
/// `--idempotency-retention-secs` knobs are inert (audit #146 C4).
pub(crate) async fn maintenance_loop(
    history: Arc<dyn RunHistory>,
    retain: Duration,
    log_retain: Duration,
    period: Duration,
    shutdown: CancellationToken,
) {
    let mut tick = tokio::time::interval(period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tick.tick().await; // consume the immediate first tick so we don't purge at t=0
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tick.tick() => {
                match history.purge_expired(retain).await {
                    Ok(n) if n > 0 => {
                        tracing::info!(purged = n, "purged expired run records / idempotency claims")
                    }
                    Ok(_) => {}
                    Err(e) => tracing::warn!(error = %e, "history purge_expired failed"),
                }
                // Persistent run logs (#529) have their own retention window.
                if !log_retain.is_zero() {
                    match history.purge_run_logs(log_retain).await {
                        Ok(n) if n > 0 => {
                            crate::serve::metrics::inc_run_logs_purged(n);
                            tracing::info!(purged = n, "purged expired run logs")
                        }
                        Ok(_) => {}
                        Err(e) => tracing::warn!(error = %e, "history purge_run_logs failed"),
                    }
                }
            },
        }
    }
}

/// How often the local-output GC sweeps (#587).
///
/// A quarter of the retention window, clamped to `[60s, 1h]` — the same shape as
/// [`purge_interval`]. With the 7-day default that is hourly, which bounds the
/// local footprint without stat-ing the ledger in a tight loop.
#[cfg(feature = "catalog")]
fn local_output_gc_interval(retention_days: u32) -> Duration {
    let window = Duration::from_secs(u64::from(retention_days) * 86_400);
    (window / 4).clamp(Duration::from_secs(60), Duration::from_secs(3600))
}

/// Background local-output GC (#587): every `period`, delete the recorded local
/// sink output files whose retention window has elapsed, until `shutdown` fires.
///
/// Deletes **only** paths the ledger records as faucet's own sink outputs — never
/// a glob, never a directory, and never a file faucet merely appended to (see
/// [`crate::local_outputs`]). Outputs of runs still in flight on this instance
/// are skipped and retried next tick, so a sweep can never unlink a file
/// mid-write.
///
/// Run records, catalog entries, and lineage are untouched: this reclaims data
/// files, and a swept output keeps its ledger row marked `expired`.
#[cfg(feature = "catalog")]
pub(crate) async fn local_output_gc_loop(
    state: ServerState,
    retention_days: u32,
    period: Duration,
    shutdown: CancellationToken,
) {
    use crate::local_outputs::{SweepOptions, SweepScope, sweep};
    let grace = state.local_output_in_flight_grace();
    let mut tick = tokio::time::interval(period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tick.tick().await; // consume the immediate first tick so nothing is swept at t=0
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tick.tick() => {
                let opts = SweepOptions::new(retention_days)
                    .in_flight(state.registry().live_run_ids())
                    .in_flight_grace(grace);
                match sweep::run(state.history().as_ref(), &SweepScope::Expired, &opts).await {
                    Ok(_) => {}
                    // A store failure means the sweep does not know what it is
                    // allowed to touch, so it deleted nothing. Log and retry next
                    // tick rather than taking the server down over housekeeping.
                    Err(e) => tracing::warn!(error = %e, "local-output GC sweep failed"),
                }
            },
        }
    }
}

/// The lease heartbeat / orphan-recovery cadence: one third of the lease TTL
/// (so a run sees ≥2 renewals before its lease could expire), floored at 1s.
fn lease_interval(lease_ttl: Duration) -> Duration {
    (lease_ttl / 3).max(Duration::from_secs(1))
}

/// Background lease-maintenance loop (#146 H7). Every `period`:
///
/// 1. **Heartbeat** — renew this instance's own non-terminal runs' leases, so a
///    peer never reclaims a run we are still executing.
/// 2. **Recover** — fail any non-terminal run whose owning instance's lease has
///    expired (a crashed/gone peer), so a survivor eventually cleans up orphans
///    rather than waiting for the next process restart.
///
/// Renew runs *before* recover so this instance's leases are fresh when the
/// expiry scan runs. For the in-memory backend both calls are no-ops.
///
/// In cluster mode (#197) the recover step is replaced by: a membership
/// heartbeat, a live-member refresh (`member_ttl = period * 3`), and a
/// failover `reclaim_orphans` that re-queues an expired-lease peer's runs
/// (capped at `max_attempts`) rather than failing them outright.
pub(crate) async fn lease_loop(state: ServerState, period: Duration, shutdown: CancellationToken) {
    let cluster = state.cluster().clone();
    // Member-liveness window ≈ the real lease TTL (period == lease_ttl/3), so a
    // member must miss ~3 heartbeats before peers treat it as gone.
    let member_ttl = period.saturating_mul(3);
    let mut tick = tokio::time::interval(period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tick.tick().await; // consume the immediate first tick
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tick.tick() => {
                if let Err(e) = state.history().renew_leases().await {
                    tracing::warn!(error = %e, "lease heartbeat (renew_leases) failed");
                }
                if cluster.enabled() {
                    // Membership heartbeat.
                    let beat = crate::serve::history::InstanceHeartbeat {
                        started_at: cluster.started_at(),
                        listen: Some(cluster.listen().to_string()),
                        max_concurrent: cluster.max_concurrent(),
                        in_flight: state.registry().in_flight() as u32,
                    };
                    if let Err(e) = state.history().heartbeat_instance(&beat).await {
                        tracing::warn!(error = %e, "cluster: heartbeat_instance failed");
                    }
                    match state.history().live_instances(member_ttl).await {
                        Ok(members) => {
                            cluster.set_members(members.len());
                            crate::serve::metrics::set_cluster_instances(members.len());
                        }
                        Err(e) => tracing::warn!(error = %e, "cluster: live_instances failed"),
                    }
                    // Failover reclaim (re-run orphans).
                    match state.history().reclaim_orphans(cluster.max_attempts()).await {
                        Ok(r) if r.requeued > 0 || r.failed > 0 => {
                            crate::serve::metrics::record_runs_reclaimed(r.requeued, r.failed);
                            tracing::warn!(
                                requeued = r.requeued, failed = r.failed,
                                "cluster: reclaimed orphaned runs from an expired-lease instance"
                            );
                        }
                        Ok(_) => {}
                        Err(e) => tracing::warn!(error = %e, "cluster: reclaim_orphans failed"),
                    }
                    // Mode B (#230): heartbeat this instance's running shards, and
                    // rebalance shards whose owner's lease expired (requeue →
                    // another worker, or poison past max_attempts).
                    if let Err(e) = state.history().renew_shard_leases().await {
                        tracing::warn!(error = %e, "cluster: renew_shard_leases failed");
                    }
                    match state.history().reclaim_shards(cluster.max_attempts()).await {
                        Ok(r) if r.requeued > 0 || r.failed > 0 => {
                            crate::serve::metrics::record_shards_reclaimed(r.requeued, r.failed);
                            tracing::warn!(
                                requeued = r.requeued, failed = r.failed,
                                "cluster: reclaimed orphaned shards from an expired-lease instance"
                            );
                        }
                        Ok(_) => {}
                        Err(e) => tracing::warn!(error = %e, "cluster: reclaim_shards failed"),
                    }
                    // Mode B (#230 / F11): finalize any `sharded` parent whose
                    // shards are all terminal but which no shard task finalized
                    // inline (e.g. the coordinator crashed after the last shard
                    // completed on another instance). Status-fenced + metric
                    // recorded inside the backend, so a parent already finalized by
                    // `maybe_finalize_parent` is not re-finalized or double-counted.
                    match state.history().finalize_completed_sharded_parents().await {
                        Ok(n) if n > 0 => tracing::info!(
                            finalized = n,
                            "cluster: finalized completed sharded parent run(s) via sweep"
                        ),
                        Ok(_) => {}
                        Err(e) => {
                            tracing::warn!(error = %e, "cluster: finalize_completed_sharded_parents failed")
                        }
                    }
                } else {
                    // Single-instance: mark orphans failed (today's behavior).
                    match state.history().recover_orphans().await {
                        Ok(n) if n > 0 => tracing::warn!(
                            recovered = n,
                            "recovered orphaned runs from an expired-lease instance"
                        ),
                        Ok(_) => {}
                        Err(e) => tracing::warn!(error = %e, "orphan recovery failed"),
                    }
                }
            }
        }
    }
}

/// Boot the server: install observability, build state + router, bind, serve
/// until SIGTERM/SIGINT, then drain in-flight runs up to the grace window.
pub async fn serve(config: ServeConfig, mcp: crate::serve::McpServeSettings) -> CliResult<()> {
    let (prom, log_hub) =
        crate::serve::observability::install(&config.log_level, config.log_format);
    crate::serve::metrics::set_cluster_enabled(config.cluster.enabled);

    // This process's identity for run-ownership leases (#146 H7). A fresh id per
    // process, so a restarted instance recovers its prior incarnation's runs only
    // once their lease expires — never another live instance's heartbeated runs.
    let instance_id = uuid::Uuid::new_v4().to_string();
    tracing::info!(
        instance_id = %instance_id,
        lease_ttl_secs = config.lease_ttl.as_secs(),
        "faucet serve instance id"
    );

    let history = crate::serve::history::connect(
        &config.history,
        config.idempotency_retention,
        config.lease_ttl,
        &instance_id,
    )
    .await?;
    if config.cluster.enabled {
        // Cluster mode: a restarting instance re-queues its prior incarnation's
        // in-flight runs (capped) rather than failing them.
        let report = history
            .reclaim_orphans(config.cluster.max_attempts)
            .await
            .map_err(|e| CliError::Serve(format!("history recovery: {e}")))?;
        if report.requeued > 0 || report.failed > 0 {
            tracing::warn!(
                requeued = report.requeued,
                failed = report.failed,
                "startup reclaim of orphaned runs from an expired-lease instance"
            );
        }
    } else {
        let recovered = history
            .recover_orphans()
            .await
            .map_err(|e| CliError::Serve(format!("history recovery: {e}")))?;
        if recovered > 0 {
            tracing::warn!(
                recovered,
                "marked orphaned non-terminal runs (expired owner lease) as failed"
            );
        }
    }
    // Persistent run logs (#529): route captured (already-redacted) logs into the
    // durable backend so they survive past the ephemeral SSE drain window. The
    // in-memory backend stays ephemeral by default (persist only to a real DB).
    if !config.log_retention.is_zero()
        && !matches!(
            config.history,
            crate::serve::config::HistoryBackendSpec::Memory
        )
    {
        log_hub.enable_persistence(history.clone(), config.log_max_lines_per_run);
        tracing::info!(
            retention_secs = config.log_retention.as_secs(),
            max_lines_per_run = config.log_max_lines_per_run,
            "persistent run logs enabled"
        );
    }

    let default_base = load_default_base(&config).await?;

    // Event-driven triggers (#196): load + validate the file (fail-fast), then
    // build the shared handle (webhook table + health rows).
    #[cfg(feature = "triggers")]
    let triggers = match &config.triggers_path {
        Some(path) => {
            // Register HELP text for the trigger metric family once at startup so
            // the series carry descriptions in `/metrics` (mirrors schedule).
            crate::serve::triggers::metrics::describe();
            Some(crate::serve::triggers::load_triggers(path).await?)
        }
        None => None,
    };
    #[cfg(feature = "triggers")]
    let triggers_handle = match &triggers {
        Some(c) => crate::serve::triggers::health::TriggersHandle::from_compiled(&c.triggers),
        None => crate::serve::triggers::health::TriggersHandle::empty(),
    };
    // A `--triggers` path in a build without the feature is a clear error.
    #[cfg(not(feature = "triggers"))]
    if config.triggers_path.is_some() {
        return Err(CliError::Serve(
            "--triggers requires a build with the `triggers` feature".into(),
        ));
    }
    // Template origins (RFC 0006 / #589): load + validate the sync file
    // fail-fast; the pull itself runs after the state exists.
    #[cfg(feature = "templates-sync")]
    let templates_sync = match &config.templates_sync_path {
        Some(path) => {
            crate::templates::sync::describe_metrics();
            Some(std::sync::Arc::new(
                crate::templates::sync::load_sync_file(path).await?,
            ))
        }
        None => None,
    };
    #[cfg(not(feature = "templates-sync"))]
    if config.templates_sync_path.is_some() {
        return Err(CliError::Serve(
            "--templates-sync requires a build with the `templates-sync` feature".into(),
        ));
    }

    // Data-flow policy (#702): load + validate fail-fast; a policy that does
    // not parse must refuse to start the server rather than silently not apply.
    #[cfg(feature = "policy")]
    let policy = match &config.policy_path {
        Some(path) => Some(std::sync::Arc::new(crate::policy::load_file(path)?)),
        None => None,
    };
    #[cfg(not(feature = "policy"))]
    if config.policy_path.is_some() {
        return Err(CliError::Serve(
            "--policy requires a build with the `policy` feature".into(),
        ));
    }

    let shutdown = CancellationToken::new();
    let state = ServerState::new(
        &config,
        prom,
        shutdown.clone(),
        history,
        log_hub,
        default_base,
        #[cfg(feature = "triggers")]
        triggers_handle,
    );
    #[cfg(feature = "policy")]
    if let Some(p) = policy {
        tracing::info!(rules = p.rules.len(), "data-flow policy loaded");
        state.set_policy(p);
    }
    // Attach the origins, pull each once so the registry is populated before
    // the listener opens, then start the periodic pulls. A network failure on
    // the initial pull is logged and counted, not fatal — the server must come
    // up on a stale registry rather than not at all; the file itself was
    // already validated above.
    #[cfg(feature = "templates-sync")]
    let sync_handles = match &templates_sync {
        Some(file) => {
            state.set_templates_sync(std::sync::Arc::clone(file));
            let store: crate::templates::TemplateStore = state.history();
            for origin in &file.origins {
                match crate::templates::sync::sync_origin(&store, origin, false, None).await {
                    Ok(r) => tracing::info!(
                        origin = %r.origin,
                        mutations = r.mutations(),
                        failed = r.failed(),
                        "initial template sync"
                    ),
                    Err(e) => {
                        tracing::warn!(origin = %origin.name, error = %e, "initial template sync failed")
                    }
                }
            }
            crate::templates::sync::spawn_interval_syncs(
                store,
                std::sync::Arc::clone(file),
                shutdown.clone(),
            )
        }
        None => Vec::new(),
    };
    let app = build_router(state.clone(), &config, &mcp);

    let listener = tokio::net::TcpListener::bind(config.listen)
        .await
        .map_err(|e| CliError::Serve(format!("failed to bind {}: {e}", config.listen)))?;
    let local = listener
        .local_addr()
        .map_err(|e| CliError::Serve(e.to_string()))?;
    tracing::info!(listen = %local, "faucet serve listening");

    // Background history maintenance: bounds run-record / idempotency-claim
    // growth and makes the retention knobs effective (audit #146 C4).
    let purge_period = purge_interval(config.retain_terminal_runs, config.idempotency_retention);
    tracing::info!(
        interval_secs = purge_period.as_secs(),
        retain_secs = config.retain_terminal_runs.as_secs(),
        "history maintenance task started"
    );
    let maintenance = tokio::spawn(maintenance_loop(
        state.history(),
        config.retain_terminal_runs,
        config.log_retention,
        purge_period,
        shutdown.clone(),
    ));

    // Local sink output retention GC (#587). `--local-output-retention-days 0`
    // disables the automatic sweep; on-demand cleanup from the Datasets page and
    // `faucet cleanup` still works.
    #[cfg(feature = "catalog")]
    let local_output_gc = if config.local_output_retention_days > 0 {
        let period = local_output_gc_interval(config.local_output_retention_days);
        tracing::info!(
            interval_secs = period.as_secs(),
            retention_days = config.local_output_retention_days,
            "local sink output retention GC started"
        );
        Some(tokio::spawn(local_output_gc_loop(
            state.clone(),
            config.local_output_retention_days,
            period,
            shutdown.clone(),
        )))
    } else {
        tracing::info!(
            "local sink output retention GC disabled (--local-output-retention-days 0); \
             outputs are still tracked and can be cleaned on demand"
        );
        None
    };

    // Lease heartbeat + cross-instance orphan recovery (#146 H7). Renews this
    // instance's run leases and reclaims runs whose owning instance's lease has
    // expired. A no-op for the in-memory backend.
    let lease_period = lease_interval(config.lease_ttl);
    let leases = tokio::spawn(lease_loop(state.clone(), lease_period, shutdown.clone()));

    // Cluster claim loop: pulls Pending runs from the shared DB (cluster only).
    let claim = if config.cluster.enabled {
        tracing::info!(
            poll_secs = config.cluster.poll.as_secs(),
            max_attempts = config.cluster.max_attempts,
            "cluster mode enabled; starting claim loop"
        );
        Some(tokio::spawn(crate::serve::cluster::claim_loop(
            state.clone(),
            shutdown.clone(),
        )))
    } else {
        None
    };

    // Event-driven trigger watchers (#196): spawn one supervised task per enabled
    // polling trigger (object_arrival / queue_depth). Webhook triggers are
    // handled by the router — no watcher task needed for them.
    #[cfg(feature = "triggers")]
    let trigger_handles = match &triggers {
        Some(c) => {
            tracing::info!(count = c.triggers.len(), "spawning trigger watchers");
            crate::serve::triggers::spawn_watchers(state.clone(), c, shutdown.clone())
        }
        None => Vec::new(),
    };

    // The HTTP graceful-shutdown future resolves on signal, then drives the run
    // drain *inside itself* — this is load-bearing: `axum::serve(...).await` does
    // not return until every open connection closes, and an open SSE
    // `/v1/runs/{id}/logs` stream stays open until its run ends. If we deferred
    // `shutdown.cancel()` to after the `.await` (as before), a long run with an
    // open SSE stream would deadlock shutdown forever (audit #321 M9): axum waits
    // on the SSE, the SSE waits on the run, the run waits on a cancel that never
    // fires. Draining + cancelling from within the signal handler breaks that
    // cycle — cancelled runs end, their SSE streams close, and axum can return.
    // `into_make_service_with_connect_info` exposes the peer address so the auth
    // layer can record a `source_ip` on audit records (#205).
    let drain_state = state.clone();
    let drain_shutdown = shutdown.clone();
    let drain_grace = config.shutdown_grace;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        wait_for_signal().await;
        tracing::info!("shutdown signal received; draining in-flight runs");
        // Stop pulling NEW work the moment we begin draining.
        if let Some(claim) = claim {
            claim.abort();
        }
        // Grace for in-flight runs to finish naturally, then cooperatively
        // cancel any still running so their sinks flush at the next page
        // boundary AND their SSE streams close.
        let drained =
            tokio::time::timeout(drain_grace, drain_state.registry().wait_drained()).await;
        if drained.is_err() {
            let remaining = drain_state.registry().in_flight();
            tracing::warn!(remaining, "grace window expired; cancelling in-flight runs");
            drain_shutdown.cancel();
        }
    })
    .await
    .map_err(|e| CliError::Serve(format!("server error: {e}")))?;

    // Serve has returned (connections closed). Give any just-cancelled runs the
    // full cooperative-flush grace to write their terminal status / complete an
    // S3 multipart upload — matching the pipeline's own `RUN_FLUSH_GRACE`, not
    // the old hardcoded 5s that cut buffered sinks off early (audit #321 M8).
    let _ = tokio::time::timeout(
        crate::serve::runner::RUN_FLUSH_GRACE,
        state.registry().wait_drained(),
    )
    .await;
    maintenance.abort();
    leases.abort();
    #[cfg(feature = "catalog")]
    if let Some(gc) = local_output_gc {
        gc.abort();
    }
    #[cfg(feature = "triggers")]
    for h in trigger_handles {
        h.abort();
    }
    #[cfg(feature = "templates-sync")]
    for h in sync_handles {
        h.abort();
    }
    // Flush any buffered OTLP telemetry after in-flight runs drain (no-op without
    // the `otel` feature).
    faucet_core::shutdown_otel();
    tracing::info!("faucet serve stopped");
    Ok(())
}

/// Resolve on SIGTERM (Unix) or Ctrl-C (any platform).
async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::history::memory::MemoryHistory;
    use crate::serve::history::{RunRecord, RunStatus};
    use chrono::Utc;
    use std::collections::BTreeMap;

    #[test]
    fn lease_interval_is_third_of_ttl_floored_at_one_sec() {
        assert_eq!(
            lease_interval(Duration::from_secs(30)),
            Duration::from_secs(10)
        );
        assert_eq!(
            lease_interval(Duration::from_secs(90)),
            Duration::from_secs(30)
        );
        // Floor: a tiny TTL still heartbeats at least once per second.
        assert_eq!(
            lease_interval(Duration::from_secs(1)),
            Duration::from_secs(1)
        );
        assert_eq!(
            lease_interval(Duration::from_secs(2)),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn purge_interval_is_quarter_of_shorter_window_clamped() {
        // Defaults (retain 7d, idem 1d) → min 1d, /4 = 6h → clamped to the 1h cap.
        assert_eq!(
            purge_interval(Duration::from_secs(604_800), Duration::from_secs(86_400)),
            Duration::from_secs(3600)
        );
        // A short idempotency window drives a faster cadence (but never below 60s).
        assert_eq!(
            purge_interval(Duration::from_secs(604_800), Duration::from_secs(120)),
            Duration::from_secs(60)
        );
        // Both tiny → the 60s floor.
        assert_eq!(
            purge_interval(Duration::from_secs(1), Duration::from_secs(1)),
            Duration::from_secs(60)
        );
        // A 40-minute window lands inside the range: 2400/4 = 600s.
        assert_eq!(
            purge_interval(Duration::from_secs(2400), Duration::from_secs(2400)),
            Duration::from_secs(600)
        );
    }

    #[tokio::test]
    async fn maintenance_loop_purges_expired_terminal_runs() {
        let history: Arc<dyn RunHistory> = Arc::new(MemoryHistory::new(Duration::from_secs(60)));

        // An old terminal record (eligible for purge with retain=0) and a
        // non-terminal one (must be kept).
        let mut old = RunRecord::queued(
            "old".into(),
            None,
            BTreeMap::new(),
            None,
            Utc::now() - chrono::Duration::seconds(10),
        );
        old.status = RunStatus::Completed;
        old.finished_at = Some(Utc::now() - chrono::Duration::seconds(10));
        history.upsert(&old).await.unwrap();
        let live = RunRecord::queued("live".into(), None, BTreeMap::new(), None, Utc::now());
        history.upsert(&live).await.unwrap();

        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(maintenance_loop(
            history.clone(),
            Duration::ZERO,            // retain=0 → every terminal record is expired
            Duration::ZERO,            // log_retain=0 → not exercised by this test
            Duration::from_millis(10), // fast tick for the test
            shutdown.clone(),
        ));

        // Allow several ticks (the first is consumed at t=0).
        tokio::time::sleep(Duration::from_millis(80)).await;
        shutdown.cancel();
        let _ = handle.await;

        assert!(
            history.get("old").await.unwrap().is_none(),
            "expired terminal run should have been purged by the maintenance loop"
        );
        assert!(
            history.get("live").await.unwrap().is_some(),
            "non-terminal run must be kept"
        );
    }
}
