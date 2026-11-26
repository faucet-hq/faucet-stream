//! axum router assembly and the bind / graceful-shutdown serve loop.

use crate::error::{CliError, CliResult};
use crate::serve::config::ServeConfig;
use crate::serve::handlers::state as state_h;
use crate::serve::handlers::{
    audit, backfill, changes, dlq, doctor, health, logs, overview, plan, reload, runs, schemas,
    verify, whoami,
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

/// How long the startup pull of one template-sync origin may take before the
/// server opens its listener without it.
#[cfg(feature = "templates-sync")]
const INITIAL_TEMPLATE_SYNC_TIMEOUT: Duration = Duration::from_secs(120);

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
        not(any(
            feature = "triggers",
            feature = "catalog",
            feature = "templates",
            feature = "tenants"
        )),
        allow(unused_mut)
    )]
    let mut api = Router::new()
        .route("/v1/runs", post(runs::submit_run).get(runs::list_runs))
        .route(
            "/v1/runs/{id}",
            get(runs::get_run_detail).delete(runs::delete_run),
        )
        .route("/v1/runs/{id}/cancel", post(runs::cancel_run))
        .route("/v1/runs/{id}/rollback", post(runs::rollback_run))
        .route("/v1/runs/{id}/logs", get(logs::stream_logs))
        .route("/v1/runs/{id}/config", get(runs::get_run_config))
        .route("/v1/overview", get(overview::overview))
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
        .route("/v1/audit/{id}", get(audit::get_audit))
        .route("/v1/reload", post(reload::reload))
        .route("/v1/whoami", get(whoami::whoami))
        .route(
            "/v1/status",
            get(state_h::get_status).post(state_h::post_status),
        )
        .route(
            "/v1/mirror/{name}",
            get(state_h::get_mirror).post(state_h::post_mirror),
        )
        .route(
            "/v1/state/{pipeline}/{row}",
            get(state_h::get_state)
                .put(state_h::put_state)
                .delete(state_h::delete_state),
        )
        // Change requests (#703): plan → approve → run.
        .route(
            "/v1/changes",
            post(changes::create_change).get(changes::list_changes),
        )
        .route("/v1/changes/counts", get(changes::count_changes))
        .route("/v1/changes/{id}", get(changes::get_change))
        .route("/v1/changes/{id}/approve", post(changes::approve_change))
        .route("/v1/changes/{id}/reject", post(changes::reject_change));
    #[cfg(feature = "triggers")]
    {
        api = api
            .route(
                "/v1/triggers",
                get(crate::serve::triggers::list::list_triggers),
            )
            .route(
                "/v1/triggers/{name}",
                post(crate::serve::triggers::webhook::handle)
                    .put(crate::serve::triggers::webhook::handle),
            );
    }
    // Schedules (#1044): a build without them still answers the list, with
    // `available: false`.
    #[cfg(not(all(feature = "schedule", feature = "templates")))]
    {
        api = api.route(
            "/v1/schedules",
            get(crate::serve::handlers::schedules::list),
        );
    }
    // Feeds (#1034). The ingest routes are a router of their own.
    #[cfg(feature = "feeds")]
    {
        use crate::serve::handlers::feeds as f;
        use axum::routing::put;
        api = api
            .route("/v1/feeds", get(f::list).post(f::create))
            .route(
                "/v1/feeds/{name}",
                get(f::get).put(f::update).delete(f::delete),
            )
            .route("/v1/feeds/{name}/deliveries", post(f::delivery_create))
            .route(
                "/v1/feeds/{name}/deliveries/{delivery}",
                put(f::delivery_update).delete(f::delivery_delete),
            )
            .route(
                "/v1/feeds/{name}/deliveries/{delivery}/pause",
                post(f::pause),
            )
            .route(
                "/v1/feeds/{name}/deliveries/{delivery}/resume",
                post(f::resume),
            )
            .route(
                "/v1/feeds/{name}/deliveries/{delivery}/deliver-now",
                post(f::deliver_now),
            )
            .route(
                "/v1/feeds/{name}/deliveries/{delivery}/replay",
                post(f::replay),
            )
            .route("/v1/feeds/{name}/deliveries/{delivery}/skip", post(f::skip))
            .route(
                "/v1/feeds/{name}/deliveries/{delivery}/recover",
                post(f::recover),
            )
            .route("/v1/feeds/{name}/adopt-store", post(f::adopt_store))
            .route(
                "/v1/feeds/{name}/tokens",
                get(f::tokens).post(f::token_create),
            )
            .route(
                "/v1/feeds/{name}/tokens/{id}",
                axum::routing::delete(f::token_revoke),
            )
            .route("/v1/feeds/{name}/events", get(f::events))
            .route("/v1/feeds/{name}/events/{seq}", get(f::event))
            .route("/v1/feeds/{name}/refusals", get(f::refusals));
    }
    #[cfg(all(feature = "schedule", feature = "templates"))]
    {
        use crate::serve::handlers::schedules as s;
        api = api
            .route("/v1/schedules", get(s::list).post(s::create))
            .route("/v1/schedules/preview", post(s::preview))
            .route(
                "/v1/schedules/{name}",
                get(s::get).put(s::update).delete(s::delete),
            )
            .route("/v1/schedules/{name}/pause", post(s::pause))
            .route("/v1/schedules/{name}/resume", post(s::resume))
            .route("/v1/schedules/{name}/run", post(s::run));
    }
    #[cfg(feature = "catalog")]
    {
        use crate::serve::handlers::{catalog, local_outputs, preview, usage};
        api = api
            // Cost & usage accounting (#704): priced usage of finished runs.
            .route("/v1/usage", get(usage::list_usage))
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
            .route(
                "/v1/templates/{id}/runs",
                post(templates::trigger_template).get(templates::list_template_runs),
            )
            .route("/v1/templates/{id}/rows", get(templates::template_rows))
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
            )
            .route(
                "/v1/templates/{id}/versions/{version}/test",
                post(templates::test_version),
            )
            .route(
                "/v1/templates/{id}/versions/{version}/tests",
                get(templates::version_tests),
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
    // Multi-tenant embedded integrations (#709).
    #[cfg(feature = "tenants")]
    {
        use crate::serve::handlers::tenants;
        api = api
            .route(
                "/v1/tenants",
                post(tenants::create_tenant).get(tenants::list_tenants),
            )
            .route(
                "/v1/tenants/{tenant}",
                get(tenants::get_tenant)
                    .patch(tenants::patch_tenant)
                    .delete(tenants::delete_tenant),
            )
            .route(
                "/v1/tenants/{tenant}/connections",
                get(tenants::list_connections).post(tenants::create_connection),
            )
            .route(
                "/v1/tenants/{tenant}/connections/{name}",
                get(tenants::get_connection)
                    .put(tenants::put_connection)
                    .delete(tenants::delete_connection),
            )
            .route(
                "/v1/tenants/{tenant}/connect/{provider}",
                post(tenants::start_connect),
            )
            .route(
                "/v1/tenants/{tenant}/runs",
                post(tenants::submit_tenant_run),
            )
            .route(
                "/v1/tenants/{tenant}/templates/{id}/runs",
                post(tenants::trigger_tenant_template),
            )
            .route("/v1/templates/{id}/fanout", post(tenants::fanout_template))
            .route(
                "/v1/connect/providers",
                get(tenants::list_connect_providers),
            );
    }
    // The data-flow policy in the console (#1043).
    #[cfg(feature = "policy")]
    {
        use crate::serve::handlers::policy;
        api = api
            .route(
                "/v1/policy",
                get(policy::get_policy).post(policy::propose_server),
            )
            .route("/v1/policy/versions", get(policy::list_versions))
            .route("/v1/policy/refusals", get(policy::list_refusals))
            .route("/v1/policy/check", post(policy::check))
            .route("/v1/policy/preview", post(policy::preview))
            .route("/v1/policy/rollback", post(policy::rollback_server));
    }
    #[cfg(all(feature = "policy", feature = "tenants"))]
    {
        use crate::serve::handlers::policy;
        api = api
            .route(
                "/v1/tenants/{tenant}/policy",
                get(policy::get_tenant_policy).post(policy::propose_tenant),
            )
            .route(
                "/v1/tenants/{tenant}/policy/preview",
                post(policy::preview_tenant),
            )
            .route(
                "/v1/tenants/{tenant}/policy/rollback",
                post(policy::rollback_tenant),
            );
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
        cors_layer(origins)
    };

    // The hosted-OAuth callback is public: the provider redirects a browser
    // here, and the single-use `state` parameter is the credential (#709).
    #[cfg(feature = "tenants")]
    let public = public.route(
        "/v1/connect/callback",
        get(crate::serve::handlers::tenants::connect_callback),
    );

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

    let guard = std::sync::Arc::new(crate::serve::guard::RequestGuard::new(
        &config.request_guard,
        config.listen,
        matches!(config.auth, crate::serve::config::AuthMode::None),
        &config.cors_origins,
    ));
    let router = router
        // axum's own 2 MiB extractor cap would otherwise win over a larger
        // `--body-limit-bytes`.
        .layer(axum::extract::DefaultBodyLimit::disable())
        .layer(RequestBodyLimitLayer::new(config.body_limit_bytes))
        .layer(axum::middleware::from_fn(metrics::track_metrics))
        .layer(cors)
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            scope_test_policy,
        ))
        .layer(axum::middleware::from_fn_with_state(
            guard.clone(),
            crate::serve::guard::guard,
        ))
        .with_state(state.clone());
    // Ingest routes authenticate themselves; `--feeds-listen` moves them (#1034).
    #[cfg(feature = "feeds")]
    let router = if config.feeds.listen.is_none() {
        router.merge(crate::serve::feeds::ingest::router(state, guard))
    } else {
        router
    };
    #[cfg(not(feature = "feeds"))]
    let _ = (state, guard);
    router
}

/// Run each request as this server's work, so template tests it starts
/// screen for this server's credentials and record its instance (#856).
async fn scope_test_policy(
    axum::extract::State(state): axum::extract::State<ServerState>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    #[cfg(feature = "templates")]
    if let Some(policy) = state.test_policy() {
        return crate::template_tests::trust::with_server_policy(policy, next.run(req)).await;
    }
    #[cfg(not(feature = "templates"))]
    let _ = &state;
    next.run(req).await
}

/// The `--cors-origin` layer: the listed origins may call every `/v1` route
/// with a bearer token, a JSON body and an idempotency key.
fn cors_layer(origins: Vec<axum::http::HeaderValue>) -> CorsLayer {
    use axum::http::{Method, header};
    CorsLayer::new()
        .allow_origin(AllowOrigin::list(origins))
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::PATCH,
            Method::DELETE,
        ])
        .allow_headers([
            header::AUTHORIZATION,
            header::CONTENT_TYPE,
            header::HeaderName::from_static("idempotency-key"),
        ])
        .expose_headers([header::RETRY_AFTER])
}

/// Drain the Prometheus histograms on an interval: they are otherwise drained
/// only when `/metrics` is scraped, so an unscraped server grows without bound.
fn spawn_metrics_upkeep(
    handle: metrics_exporter_prometheus::PrometheusHandle,
    every: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(every);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            handle.run_upkeep();
        }
    })
}

/// Load the optional `--default-config` once at startup, fully resolved, as a
/// merge base `Value`.
async fn load_default_base(config: &ServeConfig) -> CliResult<Option<Value>> {
    match &config.default_config_path {
        None => Ok(None),
        Some(path) => {
            crate::serve::load::protect_file(path);
            // `serve` has no --profile flag; honour FAUCET_PROFILE from the environment directly.
            let profile = std::env::var("FAUCET_PROFILE").ok();
            if let Ok(text) = std::fs::read_to_string(path)
                && let Ok(doc) = crate::yaml::from_str::<Value>(&text)
            {
                crate::serve::load::protect_references(&doc);
            }
            let cfg =
                crate::config::PipelineConfig::from_path_async(path, profile.as_deref()).await?;
            let base = serde_json::to_value(&cfg)
                .map_err(|e| CliError::Serve(format!("serializing --default-config: {e}")))?;
            crate::serve::load::protect_default_state(&base);
            Ok(Some(base))
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
pub(crate) fn lease_interval(lease_ttl: Duration) -> Duration {
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
/// This process's clock minus the history database's, in milliseconds,
/// measured against the midpoint of the round trip; `None` when the read fails.
pub(crate) async fn measure_clock_offset(state: &ServerState) -> Option<i64> {
    let before = chrono::Utc::now();
    let db = state.history().db_now().await.ok()?;
    let after = chrono::Utc::now();
    let offset = clock_offset_ms(before, after, db);
    crate::serve::metrics::set_clock_offset(offset);
    Some(offset)
}

/// `(before + after) / 2 - db` in milliseconds.
pub(crate) fn clock_offset_ms(
    before: chrono::DateTime<chrono::Utc>,
    after: chrono::DateTime<chrono::Utc>,
    db: chrono::DateTime<chrono::Utc>,
) -> i64 {
    let mid = before + (after - before) / 2;
    (mid - db).num_milliseconds()
}

/// Heartbeat this member and refresh the live-member view: the member count
/// and the state format every member reads (#736).
pub(crate) async fn refresh_membership(state: &ServerState, member_ttl: Duration) {
    heartbeat_member(state).await;
    refresh_member_view(state, member_ttl, None).await;
}

/// Whether this server writes a membership heartbeat: in a cluster, and on
/// any SQL history, so servers sharing one history without `--cluster` see
/// each other's tzdata release before a schedule fires (#1116).
pub(crate) fn heartbeats(state: &ServerState) -> bool {
    state.cluster().enabled() || state.persistent_history()
}

/// Write this member's heartbeat row.
pub(crate) async fn heartbeat_member(state: &ServerState) {
    let cluster = state.cluster();
    let clock_offset_ms = measure_clock_offset(state).await;
    let beat = crate::serve::history::InstanceHeartbeat {
        started_at: cluster.started_at(),
        listen: Some(cluster.listen().to_string()),
        max_concurrent: cluster.max_concurrent(),
        in_flight: state.registry().in_flight() as u32,
        state_format: faucet_core::state_version::STATE_FORMAT,
        pools: cluster.pools().to_vec(),
        capabilities: crate::serve::history::instance_capabilities(),
        clock_offset_ms,
    };
    if let Err(e) = state.history().heartbeat_instance(&beat).await {
        tracing::warn!(error = %e, "cluster: heartbeat_instance failed");
    }
}

/// Re-read the live members, the state format they share and the pools,
/// with the reads bounded by `read_bound`.
async fn refresh_member_view(
    state: &ServerState,
    member_ttl: Duration,
    read_bound: Option<Duration>,
) {
    let cluster = state.cluster();
    let members = match read_bound {
        Some(bound) => {
            crate::serve::history::fallback::with_call_bound(
                bound,
                state.history().live_instances(member_ttl),
            )
            .await
        }
        None => state.history().live_instances(member_ttl).await,
    };
    match members {
        Ok(members) => {
            cluster.set_members(members.len());
            cluster.set_state_format(crate::serve::history::cluster_state_format(&members));
            crate::serve::metrics::set_cluster_instances(members.len());
            cluster.set_live_members(members);
        }
        Err(e) => tracing::warn!(error = %e, "cluster: live_instances failed"),
    }
    crate::serve::pools::upkeep(state, read_bound).await;
}

/// How long ago this instance last renewed its leases, and whether it has
/// already fenced itself for the current outage.
#[derive(Debug)]
pub struct LeaseHealth {
    last_renewed: std::time::Instant,
    fenced: bool,
}

impl LeaseHealth {
    pub fn new(now: std::time::Instant) -> Self {
        Self {
            last_renewed: now,
            fenced: false,
        }
    }

    /// When the fence falls due if no renewal succeeds first; `None` once
    /// this outage is fenced.
    pub fn fence_due(&self, fence_after: Duration) -> Option<std::time::Instant> {
        if self.fenced {
            return None;
        }
        self.last_renewed.checked_add(fence_after)
    }

    /// Record one heartbeat outcome. Returns `true` exactly once per outage:
    /// when renewal has failed for `fence_after`, which the caller sets short
    /// of the lease TTL so this instance stops before peers may reclaim.
    pub fn observe(
        &mut self,
        renewed: bool,
        now: std::time::Instant,
        fence_after: Duration,
    ) -> bool {
        if renewed {
            self.last_renewed = now;
            self.fenced = false;
            return false;
        }
        if self.fenced || now.duration_since(self.last_renewed) < fence_after {
            return false;
        }
        self.fenced = true;
        true
    }
}

/// One lease-heartbeat step: probe a degraded history back to health, renew
/// this instance's run (and, clustered, shard) leases, and — clustered — when
/// renewal has failed for `fence_after`, cancel every local run and shard so a
/// peer that reclaims them once their leases expire never runs them at the
/// same time (#789 SERVE-04). Then the tick's upkeep: the policy seed, the
/// feed check and the terminal records still waiting to be written. Returns
/// how many local runs and shards were cancelled.
pub async fn maintain_leases(
    state: &ServerState,
    health: &mut LeaseHealth,
    fence_after: Duration,
) -> usize {
    let cancelled = renew_and_fence(state, health, fence_after).await;
    lease_upkeep(state).await;
    cancelled
}

/// The renewal half of a lease tick. Every call to the history is cut off
/// after [`lease_call_bound`](crate::serve::history::sql::lease_call_bound),
/// and once renewal has failed for `fence_after` the fence fires without
/// waiting on any history call still in flight.
async fn renew_and_fence(
    state: &ServerState,
    health: &mut LeaseHealth,
    fence_after: Duration,
) -> usize {
    use crate::serve::history::fallback::with_call_bound;
    let bound = crate::serve::history::sql::lease_call_bound(state.lease_ttl());
    let mut cancelled = 0usize;
    let recovered = fence_on_time(
        state,
        health,
        fence_after,
        &mut cancelled,
        with_call_bound(bound, state.history().recover_degraded()),
    )
    .await;
    if recovered {
        tracing::info!("run-history backend recovered");
    }
    let forgotten = state.forgotten_runs();
    // The fence counts from before the renewal: the lease a renewal writes
    // runs from the database's clock read at its start, not from when the
    // whole tick ends.
    let attempted = lease_now();
    let renewal =
        tokio::time::timeout_at((attempted + bound).into(), renew_leases(state, &forgotten));
    let renewed = match fence_on_time(state, health, fence_after, &mut cancelled, renewal).await {
        Ok(renewed) => renewed,
        Err(_) => {
            crate::serve::metrics::record_lease_renewal_timeout();
            tracing::warn!(
                waited_ms = bound.as_millis() as u64,
                "lease heartbeat: the run history did not answer in time"
            );
            false
        }
    };
    state.cluster().set_history_reachable(renewed);
    if renewed && state.cluster().enabled() {
        state.cluster().set_fenced(None);
        // A renewal only extends the rows this instance still owns. A run a
        // peer reclaimed (this instance's lease lapsed while it was alive:
        // clock skew, a stall) is no longer among them, and must stop here
        // rather than run on beside the peer's execution.
        let lost = cancel_lost_leases(state, bound).await;
        if lost > 0 {
            return cancelled + lost;
        }
    }
    let seen = if renewed { attempted } else { lease_now() };
    if !health.observe(renewed, seen, fence_after) || !state.cluster().enabled() {
        return cancelled;
    }
    cancelled + fence(state, fence_after)
}

/// The lease clock: the runtime's, so a paused test clock drives it too.
fn lease_now() -> std::time::Instant {
    tokio::time::Instant::now().into_std()
}

/// Await `work`; if the outage reaches its fence instant first (or already
/// has), fence then, and keep awaiting `work`.
async fn fence_on_time<F: std::future::Future>(
    state: &ServerState,
    health: &mut LeaseHealth,
    fence_after: Duration,
    cancelled: &mut usize,
    work: F,
) -> F::Output {
    tokio::pin!(work);
    if let Some(due) = health.fence_due(fence_after) {
        let fire_at = due.max(lease_now());
        match tokio::time::timeout_at(fire_at.into(), &mut work).await {
            Ok(out) => return out,
            Err(_) => {
                if health.observe(false, fire_at, fence_after) && state.cluster().enabled() {
                    *cancelled += fence(state, fence_after);
                }
            }
        }
    }
    work.await
}

/// Renew this instance's run leases and, clustered, its shard leases.
async fn renew_leases(state: &ServerState, forgotten: &[String]) -> bool {
    let mut renewed = true;
    if let Err(e) = state.history().renew_leases_except(forgotten).await {
        tracing::warn!(error = %e, "lease heartbeat (renew_leases) failed");
        renewed = false;
    }
    if state.cluster().enabled()
        && let Err(e) = state.history().renew_shard_leases().await
    {
        tracing::warn!(error = %e, "cluster: renew_shard_leases failed");
        renewed = false;
    }
    renewed
}

/// Cancel every local run and shard: the fence fires `fence_after` into the
/// outage, short of the lease TTL; a cancelled run gets a fraction of the
/// remaining time to flush, so it has stopped writing before a peer may
/// reclaim it.
fn fence(state: &ServerState, fence_after: Duration) -> usize {
    state.cluster().set_fenced(Some(fence_after / 4));
    let cancelled = state.registry().cancel_all();
    tracing::error!(
        cancelled,
        unrenewed_secs = fence_after.as_secs(),
        "cluster: could not renew run leases; cancelling every local run and shard before \
         the leases expire so a peer that reclaims them never runs them concurrently"
    );
    cancelled
}

/// The part of a lease tick that renews nothing: the policy seed and feed
/// check (bounded), then the pending terminal writes (never cut).
async fn lease_upkeep(state: &ServerState) {
    let bound = crate::serve::history::sql::lease_call_bound(state.lease_ttl());
    crate::serve::history::fallback::with_call_bound(bound, async {
        #[cfg(feature = "policy")]
        if let Err(e) = retry_policy_seed(state).await {
            tracing::error!(error = %e, "seeding the policy store from the --policy file failed; new work waits for it");
        }
        #[cfg(feature = "feeds")]
        state.feeds().lease_tick(state).await;
    })
    .await;
    crate::serve::runner::retry_pending_finalize(state).await;
}

/// Cancel every local run and shard whose lease a peer now holds (the row is
/// live but owned by another instance, or requeued). Returns how many.
async fn cancel_lost_leases(state: &ServerState, bound: Duration) -> usize {
    let keys = state.registry().live_keys();
    if keys.is_empty() {
        return 0;
    }
    let lost = match tokio::time::timeout(bound, state.history().leases_lost(&keys)).await {
        Ok(Ok(lost)) => lost,
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "cluster: lease ownership check failed; checking each run");
            leases_lost_one_by_one(state, &keys, bound).await
        }
        Err(_) => {
            crate::serve::metrics::record_lease_renewal_timeout();
            tracing::warn!("cluster: lease ownership check did not answer in time");
            return 0;
        }
    };
    for key in &lost {
        tracing::error!(
            key = %key,
            "cluster: another instance holds this run's lease; cancelling the local execution"
        );
        state.registry().cancel(key);
    }
    lost.len()
}

/// The keys a peer holds, asked one at a time within `bound`.
async fn leases_lost_one_by_one(
    state: &ServerState,
    keys: &[String],
    bound: Duration,
) -> Vec<String> {
    let deadline = tokio::time::Instant::now() + bound;
    let mut lost = Vec::new();
    for key in keys {
        let check = async {
            match key.split_once("::") {
                Some((run_id, shard_id)) => {
                    state.history().shard_lease_lost(run_id, shard_id).await
                }
                None => state.history().lease_lost(key).await,
            }
        };
        match tokio::time::timeout_at(deadline, check).await {
            Ok(Ok(true)) => lost.push(key.clone()),
            Ok(Ok(false)) => {}
            Ok(Err(e)) => {
                tracing::debug!(key = %key, error = %e, "cluster: lease ownership check failed")
            }
            Err(_) => break,
        }
    }
    lost
}

/// Aborts the task when dropped.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(crate) async fn lease_loop(state: ServerState, period: Duration, shutdown: CancellationToken) {
    // Member-liveness window ≈ the real lease TTL (period == lease_ttl/3), so a
    // member must miss ~3 heartbeats before peers treat it as gone.
    let member_ttl = period.saturating_mul(3);
    let bound = crate::serve::history::sql::lease_call_bound(state.lease_ttl());
    let mut health = LeaseHealth::new(lease_now());
    let mut upkeep: Option<AbortOnDrop> = None;
    let mut heartbeat: Option<AbortOnDrop> = None;
    let mut tick = tokio::time::interval(period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tick.tick().await; // consume the immediate first tick
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tick.tick() => {
                renew_and_fence(&state, &mut health, member_ttl.saturating_sub(period)).await;
                if heartbeats(&state) {
                    if heartbeat.as_ref().is_some_and(|beat| !beat.0.is_finished()) {
                        tracing::warn!("cluster: the previous membership heartbeat is still running; skipping this one");
                    } else {
                        let state = state.clone();
                        heartbeat = Some(AbortOnDrop(tokio::spawn(async move {
                            if tokio::time::timeout(bound, heartbeat_member(&state)).await.is_err() {
                                tracing::warn!("cluster: the membership heartbeat did not answer in time");
                            }
                        })));
                    }
                }
                // Upkeep runs beside the loop, one pass at a time.
                if upkeep.as_ref().is_some_and(|pass| !pass.0.is_finished()) {
                    crate::serve::metrics::record_lease_upkeep_skipped();
                    tracing::warn!("lease upkeep from an earlier tick is still running; skipping this one");
                } else {
                    upkeep = Some(AbortOnDrop(tokio::spawn(lease_upkeep_pass(
                        state.clone(),
                        member_ttl,
                    ))));
                }
            }
        }
    }
    if let Some(mut beat) = heartbeat.take() {
        let _ = (&mut beat.0).await;
    }
    if let Some(mut pass) = upkeep.take() {
        let _ = (&mut pass.0).await;
    }
}

/// Everything a lease tick does besides renewing and heartbeating. History
/// calls are bounded, except the terminal writes whose results fire
/// completion side effects.
async fn lease_upkeep_pass(state: ServerState, member_ttl: Duration) {
    use crate::serve::history::fallback::with_call_bound;
    let cluster = state.cluster().clone();
    lease_upkeep(&state).await;
    let bound = crate::serve::history::sql::lease_call_bound(state.lease_ttl());
    let state = &state;
    if cluster.enabled() {
        refresh_member_view(state, member_ttl, Some(bound)).await;
        with_call_bound(bound, cluster_reclaim(state, &cluster)).await;
        // Mode B (#230 / F11): finalize any `sharded` parent whose
        // shards are all terminal but which no shard task finalized
        // inline (e.g. the coordinator crashed after the last shard
        // completed on another instance). Status-fenced + metric
        // recorded inside the backend, so a parent already finalized by
        // `maybe_finalize_parent` is not re-finalized or double-counted.
        match crate::serve::runner::sweep_sharded_parents(state).await {
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
        with_call_bound(bound, async {
            // Single-instance: mark orphans failed. This instance's own
            // runs are live or held for a retry, never orphans, unless
            // their terminal record was dropped (#1050).
            let forgotten = state.forgotten_runs();
            match state.history().recover_orphans_of_peers(&forgotten).await {
                Ok(n) if n > 0 => tracing::warn!(
                    recovered = n,
                    "recovered orphaned runs from an expired-lease instance"
                ),
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "orphan recovery failed"),
            }
        })
        .await;
    }
    with_call_bound(bound, clear_settled_forgotten(state)).await;
}

/// Failover reclaim, the sealed-param purge and the flagged-shard cancel.
async fn cluster_reclaim(state: &ServerState, cluster: &crate::serve::cluster::ClusterHandle) {
    // Failover reclaim (re-run orphans).
    match state
        .history()
        .reclaim_orphans(cluster.max_attempts())
        .await
    {
        Ok(r) if r.requeued > 0 || r.failed > 0 => {
            crate::serve::metrics::record_runs_reclaimed(r.requeued, r.failed);
            tracing::warn!(
                requeued = r.requeued,
                failed = r.failed,
                "cluster: reclaimed orphaned runs from an expired-lease instance"
            );
        }
        Ok(_) => {}
        Err(e) => tracing::warn!(error = %e, "cluster: reclaim_orphans failed"),
    }
    // Mode B (#230): rebalance shards whose owner's lease expired
    // (requeue → another worker, or poison past max_attempts).
    match state.history().reclaim_shards(cluster.max_attempts()).await {
        Ok(r) if r.requeued > 0 || r.failed > 0 => {
            crate::serve::metrics::record_shards_reclaimed(r.requeued, r.failed);
            tracing::warn!(
                requeued = r.requeued,
                failed = r.failed,
                "cluster: reclaimed orphaned shards from an expired-lease instance"
            );
        }
        Ok(_) => {}
        Err(e) => tracing::warn!(error = %e, "cluster: reclaim_shards failed"),
    }
    // Sealed secret params outlive no terminal run (#956).
    match state.history().run_secrets_purge_terminal().await {
        Ok(n) if n > 0 => tracing::debug!(
            purged = n,
            "cluster: deleted the sealed secret params of finished runs"
        ),
        Ok(_) => {}
        Err(e) => {
            tracing::warn!(error = %e, "cluster: purging sealed secret params failed")
        }
    }
    // A cancel recorded for a sharded run stops its pending
    // shards wherever it was requested.
    match state.history().cancel_flagged_shards().await {
        Ok(n) if n > 0 => tracing::info!(
            cancelled = n,
            "cluster: cancelled the pending shards of cancelled runs"
        ),
        Ok(_) => {}
        Err(e) => tracing::warn!(error = %e, "cluster: cancelling flagged shards failed"),
    }
}

/// Drop from the forgotten set every run whose record is now terminal, or
/// gone, so the set stays bounded (#1050).
async fn clear_settled_forgotten(state: &ServerState) {
    for id in state.forgotten_runs() {
        match state.history().get(&id).await {
            Ok(Some(rec)) if !rec.status.is_terminal() => {}
            Ok(_) => state.clear_forgotten_run(&id),
            Err(_) => {}
        }
    }
}

/// Put the `--policy` file in its place (#1043). With `--policy-store file`,
/// or with nowhere to keep versions (in-memory history), the file is the
/// policy, and its content is stored once under its content address for the
/// runs pinned to it (#1051). Otherwise it only seeds an empty store; a store that already holds
/// a version keeps it, and a file that differs is reported, not applied.
/// While the history is unreachable the seed stays pending and admission
/// waits for it (#1050); any other failure is the caller's.
#[cfg(feature = "policy")]
pub async fn apply_policy_file(
    state: &ServerState,
    store: crate::serve::config::PolicyStore,
    policy: std::sync::Arc<faucet_core::PolicySpec>,
    path: &std::path::Path,
) -> Result<(), crate::serve::error::ServeError> {
    use crate::serve::policies::{PolicyFile, PolicySeed};
    let file = PolicyFile::read(path);
    if store == crate::serve::config::PolicyStore::File || !state.persistent_history() {
        tracing::info!(rules = policy.rules.len(), path = %file.path, "data-flow policy loaded from file");
        state.set_policy(policy);
        state.set_policy_file(file);
        // Runs are pinned to its content, stored once here; until it is,
        // nothing is admitted (#1051).
        state.set_policy_seed_pending(true);
        return retry_policy_seed(state).await;
    }
    state.set_policy_seed(PolicySeed {
        file,
        spec: (*policy).clone(),
    });
    state.set_policy_seed_pending(true);
    retry_policy_seed(state).await
}

/// Seed the policy store from the `--policy` file while the seed is pending
/// (at startup, then from every lease tick). An unreachable history keeps it
/// pending; any other failure is returned and the seed stays pending.
#[cfg(feature = "policy")]
pub async fn retry_policy_seed(state: &ServerState) -> Result<(), crate::serve::error::ServeError> {
    use crate::serve::policies::{self, SeedOutcome};
    if !state.policy_seed_pending() {
        return Ok(());
    }
    if state.policy_file().is_some() {
        return match policies::store_file_scope(state).await {
            Ok(()) => {
                state.set_policy_seed_pending(false);
                Ok(())
            }
            Err(crate::serve::error::ServeError::Unavailable(m)) => {
                tracing::warn!(
                    error = %m,
                    "the run history is unreachable: the --policy file is stored once it \
                     answers, and new work waits until then"
                );
                Ok(())
            }
            Err(e) => Err(e),
        };
    }
    let Some(seed) = state.policy_seed() else {
        return Ok(());
    };
    match policies::seed_store(state, &seed).await {
        Ok(outcome) => {
            state.set_policy_seed_pending(false);
            match outcome {
                SeedOutcome::Imported => tracing::info!(
                    path = %seed.file.path,
                    "the policy store was empty: imported the --policy file as version 1"
                ),
                SeedOutcome::Kept { differs: false } => tracing::info!(
                    path = %seed.file.path,
                    "the stored policy version is in force; the --policy file matches it"
                ),
                SeedOutcome::Kept { differs: true } => tracing::warn!(
                    path = %seed.file.path,
                    "the --policy file differs from the policy version in force; it was not \
                     applied (import it as a new version from the console, or start with \
                     --policy-store file)"
                ),
            }
            Ok(())
        }
        Err(crate::serve::error::ServeError::Unavailable(m)) => {
            tracing::warn!(
                error = %m,
                "the run history is unreachable: the --policy file seeds the policy store once \
                 it answers, and new work waits until then"
            );
            Ok(())
        }
        Err(e) => Err(e),
    }
}

/// Boot the server: install observability, build state + router, bind, serve
/// until SIGTERM/SIGINT, then drain in-flight runs up to the grace window.
pub async fn serve(config: ServeConfig, mcp: crate::serve::McpServeSettings) -> CliResult<()> {
    let (prom, log_hub) = crate::serve::observability::install_with(
        &config.log_level,
        config.log_format,
        config.otel.as_ref(),
        config.metrics_idle_timeout,
    );
    if let (Some(handle), Some(idle)) = (&prom, config.metrics_idle_timeout) {
        tokio::spawn(faucet_core::observability::prune_idle_series(
            handle.clone(),
            idle,
        ));
    }
    crate::serve::callback::set_allow_hosts(config.callback_allow_hosts.clone());
    let short = crate::serve::auth::short_tokens(&config.auth);
    if short > 0 {
        tracing::warn!(
            tokens = short,
            min_len = crate::serve::auth::MIN_TOKEN_LEN,
            "a bearer token is shorter than the recommended minimum; use a long random value \
             (for example `openssl rand -hex 32`)"
        );
    }
    crate::serve::references::install(config.references.clone());
    // Unscoped connector builds may not use the server's own identity.
    faucet_core::resolve::deny_unscoped_host_credentials();
    crate::serve::metrics::set_cluster_enabled(config.cluster.enabled);
    if let Some(h) = &prom {
        spawn_metrics_upkeep(h.clone(), Duration::from_secs(5));
    }

    // This process's identity for run-ownership leases (#146 H7). A fresh id per
    // process, so a restarted instance recovers its prior incarnation's runs only
    // once their lease expires — never another live instance's heartbeated runs.
    let instance_id = uuid::Uuid::new_v4().to_string();
    tracing::info!(
        instance_id = %instance_id,
        lease_ttl_secs = config.lease_ttl.as_secs(),
        "faucet serve instance id"
    );

    if let crate::serve::config::HistoryBackendSpec::Sqlite(url) = &config.history {
        crate::serve::load::protect_sqlite_url(url);
    }
    if let Ok(cwd) = std::env::current_dir() {
        crate::serve::load::protect_env_files(&cwd, config.env_file.as_deref());
    }
    faucet_core::path_guard::install(Box::new(crate::serve::load::server_file_refusal));
    for path in [
        &config.policy_path,
        &config.triggers_path,
        &config.templates_sync_path,
        &config.connect_providers_path,
        &config.default_config_path,
    ]
    .into_iter()
    .flatten()
    {
        crate::serve::load::protect_file(path);
    }
    #[cfg(feature = "catalog")]
    if let Some(key) = crate::catalog::snapshot::key_file_path() {
        crate::serve::load::protect_file(&key);
    }
    // The default history file (#1049): claim its directory first, and hold
    // the lock until this server stops.
    let history_lock = crate::serve::config::claim_default_history(&config.history)?;
    if matches!(
        config.history,
        crate::serve::config::HistoryBackendSpec::Memory
    ) {
        tracing::warn!(
            "--history memory: the run history is kept in this process only; on restart it \
             loses {}",
            crate::serve::config::MEMORY_HISTORY_LOSES
        );
    }
    let history = crate::serve::history::connect(
        &config.history,
        config.idempotency_retention,
        config.lease_ttl,
        &instance_id,
    )
    .await?;
    if history_lock.is_some() && history.degraded() {
        // The default file could not be opened: refuse rather than run on
        // memory without being asked to.
        return Err(CliError::Serve(format!(
            "could not open the default run history in {}; set $FAUCET_DATA_DIR or --history, \
             or --history memory to keep nothing across restarts",
            crate::serve::config::default_data_dir().display()
        )));
    }
    let _history_lock = history_lock;
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
    // With log shipping on (#806) the log store is the shipping buffer, so it
    // is enabled whatever the backend — the in-memory one is a non-durable
    // buffer, lost on restart.
    let shipping = crate::serve::log_export::LogExport::from_config(&config).configured();
    let memory_history = matches!(
        config.history,
        crate::serve::config::HistoryBackendSpec::Memory
    );
    if crate::serve::log_export::LogExport::unsupported(&config) {
        tracing::warn!(
            "--otel-config exports `logs` but this binary was built without --features otel; \
             run logs are not shipped"
        );
    }
    if shipping && memory_history {
        tracing::warn!(
            "log shipping is on with --history memory: undelivered run logs are buffered in \
             memory and lost if the server restarts; use a SQLite or Postgres --history for a \
             durable buffer"
        );
    }
    if shipping || (!config.log_retention.is_zero() && !memory_history) {
        log_hub.enable_persistence(history.clone(), config.log_max_lines_per_run);
        tracing::info!(
            retention_secs = config.log_retention.as_secs(),
            max_lines_per_run = config.log_max_lines_per_run,
            shipping,
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
    // Schedules (#1044): load + validate the file fail-fast.
    #[cfg(all(feature = "schedule", feature = "templates"))]
    let schedules_file = match &config.schedules.path {
        Some(path) => crate::serve::schedules::load_file(path).await?.schedules,
        None => Vec::new(),
    };
    #[cfg(not(all(feature = "schedule", feature = "templates")))]
    if config.schedules.path.is_some() {
        return Err(CliError::Serve(
            "--schedules requires a build with the `schedule` and `templates` features".into(),
        ));
    }
    // Feeds (#1034): validate the file first and protect every SQLite feed store.
    #[cfg(feature = "feeds")]
    let feeds_file = match &config.feeds.path {
        Some(path) => crate::serve::feeds::spec::load_file(path).await?,
        None => Default::default(),
    };
    #[cfg(feature = "feeds")]
    for target in crate::serve::feeds::stores::targets(&config.feeds, &config.history)
        .map_err(CliError::Serve)?
        .values()
    {
        if let crate::serve::feeds::stores::StoreTarget::Sqlite(path) = target {
            for file in crate::serve::feeds::stores::protected_files(path) {
                crate::serve::load::protect_file(&file);
            }
        }
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
    if config.policy_store == crate::serve::config::PolicyStore::File
        && config.policy_path.is_none()
    {
        return Err(CliError::Serve(
            "--policy-store file needs --policy <file>: the file is the policy".into(),
        ));
    }

    // The vault key (#709, #956), validated before the listener opens.
    #[cfg(feature = "encryption")]
    let vault = match &config.vault {
        Some(v) => Some(std::sync::Arc::new(
            crate::serve::vault::Vault::new(&v.key, &v.previous).map_err(CliError::Serve)?,
        )),
        None => None,
    };
    #[cfg(not(feature = "encryption"))]
    if config.vault.is_some() {
        return Err(CliError::Serve(
            "--vault-key requires a build with the `encryption` feature".into(),
        ));
    }
    // Tenants (#709): the hosted-OAuth providers.
    #[cfg(feature = "tenants")]
    let tenants_runtime = {
        let providers = match &config.connect_providers_path {
            Some(path) => crate::serve::tenants::connect::ConnectProviders::load(path)
                .map_err(CliError::Serve)?,
            None => Default::default(),
        };
        if vault.is_none() && !providers.names().is_empty() {
            return Err(CliError::Serve(
                "--connect-providers needs --vault-key (or FAUCET_VAULT_KEY): a connect \
                 flow stores the tenant's grant sealed under it"
                    .into(),
            ));
        }
        let mut rt = crate::serve::tenants::TenantsRuntime::new(None, providers);
        rt.vault = vault.clone();
        rt
    };
    #[cfg(not(feature = "tenants"))]
    if config.connect_providers_path.is_some() {
        return Err(CliError::Serve(
            "--connect-providers requires a build with the `tenants` feature".into(),
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
    // Template versions this server's own files pin are never pruned.
    {
        #[allow(unused_mut)]
        let mut references: Vec<serde_json::Value> = Vec::new();
        #[cfg(all(feature = "schedule", feature = "templates"))]
        references.extend(
            schedules_file
                .iter()
                .filter_map(|s| serde_json::to_value(&s.template).ok()),
        );
        #[cfg(feature = "triggers")]
        if let Some(t) = &triggers {
            references.extend(
                t.triggers
                    .iter()
                    .filter_map(|c| c.spec.template.as_ref())
                    .filter_map(|t| serde_json::to_value(t).ok()),
            );
        }
        #[cfg(feature = "feeds")]
        references.extend(
            feeds_file
                .feeds
                .iter()
                .flat_map(|f| f.deliveries.iter())
                .filter_map(|d| d.template.as_ref())
                .filter_map(|t| serde_json::to_value(t).ok()),
        );
        state.history().set_file_pins(
            references
                .iter()
                .flat_map(crate::serve::history::templates::reference_pins)
                .collect(),
        );
    }
    // Template tests this server runs screen for its credentials and record
    // this instance on their results (#856).
    #[cfg(feature = "templates")]
    {
        state.set_test_policy(crate::template_tests::trust::ServerTestPolicy {
            instance: instance_id.clone(),
        });
        crate::templates::metrics::describe();
    }
    #[cfg(feature = "policy")]
    if let (Some(p), Some(path)) = (policy, &config.policy_path) {
        apply_policy_file(&state, config.policy_store, p, path)
            .await
            .map_err(|e| CliError::Serve(format!("seeding the policy store from --policy: {e}")))?;
    }
    #[cfg(all(feature = "encryption", feature = "masking"))]
    crate::dlq_replay::proof::install_keys(
        vault
            .as_ref()
            .map(|v| v.derive(crate::dlq_replay::proof::KEY_LABEL))
            .unwrap_or_default(),
    );
    #[cfg(feature = "encryption")]
    state.set_vault(vault);
    #[cfg(feature = "tenants")]
    {
        crate::serve::tenants::metrics::describe();
        state.set_tenants(tenants_runtime);
        crate::serve::tenants::reseal_plain_notifications(&state).await;
        crate::serve::tenants::metrics::refresh_connection_gauges(&state).await;
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
                let synced = crate::templates::sync::sync_origin(&store, origin, false, None);
                let synced = async {
                    match state.test_policy() {
                        Some(p) => {
                            crate::template_tests::trust::with_server_policy(p, synced).await
                        }
                        None => synced.await,
                    }
                };
                let synced = match tokio::time::timeout(INITIAL_TEMPLATE_SYNC_TIMEOUT, synced).await
                {
                    Ok(r) => r,
                    Err(_) => Err(CliError::Config(format!(
                        "no answer within {}s; the periodic pull retries it",
                        INITIAL_TEMPLATE_SYNC_TIMEOUT.as_secs()
                    ))),
                };
                match synced {
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
                state.test_policy(),
            )
        }
        None => Vec::new(),
    };
    // On a new faucet release line, re-test every label holder and rollback
    // target in the background; it records results only (#856).
    #[cfg(feature = "templates")]
    {
        let store: crate::templates::TemplateStore = state.history();
        let policy = state.test_policy();
        tokio::spawn(async move {
            let revalidated = crate::templates::revalidate::revalidate_if_new_release(&store);
            let revalidated = match policy {
                Some(p) => crate::template_tests::trust::with_server_policy(p, revalidated).await,
                None => revalidated.await,
            };
            match revalidated {
                Ok(Some(r)) => tracing::info!(
                    versions = r.versions.len(),
                    failed = r.failed(),
                    faucet = %r.faucet,
                    "revalidated template versions under a new faucet release"
                ),
                Ok(None) => {}
                Err(e) => tracing::warn!(error = %e, "template revalidation failed"),
            }
        });
    }
    let app = build_router(state.clone(), &config, &mcp);

    let listener = tokio::net::TcpListener::bind(config.listen)
        .await
        .map_err(|e| CliError::Serve(format!("failed to bind {}: {e}", config.listen)))?;
    let local = listener
        .local_addr()
        .map_err(|e| CliError::Serve(e.to_string()))?;
    tracing::info!(listen = %local, "faucet serve listening");
    // `--feeds-listen` (#1034): the ingest routes' own listener.
    #[cfg(feature = "feeds")]
    let feeds_listener = match config.feeds.listen {
        Some(addr) => {
            let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
                CliError::Serve(format!("failed to bind --feeds-listen {addr}: {e}"))
            })?;
            let local = listener
                .local_addr()
                .map_err(|e| CliError::Serve(e.to_string()))?;
            tracing::info!(listen = %local, "feed ingest listening");
            Some((listener, local))
        }
        None => None,
    };

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
        Duration::ZERO,
        purge_period,
        shutdown.clone(),
    ));
    // Run-log retention + log shipping (#806): delivery-aware retention and
    // the OTLP shipper when `--otel-config` exports logs, time-based
    // retention otherwise.
    let log_shipper = tokio::spawn(crate::serve::log_export::run_loop(
        state.log_export().clone(),
        state.history(),
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

    // Change-request expiry sweep (#703): a pending request past its window
    // lapses even if nobody looks at it.
    if !state.require_approval().is_empty() {
        tracing::info!(
            kinds = ?state.require_approval(),
            "approval required for these change kinds"
        );
    }
    let change_expiry = tokio::spawn(crate::serve::changes::expiry_loop(
        state.clone(),
        Duration::from_secs(60),
        shutdown.clone(),
    ));

    // Cluster claim loop: pulls Pending runs from the shared DB (cluster only).
    // Membership is read first, so the first claimed run already knows whether
    // an older member still needs bare bookmarks (#736).
    let claim = if config.cluster.enabled {
        refresh_membership(&state, lease_period.saturating_mul(3)).await;
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
        if heartbeats(&state) {
            heartbeat_member(&state).await;
        }
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

    // Schedules (#1044): one loop ticks the file's schedules and the stored
    // ones; every cluster member runs it and the per-tick idempotency key lets
    // one run through.
    #[cfg(all(feature = "schedule", feature = "templates"))]
    let schedule_loop = {
        tracing::info!(file = schedules_file.len(), "starting the schedule loop");
        state.set_schedules(crate::serve::schedules::SchedulesRuntime::new(
            schedules_file,
            config.schedules.tenant_min_interval,
        ));
        crate::serve::schedules::spawn(state.clone(), shutdown.clone())
    };

    // Feeds (#1034): a `--feeds` file with feeds starts the scheduler now.
    #[cfg(feature = "feeds")]
    {
        let stores = crate::serve::feeds::stores::Stores::from_config(&config);
        state.set_feeds(crate::serve::feeds::FeedsRuntime::new(
            config.feeds.clone(),
            feeds_file,
            stores,
        ));
        crate::serve::feeds::service::check_file_deliveries(&state)
            .await
            .map_err(CliError::Serve)?;
        crate::serve::feeds::service::refuse_moved_file_feeds(&state)
            .await
            .map_err(CliError::Serve)?;
        state.feeds().lease_tick(&state).await;
    }

    // The HTTP graceful-shutdown future resolves on signal, then drives the run
    // drain *inside itself* — this is load-bearing: `axum::serve(...).await` does
    // not return until every open connection closes, and an open SSE
    // `/v1/runs/{id}/logs` stream stays open until its run ends. If we deferred
    // `shutdown.cancel()` to after the `.await` (as before), a long run with an
    // open SSE stream would deadlock shutdown forever (audit #321 M9): axum waits
    // on the SSE, the SSE waits on the run, the run waits on a cancel that never
    // fires. Draining + cancelling from within the signal handler breaks that
    // cycle — cancelled runs end, their SSE streams close, and axum can return.
    #[cfg(feature = "feeds")]
    let ingest_stop = CancellationToken::new();
    #[cfg(feature = "feeds")]
    let ingest_server = feeds_listener.map(|(listener, local)| {
        let app = crate::serve::feeds::ingest::listener_router(state.clone(), &config.feeds, local);
        let stop = ingest_stop.clone();
        tokio::spawn(async move {
            let served = axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(stop.cancelled_owned())
            .await;
            if let Err(e) = served {
                tracing::error!(error = %e, "the feed ingest listener stopped");
            }
        })
    });
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
        drain_state.set_draining();
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
    // Let ingest requests admitted before the drain get their answer.
    #[cfg(feature = "feeds")]
    {
        ingest_stop.cancel();
        if let Some(server) = ingest_server {
            let _ = tokio::time::timeout(Duration::from_secs(10), server).await;
        }
    }

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
    log_shipper.abort();
    leases.abort();
    change_expiry.abort();
    #[cfg(feature = "catalog")]
    if let Some(gc) = local_output_gc {
        gc.abort();
    }
    #[cfg(feature = "triggers")]
    for h in trigger_handles {
        h.abort();
    }
    #[cfg(all(feature = "schedule", feature = "templates"))]
    schedule_loop.abort();
    #[cfg(feature = "templates-sync")]
    for h in sync_handles {
        h.abort();
    }
    // Persist the run-log lines still buffered (#789 SERVE-49).
    state
        .log_hub()
        .shutdown_persistence(Duration::from_secs(10))
        .await;
    // Ship what the last runs logged (#806), bounded.
    crate::serve::log_export::final_flush(
        state.log_export(),
        state.history().as_ref(),
        Duration::from_secs(10),
    )
    .await;
    // Flush any buffered OTLP telemetry after in-flight runs drain (no-op without
    // the `otel` feature).
    faucet_core::shutdown_otel();
    tracing::info!("faucet serve stopped");
    Ok(())
}

/// Resolve on a stop signal (Ctrl-C; SIGTERM on Unix; Ctrl-Break, console
/// close or shutdown on Windows).
async fn wait_for_signal() {
    crate::signals::wait_for_termination().await;
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)] // tests drive time and raw clients directly
mod tests {
    use super::*;
    use crate::serve::history::memory::MemoryHistory;
    use crate::serve::history::{
        Claim, DeleteOutcome, HistoryError, ListFilter, ListPage, RunHistory, RunRecord,
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    /// A history whose renewals, upkeep and reads can stop answering.
    #[derive(Default)]
    struct Stalling {
        stall_renewal: AtomicBool,
        fail_renewal: AtomicBool,
        stall_recover: AtomicBool,
        stall_upkeep: AtomicBool,
        stall_reads: AtomicBool,
        stall_finalize: AtomicBool,
        renewal_takes: std::sync::Mutex<Duration>,
        finalize_takes: std::sync::Mutex<Duration>,
        renewals: AtomicUsize,
        batched_checks: AtomicUsize,
        single_checks: AtomicUsize,
        upkeep_calls: AtomicUsize,
        finalizes: AtomicUsize,
        secrets_deleted: AtomicUsize,
        heartbeats: AtomicUsize,
        fail_batch_check: AtomicBool,
        write_takes: std::sync::Mutex<Duration>,
        sweep_records: std::sync::Mutex<Vec<RunRecord>>,
        stranded: std::sync::Mutex<Vec<RunRecord>>,
        members: std::sync::Mutex<Vec<crate::serve::history::InstanceRecord>>,
        pending: std::sync::Mutex<Vec<(String, usize)>>,
        terminal_writes: AtomicUsize,
    }

    async fn stall_if(flag: &AtomicBool) {
        while flag.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    #[async_trait::async_trait]
    impl RunHistory for Arc<Stalling> {
        async fn claim_idempotency(
            &self,
            _: &str,
            _: &str,
            run_id: &str,
            _: Duration,
        ) -> Result<Claim, HistoryError> {
            let _ = run_id;
            Ok(Claim::Fresh)
        }
        async fn upsert(&self, _: &RunRecord) -> Result<(), HistoryError> {
            Ok(())
        }
        async fn get(&self, _: &str) -> Result<Option<RunRecord>, HistoryError> {
            stall_if(&self.stall_reads).await;
            Ok(None)
        }
        async fn list(&self, _: &ListFilter) -> Result<ListPage, HistoryError> {
            Ok(ListPage {
                runs: Vec::new(),
                next_cursor: None,
            })
        }
        async fn delete(&self, _: &str) -> Result<DeleteOutcome, HistoryError> {
            Ok(DeleteOutcome::NotFound)
        }
        async fn purge_expired(&self, _: Duration) -> Result<usize, HistoryError> {
            Ok(0)
        }
        async fn recover_orphans(&self) -> Result<usize, HistoryError> {
            self.upkeep_calls.fetch_add(1, Ordering::SeqCst);
            stall_if(&self.stall_upkeep).await;
            Ok(0)
        }
        async fn renew_leases(&self) -> Result<usize, HistoryError> {
            stall_if(&self.stall_renewal).await;
            if self.fail_renewal.load(Ordering::SeqCst) {
                return Err(HistoryError::BackendClassified {
                    message: "refused".into(),
                    transience: crate::serve::history::Transience::Permanent,
                });
            }
            let takes = *self.renewal_takes.lock().unwrap();
            tokio::time::sleep(takes).await;
            self.renewals.fetch_add(1, Ordering::SeqCst);
            Ok(0)
        }
        async fn renew_shard_leases(&self) -> Result<usize, HistoryError> {
            Ok(0)
        }
        async fn lease_lost(&self, run_id: &str) -> Result<bool, HistoryError> {
            self.single_checks.fetch_add(1, Ordering::SeqCst);
            stall_if(&self.stall_reads).await;
            if run_id.starts_with("broken") {
                return Err(HistoryError::BackendClassified {
                    message: "refused".into(),
                    transience: crate::serve::history::Transience::Permanent,
                });
            }
            Ok(run_id.starts_with("taken"))
        }
        async fn shard_lease_lost(&self, run_id: &str, _: &str) -> Result<bool, HistoryError> {
            self.single_checks.fetch_add(1, Ordering::SeqCst);
            Ok(run_id.starts_with("taken"))
        }
        async fn leases_lost(&self, keys: &[String]) -> Result<Vec<String>, HistoryError> {
            self.batched_checks.fetch_add(1, Ordering::SeqCst);
            if self.fail_batch_check.load(Ordering::SeqCst) {
                return Err(HistoryError::BackendClassified {
                    message: "refused".into(),
                    transience: crate::serve::history::Transience::Permanent,
                });
            }
            Ok(keys
                .iter()
                .filter(|k| k.starts_with("taken"))
                .cloned()
                .collect())
        }
        async fn finalize_owned(&self, _: &RunRecord) -> Result<bool, HistoryError> {
            stall_if(&self.stall_finalize).await;
            let takes = *self.finalize_takes.lock().unwrap();
            tokio::time::sleep(takes).await;
            self.finalizes.fetch_add(1, Ordering::SeqCst);
            Ok(true)
        }
        async fn run_secrets_delete(&self, _: &str) -> Result<(), HistoryError> {
            self.secrets_deleted.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn heartbeat_instance(
            &self,
            _: &crate::serve::history::InstanceHeartbeat,
        ) -> Result<(), HistoryError> {
            self.heartbeats.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn recover_degraded(&self) -> bool {
            stall_if(&self.stall_recover).await;
            false
        }
        fn degraded(&self) -> bool {
            false
        }
        async fn live_instances(
            &self,
            _: Duration,
        ) -> Result<Vec<crate::serve::history::InstanceRecord>, HistoryError> {
            Ok(self.members.lock().unwrap().clone())
        }
        async fn pending_by_pool(&self) -> Result<Vec<(String, usize)>, HistoryError> {
            Ok(self.pending.lock().unwrap().clone())
        }
        async fn stranded_pending(
            &self,
            _: &[String],
            _: chrono::DateTime<chrono::Utc>,
        ) -> Result<Vec<RunRecord>, HistoryError> {
            Ok(std::mem::take(&mut *self.stranded.lock().unwrap()))
        }
        async fn fail_pending(&self, _: &RunRecord) -> Result<bool, HistoryError> {
            let takes = *self.write_takes.lock().unwrap();
            tokio::time::sleep(takes).await;
            self.terminal_writes.fetch_add(1, Ordering::SeqCst);
            Ok(true)
        }
        async fn finalize_completed_sharded_parents_records(
            &self,
        ) -> Result<Vec<RunRecord>, HistoryError> {
            let done = std::mem::take(&mut *self.sweep_records.lock().unwrap());
            let takes = *self.write_takes.lock().unwrap();
            tokio::time::sleep(takes).await;
            self.terminal_writes.fetch_add(1, Ordering::SeqCst);
            Ok(done)
        }
    }

    fn state_over(clustered: bool, history: Arc<dyn RunHistory>) -> ServerState {
        let mut config = crate::serve::test_support::test_config();
        config.cluster.enabled = clustered;
        config.lease_ttl = Duration::from_secs(30);
        state_with(&config, history)
    }

    fn state_with(
        config: &crate::serve::config::ServeConfig,
        history: Arc<dyn RunHistory>,
    ) -> ServerState {
        ServerState::new(
            config,
            None,
            CancellationToken::new(),
            history,
            crate::serve::logs::LogHub::new(),
            None,
            #[cfg(feature = "triggers")]
            crate::serve::triggers::health::TriggersHandle::empty(),
        )
    }

    fn queue_run_finalize(state: &ServerState, id: &str) {
        let mut rec = RunRecord::queued(
            id.into(),
            None,
            std::collections::BTreeMap::new(),
            None,
            chrono::Utc::now(),
        );
        rec.status = crate::serve::history::RunStatus::Completed;
        state.queue_finalize(crate::serve::runner::PendingFinalize::Run {
            rec: Box::new(rec),
            reason: "completed",
            failures: 0,
        });
    }

    fn stalling_state(clustered: bool) -> (ServerState, Arc<Stalling>) {
        let double = Arc::new(Stalling::default());
        let history = Arc::new(crate::serve::history::fallback::FallbackHistory::healthy(
            Box::new(Arc::clone(&double)),
            crate::serve::test_support::test_config().idempotency_retention,
            "postgres",
        )) as Arc<dyn RunHistory>;
        (state_over(clustered, history), double)
    }

    #[tokio::test(start_paused = true)]
    async fn a_stalled_renewal_is_given_up_and_the_fence_fires_on_time() {
        let (state, double) = stalling_state(true);
        double.stall_renewal.store(true, Ordering::SeqCst);
        let run = CancellationToken::new();
        state.registry().register("r1".into(), run.clone());
        let started = tokio::time::Instant::now();
        let fenced_after = tokio::spawn({
            let run = run.clone();
            async move {
                run.cancelled().await;
                started.elapsed()
            }
        });
        let mut health = LeaseHealth::new(std::time::Instant::now());
        let cancelled = maintain_leases(&state, &mut health, Duration::from_secs(5)).await;
        let took = started.elapsed();
        assert_eq!(cancelled, 1);
        assert!(
            took >= Duration::from_secs(19) && took <= Duration::from_secs(21),
            "the attempt ends at two thirds of the 30s lease, took {took:?}"
        );
        let fenced_after = fenced_after.await.unwrap();
        assert!(
            fenced_after >= Duration::from_secs(4) && fenced_after <= Duration::from_secs(6),
            "the fence does not wait for the stalled renewal, fired at {fenced_after:?}"
        );
        assert!(!state.cluster().history_reachable());
        assert!(state.cluster().is_fenced());
        assert_eq!(
            maintain_leases(&state, &mut health, Duration::from_secs(5)).await,
            0,
            "fenced once per outage"
        );

        double.stall_renewal.store(false, Ordering::SeqCst);
        maintain_leases(&state, &mut health, Duration::from_secs(5)).await;
        assert!(state.cluster().history_reachable());
        assert!(!state.cluster().is_fenced());
    }

    #[tokio::test(start_paused = true)]
    async fn a_slow_renewal_that_answers_before_the_fence_keeps_the_runs() {
        let (state, double) = stalling_state(true);
        *double.renewal_takes.lock().unwrap() = Duration::from_secs(3);
        let run = CancellationToken::new();
        state.registry().register("r1".into(), run.clone());
        let mut health = LeaseHealth::new(std::time::Instant::now());
        assert_eq!(
            maintain_leases(&state, &mut health, Duration::from_secs(5)).await,
            0
        );
        assert!(!run.is_cancelled());
        assert!(state.cluster().history_reachable());
        assert_eq!(double.renewals.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn lost_leases_are_checked_in_one_call() {
        let (state, double) = stalling_state(true);
        let (mine, taken, shard) = (
            CancellationToken::new(),
            CancellationToken::new(),
            CancellationToken::new(),
        );
        state.registry().register("mine-1".into(), mine.clone());
        state.registry().register("taken-1".into(), taken.clone());
        state
            .registry()
            .register_shard("taken-2", "0", shard.clone());
        let mut health = LeaseHealth::new(std::time::Instant::now());
        assert_eq!(
            maintain_leases(&state, &mut health, Duration::from_secs(3600)).await,
            2
        );
        assert!(taken.is_cancelled() && shard.is_cancelled() && !mine.is_cancelled());
        assert_eq!(double.batched_checks.load(Ordering::SeqCst), 1);
        assert_eq!(double.single_checks.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn a_failed_batch_check_falls_back_to_one_check_per_run() {
        let (state, double) = stalling_state(true);
        double.fail_batch_check.store(true, Ordering::SeqCst);
        let (mine, taken, broken, shard) = (
            CancellationToken::new(),
            CancellationToken::new(),
            CancellationToken::new(),
            CancellationToken::new(),
        );
        state.registry().register("mine-1".into(), mine.clone());
        state.registry().register("taken-1".into(), taken.clone());
        state.registry().register("broken-1".into(), broken.clone());
        state
            .registry()
            .register_shard("taken-2", "0", shard.clone());
        let mut health = LeaseHealth::new(std::time::Instant::now());
        assert_eq!(
            maintain_leases(&state, &mut health, Duration::from_secs(3600)).await,
            2
        );
        assert!(taken.is_cancelled() && shard.is_cancelled());
        assert!(!mine.is_cancelled() && !broken.is_cancelled());
        assert_eq!(double.batched_checks.load(Ordering::SeqCst), 1);
        assert_eq!(double.single_checks.load(Ordering::SeqCst), 4);
    }

    fn sharded_state(double: &Arc<Stalling>, pool_wait: Option<Duration>) -> ServerState {
        let mut config = crate::serve::test_support::test_config();
        config.cluster.enabled = true;
        config.cluster.pool_wait_timeout = pool_wait;
        config.lease_ttl = Duration::from_millis(1500);
        let history = Arc::new(crate::serve::history::fallback::FallbackHistory::healthy(
            Box::new(Arc::clone(double)),
            config.idempotency_retention,
            "postgres",
        )) as Arc<dyn RunHistory>;
        state_with(&config, history)
    }

    async fn callback_server() -> (wiremock::MockServer, crate::serve::callback::CallbackSpec) {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        let spec = serde_json::from_value(serde_json::json!({
            "url": format!("{}/done", server.uri())
        }))
        .unwrap();
        (server, spec)
    }

    fn finished(id: &str, status: crate::serve::history::RunStatus) -> RunRecord {
        let mut rec = RunRecord::queued(
            id.into(),
            None,
            std::collections::BTreeMap::new(),
            None,
            chrono::Utc::now(),
        );
        rec.status = status;
        rec.finished_at = Some(chrono::Utc::now());
        rec
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_sweep_slower_than_the_bound_still_fires_its_callback_once() {
        let double = Arc::new(Stalling::default());
        let state = sharded_state(&double, None);
        *double.write_takes.lock().unwrap() = Duration::from_millis(1600);
        let (server, callback) = callback_server().await;
        let mut parent = finished("parent", crate::serve::history::RunStatus::Completed);
        parent.callback = Some(callback);
        double.sweep_records.lock().unwrap().push(parent);
        lease_upkeep_pass(state.clone(), Duration::from_secs(3)).await;
        lease_upkeep_pass(state.clone(), Duration::from_secs(3)).await;
        assert_eq!(double.terminal_writes.load(Ordering::SeqCst), 2);
        server.verify().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn failing_an_unserved_pools_run_slower_than_the_bound_fires_its_callback_once() {
        let double = Arc::new(Stalling::default());
        let state = sharded_state(&double, Some(Duration::from_millis(1)));
        *double.members.lock().unwrap() = vec![crate::serve::history::InstanceRecord {
            instance_id: "peer".into(),
            started_at: chrono::Utc::now(),
            last_heartbeat: chrono::Utc::now(),
            listen: None,
            max_concurrent: 1,
            in_flight: 0,
            state_format: faucet_core::state_version::STATE_FORMAT,
            pools: Some(vec!["default".into()]),
            capabilities: None,
            clock_offset_ms: None,
        }];
        *double.pending.lock().unwrap() = vec![("gpu".into(), 1)];
        let (server, callback) = callback_server().await;
        let mut rec = finished("stranded", crate::serve::history::RunStatus::Pending);
        rec.pool = "gpu".into();
        rec.callback = Some(callback);
        lease_upkeep_pass(state.clone(), Duration::from_secs(3)).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        double.stranded.lock().unwrap().push(rec);
        *double.write_takes.lock().unwrap() = Duration::from_millis(1600);
        lease_upkeep_pass(state.clone(), Duration::from_secs(3)).await;
        lease_upkeep_pass(state.clone(), Duration::from_secs(3)).await;
        let fails = double.terminal_writes.load(Ordering::SeqCst);
        assert_eq!(fails, 4, "one failed pending run and three sweeps");
        server.verify().await;
    }

    #[tokio::test(start_paused = true)]
    async fn the_per_run_checks_stop_at_the_bound() {
        let (state, double) = stalling_state(true);
        double.fail_batch_check.store(true, Ordering::SeqCst);
        double.stall_reads.store(true, Ordering::SeqCst);
        let taken = CancellationToken::new();
        state.registry().register("taken-1".into(), taken.clone());
        let started = tokio::time::Instant::now();
        assert_eq!(cancel_lost_leases(&state, Duration::from_secs(2)).await, 0);
        assert_eq!(started.elapsed(), Duration::from_secs(2));
        assert!(!taken.is_cancelled());
        double.stall_reads.store(false, Ordering::SeqCst);
    }

    #[tokio::test]
    async fn no_live_run_means_no_lost_lease_query() {
        let (state, double) = stalling_state(true);
        let mut health = LeaseHealth::new(std::time::Instant::now());
        maintain_leases(&state, &mut health, Duration::from_secs(3600)).await;
        assert_eq!(double.batched_checks.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_bounded_history_call_fails_instead_of_waiting() {
        use crate::serve::history::fallback::with_call_bound;
        let (state, double) = stalling_state(false);
        double.stall_reads.store(true, Ordering::SeqCst);
        let started = tokio::time::Instant::now();
        let err = with_call_bound(Duration::from_secs(2), state.history().get("r1"))
            .await
            .unwrap_err();
        assert_eq!(started.elapsed(), Duration::from_secs(2));
        assert!(err.is_transient(), "{err:?}");
        assert!(!err.is_unreachable(), "a slow call is not an outage");
        assert!(
            err.to_string().contains("did not answer within 2000ms"),
            "{err}"
        );
        assert!(!state.history().degraded());

        let history = state.history();
        let unbounded = tokio::time::timeout(Duration::from_secs(600), history.get("r1"));
        assert!(unbounded.await.is_err(), "outside the scope nothing is cut");
    }

    #[tokio::test(start_paused = true)]
    async fn a_stalled_upkeep_pass_never_delays_a_renewal() {
        let (state, double) = stalling_state(false);
        double.stall_upkeep.store(true, Ordering::SeqCst);
        let shutdown = CancellationToken::new();
        let looping = tokio::spawn(lease_loop(
            state.clone(),
            Duration::from_secs(1),
            shutdown.clone(),
        ));
        tokio::time::sleep(Duration::from_millis(10_500)).await;
        assert_eq!(
            double.renewals.load(Ordering::SeqCst),
            10,
            "one renewal per period while the upkeep call hangs"
        );
        assert_eq!(
            double.upkeep_calls.load(Ordering::SeqCst),
            1,
            "one pass at a time: no second pass starts behind a running one"
        );
        // The hung call is cut at two thirds of the lease TTL (20s), then the
        // next tick starts a new pass.
        tokio::time::sleep(Duration::from_secs(12)).await;
        assert_eq!(double.upkeep_calls.load(Ordering::SeqCst), 2);
        double.stall_upkeep.store(false, Ordering::SeqCst);
        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(60), looping)
            .await
            .expect("the loop ends once its pass has")
            .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_stall_after_a_fast_failure_fences_at_the_fence_instant() {
        let (state, double) = stalling_state(true);
        let run = CancellationToken::new();
        state.registry().register("r1".into(), run.clone());
        let started = tokio::time::Instant::now();
        let mut health = LeaseHealth::new(lease_now());
        double.fail_renewal.store(true, Ordering::SeqCst);
        assert_eq!(
            maintain_leases(&state, &mut health, Duration::from_secs(5)).await,
            0
        );
        assert!(!run.is_cancelled());
        tokio::time::sleep(Duration::from_secs(6)).await;
        double.fail_renewal.store(false, Ordering::SeqCst);
        double.stall_renewal.store(true, Ordering::SeqCst);
        let fenced_after = tokio::spawn({
            let run = run.clone();
            async move {
                run.cancelled().await;
                started.elapsed()
            }
        });
        assert_eq!(
            maintain_leases(&state, &mut health, Duration::from_secs(5)).await,
            1
        );
        let fenced_after = fenced_after.await.unwrap();
        assert!(
            fenced_after <= Duration::from_millis(6_200),
            "fenced as soon as the tick finds the fence due, not when the renewal gives up: {fenced_after:?}"
        );
        assert!(state.cluster().is_fenced());
        double.stall_renewal.store(false, Ordering::SeqCst);
    }

    #[tokio::test(start_paused = true)]
    async fn a_stalled_recovery_probe_cannot_hold_back_the_fence() {
        let double = Arc::new(Stalling::default());
        let state = state_over(true, Arc::new(Arc::clone(&double)) as Arc<dyn RunHistory>);
        double.stall_recover.store(true, Ordering::SeqCst);
        let run = CancellationToken::new();
        state.registry().register("r1".into(), run.clone());
        let started = tokio::time::Instant::now();
        let ticking = tokio::spawn({
            let state = state.clone();
            async move {
                let mut health = LeaseHealth::new(lease_now());
                maintain_leases(&state, &mut health, Duration::from_secs(5)).await
            }
        });
        tokio::time::timeout(Duration::from_secs(6), run.cancelled())
            .await
            .expect("the fence fires while the recovery probe hangs");
        assert!(started.elapsed() <= Duration::from_millis(5_200));
        assert!(!ticking.is_finished());
        double.stall_recover.store(false, Ordering::SeqCst);
        assert_eq!(ticking.await.unwrap(), 1);
        assert_eq!(double.renewals.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_finalize_slower_than_the_bound_still_completes_with_its_side_effects() {
        let (state, double) = stalling_state(true);
        *double.finalize_takes.lock().unwrap() = Duration::from_secs(25);
        queue_run_finalize(&state, "slow");
        let mut health = LeaseHealth::new(lease_now());
        let started = tokio::time::Instant::now();
        maintain_leases(&state, &mut health, Duration::from_secs(3600)).await;
        assert!(started.elapsed() >= Duration::from_secs(25));
        assert_eq!(double.finalizes.load(Ordering::SeqCst), 1);
        assert_eq!(
            double.secrets_deleted.load(Ordering::SeqCst),
            1,
            "the completion side effects ran"
        );
        assert_eq!(state.pending_finalize_len(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_long_upkeep_pass_does_not_stop_heartbeats() {
        let (state, double) = stalling_state(true);
        double.stall_finalize.store(true, Ordering::SeqCst);
        queue_run_finalize(&state, "stuck");
        let shutdown = CancellationToken::new();
        let looping = tokio::spawn(lease_loop(
            state.clone(),
            Duration::from_secs(1),
            shutdown.clone(),
        ));
        tokio::time::sleep(Duration::from_millis(40_500)).await;
        assert_eq!(double.finalizes.load(Ordering::SeqCst), 0);
        assert_eq!(
            double.heartbeats.load(Ordering::SeqCst),
            40,
            "one heartbeat per period while the upkeep pass hangs"
        );
        double.stall_finalize.store(false, Ordering::SeqCst);
        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(60), looping)
            .await
            .expect("the loop ends once its pass has")
            .unwrap();
        assert_eq!(double.finalizes.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn the_fence_instant_is_known_until_the_outage_is_fenced() {
        let start = std::time::Instant::now();
        let mut health = LeaseHealth::new(start);
        let after = Duration::from_secs(20);
        assert_eq!(health.fence_due(after), Some(start + after));
        assert!(health.observe(false, start + after, after));
        assert_eq!(health.fence_due(after), None);
        assert!(!health.observe(true, start + after, after));
        assert_eq!(health.fence_due(after), Some(start + after + after));
    }

    #[test]
    fn the_clock_offset_is_measured_at_the_round_trip_midpoint() {
        let before = chrono::Utc::now();
        let after = before + chrono::Duration::milliseconds(100);
        assert_eq!(clock_offset_ms(before, after, before), 50);
        assert_eq!(
            clock_offset_ms(before, after, before + chrono::Duration::seconds(2)),
            -1950
        );
    }

    #[tokio::test]
    async fn the_heartbeat_reports_a_measured_offset() {
        let state = crate::serve::test_support::test_state();
        let offset = measure_clock_offset(&state).await.unwrap();
        assert!(offset.abs() < 1000, "{offset}");
    }
    use crate::serve::history::RunStatus;
    use chrono::Utc;
    use std::collections::BTreeMap;

    /// A forgotten run is dropped from the set once its record is terminal
    /// or gone, kept while it is live, and kept while the history is down.
    #[tokio::test]
    async fn settled_forgotten_runs_are_cleared() {
        let down = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let history = std::sync::Arc::new(crate::serve::test_support::Flaky {
            inner: std::sync::Arc::new(MemoryHistory::new(Duration::from_secs(60))),
            down: down.clone(),
        });
        let state = ServerState::new(
            &crate::serve::test_support::test_config(),
            None,
            CancellationToken::new(),
            history,
            crate::serve::logs::LogHub::new(),
            None,
            #[cfg(feature = "triggers")]
            crate::serve::triggers::health::TriggersHandle::empty(),
        );
        for (id, status) in [("done", RunStatus::Completed), ("live", RunStatus::Running)] {
            let mut rec = RunRecord::queued(id.into(), None, BTreeMap::new(), None, Utc::now());
            rec.status = status;
            state.history().upsert(&rec).await.unwrap();
        }
        for id in ["done", "live", "gone"] {
            state.forget_run_lease(id);
        }
        down.store(true, std::sync::atomic::Ordering::Release);
        clear_settled_forgotten(&state).await;
        assert_eq!(state.forgotten_runs().len(), 3, "nothing is cleared blind");
        down.store(false, std::sync::atomic::Ordering::Release);
        clear_settled_forgotten(&state).await;
        assert_eq!(state.forgotten_runs(), vec!["live".to_string()]);
    }

    /// A pending seed with neither a file nor a seed to apply stays pending.
    #[cfg(feature = "policy")]
    #[tokio::test]
    async fn a_pending_seed_with_nothing_to_seed_stays_pending() {
        let state = crate::serve::test_support::test_state();
        state.set_policy_seed_pending(true);
        retry_policy_seed(&state).await.unwrap();
        assert!(state.policy_seed_pending());
    }

    #[tokio::test]
    async fn metrics_upkeep_drains_histograms_on_its_own() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        ::metrics::with_local_recorder(&recorder, || {
            ::metrics::histogram!("faucet_upkeep_probe").record(1.0);
        });
        let task = spawn_metrics_upkeep(handle.clone(), Duration::from_millis(5));
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!task.is_finished(), "the upkeep loop keeps running");
        task.abort();
        assert!(handle.render().contains("faucet_upkeep_probe"));
    }

    #[test]
    fn lease_health_fences_once_per_outage_after_the_window() {
        let t0 = std::time::Instant::now();
        let window = Duration::from_secs(20);
        let mut h = LeaseHealth::new(t0);
        assert!(!h.observe(false, t0 + Duration::from_secs(10), window));
        assert!(h.observe(false, t0 + Duration::from_secs(20), window));
        assert!(
            !h.observe(false, t0 + Duration::from_secs(40), window),
            "once"
        );
        assert!(!h.observe(true, t0 + Duration::from_secs(41), window));
        assert!(!h.observe(false, t0 + Duration::from_secs(50), window));
        assert!(
            h.observe(false, t0 + Duration::from_secs(61), window),
            "a new outage"
        );
    }

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
