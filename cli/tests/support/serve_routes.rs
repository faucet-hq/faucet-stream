//! The `faucet serve` route inventory for the current feature set, shared by
//! `serve_openapi` (spec ↔ router) and `serve_rbac` (every route classified),
//! so the two can never disagree about which routes exist.
#![allow(dead_code)]

use std::collections::BTreeSet;

/// Base (METHOD, path-template) set — always-present routes in `serve::server::build_router`.
const ROUTES_BASE: &[(&str, &str)] = &[
    ("POST", "/v1/runs"),
    ("GET", "/v1/runs"),
    ("GET", "/v1/runs/{id}"),
    ("DELETE", "/v1/runs/{id}"),
    ("POST", "/v1/runs/{id}/cancel"),
    ("POST", "/v1/runs/{id}/rollback"),
    ("GET", "/v1/runs/{id}/logs"),
    ("GET", "/v1/schemas"),
    ("GET", "/v1/schemas/{kind}/{name}"),
    ("POST", "/v1/doctor"),
    ("POST", "/v1/backfill"),
    ("POST", "/v1/verify"),
    ("POST", "/v1/plan"),
    ("POST", "/v1/dlq/inspect"),
    ("POST", "/v1/dlq/replay"),
    ("POST", "/v1/dlq/discard"),
    ("GET", "/v1/audit"),
    ("POST", "/v1/reload"),
    ("GET", "/v1/whoami"),
    // Pipeline health + durable state (#732, #735).
    ("GET", "/v1/status"),
    ("POST", "/v1/status"),
    // Multi-table mirror status (#731).
    ("GET", "/v1/mirror/{name}"),
    ("POST", "/v1/mirror/{name}"),
    ("GET", "/v1/state/{pipeline}/{row}"),
    ("PUT", "/v1/state/{pipeline}/{row}"),
    ("DELETE", "/v1/state/{pipeline}/{row}"),
    // Change requests (#703).
    ("POST", "/v1/changes"),
    ("GET", "/v1/changes"),
    ("GET", "/v1/changes/{id}"),
    ("POST", "/v1/changes/{id}/approve"),
    ("POST", "/v1/changes/{id}/reject"),
    ("GET", "/healthz"),
    ("GET", "/readyz"),
    ("GET", "/metrics"),
];

/// Routes that are only registered when the `triggers` feature is compiled in.
#[cfg(feature = "triggers")]
const ROUTES_TRIGGERS: &[(&str, &str)] = &[
    ("POST", "/v1/triggers/{name}"),
    ("PUT", "/v1/triggers/{name}"),
];

/// Routes that are only registered when the `catalog` feature is compiled in.
#[cfg(feature = "catalog")]
const ROUTES_CATALOG: &[(&str, &str)] = &[
    ("GET", "/v1/catalog/datasets"),
    ("GET", "/v1/catalog/datasets/{id}"),
    ("POST", "/v1/catalog/datasets/{id}/consumers"),
    ("GET", "/v1/catalog/lineage"),
    // Cost & usage accounting (#704).
    ("GET", "/v1/usage"),
    // Local sink output retention (#587).
    ("GET", "/v1/local-outputs"),
    ("DELETE", "/v1/local-outputs/{id}"),
    ("POST", "/v1/local-outputs/cleanup"),
    // Dataset preview of a tracked local output (#586). Always routed; the
    // `--preview-local-outputs` opt-in is enforced inside the handler (a
    // disabled server answers 403, never a bare 404), which is also what keeps
    // this probe meaningful.
    ("GET", "/v1/local-outputs/{id}/preview"),
];

/// Routes that are only registered when the `templates` feature is compiled in.
#[cfg(feature = "templates")]
const ROUTES_TEMPLATES: &[(&str, &str)] = &[
    ("POST", "/v1/templates"),
    ("GET", "/v1/templates"),
    ("GET", "/v1/templates/matrix"),
    ("GET", "/v1/templates/{id}"),
    ("DELETE", "/v1/templates/{id}"),
    ("POST", "/v1/templates/{id}/runs"),
    ("GET", "/v1/templates/{id}/rows"),
    ("POST", "/v1/templates/{id}/tags"),
    ("POST", "/v1/templates/{id}/launch"),
    ("POST", "/v1/templates/{id}/rollback"),
    ("POST", "/v1/templates/{id}/deprecate"),
    ("POST", "/v1/templates/{id}/versions/{version}/deprecate"),
    ("POST", "/v1/templates/{id}/versions/{version}/test"),
    ("GET", "/v1/templates/{id}/versions/{version}/tests"),
];

#[cfg(feature = "templates-sync")]
const ROUTES_TEMPLATES_SYNC: &[(&str, &str)] = &[
    ("POST", "/v1/templates/sync"),
    ("POST", "/v1/templates/{id}/publish"),
];

#[cfg(feature = "tenants")]
const ROUTES_TENANTS: &[(&str, &str)] = &[
    ("GET", "/v1/tenants"),
    ("POST", "/v1/tenants"),
    ("GET", "/v1/tenants/{tenant}"),
    ("PATCH", "/v1/tenants/{tenant}"),
    ("DELETE", "/v1/tenants/{tenant}"),
    ("GET", "/v1/tenants/{tenant}/connections"),
    ("POST", "/v1/tenants/{tenant}/connections"),
    ("GET", "/v1/tenants/{tenant}/connections/{name}"),
    ("PUT", "/v1/tenants/{tenant}/connections/{name}"),
    ("DELETE", "/v1/tenants/{tenant}/connections/{name}"),
    ("POST", "/v1/tenants/{tenant}/connect/{provider}"),
    ("POST", "/v1/tenants/{tenant}/runs"),
    ("POST", "/v1/tenants/{tenant}/templates/{id}/runs"),
    ("POST", "/v1/templates/{id}/fanout"),
    ("GET", "/v1/connect/providers"),
    ("GET", "/v1/connect/callback"),
];

#[cfg(feature = "mcp")]
const ROUTES_MCP: &[(&str, &str)] = &[("POST", "/mcp")];

/// Returns the full canonical route set for the current feature configuration.
pub fn canonical_routes() -> BTreeSet<(String, String)> {
    #[allow(unused_mut)]
    let mut set: BTreeSet<(String, String)> = ROUTES_BASE
        .iter()
        .map(|(m, p)| (m.to_string(), p.to_string()))
        .collect();
    #[cfg(feature = "triggers")]
    for (m, p) in ROUTES_TRIGGERS {
        set.insert((m.to_string(), p.to_string()));
    }
    #[cfg(feature = "catalog")]
    for (m, p) in ROUTES_CATALOG {
        set.insert((m.to_string(), p.to_string()));
    }
    #[cfg(feature = "templates")]
    for (m, p) in ROUTES_TEMPLATES {
        set.insert((m.to_string(), p.to_string()));
    }
    #[cfg(feature = "templates-sync")]
    for (m, p) in ROUTES_TEMPLATES_SYNC {
        set.insert((m.to_string(), p.to_string()));
    }
    #[cfg(feature = "tenants")]
    for (m, p) in ROUTES_TENANTS {
        set.insert((m.to_string(), p.to_string()));
    }
    #[cfg(feature = "mcp")]
    for (m, p) in ROUTES_MCP {
        set.insert((m.to_string(), p.to_string()));
    }
    set
}

/// The authenticated routes: everything under `/v1` plus `/mcp`, without the
/// public OAuth callback (it sits outside the auth layer).
pub fn authenticated_routes() -> Vec<(&'static str, &'static str)> {
    let mut all: Vec<(&'static str, &'static str)> = ROUTES_BASE.to_vec();
    #[cfg(feature = "triggers")]
    all.extend_from_slice(ROUTES_TRIGGERS);
    #[cfg(feature = "catalog")]
    all.extend_from_slice(ROUTES_CATALOG);
    #[cfg(feature = "templates")]
    all.extend_from_slice(ROUTES_TEMPLATES);
    #[cfg(feature = "templates-sync")]
    all.extend_from_slice(ROUTES_TEMPLATES_SYNC);
    #[cfg(feature = "tenants")]
    all.extend_from_slice(ROUTES_TENANTS);
    #[cfg(feature = "mcp")]
    all.extend_from_slice(ROUTES_MCP);
    all.into_iter()
        .filter(|(_, p)| (p.starts_with("/v1/") || *p == "/mcp") && *p != "/v1/connect/callback")
        .collect()
}
