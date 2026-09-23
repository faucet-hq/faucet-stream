//! Keeps `docs/openapi.yaml` in two-way sync with the live `faucet serve`
//! router (Phase 6, #127):
//!
//! 1. The documented `(method, path)` set equals the canonical route list below
//!    (which mirrors `serve::server::build_router`).
//! 2. Every canonical route is actually wired on a booted server — a request
//!    reaches a handler (never a bare axum routing 404 or a 405).
//!
//! Together these bind the spec to the real router. Residual gap (axum cannot
//! enumerate its routes): a route added to `build_router` but to neither this
//! list nor the spec would slip through — so a reviewer must update both when
//! adding an endpoint. Requires the `serve` feature.
#![cfg(feature = "serve")]

use faucet_cli::cli::ServeArgs;
use faucet_cli::serve::ServeConfig;
use std::collections::BTreeSet;
use std::time::Duration;

/// Base (METHOD, path-template) set — always-present routes in `serve::server::build_router`.
const ROUTES_BASE: &[(&str, &str)] = &[
    ("POST", "/v1/runs"),
    ("GET", "/v1/runs"),
    ("GET", "/v1/runs/{id}"),
    ("DELETE", "/v1/runs/{id}"),
    ("POST", "/v1/runs/{id}/cancel"),
    ("GET", "/v1/runs/{id}/logs"),
    ("GET", "/v1/schemas"),
    ("GET", "/v1/schemas/{kind}/{name}"),
    ("POST", "/v1/doctor"),
    ("POST", "/v1/backfill"),
    ("POST", "/v1/dlq/inspect"),
    ("POST", "/v1/dlq/replay"),
    ("POST", "/v1/dlq/discard"),
    ("GET", "/v1/audit"),
    ("POST", "/v1/reload"),
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
    ("GET", "/v1/catalog/lineage"),
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
    ("POST", "/v1/templates/{id}/tags"),
    ("POST", "/v1/templates/{id}/launch"),
    ("POST", "/v1/templates/{id}/rollback"),
    ("POST", "/v1/templates/{id}/deprecate"),
];

#[cfg(feature = "templates-sync")]
const ROUTES_TEMPLATES_SYNC: &[(&str, &str)] = &[
    ("POST", "/v1/templates/sync"),
    ("POST", "/v1/templates/{id}/publish"),
];

/// Returns the full canonical route set for the current feature configuration.
fn canonical_routes() -> BTreeSet<(String, String)> {
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
    set
}

fn openapi_routes() -> BTreeSet<(String, String)> {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../docs/openapi.yaml");
    let text = std::fs::read_to_string(path).expect("read docs/openapi.yaml");
    let doc: serde_yaml::Value = serde_yaml::from_str(&text).expect("parse openapi.yaml");
    let methods = ["get", "post", "put", "delete", "patch", "head", "options"];
    let mut set = BTreeSet::new();
    let paths = doc["paths"].as_mapping().expect("paths mapping");
    for (path, ops) in paths {
        let path = path.as_str().unwrap().to_string();
        // Skip trigger routes when the `triggers` feature is not compiled in —
        // the openapi spec documents all routes, but only a subset are wired
        // for a given feature combination.
        #[cfg(not(feature = "triggers"))]
        if path.starts_with("/v1/triggers") {
            continue;
        }
        // Likewise for the `catalog` feature's routes — including the
        // local-output retention endpoints (#587), which ride `catalog` (the
        // ledger's console surface is the Datasets page) despite not living
        // under the `/v1/catalog` prefix.
        #[cfg(not(feature = "catalog"))]
        if path.starts_with("/v1/catalog") || path.starts_with("/v1/local-outputs") {
            continue;
        }
        // …and the `templates` feature's routes (#444).
        #[cfg(not(feature = "templates"))]
        if path.starts_with("/v1/templates") {
            continue;
        }
        // …and the `templates-sync` routes (RFC 0006 / #589).
        #[cfg(not(feature = "templates-sync"))]
        if path == "/v1/templates/sync" || path.ends_with("/publish") {
            continue;
        }
        let ops = ops.as_mapping().unwrap();
        for (method, _) in ops {
            let m = method.as_str().unwrap().to_ascii_lowercase();
            if methods.contains(&m.as_str()) {
                set.insert((m.to_ascii_uppercase(), path.clone()));
            }
        }
    }
    set
}

#[test]
fn openapi_paths_match_canonical_routes() {
    let documented = openapi_routes();
    let canonical = canonical_routes();

    let undocumented: Vec<_> = canonical.difference(&documented).collect();
    let unrouted: Vec<_> = documented.difference(&canonical).collect();
    assert!(
        undocumented.is_empty(),
        "routes missing from docs/openapi.yaml: {undocumented:?}"
    );
    assert!(
        unrouted.is_empty(),
        "openapi.yaml documents paths that are not routed: {unrouted:?}"
    );
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[tokio::test(flavor = "multi_thread")]
async fn every_documented_route_is_wired_on_the_live_server() {
    let port = free_port();
    let args = ServeArgs {
        listen: format!("127.0.0.1:{port}"),
        auth_token: None,
        auth_config: None,
        read_token: None,
        write_token: None,
        admin_token: None,
        no_auth: true,
        max_concurrent_runs: Some(2),
        max_queued_runs: Some(8),
        default_config: None,
        history: None,
        cors_origin: vec![],
        body_limit_bytes: 1_048_576,
        shutdown_grace_secs: 5,
        retain_terminal_runs_secs: 604_800,
        idempotency_retention_secs: 86_400,
        log_retention_secs: 604_800,
        log_max_lines_per_run: 100_000,
        local_output_retention_days: 7,
        local_output_in_flight_grace_secs: 60,
        preview_local_outputs: false,
        preview_default_rows: 500,
        preview_max_rows: 5_000,
        lease_ttl_secs: 30,
        probe_timeout_secs: 5,
        env_file: None,
        no_env_file: true,
        no_ui: false,
        cluster: false,
        cluster_poll_secs: 2,
        cluster_max_attempts: 3,
        triggers: None,
        templates_sync: None,
        callback_allow_host: Vec::new(),
        mcp: false,
        mcp_allow_mutations: false,
    };
    let mut config = ServeConfig::from_args(args).unwrap();
    config.log_level = "warn".into();
    tokio::spawn(async move {
        let _ = faucet_cli::serve::run_server(config, Default::default()).await;
    });

    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");
    let mut up = false;
    for _ in 0..200 {
        if client
            .get(format!("{base}/healthz"))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
        {
            up = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(up, "server did not come up");

    // The webhook trigger route is only wired when --triggers is active; the
    // test server starts with triggers_path: None so we probe only base routes.
    #[cfg(not(feature = "triggers"))]
    let routes = canonical_routes();
    #[cfg(feature = "triggers")]
    let routes: BTreeSet<(String, String)> = canonical_routes()
        .into_iter()
        .filter(|(_, p)| !p.starts_with("/v1/triggers"))
        .collect();

    for (method, template) in &routes {
        // Unknown id so {id} routes hit their handler's NotFound, not bare 404.
        let path = template.replace("{id}", "probe-missing-run");
        let url = format!("{base}{path}");
        let m: reqwest::Method = method.parse().unwrap();
        let mut req = client.request(m, &url);
        if *method == "POST" {
            // `{}` fails SubmitRequest validation → a handler error, never 404/405.
            req = req.json(&serde_json::json!({}));
        }
        let resp = req
            .send()
            .await
            .unwrap_or_else(|e| panic!("{method} {path}: {e}"));
        let status = resp.status().as_u16();
        assert_ne!(
            status, 405,
            "{method} {template} returned 405 — method not routed on this path"
        );
        if status == 404 {
            // A handler 404 carries the ApiError JSON envelope; a bare axum
            // routing 404 (no such route) does not.
            let body = resp.text().await.unwrap_or_default();
            assert!(
                body.contains("\"error\""),
                "{method} {template} returned a bare 404 (route not wired); body: {body:?}"
            );
        }
    }
}
