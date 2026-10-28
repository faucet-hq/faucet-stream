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
#![allow(clippy::disallowed_methods)] // tests drive time and raw clients directly

use faucet_cli::cli::ServeArgs;
use faucet_cli::serve::ServeConfig;
use std::collections::BTreeSet;
use std::time::Duration;

#[path = "support/serve_routes.rs"]
mod serve_routes;
use serve_routes::canonical_routes;

fn openapi_routes() -> BTreeSet<(String, String)> {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../docs/openapi.yaml");
    let text = std::fs::read_to_string(path).expect("read docs/openapi.yaml");
    let doc: faucet_cli::yaml::Value =
        faucet_cli::yaml::from_str(&text).expect("parse openapi.yaml");
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
        // local-output retention endpoints (#587) and the usage report
        // (#704), which ride `catalog` despite not living under the
        // `/v1/catalog` prefix.
        #[cfg(not(feature = "catalog"))]
        if path.starts_with("/v1/catalog")
            || path.starts_with("/v1/local-outputs")
            || path == "/v1/usage"
        {
            continue;
        }
        // …and the `tenants` feature's routes (#709).
        #[cfg(not(feature = "tenants"))]
        if path.starts_with("/v1/tenants")
            || path.starts_with("/v1/connect/")
            || path.ends_with("/fanout")
        {
            continue;
        }
        // …and the `mcp` feature's transport (#420).
        #[cfg(not(feature = "mcp"))]
        if path == "/mcp" {
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

#[path = "support/ports.rs"]
mod ports;
use ports::free_port;

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
        allow_unauthenticated_network: false,
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
        log_buffer: Default::default(),
        local_output_retention_days: 7,
        local_output_in_flight_grace_secs: 60,
        preview_local_outputs: false,
        preview_default_rows: 500,
        preview_max_rows: 5_000,
        lease_ttl_secs: 30,
        probe_timeout_secs: 5,
        env_file: None,
        no_env_file: true,
        // The console's SPA fallback answers an unrouted path with the same
        // JSON 404 a handler gives, which would hide an unwired route.
        no_ui: true,
        cluster: false,
        cluster_poll_secs: 2,
        cluster_max_attempts: 3,
        triggers: None,
        templates_sync: None,
        policy: None,
        otel_config: None,
        callback_allow_host: Vec::new(),
        mcp: false,
        mcp_allow_mutations: false,
        require_approval: Vec::new(),
        approval_expiry_secs: 86_400,
        vault_key: None,
        vault_previous_key: Vec::new(),
        connect_providers: None,
        allow_subprocess_connectors: false,
        references: Default::default(),
    };
    let mut config = ServeConfig::from_args(args).unwrap();
    config.log_level = "warn".into();
    tokio::spawn(async move {
        let mcp = faucet_cli::serve::McpServeSettings {
            enabled: cfg!(feature = "mcp"),
            allow_mutations: false,
        };
        let _ = faucet_cli::serve::run_server(config, mcp).await;
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

    // The probe must tell an unrouted path from a handler 404: an undocumented
    // path gets a bare routing 404 with no error envelope.
    let unrouted = client
        .get(format!("{base}/v1/probe-no-such-route"))
        .send()
        .await
        .unwrap();
    assert_eq!(unrouted.status().as_u16(), 404);
    let body = unrouted.text().await.unwrap_or_default();
    assert!(
        !body.contains("\"error\""),
        "an unrouted path must not look like a handler 404: {body:?}"
    );

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

#[test]
fn list_runs_documents_every_query_parameter() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../docs/openapi.yaml");
    let doc: faucet_cli::yaml::Value =
        faucet_cli::yaml::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let params: BTreeSet<String> = doc["paths"]["/v1/runs"]["get"]["parameters"]
        .as_sequence()
        .expect("listRuns parameters")
        .iter()
        .map(|p| p["name"].as_str().unwrap().to_string())
        .collect();
    let expected: BTreeSet<String> = [
        "status", "name", "since", "until", "limit", "cursor", "tenant", "label", "labels",
    ]
    .into_iter()
    .map(str::to_string)
    .collect();
    assert_eq!(params, expected);
}
