//! RBAC + audit-log integration tests for `faucet serve` (#205). Boots a real
//! server with a multi-principal `--auth-config` and asserts role enforcement
//! (`403` for a viewer's write, `200` for its read) and that mutating / denied
//! actions land in the admin-only audit log. Requires the `serve` feature.
#![cfg(feature = "serve")]

use faucet_cli::cli::ServeArgs;
use faucet_cli::serve::ServeConfig;
use std::time::Duration;

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Three principals: `admin-tok` → admin, `viewer-tok` → viewer,
/// `operator-tok` → operator.
const AUTH_CONFIG: &str = "principals:\n\
    \x20 - name: alice\n\
    \x20   token: admin-tok\n\
    \x20   role: admin\n\
    \x20 - name: bob\n\
    \x20   token: viewer-tok\n\
    \x20   role: viewer\n\
    \x20 - name: carol\n\
    \x20   token: operator-tok\n\
    \x20   role: operator\n";

fn args_with_auth_config(port: u16, auth_config: std::path::PathBuf) -> ServeArgs {
    ServeArgs {
        listen: format!("127.0.0.1:{port}"),
        auth_token: None,
        auth_config: Some(auth_config),
        read_token: None,
        write_token: None,
        admin_token: None,
        no_auth: false,
        max_concurrent_runs: Some(4),
        max_queued_runs: Some(16),
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
        policy: None,
        callback_allow_host: Vec::new(),
        mcp: false,
        mcp_allow_mutations: false,
        require_approval: Vec::new(),
        approval_expiry_secs: 86_400,
        vault_key: None,
        vault_previous_key: Vec::new(),
        connect_providers: None,
    }
}

/// Boot a server whose auth is the two-principal RBAC config. Returns the
/// tempdir (kept alive for the server's lifetime — it holds the auth file).
async fn spawn_rbac_server(port: u16) -> tempfile::TempDir {
    spawn_rbac_server_with(port, Default::default()).await
}

async fn spawn_rbac_server_with(
    port: u16,
    mcp: faucet_cli::serve::McpServeSettings,
) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let auth_path = dir.path().join("auth.yaml");
    std::fs::write(&auth_path, AUTH_CONFIG).unwrap();
    let mut config = ServeConfig::from_args(args_with_auth_config(port, auth_path)).unwrap();
    config.log_level = "warn".into();
    tokio::spawn(async move {
        let _ = faucet_cli::serve::run_server(config, mcp).await;
    });
    let client = reqwest::Client::new();
    // 30s: this polls a server starting up next to the whole
    // workspace test suite, so the budget has to survive a loaded
    // runner. Costs nothing when it is already up.
    for _ in 0..1200 {
        if client
            .get(format!("http://127.0.0.1:{port}/healthz"))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
        {
            return dir;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("server did not become healthy on port {port}");
}

fn csv_to_jsonl_yaml(input: &std::path::Path, output: &std::path::Path) -> String {
    format!(
        "version: 1\npipeline:\n  source: {{ type: csv, config: {{ path: \"{}\" }} }}\n  sink: {{ type: jsonl, config: {{ path: \"{}\" }} }}\n",
        input.display(),
        output.display()
    )
}

/// A viewer is denied `POST /v1/runs` (403) but allowed `GET /v1/runs` (200);
/// an admin can submit (202). Covers the core acceptance criterion.
#[tokio::test(flavor = "multi_thread")]
async fn viewer_is_readonly_admin_can_write() {
    let port = free_port();
    let _dir = spawn_rbac_server(port).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");

    // No token → 401.
    assert_eq!(
        client
            .get(format!("{base}/v1/runs"))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );

    // Viewer can read.
    assert_eq!(
        client
            .get(format!("{base}/v1/runs"))
            .bearer_auth("viewer-tok")
            .send()
            .await
            .unwrap()
            .status(),
        200,
        "viewer must be allowed GET /v1/runs"
    );

    // Viewer cannot write.
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    std::fs::write(&input, "name\nalice\n").unwrap();
    let cfg = csv_to_jsonl_yaml(&input, &dir.path().join("out.jsonl"));
    let denied = client
        .post(format!("{base}/v1/runs"))
        .bearer_auth("viewer-tok")
        .json(&serde_json::json!({ "config": cfg }))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), 403, "viewer must be denied POST /v1/runs");
    let err: serde_json::Value = denied.json().await.unwrap();
    assert_eq!(err["error"]["code"], "forbidden");

    // Admin can write.
    let accepted = client
        .post(format!("{base}/v1/runs"))
        .bearer_auth("admin-tok")
        .json(&serde_json::json!({ "config": cfg }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        accepted.status(),
        202,
        "admin must be allowed POST /v1/runs"
    );
}

/// The audit log is admin-only (viewer → 403) and records both a successful
/// mutating action (admin's `run.submit`, result `ok`) and a denied one
/// (viewer's `run.submit`, result `denied`).
#[tokio::test(flavor = "multi_thread")]
async fn audit_log_records_actions_and_is_admin_only() {
    let port = free_port();
    let _dir = spawn_rbac_server(port).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");

    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    std::fs::write(&input, "name\nalice\n").unwrap();
    let cfg = csv_to_jsonl_yaml(&input, &dir.path().join("out.jsonl"));
    let body = serde_json::json!({ "config": cfg });

    // Admin submit (recorded ok) and a viewer submit (denied, recorded).
    let submit: serde_json::Value = client
        .post(format!("{base}/v1/runs"))
        .bearer_auth("admin-tok")
        .json(&body)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let run_id = submit["run_id"].as_str().unwrap().to_string();

    client
        .post(format!("{base}/v1/runs"))
        .bearer_auth("viewer-tok")
        .json(&body)
        .send()
        .await
        .unwrap();

    // Viewer is denied the audit log.
    assert_eq!(
        client
            .get(format!("{base}/v1/audit"))
            .bearer_auth("viewer-tok")
            .send()
            .await
            .unwrap()
            .status(),
        403,
        "viewer must be denied GET /v1/audit"
    );

    // Admin reads the audit log.
    let audit: serde_json::Value = client
        .get(format!("{base}/v1/audit"))
        .bearer_auth("admin-tok")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let entries = audit["entries"].as_array().expect("entries array");

    // A successful run.submit by alice, carrying the run_id + a fingerprint.
    let submit_ok = entries
        .iter()
        .find(|e| e["action"] == "run.submit" && e["result"] == "ok" && e["principal"] == "alice");
    let submit_ok = submit_ok.unwrap_or_else(|| panic!("no admin run.submit entry in {audit:#}"));
    assert_eq!(submit_ok["run_id"], run_id);
    assert!(
        submit_ok["config_fingerprint"].is_string(),
        "submit audit must carry a config fingerprint"
    );

    // A denied run.submit by bob.
    assert!(
        entries.iter().any(|e| {
            e["action"] == "run.submit" && e["result"] == "denied" && e["principal"] == "bob"
        }),
        "denied viewer submit must be audited; got {audit:#}"
    );

    // Filtering by principal narrows the result set.
    let bob_only: serde_json::Value = client
        .get(format!("{base}/v1/audit?principal=bob"))
        .bearer_auth("admin-tok")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        bob_only["entries"]
            .as_array()
            .unwrap()
            .iter()
            .all(|e| e["principal"] == "bob"),
        "principal filter must only return that principal's entries"
    );
}

// ── #608: the verified read/write/admin permission matrix ────────────────────
//
// The contract an operator is handed three tokens on: a read token can never
// change anything. Enforced as a table walk over **every** registered `/v1`
// route rather than a hand-picked sample, so a new mutating route that nobody
// classified fails here instead of quietly becoming viewer-reachable.

/// Every (method, route-template) the router registers under the current
/// feature set — the same inventory `serve_openapi.rs` checks the spec against.
/// Duplicated deliberately: if the two ever disagree, one of them is wrong and
/// this test is the one that decides whether a route is safe.
fn all_v1_routes() -> Vec<(axum::http::Method, &'static str)> {
    use axum::http::Method;
    #[allow(unused_mut)]
    let mut v: Vec<(Method, &'static str)> = vec![
        (Method::POST, "/v1/runs"),
        (Method::GET, "/v1/runs"),
        (Method::GET, "/v1/runs/{id}"),
        (Method::DELETE, "/v1/runs/{id}"),
        (Method::POST, "/v1/runs/{id}/cancel"),
        (Method::GET, "/v1/runs/{id}/logs"),
        (Method::GET, "/v1/schemas"),
        (Method::GET, "/v1/schemas/{kind}/{name}"),
        (Method::POST, "/v1/doctor"),
        (Method::POST, "/v1/backfill"),
        (Method::POST, "/v1/plan"),
        (Method::POST, "/v1/dlq/inspect"),
        (Method::POST, "/v1/dlq/replay"),
        (Method::POST, "/v1/dlq/discard"),
        (Method::GET, "/v1/audit"),
        (Method::POST, "/v1/reload"),
        (Method::GET, "/v1/whoami"),
        (Method::POST, "/mcp"),
        (Method::POST, "/v1/changes"),
        (Method::GET, "/v1/changes"),
        (Method::GET, "/v1/changes/{id}"),
        (Method::POST, "/v1/changes/{id}/approve"),
        (Method::POST, "/v1/changes/{id}/reject"),
    ];
    #[cfg(feature = "triggers")]
    v.extend([
        (Method::POST, "/v1/triggers/{name}"),
        (Method::PUT, "/v1/triggers/{name}"),
    ]);
    #[cfg(feature = "catalog")]
    v.extend([
        (Method::GET, "/v1/catalog/datasets"),
        (Method::GET, "/v1/catalog/datasets/{id}"),
        (Method::GET, "/v1/catalog/lineage"),
        (Method::POST, "/v1/catalog/datasets/{id}/consumers"),
        (Method::GET, "/v1/usage"),
        (Method::GET, "/v1/local-outputs"),
        (Method::DELETE, "/v1/local-outputs/{id}"),
        (Method::POST, "/v1/local-outputs/cleanup"),
        (Method::GET, "/v1/local-outputs/{id}/preview"),
    ]);
    #[cfg(feature = "templates")]
    v.extend([
        (Method::POST, "/v1/templates"),
        (Method::GET, "/v1/templates"),
        (Method::GET, "/v1/templates/{id}"),
        (Method::DELETE, "/v1/templates/{id}"),
        (Method::POST, "/v1/templates/{id}/runs"),
        (Method::POST, "/v1/templates/{id}/tags"),
        (Method::POST, "/v1/templates/{id}/launch"),
        (Method::POST, "/v1/templates/{id}/rollback"),
        (Method::POST, "/v1/templates/{id}/deprecate"),
        (
            Method::POST,
            "/v1/templates/{id}/versions/{version}/deprecate",
        ),
    ]);
    #[cfg(feature = "templates-sync")]
    v.extend([
        (Method::POST, "/v1/templates/sync"),
        (Method::POST, "/v1/templates/{id}/publish"),
    ]);
    #[cfg(feature = "tenants")]
    v.extend([
        (Method::GET, "/v1/tenants"),
        (Method::POST, "/v1/tenants"),
        (Method::GET, "/v1/tenants/{tenant}"),
        (Method::PATCH, "/v1/tenants/{tenant}"),
        (Method::DELETE, "/v1/tenants/{tenant}"),
        (Method::GET, "/v1/tenants/{tenant}/connections"),
        (Method::POST, "/v1/tenants/{tenant}/connections"),
        (Method::GET, "/v1/tenants/{tenant}/connections/{name}"),
        (Method::PUT, "/v1/tenants/{tenant}/connections/{name}"),
        (Method::DELETE, "/v1/tenants/{tenant}/connections/{name}"),
        (Method::POST, "/v1/tenants/{tenant}/connect/{provider}"),
        (Method::POST, "/v1/tenants/{tenant}/runs"),
        (Method::POST, "/v1/tenants/{tenant}/templates/{id}/runs"),
        (Method::POST, "/v1/templates/{id}/fanout"),
        (Method::GET, "/v1/connect/providers"),
    ]);
    v
}

/// A route is *mutating* if it can change server- or destination-side state.
/// `GET` never is. `/mcp` is a POST that is **not** mutating at this layer —
/// its baseline is a read scope and its one mutating tool re-checks `RunWrite`
/// inside the handler.
fn is_mutating(method: &axum::http::Method, path: &str) -> bool {
    use axum::http::Method;
    // POSTs that carry a body but change nothing. Enumerated, not inferred:
    // adding a route here is the one way to make it viewer-reachable, so the
    // decision is visible in a diff.
    //
    // - `/mcp` — its baseline is a read scope; the one mutating tool
    //   (`run_pipeline`) re-checks `RunWrite` inside the handler.
    // - `/v1/dlq/inspect` — reads a DLQ location and summarises it. Note for
    //   operators: the location is caller-supplied, so a read token can ask
    //   the server to read a path on its filesystem. That is the same trust
    //   boundary as run logs (which carry record data) and is why the DLQ
    //   endpoints are not exposed to the public internet.
    // - `/v1/plan` — plans a config: expands it, runs a caller-supplied sample
    //   through the offline harness, reads the catalog. No sink is written and
    //   no run starts; the one connector it builds is the sink, for its
    //   non-mutating `check()` probe — the same as `POST /v1/doctor` does, but
    //   doctor stays operator-only because it probes *sources* with real reads.
    const READ_ONLY_POSTS: &[&str] = &["/mcp", "/v1/dlq/inspect", "/v1/plan"];
    if READ_ONLY_POSTS.contains(&path) {
        return false;
    }
    matches!(
        *method,
        Method::POST | Method::PUT | Method::DELETE | Method::PATCH
    )
}

#[test]
fn a_read_token_is_denied_on_every_mutating_route() {
    use faucet_cli::serve::rbac::{Role, required_permission};

    let mut reachable: Vec<String> = Vec::new();
    for (method, path) in all_v1_routes() {
        if !is_mutating(&method, path) {
            continue;
        }
        let perm = required_permission(&method, path);
        // An unmapped route is admin-only (fail-closed), which is also a deny
        // for a viewer — but it must be *deliberate*, so it is flagged below.
        let allowed = perm.is_some_and(|p| Role::Viewer.grants(p));
        if allowed {
            reachable.push(format!("{method} {path}"));
        }
    }
    assert!(
        reachable.is_empty(),
        "a read token must never reach a mutating route — these are reachable: {reachable:?}"
    );
}

#[test]
fn a_read_token_can_reach_every_read_route() {
    use faucet_cli::serve::rbac::{Role, required_permission};

    let mut denied: Vec<String> = Vec::new();
    for (method, path) in all_v1_routes() {
        if method != axum::http::Method::GET || path == "/v1/audit" {
            continue; // the audit log is admin-only by design
        }
        let ok = required_permission(&method, path).is_some_and(|p| Role::Viewer.grants(p));
        if !ok {
            denied.push(format!("{method} {path}"));
        }
    }
    assert!(
        denied.is_empty(),
        "a read token must be able to read — these GETs are denied to a viewer, so either \
         the route is misclassified or it belongs on the admin-only list: {denied:?}"
    );
}

#[test]
fn every_route_is_explicitly_classified() {
    use faucet_cli::serve::rbac::required_permission;

    let unmapped: Vec<String> = all_v1_routes()
        .into_iter()
        .filter(|(m, p)| required_permission(m, p).is_none())
        .map(|(m, p)| format!("{m} {p}"))
        .collect();
    assert!(
        unmapped.is_empty(),
        "these routes fall through to the admin-only default. That is fail-closed, so \
         nothing is exposed — but an unclassified route is an accident waiting to be \
         reclassified wrongly. Add them to `required_permission`: {unmapped:?}"
    );
}

#[test]
fn an_operator_token_is_denied_the_audit_log_and_reload() {
    use faucet_cli::serve::rbac::{Permission, Role};
    assert!(!Role::Operator.grants(Permission::AuditRead));
    assert!(!Role::Operator.grants(Permission::Reload));
    assert!(Role::Admin.grants(Permission::AuditRead));
    assert!(Role::Admin.grants(Permission::Reload));
}

/// #698: an operator runs registered templates but never changes what they
/// are — every template lifecycle write is admin-only.
#[cfg(feature = "templates")]
#[test]
fn an_operator_triggers_templates_but_cannot_manage_them() {
    use axum::http::Method;
    use faucet_cli::serve::rbac::{Role, required_permission};
    let allowed =
        |m: &Method, p: &str| required_permission(m, p).is_some_and(|x| Role::Operator.grants(x));
    assert!(allowed(&Method::POST, "/v1/templates/{id}/runs"));
    assert!(allowed(&Method::GET, "/v1/templates/{id}"));
    for path in [
        "/v1/templates",
        "/v1/templates/{id}/tags",
        "/v1/templates/{id}/launch",
        "/v1/templates/{id}/rollback",
        "/v1/templates/{id}/deprecate",
        "/v1/templates/{id}/versions/{version}/deprecate",
        "/v1/templates/sync",
        "/v1/templates/{id}/publish",
    ] {
        assert!(
            !allowed(&Method::POST, path),
            "operator reached POST {path}"
        );
    }
    assert!(!allowed(&Method::DELETE, "/v1/templates/{id}"));
}

/// `GET /v1/whoami` reports each principal's own role and permissions.
#[tokio::test(flavor = "multi_thread")]
async fn whoami_reports_each_principals_role_and_permissions() {
    let port = free_port();
    let _dir = spawn_rbac_server(port).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");
    for (token, principal, role, can_run, can_manage) in [
        ("viewer-tok", "bob", "viewer", false, false),
        ("operator-tok", "carol", "operator", true, false),
        ("admin-tok", "alice", "admin", true, true),
    ] {
        let r = client
            .get(format!("{base}/v1/whoami"))
            .bearer_auth(token)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200, "{role}");
        let me: serde_json::Value = r.json().await.unwrap();
        assert_eq!(me["principal"], principal);
        assert_eq!(me["role"], role);
        let perms: Vec<&str> = me["permissions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p.as_str().unwrap())
            .collect();
        assert!(perms.contains(&"identity"), "{role}: {perms:?}");
        assert_eq!(perms.contains(&"run_write"), can_run, "{role}: {perms:?}");
        assert_eq!(
            perms.contains(&"template_admin"),
            can_manage,
            "{role}: {perms:?}"
        );
    }
    let r = client
        .get(format!("{base}/v1/whoami"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
}

/// #698 over `/mcp`: the template-lifecycle tools are listed for an admin
/// only; an operator still gets `run_template`.
#[cfg(all(feature = "templates", feature = "mcp"))]
#[tokio::test(flavor = "multi_thread")]
async fn mcp_lists_the_template_lifecycle_tools_to_admins_only() {
    let port = free_port();
    let _dir = spawn_rbac_server_with(
        port,
        faucet_cli::serve::McpServeSettings {
            enabled: true,
            allow_mutations: true,
        },
    )
    .await;
    let client = reqwest::Client::new();
    for (token, admin) in [("operator-tok", false), ("admin-tok", true)] {
        let r: serde_json::Value = client
            .post(format!("http://127.0.0.1:{port}/mcp"))
            .bearer_auth(token)
            .json(&serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let names: Vec<&str> = r["result"]["tools"]
            .as_array()
            .unwrap_or_else(|| panic!("{r}"))
            .iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"run_template"), "{token}: {names:?}");
        for t in [
            "register_template",
            "launch_template",
            "rollback_template",
            "deprecate_template",
        ] {
            assert_eq!(names.contains(&t), admin, "{token}: {t} in {names:?}");
        }
    }
}

#[test]
fn an_admin_token_reaches_every_route() {
    use faucet_cli::serve::rbac::{Role, required_permission};
    for (method, path) in all_v1_routes() {
        // An unmapped route is admin-only, so admin reaches it either way.
        if let Some(p) = required_permission(&method, path) {
            assert!(Role::Admin.grants(p), "admin denied {method} {path}");
        }
    }
}

#[test]
fn the_token_trio_maps_each_token_to_its_role() {
    use faucet_cli::serve::rbac::{RbacConfig, Role};

    let cfg = RbacConfig::from_token_trio(Some("r"), Some("w"), Some("a"))
        .expect("valid trio")
        .expect("three tokens means an RBAC config");
    assert_eq!(
        cfg.authenticate("r").expect("read token").role,
        Role::Viewer
    );
    assert_eq!(
        cfg.authenticate("w").expect("write token").role,
        Role::Operator
    );
    assert_eq!(
        cfg.authenticate("a").expect("admin token").role,
        Role::Admin
    );
    assert!(cfg.authenticate("nope").is_none());
}

#[test]
fn a_partial_token_trio_is_allowed() {
    use faucet_cli::serve::rbac::{RbacConfig, Role};
    // A deployment that only hands out read + admin should not have to invent
    // an operator token it will never use.
    let cfg = RbacConfig::from_token_trio(Some("r"), None, Some("a"))
        .expect("valid")
        .expect("some");
    assert_eq!(cfg.authenticate("r").expect("read").role, Role::Viewer);
    assert_eq!(cfg.authenticate("a").expect("admin").role, Role::Admin);
}

#[test]
fn no_trio_tokens_means_no_rbac_config() {
    use faucet_cli::serve::rbac::RbacConfig;
    assert!(
        RbacConfig::from_token_trio(None, None, None)
            .expect("no error")
            .is_none(),
        "with none of the three set the caller must fall through to the other auth modes"
    );
}

#[test]
fn the_trio_rejects_an_empty_or_reused_token() {
    use faucet_cli::serve::rbac::RbacConfig;
    let err = RbacConfig::from_token_trio(Some("  "), None, None).expect_err("empty token");
    assert!(err.to_string().contains("read-token"), "{err}");

    // The same token for two roles would make the role assignment depend on
    // scan order — refused rather than silently resolved.
    let err =
        RbacConfig::from_token_trio(Some("same"), None, Some("same")).expect_err("reused token");
    assert!(err.to_string().contains("reuses a token"), "{err}");
}
