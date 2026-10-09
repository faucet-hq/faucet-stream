//! Template test bundles and the launch gate (#856), end to end: a SQLite
//! registry (storage, retention, cascade), the HTTP control plane with
//! `--require-template-tests` (refusal with failing cases, the admin override
//! and its audit entry, the tests endpoints, rollback, MCP), approved change
//! requests, template sync, the Template Hub bundle runner and the CLI.
#![cfg(all(
    feature = "templates",
    feature = "templates-sync",
    feature = "mcp",
    feature = "serve-history-sqlite"
))]

use std::time::Duration;

use faucet_cli::serve::history::templates::{TemplateTestResult, VersionSelector};
use faucet_cli::template_tests::{GateStatus, LaunchGate};
use faucet_cli::templates::{RegisterRequest, TemplateStore};
use serde_json::{Value, json};

fn pipeline(name: &str, expect_written: usize) -> String {
    format!(
        r#"kind: pipeline
version: 1
name: {name}
params:
  region:
    type: string
    default: us
    values: [us, eu]
pipeline:
  source:
    type: rest
    config:
      base_url: "https://${{param.region}}.example.com"
      path: /rows
  sink:
    type: jsonl
    config:
      path: ./out.jsonl
tests:
  suite:
    auto: {{ enum_coverage: true, defaults_baseline: true }}
  fixtures:
    - name: rows
      retries: 1
      input: [{{ "id": 1 }}, {{ "id": 2 }}]
      expect: {{ records_written: {expect_written} }}
"#
    )
}

fn req(body: String) -> RegisterRequest {
    RegisterRequest {
        id: None,
        body,
        format: faucet_cli::serve::load::ConfigFormat::Yaml,
        description: None,
        tags: Vec::new(),
        launch: false,
        created_by: Some("ci".into()),
        test: false,
        gate: LaunchGate::default(),
    }
}

async fn sqlite(dir: &std::path::Path) -> TemplateStore {
    faucet_cli::templates::resolve_store_url(&format!(
        "sqlite:{}",
        dir.join("registry.db").display()
    ))
    .await
    .unwrap()
}

#[tokio::test]
async fn sqlite_records_prunes_cascades_and_gates() {
    let dir = tempfile::tempdir().unwrap();
    let s = sqlite(dir.path()).await;
    let mut r = req(pipeline("orders", 9));
    r.test = true;
    let reg = faucet_cli::templates::register_tested(&s, r).await.unwrap();
    let first = reg.tests.unwrap();
    assert!(!first.passed);
    assert_eq!(first.cases.iter().find(|c| c.name == "rows").unwrap().attempts, 2);

    let gate = LaunchGate::new(true);
    let err = faucet_cli::templates::launch_gated(&s, "orders", VersionSelector::Pinned(1), None, &gate)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("failed") && err.contains("rows:"), "{err}");
    let skip = gate.clone().with_skip(Some("INC-7".into())).unwrap();
    faucet_cli::templates::launch_gated(&s, "orders", VersionSelector::Pinned(1), Some("root"), &skip)
        .await
        .unwrap();

    // Reopen: the result and the launch note survived.
    drop(s);
    let s = sqlite(dir.path()).await;
    let log = s.template_launches("orders").await.unwrap();
    assert_eq!(log[0].tests_skipped.as_deref(), Some("INC-7"));
    let stored = s.template_test_results("orders", Some(1), 5).await.unwrap();
    assert_eq!(stored, vec![first.clone()]);

    // Retention keeps the newest RESULTS_RETAIN per version.
    for i in 0..(faucet_cli::serve::history::templates::RESULTS_RETAIN + 3) {
        let mut t: TemplateTestResult = first.clone();
        t.recorded_at = first.recorded_at + chrono::Duration::seconds(i as i64 + 1);
        s.template_record_test(&t).await.unwrap();
    }
    let all = s.template_test_results("orders", None, 100).await.unwrap();
    assert_eq!(all.len(), faucet_cli::serve::history::templates::RESULTS_RETAIN);
    assert!(all[0].recorded_at > all[1].recorded_at, "newest first");

    faucet_cli::templates::register(&s, req(pipeline("orders", 2))).await.unwrap();
    let t2 = faucet_cli::templates::bundle::test_version(&s, "orders", 2, None)
        .await
        .unwrap();
    assert!(t2.passed);
    let v = faucet_cli::templates::bundle::version_tests(&s, "orders", 2, &gate, 3)
        .await
        .unwrap();
    assert_eq!(v.gate.status, GateStatus::Passed);

    s.template_delete("orders", Some(1)).await.unwrap();
    assert!(s.template_test_results("orders", Some(1), 5).await.unwrap().is_empty());
    assert_eq!(s.template_test_results("orders", None, 5).await.unwrap().len(), 1);
    s.template_delete("orders", None).await.unwrap();
    assert!(s.template_test_results("orders", None, 5).await.unwrap().is_empty());
}

#[tokio::test]
async fn sync_records_results_and_launches_through_the_gate() {
    use faucet_cli::templates::sync::{apply, plan};
    let dir = tempfile::tempdir().unwrap();
    let s = sqlite(dir.path()).await;
    let register = |id: &str, body: String| plan::SyncAction::Register {
        id: id.into(),
        body,
        format: faucet_cli::serve::load::ConfigFormat::Yaml,
        description: None,
        launch: true,
        tags: Vec::new(),
        replaces: None,
    };
    let out = apply::apply_gated(
        &s,
        plan::SyncPlan {
            origin: "gh".into(),
            actions: vec![
                register("bad", pipeline("bad", 9)),
                register("good", pipeline("good", 2)),
            ],
        },
        "sync:gh",
        &LaunchGate::new(true),
    )
    .await;
    assert_eq!(out.registered.len(), 2, "{out:?}");
    assert_eq!(out.tested.len(), 2);
    assert!(!out.tested[0].passed && out.tested[1].passed);
    assert_eq!(out.failed.len(), 1);
    assert!(out.failed[0].error.contains("cannot be launched"), "{out:?}");
    assert!(!out.registered[0].launched && out.registered[1].launched);
    assert_eq!(s.template_state("good").await.unwrap().stable, Some(1));
    assert_eq!(s.template_state("bad").await.unwrap().stable, None);

    let relaunch = apply::apply_gated(
        &s,
        plan::SyncPlan {
            origin: "gh".into(),
            actions: vec![plan::SyncAction::Launch {
                id: "bad".into(),
                version: 1,
            }],
        },
        "sync:gh",
        &LaunchGate::new(true),
    )
    .await;
    assert_eq!(relaunch.failed.len(), 1);
    let report = faucet_cli::templates::sync::SyncReport {
        origin: "gh".into(),
        kind: "github",
        dry_run: false,
        warnings: Vec::new(),
        plan: Vec::new(),
        outcome: Some(out),
    };
    assert!(report.render_human().contains("tested     bad v1: FAIL"));
}

fn write(dir: &std::path::Path, rel: &str, body: &str) -> std::path::PathBuf {
    let p = dir.join(rel);
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    std::fs::write(&p, body).unwrap();
    p
}

const HUB_SOURCE: &str = r#"kind: source-template
name: acme
description: acme exports
source:
  type: rest
  config: { base_url: "https://example.com" }
streams:
  - name: orders
    source: { config: { path: /orders } }
tests:
  sink: files
  suite:
    auto: { defaults_baseline: true }
  requires_suites:
    - { name: rest-conformance, version: "^1" }
"#;

const HUB_SINK: &str = r#"kind: sink-template
name: files
description: local files
sink: { type: jsonl, config: {} }
per_stream: { path: "./out/${stream}.jsonl" }
write_mode_aliases: { overwrite: append }
"#;

const HUB_SUITE: &str = "kind: test-suite\nname: rest-conformance\nrelease: 1.4.0\nsuite:\n  auto: { defaults_baseline: true }\n";

#[tokio::test]
async fn hub_lint_and_check_run_bundles() {
    use faucet_cli::cli::{HubArgs, HubCheckArgs, HubCommand, HubLintArgs, HubPairArgs};
    let dir = tempfile::tempdir().unwrap();
    let hub = dir.path().to_string_lossy().to_string();
    write(dir.path(), "source-templates/acme.yaml", HUB_SOURCE);
    write(dir.path(), "sink-templates/files.yaml", HUB_SINK);
    write(dir.path(), "test-suites/rest-conformance.yaml", HUB_SUITE);
    let out = faucet_cli::hub::bundles::run_file(
        &dir.path().join("source-templates/acme.yaml"),
        dir.path(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(out.passed(), "{out:#?}");

    let lint = |files: Vec<std::path::PathBuf>| HubArgs {
        command: HubCommand::Lint(HubLintArgs {
            hub: Some(hub.clone()),
            files,
            json: true,
        }),
    };
    faucet_cli::commands::hub::run(lint(Vec::new())).await.unwrap();
    let check = || HubArgs {
        command: HubCommand::Check(HubCheckArgs {
            pair: HubPairArgs {
                source: "acme".into(),
                sink: "files".into(),
                overlay: None,
                hub: vec![hub.clone()],
                source_hub: None,
                sink_hub: None,
                overlay_hub: None,
                trust: Vec::new(),
            },
            json: false,
        }),
    };
    faucet_cli::commands::hub::run(check()).await.unwrap();

    // A shared suite the range no longer matches fails both.
    write(
        dir.path(),
        "test-suites/rest-conformance.yaml",
        &HUB_SUITE.replace("1.4.0", "2.0.0"),
    );
    let err = faucet_cli::commands::hub::run(lint(Vec::new())).await.unwrap_err();
    assert!(err.to_string().contains("findings"), "{err}");
    let err = faucet_cli::commands::hub::run(check()).await.unwrap_err();
    assert!(err.to_string().contains("test bundle failed"), "{err}");
    let err = faucet_cli::commands::hub::run(lint(vec![
        dir.path().join("source-templates/acme.yaml"),
        dir.path().join("test-suites/rest-conformance.yaml"),
    ]))
    .await
    .unwrap_err();
    assert!(err.to_string().contains("findings"), "{err}");
}

#[tokio::test]
async fn cli_tests_a_registered_version_and_a_template_file() {
    use faucet_cli::cli::{TemplateArgs, TemplateCommand, TemplateTestArgs};
    let dir = tempfile::tempdir().unwrap();
    let store_url = format!("sqlite:{}", dir.path().join("registry.db").display());
    let s = faucet_cli::templates::resolve_store_url(&store_url).await.unwrap();
    faucet_cli::templates::register(&s, req(pipeline("orders", 2))).await.unwrap();
    faucet_cli::templates::register(&s, req(pipeline("orders", 7))).await.unwrap();
    let args = |target: &str, no_record: bool, filter: Option<&str>| TemplateArgs {
        command: TemplateCommand::Test(TemplateTestArgs {
            suite: target.into(),
            store: Some(store_url.clone()),
            select: None,
            filter: filter.map(str::to_string),
            json: false,
            env_file: None,
            no_env_file: true,
            no_record,
            hub: Some(dir.path().to_string_lossy().to_string()),
        }),
    };
    faucet_cli::commands::template::run(args("orders@1", false, None))
        .await
        .unwrap();
    let err = faucet_cli::commands::template::run(args("orders", false, None))
        .await
        .unwrap_err();
    assert!(matches!(err, faucet_cli::error::CliError::TestsFailed { .. }));
    assert_eq!(s.template_test_results("orders", None, 10).await.unwrap().len(), 2);
    faucet_cli::commands::template::run(args("orders@1", true, Some("auto:*")))
        .await
        .unwrap();
    assert_eq!(s.template_test_results("orders", None, 10).await.unwrap().len(), 2);
    let err = faucet_cli::commands::template::run(args("orders@1", false, Some("auto:*")))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("--no-record"), "{err}");

    let file = write(dir.path(), "orders.yaml", &pipeline("orders", 2));
    faucet_cli::commands::template::run(args(file.to_str().unwrap(), false, None))
        .await
        .unwrap();
    let bad = write(dir.path(), "bad.yaml", &pipeline("bad", 3));
    assert!(
        faucet_cli::commands::template::run(args(bad.to_str().unwrap(), false, None))
            .await
            .is_err()
    );
}

// ── HTTP ────────────────────────────────────────────────────────────────────

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

const AUTH: &str = "principals:\n\
    \x20 - { name: alice, token: admin-tok, role: admin }\n\
    \x20 - { name: erin, token: erin-tok, role: admin }\n\
    \x20 - { name: bob, token: op-tok, role: operator }\n\
    \x20 - { name: carol, token: viewer-tok, role: viewer }\n\
    approvals:\n\
    \x20 rules:\n\
    \x20   - { kinds: [template_register, template_launch], roles: [admin, operator] }\n";

async fn serve(dir: &std::path::Path, require_approval: Vec<String>) -> String {
    let port = free_port();
    let auth = dir.join(format!("auth-{port}.yaml"));
    std::fs::write(&auth, AUTH).unwrap();
    let args = faucet_cli::cli::ServeArgs {
        listen: format!("127.0.0.1:{port}"),
        auth_token: None,
        auth_config: Some(auth),
        read_token: None,
        write_token: None,
        admin_token: None,
        no_auth: false,
        max_concurrent_runs: Some(2),
        max_queued_runs: Some(8),
        default_config: None,
        history: Some(format!("sqlite:{}", dir.join(format!("h-{port}.db")).display())),
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
        no_ui: true,
        cluster: false,
        cluster_poll_secs: 2,
        cluster_max_attempts: 3,
        triggers: None,
        templates_sync: None,
        policy: None,
        otel_config: None,
        callback_allow_host: Vec::new(),
        mcp: true,
        mcp_allow_mutations: true,
        require_approval,
        approval_expiry_secs: 86_400,
        require_template_tests: true,
        vault_key: None,
        vault_previous_key: Vec::new(),
        connect_providers: None,
        allow_subprocess_connectors: false,
    };
    let mut config = faucet_cli::serve::ServeConfig::from_args(args).unwrap();
    config.log_level = "warn".into();
    tokio::spawn(async move {
        let _ = faucet_cli::serve::run_server(
            config,
            faucet_cli::serve::McpServeSettings {
                enabled: true,
                allow_mutations: true,
            },
        )
        .await;
    });
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    for _ in 0..1200 {
        if client
            .get(format!("{base}/healthz"))
            .send()
            .await
            .is_ok_and(|r| r.status().is_success())
        {
            return base;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("server did not start");
}

async fn call(base: &str, method: &str, token: &str, path: &str, body: Option<Value>) -> (u16, Value) {
    let client = reqwest::Client::new();
    let mut rb = client
        .request(method.parse().unwrap(), format!("{base}{path}"))
        .bearer_auth(token);
    if let Some(b) = body {
        rb = rb.json(&b);
    }
    let r = rb.send().await.unwrap();
    let code = r.status().as_u16();
    let text = r.text().await.unwrap();
    (code, serde_json::from_str(&text).unwrap_or(Value::String(text)))
}

#[tokio::test(flavor = "multi_thread")]
async fn http_gate_refuses_overrides_audits_and_reports() {
    let dir = tempfile::tempdir().unwrap();
    let base = serve(dir.path(), Vec::new()).await;
    let b = base.as_str();

    let (code, r) = call(
        b,
        "POST",
        "admin-tok",
        "/v1/templates",
        Some(json!({ "config": pipeline("orders", 9), "test": true })),
    )
    .await;
    assert_eq!(code, 201, "{r}");
    assert_eq!(r["tests"]["passed"], false, "{r}");

    let (code, r) = call(b, "POST", "admin-tok", "/v1/templates/orders/launch", Some(json!({ "version": 1 }))).await;
    assert_eq!(code, 422, "{r}");
    let failing = r["error"]["details"]["gate"]["failing"].as_array().unwrap();
    assert!(failing.iter().any(|f| f.as_str().unwrap().starts_with("rows:")), "{r}");

    let (code, _) = call(
        b,
        "POST",
        "op-tok",
        "/v1/templates/orders/launch",
        Some(json!({ "version": 1, "skip_tests_reason": "x" })),
    )
    .await;
    assert_eq!(code, 403, "the override rides an admin-only route");
    let (code, r) = call(
        b,
        "POST",
        "admin-tok",
        "/v1/templates/orders/launch",
        Some(json!({ "version": 1, "skip_tests_reason": "  " })),
    )
    .await;
    assert_eq!(code, 422, "{r}");
    let (code, r) = call(
        b,
        "POST",
        "admin-tok",
        "/v1/templates/orders/launch",
        Some(json!({ "version": 1, "skip_tests_reason": "INC-42 hotfix" })),
    )
    .await;
    assert_eq!(code, 200, "{r}");
    assert_eq!(r["tests"]["skipped"], "INC-42 hotfix");

    let (_, audit) = call(b, "GET", "admin-tok", "/v1/audit?action=template.launch", None).await;
    let entries = audit["entries"].as_array().unwrap();
    assert!(
        entries
            .iter()
            .any(|e| e["result"] == "refused" && e["target"] == "template:orders@1"),
        "{audit}"
    );
    assert!(
        entries
            .iter()
            .any(|e| e["result"] == "ok" && e["detail"] == "tests skipped: INC-42 hotfix"),
        "{audit}"
    );

    let (code, r) = call(
        b,
        "POST",
        "admin-tok",
        "/v1/templates",
        Some(json!({ "config": pipeline("orders", 2), "launch": true })),
    )
    .await;
    assert_eq!(code, 201, "{r}");
    assert_eq!(r["gate"]["status"], "passed", "{r}");
    assert_eq!(r["tests"]["passed"], true);

    let (code, r) = call(b, "GET", "viewer-tok", "/v1/templates/orders/versions/1/tests", None).await;
    assert_eq!(code, 200, "{r}");
    assert_eq!(r["gate"]["status"], "failed");
    assert_eq!(r["gate"]["allowed"], false);
    let (code, r) = call(b, "POST", "op-tok", "/v1/templates/orders/versions/2/test", None).await;
    assert_eq!(code, 403, "{r}");
    let (code, r) = call(b, "POST", "admin-tok", "/v1/templates/orders/versions/2/test", None).await;
    assert_eq!(code, 200, "{r}");
    assert_eq!(r["passed"], true);

    let (_, d) = call(b, "GET", "viewer-tok", "/v1/templates/orders?version=newest", None).await;
    assert_eq!(d["tests"]["require_tests"], true, "{d}");
    assert_eq!(d["tests"]["versions"].as_array().unwrap().len(), 2);
    assert_eq!(d["launches"][1]["tests_skipped"], "INC-42 hotfix", "{d}");

    let (code, r) = call(b, "POST", "admin-tok", "/v1/templates/orders/rollback", None).await;
    assert_eq!(code, 422, "{r}");
    let (code, r) = call(
        b,
        "POST",
        "admin-tok",
        "/v1/templates/orders/rollback",
        Some(json!({ "skip_tests_reason": "back out v2" })),
    )
    .await;
    assert_eq!(code, 200, "{r}");
    assert_eq!(r["version"], 1);

    // MCP: the same gate, the same override, and the test tool.
    let mcp = |id: u32, name: &str, args: Value| {
        json!({ "jsonrpc": "2.0", "id": id, "method": "tools/call", "params": { "name": name, "arguments": args } })
    };
    let (_, r) = call(b, "POST", "admin-tok", "/mcp", Some(mcp(1, "launch_template", json!({ "id": "orders", "version": 1 })))).await;
    assert_eq!(r["result"]["isError"], false, "already live: {r}");
    let (_, r) = call(b, "POST", "admin-tok", "/mcp", Some(mcp(2, "launch_template", json!({ "id": "orders", "version": 2 })))).await;
    assert_eq!(r["result"]["isError"], false, "{r}");
    let (_, r) = call(b, "POST", "admin-tok", "/mcp", Some(mcp(3, "rollback_template", json!({ "id": "orders" })))).await;
    assert_eq!(r["result"]["isError"], true, "{r}");
    assert!(r.to_string().contains("cannot be launched"), "{r}");
    let (_, r) = call(
        b,
        "POST",
        "admin-tok",
        "/mcp",
        Some(mcp(4, "rollback_template", json!({ "id": "orders", "skip_tests_reason": "agent rollback" }))),
    )
    .await;
    assert_eq!(r["result"]["isError"], false, "{r}");
    let (_, r) = call(b, "POST", "admin-tok", "/mcp", Some(mcp(5, "test_template", json!({ "id": "orders", "version": 2 })))).await;
    assert_eq!(r["result"]["isError"], false, "{r}");
    let (_, r) = call(
        b,
        "POST",
        "admin-tok",
        "/mcp",
        Some(mcp(6, "register_template", json!({ "config": pipeline("orders", 2), "test": true }))),
    )
    .await;
    let text = r["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("\"passed\": true"), "{text}");
}

#[tokio::test(flavor = "multi_thread")]
async fn approved_launch_change_goes_through_the_gate() {
    let dir = tempfile::tempdir().unwrap();
    let base = serve(dir.path(), vec!["template_launch".into()]).await;
    let b = base.as_str();
    let (code, r) = call(b, "POST", "admin-tok", "/v1/templates", Some(json!({ "config": pipeline("orders", 9), "test": true }))).await;
    assert_eq!(code, 201, "{r}");

    let propose = |payload: Value| json!({ "kind": "template_launch", "payload": payload, "reason": "ship" });
    let (code, r) = call(b, "POST", "op-tok", "/v1/changes", Some(propose(json!({ "id": "orders", "version": 1, "skip_tests_reason": "x" })))).await;
    assert_eq!(code, 403, "an operator may not ask to skip tests: {r}");

    let (code, c) = call(b, "POST", "admin-tok", "/v1/changes", Some(propose(json!({ "id": "orders", "version": 1 })))).await;
    assert_eq!(code, 201, "{c}");
    assert_eq!(c["plan"]["summary"]["tests"]["status"], "failed", "{c}");
    let id = c["id"].as_str().unwrap().to_string();
    let (_, done) = call(b, "POST", "erin-tok", &format!("/v1/changes/{id}/approve"), Some(json!({}))).await;
    assert_eq!(done["status"], "failed", "{done}");
    assert!(done["error"].as_str().unwrap_or_default().contains("cannot be launched"), "{done}");

    let (_, c) = call(b, "POST", "admin-tok", "/v1/changes", Some(propose(json!({ "id": "orders", "version": 1, "skip_tests_reason": "approved risk" })))).await;
    let id = c["id"].as_str().unwrap().to_string();
    let (_, done) = call(b, "POST", "erin-tok", &format!("/v1/changes/{id}/approve"), Some(json!({}))).await;
    assert_eq!(done["status"], "executed", "{done}");
    let (_, d) = call(b, "GET", "viewer-tok", "/v1/templates/orders?version=newest", None).await;
    assert_eq!(d["launches"][0]["tests_skipped"], "approved risk", "{d}");
}
