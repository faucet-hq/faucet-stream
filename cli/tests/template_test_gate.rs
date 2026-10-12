//! Template tests and labels (#856), end to end: a SQLite registry (evidence on
//! every label move, a stale claim refused in the write, retention, cascade,
//! the revalidation marker), the HTTP control plane (refused label moves with
//! their failing cases, runs authorized by labels, `allow_untested` labelled
//! and audited, tenants refused, server-side screening and provenance, the
//! background revalidation), MCP, approved change requests, template sync,
//! the Template Hub bundle runner and the CLI.
#![cfg(all(
    feature = "templates",
    feature = "templates-sync",
    feature = "mcp",
    feature = "serve-history-sqlite"
))]

use std::time::Duration;

use faucet_cli::serve::history::templates::{TemplateTestResult, VersionSelector};
use faucet_cli::template_tests::TestStatus;
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
async fn sqlite_records_evidence_refuses_stale_claims_prunes_and_cascades() {
    use faucet_cli::serve::history::templates::EvidenceClaim;
    let dir = tempfile::tempdir().unwrap();
    let s = sqlite(dir.path()).await;
    let reg = faucet_cli::templates::register_tested(&s, req(pipeline("orders", 9)))
        .await
        .unwrap();
    let first = reg.tests.unwrap();
    assert!(!first.passed);
    assert_eq!(
        first
            .cases
            .iter()
            .find(|c| c.name == "rows")
            .unwrap()
            .attempts,
        2
    );
    let err = faucet_cli::templates::launch(&s, "orders", VersionSelector::Pinned(1), None)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("v1 is failing") && err.contains("rows:"),
        "{err}"
    );

    faucet_cli::templates::register(&s, req(pipeline("orders", 2)))
        .await
        .unwrap();
    let out = faucet_cli::templates::launch(&s, "orders", VersionSelector::Pinned(2), Some("root"))
        .await
        .unwrap();
    let evidence = out.tests.unwrap().evidence.unwrap();
    faucet_cli::templates::promote(
        &s,
        "orders",
        faucet_cli::serve::history::templates::VersionChannel::Prod,
        VersionSelector::Pinned(2),
    )
    .await
    .unwrap();

    // Reopen: results, the launch's evidence and the channel's survived.
    drop(s);
    let s = sqlite(dir.path()).await;
    let log = s.template_launches("orders").await.unwrap();
    assert_eq!(log[0].evidence.as_deref(), Some(evidence.as_str()));
    let tags = s.template_tag_records("orders").await.unwrap();
    assert_eq!(tags[0].evidence.as_deref(), Some(evidence.as_str()));
    let stored = s.template_test_results("orders", Some(1), 5).await.unwrap();
    assert_eq!(stored, vec![first.clone()]);

    // A claim a newer result superseded is refused inside the write.
    let rec = s.template_get("orders", Some(2)).await.unwrap().unwrap();
    let claim = EvidenceClaim {
        result_id: evidence.clone(),
        body_sha256: faucet_cli::templates::bundle::body_sha256(&rec.body),
    };
    faucet_cli::templates::bundle::test_version(&s, "orders", 2, None)
        .await
        .unwrap();
    let err = s
        .template_set_tag("orders", "dev", 2, Some(&claim))
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            faucet_cli::serve::history::HistoryError::StaleEvidence(_)
        ),
        "{err}"
    );
    assert!(
        s.template_launch("orders", 1, None, Some(&claim))
            .await
            .is_err()
    );

    // A result row that does not decode makes that version `unknown`; the
    // template's state still reads.
    {
        use sqlx::Connection as _;
        let mut conn = sqlx::SqliteConnection::connect(&format!(
            "sqlite:{}",
            dir.path().join("registry.db").display()
        ))
        .await
        .unwrap();
        sqlx::query(
            "UPDATE faucet_template_test_results SET body='{not json' WHERE id='orders' AND version='1'",
        )
        .execute(&mut conn)
        .await
        .unwrap();
    }
    let st = faucet_cli::templates::template_state(&s, "orders")
        .await
        .unwrap();
    assert_eq!(st.test_status.get(&1), Some(&TestStatus::Unknown));
    assert_eq!(st.stable, Some(2));
    assert!(
        faucet_cli::templates::launch(&s, "orders", VersionSelector::Pinned(1), None)
            .await
            .is_err()
    );
    // A result for a version that no longer exists is refused.
    s.template_delete("orders", Some(1)).await.unwrap();
    let mut orphan = first.clone();
    orphan.result_id = "orphan".into();
    assert!(
        s.template_record_test(&orphan)
            .await
            .unwrap_err()
            .to_string()
            .contains("no longer exists")
    );

    // Server settings.
    assert_eq!(s.meta_get("k").await.unwrap(), None);
    s.meta_put("k", "1.13").await.unwrap();
    s.meta_put("k", "1.14").await.unwrap();
    assert_eq!(s.meta_get("k").await.unwrap().as_deref(), Some("1.14"));

    let v = faucet_cli::templates::bundle::version_tests(&s, "orders", 2, 3)
        .await
        .unwrap();
    assert_eq!(v.status.status, TestStatus::Passing);

    // Retention keeps the newest RESULTS_RETAIN per version.
    for i in 0..(faucet_cli::serve::history::templates::RESULTS_RETAIN + 3) {
        let mut t: TemplateTestResult = first.clone();
        t.version = 2;
        t.result_id = format!("r-{i}");
        t.recorded_at = first.recorded_at + chrono::Duration::seconds(i as i64 + 1);
        s.template_record_test(&t).await.unwrap();
    }
    assert_eq!(
        s.template_test_results("orders", Some(2), 100)
            .await
            .unwrap()
            .len(),
        faucet_cli::serve::history::templates::RESULTS_RETAIN
    );

    s.template_delete("orders", Some(1)).await.unwrap();
    assert!(
        s.template_test_results("orders", Some(1), 5)
            .await
            .unwrap()
            .is_empty()
    );
    s.template_delete("orders", None).await.unwrap();
    assert!(
        s.template_test_results("orders", None, 5)
            .await
            .unwrap()
            .is_empty()
    );
}

/// A deployment whose `tests:` block names only its companions runs the
/// structural case against a SQLite registry: compose with the registered
/// source and sink, then validate and expand the result.
#[tokio::test]
async fn a_deployment_bundle_without_cases_runs_the_structural_suite() {
    let dir = tempfile::tempdir().unwrap();
    let s = sqlite(dir.path()).await;
    faucet_cli::templates::register(
        &s,
        req("kind: sink-template\nname: files\ndescription: files\nsink: { type: file, config: {} }\nper_stream: { path: \"./out/${stream}.jsonl\" }\n".into()),
    )
    .await
    .unwrap();
    faucet_cli::templates::register(
        &s,
        req("kind: source-template\nname: acme\ndescription: acme\nsource:\n  type: rest\n  config: { base_url: \"https://example.com\" }\nstreams:\n  - name: orders\n    source: { config: { path: /orders } }\n".into()),
    )
    .await
    .unwrap();
    let reg = faucet_cli::templates::register_tested(
        &s,
        req("kind: deployment\nname: ops\nstate: { type: memory }\ntests:\n  source: acme\n  source_select: newest\n  sink: files\n  sink_select: newest\n".into()),
    )
    .await
    .unwrap();
    let t = reg.tests.expect("the structural bundle ran");
    assert!(t.passed, "{t:#?}");
    assert!(!t.cases.is_empty(), "{t:#?}");
    assert_eq!(
        t.provenance.companions,
        vec!["acme@1".to_string(), "files@1".to_string()]
    );
}

#[tokio::test]
async fn sync_registers_failing_versions_without_launching_them() {
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
    let out = apply::apply(
        &s,
        plan::SyncPlan {
            origin: "gh".into(),
            actions: vec![
                register("bad", pipeline("bad", 9)),
                register("good", pipeline("good", 2)),
            ],
        },
        "sync:gh",
    )
    .await;
    assert_eq!(out.registered.len(), 2, "{out:?}");
    assert_eq!(out.failed.len(), 1, "{out:?}");
    assert!(
        out.failed[0].error.contains("needs a passing version"),
        "{out:?}"
    );
    assert_eq!(s.template_state("good").await.unwrap().stable, Some(1));
    assert_eq!(s.template_state("bad").await.unwrap().stable, None);

    // A later pull of the same body is not a new version, even though the
    // stored one now carries results.
    let again = apply::apply(
        &s,
        plan::SyncPlan {
            origin: "gh".into(),
            actions: vec![plan::SyncAction::Launch {
                id: "bad".into(),
                version: 1,
            }],
        },
        "sync:gh",
    )
    .await;
    assert_eq!(again.failed.len(), 1);
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
    faucet_cli::commands::hub::run(lint(Vec::new()))
        .await
        .unwrap();
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
    let err = faucet_cli::commands::hub::run(lint(Vec::new()))
        .await
        .unwrap_err();
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
    let s = faucet_cli::templates::resolve_store_url(&store_url)
        .await
        .unwrap();
    faucet_cli::templates::register(&s, req(pipeline("orders", 2)))
        .await
        .unwrap();
    faucet_cli::templates::register(&s, req(pipeline("orders", 7)))
        .await
        .unwrap();
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
    assert!(matches!(
        err,
        faucet_cli::error::CliError::TestsFailed { .. }
    ));
    assert_eq!(
        s.template_test_results("orders", None, 10)
            .await
            .unwrap()
            .len(),
        4,
        "two on register, two from the command"
    );
    faucet_cli::commands::template::run(args("orders@1", true, Some("auto:*")))
        .await
        .unwrap();
    assert_eq!(
        s.template_test_results("orders", None, 10)
            .await
            .unwrap()
            .len(),
        4
    );
    let err = faucet_cli::commands::template::run(args("orders@1", false, Some("auto:*")))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("--no-record"), "{err}");

    // Revalidation re-tests label holders only, and records the release line.
    faucet_cli::templates::launch(&s, "orders", VersionSelector::Pinned(1), None)
        .await
        .unwrap();
    let before = s
        .template_test_results("orders", None, 50)
        .await
        .unwrap()
        .len();
    faucet_cli::commands::template::run(TemplateArgs {
        command: TemplateCommand::Revalidate(faucet_cli::cli::TemplateRevalidateArgs {
            common: faucet_cli::cli::TemplateStoreArgs {
                store: store_url.clone(),
                json: true,
                env_file: None,
                no_env_file: true,
            },
        }),
    })
    .await
    .unwrap();
    assert_eq!(
        s.template_test_results("orders", None, 50)
            .await
            .unwrap()
            .len(),
        before + 1
    );
    assert!(
        s.meta_get(faucet_cli::templates::revalidate::META_KEY)
            .await
            .unwrap()
            .is_some()
    );

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

    // A hub kind reads its companions and shared suites from the hub it lives in.
    let src = write(dir.path(), "source-templates/acme.yaml", HUB_SOURCE);
    write(dir.path(), "sink-templates/files.yaml", HUB_SINK);
    write(dir.path(), "test-suites/rest-conformance.yaml", HUB_SUITE);
    faucet_cli::commands::template::run(args(src.to_str().unwrap(), false, None))
        .await
        .unwrap();
    let plain = write(
        dir.path(),
        "plain.yaml",
        "kind: pipeline\nversion: 1\npipeline: {}\n",
    );
    let err = faucet_cli::commands::template::run(args(plain.to_str().unwrap(), false, None))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("template test suite"), "{err}");
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
    serve_with_history(dir, require_approval, None).await
}

async fn serve_with_history(
    dir: &std::path::Path,
    require_approval: Vec<String>,
    history: Option<String>,
) -> String {
    let port = free_port();
    let auth = dir.join(format!("auth-{port}.yaml"));
    std::fs::write(&auth, AUTH).unwrap();
    let args =
        faucet_cli::cli::ServeArgs {
            listen: format!("127.0.0.1:{port}"),
            auth_token: None,
            auth_config: Some(auth),
            read_token: None,
            write_token: None,
            admin_token: None,
            no_auth: false,
            allow_unauthenticated_network: false,
            max_concurrent_runs: Some(2),
            max_queued_runs: Some(8),
            default_config: None,
            history: Some(history.unwrap_or_else(|| {
                format!("sqlite:{}", dir.join(format!("h-{port}.db")).display())
            })),
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

async fn call(
    base: &str,
    method: &str,
    token: &str,
    path: &str,
    body: Option<Value>,
) -> (u16, Value) {
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
    (
        code,
        serde_json::from_str(&text).unwrap_or(Value::String(text)),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn http_labels_need_passing_versions_and_runs_follow_labels() {
    let dir = tempfile::tempdir().unwrap();
    let base = serve(dir.path(), Vec::new()).await;
    let b = base.as_str();

    let (code, r) = call(
        b,
        "POST",
        "admin-tok",
        "/v1/templates",
        Some(json!({ "config": pipeline("orders", 9) })),
    )
    .await;
    assert_eq!(code, 201, "{r}");
    assert_eq!(
        r["tests"]["passed"], false,
        "registered, and its result recorded: {r}"
    );
    assert!(r["tests"]["instance"].is_string(), "server provenance: {r}");
    assert_eq!(r["tests"]["schema"], 1);

    // A register whose launch is refused still audits the registration.
    let (code, r) = call(
        b,
        "POST",
        "admin-tok",
        "/v1/templates",
        Some(json!({ "config": pipeline("refused", 9), "launch": true })),
    )
    .await;
    assert_eq!(code, 422, "{r}");
    let (_, audit) = call(
        b,
        "GET",
        "admin-tok",
        "/v1/audit?action=template.register",
        None,
    )
    .await;
    assert!(
        audit["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["target"] == "template:refused@1" && e["result"] == "ok"),
        "{audit}"
    );
    let (_, audit) = call(
        b,
        "GET",
        "admin-tok",
        "/v1/audit?action=template.launch",
        None,
    )
    .await;
    assert!(
        audit["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["target"] == "template:refused@1" && e["result"] == "refused"),
        "{audit}"
    );
    let (code, r) = call(
        b,
        "POST",
        "admin-tok",
        "/v1/templates/orders/tags",
        Some(json!({ "tag": "prod", "version": 1 })),
    )
    .await;
    assert_eq!(code, 422, "{r}");
    let (_, audit) = call(
        b,
        "GET",
        "admin-tok",
        "/v1/audit?action=template.promote",
        None,
    )
    .await;
    assert!(
        audit["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["target"] == "template:orders@1" && e["result"] == "refused"),
        "{audit}"
    );

    let (code, r) = call(
        b,
        "POST",
        "admin-tok",
        "/v1/templates/orders/launch",
        Some(json!({ "version": 1 })),
    )
    .await;
    assert_eq!(code, 422, "{r}");
    let failing = r["error"]["details"]["tests"]["failing"]
        .as_array()
        .unwrap();
    assert!(
        failing
            .iter()
            .any(|f| f.as_str().unwrap().starts_with("rows:")),
        "{r}"
    );
    assert_eq!(r["error"]["details"]["tests"]["status"], "failing");
    let (code, r) = call(
        b,
        "POST",
        "admin-tok",
        "/v1/templates/orders/tags",
        Some(json!({ "tag": "dev", "version": 1 })),
    )
    .await;
    assert_eq!(
        code, 422,
        "every label needs a passing version, dev too: {r}"
    );

    // A draft runs any version, pinned.
    let run = |v: Value, extra: Value| {
        let mut body = json!({ "version": v, "params": {} });
        body.as_object_mut()
            .unwrap()
            .extend(extra.as_object().cloned().unwrap_or_default());
        body
    };
    let (code, r) = call(
        b,
        "POST",
        "op-tok",
        "/v1/templates/orders/runs",
        Some(run(json!(1), json!({}))),
    )
    .await;
    assert_eq!(code, 202, "a draft runs any version: {r}");

    let (code, r) = call(
        b,
        "POST",
        "admin-tok",
        "/v1/templates",
        Some(json!({ "config": pipeline("orders", 2), "launch": true })),
    )
    .await;
    assert_eq!(code, 201, "{r}");
    assert_eq!(r["tests"]["passed"], true);
    let (_, d) = call(
        b,
        "GET",
        "viewer-tok",
        "/v1/templates/orders?version=newest",
        None,
    )
    .await;
    let evidence = d["launches"][0]["evidence"].as_str().unwrap().to_string();
    assert_eq!(d["evidence"]["stable"], evidence.as_str(), "{d}");
    assert_eq!(d["test_status"]["1"], "failing", "{d}");
    assert_eq!(d["test_status"]["2"], "passing", "{d}");
    assert_eq!(d["tests"]["versions"].as_array().unwrap().len(), 2);

    // Launched: the failing, unlabelled v1 needs `allow_untested`, which
    // labels and audits the run; the stable version runs plainly.
    let (code, r) = call(
        b,
        "POST",
        "op-tok",
        "/v1/templates/orders/runs",
        Some(run(json!(1), json!({}))),
    )
    .await;
    assert_eq!(code, 422, "{r}");
    assert!(r.to_string().contains("allow_untested"), "{r}");
    let (code, r) = call(
        b,
        "POST",
        "op-tok",
        "/v1/templates/orders/runs",
        Some(run(json!(1), json!({ "allow_untested": true }))),
    )
    .await;
    assert_eq!(code, 202, "{r}");
    let run_id = r["run_id"].as_str().unwrap().to_string();
    let (_, rec) = call(b, "GET", "op-tok", &format!("/v1/runs/{run_id}"), None).await;
    assert_eq!(rec["labels"]["allow_untested"], "true", "{rec}");
    let (_, audit) = call(b, "GET", "admin-tok", "/v1/audit?action=template.run", None).await;
    assert!(
        audit["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["detail"] == "allow_untested: v1 is failing"),
        "{audit}"
    );
    let (code, r) = call(
        b,
        "POST",
        "op-tok",
        "/v1/templates/orders/runs",
        Some(json!({ "params": {} })),
    )
    .await;
    assert_eq!(code, 202, "{r}");

    let (_, audit) = call(
        b,
        "GET",
        "admin-tok",
        "/v1/audit?action=template.launch",
        None,
    )
    .await;
    assert!(
        audit["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["result"] == "refused" && e["target"] == "template:orders@1"),
        "{audit}"
    );

    let (code, r) = call(
        b,
        "GET",
        "viewer-tok",
        "/v1/templates/orders/versions/1/tests",
        None,
    )
    .await;
    assert_eq!(code, 200, "{r}");
    assert_eq!(r["status"], "failing");
    let (code, _) = call(
        b,
        "POST",
        "op-tok",
        "/v1/templates/orders/versions/2/test",
        None,
    )
    .await;
    assert_eq!(code, 403);
    let (code, r) = call(
        b,
        "POST",
        "admin-tok",
        "/v1/templates/orders/versions/2/test",
        None,
    )
    .await;
    assert_eq!(code, 200, "{r}");
    assert_eq!(r["passed"], true);

    // Rollback: the previous version must pass too.
    let (code, r) = call(
        b,
        "POST",
        "admin-tok",
        "/v1/templates",
        Some(json!({ "config": pipeline("orders", 2).replace("rows", "rows-v3"), "launch": true })),
    )
    .await;
    assert_eq!(code, 201, "{r}");
    let (code, r) = call(
        b,
        "POST",
        "admin-tok",
        "/v1/templates/orders/rollback",
        None,
    )
    .await;
    assert_eq!(code, 200, "{r}");
    assert_eq!(r["version"], 2);

    // A version registered without tests inherits them, copied in.
    let bare = pipeline("orders", 2);
    let bare = bare[..bare.find("tests:").unwrap()].to_string();
    let (code, r) = call(
        b,
        "POST",
        "admin-tok",
        "/v1/templates",
        Some(json!({ "config": bare })),
    )
    .await;
    assert_eq!(code, 201, "{r}");
    assert_eq!(r["tests_inherited_from"], 3, "{r}");

    // MCP: the same rules, and `allow_untested` on run_template.
    let mcp = |id: u32, name: &str, args: Value| json!({ "jsonrpc": "2.0", "id": id, "method": "tools/call", "params": { "name": name, "arguments": args } });
    let (_, r) = call(
        b,
        "POST",
        "admin-tok",
        "/mcp",
        Some(mcp(
            1,
            "launch_template",
            json!({ "id": "orders", "version": 1 }),
        )),
    )
    .await;
    assert_eq!(r["result"]["isError"], true, "{r}");
    assert!(r.to_string().contains("needs a passing version"), "{r}");
    let (_, r) = call(
        b,
        "POST",
        "admin-tok",
        "/mcp",
        Some(mcp(
            2,
            "run_template",
            json!({ "id": "orders", "version": 1, "params": {} }),
        )),
    )
    .await;
    assert_eq!(r["result"]["isError"], true, "{r}");
    let (_, r) = call(
        b,
        "POST",
        "admin-tok",
        "/mcp",
        Some(mcp(
            3,
            "run_template",
            json!({ "id": "orders", "version": 1, "params": {}, "allow_untested": true }),
        )),
    )
    .await;
    assert_eq!(r["result"]["isError"], false, "{r}");
    let (_, r) = call(
        b,
        "POST",
        "admin-tok",
        "/mcp",
        Some(mcp(
            4,
            "test_template",
            json!({ "id": "orders", "version": 2 }),
        )),
    )
    .await;
    assert_eq!(r["result"]["isError"], false, "{r}");

    // A server-side test run never reads the server's own credentials.
    let (code, r) = call(
        b,
        "POST",
        "admin-tok",
        "/v1/templates",
        Some(json!({ "config": pipeline("leaky", 2).replace("./out.jsonl", "${env:FAUCET_VAULT_KEY}") })),
    )
    .await;
    assert_eq!(code, 201, "{r}");
    assert_eq!(r["tests"]["passed"], false, "{r}");
    assert!(r.to_string().contains("refused by the server"), "{r}");
}

#[tokio::test(flavor = "multi_thread")]
async fn approved_launch_change_is_checked_when_it_executes() {
    let dir = tempfile::tempdir().unwrap();
    let base = serve(dir.path(), vec!["template_launch".into()]).await;
    let b = base.as_str();
    let (code, r) = call(
        b,
        "POST",
        "admin-tok",
        "/v1/templates",
        Some(json!({ "config": pipeline("orders", 9) })),
    )
    .await;
    assert_eq!(code, 201, "{r}");

    let propose =
        |payload: Value| json!({ "kind": "template_launch", "payload": payload, "reason": "ship" });
    let (code, c) = call(
        b,
        "POST",
        "admin-tok",
        "/v1/changes",
        Some(propose(json!({ "id": "orders", "version": 1 }))),
    )
    .await;
    assert_eq!(code, 201, "{c}");
    assert_eq!(c["plan"]["summary"]["tests"]["status"], "failing", "{c}");
    let id = c["id"].as_str().unwrap().to_string();
    let (_, done) = call(
        b,
        "POST",
        "erin-tok",
        &format!("/v1/changes/{id}/approve"),
        Some(json!({})),
    )
    .await;
    assert_eq!(done["status"], "failed", "{done}");
    assert!(
        done["error"]
            .as_str()
            .unwrap_or_default()
            .contains("needs a passing version"),
        "{done}"
    );

    let (code, r) = call(
        b,
        "POST",
        "admin-tok",
        "/v1/templates",
        Some(json!({ "config": pipeline("orders", 2) })),
    )
    .await;
    assert_eq!(code, 201, "{r}");
    let (_, c) = call(
        b,
        "POST",
        "admin-tok",
        "/v1/changes",
        Some(propose(json!({ "id": "orders", "version": 2 }))),
    )
    .await;
    let id = c["id"].as_str().unwrap().to_string();
    let (_, done) = call(
        b,
        "POST",
        "erin-tok",
        &format!("/v1/changes/{id}/approve"),
        Some(json!({})),
    )
    .await;
    assert_eq!(done["status"], "executed", "{done}");
    let (_, d) = call(
        b,
        "GET",
        "viewer-tok",
        "/v1/templates/orders?version=newest",
        None,
    )
    .await;
    assert!(d["launches"][0]["evidence"].is_string(), "{d}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_server_on_a_new_release_line_revalidates_label_holders() {
    let dir = tempfile::tempdir().unwrap();
    let port_db = dir.path().join("pre.db");
    let url = format!("sqlite:{}", port_db.display());
    let s = faucet_cli::templates::resolve_store_url(&url)
        .await
        .unwrap();
    let mut r = req(pipeline("orders", 2));
    r.launch = true;
    faucet_cli::templates::register_tested(&s, r).await.unwrap();
    s.meta_put(faucet_cli::templates::revalidate::META_KEY, "0.0")
        .await
        .unwrap();
    let before = s
        .template_test_results("orders", None, 50)
        .await
        .unwrap()
        .len();
    drop(s);
    let base = serve_with_history(dir.path(), Vec::new(), Some(url.clone())).await;
    let s = faucet_cli::templates::resolve_store_url(&url)
        .await
        .unwrap();
    for _ in 0..400 {
        if s.template_test_results("orders", None, 50)
            .await
            .unwrap()
            .len()
            > before
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let after = s.template_test_results("orders", None, 50).await.unwrap();
    assert_eq!(after.len(), before + 1, "stable re-tested once");
    assert_eq!(after[0].recorded_by.as_deref(), Some("revalidate"));
    assert!(after[0].provenance.instance.is_some());
    assert_eq!(s.template_state("orders").await.unwrap().stable, Some(1));
    let _ = base;
}
