//! Topology mode across the runtimes and the run-level blocks (#789 CLI-16,
//! CLI-17, CLI-18): budgets, metadata columns and reconciliation apply per sink
//! node; `verify:` / `rollback:` are refused; serve, schedule and the library
//! entry run the node graph instead of a synthetic default row; a failed node is
//! reported on every sink it feeds.
#![cfg(all(feature = "source-csv", feature = "sink-jsonl", feature = "transforms"))]

use assert_cmd::Command;
use faucet_cli::auth_catalog::build_auth_catalog;
use faucet_cli::config::PipelineConfig;
use faucet_cli::error::CliError;
use faucet_cli::topology::{TopologyRunOptions, run_topology, validate_topology_spec};
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

fn write(path: &Path, body: &str) {
    fs::write(path, body).unwrap();
}

fn orders_csv(dir: &Path) -> PathBuf {
    let p = dir.join("orders.csv");
    write(
        &p,
        "order_id,country_code,amount\n1,US,10\n2,US,5\n3,IN,7\n4,DE,3\n",
    );
    p
}

fn parse(yaml: &str) -> PipelineConfig {
    PipelineConfig::from_text(yaml, Path::new("test.yaml")).expect("parses")
}

fn lines(path: &Path) -> Vec<serde_json::Value> {
    fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

/// A source tee'd to two JSONL sinks, with `extra` spliced in at the top level.
fn tee_yaml(dir: &Path, extra: &str) -> (String, PathBuf, PathBuf) {
    let csv = orders_csv(dir);
    let a = dir.join("a.jsonl");
    let b = dir.join("b.jsonl");
    let yaml = format!(
        r#"version: 1
name: graph
{extra}
pipeline:
  sources:
    orders: {{ type: csv, config: {{ path: {csv} }} }}
  sinks:
    a: {{ type: jsonl, config: {{ path: {a} }} }}
    b: {{ type: jsonl, config: {{ path: {b} }} }}
  nodes:
    src: {{ kind: source, ref: orders }}
    fan: {{ kind: tee, fanout: 2 }}
    wa: {{ kind: sink, ref: a }}
    wb: {{ kind: sink, ref: b }}
  edges:
    - {{ from: src, to: fan }}
    - {{ from: fan, to: wa }}
    - {{ from: fan, to: wb }}
"#,
        csv = csv.display(),
        a = a.display(),
        b = b.display()
    );
    (yaml, a, b)
}

// ── CLI-16: run-level blocks ─────────────────────────────────────────────────

#[tokio::test]
async fn a_record_budget_stops_every_sink_node() {
    let dir = TempDir::new().unwrap();
    let (yaml, _, _) = tee_yaml(dir.path(), "budget: { max_records: 2 }");
    let cfg = parse(&yaml);
    let auth = build_auth_catalog(None).unwrap();
    let summary = run_topology(
        &cfg,
        &auth,
        TopologyRunOptions {
            budget: cfg.budget.clone(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let errors: Vec<&str> = summary
        .invocations
        .iter()
        .filter_map(|i| i.error.as_deref())
        .collect();
    assert!(
        errors.iter().any(|e| e.contains("max_records")),
        "the crossing page is refused: {errors:?}"
    );
}

#[tokio::test]
async fn a_budget_refuses_a_sink_node_it_does_not_allow() {
    let dir = TempDir::new().unwrap();
    let (yaml, a, _) = tee_yaml(dir.path(), "budget: { allowed_sinks: [a] }");
    let cfg = parse(&yaml);
    let auth = build_auth_catalog(None).unwrap();
    let err = run_topology(
        &cfg,
        &auth,
        TopologyRunOptions {
            budget: cfg.budget.clone(),
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, CliError::BudgetSinkNotAllowed { ref row, .. } if row == "wb"),
        "{err}"
    );
    assert!(!a.exists(), "nothing ran");
}

#[tokio::test]
async fn metadata_columns_are_stamped_on_every_sink_node() {
    let dir = TempDir::new().unwrap();
    let (yaml, a, b) = tee_yaml(dir.path(), "metadata_columns: {}");
    let cfg = parse(&yaml);
    let auth = build_auth_catalog(None).unwrap();
    run_topology(
        &cfg,
        &auth,
        TopologyRunOptions {
            run_id: Some("run-7".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    for out in [a, b] {
        let rows = lines(&out);
        assert_eq!(rows.len(), 4);
        assert!(
            rows.iter().all(|r| r["_faucet_run_id"] == "run-7"),
            "{rows:?}"
        );
        assert!(
            rows.iter().all(|r| r["_faucet_source"] == "csv"),
            "{rows:?}"
        );
    }
}

#[tokio::test]
async fn a_reconcile_shortfall_fails_each_sink_node() {
    let dir = TempDir::new().unwrap();
    let count = dir.path().join("count.csv");
    write(&count, "n\n100\n");
    let (yaml, _, _) = tee_yaml(
        dir.path(),
        &format!(
            "reconcile: {{ count: {{ type: csv, config: {{ path: {} }}, count_field: n }} }}",
            count.display()
        ),
    );
    let cfg = parse(&yaml);
    let auth = build_auth_catalog(None).unwrap();
    let summary = run_topology(&cfg, &auth, TopologyRunOptions::default())
        .await
        .unwrap();
    for node in ["wa", "wb"] {
        let row = summary
            .invocations
            .iter()
            .find(|i| i.row_id == node)
            .unwrap();
        assert!(
            row.error.as_deref().unwrap_or_default().contains("100"),
            "{node}: {:?}",
            row.error
        );
    }
}

#[tokio::test]
async fn a_complete_reconcile_keeps_the_run_green() {
    let dir = TempDir::new().unwrap();
    let count = dir.path().join("count.csv");
    write(&count, "n\n4\n");
    let (yaml, _, _) = tee_yaml(
        dir.path(),
        &format!(
            "reconcile: {{ count: {{ type: csv, config: {{ path: {} }}, count_field: n }} }}",
            count.display()
        ),
    );
    let cfg = parse(&yaml);
    let auth = build_auth_catalog(None).unwrap();
    let summary = run_topology(&cfg, &auth, TopologyRunOptions::default())
        .await
        .unwrap();
    assert!(!summary.had_failures(), "{:?}", summary.invocations);
}

#[test]
fn verify_and_rollback_are_refused_in_topology_mode() {
    let dir = TempDir::new().unwrap();
    let (yaml, _, _) = tee_yaml(dir.path(), "verify: { key: [order_id] }");
    let err = validate_topology_spec(&parse(&yaml))
        .unwrap_err()
        .to_string();
    assert!(err.contains("`verify:` is not supported"), "{err}");

    let (yaml, _, _) = tee_yaml(dir.path(), "rollback: {}");
    let err = validate_topology_spec(&parse(&yaml))
        .unwrap_err()
        .to_string();
    assert!(err.contains("`rollback:` is not supported"), "{err}");

    let (yaml, _, _) = tee_yaml(dir.path(), "rollback: { enabled: false }");
    validate_topology_spec(&parse(&yaml)).expect("a disabled rollback block is inert");
}

#[test]
fn a_usage_block_is_reported_as_not_applied() {
    let dir = TempDir::new().unwrap();
    let (yaml, _, _) = tee_yaml(dir.path(), "usage: {}");
    let cfg = dir.path().join("faucet.yaml");
    write(&cfg, &yaml);
    let out = Command::cargo_bin("faucet")
        .unwrap()
        .args(["validate", "--json", cfg.to_str().unwrap()])
        .assert()
        .success();
    let stdout = String::from_utf8_lossy(&out.get_output().stdout).to_string();
    assert!(stdout.contains("\"block\": \"usage\""), "{stdout}");
}

// ── CLI-18: failures reach the sinks they feed ───────────────────────────────

fn broken_source_yaml(dir: &Path, extra: &str) -> (String, PathBuf) {
    let missing = dir.join("missing.csv");
    let out = dir.join("o.jsonl");
    let yaml = format!(
        r#"version: 1
name: broken
{extra}
pipeline:
  sources:
    o: {{ type: csv, config: {{ path: {missing} }} }}
  sinks:
    out: {{ type: jsonl, config: {{ path: {out} }} }}
  nodes:
    s: {{ kind: source, ref: o }}
    w: {{ kind: sink, ref: out }}
  edges:
    - {{ from: s, to: w }}
"#,
        missing = missing.display(),
        out = out.display()
    );
    (yaml, out)
}

#[tokio::test]
async fn a_failed_source_fails_the_sink_it_feeds() {
    let dir = TempDir::new().unwrap();
    let (yaml, _) = broken_source_yaml(dir.path(), "");
    let auth = build_auth_catalog(None).unwrap();
    let summary = run_topology(&parse(&yaml), &auth, TopologyRunOptions::default())
        .await
        .unwrap();
    let sink = summary
        .invocations
        .iter()
        .find(|i| i.row_id == "w")
        .unwrap();
    assert!(
        sink.error
            .as_deref()
            .unwrap_or_default()
            .contains("upstream node 's' failed"),
        "{:?}",
        sink.error
    );
}

#[cfg(feature = "lineage")]
#[tokio::test]
async fn a_stop_on_error_failure_is_still_reported_per_sink_node() {
    let dir = TempDir::new().unwrap();
    let lineage = dir.path().join("lineage.jsonl");
    let (yaml, _) = broken_source_yaml(
        dir.path(),
        &format!(
            "execution: {{ on_error: stop }}\nlineage:\n  namespace: t\n  transport: {{ type: file, config: {{ path: {} }} }}",
            lineage.display()
        ),
    );
    let auth = build_auth_catalog(None).unwrap();
    let err = run_topology(&parse(&yaml), &auth, TopologyRunOptions::default()).await;
    assert!(err.is_err(), "on_error: stop surfaces the failure");
    let events = fs::read_to_string(&lineage).unwrap_or_default();
    assert!(
        events.contains("\"eventType\":\"FAIL\""),
        "the sink node's terminal event is FAIL: {events}"
    );
}

// ── CLI-17: every runtime runs the graph or refuses it ───────────────────────

#[test]
fn expand_refuses_a_topology_config() {
    let dir = TempDir::new().unwrap();
    let (yaml, _, _) = tee_yaml(dir.path(), "");
    let err = faucet_cli::expand::expand(&parse(&yaml)).unwrap_err();
    assert!(matches!(err, CliError::TopologyNotSupported), "{err}");
}

#[test]
fn doctor_refuses_a_topology_config_instead_of_probing_a_default_row() {
    let dir = TempDir::new().unwrap();
    let (yaml, _, _) = tee_yaml(dir.path(), "");
    let cfg = dir.path().join("faucet.yaml");
    write(&cfg, &yaml);
    let out = Command::cargo_bin("faucet")
        .unwrap()
        .args(["doctor", cfg.to_str().unwrap()])
        .assert()
        .failure();
    let stderr = String::from_utf8_lossy(&out.get_output().stderr).to_string();
    assert!(stderr.contains("topology config"), "{stderr}");
}

#[tokio::test]
async fn the_library_entry_runs_the_graph() {
    let dir = TempDir::new().unwrap();
    let (yaml, a, b) = tee_yaml(dir.path(), "");
    let summary = faucet_cli::run_from_yaml_str(&yaml).await.unwrap();
    assert_eq!(summary.invocations.len(), 2);
    assert_eq!(lines(&a).len(), 4);
    assert_eq!(lines(&b).len(), 4);
}

#[tokio::test]
async fn the_library_entry_refuses_a_row_selection_on_a_graph() {
    let dir = TempDir::new().unwrap();
    let (yaml, a, _) = tee_yaml(dir.path(), "");
    let selection: faucet_cli::select::SelectionRequest =
        serde_json::from_value(serde_json::json!({ "select": ["wa"] })).unwrap();
    let err = faucet_cli::run_from_yaml_str_selected(&yaml, Some(&selection))
        .await
        .unwrap_err();
    assert!(matches!(err, CliError::Config(_)), "{err}");
    assert!(!a.exists());
}

#[cfg(feature = "schedule")]
#[test]
fn schedule_once_runs_the_graph() {
    let dir = TempDir::new().unwrap();
    let (yaml, a, b) = tee_yaml(dir.path(), "schedule: { cron: \"0 0 * * *\" }");
    let cfg = dir.path().join("faucet.yaml");
    write(&cfg, &yaml);
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["schedule", "--once", cfg.to_str().unwrap()])
        .assert()
        .success();
    assert_eq!(lines(&a).len(), 4, "the tee's first sink");
    assert_eq!(lines(&b).len(), 4, "the tee's second sink");
}

#[cfg(feature = "serve")]
mod serve {
    use super::*;
    use faucet_cli::cli::ServeArgs;
    use faucet_cli::serve::ServeConfig;
    use std::time::Duration;

    fn free_port() -> u16 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    }

    async fn spawn_server(port: u16) {
        let args = ServeArgs {
            listen: format!("127.0.0.1:{port}"),
            auth_token: None,
            auth_config: None,
            read_token: None,
            write_token: None,
            admin_token: None,
            no_auth: true,
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
            mcp: false,
            mcp_allow_mutations: false,
            require_approval: Vec::new(),
            approval_expiry_secs: 86_400,
            require_template_tests: false,
            vault_key: None,
            vault_previous_key: Vec::new(),
            connect_providers: None,
            allow_subprocess_connectors: false,
        };
        let mut config = ServeConfig::from_args(args).unwrap();
        config.log_level = "warn".into();
        tokio::spawn(async move {
            let _ = faucet_cli::serve::run_server(config, Default::default()).await;
        });
        let client = reqwest::Client::new();
        for _ in 0..1200 {
            if client
                .get(format!("http://127.0.0.1:{port}/healthz"))
                .send()
                .await
                .map(|r| r.status().is_success())
                .unwrap_or(false)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("server did not become healthy on port {port}");
    }

    async fn wait_terminal(
        client: &reqwest::Client,
        base: &str,
        run_id: &str,
    ) -> serde_json::Value {
        for _ in 0..800 {
            let rec: serde_json::Value = client
                .get(format!("{base}/v1/runs/{run_id}"))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            if matches!(
                rec["status"].as_str(),
                Some("completed" | "failed" | "cancelled")
            ) {
                return rec;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("run {run_id} never finished");
    }

    #[tokio::test]
    async fn a_submitted_topology_runs_as_its_graph() {
        let dir = TempDir::new().unwrap();
        let (yaml, a, b) = tee_yaml(dir.path(), "");
        let port = free_port();
        spawn_server(port).await;
        let base = format!("http://127.0.0.1:{port}");
        let client = reqwest::Client::new();
        let resp: serde_json::Value = client
            .post(format!("{base}/v1/runs"))
            .json(&serde_json::json!({ "config": yaml }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let run_id = resp["run_id"].as_str().expect("accepted").to_string();
        let rec = wait_terminal(&client, &base, &run_id).await;
        assert_eq!(rec["status"], "completed", "{rec}");
        assert_eq!(lines(&a).len(), 4);
        assert_eq!(lines(&b).len(), 4);
    }

    #[tokio::test]
    async fn plan_and_doctor_first_refuse_a_topology_explicitly() {
        let dir = TempDir::new().unwrap();
        let (yaml, a, _) = tee_yaml(dir.path(), "");
        let port = free_port();
        spawn_server(port).await;
        let base = format!("http://127.0.0.1:{port}");
        let client = reqwest::Client::new();

        let plan = client
            .post(format!("{base}/v1/plan"))
            .json(&serde_json::json!({ "config": yaml }))
            .send()
            .await
            .unwrap();
        assert_eq!(plan.status(), 422);
        let body = plan.text().await.unwrap();
        assert!(body.contains("topology config"), "{body}");

        let doctor = client
            .post(format!("{base}/v1/runs"))
            .json(&serde_json::json!({ "config": yaml, "doctor_first": true }))
            .send()
            .await
            .unwrap();
        assert_eq!(doctor.status(), 422);
        assert!(!a.exists(), "nothing ran");
    }

    #[tokio::test]
    async fn backfill_rollback_and_change_requests_refuse_a_topology() {
        let dir = TempDir::new().unwrap();
        let (yaml, a, _) = tee_yaml(dir.path(), "");
        let port = free_port();
        spawn_server(port).await;
        let base = format!("http://127.0.0.1:{port}");
        let client = reqwest::Client::new();

        let backfill = client
            .post(format!("{base}/v1/backfill"))
            .json(&serde_json::json!({
                "config": yaml, "from": "2026-06-01", "to": "2026-06-03", "window": "1d"
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(backfill.status(), 422);
        assert!(backfill.text().await.unwrap().contains("topology config"));

        let change = client
            .post(format!("{base}/v1/changes"))
            .json(&serde_json::json!({ "kind": "run", "payload": { "config": yaml } }))
            .send()
            .await
            .unwrap();
        assert_eq!(change.status(), 422);
        assert!(change.text().await.unwrap().contains("topology config"));
        assert!(!a.exists(), "nothing ran");

        let run: serde_json::Value = client
            .post(format!("{base}/v1/runs"))
            .json(&serde_json::json!({ "config": yaml }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let run_id = run["run_id"].as_str().unwrap().to_string();
        wait_terminal(&client, &base, &run_id).await;
        let rollback = client
            .post(format!("{base}/v1/runs/{run_id}/rollback"))
            .json(&serde_json::json!({ "invocation_id": "x", "config": yaml }))
            .send()
            .await
            .unwrap();
        assert_eq!(rollback.status(), 422);
        assert!(rollback.text().await.unwrap().contains("topology config"));
    }
}

// ── MSG-42: run leases per sink node ─────────────────────────────────────────

/// A live lease on one sink node refuses the whole graph, and the leases the
/// run already took on the other sink nodes are released, not left to expire.
#[tokio::test(flavor = "multi_thread")]
async fn a_held_sink_node_lease_refuses_the_graph_and_releases_the_rest() {
    use faucet_cli::pipeline_state::lease;
    use faucet_core::{FileStateStore, StateStore};
    use std::sync::Arc;

    let dir = TempDir::new().unwrap();
    let state = dir.path().join("state");
    let (yaml, a, b) = tee_yaml(dir.path(), "");
    let yaml = yaml.replace(
        "  nodes:\n",
        &format!(
            "  state: {{ type: file, config: {{ path: {} }} }}\n  nodes:\n",
            state.display()
        ),
    );
    let cfg = parse(&yaml);
    let auth = build_auth_catalog(None).unwrap();
    let store: Arc<dyn StateStore> = Arc::new(FileStateStore::new(&state));
    // Sink nodes are leased in id order: `wa` is taken before `wb` refuses.
    let other = lease::acquire(Arc::clone(&store), "graph::wb", "other-run")
        .await
        .expect("holder lease");

    let err = run_topology(&cfg, &auth, TopologyRunOptions::default())
        .await
        .expect_err("a held lease refuses the run");
    let msg = err.to_string();
    assert!(
        msg.contains("other-run") && msg.contains("--force"),
        "{msg}"
    );
    assert!(
        lease::read(store.as_ref(), "graph::wa")
            .await
            .unwrap()
            .is_none(),
        "the lease taken on `wa` is released"
    );
    assert_eq!(
        lease::read(store.as_ref(), "graph::wb")
            .await
            .unwrap()
            .unwrap()
            .run_id,
        "other-run",
        "the holder's lease is left alone"
    );
    assert!(lines(&a).is_empty() && lines(&b).is_empty(), "nothing ran");

    let summary = run_topology(
        &cfg,
        &auth,
        TopologyRunOptions {
            force_lease: true,
            ..Default::default()
        },
    )
    .await
    .expect("--force takes the lease");
    assert!(!summary.had_failures(), "{summary:?}");
    assert_eq!(lines(&a).len(), 4);
    other.release().await;
}
