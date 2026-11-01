//! Integration test for `GET /v1/overview` (#1006): boots a real RBAC server on
//! the memory history and on SQLite, runs pipelines (one failing), and asserts
//! the exact aggregates, tenant scoping, permissions, the Grafana links and
//! the sections a server leaves out.
#![cfg(all(feature = "catalog", feature = "source-csv", feature = "sink-jsonl"))]
#![allow(clippy::disallowed_methods)] // tests drive time and raw clients directly

use serde_json::Value;
use std::time::Duration;

#[path = "support/ports.rs"]
mod ports;
use ports::free_port;

const AUTH_CONFIG: &str = "principals:\n\
    \x20 - name: alice\n\
    \x20   token: admin-tok\n\
    \x20   role: admin\n\
    \x20 - name: bob\n\
    \x20   token: viewer-tok\n\
    \x20   role: viewer\n\
    \x20 - name: carol\n\
    \x20   token: tenant-tok\n\
    \x20   role: viewer\n\
    \x20   tenant: acme\n";

fn serve_args(
    port: u16,
    auth_config: std::path::PathBuf,
    history: Option<String>,
    grafana_url: Option<String>,
) -> faucet_cli::cli::ServeArgs {
    faucet_cli::cli::ServeArgs {
        listen: format!("127.0.0.1:{port}"),
        auth_token: None,
        auth_config: Some(auth_config),
        read_token: None,
        write_token: None,
        admin_token: None,
        no_auth: false,
        allow_unauthenticated_network: false,
        max_concurrent_runs: Some(4),
        max_queued_runs: Some(16),
        default_config: None,
        history,
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
        no_ui: false,
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
        grafana_url,
        references: Default::default(),
        pools: Default::default(),
    }
}

async fn spawn_server(port: u16, dir: &std::path::Path, args: faucet_cli::cli::ServeArgs) {
    let mut config = faucet_cli::serve::ServeConfig::from_args(args).unwrap();
    config.log_level = "warn".into();
    let _ = dir;
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

async fn run_config(base: &str, client: &reqwest::Client, name: &str, config: &str) -> Value {
    let resp = client
        .post(format!("{base}/v1/runs"))
        .bearer_auth("admin-tok")
        .json(&serde_json::json!({ "config": config, "name": name }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        202,
        "{}",
        resp.text().await.unwrap()
    );
    let body: Value = resp.json().await.unwrap();
    let run_id = body["run_id"].as_str().unwrap().to_string();
    for _ in 0..400 {
        let rec: Value = client
            .get(format!("{base}/v1/runs/{run_id}"))
            .bearer_auth("admin-tok")
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        match rec["status"].as_str().unwrap() {
            "completed" | "failed" | "cancelled" => return rec,
            _ => tokio::time::sleep(Duration::from_millis(25)).await,
        }
    }
    panic!("run did not finish in time");
}

async fn get(client: &reqwest::Client, url: &str, token: &str) -> (u16, Value) {
    let resp = client.get(url).bearer_auth(token).send().await.unwrap();
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap();
    (
        status,
        serde_json::from_str(&text).unwrap_or(Value::String(text)),
    )
}

fn sum(series: &Value, key: &str) -> u64 {
    series
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b[key].as_u64().unwrap())
        .sum()
}

async fn exercise(history: Option<String>, grafana: Option<&str>) {
    let dir = tempfile::tempdir().unwrap();
    let auth_path = dir.path().join("auth.yaml");
    std::fs::write(&auth_path, AUTH_CONFIG).unwrap();
    let input = dir.path().join("in.csv");
    std::fs::write(&input, "id,name\n1,alice\n2,bob\n3,carol\n").unwrap();
    let port = free_port();
    spawn_server(
        port,
        dir.path(),
        serve_args(port, auth_path, history, grafana.map(String::from)),
    )
    .await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");
    let url = |q: &str| format!("{base}/v1/overview{q}");

    let unauth = client.get(url("")).send().await.unwrap();
    assert_eq!(unauth.status().as_u16(), 401);

    let (status, empty) = get(&client, &url("?since=24h"), "viewer-tok").await;
    assert_eq!(status, 200, "{empty}");
    assert_eq!(empty["runs"]["total"], 0);
    assert_eq!(empty["bucket"], "hour");
    let buckets = empty["runs"]["series"].as_array().unwrap().len();
    assert!(
        (24..=25).contains(&buckets),
        "{buckets} hourly buckets in a day"
    );
    assert!(empty.get("cost").is_none(), "{empty}");
    assert!(empty.get("freshness").is_none(), "{empty}");
    assert!(
        empty["attention"].as_object().unwrap().is_empty(),
        "{empty}"
    );

    for (i, name) in ["ok-a", "ok-a"].iter().enumerate() {
        let out = dir.path().join(format!("out-{i}.jsonl"));
        let config = format!(
            "version: 1\nname: overview-e2e\npipeline:\n  source: {{ type: csv, config: {{ path: {} }} }}\n  sink: {{ type: jsonl, config: {{ path: {} }} }}\n",
            input.display(),
            out.display()
        );
        let rec = run_config(&base, &client, name, &config).await;
        assert_eq!(rec["status"], "completed", "{rec}");
    }
    let missing = dir.path().join("missing.csv");
    let bad = format!(
        "version: 1\nname: overview-bad\npipeline:\n  source: {{ type: csv, config: {{ path: {} }} }}\n  sink: {{ type: jsonl, config: {{ path: {} }} }}\n",
        missing.display(),
        dir.path().join("bad.jsonl").display()
    );
    let rec = run_config(&base, &client, "bad-b", &bad).await;
    assert_eq!(rec["status"], "failed", "{rec}");

    let (status, o) = get(&client, &url("?since=24h"), "viewer-tok").await;
    assert_eq!(status, 200, "{o}");
    let runs = &o["runs"];
    assert_eq!(runs["total"], 3, "{o}");
    assert_eq!(runs["by_status"]["completed"], 2);
    assert_eq!(runs["by_status"]["failed"], 1);
    assert_eq!(sum(&runs["series"], "succeeded"), 2);
    assert_eq!(sum(&runs["series"], "failed"), 1);
    assert_eq!(runs["queued"], 0);
    assert_eq!(runs["running"], 0);
    assert_eq!(
        runs["failure_rate_by_pipeline"],
        serde_json::json!([{ "pipeline": "bad-b", "runs": 1, "failed": 1, "rate": 1.0 }])
    );
    let slowest = runs["slowest"].as_array().unwrap();
    assert!(
        slowest
            .iter()
            .any(|p| p["pipeline"] == "ok-a" && p["runs"] == 2),
        "{o}"
    );
    assert_eq!(o["records"]["total"], 6);
    assert_eq!(sum(&o["records"]["series"], "written"), 6);
    assert_eq!(
        o["records"]["top_pipelines"],
        serde_json::json!([{ "pipeline": "ok-a", "written": 6 }])
    );
    assert_eq!(o["governance"]["totals"]["quarantined"], 0);
    let cost = &o["cost"];
    assert_eq!(cost["currency"], "USD", "{o}");
    assert!(cost["invocations"].as_u64().unwrap() >= 2, "{o}");
    assert!(
        cost["by_pipeline"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["pipeline"] == "overview-e2e")
    );
    let fresh = &o["freshness"];
    assert!(fresh["datasets"].as_u64().unwrap() >= 2, "{o}");
    assert_eq!(fresh["stale_count"], 0);
    match grafana {
        Some(_) => {
            let g = &o["grafana"];
            assert!(
                g["cost"]
                    .as_str()
                    .unwrap()
                    .starts_with("https://grafana.example/d/faucet-cost?from="),
                "{o}"
            );
            assert!(
                g["runs"]
                    .as_str()
                    .unwrap()
                    .contains("/d/faucet-pipeline-overview?")
            );
        }
        None => assert!(o.get("grafana").is_none()),
    }

    let (_, only) = get(&client, &url("?since=7d&pipeline=ok-a"), "viewer-tok").await;
    assert_eq!(only["bucket"], "day");
    assert_eq!(only["runs"]["total"], 2);
    assert_eq!(
        only["runs"]["failure_rate_by_pipeline"],
        serde_json::json!([])
    );
    assert_eq!(only["records"]["total"], 6);
    assert_eq!(only["cost"]["invocations"], 2, "{only}");

    let (_, none) = get(&client, &url("?since=24h&label=team:none"), "viewer-tok").await;
    assert_eq!(none["runs"]["total"], 0);

    let (_, scoped) = get(&client, &url("?since=24h"), "tenant-tok").await;
    assert_eq!(scoped["runs"]["total"], 0, "{scoped}");
    assert!(scoped.get("freshness").is_none(), "{scoped}");
    let (status, _) = get(&client, &url("?tenant=other"), "tenant-tok").await;
    assert_eq!(status, 404);

    for bad in [
        "?since=soon",
        "?bucket=week",
        "?top=0",
        "?nope=1",
        "?since=90d&bucket=hour",
    ] {
        let (status, body) = get(&client, &url(bad), "viewer-tok").await;
        assert_eq!(status, 400, "{bad}: {body}");
    }

    let (status, audit) = get(
        &client,
        &format!("{base}/v1/audit?action=overview.get"),
        "admin-tok",
    )
    .await;
    assert_eq!(status, 200, "{audit}");
    let entries = audit
        .as_array()
        .or_else(|| audit["entries"].as_array())
        .unwrap();
    assert!(entries.iter().any(|e| e["principal"] == "bob"), "{audit}");
}

#[tokio::test(flavor = "multi_thread")]
async fn overview_on_the_memory_history() {
    exercise(None, None).await;
}

#[cfg(feature = "serve-history-sqlite")]
#[tokio::test(flavor = "multi_thread")]
async fn overview_on_sqlite_with_grafana_links() {
    let dir = tempfile::tempdir().unwrap();
    let history = format!("sqlite:{}", dir.path().join("history.db").display());
    exercise(Some(history), Some("https://grafana.example/")).await;
}

#[test]
fn a_grafana_url_must_be_a_plain_http_base() {
    use faucet_cli::serve::config::grafana_url;
    assert_eq!(grafana_url(None).unwrap(), None);
    assert_eq!(grafana_url(Some("  ")).unwrap(), None);
    assert_eq!(
        grafana_url(Some("https://g.example/grafana/"))
            .unwrap()
            .as_deref(),
        Some("https://g.example/grafana")
    );
    for bad in [
        "ftp://g.example",
        "not a url",
        "https://u:p@g.example",
        "https://g.example/?orgId=1",
    ] {
        assert!(grafana_url(Some(bad)).is_err(), "{bad}");
    }
}
