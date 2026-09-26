//! `/metrics` integration test for `faucet serve` — ISOLATED in its own test
//! binary because the Prometheus recorder is a process-global singleton (only the
//! first server in a process gets a render handle). Requires the `serve` feature.
#![cfg(feature = "serve")]

use faucet_cli::cli::ServeArgs;
use faucet_cli::serve::ServeConfig;
use std::time::Duration;

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn args_on(port: u16, token: Option<&str>) -> ServeArgs {
    ServeArgs {
        listen: format!("127.0.0.1:{port}"),
        auth_token: token.map(|t| t.to_string()),
        auth_config: None,
        read_token: None,
        write_token: None,
        admin_token: None,
        no_auth: token.is_none(),
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
    }
}

async fn spawn_server(port: u16, token: Option<&str>) {
    let mut config = ServeConfig::from_args(args_on(port, token)).unwrap();
    config.log_level = "warn".into();
    tokio::spawn(async move {
        let _ = faucet_cli::serve::run_server(config, Default::default()).await;
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
            return;
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

/// Submit a run, poll until complete, then assert /metrics contains the expected
/// counters. Isolated here so this process is the first (and only) to install
/// the Prometheus recorder — the /metrics endpoint returns 200 with content.
#[tokio::test]
async fn submit_poll_get_completes_and_records_metrics() {
    let port = free_port();
    spawn_server(port, None).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");

    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.jsonl");
    std::fs::write(&input, "name\nalice\nbob\n").unwrap();
    let body = serde_json::json!({
        "config": csv_to_jsonl_yaml(&input, &output),
        "config_format": "yaml"
    });

    let resp = client
        .post(format!("{base}/v1/runs"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202, "expected 202 Accepted on submit");
    let submit: serde_json::Value = resp.json().await.unwrap();
    let run_id = submit["run_id"].as_str().unwrap().to_string();
    assert_eq!(submit["status"], "queued");

    // Poll until the run reaches a terminal state.
    let mut status = String::new();
    for _ in 0..200 {
        let rec: serde_json::Value = client
            .get(format!("{base}/v1/runs/{run_id}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        status = rec["status"].as_str().unwrap().to_string();
        if status == "completed" || status == "failed" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        status, "completed",
        "run did not complete within poll window"
    );
    assert!(output.exists(), "sink output file was not created");

    // Assert /metrics contains the expected metric names.
    let metrics_resp = client.get(format!("{base}/metrics")).send().await.unwrap();
    assert_eq!(
        metrics_resp.status(),
        200,
        "/metrics should render (recorder installed)"
    );
    let metrics = metrics_resp.text().await.unwrap();
    assert!(
        metrics.contains("faucet_serve_runs_total"),
        "metrics missing runs_total:\n{metrics}"
    );
    assert!(
        metrics.contains("faucet_serve_requests_total"),
        "metrics missing requests_total"
    );

    // Verify GET /v1/runs lists the completed run.
    let list: serde_json::Value = client
        .get(format!("{base}/v1/runs"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        list["runs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["run_id"] == run_id),
        "completed run not found in GET /v1/runs listing"
    );

    // GET /v1/runs/<nonexistent> must return 404.
    assert_eq!(
        client
            .get(format!("{base}/v1/runs/does-not-exist"))
            .send()
            .await
            .unwrap()
            .status(),
        404,
        "missing run must return 404"
    );
}
