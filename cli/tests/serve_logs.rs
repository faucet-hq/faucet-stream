//! SSE log-streaming integration tests for `faucet serve` (Phase 4, #127).
//!
//! These run in their own test binary so this process is the first (and only)
//! installer of the global tracing subscriber — the `RunLogLayer` must be wired
//! in for `/logs` to capture anything. The server runs at `log_level = info` so
//! the run-start line passes the `EnvFilter`. Requires the `serve` feature.
#![cfg(feature = "serve")]

use faucet_cli::cli::ServeArgs;
use faucet_cli::serve::ServeConfig;
use std::time::Duration;

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn args_on(port: u16) -> ServeArgs {
    ServeArgs {
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
        callback_allow_host: Vec::new(),
        mcp: false,
        mcp_allow_mutations: false,
    }
}

async fn spawn_server(port: u16) {
    let mut config = ServeConfig::from_args(args_on(port)).unwrap();
    // `info` so the run-start line is captured by the SSE log layer.
    config.log_level = "info".into();
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

/// Submit a run, wait for it to finish, then stream `/logs`: the SSE body must
/// contain the captured run-start line and terminate with an `end` event.
#[tokio::test(flavor = "multi_thread")]
async fn logs_stream_replays_and_ends() {
    let port = free_port();
    spawn_server(port).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");

    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.jsonl");
    std::fs::write(&input, "name\nalice\nbob\n").unwrap();
    let body = serde_json::json!({ "config": csv_to_jsonl_yaml(&input, &output) });

    let submit: serde_json::Value = client
        .post(format!("{base}/v1/runs"))
        .json(&body)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let run_id = submit["run_id"].as_str().unwrap().to_string();

    // Wait for the run to reach a terminal state so all log lines are captured
    // and the buffer's `ended` flag is set (buffer survives the drain window).
    for _ in 0..400 {
        let rec: serde_json::Value = client
            .get(format!("{base}/v1/runs/{run_id}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if matches!(
            rec["status"].as_str().unwrap_or(""),
            "completed" | "failed" | "cancelled"
        ) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // Stream the logs. The stream closes when the run's `end` event is emitted,
    // so `.text()` returns the full body; the timeout guards against a hang.
    let resp = client
        .get(format!("{base}/v1/runs/{run_id}/logs"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "/logs must return 200");
    assert!(
        resp.headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .map(|ct| ct.contains("text/event-stream"))
            .unwrap_or(false),
        "/logs must be text/event-stream"
    );

    let body = tokio::time::timeout(Duration::from_secs(10), resp.text())
        .await
        .expect("log stream did not terminate within 10s")
        .unwrap();

    assert!(
        body.contains("event: log"),
        "expected at least one `log` event; body:\n{body}"
    );
    assert!(
        body.contains("pipeline run starting"),
        "expected the captured run-start line; body:\n{body}"
    );
    assert!(
        body.contains("event: end"),
        "stream must terminate with an `end` event; body:\n{body}"
    );
}

/// `/logs` for an unknown run id returns 404.
#[tokio::test]
async fn logs_unknown_run_is_404() {
    let port = free_port();
    spawn_server(port).await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!(
            "http://127.0.0.1:{port}/v1/runs/does-not-exist/logs"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "unknown run must yield 404");
}
