//! Integration tests for per-run completion callbacks (#481).
//!
//! Boots a real `faucet serve`, submits runs over HTTP with a `callback`, and
//! asserts the callback lands at a wiremock receiver with the right body — plus
//! the refusals (bad egress target, backfill fan-out, reserved keys).
#![cfg(feature = "serve")]

use faucet_cli::cli::ServeArgs;
use faucet_cli::serve::ServeConfig;
use serde_json::{Value, json};
use std::time::Duration;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

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
        templates_sync: None,
        policy: None,
        callback_allow_host: Vec::new(),
        mcp: false,
        mcp_allow_mutations: false,
    }
}

async fn spawn_server(port: u16) {
    let mut config = ServeConfig::from_args(args_on(port)).unwrap();
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

/// A pipeline that succeeds.
fn ok_yaml(input: &std::path::Path, output: &std::path::Path) -> String {
    format!(
        "version: 1\nname: cb\npipeline:\n  source: {{ type: csv, config: {{ path: \"{}\" }} }}\n  sink: {{ type: jsonl, config: {{ path: \"{}\" }} }}\n",
        input.display(),
        output.display()
    )
}

/// A pipeline that fails: the csv source points at a path that does not exist.
fn failing_yaml(output: &std::path::Path) -> String {
    format!(
        "version: 1\nname: cb_fail\npipeline:\n  source: {{ type: csv, config: {{ path: \"/nonexistent/definitely-not-here.csv\" }} }}\n  sink: {{ type: jsonl, config: {{ path: \"{}\" }} }}\n",
        output.display()
    )
}

/// Poll the receiver until it has at least one request, or time out.
async fn wait_for_callback(server: &MockServer) -> Value {
    for _ in 0..200 {
        let reqs = server.received_requests().await.unwrap();
        if let Some(r) = reqs.first() {
            return serde_json::from_slice(&r.body).unwrap();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no callback received within the timeout");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn callback_fires_on_success_with_run_identity() {
    let receiver = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/cb"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&receiver)
        .await;

    let port = free_port();
    spawn_server(port).await;

    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.jsonl");
    std::fs::write(&input, "id,name\n1,alice\n2,bob\n").unwrap();

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://127.0.0.1:{port}/v1/runs"))
        .json(&json!({
            "config": ok_yaml(&input, &output),
            "callback": {
                "url": format!("{}/cb", receiver.uri()),
                "extra_fields": { "job_id": "caller-job-1" }
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202, "submission accepted");
    let submitted: Value = resp.json().await.unwrap();
    let run_id = submitted["run_id"].as_str().unwrap().to_string();

    let body = wait_for_callback(&receiver).await;
    // The whole point: the callback correlates to the id the submission returned.
    assert_eq!(body["run_id"], run_id);
    assert_eq!(body["event"], "run.completed");
    assert_eq!(body["status"], "completed");
    assert_eq!(body["records_written"], 2);
    assert!(body["error"].is_null());
    assert!(body["finished_at"].is_string());
    assert!(body["elapsed_secs"].as_f64().unwrap() >= 0.0);
    // Caller-supplied metadata round-trips.
    assert_eq!(body["job_id"], "caller-job-1");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn callback_fires_on_failure() {
    let receiver = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/cb"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&receiver)
        .await;

    let port = free_port();
    spawn_server(port).await;
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("out.jsonl");

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://127.0.0.1:{port}/v1/runs"))
        .json(&json!({
            "config": failing_yaml(&output),
            "callback": { "url": format!("{}/cb", receiver.uri()) }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);

    let body = wait_for_callback(&receiver).await;
    assert_eq!(body["event"], "run.failed");
    assert_eq!(body["status"], "failed");
    assert!(
        body["error"].is_string(),
        "a failed run must report its error: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_filter_suppresses_unsubscribed_terminal_states() {
    // Subscribing only to `failed` must not deliver on a successful run.
    let receiver = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/cb"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&receiver)
        .await;

    let port = free_port();
    spawn_server(port).await;
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.jsonl");
    std::fs::write(&input, "id\n1\n").unwrap();

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://127.0.0.1:{port}/v1/runs"))
        .json(&json!({
            "config": ok_yaml(&input, &output),
            "callback": { "url": format!("{}/cb", receiver.uri()), "on": ["failed"] }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);
    let run_id = resp.json::<Value>().await.unwrap()["run_id"]
        .as_str()
        .unwrap()
        .to_string();

    // Wait for the run itself to reach a terminal state, then assert silence.
    for _ in 0..200 {
        let rec: Value = client
            .get(format!("http://127.0.0.1:{port}/v1/runs/{run_id}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if rec["status"] == "completed" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        receiver.received_requests().await.unwrap().is_empty(),
        "a callback subscribed only to `failed` must not fire on success"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refuses_link_local_callback_target_at_submit_time() {
    let port = free_port();
    spawn_server(port).await;
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.jsonl");
    std::fs::write(&input, "id\n1\n").unwrap();

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://127.0.0.1:{port}/v1/runs"))
        .json(&json!({
            "config": ok_yaml(&input, &output),
            // The concrete SSRF risk documented for serve: cloud instance metadata.
            "callback": { "url": "http://169.254.169.254/latest/meta-data/" }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        422,
        "a link-local callback target must be refused at submit time"
    );
    let err: Value = resp.json().await.unwrap();
    assert!(
        err.to_string().contains("link-local"),
        "error should explain why: {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refuses_non_http_scheme_and_reserved_extra_field() {
    let port = free_port();
    spawn_server(port).await;
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.jsonl");
    std::fs::write(&input, "id\n1\n").unwrap();
    let cfg = ok_yaml(&input, &output);
    let client = reqwest::Client::new();

    for (cb, needle) in [
        (json!({ "url": "file:///etc/passwd" }), "scheme"),
        (
            json!({ "url": "https://x.example/h", "extra_fields": { "status": "ok" } }),
            "status",
        ),
    ] {
        let resp = client
            .post(format!("http://127.0.0.1:{port}/v1/runs"))
            .json(&json!({ "config": cfg, "callback": cb }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 422, "must be refused: {cb}");
        let err: Value = resp.json().await.unwrap();
        assert!(
            err.to_string().contains(needle),
            "error should mention `{needle}`: {err}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backfill_refuses_a_callback_rather_than_dropping_it() {
    let port = free_port();
    spawn_server(port).await;
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("out.jsonl");

    // Window-scoped source so the backfill gate itself passes.
    let cfg = format!(
        "version: 1\nname: bf\npipeline:\n  source: {{ type: csv, config: {{ path: \"./data-${{backfill.start}}.csv\" }} }}\n  sink: {{ type: jsonl, config: {{ path: \"{}\" }} }}\n",
        output.display()
    );

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://127.0.0.1:{port}/v1/backfill"))
        .json(&json!({
            "config": cfg,
            "from": "2026-01-01",
            "to": "2026-01-04",
            "window": "1d",
            "callback": { "url": "https://caller.example/hook" }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        422,
        "one POST /v1/backfill fans out into N runs, so a single callback is ambiguous"
    );
    let err: Value = resp.json().await.unwrap();
    let text = err.to_string();
    assert!(
        text.contains("callback") && text.contains("window unit"),
        "error should explain the fan-out: {err}"
    );
}
