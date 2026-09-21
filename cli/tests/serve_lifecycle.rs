//! Lifecycle, auth, idempotency, doctor_first, and cancel integration tests for
//! `faucet serve`. These tests never assert /metrics CONTENT (only status codes
//! and run lifecycle), so they are safe to run in parallel within one process —
//! they don't depend on being the first installer of the Prometheus recorder.
//! Requires the `serve` feature.
#![cfg(feature = "serve")]

use faucet_cli::cli::ServeArgs;
use faucet_cli::serve::ServeConfig;
use std::time::Duration;

// ── shared helpers ────────────────────────────────────────────────────────────

fn free_port() -> u16 {
    // The TcpListener is dropped immediately after obtaining the port. There is a
    // small TOCTOU window; acceptable for integration tests on a loopback interface.
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
        callback_allow_host: Vec::new(),
        mcp: false,
        mcp_allow_mutations: false,
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

// ── tests ─────────────────────────────────────────────────────────────────────

/// Auth middleware: /healthz is public; /v1/* requires a valid Bearer token.
#[tokio::test]
async fn auth_required_when_token_set() {
    let port = free_port();
    spawn_server(port, Some("s3cret")).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");

    // /healthz is always unauthenticated.
    assert!(
        client
            .get(format!("{base}/healthz"))
            .send()
            .await
            .unwrap()
            .status()
            .is_success(),
        "/healthz must not require auth"
    );

    // /v1/runs without a token returns 401.
    assert_eq!(
        client
            .get(format!("{base}/v1/runs"))
            .send()
            .await
            .unwrap()
            .status(),
        401,
        "missing token must yield 401"
    );

    // /v1/runs with the correct token returns 200.
    let ok = client
        .get(format!("{base}/v1/runs"))
        .bearer_auth("s3cret")
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), 200, "correct bearer token must yield 200");
}

/// Same idempotency key + same payload → replay (same run_id).
/// Same key + different payload → 409 Conflict.
#[tokio::test]
async fn idempotency_replays_and_conflicts() {
    let port = free_port();
    spawn_server(port, None).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");

    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.jsonl");
    std::fs::write(&input, "name\nalice\n").unwrap();
    let cfg = csv_to_jsonl_yaml(&input, &output);
    let body = serde_json::json!({ "config": cfg, "idempotency_key": "k1" });

    let first: serde_json::Value = client
        .post(format!("{base}/v1/runs"))
        .json(&body)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let second: serde_json::Value = client
        .post(format!("{base}/v1/runs"))
        .json(&body)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        first["run_id"], second["run_id"],
        "same key + same payload must replay the original run_id"
    );

    // Different payload with the same key must conflict.
    let other_cfg = csv_to_jsonl_yaml(&input, &dir.path().join("other.jsonl"));
    let conflict = client
        .post(format!("{base}/v1/runs"))
        .json(&serde_json::json!({ "config": other_cfg, "idempotency_key": "k1" }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        conflict.status(),
        409,
        "different payload with same idempotency key must return 409"
    );
}

/// `doctor_first: true` with a source pointing at a nonexistent file must reject
/// the submit with 422 Unprocessable, carrying an `invocations` details array.
#[tokio::test]
async fn doctor_first_rejects_bad_connector() {
    let port = free_port();
    spawn_server(port, None).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");

    let body = serde_json::json!({
        "config": "version: 1\npipeline:\n  source: { type: csv, config: { path: /no/such/file.csv } }\n  sink: { type: jsonl, config: { path: /tmp/out.jsonl } }\n",
        "doctor_first": true
    });
    let resp = client
        .post(format!("{base}/v1/runs"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        422,
        "doctor_first failure must return 422 Unprocessable"
    );
    let err: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        err["error"]["code"], "unprocessable",
        "error code must be 'unprocessable'"
    );
    assert!(
        err["error"]["details"]["invocations"].is_array(),
        "details must contain an invocations array; got:\n{err:#}"
    );
}

/// A run that blocks (webhook source with a long timeout and no senders) can be
/// cancelled: after POST /v1/runs/{id}/cancel the status converges to "cancelled".
///
/// Blocking strategy: webhook source bound to a free port with `timeout_secs`
/// long enough that the test completes the cancel interaction before it fires.
/// We never send a POST to the webhook, so the run stays "running" indefinitely
/// until the cancellation token fires.
#[tokio::test(flavor = "multi_thread")]
async fn cancel_transitions_to_cancelled() {
    let port = free_port();
    spawn_server(port, None).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");

    // The webhook source binds an ephemeral port chosen via free_port() and then
    // blocks waiting for POSTs that never arrive (timeout_secs is large), so the
    // run stays `running` until we cancel it. The test never sends POSTs, so the
    // exact port is irrelevant beyond needing to bind successfully.
    let webhook_port = free_port();
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("out.jsonl");
    let config_yaml = format!(
        "version: 1\npipeline:\n  source:\n    type: webhook\n    config:\n      listen_addr: \"127.0.0.1:{webhook_port}\"\n      timeout_secs: 3600\n  sink:\n    type: jsonl\n    config:\n      path: \"{output}\"\n",
        webhook_port = webhook_port,
        output = output.display(),
    );
    let body = serde_json::json!({ "config": config_yaml });

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

    // Wait for the run to reach "running" (webhook source has started its server
    // and is blocking inside the receive loop).
    for _ in 0..200 {
        let rec: serde_json::Value = client
            .get(format!("{base}/v1/runs/{run_id}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let s = rec["status"].as_str().unwrap_or("");
        if s == "running" || s == "completed" || s == "failed" || s == "cancelled" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // Issue cancel. The run must be running (or queued) here; 202 means in-flight
    // cancellation was requested.
    let cancel_resp = client
        .post(format!("{base}/v1/runs/{run_id}/cancel"))
        .send()
        .await
        .unwrap();
    assert!(
        cancel_resp.status() == 202 || cancel_resp.status() == 200,
        "cancel must return 202 (in-flight) or 200 (terminal no-op), got {}",
        cancel_resp.status()
    );

    // Poll until cancelled (or any terminal state so the test doesn't hang forever).
    let mut status = String::new();
    for _ in 0..400 {
        let rec: serde_json::Value = client
            .get(format!("{base}/v1/runs/{run_id}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        status = rec["status"].as_str().unwrap_or("").to_string();
        if status == "cancelled" || status == "completed" || status == "failed" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(status, "cancelled", "run must transition to 'cancelled'");
}

/// `doctor_first: true` against a healthy pipeline must store the (redacted)
/// preflight report on the run record, so `GET /v1/runs/{id}` exposes
/// `doctor_report` (#146 R: the field was declared but never populated).
#[tokio::test]
async fn doctor_first_success_populates_doctor_report() {
    let port = free_port();
    spawn_server(port, None).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");

    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    std::fs::write(&input, "name\nalice\n").unwrap();
    let output = dir.path().join("out.jsonl");
    let body = serde_json::json!({
        "config": csv_to_jsonl_yaml(&input, &output),
        "doctor_first": true,
    });
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

    let rec: serde_json::Value = client
        .get(format!("{base}/v1/runs/{run_id}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        rec["doctor_report"]["invocations"].is_array(),
        "a doctor_first run must expose its preflight report; got:\n{rec:#}"
    );
}

/// Cancelling a run that is still QUEUED (no execution permit yet) must finalize
/// it as `cancelled` promptly — not only after a permit frees. With
/// `max_concurrent_runs=1`, a blocking run holds the sole permit so a second run
/// stays queued; before the #146 fix the queued run's cancel was ignored until
/// the permit freed (here: never, since the blocker runs for an hour), so this
/// test would hang/fail.
#[tokio::test(flavor = "multi_thread")]
async fn cancel_of_queued_run_transitions_to_cancelled() {
    let port = free_port();
    let mut config = ServeConfig::from_args({
        let mut a = args_on(port, None);
        a.max_concurrent_runs = Some(1);
        a
    })
    .unwrap();
    config.log_level = "warn".into();
    tokio::spawn(async move {
        let _ = faucet_cli::serve::run_server(config, Default::default()).await;
    });
    let client = reqwest::Client::new();
    for _ in 0..100 {
        if client
            .get(format!("http://127.0.0.1:{port}/healthz"))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let base = format!("http://127.0.0.1:{port}");

    // Run A: a webhook source that blocks for an hour, taking the only permit.
    let webhook_port = free_port();
    let dir = tempfile::tempdir().unwrap();
    let block_cfg = format!(
        "version: 1\npipeline:\n  source:\n    type: webhook\n    config:\n      listen_addr: \"127.0.0.1:{webhook_port}\"\n      timeout_secs: 3600\n  sink:\n    type: jsonl\n    config:\n      path: \"{}\"\n",
        dir.path().join("a.jsonl").display(),
    );
    let a_id = client
        .post(format!("{base}/v1/runs"))
        .json(&serde_json::json!({ "config": block_cfg }))
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap()["run_id"]
        .as_str()
        .unwrap()
        .to_string();
    for _ in 0..200 {
        let rec: serde_json::Value = client
            .get(format!("{base}/v1/runs/{a_id}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if rec["status"] == "running" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // Run B: a trivial csv→jsonl run that stays QUEUED behind A's permit.
    let input = dir.path().join("in.csv");
    std::fs::write(&input, "name\nbob\n").unwrap();
    let b_id = client
        .post(format!("{base}/v1/runs"))
        .json(&serde_json::json!({ "config": csv_to_jsonl_yaml(&input, &dir.path().join("b.jsonl")) }))
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap()["run_id"]
        .as_str()
        .unwrap()
        .to_string();
    tokio::time::sleep(Duration::from_millis(150)).await;
    let b_rec: serde_json::Value = client
        .get(format!("{base}/v1/runs/{b_id}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        b_rec["status"], "queued",
        "B must be queued behind the single permit; got:\n{b_rec:#}"
    );

    // Cancel the QUEUED run → must converge to cancelled while A still blocks.
    let c = client
        .post(format!("{base}/v1/runs/{b_id}/cancel"))
        .send()
        .await
        .unwrap();
    assert!(
        c.status() == 202 || c.status() == 200,
        "cancel status {}",
        c.status()
    );
    let mut status = String::new();
    for _ in 0..200 {
        let rec: serde_json::Value = client
            .get(format!("{base}/v1/runs/{b_id}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        status = rec["status"].as_str().unwrap_or("").to_string();
        if status == "cancelled" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        status, "cancelled",
        "a queued run must cancel promptly without waiting for a permit (#146 R E6)"
    );
}
