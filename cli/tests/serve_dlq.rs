//! Integration tests for the `/v1/dlq/*` control-plane endpoints (#281).
//! Boots a real RBAC server and drives inspect / replay / discard over HTTP,
//! asserting the handler bodies run, RBAC is enforced (viewer may inspect but
//! not replay/discard), and the returned summaries match.
#![cfg(feature = "serve")]

use serde_json::{Value, json};
use std::time::Duration;

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// `admin-tok` → admin (operator+), `viewer-tok` → viewer (read-only).
const AUTH_CONFIG: &str = "principals:\n\
    \x20 - name: alice\n\
    \x20   token: admin-tok\n\
    \x20   role: admin\n\
    \x20 - name: bob\n\
    \x20   token: viewer-tok\n\
    \x20   role: viewer\n";

fn serve_args(port: u16, auth_config: std::path::PathBuf) -> faucet_cli::cli::ServeArgs {
    faucet_cli::cli::ServeArgs {
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
    }
}

async fn spawn_server(port: u16, dir: &std::path::Path) {
    let auth_path = dir.join("auth.yaml");
    std::fs::write(&auth_path, AUTH_CONFIG).unwrap();
    let mut config =
        faucet_cli::serve::ServeConfig::from_args(serve_args(port, auth_path)).unwrap();
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

fn envelope(reason: &str, payload: Value) -> String {
    json!({
        "error": { "kind": "QualityFailure", "message": "boom" },
        "reason": reason,
        "payload": payload,
        "ts_ms": 1_751_760_000_000i64,
        "sink": "jsonl", "pipeline": "orig", "row": "", "record_index": 0,
    })
    .to_string()
}

#[tokio::test(flavor = "multi_thread")]
async fn dlq_endpoints_inspect_replay_discard_with_rbac() {
    let dir = tempfile::tempdir().unwrap();
    let dlq = dir.path().join("dlq.jsonl");
    std::fs::write(
        &dlq,
        format!(
            "{}\n{}\n",
            envelope("quality", json!({"id": 1})),
            envelope("quality", json!({"id": 2})),
        ),
    )
    .unwrap();

    let port = free_port();
    spawn_server(port, dir.path()).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");

    // Unauthenticated inspect → 401.
    let unauth = client
        .post(format!("{base}/v1/dlq/inspect"))
        .json(&json!({ "location": dlq.to_str().unwrap() }))
        .send()
        .await
        .unwrap();
    assert_eq!(unauth.status(), 401);

    // Viewer may inspect (DlqRead) → 200 with the grouped summary.
    let insp = client
        .post(format!("{base}/v1/dlq/inspect"))
        .bearer_auth("viewer-tok")
        .json(&json!({ "location": dlq.to_str().unwrap() }))
        .send()
        .await
        .unwrap();
    assert_eq!(insp.status(), 200, "viewer must be allowed dlq inspect");
    let summary: Value = insp.json().await.unwrap();
    assert_eq!(summary["total_envelopes"], 2);
    assert_eq!(summary["by_reason"]["quality"], 2);

    // Viewer is denied replay/discard (DlqManage = operator+) → 403.
    let denied = client
        .post(format!("{base}/v1/dlq/discard"))
        .bearer_auth("viewer-tok")
        .json(&json!({ "location": dlq.to_str().unwrap(), "delete": true }))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), 403, "viewer must be denied dlq discard");

    // Admin replays (dry-run) through a csv→jsonl config → 200 with the outcome.
    let out = dir.path().join("out.jsonl");
    let cfg = format!(
        "version: 1\npipeline:\n  source: {{ type: csv, config: {{ path: /dev/null }} }}\n  sink: {{ type: jsonl, config: {{ path: \"{}\" }} }}\n",
        out.display()
    );
    let replay = client
        .post(format!("{base}/v1/dlq/replay"))
        .bearer_auth("admin-tok")
        .json(&json!({ "config": cfg, "from": dlq.to_str().unwrap(), "dry_run": true }))
        .send()
        .await
        .unwrap();
    assert_eq!(replay.status(), 200, "admin must be allowed dlq replay");
    let outcome: Value = replay.json().await.unwrap();
    assert_eq!(outcome["candidates"], 2);
    assert_eq!(outcome["dry_run"], true);

    // Admin discards (delete) → 200; both envelopes removed.
    let discard = client
        .post(format!("{base}/v1/dlq/discard"))
        .bearer_auth("admin-tok")
        .json(&json!({ "location": dlq.to_str().unwrap(), "delete": true }))
        .send()
        .await
        .unwrap();
    assert_eq!(discard.status(), 200, "admin must be allowed dlq discard");
    let dout: Value = discard.json().await.unwrap();
    assert_eq!(dout["discarded"], 2);
    // The source file now holds no envelopes.
    let remaining = std::fs::read_to_string(&dlq).unwrap();
    assert!(remaining.trim().is_empty());
}
