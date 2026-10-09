//! HTTP-boundary details of `faucet serve` (#789): CORS preflights for
//! `--cors-origin`, `--body-limit-bytes` above axum's 2 MiB default, and the
//! `concurrency: 0` refusal text.
#![cfg(feature = "serve")]

use faucet_cli::serve::{ServeConfig, run_server};
use std::time::Duration;

fn config(listen: &str) -> ServeConfig {
    let args = faucet_cli::cli::ServeArgs {
        listen: listen.into(),
        auth_token: Some("tok".into()),
        auth_config: None,
        read_token: None,
        write_token: None,
        admin_token: None,
        no_auth: false,
        max_concurrent_runs: Some(2),
        max_queued_runs: Some(8),
        default_config: None,
        history: None,
        cors_origin: vec!["https://app.example".into()],
        body_limit_bytes: 8 * 1024 * 1024,
        shutdown_grace_secs: 5,
        retain_terminal_runs_secs: 60,
        idempotency_retention_secs: 60,
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
        vault_key: None,
        vault_previous_key: Vec::new(),
        connect_providers: None,
        allow_subprocess_connectors: false,
    };
    ServeConfig::from_args(args).unwrap()
}

async fn spawn() -> String {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);
    let listen = format!("127.0.0.1:{port}");
    tokio::spawn(run_server(config(&listen), Default::default()));
    let base = format!("http://{listen}");
    for _ in 0..400 {
        if reqwest::get(format!("{base}/healthz")).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    base
}

#[tokio::test(flavor = "multi_thread")]
async fn cors_bodies_and_refusal_messages() {
    let base = spawn().await;
    let client = reqwest::Client::new();

    // A preflight for an authenticated JSON POST names the allowed method and
    // headers, so the browser goes ahead (SERVE-42).
    let pre = client
        .request(reqwest::Method::OPTIONS, format!("{base}/v1/runs"))
        .header("Origin", "https://app.example")
        .header("Access-Control-Request-Method", "POST")
        .header(
            "Access-Control-Request-Headers",
            "authorization,content-type,idempotency-key",
        )
        .send()
        .await
        .unwrap();
    assert!(pre.status().is_success(), "{}", pre.status());
    let h = pre.headers();
    assert_eq!(
        h["access-control-allow-origin"].to_str().unwrap(),
        "https://app.example"
    );
    assert!(
        h["access-control-allow-methods"]
            .to_str()
            .unwrap()
            .contains("POST")
    );
    let allowed = h["access-control-allow-headers"]
        .to_str()
        .unwrap()
        .to_ascii_lowercase();
    for want in ["authorization", "content-type", "idempotency-key"] {
        assert!(allowed.contains(want), "{allowed}");
    }

    // A 3 MiB body under an 8 MiB `--body-limit-bytes` reaches the handler
    // (a config error, not 413) (SERVE-52).
    let big = format!("version: 1\n# {}\n", "x".repeat(3 * 1024 * 1024));
    let r = client
        .post(format!("{base}/v1/runs"))
        .bearer_auth("tok")
        .json(&serde_json::json!({ "config": big }))
        .send()
        .await
        .unwrap();
    assert_ne!(r.status().as_u16(), 413, "{}", r.text().await.unwrap());

    // The `concurrency: 0` refusal reads as one sentence (SERVE-55).
    let r = client
        .post(format!("{base}/v1/runs"))
        .bearer_auth("tok")
        .json(&serde_json::json!({ "config": "version: 1\n", "concurrency": 0 }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 422);
    let text = r.text().await.unwrap();
    assert!(text.contains("count, not a"), "{text}");
    assert!(!text.contains("  "), "{text}");
}
