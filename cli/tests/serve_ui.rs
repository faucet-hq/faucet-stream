//! Embedded web-console serving + auth-boundary tests (serve-ui feature).
#![cfg(feature = "serve-ui")]

use faucet_cli::cli::ServeArgs;
use faucet_cli::serve::ServeConfig;
use std::time::Duration;

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn args(port: u16) -> ServeArgs {
    ServeArgs {
        listen: format!("127.0.0.1:{port}"),
        auth_token: None,
        auth_config: None,
        read_token: None,
        write_token: None,
        admin_token: None,
        no_auth: true,
        max_concurrent_runs: Some(2),
        max_queued_runs: Some(8),
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
        callback_allow_host: Vec::new(),
        mcp: false,
        mcp_allow_mutations: false,
    }
}

async fn boot(args: ServeArgs) -> (String, reqwest::Client) {
    let port = args.listen.rsplit(':').next().unwrap().to_string();
    let mut config = ServeConfig::from_args(args).unwrap();
    config.log_level = "warn".into();
    tokio::spawn(async move {
        let _ = faucet_cli::serve::run_server(config, Default::default()).await;
    });
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");
    for _ in 0..200 {
        if client
            .get(format!("{base}/healthz"))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
        {
            return (base, client);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("server did not come up");
}

#[tokio::test(flavor = "multi_thread")]
async fn serves_shell_assets_and_spa_fallback() {
    let port = free_port();
    let (base, client) = boot(args(port)).await;

    let r = client.get(format!("{base}/")).send().await.unwrap();
    assert_eq!(r.status(), 200);
    assert!(
        r.headers()["content-type"]
            .to_str()
            .unwrap()
            .contains("text/html"),
        "index should be html"
    );

    let r = client
        .get(format!("{base}/assets/styles.css"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert!(
        r.headers()["content-type"]
            .to_str()
            .unwrap()
            .contains("css")
    );

    let r = client
        .get(format!("{base}/runs"))
        .header("accept", "text/html")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert!(
        r.headers()["content-type"]
            .to_str()
            .unwrap()
            .contains("text/html")
    );

    let r = client
        .get(format!("{base}/nope"))
        .header("accept", "application/json")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
    assert!(r.text().await.unwrap().contains("\"error\""));
}

#[tokio::test(flavor = "multi_thread")]
async fn ui_is_public_but_api_is_gated() {
    let port = free_port();
    let mut a = args(port);
    a.no_auth = false;
    a.auth_token = Some("s3cret".into());
    let (base, client) = boot(a).await;

    let r = client.get(format!("{base}/")).send().await.unwrap();
    assert_eq!(r.status(), 200);

    let r = client.get(format!("{base}/v1/runs")).send().await.unwrap();
    assert_eq!(r.status(), 401);

    // The new read/probe endpoints sit on the same gated sub-router.
    let r = client
        .get(format!("{base}/v1/schemas"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
    let r = client
        .post(format!("{base}/v1/doctor"))
        .json(&serde_json::json!({ "config": "version: 1" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
}

#[tokio::test(flavor = "multi_thread")]
async fn schemas_catalog_and_one_schema() {
    let port = free_port();
    let (base, client) = boot(args(port)).await;

    let r = client
        .get(format!("{base}/v1/schemas"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let body: serde_json::Value = r.json().await.unwrap();
    assert!(body["sources"].is_array() && body["sinks"].is_array());
    let blocks: Vec<&str> = body["blocks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b["name"].as_str().unwrap())
        .collect();
    for name in ["state", "dlq", "delivery", "resilience", "sla", "schema"] {
        assert!(blocks.contains(&name), "missing block {name}: {blocks:?}");
    }

    for name in &blocks {
        let r = client
            .get(format!("{base}/v1/schemas/block/{name}"))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200, "block {name}");
        assert!(r.json::<serde_json::Value>().await.unwrap().is_object());
    }
    let r = client
        .get(format!("{base}/v1/schemas/block/nope"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);

    let r = client
        .get(format!("{base}/v1/schemas/source/rest"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert!(r.json::<serde_json::Value>().await.unwrap().is_object());

    let r = client
        .get(format!("{base}/v1/schemas/source/nope"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
}

#[tokio::test(flavor = "multi_thread")]
async fn doctor_rejects_invalid_config() {
    let port = free_port();
    let (base, client) = boot(args(port)).await;
    let r = client
        .post(format!("{base}/v1/doctor"))
        .json(&serde_json::json!({ "config": "::: not yaml :::" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    assert!(r.text().await.unwrap().contains("\"error\""));
}
