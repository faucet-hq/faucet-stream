//! Integration test for `GET /v1/usage` (#704): boots a real RBAC server,
//! runs a pipeline through `POST /v1/runs`, and asserts the usage report the
//! server accumulated — plus RBAC (a viewer can read it; an unauthenticated
//! caller cannot) and the query validation.
#![cfg(all(feature = "catalog", feature = "source-csv", feature = "sink-jsonl"))]

use serde_json::Value;
use std::time::Duration;

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

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
        require_approval: Vec::new(),
        approval_expiry_secs: 86_400,
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

/// Submit a config and wait for the run to finish; returns its terminal record.
async fn run_config(base: &str, client: &reqwest::Client, config: &str) -> Value {
    let resp = client
        .post(format!("{base}/v1/runs"))
        .bearer_auth("admin-tok")
        .json(&serde_json::json!({ "config": config }))
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

#[tokio::test(flavor = "multi_thread")]
async fn usage_endpoint_reports_runs_and_enforces_rbac() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.jsonl");
    std::fs::write(&input, "id,name\n1,alice\n2,bob\n3,carol\n").unwrap();
    let port = free_port();
    spawn_server(port, dir.path()).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");

    // Unauthenticated → 401.
    let unauth = client.get(format!("{base}/v1/usage")).send().await.unwrap();
    assert_eq!(unauth.status().as_u16(), 401);

    // Before any run: an empty report, not an error.
    let empty: Value = client
        .get(format!("{base}/v1/usage"))
        .bearer_auth("viewer-tok")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(empty["report"]["records"], 0);
    assert_eq!(empty["report"]["rows"].as_array().unwrap().len(), 0);

    // A submitted config sets its own pricing inline; a file is refused.
    let config = format!(
        "version: 1\nname: usage-e2e\npipeline:\n  source: {{ type: csv, config: {{ path: {} }} }}\n  sink: {{ type: jsonl, config: {{ path: {} }} }}\nusage:\n  pricing:\n    currency: EUR\n    hosted_elt_per_million_rows: 1000000\n",
        input.display(),
        output.display()
    );
    let rec = run_config(&base, &client, &config).await;
    assert_eq!(rec["status"], "completed", "{rec}");
    let inv = &rec["invocations"][0];
    assert_eq!(inv["usage"]["usage"]["records_written"], 3, "{rec}");
    assert_eq!(inv["usage"]["cost"]["currency"], "EUR");

    // A viewer reads the report: one invocation, priced in the run's currency.
    let report: Value = client
        .get(format!(
            "{base}/v1/usage?by=sink&include_records=true&since=2000-01-01"
        ))
        .bearer_auth("viewer-tok")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let r = &report["report"];
    assert_eq!(r["by"], "sink");
    assert_eq!(r["currency"], "EUR");
    assert_eq!(r["records"], 1);
    let rows = r["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1, "{report}");
    assert_eq!(rows[0]["key"], "jsonl");
    assert_eq!(rows[0]["records_written"], 3);
    assert_eq!(rows[0]["runs"], 1);
    // 3 rows at 1,000,000 per million = 3.00 hosted equivalent.
    assert!((rows[0]["hosted_equivalent"].as_f64().unwrap() - 3.0).abs() < 1e-9);
    assert_eq!(r["total"]["records_written"], 3);
    let records = report["records"].as_array().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["source_kind"], "csv");
    assert_eq!(records[0]["pipeline"], "usage-e2e");

    // The window excludes the run → nothing; a bad `by` is a client error.
    let none: Value = client
        .get(format!("{base}/v1/usage?until=2000-01-01"))
        .bearer_auth("viewer-tok")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(none["report"]["records"], 0);
    let bad = client
        .get(format!("{base}/v1/usage?by=hour"))
        .bearer_auth("viewer-tok")
        .send()
        .await
        .unwrap();
    assert!(bad.status().is_client_error(), "{}", bad.status());

    // `pricing_file` is refused for a submitted config (no server filesystem).
    let with_file = format!(
        "version: 1\nname: usage-file\npipeline:\n  source: {{ type: csv, config: {{ path: {} }} }}\n  sink: {{ type: jsonl, config: {{ path: {} }} }}\nusage:\n  pricing_file: /etc/pricing.yaml\n",
        input.display(),
        output.display()
    );
    let rec = run_config(&base, &client, &with_file).await;
    assert_eq!(rec["status"], "failed", "{rec}");
    assert!(
        rec["error"]
            .as_str()
            .unwrap_or_default()
            .contains("pricing_file"),
        "{rec}"
    );
}
