//! #862 H-04: SQL connectors in a config `faucet serve` loads are sandboxed —
//! a duckdb query cannot read host files and a sqlite query cannot attach
//! another database — while a config from the server's own triggers file keeps
//! host access.
#![cfg(all(
    feature = "serve",
    feature = "source-duckdb",
    feature = "source-sqlite",
    feature = "sink-jsonl"
))]

use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::time::Duration;

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn args(port: u16, triggers: Option<PathBuf>) -> faucet_cli::cli::ServeArgs {
    faucet_cli::cli::ServeArgs {
        listen: format!("127.0.0.1:{port}"),
        auth_token: Some("tok".into()),
        auth_config: None,
        read_token: None,
        write_token: None,
        admin_token: None,
        no_auth: false,
        allow_unauthenticated_network: false,
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
        triggers,
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
    }
}

async fn spawn(triggers: Option<PathBuf>) -> (String, reqwest::Client) {
    let port = free_port();
    let mut config = faucet_cli::serve::ServeConfig::from_args(args(port, triggers)).unwrap();
    config.log_level = "warn".into();
    tokio::spawn(async move {
        let _ = faucet_cli::serve::run_server(
            config,
            faucet_cli::serve::McpServeSettings {
                enabled: false,
                allow_mutations: false,
            },
        )
        .await;
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
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    (format!("http://127.0.0.1:{port}"), client)
}

async fn submit(base: &str, client: &reqwest::Client, config: &str) -> Value {
    let r = client
        .post(format!("{base}/v1/runs"))
        .bearer_auth("tok")
        .json(&json!({ "config": config }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 202);
    let id = r.json::<Value>().await.unwrap()["run_id"]
        .as_str()
        .unwrap()
        .to_string();
    finished(base, client, &id).await
}

async fn finished(base: &str, client: &reqwest::Client, id: &str) -> Value {
    for _ in 0..800 {
        let rec: Value = client
            .get(format!("{base}/v1/runs/{id}"))
            .bearer_auth("tok")
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
    panic!("run {id} did not finish");
}

fn secret(dir: &Path) -> PathBuf {
    let path = dir.join("secret.txt");
    std::fs::write(&path, "top-secret").unwrap();
    path
}

fn duckdb_reading(file: &Path, out: &Path, extra: &str) -> String {
    format!(
        "version: 1\nname: leak\npipeline:\n  source: {{ type: duckdb, config: {{ database: \":memory:\", query: \"SELECT content FROM read_text('{}')\"{extra} }} }}\n  sink: {{ type: jsonl, config: {{ path: \"{}\" }} }}\n",
        file.display(),
        out.display()
    )
}

fn error_text(rec: &Value) -> String {
    rec.to_string()
}

#[tokio::test(flavor = "multi_thread")]
async fn submitted_sql_cannot_reach_host_files() {
    let dir = tempfile::tempdir().unwrap();
    let (base, client) = spawn(None).await;
    let file = secret(dir.path());
    let out = dir.path().join("out.jsonl");

    for (config, expect) in [
        (
            duckdb_reading(Path::new("/etc/passwd"), &out, ""),
            "disabled",
        ),
        (duckdb_reading(&file, &out, ", sandbox: false"), "disabled"),
    ] {
        let rec = submit(&base, &client, &config).await;
        assert_eq!(rec["status"], "failed", "{rec}");
        assert!(error_text(&rec).contains(expect), "{rec}");
        assert!(!error_text(&rec).contains("top-secret"), "{rec}");
    }

    let other = dir.path().join("other.db");
    std::fs::write(&other, b"").unwrap();
    let attach = format!(
        "version: 1\nname: attach\npipeline:\n  source: {{ type: sqlite, config: {{ database_url: \"sqlite::memory:\", max_connections: 1, query: \"ATTACH '{}' AS o\" }} }}\n  sink: {{ type: jsonl, config: {{ path: \"{}\" }} }}\n",
        other.display(),
        out.display()
    );
    let rec = submit(&base, &client, &attach).await;
    assert_eq!(rec["status"], "failed", "{rec}");
    assert!(error_text(&rec).contains("too many attached"), "{rec}");

    let fine = format!(
        "version: 1\nname: fine\npipeline:\n  source: {{ type: duckdb, config: {{ database: \":memory:\", query: \"SELECT 1 AS one\" }} }}\n  sink: {{ type: jsonl, config: {{ path: \"{}\" }} }}\n",
        out.display()
    );
    let rec = submit(&base, &client, &fine).await;
    assert_eq!(
        rec["status"], "completed",
        "a query over its own database runs: {rec}"
    );
    assert!(std::fs::read_to_string(&out).unwrap().contains("\"one\":1"));
}

#[cfg(feature = "triggers")]
#[tokio::test(flavor = "multi_thread")]
async fn a_triggers_file_config_keeps_host_access() {
    let dir = tempfile::tempdir().unwrap();
    let file = secret(dir.path());
    let out = dir.path().join("trig.jsonl");
    std::fs::write(
        dir.path().join("pipe.yaml"),
        duckdb_reading(&file, &out, ""),
    )
    .unwrap();
    let triggers = dir.path().join("triggers.yaml");
    std::fs::write(
        &triggers,
        "version: 1\ntriggers:\n  - name: hook\n    type: webhook\n    config: pipe.yaml\n",
    )
    .unwrap();
    let (base, client) = spawn(Some(triggers)).await;
    let r = client
        .post(format!("{base}/v1/triggers/hook"))
        .bearer_auth("tok")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 202);
    let id = r.json::<Value>().await.unwrap()["run_id"]
        .as_str()
        .unwrap()
        .to_string();
    let rec = finished(&base, &client, &id).await;
    assert_eq!(rec["status"], "completed", "{rec}");
    assert_eq!(rec["host_access"], true, "{rec}");
    assert!(
        std::fs::read_to_string(&out)
            .unwrap()
            .contains("top-secret")
    );
}
