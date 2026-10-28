//! A `faucet serve` with an empty in-memory registry and no auth, for the
//! template integration tests.
#![allow(dead_code, clippy::disallowed_methods)] // tests drive time and raw clients directly

use std::time::Duration;

use faucet_cli::cli::ServeArgs;
use faucet_cli::serve::config::ServeConfig;
use serde_json::Value;

#[path = "ports.rs"]
mod ports;

fn args(port: u16) -> ServeArgs {
    ServeArgs {
        listen: format!("127.0.0.1:{port}"),
        auth_token: None,
        auth_config: None,
        read_token: None,
        write_token: None,
        admin_token: None,
        no_auth: true,
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
        references: Default::default(),
        pools: Default::default(),
    }
}

/// A started server.
pub struct Server {
    pub client: reqwest::Client,
    pub base: String,
}

impl Server {
    pub async fn start() -> Self {
        let port = ports::free_port();
        let mut config = ServeConfig::from_args(args(port)).unwrap();
        config.log_level = "warn".into();
        tokio::spawn(async move {
            let _ = faucet_cli::serve::run_server(config, Default::default()).await;
        });
        let client = reqwest::Client::new();
        let base = format!("http://127.0.0.1:{port}");
        for _ in 0..400 {
            if client
                .get(format!("{base}/healthz"))
                .send()
                .await
                .is_ok_and(|r| r.status().is_success())
            {
                return Self { client, base };
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("server did not come up");
    }

    /// `method path` with an optional JSON body: (status, body).
    pub async fn call(&self, method: &str, path: &str, body: Option<Value>) -> (u16, Value) {
        let mut rb = self
            .client
            .request(method.parse().unwrap(), format!("{}{path}", self.base));
        if let Some(b) = body {
            rb = rb.json(&b);
        }
        let r = rb.send().await.unwrap();
        let code = r.status().as_u16();
        let text = r.text().await.unwrap();
        (
            code,
            serde_json::from_str(&text).unwrap_or(Value::String(text)),
        )
    }

    /// Register a template document.
    pub async fn register(&self, body: &str) -> (u16, Value) {
        self.call(
            "POST",
            "/v1/templates",
            Some(serde_json::json!({ "config": body })),
        )
        .await
    }

    /// Poll a run until it is terminal.
    pub async fn wait_run(&self, run_id: &str) -> Value {
        for _ in 0..800 {
            let (_, rec) = self.call("GET", &format!("/v1/runs/{run_id}"), None).await;
            match rec["status"].as_str() {
                Some("completed" | "failed" | "cancelled") => return rec,
                _ => tokio::time::sleep(Duration::from_millis(25)).await,
            }
        }
        panic!("run {run_id} did not finish in time");
    }
}
