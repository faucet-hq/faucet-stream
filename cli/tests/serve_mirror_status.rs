//! `GET|POST /v1/mirror/{name}` (#731): a mirror's per-table status read from
//! its state store on a real RBAC server — viewers may read it, the config must
//! name the mirror, a mirror that never ran is a 422, and every read is audited.
#![cfg(all(feature = "serve", feature = "source-csv", feature = "sink-jsonl"))]

use faucet_core::{FileStateStore, StateStore};
use serde_json::{Value, json};
use std::path::Path;
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

async fn spawn_server(port: u16, dir: &Path) {
    let auth_path = dir.join("auth.yaml");
    std::fs::write(&auth_path, AUTH_CONFIG).unwrap();
    let args = faucet_cli::cli::ServeArgs {
        listen: format!("127.0.0.1:{port}"),
        auth_token: None,
        auth_config: Some(auth_path),
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
        vault_key: None,
        vault_previous_key: Vec::new(),
        connect_providers: None,
        allow_subprocess_connectors: false,
    };
    let mut config = faucet_cli::serve::ServeConfig::from_args(args).unwrap();
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

fn config_text(dir: &Path) -> String {
    format!(
        r#"version: 1
name: shop
pipeline:
  source: {{ type: csv, config: {{ path: {input} }} }}
  sink: {{ type: jsonl, config: {{ path: {out} }} }}
  state: {{ type: file, config: {{ path: {state} }} }}
mirror:
  mode: snapshot_then_cdc
  snapshot:
    source: {{ type: csv, config: {{ path: {input} }} }}
  tables: {{ include: ["*"], destination: {{ path: "out/{{table}}.jsonl" }} }}
"#,
        input = dir.join("in.csv").display(),
        out = dir.join("out.jsonl").display(),
        state = dir.join("state").display(),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn mirror_status_over_http() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("in.csv"), "id\n1\n").unwrap();
    let port = free_port();
    spawn_server(port, dir.path()).await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let cfg = config_text(dir.path());
    let get = |name: &str, token: Option<&str>| {
        let mut req = client
            .get(format!("{base}/v1/mirror/{name}"))
            .query(&[("config", cfg.as_str())]);
        if let Some(t) = token {
            req = req.bearer_auth(t);
        }
        req
    };

    assert_eq!(get("shop", None).send().await.unwrap().status(), 401);
    let refused = get("shop", Some("viewer-tok")).send().await.unwrap();
    assert_eq!(
        refused.status(),
        403,
        "an inline config needs the operator role"
    );
    let not_started = get("shop", Some("admin-tok")).send().await.unwrap();
    assert_eq!(not_started.status(), 422);
    let body: Value = not_started.json().await.unwrap();
    assert!(body.to_string().contains("has not started"), "{body}");

    let store = FileStateStore::new(dir.path().join("state"));
    let now = chrono::Utc::now().to_rfc3339();
    store
        .put(
            "shop::__replication__",
            &json!({
                "version": 2,
                "updated_at": now,
                "tables": {
                    "public.orders": {
                        "phase": "active", "since": now, "id": "public.orders",
                        "changes": 42, "last_applied_at": now, "key": ["id"], "write_mode": "upsert"
                    },
                    "public.logs": {
                        "phase": "refused", "since": now, "last_error": "table 'public.logs' has no primary key"
                    }
                }
            }),
        )
        .await
        .unwrap();
    store
        .put("shop::public.orders", &json!({"last_lsn": "0/16B3748"}))
        .await
        .unwrap();

    let ok = get("shop", Some("admin-tok")).send().await.unwrap();
    assert_eq!(ok.status(), 200);
    let status: Value = ok.json().await.unwrap();
    assert_eq!(status["mode"], "tables");
    assert_eq!(status["summary"]["tables"], 2);
    let orders = status["tables"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["table"] == "public.orders")
        .unwrap();
    assert_eq!(orders["phase"], "active");
    assert_eq!(orders["changes"], 42);
    assert_eq!(orders["position"], json!({"last_lsn": "0/16B3748"}));

    let posted = client
        .post(format!("{base}/v1/mirror/shop"))
        .bearer_auth("admin-tok")
        .json(&json!({ "config": cfg }))
        .send()
        .await
        .unwrap();
    assert_eq!(posted.status(), 200);
    let posted: Value = posted.json().await.unwrap();
    assert_eq!(posted["summary"]["by_phase"]["refused"], 1);

    let wrong = get("other", Some("admin-tok")).send().await.unwrap();
    assert_eq!(wrong.status(), 422);

    let audit: Value = client
        .get(format!("{base}/v1/audit"))
        .query(&[("action", "mirror.status")])
        .bearer_auth("admin-tok")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(audit.to_string().contains("mirror.status"), "{audit}");
}
