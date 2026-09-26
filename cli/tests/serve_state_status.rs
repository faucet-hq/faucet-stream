//! `GET|POST /v1/status` (#732) and `GET|PUT|DELETE /v1/state/{pipeline}/{row}`
//! (#735) on a real RBAC server: status from an inline config and from a
//! registered template, RBAC (viewers read status, only admins touch state),
//! the move / reset verbs against the file store, the 409 while a run is in
//! flight, and the audit trail.
#![cfg(all(
    feature = "serve",
    feature = "templates",
    feature = "source-csv",
    feature = "source-rest",
    feature = "sink-jsonl"
))]

use faucet_core::{FileStateStore, StateStore};
use serde_json::{Value, json};
use std::path::Path;
use std::sync::Arc;
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
kind: pipeline
name: shop
pipeline:
  source: {{ type: csv, config: {{ path: {input} }} }}
  sink: {{ type: jsonl, config: {{ path: {out} }} }}
  state: {{ type: file, config: {{ path: {state} }} }}
"#,
        input = dir.join("in.csv").display(),
        out = dir.join("out.jsonl").display(),
        state = dir.join("state").display(),
    )
}

async fn wait_run(base: &str, client: &reqwest::Client, run_id: &str) -> Value {
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

async fn submit(base: &str, client: &reqwest::Client, config: &str) -> String {
    let resp = client
        .post(format!("{base}/v1/runs"))
        .bearer_auth("admin-tok")
        .json(&json!({ "config": config }))
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
    body["run_id"].as_str().unwrap().to_string()
}

#[tokio::test(flavor = "multi_thread")]
async fn status_and_state_endpoints() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("in.csv"), "id\n1\n2\n").unwrap();
    let port = free_port();
    spawn_server(port, dir.path()).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");
    let config = config_text(dir.path());

    let run_id = submit(&base, &client, &config).await;
    let rec = wait_run(&base, &client, &run_id).await;
    assert_eq!(rec["status"], "completed", "{rec}");

    // Status: a viewer may read it, via query string and body.
    let resp = client
        .get(format!("{base}/v1/status"))
        .query(&[("config", config.as_str())])
        .bearer_auth("viewer-tok")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let report: Value = resp.json().await.unwrap();
    assert_eq!(report["pipeline"], "shop");
    assert_eq!(report["health"], "ok", "{report}");
    assert_eq!(report["exit_code"], 0);
    assert_eq!(report["rows"][0]["last_success"]["records"], 2);
    let resp = client
        .post(format!("{base}/v1/status"))
        .bearer_auth("viewer-tok")
        .json(&json!({ "config": config, "row": "row-0", "probe": true }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    for (body, code) in [
        (json!({}), 400),
        (json!({ "config": config, "template": "x" }), 400),
        (json!({ "config": config, "row": "nope" }), 422),
        (json!({ "config": "[unclosed" }), 400),
        (json!({ "template": "no-such-template" }), 404),
    ] {
        let resp = client
            .post(format!("{base}/v1/status"))
            .bearer_auth("viewer-tok")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), code, "{body}");
    }
    let resp = client
        .get(format!("{base}/v1/status"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 401);

    // Status of a registered template.
    let resp = client
        .post(format!("{base}/v1/templates"))
        .bearer_auth("admin-tok")
        .json(&json!({ "id": "shop", "config": config, "launch": true }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        201,
        "{}",
        resp.text().await.unwrap()
    );
    let resp = client
        .get(format!("{base}/v1/status"))
        .query(&[("template", "shop"), ("version", "stable")])
        .bearer_auth("viewer-tok")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let report: Value = resp.json().await.unwrap();
    assert_eq!(report["rows"][0]["health"], "ok");
    let resp = client
        .get(format!("{base}/v1/status"))
        .query(&[("template", "shop"), ("version", "not a version")])
        .bearer_auth("viewer-tok")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 400);

    // State: admin-only.
    let state_url = format!("{base}/v1/state/shop/row-0");
    let resp = client
        .get(&state_url)
        .query(&[("config", config.as_str())])
        .bearer_auth("viewer-tok")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 403);
    let resp = client
        .get(&state_url)
        .query(&[("template", "shop")])
        .bearer_auth("admin-tok")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let row: Value = resp.json().await.unwrap();
    assert_eq!(row["row"], "row-0");
    assert!(
        row["markers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["kind"] == "status")
    );

    let store = FileStateStore::new(dir.path().join("state"));
    let resp = client
        .put(&state_url)
        .bearer_auth("admin-tok")
        .json(&json!({ "config": config, "bookmark": {"id": 1}, "dry_run": true }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.json::<Value>().await.unwrap()["applied"], false);
    assert!(store.get("shop::row-0").await.unwrap().is_none());
    let resp = client
        .put(&state_url)
        .bearer_auth("admin-tok")
        .json(&json!({ "config": config, "bookmark": {"id": 1} }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(
        store.get("shop::row-0").await.unwrap(),
        Some(json!({"id": 1}))
    );

    // A config for another pipeline, or a null bookmark, is refused.
    let resp = client
        .put(format!("{base}/v1/state/other/row-0"))
        .bearer_auth("admin-tok")
        .json(&json!({ "config": config, "bookmark": 1 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 422);
    let resp = client
        .put(&state_url)
        .bearer_auth("admin-tok")
        .json(&json!({ "config": config, "bookmark": null }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 422);

    // A live run lease on the row → 409 until forced.
    let shared: Arc<dyn StateStore> = Arc::new(FileStateStore::new(dir.path().join("state")));
    let lease = faucet_cli::pipeline_state::lease::acquire(Arc::clone(&shared), "shop::row-0", "x")
        .await
        .unwrap();
    let resp = client
        .delete(&state_url)
        .query(&[("config", config.as_str())])
        .bearer_auth("admin-tok")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 409);
    let resp = client
        .delete(&state_url)
        .query(&[
            ("config", config.as_str()),
            ("force", "true"),
            ("include_markers", "true"),
            ("dry_run", "true"),
        ])
        .bearer_auth("admin-tok")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.json::<Value>().await.unwrap()["applied"], false);
    lease.release().await;
    let resp = client
        .delete(&state_url)
        .query(&[("config", config.as_str()), ("include_markers", "true")])
        .bearer_auth("admin-tok")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    assert!(store.get("shop::row-0").await.unwrap().is_none());
    assert!(
        store
            .get("shop::row-0::__status__")
            .await
            .unwrap()
            .is_none()
    );

    // The audit trail names every state operation.
    let audit: Value = client
        .get(format!("{base}/v1/audit"))
        .bearer_auth("admin-tok")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let text = audit.to_string();
    for action in ["status", "state.get", "state.set", "state.reset"] {
        assert!(
            text.contains(&format!("\"{action}\"")),
            "missing {action}: {text}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_run_in_flight_blocks_state_changes() {
    let mock = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .respond_with(
            wiremock::ResponseTemplate::new(200)
                .set_body_json(json!([{"id": 1}]))
                .set_delay(Duration::from_secs(4)),
        )
        .mount(&mock)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    spawn_server(port, dir.path()).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");
    let config = format!(
        r#"version: 1
name: slow
pipeline:
  source:
    type: rest
    config:
      base_url: "{uri}"
      path: "/items"
      method: GET
      auth: {{ type: none }}
      query_params: {{}}
      pagination: {{ type: None }}
      max_retries: 0
      retry_backoff: 0
      tolerated_http_errors: []
      replication_method: {{ type: FullTable }}
      primary_keys: []
      requests: []
      schema_sample_size: 0
  sink: {{ type: jsonl, config: {{ path: {out} }} }}
  state: {{ type: file, config: {{ path: {state} }} }}
"#,
        uri = mock.uri(),
        out = dir.path().join("out.jsonl").display(),
        state = dir.path().join("state").display(),
    );
    let run_id = submit(&base, &client, &config).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let url = format!("{base}/v1/state/slow/row-0");
    let resp = client
        .put(&url)
        .bearer_auth("admin-tok")
        .json(&json!({ "config": config, "bookmark": 1 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 409);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("is in flight"),
        "the run history refused it: {body}"
    );
    // Status reports the run in flight.
    let report: Value = client
        .post(format!("{base}/v1/status"))
        .bearer_auth("viewer-tok")
        .json(&json!({ "config": config }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(report["rows"][0]["health"], "running", "{report}");
    let rec = wait_run(&base, &client, &run_id).await;
    assert_eq!(rec["status"], "completed", "{rec}");
    let resp = client
        .put(&url)
        .bearer_auth("admin-tok")
        .json(&json!({ "config": config, "bookmark": 1 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
}
