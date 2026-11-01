//! Run submitter, template registration actor and exact status counts (#998),
//! end to end over an RBAC server: `submitted_by` is the authenticated
//! principal (never the request body), `GET /v1/runs` filters on it
//! (`submitted_by=` / `mine=true`) and returns per-status `counts` over the
//! whole filter, `GET /v1/changes/counts` counts change requests, and the
//! template detail names who registered each version. Runs against the
//! in-memory history and, with `serve-history-sqlite`, a SQLite one.
#![cfg(all(feature = "templates", feature = "source-csv", feature = "sink-jsonl"))]
#![allow(clippy::disallowed_methods)] // tests drive time and raw clients directly

use serde_json::{Value, json};
use std::time::Duration;

#[path = "support/ports.rs"]
mod ports;
use ports::free_port;

/// alice = admin, bob + dave = operators, carol = viewer. Runs need one
/// approval from an operator or admin other than the requester.
const AUTH_CONFIG: &str = "principals:\n\
    \x20 - { name: alice, token: admin-tok, role: admin }\n\
    \x20 - { name: bob, token: bob-tok, role: operator }\n\
    \x20 - { name: dave, token: dave-tok, role: operator }\n\
    \x20 - { name: carol, token: viewer-tok, role: viewer }\n\
    approvals:\n\
    \x20 rules:\n\
    \x20   - { kinds: [run], roles: [operator, admin], self_approve: false }\n";

fn serve_args(
    port: u16,
    auth_config: std::path::PathBuf,
    history: Option<String>,
) -> faucet_cli::cli::ServeArgs {
    faucet_cli::cli::ServeArgs {
        listen: format!("127.0.0.1:{port}"),
        auth_token: None,
        auth_config: Some(auth_config),
        read_token: None,
        write_token: None,
        admin_token: None,
        no_auth: false,
        allow_unauthenticated_network: false,
        max_concurrent_runs: Some(4),
        max_queued_runs: Some(16),
        default_config: None,
        history,
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
        no_ui: false,
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
        grafana_url: None,
    }
}

async fn spawn_server(port: u16, dir: &std::path::Path, history: Option<String>) {
    let auth_path = dir.join("auth.yaml");
    std::fs::write(&auth_path, AUTH_CONFIG).unwrap();
    let mut config =
        faucet_cli::serve::ServeConfig::from_args(serve_args(port, auth_path, history)).unwrap();
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
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("server did not become healthy on port {port}");
}

struct Api {
    base: String,
    client: reqwest::Client,
}

impl Api {
    async fn send(&self, req: reqwest::RequestBuilder) -> (u16, Value) {
        let r = req.send().await.unwrap();
        let status = r.status().as_u16();
        let text = r.text().await.unwrap();
        let v = serde_json::from_str(&text).unwrap_or(Value::String(text));
        (status, v)
    }
    async fn post(&self, token: &str, path: &str, body: Value) -> (u16, Value) {
        self.send(
            self.client
                .post(format!("{}{path}", self.base))
                .bearer_auth(token)
                .json(&body),
        )
        .await
    }
    async fn get(&self, token: &str, path: &str) -> (u16, Value) {
        self.send(
            self.client
                .get(format!("{}{path}", self.base))
                .bearer_auth(token),
        )
        .await
    }
    async fn wait_run(&self, run_id: &str) -> Value {
        for _ in 0..400 {
            let (_, rec) = self.get("admin-tok", &format!("/v1/runs/{run_id}")).await;
            match rec["status"].as_str().unwrap_or("") {
                "completed" | "failed" | "cancelled" => return rec,
                _ => tokio::time::sleep(Duration::from_millis(25)).await,
            }
        }
        panic!("run {run_id} did not finish");
    }
    async fn submit(&self, token: &str, body: Value) -> String {
        let (code, resp) = self.post(token, "/v1/runs", body).await;
        assert_eq!(code, 202, "{resp}");
        let id = resp["run_id"].as_str().unwrap().to_string();
        self.wait_run(&id).await;
        id
    }
}

fn csv_config(input: &std::path::Path, output: &std::path::Path, name: &str) -> String {
    format!(
        "version: 1\nname: {name}\npipeline:\n  source: {{ type: csv, config: {{ path: {} }} }}\n  sink: {{ type: jsonl, config: {{ path: {} }} }}\n",
        input.display(),
        output.display()
    )
}

fn ids(list: &Value) -> Vec<String> {
    let mut out: Vec<String> = list["runs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["run_id"].as_str().unwrap().to_string())
        .collect();
    out.sort();
    out
}

async fn scenario(history: Option<String>) {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    std::fs::write(&input, "id,name\n1,alice\n2,bob\n").unwrap();
    let port = free_port();
    spawn_server(port, dir.path(), history).await;
    let api = Api {
        base: format!("http://127.0.0.1:{port}"),
        client: reqwest::Client::new(),
    };
    let good = csv_config(&input, &dir.path().join("out.jsonl"), "who-ok");
    let bad = csv_config(
        &dir.path().join("missing.csv"),
        &dir.path().join("x.jsonl"),
        "who-bad",
    );

    // alice runs one good and one failing pipeline; bob runs one good one
    // and tries to pass himself off as alice through the request body.
    let a1 = api.submit("admin-tok", json!({ "config": good })).await;
    let a2 = api.submit("admin-tok", json!({ "config": bad })).await;
    let b1 = api
        .submit(
            "bob-tok",
            json!({ "config": good, "submitted_by": "alice" }),
        )
        .await;

    let (_, rec) = api.get("viewer-tok", &format!("/v1/runs/{a1}")).await;
    assert_eq!(rec["submitted_by"], "alice", "{rec}");
    let (_, rec) = api.get("viewer-tok", &format!("/v1/runs/{b1}")).await;
    assert_eq!(
        rec["submitted_by"], "bob",
        "a body-supplied submitter is ignored: {rec}"
    );
    let (_, rec) = api.get("viewer-tok", &format!("/v1/runs/{a2}")).await;
    assert_eq!(rec["status"], "failed", "{rec}");

    // Counts are opt-in: a plain listing carries none.
    let (_, plain) = api.get("viewer-tok", "/v1/runs?limit=1").await;
    assert!(plain.get("counts").is_none(), "{plain}");
    // Exact counts over the whole filter, whatever the page size.
    let (code, list) = api.get("viewer-tok", "/v1/runs?limit=1&counts=true").await;
    assert_eq!(code, 200, "{list}");
    assert_eq!(list["runs"].as_array().unwrap().len(), 1);
    assert!(list["next_cursor"].is_string());
    assert_eq!(
        list["counts"],
        json!({ "completed": 2, "failed": 1 }),
        "{list}"
    );

    // The submitter filter, by name and as `mine`.
    let (code, list) = api
        .get("viewer-tok", "/v1/runs?submitted_by=alice&counts=true")
        .await;
    assert_eq!(code, 200, "{list}");
    let mut want = vec![a1.clone(), a2.clone()];
    want.sort();
    assert_eq!(ids(&list), want);
    assert_eq!(list["counts"], json!({ "completed": 1, "failed": 1 }));
    let (_, list) = api.get("bob-tok", "/v1/runs?mine=true&counts=true").await;
    assert_eq!(ids(&list), vec![b1.clone()]);
    assert_eq!(list["counts"], json!({ "completed": 1 }));
    let (_, list) = api
        .get("viewer-tok", "/v1/runs?mine=true&counts=true")
        .await;
    assert!(ids(&list).is_empty());
    assert_eq!(list["counts"], json!({}));
    // Filters compose with the counts.
    let (_, list) = api
        .get(
            "viewer-tok",
            "/v1/runs?submitted_by=alice&status=failed&counts=true",
        )
        .await;
    assert_eq!(ids(&list), vec![a2.clone()]);
    assert_eq!(list["counts"], json!({ "failed": 1 }));
    let (code, err) = api
        .get("viewer-tok", "/v1/runs?mine=true&submitted_by=bob")
        .await;
    assert_eq!(code, 400, "{err}");

    // An approved change request runs as its requester.
    let (code, change) = api
        .post(
            "dave-tok",
            "/v1/changes",
            json!({ "kind": "run", "payload": { "config": good }, "reason": "x" }),
        )
        .await;
    assert_eq!(code, 201, "{change}");
    let change_id = change["id"].as_str().unwrap().to_string();
    let (code, counts) = api.get("viewer-tok", "/v1/changes/counts").await;
    assert_eq!(code, 200, "{counts}");
    assert_eq!(counts["counts"], json!({ "pending": 1 }));
    let (code, done) = api
        .post(
            "bob-tok",
            &format!("/v1/changes/{change_id}/approve"),
            json!({}),
        )
        .await;
    assert_eq!(code, 200, "{done}");
    assert_eq!(done["status"], "executed", "{done}");
    let d1 = done["run_id"].as_str().unwrap().to_string();
    let rec = api.wait_run(&d1).await;
    assert_eq!(rec["submitted_by"], "dave", "{rec}");
    let (_, counts) = api.get("viewer-tok", "/v1/changes/counts").await;
    assert_eq!(counts["counts"], json!({ "executed": 1 }));
    let (_, counts) = api
        .get("viewer-tok", "/v1/changes/counts?requester=bob")
        .await;
    assert_eq!(counts["counts"], json!({}));
    let (code, _) = api.get("viewer-tok", "/v1/changes/counts?limit=5").await;
    assert_eq!(code, 400, "the counts take no limit");

    // A registered template version names who registered it.
    let template = format!("kind: pipeline\n{good}");
    let (code, reg) = api
        .post(
            "admin-tok",
            "/v1/templates",
            json!({ "id": "who-tpl", "config": template }),
        )
        .await;
    assert_eq!(code, 201, "{reg}");
    let (code, reg) = api
        .post(
            "admin-tok",
            "/v1/templates",
            json!({ "id": "who-tpl", "config": format!("{template}# v2\n") }),
        )
        .await;
    assert_eq!(code, 201, "{reg}");
    let (code, detail) = api
        .get("viewer-tok", "/v1/templates/who-tpl?version=newest")
        .await;
    assert_eq!(code, 200, "{detail}");
    let regs = detail["registrations"].as_array().unwrap();
    assert_eq!(regs.len(), 2, "{detail}");
    assert_eq!(regs[0]["version"], 2);
    assert_eq!(regs[1]["version"], 1);
    for r in regs {
        assert_eq!(r["registered_by"], "alice", "{detail}");
        assert!(r["registered_at"].is_string());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn submitter_and_counts_on_the_memory_history() {
    scenario(None).await;
}

#[cfg(feature = "serve-history-sqlite")]
#[tokio::test(flavor = "multi_thread")]
async fn submitter_and_counts_on_a_sqlite_history() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite:{}", dir.path().join("history.db").display());
    scenario(Some(url)).await;
}
