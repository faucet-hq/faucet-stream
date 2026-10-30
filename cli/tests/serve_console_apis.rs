//! The console's read APIs (#998), end to end over an RBAC server:
//! `GET /v1/runs/{id}/config` (masked below an unscoped admin, 404 without a
//! stored body), `RunRecord.dlq_location` (server-set, never from the body),
//! `GET /v1/runs?dataset_id=`, `GET /v1/audit?result=&before=` paging and
//! `GET /v1/audit/{id}`, `GET /v1/triggers`, `GET /v1/templates/{id}/runs`
//! and the reload response's `loaded_at`. Runs against the in-memory history
//! and a clustered SQLite one.
#![cfg(all(
    feature = "templates",
    feature = "triggers",
    feature = "catalog",
    feature = "source-file",
    feature = "sink-file"
))]
#![allow(clippy::disallowed_methods)] // tests drive time and raw clients directly

use serde_json::{Value, json};
use std::path::Path;
use std::time::Duration;

#[path = "support/ports.rs"]
mod ports;
use ports::free_port;

/// alice = admin, bob = operator, carol = viewer.
const AUTH_CONFIG: &str = "principals:\n\
    \x20 - { name: alice, token: admin-tok, role: admin }\n\
    \x20 - { name: bob, token: bob-tok, role: operator }\n\
    \x20 - { name: carol, token: viewer-tok, role: viewer }\n";

struct Setup {
    history: Option<String>,
    cluster: bool,
    triggers: Option<std::path::PathBuf>,
    default_config: Option<std::path::PathBuf>,
}

fn serve_args(port: u16, auth_config: std::path::PathBuf, s: Setup) -> faucet_cli::cli::ServeArgs {
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
        default_config: s.default_config,
        history: s.history,
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
        cluster: s.cluster,
        cluster_poll_secs: 1,
        cluster_max_attempts: 3,
        triggers: s.triggers,
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

async fn spawn_server(dir: &Path, setup: Setup) -> Api {
    let port = free_port();
    let auth_path = dir.join("auth.yaml");
    std::fs::write(&auth_path, AUTH_CONFIG).unwrap();
    let mut config =
        faucet_cli::serve::ServeConfig::from_args(serve_args(port, auth_path, setup)).unwrap();
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
            return Api {
                base: format!("http://127.0.0.1:{port}"),
                client,
            };
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
        for _ in 0..800 {
            let (_, rec) = self.get("admin-tok", &format!("/v1/runs/{run_id}")).await;
            match rec["status"].as_str().unwrap_or("") {
                "completed" | "failed" | "cancelled" => return rec,
                _ => tokio::time::sleep(Duration::from_millis(25)).await,
            }
        }
        panic!("run {run_id} did not finish");
    }
    async fn submit(&self, body: Value) -> Value {
        let (code, resp) = self.post("admin-tok", "/v1/runs", body).await;
        assert_eq!(code, 202, "{resp}");
        self.wait_run(resp["run_id"].as_str().unwrap()).await
    }
}

/// A file → file config with a file DLQ and a credential-looking var.
fn config(dir: &Path, name: &str) -> String {
    let input = dir.join("in.csv");
    if !input.exists() {
        std::fs::write(&input, "id,name\n1,alice\n2,bob\n").unwrap();
    }
    format!(
        "version: 1\nname: {name}\nvars:\n  api_token: hunter2-plaintext\n\
         pipeline:\n  source: {{ type: file, config: {{ path: {} }} }}\n  sink: {{ type: file, config: {{ path: {} }} }}\n  \
         dlq: {{ sink: {{ type: file, config: {{ path: {} }} }} }}\n",
        input.display(),
        dir.join(format!("{name}.jsonl")).display(),
        dir.join("dlq.jsonl").display(),
    )
}

/// Runs, run config, DLQ location, dataset filter and audit paging.
async fn runs_and_audit(history: Option<String>, cluster: bool) {
    let dir = tempfile::tempdir().unwrap();
    let api = spawn_server(
        dir.path(),
        Setup {
            history,
            cluster,
            triggers: None,
            default_config: None,
        },
    )
    .await;

    // `dlq_location` is set by the server from the config, never from the body.
    let run = api
        .submit(json!({ "config": config(dir.path(), "orders"), "dlq_location": "/etc/evil" }))
        .await;
    assert_eq!(run["status"], "completed", "{run}");
    let id = run["run_id"].as_str().unwrap().to_string();
    assert_eq!(
        run["dlq_location"],
        dir.path().join("dlq.jsonl").display().to_string()
    );
    let plain = format!(
        "version: 1\nname: nodlq\npipeline:\n  source: {{ type: file, config: {{ path: {} }} }}\n  sink: {{ type: file, config: {{ path: {} }} }}\n",
        dir.path().join("in.csv").display(),
        dir.path().join("nodlq.jsonl").display(),
    );
    let run2 = api
        .submit(json!({ "config": plain, "dlq_location": "/etc/evil" }))
        .await;
    assert!(run2.get("dlq_location").is_none(), "{run2}");

    // The stored config: only clustered runs keep one.
    let path = format!("/v1/runs/{id}/config");
    if cluster {
        let (code, admin) = api.get("admin-tok", &path).await;
        assert_eq!(code, 200, "{admin}");
        assert_eq!(admin["format"], "yaml");
        assert_eq!(admin["redacted"], false);
        assert!(
            admin["body"]
                .as_str()
                .unwrap()
                .contains("hunter2-plaintext")
        );
        let (code, viewer) = api.get("viewer-tok", &path).await;
        assert_eq!(code, 200, "{viewer}");
        let body = viewer["body"].as_str().unwrap();
        assert_eq!(viewer["redacted"], true);
        assert!(!body.contains("hunter2-plaintext"), "{body}");
        assert!(body.contains("***"), "{body}");
        // Every read is audited.
        let (_, audit) = api
            .get("admin-tok", "/v1/audit?action=run.config&limit=10")
            .await;
        assert_eq!(audit["entries"].as_array().unwrap().len(), 2, "{audit}");
    } else {
        let (code, _) = api.get("admin-tok", &path).await;
        assert_eq!(code, 404);
    }
    let (code, _) = api.get("viewer-tok", "/v1/runs/no-such-run/config").await;
    assert_eq!(code, 404);

    // The dataset filter keeps the runs that wrote the dataset.
    let dataset = run["invocations"][0]["usage"]["dataset_id"]
        .as_str()
        .expect("serve records the sink dataset")
        .to_string();
    let (code, list) = api
        .get(
            "viewer-tok",
            &format!("/v1/runs?dataset_id={dataset}&counts=true"),
        )
        .await;
    assert_eq!(code, 200, "{list}");
    let ids: Vec<&str> = list["runs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["run_id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, [id.as_str()]);
    assert_eq!(list["counts"]["completed"], 1);
    let (_, none) = api.get("viewer-tok", "/v1/runs?dataset_id=ffff").await;
    assert!(none["runs"].as_array().unwrap().is_empty(), "{none}");
    let (code, bad) = api.get("viewer-tok", "/v1/runs?dataset_id=a%25b").await;
    assert_eq!(code, 400, "{bad}");

    // Title (name, else `pipeline` label) substring and pool filters run on
    // the server, over the whole history.
    let names = |v: &Value| -> Vec<String> {
        let mut n: Vec<String> = v["runs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| {
                r["name"]
                    .as_str()
                    .or(r["labels"]["pipeline"].as_str())
                    .unwrap_or_default()
                    .to_string()
            })
            .collect();
        n.sort();
        n
    };
    let (code, list) = api.get("viewer-tok", "/v1/runs?name_contains=rder").await;
    assert_eq!(code, 200, "{list}");
    assert_eq!(names(&list), ["orders"], "{list}");
    let (_, list) = api.get("viewer-tok", "/v1/runs?name_contains=RDER").await;
    assert!(names(&list).is_empty(), "case-sensitive: {list}");
    let (_, list) = api.get("viewer-tok", "/v1/runs?pool=default").await;
    assert_eq!(names(&list), ["nodlq", "orders"], "{list}");
    let (_, list) = api.get("viewer-tok", "/v1/runs?pool=gpu&counts=true").await;
    assert!(names(&list).is_empty(), "{list}");
    assert_eq!(list["counts"], json!({}), "{list}");
    let (code, bad) = api.get("viewer-tok", "/v1/runs?pool=Not%20A%20Pool").await;
    assert_eq!(code, 400, "{bad}");

    // A denial, then the audit filters and the keyset cursor.
    let (code, _) = api
        .post("viewer-tok", "/v1/runs", json!({ "config": "version: 1" }))
        .await;
    assert_eq!(code, 403);
    let (code, denied) = api.get("admin-tok", "/v1/audit?result=denied").await;
    assert_eq!(code, 200);
    let denied = denied["entries"].as_array().unwrap();
    assert!(!denied.is_empty());
    assert!(denied.iter().all(|e| e["result"] == "denied"));

    let (_, all) = api.get("admin-tok", "/v1/audit?limit=1000").await;
    let all: Vec<String> = all["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["id"].as_str().unwrap().to_string())
        .collect();
    assert!(all.len() >= 3, "{all:?}");
    let mut paged = Vec::new();
    let mut before: Option<String> = None;
    loop {
        let q = match &before {
            Some(b) => format!("/v1/audit?limit=2&before={b}"),
            None => "/v1/audit?limit=2".to_string(),
        };
        let (code, page) = api.get("admin-tok", &q).await;
        assert_eq!(code, 200, "{page}");
        for e in page["entries"].as_array().unwrap() {
            paged.push(e["id"].as_str().unwrap().to_string());
        }
        match page["next_before"].as_str() {
            Some(b) => before = Some(b.to_string()),
            None => break,
        }
    }
    // Entries written while paging (the reads themselves are not audited)
    // could only be newer; the pages are the full, ordered list.
    assert_eq!(paged, all);
    let (_, gone) = api.get("admin-tok", "/v1/audit?before=no-such-entry").await;
    assert!(gone["entries"].as_array().unwrap().is_empty(), "{gone}");

    // One entry by id; admin only.
    let first = &all[0];
    let (code, entry) = api.get("admin-tok", &format!("/v1/audit/{first}")).await;
    assert_eq!(code, 200, "{entry}");
    assert_eq!(entry["id"], first.as_str());
    let (code, _) = api.get("viewer-tok", &format!("/v1/audit/{first}")).await;
    assert_eq!(code, 403);
    let (code, _) = api.get("admin-tok", "/v1/audit/no-such-entry").await;
    assert_eq!(code, 404);
}

#[tokio::test(flavor = "multi_thread")]
async fn console_run_and_audit_apis_on_the_memory_history() {
    runs_and_audit(None, false).await;
}

#[cfg(feature = "serve-history-sqlite")]
#[tokio::test(flavor = "multi_thread")]
async fn console_run_and_audit_apis_on_a_clustered_sqlite_history() {
    let db = tempfile::tempdir().unwrap();
    let url = format!("sqlite:{}", db.path().join("h.db").display());
    runs_and_audit(Some(url), true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_trigger_list_shows_every_trigger_with_its_fires() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = dir.path().join("pipeline.yaml");
    std::fs::write(&cfg, config(dir.path(), "hooked")).unwrap();
    let triggers = dir.path().join("triggers.yaml");
    std::fs::write(
        &triggers,
        format!(
            "version: 1\ntriggers:\n  - name: on-push\n    type: webhook\n    methods: [POST]\n    debounce_secs: 0\n    config: {}\n  - name: off\n    enabled: false\n    type: webhook\n    config:\n      version: 1\n      pipeline: {{}}\n",
            cfg.display()
        ),
    )
    .unwrap();
    let api = spawn_server(
        dir.path(),
        Setup {
            history: None,
            cluster: false,
            triggers: Some(triggers),
            default_config: None,
        },
    )
    .await;
    let (code, fired) = api.post("bob-tok", "/v1/triggers/on-push", json!({})).await;
    assert_eq!(code, 202, "{fired}");

    let (code, list) = api.get("viewer-tok", "/v1/triggers").await;
    assert_eq!(code, 200, "{list}");
    let t = list["triggers"].as_array().unwrap();
    assert_eq!(t.len(), 2, "{list}");
    assert_eq!(t[0]["name"], "on-push");
    assert_eq!(t[0]["type"], "webhook");
    assert_eq!(t[0]["enabled"], true);
    assert_eq!(t[0]["healthy"], true);
    assert_eq!(t[0]["methods"], json!(["POST"]));
    assert_eq!(t[0]["config"], "pipeline.yaml");
    assert_eq!(t[0]["fired_total"], 1);
    assert_eq!(t[0]["errors_total"], 0);
    assert_eq!(t[1]["name"], "off");
    assert_eq!(t[1]["enabled"], false);
    assert_eq!(t[1]["config"], "inline");
    assert_eq!(t[1]["fired_total"], 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_template_lists_its_runs_and_reload_names_the_load_time() {
    let dir = tempfile::tempdir().unwrap();
    let default = dir.path().join("default.yaml");
    std::fs::write(&default, "version: 1\npipeline: {}\n").unwrap();
    let api = spawn_server(
        dir.path(),
        Setup {
            history: None,
            cluster: false,
            triggers: None,
            default_config: Some(default),
        },
    )
    .await;

    let (code, reg) = api
        .post(
            "admin-tok",
            "/v1/templates",
            json!({ "id": "orders-sync", "config": config(dir.path(), "orders-sync") }),
        )
        .await;
    assert_eq!(code, 201, "{reg}");
    let (code, trig) = api
        .post(
            "bob-tok",
            "/v1/templates/orders-sync/runs",
            json!({ "version": 1 }),
        )
        .await;
    assert_eq!(code, 202, "{trig}");
    api.wait_run(trig["run_id"].as_str().unwrap()).await;
    api.submit(json!({ "config": config(dir.path(), "other") }))
        .await;

    let (code, runs) = api
        .get("viewer-tok", "/v1/templates/orders-sync/runs?counts=true")
        .await;
    assert_eq!(code, 200, "{runs}");
    let runs_list = runs["runs"].as_array().unwrap();
    assert_eq!(runs_list.len(), 1, "{runs}");
    assert_eq!(runs_list[0]["run_id"], trig["run_id"]);
    assert_eq!(runs["counts"]["completed"], 1);
    let (code, _) = api.get("viewer-tok", "/v1/templates/no-such/runs").await;
    assert_eq!(code, 404);
    let (code, _) = api
        .get("viewer-tok", "/v1/templates/orders-sync/runs?bogus=1")
        .await;
    assert_eq!(code, 400);

    let (code, reload) = api.post("admin-tok", "/v1/reload", json!({})).await;
    assert_eq!(code, 200, "{reload}");
    assert_eq!(reload["reloaded"], true);
    assert!(reload["loaded_at"].as_str().is_some(), "{reload}");
}
