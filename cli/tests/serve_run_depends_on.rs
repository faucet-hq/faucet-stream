//! Completion dependencies a run request adds between its rows / streams
//! (#960): `depends_on` on `POST /v1/runs` and `POST /v1/templates/{id}/runs`
//! orders rows the config never ordered, joins the config's own edges, is
//! refused (400) for an unknown name or a cycle, follows `include_parents`
//! under a selection, is stored on the run and survives a cluster claim.
#![allow(clippy::disallowed_methods)] // tests drive time and raw clients directly
#![cfg(feature = "templates")]

use std::path::Path;
use std::time::Duration;

use faucet_cli::cli::ServeArgs;
use faucet_cli::serve::config::ServeConfig;
use serde_json::{Value, json};

#[path = "support/ports.rs"]
mod ports;
use ports::free_port;

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
        max_concurrent_runs: Some(2),
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
        cluster_poll_secs: 1,
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

struct Api {
    client: reqwest::Client,
    base: String,
}

impl Api {
    async fn get(&self, path: &str) -> (u16, Value) {
        let r = self
            .client
            .get(format!("{}{path}", self.base))
            .send()
            .await
            .unwrap();
        let code = r.status().as_u16();
        (code, r.json().await.unwrap_or(Value::Null))
    }
    async fn post(&self, path: &str, body: Value) -> (u16, Value) {
        let r = self
            .client
            .post(format!("{}{path}", self.base))
            .json(&body)
            .send()
            .await
            .unwrap();
        let code = r.status().as_u16();
        (code, r.json().await.unwrap_or(Value::Null))
    }
    async fn register(&self, body: String) {
        let (code, v) = self.post("/v1/templates", json!({ "config": body })).await;
        assert_eq!(code, 201, "{v}");
    }
    async fn wait_terminal(&self, run_id: &str) -> Value {
        for _ in 0..800 {
            let (_, r) = self.get(&format!("/v1/runs/{run_id}")).await;
            if matches!(
                r["status"].as_str(),
                Some("completed" | "failed" | "cancelled")
            ) {
                return r;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("run {run_id} did not finish");
    }
}

async fn start(a: ServeArgs) -> Api {
    let port: u16 = a.listen.rsplit(':').next().unwrap().parse().unwrap();
    let mut config = ServeConfig::from_args(a).unwrap();
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
            .map(|r| r.status().is_success())
            .unwrap_or(false)
        {
            return Api { client, base };
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("server did not come up");
}

/// `ok.csv` exists; `missing.csv` does not, so a row reading it fails.
fn data(dir: &Path) -> std::path::PathBuf {
    let d = dir.join("data");
    std::fs::create_dir_all(&d).unwrap();
    std::fs::write(d.join("ok.csv"), "id,name\n1,acme\n").unwrap();
    d
}

/// Rows `a` (fails: its file is missing), `b`, `c` (declares `depends_on: [a]`)
/// and `d`. Nothing but `c` is ordered by the config.
fn config(d: &Path, out: &Path, name: &str) -> String {
    format!(
        r#"version: 1
name: {name}
execution: {{ on_error: continue }}
pipeline:
  source: {{ type: csv, config: {{ path: "{d}/ok.csv" }} }}
  sink: {{ type: jsonl, config: {{ path: "{o}/{name}-x.jsonl" }} }}
matrix:
  - {{ id: a, source: {{ config: {{ path: "{d}/missing.csv" }} }}, sink: {{ config: {{ path: "{o}/{name}-a.jsonl" }} }} }}
  - {{ id: b, sink: {{ config: {{ path: "{o}/{name}-b.jsonl" }} }} }}
  - {{ id: c, depends_on: [a], sink: {{ config: {{ path: "{o}/{name}-c.jsonl" }} }} }}
  - {{ id: d, sink: {{ config: {{ path: "{o}/{name}-d.jsonl" }} }} }}
"#,
        d = d.display(),
        o = out.display()
    )
}

/// Each row's invocation status in a finished run.
fn statuses(run: &Value) -> std::collections::BTreeMap<String, String> {
    run["invocations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| {
            (
                i["row_id"].as_str().unwrap().to_string(),
                i["status"].as_str().unwrap_or("?").to_string(),
            )
        })
        .collect()
}

fn st<'a>(s: &'a std::collections::BTreeMap<String, String>, row: &str) -> &'a str {
    s.get(row).map(String::as_str).unwrap_or("absent")
}

#[tokio::test(flavor = "multi_thread")]
async fn post_runs_orders_rows_by_the_request_edges() {
    let dir = tempfile::tempdir().unwrap();
    let d = data(dir.path());
    let out = dir.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let api = start(args(free_port())).await;
    let cfg = config(&d, &out, "direct");

    // Without the request edge `b` does not wait for the failing `a`.
    let (code, v) = api.post("/v1/runs", json!({ "config": cfg })).await;
    assert_eq!(code, 202, "{v}");
    let run = api.wait_terminal(v["run_id"].as_str().unwrap()).await;
    let s = statuses(&run);
    assert_eq!(st(&s, "b"), "succeeded", "{run}");
    assert_eq!(st(&s, "c"), "skipped", "{run}");
    assert!(run.get("depends_on").is_none(), "{run}");

    // With it, `b` waits for `a` and is skipped when `a` fails; the config's
    // own `c → a` edge still holds, and `d` is untouched.
    let (code, v) = api
        .post(
            "/v1/runs",
            json!({ "config": cfg, "depends_on": { "b": ["a"] } }),
        )
        .await;
    assert_eq!(code, 202, "{v}");
    let run = api.wait_terminal(v["run_id"].as_str().unwrap()).await;
    let s = statuses(&run);
    assert_eq!(st(&s, "a"), "failed", "{run}");
    assert_eq!(st(&s, "b"), "skipped", "{run}");
    assert_eq!(st(&s, "c"), "skipped", "{run}");
    assert_eq!(st(&s, "d"), "succeeded", "{run}");
    assert_eq!(run["depends_on"], json!({ "b": ["a"] }), "{run}");
    assert_eq!(run["labels"]["depends_on"], "b:a", "{run}");
    // The filter finds it by the label.
    let (_, page) = api.get("/v1/runs?label=depends_on").await;
    assert_eq!(page["runs"].as_array().unwrap().len(), 1, "{page}");

    // A label of that name is the server's to set.
    let (code, v) = api
        .post(
            "/v1/runs",
            json!({ "config": cfg, "labels": { "depends_on": "x" } }),
        )
        .await;
    assert_eq!(code, 422, "{v}");

    // An unknown name or a cycle is a 400 naming the edge.
    for (deps, needle) in [
        (json!({ "b": ["ghost"] }), "b → ghost"),
        (json!({ "ghost": ["a"] }), "ghost → a"),
        (json!({ "a": ["c"] }), "a → c"),
        (json!({ "b": ["b"] }), "b → b"),
    ] {
        let (code, v) = api
            .post("/v1/runs", json!({ "config": cfg, "depends_on": deps }))
            .await;
        assert_eq!(code, 400, "{deps}: {v}");
        assert!(v.to_string().contains(needle), "{deps}: {v}");
    }
    // A config with no matrix rows has nothing to order.
    let single = format!(
        "version: 1\npipeline:\n  source: {{ type: csv, config: {{ path: \"{d}/ok.csv\" }} }}\n  sink: {{ type: jsonl, config: {{ path: \"{o}/single.jsonl\" }} }}\n",
        d = d.display(),
        o = out.display()
    );
    let (code, v) = api
        .post(
            "/v1/runs",
            json!({ "config": single, "depends_on": { "a": ["b"] } }),
        )
        .await;
    assert_eq!(code, 400, "{v}");

    // A selection follows the request edges like declared ones.
    let (code, v) = api
        .post(
            "/v1/runs",
            json!({ "config": cfg, "depends_on": { "b": ["d"] }, "selection": { "select": ["b"] } }),
        )
        .await;
    assert_eq!(code, 400, "{v}");
    assert!(v.to_string().contains("dependency violation"), "{v}");
    let (code, v) = api
        .post(
            "/v1/runs",
            json!({ "config": cfg, "depends_on": { "b": ["d"] },
                    "selection": { "select": ["b"], "include_parents": "all" } }),
        )
        .await;
    assert_eq!(code, 202, "{v}");
    let run = api.wait_terminal(v["run_id"].as_str().unwrap()).await;
    let s = statuses(&run);
    assert_eq!(st(&s, "d"), "succeeded", "{run}");
    assert_eq!(st(&s, "b"), "succeeded", "{run}");
    assert_eq!(st(&s, "a"), "absent", "{run}");

    // The same idempotency key with different edges is a different run.
    let key = json!("deps-key");
    let (code, _) = api
        .post(
            "/v1/runs",
            json!({ "config": cfg, "idempotency_key": key, "depends_on": { "b": ["a"] } }),
        )
        .await;
    assert_eq!(code, 202);
    let (code, _) = api
        .post(
            "/v1/runs",
            json!({ "config": cfg, "idempotency_key": key, "depends_on": { "d": ["a"] } }),
        )
        .await;
    assert_eq!(code, 409);

    // A change request shows the edges, and different edges are a
    // different material fingerprint.
    let (code, a) = api
        .post(
            "/v1/runs",
            json!({ "config": cfg, "require_approval": true, "depends_on": { "b": ["a"] } }),
        )
        .await;
    assert_eq!(code, 202, "{a}");
    let (_, b) = api
        .post(
            "/v1/runs",
            json!({ "config": cfg, "require_approval": true, "depends_on": { "d": ["a"] } }),
        )
        .await;
    assert_eq!(
        a["change"]["plan"]["summary"]["depends_on"],
        json!({ "b": ["a"] }),
        "{a}"
    );
    assert_eq!(a["change"]["payload"]["depends_on"], json!({ "b": ["a"] }));
    assert_ne!(
        a["change"]["plan"]["material"],
        b["change"]["plan"]["material"]
    );
}

fn pipeline_template(d: &Path, out: &Path) -> String {
    format!("kind: pipeline\n{}", config(d, out, "tpl"))
}

fn source_template(d: &Path) -> String {
    format!(
        r#"
kind: source-template
name: shop
description: A shop's exports
source:
  type: csv
  config: {{ path: "{d}/ok.csv" }}
streams:
  - name: customers
    description: fails — its file is missing
    source: {{ config: {{ path: "{d}/missing.csv" }} }}
  - name: orders
    description: unordered by the template
  - name: refunds
    description: after orders, by the template
    depends_on: [orders]
"#,
        d = d.display()
    )
}

fn sink_template(out: &Path) -> String {
    format!(
        r#"
kind: sink-template
name: files
description: Local JSON Lines
sink:
  type: jsonl
  config: {{ append: true }}
per_stream:
  path: "{o}/${{stream}}.jsonl"
"#,
        o = out.display()
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn template_runs_take_request_edges() {
    let dir = tempfile::tempdir().unwrap();
    let d = data(dir.path());
    let out = dir.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let api = start(args(free_port())).await;
    api.register(pipeline_template(&d, &out)).await;
    api.register(source_template(&d)).await;
    api.register(sink_template(&out)).await;

    // A pipeline template that declares no `b` edge.
    let (code, v) = api
        .post(
            "/v1/templates/tpl/runs",
            json!({ "version": 1, "depends_on": { "b": ["a"] } }),
        )
        .await;
    assert_eq!(code, 202, "{v}");
    assert_eq!(v["depends_on"], json!({ "b": ["a"] }), "{v}");
    let run = api.wait_terminal(v["run_id"].as_str().unwrap()).await;
    let s = statuses(&run);
    assert_eq!(st(&s, "b"), "skipped", "{run}");
    assert_eq!(st(&s, "c"), "skipped", "{run}");
    assert_eq!(st(&s, "d"), "succeeded", "{run}");
    assert_eq!(run["labels"]["depends_on"], "b:a", "{run}");
    let (code, v) = api
        .post(
            "/v1/templates/tpl/runs",
            json!({ "version": 1, "depends_on": { "b": ["nope"] } }),
        )
        .await;
    assert_eq!(code, 400, "{v}");
    assert!(v.to_string().contains("b → nope"), "{v}");

    // A source template: the request orders `orders` after `customers`; the
    // template's own `refunds → orders` edge holds too.
    let (code, v) = api
        .post(
            "/v1/templates/shop/runs",
            json!({ "version": 1, "sink": "files", "sink_version": 1,
                    "depends_on": { "orders": ["customers"] } }),
        )
        .await;
    assert_eq!(code, 202, "{v}");
    let run = api.wait_terminal(v["run_id"].as_str().unwrap()).await;
    let s = statuses(&run);
    assert_eq!(st(&s, "customers"), "failed", "{run}");
    assert_eq!(st(&s, "orders"), "skipped", "{run}");
    assert_eq!(st(&s, "refunds"), "skipped", "{run}");
    // A cycle through the template's edge.
    let (code, v) = api
        .post(
            "/v1/templates/shop/runs",
            json!({ "version": 1, "sink": "files", "sink_version": 1,
                    "depends_on": { "orders": ["refunds"] } }),
        )
        .await;
    assert_eq!(code, 400, "{v}");
    assert!(v.to_string().contains("orders → refunds"), "{v}");
    // Under a selection the request edge pulls its prerequisite in only as
    // `include_parents` allows.
    let (code, v) = api
        .post(
            "/v1/templates/shop/runs",
            json!({ "version": 1, "sink": "files", "sink_version": 1,
                    "depends_on": { "orders": ["customers"] },
                    "selection": { "select": ["orders"] } }),
        )
        .await;
    assert_eq!(code, 400, "{v}");
    assert!(v.to_string().contains("dependency violation"), "{v}");
    // A request edge on a stream the selection leaves out is dropped with it.
    let (code, v) = api
        .post(
            "/v1/templates/shop/runs",
            json!({ "version": 1, "sink": "files", "sink_version": 1,
                    "depends_on": { "orders": ["customers"] },
                    "selection": { "select": ["customers"] } }),
        )
        .await;
    assert_eq!(code, 202, "{v}");
    assert!(v.get("depends_on").is_none(), "{v}");
    let run = api.wait_terminal(v["run_id"].as_str().unwrap()).await;
    assert_eq!(st(&statuses(&run), "orders"), "absent", "{run}");
}

#[cfg(feature = "serve-history-sqlite")]
#[tokio::test(flavor = "multi_thread")]
async fn a_clustered_run_applies_the_request_edges() {
    let dir = tempfile::tempdir().unwrap();
    let d = data(dir.path());
    let out = dir.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let mut a = args(free_port());
    a.history = Some(format!("sqlite:{}", dir.path().join("h.db").display()));
    a.cluster = true;
    let api = start(a).await;
    let (code, v) = api
        .post(
            "/v1/runs",
            json!({ "config": config(&d, &out, "clustered"), "depends_on": { "b": ["a"] } }),
        )
        .await;
    assert_eq!(code, 202, "{v}");
    assert_eq!(v["status"], "pending");
    let run = api.wait_terminal(v["run_id"].as_str().unwrap()).await;
    let s = statuses(&run);
    assert_eq!(st(&s, "b"), "skipped", "{run}");
    assert_eq!(st(&s, "d"), "succeeded", "{run}");
    assert_eq!(run["depends_on"], json!({ "b": ["a"] }), "{run}");
}
