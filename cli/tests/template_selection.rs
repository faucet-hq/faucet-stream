//! Row selection for registered templates and serve runs (#741): the rows API
//! (`GET /v1/templates/{id}/rows`) with its metadata groups and dry-run
//! resolve, and a `selection` honoured by every trigger path — the template
//! trigger, `POST /v1/runs`, a clustered run, a schedule trigger, a change
//! request, and the CLI.

#![cfg(feature = "templates")]

use std::path::Path;
use std::time::Duration;

use clap::Parser as _;
use faucet_cli::cli::{Cli, ServeArgs};
use faucet_cli::serve::config::ServeConfig;
use serde_json::{Value, json};

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
        no_ui: true,
        cluster: false,
        cluster_poll_secs: 1,
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
        let (code, v) = self
            .post("/v1/templates", json!({ "config": body, "launch": true }))
            .await;
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

/// CSV inputs for every stream / row.
fn data(dir: &Path) -> std::path::PathBuf {
    let d = dir.join("data");
    std::fs::create_dir_all(&d).unwrap();
    std::fs::write(d.join("accounts.csv"), "id,name\n1,acme\n").unwrap();
    std::fs::write(d.join("deals.csv"), "id,amount\n7,10\n").unwrap();
    std::fs::write(d.join("lines-7.csv"), "line,sku\n1,a\n2,b\n").unwrap();
    std::fs::write(d.join("audit.csv"), "id,what\n1,x\n").unwrap();
    d
}

fn source_template(data: &Path) -> String {
    format!(
        r#"
kind: source-template
name: crm
description: A CRM's exports
params:
  data_dir: {{ type: string, default: "{d}" }}
source:
  type: csv
  config:
    path: "${{param.data_dir}}/accounts.csv"
streams:
  - name: accounts
    description: Customer accounts
    primary_keys: [id]
    write: [upsert, append]
  - name: deals
    source: {{ config: {{ path: "${{param.data_dir}}/deals.csv" }} }}
    primary_keys: [id]
    write: [overwrite]
  - name: deal_lines
    parent: deals
    source: {{ config: {{ path: "{d}/lines-${{deals.id}}.csv" }} }}
    write: append
  - name: audit
    source: {{ config: {{ path: "${{param.data_dir}}/audit.csv" }} }}
    primary_keys: [id]
    write: upsert
"#,
        d = data.display()
    )
}

fn sink_template(out: &Path) -> String {
    format!(
        r#"
kind: sink-template
name: files
description: Local JSON Lines
params:
  out_dir: {{ type: string, default: "{o}" }}
sink:
  type: jsonl
  config: {{ append: true }}
per_stream:
  path: "${{param.out_dir}}/${{stream}}.jsonl"
write_mode_aliases:
  overwrite: append
"#,
        o = out.display()
    )
}

/// A pipeline template: `payroll` depends on the parked `audit` row.
fn pipeline_template(data: &Path, out: &Path, state: &Path) -> String {
    format!(
        r#"
kind: pipeline
version: 1
name: hr
pipeline:
  sources:
    csv: {{ type: csv, config: {{ path: "{d}/accounts.csv" }} }}
  sinks:
    out: {{ type: jsonl, config: {{ path: "{o}/hr.jsonl" }} }}
  state: {{ type: file, config: {{ path: "{s}" }} }}
matrix:
  - id: people
    source: {{ ref: csv }}
    sink: {{ ref: out, config: {{ path: "{o}/people.jsonl" }} }}
    tags: [core]
  - id: audit
    source: {{ ref: csv, status: available, config: {{ path: "{d}/audit.csv" }} }}
    sink: {{ ref: out, config: {{ path: "{o}/audit.jsonl" }} }}
  - id: payroll
    source: {{ ref: csv, config: {{ path: "{d}/deals.csv" }} }}
    sink: {{ ref: out, config: {{ path: "{o}/payroll.jsonl" }} }}
    depends_on: [audit]
    tags: [finance]
"#,
        d = data.display(),
        o = out.display(),
        s = state.display()
    )
}

fn topology_template(data: &Path, out: &Path) -> String {
    format!(
        "kind: pipeline\nversion: 1\nname: topo\npipeline:\n  sources:\n    s: {{ type: csv, config: {{ path: \"{}/accounts.csv\" }} }}\n  sinks:\n    o: {{ type: jsonl, config: {{ path: \"{}/topo.jsonl\" }} }}\n  nodes:\n    src: {{ kind: source, ref: s }}\n    w: {{ kind: sink, ref: o }}\n  edges:\n    - {{ from: src, to: w }}\n",
        data.display(),
        out.display()
    )
}

fn row<'a>(report: &'a Value, id: &str) -> &'a Value {
    report["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["id"] == id)
        .unwrap_or_else(|| panic!("no row {id} in {report}"))
}

fn exists(out: &Path, name: &str) -> bool {
    out.join(format!("{name}.jsonl")).exists()
}

#[tokio::test(flavor = "multi_thread")]
async fn rows_api_describes_every_group_and_resolves_selections() {
    let dir = tempfile::tempdir().unwrap();
    let d = data(dir.path());
    let out = dir.path().join("out");
    let state = dir.path().join("state");
    let api = start(args(free_port())).await;
    api.register(source_template(&d)).await;
    api.register(sink_template(&out)).await;
    api.register(pipeline_template(&d, &out, &state)).await;
    api.register(topology_template(&d, &out)).await;

    // A source template without a sink: identity, hierarchy, read, shape,
    // inputs — no sink-dependent facts.
    let (code, r) = api.get("/v1/templates/crm/rows").await;
    assert_eq!(code, 200, "{r}");
    assert_eq!(r["kind"], "source-template");
    assert_eq!(r["version"], 1);
    assert_eq!(r["selectable"], true);
    assert_eq!(r["rows"].as_array().unwrap().len(), 4);
    let accounts = row(&r, "accounts");
    assert_eq!(accounts["kind"], "stream");
    assert_eq!(accounts["description"], "Customer accounts");
    assert_eq!(accounts["status"], "active");
    assert_eq!(accounts["default_selected"], true);
    assert_eq!(accounts["primary_keys"], json!(["id"]));
    assert_eq!(accounts["write"]["requested"], json!(["upsert", "append"]));
    assert!(accounts["write"].get("resolved").is_none());
    assert!(accounts.get("guarantees").is_none());
    assert_eq!(accounts["read"]["source_kind"], "csv");
    assert_eq!(accounts["read"]["replication"]["method"], "full");
    assert_eq!(accounts["params_used"], json!(["data_dir"]));
    assert_eq!(accounts["shape"]["transforms"]["count"], 0);
    assert_eq!(row(&r, "deals")["children"], json!(["deal_lines"]));
    let lines = row(&r, "deal_lines");
    assert_eq!(lines["parent"], "deals");
    assert_eq!(lines["per_parent_record"], true);
    assert_eq!(lines["depth"], 1);
    assert!(
        r["notes"][0]
            .as_str()
            .unwrap()
            .contains("pass a sink template")
    );

    // With a sink: write resolution, aliases, unsupported streams, guarantees.
    let (_, r) = api.get("/v1/templates/crm/rows?sink=files").await;
    assert_eq!(r["sink"], "files");
    assert_eq!(r["sink_version"], 1);
    assert_eq!(r["sink_kind"], "jsonl");
    let accounts = row(&r, "accounts");
    assert_eq!(accounts["write"]["resolved"], "append");
    assert_eq!(accounts["write"]["supported"], true);
    assert_eq!(
        accounts["guarantees"]["delivery_guarantee"],
        "at-least-once"
    );
    assert_eq!(
        row(&r, "deals")["write"]["alias_applied"],
        "overwrite→append"
    );
    let audit = row(&r, "audit");
    assert_eq!(audit["write"]["supported"], false);
    assert!(
        audit["write"]["unsupported_reason"]
            .as_str()
            .unwrap()
            .contains("upsert")
    );

    // Dry-run resolve: a child pulls its parent in under `eligible`…
    let (_, r) = api
        .get("/v1/templates/crm/rows?select=deal_lines&include_parents=eligible&state=false")
        .await;
    assert_eq!(r["run_set"], json!(["deals", "deal_lines"]));
    assert_eq!(row(&r, "deal_lines")["selected"], true);
    assert_eq!(row(&r, "deals")["pulled_in"]["because"], "deal_lines");
    assert!(r.get("error").is_none());
    // …and blocks on it under the default `off`.
    let (_, r) = api
        .get("/v1/templates/crm/rows?select=deal_lines&state=false")
        .await;
    assert!(r["error"].as_str().unwrap().contains("deal_lines"));
    assert!(row(&r, "deals")["blocked"].is_string());
    // An unknown name is reported with the valid ones.
    let (_, r) = api.get("/v1/templates/crm/rows?select=dealz").await;
    let e = r["error"].as_str().unwrap();
    assert!(e.contains("dealz") && e.contains("deal_lines"), "{e}");
    let (code, _) = api.get("/v1/templates/crm/rows?status=nope").await;
    assert_eq!(code, 400);

    // A pipeline template: matrix rows, statuses, tags, depends_on, the write
    // group from the row's sink, and a parked-parent error under `eligible`.
    let (_, r) = api.get("/v1/templates/hr/rows").await;
    assert_eq!(r["kind"], "pipeline");
    let payroll = row(&r, "payroll");
    assert_eq!(payroll["kind"], "row");
    assert_eq!(payroll["depends_on"], json!(["audit"]));
    assert_eq!(payroll["depth"], 1);
    assert_eq!(payroll["tags"], json!(["finance"]));
    assert_eq!(payroll["write"]["resolved"], "append");
    assert_eq!(payroll["guarantees"]["delivery_guarantee"], "at-least-once");
    assert_eq!(row(&r, "audit")["default_selected"], false);
    assert!(row(&r, "people").get("state").is_some(), "{r}");
    let (_, r) = api
        .get("/v1/templates/hr/rows?select=payroll&include_parents=eligible")
        .await;
    assert!(r["error"].as_str().unwrap().contains("parked"));
    assert!(
        row(&r, "audit")["blocked"]
            .as_str()
            .unwrap()
            .contains("parked")
    );
    let (_, r) = api
        .get("/v1/templates/hr/rows?select=payroll&include_parents=all")
        .await;
    assert_eq!(r["run_set"], json!(["audit", "payroll"]));

    // A topology has no rows.
    let (_, r) = api.get("/v1/templates/topo/rows").await;
    assert_eq!(r["selectable"], false);
    assert_eq!(r["rows"], json!([]));

    // A sink template has no rows of its own; an unknown template is a 404.
    let (code, _) = api.get("/v1/templates/files/rows").await;
    assert_eq!(code, 422);
    let (code, _) = api.get("/v1/templates/nope/rows").await;
    assert_eq!(code, 404);
    // A pipeline takes no sink; a sink must be a sink template.
    let (code, _) = api.get("/v1/templates/hr/rows?sink=files").await;
    assert_eq!(code, 422);
    let (code, _) = api.get("/v1/templates/crm/rows?sink=hr").await;
    assert_eq!(code, 422);

    // An overlay's state store backs the state group of a composed listing.
    let ops = format!(
        "kind: deployment\nname: ops\nstate: {{ type: file, config: {{ path: \"{}\" }} }}\n",
        dir.path().join("ops-state").display()
    );
    api.register(ops).await;
    let (code, r) = api
        .get("/v1/templates/crm/rows?sink=files&overlay=ops")
        .await;
    assert_eq!(code, 200, "{r}");
    assert!(row(&r, "accounts")["state"]["health"].is_string(), "{r}");
    assert_eq!(
        row(&r, "accounts")["read"]["resumable"],
        false,
        "a full-refresh CSV stream does not bookmark"
    );
}

#[test]
fn a_topology_refuses_a_selection_on_the_cli() {
    on_big_stack(|| async {
        let dir = tempfile::tempdir().unwrap();
        let d = data(dir.path());
        let out = dir.path().join("out");
        let p = dir.path().join("topo.yaml");
        std::fs::write(&p, topology_template(&d, &out)).unwrap();
        let e = cli(&[
            "run".to_string(),
            p.to_str().unwrap().to_string(),
            "--select".to_string(),
            "src".to_string(),
        ])
        .await
        .unwrap_err();
        assert!(e.to_string().contains("topology"), "{e}");
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn a_selection_runs_the_subset_from_every_http_path() {
    let dir = tempfile::tempdir().unwrap();
    let d = data(dir.path());
    let out = dir.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let state = dir.path().join("state");
    let api = start(args(free_port())).await;
    api.register(source_template(&d)).await;
    api.register(sink_template(&out)).await;
    api.register(pipeline_template(&d, &out, &state)).await;
    api.register(topology_template(&d, &out)).await;

    // The whole pairing cannot run (`audit` wants upsert)…
    let (code, v) = api
        .post("/v1/templates/crm/runs", json!({ "sink": "files" }))
        .await;
    assert_eq!(code, 422, "{v}");
    // …but a selection that leaves it out can.
    let (code, v) = api
        .post(
            "/v1/templates/crm/runs",
            json!({ "sink": "files", "selection": { "select": ["deal_lines"], "include_parents": "eligible" } }),
        )
        .await;
    assert_eq!(code, 202, "{v}");
    assert_eq!(v["selection"]["select"], json!(["deal_lines"]));
    let run = api.wait_terminal(v["run_id"].as_str().unwrap()).await;
    assert_eq!(run["status"], "completed", "{run}");
    assert_eq!(
        run["labels"]["selection"],
        "select=deal_lines;include_parents=eligible"
    );
    assert!(exists(&out, "deals") && exists(&out, "deal_lines"));
    assert!(!exists(&out, "accounts"));

    // Unknown stream → 400 naming the valid ones.
    let (code, v) = api
        .post(
            "/v1/templates/crm/runs",
            json!({ "sink": "files", "selection": { "select": ["dealz"] } }),
        )
        .await;
    assert_eq!(code, 400, "{v}");
    assert!(v.to_string().contains("accounts"), "{v}");

    // A pipeline template: only `people` runs, and only its state moves.
    let (code, v) = api
        .post(
            "/v1/templates/hr/runs",
            json!({ "selection": { "select": ["people"] } }),
        )
        .await;
    assert_eq!(code, 202, "{v}");
    let run = api.wait_terminal(v["run_id"].as_str().unwrap()).await;
    assert_eq!(run["status"], "completed", "{run}");
    assert!(exists(&out, "people"));
    assert!(!exists(&out, "payroll") && !exists(&out, "audit"));
    let (_, rows) = api.get("/v1/templates/hr/rows").await;
    assert!(row(&rows, "people")["state"]["last_success"].is_string());
    assert!(row(&rows, "payroll")["state"]["last_success"].is_null());

    // A dependency violation is refused with the ancestors named.
    let (code, v) = api
        .post(
            "/v1/templates/hr/runs",
            json!({ "selection": { "select": ["payroll"] } }),
        )
        .await;
    assert_eq!(code, 400);
    assert!(v.to_string().contains("audit"), "{v}");

    // A topology template refuses any selection.
    let (code, v) = api
        .post(
            "/v1/templates/topo/runs",
            json!({ "selection": { "select": ["src"] } }),
        )
        .await;
    assert_eq!(code, 400, "{v}");

    // POST /v1/runs takes the same selection.
    let config = format!(
        "version: 1\nname: direct\npipeline:\n  source: {{ type: csv, config: {{ path: \"{d}/accounts.csv\" }} }}\n  sink: {{ type: jsonl, config: {{ path: \"{o}/direct-x.jsonl\" }} }}\nmatrix:\n  - {{ id: one, sink: {{ config: {{ path: \"{o}/direct-one.jsonl\" }} }} }}\n  - {{ id: two, sink: {{ config: {{ path: \"{o}/direct-two.jsonl\" }} }} }}\n",
        d = d.display(),
        o = out.display()
    );
    let (code, v) = api
        .post(
            "/v1/runs",
            json!({ "config": config, "selection": { "skip": ["one"] } }),
        )
        .await;
    assert_eq!(code, 202, "{v}");
    let run = api.wait_terminal(v["run_id"].as_str().unwrap()).await;
    assert_eq!(run["labels"]["selection"], "skip=one");
    assert!(exists(&out, "direct-two") && !exists(&out, "direct-one"));
    let (code, _) = api
        .post(
            "/v1/runs",
            json!({ "config": config, "selection": { "select": ["three"] } }),
        )
        .await;
    assert_eq!(code, 400);
    // The same idempotency key with a different subset is a conflict.
    let key = json!("same-key");
    let (code, _) = api
        .post(
            "/v1/runs",
            json!({ "config": config, "idempotency_key": key, "selection": { "select": ["one"] } }),
        )
        .await;
    assert_eq!(code, 202);
    let (code, _) = api
        .post(
            "/v1/runs",
            json!({ "config": config, "idempotency_key": key, "selection": { "select": ["two"] } }),
        )
        .await;
    assert_eq!(code, 409);

    // A change request carries the selection in its plan, and a different
    // subset is a different material fingerprint.
    let (code, a) = api
        .post(
            "/v1/runs",
            json!({ "config": config, "require_approval": true, "selection": { "select": ["one"] } }),
        )
        .await;
    assert_eq!(code, 202, "{a}");
    let (_, b) = api
        .post(
            "/v1/runs",
            json!({ "config": config, "require_approval": true, "selection": { "select": ["two"] } }),
        )
        .await;
    assert_eq!(a["change"]["plan"]["summary"]["selection"], "select=one");
    assert_ne!(
        a["change"]["plan"]["material"],
        b["change"]["plan"]["material"]
    );
}

#[cfg(feature = "serve-history-sqlite")]
#[tokio::test(flavor = "multi_thread")]
async fn a_clustered_run_applies_the_stored_selection() {
    let dir = tempfile::tempdir().unwrap();
    let d = data(dir.path());
    let out = dir.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let state = dir.path().join("state");
    let mut a = args(free_port());
    a.history = Some(format!("sqlite:{}", dir.path().join("h.db").display()));
    a.cluster = true;
    let api = start(a).await;
    api.register(source_template(&d)).await;
    api.register(sink_template(&out)).await;
    api.register(pipeline_template(&d, &out, &state)).await;

    let (code, v) = api
        .post(
            "/v1/templates/hr/runs",
            json!({ "selection": { "select": ["payroll"], "include_parents": "all" } }),
        )
        .await;
    assert_eq!(code, 202, "{v}");
    assert_eq!(v["status"], "pending");
    let run = api.wait_terminal(v["run_id"].as_str().unwrap()).await;
    assert_eq!(run["status"], "completed", "{run}");
    assert!(exists(&out, "payroll") && exists(&out, "audit"));
    assert!(!exists(&out, "people"));

    let (code, v) = api
        .post(
            "/v1/templates/crm/runs",
            json!({ "sink": "files", "selection": { "select": ["accounts"] } }),
        )
        .await;
    assert_eq!(code, 202, "{v}");
    let run = api.wait_terminal(v["run_id"].as_str().unwrap()).await;
    assert_eq!(run["status"], "completed", "{run}");
    assert!(exists(&out, "accounts") && !exists(&out, "deals"));
}

#[cfg(all(feature = "triggers", feature = "schedule"))]
#[tokio::test(flavor = "multi_thread")]
async fn a_schedule_trigger_runs_its_selection() {
    let dir = tempfile::tempdir().unwrap();
    let d = data(dir.path());
    let out = dir.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let triggers = dir.path().join("triggers.yaml");
    std::fs::write(
        &triggers,
        "version: 1\ntriggers:\n  - name: hourly-cheap\n    type: schedule\n    cron: \"* * * * * *\"\n    template: { id: crm, sink: files }\n    run:\n      selection: { select: [accounts] }\n",
    )
    .unwrap();
    let mut a = args(free_port());
    a.triggers = Some(triggers);
    let api = start(a).await;
    api.register(source_template(&d)).await;
    api.register(sink_template(&out)).await;
    let mut done = false;
    for _ in 0..400 {
        let (_, page) = api.get("/v1/runs").await;
        if page["runs"].as_array().unwrap().iter().any(|r| {
            r["status"] == "completed"
                && r["labels"]["faucet.trigger.name"] == "hourly-cheap"
                && r["labels"]["selection"] == "select=accounts"
        }) {
            done = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(done, "no scheduled subset run completed");
    assert!(exists(&out, "accounts") && !exists(&out, "deals"));
}

async fn cli(args: &[String]) -> Result<(), faucet_cli::error::CliError> {
    let mut argv = vec!["faucet".to_string()];
    argv.extend_from_slice(args);
    let parsed = Cli::try_parse_from(argv).expect("argv parses");
    Box::pin(faucet_cli::run_command(parsed)).await
}

fn on_big_stack<F>(f: impl FnOnce() -> F + Send + 'static)
where
    F: std::future::Future<Output = ()>,
{
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(f())
        })
        .unwrap()
        .join()
        .unwrap();
}

#[cfg(feature = "serve-history-sqlite")]
#[test]
fn the_cli_lists_rows_and_runs_a_subset() {
    on_big_stack(|| async {
        let dir = tempfile::tempdir().unwrap();
        let d = data(dir.path());
        let out = dir.path().join("out");
        std::fs::create_dir_all(&out).unwrap();
        let state = dir.path().join("state");
        let store = format!("sqlite:{}", dir.path().join("t.db").display());
        let files = [
            ("crm.yaml", source_template(&d)),
            ("files.yaml", sink_template(&out)),
            ("hr.yaml", pipeline_template(&d, &out, &state)),
        ];
        for (name, body) in &files {
            let p = dir.path().join(name);
            std::fs::write(&p, body).unwrap();
            let a: Vec<String> = [
                "template",
                "register",
                p.to_str().unwrap(),
                "--launch",
                "--store",
                &store,
            ]
            .iter()
            .map(|s| s.to_string())
            .collect();
            cli(&a).await.unwrap();
        }
        let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        cli(&s(&[
            "template", "rows", "crm", "--sink", "files", "--store", &store,
        ]))
        .await
        .unwrap();
        cli(&s(&[
            "template",
            "rows",
            "hr",
            "--json",
            "--no-state",
            "--store",
            &store,
        ]))
        .await
        .unwrap();
        // A resolve whose selection would be refused exits non-zero.
        assert!(
            cli(&s(&[
                "template", "rows", "hr", "--select", "payroll", "--store", &store
            ]))
            .await
            .is_err()
        );
        cli(&s(&[
            "template",
            "run",
            "crm",
            "--sink",
            "files",
            "--select",
            "deal_lines",
            "--include-parents",
            "eligible",
            "--store",
            &store,
        ]))
        .await
        .unwrap();
        assert!(exists(&out, "deals") && exists(&out, "deal_lines"));
        assert!(!exists(&out, "accounts"));
        cli(&s(&[
            "template", "run", "hr", "--select", "people", "--store", &store,
        ]))
        .await
        .unwrap();
        assert!(exists(&out, "people") && !exists(&out, "payroll"));

        // `faucet hub rows` over a catalog directory and a pipeline file.
        let hub = dir.path().join("hub");
        std::fs::create_dir_all(hub.join("source-templates")).unwrap();
        std::fs::create_dir_all(hub.join("sink-templates")).unwrap();
        std::fs::write(hub.join("source-templates/crm.yaml"), source_template(&d)).unwrap();
        std::fs::write(hub.join("sink-templates/files.yaml"), sink_template(&out)).unwrap();
        let hub = hub.to_str().unwrap().to_string();
        cli(&s(&[
            "hub", "rows", "crm", "--sink", "files", "--hub", &hub,
        ]))
        .await
        .unwrap();
        cli(&s(&[
            "hub",
            "rows",
            "crm",
            "--json",
            "--select",
            "deal_lines",
            "--include-parents",
            "eligible",
            "--hub",
            &hub,
        ]))
        .await
        .unwrap();
        let hr = dir.path().join("hr.yaml");
        cli(&s(&["hub", "rows", hr.to_str().unwrap(), "--state"]))
            .await
            .unwrap();
        assert!(
            cli(&s(&[
                "hub",
                "rows",
                hr.to_str().unwrap(),
                "--sink",
                "files"
            ]))
            .await
            .is_err()
        );
    });
}
