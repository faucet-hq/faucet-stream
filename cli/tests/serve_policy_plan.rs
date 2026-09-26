//! Control-plane tests for data-flow policies (#702) and plan / impact (#707):
//! `faucet serve --policy` refuses a violating submission (422 + a
//! `policy.denied` audit entry), warns on a violating template registration,
//! audits a runtime backstop denial, serves `POST /v1/plan` to a viewer, and
//! annotates datasets with owners / consumers (`operator`+) that
//! `POST /v1/plan { impact: true }` then reports.
#![cfg(all(
    feature = "serve",
    feature = "policy",
    feature = "source-csv",
    feature = "sink-jsonl"
))]

use serde_json::{Value, json};
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
    \x20 - name: oscar\n\
    \x20   token: op-tok\n\
    \x20   role: operator\n\
    \x20 - name: bob\n\
    \x20   token: viewer-tok\n\
    \x20   role: viewer\n";

const POLICY: &str = r#"
version: 1
classifications:
  - label: pii
    fields: [email]
    value_detector: email
rules:
  - name: pii-eu
    when: { label: pii }
    require: { residency: [eu] }
"#;

fn serve_args(
    port: u16,
    auth_config: std::path::PathBuf,
    policy: Option<std::path::PathBuf>,
) -> faucet_cli::cli::ServeArgs {
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
        policy,
        callback_allow_host: Vec::new(),
        mcp: false,
        mcp_allow_mutations: false,
        require_approval: Vec::new(),
        approval_expiry_secs: 86_400,
        vault_key: None,
        vault_previous_key: Vec::new(),
        connect_providers: None,
    }
}

async fn spawn_server(port: u16, dir: &std::path::Path, with_policy: bool) {
    let auth_path = dir.join("auth.yaml");
    std::fs::write(&auth_path, AUTH_CONFIG).unwrap();
    let policy = with_policy.then(|| {
        let p = dir.join("policy.yaml");
        std::fs::write(&p, POLICY).unwrap();
        p
    });
    let mut config =
        faucet_cli::serve::ServeConfig::from_args(serve_args(port, auth_path, policy)).unwrap();
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

/// Submit and wait; returns the terminal record.
async fn run_config(base: &str, client: &reqwest::Client, config: &str) -> Value {
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

async fn audit(base: &str, client: &reqwest::Client, action: &str) -> Vec<Value> {
    let v: Value = client
        .get(format!("{base}/v1/audit?action={action}"))
        .bearer_auth("admin-tok")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    v["entries"]
        .as_array()
        .cloned()
        .unwrap_or_else(|| v.as_array().cloned().unwrap_or_default())
}

fn csv_to_jsonl(name: &str, input: &str, output: &str, contract: bool, attrs: &str) -> String {
    format!(
        "version: 1\nname: {name}\npipeline:\n  source: {{ type: csv, config: {{ path: {input} }} }}\n{contract}  sink:\n    type: jsonl\n{attrs}    config: {{ path: {output} }}\n",
        contract = if contract {
            "  contract:\n    version: \"1\"\n    fields:\n      - { name: id, type: string }\n      - { name: email, type: string }\n"
        } else {
            ""
        },
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn policy_refuses_submissions_warns_on_templates_and_audits_runtime_denials() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    spawn_server(port, dir.path(), true).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");
    let input = dir.path().join("in.csv").display().to_string();
    let output = dir.path().join("out.jsonl").display().to_string();
    std::fs::write(dir.path().join("in.csv"), "id,email\n1,a@x.io\n").unwrap();

    // A contract names `email`; the sink has no residency → refused, 422.
    let violating = csv_to_jsonl("v", &input, &output, true, "");
    let resp = client
        .post(format!("{base}/v1/runs"))
        .bearer_auth("admin-tok")
        .json(&json!({ "config": violating }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 422);
    let body: Value = resp.json().await.unwrap();
    let text = body.to_string();
    assert!(text.contains("pii-eu"), "{text}");
    assert!(
        body["details"]["violations"] == 1 || body["error"]["details"]["violations"] == 1,
        "{body}"
    );
    let denied = audit(&base, &client, "policy.denied").await;
    assert_eq!(denied.len(), 1, "{denied:?}");
    assert_eq!(denied[0]["principal"], "alice");
    assert_eq!(denied[0]["result"], "denied");
    assert!(!dir.path().join("out.jsonl").exists());

    // The compliant twin runs.
    let compliant = csv_to_jsonl(
        "ok",
        &input,
        &output,
        true,
        "    attributes: { residency: eu }\n",
    );
    let rec = run_config(&base, &client, &compliant).await;
    assert_eq!(rec["status"], "completed", "{rec}");

    // Runtime backstop: no contract (nothing known statically), but the
    // `mail` column carries an address the detector catches → the run fails
    // and the denial is audited as `runtime`.
    std::fs::write(dir.path().join("rt.csv"), "id,mail\n1,alice@example.com\n").unwrap();
    let rt = csv_to_jsonl(
        "rt",
        &dir.path().join("rt.csv").display().to_string(),
        &dir.path().join("rt.jsonl").display().to_string(),
        false,
        "",
    );
    let rec = run_config(&base, &client, &rt).await;
    assert_eq!(rec["status"], "failed", "{rec}");
    assert!(
        rec["error"].as_str().unwrap_or("").contains("pii-eu")
            || rec["invocations"][0]["error"]
                .as_str()
                .unwrap_or("")
                .contains("pii-eu"),
        "{rec}"
    );
    let denied = audit(&base, &client, "policy.denied").await;
    assert_eq!(denied.len(), 2, "{denied:?}");
    let runtime = denied
        .iter()
        .find(|e| e["principal"] == "runtime")
        .expect("runtime denial audited");
    assert_eq!(runtime["run_id"], rec["run_id"]);
    assert!(runtime["result"].as_str().unwrap().starts_with("denied"));

    // A viewer can plan the violating config: the verdict is in the report.
    let resp = client
        .post(format!("{base}/v1/plan"))
        .bearer_auth("viewer-tok")
        .json(&json!({ "config": violating, "sample": [{ "id": "1", "email": "a@x.io" }] }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let plan: Value = resp.json().await.unwrap();
    assert_eq!(plan["row"], "row-0");
    assert_eq!(plan["sample"]["output_records"], 1);
    assert_eq!(plan["policy"]["violations"].as_array().unwrap().len(), 1);
    assert_eq!(plan["policy"]["column_source"], "schema");

    // Registering a violating pipeline template warns (it may still be paired
    // with a compliant sink later); triggering it would hit the submit gate.
    #[cfg(feature = "templates")]
    {
        let resp = client
            .post(format!("{base}/v1/templates"))
            .bearer_auth("admin-tok")
            .json(&json!({ "config": violating, "id": "violating" }))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status().as_u16(),
            201,
            "{}",
            resp.text().await.unwrap()
        );
        let summary: Value = resp.json().await.unwrap();
        let warnings = summary["warnings"].as_array().expect("warnings");
        assert!(
            warnings
                .iter()
                .any(|w| w.as_str().unwrap().contains("pii-eu")),
            "{summary}"
        );
        let resp = client
            .post(format!("{base}/v1/templates/violating/runs"))
            .bearer_auth("admin-tok")
            .json(&json!({}))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status().as_u16(),
            422,
            "{}",
            resp.text().await.unwrap()
        );
    }
}

#[cfg(all(feature = "catalog", feature = "sink-csv"))]
#[tokio::test(flavor = "multi_thread")]
async fn consumers_endpoint_and_plan_impact_over_chained_pipelines() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    spawn_server(port, dir.path(), false).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");
    let in_csv = dir.path().join("in.csv");
    let a_csv = dir.path().join("a.csv");
    let b_jsonl = dir.path().join("b.jsonl");
    std::fs::write(&in_csv, "id,email,amount\n1,a@x.io,10\n2,b@x.io,20\n").unwrap();

    // A: in.csv → a.csv ; B: a.csv → b.jsonl (renames email → contact, with a contract).
    let a = format!(
        "version: 1\nname: a\npipeline:\n  source: {{ type: csv, config: {{ path: {} }} }}\n  sink: {{ type: csv, config: {{ path: {} }} }}\n",
        in_csv.display(),
        a_csv.display()
    );
    let b = format!(
        "version: 1\nname: b\npipeline:\n  source: {{ type: csv, config: {{ path: {} }} }}\n  transforms:\n    - type: rename_field\n      config: {{ fields: {{ email: contact }} }}\n  contract:\n    version: \"3\"\n    fields:\n      - {{ name: id, type: string }}\n      - {{ name: contact, type: string }}\n  sink: {{ type: jsonl, config: {{ path: {} }} }}\n",
        a_csv.display(),
        b_jsonl.display()
    );
    assert_eq!(run_config(&base, &client, &a).await["status"], "completed");
    assert_eq!(run_config(&base, &client, &b).await["status"], "completed");

    // Find B's sink dataset id.
    let page: Value = client
        .get(format!("{base}/v1/catalog/datasets"))
        .bearer_auth("viewer-tok")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let b_sink = page["datasets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|d| d["uri"].as_str().unwrap().ends_with("b.jsonl"))
        .expect("b.jsonl catalogued");
    let b_id = b_sink["id"].as_str().unwrap().to_string();

    // RBAC: a viewer cannot annotate; an operator can; unknown id → 404; an
    // empty annotation → 422.
    let ann = json!({
        "owners": ["team-b"],
        "consumers": [
            { "name": "contacts-dashboard", "kind": "dashboard", "contact": "#bi", "columns": ["contact"] },
            { "name": "id-export", "columns": ["id"] }
        ]
    });
    let resp = client
        .post(format!("{base}/v1/catalog/datasets/{b_id}/consumers"))
        .bearer_auth("viewer-tok")
        .json(&ann)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 403);
    let resp = client
        .post(format!("{base}/v1/catalog/datasets/{b_id}/consumers"))
        .bearer_auth("op-tok")
        .json(&ann)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        200,
        "{}",
        resp.text().await.unwrap()
    );
    let detail: Value = resp.json().await.unwrap();
    assert_eq!(detail["owners"], json!(["team-b"]));
    assert_eq!(detail["consumers"].as_array().unwrap().len(), 2);
    assert_eq!(detail["consumers"][0]["registered_by"], "oscar");
    let resp = client
        .post(format!(
            "{base}/v1/catalog/datasets/0000000000000000/consumers"
        ))
        .bearer_auth("op-tok")
        .json(&ann)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 404);
    let resp = client
        .post(format!("{base}/v1/catalog/datasets/{b_id}/consumers"))
        .bearer_auth("op-tok")
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 422);
    // Upsert by name replaces; `replace` drops the unlisted one.
    let resp = client
        .post(format!("{base}/v1/catalog/datasets/{b_id}/consumers"))
        .bearer_auth("admin-tok")
        .json(&json!({ "consumers": [{ "name": "id-export", "kind": "export", "columns": ["id"] }], "replace": true }))
        .send()
        .await
        .unwrap();
    let detail: Value = resp.json().await.unwrap();
    assert_eq!(detail["consumers"].as_array().unwrap().len(), 1);
    assert_eq!(detail["consumers"][0]["kind"], "export");
    assert_eq!(detail["owners"], json!(["team-b"]), "owners untouched");
    // Put the dashboard back for the impact check.
    client
        .post(format!("{base}/v1/catalog/datasets/{b_id}/consumers"))
        .bearer_auth("admin-tok")
        .json(&json!({ "consumers": [{ "name": "contacts-dashboard", "kind": "dashboard", "columns": ["contact"] }] }))
        .send()
        .await
        .unwrap();
    let annotated = audit(&base, &client, "catalog.annotate").await;
    assert!(annotated.len() >= 3, "{annotated:?}");

    // Plan A with a sample that drops `email`: B's sink (contact reads email)
    // is breaking, its contract v3 is named, its owner + the dashboard listed.
    let resp = client
        .post(format!("{base}/v1/plan"))
        .bearer_auth("viewer-tok")
        .json(&json!({
            "config": a,
            "sample": [{ "id": "1", "amount": "10" }],
            "impact": true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        200,
        "{}",
        resp.text().await.unwrap()
    );
    let plan: Value = resp.json().await.unwrap();
    let impact = &plan["impact"];
    assert_eq!(impact["severity"], "breaking", "{impact}");
    assert_eq!(impact["delta"]["removed"], json!(["email"]));
    let affected = impact["affected"].as_array().unwrap();
    assert_eq!(affected.len(), 2, "{impact}");
    let b_hit = &affected[1];
    assert_eq!(b_hit["id"], b_id);
    assert_eq!(b_hit["severity"], "breaking");
    assert_eq!(b_hit["columns"][0]["column"], "contact");
    assert_eq!(b_hit["contract"]["version"], "3");
    assert_eq!(b_hit["owners"], json!(["team-b"]));
    assert_eq!(b_hit["consumers"][0]["name"], "contacts-dashboard");
    assert_eq!(impact["owners"], json!(["team-b"]));

    // Adding a column is additive and stops at A's own sink.
    let resp = client
        .post(format!("{base}/v1/plan"))
        .bearer_auth("viewer-tok")
        .json(&json!({
            "config": a,
            "sample": [{ "id": "1", "email": "a@x.io", "amount": "10", "country": "fr" }],
            "impact": true
        }))
        .send()
        .await
        .unwrap();
    let plan: Value = resp.json().await.unwrap();
    assert_eq!(plan["impact"]["severity"], "additive", "{plan}");
    assert_eq!(plan["impact"]["affected"].as_array().unwrap().len(), 1);
    let planned = audit(&base, &client, "plan").await;
    assert!(planned.len() >= 2, "{planned:?}");
}
