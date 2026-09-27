//! Change requests end to end (#703): plan → approve → run over a real RBAC
//! server — proposing, the approval policy (roles, self-approval), execution
//! with a budget, rejection, invalidation when the world moves, template
//! kinds, `--require-approval run`, the MCP `propose_run` tool, and the audit
//! trail.
#![cfg(all(
    feature = "templates",
    feature = "mcp",
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

/// alice = admin, bob + dave = operators, carol = viewer. Runs may be approved
/// by any operator or admin except the requester; template changes by admins.
const AUTH_CONFIG: &str = "principals:\n\
    \x20 - { name: alice, token: admin-tok, role: admin }\n\
    \x20 - { name: bob, token: bob-tok, role: operator }\n\
    \x20 - { name: dave, token: dave-tok, role: operator }\n\
    \x20 - { name: carol, token: viewer-tok, role: viewer }\n\
    approvals:\n\
    \x20 expire_secs: 3600\n\
    \x20 rules:\n\
    \x20   - { kinds: [run], roles: [operator, admin], self_approve: false }\n\
    \x20   - { kinds: [template_register, template_launch], roles: [admin] }\n";

fn serve_args(
    port: u16,
    auth_config: std::path::PathBuf,
    require_approval: Vec<String>,
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
        policy: None,
        callback_allow_host: Vec::new(),
        mcp: false,
        mcp_allow_mutations: false,
        require_approval,
        approval_expiry_secs: 86_400,
        vault_key: None,
        vault_previous_key: Vec::new(),
        connect_providers: None,
    }
}

async fn spawn_server(port: u16, dir: &std::path::Path, require_approval: Vec<String>) {
    let auth_path = dir.join("auth.yaml");
    std::fs::write(&auth_path, AUTH_CONFIG).unwrap();
    let mut config =
        faucet_cli::serve::ServeConfig::from_args(serve_args(port, auth_path, require_approval))
            .unwrap();
    config.log_level = "warn".into();
    tokio::spawn(async move {
        let _ = faucet_cli::serve::run_server(
            config,
            faucet_cli::serve::McpServeSettings {
                enabled: true,
                allow_mutations: true,
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
    async fn post(&self, token: &str, path: &str, body: Value) -> (u16, Value) {
        let r = self
            .client
            .post(format!("{}{path}", self.base))
            .bearer_auth(token)
            .json(&body)
            .send()
            .await
            .unwrap();
        let status = r.status().as_u16();
        let text = r.text().await.unwrap();
        let v = serde_json::from_str(&text).unwrap_or(Value::String(text));
        (status, v)
    }
    async fn get(&self, token: &str, path: &str) -> (u16, Value) {
        let r = self
            .client
            .get(format!("{}{path}", self.base))
            .bearer_auth(token)
            .send()
            .await
            .unwrap();
        let status = r.status().as_u16();
        let text = r.text().await.unwrap();
        let v = serde_json::from_str(&text).unwrap_or(Value::String(text));
        (status, v)
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
}

fn csv_config(input: &std::path::Path, output: &std::path::Path, name: &str) -> String {
    format!(
        "version: 1\nname: {name}\npipeline:\n  source: {{ type: csv, config: {{ path: {} }} }}\n  sink: {{ type: jsonl, config: {{ path: {} }} }}\n",
        input.display(),
        output.display()
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn plan_approve_run_with_policy_budget_rejection_and_audit() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    std::fs::write(&input, "id,name\n1,alice\n2,bob\n3,carol\n").unwrap();
    let output = dir.path().join("out.jsonl");
    let port = free_port();
    spawn_server(port, dir.path(), Vec::new()).await;
    let api = Api {
        base: format!("http://127.0.0.1:{port}"),
        client: reqwest::Client::new(),
    };
    let config = csv_config(&input, &output, "chg-e2e");

    // A viewer may read but not propose.
    let (code, _) = api
        .post(
            "viewer-tok",
            "/v1/changes",
            json!({ "kind": "run", "payload": { "config": config }, "reason": "x" }),
        )
        .await;
    assert_eq!(code, 403);

    // bob proposes a run with a budget; the plan is computed up front.
    let (code, change) = api
        .post(
            "bob-tok",
            "/v1/changes",
            json!({
                "kind": "run",
                "payload": { "config": config, "name": "approved-run", "labels": { "team": "data" } },
                "reason": "ship the customer export",
                "budget": { "max_records": 10 }
            }),
        )
        .await;
    assert_eq!(code, 201, "{change}");
    let id = change["id"].as_str().unwrap().to_string();
    assert_eq!(change["status"], "pending");
    assert_eq!(change["requester"], "bob");
    assert_eq!(change["required_approvals"], 1);
    assert_eq!(change["plan"]["summary"]["rows"], 1, "{change}");
    assert_eq!(change["plan"]["rows"].as_array().unwrap().len(), 1);
    assert_eq!(change["plan"]["rows"][0]["sink"], "jsonl");
    assert_eq!(change["budget"]["max_records"], 10);
    assert!(change["expires_at"].is_string());

    // Listing strips plan rows; the detail keeps them; a viewer may read both.
    let (code, list) = api.get("viewer-tok", "/v1/changes?status=pending").await;
    assert_eq!(code, 200);
    assert_eq!(list.as_array().unwrap().len(), 1);
    assert!(list[0]["plan"].get("rows").is_none(), "{list}");
    let (_, detail) = api.get("viewer-tok", &format!("/v1/changes/{id}")).await;
    assert_eq!(detail["plan"]["rows"].as_array().unwrap().len(), 1);

    // Approval policy: a viewer cannot reach the route; the requester may not
    // approve their own request.
    let (code, _) = api
        .post(
            "viewer-tok",
            &format!("/v1/changes/{id}/approve"),
            json!({}),
        )
        .await;
    assert_eq!(code, 403);
    let (code, err) = api
        .post("bob-tok", &format!("/v1/changes/{id}/approve"), json!({}))
        .await;
    assert_eq!(code, 403, "{err}");
    assert!(
        err["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("self-approval"),
        "{err}"
    );

    // Another operator approves: the request executes and the run starts.
    let (code, executed) = api
        .post(
            "dave-tok",
            &format!("/v1/changes/{id}/approve"),
            json!({ "comment": "looks right" }),
        )
        .await;
    assert_eq!(code, 200, "{executed}");
    assert_eq!(executed["status"], "executed", "{executed}");
    assert_eq!(executed["approvals"][0]["principal"], "dave");
    assert_eq!(executed["approvals"][0]["comment"], "looks right");
    let run_id = executed["run_id"].as_str().expect("run id").to_string();
    let rec = api.wait_run(&run_id).await;
    assert_eq!(rec["status"], "completed", "{rec}");
    assert_eq!(rec["labels"]["change"], id);
    assert_eq!(rec["labels"]["team"], "data");
    assert_eq!(rec["invocations"][0]["records_written"], 3);
    assert_eq!(std::fs::read_to_string(&output).unwrap().lines().count(), 3);
    // Approving again is a conflict: the request is no longer pending.
    let (code, _) = api
        .post("dave-tok", &format!("/v1/changes/{id}/approve"), json!({}))
        .await;
    assert_eq!(code, 409);

    // A budget that the run would cross stops it: the page is refused whole.
    std::fs::remove_file(&output).ok();
    let (_, change) = api
        .post(
            "bob-tok",
            "/v1/changes",
            json!({
                "kind": "run",
                "payload": { "config": config },
                "reason": "tiny budget",
                "budget": { "max_records": 2 }
            }),
        )
        .await;
    let id2 = change["id"].as_str().unwrap().to_string();
    let (_, executed) = api
        .post("dave-tok", &format!("/v1/changes/{id2}/approve"), json!({}))
        .await;
    assert_eq!(executed["status"], "executed", "{executed}");
    let rec = api.wait_run(executed["run_id"].as_str().unwrap()).await;
    assert_eq!(rec["status"], "failed", "{rec}");
    assert!(
        rec["invocations"][0]["error"]
            .as_str()
            .unwrap_or_default()
            .contains("max_records"),
        "{rec}"
    );
    assert!(!output.exists() || std::fs::read_to_string(&output).unwrap().trim().is_empty());

    // Rejection, with a reason; then approving is a conflict.
    let (_, change) = api
        .post(
            "bob-tok",
            "/v1/changes",
            json!({ "kind": "run", "payload": { "config": config }, "reason": "again" }),
        )
        .await;
    let id3 = change["id"].as_str().unwrap().to_string();
    let (code, _) = api
        .post("dave-tok", &format!("/v1/changes/{id3}/reject"), json!({}))
        .await;
    assert_eq!(code, 422, "the body must carry a reason");
    let (code, _) = api
        .post(
            "dave-tok",
            &format!("/v1/changes/{id3}/reject"),
            json!({ "reason": "  " }),
        )
        .await;
    assert_eq!(code, 400, "a blank reason is refused");
    let (code, rejected) = api
        .post(
            "dave-tok",
            &format!("/v1/changes/{id3}/reject"),
            json!({ "reason": "not this week" }),
        )
        .await;
    assert_eq!(code, 200, "{rejected}");
    assert_eq!(rejected["status"], "rejected");
    assert_eq!(rejected["rejection"]["reason"], "not this week");
    let (code, _) = api
        .post("dave-tok", &format!("/v1/changes/{id3}/approve"), json!({}))
        .await;
    assert_eq!(code, 409);
    // The requester may withdraw their own.
    let (_, change) = api
        .post(
            "bob-tok",
            "/v1/changes",
            json!({ "kind": "run", "payload": { "config": config }, "reason": "oops" }),
        )
        .await;
    let id4 = change["id"].as_str().unwrap().to_string();
    let (code, _) = api
        .post(
            "bob-tok",
            &format!("/v1/changes/{id4}/reject"),
            json!({ "reason": "withdrawn" }),
        )
        .await;
    assert_eq!(code, 200);

    // Template kinds: a register proposed by an operator needs an admin.
    let tpl = "kind: pipeline\nversion: 1\nname: tpl-a\npipeline:\n  source: { type: csv, config: { path: ./in.csv } }\n  sink: { type: jsonl, config: { path: ./out.jsonl } }\n";
    let (code, change) = api
        .post(
            "bob-tok",
            "/v1/changes",
            json!({ "kind": "template_register", "payload": { "config": tpl }, "reason": "new template" }),
        )
        .await;
    assert_eq!(code, 201, "{change}");
    let tid = change["id"].as_str().unwrap().to_string();
    assert_eq!(change["plan"]["summary"]["template"], "tpl-a");
    assert_eq!(change["plan"]["summary"]["template_kind"], "pipeline");
    assert!(change["plan"]["summary"]["previous_version"].is_null());
    assert_eq!(change["plan"]["rows"].as_array().unwrap().len(), 1);
    let (code, err) = api
        .post("dave-tok", &format!("/v1/changes/{tid}/approve"), json!({}))
        .await;
    assert_eq!(code, 403, "{err}");
    let (code, executed) = api
        .post(
            "admin-tok",
            &format!("/v1/changes/{tid}/approve"),
            json!({}),
        )
        .await;
    assert_eq!(code, 200, "{executed}");
    assert_eq!(executed["status"], "executed");
    assert_eq!(executed["template"]["id"], "tpl-a");
    assert_eq!(executed["template"]["version"], 1);
    let (code, t) = api
        .get("viewer-tok", "/v1/templates/tpl-a?version=newest")
        .await;
    assert_eq!(code, 200, "{t}");
    assert_eq!(t["status"], "draft");

    // A launch proposal is invalidated when stable moves before approval.
    let (code, change) = api
        .post(
            "bob-tok",
            "/v1/changes",
            json!({ "kind": "template_launch", "payload": { "id": "tpl-a" }, "reason": "go live" }),
        )
        .await;
    assert_eq!(code, 201, "{change}");
    let lid = change["id"].as_str().unwrap().to_string();
    assert_eq!(change["plan"]["summary"]["target_version"], 1);
    // Meanwhile an admin registers + launches v2 directly.
    let (code, v2) = api
        .post(
            "admin-tok",
            "/v1/templates",
            json!({ "config": tpl.replace("./out.jsonl", "./out-v2.jsonl"), "id": "tpl-a", "launch": true }),
        )
        .await;
    assert_eq!(code, 201, "{v2}");
    let (code, invalidated) = api
        .post(
            "admin-tok",
            &format!("/v1/changes/{lid}/approve"),
            json!({}),
        )
        .await;
    assert_eq!(code, 200, "{invalidated}");
    assert_eq!(invalidated["status"], "invalidated", "{invalidated}");
    assert!(
        invalidated["error"]
            .as_str()
            .unwrap_or_default()
            .contains("changed since approval"),
        "{invalidated}"
    );
    // A fresh launch proposal for an explicit version executes.
    let (_, change) = api
        .post(
            "bob-tok",
            "/v1/changes",
            json!({ "kind": "template_launch", "payload": { "id": "tpl-a", "version": 1 }, "reason": "roll to v1" }),
        )
        .await;
    let lid2 = change["id"].as_str().unwrap().to_string();
    let (_, executed) = api
        .post(
            "admin-tok",
            &format!("/v1/changes/{lid2}/approve"),
            json!({}),
        )
        .await;
    assert_eq!(executed["status"], "executed", "{executed}");
    assert_eq!(executed["template"]["version"], 1);
    let (_, t) = api.get("viewer-tok", "/v1/templates/tpl-a").await;
    assert_eq!(t["stable"], 1, "{t}");

    // The MCP `propose_run` tool files a request as the caller.
    let (code, r) = api
        .post(
            "dave-tok",
            "/mcp",
            json!({ "jsonrpc": "2.0", "id": 7, "method": "tools/call", "params": {
                "name": "propose_run",
                "arguments": { "config": config, "reason": "agent proposal", "budget": { "max_records": 100 } }
            }}),
        )
        .await;
    assert_eq!(code, 200, "{r}");
    let text = r["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_default();
    assert!(!r["result"]["isError"].as_bool().unwrap_or(false), "{r}");
    let proposed: Value = serde_json::from_str(text).unwrap();
    assert_eq!(proposed["status"], "pending");
    assert_eq!(proposed["requester"], "dave");
    let (_, list) = api
        .get("viewer-tok", "/v1/changes?status=pending&requester=dave")
        .await;
    assert_eq!(list.as_array().unwrap().len(), 1, "{list}");
    assert_eq!(list[0]["id"], proposed["change_id"]);
    // A proposal whose config cannot be planned comes back as a tool error
    // carrying the server error's `code: message` rendering.
    let (_, bad) = api
        .post(
            "dave-tok",
            "/mcp",
            json!({ "jsonrpc": "2.0", "id": 9, "method": "tools/call", "params": {
                "name": "propose_run",
                "arguments": { "config": "{ not yaml", "reason": "broken" }
            }}),
        )
        .await;
    assert_eq!(bad["result"]["isError"], true, "{bad}");
    let msg = bad["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_default();
    assert!(msg.contains(": "), "{msg}");
    // A viewer's MCP session is not offered the tool at all.
    let (_, tools) = api
        .post(
            "viewer-tok",
            "/mcp",
            json!({ "jsonrpc": "2.0", "id": 8, "method": "tools/list" }),
        )
        .await;
    let names: Vec<&str> = tools["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert!(!names.contains(&"propose_run"), "{names:?}");

    // Every step is in the audit log.
    let (_, audit) = api.get("admin-tok", "/v1/audit?limit=200").await;
    let actions: Vec<&str> = audit["entries"]
        .as_array()
        .or_else(|| audit.as_array())
        .unwrap_or_else(|| panic!("{audit}"))
        .iter()
        .filter_map(|e| e["action"].as_str())
        .collect();
    for want in [
        "change.requested",
        "change.approved",
        "change.executed",
        "change.rejected",
        "change.invalidated",
    ] {
        assert!(actions.contains(&want), "{want} missing in {actions:?}");
    }
    // The executed run's `change.executed` entry links to the run.
    let linked = audit["entries"]
        .as_array()
        .or_else(|| audit.as_array())
        .unwrap()
        .iter()
        .any(|e| e["action"] == "change.executed" && e["run_id"] == run_id);
    assert!(linked, "{audit}");
}

#[tokio::test(flavor = "multi_thread")]
async fn require_approval_turns_submissions_into_change_requests() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    std::fs::write(&input, "id\n1\n2\n").unwrap();
    let output = dir.path().join("out.jsonl");
    let port = free_port();
    spawn_server(port, dir.path(), vec!["run".into()]).await;
    let api = Api {
        base: format!("http://127.0.0.1:{port}"),
        client: reqwest::Client::new(),
    };
    let config = csv_config(&input, &output, "gated");

    // POST /v1/runs answers with a pending change request, not a run.
    let (code, resp) = api
        .post(
            "bob-tok",
            "/v1/runs",
            json!({ "config": config, "reason": "because", "name": "gated-run" }),
        )
        .await;
    assert_eq!(code, 202, "{resp}");
    assert_eq!(resp["status"], "pending_approval");
    let id = resp["change_id"].as_str().unwrap().to_string();
    assert_eq!(resp["change"]["kind"], "run");
    assert_eq!(resp["change"]["reason"], "because");
    assert_eq!(resp["change"]["payload"]["name"], "gated-run");
    let (_, runs) = api.get("admin-tok", "/v1/runs").await;
    assert!(
        runs["runs"].as_array().is_none_or(|r| r.is_empty()),
        "nothing ran: {runs}"
    );
    assert!(!output.exists());

    // A backfill is refused outright under the gate.
    let (code, _) = api
        .post(
            "bob-tok",
            "/v1/backfill",
            json!({ "config": config, "from": "2026-01-01", "to": "2026-01-02", "window": "1d" }),
        )
        .await;
    assert_eq!(code, 403);

    // The MCP `run_pipeline` is refused in favour of `propose_run`.
    let (_, r) = api
        .post(
            "admin-tok",
            "/mcp",
            json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {
                "name": "run_pipeline", "arguments": { "config": config }
            }}),
        )
        .await;
    assert!(r["result"]["isError"].as_bool().unwrap_or(false), "{r}");
    assert!(
        r["result"]["content"][0]["text"]
            .as_str()
            .unwrap_or_default()
            .contains("propose_run"),
        "{r}"
    );

    // Approval runs it.
    let (code, executed) = api
        .post("admin-tok", &format!("/v1/changes/{id}/approve"), json!({}))
        .await;
    assert_eq!(code, 200, "{executed}");
    assert_eq!(executed["status"], "executed");
    let rec = api.wait_run(executed["run_id"].as_str().unwrap()).await;
    assert_eq!(rec["status"], "completed", "{rec}");
    assert_eq!(rec["name"], "gated-run");
    assert_eq!(std::fs::read_to_string(&output).unwrap().lines().count(), 2);
}
