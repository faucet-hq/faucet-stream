//! The data-flow policy in the console (#1043), end to end over a real RBAC
//! server: reading the policy, previewing a draft, proposing a change that
//! a second admin approves, invalidation when the base moves, rollback, the
//! refusals list, the dry run, tenant add-only policies (scope, approval,
//! enforcement on tenant runs only, deletion), version pinning on the run
//! record, and a `--policy` file shown read-only.
#![cfg(all(
    feature = "serve",
    feature = "policy",
    feature = "tenants",
    feature = "source-csv",
    feature = "sink-jsonl"
))]
#![allow(clippy::disallowed_methods)] // tests drive time and raw clients directly

use serde_json::{Value, json};
use std::time::Duration;

#[path = "support/ports.rs"]
mod ports;
use ports::free_port;

const AUTH_CONFIG: &str = "principals:\n\
    \x20 - { name: alice, token: alice-tok, role: admin }\n\
    \x20 - { name: ann, token: ann-tok, role: admin }\n\
    \x20 - { name: oscar, token: oscar-tok, role: operator }\n\
    \x20 - { name: vera, token: vera-tok, role: viewer }\n\
    \x20 - { name: acme-admin, token: acme-admin-tok, role: admin, tenant: acme }\n\
    \x20 - { name: acme-sec, token: acme-sec-tok, role: admin, tenant: acme }\n\
    \x20 - { name: acme-ops, token: acme-ops-tok, role: operator, tenant: acme }\n\
    \x20 - { name: globex-admin, token: globex-tok, role: admin, tenant: globex }\n";

const POLICY_V1: &str = "version: 1
description: EU personal data stays in the EU
classifications:
  - label: pii.email
    fields: [email]
rules:
  - name: pii-to-eu-only
    when: { label: pii.email }
    require: { residency: [eu] }
";

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
        no_ui: false,
        cluster: false,
        cluster_poll_secs: 2,
        cluster_max_attempts: 3,
        triggers: None,
        schedules: Default::default(),
        templates_sync: None,
        policy,
        otel_config: None,
        callback_allow_host: Vec::new(),
        origins: Default::default(),
        mcp: false,
        mcp_allow_mutations: false,
        require_approval: Vec::new(),
        approval_expiry_secs: 86_400,
        vault_key: None,
        vault_previous_key: Vec::new(),
        connect_providers: None,
        allow_subprocess_connectors: false,
        grafana_url: None,
        references: Default::default(),
        pools: Default::default(),
    }
}

async fn spawn_server(port: u16, dir: &std::path::Path, policy_file: bool) {
    let auth_path = dir.join("auth.yaml");
    std::fs::write(&auth_path, AUTH_CONFIG).unwrap();
    let policy = policy_file.then(|| {
        let p = dir.join("policy.yaml");
        std::fs::write(&p, POLICY_V1).unwrap();
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

struct Api {
    base: String,
    client: reqwest::Client,
}

impl Api {
    fn new(port: u16) -> Self {
        Self {
            base: format!("http://127.0.0.1:{port}"),
            client: reqwest::Client::new(),
        }
    }

    async fn get(&self, token: &str, path: &str) -> (u16, Value) {
        let r = self
            .client
            .get(format!("{}{path}", self.base))
            .bearer_auth(token)
            .send()
            .await
            .unwrap();
        let s = r.status().as_u16();
        (s, r.json().await.unwrap_or(Value::Null))
    }

    async fn post(&self, token: &str, path: &str, body: Value) -> (u16, Value) {
        let r = self
            .client
            .post(format!("{}{path}", self.base))
            .bearer_auth(token)
            .json(&body)
            .send()
            .await
            .unwrap();
        let s = r.status().as_u16();
        (s, r.json().await.unwrap_or(Value::Null))
    }

    async fn delete(&self, token: &str, path: &str) -> (u16, Value) {
        let r = self
            .client
            .delete(format!("{}{path}", self.base))
            .bearer_auth(token)
            .send()
            .await
            .unwrap();
        let s = r.status().as_u16();
        (s, r.json().await.unwrap_or(Value::Null))
    }

    /// Propose `policy` as `token` on `path` and return the change id.
    async fn propose(&self, token: &str, path: &str, policy: &str, base: Value) -> String {
        let (s, c) = self
            .post(
                token,
                path,
                json!({ "policy": policy, "reason": "test change", "base_version": base }),
            )
            .await;
        assert_eq!(s, 201, "{c}");
        c["id"].as_str().unwrap().to_string()
    }

    async fn approve(&self, token: &str, id: &str) -> (u16, Value) {
        self.post(token, &format!("/v1/changes/{id}/approve"), json!({}))
            .await
    }
}

/// A csv → jsonl config whose contract declares `email`, with sink `attrs`.
fn config(dir: &std::path::Path, name: &str, residency: &str) -> String {
    let input = dir.join("in.csv");
    std::fs::write(&input, "id,email\n1,a@example.com\n").unwrap();
    let output = dir.join(format!("{name}.jsonl"));
    format!(
        "version: 1\nname: {name}\npipeline:\n  source: {{ type: csv, config: {{ path: {} }} }}\n  \
         contract:\n    version: \"1\"\n    fields:\n      - {{ name: id, type: string }}\n      \
         - {{ name: email, type: string }}\n  sink:\n    type: jsonl\n    attributes: {{ residency: \
         {residency}, tier: partner }}\n    config: {{ path: {} }}\n",
        input.display(),
        output.display()
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn server_managed_policy_is_changed_only_through_a_second_admins_approval() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    spawn_server(port, dir.path(), false).await;
    let api = Api::new(port);

    // Nothing loaded; only an admin may edit.
    let (s, p) = api.get("vera-tok", "/v1/policy").await;
    assert_eq!(s, 200, "{p}");
    assert_eq!(p["source"], "server");
    assert_eq!(p["loaded"], false);
    assert_eq!(p["editable"], false);
    assert_eq!(api.get("alice-tok", "/v1/policy").await.1["editable"], true);

    // A preview reports problems with their line, then the diff and impact.
    let (s, bad) = api
        .post(
            "alice-tok",
            "/v1/policy/preview",
            json!({ "policy": "version: 1\nrules:\n  - name: x\n    when: { label: nope }\n    deny: true\n" }),
        )
        .await;
    assert_eq!(s, 200);
    assert_eq!(bad["valid"], false);
    assert_eq!(bad["problems"][0]["line"], 4, "{bad}");
    let (_, good) = api
        .post(
            "alice-tok",
            "/v1/policy/preview",
            json!({ "policy": POLICY_V1 }),
        )
        .await;
    assert_eq!(good["valid"], true, "{good}");
    assert_eq!(good["next_version"], 1);
    assert!(
        good["diff"]
            .as_array()
            .unwrap()
            .iter()
            .all(|l| l["op"] == "+")
    );
    assert!(good["impact"].is_object());

    // Who may propose, and the checks before a proposal.
    let body = json!({ "policy": POLICY_V1, "reason": "first", "base_version": null });
    assert_eq!(
        api.post("oscar-tok", "/v1/policy", body.clone()).await.0,
        403
    );
    assert_eq!(
        api.post("vera-tok", "/v1/policy", body.clone()).await.0,
        403
    );
    assert_eq!(
        api.post("acme-admin-tok", "/v1/policy", body.clone())
            .await
            .0,
        403
    );
    let no_reason = json!({ "policy": POLICY_V1, "reason": " ", "base_version": null });
    assert_eq!(api.post("alice-tok", "/v1/policy", no_reason).await.0, 400);
    let stale = json!({ "policy": POLICY_V1, "reason": "x", "base_version": 3 });
    assert_eq!(api.post("alice-tok", "/v1/policy", stale).await.0, 409);
    // POST /v1/changes is not a way around those checks.
    let (s, _) = api
        .post(
            "alice-tok",
            "/v1/changes",
            json!({ "kind": "policy_update", "payload": { "policy": POLICY_V1 } }),
        )
        .await;
    assert_eq!(s, 400);

    let id = api
        .propose("alice-tok", "/v1/policy", POLICY_V1, Value::Null)
        .await;
    let (_, p) = api.get("vera-tok", "/v1/policy").await;
    assert_eq!(p["loaded"], false, "nothing is enforced before approval");
    assert_eq!(
        api.get("alice-tok", "/v1/policy").await.1["pending_change"]["id"],
        id
    );

    // The requester and an operator cannot approve; a second admin can.
    assert_eq!(api.approve("alice-tok", &id).await.0, 403);
    assert_eq!(api.approve("oscar-tok", &id).await.0, 403);
    let (s, c) = api.approve("ann-tok", &id).await;
    assert_eq!(s, 200, "{c}");
    assert_eq!(c["status"], "executed", "{c}");
    assert_eq!(c["plan"]["summary"]["version"], 1);

    let (_, p) = api.get("vera-tok", "/v1/policy").await;
    assert_eq!(p["loaded"], true);
    assert_eq!(p["version"]["version"], 1);
    assert_eq!(p["version"]["approved_by"], "ann");
    assert_eq!(p["summary"]["rules"], 1);
    assert_eq!(p["rules"][0]["name"], "pii-to-eu-only");
    let (_, versions) = api.get("vera-tok", "/v1/policy/versions").await;
    assert_eq!(versions.as_array().unwrap().len(), 1);
    assert!(
        versions[0]["yaml"]
            .as_str()
            .unwrap()
            .contains("pii-to-eu-only")
    );

    // Enforcement: a us sink is refused at submit and listed as a refusal; an
    // eu sink runs, and its record pins version 1.
    let (s, refused) = api
        .post(
            "alice-tok",
            "/v1/runs",
            json!({ "config": config(dir.path(), "us", "us") }),
        )
        .await;
    assert_eq!(s, 422, "{refused}");
    let (_, list) = api.get("vera-tok", "/v1/policy/refusals").await;
    let first = &list[0];
    assert_eq!(first["phase"], "submit");
    assert_eq!(first["rule"], "pii-to-eu-only");
    assert_eq!(first["column"], "email");
    assert_eq!(first["layer"], "server");
    let (s, ok) = api
        .post(
            "alice-tok",
            "/v1/runs",
            json!({ "config": config(dir.path(), "eu", "eu") }),
        )
        .await;
    assert_eq!(s, 202, "{ok}");
    let run_id = ok["run_id"].as_str().unwrap();
    let (_, rec) = api.get("alice-tok", &format!("/v1/runs/{run_id}")).await;
    assert_eq!(rec["policy_pin"]["server"], 1, "{rec}");

    // The dry run reports the violation without running anything.
    let (s, report) = api
        .post(
            "oscar-tok",
            "/v1/policy/check",
            json!({ "config": config(dir.path(), "dry", "us") }),
        )
        .await;
    assert_eq!(s, 200, "{report}");
    assert_eq!(report["violations"], 1);
    assert_eq!(
        api.post("acme-ops-tok", "/v1/policy/check", json!({ "config": "x" }))
            .await
            .0,
        403
    );

    // Two proposals on v1: the first approved wins, the second is invalidated.
    let v2 = POLICY_V1.replace("[eu]", "[eu, uk]");
    let v2b = POLICY_V1.replace("[eu]", "[eu, ch]");
    let a = api.propose("alice-tok", "/v1/policy", &v2, json!(1)).await;
    let b = api.propose("alice-tok", "/v1/policy", &v2b, json!(1)).await;
    assert_eq!(api.approve("ann-tok", &a).await.1["status"], "executed");
    let (_, c) = api.approve("ann-tok", &b).await;
    assert_eq!(c["status"], "invalidated", "{c}");

    // Rollback to v1 is a change request too.
    let (s, rb) = api
        .post(
            "alice-tok",
            "/v1/policy/rollback",
            json!({ "version": 1, "reason": "back out uk" }),
        )
        .await;
    assert_eq!(s, 201, "{rb}");
    assert_eq!(rb["plan"]["summary"]["restores"], 1);
    let (_, c) = api.approve("ann-tok", rb["id"].as_str().unwrap()).await;
    assert_eq!(c["status"], "executed");
    let (_, p) = api.get("vera-tok", "/v1/policy").await;
    assert_eq!(p["version"]["version"], 3);
    assert_eq!(p["version"]["restores"], 1);
    assert_eq!(
        api.post(
            "alice-tok",
            "/v1/policy/rollback",
            json!({ "version": 3, "reason": "x" })
        )
        .await
        .0,
        409
    );

    // The audit trail names both people.
    let (_, audit) = api
        .get("alice-tok", "/v1/audit?action=policy.updated")
        .await;
    let entries = audit["entries"]
        .as_array()
        .cloned()
        .unwrap_or_else(|| audit.as_array().cloned().unwrap_or_default());
    assert_eq!(entries.len(), 3);
    assert!(entries.iter().all(|e| e["principal"] == "ann"));
}

#[tokio::test(flavor = "multi_thread")]
async fn tenant_policies_only_add_and_apply_to_that_tenants_runs() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    spawn_server(port, dir.path(), false).await;
    let api = Api::new(port);
    for t in ["acme", "globex"] {
        let (s, b) = api
            .post("alice-tok", "/v1/tenants", json!({ "id": t }))
            .await;
        assert_eq!(s, 201, "{b}");
    }
    let id = api
        .propose("alice-tok", "/v1/policy", POLICY_V1, Value::Null)
        .await;
    assert_eq!(api.approve("ann-tok", &id).await.1["status"], "executed");

    // The tenant view: server rules outlined, no tenant rules yet.
    let (s, tp) = api.get("acme-ops-tok", "/v1/tenants/acme/policy").await;
    assert_eq!(s, 200, "{tp}");
    assert_eq!(tp["loaded"], false);
    assert_eq!(tp["server"]["rules"][0]["name"], "pii-to-eu-only");
    assert_eq!(tp["editable"], false);
    assert_eq!(
        api.get("acme-admin-tok", "/v1/tenants/acme/policy").await.1["editable"],
        true
    );
    assert_eq!(
        api.get("acme-ops-tok", "/v1/tenants/globex/policy").await.0,
        404
    );
    // A tenant principal sees server rule outlines on /v1/policy, not the policy.
    let (_, p) = api.get("acme-ops-tok", "/v1/policy").await;
    assert!(p.get("policy").is_none(), "{p}");
    assert_eq!(p["rules"][0]["requires"], "sink residency in [eu]");
    assert_eq!(api.get("acme-ops-tok", "/v1/policy/versions").await.0, 403);

    // Add-only: reusing a server rule name is refused.
    let clash = "version: 1\nrules:\n  - name: pii-to-eu-only\n    when: { label: pii.email }\n    deny: true\n";
    let (_, pv) = api
        .post(
            "acme-admin-tok",
            "/v1/tenants/acme/policy/preview",
            json!({ "policy": clash }),
        )
        .await;
    assert_eq!(pv["valid"], false);
    assert!(
        pv["problems"][0]["message"]
            .as_str()
            .unwrap()
            .contains("Tenant rules can only add")
    );
    let disable = "version: 1\ndisable: [pii-to-eu-only]\n";
    let (_, pv) = api
        .post(
            "acme-admin-tok",
            "/v1/tenants/acme/policy/preview",
            json!({ "policy": disable }),
        )
        .await;
    assert!(
        pv["problems"][0]["message"]
            .as_str()
            .unwrap()
            .contains("cannot disable")
    );

    // acme: email may not reach a partner-tier sink.
    let acme = "version: 1\nrules:\n  - name: acme-no-partner\n    when: { label: pii.email }\n    \
                require: { tier: [internal] }\n";
    let cid = api
        .propose(
            "acme-admin-tok",
            "/v1/tenants/acme/policy",
            acme,
            Value::Null,
        )
        .await;
    let (_, c) = api.get("acme-ops-tok", &format!("/v1/changes/{cid}")).await;
    assert_eq!(c["kind"], "tenant_policy_update");
    assert_eq!(c["tenant"], "acme");
    // globex cannot see it; the requester and an acme operator cannot approve.
    assert_eq!(
        api.get("globex-tok", &format!("/v1/changes/{cid}")).await.0,
        404
    );
    assert_eq!(api.approve("acme-admin-tok", &cid).await.0, 403);
    assert_eq!(api.approve("acme-ops-tok", &cid).await.0, 403);
    let (_, c) = api.approve("acme-sec-tok", &cid).await;
    assert_eq!(c["status"], "executed", "{c}");

    let (_, tp) = api.get("acme-ops-tok", "/v1/tenants/acme/policy").await;
    assert_eq!(tp["loaded"], true);
    assert_eq!(tp["version"]["version"], 1);
    assert_eq!(tp["versions"].as_array().unwrap().len(), 1);

    // The tenant layer applies to acme's runs only.
    let cfg = config(dir.path(), "partner", "eu");
    let (s, b) = api
        .post(
            "alice-tok",
            "/v1/tenants/acme/runs",
            json!({ "config": cfg }),
        )
        .await;
    assert_eq!(s, 422, "{b}");
    let (s, b) = api
        .post("alice-tok", "/v1/runs", json!({ "config": cfg }))
        .await;
    assert_eq!(s, 202, "{b}");
    // acme sees its refusal, attributed to its own layer; globex sees none.
    let (_, mine) = api.get("acme-ops-tok", "/v1/policy/refusals").await;
    assert_eq!(mine.as_array().unwrap().len(), 1, "{mine}");
    assert_eq!(mine[0]["rule"], "acme-no-partner");
    assert_eq!(mine[0]["layer"], "tenant");
    assert_eq!(
        api.get("globex-tok", "/v1/policy/refusals").await.1,
        json!([])
    );

    // A server admin may change acme's rules too (and a server change that
    // reuses acme's rule name is refused).
    let reuse = POLICY_V1.replace("pii-to-eu-only", "acme-no-partner");
    let (_, pv) = api
        .post(
            "alice-tok",
            "/v1/policy/preview",
            json!({ "policy": reuse }),
        )
        .await;
    assert!(
        pv["problems"][0]["message"]
            .as_str()
            .unwrap()
            .contains("tenant acme")
    );

    // Deleting the tenant deletes its policy versions.
    let (s, report) = api.delete("alice-tok", "/v1/tenants/acme").await;
    assert_eq!(s, 200, "{report}");
    assert_eq!(report["policy_versions"], 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_policy_file_is_read_only() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    spawn_server(port, dir.path(), true).await;
    let api = Api::new(port);
    let (_, p) = api.get("vera-tok", "/v1/policy").await;
    assert_eq!(p["source"], "file");
    assert_eq!(p["loaded"], true);
    assert_eq!(p["editable"], false);
    assert!(p["file"]["path"].as_str().unwrap().ends_with("policy.yaml"));
    assert_eq!(p["file"]["sha256"].as_str().unwrap().len(), 64);
    let body = json!({ "policy": POLICY_V1, "reason": "x", "base_version": null });
    assert_eq!(api.post("alice-tok", "/v1/policy", body).await.0, 409);
    assert_eq!(
        api.post(
            "alice-tok",
            "/v1/policy/preview",
            json!({ "policy": POLICY_V1 })
        )
        .await
        .0,
        409
    );
    assert_eq!(
        api.post(
            "alice-tok",
            "/v1/policy/rollback",
            json!({ "version": 1, "reason": "x" })
        )
        .await
        .0,
        409
    );
}
