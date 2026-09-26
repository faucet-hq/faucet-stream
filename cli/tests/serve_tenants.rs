//! Multi-tenant embedded integrations end to end (#709): tenants, sealed
//! connections resolved by `auth: { ref }`, `${tenant.*}` routing, tenant
//! state namespaces, per-tenant limits, template runs and fan-out,
//! tenant-scoped principals, the hosted OAuth connect flow, re-auth
//! detection, the audit trail and the delete cascade — over a real server,
//! once on the in-memory history and once on SQLite.
#![cfg(all(
    feature = "tenants",
    feature = "source-rest",
    feature = "sink-jsonl",
    feature = "serve-history-sqlite"
))]

use serde_json::{Value, json};
use std::time::Duration;
use wiremock::matchers::{body_string_contains, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

const AUTH_CONFIG: &str = "principals:\n\
    \x20 - { name: alice, token: admin-tok, role: admin }\n\
    \x20 - { name: bob, token: op-tok, role: operator }\n\
    \x20 - { name: acme-app, token: acme-tok, role: operator, tenant: acme }\n";

fn serve_args(
    port: u16,
    dir: &std::path::Path,
    history: Option<String>,
    providers: std::path::PathBuf,
) -> faucet_cli::cli::ServeArgs {
    let auth_path = dir.join("auth.yaml");
    std::fs::write(&auth_path, AUTH_CONFIG).unwrap();
    faucet_cli::cli::ServeArgs {
        listen: format!("127.0.0.1:{port}"),
        auth_token: None,
        auth_config: Some(auth_path),
        read_token: None,
        write_token: None,
        admin_token: None,
        no_auth: false,
        max_concurrent_runs: Some(4),
        max_queued_runs: Some(32),
        default_config: None,
        history,
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
        vault_key: Some("test-vault-key".into()),
        vault_previous_key: Vec::new(),
        connect_providers: Some(providers),
    }
}

struct Api {
    base: String,
    client: reqwest::Client,
}

impl Api {
    async fn send(&self, m: reqwest::Method, token: &str, p: &str, body: Option<Value>) -> (u16, Value) {
        let mut req = self.client.request(m, format!("{}{p}", self.base)).bearer_auth(token);
        if let Some(b) = body {
            req = req.json(&b);
        }
        let r = req.send().await.unwrap();
        let status = r.status().as_u16();
        let text = r.text().await.unwrap();
        (status, serde_json::from_str(&text).unwrap_or(Value::String(text)))
    }
    async fn post(&self, token: &str, p: &str, body: Value) -> (u16, Value) {
        self.send(reqwest::Method::POST, token, p, Some(body)).await
    }
    async fn get(&self, token: &str, p: &str) -> (u16, Value) {
        self.send(reqwest::Method::GET, token, p, None).await
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
}

async fn spawn(dir: &std::path::Path, history: Option<String>, idp: &str) -> Api {
    let providers = dir.join("providers.yaml");
    std::fs::write(
        &providers,
        format!(
            "version: 1\nproviders:\n  - name: crm\n    authorize_url: {idp}/authorize\n    token_url: {idp}/token\n    client_id: cid\n    client_secret: csecret\n    scopes: [read, offline_access]\n    redirect_base: https://faucet.example\n    allowed_redirects: [https://app.example]\n"
        ),
    )
    .unwrap();
    let port = free_port();
    let mut config =
        faucet_cli::serve::ServeConfig::from_args(serve_args(port, dir, history, providers)).unwrap();
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
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    for _ in 0..1200 {
        if client
            .get(format!("http://127.0.0.1:{port}/healthz"))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Api {
        base: format!("http://127.0.0.1:{port}"),
        client,
    }
}

fn template(api_base: &str, out_dir: &std::path::Path, state_dir: &std::path::Path) -> String {
    format!(
        "version: 1\nkind: pipeline\nname: tenant-sync\npipeline:\n  source:\n    type: rest\n    config:\n      base_url: {api_base}\n      path: /items\n      records_path: \"$.items[*]\"\n      auth: {{ ref: api }}\n  sink:\n    type: jsonl\n    config:\n      path: \"{}/out-${{tenant.id}}.jsonl\"\n  state:\n    type: file\n    config:\n      path: {}\n",
        out_dir.display(),
        state_dir.display()
    )
}

fn lines(p: &std::path::Path) -> usize {
    std::fs::read_to_string(p)
        .map(|s| s.lines().filter(|l| !l.trim().is_empty()).count())
        .unwrap_or(0)
}

async fn scenario(history: impl Fn(&std::path::Path) -> Option<String>) {
    let dir = tempfile::tempdir().unwrap();
    let data = MockServer::start().await;
    let idp = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .and(header("authorization", "Bearer acme-secret-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"items": [{"id": 1}, {"id": 2}]})))
        .mount(&data)
        .await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .and(header("authorization", "Bearer globex-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"items": [{"id": 9}]})))
        .mount(&data)
        .await;
    // Hosted connect: the code exchange grants a refresh token; refreshing it
    // is refused (a revoked grant).
    Mock::given(method("POST"))
        .and(path("/token"))
        .and(body_string_contains("grant_type=authorization_code"))
        .and(body_string_contains("code_verifier="))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": "at1", "refresh_token": "rt-granted", "expires_in": 3600
        })))
        .mount(&idp)
        .await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .and(body_string_contains("grant_type=refresh_token"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({"error": "invalid_grant"})))
        .mount(&idp)
        .await;

    let api = spawn(dir.path(), history(dir.path()), &idp.uri()).await;
    let out = dir.path().join("out");
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&out).unwrap();

    // ── Tenants ────────────────────────────────────────────────────────────
    let (code, t) = api
        .post(
            "admin-tok",
            "/v1/tenants",
            json!({"id": "acme", "name": "Acme", "labels": {"region": "eu"},
                   "limits": {"max_concurrent_runs": 5, "max_records_per_run": 100}}),
        )
        .await;
    assert_eq!(code, 201, "{t}");
    assert_eq!(t["limits"]["max_records_per_run"], 100);
    let (code, _) = api.post("admin-tok", "/v1/tenants", json!({"id": "globex"})).await;
    assert_eq!(code, 201);
    let (code, _) = api.post("admin-tok", "/v1/tenants", json!({"id": "acme"})).await;
    assert_eq!(code, 409);
    let (code, _) = api.post("admin-tok", "/v1/tenants", json!({"id": "Bad Id"})).await;
    assert_eq!(code, 400);
    let (code, _) = api
        .post("admin-tok", "/v1/tenants", json!({"id": "z", "limits": {"max_concurrent_runs": 0}}))
        .await;
    assert_eq!(code, 400);
    // An operator may not create tenants.
    let (code, _) = api.post("op-tok", "/v1/tenants", json!({"id": "nope"})).await;
    assert_eq!(code, 403);

    // ── Connections ────────────────────────────────────────────────────────
    let (code, c) = api
        .post(
            "op-tok",
            "/v1/tenants/acme/connections",
            json!({"name": "api", "provider": {"type": "static", "config": {"token": "acme-secret-token"}}}),
        )
        .await;
    assert_eq!(code, 201, "{c}");
    assert_eq!(c["provider_type"], "static");
    assert_eq!(c["status"], "active");
    let (code, _) = api
        .post(
            "op-tok",
            "/v1/tenants/globex/connections",
            json!({"name": "api", "provider": {"type": "static", "config": {"token": "globex-token"}}}),
        )
        .await;
    assert_eq!(code, 201);
    let (code, _) = api
        .post(
            "op-tok",
            "/v1/tenants/acme/connections",
            json!({"name": "api", "provider": {"type": "static", "config": {"token": "x"}}}),
        )
        .await;
    assert_eq!(code, 409, "a second create of the same name conflicts");
    let (code, _) = api
        .post(
            "op-tok",
            "/v1/tenants/acme/connections",
            json!({"name": "bad", "provider": {"type": "nope", "config": {}}}),
        )
        .await;
    assert_eq!(code, 400);
    let (_, listed) = api.get("admin-tok", "/v1/tenants/acme/connections").await;
    assert!(!listed.to_string().contains("acme-secret-token"), "{listed}");
    let (_, detail) = api.get("admin-tok", "/v1/tenants/acme").await;
    assert!(!detail.to_string().contains("acme-secret-token"), "{detail}");
    assert_eq!(detail["connections"][0]["name"], "api");

    // ── A template run for a tenant ────────────────────────────────────────
    let (code, reg) = api
        .post(
            "admin-tok",
            "/v1/templates",
            json!({"config": template(&data.uri(), &out, &state_dir), "launch": true}),
        )
        .await;
    assert_eq!(code, 201, "{reg}");
    let id = reg["id"].as_str().unwrap().to_string();

    let (code, r) = api
        .post("op-tok", &format!("/v1/tenants/acme/templates/{id}/runs"), json!({}))
        .await;
    assert_eq!(code, 202, "{r}");
    let rec = api.wait_run(r["run_id"].as_str().unwrap()).await;
    assert_eq!(rec["status"], "completed", "{rec}");
    assert_eq!(rec["tenant"], "acme");
    assert_eq!(
        lines(&out.join("out-acme.jsonl")),
        2,
        "{}",
        std::fs::read_to_string(out.join("out-acme.jsonl")).unwrap_or_default()
    );

    // ── Fan-out across every tenant ────────────────────────────────────────
    let (code, fan) = api
        .post("op-tok", &format!("/v1/templates/{id}/fanout"), json!({"tenants": "all"}))
        .await;
    assert_eq!(code, 200, "{fan}");
    let results = fan["results"].as_array().unwrap();
    assert_eq!(results.len(), 2, "{fan}");
    for res in results {
        assert_eq!(res["status"], "submitted", "{fan}");
        let rec = api.wait_run(res["run_id"].as_str().unwrap()).await;
        assert_eq!(rec["status"], "completed", "{rec}");
        assert_eq!(rec["labels"]["fanout"], fan["fanout_id"]);
    }
    assert_eq!(lines(&out.join("out-globex.jsonl")), 1);
    let (_, fan) = api
        .post(
            "op-tok",
            &format!("/v1/templates/{id}/fanout"),
            json!({"tenants": ["globex", "ghost"]}),
        )
        .await;
    let ghost = fan["results"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["tenant"] == "ghost")
        .cloned()
        .unwrap();
    assert_eq!(ghost["status"], "skipped");
    for res in fan["results"].as_array().unwrap() {
        if let Some(run) = res["run_id"].as_str() {
            api.wait_run(run).await;
        }
    }

    // Runs are listed per tenant.
    let (_, page) = api.get("admin-tok", "/v1/runs?tenant=acme").await;
    let acme_runs = page["runs"].as_array().unwrap();
    assert_eq!(acme_runs.len(), 2, "{page}");
    assert!(acme_runs.iter().all(|r| r["tenant"] == "acme"));

    // ── Tenant-scoped principal ────────────────────────────────────────────
    let (_, mine) = api.get("acme-tok", "/v1/runs").await;
    assert!(mine["runs"].as_array().unwrap().iter().all(|r| r["tenant"] == "acme"));
    let (_, page) = api.get("admin-tok", "/v1/runs?tenant=globex").await;
    let globex_run = page["runs"][0]["run_id"].as_str().unwrap().to_string();
    let (code, _) = api.get("acme-tok", &format!("/v1/runs/{globex_run}")).await;
    assert_eq!(code, 404);
    let (code, _) = api.get("acme-tok", "/v1/runs?tenant=globex").await;
    assert_eq!(code, 404);
    let (code, _) = api.get("acme-tok", "/v1/tenants/globex").await;
    assert_eq!(code, 404);
    let (code, tenants) = api.get("acme-tok", "/v1/tenants").await;
    assert_eq!(code, 200);
    assert_eq!(tenants.as_array().unwrap().len(), 1);
    let (code, _) = api.get("acme-tok", "/v1/audit").await;
    assert_eq!(code, 403);
    let (code, _) = api.post("acme-tok", "/v1/tenants", json!({"id": "x"})).await;
    assert_eq!(code, 403);
    let (code, _) = api
        .send(reqwest::Method::DELETE, "acme-tok", "/v1/tenants/acme", None)
        .await;
    assert_eq!(code, 403);
    // A plain submission by a scoped principal runs for its tenant.
    let (code, r) = api
        .post(
            "acme-tok",
            "/v1/runs",
            json!({"config": template(&data.uri(), &out, &state_dir).replace("name: tenant-sync", "name: scoped")}),
        )
        .await;
    assert_eq!(code, 202, "{r}");
    let rec = api.wait_run(r["run_id"].as_str().unwrap()).await;
    assert_eq!(rec["tenant"], "acme");
    assert_eq!(rec["status"], "completed", "{rec}");
    let (_, usage) = api.get("acme-tok", "/v1/usage?by=tenant").await;
    assert_eq!(usage["report"]["rows"][0]["key"], "acme", "{usage}");

    // `${tenant.*}` is refused outside a tenant run.
    let (code, err) = api
        .post("op-tok", "/v1/runs", json!({"config": template(&data.uri(), &out, &state_dir)}))
        .await;
    assert_eq!(code, 422, "{err}");
    assert!(err.to_string().contains("tenant"), "{err}");

    // ── Hosted OAuth connect ───────────────────────────────────────────────
    let (code, _) = api
        .post(
            "op-tok",
            "/v1/tenants/acme/connect/crm",
            json!({"connection": "crm", "redirect": "https://evil.example/x"}),
        )
        .await;
    assert_eq!(code, 422, "an unlisted redirect is refused");
    let (code, _) = api
        .post(
            "op-tok",
            "/v1/tenants/acme/connect/nope",
            json!({"connection": "crm", "redirect": "https://app.example/done"}),
        )
        .await;
    assert_eq!(code, 422);
    let (code, started) = api
        .post(
            "op-tok",
            "/v1/tenants/acme/connect/crm",
            json!({"connection": "crm", "redirect": "https://app.example/done"}),
        )
        .await;
    assert_eq!(code, 200, "{started}");
    let authorize = reqwest::Url::parse(started["authorize_url"].as_str().unwrap()).unwrap();
    let q: std::collections::BTreeMap<String, String> = authorize.query_pairs().into_owned().collect();
    assert_eq!(q["code_challenge_method"], "S256");
    assert_eq!(q["redirect_uri"], "https://faucet.example/v1/connect/callback");
    let state = q["state"].clone();
    let resp = api
        .client
        .get(format!("{}/v1/connect/callback?code=abc&state={state}", api.base))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_redirection(), "{}", resp.status());
    let location = resp.headers()["location"].to_str().unwrap().to_string();
    assert_eq!(location, "https://app.example/done?connection=crm&status=ok");
    let resp = api
        .client
        .get(format!("{}/v1/connect/callback?code=abc&state={state}", api.base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 400, "a connect session is single-use");
    let (_, crm) = api.get("admin-tok", "/v1/tenants/acme/connections/crm").await;
    assert_eq!(crm["provider_type"], "oauth2_refresh");
    assert_eq!(crm["connect_provider"], "crm");
    assert!(!crm.to_string().contains("rt-granted"));
    // A provider-side refusal lands on the caller's redirect.
    let (_, started) = api
        .post(
            "op-tok",
            "/v1/tenants/acme/connect/crm",
            json!({"connection": "crm2", "redirect": "https://app.example/done"}),
        )
        .await;
    let url = reqwest::Url::parse(started["authorize_url"].as_str().unwrap()).unwrap();
    let st = url.query_pairs().find(|(k, _)| k == "state").unwrap().1.into_owned();
    let resp = api
        .client
        .get(format!("{}/v1/connect/callback?error=access_denied&state={st}", api.base))
        .send()
        .await
        .unwrap();
    assert!(resp.headers()["location"].to_str().unwrap().contains("status=error&error=access_denied"));

    // ── Re-auth: the refresh is refused, the connection is flagged ─────────
    let crm_config = template(&data.uri(), &out, &state_dir)
        .replace("name: tenant-sync", "name: crm-sync")
        .replace("ref: api", "ref: crm");
    let (code, r) = api
        .post("op-tok", "/v1/tenants/acme/runs", json!({"config": crm_config}))
        .await;
    assert_eq!(code, 202, "{r}");
    let rec = api.wait_run(r["run_id"].as_str().unwrap()).await;
    assert_eq!(rec["status"], "failed", "{rec}");
    let mut flagged = Value::Null;
    for _ in 0..200 {
        let (_, c) = api.get("admin-tok", "/v1/tenants/acme/connections/crm").await;
        if c["status"] == "needs_reauth" {
            flagged = c;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(flagged["status"], "needs_reauth", "{flagged}");
    let (code, err) = api
        .post("op-tok", "/v1/tenants/acme/runs", json!({"config": crm_config}))
        .await;
    assert_eq!(code, 409, "{err}");
    assert!(err.to_string().contains("re-authorization"), "{err}");
    // Replacing the credentials reconnects it.
    let (code, c) = api
        .send(
            reqwest::Method::PUT,
            "op-tok",
            "/v1/tenants/acme/connections/crm",
            Some(json!({"provider": {"type": "static", "config": {"token": "acme-secret-token"}}})),
        )
        .await;
    assert_eq!(code, 200, "{c}");
    assert_eq!(c["status"], "active");

    // A missing connection is refused up front.
    let (code, err) = api
        .post(
            "op-tok",
            "/v1/tenants/globex/runs",
            json!({"config": crm_config.replace("ref: crm", "ref: nothing")}),
        )
        .await;
    assert_eq!(code, 409, "{err}");

    // ── Limits: suspension and the per-run budget ──────────────────────────
    let (code, _) = api
        .send(
            reqwest::Method::PATCH,
            "admin-tok",
            "/v1/tenants/globex",
            Some(json!({"suspended": true})),
        )
        .await;
    assert_eq!(code, 200);
    let (code, _) = api
        .post("op-tok", &format!("/v1/tenants/globex/templates/{id}/runs"), json!({}))
        .await;
    assert_eq!(code, 409);
    let (_, fan) = api
        .post("op-tok", &format!("/v1/templates/{id}/fanout"), json!({"tenants": "all"}))
        .await;
    assert!(
        fan["results"].as_array().unwrap().iter().all(|r| r["tenant"] != "globex"),
        "a suspended tenant is not fanned out to: {fan}"
    );
    for res in fan["results"].as_array().unwrap() {
        if let Some(run) = res["run_id"].as_str() {
            api.wait_run(run).await;
        }
    }
    let (_, t) = api
        .send(
            reqwest::Method::PATCH,
            "admin-tok",
            "/v1/tenants/acme",
            Some(json!({"limits": {"max_records_per_run": 1}})),
        )
        .await;
    assert_eq!(t["limits"]["max_records_per_run"], 1);
    let (_, r) = api
        .post("op-tok", &format!("/v1/tenants/acme/templates/{id}/runs"), json!({}))
        .await;
    let rec = api.wait_run(r["run_id"].as_str().unwrap()).await;
    assert_eq!(rec["status"], "failed", "the tenant budget stops the run: {rec}");
    assert!(rec.to_string().to_lowercase().contains("budget"), "{rec}");

    // ── Audit carries the tenant ───────────────────────────────────────────
    let (_, audit) = api.get("admin-tok", "/v1/audit?tenant=acme&limit=500").await;
    let actions: Vec<&str> = audit["entries"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|e| e["action"].as_str())
        .collect();
    for a in ["tenant.create", "connection.upsert", "connect.start", "connect.complete", "connection.needs_reauth"] {
        assert!(actions.contains(&a), "{a} missing from {actions:?}");
    }

    // ── Delete cascade ─────────────────────────────────────────────────────
    let (code, report) = api
        .send(reqwest::Method::DELETE, "admin-tok", "/v1/tenants/acme", None)
        .await;
    assert_eq!(code, 200, "{report}");
    assert!(report["runs"].as_u64().unwrap() >= 4, "{report}");
    assert!(report["state_keys_deleted"].as_u64().unwrap() >= 1, "{report}");
    let (code, _) = api.get("admin-tok", "/v1/tenants/acme").await;
    assert_eq!(code, 404);
    let (_, page) = api.get("admin-tok", "/v1/runs?tenant=acme").await;
    assert!(page["runs"].as_array().unwrap().is_empty());
    let (_, conns) = api.get("admin-tok", "/v1/tenants/globex/connections").await;
    assert_eq!(conns.as_array().unwrap().len(), 1, "another tenant is untouched");
    let (code, providers) = api.get("admin-tok", "/v1/connect/providers").await;
    assert_eq!(code, 200);
    assert_eq!(providers[0]["name"], "crm");
}

#[tokio::test(flavor = "multi_thread")]
async fn tenants_end_to_end_in_memory() {
    scenario(|_| None).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn tenants_end_to_end_on_sqlite() {
    scenario(|dir| Some(format!("sqlite:{}", dir.join("history.db").display()))).await;
}
