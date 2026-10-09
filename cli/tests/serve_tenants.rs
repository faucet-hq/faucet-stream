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
    \x20 - { name: carol, token: view-tok, role: viewer }\n\
    \x20 - { name: acme-app, token: acme-tok, role: operator, tenant: acme }\n";

fn serve_args(
    port: u16,
    dir: &std::path::Path,
    history: Option<String>,
    providers: std::path::PathBuf,
    triggers: Option<std::path::PathBuf>,
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
        cluster_poll_secs: 2,
        cluster_max_attempts: 3,
        triggers,
        templates_sync: None,
        policy: None,
        otel_config: None,
        callback_allow_host: Vec::new(),
        mcp: false,
        mcp_allow_mutations: false,
        require_approval: Vec::new(),
        approval_expiry_secs: 86_400,
        require_template_tests: false,
        vault_key: Some("test-vault-key-0123456789abcdef0123".into()),
        vault_previous_key: Vec::new(),
        connect_providers: Some(providers),
        allow_subprocess_connectors: false,
    }
}

struct Api {
    base: String,
    client: reqwest::Client,
}

impl Api {
    async fn send(
        &self,
        m: reqwest::Method,
        token: &str,
        p: &str,
        body: Option<Value>,
    ) -> (u16, Value) {
        let mut req = self
            .client
            .request(m, format!("{}{p}", self.base))
            .bearer_auth(token);
        if let Some(b) = body {
            req = req.json(&b);
        }
        let r = req.send().await.unwrap();
        let status = r.status().as_u16();
        let text = r.text().await.unwrap();
        (
            status,
            serde_json::from_str(&text).unwrap_or(Value::String(text)),
        )
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
    spawn_with(dir, history, idp, None).await
}

async fn spawn_with(
    dir: &std::path::Path,
    history: Option<String>,
    idp: &str,
    triggers: Option<std::path::PathBuf>,
) -> Api {
    let providers = dir.join("providers.yaml");
    std::fs::write(
        &providers,
        format!(
            "version: 1\nproviders:\n  - name: crm\n    authorize_url: {idp}/authorize\n    token_url: {idp}/token\n    client_id: cid\n    client_secret: csecret\n    scopes: [read, offline_access]\n    redirect_base: https://faucet.example\n    allowed_redirects: [https://app.example]\n"
        ),
    )
    .unwrap();
    let port = free_port();
    let mut config = faucet_cli::serve::ServeConfig::from_args(serve_args(
        port, dir, history, providers, triggers,
    ))
    .unwrap();
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
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"items": [{"id": 1}, {"id": 2}]})),
        )
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
    let (code, _) = api
        .post("admin-tok", "/v1/tenants", json!({"id": "globex"}))
        .await;
    assert_eq!(code, 201);
    let (code, _) = api
        .post("admin-tok", "/v1/tenants", json!({"id": "acme"}))
        .await;
    assert_eq!(code, 409);
    let (code, _) = api
        .post("admin-tok", "/v1/tenants", json!({"id": "Bad Id"}))
        .await;
    assert_eq!(code, 400);
    let (code, _) = api
        .post(
            "admin-tok",
            "/v1/tenants",
            json!({"id": "z", "limits": {"max_concurrent_runs": 0}}),
        )
        .await;
    assert_eq!(code, 400);
    // An operator may not create tenants.
    let (code, _) = api
        .post("op-tok", "/v1/tenants", json!({"id": "nope"}))
        .await;
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
    assert!(
        !listed.to_string().contains("acme-secret-token"),
        "{listed}"
    );
    let (_, detail) = api.get("admin-tok", "/v1/tenants/acme").await;
    assert!(
        !detail.to_string().contains("acme-secret-token"),
        "{detail}"
    );
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
        .post(
            "op-tok",
            &format!("/v1/tenants/acme/templates/{id}/runs"),
            json!({}),
        )
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
        .post(
            "op-tok",
            &format!("/v1/templates/{id}/fanout"),
            json!({"tenants": "all"}),
        )
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

    // A retried keyed fan-out replays every tenant's run (#789 SERVE-36).
    let keyed = json!({"tenants": "all", "idempotency_key": "fan-1"});
    let (_, first) = api
        .post(
            "op-tok",
            &format!("/v1/templates/{id}/fanout"),
            keyed.clone(),
        )
        .await;
    let (_, again) = api
        .post("op-tok", &format!("/v1/templates/{id}/fanout"), keyed)
        .await;
    assert_eq!(first["fanout_id"], again["fanout_id"], "{again}");
    for (a, b) in first["results"]
        .as_array()
        .unwrap()
        .iter()
        .zip(again["results"].as_array().unwrap())
    {
        assert_eq!(b["status"], "submitted", "{again}");
        assert_eq!(a["run_id"], b["run_id"], "{again}");
        api.wait_run(a["run_id"].as_str().unwrap()).await;
    }
    // The same key with another payload is a failure, not a skip.
    let (_, other) = api
        .post(
            "op-tok",
            &format!("/v1/templates/{id}/fanout"),
            json!({"tenants": "all", "idempotency_key": "fan-1", "labels": {"x": "y"}}),
        )
        .await;
    assert!(
        other["results"]
            .as_array()
            .unwrap()
            .iter()
            .all(|r| r["status"] == "failed"),
        "{other}"
    );

    // ── Tenant-scoped principal ────────────────────────────────────────────
    let (_, mine) = api.get("acme-tok", "/v1/runs").await;
    assert!(
        mine["runs"]
            .as_array()
            .unwrap()
            .iter()
            .all(|r| r["tenant"] == "acme")
    );
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
    // The rows endpoint shows a tenant its own state and runs, read under
    // its namespace and filtered by tenant (#789 SERVE-31).
    let (code, rows) = api
        .get("acme-tok", &format!("/v1/templates/{id}/rows"))
        .await;
    assert_eq!(code, 200, "{rows}");
    let row_state = &rows["rows"][0]["state"];
    assert!(row_state["last_success"].is_string(), "{rows}");
    assert!(!rows.to_string().contains("state omitted"), "{rows}");
    let (code, _) = api.get("acme-tok", "/v1/audit").await;
    assert_eq!(code, 403);
    // An operator-level route outside the tenant's scope is refused for the
    // scoped principal even though its role would allow it.
    let (code, err) = api
        .post(
            "acme-tok",
            &format!("/v1/templates/{id}/fanout"),
            json!({"tenants": "all"}),
        )
        .await;
    assert_eq!(code, 403, "{err}");
    assert!(err.to_string().contains("confined to tenant"), "{err}");
    let (code, _) = api
        .post("acme-tok", "/v1/tenants", json!({"id": "x"}))
        .await;
    assert_eq!(code, 403);
    let (code, _) = api
        .send(
            reqwest::Method::DELETE,
            "acme-tok",
            "/v1/tenants/acme",
            None,
        )
        .await;
    assert_eq!(code, 403);
    // A scoped principal may not submit a config of its own, through any
    // route: it runs registered templates only.
    let raw = json!({"config": template(&data.uri(), &out, &state_dir).replace("name: tenant-sync", "name: scoped")});
    for route in ["/v1/runs", "/v1/tenants/acme/runs"] {
        let (code, err) = api.post("acme-tok", route, raw.clone()).await;
        assert_eq!(code, 403, "{route}: {err}");
        assert!(err.to_string().contains("registered templates"), "{err}");
    }
    let (code, err) = api
        .post(
            "acme-tok",
            "/v1/changes",
            json!({"kind": "run", "payload": raw.clone()}),
        )
        .await;
    assert_eq!(code, 403, "{err}");
    // An unscoped operator may still run a config for the tenant.
    let (code, r) = api.post("op-tok", "/v1/tenants/acme/runs", raw).await;
    assert_eq!(code, 202, "{r}");
    api.wait_run(r["run_id"].as_str().unwrap()).await;
    // The same idempotency key from two tenants starts two runs.
    let mut keyed = Vec::new();
    for tenant in ["acme", "globex"] {
        let (code, r) = api
            .post(
                "op-tok",
                &format!("/v1/tenants/{tenant}/templates/{id}/runs"),
                json!({"idempotency_key": "nightly-2026-10-01"}),
            )
            .await;
        assert_eq!(code, 202, "{tenant}: {r}");
        let rec = api.wait_run(r["run_id"].as_str().unwrap()).await;
        assert_eq!(rec["tenant"], tenant, "{rec}");
        keyed.push(r["run_id"].as_str().unwrap().to_string());
    }
    assert_ne!(keyed[0], keyed[1]);
    let (code, again) = api
        .post(
            "acme-tok",
            &format!("/v1/tenants/acme/templates/{id}/runs"),
            json!({"idempotency_key": "nightly-2026-10-01"}),
        )
        .await;
    assert_eq!(code, 202, "{again}");
    assert_eq!(
        again["run_id"],
        keyed[0].as_str(),
        "a replay within the tenant"
    );
    let (code, r) = api
        .post(
            "acme-tok",
            &format!("/v1/tenants/acme/templates/{id}/runs"),
            json!({}),
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
        .post(
            "op-tok",
            "/v1/runs",
            json!({"config": template(&data.uri(), &out, &state_dir)}),
        )
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
    let q: std::collections::BTreeMap<String, String> =
        authorize.query_pairs().into_owned().collect();
    assert_eq!(q["code_challenge_method"], "S256");
    assert_eq!(
        q["redirect_uri"],
        "https://faucet.example/v1/connect/callback"
    );
    let state = q["state"].clone();
    let resp = api
        .client
        .get(format!(
            "{}/v1/connect/callback?code=abc&state={state}",
            api.base
        ))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_redirection(), "{}", resp.status());
    let location = resp.headers()["location"].to_str().unwrap().to_string();
    assert_eq!(
        location,
        "https://app.example/done?connection=crm&status=ok"
    );
    let resp = api
        .client
        .get(format!(
            "{}/v1/connect/callback?code=abc&state={state}",
            api.base
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        400,
        "a connect session is single-use"
    );
    let (_, crm) = api
        .get("admin-tok", "/v1/tenants/acme/connections/crm")
        .await;
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
    let st = url
        .query_pairs()
        .find(|(k, _)| k == "state")
        .unwrap()
        .1
        .into_owned();
    let resp = api
        .client
        .get(format!(
            "{}/v1/connect/callback?error=access_denied&state={st}",
            api.base
        ))
        .send()
        .await
        .unwrap();
    assert!(
        resp.headers()["location"]
            .to_str()
            .unwrap()
            .contains("status=error&error=access_denied")
    );

    // ── Re-auth: the refresh is refused, the connection is flagged ─────────
    let crm_config = template(&data.uri(), &out, &state_dir)
        .replace("name: tenant-sync", "name: crm-sync")
        .replace("ref: api", "ref: crm");
    let (code, r) = api
        .post(
            "op-tok",
            "/v1/tenants/acme/runs",
            json!({"config": crm_config}),
        )
        .await;
    assert_eq!(code, 202, "{r}");
    let rec = api.wait_run(r["run_id"].as_str().unwrap()).await;
    assert_eq!(rec["status"], "failed", "{rec}");
    let mut flagged = Value::Null;
    for _ in 0..200 {
        let (_, c) = api
            .get("admin-tok", "/v1/tenants/acme/connections/crm")
            .await;
        if c["status"] == "needs_reauth" {
            flagged = c;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(flagged["status"], "needs_reauth", "{flagged}");
    let (code, err) = api
        .post(
            "op-tok",
            "/v1/tenants/acme/runs",
            json!({"config": crm_config}),
        )
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
        .post(
            "op-tok",
            &format!("/v1/tenants/globex/templates/{id}/runs"),
            json!({}),
        )
        .await;
    assert_eq!(code, 409);
    let (_, fan) = api
        .post(
            "op-tok",
            &format!("/v1/templates/{id}/fanout"),
            json!({"tenants": "all"}),
        )
        .await;
    assert!(
        fan["results"]
            .as_array()
            .unwrap()
            .iter()
            .all(|r| r["tenant"] != "globex"),
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
        .post(
            "op-tok",
            &format!("/v1/tenants/acme/templates/{id}/runs"),
            json!({}),
        )
        .await;
    let rec = api.wait_run(r["run_id"].as_str().unwrap()).await;
    assert_eq!(
        rec["status"], "failed",
        "the tenant budget stops the run: {rec}"
    );
    assert!(rec.to_string().to_lowercase().contains("budget"), "{rec}");

    // ── Edges: patch fields, 404s, callback errors, fan-out outcomes ───────
    let (code, t) = api
        .send(
            reqwest::Method::PATCH,
            "admin-tok",
            "/v1/tenants/acme",
            Some(json!({"name": "Acme Inc", "labels": {"region": "us"}, "notifications": []})),
        )
        .await;
    assert_eq!(code, 200, "{t}");
    assert_eq!(t["name"], "Acme Inc");
    assert_eq!(t["labels"]["region"], "us");
    let (code, _) = api
        .send(
            reqwest::Method::PATCH,
            "admin-tok",
            "/v1/tenants/acme",
            Some(json!({"notifications": [{"nope": 1}]})),
        )
        .await;
    assert_eq!(code, 400);
    let (code, _) = api.get("admin-tok", "/v1/tenants/ghost/connections").await;
    assert_eq!(code, 404);
    let (code, _) = api
        .get("admin-tok", "/v1/tenants/acme/connections/ghost")
        .await;
    assert_eq!(code, 404);
    let (code, _) = api
        .send(
            reqwest::Method::DELETE,
            "op-tok",
            "/v1/tenants/acme/connections/ghost",
            None,
        )
        .await;
    assert_eq!(code, 404);
    let (code, _) = api
        .post(
            "op-tok",
            "/v1/tenants/acme/connections",
            json!({"name": "scratch", "provider": {"type": "static", "config": {"token": "s"}}}),
        )
        .await;
    assert_eq!(code, 201);
    let (code, _) = api
        .send(
            reqwest::Method::DELETE,
            "op-tok",
            "/v1/tenants/acme/connections/scratch",
            None,
        )
        .await;
    assert_eq!(code, 204);
    let resp = api
        .client
        .get(format!("{}/v1/connect/callback", api.base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 400, "a callback without state");
    let (_, started) = api
        .post(
            "op-tok",
            "/v1/tenants/acme/connect/crm",
            json!({"connection": "crm3", "redirect": "https://app.example/done"}),
        )
        .await;
    let url = reqwest::Url::parse(started["authorize_url"].as_str().unwrap()).unwrap();
    let st = url
        .query_pairs()
        .find(|(k, _)| k == "state")
        .unwrap()
        .1
        .into_owned();
    let resp = api
        .client
        .get(format!("{}/v1/connect/callback?state={st}", api.base))
        .send()
        .await
        .unwrap();
    assert!(
        resp.headers()["location"]
            .to_str()
            .unwrap()
            .contains("error=missing_code")
    );
    let (_, fan) = api
        .post(
            "op-tok",
            "/v1/templates/no-such-template/fanout",
            json!({"tenants": ["acme"]}),
        )
        .await;
    assert_eq!(fan["results"][0]["status"], "failed", "{fan}");
    let (_, fan) = api
        .post(
            "op-tok",
            &format!("/v1/templates/{id}/fanout"),
            json!({"tenants": ["globex"]}),
        )
        .await;
    assert_eq!(
        fan["results"][0]["status"], "skipped",
        "a suspended tenant: {fan}"
    );
    let (code, _) = api
        .post(
            "op-tok",
            &format!("/v1/templates/{id}/fanout"),
            json!({"tenants": []}),
        )
        .await;
    assert_eq!(code, 400);

    // ── Audit carries the tenant ───────────────────────────────────────────
    let (_, audit) = api
        .get("admin-tok", "/v1/audit?tenant=acme&limit=500")
        .await;
    let actions: Vec<&str> = audit["entries"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|e| e["action"].as_str())
        .collect();
    for a in [
        "tenant.create",
        "connection.upsert",
        "connect.start",
        "connect.complete",
        "connection.needs_reauth",
    ] {
        assert!(actions.contains(&a), "{a} missing from {actions:?}");
    }

    // ── Delete cascade ─────────────────────────────────────────────────────
    #[cfg(feature = "catalog")]
    let acme_edges = |lineage: &serde_json::Value| -> usize {
        lineage["edges"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["pipeline"].as_str().unwrap_or("").starts_with("acme::"))
            .count()
    };
    #[cfg(feature = "catalog")]
    {
        let (_, u) = api
            .get("admin-tok", "/v1/usage?tenant=acme&include_records=true")
            .await;
        assert!(!u["records"].as_array().unwrap().is_empty(), "{u}");
        // A tenant run's catalog rows are recorded under its namespace
        // (#789 SERVE-51).
        let (_, lineage) = api.get("admin-tok", "/v1/catalog/lineage").await;
        assert!(acme_edges(&lineage) >= 1, "{lineage}");
    }
    let (code, report) = api
        .send(
            reqwest::Method::DELETE,
            "admin-tok",
            "/v1/tenants/acme",
            None,
        )
        .await;
    assert_eq!(code, 200, "{report}");
    assert!(report["runs"].as_u64().unwrap() >= 4, "{report}");
    assert!(
        report["state_keys_deleted"].as_u64().unwrap() >= 1,
        "{report}"
    );
    // Usage rows are keyed by invocation id; they go with the tenant
    // (#789 SERVE-30).
    #[cfg(feature = "catalog")]
    {
        assert!(report["usage_records"].as_u64().unwrap() >= 1, "{report}");
        let (_, u) = api
            .get("admin-tok", "/v1/usage?tenant=acme&include_records=true")
            .await;
        assert!(u["records"].as_array().unwrap().is_empty(), "{u}");
        assert!(report["catalog_edges"].as_u64().unwrap() >= 1, "{report}");
        assert!(
            report["catalog_datasets"].as_u64().unwrap() >= 1,
            "{report}"
        );
        let (_, lineage) = api.get("admin-tok", "/v1/catalog/lineage").await;
        assert_eq!(acme_edges(&lineage), 0, "{lineage}");
    }
    let (code, _) = api.get("admin-tok", "/v1/tenants/acme").await;
    assert_eq!(code, 404);
    let (_, page) = api.get("admin-tok", "/v1/runs?tenant=acme").await;
    assert!(page["runs"].as_array().unwrap().is_empty());
    let (_, conns) = api.get("admin-tok", "/v1/tenants/globex/connections").await;
    assert_eq!(
        conns.as_array().unwrap().len(),
        1,
        "another tenant is untouched"
    );
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

/// A `schedule` trigger with `tenants: all` runs a template once per tenant
/// per tick, each run keyed by the tick and the tenant.
#[cfg(all(feature = "triggers", feature = "schedule"))]
#[tokio::test(flavor = "multi_thread")]
async fn a_schedule_trigger_fans_a_template_out_to_every_tenant() {
    let dir = tempfile::tempdir().unwrap();
    let data = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"items": [{"id": 1}]})))
        .mount(&data)
        .await;
    let out = dir.path().join("out");
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&out).unwrap();
    let triggers = dir.path().join("triggers.yaml");
    std::fs::write(
        &triggers,
        "version: 1\ntriggers:\n  - name: every-second\n    type: schedule\n    cron: \"* * * * * *\"\n    template: { id: tenant-sync }\n    tenants: all\n",
    )
    .unwrap();
    let api = spawn_with(dir.path(), None, "http://127.0.0.1:9", Some(triggers)).await;
    for t in ["acme", "globex"] {
        let (code, _) = api.post("admin-tok", "/v1/tenants", json!({"id": t})).await;
        assert_eq!(code, 201);
        let (code, _) = api
            .post(
                "op-tok",
                &format!("/v1/tenants/{t}/connections"),
                json!({"name": "api", "provider": {"type": "static", "config": {"token": "t"}}}),
            )
            .await;
        assert_eq!(code, 201);
    }
    let (code, reg) = api
        .post(
            "admin-tok",
            "/v1/templates",
            json!({"config": template(&data.uri(), &out, &state_dir), "launch": true}),
        )
        .await;
    assert_eq!(code, 201, "{reg}");
    for t in ["acme", "globex"] {
        let mut done = false;
        for _ in 0..400 {
            let (_, page) = api.get("admin-tok", &format!("/v1/runs?tenant={t}")).await;
            if page["runs"].as_array().unwrap().iter().any(|r| {
                r["status"] == "completed"
                    && r["labels"]["faucet.trigger.name"] == "every-second"
                    && r["labels"]["faucet.trigger.tick"].is_string()
            }) {
                done = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(done, "no scheduled run completed for {t}");
        assert!(lines(&out.join(format!("out-{t}.jsonl"))) >= 1);
    }
}

/// SERVE-10: one tenant whose fire fails deterministically (a missing label
/// its `${tenant.labels.*}` routing needs) must not stop the schedule for the
/// other tenants — the schedule advances tick after tick.
#[cfg(all(feature = "triggers", feature = "schedule"))]
#[tokio::test(flavor = "multi_thread")]
async fn a_failing_tenant_does_not_stop_a_scheduled_fan_out() {
    let dir = tempfile::tempdir().unwrap();
    let data = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"items": [{"id": 1}]})))
        .mount(&data)
        .await;
    let out = dir.path().join("out");
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&out).unwrap();
    let triggers = dir.path().join("triggers.yaml");
    std::fs::write(
        &triggers,
        "version: 1\ntriggers:\n  - name: every-second\n    type: schedule\n    cron: \"* * * * * *\"\n    template: { id: tenant-sync }\n    tenants: all\n",
    )
    .unwrap();
    let api = spawn_with(dir.path(), None, "http://127.0.0.1:9", Some(triggers)).await;
    for (t, labels) in [
        ("acme", json!({"region": "eu"})),
        ("broken", json!({})),
        ("globex", json!({"region": "us"})),
    ] {
        let (code, _) = api
            .post(
                "admin-tok",
                "/v1/tenants",
                json!({"id": t, "labels": labels}),
            )
            .await;
        assert_eq!(code, 201);
        let (code, _) = api
            .post(
                "op-tok",
                &format!("/v1/tenants/{t}/connections"),
                json!({"name": "api", "provider": {"type": "static", "config": {"token": "t"}}}),
            )
            .await;
        assert_eq!(code, 201);
    }
    let body = template(&data.uri(), &out, &state_dir).replace(
        "out-${tenant.id}.jsonl",
        "out-${tenant.id}-${tenant.labels.region}.jsonl",
    );
    let (code, reg) = api
        .post(
            "admin-tok",
            "/v1/templates",
            json!({"config": body, "launch": true}),
        )
        .await;
    assert_eq!(code, 201, "{reg}");
    for t in ["acme", "globex"] {
        let mut ticks = std::collections::BTreeSet::new();
        for _ in 0..400 {
            let (_, page) = api.get("admin-tok", &format!("/v1/runs?tenant={t}")).await;
            for r in page["runs"].as_array().unwrap() {
                if r["labels"]["faucet.trigger.name"] == "every-second" {
                    ticks.insert(r["labels"]["faucet.trigger.tick"].to_string());
                }
            }
            if ticks.len() >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(
            ticks.len() >= 2,
            "the schedule stalled for {t} behind the failing tenant: {ticks:?}"
        );
    }
    let (_, page) = api.get("admin-tok", "/v1/runs?tenant=broken").await;
    assert!(page["runs"].as_array().unwrap().is_empty());
}

/// `max_concurrent_runs` refuses a submission over the limit with 429.
#[tokio::test(flavor = "multi_thread")]
async fn a_tenant_over_its_concurrency_limit_is_refused_with_429() {
    let dir = tempfile::tempdir().unwrap();
    let slow = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/slow"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!([{"id": 1}]))
                .set_delay(Duration::from_secs(30)),
        )
        .mount(&slow)
        .await;
    let api = spawn(dir.path(), None, &slow.uri()).await;
    let (code, t) = api
        .post(
            "admin-tok",
            "/v1/tenants",
            json!({"id": "busy", "limits": {"max_concurrent_runs": 1}}),
        )
        .await;
    assert_eq!(code, 201, "{t}");
    let config = format!(
        "version: 1\nname: busy-sync\npipeline:\n  source:\n    type: rest\n    config:\n      base_url: \"{}\"\n      path: /slow\n  sink:\n    type: jsonl\n    config:\n      path: \"{}\"\n",
        slow.uri(),
        dir.path().join("busy.jsonl").display()
    );
    let (code, first) = api
        .post("op-tok", "/v1/tenants/busy/runs", json!({"config": config}))
        .await;
    assert_eq!(code, 202, "{first}");
    let (code, err) = api
        .post("op-tok", "/v1/tenants/busy/runs", json!({"config": config}))
        .await;
    assert_eq!(code, 429, "{err}");
    assert_eq!(err["error"]["code"], "limit_exceeded", "{err}");
    let run_id = first["run_id"].as_str().unwrap();
    let (code, _) = api
        .post("admin-tok", &format!("/v1/runs/{run_id}/cancel"), json!({}))
        .await;
    assert!(code == 202 || code == 200, "cancel returned {code}");
}

#[tokio::test(flavor = "multi_thread")]
async fn tenant_notification_secrets_are_sealed_and_masked_and_run_credentials_are_hidden() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("h.db");
    let api = spawn(
        dir.path(),
        Some(format!("sqlite:{}", db.display())),
        "http://127.0.0.1:9",
    )
    .await;
    let rule = json!({"name": "ops", "on": ["connection_needs_reauth"],
        "channel": {"type": "webhook", "config": {
            "url": "https://hooks.example/abc-secret-path", "hmac_secret": "hmac-s3cret"}}});
    let (code, t) = api
        .post(
            "admin-tok",
            "/v1/tenants",
            json!({"id": "acme", "notifications": [rule]}),
        )
        .await;
    assert_eq!(code, 201, "{t}");
    for tok in ["admin-tok", "view-tok"] {
        let (code, t) = api.get(tok, "/v1/tenants/acme").await;
        assert_eq!(code, 200, "{t}");
        let text = t.to_string();
        assert!(
            !text.contains("abc-secret-path") && !text.contains("hmac-s3cret"),
            "{text}"
        );
        assert_eq!(
            t["notifications"][0]["channel"]["config"]["hmac_secret"],
            "***"
        );
        assert_eq!(t["notifications"][0]["name"], "ops");
        assert!(t.get("notifications_sealed").is_none(), "{t}");
    }
    // A tenant principal may not override a template's environment (SERVE-23).
    let (code, r) = api
        .post(
            "acme-tok",
            "/v1/tenants/acme/templates/any/runs",
            json!({"env": {"API_HOST": "elsewhere"}}),
        )
        .await;
    assert_eq!(code, 403, "{r}");
    let (_, list) = api.get("view-tok", "/v1/tenants").await;
    assert!(!list.to_string().contains("hmac-s3cret"));
    let (code, _) = api
        .send(
            reqwest::Method::PATCH,
            "admin-tok",
            "/v1/tenants/acme",
            Some(json!({"notifications": [{"name": "bad"}]})),
        )
        .await;
    assert_eq!(code, 400);
    // A run whose tenant mapping cannot be written is not recorded at all
    // (#789 SERVE-29): no stranded `queued` record behind a 500.
    {
        let pool = sqlx::SqlitePool::connect(&format!("sqlite:{}", db.display()))
            .await
            .unwrap();
        sqlx::query("DROP TABLE faucet_tenant_runs")
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;
    }
    let cfg = format!(
        "version: 1\nname: linkfail\npipeline:\n  source:\n    type: rest\n    config:\n      base_url: http://127.0.0.1:9\n      path: /x\n  sink:\n    type: jsonl\n    config:\n      path: {}/l.jsonl\n",
        dir.path().display()
    );
    let (code, r) = api
        .post("op-tok", "/v1/tenants/acme/runs", json!({ "config": cfg }))
        .await;
    assert_eq!(code, 500, "{r}");
    let (_, page) = api.get("admin-tok", "/v1/runs").await;
    assert!(
        !page.to_string().contains("linkfail"),
        "no record was written: {page}"
    );
    drop(api);
    let conn = rusqlite_free_read(&db, "acme");
    assert!(
        !conn.contains("hmac-s3cret") && !conn.contains("abc-secret-path"),
        "{conn}"
    );

    let api = spawn(dir.path(), None, "http://127.0.0.1:9").await;
    let cfg = format!(
        "version: 1\nname: cb\npipeline:\n  source:\n    type: rest\n    config:\n      base_url: http://127.0.0.1:9\n      path: /x\n  sink:\n    type: jsonl\n    config:\n      path: {}/o.jsonl\n",
        dir.path().display()
    );
    let (code, sub) = api
        .post(
            "op-tok",
            "/v1/runs",
            json!({"config": cfg, "callback": {"url": "https://user:pw@cb.example/done",
                "headers": {"Authorization": "Bearer cb-token"}}}),
        )
        .await;
    assert_eq!(code, 202, "{sub}");
    let id = sub["run_id"].as_str().unwrap();
    api.wait_run(id).await;
    for tok in ["view-tok", "admin-tok"] {
        let (_, rec) = api.get(tok, &format!("/v1/runs/{id}")).await;
        let text = rec.to_string();
        assert!(
            !text.contains("cb-token") && !text.contains("pw@"),
            "{text}"
        );
        assert_eq!(rec["callback"]["headers"]["Authorization"], "***");
        let (_, page) = api.get(tok, "/v1/runs").await;
        assert!(!page.to_string().contains("cb-token"));
    }
}

/// Every row of the SQLite tenants table as text, read without the server.
fn rusqlite_free_read(db: &std::path::Path, needle_tenant: &str) -> String {
    let mut bytes = std::fs::read(db).unwrap();
    if let Ok(wal) = std::fs::read(db.with_extension("db-wal")) {
        bytes.extend(wal);
    }
    let text = String::from_utf8_lossy(&bytes).into_owned();
    assert!(
        text.contains(needle_tenant),
        "tenant row missing from the database"
    );
    text
}
