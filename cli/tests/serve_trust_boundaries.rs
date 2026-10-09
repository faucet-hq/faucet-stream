//! The trust boundaries of a `faucet serve` that faces more than one party:
//! `--auth-config` tokens resolve their references, a loaded config can never
//! read the server's own credentials, subprocess connectors need
//! `--allow-subprocess-connectors`, a viewer cannot make the server load a
//! config, `--require-approval` gates the template lifecycle, and a gated
//! template trigger stores no resolved secret.
#![cfg(all(feature = "serve", feature = "source-csv", feature = "sink-jsonl"))]

use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::time::Duration;

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn args(port: u16, auth_config: PathBuf) -> faucet_cli::cli::ServeArgs {
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
        triggers: None,
        templates_sync: None,
        policy: None,
        otel_config: None,
        callback_allow_host: Vec::new(),
        mcp: false,
        mcp_allow_mutations: false,
        require_approval: Vec::new(),
        approval_expiry_secs: 86_400,
        require_template_tests: false,
        vault_key: None,
        vault_previous_key: Vec::new(),
        connect_providers: None,
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
}

const PRINCIPALS: &str = "principals:\n\
    \x20 - { name: alice, token: admin-tok, role: admin }\n\
    \x20 - { name: carol, token: op-tok, role: operator }\n\
    \x20 - { name: bob, token: viewer-tok, role: viewer }\n";

async fn spawn(
    dir: &Path,
    auth: &str,
    tweak: impl FnOnce(&mut faucet_cli::cli::ServeArgs),
    mcp: bool,
) -> Api {
    let auth_path = dir.join("auth.yaml");
    std::fs::write(&auth_path, auth).unwrap();
    let port = free_port();
    let mut a = args(port, auth_path);
    tweak(&mut a);
    let mut config = faucet_cli::serve::ServeConfig::from_args(a).unwrap();
    config.log_level = "warn".into();
    tokio::spawn(async move {
        let _ = faucet_cli::serve::run_server(
            config,
            faucet_cli::serve::McpServeSettings {
                enabled: mcp,
                allow_mutations: mcp,
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
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Api {
        base: format!("http://127.0.0.1:{port}"),
        client,
    }
}

fn csv_input(dir: &Path) -> PathBuf {
    let input = dir.join("in.csv");
    std::fs::write(&input, "id,name\n1,a\n2,b\n").unwrap();
    input
}

fn csv_config(dir: &Path, path_value: &str) -> String {
    format!(
        "version: 1\nname: trust\npipeline:\n  source: {{ type: csv, config: {{ path: \"{path_value}\" }} }}\n  sink: {{ type: jsonl, config: {{ path: \"{}\" }} }}\n",
        dir.join("out.jsonl").display()
    )
}

fn singer_config(dir: &Path) -> String {
    format!(
        "version: 1\nname: tap\npipeline:\n  source: {{ type: singer, config: {{ executable: /bin/sh, args: [\"-c\", \"true\"], stream: x }} }}\n  sink: {{ type: jsonl, config: {{ path: \"{}\" }} }}\n",
        dir.join("out.jsonl").display()
    )
}

fn message(v: &Value) -> String {
    v["error"]["message"]
        .as_str()
        .unwrap_or_default()
        .to_string()
}

#[tokio::test(flavor = "multi_thread")]
async fn auth_config_references_resolve_and_stay_out_of_reach() {
    let dir = tempfile::tempdir().unwrap();
    let token_file = dir.path().join("viewer.token");
    std::fs::write(&token_file, "viewer-from-file\n").unwrap();
    // SAFETY: a variable only this test reads, set before the server starts.
    unsafe { std::env::set_var("FAUCET_TRUST_TEST_ADMIN_TOKEN", "admin-from-env") };
    let auth = format!(
        "principals:\n\
         \x20 - {{ name: alice, token: \"${{env:FAUCET_TRUST_TEST_ADMIN_TOKEN}}\", role: admin }}\n\
         \x20 - {{ name: bob, token: \"${{file:{}}}\", role: viewer }}\n",
        token_file.display()
    );
    let api = spawn(dir.path(), &auth, |_| {}, false).await;

    let (code, _) = api
        .get("${env:FAUCET_TRUST_TEST_ADMIN_TOKEN}", "/v1/runs")
        .await;
    assert_eq!(code, 401, "the documented literal must not authenticate");
    let (code, me) = api.get("admin-from-env", "/v1/whoami").await;
    assert_eq!(code, 200, "{me}");
    assert_eq!(me["role"], "admin");
    let (code, me) = api.get("viewer-from-file", "/v1/whoami").await;
    assert_eq!(code, 200, "{me}");
    assert_eq!(me["role"], "viewer");

    let input = csv_input(dir.path());
    let (code, r) = api
        .post(
            "admin-from-env",
            "/v1/runs",
            json!({"config": csv_config(dir.path(), &input.display().to_string())}),
        )
        .await;
    assert_eq!(code, 202, "{r}");

    for reference in [
        "${env:FAUCET_TRUST_TEST_ADMIN_TOKEN}".to_string(),
        "${secret:FAUCET_VAULT_KEY}".to_string(),
        format!("${{file:{}}}", token_file.display()),
        "${file:/proc/self/environ}".to_string(),
        "${file:/nonexistent/../proc/self/environ}".to_string(),
    ] {
        let (code, err) = api
            .post(
                "admin-from-env",
                "/v1/runs",
                json!({"config": csv_config(dir.path(), &reference)}),
            )
            .await;
        assert_eq!(code, 403, "{reference}: {err}");
        assert!(message(&err).contains("server's own credentials"), "{err}");
        assert!(!err.to_string().contains("admin-from-env"), "{err}");
    }
}

#[test]
fn an_unresolvable_auth_config_token_refuses_to_start() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("auth.yaml");
    std::fs::write(
        &path,
        "principals:\n  - { name: a, token: \"${env:FAUCET_TRUST_TEST_UNSET_VAR}\", role: admin }\n",
    )
    .unwrap();
    let err = faucet_cli::serve::ServeConfig::from_args(args(free_port(), path.clone()))
        .unwrap_err()
        .to_string();
    assert!(err.contains("FAUCET_TRUST_TEST_UNSET_VAR"), "{err}");

    std::fs::write(
        &path,
        "principals:\n  - { name: a, token: \"$${literal}\", role: admin }\n",
    )
    .unwrap();
    let err = faucet_cli::serve::ServeConfig::from_args(args(free_port(), path))
        .unwrap_err()
        .to_string();
    assert!(err.contains("unresolved"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn subprocess_connectors_need_the_server_flag() {
    let dir = tempfile::tempdir().unwrap();
    let api = spawn(dir.path(), PRINCIPALS, |_| {}, true).await;
    let config = singer_config(dir.path());
    for route in ["/v1/runs", "/v1/doctor", "/v1/plan"] {
        let (code, err) = api
            .post("admin-tok", route, json!({"config": config}))
            .await;
        assert_eq!(code, 422, "{route}: {err}");
        assert!(
            message(&err).contains("--allow-subprocess-connectors"),
            "{route}: {err}"
        );
    }
    #[cfg(feature = "mcp")]
    for tool in ["validate_config", "preview", "run_pipeline"] {
        let (code, body) = api
            .post(
                "admin-tok",
                "/mcp",
                json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                       "params": {"name": tool, "arguments": {"config": config}}}),
            )
            .await;
        assert_eq!(code, 200, "{tool}: {body}");
        let text = body.to_string();
        assert!(text.contains("\"isError\":true"), "{tool}: {text}");
        assert!(
            text.contains("--allow-subprocess-connectors"),
            "{tool}: {text}"
        );
    }

    let allowed = spawn(
        dir.path(),
        PRINCIPALS,
        |a| a.allow_subprocess_connectors = true,
        false,
    )
    .await;
    let (code, body) = allowed
        .post("admin-tok", "/v1/plan", json!({"config": config}))
        .await;
    assert!(
        !message(&body).contains("--allow-subprocess-connectors"),
        "{code}: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_viewer_cannot_make_the_server_load_a_config() {
    let dir = tempfile::tempdir().unwrap();
    let api = spawn(dir.path(), PRINCIPALS, |_| {}, false).await;
    let input = csv_input(dir.path());
    let config = csv_config(dir.path(), &input.display().to_string());
    for (route, body) in [
        ("/v1/plan", json!({"config": config})),
        ("/v1/status", json!({"config": config})),
        ("/v1/mirror/trust", json!({"config": config})),
    ] {
        let (code, err) = api.post("viewer-tok", route, body.clone()).await;
        assert_eq!(code, 403, "{route}: {err}");
        let (code, ok) = api.post("op-tok", route, body).await;
        assert_ne!(code, 403, "{route}: {ok}");
    }
    let (code, err) = api
        .get(
            "viewer-tok",
            &format!("/v1/status?config={}", url_encode(&config)),
        )
        .await;
    assert_eq!(code, 403, "{err}");
    #[cfg(feature = "templates")]
    {
        let (code, _) = api.get("viewer-tok", "/v1/status?template=ghost").await;
        assert_eq!(code, 404, "a template is still readable by a viewer");
    }
}

fn url_encode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

#[cfg(feature = "templates")]
#[tokio::test(flavor = "multi_thread")]
async fn approval_gates_the_template_lifecycle_and_keeps_secrets_out_of_requests() {
    let dir = tempfile::tempdir().unwrap();
    let input = csv_input(dir.path());
    // SAFETY: a variable only this test reads, set before the server starts.
    unsafe { std::env::set_var("FAUCET_TRUST_TEST_INPUT", input.display().to_string()) };
    let api = spawn(
        dir.path(),
        PRINCIPALS,
        |a| {
            a.require_approval = vec!["run,template_launch".into()];
            a.callback_allow_host = vec!["127.0.0.1".into()];
        },
        false,
    )
    .await;
    let body = format!(
        "version: 1\nkind: pipeline\nname: gated\npipeline:\n  source: {{ type: csv, config: {{ path: \"${{env:FAUCET_TRUST_TEST_INPUT}}\" }} }}\n  sink: {{ type: jsonl, config: {{ path: \"{}\" }} }}\n",
        dir.path().join("gated.jsonl").display()
    );
    let with_secret = body.replace(
        "name: gated\n",
        "name: with-secret\nparams:\n  token: { type: string, secret: true, required: true }\n",
    );

    let (code, reg) = api
        .post(
            "admin-tok",
            "/v1/templates",
            json!({"config": body, "id": "gated"}),
        )
        .await;
    assert_eq!(code, 201, "a plain register is not gated: {reg}");
    let (code, reg) = api
        .post(
            "admin-tok",
            "/v1/templates",
            json!({"config": with_secret, "id": "with-secret"}),
        )
        .await;
    assert_eq!(code, 201, "{reg}");
    for (route, payload) in [
        ("/v1/templates/gated/launch", json!({})),
        ("/v1/templates/gated/rollback", json!({})),
        (
            "/v1/templates/gated/tags",
            json!({"tag": "prod", "version": 1}),
        ),
        (
            "/v1/templates",
            json!({"config": body, "id": "gated", "launch": true}),
        ),
    ] {
        let (code, err) = api.post("admin-tok", route, payload).await;
        assert_eq!(code, 409, "{route}: {err}");
        assert!(message(&err).contains("POST /v1/changes"), "{route}: {err}");
    }

    // A secret param on a gated trigger is refused: it would be stored.
    let (code, err) = api
        .post(
            "op-tok",
            "/v1/templates/with-secret/runs",
            json!({"version": 1, "params": {"token": "hunter2-secret"}}),
        )
        .await;
    assert_eq!(code, 422, "{err}");
    assert!(message(&err).contains("approved change request"), "{err}");

    // Without one, the stored request carries the unresolved reference.
    let (code, pending) = api
        .post(
            "op-tok",
            "/v1/templates/gated/runs",
            json!({"version": 1, "callback": {"url": "http://127.0.0.1:9/cb", "headers": {"authorization": "Bearer cb-secret"}}}),
        )
        .await;
    assert_eq!(code, 202, "{pending}");
    assert_eq!(pending["status"], "pending_approval", "{pending}");
    let change_id = pending["change_id"].as_str().unwrap();
    let (_, as_admin) = api
        .get("admin-tok", &format!("/v1/changes/{change_id}"))
        .await;
    let stored = as_admin["payload"]["config"].as_str().unwrap();
    assert!(
        stored.contains("${env:FAUCET_TRUST_TEST_INPUT}"),
        "{stored}"
    );
    assert!(!stored.contains(&input.display().to_string()), "{stored}");
    assert_eq!(
        as_admin["payload"]["callback"]["headers"]["authorization"],
        "Bearer cb-secret"
    );
    let (_, as_operator) = api.get("op-tok", &format!("/v1/changes/{change_id}")).await;
    assert!(
        as_operator["payload"]["config"]
            .as_str()
            .unwrap()
            .contains("csv")
    );
    assert_eq!(
        as_operator["payload"]["callback"]["headers"]["authorization"],
        "***"
    );
    let (_, as_viewer) = api
        .get("viewer-tok", &format!("/v1/changes/{change_id}"))
        .await;
    assert_eq!(as_viewer["payload"]["config"], "***");
    let (_, listed) = api.get("viewer-tok", "/v1/changes").await;
    assert!(!listed.to_string().contains("cb-secret"), "{listed}");
    assert!(
        !listed.to_string().contains("FAUCET_TRUST_TEST_INPUT"),
        "{listed}"
    );
}

#[cfg(all(feature = "templates", feature = "source-singer"))]
#[tokio::test(flavor = "multi_thread")]
async fn a_registered_template_may_use_a_subprocess_connector() {
    let dir = tempfile::tempdir().unwrap();
    let api = spawn(dir.path(), PRINCIPALS, |_| {}, false).await;
    let (code, reg) = api
        .post(
            "admin-tok",
            "/v1/templates",
            json!({"config": singer_config(dir.path()).replace("version: 1\n", "version: 1\nkind: pipeline\n"), "id": "tap", "launch": true}),
        )
        .await;
    assert_eq!(code, 201, "{reg}");
    let (code, r) = api
        .post("op-tok", "/v1/templates/tap/runs", json!({}))
        .await;
    assert_eq!(code, 202, "{r}");
}
