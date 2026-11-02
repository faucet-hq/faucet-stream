//! `faucet serve` answers only the host names and origins it is configured
//! for, takes JSON bodies where it parses JSON, throttles failed bearer
//! tokens, and keeps callback credentials out of change requests.
#![cfg(all(
    feature = "serve",
    feature = "source-csv",
    feature = "sink-jsonl",
    feature = "serve-history-sqlite"
))]
#![allow(clippy::disallowed_methods)] // tests drive time and raw clients directly

use clap::Parser;
use faucet_cli::cli::{Cli, Command};
use serde_json::{Value, json};
use std::time::Duration;

#[path = "support/ports.rs"]
mod ports;
use ports::free_port;

const READ: &str = "viewer-token-0123456789";
const WRITE: &str = "operator-token-0123456789";
const ADMIN: &str = "admin-token-0123456789ab";

async fn spawn(extra: &[&str]) -> String {
    let port = free_port();
    let listen = format!("127.0.0.1:{port}");
    let mut argv = vec!["faucet", "serve", "--listen", &listen, "--no-env-file"];
    argv.extend_from_slice(extra);
    let cli = Cli::try_parse_from(argv).unwrap();
    let Command::Serve(args) = cli.command else {
        panic!("serve args");
    };
    let mut config = faucet_cli::serve::ServeConfig::from_args(*args).unwrap();
    config.log_level = "warn".into();
    let mcp = faucet_cli::serve::McpServeSettings {
        enabled: true,
        allow_mutations: false,
    };
    tokio::spawn(async move {
        let _ = faucet_cli::serve::run_server(config, mcp).await;
    });
    let base = format!("http://{listen}");
    let client = reqwest::Client::new();
    for _ in 0..400 {
        if client
            .get(format!("{base}/healthz"))
            .send()
            .await
            .is_ok_and(|r| r.status().is_success())
        {
            return base;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("server did not come up");
}

fn rbac(history: &str) -> Vec<String> {
    vec![
        "--read-token".into(),
        READ.into(),
        "--write-token".into(),
        WRITE.into(),
        "--admin-token".into(),
        ADMIN.into(),
        "--history".into(),
        history.into(),
    ]
}

#[tokio::test(flavor = "multi_thread")]
async fn a_no_auth_server_answers_only_its_own_host_names_and_origins() {
    let base = spawn(&["--no-auth", "--allowed-origin", "https://app.example"]).await;
    let port = base.rsplit(':').next().unwrap().to_string();
    let client = reqwest::Client::new();

    let ok = client.get(format!("{base}/v1/runs")).send().await.unwrap();
    assert_eq!(ok.status(), 200);
    assert_eq!(ok.headers()["x-content-type-options"], "nosniff");
    assert_eq!(ok.headers()["cross-origin-resource-policy"], "same-origin");

    let other = client
        .get(format!("{base}/v1/runs"))
        .header("host", format!("other.example:{port}"))
        .send()
        .await
        .unwrap();
    assert_eq!(other.status(), 403);
    let probe = client
        .get(format!("{base}/healthz"))
        .header("host", format!("other.example:{port}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        probe.status(),
        200,
        "health probes reach the server by address"
    );

    let submit = |origin: Option<String>| {
        let mut req = client
            .post(format!("{base}/v1/runs"))
            .json(&json!({ "config": "not: [valid" }));
        if let Some(o) = origin {
            req = req.header("origin", o);
        }
        req.send()
    };
    let cross = submit(Some("https://other.example".into())).await.unwrap();
    assert_eq!(cross.status(), 403);
    let null = submit(Some("null".into())).await.unwrap();
    assert_eq!(null.status(), 403);
    for allowed in [
        Some(format!("http://127.0.0.1:{port}")),
        Some("https://app.example".into()),
        None,
    ] {
        let resp = submit(allowed.clone()).await.unwrap();
        assert_ne!(resp.status(), 403, "{allowed:?}");
    }
}

#[cfg(feature = "mcp")]
#[tokio::test(flavor = "multi_thread")]
async fn the_mcp_endpoint_takes_only_json_bodies() {
    let base = spawn(&["--no-auth"]).await;
    let client = reqwest::Client::new();
    let text = client
        .post(format!("{base}/mcp"))
        .header("content-type", "text/plain")
        .body(r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(text.status(), 415);
    let json_rpc = client
        .post(format!("{base}/mcp"))
        .json(&json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}))
        .send()
        .await
        .unwrap();
    assert_eq!(json_rpc.status(), 200);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_authenticating_server_checks_hosts_once_they_are_listed() {
    let dir = tempfile::tempdir().unwrap();
    let history = format!("sqlite:{}", dir.path().join("h.db").display());
    let mut args = rbac(&history);
    args.extend(["--allowed-host".into(), "faucet.example".into()]);
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let base = spawn(&args).await;
    let client = reqwest::Client::new();
    let listed = client
        .get(format!("{base}/v1/runs"))
        .bearer_auth(READ)
        .header("host", "faucet.example")
        .send()
        .await
        .unwrap();
    assert_eq!(listed.status(), 200);
    let other = client
        .get(format!("{base}/v1/runs"))
        .bearer_auth(READ)
        .header("host", "other.example")
        .send()
        .await
        .unwrap();
    assert_eq!(other.status(), 403);
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_tokens_are_audited_and_then_refused_for_a_while() {
    let dir = tempfile::tempdir().unwrap();
    let history = format!("sqlite:{}", dir.path().join("h.db").display());
    let args = rbac(&history);
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let base = spawn(&args).await;
    let client = reqwest::Client::new();
    let attempt = |token: &'static str| {
        client
            .get(format!("{base}/v1/runs"))
            .bearer_auth(token)
            .send()
    };
    assert_eq!(attempt("wrong-token").await.unwrap().status(), 401);
    let audit: Value = client
        .get(format!("{base}/v1/audit?action=auth.failed"))
        .bearer_auth(ADMIN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let entries = audit["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1, "{audit}");
    assert_eq!(entries[0]["result"], "denied");
    assert_eq!(entries[0]["source_ip"], "127.0.0.1");
    assert!(!audit.to_string().contains("wrong-token"));

    for _ in 1..faucet_cli::serve::auth::MAX_FAILURES {
        assert_eq!(attempt("wrong-token").await.unwrap().status(), 401);
    }
    assert_eq!(attempt("wrong-token").await.unwrap().status(), 429);
    assert_eq!(
        attempt("wrong-token").await.unwrap().status(),
        429,
        "invalid tokens from the address are refused until its window ends"
    );
    assert_eq!(
        attempt(ADMIN).await.unwrap().status(),
        200,
        "a valid token from a locked address still proceeds"
    );
    let anonymous = client.get(format!("{base}/v1/runs")).send().await.unwrap();
    assert_eq!(anonymous.status(), 401);
}

#[tokio::test(flavor = "multi_thread")]
async fn change_request_callbacks_keep_their_credentials_to_admins() {
    let dir = tempfile::tempdir().unwrap();
    let history = format!("sqlite:{}", dir.path().join("h.db").display());
    let args = rbac(&history);
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let base = spawn(&args).await;
    let client = reqwest::Client::new();
    let csv = dir.path().join("in.csv");
    std::fs::write(&csv, "a\n1\n").unwrap();
    let config = format!(
        "version: 1\nname: cr\npipeline:\n  source:\n    type: csv\n    config:\n      path: {}\n  sink:\n    type: jsonl\n    config:\n      path: {}\n",
        csv.display(),
        dir.path().join("out.jsonl").display()
    );
    let propose = |callback: Value| {
        client
            .post(format!("{base}/v1/changes"))
            .bearer_auth(WRITE)
            .json(&json!({"kind": "run", "payload": {"config": config, "callback": callback}}))
            .send()
    };

    let userinfo = propose(json!({"url": "https://u:dummy-secret@hooks.example/x"}))
        .await
        .unwrap();
    assert_eq!(userinfo.status(), 422);
    let body: Value = userinfo.json().await.unwrap();
    assert!(!body.to_string().contains("dummy-secret"), "{body}");

    let headers = propose(json!({
        "url": "https://hooks.example/x",
        "headers": {"Authorization": "Bearer dummy-secret"}
    }))
    .await
    .unwrap();
    assert_eq!(
        headers.status(),
        422,
        "a persistent history refuses callback headers"
    );

    let created = propose(json!({"url": "https://hooks.example/x?token=dummy-secret&job=7"}))
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
    let created: Value = created.json().await.unwrap();
    let id = created["id"].as_str().unwrap();
    let read = |token: &'static str| {
        let url = format!("{base}/v1/changes/{id}");
        let client = client.clone();
        async move {
            client
                .get(url)
                .bearer_auth(token)
                .send()
                .await
                .unwrap()
                .json::<Value>()
                .await
                .unwrap()
        }
    };
    let viewer = read(READ).await;
    let url = viewer["payload"]["callback"]["url"].as_str().unwrap();
    assert!(
        !url.contains("dummy-secret") && url.contains("job=7"),
        "{url}"
    );
    let admin = read(ADMIN).await;
    assert!(
        admin["payload"]["callback"]["url"]
            .as_str()
            .unwrap()
            .contains("dummy-secret")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dlq_location_that_does_not_exist_is_a_client_error() {
    let dir = tempfile::tempdir().unwrap();
    let base = spawn(&["--no-auth"]).await;
    let client = reqwest::Client::new();
    for route in ["inspect", "discard"] {
        let resp = client
            .post(format!("{base}/v1/dlq/{route}"))
            .json(&json!({"location": dir.path().join("missing.jsonl")}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 400, "{route}");
        let body: Value = resp.json().await.unwrap();
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap()
                .contains("matched no files"),
            "{body}"
        );
    }
}
