//! #874: a run error is redacted where it is created, so the text persisted
//! to the shared history database is already clean — read straight from the
//! table, and served masked by an instance whose secrets registry never held
//! the value.
#![cfg(all(
    feature = "serve-history-sqlite",
    feature = "source-csv",
    feature = "sink-jsonl"
))]
#![allow(clippy::disallowed_methods)] // tests drive time and raw clients directly

use clap::Parser;
use faucet_cli::cli::{Cli, Command};
use serde_json::{Value, json};
use std::time::Duration;

const SECRET: &str = "s3cr3t-874-dir-value";

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

async fn spawn(db: &std::path::Path) -> String {
    let port = free_port();
    let listen = format!("127.0.0.1:{port}");
    let history = format!("sqlite:{}", db.display());
    let cli = Cli::try_parse_from([
        "faucet",
        "serve",
        "--listen",
        &listen,
        "--no-auth",
        "--no-env-file",
        "--history",
        &history,
        "--allow-env",
        "FAUCET_874_*",
    ])
    .unwrap();
    let Command::Serve(args) = cli.command else {
        panic!("serve args");
    };
    let mut config = faucet_cli::serve::ServeConfig::from_args(args).unwrap();
    config.log_level = "warn".into();
    tokio::spawn(async move {
        let _ = faucet_cli::serve::run_server(config, Default::default()).await;
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

async fn wait_terminal(client: &reqwest::Client, base: &str, id: &str) -> Value {
    for _ in 0..800 {
        let rec: Value = client
            .get(format!("{base}/v1/runs/{id}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if matches!(rec["status"].as_str(), Some("failed" | "completed")) {
            return rec;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("run {id} did not finish");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_run_error_is_stored_and_served_redacted() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("history.db");
    // SAFETY: a test-only variable, set before the server reads it.
    unsafe { std::env::set_var("FAUCET_874_SECRET_DIR", SECRET) };
    let base = spawn(&db).await;
    let client = reqwest::Client::new();
    let config = format!(
        "version: 1\nname: redaction\npipeline:\n  source: {{ type: csv, config: {{ path: \"{}/${{secret:FAUCET_874_SECRET_DIR}}/missing.csv\" }} }}\n  sink: {{ type: jsonl, config: {{ path: \"{}\" }} }}\n",
        dir.path().display(),
        dir.path().join("out.jsonl").display()
    );
    let resp: Value = client
        .post(format!("{base}/v1/runs"))
        .json(&json!({ "config": config }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let id = resp["run_id"].as_str().expect("run accepted").to_string();
    let rec = wait_terminal(&client, &base, &id).await;
    assert_eq!(rec["status"], "failed", "{rec}");

    let pool = sqlx::SqlitePool::connect(&format!("sqlite:{}", db.display()))
        .await
        .unwrap();
    let (body,): (String,) = sqlx::query_as("SELECT body FROM faucet_serve_runs WHERE run_id = ?")
        .bind(&id)
        .fetch_one(&pool)
        .await
        .unwrap();
    let stored: Value = serde_json::from_str(&body).unwrap();
    let error = stored["invocations"][0]["error"]
        .as_str()
        .expect("the invocation error is stored");
    assert!(
        !body.contains(SECRET),
        "stored body leaks the secret: {body}"
    );
    assert!(error.contains("***"), "{error}");

    faucet_cli::secrets::registry::unregister(SECRET);
    let peer = spawn(&db).await;
    let served: Value = client
        .get(format!("{peer}/v1/runs/{id}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let text = served.to_string();
    assert!(!text.contains(SECRET), "{text}");
    assert_eq!(served["invocations"][0]["error"], error, "{served}");
}
