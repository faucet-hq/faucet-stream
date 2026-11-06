//! #1050: a server whose Postgres run history becomes unreachable keeps
//! accepting runs, journals their records to `$FAUCET_DATA_DIR`, refuses
//! shared-config writes with 503, reports what it holds on `/readyz`, and
//! replays the journal into Postgres once it is back.
#![cfg(all(
    feature = "serve-history-postgres",
    feature = "templates",
    feature = "source-csv",
    feature = "sink-jsonl"
))]
#![allow(clippy::disallowed_methods)] // tests drive time and raw clients directly

use faucet_cli::serve::history::RunHistory;
use faucet_cli::serve::history::postgres::PostgresHistory;
use serde_json::{Value, json};
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use testcontainers::ImageExt;
use testcontainers::core::IntoContainerPort;
use testcontainers_modules::postgres::Postgres;

#[path = "support/ports.rs"]
mod ports;
use ports::free_port;

const AUTH: &str = "principals:\n  - { name: alice, token: alice-tok, role: admin }\n";

struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn call(method: reqwest::Method, url: String, body: Option<Value>) -> (u16, Value) {
    let client = reqwest::Client::new();
    let mut req = client.request(method, url).bearer_auth("alice-tok");
    if let Some(b) = body {
        req = req.json(&b);
    }
    match req.send().await {
        Ok(r) => (r.status().as_u16(), r.json().await.unwrap_or(Value::Null)),
        Err(_) => (0, Value::Null),
    }
}

async fn wait_terminal(base: &str, id: &str) -> Value {
    for _ in 0..600 {
        let (_, rec) = call(reqwest::Method::GET, format!("{base}/v1/runs/{id}"), None).await;
        if matches!(rec["status"].as_str(), Some("completed" | "failed")) {
            return rec;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("run {id} did not finish");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_degraded_server_journals_runs_and_replays_them_into_postgres() {
    if std::env::var_os("CARGO_LLVM_COV").is_some() {
        // A spawned, instrumented server is too slow for the lease windows.
        return;
    }
    let pg_port = free_port();
    let Some(pg) = faucet_conformance::containers::start_or_skip(
        || {
            Postgres::default()
                .with_tag("16-alpine")
                .with_mapped_port(pg_port, 5432.tcp())
        },
        &Default::default(),
    )
    .await
    else {
        return;
    };
    let url = format!("postgres://postgres:postgres@127.0.0.1:{pg_port}/postgres");

    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let auth = dir.path().join("auth.yaml");
    std::fs::write(&auth, AUTH).unwrap();
    std::fs::write(dir.path().join("in.csv"), "id\n1\n2\n").unwrap();
    let config = |name: &str| {
        format!(
            "version: 1\nname: {name}\npipeline:\n  source: {{ type: csv, config: {{ path: {} }} }}\n  sink: {{ type: jsonl, config: {{ path: {} }} }}\n",
            dir.path().join("in.csv").display(),
            dir.path().join(format!("{name}.jsonl")).display()
        )
    };
    let port = free_port();
    let base = format!("http://127.0.0.1:{port}");
    let _server = Server(
        Command::new(env!("CARGO_BIN_EXE_faucet"))
            .args([
                "serve",
                "--listen",
                &format!("127.0.0.1:{port}"),
                "--no-env-file",
            ])
            .arg("--auth-config")
            .arg(&auth)
            .args(["--history", &url, "--lease-ttl-secs", "3"])
            .env("FAUCET_DATA_DIR", &data)
            .env("FAUCET_LOG", "warn")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    for _ in 0..1200 {
        if call(reqwest::Method::GET, format!("{base}/healthz"), None)
            .await
            .0
            == 200
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let (code, first) = call(
        reqwest::Method::POST,
        format!("{base}/v1/runs"),
        Some(json!({ "config": config("before") })),
    )
    .await;
    assert_eq!(code, 202, "{first}");
    wait_terminal(&base, first["run_id"].as_str().unwrap()).await;

    // Postgres goes away.
    pg.stop().await.expect("stop postgres");
    let (code, during) = call(
        reqwest::Method::POST,
        format!("{base}/v1/runs"),
        Some(json!({ "config": config("during") })),
    )
    .await;
    assert_eq!(code, 202, "a single server keeps accepting runs: {during}");
    let during_id = during["run_id"].as_str().unwrap().to_string();
    let rec = wait_terminal(&base, &during_id).await;
    assert_eq!(rec["status"], "completed", "{rec}");
    let (code, ready) = call(reqwest::Method::GET, format!("{base}/readyz"), None).await;
    assert_eq!(code, 503, "{ready}");
    assert!(ready["degraded"]["since"].is_string(), "{ready}");
    assert!(
        ready["degraded"]["journaled"].as_u64().unwrap_or(0) >= 1,
        "{ready}"
    );
    assert!(data.join("history-journal").join("journal.jsonl").exists());
    // Shared configuration is not written to memory.
    let (code, refused) = call(
        reqwest::Method::POST,
        format!("{base}/v1/templates"),
        Some(json!({ "config": config("tpl") })),
    )
    .await;
    assert_eq!(code, 503, "{refused}");

    // Postgres is back: the journal is replayed and emptied.
    pg.start().await.expect("restart postgres");
    let mut recovered = false;
    for _ in 0..240 {
        let (code, ready) = call(reqwest::Method::GET, format!("{base}/readyz"), None).await;
        if code == 200 && ready["degraded"].is_null() {
            recovered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(recovered, "the server never left degraded mode");
    let h = PostgresHistory::connect(
        &url,
        Duration::from_secs(3600),
        Duration::from_secs(30),
        "reader".into(),
    )
    .await
    .unwrap();
    let stored = h
        .get(&during_id)
        .await
        .unwrap()
        .expect("replayed into postgres");
    assert_eq!(stored.status.as_str(), "completed");
    let audit = h
        .list_audit(&faucet_cli::serve::history::AuditFilter::default())
        .await
        .unwrap();
    assert!(
        audit
            .iter()
            .any(|a| a.run_id.as_deref() == Some(during_id.as_str())),
        "the run's audit entry was replayed"
    );
    assert_eq!(
        std::fs::metadata(data.join("history-journal").join("journal.jsonl"))
            .unwrap()
            .len(),
        0
    );
}
