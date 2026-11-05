//! `faucet serve` keeps its run history in a SQLite file by default (#1049):
//! `$FAUCET_DATA_DIR/history.db`. Runs, templates, schedules and policy
//! versions survive a restart; `--history memory` is an explicit opt-in that
//! warns; a data directory that cannot be used refuses to start rather than
//! falling back to memory; a second server on the same file refuses too.
#![cfg(all(
    feature = "serve",
    feature = "policy",
    feature = "templates",
    feature = "schedule",
    feature = "source-csv",
    feature = "sink-jsonl"
))]
#![allow(clippy::disallowed_methods)] // tests drive time and raw clients directly

use serde_json::{Value, json};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

#[path = "support/ports.rs"]
mod ports;
use ports::free_port;

const AUTH: &str = "principals:\n\
    \x20 - { name: alice, token: alice-tok, role: admin }\n\
    \x20 - { name: ann, token: ann-tok, role: admin }\n";

const POLICY: &str = "version: 1
classifications:
  - label: pii.email
    fields: [email]
rules:
  - name: pii-to-eu-only
    when: { label: pii.email }
    require: { residency: [eu] }
";

/// `faucet serve` on `port` with `dir`'s auth config and `extra` arguments,
/// its data directory set to `data` (unset: `None`).
fn serve(port: u16, dir: &Path, data: Option<&Path>, extra: &[&str]) -> Command {
    let auth = dir.join("auth.yaml");
    std::fs::write(&auth, AUTH).unwrap();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_faucet"));
    cmd.args([
        "serve",
        "--listen",
        &format!("127.0.0.1:{port}"),
        "--no-env-file",
    ])
    .arg("--auth-config")
    .arg(&auth)
    .args(extra)
    .env_remove("FAUCET_SERVE_HISTORY")
    .env_remove("FAUCET_DATA_DIR")
    .env("FAUCET_LOG", "warn")
    .stdout(Stdio::piped())
    .stderr(Stdio::piped());
    if let Some(data) = data {
        cmd.env("FAUCET_DATA_DIR", data);
    }
    cmd
}

/// Start a server and wait until it answers `/healthz`.
async fn start(mut cmd: Command, port: u16) -> Child {
    let mut child = cmd.spawn().unwrap();
    let client = reqwest::Client::new();
    for _ in 0..1200 {
        if let Ok(Some(status)) = child.try_wait() {
            panic!("server exited early: {status}: {}", stderr(child));
        }
        if client
            .get(format!("http://127.0.0.1:{port}/healthz"))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
        {
            return child;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("server did not become healthy on port {port}");
}

/// Stop a server and return what it logged.
fn stop(mut child: Child) -> String {
    let _ = child.kill();
    let _ = child.wait();
    stderr(child)
}

/// Everything the server wrote (logs may go to either stream).
fn stderr(mut child: Child) -> String {
    use std::io::Read;
    let mut out = String::new();
    if let Some(mut o) = child.stdout.take() {
        let _ = o.read_to_string(&mut out);
    }
    if let Some(mut e) = child.stderr.take() {
        let _ = e.read_to_string(&mut out);
    }
    out
}

/// Run a server that must refuse to start, and return what it wrote.
fn refused(mut cmd: Command) -> String {
    let out = cmd.output().unwrap();
    assert!(!out.status.success(), "the server started");
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
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

    async fn get(&self, path: &str) -> (u16, Value) {
        let r = self
            .client
            .get(format!("{}{path}", self.base))
            .bearer_auth("alice-tok")
            .send()
            .await
            .unwrap();
        (r.status().as_u16(), r.json().await.unwrap_or(Value::Null))
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
        (r.status().as_u16(), r.json().await.unwrap_or(Value::Null))
    }
}

fn pipeline(dir: &Path, name: &str) -> String {
    let input = dir.join("in.csv");
    std::fs::write(&input, "id,email\n1,a@example.com\n").unwrap();
    format!(
        "version: 1\nname: {name}\npipeline:\n  source: {{ type: csv, config: {{ path: {} }} }}\n  \
         sink: {{ type: jsonl, attributes: {{ residency: eu }}, config: {{ path: {} }} }}\n",
        input.display(),
        dir.join(format!("{name}.jsonl")).display()
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn the_default_history_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let port = free_port();
    let server = start(serve(port, dir.path(), Some(&data), &[]), port).await;
    let api = Api::new(port);

    // A file in the data directory, private to its owner.
    let (_, ready) = api.get("/readyz").await;
    assert_eq!(
        ready["history"],
        json!({ "kind": "sqlite", "persistent": true })
    );
    assert!(data.join("history.db").exists());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&data).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o700);
    }

    // A second server on the same data directory refuses to start.
    let err = refused(serve(free_port(), dir.path(), Some(&data), &[]));
    assert!(
        err.contains("another faucet server is using the run history"),
        "{err}"
    );

    // A run, a template, a schedule and a policy version.
    let (s, run) = api
        .post(
            "alice-tok",
            "/v1/runs",
            json!({ "config": pipeline(dir.path(), "kept") }),
        )
        .await;
    assert_eq!(s, 202, "{run}");
    let run_id = run["run_id"].as_str().unwrap().to_string();
    let (s, t) = api
        .post(
            "alice-tok",
            "/v1/templates",
            json!({ "id": "kept-template", "config": pipeline(dir.path(), "tpl") }),
        )
        .await;
    assert_eq!(s, 201, "{t}");
    let (s, sched) = api
        .post(
            "alice-tok",
            "/v1/schedules",
            json!({ "name": "nightly", "cron": "0 3 * * *", "template": { "id": "kept-template", "version": "1" } }),
        )
        .await;
    assert_eq!(s, 201, "{sched}");
    let (s, change) = api
        .post(
            "alice-tok",
            "/v1/policy",
            json!({ "policy": POLICY, "reason": "first", "base_version": null }),
        )
        .await;
    assert_eq!(s, 201, "{change}");
    let id = change["id"].as_str().unwrap();
    let (_, approved) = api
        .post("ann-tok", &format!("/v1/changes/{id}/approve"), json!({}))
        .await;
    assert_eq!(approved["status"], "executed", "{approved}");
    for _ in 0..400 {
        let (_, r) = api.get(&format!("/v1/runs/{run_id}")).await;
        if r["status"] == "completed" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    stop(server);

    // Restarted on the same data directory: everything is still there.
    let port = free_port();
    let server = start(serve(port, dir.path(), Some(&data), &[]), port).await;
    let api = Api::new(port);
    let (s, r) = api.get(&format!("/v1/runs/{run_id}")).await;
    assert_eq!(s, 200, "{r}");
    assert_eq!(r["status"], "completed", "{r}");
    assert_eq!(api.get("/v1/templates/kept-template").await.0, 200);
    let (s, sched) = api.get("/v1/schedules/nightly").await;
    assert_eq!(s, 200, "{sched}");
    let (_, p) = api.get("/v1/policy").await;
    assert_eq!(p["version"]["version"], 1, "{p}");
    assert_eq!(p["editable"], true);
    stop(server);
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_history_is_an_explicit_opt_in_that_warns() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let port = free_port();
    let server = start(
        serve(port, dir.path(), Some(&data), &["--history", "memory"]),
        port,
    )
    .await;
    let api = Api::new(port);
    let (_, ready) = api.get("/readyz").await;
    assert_eq!(
        ready["history"],
        json!({ "kind": "memory", "persistent": false })
    );
    let (s, refused) = api
        .post(
            "alice-tok",
            "/v1/policy",
            json!({ "policy": POLICY, "reason": "x", "base_version": null }),
        )
        .await;
    assert_eq!(s, 409, "{refused}");
    let log = stop(server);
    assert!(log.contains("--history memory"), "{log}");
    assert!(log.contains("policy versions"), "{log}");
    assert!(!data.exists(), "nothing is written to the data directory");
}

#[test]
fn an_unusable_data_directory_refuses_to_start() {
    let dir = tempfile::tempdir().unwrap();
    // A data directory below a file cannot be created.
    let file = dir.path().join("a-file");
    std::fs::write(&file, "").unwrap();
    let err = refused(serve(
        free_port(),
        dir.path(),
        Some(&file.join("data")),
        &[],
    ));
    assert!(err.contains("--history memory"), "{err}");
    assert!(err.contains("$FAUCET_DATA_DIR"), "{err}");

    // A history file that is not a database is never swapped for memory.
    let data = dir.path().join("data");
    std::fs::create_dir_all(data.join("history.db")).unwrap();
    let err = refused(serve(free_port(), dir.path(), Some(&data), &[]));
    assert!(
        err.contains("could not open the default run history"),
        "{err}"
    );
}
