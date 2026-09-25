//! Integration tests for `POST /v1/verify` (#701) and
//! `POST /v1/runs/{id}/rollback` (#706) over a real RBAC server: the handlers
//! run end to end against SQLite, verification reports differences, a run
//! submitted through the control plane is undone by its recorded invocation
//! id, and RBAC is enforced (viewer cannot verify; operator cannot roll back).
#![cfg(feature = "serve")]

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
    \x20 - name: bob\n\
    \x20   token: viewer-tok\n\
    \x20   role: viewer\n\
    \x20 - name: carol\n\
    \x20   token: op-tok\n\
    \x20   role: operator\n";

fn serve_args(port: u16, auth_config: std::path::PathBuf) -> faucet_cli::cli::ServeArgs {
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
        callback_allow_host: Vec::new(),
        mcp: false,
        mcp_allow_mutations: false,
    }
}

async fn spawn_server(port: u16, dir: &std::path::Path) {
    let auth_path = dir.join("auth.yaml");
    std::fs::write(&auth_path, AUTH_CONFIG).unwrap();
    let mut config =
        faucet_cli::serve::ServeConfig::from_args(serve_args(port, auth_path)).unwrap();
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

async fn exec(db: &str, sql: &str) {
    let pool = sqlx::SqlitePool::connect(db).await.unwrap();
    sqlx::query(sql).execute(&pool).await.unwrap();
    pool.close().await;
}

async fn count(db: &str, sql: &str) -> i64 {
    let pool = sqlx::SqlitePool::connect(db).await.unwrap();
    let n: i64 = sqlx::query_scalar(sql).fetch_one(&pool).await.unwrap();
    pool.close().await;
    n
}

fn config_yaml(src: &str, dst: &str, state: &std::path::Path, extra: &str) -> String {
    format!(
        r#"
version: 1
name: mirror
pipeline:
  source:
    type: sqlite
    config:
      database_url: "{src}"
      query: "SELECT id, name FROM src ORDER BY id"
  sink:
    type: sqlite
    config:
      database_url: "{dst}"
      table_name: dst
      column_mapping: auto_map
      write_mode: upsert
      key: [id]
  state:
    type: file
    config:
      path: "{}"
{extra}
"#,
        state.display()
    )
}

async fn wait_terminal(client: &reqwest::Client, base: &str, id: &str) -> Value {
    for _ in 0..400 {
        let rec: Value = client
            .get(format!("{base}/v1/runs/{id}"))
            .bearer_auth("admin-tok")
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if ["completed", "failed", "cancelled"].contains(&rec["status"].as_str().unwrap_or("")) {
            return rec;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("run {id} did not finish");
}

#[tokio::test(flavor = "multi_thread")]
async fn verify_and_rollback_endpoints_with_rbac() {
    let dir = tempfile::tempdir().unwrap();
    let src = format!("sqlite://{}?mode=rwc", dir.path().join("src.db").display());
    let dst = format!("sqlite://{}?mode=rwc", dir.path().join("dst.db").display());
    exec(&src, "CREATE TABLE src (id INTEGER PRIMARY KEY, name TEXT)").await;
    exec(
        &src,
        "INSERT INTO src VALUES (1, 'one'), (2, 'two'), (3, 'three')",
    )
    .await;
    exec(
        &dst,
        "CREATE TABLE dst (id INTEGER PRIMARY KEY, name TEXT, _faucet_run_id TEXT)",
    )
    .await;
    exec(&dst, "INSERT INTO dst VALUES (1, 'old-one', 'r0')").await;
    let state = dir.path().join("state");
    let cfg = config_yaml(&src, &dst, &state, "rollback: {}");

    let port = free_port();
    spawn_server(port, dir.path()).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");

    // ── run the mirror through the control plane ──
    let submitted: Value = client
        .post(format!("{base}/v1/runs"))
        .bearer_auth("op-tok")
        .json(&json!({ "config": cfg }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let run_id = submitted["run_id"].as_str().unwrap().to_string();
    let rec = wait_terminal(&client, &base, &run_id).await;
    assert_eq!(rec["status"], "completed", "{rec}");
    let inv_id = rec["invocations"][0]["run_id"]
        .as_str()
        .expect("invocation run id recorded")
        .to_string();
    assert_eq!(count(&dst, "SELECT count(*) FROM dst").await, 3);

    // ── verify: clean, then drifted ──
    let unauth = client
        .post(format!("{base}/v1/verify"))
        .json(&json!({ "config": cfg }))
        .send()
        .await
        .unwrap();
    assert_eq!(unauth.status(), 401);
    let viewer = client
        .post(format!("{base}/v1/verify"))
        .bearer_auth("viewer-tok")
        .json(&json!({ "config": cfg }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        viewer.status(),
        403,
        "a viewer cannot verify (a repair writes)"
    );
    let clean = client
        .post(format!("{base}/v1/verify"))
        .bearer_auth("op-tok")
        .json(&json!({ "config": cfg }))
        .send()
        .await
        .unwrap();
    assert_eq!(clean.status(), 200);
    let clean: Value = clean.json().await.unwrap();
    assert_eq!(clean["differences"].as_array().unwrap().len(), 0, "{clean}");
    assert_eq!(clean["strategy"], "range");

    exec(&dst, "UPDATE dst SET name = 'TWO' WHERE id = 2").await;
    let drifted: Value = client
        .post(format!("{base}/v1/verify"))
        .bearer_auth("op-tok")
        .json(&json!({ "config": cfg, "repair": true }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        drifted["differences"].as_array().unwrap().len(),
        1,
        "{drifted}"
    );
    assert_eq!(drifted["differences"][0]["kind"], "changed");
    assert_eq!(drifted["repaired_upserts"], 1);
    assert_eq!(
        count(&dst, "SELECT count(*) FROM dst WHERE name = 'two'").await,
        1,
        "repaired"
    );

    // ── rollback: RBAC, unknown run, dry run, real ──
    let op = client
        .post(format!("{base}/v1/runs/{run_id}/rollback"))
        .bearer_auth("op-tok")
        .json(&json!({ "config": cfg }))
        .send()
        .await
        .unwrap();
    assert_eq!(op.status(), 403, "rollback is admin-only");
    let missing = client
        .post(format!("{base}/v1/runs/nope/rollback"))
        .bearer_auth("admin-tok")
        .json(&json!({ "config": cfg }))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 404);
    // No stored config on a non-cluster server, none supplied → 422.
    let no_cfg = client
        .post(format!("{base}/v1/runs/{run_id}/rollback"))
        .bearer_auth("admin-tok")
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(no_cfg.status(), 422, "{}", no_cfg.text().await.unwrap());
    let bad_inv = client
        .post(format!("{base}/v1/runs/{run_id}/rollback"))
        .bearer_auth("admin-tok")
        .json(&json!({ "config": cfg, "invocation_id": "not-there" }))
        .send()
        .await
        .unwrap();
    assert_eq!(bad_inv.status(), 400);

    let dry = client
        .post(format!("{base}/v1/runs/{run_id}/rollback"))
        .bearer_auth("admin-tok")
        .json(&json!({ "config": cfg, "dry_run": true }))
        .send()
        .await
        .unwrap();
    assert_eq!(dry.status(), 200);
    let dry: Value = dry.json().await.unwrap();
    assert_eq!(dry["run_id"], inv_id);
    assert_eq!(dry["applied"], false);
    assert_eq!(dry["dry_run"], true);
    // The repair re-upserted key 2 under another run id → a conflict blocks.
    let blocked: Value = client
        .post(format!("{base}/v1/runs/{run_id}/rollback"))
        .bearer_auth("admin-tok")
        .json(&json!({ "config": cfg, "invocation_id": inv_id }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(blocked["applied"], false, "{blocked}");
    assert_eq!(blocked["conflicts"], 1);
    assert_eq!(
        count(&dst, "SELECT count(*) FROM dst").await,
        3,
        "blocked: untouched"
    );
    let forced: Value = client
        .post(format!("{base}/v1/runs/{run_id}/rollback"))
        .bearer_auth("admin-tok")
        .json(&json!({ "config": cfg, "force": true }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(forced["applied"], true, "{forced}");
    assert_eq!(forced["mode"], "upsert");
    assert_eq!(forced["restored"], 1);
    assert_eq!(forced["deleted"], 2);
    assert_eq!(forced["bookmark_rewound"], true);
    assert_eq!(count(&dst, "SELECT count(*) FROM dst").await, 1);
    assert_eq!(
        count(&dst, "SELECT count(*) FROM dst WHERE name = 'old-one'").await,
        1
    );

    // The audit log saw both actions.
    let audit: Value = client
        .get(format!("{base}/v1/audit"))
        .bearer_auth("admin-tok")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let actions: Vec<&str> = audit["entries"]
        .as_array()
        .or_else(|| audit.as_array())
        .map(|a| a.iter().filter_map(|e| e["action"].as_str()).collect())
        .unwrap_or_default();
    assert!(actions.contains(&"verify"), "{audit}");
    assert!(actions.contains(&"run.rollback"), "{audit}");
}
