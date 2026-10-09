//! `POST /v1/backfill` integration tests (#282): unit planning + one tracked
//! run per unit with backfill labels, deterministic idempotency (re-POST is
//! replay-safe), the scoping gate, and range validation.
#![cfg(all(feature = "serve", feature = "source-sqlite", feature = "sink-jsonl"))]

use faucet_cli::serve::{ServeConfig, run_server};
use serde_json::{Value, json};
use std::time::Duration;

fn test_config(listen: &str) -> ServeConfig {
    let args = faucet_cli::cli::ServeArgs {
        listen: listen.into(),
        auth_token: None,
        auth_config: None,
        read_token: None,
        write_token: None,
        admin_token: None,
        no_auth: true,
        max_concurrent_runs: Some(2),
        max_queued_runs: Some(32),
        default_config: None,
        history: None,
        cors_origin: vec![],
        body_limit_bytes: 1_048_576,
        shutdown_grace_secs: 5,
        retain_terminal_runs_secs: 60,
        idempotency_retention_secs: 60,
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
    };
    ServeConfig::from_args(args).unwrap()
}

async fn boot() -> (String, reqwest::Client, tokio::task::JoinHandle<()>) {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);
    let listen = format!("127.0.0.1:{port}");
    let cfg = test_config(&listen);
    let server = tokio::spawn(async move {
        let _ = run_server(cfg, Default::default()).await;
    });
    let url = format!("http://{listen}");
    let client = reqwest::Client::new();
    for _ in 0..50 {
        if client.get(format!("{url}/healthz")).send().await.is_ok() {
            return (url, client, server);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("server never became reachable on {listen}");
}

/// A window-scoped config: the SQLite source query references `${backfill.*}`
/// tokens; the sink writes to a per-window JSONL path. The source DB is
/// seeded by the test so unit runs actually execute end-to-end.
fn scoped_config(src_db: &str, out_dir: &str) -> String {
    format!(
        r#"
version: 1
name: orders
pipeline:
  source:
    type: sqlite
    config:
      database_url: "sqlite://{src_db}"
      query: >-
        SELECT id, day FROM events
        WHERE day >= '${{backfill.start_date}}' AND day < '${{backfill.end_date}}'
  sink:
    type: jsonl
    config:
      path: "{out_dir}/part-${{backfill.unit}}.jsonl"
"#
    )
}

async fn seed_sqlite(path: &str) {
    let pool = sqlx::SqlitePool::connect(&format!("sqlite://{path}?mode=rwc"))
        .await
        .expect("create db");
    sqlx::query("CREATE TABLE events (id INTEGER PRIMARY KEY, day TEXT NOT NULL)")
        .execute(&pool)
        .await
        .expect("ddl");
    for (id, day) in [(1, "2026-06-01"), (2, "2026-06-01"), (3, "2026-06-02")] {
        sqlx::query("INSERT INTO events (id, day) VALUES (?, ?)")
            .bind(id)
            .bind(day)
            .execute(&pool)
            .await
            .expect("seed");
    }
    pool.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn backfill_submits_one_run_per_unit_and_replays_on_repost() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.db").display().to_string();
    seed_sqlite(&src).await;
    let out_dir = dir.path().join("out").display().to_string();
    let (url, client, server) = boot().await;

    let body = json!({
        "config": scoped_config(&src, &out_dir),
        "from": "2026-06-01",
        "to": "2026-06-03",
        "window": "1d",
    });
    let resp = client
        .post(format!("{url}/v1/backfill"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);
    let out: Value = resp.json().await.unwrap();
    assert_eq!(out["planned"], 2, "{out}");
    assert_eq!(out["submitted"], 2);
    let units = out["units"].as_array().unwrap();
    assert_eq!(units.len(), 2);
    assert!(units.iter().all(|u| u["status"] == "submitted"));
    let first_run_id = units[0]["run_id"].as_str().unwrap().to_string();

    // Each unit is a normal tracked run, named + labelled for the backfill.
    let rec: Value = client
        .get(format!("{url}/v1/runs/{first_run_id}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        rec["name"], "orders-backfill-20260601T000000Z",
        "namespaced unit run name: {rec}"
    );
    assert_eq!(rec["labels"]["backfill"], out["backfill"]);
    assert_eq!(rec["labels"]["backfill_unit"], "20260601T000000Z");

    // Wait for both unit runs to succeed end-to-end.
    let mut succeeded = 0;
    for _ in 0..100 {
        let runs: Value = client
            .get(format!("{url}/v1/runs?limit=10"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let items = runs["runs"].as_array().cloned().unwrap_or_default();
        succeeded = items.iter().filter(|r| r["status"] == "completed").count();
        if succeeded == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(succeeded, 2, "both unit runs complete");

    // Per-window outputs: day 1 has two records, day 2 has one.
    let day1 = std::fs::read_to_string(format!("{out_dir}/part-20260601T000000Z.jsonl")).unwrap();
    assert_eq!(day1.lines().count(), 2, "{day1}");
    let day2 = std::fs::read_to_string(format!("{out_dir}/part-20260602T000000Z.jsonl")).unwrap();
    assert_eq!(day2.lines().count(), 1, "{day2}");

    // Re-POSTing the same backfill replays via the deterministic idempotency
    // keys — same run ids, no new runs.
    let resp = client
        .post(format!("{url}/v1/backfill"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);
    let replay: Value = resp.json().await.unwrap();
    assert_eq!(replay["units"][0]["run_id"], first_run_id.as_str());

    server.abort();
}

async fn wait_completed(client: &reqwest::Client, url: &str, want: usize) {
    for _ in 0..200 {
        let runs: Value = client
            .get(format!("{url}/v1/runs?limit=50"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let items = runs["runs"].as_array().cloned().unwrap_or_default();
        if items.iter().filter(|r| r["status"] == "completed").count() >= want {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("{want} unit runs never completed");
}

/// SERVE-13: with a durable `state:` block a re-POST of a range that already
/// ran is refused instead of silently resuming each unit's old bookmark;
/// `resume` continues it, `restart` clears each unit's state and runs it again.
#[tokio::test(flavor = "multi_thread")]
async fn a_stateful_backfill_refuses_a_rerun_unless_resumed_or_restarted() {
    use faucet_core::StateStore as _;
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.db").display().to_string();
    seed_sqlite(&src).await;
    let out_dir = dir.path().join("out").display().to_string();
    let state_dir = dir.path().join("state");
    let config = format!(
        "{}  state:\n    type: file\n    config: {{ path: \"{}\" }}\n",
        scoped_config(&src, &out_dir),
        state_dir.display()
    );
    let (url, client, server) = boot().await;
    let body = |extra: Value| {
        let mut b = json!({
            "config": config,
            "from": "2026-06-01",
            "to": "2026-06-03",
            "window": "1d",
        });
        if let Value::Object(m) = extra {
            b.as_object_mut().unwrap().extend(m);
        }
        b
    };
    let post = |b: Value| {
        let client = client.clone();
        let url = url.clone();
        async move {
            let r = client
                .post(format!("{url}/v1/backfill"))
                .json(&b)
                .send()
                .await
                .unwrap();
            let status = r.status().as_u16();
            (status, r.json::<Value>().await.unwrap())
        }
    };

    let (status, first) = post(body(json!({}))).await;
    assert_eq!(status, 202, "{first}");
    let first_run = first["units"][0]["run_id"].as_str().unwrap().to_string();
    wait_completed(&client, &url, 2).await;

    let (status, refused) = post(body(json!({}))).await;
    assert_eq!(status, 409, "{refused}");
    let msg = refused["error"]["message"].as_str().unwrap_or_default();
    assert!(msg.contains("resume") && msg.contains("restart"), "{msg}");

    let (status, both) = post(body(json!({"resume": true, "restart": true}))).await;
    assert_eq!(status, 422, "{both}");

    let (status, resumed) = post(body(json!({"resume": true}))).await;
    assert_eq!(status, 202, "{resumed}");
    assert_eq!(resumed["units"][0]["run_id"], first_run.as_str());

    let store = faucet_core::FileStateStore::new(&state_dir);
    let unit_key = "orders-backfill-20260601T000000Z::row-0";
    store
        .put(unit_key, &json!({"day": "2026-06-30"}))
        .await
        .unwrap();
    let (status, restarted) = post(body(json!({"restart": true}))).await;
    assert_eq!(status, 202, "{restarted}");
    assert_ne!(restarted["units"][0]["run_id"], first_run.as_str());
    assert!(
        store.get(unit_key).await.unwrap().is_none(),
        "restart clears each unit's scoped state"
    );
    wait_completed(&client, &url, 4).await;

    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn backfill_rejects_unscoped_source_and_bad_range() {
    let (url, client, server) = boot().await;

    // Unscoped source → 422 naming the offending root.
    let unscoped = r#"
version: 1
name: plain
pipeline:
  source: { type: sqlite, config: { database_url: "sqlite::memory:", query: "SELECT 1 AS x" } }
  sink:   { type: jsonl, config: { path: ./out.jsonl } }
"#;
    let resp = client
        .post(format!("{url}/v1/backfill"))
        .json(&json!({ "config": unscoped, "from": "2026-06-01", "to": "2026-06-02" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "{}", resp.text().await.unwrap());

    // Inverted range → 400.
    let scoped = r#"
version: 1
name: plain
pipeline:
  source: { type: sqlite, config: { database_url: "sqlite::memory:", query: "SELECT '${backfill.start}' AS s" } }
  sink:   { type: jsonl, config: { path: ./out.jsonl } }
"#;
    let resp = client
        .post(format!("{url}/v1/backfill"))
        .json(&json!({ "config": scoped, "from": "2026-06-02", "to": "2026-06-01" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "{}", resp.text().await.unwrap());

    server.abort();
}
