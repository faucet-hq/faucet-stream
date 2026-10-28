//! `POST /v1/backfill` integration tests (#282): unit planning + one tracked
//! run per unit with backfill labels, deterministic idempotency (re-POST is
//! replay-safe), the scoping gate, and range validation.
#![cfg(all(feature = "serve", feature = "source-sqlite", feature = "sink-jsonl"))]
#![allow(clippy::disallowed_methods)] // tests drive time and raw clients directly

use faucet_cli::serve::{ServeConfig, run_server};
use serde_json::{Value, json};
use std::time::Duration;

fn test_config_with(listen: &str, concurrent: usize, queued: usize) -> ServeConfig {
    let args = faucet_cli::cli::ServeArgs {
        listen: listen.into(),
        auth_token: None,
        auth_config: None,
        read_token: None,
        write_token: None,
        admin_token: None,
        no_auth: true,
        allow_unauthenticated_network: false,
        max_concurrent_runs: Some(concurrent),
        max_queued_runs: Some(queued),
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
        vault_key: None,
        vault_previous_key: Vec::new(),
        connect_providers: None,
        allow_subprocess_connectors: false,
        references: Default::default(),
        pools: Default::default(),
    };
    ServeConfig::from_args(args).unwrap()
}

async fn boot() -> (String, reqwest::Client, tokio::task::JoinHandle<()>) {
    boot_with(2, 32).await
}

async fn boot_with(
    concurrent: usize,
    queued: usize,
) -> (String, reqwest::Client, tokio::task::JoinHandle<()>) {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);
    let listen = format!("127.0.0.1:{port}");
    let cfg = test_config_with(&listen, concurrent, queued);
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
        rec["name"].as_str().unwrap(),
        format!(
            "orders-backfill-{}-20260601T000000Z",
            out["backfill"].as_str().unwrap()
        ),
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
    let unit_key = format!(
        "orders-backfill-{}-20260601T000000Z::row-0",
        resumed["backfill"].as_str().unwrap()
    );
    let unit_key = unit_key.as_str();
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

    // #862 M-007: an overlapping, different range keeps its own unit state.
    let mut overlap = body(json!({}));
    overlap["from"] = json!("2026-06-02");
    overlap["to"] = json!("2026-06-04");
    let (status, other) = post(overlap).await;
    assert_eq!(status, 202, "{other}");
    assert_ne!(other["backfill"], first["backfill"]);
    let shared_day = other["units"][0]["run_id"].as_str().unwrap();
    let rec: Value = client
        .get(format!("{url}/v1/runs/{shared_day}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        rec["name"].as_str().unwrap(),
        format!(
            "orders-backfill-{}-20260602T000000Z",
            other["backfill"].as_str().unwrap()
        )
    );
    wait_completed(&client, &url, 6).await;

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

    // Several windows into one fixed file destination → 400 (#862 M-096).
    let resp = client
        .post(format!("{url}/v1/backfill"))
        .json(
            &json!({ "config": scoped, "from": "2026-06-01", "to": "2026-06-03", "window": "1d" }),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body = resp.text().await.unwrap();
    assert!(body.contains("out.jsonl"), "{body}");

    // Several windows into one overwrite table → 400: each unit would swap
    // away the windows before it.
    let overwrite = r#"
version: 1
name: plain
pipeline:
  source: { type: sqlite, config: { database_url: "sqlite::memory:", query: "SELECT '${backfill.start}' AS s" } }
  sink:   { type: sqlite, config: { database_url: "sqlite::memory:", table_name: t, column_mapping: auto_map, write_mode: overwrite } }
"#;
    let resp = client
        .post(format!("{url}/v1/backfill"))
        .json(
            &json!({ "config": overwrite, "from": "2026-06-01", "to": "2026-06-03", "window": "1d" }),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body = resp.text().await.unwrap();
    assert!(body.contains("write_mode: overwrite"), "{body}");

    // Several windows into one fixed object in replace mode → 400 too.
    let object = r#"
version: 1
name: plain
pipeline:
  source: { type: sqlite, config: { database_url: "sqlite::memory:", query: "SELECT '${backfill.start}' AS s" } }
  sink:   { type: s3, config: { bucket: b, path: exports/orders.jsonl } }
"#;
    let resp = client
        .post(format!("{url}/v1/backfill"))
        .json(
            &json!({ "config": object, "from": "2026-06-01", "to": "2026-06-03", "window": "1d" }),
        )
        .send()
        .await
        .unwrap();
    if cfg!(feature = "sink-s3") {
        assert_eq!(resp.status(), 400);
        let body = resp.text().await.unwrap();
        assert!(body.contains("s3://b/exports/orders.jsonl"), "{body}");
    }

    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_backfill_that_submits_nothing_leaves_no_range_marker() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.db").display().to_string();
    seed_sqlite(&src).await;
    let out_dir = dir.path().join("out").display().to_string();
    let state_dir = dir.path().join("state");
    let good = format!(
        "{}  state:\n    type: file\n    config: {{ path: \"{}\" }}\n",
        scoped_config(&src, &out_dir),
        state_dir.display()
    );
    let bad = good.replace("part-${backfill.unit}", "part-${backfill.nope}");
    let (url, client, server) = boot().await;
    let post = |config: String| {
        let client = client.clone();
        let url = url.clone();
        async move {
            let r = client
                .post(format!("{url}/v1/backfill"))
                .json(&json!({
                    "config": config,
                    "from": "2026-06-01",
                    "to": "2026-06-03",
                    "window": "1d",
                }))
                .send()
                .await
                .unwrap();
            let status = r.status().as_u16();
            (status, r.json::<Value>().await.unwrap())
        }
    };
    let (status, failed) = post(bad).await;
    assert_eq!(status, 400, "{failed}");
    let (status, retried) = post(good).await;
    assert_eq!(
        status, 202,
        "a failed first attempt must not block a retry: {retried}"
    );
    wait_completed(&client, &url, 2).await;
    server.abort();
}

/// A `restart` that submits nothing (the queue is full) still moves the range
/// to a new generation, so the `resume` its response asks for runs every unit
/// again instead of replaying the runs from before the restart.
#[tokio::test(flavor = "multi_thread")]
async fn a_restart_that_submits_nothing_still_starts_a_new_generation() {
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
    let (url, client, server) = boot_with(1, 1).await;
    let backfill = |extra: Value| {
        let client = client.clone();
        let url = url.clone();
        let mut b = json!({
            "config": config,
            "from": "2026-06-01",
            "to": "2026-06-02",
            "window": "1d",
        });
        b.as_object_mut()
            .unwrap()
            .extend(extra.as_object().cloned().unwrap_or_default());
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

    let (status, first) = backfill(json!({})).await;
    assert_eq!(status, 202, "{first}");
    let first_run = first["units"][0]["run_id"].as_str().unwrap().to_string();
    wait_completed(&client, &url, 1).await;

    // Occupy the one execution slot and the one queue slot with slow runs.
    let slow = format!(
        "version: 1\nname: slow\npipeline:\n  source:\n    type: sqlite\n    config:\n      database_url: \"sqlite://{src}\"\n      query: \"WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 15000000) SELECT count(*) AS n FROM c\"\n  sink:\n    type: jsonl\n    config:\n      path: \"{out_dir}/slow.jsonl\"\n"
    );
    let mut accepted = 0;
    for _ in 0..200 {
        let r = client
            .post(format!("{url}/v1/runs"))
            .json(&json!({"config": slow}))
            .send()
            .await
            .unwrap();
        if r.status().as_u16() == 202 {
            accepted += 1;
            if accepted == 2 {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(accepted, 2, "one slow run running and one queued");

    let (status, restarted) = backfill(json!({"restart": true})).await;
    assert_eq!(status, 202, "{restarted}");
    assert_eq!(restarted["submitted"], 0, "{restarted}");
    assert_eq!(
        restarted["units"][0]["status"], "not_submitted",
        "{restarted}"
    );

    wait_completed(&client, &url, 3).await;
    let (status, resumed) = backfill(json!({"resume": true})).await;
    assert_eq!(status, 202, "{resumed}");
    assert_eq!(resumed["units"][0]["status"], "submitted", "{resumed}");
    assert_ne!(
        resumed["units"][0]["run_id"],
        first_run.as_str(),
        "resume after the restart runs the unit again: {resumed}"
    );
    server.abort();
}

/// #958: the poll URL the backfill API advertises lists exactly that
/// backfill's unit runs, not every run on the server.
#[tokio::test(flavor = "multi_thread")]
async fn the_advertised_label_poll_lists_only_that_backfills_units() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.db").display().to_string();
    seed_sqlite(&src).await;
    let out_dir = dir.path().join("out").display().to_string();
    let (url, client, server) = boot().await;
    let post = |from: &str, to: &str| {
        client
            .post(format!("{url}/v1/backfill"))
            .json(&json!({
                "config": scoped_config(&src, &out_dir),
                "from": from,
                "to": to,
                "window": "1d",
                "labels": { "team": "data" },
            }))
            .send()
    };
    let first: Value = post("2026-06-01", "2026-06-03")
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let second: Value = post("2026-06-03", "2026-06-04")
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_ne!(first["backfill"], second["backfill"]);
    let plain = client
        .post(format!("{url}/v1/runs"))
        .json(&json!({ "config": scoped_config(&src, &out_dir).replace("${backfill.start_date}", "2026-06-01").replace("${backfill.end_date}", "2026-06-02").replace("${backfill.unit}", "plain"), "labels": { "team": "data" } }))
        .send()
        .await
        .unwrap();
    assert_eq!(plain.status(), 202, "{}", plain.text().await.unwrap());
    wait_completed(&client, &url, 4).await;

    let list = |q: String| {
        let client = client.clone();
        let url = url.clone();
        async move {
            let resp = client
                .get(format!("{url}/v1/runs?{q}"))
                .send()
                .await
                .unwrap();
            let status = resp.status();
            (status, resp.json::<Value>().await.unwrap())
        }
    };
    let unit_ids = |v: &Value| {
        let mut ids: Vec<String> = v["runs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["run_id"].as_str().unwrap().to_string())
            .collect();
        ids.sort();
        ids
    };
    let mut want: Vec<String> = first["units"]
        .as_array()
        .unwrap()
        .iter()
        .map(|u| u["run_id"].as_str().unwrap().to_string())
        .collect();
    want.sort();
    let hash = first["backfill"].as_str().unwrap();

    let (code, page) = list(format!("label=backfill:{hash}")).await;
    assert_eq!(code, 200);
    assert_eq!(unit_ids(&page), want, "{page}");
    let (_, page) = list(format!("labels=backfill:{hash},team:data")).await;
    assert_eq!(unit_ids(&page), want, "the comma alias");
    let (_, page) = list(format!(
        "label=backfill:{hash}&label=backfill_unit:20260602T000000Z"
    ))
    .await;
    assert_eq!(page["runs"].as_array().unwrap().len(), 1);
    let (_, page) = list("label=team:data&limit=500".into()).await;
    assert_eq!(
        page["runs"].as_array().unwrap().len(),
        4,
        "units + the plain run"
    );
    let (_, page) = list("label=backfill&limit=500".into()).await;
    assert_eq!(page["runs"].as_array().unwrap().len(), 3, "key present");

    // A typo is refused instead of returning an unfiltered list.
    let (code, body) = list(format!("lables=backfill:{hash}")).await;
    assert_eq!(code, 400, "{body}");
    assert!(body.to_string().contains("lables"), "{body}");
    let too_many: Vec<String> = (0..11).map(|i| format!("label=k{i}")).collect();
    let (code, body) = list(too_many.join("&")).await;
    assert_eq!(code, 400);
    assert!(body.to_string().contains("at most 10"), "{body}");
    let (code, _) = list("label=:v".into()).await;
    assert_eq!(code, 400);

    // The callback refusal names the working poll.
    let resp = client
        .post(format!("{url}/v1/backfill"))
        .json(&json!({
            "config": scoped_config(&src, &out_dir),
            "from": "2026-06-01",
            "to": "2026-06-02",
            "window": "1d",
            "callback": { "url": "https://example.com/hook" },
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 422);
    assert!(
        resp.text()
            .await
            .unwrap()
            .contains("GET /v1/runs?label=backfill:<hash>")
    );

    server.abort();
}

/// #958: label keys/values are capped and provenance keys refused, on runs
/// and backfills alike.
#[tokio::test(flavor = "multi_thread")]
async fn submitted_labels_are_checked() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.db").display().to_string();
    seed_sqlite(&src).await;
    let out_dir = dir.path().join("out").display().to_string();
    let (url, client, server) = boot().await;
    let run = |labels: Value| {
        client
            .post(format!("{url}/v1/runs"))
            .json(&json!({ "config": scoped_config(&src, &out_dir), "labels": labels }))
            .send()
    };
    for (labels, needle) in [
        (json!({ ("k".repeat(64)): "v" }), "longer than 63"),
        (json!({ "k": ("v".repeat(257)) }), "longer than 256"),
        (json!({ "template": "fake" }), "set by the server"),
    ] {
        let resp = run(labels).await.unwrap();
        assert_eq!(resp.status(), 422);
        assert!(resp.text().await.unwrap().contains(needle), "{needle}");
    }
    let resp = client
        .post(format!("{url}/v1/backfill"))
        .json(&json!({
            "config": scoped_config(&src, &out_dir),
            "from": "2026-06-01",
            "to": "2026-06-02",
            "window": "1d",
            "labels": { "selection": "x" },
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 422);
    assert!(resp.text().await.unwrap().contains("set by the server"));
    server.abort();
}
