//! Execution pools (#957): two clustered servers in pools `a` and `b` on one
//! SQLite history run every run, shard and re-claim only on a replica of its
//! pool; a pool nobody serves is refused with 422; a non-clustered server
//! serves only its own pools.
#![cfg(all(
    feature = "serve",
    feature = "serve-history-sqlite",
    feature = "source-sqlite",
    feature = "sink-sqlite"
))]
#![allow(clippy::disallowed_methods)] // tests drive time and raw clients directly

use faucet_cli::serve::history::sqlite::SqliteHistory;
use faucet_cli::serve::history::{
    InstanceHeartbeat, RunHistory, RunRecord, RunStatus, ShardInsert,
};
use serde_json::{Value, json};
use sqlx::Connection as _;
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

#[path = "support/ports.rs"]
mod ports;
use ports::free_port;

fn args(port: u16, history: String, cluster: bool, pools: &[&str]) -> faucet_cli::cli::ServeArgs {
    faucet_cli::cli::ServeArgs {
        listen: format!("127.0.0.1:{port}"),
        auth_token: None,
        auth_config: None,
        read_token: None,
        write_token: None,
        admin_token: None,
        no_auth: true,
        allow_unauthenticated_network: false,
        max_concurrent_runs: Some(4),
        max_queued_runs: Some(32),
        default_config: None,
        history: Some(history),
        cors_origin: vec![],
        body_limit_bytes: 1_048_576,
        shutdown_grace_secs: 5,
        retain_terminal_runs_secs: 604_800,
        idempotency_retention_secs: 86_400,
        log_retention_secs: 0,
        log_max_lines_per_run: 100_000,
        log_buffer: Default::default(),
        local_output_retention_days: 7,
        local_output_in_flight_grace_secs: 60,
        preview_local_outputs: false,
        preview_default_rows: 500,
        preview_max_rows: 5_000,
        lease_ttl_secs: 3,
        probe_timeout_secs: 5,
        env_file: None,
        no_env_file: true,
        no_ui: true,
        cluster,
        cluster_poll_secs: 1,
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
        grafana_url: None,
        references: Default::default(),
        pools: faucet_cli::cli::PoolArgs {
            pool: pools.iter().map(|p| p.to_string()).collect(),
            pool_wait_timeout_secs: None,
        },
    }
}

async fn start(a: faucet_cli::cli::ServeArgs) -> String {
    let base = format!("http://{}", a.listen);
    let mut config = faucet_cli::serve::ServeConfig::from_args(a).unwrap();
    config.log_level = "warn".into();
    tokio::spawn(async move {
        let _ = faucet_cli::serve::run_server(config, Default::default()).await;
    });
    let client = reqwest::Client::new();
    for _ in 0..1200 {
        if client
            .get(format!("{base}/healthz"))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
        {
            return base;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("server at {base} never became healthy");
}

async fn seed_source(db: &Path) {
    let mut conn = sqlx::SqliteConnection::connect(&format!("sqlite:{}?mode=rwc", db.display()))
        .await
        .unwrap();
    sqlx::query("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .execute(&mut conn)
        .await
        .unwrap();
    for i in 1..=20 {
        sqlx::query("INSERT INTO t VALUES (?, 'x')")
            .bind(i)
            .execute(&mut conn)
            .await
            .unwrap();
    }
}

/// A config reading the seeded table into its own sink database.
fn config(src: &Path, out: &Path, pool: Option<&str>, shards: bool) -> String {
    let mut yaml = String::from("version: 1\n");
    if let Some(p) = pool {
        yaml.push_str(&format!("pool: {p}\n"));
    }
    if shards {
        yaml.push_str("shard: { count: 2 }\n");
    }
    yaml.push_str(&format!(
        "pipeline:\n  \
         source: {{ type: sqlite, config: {{ database_url: \"sqlite:{}\", query: \"SELECT id, v FROM t\"{} }} }}\n  \
         sink: {{ type: sqlite, config: {{ database_url: \"sqlite:{}?mode=rwc\", table_name: out, create_table: true, column_mapping: auto_map }} }}\n",
        src.display(),
        if shards { ", shard: { key: id }" } else { "" },
        out.display(),
    ));
    yaml
}

async fn submit(base: &str, body: Value) -> (u16, Value) {
    let client = reqwest::Client::new();
    for _ in 0..100 {
        let resp = client
            .post(format!("{base}/v1/runs"))
            .json(&body)
            .send()
            .await
            .unwrap();
        let status = resp.status().as_u16();
        if status == 429 || status == 503 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        return (status, resp.json().await.unwrap_or(Value::Null));
    }
    panic!("submission kept being throttled");
}

async fn wait_terminal(base: &str, id: &str) -> Value {
    let client = reqwest::Client::new();
    for _ in 0..600 {
        let body: Value = client
            .get(format!("{base}/v1/runs/{id}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if matches!(
            body["status"].as_str(),
            Some("completed" | "failed" | "cancelled")
        ) {
            return body;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("run {id} never finished");
}

async fn wait_members(base: &str, n: usize) {
    let client = reqwest::Client::new();
    for _ in 0..400 {
        let v: Value = client
            .get(format!("{base}/readyz"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if v["cluster"]["members"].as_array().map_or(0, Vec::len) >= n {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{base} never saw {n} members");
}

/// Owner of every run and shard, and the pools each member advertises.
async fn ownership(
    history: &Path,
) -> (
    BTreeMap<String, String>,
    Vec<(String, String, String)>,
    BTreeMap<String, String>,
) {
    let mut conn = sqlx::SqliteConnection::connect(&format!("sqlite:{}", history.display()))
        .await
        .unwrap();
    let runs: Vec<(String, Option<String>)> =
        sqlx::query_as("SELECT run_id, owner FROM faucet_serve_runs")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    let shards: Vec<(String, String, Option<String>)> =
        sqlx::query_as("SELECT run_id, shard_id, owner FROM faucet_serve_shards")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    let caps: Vec<(String, Option<String>)> =
        sqlx::query_as("SELECT instance_id, pools FROM faucet_serve_instance_caps")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    (
        runs.into_iter()
            .filter_map(|(r, o)| Some((r, o?)))
            .collect(),
        shards
            .into_iter()
            .filter_map(|(r, s, o)| Some((r, s, o?)))
            .collect(),
        caps.into_iter()
            .filter_map(|(i, p)| Some((i, p?)))
            .collect(),
    )
}

fn serves(caps: &BTreeMap<String, String>, owner: &str, pool: &str) -> bool {
    caps.get(owner)
        .is_some_and(|p| p.split(',').any(|x| x == pool))
}

#[tokio::test]
async fn two_pools_share_one_history_and_never_run_each_others_work() {
    let dir = tempfile::tempdir().unwrap();
    let history = dir.path().join("h.db");
    let url = format!("sqlite:{}", history.display());
    let src = dir.path().join("src.db");
    seed_source(&src).await;

    let a = start(args(free_port(), url.clone(), true, &["a"])).await;
    let mut b_args = args(free_port(), url.clone(), true, &["b"]);
    b_args.pools.pool_wait_timeout_secs = Some(1);
    let b = start(b_args).await;
    wait_members(&a, 2).await;
    wait_members(&b, 2).await;

    let ready: Value = reqwest::get(format!("{a}/readyz"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(ready["cluster"]["pools"], json!(["a"]));
    let mut advertised: Vec<Value> = ready["cluster"]["members"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["pools"].clone())
        .collect();
    advertised.sort_by_key(|v| v.to_string());
    assert_eq!(advertised, vec![json!(["a"]), json!(["b"])]);

    // A pool nobody serves, `default` included, is refused naming the pool.
    let out = |n: &str| dir.path().join(format!("{n}.db"));
    let (code, body) = submit(
        &a,
        json!({ "config": config(&src, &out("x"), None, false), "pool": "c" }),
    )
    .await;
    assert_eq!(code, 422, "{body}");
    let msg = body.to_string();
    assert!(
        msg.contains("pool `c`") && msg.contains("live members serve: a, b"),
        "{msg}"
    );
    let (code, body) = submit(
        &a,
        json!({ "config": config(&src, &out("x"), None, false) }),
    )
    .await;
    assert_eq!(code, 422, "{body}");
    assert!(body.to_string().contains("pool `default`"), "{body}");
    let (code, body) = submit(
        &a,
        json!({ "config": config(&src, &out("x"), Some("Bad"), false) }),
    )
    .await;
    assert_eq!(code, 422, "{body}");
    assert!(body.to_string().contains("config `pool:`"), "{body}");

    // Request pool, config pool, request-over-config, and a sharded run per
    // pool — all submitted to A, so B must claim its own from the history.
    let cases = [
        ("req_b", None, Some("b"), false, "b"),
        ("cfg_a", Some("a"), None, false, "a"),
        ("req_wins", Some("a"), Some("b"), false, "b"),
        ("shard_a", Some("a"), None, true, "a"),
        ("shard_b", None, Some("b"), true, "b"),
    ];
    let mut submitted = Vec::new();
    for (name, cfg_pool, req_pool, shards, want) in cases {
        let mut req = json!({ "config": config(&src, &out(name), cfg_pool, shards) });
        if let Some(p) = req_pool {
            req["pool"] = json!(p);
        }
        let (code, body) = submit(&a, req).await;
        assert_eq!(code, 202, "{name}: {body}");
        assert_eq!(body["status"], "pending", "{name}");
        submitted.push((
            name,
            body["run_id"].as_str().unwrap().to_string(),
            want,
            shards,
        ));
    }
    for (name, id, want, _) in &submitted {
        let rec = wait_terminal(&a, id).await;
        assert_eq!(rec["status"], "completed", "{name}: {rec}");
        assert_eq!(rec["pool"], *want, "{name}");
    }

    // A crashed member of pool `a` holds a run: the survivors re-queue it
    // once its lease lapses, and only A claims it again.
    let ghost = SqliteHistory::connect(
        &url,
        Duration::from_secs(3600),
        Duration::ZERO,
        "ghost".into(),
    )
    .await
    .unwrap();
    let mut orphan = RunRecord::queued(
        "orphan".into(),
        None,
        Default::default(),
        None,
        chrono::Utc::now(),
    );
    orphan.status = RunStatus::Pending;
    orphan.pool = "a".into();
    orphan.config_body = Some(config(&src, &out("orphan"), None, false));
    orphan.config_format = Some(faucet_cli::serve::load::ConfigFormat::Yaml);
    ghost.upsert(&orphan).await.unwrap();
    let held = ghost.claim_pending(4, &["a".to_string()]).await.unwrap();
    assert_eq!(held.len(), 1, "the ghost holds the orphan");
    let rec = wait_terminal(&a, "orphan").await;
    assert_eq!(rec["status"], "completed", "{rec}");
    assert_eq!(rec["pool"], "a");
    assert!(
        rec["attempt"].as_u64().unwrap() >= 1,
        "it was re-claimed: {rec}"
    );

    // Every run and shard ran on a replica serving its pool.
    let (owners, shard_owners, caps) = ownership(&history).await;
    assert_eq!(caps.len(), 2, "{caps:?}");
    for (name, id, want, shards) in submitted
        .iter()
        .map(|(n, i, w, s)| (*n, i.clone(), *w, *s))
        .chain(std::iter::once((
            "orphan",
            "orphan".to_string(),
            "a",
            false,
        )))
    {
        if shards {
            let mine: Vec<_> = shard_owners.iter().filter(|(r, _, _)| *r == id).collect();
            assert_eq!(mine.len(), 2, "{name}: both shards ran: {shard_owners:?}");
            for (_, shard, owner) in mine {
                assert!(
                    serves(&caps, owner, want),
                    "{name} shard {shard} ran on {owner}: {caps:?}"
                );
            }
        } else {
            let owner = owners
                .get(&id)
                .unwrap_or_else(|| panic!("{name}: no owner"));
            assert!(
                serves(&caps, owner, want),
                "{name} ran on {owner}: {caps:?}"
            );
        }
    }

    // A run stranded in a pool nobody serves is failed by B's
    // `--pool-wait-timeout-secs`, and the pending gauge reported it.
    let mut stranded = RunRecord::queued(
        "stranded".into(),
        None,
        Default::default(),
        None,
        chrono::Utc::now() - chrono::Duration::seconds(30),
    );
    stranded.status = RunStatus::Pending;
    stranded.pool = "z".into();
    stranded.config_body = Some(config(&src, &out("stranded"), None, false));
    ghost.upsert(&stranded).await.unwrap();
    let rec = wait_terminal(&b, "stranded").await;
    assert_eq!(rec["status"], "failed", "{rec}");
    let err = rec["error"].as_str().unwrap();
    assert!(
        err.starts_with("pool_unavailable: ") && err.contains("pool `z`"),
        "{err}"
    );

    // A live member that predates pools would claim anything: pools other
    // than `default` are refused while it heartbeats.
    {
        let mut conn = sqlx::SqliteConnection::connect(&format!("sqlite:{}", history.display()))
            .await
            .unwrap();
        let far = (chrono::Utc::now() + chrono::Duration::hours(1))
            .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        sqlx::query(
            "INSERT INTO faucet_serve_instances (instance_id, started_at, last_heartbeat, listen, max_concurrent, in_flight) VALUES ('old', ?, ?, 'old:1', '1', '0')",
        )
        .bind(&far)
        .bind(&far)
        .execute(&mut conn)
        .await
        .unwrap();
    }
    wait_members(&a, 3).await;
    let (code, body) = submit(
        &a,
        json!({ "config": config(&src, &out("y"), Some("a"), false) }),
    )
    .await;
    assert_eq!(code, 422, "{body}");
    assert!(
        body.to_string().contains("without execution pools"),
        "{body}"
    );
}

/// A non-clustered server runs only its own pools.
#[tokio::test]
async fn a_single_server_refuses_a_pool_it_does_not_serve() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite:{}", dir.path().join("h.db").display());
    let src = dir.path().join("src.db");
    seed_source(&src).await;
    let base = start(args(free_port(), url, false, &["a", "default"])).await;
    let out = dir.path().join("o.db");

    let (code, body) = submit(
        &base,
        json!({ "config": config(&src, &out, None, false), "pool": "gpu" }),
    )
    .await;
    assert_eq!(code, 422, "{body}");
    assert!(
        body.to_string().contains("does not serve pool `gpu`"),
        "{body}"
    );
    let (code, body) = submit(
        &base,
        json!({ "config": config(&src, &out, None, false), "pool": "a" }),
    )
    .await;
    assert_eq!(code, 202, "{body}");
    let rec = wait_terminal(&base, body["run_id"].as_str().unwrap()).await;
    assert_eq!(
        (rec["status"].as_str(), rec["pool"].as_str()),
        (Some("completed"), Some("a"))
    );
    let (code, body) = submit(&base, json!({ "config": config(&src, &out, None, false) })).await;
    assert_eq!(code, 202, "{body}");
    let rec = wait_terminal(&base, body["run_id"].as_str().unwrap()).await;
    assert_eq!(rec["pool"], "default");
}

/// The history layer: claims, shard claims, re-claims and member rows keep
/// to pools; rows written before pools read as `default`.
#[tokio::test]
async fn history_claims_filter_on_pool() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite:{}", dir.path().join("h.db").display());
    let a = SqliteHistory::connect(
        &url,
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        "a".into(),
    )
    .await
    .unwrap();
    let dead = SqliteHistory::connect(
        &url,
        Duration::from_secs(3600),
        Duration::ZERO,
        "dead".into(),
    )
    .await
    .unwrap();
    let pending = |id: &str, pool: &str| {
        let mut r = RunRecord::queued(
            id.into(),
            None,
            Default::default(),
            None,
            chrono::Utc::now(),
        );
        r.status = RunStatus::Pending;
        r.pool = pool.into();
        r
    };
    for (id, pool) in [("p1", "a"), ("p2", "b"), ("p3", "default")] {
        a.upsert(&pending(id, pool)).await.unwrap();
    }
    // A row an older release wrote has no pool column value.
    {
        let mut conn = sqlx::SqliteConnection::connect(&url).await.unwrap();
        let mut legacy = pending("p4", "default");
        legacy.pool = "default".into();
        a.upsert(&legacy).await.unwrap();
        sqlx::query("UPDATE faucet_serve_runs SET pool = NULL WHERE run_id = 'p4'")
            .execute(&mut conn)
            .await
            .unwrap();
    }
    let mut counts = a.pending_by_pool().await.unwrap();
    counts.sort();
    assert_eq!(
        counts,
        vec![("a".into(), 1), ("b".into(), 1), ("default".into(), 2)]
    );
    let b = vec!["b".to_string()];
    let got: Vec<String> = a
        .claim_pending(10, &b)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.run_id)
        .collect();
    assert_eq!(got, vec!["p2"]);
    let mut got: Vec<String> = a
        .claim_pending(10, &faucet_cli::serve::pools::default_only())
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.run_id)
        .collect();
    got.sort();
    assert_eq!(got, vec!["p3", "p4"], "a NULL pool is `default`");

    // A re-claimed run keeps its pool.
    let held = dead.claim_pending(10, &["a".to_string()]).await.unwrap();
    assert_eq!(held[0].run_id, "p1");
    assert_eq!(a.reclaim_orphans(3).await.unwrap().requeued, 1);
    assert!(
        a.claim_pending(10, &b).await.unwrap().is_empty(),
        "pool b never sees it"
    );
    let again = a.claim_pending(10, &["a".to_string()]).await.unwrap();
    assert_eq!(
        (again[0].run_id.as_str(), again[0].pool.as_str()),
        ("p1", "a")
    );

    // Shards follow their parent's pool, through a re-claim too.
    let mut parent = pending("s", "b");
    parent.status = RunStatus::Sharded;
    a.upsert(&parent).await.unwrap();
    let shards: Vec<ShardInsert> = (0..2)
        .map(|i| ShardInsert {
            shard_id: i.to_string(),
            descriptor: json!({}),
            size_estimate: None,
        })
        .collect();
    a.insert_shards("s", &shards).await.unwrap();
    assert!(
        a.claim_shards(10, &["a".to_string()])
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(dead.claim_shards(1, &b).await.unwrap().len(), 1);
    assert_eq!(a.reclaim_shards(3).await.unwrap().requeued, 1);
    assert!(
        a.claim_shards(10, &["a".to_string()])
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(a.claim_shards(10, &b).await.unwrap().len(), 2);

    // Stranded runs: pending, old enough, in a pool not in the served list.
    let mut old = pending("old", "z");
    old.submitted_at = chrono::Utc::now() - chrono::Duration::seconds(60);
    a.upsert(&old).await.unwrap();
    a.upsert(&pending("new", "z")).await.unwrap();
    let cutoff = chrono::Utc::now() - chrono::Duration::seconds(30);
    let found = a.stranded_pending(&b, cutoff).await.unwrap();
    assert_eq!(
        found.iter().map(|r| r.run_id.as_str()).collect::<Vec<_>>(),
        vec!["old"]
    );
    assert!(
        a.stranded_pending(&["z".to_string()], cutoff)
            .await
            .unwrap()
            .is_empty()
    );
    let mut failed = found[0].clone();
    failed.status = RunStatus::Failed;
    failed.finished_at = Some(chrono::Utc::now());
    assert!(a.fail_pending(&failed).await.unwrap());
    assert!(
        !a.fail_pending(&failed).await.unwrap(),
        "only a pending run is failed"
    );
    assert_eq!(
        a.get("old").await.unwrap().unwrap().status,
        RunStatus::Failed
    );

    // Members advertise their pools; one without a capability row predates them.
    a.heartbeat_instance(&InstanceHeartbeat {
        started_at: chrono::Utc::now(),
        listen: Some("a:1".into()),
        max_concurrent: 2,
        in_flight: 0,
        state_format: faucet_core::state_version::STATE_FORMAT,
        pools: vec!["a".into(), "gpu".into()],
    })
    .await
    .unwrap();
    {
        let mut conn = sqlx::SqliteConnection::connect(&url).await.unwrap();
        let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        sqlx::query("INSERT INTO faucet_serve_instances (instance_id, started_at, last_heartbeat) VALUES ('old', ?, ?)")
            .bind(&now)
            .bind(&now)
            .execute(&mut conn)
            .await
            .unwrap();
    }
    let mut members = a.live_instances(Duration::from_secs(60)).await.unwrap();
    members.sort_by(|x, y| x.instance_id.cmp(&y.instance_id));
    assert_eq!(
        members[0].pools,
        Some(vec!["a".to_string(), "gpu".to_string()])
    );
    assert_eq!(members[1].instance_id, "old");
    assert_eq!(members[1].pools, None);
}
