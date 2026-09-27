//! `faucet state show|set|reset|export|import` (#735) end to end: the verbs
//! against a file state store, the run-lease / run-history guards, the
//! exactly-once envelope rules against a real SQLite sink watermark, and the
//! same verbs against Redis and Postgres state stores (Docker; skipped when no
//! Docker daemon is reachable).
#![cfg(all(feature = "source-csv", feature = "sink-sqlite"))]

use clap::Parser;
use faucet_cli::cli::Cli;
use faucet_cli::error::CliError;
use faucet_core::idempotency::{format_token_with_bookmark, unwrap_state, wrap_state};
use faucet_core::{FileStateStore, StateStore};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::sync::Arc;

async fn run(args: &[&str]) -> Result<(), CliError> {
    let mut argv = vec!["faucet"];
    argv.extend_from_slice(args);
    let cli = Cli::try_parse_from(argv).expect("argv parses");
    Box::pin(faucet_cli::run_command(cli)).await
}

fn s(p: &Path) -> String {
    p.display().to_string()
}

/// csv → sqlite with a file state store, rows `a` and `b`.
fn write_config(dir: &Path, state: &str, extra_top: &str) -> PathBuf {
    std::fs::write(dir.join("in.csv"), "id,name\n1,x\n2,y\n").unwrap();
    let text = format!(
        r#"version: 1
name: orders
pipeline:
  source: {{ type: csv, config: {{ path: {input} }} }}
  sink: {{ type: sqlite, config: {{ database_url: "sqlite://{db}?mode=rwc", table_name: t, column_mapping: auto_map, create_table: true }} }}
  state: {state}
{extra_top}
execution: {{ max_concurrent: 1 }}
matrix:
  - id: a
  - id: b
"#,
        input = s(&dir.join("in.csv")),
        db = s(&dir.join("out.db")),
    );
    let path = dir.join("orders.yaml");
    std::fs::write(&path, text).unwrap();
    path
}

fn file_state(dir: &Path) -> String {
    format!(
        "{{ type: file, config: {{ path: {} }} }}",
        s(&dir.join("state"))
    )
}

#[tokio::test]
async fn verbs_round_trip_on_a_file_store() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), &file_state(dir.path()), "");
    let cfgs = s(&cfg);
    let store = FileStateStore::new(dir.path().join("state"));

    // A real run leaves run-outcome markers and releases its lease.
    let text = std::fs::read_to_string(&cfg).unwrap();
    let summary = faucet_cli::run_from_yaml_str(&text).await.unwrap();
    assert!(!summary.had_failures(), "{summary:?}");
    assert!(store.get("orders::a::__status__").await.unwrap().is_some());
    assert!(store.get("orders::a::__lease__").await.unwrap().is_none());

    store.put("orders::a", &json!({"id": 2})).await.unwrap();
    run(&["state", "show", &cfgs]).await.unwrap();
    run(&["state", "show", &cfgs, "--row", "a", "--json"])
        .await
        .unwrap();

    // set: needs --yes without a terminal; --dry-run writes nothing.
    let err = run(&[
        "state",
        "set",
        &cfgs,
        "--row",
        "a",
        "--bookmark",
        r#"{"id":9}"#,
    ])
    .await
    .unwrap_err();
    assert!(err.to_string().contains("--yes"), "{err}");
    run(&[
        "state",
        "set",
        &cfgs,
        "--row",
        "a",
        "--bookmark",
        r#"{"id":9}"#,
        "--dry-run",
    ])
    .await
    .unwrap();
    assert_eq!(
        store
            .get("orders::a")
            .await
            .unwrap()
            .map(|v| faucet_core::state_version::peel_versioned(&v)),
        Some(json!({"id": 2}))
    );
    run(&[
        "state",
        "set",
        &cfgs,
        "--row",
        "a",
        "--bookmark",
        r#"{"id":9}"#,
        "--yes",
        "--json",
    ])
    .await
    .unwrap();
    assert_eq!(
        store
            .get("orders::a")
            .await
            .unwrap()
            .map(|v| faucet_core::state_version::peel_versioned(&v)),
        Some(json!({"id": 9}))
    );
    let err = run(&[
        "state",
        "set",
        &cfgs,
        "--row",
        "a",
        "--bookmark",
        "not json",
        "--yes",
    ])
    .await
    .unwrap_err();
    assert!(err.to_string().contains("must be JSON"), "{err}");
    let err = run(&[
        "state",
        "set",
        &cfgs,
        "--row",
        "zz",
        "--bookmark",
        "1",
        "--yes",
    ])
    .await
    .unwrap_err();
    assert!(err.to_string().contains("no row 'zz'"), "{err}");

    // export → import onto another backend location.
    let backup = dir.path().join("backup.json");
    run(&["state", "export", &cfgs, "-o", &s(&backup)])
        .await
        .unwrap();
    run(&["state", "export", &cfgs]).await.unwrap();
    let doc: Value = serde_json::from_str(&std::fs::read_to_string(&backup).unwrap()).unwrap();
    assert_eq!(doc["version"], 1);
    assert_eq!(doc["pipeline"], "orders");
    assert!(doc["keys"]["orders::a"].is_object());
    assert!(doc["keys"]["orders::a::__status__"].is_object());

    let moved = dir.path().join("moved");
    let to = format!("file:{}", s(&moved));
    run(&[
        "state",
        "import",
        &cfgs,
        &s(&backup),
        "--to-state",
        &to,
        "--dry-run",
    ])
    .await
    .unwrap();
    assert!(!moved.exists(), "dry run writes nothing");
    run(&[
        "state",
        "import",
        &cfgs,
        &s(&backup),
        "--to-state",
        &to,
        "--yes",
    ])
    .await
    .unwrap();
    let target = FileStateStore::new(&moved);
    assert_eq!(
        target
            .get("orders::a")
            .await
            .unwrap()
            .map(|v| faucet_core::state_version::peel_versioned(&v)),
        Some(json!({"id": 9}))
    );
    let err = run(&[
        "state",
        "import",
        &cfgs,
        &s(&backup),
        "--to-state",
        &to,
        "--yes",
    ])
    .await
    .unwrap_err();
    assert!(err.to_string().contains("--overwrite"), "{err}");
    target.put("orders::b::stale", &json!(1)).await.unwrap();
    run(&[
        "state",
        "import",
        &cfgs,
        &s(&backup),
        "--to-state",
        &to,
        "--overwrite",
        "--yes",
        "--json",
    ])
    .await
    .unwrap();
    assert!(target.get("orders::b::stale").await.unwrap().is_none());

    // A document for another pipeline, a future version, or garbage is refused.
    let mut other = doc.clone();
    other["pipeline"] = json!("else");
    other["keys"] = json!({});
    let other_path = dir.path().join("other.json");
    std::fs::write(&other_path, other.to_string()).unwrap();
    let err = run(&[
        "state",
        "import",
        &cfgs,
        &s(&other_path),
        "--to-state",
        &to,
        "--yes",
    ])
    .await
    .unwrap_err();
    assert!(err.to_string().contains("holds pipeline 'else'"), "{err}");
    let mut future = doc.clone();
    future["version"] = json!(99);
    std::fs::write(&other_path, future.to_string()).unwrap();
    assert!(
        run(&["state", "import", &cfgs, &s(&other_path), "--yes"])
            .await
            .is_err()
    );
    std::fs::write(&other_path, "nope").unwrap();
    assert!(
        run(&["state", "import", &cfgs, &s(&other_path), "--yes"])
            .await
            .is_err()
    );
    assert!(
        run(&[
            "state",
            "import",
            &cfgs,
            &s(&dir.path().join("missing.json")),
            "--yes"
        ])
        .await
        .is_err()
    );

    // reset forgets the position; --include-markers the markers too.
    run(&["state", "reset", &cfgs, "--row", "a", "--dry-run"])
        .await
        .unwrap();
    assert!(store.get("orders::a").await.unwrap().is_some());
    run(&["state", "reset", &cfgs, "--row", "a", "--yes"])
        .await
        .unwrap();
    assert!(store.get("orders::a").await.unwrap().is_none());
    assert!(store.get("orders::a::__status__").await.unwrap().is_some());
    run(&[
        "state",
        "reset",
        &cfgs,
        "--row",
        "a",
        "--include-markers",
        "--yes",
        "--json",
    ])
    .await
    .unwrap();
    assert!(store.get("orders::a::__status__").await.unwrap().is_none());
    // Nothing left to reset is not an error.
    run(&["state", "reset", &cfgs, "--row", "a", "--yes"])
        .await
        .unwrap();
    run(&["state", "reset", &cfgs, "--row", "a", "--yes", "--json"])
        .await
        .unwrap();
}

#[tokio::test]
async fn a_live_run_lease_blocks_mutations_until_forced() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), &file_state(dir.path()), "");
    let cfgs = s(&cfg);
    let store: Arc<dyn StateStore> = Arc::new(FileStateStore::new(dir.path().join("state")));
    let lease = faucet_cli::pipeline_state::lease::acquire(Arc::clone(&store), "orders::a", "busy")
        .await
        .unwrap();
    let err = run(&[
        "state",
        "set",
        &cfgs,
        "--row",
        "a",
        "--bookmark",
        "1",
        "--yes",
    ])
    .await
    .unwrap_err();
    assert!(matches!(err, CliError::StateBusy(_)), "{err}");
    let err = run(&["state", "reset", &cfgs, "--row", "a", "--yes"])
        .await
        .unwrap_err();
    assert!(matches!(err, CliError::StateBusy(_)), "{err}");
    let backup = dir.path().join("b.json");
    std::fs::write(
        &backup,
        json!({"version": 1, "pipeline": "orders", "keys": {"orders::a": 1}}).to_string(),
    )
    .unwrap();
    let err = run(&["state", "import", &cfgs, &s(&backup), "--yes"])
        .await
        .unwrap_err();
    assert!(matches!(err, CliError::StateBusy(_)), "{err}");
    run(&[
        "state",
        "set",
        &cfgs,
        "--row",
        "a",
        "--bookmark",
        "1",
        "--yes",
        "--force",
    ])
    .await
    .unwrap();
    assert_eq!(
        store
            .get("orders::a")
            .await
            .unwrap()
            .map(|v| faucet_core::state_version::peel_versioned(&v)),
        Some(json!(1))
    );
    lease.release().await;
}

#[cfg(feature = "catalog")]
#[tokio::test]
async fn an_in_flight_run_in_the_catalog_history_blocks_mutations() {
    use faucet_cli::serve::history::{RunRecord, RunStatus};
    let dir = tempfile::tempdir().unwrap();
    let catalog = format!(
        "catalog: {{ url: \"sqlite:{}\" }}",
        s(&dir.path().join("cat.db"))
    );
    let cfg = write_config(dir.path(), &file_state(dir.path()), &catalog);
    let cfgs = s(&cfg);
    let spec = faucet_cli::config::PipelineConfig::from_path(&cfg, None)
        .unwrap()
        .catalog
        .unwrap();
    let handle = faucet_cli::catalog::connect_from_spec(&spec).await.unwrap();
    let mut rec = RunRecord::queued(
        "run-in-flight".into(),
        Some("orders".into()),
        Default::default(),
        None,
        chrono::Utc::now(),
    );
    rec.status = RunStatus::Running;
    handle.store.upsert(&rec).await.unwrap();
    let err = run(&[
        "state",
        "set",
        &cfgs,
        "--row",
        "a",
        "--bookmark",
        "1",
        "--yes",
    ])
    .await
    .unwrap_err();
    assert!(err.to_string().contains("run-in-flight"), "{err}");
    run(&[
        "state",
        "set",
        &cfgs,
        "--row",
        "a",
        "--bookmark",
        "1",
        "--yes",
        "--force",
    ])
    .await
    .unwrap();
}

/// An exactly-once config: a CDC source (never built here) into a SQLite sink
/// that commits a watermark with each page.
fn eo_config(dir: &Path) -> (PathBuf, Value) {
    let sink_cfg = json!({
        "database_url": format!("sqlite://{}?mode=rwc", s(&dir.join("eo.db"))),
        "table_name": "t",
        "column_mapping": "auto_map",
        "create_table": true,
    });
    let text = format!(
        r#"version: 1
name: cdc
delivery: exactly_once
pipeline:
  source: {{ type: postgres-cdc, config: {{ connection_url: "postgres://u:p@localhost/db", slot_name: s, publication: p }} }}
  sink: {{ type: sqlite, config: {sink} }}
  state: {state}
"#,
        sink = sink_cfg,
        state = file_state(dir),
    );
    let path = dir.join("cdc.yaml");
    std::fs::write(&path, text).unwrap();
    (path, sink_cfg)
}

async fn commit_token(sink_cfg: &Value, scope: &str, seq: u64, bookmark: Value) {
    let sink = faucet_cli::registry::build_sink("sqlite", sink_cfg.clone(), &Default::default())
        .await
        .unwrap();
    sink.write_batch_idempotent(
        &[json!({"id": seq})],
        scope,
        &format_token_with_bookmark(seq, Some(&bookmark)),
    )
    .await
    .unwrap();
}

async fn sink_token(sink_cfg: &Value, scope: &str) -> Option<String> {
    faucet_cli::registry::build_sink("sqlite", sink_cfg.clone(), &Default::default())
        .await
        .unwrap()
        .last_committed_token(scope)
        .await
        .unwrap()
}

#[tokio::test]
async fn exactly_once_rows_keep_a_sink_safe_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let (cfg, sink_cfg) = eo_config(dir.path());
    let cfgs = s(&cfg);
    let store = FileStateStore::new(dir.path().join("state"));
    let key = "cdc::row-0";
    store
        .put(key, &wrap_state(Some(&json!({"lsn": "0/3"})), 3))
        .await
        .unwrap();
    commit_token(&sink_cfg, key, 7, json!({"lsn": "0/7"})).await;

    // The sink is ahead: `set` writes its sequence so the move sticks.
    run(&[
        "state",
        "set",
        &cfgs,
        "--row",
        "row-0",
        "--bookmark",
        r#"{"lsn":"0/1"}"#,
        "--yes",
    ])
    .await
    .unwrap();
    let (bm, seq) = unwrap_state(&store.get(key).await.unwrap().unwrap());
    assert_eq!((bm, seq), (Some(json!({"lsn": "0/1"})), 7));

    // Unchecked, the sequence is only kept.
    store
        .put(key, &wrap_state(Some(&json!({"lsn": "0/3"})), 3))
        .await
        .unwrap();
    run(&[
        "state",
        "set",
        &cfgs,
        "--row",
        "row-0",
        "--bookmark",
        r#"{"lsn":"0/2"}"#,
        "--yes",
        "--skip-watermark-check",
    ])
    .await
    .unwrap();
    assert_eq!(unwrap_state(&store.get(key).await.unwrap().unwrap()).1, 3);

    // reset keeps the envelope with a null bookmark at the sink's sequence.
    run(&["state", "reset", &cfgs, "--row", "row-0", "--yes"])
        .await
        .unwrap();
    assert_eq!(
        unwrap_state(&store.get(key).await.unwrap().unwrap()),
        (None, 7)
    );

    // --rewind-token deletes the sink's token and the envelope.
    run(&[
        "state",
        "reset",
        &cfgs,
        "--row",
        "row-0",
        "--rewind-token",
        "--yes",
    ])
    .await
    .unwrap();
    assert!(store.get(key).await.unwrap().is_none());
    assert!(sink_token(&sink_cfg, key).await.is_none());
}

/// Start a container, or `None` (skip) when no Docker daemon answers.
#[cfg(any(feature = "state-postgres", feature = "state-redis"))]
async fn start<I: testcontainers::Image>(
    image: testcontainers::ContainerRequest<I>,
) -> Option<testcontainers::ContainerAsync<I>> {
    use testcontainers::runners::AsyncRunner;
    match image.start().await {
        Ok(c) => Some(c),
        Err(e) => {
            eprintln!("skipping: no Docker daemon ({e})");
            None
        }
    }
}

#[cfg(all(feature = "state-postgres", feature = "state-redis"))]
#[tokio::test(flavor = "multi_thread")]
async fn verbs_work_against_redis_and_postgres_and_migrate_between_them() {
    use testcontainers::ImageExt;
    let Some(pg) =
        start(testcontainers_modules::postgres::Postgres::default().with_tag("16-alpine")).await
    else {
        return;
    };
    let Some(rd) = start(testcontainers::ContainerRequest::from(
        testcontainers_modules::redis::Redis::default(),
    ))
    .await
    else {
        return;
    };
    let pg_url = format!(
        "postgres://postgres:postgres@127.0.0.1:{}/postgres",
        pg.get_host_port_ipv4(5432).await.unwrap()
    );
    let redis_url = format!(
        "redis://127.0.0.1:{}",
        rd.get_host_port_ipv4(6379).await.unwrap()
    );

    let dir = tempfile::tempdir().unwrap();
    // Seed on Redis, operate there, then migrate to Postgres.
    let redis_dir = dir.path().join("r");
    std::fs::create_dir_all(&redis_dir).unwrap();
    let redis_cfg = write_config(
        &redis_dir,
        &format!("{{ type: redis, config: {{ url: \"{redis_url}\", namespace: faucet }} }}"),
        "",
    );
    let rcfg = s(&redis_cfg);
    let mut redis_store = None;
    for _ in 0..50 {
        match faucet_state_redis::RedisStateStore::connect(&redis_url, "faucet").await {
            Ok(s) => {
                redis_store = Some(s);
                break;
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(200)).await,
        }
    }
    let redis_store = redis_store.expect("redis reachable");
    redis_store
        .put("orders::a", &json!({"id": 1}))
        .await
        .unwrap();
    redis_store
        .put("orders::b::__sla__", &json!({"last_success_unix": 5}))
        .await
        .unwrap();
    run(&["state", "show", &rcfg]).await.unwrap();
    run(&[
        "state",
        "set",
        &rcfg,
        "--row",
        "b",
        "--bookmark",
        r#"{"id":4}"#,
        "--yes",
    ])
    .await
    .unwrap();
    assert_eq!(
        redis_store
            .get("orders::b")
            .await
            .unwrap()
            .map(|v| faucet_core::state_version::peel_versioned(&v)),
        Some(json!({"id": 4}))
    );
    let backup = dir.path().join("redis.json");
    run(&["state", "export", &rcfg, "-o", &s(&backup)])
        .await
        .unwrap();

    run(&[
        "state",
        "import",
        &rcfg,
        &s(&backup),
        "--to-state",
        &pg_url,
        "--yes",
    ])
    .await
    .unwrap();
    let pg_dir = dir.path().join("p");
    std::fs::create_dir_all(&pg_dir).unwrap();
    let pg_cfg = write_config(
        &pg_dir,
        &format!("{{ type: postgres, config: {{ url: \"{pg_url}\" }} }}"),
        "",
    );
    let pcfg = s(&pg_cfg);
    let pg_store = faucet_state_postgres::PostgresStateStore::connect(&pg_url)
        .await
        .unwrap();
    assert_eq!(
        pg_store
            .get("orders::a")
            .await
            .unwrap()
            .map(|v| faucet_core::state_version::peel_versioned(&v)),
        Some(json!({"id": 1}))
    );
    assert_eq!(
        pg_store
            .get("orders::b")
            .await
            .unwrap()
            .map(|v| faucet_core::state_version::peel_versioned(&v)),
        Some(json!({"id": 4}))
    );
    assert!(pg_store.get("orders::b::__sla__").await.unwrap().is_some());
    run(&["state", "show", &pcfg, "--json"]).await.unwrap();
    run(&[
        "state",
        "reset",
        &pcfg,
        "--row",
        "a",
        "--include-markers",
        "--yes",
    ])
    .await
    .unwrap();
    assert!(pg_store.get("orders::a").await.unwrap().is_none());
    // Replace with --overwrite restores it exactly.
    run(&[
        "state",
        "import",
        &pcfg,
        &s(&backup),
        "--overwrite",
        "--yes",
    ])
    .await
    .unwrap();
    assert_eq!(
        pg_store
            .get("orders::a")
            .await
            .unwrap()
            .map(|v| faucet_core::state_version::peel_versioned(&v)),
        Some(json!({"id": 1}))
    );
    run(&["state", "reset", &rcfg, "--row", "b", "--yes"])
        .await
        .unwrap();
    assert!(redis_store.get("orders::b").await.unwrap().is_none());
}
