//! Multi-table `faucet mirror` against a real Postgres (#731): one replication
//! slot for every table, per-table snapshot handoff, crash recovery at the
//! snapshot and stream crash points (verified with `faucet verify`), new-table
//! pickup, dropped tables, and the status view. Requires Docker.
#![cfg(all(
    feature = "source-postgres-cdc",
    feature = "source-postgres",
    feature = "sink-postgres",
    feature = "transform-cdc-unwrap"
))]

use faucet_cli::config::PipelineConfig;
use faucet_cli::replication::compiled::CompiledReplication;
use faucet_cli::replication::multi_state::{MirrorState, TablePhase};
use faucet_cli::replication::{ReplicationOptions, run_replication};
use faucet_cli::verify::{VerifyInputs, VerifySpec};
use faucet_core::StateStore as _;
use std::path::Path;
use std::time::{Duration, Instant};
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::NoTls;

async fn start_postgres() -> (ContainerAsync<Postgres>, String) {
    let image = Postgres::default()
        .with_host_auth()
        .with_tag("16-alpine")
        .with_cmd([
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_wal_senders=8",
            "-c",
            "max_replication_slots=8",
        ]);
    let container = image.start().await.expect("pg start");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    (
        container,
        format!("postgres://postgres@127.0.0.1:{port}/postgres"),
    )
}

async fn client(url: &str) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(url, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client
}

async fn sql(url: &str, stmt: &str) {
    client(url).await.batch_execute(stmt).await.expect("exec");
}

async fn count(url: &str, query: &str) -> i64 {
    client(url)
        .await
        .query_one(query, &[])
        .await
        .expect("count")
        .get::<_, i64>(0)
}

async fn table_exists(url: &str, table: &str) -> bool {
    count(
        url,
        &format!("SELECT count(*) FROM information_schema.tables WHERE table_name = '{table}'"),
    )
    .await
        > 0
}

fn config(url: &str, state: &Path, continuous: bool, delivery: &str, extra_tables: &str) -> String {
    format!(
        r#"
version: 1
name: shop
delivery: {delivery}
pipeline:
  source:
    type: postgres-cdc
    config:
      connection_url: "{url}"
      slot_name: shop_slot
      publication_name: shop_pub
      idle_timeout: 2
      status_update_interval: 1
  transforms:
    - type: cdc_unwrap
      config: {{}}
  sink:
    type: postgres
    config:
      connection_url: "{url}"
      column_mapping: auto_map
      max_connections: 2
      delete_marker: {{ field: __op, values: [d] }}
  state:
    type: file
    config: {{ path: "{state}" }}
mirror:
  mode: snapshot_then_cdc
  continuous: {continuous}
  snapshot:
    source:
      type: postgres
      config:
        connection_url: "{url}"
        query: "SELECT 1"
    concurrency: 1
  tables:
    include: ["shop.*"]
    exclude: ["shop.audit_*"]
    destination: {{ table_name: "mirror_{{table_name}}" }}
    discover_interval_secs: 2
{extra_tables}"#,
        state = state.display()
    )
}

fn load(yaml: &str) -> (PipelineConfig, CompiledReplication) {
    let cfg = PipelineConfig::from_text(yaml, Path::new("shop.yaml")).unwrap();
    let compiled = CompiledReplication::compile(cfg.replication.as_ref().unwrap(), &cfg).unwrap();
    (cfg, compiled)
}

fn options() -> ReplicationOptions {
    ReplicationOptions {
        pipeline_name: "shop".into(),
        execution: None,
        auth: Default::default(),
        clock: chrono::Utc::now().fixed_offset(),
        resilience: None,
        sla: None,
        reconcile: None,
        verify: None,
        rollback: None,
        usage: Default::default(),
        budget: None,
        #[cfg(feature = "notify")]
        notifier: None,
        #[cfg(feature = "catalog")]
        catalog: None,
    }
}

async fn run(yaml: &str) {
    let (cfg, compiled) = load(yaml);
    run_replication(&cfg, &compiled, options())
        .await
        .expect("mirror run");
}

fn spawn_run(yaml: &str) -> tokio::task::JoinHandle<()> {
    let yaml = yaml.to_string();
    tokio::spawn(async move {
        let (cfg, compiled) = load(&yaml);
        let _ = run_replication(&cfg, &compiled, options()).await;
    })
}

async fn marker(state: &Path) -> Option<MirrorState> {
    let store = faucet_core::FileStateStore::new(state);
    let v = store.get("shop::__replication__").await.ok()??;
    MirrorState::from_value(v).ok()
}

async fn wait_for(what: &str, timeout: Duration, mut check: impl AsyncFnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("timed out waiting for {what}");
}

/// `faucet verify` over one mirrored table: source rows vs destination rows,
/// keyed by `key`, digested inside Postgres.
async fn verify_table(url: &str, state: &Path, source: &str, dest: &str, key: &str) {
    let yaml = format!(
        r#"
version: 1
name: verify_{dest}
pipeline:
  source: {{ type: postgres, config: {{ connection_url: "{url}", query: "SELECT * FROM {source}" }} }}
  sink: {{ type: postgres, config: {{ connection_url: "{url}", table_name: {dest}, column_mapping: auto_map, write_mode: upsert, key: [{key}] }} }}
  state: {{ type: file, config: {{ path: "{}" }} }}
"#,
        state.join("verify").display()
    );
    let cfg = PipelineConfig::from_text(&yaml, Path::new("v.yaml")).unwrap();
    let spec: VerifySpec = serde_yaml::from_str(&format!(
        "key: [{key}]\nranges: 4\nleaf_rows: 16\nexclude: [\"_faucet_*\", \"__op\"]"
    ))
    .unwrap();
    let outcome = faucet_cli::verify::verify(
        &cfg,
        &spec,
        VerifyInputs {
            row: None,
            repair: false,
            allow_delete: false,
            dry_run: false,
            pipeline_name: format!("verify_{dest}"),
            execution: None,
            auth: Default::default(),
            clock: chrono::Utc::now().fixed_offset(),
        },
    )
    .await
    .expect("verify");
    assert!(
        outcome.report.equal(),
        "{source} → {dest} must be an exact mirror: {:?}",
        outcome.report
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn mirrors_every_matching_table_over_one_slot() {
    let (_pg, url) = start_postgres().await;
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    sql(
        &url,
        "CREATE SCHEMA shop; \
         CREATE TABLE shop.orders (id int8 PRIMARY KEY, amount int8); \
         CREATE TABLE shop.items (sku text, loc int8, qty int8, PRIMARY KEY (sku, loc)); \
         CREATE TABLE shop.logs (msg text); \
         CREATE TABLE shop.audit_trail (id int8 PRIMARY KEY); \
         CREATE PUBLICATION shop_pub FOR TABLES IN SCHEMA shop; \
         INSERT INTO shop.orders SELECT g, g * 10 FROM generate_series(1, 50) g; \
         INSERT INTO shop.items VALUES ('a', 1, 1), ('b', 1, 2); \
         INSERT INTO shop.logs VALUES ('x');",
    )
    .await;
    let yaml = config(&url, &state, false, "at_least_once", "");

    let writer_url = url.clone();
    let writer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        sql(
            &writer_url,
            "UPDATE shop.orders SET amount = 0 WHERE id <= 5; \
             DELETE FROM shop.orders WHERE id = 50; \
             INSERT INTO shop.orders VALUES (51, 510); \
             UPDATE shop.items SET qty = 9 WHERE sku = 'a'; \
             INSERT INTO shop.items VALUES ('c', 2, 3);",
        )
        .await;
    });
    run(&yaml).await;
    writer.await.unwrap();
    // A second one-shot pass drains anything that committed after the first
    // pass's stream went idle.
    run(&yaml).await;

    assert_eq!(
        count(&url, "SELECT count(*) FROM pg_replication_slots").await,
        1,
        "every table shares one replication slot"
    );
    verify_table(&url, &state, "shop.orders", "mirror_orders", "id").await;
    verify_table(&url, &state, "shop.items", "mirror_items", "sku, loc").await;
    assert!(
        !table_exists(&url, "mirror_logs").await,
        "a keyless table is refused"
    );
    assert!(
        !table_exists(&url, "mirror_audit_trail").await,
        "excluded by glob"
    );

    let m = marker(&state).await.expect("marker");
    assert_eq!(m.tables["shop.orders"].phase, TablePhase::Active);
    assert_eq!(m.tables["shop.orders"].key, vec!["id"]);
    assert_eq!(m.tables["shop.items"].phase, TablePhase::Active);
    assert_eq!(m.tables["shop.items"].key, vec!["sku", "loc"]);
    assert_eq!(m.tables["shop.logs"].phase, TablePhase::Refused);
    assert!(!m.tables.contains_key("shop.audit_trail"));
    assert_eq!(
        m.tables["shop.orders"].snapshot.attempts, 1,
        "no redo on the second pass"
    );

    let (cfg, _) = load(&yaml);
    let status = faucet_cli::replication::status::read_status(&cfg, "shop")
        .await
        .unwrap();
    assert_eq!(status.mode, "tables");
    assert_eq!(status.summary.by_phase["active"], 2);
    assert_eq!(status.summary.by_phase["refused"], 1);
    let orders = status
        .tables
        .iter()
        .find(|t| t.table == "shop.orders")
        .unwrap();
    let committed = orders.position.as_ref().unwrap();
    assert!(committed.get("last_lsn").is_some());
    assert_ne!(
        Some(committed),
        m.tables["shop.orders"].position.as_ref(),
        "the table's own pipeline advanced its position past the snapshot's"
    );
    assert_eq!(orders.snapshot.percent, Some(100.0));
    let text = faucet_cli::replication::status::render_human(&status);
    assert!(
        text.contains("shop.logs") && text.contains("no primary key"),
        "{text}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn crash_mid_snapshot_redoes_only_the_interrupted_table() {
    let (_pg, url) = start_postgres().await;
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    sql(
        &url,
        "CREATE SCHEMA shop; \
         CREATE TABLE shop.a_small (id int8 PRIMARY KEY, v text); \
         CREATE TABLE shop.b_large (id int8 PRIMARY KEY, v text); \
         CREATE PUBLICATION shop_pub FOR TABLES IN SCHEMA shop; \
         INSERT INTO shop.a_small SELECT g, 'a' || g FROM generate_series(1, 20) g; \
         INSERT INTO shop.b_large SELECT g, repeat('b', 200) FROM generate_series(1, 400000) g;",
    )
    .await;
    let yaml = config(&url, &state, false, "at_least_once", "");
    let handle = spawn_run(&yaml);
    wait_for(
        "a_small active while b_large snapshots",
        Duration::from_secs(120),
        async || {
            marker(&state).await.is_some_and(|m| {
                m.tables
                    .get("shop.a_small")
                    .is_some_and(|t| t.phase == TablePhase::Active)
                    && m.tables
                        .get("shop.b_large")
                        .is_some_and(|t| t.phase == TablePhase::Snapshotting)
            })
        },
    )
    .await;
    handle.abort();
    let _ = handle.await;
    let crashed = marker(&state).await.unwrap();
    assert_eq!(
        crashed.tables["shop.b_large"].phase,
        TablePhase::Snapshotting
    );

    sql(
        &url,
        "UPDATE shop.a_small SET v = 'changed' WHERE id = 1; \
         DELETE FROM shop.b_large WHERE id <= 10; \
         UPDATE shop.b_large SET v = 'z' WHERE id = 399999;",
    )
    .await;
    run(&yaml).await;
    run(&yaml).await;

    let m = marker(&state).await.unwrap();
    assert_eq!(
        m.tables["shop.a_small"].snapshot.attempts, 1,
        "the finished table is kept"
    );
    assert_eq!(
        m.tables["shop.b_large"].snapshot.attempts, 2,
        "only the interrupted table redoes"
    );
    assert_eq!(m.tables["shop.b_large"].phase, TablePhase::Active);
    verify_table(&url, &state, "shop.a_small", "mirror_a_small", "id").await;
    verify_table(&url, &state, "shop.b_large", "mirror_b_large", "id").await;
    assert_eq!(
        count(&url, "SELECT count(*) FROM pg_replication_slots").await,
        1
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn crash_mid_stream_resumes_every_table_exactly_once() {
    let (_pg, url) = start_postgres().await;
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    sql(
        &url,
        "CREATE SCHEMA shop; \
         CREATE TABLE shop.orders (id int8 PRIMARY KEY, amount int8); \
         CREATE TABLE shop.events (id int8, kind text); \
         CREATE PUBLICATION shop_pub FOR TABLES IN SCHEMA shop; \
         INSERT INTO shop.orders SELECT g, g FROM generate_series(1, 10) g; \
         INSERT INTO shop.events SELECT g, 'seed' FROM generate_series(1, 10) g;",
    )
    .await;
    let extra = "    without_primary_key: append\n";
    let live = config(&url, &state, true, "exactly_once", extra);
    let handle = spawn_run(&live);
    wait_for("both tables active", Duration::from_secs(90), async || {
        marker(&state).await.is_some_and(|m| {
            m.tables.len() == 2 && m.tables.values().all(|t| t.phase == TablePhase::Active)
        })
    })
    .await;
    let writer_url = url.clone();
    let writer = tokio::spawn(async move {
        for i in 11..=400 {
            sql(
                &writer_url,
                &format!(
                    "INSERT INTO shop.orders VALUES ({i}, {i}); \
                     UPDATE shop.orders SET amount = amount + 1 WHERE id = {}; \
                     INSERT INTO shop.events VALUES ({i}, 'live');",
                    i % 10 + 1
                ),
            )
            .await;
        }
    });
    wait_for("changes flowing", Duration::from_secs(60), async || {
        table_exists(&url, "mirror_events").await
            && count(&url, "SELECT count(*) FROM mirror_events").await > 60
    })
    .await;
    handle.abort();
    let _ = handle.await;
    writer.await.unwrap();

    run(&config(&url, &state, false, "exactly_once", extra)).await;
    run(&config(&url, &state, false, "exactly_once", extra)).await;

    verify_table(&url, &state, "shop.orders", "mirror_orders", "id").await;
    assert_eq!(
        count(&url, "SELECT count(*) FROM mirror_events").await,
        count(&url, "SELECT count(*) FROM shop.events").await,
        "an append-only table under exactly-once has no duplicate and no gap"
    );
    assert_eq!(
        count(&url, "SELECT count(DISTINCT id) FROM mirror_events").await,
        400
    );
    let m = marker(&state).await.unwrap();
    assert_eq!(m.tables["shop.events"].write_mode, "append");
    assert_eq!(m.tables["shop.events"].snapshot.attempts, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn new_tables_are_picked_up_and_dropped_tables_retired() {
    let (_pg, url) = start_postgres().await;
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    sql(
        &url,
        "CREATE SCHEMA shop; \
         CREATE TABLE shop.orders (id int8 PRIMARY KEY, amount int8); \
         CREATE TABLE shop.legacy (id int8 PRIMARY KEY); \
         CREATE PUBLICATION shop_pub FOR TABLES IN SCHEMA shop; \
         INSERT INTO shop.orders VALUES (1, 1); \
         INSERT INTO shop.legacy VALUES (1);",
    )
    .await;
    let yaml = config(&url, &state, true, "at_least_once", "");
    let handle = spawn_run(&yaml);
    wait_for(
        "initial tables active",
        Duration::from_secs(90),
        async || {
            marker(&state)
                .await
                .is_some_and(|m| m.in_phase(TablePhase::Active).len() == 2)
        },
    )
    .await;

    sql(
        &url,
        "CREATE TABLE shop.fresh (id int8 PRIMARY KEY, note text); \
         INSERT INTO shop.fresh SELECT g, 'n' || g FROM generate_series(1, 25) g; \
         DROP TABLE shop.legacy;",
    )
    .await;
    wait_for(
        "fresh picked up, legacy dropped",
        Duration::from_secs(90),
        async || {
            marker(&state).await.is_some_and(|m| {
                m.tables
                    .get("shop.fresh")
                    .is_some_and(|t| t.phase == TablePhase::Active)
                    && m.tables
                        .get("shop.legacy")
                        .is_some_and(|t| t.phase == TablePhase::Dropped)
            })
        },
    )
    .await;
    sql(
        &url,
        "INSERT INTO shop.fresh VALUES (26, 'after join'); UPDATE shop.orders SET amount = 2;",
    )
    .await;
    wait_for(
        "changes after the join applied",
        Duration::from_secs(60),
        async || {
            table_exists(&url, "mirror_fresh").await
                && count(&url, "SELECT count(*) FROM mirror_fresh").await == 26
                && count(&url, "SELECT count(*) FROM mirror_orders WHERE amount = 2").await == 1
        },
    )
    .await;
    handle.abort();
    let _ = handle.await;

    verify_table(&url, &state, "shop.fresh", "mirror_fresh", "id").await;
    assert!(
        table_exists(&url, "mirror_legacy").await,
        "a dropped table's destination is kept"
    );
    assert_eq!(
        count(&url, "SELECT count(*) FROM pg_replication_slots").await,
        1
    );
}
