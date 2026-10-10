//! Multi-table `faucet mirror` against a real MySQL (#731): every table shares
//! one binlog reader, a crash mid-stream converges on restart (verified with
//! `faucet verify`). Requires Docker.
#![cfg(all(
    feature = "source-mysql-cdc",
    feature = "source-mysql",
    feature = "sink-mysql",
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
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::mysql::Mysql;

async fn start_mysql() -> (ContainerAsync<Mysql>, String) {
    let container = faucet_conformance::containers::start(|| {
        Mysql::default().with_tag("8.1").with_cmd([
            "--server-id=1",
            "--log-bin=mysql-bin",
            "--binlog-format=ROW",
            "--binlog-row-image=FULL",
            "--binlog-row-metadata=FULL",
        ])
    })
    .await;
    let port = container.get_host_port_ipv4(3306).await.expect("port");
    (container, format!("mysql://root@127.0.0.1:{port}/test"))
}

async fn exec(url: &str, stmts: &[&str]) {
    let pool = sqlx::MySqlPool::connect(url).await.expect("connect");
    for s in stmts {
        sqlx::query(s).execute(&pool).await.expect("exec");
    }
    pool.close().await;
}

async fn count(url: &str, query: &str) -> i64 {
    let pool = sqlx::MySqlPool::connect(url).await.expect("connect");
    let n: i64 = sqlx::query_scalar(query)
        .fetch_one(&pool)
        .await
        .expect("count");
    pool.close().await;
    n
}

fn config(url: &str, state: &Path, continuous: bool) -> String {
    format!(
        r#"
version: 1
name: shop
pipeline:
  source:
    type: mysql-cdc
    config:
      connection_url: "{url}"
      server_id: 7301
      idle_timeout: 2
  transforms:
    - type: cdc_unwrap
      config: {{}}
  sink:
    type: mysql
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
      type: mysql
      config:
        connection_url: "{url}"
        query: "SELECT 1"
  tables:
    include: ["*"]
    exclude: ["mirror_*", "_faucet*"]
    destination: {{ table_name: "mirror_{{table_name}}" }}
    discover_interval_secs: 2
"#,
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

async fn verify_table(url: &str, state: &Path, source: &str, dest: &str) {
    let yaml = format!(
        r#"
version: 1
name: verify_{dest}
pipeline:
  source: {{ type: mysql, config: {{ connection_url: "{url}", query: "SELECT * FROM {source}" }} }}
  sink: {{ type: mysql, config: {{ connection_url: "{url}", table_name: {dest}, column_mapping: auto_map, write_mode: upsert, key: [id] }} }}
  state: {{ type: file, config: {{ path: "{}" }} }}
"#,
        state.join("verify").display()
    );
    let cfg = PipelineConfig::from_text(&yaml, Path::new("v.yaml")).unwrap();
    let spec: VerifySpec = serde_yaml::from_str("key: [id]\nranges: 4\nleaf_rows: 16").unwrap();
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
        "{source} → {dest}: {:?}",
        outcome.report
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn mysql_tables_share_one_binlog_reader_and_converge_after_a_crash() {
    let (_c, url) = start_mysql().await;
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    exec(
        &url,
        &[
            "CREATE TABLE orders (id BIGINT PRIMARY KEY, amount BIGINT)",
            "CREATE TABLE items (id BIGINT PRIMARY KEY, sku VARCHAR(32))",
            "INSERT INTO orders VALUES (1, 10), (2, 20), (3, 30)",
            "INSERT INTO items VALUES (1, 'a'), (2, 'b')",
        ],
    )
    .await;
    let live = config(&url, &state, true);
    let handle = {
        let live = live.clone();
        tokio::spawn(async move {
            let (cfg, compiled) = load(&live);
            let _ = run_replication(&cfg, &compiled, options()).await;
        })
    };
    wait_for("both tables active", Duration::from_secs(120), async || {
        marker(&state).await.is_some_and(|m| {
            m.tables
                .get("orders")
                .is_some_and(|t| t.phase == TablePhase::Active)
                && m.tables
                    .get("items")
                    .is_some_and(|t| t.phase == TablePhase::Active)
        })
    })
    .await;
    exec(
        &url,
        &[
            "UPDATE orders SET amount = 99 WHERE id = 1",
            "DELETE FROM orders WHERE id = 2",
            "INSERT INTO items VALUES (3, 'c')",
        ],
    )
    .await;
    let dumps =
        "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE COMMAND LIKE 'Binlog Dump%'";
    let mut seen_one = false;
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        let n = count(&url, dumps).await;
        assert!(n <= 1, "{n} binlog readers for one mirror");
        seen_one |= n == 1;
        if seen_one && count(&url, "SELECT COUNT(*) FROM mirror_items").await == 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(seen_one, "the mirror streams through one binlog reader");
    handle.abort();
    let _ = handle.await;

    exec(
        &url,
        &[
            "INSERT INTO orders VALUES (4, 40)",
            "UPDATE items SET sku = 'z' WHERE id = 1",
        ],
    )
    .await;
    let (cfg, compiled) = load(&config(&url, &state, false));
    run_replication(&cfg, &compiled, options())
        .await
        .expect("resume");
    run_replication(&cfg, &compiled, options())
        .await
        .expect("drain");

    verify_table(&url, &state, "orders", "mirror_orders").await;
    verify_table(&url, &state, "items", "mirror_items").await;
    let m = marker(&state).await.unwrap();
    assert_eq!(m.tables["orders"].snapshot.attempts, 1);
    assert_eq!(m.tables["orders"].key, vec!["id"]);
}
