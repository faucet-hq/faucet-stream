//! End-to-end `faucet replicate` tests against a real Postgres (testcontainers).
//! Requires Docker. Source + destination tables live in the same instance.
#![cfg(all(
    feature = "source-postgres-cdc",
    feature = "source-postgres",
    feature = "sink-postgres",
    feature = "transform-cdc-unwrap"
))]

use faucet_cli::config::PipelineConfig;
use faucet_cli::replication::compiled::CompiledReplication;
use faucet_cli::replication::{ReplicationOptions, run_replication};
use std::time::Duration;
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
            "max_wal_senders=4",
            "-c",
            "max_replication_slots=4",
        ]);
    let container = image.start().await.expect("pg start");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres@127.0.0.1:{port}/postgres");
    (container, url)
}

async fn sql(url: &str, stmt: &str) {
    let (client, conn) = tokio_postgres::connect(url, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client.batch_execute(stmt).await.expect("exec");
}

async fn rows(url: &str, query: &str) -> Vec<(i32, i64)> {
    let (client, conn) = tokio_postgres::connect(url, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client
        .query(query, &[])
        .await
        .expect("query")
        .iter()
        .map(|r| (r.get::<_, i32>(0), r.get::<_, i64>(1)))
        .collect()
}

fn config_yaml(url: &str, state_dir: &str) -> String {
    format!(
        r#"
version: 1
name: orders_mirror
pipeline:
  source:
    type: postgres-cdc
    config:
      connection_url: "{url}"
      slot_name: repl_slot
      publication_name: orders_pub
      idle_timeout: 4
      status_update_interval: 1
  transforms:
    - type: cdc_unwrap
      config: {{}}
  sink:
    type: postgres
    config:
      connection_url: "{url}"
      table_name: orders_mirror
      column_mapping: auto_map
      max_connections: 2
      write_mode: upsert
      key: [id]
      delete_marker: {{ field: __op, values: [d] }}
  state:
    type: file
    config: {{ path: "{state_dir}" }}
replication:
  mode: snapshot_then_cdc
  continuous: false
  snapshot:
    source:
      type: postgres
      config:
        connection_url: "{url}"
        query: "SELECT id, amount FROM public.orders"
"#
    )
}

async fn run_once(url: &str, state_dir: &str) {
    let yaml = config_yaml(url, state_dir);
    let cfg = PipelineConfig::from_text(&yaml, std::path::Path::new("repl.yaml")).unwrap();
    let spec = cfg.replication.clone().unwrap();
    let compiled = CompiledReplication::compile(&spec, &cfg).unwrap();
    run_replication(
        &cfg,
        &compiled,
        ReplicationOptions {
            pipeline_name: "orders_mirror".into(),
            execution: None,
            auth: Default::default(),
            clock: chrono::Utc::now().fixed_offset(),
            resilience: None,
            sla: None,
            reconcile: None,
            verify: None,
            rollback: None,
            #[cfg(feature = "notify")]
            notifier: None,
            #[cfg(feature = "catalog")]
            catalog: None,
        },
    )
    .await
    .expect("replication run");
}

#[tokio::test(flavor = "multi_thread")]
async fn snapshot_then_cdc_mirrors_with_concurrent_writes() {
    let (_pg, url) = start_postgres().await;
    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().to_str().unwrap();

    sql(
        &url,
        "CREATE TABLE public.orders (id int4 PRIMARY KEY, amount int8); \
         CREATE TABLE public.orders_mirror (id int4 PRIMARY KEY, amount int8); \
         CREATE PUBLICATION orders_pub FOR TABLE public.orders; \
         INSERT INTO public.orders VALUES (1, 100), (2, 200);",
    )
    .await;

    // Apply post-bootstrap changes concurrently (after the slot/position is
    // captured these land in the CDC stream; under upsert the boundary is
    // idempotent regardless of interleaving).
    let url2 = url.clone();
    let writer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        sql(&url2, "UPDATE public.orders SET amount = 111 WHERE id = 1;").await;
        sql(&url2, "INSERT INTO public.orders VALUES (3, 300);").await;
        sql(&url2, "DELETE FROM public.orders WHERE id = 2;").await;
    });

    run_once(&url, state_dir).await;
    writer.await.unwrap();
    // CDC drained once (continuous:false, idle_timeout 4s). The writes above all
    // committed before the CDC phase, so they are replayed. Final mirror must
    // equal the final source state: {1:111, 3:300}, no row 2.
    let mut mirror = rows(
        &url,
        "SELECT id, amount FROM public.orders_mirror ORDER BY id",
    )
    .await;
    let mut source = rows(&url, "SELECT id, amount FROM public.orders ORDER BY id").await;
    mirror.sort();
    source.sort();
    assert_eq!(mirror, source, "mirror must equal source after handoff");
    assert_eq!(source, vec![(1, 111), (3, 300)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn resume_skips_snapshot_and_continues_cdc() {
    let (_pg, url) = start_postgres().await;
    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().to_str().unwrap();

    sql(
        &url,
        "CREATE TABLE public.orders (id int4 PRIMARY KEY, amount int8); \
         CREATE TABLE public.orders_mirror (id int4 PRIMARY KEY, amount int8); \
         CREATE PUBLICATION orders_pub FOR TABLE public.orders; \
         INSERT INTO public.orders VALUES (1, 100);",
    )
    .await;

    run_once(&url, state_dir).await; // snapshot_done = true persisted
    assert_eq!(
        rows(
            &url,
            "SELECT id, amount FROM public.orders_mirror ORDER BY id"
        )
        .await,
        vec![(1, 100)]
    );

    // New changes after the first run; a second `replicate` must skip the
    // snapshot and resume CDC from the persisted bookmark.
    sql(
        &url,
        "INSERT INTO public.orders VALUES (2, 200); UPDATE public.orders SET amount = 150 WHERE id = 1;",
    )
    .await;
    run_once(&url, state_dir).await;

    let mut mirror = rows(
        &url,
        "SELECT id, amount FROM public.orders_mirror ORDER BY id",
    )
    .await;
    mirror.sort();
    assert_eq!(mirror, vec![(1, 150), (2, 200)]);
}
