//! `faucet verify` between two Postgres tables (#701): both sides report the
//! same digest algorithm, so a matching dataset verifies **server-side**
//! without shipping rows, and a single changed row is found by bisecting the
//! key ranges down to `leaf_rows`. Requires Docker.

use faucet_cli::config::PipelineConfig;
use faucet_cli::executor::{ExecuteOptions, run_expanded};
use faucet_cli::expand::expand;
use faucet_cli::verify::{VerifyInputs, VerifySpec};
use std::path::Path;
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;

async fn start_postgres() -> (ContainerAsync<Postgres>, String) {
    let image = Postgres::default().with_tag("16-alpine");
    let container: ContainerAsync<Postgres> =
        image.start().await.expect("postgres container start");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("postgres port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    (container, url)
}

async fn exec(url: &str, sql: &str) {
    let pool = sqlx::PgPool::connect(url).await.unwrap();
    sqlx::query(sql).execute(&pool).await.unwrap();
    pool.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn postgres_pair_verifies_with_server_digests_and_bisects_drift() {
    let (_c, url) = start_postgres().await;
    exec(
        &url,
        "CREATE TABLE src (id BIGINT PRIMARY KEY, name TEXT, amount NUMERIC(8,2))",
    )
    .await;
    exec(
        &url,
        "INSERT INTO src SELECT g, 'n' || g, g * 1.5 FROM generate_series(1, 200) g",
    )
    .await;
    exec(
        &url,
        "CREATE TABLE dst (id BIGINT PRIMARY KEY, name TEXT, amount NUMERIC(8,2), _faucet_run_id TEXT)",
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
version: 1
name: pg_mirror
pipeline:
  source:
    type: postgres
    config:
      connection_url: "{url}"
      query: "SELECT id, name, amount FROM src"
  sink:
    type: postgres
    config:
      connection_url: "{url}"
      table_name: dst
      column_mapping: auto_map
      write_mode: upsert
      key: [id]
  state:
    type: file
    config:
      path: "{}"
"#,
        dir.path().join("state").display()
    );
    let cfg = PipelineConfig::from_text(&yaml, Path::new("pg.yaml")).unwrap();
    let summary = run_expanded(
        expand(&cfg).unwrap(),
        ExecuteOptions {
            pipeline_name: "pg_mirror".into(),
            run_id: None,
            execution: None,
            concurrency: None,
            dry_run: false,
            limit: None,
            state_path_override: None,
            shard: None,
            auth: Default::default(),
            clock: chrono::Utc::now().fixed_offset(),
            cancel: None,
            resilience: None,
            sla: None,
            reconcile: None,
            verify: None,
            rollback: None,
            #[cfg(feature = "lineage")]
            lineage: None,
            #[cfg(feature = "lineage")]
            lineage_cfg: None,
            #[cfg(feature = "notify")]
            notifier: None,
            #[cfg(feature = "catalog")]
            catalog: None,
        },
    )
    .await
    .unwrap();
    assert!(!summary.had_failures(), "{summary:?}");

    let inputs = || VerifyInputs {
        row: None,
        repair: false,
        allow_delete: false,
        dry_run: false,
        pipeline_name: "pg_mirror".into(),
        execution: None,
        auth: Default::default(),
        clock: chrono::Utc::now().fixed_offset(),
    };
    let spec: VerifySpec = serde_yaml::from_str("ranges: 4\nleaf_rows: 8").unwrap();

    // Equal: the whole-dataset server digests agree and no row is fetched.
    let equal = faucet_cli::verify::verify(&cfg, &spec, inputs())
        .await
        .unwrap();
    assert!(equal.report.equal(), "{equal:?}");
    assert!(equal.report.server_digests, "digested inside postgres");
    assert_eq!(equal.report.rows_fetched_source, 0);
    assert_eq!(equal.report.rows_fetched_dest, 0);

    // One changed row: the planned ranges disagree in exactly one place, the
    // bisection narrows it to a leaf of ≤ 8 rows, and only that leaf ships.
    exec(&url, "UPDATE dst SET amount = 0 WHERE id = 137").await;
    let drifted = faucet_cli::verify::verify(&cfg, &spec, inputs())
        .await
        .unwrap();
    let (missing, extra, changed, dup) = drifted.report.tally();
    assert_eq!((missing, extra, changed, dup), (0, 0, 1, 0), "{drifted:?}");
    assert_eq!(drifted.report.differences[0].key["id"], 137);
    assert!(drifted.report.server_digests);
    assert!(
        drifted.report.rows_fetched_source <= 8,
        "only the leaf was fetched: {drifted:?}"
    );
    assert!(
        drifted.report.ranges_compared > 4,
        "bisected past the first pass"
    );

    // Repair heals it through the sink's upsert path.
    let repaired = faucet_cli::verify::verify(
        &cfg,
        &spec,
        VerifyInputs {
            repair: true,
            ..inputs()
        },
    )
    .await
    .unwrap();
    assert_eq!(repaired.report.repaired_upserts, Some(1));
    let clean = faucet_cli::verify::verify(&cfg, &spec, inputs())
        .await
        .unwrap();
    assert!(clean.report.equal(), "{clean:?}");
}
