//! #828: concurrent first writes — backfill windows, matrix rows — bootstrap
//! the target schema and tables at the same time. Requires Docker.

use faucet_core::{Sink, WriteMode, WriteSpec};
use faucet_sink_postgres::{PostgresColumnMapping, PostgresSink, PostgresSinkConfig};
use serde_json::json;
use std::sync::Arc;
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;

async fn start_postgres() -> (ContainerAsync<Postgres>, String) {
    let container = Postgres::default()
        .with_tag("16-alpine")
        .start()
        .await
        .expect("postgres container start");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    (container, url)
}

fn config(url: &str, schema: &str, table: &str) -> PostgresSinkConfig {
    let mut c = PostgresSinkConfig::new(url, table).column_mapping(PostgresColumnMapping::AutoMap);
    c.schema = Some(schema.to_string());
    c
}

async fn scalar(url: &str, sql: &str) -> i64 {
    let pool = sqlx::PgPool::connect(url).await.expect("pool");
    let n: i64 = sqlx::query_scalar(sql)
        .fetch_one(&pool)
        .await
        .expect("scalar");
    pool.close().await;
    n
}

/// Writes one record through each sink at once, released together.
async fn write_all_at_once(
    sinks: Vec<PostgresSink>,
) -> Vec<Result<usize, faucet_core::FaucetError>> {
    let barrier = Arc::new(tokio::sync::Barrier::new(sinks.len()));
    let mut tasks = tokio::task::JoinSet::new();
    for (i, sink) in sinks.into_iter().enumerate() {
        let barrier = barrier.clone();
        tasks.spawn(async move {
            barrier.wait().await;
            sink.write_batch(&[json!({"id": i as i64, "name": "n"})])
                .await
        });
    }
    let mut out = Vec::new();
    while let Some(r) = tasks.join_next().await {
        out.push(r.expect("task"));
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_bootstraps_into_a_missing_schema_all_succeed() {
    let (_c, url) = start_postgres().await;
    const N: usize = 8;
    // Half share one table, half each get their own, all in one new schema.
    let mut sinks = Vec::with_capacity(N);
    for i in 0..N {
        let table = if i % 2 == 0 {
            "shared".to_string()
        } else {
            format!("own_{i}")
        };
        sinks.push(
            PostgresSink::new(config(&url, "fresh", &table))
                .await
                .expect("sink"),
        );
    }
    for r in write_all_at_once(sinks).await {
        assert_eq!(r.expect("concurrent first write"), 1);
    }
    assert_eq!(scalar(&url, "SELECT count(*) FROM fresh.shared").await, 4);
    assert_eq!(
        scalar(
            &url,
            "SELECT count(*) FROM pg_tables WHERE schemaname = 'fresh' AND tablename LIKE 'own_%'"
        )
        .await,
        4
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_exactly_once_writers_create_the_commit_table_once() {
    let (_c, url) = start_postgres().await;
    const N: usize = 8;
    let barrier = Arc::new(tokio::sync::Barrier::new(N));
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..N {
        // The default schema: the commit-token table is what races here.
        let c = PostgresSinkConfig::new(&url, format!("t_{i}"))
            .column_mapping(PostgresColumnMapping::AutoMap);
        let sink = PostgresSink::new(c).await.expect("sink");
        let barrier = barrier.clone();
        tasks.spawn(async move {
            barrier.wait().await;
            sink.write_batch_idempotent(
                &[json!({"id": i as i64})],
                &format!("scope-{i}"),
                &faucet_core::idempotency::format_token(1),
            )
            .await
        });
    }
    while let Some(r) = tasks.join_next().await {
        assert_eq!(r.expect("task").expect("idempotent first write"), 1);
    }
    assert_eq!(
        scalar(&url, "SELECT count(*) FROM _faucet_commit_token").await,
        N as i64
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_journaled_first_writes_create_the_journal_once() {
    let (_c, url) = start_postgres().await;
    const N: usize = 6;
    let barrier = Arc::new(tokio::sync::Barrier::new(N));
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..N {
        let mut c = config(&url, "jr", &format!("t_{i}"));
        c.write = WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["id".into()],
            delete_marker: None,
            rollback: Some(faucet_core::rollback::RollbackWriteSpec {
                run_id: format!("run-{i}"),
                run_id_column: "_faucet_run_id".into(),
                journal: true,
                keep_previous: false,
            }),
        };
        let sink = PostgresSink::new(c).await.expect("sink");
        let barrier = barrier.clone();
        tasks.spawn(async move {
            barrier.wait().await;
            sink.write_batch(&[json!({"id": i as i64, "_faucet_run_id": format!("run-{i}")})])
                .await
        });
    }
    while let Some(r) = tasks.join_next().await {
        assert_eq!(r.expect("task").expect("journaled first write"), 1);
    }
    assert_eq!(
        scalar(&url, "SELECT count(*) FROM jr._faucet_run_journal").await,
        N as i64
    );
}

/// A clash that is not a race (a type already owns the name) is retried a
/// bounded number of times and then reported, never swallowed.
#[tokio::test(flavor = "multi_thread")]
async fn a_persistent_catalog_clash_is_reported_after_bounded_retries() {
    let (_c, url) = start_postgres().await;
    let pool = sqlx::PgPool::connect(&url).await.expect("pool");
    sqlx::raw_sql("CREATE SCHEMA clash; CREATE TYPE clash.taken AS ENUM ('a')")
        .execute(&pool)
        .await
        .expect("setup");
    pool.close().await;
    let sink = PostgresSink::new(config(&url, "clash", "taken"))
        .await
        .expect("sink");
    let err = sink
        .write_batch(&[json!({"id": 1})])
        .await
        .expect_err("a type owns the name");
    let msg = err.to_string();
    assert!(msg.contains("CREATE SCHEMA/TABLE failed"), "{msg}");
    assert!(msg.contains("already exists"), "{msg}");
}
