//! Runs the reusable `faucet-conformance` battery against the real PostgreSQL
//! sink.
//!
//! - check 1 `assert_config_schema_valid_value` — static, no Docker; passing it
//!   is the Tier-1 (supported) criterion.
//! - check 4 `assert_idempotent_replay` — the atomic-watermark path
//!   (`write_batch_idempotent` + `last_committed_token`) leaves no duplicates.
//! - check 5 `assert_capabilities_truthful` — the advertised idempotent /
//!   upsert / schema-evolution capabilities all hold.
//!
//! Checks 4 and 5 boot a keyed upsert sink against a real Postgres container, so
//! they require Docker.

use faucet_conformance::assert_config_schema_valid_value;
use faucet_core::{DeleteMarker, Sink, WriteMode, WriteSpec};
use faucet_sink_postgres::{PostgresColumnMapping, PostgresSink, PostgresSinkConfig};
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(
        faucet_sink_postgres::PostgresSinkConfig
    ))
    .unwrap();
    assert_config_schema_valid_value(&schema, "postgres");
}

/// A fresh container with a keyed table `t(id PK, v)` and an upsert-mode
/// Postgres sink pointed at it.
async fn fresh_sink() -> (ContainerAsync<Postgres>, String, PostgresSink) {
    let image = Postgres::default().with_tag("16-alpine");
    let container: ContainerAsync<Postgres> =
        image.start().await.expect("postgres container start");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("postgres port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

    let pool = sqlx::PgPool::connect(&url).await.expect("pool connect");
    sqlx::query("CREATE TABLE t (id BIGINT PRIMARY KEY, v TEXT)")
        .execute(&pool)
        .await
        .expect("create table");
    pool.close().await;

    let mut cfg = PostgresSinkConfig::new(&url, "t")
        .column_mapping(PostgresColumnMapping::AutoMap)
        .with_batch_size(0);
    cfg.write = WriteSpec {
        write_mode: WriteMode::Upsert,
        key: vec!["id".to_string()],
        delete_marker: Some(DeleteMarker {
            field: "__op".to_string(),
            values: vec!["d".to_string()],
        }),
        rollback: None,
    };
    let sink = PostgresSink::new(cfg).await.expect("sink");
    (container, url, sink)
}

async fn count_rows(url: &str) -> usize {
    let pool = sqlx::PgPool::connect(url).await.expect("pool connect");
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*)::BIGINT FROM t")
        .fetch_one(&pool)
        .await
        .expect("count");
    pool.close().await;
    count as usize
}

/// Empty the data table so a following check starts from a known-clean state.
async fn reset_rows(url: &str) {
    let pool = sqlx::PgPool::connect(url).await.expect("pool connect");
    sqlx::query("DELETE FROM t")
        .execute(&pool)
        .await
        .expect("reset rows");
    pool.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_idempotent_replay() {
    let (_container, url, sink) = fresh_sink().await;
    // Check 10: connector_name is non-empty.
    faucet_conformance::assert_connector_name_nonempty_value(
        sink.connector_name(),
        sink.connector_name(),
    );
    assert_eq!(sink.connector_name(), "postgres");
    faucet_conformance::assert_idempotent_replay(&sink, || {
        let url = url.clone();
        async move { count_rows(&url).await }
    })
    .await;
    // Check 8: schema evolution is effective (add-column surfaces in a fresh
    // current_schema()). Independent of row state, so it can follow the replay.
    faucet_conformance::assert_schema_evolution_effective(&sink).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_capabilities_truthful() {
    let (_container, url, sink) = fresh_sink().await;
    // Check 11: the sink's preflight check() returns Ok(report) with well-formed
    // probes (non-mutating).
    faucet_conformance::assert_sink_preflight_check_wellformed(
        &sink,
        &faucet_core::check::CheckContext::default(),
    )
    .await;
    faucet_conformance::assert_capabilities_truthful(&sink, || {
        let url = url.clone();
        async move { count_rows(&url).await }
    })
    .await;
    // Check 7: write modes are truthful (upsert converges, delete removes,
    // missing/null keys are reported failed). Needs a clean table because the
    // capabilities check above wrote rows.
    reset_rows(&url).await;
    faucet_conformance::assert_write_modes_truthful(&sink, || {
        let url = url.clone();
        async move { count_rows(&url).await }
    })
    .await;
}
