//! `faucet-conformance` battery for the Amazon Redshift sink.
//!
//! Check 1 — the config JSON Schema is a valid, well-formed value. Redshift has
//! no local container image and the sink both loads over the PG wire and stages
//! to S3, so end-to-end load coverage lives in the `#[ignore]`d
//! `tests/integration.rs` (a live cluster + S3 bucket in CI).

use faucet_conformance::assert_config_schema_valid_value;

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(
        faucet_sink_redshift::RedshiftSinkConfig
    ))
    .unwrap();
    assert_config_schema_valid_value(&schema, "faucet-sink-redshift");
}

#[tokio::test]
async fn descriptor_matches_the_built_sink() {
    let conn = serde_json::json!({
        "host": "localhost",
        "database": "db",
        "user": "u",
        "credentials": {"type": "password", "config": {"password": "pw"}},
        "table_name": "t",
        "write_strategy": "insert"
    });
    let mut copy = conn.clone();
    copy["write_strategy"] = serde_json::json!("copy");
    copy["copy"] = serde_json::json!({
        "staging_bucket": "bucket",
        "iam_role": "arn:aws:iam::123456789012:role/r",
        "region": "us-east-1"
    });
    for v in [conn, copy] {
        let cfg: faucet_sink_redshift::RedshiftSinkConfig =
            serde_json::from_value(v.clone()).unwrap();
        let built = faucet_sink_redshift::RedshiftSink::new(cfg)
            .await
            .expect("sink");
        faucet_conformance::assert_descriptor_matches_sink(
            &faucet_sink_redshift::DESCRIPTOR,
            "redshift",
            &v,
            &built,
        );
    }
}

// ── Check: backend calls honour the call deadline and cancel (#880) ──────────

#[tokio::test(flavor = "multi_thread")]
async fn conformance_call_deadline() {
    let hung = faucet_conformance::hung::SilentListener::start()
        .await
        .expect("listener");
    let v = serde_json::json!({
        "host": "127.0.0.1",
        "port": hung.port(),
        "database": "db",
        "user": "u",
        "credentials": {"type": "password", "config": {"password": "pw"}},
        "tls_mode": "disable",
        "table_name": "t",
        "write_strategy": "insert"
    });
    let make = || {
        let cfg: faucet_sink_redshift::RedshiftSinkConfig =
            serde_json::from_value(v.clone()).unwrap();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(faucet_sink_redshift::RedshiftSink::new(cfg))
        })
        .unwrap()
    };
    faucet_conformance::calls::assert_sink_call_deadline(make, &[serde_json::json!({"id": 1})])
        .await;
}

// ── #878: declared capabilities over the PostgreSQL wire ──────────────────────

/// The `insert` strategy speaks the PostgreSQL wire protocol, so a stock
/// Postgres container stands in for a cluster (as in `tests/integration.rs`):
/// per-row outcomes, and a cancelled run flushing the commit group the sink
/// buffers (`commit_rows`).
#[tokio::test(flavor = "multi_thread")]
async fn conformance_declared_capabilities() {
    use testcontainers::ImageExt;
    use testcontainers_modules::postgres::Postgres;

    let Some(container) = faucet_conformance::containers::start_or_skip(
        || Postgres::default().with_tag("16-alpine"),
        &faucet_conformance::containers::StartOptions::default(),
    )
    .await
    else {
        return;
    };
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let pool = sqlx::PgPool::connect(&url).await.expect("pool");
    sqlx::query("CREATE TABLE cap (id BIGINT, v TEXT)")
        .execute(&pool)
        .await
        .expect("create");
    let pool = &pool;
    let count = move || async move {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM cap")
            .fetch_one(pool)
            .await
            .expect("count") as usize
    };

    let v = serde_json::json!({
        "host": "127.0.0.1",
        "port": port,
        "database": "postgres",
        "user": "postgres",
        "credentials": {"type": "password", "config": {"password": "postgres"}},
        "tls": false,
        "table_name": "cap",
        "write_strategy": "insert",
        "commit_rows": 1000
    });
    let cfg: faucet_sink_redshift::RedshiftSinkConfig = serde_json::from_value(v).unwrap();
    let sink = faucet_sink_redshift::RedshiftSink::new(cfg)
        .await
        .expect("sink");
    faucet_conformance::assert_partial_outcomes_truthful(&sink, count).await;
    faucet_conformance::assert_cancellation_flushes(&sink, count).await;
}
