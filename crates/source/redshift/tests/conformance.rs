//! `faucet-conformance` battery for the Amazon Redshift source.
//!
//! Check 1 — the config JSON Schema is a valid, well-formed value.
//!
//! Bounded-memory (Check 2) requires a live Redshift cluster (it speaks the
//! PostgreSQL wire protocol, so there is no mock/wiremock path as for the
//! HTTP-based warehouse sources). It is exercised by the `#[ignore]`d
//! `tests/integration.rs` against a real cluster in CI instead.
//!
//! Check 22 runs against a PostgreSQL container, which serves the same wire
//! protocol the source reads (see `tests/integration.rs`).

use faucet_conformance::assert_config_schema_valid_value;

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(
        faucet_source_redshift::RedshiftSourceConfig
    ))
    .unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-redshift");
}

#[tokio::test]
async fn descriptor_matches_the_built_source() {
    let v = serde_json::json!({
        "host": "localhost",
        "database": "db",
        "user": "u",
        "credentials": {"type": "password", "config": {"password": "pw"}},
        "query": "SELECT 1"
    });
    let cfg: faucet_source_redshift::RedshiftSourceConfig =
        serde_json::from_value(v.clone()).unwrap();
    let built = faucet_source_redshift::RedshiftSource::new(cfg).expect("source");
    faucet_conformance::assert_descriptor_matches_source(
        &faucet_source_redshift::DESCRIPTOR,
        "redshift",
        &v,
        &built,
    );
}

// ── Check: backend calls honour the call deadline and cancel (#880) ──────────

#[tokio::test]
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
        "query": "SELECT 1"
    });
    faucet_conformance::calls::assert_source_call_deadline(|| {
        faucet_source_redshift::RedshiftSource::new(serde_json::from_value(v.clone()).unwrap())
            .unwrap()
    })
    .await;
}

/// Check 22: the first page arrives before the result set is exhausted. Every
/// produced row calls `nextval('served')`; sequences are not transactional, so
/// another connection reads how many rows the server had produced when the
/// first page arrived.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_first_page_before_exhaustion() {
    use testcontainers::ImageExt;
    let container = faucet_conformance::containers::start(|| {
        testcontainers_modules::postgres::Postgres::default().with_tag("16-alpine")
    })
    .await;
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("postgres port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let probe = sqlx::PgPool::connect(&url).await.expect("probe pool");
    sqlx::query("CREATE SEQUENCE served")
        .execute(&probe)
        .await
        .expect("create sequence");
    let total = 50_000;
    let batch = 100;
    let mut connection = faucet_source_redshift::RedshiftConnection::new(
        "127.0.0.1",
        "postgres",
        "postgres",
        "postgres",
    );
    connection.port = port;
    connection.tls = false;
    let source =
        faucet_source_redshift::RedshiftSource::new(faucet_source_redshift::RedshiftSourceConfig {
            connection,
            query: format!(
                "SELECT g AS id, nextval('served') AS n, repeat('x', 200) AS pad \
                 FROM generate_series(1, {total}) g"
            ),
            params: Vec::new(),
            max_connections: 2,
            batch_size: batch,
            replication: faucet_source_redshift::RedshiftReplication::Full,
            state_key: None,
        })
        .expect("source");
    let served = faucet_conformance::served_probe(|| async {
        sqlx::query_scalar::<_, i64>(
            "SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM served",
        )
        .fetch_one(&probe)
        .await
        .expect("read sequence") as usize
    });
    faucet_conformance::assert_first_page_before_exhaustion(&source, batch, total, &served).await;
    // The counter is live: once the stream is drained it has seen every row.
    assert!(
        served() >= total,
        "the served-row counter missed rows: {}",
        served()
    );
}
