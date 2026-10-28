//! Runs the reusable `faucet-conformance` battery against the real PostgreSQL
//! query source. Checks 2 & 6 boot a Postgres container via testcontainers
//! (require Docker); check 1 is offline.
//!
//! PostgreSQL query is full-table (no bookmark), so check 3 does not apply;
//! checks 4/5 are sink-only.

use faucet_core::Source;
use faucet_source_postgres::{PostgresSource, PostgresSourceConfig};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;

async fn start_postgres() -> (ContainerAsync<Postgres>, String) {
    let container: ContainerAsync<Postgres> =
        faucet_conformance::containers::start(|| Postgres::default().with_tag("16-alpine")).await;
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("postgres port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    (container, url)
}

async fn seed(url: &str, n: i64) {
    let pool = sqlx::PgPool::connect(url).await.expect("pool connect");
    sqlx::query("CREATE TABLE events (id BIGINT PRIMARY KEY, name TEXT)")
        .execute(&pool)
        .await
        .expect("create table");
    sqlx::query(
        "INSERT INTO events (id, name) SELECT g, 'row-' || g FROM generate_series(1, $1) g",
    )
    .bind(n)
    .execute(&pool)
    .await
    .expect("insert rows");
    pool.close().await;
}

#[test]
fn conformance_config_schema_valid() {
    // Offline: the schema is static, so no container is needed.
    let schema = serde_json::to_value(schemars::schema_for!(PostgresSourceConfig)).unwrap();
    faucet_conformance::assert_config_schema_valid_value(&schema, "postgres");
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_bounded_memory() {
    let total = 5_000;
    let batch = 500;
    let (_container, url) = start_postgres().await;
    seed(&url, total as i64).await;

    let source = PostgresSource::new(
        PostgresSourceConfig::new(&url, "SELECT id, name FROM events ORDER BY id")
            .with_batch_size(batch),
    )
    .await
    .expect("source new");
    faucet_conformance::assert_bounded_memory(&source, batch, total).await;

    // Check 10: connector_name is non-empty.
    faucet_conformance::assert_connector_name_nonempty(&source);
    assert_eq!(source.connector_name(), "postgres");

    // Check 11: preflight check() returns Ok(report) with well-formed probes.
    faucet_conformance::assert_preflight_check_wellformed(
        &source,
        &faucet_core::check::CheckContext::default(),
    )
    .await;

    // Check 9: batch_size=0 yields the entire result set as a single page. Reuse
    // the same seeded container with a fresh source configured for no batching.
    let single_page = PostgresSource::new(
        PostgresSourceConfig::new(&url, "SELECT id, name FROM events ORDER BY id")
            .with_batch_size(0),
    )
    .await
    .expect("source new (batch_size=0)");
    faucet_conformance::assert_batch_size_zero_single_page(&single_page).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_discover_roundtrips() {
    // Check 12: every dataset `discover()` reports must be genuinely selectable
    // — deep-merge its config_patch onto the base config, rebuild, and read.
    let (_container, url) = start_postgres().await;
    seed(&url, 3).await;
    let source = PostgresSource::new(PostgresSourceConfig::new(&url, "SELECT 1"))
        .await
        .expect("source new");
    faucet_conformance::assert_discover_roundtrips(&source, |patch| {
        let url = url.clone();
        async move {
            let base = serde_json::to_value(PostgresSourceConfig::new(&url, "SELECT 1"))
                .expect("serialize base config");
            let merged = faucet_conformance::merge_config_patch(base, &patch);
            let cfg: PostgresSourceConfig =
                serde_json::from_value(merged).expect("deserialize merged config");
            Box::new(PostgresSource::new(cfg).await.expect("rebuilt source"))
                as Box<dyn faucet_core::Source>
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_errors_not_panics() {
    let (_container, url) = start_postgres().await;
    // Valid connection, but the query hits a table that does not exist — the
    // read path must surface a typed FaucetError, never a panic. (Construction
    // connects eagerly and succeeds; the query only fails at read time.)
    let source = PostgresSource::new(PostgresSourceConfig::new(
        url,
        "SELECT * FROM missing_table",
    ))
    .await
    .expect("source builds; the query only fails at read time");
    faucet_conformance::assert_errors_not_panics(&source).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_call_deadline() {
    // A query the server sits on stands in for a hung backend (#880).
    let (_container, url) = start_postgres().await;
    let mut built = Vec::new();
    for _ in 0..2 {
        built.push(
            PostgresSource::new(PostgresSourceConfig::new(&url, "SELECT pg_sleep(60)"))
                .await
                .expect("source new"),
        );
    }
    let built = std::sync::Mutex::new(built);
    faucet_conformance::calls::assert_source_call_deadline(|| built.lock().unwrap().pop().unwrap())
        .await;
}

// ── Check 22: the first page arrives before the result set is exhausted ──────
// Every produced row calls `nextval('served')`; sequences are not
// transactional, so another connection reads how many rows the server had
// produced when the first page arrived.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_first_page_before_exhaustion() {
    let (_container, url) = start_postgres().await;
    let probe = sqlx::PgPool::connect(&url).await.expect("probe pool");
    sqlx::query("CREATE SEQUENCE served")
        .execute(&probe)
        .await
        .expect("create sequence");
    let total = 50_000;
    let batch = 100;
    let source = PostgresSource::new(
        PostgresSourceConfig::new(
            &url,
            format!(
                "SELECT g AS id, nextval('served') AS n, repeat('x', 200) AS pad \
                 FROM generate_series(1, {total}) g"
            ),
        )
        .with_batch_size(batch),
    )
    .await
    .expect("source new");
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

/// Check 25: the declaration names every emitted column with its type, and a
/// `numeric` read from a table column carries its precision.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_declared_columns_truthful() {
    let (_container, url) = start_postgres().await;
    let pool = sqlx::PgPool::connect(&url).await.expect("pool connect");
    for sql in [
        "CREATE TABLE typed (id integer NOT NULL, amount numeric(12,2), total numeric, \
         day date, at timestamptz, local timestamp, t time, uid uuid, b bytea, j jsonb, \
         note text, flag boolean, f double precision, span interval, tags int[])",
        "INSERT INTO typed VALUES (1, 12.50, 1.000000000001, '2024-10-27', \
         '2024-02-22 02:00:00+00', '2024-02-22 02:00:00.5', '10:11:12', \
         '123e4567-e89b-12d3-a456-426614174000', '\\x0102', '{\"a\": 1}', 'x', true, 1.5, \
         '1 day', '{1,2}'), (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, \
         NULL, NULL, NULL, NULL)",
    ] {
        sqlx::query(sql).execute(&pool).await.expect("seed");
    }
    pool.close().await;
    let source = PostgresSource::new(PostgresSourceConfig::new(&url, "SELECT * FROM typed"))
        .await
        .expect("source");
    faucet_conformance::assert_declared_columns_truthful(&source).await;
    let declared = source
        .declared_columns(&Default::default())
        .await
        .unwrap()
        .unwrap();
    use faucet_core::LogicalType as L;
    let t = |n: &str| declared.get(n).unwrap().logical_type;
    assert_eq!(
        t("amount"),
        L::Decimal {
            precision: Some(12),
            scale: Some(2)
        }
    );
    assert_eq!(
        t("total"),
        L::Decimal {
            precision: None,
            scale: None
        }
    );
    assert_eq!(t("at"), L::Timestamp { tz: true });
    assert_eq!(t("local"), L::Timestamp { tz: false });
    assert_eq!(t("span"), L::String);
    assert!(!declared.get("id").unwrap().nullable);
}
