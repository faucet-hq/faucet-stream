//! `faucet-conformance` battery for the MySQL source.
//!
//! Check 1 (config-schema validity) is pure and offline. Check 2
//! (bounded-memory streaming) boots a real MySQL via testcontainers and so
//! requires Docker — it runs in CI alongside the other integration tests.

use faucet_conformance::assert_config_schema_valid_value;
use faucet_core::Source;
use faucet_source_mysql::{MysqlSource, MysqlSourceConfig};
use std::sync::OnceLock;
use testcontainers::ContainerAsync;
use testcontainers_modules::mysql::Mysql;
use tokio::sync::Semaphore;

// ── Check 1: config schema ──────────────────────────────────────────────────

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(MysqlSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-mysql");
}

// ── Check 2: bounded-memory streaming (Docker) ──────────────────────────────

/// Bounds concurrent MySQL container startups across all tests in this
/// binary. MySQL 8.x init is heavy (~2-3 GB RSS per container during
/// startup) and starting several in parallel exhausts memory on Colima/Docker
/// Desktop, surfacing as random "Failed to start mysqld daemon" errors. We
/// allow at most two simultaneous startups; once a container is running it
/// is steady-state cheap, so the cap only serialises the spin-up window.
fn startup_limit() -> &'static Semaphore {
    static SEM: OnceLock<Semaphore> = OnceLock::new();
    SEM.get_or_init(|| Semaphore::new(2))
}

/// Start a MySQL container and return both the container handle and a
/// connection URL. The container is kept alive by the returned handle; drop
/// it to stop the container.
async fn start_mysql() -> (ContainerAsync<Mysql>, String) {
    let _permit = startup_limit()
        .acquire()
        .await
        .expect("startup semaphore closed");
    let container: ContainerAsync<Mysql> =
        faucet_conformance::containers::start(Mysql::default).await;
    let port = container
        .get_host_port_ipv4(3306)
        .await
        .expect("mysql port");
    let url = format!("mysql://root@127.0.0.1:{port}/test");
    (container, url)
}

/// Create a single-column `events` table and insert `n` rows of `(id)` with
/// values `1..=n`. Uses a recursive CTE for a fast bulk insert — avoids
/// per-row round trips.
async fn seed_events(url: &str, n: i64) {
    use sqlx::Connection;

    let mut conn = sqlx::MySqlConnection::connect(url)
        .await
        .expect("connect for seed");
    sqlx::query("CREATE TABLE events (id BIGINT PRIMARY KEY)")
        .execute(&mut conn)
        .await
        .expect("create table");
    // MySQL 8 supports recursive CTEs; bump cte_max_recursion_depth so large
    // n works. The SET SESSION must run on the same connection as the
    // INSERT, hence the explicit single-connection seeding.
    sqlx::query("SET SESSION cte_max_recursion_depth = 1000000")
        .execute(&mut conn)
        .await
        .expect("set cte depth");
    sqlx::query(
        "INSERT INTO events (id) \
         WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < ?) \
         SELECT n FROM seq",
    )
    .bind(n)
    .execute(&mut conn)
    .await
    .expect("insert rows");
    conn.close().await.expect("close conn");
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_bounded_memory() {
    let (_container, url) = start_mysql().await;
    seed_events(&url, 5_000).await;

    let config =
        MysqlSourceConfig::new(&url, "SELECT id FROM events ORDER BY id").with_batch_size(250);
    let source = MysqlSource::new(config).await.expect("source new");

    faucet_conformance::assert_bounded_memory(&source, 250, 5_000).await;

    // Check 10: connector_name is non-empty.
    faucet_conformance::assert_connector_name_nonempty(&source);
    assert_eq!(source.connector_name(), "mysql");

    // Check 11: preflight check() returns Ok(report) with well-formed probes.
    faucet_conformance::assert_preflight_check_wellformed(
        &source,
        &faucet_core::check::CheckContext::default(),
    )
    .await;

    // Check 9: batch_size=0 yields the entire result set as a single page. Reuse
    // the same seeded container with a fresh source configured for no batching.
    let single_page = MysqlSource::new(
        MysqlSourceConfig::new(&url, "SELECT id FROM events ORDER BY id").with_batch_size(0),
    )
    .await
    .expect("source new (batch_size=0)");
    faucet_conformance::assert_batch_size_zero_single_page(&single_page).await;
    // _container stays alive to here
}

// ── Check 12: discovery round-trips (Docker) ────────────────────────────────

/// Every dataset `discover()` reports must be genuinely selectable: deep-merge
/// its config_patch onto the base config, rebuild the source, and read it.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_discover_roundtrips() {
    let (_container, url) = start_mysql().await;
    seed_events(&url, 3).await;
    let source = MysqlSource::new(MysqlSourceConfig::new(&url, "SELECT 1"))
        .await
        .expect("source new");
    faucet_conformance::assert_discover_roundtrips(&source, |patch| {
        let url = url.clone();
        async move {
            let base = serde_json::to_value(MysqlSourceConfig::new(&url, "SELECT 1"))
                .expect("serialize base config");
            let merged = faucet_conformance::merge_config_patch(base, &patch);
            let cfg: MysqlSourceConfig =
                serde_json::from_value(merged).expect("deserialize merged config");
            Box::new(MysqlSource::new(cfg).await.expect("rebuilt source"))
                as Box<dyn faucet_core::Source>
        }
    })
    .await;
    // _container stays alive to here
}

// ── Check 6: errors, not panics (Docker) ────────────────────────────────────

/// The source builds against a live container (so `new()` — which eagerly
/// connects the pool — succeeds), but the configured query targets a table that
/// does not exist, so the failure surfaces at read time. The battery asserts
/// both `fetch_all` and `stream_pages` return a typed `FaucetError` rather than
/// panicking.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_errors_not_panics() {
    let (_container, url) = start_mysql().await;

    let config = MysqlSourceConfig::new(url, "SELECT id FROM missing_table");
    let source = MysqlSource::new(config)
        .await
        .expect("builds; query fails at read");

    faucet_conformance::assert_errors_not_panics(&source).await;
    // _container stays alive to here
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_call_deadline() {
    // A query the server sits on stands in for a hung backend (#880).
    let (_container, url) = start_mysql().await;
    let mut built = Vec::new();
    for _ in 0..2 {
        built.push(
            MysqlSource::new(MysqlSourceConfig::new(&url, "SELECT SLEEP(60) AS s"))
                .await
                .expect("source new"),
        );
    }
    let built = std::sync::Mutex::new(built);
    faucet_conformance::calls::assert_source_call_deadline(|| built.lock().unwrap().pop().unwrap())
        .await;
}

// ── Check 22: the first page arrives before the result set is exhausted ──────
// A full scan of `events` counts every row the server reads in the reading
// connection's `Handler_read_*` status, which `performance_schema` exposes to
// another connection while the statement is still sending rows.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_first_page_before_exhaustion() {
    let (_container, url) = start_mysql().await;
    let total = 50_000;
    let batch = 100;
    seed_events(&url, total as i64).await;
    let probe = sqlx::MySqlPool::connect(&url).await.expect("probe pool");
    let source = MysqlSource::new(
        MysqlSourceConfig::new(&url, "SELECT id, REPEAT('x', 200) AS pad FROM events")
            .with_batch_size(batch),
    )
    .await
    .expect("source new");
    let served = faucet_conformance::served_probe(|| async {
        sqlx::query_scalar::<_, String>(
            "SELECT CAST(COALESCE(SUM(CAST(VARIABLE_VALUE AS UNSIGNED)), 0) AS CHAR) \
             FROM performance_schema.status_by_thread \
             WHERE VARIABLE_NAME LIKE 'Handler_read%' \
               AND THREAD_ID <> PS_CURRENT_THREAD_ID()",
        )
        .fetch_one(&probe)
        .await
        .expect("read handler status")
        .parse::<usize>()
        .expect("a count")
    });
    faucet_conformance::assert_first_page_before_exhaustion(&source, batch, total, &served).await;
    // The counter is live: once the stream is drained it has seen every row.
    assert!(
        served() >= total,
        "the served-row counter missed rows: {}",
        served()
    );
}

/// Check 25: the declaration names every emitted column with its type.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_declared_columns_truthful() {
    let (_container, url) = start_mysql().await;
    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool connect");
    for sql in [
        "CREATE TABLE typed (id INT NOT NULL, amount DECIMAL(12,2), day DATE, \
         at TIMESTAMP NULL, local DATETIME(3), t TIME, note VARCHAR(20), flag BOOLEAN, \
         f DOUBLE, j JSON, b VARBINARY(4))",
        "INSERT INTO typed VALUES (1, 12.50, '2024-10-27', '2024-02-22 02:00:00', \
         '2024-02-22 02:00:00.5', '10:11:12', 'x', true, 1.5, '{\"a\": 1}', 0x0102), \
         (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
    ] {
        sqlx::query(sql).execute(&pool).await.expect("seed");
    }
    pool.close().await;
    let source = MysqlSource::new(MysqlSourceConfig::new(&url, "SELECT * FROM typed"))
        .await
        .expect("source");
    faucet_conformance::assert_declared_columns_truthful(&source).await;
}
