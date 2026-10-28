//! `faucet-conformance` battery for the Microsoft SQL Server source.
//!
//! Check 1 (config-schema validity) is pure and offline. Check 2
//! (bounded-memory streaming) boots a real SQL Server via testcontainers and
//! so requires Docker — it runs in CI alongside the other integration tests.

use faucet_common_mssql::{MssqlConnectionConfig, MssqlTls, MssqlTlsMode, build_pool};
use faucet_conformance::assert_config_schema_valid_value;
use faucet_source_mssql::{MssqlReplication, MssqlSource, MssqlSourceConfig};
use serde_json::Value;
use testcontainers_modules::mssql_server::MssqlServer;
use testcontainers_modules::testcontainers::ContainerAsync;

// ── Check 1: config schema ──────────────────────────────────────────────────

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(MssqlSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-mssql");
}

// ── Check 2: bounded-memory streaming (Docker) ──────────────────────────────

/// `yourStrong(!)Password` percent-encoded for a URL userinfo segment.
const ENCODED_PW: &str = "yourStrong%28%21%29Password";

// SQL Server containers need ~2 GB RAM each. `cargo test` runs a binary's tests
// in parallel; serialize them so at most one container runs at a time and the
// CI runner doesn't run out of memory.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn start_mssql() -> (ContainerAsync<MssqlServer>, u16) {
    let container =
        faucet_conformance::containers::start(|| MssqlServer::default().with_accept_eula()).await;
    let port = container
        .get_host_port_ipv4(1433)
        .await
        .expect("mssql host port");
    (container, port)
}

fn conn_cfg(port: u16) -> MssqlConnectionConfig {
    MssqlConnectionConfig {
        connection_url: Some(format!("mssql://sa:{ENCODED_PW}@127.0.0.1:{port}/master")),
        connection_string: None,
        tls: MssqlTls {
            mode: MssqlTlsMode::TrustServerCertificate,
            ca_cert_path: None,
        },
    }
}

async fn exec(pool: &faucet_common_mssql::MssqlPool, sql: &str) {
    let mut conn = pool.get().await.expect("checkout");
    conn.execute(sql, &[]).await.expect("execute setup sql");
}

/// Create a single-column `events` table and insert `n` rows of `(id)` with
/// values `1..=n`, using multi-row VALUES batches to keep round trips down.
async fn seed_events(pool: &faucet_common_mssql::MssqlPool, n: i64) {
    exec(pool, "CREATE TABLE dbo.events (id BIGINT)").await;
    for chunk in (1..=n).collect::<Vec<_>>().chunks(1000) {
        let values: Vec<String> = chunk.iter().map(|i| format!("({i})")).collect();
        exec(
            pool,
            &format!("INSERT INTO dbo.events (id) VALUES {}", values.join(", ")),
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_bounded_memory() {
    let _serial = SERIAL.lock().await;
    let (_container, port) = start_mssql().await;
    let cfg = conn_cfg(port);
    let pool = build_pool(&cfg, 4).await.expect("pool");

    seed_events(&pool, 5_000).await;

    let mut scfg = MssqlSourceConfig::new(
        cfg.connection_url.clone().unwrap(),
        "SELECT id FROM dbo.events ORDER BY id",
    );
    scfg.connection.tls = cfg.tls.clone();
    scfg.batch_size = 250;
    let source = MssqlSource::new(scfg).await.expect("source new");

    faucet_conformance::assert_bounded_memory(&source, 250, 5_000).await;

    // Check 10: connector_name is non-empty.
    faucet_conformance::assert_connector_name_nonempty(&source);

    // Check 11: preflight check() returns Ok(report) with well-formed probes.
    faucet_conformance::assert_preflight_check_wellformed(
        &source,
        &faucet_core::check::CheckContext::default(),
    )
    .await;

    // Check 9: batch_size=0 yields the entire result set as a single page. Reuse
    // the same seeded container with a fresh source configured for no batching.
    let mut scfg0 = MssqlSourceConfig::new(
        cfg.connection_url.clone().unwrap(),
        "SELECT id FROM dbo.events ORDER BY id",
    );
    scfg0.connection.tls = cfg.tls.clone();
    scfg0.batch_size = 0;
    let single_page = MssqlSource::new(scfg0)
        .await
        .expect("source new (batch_size=0)");
    faucet_conformance::assert_batch_size_zero_single_page(&single_page).await;
    // _container stays alive to here
}

// ── Check 3: bookmark round-trip (Docker) ───────────────────────────────────

/// Seed an `events` table with an `updated_at` cursor column, configure
/// `replication: incremental` on it (`>=` cursor read, boundary rows
/// deduplicated by fingerprint), then let the battery
/// drive the source to completion, capture its bookmark, re-apply it, seed one
/// strictly-newer row, and re-drive — asserting the resumed run replays strictly
/// fewer records than the first (i.e. the bookmark is honored).
#[tokio::test(flavor = "multi_thread")]
async fn conformance_bookmark_roundtrip() {
    let _serial = SERIAL.lock().await;
    let (_container, port) = start_mssql().await;
    let cfg = conn_cfg(port);
    let pool = build_pool(&cfg, 4).await.expect("pool");

    exec(
        &pool,
        "CREATE TABLE dbo.events (id INT, updated_at NVARCHAR(30))",
    )
    .await;
    for (id, ts) in [(1, "2024-01-01"), (2, "2024-02-01"), (3, "2024-03-01")] {
        exec(
            &pool,
            &format!("INSERT INTO dbo.events (id, updated_at) VALUES ({id}, '{ts}')"),
        )
        .await;
    }

    let mut scfg = MssqlSourceConfig::new(
        cfg.connection_url.clone().unwrap(),
        "SELECT id, updated_at FROM dbo.events WHERE updated_at >= @bookmark",
    );
    scfg.connection.tls = cfg.tls.clone();
    scfg.replication = MssqlReplication::Incremental {
        column: "updated_at".into(),
        initial_value: Value::from("2024-01-15"),
    };
    let source = MssqlSource::new(scfg).await.expect("source new");

    // The battery drains the source once (capturing the max `updated_at` as the
    // bookmark), applies it, then drains again. Seed a newer row *before* the
    // battery runs so the resumed run has strictly fewer rows to emit than the
    // first — the battery re-drives the same source in place.
    exec(
        &pool,
        "INSERT INTO dbo.events (id, updated_at) VALUES (4, '2024-04-01')",
    )
    .await;

    faucet_conformance::assert_bookmark_roundtrip(&source).await;
    // _container stays alive to here
}

// ── Check 12: discovery round-trips (Docker) ────────────────────────────────

/// Every dataset `discover()` reports must be genuinely selectable: take its
/// config_patch (`{"query": …}`), rebuild the source pointed at that query, and
/// read it.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_discover_roundtrips() {
    let _serial = SERIAL.lock().await;
    let (_container, port) = start_mssql().await;
    let cfg = conn_cfg(port);
    let pool = build_pool(&cfg, 4).await.expect("pool");
    seed_events(&pool, 3).await;

    let url = cfg.connection_url.clone().unwrap();
    let tls = cfg.tls.clone();
    let mut base = MssqlSourceConfig::new(url.clone(), "SELECT 1");
    base.connection.tls = tls.clone();
    let source = MssqlSource::new(base).await.expect("source new");

    faucet_conformance::assert_discover_roundtrips(&source, |patch| {
        let url = url.clone();
        let tls = tls.clone();
        async move {
            let query = patch["query"].as_str().expect("query patch").to_string();
            let mut cfg = MssqlSourceConfig::new(url, query);
            cfg.connection.tls = tls;
            Box::new(MssqlSource::new(cfg).await.expect("rebuilt source"))
                as Box<dyn faucet_core::Source>
        }
    })
    .await;
    // _container stays alive to here
}

// ── Check 6: errors, not panics (Docker) ────────────────────────────────────

/// The source builds against a live container (so `new()` — which eagerly
/// builds the connection pool — succeeds), but the configured query is invalid,
/// so the failure surfaces at read time. The battery asserts both `fetch_all`
/// and `stream_pages` return a typed `FaucetError` rather than panicking.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_errors_not_panics() {
    let _serial = SERIAL.lock().await;
    let (_container, port) = start_mssql().await;
    let cfg = conn_cfg(port);

    let mut scfg = MssqlSourceConfig::new(
        cfg.connection_url.clone().unwrap(),
        "SELECT * FROM dbo.missing_table",
    );
    scfg.connection.tls = cfg.tls.clone();
    let source = MssqlSource::new(scfg)
        .await
        .expect("builds; query fails at read");

    faucet_conformance::assert_errors_not_panics(&source).await;
    // _container stays alive to here
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_call_deadline() {
    // A statement the server sits on stands in for a hung backend (#880).
    let _serial = SERIAL.lock().await;
    let (_container, port) = start_mssql().await;
    let cfg = conn_cfg(port);
    let mut built = Vec::new();
    for _ in 0..2 {
        let mut scfg = MssqlSourceConfig::new(
            cfg.connection_url.clone().unwrap(),
            "WAITFOR DELAY '00:01:00'; SELECT 1 AS x",
        );
        scfg.connection.tls = cfg.tls.clone();
        built.push(MssqlSource::new(scfg).await.expect("source"));
    }
    let built = std::sync::Mutex::new(built);
    faucet_conformance::calls::assert_source_call_deadline(|| built.lock().unwrap().pop().unwrap())
        .await;
}

// ── Check 22: the first page arrives before the result set is exhausted ──────
// `sys.dm_exec_requests.row_count` is the number of rows a running request
// has returned to its client; once the request has finished it is gone, which
// counts as the whole result served. CI-only: SQL Server containers do not run
// on every development host.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_first_page_before_exhaustion() {
    let _serial = SERIAL.lock().await;
    let (_container, port) = start_mssql().await;
    let cfg = conn_cfg(port);
    let probe = build_pool(&cfg, 2).await.expect("probe pool");
    let total = 50_000;
    let batch = 100;
    let mut scfg = MssqlSourceConfig::new(
        cfg.connection_url.clone().unwrap(),
        format!(
            "SELECT TOP ({total}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS id, \
             REPLICATE('x', 200) AS pad \
             FROM sys.all_objects a CROSS JOIN sys.all_objects b /* faucet-check-22 */"
        ),
    );
    scfg.connection.tls = cfg.tls.clone();
    scfg.batch_size = batch;
    let source = MssqlSource::new(scfg).await.expect("source new");
    let served = faucet_conformance::served_probe(|| async {
        let mut conn = probe.get().await.expect("checkout");
        let row = conn
            .query(
                "SELECT CAST(COALESCE(SUM(r.row_count), 0) AS BIGINT), CAST(COUNT(*) AS BIGINT) \
                 FROM sys.dm_exec_requests r CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t \
                 WHERE r.session_id <> @@SPID AND t.text LIKE '%faucet-check-22%'",
                &[],
            )
            .await
            .expect("probe query")
            .into_row()
            .await
            .expect("probe row")
            .expect("one row");
        let returned: i64 = row.get(0).expect("row_count");
        let running: i64 = row.get(1).expect("count");
        if running == 0 {
            total
        } else {
            returned as usize
        }
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
    let _serial = SERIAL.lock().await;
    let (_container, port) = start_mssql().await;
    let cfg = conn_cfg(port);
    let pool = build_pool(&cfg, 4).await.expect("pool");
    exec(
        &pool,
        "CREATE TABLE dbo.typed (id INT NOT NULL, amount DECIMAL(12,2), day DATE, \
         at DATETIMEOFFSET, local DATETIME2, t TIME, note NVARCHAR(20), flag BIT, \
         f FLOAT, uid UNIQUEIDENTIFIER, m MONEY, b VARBINARY(4))",
    )
    .await;
    exec(
        &pool,
        "INSERT INTO dbo.typed VALUES (1, 12.50, '2024-10-27', '2024-02-22T02:00:00+00:00', \
         '2024-02-22T02:00:00.5', '10:11:12', N'x', 1, 1.5, \
         '123E4567-E89B-12D3-A456-426614174000', 3.25, 0x0102), \
         (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
    )
    .await;
    let mut scfg = MssqlSourceConfig::new(
        cfg.connection_url.clone().unwrap(),
        "SELECT * FROM dbo.typed",
    );
    scfg.connection.tls = cfg.tls.clone();
    let source = MssqlSource::new(scfg).await.expect("source new");
    faucet_conformance::assert_declared_columns_truthful(&source).await;
}
