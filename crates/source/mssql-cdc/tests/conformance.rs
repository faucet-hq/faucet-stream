//! `faucet-conformance` battery for the SQL Server CDC source.
//!
//! Check 1: the connector's config JSON Schema is a well-formed schema value
//!          (pure, offline — always runs).
//! Check 2: `stream_pages` bounds peak memory. Like postgres-cdc / mysql-cdc,
//!          this source emits **one page per committed transaction**; a single
//!          transaction is never split across pages. We seed many *small,
//!          single-row* autocommitted `INSERT`s — each its own transaction and
//!          hence its own one-record page — so the peak page (1) is ≤ the batch
//!          cap and strictly < the total.
//! Check 3: bookmark round-trip. Because CDC capture is asynchronous (the
//!          capture job scans the log into the change tables), the changes are
//!          pre-seeded and drained with `start_position: earliest`, so both
//!          drives run over an already-populated, static change set.
//!
//! `assert_errors_not_panics` is intentionally omitted: this source runs an
//! eager preflight in `new()` (verifying CDC + capture instances), so the
//! obvious read-error causes surface at *build* time, not read time — there is
//! no clean "builds but fails at first read" state to exercise.
//!
//! The Docker checks require SQL Server with the Agent enabled (so the capture
//! job populates the change tables). They are `#[ignore]`d — the ~2 GB SQL Server
//! image is too heavy/slow to come up reliably on the shared CI runner — so the
//! general `cargo test` run skips them (only the offline schema check runs). Run
//! them explicitly with `cargo test -p faucet-source-mssql-cdc -- --ignored`.
//! The offline `conformance_config_schema_valid` check always runs.

use std::time::Duration;

use faucet_common_mssql::{MssqlConnectionConfig, MssqlPool, MssqlTls, MssqlTlsMode, build_pool};
use faucet_conformance::containers::{self, Endpoint, ReadyProbe, StartOptions};
use faucet_conformance::{
    assert_bookmark_roundtrip, assert_bounded_memory, assert_config_schema_valid_value,
    assert_connector_name_nonempty, assert_preflight_check_wellformed,
};
use faucet_source_mssql_cdc::{MssqlCdcSource, MssqlCdcSourceConfig};
use serde_json::json;
use testcontainers::ImageExt;
use testcontainers_modules::mssql_server::MssqlServer;
use testcontainers_modules::testcontainers::ContainerAsync;

const ENCODED_PW: &str = "yourStrong%28%21%29Password";
const RAW_PW: &str = "yourStrong(!)Password";
const CAPTURE_INSTANCE: &str = "dbo_events";
const BATCH: usize = 250;
const TOTAL: usize = 600;

// SQL Server containers need ~2 GB RAM each; serialize them.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

// ── Check 1: config schema validity (offline) ────────────────────────────────

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(MssqlCdcSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-mssql-cdc");
}

// ── Docker helpers ───────────────────────────────────────────────────────────

/// Start a SQL Server container with CDC (Agent) enabled once it accepts TDS
/// connections: the container is reported started well before the engine
/// does. `None` (skip) when Docker is unavailable.
async fn start_mssql_cdc() -> Option<(ContainerAsync<MssqlServer>, u16)> {
    let opts = StartOptions::default()
        .ready(ReadyProbe::custom(1433, |ep: Endpoint| async move {
            let pool = build_pool(&conn_cfg(ep.port, "master"), 1)
                .await
                .map_err(|e| e.to_string())?;
            let mut conn = pool.get().await.map_err(|e| e.to_string())?;
            conn.execute("SELECT 1", &[])
                .await
                .map(drop)
                .map_err(|e| e.to_string())
        }))
        .ready_timeout(Duration::from_secs(180));
    let container = containers::start_or_skip(
        || {
            MssqlServer::default()
                .with_accept_eula()
                // SQL Server Agent runs the CDC capture job.
                .with_env_var("MSSQL_AGENT_ENABLED", "true")
        },
        &opts,
    )
    .await?;
    let port = container
        .get_host_port_ipv4(1433)
        .await
        .expect("mssql host port");
    Some((container, port))
}

fn conn_cfg(port: u16, database: &str) -> MssqlConnectionConfig {
    MssqlConnectionConfig {
        connection_url: Some(format!(
            "mssql://sa:{ENCODED_PW}@127.0.0.1:{port}/{database}"
        )),
        connection_string: None,
        tls: MssqlTls {
            mode: MssqlTlsMode::TrustServerCertificate,
            ca_cert_path: None,
        },
    }
}

async fn exec(pool: &MssqlPool, sql: &str) {
    let mut conn = pool.get().await.expect("checkout");
    conn.execute(sql, &[]).await.expect("execute setup sql");
}

async fn scalar_i64(pool: &MssqlPool, sql: &str) -> i64 {
    let mut conn = pool.get().await.expect("checkout");
    let rows = conn
        .query(sql, &[])
        .await
        .expect("query")
        .into_first_result()
        .await
        .expect("result");
    let Some(row) = rows.first() else { return 0 };
    // tiberius is width-strict on `try_get`: the readiness `CASE … THEN 0 ELSE 1`
    // comes back as INT (i32) while `CONVERT(BIGINT, COUNT(*))` comes back as
    // BIGINT (i64). Accept either width so a probe never silently reads 0.
    if let Ok(Some(v)) = row.try_get::<i64, _>(0) {
        return v;
    }
    row.try_get::<i32, _>(0)
        .ok()
        .flatten()
        .map(i64::from)
        .unwrap_or(0)
}

/// Enable CDC on a fresh database, create `dbo.events`, enable a capture
/// instance, insert `n` single-row transactions, and wait for the capture job
/// to populate the change table. Returns the master-scoped pool (unused) and the
/// per-database connection config.
async fn setup_cdc(port: u16, n: usize) -> MssqlConnectionConfig {
    let db = "cdc_conformance";
    let master = build_pool(&conn_cfg(port, "master"), 2)
        .await
        .expect("master pool");
    exec(
        &master,
        &format!("IF DB_ID('{db}') IS NULL CREATE DATABASE {db}"),
    )
    .await;

    let dbcfg = conn_cfg(port, db);
    let pool = build_pool(&dbcfg, 4).await.expect("db pool");

    exec(
        &pool,
        "IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()) = 0 \
                 EXEC sys.sp_cdc_enable_db",
    )
    .await;
    exec(
        &pool,
        "IF OBJECT_ID('dbo.events') IS NULL CREATE TABLE dbo.events (id INT PRIMARY KEY)",
    )
    .await;
    exec(
        &pool,
        "IF NOT EXISTS (SELECT 1 FROM cdc.change_tables ct \
             JOIN sys.objects o ON o.object_id = ct.source_object_id WHERE o.name = 'events') \
         EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'events', \
             @role_name = NULL, @capture_instance = N'dbo_events'",
    )
    .await;

    // Give the capture job a moment to come up before writing.
    tokio::time::sleep(Duration::from_secs(2)).await;
    for i in 0..n {
        exec(&pool, &format!("INSERT INTO dbo.events (id) VALUES ({i})")).await;
    }

    // Wait for the async capture to move all n rows into the change table.
    let count_sql = format!(
        "SELECT CONVERT(BIGINT, COUNT(*)) FROM cdc.fn_cdc_get_all_changes_{CAPTURE_INSTANCE}(\
             sys.fn_cdc_get_min_lsn(N'{CAPTURE_INSTANCE}'), sys.fn_cdc_get_max_lsn(), N'all')"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    loop {
        // fn_cdc_get_min_lsn is NULL until the first scan; guard with a max check.
        let max_ready = scalar_i64(
            &pool,
            "SELECT CASE WHEN sys.fn_cdc_get_min_lsn(N'dbo_events') IS NULL THEN 0 ELSE 1 END",
        )
        .await;
        if max_ready == 1 && scalar_i64(&pool, &count_sql).await >= n as i64 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "CDC capture did not populate {n} change rows within the timeout — is the SQL Server \
             Agent running?"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    let _ = RAW_PW;
    dbcfg
}

fn build_config(conn: &MssqlConnectionConfig) -> MssqlCdcSourceConfig {
    serde_json::from_value(json!({
        "connection_url": conn.connection_url,
        "tls": { "type": "trust_server_certificate" },
        "capture_instances": [CAPTURE_INSTANCE],
        "start_position": { "type": "earliest" },
        "idle_timeout": 5,
        "poll_interval": 1,
        "batch_size": BATCH,
    }))
    .expect("config")
}

// ── Check 2: bounded-memory streaming (Docker) ───────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a SQL Server (mssql) container: the ~2 GB image is too heavy/slow \
            to come up reliably on the shared CI runner (unlike the lighter \
            Postgres/MySQL/ClickHouse ones). Run explicitly with \
            `cargo test -p faucet-source-mssql-cdc -- --ignored`."]
async fn conformance_bounded_memory() {
    let _serial = SERIAL.lock().await;
    let Some((_c, port)) = start_mssql_cdc().await else {
        return;
    };
    let conn = setup_cdc(port, TOTAL).await;

    let source = MssqlCdcSource::new(build_config(&conn))
        .await
        .expect("source new");

    // Check 10: connector_name is non-empty (pure — no I/O).
    assert_connector_name_nonempty(&source);
    // Check 11: preflight check() is well-formed against the live server. The
    // CDC check() is overridden to run a read-only connect + CDC-status +
    // capture-instance probe (never a page pull), so it is safe in the live test.
    assert_preflight_check_wellformed(&source, &faucet_core::check::CheckContext::default()).await;

    // Each committed single-row transaction is its own page (1 record) → peak
    // == 1, which is ≤ BATCH and < TOTAL.
    assert_bounded_memory(&source, BATCH, TOTAL).await;
}

// ── Check 3: bookmark round-trip (Docker) ────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a SQL Server (mssql) container: the ~2 GB image is too heavy/slow \
            to come up reliably on the shared CI runner (unlike the lighter \
            Postgres/MySQL/ClickHouse ones). Run explicitly with \
            `cargo test -p faucet-source-mssql-cdc -- --ignored`."]
async fn conformance_bookmark_roundtrip() {
    let _serial = SERIAL.lock().await;
    const N: usize = 300;
    let Some((_c, port)) = start_mssql_cdc().await else {
        return;
    };
    let conn = setup_cdc(port, N).await;

    let source = MssqlCdcSource::new(build_config(&conn))
        .await
        .expect("source new");

    // First drain consumes all N pre-seeded change rows and emits an LSN
    // bookmark; no new writes occur, so the resumed drain yields zero.
    assert_bookmark_roundtrip(&source).await;
}
