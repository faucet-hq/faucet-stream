//! Integration tests for `MssqlCdcSource` against a real SQL Server in Docker.
//!
//! Requires Docker (the `mcr.microsoft.com/mssql/server` image) with SQL Server
//! Agent enabled so the CDC capture job populates the change tables. These are
//! `#[ignore]`d: the ~2 GB SQL Server image is too heavy/slow to come up
//! reliably on the shared CI runner (unlike the lighter Postgres/MySQL/ClickHouse
//! containers), so the general `cargo test` run skips them. Run them explicitly
//! against a real SQL Server with
//! `cargo test -p faucet-source-mssql-cdc -- --ignored`.
//!
//! Change capture is asynchronous, so every test seeds its writes, waits for the
//! capture job to move them into the change table, and then drains the source
//! with `start_position: earliest` (a static, fully-populated change set).

use std::time::Duration;

use faucet_common_mssql::{MssqlConnectionConfig, MssqlPool, MssqlTls, MssqlTlsMode, build_pool};
use faucet_core::Source;
use faucet_core::check::{CheckContext, ProbeStatus};
use faucet_source_mssql_cdc::{MssqlCdcSource, MssqlCdcSourceConfig};
use futures::StreamExt;
use serde_json::{Value, json};
use std::collections::HashMap;
use testcontainers::ImageExt;
use testcontainers_modules::mssql_server::MssqlServer;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const ENCODED_PW: &str = "yourStrong%28%21%29Password";
const CI: &str = "dbo_users";

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Start a SQL Server container with CDC (Agent) enabled and wait until it
/// actually accepts TDS connections. Returns `None` — so the caller skips the
/// test rather than failing — when Docker is unavailable or the image can't come
/// up. SQL Server boots far more slowly than Postgres/MySQL: the container is
/// reported "started" well before the engine accepts connections, so a plain
/// checkout right after start races the boot and fails. Poll until ready.
/// A missing test backend: a skip locally, a failure when CI requires the
/// backends (`FAUCET_REQUIRE_BACKENDS`).
fn backend_missing(why: &str) {
    if std::env::var("FAUCET_REQUIRE_BACKENDS").is_ok() {
        panic!("{why} (FAUCET_REQUIRE_BACKENDS is set)");
    }
    eprintln!("skipping: {why}");
}

async fn start_mssql_cdc() -> Option<(ContainerAsync<MssqlServer>, u16)> {
    let container = match MssqlServer::default()
        .with_accept_eula()
        .with_env_var("MSSQL_AGENT_ENABLED", "true")
        .start()
        .await
    {
        Ok(c) => c,
        Err(e) => {
            backend_missing(&format!(
                "mssql-cdc test: could not start SQL Server container: {e}"
            ));
            return None;
        }
    };
    let port = container
        .get_host_port_ipv4(1433)
        .await
        .expect("mssql host port");
    if !wait_until_ready(port).await {
        backend_missing("mssql-cdc test: SQL Server never accepted connections in time");
        return None;
    }
    Some((container, port))
}

/// Poll a `master` checkout until SQL Server accepts connections, or give up
/// after a generous deadline (returns `false`).
async fn wait_until_ready(port: u16) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_secs(180);
    loop {
        if let Ok(pool) = build_pool(&conn_cfg(port, "master"), 1).await
            && let Ok(mut conn) = pool.get().await
            && conn.execute("SELECT 1", &[]).await.is_ok()
        {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
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

/// Create `db`, enable CDC on it and on `dbo.users`, returning the db pool +
/// config. `dbo.users(id INT PK, name NVARCHAR(64))`.
async fn setup(port: u16, db: &str) -> (MssqlPool, MssqlConnectionConfig) {
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
        "IF OBJECT_ID('dbo.users') IS NULL \
                 CREATE TABLE dbo.users (id INT PRIMARY KEY, name NVARCHAR(64))",
    )
    .await;
    exec(
        &pool,
        "IF NOT EXISTS (SELECT 1 FROM cdc.change_tables ct \
             JOIN sys.objects o ON o.object_id = ct.source_object_id WHERE o.name = 'users') \
         EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'users', \
             @role_name = NULL, @capture_instance = N'dbo_users'",
    )
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    (pool, dbcfg)
}

/// Wait until the change table holds at least `n` rows.
async fn wait_for_changes(pool: &MssqlPool, n: i64) {
    let count_sql = format!(
        "SELECT CONVERT(BIGINT, COUNT(*)) FROM cdc.fn_cdc_get_all_changes_{CI}(\
             sys.fn_cdc_get_min_lsn(N'{CI}'), sys.fn_cdc_get_max_lsn(), N'all')"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    loop {
        let ready = scalar_i64(
            pool,
            "SELECT CASE WHEN sys.fn_cdc_get_min_lsn(N'dbo_users') IS NULL THEN 0 ELSE 1 END",
        )
        .await;
        if ready == 1 && scalar_i64(pool, &count_sql).await >= n {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "CDC capture did not populate {n} rows within the timeout (is SQL Agent running?)"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

fn build_config(conn: &MssqlConnectionConfig) -> MssqlCdcSourceConfig {
    serde_json::from_value(json!({
        "connection_url": conn.connection_url,
        "tls": { "type": "trust_server_certificate" },
        "capture_instances": [CI],
        "start_position": { "type": "earliest" },
        "idle_timeout": 5,
        "poll_interval": 1,
        "batch_size": 0,
    }))
    .expect("config")
}

async fn drain(source: &MssqlCdcSource) -> (Vec<Value>, Option<Value>) {
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 0);
    let mut records = Vec::new();
    let mut bookmark = None;
    while let Some(page) = pages.next().await {
        let page = page.expect("page");
        records.extend(page.records);
        if page.bookmark.is_some() {
            bookmark = page.bookmark;
        }
    }
    (records, bookmark)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a SQL Server (mssql) container: the ~2 GB image is too heavy/slow \
            to come up reliably on the shared CI runner (unlike the lighter \
            Postgres/MySQL/ClickHouse ones). Run explicitly with \
            `cargo test -p faucet-source-mssql-cdc -- --ignored`."]
async fn cdc_captures_crud_then_resumes_without_replay() {
    let _serial = SERIAL.lock().await;
    let Some((_c, port)) = start_mssql_cdc().await else {
        return;
    };
    let (pool, conn) = setup(port, "cdc_crud").await;

    exec(
        &pool,
        "INSERT INTO dbo.users (id, name) VALUES (1, N'alice')",
    )
    .await;
    exec(&pool, "UPDATE dbo.users SET name = N'bob' WHERE id = 1").await;
    exec(&pool, "DELETE FROM dbo.users WHERE id = 1").await;
    // insert(op2) + update-after(op4) + delete(op1) = 3 change rows.
    wait_for_changes(&pool, 3).await;

    let source = MssqlCdcSource::new(build_config(&conn))
        .await
        .expect("source new");
    let (records, bookmark) = drain(&source).await;

    let ops: Vec<&str> = records
        .iter()
        .map(|r| r["op"].as_str().unwrap_or(""))
        .collect();
    assert!(ops.contains(&"i"), "expected an insert op, got {ops:?}");
    assert!(ops.contains(&"u"), "expected an update op, got {ops:?}");
    assert!(ops.contains(&"d"), "expected a delete op, got {ops:?}");

    let insert = records
        .iter()
        .find(|r| r["op"] == "i")
        .expect("insert record");
    assert_eq!(insert["schema"], "dbo");
    assert_eq!(insert["table"], "users");
    assert_eq!(insert["after"]["name"], "alice", "envelope: {insert:?}");
    assert_eq!(insert["before"], Value::Null);
    assert!(insert["lsn"].is_string(), "lsn present: {insert:?}");

    let delete = records
        .iter()
        .find(|r| r["op"] == "d")
        .expect("delete record");
    assert_eq!(
        delete["before"]["id"],
        json!(1),
        "delete before image: {delete:?}"
    );
    assert_eq!(delete["after"], Value::Null);

    let bookmark = bookmark.expect("cycle 1 must produce a bookmark");
    assert!(
        bookmark.get(CI).and_then(Value::as_str).is_some(),
        "bookmark map: {bookmark}"
    );

    // Resume: a new insert (id=2) must be the only thing delivered.
    source
        .apply_start_bookmark(bookmark)
        .await
        .expect("apply bookmark");
    exec(
        &pool,
        "INSERT INTO dbo.users (id, name) VALUES (2, N'carol')",
    )
    .await;
    wait_for_changes(&pool, 4).await;

    let (records2, _) = drain(&source).await;
    assert!(!records2.is_empty(), "expected the post-bookmark insert");
    for r in &records2 {
        assert_eq!(
            r["after"]["id"],
            json!(2),
            "resume replayed a pre-bookmark event: {r:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a SQL Server (mssql) container: the ~2 GB image is too heavy/slow \
            to come up reliably on the shared CI runner (unlike the lighter \
            Postgres/MySQL/ClickHouse ones). Run explicitly with \
            `cargo test -p faucet-source-mssql-cdc -- --ignored`."]
async fn source_metadata_and_check_probes_pass() {
    let _serial = SERIAL.lock().await;
    let Some((_c, port)) = start_mssql_cdc().await else {
        return;
    };
    let (_pool, conn) = setup(port, "cdc_meta").await;

    let source = MssqlCdcSource::new(build_config(&conn))
        .await
        .expect("source new");

    assert_eq!(source.connector_name(), "mssql-cdc");
    assert!(source.supports_exactly_once());
    assert_eq!(
        source.state_key().as_deref(),
        Some("mssql-cdc:cdc_meta:dbo_users")
    );

    let uri = source.dataset_uri();
    assert!(
        !uri.contains("sa:") && !uri.contains(ENCODED_PW),
        "credentials stripped: {uri}"
    );
    assert!(
        uri.ends_with("?capture_instances=dbo_users"),
        "dataset_uri: {uri}"
    );

    assert!(source.config_schema().get("properties").is_some());

    // capture_resume_position anchors at the current max LSN (Some once CDC is
    // active). Enable one write first so a max LSN exists.
    exec(&_pool, "INSERT INTO dbo.users (id, name) VALUES (9, N'x')").await;
    wait_for_changes(&_pool, 1).await;
    let pos = source.capture_resume_position().await.expect("capture");
    let pos = pos.expect("mssql-cdc must capture a position once CDC is active");
    assert!(
        pos.get(CI).and_then(Value::as_str).is_some(),
        "resume position map: {pos}"
    );

    let report = source
        .check(&CheckContext::default())
        .await
        .expect("check report");
    assert_eq!(report.failed_count(), 0, "all probes must pass: {report:?}");
    let names: Vec<&str> = report.probes.iter().map(|p| p.name).collect();
    assert!(names.contains(&"connection"));
    assert!(names.contains(&"cdc-enabled"));
    assert!(names.contains(&"capture-instances"));
    for probe in &report.probes {
        assert!(
            matches!(probe.status, ProbeStatus::Pass),
            "probe {} must pass",
            probe.name
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a SQL Server (mssql) container: the ~2 GB image is too heavy/slow \
            to come up reliably on the shared CI runner (unlike the lighter \
            Postgres/MySQL/ClickHouse ones). Run explicitly with \
            `cargo test -p faucet-source-mssql-cdc -- --ignored`."]
async fn new_rejects_missing_capture_instance() {
    let _serial = SERIAL.lock().await;
    let Some((_c, port)) = start_mssql_cdc().await else {
        return;
    };
    let (_pool, conn) = setup(port, "cdc_missing").await;

    let config: MssqlCdcSourceConfig = serde_json::from_value(json!({
        "connection_url": conn.connection_url,
        "tls": { "type": "trust_server_certificate" },
        "capture_instances": ["dbo_does_not_exist"],
    }))
    .expect("config");
    let msg = match MssqlCdcSource::new(config).await {
        Ok(_) => panic!("new() must reject a nonexistent capture instance"),
        Err(e) => e.to_string(),
    };
    assert!(
        msg.contains("dbo_does_not_exist"),
        "error must name the instance: {msg}"
    );
    assert!(
        msg.contains("sp_cdc_enable_table"),
        "error must hint the fix: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a SQL Server (mssql) container: the ~2 GB image is too heavy/slow \
            to come up reliably on the shared CI runner (unlike the lighter \
            Postgres/MySQL/ClickHouse ones). Run explicitly with \
            `cargo test -p faucet-source-mssql-cdc -- --ignored`."]
async fn new_rejects_cdc_disabled_database() {
    let _serial = SERIAL.lock().await;
    let Some((_c, port)) = start_mssql_cdc().await else {
        return;
    };
    // A database that never had `sp_cdc_enable_db` run.
    let master = build_pool(&conn_cfg(port, "master"), 2)
        .await
        .expect("master pool");
    exec(
        &master,
        "IF DB_ID('cdc_off') IS NULL CREATE DATABASE cdc_off",
    )
    .await;
    let conn = conn_cfg(port, "cdc_off");

    let config: MssqlCdcSourceConfig = serde_json::from_value(json!({
        "connection_url": conn.connection_url,
        "tls": { "type": "trust_server_certificate" },
        "capture_instances": ["dbo_anything"],
    }))
    .expect("config");
    let msg = match MssqlCdcSource::new(config).await {
        Ok(_) => panic!("new() must reject a CDC-disabled database"),
        Err(e) => e.to_string(),
    };
    assert!(
        msg.contains("change data capture is not enabled"),
        "msg: {msg}"
    );
    assert!(
        msg.contains("sp_cdc_enable_db"),
        "error must hint the fix: {msg}"
    );
}

/// #733 — `lag()` counts the change transactions behind the consumed LSN and
/// the age of the oldest; none before a position is known, zero once drained.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a SQL Server (mssql) container: the ~2 GB image is too heavy/slow \
            to come up reliably on the shared CI runner. Run explicitly with \
            `cargo test -p faucet-source-mssql-cdc -- --ignored`."]
async fn lag_counts_unread_change_transactions() {
    let _serial = SERIAL.lock().await;
    let Some((_c, port)) = start_mssql_cdc().await else {
        return;
    };
    let (pool, conn) = setup(port, "cdc_lag").await;
    exec(&pool, "INSERT INTO dbo.users (id, name) VALUES (1, N'a')").await;
    wait_for_changes(&pool, 1).await;

    let source = MssqlCdcSource::new(build_config(&conn))
        .await
        .expect("source new");
    assert_eq!(source.lag().await.expect("lag"), None);
    let (_, bookmark) = drain(&source).await;
    let drained = source.lag().await.expect("lag").expect("a reading");
    assert_eq!(drained.events, Some(0));

    exec(&pool, "INSERT INTO dbo.users (id, name) VALUES (2, N'b')").await;
    exec(&pool, "INSERT INTO dbo.users (id, name) VALUES (3, N'c')").await;
    wait_for_changes(&pool, 3).await;
    let probe = MssqlCdcSource::new(build_config(&conn))
        .await
        .expect("source new");
    probe
        .apply_start_bookmark(bookmark.expect("bookmark"))
        .await
        .expect("apply");
    let behind = probe.lag().await.expect("lag").expect("a reading");
    assert_eq!(behind.events, Some(2), "{behind:?}");
    assert!(behind.seconds.unwrap() >= 0.0);
}

/// SQL-26: a bookmark older than the retained minimum LSN fails the run by
/// default instead of skipping the purged changes with a warning.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a SQL Server (mssql) container: the ~2 GB image is too heavy/slow \
            to come up reliably on the shared CI runner. Run explicitly with \
            `cargo test -p faucet-source-mssql-cdc -- --ignored`."]
async fn a_bookmark_behind_the_retained_history_fails_unless_skipped() {
    let _serial = SERIAL.lock().await;
    let Some((_c, port)) = start_mssql_cdc().await else {
        return;
    };
    let (pool, conn) = setup(port, "cdc_gap").await;
    exec(
        &pool,
        "INSERT INTO dbo.users (id, name) VALUES (1, N'alice')",
    )
    .await;
    wait_for_changes(&pool, 1).await;

    let purged = json!({ CI: "00000000000000000001" });
    let source = MssqlCdcSource::new(build_config(&conn))
        .await
        .expect("source new");
    source.apply_start_bookmark(purged.clone()).await.unwrap();
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 0);
    let mut failure = None;
    while let Some(page) = pages.next().await {
        if let Err(e) = page {
            failure = Some(e.to_string());
            break;
        }
    }
    let failure = failure.expect("the gap must fail the run");
    assert!(failure.contains("on_gap: skip"), "{failure}");

    let mut skip = build_config(&conn);
    skip.on_gap = faucet_source_mssql_cdc::OnGap::Skip;
    let source = MssqlCdcSource::new(skip).await.expect("source new");
    source.apply_start_bookmark(purged).await.unwrap();
    let (records, _) = drain(&source).await;
    assert!(!records.is_empty(), "skip resumes from the earliest change");
}

/// SQL-55: a transaction touching two captured tables is delivered as one
/// page, in its own order, with one bookmark covering both instances.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a SQL Server (mssql) container; run with `--ignored`."]
async fn a_transaction_across_two_tables_is_one_page_in_order() {
    let _serial = SERIAL.lock().await;
    let Some((_c, port)) = start_mssql_cdc().await else {
        return;
    };
    let (pool, conn) = setup(port, "cdc_multi").await;
    exec(
        &pool,
        "IF OBJECT_ID('dbo.orders') IS NULL \
           CREATE TABLE dbo.orders (id INT PRIMARY KEY, user_id INT)",
    )
    .await;
    exec(
        &pool,
        "IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE capture_instance = 'dbo_orders') \
         EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'orders', \
             @role_name = NULL, @capture_instance = N'dbo_orders'",
    )
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    exec(
        &pool,
        "BEGIN TRAN; INSERT INTO dbo.orders (id, user_id) VALUES (10, 7); \
         INSERT INTO dbo.users (id, name) VALUES (7, N'parent'); COMMIT",
    )
    .await;
    wait_for_changes(&pool, 1).await;
    tokio::time::sleep(Duration::from_secs(5)).await;

    let mut cfg = build_config(&conn);
    cfg.capture_instances = vec![CI.to_string(), "dbo_orders".to_string()];
    let source = MssqlCdcSource::new(cfg).await.expect("source new");
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 1000);
    let mut data_pages = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.expect("page");
        if !page.records.is_empty() {
            data_pages.push(page);
        }
    }
    assert_eq!(data_pages.len(), 1, "one transaction, one page");
    let tables: Vec<&str> = data_pages[0]
        .records
        .iter()
        .map(|r| r["table"].as_str().unwrap())
        .collect();
    assert_eq!(tables, vec!["orders", "users"], "commit order is kept");
    let bm = data_pages[0].bookmark.clone().expect("bookmark");
    assert_eq!(
        bm[CI], bm["dbo_orders"],
        "both instances resume after the commit"
    );
}
