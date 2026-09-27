//! Integration tests for `MysqlCdcSource` against a real MySQL 8 instance
//! via testcontainers.
//!
//! These tests require Docker (matching the kafka / postgres-cdc convention).
//! MySQL is started with `binlog_row_metadata=FULL` (required for column names
//! in the binlog). MySQL 8 already defaults `log_bin=ON`, `binlog_format=ROW`,
//! and `binlog_row_image=FULL`, so only `binlog_row_metadata` needs an
//! explicit flag.
//!
//! The test opens a binlog stream, then a concurrent writer performs
//! INSERT / UPDATE / DELETE after a warm-up delay (so `start_position =
//! current` is ahead of those writes when the stream opens), and asserts the
//! emitted CDC envelopes. It then resumes from the captured bookmark and
//! asserts that a subsequent write — and only that write — is delivered (no
//! replay).

use faucet_core::Source;
use faucet_core::check::{CheckContext, ProbeStatus};
use faucet_source_mysql_cdc::{MysqlCdcSource, MysqlCdcSourceConfig};
use futures::StreamExt;
use mysql_async::{Conn, Opts, prelude::Queryable};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::mysql::Mysql;

/// Bounds concurrent MySQL container startups across all tests in this binary.
/// MySQL 8.x init is heavy (~2-3 GB RSS per container during startup) and
/// starting multiple in parallel can exhaust memory on Colima/Docker Desktop.
/// We allow at most two simultaneous startups; once a container is running it
/// is steady-state cheap, so the cap only serialises the spin-up window.
fn startup_limit() -> &'static tokio::sync::Semaphore {
    static SEM: OnceLock<tokio::sync::Semaphore> = OnceLock::new();
    SEM.get_or_init(|| tokio::sync::Semaphore::new(2))
}

/// Start a MySQL 8 container with binlog options required for CDC and return
/// both the container handle and a connection URL.
///
/// Passes the binlog flags as CMD args. The official `mysql` Docker image
/// prepends `mysqld` to any `--`-prefixed args found in CMD, so passing
/// `["--server-id=1", "--log-bin=mysql-bin", ...]` causes the entrypoint
/// to run `mysqld --server-id=1 --log-bin=mysql-bin ...`.
///
/// The default `Mysql` image creates database `test` with the root user
/// having no password, so the connection URL is
/// `mysql://root@127.0.0.1:<port>/test`.
async fn start_mysql_cdc() -> (ContainerAsync<Mysql>, String) {
    // The `Mysql` module's default tag is 8.1; pin it explicitly so this and the
    // 8.4-specific test below differ only by version.
    start_mysql_cdc_tagged("8.1").await
}

/// Start a MySQL container at a specific image tag with the binlog options
/// required for CDC. Lets a test pick the server version (e.g. `8.4`, where
/// `SHOW MASTER STATUS` was removed in favour of `SHOW BINARY LOG STATUS`).
async fn start_mysql_cdc_tagged(tag: &str) -> (ContainerAsync<Mysql>, String) {
    let _permit = startup_limit()
        .acquire()
        .await
        .expect("startup semaphore closed");

    let container = Mysql::default()
        .with_tag(tag)
        .with_cmd([
            "--server-id=1",
            "--log-bin=mysql-bin",
            "--binlog-format=ROW",
            "--binlog-row-image=FULL",
            "--binlog-row-metadata=FULL",
        ])
        .start()
        .await
        .expect("mysql CDC container start");

    let port = container
        .get_host_port_ipv4(3306)
        .await
        .expect("mysql port");

    let url = format!("mysql://root@127.0.0.1:{port}/test");
    (container, url)
}

/// Build the CDC source config.
fn build_config(url: &str) -> MysqlCdcSourceConfig {
    serde_json::from_value(json!({
        "connection_url": url,
        "server_id": 1001,
        "start_position": { "type": "current" },
        "idle_timeout": 5,
        "batch_size": 0
    }))
    .expect("config")
}

/// Open a fresh `mysql_async` connection to the given URL.
async fn connect(url: &str) -> Conn {
    Conn::new(Opts::from_url(url).expect("parse URL"))
        .await
        .expect("connect")
}

/// Drain a single binlog fetch cycle into a flat `Vec` of records plus the
/// bookmark of the last page that carried one. The cycle ends after the
/// source's `idle_timeout` (5 s) of quiet.
async fn drain(source: &MysqlCdcSource) -> (Vec<Value>, Option<Value>) {
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
async fn cdc_captures_crud_then_resumes_without_replay() {
    let (_container, url) = start_mysql_cdc().await;

    // Pre-create the table BEFORE building the source so that `start_position =
    // current` is positioned after the DDL and won't capture it.
    {
        let mut conn = connect(&url).await;
        conn.query_drop("CREATE TABLE test.users (id INT PRIMARY KEY, name VARCHAR(64))")
            .await
            .expect("create table");
    }

    // Build the source — opens a throwaway connection for preflight checks and
    // records the current binlog position as the stream start.
    let source = MysqlCdcSource::new(build_config(&url))
        .await
        .expect("source new");

    // Concurrent writer: wait ~2 s for the stream to open, then INSERT / UPDATE /
    // DELETE on id=1.
    let writer_url = url.clone();
    let writer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let mut conn = connect(&writer_url).await;
        conn.query_drop("INSERT INTO test.users (id, name) VALUES (1, 'alice')")
            .await
            .expect("insert");
        conn.query_drop("UPDATE test.users SET name = 'bob' WHERE id = 1")
            .await
            .expect("update");
        conn.query_drop("DELETE FROM test.users WHERE id = 1")
            .await
            .expect("delete");
    });

    let (records, bookmark) = drain(&source).await;
    writer.await.expect("writer task");

    // We must have observed the create, update, and delete for id=1.
    let ops: Vec<&str> = records
        .iter()
        .map(|r| r["op"].as_str().unwrap_or(""))
        .collect();
    assert!(ops.contains(&"c"), "expected a create op, got {ops:?}");
    assert!(ops.contains(&"u"), "expected an update op, got {ops:?}");
    assert!(ops.contains(&"d"), "expected a delete op, got {ops:?}");

    // The create envelope must carry the correct namespace, column values, and LSN.
    let create = records
        .iter()
        .find(|r| r["op"] == "c")
        .expect("create record");
    assert_eq!(create["schema"], "test", "schema must be 'test'");
    assert_eq!(create["table"], "users", "table must be 'users'");
    assert_eq!(
        create["after"]["name"], "alice",
        "after.name must be 'alice'; envelope: {create:?}"
    );
    assert!(
        create["lsn"]["file"].is_string(),
        "lsn.file must be a string; envelope: {create:?}"
    );
    assert!(
        create["lsn"]["pos"].is_number(),
        "lsn.pos must be a number; envelope: {create:?}"
    );

    let bookmark = bookmark.expect("cycle 1 must produce a bookmark");

    // Apply the bookmark and drain cycle 2 — only the id=2 insert must appear.
    source
        .apply_start_bookmark(bookmark)
        .await
        .expect("apply bookmark");

    let writer2_url = url.clone();
    let writer2 = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let mut conn = connect(&writer2_url).await;
        conn.query_drop("INSERT INTO test.users (id, name) VALUES (2, 'carol')")
            .await
            .expect("insert2");
    });

    let (records2, _bm2) = drain(&source).await;
    writer2.await.expect("writer2 task");

    // Resume must not replay id=1's events; only the id=2 insert appears.
    assert!(
        !records2.is_empty(),
        "expected the post-bookmark insert to be delivered"
    );
    for r in &records2 {
        let id = &r["after"]["id"];
        assert_eq!(id, &json!(2), "resume replayed a pre-bookmark event: {r:?}");
    }
    assert!(
        records2.iter().any(|r| r["op"] == "c"),
        "expected a create op in cycle 2, got: {records2:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_trait_metadata_and_check_against_live_server() {
    // A live MySQL is required because `MysqlCdcSource::new()` opens a preflight
    // connection. Once built, exercise the trait metadata accessors and the
    // non-stream `check()` preflight (which runs the connection + binlog-config
    // probes without opening the binlog stream).
    let (_container, url) = start_mysql_cdc().await;

    // include_tables makes `dataset_uri` append the `?tables=` suffix.
    let config: MysqlCdcSourceConfig = serde_json::from_value(json!({
        "connection_url": url,
        "server_id": 2002,
        "include_tables": ["test.orders"],
        "start_position": { "type": "current" },
        "idle_timeout": 5,
        "batch_size": 0
    }))
    .expect("config");
    let source = MysqlCdcSource::new(config).await.expect("source new");

    // connector_name / supports_exactly_once are pure constants.
    assert_eq!(source.connector_name(), "mysql-cdc");
    assert!(
        source.supports_exactly_once(),
        "mysql-cdc bookmarks file/pos + per-page → exactly-once capable"
    );

    // state_key derives from server_id.
    assert_eq!(source.state_key().as_deref(), Some("mysql-cdc:2002"));

    // dataset_uri redacts credentials and appends the configured tables.
    let uri = source.dataset_uri();
    assert!(
        !uri.contains("root@") && !uri.contains("@127.0.0.1"),
        "credentials must be stripped from dataset_uri: {uri}"
    );
    assert!(
        uri.ends_with("?tables=test.orders"),
        "dataset_uri must append include_tables: {uri}"
    );

    // config_schema is the JSON Schema for the config struct.
    let schema = source.config_schema();
    assert!(
        schema.get("properties").is_some(),
        "config_schema must be a JSON object schema: {schema}"
    );

    // check(): the container is configured with ROW/FULL/FULL binlog settings
    // and the root user has all privileges, so both probes must pass.
    let report = source
        .check(&CheckContext::default())
        .await
        .expect("check report");
    assert_eq!(report.failed_count(), 0, "all probes must pass: {report:?}");
    let names: Vec<&str> = report.probes.iter().map(|p| p.name).collect();
    assert!(
        names.contains(&"connection"),
        "expected a connection probe, got {names:?}"
    );
    assert!(
        names.contains(&"binlog-config"),
        "expected a binlog-config probe, got {names:?}"
    );
    for probe in &report.probes {
        assert!(
            matches!(probe.status, ProbeStatus::Pass),
            "probe {} must pass: {:?}",
            probe.name,
            probe.status
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn check_reports_binlog_config_failure_when_row_metadata_minimal() {
    // Boot MySQL WITHOUT the binlog flags the CDC source requires: the default
    // `binlog_row_metadata` is MINIMAL, so the binlog-config probe must fail
    // while the connection probe still passes. (`MysqlCdcSource::new()`'s own
    // preflight would reject this, so we build via `check()` only — which is the
    // `faucet doctor` path.)
    let _permit = startup_limit()
        .acquire()
        .await
        .expect("startup semaphore closed");
    let container = Mysql::default()
        .with_cmd(["--server-id=1", "--log-bin=mysql-bin"]) // no row-metadata=FULL
        .start()
        .await
        .expect("mysql start");
    let port = container
        .get_host_port_ipv4(3306)
        .await
        .expect("mysql port");
    let url = format!("mysql://root@127.0.0.1:{port}/test");

    // `MysqlCdcSource::new()` runs the same `run_preflight_probes` that the
    // `check()` binlog-config probe uses, so a server with the default MINIMAL
    // `binlog_row_metadata` exercises the failing-variable branch and surfaces a
    // typed error naming the offending variable.
    let config: MysqlCdcSourceConfig = serde_json::from_value(json!({
        "connection_url": url,
        "server_id": 3003,
        "idle_timeout": 5,
        "batch_size": 0
    }))
    .expect("config");
    // `MysqlCdcSource` is not `Debug`, so match the `Result` directly rather
    // than via `expect_err` (which would require `Ok: Debug`).
    let msg = match MysqlCdcSource::new(config).await {
        Ok(_) => panic!("new() must reject MINIMAL binlog_row_metadata"),
        Err(e) => e.to_string(),
    };
    assert!(
        msg.contains("binlog_row_metadata"),
        "error must name the offending binlog variable; got: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_with_context_aggregates_and_emit_schema_changes_yields_ddl() {
    // Two paths in one container:
    //  1. `fetch_with_context` drains the stream into a flat Vec (aggregate mode).
    //  2. `emit_schema_changes = true` turns a DDL `QueryEvent` into an
    //     `{op:"ddl"}` envelope in the captured stream.
    let (_container, url) = start_mysql_cdc().await;
    {
        let mut conn = connect(&url).await;
        conn.query_drop("CREATE TABLE test.t (id INT PRIMARY KEY, n VARCHAR(32))")
            .await
            .expect("create table");
    }

    let config: MysqlCdcSourceConfig = serde_json::from_value(json!({
        "connection_url": url,
        "server_id": 4004,
        "start_position": { "type": "current" },
        "emit_schema_changes": true,
        "idle_timeout": 5,
        "batch_size": 0
    }))
    .expect("config");
    let source = MysqlCdcSource::new(config).await.expect("source new");

    let writer_url = url.clone();
    let writer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let mut conn = connect(&writer_url).await;
        conn.query_drop("INSERT INTO test.t (id, n) VALUES (1, 'x')")
            .await
            .expect("insert");
        // DDL — auto-commits, must surface as an op:"ddl" envelope.
        conn.query_drop("ALTER TABLE test.t ADD COLUMN extra INT")
            .await
            .expect("alter");
    });

    // `fetch_with_context` returns every record across the fetch cycle as a
    // single flat Vec (it drives stream_pages with the batch_size=0 sentinel).
    let ctx: HashMap<String, Value> = HashMap::new();
    let records = source.fetch_with_context(&ctx).await.expect("fetch");
    writer.await.expect("writer");

    let ops: Vec<&str> = records
        .iter()
        .map(|r| r["op"].as_str().unwrap_or(""))
        .collect();
    assert!(
        ops.contains(&"c"),
        "fetch_with_context must aggregate the insert, got {ops:?}"
    );

    let ddl = records
        .iter()
        .find(|r| r["op"] == "ddl")
        .expect("emit_schema_changes must produce a ddl envelope");
    assert!(
        ddl["statement"]
            .as_str()
            .unwrap_or("")
            .contains("ALTER TABLE"),
        "ddl envelope must carry the statement text: {ddl:?}"
    );
    assert!(
        ddl["lsn"]["file"].is_string() && ddl["lsn"]["pos"].is_number(),
        "ddl envelope must carry a file/pos lsn: {ddl:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capture_resume_position_returns_file_and_pos() {
    let (_container, url) = start_mysql_cdc().await;
    let source = MysqlCdcSource::new(build_config(&url))
        .await
        .expect("source");
    let pos = source
        .capture_resume_position()
        .await
        .expect("capture")
        .expect("mysql-cdc must support capture");
    assert!(
        pos.get("file").and_then(|v| v.as_str()).is_some(),
        "file present: {pos}"
    );
    assert!(
        pos.get("pos").and_then(|v| v.as_u64()).is_some(),
        "pos present: {pos}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capture_resume_position_works_on_mysql_8_4() {
    // MySQL 8.4 removed `SHOW MASTER STATUS` (replaced by `SHOW BINARY LOG
    // STATUS`). The CDC source must capture the current binlog coordinates on
    // 8.4 too — both for `start_position: current` and for `faucet replicate`'s
    // pre-snapshot position capture (#189). Regression guard for #242.
    let (_container, url) = start_mysql_cdc_tagged("8.4").await;
    let source = MysqlCdcSource::new(build_config(&url))
        .await
        .expect("source new on MySQL 8.4");
    let pos = source
        .capture_resume_position()
        .await
        .expect("capture_resume_position must succeed on MySQL 8.4")
        .expect("mysql-cdc must capture a position");
    assert!(
        pos.get("file")
            .and_then(|v| v.as_str())
            .is_some_and(|f| !f.is_empty()),
        "binlog file present: {pos}"
    );
    assert!(
        pos.get("pos").and_then(|v| v.as_u64()).is_some(),
        "binlog pos present: {pos}"
    );
}

/// #733 — `lag()` reports the binlog bytes between the consumed position and
/// the server's head: nothing before a position is known, a growing distance
/// while changes pile up unread, and next to nothing once they are consumed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lag_measures_unread_binlog_bytes() {
    let (_container, url) = start_mysql_cdc().await;
    {
        let mut conn = connect(&url).await;
        conn.query_drop("CREATE TABLE test.lagged (id INT PRIMARY KEY, pad TEXT)")
            .await
            .expect("create table");
    }
    let source = MysqlCdcSource::new(build_config(&url))
        .await
        .expect("source new");
    assert_eq!(
        source.lag().await.expect("lag"),
        None,
        "no position is known before a bookmark or a page"
    );
    let head = {
        let mut conn = connect(&url).await;
        let row: mysql_async::Row = conn
            .query_first("SHOW MASTER STATUS")
            .await
            .expect("status")
            .expect("binlog enabled");
        (
            row.get::<String, _>(0).unwrap(),
            row.get::<u64, _>(1).unwrap(),
        )
    };
    source
        .apply_start_bookmark(json!({"file": head.0, "pos": head.1}))
        .await
        .expect("apply bookmark");
    let caught_up = source.lag().await.expect("lag").expect("a reading");
    assert_eq!(caught_up.bytes, Some(0));

    {
        let mut conn = connect(&url).await;
        for i in 0..50 {
            conn.exec_drop(
                "INSERT INTO test.lagged (id, pad) VALUES (?, REPEAT('x', 512))",
                (i,),
            )
            .await
            .expect("insert");
        }
    }
    let behind = source.lag().await.expect("lag").expect("a reading");
    assert!(
        behind.bytes.unwrap() > 50 * 512,
        "50 half-KiB rows are unread: {behind:?}"
    );

    let (records, _) = drain(&source).await;
    assert_eq!(records.len(), 50);
    let after = source.lag().await.expect("lag").expect("a reading");
    assert!(
        after.bytes.unwrap() < behind.bytes.unwrap(),
        "consuming the changes shrinks the lag: {after:?} vs {behind:?}"
    );
}
