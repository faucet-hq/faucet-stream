//! Integration tests for `MssqlSink` `write_mode: overwrite` (#492) against a
//! real Microsoft SQL Server in Docker.
//!
//! Run with `cargo test -p faucet-sink-mssql --test overwrite`. Each test boots
//! a container (serialized via `SERIAL`). The lifecycle the pipeline drives is
//! `begin_overwrite` → N × `write_batch` → `commit_overwrite` (success) or
//! `abort_overwrite` (failure). The guarantees under test: writes stage until
//! the swap, a successful commit replaces the whole destination, and an abort
//! leaves the previous rows completely intact.

use faucet_common_mssql::{MssqlConnectionConfig, MssqlPool, MssqlTls, MssqlTlsMode, build_pool};
use faucet_core::{Sink, WriteMode, WriteSpec};
use faucet_sink_mssql::{MssqlColumnMapping, MssqlSink, MssqlSinkConfig, OnUnknownField};
use serde_json::json;
use testcontainers_modules::mssql_server::MssqlServer;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const ENCODED_PW: &str = "yourStrong%28%21%29Password";

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn start_mssql() -> (ContainerAsync<MssqlServer>, u16) {
    let container = MssqlServer::default()
        .with_accept_eula()
        .start()
        .await
        .expect("start mssql container");
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

async fn exec(pool: &MssqlPool, sql: &str) {
    let mut conn = pool.get().await.expect("checkout");
    conn.execute(sql, &[]).await.expect("execute setup sql");
}

async fn names(pool: &MssqlPool, table: &str) -> Vec<String> {
    let mut conn = pool.get().await.expect("checkout");
    let rows = conn
        .query(format!("SELECT name FROM {table} ORDER BY id"), &[])
        .await
        .expect("query")
        .into_first_result()
        .await
        .expect("result");
    rows.iter()
        .map(|r| r.get::<&str, _>("name").unwrap_or_default().to_string())
        .collect()
}

fn overwrite_cfg(cfg: &MssqlConnectionConfig) -> MssqlSinkConfig {
    let mut s = MssqlSinkConfig::new(cfg.connection_url.clone().unwrap(), "dbo.t");
    s.connection.tls = cfg.tls.clone();
    s.column_mapping = MssqlColumnMapping::AutoColumns {
        on_unknown_field: OnUnknownField::Warn,
    };
    s.write = WriteSpec {
        write_mode: WriteMode::Overwrite,
        key: vec![],
        delete_marker: None,
        rollback: None,
    };
    s
}

#[tokio::test(flavor = "multi_thread")]
async fn overwrite_replaces_all_rows_on_commit() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_mssql().await;
    let cfg = conn_cfg(port);
    let pool = build_pool(&cfg, 4).await.expect("pool");
    exec(
        &pool,
        "CREATE TABLE dbo.t (id INT PRIMARY KEY, name NVARCHAR(255))",
    )
    .await;
    exec(
        &pool,
        "INSERT INTO dbo.t (id, name) VALUES (1, 'old_a'), (2, 'old_b')",
    )
    .await;

    let sink = MssqlSink::new(overwrite_cfg(&cfg)).await.expect("sink");
    sink.begin_overwrite().await.expect("begin");
    sink.write_batch(&[json!({"id": 10, "name": "new_x"})])
        .await
        .expect("w1");
    sink.write_batch(&[json!({"id": 11, "name": "new_y"})])
        .await
        .expect("w2");

    // Staged: the destination still holds the OLD rows until the swap.
    assert_eq!(names(&pool, "dbo.t").await, vec!["old_a", "old_b"]);

    sink.commit_overwrite().await.expect("commit");
    assert_eq!(names(&pool, "dbo.t").await, vec!["new_x", "new_y"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn overwrite_abort_leaves_previous_data_intact() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_mssql().await;
    let cfg = conn_cfg(port);
    let pool = build_pool(&cfg, 4).await.expect("pool");
    exec(
        &pool,
        "CREATE TABLE dbo.t (id INT PRIMARY KEY, name NVARCHAR(255))",
    )
    .await;
    exec(
        &pool,
        "INSERT INTO dbo.t (id, name) VALUES (1, 'old_a'), (2, 'old_b')",
    )
    .await;

    let sink = MssqlSink::new(overwrite_cfg(&cfg)).await.expect("sink");
    sink.begin_overwrite().await.expect("begin");
    sink.write_batch(&[json!({"id": 99, "name": "doomed"})])
        .await
        .expect("w");
    sink.abort_overwrite().await.expect("abort");

    assert_eq!(names(&pool, "dbo.t").await, vec!["old_a", "old_b"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn overwrite_advertised_in_capabilities() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_mssql().await;
    let cfg = conn_cfg(port);
    let pool = build_pool(&cfg, 4).await.expect("pool");
    exec(
        &pool,
        "CREATE TABLE dbo.t (id INT PRIMARY KEY, name NVARCHAR(255))",
    )
    .await;

    let sink = MssqlSink::new(overwrite_cfg(&cfg)).await.expect("sink");
    assert!(sink.supported_write_modes().contains(&WriteMode::Overwrite));
    assert!(sink.is_overwrite());
}

async fn target_present(pool: &MssqlPool) -> bool {
    let mut conn = pool.get().await.expect("checkout");
    let rows = conn
        .query("SELECT OBJECT_ID(N'dbo.t', N'U')", &[])
        .await
        .expect("query")
        .into_first_result()
        .await
        .expect("result");
    rows.first()
        .and_then(|r| r.try_get::<i32, _>(0).ok().flatten())
        .is_some()
}

#[tokio::test(flavor = "multi_thread")]
async fn overwrite_on_a_fresh_database_creates_then_swaps() {
    // #676. The CLI runs begin, the writes and the commit on *different* sink
    // instances, so each step gets its own sink.
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_mssql().await;
    let cfg = conn_cfg(port);
    let pool = build_pool(&cfg, 4).await.expect("pool");
    let sink = || MssqlSink::new(overwrite_cfg(&cfg));

    sink()
        .await
        .unwrap()
        .begin_overwrite()
        .await
        .expect("begin");
    sink()
        .await
        .unwrap()
        .write_batch(&[json!({"id": 1, "name": "a"}), json!({"id": 2, "name": "b"})])
        .await
        .expect("w1");
    assert!(
        !target_present(&pool).await,
        "a first run stages; the target appears at commit"
    );
    sink()
        .await
        .unwrap()
        .commit_overwrite()
        .await
        .expect("commit");
    assert_eq!(names(&pool, "dbo.t").await, vec!["a", "b"]);

    sink()
        .await
        .unwrap()
        .begin_overwrite()
        .await
        .expect("begin");
    sink()
        .await
        .unwrap()
        .write_batch(&[json!({"id": 3, "name": "c"})])
        .await
        .expect("w2");
    assert_eq!(
        names(&pool, "dbo.t").await,
        vec!["a", "b"],
        "second run is staged"
    );
    sink()
        .await
        .unwrap()
        .commit_overwrite()
        .await
        .expect("commit");
    assert_eq!(names(&pool, "dbo.t").await, vec!["c"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn aborted_first_overwrite_leaves_no_table_behind() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_mssql().await;
    let cfg = conn_cfg(port);
    let pool = build_pool(&cfg, 4).await.expect("pool");
    let sink = || MssqlSink::new(overwrite_cfg(&cfg));
    sink()
        .await
        .unwrap()
        .begin_overwrite()
        .await
        .expect("begin");
    sink()
        .await
        .unwrap()
        .write_batch(&[json!({"id": 1, "name": "a"})])
        .await
        .expect("w1");
    sink()
        .await
        .unwrap()
        .abort_overwrite()
        .await
        .expect("abort");
    assert!(!target_present(&pool).await);
}
