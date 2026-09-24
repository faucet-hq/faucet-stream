//! Integration tests for `MssqlSink` upsert / delete write modes against a real
//! Microsoft SQL Server in Docker (`mcr.microsoft.com/mssql/server`).
//!
//! Run with `cargo test -p faucet-sink-mssql --test upsert`. Each test boots a
//! container (serialized via `SERIAL` — SQL Server needs ~2 GB RAM each) and
//! creates `CREATE TABLE dbo.t (id INT PRIMARY KEY, name NVARCHAR(255))`.

use faucet_common_mssql::{MssqlConnectionConfig, MssqlPool, MssqlTls, MssqlTlsMode, build_pool};
use faucet_core::{Sink, WriteMode, WriteSpec};
use faucet_sink_mssql::{MssqlColumnMapping, MssqlSink, MssqlSinkConfig, OnUnknownField};
use serde_json::json;
use testcontainers_modules::mssql_server::MssqlServer;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const ENCODED_PW: &str = "yourStrong%28%21%29Password";

// SQL Server containers need ~2 GB RAM each. `cargo test` runs a binary's tests
// in parallel, so without this guard they would all start a container at once
// and exhaust the runner. Serialize: at most one container at a time.
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

async fn count(pool: &MssqlPool, table: &str) -> i32 {
    let mut conn = pool.get().await.expect("checkout");
    let rows = conn
        .query(format!("SELECT COUNT(*) AS c FROM {table}"), &[])
        .await
        .expect("count query")
        .into_first_result()
        .await
        .expect("count result");
    rows[0].get::<i32, _>("c").expect("count value")
}

async fn name_of(pool: &MssqlPool, table: &str, id: i32) -> String {
    let mut conn = pool.get().await.expect("checkout");
    let rows = conn
        .query(format!("SELECT name FROM {table} WHERE id = @P1"), &[&id])
        .await
        .expect("name query")
        .into_first_result()
        .await
        .expect("name result");
    rows[0]
        .get::<&str, _>("name")
        .expect("name value")
        .to_string()
}

/// Build an upsert sink targeting `dbo.t` with `auto_columns` mapping.
fn upsert_sink_cfg(cfg: &MssqlConnectionConfig, write: WriteSpec) -> MssqlSinkConfig {
    let mut s = MssqlSinkConfig::new(cfg.connection_url.clone().unwrap(), "dbo.t");
    s.connection.tls = cfg.tls.clone();
    s.column_mapping = MssqlColumnMapping::AutoColumns {
        on_unknown_field: OnUnknownField::Warn,
    };
    s.write = write;
    s
}

// ---------------------------------------------------------------------------
// Test 1: second upsert with the same key updates the row (last-write-wins)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn upsert_second_write_updates_existing_row() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_mssql().await;
    let cfg = conn_cfg(port);
    let pool = build_pool(&cfg, 4).await.expect("pool");
    exec(
        &pool,
        "CREATE TABLE dbo.t (id INT PRIMARY KEY, name NVARCHAR(255))",
    )
    .await;

    let scfg = upsert_sink_cfg(
        &cfg,
        WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["id".to_string()],
            delete_marker: None,
        },
    );
    let sink = MssqlSink::new(scfg).await.expect("sink");

    sink.write_batch(&[json!({"id": 1, "name": "alice"})])
        .await
        .expect("first write");
    sink.write_batch(&[json!({"id": 1, "name": "alice2"})])
        .await
        .expect("second write");

    assert_eq!(count(&pool, "dbo.t").await, 1, "upsert must not duplicate");
    assert_eq!(name_of(&pool, "dbo.t", 1).await, "alice2", "name updated");
}

// ---------------------------------------------------------------------------
// Test 2: delete_marker routes a row to delete; the table ends up empty
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn upsert_with_delete_marker_removes_row() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_mssql().await;
    let cfg = conn_cfg(port);
    let pool = build_pool(&cfg, 4).await.expect("pool");
    exec(
        &pool,
        "CREATE TABLE dbo.t (id INT PRIMARY KEY, name NVARCHAR(255))",
    )
    .await;

    let scfg = upsert_sink_cfg(
        &cfg,
        WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["id".to_string()],
            delete_marker: Some(faucet_core::DeleteMarker {
                field: "__op".to_string(),
                values: vec!["d".to_string()],
            }),
        },
    );
    let sink = MssqlSink::new(scfg).await.expect("sink");

    // Insert the row first (marker field stripped from the upsert).
    sink.write_batch(&[json!({"id": 1, "name": "x", "__op": "u"})])
        .await
        .expect("insert");
    assert_eq!(count(&pool, "dbo.t").await, 1, "row present after upsert");

    // Delete it via the marker.
    sink.write_batch(&[json!({"id": 1, "__op": "d"})])
        .await
        .expect("delete");
    assert_eq!(count(&pool, "dbo.t").await, 0, "row deleted via marker");
}

// ---------------------------------------------------------------------------
// Test 3: same key twice in one batch → last-write-wins (count 1, name "new")
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn upsert_same_key_twice_in_one_batch_last_write_wins() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_mssql().await;
    let cfg = conn_cfg(port);
    let pool = build_pool(&cfg, 4).await.expect("pool");
    exec(
        &pool,
        "CREATE TABLE dbo.t (id INT PRIMARY KEY, name NVARCHAR(255))",
    )
    .await;

    let scfg = upsert_sink_cfg(
        &cfg,
        WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["id".to_string()],
            delete_marker: None,
        },
    );
    let sink = MssqlSink::new(scfg).await.expect("sink");

    // Two records with the same id in one batch — the planner dedups so the
    // MERGE source never double-hits the same key (MERGE rejects that).
    sink.write_batch(&[
        json!({"id": 1, "name": "old"}),
        json!({"id": 1, "name": "new"}),
    ])
    .await
    .expect("batch write");

    assert_eq!(count(&pool, "dbo.t").await, 1, "dedup collapses to one row");
    assert_eq!(name_of(&pool, "dbo.t", 1).await, "new", "last-write-wins");
}

// ---------------------------------------------------------------------------
// Test 4: supported_write_modes advertises Append, Upsert, Delete
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn supported_write_modes_includes_upsert_and_delete() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_mssql().await;
    let cfg = conn_cfg(port);

    let mut scfg = MssqlSinkConfig::new(cfg.connection_url.clone().unwrap(), "dbo.t");
    scfg.connection.tls = cfg.tls.clone();
    let sink = MssqlSink::new(scfg).await.expect("sink");

    let modes = sink.supported_write_modes();
    assert!(modes.contains(&WriteMode::Append));
    assert!(modes.contains(&WriteMode::Upsert));
    assert!(modes.contains(&WriteMode::Delete));
}

// ---------------------------------------------------------------------------
// Test 5: config validation rejects upsert without a key (no container needed —
//         the error fires before the connection attempt).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn new_rejects_upsert_without_key() {
    let mut config = MssqlSinkConfig::new("mssql://sa:pw@127.0.0.1:11433/master", "dbo.t");
    config.column_mapping = MssqlColumnMapping::AutoColumns {
        on_unknown_field: OnUnknownField::Warn,
    };
    config.write = WriteSpec {
        write_mode: WriteMode::Upsert,
        key: vec![], // missing key → rejected before any connection attempt
        delete_marker: None,
    };

    let err = MssqlSink::new(config)
        .await
        .err()
        .expect("must fail without key");
    assert!(err.to_string().contains("non-empty `key`"), "got: {err}");
}

// ---------------------------------------------------------------------------
// Test 6: config validation rejects upsert with json_column mapping (no
//         container needed; fires before the connection attempt).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn new_rejects_upsert_with_json_column_mapping() {
    // Default column_mapping is json_column — upsert must be rejected.
    let mut config = MssqlSinkConfig::new("mssql://sa:pw@127.0.0.1:11433/master", "dbo.t");
    config.write = WriteSpec {
        write_mode: WriteMode::Upsert,
        key: vec!["id".to_string()],
        delete_marker: None,
    };

    let err = MssqlSink::new(config)
        .await
        .err()
        .expect("must fail with json_column mapping");
    assert!(
        err.to_string().contains("auto_columns"),
        "error must mention auto_columns; got: {err}"
    );
}

// ---------------------------------------------------------------------------
// Test 7: write_batch_partial routes missing-key rows to the DLQ per-row.
// The good row is applied (upsert); only the missing-key row comes back Err.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn write_batch_partial_routes_missing_key_per_row() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_mssql().await;
    let cfg = conn_cfg(port);
    let pool = build_pool(&cfg, 4).await.expect("pool");
    exec(
        &pool,
        "CREATE TABLE dbo.t (id INT PRIMARY KEY, name NVARCHAR(255))",
    )
    .await;

    let scfg = upsert_sink_cfg(
        &cfg,
        WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["id".to_string()],
            delete_marker: None,
        },
    );
    let sink = MssqlSink::new(scfg).await.expect("sink");

    let records = [
        json!({"id": 1, "name": "ok"}),
        json!({"name": "missing-id"}),
    ];
    let outcomes = sink
        .write_batch_partial(&records)
        .await
        .expect("partial write");

    assert_eq!(outcomes.len(), 2, "one outcome per input row");
    assert!(outcomes[0].is_ok(), "the good row must be Ok");
    assert!(
        outcomes[1].is_err(),
        "the missing-key row must be Err (routed to the DLQ)"
    );

    assert_eq!(
        count(&pool, "dbo.t").await,
        1,
        "only the good row is written"
    );
    assert_eq!(name_of(&pool, "dbo.t", 1).await, "ok", "id=1 → name 'ok'");
}

#[tokio::test(flavor = "multi_thread")]
async fn upsert_on_a_fresh_database_creates_a_keyed_table_and_dedups() {
    // #676: auto_columns creates dbo.t with PRIMARY KEY (id) on the first page.
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_mssql().await;
    let cfg = conn_cfg(port);
    let pool = build_pool(&cfg, 4).await.expect("pool");
    let write = || WriteSpec {
        write_mode: WriteMode::Upsert,
        key: vec!["id".to_string()],
        delete_marker: None,
    };
    let first = MssqlSink::new(upsert_sink_cfg(&cfg, write()))
        .await
        .expect("sink");
    first
        .write_batch(&[json!({"id": 1, "name": "a"}), json!({"id": 2, "name": "b"})])
        .await
        .expect("first write");
    let second = MssqlSink::new(upsert_sink_cfg(&cfg, write()))
        .await
        .expect("sink");
    second
        .write_batch(&[
            json!({"id": 1, "name": "a2"}),
            json!({"id": 3, "name": "c"}),
        ])
        .await
        .expect("second write");
    assert_eq!(count(&pool, "dbo.t").await, 3);
    assert_eq!(name_of(&pool, "dbo.t", 1).await, "a2");
}
