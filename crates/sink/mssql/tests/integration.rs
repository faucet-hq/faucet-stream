//! Integration tests against a real Microsoft SQL Server in Docker.
//!
//! Requires Docker (the `mcr.microsoft.com/mssql/server` image). Run with:
//! `cargo test -p faucet-sink-mssql --test integration`.

use faucet_common_mssql::{MssqlConnectionConfig, MssqlPool, MssqlTls, MssqlTlsMode, build_pool};
use faucet_core::Sink;
use faucet_sink_mssql::{MssqlColumnMapping, MssqlSink, MssqlSinkConfig};
use serde_json::{Value, json};
use testcontainers_modules::mssql_server::MssqlServer;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const ENCODED_PW: &str = "yourStrong%28%21%29Password";

// SQL Server containers need ~2 GB RAM each. `cargo test` runs a binary's tests
// in parallel, so without this guard all three would start a container at once
// and exhaust the CI runner. Serialize them: at most one container at a time.
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

fn sink_cfg(cfg: &MssqlConnectionConfig, table: &str) -> MssqlSinkConfig {
    let mut s = MssqlSinkConfig::new(cfg.connection_url.clone().unwrap(), table);
    s.connection.tls = cfg.tls.clone();
    s
}

#[tokio::test(flavor = "multi_thread")]
async fn auto_columns_bulk_write_splits_param_limit() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_mssql().await;
    let cfg = conn_cfg(port);
    let pool = build_pool(&cfg, 4).await.expect("pool");

    exec(
        &pool,
        "CREATE TABLE dbo.bulk_rows (id INT, name NVARCHAR(50))",
    )
    .await;

    let mut scfg = sink_cfg(&cfg, "dbo.bulk_rows");
    scfg.column_mapping = MssqlColumnMapping::AutoColumns {
        on_unknown_field: faucet_sink_mssql::OnUnknownField::Warn,
    };
    // batch_size 0 forces the whole 5000-row page through one write_batch, so
    // the 2100-parameter auto-split (5000 rows * 2 cols = 10000 params) is hit.
    scfg.batch_size = 0;
    let sink = MssqlSink::new(scfg).await.expect("sink");

    let records: Vec<Value> = (1..=5000)
        .map(|i| json!({"id": i, "name": format!("user-{i}")}))
        .collect();
    let written = sink.write_batch(&records).await.expect("write");
    assert_eq!(written, 5000);
    assert_eq!(count(&pool, "dbo.bulk_rows").await, 5000);
}

#[tokio::test(flavor = "multi_thread")]
async fn row_isolation_routes_only_the_bad_row() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_mssql().await;
    let cfg = conn_cfg(port);
    let pool = build_pool(&cfg, 4).await.expect("pool");

    exec(
        &pool,
        "CREATE TABLE dbo.strict (id INT NOT NULL, n INT NOT NULL)",
    )
    .await;

    let mut scfg = sink_cfg(&cfg, "dbo.strict");
    scfg.column_mapping = MssqlColumnMapping::AutoColumns {
        on_unknown_field: faucet_sink_mssql::OnUnknownField::Warn,
    };
    let sink = MssqlSink::new(scfg).await.expect("sink");

    // Row 3 is missing `n` -> binds NULL -> violates NOT NULL.
    let records = vec![
        json!({"id": 1, "n": 10}),
        json!({"id": 2, "n": 20}),
        json!({"id": 3}),
    ];
    let outcomes = sink
        .write_batch_partial(&records)
        .await
        .expect("partial write");
    assert_eq!(outcomes.len(), 3);
    assert!(outcomes[0].is_ok(), "row 1 ok");
    assert!(outcomes[1].is_ok(), "row 2 ok");
    assert!(outcomes[2].is_err(), "row 3 (NULL into NOT NULL) -> DLQ");

    // Only the two good rows persisted.
    assert_eq!(count(&pool, "dbo.strict").await, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn json_column_with_create_table() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_mssql().await;
    let cfg = conn_cfg(port);
    let pool = build_pool(&cfg, 4).await.expect("pool");

    let mut scfg = sink_cfg(&cfg, "dbo.payloads");
    scfg.column_mapping = MssqlColumnMapping::JsonColumn {
        column: "body".into(),
    };
    scfg.create_table = true;
    // new() should create the table (id IDENTITY + body NVARCHAR(MAX)).
    let sink = MssqlSink::new(scfg).await.expect("sink creates table");

    let written = sink
        .write_batch(&[json!({"a": 1, "nested": {"x": true}}), json!({"b": 2})])
        .await
        .expect("write");
    assert_eq!(written, 2);
    assert_eq!(count(&pool, "dbo.payloads").await, 2);

    // The body column holds the serialized JSON.
    let mut conn = pool.get().await.unwrap();
    let rows = conn
        .query("SELECT body FROM dbo.payloads ORDER BY id", &[])
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();
    let first: &str = rows[0].get("body").unwrap();
    let parsed: Value = serde_json::from_str(first).unwrap();
    assert_eq!(parsed["a"], json!(1));
    assert_eq!(parsed["nested"]["x"], json!(true));
}

fn auto_cfg(cfg: &MssqlConnectionConfig, table: &str) -> MssqlSinkConfig {
    let mut s = sink_cfg(cfg, table);
    s.column_mapping = MssqlColumnMapping::AutoColumns {
        on_unknown_field: faucet_sink_mssql::OnUnknownField::Warn,
    };
    s
}

/// SQL-38 / SQL-39: a NULL (or a missing key) binds into date/time,
/// uniqueidentifier and varbinary columns, and one chunk may mix NULLs,
/// strings and numbers in a text column — on append and on upsert.
#[tokio::test(flavor = "multi_thread")]
async fn nulls_and_mixed_values_bind_with_each_columns_type() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_mssql().await;
    let cfg = conn_cfg(port);
    let pool = build_pool(&cfg, 4).await.expect("pool");

    exec(
        &pool,
        "CREATE TABLE dbo.typed (id INT NOT NULL PRIMARY KEY, note NVARCHAR(50) NULL, \
         deleted_at DATETIME2 NULL, ref UNIQUEIDENTIFIER NULL, blob VARBINARY(16) NULL, \
         day DATE NULL)",
    )
    .await;
    let rows: Vec<Value> = vec![
        json!({"id": 1, "note": "a", "deleted_at": null, "ref": null, "blob": null, "day": "2024-01-02"}),
        json!({"id": 2, "note": null, "deleted_at": "2024-01-02T03:04:05", "day": null}),
        json!({"id": 3, "note": 7, "ref": "6F9619FF-8B86-D011-B42D-00C04FC964FF"}),
    ];
    let sink = MssqlSink::new(auto_cfg(&cfg, "dbo.typed"))
        .await
        .expect("sink");
    assert_eq!(sink.write_batch(&rows).await.expect("append"), 3);
    assert_eq!(count(&pool, "dbo.typed").await, 3);

    let mut up = auto_cfg(&cfg, "dbo.typed");
    up.write = serde_json::from_value(json!({"write_mode": "upsert", "key": ["id"]})).unwrap();
    let upsert = MssqlSink::new(up).await.expect("upsert sink");
    let changed: Vec<Value> = vec![
        json!({"id": 1, "note": null, "deleted_at": "2024-02-03T00:00:00"}),
        json!({"id": 4, "note": "x", "deleted_at": null, "ref": null}),
        json!({"id": 2, "note": 9}),
    ];
    upsert.write_batch(&changed).await.expect("upsert");
    assert_eq!(count(&pool, "dbo.typed").await, 4);

    let mut conn = pool.get().await.expect("checkout");
    let r = conn
        .query("SELECT note FROM dbo.typed WHERE id = 2", &[])
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();
    assert_eq!(r[0].get::<&str, _>("note"), Some("9"));
}

/// SQL-37: a write that times out (here: blocked by another session's lock)
/// must not leave its transaction open on a pooled connection that a later
/// page then "commits" into.
#[tokio::test(flavor = "multi_thread")]
async fn a_timed_out_write_leaves_no_transaction_for_the_next_page() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_mssql().await;
    let cfg = conn_cfg(port);
    let pool = build_pool(&cfg, 4).await.expect("pool");
    exec(&pool, "CREATE TABLE dbo.locked (id INT NOT NULL)").await;

    let mut s = auto_cfg(&cfg, "dbo.locked");
    s.statement_timeout_secs = 1;
    s.max_connections = 1;
    let sink = MssqlSink::new(s).await.expect("sink");

    // Another session holds an exclusive table lock, so the insert waits.
    let mut blocker = pool.get().await.expect("blocker");
    blocker
        .simple_query("BEGIN TRAN; SELECT * FROM dbo.locked WITH (TABLOCKX, HOLDLOCK)")
        .await
        .expect("lock")
        .into_results()
        .await
        .expect("lock");
    let err = sink
        .write_batch(&[json!({"id": 1})])
        .await
        .expect_err("blocked write times out");
    assert!(err.to_string().contains("timed out"), "{err}");
    blocker
        .simple_query("ROLLBACK TRAN")
        .await
        .expect("release")
        .into_results()
        .await
        .expect("release");
    drop(blocker);

    // The next page runs on the (only) pooled connection and must really commit.
    assert_eq!(
        sink.write_batch(&[json!({"id": 2})]).await.expect("write"),
        1
    );
    drop(sink);

    let mut conn = pool.get().await.expect("checkout");
    conn.simple_query("SET LOCK_TIMEOUT 5000")
        .await
        .unwrap()
        .into_results()
        .await
        .unwrap();
    let rows = conn
        .query("SELECT id FROM dbo.locked ORDER BY id", &[])
        .await
        .expect("read after the sink is gone")
        .into_first_result()
        .await
        .expect("read");
    let ids: Vec<i32> = rows.iter().filter_map(|r| r.get::<i32, _>("id")).collect();
    assert_eq!(ids, vec![2], "the page after the timeout must be committed");
}
