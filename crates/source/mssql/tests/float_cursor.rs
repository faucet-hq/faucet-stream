//! A `>=` cursor on a REAL column keeps the rows at the bookmark value
//! (#1114): `0.7` as a REAL is `0.699999988…`, below its decimal.

use faucet_common_mssql::{MssqlConnectionConfig, MssqlTls, MssqlTlsMode, build_pool};
use faucet_core::Source;
use faucet_source_mssql::{MssqlReplication, MssqlSource, MssqlSourceConfig};
use serde_json::{Value, json};
use testcontainers_modules::mssql_server::MssqlServer;

const ENCODED_PW: &str = "yourStrong%28%21%29Password";

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn exec(pool: &faucet_common_mssql::MssqlPool, sql: &str) {
    let mut conn = pool.get().await.expect("checkout");
    conn.execute(sql, &[]).await.expect(sql);
}

async fn read(cfg: MssqlSourceConfig, bookmark: Option<&Value>) -> (Vec<i64>, Value) {
    let source = MssqlSource::new(cfg).await.expect("source");
    if let Some(b) = bookmark {
        source
            .apply_start_bookmark(b.clone())
            .await
            .expect("bookmark");
    }
    let (rows, last) = source.fetch_all_incremental().await.expect("read");
    let mut ids: Vec<i64> = rows.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    ids.sort_unstable();
    (ids, last.unwrap_or(Value::Null))
}

async fn rows_at_the_bookmark_are_kept(table: &str, column_type: &str, query: &str) {
    let _serial = SERIAL.lock().await;
    let container =
        faucet_conformance::containers::start(|| MssqlServer::default().with_accept_eula()).await;
    let port = container.get_host_port_ipv4(1433).await.expect("port");
    let conn = MssqlConnectionConfig {
        connection_url: Some(format!("mssql://sa:{ENCODED_PW}@127.0.0.1:{port}/master")),
        connection_string: None,
        tls: MssqlTls {
            mode: MssqlTlsMode::TrustServerCertificate,
            ca_cert_path: None,
        },
    };
    let pool = build_pool(&conn, 2).await.expect("pool");
    exec(
        &pool,
        &format!("CREATE TABLE dbo.{table} (id INT, c {column_type})"),
    )
    .await;
    exec(
        &pool,
        &format!("INSERT INTO dbo.{table} VALUES (1, 0.5), (2, 0.7)"),
    )
    .await;

    let mut cfg = MssqlSourceConfig::new(conn.connection_url.clone().unwrap(), query);
    cfg.connection.tls = conn.tls.clone();
    cfg.replication = MssqlReplication::Incremental {
        column: "c".into(),
        initial_value: json!(0.0),
    };

    let (first, bookmark) = read(cfg.clone(), None).await;
    assert_eq!(first, vec![1, 2]);
    assert_eq!(bookmark["value"], json!(0.7), "{bookmark}");

    exec(
        &pool,
        &format!("INSERT INTO dbo.{table} VALUES (3, 0.7), (4, 0.9)"),
    )
    .await;
    let (second, _) = read(cfg, Some(&bookmark)).await;
    assert_eq!(
        second,
        vec![3, 4],
        "{column_type}: rows committed at the bookmark value"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn real_cursor_keeps_rows_at_the_bookmark() {
    rows_at_the_bookmark_are_kept("r", "REAL", "SELECT id, c FROM dbo.r").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn real_cursor_keeps_rows_at_a_user_bookmark() {
    rows_at_the_bookmark_are_kept(
        "ru",
        "REAL",
        "SELECT id, c FROM dbo.ru WHERE c >= @bookmark",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn float_cursor_keeps_rows_at_the_bookmark() {
    rows_at_the_bookmark_are_kept("f", "FLOAT", "SELECT id, c FROM dbo.f").await;
}
