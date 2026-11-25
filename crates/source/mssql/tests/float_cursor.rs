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

async fn server() -> (
    testcontainers_modules::testcontainers::ContainerAsync<MssqlServer>,
    MssqlConnectionConfig,
) {
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
    (container, conn)
}

fn incremental(conn: &MssqlConnectionConfig, query: &str, initial: Value) -> MssqlSourceConfig {
    let mut cfg = MssqlSourceConfig::new(conn.connection_url.clone().unwrap(), query);
    cfg.connection.tls = conn.tls.clone();
    cfg.replication = MssqlReplication::Incremental {
        column: "c".into(),
        initial_value: initial,
    };
    cfg
}

/// Values whose single precision lies above or below their decimal, at the
/// range ends and an integer beyond 2^24, each with two rows tied at the
/// bookmark.
const VALUES: &[&str] = &[
    "0.1",
    "0.3",
    "-0.1",
    "-0.7",
    "7.038531e-26",
    "-7.038531e-26",
    "3.4e38",
    "16777217",
    "1.2e-38",
];

#[tokio::test(flavor = "multi_thread")]
async fn real_cursor_keeps_tied_rows_for_values_on_both_sides_of_their_decimal() {
    let _serial = SERIAL.lock().await;
    let (_c, conn) = server().await;
    let pool = build_pool(&conn, 2).await.expect("pool");
    for (i, v) in VALUES.iter().enumerate() {
        let t = format!("dbo.v{i}");
        exec(&pool, &format!("CREATE TABLE {t} (id INT, c REAL)")).await;
        exec(
            &pool,
            &format!("INSERT INTO {t} VALUES (0, -3e38), (1, {v}), (2, {v})"),
        )
        .await;
        for query in [
            format!("SELECT id, c FROM {t}"),
            format!("SELECT id, c FROM {t} WHERE c >= @bookmark"),
        ] {
            let cfg = incremental(&conn, &query, json!(-3.3e38));
            let (first, bookmark) = read(cfg.clone(), None).await;
            assert_eq!(first, vec![0, 1, 2], "{v}: {query}");
            let (again, _) = read(cfg, Some(&bookmark)).await;
            assert!(again.is_empty(), "{v}: {query}: {again:?}");
        }
        exec(&pool, &format!("INSERT INTO {t} VALUES (3, {v}), (4, {v})")).await;
        let one = incremental(
            &conn,
            &format!("SELECT id, c FROM {t} WHERE id = 1"),
            json!(-3.3e38),
        );
        let value = read(one, None).await.1["value"].clone();
        let cfg = incremental(&conn, &format!("SELECT id, c FROM {t}"), json!(-3.3e38));
        let tied = json!({"value": value, "boundary": []});
        let (second, _) = read(cfg, Some(&tied)).await;
        assert_eq!(
            second,
            vec![1, 2, 3, 4],
            "{v}: every row at the bookmark value"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_exact_spelling_of_a_real_reads_the_rows_at_it() {
    let _serial = SERIAL.lock().await;
    let (_c, conn) = server().await;
    let pool = build_pool(&conn, 2).await.expect("pool");
    exec(&pool, "CREATE TABLE dbo.e (id INT, c REAL)").await;
    exec(
        &pool,
        "INSERT INTO dbo.e VALUES (1, 0.5), (2, 0.7), (3, 0.7)",
    )
    .await;
    let cfg = incremental(
        &conn,
        "SELECT id, c FROM dbo.e",
        json!(0.699_999_988_079_071),
    );
    assert_eq!(read(cfg.clone(), None).await.0, vec![2, 3]);
    let stored = json!({"value": 0.699_999_988_079_071, "boundary": []});
    assert_eq!(read(cfg, Some(&stored)).await.0, vec![2, 3]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bookmark_token_on_a_double_expression_keeps_its_rows() {
    let _serial = SERIAL.lock().await;
    let (_c, conn) = server().await;
    let pool = build_pool(&conn, 2).await.expect("pool");
    exec(&pool, "CREATE TABLE dbo.d (id INT, d FLOAT)").await;
    exec(&pool, "INSERT INTO dbo.d VALUES (1, 0.5), (2, 0.7)").await;
    let cfg = incremental(
        &conn,
        "SELECT id, CAST(d AS REAL) AS c FROM dbo.d WHERE d >= @bookmark",
        json!(0.0),
    );
    let (first, bookmark) = read(cfg.clone(), None).await;
    assert_eq!(first, vec![1, 2]);
    exec(&pool, "INSERT INTO dbo.d VALUES (3, 0.7), (4, 0.9)").await;
    assert_eq!(read(cfg, Some(&bookmark)).await.0, vec![3, 4]);
}

/// `sp_describe_first_result_set` cannot describe a query over a temporary
/// table; the cursor type then comes from a `TOP 0` probe.
#[tokio::test(flavor = "multi_thread")]
async fn a_query_over_a_temp_table_still_compares_in_single_precision() {
    let _serial = SERIAL.lock().await;
    let (_c, conn) = server().await;
    let pool = build_pool(&conn, 1).await.expect("pool");
    let mut setup = pool.get().await.expect("checkout");
    for sql in [
        "CREATE TABLE ##ft (id INT, c REAL)",
        "INSERT INTO ##ft VALUES (1, 0.5), (2, 0.7)",
    ] {
        setup.execute(sql, &[]).await.expect(sql);
    }
    let cfg = incremental(&conn, "SELECT id, c FROM ##ft", json!(0.0));
    let (first, bookmark) = read(cfg.clone(), None).await;
    assert_eq!(first, vec![1, 2]);
    setup
        .execute("INSERT INTO ##ft VALUES (3, 0.7), (4, 0.9)", &[])
        .await
        .expect("late rows");
    assert_eq!(read(cfg, Some(&bookmark)).await.0, vec![3, 4]);
}
