//! #789 SQL-79 / SQL-128 / SQL-159 / SQL-160: TIME keeps its sign and long
//! hours, DECIMAL is plain text, FLOAT keeps its short form, and the session
//! raises `net_write_timeout`. Requires Docker.

use faucet_core::Source;
use faucet_source_mysql::{MysqlSource, MysqlSourceConfig};
use serde_json::json;
use testcontainers::ContainerAsync;
use testcontainers_modules::mysql::Mysql;

async fn start_mysql() -> (ContainerAsync<Mysql>, String) {
    let container = faucet_conformance::containers::start(Mysql::default).await;
    let port = container
        .get_host_port_ipv4(3306)
        .await
        .expect("mysql port");
    (container, format!("mysql://root@127.0.0.1:{port}/test"))
}

#[tokio::test(flavor = "multi_thread")]
async fn times_decimals_floats_and_session_timeout() {
    let (_c, url) = start_mysql().await;
    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool");
    for sql in [
        "CREATE TABLE v (id INT PRIMARY KEY, t TIME(6), d DECIMAL(30, 10), f FLOAT)",
        "INSERT INTO v VALUES (1, '-01:30:00', 0.0000001, 0.1), \
         (2, '30:00:00.5', 12345678901234567890.5, NULL), \
         (3, '838:59:59', NULL, NULL)",
    ] {
        sqlx::query(sql).execute(&pool).await.expect(sql);
    }
    pool.close().await;

    let source = MysqlSource::new(MysqlSourceConfig::new(
        &url,
        "SELECT t, d, f FROM v ORDER BY id",
    ))
    .await
    .expect("source");
    let rows = source.fetch_all().await.expect("fetch");
    assert_eq!(
        rows[0],
        json!({"t": "-01:30:00", "d": "0.0000001000", "f": 0.1})
    );
    assert_eq!(rows[1]["t"], json!("30:00:00.500"));
    assert_eq!(rows[1]["d"], json!("12345678901234567890.5000000000"));
    assert_eq!(rows[2]["t"], json!("838:59:59"));

    let probe = MysqlSource::new(MysqlSourceConfig::new(
        &url,
        "SELECT @@SESSION.net_write_timeout AS nwt",
    ))
    .await
    .expect("source");
    let rows = probe.fetch_all().await.expect("fetch");
    assert_eq!(rows[0]["nwt"], json!(3600));
}

/// #789 SQL-49: a JSON number a 64-bit float cannot hold exactly fails the
/// read by default and, with `json_big_numbers: string`, arrives as its exact
/// digits.
#[tokio::test(flavor = "multi_thread")]
async fn big_json_numbers_fail_or_stay_exact() {
    let (_c, url) = start_mysql().await;
    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool");
    for sql in [
        "CREATE TABLE j (id INT PRIMARY KEY, doc JSON)",
        // A JSON literal number becomes a DOUBLE in MySQL; a DECIMAL keeps
        // its digits inside the document.
        "INSERT INTO j VALUES (1, JSON_OBJECT('n', CAST('12345678901234567890.123' AS \
         DECIMAL(30, 3)), 'k', 1.5))",
    ] {
        sqlx::query(sql).execute(&pool).await.expect(sql);
    }
    pool.close().await;

    let strict = MysqlSource::new(MysqlSourceConfig::new(&url, "SELECT doc FROM j"))
        .await
        .expect("source");
    let err = strict.fetch_all().await.expect_err("inexact number");
    assert!(err.to_string().contains("column doc"), "{err}");

    let mut cfg = MysqlSourceConfig::new(&url, "SELECT doc FROM j");
    cfg.json_big_numbers = faucet_core::JsonBigNumbers::String;
    let lenient = MysqlSource::new(cfg).await.expect("source");
    let rows = lenient.fetch_all().await.expect("fetch");
    assert_eq!(rows[0]["doc"]["k"], json!(1.5));
    assert_eq!(rows[0]["doc"]["n"], json!("12345678901234567890.123"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_result_with_two_columns_of_one_name_is_refused() {
    use futures::StreamExt;
    let (_c, url) = start_mysql().await;
    let source = MysqlSource::new(MysqlSourceConfig::new(&url, "SELECT 1 AS id, 2 AS id"))
        .await
        .expect("source");
    let err = source.fetch_all().await.expect_err("a column would vanish");
    assert!(
        err.to_string()
            .contains("MySQL query returns two columns named \"id\""),
        "{err}"
    );
    let ctx = std::collections::HashMap::new();
    let mut pages = source.stream_pages(&ctx, 0);
    let err = pages.next().await.expect("an item").expect_err("refused");
    assert!(err.to_string().contains("two columns named"), "{err}");
}

/// A zero or invalid date reads as MySQL's own text (as mysql-cdc spells
/// it), a type the source cannot decode fails the read, and only SQL NULL is
/// `null`.
#[tokio::test(flavor = "multi_thread")]
async fn zero_dates_render_as_text_and_undecodable_cells_fail() {
    let (_c, url) = start_mysql().await;
    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool");
    sqlx::raw_sql(
        "SET SESSION sql_mode = '';
         CREATE TABLE z (id INT PRIMARY KEY, d DATE, dt DATETIME(3), ts TIMESTAMP NULL, g GEOMETRY NULL);
         INSERT INTO z VALUES (1, '0000-00-00', '0000-00-00 00:00:00.000', '0000-00-00 00:00:00', NULL),
                              (2, NULL, NULL, NULL, ST_GeomFromText('POINT(1 1)'));",
    )
    .execute(&pool)
    .await
    .expect("seed");
    pool.close().await;

    let dates = MysqlSource::new(MysqlSourceConfig::new(
        &url,
        "SELECT d, dt, ts FROM z ORDER BY id",
    ))
    .await
    .expect("source");
    let rows = dates.fetch_all().await.expect("fetch");
    assert_eq!(
        rows[0],
        json!({"d": "0000-00-00", "dt": "0000-00-00 00:00:00", "ts": "0000-00-00 00:00:00"})
    );
    assert_eq!(rows[1], json!({"d": null, "dt": null, "ts": null}));

    let geometry = MysqlSource::new(MysqlSourceConfig::new(&url, "SELECT g FROM z WHERE id = 2"))
        .await
        .expect("source");
    let err = geometry.fetch_all().await.expect_err("not silently null");
    assert!(err.to_string().contains("MySQL column g"), "{err}");
    assert!(err.to_string().contains("ST_AsText(g)"), "{err}");
}
