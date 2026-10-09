//! #789 SQL-02 / SQL-12: every column type reaches JSON with its value — types
//! without a native decode are read as text instead of becoming `null`, and a
//! cell that still cannot be decoded fails the run.

use faucet_core::Source;
use faucet_source_postgres::{PostgresSource, PostgresSourceConfig};
use serde_json::json;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;

async fn start_postgres() -> (ContainerAsync<Postgres>, String) {
    let container =
        faucet_conformance::containers::start(|| Postgres::default().with_tag("16-alpine")).await;
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("postgres port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    (container, url)
}

#[tokio::test(flavor = "multi_thread")]
async fn types_without_a_native_decode_are_read_as_text() {
    let (_c, url) = start_postgres().await;
    let pool = sqlx::PgPool::connect(&url).await.expect("pool");
    for sql in [
        "CREATE TYPE mood AS ENUM ('sad', 'happy')",
        "CREATE TABLE t (id INT PRIMARY KEY, tags TEXT[], m mood, span INTERVAL, \
         addr INET, cash MONEY, f FLOAT8, n NUMERIC, at TIME, tz TIMETZ, \
         r INT4RANGE, x XML, b BYTEA, missing TEXT[])",
        "INSERT INTO t VALUES (1, '{a,b}', 'happy', '1 day 02:00:00', '10.0.0.1', \
         12.5, 'NaN', 'NaN', '24:00:00', '10:00:00+02', '[1,5)', '<a/>', '\\x0001', NULL), \
         (2, NULL, NULL, NULL, NULL, NULL, '-Infinity', 1.50, NULL, NULL, NULL, NULL, NULL, NULL)",
    ] {
        sqlx::query(sql).execute(&pool).await.expect(sql);
    }
    pool.close().await;

    let source = PostgresSource::new(PostgresSourceConfig::new(
        &url,
        "SELECT * FROM t ORDER BY id",
    ))
    .await
    .expect("source");
    let rows = source.fetch_all().await.expect("fetch");
    assert_eq!(
        rows[0],
        json!({
            "id": 1, "tags": "{a,b}", "m": "happy", "span": "1 day 02:00:00",
            "addr": "10.0.0.1", "cash": "$12.50", "f": "NaN", "n": "NaN",
            "at": "24:00:00", "tz": "10:00:00+02", "r": "[1,5)", "x": "<a/>",
            "b": "AAE=", "missing": null
        })
    );
    assert_eq!(rows[1]["f"], json!("-Infinity"));
    assert_eq!(rows[1]["n"], json!("1.50"));
    assert_eq!(rows[1]["tags"], json!(null));

    let streamed = {
        use futures::StreamExt;
        let ctx = std::collections::HashMap::new();
        let pages: Vec<_> = source.stream_pages(&ctx, 10).collect().await;
        pages
            .into_iter()
            .flat_map(|p| p.expect("page").records)
            .collect::<Vec<_>>()
    };
    assert_eq!(streamed, rows, "the streaming path decodes the same way");

    // Duplicate column names cannot be addressed from a wrapper query, so an
    // undecodable value there is an error, never a silent null.
    let dup = PostgresSource::new(PostgresSourceConfig::new(
        &url,
        "SELECT t.span, t.span FROM t WHERE id = 1",
    ))
    .await
    .expect("source");
    let err = dup.fetch_all().await.unwrap_err().to_string();
    assert!(err.contains("cannot decode"), "{err}");

    let exact = PostgresSource::new(PostgresSourceConfig::new(
        &url,
        "SELECT 1.50::numeric AS n, 1.50::numeric AS n, '12:30:00'::time AS t, '12:30:00'::time AS t",
    ))
    .await
    .expect("source");
    let row = &exact.fetch_all().await.expect("fetch")[0];
    assert_eq!(row["t"], json!("12:30:00"));
    let n = row["n"].as_str().expect("numeric is a string");
    assert_eq!(n.parse::<f64>().unwrap(), 1.5, "{n}");
}

#[tokio::test(flavor = "multi_thread")]
async fn infinite_dates_reals_and_partitions() {
    let (_c, url) = start_postgres().await;
    let pool = sqlx::PgPool::connect(&url).await.expect("pool");
    for sql in [
        "CREATE TABLE inf (id INT PRIMARY KEY, d DATE, ts TIMESTAMP, tz TIMESTAMPTZ, r REAL)",
        "INSERT INTO inf VALUES (1, 'infinity', 'infinity', '-infinity', 0.1), \
         (2, '-infinity', '-infinity', 'infinity', NULL), \
         (3, '2024-01-02', '2024-01-02 03:04:05', '2024-01-02 03:04:05+00', 'NaN')",
        "CREATE TABLE parted (id INT, region TEXT) PARTITION BY LIST (region)",
        "CREATE TABLE parted_eu PARTITION OF parted FOR VALUES IN ('eu')",
        "CREATE TABLE parted_us PARTITION OF parted FOR VALUES IN ('us')",
    ] {
        sqlx::query(sql).execute(&pool).await.expect(sql);
    }
    pool.close().await;

    let source = PostgresSource::new(PostgresSourceConfig::new(
        &url,
        "SELECT d, ts, tz, r FROM inf ORDER BY id",
    ))
    .await
    .expect("source");
    let rows = source.fetch_all().await.expect("fetch");
    assert_eq!(
        rows[0],
        json!({"d": "infinity", "ts": "infinity", "tz": "-infinity", "r": 0.1})
    );
    assert_eq!(
        rows[1],
        json!({"d": "-infinity", "ts": "-infinity", "tz": "infinity", "r": null})
    );
    assert_eq!(rows[2]["d"], json!("2024-01-02"));
    assert_eq!(rows[2]["r"], json!("NaN"));

    let names: Vec<String> = source
        .discover()
        .await
        .expect("discover")
        .into_iter()
        .map(|d| d.name)
        .collect();
    assert!(names.iter().any(|n| n.ends_with("parted")), "{names:?}");
    assert!(
        !names.iter().any(|n| n.contains("parted_")),
        "partitions must not be listed beside their parent: {names:?}"
    );
}

/// #789 SQL-49: a JSON number a 64-bit float cannot hold exactly fails the
/// read by default and, with `json_big_numbers: string`, arrives as its exact
/// digits; ordinary numbers stay numbers.
#[tokio::test(flavor = "multi_thread")]
async fn big_json_numbers_fail_or_stay_exact() {
    let (_c, url) = start_postgres().await;
    let pool = sqlx::PgPool::connect(&url).await.expect("pool");
    for sql in [
        "CREATE TABLE j (id INT PRIMARY KEY, b JSONB, t JSON)",
        "INSERT INTO j VALUES (1, '{\"n\": 12345678901234567890.123, \"k\": 1.5}', \
         '[98765432109876543210987]')",
    ] {
        sqlx::query(sql).execute(&pool).await.expect(sql);
    }
    pool.close().await;

    let strict = PostgresSource::new(PostgresSourceConfig::new(&url, "SELECT b FROM j"))
        .await
        .expect("source");
    let err = strict.fetch_all().await.expect_err("inexact number");
    assert!(err.to_string().contains("column b"), "{err}");
    assert!(
        err.to_string().contains("12345678901234567890.123"),
        "{err}"
    );

    let mut cfg = PostgresSourceConfig::new(&url, "SELECT b, t FROM j");
    cfg.json_big_numbers = faucet_core::JsonBigNumbers::String;
    let lenient = PostgresSource::new(cfg).await.expect("source");
    let rows = lenient.fetch_all().await.expect("fetch");
    assert_eq!(
        rows[0]["b"],
        json!({"n": "12345678901234567890.123", "k": 1.5})
    );
    assert_eq!(rows[0]["t"], json!(["98765432109876543210987"]));
}
