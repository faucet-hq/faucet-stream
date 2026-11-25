//! A `>=` cursor on a single-precision column keeps the rows at the bookmark
//! value (#1114): `0.7` as a FLOAT is `0.699999988…`, below its decimal.

use faucet_core::{Source, StreamPage};
use faucet_source_mysql::{MysqlSource, MysqlSourceConfig};
use futures::StreamExt;
use serde_json::{Value, json};
use std::collections::HashMap;
use testcontainers::ContainerAsync;
use testcontainers_modules::mysql::Mysql;

async fn start_mysql() -> (ContainerAsync<Mysql>, String) {
    let container = faucet_conformance::containers::start(Mysql::default).await;
    let port = container.get_host_port_ipv4(3306).await.expect("port");
    (container, format!("mysql://root@127.0.0.1:{port}/test"))
}

async fn exec(url: &str, statements: &[&str]) {
    use sqlx::Connection;
    let mut conn = sqlx::MySqlConnection::connect(url).await.expect("connect");
    for sql in statements {
        sqlx::raw_sql(sql).execute(&mut conn).await.expect(sql);
    }
}

async fn read(cfg: MysqlSourceConfig, bookmark: Option<&Value>) -> (Vec<i64>, Value) {
    let source = MysqlSource::new(cfg).await.expect("source");
    if let Some(b) = bookmark {
        source
            .apply_start_bookmark(b.clone())
            .await
            .expect("bookmark");
    }
    let ctx = HashMap::new();
    let mut stream = source.stream_pages(&ctx, 0);
    let mut ids = Vec::new();
    let mut last = Value::Null;
    while let Some(page) = stream.next().await {
        let StreamPage { records, bookmark } = page.expect("page");
        ids.extend(records.iter().map(|r| r["id"].as_i64().unwrap()));
        if let Some(b) = bookmark {
            last = b;
        }
    }
    ids.sort_unstable();
    (ids, last)
}

async fn rows_at_the_bookmark_are_kept(column_type: &str, query: &str) {
    let (_c, url) = start_mysql().await;
    let create = format!("CREATE TABLE t (id BIGINT PRIMARY KEY, c {column_type} NOT NULL)");
    exec(&url, &[&create, "INSERT INTO t VALUES (1, 0.5), (2, 0.7)"]).await;
    let cfg = MysqlSourceConfig::new(&url, query)
        .with_batch_size(0)
        .incremental("c", json!(0.0));

    let (first, bookmark) = read(cfg.clone(), None).await;
    assert_eq!(first, vec![1, 2]);
    assert_eq!(bookmark["value"], json!(0.7), "{bookmark}");

    exec(&url, &["INSERT INTO t VALUES (3, 0.7), (4, 0.9)"]).await;
    let (second, _) = read(cfg, Some(&bookmark)).await;
    assert_eq!(
        second,
        vec![3, 4],
        "{column_type}: rows committed at the bookmark value"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn float_cursor_keeps_rows_at_the_bookmark() {
    rows_at_the_bookmark_are_kept("FLOAT", "SELECT id, c FROM t").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn float_cursor_keeps_rows_at_a_user_bookmark() {
    rows_at_the_bookmark_are_kept("FLOAT", "SELECT id, c FROM t WHERE c >= @bookmark").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn double_cursor_keeps_rows_at_the_bookmark() {
    rows_at_the_bookmark_are_kept("DOUBLE", "SELECT id, c FROM t").await;
}

/// Values whose single precision lies above or below their decimal, at the
/// range ends, a subnormal and an integer beyond 2^24, each with two rows
/// tied at the bookmark.
const VALUES: &[&str] = &[
    "0.1",
    "0.3",
    "-0.1",
    "-0.7",
    "7.038531e-26",
    "-7.038531e-26",
    "3.4e38",
    "16777217",
    "1.4e-45",
];

#[tokio::test(flavor = "multi_thread")]
async fn float_cursor_keeps_tied_rows_for_values_on_both_sides_of_their_decimal() {
    let (_c, url) = start_mysql().await;
    for (i, v) in VALUES.iter().enumerate() {
        let t = format!("v{i}");
        exec(
            &url,
            &[
                &format!("CREATE TABLE {t} (id BIGINT PRIMARY KEY, c FLOAT NOT NULL)"),
                &format!("INSERT INTO {t} VALUES (0, -3e38), (1, {v}), (2, {v})"),
            ],
        )
        .await;
        for query in [
            format!("SELECT id, c FROM {t}"),
            format!("SELECT id, c FROM {t} WHERE c >= @bookmark"),
        ] {
            let cfg = MysqlSourceConfig::new(&url, &query)
                .with_batch_size(0)
                .incremental("c", json!(-3.3e38));
            let (first, bookmark) = read(cfg.clone(), None).await;
            assert_eq!(first, vec![0, 1, 2], "{v}: {query}");
            let (again, _) = read(cfg, Some(&bookmark)).await;
            assert!(again.is_empty(), "{v}: {query}: {again:?}");
        }
        exec(
            &url,
            &[&format!("INSERT INTO {t} VALUES (3, {v}), (4, {v})")],
        )
        .await;
        let query = format!("SELECT id, c FROM {t}");
        let cfg = MysqlSourceConfig::new(&url, &query)
            .with_batch_size(0)
            .incremental("c", json!(-3.3e38));
        let (all, _) = read(cfg.clone(), None).await;
        assert_eq!(all, vec![0, 1, 2, 3, 4], "{v}");
        let tied = json!({"value": bookmark_of(&url, &t).await, "boundary": []});
        let (second, _) = read(cfg, Some(&tied)).await;
        assert_eq!(
            second,
            vec![1, 2, 3, 4],
            "{v}: every row at the bookmark value"
        );
    }
}

/// The bookmark value a read of `t`'s largest cursor stores.
async fn bookmark_of(url: &str, t: &str) -> Value {
    let cfg = MysqlSourceConfig::new(url, format!("SELECT id, c FROM {t} WHERE id = 1"))
        .with_batch_size(0)
        .incremental("c", json!(-3.3e38));
    read(cfg, None).await.1["value"].clone()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_exact_spelling_of_a_float_reads_the_rows_at_it() {
    let (_c, url) = start_mysql().await;
    exec(
        &url,
        &[
            "CREATE TABLE t (id BIGINT PRIMARY KEY, c FLOAT NOT NULL)",
            "INSERT INTO t VALUES (1, 0.5), (2, 0.7), (3, 0.7)",
        ],
    )
    .await;
    let cfg = MysqlSourceConfig::new(&url, "SELECT id, c FROM t")
        .with_batch_size(0)
        .incremental("c", json!(0.699_999_988_079_071));
    assert_eq!(read(cfg.clone(), None).await.0, vec![2, 3]);
    let stored = json!({"value": 0.699_999_988_079_071, "boundary": []});
    assert_eq!(read(cfg, Some(&stored)).await.0, vec![2, 3]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bookmark_token_on_a_double_expression_keeps_its_rows() {
    let (_c, url) = start_mysql().await;
    exec(
        &url,
        &[
            "CREATE TABLE t (id BIGINT PRIMARY KEY, d DOUBLE NOT NULL)",
            "INSERT INTO t VALUES (1, 0.5), (2, 0.7)",
        ],
    )
    .await;
    let cfg = MysqlSourceConfig::new(
        &url,
        "SELECT id, CAST(d AS FLOAT) AS c FROM t WHERE d >= @bookmark",
    )
    .with_batch_size(0)
    .incremental("c", json!(0.0));
    let (first, bookmark) = read(cfg.clone(), None).await;
    assert_eq!(first, vec![1, 2]);
    exec(&url, &["INSERT INTO t VALUES (3, 0.7), (4, 0.9)"]).await;
    assert_eq!(read(cfg, Some(&bookmark)).await.0, vec![3, 4]);
}
