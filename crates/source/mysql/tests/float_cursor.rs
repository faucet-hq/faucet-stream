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
