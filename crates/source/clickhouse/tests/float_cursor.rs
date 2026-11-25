//! A `>=` cursor on a single-precision column keeps the rows at the bookmark
//! value (#1114): `0.7` as a Float32 is `0.699999988…`, below its decimal.

mod common;

use faucet_core::Source as _;
use faucet_source_clickhouse::{ClickHouseSource, ClickHouseSourceConfig};
use futures::StreamExt as _;
use serde_json::{Value, json};
use std::collections::HashMap;

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[allow(clippy::disallowed_methods)] // a raw client seeds rows out of band
async fn http_exec(base: &str, sql: &str) {
    let resp = reqwest::Client::new()
        .post(base)
        .body(sql.to_string())
        .send()
        .await
        .expect("send");
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    assert!(status.is_success(), "{sql}: {status} {body}");
}

async fn read(cfg: ClickHouseSourceConfig, bookmark: Option<&Value>) -> (Vec<i64>, Value) {
    let source = ClickHouseSource::new(cfg).expect("source");
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
        let page = page.expect("page");
        ids.extend(page.records.iter().map(|r| r["id"].as_i64().unwrap()));
        if let Some(b) = page.bookmark {
            last = b;
        }
    }
    ids.sort_unstable();
    (ids, last)
}

async fn rows_at_the_bookmark_are_kept(column_type: &str, query: &str) {
    let _serial = SERIAL.lock().await;
    let (_c, base) = common::start_clickhouse()
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    http_exec(
        &base,
        &format!("CREATE TABLE t (id UInt32, c {column_type}) ENGINE = MergeTree ORDER BY id"),
    )
    .await;
    http_exec(&base, "INSERT INTO t VALUES (1, 0.5), (2, 0.7)").await;
    let cfg = ClickHouseSourceConfig::new(&base, query).incremental("c", json!(0.0));

    let (first, bookmark) = read(cfg.clone(), None).await;
    assert_eq!(first, vec![1, 2]);
    assert_eq!(bookmark["value"], json!(0.7), "{bookmark}");

    http_exec(&base, "INSERT INTO t VALUES (3, 0.7), (4, 0.9)").await;
    let (second, _) = read(cfg, Some(&bookmark)).await;
    assert_eq!(
        second,
        vec![3, 4],
        "{column_type}: rows committed at the bookmark value"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn float32_cursor_keeps_rows_at_the_bookmark() {
    rows_at_the_bookmark_are_kept("Float32", "SELECT id, c FROM t").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn float32_cursor_keeps_rows_at_a_user_bookmark() {
    rows_at_the_bookmark_are_kept("Float32", "SELECT id, c FROM t WHERE c >= @bookmark").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn float64_cursor_keeps_rows_at_the_bookmark() {
    rows_at_the_bookmark_are_kept("Float64", "SELECT id, c FROM t").await;
}
