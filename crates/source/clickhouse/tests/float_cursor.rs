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

async fn bookmark_of(base: &str, t: &str) -> Value {
    let cfg = ClickHouseSourceConfig::new(base, format!("SELECT id, c FROM {t} WHERE id = 1"))
        .incremental("c", json!(-3.3e38));
    read(cfg, None).await.1["value"].clone()
}

#[tokio::test(flavor = "multi_thread")]
async fn float32_cursor_keeps_tied_rows_for_values_on_both_sides_of_their_decimal() {
    let _serial = SERIAL.lock().await;
    let (_c, base) = common::start_clickhouse()
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    for (i, v) in VALUES.iter().enumerate() {
        let t = format!("v{i}");
        http_exec(
            &base,
            &format!(
                "CREATE TABLE {t} (id UInt32, c Nullable(Float32)) ENGINE = MergeTree ORDER BY id"
            ),
        )
        .await;
        http_exec(
            &base,
            &format!("INSERT INTO {t} VALUES (0, -3e38), (1, {v}), (2, {v})"),
        )
        .await;
        for query in [
            format!("SELECT id, c FROM {t}"),
            format!("SELECT id, c FROM {t} WHERE c >= @bookmark"),
        ] {
            let cfg = ClickHouseSourceConfig::new(&base, &query).incremental("c", json!(-3.3e38));
            let (first, bookmark) = read(cfg.clone(), None).await;
            assert_eq!(first, vec![0, 1, 2], "{v}: {query}");
            let (again, _) = read(cfg, Some(&bookmark)).await;
            assert!(again.is_empty(), "{v}: {query}: {again:?}");
        }
        http_exec(&base, &format!("INSERT INTO {t} VALUES (3, {v}), (4, {v})")).await;
        let tied = json!({"value": bookmark_of(&base, &t).await, "boundary": []});
        let cfg = ClickHouseSourceConfig::new(&base, format!("SELECT id, c FROM {t}"))
            .incremental("c", json!(-3.3e38));
        let (second, _) = read(cfg, Some(&tied)).await;
        assert_eq!(
            second,
            vec![1, 2, 3, 4],
            "{v}: every row at the bookmark value"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_exact_spelling_of_a_float32_reads_the_rows_at_it() {
    let _serial = SERIAL.lock().await;
    let (_c, base) = common::start_clickhouse()
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    http_exec(
        &base,
        "CREATE TABLE e (id UInt32, c Float32) ENGINE = MergeTree ORDER BY id",
    )
    .await;
    http_exec(&base, "INSERT INTO e VALUES (1, 0.5), (2, 0.7), (3, 0.7)").await;
    let cfg = ClickHouseSourceConfig::new(&base, "SELECT id, c FROM e")
        .incremental("c", json!(0.699_999_988_079_071));
    assert_eq!(read(cfg.clone(), None).await.0, vec![2, 3]);
    let stored = json!({"value": 0.699_999_988_079_071, "boundary": []});
    assert_eq!(read(cfg, Some(&stored)).await.0, vec![2, 3]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bookmark_token_on_a_double_expression_keeps_its_rows() {
    let _serial = SERIAL.lock().await;
    let (_c, base) = common::start_clickhouse()
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    http_exec(
        &base,
        "CREATE TABLE d (id UInt32, d Float64) ENGINE = MergeTree ORDER BY id",
    )
    .await;
    http_exec(&base, "INSERT INTO d VALUES (1, 0.5), (2, 0.7)").await;
    let cfg = ClickHouseSourceConfig::new(
        &base,
        "SELECT id, toFloat32(d) AS c FROM d WHERE d >= @bookmark",
    )
    .incremental("c", json!(0.0));
    let (first, bookmark) = read(cfg.clone(), None).await;
    assert_eq!(first, vec![1, 2]);
    http_exec(&base, "INSERT INTO d VALUES (3, 0.7), (4, 0.9)").await;
    assert_eq!(read(cfg, Some(&bookmark)).await.0, vec![3, 4]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cursor_missing_from_the_output_fails_the_read() {
    let _serial = SERIAL.lock().await;
    let (_c, base) = common::start_clickhouse()
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    http_exec(
        &base,
        "CREATE TABLE m (id UInt32, c Float32) ENGINE = MergeTree ORDER BY id",
    )
    .await;
    for (query, wanted) in [
        ("SELECT id, c AS C FROM m", "exactly one column"),
        ("SELECT id, c FROM missing_table", "could not describe"),
    ] {
        let source = ClickHouseSource::new(
            ClickHouseSourceConfig::new(&base, query).incremental("c", json!(0.5)),
        )
        .unwrap();
        let err = source
            .fetch_all_incremental()
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains(wanted), "{query}: {err}");
    }
}
