//! Integration tests against a real ClickHouse server in Docker.
//!
//! These **auto-start** a `clickhouse/clickhouse-server` container via
//! `testcontainers` (no env var, not `#[ignore]`d), so they run in CI wherever
//! Docker is present and count toward patch coverage. They exercise the
//! streaming HTTP I/O paths in `src/stream.rs` — the `JSONEachRow` streaming
//! decoder, line-buffering across chunks, `batch_size` paging, incremental
//! `@bookmark` pushdown + resume, and the live `check()` probe — that the pure
//! unit tests can't reach. Mirrors the postgres/mssql integration-test pattern.
//!
//! Run explicitly with:
//! `cargo test -p faucet-source-clickhouse --test integration`.

use std::collections::HashMap;

use faucet_core::Source as _;
use faucet_core::check::{CheckContext, ProbeStatus};
use faucet_source_clickhouse::{ClickHouseSource, ClickHouseSourceConfig};
use futures::StreamExt as _;
use serde_json::{Value, json};
use testcontainers_modules::clickhouse::ClickHouse;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

// Serialize container starts so at most one runs at a time on a small CI runner.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn start_clickhouse() -> (ContainerAsync<ClickHouse>, String) {
    let container = ClickHouse::default()
        .start()
        .await
        .expect("start clickhouse container");
    let port = container
        .get_host_port_ipv4(8123)
        .await
        .expect("clickhouse host port");
    let base = format!("http://127.0.0.1:{port}");
    (container, base)
}

/// POST a statement over the HTTP interface, asserting a 2xx. Used for DDL and
/// seeding rows out-of-band from the source under test.
async fn http_exec(base: &str, sql: &str) {
    let resp = reqwest::Client::new()
        .post(base)
        .body(sql.to_string())
        .send()
        .await
        .expect("http exec send");
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    assert!(
        status.is_success(),
        "statement failed ({status}): {sql}\n{body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn fetch_all_decodes_types_and_stream_pages_chunk() {
    let _serial = SERIAL.lock().await;
    let (_c, base) = start_clickhouse().await;

    http_exec(
        &base,
        "CREATE TABLE types_test (id UInt32, name String, score Float64, flag Bool) \
         ENGINE = MergeTree ORDER BY id",
    )
    .await;
    http_exec(
        &base,
        "INSERT INTO types_test FORMAT JSONEachRow \
         {\"id\":1,\"name\":\"alice\",\"score\":1.5,\"flag\":true}",
    )
    .await;

    // fetch_all drives the buffered collect_all path + JSONEachRow decode.
    let source = ClickHouseSource::new(ClickHouseSourceConfig::new(
        &base,
        "SELECT id, name, score, flag FROM types_test ORDER BY id",
    ))
    .expect("source");
    let rows = source.fetch_all().await.expect("fetch_all");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], json!(1));
    assert_eq!(rows[0]["name"], json!("alice"));
    assert_eq!(rows[0]["score"], json!(1.5));
    assert_eq!(rows[0]["flag"], json!(true));

    // Streaming: 5 rows at batch_size 2 -> pages of 2, 2, 1. This exercises the
    // streaming decoder, line-buffering, and page-boundary logic in stream_pages.
    http_exec(
        &base,
        "CREATE TABLE nums (n UInt32) ENGINE = MergeTree ORDER BY n",
    )
    .await;
    let vals: Vec<String> = (1..=5).map(|n| format!("{{\"n\":{n}}}")).collect();
    http_exec(
        &base,
        &format!("INSERT INTO nums FORMAT JSONEachRow {}", vals.join(" ")),
    )
    .await;

    let source = ClickHouseSource::new(
        ClickHouseSourceConfig::new(&base, "SELECT n FROM nums ORDER BY n").with_batch_size(2),
    )
    .expect("source");
    let ctx: HashMap<String, Value> = HashMap::new();
    let pages: Vec<_> = source
        .stream_pages(&ctx, 2)
        .map(|p| p.expect("page"))
        .collect()
        .await;
    let sizes: Vec<usize> = pages.iter().map(|p| p.records.len()).collect();
    assert_eq!(sizes, vec![2, 2, 1], "batch_size=2 over 5 rows");
    let all: Vec<i64> = pages
        .iter()
        .flat_map(|p| p.records.iter())
        .map(|r| r["n"].as_i64().unwrap())
        .collect();
    assert_eq!(all, vec![1, 2, 3, 4, 5]);
    // Full replication carries no bookmark on any page.
    assert!(pages.iter().all(|p| p.bookmark.is_none()));

    // A live connect probe.
    let probe_ctx = CheckContext {
        timeout: std::time::Duration::from_secs(5),
    };
    let report = source.check(&probe_ctx).await.expect("check");
    assert!(
        matches!(report.probes[0].status, ProbeStatus::Pass),
        "connect probe against a live server must pass: {:?}",
        report.probes[0].status
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn incremental_pushdown_and_resume() {
    let _serial = SERIAL.lock().await;
    let (_c, base) = start_clickhouse().await;

    http_exec(
        &base,
        "CREATE TABLE events (id UInt32, updated_at String) ENGINE = MergeTree ORDER BY id",
    )
    .await;
    http_exec(
        &base,
        "INSERT INTO events FORMAT JSONEachRow \
         {\"id\":1,\"updated_at\":\"2024-01-01\"} \
         {\"id\":2,\"updated_at\":\"2024-02-01\"} \
         {\"id\":3,\"updated_at\":\"2024-03-01\"}",
    )
    .await;

    // `@bookmark` is substituted server-side as an injection-safe literal, and
    // the source also filters client-side. initial_value 2024-01-15 -> rows
    // 2024-02-01 and 2024-03-01, bookmark = the new max.
    let source = ClickHouseSource::new(
        ClickHouseSourceConfig::new(
            &base,
            "SELECT id, updated_at FROM events WHERE updated_at > @bookmark ORDER BY updated_at",
        )
        .incremental("updated_at", json!("2024-01-15")),
    )
    .expect("source");

    let (rows, bookmark) = source.fetch_all_incremental().await.expect("run 1");
    let ids: Vec<i64> = rows.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    assert_eq!(ids, vec![2, 3], "run 1 honours initial_value pushdown");
    assert_eq!(bookmark, Some(json!("2024-03-01")));

    // Persist the bookmark, add a newer row, run again — no duplicates.
    source
        .apply_start_bookmark(bookmark.unwrap())
        .await
        .expect("apply bookmark");
    http_exec(
        &base,
        "INSERT INTO events FORMAT JSONEachRow {\"id\":4,\"updated_at\":\"2024-04-01\"}",
    )
    .await;

    let (rows2, bookmark2) = source.fetch_all_incremental().await.expect("run 2");
    let ids2: Vec<i64> = rows2.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    assert_eq!(ids2, vec![4], "run 2 resumes from bookmark, no duplicates");
    assert_eq!(bookmark2, Some(json!("2024-04-01")));

    // The streaming path carries the incremental bookmark on its final page.
    // A fresh source (initial_value 2024-01-15) sees rows 2,3,4.
    let stream_source = ClickHouseSource::new(
        ClickHouseSourceConfig::new(
            &base,
            "SELECT id, updated_at FROM events WHERE updated_at > @bookmark ORDER BY updated_at",
        )
        .incremental("updated_at", json!("2024-01-15")),
    )
    .expect("stream source");
    let ctx: HashMap<String, Value> = HashMap::new();
    let pages: Vec<_> = stream_source
        .stream_pages(&ctx, 1000)
        .map(|p| p.expect("page"))
        .collect()
        .await;
    let streamed: Vec<i64> = pages
        .iter()
        .flat_map(|p| p.records.iter())
        .map(|r| r["id"].as_i64().unwrap())
        .collect();
    assert_eq!(streamed, vec![2, 3, 4]);
    assert_eq!(
        pages.last().and_then(|p| p.bookmark.clone()),
        Some(json!("2024-04-01")),
        "final streamed page carries the incremental bookmark"
    );

    // state_key is stable and derived for incremental replication.
    assert!(
        stream_source.state_key().is_some(),
        "incremental replication must expose a bookmark state key"
    );
}

/// #789 SQL-05: a 64-bit integer cursor orders numerically — `100` follows
/// `99` — whatever the server's `output_format_json_quote_64bit_integers`
/// default, because the source quotes them and decodes by column type.
#[tokio::test(flavor = "multi_thread")]
async fn a_uint64_cursor_orders_numerically_across_runs() {
    let _serial = SERIAL.lock().await;
    let (_c, base) = start_clickhouse().await;
    http_exec(
        &base,
        "CREATE TABLE seq (id UInt64, v Int64) ENGINE = MergeTree ORDER BY id",
    )
    .await;
    http_exec(
        &base,
        "INSERT INTO seq VALUES (9, -1), (99, -2), (100, -3), (101, 9223372036854775807)",
    )
    .await;

    let source = ClickHouseSource::new(
        ClickHouseSourceConfig::new(&base, "SELECT id, v FROM seq ORDER BY id")
            .incremental("id", json!(9)),
    )
    .expect("source");
    let (rows, bookmark) = source.fetch_all_incremental().await.expect("run 1");
    let ids: Vec<u64> = rows.iter().map(|r| r["id"].as_u64().unwrap()).collect();
    assert_eq!(ids, vec![99, 100, 101]);
    assert_eq!(bookmark, Some(json!(101)));
    assert_eq!(rows[2]["v"], json!(9_223_372_036_854_775_807i64));

    source
        .apply_start_bookmark(bookmark.unwrap())
        .await
        .expect("bookmark");
    http_exec(&base, "INSERT INTO seq VALUES (200, 0)").await;
    let ctx: HashMap<String, Value> = HashMap::new();
    let pages: Vec<_> = source
        .stream_pages(&ctx, 1000)
        .map(|p| p.expect("page"))
        .collect()
        .await;
    let streamed: Vec<u64> = pages
        .iter()
        .flat_map(|p| p.records.iter())
        .map(|r| r["id"].as_u64().unwrap())
        .collect();
    assert_eq!(
        streamed,
        vec![200],
        "the run after 101 reads 200, not nothing"
    );
}

/// #789 SQL-15: decimals and 128/256-bit integers arrive as exact strings.
#[tokio::test(flavor = "multi_thread")]
async fn decimals_and_wide_integers_stay_exact() {
    let _serial = SERIAL.lock().await;
    let (_c, base) = start_clickhouse().await;
    http_exec(
        &base,
        "CREATE TABLE money (id UInt8, amount Decimal(38, 10), big Int128, huge UInt256) \
         ENGINE = MergeTree ORDER BY id",
    )
    .await;
    http_exec(
        &base,
        "INSERT INTO money VALUES (1, '1234567890123456789012345678.0123456789', \
         '-170141183460469231731687303715884105728', \
         '115792089237316195423570985008687907853269984665640564039457584007913129639935')",
    )
    .await;
    let source = ClickHouseSource::new(ClickHouseSourceConfig::new(
        &base,
        "SELECT id, amount, big, huge FROM money",
    ))
    .expect("source");
    let rows = source.fetch_all().await.expect("fetch");
    assert_eq!(rows[0]["id"], json!(1));
    assert_eq!(
        rows[0]["amount"],
        json!("1234567890123456789012345678.0123456789")
    );
    assert_eq!(
        rows[0]["big"],
        json!("-170141183460469231731687303715884105728")
    );
    assert_eq!(
        rows[0]["huge"],
        json!("115792089237316195423570985008687907853269984665640564039457584007913129639935")
    );
}
