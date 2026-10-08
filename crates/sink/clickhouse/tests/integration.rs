//! Integration tests against a real ClickHouse server in Docker.
//!
//! These **auto-start** a `clickhouse/clickhouse-server` container via
//! `testcontainers` (no env var, not `#[ignore]`d), so they run in CI wherever
//! Docker is present and count toward patch coverage. They exercise the HTTP
//! I/O paths in `src/sink.rs` — the `INSERT … FORMAT JSONEachRow` body builder,
//! the async-insert query-param toggle, `batch_size` re-chunking, `flush`, and
//! the live `check()` probe — that the pure unit tests can't reach. Mirrors the
//! postgres/mssql integration-test pattern.
//!
//! Run explicitly with:
//! `cargo test -p faucet-sink-clickhouse --test integration`.

mod common;

use faucet_core::Sink as _;
use faucet_core::check::{CheckContext, ProbeStatus};
use faucet_sink_clickhouse::{ClickHouseSink, ClickHouseSinkConfig};
use serde_json::{Value, json};
use testcontainers_modules::clickhouse::ClickHouse;
use testcontainers_modules::testcontainers::ContainerAsync;

// `cargo test` runs a binary's tests in parallel; serialize so at most one
// container runs at a time on a small CI runner. Mirrors the mssql/postgres
// integration suites.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn start_clickhouse() -> (ContainerAsync<ClickHouse>, String) {
    common::start_clickhouse()
        .await
        .unwrap_or_else(|e| panic!("{e}"))
}

/// POST a statement over the HTTP interface, asserting a 2xx. Used to run DDL
/// out-of-band from the sink under test.
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

/// Read rows back with `FORMAT JSONEachRow` and decode each line into a
/// [`Value`]. The independent read path confirms the sink's writes landed.
async fn read_rows(base: &str, sql: &str) -> Vec<Value> {
    let body = reqwest::Client::new()
        .post(base)
        .body(format!("{sql} FORMAT JSONEachRow"))
        .send()
        .await
        .expect("query send")
        .text()
        .await
        .expect("query body");
    body.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("decode JSONEachRow line"))
        .collect()
}

/// Read a single `count()` back. ClickHouse serialises `UInt64` as a JSON
/// **string** in `JSONEachRow`, so accept either shape rather than silently
/// reading `-1`.
async fn count_of(base: &str, sql: &str) -> i64 {
    let rows = read_rows(base, sql).await;
    let v = &rows[0]["n"];
    v.as_i64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| panic!("count came back as {v}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn write_batch_inserts_rows_and_rechunks() {
    let _serial = SERIAL.lock().await;
    let (_c, base) = start_clickhouse().await;

    http_exec(
        &base,
        "CREATE TABLE events (id UInt32, name String, score Float64) \
         ENGINE = MergeTree ORDER BY id",
    )
    .await;

    // Single-chunk write (default batch_size) exercises the JSONEachRow body
    // builder + a plain (non-async) INSERT request, and type round-tripping.
    let sink = ClickHouseSink::new(ClickHouseSinkConfig::new(&base, "events")).expect("sink");
    let rows = vec![
        json!({"id": 1, "name": "alice", "score": 1.5}),
        json!({"id": 2, "name": "bob", "score": 2.25}),
        json!({"id": 3, "name": "carol", "score": 3.0}),
    ];
    let written = sink.write_batch(&rows).await.expect("write_batch");
    assert_eq!(written, 3);
    sink.flush().await.expect("flush");

    let back = read_rows(&base, "SELECT id, name, score FROM events ORDER BY id").await;
    assert_eq!(back.len(), 3);
    assert_eq!(back[0]["id"], json!(1));
    assert_eq!(back[0]["name"], json!("alice"));
    assert_eq!(back[0]["score"], json!(1.5));
    assert_eq!(back[2]["name"], json!("carol"));

    // batch_size = 2 over 5 rows splits into requests of 2 + 2 + 1; the return
    // value is the total, and every row must land.
    http_exec(
        &base,
        "CREATE TABLE nums (n UInt32) ENGINE = MergeTree ORDER BY n",
    )
    .await;
    let sink2 = ClickHouseSink::new(ClickHouseSinkConfig::new(&base, "nums").with_batch_size(2))
        .expect("sink");
    let nums: Vec<Value> = (1..=5).map(|n| json!({ "n": n })).collect();
    assert_eq!(sink2.write_batch(&nums).await.expect("chunked write"), 5);
    // Since #617 the accumulated group inserts at `flush`.
    sink2.flush().await.expect("flush");
    let back = read_rows(&base, "SELECT n FROM nums ORDER BY n").await;
    let got: Vec<i64> = back.iter().map(|r| r["n"].as_i64().unwrap()).collect();
    assert_eq!(got, vec![1, 2, 3, 4, 5]);

    // An empty page is a no-op that issues no request.
    assert_eq!(sink.write_batch(&[]).await.expect("empty"), 0);

    // Write modes + a live connect probe.
    assert_eq!(
        sink.supported_write_modes(),
        &[faucet_core::WriteMode::Append]
    );
    let ctx = CheckContext {
        timeout: std::time::Duration::from_secs(5),
    };
    let report = sink.check(&ctx).await.expect("check");
    assert!(
        matches!(report.probes[0].status, ProbeStatus::Pass),
        "connect probe against a live server must pass: {:?}",
        report.probes[0].status
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn write_batch_with_async_insert_lands_rows() {
    let _serial = SERIAL.lock().await;
    let (_c, base) = start_clickhouse().await;

    http_exec(
        &base,
        "CREATE TABLE async_events (id UInt32, name String) ENGINE = MergeTree ORDER BY id",
    )
    .await;

    // async_insert = 1 with wait_for_async_insert = 1 (the default) preserves
    // at-least-once durability, so the rows are queryable immediately after the
    // acknowledged write. Exercises the async-insert query-param branch.
    let sink = ClickHouseSink::new(
        ClickHouseSinkConfig::new(&base, "async_events").with_async_insert(true),
    )
    .expect("sink");
    let rows = vec![
        json!({"id": 10, "name": "x"}),
        json!({"id": 20, "name": "y"}),
    ];
    assert_eq!(sink.write_batch(&rows).await.expect("async write"), 2);
    sink.flush().await.expect("flush");

    let back = read_rows(&base, "SELECT id, name FROM async_events ORDER BY id").await;
    let ids: Vec<i64> = back.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    assert_eq!(ids, vec![10, 20]);
    assert_eq!(back[0]["name"], json!("x"));
}

/// #617 — small pages must merge into one insert.
///
/// ClickHouse creates a MergeTree part per insert, so one insert per small
/// page is not merely slow: it trips "too many parts", a hard failure. This is
/// what makes the engine's own guidance ("insert in large batches") reachable
/// from a source that pages small.
#[tokio::test(flavor = "multi_thread")]
async fn small_pages_merge_into_one_insert() {
    let _serial = SERIAL.lock().await;
    let (_c, base) = start_clickhouse().await;
    http_exec(
        &base,
        "CREATE TABLE merged (id Int64, name String) ENGINE = MergeTree ORDER BY id",
    )
    .await;

    let sink = ClickHouseSink::new(
        ClickHouseSinkConfig::new(&base, "merged")
            .with_batch_size(0)
            .with_create_table(false),
    )
    .expect("sink");

    // Ten pages of 10 — the shape that used to be ten inserts, ten parts.
    for page in 0..10 {
        let rows: Vec<Value> = (0..10)
            .map(|i| json!({ "id": page * 10 + i, "name": "x" }))
            .collect();
        sink.write_batch(&rows).await.expect("write");
    }
    sink.flush().await.expect("flush");

    assert_eq!(
        count_of(&base, "SELECT count() AS n FROM merged").await,
        100,
        "every row must land"
    );

    // One insert ⇒ one part. `system.parts` is the engine's own view of the
    // thing that actually fails, so assert on it rather than a request count.
    let parts = count_of(
        &base,
        "SELECT count() AS n FROM system.parts WHERE table = 'merged' AND active",
    )
    .await;
    assert_eq!(parts, 1, "ten pages must produce ONE part, not ten");
}

fn rows(from: i64, n: i64) -> Vec<Value> {
    (from..from + n)
        .map(|i| json!({ "id": i, "name": "x" }))
        .collect()
}

/// #789 SQL-04: a `flush` whose group commit fails keeps the group, so the
/// retry the resilience policy makes commits it instead of returning `Ok` on
/// an empty accumulator.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_flush_keeps_its_group_for_the_retry() {
    let _serial = SERIAL.lock().await;
    let (_c, base) = start_clickhouse().await;
    let mut cfg = ClickHouseSinkConfig::new(&base, "late")
        .with_batch_size(0)
        .with_create_table(false);
    cfg.commit_rows = Some(1_000);
    let sink = ClickHouseSink::new(cfg).expect("sink");

    sink.write_batch(&rows(0, 10)).await.expect("buffered");
    sink.write_batch(&rows(10, 10)).await.expect("buffered");
    assert!(sink.flush().await.is_err(), "the table does not exist yet");

    http_exec(
        &base,
        "CREATE TABLE late (id Int64, name String) ENGINE = MergeTree ORDER BY id",
    )
    .await;
    sink.flush()
        .await
        .expect("the retry commits the kept group");
    assert_eq!(count_of(&base, "SELECT count() AS n FROM late").await, 20);
}

/// #789 SQL-04: when the group a `write_batch` call fills fails to commit, the
/// rows of earlier pages stay buffered; the current page is the caller's to
/// retry or route, so it is not kept twice.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_group_in_write_batch_keeps_only_the_earlier_pages() {
    let _serial = SERIAL.lock().await;
    let (_c, base) = start_clickhouse().await;
    let mut cfg = ClickHouseSinkConfig::new(&base, "grouped")
        .with_batch_size(0)
        .with_create_table(false);
    cfg.commit_rows = Some(15);
    let sink = ClickHouseSink::new(cfg).expect("sink");

    sink.write_batch(&rows(0, 10)).await.expect("buffered");
    assert!(sink.write_batch(&rows(10, 10)).await.is_err());

    http_exec(
        &base,
        "CREATE TABLE grouped (id Int64, name String) ENGINE = MergeTree ORDER BY id",
    )
    .await;
    sink.write_batch(&rows(10, 10))
        .await
        .expect("the caller's retry of the failed page");
    sink.flush().await.expect("flush");
    assert_eq!(
        count_of(&base, "SELECT count() AS n FROM grouped").await,
        20
    );
    assert_eq!(
        count_of(&base, "SELECT count(DISTINCT id) AS n FROM grouped").await,
        20,
        "no row is written twice"
    );
}

/// #789 SQL-21: with a DLQ the sink commits per page — earlier buffered rows
/// first — and reports exactly the rows of the page that did not land.
#[tokio::test(flavor = "multi_thread")]
async fn the_dlq_path_reports_the_rows_of_the_page_that_did_not_land() {
    let _serial = SERIAL.lock().await;
    let (_c, base) = start_clickhouse().await;
    http_exec(
        &base,
        "CREATE TABLE typed (id Int64, n UInt8) ENGINE = MergeTree ORDER BY id",
    )
    .await;
    let sink = ClickHouseSink::new(
        ClickHouseSinkConfig::new(&base, "typed")
            .with_batch_size(2)
            .with_create_table(false),
    )
    .expect("sink");

    sink.write_batch(&[json!({ "id": 100, "n": 1 })])
        .await
        .expect("buffered");
    let page = vec![
        json!({ "id": 1, "n": 1 }),
        json!({ "id": 2, "n": 2 }),
        json!({ "id": 3, "n": "not a number" }),
        json!({ "id": 4, "n": 4 }),
    ];
    let outcomes = sink.write_batch_partial(&page).await.expect("partial");
    let landed: Vec<bool> = outcomes.iter().map(|o| o.is_ok()).collect();
    assert_eq!(landed, vec![true, true, false, false]);
    assert_eq!(
        count_of(&base, "SELECT count() AS n FROM typed").await,
        3,
        "the buffered row and the first chunk landed"
    );

    let bad_first = vec![json!({ "id": 5, "n": "x" }), json!({ "id": 6, "n": 6 })];
    assert!(
        sink.write_batch_partial(&bad_first).await.is_err(),
        "nothing landed, so the whole page is the router's to route"
    );
    assert!(
        sink.write_batch_partial(&[])
            .await
            .expect("empty")
            .is_empty()
    );
}

/// A group whose earlier pages landed before the failing chunk has nothing to
/// restore; a fully successful per-page write reports every row as written.
#[tokio::test(flavor = "multi_thread")]
async fn landed_rows_are_not_restored_and_a_clean_page_reports_all_rows() {
    let _serial = SERIAL.lock().await;
    let (_c, base) = start_clickhouse().await;
    http_exec(
        &base,
        "CREATE TABLE landed (id Int64, name String) ENGINE = MergeTree ORDER BY id",
    )
    .await;
    let mut cfg = ClickHouseSinkConfig::new(&base, "landed")
        .with_batch_size(10)
        .with_create_table(false);
    cfg.commit_rows = Some(15);
    let sink = ClickHouseSink::new(cfg).expect("sink");

    sink.write_batch(&rows(0, 10)).await.expect("buffered");
    let mut bad = rows(10, 10);
    bad[3] = json!({ "id": "not a number", "name": "x" });
    assert!(sink.write_batch(&bad).await.is_err());
    sink.flush().await.expect("nothing left to commit");
    assert_eq!(count_of(&base, "SELECT count() AS n FROM landed").await, 10);

    let outcomes = sink
        .write_batch_partial(&rows(20, 5))
        .await
        .expect("all rows land");
    assert!(outcomes.iter().all(Result::is_ok));
    assert_eq!(count_of(&base, "SELECT count() AS n FROM landed").await, 15);
}

#[tokio::test(flavor = "multi_thread")]
async fn rfc3339_timestamps_load_into_datetime_columns() {
    let _serial = SERIAL.lock().await;
    let (_c, base) = start_clickhouse().await;
    http_exec(
        &base,
        "CREATE TABLE ts_events (id UInt32, at DateTime('UTC'), at64 DateTime64(3, 'UTC')) \
         ENGINE = MergeTree ORDER BY id",
    )
    .await;
    let sink = ClickHouseSink::new(ClickHouseSinkConfig::new(&base, "ts_events")).expect("sink");
    let rows = vec![json!({
        "id": 1,
        "at": "2024-01-01T05:00:00+05:30",
        "at64": "2024-01-01T00:00:00.123Z"
    })];
    sink.write_batch(&rows).await.expect("write_batch");
    sink.flush().await.expect("flush");
    let back = read_rows(
        &base,
        "SELECT toString(at) AS at, toString(at64) AS at64 FROM ts_events",
    )
    .await;
    assert_eq!(back[0]["at"], json!("2023-12-31 23:30:00"));
    assert_eq!(back[0]["at64"], json!("2024-01-01 00:00:00.123"));
}

#[tokio::test(flavor = "multi_thread")]
async fn auto_create_quotes_hostile_column_names() {
    let _serial = SERIAL.lock().await;
    let (_c, base) = start_clickhouse().await;
    let sink = ClickHouseSink::new(ClickHouseSinkConfig::new(&base, "hostile")).expect("sink");
    let evil = "x\\\" Int64, y String) ENGINE=Log --";
    let mut row = serde_json::Map::new();
    row.insert(evil.to_string(), json!("v"));
    row.insert("trail\\".to_string(), json!("w"));
    sink.write_batch(&[Value::Object(row)])
        .await
        .expect("write_batch");
    sink.flush().await.expect("flush");
    let cols = read_rows(
        &base,
        "SELECT name FROM system.columns WHERE table = 'hostile' ORDER BY name",
    )
    .await;
    let names: Vec<&str> = cols.iter().map(|r| r["name"].as_str().unwrap()).collect();
    assert_eq!(names, vec!["trail\\", evil]);
    let engine = read_rows(
        &base,
        "SELECT engine FROM system.tables WHERE name = 'hostile'",
    )
    .await;
    assert_eq!(engine[0]["engine"], json!("MergeTree"));
}

/// SQL-82: a field first seen after the table existed becomes a column with
/// `create_table`, and fails loudly (never silently dropped) without it.
#[tokio::test(flavor = "multi_thread")]
async fn a_later_field_becomes_a_column_or_fails_loudly() {
    let _serial = SERIAL.lock().await;
    let (_c, base) = start_clickhouse().await;

    let sink = ClickHouseSink::new(ClickHouseSinkConfig::new(&base, "evolving")).expect("sink");
    sink.write_batch(&[json!({"id": 1})]).await.expect("page 1");
    sink.write_batch(&[json!({"id": 2, "extra": "x"})])
        .await
        .expect("page 2");
    sink.flush().await.expect("flush");
    let rows = read_rows(&base, "SELECT id, extra FROM evolving ORDER BY id").await;
    assert_eq!(rows[1]["extra"], json!("x"));

    http_exec(
        &base,
        "CREATE TABLE fixed (id Int64) ENGINE = MergeTree ORDER BY id",
    )
    .await;
    let sink =
        ClickHouseSink::new(ClickHouseSinkConfig::new(&base, "fixed").with_create_table(false))
            .expect("sink");
    let err = sink
        .write_batch(&[json!({"id": 1, "surprise": 2})])
        .await
        .expect_err("no column for the field");
    assert!(err.to_string().contains("surprise"), "{err}");
}
