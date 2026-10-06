//! Spanner-emulator integration tests: full-type round-trip, incremental
//! resume, discover, and PK-range sharding. Each test provisions its own
//! database inside a fresh emulator container and skips cleanly when Docker
//! is unavailable.

mod support;

use std::collections::HashMap;

use faucet_core::Source;
use faucet_source_spanner::{ShardConfig, SpannerReplication, SpannerSource, SpannerSourceConfig};
use futures::TryStreamExt;
use serde_json::{Value, json};

fn cfg(host: &str, database: &str, query: &str) -> SpannerSourceConfig {
    let mut cfg = SpannerSourceConfig::new(support::PROJECT, support::INSTANCE, database, query);
    cfg.connection = support::connection(host, database);
    cfg
}

async fn drain(source: &SpannerSource) -> (Vec<Value>, Option<Value>) {
    let ctx = HashMap::new();
    let mut stream = source.stream_pages(&ctx, 0);
    let mut records = Vec::new();
    let mut bookmark = None;
    while let Some(page) = stream.try_next().await.expect("page") {
        records.extend(page.records);
        if page.bookmark.is_some() {
            bookmark = page.bookmark;
        }
    }
    (records, bookmark)
}

// ── Full-type round-trip ─────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn all_types_round_trip_to_json() {
    let Some(emu) = support::start_emulator().await else {
        return;
    };
    support::create_database(
        &emu.host,
        "types",
        &["CREATE TABLE typed (
            id INT64 NOT NULL,
            big INT64,
            f FLOAT64,
            b BOOL,
            s STRING(MAX),
            bin BYTES(MAX),
            n NUMERIC,
            ts TIMESTAMP,
            d DATE,
            j JSON,
            arr ARRAY<INT64>
        ) PRIMARY KEY (id)"],
    )
    .await;
    let client = support::raw_client(&emu.host, "types").await;
    // 2^53 + 1 breaks under f64 — must survive losslessly.
    support::execute_dml(
        &client,
        "INSERT INTO typed (id, big, f, b, s, bin, n, ts, d, j, arr) VALUES (
            1, 9007199254740993, 2.5, TRUE, 'hello',
            b'\\x01\\x02', NUMERIC '123.456789',
            TIMESTAMP '2026-01-02 03:04:05+00:00', DATE '2026-01-02',
            JSON '{\"a\": [1, 2]}', [1, 2, 3]
        )",
    )
    .await;
    support::execute_dml(
        &client,
        "INSERT INTO typed (id) VALUES (2)", // all-NULL row
    )
    .await;

    let source = SpannerSource::new(cfg(&emu.host, "types", "SELECT * FROM typed ORDER BY id"))
        .await
        .expect("source");
    let (records, bookmark) = drain(&source).await;
    assert!(bookmark.is_none(), "full replication carries no bookmark");
    assert_eq!(records.len(), 2);

    let r = &records[0];
    assert_eq!(r["id"], json!(1));
    assert_eq!(r["big"], json!(9007199254740993i64), "INT64 is lossless");
    assert_eq!(r["f"], json!(2.5));
    assert_eq!(r["b"], json!(true));
    assert_eq!(r["s"], json!("hello"));
    assert_eq!(r["bin"], json!("AQI="), "BYTES arrive base64-encoded");
    assert_eq!(r["n"], json!("123.456789"), "NUMERIC stays a string");
    assert!(
        r["ts"].as_str().unwrap().starts_with("2026-01-02T03:04:05"),
        "TIMESTAMP is RFC 3339: {}",
        r["ts"]
    );
    assert_eq!(r["d"], json!("2026-01-02"));
    assert_eq!(r["j"], json!({"a": [1, 2]}), "JSON decodes structurally");
    assert_eq!(r["arr"], json!([1, 2, 3]));

    let null_row = &records[1];
    assert_eq!(null_row["id"], json!(2));
    for col in ["big", "f", "b", "s", "bin", "n", "ts", "d", "j", "arr"] {
        assert_eq!(null_row[col], Value::Null, "column {col} must be null");
    }
}

// ── Incremental resume across two runs ──────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn incremental_resume_reads_only_new_rows() {
    let Some(emu) = support::start_emulator().await else {
        return;
    };
    support::create_database(
        &emu.host,
        "incr",
        &["CREATE TABLE ev (id INT64 NOT NULL, v STRING(MAX)) PRIMARY KEY (id)"],
    )
    .await;
    let client = support::raw_client(&emu.host, "incr").await;
    support::execute_dml(&client, "INSERT INTO ev (id, v) VALUES (1, 'a'), (2, 'b')").await;

    let mut config = cfg(&emu.host, "incr", "SELECT * FROM ev WHERE id > @bookmark");
    config.replication = SpannerReplication::Incremental {
        column: "id".into(),
        initial_value: json!(0),
    };
    let source = SpannerSource::new(config).await.expect("source");
    assert!(source.state_key().is_some(), "incremental is resumable");

    let (first, bookmark) = drain(&source).await;
    assert_eq!(first.len(), 2);
    let bookmark = bookmark.expect("bookmark on final page");
    assert_eq!(bookmark, json!(2));

    // New rows land; resume from the bookmark must read only those.
    support::execute_dml(&client, "INSERT INTO ev (id, v) VALUES (3, 'c'), (4, 'd')").await;
    source
        .apply_start_bookmark(bookmark)
        .await
        .expect("apply bookmark");
    let (second, bookmark2) = drain(&source).await;
    let ids: Vec<i64> = second.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    assert_eq!(ids, vec![3, 4], "only rows past the bookmark");
    assert_eq!(bookmark2, Some(json!(4)));
}

// ── Discover ─────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn discover_returns_tables_with_schema() {
    let Some(emu) = support::start_emulator().await else {
        return;
    };
    support::create_database(
        &emu.host,
        "disco",
        &[
            "CREATE TABLE orders (id INT64 NOT NULL, note STRING(64)) PRIMARY KEY (id)",
            "CREATE TABLE users (uid INT64 NOT NULL, score FLOAT64 NOT NULL) PRIMARY KEY (uid)",
        ],
    )
    .await;

    let source = SpannerSource::new(cfg(&emu.host, "disco", "SELECT 1"))
        .await
        .expect("source");
    assert!(source.supports_discover());
    let mut datasets = source.discover().await.expect("discover");
    datasets.sort_by(|a, b| a.name.cmp(&b.name));
    assert_eq!(datasets.len(), 2, "system schemas are excluded");

    assert_eq!(datasets[0].name, "orders");
    assert_eq!(datasets[0].kind, "table");
    assert_eq!(datasets[0].config_patch["query"], "SELECT * FROM `orders`");
    let schema = datasets[0].schema.as_ref().expect("schema");
    assert_eq!(schema["properties"]["id"]["type"], "integer");
    assert_eq!(
        schema["properties"]["note"]["type"],
        json!(["string", "null"])
    );

    assert_eq!(datasets[1].name, "users");
    assert_eq!(
        datasets[1].schema.as_ref().unwrap()["properties"]["score"]["type"],
        "number"
    );
}

// ── PK-range sharding ────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn shards_partition_the_read_without_loss_or_duplication() {
    let Some(emu) = support::start_emulator().await else {
        return;
    };
    support::create_database(
        &emu.host,
        "shards",
        &["CREATE TABLE items (id INT64 NOT NULL, v STRING(MAX)) PRIMARY KEY (id)"],
    )
    .await;
    let client = support::raw_client(&emu.host, "shards").await;
    let values: Vec<String> = (0..100).map(|i| format!("({i}, 'v{i}')")).collect();
    support::execute_dml(
        &client,
        &format!("INSERT INTO items (id, v) VALUES {}", values.join(", ")),
    )
    .await;

    let mut config = cfg(&emu.host, "shards", "SELECT * FROM items");
    config.shard = Some(ShardConfig { key: "id".into() });
    let source = SpannerSource::new(config).await.expect("source");
    assert!(source.is_shardable());

    let shards = source.enumerate_shards(4).await.expect("enumerate");
    assert_eq!(shards.len(), 4);

    let mut seen: Vec<i64> = Vec::new();
    for shard in &shards {
        source.apply_shard(shard).await.expect("apply");
        let (records, _) = drain(&source).await;
        seen.extend(records.iter().map(|r| r["id"].as_i64().unwrap()));
    }
    seen.sort_unstable();
    assert_eq!(seen, (0..100).collect::<Vec<i64>>(), "no loss, no dupes");

    // The whole-dataset shard clears the narrowing.
    source
        .apply_shard(&faucet_core::ShardSpec::whole())
        .await
        .expect("clear shard");
    let (all, _) = drain(&source).await;
    assert_eq!(all.len(), 100);
}

// ── Coverage: stale reads, convenience fetch, default check probe ────────────

#[tokio::test(flavor = "multi_thread")]
async fn stale_reads_fetch_all_and_check_probe() {
    let Some(emu) = support::start_emulator().await else {
        return;
    };
    support::create_database(
        &emu.host,
        "stale",
        &["CREATE TABLE ev (id INT64 NOT NULL, v STRING(MAX)) PRIMARY KEY (id)"],
    )
    .await;
    let client = support::raw_client(&emu.host, "stale").await;
    support::execute_dml(&client, "INSERT INTO ev (id, v) VALUES (1, 'a'), (2, 'b')").await;
    // Let the staleness horizon move safely past the commit.
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

    let mut config = cfg(&emu.host, "stale", "SELECT * FROM ev ORDER BY id");
    config.exact_staleness_secs = Some(1);
    let source = SpannerSource::new(config).await.expect("source");

    // The non-streaming convenience path drives the same decode pipeline.
    let records = source.fetch_all().await.expect("fetch_all");
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["v"], json!("a"));

    // The default check() probe pulls one real page through stream_pages.
    let report = source
        .check(&faucet_core::CheckContext::default())
        .await
        .expect("check");
    assert!(
        report
            .probes
            .iter()
            .all(|p| !matches!(p.status, faucet_core::ProbeStatus::Fail { .. })),
        "probe failed: {report:?}"
    );

    assert_eq!(source.connector_name(), "spanner");
    assert!(source.config_schema().is_object());
}

// ── SQL-30: TIMESTAMP and NUMERIC cursors compare by value ──────────────────

#[tokio::test(flavor = "multi_thread")]
async fn timestamp_and_numeric_cursors_keep_rows_of_every_fraction_width() {
    let Some(emu) = support::start_emulator().await else {
        return;
    };
    support::create_database(
        &emu.host,
        "cursors",
        &["CREATE TABLE ev (id INT64 NOT NULL, ts TIMESTAMP, n NUMERIC) PRIMARY KEY (id)"],
    )
    .await;
    let client = support::raw_client(&emu.host, "cursors").await;
    support::execute_dml(
        &client,
        "INSERT INTO ev (id, ts, n) VALUES \
         (1, TIMESTAMP '2024-01-01T00:00:00Z', NUMERIC '9'), \
         (2, TIMESTAMP '2024-01-01T00:00:00.5Z', NUMERIC '10')",
    )
    .await;

    let mut ts = cfg(
        &emu.host,
        "cursors",
        "SELECT * FROM ev WHERE ts > TIMESTAMP(@bookmark)",
    );
    ts.replication = SpannerReplication::Incremental {
        column: "ts".into(),
        initial_value: json!("2023-12-31T00:00:00Z"),
    };
    let source = SpannerSource::new(ts).await.expect("source");
    let (rows, bookmark) = drain(&source).await;
    assert_eq!(rows.len(), 2, "both widths kept: {rows:?}");
    let bookmark = bookmark.expect("bookmark");
    let instant = chrono::DateTime::parse_from_rfc3339(bookmark.as_str().unwrap()).unwrap();
    assert_eq!(instant.timestamp_subsec_millis(), 500, "{bookmark}");
    support::execute_dml(
        &client,
        "INSERT INTO ev (id, ts, n) VALUES (3, TIMESTAMP '2024-01-01T00:00:00.75Z', NUMERIC '100')",
    )
    .await;
    source.apply_start_bookmark(bookmark).await.unwrap();
    let (again, _) = drain(&source).await;
    let ids: Vec<i64> = again.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    assert_eq!(ids, vec![3], "the row in the bookmark's second is read");

    let mut num = cfg(
        &emu.host,
        "cursors",
        "SELECT * FROM ev WHERE n > CAST(@bookmark AS NUMERIC) ORDER BY id",
    );
    num.replication = SpannerReplication::Incremental {
        column: "n".into(),
        initial_value: json!("9"),
    };
    let source = SpannerSource::new(num)
        .await
        .expect("NUMERIC cursor accepted");
    let (rows, bookmark) = drain(&source).await;
    let ids: Vec<i64> = rows.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    assert_eq!(ids, vec![2, 3]);
    assert_eq!(bookmark, Some(json!("100")));
}
