//! Integration tests for `PostgresSource::stream_pages` against a real
//! Postgres instance via testcontainers.
//!
//! These tests require Docker. Each test boots its own container and seeds
//! its own table so they are fully isolated and safe to run in parallel.

use faucet_core::{DEFAULT_BATCH_SIZE, Source};
use faucet_source_postgres::{PostgresSource, PostgresSourceConfig};
use futures::StreamExt;
use std::collections::HashMap;
use std::time::Instant;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;

/// Start a Postgres container and return both the container handle and a
/// connection URL. The container is kept alive by the returned handle; drop
/// it to stop the container.
async fn start_postgres() -> (ContainerAsync<Postgres>, String) {
    let container: ContainerAsync<Postgres> =
        faucet_conformance::containers::start(|| Postgres::default().with_tag("16-alpine")).await;
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("postgres port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    (container, url)
}

/// Create a single-column `events` table and insert `n` rows of `(id)` with
/// values `1..=n`.
async fn seed_events(url: &str, n: i64) {
    let pool = sqlx::PgPool::connect(url).await.expect("pool connect");
    sqlx::query("CREATE TABLE events (id BIGINT PRIMARY KEY)")
        .execute(&pool)
        .await
        .expect("create table");
    // Use generate_series for a fast bulk insert — avoids 10k round-trips.
    sqlx::query("INSERT INTO events (id) SELECT generate_series(1, $1)")
        .bind(n)
        .execute(&pool)
        .await
        .expect("insert rows");
    pool.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_chunks_rows_into_batch_sized_pages() {
    let (_container, url) = start_postgres().await;
    seed_events(&url, 10_000).await;

    let config =
        PostgresSourceConfig::new(url, "SELECT id FROM events ORDER BY id").with_batch_size(1000);
    let source = PostgresSource::new(config).await.expect("source new");

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 1000);

    let mut page_count = 0;
    let mut total_rows = 0;
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        page_count += 1;
        total_rows += page.records.len();
        assert_eq!(
            page.records.len(),
            1000,
            "every page must be exactly batch_size rows when total is a multiple"
        );
        assert!(
            page.bookmark.is_none(),
            "postgres source has no incremental mode yet; bookmark must be None"
        );
    }

    assert_eq!(page_count, 10, "10_000 / 1000 = 10 pages");
    assert_eq!(total_rows, 10_000);
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_partial_final_page() {
    let (_container, url) = start_postgres().await;
    seed_events(&url, 2_500).await;

    let config =
        PostgresSourceConfig::new(url, "SELECT id FROM events ORDER BY id").with_batch_size(1000);
    let source = PostgresSource::new(config).await.expect("source new");

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 1000);

    let mut sizes = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        sizes.push(page.records.len());
    }
    assert_eq!(
        sizes,
        vec![1000, 1000, 500],
        "partial trailing page must hold the remainder"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_batch_size_zero_emits_single_page() {
    let (_container, url) = start_postgres().await;
    seed_events(&url, 10_000).await;

    let config =
        PostgresSourceConfig::new(url, "SELECT id FROM events ORDER BY id").with_batch_size(0);
    let source = PostgresSource::new(config).await.expect("source new");

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 0);

    let mut collected = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        collected.push(page.records.len());
    }
    assert_eq!(
        collected,
        vec![10_000],
        "batch_size = 0 must drain the cursor and emit exactly one page"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_empty_result_yields_no_pages() {
    let (_container, url) = start_postgres().await;
    // Create the table but insert no rows.
    let pool = sqlx::PgPool::connect(&url).await.expect("pool connect");
    sqlx::query("CREATE TABLE events (id BIGINT PRIMARY KEY)")
        .execute(&pool)
        .await
        .expect("create table");
    pool.close().await;

    let config =
        PostgresSourceConfig::new(url, "SELECT id FROM events").with_batch_size(DEFAULT_BATCH_SIZE);
    let source = PostgresSource::new(config).await.expect("source new");

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, DEFAULT_BATCH_SIZE);

    let mut page_count = 0;
    while let Some(page) = pages.next().await {
        let _ = page.expect("page ok");
        page_count += 1;
    }
    assert_eq!(
        page_count, 0,
        "empty result with no bookmark must yield zero pages"
    );
}

/// Catches the "buffered-then-chunked" anti-pattern.
///
/// The Postgres wire protocol sends all rows from a simple `SELECT` in one
/// batch (without a server-side cursor), so a `pg_sleep`-style server-side
/// timing test would always look identical regardless of client-side
/// streaming.
///
/// Instead, we test the *client-side* signal: the default `stream_pages`
/// impl calls `fetch_with_context_incremental` which materialises every row
/// into a `Vec<Value>` before any page is yielded, while the true-streaming
/// impl parses rows from the wire and yields after `batch_size` are buffered.
///
/// For a large result, the parse-and-buffer cost dominates and the
/// difference is observable: dropping the stream after the first page in the
/// streaming impl avoids parsing the remaining ~99% of rows.
#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_first_page_completes_without_parsing_full_result() {
    let (_container, url) = start_postgres().await;
    seed_events(&url, 200_000).await;

    // Time a full drain so we have a reference for "parse all rows".
    let config_full =
        PostgresSourceConfig::new(&url, "SELECT id FROM events ORDER BY id").with_batch_size(1000);
    let source = PostgresSource::new(config_full).await.expect("source new");
    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let start = Instant::now();
    let mut full_pages = source.stream_pages(&ctx, 1000);
    while let Some(page) = full_pages.next().await {
        let _ = page.expect("page ok");
    }
    let full_elapsed = start.elapsed();
    drop(full_pages);
    drop(source);

    // Now grab just the first page and drop the stream.
    let config_first =
        PostgresSourceConfig::new(&url, "SELECT id FROM events ORDER BY id").with_batch_size(1000);
    let source = PostgresSource::new(config_first).await.expect("source new");
    let start = Instant::now();
    let mut first_pages = source.stream_pages(&ctx, 1000);
    let first_page = first_pages
        .next()
        .await
        .expect("first page exists")
        .expect("page ok");
    let first_elapsed = start.elapsed();
    drop(first_pages);
    assert_eq!(first_page.records.len(), 1000);

    // First page should arrive in well under half the full-drain time.
    // The default (buffer-then-chunk) impl would parse all 200k rows before
    // the first page, making first_elapsed ≈ full_elapsed.
    assert!(
        first_elapsed * 2 < full_elapsed,
        "first page should arrive without parsing the full result; \
         first page took {first_elapsed:?}, full drain took {full_elapsed:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_preserves_row_contents() {
    let (_container, url) = start_postgres().await;
    let pool = sqlx::PgPool::connect(&url).await.expect("pool connect");
    sqlx::query("CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT NOT NULL)")
        .execute(&pool)
        .await
        .expect("create table");
    sqlx::query("INSERT INTO items (id, name) VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")
        .execute(&pool)
        .await
        .expect("insert");
    pool.close().await;

    let config =
        PostgresSourceConfig::new(url, "SELECT id, name FROM items ORDER BY id").with_batch_size(2);
    let source = PostgresSource::new(config).await.expect("source new");

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 2);

    let mut all_records = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        all_records.extend(page.records);
    }

    assert_eq!(all_records.len(), 3);
    assert_eq!(all_records[0]["id"], 1);
    assert_eq!(all_records[0]["name"], "alpha");
    assert_eq!(all_records[2]["name"], "gamma");
}

/// Exercises every arm of `pg_value_to_json` by selecting one row whose columns
/// span the full set of supported Postgres types. Without this, the converter's
/// integer-width / float / temporal / uuid / numeric / bytea branches were
/// never executed (the other tests only use BIGINT and TEXT).
#[tokio::test(flavor = "multi_thread")]
async fn all_column_types_decode_to_expected_json() {
    let (_container, url) = start_postgres().await;
    let pool = sqlx::PgPool::connect(&url).await.expect("pool connect");
    sqlx::query(
        "CREATE TABLE types_t (
            jb JSONB, t TEXT, big BIGINT, i4 INT, sm SMALLINT,
            dp DOUBLE PRECISION, r REAL, b BOOLEAN,
            tstz TIMESTAMPTZ, ts TIMESTAMP, d DATE, tm TIME,
            u UUID, num NUMERIC, by BYTEA
        )",
    )
    .execute(&pool)
    .await
    .expect("create table");
    sqlx::query(
        "INSERT INTO types_t VALUES (
            '{\"nested\":[1,2],\"ok\":true}'::jsonb, 'hello',
            9223372036854775807, 2147483647, 32767,
            3.5, 1.5, true,
            '2024-01-02T03:04:05Z'::timestamptz, '2024-01-02 03:04:05'::timestamp,
            '2024-01-02'::date, '03:04:05'::time,
            '00000000-0000-0000-0000-000000000001'::uuid, '123.45'::numeric,
            '\\x68690a'::bytea
        )",
    )
    .execute(&pool)
    .await
    .expect("insert");
    pool.close().await;

    let config = PostgresSourceConfig::new(url, "SELECT * FROM types_t");
    let source = PostgresSource::new(config).await.expect("source new");
    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, DEFAULT_BATCH_SIZE);
    let page = pages.next().await.expect("one page").expect("page ok");
    assert_eq!(page.records.len(), 1);
    let row = &page.records[0];

    assert_eq!(row["jb"], serde_json::json!({"nested": [1, 2], "ok": true}));
    assert_eq!(row["t"], "hello");
    assert_eq!(row["big"], 9223372036854775807i64);
    assert_eq!(row["i4"], 2147483647);
    assert_eq!(row["sm"], 32767);
    assert_eq!(row["dp"], 3.5);
    assert_eq!(row["r"], 1.5);
    assert_eq!(row["b"], true);
    assert!(
        row["tstz"]
            .as_str()
            .unwrap()
            .starts_with("2024-01-02T03:04:05"),
        "timestamptz should render as RFC3339, got {:?}",
        row["tstz"]
    );
    assert_eq!(row["ts"], "2024-01-02 03:04:05");
    assert_eq!(row["d"], "2024-01-02");
    assert_eq!(row["tm"], "03:04:05");
    assert_eq!(row["u"], "00000000-0000-0000-0000-000000000001");
    // NUMERIC -> string, preserving precision. sqlx's PgNumeric -> BigDecimal
    // conversion pads the scale to a multiple of 4 ("123.4500"), so assert on
    // the meaningful prefix rather than an exact rendering.
    assert!(
        row["num"].as_str().unwrap().starts_with("123.45"),
        "NUMERIC should render as a precise decimal string, got {:?}",
        row["num"]
    );
    assert_eq!(row["by"], "aGkK"); // BYTEA 0x68690a ("hi\n") -> base64
}

/// Context tokens (`{key}`) must become positional bind markers bound as native
/// scalar types — exercising `resolve_query`'s context branch and the typed
/// arms of `bind_params` (integer + bool), not a raw jsonb bind.
#[tokio::test(flavor = "multi_thread")]
async fn context_tokens_bind_as_typed_params() {
    let (_container, url) = start_postgres().await;
    let pool = sqlx::PgPool::connect(&url).await.expect("pool connect");
    sqlx::query("CREATE TABLE acct (id BIGINT, name TEXT, active BOOLEAN)")
        .execute(&pool)
        .await
        .expect("create table");
    sqlx::query("INSERT INTO acct VALUES (1, 'alice', true), (2, 'bob', false)")
        .execute(&pool)
        .await
        .expect("insert");
    pool.close().await;

    let config = PostgresSourceConfig::new(
        url,
        "SELECT name FROM acct WHERE id = {id} AND active = {active} ORDER BY name",
    );
    let source = PostgresSource::new(config).await.expect("source new");
    let mut ctx: HashMap<String, serde_json::Value> = HashMap::new();
    ctx.insert("id".into(), serde_json::json!(1));
    ctx.insert("active".into(), serde_json::json!(true));

    let mut pages = source.stream_pages(&ctx, DEFAULT_BATCH_SIZE);
    let page = pages.next().await.expect("one page").expect("page ok");
    assert_eq!(page.records.len(), 1, "only account id=1 is active");
    assert_eq!(page.records[0]["name"], "alice");
}

// ── PK-range sharding (Mode B, #230) ────────────────────────────────────────

/// The core Mode B correctness guarantee: enumerating a source into N shards and
/// reading each shard yields every row exactly once — no duplication, no loss.
#[tokio::test(flavor = "multi_thread")]
async fn shards_partition_rows_disjointly_and_completely() {
    use faucet_source_postgres::ShardConfig;

    let (_container, url) = start_postgres().await;
    seed_events(&url, 1000).await; // ids 1..=1000

    let mut config = PostgresSourceConfig::new(&url, "SELECT id FROM events");
    config.shard = Some(ShardConfig { key: "id".into() });

    // A coordinator enumerates the shard set.
    let coordinator = PostgresSource::new(config.clone())
        .await
        .expect("coordinator source");
    assert!(coordinator.is_shardable());
    let shards = coordinator.enumerate_shards(4).await.expect("enumerate");
    assert!(
        (2..=4).contains(&shards.len()),
        "expected 2..=4 shards, got {}",
        shards.len()
    );

    // Each shard runs on a fresh source narrowed via apply_shard.
    let mut all_ids: Vec<i64> = Vec::new();
    for shard in &shards {
        let s = PostgresSource::new(config.clone())
            .await
            .expect("shard source");
        s.apply_shard(shard).await.expect("apply_shard");
        let ctx: HashMap<String, serde_json::Value> = HashMap::new();
        let mut pages = s.stream_pages(&ctx, 0);
        while let Some(page) = pages.next().await {
            for rec in page.expect("page ok").records {
                all_ids.push(rec["id"].as_i64().expect("id is int"));
            }
        }
    }

    all_ids.sort();
    let expected: Vec<i64> = (1..=1000).collect();
    assert_eq!(
        all_ids, expected,
        "shards must union to all rows exactly once (no dup, no loss)"
    );
}

/// `enumerate_shards` surfaces an error when the shard key can't be computed
/// (here: a non-existent column), and `apply_shard` rejects a malformed shard
/// descriptor — the error paths a coordinator must handle.
#[tokio::test(flavor = "multi_thread")]
async fn shard_error_paths() {
    use faucet_core::ShardSpec;
    use faucet_source_postgres::ShardConfig;

    let (_container, url) = start_postgres().await;
    seed_events(&url, 5).await;

    let mut config = PostgresSourceConfig::new(&url, "SELECT id FROM events");
    config.shard = Some(ShardConfig {
        key: "no_such_column".into(),
    });
    let source = PostgresSource::new(config).await.expect("source");
    // MIN/MAX over a non-existent column → SQL error → enumerate_shards errors.
    assert!(source.enumerate_shards(4).await.is_err());

    // A descriptor missing lo/hi is rejected by apply_shard.
    let bad = ShardSpec::new("0", serde_json::json!({ "key": "id" }));
    assert!(source.apply_shard(&bad).await.is_err());
}

/// A config without a `shard` block is not shardable: it enumerates to a single
/// whole-dataset shard, preserving single-worker behavior.
#[tokio::test(flavor = "multi_thread")]
async fn unsharded_config_enumerates_one_whole_shard() {
    let (_container, url) = start_postgres().await;
    seed_events(&url, 10).await;

    let source = PostgresSource::new(PostgresSourceConfig::new(&url, "SELECT id FROM events"))
        .await
        .expect("source");
    assert!(!source.is_shardable());
    let shards = source.enumerate_shards(4).await.expect("enumerate");
    assert_eq!(shards.len(), 1);
    assert!(shards[0].is_whole());
}

/// `discover()` enumerates real tables with column schemas, row estimates,
/// and a ready-to-run `query` config patch (#211).
#[tokio::test(flavor = "multi_thread")]
async fn discover_enumerates_tables_with_schemas() {
    let (_container, url) = start_postgres().await;
    let pool = sqlx::PgPool::connect(&url).await.expect("pool connect");
    sqlx::query("CREATE TABLE orders (id BIGINT PRIMARY KEY, note TEXT, total NUMERIC(10,2))")
        .execute(&pool)
        .await
        .expect("create orders");
    sqlx::query("CREATE SCHEMA sales")
        .execute(&pool)
        .await
        .expect("create schema");
    sqlx::query("CREATE TABLE sales.leads (id INT NOT NULL, active BOOLEAN)")
        .execute(&pool)
        .await
        .expect("create leads");
    sqlx::query("INSERT INTO orders (id) SELECT generate_series(1, 50)")
        .execute(&pool)
        .await
        .expect("seed orders");
    sqlx::query("ANALYZE orders")
        .execute(&pool)
        .await
        .expect("analyze");
    pool.close().await;

    let source = PostgresSource::new(PostgresSourceConfig::new(&url, "SELECT 1"))
        .await
        .expect("source");
    assert!(source.supports_discover());
    let datasets = source.discover().await.expect("discover");

    let names: Vec<&str> = datasets.iter().map(|d| d.name.as_str()).collect();
    assert!(names.contains(&"public.orders"), "got: {names:?}");
    assert!(names.contains(&"sales.leads"), "got: {names:?}");

    let orders = datasets
        .iter()
        .find(|d| d.name == "public.orders")
        .expect("orders dataset");
    assert_eq!(orders.kind, "table");
    assert_eq!(
        orders.config_patch["query"],
        r#"SELECT * FROM "public"."orders""#
    );
    assert_eq!(orders.estimated_rows, Some(50), "reltuples after ANALYZE");
    assert_eq!(orders.primary_key, Some(vec!["id".to_string()]));
    let leads = datasets.iter().find(|d| d.name == "sales.leads").unwrap();
    assert_eq!(leads.primary_key, None, "no PRIMARY KEY declared");
    let schema = orders.schema.as_ref().expect("schema");
    assert_eq!(schema["properties"]["id"]["type"], "integer");
    assert_eq!(
        schema["properties"]["total"]["type"],
        serde_json::json!(["number", "null"]),
        "total is declared without NOT NULL, so it is nullable-wrapped"
    );
    assert_eq!(
        schema["properties"]["note"]["type"],
        serde_json::json!(["string", "null"]),
        "nullable column"
    );
}
