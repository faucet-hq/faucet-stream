//! Integration tests for `MysqlSource::stream_pages` against a real MySQL
//! instance via testcontainers.
//!
//! These tests require Docker. Each test boots its own container and seeds
//! its own table so they are fully isolated and safe to run in parallel.

use faucet_core::{DEFAULT_BATCH_SIZE, Source};
use faucet_source_mysql::{MysqlSource, MysqlSourceConfig};
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Instant;
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::mysql::Mysql;
use tokio::sync::Semaphore;

/// Bounds concurrent MySQL container startups across all tests in this
/// binary. MySQL 8.x init is heavy (~2-3 GB RSS per container during
/// startup) and starting 6 in parallel exhausts memory on Colima/Docker
/// Desktop, surfacing as random "Failed to start mysqld daemon" errors. We
/// allow at most two simultaneous startups; once a container is running it
/// is steady-state cheap, so the cap only serialises the spin-up window.
fn startup_limit() -> &'static Semaphore {
    static SEM: OnceLock<Semaphore> = OnceLock::new();
    SEM.get_or_init(|| Semaphore::new(2))
}

/// Start a MySQL container and return both the container handle and a
/// connection URL. The container is kept alive by the returned handle; drop
/// it to stop the container.
async fn start_mysql() -> (ContainerAsync<Mysql>, String) {
    let _permit = startup_limit()
        .acquire()
        .await
        .expect("startup semaphore closed");
    let image = Mysql::default();
    let container: ContainerAsync<Mysql> = image.start().await.expect("mysql container start");
    let port = container
        .get_host_port_ipv4(3306)
        .await
        .expect("mysql port");
    let url = format!("mysql://root@127.0.0.1:{port}/test");
    (container, url)
}

/// Create a single-column `events` table and insert `n` rows of `(id)` with
/// values `1..=n`. Uses a recursive CTE for a fast bulk insert — avoids
/// per-row round trips.
async fn seed_events(url: &str, n: i64) {
    use sqlx::Connection;

    let mut conn = sqlx::MySqlConnection::connect(url)
        .await
        .expect("connect for seed");
    sqlx::query("CREATE TABLE events (id BIGINT PRIMARY KEY)")
        .execute(&mut conn)
        .await
        .expect("create table");
    // MySQL 8 supports recursive CTEs; bump cte_max_recursion_depth so large
    // n works. The SET SESSION must run on the same connection as the
    // INSERT, hence the explicit single-connection seeding.
    sqlx::query("SET SESSION cte_max_recursion_depth = 1000000")
        .execute(&mut conn)
        .await
        .expect("set cte depth");
    sqlx::query(
        "INSERT INTO events (id) \
         WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < ?) \
         SELECT n FROM seq",
    )
    .bind(n)
    .execute(&mut conn)
    .await
    .expect("insert rows");
    conn.close().await.expect("close conn");
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_chunks_rows_into_batch_sized_pages() {
    let (_container, url) = start_mysql().await;
    seed_events(&url, 10_000).await;

    let config =
        MysqlSourceConfig::new(url, "SELECT id FROM events ORDER BY id").with_batch_size(1000);
    let source = MysqlSource::new(config).await.expect("source new");

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
            "mysql source has no incremental mode yet; bookmark must be None"
        );
    }

    assert_eq!(page_count, 10, "10_000 / 1000 = 10 pages");
    assert_eq!(total_rows, 10_000);
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_partial_final_page() {
    let (_container, url) = start_mysql().await;
    seed_events(&url, 2_500).await;

    let config =
        MysqlSourceConfig::new(url, "SELECT id FROM events ORDER BY id").with_batch_size(1000);
    let source = MysqlSource::new(config).await.expect("source new");

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
    let (_container, url) = start_mysql().await;
    seed_events(&url, 10_000).await;

    let config =
        MysqlSourceConfig::new(url, "SELECT id FROM events ORDER BY id").with_batch_size(0);
    let source = MysqlSource::new(config).await.expect("source new");

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
    let (_container, url) = start_mysql().await;
    // Create the table but insert no rows.
    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool connect");
    sqlx::query("CREATE TABLE events (id BIGINT PRIMARY KEY)")
        .execute(&pool)
        .await
        .expect("create table");
    pool.close().await;

    let config =
        MysqlSourceConfig::new(url, "SELECT id FROM events").with_batch_size(DEFAULT_BATCH_SIZE);
    let source = MysqlSource::new(config).await.expect("source new");

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
/// MySQL's wire protocol sends all rows from a simple `SELECT` in a single
/// response (without using a server-side cursor), so a `SLEEP`-style
/// server-side timing test would always look identical regardless of
/// client-side streaming.
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
    let (_container, url) = start_mysql().await;
    seed_events(&url, 200_000).await;

    // Time a full drain so we have a reference for "parse all rows".
    let config_full =
        MysqlSourceConfig::new(&url, "SELECT id FROM events ORDER BY id").with_batch_size(1000);
    let source = MysqlSource::new(config_full).await.expect("source new");
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
        MysqlSourceConfig::new(&url, "SELECT id FROM events ORDER BY id").with_batch_size(1000);
    let source = MysqlSource::new(config_first).await.expect("source new");
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
    let (_container, url) = start_mysql().await;
    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool connect");
    sqlx::query("CREATE TABLE items (id BIGINT PRIMARY KEY, name VARCHAR(64) NOT NULL)")
        .execute(&pool)
        .await
        .expect("create table");
    sqlx::query("INSERT INTO items (id, name) VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")
        .execute(&pool)
        .await
        .expect("insert");
    pool.close().await;

    let config =
        MysqlSourceConfig::new(url, "SELECT id, name FROM items ORDER BY id").with_batch_size(2);
    let source = MysqlSource::new(config).await.expect("source new");

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

/// Exercises the type arms of `mysql_value_to_json` by selecting one row whose
/// columns span the supported MySQL types. Without this, the converter's
/// float / temporal / decimal / blob / json branches were never executed (the
/// other tests only use BIGINT and VARCHAR).
#[tokio::test(flavor = "multi_thread")]
async fn all_column_types_decode_to_expected_json() {
    let (_container, url) = start_mysql().await;
    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool connect");
    sqlx::query(
        "CREATE TABLE types_t (
            jb JSON, t VARCHAR(32), big BIGINT,
            dp DOUBLE, fl FLOAT,
            dt DATETIME, d DATE, tm TIME,
            dec_col DECIMAL(10,2), bl BLOB
        )",
    )
    .execute(&pool)
    .await
    .expect("create table");
    sqlx::query(
        "INSERT INTO types_t VALUES (
            '{\"k\": 1}', 'hello', 9223372036854775807,
            3.5, 1.5,
            '2024-01-02 03:04:05', '2024-01-02', '03:04:05',
            123.45, x'68690a'
        )",
    )
    .execute(&pool)
    .await
    .expect("insert");
    pool.close().await;

    let config = MysqlSourceConfig::new(url, "SELECT * FROM types_t");
    let source = MysqlSource::new(config).await.expect("source new");
    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, DEFAULT_BATCH_SIZE);
    let page = pages.next().await.expect("one page").expect("page ok");
    assert_eq!(page.records.len(), 1);
    let row = &page.records[0];

    assert_eq!(row["jb"], serde_json::json!({"k": 1}));
    assert_eq!(row["t"], "hello");
    assert_eq!(row["big"], 9223372036854775807i64);
    assert_eq!(row["dp"], 3.5);
    assert_eq!(row["fl"], 1.5);
    // MySQL DATETIME decodes via sqlx as DateTime<Utc> -> RFC3339.
    assert!(
        row["dt"]
            .as_str()
            .unwrap()
            .starts_with("2024-01-02T03:04:05"),
        "DATETIME should render as RFC3339, got {:?}",
        row["dt"]
    );
    assert_eq!(row["d"], "2024-01-02");
    assert_eq!(row["tm"], "03:04:05");
    // DECIMAL -> string, preserving precision (assert the meaningful prefix to
    // be robust to any trailing-zero scale padding).
    assert!(
        row["dec_col"].as_str().unwrap().starts_with("123.45"),
        "DECIMAL should render as a precise decimal string, got {:?}",
        row["dec_col"]
    );
    assert_eq!(row["bl"], "aGkK"); // BLOB 0x68690a ("hi\n") -> base64
}

/// UNSIGNED integer columns (#264) must decode to JSON numbers, not bools or
/// nulls. Before the fix, sqlx's signed-int decoders rejected UNSIGNED columns
/// so they fell through to the `bool` probe (TINYINT UNSIGNED -> bool) or the
/// `Null` fall-through (wider UNSIGNED -> null) — total silent corruption.
///
/// This asserts every UNSIGNED width round-trips to its exact numeric value,
/// including a `BIGINT UNSIGNED` above `i64::MAX` (9223372036854775807) which a
/// signed decoder could never represent.
#[tokio::test(flavor = "multi_thread")]
async fn unsigned_columns_decode_to_json_numbers() {
    let (_container, url) = start_mysql().await;
    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool connect");
    sqlx::query(
        "CREATE TABLE unsigned_t (
            ti TINYINT UNSIGNED,
            si SMALLINT UNSIGNED,
            mi MEDIUMINT UNSIGNED,
            ii INT UNSIGNED,
            bi BIGINT UNSIGNED,
            flag TINYINT(1) UNSIGNED
        )",
    )
    .execute(&pool)
    .await
    .expect("create table");
    // Each column holds its max (or near-max) unsigned value:
    //   TINYINT UNSIGNED max = 255
    //   SMALLINT UNSIGNED max = 65535
    //   MEDIUMINT UNSIGNED max = 16777215
    //   INT UNSIGNED max = 4294967295
    //   BIGINT UNSIGNED: 18446744073709551000 (> i64::MAX = 9223372036854775807)
    //   flag: 1 — must stay the number 1, NOT decode to a JSON bool.
    sqlx::query(
        "INSERT INTO unsigned_t VALUES (255, 65535, 16777215, 4294967295, 18446744073709551000, 1)",
    )
    .execute(&pool)
    .await
    .expect("insert");
    pool.close().await;

    let config = MysqlSourceConfig::new(url, "SELECT * FROM unsigned_t");
    let source = MysqlSource::new(config).await.expect("source new");
    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, DEFAULT_BATCH_SIZE);
    let page = pages.next().await.expect("one page").expect("page ok");
    assert_eq!(page.records.len(), 1);
    let row = &page.records[0];

    assert_eq!(row["ti"], serde_json::json!(255u64));
    assert_eq!(row["si"], serde_json::json!(65535u64));
    assert_eq!(row["mi"], serde_json::json!(16777215u64));
    assert_eq!(row["ii"], serde_json::json!(4294967295u64));
    // The critical case: a BIGINT UNSIGNED above i64::MAX must round-trip as an
    // exact JSON number, never null.
    assert_eq!(row["bi"], serde_json::json!(18446744073709551000u64));
    assert!(
        row["bi"].is_number(),
        "BIGINT UNSIGNED above i64::MAX must be a JSON number, got {:?}",
        row["bi"]
    );
    // A TINYINT(1) UNSIGNED carrying 1 must stay the number 1, not become
    // `true` — the unsigned probes are deliberately ahead of the bool probe.
    assert_eq!(row["flag"], serde_json::json!(1u64));
    assert!(
        row["flag"].is_number(),
        "TINYINT(1) UNSIGNED must decode as a number, not a bool, got {:?}",
        row["flag"]
    );
}

/// Context tokens (`{key}`) must become `?` bind markers bound as native scalar
/// types — exercising `resolve_query`'s context branch and the typed arms of
/// `bind_params` (integer + bool).
#[tokio::test(flavor = "multi_thread")]
async fn context_tokens_bind_as_typed_params() {
    let (_container, url) = start_mysql().await;
    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool connect");
    sqlx::query("CREATE TABLE acct (id BIGINT, name VARCHAR(32), active BOOLEAN)")
        .execute(&pool)
        .await
        .expect("create table");
    sqlx::query("INSERT INTO acct VALUES (1, 'alice', true), (2, 'bob', false)")
        .execute(&pool)
        .await
        .expect("insert");
    pool.close().await;

    let config = MysqlSourceConfig::new(
        url,
        "SELECT name FROM acct WHERE id = {id} AND active = {active} ORDER BY name",
    );
    let source = MysqlSource::new(config).await.expect("source new");
    let mut ctx: HashMap<String, serde_json::Value> = HashMap::new();
    ctx.insert("id".into(), serde_json::json!(1));
    ctx.insert("active".into(), serde_json::json!(true));

    let mut pages = source.stream_pages(&ctx, DEFAULT_BATCH_SIZE);
    let page = pages.next().await.expect("one page").expect("page ok");
    assert_eq!(page.records.len(), 1, "only account id=1 is active");
    assert_eq!(page.records[0]["name"], "alice");
}

// ── PK-range sharding (Mode B, #262) ────────────────────────────────────────

/// The core Mode B correctness guarantee: enumerating a source into N shards and
/// reading each shard yields every row exactly once — no duplication, no loss.
#[tokio::test(flavor = "multi_thread")]
async fn shards_partition_rows_disjointly_and_completely() {
    use faucet_source_mysql::ShardConfig;

    let (_container, url) = start_mysql().await;
    seed_events(&url, 1000).await; // ids 1..=1000

    let mut config = MysqlSourceConfig::new(&url, "SELECT id FROM events");
    config.shard = Some(ShardConfig { key: "id".into() });

    // A coordinator enumerates the shard set.
    let coordinator = MysqlSource::new(config.clone())
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
        let s = MysqlSource::new(config.clone())
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
    use faucet_source_mysql::ShardConfig;

    let (_container, url) = start_mysql().await;
    seed_events(&url, 5).await;

    let mut config = MysqlSourceConfig::new(&url, "SELECT id FROM events");
    config.shard = Some(ShardConfig {
        key: "no_such_column".into(),
    });
    let source = MysqlSource::new(config).await.expect("source");
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
    let (_container, url) = start_mysql().await;
    seed_events(&url, 10).await;

    let source = MysqlSource::new(MysqlSourceConfig::new(&url, "SELECT id FROM events"))
        .await
        .expect("source");
    assert!(!source.is_shardable());
    let shards = source.enumerate_shards(4).await.expect("enumerate");
    assert_eq!(shards.len(), 1);
    assert!(shards[0].is_whole());
}

/// `discover()` enumerates real tables with column schemas, a row estimate
/// from `information_schema.tables.table_rows`, and a ready-to-run `query`
/// config patch (#211).
#[tokio::test(flavor = "multi_thread")]
async fn discover_enumerates_tables_with_schemas() {
    use sqlx::Connection;

    let (_container, url) = start_mysql().await;
    let mut conn = sqlx::MySqlConnection::connect(&url).await.expect("connect");
    sqlx::query(
        "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, note TEXT, total DECIMAL(10,2))",
    )
    .execute(&mut conn)
    .await
    .expect("create orders");
    sqlx::query("CREATE TABLE leads (id INT NOT NULL, active BOOLEAN)")
        .execute(&mut conn)
        .await
        .expect("create leads");
    sqlx::query("INSERT INTO orders (id) VALUES (1), (2), (3), (4), (5)")
        .execute(&mut conn)
        .await
        .expect("seed orders");
    // Refresh InnoDB statistics so table_rows carries a real estimate.
    sqlx::query("ANALYZE TABLE orders")
        .execute(&mut conn)
        .await
        .expect("analyze");
    conn.close().await.expect("close conn");

    let source = MysqlSource::new(MysqlSourceConfig::new(&url, "SELECT 1"))
        .await
        .expect("source");
    assert!(source.supports_discover());
    let datasets = source.discover().await.expect("discover");

    let names: Vec<&str> = datasets.iter().map(|d| d.name.as_str()).collect();
    assert!(names.contains(&"orders"), "got: {names:?}");
    assert!(names.contains(&"leads"), "got: {names:?}");

    let orders = datasets
        .iter()
        .find(|d| d.name == "orders")
        .expect("orders dataset");
    assert_eq!(orders.primary_key, Some(vec!["id".to_string()]));
    assert_eq!(
        datasets
            .iter()
            .find(|d| d.name == "leads")
            .unwrap()
            .primary_key,
        None
    );
    assert_eq!(orders.kind, "table");
    assert_eq!(orders.config_patch["query"], "SELECT * FROM `orders`");
    // InnoDB's table_rows is approximate; after ANALYZE on a 5-row table it
    // must be a small positive number (assert the range, not the exact
    // value, to stay robust across MySQL versions).
    let est = orders.estimated_rows.expect("table_rows estimate");
    assert!((1..=50).contains(&est), "estimate near 5, got {est}");
    let schema = orders.schema.as_ref().expect("schema");
    assert_eq!(schema["properties"]["id"]["type"], "integer");
    assert_eq!(
        schema["properties"]["total"]["type"],
        serde_json::json!(["number", "null"]),
        "nullable DECIMAL column"
    );
    assert_eq!(
        schema["properties"]["note"]["type"],
        serde_json::json!(["string", "null"]),
        "nullable TEXT column"
    );

    let leads = datasets
        .iter()
        .find(|d| d.name == "leads")
        .expect("leads dataset");
    let lschema = leads.schema.as_ref().expect("schema");
    assert_eq!(lschema["properties"]["id"]["type"], "integer");
    // MySQL's BOOLEAN is an alias for TINYINT(1); information_schema reports
    // data_type = "tinyint", which maps to JSON integer.
    assert_eq!(
        lschema["properties"]["active"]["type"],
        serde_json::json!(["integer", "null"])
    );
}
