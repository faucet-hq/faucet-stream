//! Integration tests for `SqliteSource::stream_pages` against a real SQLite
//! database on disk.
//!
//! SQLite is file-based and in-process, so no testcontainers are needed —
//! each test creates its own `tempfile::NamedTempFile`, opens a pool to seed
//! it, drops the seed pool, then constructs a `SqliteSource` against the
//! same file path so both halves see the same data.

use faucet_core::{DEFAULT_BATCH_SIZE, Source};
use faucet_source_sqlite::{SqliteSource, SqliteSourceConfig};
use futures::StreamExt;
use std::collections::HashMap;
use std::time::Instant;
use tempfile::NamedTempFile;

/// Build a `sqlite:` URL that creates the file if missing and points at the
/// given absolute path. `mode=rwc` lets sqlx open a brand-new file without
/// erroring on first connect.
fn sqlite_url(path: &std::path::Path) -> String {
    format!("sqlite:{}?mode=rwc", path.display())
}

/// Create a single-column `events` table and insert `n` rows of `(id)` with
/// values `1..=n`. The seed pool is closed before returning so the temp file
/// is no longer locked by us.
async fn seed_events(url: &str, n: i64) {
    let pool = sqlx::SqlitePool::connect(url)
        .await
        .expect("seed pool connect");
    sqlx::query("CREATE TABLE events (id INTEGER PRIMARY KEY)")
        .execute(&pool)
        .await
        .expect("create table");
    // Use a recursive CTE for a fast bulk insert — avoids `n` round-trips.
    sqlx::query(
        "WITH RECURSIVE seq(n) AS (\
             SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < ?\
         ) INSERT INTO events (id) SELECT n FROM seq",
    )
    .bind(n)
    .execute(&pool)
    .await
    .expect("insert rows");
    pool.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_chunks_rows_into_batch_sized_pages() {
    let tmp = NamedTempFile::new().expect("tempfile");
    let url = sqlite_url(tmp.path());
    seed_events(&url, 10_000).await;

    let config =
        SqliteSourceConfig::new(&url, "SELECT id FROM events ORDER BY id").with_batch_size(1000);
    let source = SqliteSource::new(config).await.expect("source new");

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
            "sqlite source has no incremental mode yet; bookmark must be None"
        );
    }

    assert_eq!(page_count, 10, "10_000 / 1000 = 10 pages");
    assert_eq!(total_rows, 10_000);
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_partial_final_page() {
    let tmp = NamedTempFile::new().expect("tempfile");
    let url = sqlite_url(tmp.path());
    seed_events(&url, 2_500).await;

    let config =
        SqliteSourceConfig::new(&url, "SELECT id FROM events ORDER BY id").with_batch_size(1000);
    let source = SqliteSource::new(config).await.expect("source new");

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
    let tmp = NamedTempFile::new().expect("tempfile");
    let url = sqlite_url(tmp.path());
    seed_events(&url, 10_000).await;

    let config =
        SqliteSourceConfig::new(&url, "SELECT id FROM events ORDER BY id").with_batch_size(0);
    let source = SqliteSource::new(config).await.expect("source new");

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
    let tmp = NamedTempFile::new().expect("tempfile");
    let url = sqlite_url(tmp.path());

    // Create the table but insert no rows.
    let pool = sqlx::SqlitePool::connect(&url)
        .await
        .expect("seed pool connect");
    sqlx::query("CREATE TABLE events (id INTEGER PRIMARY KEY)")
        .execute(&pool)
        .await
        .expect("create table");
    pool.close().await;

    let config =
        SqliteSourceConfig::new(&url, "SELECT id FROM events").with_batch_size(DEFAULT_BATCH_SIZE);
    let source = SqliteSource::new(config).await.expect("source new");

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

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_preserves_row_contents() {
    let tmp = NamedTempFile::new().expect("tempfile");
    let url = sqlite_url(tmp.path());

    let pool = sqlx::SqlitePool::connect(&url)
        .await
        .expect("seed pool connect");
    sqlx::query("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        .execute(&pool)
        .await
        .expect("create table");
    sqlx::query("INSERT INTO items (id, name) VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")
        .execute(&pool)
        .await
        .expect("insert");
    pool.close().await;

    let config =
        SqliteSourceConfig::new(&url, "SELECT id, name FROM items ORDER BY id").with_batch_size(2);
    let source = SqliteSource::new(config).await.expect("source new");

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

/// Catches the "buffered-then-chunked" anti-pattern.
///
/// The default `stream_pages` impl calls `fetch_with_context_incremental`
/// which materialises every row into a `Vec<Value>` before any page is
/// yielded, while the true-streaming impl parses rows from the sqlx cursor
/// and yields after `batch_size` are buffered.
///
/// For a large result, the parse-and-buffer cost dominates and the
/// difference is observable: dropping the stream after the first page in the
/// streaming impl avoids parsing the remaining ~99% of rows. SQLite is
/// in-process so there is no wire latency to muddy the comparison — the
/// signal is purely about how much CPU work happens before the first
/// `StreamPage` is yielded.
#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_first_page_completes_without_parsing_full_result() {
    let tmp = NamedTempFile::new().expect("tempfile");
    let url = sqlite_url(tmp.path());
    seed_events(&url, 200_000).await;

    // Time a full drain so we have a reference for "parse all rows".
    let config_full =
        SqliteSourceConfig::new(&url, "SELECT id FROM events ORDER BY id").with_batch_size(1000);
    let source = SqliteSource::new(config_full).await.expect("source new");
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
        SqliteSourceConfig::new(&url, "SELECT id FROM events ORDER BY id").with_batch_size(1000);
    let source = SqliteSource::new(config_first).await.expect("source new");
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

/// `discover()` enumerates real tables from a database file with column
/// schemas, nullability, and a ready-to-run `query` config patch (#211).
/// SQLite exposes no cheap row-count statistic, so `estimated_rows` is
/// always absent — discovery must never scan data to count.
#[tokio::test(flavor = "multi_thread")]
async fn discover_enumerates_tables_with_schemas() {
    let tmp = NamedTempFile::new().expect("tempfile");
    let url = sqlite_url(tmp.path());

    let pool = sqlx::SqlitePool::connect(&url).await.expect("seed pool");
    sqlx::query(
        "CREATE TABLE orders (id INTEGER NOT NULL PRIMARY KEY, note TEXT, total REAL NOT NULL)",
    )
    .execute(&pool)
    .await
    .expect("create orders");
    sqlx::query("CREATE TABLE leads (id INTEGER NOT NULL, active BOOLEAN)")
        .execute(&pool)
        .await
        .expect("create leads");
    sqlx::query("INSERT INTO orders (id, total) VALUES (1, 9.5), (2, 3.25)")
        .execute(&pool)
        .await
        .expect("seed orders");
    pool.close().await;

    let source = SqliteSource::new(SqliteSourceConfig::new(&url, "SELECT 1"))
        .await
        .expect("source");
    assert!(source.supports_discover());
    let datasets = source.discover().await.expect("discover");

    let names: Vec<&str> = datasets.iter().map(|d| d.name.as_str()).collect();
    assert_eq!(names, vec!["leads", "orders"], "ordered by table name");

    let orders = datasets
        .iter()
        .find(|d| d.name == "orders")
        .expect("orders dataset");
    assert_eq!(orders.kind, "table");
    assert_eq!(orders.config_patch["query"], "SELECT * FROM `orders`");
    assert_eq!(
        orders.estimated_rows, None,
        "no cheap estimate in SQLite — discovery never scans"
    );
    let schema = orders.schema.as_ref().expect("schema");
    assert_eq!(schema["properties"]["id"]["type"], "integer");
    assert_eq!(schema["properties"]["total"]["type"], "number");
    assert_eq!(
        schema["properties"]["note"]["type"],
        serde_json::json!(["string", "null"]),
        "nullable column"
    );

    let leads = datasets
        .iter()
        .find(|d| d.name == "leads")
        .expect("leads dataset");
    let lschema = leads.schema.as_ref().expect("schema");
    assert_eq!(lschema["properties"]["id"]["type"], "integer");
    // The declared type "BOOLEAN" is preserved by pragma_table_info and maps
    // to JSON boolean (SQLite stores it with NUMERIC affinity, but the
    // declared name is what the catalog reports).
    assert_eq!(
        lschema["properties"]["active"]["type"],
        serde_json::json!(["boolean", "null"])
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn null_cells_of_every_storage_class_decode_as_null() {
    let tmp = NamedTempFile::new().expect("tempfile");
    let url = sqlite_url(tmp.path());
    let pool = sqlx::SqlitePool::connect(&url).await.expect("seed pool");
    sqlx::query(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c REAL, d BLOB, e BOOLEAN)",
    )
    .execute(&pool)
    .await
    .expect("create");
    sqlx::query("INSERT INTO t VALUES (1, NULL, NULL, NULL, NULL, NULL)")
        .execute(&pool)
        .await
        .expect("insert nulls");
    sqlx::query("INSERT INTO t VALUES (2, 'x', 7, 1.5, X'0102', 1)")
        .execute(&pool)
        .await
        .expect("insert values");
    sqlx::query("INSERT INTO t VALUES (3, '{\"k\":1}', 0, 2.0, X'', 0)")
        .execute(&pool)
        .await
        .expect("insert json");
    pool.close().await;

    let source = SqliteSource::new(SqliteSourceConfig::new(
        &url,
        "SELECT id, a, b, c, d, e FROM t ORDER BY id",
    ))
    .await
    .expect("source");
    let rows = source.fetch_all().await.expect("fetch");
    assert_eq!(
        rows[0],
        serde_json::json!({"id": 1, "a": null, "b": null, "c": null, "d": null, "e": null})
    );
    assert_eq!(
        rows[1],
        serde_json::json!({"id": 2, "a": "x", "b": 7, "c": 1.5, "d": "AQI=", "e": 1})
    );
    assert_eq!(
        rows[2],
        serde_json::json!({"id": 3, "a": {"k": 1}, "b": 0, "c": 2.0, "d": "", "e": 0})
    );
}
