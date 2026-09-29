#![allow(deprecated)]
//! Integration tests for `CsvSource::stream_pages`.
//!
//! These tests exercise the async line-streaming path end-to-end against
//! real temporary files. They are pure-filesystem so no external services or
//! containers are required.

use faucet_core::Source;
use faucet_source_csv::{CsvSource, CsvSourceConfig};
use futures::StreamExt;
use std::collections::HashMap;
use std::io::Write;
use std::time::Instant;
use tempfile::NamedTempFile;

/// Build a temp CSV with a single `id` column populated with `1..=n`.
fn seed_csv(n: usize) -> NamedTempFile {
    let mut tmp = NamedTempFile::new().expect("tempfile");
    writeln!(tmp, "id").expect("write header");
    for i in 1..=n {
        writeln!(tmp, "{i}").expect("write row");
    }
    tmp.flush().expect("flush");
    tmp
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_chunks_rows_into_batch_sized_pages() {
    let tmp = seed_csv(10_000);
    let config = CsvSourceConfig::new(tmp.path().to_str().unwrap()).with_batch_size(1000);
    let source = CsvSource::new(config);

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
            "csv source has no incremental mode; bookmark must be None"
        );
    }

    assert_eq!(page_count, 10, "10_000 / 1000 = 10 pages");
    assert_eq!(total_rows, 10_000);
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_partial_final_page() {
    let tmp = seed_csv(2_500);
    let config = CsvSourceConfig::new(tmp.path().to_str().unwrap()).with_batch_size(1000);
    let source = CsvSource::new(config);

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
    let tmp = seed_csv(2_500);
    let config = CsvSourceConfig::new(tmp.path().to_str().unwrap()).with_batch_size(0);
    let source = CsvSource::new(config);

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 0);

    let mut sizes = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        sizes.push(page.records.len());
    }
    assert_eq!(
        sizes,
        vec![2_500],
        "batch_size = 0 must drain the file and emit exactly one page"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_empty_file_yields_no_pages() {
    // Only a header — zero data rows.
    let mut tmp = NamedTempFile::new().unwrap();
    writeln!(tmp, "id,name").unwrap();
    tmp.flush().unwrap();

    let config = CsvSourceConfig::new(tmp.path().to_str().unwrap()).with_batch_size(1000);
    let source = CsvSource::new(config);

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 1000);

    let mut page_count = 0;
    while let Some(page) = pages.next().await {
        let _ = page.expect("page ok");
        page_count += 1;
    }
    assert_eq!(page_count, 0, "empty file must yield zero pages");
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_headerless_uses_positional_keys() {
    let mut tmp = NamedTempFile::new().unwrap();
    writeln!(tmp, "Alice,30").unwrap();
    writeln!(tmp, "Bob,25").unwrap();
    tmp.flush().unwrap();

    let config = CsvSourceConfig::new(tmp.path().to_str().unwrap())
        .has_headers(false)
        .with_batch_size(10);
    let source = CsvSource::new(config);

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 10);

    let mut all_records = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        all_records.extend(page.records);
    }

    assert_eq!(all_records.len(), 2);
    assert_eq!(all_records[0]["column_0"], "Alice");
    assert_eq!(all_records[0]["column_1"], "30");
    assert_eq!(all_records[1]["column_0"], "Bob");
    assert_eq!(all_records[1]["column_1"], "25");
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_preserves_multi_column_and_quoted_fields() {
    // Include a field with a comma inside double quotes — the per-line CSV
    // parser must split it correctly.
    let mut tmp = NamedTempFile::new().unwrap();
    writeln!(tmp, "id,name,note").unwrap();
    writeln!(tmp, "1,Alice,\"hello, world\"").unwrap();
    writeln!(tmp, "2,Bob,plain").unwrap();
    writeln!(tmp, "3,Carol,\"a,b,c\"").unwrap();
    tmp.flush().unwrap();

    let config = CsvSourceConfig::new(tmp.path().to_str().unwrap()).with_batch_size(2);
    let source = CsvSource::new(config);

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 2);

    let mut all_records = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        all_records.extend(page.records);
    }

    assert_eq!(all_records.len(), 3);
    assert_eq!(all_records[0]["id"], "1");
    assert_eq!(all_records[0]["name"], "Alice");
    assert_eq!(all_records[0]["note"], "hello, world");
    assert_eq!(all_records[1]["note"], "plain");
    assert_eq!(all_records[2]["note"], "a,b,c");
}

/// Catches the "buffered-then-chunked" anti-pattern.
///
/// The default `stream_pages` impl calls `fetch_with_context_incremental`,
/// which materialises every CSV row into a `Vec<Value>` before any page is
/// yielded. The true-streaming impl reads lines from the file lazily and
/// emits a page after `batch_size` are parsed.
///
/// For a large file, the read+parse cost dominates and the difference is
/// observable: dropping the stream after the first page in the streaming
/// impl avoids reading the remaining ~99% of lines.
#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_first_page_completes_without_parsing_full_file() {
    let tmp = seed_csv(200_000);
    let path = tmp.path().to_str().unwrap();

    // Time a full drain so we have a reference for "parse all rows".
    let source = CsvSource::new(CsvSourceConfig::new(path).with_batch_size(1000));
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
    let source = CsvSource::new(CsvSourceConfig::new(path).with_batch_size(1000));
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
        "first page should arrive without parsing the full file; \
         first page took {first_elapsed:?}, full drain took {full_elapsed:?}"
    );
}

#[tokio::test]
async fn null_values_map_listed_fields_to_null_but_never_headers() {
    let mut tmp = NamedTempFile::new().expect("tempfile");
    write!(tmp, "id,NULL,amount\n1,,NULL\n2,x,5\n").expect("write");
    tmp.flush().expect("flush");
    let config = CsvSourceConfig::new(tmp.path().to_str().unwrap())
        .null_values(vec![String::new(), "NULL".into()]);
    let source = CsvSource::new(config);
    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 1000);
    let mut rows = Vec::new();
    while let Some(page) = pages.next().await {
        rows.extend(page.expect("page ok").records);
    }
    assert_eq!(
        rows,
        vec![
            serde_json::json!({"id": "1", "NULL": null, "amount": null}),
            serde_json::json!({"id": "2", "NULL": "x", "amount": "5"}),
        ]
    );
}
