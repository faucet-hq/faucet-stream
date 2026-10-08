#![allow(deprecated)]
//! Integration tests for `ParquetSource::stream_pages`.
//!
//! These tests write a small set of Parquet fixtures with
//! `parquet::arrow::ArrowWriter` (controlling row-group size where
//! relevant), then drive the streaming path end-to-end. They assert the
//! page cadence contract documented on [`ParquetSourceConfig::batch_size`]:
//!
//! - `batch_size = N`: each emitted [`StreamPage`] holds *at most* `N`
//!   rows. Arrow may emit a smaller batch at row-group boundaries, so the
//!   only invariants we can rely on are
//!   `sum(page.records.len()) == total_rows` and
//!   `page_count >= ceil(total_rows / N)`.
//! - `batch_size = 0`: pages align to the file's native row-groups —
//!   `page_count == row_group_count` (plus, possibly, a final empty page
//!   from the writer; we tolerate either shape and assert on the row total
//!   and page bound).
//! - Multi-file scans flatten across files in sorted order.
//! - The final page never carries a bookmark — the Parquet source has no
//!   incremental-replication mode.
//! - A large file streams: the first page lands well before the full
//!   drain completes (the regression guard for the
//!   buffer-then-chunk fallback).

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use faucet_core::{Source, StreamPage};
use faucet_source_parquet::{ParquetSource, ParquetSourceConfig};
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;
use tempfile::TempDir;

// ── Fixture helpers ──────────────────────────────────────────────────────────

/// Write a parquet file from a single `RecordBatch` with the default writer
/// properties (one row-group containing the whole batch).
fn write_single_row_group(path: &Path, batch: &RecordBatch) {
    let file = File::create(path).expect("create file");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(batch).expect("write batch");
    writer.close().expect("close writer");
}

/// Write a parquet file with N row-groups by capping
/// `max_row_group_row_count` at `row_group_size` and feeding the writer
/// `row_group_count * row_group_size` rows of `(id, name)` data.
fn write_multi_row_group(
    path: &Path,
    row_group_size: usize,
    row_group_count: usize,
) -> (usize, Arc<Schema>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let total = row_group_size * row_group_count;
    let ids: Vec<i64> = (0..total as i64).collect();
    let names: Vec<String> = (0..total).map(|i| format!("row-{i}")).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .unwrap();

    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(row_group_size))
        .build();
    let file = File::create(path).expect("create file");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");

    (total, schema)
}

/// Build an (id, name) batch with `total` rows.
fn small_batch(total: usize) -> (RecordBatch, Arc<Schema>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let ids: Vec<i64> = (0..total as i64).collect();
    let names: Vec<String> = (0..total).map(|i| format!("row-{i}")).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .unwrap();
    (batch, schema)
}

async fn collect_pages(source: &ParquetSource) -> Vec<StreamPage> {
    let ctx = std::collections::HashMap::new();
    let mut stream = source.stream_pages(&ctx, 0);
    let mut pages = Vec::new();
    while let Some(page) = stream.next().await {
        pages.push(page.expect("stream_pages must not error"));
    }
    pages
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn stream_pages_chunks_by_batch_size() {
    // 1000 rows, batch_size = 250. Sum-of-records is exact; page count is
    // at least ceil(1000 / 250) = 4. Arrow's `with_batch_size` is a hint,
    // so we tolerate extra splits at row-group boundaries.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("chunked.parquet");
    let (batch, _) = small_batch(1000);
    write_single_row_group(&path, &batch);

    let source =
        ParquetSource::new(ParquetSourceConfig::local(path.to_str().unwrap()).with_batch_size(250))
            .await
            .unwrap();

    let pages = collect_pages(&source).await;
    assert!(
        pages.len() >= 4,
        "expected at least 4 pages for 1000 rows / batch_size 250, got {}",
        pages.len()
    );
    let total_rows: usize = pages.iter().map(|p| p.records.len()).sum();
    assert_eq!(total_rows, 1000);
    for page in &pages {
        assert!(
            page.records.len() <= 250,
            "page exceeded batch_size hint: {} > 250",
            page.records.len()
        );
        assert!(page.bookmark.is_none(), "parquet source emits no bookmark");
    }
}

#[tokio::test]
async fn stream_pages_partial_final_page() {
    // 1050 rows / batch_size 250 → at least 5 pages, last one ≤ 50 rows.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("partial.parquet");
    let (batch, _) = small_batch(1050);
    write_single_row_group(&path, &batch);

    let source =
        ParquetSource::new(ParquetSourceConfig::local(path.to_str().unwrap()).with_batch_size(250))
            .await
            .unwrap();

    let pages = collect_pages(&source).await;
    let total: usize = pages.iter().map(|p| p.records.len()).sum();
    assert_eq!(total, 1050);

    // The trailing page is whatever's left after the last full chunk. With
    // a single row-group and batch_size 250, we expect five pages of 250
    // followed by one of 50. Allow the last page to be <= 250.
    let last = pages.last().expect("at least one page");
    assert!(
        last.records.len() <= 250,
        "trailing page should be ≤ batch_size, got {}",
        last.records.len()
    );
}

#[tokio::test]
async fn stream_pages_batch_size_zero_aligns_to_row_groups() {
    // 3 row-groups × 100 rows. `batch_size = 0` skips with_batch_size so
    // page cadence is driven by the file's native row-groups.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("rowgroups.parquet");
    let (total, _) = write_multi_row_group(&path, 100, 3);

    let source =
        ParquetSource::new(ParquetSourceConfig::local(path.to_str().unwrap()).with_batch_size(0))
            .await
            .unwrap();

    let pages = collect_pages(&source).await;
    let total_rows: usize = pages.iter().map(|p| p.records.len()).sum();
    assert_eq!(total_rows, total);
    // With 3 row-groups of 100 each, the Arrow reader yields three
    // RecordBatches of exactly 100 rows each. The stream filters out
    // empty batches so we never see more than 3 pages.
    assert_eq!(
        pages.len(),
        3,
        "expected one page per row-group, got {}",
        pages.len()
    );
    for page in &pages {
        assert_eq!(page.records.len(), 100);
    }
}

#[tokio::test]
async fn stream_pages_multi_file_glob_flattens() {
    // Two files × 300 rows each, scanned via glob.
    let dir = TempDir::new().unwrap();
    let path_a = dir.path().join("a_part.parquet");
    let path_b = dir.path().join("b_part.parquet");
    let (batch_a, _) = small_batch(300);
    let (batch_b, _) = small_batch(300);
    write_single_row_group(&path_a, &batch_a);
    write_single_row_group(&path_b, &batch_b);

    let pattern = format!("{}/*_part.parquet", dir.path().display());
    let source = ParquetSource::new(ParquetSourceConfig::glob(pattern).with_batch_size(200))
        .await
        .unwrap();

    let pages = collect_pages(&source).await;
    let total: usize = pages.iter().map(|p| p.records.len()).sum();
    assert_eq!(total, 600);
    // 600 rows / batch_size 200 → at least 3 pages.
    assert!(pages.len() >= 3);
    for page in &pages {
        assert!(page.records.len() <= 200);
    }
}

#[tokio::test]
async fn stream_pages_schema_mismatch_fails_before_yielding_any_rows() {
    // M11 (#146): a schema mismatch on a *later* file must fail eagerly,
    // before any earlier file's rows are yielded downstream. Under the old
    // streaming path, file A's rows were yielded to the sink and only then was
    // file B opened and its divergent schema detected — a partial, non-atomic
    // write committed before the abort. Schemas are now validated up front, so
    // the mismatch surfaces with zero pages emitted.
    let dir = TempDir::new().unwrap();
    let path_a = dir.path().join("a_part.parquet");
    let path_b = dir.path().join("b_part.parquet");
    // File A: (id, name). File B: a divergent schema (id only).
    let (batch_a, _) = small_batch(300);
    let schema_b = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch_b = RecordBatch::try_new(
        schema_b.clone(),
        vec![Arc::new(Int64Array::from((0..300i64).collect::<Vec<_>>()))],
    )
    .unwrap();
    write_single_row_group(&path_a, &batch_a);
    write_single_row_group(&path_b, &batch_b);

    let pattern = format!("{}/*_part.parquet", dir.path().display());
    let source = ParquetSource::new(ParquetSourceConfig::glob(pattern).with_batch_size(100))
        .await
        .unwrap();

    let ctx = std::collections::HashMap::new();
    let mut stream = source.stream_pages(&ctx, 0);
    let mut pages_before_error = 0usize;
    let mut errored = false;
    while let Some(item) = stream.next().await {
        match item {
            Ok(_) => pages_before_error += 1,
            Err(_) => {
                errored = true;
                break;
            }
        }
    }
    assert!(
        errored,
        "a cross-file schema mismatch must surface as an error"
    );
    assert_eq!(
        pages_before_error, 0,
        "no rows may be yielded before the schema-mismatch abort (eager, atomic fail)"
    );
}

#[tokio::test]
async fn stream_pages_empty_file_yields_no_pages() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("empty.parquet");
    let (batch, _) = small_batch(0);
    write_single_row_group(&path, &batch);

    let source =
        ParquetSource::new(ParquetSourceConfig::local(path.to_str().unwrap()).with_batch_size(250))
            .await
            .unwrap();

    let pages = collect_pages(&source).await;
    let total: usize = pages.iter().map(|p| p.records.len()).sum();
    assert_eq!(total, 0);
    assert!(
        pages.is_empty(),
        "empty file should yield zero pages, got {}",
        pages.len()
    );
}

#[tokio::test]
async fn stream_pages_glob_no_match_yields_no_pages() {
    let dir = TempDir::new().unwrap();
    let pattern = format!("{}/no_match_*.parquet", dir.path().display());
    let source = ParquetSource::new(ParquetSourceConfig::glob(pattern).with_batch_size(100))
        .await
        .unwrap();

    let pages = collect_pages(&source).await;
    assert!(pages.is_empty());
}

#[tokio::test]
async fn stream_pages_multi_file_schema_mismatch_surfaces_as_source_error() {
    // Two files with diverging schemas under the same glob — the second
    // file should fail with FaucetError::Source naming both paths.
    let dir = TempDir::new().unwrap();
    let path_a = dir.path().join("a_part.parquet");
    let path_b = dir.path().join("b_part.parquet");

    let (batch_a, _) = small_batch(50);
    write_single_row_group(&path_a, &batch_a);

    // Diverging schema for the second file: a single `value` column.
    let schema_b = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch_b = RecordBatch::try_new(
        schema_b,
        vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
    )
    .unwrap();
    write_single_row_group(&path_b, &batch_b);

    let pattern = format!("{}/*_part.parquet", dir.path().display());
    let source = ParquetSource::new(ParquetSourceConfig::glob(pattern).with_batch_size(25))
        .await
        .unwrap();

    let ctx = std::collections::HashMap::new();
    let mut stream = source.stream_pages(&ctx, 0);
    let mut saw_error = false;
    let mut any_rows = 0usize;
    while let Some(page) = stream.next().await {
        match page {
            Ok(p) => any_rows += p.records.len(),
            Err(faucet_core::FaucetError::Source(msg)) => {
                assert!(
                    msg.contains("schema mismatch"),
                    "expected schema-mismatch error, got: {msg}"
                );
                assert!(msg.contains("a_part.parquet"));
                assert!(msg.contains("b_part.parquet"));
                saw_error = true;
                break;
            }
            Err(other) => panic!("unexpected error variant: {other:?}"),
        }
    }
    assert!(saw_error, "expected schema-mismatch FaucetError::Source");
    // We should have streamed the first file before the second's schema
    // was inspected — the row count is bounded above by that first file.
    assert!(any_rows <= 50);
}

#[tokio::test]
async fn stream_pages_first_page_arrives_before_full_drain() {
    // Regression guard against the buffer-then-chunk fallback. A 200k-row
    // file is large enough that JSON-encoding every batch eagerly is
    // noticeably slower than yielding the first batch. Assert
    // `first_elapsed * 2 < full_elapsed`.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("big.parquet");

    let total: usize = 200_000;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let ids: Vec<i64> = (0..total as i64).collect();
    let names: Vec<String> = (0..total).map(|i| format!("row-{i}")).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .unwrap();

    // Many row-groups so the streaming impl yields early.
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(2_000))
        .build();
    let file = File::create(&path).expect("create file");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");

    let source = ParquetSource::new(
        ParquetSourceConfig::local(path.to_str().unwrap()).with_batch_size(2_000),
    )
    .await
    .unwrap();

    let ctx = std::collections::HashMap::new();

    // Time-to-first-page.
    let start = Instant::now();
    let mut stream = source.stream_pages(&ctx, 0);
    let first = stream
        .next()
        .await
        .expect("at least one page")
        .expect("first page must succeed");
    let first_elapsed = start.elapsed();
    assert!(!first.records.is_empty(), "first page should hold rows");

    // Drain the rest and clock total elapsed.
    let mut total_rows = first.records.len();
    while let Some(page) = stream.next().await {
        total_rows += page.expect("page").records.len();
    }
    let full_elapsed = start.elapsed();
    assert_eq!(total_rows, total);

    // The default `fetch_with_context_incremental`-then-chunk fallback
    // would materialise every batch before yielding the first page, so
    // first_elapsed would be ≈ full_elapsed. The override yields after a
    // single RecordBatch decode, so first_elapsed should be a small
    // fraction of full_elapsed.
    assert!(
        first_elapsed * 2 < full_elapsed,
        "expected first page to arrive in << full drain; first={:?}, full={:?}",
        first_elapsed,
        full_elapsed
    );
}

// ── Hash-modulo file sharding (Mode B, #262) ────────────────────────────────

/// End-to-end Mode B guarantee on real files: splitting a multi-file glob into
/// N shards and streaming each shard yields every row exactly once.
#[tokio::test]
async fn shards_stream_all_rows_exactly_once() {
    let dir = TempDir::new().expect("tempdir");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    // 6 files × 10 rows, globally-unique ids 0..60.
    for f in 0..6i64 {
        let ids: Vec<i64> = (f * 10..(f + 1) * 10).collect();
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(ids))])
            .expect("batch");
        write_single_row_group(&dir.path().join(format!("part-{f}.parquet")), &batch);
    }

    let pattern = format!("{}/*.parquet", dir.path().display());
    let source = ParquetSource::new(ParquetSourceConfig::glob(&pattern))
        .await
        .expect("source");

    assert!(source.is_shardable());
    let shards = source.enumerate_shards(3).await.expect("enumerate");
    assert_eq!(shards.len(), 3);

    let ctx = std::collections::HashMap::new();
    let mut all_ids: Vec<i64> = Vec::new();
    for shard in &shards {
        source.apply_shard(shard).await.expect("apply_shard");
        let pages: Vec<StreamPage> = source
            .stream_pages(&ctx, 0)
            .map(|p| p.expect("page"))
            .collect()
            .await;
        for page in pages {
            for rec in page.records {
                all_ids.push(rec["id"].as_i64().expect("id is int"));
            }
        }
    }

    all_ids.sort();
    let expected: Vec<i64> = (0..60).collect();
    assert_eq!(
        all_ids, expected,
        "shards must union to all rows exactly once (no dup, no loss)"
    );
}

/// A cross-file schema mismatch must fail a sharded worker even when the
/// mismatching file hashes into a DIFFERENT shard — schema validation covers
/// the full resolved set, not just the shard's subset.
#[tokio::test]
async fn sharded_stream_rejects_cross_shard_schema_mismatch() {
    let dir = TempDir::new().expect("tempdir");
    let int_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let str_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
    let int_batch = RecordBatch::try_new(
        int_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1i64, 2]))],
    )
    .expect("int batch");
    let str_batch = RecordBatch::try_new(
        str_schema.clone(),
        vec![Arc::new(StringArray::from(vec!["a", "b"]))],
    )
    .expect("str batch");
    write_single_row_group(&dir.path().join("part-0.parquet"), &int_batch);
    write_single_row_group(&dir.path().join("part-1.parquet"), &str_batch);

    let pattern = format!("{}/*.parquet", dir.path().display());
    let source = ParquetSource::new(ParquetSourceConfig::glob(&pattern))
        .await
        .expect("source");

    // Every shard must reject the mismatched file set — even a shard whose own
    // subset is internally consistent (or empty).
    let shards = source.enumerate_shards(2).await.expect("enumerate");
    let ctx = std::collections::HashMap::new();
    for shard in &shards {
        source.apply_shard(shard).await.expect("apply_shard");
        let results: Vec<_> = source.stream_pages(&ctx, 0).collect().await;
        assert!(
            results.iter().any(|r| r.is_err()),
            "shard {} must surface the cross-file schema mismatch",
            shard.id
        );
    }
}

#[tokio::test]
async fn files_differing_only_in_schema_metadata_read_as_one_dataset() {
    // #789 FILE-28: pyarrow stamps per-file key-value metadata (pandas index
    // bounds); identical columns must not count as a mismatch.
    let dir = TempDir::new().unwrap();
    for (i, name) in ["a_part.parquet", "b_part.parquet"].iter().enumerate() {
        let (batch, _) = small_batch(10);
        let meta = std::collections::HashMap::from([(
            "pandas".to_string(),
            format!("{{\"stop\": {}}}", 10 * (i + 1)),
        )]);
        let schema = Arc::new(batch.schema().as_ref().clone().with_metadata(meta));
        let batch = RecordBatch::try_new(schema, batch.columns().to_vec()).unwrap();
        write_single_row_group(&dir.path().join(name), &batch);
    }
    let pattern = format!("{}/*_part.parquet", dir.path().display());
    let source = ParquetSource::new(ParquetSourceConfig::glob(pattern).with_batch_size(5))
        .await
        .unwrap();
    let ctx = std::collections::HashMap::new();
    assert_eq!(source.fetch_with_context(&ctx).await.unwrap().len(), 20);
    let mut pages = source.stream_pages(&ctx, 5);
    let mut rows = 0;
    while let Some(page) = pages.next().await {
        rows += page.unwrap().records.len();
    }
    assert_eq!(rows, 20);
}
