#![allow(deprecated)]
//! Additional coverage tests for `faucet-source-parquet`.
//!
//! These exercise branches not covered by `round_trip.rs` / `streaming.rs`:
//! the metadata-read error path on a non-Parquet file, `config_schema()`, the
//! S3 `dataset_uri` prefix / bucket-only branches (pure config, no network),
//! the streaming projection path on a single local file, and glob expansion
//! that skips directory entries. Fixtures are written with the raw
//! `parquet::arrow::ArrowWriter` (no dependency on `faucet-sink-parquet`),
//! mirroring the existing test style.

use std::sync::Arc;

use arrow::array::{Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use faucet_core::{Source, StreamPage};
use faucet_source_parquet::{ParquetS3Config, ParquetSource, ParquetSourceConfig};
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::path::Path;
use tempfile::TempDir;

fn write_parquet(path: &Path, batch: &RecordBatch) {
    let file = File::create(path).expect("create file");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(batch).expect("write batch");
    writer.close().expect("close writer");
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

/// Columnar fast path (feature `arrow`, RFC 0002 / #375): the source advertises
/// `supports_columnar` and `stream_batches` yields the file's Arrow
/// `RecordBatch`es directly (no JSON conversion), each as a bookmark-less
/// `ColumnarPage`.
#[cfg(feature = "arrow")]
#[tokio::test]
async fn stream_batches_yields_columnar_pages() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("data.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])),
        ],
    )
    .unwrap();
    write_parquet(&path, &batch);

    let source = ParquetSource::new(ParquetSourceConfig::local(path.to_str().unwrap()))
        .await
        .unwrap();
    assert!(
        source.supports_columnar(),
        "parquet source is columnar-capable"
    );

    let ctx = std::collections::HashMap::new();
    let mut stream = source.stream_batches(&ctx, 0);
    let mut pages = Vec::new();
    while let Some(p) = stream.next().await {
        pages.push(p.expect("stream_batches must not error"));
    }

    let total_rows: usize = pages.iter().map(|p| p.num_rows()).sum();
    assert_eq!(total_rows, 3, "all rows streamed via the columnar path");
    assert!(
        pages.iter().all(|p| p.bookmark.is_none()),
        "parquet has no incremental mode, so every columnar page is bookmark-less"
    );
    // Schema/columns preserved end-to-end — no Value round-trip.
    let first = &pages[0].batch;
    assert_eq!(first.num_columns(), 2);
    assert_eq!(first.schema().field(0).name(), "id");
}

#[tokio::test]
async fn non_parquet_file_surfaces_metadata_read_error() {
    // A file that exists but is not valid Parquet must fail at metadata-read
    // time with a `FaucetError::Source` naming the file — not panic.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("garbage.parquet");
    std::fs::write(&path, b"this is not a parquet file at all").unwrap();

    let source = ParquetSource::new(ParquetSourceConfig::local(path.to_str().unwrap()))
        .await
        .unwrap();
    let err = source.fetch_all().await.expect_err("should fail");
    match err {
        faucet_core::FaucetError::Source(msg) => {
            assert!(
                msg.contains("garbage.parquet"),
                "error must name the file: {msg}"
            );
        }
        other => panic!("expected Source error, got {other:?}"),
    }
}

#[tokio::test]
async fn non_parquet_file_surfaces_error_in_stream_pages() {
    // Same corrupt-file failure mode, but driven through the streaming path:
    // `open_target_stream` fails during the up-front schema-validation pass.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("garbage.parquet");
    std::fs::write(&path, b"definitely not parquet").unwrap();

    let source = ParquetSource::new(ParquetSourceConfig::local(path.to_str().unwrap()))
        .await
        .unwrap();
    let ctx = std::collections::HashMap::new();
    let mut stream = source.stream_pages(&ctx, 0);
    let mut errored = false;
    let mut pages = 0usize;
    while let Some(item) = stream.next().await {
        match item {
            Ok(_) => pages += 1,
            Err(faucet_core::FaucetError::Source(msg)) => {
                assert!(msg.contains("garbage.parquet"), "names file: {msg}");
                errored = true;
                break;
            }
            Err(other) => panic!("unexpected error: {other:?}"),
        }
    }
    assert!(errored, "corrupt file must surface as a streaming error");
    assert_eq!(pages, 0, "no pages may be yielded before the error");
}

#[tokio::test]
async fn config_schema_serializes_to_object() {
    // `config_schema()` must serialise the JSON Schema without panicking.
    let source = ParquetSource::new(ParquetSourceConfig::local("/tmp/x.parquet"))
        .await
        .unwrap();
    let schema = source.config_schema();
    assert!(schema.is_object(), "schema should be a JSON object");
    // The schema describes the `source` discriminated union field.
    assert!(
        schema.get("properties").is_some() || schema.get("$ref").is_some(),
        "schema should expose properties or a $ref: {schema}"
    );
}

#[tokio::test]
async fn dataset_uri_s3_prefix_and_bucket_only() {
    // The S3 prefix branch of `dataset_uri`.
    let s3 = ParquetS3Config::prefix("my-bucket", "events/2024/");
    let source = ParquetSource::new(ParquetSourceConfig::s3(s3))
        .await
        .unwrap();
    assert_eq!(source.dataset_uri(), "s3://my-bucket/events/2024/");

    // The bucket-only branch (neither key nor prefix set). `new()` succeeds —
    // the key/prefix requirement is enforced at resolve time, not construction.
    let s3 = ParquetS3Config {
        bucket: "only-bucket".into(),
        key: None,
        prefix: None,
        region: None,
        endpoint_url: None,
    };
    let source = ParquetSource::new(ParquetSourceConfig::s3(s3))
        .await
        .unwrap();
    assert_eq!(source.dataset_uri(), "s3://only-bucket");
}

#[tokio::test]
async fn stream_pages_applies_column_projection() {
    // The projection (`ProjectionMask`) path is exercised on a single local
    // file through the streaming reader: only the requested columns appear in
    // the emitted pages.
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
        Field::new("c", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![10, 20, 30])),
            Arc::new(Int32Array::from(vec![100, 200, 300])),
        ],
    )
    .unwrap();

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("proj.parquet");
    write_parquet(&path, &batch);

    let cfg = ParquetSourceConfig::local(path.to_str().unwrap())
        .columns(["b"])
        .with_batch_size(2);
    let source = ParquetSource::new(cfg).await.unwrap();

    let pages = collect_pages(&source).await;
    let total: usize = pages.iter().map(|p| p.records.len()).sum();
    assert_eq!(total, 3);
    for page in &pages {
        for rec in &page.records {
            let obj = rec.as_object().expect("object");
            assert_eq!(obj.len(), 1, "only the projected column should remain");
            assert!(obj.contains_key("b"));
            assert!(!obj.contains_key("a"));
            assert!(!obj.contains_key("c"));
        }
    }
    // First emitted value carries the projected column's data.
    assert_eq!(pages[0].records[0]["b"], 10);
}

#[tokio::test]
async fn stream_pages_projection_missing_column_errors() {
    // Streaming path: a projected column absent from the file fails during the
    // up-front schema-validation pass, naming the missing column.
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("only_a.parquet");
    write_parquet(&path, &batch);

    let cfg = ParquetSourceConfig::local(path.to_str().unwrap()).columns(["missing"]);
    let source = ParquetSource::new(cfg).await.unwrap();

    let ctx = std::collections::HashMap::new();
    let mut stream = source.stream_pages(&ctx, 0);
    let first = stream.next().await.expect("an item");
    match first {
        Err(faucet_core::FaucetError::Source(msg)) => {
            assert!(msg.contains("missing"), "names the column: {msg}");
        }
        other => panic!("expected projection Source error, got {other:?}"),
    }
}

#[tokio::test]
async fn glob_skips_directory_entries() {
    // `expand_glob` keeps only entries where `is_file()` is true. A pattern
    // that also matches a subdirectory must not try to read the directory as a
    // parquet file — only the real `.parquet` file is read.
    let dir = TempDir::new().unwrap();
    // A subdirectory that matches `*` but is not a file.
    std::fs::create_dir(dir.path().join("nested.parquet")).unwrap();

    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))]).unwrap();
    write_parquet(&dir.path().join("data.parquet"), &batch);

    let pattern = format!("{}/*.parquet", dir.path().display());
    let source = ParquetSource::new(ParquetSourceConfig::glob(pattern))
        .await
        .unwrap();
    let rows = source.fetch_all().await.unwrap();
    assert_eq!(
        rows.len(),
        3,
        "directory entry must be skipped, only the file read"
    );
    let mut nums: Vec<i64> = rows.iter().map(|r| r["v"].as_i64().unwrap()).collect();
    nums.sort();
    assert_eq!(nums, vec![1, 2, 3]);
}

#[tokio::test]
async fn invalid_glob_pattern_is_config_error() {
    // A syntactically invalid glob pattern surfaces as `FaucetError::Config`
    // from `expand_glob`'s `glob::glob` parse step.
    let source = ParquetSource::new(ParquetSourceConfig::glob("/tmp/[invalid"))
        .await
        .unwrap();
    let err = source
        .fetch_all()
        .await
        .expect_err("invalid glob should fail");
    assert!(
        matches!(err, faucet_core::FaucetError::Config(_)),
        "expected Config error, got {err:?}"
    );
}

#[tokio::test]
async fn stream_pages_single_local_file_round_trips() {
    // A straightforward single-file streaming round-trip with the default
    // batch size: every written row is recovered in order across the pages.
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap();
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("single.parquet");
    write_parquet(&path, &batch);

    let source = ParquetSource::new(ParquetSourceConfig::local(path.to_str().unwrap()))
        .await
        .unwrap();
    let pages = collect_pages(&source).await;
    let rows: Vec<_> = pages.into_iter().flat_map(|p| p.records).collect();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["id"], 1);
    assert_eq!(rows[0]["name"], "a");
    assert_eq!(rows[2]["id"], 3);
    assert_eq!(rows[2]["name"], "c");
}
