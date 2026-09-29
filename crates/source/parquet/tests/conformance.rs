#![allow(deprecated)]
//! `faucet-conformance` Tier-1 battery for the Parquet source.
//!
//! Check 1 — the connector's config JSON Schema is a valid, well-formed value.
//! Check 2 — `stream_pages` pages under a bounded batch size (every record
//! streamed; peak page ≤ batch_size and < total), i.e. memory is O(batch_size)
//! regardless of total volume.
//! Check 6 — a bad path surfaces a typed error, never a panic.
//! Check 9 — `batch_size = 0` yields the whole (small) result set as one page.
//! Check 10 — `connector_name()` is non-empty.
//! Check 11 — `check()` returns a well-formed `Ok(report)`.
//!
//! Parquet is a snapshot source (no incremental bookmark) and not a sink, so
//! checks 3 and 4/5/7/8 do not apply.

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use faucet_conformance::{
    assert_batch_size_zero_single_page, assert_bounded_memory, assert_config_schema_valid_value,
    assert_connector_name_nonempty, assert_errors_not_panics, assert_preflight_check_wellformed,
};
use faucet_core::Source;
use faucet_source_parquet::{ParquetSource, ParquetSourceConfig};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;

/// Write a parquet file with `total` rows of `(id, name)`, split into small
/// row-groups so the streaming reader yields many bounded batches.
fn write_fixture(path: &Path, total: usize) {
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

    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(250))
        .build();
    let file = File::create(path).expect("create file");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
}

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(ParquetSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-parquet");
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_bounded_memory() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("conformance.parquet");
    write_fixture(&path, 5_000);

    // Config batch_size must equal the batch passed to the battery — this
    // overriding source treats its config batch_size as authoritative.
    let source =
        ParquetSource::new(ParquetSourceConfig::local(path.to_str().unwrap()).with_batch_size(250))
            .await
            .unwrap();

    assert_bounded_memory(&source, 250, 5_000).await;
}

// ── Check 6: errors, not panics (no container) ──────────────────────────────

/// Point the source at a nonexistent local parquet file. `new()` for a local
/// path does no file I/O (it only builds an S3 store when configured for S3),
/// so the source constructs cleanly; the first read then fails opening the file
/// with a typed `FaucetError::Source` on both the `fetch_all` and `stream_pages`
/// paths, never a panic.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_errors_not_panics() {
    let source = ParquetSource::new(ParquetSourceConfig::local(
        "/nonexistent/does-not-exist.parquet",
    ))
    .await
    .expect("source builds without touching the file");
    assert_errors_not_panics(&source).await;
}

// ── Check 10: connector_name is non-empty ─────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
async fn conformance_connector_name_nonempty() {
    let source = ParquetSource::new(ParquetSourceConfig::local("/tmp/does-not-matter.parquet"))
        .await
        .unwrap();
    assert_connector_name_nonempty(&source);
    assert_eq!(source.connector_name(), "parquet");
}

// ── Check 9: batch_size=0 yields a single page ────────────────────────────────
/// A small single-row-group fixture read under the `batch_size = 0` sentinel
/// (native row-group cadence) must surface as exactly one page.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_batch_size_zero_single_page() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("small.parquet");
    write_fixture(&path, 6);

    let source =
        ParquetSource::new(ParquetSourceConfig::local(path.to_str().unwrap()).with_batch_size(0))
            .await
            .unwrap();
    assert_batch_size_zero_single_page(&source).await;
}

// ── Check 11: preflight check() is well-formed ────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
async fn conformance_preflight_check_wellformed() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("small.parquet");
    write_fixture(&path, 6);

    let source = ParquetSource::new(ParquetSourceConfig::local(path.to_str().unwrap()))
        .await
        .unwrap();
    assert_preflight_check_wellformed(&source, &faucet_core::check::CheckContext::default()).await;
}
