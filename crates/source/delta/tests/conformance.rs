//! `faucet-conformance` battery for the Delta Lake source.
//!
//! Runs **entirely on the local filesystem** (no Docker, no object store): a
//! small Delta table is seeded with delta-rs's own writer, then read back
//! through [`DeltaSource`]. Passing this battery in CI is the Tier-1 (supported)
//! criterion — see the connector catalog's "Support tiers" note.
//!
//! Checks exercised: 1 (config schema, offline), 2 (bounded-memory streaming),
//! 6 (errors, not panics), 9 (`batch_size = 0` single page), 10
//! (`connector_name()` non-empty), and 11 (`check()` well-formed). Delta is a
//! snapshot source (no incremental bookmark) and not a sink, so checks 3 and
//! 4/5/7/8 do not apply.

use std::sync::Arc;

use deltalake::arrow::array::Int64Array;
use deltalake::arrow::datatypes::{DataType, Field, Schema};
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::kernel::StructType;
use deltalake::kernel::engine::arrow_conversion::TryIntoKernel;
use deltalake::operations::create::CreateBuilder;
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use faucet_conformance::{
    assert_batch_size_zero_single_page, assert_bounded_memory, assert_config_schema_valid_value,
    assert_connector_name_nonempty, assert_errors_not_panics, assert_preflight_check_wellformed,
};
use faucet_core::Source as _;
use faucet_source_delta::{DeltaSource, DeltaSourceConfig};

fn table_uri(dir: &tempfile::TempDir, name: &str) -> String {
    dir.path().join(name).to_string_lossy().into_owned()
}

/// Seed a fresh Delta table at `uri` with `n` rows `{id: 0..n}` using delta-rs's
/// high-level write op (creates the table from the batch's schema).
async fn seed(uri: &str, n: i64) {
    let arrow_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let ids = Int64Array::from((0..n).collect::<Vec<i64>>());
    let batch =
        RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(ids)]).expect("seed batch");

    // Create the table from the Arrow schema, then commit one batch via the
    // low-level RecordBatchWriter — the same write path the Delta *sink* uses
    // (delta-rs's high-level `DeltaOps::write` needs the datafusion feature,
    // which the workspace disables).
    let delta_schema: StructType = arrow_schema
        .as_ref()
        .try_into_kernel()
        .expect("arrow schema → delta kernel");
    let mut table = CreateBuilder::new()
        .with_location(uri)
        .with_columns(delta_schema.fields().cloned())
        .await
        .expect("create table");
    let mut writer = RecordBatchWriter::for_table(&table).expect("record-batch writer");
    writer.write(batch).await.expect("write batch");
    writer
        .flush_and_commit(&mut table)
        .await
        .expect("commit batch");
}

// ── Check 1: config schema validity (pure, offline) ──────────────────────────
#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(DeltaSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "delta");
}

// ── Check 2: bounded-memory streaming ────────────────────────────────────────
/// 250 rows at batch_size 50 must page (5 pages of 50) rather than buffer the
/// whole table into one page.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_bounded_memory() {
    let dir = tempfile::tempdir().unwrap();
    let uri = table_uri(&dir, "events");
    seed(&uri, 250).await;

    // Delta pages via its config `batch_size` (the `stream_pages` arg is only a
    // hint — the source uses the authoritative config value), so set it to 50.
    let mut cfg = DeltaSourceConfig::new(&uri);
    cfg.batch_size = 50;
    let source = DeltaSource::new(cfg).await.expect("source");
    assert_bounded_memory(&source, 50, 250).await;

    // Sanity: the whole table is readable through the batch path too.
    let all = source.fetch_all().await.expect("fetch_all");
    assert_eq!(all.len(), 250);
}

// ── Check 6: errors, not panics ──────────────────────────────────────────────
/// Point the source at a path that is not a Delta table. `new()` is lazy, so it
/// builds; the read surfaces a typed `FaucetError`, never a panic.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_errors_not_panics() {
    let dir = tempfile::tempdir().unwrap();
    let uri = table_uri(&dir, "not-a-table");
    // A typed error at construction is contract-compliant (no panic); otherwise
    // the read path must surface a typed error, still never a panic.
    if let Ok(source) = DeltaSource::new(DeltaSourceConfig::new(&uri)).await {
        assert_errors_not_panics(&source).await;
    }
}

// ── Check 10: connector_name is non-empty ─────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
async fn conformance_connector_name_nonempty() {
    let dir = tempfile::tempdir().unwrap();
    let uri = table_uri(&dir, "name");
    seed(&uri, 1).await;
    let source = DeltaSource::new(DeltaSourceConfig::new(&uri))
        .await
        .expect("source");
    assert_connector_name_nonempty(&source);
}

// ── Check 9: batch_size=0 yields a single page ────────────────────────────────
/// A small single-commit table read under the `batch_size = 0` sentinel (native
/// row-group cadence) must surface as exactly one page.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_batch_size_zero_single_page() {
    let dir = tempfile::tempdir().unwrap();
    let uri = table_uri(&dir, "single_page");
    seed(&uri, 6).await;

    let mut cfg = DeltaSourceConfig::new(&uri);
    cfg.batch_size = 0;
    let source = DeltaSource::new(cfg).await.expect("source");
    assert_batch_size_zero_single_page(&source).await;
}

// ── Check 11: preflight check() is well-formed ────────────────────────────────
/// A seeded table makes delta's metadata-open probe pass; the check must return
/// Ok(report) with a well-formed probe.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_preflight_check_wellformed() {
    let dir = tempfile::tempdir().unwrap();
    let uri = table_uri(&dir, "preflight");
    seed(&uri, 6).await;
    let source = DeltaSource::new(DeltaSourceConfig::new(&uri))
        .await
        .expect("source");
    assert_preflight_check_wellformed(&source, &faucet_core::check::CheckContext::default()).await;
}
