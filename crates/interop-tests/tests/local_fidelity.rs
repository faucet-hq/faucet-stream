#![allow(deprecated)]
#![cfg(all(feature = "file-formats", feature = "arrow"))]

//! The value-fidelity corpus (#885) through every source/sink pair that needs
//! no container: each sink writes the corpus, its source reads it back, and
//! every value must land unchanged except where a tolerance names the
//! destination's genuine limitation.

#[path = "iceberg_common/mod.rs"]
mod iceberg_common;

use faucet_conformance::fidelity::{self, Tolerance};
use faucet_core::Sink;
use serde_json::{Value, json};
use std::path::Path;

fn p(dir: &Path, name: &str) -> String {
    dir.join(name).to_string_lossy().into_owned()
}

async fn write(sink: &dyn Sink, records: &[Value]) {
    sink.write_batch(records).await.expect("write the corpus");
    sink.flush().await.expect("flush the corpus");
}

/// Fields a typed, one-schema destination stores as JSON text or flattens:
/// the nested values of the wide row.
const NESTED: [&str; 4] = [
    "nested_object",
    "nested_array",
    "nested_empty_object",
    "nested_empty_array",
];

fn typed(tol: Tolerance) -> Tolerance {
    NESTED.iter().fold(tol, |t, f| t.skipping(f))
}

/// The wide row for an Arrow-typed destination: an Arrow list holds one
/// element type and Parquet has no empty struct, so those two values are left
/// out here and their refusal is asserted in
/// `arrow_destinations_refuse_what_they_cannot_hold`.
fn wide_arrow() -> Vec<Value> {
    let mut row = fidelity::flat_corpus();
    let obj = row.as_object_mut().unwrap();
    obj.remove("nested_array");
    obj.remove("nested_empty_object");
    vec![row]
}

#[tokio::test]
async fn arrow_destinations_refuse_what_they_cannot_hold() {
    let dir = tempfile::tempdir().unwrap();
    for (i, (field, value, says)) in [
        ("mixed", json!([1, "two"]), "mixed"),
        ("empty", json!({}), "empty struct"),
    ]
    .into_iter()
    .enumerate()
    {
        let path = p(dir.path(), &format!("refused{i}.parquet"));
        let sink =
            faucet_sink_file::FileSink::new(serde_json::from_value(json!({"path": path})).unwrap())
                .unwrap();
        let err = async {
            sink.write_batch(&[json!({ field: value })]).await?;
            sink.flush().await
        }
        .await
        .expect_err("the value has no Parquet representation");
        assert!(
            err.to_string().contains(field) || err.to_string().contains(says),
            "{err}"
        );
    }
}

#[tokio::test]
async fn file_sink_and_file_source_keep_json_documents_exactly() {
    let dir = tempfile::tempdir().unwrap();
    for name in ["corpus.jsonl", "corpus.json"] {
        let path = p(dir.path(), name);
        let sink =
            faucet_sink_file::FileSink::new(serde_json::from_value(json!({"path": path})).unwrap())
                .unwrap();
        write(&sink, &fidelity::corpus()).await;
        let source = faucet_source_file::FileSource::new(
            serde_json::from_value(json!({"path": path})).unwrap(),
        )
        .unwrap();
        fidelity::assert_corpus_read(&source, &fidelity::corpus(), Tolerance::exact()).await;
    }
}

#[tokio::test]
async fn file_sink_and_file_source_keep_typed_formats() {
    let dir = tempfile::tempdir().unwrap();
    for name in ["corpus.parquet", "corpus.avro"] {
        let path = p(dir.path(), name);
        let sink =
            faucet_sink_file::FileSink::new(serde_json::from_value(json!({"path": path})).unwrap())
                .unwrap();
        let sent = wide_arrow();
        write(&sink, &sent).await;
        let source = faucet_source_file::FileSource::new(
            serde_json::from_value(json!({"path": path})).unwrap(),
        )
        .unwrap();
        // Nested values have one schema per file; their own fidelity is the
        // format layer's (crates/conformance/tests/format_fidelity.rs).
        fidelity::assert_corpus_read(&source, &sent, typed(Tolerance::exact())).await;
    }
}

#[tokio::test]
async fn csv_sink_and_csv_source_keep_every_rendering() {
    let dir = tempfile::tempdir().unwrap();
    let path = p(dir.path(), "corpus.csv");
    let sink = faucet_sink_csv::CsvSink::new(faucet_sink_csv::CsvSinkConfig::new(&path));
    let sent = vec![fidelity::flat_corpus()];
    write(&sink, &sent).await;
    let source = faucet_source_csv::CsvSource::new(faucet_source_csv::CsvSourceConfig::new(&path));
    // CSV has no types: a cell is its text, so a number lands as the string
    // that renders it, and a null is written as the empty cell.
    fidelity::assert_corpus_read(
        &source,
        &sent,
        typed(
            Tolerance::exact()
                .lenient_scalar_kind()
                .skipping("booleans_and_null_null_val"),
        ),
    )
    .await;
}

#[tokio::test]
async fn parquet_sink_and_parquet_source_keep_the_wide_row() {
    let dir = tempfile::tempdir().unwrap();
    let path = p(dir.path(), "corpus.parquet");
    let sink =
        faucet_sink_parquet::ParquetSink::new(faucet_sink_parquet::ParquetSinkConfig::local(&path))
            .await
            .unwrap();
    let sent = wide_arrow();
    write(&sink, &sent).await;
    let source = faucet_source_parquet::ParquetSource::new(
        faucet_source_parquet::ParquetSourceConfig::local(&path),
    )
    .await
    .unwrap();
    fidelity::assert_corpus_read(&source, &sent, typed(Tolerance::exact())).await;
}

#[tokio::test]
async fn delta_sink_and_delta_source_keep_the_wide_row() {
    let dir = tempfile::tempdir().unwrap();
    let uri = p(dir.path(), "corpus");
    let sink = faucet_sink_delta::DeltaSink::new(faucet_sink_delta::DeltaSinkConfig::new(&uri))
        .await
        .unwrap();
    let sent = wide_arrow();
    write(&sink, &sent).await;
    let source =
        faucet_source_delta::DeltaSource::new(faucet_source_delta::DeltaSourceConfig::new(&uri))
            .await
            .unwrap();
    fidelity::assert_corpus_read(&source, &sent, typed(Tolerance::exact())).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn iceberg_sink_and_iceberg_source_keep_the_wide_row() {
    let lake = iceberg_common::Lake::new();
    let cfg: faucet_sink_iceberg::IcebergSinkConfig = serde_json::from_value(json!({
        "catalog": lake.catalog_json(),
        "namespace": ["db"],
        "table": "corpus",
        "create_if_missing": true,
        "batch_size": 0
    }))
    .unwrap();
    let sink = faucet_sink_iceberg::IcebergSink::new(cfg).await.unwrap();
    let sent = wide_arrow();
    write(&sink, &sent).await;
    let source: faucet_source_iceberg::IcebergSource = lake.source("corpus", json!({})).await;
    fidelity::assert_corpus_read(&source, &sent, typed(Tolerance::exact())).await;
}

#[tokio::test]
async fn sqlite_sink_and_sqlite_source_keep_the_corpus() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}?mode=rwc", p(dir.path(), "corpus.db"));
    let sink: faucet_sink_sqlite::SqliteSinkConfig = serde_json::from_value(json!({
        "database_url": url,
        "table_name": "corpus",
        "column_mapping": "auto_map",
        "create_table": true,
    }))
    .unwrap();
    let sink = faucet_sink_sqlite::SqliteSink::new(sink).await.unwrap();
    let sent = vec![fidelity::flat_corpus()];
    write(&sink, &sent).await;
    let source: faucet_source_sqlite::SqliteSourceConfig = serde_json::from_value(json!({
        "database_url": url,
        "query": "SELECT * FROM corpus",
    }))
    .unwrap();
    let source = faucet_source_sqlite::SqliteSource::new(source)
        .await
        .unwrap();
    // SQLite has no boolean (`true` is stored as 1) and no negative zero.
    fidelity::assert_corpus_read(
        &source,
        &sent,
        typed(
            Tolerance::exact()
                .skipping("booleans_and_null_true_val")
                .skipping("booleans_and_null_false_val")
                .skipping("floats_negative_zero"),
        ),
    )
    .await;
}

#[tokio::test]
async fn duckdb_sink_and_duckdb_source_keep_the_corpus() {
    let dir = tempfile::tempdir().unwrap();
    let db = p(dir.path(), "corpus.duckdb");
    let cfg: faucet_sink_duckdb::DuckdbSinkConfig = serde_json::from_value(json!({
        "database": db,
        "table_name": "corpus",
        "column_mapping": "auto_map",
    }))
    .unwrap();
    let sink = faucet_sink_duckdb::DuckdbSink::new(cfg).await.unwrap();
    let sent = vec![fidelity::flat_corpus()];
    write(&sink, &sent).await;
    drop(sink);
    let source = faucet_source_duckdb::DuckdbSource::new(
        faucet_source_duckdb::DuckdbSourceConfig::new(&db, "SELECT * FROM corpus"),
    )
    .await
    .unwrap();
    fidelity::assert_corpus_read(&source, &sent, typed(Tolerance::exact())).await;
}
