//! #789 FILE-03: a table whose active files carry deletion vectors is refused
//! rather than read whole (which would return deleted rows).

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use faucet_core::Source as _;
use faucet_source_delta::{DeltaSource, DeltaSourceConfig};

fn write_table(dir: &std::path::Path, add_extra: &str) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let file = std::fs::File::create(dir.join("part-0.parquet")).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(file, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let size = std::fs::metadata(dir.join("part-0.parquet")).unwrap().len();

    let log = dir.join("_delta_log");
    std::fs::create_dir_all(&log).unwrap();
    let schema_string = serde_json::json!({
        "type": "struct",
        "fields": [{"name": "id", "type": "long", "nullable": true, "metadata": {}}]
    })
    .to_string();
    let lines = [
        serde_json::json!({"protocol": {
            "minReaderVersion": 3, "minWriterVersion": 7,
            "readerFeatures": ["deletionVectors"], "writerFeatures": ["deletionVectors"]
        }}),
        serde_json::json!({"metaData": {
            "id": "5fba94ed-9794-4965-ba6e-6ee3c0d22af9",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": schema_string,
            "partitionColumns": [],
            "configuration": {"delta.enableDeletionVectors": "true"},
            "createdTime": 0
        }}),
        serde_json::from_str(&format!(
            r#"{{"add": {{"path": "part-0.parquet", "partitionValues": {{}}, "size": {size},
                "modificationTime": 0, "dataChange": true{add_extra}}}}}"#
        ))
        .unwrap(),
    ];
    let body: String = lines.iter().map(|l| format!("{l}\n")).collect();
    std::fs::write(log.join("00000000000000000000.json"), body).unwrap();
}

async fn read(dir: &std::path::Path) -> Result<usize, faucet_core::FaucetError> {
    let source = DeltaSource::new(DeltaSourceConfig::new(dir.to_string_lossy()))
        .await
        .unwrap();
    Ok(source.fetch_all().await?.len())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_file_with_a_deletion_vector_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    write_table(
        dir.path(),
        r#", "deletionVector": {"storageType": "i",
            "pathOrInlineDv": "wi5b=000010000siXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L",
            "sizeInBytes": 40, "cardinality": 1}"#,
    );
    let err = read(dir.path())
        .await
        .expect_err("deleted rows must not be read");
    assert!(err.to_string().contains("deletion vectors"), "{err}");
    assert!(err.to_string().contains("part-0.parquet"), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_same_table_without_a_deletion_vector_reads() {
    let dir = tempfile::tempdir().unwrap();
    write_table(dir.path(), "");
    assert_eq!(read(dir.path()).await.unwrap(), 3);
}
