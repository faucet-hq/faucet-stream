#![allow(deprecated)]

//! End-to-end reads through `FileSource` against real files on disk and a
//! wiremock HTTP server (#720, #719).
#![cfg(feature = "file-formats")]

use faucet_core::file_format::{FileFormat, FormatOptions, encode};
use faucet_core::{Pipeline, Sink, Source};
use faucet_source_file::{FileSource, FileSourceConfig, FileSourceFormat};
use serde_json::{Value, json};

use std::path::Path;
use std::sync::{Arc, Mutex};

fn write(dir: &Path, name: &str, bytes: &[u8]) -> String {
    let p = dir.join(name);
    if let Some(parent) = p.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(&p, bytes).unwrap();
    p.to_string_lossy().into_owned()
}

fn dir_str(dir: &Path) -> String {
    dir.to_string_lossy().into_owned()
}

/// A sink that accepts only Arrow batches, forwarding them to the real
/// Parquet sink — so a successful run proves the columnar path was taken.
struct ColumnarOnly(faucet_sink_parquet::ParquetSink, Arc<Mutex<usize>>);

#[async_trait::async_trait]
impl Sink for ColumnarOnly {
    async fn write_batch(&self, _records: &[Value]) -> Result<usize, faucet_core::FaucetError> {
        Err(faucet_core::FaucetError::Sink(
            "the row path must not be used".into(),
        ))
    }
    fn supports_columnar(&self) -> bool {
        true
    }
    async fn write_batch_columnar(
        &self,
        batch: &arrow::array::RecordBatch,
    ) -> Result<usize, faucet_core::FaucetError> {
        *self.1.lock().unwrap() += 1;
        self.0.write_batch_columnar(batch).await
    }
    async fn flush(&self) -> Result<(), faucet_core::FaucetError> {
        self.0.flush().await
    }
}

#[tokio::test]
async fn avro_to_parquet_runs_on_the_columnar_path() {
    use arrow::datatypes::{DataType, TimeUnit};
    let dir = tempfile::tempdir().unwrap();
    let schema = json!({"type": "record", "name": "r", "fields": [
        {"name": "id", "type": "long"},
        {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 9, "scale": 2}},
        {"name": "day", "type": {"type": "int", "logicalType": "date"}},
        {"name": "at", "type": {"type": "long", "logicalType": "timestamp-micros"}}
    ]});
    let mut opts = FormatOptions::default();
    opts.avro.schema = Some(schema);
    let rows: Vec<Value> = (0..5)
        .map(|i| json!({"id": i, "amount": format!("{i}.25"), "day": "2024-02-29", "at": "2024-02-29T00:00:00Z"}))
        .collect();
    write(
        dir.path(),
        "in/a.avro",
        &encode(&rows[..3], FileFormat::Avro, &opts).unwrap(),
    );
    write(
        dir.path(),
        "in/b.avro",
        &encode(&rows[3..], FileFormat::Avro, &opts).unwrap(),
    );
    let out = dir
        .path()
        .join("out.parquet")
        .to_string_lossy()
        .into_owned();

    let src = FileSource::new(
        FileSourceConfig::new(format!("{}/in", dir_str(dir.path())))
            .format(FileSourceFormat::Avro)
            .with_batch_size(2),
    )
    .unwrap();
    assert!(src.supports_columnar());
    let calls = Arc::new(Mutex::new(0));
    let sink = ColumnarOnly(
        faucet_sink_parquet::ParquetSink::new(faucet_sink_parquet::ParquetSinkConfig::local(&out))
            .await
            .unwrap(),
        calls.clone(),
    );
    let result = Pipeline::new(&src, &sink).run().await.unwrap();
    assert_eq!(result.records_written, 5);
    assert!(*calls.lock().unwrap() >= 2);
    drop(sink);

    let file = std::fs::File::open(&out).unwrap();
    let reader =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let s = reader.schema().clone();
    assert_eq!(
        s.field_with_name("amount").unwrap().data_type(),
        &DataType::Decimal128(9, 2)
    );
    assert_eq!(
        s.field_with_name("day").unwrap().data_type(),
        &DataType::Date32
    );
    assert!(matches!(
        s.field_with_name("at").unwrap().data_type(),
        DataType::Timestamp(TimeUnit::Microsecond, _)
    ));
    let n: usize = reader.build().unwrap().map(|b| b.unwrap().num_rows()).sum();
    assert_eq!(n, 5);
}
