#![allow(deprecated)]
//! A file larger than one upload part goes up as a multipart upload whose
//! parts are sent while encoding continues, and arrives whole.

use arrow::array::{Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use faucet_core::Sink;
use faucet_sink_parquet::{ParquetCompression, ParquetSink, ParquetSinkConfig};
use futures::TryStreamExt;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use serde_json::{Value, json};
use tempfile::TempDir;

fn payload(seed: &mut u64) -> String {
    let mut out = String::with_capacity(1024);
    while out.len() < 1024 {
        *seed ^= *seed << 13;
        *seed ^= *seed >> 7;
        *seed ^= *seed << 17;
        out.push_str(&format!("{:016x}", *seed));
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn a_file_spanning_several_parts_uploads_whole() {
    let tmp = TempDir::new().unwrap();
    let mut cfg = ParquetSinkConfig::local(tmp.path().to_string_lossy().to_string());
    cfg.compression = ParquetCompression::Uncompressed;
    cfg.row_group_size = 1_000;
    let sink = ParquetSink::new(cfg).await.unwrap();

    let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
    let mut expected = Vec::new();
    for page in 0..24 {
        let records: Vec<Value> = (0..1_000)
            .map(|i| {
                let id = page * 1_000 + i;
                let p = payload(&mut seed);
                expected.push((id, p.clone()));
                json!({ "id": id, "payload": p })
            })
            .collect();
        assert_eq!(sink.write_batch(&records).await.unwrap(), 1_000);
    }
    sink.flush().await.unwrap();

    let files: Vec<std::path::PathBuf> = std::fs::read_dir(tmp.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("parquet"))
        .collect();
    assert_eq!(files.len(), 1, "{files:?}");
    let size = std::fs::metadata(&files[0]).unwrap().len();
    assert!(size > 20 * 1024 * 1024, "spans three 10 MiB parts: {size}");

    let file = tokio::fs::File::open(&files[0]).await.unwrap();
    let batches: Vec<RecordBatch> = ParquetRecordBatchStreamBuilder::new(file)
        .await
        .unwrap()
        .build()
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut got = Vec::new();
    for b in &batches {
        let ids = b
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let payloads = b
            .column_by_name("payload")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..b.num_rows() {
            got.push((ids.value(i), payloads.value(i).to_string()));
        }
    }
    assert_eq!(got.len(), expected.len());
    assert!(got == expected, "every row arrives once, in order");
}
