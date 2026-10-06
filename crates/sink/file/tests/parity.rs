#![allow(deprecated)]
#![cfg(feature = "file-formats")]

//! Parity with the `csv`, `jsonl` and `parquet` sinks (#777): the same
//! records written through the old sink and through the file sink with the
//! equivalent options produce the same file.

use faucet_core::Sink;
use faucet_sink_file::FileSink;
use serde_json::{Value, json};

use std::path::Path;

fn sink(v: Value) -> FileSink {
    FileSink::new(serde_json::from_value(v).unwrap()).unwrap()
}

fn sink_err(v: Value) -> String {
    match serde_json::from_value(v) {
        Ok(cfg) => FileSink::new(cfg).err().expect("rejected").to_string(),
        Err(e) => e.to_string(),
    }
}

fn p(dir: &Path, name: &str) -> String {
    dir.join(name).to_string_lossy().into_owned()
}

async fn pages(s: &dyn Sink, pages: &[Vec<Value>]) -> Result<(), String> {
    for page in pages {
        s.write_batch(page).await.map_err(|e| e.to_string())?;
    }
    s.flush().await.map_err(|e| e.to_string())
}

fn read_parquet(path: &str) -> (Vec<Value>, parquet::file::metadata::ParquetMetaData) {
    let file = std::fs::File::open(path).unwrap();
    let builder =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let meta = builder.metadata().as_ref().clone();
    let mut out = Vec::new();
    for b in builder.build().unwrap() {
        out.extend(faucet_core::columnar::record_batch_to_values(&b.unwrap()).unwrap());
    }
    (out, meta)
}

#[tokio::test]
async fn an_explicit_parquet_schema_types_and_fixes_the_columns() {
    use arrow::datatypes::{DataType, TimeUnit};
    let dir = tempfile::tempdir().unwrap();
    let schema = json!([
        {"name": "id", "type": "int32", "nullable": false},
        {"name": "amount", "type": {"decimal": {"precision": 10, "scale": 2}}},
        {"name": "day", "type": "date"},
        {"name": "at", "type": "timestamp_us"},
        {"name": "label", "type": "string"},
        {"name": "big", "type": "uint64"},
        {"name": "f", "type": "float32"},
        {"name": "flag", "type": "boolean"},
        {"name": "ms", "type": "timestamp_ms"},
        {"name": "ns", "type": "timestamp_ns"},
        {"name": "n", "type": "int64"},
        {"name": "d", "type": "float64"}
    ]);
    let out = p(dir.path(), "typed.parquet");
    let s = sink(json!({"path": out, "parquet": {"schema": schema}}));
    pages(
        &s,
        &[vec![json!({"id": 1, "amount": "12.50", "day": "2024-02-29", "at": "2024-02-29T10:00:00Z", "big": 18446744073709551615u64})],
            vec![json!({"id": 2})]],
    )
    .await
    .unwrap();
    let file = std::fs::File::open(&out).unwrap();
    let b = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let got: Vec<(String, DataType)> = b
        .schema()
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f.data_type().clone()))
        .collect();
    assert_eq!(got[0], ("id".into(), DataType::Int32));
    assert_eq!(got[1], ("amount".into(), DataType::Decimal128(10, 2)));
    assert_eq!(got[2], ("day".into(), DataType::Date32));
    assert_eq!(
        got[3],
        (
            "at".into(),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        )
    );
    assert_eq!(got.len(), 12);
    assert!(!b.schema().field(0).is_nullable());
    let (rows, _) = read_parquet(&out);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["amount"], json!("12.50"));
    assert_eq!(rows[1]["label"], Value::Null);

    let e = pages(
        &sink(json!({"path": p(dir.path(), "x.parquet"), "parquet": {"schema": schema}})),
        &[vec![json!({"id": 1, "extra": true})]],
    )
    .await
    .unwrap_err();
    assert!(e.contains("[extra]"), "{e}");
    let e = pages(
        &sink(json!({"path": p(dir.path(), "y.parquet"), "parquet": {"schema": schema}})),
        &[vec![json!({"label": "no id"})]],
    )
    .await
    .unwrap_err();
    assert!(e.contains("parquet.schema"), "{e}");

    for bad in [
        json!([]),
        json!([{"name": "a", "type": "int32"}, {"name": "a", "type": "int32"}]),
        json!([{"name": "", "type": "int32"}]),
        json!([{"name": "a", "type": {"decimal": {"precision": 0, "scale": 0}}}]),
        json!([{"name": "a", "type": {"decimal": {"precision": 4, "scale": 5}}}]),
    ] {
        let err = sink_err(json!({"path": p(dir.path(), "z.parquet"), "parquet": {"schema": bad}}));
        assert!(err.contains("parquet.schema"), "{err}");
    }
}

#[tokio::test]
async fn an_explicit_schema_casts_and_checks_columnar_batches() {
    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    let dir = tempfile::tempdir().unwrap();
    let out = p(dir.path(), "c.parquet");
    let s = sink(json!({"path": out, "parquet": {"schema": [{"name": "id", "type": "int32"}]}}));
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
    s.write_batch_columnar(&batch).await.unwrap();
    s.flush().await.unwrap();
    assert_eq!(
        read_parquet(&out).0,
        vec![json!({"id": 1}), json!({"id": 2})]
    );

    let extra = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("more", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        extra,
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["m"])),
        ],
    )
    .unwrap();
    let s = sink(
        json!({"path": p(dir.path(), "d.parquet"), "parquet": {"schema": [{"name": "id", "type": "int32"}]}}),
    );
    let e = s
        .write_batch_columnar(&batch)
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("[more]"), "{e}");
}
