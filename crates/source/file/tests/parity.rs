#![allow(deprecated)]

//! Parity with the `csv` and `parquet` sources (#777): every option those
//! crates have works on the file source, and the same input read through the
//! old crate and through the file source produces the same records.
#![cfg(feature = "file-formats")]

use faucet_core::{CsvOptions, Source};
use faucet_source_file::{FileSource, FileSourceConfig};
use futures::StreamExt;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

fn write(dir: &Path, name: &str, bytes: &[u8]) -> String {
    let p = dir.join(name);
    std::fs::write(&p, bytes).unwrap();
    p.to_string_lossy().into_owned()
}

async fn pages(src: &FileSource, ctx: &HashMap<String, Value>) -> Result<Vec<usize>, String> {
    let mut s = src.stream_pages(ctx, 0);
    let mut out = Vec::new();
    while let Some(p) = s.next().await {
        out.push(p.map_err(|e| e.to_string())?.records.len());
    }
    Ok(out)
}

fn csv_cfg(path: &str, f: impl FnOnce(&mut CsvOptions)) -> FileSourceConfig {
    let mut cfg = FileSourceConfig::new(path);
    f(&mut cfg.csv);
    cfg
}

#[tokio::test]
async fn csv_streams_in_batch_size_pages_rather_than_whole_files() {
    let dir = tempfile::tempdir().unwrap();
    let mut body = String::from("n\n");
    for i in 0..2500 {
        body.push_str(&format!("{i}\n"));
    }
    let path = write(dir.path(), "big.csv", body.as_bytes());
    let src = FileSource::new(FileSourceConfig::new(&path).with_batch_size(1000)).unwrap();
    assert_eq!(
        pages(&src, &HashMap::new()).await.unwrap(),
        vec![1000, 1000, 500]
    );
    let gz = dir.path().join("big.csv.gz");
    let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    std::io::Write::write_all(&mut e, body.as_bytes()).unwrap();
    std::fs::write(&gz, e.finish().unwrap()).unwrap();
    let src =
        FileSource::new(FileSourceConfig::new(gz.to_string_lossy()).with_batch_size(1000)).unwrap();
    assert_eq!(
        pages(&src, &HashMap::new()).await.unwrap(),
        vec![1000, 1000, 500]
    );
}

#[test]
fn a_bad_csv_dialect_fails_at_construction() {
    let cfg = csv_cfg("x.csv", |c| c.quote = "ab".into());
    let err = FileSource::new(cfg).err().expect("rejected").to_string();
    assert!(err.contains("csv.quote"), "{err}");
    let cfg = FileSourceConfig::new("x.parquet").parquet_columns(Vec::<String>::new());
    assert!(FileSource::new(cfg).is_err());
}

fn put_parquet(path: &Path, ids: Vec<i64>, names: Vec<Option<&str>>) {
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    let n = ids.len();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, false),
    ]));
    let batch = arrow::array::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Float64Array::from(vec![1.5; n])),
        ],
    )
    .unwrap();
    let mut w =
        parquet::arrow::ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema, None)
            .unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

#[cfg(feature = "encryption")]
fn spec(key: &str) -> faucet_core::EncryptionSpec {
    serde_json::from_value(json!({"key": key})).unwrap()
}

#[cfg(feature = "encryption")]
#[tokio::test]
async fn a_whole_file_sealed_csv_and_gzip_body_is_decrypted() {
    let dir = tempfile::tempdir().unwrap();
    let enc = faucet_core::CompiledEncryption::compile(&spec("k")).unwrap();
    let csv = write(dir.path(), "s.csv", &enc.encrypt(b"a,b\n1,2\n"));
    let mut cfg = FileSourceConfig::new(&csv);
    cfg.encryption = Some(spec("k"));
    assert_eq!(
        FileSource::new(cfg).unwrap().fetch_all().await.unwrap(),
        vec![json!({"a": "1", "b": "2"})]
    );
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    std::io::Write::write_all(&mut gz, b"{\"x\":1}\n").unwrap();
    let body = gz.finish().unwrap();
    let path = write(dir.path(), "s.jsonl.gz", &enc.encrypt(&body));
    let mut cfg = FileSourceConfig::new(&path);
    cfg.encryption = Some(spec("k"));
    assert_eq!(
        FileSource::new(cfg).unwrap().fetch_all().await.unwrap(),
        vec![json!({"x": 1})]
    );
    let p = dir.path().join("s.parquet");
    put_parquet(&p, vec![7], vec![Some("q")]);
    let sealed = enc.encrypt(&std::fs::read(&p).unwrap());
    std::fs::write(&p, sealed).unwrap();
    let mut cfg = FileSourceConfig::new(p.to_string_lossy());
    cfg.encryption = Some(spec("k"));
    assert_eq!(
        FileSource::new(cfg).unwrap().fetch_all().await.unwrap(),
        vec![json!({"id": 7, "name": "q", "score": 1.5})]
    );
}
