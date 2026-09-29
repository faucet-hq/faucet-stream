//! Parity with the `csv` and `parquet` sources (#777): every option those
//! crates have works on the file source, and the same input read through the
//! old crate and through the file source produces the same records.
#![cfg(feature = "file-formats")]

use faucet_core::{CsvOptions, Source};
use faucet_source_file::{FileSource, FileSourceConfig, FileSourceFormat};
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
async fn csv_dialect_options_match_the_csv_source() {
    let dir = tempfile::tempdir().unwrap();
    let body = b"id;name;note\n1;'Smith; J';NULL\n2;Doe;\n";
    let path = write(dir.path(), "people.csv", body);

    let old = faucet_source_csv::CsvSource::new(
        faucet_source_csv::CsvSourceConfig::new(&path)
            .delimiter(b';')
            .quote(b'\'')
            .null_values(vec!["NULL".into(), "".into()]),
    )
    .fetch_all()
    .await
    .unwrap();
    let new = FileSource::new(csv_cfg(&path, |c| {
        c.delimiter = ";".into();
        c.quote = "'".into();
        c.null_values = vec!["NULL".into(), "".into()];
    }))
    .unwrap()
    .fetch_all()
    .await
    .unwrap();
    assert_eq!(new, old);
    assert_eq!(
        new,
        vec![
            json!({"id": "1", "name": "Smith; J", "note": null}),
            json!({"id": "2", "name": "Doe", "note": null}),
        ]
    );
}

#[tokio::test]
async fn csv_headers_off_matches_the_csv_source() {
    let dir = tempfile::tempdir().unwrap();
    let path = write(dir.path(), "raw.csv", b"a,b\n1,2\n");
    let old = faucet_source_csv::CsvSource::new(
        faucet_source_csv::CsvSourceConfig::new(&path).has_headers(false),
    )
    .fetch_all()
    .await
    .unwrap();
    let new = FileSource::new(csv_cfg(&path, |c| c.has_headers = false))
        .unwrap()
        .fetch_all()
        .await
        .unwrap();
    assert_eq!(new, old);
}

#[tokio::test]
async fn ragged_rows_are_strict_by_default_like_the_csv_source() {
    let dir = tempfile::tempdir().unwrap();
    let path = write(dir.path(), "ragged.csv", b"a,b\n1,2\n3\n");
    let old = faucet_source_csv::CsvSource::new(faucet_source_csv::CsvSourceConfig::new(&path))
        .fetch_all()
        .await
        .unwrap_err();
    let new = FileSource::new(FileSourceConfig::new(&path))
        .unwrap()
        .fetch_all()
        .await
        .unwrap_err()
        .to_string();
    assert!(old.to_string().contains("line 3"), "{old}");
    assert!(
        new.contains("ragged row at line 3") && new.contains("ragged.csv"),
        "{new}"
    );

    let old = faucet_source_csv::CsvSource::new(
        faucet_source_csv::CsvSourceConfig::new(&path).flexible(true),
    )
    .fetch_all()
    .await
    .unwrap();
    let new = FileSource::new(csv_cfg(&path, |c| c.flexible = Some(true)))
        .unwrap()
        .fetch_all()
        .await
        .unwrap();
    assert_eq!(new, old);
    assert_eq!(new[1], json!({"a": "3"}));
}

#[tokio::test]
async fn a_duplicate_csv_header_fails_like_the_csv_source() {
    let dir = tempfile::tempdir().unwrap();
    let path = write(dir.path(), "dup.csv", b"id,id\n1,2\n");
    assert!(
        faucet_source_csv::CsvSource::new(faucet_source_csv::CsvSourceConfig::new(&path))
            .fetch_all()
            .await
            .is_err()
    );
    let err = FileSource::new(FileSourceConfig::new(&path))
        .unwrap()
        .fetch_all()
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("duplicate header id"), "{err}");
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

#[tokio::test]
async fn the_path_takes_context_values_like_the_csv_and_parquet_sources() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "acme.csv", b"a\n1\n");
    let template = format!("{}/{{tenant}}.csv", dir.path().display());
    let ctx = HashMap::from([("tenant".to_string(), json!("acme"))]);
    let src = FileSource::new(FileSourceConfig::new(&template)).unwrap();
    let recs = src.fetch_with_context(&ctx).await.unwrap();
    assert_eq!(recs, vec![json!({"a": "1"})]);
    let old = faucet_source_csv::CsvSource::new(faucet_source_csv::CsvSourceConfig::new(&template))
        .fetch_with_context(&ctx)
        .await
        .unwrap();
    assert_eq!(recs, old);
    assert!(src.fetch_all().await.is_err());
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

async fn old_parquet(
    cfg: faucet_source_parquet::ParquetSourceConfig,
) -> Result<Vec<Value>, String> {
    faucet_source_parquet::ParquetSource::new(cfg)
        .await
        .map_err(|e| e.to_string())?
        .fetch_all()
        .await
        .map_err(|e| e.to_string())
}

#[tokio::test]
async fn parquet_projection_matches_the_parquet_source() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.parquet");
    put_parquet(&path, vec![1, 2], vec![Some("x"), Some("y")]);
    let p = path.to_string_lossy().into_owned();

    let old =
        old_parquet(faucet_source_parquet::ParquetSourceConfig::local(&p).columns(["id", "name"]))
            .await
            .unwrap();
    let new = FileSource::new(FileSourceConfig::new(&p).parquet_columns(["id", "name"]))
        .unwrap()
        .fetch_all()
        .await
        .unwrap();
    assert_eq!(new, old);
    assert_eq!(
        new,
        vec![json!({"id": 1, "name": "x"}), json!({"id": 2, "name": "y"})]
    );

    let whole_old = old_parquet(faucet_source_parquet::ParquetSourceConfig::local(&p))
        .await
        .unwrap();
    let whole_new = FileSource::new(FileSourceConfig::new(&p))
        .unwrap()
        .fetch_all()
        .await
        .unwrap();
    assert_eq!(whole_new, whole_old);

    let ctx = HashMap::new();
    let columnar = FileSource::new(
        FileSourceConfig::new(&p)
            .format(FileSourceFormat::Parquet)
            .parquet_columns(["score"]),
    )
    .unwrap();
    let mut batches = columnar.stream_batches(&ctx, 0);
    let page = batches.next().await.unwrap().unwrap();
    let names: Vec<_> = page
        .batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(names, vec!["score"]);

    let err = FileSource::new(FileSourceConfig::new(&p).parquet_columns(["nope"]))
        .unwrap()
        .fetch_all()
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("'nope'") && err.contains("available: id, name, score"),
        "{err}"
    );
}

#[tokio::test]
async fn parquet_nulls_are_explicit_where_the_parquet_source_omitted_them() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("n.parquet");
    put_parquet(&path, vec![1], vec![None]);
    let p = path.to_string_lossy().into_owned();
    let old = old_parquet(faucet_source_parquet::ParquetSourceConfig::local(&p))
        .await
        .unwrap();
    let new = FileSource::new(FileSourceConfig::new(&p))
        .unwrap()
        .fetch_all()
        .await
        .unwrap();
    assert_eq!(old, vec![json!({"id": 1, "score": 1.5})]);
    assert_eq!(new, vec![json!({"id": 1, "name": null, "score": 1.5})]);
}

#[tokio::test]
async fn a_parquet_schema_mismatch_fails_before_any_row_like_the_parquet_source() {
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    let dir = tempfile::tempdir().unwrap();
    put_parquet(
        &dir.path().join("a.parquet"),
        vec![1, 2, 3],
        vec![Some("x"); 3],
    );
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
    let batch = arrow::array::RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["z"]))],
    )
    .unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(
        std::fs::File::create(dir.path().join("b.parquet")).unwrap(),
        schema,
        None,
    )
    .unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();

    let glob = format!("{}/*.parquet", dir.path().display());
    assert!(
        old_parquet(faucet_source_parquet::ParquetSourceConfig::glob(&glob))
            .await
            .is_err()
    );
    let src = FileSource::new(FileSourceConfig::new(&glob).with_batch_size(1)).unwrap();
    let ctx = HashMap::new();
    let mut s = src.stream_pages(&ctx, 0);
    let first = s.next().await.unwrap();
    let err = first.expect_err("fails before the first page").to_string();
    assert!(
        err.contains("a.parquet") && err.contains("b.parquet"),
        "{err}"
    );

    let columnar =
        FileSource::new(FileSourceConfig::new(&glob).format(FileSourceFormat::Parquet)).unwrap();
    let mut b = columnar.stream_batches(&ctx, 0);
    assert!(b.next().await.unwrap().is_err());

    let projected = FileSource::new(FileSourceConfig::new(&glob).parquet_columns(["id"])).unwrap();
    assert!(projected.fetch_all().await.is_err());
}

#[cfg(feature = "encryption")]
fn spec(key: &str) -> faucet_core::EncryptionSpec {
    serde_json::from_value(json!({"key": key})).unwrap()
}

#[cfg(feature = "encryption")]
#[tokio::test]
async fn files_the_jsonl_sink_encrypted_are_decrypted() {
    use faucet_core::Sink;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sealed.jsonl");
    let sink = faucet_sink_jsonl::JsonlSink::new(
        faucet_sink_jsonl::JsonlSinkConfig::new(&path).encryption(spec("k1")),
    );
    let records = vec![json!({"a": 1}), json!({"b": "two"})];
    sink.write_batch(&records).await.unwrap();
    sink.flush().await.unwrap();
    let p = path.to_string_lossy().into_owned();
    assert!(!std::fs::read_to_string(&path).unwrap().contains("two"));

    let mut cfg = FileSourceConfig::new(&p);
    cfg.encryption = Some(spec("k1"));
    assert_eq!(
        FileSource::new(cfg).unwrap().fetch_all().await.unwrap(),
        records
    );

    let mut rotated = FileSourceConfig::new(&p);
    rotated.encryption =
        Some(serde_json::from_value(json!({"key": "k2", "previous_keys": ["k1"]})).unwrap());
    assert_eq!(
        FileSource::new(rotated).unwrap().fetch_all().await.unwrap(),
        records
    );

    let mut wrong = FileSourceConfig::new(&p);
    wrong.encryption = Some(spec("nope"));
    assert!(FileSource::new(wrong).unwrap().fetch_all().await.is_err());

    let plain = write(dir.path(), "plain.jsonl", b"{\"a\":1}\n");
    let mut cfg = FileSourceConfig::new(&plain);
    cfg.encryption = Some(spec("k1"));
    let err = FileSource::new(cfg)
        .unwrap()
        .fetch_all()
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("not encrypted"), "{err}");

    let plain_csv = write(dir.path(), "plain.csv", b"a\n1\n");
    let mut cfg = FileSourceConfig::new(&plain_csv);
    cfg.encryption = Some(spec("k1"));
    let err = FileSource::new(cfg)
        .unwrap()
        .fetch_all()
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("not encrypted"), "{err}");

    let mut bad = FileSourceConfig::new(&p);
    bad.encryption = Some(spec(" "));
    assert!(FileSource::new(bad).is_err());
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
