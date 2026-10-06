#![allow(deprecated)]
//! #789 FILE-08: `append: true` continues the existing file's header and
//! writes a header into a file that does not exist yet.

use faucet_core::{Sink, json};
use faucet_sink_csv::config::OnUnknownField;
use faucet_sink_csv::{CsvSink, CsvSinkConfig};

async fn write(cfg: CsvSinkConfig, rows: &[faucet_core::Value]) -> Result<(), String> {
    let sink = CsvSink::new(cfg);
    sink.write_batch(rows).await.map_err(|e| e.to_string())?;
    sink.flush().await.map_err(|e| e.to_string())
}

#[tokio::test]
async fn appending_keeps_the_existing_header_and_order() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out.csv");
    let cfg = || CsvSinkConfig::new(path.to_string_lossy()).append(true);

    write(cfg(), &[json!({"id": 1, "name": "a"})])
        .await
        .unwrap();
    write(cfg(), &[json!({"name": "b", "id": 2})])
        .await
        .unwrap();
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        "id,name\n1,a\n2,b\n",
        "a new file gets its header; a later run keeps its column order"
    );

    let err = write(
        cfg().on_unknown_field(OnUnknownField::Error),
        &[json!({"id": 3, "extra": "x"})],
    )
    .await
    .unwrap_err();
    assert!(err.contains("extra"), "{err}");
}

#[tokio::test]
async fn appending_to_an_empty_file_writes_the_header() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("empty.csv");
    std::fs::write(&path, "").unwrap();
    write(
        CsvSinkConfig::new(path.to_string_lossy()).append(true),
        &[json!({"id": 1})],
    )
    .await
    .unwrap();
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "id\n1\n");
}

#[cfg(feature = "compression")]
#[tokio::test]
async fn appending_to_a_gzip_file_reads_its_header() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out.csv.gz");
    let cfg = || CsvSinkConfig::new(path.to_string_lossy()).append(true);
    write(cfg(), &[json!({"a": 1, "b": 2})]).await.unwrap();
    write(cfg(), &[json!({"b": 4, "a": 3})]).await.unwrap();
    let mut text = String::new();
    std::io::Read::read_to_string(
        &mut flate2::read::MultiGzDecoder::new(std::fs::File::open(&path).unwrap()),
        &mut text,
    )
    .unwrap();
    assert_eq!(text, "a,b\n1,2\n3,4\n");
}
