#![cfg(all(feature = "file-formats", feature = "encryption"))]
//! Every readable format × every read-side option of the file source (#777):
//! decompression, decryption, globs over several files, and incremental
//! reads by name and by modification time. Files are produced by the file
//! sink (ORC, which has no writer, from a reference fixture).

use faucet_core::{CompiledEncryption, EncryptionSpec, Sink, Source};
use faucet_sink_file::FileSink;
use faucet_source_file::{FileSource, FileSourceConfig, IncrementalBy};
use futures::StreamExt;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::Path;

const ORC: &[u8] = include_bytes!("../../core/tests/fixtures/orc/people.orc");

const FORMATS: &[(&str, &str)] = &[
    ("json_lines", "jsonl"),
    ("json_array", "json"),
    ("csv", "csv"),
    ("xml", "xml"),
    ("xlsx", "xlsx"),
    ("parquet", "parquet"),
    ("raw_text", "txt"),
    ("avro", "avro"),
    ("orc", "orc"),
];

const CODECS: &[(&str, &str)] = &[("none", ""), ("gzip", ".gz"), ("zstd", ".zst")];

fn spec() -> EncryptionSpec {
    serde_json::from_value(json!({"key": "read-matrix"})).unwrap()
}

fn records(format: &str, file: usize) -> Vec<Value> {
    (0..2)
        .map(|i| {
            if format == "raw_text" {
                json!({"text": format!("f{file} line {i}")})
            } else {
                json!({"id": format!("{file}-{i}"), "name": "x"})
            }
        })
        .collect()
}

/// Write file number `n` of the case into `dir`; returns what reading it
/// must yield.
async fn produce(
    dir: &Path,
    format: (&str, &str),
    codec: (&str, &str),
    enc: bool,
    n: usize,
) -> Vec<Value> {
    let path = dir.join(format!("f{n}.{}{}", format.1, codec.1));
    if format.0 == "orc" {
        let mut body = faucet_core::compress_buf(ORC, codec_of(codec.0)).unwrap();
        if enc {
            body = CompiledEncryption::compile(&spec()).unwrap().encrypt(&body);
        }
        std::fs::write(&path, body).unwrap();
        let plain = dir.join(format!(".ref{n}.orc"));
        std::fs::write(&plain, ORC).unwrap();
        let rows = FileSource::new(FileSourceConfig::new(plain.to_string_lossy()))
            .unwrap()
            .fetch_all()
            .await
            .unwrap();
        std::fs::remove_file(plain).unwrap();
        return rows;
    }
    let mut cfg = json!({"path": path});
    if enc {
        cfg["encryption"] = serde_json::to_value(spec()).unwrap();
    }
    let sink = FileSink::new(serde_json::from_value(cfg).unwrap()).unwrap();
    let recs = records(format.0, n);
    sink.write_batch(&recs).await.unwrap();
    sink.flush().await.unwrap();
    if format.0 == "raw_text" {
        let content: String = recs
            .iter()
            .map(|r| format!("{}\n", r["text"].as_str().unwrap()))
            .collect();
        return vec![json!({"path": path.to_string_lossy(), "content": content})];
    }
    recs
}

fn codec_of(name: &str) -> faucet_core::Compression {
    match name {
        "gzip" => faucet_core::Compression::Gzip,
        "zstd" => faucet_core::Compression::Zstd,
        _ => faucet_core::Compression::None,
    }
}

fn source(root: &str, enc: bool, by: Option<IncrementalBy>) -> FileSource {
    let mut cfg = FileSourceConfig::new(root);
    if enc {
        cfg.encryption = Some(spec());
    }
    if let Some(by) = by {
        cfg = cfg.incremental(by);
    }
    FileSource::new(cfg).unwrap()
}

async fn read(src: &FileSource) -> Result<(Vec<Value>, Option<Value>), String> {
    let ctx = HashMap::new();
    let mut pages = src.stream_pages(&ctx, 0);
    let (mut rows, mut mark) = (Vec::new(), None);
    while let Some(p) = pages.next().await {
        let p = p.map_err(|e| e.to_string())?;
        rows.extend(p.records);
        if p.bookmark.is_some() {
            mark = p.bookmark;
        }
    }
    Ok((rows, mark))
}

fn set_mtime(path: &Path, secs: u64) {
    let t = std::time::UNIX_EPOCH + std::time::Duration::from_secs(secs);
    std::fs::File::options()
        .write(true)
        .open(path)
        .unwrap()
        .set_modified(t)
        .unwrap();
}

async fn case(format: (&str, &str), codec: (&str, &str), enc: bool) -> Result<(), String> {
    let dir = tempfile::tempdir().unwrap();
    let mut expected = produce(dir.path(), format, codec, enc, 1).await;
    expected.extend(produce(dir.path(), format, codec, enc, 2).await);
    let glob = format!("{}/*.{}{}", dir.path().display(), format.1, codec.1);
    for (i, n) in [1, 2].iter().enumerate() {
        set_mtime(
            &dir.path().join(format!("f{n}.{}{}", format.1, codec.1)),
            1_000 + i as u64,
        );
    }

    let (rows, _) = read(&source(&glob, enc, None))
        .await
        .map_err(|e| format!("glob: {e}"))?;
    if rows != expected {
        return Err(format!("glob read {rows:?}, expected {expected:?}"));
    }
    let (rows, _) = read(&source(&dir.path().to_string_lossy(), enc, None))
        .await
        .map_err(|e| format!("directory: {e}"))?;
    if rows != expected {
        return Err(format!("directory read {rows:?}"));
    }

    for by in [IncrementalBy::Name, IncrementalBy::Mtime] {
        let first = source(&glob, enc, Some(by));
        let (rows, mark) = read(&first)
            .await
            .map_err(|e| format!("{by:?} run 1: {e}"))?;
        if rows != expected {
            return Err(format!("{by:?} run 1 read {rows:?}"));
        }
        let mark = mark.ok_or_else(|| format!("{by:?}: no bookmark"))?;
        let third = produce(dir.path(), format, codec, enc, 3).await;
        set_mtime(
            &dir.path().join(format!("f3.{}{}", format.1, codec.1)),
            2_000,
        );
        let second = source(&glob, enc, Some(by));
        second
            .apply_start_bookmark(mark)
            .await
            .map_err(|e| e.to_string())?;
        let (rows, _) = read(&second)
            .await
            .map_err(|e| format!("{by:?} run 2: {e}"))?;
        if rows != third {
            return Err(format!(
                "{by:?} run 2 read {rows:?}, expected only {third:?}"
            ));
        }
        std::fs::remove_file(dir.path().join(format!("f3.{}{}", format.1, codec.1))).unwrap();
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_readable_format_takes_every_read_option() {
    let mut failures = Vec::new();
    let mut cases = 0;
    for &format in FORMATS {
        for &codec in CODECS {
            for enc in [false, true] {
                cases += 1;
                if let Err(e) = case(format, codec, enc).await {
                    failures.push(format!(
                        "{} × {} × encryption={enc}: {e}",
                        format.0, codec.0
                    ));
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {cases} combinations failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
