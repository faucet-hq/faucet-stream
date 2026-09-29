#![cfg(all(feature = "file-formats", feature = "encryption"))]
//! Every writable format × every cross-cutting option (#777). Each
//! combination must either round-trip — written by the file sink, read back
//! through the sink's own read-back config with the file source, same
//! records — or be refused with a typed config error where the refusal is
//! intrinsic (appending to one whole-document file). A format that silently
//! lacks an option fails here.

use faucet_core::{FaucetError, Sink, Source};
use faucet_sink_file::FileSink;
use faucet_source_file::FileSource;
use serde_json::{Value, json};
use std::path::Path;

const FORMATS: &[(&str, &str)] = &[
    ("json_lines", "jsonl"),
    ("json_array", "json"),
    ("csv", "csv"),
    ("xml", "xml"),
    ("xlsx", "xlsx"),
    ("parquet", "parquet"),
    ("raw_text", "txt"),
    ("avro", "avro"),
];

const CODECS: &[(&str, &str)] = &[("none", ""), ("gzip", ".gz"), ("zstd", ".zst")];

#[derive(Debug, Clone, Copy)]
enum Layout {
    Single,
    RollRecords,
    RollBytes,
    Template,
    Directory,
}

const LAYOUTS: &[Layout] = &[
    Layout::Single,
    Layout::RollRecords,
    Layout::RollBytes,
    Layout::Template,
    Layout::Directory,
];

#[derive(Debug, Clone, Copy, PartialEq)]
enum Run {
    Overwrite,
    Append,
    ErrorIfExists,
    StagedOverwrite,
}

const RUNS: &[Run] = &[
    Run::Overwrite,
    Run::Append,
    Run::ErrorIfExists,
    Run::StagedOverwrite,
];

fn records(format: &str, from: usize, n: usize) -> Vec<Value> {
    (from..from + n)
        .map(|i| {
            if format == "raw_text" {
                json!({"text": format!("secret-name line {i}")})
            } else {
                json!({"id": i.to_string(), "name": format!("secret-name-{i}")})
            }
        })
        .collect()
}

fn config(
    dir: &Path,
    format: (&str, &str),
    codec: (&str, &str),
    enc: bool,
    layout: Layout,
) -> Value {
    let (name, ext) = format;
    let base = dir.join("out").to_string_lossy().into_owned();
    let mut cfg = match layout {
        Layout::Single => json!({"path": format!("{base}.{ext}{}", codec.1)}),
        Layout::RollRecords => {
            json!({"path": format!("{base}.{ext}{}", codec.1), "max_records_per_file": 2})
        }
        Layout::RollBytes => {
            json!({"path": format!("{base}.{ext}{}", codec.1), "max_bytes_per_file": 80})
        }
        Layout::Template => json!({
            "path": format!("{base}-{{part}}.{ext}{}", codec.1),
            "max_records_per_file": 3
        }),
        Layout::Directory => json!({
            "path": format!("{base}/"),
            "format": name,
            "compression": codec.0
        }),
    };
    if enc {
        cfg["encryption"] = json!({"key": "matrix-key"});
    }
    cfg
}

async fn write(cfg: &Value, pages: &[Vec<Value>]) -> Result<FileSink, FaucetError> {
    let sink = FileSink::new(serde_json::from_value(cfg.clone()).map_err(FaucetError::Json)?)?;
    for p in pages {
        sink.write_batch(p).await?;
    }
    sink.flush().await?;
    sink.complete_run().await?;
    Ok(sink)
}

async fn staged(cfg: &Value, pages: &[Vec<Value>]) -> Result<FileSink, FaucetError> {
    let mut cfg = cfg.clone();
    cfg["write_mode"] = json!("overwrite");
    let begin = FileSink::new(serde_json::from_value(cfg.clone()).map_err(FaucetError::Json)?)?;
    begin.begin_overwrite().await?;
    let sink = FileSink::new(serde_json::from_value(cfg.clone()).map_err(FaucetError::Json)?)?;
    for p in pages {
        sink.write_batch(p).await?;
    }
    sink.flush().await?;
    let commit = FileSink::new(serde_json::from_value(cfg).map_err(FaucetError::Json)?)?;
    commit.commit_overwrite().await?;
    Ok(sink)
}

async fn read_back(sink: &FileSink) -> Result<Vec<Value>, String> {
    let (kind, cfg) = sink.readback_source().ok_or("no read-back source")?;
    assert_eq!(kind, "file");
    let cfg = serde_json::from_value(cfg).map_err(|e| e.to_string())?;
    FileSource::new(cfg)
        .map_err(|e| e.to_string())?
        .fetch_all()
        .await
        .map_err(|e| e.to_string())
}

/// What a read of raw-text output yields, flattened back to the records'
/// lines so it compares like any other format.
fn normalise(format: &str, rows: Vec<Value>) -> Vec<Value> {
    if format != "raw_text" {
        return rows;
    }
    rows.iter()
        .flat_map(|r| {
            r["content"]
                .as_str()
                .unwrap_or_default()
                .lines()
                .map(|l| json!({"text": l}))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn intrinsic_refusal(format: &str, layout: Layout, run: Run) -> bool {
    let appendable = matches!(format, "json_lines" | "csv" | "raw_text");
    let numbered = !matches!(layout, Layout::Single);
    run == Run::Append && !appendable && !numbered
}

async fn case(
    format: (&str, &str),
    codec: (&str, &str),
    enc: bool,
    layout: Layout,
    run: Run,
) -> Result<(), String> {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), format, codec, enc, layout);
    let first = records(format.0, 0, 5);
    let second = records(format.0, 5, 3);
    let initial = write(&cfg, &[first[..2].to_vec(), first[2..].to_vec()])
        .await
        .map_err(|e| format!("first run: {e}"))?;
    let back = normalise(format.0, read_back(&initial).await?);
    if back != first {
        return Err(format!("first run read back {back:?}"));
    }
    let (sink, expected) = match run {
        Run::Overwrite => (
            write(&cfg, std::slice::from_ref(&second)).await,
            second.clone(),
        ),
        Run::StagedOverwrite => (
            staged(&cfg, std::slice::from_ref(&second)).await,
            second.clone(),
        ),
        Run::Append => {
            let mut c = cfg.clone();
            c["mode"] = json!("append");
            let r = write(&c, std::slice::from_ref(&second)).await;
            if intrinsic_refusal(format.0, layout, run) {
                return match r {
                    Err(FaucetError::Config(m)) if m.contains("append") => Ok(()),
                    Err(e) => Err(format!("refused with the wrong error: {e}")),
                    Ok(_) => Err("append to a whole-document file was accepted".into()),
                };
            }
            (r, first.iter().chain(&second).cloned().collect())
        }
        Run::ErrorIfExists => {
            let mut c = cfg.clone();
            c["mode"] = json!("error_if_exists");
            return match write(&c, std::slice::from_ref(&second)).await {
                Err(FaucetError::Sink(m)) if m.contains("already exists") => Ok(()),
                Err(e) => Err(format!("error_if_exists failed oddly: {e}")),
                Ok(_) => Err("error_if_exists overwrote an existing file".into()),
            };
        }
    };
    let sink = sink.map_err(|e| format!("second run: {e}"))?;
    let back = normalise(format.0, read_back(&sink).await?);
    if back != expected {
        return Err(format!(
            "second run read back {back:?}, expected {expected:?}"
        ));
    }
    if enc {
        for entry in walk(dir.path()) {
            let raw = std::fs::read(&entry).unwrap();
            if String::from_utf8_lossy(&raw).contains("secret-name") {
                return Err(format!("{} holds plaintext", entry.display()));
            }
        }
    }
    Ok(())
}

fn walk(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let p = e.path();
        if p.is_dir() {
            out.extend(walk(&p));
        } else {
            out.push(p);
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn every_writable_format_takes_every_option() {
    let mut set = tokio::task::JoinSet::new();
    for &format in FORMATS {
        for &codec in CODECS {
            for enc in [false, true] {
                for &layout in LAYOUTS {
                    for &run in RUNS {
                        set.spawn(async move {
                            case(format, codec, enc, layout, run).await.map_err(|e| {
                                format!(
                                    "{} × {} × encryption={enc} × {layout:?} × {run:?}: {e}",
                                    format.0, codec.0
                                )
                            })
                        });
                    }
                }
            }
        }
    }
    let mut failures = Vec::new();
    let mut cases = 0;
    while let Some(r) = set.join_next().await {
        cases += 1;
        if let Err(e) = r.expect("case task") {
            failures.push(e);
        }
    }
    failures.sort();
    assert_eq!(
        cases,
        FORMATS.len() * CODECS.len() * 2 * LAYOUTS.len() * RUNS.len()
    );
    assert!(
        failures.is_empty(),
        "{} of {cases} combinations failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn orc_is_refused_as_read_only() {
    let dir = tempfile::tempdir().unwrap();
    for cfg in [
        json!({"path": dir.path().join("a.orc")}),
        json!({"path": format!("{}/", dir.path().display()), "format": "orc"}),
    ] {
        let err = serde_json::from_value(cfg)
            .map_err(FaucetError::Json)
            .and_then(FileSink::new)
            .err()
            .expect("refused");
        assert!(
            matches!(&err, FaucetError::Config(m) if m.contains("orc")),
            "{err}"
        );
    }
}
