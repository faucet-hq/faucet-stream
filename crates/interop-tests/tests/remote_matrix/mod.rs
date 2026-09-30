//! Format × option matrix for a remote file sink (#777), shared by the
//! object-store and SFTP sink crates (`#[path]`-included into each one's
//! `tests/`). The same combinations as the local file sink's matrix: every
//! writable format × codec × encryption × layout × run mode. Each case writes
//! through the real sink against its emulator, downloads what landed, and
//! reads it back with the file source — the records must match, or the case
//! must be refused where the refusal is intrinsic.

use faucet_core::{FaucetError, Sink, Source};
use serde_json::{Value, json};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

pub type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// How the matrix reaches one backend.
pub trait Remote: Send + Sync + 'static {
    /// The sink config for `fields` (the layout's `path`, format, caps,
    /// codec…) with every object under `prefix`.
    fn config(&self, prefix: &str, fields: Value) -> Value;
    /// Build the sink.
    fn sink(&self, cfg: Value) -> BoxFut<'_, Result<Box<dyn Sink>, FaucetError>>;
    /// Every object under `prefix`, as (name relative to `prefix`, body).
    fn objects(&self, prefix: &str) -> BoxFut<'_, Vec<(String, Vec<u8>)>>;
}

pub const FORMATS: &[(&str, &str)] = &[
    ("json_lines", "jsonl"),
    ("json_array", "json"),
    ("csv", "csv"),
    ("xml", "xml"),
    ("xlsx", "xlsx"),
    ("parquet", "parquet"),
    ("raw_text", "txt"),
    ("avro", "avro"),
];

pub const CODECS: &[(&str, &str)] = &[("none", ""), ("gzip", ".gz"), ("zstd", ".zst")];

#[derive(Debug, Clone, Copy)]
pub enum Layout {
    Single,
    RollRecords,
    RollBytes,
    Template,
    Directory,
}

pub const LAYOUTS: &[Layout] = &[
    Layout::Single,
    Layout::RollRecords,
    Layout::RollBytes,
    Layout::Template,
    Layout::Directory,
];

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Run {
    Overwrite,
    Append,
    ErrorIfExists,
    StagedOverwrite,
}

pub const RUNS: &[Run] = &[
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

fn fields(format: (&str, &str), codec: (&str, &str), enc: bool, layout: Layout) -> Value {
    let (name, ext) = format;
    let mut f = match layout {
        Layout::Single => json!({"path": format!("out.{ext}{}", codec.1)}),
        Layout::RollRecords => {
            json!({"path": format!("out.{ext}{}", codec.1), "max_records_per_file": 2})
        }
        Layout::RollBytes => {
            json!({"path": format!("out.{ext}{}", codec.1), "max_bytes_per_file": 80})
        }
        Layout::Template => json!({
            "path": format!("out-{{part}}.{ext}{}", codec.1),
            "max_records_per_file": 3
        }),
        Layout::Directory => json!({"path": "out/", "compression": codec.0}),
    };
    f["format"] = json!(if matches!(layout, Layout::Directory) {
        name
    } else {
        "auto"
    });
    f["batch_size"] = json!(0);
    if enc {
        f["encryption"] = json!({"key": "matrix-key"});
    }
    f
}

fn with(cfg: &Value, key: &str, v: Value) -> Value {
    let mut c = cfg.clone();
    c[key] = v;
    c
}

async fn write<R: Remote>(r: &R, cfg: &Value, pages: &[Vec<Value>]) -> Result<(), FaucetError> {
    let sink = r.sink(cfg.clone()).await?;
    for p in pages {
        sink.write_batch(p).await?;
    }
    sink.flush().await?;
    sink.complete_run().await
}

async fn staged<R: Remote>(r: &R, cfg: &Value, pages: &[Vec<Value>]) -> Result<(), FaucetError> {
    let cfg = with(cfg, "write_mode", json!("overwrite"));
    r.sink(cfg.clone()).await?.begin_overwrite().await?;
    let sink = r.sink(cfg.clone()).await?;
    for p in pages {
        sink.write_batch(p).await?;
    }
    sink.flush().await?;
    r.sink(cfg).await?.commit_overwrite().await
}

/// Download everything under `prefix` into `dir` and read it with the file
/// source (format from each file's extension).
async fn read_back<R: Remote>(
    r: &R,
    prefix: &str,
    dir: &Path,
    enc: bool,
) -> Result<(Vec<Value>, Vec<Vec<u8>>), String> {
    let objects = r.objects(prefix).await;
    if objects.iter().any(|(n, _)| n.contains(".faucet-")) {
        return Err(format!(
            "scratch or staging objects left behind: {:?}",
            objects.iter().map(|(n, _)| n).collect::<Vec<_>>()
        ));
    }
    for (name, body) in &objects {
        let path = dir.join(name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, body).unwrap();
    }
    let mut cfg = json!({"path": format!("{}/", dir.display()), "recursive": true});
    if enc {
        cfg["encryption"] = json!({"key": "matrix-key"});
    }
    let cfg = serde_json::from_value(cfg).map_err(|e| e.to_string())?;
    let rows = faucet_source_file::FileSource::new(cfg)
        .map_err(|e| e.to_string())?
        .fetch_all()
        .await
        .map_err(|e| e.to_string())?;
    Ok((rows, objects.into_iter().map(|(_, b)| b).collect()))
}

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
    run == Run::Append && !appendable && matches!(layout, Layout::Single)
}

/// One combination; `id` keeps its objects apart from every other case.
pub async fn case<R: Remote>(
    r: &R,
    id: &str,
    format: (&str, &str),
    codec: (&str, &str),
    enc: bool,
    layout: Layout,
    run: Run,
) -> Result<(), String> {
    let prefix = format!("matrix/{id}/");
    let cfg = r.config(&prefix, fields(format, codec, enc, layout));
    let first = records(format.0, 0, 5);
    let second = records(format.0, 5, 3);
    write(r, &cfg, &[first[..2].to_vec(), first[2..].to_vec()])
        .await
        .map_err(|e| format!("first run: {e}"))?;
    let dir = tempfile::tempdir().unwrap();
    let (back, _) = read_back(r, &prefix, &dir.path().join("1"), enc).await?;
    if normalise(format.0, back.clone()) != first {
        return Err(format!("first run read back {back:?}"));
    }
    let expected = match run {
        Run::Overwrite => {
            write(r, &cfg, std::slice::from_ref(&second))
                .await
                .map_err(|e| format!("second run: {e}"))?;
            second.clone()
        }
        Run::StagedOverwrite => {
            staged(r, &cfg, std::slice::from_ref(&second))
                .await
                .map_err(|e| format!("second run: {e}"))?;
            second.clone()
        }
        Run::Append => {
            let res = write(
                r,
                &with(&cfg, "if_exists", json!("append")),
                std::slice::from_ref(&second),
            )
            .await;
            if intrinsic_refusal(format.0, layout, run) {
                return match res {
                    Err(FaucetError::Config(m)) if m.contains("append") => Ok(()),
                    Err(e) => Err(format!("refused with the wrong error: {e}")),
                    Ok(()) => Err("append to a whole-document object was accepted".into()),
                };
            }
            res.map_err(|e| format!("second run: {e}"))?;
            first.iter().chain(&second).cloned().collect()
        }
        Run::ErrorIfExists => {
            let res = write(
                r,
                &with(&cfg, "if_exists", json!("error")),
                std::slice::from_ref(&second),
            )
            .await;
            return match res {
                Err(FaucetError::Sink(m)) if m.contains("already exists") => Ok(()),
                Err(e) => Err(format!("error_if_exists failed oddly: {e}")),
                Ok(()) => Err("error_if_exists overwrote an existing object".into()),
            };
        }
    };
    let (back, bodies) = read_back(r, &prefix, &dir.path().join("2"), enc).await?;
    let back = normalise(format.0, back);
    if back != expected {
        return Err(format!(
            "second run read back {back:?}, expected {expected:?}"
        ));
    }
    if enc
        && bodies
            .iter()
            .any(|b| String::from_utf8_lossy(b).contains("secret-name"))
    {
        return Err("an object holds plaintext".into());
    }
    Ok(())
}

/// Every combination (`full`), or a covering subset — every format with
/// every run mode, rotating through the codecs, encryption and layouts so
/// each value of each option still appears for several formats.
#[allow(dead_code)]
pub async fn run_matrix<R: Remote>(remote: R, full: bool) {
    run_matrix_with(remote, full, 16).await
}

/// [`run_matrix`] with at most `concurrency` cases in flight (a server that
/// limits concurrent sessions needs fewer).
#[allow(dead_code)]
pub async fn run_matrix_with<R: Remote>(remote: R, full: bool, concurrency: usize) {
    let remote = Arc::new(remote);
    let mut cases = Vec::new();
    for (fi, &format) in FORMATS.iter().enumerate() {
        for (ri, &run) in RUNS.iter().enumerate() {
            if full {
                for &codec in CODECS {
                    for enc in [false, true] {
                        for &layout in LAYOUTS {
                            cases.push((format, codec, enc, layout, run));
                        }
                    }
                }
            } else {
                for (k, &layout) in LAYOUTS.iter().enumerate() {
                    let n = fi + ri + k;
                    cases.push((format, CODECS[n % 3], n % 2 == 0, layout, run));
                }
            }
        }
    }
    let total = cases.len();
    let mut set = tokio::task::JoinSet::new();
    let limit = Arc::new(tokio::sync::Semaphore::new(concurrency));
    for (i, (format, codec, enc, layout, run)) in cases.into_iter().enumerate() {
        let remote = remote.clone();
        let limit = limit.clone();
        set.spawn(async move {
            let _permit = limit.acquire_owned().await.unwrap();
            case(
                remote.as_ref(),
                &format!("c{i}"),
                format,
                codec,
                enc,
                layout,
                run,
            )
            .await
            .map_err(|e| {
                format!(
                    "{} × {} × encryption={enc} × {layout:?} × {run:?}: {e}",
                    format.0, codec.0
                )
            })
        });
    }
    let mut failures = Vec::new();
    while let Some(r) = set.join_next().await {
        if let Err(e) = r.expect("case task") {
            failures.push(e);
        }
    }
    failures.sort();
    assert!(
        failures.is_empty(),
        "{} of {total} combinations failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
