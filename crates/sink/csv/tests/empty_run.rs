#![allow(deprecated)]
//! #753 (csv): with `append: false` the file reflects the latest successful run,
//! including a run that wrote zero records; failed and cancelled runs keep the
//! previous file.

use faucet_core::{CancellationToken, FaucetError, Pipeline, Source, Value, async_trait, json};
use faucet_sink_csv::{CsvSink, CsvSinkConfig};
use std::collections::HashMap;
use std::path::Path;

struct Rows(Result<Vec<Value>, String>);

#[async_trait]
impl Source for Rows {
    async fn fetch_with_context(
        &self,
        _ctx: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        self.0.clone().map_err(FaucetError::Source)
    }
}

fn rows(n: usize) -> Rows {
    Rows(Ok((0..n).map(|i| json!({ "id": i })).collect()))
}

async fn run(path: &Path, append: bool, source: Rows, cancel: Option<CancellationToken>) -> bool {
    let sink = CsvSink::new(CsvSinkConfig::new(path.to_string_lossy()).append(append));
    let mut p = Pipeline::new(&source, &sink);
    if let Some(c) = cancel {
        p = p.with_cancel(c);
    }
    p.run().await.is_ok()
}

fn lines(path: &Path) -> usize {
    std::fs::read_to_string(path).unwrap().lines().count()
}

#[tokio::test]
async fn an_empty_successful_run_truncates_the_previous_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out.csv");
    assert!(run(&path, false, rows(3), None).await);
    assert_eq!(lines(&path), 4);
    assert!(run(&path, false, rows(0), None).await);
    assert!(path.exists());
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
}

#[tokio::test]
async fn an_empty_run_creates_the_file_when_it_does_not_exist() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nested/out.csv");
    assert!(run(&path, false, rows(0), None).await);
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
}

#[tokio::test]
async fn failed_and_cancelled_runs_keep_the_previous_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out.csv");
    assert!(run(&path, false, rows(3), None).await);

    assert!(!run(&path, false, Rows(Err("boom".into())), None).await);
    assert_eq!(lines(&path), 4);

    let token = CancellationToken::new();
    token.cancel();
    assert!(run(&path, false, rows(0), Some(token)).await);
    assert_eq!(lines(&path), 4);
}

#[tokio::test]
async fn append_mode_never_truncates_on_an_empty_run() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out.csv");
    assert!(run(&path, true, rows(2), None).await);
    assert!(run(&path, true, rows(0), None).await);
    assert_eq!(lines(&path), 3, "header + 2 rows");
    assert!(run(&path, true, rows(1), None).await);
    assert_eq!(lines(&path), 4);
}

#[cfg(feature = "compression")]
#[tokio::test]
async fn an_empty_compressed_run_is_a_valid_empty_stream() {
    use faucet_core::compression::{Compression, wrap_sync_reader};
    use std::io::Read;
    for (name, codec) in [
        ("out.csv.gz", Compression::Gzip),
        ("out.csv.zst", Compression::Zstd),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(name);
        assert!(run(&path, false, rows(3), None).await);
        assert!(run(&path, false, rows(0), None).await);
        let file = std::fs::File::open(&path).unwrap();
        assert!(
            file.metadata().unwrap().len() > 0,
            "{name}: an empty member, not zero bytes"
        );
        let mut out = String::new();
        wrap_sync_reader(std::io::BufReader::new(file), codec)
            .read_to_string(&mut out)
            .unwrap();
        assert!(out.is_empty(), "{name}: {out:?}");
    }
}
