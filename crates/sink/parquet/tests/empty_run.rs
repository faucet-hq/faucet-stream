#![allow(deprecated)]
//! #753 (parquet): a fixed `*.parquet` path reflects the latest successful run.
//! An empty successful run removes the previous file (no schema exists to write
//! a valid empty one); failed and cancelled runs keep it.

use faucet_core::{CancellationToken, FaucetError, Pipeline, Source, Value, async_trait, json};
use faucet_sink_parquet::{ParquetSink, ParquetSinkConfig};
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

async fn run(target: &str, source: Rows, cancel: Option<CancellationToken>) -> bool {
    let sink = ParquetSink::new(ParquetSinkConfig::local(target))
        .await
        .unwrap();
    let mut p = Pipeline::new(&source, &sink);
    if let Some(c) = cancel {
        p = p.with_cancel(c);
    }
    p.run().await.is_ok()
}

fn row_count(path: &Path) -> i64 {
    let file = std::fs::File::open(path).unwrap();
    let reader = parquet::file::reader::SerializedFileReader::new(file).unwrap();
    parquet::file::reader::FileReader::metadata(&reader)
        .file_metadata()
        .num_rows()
}

#[tokio::test(flavor = "multi_thread")]
async fn an_empty_successful_run_removes_the_stale_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out.parquet");
    let target = path.to_string_lossy().to_string();
    assert!(run(&target, rows(3), None).await);
    assert_eq!(row_count(&path), 3);
    assert!(run(&target, rows(0), None).await);
    assert!(!path.exists(), "the previous run's rows must not survive");
    assert!(run(&target, rows(0), None).await, "a missing file is fine");
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_and_cancelled_empty_runs_keep_the_previous_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out.parquet");
    let target = path.to_string_lossy().to_string();
    assert!(run(&target, rows(3), None).await);

    assert!(!run(&target, Rows(Err("boom".into())), None).await);
    assert_eq!(row_count(&path), 3);

    let token = CancellationToken::new();
    token.cancel();
    assert!(run(&target, rows(0), Some(token)).await);
    assert_eq!(row_count(&path), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn directory_mode_leaves_earlier_parts_alone() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().to_string_lossy().to_string();
    assert!(run(&target, rows(2), None).await);
    assert!(run(&target, rows(0), None).await);
    let parts = std::fs::read_dir(dir.path()).unwrap().count();
    assert_eq!(parts, 1, "UUID-named parts are not a fixed destination");
}
