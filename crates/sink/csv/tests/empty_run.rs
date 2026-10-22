#![allow(deprecated)]
//! #753 (csv): with `append: false` the file reflects the latest successful run,
//! including a run that wrote zero records; a run that fails or is cancelled
//! before its first write keeps the previous file. The first write truncates
//! in place, so a run failing after it leaves only its own rows.

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
async fn runs_failing_or_cancelled_before_any_write_keep_the_previous_file() {
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

/// Page one, then a source failure.
struct FailsAfterFirstPage;

#[async_trait]
impl Source for FailsAfterFirstPage {
    async fn fetch_with_context(
        &self,
        _ctx: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        unreachable!("the pipeline streams pages")
    }

    fn stream_pages<'a>(
        &'a self,
        _context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> std::pin::Pin<
        Box<
            dyn faucet_core::Stream<Item = Result<faucet_core::StreamPage, FaucetError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(faucet_core::async_stream::stream! {
            yield Ok(faucet_core::StreamPage {
                records: vec![json!({"id": 100}), json!({"id": 101})],
                bookmark: None,
            });
            yield Err(FaucetError::Source("upstream 503".into()));
        })
    }
}

#[tokio::test]
async fn a_run_failing_after_its_first_write_leaves_only_its_own_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out.csv");
    assert!(run(&path, false, rows(3), None).await);
    assert_eq!(lines(&path), 4);

    let sink = CsvSink::new(CsvSinkConfig::new(path.to_string_lossy()).append(false));
    assert!(
        Pipeline::new(&FailsAfterFirstPage, &sink)
            .run()
            .await
            .is_err()
    );
    let text = std::fs::read_to_string(&path).unwrap();
    assert_eq!(text.lines().collect::<Vec<_>>(), vec!["id", "100", "101"]);
}
