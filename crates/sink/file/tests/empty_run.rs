//! #753 (file sink): with `mode: overwrite` (and the default
//! `write_mode: append`) the destination reflects the latest successful run —
//! an empty run removes the previous file, a shorter rollover run removes the
//! stale higher parts, and failed or cancelled runs change nothing.

use faucet_core::{CancellationToken, FaucetError, Pipeline, Source, Value, async_trait, json};
use faucet_sink_file::FileSink;
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

async fn run(cfg: Value, source: Rows, cancel: Option<CancellationToken>) -> bool {
    let sink = FileSink::new(serde_json::from_value(cfg).unwrap()).unwrap();
    let mut p = Pipeline::new(&source, &sink);
    if let Some(c) = cancel {
        p = p.with_cancel(c);
    }
    p.run().await.is_ok()
}

fn names(dir: &Path) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    v.sort();
    v
}

#[tokio::test(flavor = "multi_thread")]
async fn an_empty_run_removes_the_previous_file() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = json!({ "path": format!("{}/out.jsonl", dir.path().display()) });
    assert!(run(cfg.clone(), rows(3), None).await);
    assert_eq!(names(dir.path()), ["out.jsonl"]);
    assert!(run(cfg.clone(), rows(0), None).await);
    assert!(names(dir.path()).is_empty());
    assert!(run(cfg, rows(0), None).await);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_shorter_rollover_run_removes_stale_parts() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = json!({
        "path": format!("{}/out-{{part}}.jsonl", dir.path().display()),
        "max_records_per_file": 2,
    });
    assert!(run(cfg.clone(), rows(5), None).await);
    assert_eq!(names(dir.path()).len(), 3);
    assert!(run(cfg.clone(), rows(3), None).await);
    assert_eq!(names(dir.path()), ["out-00001.jsonl", "out-00002.jsonl"]);
    assert!(run(cfg.clone(), rows(4), None).await);
    assert_eq!(names(dir.path()), ["out-00001.jsonl", "out-00002.jsonl"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_and_cancelled_runs_and_append_mode_keep_files() {
    let dir = tempfile::tempdir().unwrap();
    let path = format!("{}/out.jsonl", dir.path().display());
    let cfg = json!({ "path": path });
    assert!(run(cfg.clone(), rows(3), None).await);
    assert!(!run(cfg.clone(), Rows(Err("boom".into())), None).await);
    assert_eq!(std::fs::read_to_string(&path).unwrap().lines().count(), 3);
    let token = CancellationToken::new();
    token.cancel();
    assert!(run(cfg, rows(0), Some(token)).await);
    assert_eq!(std::fs::read_to_string(&path).unwrap().lines().count(), 3);

    let append = json!({ "path": path, "mode": "append" });
    assert!(run(append, rows(0), None).await);
    assert_eq!(std::fs::read_to_string(&path).unwrap().lines().count(), 3);
}
