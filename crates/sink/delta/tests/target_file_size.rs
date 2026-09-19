//! #620 — `target_file_size` must actually control output file size and bound
//! peak memory.
//!
//! The knob was declared in the config and validated (a zero was rejected), and
//! then **never read**: `RecordBatchWriter::for_table` was built with no sizing,
//! and the only `flush_and_commit` was at end-of-run. Since a bulk source emits
//! no bookmarks, that meant one commit per run and a parquet buffer holding the
//! *whole dataset* — an OOM on a large table, and a documented knob that did
//! nothing.
//!
//! delta-rs offers no target-size parameter: `RecordBatchWriter` writes exactly
//! one file per commit, so file size is governed by flush cadence. Wiring the
//! knob and bounding memory are therefore the same fix, and these tests assert
//! it through the observable consequence — the number of Delta versions and
//! parquet files the table ends up with.

use std::collections::HashMap;

use faucet_core::{Sink, Source};
use faucet_sink_delta::{DeltaSink, DeltaSinkConfig};
use faucet_source_delta::{DeltaSource, DeltaSourceConfig};
use serde_json::{Value, json};

fn table_uri(dir: &tempfile::TempDir, name: &str) -> String {
    dir.path().join(name).to_string_lossy().into_owned()
}

/// Records wide enough that a few hundred exceed a small target.
fn records(n: usize) -> Vec<Value> {
    (0..n)
        .map(|i| json!({ "id": i, "payload": "x".repeat(512) }))
        .collect()
}

async fn read_all(uri: &str) -> Vec<Value> {
    let source = DeltaSource::new(DeltaSourceConfig::new(uri))
        .await
        .expect("source");
    source
        .fetch_with_context(&HashMap::new())
        .await
        .expect("read")
}

/// Count `.parquet` data files in the table directory — one per commit.
fn parquet_files(dir: &std::path::Path) -> usize {
    fn walk(p: &std::path::Path, n: &mut usize) {
        let Ok(rd) = std::fs::read_dir(p) else { return };
        for e in rd.flatten() {
            let path = e.path();
            if path.is_dir() {
                // Skip `_delta_log`; it holds JSON commits, not data.
                if path.file_name().and_then(|f| f.to_str()) != Some("_delta_log") {
                    walk(&path, n);
                }
            } else if path.extension().and_then(|x| x.to_str()) == Some("parquet") {
                *n += 1;
            }
        }
    }
    let mut n = 0;
    walk(dir, &mut n);
    n
}

#[tokio::test]
async fn a_small_target_rolls_into_several_files_and_keeps_every_row() {
    let dir = tempfile::tempdir().unwrap();
    let uri = table_uri(&dir, "rolled");

    let mut cfg = DeltaSinkConfig::new(&uri);
    cfg.target_file_size = Some(16 * 1024); // 16 KiB — several rolls over 2000 rows
    // Chunk the page so the roll is checked *within* one `write_batch`. That is
    // the memory-bounding property that matters: a single 2000-row chunk would
    // buffer the whole page before the first size check, which is exactly the
    // O(whole dataset) behaviour this knob exists to cap.
    cfg.batch_size = 200;
    let sink = DeltaSink::new(cfg).await.expect("sink");

    let sent = records(2_000);
    let n = sink.write_batch(&sent).await.expect("write");
    sink.flush().await.expect("flush");
    assert_eq!(n, sent.len());

    // The knob's observable effect: more than one data file.
    let files = parquet_files(&dir.path().join("rolled"));
    assert!(
        files > 1,
        "a 16 KiB target over ~1 MB of data must roll into several files, got {files}. \
         If this is 1, `target_file_size` is being ignored again."
    );

    // And rolling must not lose or duplicate anything — the whole point is that
    // bounding memory is free of correctness cost.
    let landed = read_all(&uri).await;
    assert_eq!(
        landed.len(),
        sent.len(),
        "rolling must preserve every row; got {} of {}",
        landed.len(),
        sent.len()
    );
    let mut ids: Vec<i64> = landed
        .iter()
        .map(|r| r["id"].as_i64().expect("id"))
        .collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        (0..2_000).collect::<Vec<i64>>(),
        "no gaps, no duplicates"
    );
}

#[tokio::test]
async fn an_unset_target_keeps_the_single_commit_behaviour() {
    // The default must not change: unset means unbounded, one commit per run,
    // which is what anyone relying on today's behaviour gets.
    let dir = tempfile::tempdir().unwrap();
    let uri = table_uri(&dir, "single");

    let mut cfg = DeltaSinkConfig::new(&uri);
    cfg.target_file_size = None;
    let sink = DeltaSink::new(cfg).await.expect("sink");

    let sent = records(2_000);
    sink.write_batch(&sent).await.expect("write");
    sink.flush().await.expect("flush");

    assert_eq!(
        parquet_files(&dir.path().join("single")),
        1,
        "with no target, the run must still produce exactly one data file"
    );
    assert_eq!(read_all(&uri).await.len(), sent.len());
}

#[tokio::test]
async fn a_target_larger_than_the_data_produces_one_file() {
    // The knob is a ceiling, not a mandate to split.
    let dir = tempfile::tempdir().unwrap();
    let uri = table_uri(&dir, "one");

    let mut cfg = DeltaSinkConfig::new(&uri);
    cfg.target_file_size = Some(512 * 1024 * 1024);
    let sink = DeltaSink::new(cfg).await.expect("sink");

    let sent = records(50);
    sink.write_batch(&sent).await.expect("write");
    sink.flush().await.expect("flush");

    assert_eq!(parquet_files(&dir.path().join("one")), 1);
    assert_eq!(read_all(&uri).await.len(), 50);
}

#[tokio::test]
async fn rolling_across_multiple_write_batch_calls_is_still_exact() {
    // Multi-page: a roll can land mid-page or between pages, and neither may
    // drop a row.
    let dir = tempfile::tempdir().unwrap();
    let uri = table_uri(&dir, "pages");

    let mut cfg = DeltaSinkConfig::new(&uri);
    cfg.target_file_size = Some(8 * 1024);
    let sink = DeltaSink::new(cfg).await.expect("sink");

    let mut total = 0usize;
    for page in 0..5 {
        let batch: Vec<Value> = (0..400)
            .map(|i| json!({ "id": page * 400 + i, "payload": "y".repeat(512) }))
            .collect();
        total += sink.write_batch(&batch).await.expect("write");
    }
    sink.flush().await.expect("flush");

    assert_eq!(total, 2_000);
    let landed = read_all(&uri).await;
    assert_eq!(landed.len(), 2_000, "every row across every page must land");
    assert!(
        parquet_files(&dir.path().join("pages")) > 1,
        "an 8 KiB target across 5 pages must roll"
    );
}

#[tokio::test]
async fn a_zero_target_is_still_rejected_at_config_load() {
    // Pre-existing validation, pinned so wiring the knob did not loosen it — a
    // zero would otherwise commit on every single row.
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = DeltaSinkConfig::new(table_uri(&dir, "zero"));
    cfg.target_file_size = Some(0);
    assert!(
        cfg.validate().is_err(),
        "a zero target_file_size must remain a config error"
    );
}
