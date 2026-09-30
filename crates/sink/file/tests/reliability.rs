//! Failure paths of the shared file writer, through the file sink (#783):
//! records reported as written are never lost silently, an interrupted
//! overwrite is finished by the next run, and storage I/O never blocks a
//! caller's timeout.

use faucet_core::Sink;
use faucet_sink_file::FileSink;
use serde_json::{Value, json};
use std::path::Path;

fn sink(v: Value) -> FileSink {
    FileSink::new(serde_json::from_value(v).unwrap()).unwrap()
}

fn p(dir: &Path, name: &str) -> String {
    dir.join(name).to_string_lossy().into_owned()
}

fn rows(from: usize, n: usize) -> Vec<Value> {
    (from..from + n).map(|i| json!({ "n": i })).collect()
}

fn lines(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

/// C1: a rollover that fails to publish its file loses the records earlier
/// pages put in it. The run must not go on to succeed without them.
#[tokio::test]
async fn a_failed_rollover_fails_the_rest_of_the_run() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("sub");
    let s = sink(json!({"path": p(&out, "r.jsonl"), "max_records_per_file": 3}));
    s.write_batch(&rows(0, 2)).await.unwrap();
    std::fs::remove_dir_all(&out).unwrap();
    assert!(s.write_batch(&rows(2, 2)).await.is_err());
    std::fs::create_dir_all(&out).unwrap();
    let later = s.write_batch(&rows(4, 1)).await;
    let flushed = s.flush().await;
    assert!(
        later.is_err() && flushed.is_err(),
        "the records of the first page were lost, so the run cannot succeed: {later:?} {flushed:?}"
    );
}

/// C5: when moving the overwritten files into place stops half-way, the
/// next run finishes the move instead of discarding the rest.
#[tokio::test]
async fn an_interrupted_overwrite_is_finished_by_the_next_run() {
    let dir = tempfile::tempdir().unwrap();
    for n in 1..=3 {
        std::fs::write(dir.path().join(format!("o-0000{n}.jsonl")), "{\"old\":1}\n").unwrap();
    }
    let cfg = json!({
        "path": p(dir.path(), "o-{part}.jsonl"),
        "write_mode": "overwrite",
        "max_records_per_file": 1,
    });
    sink(cfg.clone()).begin_overwrite().await.unwrap();
    let w = sink(cfg.clone());
    w.write_batch(&rows(0, 2)).await.unwrap();
    w.flush().await.unwrap();

    let blocker = dir.path().join("o-00002.jsonl");
    std::fs::remove_file(&blocker).unwrap();
    std::fs::create_dir(&blocker).unwrap();
    std::fs::write(blocker.join("x"), b"").unwrap();
    assert!(sink(cfg.clone()).commit_overwrite().await.is_err());
    std::fs::remove_dir_all(&blocker).unwrap();

    sink(cfg.clone()).begin_overwrite().await.unwrap();
    assert_eq!(lines(&dir.path().join("o-00001.jsonl")), rows(0, 1));
    assert_eq!(lines(&dir.path().join("o-00002.jsonl")), rows(1, 1));
    assert!(!dir.path().join("o-00003.jsonl").exists());
}

/// C5: an abort after the move started finishes it rather than leaving the
/// destination half old, half new.
#[tokio::test]
async fn an_abort_after_the_move_started_finishes_it() {
    let dir = tempfile::tempdir().unwrap();
    for n in 1..=3 {
        std::fs::write(dir.path().join(format!("o-0000{n}.jsonl")), "{\"old\":1}\n").unwrap();
    }
    let cfg = json!({
        "path": p(dir.path(), "o-{part}.jsonl"),
        "write_mode": "overwrite",
        "max_records_per_file": 1,
    });
    sink(cfg.clone()).begin_overwrite().await.unwrap();
    let w = sink(cfg.clone());
    w.write_batch(&rows(0, 2)).await.unwrap();
    w.flush().await.unwrap();
    let blocker = dir.path().join("o-00002.jsonl");
    std::fs::remove_file(&blocker).unwrap();
    std::fs::create_dir(&blocker).unwrap();
    std::fs::write(blocker.join("x"), b"").unwrap();
    assert!(sink(cfg.clone()).commit_overwrite().await.is_err());
    std::fs::remove_dir_all(&blocker).unwrap();

    sink(cfg.clone()).abort_overwrite().await.unwrap();
    assert_eq!(lines(&dir.path().join("o-00002.jsonl")), rows(1, 1));
    assert!(!dir.path().join("o-00003.jsonl").exists());
}

/// H2: a write stuck in storage I/O (here: reading a FIFO nobody writes)
/// does not stop the caller's timeout from firing.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stuck_storage_io_does_not_block_a_timeout() {
    let dir = tempfile::tempdir().unwrap();
    let fifo = dir.path().join("f.jsonl");
    let made = std::process::Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .unwrap();
    assert!(made.success());
    let s = sink(json!({"path": fifo.to_string_lossy(), "mode": "append"}));
    let unblock = {
        let fifo = fifo.clone();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_secs(3));
            drop(std::fs::OpenOptions::new().write(true).open(&fifo));
        })
    };
    let started = std::time::Instant::now();
    let r = tokio::time::timeout(
        std::time::Duration::from_millis(300),
        s.write_batch(&rows(0, 1)),
    )
    .await;
    let waited = started.elapsed();
    unblock.join().unwrap();
    assert!(r.is_err(), "the write is still waiting on the FIFO");
    assert!(
        waited < std::time::Duration::from_secs(2),
        "the timeout fired on time, not after the I/O: {waited:?}"
    );
}
