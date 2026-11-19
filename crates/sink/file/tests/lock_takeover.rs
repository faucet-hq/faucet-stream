//! A writer that crashed while holding the output lock delays the next
//! commit only until its lease runs out (#949 follow-up).
#![allow(deprecated)] // the tests drive the 1.x lifecycle hooks directly (RFC 0014)
use faucet_common_file::write::{LockTimings, set_lock_timings};
use faucet_core::Sink;
use faucet_sink_file::FileSink;
use serde_json::json;
use std::time::{Duration, Instant};

#[tokio::test]
async fn a_crashed_holders_lock_is_taken_over_after_its_lease() {
    set_lock_timings(Some(LockTimings {
        lease: Duration::from_millis(600),
        renew: Duration::from_millis(100),
        poll: Duration::from_millis(20),
        wait_max: Duration::from_secs(30),
        abort_wait: Duration::from_millis(300),
    }));
    let dir = tempfile::tempdir().unwrap();
    let side = dir.path().join(".faucet-overwrite-o-_part_.jsonl.commits");
    std::fs::create_dir_all(&side).unwrap();
    std::fs::write(
        side.join(format!("lock-{:020}", 7)),
        "owner=crashed beat=9 wall=1 purpose=commit",
    )
    .unwrap();
    let cfg = json!({
        "path": dir.path().join("o-{part}.jsonl").to_string_lossy(),
        "write_mode": "overwrite",
        "max_records_per_file": 1,
    });
    let sink = || FileSink::new(serde_json::from_value(cfg.clone()).unwrap()).unwrap();
    sink().begin_overwrite().await.unwrap();
    let w = sink();
    w.write_batch(&[json!({"n": 1})]).await.unwrap();
    w.flush().await.unwrap();
    let started = Instant::now();
    sink().commit_overwrite().await.unwrap();
    assert!(
        started.elapsed() >= Duration::from_millis(600),
        "waited out the lease"
    );
    assert!(dir.path().join("o-00001.jsonl").exists());
    let locks: Vec<String> = std::fs::read_dir(&side)
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("lock-"))
        .collect();
    assert_eq!(locks, [format!("lock-{:020}", 8)]);
    assert_eq!(
        std::fs::read_to_string(side.join(&locks[0])).unwrap(),
        "released"
    );
    set_lock_timings(None);
}
