
use super::*;
use serde_json::json;

/// The run waits for the burst to end, so a save inside the debounce
/// window is never dropped (#789 CLI-156).
#[tokio::test]
async fn the_debounce_waits_for_the_last_save() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let sender = tokio::spawn(async move {
        for _ in 0..3 {
            tx.send(()).unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        tx
    });
    let start = Instant::now();
    assert!(wait_quiet(&mut rx, Duration::from_millis(80)).await);
    assert!(
        start.elapsed() >= Duration::from_millis(100),
        "{:?}",
        start.elapsed()
    );
    let tx = sender.await.unwrap();
    drop(tx);
    assert!(!wait_quiet(&mut rx, Duration::from_millis(80)).await);
}

#[test]
fn diff_records_detects_add_remove_keep() {
    let prev = vec![json!({"a": 1}), json!({"a": 2})];
    let curr = vec![json!({"a": 2}), json!({"a": 3})];
    let d = diff_records(&prev, &curr);
    assert_eq!(d.kept, 1);
    assert_eq!(d.added.len(), 1);
    assert_eq!(d.removed.len(), 1);
    assert!(d.added[0].contains("\"a\":3"));
    assert!(d.removed[0].contains("\"a\":1"));
}

#[test]
fn referenced_paths_extracts_extends_and_include() {
    let dir = Path::new("/cfg");
    let text = "extends: base.yaml\npipeline:\n  source: !include src.yaml\n";
    let paths = referenced_paths(dir, text);
    assert!(paths.contains(&PathBuf::from("/cfg/base.yaml")));
    assert!(paths.contains(&PathBuf::from("/cfg/src.yaml")));
}

#[test]
fn referenced_paths_handles_extends_list() {
    let dir = Path::new("/cfg");
    let text = "extends:\n  - base1.yaml\n  - base2.yaml\nname: x\n";
    let paths = referenced_paths(dir, text);
    assert!(paths.contains(&PathBuf::from("/cfg/base1.yaml")));
    assert!(paths.contains(&PathBuf::from("/cfg/base2.yaml")));
}

#[test]
fn should_refire_respects_min_gap() {
    let now = Instant::now();
    assert!(should_refire(None, now, Duration::from_millis(100)));
    assert!(!should_refire(Some(now), now, Duration::from_millis(100)));
    assert!(should_refire(
        Some(now - Duration::from_millis(200)),
        now,
        Duration::from_millis(100)
    ));
}

#[tokio::test]
async fn a_live_sample_reads_at_most_the_limit_from_the_real_source() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("in.csv"), "id\n1\n2\n3\n4\n").unwrap();
    let cfg = dir.path().join("p.yaml");
    std::fs::write(
            &cfg,
            format!(
                "version: 1\npipeline:\n  source:\n    type: csv\n    config:\n      path: {}\n  sink:\n    type: jsonl\n    config:\n      path: out.jsonl\n",
                dir.path().join("in.csv").display()
            ),
        )
        .unwrap();
    let args = DevArgs {
        config: cfg,
        row: None,
        sample: None,
        live: true,
        limit: 2,
        once: true,
        debounce_ms: 300,
        profile: None,
    };
    let records = live_sample(&args).await.unwrap();
    assert_eq!(records.len(), 2, "{records:?}");
    run(args).await.expect("faucet dev --live --once");
}

#[tokio::test]
async fn run_once_produces_output_offline() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = dir.path().join("p.yaml");
    std::fs::write(
            &cfg,
            "version: 1\npipeline:\n  source:\n    type: csv\n    config:\n      path: in.csv\n  sink:\n    type: jsonl\n    config:\n      path: out.jsonl\n",
        )
        .unwrap();
    let args = DevArgs {
        config: cfg,
        row: None,
        sample: None,
        live: false,
        limit: 10,
        once: true,
        debounce_ms: 300,
        profile: None,
    };
    let (out, err, _dlq) = run_once(&args, &[json!({"a": 1})]).await.unwrap();
    assert_eq!(out.len(), 1);
    assert!(err.is_none());
}
