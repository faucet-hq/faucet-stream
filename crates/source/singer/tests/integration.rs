//! Integration tests: drive the fake Singer tap through a real
//! [`faucet_core::Pipeline`] into a keyed (upsert) sink double.
//!
//! These prove the whole design:
//!   (a) a clean run writes every row once (no duplicates);
//!   (b) a crash mid-run, then a resume, replays from the last persisted STATE
//!       and — with a keyed/idempotent sink absorbing the tap's coarse-resume
//!       overlap — produces **no duplicates**.
//!
//! The bridge source cannot deterministically replay (a Singer tap resumes
//! coarsely), so this uses at-least-once delivery + a keyed sink; that keyed
//! dedup is the real no-duplicate mechanism — effectively-once (idempotent
//! at-least-once) — exactly as a production SQLite/Postgres sink with
//! `write_mode: upsert` behaves.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use faucet_core::{FaucetError, Pipeline, StateStore, Value, async_trait};
use faucet_core::{MemoryStateStore, Sink};
use faucet_source_singer::{SingerSource, SingerSourceConfig};
use serde_json::json;

/// Absolute path to the dependency-free fake tap shipped with the crate.
fn fake_tap() -> String {
    format!("{}/tests/fake_taps/fake_tap.sh", env!("CARGO_MANIFEST_DIR"))
}

/// A sink that upserts by the record's `id` — the in-crate stand-in for a
/// SQLite/Postgres `write_mode: upsert` sink. Storing into a map keyed by `id`
/// means a re-delivered row overwrites rather than duplicates, so the final
/// contents contain each id once regardless of overlap on resume.
#[derive(Clone, Default)]
struct UpsertSink {
    rows: Arc<Mutex<BTreeMap<i64, Value>>>,
    /// Every write call's row count, to prove overlap actually happened.
    total_writes: Arc<Mutex<usize>>,
}

impl UpsertSink {
    fn ids(&self) -> Vec<i64> {
        self.rows.lock().unwrap().keys().copied().collect()
    }
    fn total_writes(&self) -> usize {
        *self.total_writes.lock().unwrap()
    }
}

#[async_trait]
impl Sink for UpsertSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        let mut rows = self.rows.lock().unwrap();
        for r in records {
            let id = r
                .get("id")
                .and_then(Value::as_i64)
                .ok_or_else(|| FaucetError::Sink("record missing integer `id`".into()))?;
            rows.insert(id, r.clone()); // upsert by key
        }
        *self.total_writes.lock().unwrap() += records.len();
        Ok(records.len())
    }

    fn connector_name(&self) -> &'static str {
        "test-upsert"
    }
}

fn config_with_args(args: &[&str]) -> SingerSourceConfig {
    SingerSourceConfig {
        args: args.iter().map(|s| s.to_string()).collect(),
        // Explicit, valid state key (independent of the tap's path).
        state_key: Some("singer_it".into()),
        ..SingerSourceConfig::new(fake_tap(), "s")
    }
}

#[tokio::test]
async fn clean_run_writes_all_rows_once() {
    let source = SingerSource::new(config_with_args(&["--stream", "s", "--total", "6"]));
    let sink = UpsertSink::default();
    let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());

    let result = Pipeline::new(&source, &sink)
        .with_state_store(store.clone())
        .run()
        .await
        .expect("clean run should succeed");

    assert_eq!(result.records_written, 6, "all 6 rows written");
    assert_eq!(sink.ids(), vec![1, 2, 3, 4, 5, 6]);
    // Bookmark persisted at the final STATE.
    let saved = store
        .get("singer_it")
        .await
        .unwrap()
        .map(|v| faucet_core::state_version::peel_versioned(&v));
    assert_eq!(saved, Some(serde_json::json!({"last_id": 6})));
}

#[tokio::test]
async fn crash_then_resume_produces_no_duplicates() {
    let sink = UpsertSink::default();
    let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());

    // ── Run 1: crashes after emitting records 4,5 (past the STATE at id=3). ──
    {
        let source = SingerSource::new(config_with_args(&[
            "--stream",
            "s",
            "--total",
            "6",
            "--state-at",
            "3",
            "--crash-after-new",
            "5",
        ]));
        let err = Pipeline::new(&source, &sink)
            .with_state_store(store.clone())
            .run()
            .await
            .expect_err("run 1 must fail (tap crashed)");
        match err {
            FaucetError::Source(_) => {}
            other => panic!("expected Source error, got {other:?}"),
        }
    }

    // Run 1 committed only through the STATE at id=3.
    assert_eq!(
        sink.ids(),
        vec![1, 2, 3],
        "only checkpointed rows are visible"
    );
    assert_eq!(
        store
            .get("singer_it")
            .await
            .unwrap()
            .map(|v| faucet_core::state_version::peel_versioned(&v)),
        Some(serde_json::json!({"last_id": 3}))
    );
    let writes_after_run1 = sink.total_writes();

    // ── Run 2: resume. The tap re-emits the boundary record (id=3) plus 4,5,6. ──
    {
        let source = SingerSource::new(config_with_args(&[
            "--stream",
            "s",
            "--total",
            "6",
            "--state-at",
            "6",
        ]));
        let result = Pipeline::new(&source, &sink)
            .with_state_store(store.clone())
            .run()
            .await
            .expect("resume run should succeed");
        // The tap re-emitted id=3 (overlap) plus 4,5,6 → 4 rows written.
        assert_eq!(result.records_written, 4);
    }

    // The overlap really happened: total write calls exceed the 6 unique ids.
    assert!(
        sink.total_writes() > 6,
        "expected an overlapping re-delivery (got {} total writes)",
        sink.total_writes()
    );
    assert!(sink.total_writes() > writes_after_run1);

    // …but the keyed sink deduped it: exactly ids 1..=6, each once.
    assert_eq!(
        sink.ids(),
        vec![1, 2, 3, 4, 5, 6],
        "no duplicates after crash + resume"
    );
    assert_eq!(
        store
            .get("singer_it")
            .await
            .unwrap()
            .map(|v| faucet_core::state_version::peel_versioned(&v)),
        Some(serde_json::json!({"last_id": 6}))
    );
}

#[tokio::test]
async fn discover_returns_catalog_streams() {
    let cfg = SingerSourceConfig::new(fake_tap(), "s");
    let catalog = faucet_source_singer::discover(&cfg)
        .await
        .expect("discovery should succeed");
    let ids = faucet_source_singer::catalog_stream_ids(&catalog);
    assert!(ids.contains(&"s".to_string()), "got {ids:?}");
    assert!(ids.contains(&"audit_log".to_string()), "got {ids:?}");
}

/// End-to-end against a real Python tap. Ignored by default: requires
/// `pip install tap-csv target-jsonl` (or a `tap-csv` on PATH) and network.
/// Run with: `cargo test -p faucet-source-singer -- --ignored real_tap`.
#[tokio::test]
#[ignore = "requires a real Singer tap installed on the machine"]
async fn real_tap_csv_end_to_end() {
    // Intentionally minimal: a real-tap smoke test is wired here so the harness
    // exists, but it must never run in CI without the tap present.
    let source = SingerSource::new(SingerSourceConfig::new("tap-csv", "sample"));
    let sink = UpsertSink::default();
    let _ = Pipeline::new(&source, &sink).run().await;
}

fn script(dir: &tempfile::TempDir, body: &str) -> String {
    use std::os::unix::fs::PermissionsExt;
    let path = dir.path().join("tap.sh");
    std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path.to_string_lossy().into_owned()
}

/// API-26: a configured stream the tap never uses on RECORD used to drop every
/// row and still checkpoint the tap's STATE.
#[tokio::test]
async fn records_for_only_another_stream_fail_before_checkpointing() {
    let mut cfg = config_with_args(&["--stream", "s", "--total", "3", "--state-at", "3"]);
    cfg.stream = "public-s".into();
    let source = SingerSource::new(cfg);
    let sink = UpsertSink::default();
    let err = faucet_core::Pipeline::new(&source, &sink).run().await.unwrap_err();
    assert!(err.to_string().contains("only for other streams"), "{err}");
}

/// API-26: the catalog's other name for the stream is accepted on RECORD.
#[tokio::test]
async fn the_catalog_alias_of_the_stream_is_accepted() {
    let mut cfg = config_with_args(&["--stream", "s", "--total", "3"]);
    cfg.stream = "public-s".into();
    cfg.catalog = Some(json!({"streams": [{"tap_stream_id": "public-s", "stream": "s"}]}));
    let source = SingerSource::new(cfg);
    let sink = UpsertSink::default();
    faucet_core::Pipeline::new(&source, &sink).run().await.unwrap();
    assert_eq!(sink.ids(), vec![1, 2, 3]);
}

/// API-44: a line longer than max_line_bytes fails instead of growing a buffer.
#[tokio::test]
async fn an_overlong_tap_line_fails() {
    let dir = tempfile::tempdir().unwrap();
    let tap = script(&dir, "head -c 5000 /dev/zero | tr '\\0' 'a'; echo; sleep 30");
    let mut cfg = SingerSourceConfig::new(tap, "s");
    cfg.max_line_bytes = 100;
    let source = SingerSource::new(cfg);
    let err = tokio::time::timeout(std::time::Duration::from_secs(20), faucet_core::Source::fetch_all(&source))
        .await
        .expect("must not hang")
        .unwrap_err();
    assert!(err.to_string().contains("max_line_bytes (100)"), "{err}");
}

/// API-45: discovery is bounded and never echoes the tap's credentials.
#[tokio::test]
async fn discovery_times_out_and_redacts_stderr() {
    let dir = tempfile::tempdir().unwrap();
    let tap = script(&dir, "echo 'bad token hunter2-secret-value' >&2; exit 1");
    let mut cfg = SingerSourceConfig::new(tap, "s");
    cfg.tap_config = json!({"api_token": "hunter2-secret-value"});
    let err = faucet_source_singer::discover(&cfg).await.unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("exited with status"), "{msg}");
    assert!(!msg.contains("hunter2-secret-value"), "{msg}");

    let slow = script(&dir, "sleep 30");
    let mut cfg = SingerSourceConfig::new(slow, "s");
    cfg.idle_timeout_secs = Some(1);
    let started = std::time::Instant::now();
    let err = faucet_source_singer::discover(&cfg).await.unwrap_err();
    assert!(err.to_string().contains("did not finish within 1s"), "{err}");
    assert!(started.elapsed() < std::time::Duration::from_secs(10));
}
