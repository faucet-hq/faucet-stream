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
