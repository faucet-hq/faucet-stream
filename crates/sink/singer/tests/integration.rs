#![allow(deprecated)]

//! End-to-end tests for `faucet-sink-singer` against real target subprocesses
//! (a dependency-free fake Singer target written in Python).

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use faucet_conformance::scripted::PagedSource;
use faucet_core::{FaucetError, MemoryStateStore, Pipeline, Sink, StateStore, Value, async_trait};
use faucet_sink_singer::{FlushOn, SingerSink, SingerSinkConfig};
use serde_json::json;

fn fake_target() -> String {
    format!(
        "{}/tests/fake_targets/fake_target.py",
        env!("CARGO_MANIFEST_DIR")
    )
}

struct Fixture {
    dir: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Self {
        Self {
            dir: tempfile::tempdir().unwrap(),
        }
    }
    fn out(&self) -> PathBuf {
        self.dir.path().join("out.jsonl")
    }
    fn log(&self) -> PathBuf {
        self.dir.path().join("log.jsonl")
    }
    fn config(&self, extra: Value) -> SingerSinkConfig {
        let mut target_config = json!({
            "path": self.out(),
            "log": self.log(),
        });
        for (k, v) in extra.as_object().unwrap() {
            target_config[k] = v.clone();
        }
        let mut cfg = SingerSinkConfig::new(fake_target());
        cfg.target_config = target_config;
        cfg.stream = Some("orders".into());
        cfg
    }
    fn records(&self) -> Vec<Value> {
        read_jsonl(&self.out())
    }
    fn events(&self, kind: &str) -> Vec<Value> {
        read_jsonl(&self.log())
            .into_iter()
            .filter(|e| e["event"] == kind)
            .collect()
    }
}

fn read_jsonl(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

/// A state store that, on every bookmark write, proves the target has already
/// persisted every record of the pages up to and including that bookmark and
/// logged receipt of the flush STATE.
struct CheckingStore {
    inner: MemoryStateStore,
    out: PathBuf,
    log: PathBuf,
    per_page: usize,
    require_echo: bool,
    puts: Mutex<Vec<Value>>,
}

#[async_trait]
impl StateStore for CheckingStore {
    async fn get(&self, key: &str) -> Result<Option<Value>, FaucetError> {
        self.inner.get(key).await
    }
    async fn put(&self, key: &str, value: &Value) -> Result<(), FaucetError> {
        let page = value
            .pointer("/data/page")
            .or_else(|| value.get("page"))
            .and_then(Value::as_u64)
            .ok_or_else(|| FaucetError::State(format!("unexpected bookmark {value}")))?
            as usize;
        let persisted = read_jsonl(&self.out).len();
        let expected = (page + 1) * self.per_page;
        if persisted < expected {
            return Err(FaucetError::State(format!(
                "bookmark for page {page} written before the target persisted it ({persisted} < {expected})"
            )));
        }
        let states = read_jsonl(&self.log)
            .into_iter()
            .filter(|e| e["event"] == "state")
            .count();
        {
            let mut puts = self.puts.lock().unwrap();
            if self.require_echo && states < puts.len() + 1 {
                return Err(FaucetError::State(format!(
                    "bookmark written before the target received flush STATE #{}",
                    puts.len() + 1
                )));
            }
            puts.push(value.clone());
        }
        self.inner.put(key, value).await
    }
    async fn delete(&self, key: &str) -> Result<(), FaucetError> {
        self.inner.delete(key).await
    }
}

fn checking_store(fx: &Fixture, per_page: usize, require_echo: bool) -> Arc<CheckingStore> {
    Arc::new(CheckingStore {
        inner: MemoryStateStore::new(),
        out: fx.out(),
        log: fx.log(),
        per_page,
        require_echo,
        puts: Mutex::new(Vec::new()),
    })
}

#[tokio::test]
async fn bookmark_advances_only_after_state_echo() {
    let fx = Fixture::new();
    let mut cfg = fx.config(json!({"mode": "echo"}));
    cfg.flush_on = FlushOn::State;
    let sink = SingerSink::new(cfg).unwrap();
    let source = PagedSource::new(4, 25);
    let store = checking_store(&fx, 25, true);
    let result = Pipeline::new(&source, &sink)
        .with_state_store(store.clone())
        .run()
        .await
        .unwrap();
    assert_eq!(result.records_written, 100);
    assert_eq!(store.puts.lock().unwrap().len(), 4);
    assert_eq!(fx.records().len(), 100);
    assert_eq!(
        fx.events("spawn").len(),
        1,
        "flush_on: state keeps one target"
    );
    let states = fx.events("state");
    assert_eq!(states.len(), 4);
    assert_eq!(states[3]["value"]["faucet_flush"]["seq"], json!(4));
    assert_eq!(
        states[3]["value"]["faucet_flush"]["stream"],
        json!("orders")
    );
    drop(sink);
    // Dropping the sink closes the target's input; it exits on its own.
    let deadline = Instant::now() + Duration::from_secs(10);
    while fx.events("exit").is_empty() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(fx.events("exit").len(), 1);
}

#[tokio::test]
async fn flush_on_exit_restarts_a_target_that_never_echoes() {
    let fx = Fixture::new();
    let mut cfg = fx.config(json!({"mode": "silent"}));
    cfg.flush_on = FlushOn::Exit;
    cfg.args = vec!["--extra".into(), "flag".into()];
    cfg.env.insert("FAKE_TARGET_TAG".into(), "tagged".into());
    let sink = SingerSink::new(cfg).unwrap();
    let source = PagedSource::new(3, 10);
    let store = checking_store(&fx, 10, false);
    let result = Pipeline::new(&source, &sink)
        .with_state_store(store.clone())
        .run()
        .await
        .unwrap();
    assert_eq!(result.records_written, 30);
    assert_eq!(fx.records().len(), 30);
    assert_eq!(store.puts.lock().unwrap().len(), 3);
    let spawns = fx.events("spawn");
    assert_eq!(spawns.len(), 3, "one target process per flush");
    assert_eq!(fx.events("exit").len(), 3);
    assert_eq!(spawns[0]["tag"], "tagged");
    assert_eq!(spawns[0]["args"], json!(["--extra", "flag"]));
    assert_eq!(
        fx.events("schema").len(),
        3,
        "each fresh target gets a SCHEMA"
    );
}

#[tokio::test]
async fn flush_on_state_times_out_on_a_target_that_never_echoes() {
    let fx = Fixture::new();
    let mut cfg = fx.config(json!({"mode": "silent"}));
    cfg.flush_on = FlushOn::State;
    cfg.flush_timeout_secs = 1;
    let sink = SingerSink::new(cfg).unwrap();
    sink.write_batch(&[json!({"id": 1})]).await.unwrap();
    let err = sink.flush().await.unwrap_err().to_string();
    assert!(err.contains("flush_on: exit"), "{err}");
    assert!(err.contains("within 1s"), "{err}");
}

#[tokio::test]
async fn crashing_target_fails_the_run_with_redacted_stderr() {
    let fx = Fixture::new();
    let secret = "hunter2-very-secret";
    let sink = SingerSink::new(fx.config(json!({
        "mode": "crash", "crash_after": 5, "secret": secret
    })))
    .unwrap();
    let source = PagedSource::new(2, 50);
    let store = checking_store(&fx, 50, false);
    let err = Pipeline::new(&source, &sink)
        .with_state_store(store.clone())
        .run()
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("fatal: cannot load record"), "{err}");
    assert!(err.contains("exit"), "{err}");
    assert!(!err.contains(secret), "stderr must be redacted: {err}");
    assert!(err.contains("***"), "{err}");
    assert!(
        store.puts.lock().unwrap().is_empty(),
        "no bookmark after a crash"
    );
}

#[tokio::test]
async fn crash_detected_at_flush_when_writes_fit_in_the_pipe() {
    let fx = Fixture::new();
    let mut cfg = fx.config(json!({"mode": "crash", "crash_after": 1, "secret": "s3cr3t-value"}));
    cfg.flush_on = FlushOn::State;
    let sink = SingerSink::new(cfg).unwrap();
    sink.write_batch(&[json!({"id": 1})]).await.unwrap();
    let err = sink.flush().await.unwrap_err().to_string();
    assert!(
        err.contains("fatal: cannot load record with key ***"),
        "{err}"
    );
}

#[tokio::test]
async fn target_that_closes_stdin_fails_the_write() {
    let fx = Fixture::new();
    let sink = SingerSink::new(fx.config(json!({"mode": "exit_early"}))).unwrap();
    let big = "x".repeat(4096);
    let records: Vec<Value> = (0..200).map(|i| json!({"id": i, "pad": big})).collect();
    let err = sink.write_batch(&records).await.unwrap_err().to_string();
    assert!(err.contains("closed its stdin"), "{err}");
    assert!(err.contains("refusing input"), "{err}");
    // The sink recovers: the next page starts a fresh target.
    let err2 = sink.write_batch(&records).await.unwrap_err().to_string();
    assert!(err2.contains("closed its stdin"), "{err2}");
    assert_eq!(fx.events("spawn").len(), 2);
}

#[tokio::test]
async fn exit_mode_reports_a_non_zero_exit_at_flush() {
    let fx = Fixture::new();
    let sink =
        SingerSink::new(fx.config(json!({"mode": "crash", "crash_after": 1, "secret": "zzzz"})))
            .unwrap();
    sink.write_batch(&[json!({"id": 1})]).await.unwrap();
    let err = sink.flush().await.unwrap_err().to_string();
    assert!(err.contains("exit"), "{err}");
    assert!(err.contains("fatal"), "{err}");
}

#[tokio::test]
async fn slow_target_back_pressures_the_writer() {
    let fx = Fixture::new();
    let sink = SingerSink::new(fx.config(json!({"mode": "echo", "delay_start": 1.5}))).unwrap();
    let pad = "y".repeat(1024);
    let records: Vec<Value> = (0..4000).map(|i| json!({"id": i, "pad": pad})).collect();
    let started = Instant::now();
    sink.write_batch(&records).await.unwrap();
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_millis(1200),
        "4 MB must not be buffered while the target is not reading (took {elapsed:?})"
    );
    sink.flush().await.unwrap();
    assert_eq!(fx.records().len(), 4000);
}

#[tokio::test]
async fn upsert_keys_and_widening_schema_are_sent() {
    let fx = Fixture::new();
    let mut cfg = fx.config(json!({"mode": "echo"}));
    cfg.flush_on = FlushOn::State;
    cfg.write.write_mode = faucet_core::WriteMode::Upsert;
    cfg.write.key = vec!["id".into()];
    let sink = SingerSink::new(cfg).unwrap();
    sink.write_batch(&[json!({"id": 1, "a": "x"})])
        .await
        .unwrap();
    sink.write_batch(&[json!({"id": 2, "a": "y"})])
        .await
        .unwrap();
    sink.write_batch(&[json!({"id": 3, "a": "z", "b": true})])
        .await
        .unwrap();
    sink.flush().await.unwrap();
    let schemas = fx.events("schema");
    assert_eq!(
        schemas.len(),
        2,
        "a SCHEMA again only when the schema widened"
    );
    assert_eq!(schemas[0]["key_properties"], json!(["id"]));
    assert!(schemas[0]["schema"]["properties"].get("b").is_none());
    assert_eq!(
        schemas[1]["schema"]["properties"]["b"]["type"],
        json!(["boolean", "null"])
    );
    assert_eq!(fx.records().len(), 3);
}

#[tokio::test]
async fn explicit_schema_is_sent_once_per_process() {
    let fx = Fixture::new();
    let mut cfg = fx.config(json!({"mode": "echo"}));
    cfg.schema = Some(json!({"type": "object", "properties": {"id": {"type": "integer"}}}));
    let sink = SingerSink::new(cfg).unwrap();
    sink.write_batch(&[json!({"id": 1})]).await.unwrap();
    sink.write_batch(&[json!({"id": 2, "extra": 1})])
        .await
        .unwrap();
    sink.flush().await.unwrap();
    sink.write_batch(&[json!({"id": 3})]).await.unwrap();
    sink.flush().await.unwrap();
    let schemas = fx.events("schema");
    assert_eq!(schemas.len(), 2, "one per target process");
    assert!(
        schemas
            .iter()
            .all(|s| s["schema"]["properties"].get("extra").is_none())
    );
}

async fn overwrite_run(fx: &Fixture, version: i64, ids: &[i64], commit: bool) {
    let mut cfg = fx.config(json!({"mode": "echo"}));
    cfg.write.write_mode = faucet_core::WriteMode::Overwrite;
    cfg.activate_version = Some(version);
    let writer = SingerSink::new(cfg.clone()).unwrap();
    let lifecycle = SingerSink::new(cfg).unwrap();
    assert!(lifecycle.is_overwrite());
    lifecycle.begin_overwrite().await.unwrap();
    let records: Vec<Value> = ids.iter().map(|i| json!({"id": i})).collect();
    writer.write_batch(&records).await.unwrap();
    writer.flush().await.unwrap();
    if commit {
        lifecycle.commit_overwrite().await.unwrap();
    } else {
        lifecycle.abort_overwrite().await.unwrap();
    }
}

#[tokio::test]
async fn overwrite_activates_the_new_version_only_on_commit() {
    let fx = Fixture::new();
    overwrite_run(&fx, 100, &[1, 2, 3], true).await;
    overwrite_run(&fx, 200, &[4, 5], true).await;
    let ids: Vec<i64> = fx
        .records()
        .iter()
        .map(|r| r["id"].as_i64().unwrap())
        .collect();
    assert_eq!(ids, [4, 5]);
    assert!(fx.records().iter().all(|r| r["_version"] == 200));
    let activates = fx.events("activate");
    assert_eq!(activates.len(), 2);
    assert_eq!(activates[1]["version"], json!(200));

    overwrite_run(&fx, 300, &[6], false).await;
    assert_eq!(fx.events("activate").len(), 2, "abort never activates");
    let ids: Vec<i64> = fx
        .records()
        .iter()
        .map(|r| r["id"].as_i64().unwrap())
        .collect();
    assert_eq!(
        ids,
        [4, 5, 6],
        "the previous version stays live after an abort"
    );
}

#[tokio::test]
async fn overwrite_on_one_instance_commits_its_own_records_first() {
    let fx = Fixture::new();
    let mut cfg = fx.config(json!({"mode": "echo"}));
    cfg.write.write_mode = faucet_core::WriteMode::Overwrite;
    cfg.flush_on = FlushOn::State;
    let sink = SingerSink::new(cfg).unwrap();
    sink.begin_overwrite().await.unwrap();
    sink.write_batch(&[json!({"id": 1})]).await.unwrap();
    sink.commit_overwrite().await.unwrap();
    let recs = fx.records();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0]["_version"], json!(sink.activate_version()));
    assert_eq!(fx.events("activate").len(), 1);
}

#[tokio::test]
async fn abort_stops_a_running_target() {
    let fx = Fixture::new();
    let mut cfg = fx.config(json!({"mode": "echo"}));
    cfg.write.write_mode = faucet_core::WriteMode::Overwrite;
    cfg.flush_on = FlushOn::State;
    let sink = SingerSink::new(cfg).unwrap();
    sink.write_batch(&[json!({"id": 1})]).await.unwrap();
    sink.abort_overwrite().await.unwrap();
    assert!(fx.events("activate").is_empty());
    sink.flush().await.unwrap();
}

#[tokio::test]
async fn interrupted_write_is_reported_and_the_target_restarted() {
    let fx = Fixture::new();
    let sink = SingerSink::new(fx.config(json!({"mode": "echo", "delay_start": 5}))).unwrap();
    let pad = "z".repeat(1024);
    let records: Vec<Value> = (0..2000).map(|i| json!({"id": i, "pad": pad})).collect();
    let cut = tokio::time::timeout(Duration::from_millis(300), sink.write_batch(&records)).await;
    assert!(cut.is_err(), "the write is blocked on the pipe");
    let err = sink
        .write_batch(&[json!({"id": 1})])
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("interrupted"), "{err}");
}

#[tokio::test]
async fn failed_flush_poisons_the_sink() {
    let fx = Fixture::new();
    let mut cfg = fx.config(json!({"mode": "silent"}));
    cfg.flush_on = FlushOn::State;
    cfg.flush_timeout_secs = 1;
    let sink = SingerSink::new(cfg).unwrap();
    sink.write_batch(&[json!({"id": 1})]).await.unwrap();
    assert!(sink.flush().await.is_err());
    let again = sink.flush().await.unwrap_err().to_string();
    assert!(again.contains("cannot be recovered"), "{again}");
    let write = sink
        .write_batch(&[json!({"id": 2})])
        .await
        .unwrap_err()
        .to_string();
    assert!(write.contains("cannot be recovered"), "{write}");
}

#[tokio::test]
async fn write_failure_after_unconfirmed_pages_poisons_the_sink() {
    let fx = Fixture::new();
    let mut cfg = fx.config(json!({"mode": "crash", "crash_after": 1, "secret": "abcd"}));
    cfg.flush_on = FlushOn::State;
    let sink = SingerSink::new(cfg).unwrap();
    sink.write_batch(&[json!({"id": 1})]).await.unwrap();
    let pad = "p".repeat(4096);
    let big: Vec<Value> = (0..500).map(|i| json!({"id": i, "pad": pad})).collect();
    let err = sink.write_batch(&big).await.unwrap_err().to_string();
    assert!(err.contains("closed its stdin"), "{err}");
    let err = sink
        .write_batch(&[json!({"id": 9})])
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("cannot be recovered"), "{err}");
}

#[tokio::test]
async fn interrupted_write_after_unconfirmed_pages_poisons_the_sink() {
    let fx = Fixture::new();
    let mut cfg = fx.config(json!({"mode": "echo", "delay_start": 5}));
    cfg.flush_on = FlushOn::State;
    let sink = SingerSink::new(cfg).unwrap();
    sink.write_batch(&[json!({"id": 0})]).await.unwrap();
    let pad = "z".repeat(1024);
    let records: Vec<Value> = (0..2000).map(|i| json!({"id": i, "pad": pad})).collect();
    let cut = tokio::time::timeout(Duration::from_millis(300), sink.write_batch(&records)).await;
    assert!(cut.is_err());
    let err = sink
        .write_batch(&[json!({"id": 1})])
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("interrupted"), "{err}");
    let err = sink.flush().await.unwrap_err().to_string();
    assert!(err.contains("cannot be recovered"), "{err}");
}

#[tokio::test]
async fn stderr_tail_keeps_the_last_lines() {
    let fx = Fixture::new();
    let mut cfg =
        fx.config(json!({"mode": "crash", "crash_after": 1, "secret": "abcd", "stderr_lines": 30}));
    cfg.flush_on = FlushOn::State;
    let sink = SingerSink::new(cfg).unwrap();
    sink.write_batch(&[json!({"id": 1})]).await.unwrap();
    let err = sink.flush().await.unwrap_err().to_string();
    assert!(err.contains("noise line 29"), "{err}");
    assert!(
        !err.contains("noise line 5\n"),
        "only the tail is kept: {err}"
    );
}

#[tokio::test]
async fn exit_without_reading_input_is_not_a_confirmation() {
    let fx = Fixture::new();
    let sink = SingerSink::new(fx.config(json!({"mode": "stale_state"}))).unwrap();
    sink.write_batch(&[json!({"id": 1})]).await.unwrap();
    let err = sink.flush().await.unwrap_err().to_string();
    assert!(
        err.contains("without echoing the final flush STATE") || err.contains("closed its stdin"),
        "{err}"
    );
}

#[tokio::test]
async fn target_that_never_exits_is_killed_after_the_timeout() {
    let fx = Fixture::new();
    let mut cfg = fx.config(json!({"mode": "hang"}));
    cfg.flush_timeout_secs = 1;
    let sink = SingerSink::new(cfg).unwrap();
    sink.write_batch(&[json!({"id": 1})]).await.unwrap();
    let err = sink.flush().await.unwrap_err().to_string();
    assert!(err.contains("did not exit within 1s"), "{err}");
    assert_eq!(fx.records().len(), 1);
}

#[test]
fn dropping_outside_a_runtime_kills_the_target() {
    let fx = Fixture::new();
    let mut cfg = fx.config(json!({"mode": "hang"}));
    cfg.flush_on = FlushOn::State;
    let sink = SingerSink::new(cfg).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(sink.write_batch(&[json!({"id": 1})])).unwrap();
    drop(rt);
    drop(sink);
}

#[tokio::test]
async fn echo_at_exit_target_works_with_flush_on_exit() {
    let fx = Fixture::new();
    let sink = SingerSink::new(fx.config(json!({"mode": "echo_at_exit"}))).unwrap();
    sink.write_batch(&[json!({"id": 1}), json!({"id": 2})])
        .await
        .unwrap();
    sink.flush().await.unwrap();
    assert_eq!(fx.records().len(), 2);
    sink.flush().await.unwrap();
}

#[test]
fn conformance_config_schema_and_capabilities() {
    let sink = SingerSink::new(SingerSinkConfig::new(fake_target())).unwrap();
    faucet_conformance::assert_config_schema_valid_value(
        &sink.config_schema(),
        sink.connector_name(),
    );
    assert_eq!(
        faucet_conformance::assert_batch_atomicity_declared(&sink),
        faucet_core::BatchAtomicity::BestEffort
    );
}

#[tokio::test]
async fn conformance_preflight_check_wellformed() {
    let sink = SingerSink::new(SingerSinkConfig::new(fake_target())).unwrap();
    faucet_conformance::assert_sink_preflight_check_wellformed(
        &sink,
        &faucet_core::check::CheckContext::default(),
    )
    .await;
    let report = sink
        .check(&faucet_core::check::CheckContext::default())
        .await
        .unwrap();
    assert_eq!(report.failed_count(), 0);
}

/// API-27: a target that stops reading used to stall the writer forever.
#[tokio::test]
async fn a_target_that_stops_reading_fails_the_write_after_flush_timeout() {
    let fx = Fixture::new();
    let mut cfg = fx.config(json!({"mode": "echo", "delay_start": 30}));
    cfg.flush_timeout_secs = 1;
    let sink = SingerSink::new(cfg).unwrap();
    let pad = "y".repeat(1024);
    let records: Vec<Value> = (0..4000).map(|i| json!({"id": i, "pad": pad})).collect();
    let started = Instant::now();
    let err = tokio::time::timeout(Duration::from_secs(20), sink.write_batch(&records))
        .await
        .expect("must not stall")
        .unwrap_err();
    assert!(
        err.to_string().contains("stopped reading its input"),
        "{err}"
    );
    assert!(started.elapsed() < Duration::from_secs(15));
}

/// API-60: the committing instance never wrote a record, so it knows no
/// schema; it must not send an empty SCHEMA with ACTIVATE_VERSION.
#[tokio::test]
async fn the_overwrite_commit_sends_no_empty_schema() {
    let fx = Fixture::new();
    overwrite_run(&fx, 100, &[1, 2], true).await;
    let schemas = fx.events("schema");
    assert!(!schemas.is_empty());
    assert!(
        schemas
            .iter()
            .all(|e| e["schema"] != json!({"type": "object", "properties": {}})),
        "{schemas:?}"
    );
    assert_eq!(fx.events("activate").len(), 1);
}
