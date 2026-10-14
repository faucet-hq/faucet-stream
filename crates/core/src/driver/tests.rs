#![allow(deprecated)] // these tests drive the deprecated hooks DriverSink implements
#![allow(clippy::disallowed_methods)] // the tests drive time directly

use super::*;
use crate::accounting::WriteReport;
use crate::check::{CheckContext, CheckReport, Probe};
use crate::cleanup::SeenKeys;
use crate::drift::SchemaEvolution;
use crate::io::{CallContext, CallSlot};
use crate::rollback::{RollbackMode, RollbackOptions, RollbackOutcome};
use crate::traits::{Sink, Source};
use crate::write_mode::WriteMode;
use crate::{FaucetError, Pipeline, StreamPage};
use async_trait::async_trait;
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

type Log = Arc<Mutex<Vec<String>>>;

fn log(l: &Log, s: impl Into<String>) {
    l.lock().unwrap().push(s.into());
}

/// A driver with every group, backed by memory.
#[derive(Default)]
struct Full {
    log: Log,
    rows: Mutex<Vec<Value>>,
    staged: Mutex<Vec<Value>>,
    overwrite: bool,
    token: Mutex<Option<String>>,
}

impl Full {
    fn overwriting() -> Self {
        Self {
            overwrite: true,
            ..Default::default()
        }
    }
    fn rows(&self) -> Vec<Value> {
        self.rows.lock().unwrap().clone()
    }
}

#[async_trait]
impl SinkDriver for Full {
    fn connector_name(&self) -> &'static str {
        "full"
    }
    fn config_schema(&self) -> Value {
        json!({"full": true})
    }
    fn dataset_uri(&self) -> String {
        "full://x".into()
    }
    async fn check(&self, _: &CallSlot, _: &CheckContext) -> Result<CheckReport, FaucetError> {
        Ok(CheckReport::single(Probe::skip("full", "ok")))
    }
    async fn write(
        &self,
        _: &CallSlot,
        rows: &[Value],
        policy: RowPolicy,
    ) -> Result<PageWrite, FaucetError> {
        log(&self.log, format!("write {policy:?}"));
        let mut failed = Vec::new();
        let mut good = Vec::new();
        for (i, r) in rows.iter().enumerate() {
            if r.get("bad").is_some() {
                failed.push((i, FaucetError::Sink(format!("bad row {i}"))));
            } else {
                good.push(r.clone());
            }
        }
        if policy == RowPolicy::WholePage && !failed.is_empty() {
            return Ok(PageWrite::accepted(0).with_failures(failed));
        }
        let n = good.len();
        let target = if self.overwrite {
            &self.staged
        } else {
            &self.rows
        };
        target.lock().unwrap().extend(good);
        Ok(PageWrite::accepted(n).with_failures(failed))
    }
    async fn flush(&self, _: &CallSlot) -> Result<(), FaucetError> {
        log(&self.log, "flush");
        Ok(())
    }
    fn write_modes(&self) -> &'static [WriteMode] {
        &[WriteMode::Append, WriteMode::Upsert, WriteMode::Overwrite]
    }
    fn dedups_by_key(&self) -> bool {
        true
    }
    fn batch_atomicity(&self) -> crate::BatchAtomicity {
        crate::BatchAtomicity::Atomic
    }
    fn staged_load(&self) -> bool {
        true
    }
    fn capabilities(&self, derived: crate::Capabilities) -> crate::Capabilities {
        derived.to_builder().scoped_overwrite(true).build()
    }
    fn staging(&self) -> Option<&dyn StagingDriver> {
        Some(self)
    }
    fn commit_tokens(&self) -> Option<&dyn CommitTokenDriver> {
        Some(self)
    }
    fn cleanup(&self) -> Option<&dyn CleanupDriver> {
        Some(self)
    }
    fn schema(&self) -> Option<&dyn SchemaDriver> {
        Some(self)
    }
    fn rollback(&self) -> Option<&dyn RollbackDriver> {
        Some(self)
    }
    #[cfg(feature = "arrow")]
    fn columnar(&self) -> Option<&dyn ColumnarDriver> {
        Some(self)
    }
    fn readback_source(&self) -> Option<(String, Value)> {
        Some(("full".into(), json!({})))
    }
    async fn complete_run(&self, _: &CallSlot) -> Result<(), FaucetError> {
        log(&self.log, "complete");
        Ok(())
    }
    async fn local_outputs(&self) -> Vec<crate::LocalOutput> {
        vec![crate::LocalOutput::created("/full")]
    }
    fn set_call_context(&self, _: CallContext) {
        log(&self.log, "ctx");
    }
    fn set_roundtrip_recorder(&self, _: Arc<crate::observability::RoundtripRecorder>) {
        log(&self.log, "recorder");
    }
}

#[async_trait]
impl StagingDriver for Full {
    fn active(&self) -> bool {
        self.overwrite
    }
    async fn stage(&self, _: &CallSlot) -> Result<(), FaucetError> {
        log(&self.log, "stage");
        self.staged.lock().unwrap().clear();
        Ok(())
    }
    async fn publish(&self, _: &CallSlot) -> Result<(), FaucetError> {
        log(&self.log, "publish");
        let staged = std::mem::take(&mut *self.staged.lock().unwrap());
        *self.rows.lock().unwrap() = staged;
        Ok(())
    }
    async fn discard(&self, _: &CallSlot) -> Result<(), FaucetError> {
        log(&self.log, "discard");
        self.staged.lock().unwrap().clear();
        Ok(())
    }
    async fn staged(&self, _: &CallSlot) -> Result<bool, FaucetError> {
        Ok(!self.staged.lock().unwrap().is_empty())
    }
}

#[async_trait]
impl CommitTokenDriver for Full {
    async fn last_token(&self, _: &CallSlot, _: &str) -> Result<Option<String>, FaucetError> {
        Ok(self.token.lock().unwrap().clone())
    }
    async fn write_with_token(
        &self,
        _: &CallSlot,
        rows: &[Value],
        _: &str,
        token: &str,
    ) -> Result<WriteReport, FaucetError> {
        self.rows.lock().unwrap().extend_from_slice(rows);
        *self.token.lock().unwrap() = Some(token.into());
        Ok(WriteReport::accepted(rows.len()))
    }
}

#[async_trait]
impl CleanupDriver for Full {
    async fn delete_absent(
        &self,
        _: &CallSlot,
        _: &BTreeMap<String, Value>,
        _: &SeenKeys,
    ) -> Result<u64, FaucetError> {
        Ok(4)
    }
}

#[async_trait]
impl SchemaDriver for Full {
    async fn current_schema(&self, _: &CallSlot) -> Result<Option<Value>, FaucetError> {
        Ok(Some(json!({"type": "object"})))
    }
    fn evolvable(&self) -> bool {
        true
    }
    async fn evolve(&self, _: &CallSlot, _: &SchemaEvolution) -> Result<(), FaucetError> {
        log(&self.log, "evolve");
        Ok(())
    }
}

#[async_trait]
impl RollbackDriver for Full {
    async fn rollback_run(
        &self,
        _: &CallSlot,
        _: &str,
        _: &RollbackOptions,
    ) -> Result<RollbackOutcome, FaucetError> {
        Ok(RollbackOutcome::blocked(1))
    }
    async fn forget_run(&self, _: &CallSlot, _: &str) -> Result<(), FaucetError> {
        log(&self.log, "forget");
        Ok(())
    }
    async fn rewind_commit_token(
        &self,
        _: &CallSlot,
        _: &str,
        token: Option<&str>,
    ) -> Result<(), FaucetError> {
        *self.token.lock().unwrap() = token.map(str::to_string);
        Ok(())
    }
}

#[cfg(feature = "arrow")]
#[async_trait]
impl ColumnarDriver for Full {
    async fn write_columnar(
        &self,
        _: &CallSlot,
        batch: &arrow::array::RecordBatch,
    ) -> Result<usize, FaucetError> {
        log(&self.log, "columnar");
        Ok(batch.num_rows())
    }
}

/// A driver with no group: append only.
struct Bare;

#[async_trait]
impl SinkDriver for Bare {
    fn connector_name(&self) -> &'static str {
        "bare"
    }
    async fn write(
        &self,
        _: &CallSlot,
        rows: &[Value],
        _: RowPolicy,
    ) -> Result<PageWrite, FaucetError> {
        Ok(PageWrite::accepted(rows.len()))
    }
}

fn opts() -> RollbackOptions {
    RollbackOptions {
        run_id_column: "_faucet_run_id".into(),
        mode: RollbackMode::Append,
        force: false,
        dry_run: true,
        later_runs: false,
    }
}

#[tokio::test]
async fn capabilities_follow_the_groups_a_driver_has() {
    let full = DriverSink::new(Full::default());
    full.check_declarations().unwrap();
    let caps = full.capabilities();
    assert!(caps.overwrite() && caps.idempotent_writes() && caps.cleanup());
    assert!(caps.rollback() && caps.schema_evolution() && caps.keyed_dedup());
    assert!(caps.staged_load() && caps.scoped_overwrite());
    assert!(full.supports_idempotent_writes() && full.supports_cleanup());
    assert!(full.write_batch_is_replay_safe() && full.dedups_by_key());
    assert_eq!(full.batch_atomicity(), crate::BatchAtomicity::Atomic);
    assert_eq!(full.connector_name(), "full");
    assert_eq!(full.config_schema(), json!({"full": true}));
    assert_eq!(full.dataset_uri(), "full://x");
    assert!(full.readback_source().is_some());
    assert_eq!(full.local_outputs().await.len(), 1);
    assert!(full.check(&CheckContext::default()).await.is_ok());

    let bare = DriverSink::new(Bare);
    bare.check_declarations().unwrap();
    let caps = bare.capabilities();
    assert!(!caps.overwrite() && !caps.idempotent_writes() && !caps.cleanup());
    assert!(!caps.rollback() && !caps.schema_evolution());
    assert!(!bare.is_overwrite());
    assert_eq!(bare.supported_write_modes(), &[WriteMode::Append]);
    assert!(bare.current_schema().await.unwrap().is_none());
    assert_eq!(bare.dataset_uri(), "bare://unknown");
    assert_eq!(bare.config_schema()["type"], "object");
    assert!(bare.readback_source().is_none());
    assert!(bare.local_outputs().await.is_empty());
    assert!(bare.last_committed_token("s").await.unwrap().is_none());
    assert!(bare.overwrite_staging_exists().await.unwrap().is_none());
    assert!(bare.abort_overwrite().await.is_ok());
    assert!(bare.forget_run("r").await.is_ok());
    assert!(bare.flush().await.is_ok());
    assert!(bare.complete_run().await.is_ok());
    for err in [
        bare.begin_overwrite().await.unwrap_err(),
        bare.commit_overwrite().await.unwrap_err(),
        bare.evolve_schema(&SchemaEvolution::default())
            .await
            .unwrap_err(),
        bare.cleanup_scope(&BTreeMap::new(), &SeenKeys::new())
            .await
            .unwrap_err(),
        bare.rollback_run("r", &opts()).await.unwrap_err(),
        bare.rewind_commit_token("s", None).await.unwrap_err(),
    ] {
        assert!(err.to_string().contains("bare"), "{err}");
    }
    let report = bare.check(&CheckContext::default()).await.unwrap();
    assert_eq!(report.probes[0].name, "check");
    assert_eq!(
        bare.write_batch_idempotent(&[json!(1)], "s", "t")
            .await
            .unwrap(),
        1,
        "without commit tokens the idempotent write is a plain one"
    );
}

#[tokio::test]
async fn one_write_primitive_serves_the_plain_reported_and_per_row_paths() {
    let sink = DriverSink::new(Full::default());
    let page = [json!({"id": 1}), json!({"bad": true}), json!({"id": 3})];
    let err = sink.write_batch(&page).await.unwrap_err();
    assert!(err.to_string().contains("bad row 1"), "{err}");
    assert!(
        sink.driver().rows().is_empty(),
        "a whole-page failure writes nothing"
    );
    assert!(sink.write_batch_reported(&page).await.is_err());

    let outcomes = sink.write_batch_partial(&page).await.unwrap();
    assert!(outcomes[0].is_ok() && outcomes[1].is_err() && outcomes[2].is_ok());
    assert_eq!(sink.driver().rows().len(), 2);
    assert_eq!(
        sink.write_batch_reported(&[json!({"id": 4})])
            .await
            .unwrap()
            .accepted,
        1
    );
    assert_eq!(sink.write_batch(&[json!({"id": 5})]).await.unwrap(), 1);
}

/// A driver that loses a row on the per-row path, or reports a row the page
/// does not have.
struct Lossy {
    out_of_range: bool,
}

#[async_trait]
impl SinkDriver for Lossy {
    fn connector_name(&self) -> &'static str {
        "lossy"
    }
    async fn write(
        &self,
        _: &CallSlot,
        rows: &[Value],
        _: RowPolicy,
    ) -> Result<PageWrite, FaucetError> {
        if self.out_of_range {
            return Ok(PageWrite::accepted(rows.len())
                .with_failures(vec![(rows.len() + 3, FaucetError::Sink("x".into()))]));
        }
        Ok(PageWrite::accepted(rows.len().saturating_sub(1)))
    }
}

#[tokio::test]
async fn a_per_row_write_that_loses_rows_fails_the_page() {
    let sink = DriverSink::new(Lossy {
        out_of_range: false,
    });
    let err = sink
        .write_batch_partial(&[json!(1), json!(2)])
        .await
        .unwrap_err();
    assert!(matches!(err, FaucetError::RowsUnaccounted { .. }), "{err}");
    let sink = DriverSink::new(Lossy { out_of_range: true });
    let err = sink.write_batch_partial(&[json!(1)]).await.unwrap_err();
    assert!(err.to_string().contains("row 4"), "{err}");
}

#[tokio::test]
async fn the_overwrite_lifecycle_runs_on_the_staging_primitives() {
    let sink = DriverSink::new(Full::overwriting());
    assert!(sink.is_overwrite());
    sink.begin_overwrite().await.unwrap();
    sink.write_batch(&[json!({"id": 1})]).await.unwrap();
    assert!(sink.overwrite_staging_exists().await.unwrap().unwrap());
    assert!(
        sink.driver().rows().is_empty(),
        "staged rows stay invisible"
    );
    sink.commit_overwrite().await.unwrap();
    assert_eq!(sink.driver().rows(), vec![json!({"id": 1})]);
    sink.write_batch(&[json!({"id": 2})]).await.unwrap();
    sink.abort_overwrite().await.unwrap();
    assert!(!sink.overwrite_staging_exists().await.unwrap().unwrap());
    assert_eq!(sink.driver().rows(), vec![json!({"id": 1})]);
    let log = sink.driver().log.lock().unwrap().clone();
    assert!(log.contains(&"stage".to_string()) && log.contains(&"publish".to_string()));
    assert!(log.contains(&"discard".to_string()));
}

#[tokio::test]
async fn the_pipeline_drives_a_driver_sink_end_to_end() {
    struct Rows(Vec<Value>);
    #[async_trait]
    impl Source for Rows {
        async fn fetch_with_context(
            &self,
            _: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(self.0.clone())
        }
    }
    let sink = DriverSink::new(Full::overwriting());
    let r = Pipeline::new(&Rows(vec![json!({"id": 1}), json!({"id": 2})]), &sink)
        .run()
        .await
        .unwrap();
    assert_eq!(r.records_written, 2);
    assert_eq!(sink.driver().rows().len(), 2, "published after the run");
    let log = sink.driver().log.lock().unwrap().clone();
    let pos = |s: &str| log.iter().position(|l| l == s).unwrap();
    assert!(pos("stage") < pos("write WholePage") && pos("flush") < pos("publish"));
    assert!(log.contains(&"complete".to_string()));
}

#[tokio::test]
async fn exactly_once_cleanup_schema_and_rollback_reach_their_primitives() {
    let sink = DriverSink::new(Full::default());
    let report = sink
        .write_batch_idempotent_reported(&[json!({"id": 1})], "s", "t1")
        .await
        .unwrap();
    assert_eq!(report.accepted, 1);
    assert_eq!(
        sink.write_batch_idempotent(&[json!({"id": 2})], "s", "t2")
            .await
            .unwrap(),
        1
    );
    assert_eq!(sink.last_committed_token("s").await.unwrap().unwrap(), "t2");
    sink.rewind_commit_token("s", Some("t1")).await.unwrap();
    assert_eq!(sink.last_committed_token("s").await.unwrap().unwrap(), "t1");
    assert_eq!(
        sink.cleanup_scope(&BTreeMap::new(), &SeenKeys::new())
            .await
            .unwrap(),
        4
    );
    assert!(sink.current_schema().await.unwrap().is_some());
    assert!(sink.supports_schema_evolution());
    sink.evolve_schema(&SchemaEvolution::default())
        .await
        .unwrap();
    assert_eq!(sink.rollback_run("r", &opts()).await.unwrap().conflicts, 1);
    sink.forget_run("r").await.unwrap();
    sink.set_call_context(CallContext::new());
    sink.set_roundtrip_recorder(Arc::new(crate::observability::RoundtripRecorder::new(
        crate::observability::RoundtripSide::Sink,
        "p",
        "r",
        "full",
    )));
    let log = sink.driver().log.lock().unwrap().clone();
    for s in ["evolve", "forget", "ctx", "recorder"] {
        assert!(log.contains(&s.to_string()), "{s}");
    }
}

#[cfg(feature = "arrow")]
#[tokio::test]
async fn the_columnar_path_reaches_the_columnar_driver_or_falls_back_to_rows() {
    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![1, 2]))],
    )
    .unwrap();
    let full = DriverSink::new(Full::default());
    assert!(full.supports_columnar());
    assert_eq!(full.write_batch_columnar(&batch).await.unwrap(), 2);
    let bare = DriverSink::new(Bare);
    assert!(!bare.supports_columnar());
    assert_eq!(bare.write_batch_columnar(&batch).await.unwrap(), 2);
}

/// Overwrite declared without staging, keyed dedup without a keyed mode.
struct Inconsistent {
    dedup: bool,
}

#[async_trait]
impl SinkDriver for Inconsistent {
    fn connector_name(&self) -> &'static str {
        "inconsistent"
    }
    async fn write(
        &self,
        _: &CallSlot,
        rows: &[Value],
        _: RowPolicy,
    ) -> Result<PageWrite, FaucetError> {
        Ok(PageWrite::accepted(rows.len()))
    }
    fn write_modes(&self) -> &'static [WriteMode] {
        if self.dedup {
            &[WriteMode::Append]
        } else {
            &[WriteMode::Append, WriteMode::Overwrite]
        }
    }
    fn dedups_by_key(&self) -> bool {
        self.dedup
    }
}

#[test]
fn declarations_that_disagree_with_the_primitives_are_refused() {
    let err = DriverSink::new_unchecked(Inconsistent { dedup: false })
        .check_declarations()
        .unwrap_err();
    assert!(
        err.to_string().contains("overwrite is declared on"),
        "{err}"
    );
    let err = DriverSink::new_unchecked(Inconsistent { dedup: true })
        .check_declarations()
        .unwrap_err();
    assert!(err.to_string().contains("no keyed write mode"), "{err}");
    struct StagingWithoutMode(Full);
    #[async_trait]
    impl SinkDriver for StagingWithoutMode {
        fn connector_name(&self) -> &'static str {
            "s"
        }
        async fn write(
            &self,
            c: &CallSlot,
            rows: &[Value],
            p: RowPolicy,
        ) -> Result<PageWrite, FaucetError> {
            self.0.write(c, rows, p).await
        }
        fn staging(&self) -> Option<&dyn StagingDriver> {
            Some(&self.0)
        }
    }
    let err = DriverSink::new_unchecked(StagingWithoutMode(Full::default()))
        .check_declarations()
        .unwrap_err();
    assert!(err.to_string().contains("declared off"), "{err}");
    let built = std::panic::catch_unwind(|| DriverSink::new(Inconsistent { dedup: false }));
    assert_eq!(built.is_err(), cfg!(debug_assertions));
    let mut s = DriverSink::new(Bare);
    let _ = s.driver_mut();
    let _ = s.calls();
    let _ = s.into_driver();
}

#[test]
fn page_write_constructors() {
    let w = PageWrite::from_outcomes(vec![Ok(()), Err(FaucetError::Sink("x".into())), Ok(())]);
    assert_eq!(w.report.accepted, 2);
    assert_eq!(w.failed.len(), 1);
    assert_eq!(w.failed[0].0, 1);
    let w = PageWrite::reported(WriteReport::accepted(3).with_coalesced(1));
    assert_eq!(w.report.coalesced, 1);
}

// ── sources ────────────────────────────────────────────────────────────────

struct Pages {
    pages: Vec<StreamPage>,
    resumed: Mutex<Option<Value>>,
    probe: bool,
}

#[async_trait]
impl SourceDriver for Pages {
    fn connector_name(&self) -> &'static str {
        "pages"
    }
    fn config_schema(&self) -> Value {
        json!({"pages": 1})
    }
    fn dataset_uri(&self) -> String {
        "pages://x".into()
    }
    fn read<'a>(
        &'a self,
        _: &'a CallSlot,
        _: &'a HashMap<String, Value>,
        _: usize,
    ) -> PageStream<'a> {
        let pages: Vec<Result<StreamPage, FaucetError>> = self
            .pages
            .iter()
            .map(|p| {
                Ok(StreamPage {
                    records: p.records.clone(),
                    bookmark: p.bookmark.clone(),
                })
            })
            .collect();
        Box::pin(futures::stream::iter(pages))
    }
    fn state_key(&self) -> Option<String> {
        Some("pages".into())
    }
    async fn resume_from(&self, bookmark: Value) -> Result<(), FaucetError> {
        *self.resumed.lock().unwrap() = Some(bookmark);
        Ok(())
    }
    fn deterministic_replay(&self) -> bool {
        true
    }
    fn set_cancel(&self, _: crate::CancellationToken) -> bool {
        true
    }
    async fn check(
        &self,
        _: &CallSlot,
        _: &CheckContext,
    ) -> Result<Option<CheckReport>, FaucetError> {
        Ok(self
            .probe
            .then(|| CheckReport::single(Probe::skip("pages", "custom"))))
    }
    fn capabilities(&self, derived: crate::Capabilities) -> crate::Capabilities {
        derived.to_builder().reports_lag(true).build()
    }
}

fn pages(probe: bool) -> Pages {
    Pages {
        pages: vec![
            StreamPage {
                records: vec![json!(1), json!(2)],
                bookmark: Some(json!(2)),
            },
            StreamPage {
                records: vec![json!(3)],
                bookmark: None,
            },
        ],
        resumed: Mutex::new(None),
        probe,
    }
}

#[tokio::test]
async fn a_driver_source_reads_resumes_and_probes() {
    let source = DriverSource::new(pages(false));
    assert_eq!(source.fetch_all().await.unwrap().len(), 3);
    let (rows, bm) = source.fetch_all_incremental().await.unwrap();
    assert_eq!((rows.len(), bm), (3, Some(json!(2))));
    source.apply_start_bookmark(json!(7)).await.unwrap();
    assert_eq!(*source.driver().resumed.lock().unwrap(), Some(json!(7)));
    assert_eq!(source.state_key().as_deref(), Some("pages"));
    assert!(source.supports_exactly_once() && !source.consumes_destructively());
    assert!(source.set_cancel(crate::CancellationToken::new()));
    assert!(source.capabilities().reports_lag() && source.capabilities().exactly_once());
    assert_eq!(source.connector_name(), "pages");
    assert_eq!(source.config_schema(), json!({"pages": 1}));
    assert_eq!(source.dataset_uri(), "pages://x");
    source.set_call_context(CallContext::new());
    let report = source.check(&CheckContext::default()).await.unwrap();
    assert_eq!(report.failed_count(), 0, "{report:?}");
    assert_eq!(report.probes[0].name, "read");
    let custom = DriverSource::new(pages(true))
        .check(&CheckContext::default())
        .await
        .unwrap();
    assert_eq!(custom.probes[0].name, "pages");
    let mut s = DriverSource::new(pages(false));
    let _ = s.driver_mut();
    let _ = s.calls();
    let _ = s.into_driver();
}

struct FailingRead;

#[async_trait]
impl SourceDriver for FailingRead {
    fn connector_name(&self) -> &'static str {
        "failing"
    }
    fn read<'a>(
        &'a self,
        _: &'a CallSlot,
        _: &'a HashMap<String, Value>,
        _: usize,
    ) -> PageStream<'a> {
        Box::pin(futures::stream::iter(vec![Err(FaucetError::Source(
            "down".into(),
        ))]))
    }
}

#[tokio::test]
async fn a_failing_read_fails_the_fetch_and_the_probe() {
    let s = DriverSource::new(FailingRead);
    assert!(s.fetch_all().await.is_err());
    let report = s.check(&CheckContext::default()).await.unwrap();
    assert_eq!(report.failed_count(), 1);
    assert_eq!(s.dataset_uri(), "failing://unknown");
    assert_eq!(s.config_schema()["type"], "object");
    assert!(s.state_key().is_none() && !s.supports_exactly_once());
    assert!(!s.set_cancel(crate::CancellationToken::new()));
    s.apply_start_bookmark(json!(1)).await.unwrap();
}

// ── messaging ──────────────────────────────────────────────────────────────

#[derive(Default)]
struct Broker {
    published: Mutex<Vec<OutboundMessage>>,
    nack: Mutex<Vec<String>>,
    hang_group: Mutex<Option<usize>>,
    groups: AtomicUsize,
    transport_down: AtomicBool,
    hang_flush: AtomicBool,
    timeout: Option<Duration>,
    size: usize,
    tx: Option<Tx>,
    own_calls: bool,
    page_err: bool,
}

#[derive(Default)]
struct Tx {
    open: Mutex<Vec<OutboundMessage>>,
    in_tx: AtomicBool,
    token: Mutex<Option<String>>,
    fail_commit: AtomicBool,
}

#[async_trait]
impl MessageDriver for Broker {
    fn connector_name(&self) -> &'static str {
        "broker"
    }
    fn encode(&self, record: &Value) -> Result<OutboundMessage, FaucetError> {
        let subject = record["to"]
            .as_str()
            .ok_or_else(|| FaucetError::Sink("no subject".into()))?;
        Ok(OutboundMessage::new(subject, record.to_string())
            .with_key(b"k".to_vec())
            .with_header("h", "v"))
    }
    fn group_size(&self) -> usize {
        self.size
    }
    fn group_timeout(&self) -> Option<Duration> {
        self.timeout
    }
    async fn publish(
        &self,
        _: &CallSlot,
        group: Vec<OutboundMessage>,
    ) -> Result<Vec<crate::RowOutcome>, FaucetError> {
        let g = self.groups.fetch_add(1, Ordering::SeqCst);
        if self.transport_down.load(Ordering::SeqCst) {
            return Err(FaucetError::Sink("transport down".into()));
        }
        if *self.hang_group.lock().unwrap() == Some(g) {
            tokio::time::sleep(Duration::from_secs(3600)).await;
        }
        let nack = self.nack.lock().unwrap().clone();
        let mut out = Vec::new();
        for m in group {
            if nack.contains(&m.destination) {
                out.push(Err(FaucetError::Sink("nacked".into())));
            } else {
                match &self.tx {
                    Some(tx) if tx.in_tx.load(Ordering::SeqCst) => tx.open.lock().unwrap().push(m),
                    _ => self.published.lock().unwrap().push(m),
                }
                out.push(Ok(()));
            }
        }
        Ok(out)
    }
    async fn flush(&self, _: &CallSlot) -> Result<(), FaucetError> {
        if self.hang_flush.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_secs(3600)).await;
        }
        Ok(())
    }
    fn transactions(&self) -> Option<&dyn MessageTransactions> {
        self.tx.as_ref().map(|t| t as &dyn MessageTransactions)
    }
    fn publish_is_one_call(&self) -> bool {
        !self.own_calls
    }
    fn page_error(&self, offered: usize, failed: &[(usize, FaucetError)]) -> Option<FaucetError> {
        self.page_err.then(|| {
            FaucetError::Sink(format!(
                "{} of {offered} failed (first at {})",
                failed.len(),
                failed[0].0
            ))
        })
    }
}

#[async_trait]
impl MessageTransactions for Tx {
    async fn begin(&self, _: &CallSlot, _: &str) -> Result<(), FaucetError> {
        self.in_tx.store(true, Ordering::SeqCst);
        Ok(())
    }
    async fn commit(&self, _: &CallSlot, _: &str, token: &str) -> Result<(), FaucetError> {
        if self.fail_commit.load(Ordering::SeqCst) {
            return Err(FaucetError::Sink("commit refused".into()));
        }
        self.in_tx.store(false, Ordering::SeqCst);
        *self.token.lock().unwrap() = Some(token.into());
        Ok(())
    }
    async fn abort(&self, _: &CallSlot, _: &str) {
        self.in_tx.store(false, Ordering::SeqCst);
        self.open.lock().unwrap().clear();
    }
    async fn last_token(&self, _: &CallSlot, _: &str) -> Result<Option<String>, FaucetError> {
        Ok(self.token.lock().unwrap().clone())
    }
}

fn msg(to: &str) -> Value {
    json!({ "to": to })
}

#[tokio::test]
async fn a_message_sink_reports_each_row_and_fails_the_page_on_the_first() {
    let broker = Broker {
        size: 2,
        ..Default::default()
    };
    broker.nack.lock().unwrap().push("nope".into());
    let sink = DriverSink::new(MessageSinkDriver::new(broker));
    let page = [msg("a"), json!({}), msg("nope"), msg("b")];
    let outcomes = sink.write_batch_partial(&page).await.unwrap();
    assert!(outcomes[0].is_ok() && outcomes[3].is_ok());
    assert!(
        outcomes[1]
            .as_ref()
            .unwrap_err()
            .to_string()
            .contains("no subject")
    );
    assert!(
        outcomes[2]
            .as_ref()
            .unwrap_err()
            .to_string()
            .contains("nacked")
    );
    let err = sink.write_batch(&page).await.unwrap_err();
    assert!(
        err.to_string().contains("no subject"),
        "the first row's error"
    );
    let published = sink.driver().driver().published.lock().unwrap().clone();
    assert_eq!(published[0].key.as_deref(), Some(b"k".as_slice()));
    assert_eq!(published[0].headers, vec![("h".into(), "v".into())]);
    assert!(sink.write_batch(&[]).await.is_ok());
    sink.flush().await.unwrap();
    assert!(!sink.supports_idempotent_writes());
    assert!(sink.last_committed_token("s").await.unwrap().is_none());
}

#[tokio::test]
async fn a_driver_can_word_the_whole_page_error() {
    let broker = Broker {
        page_err: true,
        ..Default::default()
    };
    let sink = DriverSink::new(MessageSinkDriver::new(broker));
    let page = [msg("a"), json!({}), json!({})];
    let err = sink.write_batch(&page).await.unwrap_err();
    assert_eq!(err.to_string(), "Sink error: 2 of 3 failed (first at 1)");
    let outcomes = sink.write_batch_partial(&page).await.unwrap();
    assert!(outcomes[0].is_ok() && outcomes[1].is_err() && outcomes[2].is_err());
    assert_eq!(sink.write_batch(&[msg("b")]).await.unwrap(), 1);
}

#[tokio::test]
async fn a_driver_making_its_own_calls_is_awaited_directly() {
    let cancelled = || {
        let token = crate::CancellationToken::new();
        token.cancel();
        CallContext::new().with_cancel(token)
    };
    let wrapped = DriverSink::new(MessageSinkDriver::new(Broker::default()));
    wrapped.set_call_context(cancelled());
    let outcomes = wrapped.write_batch_partial(&[msg("a")]).await.unwrap();
    assert!(matches!(outcomes[0], Err(FaucetError::Cancelled { .. })));

    let own = DriverSink::new(MessageSinkDriver::new(Broker {
        own_calls: true,
        ..Default::default()
    }));
    own.set_call_context(cancelled());
    let outcomes = own.write_batch_partial(&[msg("a")]).await.unwrap();
    assert!(outcomes[0].is_ok(), "{outcomes:?}");
    own.flush().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_driver_making_its_own_calls_is_still_bounded_by_the_group_timeout() {
    let broker = Broker {
        own_calls: true,
        size: 1,
        timeout: Some(Duration::from_secs(5)),
        ..Default::default()
    };
    *broker.hang_group.lock().unwrap() = Some(0);
    broker.hang_flush.store(true, Ordering::SeqCst);
    let sink = DriverSink::new(MessageSinkDriver::new(broker));
    let outcomes = sink
        .write_batch_partial(&[msg("a"), msg("b")])
        .await
        .unwrap();
    assert!(
        outcomes
            .iter()
            .all(|o| matches!(o, Err(FaucetError::Timeout { .. }))),
        "{outcomes:?}"
    );
    let err = sink.flush().await.unwrap_err();
    assert!(
        matches!(&err, FaucetError::Timeout { operation, .. } if operation == "broker flush"),
        "{err:?}"
    );
}

#[tokio::test]
async fn a_transport_failure_fails_the_whole_page() {
    let broker = Broker::default();
    broker.transport_down.store(true, Ordering::SeqCst);
    let sink = DriverSink::new(MessageSinkDriver::new(broker));
    let err = sink.write_batch_partial(&[msg("a")]).await.unwrap_err();
    assert!(err.to_string().contains("transport down"), "{err}");
}

#[tokio::test(start_paused = true)]
async fn a_group_timeout_fails_the_unconfirmed_rows_and_every_row_after() {
    let broker = Broker {
        size: 1,
        timeout: Some(Duration::from_secs(5)),
        ..Default::default()
    };
    *broker.hang_group.lock().unwrap() = Some(1);
    let sink = DriverSink::new(MessageSinkDriver::new(broker));
    let outcomes = sink
        .write_batch_partial(&[msg("a"), msg("b"), msg("c")])
        .await
        .unwrap();
    assert!(outcomes[0].is_ok());
    for o in &outcomes[1..] {
        match o {
            Err(FaucetError::Timeout { operation, after }) => {
                assert_eq!(operation, "broker publish");
                assert_eq!(*after, Duration::from_secs(5));
            }
            other => panic!("{other:?}"),
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_flush_past_the_group_timeout_is_a_typed_timeout() {
    let broker = Broker {
        timeout: Some(Duration::from_secs(1)),
        ..Default::default()
    };
    broker.hang_flush.store(true, Ordering::SeqCst);
    let sink = DriverSink::new(MessageSinkDriver::new(broker));
    let err = sink.flush().await.unwrap_err();
    assert!(
        matches!(&err, FaucetError::Timeout { operation, after }
            if operation == "broker flush" && *after == Duration::from_secs(1)),
        "{err:?}"
    );
    assert!(err.is_retriable());
}

/// A configuration that cannot roll back still answers through its driver,
/// so the error can say why — but does not declare the capability.
#[tokio::test]
async fn a_disabled_rollback_driver_explains_itself() {
    struct NoRollback;
    #[async_trait]
    impl SinkDriver for NoRollback {
        fn connector_name(&self) -> &'static str {
            "norb"
        }
        async fn write(
            &self,
            _: &CallSlot,
            rows: &[Value],
            _: RowPolicy,
        ) -> Result<PageWrite, FaucetError> {
            Ok(PageWrite::accepted(rows.len()))
        }
        fn rollback(&self) -> Option<&dyn RollbackDriver> {
            Some(self)
        }
    }
    #[async_trait]
    impl RollbackDriver for NoRollback {
        fn enabled(&self) -> bool {
            false
        }
        async fn rollback_run(
            &self,
            _: &CallSlot,
            _: &str,
            _: &RollbackOptions,
        ) -> Result<RollbackOutcome, FaucetError> {
            Err(FaucetError::Sink("needs mapped columns".into()))
        }
        async fn forget_run(&self, _: &CallSlot, _: &str) -> Result<(), FaucetError> {
            Ok(())
        }
        async fn rewind_commit_token(
            &self,
            _: &CallSlot,
            _: &str,
            _: Option<&str>,
        ) -> Result<(), FaucetError> {
            Ok(())
        }
    }
    let sink = DriverSink::new(NoRollback);
    assert!(!sink.supports_rollback() && !sink.capabilities().rollback());
    let err = sink.rollback_run("r", &opts()).await.unwrap_err();
    assert!(err.to_string().contains("mapped columns"), "{err}");
    sink.forget_run("r").await.unwrap();
    sink.rewind_commit_token("s", None).await.unwrap();
}

#[tokio::test]
async fn a_cancelled_publish_fails_the_rows_it_did_not_confirm() {
    let broker = Broker {
        size: 1,
        ..Default::default()
    };
    *broker.hang_group.lock().unwrap() = Some(1);
    let sink = DriverSink::new(MessageSinkDriver::new(broker));
    let cancel = crate::CancellationToken::new();
    sink.calls()
        .install(CallContext::new().with_cancel(cancel.clone()));
    let page = [msg("a"), msg("b")];
    let write = sink.write_batch_partial(&page);
    let (outcomes, ()) = tokio::join!(write, async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();
    });
    let outcomes = outcomes.unwrap();
    assert!(outcomes[0].is_ok());
    assert!(
        matches!(outcomes[1], Err(FaucetError::Cancelled { .. })),
        "{:?}",
        outcomes[1]
    );
}

#[tokio::test]
async fn exactly_once_commits_the_page_and_its_token_in_one_transaction() {
    let broker = Broker {
        tx: Some(Tx::default()),
        ..Default::default()
    };
    let sink = DriverSink::new(MessageSinkDriver::new(broker));
    assert!(sink.supports_idempotent_writes());
    let r = sink
        .write_batch_idempotent_reported(&[msg("a"), msg("b")], "s", "t1")
        .await
        .unwrap();
    assert_eq!(r.accepted, 2);
    assert_eq!(sink.last_committed_token("s").await.unwrap().unwrap(), "t1");
    let tx = sink.driver().driver().tx.as_ref().unwrap();
    assert_eq!(
        tx.open.lock().unwrap().len(),
        2,
        "published inside the transaction"
    );

    // A row that fails aborts the transaction and commits nothing.
    tx.open.lock().unwrap().clear();
    let err = sink
        .write_batch_idempotent_reported(&[msg("a"), json!({})], "s", "t2")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("no subject"), "{err}");
    assert!(tx.open.lock().unwrap().is_empty());
    assert_eq!(sink.last_committed_token("s").await.unwrap().unwrap(), "t1");

    // A refused commit aborts too.
    tx.fail_commit.store(true, Ordering::SeqCst);
    assert!(
        sink.write_batch_idempotent_reported(&[msg("a")], "s", "t3")
            .await
            .is_err()
    );
    assert!(tx.open.lock().unwrap().is_empty());
}

#[tokio::test]
async fn exactly_once_aborts_on_a_transport_failure() {
    let broker = Broker {
        tx: Some(Tx::default()),
        ..Default::default()
    };
    broker.transport_down.store(true, Ordering::SeqCst);
    let sink = DriverSink::new(MessageSinkDriver::new(broker));
    assert!(
        sink.write_batch_idempotent_reported(&[msg("a")], "s", "t")
            .await
            .is_err()
    );
    let tx = sink.driver().driver().tx.as_ref().unwrap();
    assert!(!tx.in_tx.load(Ordering::SeqCst));
}

#[tokio::test]
async fn write_with_token_without_transactions_is_refused() {
    let adapter = MessageSinkDriver::new(Broker::default());
    let err = CommitTokenDriver::write_with_token(
        &adapter,
        &CallSlot::new(CallContext::new()),
        &[msg("a")],
        "s",
        "t",
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("transactional"), "{err}");
    assert!(
        CommitTokenDriver::last_token(&adapter, &CallSlot::new(CallContext::new()), "s")
            .await
            .unwrap()
            .is_none()
    );
    let mut adapter = adapter;
    let _ = adapter.driver_mut();
    assert_eq!(SinkDriver::dataset_uri(&adapter), "broker://unknown");
    assert_eq!(SinkDriver::config_schema(&adapter)["type"], "object");
    let report = SinkDriver::check(
        &adapter,
        &CallSlot::new(CallContext::new()),
        &CheckContext::default(),
    )
    .await
    .unwrap();
    assert_eq!(report.probes[0].name, "check");
}

/// A queue of messages; acknowledgements are recorded.
struct Queue {
    messages: Mutex<Vec<InboundMessage>>,
    acked: Mutex<Vec<AckToken>>,
    max: Option<usize>,
}

impl Queue {
    fn of(n: usize, max: Option<usize>) -> Self {
        Self {
            messages: Mutex::new(
                (0..n)
                    .map(|i| {
                        InboundMessage::new(
                            "q",
                            json!({ "i": i }).to_string(),
                            AckToken(i.to_string()),
                        )
                    })
                    .collect(),
            ),
            acked: Mutex::new(Vec::new()),
            max,
        }
    }
}

#[async_trait]
impl MessageConsumer for Queue {
    fn connector_name(&self) -> &'static str {
        "queue"
    }
    async fn receive(&self, _: &CallSlot, max: usize) -> Result<Vec<InboundMessage>, FaucetError> {
        let mut q = self.messages.lock().unwrap();
        let n = max.min(q.len());
        Ok(q.drain(..n).collect())
    }
    async fn ack(&self, _: &CallSlot, tokens: Vec<AckToken>) -> Result<(), FaucetError> {
        self.acked.lock().unwrap().extend(tokens);
        Ok(())
    }
    fn decode(&self, m: &InboundMessage) -> Result<Value, FaucetError> {
        serde_json::from_slice(&m.payload).map_err(|e| FaucetError::Source(e.to_string()))
    }
    fn max_messages(&self) -> Option<usize> {
        self.max
    }
}

#[derive(Default)]
struct Collect(Mutex<Vec<Value>>, AtomicBool);

#[async_trait]
impl Sink for Collect {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if self.1.load(Ordering::SeqCst) {
            return Err(FaucetError::Sink("refused".into()));
        }
        self.0.lock().unwrap().extend_from_slice(records);
        Ok(records.len())
    }
}

#[tokio::test]
async fn messages_are_acknowledged_only_after_the_pipeline_wrote_them() {
    let source = DriverSource::new(MessageSourceDriver::new(Queue::of(5, None)));
    assert!(source.consumes_destructively());
    let sink = Collect::default();
    let ctx = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 2);
    use futures::StreamExt;
    let first = pages.next().await.unwrap().unwrap();
    assert_eq!(first.records.len(), 2);
    assert_eq!(first.bookmark, Some(json!({"consumed": 2})));
    assert!(
        source.driver().consumer().acked.lock().unwrap().is_empty(),
        "nothing acked before the page is written"
    );
    sink.write_batch(&first.records).await.unwrap();
    let _second = pages.next().await.unwrap().unwrap();
    assert_eq!(source.driver().consumer().acked.lock().unwrap().len(), 2);
    drop(pages);

    let source = DriverSource::new(MessageSourceDriver::new(Queue::of(5, None)));
    let r = Pipeline::new(&source, &sink).run().await.unwrap();
    assert_eq!(r.records_written, 5);
    assert_eq!(source.driver().consumer().acked.lock().unwrap().len(), 5);
}

#[tokio::test]
async fn a_failed_write_leaves_its_messages_unacknowledged() {
    let source = DriverSource::new(MessageSourceDriver::new(Queue::of(3, None)));
    let sink = Collect::default();
    sink.1.store(true, Ordering::SeqCst);
    assert!(Pipeline::new(&source, &sink).run().await.is_err());
    assert!(source.driver().consumer().acked.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_consumer_stops_at_its_message_limit() {
    let source = DriverSource::new(MessageSourceDriver::new(Queue::of(10, Some(3))));
    let rows = source.fetch_all().await.unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(source.dataset_uri(), "queue://unknown");
    assert_eq!(source.config_schema()["type"], "object");
    use futures::StreamExt;
    let ctx = HashMap::new();
    let pages: Vec<_> = source.stream_pages(&ctx, 0).collect().await;
    assert_eq!(pages.len(), 1);
    assert_eq!(source.driver().consumer().acked.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn a_buffered_read_never_acknowledges_what_nobody_wrote() {
    let source = DriverSource::new(MessageSourceDriver::new(Queue::of(5, None)));
    let rows = source.fetch_all().await.unwrap();
    assert_eq!(rows.len(), 5);
    assert!(source.driver().consumer().acked.lock().unwrap().is_empty());
}

#[tokio::test]
async fn the_preflight_probe_consumes_nothing() {
    let source = DriverSource::new(MessageSourceDriver::new(Queue::of(2, None)));
    let report = source.check(&CheckContext::default()).await.unwrap();
    assert!(matches!(
        report.probes[0].status,
        crate::check::ProbeStatus::Skip { .. }
    ));
    assert_eq!(source.fetch_all().await.unwrap().len(), 2);
}

// ── HTTP ───────────────────────────────────────────────────────────────────

struct Endpoint {
    batching: HttpBatching,
    fail_after: Option<usize>,
    sent: Mutex<Vec<Value>>,
    sessions: AtomicUsize,
}

impl Endpoint {
    fn new(batching: HttpBatching, fail_after: Option<usize>) -> Self {
        Self {
            batching,
            fail_after,
            sent: Mutex::new(Vec::new()),
            sessions: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl HttpDriver for Endpoint {
    type Session = String;
    fn connector_name(&self) -> &'static str {
        "endpoint"
    }
    fn batching(&self) -> HttpBatching {
        self.batching
    }
    async fn session(&self, _: &CallSlot) -> Result<String, FaucetError> {
        self.sessions.fetch_add(1, Ordering::SeqCst);
        Ok("token".into())
    }
    async fn send(&self, _: &CallSlot, session: &String, body: &Value) -> Result<(), FaucetError> {
        assert_eq!(session, "token");
        let mut sent = self.sent.lock().unwrap();
        if self.fail_after.is_some_and(|n| sent.len() >= n) || body.get("bad").is_some() {
            return Err(FaucetError::Sink("rejected".into()));
        }
        sent.push(body.clone());
        Ok(())
    }
}

#[tokio::test]
async fn per_record_requests_report_each_row_or_stop_at_the_first_failure() {
    let sink = DriverSink::new(HttpSinkDriver::new(Endpoint::new(
        HttpBatching::PerRecord { concurrency: 2 },
        None,
    )));
    let page = [json!({"a": 1}), json!({"bad": 1}), json!({"a": 3})];
    let outcomes = sink.write_batch_partial(&page).await.unwrap();
    assert!(outcomes[0].is_ok() && outcomes[1].is_err() && outcomes[2].is_ok());
    assert!(sink.write_batch(&page).await.is_err());
    assert_eq!(sink.write_batch(&[json!({"a": 9})]).await.unwrap(), 1);
    assert_eq!(sink.write_batch(&[]).await.unwrap(), 0);
    assert_eq!(
        sink.driver().driver().sessions.load(Ordering::SeqCst),
        3,
        "credentials once per non-empty page"
    );
}

#[tokio::test]
async fn array_requests_never_report_a_delivered_row_failed() {
    let page: Vec<Value> = (0..5).map(|i| json!({ "i": i })).collect();
    let sink = DriverSink::new(HttpSinkDriver::new(Endpoint::new(
        HttpBatching::Array { batch_size: 2 },
        Some(1),
    )));
    let outcomes = sink.write_batch_partial(&page).await.unwrap();
    assert!(outcomes[0].is_ok() && outcomes[1].is_ok());
    let failed = outcomes[2].as_ref().unwrap_err().to_string();
    assert!(failed.contains("chunk POST failed"), "{failed}");
    let unsent = outcomes[4].as_ref().unwrap_err().to_string();
    assert!(
        unsent.contains("not sent after earlier failure"),
        "{unsent}"
    );

    let none_delivered = DriverSink::new(HttpSinkDriver::new(Endpoint::new(
        HttpBatching::Array { batch_size: 2 },
        Some(0),
    )));
    assert!(none_delivered.write_batch_partial(&page).await.is_err());
    assert!(none_delivered.write_batch(&page).await.is_err());

    let all = DriverSink::new(HttpSinkDriver::new(Endpoint::new(
        HttpBatching::Array { batch_size: 0 },
        None,
    )));
    assert_eq!(all.write_batch(&page).await.unwrap(), 5);
    assert_eq!(all.driver().driver().sent.lock().unwrap().len(), 1);
    let mut adapter =
        HttpSinkDriver::new(Endpoint::new(HttpBatching::Array { batch_size: 1 }, None));
    let _ = adapter.driver_mut();
    assert_eq!(SinkDriver::dataset_uri(&adapter), "endpoint://unknown");
    assert_eq!(SinkDriver::config_schema(&adapter)["type"], "object");
    let report = SinkDriver::check(
        &adapter,
        &CallSlot::new(CallContext::new()),
        &CheckContext::default(),
    )
    .await
    .unwrap();
    assert_eq!(report.probes[0].name, "check");
}

// ── trait defaults ─────────────────────────────────────────────────────────

/// A producer with only the required primitives; `bad` makes it return the
/// wrong number of outcomes.
struct Minimal {
    bad: bool,
}

#[async_trait]
impl MessageDriver for Minimal {
    fn connector_name(&self) -> &'static str {
        "minimal"
    }
    fn encode(&self, record: &Value) -> Result<OutboundMessage, FaucetError> {
        if record.is_null() {
            return Err(FaucetError::Sink("null record".into()));
        }
        Ok(OutboundMessage::new("d", record.to_string()))
    }
    async fn publish(
        &self,
        _: &CallSlot,
        group: Vec<OutboundMessage>,
    ) -> Result<Vec<crate::RowOutcome>, FaucetError> {
        let n = if self.bad {
            group.len() + 1
        } else {
            group.len()
        };
        Ok((0..n).map(|_| Ok(())).collect())
    }
}

#[tokio::test]
async fn a_minimal_message_driver_takes_the_defaults() {
    let sink = DriverSink::new(MessageSinkDriver::new(Minimal { bad: false }));
    assert_eq!(sink.batch_atomicity(), crate::BatchAtomicity::BestEffort);
    let outcomes = sink
        .write_batch_partial(&[json!(1), json!(2), Value::Null])
        .await
        .unwrap();
    assert!(outcomes[0].is_ok() && outcomes[1].is_ok() && outcomes[2].is_err());
    let outcomes = sink.write_batch_partial(&[Value::Null]).await.unwrap();
    assert!(
        outcomes[0].is_err(),
        "a group no row of which encodes is skipped"
    );
    sink.flush().await.unwrap();
    let wrong = DriverSink::new(MessageSinkDriver::new(Minimal { bad: true }));
    let err = wrong.write_batch(&[json!(1)]).await.unwrap_err();
    assert!(
        err.to_string().contains("2 outcomes for a group of 1"),
        "{err}"
    );
}

/// A consumer that is empty on its first receive, then has one message,
/// then is empty again; with the default limits it reads that one.
struct Trickle(Mutex<Vec<Vec<InboundMessage>>>);

#[async_trait]
impl MessageConsumer for Trickle {
    fn connector_name(&self) -> &'static str {
        "trickle"
    }
    fn idle_receives_to_stop(&self) -> u32 {
        2
    }
    async fn receive(&self, _: &CallSlot, _: usize) -> Result<Vec<InboundMessage>, FaucetError> {
        Ok(self.0.lock().unwrap().pop().unwrap_or_default())
    }
    async fn ack(&self, _: &CallSlot, _: Vec<AckToken>) -> Result<(), FaucetError> {
        Ok(())
    }
    fn decode(&self, m: &InboundMessage) -> Result<Value, FaucetError> {
        Ok(json!(m.payload.len()))
    }
}

#[tokio::test]
async fn a_consumer_with_default_limits_reads_until_idle() {
    let consumer = Trickle(Mutex::new(vec![
        vec![],
        vec![InboundMessage::new(
            "q",
            b"abc".to_vec(),
            AckToken("1".into()),
        )],
        vec![],
    ]));
    assert!(consumer.max_messages().is_none());
    let source = DriverSource::new(MessageSourceDriver::new(consumer));
    let ctx = HashMap::new();
    let pages: Vec<_> = futures::StreamExt::collect::<Vec<_>>(source.stream_pages(&ctx, 0)).await;
    assert_eq!(pages.len(), 1);
    assert_eq!(pages[0].as_ref().unwrap().records, vec![json!(3)]);
}

#[tokio::test]
async fn the_remaining_defaults_answer() {
    struct Schemaless;
    #[async_trait]
    impl SchemaDriver for Schemaless {
        async fn current_schema(&self, _: &CallSlot) -> Result<Option<Value>, FaucetError> {
            Ok(None)
        }
    }
    let calls = CallSlot::new(CallContext::new());
    assert!(!Schemaless.evolvable());
    assert!(
        Schemaless
            .evolve(&calls, &SchemaEvolution::default())
            .await
            .is_err()
    );
    assert_eq!(
        SinkDriver::batch_atomicity(&Bare),
        crate::BatchAtomicity::BestEffort
    );
    let caps = DriverSource::new(FailingRead).capabilities();
    assert!(!caps.reports_lag());
    let http = Endpoint::new(HttpBatching::PerRecord { concurrency: 1 }, None);
    assert_eq!(
        HttpDriver::batch_atomicity(&http),
        crate::BatchAtomicity::BestEffort
    );
}

#[test]
fn sink_guarantee_is_derived_and_a_driver_may_lower_it() {
    use crate::idempotency::SinkGuarantee;
    struct Keyed(bool);
    #[async_trait]
    impl SinkDriver for Keyed {
        fn connector_name(&self) -> &'static str {
            "keyed"
        }
        async fn write(
            &self,
            _: &CallSlot,
            r: &[Value],
            _: RowPolicy,
        ) -> Result<PageWrite, FaucetError> {
            Ok(PageWrite::accepted(r.len()))
        }
        fn write_modes(&self) -> &'static [WriteMode] {
            &[WriteMode::Append, WriteMode::Upsert]
        }
        fn sink_guarantee(&self, derived: SinkGuarantee) -> SinkGuarantee {
            if self.0 {
                SinkGuarantee::AtLeastOnce
            } else {
                derived
            }
        }
    }
    assert_eq!(
        DriverSink::new(Keyed(false)).sink_guarantee(),
        SinkGuarantee::KeyedUpsert
    );
    assert_eq!(
        DriverSink::new(Keyed(true)).sink_guarantee(),
        SinkGuarantee::AtLeastOnce
    );
    struct Plain;
    #[async_trait]
    impl SinkDriver for Plain {
        fn connector_name(&self) -> &'static str {
            "plain"
        }
        async fn write(
            &self,
            _: &CallSlot,
            r: &[Value],
            _: RowPolicy,
        ) -> Result<PageWrite, FaucetError> {
            Ok(PageWrite::accepted(r.len()))
        }
    }
    assert_eq!(
        DriverSink::new(Plain).sink_guarantee(),
        SinkGuarantee::AtLeastOnce
    );
}

#[tokio::test]
async fn a_staging_driver_that_cannot_look_for_staging_reports_unknown() {
    struct Versioned;
    #[async_trait]
    impl SinkDriver for Versioned {
        fn connector_name(&self) -> &'static str {
            "versioned"
        }
        async fn write(
            &self,
            _: &CallSlot,
            r: &[Value],
            _: RowPolicy,
        ) -> Result<PageWrite, FaucetError> {
            Ok(PageWrite::accepted(r.len()))
        }
        fn write_modes(&self) -> &'static [WriteMode] {
            &[WriteMode::Append, WriteMode::Overwrite]
        }
        fn staging(&self) -> Option<&dyn StagingDriver> {
            Some(self)
        }
    }
    #[async_trait]
    impl StagingDriver for Versioned {
        fn active(&self) -> bool {
            true
        }
        async fn stage(&self, _: &CallSlot) -> Result<(), FaucetError> {
            Ok(())
        }
        async fn publish(&self, _: &CallSlot) -> Result<(), FaucetError> {
            Ok(())
        }
        async fn discard(&self, _: &CallSlot) -> Result<(), FaucetError> {
            Ok(())
        }
        async fn staged(&self, _: &CallSlot) -> Result<bool, FaucetError> {
            unreachable!("never probed")
        }
        fn reports_staged(&self) -> bool {
            false
        }
    }
    let sink = DriverSink::new(Versioned);
    assert!(sink.is_overwrite());
    assert_eq!(sink.overwrite_staging_exists().await.unwrap(), None);
    sink.begin_overwrite().await.unwrap();
    sink.commit_overwrite().await.unwrap();
    sink.abort_overwrite().await.unwrap();
}

#[tokio::test]
async fn source_driver_defaults_and_overrides_reach_the_source() {
    struct Plain;
    #[async_trait]
    impl SourceDriver for Plain {
        fn connector_name(&self) -> &'static str {
            "plain"
        }
        fn read<'a>(
            &'a self,
            _: &'a CallSlot,
            _: &'a HashMap<String, Value>,
            _: usize,
        ) -> PageStream<'a> {
            Box::pin(futures::stream::iter(vec![Ok(StreamPage {
                records: vec![json!(1)],
                bookmark: Some(json!(1)),
            })]))
        }
    }
    let plain = DriverSource::new(Plain);
    plain.set_run_clock(chrono::Utc::now());
    plain.set_call_context(CallContext::new());
    assert!(plain.native_output_formats().is_empty());
    assert!(!plain.supports_discover());
    assert!(
        plain
            .discover()
            .await
            .unwrap_err()
            .to_string()
            .contains("discovery")
    );
    let empty = HashMap::new();
    let mut native = plain.stream_native(&empty, crate::native::NativeFormat::NdJson, 1);
    use futures::StreamExt as _;
    assert!(native.next().await.unwrap().is_err());
    #[cfg(feature = "arrow")]
    {
        assert!(!plain.supports_columnar());
        let mut cols = plain.stream_batches(&empty, 1);
        assert!(cols.next().await.unwrap().is_err());
    }
    assert_eq!(
        plain.fetch_all_incremental().await.unwrap().1,
        Some(json!(1))
    );

    struct Buffered(Arc<Mutex<Vec<String>>>);
    #[async_trait]
    impl SourceDriver for Buffered {
        fn connector_name(&self) -> &'static str {
            "buffered"
        }
        fn read<'a>(
            &'a self,
            _: &'a CallSlot,
            _: &'a HashMap<String, Value>,
            _: usize,
        ) -> PageStream<'a> {
            Box::pin(futures::stream::empty())
        }
        async fn fetch(
            &self,
            _: &CallSlot,
            _: &HashMap<String, Value>,
        ) -> Option<Result<(Vec<Value>, Option<Value>), FaucetError>> {
            Some(Ok((vec![json!("a"), json!("b")], None)))
        }
        fn set_run_clock(&self, _: chrono::DateTime<chrono::Utc>) {
            self.0.lock().unwrap().push("clock".into());
        }
        fn set_call_context(&self, _: CallContext) {
            self.0.lock().unwrap().push("ctx".into());
        }
        fn set_roundtrip_recorder(&self, _: Arc<crate::observability::RoundtripRecorder>) {
            self.0.lock().unwrap().push("recorder".into());
        }
        fn native_output_formats(&self) -> &'static [crate::native::NativeFormat] {
            &[crate::native::NativeFormat::NdJson]
        }
        fn supports_discover(&self) -> bool {
            true
        }
        async fn discover(
            &self,
            _: &CallSlot,
        ) -> Result<Vec<crate::discover::DatasetDescriptor>, FaucetError> {
            Ok(Vec::new())
        }
    }
    let log = Arc::new(Mutex::new(Vec::new()));
    let b = DriverSource::new(Buffered(Arc::clone(&log)));
    assert_eq!(b.fetch_all().await.unwrap().len(), 2);
    assert_eq!(b.fetch_all_incremental().await.unwrap().1, None);
    b.set_run_clock(chrono::Utc::now());
    b.set_call_context(CallContext::new());
    b.set_roundtrip_recorder(Arc::new(crate::observability::RoundtripRecorder::new(
        crate::observability::RoundtripSide::Source,
        "p",
        "r",
        "buffered",
    )));
    assert_eq!(*log.lock().unwrap(), vec!["clock", "ctx", "recorder"]);
    assert_eq!(b.native_output_formats().len(), 1);
    assert!(b.supports_discover());
    assert!(b.discover().await.unwrap().is_empty());
}

#[test]
fn pages_of_chunks_and_puts_the_bookmark_last() {
    let rows = |n: usize| (0..n).map(|i| json!(i)).collect::<Vec<_>>();
    let pages: Vec<_> = pages_of(rows(5), Some(json!("b")), 2).collect();
    assert_eq!(
        pages.iter().map(|p| p.records.len()).collect::<Vec<_>>(),
        vec![2, 2, 1]
    );
    assert!(pages[..2].iter().all(|p| p.bookmark.is_none()));
    assert_eq!(pages[2].bookmark, Some(json!("b")));
    assert_eq!(pages_of(rows(5), None, 0).count(), 1);
    let empty: Vec<_> = pages_of(Vec::new(), Some(json!(1)), 10).collect();
    assert_eq!(empty.len(), 1);
    assert!(empty[0].records.is_empty());
    assert_eq!(pages_of(Vec::new(), None, 10).count(), 0);
}
