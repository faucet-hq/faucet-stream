//! Forwarding harnesses for [`Source`] and [`Sink`] wrappers (RFC 0013, #877).
//!
//! A wrapper that holds an inner connector must forward every defaulted hook,
//! or the hook silently reverts to the trait default for everything behind
//! it. [`HookSource`] / [`HookSink`] answer every hook differently from the
//! default and record which hooks were reached; wrap one in your decorator and
//! call [`assert_source_forwards_every_hook`] /
//! [`assert_sink_forwards_every_hook`]:
//!
//! ```
//! # async fn ex() {
//! use faucet_conformance::forwarding::{HookSink, assert_sink_forwards_every_hook};
//! use faucet_core::{Sink, async_trait, forward_sink};
//!
//! struct Wrapper {
//!     inner: HookSink,
//! }
//!
//! #[async_trait]
//! impl Sink for Wrapper {
//!     forward_sink!(inner);
//! }
//!
//! let probe = HookSink::default();
//! let wrapped = Wrapper { inner: probe.clone() };
//! assert_sink_forwards_every_hook(&wrapped, &probe, &[]).await;
//! # }
//! ```
//!
//! `skip` names the hooks the wrapper deliberately does not forward (it takes
//! the trait default or answers itself). Every name in it must be a hook.
//!
//! [`SOURCE_HOOKS`] / [`SINK_HOOKS`] are the canonical hook lists. A test in
//! this crate checks them against the trait definitions, so a hook added to
//! either trait without being added here (and so to the harness) fails CI.
//!
//! This file is also compiled into `faucet-core`'s own unit tests (through
//! `#[path]`), so it names everything through `faucet_core::` and nothing else.

use faucet_core::check::{CheckContext, CheckReport, Probe};
use faucet_core::cleanup::SeenKeys;
use faucet_core::diff::{KeyRange, ServerDigest};
use faucet_core::discover::DatasetDescriptor;
use faucet_core::dlq::BatchAtomicity;
use faucet_core::drift::SchemaEvolution;
use faucet_core::idempotency::{ReplayGuarantee, SinkGuarantee};
use faucet_core::lag::SourceLag;
use faucet_core::local_outputs::LocalOutput;
use faucet_core::native::{
    CsvDialect, NativeBatch, NativeFormat, NativeLoadCapability, NativeLoadContext, NativePayload,
};
use faucet_core::observability::{RoundtripRecorder, RoundtripSide};
use faucet_core::pipeline::StreamPage;
use faucet_core::rollback::{RollbackMode, RollbackOptions, RollbackOutcome};
use faucet_core::serde_json::{Value, json};
use faucet_core::shard::ShardSpec;
use faucet_core::traits::{RowOutcome, Sink, Source};
use faucet_core::write_mode::WriteMode;
use faucet_core::{CancellationToken, FaucetError, Stream, WriteReport};
use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::sync::{Arc, Mutex};

/// Every [`Source`] hook a wrapper must forward, by trait-method name.
pub const SOURCE_HOOKS: &[&str] = &[
    "fetch_with_context",
    "fetch_all",
    "fetch_with_context_incremental",
    "fetch_all_incremental",
    "stream_pages",
    "supports_columnar",
    "stream_batches",
    "native_output_formats",
    "stream_native",
    "config_schema",
    "state_key",
    "apply_start_bookmark",
    "capture_resume_position",
    "lag",
    "state_schema",
    "migrate_state",
    "supports_exactly_once",
    "consumes_destructively",
    "replay_guarantee",
    "capabilities",
    "is_shardable",
    "enumerate_shards",
    "apply_shard",
    "range_digest",
    "supports_discover",
    "discover",
    "record_table",
    "position_le",
    "record_covered",
    "position_min",
    "connector_name",
    "set_roundtrip_recorder",
    "set_call_context",
    "set_run_clock",
    "set_cancel",
    "dataset_uri",
    "check",
];

/// Every [`Sink`] hook a wrapper must forward, by trait-method name.
pub const SINK_HOOKS: &[&str] = &[
    "write_batch",
    "write_batch_reported",
    "flush",
    "write_batch_partial",
    "batch_atomicity",
    "admit_page",
    "supports_columnar",
    "write_batch_columnar",
    "native_load_capabilities",
    "load_native",
    "supports_idempotent_writes",
    "sink_guarantee",
    "capabilities",
    "write_batch_is_replay_safe",
    "dedups_by_key",
    "supported_write_modes",
    "current_schema",
    "supports_schema_evolution",
    "evolve_schema",
    "supports_cleanup",
    "supports_staged_load",
    "cleanup_scope",
    "write_batch_idempotent",
    "write_batch_idempotent_reported",
    "last_committed_token",
    "is_overwrite",
    "begin_overwrite",
    "commit_overwrite",
    "supports_rollback",
    "rollback_run",
    "forget_run",
    "rewind_commit_token",
    "readback_source",
    "abort_overwrite",
    "complete_run",
    "overwrite_staging_exists",
    "config_schema",
    "connector_name",
    "set_roundtrip_recorder",
    "set_call_context",
    "dataset_uri",
    "local_outputs",
    "check",
];

/// Hooks that exist only when `faucet-core` is built with `arrow`.
pub const ARROW_HOOKS: &[&str] = &[
    "supports_columnar",
    "stream_batches",
    "write_batch_columnar",
];

fn expected(hooks: &'static [&'static str]) -> impl Iterator<Item = &'static str> {
    hooks
        .iter()
        .copied()
        .filter(|h| faucet_core::forward::__forward::ARROW || !ARROW_HOOKS.contains(h))
}

fn check_reached(kind: &str, hooks: &'static [&'static str], reached: &[&str], skip: &[&str]) {
    let unknown: Vec<&str> = skip
        .iter()
        .copied()
        .filter(|s| !hooks.contains(s))
        .collect();
    assert!(
        unknown.is_empty(),
        "skip names that are not {kind} hooks: {unknown:?}"
    );
    let missing: Vec<&str> = expected(hooks)
        .filter(|h| !skip.contains(h) && !reached.contains(h))
        .collect();
    assert!(
        missing.is_empty(),
        "{kind} hooks not forwarded: {missing:?}"
    );
}

type Recorded = Arc<Mutex<Vec<&'static str>>>;

fn hit(calls: &Recorded, name: &'static str) {
    calls.lock().unwrap().push(name);
}

/// A sink whose every hook answers differently from the trait default and
/// records that it was reached. Clones share the record.
#[derive(Debug, Default, Clone)]
pub struct HookSink {
    calls: Recorded,
}

impl HookSink {
    fn hit(&self, name: &'static str) {
        hit(&self.calls, name);
    }

    /// The hooks reached so far, in call order.
    pub fn reached(&self) -> Vec<&'static str> {
        self.calls.lock().unwrap().clone()
    }
}

#[faucet_core::async_trait]
impl Sink for HookSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        self.hit("write_batch");
        Ok(records.len())
    }
    async fn write_batch_reported(&self, records: &[Value]) -> Result<WriteReport, FaucetError> {
        self.hit("write_batch_reported");
        Ok(WriteReport::accepted(records.len()))
    }
    async fn flush(&self) -> Result<(), FaucetError> {
        self.hit("flush");
        Ok(())
    }
    async fn write_batch_partial(&self, records: &[Value]) -> Result<Vec<RowOutcome>, FaucetError> {
        self.hit("write_batch_partial");
        Ok(records.iter().map(|_| Ok(())).collect())
    }
    fn batch_atomicity(&self) -> BatchAtomicity {
        self.hit("batch_atomicity");
        BatchAtomicity::Atomic
    }
    async fn admit_page(&self, _records: &[Value]) -> Result<(), FaucetError> {
        self.hit("admit_page");
        Ok(())
    }
    faucet_core::__faucet_if_arrow! {
        fn supports_columnar(&self) -> bool {
            self.hit("supports_columnar");
            true
        }
        fn write_batch_columnar<'life0, 'life1, 'async_trait>(
            &'life0 self,
            batch: &'life1 faucet_core::forward::__forward::arrow::array::RecordBatch,
        ) -> Pin<Box<dyn std::future::Future<Output = Result<usize, FaucetError>> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            self.hit("write_batch_columnar");
            let rows = batch.num_rows();
            Box::pin(async move { Ok(rows) })
        }
    }
    fn native_load_capabilities(&self) -> Vec<NativeLoadCapability> {
        self.hit("native_load_capabilities");
        vec![NativeLoadCapability {
            format: NativeFormat::NdJson,
            mechanism: "hook",
            write_modes: &[WriteMode::Append],
        }]
    }
    async fn load_native(
        &self,
        _batch: NativeBatch,
        _scope: &str,
        _ctx: NativeLoadContext,
    ) -> Result<usize, FaucetError> {
        self.hit("load_native");
        Ok(7)
    }
    fn supports_idempotent_writes(&self) -> bool {
        self.hit("supports_idempotent_writes");
        true
    }
    fn sink_guarantee(&self) -> SinkGuarantee {
        self.hit("sink_guarantee");
        SinkGuarantee::KeyedUpsert
    }
    fn capabilities(&self) -> faucet_core::Capabilities {
        self.hit("capabilities");
        faucet_core::Capabilities::builder()
            .range_digest(true)
            .build()
    }
    fn write_batch_is_replay_safe(&self) -> bool {
        self.hit("write_batch_is_replay_safe");
        true
    }
    fn dedups_by_key(&self) -> bool {
        self.hit("dedups_by_key");
        false
    }
    fn supported_write_modes(&self) -> &'static [WriteMode] {
        self.hit("supported_write_modes");
        &[WriteMode::Append, WriteMode::Upsert]
    }
    async fn current_schema(&self) -> Result<Option<Value>, FaucetError> {
        self.hit("current_schema");
        Ok(Some(json!({"type": "object"})))
    }
    fn supports_schema_evolution(&self) -> bool {
        self.hit("supports_schema_evolution");
        true
    }
    async fn evolve_schema(&self, _evolution: &SchemaEvolution) -> Result<(), FaucetError> {
        self.hit("evolve_schema");
        Ok(())
    }
    fn supports_cleanup(&self) -> bool {
        self.hit("supports_cleanup");
        true
    }
    fn supports_staged_load(&self) -> bool {
        self.hit("supports_staged_load");
        true
    }
    async fn cleanup_scope(
        &self,
        _scope: &BTreeMap<String, Value>,
        _seen: &SeenKeys,
    ) -> Result<u64, FaucetError> {
        self.hit("cleanup_scope");
        Ok(3)
    }
    async fn write_batch_idempotent(
        &self,
        records: &[Value],
        _scope: &str,
        _token: &str,
    ) -> Result<usize, FaucetError> {
        self.hit("write_batch_idempotent");
        Ok(records.len())
    }
    async fn write_batch_idempotent_reported(
        &self,
        records: &[Value],
        _scope: &str,
        _token: &str,
    ) -> Result<WriteReport, FaucetError> {
        self.hit("write_batch_idempotent_reported");
        Ok(WriteReport::accepted(records.len()))
    }
    async fn last_committed_token(&self, _scope: &str) -> Result<Option<String>, FaucetError> {
        self.hit("last_committed_token");
        Ok(Some("tok".into()))
    }
    fn is_overwrite(&self) -> bool {
        self.hit("is_overwrite");
        true
    }
    async fn begin_overwrite(&self) -> Result<(), FaucetError> {
        self.hit("begin_overwrite");
        Ok(())
    }
    async fn commit_overwrite(&self) -> Result<(), FaucetError> {
        self.hit("commit_overwrite");
        Ok(())
    }
    fn supports_rollback(&self) -> bool {
        self.hit("supports_rollback");
        true
    }
    async fn rollback_run(
        &self,
        _run_id: &str,
        _opts: &RollbackOptions,
    ) -> Result<RollbackOutcome, FaucetError> {
        self.hit("rollback_run");
        Ok(RollbackOutcome::blocked(2))
    }
    async fn forget_run(&self, _run_id: &str) -> Result<(), FaucetError> {
        self.hit("forget_run");
        Ok(())
    }
    async fn rewind_commit_token(
        &self,
        _scope: &str,
        _token: Option<&str>,
    ) -> Result<(), FaucetError> {
        self.hit("rewind_commit_token");
        Ok(())
    }
    fn readback_source(&self) -> Option<(String, Value)> {
        self.hit("readback_source");
        Some(("hook".into(), json!({})))
    }
    async fn abort_overwrite(&self) -> Result<(), FaucetError> {
        self.hit("abort_overwrite");
        Ok(())
    }
    async fn complete_run(&self) -> Result<(), FaucetError> {
        self.hit("complete_run");
        Ok(())
    }
    async fn overwrite_staging_exists(&self) -> Result<Option<bool>, FaucetError> {
        self.hit("overwrite_staging_exists");
        Ok(Some(true))
    }
    fn config_schema(&self) -> Value {
        self.hit("config_schema");
        json!({"hook": true})
    }
    fn connector_name(&self) -> &'static str {
        self.hit("connector_name");
        "hook"
    }
    fn set_roundtrip_recorder(&self, _recorder: Arc<RoundtripRecorder>) {
        self.hit("set_roundtrip_recorder");
    }
    fn set_call_context(&self, _ctx: faucet_core::io::CallContext) {
        self.hit("set_call_context");
    }
    fn dataset_uri(&self) -> String {
        self.hit("dataset_uri");
        "hook://dataset".into()
    }
    async fn local_outputs(&self) -> Vec<LocalOutput> {
        self.hit("local_outputs");
        vec![LocalOutput::created("/hook")]
    }
    async fn check(&self, _ctx: &CheckContext) -> Result<CheckReport, FaucetError> {
        self.hit("check");
        Ok(CheckReport::single(Probe::skip("hook", "hook")))
    }
}

/// Call every [`Sink`] hook on `sink`, then panic naming each hook in
/// [`SINK_HOOKS`] (outside `skip`) that did not reach `probe`, the inner
/// [`HookSink`] the wrapper holds.
pub async fn assert_sink_forwards_every_hook(sink: &dyn Sink, probe: &HookSink, skip: &[&str]) {
    let page = [json!({"id": 1})];
    let _ = sink.write_batch(&page).await;
    let _ = sink.write_batch_reported(&page).await;
    let _ = sink.flush().await;
    let _ = sink.write_batch_partial(&page).await;
    let _ = sink.batch_atomicity();
    let _ = sink.admit_page(&page).await;
    faucet_core::__faucet_if_arrow! {
        let _ = sink.supports_columnar();
        let _ = sink.write_batch_columnar(&one_row_batch()).await;
    }
    let _ = sink.native_load_capabilities();
    let _ = sink
        .load_native(
            NativeBatch {
                format: NativeFormat::NdJson,
                payload: NativePayload::Bytes(b"{}\n".to_vec()),
                csv: CsvDialect::default(),
                records: Some(1),
                bookmark: None,
            },
            "scope",
            NativeLoadContext {
                write_mode: WriteMode::Append,
                first_batch: true,
            },
        )
        .await;
    let _ = sink.supports_idempotent_writes();
    let _ = sink.sink_guarantee();
    let _ = sink.capabilities();
    let _ = sink.write_batch_is_replay_safe();
    let _ = sink.dedups_by_key();
    let _ = sink.supported_write_modes();
    let _ = sink.current_schema().await;
    let _ = sink.supports_schema_evolution();
    let _ = sink.evolve_schema(&SchemaEvolution::default()).await;
    let _ = sink.supports_cleanup();
    let _ = sink.supports_staged_load();
    let _ = sink.cleanup_scope(&BTreeMap::new(), &SeenKeys::new()).await;
    let _ = sink.write_batch_idempotent(&page, "scope", "tok").await;
    let _ = sink
        .write_batch_idempotent_reported(&page, "scope", "tok")
        .await;
    let _ = sink.last_committed_token("scope").await;
    let _ = sink.is_overwrite();
    let _ = sink.begin_overwrite().await;
    let _ = sink.commit_overwrite().await;
    let _ = sink.supports_rollback();
    let _ = sink
        .rollback_run(
            "run",
            &RollbackOptions {
                run_id_column: "_faucet_run_id".into(),
                mode: RollbackMode::Append,
                force: false,
                dry_run: true,
                later_runs: false,
            },
        )
        .await;
    let _ = sink.forget_run("run").await;
    let _ = sink.rewind_commit_token("scope", None).await;
    let _ = sink.readback_source();
    let _ = sink.abort_overwrite().await;
    let _ = sink.complete_run().await;
    let _ = sink.overwrite_staging_exists().await;
    let _ = sink.config_schema();
    let _ = sink.connector_name();
    sink.set_roundtrip_recorder(recorder(RoundtripSide::Sink));
    sink.set_call_context(faucet_core::io::CallContext::new());
    let _ = sink.dataset_uri();
    let _ = sink.local_outputs().await;
    let _ = sink.check(&CheckContext::default()).await;

    check_reached("Sink", SINK_HOOKS, &probe.reached(), skip);
}

faucet_core::__faucet_if_arrow! {
    fn one_row_batch() -> faucet_core::forward::__forward::arrow::array::RecordBatch {
        use faucet_core::forward::__forward::arrow;
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
        ]));
        arrow::array::RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::Int64Array::from(vec![1]))],
        )
        .expect("a one-column batch")
    }
}

fn recorder(side: RoundtripSide) -> Arc<RoundtripRecorder> {
    Arc::new(RoundtripRecorder::new(side, "p", "r", "hook"))
}

type BoxStream<'a, T> = Pin<Box<dyn Stream<Item = Result<T, FaucetError>> + Send + 'a>>;

fn empty<'a, T: Send + 'a>() -> BoxStream<'a, T> {
    Box::pin(futures::stream::empty())
}

/// A source whose every hook answers differently from the trait default and
/// records that it was reached. Clones share the record.
#[derive(Debug, Default, Clone)]
pub struct HookSource {
    calls: Recorded,
}

impl HookSource {
    fn hit(&self, name: &'static str) {
        hit(&self.calls, name);
    }

    /// The hooks reached so far, in call order.
    pub fn reached(&self) -> Vec<&'static str> {
        self.calls.lock().unwrap().clone()
    }
}

#[faucet_core::async_trait]
impl Source for HookSource {
    async fn fetch_with_context(
        &self,
        _context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        self.hit("fetch_with_context");
        Ok(vec![json!({"id": 1})])
    }
    async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
        self.hit("fetch_all");
        Ok(vec![json!({"id": 1})])
    }
    async fn fetch_with_context_incremental(
        &self,
        _context: &HashMap<String, Value>,
    ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        self.hit("fetch_with_context_incremental");
        Ok((vec![json!({"id": 1})], Some(json!(1))))
    }
    async fn fetch_all_incremental(&self) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        self.hit("fetch_all_incremental");
        Ok((vec![json!({"id": 1})], Some(json!(1))))
    }
    fn stream_pages<'a>(
        &'a self,
        _context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> BoxStream<'a, StreamPage> {
        self.hit("stream_pages");
        empty()
    }
    faucet_core::__faucet_if_arrow! {
        fn supports_columnar(&self) -> bool {
            self.hit("supports_columnar");
            true
        }
        fn stream_batches<'a>(
            &'a self,
            _context: &'a HashMap<String, Value>,
            _batch_size: usize,
        ) -> BoxStream<'a, faucet_core::columnar::ColumnarPage> {
            self.hit("stream_batches");
            empty()
        }
    }
    fn native_output_formats(&self) -> &'static [NativeFormat] {
        self.hit("native_output_formats");
        &[NativeFormat::NdJson]
    }
    fn stream_native<'a>(
        &'a self,
        _context: &'a HashMap<String, Value>,
        _format: NativeFormat,
        _batch_size: usize,
    ) -> BoxStream<'a, NativeBatch> {
        self.hit("stream_native");
        empty()
    }
    fn config_schema(&self) -> Value {
        self.hit("config_schema");
        json!({"hook": true})
    }
    fn state_key(&self) -> Option<String> {
        self.hit("state_key");
        Some("hook".into())
    }
    async fn apply_start_bookmark(&self, _bookmark: Value) -> Result<(), FaucetError> {
        self.hit("apply_start_bookmark");
        Ok(())
    }
    async fn capture_resume_position(&self) -> Result<Option<Value>, FaucetError> {
        self.hit("capture_resume_position");
        Ok(Some(json!(7)))
    }
    async fn lag(&self) -> Result<Option<SourceLag>, FaucetError> {
        self.hit("lag");
        Ok(Some(SourceLag::events(3)))
    }
    fn state_schema(&self) -> u32 {
        self.hit("state_schema");
        2
    }
    fn migrate_state(&self, _from: u32, data: Value) -> Result<Value, FaucetError> {
        self.hit("migrate_state");
        Ok(data)
    }
    fn supports_exactly_once(&self) -> bool {
        self.hit("supports_exactly_once");
        true
    }
    fn consumes_destructively(&self) -> bool {
        self.hit("consumes_destructively");
        true
    }
    fn replay_guarantee(&self) -> ReplayGuarantee {
        self.hit("replay_guarantee");
        ReplayGuarantee::Deterministic
    }
    fn capabilities(&self) -> faucet_core::Capabilities {
        self.hit("capabilities");
        faucet_core::Capabilities::builder()
            .range_digest(true)
            .build()
    }
    fn is_shardable(&self) -> bool {
        self.hit("is_shardable");
        true
    }
    async fn enumerate_shards(&self, _target: usize) -> Result<Vec<ShardSpec>, FaucetError> {
        self.hit("enumerate_shards");
        Ok(vec![ShardSpec::whole(), ShardSpec::whole()])
    }
    async fn apply_shard(&self, _shard: &ShardSpec) -> Result<(), FaucetError> {
        self.hit("apply_shard");
        Ok(())
    }
    async fn range_digest(
        &self,
        _range: &KeyRange,
        _key: &str,
        _columns: &[String],
    ) -> Result<Option<ServerDigest>, FaucetError> {
        self.hit("range_digest");
        Ok(Some(ServerDigest {
            algorithm: "hook".into(),
            rows: 1,
            digest: "1".into(),
            key_min: Some(1),
            key_max: Some(1),
        }))
    }
    fn supports_discover(&self) -> bool {
        self.hit("supports_discover");
        true
    }
    async fn discover(&self) -> Result<Vec<DatasetDescriptor>, FaucetError> {
        self.hit("discover");
        Ok(vec![DatasetDescriptor::new("hook", "table", json!({}))])
    }
    fn record_table(&self, _record: &Value) -> Option<String> {
        self.hit("record_table");
        Some("hook".into())
    }
    fn position_le(&self, _a: &Value, _b: &Value) -> Option<bool> {
        self.hit("position_le");
        Some(true)
    }
    fn record_covered(&self, _record: &Value, _position: &Value) -> Option<bool> {
        self.hit("record_covered");
        Some(true)
    }
    fn position_min(&self, positions: &[Value]) -> Option<Value> {
        self.hit("position_min");
        positions.first().cloned()
    }
    fn connector_name(&self) -> &'static str {
        self.hit("connector_name");
        "hook"
    }
    fn set_roundtrip_recorder(&self, _recorder: Arc<RoundtripRecorder>) {
        self.hit("set_roundtrip_recorder");
    }
    fn set_call_context(&self, _ctx: faucet_core::io::CallContext) {
        self.hit("set_call_context");
    }
    fn set_run_clock(
        &self,
        _now: faucet_core::forward::__forward::chrono::DateTime<
            faucet_core::forward::__forward::chrono::Utc,
        >,
    ) {
        self.hit("set_run_clock");
    }
    fn set_cancel(&self, _cancel: CancellationToken) -> bool {
        self.hit("set_cancel");
        true
    }
    fn dataset_uri(&self) -> String {
        self.hit("dataset_uri");
        "hook://dataset".into()
    }
    async fn check(&self, _ctx: &CheckContext) -> Result<CheckReport, FaucetError> {
        self.hit("check");
        Ok(CheckReport::single(Probe::skip("hook", "hook")))
    }
}

async fn drain<T>(mut stream: BoxStream<'_, T>) {
    use futures::StreamExt;
    while stream.next().await.is_some() {}
}

/// Call every [`Source`] hook on `source` (draining each stream it returns),
/// then panic naming each hook in [`SOURCE_HOOKS`] (outside `skip`) that did
/// not reach `probe`, the inner [`HookSource`] the wrapper holds.
pub async fn assert_source_forwards_every_hook(
    source: &dyn Source,
    probe: &HookSource,
    skip: &[&str],
) {
    let ctx = HashMap::new();
    let record = json!({"id": 1});
    let _ = source.fetch_with_context(&ctx).await;
    let _ = source.fetch_all().await;
    let _ = source.fetch_with_context_incremental(&ctx).await;
    let _ = source.fetch_all_incremental().await;
    drain(source.stream_pages(&ctx, 10)).await;
    faucet_core::__faucet_if_arrow! {
        let _ = source.supports_columnar();
        drain(source.stream_batches(&ctx, 10)).await;
    }
    let _ = source.native_output_formats();
    drain(source.stream_native(&ctx, NativeFormat::NdJson, 10)).await;
    let _ = source.config_schema();
    let _ = source.state_key();
    let _ = source.apply_start_bookmark(json!(1)).await;
    let _ = source.capture_resume_position().await;
    let _ = source.lag().await;
    let _ = source.state_schema();
    let _ = source.migrate_state(1, json!(1));
    let _ = source.supports_exactly_once();
    let _ = source.consumes_destructively();
    let _ = source.replay_guarantee();
    let _ = source.capabilities();
    let _ = source.is_shardable();
    let _ = source.enumerate_shards(2).await;
    let _ = source.apply_shard(&ShardSpec::whole()).await;
    let _ = source
        .range_digest(
            &KeyRange {
                lo: Some(0),
                hi: Some(10),
            },
            "id",
            &["id".to_string()],
        )
        .await;
    let _ = source.supports_discover();
    let _ = source.discover().await;
    let _ = source.record_table(&record);
    let _ = source.position_le(&json!(1), &json!(2));
    let _ = source.record_covered(&record, &json!(1));
    let _ = source.position_min(&[json!(2), json!(1)]);
    let _ = source.connector_name();
    source.set_roundtrip_recorder(recorder(RoundtripSide::Source));
    source.set_call_context(faucet_core::io::CallContext::new());
    source.set_run_clock(faucet_core::forward::__forward::chrono::Utc::now());
    let _ = source.set_cancel(CancellationToken::new());
    let _ = source.dataset_uri();
    let _ = source.check(&CheckContext::default()).await;

    check_reached("Source", SOURCE_HOOKS, &probe.reached(), skip);
}
