//! Test support: a sink that overrides every defaulted [`Sink`] hook with a
//! recognisable answer, and an assertion that a decorator forwards each one.

use crate::FaucetError;
use crate::check::{CheckContext, CheckReport, Probe};
use crate::cleanup::SeenKeys;
use crate::dlq::BatchAtomicity;
use crate::drift::SchemaEvolution;
use crate::idempotency::SinkGuarantee;
use crate::local_outputs::LocalOutput;
use crate::native::{
    CsvDialect, NativeBatch, NativeFormat, NativeLoadCapability, NativeLoadContext, NativePayload,
};
use crate::observability::{RoundtripRecorder, RoundtripSide};
use crate::rollback::{RollbackMode, RollbackOptions, RollbackOutcome};
use crate::traits::{RowOutcome, Sink};
use crate::write_mode::WriteMode;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// Every defaulted `Sink` hook a decorator must forward, by name.
pub(crate) const HOOKS: &[&str] = &[
    "write_batch",
    "flush",
    "write_batch_partial",
    "batch_atomicity",
    "admit_page",
    #[cfg(feature = "arrow")]
    "supports_columnar",
    #[cfg(feature = "arrow")]
    "write_batch_columnar",
    "native_load_capabilities",
    "load_native",
    "supports_idempotent_writes",
    "sink_guarantee",
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
    "dataset_uri",
    "local_outputs",
    "check",
];

/// A sink whose every hook answers differently from the trait default and
/// records that it was reached.
#[derive(Debug, Default, Clone)]
pub(crate) struct HookSink {
    pub calls: Arc<Mutex<Vec<&'static str>>>,
}

impl HookSink {
    fn hit(&self, name: &'static str) {
        self.calls.lock().unwrap().push(name);
    }

    pub fn reached(&self) -> Vec<&'static str> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl Sink for HookSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        self.hit("write_batch");
        Ok(records.len())
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
    #[cfg(feature = "arrow")]
    fn supports_columnar(&self) -> bool {
        self.hit("supports_columnar");
        true
    }
    #[cfg(feature = "arrow")]
    async fn write_batch_columnar(
        &self,
        batch: &arrow::array::RecordBatch,
    ) -> Result<usize, FaucetError> {
        self.hit("write_batch_columnar");
        Ok(batch.num_rows())
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

/// Call every hook on `sink` and return the names `probe` saw, minus `skip`.
/// Panics naming every hook in [`HOOKS`] (outside `skip`) that did not reach
/// the inner sink.
pub(crate) async fn assert_forwards_every_hook(sink: &dyn Sink, probe: &HookSink, skip: &[&str]) {
    let page = [json!({"id": 1})];
    let _ = sink.write_batch(&page).await;
    let _ = sink.flush().await;
    let _ = sink.write_batch_partial(&page).await;
    let _ = sink.batch_atomicity();
    let _ = sink.admit_page(&page).await;
    #[cfg(feature = "arrow")]
    {
        let _ = sink.supports_columnar();
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
        ]));
        let batch = arrow::array::RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::Int64Array::from(vec![1]))],
        )
        .unwrap();
        let _ = sink.write_batch_columnar(&batch).await;
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
    sink.set_roundtrip_recorder(Arc::new(RoundtripRecorder::new(
        RoundtripSide::Sink,
        "p",
        "r",
        "hook",
    )));
    let _ = sink.dataset_uri();
    let _ = sink.local_outputs().await;
    let _ = sink.check(&CheckContext::default()).await;

    let reached = probe.reached();
    let missing: Vec<&str> = HOOKS
        .iter()
        .copied()
        .filter(|h| !skip.contains(h) && !reached.contains(h))
        .collect();
    assert!(missing.is_empty(), "hooks not forwarded: {missing:?}");
}
