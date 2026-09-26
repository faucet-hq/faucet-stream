//! [`ProfilingSink`] — a sink decorator that profiles every record the inner
//! sink accepted (#708). Installed by the CLI executor outermost on the sink,
//! so it observes records after transforms and masking and never sees the
//! `_faucet_*` metadata columns a lower decorator adds — the profile
//! describes exactly what landed in the destination, and a masked column's
//! values are its masked values.

use super::column::Profiler;
use crate::error::FaucetError;
use crate::traits::{RowOutcome, Sink};
use async_trait::async_trait;
use serde_json::Value;
use std::sync::{Arc, Mutex};

/// Wraps a sink and feeds accepted records into a shared [`Profiler`].
pub struct ProfilingSink {
    inner: Box<dyn Sink>,
    profiler: Arc<Mutex<Profiler>>,
}

impl ProfilingSink {
    pub fn new(inner: Box<dyn Sink>, profiler: Arc<Mutex<Profiler>>) -> Self {
        Self { inner, profiler }
    }

    fn observe(&self, records: &[Value]) {
        if let Ok(mut p) = self.profiler.lock() {
            p.observe_page(records);
        }
    }
}

#[async_trait]
impl Sink for ProfilingSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        let n = self.inner.write_batch(records).await?;
        self.observe(records);
        Ok(n)
    }

    /// Only rows the inner sink accepted are profiled — a row that failed into
    /// the DLQ never reached the destination.
    async fn write_batch_partial(&self, records: &[Value]) -> Result<Vec<RowOutcome>, FaucetError> {
        let outcomes = self.inner.write_batch_partial(records).await?;
        if outcomes.iter().all(|o| o.is_ok()) {
            self.observe(records);
        } else {
            let accepted: Vec<Value> = records
                .iter()
                .zip(&outcomes)
                .filter(|(_, o)| o.is_ok())
                .map(|(r, _)| r.clone())
                .collect();
            self.observe(&accepted);
        }
        Ok(outcomes)
    }

    async fn write_batch_idempotent(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<usize, FaucetError> {
        let n = self
            .inner
            .write_batch_idempotent(records, scope, token)
            .await?;
        self.observe(records);
        Ok(n)
    }

    async fn flush(&self) -> Result<(), FaucetError> {
        self.inner.flush().await
    }
    fn connector_name(&self) -> &'static str {
        self.inner.connector_name()
    }
    fn dataset_uri(&self) -> String {
        self.inner.dataset_uri()
    }
    async fn local_outputs(&self) -> Vec<crate::local_outputs::LocalOutput> {
        self.inner.local_outputs().await
    }
    fn supports_idempotent_writes(&self) -> bool {
        self.inner.supports_idempotent_writes()
    }
    fn sink_guarantee(&self) -> crate::idempotency::SinkGuarantee {
        self.inner.sink_guarantee()
    }
    fn write_batch_is_replay_safe(&self) -> bool {
        self.inner.write_batch_is_replay_safe()
    }
    fn dedups_by_key(&self) -> bool {
        self.inner.dedups_by_key()
    }
    fn supported_write_modes(&self) -> &'static [crate::write_mode::WriteMode] {
        self.inner.supported_write_modes()
    }
    async fn last_committed_token(&self, scope: &str) -> Result<Option<String>, FaucetError> {
        self.inner.last_committed_token(scope).await
    }
    async fn current_schema(&self) -> Result<Option<Value>, FaucetError> {
        self.inner.current_schema().await
    }
    fn supports_schema_evolution(&self) -> bool {
        self.inner.supports_schema_evolution()
    }
    async fn evolve_schema(
        &self,
        evolution: &crate::drift::SchemaEvolution,
    ) -> Result<(), FaucetError> {
        self.inner.evolve_schema(evolution).await
    }
    fn supports_cleanup(&self) -> bool {
        self.inner.supports_cleanup()
    }
    fn supports_staged_load(&self) -> bool {
        self.inner.supports_staged_load()
    }
    async fn cleanup_scope(
        &self,
        scope: &std::collections::BTreeMap<String, Value>,
        seen: &crate::cleanup::SeenKeys,
    ) -> Result<u64, FaucetError> {
        self.inner.cleanup_scope(scope, seen).await
    }
    fn is_overwrite(&self) -> bool {
        self.inner.is_overwrite()
    }
    async fn begin_overwrite(&self) -> Result<(), FaucetError> {
        self.inner.begin_overwrite().await
    }
    async fn commit_overwrite(&self) -> Result<(), FaucetError> {
        self.inner.commit_overwrite().await
    }
    async fn abort_overwrite(&self) -> Result<(), FaucetError> {
        self.inner.abort_overwrite().await
    }
    fn supports_rollback(&self) -> bool {
        self.inner.supports_rollback()
    }
    async fn rollback_run(
        &self,
        run_id: &str,
        opts: &crate::rollback::RollbackOptions,
    ) -> Result<crate::rollback::RollbackOutcome, FaucetError> {
        self.inner.rollback_run(run_id, opts).await
    }
    async fn forget_run(&self, run_id: &str) -> Result<(), FaucetError> {
        self.inner.forget_run(run_id).await
    }
    async fn rewind_commit_token(
        &self,
        scope: &str,
        token: Option<&str>,
    ) -> Result<(), FaucetError> {
        self.inner.rewind_commit_token(scope, token).await
    }
    fn readback_source(&self) -> Option<(String, Value)> {
        self.inner.readback_source()
    }
    fn set_roundtrip_recorder(
        &self,
        recorder: std::sync::Arc<crate::observability::RoundtripRecorder>,
    ) {
        self.inner.set_roundtrip_recorder(recorder)
    }
    async fn check(
        &self,
        ctx: &crate::check::CheckContext,
    ) -> Result<crate::check::CheckReport, FaucetError> {
        self.inner.check(ctx).await
    }
    // The columnar fast path is kept: the batch is forwarded untouched and a
    // `Value` view of it is profiled (bounded by the batch size). The native
    // byte path is deliberately NOT forwarded — profiling needs parsed records,
    // so a profiled run takes the row path instead of streaming raw bytes.
    #[cfg(feature = "arrow")]
    fn supports_columnar(&self) -> bool {
        self.inner.supports_columnar()
    }
    #[cfg(feature = "arrow")]
    async fn write_batch_columnar(
        &self,
        batch: &arrow::array::RecordBatch,
    ) -> Result<usize, FaucetError> {
        let n = self.inner.write_batch_columnar(batch).await?;
        if let Ok(values) = crate::columnar::record_batch_to_values(batch) {
            self.observe(&values);
        }
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profiling::ProfilingSpec;
    use serde_json::json;

    struct Partial;
    #[async_trait]
    impl Sink for Partial {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            Ok(records.len())
        }
        async fn write_batch_partial(
            &self,
            records: &[Value],
        ) -> Result<Vec<RowOutcome>, FaucetError> {
            Ok(records
                .iter()
                .map(|r| {
                    if r["bad"].as_bool() == Some(true) {
                        Err(FaucetError::Sink("rejected".into()))
                    } else {
                        Ok(())
                    }
                })
                .collect())
        }
        fn connector_name(&self) -> &'static str {
            "partial"
        }
        fn supports_idempotent_writes(&self) -> bool {
            true
        }
        fn readback_source(&self) -> Option<(String, Value)> {
            Some(("stub".into(), json!({})))
        }
    }

    fn wrapped() -> (ProfilingSink, Arc<Mutex<Profiler>>) {
        let profiler = Arc::new(Mutex::new(Profiler::new(ProfilingSpec::default())));
        (
            ProfilingSink::new(Box::new(Partial), Arc::clone(&profiler)),
            profiler,
        )
    }

    #[tokio::test]
    async fn profiles_accepted_rows_only() {
        let (sink, profiler) = wrapped();
        sink.write_batch(&[json!({"a": 1}), json!({"a": 2})])
            .await
            .unwrap();
        let outcomes = sink
            .write_batch_partial(&[json!({"a": 3}), json!({"a": null, "bad": true})])
            .await
            .unwrap();
        assert!(outcomes[1].is_err());
        sink.write_batch_idempotent(&[json!({"a": 4})], "scope", "t")
            .await
            .unwrap();
        let profile = profiler.lock().unwrap().finish();
        assert_eq!(profile.rows, 4, "the rejected row is not profiled");
        assert_eq!(profile.columns["a"].nulls, 0);
        assert!(!profile.columns.contains_key("bad"));
    }

    #[tokio::test]
    async fn all_accepted_partial_page_is_profiled_whole() {
        let (sink, profiler) = wrapped();
        sink.write_batch_partial(&[json!({"a": 1}), json!({"a": 2})])
            .await
            .unwrap();
        assert_eq!(profiler.lock().unwrap().rows(), 2);
    }

    #[tokio::test]
    async fn capabilities_forward_to_the_inner_sink() {
        let (sink, _) = wrapped();
        assert_eq!(sink.connector_name(), "partial");
        assert!(sink.supports_idempotent_writes());
        assert!(sink.readback_source().is_some());
        assert!(sink.flush().await.is_ok());
        assert!(sink.local_outputs().await.is_empty());
        assert!(!sink.dedups_by_key());
        assert!(!sink.is_overwrite());
        assert!(!sink.supports_rollback());
        assert!(!sink.supports_cleanup());
        assert!(!sink.supports_staged_load());
        assert!(!sink.supports_schema_evolution());
        assert!(sink.current_schema().await.unwrap().is_none());
        assert!(sink.last_committed_token("s").await.unwrap().is_none());
        assert!(sink.begin_overwrite().await.is_err(), "the default rejects");
        assert!(sink.commit_overwrite().await.is_err());
        assert!(sink.abort_overwrite().await.is_ok());
        assert!(sink.forget_run("r").await.is_ok());
        assert!(
            sink.rewind_commit_token("s", None).await.is_err(),
            "the default rejects"
        );
        let opts = crate::rollback::RollbackOptions {
            run_id_column: "_faucet_run_id".into(),
            mode: crate::rollback::RollbackMode::Append,
            force: false,
            dry_run: true,
        };
        assert!(sink.rollback_run("r", &opts).await.is_err());
        assert!(
            sink.cleanup_scope(&Default::default(), &crate::cleanup::SeenKeys::default())
                .await
                .is_err()
        );
        assert!(
            sink.check(&crate::check::CheckContext::default())
                .await
                .is_ok()
        );
        assert_eq!(sink.dataset_uri(), Partial.dataset_uri());
        assert_eq!(
            sink.supported_write_modes(),
            &[crate::write_mode::WriteMode::Append]
        );
        // Native loading is not advertised, so the pipeline profiles rows.
        assert!(sink.native_load_capabilities().is_empty());
    }
}
