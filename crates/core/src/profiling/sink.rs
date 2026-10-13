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
    // Profiling needs parsed rows, so the native byte path stays off; everything
    // else reaches the wrapped sink.
    crate::forward_sink!(inner; except
        write_batch, write_batch_reported, write_batch_partial, write_batch_idempotent,
        write_batch_idempotent_reported,
        write_batch_columnar, native_load_capabilities, load_native);

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        let n = self.inner.write_batch(records).await?;
        self.observe(records);
        Ok(n)
    }

    async fn write_batch_reported(
        &self,
        records: &[Value],
    ) -> Result<crate::accounting::WriteReport, FaucetError> {
        let r = self.inner.write_batch_reported(records).await?;
        self.observe(records);
        Ok(r)
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

    async fn write_batch_idempotent_reported(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<crate::accounting::WriteReport, FaucetError> {
        let r = self
            .inner
            .write_batch_idempotent_reported(records, scope, token)
            .await?;
        self.observe(records);
        Ok(r)
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
        assert_eq!(
            sink.batch_atomicity(),
            crate::dlq::BatchAtomicity::BestEffort
        );
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
        assert!(sink.complete_run().await.is_ok());
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
            later_runs: false,
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

    #[tokio::test]
    async fn profiling_sink_forwards_every_hook_but_native() {
        let probe = crate::forwarding::HookSink::default();
        let sink = ProfilingSink::new(
            Box::new(probe.clone()),
            Arc::new(Mutex::new(Profiler::new(ProfilingSpec::default()))),
        );
        crate::forwarding::assert_sink_forwards_every_hook(
            &sink,
            &probe,
            &["native_load_capabilities", "load_native"],
        )
        .await;
    }
}
