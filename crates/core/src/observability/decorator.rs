//! Pipeline-internal decorators that emit spans + metrics around every
//! source / sink trait call. See the design spec for the full vocabulary.

use crate::error::FaucetError;
use crate::observability::labels::Labels;
use crate::observability::timer::DurationGuard;
use crate::pipeline::StreamPage;
use crate::traits::{Sink, Source};
use async_trait::async_trait;
use futures::FutureExt;
use futures_core::Stream;
use metrics::{Label, SharedString, counter, gauge};
use serde_json::Value;
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::{Instrument, info_span};

/// Guard an inner connector's `connector_name()` so an empty string maps to
/// the `"unknown"` fallback. Used both for the `connector` metric label and the
/// `connector_name()` passthrough so the two never disagree.
fn guarded_connector_name(raw: &'static str) -> &'static str {
    if raw.is_empty() { "unknown" } else { raw }
}

/// Build the base `pipeline` / `row` / `connector` label vec once. The two
/// `pipeline` / `row` heap allocations and the vec construction happen a single
/// time at decorator construction; per-call sites `clone()` this instead of
/// rebuilding from the `Arc<str>` labels on every page / write / flush.
fn base_metric_labels(labels: &Labels, connector: &SharedString) -> Vec<Label> {
    vec![
        Label::new("pipeline", SharedString::from(labels.pipeline.to_string())),
        Label::new("row", SharedString::from(labels.row.to_string())),
        Label::new("connector", connector.clone()),
    ]
}

/// Wraps a `&dyn Source` (or any `&S: Source`) and emits spans + metrics
/// around every call. Constructed by `Pipeline::run` and never exposed to
/// end users; the wrapped source remains the user-facing object.
pub struct InstrumentedSource<'a, S: Source + ?Sized> {
    inner: &'a S,
    labels: Labels,
    connector: SharedString,
    /// Precomputed `pipeline` / `row` / `connector` labels, cloned per call.
    base_labels: Vec<Label>,
    page_index: Arc<AtomicUsize>,
}

impl<'a, S: Source + ?Sized> InstrumentedSource<'a, S> {
    pub fn new(inner: &'a S, labels: Labels) -> Self {
        let raw = inner.connector_name();
        debug_assert!(
            !raw.is_empty(),
            "connector_name() must return a non-empty string"
        );
        let connector: SharedString = SharedString::const_str(guarded_connector_name(raw));
        let base_labels = base_metric_labels(&labels, &connector);
        Self {
            inner,
            labels,
            connector,
            base_labels,
            page_index: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn metric_labels(&self) -> Vec<Label> {
        self.base_labels.clone()
    }

    /// Returns `metric_labels()` with an additional `kind` label appended.
    /// Used by `InstrumentedSink::write_batch` (Task 9) and any future
    /// instrumentation paths where `self` is in scope.
    #[allow(dead_code)]
    fn error_labels(&self, kind: &'static str) -> Vec<Label> {
        let mut l = self.metric_labels();
        l.push(Label::new("kind", SharedString::const_str(kind)));
        l
    }
}

#[async_trait]
impl<'a, S: Source + ?Sized> Source for InstrumentedSource<'a, S> {
    fn connector_name(&self) -> &'static str {
        // Return the guarded name so an inner connector that returns "" maps to
        // the "unknown" fallback — keeping this passthrough consistent with the
        // `connector` metric label rather than leaking an empty string.
        guarded_connector_name(self.inner.connector_name())
    }

    fn state_key(&self) -> Option<String> {
        self.inner.state_key()
    }

    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        self.inner.apply_start_bookmark(bookmark).await
    }

    fn supports_exactly_once(&self) -> bool {
        self.inner.supports_exactly_once()
    }

    fn replay_guarantee(&self) -> crate::idempotency::ReplayGuarantee {
        self.inner.replay_guarantee()
    }

    async fn capture_resume_position(&self) -> Result<Option<Value>, FaucetError> {
        self.inner.capture_resume_position().await
    }

    async fn fetch_with_context(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        // Library-call path; the pipeline drives through stream_pages.
        self.inner.fetch_with_context(context).await
    }

    async fn fetch_with_context_incremental(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        self.inner.fetch_with_context_incremental(context).await
    }

    // Columnar fast path (feature `arrow`): forward transparently to the inner
    // source. The columnar streaming loop in `pipeline.rs` emits the source
    // metrics itself, so no instrumentation is layered here (RFC 0002 / #375).
    #[cfg(feature = "arrow")]
    fn supports_columnar(&self) -> bool {
        self.inner.supports_columnar()
    }

    #[cfg(feature = "arrow")]
    fn stream_batches<'b>(
        &'b self,
        context: &'b HashMap<String, Value>,
        batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<crate::columnar::ColumnarPage, FaucetError>> + Send + 'b>>
    {
        self.inner.stream_batches(context, batch_size)
    }

    // Native byte-passthrough capability (#633) forwards to the inner source; the
    // native streaming loop in `pipeline.rs` emits the source metrics itself.
    fn native_output_formats(&self) -> &'static [crate::native::NativeFormat] {
        self.inner.native_output_formats()
    }

    fn stream_native<'b>(
        &'b self,
        context: &'b HashMap<String, Value>,
        format: crate::native::NativeFormat,
        batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<crate::native::NativeBatch, FaucetError>> + Send + 'b>>
    {
        self.inner.stream_native(context, format, batch_size)
    }

    fn stream_pages<'b>(
        &'b self,
        context: &'b HashMap<String, Value>,
        batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'b>> {
        let inner_stream = self.inner.stream_pages(context, batch_size);
        let labels = self.labels.clone();
        let connector = self.connector.clone();
        let page_index = Arc::clone(&self.page_index);
        let metric_labels = self.metric_labels();
        let pipeline = self.labels.pipeline.clone();
        let row = self.labels.row.clone();

        Box::pin(async_stream::try_stream! {
            // In-flight gauge tracks open streams. Decrement on drop so
            // cancellation leaves the gauge consistent.
            struct InFlightGuard(Vec<Label>);
            impl Drop for InFlightGuard {
                fn drop(&mut self) {
                    gauge!("faucet_source_in_flight", self.0.clone()).decrement(1.0);
                }
            }
            gauge!("faucet_source_in_flight", metric_labels.clone()).increment(1.0);
            let _in_flight = InFlightGuard(metric_labels.clone());

            let mut inner = inner_stream;
            loop {
                let idx = page_index.fetch_add(1, Ordering::Relaxed);
                let span = info_span!(
                    "faucet.source.page",
                    pipeline = %pipeline,
                    row = %row,
                    run_id = %labels.run_id,
                    connector = %connector,
                    page_index = idx,
                );
                // Armed across the poll so a cancelled / panicking page-fetch
                // still records the time spent. Disarmed on the terminal empty
                // poll (`Ok(None)`) so end-of-stream doesn't record a spurious
                // ~0 sample into the page-duration histogram.
                let mut _timer = DurationGuard::new(
                    "faucet_source_page_duration_seconds",
                    metric_labels.clone(),
                );

                let next = AssertUnwindSafe(async {
                    use futures::StreamExt;
                    inner.next().await
                })
                .catch_unwind()
                .instrument(span)
                .await;

                match next {
                    Ok(Some(Ok(page))) => {
                        counter!("faucet_source_pages_total", metric_labels.clone()).increment(1);
                        counter!("faucet_source_records_total", metric_labels.clone())
                            .increment(page.records.len() as u64);
                        // Close the timing window BEFORE yielding: in an
                        // `async_stream` the timer local persists across the
                        // yield, so dropping it at scope-exit would fold the
                        // downstream sink/consumer latency into the source's
                        // page-duration histogram (audit #321 M10).
                        _timer.record_now();
                        yield page;
                    }
                    Ok(Some(Err(e))) => {
                        let mut l = metric_labels.clone();
                        l.push(Label::new("kind", SharedString::const_str(error_kind(&e))));
                        counter!("faucet_source_errors_total", l).increment(1);
                        Err(e)?;
                    }
                    Ok(None) => {
                        _timer.disarm();
                        break;
                    }
                    Err(panic) => {
                        let mut l = metric_labels.clone();
                        l.push(Label::new("kind", SharedString::const_str("Panic")));
                        counter!("faucet_source_errors_total", l).increment(1);
                        let msg = panic.downcast_ref::<&'static str>().map(|s| (*s).to_string())
                            .or_else(|| panic.downcast_ref::<String>().cloned())
                            .unwrap_or_else(|| "<non-string panic payload>".to_string());
                        Err(FaucetError::Custom(format!("panic in source: {msg}").into()))?;
                    }
                }
            }
        })
    }
}

/// Map a `FaucetError` variant to its stable `kind` label value. The match
/// must be exhaustive; update when new variants are added.
pub(crate) fn error_kind(e: &FaucetError) -> &'static str {
    match e {
        FaucetError::Http(_) => "Http",
        FaucetError::HttpStatus { .. } => "HttpStatus",
        FaucetError::Json(_) => "Json",
        FaucetError::JsonPath(_) => "JsonPath",
        FaucetError::Auth(_) => "Auth",
        FaucetError::RateLimited { .. } => "RateLimited",
        FaucetError::Url(_) => "Url",
        FaucetError::Transform(_) => "Transform",
        FaucetError::Config(_) => "Config",
        FaucetError::Source(_) => "Source",
        FaucetError::Sink(_) => "Sink",
        FaucetError::QualityFailure { .. } => "QualityFailure",
        FaucetError::SchemaDrift { .. } => "SchemaDrift",
        FaucetError::ContractViolation { .. } => "ContractViolation",
        FaucetError::State(_) => "State",
        FaucetError::CircuitOpen { .. } => "CircuitOpen",
        FaucetError::Custom(_) => "Custom",
    }
}

/// Wraps a `&dyn Sink` (or any `&S: Sink`) and emits spans + metrics around
/// `write_batch` and `flush`. Constructed by `Pipeline::run`.
pub struct InstrumentedSink<'a, S: Sink + ?Sized> {
    inner: &'a S,
    labels: Labels,
    connector: SharedString,
    /// Precomputed `pipeline` / `row` / `connector` labels, cloned per call.
    base_labels: Vec<Label>,
}

impl<'a, S: Sink + ?Sized> InstrumentedSink<'a, S> {
    pub fn new(inner: &'a S, labels: Labels) -> Self {
        let raw = inner.connector_name();
        debug_assert!(
            !raw.is_empty(),
            "connector_name() must return a non-empty string"
        );
        let connector: SharedString = SharedString::const_str(guarded_connector_name(raw));
        let base_labels = base_metric_labels(&labels, &connector);
        Self {
            inner,
            labels,
            connector,
            base_labels,
        }
    }

    fn metric_labels(&self) -> Vec<Label> {
        self.base_labels.clone()
    }

    fn error_labels(&self, kind: &'static str) -> Vec<Label> {
        let mut l = self.metric_labels();
        l.push(Label::new("kind", SharedString::const_str(kind)));
        l
    }
}

#[async_trait]
impl<'a, S: Sink + ?Sized> Sink for InstrumentedSink<'a, S> {
    fn connector_name(&self) -> &'static str {
        // Return the guarded name so an inner connector that returns "" maps to
        // the "unknown" fallback — keeping this passthrough consistent with the
        // `connector` metric label rather than leaking an empty string.
        guarded_connector_name(self.inner.connector_name())
    }

    // Identity + provenance passthroughs. Instrumentation must be invisible to
    // anything asking the sink *what* it is or *what it wrote* — a decorator that
    // falls back to the trait defaults reports `"<name>://unknown"` and an empty
    // output list, which for `local_outputs` means the retention GC (#587) never
    // learns about files this sink created and can never reclaim them. Silent, and
    // only observable as disk filling up.
    fn dataset_uri(&self) -> String {
        self.inner.dataset_uri()
    }

    async fn local_outputs(&self) -> Vec<crate::local_outputs::LocalOutput> {
        self.inner.local_outputs().await
    }

    // Columnar fast path (feature `arrow`): forward transparently to the inner
    // sink; the columnar loop in `pipeline.rs` emits the sink metrics (RFC 0002).
    #[cfg(feature = "arrow")]
    fn supports_columnar(&self) -> bool {
        self.inner.supports_columnar()
    }

    #[cfg(feature = "arrow")]
    async fn write_batch_columnar(
        &self,
        batch: &arrow::array::RecordBatch,
    ) -> Result<usize, FaucetError> {
        self.inner.write_batch_columnar(batch).await
    }

    // Native byte-passthrough load (#633): forward to the inner sink; the native
    // loop in `pipeline.rs` emits the sink metrics.
    fn native_load_capabilities(&self) -> Vec<crate::native::NativeLoadCapability> {
        self.inner.native_load_capabilities()
    }

    async fn load_native(
        &self,
        batch: crate::native::NativeBatch,
        scope: &str,
        ctx: crate::native::NativeLoadContext,
    ) -> Result<usize, FaucetError> {
        self.inner.load_native(batch, scope, ctx).await
    }

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        let span = info_span!(
            "faucet.sink.write",
            pipeline = %self.labels.pipeline,
            row = %self.labels.row,
            run_id = %self.labels.run_id,
            connector = %self.connector,
            records = records.len(),
        );
        let metric_labels = self.metric_labels();
        gauge!("faucet_sink_in_flight", metric_labels.clone()).increment(1.0);

        // RAII guard ensures the gauge is decremented even if write_batch
        // panics or the future is cancelled.
        struct InFlightGuard(Vec<Label>);
        impl Drop for InFlightGuard {
            fn drop(&mut self) {
                gauge!("faucet_sink_in_flight", self.0.clone()).decrement(1.0);
            }
        }
        let _in_flight = InFlightGuard(metric_labels.clone());

        let _timer =
            DurationGuard::new("faucet_sink_write_duration_seconds", metric_labels.clone());

        let result = AssertUnwindSafe(self.inner.write_batch(records))
            .catch_unwind()
            .instrument(span)
            .await;

        match result {
            Ok(Ok(n)) => {
                counter!("faucet_sink_writes_total", metric_labels.clone()).increment(1);
                counter!("faucet_sink_records_total", metric_labels.clone()).increment(n as u64);
                Ok(n)
            }
            Ok(Err(e)) => {
                counter!(
                    "faucet_sink_errors_total",
                    self.error_labels(error_kind(&e))
                )
                .increment(1);
                Err(e)
            }
            Err(panic) => {
                counter!("faucet_sink_errors_total", self.error_labels("Panic")).increment(1);
                let msg = panic
                    .downcast_ref::<&'static str>()
                    .map(|s| (*s).to_string())
                    .or_else(|| panic.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "<non-string panic payload>".to_string());
                Err(FaucetError::Custom(format!("panic in sink: {msg}").into()))
            }
        }
    }

    async fn write_batch_partial(
        &self,
        records: &[Value],
    ) -> Result<Vec<crate::traits::RowOutcome>, FaucetError> {
        let span = info_span!(
            "faucet.sink.write_partial",
            pipeline = %self.labels.pipeline,
            row = %self.labels.row,
            run_id = %self.labels.run_id,
            connector = %self.connector,
            records = records.len(),
        );
        let metric_labels = self.metric_labels();
        gauge!("faucet_sink_in_flight", metric_labels.clone()).increment(1.0);

        // RAII guard ensures the gauge is decremented even if write_batch_partial
        // panics or the future is cancelled.
        struct InFlightGuard(Vec<Label>);
        impl Drop for InFlightGuard {
            fn drop(&mut self) {
                gauge!("faucet_sink_in_flight", self.0.clone()).decrement(1.0);
            }
        }
        let _in_flight = InFlightGuard(metric_labels.clone());

        let _timer =
            DurationGuard::new("faucet_sink_write_duration_seconds", metric_labels.clone());

        let result = AssertUnwindSafe(self.inner.write_batch_partial(records))
            .catch_unwind()
            .instrument(span)
            .await;

        match result {
            Ok(Ok(outcomes)) => {
                let success_count = outcomes.iter().filter(|o| o.is_ok()).count();
                counter!("faucet_sink_writes_total", metric_labels.clone()).increment(1);
                counter!("faucet_sink_records_total", metric_labels.clone())
                    .increment(success_count as u64);
                Ok(outcomes)
            }
            Ok(Err(e)) => {
                counter!(
                    "faucet_sink_errors_total",
                    self.error_labels(error_kind(&e))
                )
                .increment(1);
                Err(e)
            }
            Err(panic) => {
                counter!("faucet_sink_errors_total", self.error_labels("Panic")).increment(1);
                let msg = panic
                    .downcast_ref::<&'static str>()
                    .map(|s| (*s).to_string())
                    .or_else(|| panic.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "<non-string panic payload>".to_string());
                Err(FaucetError::Custom(format!("panic in sink: {msg}").into()))
            }
        }
    }

    async fn flush(&self) -> Result<(), FaucetError> {
        let span = info_span!(
            "faucet.sink.flush",
            pipeline = %self.labels.pipeline,
            row = %self.labels.row,
            run_id = %self.labels.run_id,
            connector = %self.connector,
        );
        let metric_labels = self.metric_labels();
        let _timer =
            DurationGuard::new("faucet_sink_flush_duration_seconds", metric_labels.clone());

        let result = AssertUnwindSafe(self.inner.flush())
            .catch_unwind()
            .instrument(span)
            .await;

        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => {
                counter!(
                    "faucet_sink_errors_total",
                    self.error_labels(error_kind(&e))
                )
                .increment(1);
                Err(e)
            }
            Err(panic) => {
                counter!("faucet_sink_errors_total", self.error_labels("Panic")).increment(1);
                let msg = panic
                    .downcast_ref::<&'static str>()
                    .map(|s| (*s).to_string())
                    .or_else(|| panic.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "<non-string panic payload>".to_string());
                Err(FaucetError::Custom(format!("panic in flush: {msg}").into()))
            }
        }
    }

    // ── Non-instrumented passthroughs ────────────────────────────────────────
    // These carry no per-call metric/span of their own, but they MUST delegate
    // to the inner sink — the `Sink` trait gives each a default that disables
    // the corresponding feature (schema-drift, upsert, exactly-once). Because
    // the pipeline drives the *wrapped* sink, failing to forward them silently
    // makes those features inert through the entire CLI/observability path.

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

    fn supported_write_modes(&self) -> &'static [crate::write_mode::WriteMode] {
        self.inner.supported_write_modes()
    }

    fn supports_cleanup(&self) -> bool {
        self.inner.supports_cleanup()
    }

    async fn cleanup_scope(
        &self,
        scope: &std::collections::BTreeMap<String, Value>,
        seen: &crate::cleanup::SeenKeys,
    ) -> Result<u64, FaucetError> {
        self.inner.cleanup_scope(scope, seen).await
    }

    fn supports_idempotent_writes(&self) -> bool {
        self.inner.supports_idempotent_writes()
    }

    fn sink_guarantee(&self) -> crate::idempotency::SinkGuarantee {
        self.inner.sink_guarantee()
    }

    fn dedups_by_key(&self) -> bool {
        self.inner.dedups_by_key()
    }

    async fn write_batch_idempotent(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<usize, FaucetError> {
        self.inner
            .write_batch_idempotent(records, scope, token)
            .await
    }

    async fn last_committed_token(&self, scope: &str) -> Result<Option<String>, FaucetError> {
        self.inner.last_committed_token(scope).await
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
}

#[cfg(test)]
pub(crate) mod source_tests {
    use super::*;
    use async_trait::async_trait;
    use futures::StreamExt;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
    use serde_json::json;
    use std::sync::{Mutex, OnceLock};

    // Process-global recorder shared across all observability tests in this
    // crate. Task 5 established the same pattern.
    pub(crate) static LOCK: Mutex<()> = Mutex::new(());
    static SNAPSHOTTER: OnceLock<Snapshotter> = OnceLock::new();

    pub(crate) fn snapshotter() -> &'static Snapshotter {
        SNAPSHOTTER.get_or_init(|| {
            let recorder = DebuggingRecorder::new();
            let snap = recorder.snapshotter();
            // First test installs; the OnceLock guarantees we never install
            // twice. If something else (e.g. the timer test) already installed
            // a recorder, `set_global_recorder` will Err — but in that case
            // *our* snapshotter is disconnected from the live recorder. The
            // workaround is for all observability tests to share one source of
            // truth — this file. If a future test elsewhere installs a
            // recorder first, restructure so all tests share this OnceLock.
            let _ = metrics::set_global_recorder(recorder);
            snap
        })
    }

    pub(in crate::observability) fn labels() -> Labels {
        Labels::new("p", "r", "rid")
    }

    struct MockSource(Vec<Value>);
    #[async_trait]
    impl Source for MockSource {
        async fn fetch_with_context(
            &self,
            _: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(self.0.clone())
        }
        fn connector_name(&self) -> &'static str {
            "mock"
        }
    }

    struct PanickingSource;
    #[async_trait]
    impl Source for PanickingSource {
        async fn fetch_with_context(
            &self,
            _: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            panic!("kaboom")
        }
        fn connector_name(&self) -> &'static str {
            "panic-test"
        }
    }

    // Inner connector that returns an empty name. The instrumented wrapper must
    // map this to the `"unknown"` fallback so the `connector_name()` passthrough
    // never disagrees with the `connector` metric label.
    struct EmptyNameSource;
    #[async_trait]
    impl Source for EmptyNameSource {
        async fn fetch_with_context(
            &self,
            _: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(vec![])
        }
        fn connector_name(&self) -> &'static str {
            ""
        }
    }

    #[test]
    fn empty_inner_connector_name_falls_back_to_unknown() {
        let inner = EmptyNameSource;
        // `InstrumentedSource::new` debug_asserts on an empty inner name, so
        // build the wrapper directly with the fallback name to exercise the
        // passthrough without tripping the assertion in debug builds.
        let wrapped = InstrumentedSource {
            inner: &inner,
            labels: labels(),
            connector: SharedString::const_str("unknown"),
            base_labels: Vec::new(),
            page_index: Arc::new(AtomicUsize::new(0)),
        };
        assert_eq!(
            Source::connector_name(&wrapped),
            "unknown",
            "instrumented source must not leak an empty connector name"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn records_records_counter_per_page() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();
        let inner = MockSource((0..5).map(|i| json!({"i": i})).collect());
        let wrapped = InstrumentedSource::new(&inner, labels());
        let ctx = HashMap::new();
        let mut s = wrapped.stream_pages(&ctx, 2);
        while s.next().await.is_some() {}
        let snapshot = snap.snapshot();
        let records: u64 = snapshot
            .into_vec()
            .into_iter()
            .filter_map(|(key, _u, _d, v)| {
                if key.key().name() == "faucet_source_records_total"
                    && let DebugValue::Counter(c) = v
                {
                    return Some(c);
                }
                None
            })
            .sum();
        assert!(
            records >= 5,
            "expected at least 5 records counted, got {records}"
        );
    }

    // Source with a unique connector name so the page-duration histogram for
    // this run can be isolated in the shared global recorder.
    struct PageCountSource(Vec<Value>);
    #[async_trait]
    impl Source for PageCountSource {
        async fn fetch_with_context(
            &self,
            _: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(self.0.clone())
        }
        fn connector_name(&self) -> &'static str {
            "page-count-probe"
        }
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn page_duration_records_one_sample_per_yielded_page() {
        // 5 records at batch_size 2 → pages [2, 2, 1] = 3 yielded pages. The
        // terminal empty poll must NOT add a 4th (spurious ~0) sample.
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();
        let inner = PageCountSource((0..5).map(|i| json!({"i": i})).collect());
        let wrapped = InstrumentedSource::new(&inner, labels());
        let ctx = HashMap::new();
        let mut s = wrapped.stream_pages(&ctx, 2);
        let mut pages = 0usize;
        while s.next().await.is_some() {
            pages += 1;
        }
        assert_eq!(pages, 3, "expected 3 yielded pages");

        let snapshot = snap.snapshot();
        let samples: usize = snapshot
            .into_vec()
            .into_iter()
            .filter_map(|(key, _u, _d, v)| {
                if key.key().name() == "faucet_source_page_duration_seconds"
                    && key
                        .key()
                        .labels()
                        .any(|l| l.key() == "connector" && l.value() == "page-count-probe")
                    && let DebugValue::Histogram(h) = v
                {
                    return Some(h.len());
                }
                None
            })
            .sum();
        assert_eq!(
            samples, pages,
            "page-duration histogram must have exactly one sample per yielded \
             page ({pages}), not page+1 (no spurious terminal sample)"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn maps_panic_to_custom_error_with_kind_panic() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _snap = snapshotter();
        let inner = PanickingSource;
        let wrapped = InstrumentedSource::new(&inner, labels());
        let ctx = HashMap::new();
        let mut s = wrapped.stream_pages(&ctx, 10);
        let first = s
            .next()
            .await
            .expect("stream yields at least one item before terminating");
        assert!(matches!(first, Err(FaucetError::Custom(_))));
        // Process did not abort — implicit by reaching this line.
    }

    // ── error_kind: exhaustive variant → label mapping ───────────────────────

    #[test]
    fn error_kind_covers_all_variants() {
        use std::time::Duration;
        // Build one of every non-`Http` FaucetError variant and assert its
        // stable label. (`Http` wraps a `reqwest::Error`, which has no public
        // constructor; it is exercised through the live request paths in the
        // connector crates' tests.)
        let cases: Vec<(FaucetError, &str)> = vec![
            (
                FaucetError::HttpStatus {
                    status: 500,
                    url: "u".into(),
                    body: "b".into(),
                },
                "HttpStatus",
            ),
            (
                FaucetError::Json(serde_json::from_str::<Value>("nope").unwrap_err()),
                "Json",
            ),
            (FaucetError::JsonPath("bad".into()), "JsonPath"),
            (FaucetError::Auth("a".into()), "Auth"),
            (
                FaucetError::RateLimited(Duration::from_secs(1)),
                "RateLimited",
            ),
            (FaucetError::Url("bad url".into()), "Url"),
            (FaucetError::Transform("t".into()), "Transform"),
            (FaucetError::Config("c".into()), "Config"),
            (FaucetError::Source("s".into()), "Source"),
            (FaucetError::Sink("s".into()), "Sink"),
            (
                FaucetError::QualityFailure {
                    check: "chk".into(),
                    message: "m".into(),
                },
                "QualityFailure",
            ),
            (FaucetError::State("st".into()), "State"),
            (
                FaucetError::CircuitOpen {
                    failures: 3,
                    cooldown: Duration::from_secs(60),
                },
                "CircuitOpen",
            ),
            (
                FaucetError::Custom(Box::new(std::io::Error::other("boom"))),
                "Custom",
            ),
        ];
        for (err, expected) in cases {
            assert_eq!(error_kind(&err), expected, "mismatch for {err:?}");
        }
    }

    // ── Source passthrough methods ───────────────────────────────────────────

    // A source that overrides every passthrough so the instrumented wrapper's
    // delegating methods (state_key / apply_start_bookmark / fetch_with_context
    // / fetch_with_context_incremental) are exercised.
    struct PassthroughSource {
        seen_bookmark: Mutex<Option<Value>>,
    }
    #[async_trait]
    impl Source for PassthroughSource {
        async fn fetch_with_context(
            &self,
            _: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(vec![json!({"fwc": 1})])
        }
        async fn fetch_with_context_incremental(
            &self,
            _: &HashMap<String, Value>,
        ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
            Ok((vec![json!({"inc": 1})], Some(json!("bm"))))
        }
        fn state_key(&self) -> Option<String> {
            Some("passthrough_key".into())
        }
        async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
            *self.seen_bookmark.lock().unwrap() = Some(bookmark);
            Ok(())
        }
        fn connector_name(&self) -> &'static str {
            "passthrough"
        }
    }

    #[tokio::test]
    async fn source_passthroughs_delegate_to_inner() {
        let inner = PassthroughSource {
            seen_bookmark: Mutex::new(None),
        };
        let wrapped = InstrumentedSource::new(&inner, labels());

        // state_key passthrough
        assert_eq!(wrapped.state_key(), Some("passthrough_key".to_string()));

        // fetch_with_context passthrough
        let ctx = HashMap::new();
        assert_eq!(
            wrapped.fetch_with_context(&ctx).await.unwrap(),
            vec![json!({"fwc": 1})]
        );

        // fetch_with_context_incremental passthrough
        let (recs, bm) = wrapped.fetch_with_context_incremental(&ctx).await.unwrap();
        assert_eq!(recs, vec![json!({"inc": 1})]);
        assert_eq!(bm, Some(json!("bm")));

        // apply_start_bookmark passthrough
        wrapped.apply_start_bookmark(json!("resume")).await.unwrap();
        assert_eq!(
            *inner.seen_bookmark.lock().unwrap(),
            Some(json!("resume")),
            "apply_start_bookmark must reach the inner source"
        );

        // capability passthroughs: defaults for this inner source…
        assert!(!wrapped.supports_exactly_once());
        assert_eq!(
            wrapped.replay_guarantee(),
            crate::idempotency::ReplayGuarantee::NonDeterministic
        );
        assert_eq!(wrapped.capture_resume_position().await.unwrap(), None);
    }

    /// A source advertising exactly-once — the decorator must not mask it
    /// (the pipeline's mechanism selection reads these through the wrapper).
    struct ExactlyOnceSource;
    #[async_trait]
    impl Source for ExactlyOnceSource {
        async fn fetch_with_context(
            &self,
            _context: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(vec![])
        }
        fn supports_exactly_once(&self) -> bool {
            true
        }
        async fn capture_resume_position(&self) -> Result<Option<Value>, FaucetError> {
            Ok(Some(json!("pos")))
        }
        fn connector_name(&self) -> &'static str {
            "eo-source"
        }
    }

    #[tokio::test]
    async fn source_capability_passthroughs_delegate_to_inner() {
        let inner = ExactlyOnceSource;
        let wrapped = InstrumentedSource::new(&inner, labels());
        assert!(wrapped.supports_exactly_once());
        assert_eq!(
            wrapped.replay_guarantee(),
            crate::idempotency::ReplayGuarantee::Deterministic,
            "typed capability derives through the wrapper"
        );
        assert_eq!(
            wrapped.capture_resume_position().await.unwrap(),
            Some(json!("pos"))
        );
    }
}

#[cfg(test)]
mod sink_tests {
    use super::source_tests::{LOCK, labels, snapshotter};
    use super::*;
    use async_trait::async_trait;
    use metrics_util::debugging::DebugValue;
    use serde_json::json;

    struct MockSink(std::sync::Mutex<Vec<Value>>);
    #[async_trait]
    impl Sink for MockSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            self.0.lock().unwrap().extend(records.iter().cloned());
            Ok(records.len())
        }
        fn connector_name(&self) -> &'static str {
            "mock-sink"
        }
    }

    struct FailingSink;
    #[async_trait]
    impl Sink for FailingSink {
        async fn write_batch(&self, _: &[Value]) -> Result<usize, FaucetError> {
            Err(FaucetError::Sink("nope".into()))
        }
        fn connector_name(&self) -> &'static str {
            "failing-sink"
        }
    }

    struct EmptyNameSink;
    #[async_trait]
    impl Sink for EmptyNameSink {
        async fn write_batch(&self, _: &[Value]) -> Result<usize, FaucetError> {
            Ok(0)
        }
        fn connector_name(&self) -> &'static str {
            ""
        }
    }

    #[test]
    fn empty_inner_connector_name_falls_back_to_unknown() {
        let inner = EmptyNameSink;
        // `InstrumentedSink::new` debug_asserts on an empty inner name, so build
        // the wrapper directly with the fallback name to exercise the
        // passthrough without tripping the assertion in debug builds.
        let wrapped = InstrumentedSink {
            inner: &inner,
            labels: labels(),
            connector: SharedString::const_str("unknown"),
            base_labels: Vec::new(),
        };
        assert_eq!(
            Sink::connector_name(&wrapped),
            "unknown",
            "instrumented sink must not leak an empty connector name"
        );
    }

    /// Regression (#194): the pipeline drives the *wrapped* sink, so
    /// `InstrumentedSink` MUST forward the capability methods to the inner sink.
    /// Before this was fixed, the trait defaults (`current_schema -> None`,
    /// `supports_schema_evolution -> false`, `supports_idempotent_writes ->
    /// false`) silently disabled schema-drift, evolution, and exactly-once
    /// detection through the entire observability/CLI path even when the real
    /// sink supported them.
    struct CapableSink;
    #[async_trait]
    impl Sink for CapableSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            Ok(records.len())
        }
        fn connector_name(&self) -> &'static str {
            "capable-sink"
        }
        async fn current_schema(&self) -> Result<Option<Value>, FaucetError> {
            Ok(Some(
                json!({"type": "object", "properties": {"id": {"type": "integer"}}}),
            ))
        }
        fn supports_schema_evolution(&self) -> bool {
            true
        }
        fn supports_idempotent_writes(&self) -> bool {
            true
        }
        fn supported_write_modes(&self) -> &'static [crate::write_mode::WriteMode] {
            &[
                crate::write_mode::WriteMode::Append,
                crate::write_mode::WriteMode::Upsert,
            ]
        }
        async fn last_committed_token(&self, _scope: &str) -> Result<Option<String>, FaucetError> {
            Ok(Some("tok-1".into()))
        }
        fn dedups_by_key(&self) -> bool {
            true
        }
    }

    #[tokio::test]
    async fn instrumented_sink_forwards_capability_methods_to_inner() {
        let inner = CapableSink;
        let wrapped = InstrumentedSink::new(&inner, labels());

        // Schema-drift (#194): the wrapper must surface the inner schema, not the
        // `None` default — otherwise drift detection is inert through the pipeline.
        assert_eq!(
            wrapped.current_schema().await.unwrap(),
            Some(json!({"type": "object", "properties": {"id": {"type": "integer"}}})),
            "current_schema must delegate to the inner sink"
        );
        assert!(
            wrapped.supports_schema_evolution(),
            "supports_schema_evolution must delegate"
        );
        // Pre-existing capabilities the wrapper must also forward.
        assert!(
            wrapped.supports_idempotent_writes(),
            "supports_idempotent_writes must delegate (exactly-once)"
        );
        assert!(
            wrapped
                .supported_write_modes()
                .contains(&crate::write_mode::WriteMode::Upsert),
            "supported_write_modes must delegate"
        );
        assert_eq!(
            wrapped.last_committed_token("scope").await.unwrap(),
            Some("tok-1".to_string()),
            "last_committed_token must delegate"
        );
        // Typed delivery capabilities (#292): the pipeline's mechanism
        // selection reads these through the wrapper.
        assert_eq!(
            wrapped.sink_guarantee(),
            crate::idempotency::SinkGuarantee::AtomicWatermark,
            "sink_guarantee must delegate"
        );
        assert!(wrapped.dedups_by_key(), "dedups_by_key must delegate");
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn records_writes_and_records_counters() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();
        let inner = MockSink(std::sync::Mutex::new(Vec::new()));
        let wrapped = InstrumentedSink::new(&inner, labels());
        wrapped
            .write_batch(&[json!({"a": 1}), json!({"a": 2})])
            .await
            .unwrap();
        let snapshot = snap.snapshot();
        let writes: u64 = snapshot
            .into_vec()
            .into_iter()
            .filter_map(|(key, _u, _d, v)| {
                if key.key().name() == "faucet_sink_writes_total"
                    && let DebugValue::Counter(c) = v
                {
                    return Some(c);
                }
                None
            })
            .sum();
        assert!(writes >= 1, "expected at least one write counted");
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn error_increments_errors_total_with_kind() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();
        let inner = FailingSink;
        let wrapped = InstrumentedSink::new(&inner, labels());
        let _ = wrapped.write_batch(&[json!({})]).await;
        let snapshot = snap.snapshot();
        let found = snapshot.into_vec().into_iter().any(|(key, _u, _d, v)| {
            key.key().name() == "faucet_sink_errors_total"
                && key
                    .key()
                    .labels()
                    .any(|l| l.key() == "kind" && l.value() == "Sink")
                && matches!(v, DebugValue::Counter(c) if c >= 1)
        });
        assert!(found, "expected sink_errors_total with kind=Sink");
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn instrumented_sink_write_batch_partial_counts_successful_outcomes() {
        use crate::traits::RowOutcome;
        use metrics_util::debugging::DebugValue;

        // Sink that returns 2 Ok + 1 Err.
        struct MixedSink;
        #[async_trait]
        impl Sink for MixedSink {
            async fn write_batch(&self, _r: &[Value]) -> Result<usize, FaucetError> {
                unreachable!()
            }
            async fn write_batch_partial(
                &self,
                _r: &[Value],
            ) -> Result<Vec<RowOutcome>, FaucetError> {
                Ok(vec![
                    Ok(()),
                    Err(FaucetError::Sink("bad row".into())),
                    Ok(()),
                ])
            }
            fn connector_name(&self) -> &'static str {
                "mixed"
            }
        }

        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();

        let inner = MixedSink;
        let wrapped = InstrumentedSink::new(&inner, labels());
        let _ = wrapped
            .write_batch_partial(&[json!({}), json!({}), json!({})])
            .await
            .unwrap();

        // faucet_sink_records_total should reflect 2 (Ok count), not 3.
        // Filter to this test's own labels (connector="mixed") — prior tests in
        // the same `mod sink_tests` (e.g. records_writes_and_records_counters
        // for connector="mock-sink") leave entries in the shared global
        // recorder, and the HashMap-iteration order of `Snapshot::into_vec()`
        // is non-deterministic, so a naïve `find_map` returns an arbitrary
        // entry.
        let snapshot = snap.snapshot();
        let records: u64 = snapshot
            .into_vec()
            .into_iter()
            .filter_map(|(k, _u, _d, v): (metrics_util::CompositeKey, _, _, _)| {
                if k.key().name() == "faucet_sink_records_total"
                    && k.key()
                        .labels()
                        .any(|l| l.key() == "connector" && l.value() == "mixed")
                    && let DebugValue::Counter(c) = v
                {
                    Some(c)
                } else {
                    None
                }
            })
            .sum();
        assert!(
            records >= 2,
            "expected faucet_sink_records_total{{connector=mixed}} >= 2, got {records}"
        );
    }

    // ── flush error path ─────────────────────────────────────────────────────

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn flush_error_increments_errors_total_and_propagates() {
        // A sink whose flush() returns Err must surface the error and emit
        // faucet_sink_errors_total with the matching kind label.
        struct FlushFailSink;
        #[async_trait]
        impl Sink for FlushFailSink {
            async fn write_batch(&self, r: &[Value]) -> Result<usize, FaucetError> {
                Ok(r.len())
            }
            async fn flush(&self) -> Result<(), FaucetError> {
                Err(FaucetError::Sink("flush boom".into()))
            }
            fn connector_name(&self) -> &'static str {
                "flush-fail-sink"
            }
        }

        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();
        let inner = FlushFailSink;
        let wrapped = InstrumentedSink::new(&inner, labels());
        let err = wrapped.flush().await.unwrap_err();
        assert!(matches!(&err, FaucetError::Sink(m) if m.contains("flush boom")));

        let snapshot = snap.snapshot();
        let found = snapshot.into_vec().into_iter().any(|(key, _u, _d, v)| {
            key.key().name() == "faucet_sink_errors_total"
                && key
                    .key()
                    .labels()
                    .any(|l| l.key() == "connector" && l.value() == "flush-fail-sink")
                && key
                    .key()
                    .labels()
                    .any(|l| l.key() == "kind" && l.value() == "Sink")
                && matches!(v, DebugValue::Counter(c) if c >= 1)
        });
        assert!(
            found,
            "expected sink_errors_total{{connector=flush-fail-sink,kind=Sink}}"
        );
    }

    // ── panic isolation on every sink call ───────────────────────────────────

    struct PanickingSink;
    #[async_trait]
    impl Sink for PanickingSink {
        async fn write_batch(&self, _: &[Value]) -> Result<usize, FaucetError> {
            panic!("write kaboom")
        }
        async fn write_batch_partial(
            &self,
            _: &[Value],
        ) -> Result<Vec<crate::traits::RowOutcome>, FaucetError> {
            panic!("partial kaboom")
        }
        async fn flush(&self) -> Result<(), FaucetError> {
            panic!("flush kaboom")
        }
        fn connector_name(&self) -> &'static str {
            "panic-sink"
        }
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn write_batch_panic_maps_to_custom_error() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _snap = snapshotter();
        let inner = PanickingSink;
        let wrapped = InstrumentedSink::new(&inner, labels());
        let err = wrapped.write_batch(&[json!({})]).await.unwrap_err();
        match err {
            FaucetError::Custom(b) => {
                assert!(b.to_string().contains("panic in sink: write kaboom"))
            }
            other => panic!("expected Custom panic error, got {other:?}"),
        }
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn write_batch_partial_panic_maps_to_custom_error() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _snap = snapshotter();
        let inner = PanickingSink;
        let wrapped = InstrumentedSink::new(&inner, labels());
        let err = wrapped.write_batch_partial(&[json!({})]).await.unwrap_err();
        match err {
            FaucetError::Custom(b) => {
                assert!(b.to_string().contains("panic in sink: partial kaboom"))
            }
            other => panic!("expected Custom panic error, got {other:?}"),
        }
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn flush_panic_maps_to_custom_error() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _snap = snapshotter();
        let inner = PanickingSink;
        let wrapped = InstrumentedSink::new(&inner, labels());
        let err = wrapped.flush().await.unwrap_err();
        match err {
            FaucetError::Custom(b) => {
                assert!(b.to_string().contains("panic in flush: flush kaboom"))
            }
            other => panic!("expected Custom panic error, got {other:?}"),
        }
    }
    /// `InstrumentedSink` must forward the identity + provenance methods.
    ///
    /// It wraps every sink whenever observability is active — i.e. the default
    /// build — and it forwarded neither of these, which review caught. The
    /// failure mode is silent: `local_outputs()` falling back to the trait
    /// default hides every file the inner sink created from the retention GC
    /// (#587), so those files are never reclaimed and nothing logs or errors.
    /// `dataset_uri()` falling back records `jsonl://unknown` in lineage and the
    /// catalog.
    ///
    /// The sibling decorators are covered in
    /// `tests/local_output_forwarding.rs`; this one lives here because the
    /// module is private.
    #[tokio::test]
    async fn instrumented_sink_forwards_identity_and_local_outputs() {
        use crate::local_outputs::{LocalOutput, LocalOutputLog};

        struct FileSink {
            outputs: LocalOutputLog,
        }

        #[async_trait]
        impl Sink for FileSink {
            async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
                Ok(records.len())
            }
            fn connector_name(&self) -> &'static str {
                "jsonl"
            }
            fn dataset_uri(&self) -> String {
                "file:///tmp/out.jsonl".to_string()
            }
            async fn local_outputs(&self) -> Vec<LocalOutput> {
                self.outputs.snapshot()
            }
        }

        let outputs = LocalOutputLog::new();
        outputs.record_open("/tmp/out.jsonl", false);
        let inner = FileSink { outputs };
        let sink = InstrumentedSink::new(&inner, Labels::new("p", "r", "run-1"));

        let reported = sink.local_outputs().await;
        assert_eq!(
            reported.len(),
            1,
            "InstrumentedSink must not hide the inner sink's files from the GC"
        );
        assert_eq!(reported[0].path, std::path::PathBuf::from("/tmp/out.jsonl"));
        assert!(
            !reported[0].pre_existing,
            "classification must survive verbatim"
        );
        assert_eq!(sink.dataset_uri(), "file:///tmp/out.jsonl");
    }
}
