//! Source-to-sink pipeline orchestration.
//!
//! The [`Pipeline`] struct connects any [`Source`] to any
//! [`Sink`] and handles moving data between them.
//!
//! # Batch mode
//!
//! Fetches all records from the source, then writes them to the sink in one
//! shot.  Supports incremental replication (returns a bookmark for the next
//! run).
//!
//! ```rust,no_run
//! use faucet_core::{Pipeline, Source, Sink};
//! # async fn example(source: impl Source, sink: impl Sink) -> Result<(), faucet_core::FaucetError> {
//! let result = Pipeline::new(&source, &sink).run().await?;
//! println!("wrote {} records", result.records_written);
//! // Persist result.bookmark for the next incremental run
//! # Ok(())
//! # }
//! ```
//!
//! # Streaming mode
//!
//! Writes records page-by-page as they arrive from a source's
//! [`stream_pages`](crate::Source::stream_pages) implementation, keeping
//! memory usage bounded.  [`Pipeline::run`] uses this internally; callers
//! that have already assembled a [`StreamPage`] stream can drive it directly
//! via [`run_stream`].
//!
//! ```rust,no_run
//! use faucet_core::{run_stream, RunStreamOptions, Sink, StreamPage, FaucetError};
//! use futures_core::Stream;
//! # async fn example(
//! #     pages: impl Stream<Item = Result<StreamPage, FaucetError>> + Unpin,
//! #     sink: impl Sink,
//! # ) -> Result<(), FaucetError> {
//! let result = run_stream(pages, &sink, RunStreamOptions::new()).await?;
//! # Ok(())
//! # }
//! ```

use crate::dlq::{DlqConfig, DlqStats};
use crate::error::FaucetError;
use crate::observability::RunStreamOptions;
use crate::state::{StateStore, validate_state_key};
use crate::traits::{Sink, Source};
use futures_core::Stream;
use serde_json::Value;
use std::pin::Pin;
use std::sync::Arc;

/// Default page size used when a caller does not specify one.
///
/// Sources are free to override this from their own config when implementing
/// [`Source::stream_pages`]; the value passed
/// from the pipeline acts as a hint when no source-side preference exists.
pub const DEFAULT_BATCH_SIZE: usize = 1000;

/// Hard upper bound on `batch_size`. Values above this (other than the
/// special `0` "no batching" sentinel) are rejected at config validation
/// time to prevent accidental O(total) buffering in the default
/// implementation of [`Source::stream_pages`].
pub const MAX_BATCH_SIZE: usize = 1_000_000;

/// Validate a `batch_size` value against the global constraints.
///
/// `batch_size = 0` is the **opt-out-of-batching sentinel**: sources and
/// sinks should treat it as "emit / accept the entire result set in one
/// page." This is useful for small lookup tables or for sinks (e.g. SQL
/// `COPY`, BigQuery load jobs) that prefer one large request to many small
/// ones. Any non-zero value above [`MAX_BATCH_SIZE`] is rejected to prevent
/// accidental unbounded buffering through a typo.
///
/// Returns the unchanged value on success. Returns `FaucetError::Config`
/// only for values strictly greater than [`MAX_BATCH_SIZE`].
pub fn validate_batch_size(batch_size: usize) -> Result<usize, FaucetError> {
    if batch_size > MAX_BATCH_SIZE {
        return Err(FaucetError::Config(format!(
            "batch_size {batch_size} exceeds maximum {MAX_BATCH_SIZE} \
             (use 0 to opt out of batching entirely)"
        )));
    }
    Ok(batch_size)
}

/// One page emitted by [`Source::stream_pages`].
///
/// `records` is the chunk of records for this page. `bookmark` is `Some` only
/// when the source has a durable checkpoint to advance — most sources emit
/// `Some` only on the final page (max-replication-value semantics); CDC-style
/// sources emit `Some` per committed transaction. The pipeline flushes the
/// sink and persists the bookmark every time a page carries one, so a
/// mid-stream crash never advances past records the sink has not durably
/// written.
#[derive(Debug, Clone, Default)]
pub struct StreamPage {
    /// Records to write to the sink for this page.
    pub records: Vec<Value>,
    /// Optional bookmark to checkpoint after this page is durably written.
    pub bookmark: Option<Value>,
}

/// Result of a pipeline run.
#[derive(Debug, Clone)]
pub struct PipelineResult {
    /// Total number of records written to the sink.
    pub records_written: usize,
    /// Bookmark value for incremental replication.
    ///
    /// `Some(value)` when the source returned a bookmark on its final
    /// (or, for streaming CDC sources, most recent) page. Persist this and
    /// pass it back as `start_replication_value` on the next run; this is
    /// handled automatically when a [`StateStore`] is attached via
    /// [`Pipeline::with_state_store`].
    pub bookmark: Option<Value>,
    /// DLQ counters. `None` when no DLQ is configured.
    pub dlq: Option<DlqStats>,
}

/// A pipeline that moves data from a [`Source`] to a [`Sink`].
///
/// The pipeline is generic over the source and sink types — any combination
/// of connectors works as long as they implement the respective traits.
pub struct Pipeline<'a, So: Source + ?Sized, Si: Sink + ?Sized> {
    source: &'a So,
    sink: &'a Si,
    state_store: Option<Arc<dyn StateStore>>,
    name: Option<String>,
    row: Option<String>,
    run_id: Option<String>,
    dlq: Option<DlqConfig>,
    #[cfg(feature = "quality")]
    quality: Option<Arc<crate::quality::CompiledQuality>>,
    #[cfg(feature = "contract")]
    contract: Option<Arc<crate::contract::CompiledContract>>,
    #[cfg(feature = "masking")]
    masking: Option<Arc<crate::masking::CompiledMasking>>,
    adaptive: Option<crate::adaptive::AdaptiveBatchConfig>,
    cancel: Option<tokio_util::sync::CancellationToken>,
    delivery: crate::idempotency::DeliveryMode,
    resilience: Option<crate::resilience::ResiliencePolicy>,
    schema_drift: Option<crate::drift::SchemaDriftPolicy>,
    cleanup: Option<std::sync::Arc<crate::cleanup::CleanupPolicy>>,
    /// When `true`, `run()` skips the overwrite begin/commit/abort lifecycle
    /// even for an overwrite-configured sink — an external caller (the CLI
    /// executor) owns that lifecycle so it can run it **once per destination
    /// across a whole fan-out group** instead of once per invocation (#552).
    /// Writes still land in the sink's staging target (the sink routes there
    /// off its own `is_overwrite()` config, independent of this flag). Default
    /// `false`: a standalone `Pipeline::run` manages its own overwrite.
    suppress_overwrite_lifecycle: bool,
}

impl<'a, So: Source + ?Sized, Si: Sink + ?Sized> Pipeline<'a, So, Si> {
    /// Create a new pipeline from a source and a sink.
    pub fn new(source: &'a So, sink: &'a Si) -> Self {
        Self {
            source,
            sink,
            state_store: None,
            name: None,
            row: None,
            run_id: None,
            dlq: None,
            #[cfg(feature = "quality")]
            quality: None,
            #[cfg(feature = "contract")]
            contract: None,
            #[cfg(feature = "masking")]
            masking: None,
            adaptive: None,
            cancel: None,
            delivery: crate::idempotency::DeliveryMode::AtLeastOnce,
            resilience: None,
            schema_drift: None,
            cleanup: None,
            suppress_overwrite_lifecycle: false,
        }
    }

    /// Suppress the overwrite begin/commit/abort lifecycle inside `run()` so an
    /// external orchestrator can drive it **once per destination across a whole
    /// fan-out group** rather than once per invocation (#552). Only meaningful
    /// with a `write_mode: overwrite` sink; a no-op otherwise. The sink still
    /// writes into its staging target (it routes there off its own config), so
    /// the orchestrator must call `begin_overwrite` before, and
    /// `commit_overwrite`/`abort_overwrite` after, all the suppressed
    /// invocations that share the destination.
    pub fn with_suppress_overwrite(mut self, suppress: bool) -> Self {
        self.suppress_overwrite_lifecycle = suppress;
        self
    }

    /// Attach a [`StateStore`] for persistent incremental-replication bookmarks.
    ///
    /// When configured, `run()` will:
    /// 1. Read any previously stored bookmark at the source's
    ///    [`state_key`](Source::state_key) and call
    ///    [`apply_start_bookmark`](Source::apply_start_bookmark) on the source
    ///    so it can resume from that point.
    /// 2. Run the fetch + write as usual.
    /// 3. Persist the new bookmark **only after** the sink confirms the
    ///    batch was written and flushed.
    ///
    /// Sources that do not return a [`state_key`](Source::state_key) are
    /// unaffected — the store is consulted only when the source opts in.
    pub fn with_state_store(mut self, store: Arc<dyn StateStore>) -> Self {
        self.state_store = Some(store);
        self
    }

    /// Set the pipeline name used in spans and metric labels.
    /// Defaults to `"unnamed"` when unset.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Set the matrix row id used in spans and metric labels.
    /// Defaults to `""` (Prometheus treats empty labels as absent).
    pub fn with_row(mut self, row: impl Into<String>) -> Self {
        self.row = Some(row.into());
        self
    }

    /// Set an explicit run id (UUIDv7-shaped). When unset, `Pipeline::run`
    /// generates one. Used only as a tracing span attribute — never a metric
    /// label.
    pub fn with_run_id(mut self, run_id: impl Into<String>) -> Self {
        self.run_id = Some(run_id.into());
        self
    }

    /// Attach a DLQ for per-row failure routing.
    pub fn with_dlq(mut self, dlq: DlqConfig) -> Self {
        self.dlq = Some(dlq);
        self
    }

    /// Attach a compiled quality spec. Checks run after transforms, before the
    /// sink, per page.
    #[cfg(feature = "quality")]
    pub fn with_quality(mut self, quality: Arc<crate::quality::CompiledQuality>) -> Self {
        self.quality = Some(quality);
        self
    }

    /// Attach a compiled data contract (issue #204). The pass runs after the
    /// quality pass and before the schema-drift pass, per page.
    #[cfg(feature = "contract")]
    pub fn with_contract(mut self, contract: Arc<crate::contract::CompiledContract>) -> Self {
        self.contract = Some(contract);
        self
    }

    /// Attach a compiled masking policy (issue #206). The masking pass runs
    /// per page *first* — before quality/contract/drift and every sink write —
    /// so PII never reaches a sink, the DLQ, or a lineage sample unmasked.
    #[cfg(feature = "masking")]
    pub fn with_masking(mut self, masking: Arc<crate::masking::CompiledMasking>) -> Self {
        self.masking = Some(masking);
        self
    }

    /// Attach an adaptive batch-size controller (opt-in). When `enabled`, the
    /// pipeline reslices each source page into sub-batches whose size the
    /// controller tunes from observed sink latency + error rate.
    pub fn with_adaptive(mut self, cfg: crate::adaptive::AdaptiveBatchConfig) -> Self {
        self.adaptive = Some(cfg);
        self
    }

    /// Attach a cancellation token. When cancelled mid-run, the streaming loop
    /// stops at the next page boundary, flushes the sink(s) so buffered output
    /// (e.g. a Parquet footer) is durable, and returns the partial result
    /// instead of leaving the file unreadable (#146 H16).
    pub fn with_cancel(mut self, cancel: tokio_util::sync::CancellationToken) -> Self {
        self.cancel = Some(cancel);
        self
    }

    /// Set the delivery guarantee. `ExactlyOnce` requires a state store, an
    /// idempotent sink (`Sink::supports_idempotent_writes`), and a
    /// deterministic-replay source — otherwise `run` returns
    /// `FaucetError::Config`.
    pub fn with_delivery(mut self, mode: crate::idempotency::DeliveryMode) -> Self {
        self.delivery = mode;
        self
    }

    /// Attach a resilience policy (retry/backoff/circuit-breaker/poison-pill).
    pub fn with_resilience(mut self, policy: crate::resilience::ResiliencePolicy) -> Self {
        self.resilience = Some(policy);
        self
    }

    /// Attach a schema-drift policy. The drift pass runs after the quality pass
    /// and before the sink write, per page.
    pub fn with_schema_drift(mut self, policy: crate::drift::SchemaDriftPolicy) -> Self {
        self.schema_drift = Some(policy);
        self
    }

    /// Attach a scoped-cleanup policy (#478). After a fully successful,
    /// uncancelled run the sink is asked to delete rows in the claimed scope
    /// that this run did not write. See [`crate::cleanup`] for why the timing
    /// matters.
    pub fn with_cleanup(mut self, policy: std::sync::Arc<crate::cleanup::CleanupPolicy>) -> Self {
        self.cleanup = Some(policy);
        self
    }

    /// Run the pipeline in streaming mode.
    ///
    /// 1. Loads the stored bookmark and pushes it to the source (if a state
    ///    store is configured and the source returns a `state_key`).
    /// 2. Drives [`Source::stream_pages`] with [`DEFAULT_BATCH_SIZE`],
    ///    writing each page to the sink as it arrives via
    ///    [`Sink::write_batch`].
    /// 3. Whenever a page carries `Some(bookmark)`, flushes the sink and
    ///    persists the bookmark to the state store before polling the next
    ///    page. This makes per-page CDC checkpointing automatic.
    /// 4. Flushes the sink one final time after the stream completes.
    /// 5. Returns a [`PipelineResult`] with the total count and the last
    ///    bookmark observed.
    pub async fn run(&self) -> Result<PipelineResult, FaucetError> {
        use crate::observability::{
            DurationGuard, InstrumentedSink, InstrumentedSource, InstrumentedStateStore, Labels,
        };
        use metrics::{Label, SharedString, counter, gauge};
        use tracing::Instrument;

        // Resolve identity for this run.
        let name = self.name.clone().unwrap_or_else(|| "unnamed".to_string());
        let row = self.row.clone().unwrap_or_default();
        let run_id = self
            .run_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::now_v7().to_string());
        let obs_labels = Labels::new(name.clone(), row.clone(), run_id.clone());

        // Wrap source, sink, state-store.
        let wrapped_source = InstrumentedSource::new(self.source, obs_labels.clone());
        let wrapped_sink = InstrumentedSink::new(self.sink, obs_labels.clone());
        let wrapped_state_store: Option<Arc<dyn StateStore>> = self.state_store.as_ref().map(|s| {
            Arc::new(InstrumentedStateStore::new(
                Arc::clone(s),
                obs_labels.clone(),
            )) as Arc<dyn StateStore>
        });

        // Pipeline-level span. Use .instrument(span) on the inner future so
        // the span correctly enters/exits across awaits.
        let span = tracing::info_span!(
            "faucet.pipeline.run",
            pipeline = %name,
            row = %row,
            run_id = %run_id,
            source = %wrapped_source.connector_name(),
            sink = %wrapped_sink.connector_name(),
        );

        // Per-pipeline metric labels (pipeline + row).
        let base_labels: Vec<Label> = vec![
            Label::new("pipeline", SharedString::from(name.clone())),
            Label::new("row", SharedString::from(row.clone())),
        ];
        let run_labels: Vec<Label> = {
            let mut v = base_labels.clone();
            v.push(Label::new(
                "source",
                SharedString::from(wrapped_source.connector_name().to_string()),
            ));
            v.push(Label::new(
                "sink",
                SharedString::from(wrapped_sink.connector_name().to_string()),
            ));
            v
        };

        // RAII guard so the in-flight gauge stays consistent even on cancellation.
        struct InFlightGuard(Vec<Label>);
        impl Drop for InFlightGuard {
            fn drop(&mut self) {
                gauge!("faucet_pipeline_in_flight", self.0.clone()).decrement(1.0);
            }
        }
        gauge!("faucet_pipeline_in_flight", base_labels.clone()).increment(1.0);
        let _in_flight = InFlightGuard(base_labels.clone());

        // Stamp the start time so dashboards can compute uptime for long-running
        // (streaming / CDC) pipelines where `*_run_duration_seconds` never fires.
        let start_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        gauge!(
            "faucet_pipeline_start_time_unix_seconds",
            base_labels.clone()
        )
        .set(start_unix);

        // Histogram timer for the whole run.
        let _run_timer =
            DurationGuard::new("faucet_pipeline_run_duration_seconds", run_labels.clone());

        // Run inside the span.
        let result = async {
            // Bookmark resume — goes through the wrapped state store so the
            // get is instrumented too.
            let state_key = self.source.state_key();
            let mut start_seq = 0u64;
            if let (Some(store), Some(key)) = (wrapped_state_store.as_ref(), state_key.as_ref()) {
                validate_state_key(key)?;
                if let Some(prior) = store.get(key).await? {
                    if self.delivery == crate::idempotency::DeliveryMode::ExactlyOnce {
                        let (bookmark, seq) = crate::idempotency::unwrap_state(&prior);
                        start_seq = seq;
                        if let Some(bm) = bookmark {
                            wrapped_source.apply_start_bookmark(bm).await?;
                        }
                    } else {
                        wrapped_source.apply_start_bookmark(prior).await?;
                    }
                }
            }

            // Sink-anchored resume (atomic-watermark mechanism): the sink's
            // committed watermark embeds the exact stream position of the last
            // committed page. In the crash window between "sink durably
            // committed" and "state store persisted" the sink is one page
            // ahead of the state store — recover that position from the sink
            // and re-anchor the source there, so nothing is re-written *and*
            // nothing depends on the source replaying identical page
            // boundaries (which log-positional sources like Kafka cannot
            // promise). Tokens written before bookmarks were embedded parse
            // with no bookmark and fall back to the skip-on-resume path.
            if self.delivery == crate::idempotency::DeliveryMode::ExactlyOnce
                && wrapped_sink.supports_idempotent_writes()
                && wrapped_source.replay_guarantee()
                    == crate::idempotency::ReplayGuarantee::Deterministic
                && let Some(key) = state_key.as_ref()
                && let Some(token) = wrapped_sink.last_committed_token(key).await?
                && let Some((sink_seq, Some(bm))) = crate::idempotency::parse_token_parts(&token)
                && sink_seq > start_seq
            {
                wrapped_source.apply_start_bookmark(bm).await?;
                start_seq = sink_seq;
            }

            // Native byte-passthrough fast path (#633) — the cheapest mechanism
            // (no `Value` and no Arrow materialization), so it is tried first.
            // When the source can emit its wire bytes in a format the sink can
            // bulk-load directly, and nothing needs `Value` access, stream the
            // bytes straight through. Any `Value`-shaped or per-page mechanism
            // (DLQ, exactly-once, quality/contract/masking/drift, cleanup,
            // adaptive, resilience) disqualifies it and falls through below.
            {
                let source_formats = wrapped_source.native_output_formats();
                // Pipeline mechanics that live in `run_stream` and would be
                // silently skipped by the byte path (mirrors the columnar guards).
                let native_mechanics_ok = !source_formats.is_empty()
                    && self.schema_drift.is_none()
                    && self.adaptive.is_none()
                    && self.resilience.is_none()
                    && self.cleanup.is_none();
                // Governance passes need `Value`; the planner's `requires_passthrough`
                // prerequisite rejects them, so surface them as `has_governance`.
                // `mut` is used only when a governance feature is enabled.
                #[allow(unused_mut)]
                let mut has_governance = false;
                #[cfg(feature = "quality")]
                {
                    has_governance = has_governance || self.quality.is_some();
                }
                #[cfg(feature = "contract")]
                {
                    has_governance = has_governance || self.contract.is_some();
                }
                #[cfg(feature = "masking")]
                {
                    has_governance = has_governance || self.masking.is_some();
                }
                if native_mechanics_ok {
                    let sink_caps = wrapped_sink.native_load_capabilities();
                    let write_mode = if wrapped_sink.is_overwrite() {
                        crate::write_mode::WriteMode::Overwrite
                    } else {
                        crate::write_mode::WriteMode::Append
                    };
                    let plan =
                        crate::native::plan_native_transfer(&crate::native::NativePlanInputs {
                            source_formats,
                            sink_caps: &sink_caps,
                            // Transforms are enforced by the wrapper not advertising
                            // native formats; the pipeline itself holds no transforms.
                            has_transforms: false,
                            has_governance,
                            delivery: self.delivery,
                            write_mode,
                            has_dlq: self.dlq.is_some(),
                        });
                    if let Some(plan) = plan {
                        let state = match (wrapped_state_store.clone(), state_key.clone()) {
                            (Some(store), Some(key)) => Some((store, key)),
                            _ => None,
                        };
                        return run_stream_native(
                            &wrapped_source,
                            &wrapped_sink,
                            plan,
                            write_mode,
                            state,
                            self.cancel.clone(),
                            &name,
                            &row,
                        )
                        .await;
                    }
                }
            }

            // Columnar (Arrow) fast path — feature `arrow`, RFC 0002 / #375.
            // When both the source and sink speak Arrow *and* no `Value`-shaped
            // stage needs to observe the records, drive the columnar loop and
            // skip `Value` materialization entirely. Any complicating feature
            // (DLQ, exactly-once, masking/quality/contract/drift, adaptive,
            // resilience) falls through to the `Value` path below. Bookmark
            // resume above already ran, so a columnar source resumes correctly.
            #[cfg(feature = "arrow")]
            {
                let columnar_ok = wrapped_source.supports_columnar()
                    && wrapped_sink.supports_columnar()
                    && self.dlq.is_none()
                    && self.delivery == crate::idempotency::DeliveryMode::AtLeastOnce
                    && self.schema_drift.is_none()
                    && self.adaptive.is_none()
                    && self.resilience.is_none()
                    // The columnar fast-path bypasses `run_stream`, which is
                    // where cleanup accumulates written keys and fires the
                    // delete. Taking it with a policy attached would silently
                    // skip the cleanup and leave the stale rows behind — the
                    // exact failure this feature exists to prevent (#478).
                    && self.cleanup.is_none()
                    // Overwrite (#492) needs the begin/commit staging lifecycle
                    // wired below; the columnar path would append straight to
                    // the destination and skip the atomic swap.
                    && !wrapped_sink.is_overwrite();
                #[cfg(feature = "quality")]
                let columnar_ok = columnar_ok && self.quality.is_none();
                #[cfg(feature = "contract")]
                let columnar_ok = columnar_ok && self.contract.is_none();
                #[cfg(feature = "masking")]
                let columnar_ok = columnar_ok && self.masking.is_none();
                if columnar_ok {
                    let state = match (wrapped_state_store.clone(), state_key.clone()) {
                        (Some(store), Some(key)) => Some((store, key)),
                        _ => None,
                    };
                    return run_stream_columnar(
                        &wrapped_source,
                        &wrapped_sink,
                        state,
                        self.cancel.clone(),
                        &name,
                        &row,
                        &run_id,
                    )
                    .await;
                }
            }

            let ctx = std::collections::HashMap::new();
            let pages = wrapped_source.stream_pages(&ctx, DEFAULT_BATCH_SIZE);

            let mut opts = RunStreamOptions::new()
                .with_name(name.clone())
                .with_row(row.clone())
                .with_run_id(run_id.clone());
            if let (Some(store), Some(key)) = (wrapped_state_store.clone(), state_key) {
                opts = opts.with_state(store, key);
            }
            if let Some(dlq) = self.dlq.clone() {
                opts = opts.with_dlq(dlq);
            }
            #[cfg(feature = "quality")]
            if let Some(q) = self.quality.clone() {
                opts = opts.with_quality(q);
            }
            #[cfg(feature = "contract")]
            if let Some(c) = self.contract.clone() {
                opts = opts.with_contract(c);
            }
            #[cfg(feature = "masking")]
            if let Some(m) = self.masking.clone() {
                opts = opts.with_masking(m);
            }
            if let Some(ad) = self.adaptive.clone() {
                opts = opts.with_adaptive(ad);
            }
            if let Some(cancel) = self.cancel.clone() {
                opts = opts.with_cancel(cancel);
            }
            if let Some(policy) = self.resilience.clone() {
                opts = opts.with_resilience(policy);
            }
            if let Some(p) = self.schema_drift {
                opts = opts.with_schema_drift(p);
            }
            opts = opts
                .with_delivery(self.delivery)
                .with_start_seq(start_seq)
                .with_replay_guarantee(wrapped_source.replay_guarantee());

            // ── Scoped cleanup (#478) ────────────────────────────────────
            // The tracker wraps the sink so the written-key set is recorded by
            // the thing doing the writing, rather than by threading a second
            // concern through the page loop. It also keeps the policy off
            // `RunStreamOptions`, whose shape is public API.
            //
            // The delete fires here, after `run_stream` returns Ok, and only
            // when the run was not cancelled: a cancelled run read a partial set
            // of the scope, so "delete what I didn't see" would remove rows that
            // simply had not arrived yet.
            // ── Overwrite lifecycle (#492) ───────────────────────────────
            // Stage before the first write, swap only after a fully successful
            // uncancelled run, and abort (best-effort) otherwise — so a mid-run
            // failure or cancel never destroys the existing destination.
            // #552: when the lifecycle is suppressed, an external orchestrator
            // (the CLI executor) owns begin/commit/abort across the fan-out
            // group, so a per-invocation begin here would re-create (wipe) the
            // shared staging table each parent — the silent data-loss bug.
            let overwriting = wrapped_sink.is_overwrite() && !self.suppress_overwrite_lifecycle;
            if overwriting {
                wrapped_sink.begin_overwrite().await?;
            }

            let run_result = match self.cleanup.clone() {
                None => run_stream(pages, &wrapped_sink, opts).await,
                Some(policy) => {
                    if !wrapped_sink.supports_cleanup() {
                        return Err(FaucetError::Config(format!(
                            "cleanup is configured but sink '{}' does not support scoped cleanup",
                            wrapped_sink.connector_name()
                        )));
                    }
                    let tracker = crate::cleanup::CleanupTracker::new(&wrapped_sink, &policy);
                    let result = run_stream(pages, &tracker, opts).await;
                    match result {
                        Ok(r) => {
                            let cancelled = self.cancel.as_ref().is_some_and(|c| c.is_cancelled());
                            if cancelled {
                                tracing::warn!(
                                    "scoped cleanup skipped: the run was cancelled, so the \
                                     set of written keys is incomplete and a delete would \
                                     remove rows that were never read"
                                );
                                crate::observability::cleanup_run(&name, &row, "skipped_cancelled");
                            } else {
                                let tracked = tracker.tracked();
                                match tracker.finish(&policy).await {
                                    Ok(deleted) => {
                                        crate::observability::cleanup_deleted(
                                            &name,
                                            &row,
                                            wrapped_sink.connector_name(),
                                            deleted,
                                        );
                                        crate::observability::cleanup_run(&name, &row, "applied");
                                        tracing::info!(
                                            deleted,
                                            kept = tracked,
                                            "scoped cleanup complete: deleted destination rows \
                                             in the claimed scope that this run did not write"
                                        );
                                    }
                                    Err(e) => {
                                        crate::observability::cleanup_run(
                                            &name,
                                            &row,
                                            "refused_overflow",
                                        );
                                        return Err(e);
                                    }
                                }
                            }
                            Ok(r)
                        }
                        Err(e) => Err(e),
                    }
                }
            };

            // Finalize the overwrite: swap staging → destination only on a
            // fully successful uncancelled run; otherwise discard staging and
            // leave the prior destination exactly as it was.
            if overwriting {
                let cancelled = self.cancel.as_ref().is_some_and(|c| c.is_cancelled());
                match &run_result {
                    Ok(_) if !cancelled => wrapped_sink.commit_overwrite().await?,
                    _ => {
                        if let Err(e) = wrapped_sink.abort_overwrite().await {
                            tracing::warn!(
                                error = %e,
                                "failed to discard overwrite staging after an unsuccessful \
                                 or cancelled run; the destination is unchanged"
                            );
                        }
                    }
                }
            }
            run_result
        }
        .instrument(span)
        .await;

        // Final run-counter increment. On error, also attach a `kind` label
        // (matching the FaucetError variant) so dashboards can break out failed
        // runs by error type without spelunking the *_errors_total surfaces.
        let status = if result.is_ok() { "ok" } else { "err" };
        let mut final_labels = run_labels;
        final_labels.push(Label::new("status", SharedString::const_str(status)));
        if let Err(ref e) = result {
            final_labels.push(Label::new(
                "kind",
                SharedString::const_str(crate::observability::decorator::error_kind(e)),
            ));
        }
        counter!("faucet_pipeline_runs_total", final_labels).increment(1);

        result
    }
}

/// Columnar (Arrow) fast path, driven by [`Pipeline::run`] when both the source
/// and sink advertise `supports_columnar()` and no `Value`-shaped stage is
/// configured (feature `arrow`, RFC 0002 / #375).
///
/// Mirrors the checkpoint ordering of the `Value` path exactly —
/// `write_batch_columnar` → `flush` → persist bookmark ([ADR 0002](https://github.com/faucet-hq/faucet-stream/blob/main/docs/adr/0002-checkpoint-ordering.md)) —
/// with cooperative, flush-completing cancellation at the page boundary
/// ([ADR 0011](https://github.com/faucet-hq/faucet-stream/blob/main/docs/adr/0011-cooperative-cancellation.md)).
/// Emits the source/sink record counters; the richer per-page histograms of the
/// `Value` path are not layered on this loop yet.
#[cfg(feature = "arrow")]
async fn run_stream_columnar<S, Si>(
    source: &S,
    sink: &Si,
    state: Option<(Arc<dyn StateStore>, String)>,
    cancel: Option<tokio_util::sync::CancellationToken>,
    pipeline: &str,
    row: &str,
    run_id: &str,
) -> Result<PipelineResult, FaucetError>
where
    S: crate::Source + ?Sized,
    Si: Sink + ?Sized,
{
    use futures::StreamExt;
    use metrics::{Label, SharedString, counter};

    let labels = |connector: &str| -> Vec<Label> {
        vec![
            Label::new("pipeline", SharedString::from(pipeline.to_string())),
            Label::new("row", SharedString::from(row.to_string())),
            Label::new("connector", SharedString::from(connector.to_string())),
        ]
    };
    let src_labels = labels(source.connector_name());
    let sink_labels = labels(sink.connector_name());
    let _ = run_id; // reserved for span attribution parity with the Value path

    let ctx = std::collections::HashMap::new();
    let mut batches = source.stream_batches(&ctx, DEFAULT_BATCH_SIZE);
    let mut records_written = 0usize;
    let mut last_bookmark: Option<Value> = None;

    loop {
        // Cooperative cancellation: race the next batch against the token so a
        // cancel stops at the boundary and still flushes (ADR 0011).
        let page = match &cancel {
            Some(token) => {
                tokio::select! {
                    biased;
                    _ = token.cancelled() => break,
                    p = batches.next() => p,
                }
            }
            None => batches.next().await,
        };
        let Some(page) = page else { break };
        let page = page?;
        let rows = page.num_rows();
        counter!("faucet_source_records_total", src_labels.clone()).increment(rows as u64);

        if rows > 0 {
            let n = sink.write_batch_columnar(&page.batch).await?;
            records_written += n;
            counter!("faucet_sink_records_total", sink_labels.clone()).increment(n as u64);
            counter!("faucet_sink_writes_total", sink_labels.clone()).increment(1);
        }

        // Checkpoint: flush then persist the bookmark, never the other way
        // round (ADR 0002) — the state store is always at or behind the sink.
        if let Some(bm) = page.bookmark {
            sink.flush().await?;
            if let Some((store, key)) = state.as_ref() {
                store.put(key, &bm).await?;
            }
            last_bookmark = Some(bm);
        }
    }

    // Final flush (mirrors the Value path's end-of-stream / on-cancel flush).
    sink.flush().await?;
    Ok(PipelineResult {
        records_written,
        bookmark: last_bookmark,
        dlq: None,
    })
}

/// Drive the **native byte-passthrough** fast path (#633): stream the source's
/// wire bytes via [`Source::stream_native`] and hand each batch to the sink's
/// [`Sink::load_native`], never materializing `Value` or Arrow. Selected in
/// [`Pipeline::run`] when [`plan_native_transfer`](crate::native::plan_native_transfer)
/// finds a qualifying mechanism.
///
/// Checkpoint ordering mirrors [`run_stream_columnar`] and the `Value` path
/// (flush → persist bookmark, ADR 0002; cooperative cancel at the batch
/// boundary, ADR 0011).
///
/// **Overwrite is owned by the mechanism**, not the generic
/// `begin`/`commit`/`abort` lifecycle: a sink whose native capability lists
/// [`WriteMode::Overwrite`](crate::write_mode::WriteMode::Overwrite) in its
/// prerequisites truncates on the first batch and appends thereafter, driven by
/// [`NativeLoadContext::first_batch`](crate::native::NativeLoadContext::first_batch).
/// The `Value`-based staging swap (#492) cannot apply here — `load_native`
/// carries bytes, not `Value` rows — so a sink that cannot own overwrite simply
/// omits `Overwrite` from `write_modes`, and the negotiation falls through to the
/// `Value` path.
#[allow(clippy::too_many_arguments)]
async fn run_stream_native<S, Si>(
    source: &S,
    sink: &Si,
    plan: crate::native::NativePlan,
    write_mode: crate::write_mode::WriteMode,
    state: Option<(Arc<dyn StateStore>, String)>,
    cancel: Option<tokio_util::sync::CancellationToken>,
    pipeline: &str,
    row: &str,
) -> Result<PipelineResult, FaucetError>
where
    S: crate::Source + ?Sized,
    Si: Sink + ?Sized,
{
    use futures::StreamExt;
    use metrics::{Label, SharedString, counter};

    let labels = |connector: &str| -> Vec<Label> {
        vec![
            Label::new("pipeline", SharedString::from(pipeline.to_string())),
            Label::new("row", SharedString::from(row.to_string())),
            Label::new("connector", SharedString::from(connector.to_string())),
        ]
    };
    let src_labels = labels(source.connector_name());
    let sink_labels = labels(sink.connector_name());
    let scope = if row.is_empty() {
        pipeline.to_string()
    } else {
        format!("{pipeline}::{row}")
    };

    tracing::info!(
        pipeline = %pipeline,
        row = %row,
        format = plan.format.as_str(),
        mechanism = plan.mechanism,
        "native byte-passthrough fast path selected"
    );
    let ctx = std::collections::HashMap::new();
    let mut batches = source.stream_native(&ctx, plan.format, DEFAULT_BATCH_SIZE);
    let mut records_written = 0usize;
    let mut last_bookmark: Option<Value> = None;
    let mut first_batch = true;

    loop {
        let next = match &cancel {
            Some(token) => {
                tokio::select! {
                    biased;
                    _ = token.cancelled() => break,
                    p = batches.next() => p,
                }
            }
            None => batches.next().await,
        };
        let Some(batch) = next else { break };
        let batch = batch?;
        if let Some(n) = batch.records {
            counter!("faucet_source_records_total", src_labels.clone()).increment(n);
        }
        let bookmark = batch.bookmark.clone();
        let load_ctx = crate::native::NativeLoadContext {
            write_mode,
            first_batch,
        };
        let n = sink.load_native(batch, &scope, load_ctx).await?;
        first_batch = false;
        records_written += n;
        counter!("faucet_sink_records_total", sink_labels.clone()).increment(n as u64);
        counter!("faucet_sink_writes_total", sink_labels.clone()).increment(1);

        // Checkpoint: flush then persist the bookmark (ADR 0002).
        if let Some(bm) = bookmark {
            sink.flush().await?;
            if let Some((store, key)) = state.as_ref() {
                store.put(key, &bm).await?;
            }
            last_bookmark = Some(bm);
        }
    }
    // Final flush (mirrors the end-of-stream / on-cancel flush).
    sink.flush().await?;

    Ok(PipelineResult {
        records_written,
        bookmark: last_bookmark,
        dlq: None,
    })
}

/// Run a streaming pipeline, writing each [`StreamPage`] to the sink as it
/// arrives and persisting bookmarks per page.
///
/// This keeps memory usage bounded — only one page of records is held at a
/// time. The stream comes from [`Source::stream_pages`] (or any
/// `Stream<Item = Result<StreamPage, FaucetError>>` a caller assembles
/// directly).
///
/// Bookmark semantics: whenever a page carries `Some(bookmark)`, the sink is
/// flushed and the bookmark is persisted (when `state_store` and `state_key`
/// are both `Some`) before the next page is polled. Sources that only know
/// their bookmark after seeing every record emit `Some` on the final page;
/// CDC-style sources emit `Some` per committed transaction and get
/// per-transaction durability automatically.
///
/// Returns the cumulative [`PipelineResult`] — `records_written` is the sum
/// across all pages and `bookmark` is the last per-page bookmark observed.
pub async fn run_stream<S, Si>(
    mut pages: S,
    sink: &Si,
    options: RunStreamOptions,
) -> Result<PipelineResult, FaucetError>
where
    S: Stream<Item = Result<StreamPage, FaucetError>> + Unpin,
    Si: Sink + ?Sized,
{
    use crate::dlq::{DlqReason, DlqStats, OnBatchError, build_envelope};

    let state_store = options.state_store.clone();
    let state_key = options.state_key.clone();
    let pipeline_name = options.pipeline_name.unwrap_or_else(|| "unnamed".into());
    let row = options.row.unwrap_or_default();
    let run_id = options.run_id.unwrap_or_default();
    let dlq = options.dlq.clone();
    let cancel = options.cancel.clone();

    #[cfg(feature = "quality")]
    let quality = options.quality.clone();

    // Fail fast: quarantine requires a DLQ sink.
    #[cfg(feature = "quality")]
    if let Some(q) = quality.as_ref()
        && q.requires_dlq()
        && dlq.is_none()
    {
        return Err(FaucetError::Config(
            "quality: on_failure 'quarantine'/'quarantine_batch' requires a DLQ sink".into(),
        ));
    }

    #[cfg(feature = "contract")]
    let contract = options.contract.clone();
    // Fail fast: contract quarantine requires a DLQ (mirrors the quality guard).
    #[cfg(feature = "contract")]
    if let Some(c) = contract.as_ref()
        && c.requires_dlq()
        && dlq.is_none()
    {
        return Err(FaucetError::Config(
            "contract: on_breach 'quarantine' requires a DLQ sink".into(),
        ));
    }
    // One-shot warn guard for contract `on_breach: warn` breaches.
    #[cfg(feature = "contract")]
    let mut warned_contract_breach = false;

    // Masking policy (issue #206). Applied *first* per page — before
    // quality/contract/drift and every sink — so PII never leaks to a sink,
    // the DLQ, or a lineage sample. Never quarantines, so no DLQ gate.
    #[cfg(feature = "masking")]
    let masking = options.masking.clone();

    // ── Schema-drift policy + lazy destination-schema cache (#194) ───────────
    let schema_drift = options.schema_drift;
    // Fail fast: quarantine drift requires a DLQ (mirrors the quality guard).
    if let Some(p) = schema_drift.as_ref()
        && p.requires_dlq()
        && dlq.is_none()
    {
        return Err(FaucetError::Config(
            "schema: on_drift 'quarantine' (or on_incompatible 'quarantine') requires a DLQ sink"
                .into(),
        ));
    }
    // Destination schema cache: fetched lazily once, refreshed after evolve.
    // The inner `None` means "fetched, sink is schemaless"; the outer `None`
    // tracks "not yet fetched".
    let mut dest_schema_cache: Option<Option<Value>> = None;
    let mut warned_drift_inert = false;

    if let Some(key) = state_key.as_ref() {
        validate_state_key(key)?;
    }

    // ── Effectively-once mechanism selection + gates ─────────────────────────
    // `delivery: exactly_once` requests ≥ effectively-once; derive which
    // mechanism this topology actually provides (issue #292):
    //   1. atomic watermark — idempotent sink + positional-replay source
    //      (`replay` unknown = trusted, for direct `run_stream` callers);
    //   2. keyed upsert — the sink is configured to dedup by key, any source;
    //   3. neither → typed error naming the limiting side.
    let mechanism: Option<crate::idempotency::EffectivelyOnceMechanism> =
        if options.delivery == crate::idempotency::DeliveryMode::ExactlyOnce {
            let replay_ok = options
                .replay
                .is_none_or(|r| r == crate::idempotency::ReplayGuarantee::Deterministic);
            if sink.supports_idempotent_writes() && replay_ok {
                if state_store.is_none() || state_key.is_none() {
                    return Err(FaucetError::Config(
                        "delivery: exactly_once (atomic watermark) requires a state store".into(),
                    ));
                }
                if dlq.is_some() {
                    return Err(FaucetError::Config(
                        "delivery: exactly_once (atomic watermark) is not compatible with a DLQ \
                         in this version"
                            .into(),
                    ));
                }
                Some(crate::idempotency::EffectivelyOnceMechanism::AtomicWatermark)
            } else if sink.dedups_by_key() {
                Some(crate::idempotency::EffectivelyOnceMechanism::KeyedUpsert)
            } else if sink.supports_idempotent_writes() {
                // Atomic-capable sink, but the source does not replay
                // positionally and no keyed dedup is configured.
                return Err(FaucetError::Config(format!(
                    "delivery: exactly_once — the source does not replay deterministically from \
                     a bookmark, so the atomic-watermark mechanism cannot be used; configure \
                     `write_mode: upsert` with a `key` on sink '{}' for keyed-upsert \
                     effectively-once instead",
                    sink.connector_name()
                )));
            } else {
                return Err(FaucetError::Config(format!(
                    "delivery: exactly_once requires an idempotent (atomic-watermark) sink or a \
                     sink configured to dedup by key (`write_mode: upsert` + `key`), but '{}' \
                     provides neither",
                    sink.connector_name()
                )));
            }
        } else {
            None
        };
    // Only the atomic-watermark mechanism changes the write/skip/state path
    // below; keyed upsert delivers its idempotence inside the sink's own
    // keyed writes, over the ordinary write path.
    let exactly_once =
        mechanism == Some(crate::idempotency::EffectivelyOnceMechanism::AtomicWatermark);
    let scope = state_key.clone().unwrap_or_default();
    let mut next_seq = options.start_seq;
    let committed_seq = if exactly_once {
        sink.last_committed_token(&scope)
            .await?
            .and_then(|t| crate::idempotency::parse_token(&t))
            .unwrap_or(0)
    } else {
        0
    };

    let mut records_written = 0usize;
    let mut last_bookmark: Option<Value> = None;
    let mut dlq_stats = DlqStats::default();

    let adaptive_cfg = options.adaptive.clone().filter(|c| c.enabled);
    // Validate at the core boundary so library callers of `run_stream` (not
    // just the CLI, which validates earlier) reject an invalid adaptive config
    // — e.g. the rejected `respect_source_max=false` knob — up front.
    if let Some(cfg) = adaptive_cfg.as_ref() {
        cfg.validate()?;
    }
    let mut controller: Option<crate::adaptive::AimdController> = None;
    let mut warned_noop_sink = false;
    // One-shot warn guard for poison-pill `Drop` action (DLQ path).
    let mut warned_poison_drop = false;

    let sink_name = sink.connector_name();
    let dlq_sink_name = dlq.as_ref().map(|d| d.sink.connector_name()).unwrap_or("");

    // Drive the streaming loop inside an inner future so that EVERY early exit
    // (a source error, a `?`-propagated write/flush/state failure, or a DLQ
    // budget overflow) funnels through one place. On any error we best-effort
    // flush the sinks before propagating, so a buffered sink that only commits
    // on flush — Parquet writes its footer there; without it the whole file is
    // unreadable — does not lose everything written so far (#78/#3).
    // Set when the loop exits because the cancellation token fired (vs. the
    // stream ending naturally). Either way we fall through to the success-path
    // flush below, so a buffered sink (Parquet footer, S3 multipart) is made
    // durable — the difference from a dropped future, which flushes nothing.
    let mut cancelled = false;

    // ── Resilience policy (retry/backoff/circuit-breaker) ────────────────────
    // When no policy is attached, `retry_policy` is `None` and the `with_retry!`
    // macro falls through to a bare `$op.await`, leaving the write path
    // byte-for-byte identical to today. The breaker is bound for later tasks
    // (DLQ-path circuit breaking) and is unused by the default/exactly-once
    // paths wrapped here.
    let resilience = options.resilience.clone();
    let retry_policy = resilience.as_ref().map(|r| r.retry.clone());
    let mut breaker = resilience
        .as_ref()
        .and_then(|r| r.circuit_breaker)
        .map(|cb| {
            (
                crate::resilience::CircuitBreaker::new(cb.consecutive_failures),
                cb.cooldown,
            )
        });
    // Poison-pill (per-row) policy, applied in the DLQ path only.
    let poison = resilience.as_ref().and_then(|r| r.poison);

    // Run a sink/state op under the retry policy, or bare if no policy is set.
    // A macro (not a closure) so it works across the differently-typed call
    // sites (`Result<usize, _>`, `Result<(), _>`) without boxing. `cancel` is
    // the `Option<CancellationToken>` already in scope; a cancel during a
    // backoff sleep returns the last error promptly so the caller can flush.
    //
    // Each call site tags its `op` (`"sink_write"` / `"flush"` / `"state_put"`)
    // so the resilience metrics (`faucet_resilience_retries_total{op,class}`,
    // `_retry_sleep_seconds{op}`, `_giveup_total{op}`) get the spec's labels via
    // the metered runner. The `RetryMetrics` (which clones the pipeline/row
    // strings) is built only when a policy is attached, so the no-policy path
    // stays allocation-free and byte-for-byte identical to today.
    macro_rules! with_retry {
        ($op_label:literal, $op:expr) => {
            match &retry_policy {
                Some(p) => {
                    let m = crate::resilience::RetryMetrics {
                        pipeline: pipeline_name.to_string(),
                        row: row.to_string(),
                        op: $op_label,
                    };
                    crate::resilience::execute_with_policy_metered(p, cancel.as_ref(), &m, || $op)
                        .await
                }
                None => $op.await,
            }
        };
    }

    // Retry wrapper for the **non-idempotent** write paths (`write_batch` /
    // `write_batch_partial`). A bare `write_batch` makes no atomicity promise:
    // if the request commits server-side but the response is lost, a
    // pipeline-level retry silently duplicates every row — the repo's #1 worst
    // bug class (F29/F32). So we only apply the retry policy when the sink
    // commits writes idempotently (`supports_idempotent_writes()`); otherwise
    // we fall through to a bare `$op.await`, exactly as the pre-resilience code
    // did. The idempotent exactly-once path (`write_batch_idempotent`) keeps
    // using `with_retry!` — replaying a token-stamped write is a no-op, so it
    // is always safe to retry.
    macro_rules! with_retry_write {
        ($op_label:literal, $op:expr) => {
            if retry_policy.is_some() && sink.supports_idempotent_writes() {
                with_retry!($op_label, $op)
            } else {
                $op.await
            }
        };
    }

    let loop_result: Result<(), FaucetError> = async {
        loop {
            // Poll the next page, but if a cancellation token is wired, race it
            // so a cancel between pages stops the run promptly and cleanly
            // (#146 H16). `biased` checks cancellation first each iteration.
            let page = match &cancel {
                Some(token) => tokio::select! {
                    biased;
                    _ = token.cancelled() => {
                        cancelled = true;
                        break;
                    }
                    p = std::future::poll_fn(|cx| Pin::new(&mut pages).poll_next(cx)) => p,
                },
                None => std::future::poll_fn(|cx| Pin::new(&mut pages).poll_next(cx)).await,
            };
            match page {
                Some(Ok(page)) => {
                    if page.records.is_empty() && page.bookmark.is_none() {
                        continue;
                    }

                    // ── Masking pass (FIRST — before quality/contract/drift and
                    // every sink write) ─────────────────────────────────────
                    // Runs ahead of everything so PII never reaches a sink, the
                    // DLQ (quarantine envelopes are built downstream from these
                    // already-masked records), or the sink-side lineage sample.
                    #[cfg(feature = "masking")]
                    let page = if let Some(m) = masking.as_ref() {
                        let labels =
                            crate::observability::Labels::new(&*pipeline_name, &*row, &*run_id);
                        let outcome = crate::observability::instrumented_apply_masking(
                            page.records,
                            m,
                            &labels,
                        );
                        StreamPage {
                            records: outcome.records,
                            bookmark: page.bookmark,
                        }
                    } else {
                        page
                    };

                    // True page positions of the records currently flowing, kept
                    // in lockstep as quality/contract remove rows, so a later
                    // schema-drift quarantine annotates the envelope with the
                    // record's real page index — not a survivor-relative one
                    // (audit #321 L6). Quality's own quarantine already uses the
                    // true `page_index`; this carries the same truth to drift.
                    let page_len = page.records.len();

                    // ── Quality pass (after transforms, before sink) ─────────
                    #[cfg(feature = "quality")]
                    let (records, quality_envelopes, page_indices): (Vec<Value>, Vec<Value>, Vec<usize>) =
                        if let Some(q) = quality.as_ref() {
                            let labels =
                                crate::observability::Labels::new(&*pipeline_name, &*row, &*run_id);
                            let outcome = crate::observability::instrumented_apply_quality(
                                page.records,
                                q,
                                &labels,
                            )?;
                            let quarantined_idx: std::collections::HashSet<usize> =
                                outcome.quarantined.iter().map(|qr| qr.page_index).collect();
                            let envelopes: Vec<Value> = outcome
                                .quarantined
                                .iter()
                                .map(|qr| {
                                    let err = FaucetError::QualityFailure {
                                        check: qr.check.to_string(),
                                        message: qr.message.clone(),
                                    };
                                    // `record_index` is the position within the PAGE
                                    // (the frozen envelope contract), not the index in
                                    // the quarantine list (#146 R).
                                    build_envelope(
                                        &qr.record,
                                        &err,
                                        DlqReason::Quality,
                                        sink_name,
                                        &pipeline_name,
                                        &row,
                                        qr.page_index,
                                    )
                                })
                                .collect();
                            let survivor_idx: Vec<usize> =
                                (0..page_len).filter(|i| !quarantined_idx.contains(i)).collect();
                            (outcome.survivors, envelopes, survivor_idx)
                        } else {
                            (page.records, Vec::new(), (0..page_len).collect())
                        };
                    #[cfg(not(feature = "quality"))]
                    let (records, quality_envelopes, page_indices): (Vec<Value>, Vec<Value>, Vec<usize>) =
                        (page.records, Vec::new(), (0..page_len).collect());

                    // ── Contract pass (after quality, before schema drift) ───
                    // `fail` mirrors a quality `abort`: the breach error
                    // propagates immediately and nothing from this page is
                    // written — a contract must never commit breaching data
                    // (unlike drift `fail`, which defers because its records
                    // are individually fine).
                    #[cfg(feature = "contract")]
                    let (records, contract_envelopes, page_indices): (Vec<Value>, Vec<Value>, Vec<usize>) =
                        if let Some(c) = contract.as_ref() {
                            let labels =
                                crate::observability::Labels::new(&*pipeline_name, &*row, &*run_id);
                            let outcome = crate::observability::instrumented_apply_contract(
                                records, c, &labels,
                            )?;
                            if !outcome.warned.is_empty() && !warned_contract_breach {
                                tracing::warn!(
                                    version = %c.version,
                                    breaches = outcome.warned.len(),
                                    first = %outcome.warned[0].describe(),
                                    "contract: breaching records written unchanged \
                                     (on_breach=warn); this warning fires once per run"
                                );
                                warned_contract_breach = true;
                            }
                            let envelopes: Vec<Value> = outcome
                                .quarantined
                                .iter()
                                .map(|vr| {
                                    let err = FaucetError::ContractViolation {
                                        version: c.version.clone(),
                                        message: vr.violation.describe(),
                                    };
                                    // `record_index` must be the position within the
                                    // original PAGE (the frozen envelope contract).
                                    // `vr.violation.page_index` is the position within
                                    // *this pass's input* — the quality survivors —
                                    // which is aligned with `page_indices`, so translate
                                    // through it. Without this, an earlier quality
                                    // quarantine on the same page shifts every contract
                                    // envelope's index by the count of quality-removed
                                    // rows (#466 M1).
                                    let page_index = page_indices
                                        .get(vr.violation.page_index)
                                        .copied()
                                        .unwrap_or(vr.violation.page_index);
                                    build_envelope(
                                        &vr.record,
                                        &err,
                                        DlqReason::Contract,
                                        sink_name,
                                        &pipeline_name,
                                        &row,
                                        page_index,
                                    )
                                })
                                .collect();
                            // Contract's `page_index` is the position within ITS
                            // input (the quality survivors) — aligned with the
                            // incoming `page_indices`. Drop those positions so the
                            // vector still maps each remaining record to its true
                            // original page index (#321 L6).
                            let contract_quarantined: std::collections::HashSet<usize> = outcome
                                .quarantined
                                .iter()
                                .map(|vr| vr.violation.page_index)
                                .collect();
                            let survivor_idx: Vec<usize> = page_indices
                                .iter()
                                .enumerate()
                                .filter(|(pos, _)| !contract_quarantined.contains(pos))
                                .map(|(_, orig)| *orig)
                                .collect();
                            (outcome.survivors, envelopes, survivor_idx)
                        } else {
                            (records, Vec::new(), page_indices)
                        };

                    // ── Schema-drift pass (after quality, before sink) ───────
                    let mut drift_envelopes: Vec<Value> = Vec::new();
                    let (records, drift_abort): (Vec<Value>, Option<FaucetError>) =
                        if let Some(policy) = schema_drift.as_ref().filter(|_| !records.is_empty()) {
                            // Lazily fetch + cache the destination schema.
                            if dest_schema_cache.is_none() {
                                dest_schema_cache = Some(sink.current_schema().await?);
                            }
                            let dest = dest_schema_cache.as_ref().and_then(|o| o.as_ref());
                            match dest {
                                None => {
                                    if !warned_drift_inert {
                                        tracing::info!(
                                            connector = sink_name,
                                            "schema-drift: sink reports no destination schema; \
                                             drift handling is inert this run"
                                        );
                                        warned_drift_inert = true;
                                    }
                                    (records, None)
                                }
                                Some(dest) => {
                                    let inferred = crate::schema::infer_schema(&records);
                                    let diff = crate::drift::diff_schema(
                                        dest,
                                        &inferred,
                                        policy.allow_widening,
                                    );
                                    if diff.is_empty() {
                                        (records, None)
                                    } else {
                                        // The cache may be replaced inside the evolve
                                        // arm; `dest` borrows it, so re-clone before the
                                        // call to drop the borrow.
                                        let dest_owned = dest.clone();
                                        apply_drift_policy(
                                            policy,
                                            &diff,
                                            &dest_owned,
                                            records,
                                            &page_indices,
                                            sink,
                                            sink_name,
                                            &pipeline_name,
                                            &row,
                                            &mut dest_schema_cache,
                                            &mut drift_envelopes,
                                        )
                                        .await?
                                    }
                                }
                            }
                        } else {
                            (records, None)
                        };
                    // Merge contract + drift quarantine envelopes into the
                    // quality envelopes so the existing DLQ path writes them
                    // together.
                    let quality_envelopes = {
                        let mut q = quality_envelopes;
                        #[cfg(feature = "contract")]
                        q.extend(contract_envelopes);
                        q.append(&mut drift_envelopes);
                        q
                    };
                    // A drift `fail` / incompatible-`fail` abort is *deferred* the
                    // same way the DLQ-budget and circuit-breaker aborts are: when a
                    // DLQ is configured this page may carry quality- or drift-
                    // quarantine envelopes that must still reach the DLQ before the
                    // run stops (dropping them on an early `return` would silently
                    // lose those rows — #146 M4). So with a DLQ we thread the error
                    // into the post-commit raise site below; with no DLQ there are
                    // no envelopes to strand (a no-DLQ quarantine config is rejected
                    // at run start), so we abort immediately and write nothing.
                    let mut drift_abort = drift_abort;
                    if dlq.is_none()
                        && let Some(e) = drift_abort.take()
                    {
                        return Err(e);
                    }

                    let page = StreamPage {
                        records,
                        bookmark: page.bookmark,
                    };

                    if let Some(ref dlq_cfg) = dlq {
                        // ── DLQ-enabled path ───────────────────────────────────
                        use metrics::{Label, SharedString, counter};
                        let metric_labels: Vec<Label> = vec![
                            Label::new("pipeline", SharedString::from(pipeline_name.clone())),
                            Label::new("row", SharedString::from(row.clone())),
                            Label::new("connector", SharedString::from(sink_name.to_string())),
                            Label::new(
                                "dlq_connector",
                                SharedString::from(dlq_sink_name.to_string()),
                            ),
                        ];
                        let span = tracing::info_span!(
                            "faucet.dlq.route",
                            pipeline = %pipeline_name,
                            row = %row,
                            run_id = %run_id,
                            connector = %sink_name,
                            dlq_connector = %dlq_sink_name,
                        );
                        let _enter = span.enter();

                        // Reslice the page into sub-batches driven by the
                        // adaptive controller (or write the whole page in one
                        // shot when adaptive is disabled — same as before).
                        let mut envelopes: Vec<Value> = Vec::new();
                        let mut page_success = 0usize;
                        let mut outer_err_recovered = false;
                        // True if any chunk reported genuine per-row sink `Err`s
                        // (as opposed to a chunk wholly synthesized from an outer
                        // error under `DlqAll`). Drives the `partial` label when a
                        // resliced page mixes the two failure modes.
                        let mut had_per_row_sink_failure = false;
                        let records_len = page.records.len();
                        let mut offset = 0usize;
                        while offset < records_len {
                            let size = match adaptive_cfg.as_ref() {
                                Some(cfg) => {
                                    let ctrl = controller.get_or_insert_with(|| {
                                        crate::adaptive::AimdController::new(cfg, records_len)
                                    });
                                    ctrl.current().max(1).min(records_len - offset)
                                }
                                None => records_len - offset, // whole page = today's behavior
                            };
                            if adaptive_cfg.is_some() {
                                maybe_warn_noop_sink(sink_name, &mut warned_noop_sink);
                            }
                            let chunk = &page.records[offset..offset + size];
                            let t0 = std::time::Instant::now();
                            // Wrap the partial write with the retry policy so a
                            // whole-batch transient `Err` (a 5xx / connection
                            // drop the sink reports at the outer level) is
                            // retried before the `on_batch_error` decision.
                            // Inert when no policy is attached.
                            let chunk_outcomes_result =
                                with_retry_write!("sink_write", sink.write_batch_partial(chunk));
                            let latency = t0.elapsed();
                            // `chunk_synthesized` is true only when this chunk's
                            // outcomes were fabricated from a single outer
                            // `write_batch_partial` error under `DlqAll` — as
                            // opposed to genuine per-row `Err`s the sink
                            // reported. Tracking it per chunk keeps the page
                            // `reason` label accurate when adaptive reslicing
                            // mixes a synthesized chunk with partial-failure
                            // chunks on the same page.
                            let (mut chunk_outcomes, chunk_synthesized): (
                                Vec<crate::RowOutcome>,
                                bool,
                            ) = match chunk_outcomes_result {
                                Ok(o) => (o, false),
                                Err(e) => match dlq_cfg.on_batch_error {
                                    OnBatchError::Propagate => return Err(e),
                                    OnBatchError::DlqAll => {
                                        outer_err_recovered = true;
                                        let msg = e.to_string();
                                        let synth = (0..chunk.len())
                                            .map(|_| Err(FaucetError::Sink(msg.clone())))
                                            .collect();
                                        (synth, true)
                                    }
                                },
                            };

                            // ── Poison-pill: retry the still-failing,
                            // retriable-row subset before enveloping. A row that
                            // succeeds on retry becomes a success; one that keeps
                            // failing falls through to the terminal `action`
                            // applied in the per-row loop below. Only genuine
                            // per-row failures are retried (not a synthesized
                            // `DlqAll` chunk — there is no per-row sink to retry
                            // against). Inert when `poison` is `None`.
                            if let Some(pp) = poison
                                && !chunk_synthesized
                            {
                                let mut attempt = 1u32; // first attempt already done
                                while attempt < pp.max_row_attempts {
                                    let failing: Vec<usize> = chunk_outcomes
                                        .iter()
                                        .enumerate()
                                        .filter_map(|(j, o)| match o {
                                            Err(e)
                                                if retry_policy
                                                    .as_ref()
                                                    .map(|p| p.is_retriable(e))
                                                    .unwrap_or(false) =>
                                            {
                                                Some(j)
                                            }
                                            _ => None,
                                        })
                                        .collect();
                                    if failing.is_empty() {
                                        break;
                                    }
                                    let subset: Vec<Value> =
                                        failing.iter().map(|&j| chunk[j].clone()).collect();
                                    // Bare resubmit — NOT through `with_retry_write!`.
                                    // The poison loop's `max_row_attempts` is the
                                    // sole bound on per-row resubmission; nesting the
                                    // resilience retry here would multiply submissions
                                    // to a non-idempotent partial sink up to
                                    // `(max_row_attempts - 1) * max_attempts`,
                                    // amplifying duplicate writes (F47).
                                    let retried = sink.write_batch_partial(&subset).await?;
                                    // `retried` aligns positionally with `failing`
                                    // (the subset was built in `failing` order).
                                    // Consume by value — `FaucetError` is not Clone.
                                    let mut retried = retried.into_iter();
                                    for &j in failing.iter() {
                                        chunk_outcomes[j] = retried.next().unwrap_or(Ok(()));
                                    }
                                    attempt += 1;
                                }
                            }

                            let mut chunk_errors = 0usize;
                            // Per-action poison counts for this chunk. Emitted to
                            // `faucet_resilience_poison_rows_total` only when a
                            // `poison` policy is configured — the default `Dlq`
                            // fallback (no policy) is ordinary DLQ traffic and must
                            // not inflate the poison metric.
                            let mut poison_dlq = 0u64;
                            let mut poison_drop = 0u64;
                            for (j, outcome) in chunk_outcomes.iter().enumerate() {
                                match outcome {
                                    Ok(()) => page_success += 1,
                                    Err(err) => {
                                        // Terminal poison action for a row that
                                        // remained failing after retries. With no
                                        // poison policy this is always the default
                                        // `Dlq` behavior (envelope).
                                        let action = poison
                                            .map(|pp| pp.action)
                                            .unwrap_or(crate::resilience::PoisonAction::Dlq);
                                        match action {
                                            crate::resilience::PoisonAction::Fail => {
                                                crate::observability::resilience::poison_rows(
                                                    &pipeline_name,
                                                    &row,
                                                    "fail",
                                                    1,
                                                );
                                                return Err(FaucetError::Sink(format!(
                                                    "poison-pill row failed permanently: {err}"
                                                )));
                                            }
                                            crate::resilience::PoisonAction::Drop => {
                                                // Count + one-shot warn, discard the
                                                // row (no envelope).
                                                poison_drop += 1;
                                                if !warned_poison_drop {
                                                    tracing::warn!(
                                                        "poison-pill: dropping permanently-failing row(s) (action=drop); this warning fires once per run"
                                                    );
                                                    warned_poison_drop = true;
                                                }
                                            }
                                            crate::resilience::PoisonAction::Dlq => {
                                                poison_dlq += 1;
                                                chunk_errors += 1;
                                                if !chunk_synthesized {
                                                    had_per_row_sink_failure = true;
                                                }
                                                envelopes.push(build_envelope(
                                                    &chunk[j],
                                                    err,
                                                    DlqReason::Partial,
                                                    sink_name,
                                                    &pipeline_name,
                                                    &row,
                                                    offset + j,
                                                ));
                                            }
                                        }
                                    }
                                }
                            }
                            // Only attribute these to the poison metric when the
                            // policy is active (otherwise `Dlq` rows are plain DLQ
                            // traffic, already counted elsewhere).
                            if poison.is_some() {
                                crate::observability::resilience::poison_rows(
                                    &pipeline_name,
                                    &row,
                                    "dlq",
                                    poison_dlq,
                                );
                                crate::observability::resilience::poison_rows(
                                    &pipeline_name,
                                    &row,
                                    "drop",
                                    poison_drop,
                                );
                            }
                            if let Some(ctrl) = controller.as_mut() {
                                let adj = ctrl.observe(crate::adaptive::Observation {
                                    batch_len: chunk.len(),
                                    errors: chunk_errors,
                                    latency,
                                });
                                emit_adaptive_metrics(ctrl, adj, &pipeline_name, &row);
                            }
                            offset += size;
                        }
                        // Quality-quarantined records share the DLQ budget/write.
                        // Capture the quality count BEFORE the splice — the splice
                        // moves `quality_envelopes`, so its length is unavailable
                        // afterward. Used below to pick the page `reason` label.
                        #[cfg(feature = "quality")]
                        let quality_count = quality_envelopes.len();
                        #[cfg(not(feature = "quality"))]
                        let quality_count = 0usize;
                        envelopes.splice(0..0, quality_envelopes);
                        let page_failures = envelopes.len();

                        // Budget checks. `write_batch_partial` above already
                        // committed this page's survivors to the main sink, so
                        // we must NOT abort here: returning now would strand
                        // those committed survivors without advancing the
                        // bookmark (they would re-deliver on the next run) and
                        // drop this page's failures before they reach the DLQ
                        // (#146 M4). Instead, record the budget error, finish
                        // committing the page below (route failures to the DLQ,
                        // flush, persist the bookmark), and abort only once the
                        // page is fully durable. The failed rows that crossed
                        // the threshold are still written to the DLQ — losing
                        // them would be strictly worse than the small overshoot.
                        let mut budget_error: Option<FaucetError> = None;
                        // Circuit-breaker accounting: a page counts as a failure
                        // for the breaker when it was non-empty and nothing
                        // succeeded (everything went to the DLQ / dropped). Any
                        // success resets the consecutive counter. When the breaker
                        // opens, defer the abort to the same site as `budget_error`
                        // so the page's failures still reach the DLQ and the
                        // bookmark advances before the run stops. Inert when no
                        // breaker is configured.
                        let mut circuit_error: Option<FaucetError> = None;
                        if let Some((b, cooldown)) = breaker.as_mut() {
                            if records_len > 0 && page_success == 0 {
                                if b.record_failure() {
                                    crate::observability::resilience::circuit_opened(
                                        &pipeline_name,
                                        &row,
                                    );
                                    circuit_error = Some(FaucetError::CircuitOpen {
                                        failures: b.consecutive(),
                                        cooldown: *cooldown,
                                    });
                                }
                            } else if page_success > 0 {
                                b.record_success();
                            }
                        }
                        if let Some(limit) = dlq_cfg.max_failures_per_page
                            && page_failures > limit
                        {
                            let mut lbl = metric_labels.clone();
                            lbl.retain(|l| l.key() != "dlq_connector");
                            lbl.push(Label::new("scope", SharedString::const_str("per_page")));
                            counter!("faucet_sink_dlq_budget_exceeded_total", lbl).increment(1);
                            budget_error = Some(FaucetError::Sink(format!(
                                "DLQ per-page budget exceeded: {page_failures} > {limit}"
                            )));
                        }
                        let new_total = dlq_stats.records_dlq + page_failures;
                        if budget_error.is_none()
                            && let Some(limit) = dlq_cfg.max_failures_total
                            && new_total > limit
                        {
                            let mut lbl = metric_labels.clone();
                            lbl.retain(|l| l.key() != "dlq_connector");
                            lbl.push(Label::new("scope", SharedString::const_str("total")));
                            counter!("faucet_sink_dlq_budget_exceeded_total", lbl).increment(1);
                            budget_error = Some(FaucetError::Sink(format!(
                                "DLQ total budget exceeded: {new_total} > {limit}"
                            )));
                        }

                        // Write to DLQ sink. Errors here are fatal, no recursion.
                        if !envelopes.is_empty() {
                            let _dlq_write_timer = crate::observability::DurationGuard::new(
                                "faucet_sink_dlq_write_duration_seconds",
                                metric_labels.clone(),
                            );
                            dlq_cfg.sink.write_batch(&envelopes).await.map_err(|e| {
                                let mut lbl = metric_labels.clone();
                                lbl.push(Label::new(
                                    "kind",
                                    SharedString::const_str(
                                        crate::observability::decorator::error_kind(&e),
                                    ),
                                ));
                                counter!("faucet_sink_dlq_errors_total", lbl).increment(1);
                                FaucetError::Sink(format!("DLQ sink write failed: {e}"))
                            })?;
                            dlq_stats.records_dlq += page_failures;
                            dlq_stats.pages_with_failures += 1;

                            // Page `reason` label, 3-way (precedence: partial > dlq_all > quality):
                            //  - `partial`  — at least one chunk reported genuine
                            //    per-row sink `Err`s. Checked FIRST so a resliced
                            //    page that mixes a synthesized chunk (DlqAll) with
                            //    partial-failure chunks is labeled `partial` — the
                            //    real per-row failure dominates. (For a
                            //    non-resliced page this is equivalent to the old
                            //    `page_failures > quality_count` test, since a
                            //    single chunk is either all-synthesized or all
                            //    per-row.)
                            //  - `dlq_all`  — every sink-side failure on the page
                            //    was synthesized from an outer `write_batch_partial`
                            //    error (OnBatchError::DlqAll); no genuine per-row
                            //    failures occurred.
                            //  - `quality`  — every envelope is quality-sourced
                            //    (no sink-side failures on this page).
                            // The per-row quality volume is separately exposed via
                            // `faucet_quality_records_quarantined_total`.
                            let reason_label = if had_per_row_sink_failure {
                                DlqReason::Partial.as_str()
                            } else if outer_err_recovered {
                                DlqReason::DlqAll.as_str()
                            } else if page_failures > quality_count {
                                DlqReason::Partial.as_str()
                            } else {
                                DlqReason::Quality.as_str()
                            };
                            counter!("faucet_sink_dlq_records_total", metric_labels.clone())
                                .increment(page_failures as u64);
                            let mut page_labels = metric_labels.clone();
                            page_labels
                                .push(Label::new("reason", SharedString::const_str(reason_label)));
                            counter!("faucet_sink_dlq_pages_total", page_labels).increment(1);
                        }

                        records_written += page_success;

                        if let Some(bookmark) = page.bookmark {
                            // Retry-wrap the main-sink flush, the DLQ-sink flush,
                            // and the state write so a transient failure on any of
                            // them is retried before aborting — same as the default
                            // and exactly-once paths. Inert when no policy is set
                            // (the macro's `None` arm is a bare `.await`).
                            with_retry!("flush", sink.flush())?;
                            let _dlq_flush_timer = crate::observability::DurationGuard::new(
                                "faucet_sink_dlq_flush_duration_seconds",
                                metric_labels.clone(),
                            );
                            with_retry!("flush", dlq_cfg.sink.flush()).map_err(|e| {
                                let mut lbl = metric_labels.clone();
                                lbl.push(Label::new(
                                    "kind",
                                    SharedString::const_str(
                                        crate::observability::decorator::error_kind(&e),
                                    ),
                                ));
                                counter!("faucet_sink_dlq_errors_total", lbl).increment(1);
                                FaucetError::Sink(format!("DLQ sink flush failed: {e}"))
                            })?;
                            let bm_labels =
                                crate::observability::Labels::new(&*pipeline_name, &*row, &*run_id);
                            crate::observability::update_bookmark_lag(&bookmark, &bm_labels);
                            if let (Some(store), Some(key)) =
                                (state_store.as_ref(), state_key.as_ref())
                            {
                                with_retry!("state_put", store.put(key, &bookmark))?;
                            }
                            last_bookmark = Some(bookmark);
                        }

                        // The page is now durable — survivors committed to the
                        // main sink, failures routed to the DLQ, and (if the
                        // page carried one) the bookmark persisted. Honor a
                        // deferred DLQ-budget abort now, so the run still stops
                        // as a circuit breaker but never re-delivers this
                        // already-committed page (#146 M4).
                        if let Some(e) = budget_error {
                            return Err(e);
                        }
                        // Circuit breaker opened after the page was made durable.
                        if let Some(e) = circuit_error {
                            return Err(e);
                        }
                        // Deferred schema-drift `fail` abort: this page's survivors
                        // are committed and its quality/drift quarantine envelopes
                        // are now in the DLQ, so the run stops without stranding
                        // them (mirrors the budget/circuit deferral above).
                        if let Some(e) = drift_abort {
                            return Err(e);
                        }
                    } else if exactly_once {
                        // ── Exactly-once path ──────────────────────────────────
                        // A token is issued only for bookmark-carrying pages, so
                        // (seq, bookmark) advance together and realign on resume.
                        if let Some(bookmark) = page.bookmark {
                            next_seq += 1;
                            // Embed the page's resume bookmark in the token so
                            // the committed watermark doubles as a durable
                            // record of the stream position — on resume the
                            // pipeline re-anchors the source there instead of
                            // relying on identical replayed page boundaries
                            // (see `Pipeline::run`).
                            let token = crate::idempotency::format_token_with_bookmark(
                                next_seq,
                                Some(&bookmark),
                            );
                            if next_seq <= committed_seq {
                                // Sink already durably committed this page. Skip
                                // the write; advance state so a later crash does
                                // not re-skip it.
                                use metrics::{Label, SharedString, counter};
                                let skip_labels: Vec<Label> = vec![
                                    Label::new(
                                        "pipeline",
                                        SharedString::from(pipeline_name.clone()),
                                    ),
                                    Label::new("row", SharedString::from(row.clone())),
                                ];
                                counter!("faucet_pipeline_pages_skipped_total", skip_labels)
                                    .increment(1);
                            } else {
                                records_written += with_retry!(
                                    "sink_write",
                                    sink.write_batch_idempotent(&page.records, &scope, &token)
                                )?;
                            }
                            with_retry!("flush", sink.flush())?;
                            let bm_labels =
                                crate::observability::Labels::new(&*pipeline_name, &*row, &*run_id);
                            crate::observability::update_bookmark_lag(&bookmark, &bm_labels);
                            if let (Some(store), Some(key)) =
                                (state_store.as_ref(), state_key.as_ref())
                            {
                                let wrapped =
                                    crate::idempotency::wrap_state(Some(&bookmark), next_seq);
                                with_retry!("state_put", store.put(key, &wrapped))?;
                            }
                            last_bookmark = Some(bookmark);
                        } else if !page.records.is_empty() {
                            // No bookmark → not individually checkpointed; write
                            // as-is (rare for EO sources, which bookmark every
                            // page). Stays at-least-once for this page.
                            records_written +=
                                with_retry_write!("sink_write", sink.write_batch(&page.records))?;
                        }
                    } else {
                        // ── DLQ-disabled path (today's behaviour) ──────────────
                        debug_assert!(
                            quality_envelopes.is_empty(),
                            "quality quarantine without DLQ should have been rejected at run start"
                        );
                        if !page.records.is_empty() {
                            if let Some(cfg) = adaptive_cfg.as_ref() {
                                let ctrl = controller.get_or_insert_with(|| {
                                    crate::adaptive::AimdController::new(cfg, page.records.len())
                                });
                                maybe_warn_noop_sink(sink_name, &mut warned_noop_sink);
                                let mut offset = 0;
                                while offset < page.records.len() {
                                    let size =
                                        ctrl.current().max(1).min(page.records.len() - offset);
                                    let chunk = &page.records[offset..offset + size];
                                    let t0 = std::time::Instant::now();
                                    let n = with_retry_write!("sink_write", sink.write_batch(chunk))?;
                                    let latency = t0.elapsed();
                                    records_written += n;
                                    offset += size;
                                    let adj = ctrl.observe(crate::adaptive::Observation {
                                        batch_len: chunk.len(),
                                        errors: 0,
                                        latency,
                                    });
                                    emit_adaptive_metrics(ctrl, adj, &pipeline_name, &row);
                                }
                            } else {
                                records_written +=
                                    with_retry_write!("sink_write", sink.write_batch(&page.records))?;
                            }
                        }
                        if let Some(bookmark) = page.bookmark {
                            with_retry!("flush", sink.flush())?;
                            let bm_labels =
                                crate::observability::Labels::new(&*pipeline_name, &*row, &*run_id);
                            crate::observability::update_bookmark_lag(&bookmark, &bm_labels);
                            if let (Some(store), Some(key)) =
                                (state_store.as_ref(), state_key.as_ref())
                            {
                                with_retry!("state_put", store.put(key, &bookmark))?;
                            }
                            last_bookmark = Some(bookmark);
                        }
                    }
                }
                Some(Err(e)) => return Err(e),
                None => break,
            }
        }
        Ok(())
    }
    .await;

    // Error/early-return unwind: best-effort flush so any buffered output is
    // made durable, then propagate the ORIGINAL error. Flush errors here are
    // logged and swallowed — the source/sink error that triggered the unwind
    // is the meaningful one to surface. DLQ is flushed first (mirroring the
    // success path below): its records are only ever written here, whereas the
    // next run re-reads post-bookmark records from the source.
    if let Err(e) = loop_result {
        if let Some(ref dlq_cfg) = dlq
            && let Err(flush_err) = dlq_cfg.sink.flush().await
        {
            tracing::warn!(
                error = %flush_err,
                "DLQ sink flush failed during error unwind; original error preserved"
            );
        }
        if let Err(flush_err) = sink.flush().await {
            tracing::warn!(
                error = %flush_err,
                "sink flush failed during error unwind; original error preserved"
            );
        }
        return Err(e);
    }

    // Flush the DLQ sink BEFORE the main sink so quarantined records are made
    // durable even if the main sink's final flush fails. The next run will
    // re-read post-bookmark records from the source and re-route any that
    // would have fallen out of the main sink's unflushed buffer; DLQ records,
    // by contrast, are only ever written here and would otherwise be lost.
    if let Some(ref dlq_cfg) = dlq {
        let final_metric_labels: Vec<metrics::Label> = vec![
            metrics::Label::new(
                "pipeline",
                metrics::SharedString::from(pipeline_name.clone()),
            ),
            metrics::Label::new("row", metrics::SharedString::from(row.clone())),
            metrics::Label::new(
                "connector",
                metrics::SharedString::from(sink_name.to_string()),
            ),
            metrics::Label::new(
                "dlq_connector",
                metrics::SharedString::from(dlq_sink_name.to_string()),
            ),
        ];
        let _final_dlq_flush_timer = crate::observability::DurationGuard::new(
            "faucet_sink_dlq_flush_duration_seconds",
            final_metric_labels.clone(),
        );
        dlq_cfg.sink.flush().await.map_err(|e| {
            let mut lbl = final_metric_labels.clone();
            lbl.push(metrics::Label::new(
                "kind",
                metrics::SharedString::const_str(crate::observability::decorator::error_kind(&e)),
            ));
            metrics::counter!("faucet_sink_dlq_errors_total", lbl).increment(1);
            FaucetError::Sink(format!("DLQ sink flush failed: {e}"))
        })?;
    }
    sink.flush().await?;

    if cancelled {
        tracing::info!(
            records_written,
            "pipeline run cancelled cooperatively; sink flushed (partial output is durable)"
        );
    }

    tracing::info!(
        records_written,
        cancelled,
        has_bookmark = last_bookmark.is_some(),
        persisted = state_store.is_some() && state_key.is_some() && last_bookmark.is_some(),
        dlq_records = dlq_stats.records_dlq,
        "pipeline streaming run complete"
    );

    Ok(PipelineResult {
        records_written,
        bookmark: last_bookmark,
        dlq: dlq.is_some().then_some(dlq_stats),
    })
}

/// Emit the adaptive controller's current state + any adjustment as metrics.
/// Labels are `pipeline,row` only (the controller is pipeline-scoped).
fn emit_adaptive_metrics(
    ctrl: &crate::adaptive::AimdController,
    adj: Option<crate::adaptive::Adjustment>,
    pipeline: &str,
    row: &str,
) {
    use metrics::{Label, SharedString, counter, gauge};
    let base = vec![
        Label::new("pipeline", SharedString::from(pipeline.to_string())),
        Label::new("row", SharedString::from(row.to_string())),
    ];
    gauge!("faucet_pipeline_adaptive_batch_size", base.clone()).set(ctrl.current() as f64);
    gauge!(
        "faucet_pipeline_adaptive_batch_cooldown_active",
        base.clone()
    )
    .set(if ctrl.cooldown_active() { 1.0 } else { 0.0 });
    if let Some(p50) = ctrl.p50_latency_ms() {
        gauge!(
            "faucet_pipeline_adaptive_batch_p50_latency_ms",
            base.clone()
        )
        .set(p50 as f64);
    }
    if let Some(a) = adj {
        let mut lbl = base;
        lbl.push(Label::new(
            "direction",
            SharedString::const_str(a.direction.as_str()),
        ));
        lbl.push(Label::new(
            "reason",
            SharedString::const_str(a.reason.as_str()),
        ));
        counter!("faucet_pipeline_adaptive_batch_adjustments_total", lbl).increment(1);
    }
}

/// One-shot info when adaptive sizing targets a per-record sink that ignores
/// `batch_size` (its adjustments are harmless no-ops).
fn maybe_warn_noop_sink(sink_name: &str, warned: &mut bool) {
    if !*warned && matches!(sink_name, "jsonl" | "csv" | "stdout") {
        tracing::info!(
            sink = sink_name,
            "adaptive batch sizing is a no-op for this per-record sink"
        );
        *warned = true;
    }
}

/// Apply the schema-drift policy to a page (#194). Returns the (possibly
/// trimmed) records and an optional deferred abort error. The caller raises the
/// error after this page is durable: with a DLQ it is threaded into the same
/// post-commit raise site as the budget/circuit aborts (so the page's
/// quality/drift quarantine envelopes reach the DLQ first); with no DLQ — where
/// no envelopes can exist — it is raised immediately and the page is not written.
/// Appends drift quarantine envelopes to `drift_envelopes`.
#[allow(clippy::too_many_arguments)]
async fn apply_drift_policy<Si: Sink + ?Sized>(
    policy: &crate::drift::SchemaDriftPolicy,
    diff: &crate::drift::SchemaDiff,
    dest: &Value,
    records: Vec<Value>,
    page_indices: &[usize],
    sink: &Si,
    sink_name: &str,
    pipeline_name: &str,
    row: &str,
    dest_schema_cache: &mut Option<Option<Value>>,
    drift_envelopes: &mut Vec<Value>,
) -> Result<(Vec<Value>, Option<FaucetError>), FaucetError> {
    use crate::drift::{OnDrift, OnIncompatible};
    use crate::observability::schema_drift as emit_drift;

    let mode = match policy.on_drift {
        OnDrift::Warn => "warn",
        OnDrift::Ignore => "ignore",
        OnDrift::Quarantine => "quarantine",
        OnDrift::Fail => "fail",
        OnDrift::Evolve => "evolve",
    };
    emit_drift(
        pipeline_name,
        row,
        sink_name,
        mode,
        "added",
        diff.additions.len() as u64,
    );
    emit_drift(
        pipeline_name,
        row,
        sink_name,
        mode,
        "widened",
        diff.widenings.len() as u64,
    );
    emit_drift(
        pipeline_name,
        row,
        sink_name,
        mode,
        "narrowed",
        diff.incompatible.len() as u64,
    );
    emit_drift(
        pipeline_name,
        row,
        sink_name,
        mode,
        "dropped",
        diff.droppable_required.len() as u64,
    );

    match policy.on_drift {
        OnDrift::Warn => {
            tracing::warn!(
                connector = sink_name,
                columns = ?diff.changed_columns(),
                "schema-drift detected (on_drift=warn); writing page unchanged"
            );
            Ok((records, None))
        }
        OnDrift::Fail => Ok((
            records,
            Some(FaucetError::SchemaDrift {
                columns: diff.changed_columns(),
                message: "schema drift detected (on_drift=fail)".to_string(),
            }),
        )),
        OnDrift::Ignore => {
            // Drop fields not present in the destination schema.
            let allowed: std::collections::HashSet<String> = dest
                .get("properties")
                .and_then(|p| p.as_object())
                .map(|m| m.keys().cloned().collect())
                .unwrap_or_default();
            let trimmed = records
                .into_iter()
                .map(|r| match r {
                    Value::Object(map) => Value::Object(
                        map.into_iter()
                            .filter(|(k, _)| allowed.contains(k))
                            .collect(),
                    ),
                    other => other,
                })
                .collect();
            Ok((trimmed, None))
        }
        OnDrift::Quarantine => {
            let (kept, env) =
                quarantine_drift_rows(diff, records, page_indices, sink_name, pipeline_name, row);
            drift_envelopes.extend(env);
            Ok((kept, None))
        }
        OnDrift::Evolve => {
            let evolution = crate::drift::SchemaEvolution {
                additions: diff.additions.clone(),
                widenings: diff
                    .widenings
                    .iter()
                    .filter(|c| {
                        c.from
                            .as_ref()
                            .map(|f| crate::drift::base_widened(f, &c.to))
                            .unwrap_or(false)
                    })
                    .cloned()
                    .collect(),
                relax_nullability: diff
                    .droppable_required
                    .iter()
                    // A column merely *absent* from this page only relaxes its
                    // NOT NULL constraint when explicitly opted in — otherwise a
                    // transient/partial page would silently and irreversibly
                    // weaken the destination schema (F28).
                    .filter(|_| policy.relax_nullability_on_missing)
                    .cloned()
                    .chain(
                        diff.widenings
                            .iter()
                            .filter(|c| {
                                c.from
                                    .as_ref()
                                    .map(|f| crate::drift::adds_null(f, &c.to))
                                    .unwrap_or(false)
                            })
                            .map(|c| c.name.clone()),
                    )
                    .collect(),
            };
            if !evolution.is_empty() {
                sink.evolve_schema(&evolution).await?;
                // Refresh the cached destination schema so later pages diff
                // against the evolved shape (re-introspect authoritatively).
                *dest_schema_cache = Some(sink.current_schema().await?);
            }
            // Handle the incompatible residue.
            if diff.incompatible.is_empty() {
                Ok((records, None))
            } else {
                match policy.on_incompatible {
                    OnIncompatible::Fail => Ok((
                        records,
                        Some(FaucetError::SchemaDrift {
                            columns: diff.incompatible.iter().map(|c| c.name.clone()).collect(),
                            message: "incompatible type change cannot be auto-evolved \
                                      (on_incompatible=fail)"
                                .into(),
                        }),
                    )),
                    OnIncompatible::Quarantine => {
                        // Build a diff carrying only the incompatible columns.
                        let incompat_only = crate::drift::SchemaDiff {
                            incompatible: diff.incompatible.clone(),
                            ..Default::default()
                        };
                        let (kept, env) = quarantine_drift_rows(
                            &incompat_only,
                            records,
                            page_indices,
                            sink_name,
                            pipeline_name,
                            row,
                        );
                        drift_envelopes.extend(env);
                        Ok((kept, None))
                    }
                }
            }
        }
    }
}

/// Partition records: those exhibiting any drift column go to the DLQ; the rest
/// are kept. Returns `(kept, envelopes)`.
///
/// A row "exhibits drift" if it either **contains** a column whose shape diverges
/// from the destination — an addition, a type widening, or an incompatible type
/// change — or **omits** a `droppable_required` column (a destination NOT NULL
/// column absent from the page). All four buckets must be covered: a widening or
/// droppable-required column written to a *non-evolved* destination is exactly
/// the silent corruption `quarantine` exists to prevent.
fn quarantine_drift_rows(
    diff: &crate::drift::SchemaDiff,
    records: Vec<Value>,
    page_indices: &[usize],
    sink_name: &str,
    pipeline_name: &str,
    row: &str,
) -> (Vec<Value>, Vec<Value>) {
    use crate::dlq::{DlqReason, build_envelope};
    // Columns that taint a row by their PRESENCE in the record.
    let present_cols: std::collections::HashSet<&str> = diff
        .additions
        .iter()
        .chain(&diff.widenings)
        .chain(&diff.incompatible)
        .map(|c| c.name.as_str())
        .collect();
    // Required destination columns that taint a row by their ABSENCE.
    let required_cols: std::collections::HashSet<&str> =
        diff.droppable_required.iter().map(|s| s.as_str()).collect();
    let mut kept = Vec::new();
    let mut envelopes = Vec::new();
    for (idx, rec) in records.into_iter().enumerate() {
        let exhibits = rec
            .as_object()
            .map(|m| {
                m.keys().any(|k| present_cols.contains(k.as_str()))
                    || required_cols.iter().any(|c| !m.contains_key(*c))
            })
            .unwrap_or(false);
        if exhibits {
            let err = FaucetError::SchemaDrift {
                columns: diff.changed_columns(),
                message: "row exhibits schema drift (on_drift=quarantine)".into(),
            };
            // Map the survivor-relative position back to the record's true page
            // index so the envelope annotation matches quality/contract (#321 L6).
            let page_index = page_indices.get(idx).copied().unwrap_or(idx);
            envelopes.push(build_envelope(
                &rec,
                &err,
                DlqReason::SchemaDrift,
                sink_name,
                pipeline_name,
                row,
                page_index,
            ));
        } else {
            kept.push(rec);
        }
    }
    (kept, envelopes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde_json::json;

    // ── Mock Source ──────────────────────────────────────────────────────────

    struct MockSource(Vec<Value>);

    #[async_trait]
    impl Source for MockSource {
        async fn fetch_with_context(
            &self,
            _context: &std::collections::HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(self.0.clone())
        }
    }

    struct IncrementalSource {
        records: Vec<Value>,
        bookmark: Value,
    }

    #[async_trait]
    impl Source for IncrementalSource {
        async fn fetch_with_context(
            &self,
            _context: &std::collections::HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(self.records.clone())
        }
        async fn fetch_with_context_incremental(
            &self,
            _context: &std::collections::HashMap<String, Value>,
        ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
            Ok((self.records.clone(), Some(self.bookmark.clone())))
        }
    }

    struct FailingSource;

    #[async_trait]
    impl Source for FailingSource {
        async fn fetch_with_context(
            &self,
            _context: &std::collections::HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Err(FaucetError::Auth("no credentials".into()))
        }
    }

    // ── Mock Sink ───────────────────────────────────────────────────────────

    struct MockSink(std::sync::Mutex<Vec<Value>>);

    impl MockSink {
        fn new() -> Self {
            Self(std::sync::Mutex::new(Vec::new()))
        }
        fn written(&self) -> Vec<Value> {
            self.0.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl Sink for MockSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            self.0.lock().unwrap().extend(records.iter().cloned());
            Ok(records.len())
        }
    }

    struct FailingSink;

    #[async_trait]
    impl Sink for FailingSink {
        async fn write_batch(&self, _records: &[Value]) -> Result<usize, FaucetError> {
            Err(FaucetError::Sink("write failed".into()))
        }
    }

    /// Records the overwrite lifecycle call order (#492) so tests can assert the
    /// pipeline stages begin → writes → commit on success, and begin → abort on
    /// failure/cancel (never destroying the destination before commit).
    struct OverwriteRecordingSink {
        events: Arc<std::sync::Mutex<Vec<String>>>,
        is_overwrite: bool,
        fail_write: bool,
    }

    impl OverwriteRecordingSink {
        fn new(is_overwrite: bool, fail_write: bool) -> (Self, Arc<std::sync::Mutex<Vec<String>>>) {
            let events = Arc::new(std::sync::Mutex::new(Vec::new()));
            (
                Self {
                    events: events.clone(),
                    is_overwrite,
                    fail_write,
                },
                events,
            )
        }
        fn log(&self, s: &str) {
            self.events.lock().unwrap().push(s.to_string());
        }
    }

    #[async_trait]
    impl Sink for OverwriteRecordingSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            self.log(&format!("write:{}", records.len()));
            if self.fail_write {
                return Err(FaucetError::Sink("write failed".into()));
            }
            Ok(records.len())
        }
        fn is_overwrite(&self) -> bool {
            self.is_overwrite
        }
        async fn begin_overwrite(&self) -> Result<(), FaucetError> {
            self.log("begin");
            Ok(())
        }
        async fn commit_overwrite(&self) -> Result<(), FaucetError> {
            self.log("commit");
            Ok(())
        }
        async fn abort_overwrite(&self) -> Result<(), FaucetError> {
            self.log("abort");
            Ok(())
        }
    }

    #[tokio::test]
    async fn overwrite_lifecycle_begins_then_commits_on_success() {
        let source = MockSource(vec![json!({"id": 1}), json!({"id": 2})]);
        let (sink, events) = OverwriteRecordingSink::new(true, false);
        let result = Pipeline::new(&source, &sink).run().await;
        assert!(result.is_ok());
        let log = events.lock().unwrap().clone();
        assert_eq!(log.first().map(String::as_str), Some("begin"));
        assert_eq!(log.last().map(String::as_str), Some("commit"));
        assert!(log.contains(&"write:2".to_string()));
        assert!(!log.contains(&"abort".to_string()));
    }

    #[tokio::test]
    async fn overwrite_lifecycle_aborts_on_write_failure() {
        let source = MockSource(vec![json!({"id": 1})]);
        let (sink, events) = OverwriteRecordingSink::new(true, true);
        let result = Pipeline::new(&source, &sink).run().await;
        assert!(result.is_err(), "a failed write must fail the run");
        let log = events.lock().unwrap().clone();
        assert_eq!(log.first().map(String::as_str), Some("begin"));
        assert!(
            log.contains(&"abort".to_string()),
            "failure must trigger abort_overwrite: {log:?}"
        );
        assert!(
            !log.contains(&"commit".to_string()),
            "a failed run must never commit the swap: {log:?}"
        );
    }

    #[tokio::test]
    async fn overwrite_lifecycle_aborts_on_cancel() {
        let source = MockSource(vec![json!({"id": 1})]);
        let (sink, events) = OverwriteRecordingSink::new(true, false);
        let token = crate::CancellationToken::new();
        token.cancel(); // pre-cancelled: the run stops at the first page boundary
        let result = Pipeline::new(&source, &sink).with_cancel(token).run().await;
        assert!(result.is_ok(), "a cooperative cancel returns Ok(partial)");
        let log = events.lock().unwrap().clone();
        assert_eq!(log.first().map(String::as_str), Some("begin"));
        assert!(
            log.contains(&"abort".to_string()),
            "a cancelled overwrite run must abort, not commit: {log:?}"
        );
        assert!(!log.contains(&"commit".to_string()));
    }

    #[tokio::test]
    async fn non_overwrite_sink_skips_the_lifecycle() {
        let source = MockSource(vec![json!({"id": 1})]);
        let (sink, events) = OverwriteRecordingSink::new(false, false);
        let result = Pipeline::new(&source, &sink).run().await;
        assert!(result.is_ok());
        let log = events.lock().unwrap().clone();
        assert!(
            !log.iter()
                .any(|e| e == "begin" || e == "commit" || e == "abort"),
            "a non-overwrite sink must never see the overwrite lifecycle: {log:?}"
        );
        assert!(log.contains(&"write:1".to_string()));
    }

    #[tokio::test]
    async fn suppressed_overwrite_writes_but_skips_the_lifecycle() {
        // #552: with the lifecycle suppressed, an overwrite sink still receives
        // writes (they land in staging via the sink's own config) but the
        // pipeline must NOT call begin/commit/abort — the external orchestrator
        // owns those once per fan-out group.
        let source = MockSource(vec![json!({"id": 1}), json!({"id": 2})]);
        let (sink, events) = OverwriteRecordingSink::new(true, false);
        let result = Pipeline::new(&source, &sink)
            .with_suppress_overwrite(true)
            .run()
            .await;
        assert!(result.is_ok());
        let log = events.lock().unwrap().clone();
        assert!(
            log.contains(&"write:2".to_string()),
            "suppressed overwrite must still write: {log:?}"
        );
        assert!(
            !log.iter()
                .any(|e| e == "begin" || e == "commit" || e == "abort"),
            "suppressed overwrite must not drive the lifecycle: {log:?}"
        );
    }

    #[tokio::test]
    async fn overwrite_abort_failure_is_logged_not_propagated() {
        // When a run fails and `abort_overwrite` itself then fails, the pipeline
        // logs a warning and still surfaces the *original* run error — the abort
        // failure must not mask it (and never panics). Covers the abort-failure
        // warn branch in `Pipeline::run`.
        struct AbortFailSink;
        #[async_trait]
        impl Sink for AbortFailSink {
            async fn write_batch(&self, _records: &[Value]) -> Result<usize, FaucetError> {
                Err(FaucetError::Sink("write failed".into()))
            }
            fn is_overwrite(&self) -> bool {
                true
            }
            async fn begin_overwrite(&self) -> Result<(), FaucetError> {
                Ok(())
            }
            async fn commit_overwrite(&self) -> Result<(), FaucetError> {
                Ok(())
            }
            async fn abort_overwrite(&self) -> Result<(), FaucetError> {
                Err(FaucetError::Sink("abort failed".into()))
            }
        }
        let source = MockSource(vec![json!({"id": 1})]);
        let sink = AbortFailSink;
        let err = Pipeline::new(&source, &sink)
            .run()
            .await
            .expect_err("a failed write must fail the run");
        assert!(
            err.to_string().contains("write failed"),
            "the run error must be the write failure, not the abort failure: {err}"
        );
    }

    /// Records writes and how many times `flush` was called. Used to assert the
    /// pipeline flushes the sink on the error/early-return path so partial
    /// output (e.g. a Parquet footer) is made durable before the error
    /// propagates.
    struct FlushTrackingSink {
        written: std::sync::Mutex<Vec<Value>>,
        flush_count: std::sync::atomic::AtomicUsize,
    }

    impl FlushTrackingSink {
        fn new() -> Self {
            Self {
                written: std::sync::Mutex::new(Vec::new()),
                flush_count: std::sync::atomic::AtomicUsize::new(0),
            }
        }
        fn written(&self) -> Vec<Value> {
            self.written.lock().unwrap().clone()
        }
        fn flush_count(&self) -> usize {
            self.flush_count.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl Sink for FlushTrackingSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            self.written.lock().unwrap().extend(records.iter().cloned());
            Ok(records.len())
        }
        async fn flush(&self) -> Result<(), FaucetError> {
            self.flush_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    // ── Exactly-once test doubles ────────────────────────────────────────────

    /// In-memory sink that commits rows + a per-scope token atomically.
    struct IdempotentMockSink {
        rows: std::sync::Mutex<Vec<Value>>,
        tokens: std::sync::Mutex<std::collections::HashMap<String, String>>,
    }
    impl IdempotentMockSink {
        fn new() -> Self {
            Self {
                rows: std::sync::Mutex::new(Vec::new()),
                tokens: std::sync::Mutex::new(std::collections::HashMap::new()),
            }
        }
        fn rows(&self) -> Vec<Value> {
            self.rows.lock().unwrap().clone()
        }
    }
    #[async_trait]
    impl Sink for IdempotentMockSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            self.rows.lock().unwrap().extend(records.iter().cloned());
            Ok(records.len())
        }
        fn supports_idempotent_writes(&self) -> bool {
            true
        }
        async fn write_batch_idempotent(
            &self,
            records: &[Value],
            scope: &str,
            token: &str,
        ) -> Result<usize, FaucetError> {
            self.rows.lock().unwrap().extend(records.iter().cloned());
            self.tokens
                .lock()
                .unwrap()
                .insert(scope.to_string(), token.to_string());
            Ok(records.len())
        }
        async fn last_committed_token(&self, scope: &str) -> Result<Option<String>, FaucetError> {
            Ok(self.tokens.lock().unwrap().get(scope).cloned())
        }
    }

    fn eo_opts(store: Arc<dyn StateStore>, key: &str, start_seq: u64) -> RunStreamOptions {
        RunStreamOptions::new()
            .with_state(store, key)
            .with_delivery(crate::idempotency::DeliveryMode::ExactlyOnce)
            .with_start_seq(start_seq)
    }

    #[tokio::test]
    async fn exactly_once_writes_pages_and_persists_wrapped_state() {
        let pages = vec![
            Ok(StreamPage {
                records: vec![json!({"id": 1})],
                bookmark: Some(json!("b1")),
            }),
            Ok(StreamPage {
                records: vec![json!({"id": 2})],
                bookmark: Some(json!("b2")),
            }),
        ];
        let sink = IdempotentMockSink::new();
        let store: Arc<dyn StateStore> = Arc::new(crate::state::MemoryStateStore::new());
        let r = run_stream(
            futures::stream::iter(pages),
            &sink,
            eo_opts(store.clone(), "k", 0),
        )
        .await
        .unwrap();
        assert_eq!(r.records_written, 2);
        let (bm, seq) = crate::idempotency::unwrap_state(&store.get("k").await.unwrap().unwrap());
        assert_eq!(bm, Some(json!("b2")));
        assert_eq!(seq, 2);
        // The committed token embeds the page's resume bookmark (sink-anchored
        // resume) — sequence and bookmark both recoverable from the sink.
        let token = sink.last_committed_token("k").await.unwrap().unwrap();
        assert_eq!(
            crate::idempotency::parse_token_parts(&token),
            Some((2, Some(json!("b2"))))
        );
    }

    #[tokio::test]
    async fn exactly_once_skips_already_committed_pages_on_resume() {
        let sink = IdempotentMockSink::new();
        // Run 1: commit page seq 1 directly (simulate crash: state lost).
        sink.write_batch_idempotent(
            &[json!({"id": 1})],
            "k",
            &crate::idempotency::format_token(1),
        )
        .await
        .unwrap();
        assert_eq!(sink.rows().len(), 1);
        // Run 2 (resume): fresh state, full replay. Page 1 must be skipped.
        let pages = vec![
            Ok(StreamPage {
                records: vec![json!({"id": 1})],
                bookmark: Some(json!("b1")),
            }),
            Ok(StreamPage {
                records: vec![json!({"id": 2})],
                bookmark: Some(json!("b2")),
            }),
        ];
        let store: Arc<dyn StateStore> = Arc::new(crate::state::MemoryStateStore::new());
        let r = run_stream(futures::stream::iter(pages), &sink, eo_opts(store, "k", 0))
            .await
            .unwrap();
        assert_eq!(r.records_written, 1);
        let rows = sink.rows();
        assert_eq!(
            rows.len(),
            2,
            "exactly one row per id — no duplicate of id=1"
        );
        assert_eq!(rows.iter().filter(|v| v["id"] == 1).count(), 1);
    }

    #[tokio::test]
    async fn exactly_once_rejects_non_idempotent_sink() {
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![];
        let store: Arc<dyn StateStore> = Arc::new(crate::state::MemoryStateStore::new());
        let r = run_stream(
            futures::stream::iter(pages),
            &MockSink::new(),
            eo_opts(store, "k", 0),
        )
        .await;
        assert!(matches!(r, Err(FaucetError::Config(_))));
    }

    #[tokio::test]
    async fn exactly_once_rejects_missing_state_store() {
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![];
        let opts =
            RunStreamOptions::new().with_delivery(crate::idempotency::DeliveryMode::ExactlyOnce);
        let r = run_stream(
            futures::stream::iter(pages),
            &IdempotentMockSink::new(),
            opts,
        )
        .await;
        assert!(matches!(r, Err(FaucetError::Config(_))));
    }

    /// A sink that is not atomic-watermark capable but is *configured* to
    /// dedup by key (`write_mode: upsert` + `key`).
    struct KeyedMockSink(MockSink);
    #[async_trait]
    impl Sink for KeyedMockSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            self.0.write_batch(records).await
        }
        fn dedups_by_key(&self) -> bool {
            true
        }
        fn supported_write_modes(&self) -> &'static [crate::write_mode::WriteMode] {
            &[
                crate::write_mode::WriteMode::Append,
                crate::write_mode::WriteMode::Upsert,
                crate::write_mode::WriteMode::Delete,
            ]
        }
    }

    #[tokio::test]
    async fn exactly_once_keyed_upsert_mechanism_uses_plain_write_path() {
        // A non-deterministic source + keyed-upsert sink is accepted under
        // `delivery: exactly_once` (effectively-once via keyed dedup, #292):
        // records flow through the ordinary write path and the bookmark is
        // persisted bare (no wrapped seq — there is no atomic watermark).
        let sink = KeyedMockSink(MockSink::new());
        let store: Arc<dyn StateStore> = Arc::new(crate::state::MemoryStateStore::new());
        let pages = vec![Ok(StreamPage {
            records: vec![json!({"id": 1})],
            bookmark: Some(json!("b1")),
        })];
        let opts = eo_opts(store.clone(), "k", 0)
            .with_replay_guarantee(crate::idempotency::ReplayGuarantee::NonDeterministic);
        let r = run_stream(futures::stream::iter(pages), &sink, opts)
            .await
            .unwrap();
        assert_eq!(r.records_written, 1);
        assert_eq!(sink.0.written(), vec![json!({"id": 1})]);
        // Bare bookmark, not the exactly-once wrapper.
        assert_eq!(store.get("k").await.unwrap(), Some(json!("b1")));
    }

    #[tokio::test]
    async fn exactly_once_rejects_non_deterministic_source_without_keyed_dedup() {
        // Atomic-capable sink, but the declared replay guarantee is
        // non-deterministic and no keyed dedup is configured: page-skip
        // correctness cannot be upheld, so the run is rejected with a hint
        // toward keyed upsert.
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![];
        let store: Arc<dyn StateStore> = Arc::new(crate::state::MemoryStateStore::new());
        let opts = eo_opts(store, "k", 0)
            .with_replay_guarantee(crate::idempotency::ReplayGuarantee::NonDeterministic);
        let r = run_stream(
            futures::stream::iter(pages),
            &IdempotentMockSink::new(),
            opts,
        )
        .await;
        match r {
            Err(FaucetError::Config(msg)) => {
                assert!(msg.contains("write_mode: upsert"), "hint present: {msg}")
            }
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn exactly_once_rejects_plain_sink_with_mechanism_hint() {
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![];
        let store: Arc<dyn StateStore> = Arc::new(crate::state::MemoryStateStore::new());
        let r = run_stream(
            futures::stream::iter(pages),
            &MockSink::new(),
            eo_opts(store, "k", 0),
        )
        .await;
        match r {
            Err(FaucetError::Config(msg)) => assert!(
                msg.contains("provides neither"),
                "names both mechanisms: {msg}"
            ),
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    /// Deterministic-replay source that records every applied bookmark and
    /// then streams one page — used to prove sink-anchored resume.
    struct AnchorRecordingSource {
        applied: std::sync::Mutex<Vec<Value>>,
    }
    #[async_trait]
    impl Source for AnchorRecordingSource {
        async fn fetch_with_context(
            &self,
            _context: &std::collections::HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(vec![json!({"id": 10})])
        }
        async fn fetch_with_context_incremental(
            &self,
            _context: &std::collections::HashMap<String, Value>,
        ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
            Ok((vec![json!({"id": 10})], Some(json!("after"))))
        }
        fn state_key(&self) -> Option<String> {
            Some("anchor_key".to_string())
        }
        async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
            self.applied.lock().unwrap().push(bookmark);
            Ok(())
        }
        fn supports_exactly_once(&self) -> bool {
            true
        }
    }

    #[tokio::test]
    async fn pipeline_run_anchors_resume_at_sink_embedded_bookmark() {
        // Crash window: the sink durably committed page seq 5 (token embeds
        // its bookmark) but the state store only persisted seq 4. On resume
        // the pipeline must re-anchor the source at the sink's embedded
        // position — the state bookmark is applied first, then overridden —
        // and continue numbering from the sink's sequence (no skips, no
        // duplicates, no reliance on replayed page boundaries).
        let source = AnchorRecordingSource {
            applied: std::sync::Mutex::new(Vec::new()),
        };
        let sink = IdempotentMockSink::new();
        sink.tokens.lock().unwrap().insert(
            "anchor_key".to_string(),
            crate::idempotency::format_token_with_bookmark(5, Some(&json!("sink-pos"))),
        );
        let store: Arc<dyn StateStore> = Arc::new(crate::state::MemoryStateStore::new());
        store
            .put(
                "anchor_key",
                &crate::idempotency::wrap_state(Some(&json!("state-pos")), 4),
            )
            .await
            .unwrap();

        let r = Pipeline::new(&source, &sink)
            .with_state_store(Arc::clone(&store))
            .with_delivery(crate::idempotency::DeliveryMode::ExactlyOnce)
            .run()
            .await
            .unwrap();

        assert_eq!(
            *source.applied.lock().unwrap(),
            vec![json!("state-pos"), json!("sink-pos")],
            "state bookmark applied, then overridden by the sink anchor"
        );
        // The replayed page is written (it is *after* the anchored position),
        // committed at seq 6 — not skipped by the stale count.
        assert_eq!(r.records_written, 1);
        let token = sink
            .last_committed_token("anchor_key")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            crate::idempotency::parse_token_parts(&token),
            Some((6, Some(json!("after"))))
        );
        let (bm, seq) =
            crate::idempotency::unwrap_state(&store.get("anchor_key").await.unwrap().unwrap());
        assert_eq!((bm, seq), (Some(json!("after")), 6));
    }

    #[tokio::test]
    async fn pipeline_run_ignores_sink_token_behind_state_seq() {
        // Sink watermark at seq 4, state already at seq 4 — nothing to anchor;
        // the source resumes from the state bookmark only.
        let source = AnchorRecordingSource {
            applied: std::sync::Mutex::new(Vec::new()),
        };
        let sink = IdempotentMockSink::new();
        sink.tokens.lock().unwrap().insert(
            "anchor_key".to_string(),
            crate::idempotency::format_token_with_bookmark(4, Some(&json!("sink-pos"))),
        );
        let store: Arc<dyn StateStore> = Arc::new(crate::state::MemoryStateStore::new());
        store
            .put(
                "anchor_key",
                &crate::idempotency::wrap_state(Some(&json!("state-pos")), 4),
            )
            .await
            .unwrap();
        Pipeline::new(&source, &sink)
            .with_state_store(Arc::clone(&store))
            .with_delivery(crate::idempotency::DeliveryMode::ExactlyOnce)
            .run()
            .await
            .unwrap();
        assert_eq!(*source.applied.lock().unwrap(), vec![json!("state-pos")]);
    }

    #[tokio::test]
    async fn pipeline_run_legacy_bare_token_falls_back_to_skip_path() {
        // A pre-upgrade watermark (bare seq, no embedded bookmark) cannot
        // anchor; the skip path applies: the replayed page (seq 1 ≤ committed
        // 1) is skipped, nothing double-written.
        let source = AnchorRecordingSource {
            applied: std::sync::Mutex::new(Vec::new()),
        };
        let sink = IdempotentMockSink::new();
        sink.tokens.lock().unwrap().insert(
            "anchor_key".to_string(),
            crate::idempotency::format_token(1),
        );
        let store: Arc<dyn StateStore> = Arc::new(crate::state::MemoryStateStore::new());
        let r = Pipeline::new(&source, &sink)
            .with_state_store(Arc::clone(&store))
            .with_delivery(crate::idempotency::DeliveryMode::ExactlyOnce)
            .run()
            .await
            .unwrap();
        assert!(source.applied.lock().unwrap().is_empty());
        assert_eq!(r.records_written, 0, "page 1 already committed → skipped");
        assert!(sink.rows().is_empty());
    }

    // ── StreamPage / batch_size tests ───────────────────────────────────────

    #[test]
    fn stream_page_constructs() {
        let page = StreamPage {
            records: vec![json!({"id": 1})],
            bookmark: Some(json!("2026-05-18")),
        };
        assert_eq!(page.records.len(), 1);
        assert_eq!(page.bookmark, Some(json!("2026-05-18")));
    }

    #[test]
    fn validate_batch_size_accepts_zero_as_no_batching_sentinel() {
        // 0 means "do not batch — emit/accept the whole result set in one page".
        assert_eq!(validate_batch_size(0).unwrap(), 0);
    }

    #[test]
    fn validate_batch_size_rejects_too_large() {
        let err = validate_batch_size(MAX_BATCH_SIZE + 1).unwrap_err();
        assert!(matches!(err, FaucetError::Config(_)));
    }

    #[test]
    fn validate_batch_size_accepts_one() {
        assert_eq!(validate_batch_size(1).unwrap(), 1);
    }

    #[test]
    fn validate_batch_size_accepts_max() {
        assert_eq!(validate_batch_size(MAX_BATCH_SIZE).unwrap(), MAX_BATCH_SIZE);
    }

    // Compile-time invariant: DEFAULT_BATCH_SIZE must be within [1, MAX_BATCH_SIZE].
    const _: () = {
        assert!(DEFAULT_BATCH_SIZE >= 1);
        assert!(DEFAULT_BATCH_SIZE <= MAX_BATCH_SIZE);
    };

    // ── Batch mode tests ────────────────────────────────────────────────────

    #[tokio::test]
    async fn batch_pipeline_writes_all_records() {
        let source = MockSource(vec![json!({"id": 1}), json!({"id": 2}), json!({"id": 3})]);
        let sink = MockSink::new();

        let result = Pipeline::new(&source, &sink).run().await.unwrap();

        assert_eq!(result.records_written, 3);
        assert!(result.bookmark.is_none());
        assert_eq!(sink.written().len(), 3);
    }

    #[tokio::test]
    async fn batch_pipeline_returns_bookmark() {
        let source = IncrementalSource {
            records: vec![json!({"id": 1, "ts": "2024-12-01"})],
            bookmark: json!("2024-12-01"),
        };
        let sink = MockSink::new();

        let result = Pipeline::new(&source, &sink).run().await.unwrap();

        assert_eq!(result.records_written, 1);
        assert_eq!(result.bookmark, Some(json!("2024-12-01")));
    }

    #[tokio::test]
    async fn batch_pipeline_empty_source() {
        let source = MockSource(vec![]);
        let sink = MockSink::new();

        let result = Pipeline::new(&source, &sink).run().await.unwrap();

        assert_eq!(result.records_written, 0);
        assert!(sink.written().is_empty());
    }

    #[tokio::test]
    async fn batch_pipeline_source_error_propagates() {
        let source = FailingSource;
        let sink = MockSink::new();

        let result = Pipeline::new(&source, &sink).run().await;
        assert!(result.is_err());
        assert!(sink.written().is_empty());
    }

    #[tokio::test]
    async fn batch_pipeline_sink_error_propagates() {
        let source = MockSource(vec![json!({"id": 1})]);
        let sink = FailingSink;

        let result = Pipeline::new(&source, &sink).run().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn batch_pipeline_with_trait_objects() {
        let source: Box<dyn Source> = Box::new(MockSource(vec![json!({"id": 1})]));
        let sink: Box<dyn Sink> = Box::new(MockSink::new());

        let result = Pipeline::new(source.as_ref(), sink.as_ref())
            .run()
            .await
            .unwrap();

        assert_eq!(result.records_written, 1);
    }

    // ── Streaming mode tests ────────────────────────────────────────────────

    #[tokio::test]
    async fn stream_pipeline_writes_pages() {
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![
            Ok(StreamPage {
                records: vec![json!({"id": 1}), json!({"id": 2})],
                bookmark: None,
            }),
            Ok(StreamPage {
                records: vec![json!({"id": 3})],
                bookmark: None,
            }),
        ];
        let stream = futures::stream::iter(pages);
        let sink = MockSink::new();

        let result = run_stream(stream, &sink, RunStreamOptions::new())
            .await
            .unwrap();

        assert_eq!(result.records_written, 3);
        assert!(result.bookmark.is_none());
        assert_eq!(sink.written().len(), 3);
    }

    #[tokio::test]
    async fn stream_pipeline_flushes_sink_on_source_error() {
        // Regression for #78/#3: a mid-stream source error must not skip the
        // sink flush. Without flushing, a buffered sink (e.g. Parquet, whose
        // footer is only written on flush) loses everything written so far.
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![
            Ok(StreamPage {
                records: vec![json!({"id": 1}), json!({"id": 2})],
                bookmark: None,
            }),
            Err(FaucetError::Source("transient blip mid-stream".into())),
        ];
        let stream = futures::stream::iter(pages);
        let sink = FlushTrackingSink::new();

        let result = run_stream(stream, &sink, RunStreamOptions::new()).await;

        // The original source error must still propagate.
        assert!(matches!(result, Err(FaucetError::Source(_))));
        // The good page must have been written before the error.
        assert_eq!(sink.written().len(), 2);
        // Crucially, the sink must have been flushed on the error path.
        assert!(
            sink.flush_count() >= 1,
            "sink must be flushed on the error path so partial output is durable"
        );
    }

    #[tokio::test]
    async fn stream_pipeline_flushes_sink_on_cancel() {
        // #146 H16: a cooperative cancellation mid-run must stop polling, flush
        // the sink (so a Parquet footer / S3 multipart is completed rather than
        // orphaned), and return the partial result — NOT drop the run future,
        // which would flush nothing.
        use tokio_util::sync::CancellationToken;

        // One page, then block forever — the only way out is the cancel token.
        let stream = Box::pin(async_stream::stream! {
            yield Ok(StreamPage {
                records: vec![json!({"id": 1}), json!({"id": 2})],
                bookmark: None,
            });
            futures::future::pending::<()>().await;
        });
        let sink = FlushTrackingSink::new();

        let token = CancellationToken::new();
        let canceller = token.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            canceller.cancel();
        });

        let result = run_stream(stream, &sink, RunStreamOptions::new().with_cancel(token))
            .await
            .expect("a cooperative cancel returns Ok with the partial result");

        // The page written before cancellation survives, and the sink was
        // flushed so that output is durable.
        assert_eq!(result.records_written, 2);
        assert_eq!(sink.written().len(), 2);
        assert!(
            sink.flush_count() >= 1,
            "sink must be flushed on the cancel path so partial output is durable"
        );
    }

    #[tokio::test]
    async fn stream_pipeline_empty() {
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![];
        let stream = futures::stream::iter(pages);
        let sink = MockSink::new();

        let result = run_stream(stream, &sink, RunStreamOptions::new())
            .await
            .unwrap();

        assert_eq!(result.records_written, 0);
    }

    #[tokio::test]
    async fn stream_pipeline_skips_empty_pages() {
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![
            Ok(StreamPage {
                records: vec![json!({"id": 1})],
                bookmark: None,
            }),
            Ok(StreamPage {
                records: vec![],
                bookmark: None,
            }),
            Ok(StreamPage {
                records: vec![json!({"id": 2})],
                bookmark: None,
            }),
        ];
        let stream = futures::stream::iter(pages);
        let sink = MockSink::new();

        let result = run_stream(stream, &sink, RunStreamOptions::new())
            .await
            .unwrap();

        assert_eq!(result.records_written, 2);
    }

    #[tokio::test]
    async fn stream_pipeline_error_in_page_propagates() {
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![
            Ok(StreamPage {
                records: vec![json!({"id": 1})],
                bookmark: None,
            }),
            Err(FaucetError::HttpStatus {
                status: 500,
                url: "https://example.com".into(),
                body: "Internal Server Error".into(),
            }),
        ];
        let stream = futures::stream::iter(pages);
        let sink = MockSink::new();

        let result = run_stream(stream, &sink, RunStreamOptions::new()).await;
        assert!(result.is_err());
        // First page was written before the error
        assert_eq!(sink.written().len(), 1);
    }

    #[tokio::test]
    async fn stream_pipeline_sink_error_propagates() {
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: vec![json!({"id": 1})],
            bookmark: None,
        })];
        let stream = futures::stream::iter(pages);
        let sink = FailingSink;

        let result = run_stream(stream, &sink, RunStreamOptions::new()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn stream_pipeline_with_trait_object_sink() {
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: vec![json!({"id": 1})],
            bookmark: None,
        })];
        let stream = futures::stream::iter(pages);
        let sink: Box<dyn Sink> = Box::new(MockSink::new());

        let result = run_stream(stream, sink.as_ref(), RunStreamOptions::new())
            .await
            .unwrap();
        assert_eq!(result.records_written, 1);
    }

    #[tokio::test]
    async fn stream_pipeline_persists_bookmark_when_page_carries_one() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![
            Ok(StreamPage {
                records: vec![json!({"id": 1})],
                bookmark: None,
            }),
            Ok(StreamPage {
                records: vec![json!({"id": 2})],
                bookmark: Some(json!("checkpoint-final")),
            }),
        ];
        let stream = futures::stream::iter(pages);
        let sink = MockSink::new();

        let result = run_stream(
            stream,
            &sink,
            RunStreamOptions::new().with_state(Arc::clone(&store), "k"),
        )
        .await
        .unwrap();

        assert_eq!(result.records_written, 2);
        assert_eq!(result.bookmark, Some(json!("checkpoint-final")));
        assert_eq!(
            store.get("k").await.unwrap(),
            Some(json!("checkpoint-final"))
        );
    }

    #[tokio::test]
    async fn stream_pipeline_persists_per_page_bookmarks() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![
            Ok(StreamPage {
                records: vec![json!({"id": 1})],
                bookmark: Some(json!("tx-1")),
            }),
            Ok(StreamPage {
                records: vec![json!({"id": 2})],
                bookmark: Some(json!("tx-2")),
            }),
        ];
        let stream = futures::stream::iter(pages);
        let sink = MockSink::new();

        run_stream(
            stream,
            &sink,
            RunStreamOptions::new().with_state(Arc::clone(&store), "k"),
        )
        .await
        .unwrap();

        // Latest per-page bookmark wins.
        assert_eq!(store.get("k").await.unwrap(), Some(json!("tx-2")));
    }

    // ── State-store integration tests ───────────────────────────────────────

    use crate::state::{FileStateStore, MemoryStateStore, StateStore};
    use std::sync::Arc;
    use tempfile::TempDir;

    /// Source that opts into state persistence. It records the bookmark it
    /// received via `apply_start_bookmark` so tests can verify the pipeline
    /// pushed the stored value back into it on resume.
    struct StatefulSource {
        key: String,
        records: Vec<Value>,
        new_bookmark: Value,
        seen_bookmark: std::sync::Mutex<Option<Value>>,
    }

    impl StatefulSource {
        fn new(key: &str, records: Vec<Value>, new_bookmark: Value) -> Self {
            Self {
                key: key.into(),
                records,
                new_bookmark,
                seen_bookmark: std::sync::Mutex::new(None),
            }
        }
        fn observed_start(&self) -> Option<Value> {
            self.seen_bookmark.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl Source for StatefulSource {
        async fn fetch_with_context(
            &self,
            _ctx: &std::collections::HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(self.records.clone())
        }
        async fn fetch_with_context_incremental(
            &self,
            _ctx: &std::collections::HashMap<String, Value>,
        ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
            Ok((self.records.clone(), Some(self.new_bookmark.clone())))
        }
        fn state_key(&self) -> Option<String> {
            Some(self.key.clone())
        }
        async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
            *self.seen_bookmark.lock().unwrap() = Some(bookmark);
            Ok(())
        }
    }

    #[tokio::test]
    async fn pipeline_with_state_store_persists_bookmark_after_sink() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let source = StatefulSource::new(
            "github_issues",
            vec![json!({"id": 1, "ts": "2026-05-01"})],
            json!("2026-05-01"),
        );
        let sink = MockSink::new();
        let result = Pipeline::new(&source, &sink)
            .with_state_store(Arc::clone(&store))
            .run()
            .await
            .unwrap();

        assert_eq!(result.records_written, 1);
        assert_eq!(result.bookmark, Some(json!("2026-05-01")));
        // Stored value matches what the source returned.
        let stored = store.get("github_issues").await.unwrap();
        assert_eq!(stored, Some(json!("2026-05-01")));
    }

    #[tokio::test]
    async fn pipeline_with_state_store_resumes_from_stored_bookmark() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        store
            .put("github_issues", &json!("2026-04-30"))
            .await
            .unwrap();

        let source =
            StatefulSource::new("github_issues", vec![json!({"id": 2})], json!("2026-05-01"));
        let sink = MockSink::new();
        Pipeline::new(&source, &sink)
            .with_state_store(Arc::clone(&store))
            .run()
            .await
            .unwrap();

        // The pipeline pushed the previously-stored bookmark back into the source.
        assert_eq!(source.observed_start(), Some(json!("2026-04-30")));
        // And then overwrote it with the new value from this run.
        assert_eq!(
            store.get("github_issues").await.unwrap(),
            Some(json!("2026-05-01"))
        );
    }

    #[tokio::test]
    async fn pipeline_with_state_store_does_not_persist_when_sink_fails() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let source = StatefulSource::new("k", vec![json!({"id": 1})], json!("2026-05-01"));
        let sink = FailingSink;

        let result = Pipeline::new(&source, &sink)
            .with_state_store(Arc::clone(&store))
            .run()
            .await;
        assert!(result.is_err());
        assert!(store.get("k").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn pipeline_with_state_store_no_state_key_means_no_persist() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let source = IncrementalSource {
            records: vec![json!({"id": 1})],
            bookmark: json!("ignored"),
        };
        let sink = MockSink::new();
        Pipeline::new(&source, &sink)
            .with_state_store(Arc::clone(&store))
            .run()
            .await
            .unwrap();
        // IncrementalSource doesn't override state_key, so nothing was persisted.
        // Cross-check that no keys exist by trying a likely one.
        assert!(store.get("anything").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn pipeline_with_state_store_skips_persist_when_bookmark_is_none() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        struct NoBookmarkSource;
        #[async_trait]
        impl Source for NoBookmarkSource {
            async fn fetch_with_context(
                &self,
                _ctx: &std::collections::HashMap<String, Value>,
            ) -> Result<Vec<Value>, FaucetError> {
                Ok(vec![json!({"id": 1})])
            }
            fn state_key(&self) -> Option<String> {
                Some("k".into())
            }
        }
        let source = NoBookmarkSource;
        let sink = MockSink::new();
        Pipeline::new(&source, &sink)
            .with_state_store(Arc::clone(&store))
            .run()
            .await
            .unwrap();
        assert!(store.get("k").await.unwrap().is_none());
    }

    // ── Pipeline::run drives stream_pages ──────────────────────────────────

    /// A source with a custom `stream_pages` impl that yields three pages.
    /// Used to prove `Pipeline::run` drives the streaming path.
    struct PagedSource;

    #[async_trait]
    impl Source for PagedSource {
        async fn fetch_with_context(
            &self,
            _ctx: &std::collections::HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            // Should never be called when stream_pages is overridden.
            unreachable!("Pipeline::run must drive stream_pages, not fetch_with_context");
        }
        fn stream_pages<'a>(
            &'a self,
            _ctx: &'a std::collections::HashMap<String, Value>,
            _batch_size: usize,
        ) -> std::pin::Pin<
            Box<dyn futures_core::Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>,
        > {
            Box::pin(async_stream::try_stream! {
                yield StreamPage { records: vec![json!({"i": 1})], bookmark: None };
                yield StreamPage { records: vec![json!({"i": 2})], bookmark: None };
                yield StreamPage { records: vec![json!({"i": 3})], bookmark: Some(json!("final")) };
            })
        }
    }

    /// Sink that counts how many distinct write_batch calls happen.
    struct CountingSink {
        calls: std::sync::Mutex<Vec<usize>>,
    }

    impl CountingSink {
        fn new() -> Self {
            Self {
                calls: std::sync::Mutex::new(Vec::new()),
            }
        }
        fn call_count(&self) -> usize {
            self.calls.lock().unwrap().len()
        }
    }

    #[async_trait]
    impl Sink for CountingSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            self.calls.lock().unwrap().push(records.len());
            Ok(records.len())
        }
    }

    #[tokio::test]
    async fn pipeline_run_drives_stream_pages() {
        let source = PagedSource;
        let sink = CountingSink::new();

        let result = Pipeline::new(&source, &sink).run().await.unwrap();

        // Three pages of one record each → three sink calls, three records.
        assert_eq!(sink.call_count(), 3);
        assert_eq!(result.records_written, 3);
        assert_eq!(result.bookmark, Some(json!("final")));
    }

    #[tokio::test]
    async fn pipeline_with_file_state_store_round_trips_across_runs() {
        let dir = TempDir::new().unwrap();
        let store: Arc<dyn StateStore> = Arc::new(FileStateStore::new(dir.path()));

        // Run 1: nothing stored yet, persist new bookmark.
        let s1 = StatefulSource::new("k", vec![json!({"i": 1})], json!("v1"));
        let sink1 = MockSink::new();
        Pipeline::new(&s1, &sink1)
            .with_state_store(Arc::clone(&store))
            .run()
            .await
            .unwrap();
        assert_eq!(s1.observed_start(), None);
        assert_eq!(store.get("k").await.unwrap(), Some(json!("v1")));

        // Run 2: resume from v1, persist v2.
        let s2 = StatefulSource::new("k", vec![json!({"i": 2})], json!("v2"));
        let sink2 = MockSink::new();
        Pipeline::new(&s2, &sink2)
            .with_state_store(Arc::clone(&store))
            .run()
            .await
            .unwrap();
        assert_eq!(s2.observed_start(), Some(json!("v1")));
        assert_eq!(store.get("k").await.unwrap(), Some(json!("v2")));
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn pipeline_run_increments_runs_total() {
        use crate::observability::decorator::source_tests::{LOCK, snapshotter};
        use metrics_util::debugging::DebugValue;
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();

        let source = MockSource(vec![json!({"i": 1})]);
        let sink = MockSink::new();
        let _ = Pipeline::new(&source, &sink)
            .with_name("test-pipeline")
            .with_row("rowA")
            .run()
            .await
            .unwrap();

        let snapshot = snap.snapshot();
        let found = snapshot.into_vec().into_iter().any(
            |(key, _u, _d, v): (metrics_util::CompositeKey, _, _, _)| {
                key.key().name() == "faucet_pipeline_runs_total"
                    && key.key().labels().any(|l: &metrics::Label| {
                        l.key() == "pipeline" && l.value() == "test-pipeline"
                    })
                    && key
                        .key()
                        .labels()
                        .any(|l: &metrics::Label| l.key() == "row" && l.value() == "rowA")
                    && key
                        .key()
                        .labels()
                        .any(|l: &metrics::Label| l.key() == "status" && l.value() == "ok")
                    && matches!(v, DebugValue::Counter(c) if c >= 1)
            },
        );
        assert!(
            found,
            "expected faucet_pipeline_runs_total{{pipeline=test-pipeline, row=rowA, status=ok}}"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn pipeline_failure_attaches_kind_label_to_runs_total() {
        use crate::observability::decorator::source_tests::{LOCK, snapshotter};
        use metrics_util::debugging::DebugValue;
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();

        let source = FailingSource;
        let sink = MockSink::new();
        let _ = Pipeline::new(&source, &sink)
            .with_name("err-pipeline")
            .with_row("rowE")
            .run()
            .await;

        let snapshot = snap.snapshot();
        let found = snapshot.into_vec().into_iter().any(
            |(key, _u, _d, v): (metrics_util::CompositeKey, _, _, _)| {
                key.key().name() == "faucet_pipeline_runs_total"
                    && key.key().labels().any(|l: &metrics::Label| {
                        l.key() == "pipeline" && l.value() == "err-pipeline"
                    })
                    && key
                        .key()
                        .labels()
                        .any(|l: &metrics::Label| l.key() == "status" && l.value() == "err")
                    && key
                        .key()
                        .labels()
                        .any(|l: &metrics::Label| l.key() == "kind" && l.value() == "Auth")
                    && matches!(v, DebugValue::Counter(c) if c >= 1)
            },
        );
        assert!(
            found,
            "expected faucet_pipeline_runs_total{{status=err, kind=Auth}} for failing source"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn pipeline_run_emits_start_time_gauge() {
        use crate::observability::decorator::source_tests::{LOCK, snapshotter};
        use metrics_util::debugging::DebugValue;
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();

        let source = MockSource(vec![json!({"i": 1})]);
        let sink = MockSink::new();
        let before = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        let _ = Pipeline::new(&source, &sink)
            .with_name("start-time-pipeline")
            .with_row("rowS")
            .run()
            .await
            .unwrap();

        let snapshot = snap.snapshot();
        let found = snapshot.into_vec().into_iter().any(
            |(key, _u, _d, v): (metrics_util::CompositeKey, _, _, _)| {
                if key.key().name() != "faucet_pipeline_start_time_unix_seconds" {
                    return false;
                }
                let labels_match = key.key().labels().any(|l: &metrics::Label| {
                    l.key() == "pipeline" && l.value() == "start-time-pipeline"
                }) && key
                    .key()
                    .labels()
                    .any(|l: &metrics::Label| l.key() == "row" && l.value() == "rowS");
                if !labels_match {
                    return false;
                }
                matches!(v, DebugValue::Gauge(g) if g.into_inner() >= before)
            },
        );
        assert!(
            found,
            "expected faucet_pipeline_start_time_unix_seconds gauge >= test-start timestamp"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn register_build_info_sets_version_gauge() {
        use crate::observability::decorator::source_tests::{LOCK, snapshotter};
        use metrics_util::debugging::DebugValue;
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();

        crate::observability::register_build_info();

        let snapshot = snap.snapshot();
        let found = snapshot.into_vec().into_iter().any(
            |(key, _u, _d, v): (metrics_util::CompositeKey, _, _, _)| {
                key.key().name() == "faucet_build_info"
                    && key.key().labels().any(|l: &metrics::Label| {
                        l.key() == "version" && l.value() == env!("CARGO_PKG_VERSION")
                    })
                    && matches!(v, DebugValue::Gauge(g) if (g.into_inner() - 1.0).abs() < f64::EPSILON)
            },
        );
        assert!(
            found,
            "expected faucet_build_info{{version=CARGO_PKG_VERSION}} = 1.0 after register_build_info()"
        );
    }

    // ── DLQ routing tests ──────────────────────────────────────────────────

    use crate::dlq::{DlqConfig, OnBatchError};

    /// Sink that returns mixed per-row outcomes: failure indices come from
    /// the constructor; everything else succeeds. Captures the rows that
    /// *would* have committed to the main sink.
    struct PartialSink {
        fail_indices: std::sync::Mutex<Vec<usize>>,
        committed: std::sync::Mutex<Vec<Value>>,
    }

    impl PartialSink {
        fn new(fail_indices: Vec<usize>) -> Self {
            Self {
                fail_indices: std::sync::Mutex::new(fail_indices),
                committed: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl Sink for PartialSink {
        async fn write_batch(&self, _records: &[Value]) -> Result<usize, FaucetError> {
            unreachable!("PartialSink only overrides write_batch_partial");
        }
        async fn write_batch_partial(
            &self,
            records: &[Value],
        ) -> Result<Vec<crate::traits::RowOutcome>, FaucetError> {
            let fails: std::collections::HashSet<usize> =
                self.fail_indices.lock().unwrap().iter().copied().collect();
            let mut outcomes = Vec::with_capacity(records.len());
            for (i, rec) in records.iter().enumerate() {
                if fails.contains(&i) {
                    outcomes.push(Err(FaucetError::Sink(format!("row {i} rejected"))));
                } else {
                    self.committed.lock().unwrap().push(rec.clone());
                    outcomes.push(Ok(()));
                }
            }
            Ok(outcomes)
        }
    }

    #[tokio::test]
    async fn dlq_routes_only_failed_rows_for_partial_success_sink() {
        let main = PartialSink::new(vec![1, 3]); // 4 rows, indices 1 and 3 fail
        let dlq = std::sync::Arc::new(MockSink::new());
        let dlq_cfg = DlqConfig::new(dlq.clone());

        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: (0..4).map(|i| json!({"i": i})).collect(),
            bookmark: None,
        })];
        let stream = futures::stream::iter(pages);
        let result = run_stream(stream, &main, RunStreamOptions::new().with_dlq(dlq_cfg))
            .await
            .unwrap();

        assert_eq!(result.records_written, 2); // 0 and 2 committed
        assert_eq!(main.committed.lock().unwrap().len(), 2);
        let envelopes = dlq.0.lock().unwrap();
        assert_eq!(envelopes.len(), 2);
        assert_eq!(envelopes[0]["payload"]["i"], 1);
        assert_eq!(envelopes[0]["record_index"], 1);
        assert_eq!(envelopes[1]["payload"]["i"], 3);
        assert_eq!(envelopes[1]["record_index"], 3);
        let stats = result.dlq.unwrap();
        assert_eq!(stats.records_dlq, 2);
        assert_eq!(stats.pages_with_failures, 1);
    }

    #[cfg(feature = "masking")]
    #[tokio::test]
    async fn masking_runs_before_the_sink() {
        use crate::masking::{CompiledMasking, MaskingSpec};
        let sink = MockSink::new();
        let spec: MaskingSpec = serde_json::from_value(json!({
            "rules": [{ "match": { "value_detector": "email" },
                        "action": { "type": "redact" } }]
        }))
        .unwrap();
        let m = std::sync::Arc::new(CompiledMasking::compile(&spec).unwrap());
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: vec![json!({"email": "a@b.com", "name": "Al"})],
            bookmark: None,
        })];
        run_stream(
            futures::stream::iter(pages),
            &sink,
            RunStreamOptions::new().with_masking(m),
        )
        .await
        .unwrap();
        assert_eq!(sink.written()[0], json!({"email": "***", "name": "Al"}));
    }

    #[cfg(feature = "masking")]
    #[tokio::test]
    async fn masking_applies_before_the_dlq_envelope() {
        // The headline correctness claim: PII must be masked before it reaches
        // *any* sink — including the DLQ. Row 0 fails at the sink and is routed
        // to the DLQ; its envelope payload must already be masked.
        use crate::masking::{CompiledMasking, MaskingSpec};
        let main = PartialSink::new(vec![0]); // row 0 fails → DLQ
        let dlq = std::sync::Arc::new(MockSink::new());
        let spec: MaskingSpec = serde_json::from_value(json!({
            "rules": [{ "match": { "value_detector": "email" },
                        "action": { "type": "redact" } }]
        }))
        .unwrap();
        let m = std::sync::Arc::new(CompiledMasking::compile(&spec).unwrap());
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: vec![
                json!({"email": "secret@x.com"}),
                json!({"email": "ok@y.com"}),
            ],
            bookmark: None,
        })];
        let opts = RunStreamOptions::new()
            .with_masking(m)
            .with_dlq(DlqConfig::new(dlq.clone()));
        run_stream(futures::stream::iter(pages), &main, opts)
            .await
            .unwrap();
        // Row 0 → DLQ, masked; row 1 → committed to the main sink, masked.
        let env = dlq.0.lock().unwrap();
        assert_eq!(env.len(), 1);
        assert_eq!(
            env[0]["payload"]["email"], "***",
            "the DLQ payload must be masked, not raw PII"
        );
        assert_eq!(main.committed.lock().unwrap()[0]["email"], "***");
    }

    #[tokio::test]
    async fn dlq_propagate_policy_bubbles_outer_err() {
        let main = FailingSink;
        let dlq = std::sync::Arc::new(MockSink::new());
        let mut dlq_cfg = DlqConfig::new(dlq.clone());
        dlq_cfg.on_batch_error = OnBatchError::Propagate;

        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: vec![json!({"i": 0}), json!({"i": 1})],
            bookmark: Some(json!("v1")),
        })];
        let stream = futures::stream::iter(pages);
        let store: std::sync::Arc<dyn StateStore> = std::sync::Arc::new(MemoryStateStore::new());
        let result = run_stream(
            stream,
            &main,
            RunStreamOptions::new()
                .with_dlq(dlq_cfg)
                .with_state(std::sync::Arc::clone(&store), "k"),
        )
        .await;
        assert!(matches!(result, Err(FaucetError::Sink(_))));
        assert!(dlq.0.lock().unwrap().is_empty());
        // Bookmark must NOT be persisted on a propagated failure.
        assert!(store.get("k").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn dlq_dlq_all_policy_routes_every_row_on_outer_err() {
        let main = FailingSink;
        let dlq = std::sync::Arc::new(MockSink::new());
        let mut dlq_cfg = DlqConfig::new(dlq.clone());
        dlq_cfg.on_batch_error = OnBatchError::DlqAll;

        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: vec![json!({"i": 0}), json!({"i": 1}), json!({"i": 2})],
            bookmark: Some(json!("v1")),
        })];
        let stream = futures::stream::iter(pages);
        let store: std::sync::Arc<dyn StateStore> = std::sync::Arc::new(MemoryStateStore::new());
        let result = run_stream(
            stream,
            &main,
            RunStreamOptions::new()
                .with_dlq(dlq_cfg)
                .with_state(std::sync::Arc::clone(&store), "k"),
        )
        .await
        .unwrap();
        assert_eq!(result.records_written, 0);
        {
            let envelopes = dlq.0.lock().unwrap();
            assert_eq!(envelopes.len(), 3);
            // Every envelope's error.message includes the underlying message.
            for env in envelopes.iter() {
                let msg = env["error"]["message"].as_str().unwrap();
                assert!(msg.contains("write failed"), "got: {msg}");
            }
        }
        assert_eq!(store.get("k").await.unwrap(), Some(json!("v1")));
        assert_eq!(result.dlq.unwrap().records_dlq, 3);
    }

    #[tokio::test]
    async fn dlq_per_page_budget_exceeded_aborts() {
        let main = PartialSink::new(vec![0, 1, 2]);
        let dlq = std::sync::Arc::new(MockSink::new());
        let mut dlq_cfg = DlqConfig::new(dlq.clone());
        dlq_cfg.max_failures_per_page = Some(2);

        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: (0..3).map(|i| json!({"i": i})).collect(),
            bookmark: None,
        })];
        let stream = futures::stream::iter(pages);
        let result = run_stream(stream, &main, RunStreamOptions::new().with_dlq(dlq_cfg)).await;
        assert!(
            matches!(&result, Err(FaucetError::Sink(m)) if m.contains("per-page budget exceeded")),
            "got: {result:?}"
        );
    }

    #[tokio::test]
    async fn dlq_total_budget_exceeded_aborts_on_later_page() {
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![
            Ok(StreamPage {
                records: (0..3).map(|i| json!({"i": i})).collect(),
                bookmark: None,
            }),
            Ok(StreamPage {
                records: (3..6).map(|i| json!({"i": i})).collect(),
                bookmark: None,
            }),
        ];
        // Fail every row across both pages.
        let main = PartialSink::new(vec![0, 1, 2]); // applied per page
        let dlq = std::sync::Arc::new(MockSink::new());
        let mut dlq_cfg = DlqConfig::new(dlq.clone());
        dlq_cfg.max_failures_total = Some(4);

        let stream = futures::stream::iter(pages);
        let result = run_stream(stream, &main, RunStreamOptions::new().with_dlq(dlq_cfg)).await;
        assert!(
            matches!(&result, Err(FaucetError::Sink(m)) if m.contains("total budget exceeded")),
            "got: {result:?}"
        );
    }

    #[tokio::test]
    async fn dlq_per_page_budget_exceeded_commits_page_before_aborting() {
        // M4 (#146): write_batch_partial already commits the survivors to the
        // main sink. When the per-page budget then trips, the run must finish
        // committing the page — route its failures to the DLQ and persist the
        // bookmark — BEFORE aborting, so the committed survivors do NOT
        // re-deliver on the next run and the failed rows are not lost.
        let main = PartialSink::new(vec![1, 2]); // rows 1,2 fail; row 0 commits
        let dlq = std::sync::Arc::new(MockSink::new());
        let mut dlq_cfg = DlqConfig::new(dlq.clone());
        dlq_cfg.max_failures_per_page = Some(1); // 2 failures > 1 → trips

        let store: std::sync::Arc<dyn StateStore> = std::sync::Arc::new(MemoryStateStore::new());
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: (0..3).map(|i| json!({ "i": i })).collect(),
            bookmark: Some(json!("v1")),
        })];
        let stream = futures::stream::iter(pages);
        let result = run_stream(
            stream,
            &main,
            RunStreamOptions::new()
                .with_dlq(dlq_cfg)
                .with_state(std::sync::Arc::clone(&store), "k"),
        )
        .await;

        // Run still aborts with the budget error.
        assert!(
            matches!(&result, Err(FaucetError::Sink(m)) if m.contains("per-page budget exceeded")),
            "got: {result:?}"
        );
        // The survivor (row 0) was committed to the main sink.
        assert_eq!(main.committed.lock().unwrap().len(), 1);
        // The two failures were routed to the DLQ (not lost on abort).
        assert_eq!(dlq.0.lock().unwrap().len(), 2);
        // The bookmark was persisted, so the survivor will NOT re-deliver.
        assert_eq!(store.get("k").await.unwrap(), Some(json!("v1")));
    }

    #[tokio::test]
    async fn dlq_total_budget_exceeded_commits_tripping_page_before_aborting() {
        // M4 (#146): same guarantee for the cumulative total budget — the page
        // that crosses the threshold is committed fully (failures→DLQ, bookmark
        // persisted) before the run aborts.
        let main = PartialSink::new(vec![1, 2]); // rows 1,2 fail; row 0 commits
        let dlq = std::sync::Arc::new(MockSink::new());
        let mut dlq_cfg = DlqConfig::new(dlq.clone());
        dlq_cfg.max_failures_total = Some(1); // 2 failures > 1 → trips

        let store: std::sync::Arc<dyn StateStore> = std::sync::Arc::new(MemoryStateStore::new());
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: (0..3).map(|i| json!({ "i": i })).collect(),
            bookmark: Some(json!("v1")),
        })];
        let stream = futures::stream::iter(pages);
        let result = run_stream(
            stream,
            &main,
            RunStreamOptions::new()
                .with_dlq(dlq_cfg)
                .with_state(std::sync::Arc::clone(&store), "k"),
        )
        .await;

        assert!(
            matches!(&result, Err(FaucetError::Sink(m)) if m.contains("total budget exceeded")),
            "got: {result:?}"
        );
        assert_eq!(main.committed.lock().unwrap().len(), 1);
        assert_eq!(dlq.0.lock().unwrap().len(), 2);
        assert_eq!(store.get("k").await.unwrap(), Some(json!("v1")));
    }

    /// DLQ sink that always fails. Used to assert the router does not
    /// recurse into itself.
    struct FailingDlqSink;
    #[async_trait]
    impl Sink for FailingDlqSink {
        async fn write_batch(&self, _records: &[Value]) -> Result<usize, FaucetError> {
            Err(FaucetError::Sink("dlq disk full".into()))
        }
    }

    /// DLQ sink that succeeds on write but fails on flush. Used to assert
    /// the router wraps DLQ flush errors and bails without persisting the
    /// bookmark.
    struct FailingFlushDlqSink {
        written: std::sync::Mutex<Vec<Value>>,
    }
    impl FailingFlushDlqSink {
        fn new() -> Self {
            Self {
                written: std::sync::Mutex::new(Vec::new()),
            }
        }
    }
    #[async_trait]
    impl Sink for FailingFlushDlqSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            self.written.lock().unwrap().extend(records.iter().cloned());
            Ok(records.len())
        }
        async fn flush(&self) -> Result<(), FaucetError> {
            Err(FaucetError::Sink("dlq flush failed".into()))
        }
    }

    #[tokio::test]
    async fn dlq_sink_failure_is_fatal_no_recursion() {
        let main = PartialSink::new(vec![0]);
        let dlq: std::sync::Arc<dyn Sink> = std::sync::Arc::new(FailingDlqSink);
        let dlq_cfg = DlqConfig::new(dlq);

        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: vec![json!({"i": 0}), json!({"i": 1})],
            bookmark: Some(json!("v1")),
        })];
        let stream = futures::stream::iter(pages);
        let store: std::sync::Arc<dyn StateStore> = std::sync::Arc::new(MemoryStateStore::new());
        let result = run_stream(
            stream,
            &main,
            RunStreamOptions::new()
                .with_dlq(dlq_cfg)
                .with_state(std::sync::Arc::clone(&store), "k"),
        )
        .await;
        assert!(
            matches!(&result, Err(FaucetError::Sink(m)) if m.contains("DLQ sink write failed")),
            "got: {result:?}"
        );
        assert!(store.get("k").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn dlq_bookmark_advances_only_after_both_flushes() {
        let main = PartialSink::new(vec![1]); // row 1 fails, row 0 commits
        let dlq = std::sync::Arc::new(MockSink::new());
        let dlq_cfg = DlqConfig::new(dlq.clone());

        let store: std::sync::Arc<dyn StateStore> = std::sync::Arc::new(MemoryStateStore::new());
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: vec![json!({"i": 0}), json!({"i": 1})],
            bookmark: Some(json!("v1")),
        })];
        let stream = futures::stream::iter(pages);
        run_stream(
            stream,
            &main,
            RunStreamOptions::new()
                .with_dlq(dlq_cfg)
                .with_state(std::sync::Arc::clone(&store), "k"),
        )
        .await
        .unwrap();
        assert_eq!(store.get("k").await.unwrap(), Some(json!("v1")));
        assert_eq!(dlq.0.lock().unwrap().len(), 1);
        assert_eq!(main.committed.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn dlq_disabled_pipeline_behaves_identically_to_today() {
        // Regression guard: omitting DLQ keeps existing behavior bit-identical.
        let main = MockSink::new();
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: vec![json!({"i": 0}), json!({"i": 1})],
            bookmark: None,
        })];
        let stream = futures::stream::iter(pages);
        let result = run_stream(stream, &main, RunStreamOptions::new())
            .await
            .unwrap();
        assert_eq!(result.records_written, 2);
        assert!(result.dlq.is_none());
    }

    #[tokio::test]
    async fn dlq_per_page_flush_failure_is_fatal_and_blocks_bookmark() {
        // Per-page flush path: page carries a bookmark, row 1 fails, the
        // DLQ write succeeds but the DLQ flush at the bookmark gate errors.
        // The pipeline must bail with "DLQ sink flush failed" and the
        // bookmark must NOT be persisted.
        let main = PartialSink::new(vec![1]);
        let dlq: std::sync::Arc<dyn Sink> = std::sync::Arc::new(FailingFlushDlqSink::new());
        let dlq_cfg = DlqConfig::new(dlq);

        let store: std::sync::Arc<dyn StateStore> = std::sync::Arc::new(MemoryStateStore::new());
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: vec![json!({"i": 0}), json!({"i": 1})],
            bookmark: Some(json!("v1")),
        })];
        let stream = futures::stream::iter(pages);
        let result = run_stream(
            stream,
            &main,
            RunStreamOptions::new()
                .with_dlq(dlq_cfg)
                .with_state(std::sync::Arc::clone(&store), "k"),
        )
        .await;
        assert!(
            matches!(&result, Err(FaucetError::Sink(m)) if m.contains("DLQ sink flush failed")),
            "got: {result:?}"
        );
        assert!(store.get("k").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn dlq_end_of_stream_flush_failure_is_fatal() {
        // End-of-stream flush path: no page carries a bookmark, but DLQ
        // received envelopes during the run. The final post-loop flush of
        // the DLQ sink errors. The pipeline must bail with "DLQ sink flush
        // failed".
        let main = PartialSink::new(vec![1]);
        let dlq: std::sync::Arc<dyn Sink> = std::sync::Arc::new(FailingFlushDlqSink::new());
        let dlq_cfg = DlqConfig::new(dlq);

        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: vec![json!({"i": 0}), json!({"i": 1})],
            bookmark: None,
        })];
        let stream = futures::stream::iter(pages);
        let result = run_stream(stream, &main, RunStreamOptions::new().with_dlq(dlq_cfg)).await;
        assert!(
            matches!(&result, Err(FaucetError::Sink(m)) if m.contains("DLQ sink flush failed")),
            "got: {result:?}"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn dlq_emits_records_total_and_pages_total() {
        use crate::observability::decorator::source_tests::{LOCK, snapshotter};
        use metrics_util::debugging::DebugValue;

        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();

        let source = MockSource(vec![json!({"i": 0}), json!({"i": 1})]);
        let main = PartialSink::new(vec![1]);
        let dlq = std::sync::Arc::new(MockSink::new());
        let _ = Pipeline::new(&source, &main)
            .with_name("p_dlq_metrics")
            .with_row("r1")
            .with_dlq(DlqConfig::new(dlq.clone()))
            .run()
            .await
            .unwrap();

        let snapshot = snap.snapshot();
        let mut saw_records = false;
        let mut saw_pages = false;
        for (k, _u, _d, v) in snapshot.into_vec() {
            let key = k.key();
            let labels = key.labels().collect::<Vec<_>>();
            let has = |k: &str, v: &str| labels.iter().any(|l| l.key() == k && l.value() == v);
            if key.name() == "faucet_sink_dlq_records_total"
                && has("pipeline", "p_dlq_metrics")
                && has("row", "r1")
                && matches!(v, DebugValue::Counter(c) if c >= 1)
            {
                saw_records = true;
            }
            if key.name() == "faucet_sink_dlq_pages_total"
                && has("pipeline", "p_dlq_metrics")
                && matches!(v, DebugValue::Counter(c) if c >= 1)
            {
                saw_pages = true;
            }
        }
        assert!(saw_records, "faucet_sink_dlq_records_total not emitted");
        assert!(saw_pages, "faucet_sink_dlq_pages_total not emitted");
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn dlq_budget_exceeded_emits_counter() {
        use crate::observability::decorator::source_tests::{LOCK, snapshotter};
        use metrics_util::debugging::DebugValue;

        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();

        let source = MockSource((0..3).map(|i| json!({"i": i})).collect());
        let main = PartialSink::new(vec![0, 1, 2]);
        let dlq = std::sync::Arc::new(MockSink::new());
        let mut cfg = DlqConfig::new(dlq);
        cfg.max_failures_per_page = Some(1);
        let _ = Pipeline::new(&source, &main)
            .with_name("p_budget")
            .with_dlq(cfg)
            .run()
            .await;

        let snapshot = snap.snapshot();
        let saw = snapshot.into_vec().into_iter().any(|(k, _, _, v)| {
            k.key().name() == "faucet_sink_dlq_budget_exceeded_total"
                && k.key()
                    .labels()
                    .any(|l| l.key() == "scope" && l.value() == "per_page")
                && matches!(v, DebugValue::Counter(c) if c >= 1)
        });
        assert!(saw, "faucet_sink_dlq_budget_exceeded_total not emitted");
    }

    #[tokio::test]
    async fn pipeline_run_with_dlq_routes_partial_failures_end_to_end() {
        // Source: 3 records. Main sink: fails index 1. DLQ: in-memory.
        let source = MockSource(vec![json!({"i": 0}), json!({"i": 1}), json!({"i": 2})]);
        let main = PartialSink::new(vec![1]);
        let dlq = std::sync::Arc::new(MockSink::new());

        let result = Pipeline::new(&source, &main)
            .with_dlq(DlqConfig::new(dlq.clone()))
            .run()
            .await
            .unwrap();

        assert_eq!(result.records_written, 2);
        let stats = result.dlq.unwrap();
        assert_eq!(stats.records_dlq, 1);
        {
            let dlq_records = dlq.0.lock().unwrap();
            assert_eq!(dlq_records.len(), 1);
        }
    }

    // ── Quality routing tests ──────────────────────────────────────────────

    #[cfg(feature = "quality")]
    #[tokio::test]
    async fn quality_quarantines_to_dlq_and_writes_survivors() {
        use crate::dlq::DlqConfig;
        use crate::quality::{CompiledQuality, OnFailure, QualitySpec, RecordCheck};

        let main = Arc::new(MockSink::new());
        let dlq_sink = Arc::new(MockSink::new());
        let spec = QualitySpec {
            record: vec![RecordCheck::NotNull {
                field: "id".into(),
                treat_missing_as_null: true,
                on_failure: OnFailure::Quarantine,
            }],
            batch: vec![],
        };
        let quality = Arc::new(CompiledQuality::compile(&spec).unwrap());
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: vec![json!({"id": 1}), json!({"id": null}), json!({"id": 3})],
            bookmark: None,
        })];
        let opts = RunStreamOptions::new()
            .with_dlq(DlqConfig::new(dlq_sink.clone()))
            .with_quality(quality);
        let result = run_stream(futures::stream::iter(pages), main.as_ref(), opts)
            .await
            .unwrap();

        assert_eq!(result.records_written, 2); // survivors
        assert_eq!(main.written(), vec![json!({"id": 1}), json!({"id": 3})]);
        // one quarantined record reached the DLQ with a QualityFailure envelope
        let dlq = dlq_sink.written();
        assert_eq!(dlq.len(), 1);
        assert_eq!(dlq[0]["error"]["kind"], "QualityFailure");
        assert_eq!(result.dlq.unwrap().records_dlq, 1);
    }

    #[cfg(feature = "quality")]
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn quality_only_page_emits_quality_reason() {
        // A page whose DLQ traffic is entirely quality-sourced (the main sink
        // accepts every survivor) must label `faucet_sink_dlq_pages_total`
        // with `reason="quality"`, not `partial`.
        use crate::dlq::DlqConfig;
        use crate::observability::decorator::source_tests::{LOCK, snapshotter};
        use crate::quality::{CompiledQuality, OnFailure, QualitySpec, RecordCheck};
        use metrics_util::debugging::DebugValue;

        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();

        // MockSink accepts everything → no sink-side failures, so the only DLQ
        // traffic comes from the quality quarantine.
        let main = Arc::new(MockSink::new());
        let dlq_sink = Arc::new(MockSink::new());
        let spec = QualitySpec {
            record: vec![RecordCheck::NotNull {
                field: "id".into(),
                treat_missing_as_null: true,
                on_failure: OnFailure::Quarantine,
            }],
            batch: vec![],
        };
        let quality = Arc::new(CompiledQuality::compile(&spec).unwrap());
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: vec![json!({"id": 1}), json!({"id": null}), json!({"id": 3})],
            bookmark: None,
        })];
        let opts = RunStreamOptions::new()
            .with_name("p_quality_reason")
            .with_dlq(DlqConfig::new(dlq_sink.clone()))
            .with_quality(quality);
        let _ = run_stream(futures::stream::iter(pages), main.as_ref(), opts)
            .await
            .unwrap();

        let snapshot = snap.snapshot();
        let saw_quality_reason = snapshot.into_vec().into_iter().any(|(k, _, _, v)| {
            k.key().name() == "faucet_sink_dlq_pages_total"
                && k.key()
                    .labels()
                    .any(|l| l.key() == "pipeline" && l.value() == "p_quality_reason")
                && k.key()
                    .labels()
                    .any(|l| l.key() == "reason" && l.value() == "quality")
                && matches!(v, DebugValue::Counter(c) if c >= 1)
        });
        assert!(
            saw_quality_reason,
            "expected faucet_sink_dlq_pages_total with reason=\"quality\""
        );
    }

    #[cfg(feature = "quality")]
    #[tokio::test]
    async fn quality_abort_fails_run() {
        use crate::quality::{BatchCheck, CompiledQuality, OnFailure, QualitySpec};
        let main = MockSink::new();
        let spec = QualitySpec {
            record: vec![],
            batch: vec![BatchCheck::RowCount {
                min: Some(5),
                max: None,
                on_failure: OnFailure::Abort,
            }],
        };
        let quality = Arc::new(CompiledQuality::compile(&spec).unwrap());
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: vec![json!({"id": 1})],
            bookmark: None,
        })];
        let opts = RunStreamOptions::new().with_quality(quality);
        let result = run_stream(futures::stream::iter(pages), &main, opts).await;
        assert!(matches!(result, Err(FaucetError::QualityFailure { .. })));
    }

    #[cfg(feature = "quality")]
    #[tokio::test]
    async fn quality_quarantine_without_dlq_is_rejected() {
        use crate::quality::{CompiledQuality, OnFailure, QualitySpec, RecordCheck};
        let main = MockSink::new();
        let spec = QualitySpec {
            record: vec![RecordCheck::NotNull {
                field: "id".into(),
                treat_missing_as_null: true,
                on_failure: OnFailure::Quarantine,
            }],
            batch: vec![],
        };
        let quality = Arc::new(CompiledQuality::compile(&spec).unwrap());
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: vec![json!({"id": null})],
            bookmark: None,
        })];
        // No .with_dlq(...) — must be rejected up front.
        let opts = RunStreamOptions::new().with_quality(quality);
        let result = run_stream(futures::stream::iter(pages), &main, opts).await;
        assert!(matches!(result, Err(FaucetError::Config(_))));
    }

    #[cfg(feature = "contract")]
    fn compiled_contract(on_breach: &str) -> Arc<crate::contract::CompiledContract> {
        let spec: crate::contract::ContractSpec = serde_json::from_value(json!({
            "version": "1.0.0",
            "on_breach": on_breach,
            "fields": [{ "name": "id", "type": "integer" }]
        }))
        .unwrap();
        Arc::new(crate::contract::CompiledContract::compile(&spec).unwrap())
    }

    #[cfg(feature = "contract")]
    #[tokio::test]
    async fn contract_quarantines_to_dlq_and_writes_survivors() {
        use crate::dlq::DlqConfig;
        let main = Arc::new(MockSink::new());
        let dlq_sink = Arc::new(MockSink::new());
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: vec![json!({"id": 1}), json!({"id": "bad"}), json!({"id": 3})],
            bookmark: None,
        })];
        let opts = RunStreamOptions::new()
            .with_dlq(DlqConfig::new(dlq_sink.clone()))
            .with_contract(compiled_contract("quarantine"));
        let result = run_stream(futures::stream::iter(pages), main.as_ref(), opts)
            .await
            .unwrap();

        assert_eq!(result.records_written, 2);
        assert_eq!(main.written(), vec![json!({"id": 1}), json!({"id": 3})]);
        let dlq = dlq_sink.written();
        assert_eq!(dlq.len(), 1);
        assert_eq!(dlq[0]["error"]["kind"], "ContractViolation");
        assert_eq!(dlq[0]["payload"], json!({"id": "bad"}));
        // record_index is the position within the page (frozen contract).
        assert_eq!(dlq[0]["record_index"], 1);
        assert_eq!(result.dlq.unwrap().records_dlq, 1);
    }

    #[cfg(feature = "contract")]
    #[tokio::test]
    async fn contract_fail_aborts_run_and_writes_nothing() {
        let main = MockSink::new();
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: vec![json!({"id": 1}), json!({"id": "bad"})],
            bookmark: None,
        })];
        let opts = RunStreamOptions::new().with_contract(compiled_contract("fail"));
        let result = run_stream(futures::stream::iter(pages), &main, opts).await;
        match result {
            Err(FaucetError::ContractViolation { version, message }) => {
                assert_eq!(version, "1.0.0");
                assert!(message.contains("id"), "message: {message}");
            }
            other => panic!("expected ContractViolation, got {other:?}"),
        }
        assert!(
            main.written().is_empty(),
            "a contract fail must not commit any of the page's records"
        );
    }

    #[cfg(feature = "contract")]
    #[tokio::test]
    async fn contract_warn_writes_everything() {
        let main = MockSink::new();
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: vec![json!({"id": 1}), json!({"id": "bad"})],
            bookmark: None,
        })];
        let opts = RunStreamOptions::new().with_contract(compiled_contract("warn"));
        let result = run_stream(futures::stream::iter(pages), &main, opts)
            .await
            .unwrap();
        assert_eq!(result.records_written, 2);
        assert_eq!(main.written(), vec![json!({"id": 1}), json!({"id": "bad"})]);
    }

    #[cfg(feature = "contract")]
    #[tokio::test]
    async fn contract_quarantine_without_dlq_is_rejected() {
        let main = MockSink::new();
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: vec![json!({"id": 1})],
            bookmark: None,
        })];
        // No .with_dlq(...) — must be rejected up front.
        let opts = RunStreamOptions::new().with_contract(compiled_contract("quarantine"));
        let result = run_stream(futures::stream::iter(pages), &main, opts).await;
        assert!(matches!(result, Err(FaucetError::Config(_))));
    }

    #[cfg(all(feature = "contract", feature = "quality"))]
    #[tokio::test]
    async fn contract_runs_after_quality_and_shares_dlq() {
        // Quality quarantines the null id; the contract then quarantines the
        // string id from the quality survivors. Both envelopes land in the
        // same DLQ write, each with its own error kind.
        use crate::dlq::DlqConfig;
        use crate::quality::{CompiledQuality, OnFailure, QualitySpec, RecordCheck};
        let main = Arc::new(MockSink::new());
        let dlq_sink = Arc::new(MockSink::new());
        let quality = Arc::new(
            CompiledQuality::compile(&QualitySpec {
                record: vec![RecordCheck::NotNull {
                    field: "id".into(),
                    treat_missing_as_null: true,
                    on_failure: OnFailure::Quarantine,
                }],
                batch: vec![],
            })
            .unwrap(),
        );
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: vec![json!({"id": null}), json!({"id": "bad"}), json!({"id": 3})],
            bookmark: None,
        })];
        let opts = RunStreamOptions::new()
            .with_dlq(DlqConfig::new(dlq_sink.clone()))
            .with_quality(quality)
            .with_contract(compiled_contract("quarantine"));
        let result = run_stream(futures::stream::iter(pages), main.as_ref(), opts)
            .await
            .unwrap();

        assert_eq!(result.records_written, 1);
        assert_eq!(main.written(), vec![json!({"id": 3})]);
        let dlq = dlq_sink.written();
        assert_eq!(dlq.len(), 2);
        let kinds: Vec<&str> = dlq
            .iter()
            .map(|e| e["error"]["kind"].as_str().unwrap())
            .collect();
        assert!(kinds.contains(&"QualityFailure"), "kinds: {kinds:?}");
        assert!(kinds.contains(&"ContractViolation"), "kinds: {kinds:?}");
        assert_eq!(result.dlq.unwrap().records_dlq, 2);

        // #466 M1: the contract envelope's `record_index` must be the position in
        // the ORIGINAL page (1 — the `"bad"` record), not its position within the
        // quality-survivor slice (0, after the null id at page-index 0 was
        // removed). Before the fix this reported 0, silently pointing DLQ readers
        // at the wrong original row whenever quality also quarantined earlier.
        let contract_env = dlq
            .iter()
            .find(|e| e["error"]["kind"] == "ContractViolation")
            .expect("a contract envelope");
        assert_eq!(
            contract_env["record_index"], 1,
            "contract record_index must be the original page position: {contract_env}"
        );
        let quality_env = dlq
            .iter()
            .find(|e| e["error"]["kind"] == "QualityFailure")
            .expect("a quality envelope");
        assert_eq!(
            quality_env["record_index"], 0,
            "null id was at page index 0"
        );
    }

    /// Sink whose write_batch_partial fails every Nth record; drives the
    /// error-rate signal. Requires a DLQ in run_stream.
    struct FlakySink {
        every: usize,
        calls: std::sync::Mutex<Vec<usize>>,
    }
    impl FlakySink {
        fn new(every: usize) -> Self {
            Self {
                every,
                calls: std::sync::Mutex::new(Vec::new()),
            }
        }
        fn call_sizes(&self) -> Vec<usize> {
            self.calls.lock().unwrap().clone()
        }
    }
    #[async_trait]
    impl Sink for FlakySink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            Ok(records.len())
        }
        async fn write_batch_partial(
            &self,
            records: &[Value],
        ) -> Result<Vec<crate::RowOutcome>, FaucetError> {
            self.calls.lock().unwrap().push(records.len());
            Ok(records
                .iter()
                .enumerate()
                .map(|(i, _)| {
                    if (i + 1) % self.every == 0 {
                        Err(FaucetError::Sink("synthetic".into()))
                    } else {
                        Ok(())
                    }
                })
                .collect())
        }
    }

    #[tokio::test]
    async fn adaptive_shrinks_under_errors_on_dlq_path() {
        use crate::adaptive::AdaptiveBatchConfig;
        use crate::dlq::{DlqConfig, OnBatchError};
        // Three pages of 400 records each, matching the pattern used by
        // adaptive_shrinks_under_latency_target_then_smaller_chunks. After
        // page 1 (single 400-record chunk, 25% error rate > threshold 0.1),
        // the controller shrinks and subsequent pages get smaller sub-batches.
        let mk = || StreamPage {
            records: (0..400).map(|i| json!({"i": i})).collect(),
            bookmark: None,
        };
        let stream = futures::stream::iter(vec![Ok(mk()), Ok(mk()), Ok(mk())]);
        let sink = FlakySink::new(4); // 25% error rate > threshold 0.1
        let dlq_sink: Arc<dyn Sink> = Arc::new(MockSink::new());
        let dlq = DlqConfig {
            sink: dlq_sink,
            on_batch_error: OnBatchError::Propagate,
            max_failures_per_page: None,
            max_failures_total: None,
            include_original_payload: true,
        };
        let cfg: AdaptiveBatchConfig = serde_json::from_value(json!({
            "enabled": true, "min": 50, "max": 400,
            "decrease_factor": 0.5, "cooldown_batches": 0, "error_threshold": 0.1
        }))
        .unwrap();
        let opts = RunStreamOptions::new().with_dlq(dlq).with_adaptive(cfg);
        let result = run_stream(stream, &sink, opts).await.unwrap();
        // 3 × 400 = 1200 records total; FlakySink(4) fails every 4th record
        // per-chunk (floor(n/4)), so exact counts depend on chunk sizes due to
        // integer arithmetic. With the controller shrinking under 25% error
        // rate: page 1 = one 400-record chunk (300 written, 100 DLQ); pages
        // 2–3 = smaller sub-batches; overall >≈75% of 1200 commit and ~25% go
        // to the DLQ.
        assert!(
            result.records_written >= 900,
            "expected ≥900 written, got {}",
            result.records_written
        );
        let sizes = sink.call_sizes();
        assert_eq!(sizes[0], 400, "first chunk is the full page");
        assert!(
            sizes.last().unwrap() < &400,
            "controller should shrink under errors: {sizes:?}"
        );
        assert!(
            result.dlq.unwrap().records_dlq >= 250,
            "expected ≥250 DLQ records"
        );
    }

    // ── Adaptive batch-size tests ──────────────────────────────────────────

    /// A sink that records each write_batch call's size and reports a fixed
    /// per-call latency, so we can assert the adaptive controller resliced.
    struct RecordingSink {
        calls: std::sync::Mutex<Vec<usize>>,
        latency: std::time::Duration,
    }
    impl RecordingSink {
        fn new(latency_ms: u64) -> Self {
            Self {
                calls: std::sync::Mutex::new(Vec::new()),
                latency: std::time::Duration::from_millis(latency_ms),
            }
        }
        fn call_sizes(&self) -> Vec<usize> {
            self.calls.lock().unwrap().clone()
        }
    }
    #[async_trait]
    impl Sink for RecordingSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            tokio::time::sleep(self.latency).await;
            self.calls.lock().unwrap().push(records.len());
            Ok(records.len())
        }
    }

    #[tokio::test]
    async fn adaptive_reslices_non_dlq_page_into_subbatches() {
        use crate::adaptive::AdaptiveBatchConfig;
        let page = StreamPage {
            records: (0..1000).map(|i| json!({ "i": i })).collect(),
            bookmark: None,
        };
        let stream = futures::stream::iter(vec![Ok(page)]);
        let sink = RecordingSink::new(0);
        let cfg: AdaptiveBatchConfig =
            serde_json::from_value(json!({"enabled": true, "min": 100, "max": 1000})).unwrap();
        let result = run_stream(stream, &sink, RunStreamOptions::new().with_adaptive(cfg))
            .await
            .unwrap();
        assert_eq!(result.records_written, 1000);
        // current starts at min(max, page_len)=1000 → one chunk (no regression).
        assert_eq!(sink.call_sizes(), vec![1000]);
    }

    #[tokio::test]
    async fn adaptive_shrinks_under_latency_target_then_smaller_chunks() {
        use crate::adaptive::AdaptiveBatchConfig;
        let mk = || StreamPage {
            records: (0..400).map(|i| json!({"i": i})).collect(),
            bookmark: None,
        };
        let stream = futures::stream::iter(vec![Ok(mk()), Ok(mk()), Ok(mk())]);
        let sink = RecordingSink::new(50);
        let cfg: AdaptiveBatchConfig = serde_json::from_value(json!({
            "enabled": true, "min": 50, "max": 400,
            "decrease_factor": 0.5, "cooldown_batches": 0,
            "target_latency_ms": 10, "latency_window": 1
        }))
        .unwrap();
        let result = run_stream(stream, &sink, RunStreamOptions::new().with_adaptive(cfg))
            .await
            .unwrap();
        assert_eq!(result.records_written, 1200);
        let sizes = sink.call_sizes();
        assert_eq!(sizes[0], 400);
        assert!(
            sizes.last().unwrap() < &400,
            "controller should have shrunk: {sizes:?}"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn adaptive_emits_batch_size_and_adjustments_metrics() {
        // Mirror the same LOCK+snapshotter pattern used by
        // `pipeline_run_increments_runs_total` and `dlq_emits_records_total_and_pages_total`.
        use crate::adaptive::AdaptiveBatchConfig;
        use crate::observability::decorator::source_tests::{LOCK, snapshotter};
        use metrics_util::debugging::DebugValue;

        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();

        // Three pages of 400 records. RecordingSink(50) reports 50ms latency.
        // Config: target_latency_ms=10, latency_window=1, cooldown_batches=0 so
        // p50 (50ms) > 10*1.2=12ms on every batch → controller shrinks each time,
        // guaranteeing at least one `faucet_pipeline_adaptive_batch_adjustments_total`
        // is emitted.
        let mk = || StreamPage {
            records: (0..400).map(|i| json!({"i": i})).collect(),
            bookmark: None,
        };
        let stream = futures::stream::iter(vec![Ok(mk()), Ok(mk()), Ok(mk())]);
        let sink = RecordingSink::new(50);
        let cfg: AdaptiveBatchConfig = serde_json::from_value(json!({
            "enabled": true, "min": 50, "max": 400,
            "decrease_factor": 0.5, "cooldown_batches": 0,
            "target_latency_ms": 10, "latency_window": 1
        }))
        .unwrap();

        let _ = run_stream(
            stream,
            &sink,
            RunStreamOptions::new()
                .with_adaptive(cfg)
                .with_name("p")
                .with_row("r"),
        )
        .await
        .unwrap();

        let snapshot = snap.snapshot();
        let mut saw_batch_size = false;
        let mut saw_adjustments = false;
        for (k, _u, _d, v) in snapshot.into_vec() {
            let key = k.key();
            let labels = key.labels().collect::<Vec<_>>();
            let has = |k: &str, val: &str| labels.iter().any(|l| l.key() == k && l.value() == val);

            if key.name() == "faucet_pipeline_adaptive_batch_size"
                && has("pipeline", "p")
                && has("row", "r")
                && matches!(v, DebugValue::Gauge(_))
            {
                saw_batch_size = true;
            }
            if key.name() == "faucet_pipeline_adaptive_batch_adjustments_total"
                && has("pipeline", "p")
                && has("row", "r")
                && matches!(v, DebugValue::Counter(c) if c >= 1)
            {
                saw_adjustments = true;
            }
        }
        assert!(
            saw_batch_size,
            "expected faucet_pipeline_adaptive_batch_size gauge with pipeline=p, row=r"
        );
        assert!(
            saw_adjustments,
            "expected faucet_pipeline_adaptive_batch_adjustments_total counter with pipeline=p, row=r"
        );
    }

    // ── run_stream: exactly-once + DLQ incompatibility gate ─────────────────

    #[tokio::test]
    async fn exactly_once_rejects_dlq() {
        // Exactly-once must reject a configured DLQ (incompatible in this
        // version): the gate fires before any page is polled.
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let dlq_sink: Arc<dyn Sink> = Arc::new(MockSink::new());
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![];
        let opts = eo_opts(store, "k", 0).with_dlq(DlqConfig::new(dlq_sink));
        let r = run_stream(
            futures::stream::iter(pages),
            &IdempotentMockSink::new(),
            opts,
        )
        .await;
        assert!(
            matches!(&r, Err(FaucetError::Config(m)) if m.contains("not compatible with a DLQ")),
            "got: {r:?}"
        );
    }

    // ── run_stream: exactly-once page with records but no bookmark ──────────

    #[tokio::test]
    async fn exactly_once_writes_unbookmarked_page_at_least_once() {
        // Under exactly-once, a page that carries records but NO bookmark is
        // not individually checkpointed: it falls through to a plain
        // `write_batch` (at-least-once for that page) and is NOT idempotently
        // tokened.
        let sink = IdempotentMockSink::new();
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let pages = vec![Ok(StreamPage {
            records: vec![json!({"id": 1}), json!({"id": 2})],
            bookmark: None,
        })];
        let r = run_stream(
            futures::stream::iter(pages),
            &sink,
            eo_opts(store.clone(), "k", 0),
        )
        .await
        .unwrap();
        assert_eq!(r.records_written, 2);
        assert_eq!(r.bookmark, None);
        // Rows were written via the plain (non-idempotent) write_batch path, so
        // no commit token was recorded for the scope.
        assert_eq!(sink.last_committed_token("k").await.unwrap(), None);
        // Nothing was persisted to the state store (no bookmark to checkpoint).
        assert!(store.get("k").await.unwrap().is_none());
        assert_eq!(sink.rows(), vec![json!({"id": 1}), json!({"id": 2})]);
    }

    // ── DLQ-with-bookmark success path: persist bookmark after routing ──────

    #[tokio::test]
    async fn dlq_with_bookmark_persists_after_routing_failures() {
        // A bookmark-carrying page whose main sink reports one per-row failure:
        // survivors commit, the failed row reaches the DLQ, the DLQ is flushed,
        // and the bookmark is persisted to the state store.
        let main = PartialSink::new(vec![1]); // 2 rows, index 1 fails
        let dlq = std::sync::Arc::new(MockSink::new());
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: vec![json!({"i": 0}), json!({"i": 1})],
            bookmark: Some(json!("ckpt")),
        })];
        let result = run_stream(
            futures::stream::iter(pages),
            &main,
            RunStreamOptions::new()
                .with_dlq(DlqConfig::new(dlq.clone()))
                .with_state(Arc::clone(&store), "k"),
        )
        .await
        .unwrap();

        assert_eq!(result.records_written, 1); // row 0 committed
        assert_eq!(result.bookmark, Some(json!("ckpt")));
        // Bookmark was persisted after the page was made durable.
        assert_eq!(store.get("k").await.unwrap(), Some(json!("ckpt")));
        // The single failed row reached the DLQ.
        let envelopes = dlq.0.lock().unwrap();
        assert_eq!(envelopes.len(), 1);
        assert_eq!(envelopes[0]["payload"]["i"], 1);
    }

    // ── run_stream drives exactly-once with resume from start_seq ───────────

    #[tokio::test]
    async fn exactly_once_resumes_sequence_from_start_seq() {
        // A resume run starts at start_seq (the persisted seq) and continues
        // numbering tokens from there; the next bookmark-carrying page is
        // committed at seq = start_seq + 1.
        let sink = IdempotentMockSink::new();
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let pages = vec![Ok(StreamPage {
            records: vec![json!({"id": 9})],
            bookmark: Some(json!("bm-after-resume")),
        })];
        let r = run_stream(
            futures::stream::iter(pages),
            &sink,
            eo_opts(store.clone(), "eo_key", 7),
        )
        .await
        .unwrap();

        assert_eq!(r.records_written, 1);
        let (bm, seq) =
            crate::idempotency::unwrap_state(&store.get("eo_key").await.unwrap().unwrap());
        assert_eq!(bm, Some(json!("bm-after-resume")));
        assert_eq!(seq, 8, "sequence resumes at start_seq + 1");
        let token = sink.last_committed_token("eo_key").await.unwrap().unwrap();
        assert_eq!(
            crate::idempotency::parse_token_parts(&token),
            Some((8, Some(json!("bm-after-resume"))))
        );
    }

    // ── Pipeline::run with quality wired through the builder ────────────────

    #[cfg(feature = "quality")]
    #[tokio::test]
    async fn pipeline_run_with_quality_aborts_on_failed_batch_check() {
        use crate::quality::{BatchCheck, CompiledQuality, OnFailure, QualitySpec};
        let source = MockSource(vec![json!({"id": 1})]);
        let main = MockSink::new();
        let spec = QualitySpec {
            record: vec![],
            batch: vec![BatchCheck::RowCount {
                min: Some(5),
                max: None,
                on_failure: OnFailure::Abort,
            }],
        };
        let quality = Arc::new(CompiledQuality::compile(&spec).unwrap());
        let result = Pipeline::new(&source, &main)
            .with_quality(quality)
            .run()
            .await;
        assert!(matches!(result, Err(FaucetError::QualityFailure { .. })));
        // The abort fired before the sink committed.
        assert!(main.written().is_empty());
    }

    // ── Pipeline::run with adaptive wired through the builder ───────────────

    #[tokio::test]
    async fn pipeline_run_with_adaptive_reslices_page() {
        use crate::adaptive::AdaptiveBatchConfig;
        let source = MockSource((0..1000).map(|i| json!({ "i": i })).collect());
        let sink = MockSink::new();
        let cfg: AdaptiveBatchConfig =
            serde_json::from_value(json!({"enabled": true, "min": 100, "max": 1000})).unwrap();
        let result = Pipeline::new(&source, &sink)
            .with_adaptive(cfg)
            .run()
            .await
            .unwrap();
        assert_eq!(result.records_written, 1000);
        assert_eq!(sink.written().len(), 1000);
    }

    // ── Adaptive no-op warning for per-record sinks (jsonl/csv/stdout) ──────

    #[tokio::test]
    async fn adaptive_noop_sink_name_is_handled() {
        use crate::adaptive::AdaptiveBatchConfig;

        // A sink whose connector_name() is one of the per-record names triggers
        // the one-shot "no-op for this per-record sink" info path.
        struct JsonlNamedSink(std::sync::Mutex<usize>);
        #[async_trait]
        impl Sink for JsonlNamedSink {
            async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
                *self.0.lock().unwrap() += records.len();
                Ok(records.len())
            }
            fn connector_name(&self) -> &'static str {
                "jsonl"
            }
        }

        let page = StreamPage {
            records: (0..10).map(|i| json!({ "i": i })).collect(),
            bookmark: None,
        };
        let stream = futures::stream::iter(vec![Ok(page)]);
        let sink = JsonlNamedSink(std::sync::Mutex::new(0));
        let cfg: AdaptiveBatchConfig =
            serde_json::from_value(json!({"enabled": true, "min": 5, "max": 10})).unwrap();
        let result = run_stream(stream, &sink, RunStreamOptions::new().with_adaptive(cfg))
            .await
            .unwrap();
        assert_eq!(result.records_written, 10);
        assert_eq!(*sink.0.lock().unwrap(), 10);
    }

    #[tokio::test]
    async fn resilience_retries_transient_sink_write() {
        use crate::resilience::{BackoffKind, ResiliencePolicy, RetryPolicy};
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::time::Duration;

        struct TransientFlakySink {
            attempts: Arc<AtomicU32>,
            written: Arc<AtomicU32>,
        }
        #[async_trait::async_trait]
        impl Sink for TransientFlakySink {
            async fn write_batch(
                &self,
                records: &[serde_json::Value],
            ) -> Result<usize, FaucetError> {
                let n = self.attempts.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    return Err(FaucetError::HttpStatus {
                        status: 503,
                        url: "u".into(),
                        body: "".into(),
                    });
                }
                self.written
                    .fetch_add(records.len() as u32, Ordering::SeqCst);
                Ok(records.len())
            }
            async fn flush(&self) -> Result<(), FaucetError> {
                Ok(())
            }
            // Idempotent so the pipeline retries its `write_batch` (F29 gate).
            fn supports_idempotent_writes(&self) -> bool {
                true
            }
        }

        let attempts = Arc::new(AtomicU32::new(0));
        let written = Arc::new(AtomicU32::new(0));
        let sink = TransientFlakySink {
            attempts: attempts.clone(),
            written: written.clone(),
        };
        let pages = futures::stream::iter(vec![Ok(StreamPage {
            records: vec![serde_json::json!({"a": 1})],
            bookmark: None,
        })]);
        let policy = ResiliencePolicy {
            retry: RetryPolicy {
                max_attempts: 5,
                backoff: BackoffKind::None,
                base: Duration::ZERO,
                max: Duration::ZERO,
                jitter: false,
                ..RetryPolicy::default()
            },
            ..ResiliencePolicy::default()
        };
        let res = run_stream(
            pages,
            &sink,
            RunStreamOptions::new().with_resilience(policy),
        )
        .await
        .unwrap();
        assert_eq!(written.load(Ordering::SeqCst), 1);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        assert_eq!(res.records_written, 1);
    }

    #[tokio::test]
    async fn resilience_does_not_retry_non_idempotent_write_batch() {
        // F29/F32: a non-idempotent `write_batch` must NOT be pipeline-retried —
        // a lost-response retry would silently duplicate rows. The first error
        // propagates after a single attempt.
        use crate::resilience::{BackoffKind, ResiliencePolicy, RetryPolicy};
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::time::Duration;

        struct NonIdempotentFlakySink {
            attempts: Arc<AtomicU32>,
        }
        #[async_trait::async_trait]
        impl Sink for NonIdempotentFlakySink {
            async fn write_batch(&self, _r: &[Value]) -> Result<usize, FaucetError> {
                self.attempts.fetch_add(1, Ordering::SeqCst);
                Err(FaucetError::HttpStatus {
                    status: 503,
                    url: "u".into(),
                    body: "".into(),
                })
            }
            async fn flush(&self) -> Result<(), FaucetError> {
                Ok(())
            }
            // supports_idempotent_writes() defaults to false.
        }

        let attempts = Arc::new(AtomicU32::new(0));
        let sink = NonIdempotentFlakySink {
            attempts: attempts.clone(),
        };
        let pages = futures::stream::iter(vec![Ok(StreamPage {
            records: vec![json!({"a": 1})],
            bookmark: None,
        })]);
        let policy = ResiliencePolicy {
            retry: RetryPolicy {
                max_attempts: 5,
                backoff: BackoffKind::None,
                base: Duration::ZERO,
                max: Duration::ZERO,
                jitter: false,
                ..RetryPolicy::default()
            },
            ..ResiliencePolicy::default()
        };
        let err = run_stream(
            pages,
            &sink,
            RunStreamOptions::new().with_resilience(policy),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, FaucetError::HttpStatus { status: 503, .. }));
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "non-idempotent write_batch must be attempted exactly once (no retry)"
        );
    }

    #[tokio::test]
    async fn resilience_circuit_opens_after_consecutive_failed_pages() {
        use crate::dlq::{DlqConfig, OnBatchError};
        use crate::resilience::{BackoffKind, CircuitBreakerConfig, ResiliencePolicy, RetryPolicy};
        use std::sync::Arc;
        use std::time::Duration;

        // A sink whose write_batch_partial always fully fails (outer Err).
        struct DeadSink;
        #[async_trait]
        impl Sink for DeadSink {
            async fn write_batch(&self, _r: &[Value]) -> Result<usize, FaucetError> {
                Err(FaucetError::Sink("down".into()))
            }
            async fn flush(&self) -> Result<(), FaucetError> {
                Ok(())
            }
        }
        // DLQ sink that accepts everything.
        struct NullSink;
        #[async_trait]
        impl Sink for NullSink {
            async fn write_batch(&self, r: &[Value]) -> Result<usize, FaucetError> {
                Ok(r.len())
            }
            async fn flush(&self) -> Result<(), FaucetError> {
                Ok(())
            }
        }

        let pages = futures::stream::iter((0..10).map(|i| {
            Ok(StreamPage {
                records: vec![json!({"i": i})],
                bookmark: None,
            })
        }));
        let dlq = DlqConfig {
            on_batch_error: OnBatchError::DlqAll,
            ..DlqConfig::new(Arc::new(NullSink))
        };
        let policy = ResiliencePolicy {
            retry: RetryPolicy {
                max_attempts: 1,
                backoff: BackoffKind::None,
                base: Duration::ZERO,
                max: Duration::ZERO,
                jitter: false,
                ..RetryPolicy::default()
            },
            circuit_breaker: Some(CircuitBreakerConfig {
                consecutive_failures: 3,
                cooldown: Duration::from_secs(60),
            }),
            poison: None,
        };
        let err = run_stream(
            pages,
            &DeadSink,
            RunStreamOptions::new()
                .with_dlq(dlq)
                .with_resilience(policy),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, FaucetError::CircuitOpen { failures: 3, .. }),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn resilience_poison_retries_then_dlqs_failing_row() {
        use crate::dlq::DlqConfig;
        use crate::resilience::{
            BackoffKind, PoisonAction, PoisonPolicy, ResiliencePolicy, RetryPolicy,
        };
        use std::sync::{Arc, Mutex};
        use std::time::Duration;

        // Sink: row {"bad":true} always fails; others succeed. Counts attempts
        // on the bad row.
        struct PickySink {
            bad_attempts: Arc<Mutex<u32>>,
        }
        #[async_trait]
        impl Sink for PickySink {
            async fn write_batch(&self, r: &[Value]) -> Result<usize, FaucetError> {
                Ok(r.len())
            }
            async fn write_batch_partial(
                &self,
                records: &[Value],
            ) -> Result<Vec<crate::RowOutcome>, FaucetError> {
                Ok(records
                    .iter()
                    .map(|rec| {
                        if rec.get("bad").and_then(|v| v.as_bool()).unwrap_or(false) {
                            *self.bad_attempts.lock().unwrap() += 1;
                            Err(FaucetError::HttpStatus {
                                status: 503,
                                url: "u".into(),
                                body: "".into(),
                            })
                        } else {
                            Ok(())
                        }
                    })
                    .collect())
            }
            async fn flush(&self) -> Result<(), FaucetError> {
                Ok(())
            }
        }
        struct CaptureSink(Arc<Mutex<Vec<Value>>>);
        #[async_trait]
        impl Sink for CaptureSink {
            async fn write_batch(&self, r: &[Value]) -> Result<usize, FaucetError> {
                self.0.lock().unwrap().extend_from_slice(r);
                Ok(r.len())
            }
            async fn flush(&self) -> Result<(), FaucetError> {
                Ok(())
            }
        }

        let captured = Arc::new(Mutex::new(Vec::new()));
        let bad_attempts = Arc::new(Mutex::new(0u32));
        let sink = PickySink {
            bad_attempts: bad_attempts.clone(),
        };
        let pages = futures::stream::iter(vec![Ok(StreamPage {
            records: vec![json!({"ok": 1}), json!({"bad": true})],
            bookmark: None,
        })]);
        let policy = ResiliencePolicy {
            retry: RetryPolicy {
                max_attempts: 1,
                backoff: BackoffKind::None,
                base: Duration::ZERO,
                max: Duration::ZERO,
                jitter: false,
                ..RetryPolicy::default()
            },
            circuit_breaker: None,
            poison: Some(PoisonPolicy {
                max_row_attempts: 3,
                action: PoisonAction::Dlq,
            }),
        };
        let res = run_stream(
            pages,
            &sink,
            RunStreamOptions::new()
                .with_dlq(DlqConfig::new(Arc::new(CaptureSink(captured.clone()))))
                .with_resilience(policy),
        )
        .await
        .unwrap();

        assert_eq!(
            *bad_attempts.lock().unwrap(),
            3,
            "bad row tried max_row_attempts times"
        );
        assert_eq!(res.records_written, 1, "the ok row");
        let dlq = captured.lock().unwrap();
        assert_eq!(dlq.len(), 1, "one row to DLQ");
        assert_eq!(dlq[0]["payload"]["bad"], json!(true));
    }

    #[tokio::test]
    async fn poison_loop_does_not_nest_resilience_retry_on_subset_resubmit() {
        // F47: with both `retry` and `poison` configured against an *idempotent*
        // sink, the poison loop's per-row resubmit must be a BARE
        // `write_batch_partial` — NOT wrapped in `with_retry!`. Otherwise an
        // outer-`Err` resubmit is retried `max_attempts` times *inside each*
        // poison iteration, multiplying submissions to the sink. We force the
        // subset (single bad row) call to return an outer retriable `Err` and
        // count how many times the sink is hit with that one-row subset.
        use crate::dlq::DlqConfig;
        use crate::resilience::{
            BackoffKind, PoisonAction, PoisonPolicy, ResiliencePolicy, RetryPolicy,
        };
        use std::sync::{Arc, Mutex};
        use std::time::Duration;

        struct OuterErrOnSubsetSink {
            subset_calls: Arc<Mutex<u32>>,
        }
        #[async_trait]
        impl Sink for OuterErrOnSubsetSink {
            async fn write_batch(&self, r: &[Value]) -> Result<usize, FaucetError> {
                Ok(r.len())
            }
            async fn write_batch_partial(
                &self,
                records: &[Value],
            ) -> Result<Vec<crate::RowOutcome>, FaucetError> {
                // The full chunk has 2 rows; the poison subset is the 1 bad row.
                if records.len() == 1 {
                    *self.subset_calls.lock().unwrap() += 1;
                    return Err(FaucetError::HttpStatus {
                        status: 503,
                        url: "u".into(),
                        body: "".into(),
                    });
                }
                Ok(records
                    .iter()
                    .map(|rec| {
                        if rec.get("bad").and_then(|v| v.as_bool()).unwrap_or(false) {
                            Err(FaucetError::HttpStatus {
                                status: 503,
                                url: "u".into(),
                                body: "".into(),
                            })
                        } else {
                            Ok(())
                        }
                    })
                    .collect())
            }
            async fn flush(&self) -> Result<(), FaucetError> {
                Ok(())
            }
            // Idempotent so `with_retry_write!` WOULD retry if it were used here —
            // this is exactly the condition the fix guards against.
            fn supports_idempotent_writes(&self) -> bool {
                true
            }
        }
        struct CaptureSink(Arc<Mutex<Vec<Value>>>);
        #[async_trait]
        impl Sink for CaptureSink {
            async fn write_batch(&self, r: &[Value]) -> Result<usize, FaucetError> {
                self.0.lock().unwrap().extend_from_slice(r);
                Ok(r.len())
            }
            async fn flush(&self) -> Result<(), FaucetError> {
                Ok(())
            }
        }

        let subset_calls = Arc::new(Mutex::new(0u32));
        let sink = OuterErrOnSubsetSink {
            subset_calls: subset_calls.clone(),
        };
        let pages = futures::stream::iter(vec![Ok(StreamPage {
            records: vec![json!({"ok": 1}), json!({"bad": true})],
            bookmark: None,
        })]);
        let policy = ResiliencePolicy {
            retry: RetryPolicy {
                max_attempts: 4, // would-be 4× amplification per poison iteration
                backoff: BackoffKind::None,
                base: Duration::ZERO,
                max: Duration::ZERO,
                jitter: false,
                ..RetryPolicy::default()
            },
            circuit_breaker: None,
            poison: Some(PoisonPolicy {
                max_row_attempts: 3,
                action: PoisonAction::Dlq,
            }),
        };
        let res = run_stream(
            pages,
            &sink,
            RunStreamOptions::new()
                .with_dlq(DlqConfig::new(Arc::new(CaptureSink(Arc::new(Mutex::new(
                    Vec::new(),
                ))))))
                .with_resilience(policy),
        )
        .await;

        // The outer-`Err` subset resubmit propagates and aborts the run.
        assert!(matches!(
            res,
            Err(FaucetError::HttpStatus { status: 503, .. })
        ));
        // Exactly ONE subset submission — the bare call. With the bug it would be
        // `max_attempts` (4) due to the nested `with_retry!`.
        assert_eq!(
            *subset_calls.lock().unwrap(),
            1,
            "poison subset resubmit must be bare (no nested resilience retry)"
        );
    }

    #[tokio::test]
    async fn resilience_retries_transient_flush_and_state_on_dlq_path() {
        // The DLQ path's `sink.flush()` and `store.put()` must be retry-wrapped
        // like the default/exactly-once paths: a transient failure on either is
        // retried before the run aborts. Drive a bookmark-carrying page through
        // the DLQ path (one row routed to the DLQ via a partial-write failure),
        // with both the main-sink flush and the state put failing twice then
        // succeeding.
        use crate::dlq::DlqConfig;
        use crate::resilience::{BackoffKind, ResiliencePolicy, RetryPolicy};
        use crate::state::StateStore;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::time::Duration;

        fn transient_503() -> FaucetError {
            FaucetError::HttpStatus {
                status: 503,
                url: "u".into(),
                body: "".into(),
            }
        }

        // Main sink: one row fails per-row (→ DLQ), the rest succeed; flush()
        // fails transiently the first two calls, then succeeds.
        struct FlakyFlushSink {
            flush_attempts: Arc<AtomicU32>,
        }
        #[async_trait]
        impl Sink for FlakyFlushSink {
            async fn write_batch(&self, r: &[Value]) -> Result<usize, FaucetError> {
                Ok(r.len())
            }
            async fn write_batch_partial(
                &self,
                records: &[Value],
            ) -> Result<Vec<crate::RowOutcome>, FaucetError> {
                Ok(records
                    .iter()
                    .map(|rec| {
                        if rec.get("bad").and_then(|v| v.as_bool()).unwrap_or(false) {
                            Err(transient_503())
                        } else {
                            Ok(())
                        }
                    })
                    .collect())
            }
            async fn flush(&self) -> Result<(), FaucetError> {
                let n = self.flush_attempts.fetch_add(1, Ordering::SeqCst);
                if n < 2 { Err(transient_503()) } else { Ok(()) }
            }
        }
        struct NullSink;
        #[async_trait]
        impl Sink for NullSink {
            async fn write_batch(&self, r: &[Value]) -> Result<usize, FaucetError> {
                Ok(r.len())
            }
            async fn flush(&self) -> Result<(), FaucetError> {
                Ok(())
            }
        }

        // State store whose put() fails transiently the first two calls.
        struct FlakyStore {
            put_attempts: Arc<AtomicU32>,
            value: Arc<std::sync::Mutex<Option<Value>>>,
        }
        #[async_trait]
        impl StateStore for FlakyStore {
            async fn get(&self, _key: &str) -> Result<Option<Value>, FaucetError> {
                Ok(self.value.lock().unwrap().clone())
            }
            async fn put(&self, _key: &str, value: &Value) -> Result<(), FaucetError> {
                let n = self.put_attempts.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    return Err(transient_503());
                }
                *self.value.lock().unwrap() = Some(value.clone());
                Ok(())
            }
            async fn delete(&self, _key: &str) -> Result<(), FaucetError> {
                Ok(())
            }
        }

        let flush_attempts = Arc::new(AtomicU32::new(0));
        let put_attempts = Arc::new(AtomicU32::new(0));
        let stored = Arc::new(std::sync::Mutex::new(None));
        let sink = FlakyFlushSink {
            flush_attempts: flush_attempts.clone(),
        };
        let store: Arc<dyn StateStore> = Arc::new(FlakyStore {
            put_attempts: put_attempts.clone(),
            value: stored.clone(),
        });
        let pages = futures::stream::iter(vec![Ok(StreamPage {
            records: vec![json!({"ok": 1}), json!({"bad": true})],
            bookmark: Some(json!({"cursor": 42})),
        })]);
        let policy = ResiliencePolicy {
            retry: RetryPolicy {
                max_attempts: 5,
                backoff: BackoffKind::None,
                base: Duration::ZERO,
                max: Duration::ZERO,
                jitter: false,
                ..RetryPolicy::default()
            },
            ..ResiliencePolicy::default()
        };
        let res = run_stream(
            pages,
            &sink,
            RunStreamOptions::new()
                .with_dlq(DlqConfig::new(Arc::new(NullSink)))
                .with_state(store, "k")
                .with_resilience(policy),
        )
        .await
        .unwrap();

        // Page-gate flush: 2 transient failures retried, success on the 3rd
        // call — proving the DLQ-path `sink.flush()` is retry-wrapped. A 4th
        // call is the (already-succeeding) end-of-stream final flush.
        assert_eq!(
            flush_attempts.load(Ordering::SeqCst),
            4,
            "page-gate flush retried past two transient failures (3) + 1 final flush"
        );
        // State put succeeded on the 3rd call (2 transient failures retried).
        assert_eq!(
            put_attempts.load(Ordering::SeqCst),
            3,
            "state put retried past two transient failures"
        );
        assert_eq!(*stored.lock().unwrap(), Some(json!({"cursor": 42})));
        assert_eq!(res.records_written, 1, "the ok row");
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn resilience_emits_retries_total_with_op_and_class_labels() {
        // Drive the flaky-sink retry path under a recorder and assert the
        // metered runner emitted `faucet_resilience_retries_total` with the
        // spec's `{op, class}` labels.
        use crate::observability::decorator::source_tests::{LOCK, snapshotter};
        use crate::resilience::{BackoffKind, ResiliencePolicy, RetryPolicy};
        use metrics_util::debugging::DebugValue;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::time::Duration;

        struct RetryProbeSink {
            attempts: Arc<AtomicU32>,
        }
        #[async_trait]
        impl Sink for RetryProbeSink {
            async fn write_batch(&self, r: &[Value]) -> Result<usize, FaucetError> {
                let n = self.attempts.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    return Err(FaucetError::HttpStatus {
                        status: 503,
                        url: "u".into(),
                        body: "".into(),
                    });
                }
                Ok(r.len())
            }
            async fn flush(&self) -> Result<(), FaucetError> {
                Ok(())
            }
            // Unique connector name unused for resilience metrics (they carry
            // pipeline/row/op only) but keeps the debug_assert happy.
            fn connector_name(&self) -> &'static str {
                "retry-probe"
            }
            // Idempotent so the pipeline retries its `write_batch` (F29 gate).
            fn supports_idempotent_writes(&self) -> bool {
                true
            }
        }

        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();

        let attempts = Arc::new(AtomicU32::new(0));
        let sink = RetryProbeSink {
            attempts: attempts.clone(),
        };
        let pages = futures::stream::iter(vec![Ok(StreamPage {
            records: vec![json!({"a": 1})],
            bookmark: None,
        })]);
        let policy = ResiliencePolicy {
            retry: RetryPolicy {
                max_attempts: 5,
                backoff: BackoffKind::None,
                base: Duration::ZERO,
                max: Duration::ZERO,
                jitter: false,
                ..RetryPolicy::default()
            },
            ..ResiliencePolicy::default()
        };
        run_stream(
            pages,
            &sink,
            RunStreamOptions::new()
                .with_name("retry-metrics-pipeline")
                .with_resilience(policy),
        )
        .await
        .unwrap();
        assert_eq!(attempts.load(Ordering::SeqCst), 3);

        let snapshot = snap.snapshot();
        let retries: u64 = snapshot
            .into_vec()
            .into_iter()
            .filter_map(|(key, _u, _d, v): (metrics_util::CompositeKey, _, _, _)| {
                if key.key().name() == "faucet_resilience_retries_total"
                    && key.key().labels().any(|l: &metrics::Label| {
                        l.key() == "pipeline" && l.value() == "retry-metrics-pipeline"
                    })
                    && key
                        .key()
                        .labels()
                        .any(|l: &metrics::Label| l.key() == "op" && l.value() == "sink_write")
                    && key
                        .key()
                        .labels()
                        .any(|l: &metrics::Label| l.key() == "class" && l.value() == "http_5xx")
                    && let DebugValue::Counter(c) = v
                {
                    Some(c)
                } else {
                    None
                }
            })
            .sum();
        assert_eq!(
            retries, 2,
            "expected 2 retries (2 transient 503s) counted with op=sink_write, class=http_5xx"
        );
    }

    // ── Schema-drift pass (#194) ─────────────────────────────────────────────

    /// Sink that reports a fixed `current_schema` and records evolve calls.
    struct SchemaSink {
        schema: Value,
        written: std::sync::Mutex<Vec<Value>>,
        evolutions: std::sync::Mutex<Vec<crate::drift::SchemaEvolution>>,
        evolvable: bool,
    }
    impl SchemaSink {
        fn new(schema: Value, evolvable: bool) -> Self {
            Self {
                schema,
                written: std::sync::Mutex::new(Vec::new()),
                evolutions: std::sync::Mutex::new(Vec::new()),
                evolvable,
            }
        }
        fn written(&self) -> Vec<Value> {
            self.written.lock().unwrap().clone()
        }
    }
    #[async_trait]
    impl Sink for SchemaSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            self.written.lock().unwrap().extend(records.iter().cloned());
            Ok(records.len())
        }
        async fn current_schema(&self) -> Result<Option<Value>, FaucetError> {
            Ok(Some(self.schema.clone()))
        }
        fn supports_schema_evolution(&self) -> bool {
            self.evolvable
        }
        async fn evolve_schema(
            &self,
            evo: &crate::drift::SchemaEvolution,
        ) -> Result<(), FaucetError> {
            if !self.evolvable {
                return Err(FaucetError::Sink("not evolvable".into()));
            }
            self.evolutions.lock().unwrap().push(evo.clone());
            Ok(())
        }
    }

    fn drift_opts(policy: crate::drift::SchemaDriftPolicy) -> RunStreamOptions {
        RunStreamOptions::new().with_schema_drift(policy)
    }

    fn one_page(
        records: Vec<Value>,
    ) -> impl futures_core::Stream<Item = Result<StreamPage, FaucetError>> + Unpin {
        Box::pin(futures::stream::iter(vec![Ok(StreamPage {
            records,
            bookmark: None,
        })]))
    }

    #[tokio::test]
    async fn drift_warn_writes_unchanged() {
        let sink = SchemaSink::new(
            json!({"type":"object","properties":{"id":{"type":"integer"}}}),
            false,
        );
        let policy = crate::drift::SchemaDriftPolicy {
            on_drift: crate::drift::OnDrift::Warn,
            allow_widening: true,
            on_incompatible: crate::drift::OnIncompatible::Fail,
            relax_nullability_on_missing: false,
        };
        let pages = one_page(vec![json!({"id": 1, "email": "a@x.com"})]);
        let res = run_stream(pages, &sink, drift_opts(policy)).await.unwrap();
        assert_eq!(res.records_written, 1);
        // Unknown field is NOT stripped under warn.
        assert_eq!(sink.written()[0], json!({"id": 1, "email": "a@x.com"}));
    }

    #[tokio::test]
    async fn drift_ignore_strips_unknown_fields() {
        let sink = SchemaSink::new(
            json!({"type":"object","properties":{"id":{"type":"integer"}}}),
            false,
        );
        let policy = crate::drift::SchemaDriftPolicy {
            on_drift: crate::drift::OnDrift::Ignore,
            allow_widening: true,
            on_incompatible: crate::drift::OnIncompatible::Fail,
            relax_nullability_on_missing: false,
        };
        let pages = one_page(vec![json!({"id": 1, "email": "a@x.com"})]);
        let res = run_stream(pages, &sink, drift_opts(policy)).await.unwrap();
        assert_eq!(res.records_written, 1);
        assert_eq!(
            sink.written()[0],
            json!({"id": 1}),
            "email must be stripped"
        );
    }

    #[tokio::test]
    async fn drift_fail_raises_schema_drift() {
        let sink = SchemaSink::new(
            json!({"type":"object","properties":{"id":{"type":"integer"}}}),
            false,
        );
        let policy = crate::drift::SchemaDriftPolicy {
            on_drift: crate::drift::OnDrift::Fail,
            allow_widening: true,
            on_incompatible: crate::drift::OnIncompatible::Fail,
            relax_nullability_on_missing: false,
        };
        let pages = one_page(vec![json!({"id": 1, "email": "a@x.com"})]);
        let err = run_stream(pages, &sink, drift_opts(policy))
            .await
            .unwrap_err();
        assert!(matches!(err, FaucetError::SchemaDrift { .. }));
    }

    #[tokio::test]
    async fn drift_evolve_calls_sink_then_writes() {
        let sink = SchemaSink::new(
            json!({"type":"object","properties":{"id":{"type":"integer"}}}),
            true,
        );
        let policy = crate::drift::SchemaDriftPolicy {
            on_drift: crate::drift::OnDrift::Evolve,
            allow_widening: true,
            on_incompatible: crate::drift::OnIncompatible::Fail,
            relax_nullability_on_missing: false,
        };
        let pages = one_page(vec![json!({"id": 1, "email": "a@x.com"})]);
        let res = run_stream(pages, &sink, drift_opts(policy)).await.unwrap();
        assert_eq!(res.records_written, 1);
        let evos = sink.evolutions.lock().unwrap();
        assert_eq!(evos.len(), 1);
        assert_eq!(evos[0].additions.len(), 1);
        assert_eq!(evos[0].additions[0].name, "email");
        // Page is written through (with the unknown field — destination now has it).
        assert_eq!(sink.written()[0], json!({"id": 1, "email": "a@x.com"}));
    }

    #[tokio::test]
    async fn drift_evolve_does_not_relax_not_null_for_merely_absent_column() {
        // Destination has a NOT NULL `legacy` column the page omits. By default
        // (relax_nullability_on_missing=false) the constraint must NOT be
        // dropped — a transiently-omitted column is not evidence of optionality
        // (F28). The evolution is empty, so evolve_schema is never called.
        let sink = SchemaSink::new(
            json!({"type":"object","properties":{
                "id":{"type":"integer"},
                "legacy":{"type":"string"}
            }}),
            true,
        );
        let policy = crate::drift::SchemaDriftPolicy {
            on_drift: crate::drift::OnDrift::Evolve,
            allow_widening: true,
            on_incompatible: crate::drift::OnIncompatible::Fail,
            relax_nullability_on_missing: false,
        };
        let pages = one_page(vec![json!({"id": 1})]);
        let res = run_stream(pages, &sink, drift_opts(policy)).await.unwrap();
        assert_eq!(res.records_written, 1);
        assert!(
            sink.evolutions.lock().unwrap().is_empty(),
            "NOT NULL must not be relaxed for a merely-absent column"
        );
    }

    #[tokio::test]
    async fn drift_evolve_relaxes_absent_column_only_with_opt_in() {
        // Same scenario, but the operator explicitly opted in.
        let sink = SchemaSink::new(
            json!({"type":"object","properties":{
                "id":{"type":"integer"},
                "legacy":{"type":"string"}
            }}),
            true,
        );
        let policy = crate::drift::SchemaDriftPolicy {
            on_drift: crate::drift::OnDrift::Evolve,
            allow_widening: true,
            on_incompatible: crate::drift::OnIncompatible::Fail,
            relax_nullability_on_missing: true,
        };
        let pages = one_page(vec![json!({"id": 1})]);
        let res = run_stream(pages, &sink, drift_opts(policy)).await.unwrap();
        assert_eq!(res.records_written, 1);
        let evos = sink.evolutions.lock().unwrap();
        assert_eq!(evos.len(), 1);
        assert_eq!(evos[0].relax_nullability, vec!["legacy".to_string()]);
    }

    #[tokio::test]
    async fn drift_inert_when_sink_reports_no_schema() {
        // MockSink::current_schema defaults to None → pass is inert.
        let sink = MockSink::new();
        let policy = crate::drift::SchemaDriftPolicy {
            on_drift: crate::drift::OnDrift::Fail, // would fail IF a schema were known
            allow_widening: true,
            on_incompatible: crate::drift::OnIncompatible::Fail,
            relax_nullability_on_missing: false,
        };
        let pages = one_page(vec![json!({"id": 1, "anything": true})]);
        let res = run_stream(pages, &sink, drift_opts(policy)).await.unwrap();
        assert_eq!(res.records_written, 1);
    }

    #[tokio::test]
    async fn drift_quarantine_routes_drift_rows_to_dlq() {
        let sink = SchemaSink::new(
            json!({"type":"object","properties":{"id":{"type":"integer"}}}),
            false,
        );
        let dlq_sink = std::sync::Arc::new(MockSink::new());
        let policy = crate::drift::SchemaDriftPolicy {
            on_drift: crate::drift::OnDrift::Quarantine,
            allow_widening: true,
            on_incompatible: crate::drift::OnIncompatible::Fail,
            relax_nullability_on_missing: false,
        };
        let pages = one_page(vec![
            json!({"id": 1}),               // conforms → written
            json!({"id": 2, "email": "x"}), // drift → DLQ
        ]);
        let opts = RunStreamOptions::new()
            .with_schema_drift(policy)
            .with_dlq(crate::dlq::DlqConfig::new(dlq_sink.clone()));
        let res = run_stream(pages, &sink, opts).await.unwrap();
        assert_eq!(res.records_written, 1, "only the conforming row is written");
        assert_eq!(sink.written(), vec![json!({"id": 1})]);
        // The drifting row is enveloped in the DLQ.
        let dlq = dlq_sink.written();
        assert_eq!(dlq.len(), 1);
        assert_eq!(dlq[0]["payload"], json!({"id": 2, "email": "x"}));
        assert_eq!(dlq[0]["error"]["kind"], "SchemaDrift");
    }

    #[test]
    fn quarantine_drift_rows_covers_widening_and_droppable_required() {
        use crate::drift::{ColumnChange, SchemaDiff};
        // Widening on `amount`; required `legacy` column dropped from the page.
        let diff = SchemaDiff {
            additions: vec![],
            widenings: vec![ColumnChange {
                name: "amount".into(),
                from: Some(json!({"type":"integer"})),
                to: json!({"type":"number"}),
            }],
            incompatible: vec![],
            droppable_required: vec!["legacy".into()],
        };
        let records = vec![
            json!({"id": 1, "amount": 1.5, "legacy": "x"}), // touches widened col → DLQ
            json!({"id": 2, "amount": 7, "legacy": "y"}),   // touches widened col → DLQ
            json!({"id": 3, "legacy": "z"}),                // no widened col, has legacy → kept
            json!({"id": 4}),                               // missing required `legacy` → DLQ
        ];
        // page_indices offset by 10 to prove the envelope carries the TRUE page
        // index, not the survivor-relative one (#321 L6).
        let page_indices = vec![10, 11, 12, 13];
        let (kept, env) = quarantine_drift_rows(&diff, records, &page_indices, "sink", "pl", "");
        assert_eq!(kept, vec![json!({"id": 3, "legacy": "z"})]);
        assert_eq!(
            env.len(),
            3,
            "widening rows + the missing-required row quarantined"
        );
        // The three quarantined rows are at page positions 10, 11, 13.
        let indices: Vec<i64> = env
            .iter()
            .map(|e| e["record_index"].as_i64().unwrap())
            .collect();
        assert_eq!(indices, vec![10, 11, 13]);
    }

    #[tokio::test]
    async fn drift_quarantine_without_dlq_is_rejected() {
        let sink = SchemaSink::new(json!({"type":"object","properties":{}}), false);
        let policy = crate::drift::SchemaDriftPolicy {
            on_drift: crate::drift::OnDrift::Quarantine,
            allow_widening: true,
            on_incompatible: crate::drift::OnIncompatible::Fail,
            relax_nullability_on_missing: false,
        };
        let pages = one_page(vec![json!({"id": 1})]);
        let err = run_stream(pages, &sink, drift_opts(policy))
            .await
            .unwrap_err();
        assert!(matches!(err, FaucetError::Config(_)));
    }

    /// Regression: a drift `fail` abort must NOT discard a co-resident
    /// quality-quarantine envelope. With a DLQ present the abort is deferred
    /// (like the budget/circuit aborts) so the quarantined row reaches the DLQ
    /// before the run stops — dropping it on an early `return` would silently
    /// lose data.
    #[cfg(feature = "quality")]
    #[tokio::test]
    async fn drift_fail_with_dlq_still_routes_quality_quarantine() {
        use crate::dlq::DlqConfig;
        use crate::quality::{CompiledQuality, OnFailure, QualitySpec, RecordCheck};

        // Destination knows only `id`; the page carries an unknown `email`
        // column → drift, and one record fails the `name` NotNull check.
        let sink = SchemaSink::new(
            json!({"type":"object","properties":{"id":{"type":"integer"}}}),
            false,
        );
        let dlq_sink = std::sync::Arc::new(MockSink::new());
        let policy = crate::drift::SchemaDriftPolicy {
            on_drift: crate::drift::OnDrift::Fail,
            allow_widening: true,
            on_incompatible: crate::drift::OnIncompatible::Fail,
            relax_nullability_on_missing: false,
        };
        let spec = QualitySpec {
            record: vec![RecordCheck::NotNull {
                field: "name".into(),
                treat_missing_as_null: true,
                on_failure: OnFailure::Quarantine,
            }],
            batch: vec![],
        };
        let quality = std::sync::Arc::new(CompiledQuality::compile(&spec).unwrap());
        let pages = one_page(vec![
            json!({"id": 1, "name": "ok", "email": "a@x"}), // survives quality, drifts
            json!({"id": 2, "email": "b@x"}),               // quarantined (no name)
        ]);
        let opts = RunStreamOptions::new()
            .with_schema_drift(policy)
            .with_quality(quality)
            .with_dlq(DlqConfig::new(dlq_sink.clone()));
        let err = run_stream(pages, &sink, opts).await.unwrap_err();
        // The run still aborts on drift.
        assert!(
            matches!(err, FaucetError::SchemaDrift { .. }),
            "got {err:?}"
        );
        // …but the quality-quarantined row was written to the DLQ first, not lost.
        let dlq = dlq_sink.written();
        assert_eq!(
            dlq.len(),
            1,
            "quarantined row must reach the DLQ before abort"
        );
        assert_eq!(dlq[0]["payload"], json!({"id": 2, "email": "b@x"}));
        assert_eq!(dlq[0]["error"]["kind"], "QualityFailure");
        // The surviving (drifting) row was committed to the main sink before the abort.
        assert_eq!(
            sink.written(),
            vec![json!({"id": 1, "name": "ok", "email": "a@x"})]
        );
    }
}

#[cfg(test)]
mod cleanup_tests {
    //! Pipeline wiring for scoped cleanup (#478). The pure policy/accumulator
    //! logic lives in `crate::cleanup`; these cover the *timing*, which is the
    //! whole safety argument: when the delete fires, when it must not, and which
    //! records count as "written".
    use super::*;
    use crate::cleanup::{CleanupPolicy, CleanupTracker, SeenKeys};
    use async_trait::async_trait;
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};
    use tokio_util::sync::CancellationToken;

    /// Records what `cleanup_scope` was asked to do, so a test can assert both
    /// that it fired and with which key set.
    #[derive(Default)]
    struct CleanupSink {
        written: Mutex<Vec<Value>>,
        cleanup_calls: Mutex<Vec<Vec<String>>>,
        supports: bool,
        fail_writes: bool,
    }

    impl CleanupSink {
        fn new() -> Self {
            Self {
                supports: true,
                ..Default::default()
            }
        }
        fn unsupported() -> Self {
            Self::default()
        }
        fn failing() -> Self {
            Self {
                supports: true,
                fail_writes: true,
                ..Default::default()
            }
        }
        fn calls(&self) -> Vec<Vec<String>> {
            self.cleanup_calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl Sink for CleanupSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            if self.fail_writes {
                return Err(FaucetError::Sink("boom".into()));
            }
            self.written.lock().unwrap().extend(records.iter().cloned());
            Ok(records.len())
        }
        fn supports_cleanup(&self) -> bool {
            self.supports
        }
        async fn cleanup_scope(
            &self,
            scope: &BTreeMap<String, Value>,
            seen: &SeenKeys,
        ) -> Result<u64, FaucetError> {
            assert!(
                !scope.is_empty(),
                "the pipeline must never pass an empty scope"
            );
            let mut ids: Vec<String> = seen
                .keys()
                .iter()
                .map(|k| {
                    k.0.iter()
                        .map(|(_, v)| v.to_string())
                        .collect::<Vec<_>>()
                        .join("|")
                })
                .collect();
            ids.sort();
            self.cleanup_calls.lock().unwrap().push(ids);
            Ok(3)
        }
    }

    fn policy(max_keys: usize) -> Arc<CleanupPolicy> {
        Arc::new(
            CleanupPolicy::new(
                BTreeMap::from([("contact_id".to_string(), json!(7))]),
                vec!["id".to_string()],
                max_keys,
            )
            .unwrap(),
        )
    }

    /// Minimal source: yields one page of `records`.
    struct VecSource(Vec<Value>);

    #[async_trait]
    impl Source for VecSource {
        async fn fetch_with_context(
            &self,
            _context: &std::collections::HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(self.0.clone())
        }
    }

    fn src(records: Vec<Value>) -> VecSource {
        VecSource(records)
    }

    // ── CleanupTracker (the accumulator half) ────────────────────────────────

    #[tokio::test]
    async fn tracker_counts_rows_that_fail_into_the_dlq() {
        // A row handed to the sink that fails is still a record the source
        // claimed present, so it must count as seen — otherwise cleanup would
        // delete its destination row.
        let sink = CleanupSink::new();
        let p = policy(100);
        let tracker = CleanupTracker::new(&sink, &p);
        let page = vec![json!({"id": 1}), json!({"id": 2})];
        // The default `write_batch_partial` delegates to `write_batch`.
        tracker.write_batch_partial(&page).await.unwrap();
        assert_eq!(tracker.tracked(), 2);
    }

    #[tokio::test]
    async fn tracker_refuses_to_delete_after_an_overflow() {
        let sink = CleanupSink::new();
        let p = policy(1);
        let tracker = CleanupTracker::new(&sink, &p);
        tracker
            .write_batch(&[json!({"id": 1}), json!({"id": 2})])
            .await
            .unwrap();
        let err = tracker
            .finish(&p)
            .await
            .expect_err("an overflowed tracker must refuse");
        assert!(err.to_string().contains("Nothing was deleted"), "{err}");
        assert!(
            sink.calls().is_empty(),
            "the sink must never be asked to delete"
        );
    }

    // ── Pipeline timing (the firing half) ────────────────────────────────────

    #[tokio::test]
    async fn fires_once_at_a_natural_end_with_every_written_key() {
        let sink = CleanupSink::new();
        let source = src(vec![json!({"id": 1}), json!({"id": 2}), json!({"id": 3})]);
        let result = Pipeline::new(&source, &sink)
            .with_cleanup(policy(100))
            .run()
            .await
            .expect("run succeeds");
        assert_eq!(result.records_written, 3);
        let calls = sink.calls();
        assert_eq!(calls.len(), 1, "exactly one cleanup per run");
        assert_eq!(calls[0], vec!["1", "2", "3"]);
    }

    #[tokio::test]
    async fn empty_result_set_still_fires_with_no_keys() {
        // THE motivating case: a scope whose records all disappeared. The fetch
        // returns nothing, so the delete must clear the scope. A design that
        // inferred scopes from observed records would never fire here.
        let sink = CleanupSink::new();
        let source = src(vec![]);
        Pipeline::new(&source, &sink)
            .with_cleanup(policy(100))
            .run()
            .await
            .unwrap();
        let calls = sink.calls();
        assert_eq!(calls.len(), 1, "must still fire on an empty fetch");
        assert!(
            calls[0].is_empty(),
            "no keys written → delete the whole scope"
        );
    }

    #[tokio::test]
    async fn skipped_when_the_run_was_cancelled() {
        let sink = CleanupSink::new();
        let source = src(vec![json!({"id": 1})]);
        let token = CancellationToken::new();
        token.cancel();
        Pipeline::new(&source, &sink)
            .with_cleanup(policy(100))
            .with_cancel(token)
            .run()
            .await
            .expect("a cancelled run still returns Ok with partial output flushed");
        assert!(
            sink.calls().is_empty(),
            "cleanup must not run after a cancellation"
        );
    }

    #[tokio::test]
    async fn not_fired_when_the_run_failed() {
        let sink = CleanupSink::failing();
        let source = src(vec![json!({"id": 1})]);
        let err = Pipeline::new(&source, &sink)
            .with_cleanup(policy(100))
            .run()
            .await
            .expect_err("the write failure must propagate");
        assert!(err.to_string().contains("boom"), "{err}");
        assert!(
            sink.calls().is_empty(),
            "a failed run never wrote the authoritative set"
        );
    }

    #[tokio::test]
    async fn refuses_and_deletes_nothing_when_the_key_ceiling_is_breached() {
        let sink = CleanupSink::new();
        let source = src(vec![json!({"id": 1}), json!({"id": 2}), json!({"id": 3})]);
        let err = Pipeline::new(&source, &sink)
            .with_cleanup(policy(2))
            .run()
            .await
            .expect_err("must fail rather than partially delete");
        assert!(err.to_string().contains("Nothing was deleted"), "{err}");
        assert!(sink.calls().is_empty());
    }

    #[tokio::test]
    async fn rejects_a_sink_that_does_not_support_cleanup() {
        let sink = CleanupSink::unsupported();
        let source = src(vec![json!({"id": 1})]);
        let err = Pipeline::new(&source, &sink)
            .with_cleanup(policy(100))
            .run()
            .await
            .expect_err("unsupported sink must be rejected");
        assert!(
            err.to_string().contains("does not support scoped cleanup"),
            "{err}"
        );
        assert!(
            sink.written.lock().unwrap().is_empty(),
            "must fail before writing anything"
        );
    }

    #[tokio::test]
    async fn no_policy_leaves_the_write_path_untouched() {
        let sink = CleanupSink::new();
        let source = src(vec![json!({"id": 1})]);
        let result = Pipeline::new(&source, &sink).run().await.unwrap();
        assert_eq!(result.records_written, 1);
        assert!(sink.calls().is_empty(), "no policy → no cleanup");
    }

    #[tokio::test]
    async fn rows_missing_the_key_are_not_tracked_but_do_not_break_the_pass() {
        let sink = CleanupSink::new();
        let source = src(vec![json!({"id": 1}), json!({"no_key": true})]);
        Pipeline::new(&source, &sink)
            .with_cleanup(policy(100))
            .run()
            .await
            .unwrap();
        assert_eq!(sink.calls()[0], vec!["1"]);
    }

    // ── Native byte-passthrough fast path (#633) ─────────────────────────────

    /// A source that emits CSV bytes natively and also supports the `Value`
    /// path (so a fallback test can exercise both).
    struct NativeCsvSource {
        batches: usize,
    }

    #[async_trait]
    impl Source for NativeCsvSource {
        async fn fetch_with_context(
            &self,
            _context: &std::collections::HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(vec![json!({"a": 1})])
        }
        fn native_output_formats(&self) -> &'static [crate::native::NativeFormat] {
            &[crate::native::NativeFormat::Csv]
        }
        fn stream_native<'a>(
            &'a self,
            _context: &'a std::collections::HashMap<String, Value>,
            format: crate::native::NativeFormat,
            _batch_size: usize,
        ) -> Pin<Box<dyn Stream<Item = Result<crate::native::NativeBatch, FaucetError>> + Send + 'a>>
        {
            assert_eq!(format, crate::native::NativeFormat::Csv);
            let n = self.batches;
            Box::pin(async_stream::stream! {
                for i in 0..n {
                    let last = i + 1 == n;
                    let bytes = format!("h\nrow{i}\n").into_bytes();
                    let bm = if last { Some(json!({"page": i})) } else { None };
                    yield Ok(crate::native::NativeBatch::bytes(
                        crate::native::NativeFormat::Csv, bytes,
                    ).with_records(Some(1)).with_bookmark(bm));
                }
            })
        }
    }

    /// A sink advertising a BigQuery-like native CSV load capability, recording
    /// every `load_native` call plus the overwrite lifecycle.
    struct NativeRecordingSink {
        loads: Arc<std::sync::Mutex<Vec<(usize, bool)>>>, // (rows, first_batch)
        modes: Arc<std::sync::Mutex<Vec<crate::write_mode::WriteMode>>>,
        events: Arc<std::sync::Mutex<Vec<String>>>,
        overwrite: bool,
    }

    impl NativeRecordingSink {
        fn new(overwrite: bool) -> Self {
            Self {
                loads: Arc::new(std::sync::Mutex::new(Vec::new())),
                modes: Arc::new(std::sync::Mutex::new(Vec::new())),
                events: Arc::new(std::sync::Mutex::new(Vec::new())),
                overwrite,
            }
        }
    }

    #[async_trait]
    impl Sink for NativeRecordingSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            self.events
                .lock()
                .unwrap()
                .push(format!("write:{}", records.len()));
            Ok(records.len())
        }
        fn native_load_capabilities(&self) -> Vec<crate::native::NativeLoadCapability> {
            vec![crate::native::NativeLoadCapability {
                format: crate::native::NativeFormat::Csv,
                mechanism: "mock-load",
                prerequisites: crate::native::NativePrerequisites {
                    requires_passthrough: true,
                    delivery: &[crate::idempotency::DeliveryMode::AtLeastOnce],
                    write_modes: &[
                        crate::write_mode::WriteMode::Append,
                        crate::write_mode::WriteMode::Overwrite,
                    ],
                    forbids_dlq: true,
                },
            }]
        }
        async fn load_native(
            &self,
            batch: crate::native::NativeBatch,
            _scope: &str,
            ctx: crate::native::NativeLoadContext,
        ) -> Result<usize, FaucetError> {
            // Drain the payload to prove the sink consumes bytes, not `Value`.
            let rows = match batch.payload {
                crate::native::NativePayload::Bytes(b) => {
                    assert!(!b.is_empty());
                    batch.records.unwrap_or(0) as usize
                }
                crate::native::NativePayload::Stream(mut s) => {
                    use futures::StreamExt;
                    let mut got = false;
                    while let Some(chunk) = s.next().await {
                        chunk?;
                        got = true;
                    }
                    assert!(got);
                    batch.records.unwrap_or(0) as usize
                }
            };
            self.loads.lock().unwrap().push((rows, ctx.first_batch));
            self.modes.lock().unwrap().push(ctx.write_mode);
            Ok(rows)
        }
        fn is_overwrite(&self) -> bool {
            self.overwrite
        }
        async fn begin_overwrite(&self) -> Result<(), FaucetError> {
            self.events.lock().unwrap().push("begin".into());
            Ok(())
        }
        async fn commit_overwrite(&self) -> Result<(), FaucetError> {
            self.events.lock().unwrap().push("commit".into());
            Ok(())
        }
        async fn abort_overwrite(&self) -> Result<(), FaucetError> {
            self.events.lock().unwrap().push("abort".into());
            Ok(())
        }
    }

    #[tokio::test]
    async fn native_path_selected_and_appends() {
        let source = NativeCsvSource { batches: 2 };
        let sink = NativeRecordingSink::new(false);
        let loads = sink.loads.clone();
        let events = sink.events.clone();
        let result = Pipeline::new(&source, &sink).run().await.unwrap();
        // Two native loads, first flagged first_batch, second not.
        let loads = loads.lock().unwrap().clone();
        assert_eq!(loads, vec![(1, true), (1, false)]);
        assert_eq!(result.records_written, 2);
        assert_eq!(result.bookmark, Some(json!({"page": 1})));
        // The `Value` write path was never used.
        assert!(
            events.lock().unwrap().is_empty(),
            "no Value writes: {events:?}"
        );
    }

    #[tokio::test]
    async fn native_path_persists_bookmark() {
        let source = NativeCsvSource { batches: 1 };
        let sink = NativeRecordingSink::new(false);
        let store: Arc<dyn StateStore> = Arc::new(crate::state::MemoryStateStore::new());
        Pipeline::new(&source, &sink)
            .with_state_store(Arc::clone(&store))
            // A state key is needed for persistence; MemoryStateStore accepts any.
            .run()
            .await
            .unwrap();
        // NativeCsvSource has no state_key(), so nothing is persisted — assert the
        // run still succeeds via the native path (load happened).
        assert_eq!(sink.loads.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn native_path_overwrite_owned_by_mechanism_via_first_batch() {
        // An overwrite sink whose native mechanism lists `Overwrite` owns the
        // truncate/append itself via `NativeLoadContext` — the pipeline does NOT
        // drive the generic begin/commit staging lifecycle (that path is
        // `Value`-based and can't apply to byte loads).
        let source = NativeCsvSource { batches: 2 };
        let sink = NativeRecordingSink::new(true);
        let modes = sink.modes.clone();
        let loads = sink.loads.clone();
        let events = sink.events.clone();
        Pipeline::new(&source, &sink).run().await.unwrap();
        // Every load saw write_mode=Overwrite; the sink keys truncate off
        // first_batch (true then false).
        assert_eq!(
            *modes.lock().unwrap(),
            vec![
                crate::write_mode::WriteMode::Overwrite,
                crate::write_mode::WriteMode::Overwrite
            ]
        );
        assert_eq!(*loads.lock().unwrap(), vec![(1, true), (1, false)]);
        // The generic overwrite lifecycle was never invoked.
        assert!(
            events.lock().unwrap().is_empty(),
            "no begin/commit: {events:?}"
        );
    }

    #[tokio::test]
    async fn native_path_falls_back_to_value_when_dlq_present() {
        // A DLQ trips the `forbids_dlq` prerequisite → the planner returns None →
        // the pipeline uses the `Value` write path, never `load_native`.
        use crate::dlq::OnBatchError;
        let source = NativeCsvSource { batches: 1 };
        let sink = NativeRecordingSink::new(false);
        let loads = sink.loads.clone();
        let events = sink.events.clone();
        let dlq_sink: Arc<dyn Sink> = Arc::new(NativeRecordingSink::new(false));
        let dlq = DlqConfig {
            on_batch_error: OnBatchError::DlqAll,
            ..DlqConfig::new(dlq_sink)
        };
        Pipeline::new(&source, &sink)
            .with_dlq(dlq)
            .run()
            .await
            .unwrap();
        assert!(loads.lock().unwrap().is_empty(), "native path must not run");
        assert!(
            events
                .lock()
                .unwrap()
                .iter()
                .any(|e| e.starts_with("write:")),
            "Value path must run: {events:?}"
        );
    }
}
