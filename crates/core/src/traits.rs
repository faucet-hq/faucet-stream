//! Shared traits for faucet sources and sinks.

use crate::error::FaucetError;
use crate::pipeline::StreamPage;
use async_trait::async_trait;
use futures_core::Stream;
use serde_json::Value;
use std::pin::Pin;

/// A source fetches records from an external system.
#[async_trait]
pub trait Source: Send + Sync {
    /// Primary fetch method. Receives context from a parent source's records.
    ///
    /// An empty context map means this is a root source (no parent).
    /// Connectors that support being a child should use
    /// [`substitute_context()`](crate::util::substitute_context) to resolve
    /// `{placeholder}` tokens in their URL path, query parameters, headers,
    /// or body. Connectors that don't need parent context ignore the map.
    async fn fetch_with_context(
        &self,
        context: &std::collections::HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError>;

    /// Convenience: fetch with no parent context.
    async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
        self.fetch_with_context(&std::collections::HashMap::new())
            .await
    }

    /// Incremental fetch with parent context support.
    ///
    /// Returns the records and an optional bookmark value for incremental
    /// replication. The default delegates to `fetch_with_context` and
    /// returns `None` for the bookmark.
    async fn fetch_with_context_incremental(
        &self,
        context: &std::collections::HashMap<String, Value>,
    ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        let records = self.fetch_with_context(context).await?;
        Ok((records, None))
    }

    /// Convenience: incremental fetch with no parent context.
    async fn fetch_all_incremental(&self) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        self.fetch_with_context_incremental(&std::collections::HashMap::new())
            .await
    }

    /// Stream records page-by-page so the pipeline can write to the sink as
    /// pages arrive instead of buffering the full result set.
    ///
    /// `batch_size` is the *hint* the pipeline passes down; sources are free
    /// to use a larger or smaller native chunk (e.g. one page per HTTP
    /// response, one row-group per Parquet file) but should approximate it
    /// where feasible. The special value `batch_size = 0` means "do not
    /// batch — emit the entire result set in a single page." Sources that
    /// stream natively should treat `0` as "skip the chunking layer and
    /// yield one page after the underlying read completes" (useful for
    /// small lookup tables or for sinks like SQL `COPY` / BigQuery load
    /// jobs that prefer one large request).
    ///
    /// The default implementation fetches the full result set via
    /// [`fetch_with_context_incremental`](Self::fetch_with_context_incremental)
    /// and chunks it in memory by `batch_size`. The bookmark (when present)
    /// is attached to the *final* page so the pipeline only persists after
    /// the entire fetch has been written. Sources that can stream natively
    /// override this method and may emit per-page bookmarks (e.g. CDC).
    ///
    /// An empty result with a `Some(bookmark)` still yields one empty page
    /// carrying the bookmark, so incremental runs that produce no records
    /// still advance their checkpoint.
    fn stream_pages<'a>(
        &'a self,
        context: &'a std::collections::HashMap<String, Value>,
        batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        Box::pin(async_stream::try_stream! {
            let (records, bookmark) = self
                .fetch_with_context_incremental(context)
                .await?;
            let total = records.len();
            // batch_size == 0 means "no batching" — emit all records as one
            // page. Otherwise chunk into pages of size `batch_size`.
            let chunk = if batch_size == 0 { usize::MAX } else { batch_size };

            if total == 0 {
                if bookmark.is_some() {
                    yield StreamPage {
                        records: Vec::new(),
                        bookmark,
                    };
                }
                return;
            }

            let mut iter = records.into_iter();
            let mut consumed = 0usize;
            loop {
                let batch: Vec<Value> = iter.by_ref().take(chunk).collect();
                if batch.is_empty() {
                    break;
                }
                consumed += batch.len();
                let page_bookmark = if consumed >= total {
                    bookmark.clone()
                } else {
                    None
                };
                yield StreamPage {
                    records: batch,
                    bookmark: page_bookmark,
                };
            }
        })
    }

    /// Whether this source can emit **columnar** ([`ColumnarPage`](crate::columnar::ColumnarPage))
    /// pages via [`stream_batches`](Self::stream_batches). Default: `false`.
    ///
    /// The pipeline uses the columnar fast path only when *both* the source and
    /// sink return `true` here (and no `Value`-shaped stage needs to observe the
    /// records), so an Arrow-native `parquet → parquet` chain never materializes
    /// `Value`. Opt-in and additive — see [`crate::columnar`] (RFC 0002 / #375).
    #[cfg(feature = "arrow")]
    fn supports_columnar(&self) -> bool {
        false
    }

    /// Stream the source natively as Arrow
    /// [`ColumnarPage`](crate::columnar::ColumnarPage)s.
    ///
    /// Only invoked when [`supports_columnar`](Self::supports_columnar) returns
    /// `true`; the default yields a single typed "unsupported" error so a source
    /// that advertises support but forgets to override this fails loudly rather
    /// than silently. Each page's `bookmark` carries the same checkpoint
    /// semantics as [`StreamPage`].
    #[cfg(feature = "arrow")]
    fn stream_batches<'a>(
        &'a self,
        context: &'a std::collections::HashMap<String, Value>,
        batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<crate::columnar::ColumnarPage, FaucetError>> + Send + 'a>>
    {
        let _ = (context, batch_size);
        let name = self.connector_name();
        let err: Result<crate::columnar::ColumnarPage, FaucetError> = Err(FaucetError::Source(
            format!("source '{name}' does not support columnar streaming (stream_batches)"),
        ));
        Box::pin(futures::stream::once(async move { err }))
    }

    /// Wire formats this source can emit as raw bytes for the **native
    /// byte-passthrough** fast path (#633), in preference order (first = best).
    /// Default: `&[]` (no native fast path).
    ///
    /// The pipeline uses the path only when this returns a non-empty slice, a
    /// sink advertises a matching [`NativeLoadCapability`](crate::NativeLoadCapability),
    /// and every prerequisite holds (see [`plan_native_transfer`](crate::plan_native_transfer)).
    /// A [`TransformingSource`](crate::TransformingSource) deliberately does not
    /// override this, so any attached transform disables the fast path.
    fn native_output_formats(&self) -> &'static [crate::native::NativeFormat] {
        &[]
    }

    /// Stream the source natively as byte batches in `format` (#633).
    ///
    /// Only invoked after the pipeline negotiated `format` — one this source
    /// advertised via [`native_output_formats`](Self::native_output_formats). The
    /// default yields a single typed "unsupported" error so a source that
    /// advertises support but forgets to override this fails loudly. Each batch's
    /// `bookmark` carries the same checkpoint semantics as [`StreamPage`].
    fn stream_native<'a>(
        &'a self,
        context: &'a std::collections::HashMap<String, Value>,
        format: crate::native::NativeFormat,
        batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<crate::native::NativeBatch, FaucetError>> + Send + 'a>>
    {
        let _ = (context, batch_size, format);
        let name = self.connector_name();
        let err: Result<crate::native::NativeBatch, FaucetError> = Err(FaucetError::Source(
            format!("source '{name}' does not support native byte streaming (stream_native)"),
        ));
        Box::pin(futures::stream::once(async move { err }))
    }

    /// Return a JSON Schema describing the configuration this source accepts.
    fn config_schema(&self) -> Value {
        serde_json::json!({"type": "object", "properties": {}})
    }

    /// Stable key under which this source's incremental-replication bookmark
    /// should be persisted in a [`StateStore`](crate::state::StateStore).
    ///
    /// Returning `Some(key)` opts this source into resumable runs: when the
    /// pipeline is configured with a state store via
    /// [`Pipeline::with_state_store`](crate::Pipeline::with_state_store), it
    /// reads the bookmark at `key` before fetching and writes the new
    /// bookmark back only after the sink confirms the batch was written.
    ///
    /// The default returns `None`, meaning the source is not persisted.
    /// Keys must satisfy [`validate_state_key`](crate::state::validate_state_key).
    fn state_key(&self) -> Option<String> {
        None
    }

    /// Apply a bookmark loaded from a [`StateStore`](crate::state::StateStore)
    /// as this run's starting point.
    ///
    /// The default implementation ignores the value, which keeps existing
    /// sources backwards-compatible. Sources that support incremental
    /// replication override this — typically by storing the value behind
    /// interior mutability and consulting it inside
    /// `fetch_with_context_incremental`.
    async fn apply_start_bookmark(&self, _bookmark: Value) -> Result<(), FaucetError> {
        Ok(())
    }

    /// Capture the source's current replication position **without consuming
    /// any changes**, ensuring any server-side resource (e.g. a logical
    /// replication slot) needed to later resume from that position exists.
    ///
    /// Returns the position as a bookmark [`Value`] — the same shape
    /// [`apply_start_bookmark`](Self::apply_start_bookmark) accepts — or `None`
    /// if this source does not support position capture.
    ///
    /// Used by the snapshot→CDC replication orchestrator (`faucet replicate`)
    /// to anchor the CDC stream at-or-before the bulk snapshot's read point so
    /// the handoff has no gap. The default returns `None`.
    async fn capture_resume_position(&self) -> Result<Option<Value>, FaucetError> {
        Ok(None)
    }

    /// Whether this source **deterministically replays** the same page sequence
    /// from a given bookmark — the requirement for the atomic-watermark
    /// effectively-once path (a non-deterministic replay could cause the pipeline
    /// to skip a page whose contents differ from the one already committed).
    /// Default: `false`.
    ///
    /// Sources with a durable monotonic position and per-page bookmarks (CDC)
    /// override this to return `true`. The pipeline rejects
    /// `DeliveryMode::ExactlyOnce` against a source that returns `false`.
    fn supports_exactly_once(&self) -> bool {
        false
    }

    /// The typed replay capability this source advertises — see
    /// [`ReplayGuarantee`](crate::ReplayGuarantee).
    ///
    /// The default derives from [`supports_exactly_once`](Self::supports_exactly_once)
    /// (the boolean stays the back-compat primitive: existing connectors that
    /// override only the boolean automatically advertise `Deterministic`
    /// here). Override this directly only to *diverge* from the boolean —
    /// there is currently no reason to.
    fn replay_guarantee(&self) -> crate::idempotency::ReplayGuarantee {
        if self.supports_exactly_once() {
            crate::idempotency::ReplayGuarantee::Deterministic
        } else {
            crate::idempotency::ReplayGuarantee::NonDeterministic
        }
    }

    /// Whether this source can split its work into independent shards for
    /// clustered (Mode B) execution. Default: `false` (single whole-dataset
    /// shard). Sources with a natural partition (object-store prefixes, table
    /// primary-key ranges) override this to `true` and implement
    /// [`enumerate_shards`](Self::enumerate_shards) +
    /// [`apply_shard`](Self::apply_shard).
    fn is_shardable(&self) -> bool {
        false
    }

    /// Enumerate the shards this source splits into, aiming for roughly `target`
    /// of them (a hint — the source may return fewer, e.g. when the dataset is
    /// small, or one per natural partition regardless of `target`).
    ///
    /// Called **once per run** by the cluster coordinator; enumeration must be
    /// deterministic enough that re-enumeration yields a compatible set (stable
    /// shard ids), since it may run on more than one instance and is reconciled
    /// by idempotent insert. May perform read-only I/O (a `LIST`, a `MIN/MAX`
    /// query). The default returns a single whole-dataset shard
    /// ([`ShardSpec::whole`](crate::ShardSpec::whole)), preserving today's
    /// single-worker behavior.
    async fn enumerate_shards(
        &self,
        _target: usize,
    ) -> Result<Vec<crate::shard::ShardSpec>, FaucetError> {
        Ok(vec![crate::shard::ShardSpec::whole()])
    }

    /// Narrow this source instance to a single shard before streaming.
    ///
    /// Called on the worker that claims `shard`, after construction and before
    /// any `stream_pages` call. Like [`apply_start_bookmark`](Self::apply_start_bookmark)
    /// this takes `&self` and is expected to record the shard behind interior
    /// mutability (the source consults it when building its query / listing).
    /// The default ignores the shard — a non-shardable source only ever receives
    /// [`ShardSpec::whole`](crate::ShardSpec::whole), so ignoring it streams the
    /// whole dataset. Implementations should accept the whole shard as a no-op.
    async fn apply_shard(&self, _shard: &crate::shard::ShardSpec) -> Result<(), FaucetError> {
        Ok(())
    }

    /// Whether this source can enumerate the datasets behind its connection
    /// via [`discover`](Self::discover). Default: `false`. Sources backed by
    /// an introspectable catalog (database `information_schema`, MongoDB
    /// collections, Elasticsearch indices, object-store prefixes) override
    /// this to `true`.
    fn supports_discover(&self) -> bool {
        false
    }

    /// Enumerate the datasets living behind this source's connection — one
    /// [`DatasetDescriptor`](crate::discover::DatasetDescriptor) per table /
    /// collection / index / prefix, each carrying a partial config override
    /// that selects it (used by `faucet discover` to scaffold one matrix row
    /// per dataset).
    ///
    /// Must be **read-only and cheap**: catalog metadata queries and listings
    /// only, never a data scan. Descriptors must never embed credentials.
    /// The default returns a typed "unsupported" error; override it (and
    /// return `true` from [`supports_discover`](Self::supports_discover))
    /// only for sources with a real catalog to introspect.
    async fn discover(&self) -> Result<Vec<crate::discover::DatasetDescriptor>, FaucetError> {
        Err(FaucetError::Source(format!(
            "source '{}' does not support dataset discovery",
            self.connector_name()
        )))
    }

    /// Stable identifier used as the `connector` label on metrics and the
    /// `connector` attribute on spans. Defaults to the final segment of
    /// `std::any::type_name::<Self>()`, e.g. `"RestSource"`. Built-in
    /// connectors override with a short, friendly snake_case name (e.g.
    /// `"rest"`). Must return a non-empty string; observability decorators
    /// fall back to `"unknown"` in release builds if it is empty (and
    /// `debug_assert!` in debug builds).
    fn connector_name(&self) -> &'static str {
        crate::observability::strip_type_name(std::any::type_name::<Self>())
    }

    /// Logical dataset identity for lineage emission, following OpenLineage
    /// naming conventions (<https://openlineage.io/docs/spec/naming>).
    ///
    /// The default returns `"<connector_name>://unknown"`. Built-in connectors
    /// override with a credential-free URI derived from their config. Strip any
    /// credentials with [`redact_uri_credentials`](crate::redact_uri_credentials).
    /// Informational metadata only — never used for I/O.
    fn dataset_uri(&self) -> String {
        format!("{}://unknown", self.connector_name())
    }

    /// Run a fast, non-mutating preflight probe (used by `faucet doctor`).
    ///
    /// The default pulls a **single page** via
    /// [`stream_pages`](Self::stream_pages) and reports success/failure — it
    /// exercises the real read path (DNS, TLS, auth, the first request, the
    /// first-record decode) but never paginates the full dataset and never
    /// repeats. The page stream is dropped immediately after the first page.
    ///
    /// Sources whose first page *blocks* waiting for inbound data (webhook,
    /// websocket) or has *side effects* (CDC consuming WAL) override this with a
    /// cheaper, side-effect-free probe. Probe-level failures are returned as a
    /// [`ProbeStatus::Fail`](crate::check::ProbeStatus) inside `Ok(report)`.
    async fn check(
        &self,
        ctx: &crate::check::CheckContext,
    ) -> Result<crate::check::CheckReport, FaucetError> {
        use crate::check::{CheckReport, Probe};
        use futures::StreamExt;

        let empty = std::collections::HashMap::new();
        let start = std::time::Instant::now();
        let mut pages = self.stream_pages(&empty, 1);
        let probe = match tokio::time::timeout(ctx.timeout, pages.next()).await {
            Err(_) => Probe::fail("read", start.elapsed(), "timed out fetching first page"),
            Ok(None) | Ok(Some(Ok(_))) => Probe::pass("read", start.elapsed()),
            Ok(Some(Err(e))) => Probe::fail("read", start.elapsed(), e.to_string()),
        };
        Ok(CheckReport::single(probe))
    }
}

/// Per-row outcome from [`Sink::write_batch_partial`].
///
/// `Ok(())` — the row was durably written to the sink.
/// `Err(_)` — the row failed; the pipeline will route it to the DLQ when
/// one is configured.
pub type RowOutcome = Result<(), FaucetError>;

/// A sink writes records to an external system.
#[async_trait]
pub trait Sink: Send + Sync {
    /// Write a batch of records to the destination.
    ///
    /// Returns the number of records successfully written.
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError>;

    /// Flush any buffered data to the destination.
    ///
    /// The default implementation is a no-op (suitable for sinks that
    /// write immediately in `write_batch`).
    async fn flush(&self) -> Result<(), FaucetError> {
        Ok(())
    }

    /// Write a batch and report per-row outcomes.
    ///
    /// Sinks whose underlying API exposes per-row results (BigQuery
    /// `insertAll`, Elasticsearch `_bulk`) override this. The default
    /// implementation delegates to [`Self::write_batch`] and maps a single success
    /// onto a uniform all-`Ok(())` vector. An outer failure is bubbled up
    /// unchanged so the pipeline's DLQ router can apply its `on_batch_error`
    /// policy at a single decision point.
    async fn write_batch_partial(&self, records: &[Value]) -> Result<Vec<RowOutcome>, FaucetError> {
        self.write_batch(records).await?;
        Ok(records.iter().map(|_| Ok(())).collect())
    }

    /// Whether this sink can consume **columnar** (`arrow::RecordBatch`) writes
    /// via [`write_batch_columnar`](Self::write_batch_columnar) without first
    /// converting to `Value`. Default: `false`.
    ///
    /// The pipeline takes the columnar fast path only when *both* the source and
    /// sink return `true` (RFC 0002 / #375).
    #[cfg(feature = "arrow")]
    fn supports_columnar(&self) -> bool {
        false
    }

    /// Write a columnar `RecordBatch` to the destination, returning the number of
    /// rows written.
    ///
    /// The default converts the batch to `Value` rows via the
    /// [`columnar`](crate::columnar) shim and delegates to
    /// [`write_batch`](Self::write_batch), so every sink participates correctly
    /// even without a native columnar path. Sinks that write Arrow/Parquet
    /// directly override this — and return `true` from
    /// [`supports_columnar`](Self::supports_columnar) — to skip the conversion.
    #[cfg(feature = "arrow")]
    async fn write_batch_columnar(
        &self,
        batch: &arrow::array::RecordBatch,
    ) -> Result<usize, FaucetError> {
        let rows = crate::columnar::record_batch_to_values(batch)?;
        self.write_batch(&rows).await
    }

    /// Native byte-passthrough load mechanisms this sink offers, each naming
    /// the wire format and the write modes it can honor (#633). Default:
    /// `vec![]` (no fast path; use [`write_batch`](Self::write_batch)).
    ///
    /// The pipeline takes the native path only when a source advertises a
    /// format this returns and the run passes the pipeline-owned gates — no
    /// transforms, no quality/contract/masking pass, no DLQ, at-least-once
    /// delivery, no upsert/delete `write_mode`, and (for overwrite) an
    /// all-or-nothing load session finalized by a single terminal `flush` —
    /// see [`plan_native_transfer`](crate::plan_native_transfer). A capability
    /// cannot waive those gates.
    fn native_load_capabilities(&self) -> Vec<crate::native::NativeLoadCapability> {
        Vec::new()
    }

    /// Bulk-load one native-format byte batch directly (#633), returning the
    /// number of rows written.
    ///
    /// Only invoked when the pipeline negotiated one of this sink's
    /// [`native_load_capabilities`](Self::native_load_capabilities) and the
    /// pipeline-owned gates passed. `ctx.first_batch` lets an overwrite sink
    /// truncate on the first load and append thereafter. The default returns a
    /// typed "unsupported" error so a sink that advertises a capability but
    /// forgets to override this fails loudly.
    ///
    /// **Contract:** an **empty payload must be a successful no-op** returning
    /// `Ok(0)` — sources emit a trailing empty batch to carry the final
    /// bookmark, and an error here would break bookmark persistence. Under
    /// `Overwrite`, nothing may become visible in the destination until the
    /// terminal [`flush`](Self::flush) — the pipeline flushes exactly once, on
    /// success only, so a mechanism that commits per batch must omit
    /// `Overwrite` from its capability's `write_modes`.
    async fn load_native(
        &self,
        batch: crate::native::NativeBatch,
        scope: &str,
        ctx: crate::native::NativeLoadContext,
    ) -> Result<usize, FaucetError> {
        let _ = (batch, scope, ctx);
        Err(FaucetError::Sink(format!(
            "sink '{}' does not support native byte loading (load_native)",
            self.connector_name()
        )))
    }

    /// Whether this sink can durably commit a page's rows **and** a commit token
    /// in a single atomic transaction. Default: `false` (at-least-once only).
    ///
    /// Only return `true` from a sink that genuinely commits both atomically —
    /// see [`write_batch_idempotent`](Self::write_batch_idempotent). The pipeline
    /// rejects `DeliveryMode::ExactlyOnce` against a sink that returns `false`.
    fn supports_idempotent_writes(&self) -> bool {
        false
    }

    /// The strongest delivery guarantee this sink can uphold — see
    /// [`SinkGuarantee`](crate::SinkGuarantee).
    ///
    /// The default derives from the two back-compat primitives:
    /// [`supports_idempotent_writes`](Self::supports_idempotent_writes) →
    /// `AtomicWatermark`, else an upsert-capable
    /// [`supported_write_modes`](Self::supported_write_modes) → `KeyedUpsert`,
    /// else `AtLeastOnce`. Existing connectors that override only the
    /// primitives automatically advertise the right capability here.
    fn sink_guarantee(&self) -> crate::idempotency::SinkGuarantee {
        if self.supports_idempotent_writes() {
            crate::idempotency::SinkGuarantee::AtomicWatermark
        } else if self
            .supported_write_modes()
            .contains(&crate::write_mode::WriteMode::Upsert)
        {
            crate::idempotency::SinkGuarantee::KeyedUpsert
        } else {
            crate::idempotency::SinkGuarantee::AtLeastOnce
        }
    }

    /// Whether this sink instance is **configured** to dedup by key — i.e.
    /// `write_mode: upsert` (or `delete`) with a non-empty `key`, so
    /// re-applying a record with the same key converges instead of
    /// duplicating. Default: `false`.
    ///
    /// Distinct from [`sink_guarantee`](Self::sink_guarantee) (capability):
    /// this reflects the *live config*. Sinks that flatten a
    /// [`WriteSpec`](crate::write_mode::WriteSpec) into their config override
    /// it as `self.config.write.dedups_by_key()`. The pipeline consults it to
    /// derive the keyed-upsert effectively-once mechanism at run time.
    fn dedups_by_key(&self) -> bool {
        false
    }

    /// Write modes this sink can apply. Default: append-only. Sinks that
    /// implement key-based merge override this to include
    /// [`WriteMode::Upsert`](crate::write_mode::WriteMode) /
    /// [`WriteMode::Delete`](crate::write_mode::WriteMode). The CLI rejects a
    /// configured mode that is not in this set at config-load time.
    fn supported_write_modes(&self) -> &'static [crate::write_mode::WriteMode] {
        &[crate::write_mode::WriteMode::Append]
    }

    /// The sink's live destination schema as an `infer_schema`-shaped object
    /// (`{"type":"object","properties":{ <col>: <type-fragment>, … }}`), or
    /// `None` for a schemaless sink or a target that does not exist yet.
    ///
    /// Used by the schema-drift policy to diff each page's shape against the
    /// real destination. Default: `Ok(None)` (drift handling is inert).
    async fn current_schema(&self) -> Result<Option<Value>, FaucetError> {
        Ok(None)
    }

    /// Whether this sink can apply additive/widening DDL via
    /// [`evolve_schema`](Self::evolve_schema). Default: `false`. The CLI rejects
    /// `on_drift: evolve` against a sink that returns `false` at config-load.
    fn supports_schema_evolution(&self) -> bool {
        false
    }

    /// Apply an additive schema evolution (new columns, lossless widenings,
    /// nullability relaxations) to the destination. MUST be idempotent
    /// (`ADD COLUMN IF NOT EXISTS` semantics) so concurrent runs converge.
    ///
    /// Default: a typed "unsupported" error. Override only when the backend
    /// supports in-place additive DDL (and return `true` from
    /// `supports_schema_evolution`).
    async fn evolve_schema(
        &self,
        evolution: &crate::drift::SchemaEvolution,
    ) -> Result<(), FaucetError> {
        let _ = evolution;
        Err(FaucetError::Sink(format!(
            "sink '{}' does not support schema evolution",
            self.connector_name()
        )))
    }

    /// Whether this sink can delete a scoped set of rows for scoped cleanup
    /// (#478). Default `false`; the upsert-capable sinks override it.
    fn supports_cleanup(&self) -> bool {
        false
    }

    /// Whether this sink can bulk-load via an object-store stage — write the
    /// page to S3/GCS/Azure, then pull it with the warehouse's native load
    /// command (`COPY` / `COPY INTO` / `s3()` table function) (#528). Default
    /// `false`; warehouse sinks that honour a [`StagingSpec`](crate::StagingSpec)
    /// override it. Object-safe (no args, no generics) so `Box<dyn Sink>` is
    /// unaffected.
    fn supports_staged_load(&self) -> bool {
        false
    }

    /// Delete rows matching `scope` whose key is **not** in `seen`.
    ///
    /// Called at most once per invocation, only after the run completed
    /// successfully and uncancelled — see [`crate::cleanup`] for why the timing
    /// is load-bearing. `scope` is a set of equality predicates in destination
    /// column terms, AND-ed together; `seen` holds the key tuples this run wrote.
    ///
    /// Returns the number of rows deleted. Implementations **must** be
    /// all-or-nothing where the backend allows it: a partial delete would remove
    /// rows the run actually wrote.
    ///
    /// The default is a typed "unsupported" error, so no existing or third-party
    /// connector breaks.
    async fn cleanup_scope(
        &self,
        scope: &std::collections::BTreeMap<String, Value>,
        seen: &crate::cleanup::SeenKeys,
    ) -> Result<u64, FaucetError> {
        let _ = (scope, seen);
        Err(FaucetError::Sink(format!(
            "sink '{}' does not support scoped cleanup",
            self.connector_name()
        )))
    }

    /// Write `records` AND durably record `token` for `scope`, atomically.
    ///
    /// `scope` namespaces the watermark (the pipeline passes the per-row state
    /// key, e.g. `"{name}::{row_id}"`). `token` is a monotonic, fixed-width
    /// string (see [`format_token`](crate::format_token)).
    ///
    /// The default is **not** idempotent: it ignores the token and delegates to
    /// [`write_batch`](Self::write_batch). Override only when the commit is
    /// genuinely atomic (and return `true` from `supports_idempotent_writes`).
    async fn write_batch_idempotent(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<usize, FaucetError> {
        let _ = (scope, token);
        self.write_batch(records).await
    }

    /// The last token durably committed for `scope`, or `None` if this sink has
    /// never committed under that scope. Default: `None`.
    async fn last_committed_token(&self, scope: &str) -> Result<Option<String>, FaucetError> {
        let _ = scope;
        Ok(None)
    }

    /// Whether this sink instance is configured for full-destination
    /// replacement ([`WriteMode::Overwrite`](crate::write_mode::WriteMode)).
    ///
    /// The pipeline consults this to drive the overwrite lifecycle:
    /// [`begin_overwrite`](Self::begin_overwrite) before the first page, then
    /// [`commit_overwrite`](Self::commit_overwrite) once the run finishes
    /// successfully, or [`abort_overwrite`](Self::abort_overwrite) on
    /// failure/cancel. Sinks that flatten a [`WriteSpec`](crate::write_mode::WriteSpec)
    /// into their config return `self.config.write.is_overwrite()`. Default `false`.
    fn is_overwrite(&self) -> bool {
        false
    }

    /// Prepare a staging target for an overwrite run.
    ///
    /// Called once, before the first [`write_batch`](Self::write_batch), only
    /// when [`is_overwrite`](Self::is_overwrite) is true. The sink stages this
    /// run's writes (a temp table / new index / temp prefix) so the existing
    /// destination is untouched until the run succeeds. Subsequent
    /// `write_batch` calls for this sink must land in the staging target.
    ///
    /// Default: a typed "unsupported" error, so a sink that advertises
    /// `WriteMode::Overwrite` but forgets to implement the lifecycle fails
    /// loudly rather than silently appending. Never called for sinks whose
    /// `is_overwrite()` is false.
    async fn begin_overwrite(&self) -> Result<(), FaucetError> {
        Err(FaucetError::Sink(format!(
            "sink '{}' does not support write_mode: overwrite",
            self.connector_name()
        )))
    }

    /// Atomically replace the destination with the staged data.
    ///
    /// Called **once, only after the run completed successfully and
    /// uncancelled**. Implementations MUST swap staging → destination
    /// atomically (or as close as the backend allows) so a reader never sees a
    /// half-replaced dataset, and MUST NOT have destroyed the prior contents
    /// before this point — a failed run leaves the old data in place.
    ///
    /// Default: a typed "unsupported" error (unreachable for a correct sink
    /// whose `is_overwrite()` is false).
    async fn commit_overwrite(&self) -> Result<(), FaucetError> {
        Err(FaucetError::Sink(format!(
            "sink '{}' does not support write_mode: overwrite",
            self.connector_name()
        )))
    }

    /// Discard the staging target after a failed or cancelled overwrite run.
    ///
    /// Called (best-effort) when an overwrite run does not reach
    /// [`commit_overwrite`](Self::commit_overwrite). The destination must be
    /// left exactly as it was before the run. Default: no-op — a leftover
    /// staging object is untidy but never data loss, so a sink may skip it.
    async fn abort_overwrite(&self) -> Result<(), FaucetError> {
        Ok(())
    }

    /// Return a JSON Schema describing the configuration this sink accepts.
    ///
    /// The schema is auto-generated from the config struct using `schemars`.
    /// Callers can inspect it to discover required fields, types, defaults,
    /// and descriptions before constructing the sink.
    ///
    /// The default returns an empty object schema.
    fn config_schema(&self) -> Value {
        serde_json::json!({"type": "object", "properties": {}})
    }

    /// Stable identifier used as the `connector` label on metrics and the
    /// `connector` attribute on spans. See `Source::connector_name`.
    fn connector_name(&self) -> &'static str {
        crate::observability::strip_type_name(std::any::type_name::<Self>())
    }

    /// Logical dataset identity for lineage emission, following OpenLineage
    /// naming conventions (<https://openlineage.io/docs/spec/naming>).
    ///
    /// The default returns `"<connector_name>://unknown"`. Built-in connectors
    /// override with a credential-free URI derived from their config. Strip any
    /// credentials with [`redact_uri_credentials`](crate::redact_uri_credentials).
    /// Informational metadata only — never used for I/O.
    fn dataset_uri(&self) -> String {
        format!("{}://unknown", self.connector_name())
    }

    /// The concrete **local files** this sink instance opened during the run.
    ///
    /// The provenance record faucet's local-output retention GC (#587) deletes
    /// from: it may only remove files faucet recorded here, never a glob or a
    /// directory. A sink that writes local files (jsonl, csv, parquet to a local
    /// destination) accumulates a
    /// [`LocalOutputLog`](crate::local_outputs::LocalOutputLog) as it opens them
    /// and returns its snapshot; every other sink keeps the empty default, which
    /// simply means "nothing local to collect".
    ///
    /// Two properties the GC depends on, so an implementation must preserve them:
    ///
    /// 1. **Every file, individually named.** A rolling parquet sink returns one
    ///    entry per UUID-named part — the directory itself is not an output.
    /// 2. **[`LocalOutput::pre_existing`](crate::local_outputs::LocalOutput) is
    ///    honest**, captured before the first open. A file faucet appended to but
    ///    did not create is never collected.
    ///
    /// Called after the run (and by `faucet cleanup`), never on the data path, so
    /// an implementation may take a lock. Decorating sinks **must forward** this
    /// to their inner sink, exactly as they do
    /// [`dataset_uri`](Self::dataset_uri) — a decorator that returns the default
    /// hides its inner sink's files from the GC and they are never reclaimed.
    async fn local_outputs(&self) -> Vec<crate::local_outputs::LocalOutput> {
        Vec::new()
    }

    /// Run a fast, non-mutating preflight probe (used by `faucet doctor`).
    ///
    /// Unlike sources, a sink has no non-mutating "first page" equivalent
    /// (`write_batch` mutates the destination), so the default returns
    /// [`CheckReport::not_implemented`](crate::check::CheckReport::not_implemented).
    /// Built-in sinks override this with a connect / auth / metadata probe.
    ///
    /// The probe **MUST be idempotent and side-effect-free** — no inserts, no
    /// residual rows or objects — and must never put credentials or connection
    /// strings in a probe `reason`/`hint`.
    async fn check(
        &self,
        _ctx: &crate::check::CheckContext,
    ) -> Result<crate::check::CheckReport, FaucetError> {
        Ok(crate::check::CheckReport::not_implemented())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── Mock Source ──────────────────────────────────────────────────────────

    struct MockSource {
        records: Vec<Value>,
    }

    #[async_trait]
    impl Source for MockSource {
        async fn fetch_with_context(
            &self,
            _context: &std::collections::HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(self.records.clone())
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

    struct MockSink {
        written: std::sync::Mutex<Vec<Value>>,
    }

    impl MockSink {
        fn new() -> Self {
            Self {
                written: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl Sink for MockSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            let mut w = self.written.lock().unwrap();
            w.extend(records.iter().cloned());
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

    // ── Native byte-passthrough defaults (#633) ──────────────────────────────

    /// A source/sink pair that advertises nothing native, so the defaulted
    /// trait methods are what answer.
    struct PlainSink;

    #[async_trait]
    impl Sink for PlainSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            Ok(records.len())
        }
    }

    #[tokio::test]
    async fn native_defaults_advertise_nothing_and_fail_loudly() {
        use futures::StreamExt as _;

        // A source that never overrides the native methods advertises no
        // formats, so the pipeline can never negotiate the byte path…
        let src = MockSource { records: vec![] };
        assert!(src.native_output_formats().is_empty());
        // …and calling it anyway yields one typed error rather than silently
        // producing an empty stream (which would look like "no rows").
        let ctx = std::collections::HashMap::new();
        let mut batches = src.stream_native(&ctx, crate::native::NativeFormat::NdJson, 10);
        let err = batches
            .next()
            .await
            .expect("one item")
            .expect_err("default must error");
        assert!(
            err.to_string()
                .contains("does not support native byte streaming"),
            "{err}"
        );
        assert!(batches.next().await.is_none(), "exactly one item");

        // Same on the sink side: no capabilities, and `load_native` is a typed
        // error so a sink that advertises but forgets to implement is obvious.
        let sink = PlainSink;
        assert!(sink.native_load_capabilities().is_empty());
        let err = sink
            .load_native(
                crate::native::NativeBatch::bytes(
                    crate::native::NativeFormat::NdJson,
                    b"{}\n".to_vec(),
                ),
                "scope",
                crate::native::NativeLoadContext {
                    write_mode: crate::write_mode::WriteMode::Append,
                    first_batch: true,
                },
            )
            .await
            .expect_err("default must error");
        assert!(
            err.to_string()
                .contains("does not support native byte loading"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn default_overwrite_methods_reject_or_noop() {
        // A sink that does not opt into overwrite (MockSink uses the defaults):
        // `is_overwrite` is false, begin/commit return the typed "unsupported"
        // error, and abort is a no-op success.
        let sink = MockSink::new();
        assert!(!sink.is_overwrite());
        assert!(sink.begin_overwrite().await.is_err());
        assert!(sink.commit_overwrite().await.is_err());
        assert!(sink.abort_overwrite().await.is_ok());
    }

    // ── Source tests ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn source_fetch_all_returns_records() {
        let source = MockSource {
            records: vec![json!({"id": 1}), json!({"id": 2})],
        };
        let records = source.fetch_all().await.unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["id"], 1);
    }

    #[tokio::test]
    async fn source_fetch_all_empty() {
        let source = MockSource { records: vec![] };
        let records = source.fetch_all().await.unwrap();
        assert!(records.is_empty());
    }

    #[tokio::test]
    async fn source_default_incremental_returns_none_bookmark() {
        let source = MockSource {
            records: vec![json!({"id": 1})],
        };
        let (records, bookmark) = source.fetch_all_incremental().await.unwrap();
        assert_eq!(records.len(), 1);
        assert!(bookmark.is_none());
    }

    #[tokio::test]
    async fn source_custom_incremental_returns_bookmark() {
        let source = IncrementalSource {
            records: vec![json!({"id": 1})],
            bookmark: json!("2024-12-01"),
        };
        let (records, bookmark) = source.fetch_all_incremental().await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(bookmark, Some(json!("2024-12-01")));
    }

    #[tokio::test]
    async fn source_error_propagates() {
        let source = FailingSource;
        let result = source.fetch_all().await;
        assert!(result.is_err());
        assert!(matches!(result, Err(FaucetError::Auth(_))));
    }

    #[tokio::test]
    async fn source_as_trait_object() {
        let source: Box<dyn Source> = Box::new(MockSource {
            records: vec![json!({"id": 42})],
        });
        let records = source.fetch_all().await.unwrap();
        assert_eq!(records[0]["id"], 42);
    }

    // ── Sink tests ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn sink_write_batch_returns_count() {
        let sink = MockSink::new();
        let records = vec![json!({"id": 1}), json!({"id": 2}), json!({"id": 3})];
        let count = sink.write_batch(&records).await.unwrap();
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn sink_write_batch_empty() {
        let sink = MockSink::new();
        let count = sink.write_batch(&[]).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn sink_accumulates_records() {
        let sink = MockSink::new();
        sink.write_batch(&[json!({"a": 1})]).await.unwrap();
        sink.write_batch(&[json!({"b": 2})]).await.unwrap();
        let written = sink.written.lock().unwrap();
        assert_eq!(written.len(), 2);
    }

    #[tokio::test]
    async fn sink_default_flush_is_noop() {
        let sink = MockSink::new();
        assert!(sink.flush().await.is_ok());
    }

    #[tokio::test]
    async fn sink_error_propagates() {
        let sink = FailingSink;
        let result = sink.write_batch(&[json!({"id": 1})]).await;
        assert!(result.is_err());
        assert!(matches!(result, Err(FaucetError::Sink(_))));
    }

    #[tokio::test]
    async fn sink_as_trait_object() {
        let sink: Box<dyn Sink> = Box::new(MockSink::new());
        let count = sink.write_batch(&[json!({"id": 1})]).await.unwrap();
        assert_eq!(count, 1);
    }

    // ── stream_pages tests ──────────────────────────────────────────────────

    use crate::pipeline::DEFAULT_BATCH_SIZE;
    use futures::StreamExt;

    #[tokio::test]
    async fn default_stream_pages_chunks_records() {
        let source = MockSource {
            records: (0..5).map(|i| json!({"i": i})).collect(),
        };
        let ctx = std::collections::HashMap::new();
        let mut pages = source.stream_pages(&ctx, 2);
        let mut all = Vec::new();
        while let Some(page) = pages.next().await {
            all.push(page.unwrap());
        }
        // 5 records, batch_size=2 → pages of [2, 2, 1]
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].records.len(), 2);
        assert_eq!(all[1].records.len(), 2);
        assert_eq!(all[2].records.len(), 1);
    }

    #[tokio::test]
    async fn default_stream_pages_attaches_bookmark_to_final_page_only() {
        let source = IncrementalSource {
            records: (0..5).map(|i| json!({"i": i})).collect(),
            bookmark: json!("v1"),
        };
        let ctx = std::collections::HashMap::new();
        let mut pages = source.stream_pages(&ctx, 2);
        let mut collected = Vec::new();
        while let Some(page) = pages.next().await {
            collected.push(page.unwrap());
        }
        assert_eq!(collected.len(), 3);
        assert!(collected[0].bookmark.is_none());
        assert!(collected[1].bookmark.is_none());
        assert_eq!(collected[2].bookmark, Some(json!("v1")));
    }

    #[tokio::test]
    async fn default_stream_pages_single_page_when_batch_size_exceeds_total() {
        let source = MockSource {
            records: vec![json!({"id": 1}), json!({"id": 2})],
        };
        let ctx = std::collections::HashMap::new();
        let mut pages = source.stream_pages(&ctx, 100);
        let mut collected = Vec::new();
        while let Some(page) = pages.next().await {
            collected.push(page.unwrap());
        }
        assert_eq!(collected.len(), 1);
        assert_eq!(collected[0].records.len(), 2);
    }

    #[tokio::test]
    async fn default_stream_pages_batch_size_zero_emits_single_page() {
        // batch_size = 0 is the "no batching" sentinel — yields every record
        // in one page regardless of total count.
        let source = MockSource {
            records: (0..50_000).map(|i| json!({"i": i})).collect(),
        };
        let ctx = std::collections::HashMap::new();
        let mut pages = source.stream_pages(&ctx, 0);
        let mut collected = Vec::new();
        while let Some(page) = pages.next().await {
            collected.push(page.unwrap());
        }
        assert_eq!(
            collected.len(),
            1,
            "batch_size=0 must emit exactly one page"
        );
        assert_eq!(collected[0].records.len(), 50_000);
    }

    #[tokio::test]
    async fn default_stream_pages_batch_size_zero_attaches_bookmark_to_sole_page() {
        let source = IncrementalSource {
            records: (0..3).map(|i| json!({"i": i})).collect(),
            bookmark: json!("v1"),
        };
        let ctx = std::collections::HashMap::new();
        let mut pages = source.stream_pages(&ctx, 0);
        let page = pages.next().await.unwrap().unwrap();
        assert_eq!(page.records.len(), 3);
        assert_eq!(page.bookmark, Some(json!("v1")));
        assert!(pages.next().await.is_none());
    }

    #[tokio::test]
    async fn default_stream_pages_empty_source_yields_no_pages() {
        let source = MockSource { records: vec![] };
        let ctx = std::collections::HashMap::new();
        let mut pages = source.stream_pages(&ctx, DEFAULT_BATCH_SIZE);
        assert!(pages.next().await.is_none());
    }

    #[tokio::test]
    async fn default_stream_pages_empty_source_with_bookmark_yields_single_empty_page() {
        let source = IncrementalSource {
            records: vec![],
            bookmark: json!("v0"),
        };
        let ctx = std::collections::HashMap::new();
        let mut pages = source.stream_pages(&ctx, DEFAULT_BATCH_SIZE);
        let mut collected = Vec::new();
        while let Some(page) = pages.next().await {
            collected.push(page.unwrap());
        }
        // One empty-records page that carries the bookmark, so the pipeline
        // still persists progress on otherwise-empty incremental runs.
        assert_eq!(collected.len(), 1);
        assert!(collected[0].records.is_empty());
        assert_eq!(collected[0].bookmark, Some(json!("v0")));
    }

    #[tokio::test]
    async fn default_stream_pages_propagates_fetch_errors() {
        let source = FailingSource;
        let ctx = std::collections::HashMap::new();
        let mut pages = source.stream_pages(&ctx, DEFAULT_BATCH_SIZE);
        let first = pages.next().await.unwrap();
        assert!(matches!(first, Err(FaucetError::Auth(_))));
    }

    #[test]
    fn source_default_connector_name_is_stripped_type_name() {
        // MockSource lives at `faucet_core::traits::tests::MockSource`; the
        // stripped type_name yields the trailing segment.
        let source = MockSource { records: vec![] };
        assert_eq!(source.connector_name(), "MockSource");
    }

    #[test]
    fn sink_default_connector_name_is_stripped_type_name() {
        let sink = MockSink::new();
        assert_eq!(sink.connector_name(), "MockSink");
    }

    #[test]
    fn source_default_dataset_uri_uses_connector_name() {
        let source = MockSource { records: vec![] };
        assert_eq!(source.dataset_uri(), "MockSource://unknown");
    }

    #[test]
    fn sink_default_dataset_uri_uses_connector_name() {
        let sink = MockSink::new();
        assert_eq!(sink.dataset_uri(), "MockSink://unknown");
    }

    // ── write_batch_partial tests ───────────────────────────────────────────

    #[tokio::test]
    async fn default_write_batch_partial_success_returns_all_ok() {
        let sink = MockSink::new();
        let records = vec![json!({"id": 1}), json!({"id": 2}), json!({"id": 3})];
        let outcomes = sink.write_batch_partial(&records).await.unwrap();
        assert_eq!(outcomes.len(), 3);
        assert!(outcomes.iter().all(|o| o.is_ok()));
        assert_eq!(sink.written.lock().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn default_write_batch_partial_bubbles_outer_err() {
        let sink = FailingSink;
        let records = vec![json!({"id": 1}), json!({"id": 2})];
        let result = sink.write_batch_partial(&records).await;
        assert!(matches!(result, Err(FaucetError::Sink(_))));
    }

    #[tokio::test]
    async fn default_write_batch_partial_empty_returns_empty_vec() {
        let sink = MockSink::new();
        let outcomes = sink.write_batch_partial(&[]).await.unwrap();
        assert!(outcomes.is_empty());
    }

    #[tokio::test]
    async fn default_write_batch_partial_callable_through_trait_object() {
        let sink: Box<dyn Sink> = Box::new(MockSink::new());
        let records = vec![json!({"id": 1}), json!({"id": 2})];
        let outcomes = sink.write_batch_partial(&records).await.unwrap();
        assert_eq!(outcomes.len(), 2);
        assert!(outcomes.iter().all(|o| o.is_ok()));
    }

    // ── check() tests ─────────────────────────────────────────────────────────

    #[tokio::test]
    async fn source_default_check_pulls_first_page_and_passes() {
        let source = MockSource {
            records: vec![json!({"id": 1}), json!({"id": 2})],
        };
        let report = source
            .check(&crate::check::CheckContext::default())
            .await
            .unwrap();
        assert_eq!(report.failed_count(), 0);
        assert!(
            report
                .probes
                .iter()
                .any(|p| p.name == "read" && matches!(p.status, crate::check::ProbeStatus::Pass))
        );
    }

    #[tokio::test]
    async fn source_default_check_passes_on_empty_source() {
        let source = MockSource { records: vec![] };
        let report = source
            .check(&crate::check::CheckContext::default())
            .await
            .unwrap();
        // Reachable but empty is still a healthy source.
        assert_eq!(report.failed_count(), 0);
    }

    #[tokio::test]
    async fn source_default_check_fails_when_fetch_errors() {
        let source = FailingSource;
        let report = source
            .check(&crate::check::CheckContext::default())
            .await
            .unwrap();
        assert_eq!(report.failed_count(), 1);
        assert!(report.probes.iter().any(
            |p| p.name == "read" && matches!(p.status, crate::check::ProbeStatus::Fail { .. })
        ));
    }

    #[tokio::test]
    async fn sink_default_check_is_not_implemented_skip() {
        let sink = MockSink::new();
        let report = sink
            .check(&crate::check::CheckContext::default())
            .await
            .unwrap();
        assert_eq!(report.probes.len(), 1);
        assert!(matches!(
            report.probes[0].status,
            crate::check::ProbeStatus::Skip { .. }
        ));
    }

    #[tokio::test]
    async fn source_check_callable_through_trait_object() {
        let source: Box<dyn Source> = Box::new(MockSource {
            records: vec![json!({"id": 1})],
        });
        let report = source
            .check(&crate::check::CheckContext::default())
            .await
            .unwrap();
        assert_eq!(report.failed_count(), 0);
    }

    // ── idempotent-write / exactly-once capability tests ──────────────────────

    #[tokio::test]
    async fn sink_default_is_not_idempotent() {
        let sink = MockSink::new();
        assert!(!sink.supports_idempotent_writes());
        // Default write_batch_idempotent ignores the token and delegates.
        let n = sink
            .write_batch_idempotent(&[json!({"id": 1})], "scope::a", "00000000000000000001")
            .await
            .unwrap();
        assert_eq!(n, 1);
        assert_eq!(sink.last_committed_token("scope::a").await.unwrap(), None);
        assert_eq!(sink.written.lock().unwrap().len(), 1);
    }

    #[test]
    fn source_default_does_not_support_exactly_once() {
        let source = MockSource { records: vec![] };
        assert!(!source.supports_exactly_once());
    }

    #[test]
    fn sink_default_supported_write_modes_is_append_only() {
        use crate::write_mode::WriteMode;
        let sink = MockSink::new();
        assert_eq!(sink.supported_write_modes(), &[WriteMode::Append]);
    }

    #[test]
    fn supported_write_modes_callable_through_trait_object() {
        use crate::write_mode::WriteMode;
        let sink: Box<dyn Sink> = Box::new(MockSink::new());
        assert!(sink.supported_write_modes().contains(&WriteMode::Append));
    }

    #[tokio::test]
    async fn sink_default_current_schema_is_none() {
        let sink = MockSink::new();
        assert_eq!(sink.current_schema().await.unwrap(), None);
    }

    #[test]
    fn sink_default_does_not_support_schema_evolution() {
        let sink = MockSink::new();
        assert!(!sink.supports_schema_evolution());
    }

    #[tokio::test]
    async fn sink_default_evolve_schema_is_unsupported_error() {
        let sink = MockSink::new();
        let evo = crate::drift::SchemaEvolution::default();
        let err = sink.evolve_schema(&evo).await.unwrap_err();
        assert!(matches!(err, FaucetError::Sink(_)));
        assert!(err.to_string().contains("schema evolution"));
    }

    #[tokio::test]
    async fn source_default_capture_resume_position_is_none() {
        let source = MockSource { records: vec![] };
        assert_eq!(source.capture_resume_position().await.unwrap(), None);
    }

    #[tokio::test]
    async fn capture_resume_position_callable_through_trait_object() {
        let source: Box<dyn Source> = Box::new(MockSource { records: vec![] });
        assert!(source.capture_resume_position().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn source_default_does_not_support_discover() {
        let source: Box<dyn Source> = Box::new(MockSource { records: vec![] });
        assert!(!source.supports_discover());
        let err = source.discover().await.unwrap_err();
        assert!(matches!(err, FaucetError::Source(_)));
        assert!(
            err.to_string().contains("dataset discovery"),
            "typed unsupported error: {err}"
        );
    }

    #[tokio::test]
    async fn source_default_is_not_shardable() {
        let source: Box<dyn Source> = Box::new(MockSource { records: vec![] });
        assert!(!source.is_shardable());
    }

    #[tokio::test]
    async fn source_default_enumerates_single_whole_shard() {
        // A non-shardable source enumerates to exactly one whole-dataset shard,
        // regardless of the requested target — preserving single-worker behavior.
        let source: Box<dyn Source> = Box::new(MockSource { records: vec![] });
        let shards = source.enumerate_shards(8).await.unwrap();
        assert_eq!(shards.len(), 1);
        assert!(shards[0].is_whole());
    }

    #[tokio::test]
    async fn source_default_apply_shard_is_noop() {
        let source: Box<dyn Source> = Box::new(MockSource { records: vec![] });
        // Applying the whole shard is a no-op and must not error.
        source
            .apply_shard(&crate::shard::ShardSpec::whole())
            .await
            .unwrap();
    }
}
