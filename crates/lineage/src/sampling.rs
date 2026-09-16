//! Sink/source wrappers that sample records for schema inference and count
//! throughput for RUNNING heartbeats. Installed only when the corresponding
//! lineage facets/events are enabled, so they add zero overhead otherwise.

use async_trait::async_trait;
use faucet_core::{FaucetError, RowOutcome, Sink, Source, StreamPage};
use futures::{Stream, StreamExt};
use serde_json::Value;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::lifecycle::InferredSchema;

/// Shared sampling/counter state for one dataset side.
pub struct SampleState {
    cap: usize,
    count: AtomicU64,
    sample: Mutex<Vec<Value>>,
}

impl SampleState {
    pub fn new(cap: usize) -> Self {
        Self {
            cap,
            count: AtomicU64::new(0),
            sample: Mutex::new(Vec::new()),
        }
    }
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }
    fn observe(&self, records: &[Value]) {
        self.count
            .fetch_add(records.len() as u64, Ordering::Relaxed);
        if self.cap == 0 {
            return;
        }
        let mut s = self.sample.lock().unwrap();
        for r in records {
            if s.len() >= self.cap {
                break;
            }
            s.push(r.clone());
        }
    }
    /// Bump only the throughput counter (no sampling) — used by the native
    /// byte-passthrough tap (#639), which counts records by newline across the
    /// whole stream but samples only a bounded prefix.
    fn add_count(&self, n: u64) {
        self.count.fetch_add(n, Ordering::Relaxed);
    }
    /// Push one already-parsed record into the sample if there is room (no count
    /// bump). Companion to [`add_count`](Self::add_count) for the native tap.
    fn sample_record(&self, v: Value) {
        if self.cap == 0 {
            return;
        }
        let mut s = self.sample.lock().unwrap();
        if s.len() < self.cap {
            s.push(v);
        }
    }
    /// Whether the sample is full — lets the native tap stop parsing early.
    fn sample_full(&self) -> bool {
        self.cap == 0 || self.sample.lock().unwrap().len() >= self.cap
    }
    /// How many more records the sample can still take (0 once full).
    #[cfg_attr(not(feature = "arrow"), allow(dead_code))]
    fn sample_remaining(&self) -> usize {
        self.cap.saturating_sub(self.sample.lock().unwrap().len())
    }
    /// A copy of the sampled records (bounded by the construction-time cap).
    /// Used by consumers that run their own schema inference over the sample —
    /// e.g. the CLI's Data Movement Catalog (#279), which feeds them to
    /// `faucet_core::schema::infer_schema` for a drift-comparable shape.
    pub fn samples(&self) -> Vec<Value> {
        self.sample.lock().unwrap().clone()
    }

    /// Infer an ordered (name, OL-type) schema from the sampled records.
    pub fn inferred_schema(&self) -> InferredSchema {
        let sample = self.sample.lock().unwrap();
        if sample.is_empty() {
            return InferredSchema::default();
        }
        // Preserve first-seen field order across the sample.
        let mut order: Vec<String> = Vec::new();
        let mut types: HashMap<String, String> = HashMap::new();
        for rec in sample.iter() {
            if let Value::Object(map) = rec {
                for (k, v) in map {
                    if !types.contains_key(k) {
                        order.push(k.clone());
                    }
                    types
                        .entry(k.clone())
                        .or_insert_with(|| ol_type_of(v).to_string());
                }
            }
        }
        InferredSchema {
            fields: order
                .into_iter()
                .map(|k| {
                    let t = types.remove(&k).unwrap_or_else(|| "string".into());
                    (k, t)
                })
                .collect(),
        }
    }
}

fn ol_type_of(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(n) if n.is_i64() || n.is_u64() => "integer",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Tap a native NDJSON byte payload (#639): pass every chunk through **unchanged**
/// (streaming preserved) while counting records by newline and sampling a bounded
/// prefix into `state`. This lets lineage/catalog sampling coexist with the native
/// byte-passthrough fast path (#633) instead of forcing the `Value` path (which
/// ballooned memory ~35×). Non-NDJSON payloads pass through untapped (no schema
/// sample), but the batch's declared row count — when the source knows it — still
/// feeds the volume counters so catalog/lineage never report zero for a
/// successful CSV/Parquet native run.
fn tap_native_payload(
    payload: faucet_core::NativePayload,
    format: faucet_core::NativeFormat,
    records: Option<u64>,
    state: std::sync::Arc<SampleState>,
) -> faucet_core::NativePayload {
    use faucet_core::{NativeFormat, NativePayload};
    if format != NativeFormat::NdJson {
        if let Some(n) = records {
            state.add_count(n);
        }
        return payload;
    }
    match payload {
        NativePayload::Bytes(b) => {
            let mut n = 0u64;
            for line in b.split(|&x| x == b'\n') {
                if line.is_empty() {
                    continue;
                }
                n += 1;
                if !state.sample_full()
                    && let Ok(v) = serde_json::from_slice::<Value>(line)
                {
                    state.sample_record(v);
                }
            }
            state.add_count(n);
            NativePayload::Bytes(b)
        }
        NativePayload::Stream(inner) => {
            let tapped = faucet_core::async_stream::try_stream! {
                let mut inner = inner;
                let mut buf: Vec<u8> = Vec::new();
                let mut sampling = !state.sample_full();
                // Track whether the payload's final byte is a newline: a last
                // line without a trailing `\n` is a real record the per-chunk
                // newline count misses (the `Bytes` arm counts it via `split`),
                // so it is counted — and sampled — at stream end.
                let mut last_byte: Option<u8> = None;
                while let Some(chunk) = inner.next().await {
                    let chunk = chunk?;
                    state.add_count(chunk.iter().filter(|&&b| b == b'\n').count() as u64);
                    if let Some(&b) = chunk.last() {
                        last_byte = Some(b);
                    }
                    if sampling {
                        buf.extend_from_slice(&chunk);
                        while let Some(pos) = buf.iter().position(|&b| b == b'\n') {
                            let line: Vec<u8> = buf.drain(..=pos).collect();
                            let trimmed = &line[..line.len() - 1];
                            if !trimmed.is_empty()
                                && let Ok(v) = serde_json::from_slice::<Value>(trimmed)
                            {
                                state.sample_record(v);
                            }
                            if state.sample_full() {
                                sampling = false;
                                buf.clear();
                                break;
                            }
                        }
                    }
                    yield chunk;
                }
                if last_byte.is_some_and(|b| b != b'\n') {
                    state.add_count(1);
                    if sampling
                        && !buf.is_empty()
                        && let Ok(v) = serde_json::from_slice::<Value>(&buf)
                    {
                        state.sample_record(v);
                    }
                }
            };
            NativePayload::Stream(Box::pin(tapped))
        }
    }
}

/// Wraps a sink, sampling written records and counting throughput.
pub struct SamplingSink {
    inner: Box<dyn Sink>,
    state: std::sync::Arc<SampleState>,
}

impl SamplingSink {
    pub fn new(inner: Box<dyn Sink>, state: std::sync::Arc<SampleState>) -> Self {
        Self { inner, state }
    }
}

#[async_trait]
impl Sink for SamplingSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        let n = self.inner.write_batch(records).await?;
        self.state.observe(records);
        Ok(n)
    }
    async fn write_batch_partial(&self, records: &[Value]) -> Result<Vec<RowOutcome>, FaucetError> {
        let outcomes = self.inner.write_batch_partial(records).await?;
        self.state.observe(records);
        Ok(outcomes)
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
    async fn local_outputs(&self) -> Vec<faucet_core::LocalOutput> {
        self.inner.local_outputs().await
    }
    // Capability + exactly-once passthroughs. Without these the wrapper's
    // trait defaults would mask the inner sink's capabilities whenever lineage
    // (or catalog) sampling is active — e.g. an exactly-once run would be
    // rejected as "sink is not idempotent" purely because sampling was on.
    fn supports_idempotent_writes(&self) -> bool {
        self.inner.supports_idempotent_writes()
    }
    fn sink_guarantee(&self) -> faucet_core::SinkGuarantee {
        self.inner.sink_guarantee()
    }
    fn dedups_by_key(&self) -> bool {
        self.inner.dedups_by_key()
    }
    fn supported_write_modes(&self) -> &'static [faucet_core::WriteMode] {
        self.inner.supported_write_modes()
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
        self.state.observe(records);
        Ok(n)
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
        evolution: &faucet_core::SchemaEvolution,
    ) -> Result<(), FaucetError> {
        self.inner.evolve_schema(evolution).await
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
    // Native byte-passthrough passthrough (#639). Without forwarding these, the
    // wrapper's trait defaults would report "no native load capability", forcing
    // the pipeline onto the `Value` path whenever sampling is active — the exact
    // regression this fix closes (memory ~35× higher). We tap the payload for the
    // sink-side schema sample while the inner sink drains it, so the fast path and
    // the catalog schema both survive.
    fn native_load_capabilities(&self) -> Vec<faucet_core::NativeLoadCapability> {
        self.inner.native_load_capabilities()
    }
    async fn load_native(
        &self,
        batch: faucet_core::NativeBatch,
        scope: &str,
        ctx: faucet_core::NativeLoadContext,
    ) -> Result<usize, FaucetError> {
        let faucet_core::NativeBatch {
            format,
            payload,
            csv,
            records,
            bookmark,
        } = batch;
        let tapped = faucet_core::NativeBatch {
            format,
            payload: tap_native_payload(
                payload,
                format,
                records,
                std::sync::Arc::clone(&self.state),
            ),
            csv,
            records,
            bookmark,
        };
        self.inner.load_native(tapped, scope, ctx).await
    }
    // Columnar (Arrow) fast-path passthrough (#639, #375). Same rationale as the
    // native methods: without forwarding, the wrapper masks the inner sink's
    // columnar capability and forces the `Value` path. We sample a bounded row
    // prefix of the batch for the schema, then forward the batch unchanged.
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
        sample_record_batch(batch, &self.state);
        Ok(n)
    }
}

/// Sample a bounded row prefix of an Arrow `RecordBatch` into `state` (#639).
/// Converts only as many leading rows as the sample still needs (never the whole
/// batch), and counts every row for volume regardless.
#[cfg(feature = "arrow")]
fn sample_record_batch(batch: &arrow::array::RecordBatch, state: &SampleState) {
    let rows = batch.num_rows();
    state.add_count(rows as u64);
    if state.sample_full() || rows == 0 {
        return;
    }
    let want = state.sample_remaining().min(rows);
    let slice = batch.slice(0, want);
    if let Ok(values) = faucet_core::columnar::record_batch_to_values(&slice) {
        for v in values {
            state.sample_record(v);
        }
    }
}

/// Wraps a source, sampling the records it yields (pre-transform input schema).
pub struct SamplingSource {
    inner: Box<dyn Source>,
    state: std::sync::Arc<SampleState>,
}

impl SamplingSource {
    pub fn new(inner: Box<dyn Source>, state: std::sync::Arc<SampleState>) -> Self {
        Self { inner, state }
    }
}

#[async_trait]
impl Source for SamplingSource {
    async fn fetch_with_context(
        &self,
        ctx: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        // Required method; sampling happens in `stream_pages` (the path the
        // pipeline actually drives). Delegate so the trait is complete.
        self.inner.fetch_with_context(ctx).await
    }
    /// Override `stream_pages` (NOT `fetch_*`) so native-streaming sources keep
    /// their bounded-memory page stream — we tap the pages as they flow rather
    /// than forcing the buffering default path.
    fn stream_pages<'a>(
        &'a self,
        ctx: &'a HashMap<String, Value>,
        batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let state = std::sync::Arc::clone(&self.state);
        let inner = self.inner.stream_pages(ctx, batch_size);
        Box::pin(faucet_core::async_stream::try_stream! {
            let mut inner = inner;
            while let Some(page) = inner.next().await {
                let page = page?;
                state.observe(&page.records);
                yield page;
            }
        })
    }
    fn connector_name(&self) -> &'static str {
        self.inner.connector_name()
    }
    fn dataset_uri(&self) -> String {
        self.inner.dataset_uri()
    }
    fn state_key(&self) -> Option<String> {
        self.inner.state_key()
    }
    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        self.inner.apply_start_bookmark(bookmark).await
    }
    // Native byte-passthrough passthrough (#639) — mirror of the sink side. We
    // forward the format advertisement and wrap each native batch's payload in a
    // tap that samples a bounded prefix + counts records by newline while passing
    // every byte through unchanged, so the fast path stays memory-flat and the
    // input-side schema sample is still captured.
    fn native_output_formats(&self) -> &'static [faucet_core::NativeFormat] {
        self.inner.native_output_formats()
    }
    fn stream_native<'a>(
        &'a self,
        context: &'a HashMap<String, Value>,
        format: faucet_core::NativeFormat,
        batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<faucet_core::NativeBatch, FaucetError>> + Send + 'a>>
    {
        let state = std::sync::Arc::clone(&self.state);
        let inner = self.inner.stream_native(context, format, batch_size);
        Box::pin(faucet_core::async_stream::try_stream! {
            let mut inner = inner;
            while let Some(batch) = inner.next().await {
                let faucet_core::NativeBatch { format, payload, csv, records, bookmark } = batch?;
                yield faucet_core::NativeBatch {
                    format,
                    payload: tap_native_payload(payload, format, records, std::sync::Arc::clone(&state)),
                    csv,
                    records,
                    bookmark,
                };
            }
        })
    }
    // Columnar (Arrow) fast-path passthrough (#639, #375) — mirror of the sink.
    #[cfg(feature = "arrow")]
    fn supports_columnar(&self) -> bool {
        self.inner.supports_columnar()
    }
    #[cfg(feature = "arrow")]
    fn stream_batches<'a>(
        &'a self,
        context: &'a HashMap<String, Value>,
        batch_size: usize,
    ) -> Pin<
        Box<
            dyn Stream<Item = Result<faucet_core::columnar::ColumnarPage, FaucetError>> + Send + 'a,
        >,
    > {
        let state = std::sync::Arc::clone(&self.state);
        let inner = self.inner.stream_batches(context, batch_size);
        Box::pin(faucet_core::async_stream::try_stream! {
            let mut inner = inner;
            while let Some(page) = inner.next().await {
                let page = page?;
                sample_record_batch(&page.batch, &state);
                yield page;
            }
        })
    }
    // Bookmark + capability passthroughs. `fetch_with_context_incremental`
    // matters even though the pipeline drives `stream_pages`: an *outer*
    // wrapper's default `stream_pages` builds on it, and the trait default
    // would silently drop the inner source's bookmark (breaking incremental
    // resume whenever sampling is active).
    async fn fetch_with_context_incremental(
        &self,
        ctx: &HashMap<String, Value>,
    ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        self.inner.fetch_with_context_incremental(ctx).await
    }
    fn supports_exactly_once(&self) -> bool {
        self.inner.supports_exactly_once()
    }
    fn replay_guarantee(&self) -> faucet_core::ReplayGuarantee {
        self.inner.replay_guarantee()
    }
    async fn capture_resume_position(&self) -> Result<Option<Value>, FaucetError> {
        self.inner.capture_resume_position().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use faucet_core::{FaucetError, Sink};
    use serde_json::{Value, json};
    use std::sync::Arc;

    struct CollectSink(std::sync::Mutex<Vec<Value>>);
    #[async_trait]
    impl Sink for CollectSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            self.0.lock().unwrap().extend(records.iter().cloned());
            Ok(records.len())
        }
        fn connector_name(&self) -> &'static str {
            "collect"
        }
    }

    #[tokio::test]
    async fn sink_counts_and_samples_first_n() {
        let shared = Arc::new(SampleState::new(2));
        let inner: Box<dyn Sink> = Box::new(CollectSink(Default::default()));
        let s = SamplingSink::new(inner, Arc::clone(&shared));
        s.write_batch(&[json!({"id":1,"name":"a"})]).await.unwrap();
        s.write_batch(&[json!({"id":2}), json!({"id":3})])
            .await
            .unwrap();
        assert_eq!(shared.count(), 3);
        // only the first 2 records were retained for schema inference
        let schema = shared.inferred_schema();
        let names: Vec<&str> = schema.fields.iter().map(|(n, _)| n.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"name"));
    }

    #[tokio::test]
    async fn sampling_sink_forwards_overwrite_lifecycle() {
        // The sampling wrapper must forward the overwrite lifecycle so an
        // overwrite run still stages/swaps when lineage/catalog sampling is on.
        struct OvwSink(Arc<std::sync::Mutex<Vec<&'static str>>>);
        #[async_trait]
        impl Sink for OvwSink {
            async fn write_batch(&self, r: &[Value]) -> Result<usize, FaucetError> {
                Ok(r.len())
            }
            fn is_overwrite(&self) -> bool {
                true
            }
            async fn begin_overwrite(&self) -> Result<(), FaucetError> {
                self.0.lock().unwrap().push("begin");
                Ok(())
            }
            async fn commit_overwrite(&self) -> Result<(), FaucetError> {
                self.0.lock().unwrap().push("commit");
                Ok(())
            }
            async fn abort_overwrite(&self) -> Result<(), FaucetError> {
                self.0.lock().unwrap().push("abort");
                Ok(())
            }
        }
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let inner: Box<dyn Sink> = Box::new(OvwSink(Arc::clone(&log)));
        let s = SamplingSink::new(inner, Arc::new(SampleState::new(2)));
        assert!(s.is_overwrite());
        // A write through the wrapper must forward to the inner sink too.
        assert_eq!(s.write_batch(&[json!({"id": 1})]).await.unwrap(), 1);
        s.begin_overwrite().await.unwrap();
        s.commit_overwrite().await.unwrap();
        s.abort_overwrite().await.unwrap();
        assert_eq!(*log.lock().unwrap(), vec!["begin", "commit", "abort"]);
    }

    struct TwoRowSource;
    #[async_trait]
    impl faucet_core::Source for TwoRowSource {
        async fn fetch_with_context(
            &self,
            _: &std::collections::HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(vec![json!({"id": 1}), json!({"id": 2})])
        }
        fn connector_name(&self) -> &'static str {
            "tworow"
        }
    }

    #[tokio::test]
    async fn source_samples_streamed_pages_without_buffering_override() {
        use faucet_core::Source as _;
        use futures::StreamExt as _;
        let shared = Arc::new(SampleState::new(10));
        let s = SamplingSource::new(Box::new(TwoRowSource), Arc::clone(&shared));
        let ctx = std::collections::HashMap::new();
        let mut pages = s.stream_pages(&ctx, 1000);
        while let Some(p) = pages.next().await {
            let _ = p.unwrap();
        }
        assert_eq!(shared.count(), 2);
        let schema = shared.inferred_schema();
        let names: Vec<&str> = schema.fields.iter().map(|(n, _)| n.as_str()).collect();
        assert!(names.contains(&"id"));
    }

    /// An idempotent, upsert-capable sink: the sampler must forward every
    /// capability + exactly-once method instead of masking them with the trait
    /// defaults (which would make an exactly-once run fail as "sink is not
    /// idempotent" whenever lineage/catalog sampling is active).
    struct IdemSink;
    #[async_trait]
    impl Sink for IdemSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            Ok(records.len())
        }
        fn connector_name(&self) -> &'static str {
            "idem"
        }
        fn supports_idempotent_writes(&self) -> bool {
            true
        }
        fn dedups_by_key(&self) -> bool {
            true
        }
        fn supported_write_modes(&self) -> &'static [faucet_core::WriteMode] {
            &[
                faucet_core::WriteMode::Append,
                faucet_core::WriteMode::Upsert,
            ]
        }
        async fn write_batch_idempotent(
            &self,
            records: &[Value],
            _scope: &str,
            _token: &str,
        ) -> Result<usize, FaucetError> {
            Ok(records.len())
        }
        async fn last_committed_token(&self, _scope: &str) -> Result<Option<String>, FaucetError> {
            Ok(Some("tok".into()))
        }
        fn supports_schema_evolution(&self) -> bool {
            true
        }
        async fn current_schema(&self) -> Result<Option<Value>, FaucetError> {
            Ok(Some(json!({"type": "object", "properties": {}})))
        }
    }

    #[tokio::test]
    async fn sink_forwards_capabilities_and_samples_idempotent_writes() {
        let shared = Arc::new(SampleState::new(10));
        let s = SamplingSink::new(Box::new(IdemSink), Arc::clone(&shared));
        assert!(s.supports_idempotent_writes());
        assert!(s.dedups_by_key());
        assert_eq!(
            s.sink_guarantee(),
            faucet_core::SinkGuarantee::AtomicWatermark
        );
        assert!(
            s.supported_write_modes()
                .contains(&faucet_core::WriteMode::Upsert)
        );
        assert!(s.supports_schema_evolution());
        assert!(s.current_schema().await.unwrap().is_some());
        assert_eq!(
            s.last_committed_token("k").await.unwrap(),
            Some("tok".into())
        );
        // Idempotent writes are observed by the sampler like plain writes.
        s.write_batch_idempotent(&[json!({"id": 1})], "k", "t")
            .await
            .unwrap();
        assert_eq!(shared.count(), 1);
    }

    struct BookmarkedSource;
    #[async_trait]
    impl faucet_core::Source for BookmarkedSource {
        async fn fetch_with_context(
            &self,
            _: &std::collections::HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(vec![json!({"id": 1})])
        }
        async fn fetch_with_context_incremental(
            &self,
            _: &std::collections::HashMap<String, Value>,
        ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
            Ok((vec![json!({"id": 1})], Some(json!("bm"))))
        }
        fn supports_exactly_once(&self) -> bool {
            true
        }
        async fn capture_resume_position(&self) -> Result<Option<Value>, FaucetError> {
            Ok(Some(json!("pos")))
        }
        fn connector_name(&self) -> &'static str {
            "bookmarked"
        }
    }

    #[tokio::test]
    async fn source_forwards_bookmarks_and_capabilities() {
        use faucet_core::Source as _;
        let shared = Arc::new(SampleState::new(10));
        let s = SamplingSource::new(Box::new(BookmarkedSource), Arc::clone(&shared));
        // The incremental fetch must surface the inner bookmark — the trait
        // default would silently drop it (breaking incremental resume when an
        // outer wrapper's default stream_pages builds on this method).
        let (_, bm) = s
            .fetch_with_context_incremental(&std::collections::HashMap::new())
            .await
            .unwrap();
        assert_eq!(bm, Some(json!("bm")));
        assert!(s.supports_exactly_once());
        assert_eq!(
            s.replay_guarantee(),
            faucet_core::ReplayGuarantee::Deterministic
        );
        assert_eq!(
            s.capture_resume_position().await.unwrap(),
            Some(json!("pos"))
        );
    }

    // ---- #639: native byte-passthrough must survive lineage/catalog sampling ----

    /// A native-load-capable sink that drains the payload into a buffer and
    /// returns the newline (record) count — modelling a real byte-loading sink.
    struct NativeSink(Arc<std::sync::Mutex<Vec<u8>>>);
    #[async_trait]
    impl Sink for NativeSink {
        async fn write_batch(&self, r: &[Value]) -> Result<usize, FaucetError> {
            Ok(r.len())
        }
        fn connector_name(&self) -> &'static str {
            "nativesink"
        }
        fn native_load_capabilities(&self) -> Vec<faucet_core::NativeLoadCapability> {
            vec![faucet_core::NativeLoadCapability {
                format: faucet_core::NativeFormat::NdJson,
                mechanism: "test-native",
                write_modes: &[faucet_core::WriteMode::Append],
            }]
        }
        async fn load_native(
            &self,
            batch: faucet_core::NativeBatch,
            _scope: &str,
            _ctx: faucet_core::NativeLoadContext,
        ) -> Result<usize, FaucetError> {
            use futures::StreamExt as _;
            match batch.payload {
                faucet_core::NativePayload::Bytes(b) => {
                    self.0.lock().unwrap().extend_from_slice(&b)
                }
                faucet_core::NativePayload::Stream(mut st) => {
                    while let Some(c) = st.next().await {
                        self.0.lock().unwrap().extend_from_slice(&c?);
                    }
                }
            }
            Ok(self
                .0
                .lock()
                .unwrap()
                .iter()
                .filter(|&&x| x == b'\n')
                .count())
        }
    }

    #[tokio::test]
    async fn sink_load_native_forwards_capability_and_taps_sample() {
        let buf = Arc::new(std::sync::Mutex::new(Vec::new()));
        let inner: Box<dyn Sink> = Box::new(NativeSink(Arc::clone(&buf)));
        let shared = Arc::new(SampleState::new(2));
        let s = SamplingSink::new(inner, Arc::clone(&shared));

        // Capability must forward (not be masked by the wrapper's default).
        let caps = s.native_load_capabilities();
        assert_eq!(caps.len(), 1);
        assert_eq!(caps[0].mechanism, "test-native");

        let bytes = b"{\"id\":1,\"name\":\"a\"}\n{\"id\":2}\n{\"id\":3}\n".to_vec();
        let batch =
            faucet_core::NativeBatch::bytes(faucet_core::NativeFormat::NdJson, bytes.clone());
        let ctx = faucet_core::NativeLoadContext {
            write_mode: faucet_core::WriteMode::Append,
            first_batch: true,
        };
        let n = s.load_native(batch, "scope", ctx).await.unwrap();

        // Inner sink saw every byte unchanged (passthrough), all 3 records.
        assert_eq!(*buf.lock().unwrap(), bytes);
        assert_eq!(n, 3);
        // Tap counted all 3 records but sampled only the first 2 for the schema.
        assert_eq!(shared.count(), 3);
        let names: Vec<String> = shared
            .inferred_schema()
            .fields
            .iter()
            .map(|(n, _)| n.clone())
            .collect();
        assert!(names.contains(&"id".to_string()));
        assert!(names.contains(&"name".to_string()));
    }

    /// A native-streaming source that emits one NDJSON batch split across two
    /// byte chunks (so the tap must reassemble a line spanning a chunk boundary).
    struct NativeSource;
    #[async_trait]
    impl faucet_core::Source for NativeSource {
        async fn fetch_with_context(
            &self,
            _: &std::collections::HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(vec![])
        }
        fn connector_name(&self) -> &'static str {
            "nativesource"
        }
        fn native_output_formats(&self) -> &'static [faucet_core::NativeFormat] {
            &[faucet_core::NativeFormat::NdJson]
        }
        fn stream_native<'a>(
            &'a self,
            _ctx: &'a std::collections::HashMap<String, Value>,
            _format: faucet_core::NativeFormat,
            _batch_size: usize,
        ) -> Pin<Box<dyn Stream<Item = Result<faucet_core::NativeBatch, FaucetError>> + Send + 'a>>
        {
            Box::pin(faucet_core::async_stream::try_stream! {
                let chunks: Vec<Vec<u8>> = vec![
                    b"{\"id\":1}\n{\"i".to_vec(),
                    b"d\":2}\n".to_vec(),
                ];
                let payload = faucet_core::NativePayload::Stream(Box::pin(
                    faucet_core::async_stream::try_stream! {
                        for c in chunks { yield c; }
                    },
                ));
                yield faucet_core::NativeBatch {
                    format: faucet_core::NativeFormat::NdJson,
                    payload,
                    csv: Default::default(),
                    records: Some(2),
                    bookmark: Some(json!("bm")),
                };
            })
        }
    }

    #[tokio::test]
    async fn source_stream_native_forwards_format_and_taps_stream() {
        use faucet_core::Source as _;
        use futures::StreamExt as _;
        let shared = Arc::new(SampleState::new(10));
        let s = SamplingSource::new(Box::new(NativeSource), Arc::clone(&shared));
        assert_eq!(
            s.native_output_formats(),
            &[faucet_core::NativeFormat::NdJson]
        );
        let ctx = std::collections::HashMap::new();
        let mut collected: Vec<u8> = Vec::new();
        let mut batches = s.stream_native(&ctx, faucet_core::NativeFormat::NdJson, 1000);
        while let Some(b) = batches.next().await {
            let b = b.unwrap();
            match b.payload {
                faucet_core::NativePayload::Bytes(bytes) => collected.extend_from_slice(&bytes),
                faucet_core::NativePayload::Stream(mut st) => {
                    while let Some(c) = st.next().await {
                        collected.extend_from_slice(&c.unwrap());
                    }
                }
            }
        }
        // Every byte flowed through unchanged (line split across the chunk boundary).
        assert_eq!(collected, b"{\"id\":1}\n{\"id\":2}\n");
        // Tap counted both records and sampled the schema.
        assert_eq!(shared.count(), 2);
        let names: Vec<String> = shared
            .inferred_schema()
            .fields
            .iter()
            .map(|(n, _)| n.clone())
            .collect();
        assert!(names.contains(&"id".to_string()));
    }

    #[tokio::test]
    async fn stream_tap_counts_and_samples_a_final_line_without_trailing_newline() {
        // `…{"id":2}` (no trailing `\n`) is a real record: the `Bytes` arm
        // counts it via `split`, so the `Stream` arm must agree — same bytes,
        // same count, regardless of payload variant.
        use futures::StreamExt as _;
        let shared = Arc::new(SampleState::new(10));
        let chunks: Vec<Vec<u8>> = vec![b"{\"id\":1}\n{\"i".to_vec(), b"d\":2}".to_vec()];
        let payload =
            faucet_core::NativePayload::Stream(Box::pin(faucet_core::async_stream::try_stream! {
                for c in chunks { yield c; }
            }));
        let tapped = tap_native_payload(
            payload,
            faucet_core::NativeFormat::NdJson,
            None,
            Arc::clone(&shared),
        );
        let mut collected: Vec<u8> = Vec::new();
        match tapped {
            faucet_core::NativePayload::Stream(mut st) => {
                while let Some(c) = st.next().await {
                    collected.extend_from_slice(&c.unwrap());
                }
            }
            faucet_core::NativePayload::Bytes(_) => panic!("stream stays a stream"),
        }
        assert_eq!(collected, b"{\"id\":1}\n{\"id\":2}");
        assert_eq!(shared.count(), 2, "trailing line must be counted");
        assert_eq!(shared.samples().len(), 2, "trailing line must be sampled");
    }

    #[tokio::test]
    async fn non_ndjson_tap_passes_through_but_still_counts_declared_records() {
        // CSV/Parquet payloads carry no per-line structure the tap can parse,
        // but a declared row count must still feed the volume counters so a
        // successful native CSV run never shows zero rows in catalog/lineage.
        let shared = Arc::new(SampleState::new(10));
        let payload = faucet_core::NativePayload::Bytes(b"h\na,b\nc,d\n".to_vec());
        let tapped = tap_native_payload(
            payload,
            faucet_core::NativeFormat::Csv,
            Some(2),
            Arc::clone(&shared),
        );
        match tapped {
            faucet_core::NativePayload::Bytes(b) => assert_eq!(b, b"h\na,b\nc,d\n".to_vec()),
            faucet_core::NativePayload::Stream(_) => panic!("bytes stay bytes"),
        }
        assert_eq!(shared.count(), 2, "declared count feeds the counter");
        assert!(shared.samples().is_empty(), "no schema sample for CSV");
    }

    #[cfg(feature = "arrow")]
    #[tokio::test]
    async fn sink_columnar_forwards_capability_and_samples() {
        use faucet_core::columnar::values_to_record_batch_inferred;
        struct ColSink(Arc<std::sync::Mutex<usize>>);
        #[async_trait]
        impl Sink for ColSink {
            async fn write_batch(&self, r: &[Value]) -> Result<usize, FaucetError> {
                Ok(r.len())
            }
            fn connector_name(&self) -> &'static str {
                "colsink"
            }
            fn supports_columnar(&self) -> bool {
                true
            }
            async fn write_batch_columnar(
                &self,
                batch: &arrow::array::RecordBatch,
            ) -> Result<usize, FaucetError> {
                *self.0.lock().unwrap() += batch.num_rows();
                Ok(batch.num_rows())
            }
        }
        let seen = Arc::new(std::sync::Mutex::new(0usize));
        let inner: Box<dyn Sink> = Box::new(ColSink(Arc::clone(&seen)));
        let shared = Arc::new(SampleState::new(2));
        let s = SamplingSink::new(inner, Arc::clone(&shared));
        // Capability must forward, not be masked by the wrapper default.
        assert!(s.supports_columnar());
        let batch = values_to_record_batch_inferred(&[
            json!({"id": 1, "name": "a"}),
            json!({"id": 2, "name": "b"}),
            json!({"id": 3, "name": "c"}),
        ])
        .unwrap();
        let n = s.write_batch_columnar(&batch).await.unwrap();
        // Inner sink saw all 3 rows; count is all 3; sample is bounded to 2.
        assert_eq!(n, 3);
        assert_eq!(*seen.lock().unwrap(), 3);
        assert_eq!(shared.count(), 3);
        let names: Vec<String> = shared
            .inferred_schema()
            .fields
            .iter()
            .map(|(n, _)| n.clone())
            .collect();
        assert!(names.contains(&"id".to_string()));
        assert!(names.contains(&"name".to_string()));
    }
}
