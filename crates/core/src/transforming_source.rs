//! Wrap any [`Source`] with a fixed list of [`TransformStage`]s applied to
//! every emitted record. The canonical way for library callers to attach
//! stages (transforms wrapped via [`TransformStage::Map`], plus `Filter` /
//! `Explode` / `Custom`); the CLI uses this same type internally.

use crate::error::FaucetError;
use crate::observability::{Labels, instrumented_apply_stages};
use crate::pipeline::StreamPage;
use crate::stage::{CompiledStage, TransformStage, compile_stage};
use crate::traits::Source;
use async_trait::async_trait;
use futures::StreamExt;
use futures_core::Stream;
use serde_json::Value;
use std::collections::HashMap;
use std::pin::Pin;

/// Source decorator that applies a fixed list of compiled stages to every
/// record. Emits `faucet_transform_*` metrics per page via
/// [`instrumented_apply_stages`].
///
/// # Example
///
/// ```no_run
/// use faucet_core::{RecordTransform, Source, TransformingSource};
/// use faucet_core::observability::Labels;
/// use faucet_core::stage::TransformStage;
/// use faucet_core::transform::KeyCaseMode;
///
/// # fn build_inner() -> Box<dyn Source> { unimplemented!() }
/// let inner: Box<dyn Source> = build_inner();
/// let wrapped = TransformingSource::new(
///     inner,
///     vec![TransformStage::Map(RecordTransform::KeysCase { mode: KeyCaseMode::Snake, on_collision: Default::default() })],
///     Labels::for_named("rest"),
/// ).unwrap();
/// ```
pub struct TransformingSource {
    inner: Box<dyn Source>,
    stages: Vec<CompiledStage>,
    labels: Labels,
    /// Optional Arrow `RecordBatch → RecordBatch` form for each stage, parallel
    /// to `stages` (#375). `Some` only for columnar-capable stages (today the
    /// SQL transform, supplied via [`new_with_batches`](Self::new_with_batches));
    /// `None` for `Value`-only stages. When every entry is `Some` and the inner
    /// source is columnar, the whole chain runs on the columnar fast path.
    #[cfg(feature = "arrow")]
    batch_fns: Vec<Option<crate::stage::PageFnBatchBox>>,
}

impl TransformingSource {
    /// Compile `stages` and wrap `inner`. Returns
    /// [`FaucetError::Transform`] if any stage's compilation fails (e.g.
    /// invalid regex in `RenameKeys`). The chain stays on the `Value` path
    /// (no columnar batch forms).
    pub fn new(
        inner: Box<dyn Source>,
        stages: Vec<TransformStage>,
        labels: Labels,
    ) -> Result<Self, FaucetError> {
        let compiled = stages
            .iter()
            .map(compile_stage)
            .collect::<Result<Vec<_>, _>>()?;
        // Auto-derive the Arrow batch form for each stage (#636): a `Map` over a
        // vectorizable record transform (select/drop/rename_field/set/redact)
        // gets a columnar kernel, so a chain of only those over a columnar
        // source+sink keeps the fast path. Any stage without one (a
        // value-inspecting transform, filter/explode/cdc-unwrap, a custom or
        // page fn) leaves a `None`, which makes `supports_columnar` false and
        // holds the whole chain on the `Value` path.
        #[cfg(feature = "arrow")]
        let batch_fns: Vec<Option<crate::stage::PageFnBatchBox>> = stages
            .iter()
            .map(|s| match s {
                TransformStage::Map(t) => crate::columnar_transform::batch_form(t),
                _ => None,
            })
            .collect();
        Ok(Self {
            inner,
            stages: compiled,
            labels,
            #[cfg(feature = "arrow")]
            batch_fns,
        })
    }

    /// Like [`new`](Self::new), but each stage may carry an Arrow `RecordBatch`
    /// form (`batch_fns[i]` parallels `stages[i]`), so the chain can run on the
    /// columnar fast path (#375) when the inner source and sink are Arrow-native
    /// and **every** stage supplies one. Used by the CLI for `sql` transforms.
    #[cfg(feature = "arrow")]
    pub fn new_with_batches(
        inner: Box<dyn Source>,
        stages: Vec<TransformStage>,
        batch_fns: Vec<Option<crate::stage::PageFnBatchBox>>,
        labels: Labels,
    ) -> Result<Self, FaucetError> {
        if batch_fns.len() != stages.len() {
            return Err(FaucetError::Transform(format!(
                "TransformingSource::new_with_batches: {} batch fns for {} stages",
                batch_fns.len(),
                stages.len()
            )));
        }
        let compiled = stages
            .iter()
            .map(compile_stage)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            inner,
            stages: compiled,
            labels,
            batch_fns,
        })
    }
}

#[async_trait]
impl Source for TransformingSource {
    async fn fetch_with_context(
        &self,
        ctx: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let records = self.inner.fetch_with_context(ctx).await?;
        instrumented_apply_stages(records, &self.stages, &self.labels)
    }

    async fn fetch_with_context_incremental(
        &self,
        ctx: &HashMap<String, Value>,
    ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        let (records, bookmark) = self.inner.fetch_with_context_incremental(ctx).await?;
        let transformed = instrumented_apply_stages(records, &self.stages, &self.labels)?;
        Ok((transformed, bookmark))
    }

    fn stream_pages<'a>(
        &'a self,
        ctx: &'a HashMap<String, Value>,
        batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        Box::pin(async_stream::try_stream! {
            let mut pages = self.inner.stream_pages(ctx, batch_size);
            while let Some(page) = pages.next().await {
                let page = page?;
                // The inner source already sized this page per its own config
                // `batch_size` (the authoritative knob — the pipeline-supplied
                // hint is only informational). Re-chunking the transformed
                // output *below* that inner page size would silently defeat an
                // explicit source `batch_size` (e.g. a 200k-row page shrunk to
                // the 1k default hint → 200 tiny sink writes / load jobs). So
                // never chunk smaller than the inner page; only bound *growth*
                // from a 1→N stage (explode) at that inner size.
                let page_len = page.records.len();
                let out = instrumented_apply_stages(
                    page.records, &self.stages, &self.labels,
                )?;
                if out.is_empty() {
                    yield StreamPage { records: vec![], bookmark: page.bookmark };
                    continue;
                }
                // A bookmark-carrying page is the unit a commit token covers, so
                // splitting it would write the earlier chunks without one and an
                // exactly-once resume would replay them. Sinks re-chunk internally.
                if batch_size == 0 || page.bookmark.is_some() {
                    yield StreamPage { records: out, bookmark: page.bookmark };
                    continue;
                }
                let effective = std::cmp::max(batch_size, page_len);
                let total = out.len();
                let mut start = 0usize;
                while start < total {
                    let end = std::cmp::min(start + effective, total);
                    let is_last = end == total;
                    let chunk: Vec<Value> = out[start..end].to_vec();
                    yield StreamPage {
                        records: chunk,
                        bookmark: if is_last { page.bookmark.clone() } else { None },
                    };
                    start = end;
                }
            }
        })
    }

    /// Columnar only when the inner source is columnar **and** every stage has
    /// an Arrow batch form (today: the SQL transform). Any `Value`-only stage
    /// (`Map` / `Filter` / `Explode` / `CdcUnwrap` / `Custom` / plain `PageFn`)
    /// keeps the whole chain on the `Value` path (#375).
    #[cfg(feature = "arrow")]
    fn supports_columnar(&self) -> bool {
        self.inner.supports_columnar()
            && !self.batch_fns.is_empty()
            && self.batch_fns.iter().all(Option::is_some)
    }

    /// Stream the inner source's Arrow batches with every stage's batch form
    /// applied in declared order — so `parquet → sql → parquet` runs Arrow
    /// end-to-end. Only reached when [`supports_columnar`](Self::supports_columnar)
    /// is `true`, i.e. every stage has a batch form.
    #[cfg(feature = "arrow")]
    fn stream_batches<'a>(
        &'a self,
        ctx: &'a HashMap<String, Value>,
        batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<crate::columnar::ColumnarPage, FaucetError>> + Send + 'a>>
    {
        Box::pin(async_stream::try_stream! {
            use metrics::{Label, SharedString, counter};
            let metric_labels = vec![
                Label::new("pipeline", SharedString::from(self.labels.pipeline.to_string())),
                Label::new("row", SharedString::from(self.labels.row.to_string())),
            ];
            let mut pages = self.inner.stream_batches(ctx, batch_size);
            while let Some(page) = pages.next().await {
                let crate::columnar::ColumnarPage { mut batch, bookmark } = page?;
                // Metric parity with the Value path (#636): the same
                // `faucet_transform_records_{in,out}_total` counters, per page.
                // A pure-projection kernel is 1→1, so in == out, but emitting
                // both keeps a dashboard identical across the two paths.
                let n_in = batch.num_rows();
                for bf in self.batch_fns.iter().flatten() {
                    batch = bf(batch).inspect_err(|_| {
                        counter!("faucet_transform_errors_total", metric_labels.clone())
                            .increment(1);
                    })?;
                }
                counter!("faucet_transform_records_in_total", metric_labels.clone())
                    .increment(n_in as u64);
                counter!("faucet_transform_records_out_total", metric_labels.clone())
                    .increment(batch.num_rows() as u64);
                yield crate::columnar::ColumnarPage { batch, bookmark };
            }
        })
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
    async fn lag(&self) -> Result<Option<crate::lag::SourceLag>, FaucetError> {
        self.inner.lag().await
    }

    fn state_schema(&self) -> u32 {
        self.inner.state_schema()
    }

    fn migrate_state(&self, from: u32, data: Value) -> Result<Value, FaucetError> {
        self.inner.migrate_state(from, data)
    }

    fn record_table(&self, record: &Value) -> Option<String> {
        self.inner.record_table(record)
    }

    fn is_shardable(&self) -> bool {
        self.inner.is_shardable()
    }

    async fn enumerate_shards(
        &self,
        target: usize,
    ) -> Result<Vec<crate::shard::ShardSpec>, FaucetError> {
        self.inner.enumerate_shards(target).await
    }

    async fn apply_shard(&self, shard: &crate::shard::ShardSpec) -> Result<(), FaucetError> {
        self.inner.apply_shard(shard).await
    }

    fn supports_discover(&self) -> bool {
        self.inner.supports_discover()
    }

    async fn discover(&self) -> Result<Vec<crate::discover::DatasetDescriptor>, FaucetError> {
        self.inner.discover().await
    }

    fn position_le(&self, a: &Value, b: &Value) -> Option<bool> {
        self.inner.position_le(a, b)
    }

    fn position_min(&self, positions: &[Value]) -> Option<Value> {
        self.inner.position_min(positions)
    }

    fn connector_name(&self) -> &'static str {
        self.inner.connector_name()
    }

    fn dataset_uri(&self) -> String {
        // Forward the wrapped connector's identity — without this, lineage and
        // the Data Movement Catalog would see the default
        // `<connector>://unknown` whenever transforms are attached.
        self.inner.dataset_uri()
    }

    fn set_roundtrip_recorder(
        &self,
        recorder: std::sync::Arc<crate::observability::RoundtripRecorder>,
    ) {
        self.inner.set_roundtrip_recorder(recorder);
    }

    fn set_run_clock(&self, now: chrono::DateTime<chrono::Utc>) {
        self.inner.set_run_clock(now);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A change-stream double with its own multi-table hooks (#731).
    #[tokio::test]
    async fn routed_double_fetches_nothing() {
        use crate::Source as _;
        assert!(
            RoutedSource
                .fetch_with_context(&Default::default())
                .await
                .unwrap()
                .is_empty()
        );
    }

    struct RoutedSource;

    #[async_trait::async_trait]
    impl crate::Source for RoutedSource {
        async fn fetch_with_context(
            &self,
            _ctx: &std::collections::HashMap<String, serde_json::Value>,
        ) -> Result<Vec<serde_json::Value>, crate::FaucetError> {
            Ok(Vec::new())
        }

        fn record_table(&self, record: &serde_json::Value) -> Option<String> {
            record.get("t")?.as_str().map(str::to_string)
        }

        fn position_le(&self, a: &serde_json::Value, b: &serde_json::Value) -> Option<bool> {
            Some(a.as_u64()? <= b.as_u64()?)
        }

        fn position_min(&self, _positions: &[serde_json::Value]) -> Option<serde_json::Value> {
            Some(serde_json::json!("inner-min"))
        }
    }

    fn assert_forwards_multi_table_hooks(s: &dyn crate::Source) {
        assert_eq!(
            s.record_table(&serde_json::json!({"t": "public.a"}))
                .as_deref(),
            Some("public.a")
        );
        assert_eq!(
            s.position_le(&serde_json::json!(1), &serde_json::json!(2)),
            Some(true)
        );
        assert_eq!(
            s.position_le(&serde_json::json!(3), &serde_json::json!(2)),
            Some(false)
        );
        assert_eq!(
            s.position_min(&[serde_json::json!(1), serde_json::json!(2)]),
            Some(serde_json::json!("inner-min"))
        );
    }

    #[test]
    fn multi_table_hooks_are_forwarded_to_the_inner_source() {
        let wrapped =
            TransformingSource::new(Box::new(RoutedSource), vec![], Labels::for_named("test"))
                .unwrap();
        assert_forwards_multi_table_hooks(&wrapped);
    }

    #[derive(Default)]
    struct ShardedSource {
        applied: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait]
    impl Source for ShardedSource {
        async fn fetch_with_context(
            &self,
            _ctx: &HashMap<String, serde_json::Value>,
        ) -> Result<Vec<serde_json::Value>, FaucetError> {
            Ok(vec![])
        }
        fn is_shardable(&self) -> bool {
            true
        }
        async fn enumerate_shards(
            &self,
            target: usize,
        ) -> Result<Vec<crate::shard::ShardSpec>, FaucetError> {
            Ok((0..target)
                .map(|i| crate::shard::ShardSpec::new(format!("s{i}"), serde_json::json!(i)))
                .collect())
        }
        async fn apply_shard(&self, shard: &crate::shard::ShardSpec) -> Result<(), FaucetError> {
            self.applied.lock().unwrap().push(shard.id.clone());
            Ok(())
        }
        async fn range_digest(
            &self,
            _range: &crate::diff::KeyRange,
            _key: &str,
            _columns: &[String],
        ) -> Result<Option<crate::diff::ServerDigest>, FaucetError> {
            Err(FaucetError::Source(
                "a raw-data digest must not be consulted".into(),
            ))
        }
        fn supports_discover(&self) -> bool {
            true
        }
        async fn discover(&self) -> Result<Vec<crate::discover::DatasetDescriptor>, FaucetError> {
            Ok(vec![crate::discover::DatasetDescriptor::new(
                "orders",
                "table",
                serde_json::json!({}),
            )])
        }
    }

    #[tokio::test]
    async fn shard_and_discover_hooks_reach_the_inner_source() {
        let inner = Arc::new(ShardedSource::default());
        struct Shared(Arc<ShardedSource>);
        #[async_trait]
        impl Source for Shared {
            async fn fetch_with_context(
                &self,
                ctx: &HashMap<String, serde_json::Value>,
            ) -> Result<Vec<serde_json::Value>, FaucetError> {
                self.0.fetch_with_context(ctx).await
            }
            fn is_shardable(&self) -> bool {
                self.0.is_shardable()
            }
            async fn enumerate_shards(
                &self,
                target: usize,
            ) -> Result<Vec<crate::shard::ShardSpec>, FaucetError> {
                self.0.enumerate_shards(target).await
            }
            async fn apply_shard(
                &self,
                shard: &crate::shard::ShardSpec,
            ) -> Result<(), FaucetError> {
                self.0.apply_shard(shard).await
            }
            async fn range_digest(
                &self,
                range: &crate::diff::KeyRange,
                key: &str,
                columns: &[String],
            ) -> Result<Option<crate::diff::ServerDigest>, FaucetError> {
                self.0.range_digest(range, key, columns).await
            }
            fn supports_discover(&self) -> bool {
                self.0.supports_discover()
            }
            async fn discover(
                &self,
            ) -> Result<Vec<crate::discover::DatasetDescriptor>, FaucetError> {
                self.0.discover().await
            }
        }
        let wrapped = TransformingSource::new(
            Box::new(Shared(inner.clone())),
            vec![],
            Labels::for_named("t"),
        )
        .unwrap();
        assert!(wrapped.is_shardable());
        let shards = wrapped.enumerate_shards(3).await.unwrap();
        assert_eq!(shards.len(), 3);
        wrapped.apply_shard(&shards[1]).await.unwrap();
        assert_eq!(*inner.applied.lock().unwrap(), vec!["s1".to_string()]);
        assert!(wrapped.supports_discover());
        assert_eq!(wrapped.discover().await.unwrap()[0].name, "orders");
        let digest = wrapped
            .range_digest(&crate::diff::KeyRange::ALL, "id", &[])
            .await
            .unwrap();
        assert!(
            digest.is_none(),
            "transformed rows must be compared client-side, never by a raw-data digest"
        );
    }
    use crate::stage::TransformStage;
    use crate::transform::{KeyCaseMode, RecordTransform};
    use serde_json::json;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct MockSource(Vec<Value>);

    #[async_trait]
    impl Source for MockSource {
        async fn fetch_with_context(
            &self,
            _ctx: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(self.0.clone())
        }
    }

    #[tokio::test]
    async fn fetch_with_context_transforms_records() {
        let inner: Box<dyn Source> = Box::new(MockSource(vec![json!({"FooBar": 1})]));
        let wrapped = TransformingSource::new(
            inner,
            vec![TransformStage::Map(RecordTransform::KeysCase {
                mode: KeyCaseMode::Snake,
                on_collision: Default::default(),
            })],
            Labels::for_named("test"),
        )
        .expect("compile succeeds");
        let out = wrapped.fetch_with_context(&HashMap::new()).await.unwrap();
        assert_eq!(out, vec![json!({"foo_bar": 1})]);
    }

    struct VersionedSource;

    #[async_trait]
    impl Source for VersionedSource {
        async fn fetch_with_context(
            &self,
            _ctx: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(Vec::new())
        }
        fn state_schema(&self) -> u32 {
            2
        }
        fn migrate_state(&self, from: u32, data: Value) -> Result<Value, FaucetError> {
            Ok(json!({"from": from, "data": data}))
        }
    }

    #[test]
    fn state_versioning_is_forwarded_to_the_inner_source() {
        let wrapped =
            TransformingSource::new(Box::new(VersionedSource), vec![], Labels::for_named("test"))
                .expect("compile succeeds");
        assert_eq!(wrapped.state_schema(), 2);
        assert_eq!(
            wrapped.migrate_state(1, json!("x")).unwrap(),
            json!({"from": 1, "data": "x"})
        );
    }

    struct IncrementalSource {
        records: Vec<Value>,
        bookmark: Value,
    }

    #[async_trait]
    impl Source for IncrementalSource {
        async fn fetch_with_context(
            &self,
            _ctx: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(self.records.clone())
        }

        async fn fetch_with_context_incremental(
            &self,
            _ctx: &HashMap<String, Value>,
        ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
            Ok((self.records.clone(), Some(self.bookmark.clone())))
        }
    }

    #[tokio::test]
    async fn fetch_with_context_incremental_transforms_and_preserves_bookmark() {
        let inner: Box<dyn Source> = Box::new(IncrementalSource {
            records: vec![json!({"FooBar": 1})],
            bookmark: json!("2026-05-28T00:00:00Z"),
        });
        let wrapped = TransformingSource::new(
            inner,
            vec![TransformStage::Map(RecordTransform::KeysCase {
                mode: KeyCaseMode::Snake,
                on_collision: Default::default(),
            })],
            Labels::for_named("test"),
        )
        .unwrap();
        let (records, bookmark) = wrapped
            .fetch_with_context_incremental(&HashMap::new())
            .await
            .unwrap();
        assert_eq!(records, vec![json!({"foo_bar": 1})]);
        assert_eq!(bookmark, Some(json!("2026-05-28T00:00:00Z")));
    }

    /// Emits records as N predetermined pages with the bookmark only on the last.
    /// Overrides `stream_pages` directly so the test catches whether the wrapper
    /// delegates to the native streaming path (correct) or falls back to the
    /// chunk-the-buffer default (wrong — the bug we're fixing).
    struct PagedSource {
        pages: Vec<Vec<Value>>,
        final_bookmark: Value,
    }

    #[async_trait]
    impl Source for PagedSource {
        async fn fetch_with_context(
            &self,
            _ctx: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(self.pages.iter().flatten().cloned().collect())
        }

        fn stream_pages<'a>(
            &'a self,
            _ctx: &'a HashMap<String, Value>,
            _batch_size: usize,
        ) -> Pin<Box<dyn futures_core::Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>>
        {
            let pages = self.pages.clone();
            let bookmark = self.final_bookmark.clone();
            Box::pin(async_stream::try_stream! {
                let n = pages.len();
                for (i, records) in pages.into_iter().enumerate() {
                    let bm = if i + 1 == n { Some(bookmark.clone()) } else { None };
                    yield StreamPage { records, bookmark: bm };
                }
            })
        }
    }

    #[tokio::test]
    async fn stream_pages_transforms_each_page_and_preserves_bookmarks() {
        let inner: Box<dyn Source> = Box::new(PagedSource {
            pages: vec![
                vec![json!({"FooBar": 1})],
                vec![json!({"FooBar": 2})],
                vec![json!({"FooBar": 3})],
            ],
            final_bookmark: json!("v1"),
        });
        let wrapped = TransformingSource::new(
            inner,
            vec![TransformStage::Map(RecordTransform::KeysCase {
                mode: KeyCaseMode::Snake,
                on_collision: Default::default(),
            })],
            Labels::for_named("test"),
        )
        .unwrap();

        let ctx = HashMap::new();
        let mut stream = wrapped.stream_pages(&ctx, 1000);
        let mut collected: Vec<StreamPage> = Vec::new();
        while let Some(page) = stream.next().await {
            collected.push(page.unwrap());
        }

        assert_eq!(collected.len(), 3);
        assert_eq!(collected[0].records, vec![json!({"foo_bar": 1})]);
        assert!(collected[0].bookmark.is_none());
        assert_eq!(collected[1].records, vec![json!({"foo_bar": 2})]);
        assert!(collected[1].bookmark.is_none());
        assert_eq!(collected[2].records, vec![json!({"foo_bar": 3})]);
        assert_eq!(collected[2].bookmark, Some(json!("v1")));
    }

    /// Regression: a 1→1 transform must NOT re-chunk the inner page below the
    /// size the inner source already chose. A source that yields one large page
    /// (its config `batch_size` honored) followed by a `keys_case` stage must
    /// still emit that page as ONE `StreamPage`, not many hint-sized sub-pages —
    /// otherwise an explicit source `batch_size` is silently defeated whenever a
    /// transform is present (the cause of 60 tiny BigQuery load jobs instead of
    /// one).
    #[tokio::test]
    async fn stream_pages_does_not_rechunk_large_page_below_inner_size() {
        let big: Vec<Value> = (0..2500).map(|i| json!({"FooBar": i})).collect();
        let inner: Box<dyn Source> = Box::new(PagedSource {
            pages: vec![big],
            final_bookmark: json!("v1"),
        });
        let wrapped = TransformingSource::new(
            inner,
            vec![TransformStage::Map(RecordTransform::KeysCase {
                mode: KeyCaseMode::Snake,
                on_collision: Default::default(),
            })],
            Labels::for_named("t"),
        )
        .unwrap();
        let ctx = HashMap::new();
        // Pipeline hint is the 1000-row default; it must NOT shrink the page.
        let mut stream = wrapped.stream_pages(&ctx, 1000);
        let mut pages: Vec<StreamPage> = Vec::new();
        while let Some(p) = stream.next().await {
            pages.push(p.unwrap());
        }
        assert_eq!(pages.len(), 1, "one inner page must stay one page");
        assert_eq!(pages[0].records.len(), 2500);
        assert_eq!(pages[0].records[0], json!({"foo_bar": 0}));
        assert_eq!(pages[0].bookmark, Some(json!("v1")));
    }

    #[tokio::test]
    async fn stream_pages_passes_through_empty_records_page_with_bookmark() {
        struct EmptyWithBookmark;
        #[async_trait]
        impl Source for EmptyWithBookmark {
            async fn fetch_with_context(
                &self,
                _ctx: &HashMap<String, Value>,
            ) -> Result<Vec<Value>, FaucetError> {
                Ok(Vec::new())
            }
            fn stream_pages<'a>(
                &'a self,
                _ctx: &'a HashMap<String, Value>,
                _batch_size: usize,
            ) -> Pin<
                Box<dyn futures_core::Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>,
            > {
                Box::pin(async_stream::try_stream! {
                    yield StreamPage { records: Vec::new(), bookmark: Some(json!("v1")) };
                })
            }
        }
        let wrapped = TransformingSource::new(
            Box::new(EmptyWithBookmark),
            vec![TransformStage::Map(RecordTransform::KeysCase {
                mode: KeyCaseMode::Snake,
                on_collision: Default::default(),
            })],
            Labels::for_named("test"),
        )
        .unwrap();
        let ctx = HashMap::new();
        let mut stream = wrapped.stream_pages(&ctx, 1000);
        let page = stream.next().await.unwrap().unwrap();
        assert!(page.records.is_empty());
        assert_eq!(page.bookmark, Some(json!("v1")));
        assert!(stream.next().await.is_none());
    }

    struct InstrumentedSource {
        started: Arc<AtomicBool>,
    }

    #[async_trait]
    impl Source for InstrumentedSource {
        async fn fetch_with_context(
            &self,
            _ctx: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(vec![])
        }
        fn connector_name(&self) -> &'static str {
            "instrumented"
        }
        fn state_key(&self) -> Option<String> {
            Some("instrumented::key".to_string())
        }
        async fn apply_start_bookmark(&self, _bookmark: Value) -> Result<(), FaucetError> {
            self.started.store(true, Ordering::Relaxed);
            Ok(())
        }
        fn supports_exactly_once(&self) -> bool {
            true
        }
        async fn capture_resume_position(&self) -> Result<Option<Value>, FaucetError> {
            Ok(Some(json!("captured")))
        }
    }

    struct RecorderProbe(Arc<AtomicBool>);

    #[async_trait]
    impl Source for RecorderProbe {
        async fn fetch_with_context(
            &self,
            _ctx: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(vec![])
        }
        fn set_roundtrip_recorder(&self, _recorder: Arc<crate::observability::RoundtripRecorder>) {
            self.0.store(true, Ordering::Relaxed);
        }
        fn set_run_clock(&self, _now: chrono::DateTime<chrono::Utc>) {
            self.0.store(true, Ordering::Relaxed);
        }
    }

    #[test]
    fn run_clock_reaches_the_wrapped_source() {
        let got = Arc::new(AtomicBool::new(false));
        let wrapped = TransformingSource::new(
            Box::new(RecorderProbe(got.clone())),
            vec![],
            Labels::for_named("test"),
        )
        .unwrap();
        wrapped.set_run_clock(chrono::Utc::now());
        assert!(got.load(Ordering::Relaxed));
    }

    #[test]
    fn roundtrip_recorder_reaches_the_wrapped_source() {
        let got = Arc::new(AtomicBool::new(false));
        let wrapped = TransformingSource::new(
            Box::new(RecorderProbe(got.clone())),
            vec![],
            Labels::for_named("test"),
        )
        .unwrap();
        wrapped.set_roundtrip_recorder(Arc::new(crate::observability::RoundtripRecorder::new(
            crate::observability::RoundtripSide::Source,
            "p",
            "r",
            "probe",
        )));
        assert!(got.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn connector_name_state_key_and_start_bookmark_delegate_to_inner() {
        let started = Arc::new(AtomicBool::new(false));
        let inner = InstrumentedSource {
            started: started.clone(),
        };
        let wrapped = TransformingSource::new(
            Box::new(inner),
            vec![TransformStage::Map(RecordTransform::KeysCase {
                mode: KeyCaseMode::Snake,
                on_collision: Default::default(),
            })],
            Labels::for_named("test"),
        )
        .unwrap();
        assert_eq!(wrapped.connector_name(), "instrumented");
        assert_eq!(wrapped.state_key(), Some("instrumented::key".to_string()));
        wrapped.apply_start_bookmark(json!("bm")).await.unwrap();
        assert!(started.load(Ordering::Relaxed));
        // Exactly-once capabilities must survive the transform wrap — the
        // pipeline's mechanism selection reads them through this layer.
        assert!(wrapped.supports_exactly_once());
        assert_eq!(
            wrapped.replay_guarantee(),
            crate::idempotency::ReplayGuarantee::Deterministic
        );
        assert_eq!(
            wrapped.capture_resume_position().await.unwrap(),
            Some(json!("captured"))
        );
        assert_eq!(wrapped.lag().await.unwrap(), None);
    }

    #[tokio::test]
    async fn new_fails_fast_on_invalid_regex() {
        let inner: Box<dyn Source> = Box::new(MockSource(vec![]));
        let result = TransformingSource::new(
            inner,
            vec![TransformStage::Map(RecordTransform::RenameKeys {
                pattern: "[invalid".to_string(),
                replacement: "x".to_string(),
            })],
            Labels::for_named("test"),
        );
        let err = match result {
            Ok(_) => panic!("invalid regex must fail at new()"),
            Err(e) => e,
        };
        assert!(matches!(err, FaucetError::Transform(_)));
    }

    #[tokio::test]
    async fn custom_closure_transform_runs() {
        let inner: Box<dyn Source> = Box::new(MockSource(vec![json!({"x": 1})]));
        let wrapped = TransformingSource::new(
            inner,
            vec![TransformStage::Map(RecordTransform::custom(
                |mut record| {
                    if let Some(obj) = record.as_object_mut() {
                        obj.insert("added".to_string(), json!(true));
                    }
                    record
                },
            ))],
            Labels::for_named("test"),
        )
        .unwrap();
        let out = wrapped.fetch_with_context(&HashMap::new()).await.unwrap();
        assert_eq!(out, vec![json!({"x": 1, "added": true})]);
    }

    #[tokio::test]
    async fn usable_as_boxed_dyn_source() {
        let inner: Box<dyn Source> = Box::new(MockSource(vec![json!({"FooBar": 1})]));
        let wrapped: Box<dyn Source> = Box::new(
            TransformingSource::new(
                inner,
                vec![TransformStage::Map(RecordTransform::KeysCase {
                    mode: KeyCaseMode::Snake,
                    on_collision: Default::default(),
                })],
                Labels::for_named("test"),
            )
            .unwrap(),
        );
        let out = wrapped.fetch_with_context(&HashMap::new()).await.unwrap();
        assert_eq!(out, vec![json!({"foo_bar": 1})]);
    }

    /// A source that emits a single page with the given records and bookmark.
    ///
    /// Only the bookmark-forwarding tests construct it, and those are gated on
    /// a transform feature, so a build without one compiles it unused.
    #[allow(dead_code)]
    struct OnePageSource {
        records: Vec<Value>,
        bookmark: Option<Value>,
    }

    #[async_trait]
    impl Source for OnePageSource {
        async fn fetch_with_context(
            &self,
            _ctx: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(self.records.clone())
        }
        async fn fetch_with_context_incremental(
            &self,
            _ctx: &HashMap<String, Value>,
        ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
            Ok((self.records.clone(), self.bookmark.clone()))
        }
        fn stream_pages<'a>(
            &'a self,
            _ctx: &'a HashMap<String, Value>,
            _batch_size: usize,
        ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
            let page = StreamPage {
                records: self.records.clone(),
                bookmark: self.bookmark.clone(),
            };
            Box::pin(async_stream::stream! { yield Ok(page); })
        }
    }

    #[cfg(feature = "transform-explode")]
    fn explode_stage() -> TransformStage {
        TransformStage::Explode(crate::stage::ExplodeSpec {
            path: "items".to_owned(),
            prefix: None,
            separator: "_".to_owned(),
            on_missing: crate::stage::OnMissing::Drop,
        })
    }

    /// Build N records each with a 10-element `items` array, so explode 10×s them.
    #[cfg(feature = "transform-explode")]
    fn explode_10x_records(n: usize) -> Vec<Value> {
        (0..n)
            .map(|i| {
                json!({
                    "id": i,
                    "items": (0..10).map(|j| json!({"k": j})).collect::<Vec<_>>(),
                })
            })
            .collect()
    }

    #[cfg(feature = "transform-explode")]
    #[tokio::test]
    async fn stream_pages_never_splits_a_bookmark_carrying_page() {
        let inner: Box<dyn Source> = Box::new(OnePageSource {
            records: explode_10x_records(100), // 100 → 1000 after explode
            bookmark: Some(json!("bm")),
        });
        let wrapped =
            TransformingSource::new(inner, vec![explode_stage()], Labels::for_named("t")).unwrap();
        let ctx = HashMap::new();
        let mut stream = wrapped.stream_pages(&ctx, 200);
        let mut sub_pages: Vec<StreamPage> = Vec::new();
        while let Some(p) = stream.next().await {
            sub_pages.push(p.unwrap());
        }
        assert_eq!(
            sub_pages.len(),
            1,
            "a bookmarked page stays one commit unit"
        );
        assert_eq!(sub_pages[0].records.len(), 1000);
        assert_eq!(sub_pages[0].bookmark, Some(json!("bm")));
    }

    #[cfg(feature = "transform-explode")]
    #[tokio::test]
    async fn stream_pages_rechunks_a_grown_page_without_a_bookmark() {
        let inner: Box<dyn Source> = Box::new(OnePageSource {
            records: explode_10x_records(100),
            bookmark: None,
        });
        let wrapped =
            TransformingSource::new(inner, vec![explode_stage()], Labels::for_named("t")).unwrap();
        let ctx = HashMap::new();
        let mut stream = wrapped.stream_pages(&ctx, 200);
        let mut sub_pages: Vec<StreamPage> = Vec::new();
        while let Some(p) = stream.next().await {
            sub_pages.push(p.unwrap());
        }
        assert_eq!(sub_pages.len(), 5, "1000 records / 200 batch = 5 sub-pages");
        assert!(sub_pages.iter().all(|p| p.records.len() == 200));
        assert!(sub_pages.iter().all(|p| p.bookmark.is_none()));
    }

    #[cfg(feature = "transform-explode")]
    #[tokio::test]
    async fn stream_pages_batch_size_zero_emits_one_page() {
        let inner: Box<dyn Source> = Box::new(OnePageSource {
            records: explode_10x_records(10), // 10 → 100 after explode
            bookmark: Some(json!("bm")),
        });
        let wrapped =
            TransformingSource::new(inner, vec![explode_stage()], Labels::for_named("t")).unwrap();
        let ctx = HashMap::new();
        let mut stream = wrapped.stream_pages(&ctx, 0);
        let mut sub_pages: Vec<StreamPage> = Vec::new();
        while let Some(p) = stream.next().await {
            sub_pages.push(p.unwrap());
        }
        assert_eq!(sub_pages.len(), 1, "batch_size=0 means one sub-page");
        assert_eq!(sub_pages[0].records.len(), 100);
        assert_eq!(sub_pages[0].bookmark, Some(json!("bm")));
    }

    #[cfg(feature = "transform-filter")]
    #[tokio::test]
    async fn stream_pages_filter_drops_all_still_yields_bookmark() {
        let inner: Box<dyn Source> = Box::new(OnePageSource {
            records: vec![json!({"deleted": true}), json!({"deleted": true})],
            bookmark: Some(json!("bm")),
        });
        let drop_all = TransformStage::Filter(crate::stage::FilterSpec {
            path: "deleted".to_owned(),
            op: crate::stage::FilterOp::Ne,
            value: Some(json!(true)),
        });
        let wrapped =
            TransformingSource::new(inner, vec![drop_all], Labels::for_named("t")).unwrap();
        let ctx = HashMap::new();
        let mut stream = wrapped.stream_pages(&ctx, 100);
        let mut sub_pages: Vec<StreamPage> = Vec::new();
        while let Some(p) = stream.next().await {
            sub_pages.push(p.unwrap());
        }
        assert_eq!(sub_pages.len(), 1);
        assert!(sub_pages[0].records.is_empty());
        assert_eq!(sub_pages[0].bookmark, Some(json!("bm")));
    }
}

#[cfg(all(test, feature = "arrow"))]
mod columnar_tests {
    use super::*;
    use crate::columnar::{ColumnarPage, record_batch_to_values, values_to_record_batch_inferred};
    use crate::stage::TransformStage;
    use serde_json::json;
    use std::sync::Arc;

    /// A source that emits one Arrow batch (columnar-capable).
    struct ColumnarMock(Vec<Value>);
    #[async_trait]
    impl Source for ColumnarMock {
        async fn fetch_with_context(
            &self,
            _ctx: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(self.0.clone())
        }
        fn supports_columnar(&self) -> bool {
            true
        }
        fn stream_batches<'a>(
            &'a self,
            _ctx: &'a HashMap<String, Value>,
            _bs: usize,
        ) -> Pin<Box<dyn Stream<Item = Result<ColumnarPage, FaucetError>> + Send + 'a>> {
            let batch = values_to_record_batch_inferred(&self.0).unwrap();
            Box::pin(async_stream::stream! {
                yield Ok(ColumnarPage { batch, bookmark: Some(json!("bm")) });
            })
        }
    }

    /// A source with no columnar support (default `supports_columnar` = false).
    struct RowOnlyMock(Vec<Value>);
    #[async_trait]
    impl Source for RowOnlyMock {
        async fn fetch_with_context(
            &self,
            _ctx: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(self.0.clone())
        }
    }

    /// An identity page stage + its identity batch form — enough to exercise
    /// the columnar wiring.
    fn identity_stage() -> (TransformStage, Option<crate::stage::PageFnBatchBox>) {
        let rows: crate::stage::PageFnBox = Arc::new(Ok);
        let batch: crate::stage::PageFnBatchBox = Arc::new(Ok);
        (TransformStage::PageFn(rows), Some(batch))
    }

    #[tokio::test]
    async fn columnar_inner_plus_columnar_stage_is_supported_and_streams() {
        let inner: Box<dyn Source> =
            Box::new(ColumnarMock(vec![json!({"id": 1}), json!({"id": 2})]));
        let (stage, batch) = identity_stage();
        let wrapped = TransformingSource::new_with_batches(
            inner,
            vec![stage],
            vec![batch],
            Labels::for_named("t"),
        )
        .unwrap();
        assert!(wrapped.supports_columnar());
        let ctx = HashMap::new();
        let mut s = wrapped.stream_batches(&ctx, 0);
        let page = s.next().await.unwrap().unwrap();
        let rows = record_batch_to_values(&page.batch).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(page.bookmark, Some(json!("bm")));
    }

    #[tokio::test]
    async fn value_only_stage_disables_columnar() {
        // A `Map` stage has no Arrow batch form (batch_fn None) → the whole
        // chain drops off the columnar path even though the inner is columnar.
        let inner: Box<dyn Source> = Box::new(ColumnarMock(vec![json!({"FooBar": 1})]));
        let wrapped = TransformingSource::new(
            inner,
            vec![TransformStage::Map(
                crate::transform::RecordTransform::KeysCase {
                    mode: crate::transform::KeyCaseMode::Snake,
                    on_collision: crate::transform::KeyCollision::Error,
                },
            )],
            Labels::for_named("t"),
        )
        .unwrap();
        assert!(!wrapped.supports_columnar());
    }

    /// #636: a chain of *built-in vectorizable* transforms (no hand-supplied
    /// batch fns) over a columnar source keeps the fast path automatically —
    /// `new` now derives their Arrow kernels. This is the whole point: a
    /// `parquet → select/drop/set → parquet` run no longer drops to `Value`.
    #[cfg(all(feature = "transform-drop", feature = "transform-set"))]
    #[tokio::test]
    async fn built_in_vectorizable_transforms_keep_the_columnar_path() {
        use crate::transform::RecordTransform;
        let inner: Box<dyn Source> = Box::new(ColumnarMock(vec![
            json!({"id": 1, "name": "ada", "secret": "x"}),
            json!({"id": 2, "name": "grace", "secret": "y"}),
        ]));
        let mut set_vals = serde_json::Map::new();
        set_vals.insert("stage".into(), json!("prod"));
        let wrapped = TransformingSource::new(
            inner,
            vec![
                TransformStage::Map(RecordTransform::Drop {
                    fields: vec!["secret".into()],
                }),
                TransformStage::Map(RecordTransform::Set { values: set_vals }),
            ],
            Labels::for_named("t"),
        )
        .unwrap();
        assert!(
            wrapped.supports_columnar(),
            "a chain of vectorizable built-ins must stay columnar"
        );
        let ctx = HashMap::new();
        let mut s = wrapped.stream_batches(&ctx, 0);
        let page = s.next().await.unwrap().unwrap();
        let rows = record_batch_to_values(&page.batch).unwrap();
        assert_eq!(rows.len(), 2);
        // `drop` removed `secret`, `set` added `stage` — the transforms really
        // ran on the columnar path, not just passed through.
        assert!(rows[0].get("secret").is_none(), "drop ran: {:?}", rows[0]);
        assert_eq!(rows[0]["stage"], json!("prod"), "set ran: {:?}", rows[0]);
    }

    /// One non-vectorizable transform anywhere in the chain holds the whole
    /// chain on the `Value` path — the vectorizable ones must not silently run
    /// columnar while the opaque one is skipped.
    #[cfg(all(feature = "transform-select", feature = "transform-flatten"))]
    #[tokio::test]
    async fn a_mixed_chain_with_one_opaque_transform_stays_on_value() {
        use crate::transform::RecordTransform;
        let inner: Box<dyn Source> = Box::new(ColumnarMock(vec![json!({"id": 1})]));
        let wrapped = TransformingSource::new(
            inner,
            vec![
                TransformStage::Map(RecordTransform::Select {
                    fields: vec!["id".into()],
                }),
                // `flatten` has no kernel → None → whole chain off columnar.
                TransformStage::Map(RecordTransform::Flatten {
                    separator: "_".into(),
                }),
            ],
            Labels::for_named("t"),
        )
        .unwrap();
        assert!(!wrapped.supports_columnar());
    }

    #[tokio::test]
    async fn columnar_stage_over_row_only_inner_is_disabled() {
        let inner: Box<dyn Source> = Box::new(RowOnlyMock(vec![json!({"id": 1})]));
        let (stage, batch) = identity_stage();
        let wrapped = TransformingSource::new_with_batches(
            inner,
            vec![stage],
            vec![batch],
            Labels::for_named("t"),
        )
        .unwrap();
        assert!(!wrapped.supports_columnar(), "inner is not columnar");
    }
}
