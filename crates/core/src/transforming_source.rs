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
        #[cfg(feature = "arrow")]
        let n = compiled.len();
        Ok(Self {
            inner,
            stages: compiled,
            labels,
            #[cfg(feature = "arrow")]
            batch_fns: vec![None; n],
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
                if batch_size == 0 {
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
            let mut pages = self.inner.stream_batches(ctx, batch_size);
            while let Some(page) = pages.next().await {
                let crate::columnar::ColumnarPage { mut batch, bookmark } = page?;
                for bf in self.batch_fns.iter().flatten() {
                    batch = bf(batch)?;
                }
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

    fn connector_name(&self) -> &'static str {
        self.inner.connector_name()
    }

    fn dataset_uri(&self) -> String {
        // Forward the wrapped connector's identity — without this, lineage and
        // the Data Movement Catalog would see the default
        // `<connector>://unknown` whenever transforms are attached.
        self.inner.dataset_uri()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    async fn stream_pages_rechunks_explosion_with_bookmark_on_last() {
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
        assert_eq!(sub_pages.len(), 5, "1000 records / 200 batch = 5 sub-pages");
        for (i, p) in sub_pages.iter().enumerate() {
            assert_eq!(p.records.len(), 200, "sub-page {i} should be size 200");
            if i < 4 {
                assert!(
                    p.bookmark.is_none(),
                    "non-final sub-page {i} carries no bookmark"
                );
            } else {
                assert_eq!(p.bookmark, Some(json!("bm")), "final sub-page has bookmark");
            }
        }
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
