//! End-to-end streaming pipeline integration tests.
//!
//! Exercises `Pipeline::run` driving `Source::stream_pages` through a
//! multi-page mock source, verifying sink-side batching and bookmark
//! persistence semantics.

use async_trait::async_trait;
use faucet_core::{FaucetError, MemoryStateStore, Pipeline, Sink, Source, StateStore, StreamPage};
use futures_core::Stream;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

/// A streaming mock source that emits records as multiple pages of size
/// `page_size` (plus a final short page if `total % page_size != 0`). The
/// final page carries `Some(bookmark)`.
struct MultiPageSource {
    total: usize,
    page_size: usize,
    bookmark: Value,
}

#[async_trait]
impl Source for MultiPageSource {
    async fn fetch_with_context(
        &self,
        _ctx: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        unreachable!("Pipeline::run must drive stream_pages, not fetch_with_context");
    }
    fn stream_pages<'a>(
        &'a self,
        _ctx: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let total = self.total;
        let page_size = self.page_size;
        let bookmark = self.bookmark.clone();
        Box::pin(async_stream::try_stream! {
            let mut emitted = 0usize;
            while emitted < total {
                let upper = (emitted + page_size).min(total);
                let records: Vec<Value> = (emitted..upper).map(|i| json!({"i": i})).collect();
                emitted = upper;
                let final_bookmark = if emitted >= total {
                    Some(bookmark.clone())
                } else {
                    None
                };
                yield StreamPage {
                    records,
                    bookmark: final_bookmark,
                };
            }
        })
    }
    fn state_key(&self) -> Option<String> {
        Some("multipage_test".into())
    }
}

/// A sink that records every `write_batch` call's record count so we can
/// assert batching boundaries.
struct CallCountSink {
    calls: Mutex<Vec<usize>>,
}

impl CallCountSink {
    fn new() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
        }
    }
    fn calls(&self) -> Vec<usize> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl Sink for CallCountSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        self.calls.lock().unwrap().push(records.len());
        Ok(records.len())
    }
}

#[tokio::test]
async fn pipeline_streams_multiple_pages_to_sink() {
    let source = MultiPageSource {
        total: 10_000,
        page_size: 1_000,
        bookmark: json!("v1"),
    };
    let sink = CallCountSink::new();

    let result = Pipeline::new(&source, &sink).run().await.unwrap();

    assert_eq!(result.records_written, 10_000);
    assert_eq!(result.bookmark, Some(json!("v1")));
    // 10k records / 1k pages = 10 write_batch calls of 1000 records each.
    assert_eq!(sink.calls(), vec![1000; 10]);
}

#[tokio::test]
async fn pipeline_streams_uneven_final_page() {
    let source = MultiPageSource {
        total: 2_500,
        page_size: 1_000,
        bookmark: json!("v1"),
    };
    let sink = CallCountSink::new();

    Pipeline::new(&source, &sink).run().await.unwrap();

    assert_eq!(sink.calls(), vec![1000, 1000, 500]);
}

#[tokio::test]
async fn pipeline_persists_bookmark_only_after_final_page() {
    let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
    let source = MultiPageSource {
        total: 3_000,
        page_size: 1_000,
        bookmark: json!("final-checkpoint"),
    };
    let sink = CallCountSink::new();

    Pipeline::new(&source, &sink)
        .with_state_store(Arc::clone(&store))
        .run()
        .await
        .unwrap();

    assert_eq!(
        store
            .get("multipage_test")
            .await
            .unwrap()
            .map(|v| faucet_core::state_version::peel_versioned(&v)),
        Some(json!("final-checkpoint"))
    );
}
