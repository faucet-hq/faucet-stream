//! Bounded record sampling for previews (`faucet preview`, the MCP `preview`
//! tool and `run_pipeline {dry_run: true}`): pull pages only until `limit`
//! transformed records are in hand, under a deadline, so a preview of a large
//! or never-ending source never reads the whole dataset into memory.

use crate::error::CliResult;
use faucet_core::Source;
use faucet_core::stage::{CompiledStage, apply_stages_to_page};
use futures::StreamExt as _;
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;

/// How long a preview may read before it returns what it has.
pub const PREVIEW_TIMEOUT: Duration = Duration::from_secs(30);

/// The records a preview collected, and whether the deadline cut it short.
#[derive(Debug, Default)]
pub struct Sample {
    pub records: Vec<Value>,
    pub timed_out: bool,
}

/// The refusal for reading a source that acks, deletes or settles messages
/// as it is read (`Source::consumes_destructively`) from a command that does
/// not durably write what it reads — the messages would leave the queue for
/// good (#789 MSG-01).
pub fn destructive_read_refusal(kind: &str, command: &str) -> String {
    format!(
        "the `{kind}` source removes messages from its queue as it reads them, and {command} \
         does not durably write what it reads, so those messages would be lost; run against a \
         test queue or subscription, or run the pipeline for real"
    )
}

/// Read pages from `source`, transform each, and stop once `limit` records
/// are collected or `timeout` passes. A source error ends the preview with
/// that error. A source that consumes destructively is refused before any
/// read.
pub async fn sample(
    source: &dyn Source,
    stages: &[CompiledStage],
    limit: usize,
    timeout: Duration,
) -> CliResult<Sample> {
    if source.consumes_destructively() {
        return Err(crate::error::CliError::Config(destructive_read_refusal(
            source.connector_name(),
            "a preview",
        )));
    }
    let mut records: Vec<Value> = Vec::new();
    let context = HashMap::new();
    let collect = async {
        let mut pages = source.stream_pages(&context, limit.max(1));
        while records.len() < limit {
            let Some(page) = pages.next().await else {
                break;
            };
            let mut out = apply_stages_to_page(page?.records, stages)?;
            out.truncate(limit - records.len());
            records.extend(out);
        }
        CliResult::Ok(())
    };
    let timed_out = match tokio::time::timeout(timeout, collect).await {
        Ok(result) => {
            result?;
            false
        }
        Err(_) => true,
    };
    Ok(Sample { records, timed_out })
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::{FaucetError, StreamPage};
    use futures::Stream;
    use serde_json::json;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Endless {
        pages: Arc<AtomicUsize>,
        stall_after: Option<usize>,
        fail: bool,
    }

    #[async_trait::async_trait]
    impl Source for Endless {
        async fn fetch_with_context(
            &self,
            _: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Err(FaucetError::Source(
                "a preview must stream, never fetch everything".into(),
            ))
        }

        fn stream_pages<'a>(
            &'a self,
            _: &'a HashMap<String, Value>,
            _: usize,
        ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
            Box::pin(async_stream::try_stream! {
                let mut i = 0u64;
                loop {
                    if self.fail {
                        Err(FaucetError::Source("boom".into()))?;
                    }
                    let n = self.pages.fetch_add(1, Ordering::SeqCst);
                    if self.stall_after == Some(n) {
                        std::future::pending::<()>().await;
                    }
                    let records = (0..10).map(|_| { i += 1; json!({ "i": i }) }).collect();
                    yield StreamPage { records, bookmark: None };
                }
            })
        }
    }

    fn endless(stall_after: Option<usize>, fail: bool) -> (Endless, Arc<AtomicUsize>) {
        let pages = Arc::new(AtomicUsize::new(0));
        (
            Endless {
                pages: pages.clone(),
                stall_after,
                fail,
            },
            pages,
        )
    }

    #[tokio::test]
    async fn stops_reading_once_the_limit_is_reached() {
        let (src, pages) = endless(None, false);
        let s = sample(&src, &[], 25, Duration::from_secs(5)).await.unwrap();
        assert_eq!(s.records.len(), 25);
        assert_eq!(s.records[24], json!({ "i": 25 }));
        assert!(!s.timed_out);
        assert_eq!(
            pages.load(Ordering::SeqCst),
            3,
            "only the pages the limit needs"
        );
        assert!(
            src.fetch_with_context(&HashMap::new()).await.is_err(),
            "the fixture refuses a whole-dataset read"
        );
    }

    #[tokio::test]
    async fn transforms_only_the_pages_it_reads() {
        let keep_above_five = CompiledStage::PageFn(Arc::new(|page: Vec<Value>| {
            Ok(page
                .into_iter()
                .filter(|r| r["i"].as_u64() > Some(5))
                .collect())
        }));
        let (src, pages) = endless(None, false);
        let s = sample(&src, &[keep_above_five], 3, Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(
            s.records,
            vec![json!({"i": 6}), json!({"i": 7}), json!({"i": 8})]
        );
        assert_eq!(pages.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_stalled_source_returns_what_it_has_at_the_deadline() {
        let (src, _) = endless(Some(1), false);
        let s = sample(&src, &[], 100, Duration::from_millis(50))
            .await
            .unwrap();
        assert!(s.timed_out);
        assert_eq!(s.records.len(), 10);
    }

    #[tokio::test]
    async fn a_source_error_ends_the_preview() {
        let (src, _) = endless(None, true);
        assert!(sample(&src, &[], 5, Duration::from_secs(5)).await.is_err());
    }

    #[tokio::test]
    async fn a_source_that_consumes_destructively_is_refused_before_any_read() {
        struct Queue(std::sync::atomic::AtomicUsize);
        #[faucet_core::async_trait]
        impl Source for Queue {
            async fn fetch_with_context(
                &self,
                _: &HashMap<String, Value>,
            ) -> Result<Vec<Value>, FaucetError> {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(Vec::new())
            }
            fn consumes_destructively(&self) -> bool {
                true
            }
            fn connector_name(&self) -> &'static str {
                "sqs"
            }
        }
        let queue = Queue(Default::default());
        let err = sample(&queue, &[], 10, PREVIEW_TIMEOUT).await.unwrap_err();
        assert!(
            err.to_string().contains("`sqs` source removes messages"),
            "{err}"
        );
        let reads = || queue.0.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(reads(), 0, "a refused preview must not read");
        queue.fetch_all().await.unwrap();
        assert_eq!(reads(), 1);
    }
}
