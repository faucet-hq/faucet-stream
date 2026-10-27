//! Bounded record sampling for previews (`faucet preview`, the MCP `preview`
//! tool and `run_pipeline {dry_run: true}`): pull pages only until `limit`
//! transformed records are in hand, under a deadline, so a preview of a large
//! or never-ending source never reads the whole dataset into memory.

use crate::error::CliResult;
use faucet_core::Source;
use faucet_core::stage::{CompiledStage, apply_stages_to_page_blocking};
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

/// The refusal for sampling a source whose read has side effects (its
/// connector declares `read_side_effects`): a CDC source creates or advances a
/// replication slot / capture position, a Kafka source joins (and rebalances)
/// the consumer group, a webhook or websocket source opens a listener.
pub fn side_effecting_read_refusal(kind: &str, command: &str) -> String {
    format!(
        "{command} would read a `{kind}` source, and reading it has side effects (a \
         replication slot, a consumer-group rebalance, a listener); pass \
         --allow-side-effects to read it anyway, or use an offline sample"
    )
}

/// Apply every rule of a config's `masking:` policy to previewed records — a
/// preview has no destination, so `applies_to` scoping does not narrow it,
/// as in `faucet test`.
/// The masking comes from [`crate::governance::preview_governance`], the
/// preview entry point's governance (#875).
#[cfg(feature = "masking")]
pub fn mask(records: Vec<Value>, spec: Option<&faucet_core::MaskingSpec>) -> CliResult<Vec<Value>> {
    let governance = crate::governance::preview_governance(spec)?;
    Ok(match governance.masking() {
        Some(m) => faucet_core::apply_masking(records, m).records,
        None => records,
    })
}

/// Cap a source config's `batch_size` at the preview `limit` (an unset or `0`
/// "no batching" size becomes `limit`), so a source that sizes its pages by
/// its own config never reads more than the preview needs into one page.
/// Configs of sources without a `batch_size` field are left alone.
pub fn bound_batch_size(kind: &str, config: &mut Value, limit: usize) {
    let limit = limit.max(1) as u64;
    let declared = crate::registry::source_schema(kind).is_ok_and(|s| {
        s.get("properties")
            .and_then(|p| p.get("batch_size"))
            .is_some()
    });
    let Value::Object(map) = config else {
        return;
    };
    if !declared {
        return;
    }
    let current = match map.get("batch_size") {
        None | Some(Value::Null) => None,
        Some(Value::Number(n)) => n.as_u64(),
        Some(Value::String(s)) => s.trim().parse::<u64>().ok(),
        Some(_) => return,
    };
    let bounded = match current {
        None | Some(0) => limit,
        Some(n) => n.min(limit),
    };
    map.insert("batch_size".into(), Value::from(bounded));
}

/// Read pages from `source`, transform each, and stop once `limit` records
/// are collected or `timeout` passes. A source error ends the preview with
/// that error. A source that consumes destructively, or whose read has side
/// effects, is refused before any read.
pub async fn sample(
    source: &dyn Source,
    stages: &[CompiledStage],
    limit: usize,
    timeout: Duration,
) -> CliResult<Sample> {
    sample_with(source, stages, limit, timeout, false).await
}

/// [`sample`], reading a source whose read has side effects when
/// `allow_side_effects` is set (`faucet preview --allow-side-effects`).
pub async fn sample_with(
    source: &dyn Source,
    stages: &[CompiledStage],
    limit: usize,
    timeout: Duration,
    allow_side_effects: bool,
) -> CliResult<Sample> {
    if source.consumes_destructively() {
        return Err(crate::error::CliError::Config(destructive_read_refusal(
            source.connector_name(),
            "a preview",
        )));
    }
    if !allow_side_effects && source.capabilities().read_side_effects() {
        return Err(crate::error::CliError::Config(side_effecting_read_refusal(
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
            let mut out = apply_stages_to_page_blocking(page?.records, stages)?;
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

/// How long a single-row probe (a partition bound, a reconcile count) may read.
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(300);

/// Read the first record of a probe source, streaming one-record pages so a
/// probe that returns many rows never loads them all. Refuses a source that
/// consumes destructively, and stops at `timeout` or when `cancel` fires.
pub async fn probe_first_record(
    source: &dyn Source,
    operation: &str,
    timeout: Duration,
    cancel: Option<&faucet_core::CancellationToken>,
) -> Result<Option<Value>, faucet_core::FaucetError> {
    use faucet_core::FaucetError;
    if source.consumes_destructively() {
        return Err(FaucetError::Config(destructive_read_refusal(
            source.connector_name(),
            operation,
        )));
    }
    let context = HashMap::new();
    let read = async {
        let mut pages = source.stream_pages(&context, 1);
        while let Some(page) = pages.next().await {
            if let Some(first) = page?.records.into_iter().next() {
                return Ok(Some(first));
            }
        }
        Ok(None)
    };
    let cancelled = async {
        match cancel {
            Some(token) => token.cancelled().await,
            None => std::future::pending().await,
        }
    };
    tokio::select! {
        biased;
        () = cancelled => Err(FaucetError::Cancelled { operation: operation.to_string() }),
        read = tokio::time::timeout(timeout, read) => read.unwrap_or_else(|_| {
            Err(FaucetError::Timeout { operation: operation.to_string(), after: timeout })
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "source-elasticsearch")]
    #[test]
    fn bound_batch_size_caps_the_page_at_the_limit() {
        let mut cfg = serde_json::json!({"batch_size": 0});
        bound_batch_size("elasticsearch", &mut cfg, 20);
        assert_eq!(cfg["batch_size"], 20);
        let mut cfg = serde_json::json!({"batch_size": "5000"});
        bound_batch_size("elasticsearch", &mut cfg, 20);
        assert_eq!(cfg["batch_size"], 20);
        let mut cfg = serde_json::json!({"batch_size": 7});
        bound_batch_size("elasticsearch", &mut cfg, 20);
        assert_eq!(cfg["batch_size"], 7);
        let mut cfg = serde_json::json!({});
        bound_batch_size("elasticsearch", &mut cfg, 0);
        assert_eq!(cfg["batch_size"], 1);
        let mut odd = serde_json::json!({"batch_size": [1]});
        bound_batch_size("elasticsearch", &mut odd, 20);
        assert_eq!(odd["batch_size"], serde_json::json!([1]));
        let mut unknown = serde_json::json!({"a": 1});
        bound_batch_size("no-such-kind", &mut unknown, 20);
        assert_eq!(unknown, serde_json::json!({"a": 1}));
        let mut scalar = serde_json::json!(3);
        bound_batch_size("elasticsearch", &mut scalar, 20);
        assert_eq!(scalar, serde_json::json!(3));
    }

    #[cfg(feature = "masking")]
    #[test]
    fn mask_applies_every_rule_and_passes_through_without_a_policy() {
        let spec: faucet_core::MaskingSpec = serde_json::from_value(serde_json::json!({
            "rules": [{ "name": "e", "match": { "fields": ["email"] },
                        "action": { "type": "redact" }, "applies_to": ["nowhere"] }]
        }))
        .unwrap();
        let rows = vec![serde_json::json!({"email": "a@b.co", "id": 1})];
        let masked = mask(rows.clone(), Some(&spec)).unwrap();
        assert_eq!(masked[0]["email"], "***");
        assert_eq!(mask(rows.clone(), None).unwrap(), rows);
    }
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
    async fn a_probe_reads_only_the_first_page() {
        let (src, pages) = endless(None, false);
        let first = probe_first_record(&src, "probe", PROBE_TIMEOUT, None)
            .await
            .unwrap();
        assert_eq!(first, Some(json!({ "i": 1 })));
        assert_eq!(pages.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_probe_stops_at_its_deadline_or_on_cancel() {
        let (src, _) = endless(Some(0), false);
        let err = probe_first_record(&src, "probe", Duration::from_millis(20), None)
            .await
            .unwrap_err();
        assert!(matches!(err, FaucetError::Timeout { .. }), "{err}");
        let token = faucet_core::CancellationToken::new();
        token.cancel();
        let (src, _) = endless(Some(0), false);
        let err = probe_first_record(&src, "probe", PROBE_TIMEOUT, Some(&token))
            .await
            .unwrap_err();
        assert!(matches!(err, FaucetError::Cancelled { .. }), "{err}");
        let (src, _) = endless(None, true);
        assert!(
            probe_first_record(&src, "probe", PROBE_TIMEOUT, None)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn an_empty_probe_returns_none() {
        struct Empty;
        #[faucet_core::async_trait]
        impl Source for Empty {
            async fn fetch_with_context(
                &self,
                _: &HashMap<String, Value>,
            ) -> Result<Vec<Value>, FaucetError> {
                Ok(Vec::new())
            }
        }
        assert_eq!(
            probe_first_record(&Empty, "probe", PROBE_TIMEOUT, None)
                .await
                .unwrap(),
            None
        );
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
        let err = probe_first_record(&queue, "a probe", PROBE_TIMEOUT, None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("removes messages"), "{err}");
        assert_eq!(reads(), 0, "a refused probe must not read");
        queue.fetch_all().await.unwrap();
        assert_eq!(reads(), 1);
    }

    /// A source whose read has side effects (a replication slot, a consumer
    /// group, a listener) is refused before any read unless the caller allows
    /// it.
    #[tokio::test]
    async fn a_source_whose_read_has_side_effects_needs_an_explicit_allow() {
        struct Cdc(std::sync::atomic::AtomicUsize);
        #[faucet_core::async_trait]
        impl Source for Cdc {
            async fn fetch_with_context(
                &self,
                _: &HashMap<String, Value>,
            ) -> Result<Vec<Value>, FaucetError> {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(vec![serde_json::json!({"id": 1})])
            }
            fn capabilities(&self) -> faucet_core::Capabilities {
                faucet_core::Capabilities::of_source(self)
                    .to_builder()
                    .read_side_effects(true)
                    .build()
            }
            fn connector_name(&self) -> &'static str {
                "postgres-cdc"
            }
        }
        let cdc = Cdc(Default::default());
        let err = sample(&cdc, &[], 10, PREVIEW_TIMEOUT).await.unwrap_err();
        assert!(
            err.to_string().contains("`postgres-cdc` source")
                && err.to_string().contains("--allow-side-effects"),
            "{err}"
        );
        assert_eq!(cdc.0.load(std::sync::atomic::Ordering::SeqCst), 0);
        let read = sample_with(&cdc, &[], 10, PREVIEW_TIMEOUT, true)
            .await
            .unwrap();
        assert_eq!(read.records.len(), 1);
    }
}
