//! The Kinesis `Source` implementation: shard discovery, bounded-concurrency
//! per-shard workers, page assembly with cumulative per-shard bookmarks, and
//! idle / max-messages termination.

use crate::config::KinesisSourceConfig;
use crate::shard::{BehindLatest, ShardEvent, probe_behind, run_shard};
use crate::state::{ShardBookmarks, state_key};
use aws_sdk_kinesis::Client;
use faucet_core::{FaucetError, Stream, StreamPage};
use serde_json::Value;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

/// AWS Kinesis Data Streams source. See the crate README for semantics.
pub struct KinesisSource {
    config: KinesisSourceConfig,
    client: Client,
    /// Bookmark applied by the pipeline before streaming (resume position).
    start_bookmarks: Mutex<Option<ShardBookmarks>>,
    /// Each shard's latest `MillisBehindLatest` this run (#733).
    behind: BehindLatest,
}

/// One discovered shard eligible for consumption.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EligibleShard {
    pub id: String,
    pub closed: bool,
}

/// Filter discovered shards per the config: explicit allowlist, and closed
/// shards only when `include_closed`. Pure.
pub(crate) fn filter_shards(
    shards: Vec<EligibleShard>,
    allowlist: &[String],
    include_closed: bool,
) -> Vec<EligibleShard> {
    shards
        .into_iter()
        .filter(|s| allowlist.is_empty() || allowlist.iter().any(|a| a == &s.id))
        .filter(|s| include_closed || !s.closed)
        .collect()
}

impl KinesisSource {
    /// Create a new Kinesis source. Validates the config and builds the AWS
    /// client; shard discovery happens lazily at stream time (construction is
    /// offline).
    pub async fn new(config: KinesisSourceConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let client = faucet_common_kinesis::build_client(
            config.region.as_deref(),
            config.endpoint_url.as_deref(),
            &config.credentials,
        )
        .await?;
        Ok(Self {
            config,
            client,
            start_bookmarks: Mutex::new(None),
            behind: BehindLatest::default(),
        })
    }

    /// Enumerate the stream's shards (paged `ListShards`), applying the
    /// config's allowlist / closed-shard filters.
    async fn discover_shards(&self) -> Result<Vec<EligibleShard>, FaucetError> {
        let mut all = Vec::new();
        let mut next_token: Option<String> = None;
        loop {
            let mut req = self.client.list_shards();
            // ListShards takes either the stream name or a pagination token,
            // never both.
            req = match &next_token {
                Some(token) => req.next_token(token),
                None => req.stream_name(&self.config.stream_name),
            };
            let out = req.send().await.map_err(|e| {
                FaucetError::Source(format!(
                    "kinesis: ListShards for stream '{}' failed: {}",
                    self.config.stream_name,
                    e.into_service_error()
                ))
            })?;
            for s in out.shards() {
                let closed = s
                    .sequence_number_range()
                    .and_then(|r| r.ending_sequence_number())
                    .is_some();
                all.push(EligibleShard {
                    id: s.shard_id().to_string(),
                    closed,
                });
            }
            next_token = out.next_token().map(str::to_string);
            if next_token.is_none() {
                break;
            }
        }
        let eligible = filter_shards(all, &self.config.shard_ids, self.config.include_closed);
        if eligible.is_empty() {
            return Err(FaucetError::Source(format!(
                "kinesis: stream '{}' has no eligible shards (shard_ids filter: {:?}, \
                 include_closed: {})",
                self.config.stream_name, self.config.shard_ids, self.config.include_closed
            )));
        }
        Ok(eligible)
    }
}

#[faucet_core::async_trait]
impl faucet_core::Source for KinesisSource {
    /// Drain the stream to termination (`idle_termination_secs` /
    /// `max_messages` — at least one is enforced at construction) and return
    /// every record.
    async fn fetch_with_context(
        &self,
        context: &std::collections::HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        use futures::StreamExt;
        let mut pages = self.stream_pages(context, self.config.batch_size);
        let mut all = Vec::new();
        while let Some(page) = pages.next().await {
            all.extend(page?.records);
        }
        Ok(all)
    }

    fn stream_pages<'a>(
        &'a self,
        _context: &'a std::collections::HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let batch_size = self.config.batch_size;
        let chunk = if batch_size == 0 {
            usize::MAX
        } else {
            batch_size
        };

        Box::pin(async_stream::try_stream! {
            let shards = self.discover_shards().await?;
            let bookmarks = self
                .start_bookmarks
                .lock()
                .expect("bookmark mutex poisoned")
                .clone()
                .unwrap_or_default();

            // Workers push decoded chunks; the semaphore bounds concurrency.
            let (tx, mut rx) = tokio::sync::mpsc::channel::<ShardEvent>(16);
            let semaphore = Arc::new(tokio::sync::Semaphore::new(
                self.config.shard_concurrency,
            ));
            let total_shards = shards.len();
            let mut handles = Vec::with_capacity(total_shards);
            for shard in &shards {
                let permit_sem = semaphore.clone();
                let client = self.client.clone();
                let config = self.config.clone();
                let shard_id = shard.id.clone();
                let bookmark = bookmarks.get(&shard_id).map(str::to_string);
                let tx = tx.clone();
                let behind = Arc::clone(&self.behind);
                handles.push(tokio::spawn(async move {
                    let _permit = permit_sem
                        .acquire_owned()
                        .await
                        .expect("semaphore closed");
                    run_shard(client, config, shard_id, bookmark, tx, behind).await;
                }));
            }
            drop(tx); // the channel closes when every worker exits

            let idle = self
                .config
                .idle_termination_secs
                .map(Duration::from_secs);
            let max_messages = self.config.max_messages;
            let mut cumulative = bookmarks;
            let mut buffer: Vec<Value> = Vec::new();
            let mut total = 0usize;
            let mut done_shards = 0usize;
            let mut failure: Option<FaucetError> = None;

            'consume: loop {
                let event = match idle {
                    Some(window) => match tokio::time::timeout(window, rx.recv()).await {
                        Ok(ev) => ev,
                        Err(_) => {
                            tracing::info!(
                                stream = %self.config.stream_name,
                                idle_secs = window.as_secs(),
                                "kinesis: idle termination reached"
                            );
                            break 'consume;
                        }
                    },
                    None => rx.recv().await,
                };
                match event {
                    Some(ShardEvent::Records { shard_id, records }) => {
                        for (sequence, record) in records {
                            buffer.push(record);
                            total += 1;
                            // Advance the shard bookmark to THIS record's sequence
                            // before any page emit, so a yielded page's bookmark
                            // never runs ahead of the records it actually carries
                            // (audit #321 C2 — the old code jumped to the batch's
                            // last sequence up front, so a mid-batch page emit
                            // persisted a bookmark past un-emitted records → silent
                            // loss on resume).
                            cumulative.advance(&shard_id, &sequence);
                            if buffer.len() >= chunk {
                                let page = std::mem::take(&mut buffer);
                                yield StreamPage {
                                    records: page,
                                    bookmark: Some(cumulative.to_value()),
                                };
                            }
                            if let Some(max) = max_messages
                                && total >= max
                            {
                                tracing::info!(
                                    stream = %self.config.stream_name,
                                    max,
                                    "kinesis: max_messages reached"
                                );
                                break 'consume;
                            }
                        }
                    }
                    Some(ShardEvent::Done { shard_id }) => {
                        done_shards += 1;
                        tracing::debug!(shard = %shard_id, done_shards, total_shards,
                            "kinesis: shard fully consumed");
                    }
                    Some(ShardEvent::Failed { shard_id, error }) => {
                        tracing::error!(shard = %shard_id, error = %error,
                            "kinesis: shard worker failed");
                        failure = Some(error);
                        break 'consume;
                    }
                    None => break 'consume, // every worker exited
                }
            }

            // Stop the workers (drops their send side) and surface results.
            rx.close();
            for h in handles {
                h.abort();
            }
            if let Some(error) = failure {
                Err(error)?;
            }
            if !buffer.is_empty() || !cumulative.shards.is_empty() {
                yield StreamPage {
                    records: buffer,
                    bookmark: Some(cumulative.to_value()),
                };
            }
            tracing::info!(
                stream = %self.config.stream_name,
                records = total,
                shards = total_shards,
                "kinesis source stream complete"
            );
        })
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(KinesisSourceConfig))
            .expect("schema serialization")
    }

    fn state_key(&self) -> Option<String> {
        Some(state_key(&self.config.stream_name))
    }

    /// How far the furthest-behind shard trails the stream's tip (#733):
    /// `MillisBehindLatest` from this run's reads, or — before any — one
    /// probing read per shard from the resume position.
    async fn lag(&self) -> Result<Option<faucet_core::SourceLag>, FaucetError> {
        let seen = self
            .behind
            .lock()
            .ok()
            .and_then(|m| m.values().copied().max());
        let millis = match seen {
            Some(ms) => Some(ms),
            None => {
                let bookmarks = self
                    .start_bookmarks
                    .lock()
                    .expect("bookmark mutex poisoned")
                    .clone()
                    .unwrap_or_default();
                let mut worst: Option<i64> = None;
                for shard in self.discover_shards().await? {
                    if let Some(ms) = probe_behind(
                        &self.client,
                        &self.config,
                        &shard.id,
                        bookmarks.get(&shard.id),
                    )
                    .await?
                    {
                        worst = Some(worst.map_or(ms, |w| w.max(ms)));
                    }
                }
                worst
            }
        };
        Ok(millis.map(|ms| faucet_core::SourceLag::seconds(ms.max(0) as f64 / 1000.0)))
    }

    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        *self
            .start_bookmarks
            .lock()
            .expect("bookmark mutex poisoned") = Some(ShardBookmarks::from_value(&bookmark));
        Ok(())
    }

    fn connector_name(&self) -> &'static str {
        "kinesis"
    }

    fn dataset_uri(&self) -> String {
        format!(
            "kinesis://{}/{}",
            self.config.region.as_deref().unwrap_or("default"),
            self.config.stream_name
        )
    }

    /// Side-effect-free probe: `DescribeStreamSummary` (no records consumed,
    /// no iterator opened). The default first-page probe could block for the
    /// full idle window on a quiet stream.
    async fn check(
        &self,
        ctx: &faucet_core::CheckContext,
    ) -> Result<faucet_core::CheckReport, FaucetError> {
        use faucet_core::{CheckReport, Probe};
        let start = std::time::Instant::now();
        let fut = self
            .client
            .describe_stream_summary()
            .stream_name(&self.config.stream_name)
            .send();
        let probe = match tokio::time::timeout(ctx.timeout, fut).await {
            Err(_) => Probe::fail("describe_stream", start.elapsed(), "timed out"),
            Ok(Ok(_)) => Probe::pass("describe_stream", start.elapsed()),
            Ok(Err(e)) => Probe::fail(
                "describe_stream",
                start.elapsed(),
                e.into_service_error().to_string(),
            ),
        };
        Ok(CheckReport::single(probe))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::Source as _;

    fn shard(id: &str, closed: bool) -> EligibleShard {
        EligibleShard {
            id: id.to_string(),
            closed,
        }
    }

    #[test]
    fn shard_filtering_applies_allowlist_and_closed_rules() {
        let all = vec![
            shard("shardId-000000000000", false),
            shard("shardId-000000000001", true),
            shard("shardId-000000000002", false),
        ];
        // Default: open shards only.
        let open = filter_shards(all.clone(), &[], false);
        assert_eq!(open.len(), 2);
        assert!(open.iter().all(|s| !s.closed));
        // include_closed keeps everything.
        assert_eq!(filter_shards(all.clone(), &[], true).len(), 3);
        // Allowlist narrows.
        let picked = filter_shards(all, &["shardId-000000000001".to_string()], true);
        assert_eq!(picked.len(), 1);
        assert_eq!(picked[0].id, "shardId-000000000001");
    }

    async fn offline_source(mut config: KinesisSourceConfig) -> KinesisSource {
        config.endpoint_url = Some("http://127.0.0.1:1".into()); // unroutable
        config.region = Some("us-east-1".into());
        config.credentials = faucet_common_kinesis::KinesisCredentials::AccessKey {
            access_key_id: "test".into(),
            secret_access_key: "test".into(),
            session_token: None,
        };
        KinesisSource::new(config).await.expect("source builds")
    }

    #[tokio::test]
    async fn new_validates_config() {
        // No termination knob → rejected.
        let err = match KinesisSource::new(KinesisSourceConfig::new("events")).await {
            Err(e) => e,
            Ok(_) => panic!("config without a termination knob must be rejected"),
        };
        assert!(err.to_string().contains("idle_termination_secs"), "{err}");
    }

    #[tokio::test]
    async fn identity_overrides() {
        let mut cfg = KinesisSourceConfig::new("events");
        cfg.max_messages = Some(10);
        let source = offline_source(cfg).await;
        assert_eq!(source.connector_name(), "kinesis");
        assert_eq!(source.dataset_uri(), "kinesis://us-east-1/events");
        assert_eq!(source.state_key().as_deref(), Some("kinesis:events"));
        assert!(!source.supports_exactly_once());
        let schema = source.config_schema();
        assert!(
            schema["properties"]["stream_name"].is_object(),
            "schema exposes config fields"
        );
    }

    #[tokio::test]
    async fn apply_start_bookmark_round_trips() {
        let mut cfg = KinesisSourceConfig::new("events");
        cfg.max_messages = Some(10);
        let source = offline_source(cfg).await;
        source
            .apply_start_bookmark(serde_json::json!({
                "shards": {"shardId-000000000000": "42"}
            }))
            .await
            .unwrap();
        let held = source.start_bookmarks.lock().unwrap().clone().unwrap();
        assert_eq!(held.get("shardId-000000000000"), Some("42"));
    }

    #[tokio::test]
    async fn stream_pages_surfaces_discovery_errors() {
        use futures::StreamExt;
        let mut cfg = KinesisSourceConfig::new("events");
        cfg.max_messages = Some(10);
        let source = offline_source(cfg).await;
        let ctx = std::collections::HashMap::new();
        let mut pages = source.stream_pages(&ctx, 10);
        let first = pages.next().await.expect("one item");
        let err = first.unwrap_err();
        assert!(err.to_string().contains("ListShards"), "{err}");
    }

    #[tokio::test]
    async fn check_probe_fails_cleanly_offline() {
        let mut cfg = KinesisSourceConfig::new("events");
        cfg.max_messages = Some(10);
        let source = offline_source(cfg).await;
        let report = source
            .check(&faucet_core::CheckContext {
                timeout: Duration::from_millis(500),
            })
            .await
            .unwrap();
        assert_eq!(
            report.failed_count(),
            1,
            "unreachable endpoint → fail probe"
        );
    }
}
