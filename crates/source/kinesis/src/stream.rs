//! The Kinesis `Source` implementation: shard discovery, bounded-concurrency
//! per-shard workers, page assembly with cumulative per-shard bookmarks, and
//! idle / max-messages termination.

use crate::config::KinesisSourceConfig;
use crate::config::StartPosition;
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
    /// `ParentShardId` and `AdjacentParentShardId` from `ListShards`.
    pub parents: Vec<String>,
}

/// A shard this run reads, and how.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlannedShard {
    pub id: String,
    /// Planned parents that must be drained before this shard starts, so a
    /// key's older records (in the parent) land before its newer ones.
    pub wait_for: Vec<String>,
    /// Start an unbookmarked shard at `TRIM_HORIZON` instead of the
    /// configured start position: its parent was read, so everything the
    /// child holds is newer than what was read and must not be skipped.
    pub from_trim_horizon: bool,
}

/// Decide which shards to read. Open shards are always read (subject to the
/// allowlist). A closed shard is read when `include_closed` is set or it has a
/// bookmark — after a reshard the parent's unread tail must be drained, never
/// dropped; a fully read one ends at its first `GetRecords`. A shard waits for its
/// planned parents, and an unbookmarked child of a read parent starts at
/// `TRIM_HORIZON`. Pure.
pub(crate) fn plan_shards(
    shards: Vec<EligibleShard>,
    allowlist: &[String],
    include_closed: bool,
    bookmarks: &ShardBookmarks,
) -> Vec<PlannedShard> {
    let selected: Vec<EligibleShard> = shards
        .into_iter()
        .filter(|s| allowlist.is_empty() || allowlist.iter().any(|a| a == &s.id))
        .filter(|s| !s.closed || include_closed || bookmarks.get(&s.id).is_some())
        .collect();
    let planned: std::collections::HashSet<String> =
        selected.iter().map(|s| s.id.clone()).collect();
    selected
        .into_iter()
        .map(|s| {
            let wait_for: Vec<String> = s
                .parents
                .iter()
                .filter(|p| planned.contains(*p))
                .cloned()
                .collect();
            let parent_read = s
                .parents
                .iter()
                .any(|p| planned.contains(p) || bookmarks.get(p).is_some());
            PlannedShard {
                from_trim_horizon: bookmarks.get(&s.id).is_none() && parent_read,
                id: s.id,
                wait_for,
            }
        })
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

    /// Enumerate the stream's shards (paged `ListShards`) and plan which to
    /// read and in what order ([`plan_shards`]).
    async fn discover_shards(
        &self,
        bookmarks: &ShardBookmarks,
    ) -> Result<Vec<PlannedShard>, FaucetError> {
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
                all.push(EligibleShard {
                    id: s.shard_id().to_string(),
                    closed: s
                        .sequence_number_range()
                        .and_then(|r| r.ending_sequence_number())
                        .is_some(),
                    parents: s
                        .parent_shard_id()
                        .into_iter()
                        .chain(s.adjacent_parent_shard_id())
                        .map(str::to_string)
                        .collect(),
                });
            }
            next_token = out.next_token().map(str::to_string);
            if next_token.is_none() {
                break;
            }
        }
        let eligible = plan_shards(
            all,
            &self.config.shard_ids,
            self.config.include_closed,
            bookmarks,
        );
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
            let bookmarks = self
                .start_bookmarks
                .lock()
                .expect("bookmark mutex poisoned")
                .clone()
                .unwrap_or_default();
            let shards = self.discover_shards(&bookmarks).await?;

            // Every shard gets a worker; the semaphore bounds concurrent
            // `GetRecords` calls, not shard lifetimes, so every shard is read.
            let (tx, mut rx) = tokio::sync::mpsc::channel::<ShardEvent>(16);
            let semaphore = Arc::new(tokio::sync::Semaphore::new(
                self.config.shard_concurrency,
            ));
            let drained: std::collections::HashMap<String, tokio::sync::watch::Sender<bool>> =
                shards
                    .iter()
                    .map(|s| (s.id.clone(), tokio::sync::watch::channel(false).0))
                    .collect();
            let total_shards = shards.len();
            let mut handles = Vec::with_capacity(total_shards);
            for shard in &shards {
                let permits = semaphore.clone();
                let client = self.client.clone();
                let mut config = self.config.clone();
                if shard.from_trim_horizon {
                    config.start_position = StartPosition::TrimHorizon;
                }
                let shard_id = shard.id.clone();
                let bookmark = bookmarks.get(&shard_id).map(str::to_string);
                let tx = tx.clone();
                let behind = Arc::clone(&self.behind);
                let parents: Vec<tokio::sync::watch::Receiver<bool>> = shard
                    .wait_for
                    .iter()
                    .filter_map(|p| drained.get(p).map(|s| s.subscribe()))
                    .collect();
                let done = drained.get(&shard_id).cloned();
                handles.push(tokio::spawn(async move {
                    for mut parent in parents {
                        if parent.wait_for(|d| *d).await.is_err() {
                            return;
                        }
                    }
                    let finished =
                        run_shard(client, config, shard_id, bookmark, tx, behind, permits).await;
                    if finished && let Some(done) = done {
                        let _ = done.send(true);
                    }
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
                for shard in self.discover_shards(&bookmarks).await? {
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
            parents: Vec::new(),
        }
    }

    fn child(id: &str, parents: &[&str]) -> EligibleShard {
        EligibleShard {
            parents: parents.iter().map(|p| p.to_string()).collect(),
            ..shard(id, false)
        }
    }

    fn ids(plan: &[PlannedShard]) -> Vec<&str> {
        plan.iter().map(|s| s.id.as_str()).collect()
    }

    fn marks(pairs: &[(&str, &str)]) -> ShardBookmarks {
        let mut b = ShardBookmarks::default();
        for (id, seq) in pairs {
            b.advance(id, seq);
        }
        b
    }

    #[test]
    fn shard_planning_applies_allowlist_and_closed_rules() {
        let all = vec![shard("s0", false), shard("s1", true), shard("s2", false)];
        let none = ShardBookmarks::default();
        assert_eq!(
            ids(&plan_shards(all.clone(), &[], false, &none)),
            ["s0", "s2"]
        );
        assert_eq!(plan_shards(all.clone(), &[], true, &none).len(), 3);
        let picked = plan_shards(all, &["s1".to_string()], true, &none);
        assert_eq!(ids(&picked), ["s1"]);
    }

    #[test]
    fn a_closed_parent_with_an_unread_tail_is_drained_before_its_children() {
        let all = vec![
            shard("parent", true),
            child("left", &["parent"]),
            child("right", &["parent"]),
        ];
        let plan = plan_shards(all.clone(), &[], false, &marks(&[("parent", "500")]));
        assert_eq!(ids(&plan), ["parent", "left", "right"]);
        assert!(plan[0].wait_for.is_empty());
        assert_eq!(plan[1].wait_for, ["parent"]);
        assert!(
            plan[1].from_trim_horizon,
            "the child holds only newer records"
        );

        let gone = plan_shards(
            vec![child("left", &["parent"])],
            &[],
            false,
            &marks(&[("parent", "900")]),
        );
        assert!(
            gone[0].wait_for.is_empty(),
            "an expired parent is not waited on"
        );
        assert!(gone[0].from_trim_horizon);
    }

    #[test]
    fn a_merged_child_waits_for_both_parents_and_keeps_its_own_bookmark() {
        let all = vec![
            shard("a", true),
            shard("b", true),
            child("merged", &["a", "b"]),
        ];
        let plan = plan_shards(all.clone(), &[], true, &ShardBookmarks::default());
        assert_eq!(plan[2].wait_for, ["a", "b"]);
        assert!(plan[2].from_trim_horizon);

        let resumed = plan_shards(all, &[], false, &marks(&[("a", "900"), ("merged", "7")]));
        assert_eq!(ids(&resumed), ["a", "merged"]);
        assert_eq!(resumed[1].wait_for, ["a"]);
        assert!(
            !resumed[0].from_trim_horizon,
            "a bookmarked shard resumes after it"
        );
    }

    #[test]
    fn an_unrelated_child_keeps_the_configured_start_position() {
        let plan = plan_shards(
            vec![child("c", &["gone"])],
            &[],
            false,
            &ShardBookmarks::default(),
        );
        assert!(plan[0].wait_for.is_empty());
        assert!(!plan[0].from_trim_horizon);
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
