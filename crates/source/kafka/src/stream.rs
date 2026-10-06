//! `KafkaSource` — the Kafka consumer implementation.

use crate::config::KafkaSourceConfig;
use crate::context::BookmarkContext;
use crate::decode;
use crate::shard::{MemberShard, commit_list, parse_member_shard, plan_member_shards};
use crate::state::{Bookmark, PartitionOffset, state_key};
use async_trait::async_trait;
use base64::Engine;
use faucet_common_kafka::OnDecodeError;
use faucet_core::shard::ShardSpec;
use faucet_core::{FaucetError, Source, Stream, StreamPage};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Headers;
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use serde_json::{Map, Value, json};
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

#[cfg(feature = "schema-registry")]
use faucet_common_kafka::KafkaValueFormat;
#[cfg(feature = "schema-registry")]
use faucet_common_kafka::schema_registry::client::SchemaRegistryClient;

pub struct KafkaSource {
    config: KafkaSourceConfig,
    consumer: Arc<StreamConsumer<BookmarkContext>>,
    context: BookmarkContext,
    state_key_value: String,
    /// Cache of watermark-derived floor offsets for assigned partitions that
    /// have produced no message (so `position()` reports no concrete offset).
    /// Resolved once per partition and reused so the streaming bookmark build
    /// doesn't issue a broker `fetch_watermarks` every page (#146 H9).
    assigned_floor: std::sync::Mutex<HashMap<(String, i32), i64>>,
    /// The group-member shard applied by a cluster coordinator (Mode B, #261),
    /// or `None` for a plain single-consumer run. `Mutex` so
    /// `apply_shard(&self, …)` can record it before streaming.
    member_shard: std::sync::Mutex<Option<MemberShard>>,
    /// The offsets last handed to the pipeline on a page bookmark — where
    /// [`Source::lag`] measures from (#733).
    emitted: std::sync::Mutex<Option<Bookmark>>,
    #[cfg(feature = "schema-registry")]
    sr_client: Option<SchemaRegistryClient>,
}

/// Messages left in one partition: from the next offset to read (or, with no
/// position, where `auto_offset_reset` starts) up to the high watermark. A
/// position below the low watermark has been truncated away, so it counts
/// from the low watermark.
fn partition_lag(low: i64, high: i64, position: Option<i64>, earliest: bool) -> u64 {
    let from = match position {
        Some(p) => p.max(low),
        None if earliest => low,
        None => high,
    };
    high.saturating_sub(from).max(0) as u64
}

impl KafkaSource {
    pub async fn new(config: KafkaSourceConfig) -> Result<Self, FaucetError> {
        config.validate()?;

        let mut client_config = ClientConfig::new();
        client_config.set("bootstrap.servers", &config.brokers);
        client_config.set("group.id", &config.group_id);
        client_config.set("enable.auto.commit", "false");
        client_config.set("auto.offset.reset", config.auto_offset_reset.as_str());
        client_config.set(
            "session.timeout.ms",
            config.session_timeout.as_millis().to_string(),
        );
        client_config.set_log_level(RDKafkaLogLevel::Warning);

        config.auth.apply(&mut client_config)?;

        for (k, v) in &config.extra_client_config {
            client_config.set(k, v);
        }

        let context = BookmarkContext::new();
        let consumer: StreamConsumer<BookmarkContext> = client_config
            .create_with_context(context.clone())
            .map_err(|e| FaucetError::Source(format!("kafka consumer init: {e}")))?;

        let topic_refs: Vec<&str> = config.topics.iter().map(String::as_str).collect();
        consumer
            .subscribe(&topic_refs)
            .map_err(|e| FaucetError::Source(format!("kafka subscribe: {e}")))?;

        let state_key_value = state_key(&config.group_id, &config.topics);

        #[cfg(feature = "schema-registry")]
        let sr_client = build_sr_client(&config.value_format, config.key_format.as_ref())?;

        Ok(Self {
            config,
            consumer: Arc::new(consumer),
            context,
            state_key_value,
            assigned_floor: std::sync::Mutex::new(HashMap::new()),
            member_shard: std::sync::Mutex::new(None),
            emitted: std::sync::Mutex::new(None),
            #[cfg(feature = "schema-registry")]
            sr_client,
        })
    }

    /// Drain the callback-error slot. Called once per poll iteration so
    /// rebalance-callback failures surface before any further messages are
    /// processed (matches the batch-mode invariant).
    fn check_callback_error(&self) -> Result<(), FaucetError> {
        let mut guard =
            self.context.callback_error.lock().map_err(|e| {
                FaucetError::State(format!("kafka callback_error mutex poisoned: {e}"))
            })?;
        if let Some(e) = guard.take() {
            return Err(e);
        }
        Ok(())
    }

    /// Resolve a starting offset for **every assigned partition**, so the
    /// persisted bookmark records partitions that produced no message this run
    /// — otherwise such a partition is absent on resume and resets to
    /// `auto.offset.reset` (default `latest`), silently skipping any records
    /// that arrived in the meantime (the H9 fix).
    ///
    /// For a partition that delivered messages, `position()` reports a concrete
    /// next-offset (cheap, local). For one that produced nothing, `position()`
    /// reports `Offset::Invalid` (librdkafka only tracks *consumed* offsets), so
    /// we fall back to its watermark via `fetch_watermarks`: the **low**
    /// watermark under `earliest`, the **high** watermark under `latest` — i.e.
    /// exactly where the consumer is positioned for that reset policy. Those
    /// watermark lookups are a broker round-trip, so each empty partition's
    /// floor is resolved once and cached.
    ///
    /// Returns `(positions, floors)`: concrete consumer positions, and the
    /// watermark floors of partitions without one, which rank below the prior
    /// bookmark in [`Bookmark::merged_with_floors`].
    async fn resolve_assigned_offsets(&self) -> (Vec<PartitionOffset>, Vec<PartitionOffset>) {
        let assigned = match self.consumer.assignment() {
            Ok(tpl) => tpl,
            Err(e) => {
                tracing::warn!(error = %e, "kafka source: assignment() failed; bookmark falls back to consumed/carry-forward offsets");
                return (Vec::new(), Vec::new());
            }
        };
        let positions: HashMap<(String, i32), rdkafka::Offset> = self
            .consumer
            .position()
            .map(|tpl| {
                tpl.elements()
                    .iter()
                    .map(|e| ((e.topic().to_string(), e.partition()), e.offset()))
                    .collect()
            })
            .unwrap_or_default();

        let mut out: Vec<PartitionOffset> = Vec::new();
        let mut floors: Vec<PartitionOffset> = Vec::new();
        let mut need_watermark: Vec<(String, i32)> = Vec::new();
        {
            let cache = self.assigned_floor.lock().ok();
            for elem in assigned.elements() {
                let key = (elem.topic().to_string(), elem.partition());
                match positions.get(&key) {
                    // A delivered partition: its concrete next-offset.
                    Some(rdkafka::Offset::Offset(n)) => out.push(PartitionOffset {
                        topic: key.0,
                        partition: key.1,
                        offset: *n,
                    }),
                    // No concrete position → use a cached watermark floor, or
                    // schedule a lookup for it.
                    _ => match cache.as_ref().and_then(|c| c.get(&key)) {
                        Some(&floor) => floors.push(PartitionOffset {
                            topic: key.0,
                            partition: key.1,
                            offset: floor,
                        }),
                        None => need_watermark.push(key),
                    },
                }
            }
        }

        if !need_watermark.is_empty() {
            let earliest = matches!(
                self.config.auto_offset_reset,
                crate::config::OffsetReset::Earliest
            );
            let consumer = Arc::clone(&self.consumer);
            let to_fetch = need_watermark.clone();
            // `fetch_watermarks` is a blocking librdkafka broker call — run it
            // off the async runtime.
            let resolved = tokio::task::spawn_blocking(move || {
                to_fetch
                    .into_iter()
                    .filter_map(|(topic, partition)| {
                        consumer
                            .fetch_watermarks(&topic, partition, Duration::from_secs(5))
                            .ok()
                            .map(|(low, high)| {
                                (topic, partition, if earliest { low } else { high })
                            })
                    })
                    .collect::<Vec<_>>()
            })
            .await
            .unwrap_or_default();

            if let Ok(mut cache) = self.assigned_floor.lock() {
                for (topic, partition, floor) in resolved {
                    cache.insert((topic.clone(), partition), floor);
                    floors.push(PartitionOffset {
                        topic,
                        partition,
                        offset: floor,
                    });
                }
            }
        }
        (out, floors)
    }

    /// The start bookmark applied via [`apply_start_bookmark`], retained for
    /// carry-forward (cloned, not consumed). `None` on a fresh run.
    fn note_emitted(&self, bookmark: Option<&Value>) {
        if let Some(b) = bookmark.and_then(|v| Bookmark::from_value(v.clone()).ok())
            && let Ok(mut g) = self.emitted.lock()
        {
            *g = Some(b);
        }
    }

    fn start_bookmark(&self) -> Option<Bookmark> {
        self.context
            .start_offsets
            .lock()
            .ok()
            .and_then(|g| g.clone())
    }

    /// Build the bookmark to persist from the offsets consumed this page/run,
    /// merging in the assigned-partition offsets and the carry-forward start
    /// bookmark. Returns `None` only when there is nothing at all to record (no
    /// partition assigned, nothing consumed, no prior state).
    async fn build_bookmark(
        &self,
        consumed: &HashMap<(String, i32), i64>,
    ) -> Result<Option<Value>, FaucetError> {
        let (positions, floors) = self.resolve_assigned_offsets().await;
        let merged = Bookmark::merged_with_floors(
            self.start_bookmark().as_ref(),
            &floors,
            &positions,
            consumed,
        );
        if merged.partition_offsets.is_empty() {
            Ok(None)
        } else {
            Ok(Some(merged.to_value()?))
        }
    }

    /// Whether a cluster coordinator applied a group-member shard (Mode B,
    /// #261) — i.e. this consumer is one of N cooperating members of the group.
    fn member_mode(&self) -> bool {
        self.context.member_mode.load(Ordering::Acquire)
    }

    /// Group-member mode only: commit `offsets` (the cumulative
    /// `(topic, partition) -> next_offset` map of pages that are already
    /// durable — written to the sink and bookmark-persisted) to the consumer
    /// group, so a partition that migrates to another member on rebalance
    /// resumes from the last durable position instead of `auto.offset.reset`.
    ///
    /// Only currently-assigned partitions are committed — committing a stale
    /// offset for a partition that migrated away mid-page would regress the
    /// new owner's commit. Best-effort by design (warn on error): offsets are
    /// cumulative, so a failed commit is retried at the next durable boundary
    /// and the cost of a lost commit is a bounded re-read (at-least-once),
    /// never data loss.
    ///
    /// Mid-run commits are `CommitMode::Async` (no broker round-trip on the
    /// hot path). The `terminal` end-of-stream commit is synchronous (off the
    /// async runtime) so the group's next member sees the final position even
    /// when the process exits right after the run — the run-end handoff case.
    async fn commit_durable(&self, offsets: &HashMap<(String, i32), i64>, terminal: bool) {
        if !self.member_mode() || offsets.is_empty() {
            return;
        }
        let assigned: HashSet<(String, i32)> = match self.consumer.assignment() {
            Ok(tpl) => tpl
                .elements()
                .iter()
                .map(|e| (e.topic().to_string(), e.partition()))
                .collect(),
            Err(e) => {
                tracing::warn!(error = %e, "kafka member mode: assignment() failed; skipping offset commit");
                return;
            }
        };
        let list = commit_list(offsets, &assigned);
        if list.is_empty() {
            return;
        }
        let mut tpl = TopicPartitionList::with_capacity(list.len());
        for (topic, partition, offset) in &list {
            if let Err(e) = tpl.add_partition_offset(topic, *partition, Offset::Offset(*offset)) {
                tracing::warn!(
                    topic,
                    partition,
                    offset,
                    error = %e,
                    "kafka member mode: building commit list failed; skipping offset commit"
                );
                return;
            }
        }
        if terminal {
            // `commit(…, Sync)` is a blocking broker round-trip — run it off
            // the async runtime.
            let consumer = Arc::clone(&self.consumer);
            match tokio::task::spawn_blocking(move || consumer.commit(&tpl, CommitMode::Sync)).await
            {
                Ok(Ok(())) => {}
                Ok(Err(e)) => tracing::warn!(
                    error = %e,
                    "kafka member mode: terminal offset commit failed; \
                     the group's next member may re-read the tail (at-least-once)"
                ),
                Err(join_err) => tracing::warn!(
                    error = %join_err,
                    "kafka member mode: terminal offset commit task failed"
                ),
            }
        } else if let Err(e) = self.consumer.commit(&tpl, CommitMode::Async) {
            tracing::warn!(
                error = %e,
                "kafka member mode: offset commit failed (retried at the next durable boundary)"
            );
        }
    }

    /// Total partition count across the subscribed topics, used to cap the
    /// member-shard plan (a member beyond the partition count would sit idle).
    /// `None` when any topic's metadata cannot be resolved — enumeration then
    /// trusts the requested member count instead of failing the run.
    async fn total_partitions(&self) -> Option<usize> {
        let consumer = Arc::clone(&self.consumer);
        let topics = self.config.topics.clone();
        // `fetch_metadata` is a blocking librdkafka broker call — run it off
        // the async runtime.
        tokio::task::spawn_blocking(move || {
            let mut total = 0usize;
            for topic in &topics {
                match consumer.fetch_metadata(Some(topic), Duration::from_secs(5)) {
                    Ok(md) => {
                        let n: usize = md
                            .topics()
                            .iter()
                            .filter(|t| t.name() == topic)
                            .map(|t| t.partitions().len())
                            .sum();
                        if n == 0 {
                            tracing::warn!(
                                topic,
                                "kafka enumerate_shards: topic has no partitions in metadata; \
                                 not capping the member count"
                            );
                            return None;
                        }
                        total += n;
                    }
                    Err(e) => {
                        tracing::warn!(
                            topic,
                            error = %e,
                            "kafka enumerate_shards: metadata fetch failed; \
                             not capping the member count"
                        );
                        return None;
                    }
                }
            }
            Some(total)
        })
        .await
        .ok()
        .flatten()
    }

    async fn message_to_value(
        &self,
        msg: &rdkafka::message::BorrowedMessage<'_>,
    ) -> Result<Value, FaucetError> {
        let value = decode::decode(
            msg.payload(),
            &self.config.value_format,
            #[cfg(feature = "schema-registry")]
            self.sr_client.as_ref(),
        )
        .await?;

        let key = match &self.config.key_format {
            Some(fmt) => {
                decode::decode(
                    msg.key(),
                    fmt,
                    #[cfg(feature = "schema-registry")]
                    self.sr_client.as_ref(),
                )
                .await?
            }
            None => match msg.key() {
                Some(bytes) => Value::String(
                    std::str::from_utf8(bytes)
                        .map_err(|e| FaucetError::Source(format!("kafka key utf-8: {e}")))?
                        .to_string(),
                ),
                None => Value::Null,
            },
        };

        let mut headers_obj = Map::new();
        if let Some(headers) = msg.headers() {
            for h in headers.iter() {
                if let Some(value_bytes) = h.value {
                    if let Ok(s) = std::str::from_utf8(value_bytes) {
                        headers_obj.insert(h.key.to_string(), Value::String(s.to_string()));
                    } else {
                        let encoded = base64::engine::general_purpose::STANDARD.encode(value_bytes);
                        headers_obj.insert(h.key.to_string(), Value::String(encoded));
                    }
                }
            }
        }

        Ok(json!({
            "key": key,
            "value": value,
            "topic": msg.topic(),
            "partition": msg.partition(),
            "offset": msg.offset(),
            "timestamp": msg.timestamp().to_millis().unwrap_or(0),
            "headers": Value::Object(headers_obj),
        }))
    }
}

#[cfg(feature = "schema-registry")]
fn build_sr_client(
    value_format: &KafkaValueFormat,
    key_format: Option<&KafkaValueFormat>,
) -> Result<Option<SchemaRegistryClient>, FaucetError> {
    fn extract_cfg(f: &KafkaValueFormat) -> Option<&faucet_common_kafka::SchemaRegistryConfig> {
        match f {
            KafkaValueFormat::ConfluentAvro { schema_registry }
            | KafkaValueFormat::ConfluentProtobuf { schema_registry } => Some(schema_registry),
            KafkaValueFormat::ConfluentJsonSchema {
                schema_registry, ..
            } => Some(schema_registry),
            _ => None,
        }
    }
    let cfg = extract_cfg(value_format).or_else(|| key_format.and_then(extract_cfg));
    cfg.map(SchemaRegistryClient::new).transpose()
}

/// Whether a consumer error event is one librdkafka recovers from on its own
/// (a broker roll, a coordinator move, a timed-out request, a poll interval
/// overrun that the next poll rejoins from). Those are logged and the poll
/// loop continues; anything else, including every fatal error, fails the run.
pub(crate) fn is_transient_consume_error(e: &rdkafka::error::KafkaError) -> bool {
    use rdkafka::error::{KafkaError, RDKafkaErrorCode as C};
    match e {
        KafkaError::PartitionEOF(_) => true,
        KafkaError::MessageConsumption(code) => matches!(
            code,
            C::BrokerTransportFailure
                | C::Resolve
                | C::AllBrokersDown
                | C::OperationTimedOut
                | C::PollExceeded
                | C::RequestTimedOut
                | C::BrokerNotAvailable
                | C::LeaderNotAvailable
                | C::NotLeaderForPartition
                | C::NetworkException
                | C::CoordinatorLoadInProgress
                | C::CoordinatorNotAvailable
                | C::NotCoordinator
        ),
        _ => false,
    }
}

#[async_trait]
impl Source for KafkaSource {
    async fn fetch_with_context(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let (records, _bookmark) = self.fetch_with_context_incremental(context).await?;
        Ok(records)
    }

    async fn fetch_with_context_incremental(
        &self,
        _context: &HashMap<String, Value>,
    ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        let mut records: Vec<Value> = Vec::new();
        let mut pending_offsets: HashMap<(String, i32), i64> = HashMap::new();
        let mut last_message_at = Instant::now();
        let max_messages = self.config.max_messages.unwrap_or(usize::MAX);
        let idle_timeout = self.config.idle_timeout;

        loop {
            // Surface any error raised by the rebalance callback (e.g. a
            // failed seek). Done before the next poll so we don't process
            // additional messages after a bookmark application failure.
            self.check_callback_error()?;

            let idle_deadline = idle_timeout.map(|t| last_message_at + t);
            let poll_budget = match idle_deadline {
                Some(deadline) => deadline
                    .checked_duration_since(Instant::now())
                    .unwrap_or(Duration::ZERO),
                None => self.config.poll_timeout,
            };

            tokio::select! {
                biased;
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("kafka source: ctrl_c received, stopping cleanly");
                    break;
                }
                recv = tokio::time::timeout(poll_budget, self.consumer.recv()) => {
                    match recv {
                        Ok(Ok(msg)) => {
                            match self.message_to_value(&msg).await {
                                Ok(record) => {
                                    pending_offsets.insert(
                                        (msg.topic().to_string(), msg.partition()),
                                        msg.offset() + 1,
                                    );
                                    self.context.note_delivered(
                                        msg.topic(),
                                        msg.partition(),
                                        msg.offset() + 1,
                                    );
                                    records.push(record);
                                    last_message_at = Instant::now();
                                    if records.len() >= max_messages {
                                        break;
                                    }
                                }
                                Err(e) => match self.config.on_decode_error {
                                    OnDecodeError::Skip => {
                                        tracing::warn!(error = %e, "kafka source: decode failed, skipping message");
                                    }
                                    OnDecodeError::Fail => return Err(e),
                                },
                            }
                        }
                        Ok(Err(e)) if is_transient_consume_error(&e) => {
                            tracing::warn!(error = %e, "kafka source: transient consumer error, continuing");
                        }
                        Ok(Err(e)) => {
                            return Err(FaucetError::Source(format!("kafka recv: {e}")));
                        }
                        Err(_timeout) => {
                            if let Some(deadline) = idle_deadline
                                && Instant::now() >= deadline
                            {
                                tracing::debug!("kafka source: idle_timeout reached, stopping");
                                break;
                            }
                        }
                    }
                }
            }
        }

        let bookmark_value = self.build_bookmark(&pending_offsets).await?;
        self.note_emitted(bookmark_value.as_ref());
        Ok((records, bookmark_value))
    }

    /// Stream Kafka messages page-by-page. Mirrors the
    /// [`fetch_with_context_incremental`](Self::fetch_with_context_incremental)
    /// poll loop but emits a [`StreamPage`] each time the in-memory buffer
    /// reaches [`KafkaSourceConfig::batch_size`] (or whenever the idle window
    /// flushes a partially-filled buffer), and continues polling until the
    /// configured `max_messages` / `idle_timeout` termination conditions are
    /// hit.
    ///
    /// The trait-level `batch_size` argument is intentionally ignored in
    /// favour of the config field — the config is the user-facing knob the
    /// README documents and routing the pipeline-supplied hint through it
    /// would silently override an explicit config value.
    ///
    /// **Per-page bookmark:** each yielded page carries a snapshot of the
    /// cumulative `(topic, partition) -> next_offset` map seen so far. The
    /// streaming pipeline persists this via the configured `StateStore`
    /// *after* the sink confirms the write, giving at-least-once delivery
    /// with per-page durability — a crash between pages re-reads only the
    /// uncommitted page on resume (the rebalance callback seeds the assigned
    /// partitions with the bookmarked offsets before any fetch happens).
    ///
    /// **`batch_size = 0`:** drains the entire run window (until
    /// `max_messages` or `idle_timeout` fires) into a single page. This
    /// negates the streaming benefit; prefer a finite `batch_size` in
    /// production so the state store advances with each successful sink
    /// write.
    fn stream_pages<'a>(
        &'a self,
        _context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let batch_size = self.config.batch_size;
        let max_messages = self.config.max_messages.unwrap_or(usize::MAX);
        let idle_timeout = self.config.idle_timeout;
        let poll_timeout = self.config.poll_timeout;
        let on_decode_error = self.config.on_decode_error;

        // batch_size == 0 means "drain entire run window into one page" —
        // effectively no per-page flush boundary. Otherwise the page flushes
        // as soon as it accumulates `batch_size` messages.
        let page_chunk = if batch_size == 0 {
            usize::MAX
        } else {
            batch_size
        };
        let initial_capacity = if batch_size == 0 { 1024 } else { batch_size };

        Box::pin(async_stream::try_stream! {
            let mut buffer: Vec<Value> = Vec::with_capacity(initial_capacity);
            let mut pending_offsets: HashMap<(String, i32), i64> = HashMap::new();
            let mut last_message_at = Instant::now();
            let mut total: usize = 0;
            // Group-member mode (#261): offsets of the last yielded page,
            // committed to the consumer group once that page is DURABLE. The
            // generator resumes after a `yield` only when the pipeline polls
            // the next page — which happens after it has written the previous
            // page to the sink and persisted its bookmark — so committing at
            // resume time never advances the group past unwritten data.
            let mut to_commit: Option<HashMap<(String, i32), i64>> = None;

            loop {
                if let Some(durable) = to_commit.take() {
                    self.commit_durable(&durable, false).await;
                }

                // Surface any error raised by the rebalance callback (e.g. a
                // failed seek) before processing the next poll batch.
                self.check_callback_error()?;

                let idle_deadline = idle_timeout.map(|t| last_message_at + t);
                let poll_budget = match idle_deadline {
                    Some(deadline) => deadline
                        .checked_duration_since(Instant::now())
                        .unwrap_or(Duration::ZERO),
                    None => poll_timeout,
                };

                // Accumulators set by the select arm — `?` cannot cross the
                // select's match boundary into the outer `try_stream!`, so we
                // collect errors and termination flags here and act on them
                // after the select returns.
                let mut stop = false;
                let mut fatal: Option<FaucetError> = None;
                tokio::select! {
                    biased;
                    _ = tokio::signal::ctrl_c() => {
                        tracing::info!("kafka source: ctrl_c received, stopping cleanly");
                        stop = true;
                    }
                    recv = tokio::time::timeout(poll_budget, self.consumer.recv()) => {
                        match recv {
                            Ok(Ok(msg)) => {
                                match self.message_to_value(&msg).await {
                                    Ok(record) => {
                                        pending_offsets.insert(
                                            (msg.topic().to_string(), msg.partition()),
                                            msg.offset() + 1,
                                        );
                                        self.context.note_delivered(
                                            msg.topic(),
                                            msg.partition(),
                                            msg.offset() + 1,
                                        );
                                        buffer.push(record);
                                        last_message_at = Instant::now();
                                        total += 1;
                                        if total >= max_messages {
                                            stop = true;
                                        }
                                    }
                                    Err(e) => match on_decode_error {
                                        OnDecodeError::Skip => {
                                            tracing::warn!(error = %e, "kafka source: decode failed, skipping message");
                                        }
                                        OnDecodeError::Fail => fatal = Some(e),
                                    },
                                }
                            }
                            Ok(Err(e)) if is_transient_consume_error(&e) => {
                                tracing::warn!(error = %e, "kafka source: transient consumer error, continuing");
                            }
                            Ok(Err(e)) => {
                                fatal = Some(FaucetError::Source(format!("kafka recv: {e}")));
                            }
                            Err(_timeout) => {
                                if let Some(deadline) = idle_deadline
                                    && Instant::now() >= deadline
                                {
                                    tracing::debug!("kafka source: idle_timeout reached, stopping");
                                    stop = true;
                                }
                            }
                        }
                    }
                }

                if let Some(e) = fatal {
                    Err(e)?;
                }

                // Yield a full page as soon as the buffer hits `page_chunk`.
                // Bookmark = cumulative snapshot of pending_offsets after this
                // page's last message. Snapshot before flushing the buffer so
                // a crash between yield and the next loop iteration re-reads
                // only the uncommitted messages, not those already in this
                // page.
                if !buffer.is_empty() && buffer.len() >= page_chunk {
                    let page_records = std::mem::replace(
                        &mut buffer,
                        Vec::with_capacity(initial_capacity),
                    );
                    let bookmark = self.build_bookmark(&pending_offsets).await?;
                    self.note_emitted(bookmark.as_ref());
                    let durable_snapshot = pending_offsets.clone();
                    yield StreamPage { records: page_records, bookmark };
                    // Resumed ⇒ the page above is durable; commit its offsets
                    // to the group at the top of the next iteration.
                    to_commit = Some(durable_snapshot);
                }

                if stop {
                    break;
                }
            }

            // Flush the trailing buffer (may be empty if the run terminated
            // exactly on a page boundary). When non-empty, emit one final
            // page carrying the cumulative bookmark.
            if !buffer.is_empty() {
                let bookmark = self.build_bookmark(&pending_offsets).await?;
                self.note_emitted(bookmark.as_ref());
                yield StreamPage { records: buffer, bookmark };
            }

            // This code runs only when the pipeline polls past the final page
            // — i.e. after every yielded page has been written and its
            // bookmark persisted — so the whole cumulative offset map is
            // durable. Committing it hands the group's next member a clean
            // starting position (covers the tail the mid-loop commit missed).
            self.commit_durable(&pending_offsets, true).await;

            tracing::info!(
                messages = total,
                batch_size,
                "kafka source: stream complete",
            );
        })
    }

    fn config_schema(&self) -> Value {
        let schema = schemars::schema_for!(KafkaSourceConfig);
        serde_json::to_value(&schema).unwrap_or(Value::Null)
    }

    fn state_key(&self) -> Option<String> {
        Some(self.state_key_value.clone())
    }

    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        let parsed = Bookmark::from_value(bookmark)?;
        // `pending_bookmark` is consumed by the rebalance callback to seed the
        // assigned partitions' starting offsets. `start_offsets` keeps a
        // retained copy so previously-known partitions that are empty this run
        // carry their offset forward into the next bookmark (the H9 fix).
        {
            let mut guard = self.context.start_offsets.lock().map_err(|e| {
                FaucetError::State(format!("kafka start_offsets mutex poisoned: {e}"))
            })?;
            *guard = Some(parsed.clone());
        }
        let mut guard = self.context.pending_bookmark.lock().map_err(|e| {
            FaucetError::State(format!("kafka pending_bookmark mutex poisoned: {e}"))
        })?;
        *guard = Some(parsed);
        Ok(())
    }

    /// Unconsumed messages across every partition of the subscribed topics
    /// (#733): each partition's high watermark minus the offset this source
    /// would read next.
    async fn lag(&self) -> Result<Option<faucet_core::SourceLag>, FaucetError> {
        let position = self
            .emitted
            .lock()
            .ok()
            .and_then(|g| g.clone())
            .or_else(|| self.start_bookmark());
        let positions: HashMap<(String, i32), i64> = position
            .map(|b| {
                b.partition_offsets
                    .into_iter()
                    .map(|p| ((p.topic, p.partition), p.offset))
                    .collect()
            })
            .unwrap_or_default();
        let earliest = matches!(
            self.config.auto_offset_reset,
            crate::config::OffsetReset::Earliest
        );
        let consumer = Arc::clone(&self.consumer);
        let topics = self.config.topics.clone();
        tokio::task::spawn_blocking(move || {
            let timeout = Duration::from_secs(5);
            let mut total = 0u64;
            for topic in &topics {
                let md = consumer
                    .fetch_metadata(Some(topic), timeout)
                    .map_err(|e| FaucetError::Source(format!("kafka lag metadata: {e}")))?;
                let partitions: Vec<i32> = md
                    .topics()
                    .iter()
                    .filter(|t| t.name() == topic)
                    .flat_map(|t| t.partitions().iter().map(|p| p.id()))
                    .collect();
                for partition in partitions {
                    let (low, high) = consumer
                        .fetch_watermarks(topic, partition, timeout)
                        .map_err(|e| FaucetError::Source(format!("kafka lag watermarks: {e}")))?;
                    total += partition_lag(
                        low,
                        high,
                        positions.get(&(topic.clone(), partition)).copied(),
                        earliest,
                    );
                }
            }
            Ok(Some(faucet_core::SourceLag::events(total)))
        })
        .await
        .map_err(|e| FaucetError::Source(format!("kafka lag task: {e}")))?
    }

    /// Kafka partitions are immutable, ordered logs and every emitted page
    /// carries a complete per-partition next-offset bookmark, so resuming from
    /// any bookmark continues the record stream at exactly that position — no
    /// record before it is re-emitted, none after it is skipped. Page
    /// *boundaries* on replay may differ (idle-timeout cuts are timing-
    /// dependent), which is why the pipeline's atomic-watermark mechanism
    /// re-anchors from the bookmark embedded in the sink's committed token
    /// rather than skipping replayed pages by count. In plain (non-member)
    /// mode the source never commits consumer-group offsets, so the broker's
    /// group position can never run ahead of the durable bookmark.
    fn supports_exactly_once(&self) -> bool {
        true
    }

    fn connector_name(&self) -> &'static str {
        "kafka"
    }

    fn dataset_uri(&self) -> String {
        let broker = self
            .config
            .brokers
            .split(',')
            .next()
            .unwrap_or(&self.config.brokers)
            .trim();
        format!("kafka://{}?topic={}", broker, self.config.topics.join(","))
    }

    /// Preflight probe that does **not** consume any message.
    ///
    /// The default `Source::check` would call `stream_pages`, which polls for
    /// a message and would block on an empty topic until the idle/max-message
    /// terminator fires. Instead we fetch cluster metadata
    /// (`fetch_metadata(None, timeout)`), which validates broker connectivity +
    /// auth without consuming or committing anything.
    ///
    /// `fetch_metadata` is a blocking librdkafka call, so it runs on a blocking
    /// thread; the whole probe is additionally bounded by `ctx.timeout`.
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};
        use rdkafka::util::Timeout;

        let start = std::time::Instant::now();
        let consumer = Arc::clone(&self.consumer);
        let rd_timeout = Timeout::After(ctx.timeout);

        // Run the blocking fetch_metadata off the async runtime, bounded by the
        // same wall-clock budget so an unreachable broker can't hang the probe.
        let fetch = tokio::task::spawn_blocking(move || {
            consumer
                .fetch_metadata(None, rd_timeout)
                .map(|md| md.brokers().len())
                .map_err(|e| e.to_string())
        });

        let probe = match tokio::time::timeout(ctx.timeout, fetch).await {
            Ok(Ok(Ok(broker_count))) => {
                tracing::debug!(broker_count, "kafka check: fetched cluster metadata");
                Probe::pass("metadata", start.elapsed())
            }
            Ok(Ok(Err(e))) => Probe::fail_hint(
                "metadata",
                start.elapsed(),
                e,
                "verify brokers, network reachability, and auth (SASL/TLS) settings",
            ),
            Ok(Err(join_err)) => Probe::fail(
                "metadata",
                start.elapsed(),
                format!("metadata fetch task failed: {join_err}"),
            ),
            Err(_elapsed) => Probe::fail_hint(
                "metadata",
                start.elapsed(),
                "metadata fetch timed out",
                "no broker responded within the check timeout",
            ),
        };
        Ok(CheckReport::single(probe))
    }

    /// The Kafka source is always shardable via **native consumer-group
    /// assignment** (#261): each shard is a membership slot in the shared
    /// group, and the broker — not faucet — assigns partitions across the
    /// members and rebalances on membership change. Sharding only takes
    /// effect when the cluster coordinator calls `apply_shard`; a plain
    /// `faucet run` consumes as a single group member, unchanged.
    fn is_shardable(&self) -> bool {
        true
    }

    /// Enumerate `target` group-membership slots, capped at the subscription's
    /// total partition count (extra members would never be assigned a
    /// partition). Falls back to the uncapped `target` when topic metadata
    /// cannot be resolved.
    async fn enumerate_shards(&self, target: usize) -> Result<Vec<ShardSpec>, FaucetError> {
        if target <= 1 {
            return Ok(vec![ShardSpec::whole()]);
        }
        let cap = self.total_partitions().await;
        Ok(plan_member_shards(target, cap))
    }

    /// Enter group-member mode for one membership slot (or leave it, for the
    /// whole-dataset shard). In member mode the source additionally:
    ///
    /// - **commits offsets to the consumer group at durable page boundaries**
    ///   (see [`stream_pages`](Self::stream_pages)), so partitions that
    ///   migrate between members resume from the last durably-written
    ///   position;
    /// - **defers bookmark seeks to the group's committed offsets** whenever
    ///   those are ahead (another member may have advanced a partition past
    ///   this member's bookmark).
    ///
    /// Only the streaming path (`stream_pages`, which every `Pipeline` run
    /// drives) commits; the batch `fetch_all` path never commits because its
    /// records are not durable until after the fetch returns.
    async fn apply_shard(&self, shard: &ShardSpec) -> Result<(), FaucetError> {
        let parsed = parse_member_shard(shard)?;
        self.context
            .member_mode
            .store(parsed.is_some(), Ordering::Release);
        *self
            .member_shard
            .lock()
            .map_err(|e| FaucetError::State(format!("kafka member_shard mutex poisoned: {e}")))? =
            parsed;
        if let Some(m) = parsed {
            tracing::info!(
                member = m.member,
                members = m.members,
                group = %self.config.group_id,
                "kafka source: joining the consumer group as one of N cooperating members (Mode B)"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn partition_lag_counts_from_the_next_offset() {
        assert_eq!(partition_lag(0, 100, Some(40), false), 60);
        assert_eq!(partition_lag(50, 100, Some(10), false), 50);
        assert_eq!(partition_lag(0, 100, None, true), 100);
        assert_eq!(partition_lag(0, 100, None, false), 0);
        assert_eq!(partition_lag(0, 100, Some(150), false), 0);
    }

    use super::*;
    use crate::config::OffsetReset;
    use std::collections::BTreeMap;

    /// Build a source pointing at an unreachable broker. rdkafka client
    /// construction and `subscribe()` are local operations (no broker
    /// round-trip), so this constructs fine offline — only actual
    /// polls/metadata fetches would fail.
    async fn offline_source() -> KafkaSource {
        let config = KafkaSourceConfig {
            brokers: "127.0.0.1:1".into(),
            topics: vec!["orders".into()],
            group_id: "g".into(),
            auth: faucet_common_kafka::KafkaAuth::None,
            value_format: faucet_common_kafka::KafkaValueFormat::Json,
            key_format: None,
            auto_offset_reset: OffsetReset::Latest,
            max_messages: Some(1),
            idle_timeout: None,
            poll_timeout: Duration::from_secs(1),
            session_timeout: Duration::from_secs(30),
            on_decode_error: OnDecodeError::Fail,
            extra_client_config: BTreeMap::new(),
            batch_size: 10,
        };
        KafkaSource::new(config)
            .await
            .expect("offline construction")
    }

    #[tokio::test]
    async fn source_is_shardable() {
        assert!(offline_source().await.is_shardable());
    }

    #[tokio::test]
    async fn apply_shard_enters_and_leaves_member_mode() {
        let source = offline_source().await;
        assert!(!source.member_mode(), "plain run starts out of member mode");

        let member = ShardSpec::new("1", serde_json::json!({ "members": 3, "member": 1 }));
        source.apply_shard(&member).await.unwrap();
        assert!(source.member_mode());
        assert_eq!(
            *source.member_shard.lock().unwrap(),
            Some(MemberShard {
                members: 3,
                member: 1
            })
        );

        // The whole-dataset shard clears member mode (plain single consumer).
        source.apply_shard(&ShardSpec::whole()).await.unwrap();
        assert!(!source.member_mode());
        assert_eq!(*source.member_shard.lock().unwrap(), None);
    }

    #[tokio::test]
    async fn apply_shard_rejects_malformed_descriptor() {
        let source = offline_source().await;
        let err = source
            .apply_shard(&ShardSpec::new("0", serde_json::json!({ "member": 0 })))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("kafka"), "got: {err}");
        assert!(
            !source.member_mode(),
            "a rejected shard must not flip modes"
        );
    }

    #[tokio::test]
    async fn enumerate_shards_target_one_is_whole() {
        // target <= 1 short-circuits before any metadata fetch, so this is
        // fast offline.
        let source = offline_source().await;
        let shards = source.enumerate_shards(1).await.unwrap();
        assert_eq!(shards.len(), 1);
        assert!(shards[0].is_whole());
    }

    #[tokio::test]
    async fn enumerate_shards_uncapped_when_metadata_unreachable() {
        // fetch_metadata against the unreachable broker fails within its
        // bounded budget; enumeration falls back to the requested member
        // count (uncapped) instead of failing the run.
        let source = offline_source().await;
        let shards = source.enumerate_shards(3).await.unwrap();
        assert_eq!(shards.len(), 3);
        assert_eq!(shards[0].descriptor["members"], 3);
        assert_eq!(shards[2].descriptor["member"], 2);
    }

    #[tokio::test]
    async fn commit_durable_with_no_assignment_commits_nothing() {
        // Member mode + a non-empty offset map, but the consumer owns no
        // partitions (it never polled): the assigned-partition filter drops
        // everything and no commit is attempted — which would otherwise
        // block against the unreachable broker.
        let source = offline_source().await;
        source
            .apply_shard(&ShardSpec::new(
                "0",
                serde_json::json!({ "members": 2, "member": 0 }),
            ))
            .await
            .unwrap();
        let mut offsets = HashMap::new();
        offsets.insert(("orders".to_string(), 0), 10i64);
        source.commit_durable(&offsets, true).await;
    }

    #[tokio::test]
    async fn commit_durable_is_a_noop_outside_member_mode() {
        // Must not touch the (unreachable) broker: returns before any
        // assignment/commit call when member mode is off or offsets are empty.
        let source = offline_source().await;
        let mut offsets = HashMap::new();
        offsets.insert(("orders".to_string(), 0), 10i64);
        source.commit_durable(&offsets, false).await; // member mode off
        source
            .apply_shard(&ShardSpec::new(
                "0",
                serde_json::json!({ "members": 2, "member": 0 }),
            ))
            .await
            .unwrap();
        source.commit_durable(&HashMap::new(), true).await; // empty offsets
    }

    #[test]
    fn dataset_uri_single_broker_single_topic() {
        let brokers = "kafka.example.com:9092";
        let topics = ["orders"];
        let broker = brokers.split(',').next().unwrap_or(brokers).trim();
        let uri = format!("kafka://{}?topic={}", broker, topics.join(","));
        assert_eq!(uri, "kafka://kafka.example.com:9092?topic=orders");
    }

    #[test]
    fn dataset_uri_multi_broker_uses_first() {
        let brokers = "b1:9092,b2:9092,b3:9092";
        let topics = ["events", "logs"];
        let broker = brokers.split(',').next().unwrap_or(brokers).trim();
        let uri = format!("kafka://{}?topic={}", broker, topics.join(","));
        assert_eq!(uri, "kafka://b1:9092?topic=events,logs");
    }

    #[cfg(feature = "schema-registry")]
    mod sr_client {
        use crate::stream::build_sr_client;
        use faucet_common_kafka::{KafkaValueFormat, SchemaRegistryConfig};

        #[test]
        fn build_sr_client_none_for_plain_formats() {
            assert!(
                build_sr_client(&KafkaValueFormat::Json, None)
                    .unwrap()
                    .is_none()
            );
            assert!(
                build_sr_client(&KafkaValueFormat::RawString, Some(&KafkaValueFormat::Bytes))
                    .unwrap()
                    .is_none()
            );
        }

        #[test]
        fn build_sr_client_some_for_confluent_avro_value() {
            let format = KafkaValueFormat::ConfluentAvro {
                schema_registry: SchemaRegistryConfig::new("http://localhost:8081"),
            };
            assert!(build_sr_client(&format, None).unwrap().is_some());
        }

        #[test]
        fn build_sr_client_some_for_confluent_protobuf_value() {
            let format = KafkaValueFormat::ConfluentProtobuf {
                schema_registry: SchemaRegistryConfig::new("http://localhost:8081"),
            };
            assert!(build_sr_client(&format, None).unwrap().is_some());
        }

        #[test]
        fn build_sr_client_some_for_confluent_json_schema_value() {
            let format = KafkaValueFormat::ConfluentJsonSchema {
                schema_registry: SchemaRegistryConfig::new("http://localhost:8081"),
                validate: true,
            };
            assert!(build_sr_client(&format, None).unwrap().is_some());
        }

        #[test]
        fn build_sr_client_falls_back_to_key_format() {
            let key = KafkaValueFormat::ConfluentAvro {
                schema_registry: SchemaRegistryConfig::new("http://localhost:8081"),
            };
            assert!(
                build_sr_client(&KafkaValueFormat::Json, Some(&key))
                    .unwrap()
                    .is_some()
            );
        }

        #[test]
        fn build_sr_client_propagates_invalid_url() {
            let format = KafkaValueFormat::ConfluentAvro {
                schema_registry: SchemaRegistryConfig::new("not-a-url"),
            };
            assert!(build_sr_client(&format, None).is_err());
        }
    }

    #[test]
    fn transient_consumer_errors_do_not_fail_the_run() {
        use rdkafka::error::{KafkaError, RDKafkaErrorCode as C};
        for code in [
            C::BrokerTransportFailure,
            C::AllBrokersDown,
            C::PollExceeded,
            C::NotCoordinator,
            C::RequestTimedOut,
        ] {
            assert!(
                is_transient_consume_error(&KafkaError::MessageConsumption(code)),
                "{code:?}"
            );
        }
        assert!(is_transient_consume_error(&KafkaError::PartitionEOF(0)));
        assert!(!is_transient_consume_error(
            &KafkaError::MessageConsumptionFatal(C::BrokerTransportFailure)
        ));
        assert!(!is_transient_consume_error(
            &KafkaError::MessageConsumption(C::TopicAuthorizationFailed)
        ));
        assert!(!is_transient_consume_error(&KafkaError::Canceled));
    }
}
