//! Exactly-once delivery support for the Kafka sink.
//!
//! Implements the watermark mechanics behind the `Sink` idempotency hooks: a
//! transactional producer commits each page's records plus a commit-token
//! record into a compacted side-topic in one Kafka transaction, and the token
//! is read back on resume. See
//! `docs/superpowers/specs/2026-06-18-kafka-sink-exactly-once-design.md`.

use crate::config::KafkaSinkConfig;
use faucet_core::FaucetError;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use std::time::Duration;

/// Build the shared connection `ClientConfig` (brokers + auth) reused by the
/// producer, the transactional producer, the admin client, and the
/// token-reader consumer. Only keys valid for every client type live here.
///
/// Producer-only keys (compression, buffering, idempotence) and the
/// `extra_client_config` overrides are layered on by the producer builders —
/// applying them here would let a producer-only property reach a consumer or
/// admin client and be rejected at create time.
pub(crate) fn client_config_base(config: &KafkaSinkConfig) -> Result<ClientConfig, FaucetError> {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", &config.brokers);
    config.auth.apply(&mut cfg)?;
    Ok(cfg)
}

/// Full producer `ClientConfig`: the connection base plus producer tuning
/// (`acks`, idempotence, compression, linger, message timeout, buffer cap) and
/// the user's `extra_client_config` overrides (applied last so they win). Used
/// by both the at-least-once producer and the transactional producer; the
/// latter then force-sets the transactional invariants on top.
pub(crate) fn producer_client_config(
    config: &KafkaSinkConfig,
) -> Result<ClientConfig, FaucetError> {
    let mut cfg = client_config_base(config)?;
    cfg.set("acks", config.acks.as_str());
    cfg.set(
        "enable.idempotence",
        if config.idempotent { "true" } else { "false" },
    );
    cfg.set("compression.type", config.compression.as_str());
    cfg.set("linger.ms", config.linger.as_millis().to_string());
    cfg.set(
        "message.timeout.ms",
        config.message_timeout.as_millis().to_string(),
    );
    if config.batch_size > 0 {
        cfg.set(
            "queue.buffering.max.messages",
            config.batch_size.to_string(),
        );
    }
    for (k, v) in &config.extra_client_config {
        cfg.set(k, v);
    }
    Ok(cfg)
}

/// Auto-create the compacted commit-token side-topic if it does not exist.
/// Idempotent: an "already exists" result is treated as success.
pub(crate) async fn ensure_commit_topic(
    config: &KafkaSinkConfig,
    base: &ClientConfig,
) -> Result<(), FaucetError> {
    let admin: AdminClient<DefaultClientContext> = base
        .create()
        .map_err(|e| FaucetError::Sink(format!("kafka admin client init: {e}")))?;
    let eo = config.exactly_once_spec();
    let topic = NewTopic::new(
        &eo.commit_token_topic,
        eo.commit_token_topic_partitions,
        TopicReplication::Fixed(eo.commit_token_topic_replication),
    )
    .set("cleanup.policy", "compact");
    let results = admin
        .create_topics([&topic], &AdminOptions::new())
        .await
        .map_err(|e| FaucetError::Sink(format!("kafka create_topics request: {e}")))?;
    for r in results {
        match r {
            Ok(_) => {}
            Err((_t, RDKafkaErrorCode::TopicAlreadyExists)) => {}
            Err((t, code)) => {
                return Err(FaucetError::Sink(format!(
                    "kafka create commit-token topic '{t}': {code:?}"
                )));
            }
        }
    }
    Ok(())
}

/// Read the latest committed token for `scope` from the compacted side-topic.
/// Returns `None` when the topic is empty or has no token for the scope.
///
/// Builds a short-lived, non-committing consumer, assigns every side-topic
/// partition from the beginning, and drains up to each partition's high
/// watermark, folding each record into a running max and discarding it. Memory
/// is O(1) in the side-topic size, so a large or non-compacted topic cannot
/// blow up the startup read. Called once per run (at startup).
pub(crate) async fn read_last_token(
    config: &KafkaSinkConfig,
    base: &ClientConfig,
    scope: &str,
) -> Result<Option<String>, FaucetError> {
    let mut cfg = base.clone();
    cfg.set("group.id", "faucet-commit-token-reader");
    cfg.set("enable.auto.commit", "false");
    cfg.set("auto.offset.reset", "earliest");
    // The side-topic is written by a transactional producer, so we must only
    // read committed records — `read_committed` also makes `fetch_watermarks`
    // return the Last Stable Offset, keeping the drain target consistent with
    // what `poll` delivers. librdkafka defaults to this, but it is load-bearing
    // for exactly-once correctness, so pin it explicitly.
    cfg.set("isolation.level", "read_committed");
    let topic = config.exactly_once_spec().commit_token_topic;
    let scope = scope.to_string();
    let timeout = config.message_timeout;

    tokio::task::spawn_blocking(move || read_last_token_blocking(&cfg, &topic, &scope, timeout))
        .await
        .map_err(|e| FaucetError::Sink(format!("kafka token read task: {e}")))?
}

fn read_last_token_blocking(
    cfg: &ClientConfig,
    topic: &str,
    scope: &str,
    timeout: Duration,
) -> Result<Option<String>, FaucetError> {
    let consumer: BaseConsumer = cfg
        .create()
        .map_err(|e| FaucetError::Sink(format!("kafka token reader init: {e}")))?;

    let metadata = consumer
        .fetch_metadata(Some(topic), timeout)
        .map_err(|e| FaucetError::Sink(format!("kafka token reader metadata: {e}")))?;
    let topic_meta = match metadata.topics().iter().find(|t| t.name() == topic) {
        Some(t) if !t.partitions().is_empty() => t,
        _ => return Ok(None),
    };

    let mut tpl = TopicPartitionList::new();
    // (partition_id, high_watermark, is_empty). Under `read_committed`, `high` is
    // the Last Stable Offset — the offset *after* the last committed batch, including
    // any transaction commit/abort control markers — so the drain target is the
    // consumer's fetch *position* reaching `high`, NOT a count of delivered records
    // (a commit marker advances the log offset but is never delivered via `poll`).
    // `is_empty` (low == high) means the
    // partition has no readable records — true on a fresh topic AND on a fully
    // compacted partition whose log-start offset advanced past 0; either way it is
    // already drained and must never block the loop while we wait on a position
    // that will never be reported (nothing is ever fetched there).
    let mut ends: Vec<(i32, i64, bool)> = Vec::new();
    for p in topic_meta.partitions() {
        let (low, high) = consumer
            .fetch_watermarks(topic, p.id(), timeout)
            .map_err(|e| FaucetError::Sink(format!("kafka token reader watermarks: {e}")))?;
        ends.push((p.id(), high, low >= high));
        tpl.add_partition_offset(topic, p.id(), Offset::Beginning)
            .map_err(|e| FaucetError::Sink(format!("kafka token reader tpl: {e}")))?;
    }
    consumer
        .assign(&tpl)
        .map_err(|e| FaucetError::Sink(format!("kafka token reader assign: {e}")))?;

    // Per-partition next-fetch position. A partition is drained once its position
    // reaches its high watermark (LSO). The consumer advances its position past a
    // control marker even though no record is delivered, so position — unlike a
    // delivered-record count — converges to `high` exactly. An empty partition has
    // nothing to fetch (no position is ever reported), so it counts as drained
    // outright.
    let position_reached = |consumer: &BaseConsumer, ends: &[(i32, i64, bool)]| -> bool {
        let Ok(pos) = consumer.position() else {
            return false;
        };
        ends.iter().all(|(pid, high, is_empty)| {
            if *is_empty {
                return true;
            }
            match pos
                .find_partition(topic, *pid)
                .and_then(|p| match p.offset() {
                    Offset::Offset(o) => Some(o),
                    _ => None,
                }) {
                Some(o) => o >= *high,
                // Non-empty partition with no fetched position yet ⇒ not drained.
                None => false,
            }
        })
    };

    // Keep only the running max token for `scope` (the FULL token string, so the
    // embedded resume bookmark survives — see `fold_token_for_scope`) and a count
    // of records drained (for the fail-loud message), never the records
    // themselves — so a non-compacted or not-yet-compacted side-topic with an
    // unbounded backlog cannot blow up memory at startup. Each polled record is
    // folded in and discarded.
    let mut max_token: Option<String> = None;
    let mut drained: u64 = 0;
    if position_reached(&consumer, &ends) {
        // Every partition empty (or already at its watermark) — nothing to read.
        return Ok(None);
    }
    loop {
        match consumer.poll(timeout) {
            Some(Ok(msg)) => {
                max_token =
                    fold_token_for_scope(max_token, msg.key().unwrap_or(&[]), msg.payload(), scope);
                drained += 1;
                if position_reached(&consumer, &ends) {
                    break;
                }
            }
            Some(Err(e)) => {
                return Err(FaucetError::Sink(format!("kafka token reader poll: {e}")));
            }
            // An empty poll means the broker delivered no record within `timeout`.
            // The consumer's fetch position still advances past control markers on
            // an empty fetch, so re-check it: if every partition has reached its
            // high watermark we are genuinely done (the remaining gap to `high` was
            // a transaction commit marker, which is never delivered). Only if the
            // position has NOT reached the watermark did the broker fail to deliver
            // committed data records in time — returning the max found so far could
            // yield a token *below* the true committed value, making the pipeline
            // re-write already-committed pages and produce duplicates. Fail loudly
            // there rather than silently degrading exactly-once.
            None => {
                if position_reached(&consumer, &ends) {
                    break;
                }
                return Err(FaucetError::Sink(format!(
                    "kafka token reader: drained {drained} record(s) but did not reach the high \
                     watermark on every partition within the {timeout:?} poll timeout — refusing \
                     to return a possibly-stale commit token"
                )));
            }
        }
    }

    Ok(max_token)
}

/// Derive the producer `transactional.id` from a stable pipeline scope.
///
/// The result is `"{prefix}.{sanitized}"`, where `sanitized` replaces any
/// character outside `[A-Za-z0-9._-]` with `_`. This keeps the id stable across
/// restarts of the same pipeline-row (so a restart fences its own zombie) and
/// unique across rows/pipelines whose scopes differ after sanitization (so
/// distinct pipelines never fence each other). Sanitization is many-to-one, so
/// callers must keep their scopes distinct under it; faucet derives scopes from
/// the pipeline/row identity (`{name}::{row_id}`), which stay distinct. `prefix`
/// is interpolated verbatim — validating it as a legal `transactional.id`
/// fragment is the caller's responsibility.
pub(crate) fn derive_transactional_id(prefix: &str, scope: &str) -> String {
    let sanitized: String = scope
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("{prefix}.{sanitized}")
}

/// Fold one side-topic record into the running maximum-sequence commit-token
/// **string** for `scope`.
///
/// Returns the updated running max: the record's token string replaces
/// `running` only when its key equals `scope`, its value parses as a commit
/// token, and that token's sequence meets or exceeds the current running
/// sequence. A tombstone (no value), a non-token value, or a record for a
/// different scope leaves `running` unchanged.
///
/// Crucially this keeps the **full** token string — including any
/// `#<bookmark-json>` suffix written by
/// [`faucet_core::idempotency::format_token_with_bookmark`] — so
/// `last_committed_token` can hand the embedded resume bookmark back to the
/// pipeline for sink-anchored exactly-once resume (audit #321 C1; the pre-fix
/// version returned only the parsed `u64`, stripping the bookmark). Comparison
/// is on the parsed sequence; a `>=` keeps the latest record in log order on an
/// equal sequence (the side-topic is keyed by `scope`, so compaction ultimately
/// collapses to the newest record anyway). O(1) per record — the reader keeps a
/// single running max instead of buffering the whole side-topic.
pub(crate) fn fold_token_for_scope(
    running: Option<String>,
    key: &[u8],
    value: Option<&[u8]>,
    scope: &str,
) -> Option<String> {
    if key != scope.as_bytes() {
        return running;
    }
    let Some(token_str) = value.and_then(|v| std::str::from_utf8(v).ok()) else {
        return running;
    };
    let Some(seq) = faucet_core::idempotency::parse_token(token_str) else {
        return running;
    };
    match &running {
        Some(cur) => {
            let cur_seq = faucet_core::idempotency::parse_token(cur).unwrap_or(0);
            if seq >= cur_seq {
                Some(token_str.to_string())
            } else {
                running
            }
        }
        None => Some(token_str.to_string()),
    }
}

/// Enqueue one record into the current transaction, retrying on `QueueFull`.
///
/// Unlike the at-least-once `send_with_queue_full_retry`, this does NOT await
/// the delivery future: inside a transaction, delivery only completes at
/// `commit_transaction`, so awaiting here would deadlock. Errors surface at
/// commit time.
pub(crate) async fn enqueue_in_txn(
    producer: &FutureProducer,
    topic: &str,
    value_bytes: Vec<u8>,
    routing: crate::sink::RecordRouting,
    max_retries: u32,
    backoff: Duration,
) -> Result<(), FaucetError> {
    let mut attempts: u32 = 0;
    loop {
        let mut record: FutureRecord<'_, [u8], [u8]> =
            FutureRecord::to(topic).payload(value_bytes.as_slice());
        if let Some(k) = routing.key.as_deref() {
            record = record.key(k);
        }
        if let Some(p) = routing.partition {
            record = record.partition(p);
        }
        if let Some(h) = routing.headers.as_ref() {
            record = record.headers(crate::sink::owned_headers(h));
        }
        match producer.send_result(record) {
            Ok(_delivery_future) => return Ok(()),
            Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _)) => {
                if attempts >= max_retries {
                    return Err(FaucetError::Sink(format!(
                        "kafka send: QueueFull after {max_retries} retries"
                    )));
                }
                tracing::warn!(attempts, "kafka send: QueueFull, backing off");
                tokio::time::sleep(backoff).await;
                attempts += 1;
            }
            Err((e, _)) => return Err(FaucetError::Sink(format!("kafka send: {e}"))),
        }
    }
}

/// Abort the current transaction (best-effort, on the blocking pool).
pub(crate) async fn abort_txn(
    producer: std::sync::Arc<FutureProducer>,
    timeout: Duration,
) -> Result<(), FaucetError> {
    tokio::task::spawn_blocking(move || producer.abort_transaction(timeout))
        .await
        .map_err(|e| FaucetError::Sink(format!("kafka abort task: {e}")))?
        .map_err(|e| FaucetError::Sink(format!("kafka abort_transaction: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_config_sets_brokers_only() {
        use crate::config::{Acks, KafkaSinkConfig, KafkaSinkTopic};
        use faucet_common_kafka::{CompressionType, KafkaAuth, KafkaValueFormat, OnKeyError};
        use std::collections::BTreeMap;
        use std::time::Duration;

        let config = KafkaSinkConfig {
            brokers: "host:9092".into(),
            topic: KafkaSinkTopic::Fixed { name: "out".into() },
            auth: KafkaAuth::None,
            value_format: KafkaValueFormat::Json,
            key_format: None,
            value_schema: None,
            key_schema: None,
            key_path: None,
            partition_path: None,
            headers_path: None,
            on_key_error: OnKeyError::Fail,
            compression: CompressionType::None,
            acks: Acks::All,
            idempotent: true,
            linger: Duration::from_millis(5),
            batch_size: faucet_core::DEFAULT_BATCH_SIZE,
            message_timeout: Duration::from_secs(30),
            max_in_flight: 100,
            queue_full_backoff: Duration::from_millis(100),
            queue_full_max_retries: 3,
            exactly_once: None,
            transactional_id_prefix: None,
            commit_token_topic: "__faucet_commit_token".into(),
            commit_token_topic_partitions: 1,
            commit_token_topic_replication: -1,
            extra_client_config: BTreeMap::new(),
        };
        let cfg = client_config_base(&config).unwrap();
        assert_eq!(cfg.get("bootstrap.servers"), Some("host:9092"));
        // compression is producer-only — layered by new(), not by the base.
        assert_eq!(cfg.get("compression.type"), None);
    }

    #[test]
    fn derive_sanitizes_scope_separators() {
        assert_eq!(
            derive_transactional_id("faucet", "pipe::row0"),
            "faucet.pipe__row0"
        );
    }

    #[test]
    fn derive_keeps_allowed_chars_and_prefix() {
        assert_eq!(derive_transactional_id("acme", "a.b-c_1"), "acme.a.b-c_1");
        assert_eq!(derive_transactional_id("faucet", "x/y z"), "faucet.x_y_z");
    }

    #[test]
    fn fold_returns_none_without_a_matching_scope() {
        let tok = |n| faucet_core::idempotency::format_token(n).into_bytes();
        // Only other-scope records seen ⇒ running max stays None (resume from start).
        let mut acc = None;
        acc = fold_token_for_scope(acc, b"s2", Some(&tok(99)), "s1");
        acc = fold_token_for_scope(acc, b"s3", Some(&tok(42)), "s1");
        assert_eq!(acc, None);
    }

    #[test]
    fn fold_keeps_running_max_for_scope_only() {
        let tok = |n| faucet_core::idempotency::format_token(n).into_bytes();
        let s = faucet_core::idempotency::format_token;
        // Out-of-order tokens for the target scope: running max only grows.
        let mut acc = None;
        acc = fold_token_for_scope(acc, b"s1", Some(&tok(3)), "s1");
        assert_eq!(acc.as_deref(), Some(s(3).as_str()));
        acc = fold_token_for_scope(acc, b"s1", Some(&tok(7)), "s1");
        assert_eq!(acc.as_deref(), Some(s(7).as_str()));
        // A lower token must not lower the running max.
        acc = fold_token_for_scope(acc, b"s1", Some(&tok(5)), "s1");
        assert_eq!(acc.as_deref(), Some(s(7).as_str()));
        // A record for a different scope is ignored (does not perturb the max).
        acc = fold_token_for_scope(acc, b"s2", Some(&tok(99)), "s1");
        assert_eq!(acc.as_deref(), Some(s(7).as_str()));
    }

    #[test]
    fn fold_ignores_tombstones_and_garbage() {
        let tok = |n| faucet_core::idempotency::format_token(n).into_bytes();
        let s = faucet_core::idempotency::format_token;
        let mut acc = None;
        // Tombstone (no value) for the scope: running max unchanged.
        acc = fold_token_for_scope(acc, b"s1", None, "s1");
        assert_eq!(acc, None);
        // Non-token value for the scope: ignored.
        acc = fold_token_for_scope(acc, b"s1", Some(b"not-a-token"), "s1");
        assert_eq!(acc, None);
        // A valid token then sets it.
        acc = fold_token_for_scope(acc, b"s1", Some(&tok(4)), "s1");
        assert_eq!(acc.as_deref(), Some(s(4).as_str()));
    }

    #[test]
    fn fold_preserves_embedded_bookmark_of_max_token() {
        // #321 C1: the running max must keep the FULL token string (with its
        // `#<bookmark-json>` suffix) so sink-anchored exactly-once resume can
        // recover the embedded resume position — not just the parsed sequence.
        use faucet_core::idempotency::{format_token_with_bookmark, parse_token_parts};
        let bm = serde_json::json!({"partition_offsets": [{"t": "x", "p": 0, "offset": 99}]});
        let with_bm = format_token_with_bookmark(7, Some(&bm)).into_bytes();
        let bare = faucet_core::idempotency::format_token(3).into_bytes();

        let mut acc = None;
        acc = fold_token_for_scope(acc, b"s1", Some(&bare), "s1");
        acc = fold_token_for_scope(acc, b"s1", Some(&with_bm), "s1");
        // The higher-sequence token (7, carrying the bookmark) wins, verbatim.
        let token = acc.expect("a token survives");
        let (seq, parsed_bm) = parse_token_parts(&token).expect("parses");
        assert_eq!(seq, 7);
        assert_eq!(
            parsed_bm,
            Some(bm),
            "embedded bookmark must survive the fold"
        );
    }
}
