//! The SQS `Sink` implementation: encode → size-aware chunking →
//! bounded-concurrency `SendMessageBatch` → per-entry partial-failure retry.

use crate::config::SqsSinkConfig;
use aws_sdk_sqs::Client;
use aws_sdk_sqs::types::SendMessageBatchRequestEntry;
use faucet_core::{FaucetError, RowOutcome};
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

/// One record encoded and ready to ship, tagged with its input position so
/// outcomes can be reported in input order.
#[derive(Debug, Clone)]
pub(crate) struct Encoded {
    pub index: usize,
    pub body: String,
    pub group_id: Option<String>,
    pub dedup_id: Option<String>,
}

impl Encoded {
    /// Bytes this entry contributes to a request (the message body — the
    /// quantity SQS counts against its 256 KiB request ceiling).
    pub fn request_bytes(&self) -> usize {
        self.body.len()
    }
}

/// Exponential backoff between the configured bounds — `initial * 2^attempt`
/// capped at `max_ms` — jittered through core's shared decorrelated helper.
///
/// The jitter is load-bearing, not cosmetic: a partially-failed
/// `SendMessageBatch` leaves every failed entry of the batch to retry, and
/// without decorrelation they (and every concurrent request) re-fire in the
/// same instant, re-creating the throttle that caused the failure. Delegating
/// to [`faucet_core::retry::apply_jitter`] also means the two jitter bugs fixed
/// there (a divisor that capped the factor at 0.733, and same-nanosecond
/// alignment across tasks) apply here rather than having to be rediscovered
/// (#654 M13).
pub(crate) fn backoff_delay(initial_ms: u64, max_ms: u64, attempt: usize) -> Duration {
    let exp = initial_ms.saturating_mul(1u64 << attempt.min(20));
    faucet_core::retry::apply_jitter(Duration::from_millis(exp.min(max_ms)))
}

/// Stringify a top-level record field for use as a `MessageDeduplicationId`.
/// Missing / null / non-scalar fields are per-record errors.
pub(crate) fn dedup_id_for(record: &Value, field: &str) -> Result<String, FaucetError> {
    match record.get(field) {
        Some(Value::String(s)) => Ok(s.clone()),
        Some(Value::Number(n)) => Ok(n.to_string()),
        Some(Value::Bool(b)) => Ok(b.to_string()),
        Some(Value::Null) | None => Err(FaucetError::Sink(format!(
            "sqs: record is missing dedup field '{field}'"
        ))),
        Some(other) => Err(FaucetError::Sink(format!(
            "sqs: dedup field '{field}' is not a scalar (got {other})"
        ))),
    }
}

/// Chunk encoded entries into `SendMessageBatch` requests honouring both the
/// entry-count and request-byte ceilings. Pure. Entries above the byte ceiling
/// are assumed to have been filtered out already.
pub(crate) fn chunk_requests(
    entries: Vec<Encoded>,
    max_entries: usize,
    max_request_bytes: usize,
) -> Vec<Vec<Encoded>> {
    let mut out = Vec::new();
    let mut current: Vec<Encoded> = Vec::new();
    let mut current_bytes = 0usize;
    for e in entries {
        let bytes = e.request_bytes();
        if !current.is_empty()
            && (current.len() >= max_entries || current_bytes + bytes > max_request_bytes)
        {
            out.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current_bytes += bytes;
        current.push(e);
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

/// AWS SQS sink. See the crate README for semantics.
pub struct SqsSink {
    config: SqsSinkConfig,
    client: Client,
}

impl SqsSink {
    /// Create a new SQS sink. Validates the config and builds the AWS client
    /// (no network I/O).
    pub async fn new(config: SqsSinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let client = faucet_common_sqs::build_client(
            config.region.as_deref(),
            config.endpoint_url.as_deref(),
            &config.credentials,
        )
        .await?;
        Ok(Self { config, client })
    }

    /// Encode every record; per-record failures (serialization, oversized body,
    /// unresolvable dedup id) land in the error map keyed by input index.
    pub(crate) fn encode_records(
        &self,
        records: &[Value],
    ) -> (Vec<Encoded>, BTreeMap<usize, FaucetError>) {
        let mut encoded = Vec::with_capacity(records.len());
        let mut failures = BTreeMap::new();
        for (index, record) in records.iter().enumerate() {
            let body = match serde_json::to_string(record) {
                Ok(b) => b,
                Err(e) => {
                    failures.insert(
                        index,
                        FaucetError::Sink(format!("sqs: record does not serialize to JSON: {e}")),
                    );
                    continue;
                }
            };
            if body.len() > crate::config::MAX_MESSAGE_BYTES {
                failures.insert(
                    index,
                    FaucetError::Sink(format!(
                        "sqs: message body is {} bytes, above the SQS {} byte limit — never sent",
                        body.len(),
                        crate::config::MAX_MESSAGE_BYTES
                    )),
                );
                continue;
            }
            let dedup_id = match &self.config.message_deduplication_id_field {
                Some(field) => match dedup_id_for(record, field) {
                    Ok(id) => Some(id),
                    Err(err) => {
                        failures.insert(index, err);
                        continue;
                    }
                },
                None => None,
            };
            encoded.push(Encoded {
                index,
                body,
                group_id: self.config.message_group_id.clone(),
                dedup_id,
            });
        }
        (encoded, failures)
    }

    /// Send one chunk, retrying per-entry partial failures with backoff.
    /// Returns per-entry outcomes keyed by input index; a whole-request failure
    /// that outlives the retry budget is the outer `Err`.
    async fn put_chunk(
        &self,
        mut pending: Vec<Encoded>,
    ) -> Result<BTreeMap<usize, Result<(), FaucetError>>, FaucetError> {
        let retry_cfg = self.config.retry_spec();
        let mut outcomes: BTreeMap<usize, Result<(), FaucetError>> = BTreeMap::new();
        let mut attempt = 0usize;
        loop {
            let entries: Vec<SendMessageBatchRequestEntry> = pending
                .iter()
                .map(|e| {
                    let mut b = SendMessageBatchRequestEntry::builder()
                        .id(e.index.to_string())
                        .message_body(&e.body);
                    if let Some(g) = &e.group_id {
                        b = b.message_group_id(g);
                    }
                    if let Some(d) = &e.dedup_id {
                        b = b.message_deduplication_id(d);
                    }
                    b.build()
                        .map_err(|err| FaucetError::Sink(format!("sqs: entry build failed: {err}")))
                })
                .collect::<Result<_, _>>()?;

            let response = self
                .client
                .send_message_batch()
                .queue_url(&self.config.queue_url)
                .set_entries(Some(entries))
                .send()
                .await;

            match response {
                Ok(out) => {
                    for s in out.successful() {
                        if let Ok(idx) = s.id().parse::<usize>() {
                            outcomes.insert(idx, Ok(()));
                        }
                    }
                    let mut retry_indices: HashSet<usize> = HashSet::new();
                    for f in out.failed() {
                        let Ok(idx) = f.id().parse::<usize>() else {
                            continue;
                        };
                        // sender_fault ⇒ a client-side problem that will not
                        // succeed on retry; treat as permanent.
                        if f.sender_fault() || attempt + 1 >= retry_cfg.max_attempts {
                            outcomes.insert(
                                idx,
                                Err(FaucetError::Sink(format!(
                                    "sqs: message rejected after {} attempt(s): {}: {}",
                                    attempt + 1,
                                    f.code(),
                                    f.message().unwrap_or("(no message)")
                                ))),
                            );
                        } else {
                            retry_indices.insert(idx);
                        }
                    }
                    if retry_indices.is_empty() {
                        return Ok(outcomes);
                    }
                    pending.retain(|e| retry_indices.contains(&e.index));
                    attempt += 1;
                    let delay = backoff_delay(
                        retry_cfg.initial_backoff_ms,
                        retry_cfg.max_backoff_ms,
                        attempt,
                    );
                    tracing::debug!(
                        queue = %self.config.queue_url,
                        retrying = pending.len(),
                        attempt,
                        delay_ms = delay.as_millis() as u64,
                        "sqs: retrying partially-failed SendMessageBatch entries"
                    );
                    tokio::time::sleep(delay).await;
                }
                Err(err) => {
                    attempt += 1;
                    let service = err.into_service_error();
                    if attempt >= retry_cfg.max_attempts {
                        return Err(FaucetError::Sink(format!(
                            "sqs: SendMessageBatch to '{}' failed after {} attempt(s): {service}",
                            self.config.queue_url, retry_cfg.max_attempts
                        )));
                    }
                    let delay = backoff_delay(
                        retry_cfg.initial_backoff_ms,
                        retry_cfg.max_backoff_ms,
                        attempt,
                    );
                    tracing::warn!(
                        queue = %self.config.queue_url,
                        error = %service,
                        attempt,
                        delay_ms = delay.as_millis() as u64,
                        "sqs: SendMessageBatch request failed; retrying"
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    /// Ship all encoded entries with bounded request concurrency, merging
    /// per-entry outcomes.
    async fn ship(
        &self,
        encoded: Vec<Encoded>,
    ) -> Result<BTreeMap<usize, Result<(), FaucetError>>, FaucetError> {
        use futures::StreamExt;
        let chunks = chunk_requests(
            encoded,
            self.config.batch_size,
            crate::config::MAX_BATCH_BYTES,
        );
        let mut merged: BTreeMap<usize, Result<(), FaucetError>> = BTreeMap::new();
        let mut stream =
            futures::stream::iter(chunks.into_iter().map(|chunk| self.put_chunk(chunk)))
                .buffer_unordered(self.config.concurrency);
        while let Some(result) = stream.next().await {
            merged.extend(result?);
        }
        Ok(merged)
    }
}

#[faucet_core::async_trait]
impl faucet_core::Sink for SqsSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        let (encoded, encode_failures) = self.encode_records(records);
        let outcomes = self.ship(encoded).await?;
        let delivered = outcomes.values().filter(|o| o.is_ok()).count();
        let failed = encode_failures.len() + outcomes.len() - delivered;
        if failed > 0 {
            let first = encode_failures
                .values()
                .next()
                .map(ToString::to_string)
                .or_else(|| {
                    outcomes
                        .values()
                        .find_map(|o| o.as_ref().err().map(ToString::to_string))
                })
                .unwrap_or_default();
            return Err(FaucetError::Sink(format!(
                "sqs: {failed} of {} record(s) failed (first: {first})",
                records.len()
            )));
        }
        tracing::info!(
            queue = %self.config.queue_url,
            records = delivered,
            "sqs sink write complete"
        );
        Ok(delivered)
    }

    /// Per-row outcomes in input order: encode failures, oversized bodies, and
    /// per-entry `SendMessageBatch` rejections (after the retry budget) come
    /// back as `Err` rows for the DLQ router; a whole-request failure that
    /// outlives every retry propagates as the outer `Err`.
    async fn write_batch_partial(&self, records: &[Value]) -> Result<Vec<RowOutcome>, FaucetError> {
        let (encoded, mut encode_failures) = self.encode_records(records);
        let mut shipped = self.ship(encoded).await?;
        Ok((0..records.len())
            .map(|i| {
                if let Some(err) = encode_failures.remove(&i) {
                    Err(err)
                } else {
                    shipped.remove(&i).unwrap_or(Ok(()))
                }
            })
            .collect())
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(SqsSinkConfig)).expect("schema serialization")
    }

    fn connector_name(&self) -> &'static str {
        "sqs"
    }

    fn dataset_uri(&self) -> String {
        let name = self
            .config
            .queue_url
            .rsplit('/')
            .find(|s| !s.is_empty())
            .unwrap_or(self.config.queue_url.as_str());
        format!(
            "sqs://{}/{}",
            self.config.region.as_deref().unwrap_or("default"),
            name
        )
    }

    /// Side-effect-free probe: `GetQueueAttributes` (no messages written).
    async fn check(
        &self,
        ctx: &faucet_core::CheckContext,
    ) -> Result<faucet_core::CheckReport, FaucetError> {
        use faucet_core::{CheckReport, Probe};
        let start = std::time::Instant::now();
        let fut = self
            .client
            .get_queue_attributes()
            .queue_url(&self.config.queue_url)
            .send();
        let probe = match tokio::time::timeout(ctx.timeout, fut).await {
            Err(_) => Probe::fail("get_queue_attributes", start.elapsed(), "timed out"),
            Ok(Ok(_)) => Probe::pass("get_queue_attributes", start.elapsed()),
            Ok(Err(e)) => Probe::fail(
                "get_queue_attributes",
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
    use serde_json::json;

    fn enc(index: usize, bytes: usize) -> Encoded {
        Encoded {
            index,
            body: "x".repeat(bytes),
            group_id: None,
            dedup_id: None,
        }
    }

    /// Assert a jittered delay sits in core's documented `[0.5, 1.5)` band
    /// around `expected_ms` and never collapses to zero (a zero sleep would
    /// busy-spin the retry loop).
    fn assert_jittered_around(actual: Duration, expected_ms: u64) {
        let lo = Duration::from_micros(expected_ms * 500);
        let hi = Duration::from_micros(expected_ms * 1500);
        assert!(
            actual >= lo && actual < hi,
            "delay {actual:?} outside jitter band [{lo:?}, {hi:?}) for {expected_ms}ms"
        );
        assert!(!actual.is_zero(), "jittered delay collapsed to zero");
    }

    #[test]
    fn backoff_doubles_and_caps_within_the_jitter_band() {
        assert_jittered_around(backoff_delay(100, 30_000, 0), 100);
        assert_jittered_around(backoff_delay(100, 30_000, 1), 200);
        assert_jittered_around(backoff_delay(100, 30_000, 4), 1_600);
        // Capped at the configured ceiling, not core's own 60s cap.
        assert_jittered_around(backoff_delay(100, 30_000, 20), 30_000);
        assert_jittered_around(backoff_delay(100, 30_000, usize::MAX), 30_000);
        // A tight configured ceiling is still honoured.
        assert_jittered_around(backoff_delay(100, 250, 9), 250);
    }

    #[test]
    fn backoff_is_decorrelated_across_calls() {
        // The whole point of #654 M13: every failed entry of a batch must not
        // retry in lockstep. Identical inputs must not yield an identical delay.
        let delays: Vec<Duration> = (0..32).map(|_| backoff_delay(100, 30_000, 3)).collect();
        assert!(
            delays.iter().any(|d| *d != delays[0]),
            "every delay identical — jitter is not being applied: {delays:?}"
        );
    }

    #[test]
    fn chunker_honours_count_and_byte_limits() {
        // Count limit.
        let entries: Vec<Encoded> = (0..7).map(|i| enc(i, 10)).collect();
        let chunks = chunk_requests(entries, 3, 1_000_000);
        assert_eq!(chunks.iter().map(Vec::len).collect::<Vec<_>>(), [3, 3, 1]);

        // Byte limit: each entry is 10 bytes.
        let entries: Vec<Encoded> = (0..6).map(|i| enc(i, 10)).collect();
        let chunks = chunk_requests(entries, 10, 20);
        assert_eq!(chunks.iter().map(Vec::len).collect::<Vec<_>>(), [2, 2, 2]);

        assert!(chunk_requests(Vec::new(), 10, 100).is_empty());
    }

    #[test]
    fn dedup_id_extraction() {
        assert_eq!(
            dedup_id_for(&json!({"id": "abc"}), "id").unwrap(),
            "abc".to_string()
        );
        assert_eq!(dedup_id_for(&json!({"id": 42}), "id").unwrap(), "42");
        assert_eq!(dedup_id_for(&json!({"id": true}), "id").unwrap(), "true");
        assert!(dedup_id_for(&json!({"other": 1}), "id").is_err());
        assert!(dedup_id_for(&json!({"id": null}), "id").is_err());
        assert!(dedup_id_for(&json!({"id": {"nested": 1}}), "id").is_err());
    }

    async fn offline_sink(mut config: SqsSinkConfig) -> SqsSink {
        config.endpoint_url = Some("http://127.0.0.1:1".into());
        config.region = Some("us-east-1".into());
        config.credentials = faucet_common_sqs::SqsCredentials::AccessKey {
            access_key_id: "test".into(),
            secret_access_key: "test".into(),
            session_token: None,
        };
        SqsSink::new(config).await.expect("sink builds")
    }

    #[tokio::test]
    async fn encode_records_partitions_failures_by_index() {
        let mut cfg = SqsSinkConfig::new("https://q");
        cfg.message_deduplication_id_field = Some("id".into());
        let sink = offline_sink(cfg).await;

        let records = vec![
            json!({"id": "a", "v": 1}), // ok
            json!({"v": 2}),            // missing dedup field
            json!({"id": "c", "v": 3}), // ok
        ];
        let (encoded, failures) = sink.encode_records(&records);
        assert_eq!(encoded.len(), 2);
        assert_eq!(encoded[0].index, 0);
        assert_eq!(encoded[1].index, 2);
        assert_eq!(encoded[0].dedup_id.as_deref(), Some("a"));
        assert_eq!(failures.len(), 1);
        assert!(failures[&1].to_string().contains("dedup field"));
    }

    #[tokio::test]
    async fn oversized_body_is_a_per_record_failure() {
        let sink = offline_sink(SqsSinkConfig::new("https://q")).await;
        let big = json!({ "blob": "x".repeat(crate::config::MAX_MESSAGE_BYTES) });
        let (encoded, failures) = sink.encode_records(&[big]);
        assert!(encoded.is_empty());
        assert!(failures[&0].to_string().contains("byte limit"));
    }

    #[tokio::test]
    async fn identity_overrides_and_empty_write() {
        use faucet_core::Sink as _;
        let sink = offline_sink(SqsSinkConfig::new(
            "https://sqs.us-east-1.amazonaws.com/1/events",
        ))
        .await;
        assert_eq!(sink.connector_name(), "sqs");
        assert_eq!(sink.dataset_uri(), "sqs://us-east-1/events");
        assert!(!sink.supports_idempotent_writes());
        assert!(!sink.dedups_by_key());
        assert_eq!(
            sink.supported_write_modes(),
            &[faucet_core::WriteMode::Append]
        );
        assert_eq!(sink.write_batch(&[]).await.unwrap(), 0, "empty is a no-op");
        let schema = sink.config_schema();
        assert!(schema["properties"]["queue_url"].is_object());
    }

    #[tokio::test]
    async fn whole_request_failure_propagates_as_outer_err() {
        use faucet_core::Sink as _;
        let mut cfg = SqsSinkConfig::new("https://q");
        cfg.retry_max_attempts = 1; // fail fast against the unroutable endpoint
        cfg.retry_initial_backoff_ms = 1;
        let sink = offline_sink(cfg).await;
        let err = sink.write_batch(&[json!({"a": 1})]).await.unwrap_err();
        assert!(err.to_string().contains("SendMessageBatch"), "{err}");
        let err = sink
            .write_batch_partial(&[json!({"a": 1})])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("SendMessageBatch"), "{err}");
    }

    #[tokio::test]
    async fn encode_only_failures_need_no_request() {
        use faucet_core::Sink as _;
        // Every record fails encoding (missing dedup field), so no network
        // request is attempted → per-record errors, outer Ok.
        let mut cfg = SqsSinkConfig::new("https://q");
        cfg.message_deduplication_id_field = Some("id".into());
        let sink = offline_sink(cfg).await;
        let outcomes = sink
            .write_batch_partial(&[json!({"no": "id"}), json!({"also": "none"})])
            .await
            .expect("no request attempted");
        assert_eq!(outcomes.len(), 2);
        assert!(outcomes.iter().all(Result::is_err));
    }

    #[tokio::test]
    async fn check_probe_fails_cleanly_offline() {
        use faucet_core::Sink as _;
        let sink = offline_sink(SqsSinkConfig::new("https://q")).await;
        let report = sink
            .check(&faucet_core::CheckContext {
                timeout: Duration::from_millis(500),
            })
            .await
            .unwrap();
        assert_eq!(report.failed_count(), 1);
    }
}
