//! The Kinesis `Sink` implementation: encode → size-aware chunking →
//! bounded-concurrency `PutRecords` → per-entry partial-failure retry.

use crate::config::KinesisSinkConfig;
use crate::partition::{derive_explicit_hash_key, derive_partition_key, encode_payload};
use aws_sdk_kinesis::Client;
use aws_sdk_kinesis::primitives::Blob;
use aws_sdk_kinesis::types::PutRecordsRequestEntry;
use faucet_core::{FaucetError, RowOutcome};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

/// Error codes a `PutRecords` request may succeed on when retried.
const RETRIABLE_CODES: &[&str] = &[
    "ProvisionedThroughputExceededException",
    "LimitExceededException",
    "KMSThrottlingException",
    "ThrottlingException",
    "InternalFailure",
    "ServiceUnavailable",
];

/// Whether a failed request is worth retrying: a transport failure or
/// timeout, an HTTP 5xx/429, or a throttling/availability error code. A
/// missing stream or denied access fails at once instead of burning the retry
/// budget (#789 MSG-91). Pure.
pub(crate) fn retriable_request(status: Option<u16>, code: Option<&str>, transport: bool) -> bool {
    transport
        || matches!(status, Some(429) | Some(500..=599))
        || code.is_some_and(|c| RETRIABLE_CODES.contains(&c))
}

impl Encoded {
    /// What Kinesis orders by: the explicit hash key when set (it picks the
    /// shard), else the partition key.
    pub(crate) fn order_key(&self) -> &str {
        self.hash_key.as_deref().unwrap_or(&self.partition_key)
    }
}

/// Split entries into `lanes` request streams so every entry with one order
/// key lands in the same lane, in input order. Lanes run concurrently; one
/// lane's requests run one after another, so a key's records are never in two
/// requests at once (#789 MSG-37). Pure.
pub(crate) fn lanes(entries: Vec<Encoded>, lanes: usize) -> Vec<Vec<Encoded>> {
    use std::hash::{Hash, Hasher};
    let n = lanes.max(1);
    let mut out: Vec<Vec<Encoded>> = (0..n).map(|_| Vec::new()).collect();
    for e in entries {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        e.order_key().hash(&mut h);
        out[(h.finish() % n as u64) as usize].push(e);
    }
    out.retain(|l| !l.is_empty());
    out
}

/// After a partial failure, the entries to send again: each failed entry and
/// every later entry of its order key in the request — Kinesis may have
/// written those ahead of it, and re-sending them after it restores the key's
/// order for consumers (#789 MSG-37). Pure.
pub(crate) fn resend_set(pending: &[Encoded], failed: &HashSet<usize>) -> HashSet<usize> {
    let mut first: HashMap<&str, usize> = HashMap::new();
    for e in pending {
        if failed.contains(&e.index) {
            first.entry(e.order_key()).or_insert(e.index);
        }
    }
    pending
        .iter()
        .filter(|e| first.get(e.order_key()).is_some_and(|&f| e.index >= f))
        .map(|e| e.index)
        .collect()
}

/// One record encoded and ready to ship, tagged with its input position so
/// outcomes can be reported in input order.
#[derive(Debug, Clone)]
pub(crate) struct Encoded {
    pub index: usize,
    pub partition_key: String,
    pub hash_key: Option<String>,
    pub data: Vec<u8>,
}

impl Encoded {
    /// Bytes this entry contributes to a request (data + partition key —
    /// the quantities Kinesis counts against its limits).
    pub fn request_bytes(&self) -> usize {
        self.data.len() + self.partition_key.len()
    }
}

/// Exponential backoff between the configured bounds — `initial * 2^attempt`
/// capped at `max_ms` — jittered through core's shared decorrelated helper.
///
/// The jitter is load-bearing, not cosmetic: a partially-failed `PutRecords`
/// can leave up to 500 entries to retry, and without decorrelation every one of
/// them (and every concurrent request) re-fires in the same instant, re-creating
/// the throttle that caused the failure. Delegating to
/// [`faucet_core::retry::apply_jitter`] also means the two jitter bugs fixed
/// there (a divisor that capped the factor at 0.733, and same-nanosecond
/// alignment across tasks) apply here rather than having to be rediscovered
/// (#654 M13).
pub(crate) fn backoff_delay(initial_ms: u64, max_ms: u64, attempt: usize) -> Duration {
    let exp = initial_ms.saturating_mul(1u64 << attempt.min(20));
    faucet_core::retry::apply_jitter(Duration::from_millis(exp.min(max_ms)))
}

/// Chunk encoded entries into `PutRecords` requests honouring both the entry
/// count and request byte ceilings. Pure. Entries above the byte ceiling are
/// assumed to have been filtered out already.
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

/// AWS Kinesis Data Streams sink. See the crate README for semantics.
pub struct KinesisSink {
    config: KinesisSinkConfig,
    client: Client,
}

impl KinesisSink {
    /// Create a new Kinesis sink. Validates the config and builds the AWS
    /// client (no network I/O).
    pub async fn new(config: KinesisSinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let client = faucet_common_kinesis::build_client(
            config.region.as_deref(),
            config.endpoint_url.as_deref(),
            &config.credentials,
        )
        .await?;
        Ok(Self { config, client })
    }

    /// Encode every record; per-record failures (bad payload for the format,
    /// unresolvable partition key, oversized) land in the error map.
    pub(crate) fn encode_records(
        &self,
        records: &[Value],
    ) -> (Vec<Encoded>, BTreeMap<usize, FaucetError>) {
        let mut encoded = Vec::with_capacity(records.len());
        let mut failures = BTreeMap::new();
        for (index, record) in records.iter().enumerate() {
            let one = derive_partition_key(record, &self.config.partition_key)
                .and_then(|partition_key| {
                    derive_explicit_hash_key(record, &self.config.explicit_hash_key)
                        .map(|hash_key| (partition_key, hash_key))
                })
                .and_then(|(partition_key, hash_key)| {
                    encode_payload(record, self.config.value_format).map(|data| Encoded {
                        index,
                        partition_key,
                        hash_key,
                        data,
                    })
                });
            match one {
                Ok(e) if e.request_bytes() > self.config.max_record_size_bytes => {
                    failures.insert(
                        index,
                        FaucetError::Sink(format!(
                            "kinesis: record is {} bytes (data + partition key), above \
                             max_record_size_bytes {} — never sent",
                            e.request_bytes(),
                            self.config.max_record_size_bytes
                        )),
                    );
                }
                Ok(e) => encoded.push(e),
                Err(err) => {
                    failures.insert(index, err);
                }
            }
        }
        (encoded, failures)
    }

    /// Send one chunk, retrying per-entry partial failures with backoff.
    /// Returns per-entry outcomes keyed by input index; a whole-request
    /// failure that outlives the retry budget is the outer `Err`.
    async fn put_chunk(
        &self,
        mut pending: Vec<Encoded>,
    ) -> Result<BTreeMap<usize, Result<(), FaucetError>>, FaucetError> {
        let retry_cfg = self.config.retry_spec();
        let mut outcomes: BTreeMap<usize, Result<(), FaucetError>> = BTreeMap::new();
        let mut attempt = 0usize;
        loop {
            let entries: Vec<PutRecordsRequestEntry> = pending
                .iter()
                .map(|e| {
                    let mut b = PutRecordsRequestEntry::builder()
                        .partition_key(&e.partition_key)
                        .data(Blob::new(e.data.clone()));
                    if let Some(h) = &e.hash_key {
                        b = b.explicit_hash_key(h);
                    }
                    b.build().map_err(|err| {
                        FaucetError::Sink(format!("kinesis: entry build failed: {err}"))
                    })
                })
                .collect::<Result<_, _>>()?;

            let response = self
                .client
                .put_records()
                .stream_name(&self.config.stream_name)
                .set_records(Some(entries))
                .send()
                .await;

            match response {
                Ok(out) => {
                    let results = out.records();
                    let failed: HashSet<usize> = pending
                        .iter()
                        .zip(results)
                        .filter(|(_, r)| r.error_code().is_some())
                        .map(|(e, _)| e.index)
                        .collect();
                    let resend = if attempt + 1 < retry_cfg.max_attempts {
                        resend_set(&pending, &failed)
                    } else {
                        HashSet::new()
                    };
                    let mut retry: Vec<Encoded> = Vec::new();
                    for (entry, result) in pending.iter().zip(results) {
                        if resend.contains(&entry.index) {
                            outcomes.remove(&entry.index);
                            retry.push(entry.clone());
                            continue;
                        }
                        match result.error_code() {
                            None => {
                                outcomes.insert(entry.index, Ok(()));
                            }
                            Some(code) => {
                                outcomes.insert(
                                    entry.index,
                                    Err(FaucetError::Sink(format!(
                                        "kinesis: record rejected after {} attempt(s): {code}: {}",
                                        attempt + 1,
                                        result.error_message().unwrap_or("(no message)")
                                    ))),
                                );
                            }
                        }
                    }
                    if retry.is_empty() {
                        return Ok(outcomes);
                    }
                    attempt += 1;
                    let delay = backoff_delay(
                        retry_cfg.initial_backoff_ms,
                        retry_cfg.max_backoff_ms,
                        attempt,
                    );
                    tracing::debug!(
                        stream = %self.config.stream_name,
                        retrying = retry.len(),
                        attempt,
                        delay_ms = delay.as_millis() as u64,
                        "kinesis: retrying partially-failed PutRecords entries"
                    );
                    tokio::time::sleep(delay).await;
                    pending = retry;
                }
                Err(err) => {
                    use aws_sdk_kinesis::error::ProvideErrorMetadata as _;
                    attempt += 1;
                    let transport = matches!(
                        err,
                        aws_sdk_kinesis::error::SdkError::DispatchFailure(_)
                            | aws_sdk_kinesis::error::SdkError::TimeoutError(_)
                            | aws_sdk_kinesis::error::SdkError::ResponseError(_)
                    );
                    let status = err.raw_response().map(|r| r.status().as_u16());
                    let retriable = retriable_request(status, err.code(), transport);
                    let service = err.into_service_error();
                    if !retriable || attempt >= retry_cfg.max_attempts {
                        return Err(FaucetError::Sink(format!(
                            "kinesis: PutRecords to '{}' failed after {attempt} attempts: {service}",
                            self.config.stream_name
                        )));
                    }
                    let delay = backoff_delay(
                        retry_cfg.initial_backoff_ms,
                        retry_cfg.max_backoff_ms,
                        attempt,
                    );
                    tracing::warn!(
                        stream = %self.config.stream_name,
                        error = %service,
                        attempt,
                        delay_ms = delay.as_millis() as u64,
                        "kinesis: PutRecords request failed; retrying"
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
        let mut merged: BTreeMap<usize, Result<(), FaucetError>> = BTreeMap::new();
        let mut stream =
            futures::stream::iter(lanes(encoded, self.config.concurrency).into_iter().map(
                |lane| async move {
                    // One lane's requests in order; a key whose record failed for
                    // good fails its later records too, keeping its order intact.
                    let mut out: BTreeMap<usize, Result<(), FaucetError>> = BTreeMap::new();
                    let mut dead: HashSet<String> = HashSet::new();
                    for chunk in
                        chunk_requests(lane, self.config.batch_size, self.config.max_request_bytes)
                    {
                        let (send, skip): (Vec<Encoded>, Vec<Encoded>) = chunk
                            .into_iter()
                            .partition(|e| !dead.contains(e.order_key()));
                        for e in skip {
                            out.insert(
                                e.index,
                                Err(FaucetError::Sink(format!(
                                    "kinesis: not sent — an earlier record with key '{}' failed",
                                    e.order_key()
                                ))),
                            );
                        }
                        if send.is_empty() {
                            continue;
                        }
                        let keys: Vec<(usize, String)> = send
                            .iter()
                            .map(|e| (e.index, e.order_key().to_string()))
                            .collect();
                        let result = self.put_chunk(send).await?;
                        for (idx, key) in keys {
                            if result.get(&idx).is_some_and(Result::is_err) {
                                dead.insert(key);
                            }
                        }
                        out.extend(result);
                    }
                    Ok::<_, FaucetError>(out)
                },
            ))
            .buffer_unordered(self.config.concurrency);
        while let Some(result) = stream.next().await {
            merged.extend(result?);
        }
        Ok(merged)
    }
}

#[faucet_core::async_trait]
impl faucet_core::Sink for KinesisSink {
    fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        self.config.batch_atomicity()
    }

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
                "kinesis: {failed} of {} record(s) failed (first: {first})",
                records.len()
            )));
        }
        tracing::info!(
            stream = %self.config.stream_name,
            records = delivered,
            "kinesis sink write complete"
        );
        Ok(delivered)
    }

    /// Per-row outcomes in input order: encode failures, oversized records,
    /// and per-entry `PutRecords` rejections (after the retry budget) come
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
        serde_json::to_value(faucet_core::schema_for!(KinesisSinkConfig))
            .expect("schema serialization")
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

    /// Side-effect-free probe: `DescribeStreamSummary` (no records written).
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

    fn keyed(index: usize, key: &str) -> Encoded {
        Encoded {
            index,
            partition_key: key.into(),
            hash_key: None,
            data: Vec::new(),
        }
    }

    #[test]
    fn a_key_stays_in_one_lane_in_order() {
        let entries: Vec<Encoded> = (0..40).map(|i| keyed(i, &format!("k{}", i % 5))).collect();
        let lanes = lanes(entries, 3);
        assert!(lanes.len() <= 3);
        for k in 0..5 {
            let key = format!("k{k}");
            let holding: Vec<&Vec<Encoded>> = lanes
                .iter()
                .filter(|l| l.iter().any(|e| e.partition_key == key))
                .collect();
            assert_eq!(holding.len(), 1, "key {key} is in exactly one lane");
            let idx: Vec<usize> = holding[0]
                .iter()
                .filter(|e| e.partition_key == key)
                .map(|e| e.index)
                .collect();
            assert!(idx.windows(2).all(|w| w[0] < w[1]));
        }
        let mut with_hash = keyed(0, "p");
        with_hash.hash_key = Some("42".into());
        assert_eq!(with_hash.order_key(), "42");
    }

    #[test]
    fn a_failure_resends_the_later_records_of_its_key() {
        let pending = vec![keyed(0, "a"), keyed(1, "a"), keyed(2, "b"), keyed(3, "a")];
        let failed: HashSet<usize> = [1].into();
        let mut got: Vec<usize> = resend_set(&pending, &failed).into_iter().collect();
        got.sort();
        assert_eq!(got, vec![1, 3]);
    }

    #[test]
    fn only_transient_request_failures_are_retried() {
        assert!(retriable_request(None, None, true));
        assert!(retriable_request(Some(500), None, false));
        assert!(retriable_request(
            Some(400),
            Some("ProvisionedThroughputExceededException"),
            false
        ));
        assert!(!retriable_request(
            Some(400),
            Some("ResourceNotFoundException"),
            false
        ));
        assert!(!retriable_request(None, None, false));
    }
    use crate::config::{PartitionKey, ValueFormat};
    use serde_json::json;

    fn enc(index: usize, key: &str, bytes: usize) -> Encoded {
        Encoded {
            index,
            partition_key: key.to_string(),
            hash_key: None,
            data: vec![0u8; bytes],
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
        // A tight configured ceiling is still honoured (no unjittered floor
        // sneaking past the cap).
        assert_jittered_around(backoff_delay(100, 250, 9), 250);
    }

    #[test]
    fn backoff_is_decorrelated_across_calls() {
        // The whole point of #654 M13: up to 500 failed PutRecords entries must
        // not retry in lockstep. Identical inputs must not yield an identical
        // delay every time.
        let delays: Vec<Duration> = (0..32).map(|_| backoff_delay(100, 30_000, 3)).collect();
        assert!(
            delays.iter().any(|d| *d != delays[0]),
            "every delay identical — jitter is not being applied: {delays:?}"
        );
    }

    #[test]
    fn chunker_honours_count_and_byte_limits() {
        // Count limit.
        let entries: Vec<Encoded> = (0..7).map(|i| enc(i, "k", 10)).collect();
        let chunks = chunk_requests(entries, 3, 1_000_000);
        assert_eq!(chunks.iter().map(Vec::len).collect::<Vec<_>>(), [3, 3, 1]);

        // Byte limit: each entry is 11 bytes (10 data + 1 key).
        let entries: Vec<Encoded> = (0..6).map(|i| enc(i, "k", 10)).collect();
        let chunks = chunk_requests(entries, 500, 23);
        assert_eq!(chunks.iter().map(Vec::len).collect::<Vec<_>>(), [2, 2, 2]);

        // A single entry larger than the byte ceiling still ships alone
        // (the per-record ceiling is enforced upstream).
        let entries = vec![enc(0, "k", 100), enc(1, "k", 5)];
        let chunks = chunk_requests(entries, 500, 50);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].len(), 1);

        assert!(chunk_requests(Vec::new(), 500, 100).is_empty());
    }

    async fn offline_sink(mut config: KinesisSinkConfig) -> KinesisSink {
        config.endpoint_url = Some("http://127.0.0.1:1".into());
        config.region = Some("us-east-1".into());
        config.credentials = faucet_common_kinesis::KinesisCredentials::AccessKey {
            access_key_id: "test".into(),
            secret_access_key: "test".into(),
            session_token: None,
        };
        KinesisSink::new(config).await.expect("sink builds")
    }

    #[tokio::test]
    async fn encode_records_partitions_failures_by_index() {
        let mut cfg = KinesisSinkConfig::new("events");
        cfg.partition_key = PartitionKey::Field {
            name: "user_id".into(),
        };
        cfg.max_record_size_bytes = 64;
        let sink = offline_sink(cfg).await;

        let records = vec![
            json!({"user_id": "u1", "v": 1}),                 // ok
            json!({"v": 2}),                                  // no key field
            json!({"user_id": "u3", "big": "x".repeat(100)}), // oversized
            json!({"user_id": "u4", "v": 4}),                 // ok
        ];
        let (encoded, failures) = sink.encode_records(&records);
        assert_eq!(encoded.len(), 2);
        assert_eq!(encoded[0].index, 0);
        assert_eq!(encoded[1].index, 3);
        assert_eq!(failures.len(), 2);
        assert!(
            failures[&1].to_string().contains("user_id"),
            "{}",
            failures[&1]
        );
        assert!(failures[&2].to_string().contains("max_record_size_bytes"));
    }

    #[tokio::test]
    async fn identity_overrides_and_empty_write() {
        use faucet_core::Sink as _;
        let sink = offline_sink(KinesisSinkConfig::new("events")).await;
        assert_eq!(sink.connector_name(), "kinesis");
        assert_eq!(sink.dataset_uri(), "kinesis://us-east-1/events");
        assert!(!sink.supports_idempotent_writes());
        assert_eq!(
            sink.supported_write_modes(),
            &[faucet_core::WriteMode::Append]
        );
        assert_eq!(sink.write_batch(&[]).await.unwrap(), 0, "empty is a no-op");
        let schema = sink.config_schema();
        assert!(schema["properties"]["stream_name"].is_object());
    }

    #[tokio::test]
    async fn value_format_string_encode_failure_is_per_record() {
        use faucet_core::Sink as _;
        let mut cfg = KinesisSinkConfig::new("events");
        cfg.value_format = ValueFormat::String;
        cfg.retry_max_attempts = 1;
        let sink = offline_sink(cfg).await;
        // Every record fails ENCODING (an object under 'string' format), so no
        // network request is ever attempted → per-record errors, outer Ok.
        let outcomes = sink
            .write_batch_partial(&[json!({"not": "a string"}), json!({"also": 1})])
            .await
            .expect("no request attempted");
        assert_eq!(outcomes.len(), 2);
        assert!(outcomes.iter().all(Result::is_err));
    }

    #[tokio::test]
    async fn whole_request_failure_propagates_as_outer_err() {
        use faucet_core::Sink as _;
        let mut cfg = KinesisSinkConfig::new("events");
        cfg.retry_max_attempts = 1; // fail fast against the unroutable endpoint
        cfg.retry_initial_backoff_ms = 1;
        let sink = offline_sink(cfg).await;
        let err = sink.write_batch(&[json!({"a": 1})]).await.unwrap_err();
        assert!(err.to_string().contains("PutRecords"), "{err}");
        let err = sink
            .write_batch_partial(&[json!({"a": 1})])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("PutRecords"), "{err}");
    }

    #[tokio::test]
    async fn check_probe_fails_cleanly_offline() {
        use faucet_core::Sink as _;
        let sink = offline_sink(KinesisSinkConfig::new("events")).await;
        let report = sink
            .check(&faucet_core::CheckContext {
                timeout: Duration::from_millis(500),
            })
            .await
            .unwrap();
        assert_eq!(report.failed_count(), 1);
    }
}
