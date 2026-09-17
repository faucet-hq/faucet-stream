//! Per-shard consumer: iterator lifecycle, the `GetRecords` loop,
//! throttle-aware backoff, and payload decoding. The pure pieces (backoff
//! schedule, iterator-type mapping, record assembly) are separated from the
//! I/O loop so they unit-test offline.

use crate::config::{KinesisSourceConfig, StartPosition, ValueFormat};
use aws_sdk_kinesis::Client;
use aws_sdk_kinesis::types::ShardIteratorType;
use faucet_core::FaucetError;
use serde_json::{Value, json};
use std::time::Duration;

/// Retry budget for non-throughput `GetRecords`/`GetShardIterator` errors.
/// Throughput throttling (`ProvisionedThroughputExceededException`) backs off
/// without consuming this budget — it is expected steady-state behavior at
/// the 2 MB/s/shard read cap, never an error.
pub(crate) const ERROR_RETRY_BUDGET: u32 = 8;

/// Hard cap on one retry sleep (before jitter). Bounds the exponential so a
/// long throttle can't stall a shard for minutes while its siblings drain.
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Exponential backoff for `attempt` consecutive failures — `base * 2^attempt`
/// capped at [`MAX_BACKOFF`] — jittered through core's shared decorrelated
/// helper.
///
/// The previous per-shard jitter was a *fixed* 0–25% fraction hashed from the
/// shard id, which does not break a herd: after one shared throttle event every
/// shard's retry still landed inside the same narrow window, in the same
/// relative order, on every attempt. [`faucet_core::retry::apply_jitter`] draws
/// a fresh `[0.5, 1.5)` factor per call, decorrelated even between two shards
/// retrying in the same nanosecond (#654 M13).
pub(crate) fn backoff_delay(base: Duration, attempt: u32) -> Duration {
    let exp = base
        .saturating_mul(2u32.saturating_pow(attempt))
        .min(MAX_BACKOFF);
    faucet_core::retry::apply_jitter(exp)
}

/// Where a shard's iterator starts: the persisted bookmark wins, else the
/// configured start position. Pure.
pub(crate) fn iterator_request(
    start: &StartPosition,
    bookmarked_sequence: Option<&str>,
) -> (ShardIteratorType, Option<String>, Option<i64>) {
    if let Some(seq) = bookmarked_sequence {
        return (
            ShardIteratorType::AfterSequenceNumber,
            Some(seq.to_string()),
            None,
        );
    }
    match start {
        StartPosition::TrimHorizon => (ShardIteratorType::TrimHorizon, None, None),
        StartPosition::Latest => (ShardIteratorType::Latest, None, None),
        StartPosition::AtTimestamp { timestamp_secs } => {
            (ShardIteratorType::AtTimestamp, None, Some(*timestamp_secs))
        }
        StartPosition::AtSequenceNumber { sequence } => (
            ShardIteratorType::AtSequenceNumber,
            Some(sequence.clone()),
            None,
        ),
        StartPosition::AfterSequenceNumber { sequence } => (
            ShardIteratorType::AfterSequenceNumber,
            Some(sequence.clone()),
            None,
        ),
    }
}

/// Decode one Kinesis record payload per the configured format. Pure.
pub(crate) fn decode_payload(
    data: &[u8],
    format: ValueFormat,
    shard_id: &str,
    sequence: &str,
) -> Result<Value, FaucetError> {
    match format {
        ValueFormat::Json => serde_json::from_slice(data).map_err(|e| {
            FaucetError::Source(format!(
                "kinesis: record {sequence} in {shard_id} is not valid JSON: {e}"
            ))
        }),
        ValueFormat::String => match std::str::from_utf8(data) {
            Ok(s) => Ok(Value::String(s.to_string())),
            Err(e) => Err(FaucetError::Source(format!(
                "kinesis: record {sequence} in {shard_id} is not valid UTF-8: {e}"
            ))),
        },
        ValueFormat::Bytes => {
            use base64::Engine as _;
            Ok(Value::String(
                base64::engine::general_purpose::STANDARD.encode(data),
            ))
        }
    }
}

/// Assemble the emitted record: decoded payload + Kinesis metadata. Pure.
pub(crate) fn assemble_record(
    payload: Value,
    partition_key: &str,
    sequence: &str,
    shard_id: &str,
    arrival_ms: Option<i64>,
) -> Value {
    json!({
        "data": payload,
        "partition_key": partition_key,
        "sequence_number": sequence,
        "shard_id": shard_id,
        "approximate_arrival_timestamp_ms": arrival_ms,
    })
}

/// A chunk of decoded records from one shard, or the worker's terminal state.
#[derive(Debug)]
pub(crate) enum ShardEvent {
    /// Decoded records paired with their per-record sequence numbers, in shard
    /// order. The consumer advances the shard bookmark **per record** as it
    /// buffers, so any emitted page's bookmark equals the sequence of its last
    /// included record — never a batch-final sequence that runs ahead of records
    /// still buffered but not yet emitted (audit #321 C2).
    Records {
        shard_id: String,
        records: Vec<(String, Value)>,
    },
    /// The shard is fully consumed (closed shard reached its end).
    Done { shard_id: String },
    /// Unrecoverable failure after the retry budget.
    Failed {
        shard_id: String,
        error: FaucetError,
    },
}

/// Run one shard's consume loop, pushing [`ShardEvent`]s into `tx`. Exits when
/// the shard closes, the channel drops (main loop finished), or retries are
/// exhausted.
pub(crate) async fn run_shard(
    client: Client,
    config: KinesisSourceConfig,
    shard_id: String,
    bookmarked_sequence: Option<String>,
    tx: tokio::sync::mpsc::Sender<ShardEvent>,
) {
    let poll_interval = config.poll_interval();
    let mut last_sequence = bookmarked_sequence.clone();
    let mut error_attempts: u32 = 0;

    let mut iterator =
        match acquire_iterator(&client, &config, &shard_id, last_sequence.as_deref()).await {
            Ok(it) => it,
            Err(error) => {
                let _ = tx.send(ShardEvent::Failed { shard_id, error }).await;
                return;
            }
        };

    loop {
        let Some(current) = iterator.clone() else {
            // NextShardIterator == None → the shard is closed and drained.
            let _ = tx.send(ShardEvent::Done { shard_id }).await;
            return;
        };

        match client
            .get_records()
            .shard_iterator(&current)
            .limit(config.records_per_request as i32)
            .send()
            .await
        {
            Ok(out) => {
                error_attempts = 0;
                let records = out.records();
                if !records.is_empty() {
                    let mut decoded = Vec::with_capacity(records.len());
                    for r in records {
                        let sequence = r.sequence_number();
                        let partition_key = r.partition_key();
                        let arrival_ms = r
                            .approximate_arrival_timestamp()
                            .map(|t| t.to_millis().unwrap_or_default());
                        let payload = match decode_payload(
                            r.data().as_ref(),
                            config.value_format,
                            &shard_id,
                            sequence,
                        ) {
                            Ok(p) => p,
                            Err(error) => {
                                let _ = tx.send(ShardEvent::Failed { shard_id, error }).await;
                                return;
                            }
                        };
                        last_sequence = Some(sequence.to_string());
                        decoded.push((
                            sequence.to_string(),
                            assemble_record(
                                payload,
                                partition_key,
                                sequence,
                                &shard_id,
                                arrival_ms,
                            ),
                        ));
                    }
                    if tx
                        .send(ShardEvent::Records {
                            shard_id: shard_id.clone(),
                            records: decoded,
                        })
                        .await
                        .is_err()
                    {
                        return; // main loop finished (max_messages / cancel)
                    }
                }
                iterator = out.next_shard_iterator().map(str::to_string);
                // Behind records arrive back-to-back; caught-up shards wait
                // the poll interval (the 5 reads/sec/shard API budget).
                let behind = out.millis_behind_latest().unwrap_or(0);
                if records.is_empty() || behind == 0 {
                    tokio::time::sleep(poll_interval).await;
                }
            }
            Err(err) => {
                let service = err.into_service_error();
                if service.is_provisioned_throughput_exceeded_exception() {
                    // Expected at the 2 MB/s/shard cap — back off, don't fail.
                    let delay = backoff_delay(poll_interval, error_attempts.min(4));
                    tracing::debug!(shard = %shard_id, delay_ms = delay.as_millis() as u64,
                        "kinesis: throughput exceeded; backing off");
                    tokio::time::sleep(delay).await;
                    continue;
                }
                if service.is_expired_iterator_exception() {
                    tracing::debug!(shard = %shard_id, "kinesis: iterator expired; re-acquiring");
                    match acquire_iterator(&client, &config, &shard_id, last_sequence.as_deref())
                        .await
                    {
                        Ok(it) => {
                            iterator = it;
                            continue;
                        }
                        Err(error) => {
                            let _ = tx.send(ShardEvent::Failed { shard_id, error }).await;
                            return;
                        }
                    }
                }
                error_attempts += 1;
                if error_attempts >= ERROR_RETRY_BUDGET {
                    let _ = tx
                        .send(ShardEvent::Failed {
                            shard_id: shard_id.clone(),
                            error: FaucetError::Source(format!(
                                "kinesis: GetRecords on {shard_id} failed after \
                                 {ERROR_RETRY_BUDGET} attempts: {service}"
                            )),
                        })
                        .await;
                    return;
                }
                let delay = backoff_delay(poll_interval, error_attempts);
                tracing::warn!(shard = %shard_id, error = %service,
                    attempt = error_attempts, delay_ms = delay.as_millis() as u64,
                    "kinesis: GetRecords failed; retrying");
                tokio::time::sleep(delay).await;
            }
        }
    }
}

/// Acquire a shard iterator at the bookmark (if any) or start position.
async fn acquire_iterator(
    client: &Client,
    config: &KinesisSourceConfig,
    shard_id: &str,
    bookmarked_sequence: Option<&str>,
) -> Result<Option<String>, FaucetError> {
    let (iterator_type, sequence, timestamp_secs) =
        iterator_request(&config.start_position, bookmarked_sequence);
    let mut req = client
        .get_shard_iterator()
        .stream_name(&config.stream_name)
        .shard_id(shard_id)
        .shard_iterator_type(iterator_type);
    if let Some(seq) = sequence {
        req = req.starting_sequence_number(seq);
    }
    if let Some(secs) = timestamp_secs {
        req = req.timestamp(aws_sdk_kinesis::primitives::DateTime::from_secs(secs));
    }
    let out = req.send().await.map_err(|e| {
        FaucetError::Source(format!(
            "kinesis: GetShardIterator for {shard_id} failed: {}",
            e.into_service_error()
        ))
    })?;
    Ok(out.shard_iterator().map(str::to_string))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Assert a jittered delay sits in core's documented `[0.5, 1.5)` band
    /// around `expected` and never collapses to zero (a zero sleep would turn
    /// the throttle backoff into a busy-spin against the 5 reads/sec/shard
    /// budget).
    fn assert_jittered_around(actual: Duration, expected: Duration) {
        let lo = expected.mul_f64(0.5);
        let hi = expected.mul_f64(1.5);
        assert!(
            actual >= lo && actual < hi,
            "delay {actual:?} outside jitter band [{lo:?}, {hi:?}) for {expected:?}"
        );
        assert!(!actual.is_zero(), "jittered delay collapsed to zero");
    }

    #[test]
    fn backoff_doubles_and_caps_within_the_jitter_band() {
        let base = Duration::from_millis(1000);
        assert_jittered_around(backoff_delay(base, 0), Duration::from_secs(1));
        assert_jittered_around(backoff_delay(base, 1), Duration::from_secs(2));
        assert_jittered_around(backoff_delay(base, 3), Duration::from_secs(8));
        // Capped at MAX_BACKOFF, including when `2^attempt` saturates.
        assert_jittered_around(backoff_delay(base, 10), MAX_BACKOFF);
        assert_jittered_around(backoff_delay(base, u32::MAX), MAX_BACKOFF);
    }

    #[test]
    fn backoff_is_decorrelated_across_calls() {
        // Shards recovering from one shared throttle event must not re-fire in
        // lockstep — the fixed per-shard fraction this replaced always did
        // (#654 M13). Identical inputs must not yield an identical delay.
        let base = Duration::from_millis(1000);
        let delays: Vec<Duration> = (0..32).map(|_| backoff_delay(base, 2)).collect();
        assert!(
            delays.iter().any(|d| *d != delays[0]),
            "every delay identical — jitter is not being applied: {delays:?}"
        );
    }

    #[test]
    fn iterator_request_maps_positions_and_prefers_bookmark() {
        // Bookmark always wins.
        let (t, seq, ts) = iterator_request(&StartPosition::TrimHorizon, Some("42"));
        assert_eq!(t, ShardIteratorType::AfterSequenceNumber);
        assert_eq!(seq.as_deref(), Some("42"));
        assert!(ts.is_none());

        let (t, seq, ts) = iterator_request(&StartPosition::TrimHorizon, None);
        assert_eq!(t, ShardIteratorType::TrimHorizon);
        assert!(seq.is_none() && ts.is_none());

        let (t, ..) = iterator_request(&StartPosition::Latest, None);
        assert_eq!(t, ShardIteratorType::Latest);

        let (t, seq, ts) = iterator_request(
            &StartPosition::AtTimestamp {
                timestamp_secs: 1_716_700_000,
            },
            None,
        );
        assert_eq!(t, ShardIteratorType::AtTimestamp);
        assert!(seq.is_none());
        assert_eq!(ts, Some(1_716_700_000));

        let (t, seq, _) = iterator_request(
            &StartPosition::AtSequenceNumber {
                sequence: "9".into(),
            },
            None,
        );
        assert_eq!(t, ShardIteratorType::AtSequenceNumber);
        assert_eq!(seq.as_deref(), Some("9"));

        let (t, seq, _) = iterator_request(
            &StartPosition::AfterSequenceNumber {
                sequence: "9".into(),
            },
            None,
        );
        assert_eq!(t, ShardIteratorType::AfterSequenceNumber);
        assert_eq!(seq.as_deref(), Some("9"));
    }

    #[test]
    fn payload_decodes_per_format() {
        let json_ok = decode_payload(br#"{"a":1}"#, ValueFormat::Json, "s", "1").unwrap();
        assert_eq!(json_ok["a"], 1);
        let json_err = decode_payload(b"not json", ValueFormat::Json, "shardId-x", "42");
        let msg = json_err.unwrap_err().to_string();
        assert!(msg.contains("shardId-x") && msg.contains("42"), "{msg}");

        let s = decode_payload("héllo".as_bytes(), ValueFormat::String, "s", "1").unwrap();
        assert_eq!(s, Value::String("héllo".into()));
        assert!(decode_payload(&[0xff, 0xfe], ValueFormat::String, "s", "1").is_err());

        let b = decode_payload(&[1, 2, 3], ValueFormat::Bytes, "s", "1").unwrap();
        assert_eq!(b, Value::String("AQID".into()));
    }

    #[test]
    fn record_assembly_shape() {
        let r = assemble_record(
            serde_json::json!({"x": 1}),
            "user-7",
            "495",
            "shardId-000000000002",
            Some(1_716_700_000_123),
        );
        assert_eq!(r["data"]["x"], 1);
        assert_eq!(r["partition_key"], "user-7");
        assert_eq!(r["sequence_number"], "495");
        assert_eq!(r["shard_id"], "shardId-000000000002");
        assert_eq!(r["approximate_arrival_timestamp_ms"], 1_716_700_000_123i64);
    }
}
