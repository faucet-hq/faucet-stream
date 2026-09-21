//! Configuration for the Kinesis source. No I/O here.

use faucet_common_kinesis::KinesisCredentials;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Where a shard starts reading when no persisted bookmark exists for it.
///
/// Serializes as `{ type: <position>, … }` (snake_case discriminators).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StartPosition {
    /// Oldest record still in the stream's retention window.
    #[default]
    TrimHorizon,
    /// Only records written after the consumer starts.
    Latest,
    /// First record at or after the timestamp (Kinesis matches at **second**
    /// granularity).
    AtTimestamp {
        /// Unix epoch seconds.
        timestamp_secs: i64,
    },
    /// The record with exactly this sequence number.
    AtSequenceNumber {
        /// Kinesis sequence number.
        sequence: String,
    },
    /// The record immediately after this sequence number.
    AfterSequenceNumber {
        /// Kinesis sequence number.
        sequence: String,
    },
}

/// How each record's `Data` blob is decoded into the emitted `data` field.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ValueFormat {
    /// Parse the payload as JSON. A record that is not valid JSON fails the
    /// stream with a typed error naming the shard + sequence number.
    #[default]
    Json,
    /// Decode the payload as UTF-8 (invalid UTF-8 fails the record).
    String,
    /// Base64-encode the raw payload bytes into a JSON string.
    Bytes,
}

/// Configuration for [`KinesisSource`](crate::KinesisSource).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct KinesisSourceConfig {
    /// Kinesis Data Stream name.
    pub stream_name: String,
    /// AWS region. `None` uses the SDK default chain (env, profile, IMDS).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    /// Custom endpoint URL for LocalStack / VPC endpoints.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_url: Option<String>,
    /// AWS credentials. Defaults to the SDK default provider chain.
    #[serde(default)]
    pub credentials: KinesisCredentials,

    /// Start position for shards with no persisted bookmark.
    #[serde(default)]
    pub start_position: StartPosition,
    /// Explicit shard-id allowlist; empty = every shard.
    #[serde(default)]
    pub shard_ids: Vec<String>,
    /// Also consume closed (post-resharding) shards still inside the
    /// retention window. Default `false` (open shards only).
    #[serde(default)]
    pub include_closed: bool,

    /// Base wait between `GetRecords` calls per shard, in milliseconds.
    /// Floored at 200 ms (the per-shard 5 reads/sec API limit). Default 1000.
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    /// `GetRecords` `Limit` per request (1–10000). Default 500.
    #[serde(default = "default_records_per_request")]
    pub records_per_request: usize,
    /// Max shards read concurrently. Default 4.
    #[serde(default = "default_shard_concurrency")]
    pub shard_concurrency: usize,

    /// Stop after this many seconds without a new record across all shards.
    /// At least one of `idle_termination_secs` and `max_messages` must be
    /// set (mirrors the Kafka source) — a batch run must terminate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_termination_secs: Option<u64>,
    /// Stop after this many records in total.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_messages: Option<usize>,

    /// Payload decoding for the record `data` field.
    #[serde(default)]
    pub value_format: ValueFormat,

    /// Records per emitted [`StreamPage`](faucet_core::StreamPage). `0` is
    /// the "no batching" sentinel: one page per drain cycle. Default 1000.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_poll_interval_ms() -> u64 {
    1000
}
fn default_records_per_request() -> usize {
    500
}
fn default_shard_concurrency() -> usize {
    4
}
fn default_batch_size() -> usize {
    faucet_core::DEFAULT_BATCH_SIZE
}

impl KinesisSourceConfig {
    /// Minimal config with defaults for everything but the stream name.
    pub fn new(stream_name: impl Into<String>) -> Self {
        Self {
            stream_name: stream_name.into(),
            region: None,
            endpoint_url: None,
            credentials: KinesisCredentials::default(),
            start_position: StartPosition::default(),
            shard_ids: Vec::new(),
            include_closed: false,
            poll_interval_ms: default_poll_interval_ms(),
            records_per_request: default_records_per_request(),
            shard_concurrency: default_shard_concurrency(),
            idle_termination_secs: None,
            max_messages: None,
            value_format: ValueFormat::default(),
            batch_size: default_batch_size(),
        }
    }

    /// Fail-fast validation, called from `KinesisSource::new`.
    pub fn validate(&self) -> Result<(), faucet_core::FaucetError> {
        use faucet_core::FaucetError;
        if self.stream_name.trim().is_empty() {
            return Err(FaucetError::Config(
                "kinesis source: stream_name must not be empty".into(),
            ));
        }
        faucet_core::validate_batch_size(self.batch_size)?;
        if self.records_per_request == 0 || self.records_per_request > 10_000 {
            return Err(FaucetError::Config(format!(
                "kinesis source: records_per_request must be 1..=10000 (got {})",
                self.records_per_request
            )));
        }
        if self.shard_concurrency == 0 {
            return Err(FaucetError::Config(
                "kinesis source: shard_concurrency must be at least 1".into(),
            ));
        }
        if self.idle_termination_secs.is_none() && self.max_messages.is_none() {
            return Err(FaucetError::Config(
                "kinesis source: set at least one of idle_termination_secs / max_messages so \
                 a run can terminate (mirrors the kafka source)"
                    .into(),
            ));
        }
        if self.idle_termination_secs == Some(0) {
            return Err(FaucetError::Config(
                "kinesis source: idle_termination_secs must be at least 1".into(),
            ));
        }
        if self.max_messages == Some(0) {
            return Err(FaucetError::Config(
                "kinesis source: max_messages must be at least 1".into(),
            ));
        }
        Ok(())
    }

    /// Effective per-shard poll interval (floored at 200 ms).
    pub fn poll_interval(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.poll_interval_ms.max(200))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid() -> KinesisSourceConfig {
        let mut c = KinesisSourceConfig::new("events");
        c.max_messages = Some(100);
        c
    }

    #[test]
    fn defaults_are_sensible() {
        let c = KinesisSourceConfig::new("events");
        assert_eq!(c.start_position, StartPosition::TrimHorizon);
        assert_eq!(c.value_format, ValueFormat::Json);
        assert_eq!(c.records_per_request, 500);
        assert_eq!(c.shard_concurrency, 4);
        assert_eq!(c.poll_interval_ms, 1000);
        assert!(!c.include_closed);
    }

    #[test]
    fn validation_bounds() {
        valid().validate().unwrap();

        let mut c = valid();
        c.stream_name = "  ".into();
        assert!(c.validate().is_err());

        let mut c = valid();
        c.records_per_request = 0;
        assert!(c.validate().is_err());
        c.records_per_request = 10_001;
        assert!(c.validate().is_err());

        let mut c = valid();
        c.shard_concurrency = 0;
        assert!(c.validate().is_err());

        let mut c = valid();
        c.max_messages = None;
        c.idle_termination_secs = None;
        let err = c.validate().unwrap_err();
        assert!(err.to_string().contains("idle_termination_secs"), "{err}");

        let mut c = valid();
        c.idle_termination_secs = Some(0);
        assert!(c.validate().is_err());
        let mut c = valid();
        c.max_messages = Some(0);
        assert!(c.validate().is_err());

        let mut c = valid();
        c.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        assert!(c.validate().is_err());
    }

    #[test]
    fn poll_interval_floored_at_200ms() {
        let mut c = valid();
        c.poll_interval_ms = 50;
        assert_eq!(c.poll_interval(), std::time::Duration::from_millis(200));
        c.poll_interval_ms = 1500;
        assert_eq!(c.poll_interval(), std::time::Duration::from_millis(1500));
    }

    #[test]
    fn start_positions_parse() {
        let p: StartPosition = serde_yaml::from_str("type: trim_horizon").unwrap();
        assert_eq!(p, StartPosition::TrimHorizon);
        let p: StartPosition = serde_yaml::from_str("type: latest").unwrap();
        assert_eq!(p, StartPosition::Latest);
        let p: StartPosition =
            serde_yaml::from_str("type: at_timestamp\ntimestamp_secs: 1716700000").unwrap();
        assert_eq!(
            p,
            StartPosition::AtTimestamp {
                timestamp_secs: 1_716_700_000
            }
        );
        let p: StartPosition =
            serde_yaml::from_str("type: after_sequence_number\nsequence: '49'").unwrap();
        assert_eq!(
            p,
            StartPosition::AfterSequenceNumber {
                sequence: "49".into()
            }
        );
    }

    #[test]
    fn full_config_parses_from_yaml() {
        let yaml = r#"
stream_name: events
region: us-east-1
endpoint_url: http://127.0.0.1:4566
credentials: { type: access_key, config: { access_key_id: a, secret_access_key: b } }
start_position: { type: latest }
shard_ids: ["shardId-000000000000"]
include_closed: true
poll_interval_ms: 500
records_per_request: 1000
shard_concurrency: 2
idle_termination_secs: 5
max_messages: 250
value_format: string
batch_size: 100
"#;
        let c: KinesisSourceConfig = serde_yaml::from_str(yaml).unwrap();
        c.validate().unwrap();
        assert_eq!(c.start_position, StartPosition::Latest);
        assert_eq!(c.value_format, ValueFormat::String);
        assert!(c.include_closed);
    }
}
