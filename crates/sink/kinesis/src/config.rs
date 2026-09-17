//! Configuration for the Kinesis sink. No I/O here.

use faucet_common_kinesis::KinesisCredentials;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Kinesis hard limit: `PutRecords` accepts at most 500 entries per request.
pub const MAX_ENTRIES_PER_REQUEST: usize = 500;
/// Kinesis hard limit: 1 MiB per record (data + partition key).
pub const MAX_RECORD_BYTES: usize = 1_048_576;
/// Kinesis hard limit: 5 MiB per `PutRecords` request.
pub const MAX_REQUEST_BYTES: usize = 5_242_880;

/// How each record's Kinesis partition key is derived.
///
/// Serializes as `{ type: <strategy>, … }` (snake_case discriminators).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PartitionKey {
    /// A fresh UUID v4 per record — uniform shard spread, no ordering.
    #[default]
    Random,
    /// A top-level field's value, stringified. A record missing the field
    /// (or with a null / container value) fails per-record (DLQ-routable).
    Field {
        /// Top-level field name.
        name: String,
    },
    /// A dot-path into the record (`a.b.c` — object keys only), stringified.
    Jsonpath {
        /// Dot path, e.g. `user.id`.
        path: String,
    },
    /// The dot-path value MD5-hashed for even distribution over hot keys.
    Hash {
        /// Dot path, e.g. `event.id`.
        path: String,
    },
    /// A constant key — every record lands in one shard (rarely correct).
    Static {
        /// The constant partition key.
        value: String,
    },
}

/// Optional explicit hash key override (routes by hash instead of the
/// partition key's MD5).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExplicitHashKey {
    /// No override (route by the partition key).
    #[default]
    None,
    /// A top-level field's value (must stringify to a 0..2^128 integer).
    Field {
        /// Top-level field name.
        name: String,
    },
    /// A dot-path into the record.
    Jsonpath {
        /// Dot path, e.g. `routing.hash_key`.
        path: String,
    },
}

/// How each record is encoded into the Kinesis `Data` blob.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ValueFormat {
    /// Serialize the whole record as JSON bytes.
    #[default]
    Json,
    /// The record must be a JSON string → raw UTF-8 bytes.
    String,
    /// The record must be a base64 JSON string → decoded bytes.
    Bytes,
}

/// Configuration for [`KinesisSink`](crate::KinesisSink).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct KinesisSinkConfig {
    /// Kinesis Data Stream name.
    pub stream_name: String,
    /// AWS region. `None` uses the SDK default chain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    /// Custom endpoint URL for LocalStack / VPC endpoints.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_url: Option<String>,
    /// AWS credentials. Defaults to the SDK default provider chain.
    #[serde(default)]
    pub credentials: KinesisCredentials,

    /// Partition-key derivation strategy. Default: `random`.
    #[serde(default)]
    pub partition_key: PartitionKey,
    /// Optional explicit hash-key override.
    #[serde(default)]
    pub explicit_hash_key: ExplicitHashKey,
    /// Record payload encoding. Default: `json`.
    #[serde(default)]
    pub value_format: ValueFormat,

    /// Max entries per `PutRecords` request (1–500). Default 500.
    ///
    /// **The house `batch_size = 0` "no batching" sentinel does not apply
    /// here** — `PutRecords` caps a request at 500 entries, so an unbatched
    /// whole-page request is not expressible. `0` (and anything above 500) is
    /// rejected at config load; pages larger than `batch_size` are re-chunked
    /// into several requests (also re-chunked to stay under
    /// [`Self::max_request_bytes`]).
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Per-record size ceiling in bytes (data + partition key; ≤ 1 MiB).
    /// Oversized records fail per-record (DLQ-routable), never sent.
    #[serde(default = "default_max_record_bytes")]
    pub max_record_size_bytes: usize,
    /// Per-request size ceiling in bytes (≤ 5 MiB); batches re-chunk to it.
    #[serde(default = "default_max_request_bytes")]
    pub max_request_bytes: usize,

    /// Bounded concurrent in-flight `PutRecords` requests. Default 4.
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    /// Per-record retry budget for partial failures. Default 5.
    #[serde(default = "default_retry_max_attempts")]
    pub retry_max_attempts: usize,
    /// Initial retry backoff (milliseconds). Default 100.
    #[serde(default = "default_retry_initial_backoff_ms")]
    pub retry_initial_backoff_ms: u64,
    /// Backoff ceiling (milliseconds). Default 30000.
    #[serde(default = "default_retry_max_backoff_ms")]
    pub retry_max_backoff_ms: u64,
}

fn default_batch_size() -> usize {
    MAX_ENTRIES_PER_REQUEST
}
fn default_max_record_bytes() -> usize {
    MAX_RECORD_BYTES
}
fn default_max_request_bytes() -> usize {
    MAX_REQUEST_BYTES
}
fn default_concurrency() -> usize {
    4
}
fn default_retry_max_attempts() -> usize {
    5
}
fn default_retry_initial_backoff_ms() -> u64 {
    100
}
fn default_retry_max_backoff_ms() -> u64 {
    30_000
}

impl KinesisSinkConfig {
    /// Minimal config with defaults for everything but the stream name.
    pub fn new(stream_name: impl Into<String>) -> Self {
        Self {
            stream_name: stream_name.into(),
            region: None,
            endpoint_url: None,
            credentials: KinesisCredentials::default(),
            partition_key: PartitionKey::default(),
            explicit_hash_key: ExplicitHashKey::default(),
            value_format: ValueFormat::default(),
            batch_size: default_batch_size(),
            max_record_size_bytes: default_max_record_bytes(),
            max_request_bytes: default_max_request_bytes(),
            concurrency: default_concurrency(),
            retry_max_attempts: default_retry_max_attempts(),
            retry_initial_backoff_ms: default_retry_initial_backoff_ms(),
            retry_max_backoff_ms: default_retry_max_backoff_ms(),
        }
    }

    /// Fail-fast validation, called from `KinesisSink::new`.
    pub fn validate(&self) -> Result<(), faucet_core::FaucetError> {
        use faucet_core::FaucetError;
        if self.stream_name.trim().is_empty() {
            return Err(FaucetError::Config(
                "kinesis sink: stream_name must not be empty".into(),
            ));
        }
        if self.batch_size == 0 || self.batch_size > MAX_ENTRIES_PER_REQUEST {
            return Err(FaucetError::Config(format!(
                "kinesis sink: batch_size must be 1..={MAX_ENTRIES_PER_REQUEST} (got {}) — \
                 the PutRecords API limit",
                self.batch_size
            )));
        }
        if self.max_record_size_bytes == 0 || self.max_record_size_bytes > MAX_RECORD_BYTES {
            return Err(FaucetError::Config(format!(
                "kinesis sink: max_record_size_bytes must be 1..={MAX_RECORD_BYTES} (got {})",
                self.max_record_size_bytes
            )));
        }
        if self.max_request_bytes < self.max_record_size_bytes
            || self.max_request_bytes > MAX_REQUEST_BYTES
        {
            return Err(FaucetError::Config(format!(
                "kinesis sink: max_request_bytes must be {}..={MAX_REQUEST_BYTES} (got {})",
                self.max_record_size_bytes, self.max_request_bytes
            )));
        }
        if self.concurrency == 0 {
            return Err(FaucetError::Config(
                "kinesis sink: concurrency must be at least 1".into(),
            ));
        }
        if let PartitionKey::Field { name } = &self.partition_key
            && name.is_empty()
        {
            return Err(FaucetError::Config(
                "kinesis sink: partition_key.name must not be empty".into(),
            ));
        }
        if let PartitionKey::Jsonpath { path } | PartitionKey::Hash { path } = &self.partition_key
            && path.is_empty()
        {
            return Err(FaucetError::Config(
                "kinesis sink: partition_key.path must not be empty".into(),
            ));
        }
        if let PartitionKey::Static { value } = &self.partition_key
            && (value.is_empty() || value.len() > 256)
        {
            return Err(FaucetError::Config(
                "kinesis sink: partition_key.value must be 1..=256 characters".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_the_api_limits() {
        let c = KinesisSinkConfig::new("events");
        c.validate().unwrap();
        assert_eq!(c.batch_size, 500);
        assert_eq!(c.max_record_size_bytes, 1_048_576);
        assert_eq!(c.max_request_bytes, 5_242_880);
        assert_eq!(c.partition_key, PartitionKey::Random);
        assert_eq!(c.value_format, ValueFormat::Json);
    }

    #[test]
    fn validation_bounds() {
        let mut c = KinesisSinkConfig::new("events");
        c.batch_size = 501;
        assert!(c.validate().is_err(), "PutRecords cap is 500");
        c.batch_size = 0;
        assert!(c.validate().is_err());

        let mut c = KinesisSinkConfig::new("events");
        c.max_record_size_bytes = MAX_RECORD_BYTES + 1;
        assert!(c.validate().is_err(), "1 MiB record cap");

        let mut c = KinesisSinkConfig::new("events");
        c.max_request_bytes = MAX_REQUEST_BYTES + 1;
        assert!(c.validate().is_err(), "5 MiB request cap");
        c.max_request_bytes = 10; // smaller than one max-size record
        assert!(c.validate().is_err());

        let mut c = KinesisSinkConfig::new("events");
        c.concurrency = 0;
        assert!(c.validate().is_err());

        let mut c = KinesisSinkConfig::new("events");
        c.partition_key = PartitionKey::Field { name: "".into() };
        assert!(c.validate().is_err());
        c.partition_key = PartitionKey::Static {
            value: "x".repeat(257),
        };
        assert!(c.validate().is_err());

        let mut c = KinesisSinkConfig::new("events");
        c.stream_name = " ".into();
        assert!(c.validate().is_err());
    }

    #[test]
    fn full_config_parses_from_yaml() {
        let yaml = r#"
stream_name: events
region: us-east-1
endpoint_url: http://127.0.0.1:4566
credentials: { type: default }
partition_key: { type: field, name: user_id }
explicit_hash_key: { type: jsonpath, path: routing.hash }
value_format: json
batch_size: 100
concurrency: 2
retry_max_attempts: 3
"#;
        let c: KinesisSinkConfig = serde_yaml::from_str(yaml).unwrap();
        c.validate().unwrap();
        assert_eq!(
            c.partition_key,
            PartitionKey::Field {
                name: "user_id".into()
            }
        );
        assert_eq!(
            c.explicit_hash_key,
            ExplicitHashKey::Jsonpath {
                path: "routing.hash".into()
            }
        );
    }
}
