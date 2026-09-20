//! Configuration for the SQS sink. No I/O here.

use faucet_common_sqs::SqsCredentials;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// SQS hard limit: `SendMessageBatch` accepts at most 10 entries per request.
pub const MAX_ENTRIES_PER_REQUEST: usize = 10;
/// SQS hard limit: 256 KiB per message body.
pub const MAX_MESSAGE_BYTES: usize = 262_144;
/// SQS hard limit: 256 KiB total payload per `SendMessageBatch` request.
pub const MAX_BATCH_BYTES: usize = 262_144;

/// Configuration for [`SqsSink`](crate::SqsSink).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SqsSinkConfig {
    /// SQS queue URL (e.g. `https://sqs.us-east-1.amazonaws.com/1234/my-q`).
    pub queue_url: String,
    /// AWS region. `None` uses the SDK default chain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    /// Custom endpoint URL for LocalStack / VPC endpoints.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_url: Option<String>,
    /// AWS credentials. Defaults to the SDK default provider chain.
    #[serde(default)]
    pub credentials: SqsCredentials,

    /// FIFO message group id, applied to every message. Required by FIFO
    /// queues; omit for standard queues. Default: none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_group_id: Option<String>,
    /// Record field whose (stringified) value becomes each message's
    /// `MessageDeduplicationId` on a FIFO queue. A record missing the field
    /// (or with a non-scalar value) fails per-record (DLQ-routable). Only used
    /// when set. Default: none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_deduplication_id_field: Option<String>,

    /// Max entries per `SendMessageBatch` request (1–10). Default 10.
    ///
    /// **The house `batch_size = 0` "no batching" sentinel does not apply
    /// here** — `SendMessageBatch` caps a request at
    /// [`MAX_ENTRIES_PER_REQUEST`](crate::MAX_ENTRIES_PER_REQUEST)
    /// (10) entries, so an unbatched whole-page request is not expressible.
    /// `0` (and anything above 10) is rejected at config load; pages larger
    /// than `batch_size` are re-chunked into several requests (also
    /// re-chunked to stay under the 256 KiB per-request payload limit).
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Bounded concurrent in-flight `SendMessageBatch` requests. Default 4.
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    /// Per-record retry for partial failures, grouped (#654 M20). When
    /// present it supersedes the three deprecated flat `retry_*` keys below.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry: Option<faucet_core::retry::PartialRetrySpec>,
    /// **Deprecated** — use `retry.max_attempts`.
    ///
    /// Per-record retry budget for partial failures. Default 5.
    #[serde(default = "default_retry_max_attempts")]
    pub retry_max_attempts: usize,
    /// **Deprecated** — use `retry.initial_backoff_ms`.
    ///
    /// Initial retry backoff (milliseconds). Default 100.
    #[serde(default = "default_retry_initial_backoff_ms")]
    pub retry_initial_backoff_ms: u64,
    /// **Deprecated** — use `retry.max_backoff_ms`.
    ///
    /// Backoff ceiling (milliseconds). Default 30000.
    #[serde(default = "default_retry_max_backoff_ms")]
    pub retry_max_backoff_ms: u64,
}

fn default_batch_size() -> usize {
    MAX_ENTRIES_PER_REQUEST
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

impl SqsSinkConfig {
    /// The effective per-record retry budget: the `retry:` block when present,
    /// otherwise the three deprecated flat `retry_*` keys (#654 M20).
    ///
    /// The block wins wholesale — a flat key has a default, so "was it set?"
    /// is not observable, and a per-field merge would silently mix two
    /// spellings of one setting.
    pub fn retry_spec(&self) -> faucet_core::retry::PartialRetrySpec {
        self.retry
            .clone()
            .unwrap_or_else(|| faucet_core::retry::PartialRetrySpec {
                max_attempts: self.retry_max_attempts,
                initial_backoff_ms: self.retry_initial_backoff_ms,
                max_backoff_ms: self.retry_max_backoff_ms,
            })
    }

    /// Minimal config with defaults for everything but the queue URL.
    pub fn new(queue_url: impl Into<String>) -> Self {
        Self {
            queue_url: queue_url.into(),
            region: None,
            endpoint_url: None,
            credentials: SqsCredentials::default(),
            message_group_id: None,
            message_deduplication_id_field: None,
            batch_size: default_batch_size(),
            concurrency: default_concurrency(),
            retry: None,
            retry_max_attempts: default_retry_max_attempts(),
            retry_initial_backoff_ms: default_retry_initial_backoff_ms(),
            retry_max_backoff_ms: default_retry_max_backoff_ms(),
        }
    }

    /// Fail-fast validation, called from `SqsSink::new`.
    pub fn validate(&self) -> Result<(), faucet_core::FaucetError> {
        use faucet_core::FaucetError;
        if self.queue_url.trim().is_empty() {
            return Err(FaucetError::Config(
                "sqs sink: queue_url must not be empty".into(),
            ));
        }
        if self.batch_size == 0 || self.batch_size > MAX_ENTRIES_PER_REQUEST {
            return Err(FaucetError::Config(format!(
                "sqs sink: batch_size must be 1..={MAX_ENTRIES_PER_REQUEST} (got {}) — the \
                 SendMessageBatch API limit",
                self.batch_size
            )));
        }
        if self.concurrency == 0 {
            return Err(FaucetError::Config(
                "sqs sink: concurrency must be at least 1".into(),
            ));
        }
        if self.retry_max_attempts == 0 {
            return Err(FaucetError::Config(
                "sqs sink: retry_max_attempts must be at least 1".into(),
            ));
        }
        if let Some(field) = &self.message_deduplication_id_field
            && field.is_empty()
        {
            return Err(FaucetError::Config(
                "sqs sink: message_deduplication_id_field must not be empty".into(),
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
        let c = SqsSinkConfig::new("https://q");
        c.validate().unwrap();
        assert_eq!(c.batch_size, 10);
        assert_eq!(c.concurrency, 4);
        assert!(c.message_group_id.is_none());
        assert!(c.message_deduplication_id_field.is_none());
    }

    #[test]
    fn validation_bounds() {
        let mut c = SqsSinkConfig::new("https://q");
        c.batch_size = 11;
        assert!(c.validate().is_err(), "SendMessageBatch cap is 10");
        c.batch_size = 0;
        assert!(c.validate().is_err());

        let mut c = SqsSinkConfig::new("https://q");
        c.concurrency = 0;
        assert!(c.validate().is_err());

        let mut c = SqsSinkConfig::new("https://q");
        c.retry_max_attempts = 0;
        assert!(c.validate().is_err());

        let mut c = SqsSinkConfig::new("https://q");
        c.message_deduplication_id_field = Some(String::new());
        assert!(c.validate().is_err());

        let mut c = SqsSinkConfig::new("  ");
        c.batch_size = 5;
        assert!(c.validate().is_err());
    }

    #[test]
    fn full_config_parses_from_yaml() {
        let yaml = r#"
queue_url: https://sqs.us-east-1.amazonaws.com/1/events.fifo
region: us-east-1
endpoint_url: http://127.0.0.1:4566
credentials: { type: default }
message_group_id: orders
message_deduplication_id_field: order_id
batch_size: 5
concurrency: 2
retry_max_attempts: 3
"#;
        let c: SqsSinkConfig = serde_yaml::from_str(yaml).unwrap();
        c.validate().unwrap();
        assert_eq!(c.message_group_id.as_deref(), Some("orders"));
        assert_eq!(
            c.message_deduplication_id_field.as_deref(),
            Some("order_id")
        );
        assert_eq!(c.batch_size, 5);
    }

    #[test]
    fn retry_block_supersedes_the_deprecated_flat_keys() {
        let flat: SqsSinkConfig = serde_json::from_value(serde_json::json!({
            "queue_url": "https://sqs.example/q",
            "retry_max_attempts": 9,
            "retry_max_backoff_ms": 77
        }))
        .unwrap();
        let r = flat.retry_spec();
        assert_eq!(r.max_attempts, 9);
        assert_eq!(r.max_backoff_ms, 77);

        let blocked: SqsSinkConfig = serde_json::from_value(serde_json::json!({
            "queue_url": "https://sqs.example/q",
            "retry_max_attempts": 9,
            "retry": { "max_backoff_ms": 25 }
        }))
        .unwrap();
        let r = blocked.retry_spec();
        assert_eq!(r.max_backoff_ms, 25);
        assert_eq!(r.max_attempts, 5, "block default, not the flat 9");
    }
}
