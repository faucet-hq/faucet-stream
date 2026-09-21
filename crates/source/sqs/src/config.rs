//! Configuration for the SQS source. No I/O here.

use faucet_common_sqs::SqsCredentials;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// SQS long-poll ceiling: `ReceiveMessage` accepts at most a 20-second wait.
pub const MAX_WAIT_TIME_SECONDS: i32 = 20;
/// SQS hard limit: at most 10 messages per `ReceiveMessage` call.
pub const MAX_RECEIVE_BATCH: i32 = 10;

/// Configuration for [`SqsSource`](crate::SqsSource).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SqsSourceConfig {
    /// SQS queue URL (e.g. `https://sqs.us-east-1.amazonaws.com/1234/my-q`).
    pub queue_url: String,
    /// AWS region. `None` uses the SDK default chain (env, profile, IMDS).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    /// Custom endpoint URL for LocalStack / VPC endpoints.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_url: Option<String>,
    /// AWS credentials. Defaults to the SDK default provider chain.
    #[serde(default)]
    pub credentials: SqsCredentials,

    /// Stop after this many seconds without a new message. At least one of
    /// `idle_timeout_secs` and `max_messages` must be set (mirrors the Kafka /
    /// Kinesis sources) — a batch run must terminate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_timeout_secs: Option<u64>,
    /// Stop after this many messages in total.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_messages: Option<usize>,

    /// Long-poll wait per `ReceiveMessage` call, in seconds (0–20). Default 10.
    #[serde(default = "default_wait_time_seconds")]
    pub wait_time_seconds: i32,

    /// Records per emitted [`StreamPage`](faucet_core::StreamPage). `0` is the
    /// "no batching" sentinel: one page per assembled drain buffer. Default
    /// 1000.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_wait_time_seconds() -> i32 {
    10
}
fn default_batch_size() -> usize {
    faucet_core::DEFAULT_BATCH_SIZE
}

impl SqsSourceConfig {
    /// Minimal config with defaults for everything but the queue URL.
    pub fn new(queue_url: impl Into<String>) -> Self {
        Self {
            queue_url: queue_url.into(),
            region: None,
            endpoint_url: None,
            credentials: SqsCredentials::default(),
            idle_timeout_secs: None,
            max_messages: None,
            wait_time_seconds: default_wait_time_seconds(),
            batch_size: default_batch_size(),
        }
    }

    /// Fail-fast validation, called from `SqsSource::new`.
    pub fn validate(&self) -> Result<(), faucet_core::FaucetError> {
        use faucet_core::FaucetError;
        if self.queue_url.trim().is_empty() {
            return Err(FaucetError::Config(
                "sqs source: queue_url must not be empty".into(),
            ));
        }
        faucet_core::validate_batch_size(self.batch_size)?;
        if self.wait_time_seconds < 0 || self.wait_time_seconds > MAX_WAIT_TIME_SECONDS {
            return Err(FaucetError::Config(format!(
                "sqs source: wait_time_seconds must be 0..={MAX_WAIT_TIME_SECONDS} (got {})",
                self.wait_time_seconds
            )));
        }
        if self.idle_timeout_secs.is_none() && self.max_messages.is_none() {
            return Err(FaucetError::Config(
                "sqs source: set at least one of idle_timeout_secs / max_messages so a run can \
                 terminate (mirrors the kafka/kinesis sources)"
                    .into(),
            ));
        }
        if self.idle_timeout_secs == Some(0) {
            return Err(FaucetError::Config(
                "sqs source: idle_timeout_secs must be at least 1".into(),
            ));
        }
        if self.max_messages == Some(0) {
            return Err(FaucetError::Config(
                "sqs source: max_messages must be at least 1".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid() -> SqsSourceConfig {
        let mut c = SqsSourceConfig::new("https://sqs.us-east-1.amazonaws.com/1/events");
        c.max_messages = Some(100);
        c
    }

    #[test]
    fn defaults_are_sensible() {
        let c = SqsSourceConfig::new("https://q");
        assert_eq!(c.wait_time_seconds, 10);
        assert_eq!(c.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
        assert!(c.idle_timeout_secs.is_none());
        assert!(c.max_messages.is_none());
    }

    #[test]
    fn validation_bounds() {
        valid().validate().unwrap();

        let mut c = valid();
        c.queue_url = "  ".into();
        assert!(c.validate().is_err());

        let mut c = valid();
        c.wait_time_seconds = 21;
        assert!(c.validate().is_err());
        c.wait_time_seconds = -1;
        assert!(c.validate().is_err());

        let mut c = valid();
        c.max_messages = None;
        c.idle_timeout_secs = None;
        let err = c.validate().unwrap_err();
        assert!(err.to_string().contains("idle_timeout_secs"), "{err}");

        let mut c = valid();
        c.idle_timeout_secs = Some(0);
        assert!(c.validate().is_err());
        let mut c = valid();
        c.max_messages = Some(0);
        assert!(c.validate().is_err());

        let mut c = valid();
        c.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        assert!(c.validate().is_err());
    }

    #[test]
    fn full_config_parses_from_yaml() {
        let yaml = r#"
queue_url: https://sqs.us-east-1.amazonaws.com/1/events
region: us-east-1
endpoint_url: http://127.0.0.1:4566
credentials: { type: access_key, config: { access_key_id: a, secret_access_key: b } }
idle_timeout_secs: 5
max_messages: 250
wait_time_seconds: 5
batch_size: 100
"#;
        let c: SqsSourceConfig = serde_yaml::from_str(yaml).unwrap();
        c.validate().unwrap();
        assert_eq!(c.wait_time_seconds, 5);
        assert_eq!(c.batch_size, 100);
        assert_eq!(c.idle_timeout_secs, Some(5));
        assert_eq!(c.max_messages, Some(250));
    }
}
