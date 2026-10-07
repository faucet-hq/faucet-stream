//! Configuration for the NATS source.

use faucet_common_nats::NatsConnectionConfig;
use faucet_core::{DEFAULT_BATCH_SIZE, FaucetError};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

/// Configuration for [`NatsSource`](crate::NatsSource).
///
/// The [`NatsConnectionConfig`] surface (`servers` / `auth` / `tls` / `name`)
/// is flattened in, so a config looks like:
///
/// ```yaml
/// servers: ["nats://127.0.0.1:4222"]
/// subject: "events.>"
/// idle_timeout_secs: 5
/// batch_size: 500
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct NatsSourceConfig {
    /// Connection settings (servers, auth, tls, name).
    #[serde(flatten)]
    pub connection: NatsConnectionConfig,

    /// Subject to subscribe to. Supports NATS wildcards (`*` for one token,
    /// `>` for the remaining tokens). In JetStream mode the durable consumer's
    /// own filter subject governs delivery and this field is informational.
    pub subject: String,

    /// Optional queue group for core-NATS load-balanced subscriptions — all
    /// subscribers sharing a group split the subject's messages. Ignored in
    /// JetStream mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_group: Option<String>,

    /// JetStream stream name. When set together with
    /// [`jetstream_consumer`](Self::jetstream_consumer) the source pulls from a
    /// durable JetStream consumer instead of a core-NATS subscription.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jetstream_stream: Option<String>,

    /// Name of the durable JetStream (pull) consumer to bind to. Required when
    /// [`jetstream_stream`](Self::jetstream_stream) is set; the consumer must
    /// already exist on the server.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jetstream_consumer: Option<String>,

    /// Stop after this many messages have been drained. At least one of
    /// `max_messages` / `idle_timeout_secs` must be set so a run terminates.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_messages: Option<usize>,

    /// Stop after this many seconds elapse with no new message. At least one of
    /// `max_messages` / `idle_timeout_secs` must be set so a run terminates.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_timeout_secs: Option<u64>,

    /// Messages per emitted [`StreamPage`](faucet_core::StreamPage). Drained
    /// messages accumulate in an in-memory buffer and a page is yielded when
    /// the buffer reaches this size (or when the run terminates with a partial
    /// buffer). Defaults to [`DEFAULT_BATCH_SIZE`].
    ///
    /// `batch_size = 0` is the "drain-entire-run-window" sentinel: every
    /// message produced before the terminator fires goes into a single page.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// JetStream only: send an in-progress ack (`+WPI`) every this many
    /// seconds for every message pulled but not yet acked (while a page is
    /// assembled and while the sink writes it), resetting the consumer's
    /// `ack_wait` so a slow page is not redelivered into the same run. Keep
    /// it below the consumer's `ack_wait` (30 s by default). `0` disables.
    /// Default 10.
    #[serde(default = "default_progress_interval_secs")]
    pub progress_interval_secs: u64,

    /// How a message payload becomes a record: `auto` (default — JSON when
    /// it parses, else the UTF-8 text), `json`, `string` or `bytes`
    /// (base64). A payload the format cannot represent fails the run rather
    /// than being altered (#789 MSG-39).
    #[serde(default)]
    pub value_format: NatsValueFormat,

    /// Wrap each record as `{ subject, sequence, message_id, payload }` so a
    /// downstream consumer can deduplicate redeliveries: `sequence` is the
    /// JetStream stream sequence (`null` in core mode) and `message_id` the
    /// `Nats-Msg-Id` header (`null` when absent). Default `false` (the
    /// payload alone).
    #[serde(default)]
    pub include_metadata: bool,
}

/// How [`NatsSourceConfig::value_format`] decodes a payload.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum NatsValueFormat {
    /// JSON when the payload parses, else its UTF-8 text; non-UTF-8 fails.
    #[default]
    Auto,
    /// The payload must be JSON.
    Json,
    /// The payload as UTF-8 text (invalid UTF-8 fails).
    String,
    /// The raw bytes, base64-encoded into a JSON string.
    Bytes,
}

fn default_progress_interval_secs() -> u64 {
    10
}

impl NatsSourceConfig {
    /// Convenience constructor for a minimal core-NATS subscription with an
    /// idle-timeout terminator.
    pub fn new(subject: impl Into<String>) -> Self {
        Self {
            connection: NatsConnectionConfig::default(),
            subject: subject.into(),
            queue_group: None,
            jetstream_stream: None,
            jetstream_consumer: None,
            max_messages: None,
            idle_timeout_secs: Some(5),
            batch_size: DEFAULT_BATCH_SIZE,
            progress_interval_secs: default_progress_interval_secs(),
            value_format: NatsValueFormat::Auto,
            include_metadata: false,
        }
    }

    /// Whether this config selects JetStream (pull-consumer) mode.
    pub fn is_jetstream(&self) -> bool {
        self.jetstream_stream.is_some()
    }

    /// Validate the config at construction time.
    pub fn validate(&self) -> Result<(), FaucetError> {
        self.connection.validate()?;

        if self.subject.trim().is_empty() {
            return Err(FaucetError::Config(
                "nats source: `subject` must not be empty".into(),
            ));
        }

        match (&self.jetstream_stream, &self.jetstream_consumer) {
            (Some(s), _) if s.trim().is_empty() => {
                return Err(FaucetError::Config(
                    "nats source: `jetstream_stream` must not be empty".into(),
                ));
            }
            (Some(_), None) => {
                return Err(FaucetError::Config(
                    "nats source: `jetstream_consumer` is required when `jetstream_stream` is set"
                        .into(),
                ));
            }
            (Some(_), Some(c)) if c.trim().is_empty() => {
                return Err(FaucetError::Config(
                    "nats source: `jetstream_consumer` must not be empty".into(),
                ));
            }
            (None, Some(_)) => {
                return Err(FaucetError::Config(
                    "nats source: `jetstream_stream` is required when `jetstream_consumer` is set"
                        .into(),
                ));
            }
            _ => {}
        }

        if self.max_messages.is_none() && self.idle_timeout_secs.is_none() {
            return Err(FaucetError::Config(
                "nats source: at least one of `max_messages` or `idle_timeout_secs` must be set \
                 so the run terminates"
                    .into(),
            ));
        }

        if self.max_messages == Some(0) || self.idle_timeout_secs == Some(0) {
            return Err(FaucetError::Config(
                "nats source: `max_messages` and `idle_timeout_secs` must be greater than 0".into(),
            ));
        }

        faucet_core::validate_batch_size(self.batch_size)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn validate_accepts_minimal() {
        assert!(NatsSourceConfig::new("events.>").validate().is_ok());
    }

    #[test]
    fn validate_rejects_empty_subject() {
        let mut c = NatsSourceConfig::new("x");
        c.subject = "  ".into();
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_missing_terminator() {
        let mut c = NatsSourceConfig::new("x");
        c.idle_timeout_secs = None;
        c.max_messages = None;
        let err = c.validate().unwrap_err();
        assert!(format!("{err}").contains("max_messages"));
    }

    #[test]
    fn validate_requires_consumer_with_stream() {
        let mut c = NatsSourceConfig::new("x");
        c.jetstream_stream = Some("ORDERS".into());
        let err = c.validate().unwrap_err();
        assert!(format!("{err}").contains("jetstream_consumer"));
    }

    #[test]
    fn validate_requires_stream_with_consumer() {
        let mut c = NatsSourceConfig::new("x");
        c.jetstream_consumer = Some("worker".into());
        let err = c.validate().unwrap_err();
        assert!(format!("{err}").contains("jetstream_stream"));
    }

    #[test]
    fn validate_accepts_full_jetstream() {
        let mut c = NatsSourceConfig::new("orders.>");
        c.jetstream_stream = Some("ORDERS".into());
        c.jetstream_consumer = Some("worker".into());
        c.max_messages = Some(100);
        assert!(c.validate().is_ok());
        assert!(c.is_jetstream());
    }

    #[test]
    fn validate_rejects_batch_size_over_max() {
        let mut c = NatsSourceConfig::new("x");
        c.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        assert!(matches!(c.validate(), Err(FaucetError::Config(_))));
    }

    #[test]
    fn deserialize_flattened_connection() {
        let c: NatsSourceConfig = serde_json::from_value(json!({
            "servers": ["nats://a:4222"],
            "auth": {"type": "token", "config": {"token": "t"}},
            "subject": "events.>",
            "max_messages": 10
        }))
        .unwrap();
        assert_eq!(c.subject, "events.>");
        assert_eq!(c.connection.servers, vec!["nats://a:4222".to_string()]);
        assert_eq!(c.max_messages, Some(10));
        assert_eq!(c.batch_size, DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn zero_terminators_are_rejected() {
        let mut c = NatsSourceConfig::new("x");
        c.max_messages = Some(0);
        assert!(c.validate().is_err());
        let mut c = NatsSourceConfig::new("x");
        c.idle_timeout_secs = Some(0);
        assert!(c.validate().is_err());
    }

    #[test]
    fn batch_size_zero_is_accepted() {
        let mut c = NatsSourceConfig::new("x");
        c.batch_size = 0;
        assert!(c.validate().is_ok());
    }

    #[test]
    fn schema_compiles() {
        let _ = schemars::schema_for!(NatsSourceConfig);
    }
}
