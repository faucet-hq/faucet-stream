//! Configuration for the Pub/Sub source. No I/O here.

use faucet_common_pubsub::PubsubConnection;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// How each message's `data` payload is decoded into the emitted `data` field.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ValueFormat {
    /// Parse the payload as JSON. A message that is not valid JSON fails the
    /// stream with a typed error naming its `message_id`.
    #[default]
    Json,
    /// Decode the payload as UTF-8 (invalid UTF-8 fails the message).
    String,
    /// Base64-encode the raw payload bytes into a JSON string.
    Bytes,
}

/// What to do with a message whose payload does not decode under
/// `value_format`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OnDecodeError {
    /// Fail the run. The message stays unacked, so Pub/Sub redelivers it and
    /// the next run fails at it again until it is removed.
    #[default]
    Fail,
    /// Drop the message: it is acked with its page and no record is emitted.
    Skip,
    /// Emit the record with the payload base64-encoded under `data` and the
    /// decode error under `decode_error`.
    Raw,
}

/// The default JSON key the per-message attribute map is surfaced under.
pub const DEFAULT_ATTRIBUTES_KEY: &str = "__attributes";

/// Configuration for [`PubsubSource`](crate::PubsubSource).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PubsubSourceConfig {
    /// Subscription id (short name, not the fully-qualified path). The client
    /// forms `projects/<project>/subscriptions/<id>` from the connection's
    /// project.
    pub subscription: String,

    /// Project / endpoint / emulator / credentials (flattened).
    #[serde(flatten)]
    pub connection: PubsubConnection,

    /// Payload decoding for the message `data` field.
    #[serde(default)]
    pub value_format: ValueFormat,

    /// JSON key the message attribute map is surfaced under. Default
    /// `__attributes`.
    #[serde(default = "default_attributes_key")]
    pub attributes_key: String,

    /// Messages requested per `pull` RPC (1–1000). Default 100.
    #[serde(default = "default_max_messages_per_pull")]
    pub max_messages_per_pull: usize,

    /// Stop after this many seconds without a new message. At least one of
    /// `idle_termination_secs` and `max_messages` must be set (mirrors the
    /// Kafka / Kinesis sources) — a batch run must terminate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_termination_secs: Option<u64>,
    /// Stop after this many messages in total.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_messages: Option<usize>,

    /// Records per emitted [`StreamPage`](faucet_core::StreamPage). `0` is the
    /// "no batching" sentinel: one page per drain. Default 1000. A page's
    /// messages are acked once the pipeline has durably written the page.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// What to do with a message whose payload does not decode. Default
    /// `fail`.
    #[serde(default)]
    pub on_decode_error: OnDecodeError,

    /// Ack deadline, in seconds, the source keeps renewing on every message it
    /// has pulled but not yet acked (while a page is assembled and while the
    /// sink writes it), so a slow page is not redelivered into the same run.
    /// Renewed every 5 s (or every third of the window, if shorter). `0`
    /// disables renewal. Range 10–600. Default 60.
    #[serde(default = "default_ack_deadline_extension_secs")]
    pub ack_deadline_extension_secs: u32,
}

fn default_ack_deadline_extension_secs() -> u32 {
    60
}

fn default_attributes_key() -> String {
    DEFAULT_ATTRIBUTES_KEY.to_string()
}
fn default_max_messages_per_pull() -> usize {
    100
}
fn default_batch_size() -> usize {
    faucet_core::DEFAULT_BATCH_SIZE
}

impl PubsubSourceConfig {
    /// Minimal config with defaults for everything but the subscription id.
    pub fn new(subscription: impl Into<String>) -> Self {
        Self {
            subscription: subscription.into(),
            connection: PubsubConnection::default(),
            value_format: ValueFormat::default(),
            attributes_key: default_attributes_key(),
            max_messages_per_pull: default_max_messages_per_pull(),
            idle_termination_secs: None,
            max_messages: None,
            batch_size: default_batch_size(),
            on_decode_error: OnDecodeError::default(),
            ack_deadline_extension_secs: default_ack_deadline_extension_secs(),
        }
    }

    /// Fail-fast validation, called from `PubsubSource::new`.
    pub fn validate(&self) -> Result<(), faucet_core::FaucetError> {
        use faucet_core::FaucetError;
        if self.subscription.trim().is_empty() {
            return Err(FaucetError::Config(
                "pubsub source: subscription must not be empty".into(),
            ));
        }
        if self.attributes_key.trim().is_empty() {
            return Err(FaucetError::Config(
                "pubsub source: attributes_key must not be empty".into(),
            ));
        }
        if self.max_messages_per_pull == 0 || self.max_messages_per_pull > 1000 {
            return Err(FaucetError::Config(format!(
                "pubsub source: max_messages_per_pull must be 1..=1000 (got {})",
                self.max_messages_per_pull
            )));
        }
        faucet_core::validate_batch_size(self.batch_size)?;
        if self.idle_termination_secs.is_none() && self.max_messages.is_none() {
            return Err(FaucetError::Config(
                "pubsub source: set at least one of idle_termination_secs / max_messages so a \
                 run can terminate (mirrors the kafka / kinesis sources)"
                    .into(),
            ));
        }
        if self.idle_termination_secs == Some(0) {
            return Err(FaucetError::Config(
                "pubsub source: idle_termination_secs must be at least 1".into(),
            ));
        }
        if self.max_messages == Some(0) {
            return Err(FaucetError::Config(
                "pubsub source: max_messages must be at least 1".into(),
            ));
        }
        if self.ack_deadline_extension_secs != 0
            && !(10..=600).contains(&self.ack_deadline_extension_secs)
        {
            return Err(FaucetError::Config(format!(
                "pubsub source: ack_deadline_extension_secs must be 10..=600 or 0 to disable \
                 renewal (got {})",
                self.ack_deadline_extension_secs
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_common_pubsub::PubsubCredentials;

    fn valid() -> PubsubSourceConfig {
        let mut c = PubsubSourceConfig::new("orders-sub");
        c.max_messages = Some(100);
        c
    }

    #[test]
    fn defaults_are_sensible() {
        let c = PubsubSourceConfig::new("orders-sub");
        assert_eq!(c.value_format, ValueFormat::Json);
        assert_eq!(c.attributes_key, "__attributes");
        assert_eq!(c.max_messages_per_pull, 100);
        assert_eq!(c.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
        assert!(c.idle_termination_secs.is_none() && c.max_messages.is_none());
        assert_eq!(c.on_decode_error, OnDecodeError::Fail);
        assert_eq!(c.ack_deadline_extension_secs, 60);
    }

    #[test]
    fn ack_deadline_extension_bounds() {
        let mut c = valid();
        c.ack_deadline_extension_secs = 0;
        c.validate().unwrap();
        c.ack_deadline_extension_secs = 9;
        assert!(c.validate().is_err());
        c.ack_deadline_extension_secs = 601;
        assert!(c.validate().is_err());
        c.ack_deadline_extension_secs = 600;
        c.validate().unwrap();
    }

    #[test]
    fn validation_bounds() {
        valid().validate().unwrap();

        let mut c = valid();
        c.subscription = "  ".into();
        assert!(c.validate().is_err());

        let mut c = valid();
        c.attributes_key = String::new();
        assert!(c.validate().is_err());

        let mut c = valid();
        c.max_messages_per_pull = 0;
        assert!(c.validate().is_err());
        c.max_messages_per_pull = 1001;
        assert!(c.validate().is_err());

        let mut c = valid();
        c.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        assert!(c.validate().is_err());

        let mut c = valid();
        c.idle_termination_secs = None;
        c.max_messages = None;
        let err = c.validate().unwrap_err();
        assert!(err.to_string().contains("idle_termination_secs"), "{err}");

        let mut c = valid();
        c.idle_termination_secs = Some(0);
        assert!(c.validate().is_err());
        let mut c = valid();
        c.max_messages = Some(0);
        assert!(c.validate().is_err());
    }

    #[test]
    fn full_config_parses_from_yaml() {
        let yaml = r#"
subscription: orders-sub
project_id: my-proj
emulator_host: "localhost:8085"
credentials: { type: anonymous }
value_format: string
attributes_key: attrs
max_messages_per_pull: 250
idle_termination_secs: 5
max_messages: 500
batch_size: 100
"#;
        let c: PubsubSourceConfig = serde_yaml::from_str(yaml).unwrap();
        c.validate().unwrap();
        assert_eq!(c.value_format, ValueFormat::String);
        assert_eq!(c.attributes_key, "attrs");
        assert_eq!(c.connection.project_id.as_deref(), Some("my-proj"));
        assert_eq!(c.connection.credentials, PubsubCredentials::Anonymous);
        assert_eq!(c.max_messages_per_pull, 250);
    }
}
