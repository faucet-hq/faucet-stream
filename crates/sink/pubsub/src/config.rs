//! Configuration for the Pub/Sub sink. No I/O here.

use faucet_common_pubsub::PubsubConnection;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// How each record is encoded into the Pub/Sub message `data` blob.
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

/// How each message's ordering key is derived. When any non-empty ordering key
/// is produced, message ordering is enabled on the publisher.
///
/// Serializes as `{ type: <strategy>, … }` (snake_case discriminators).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OrderingKey {
    /// No ordering key — messages may be delivered in any order (default).
    #[default]
    None,
    /// A top-level field's value, stringified. A record missing the field (or
    /// with a null / container value) fails per-record (DLQ-routable).
    Field {
        /// Top-level field name.
        name: String,
    },
    /// A dot-path into the record (`a.b.c`, object keys only), stringified.
    Jsonpath {
        /// Dot path, e.g. `order.id`.
        path: String,
    },
}

impl OrderingKey {
    /// Whether this strategy ever produces an ordering key (so the publisher
    /// must enable message ordering).
    pub fn enables_ordering(&self) -> bool {
        !matches!(self, OrderingKey::None)
    }
}

/// Configuration for [`PubsubSink`](crate::PubsubSink).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PubsubSinkConfig {
    /// Topic id (short name, not the fully-qualified path). The client forms
    /// `projects/<project>/topics/<id>` from the connection's project.
    pub topic: String,

    /// Project / endpoint / emulator / credentials (flattened).
    #[serde(flatten)]
    pub connection: PubsubConnection,

    /// Record payload encoding. Default: `json`.
    #[serde(default)]
    pub value_format: ValueFormat,

    /// Ordering-key derivation. Default: `none`.
    #[serde(default)]
    pub ordering_key: OrderingKey,

    /// Optional record field holding a JSON object of message attributes.
    /// Values are stringified; the field is stripped from the payload before
    /// encoding. Absent field → no attributes (not an error).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attributes_field: Option<String>,

    /// Records per publish batch (1–1000). Default 100.
    ///
    /// **The house `batch_size = 0` "no batching" sentinel does not apply
    /// here** — Pub/Sub caps a single `Publish` request at
    /// [`MAX_BATCH`](crate::MAX_BATCH) (1000) messages, so an
    /// unbatched whole-page request is not expressible. `0` (and anything
    /// above 1000) is rejected at config load; pages larger than `batch_size`
    /// are re-chunked into several publishes.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Bounded concurrent in-flight publishes. Default 4.
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
}

/// Pub/Sub caps a single `Publish` request at 1000 messages.
pub const MAX_BATCH: usize = 1000;

fn default_batch_size() -> usize {
    100
}
fn default_concurrency() -> usize {
    4
}

impl PubsubSinkConfig {
    /// Minimal config with defaults for everything but the topic id.
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            connection: PubsubConnection::default(),
            value_format: ValueFormat::default(),
            ordering_key: OrderingKey::default(),
            attributes_field: None,
            batch_size: default_batch_size(),
            concurrency: default_concurrency(),
        }
    }

    /// Fail-fast validation, called from `PubsubSink::new`.
    pub fn validate(&self) -> Result<(), faucet_core::FaucetError> {
        use faucet_core::FaucetError;
        if self.topic.trim().is_empty() {
            return Err(FaucetError::Config(
                "pubsub sink: topic must not be empty".into(),
            ));
        }
        if self.batch_size == 0 || self.batch_size > MAX_BATCH {
            return Err(FaucetError::Config(format!(
                "pubsub sink: batch_size must be 1..={MAX_BATCH} (got {})",
                self.batch_size
            )));
        }
        if self.concurrency == 0 {
            return Err(FaucetError::Config(
                "pubsub sink: concurrency must be at least 1".into(),
            ));
        }
        match &self.ordering_key {
            OrderingKey::Field { name } if name.trim().is_empty() => {
                return Err(FaucetError::Config(
                    "pubsub sink: ordering_key.name must not be empty".into(),
                ));
            }
            OrderingKey::Jsonpath { path } if path.trim().is_empty() => {
                return Err(FaucetError::Config(
                    "pubsub sink: ordering_key.path must not be empty".into(),
                ));
            }
            _ => {}
        }
        if let Some(field) = &self.attributes_field
            && field.trim().is_empty()
        {
            return Err(FaucetError::Config(
                "pubsub sink: attributes_field must not be empty when set".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_sensible() {
        let c = PubsubSinkConfig::new("orders");
        c.validate().unwrap();
        assert_eq!(c.value_format, ValueFormat::Json);
        assert_eq!(c.ordering_key, OrderingKey::None);
        assert!(!c.ordering_key.enables_ordering());
        assert_eq!(c.batch_size, 100);
        assert_eq!(c.concurrency, 4);
    }

    #[test]
    fn ordering_key_enables_flag() {
        assert!(OrderingKey::Field { name: "k".into() }.enables_ordering());
        assert!(OrderingKey::Jsonpath { path: "a.b".into() }.enables_ordering());
        assert!(!OrderingKey::None.enables_ordering());
    }

    #[test]
    fn validation_bounds() {
        let mut c = PubsubSinkConfig::new("orders");
        c.topic = "  ".into();
        assert!(c.validate().is_err());

        let mut c = PubsubSinkConfig::new("orders");
        c.batch_size = 0;
        assert!(c.validate().is_err());
        c.batch_size = MAX_BATCH + 1;
        assert!(c.validate().is_err());

        let mut c = PubsubSinkConfig::new("orders");
        c.concurrency = 0;
        assert!(c.validate().is_err());

        let mut c = PubsubSinkConfig::new("orders");
        c.ordering_key = OrderingKey::Field { name: " ".into() };
        assert!(c.validate().is_err());
        c.ordering_key = OrderingKey::Jsonpath { path: "".into() };
        assert!(c.validate().is_err());

        let mut c = PubsubSinkConfig::new("orders");
        c.attributes_field = Some("".into());
        assert!(c.validate().is_err());
    }

    #[test]
    fn full_config_parses_from_yaml() {
        let yaml = r#"
topic: orders
project_id: my-proj
credentials: { type: application_default }
value_format: json
ordering_key: { type: field, name: customer_id }
attributes_field: __attributes
batch_size: 250
concurrency: 8
"#;
        let c: PubsubSinkConfig = serde_yaml::from_str(yaml).unwrap();
        c.validate().unwrap();
        assert_eq!(
            c.ordering_key,
            OrderingKey::Field {
                name: "customer_id".into()
            }
        );
        assert_eq!(c.attributes_field.as_deref(), Some("__attributes"));
        assert_eq!(c.batch_size, 250);
        assert_eq!(c.concurrency, 8);
    }
}
