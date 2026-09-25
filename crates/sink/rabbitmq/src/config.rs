//! Configuration for the RabbitMQ sink.

use faucet_common_rabbitmq::{RabbitMqConnectionConfig, RabbitMqExchangeKind, RabbitMqValueFormat};
use faucet_core::{DEFAULT_BATCH_SIZE, FaucetError};
use jsonpath_rust::JsonPath;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

fn default_true() -> bool {
    true
}

/// Configuration for [`RabbitMqSink`](crate::RabbitMqSink).
///
/// Exactly one of `routing_key` (static), `routing_key_field` (top-level
/// record field) or `routing_key_jsonpath` (JSONPath into the record) selects
/// each message's routing key. With the default exchange (`""`) the routing
/// key is the destination queue name.
///
/// ```yaml
/// url: "amqp://guest:guest@127.0.0.1:5672/%2f"
/// exchange: events
/// exchange_kind: topic
/// routing_key_field: event_type
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RabbitMqSinkConfig {
    /// Connection settings.
    #[serde(flatten)]
    pub connection: RabbitMqConnectionConfig,

    /// Exchange to publish to. Defaults to `""`, the default exchange, which
    /// routes each message to the queue named by its routing key.
    #[serde(default)]
    pub exchange: String,

    /// When set, `exchange` is declared (durable) with this type before the
    /// first publish; otherwise it must already exist. Not allowed with the
    /// default exchange.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exchange_kind: Option<RabbitMqExchangeKind>,

    /// Static routing key for every message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing_key: Option<String>,

    /// Top-level record field whose value (string, number or bool) is the
    /// routing key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing_key_field: Option<String>,

    /// JSONPath (e.g. `$.meta.route`) whose first match is the routing key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing_key_jsonpath: Option<String>,

    /// How records encode into message bodies. Defaults to `json`.
    #[serde(default)]
    pub value_format: RabbitMqValueFormat,

    /// Publish with delivery mode 2 (persisted to disk on durable queues).
    /// Defaults to `true`.
    #[serde(default = "default_true")]
    pub persistent: bool,

    /// Publish with the `mandatory` flag: a message no queue is bound to
    /// receive is returned by the broker and surfaces as a per-row error
    /// (DLQ-routable) instead of being silently dropped. Requires `confirm`.
    /// Defaults to `false`.
    #[serde(default)]
    pub mandatory: bool,

    /// Enable publisher confirms: every batch waits for the broker to
    /// acknowledge each message before the write returns. Defaults to `true`
    /// (the reliable setting — without it a broker-side failure is invisible).
    #[serde(default = "default_true")]
    pub confirm: bool,

    /// Messages published before waiting on their confirms. Defaults to
    /// [`DEFAULT_BATCH_SIZE`]. `0` publishes the whole batch handed over by
    /// the pipeline at once.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

/// How the routing key is derived.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RoutingKey {
    Static(String),
    Field(String),
    JsonPath(String),
}

impl RabbitMqSinkConfig {
    /// A sink publishing to the default exchange with a static routing key —
    /// i.e. straight into the queue named `queue`.
    pub fn to_queue(queue: impl Into<String>) -> Self {
        Self {
            connection: RabbitMqConnectionConfig::default(),
            exchange: String::new(),
            exchange_kind: None,
            routing_key: Some(queue.into()),
            routing_key_field: None,
            routing_key_jsonpath: None,
            value_format: RabbitMqValueFormat::default(),
            persistent: true,
            mandatory: false,
            confirm: true,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    pub(crate) fn routing(&self) -> Result<RoutingKey, FaucetError> {
        match (
            &self.routing_key,
            &self.routing_key_field,
            &self.routing_key_jsonpath,
        ) {
            (Some(k), None, None) => Ok(RoutingKey::Static(k.clone())),
            (None, Some(f), None) => Ok(RoutingKey::Field(f.clone())),
            (None, None, Some(p)) => Ok(RoutingKey::JsonPath(p.clone())),
            _ => Err(FaucetError::Config(
                "rabbitmq sink: set exactly one of `routing_key`, `routing_key_field` or \
                 `routing_key_jsonpath`"
                    .into(),
            )),
        }
    }

    /// Validate the config (pure — no I/O).
    pub fn validate(&self) -> Result<(), FaucetError> {
        self.connection.validate()?;
        match self.routing()? {
            RoutingKey::Static(k) => {
                if k.is_empty() && self.exchange.is_empty() {
                    return Err(FaucetError::Config(
                        "rabbitmq sink: an empty `routing_key` on the default exchange routes \
                         nowhere — name the destination queue"
                            .into(),
                    ));
                }
                if k.len() > 255 {
                    return Err(FaucetError::Config(
                        "rabbitmq sink: `routing_key` exceeds 255 bytes".into(),
                    ));
                }
            }
            RoutingKey::Field(f) if f.trim().is_empty() => {
                return Err(FaucetError::Config(
                    "rabbitmq sink: `routing_key_field` must not be empty".into(),
                ));
            }
            RoutingKey::JsonPath(p) => {
                Value::Null.query(&p).map_err(|e| {
                    FaucetError::Config(format!(
                        "rabbitmq sink: invalid `routing_key_jsonpath` '{p}': {e}"
                    ))
                })?;
            }
            RoutingKey::Field(_) => {}
        }
        if self.exchange.len() > 255 {
            return Err(FaucetError::Config(
                "rabbitmq sink: `exchange` exceeds 255 bytes".into(),
            ));
        }
        if self.exchange.is_empty() && self.exchange_kind.is_some() {
            return Err(FaucetError::Config(
                "rabbitmq sink: `exchange_kind` cannot be set for the default exchange (`\"\"`)"
                    .into(),
            ));
        }
        if self.mandatory && !self.confirm {
            return Err(FaucetError::Config(
                "rabbitmq sink: `mandatory: true` requires `confirm: true` — returned messages \
                 are matched to rows through publisher confirms"
                    .into(),
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
    fn to_queue_is_valid() {
        let c = RabbitMqSinkConfig::to_queue("orders");
        assert!(c.validate().is_ok());
        assert_eq!(c.routing().unwrap(), RoutingKey::Static("orders".into()));
    }

    #[test]
    fn deserializes_with_defaults() {
        let c: RabbitMqSinkConfig = serde_json::from_value(json!({
            "url": "amqp://broker",
            "exchange": "events",
            "exchange_kind": "topic",
            "routing_key_field": "kind"
        }))
        .unwrap();
        assert!(c.persistent && c.confirm && !c.mandatory);
        assert_eq!(c.value_format, RabbitMqValueFormat::Json);
        assert_eq!(c.batch_size, DEFAULT_BATCH_SIZE);
        assert_eq!(c.routing().unwrap(), RoutingKey::Field("kind".into()));
        assert!(c.validate().is_ok());
    }

    #[test]
    fn exactly_one_routing_source() {
        let mut c = RabbitMqSinkConfig::to_queue("q");
        c.routing_key_field = Some("f".into());
        assert!(
            c.validate()
                .unwrap_err()
                .to_string()
                .contains("exactly one")
        );
        c.routing_key = None;
        c.routing_key_field = None;
        assert!(
            c.validate()
                .unwrap_err()
                .to_string()
                .contains("exactly one")
        );
        c.routing_key_jsonpath = Some("$.a.b".into());
        assert_eq!(c.routing().unwrap(), RoutingKey::JsonPath("$.a.b".into()));
        assert!(c.validate().is_ok());
    }

    #[test]
    fn rejects_bad_routing_values() {
        let mut c = RabbitMqSinkConfig::to_queue("");
        assert!(
            c.validate()
                .unwrap_err()
                .to_string()
                .contains("routes nowhere")
        );
        c.exchange = "fanout-ex".into();
        assert!(
            c.validate().is_ok(),
            "empty key is fine on a named exchange"
        );
        c.routing_key = Some("x".repeat(256));
        assert!(c.validate().unwrap_err().to_string().contains("255"));
        c.routing_key = None;
        c.routing_key_field = Some(" ".into());
        assert!(
            c.validate()
                .unwrap_err()
                .to_string()
                .contains("routing_key_field")
        );
        c.routing_key_field = None;
        c.routing_key_jsonpath = Some("$[".into());
        assert!(c.validate().unwrap_err().to_string().contains("jsonpath"));
    }

    #[test]
    fn exchange_rules() {
        let mut c = RabbitMqSinkConfig::to_queue("q");
        c.exchange_kind = Some(RabbitMqExchangeKind::Direct);
        assert!(
            c.validate()
                .unwrap_err()
                .to_string()
                .contains("default exchange")
        );
        c.exchange = "x".repeat(256);
        assert!(c.validate().unwrap_err().to_string().contains("255"));
    }

    #[test]
    fn mandatory_requires_confirm() {
        let mut c = RabbitMqSinkConfig::to_queue("q");
        c.mandatory = true;
        c.confirm = false;
        assert!(c.validate().unwrap_err().to_string().contains("confirm"));
        c.confirm = true;
        assert!(c.validate().is_ok());
    }

    #[test]
    fn rejects_batch_size_over_max_and_bad_connection() {
        let mut c = RabbitMqSinkConfig::to_queue("q");
        c.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        assert!(matches!(c.validate(), Err(FaucetError::Config(_))));
        let mut c = RabbitMqSinkConfig::to_queue("q");
        c.connection.connect_timeout_secs = 0;
        assert!(c.validate().is_err());
    }

    #[test]
    fn schema_compiles() {
        let _ = schemars::schema_for!(RabbitMqSinkConfig);
    }
}
