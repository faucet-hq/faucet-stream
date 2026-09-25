//! Configuration for the RabbitMQ source.

use faucet_common_rabbitmq::{RabbitMqConnectionConfig, RabbitMqExchangeKind, RabbitMqValueFormat};
use faucet_core::{DEFAULT_BATCH_SIZE, FaucetError};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

fn default_true() -> bool {
    true
}

/// When deliveries are acknowledged to the broker.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AckMode {
    /// At-least-once: a page's deliveries are acknowledged only after the
    /// pipeline has written **and flushed** that page to the sink. A crash or
    /// failed write before that leaves them unacknowledged, and the broker
    /// redelivers them — duplicates are possible, loss is not.
    #[default]
    OnSinkConfirm,
    /// At-most-once: the broker considers a message delivered as soon as it is
    /// sent (`no_ack`). Fastest, but a crash loses in-flight messages.
    Auto,
}

/// What to do with a message whose body does not decode under `value_format`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OnDecodeError {
    /// Fail the run. The message stays unacknowledged and is redelivered.
    #[default]
    Fail,
    /// Reject the message without requeue — it is dropped, or routed to the
    /// queue's dead-letter exchange when one is configured — and continue.
    Skip,
}

/// A binding from an exchange to the consumed queue, declared before consuming.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RabbitMqBinding {
    /// Exchange to bind the queue to. Must not be the default exchange (`""`).
    pub exchange: String,
    /// Binding key (a pattern for `topic` exchanges). Defaults to `""`.
    #[serde(default)]
    pub routing_key: String,
    /// When set, the exchange is declared (durable) with this type before
    /// binding; otherwise it must already exist.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exchange_kind: Option<RabbitMqExchangeKind>,
}

/// Configuration for [`RabbitMqSource`](crate::RabbitMqSource).
///
/// The [`RabbitMqConnectionConfig`] surface (`url` / `host` / `port` / `vhost`
/// / `auth` / `tls` / …) is flattened in:
///
/// ```yaml
/// url: "amqp://guest:guest@127.0.0.1:5672/%2f"
/// queue: orders
/// idle_timeout_secs: 5
/// batch_size: 500
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RabbitMqSourceConfig {
    /// Connection settings.
    #[serde(flatten)]
    pub connection: RabbitMqConnectionConfig,

    /// Queue to consume from.
    pub queue: String,

    /// Declare the queue before consuming (idempotent when it already exists
    /// with the same properties). Defaults to `true`. Set `false` to consume
    /// a queue managed elsewhere (it must exist).
    #[serde(default = "default_true")]
    pub declare_queue: bool,

    /// Whether a declared queue is durable (survives a broker restart).
    /// Defaults to `true`. Must match an existing queue's durability.
    #[serde(default = "default_true")]
    pub queue_durable: bool,

    /// Exchange → queue bindings to declare before consuming.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bindings: Vec<RabbitMqBinding>,

    /// Consumer QoS window: the most unacknowledged deliveries the broker
    /// sends ahead. `0` is unlimited. Defaults to `batch_size` (capped at
    /// 65535; unlimited when `batch_size` is 0 or larger than 65535). Under
    /// `ack_mode: on_sink_confirm` it must be `0` or ≥ `batch_size`, or the
    /// broker would stop delivering before a page could fill.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefetch: Option<u16>,

    /// When deliveries are acknowledged. Defaults to `on_sink_confirm`.
    #[serde(default)]
    pub ack_mode: AckMode,

    /// How message bodies decode into records. Defaults to `json`.
    #[serde(default)]
    pub value_format: RabbitMqValueFormat,

    /// What to do with a body that fails to decode. Defaults to `fail`.
    #[serde(default)]
    pub on_decode_error: OnDecodeError,

    /// Wrap each record as `{ "data": <body>, "exchange", "routing_key",
    /// "delivery_tag", "redelivered", "headers", … }` instead of emitting the
    /// decoded body alone. Defaults to `false`.
    #[serde(default)]
    pub include_metadata: bool,

    /// Consumer tag. Defaults to a broker-generated one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer_tag: Option<String>,

    /// Stop after this many messages. At least one of `max_messages` /
    /// `idle_timeout_secs` must be set so a run terminates.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_messages: Option<usize>,

    /// Stop after this many seconds with no new message. At least one of
    /// `max_messages` / `idle_timeout_secs` must be set so a run terminates.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_timeout_secs: Option<u64>,

    /// Messages per emitted page (and per acknowledgement under
    /// `on_sink_confirm`). Defaults to [`DEFAULT_BATCH_SIZE`]. `0` drains the
    /// whole run window into one page.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

impl RabbitMqSourceConfig {
    /// A minimal config consuming `queue` from a local broker, stopping after
    /// 5 idle seconds.
    pub fn new(queue: impl Into<String>) -> Self {
        Self {
            connection: RabbitMqConnectionConfig::default(),
            queue: queue.into(),
            declare_queue: true,
            queue_durable: true,
            bindings: Vec::new(),
            prefetch: None,
            ack_mode: AckMode::default(),
            value_format: RabbitMqValueFormat::default(),
            on_decode_error: OnDecodeError::default(),
            include_metadata: false,
            consumer_tag: None,
            max_messages: None,
            idle_timeout_secs: Some(5),
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    /// The QoS prefetch actually requested from the broker.
    pub fn effective_prefetch(&self) -> u16 {
        match self.prefetch {
            Some(p) => p,
            None if self.batch_size == 0 => 0,
            None => u16::try_from(self.batch_size).unwrap_or(0),
        }
    }

    /// Validate the config (pure — no I/O).
    pub fn validate(&self) -> Result<(), FaucetError> {
        self.connection.validate()?;
        if self.queue.trim().is_empty() {
            return Err(FaucetError::Config(
                "rabbitmq source: `queue` must not be empty".into(),
            ));
        }
        for b in &self.bindings {
            if b.exchange.trim().is_empty() {
                return Err(FaucetError::Config(
                    "rabbitmq source: a binding's `exchange` must not be empty (the default \
                     exchange cannot be bound)"
                        .into(),
                ));
            }
        }
        if self.max_messages.is_none() && self.idle_timeout_secs.is_none() {
            return Err(FaucetError::Config(
                "rabbitmq source: at least one of `max_messages` or `idle_timeout_secs` must be \
                 set so the run terminates"
                    .into(),
            ));
        }
        if self.max_messages == Some(0) {
            return Err(FaucetError::Config(
                "rabbitmq source: `max_messages` must be greater than 0".into(),
            ));
        }
        if self.idle_timeout_secs == Some(0) {
            return Err(FaucetError::Config(
                "rabbitmq source: `idle_timeout_secs` must be greater than 0".into(),
            ));
        }
        faucet_core::validate_batch_size(self.batch_size)?;
        if self.ack_mode == AckMode::OnSinkConfirm {
            let prefetch = self.effective_prefetch();
            let too_small =
                prefetch != 0 && (self.batch_size == 0 || usize::from(prefetch) < self.batch_size);
            if too_small {
                return Err(FaucetError::Config(format!(
                    "rabbitmq source: `prefetch` ({prefetch}) is below `batch_size` ({}) under \
                     `ack_mode: on_sink_confirm` — the broker would stop delivering before a \
                     page fills; raise `prefetch`, set it to 0 (unlimited), or lower `batch_size`",
                    if self.batch_size == 0 {
                        "0 = whole run".to_string()
                    } else {
                        self.batch_size.to_string()
                    }
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn minimal_is_valid() {
        let c = RabbitMqSourceConfig::new("orders");
        assert!(c.validate().is_ok());
        assert_eq!(c.effective_prefetch(), 1000);
    }

    #[test]
    fn deserializes_with_defaults() {
        let c: RabbitMqSourceConfig = serde_json::from_value(json!({
            "url": "amqp://broker",
            "queue": "q",
            "max_messages": 10
        }))
        .unwrap();
        assert!(c.declare_queue && c.queue_durable);
        assert_eq!(c.ack_mode, AckMode::OnSinkConfirm);
        assert_eq!(c.value_format, RabbitMqValueFormat::Json);
        assert_eq!(c.on_decode_error, OnDecodeError::Fail);
        assert_eq!(c.batch_size, DEFAULT_BATCH_SIZE);
        assert!(c.validate().is_ok());
    }

    #[test]
    fn deserializes_bindings_and_modes() {
        let c: RabbitMqSourceConfig = serde_json::from_value(json!({
            "queue": "q",
            "idle_timeout_secs": 2,
            "ack_mode": "auto",
            "on_decode_error": "skip",
            "value_format": "bytes",
            "bindings": [{"exchange": "events", "routing_key": "a.*", "exchange_kind": "topic"}]
        }))
        .unwrap();
        assert_eq!(c.ack_mode, AckMode::Auto);
        assert_eq!(c.on_decode_error, OnDecodeError::Skip);
        assert_eq!(
            c.bindings[0].exchange_kind,
            Some(RabbitMqExchangeKind::Topic)
        );
        assert!(c.validate().is_ok());
    }

    #[test]
    fn rejects_empty_queue_and_binding_exchange() {
        let mut c = RabbitMqSourceConfig::new(" ");
        assert!(c.validate().unwrap_err().to_string().contains("queue"));
        c.queue = "q".into();
        c.bindings.push(RabbitMqBinding {
            exchange: "".into(),
            routing_key: "".into(),
            exchange_kind: None,
        });
        assert!(c.validate().unwrap_err().to_string().contains("binding"));
    }

    #[test]
    fn requires_a_terminator() {
        let mut c = RabbitMqSourceConfig::new("q");
        c.idle_timeout_secs = None;
        assert!(
            c.validate()
                .unwrap_err()
                .to_string()
                .contains("max_messages")
        );
        c.max_messages = Some(0);
        assert!(
            c.validate()
                .unwrap_err()
                .to_string()
                .contains("greater than 0")
        );
        c.max_messages = Some(5);
        assert!(c.validate().is_ok());
        c.idle_timeout_secs = Some(0);
        assert!(
            c.validate()
                .unwrap_err()
                .to_string()
                .contains("idle_timeout")
        );
    }

    #[test]
    fn rejects_batch_size_over_max() {
        let mut c = RabbitMqSourceConfig::new("q");
        c.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        assert!(matches!(c.validate(), Err(FaucetError::Config(_))));
    }

    #[test]
    fn prefetch_rules_under_on_sink_confirm() {
        let mut c = RabbitMqSourceConfig::new("q");
        c.prefetch = Some(10);
        assert!(c.validate().unwrap_err().to_string().contains("prefetch"));
        c.prefetch = Some(0);
        assert!(c.validate().is_ok());
        c.prefetch = Some(5000);
        assert!(c.validate().is_ok());

        c.prefetch = None;
        c.batch_size = 0;
        assert_eq!(c.effective_prefetch(), 0);
        assert!(c.validate().is_ok());
        c.prefetch = Some(100);
        assert!(c.validate().unwrap_err().to_string().contains("whole run"));

        c.prefetch = None;
        c.batch_size = 100_000;
        assert_eq!(c.effective_prefetch(), 0);
        assert!(c.validate().is_ok());

        c.ack_mode = AckMode::Auto;
        c.prefetch = Some(1);
        assert!(c.validate().is_ok());
    }

    #[test]
    fn connection_errors_propagate() {
        let mut c = RabbitMqSourceConfig::new("q");
        c.connection.connect_timeout_secs = 0;
        assert!(c.validate().is_err());
    }

    #[test]
    fn schema_compiles() {
        let _ = schemars::schema_for!(RabbitMqSourceConfig);
    }
}
