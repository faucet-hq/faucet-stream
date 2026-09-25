//! Exchange types and the declare helpers shared by the source and the sink.

use crate::connection::amqp_error;
use faucet_core::FaucetError;
use lapin::options::ExchangeDeclareOptions;
use lapin::types::FieldTable;
use lapin::{Channel, ExchangeKind};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// An AMQP exchange type, used when a connector declares an exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RabbitMqExchangeKind {
    /// Route on an exact routing-key match.
    Direct,
    /// Broadcast to every bound queue, ignoring the routing key.
    Fanout,
    /// Route on a dotted routing-key pattern (`*` one word, `#` zero or more).
    Topic,
    /// Route on message header values.
    Headers,
}

impl RabbitMqExchangeKind {
    /// The lapin exchange kind.
    pub fn to_lapin(self) -> ExchangeKind {
        match self {
            RabbitMqExchangeKind::Direct => ExchangeKind::Direct,
            RabbitMqExchangeKind::Fanout => ExchangeKind::Fanout,
            RabbitMqExchangeKind::Topic => ExchangeKind::Topic,
            RabbitMqExchangeKind::Headers => ExchangeKind::Headers,
        }
    }
}

/// Declare a durable exchange of `kind`. Idempotent when the exchange already
/// exists with the same properties; a mismatch surfaces as a config error.
pub async fn declare_exchange(
    channel: &Channel,
    name: &str,
    kind: RabbitMqExchangeKind,
) -> Result<(), FaucetError> {
    channel
        .exchange_declare(
            name.into(),
            kind.to_lapin(),
            ExchangeDeclareOptions {
                durable: true,
                ..ExchangeDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .map_err(|e| amqp_error(&format!("declare exchange '{name}'"), e))
}

/// Passively check that an exchange exists (no side effects).
pub async fn check_exchange(channel: &Channel, name: &str) -> Result<(), FaucetError> {
    channel
        .exchange_declare(
            name.into(),
            ExchangeKind::Direct,
            ExchangeDeclareOptions {
                passive: true,
                ..ExchangeDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .map_err(|e| amqp_error(&format!("exchange '{name}'"), e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn kinds_map_to_lapin() {
        assert_eq!(
            RabbitMqExchangeKind::Direct.to_lapin(),
            ExchangeKind::Direct
        );
        assert_eq!(
            RabbitMqExchangeKind::Fanout.to_lapin(),
            ExchangeKind::Fanout
        );
        assert_eq!(RabbitMqExchangeKind::Topic.to_lapin(), ExchangeKind::Topic);
        assert_eq!(
            RabbitMqExchangeKind::Headers.to_lapin(),
            ExchangeKind::Headers
        );
    }

    #[test]
    fn kinds_deserialize_snake_case() {
        let k: RabbitMqExchangeKind = serde_json::from_value(json!("topic")).unwrap();
        assert_eq!(k, RabbitMqExchangeKind::Topic);
        assert!(serde_json::from_value::<RabbitMqExchangeKind>(json!("x-custom")).is_err());
    }
}
