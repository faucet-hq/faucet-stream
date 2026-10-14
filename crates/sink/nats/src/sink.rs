//! `NatsSink` — the NATS producer implementation (the one module that does I/O).
//!
//! Append-only: each record is serialized to JSON and published to a subject
//! (fixed, or per-record from `subject_field`). Core NATS flushes the client
//! after every `batch_size` records; JetStream mode awaits each publish's
//! acknowledgement and reports a per-row outcome. Every batch is bounded by
//! `publish_timeout_secs`.

use crate::config::NatsSinkConfig;
use async_trait::async_trait;
use bytes::Bytes;
use faucet_core::driver::{DriverSink, MessageDriver, MessageSinkDriver, OutboundMessage};
use faucet_core::{CallSlot, FaucetError, Sink};
use serde_json::Value;
use std::time::Duration;
use tokio::sync::OnceCell;

/// A sink that publishes each record as a JSON NATS message.
///
/// The client is built lazily on the first write (see [`NatsSink::new`]), so an
/// unreachable server fails on the first `write_batch` rather than at
/// construction time. The client is reused across batches.
///
/// Built on the messaging family driver (RFC 0014): `NatsDriver` encodes and
/// publishes; [`MessageSinkDriver`] drives the `batch_size` groups, the
/// `publish_timeout_secs` bound and the per-row outcomes.
pub struct NatsSink {
    inner: DriverSink<MessageSinkDriver<NatsDriver>>,
}

/// The NATS I/O primitives the messaging driver runs on.
pub(crate) struct NatsDriver {
    config: NatsSinkConfig,
    client: OnceCell<async_nats::Client>,
}

impl NatsSink {
    /// Create a new NATS sink. Validates the config but does **not** connect —
    /// the client is built lazily on the first write.
    pub async fn new(config: NatsSinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        Ok(Self {
            inner: DriverSink::new(MessageSinkDriver::new(NatsDriver {
                config,
                client: OnceCell::new(),
            })),
        })
    }

    #[cfg(test)]
    fn resolve_subject(&self, record: &Value) -> Result<String, FaucetError> {
        self.inner.driver().driver().resolve_subject(record)
    }
}

#[async_trait]
impl Sink for NatsSink {
    faucet_core::forward_sink!(inner);
}

impl NatsDriver {
    /// The shared NATS client, built once.
    async fn client(&self, calls: &CallSlot) -> Result<async_nats::Client, FaucetError> {
        self.client
            .get_or_try_init(|| {
                calls.call_once(
                    "connect",
                    faucet_common_nats::connect(&self.config.connection),
                )
            })
            .await
            .cloned()
    }

    /// Resolve the destination subject for a record: the per-record
    /// `subject_field` value when configured, else the fixed `subject`.
    fn resolve_subject(&self, record: &Value) -> Result<String, FaucetError> {
        match &self.config.subject_field {
            None => Ok(self.config.subject.clone()),
            Some(field) => match record.get(field) {
                Some(Value::String(s)) if !s.is_empty() => Ok(s.clone()),
                Some(Value::String(_)) => Err(FaucetError::Sink(format!(
                    "nats sink: subject_field '{field}' resolved to an empty string"
                ))),
                Some(other) => Err(FaucetError::Sink(format!(
                    "nats sink: subject_field '{field}' must be a string, got {other}"
                ))),
                None => Err(FaucetError::Sink(format!(
                    "nats sink: subject_field '{field}' is absent from record"
                ))),
            },
        }
    }
}

#[async_trait]
impl MessageDriver for NatsDriver {
    fn connector_name(&self) -> &'static str {
        "nats"
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(NatsSinkConfig)).unwrap_or(Value::Null)
    }

    fn dataset_uri(&self) -> String {
        let subject = match &self.config.subject_field {
            Some(f) => format!("(from_field:{f})"),
            None => self.config.subject.clone(),
        };
        faucet_common_nats::dataset_uri(&self.config.connection.servers, &subject)
    }

    fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        self.config.batch_atomicity()
    }

    /// A record whose subject cannot be resolved fails alone.
    fn encode(&self, record: &Value) -> Result<OutboundMessage, FaucetError> {
        let subject = self.resolve_subject(record)?;
        Ok(OutboundMessage::new(subject, message_payload(record)?))
    }

    fn group_size(&self) -> usize {
        self.config.batch_size
    }

    /// Every group is bounded by `publish_timeout_secs` (#789 MSG-59).
    fn group_timeout(&self) -> Option<Duration> {
        Some(Duration::from_secs(self.config.publish_timeout_secs))
    }

    async fn connect(&self, calls: &CallSlot) -> Result<(), FaucetError> {
        self.client(calls).await.map(|_| ())
    }

    /// Core NATS: the group's publishes then a flush. JetStream: the group's
    /// publishes, then each acknowledgement (#789 MSG-31).
    async fn publish(
        &self,
        calls: &CallSlot,
        group: Vec<OutboundMessage>,
    ) -> Result<Vec<faucet_core::RowOutcome>, FaucetError> {
        let client = self.client(calls).await?;
        let mut outcomes: Vec<faucet_core::RowOutcome> = group.iter().map(|_| Ok(())).collect();
        if self.config.jetstream {
            let js = async_nats::jetstream::new(client);
            let mut acks = Vec::new();
            for (i, m) in group.into_iter().enumerate() {
                match js.publish(m.destination, Bytes::from(m.payload)).await {
                    Ok(ack) => acks.push((i, ack)),
                    Err(e) => {
                        outcomes[i] = Err(FaucetError::Sink(format!("nats jetstream publish: {e}")))
                    }
                }
            }
            for (i, ack) in acks {
                if let Err(e) = ack.await {
                    outcomes[i] = Err(FaucetError::Sink(format!(
                        "nats jetstream publish not acknowledged: {e}"
                    )));
                }
            }
        } else {
            for m in group {
                client
                    .publish(m.destination, Bytes::from(m.payload))
                    .await
                    .map_err(|e| FaucetError::Sink(format!("nats publish: {e}")))?;
            }
            client
                .flush()
                .await
                .map_err(|e| FaucetError::Sink(format!("nats flush: {e}")))?;
        }
        Ok(outcomes)
    }

    async fn flush(&self, calls: &CallSlot) -> Result<(), FaucetError> {
        self.client(calls)
            .await?
            .flush()
            .await
            .map_err(|e| FaucetError::Sink(format!("nats flush: {e}")))
    }
}

/// The message body for `record`: its JSON text.
fn message_payload(record: &Value) -> Result<Bytes, serde_json::Error> {
    serde_json::to_vec(record).map(Bytes::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    /// The value-fidelity corpus (#885): every record this sink encodes reads
    /// back unchanged.
    #[test]
    fn the_fidelity_corpus_encodes_unchanged() {
        use faucet_conformance::fidelity;
        let sent = fidelity::corpus();
        let mut landed = Vec::new();
        for record in &sent {
            let bytes: Vec<u8> = message_payload(record).unwrap().to_vec();
            landed.push(faucet_core::fidelity::from_slice(&bytes).unwrap());
        }
        fidelity::assert_round_trip(&sent, &landed, fidelity::Tolerance::exact());
    }

    use serde_json::json;

    /// Grouping and the publish bound reach the messaging driver.
    #[tokio::test]
    async fn the_driver_publishes_in_batch_size_groups_bounded_by_the_timeout() {
        let mut cfg = NatsSinkConfig::new("events.out");
        cfg.batch_size = 7;
        cfg.publish_timeout_secs = 3;
        let sink = NatsSink::new(cfg).await.unwrap();
        let driver = sink.inner.driver().driver();
        assert_eq!(driver.group_size(), 7);
        assert_eq!(driver.group_timeout(), Some(Duration::from_secs(3)));
        let m = driver.encode(&json!({"id": 1})).unwrap();
        assert_eq!(m.destination, "events.out");
        assert_eq!(m.payload, br#"{"id":1}"#.to_vec());
        assert!(!sink.supports_idempotent_writes());
    }

    #[tokio::test]
    async fn schema_and_a_per_record_subject_uri() {
        let mut cfg = NatsSinkConfig::new("events.out");
        cfg.subject_field = Some("topic".into());
        let sink = NatsSink::new(cfg).await.unwrap();
        assert!(sink.config_schema().is_object());
        assert!(sink.dataset_uri().contains("from_field:topic"));
    }

    #[tokio::test]
    async fn new_validates_config() {
        let mut cfg = NatsSinkConfig::new("x");
        cfg.subject = " ".into();
        assert!(NatsSink::new(cfg).await.is_err());
    }

    #[tokio::test]
    async fn resolve_subject_fixed() {
        let sink = NatsSink::new(NatsSinkConfig::new("events.out"))
            .await
            .unwrap();
        let s = sink.resolve_subject(&json!({"id": 1})).unwrap();
        assert_eq!(s, "events.out");
    }

    #[tokio::test]
    async fn resolve_subject_from_field() {
        let mut cfg = NatsSinkConfig::new("events.out");
        cfg.subject_field = Some("topic".into());
        let sink = NatsSink::new(cfg).await.unwrap();
        let s = sink
            .resolve_subject(&json!({"topic": "orders.new", "id": 1}))
            .unwrap();
        assert_eq!(s, "orders.new");
    }

    #[tokio::test]
    async fn resolve_subject_missing_field_errors() {
        let mut cfg = NatsSinkConfig::new("events.out");
        cfg.subject_field = Some("topic".into());
        let sink = NatsSink::new(cfg).await.unwrap();
        let err = sink.resolve_subject(&json!({"id": 1})).unwrap_err();
        assert!(matches!(err, FaucetError::Sink(_)));
    }

    #[tokio::test]
    async fn resolve_subject_non_string_field_errors() {
        let mut cfg = NatsSinkConfig::new("events.out");
        cfg.subject_field = Some("topic".into());
        let sink = NatsSink::new(cfg).await.unwrap();
        let err = sink.resolve_subject(&json!({"topic": 42})).unwrap_err();
        assert!(matches!(err, FaucetError::Sink(_)));
    }

    #[tokio::test]
    async fn connector_name_and_uri() {
        let sink = NatsSink::new(NatsSinkConfig::new("events.out"))
            .await
            .unwrap();
        assert_eq!(sink.connector_name(), "nats");
        assert!(sink.dataset_uri().contains("subject=events.out"));
    }

    #[tokio::test]
    async fn write_batch_empty_is_noop_no_connect() {
        // No connect happens on an empty batch, so an unreachable server is fine.
        let mut cfg = NatsSinkConfig::new("events.out");
        cfg.connection.servers = vec!["nats://127.0.0.1:1".into()];
        let sink = NatsSink::new(cfg).await.unwrap();
        assert_eq!(sink.write_batch(&[]).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn write_batch_unreachable_errors_not_panics() {
        let mut cfg = NatsSinkConfig::new("events.out");
        cfg.connection.servers = vec!["nats://127.0.0.1:1".into()];
        let sink = NatsSink::new(cfg).await.unwrap();
        let err = sink.write_batch(&[json!({"id": 1})]).await.unwrap_err();
        assert!(matches!(err, FaucetError::Custom(_)));
    }
}
