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
use faucet_core::{FaucetError, Sink};
use serde_json::Value;
use std::time::Duration;
use tokio::sync::OnceCell;

/// A sink that publishes each record as a JSON NATS message.
///
/// The client is built lazily on the first write (see [`NatsSink::new`]), so an
/// unreachable server fails on the first `write_batch` rather than at
/// construction time. The client is reused across batches.
pub struct NatsSink {
    config: NatsSinkConfig,
    client: OnceCell<async_nats::Client>,
}

impl NatsSink {
    /// Create a new NATS sink. Validates the config but does **not** connect —
    /// the client is built lazily on the first write.
    pub async fn new(config: NatsSinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        Ok(Self {
            config,
            client: OnceCell::new(),
        })
    }

    /// Lazily build (once) and return the shared NATS client.
    async fn client(&self) -> Result<async_nats::Client, FaucetError> {
        self.client
            .get_or_try_init(|| faucet_common_nats::connect(&self.config.connection))
            .await
            .cloned()
    }

    /// Bound `fut` by `publish_timeout_secs` (#789 MSG-59).
    async fn bounded<F: std::future::Future>(
        &self,
        fut: F,
        what: &str,
    ) -> Result<F::Output, FaucetError> {
        tokio::time::timeout(Duration::from_secs(self.config.publish_timeout_secs), fut)
            .await
            .map_err(|_| {
                FaucetError::Sink(format!(
                    "nats {what} did not complete within {}s (server unreachable?)",
                    self.config.publish_timeout_secs
                ))
            })
    }

    /// Publish every record in `batch_size` groups and return one outcome
    /// per record. A record whose subject cannot be resolved fails alone.
    /// Core NATS: a group's publishes then a flush. JetStream: the group's
    /// publishes, then each acknowledgement (#789 MSG-31).
    async fn publish_all(
        &self,
        records: &[Value],
    ) -> Result<Vec<faucet_core::RowOutcome>, FaucetError> {
        let mut outcomes: Vec<faucet_core::RowOutcome> = records.iter().map(|_| Ok(())).collect();
        if records.is_empty() {
            return Ok(outcomes);
        }
        let client = self.client().await?;
        let js = self
            .config
            .jetstream
            .then(|| async_nats::jetstream::new(client.clone()));
        let group = if self.config.batch_size == 0 {
            records.len()
        } else {
            self.config.batch_size
        };
        for (offset, chunk) in records.chunks(group).enumerate() {
            let base = offset * group;
            let work = async {
                let mut acks = Vec::new();
                for (i, record) in chunk.iter().enumerate() {
                    let subject = match self.resolve_subject(record) {
                        Ok(s) => s,
                        Err(e) => {
                            outcomes[base + i] = Err(e);
                            continue;
                        }
                    };
                    let payload = Bytes::from(serde_json::to_vec(record)?);
                    match &js {
                        Some(js) => match js.publish(subject, payload).await {
                            Ok(ack) => acks.push((base + i, ack)),
                            Err(e) => {
                                outcomes[base + i] =
                                    Err(FaucetError::Sink(format!("nats jetstream publish: {e}")))
                            }
                        },
                        None => client
                            .publish(subject, payload)
                            .await
                            .map_err(|e| FaucetError::Sink(format!("nats publish: {e}")))?,
                    }
                }
                if js.is_some() {
                    for (idx, ack) in acks {
                        if let Err(e) = ack.await {
                            outcomes[idx] = Err(FaucetError::Sink(format!(
                                "nats jetstream publish not acknowledged: {e}"
                            )));
                        }
                    }
                } else {
                    client
                        .flush()
                        .await
                        .map_err(|e| FaucetError::Sink(format!("nats flush: {e}")))?;
                }
                Ok::<(), FaucetError>(())
            };
            self.bounded(work, "publish").await??;
        }
        Ok(outcomes)
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
impl Sink for NatsSink {
    fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        self.config.batch_atomicity()
    }

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        let outcomes = self.publish_all(records).await?;
        match outcomes.into_iter().find_map(Result::err) {
            Some(e) => Err(e),
            None => Ok(records.len()),
        }
    }

    async fn write_batch_partial(
        &self,
        records: &[Value],
    ) -> Result<Vec<faucet_core::RowOutcome>, FaucetError> {
        self.publish_all(records).await
    }

    async fn flush(&self) -> Result<(), FaucetError> {
        let client = self.client().await?;
        self.bounded(client.flush(), "flush")
            .await?
            .map_err(|e| FaucetError::Sink(format!("nats flush: {e}")))?;
        Ok(())
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(NatsSinkConfig)).unwrap_or(Value::Null)
    }

    fn connector_name(&self) -> &'static str {
        "nats"
    }

    fn dataset_uri(&self) -> String {
        let subject = match &self.config.subject_field {
            Some(f) => format!("(from_field:{f})"),
            None => self.config.subject.clone(),
        };
        faucet_common_nats::dataset_uri(&self.config.connection.servers, &subject)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
