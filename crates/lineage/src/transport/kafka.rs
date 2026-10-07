//! Kafka transport — produce each event as a JSON message. Gated on
//! `transport-kafka`.

use super::Transport;
use async_trait::async_trait;
use faucet_core::FaucetError;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;

/// Upper bound on one event's delivery, so an unreachable broker delays a run by seconds, not minutes.
pub const DELIVERY_TIMEOUT: Duration = Duration::from_secs(10);

pub struct KafkaTransport {
    producer: FutureProducer,
    topic: String,
}

impl KafkaTransport {
    pub fn new(brokers: &str, topic: String) -> Result<Self, FaucetError> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set(
                "message.timeout.ms",
                DELIVERY_TIMEOUT.as_millis().to_string(),
            )
            .create()
            .map_err(|e| FaucetError::Custom(Box::new(e)))?;
        Ok(Self { producer, topic })
    }
}

#[async_trait]
impl Transport for KafkaTransport {
    async fn send(&self, event_json: Vec<u8>) -> Result<(), FaucetError> {
        let record: FutureRecord<'_, (), [u8]> = FutureRecord::to(&self.topic).payload(&event_json);
        self.producer
            .send(record, DELIVERY_TIMEOUT)
            .await
            .map_err(|(e, _)| FaucetError::Custom(Box::new(e)))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unreachable_broker_fails_within_the_delivery_timeout() {
        let t = KafkaTransport::new("127.0.0.1:1", "lineage".into()).unwrap();
        let started = std::time::Instant::now();
        let res = t.send(b"{}".to_vec()).await;
        assert!(res.is_err());
        assert!(
            started.elapsed() < DELIVERY_TIMEOUT * 3,
            "took {:?}",
            started.elapsed()
        );
    }
}
