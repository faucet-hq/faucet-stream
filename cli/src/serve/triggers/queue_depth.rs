//! `queue_depth` trigger: poll a queue's depth and fire edge-triggered (once per
//! rising crossing of `threshold`, suppressed until it drains). The `Edge` is
//! pure; `DepthProbe` is the IO seam (Redis/Kafka impls + a fake in tests).

use super::context::TriggerEvent;
use super::enqueue::{self};
use super::spec::QueueSpec;
use super::watcher::Watcher;
use crate::serve::state::ServerState;
use async_trait::async_trait;
use std::time::Duration;

/// The IO seam: returns the current depth of the queue.
#[async_trait]
pub trait DepthProbe: Send + Sync {
    async fn depth(&self) -> Result<u64, String>;
    fn queue_label(&self) -> String;
}

/// Polls a queue and fires once per rising crossing of `threshold`, re-arming
/// only after the depth drops below it. The edge (armed flag + ordinal) lives
/// in the run history, so the idempotency key `trig:<name>:edge:<ordinal>` is
/// never reused after a restart and every cluster instance agrees on which
/// instance fired a crossing.
pub struct QueueDepthWatcher {
    name: String,
    probe: Box<dyn DepthProbe>,
    threshold: u64,
    known_armed: bool,
    poll: Duration,
    compiled: std::sync::Arc<super::compiled::CompiledTrigger>,
}

impl QueueDepthWatcher {
    pub fn new(
        compiled: std::sync::Arc<super::compiled::CompiledTrigger>,
        probe: Box<dyn DepthProbe>,
        threshold: u64,
        poll: Duration,
    ) -> Self {
        Self {
            name: compiled.name().to_string(),
            probe,
            threshold,
            known_armed: false,
            poll,
            compiled,
        }
    }
}

#[async_trait]
impl Watcher for QueueDepthWatcher {
    fn name(&self) -> &str {
        &self.name
    }
    fn kind(&self) -> &'static str {
        "queue_depth"
    }
    fn poll_interval(&self) -> Duration {
        self.poll
    }

    async fn poll(&mut self, state: &ServerState) -> Result<bool, String> {
        let depth = self.probe.depth().await?;
        let history = state.history();
        if depth < self.threshold {
            if !self.known_armed {
                history
                    .trigger_edge_rearm(&self.name)
                    .await
                    .map_err(|e| e.to_string())?;
                self.known_armed = true;
            }
            return Ok(false);
        }
        let Some(edge) = history
            .trigger_edge_rise(&self.name)
            .await
            .map_err(|e| e.to_string())?
        else {
            self.known_armed = false;
            return Ok(false);
        };
        self.known_armed = false;
        let event = TriggerEvent::QueueDepth {
            queue: self.probe.queue_label(),
            depth,
            edge,
        };
        let fired_at = chrono::Utc::now().to_rfc3339();
        let outcome = enqueue::fire(state, &self.compiled, event, &fired_at).await;
        if !outcome.committed() {
            history
                .trigger_edge_retract(&self.name, edge)
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(outcome.committed())
    }
}

/// Build the depth probe for a queue spec (feature-gated backends).
pub fn build_probe(queue: &QueueSpec) -> Result<Box<dyn DepthProbe>, String> {
    match queue {
        #[cfg(feature = "triggers-redis")]
        QueueSpec::Redis { url, key, kind } => Ok(Box::new(redis_probe::RedisProbe::new(
            url.clone(),
            key.clone(),
            *kind,
        ))),
        #[cfg(not(feature = "triggers-redis"))]
        QueueSpec::Redis { .. } => {
            Err("queue_depth redis requires the `triggers-redis` feature".into())
        }
        #[cfg(feature = "triggers-kafka")]
        QueueSpec::Kafka {
            brokers,
            topic,
            group,
        } => Ok(Box::new(kafka_probe::KafkaProbe::new(
            brokers.clone(),
            topic.clone(),
            group.clone(),
        ))),
        #[cfg(not(feature = "triggers-kafka"))]
        QueueSpec::Kafka { .. } => {
            Err("queue_depth kafka requires the `triggers-kafka` feature".into())
        }
    }
}

#[cfg(feature = "triggers-redis")]
mod redis_probe {
    use super::DepthProbe;
    use crate::serve::triggers::spec::RedisQueueKind;
    use async_trait::async_trait;
    use redis::AsyncCommands;

    pub struct RedisProbe {
        url: String,
        key: String,
        kind: RedisQueueKind,
    }
    impl RedisProbe {
        pub fn new(url: String, key: String, kind: RedisQueueKind) -> Self {
            Self { url, key, kind }
        }
    }
    #[async_trait]
    impl DepthProbe for RedisProbe {
        async fn depth(&self) -> Result<u64, String> {
            // A fresh connection per poll is intentional: depth polling is low-frequency
            // (poll_interval_secs, default 30s), so a pooled/cached client isn't worth it.
            let client = redis::Client::open(self.url.as_str())
                .map_err(|e| format!("invalid Redis URL: {e}"))?;
            let mut conn = client
                .get_multiplexed_async_connection()
                .await
                .map_err(|e| format!("Redis connect: {e}"))?;
            let n: i64 = match self.kind {
                RedisQueueKind::List => conn
                    .llen(&self.key)
                    .await
                    .map_err(|e| format!("LLEN: {e}"))?,
                RedisQueueKind::Stream => conn
                    .xlen(&self.key)
                    .await
                    .map_err(|e| format!("XLEN: {e}"))?,
            };
            Ok(n.max(0) as u64)
        }
        fn queue_label(&self) -> String {
            self.key.clone()
        }
    }
}

#[cfg(feature = "triggers-kafka")]
mod kafka_probe {
    use super::DepthProbe;
    use async_trait::async_trait;
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::{ClientConfig, Offset, TopicPartitionList};
    use std::time::Duration;

    pub struct KafkaProbe {
        brokers: String,
        topic: String,
        group: String,
    }
    impl KafkaProbe {
        pub fn new(brokers: String, topic: String, group: String) -> Self {
            Self {
                brokers,
                topic,
                group,
            }
        }
    }
    #[async_trait]
    impl DepthProbe for KafkaProbe {
        async fn depth(&self) -> Result<u64, String> {
            // rdkafka is sync; run on a blocking thread.
            let brokers = self.brokers.clone();
            let topic = self.topic.clone();
            let group = self.group.clone();
            tokio::task::spawn_blocking(move || -> Result<u64, String> {
                let consumer: BaseConsumer = ClientConfig::new()
                    .set("bootstrap.servers", &brokers)
                    .set("group.id", &group)
                    .set("enable.auto.commit", "false")
                    .create()
                    .map_err(|e| format!("kafka consumer: {e}"))?;
                let meta = consumer
                    .fetch_metadata(Some(&topic), Duration::from_secs(10))
                    .map_err(|e| format!("kafka metadata: {e}"))?;
                let parts: Vec<i32> = meta
                    .topics()
                    .iter()
                    .find(|t| t.name() == topic)
                    .map(|t| t.partitions().iter().map(|p| p.id()).collect())
                    .unwrap_or_default();
                if parts.is_empty() {
                    return Err(format!(
                        "kafka topic '{topic}' has no partitions in metadata (check the topic name / broker permissions)"
                    ));
                }
                // Sum (high watermark - committed) across partitions = consumer lag.
                let mut tpl = TopicPartitionList::new();
                for p in &parts {
                    tpl.add_partition(&topic, *p);
                }
                let committed = consumer
                    .committed_offsets(tpl, Duration::from_secs(10))
                    .map_err(|e| format!("kafka committed: {e}"))?;
                let mut lag: i64 = 0;
                for p in &parts {
                    let (_low, high) = consumer
                        .fetch_watermarks(&topic, *p, Duration::from_secs(10))
                        .map_err(|e| format!("kafka watermarks: {e}"))?;
                    let committed_off = committed
                        .find_partition(&topic, *p)
                        .map(|e| e.offset())
                        .unwrap_or(Offset::Invalid);
                    let c = match committed_off {
                        Offset::Offset(n) => n,
                        _ => 0,
                    };
                    lag += (high - c).max(0);
                }
                Ok(lag.max(0) as u64)
            })
            .await
            .map_err(|e| format!("kafka probe join: {e}"))?
        }
        fn queue_label(&self) -> String {
            self.topic.clone()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_edge_fires_once_per_crossing_and_retracts_a_failed_fire() {
        use crate::serve::history::RunHistory;
        let h = crate::serve::history::memory::MemoryHistory::new(Duration::from_secs(60));
        assert_eq!(h.trigger_edge_rise("q").await.unwrap(), Some(1));
        assert_eq!(h.trigger_edge_rise("q").await.unwrap(), None);
        h.trigger_edge_rearm("q").await.unwrap();
        assert_eq!(h.trigger_edge_rise("q").await.unwrap(), Some(2));
        h.trigger_edge_retract("q", 1).await.unwrap();
        assert_eq!(
            h.trigger_edge_rise("q").await.unwrap(),
            None,
            "stale retract"
        );
        h.trigger_edge_retract("q", 2).await.unwrap();
        assert_eq!(h.trigger_edge_rise("q").await.unwrap(), Some(2), "retried");
        h.trigger_edge_rearm("other").await.unwrap();
    }
}
