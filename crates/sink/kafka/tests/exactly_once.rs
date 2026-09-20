//! Exactly-once delivery integration tests for `KafkaSink` (#216).
//!
//! Covers the transactional write path and the commit-token round-trip:
//! `write_batch_idempotent` commits records + a token atomically; on a
//! simulated crash (sink dropped before any state persist) a rebuilt sink
//! reports the committed token via `last_committed_token`, so the pipeline
//! skips the replayed page — zero duplicates.
//!
//! Requires Docker. A single-broker container must enable transactions, so we
//! force the transaction-state-log replication/ISR + offsets replication to 1.

use faucet_common_kafka::{CompressionType, KafkaAuth, KafkaValueFormat, OnKeyError};
use faucet_core::idempotency::format_token;
use faucet_core::{DEFAULT_BATCH_SIZE, Sink};
use faucet_sink_kafka::{Acks, KafkaSink, KafkaSinkConfig, KafkaSinkTopic};
use rdkafka::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use serde_json::json;
use std::collections::BTreeMap;
use std::time::Duration;
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{KAFKA_PORT, Kafka};

async fn start_kafka() -> (testcontainers::ContainerAsync<Kafka>, String) {
    // Single-broker transactions need these replication/ISR settings at 1.
    let container = Kafka::default()
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env_var("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .start()
        .await
        .expect("kafka container start");
    let port = container
        .get_host_port_ipv4(KAFKA_PORT)
        .await
        .expect("kafka port");
    (container, format!("127.0.0.1:{port}"))
}

fn eo_config(brokers: &str, topic: &str) -> KafkaSinkConfig {
    KafkaSinkConfig {
        brokers: brokers.into(),
        topic: KafkaSinkTopic::Fixed { name: topic.into() },
        auth: KafkaAuth::None,
        value_format: KafkaValueFormat::Json,
        key_format: None,
        value_schema: None,
        key_schema: None,
        key_path: None,
        partition_path: None,
        headers_path: None,
        on_key_error: OnKeyError::Fail,
        compression: CompressionType::None,
        acks: Acks::All,
        idempotent: true,
        linger: Duration::from_millis(5),
        batch_size: DEFAULT_BATCH_SIZE,
        message_timeout: Duration::from_secs(15),
        max_in_flight: 50,
        queue_full_backoff: Duration::from_millis(100),
        queue_full_max_retries: 3,
        transactional_id_prefix: None,
        exactly_once: None,
        commit_token_topic: "__faucet_commit_token".into(),
        commit_token_topic_partitions: 1,
        commit_token_topic_replication: 1,
        extra_client_config: BTreeMap::new(),
    }
}

/// Count messages on `topic` by draining from the beginning until idle.
async fn count_messages(brokers: &str, topic: &str) -> usize {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "verifier")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("isolation.level", "read_committed")
        .create()
        .expect("verifier consumer");
    consumer.subscribe(&[topic]).expect("subscribe");
    let mut count = 0usize;
    loop {
        match tokio::time::timeout(Duration::from_secs(5), consumer.recv()).await {
            Ok(Ok(_msg)) => count += 1,
            Ok(Err(e)) => panic!("verifier recv error: {e}"),
            Err(_) => break, // idle timeout — done
        }
    }
    count
}

#[tokio::test]
async fn exactly_once_round_trip_and_no_duplicates_on_resume() {
    let (_container, brokers) = start_kafka().await;
    let topic = "eo_dest";
    let scope = "pipe::row0";

    // ---- Run 1: write page seq=1, then "crash" (drop sink, no state persist).
    {
        let sink = KafkaSink::new(eo_config(&brokers, topic)).await.unwrap();
        assert!(sink.supports_idempotent_writes());
        // Fresh topic → no committed token yet.
        assert_eq!(sink.last_committed_token(scope).await.unwrap(), None);

        let page1 = vec![json!({"id": 1}), json!({"id": 2})];
        let n = sink
            .write_batch_idempotent(&page1, scope, &format_token(1))
            .await
            .unwrap();
        assert_eq!(n, 2);
        sink.flush().await.unwrap();
        // sink dropped here — simulating a crash BEFORE the pipeline persists state.
    }

    // ---- Run 2: rebuilt sink reports page 1 committed, so the pipeline would skip it.
    let sink2 = KafkaSink::new(eo_config(&brokers, topic)).await.unwrap();
    let committed = sink2.last_committed_token(scope).await.unwrap();
    assert_eq!(
        committed,
        Some(format_token(1)),
        "page 1 must read back as committed"
    );

    // Pipeline logic skips seq<=committed, then writes the genuinely-new page 2.
    let page2 = vec![json!({"id": 3})];
    sink2
        .write_batch_idempotent(&page2, scope, &format_token(2))
        .await
        .unwrap();
    sink2.flush().await.unwrap();

    // Destination must hold exactly 3 records (2 + 1), no duplicate of page 1.
    let total = count_messages(&brokers, topic).await;
    assert_eq!(total, 3, "expected zero duplicates on resume");

    // And the latest committed token is now seq=2.
    assert_eq!(
        sink2.last_committed_token(scope).await.unwrap(),
        Some(format_token(2))
    );
}
