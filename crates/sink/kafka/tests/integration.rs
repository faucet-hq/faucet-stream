//! Integration tests for KafkaSink using a real Apache Kafka broker via
//! testcontainers.
//!
//! These tests require Docker to be running. Set RUST_LOG=info,rdkafka=warn
//! while debugging.
//!
//! Import notes for testcontainers-modules 0.15:
//! - `Kafka` lives at `testcontainers_modules::kafka::apache::Kafka`
//! - The port constant is `testcontainers_modules::kafka::apache::KAFKA_PORT`
//! - `AsyncRunner` is at `testcontainers::runners::AsyncRunner` (not via modules re-export)

use faucet_common_kafka::{CompressionType, KafkaAuth, KafkaValueFormat, OnKeyError};
use faucet_core::{DEFAULT_BATCH_SIZE, Sink};
use faucet_sink_kafka::{Acks, KafkaSink, KafkaSinkConfig, KafkaSinkTopic};
use rdkafka::ClientConfig;
use rdkafka::Message;
use rdkafka::consumer::{Consumer, StreamConsumer};
use serde_json::json;
use std::collections::BTreeMap;
use std::time::Duration;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{KAFKA_PORT, Kafka};

async fn start_kafka() -> (testcontainers::ContainerAsync<Kafka>, String) {
    let container = Kafka::default()
        .start()
        .await
        .expect("kafka container start");
    let port = container
        .get_host_port_ipv4(KAFKA_PORT)
        .await
        .expect("kafka port");
    (container, format!("127.0.0.1:{port}"))
}

fn sink_config(brokers: &str, topic: KafkaSinkTopic) -> KafkaSinkConfig {
    KafkaSinkConfig {
        brokers: brokers.into(),
        topic,
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
        message_timeout: Duration::from_secs(10),
        max_in_flight: 50,
        queue_full_backoff: Duration::from_millis(100),
        queue_full_max_retries: 3,
        transactional_id_prefix: None,
        exactly_once: None,
        commit_token_topic: "__faucet_commit_token".into(),
        commit_token_topic_partitions: 1,
        commit_token_topic_replication: -1,
        extra_client_config: BTreeMap::new(),
    }
}

async fn drain_topic(brokers: &str, topic: &str, expect: usize) -> Vec<String> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", format!("test-consumer-{topic}"))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("consumer init");
    consumer.subscribe(&[topic]).expect("subscribe");
    let mut out = Vec::new();
    while out.len() < expect {
        // 30s tolerates the initial JoinGroup/SyncGroup rebalance under CI load.
        let msg = tokio::time::timeout(Duration::from_secs(30), consumer.recv())
            .await
            .expect("recv timeout")
            .expect("recv");
        let payload = msg
            .payload()
            .map(|b| String::from_utf8_lossy(b).to_string())
            .unwrap_or_default();
        out.push(payload);
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn produce_and_consume_round_trip() {
    let (_c, brokers) = start_kafka().await;
    let topic = "rt";
    let sink = KafkaSink::new(sink_config(
        &brokers,
        KafkaSinkTopic::Fixed { name: topic.into() },
    ))
    .await
    .unwrap();
    let records = vec![json!({"id": 1}), json!({"id": 2}), json!({"id": 3})];
    let n = sink.write_batch(&records).await.unwrap();
    assert_eq!(n, 3);
    sink.flush().await.unwrap();
    let payloads = drain_topic(&brokers, topic, 3).await;
    assert_eq!(payloads.len(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn from_path_routes_to_per_record_topic() {
    let (_c, brokers) = start_kafka().await;
    let sink = KafkaSink::new(sink_config(
        &brokers,
        KafkaSinkTopic::FromPath {
            path: "$.dest".into(),
        },
    ))
    .await
    .unwrap();
    let records = vec![
        json!({"dest": "topic-a", "n": 1}),
        json!({"dest": "topic-b", "n": 2}),
        json!({"dest": "topic-a", "n": 3}),
    ];
    let n = sink.write_batch(&records).await.unwrap();
    assert_eq!(n, 3);
    sink.flush().await.unwrap();
    let a = drain_topic(&brokers, "topic-a", 2).await;
    let b = drain_topic(&brokers, "topic-b", 1).await;
    assert_eq!(a.len(), 2);
    assert_eq!(b.len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn on_key_error_skip_drops_records_without_key() {
    let (_c, brokers) = start_kafka().await;
    let topic = "skip";
    let mut cfg = sink_config(&brokers, KafkaSinkTopic::Fixed { name: topic.into() });
    cfg.key_path = Some("$.user_id".into());
    cfg.on_key_error = OnKeyError::Skip;
    let sink = KafkaSink::new(cfg).await.unwrap();
    let records = vec![
        json!({"user_id": "u1", "id": 1}),
        json!({"id": 2}), // no user_id — should be skipped
        json!({"user_id": "u3", "id": 3}),
    ];
    let n = sink.write_batch(&records).await.unwrap();
    assert_eq!(n, 2);
    sink.flush().await.unwrap();
    let payloads = drain_topic(&brokers, topic, 2).await;
    assert_eq!(payloads.len(), 2);
}

/// Verifies the streaming-pipeline contract: a 2000-record write_batch with a
/// `batch_size = 500` send window caps the FuturesUnordered at 500 in flight
/// (rather than the default `max_in_flight = 50`) and still drains cleanly
/// without any QueueFull surfacing. The broker-side
/// `queue.buffering.max.messages` is auto-set to 500 by `KafkaSink::new` —
/// exactly enough to hold the send window — and the QueueFull retry path
/// remains untouched.
#[tokio::test(flavor = "multi_thread")]
async fn batch_size_caps_in_flight_window_at_2000_records() {
    let (_c, brokers) = start_kafka().await;
    let topic = "bs-window";
    let mut cfg = sink_config(&brokers, KafkaSinkTopic::Fixed { name: topic.into() });
    cfg.batch_size = 500;
    // Set max_in_flight high enough that batch_size is the binding cap.
    cfg.max_in_flight = 1000;
    let sink = KafkaSink::new(cfg).await.unwrap();
    let records: Vec<_> = (0..2000).map(|i| json!({"id": i})).collect();
    let n = sink.write_batch(&records).await.unwrap();
    assert_eq!(n, 2000);
    sink.flush().await.unwrap();
    let payloads = drain_topic(&brokers, topic, 2000).await;
    assert_eq!(payloads.len(), 2000);
}

/// Forces backpressure by setting a tight `queue.buffering.max.messages` cap
/// via `extra_client_config` (which overrides the auto-derived value from
/// `batch_size`). With a tight broker buffer plus `batch_size = 500` capping
/// the in-flight send window, the existing QueueFull retry loop has enough
/// headroom (`queue_full_max_retries = 10`, `queue_full_backoff = 50ms`) to
/// drain successfully — verifying the new in-flight cap composes with the
/// existing retry semantics rather than replacing them.
#[tokio::test(flavor = "multi_thread")]
async fn batch_size_with_tight_queue_buffer_still_drains_via_retry() {
    let (_c, brokers) = start_kafka().await;
    let topic = "bs-backpressure";
    let mut cfg = sink_config(&brokers, KafkaSinkTopic::Fixed { name: topic.into() });
    cfg.batch_size = 500;
    cfg.max_in_flight = 1000;
    // Force backpressure: cap the producer's internal queue much lower than
    // batch_size so QueueFull is the expected hot path under high concurrency.
    cfg.extra_client_config
        .insert("queue.buffering.max.messages".into(), "100".into());
    // Generous retry budget so QueueFull never escalates to a fatal error.
    cfg.queue_full_max_retries = 50;
    cfg.queue_full_backoff = Duration::from_millis(50);
    let sink = KafkaSink::new(cfg).await.unwrap();
    let records: Vec<_> = (0..2000).map(|i| json!({"id": i})).collect();
    let n = sink.write_batch(&records).await.unwrap();
    assert_eq!(n, 2000);
    sink.flush().await.unwrap();
    let payloads = drain_topic(&brokers, topic, 2000).await;
    assert_eq!(payloads.len(), 2000);
}

/// `batch_size = 0` sentinel: the in-flight window is bounded only by
/// `max_in_flight` (the historical pre-streaming behaviour) and the producer's
/// `queue.buffering.max.messages` librdkafka knob keeps its default.
#[tokio::test(flavor = "multi_thread")]
async fn batch_size_zero_preserves_legacy_send_path() {
    let (_c, brokers) = start_kafka().await;
    let topic = "bs-zero";
    let mut cfg = sink_config(&brokers, KafkaSinkTopic::Fixed { name: topic.into() });
    cfg.batch_size = 0;
    cfg.max_in_flight = 50;
    let sink = KafkaSink::new(cfg).await.unwrap();
    let records: Vec<_> = (0..500).map(|i| json!({"id": i})).collect();
    let n = sink.write_batch(&records).await.unwrap();
    assert_eq!(n, 500);
    sink.flush().await.unwrap();
    let payloads = drain_topic(&brokers, topic, 500).await;
    assert_eq!(payloads.len(), 500);
}
