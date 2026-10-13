//! The sink writes Confluent-wire Avro to a real broker, registering its
//! schema with a (mocked) Schema Registry, and honours `bytes_encoding`.
//!
//! Requires Docker.
#![cfg(feature = "schema-registry")]

use faucet_common_kafka::{
    CompressionType, KafkaAuth, KafkaValueFormat, OnKeyError, SchemaRegistryConfig,
};
use faucet_core::{DEFAULT_BATCH_SIZE, Sink};
use faucet_sink_kafka::{Acks, KafkaSink, KafkaSinkConfig, KafkaSinkTopic};
use rdkafka::ClientConfig;
use rdkafka::Message;
use rdkafka::consumer::{Consumer, StreamConsumer};
use serde_json::json;
use std::collections::BTreeMap;
use std::time::Duration;
use testcontainers_modules::kafka::apache::{KAFKA_PORT, Kafka};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const SCHEMA: &str = r#"{"type":"record","name":"R","fields":[{"name":"raw","type":"bytes"}]}"#;

fn sink_config(brokers: &str, topic: &str, value_format: KafkaValueFormat) -> KafkaSinkConfig {
    KafkaSinkConfig {
        brokers: brokers.into(),
        topic: KafkaSinkTopic::Fixed { name: topic.into() },
        auth: KafkaAuth::None,
        value_format,
        key_format: None,
        value_schema: Some(SCHEMA.into()),
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

fn avro_format(registry: &str, bytes_encoding: &str) -> KafkaValueFormat {
    serde_json::from_value(json!({
        "type": "confluent_avro",
        "schema_registry": SchemaRegistryConfig::new(registry),
        "bytes_encoding": bytes_encoding,
    }))
    .expect("value format")
}

async fn drain(brokers: &str, topic: &str, expect: usize) -> Vec<Vec<u8>> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", format!("sr-{topic}"))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("consumer");
    consumer.subscribe(&[topic]).expect("subscribe");
    let mut out = Vec::new();
    while out.len() < expect {
        let msg = tokio::time::timeout(Duration::from_secs(30), consumer.recv())
            .await
            .expect("recv timeout")
            .expect("recv");
        out.push(msg.payload().unwrap_or_default().to_vec());
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn confluent_avro_honours_bytes_encoding_against_a_real_broker() {
    let registry = MockServer::start().await;
    for (subject, id) in [("utf8-value", 7), ("hex-value", 8)] {
        Mock::given(method("POST"))
            .and(path(format!("/subjects/{subject}/versions")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "id": id })))
            .expect(1)
            .mount(&registry)
            .await;
    }
    let container = faucet_conformance::containers::start(Kafka::default).await;
    let port = container
        .get_host_port_ipv4(KAFKA_PORT)
        .await
        .expect("kafka port");
    let brokers = format!("127.0.0.1:{port}");

    for (topic, encoding) in [("utf8", "utf8"), ("hex", "hex")] {
        let sink = KafkaSink::new(sink_config(
            &brokers,
            topic,
            avro_format(&registry.uri(), encoding),
        ))
        .await
        .expect("sink");
        assert_eq!(
            sink.write_batch(&[json!({ "raw": "cafe" })]).await.unwrap(),
            1
        );
        sink.flush().await.unwrap();
    }

    let utf8 = drain(&brokers, "utf8", 1).await;
    assert_eq!(
        utf8[0],
        [&[0u8, 0, 0, 0, 7, 8][..], b"cafe"].concat(),
        "utf8 writes the string's bytes under schema id 7"
    );
    let hex = drain(&brokers, "hex", 1).await;
    assert_eq!(
        hex[0],
        vec![0u8, 0, 0, 0, 8, 4, 0xca, 0xfe],
        "hex decodes the string under schema id 8"
    );
}

/// The run's call context and round-trip recorder reach the Schema Registry
/// client: the registration a write makes is counted on the recorder, before
/// any broker is involved.
#[tokio::test(flavor = "multi_thread")]
async fn the_registry_client_gets_the_runs_context_and_recorder() {
    use faucet_core::observability::{RoundtripRecorder, RoundtripSide};
    let registry = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/subjects/sr-ctx-value/versions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": 7})))
        .mount(&registry)
        .await;
    let mut cfg = sink_config(
        "127.0.0.1:1",
        "sr-ctx",
        avro_format(&registry.uri(), "utf8"),
    );
    cfg.message_timeout = Duration::from_millis(500);
    let sink = KafkaSink::new(cfg)
        .await
        .expect("sink builds without a broker");
    let meter = std::sync::Arc::new(faucet_core::UsageMeter::new());
    sink.set_call_context(faucet_core::CallContext::new());
    sink.set_roundtrip_recorder(std::sync::Arc::new(
        RoundtripRecorder::new(RoundtripSide::Sink, "p", "", "kafka")
            .with_meter(std::sync::Arc::clone(&meter)),
    ));
    let _ = sink.write_batch(&[json!({"raw": "aGk="})]).await;
    assert!(
        !registry
            .received_requests()
            .await
            .unwrap_or_default()
            .is_empty(),
        "the schema was registered"
    );
    assert!(
        meter
            .snapshot()
            .sink_roundtrips
            .get("register")
            .copied()
            .unwrap_or(0)
            >= 1,
        "{:?}",
        meter.snapshot()
    );
}
