//! Confluent Protobuf and JSON Schema end to end against a real Schema
//! Registry (#967): Redpanda's broker and built-in registry in one container.
//! The sink registers the schema and writes Confluent-wire messages; the
//! registry is then asked for the subject's schema, and each message is
//! decoded with the same registry client and decoders the Kafka source uses.
//!
//! Requires Docker.
#![cfg(feature = "schema-registry")]

use faucet_common_kafka::schema_registry::client::SchemaRegistryClient;
use faucet_common_kafka::schema_registry::{json_schema, protobuf};
use faucet_common_kafka::{
    CompressionType, KafkaAuth, KafkaValueFormat, OnKeyError, SchemaRegistryConfig,
};
use faucet_core::{DEFAULT_BATCH_SIZE, Sink};
use faucet_sink_kafka::{Acks, KafkaSink, KafkaSinkConfig, KafkaSinkTopic};
use rdkafka::ClientConfig;
use rdkafka::Message;
use rdkafka::consumer::{Consumer, StreamConsumer};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::time::Duration;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

const IMAGE: (&str, &str) = ("redpandadata/redpanda", "v24.2.7");
const PROTO: &str =
    "syntax = \"proto3\";\nmessage Order {\n  int64 id = 1;\n  string name = 2;\n}\n";
const JSON_SCHEMA: &str = r#"{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"}},"required":["id"]}"#;

struct Redpanda {
    _container: ContainerAsync<GenericImage>,
    brokers: String,
    registry: String,
}

async fn redpanda() -> Redpanda {
    // The broker advertises the host port, so both are chosen before start.
    let free = || {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    };
    let (kafka_port, registry_port) = (free(), free());
    let ready = faucet_conformance::containers::StartOptions::default().ready(
        faucet_conformance::containers::ReadyProbe::http(8081, "/subjects"),
    );
    let container = faucet_conformance::containers::start_with(|| {
        GenericImage::new(IMAGE.0, IMAGE.1)
            .with_wait_for(WaitFor::message_on_stderr("Successfully started Redpanda!"))
            .with_mapped_port(kafka_port, 19092.tcp())
            .with_mapped_port(registry_port, 8081.tcp())
            .with_cmd([
                "redpanda".to_string(),
                "start".to_string(),
                "--mode=dev-container".to_string(),
                "--smp=1".to_string(),
                // The registry's own client takes the first (in-container) listener.
                "--kafka-addr=internal://0.0.0.0:9092,external://0.0.0.0:19092".to_string(),
                format!(
                    "--advertise-kafka-addr=internal://127.0.0.1:9092,external://127.0.0.1:{kafka_port}"
                ),
                "--schema-registry-addr=0.0.0.0:8081".to_string(),
            ])
    }, &ready)
    .await;
    Redpanda {
        _container: container,
        brokers: format!("127.0.0.1:{kafka_port}"),
        registry: format!("http://127.0.0.1:{registry_port}"),
    }
}

fn sink_config(
    brokers: &str,
    topic: &str,
    value_format: KafkaValueFormat,
    schema: &str,
) -> KafkaSinkConfig {
    KafkaSinkConfig {
        brokers: brokers.into(),
        topic: KafkaSinkTopic::Fixed { name: topic.into() },
        auth: KafkaAuth::None,
        value_format,
        key_format: None,
        value_schema: Some(schema.into()),
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
        message_timeout: Duration::from_secs(30),
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

async fn drain(brokers: &str, topic: &str, expect: usize) -> Vec<Vec<u8>> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", format!("real-sr-{topic}"))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    consumer.subscribe(&[topic]).unwrap();
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

/// The subject's latest `(id, schemaType, schema)` from the registry.
async fn latest(registry: &str, subject: &str) -> (u32, String, String) {
    let v: Value = reqwest::get(format!("{registry}/subjects/{subject}/versions/latest"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    (
        v["id"].as_u64().unwrap() as u32,
        v["schemaType"].as_str().unwrap_or("AVRO").to_string(),
        v["schema"].as_str().unwrap().to_string(),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn protobuf_and_json_schema_round_trip_through_a_real_registry() {
    let rp = redpanda().await;
    let config = SchemaRegistryConfig::new(rp.registry.as_str());
    let client = SchemaRegistryClient::new(&config).unwrap();
    let rows = [
        json!({"id": 1, "name": "ada"}),
        json!({"id": 2, "name": "grace"}),
    ];

    let cases: [(&str, KafkaValueFormat, &str, &str); 2] = [
        (
            "orders-proto",
            serde_json::from_value(
                json!({"type": "confluent_protobuf", "schema_registry": config}),
            )
            .unwrap(),
            PROTO,
            "PROTOBUF",
        ),
        (
            "orders-json",
            serde_json::from_value(json!({
                "type": "confluent_json_schema", "schema_registry": config, "validate": true
            }))
            .unwrap(),
            JSON_SCHEMA,
            "JSON",
        ),
    ];
    for (topic, format, schema, kind) in cases {
        let sink = KafkaSink::new(sink_config(&rp.brokers, topic, format, schema))
            .await
            .unwrap_or_else(|e| panic!("{topic}: {e}"));
        assert_eq!(sink.write_batch(&rows).await.unwrap(), 2, "{topic}");
        sink.flush().await.unwrap();

        let (id, schema_type, _) = latest(&rp.registry, &format!("{topic}-value")).await;
        assert_eq!(schema_type, kind, "{topic} registered as {kind}");
        let messages = drain(&rp.brokers, topic, 2).await;
        let mut decoded = Vec::new();
        for m in &messages {
            assert_eq!(m[0], 0, "{topic}: Confluent magic byte");
            assert_eq!(
                u32::from_be_bytes(m[1..5].try_into().unwrap()),
                id,
                "{topic}: schema id"
            );
            decoded.push(
                match kind {
                    "PROTOBUF" => protobuf::decode(&client, m).await,
                    _ => json_schema::decode(&client, m, true).await,
                }
                .unwrap_or_else(|e| panic!("{topic}: {e}")),
            );
        }
        decoded.sort_by_key(|r| r["name"].as_str().map(str::to_string));
        // The proto3 JSON mapping renders int64 as a string.
        let expected: Vec<Value> = match kind {
            "PROTOBUF" => vec![
                json!({"id": "1", "name": "ada"}),
                json!({"id": "2", "name": "grace"}),
            ],
            _ => rows.to_vec(),
        };
        assert_eq!(decoded, expected, "{topic}");
    }
}
