//! Additional `KafkaSink` integration tests targeting branches the existing
//! `integration.rs` suite does not exercise: `key_path` + `key_format`
//! JSON-key encoding, the `OnKeyError::Fail` / `OnKeyError::Skip` paths through
//! `handle_key_extract`, partition routing, the `check()` metadata probe (pass
//! and fail), and `dataset_uri` for both topic modes.
//!
//! Each test boots its own container so they are isolated and parallel-safe.

use faucet_common_kafka::{CompressionType, KafkaAuth, KafkaValueFormat, OnKeyError};
use faucet_core::check::{CheckContext, ProbeStatus};
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

/// Drain `expect` messages and return (key_utf8, payload_utf8, partition).
async fn drain_keyed(
    brokers: &str,
    topic: &str,
    expect: usize,
) -> Vec<(Option<String>, String, i32)> {
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
        let msg = tokio::time::timeout(Duration::from_secs(30), consumer.recv())
            .await
            .expect("recv timeout")
            .expect("recv");
        let key = msg.key().map(|b| String::from_utf8_lossy(b).to_string());
        let payload = msg
            .payload()
            .map(|b| String::from_utf8_lossy(b).to_string())
            .unwrap_or_default();
        out.push((key, payload, msg.partition()));
    }
    out
}

/// `key_path` + `key_format = Json` extracts the sub-value at the path and
/// encodes it via the JSON encoder (the `(Some(path), Some(fmt))` arm of
/// `build_record_bytes`).
#[tokio::test(flavor = "multi_thread")]
async fn key_path_with_json_key_format_encodes_subvalue() {
    let (_c, brokers) = start_kafka().await;
    let topic = "cov-key-json";
    let mut cfg = sink_config(&brokers, KafkaSinkTopic::Fixed { name: topic.into() });
    cfg.key_path = Some("$.k".into());
    cfg.key_format = Some(KafkaValueFormat::Json);
    let sink = KafkaSink::new(cfg).await.unwrap();
    let n = sink
        .write_batch(&[json!({"k": {"id": 7}, "v": 1})])
        .await
        .unwrap();
    assert_eq!(n, 1);
    sink.flush().await.unwrap();
    let msgs = drain_keyed(&brokers, topic, 1).await;
    // The key bytes are the JSON encoding of the sub-object `{"id":7}`.
    assert_eq!(msgs[0].0.as_deref(), Some(r#"{"id":7}"#));
}

/// `key_path` + `key_format = None` uses the extracted string directly as the
/// raw key bytes (the `(Some(path), None)` arm).
#[tokio::test(flavor = "multi_thread")]
async fn key_path_without_key_format_uses_string_directly() {
    let (_c, brokers) = start_kafka().await;
    let topic = "cov-key-string";
    let mut cfg = sink_config(&brokers, KafkaSinkTopic::Fixed { name: topic.into() });
    cfg.key_path = Some("$.user_id".into());
    let sink = KafkaSink::new(cfg).await.unwrap();
    let n = sink
        .write_batch(&[json!({"user_id": "alice", "v": 1})])
        .await
        .unwrap();
    assert_eq!(n, 1);
    sink.flush().await.unwrap();
    let msgs = drain_keyed(&brokers, topic, 1).await;
    assert_eq!(msgs[0].0.as_deref(), Some("alice"));
}

/// `key_path` + `key_format = None` with a missing key and `OnKeyError::Fail`
/// returns a `Sink` error (the `(Some(path), None)` → `Fail` arm).
#[tokio::test(flavor = "multi_thread")]
async fn missing_string_key_with_fail_errors() {
    let (_c, brokers) = start_kafka().await;
    let topic = "cov-key-fail";
    let mut cfg = sink_config(&brokers, KafkaSinkTopic::Fixed { name: topic.into() });
    cfg.key_path = Some("$.user_id".into());
    cfg.on_key_error = OnKeyError::Fail;
    let sink = KafkaSink::new(cfg).await.unwrap();
    let err = sink.write_batch(&[json!({"v": 1})]).await.unwrap_err();
    assert!(
        format!("{err}").contains("key_path"),
        "expected key_path failure, got {err}"
    );
}

/// `key_path` + `key_format = Json` with a missing key and `OnKeyError::Fail`
/// goes through `handle_key_extract` and returns a `Sink` error.
#[tokio::test(flavor = "multi_thread")]
async fn missing_json_key_with_fail_errors() {
    let (_c, brokers) = start_kafka().await;
    let topic = "cov-key-json-fail";
    let mut cfg = sink_config(&brokers, KafkaSinkTopic::Fixed { name: topic.into() });
    cfg.key_path = Some("$.k".into());
    cfg.key_format = Some(KafkaValueFormat::Json);
    cfg.on_key_error = OnKeyError::Fail;
    let sink = KafkaSink::new(cfg).await.unwrap();
    let err = sink.write_batch(&[json!({"v": 1})]).await.unwrap_err();
    assert!(
        format!("{err}").contains("key_path") && format!("{err}").contains("fail"),
        "expected handle_key_extract failure, got {err}"
    );
}

/// `key_path` + `key_format = Json` with a missing key and `OnKeyError::Skip`
/// goes through `handle_key_extract` returning `None` (the `Skip => Ok(None)`
/// branch); `key_bytes` is then `None`, so the record is dropped by the
/// skip-drop guard. A record with a present key is kept, so the keyed record
/// alone is produced.
#[tokio::test(flavor = "multi_thread")]
async fn missing_json_key_with_skip_drops_record() {
    let (_c, brokers) = start_kafka().await;
    let topic = "cov-key-json-skip";
    let mut cfg = sink_config(&brokers, KafkaSinkTopic::Fixed { name: topic.into() });
    cfg.key_path = Some("$.k".into());
    cfg.key_format = Some(KafkaValueFormat::Json);
    cfg.on_key_error = OnKeyError::Skip;
    let sink = KafkaSink::new(cfg).await.unwrap();
    let n = sink
        .write_batch(&[json!({"v": 1}), json!({"k": 5, "v": 2})])
        .await
        .unwrap();
    assert_eq!(n, 1, "the keyless record is dropped, the keyed one is kept");
    sink.flush().await.unwrap();
    let msgs = drain_keyed(&brokers, topic, 1).await;
    assert_eq!(
        msgs[0].0.as_deref(),
        Some("5"),
        "only the keyed record survives"
    );
    assert_eq!(msgs[0].1, r#"{"k":5,"v":2}"#);
}

/// `partition_path` routes a record to an explicit partition (the
/// `Some(p) => extract::partition_at(...)` arm). Topic is created with 3
/// partitions so partition 2 is valid.
#[tokio::test(flavor = "multi_thread")]
async fn partition_path_routes_to_explicit_partition() {
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;

    let (_c, brokers) = start_kafka().await;
    let topic = "cov-partition";
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .expect("admin");
    admin
        .create_topics(
            &[NewTopic::new(topic, 3, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .expect("create topic");

    let mut cfg = sink_config(&brokers, KafkaSinkTopic::Fixed { name: topic.into() });
    cfg.partition_path = Some("$.p".into());
    let sink = KafkaSink::new(cfg).await.unwrap();
    let n = sink.write_batch(&[json!({"p": 2, "v": 1})]).await.unwrap();
    assert_eq!(n, 1);
    sink.flush().await.unwrap();
    let msgs = drain_keyed(&brokers, topic, 1).await;
    assert_eq!(msgs[0].2, 2, "record must land on partition 2");
}

/// `check()` returns a single passing `metadata` probe against a reachable
/// broker.
#[tokio::test(flavor = "multi_thread")]
async fn check_passes_against_reachable_broker() {
    let (_c, brokers) = start_kafka().await;
    let sink = KafkaSink::new(sink_config(
        &brokers,
        KafkaSinkTopic::Fixed {
            name: "cov-check".into(),
        },
    ))
    .await
    .unwrap();
    let report = sink.check(&CheckContext::default()).await.unwrap();
    assert_eq!(report.probes.len(), 1);
    assert_eq!(report.probes[0].name, "metadata");
    assert!(
        matches!(report.probes[0].status, ProbeStatus::Pass),
        "expected a passing metadata probe, got {:?}",
        report.probes[0].status
    );
}

/// `check()` fails (rather than hanging) when the broker is unreachable.
#[tokio::test(flavor = "multi_thread")]
async fn check_fails_against_unreachable_broker() {
    let sink = KafkaSink::new(sink_config(
        "127.0.0.1:1",
        KafkaSinkTopic::Fixed {
            name: "cov-check-bad".into(),
        },
    ))
    .await
    .unwrap();
    let ctx = CheckContext {
        timeout: Duration::from_secs(3),
    };
    let report = sink.check(&ctx).await.unwrap();
    assert_eq!(report.probes.len(), 1);
    assert_eq!(report.probes[0].name, "metadata");
    assert!(
        matches!(report.probes[0].status, ProbeStatus::Fail { .. }),
        "expected a failing metadata probe, got {:?}",
        report.probes[0].status
    );
}

/// `dataset_uri` renders the fixed-topic and from-path topic modes.
#[tokio::test(flavor = "multi_thread")]
async fn dataset_uri_for_both_topic_modes() {
    let (_c, brokers) = start_kafka().await;

    let fixed = KafkaSink::new(sink_config(
        &brokers,
        KafkaSinkTopic::Fixed {
            name: "orders".into(),
        },
    ))
    .await
    .unwrap();
    let uri = fixed.dataset_uri();
    assert!(
        uri.starts_with("kafka://") && uri.ends_with("?topic=orders"),
        "unexpected fixed dataset_uri: {uri}"
    );

    let from_path = KafkaSink::new(sink_config(
        &brokers,
        KafkaSinkTopic::FromPath {
            path: "$.dest".into(),
        },
    ))
    .await
    .unwrap();
    let uri = from_path.dataset_uri();
    assert!(
        uri.ends_with("?topic=(from_path:$.dest)"),
        "unexpected from_path dataset_uri: {uri}"
    );
}
