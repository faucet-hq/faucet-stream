//! Additional `KafkaSource` integration tests targeting branches that the
//! existing `integration.rs` / `streaming.rs` suites do not exercise: keyed
//! JSON decoding, message headers, `OnDecodeError::Skip`, `extra_client_config`
//! propagation, the `check()` metadata probe, and the trait accessor methods
//! (`connector_name`, `dataset_uri`, `config_schema`).
//!
//! Each test boots its own container so they are isolated and parallel-safe.
//! Import notes match the sibling suites (testcontainers-modules 0.15).

use faucet_common_kafka::{KafkaAuth, KafkaValueFormat, OnDecodeError};
use faucet_core::check::{CheckContext, ProbeStatus};
use faucet_core::{DEFAULT_BATCH_SIZE, Source};
use faucet_source_kafka::{KafkaSource, KafkaSourceConfig, OffsetReset};
use rdkafka::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use std::collections::BTreeMap;
use std::time::Duration;
use testcontainers_modules::kafka::apache::{KAFKA_PORT, Kafka};

async fn start_kafka() -> (testcontainers::ContainerAsync<Kafka>, String) {
    let container = faucet_conformance::containers::start(Kafka::default).await;
    let port = container
        .get_host_port_ipv4(KAFKA_PORT)
        .await
        .expect("kafka port");
    (container, format!("127.0.0.1:{port}"))
}

async fn produce(brokers: &str, topic: &str, messages: &[(Option<&str>, &str)]) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("producer init");

    for (key, value) in messages {
        let mut record: FutureRecord<'_, str, str> = FutureRecord::to(topic).payload(*value);
        if let Some(k) = key {
            record = record.key(*k);
        }
        producer
            .send(record, Duration::from_secs(5))
            .await
            .expect("producer send");
    }
    producer
        .flush(Duration::from_secs(5))
        .expect("producer flush");
}

/// Produce a single message carrying both a UTF-8 header and a binary
/// (non-UTF-8) header so both `message_to_value` header branches are hit.
async fn produce_with_headers(brokers: &str, topic: &str, value: &str) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("producer init");

    let headers = OwnedHeaders::new()
        .insert(Header {
            key: "trace-id",
            value: Some("abc-123".as_bytes()),
        })
        .insert(Header {
            key: "blob",
            value: Some(&[0xFFu8, 0x00, 0xFE][..]),
        });
    let record: FutureRecord<'_, str, str> =
        FutureRecord::to(topic).payload(value).headers(headers);
    producer
        .send(record, Duration::from_secs(5))
        .await
        .expect("producer send");
    producer
        .flush(Duration::from_secs(5))
        .expect("producer flush");
}

fn source_config(
    brokers: &str,
    topic: &str,
    group: &str,
    max_messages: usize,
) -> KafkaSourceConfig {
    KafkaSourceConfig {
        brokers: brokers.into(),
        topics: vec![topic.into()],
        group_id: group.into(),
        auth: KafkaAuth::None,
        value_format: KafkaValueFormat::Json,
        key_format: None,
        auto_offset_reset: OffsetReset::Earliest,
        max_messages: Some(max_messages),
        idle_timeout: Some(Duration::from_secs(30)),
        poll_timeout: Duration::from_secs(1),
        session_timeout: Duration::from_secs(30),
        on_decode_error: OnDecodeError::Fail,
        extra_client_config: BTreeMap::new(),
        batch_size: DEFAULT_BATCH_SIZE,
    }
}

/// `key_format = Some(Json)` routes the message key through the JSON decoder
/// (the `Some(fmt)` arm of `message_to_value`) rather than treating it as a
/// raw UTF-8 string.
#[tokio::test(flavor = "multi_thread")]
async fn key_format_json_decodes_key_as_json() {
    let (_c, brokers) = start_kafka().await;
    let topic = "cov-key-json";
    produce(&brokers, topic, &[(Some(r#"{"k":1}"#), r#"{"id":1}"#)]).await;

    let mut cfg = source_config(&brokers, topic, "g-cov-key-json", 1);
    cfg.key_format = Some(KafkaValueFormat::Json);
    let source = KafkaSource::new(cfg).await.unwrap();
    let records = source.fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
    // Decoded as a JSON object, not the raw string `{"k":1}`.
    assert_eq!(records[0]["key"]["k"], 1);
    assert_eq!(records[0]["value"]["id"], 1);
}

/// A message with no key under `key_format = None` yields a JSON `null` key
/// (the `None => Value::Null` arm of `message_to_value`).
#[tokio::test(flavor = "multi_thread")]
async fn missing_key_is_json_null() {
    let (_c, brokers) = start_kafka().await;
    let topic = "cov-no-key";
    produce(&brokers, topic, &[(None, r#"{"id":9}"#)]).await;

    let source = KafkaSource::new(source_config(&brokers, topic, "g-cov-no-key", 1))
        .await
        .unwrap();
    let records = source.fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["key"], serde_json::Value::Null);
}

/// Message headers are surfaced under `headers`: a UTF-8 header passes through
/// as a string, a non-UTF-8 header is base64-encoded.
#[tokio::test(flavor = "multi_thread")]
async fn message_headers_are_surfaced_utf8_and_base64() {
    let (_c, brokers) = start_kafka().await;
    let topic = "cov-headers";
    produce_with_headers(&brokers, topic, r#"{"id":1}"#).await;

    let source = KafkaSource::new(source_config(&brokers, topic, "g-cov-headers", 1))
        .await
        .unwrap();
    let records = source.fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
    let headers = &records[0]["headers"];
    assert_eq!(headers["trace-id"], "abc-123");
    // [0xFF, 0x00, 0xFE] base64-encodes to "/wD+".
    assert_eq!(headers["blob"], "/wD+");
}

/// `OnDecodeError::Skip` drops a record whose payload is not valid JSON and
/// keeps the surrounding valid records.
#[tokio::test(flavor = "multi_thread")]
async fn on_decode_error_skip_drops_invalid_records() {
    let (_c, brokers) = start_kafka().await;
    let topic = "cov-skip";
    produce(
        &brokers,
        topic,
        &[
            (None, r#"{"id":1}"#),
            (None, "{not json"),
            (None, r#"{"id":3}"#),
        ],
    )
    .await;

    let mut cfg = source_config(&brokers, topic, "g-cov-skip", 100);
    cfg.on_decode_error = OnDecodeError::Skip;
    // Short idle so the loop terminates after the 2 valid records are consumed
    // (max_messages can't be hit because one record is dropped).
    cfg.idle_timeout = Some(Duration::from_secs(10));
    let source = KafkaSource::new(cfg).await.unwrap();
    let records = source.fetch_all().await.unwrap();
    assert_eq!(records.len(), 2, "the invalid record must be skipped");
    assert_eq!(records[0]["value"]["id"], 1);
    assert_eq!(records[1]["value"]["id"], 3);
}

/// `OnDecodeError::Fail` (the default) surfaces a decode error as a `Source`
/// error rather than skipping.
#[tokio::test(flavor = "multi_thread")]
async fn on_decode_error_fail_surfaces_error() {
    let (_c, brokers) = start_kafka().await;
    let topic = "cov-fail";
    produce(&brokers, topic, &[(None, "{still not json")]).await;

    let cfg = source_config(&brokers, topic, "g-cov-fail", 1);
    let source = KafkaSource::new(cfg).await.unwrap();
    let err = source.fetch_all().await.unwrap_err();
    let msg = format!("{err}").to_lowercase();
    assert!(
        msg.contains("json"),
        "expected json decode error, got {msg}"
    );
}

/// `extra_client_config` entries are applied to the underlying consumer's
/// `ClientConfig` (the `for (k, v) in &config.extra_client_config` loop). A
/// benign override keeps the consumer functional and a round-trip still works.
#[tokio::test(flavor = "multi_thread")]
async fn extra_client_config_is_applied() {
    let (_c, brokers) = start_kafka().await;
    let topic = "cov-extra-config";
    produce(&brokers, topic, &[(None, r#"{"id":1}"#)]).await;

    let mut cfg = source_config(&brokers, topic, "g-cov-extra", 1);
    cfg.extra_client_config
        .insert("fetch.min.bytes".into(), "1".into());
    let source = KafkaSource::new(cfg).await.unwrap();
    let records = source.fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["value"]["id"], 1);
}

/// `check()` returns a single passing `metadata` probe against a reachable
/// broker (the `Probe::pass` arm of the metadata fetch).
#[tokio::test(flavor = "multi_thread")]
async fn check_passes_against_reachable_broker() {
    let (_c, brokers) = start_kafka().await;
    let source = KafkaSource::new(source_config(&brokers, "cov-check", "g-cov-check", 1))
        .await
        .unwrap();
    let report = source.check(&CheckContext::default()).await.unwrap();
    assert_eq!(report.probes.len(), 1);
    assert_eq!(report.probes[0].name, "metadata");
    assert!(
        matches!(report.probes[0].status, ProbeStatus::Pass),
        "expected a passing metadata probe, got {:?}",
        report.probes[0].status
    );
}

/// `check()` fails (rather than hanging) when the broker is unreachable: the
/// metadata fetch times out / errors and returns a single failing probe.
#[tokio::test(flavor = "multi_thread")]
async fn check_fails_against_unreachable_broker() {
    // 127.0.0.1:1 is a closed port — connection refused / metadata timeout.
    let mut cfg = source_config("127.0.0.1:1", "cov-check-bad", "g-cov-check-bad", 1);
    cfg.session_timeout = Duration::from_secs(6);
    let source = KafkaSource::new(cfg).await.unwrap();
    let ctx = CheckContext {
        timeout: Duration::from_secs(3),
    };
    let report = source.check(&ctx).await.unwrap();
    assert_eq!(report.probes.len(), 1);
    assert_eq!(report.probes[0].name, "metadata");
    assert!(
        matches!(report.probes[0].status, ProbeStatus::Fail { .. }),
        "expected a failing metadata probe, got {:?}",
        report.probes[0].status
    );
}

/// The trait accessor methods (`connector_name`, `dataset_uri`,
/// `config_schema`) return the expected values on a constructed source.
#[tokio::test(flavor = "multi_thread")]
async fn accessor_methods_on_constructed_source() {
    let (_c, brokers) = start_kafka().await;
    let source = KafkaSource::new(source_config(&brokers, "cov-accessors", "g-cov-acc", 1))
        .await
        .unwrap();

    assert_eq!(source.connector_name(), "kafka");

    let uri = source.dataset_uri();
    assert!(
        uri.starts_with("kafka://") && uri.ends_with("?topic=cov-accessors"),
        "unexpected dataset_uri: {uri}"
    );

    let schema = source.config_schema();
    // The schema is a JSON object describing KafkaSourceConfig's properties.
    assert!(schema.is_object());
    assert!(
        schema["properties"]["topics"].is_object(),
        "config_schema must describe the topics field"
    );
}
