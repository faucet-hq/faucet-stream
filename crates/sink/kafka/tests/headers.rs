//! #657 — `headers_path` must actually reach the broker.
//!
//! The field was accepted, extracted by a tested helper, and documented — and
//! then dropped on the floor: nothing in the produce path attached it, so a
//! config that asked for headers produced messages with none, with no error and
//! no warning. A consumer routing on a header simply saw nothing.
//!
//! These tests consume the produced messages back and read their headers, so
//! the assertion is about what reached the broker rather than about what the
//! extractor returned.

use std::collections::BTreeMap;
use std::time::Duration as StdDuration;

use faucet_common_kafka::{CompressionType, KafkaAuth, KafkaValueFormat, OnKeyError};
use faucet_core::DEFAULT_BATCH_SIZE;
use faucet_core::Sink;
use faucet_sink_kafka::{Acks, KafkaSink, KafkaSinkConfig, KafkaSinkTopic};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Headers as _;
use rdkafka::{ClientConfig, Message};
use serde_json::json;
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::kafka::apache::{KAFKA_PORT, Kafka};

async fn start_kafka() -> (ContainerAsync<Kafka>, String) {
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

fn sink_config(brokers: &str, topic: &str) -> KafkaSinkConfig {
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
        linger: StdDuration::from_millis(5),
        batch_size: DEFAULT_BATCH_SIZE,
        message_timeout: StdDuration::from_secs(15),
        max_in_flight: 50,
        queue_full_backoff: StdDuration::from_millis(100),
        queue_full_max_retries: 3,
        transactional_id_prefix: None,
        commit_token_topic: "__faucet_commit_token".into(),
        commit_token_topic_partitions: 1,
        commit_token_topic_replication: 1,
        extra_client_config: BTreeMap::new(),
    }
}

/// Drain exactly `want` messages and return `(payload, headers)` for each.
///
/// Two details are load-bearing, both learned the hard way when this suite
/// first returned zero messages against a broker that was working fine:
///
/// * every test is `flavor = "multi_thread"` — `rdkafka`'s `StreamConsumer`
///   polls on a background task, so on a current-thread runtime `recv()` never
///   completes;
/// * the per-`recv()` timeout is generous (30s) and the loop blocks until
///   `want` messages have arrived, because the first `recv()` also pays for the
///   consumer group's JoinGroup/SyncGroup rebalance. A short timeout gives up
///   mid-rebalance and reports an empty topic, which looks identical to "the
///   sink produced nothing".
async fn consume(
    brokers: &str,
    topic: &str,
    want: usize,
) -> Vec<(serde_json::Value, Vec<(String, Option<String>)>)> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", format!("hdr-{topic}"))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("consumer init");
    consumer.subscribe(&[topic]).expect("subscribe");

    let mut out = Vec::new();
    while out.len() < want {
        let msg = tokio::time::timeout(StdDuration::from_secs(30), consumer.recv())
            .await
            .expect("recv timed out waiting for a produced message")
            .expect("recv");
        let payload: serde_json::Value = msg
            .payload()
            .map(|b| serde_json::from_slice(b).expect("json payload"))
            .unwrap_or(serde_json::Value::Null);
        let headers = msg
            .headers()
            .map(|hs| {
                (0..hs.count())
                    .map(|i| {
                        let h = hs.get(i);
                        (
                            h.key.to_string(),
                            h.value.map(|v| String::from_utf8_lossy(v).to_string()),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();
        out.push((payload, headers));
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn configured_headers_reach_the_broker() {
    let (_c, brokers) = start_kafka().await;
    let topic = "hdr_basic";

    let mut cfg = sink_config(&brokers, topic);
    cfg.headers_path = Some("$.meta".into());
    let sink = KafkaSink::new(cfg).await.expect("sink");

    sink.write_batch(&[json!({
        "id": 1,
        "meta": { "tenant": "acme", "trace_id": "abc123", "source": "salesforce" }
    })])
    .await
    .expect("write");
    sink.flush().await.expect("flush");

    let got = consume(&brokers, topic, 1).await;
    assert_eq!(got.len(), 1, "the message must be produced");
    let (_payload, headers) = &got[0];

    let mut names: Vec<&str> = headers.iter().map(|(k, _)| k.as_str()).collect();
    names.sort_unstable();
    assert_eq!(
        names,
        vec!["source", "tenant", "trace_id"],
        "every entry under the configured path must become a header"
    );
    let find = |k: &str| {
        headers
            .iter()
            .find(|(n, _)| n == k)
            .map(|(_, v)| v.clone())
            .expect("header present")
    };
    assert_eq!(find("tenant"), Some("acme".into()));
    assert_eq!(find("trace_id"), Some("abc123".into()));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_null_header_value_becomes_a_valueless_header_not_the_string_null() {
    // Kafka's header model permits a valueless header, which is a truer
    // rendering of JSON `null` than the four-character string "null" — a
    // consumer would otherwise have to know to special-case it.
    let (_c, brokers) = start_kafka().await;
    let topic = "hdr_null";

    let mut cfg = sink_config(&brokers, topic);
    cfg.headers_path = Some("$.meta".into());
    let sink = KafkaSink::new(cfg).await.expect("sink");

    sink.write_batch(&[json!({ "id": 1, "meta": { "present": "yes", "absent": null } })])
        .await
        .expect("write");
    sink.flush().await.expect("flush");

    let got = consume(&brokers, topic, 1).await;
    let (_p, headers) = &got[0];
    let find = |k: &str| {
        headers
            .iter()
            .find(|(n, _)| n == k)
            .map(|(_, v)| v.clone())
            .expect("header present")
    };
    assert_eq!(find("present"), Some("yes".into()));
    assert_eq!(
        find("absent"),
        None,
        "a null must produce a header with no value, not the literal \"null\""
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn no_headers_path_produces_no_headers() {
    // The default must stay clean: an unset path must not invent headers.
    let (_c, brokers) = start_kafka().await;
    let topic = "hdr_none";

    let sink = KafkaSink::new(sink_config(&brokers, topic))
        .await
        .expect("sink");
    sink.write_batch(&[json!({ "id": 1, "meta": { "tenant": "acme" } })])
        .await
        .expect("write");
    sink.flush().await.expect("flush");

    let got = consume(&brokers, topic, 1).await;
    assert!(
        got[0].1.is_empty(),
        "no headers_path must mean no headers, got {:?}",
        got[0].1
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_path_resolving_to_nothing_is_not_an_error() {
    // A record that simply lacks the header object still has to be delivered —
    // failing the batch would make headers a de-facto required field.
    let (_c, brokers) = start_kafka().await;
    let topic = "hdr_missing";

    let mut cfg = sink_config(&brokers, topic);
    cfg.headers_path = Some("$.meta".into());
    let sink = KafkaSink::new(cfg).await.expect("sink");

    let n = sink
        .write_batch(&[json!({ "id": 1 })])
        .await
        .expect("a record with no header object must still be produced");
    assert_eq!(n, 1);
    sink.flush().await.expect("flush");

    let got = consume(&brokers, topic, 1).await;
    assert!(got[0].1.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_path_resolving_to_a_non_object_fails_the_batch() {
    // Strict, matching `partition_path`: a header map the operator asked for
    // and did not get is a config error, not something to silently skip.
    let (_c, brokers) = start_kafka().await;
    let topic = "hdr_bad";

    let mut cfg = sink_config(&brokers, topic);
    cfg.headers_path = Some("$.meta".into());
    let sink = KafkaSink::new(cfg).await.expect("sink");

    let err = sink
        .write_batch(&[json!({ "id": 1, "meta": "not-an-object" })])
        .await
        .expect_err("a non-object header path must fail the batch");
    assert!(
        err.to_string().contains("headers_path"),
        "the error must name the offending config key: {err}"
    );
}
