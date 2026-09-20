//! `faucet-conformance` battery for the Kafka sink.
//! Passing this battery in CI is the Tier-1 (supported) criterion.
//!
//! - check 1 `assert_config_schema_valid_value`
//! - check 4 `assert_idempotent_replay` — the atomic-watermark path
//!   (`write_batch_idempotent` + `last_committed_token`): a transactional
//!   producer commits the page's records plus a commit-token record (to a
//!   compacted side-topic) in one Kafka transaction.
//! - check 5 `assert_capabilities_truthful` — Append plus the advertised
//!   idempotency mechanism actually hold.
//!
//! Checks 4 and 5 boot a real single-broker Kafka container (reusing
//! `exactly_once.rs`'s transaction-enabled setup), so they require Docker.
use faucet_conformance::assert_config_schema_valid_value;

#[test]
fn conformance_config_schema_valid() {
    let schema =
        serde_json::to_value(schemars::schema_for!(faucet_sink_kafka::KafkaSinkConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "kafka");
}

mod idempotent {
    use faucet_common_kafka::{CompressionType, KafkaAuth, KafkaValueFormat, OnKeyError};
    use faucet_core::DEFAULT_BATCH_SIZE;
    use faucet_core::Sink as _;
    use faucet_sink_kafka::{Acks, KafkaSink, KafkaSinkConfig, KafkaSinkTopic};
    use rdkafka::ClientConfig;
    use rdkafka::Message;
    use rdkafka::consumer::{Consumer, StreamConsumer};
    use std::collections::{BTreeMap, BTreeSet};
    use std::time::Duration;
    use testcontainers::ImageExt;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::kafka::apache::{KAFKA_PORT, Kafka};

    /// Boot a transaction-enabled single-broker Kafka — mirrors
    /// `exactly_once.rs::start_kafka`.
    async fn start_kafka() -> (testcontainers::ContainerAsync<Kafka>, String) {
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
            // The flat `commit_token_*` keys, not the `exactly_once:` block —
            // this fixture predates it and pins the legacy spelling still
            // works (#654 M20).
            exactly_once: None,
            commit_token_topic: "__faucet_commit_token".into(),
            commit_token_topic_partitions: 1,
            commit_token_topic_replication: 1,
            extra_client_config: BTreeMap::new(),
        }
    }

    /// A fresh Kafka container + a transactional (idempotent) sink writing JSON
    /// `{id, v}` records to `conformance_dest`. The distinct-row count is the
    /// number of DISTINCT `id`s present in that topic (drain read_committed and
    /// dedup by `id`), so re-delivered pages that the watermark suppresses do
    /// not inflate the count. The commit-token record lands in the separate
    /// compacted `__faucet_commit_token` side-topic and is never counted.
    async fn fresh_sink() -> (testcontainers::ContainerAsync<Kafka>, String, KafkaSink) {
        let (container, brokers) = start_kafka().await;
        let sink = KafkaSink::new(eo_config(&brokers, "conformance_dest"))
            .await
            .expect("kafka sink new");
        (container, brokers, sink)
    }

    /// Count DISTINCT `id`s on the destination topic by draining from the
    /// beginning (read_committed) until idle, then deduping by `id` — mirrors
    /// how `exactly_once.rs` verifies no duplicates.
    async fn distinct_ids(brokers: &str, topic: &str) -> usize {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", "conformance-verifier")
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .set("isolation.level", "read_committed")
            .create()
            .expect("verifier consumer");
        consumer.subscribe(&[topic]).expect("subscribe");
        let mut ids: BTreeSet<i64> = BTreeSet::new();
        loop {
            match tokio::time::timeout(Duration::from_secs(5), consumer.recv()).await {
                Ok(Ok(msg)) => {
                    let payload = msg.payload().expect("message payload");
                    let v: serde_json::Value =
                        serde_json::from_slice(payload).expect("json payload");
                    ids.insert(v["id"].as_i64().expect("id field"));
                }
                // The battery counts once *before* any write, when the
                // destination topic does not exist yet — subscribe+recv then
                // yields UnknownTopicOrPartition. Treat any recv error as
                // end-of-stream (0 so far) rather than panicking; the battery's
                // own count assertions catch a genuinely broken write path.
                Ok(Err(_)) => break,
                Err(_) => break, // idle timeout — done
            }
        }
        ids.len()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn conformance_idempotent_replay() {
        let (_container, brokers, sink) = fresh_sink().await;
        faucet_conformance::assert_idempotent_replay(&sink, || {
            let brokers = brokers.clone();
            async move { distinct_ids(&brokers, "conformance_dest").await }
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn conformance_capabilities_truthful() {
        let (_container, brokers, sink) = fresh_sink().await;
        // Check 10: connector_name is non-empty (metric-cardinality contract).
        faucet_conformance::assert_connector_name_nonempty_value(
            sink.connector_name(),
            sink.connector_name(),
        );
        assert_eq!(sink.connector_name(), "kafka");
        // Check 11: preflight check() is well-formed against the live broker
        // (`fetch_metadata` → a Pass probe inside Ok(report); nothing produced).
        faucet_conformance::assert_sink_preflight_check_wellformed(
            &sink,
            &faucet_core::check::CheckContext::default(),
        )
        .await;
        faucet_conformance::assert_capabilities_truthful(&sink, || {
            let brokers = brokers.clone();
            async move { distinct_ids(&brokers, "conformance_dest").await }
        })
        .await;
    }
}
