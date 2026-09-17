//! Configuration for the Kafka sink.

use faucet_common_kafka::{CompressionType, KafkaAuth, KafkaValueFormat, OnKeyError};
use faucet_core::{DEFAULT_BATCH_SIZE, FaucetError};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;

/// Configuration for the Kafka sink connector.
///
/// Only [`brokers`](Self::brokers) and [`topic`](Self::topic) are required;
/// everything else has a safe default (`acks: all` + `idempotent: true`).
/// Validated at config load by [`validate`](Self::validate).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct KafkaSinkConfig {
    /// Comma-separated `host:port` bootstrap brokers, passed straight through
    /// as librdkafka's `bootstrap.servers` (e.g. `"b1:9092,b2:9092"`).
    /// Required; a blank value is rejected at config load.
    pub brokers: String,
    /// Destination topic. Either a fixed name
    /// (`{ type: fixed, name: events }`) or a per-record JSONPath lookup
    /// (`{ type: from_path, path: "$.dest" }`) for multi-topic routing.
    /// Required.
    pub topic: KafkaSinkTopic,
    /// Broker authentication — SASL/PLAIN, SASL/SCRAM, SSL client certs, or
    /// SASL-over-SSL. Defaults to `{ type: none }` (plaintext brokers only).
    #[serde(default)]
    pub auth: KafkaAuth,
    /// Encoding applied to each record to produce the message **value**.
    /// Defaults to `{ type: json }`. The Confluent Schema Registry formats
    /// (`confluent_avro` / `confluent_protobuf` / `confluent_json_schema`)
    /// additionally require [`Self::value_schema`] and the crate's
    /// `schema-registry` feature.
    #[serde(default)]
    pub value_format: KafkaValueFormat,
    /// Encoding applied to the value extracted at [`Self::key_path`] to
    /// produce the message **key**. When unset, the extracted value is used
    /// verbatim as UTF-8 key bytes (numbers and booleans are stringified) —
    /// set this only when the key needs a real encoder, e.g. a Schema
    /// Registry format. Ignored when `key_path` is unset (no key is sent).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_format: Option<KafkaValueFormat>,
    /// Schema text (Avro `.avsc` JSON, Protobuf `.proto`, or JSON Schema) for
    /// the **value** when `value_format` is a Confluent Schema Registry format
    /// (`confluent_avro` / `confluent_protobuf` / `confluent_json_schema`).
    /// Registered under the `{topic}-value` subject on first use. Required for
    /// those formats; ignored otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_schema: Option<String>,
    /// Schema text for the **key**, used when `key_format` is a Confluent
    /// Schema Registry format. Registered under `{topic}-key`. Required for
    /// those key formats; ignored otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_schema: Option<String>,
    /// JSONPath to the field that becomes each message's key (the first match
    /// wins). Unset — the default — produces keyless messages, so the broker
    /// partitions round-robin and log compaction cannot key on them. A path
    /// that resolves to nothing is handled per [`Self::on_key_error`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_path: Option<String>,
    /// JSONPath to an explicit destination partition for each message. The
    /// match must be an integer in `0..=i32::MAX`; anything else fails the
    /// record with [`FaucetError::Sink`]. Unset — the default — lets
    /// librdkafka pick the partition from the key (or round-robin when there
    /// is no key), which is almost always what you want.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_path: Option<String>,
    /// JSONPath to a flat JSON object whose entries become Kafka message
    /// headers (values are stringified). Unset by default.
    ///
    /// **Not yet applied:** the extraction helper exists and is tested, but
    /// the produce path does not attach the headers to the outgoing message,
    /// so setting this currently has no effect on what reaches the broker.
    /// Carry the values inside the record body until it is wired up.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headers_path: Option<String>,
    /// What to do when [`Self::key_path`] resolves to nothing for a record:
    /// `fail` (default) aborts the batch with [`FaucetError::Sink`], `skip`
    /// drops the record (counted and warned once per batch), `round_robin`
    /// sends it with no key and lets the broker place it. Only consulted when
    /// `key_path` is set.
    #[serde(default)]
    pub on_key_error: OnKeyError,
    /// Producer-side batch compression, set as librdkafka's
    /// `compression.type`: `none` (default), `gzip`, `snappy`, `lz4`, or
    /// `zstd`. `lz4` or `zstd` usually pays for itself on JSON payloads.
    #[serde(default)]
    pub compression: CompressionType,
    /// Broker acknowledgement level (librdkafka `acks`): `all` (default, every
    /// in-sync replica), `leader` (leader only), or `none` (fire-and-forget).
    /// Anything but `all` can lose data on a broker failure, and
    /// [`Self::idempotent`] requires `all` — that combination is rejected at
    /// config load.
    #[serde(default = "default_acks")]
    pub acks: Acks,
    /// Enable the librdkafka idempotent producer (`enable.idempotence`),
    /// which de-duplicates broker-side retries so a retried send cannot
    /// append the message twice. Defaults to `true` and requires
    /// `acks: all`. Independent of `delivery: exactly_once`, which adds a
    /// transactional producer plus a commit-token side-topic on top.
    #[serde(default = "default_idempotent")]
    pub idempotent: bool,
    /// How long the producer waits for more messages before sending a batch
    /// (librdkafka `linger.ms`). Raising it trades latency for larger,
    /// better-compressed batches. Defaults to 5 ms.
    ///
    /// **Config granularity is whole seconds**, so the sub-second default is
    /// not expressible in YAML — omit the key to keep 5 ms (writing
    /// `linger: 0` *disables* lingering), and set `linger.ms` through
    /// [`Self::extra_client_config`] for any other sub-second value.
    #[serde(
        default = "default_linger",
        with = "faucet_core::config::duration_secs"
    )]
    #[schemars(with = "u64")]
    pub linger: Duration,
    /// Maximum number of in-flight `FuturesUnordered` send futures during a
    /// single [`Sink::write_batch`](faucet_core::Sink::write_batch) call.
    /// Defaults to [`DEFAULT_BATCH_SIZE`].
    ///
    /// When `batch_size > 0`, the producer's `queue.buffering.max.messages`
    /// librdkafka property is also set to this value (so the broker-side
    /// buffer matches the sink-side send window) unless the user has
    /// already set it via `extra_client_config`. `QueueFull` errors still
    /// flow through the existing retry path (`queue_full_backoff` /
    /// `queue_full_max_retries`) — the only effect of the in-flight cap is
    /// to apply backpressure earlier inside the sink loop instead of
    /// queueing the request and waiting for librdkafka to push back.
    ///
    /// `batch_size = 0` is the "no batching" sentinel: every record in the
    /// incoming slice is enqueued into the `FuturesUnordered` immediately
    /// (bounded only by `max_in_flight`) and the librdkafka
    /// `queue.buffering.max.messages` knob is left at its default. Use it
    /// for sources that emit a single page per run (small lookup tables,
    /// one-shot drains) where forcing additional backpressure adds latency.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Delivery deadline for a single message (librdkafka
    /// `message.timeout.ms`), in **whole seconds**. Defaults to 30 s. Also
    /// used as the timeout for `flush()`, for `init_transactions`, and as the
    /// floor for the exactly-once `transaction.timeout.ms` (which is raised to
    /// at least 60 s so a long `message_timeout` cannot make
    /// `init_transactions` reject the producer).
    #[serde(
        default = "default_message_timeout",
        with = "faucet_core::config::duration_secs"
    )]
    #[schemars(with = "u64")]
    pub message_timeout: Duration,
    /// Hard ceiling on concurrent in-flight sends inside one
    /// [`Sink::write_batch`](faucet_core::Sink::write_batch) call. Defaults to
    /// 100 and must be at least 1. When [`Self::batch_size`] is non-zero the
    /// effective window is `min(max_in_flight, batch_size)`, so this is the
    /// cap that applies under the `batch_size = 0` sentinel.
    #[serde(default = "default_max_in_flight")]
    pub max_in_flight: usize,
    /// How long to wait before retrying a send that librdkafka rejected with
    /// `QueueFull`. Defaults to 100 ms. See
    /// [`Self::queue_full_max_retries`].
    ///
    /// **Config granularity is whole seconds**, so the sub-second default is
    /// not expressible in YAML — omit the key to keep 100 ms (`0` retries
    /// immediately with no pause).
    #[serde(
        default = "default_queue_full_backoff",
        with = "faucet_core::config::duration_secs"
    )]
    #[schemars(with = "u64")]
    pub queue_full_backoff: Duration,
    /// How many times to retry a `QueueFull` send before failing the batch.
    /// Defaults to 3; `0` fails on the first `QueueFull`. `QueueFull` means
    /// the local producer buffer is saturated — raising
    /// `queue.buffering.max.messages` (via [`Self::extra_client_config`]) or
    /// lowering [`Self::batch_size`] treats the cause rather than the symptom.
    #[serde(default = "default_queue_full_max_retries")]
    pub queue_full_max_retries: u32,
    /// Optional namespace prefix for the producer's auto-derived
    /// `transactional.id` (exactly-once mode only). The id is
    /// `"{prefix}.{sanitized_scope}"`; `prefix` defaults to `"faucet"` when
    /// unset. Set this to isolate transactional ids across clusters or
    /// environments that share a pipeline scope namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transactional_id_prefix: Option<String>,
    /// Compacted side-topic that holds one commit-token record per pipeline
    /// scope (exactly-once mode only). Auto-created with
    /// `cleanup.policy=compact` if absent.
    #[serde(default = "default_commit_token_topic")]
    pub commit_token_topic: String,
    /// Partition count used when auto-creating [`Self::commit_token_topic`].
    #[serde(default = "default_commit_token_topic_partitions")]
    pub commit_token_topic_partitions: i32,
    /// Replication factor used when auto-creating
    /// [`Self::commit_token_topic`]. `-1` means "use the broker default".
    #[serde(default = "default_commit_token_topic_replication")]
    pub commit_token_topic_replication: i32,
    /// Raw librdkafka producer properties, applied **last** so they override
    /// everything this config derives (`acks`, `enable.idempotence`,
    /// `compression.type`, `linger.ms`, `message.timeout.ms`,
    /// `queue.buffering.max.messages`). Empty by default.
    ///
    /// The escape hatch for a knob faucet does not model — and the sharp edge
    /// that goes with it: overriding a safety property here can weaken
    /// delivery guarantees. The only exceptions are the exactly-once
    /// invariants (`transactional.id`, `enable.idempotence`, `acks=all`),
    /// which the transactional producer force-sets on top so an override
    /// cannot break EOS.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra_client_config: BTreeMap<String, String>,
}

/// Where each message's destination topic comes from.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum KafkaSinkTopic {
    /// One fixed topic for every record.
    Fixed {
        /// Topic name. Must not be blank.
        name: String,
    },
    /// Per-record routing: the topic is read from the record itself.
    FromPath {
        /// JSONPath to the topic name (first match wins). A record whose path
        /// does not resolve fails the batch. Must not be blank.
        path: String,
    },
}

impl Default for KafkaSinkTopic {
    fn default() -> Self {
        Self::Fixed {
            name: String::new(),
        }
    }
}

/// Broker acknowledgement level required before a send is considered
/// delivered — librdkafka's `acks`.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Acks {
    /// `acks=0` — fire-and-forget: no acknowledgement is awaited, so a send
    /// can be silently lost. Fastest and least safe.
    None,
    /// `acks=1` — the partition leader has written the message; it can still
    /// be lost if the leader fails before the followers replicate it.
    Leader,
    /// `acks=all` — every in-sync replica has written the message. The
    /// default, and the only level compatible with
    /// [`KafkaSinkConfig::idempotent`].
    #[default]
    All,
}

impl Acks {
    // Used by KafkaSink (Task 20) when building the rdkafka producer config.
    #[allow(dead_code)]
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Acks::None => "0",
            Acks::Leader => "1",
            Acks::All => "all",
        }
    }
}

fn default_acks() -> Acks {
    Acks::All
}
fn default_idempotent() -> bool {
    true
}
fn default_linger() -> Duration {
    Duration::from_millis(5)
}
fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}
fn default_message_timeout() -> Duration {
    Duration::from_secs(30)
}
fn default_max_in_flight() -> usize {
    100
}
fn default_queue_full_backoff() -> Duration {
    Duration::from_millis(100)
}
fn default_queue_full_max_retries() -> u32 {
    3
}
// Double leading underscore mirrors Kafka's own internal-topic convention
// (__consumer_offsets / __transaction_state); intentionally distinct from the
// SQL sinks' `_faucet_commit_token` table constant in faucet_core::idempotency.
fn default_commit_token_topic() -> String {
    "__faucet_commit_token".to_string()
}
fn default_commit_token_topic_partitions() -> i32 {
    1
}
fn default_commit_token_topic_replication() -> i32 {
    -1
}

impl KafkaSinkConfig {
    pub fn validate(&self) -> Result<(), FaucetError> {
        if self.brokers.trim().is_empty() {
            return Err(FaucetError::Config(
                "kafka sink: brokers must not be empty".into(),
            ));
        }
        match &self.topic {
            KafkaSinkTopic::Fixed { name } if name.trim().is_empty() => {
                return Err(FaucetError::Config(
                    "kafka sink: topic.name must not be empty".into(),
                ));
            }
            KafkaSinkTopic::FromPath { path } if path.trim().is_empty() => {
                return Err(FaucetError::Config(
                    "kafka sink: topic.path must not be empty".into(),
                ));
            }
            _ => {}
        }
        if self.idempotent && self.acks != Acks::All {
            return Err(FaucetError::Config(
                "kafka sink: idempotent=true requires acks=all".into(),
            ));
        }
        if self.max_in_flight == 0 {
            return Err(FaucetError::Config(
                "kafka sink: max_in_flight must be at least 1".into(),
            ));
        }
        // Confluent Schema Registry formats need a schema to encode against;
        // without one the encoder fails on the first record (#78/#9). Catch it
        // at config-load time with a clear message. (Both checks are no-ops
        // when the schema-registry feature is off — `is_schema_registry`
        // returns false.)
        if self.value_format.is_schema_registry() && self.value_schema.is_none() {
            return Err(FaucetError::Config(
                "kafka sink: value_format is a Confluent Schema Registry format but \
                 value_schema is not set"
                    .into(),
            ));
        }
        if let Some(kf) = &self.key_format
            && kf.is_schema_registry()
            && self.key_schema.is_none()
        {
            return Err(FaucetError::Config(
                "kafka sink: key_format is a Confluent Schema Registry format but \
                 key_schema is not set"
                    .into(),
            ));
        }
        if self.commit_token_topic.trim().is_empty() {
            return Err(FaucetError::Config(
                "kafka sink: commit_token_topic must not be empty".into(),
            ));
        }
        if self.commit_token_topic_partitions < 1 {
            return Err(FaucetError::Config(
                "kafka sink: commit_token_topic_partitions must be at least 1".into(),
            ));
        }
        if self.commit_token_topic_replication < 1 && self.commit_token_topic_replication != -1 {
            return Err(FaucetError::Config(
                "kafka sink: commit_token_topic_replication must be -1 (broker default) or at least 1"
                    .into(),
            ));
        }
        faucet_core::validate_batch_size(self.batch_size)?;
        Ok(())
    }

    /// Set the in-flight send-window cap for
    /// [`Sink::write_batch`](faucet_core::Sink::write_batch).
    ///
    /// Pass `0` to opt out of the explicit cap — every record in the slice
    /// is enqueued into the `FuturesUnordered` immediately (bounded only by
    /// `max_in_flight`) and the librdkafka `queue.buffering.max.messages`
    /// knob is left at its default.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal() -> KafkaSinkConfig {
        KafkaSinkConfig {
            brokers: "b:9092".into(),
            topic: KafkaSinkTopic::Fixed { name: "out".into() },
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
            message_timeout: Duration::from_secs(30),
            max_in_flight: 100,
            queue_full_backoff: Duration::from_millis(100),
            queue_full_max_retries: 3,
            transactional_id_prefix: None,
            commit_token_topic: "__faucet_commit_token".into(),
            commit_token_topic_partitions: 1,
            commit_token_topic_replication: -1,
            extra_client_config: BTreeMap::new(),
        }
    }

    #[test]
    fn validate_accepts_minimal() {
        assert!(minimal().validate().is_ok());
    }

    #[cfg(feature = "schema-registry")]
    #[test]
    fn validate_requires_schema_for_confluent_value_format() {
        // Regression for #78/#9: a Confluent SR value_format with no
        // value_schema must be rejected at config load, not fail per-record.
        use faucet_common_kafka::SchemaRegistryConfig;
        let mut c = minimal();
        c.value_format = KafkaValueFormat::ConfluentAvro {
            schema_registry: SchemaRegistryConfig::new("http://localhost:8081"),
        };
        c.value_schema = None;
        let err = c.validate().unwrap_err();
        assert!(format!("{err}").contains("value_schema"), "{err}");

        // With a schema it validates.
        c.value_schema = Some(r#"{"type":"record","name":"R","fields":[]}"#.into());
        assert!(c.validate().is_ok());
    }

    #[test]
    fn validate_rejects_idempotent_without_acks_all() {
        let mut c = minimal();
        c.idempotent = true;
        c.acks = Acks::Leader;
        let err = c.validate().unwrap_err();
        assert!(format!("{err}").contains("idempotent"));
    }

    #[test]
    fn validate_rejects_empty_brokers() {
        let mut c = minimal();
        c.brokers = String::new();
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_empty_fixed_topic() {
        let mut c = minimal();
        c.topic = KafkaSinkTopic::Fixed {
            name: String::new(),
        };
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_empty_from_path() {
        let mut c = minimal();
        c.topic = KafkaSinkTopic::FromPath {
            path: String::new(),
        };
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_max_in_flight() {
        let mut c = minimal();
        c.max_in_flight = 0;
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_accepts_non_idempotent_with_acks_leader() {
        let mut c = minimal();
        c.idempotent = false;
        c.acks = Acks::Leader;
        assert!(c.validate().is_ok());
    }

    #[test]
    fn acks_as_str_returns_librdkafka_values() {
        assert_eq!(Acks::None.as_str(), "0");
        assert_eq!(Acks::Leader.as_str(), "1");
        assert_eq!(Acks::All.as_str(), "all");
    }

    #[test]
    fn from_path_topic_round_trips() {
        let t = KafkaSinkTopic::FromPath {
            path: "$.dest".into(),
        };
        let v = serde_json::to_value(&t).unwrap();
        assert_eq!(v["type"], "from_path");
        assert_eq!(v["path"], "$.dest");
    }

    #[test]
    fn schema_compiles() {
        let _ = schemars::schema_for!(KafkaSinkConfig);
    }

    #[test]
    fn batch_size_defaults_to_default_batch_size() {
        let raw = r#"{
            "brokers": "b:9092",
            "topic": { "type": "fixed", "name": "out" }
        }"#;
        let parsed: KafkaSinkConfig = serde_json::from_str(raw).unwrap();
        assert_eq!(parsed.batch_size, DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let c = minimal().with_batch_size(500);
        assert_eq!(c.batch_size, 500);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let c = minimal().with_batch_size(0);
        assert_eq!(c.batch_size, 0);
        assert!(c.validate().is_ok());
    }

    #[test]
    fn validate_rejects_batch_size_above_max() {
        let c = minimal().with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(c.validate().is_err());
    }

    #[test]
    fn commit_token_defaults_are_set() {
        let raw = r#"{
            "brokers": "b:9092",
            "topic": { "type": "fixed", "name": "out" }
        }"#;
        let c: KafkaSinkConfig = serde_json::from_str(raw).unwrap();
        assert_eq!(c.commit_token_topic, "__faucet_commit_token");
        assert_eq!(c.commit_token_topic_partitions, 1);
        assert_eq!(c.commit_token_topic_replication, -1);
        assert!(c.transactional_id_prefix.is_none());
    }

    #[test]
    fn validate_rejects_empty_commit_token_topic() {
        let mut c = minimal();
        c.commit_token_topic = "  ".into();
        let err = c.validate().unwrap_err();
        assert!(format!("{err}").contains("commit_token_topic"), "{err}");
    }

    #[test]
    fn validate_rejects_zero_commit_token_partitions() {
        let mut c = minimal();
        c.commit_token_topic_partitions = 0;
        assert!(c.validate().is_err());
    }

    #[test]
    fn commit_token_explicit_values_round_trip() {
        let raw = r#"{
            "brokers": "b:9092",
            "topic": { "type": "fixed", "name": "out" },
            "transactional_id_prefix": "acme",
            "commit_token_topic": "wm",
            "commit_token_topic_partitions": 3,
            "commit_token_topic_replication": 2
        }"#;
        let c: KafkaSinkConfig = serde_json::from_str(raw).unwrap();
        assert_eq!(c.transactional_id_prefix.as_deref(), Some("acme"));
        assert_eq!(c.commit_token_topic, "wm");
        assert_eq!(c.commit_token_topic_partitions, 3);
        assert_eq!(c.commit_token_topic_replication, 2);
        assert!(c.validate().is_ok());
    }

    #[test]
    fn validate_rejects_invalid_commit_token_replication() {
        let mut c = minimal();
        c.commit_token_topic_replication = 0;
        assert!(c.validate().is_err());
        c.commit_token_topic_replication = -2;
        assert!(c.validate().is_err());
        c.commit_token_topic_replication = -1; // broker default — allowed
        assert!(c.validate().is_ok());
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let raw = r#"{
            "brokers": "b:9092",
            "topic": { "type": "fixed", "name": "out" },
            "batch_size": 250
        }"#;
        let parsed: KafkaSinkConfig = serde_json::from_str(raw).unwrap();
        assert_eq!(parsed.batch_size, 250);
    }
}
