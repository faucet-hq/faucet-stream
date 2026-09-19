//! KafkaSink: the producer implementation.

use crate::config::{KafkaSinkConfig, KafkaSinkTopic};
use crate::encode;
use crate::extract;
use async_trait::async_trait;
#[cfg(feature = "schema-registry")]
use faucet_common_kafka::KafkaValueFormat;
use faucet_common_kafka::OnKeyError;
use faucet_core::{FaucetError, Sink};
use futures::stream::{FuturesUnordered, StreamExt};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "schema-registry")]
use faucet_common_kafka::schema_registry::client::SchemaRegistryClient;

pub struct KafkaSink {
    config: KafkaSinkConfig,
    producer: Arc<FutureProducer>,
    /// Lazily-built transactional producer for exactly-once writes. Built on
    /// the first `write_batch_idempotent` call (needs the scope-derived
    /// `transactional.id`, unknown at `new()`).
    txn: tokio::sync::OnceCell<Arc<FutureProducer>>,
    #[cfg(feature = "schema-registry")]
    sr_client: Option<SchemaRegistryClient>,
}

impl KafkaSink {
    pub async fn new(config: KafkaSinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;

        let client_config = crate::idempotent::producer_client_config(&config)?;

        let producer: FutureProducer = client_config
            .create()
            .map_err(|e| FaucetError::Sink(format!("kafka producer init: {e}")))?;

        #[cfg(feature = "schema-registry")]
        let sr_client = build_sr_client(&config.value_format, config.key_format.as_ref())?;

        Ok(Self {
            config,
            producer: Arc::new(producer),
            txn: tokio::sync::OnceCell::new(),
            #[cfg(feature = "schema-registry")]
            sr_client,
        })
    }

    fn resolve_topic(&self, record: &Value) -> Result<String, FaucetError> {
        match &self.config.topic {
            KafkaSinkTopic::Fixed { name } => Ok(name.clone()),
            KafkaSinkTopic::FromPath { path } => {
                extract::string_at(record, path)?.ok_or_else(|| {
                    FaucetError::Sink(format!("topic.path '{path}' did not resolve for record"))
                })
            }
        }
    }

    async fn build_record_bytes(
        &self,
        record: &Value,
        topic: &str,
    ) -> Result<(Vec<u8>, Option<Vec<u8>>), FaucetError> {
        #[cfg(feature = "schema-registry")]
        let value_ctx = encode::SchemaContext {
            subject: format!("{topic}-value"),
            schema_text: self.config.value_schema.clone(),
        };
        #[cfg(feature = "schema-registry")]
        let key_ctx = encode::SchemaContext {
            subject: format!("{topic}-key"),
            schema_text: self.config.key_schema.clone(),
        };

        let value_bytes = encode::encode(
            record,
            &self.config.value_format,
            #[cfg(feature = "schema-registry")]
            self.sr_client.as_ref(),
            #[cfg(feature = "schema-registry")]
            &value_ctx,
        )
        .await?;

        let key_bytes = match (&self.config.key_path, &self.config.key_format) {
            (Some(path), Some(fmt)) => {
                // Extract the JSON sub-value at key_path and encode per key_format.
                let sub = extract::value_at(record, path)?;
                match self.handle_key_extract(sub).await? {
                    Some(v) => Some(
                        encode::encode(
                            &v,
                            fmt,
                            #[cfg(feature = "schema-registry")]
                            self.sr_client.as_ref(),
                            #[cfg(feature = "schema-registry")]
                            &key_ctx,
                        )
                        .await?,
                    ),
                    None => None,
                }
            }
            (Some(path), None) => {
                // Extract a string at key_path and use it directly as the key bytes.
                match extract::string_at(record, path)? {
                    Some(s) => Some(s.into_bytes()),
                    None => match self.config.on_key_error {
                        OnKeyError::Fail => {
                            return Err(FaucetError::Sink(format!(
                                "key_path '{path}' did not resolve and on_key_error=fail"
                            )));
                        }
                        OnKeyError::Skip | OnKeyError::RoundRobin => None,
                    },
                }
            }
            (None, _) => None,
        };

        let _ = topic; // used in feature-gated branch
        Ok((value_bytes, key_bytes))
    }

    async fn handle_key_extract(
        &self,
        extracted: Option<Value>,
    ) -> Result<Option<Value>, FaucetError> {
        match extracted {
            Some(v) => Ok(Some(v)),
            None => match self.config.on_key_error {
                OnKeyError::Fail => Err(FaucetError::Sink(
                    "key_path did not resolve and on_key_error=fail".into(),
                )),
                OnKeyError::Skip | OnKeyError::RoundRobin => Ok(None),
            },
        }
    }

    /// Build (once) the transactional producer for `scope` and run
    /// `init_transactions` — which also fences any zombie producer sharing this
    /// `transactional.id`. Cached for the run; a fatal producer error fails the
    /// run and the whole sink is rebuilt on the next run.
    async fn txn_producer(&self, scope: &str) -> Result<Arc<FutureProducer>, FaucetError> {
        self.txn
            .get_or_try_init(|| async {
                let prefix = self
                    .config
                    .transactional_id_prefix
                    .as_deref()
                    .unwrap_or("faucet");
                let txn_id = crate::idempotent::derive_transactional_id(prefix, scope);

                let mut cfg = crate::idempotent::producer_client_config(&self.config)?;
                // Force the transactional invariants AFTER extra_client_config so
                // a user override cannot disable them and break EOS.
                cfg.set("transactional.id", &txn_id);
                cfg.set("enable.idempotence", "true");
                cfg.set("acks", "all");
                // message.timeout.ms must be <= transaction.timeout.ms; raise the
                // transaction timeout floor so a large message_timeout config does
                // not make init_transactions reject the producer.
                let msg_timeout_ms = self.config.message_timeout.as_millis();
                let txn_timeout_ms = msg_timeout_ms.max(60_000);
                cfg.set("transaction.timeout.ms", txn_timeout_ms.to_string());

                let producer: FutureProducer = cfg
                    .create()
                    .map_err(|e| FaucetError::Sink(format!("kafka txn producer init: {e}")))?;
                let producer = Arc::new(producer);

                // init_transactions is a blocking FFI call.
                let p = producer.clone();
                let timeout = self.config.message_timeout;
                tokio::task::spawn_blocking(move || p.init_transactions(timeout))
                    .await
                    .map_err(|e| FaucetError::Sink(format!("kafka init_transactions task: {e}")))?
                    .map_err(|e| FaucetError::Sink(format!("kafka init_transactions: {e}")))?;

                Ok::<_, FaucetError>(producer)
            })
            .await
            .cloned()
    }
}

#[cfg(feature = "schema-registry")]
fn build_sr_client(
    value_format: &KafkaValueFormat,
    key_format: Option<&KafkaValueFormat>,
) -> Result<Option<SchemaRegistryClient>, FaucetError> {
    fn cfg(f: &KafkaValueFormat) -> Option<&faucet_common_kafka::SchemaRegistryConfig> {
        match f {
            KafkaValueFormat::ConfluentAvro { schema_registry }
            | KafkaValueFormat::ConfluentProtobuf { schema_registry } => Some(schema_registry),
            KafkaValueFormat::ConfluentJsonSchema {
                schema_registry, ..
            } => Some(schema_registry),
            _ => None,
        }
    }
    let c = cfg(value_format).or_else(|| key_format.and_then(cfg));
    c.map(SchemaRegistryClient::new).transpose()
}

#[async_trait]
impl Sink for KafkaSink {
    fn dataset_uri(&self) -> String {
        use crate::config::KafkaSinkTopic;
        let topic = match &self.config.topic {
            KafkaSinkTopic::Fixed { name } => name.clone(),
            KafkaSinkTopic::FromPath { path } => format!("(from_path:{path})"),
        };
        format!("kafka://{}?topic={}", self.config.brokers, topic)
    }

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        let mut in_flight: FuturesUnordered<_> = FuturesUnordered::new();
        let mut produced = 0usize;
        let mut skipped = 0usize;

        // Effective in-flight cap: when `batch_size > 0`, take the smaller of
        // `max_in_flight` and `batch_size` so the FuturesUnordered window
        // never exceeds the streaming-pipeline page size. The `batch_size = 0`
        // sentinel keeps the historical behaviour — bounded only by
        // `max_in_flight`.
        let in_flight_cap = if self.config.batch_size > 0 {
            self.config.max_in_flight.min(self.config.batch_size)
        } else {
            self.config.max_in_flight
        };

        for record in records {
            // Drain one if we're at capacity.
            if in_flight.len() >= in_flight_cap
                && let Some(res) = in_flight.next().await
            {
                match res {
                    Ok(()) => produced += 1,
                    Err(e) => return Err(e),
                }
            }

            let topic = self.resolve_topic(record)?;
            let (value_bytes, key_bytes) = self.build_record_bytes(record, &topic).await?;

            // Honour OnKeyError::Skip: if key was required but missing, the
            // extract step returned None and on_key_error decided to skip.
            if self.config.key_path.is_some()
                && key_bytes.is_none()
                && matches!(self.config.on_key_error, OnKeyError::Skip)
            {
                skipped += 1;
                continue;
            }

            let partition = match &self.config.partition_path {
                Some(p) => extract::partition_at(record, p)?,
                None => None,
            };

            // Kafka headers (#657). A path that resolves to nothing yields no
            // headers; one that resolves to a non-object fails the batch, which
            // matches `partition_path`'s strictness — a header map the operator
            // asked for and did not get is not something to paper over.
            let headers = match &self.config.headers_path {
                Some(p) => extract::headers_at(record, p)?,
                None => None,
            };

            let producer = self.producer.clone();
            let topic_owned = topic.clone();
            let message_timeout = self.config.message_timeout;
            let retries = self.config.queue_full_max_retries;
            let backoff = self.config.queue_full_backoff;

            in_flight.push(async move {
                send_with_queue_full_retry(
                    &producer,
                    &topic_owned,
                    value_bytes,
                    RecordRouting {
                        key: key_bytes,
                        partition,
                        headers,
                    },
                    message_timeout,
                    retries,
                    backoff,
                )
                .await
            });
        }

        while let Some(res) = in_flight.next().await {
            match res {
                Ok(()) => produced += 1,
                Err(e) => return Err(e),
            }
        }

        if skipped > 0 {
            tracing::warn!(
                skipped,
                "kafka sink: dropped records due to OnKeyError::Skip"
            );
        }
        Ok(produced)
    }

    async fn flush(&self) -> Result<(), FaucetError> {
        self.producer
            .flush(self.config.message_timeout)
            .map_err(|e| FaucetError::Sink(format!("kafka producer flush: {e}")))?;
        Ok(())
    }

    fn supports_idempotent_writes(&self) -> bool {
        true
    }

    async fn last_committed_token(&self, scope: &str) -> Result<Option<String>, FaucetError> {
        let base = crate::idempotent::client_config_base(&self.config)?;
        crate::idempotent::ensure_commit_topic(&self.config, &base).await?;
        crate::idempotent::read_last_token(&self.config, &base, scope).await
    }

    async fn write_batch_idempotent(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<usize, FaucetError> {
        let producer = self.txn_producer(scope).await?;

        producer
            .begin_transaction()
            .map_err(|e| FaucetError::Sink(format!("kafka begin_transaction: {e}")))?;

        let mut produced = 0usize;
        let mut skipped = 0usize;

        // Enqueue all data records (non-awaiting — delivery completes at commit).
        for record in records {
            let topic = match self.resolve_topic(record) {
                Ok(t) => t,
                Err(e) => {
                    let _ =
                        crate::idempotent::abort_txn(producer.clone(), self.config.message_timeout)
                            .await;
                    return Err(e);
                }
            };
            let (value_bytes, key_bytes) = match self.build_record_bytes(record, &topic).await {
                Ok(v) => v,
                Err(e) => {
                    let _ =
                        crate::idempotent::abort_txn(producer.clone(), self.config.message_timeout)
                            .await;
                    return Err(e);
                }
            };

            if self.config.key_path.is_some()
                && key_bytes.is_none()
                && matches!(self.config.on_key_error, OnKeyError::Skip)
            {
                skipped += 1;
                continue;
            }

            let partition = match &self.config.partition_path {
                Some(p) => match extract::partition_at(record, p) {
                    Ok(v) => v,
                    Err(e) => {
                        let _ = crate::idempotent::abort_txn(
                            producer.clone(),
                            self.config.message_timeout,
                        )
                        .await;
                        return Err(e);
                    }
                },
                None => None,
            };

            let headers = match &self.config.headers_path {
                Some(p) => match extract::headers_at(record, p) {
                    Ok(v) => v,
                    Err(e) => {
                        let _ = crate::idempotent::abort_txn(
                            producer.clone(),
                            self.config.message_timeout,
                        )
                        .await;
                        return Err(e);
                    }
                },
                None => None,
            };

            if let Err(e) = crate::idempotent::enqueue_in_txn(
                &producer,
                &topic,
                value_bytes,
                RecordRouting {
                    key: key_bytes,
                    partition,
                    headers,
                },
                self.config.queue_full_max_retries,
                self.config.queue_full_backoff,
            )
            .await
            {
                let _ = crate::idempotent::abort_txn(producer.clone(), self.config.message_timeout)
                    .await;
                return Err(e);
            }
            produced += 1;
        }

        // Enqueue the commit-token record (key = scope, value = token). No
        // headers: this is faucet's own watermark on its own side-topic, not a
        // user record, so `headers_path` must not reach it.
        if let Err(e) = crate::idempotent::enqueue_in_txn(
            &producer,
            &self.config.commit_token_topic,
            token.as_bytes().to_vec(),
            RecordRouting {
                key: Some(scope.as_bytes().to_vec()),
                ..RecordRouting::default()
            },
            self.config.queue_full_max_retries,
            self.config.queue_full_backoff,
        )
        .await
        {
            let _ =
                crate::idempotent::abort_txn(producer.clone(), self.config.message_timeout).await;
            return Err(e);
        }

        // Commit atomically (blocking FFI). Errors → abort + propagate.
        let p = producer.clone();
        let timeout = self.config.message_timeout;
        let commit = tokio::task::spawn_blocking(move || p.commit_transaction(timeout))
            .await
            .map_err(|e| FaucetError::Sink(format!("kafka commit task: {e}")))?;
        if let Err(e) = commit {
            let _ =
                crate::idempotent::abort_txn(producer.clone(), self.config.message_timeout).await;
            return Err(FaucetError::Sink(format!("kafka commit_transaction: {e}")));
        }

        if skipped > 0 {
            tracing::warn!(
                skipped,
                "kafka sink: dropped records due to OnKeyError::Skip"
            );
        }
        Ok(produced)
    }

    fn connector_name(&self) -> &'static str {
        "kafka"
    }

    fn config_schema(&self) -> Value {
        let schema = schemars::schema_for!(KafkaSinkConfig);
        serde_json::to_value(&schema).unwrap_or(Value::Null)
    }

    /// Preflight check (`faucet doctor`).
    ///
    /// Fetches cluster metadata for all topics via the existing producer's
    /// underlying client (`producer.client().fetch_metadata(None, timeout)`),
    /// which validates broker connectivity and authentication without
    /// producing any messages. `fetch_metadata` is a blocking librdkafka
    /// call, so it runs on a blocking thread; the whole probe is bounded by
    /// `ctx.timeout` (also passed to librdkafka as its own metadata timeout).
    /// Connection/auth failures surface as a `Fail` probe with a hint; no
    /// credentials are placed in the reason/hint.
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};

        let started = std::time::Instant::now();
        let producer = self.producer.clone();
        let timeout = ctx.timeout;

        // `fetch_metadata` blocks (FFI into librdkafka), so run it off the
        // async runtime. Bound the await with `ctx.timeout` as well, in case
        // the blocking call ignores its own deadline.
        let join = tokio::time::timeout(
            timeout,
            tokio::task::spawn_blocking(move || producer.client().fetch_metadata(None, timeout)),
        )
        .await;

        let probe = match join {
            Ok(Ok(Ok(_metadata))) => Probe::pass("metadata", started.elapsed()),
            Ok(Ok(Err(e))) => Probe::fail_hint(
                "metadata",
                started.elapsed(),
                format!("kafka fetch_metadata failed: {e}"),
                "Verify the broker list is reachable and that any SASL/TLS \
                 credentials and protocol settings are correct.",
            ),
            Ok(Err(join_err)) => Probe::fail(
                "metadata",
                started.elapsed(),
                format!("kafka metadata probe task failed: {join_err}"),
            ),
            Err(_elapsed) => Probe::fail_hint(
                "metadata",
                started.elapsed(),
                format!("kafka fetch_metadata timed out after {timeout:?}"),
                "Check broker reachability and authentication; the cluster did \
                 not return metadata within the timeout.",
            ),
        };

        Ok(CheckReport::single(probe))
    }
}

/// Send a single record, retrying on `QueueFull` up to `max_retries` times
/// with `backoff` delay between attempts.
///
/// Where one record goes and what travels with it: the key, an explicit
/// partition, and the message headers, each resolved per record from its own
/// JSONPath.
///
/// Grouped rather than passed as three more positional `Option`s — the senders
/// were already at the edge of readability, and a caller that swapped two of
/// them would be building a subtly wrong message.
#[derive(Debug, Default)]
pub(crate) struct RecordRouting {
    pub key: Option<Vec<u8>>,
    pub partition: Option<i32>,
    pub headers: Option<serde_json::Map<String, Value>>,
}

/// Convert an extracted header map into librdkafka's `OwnedHeaders`.
///
/// `extract::headers_at` has already stringified every value, so the only
/// decision left is `null`: Kafka headers permit a *valueless* header, and that
/// is a truer rendering of JSON `null` than the four-character string
/// `"null"` — which a consumer would have to know to special-case.
pub(crate) fn owned_headers(map: &serde_json::Map<String, Value>) -> OwnedHeaders {
    let mut out = OwnedHeaders::new_with_capacity(map.len());
    for (k, v) in map {
        let value = match v {
            Value::Null => None,
            Value::String(s) => Some(s.as_str()),
            // Unreachable today (`headers_at` stringifies), but leaving the
            // match exhaustive means a change there cannot silently drop a
            // header here.
            _ => None,
        };
        out = out.insert(Header { key: k, value });
    }
    out
}

/// Uses `send_result()` (non-blocking enqueue) so we can apply our own retry
/// schedule rather than the librdkafka built-in queue timeout.
/// `_message_timeout` is kept in the signature for callers that track it but
/// is not needed with `send_result()`.
#[allow(clippy::too_many_arguments)]
async fn send_with_queue_full_retry(
    producer: &FutureProducer,
    topic: &str,
    value_bytes: Vec<u8>,
    routing: RecordRouting,
    _message_timeout: Duration,
    max_retries: u32,
    backoff: Duration,
) -> Result<(), FaucetError> {
    let mut attempts: u32 = 0;
    loop {
        // Build a fresh FutureRecord each iteration because send_result()
        // returns the record back on QueueFull so we can reconstruct it.
        let mut record: FutureRecord<'_, [u8], [u8]> =
            FutureRecord::to(topic).payload(value_bytes.as_slice());
        if let Some(k) = routing.key.as_deref() {
            record = record.key(k);
        }
        if let Some(p) = routing.partition {
            record = record.partition(p);
        }
        if let Some(h) = routing.headers.as_ref() {
            record = record.headers(owned_headers(h));
        }

        match producer.send_result(record) {
            Ok(delivery_future) => {
                // Record enqueued — await the delivery confirmation.
                match delivery_future.await {
                    Ok(Ok(_delivery)) => return Ok(()),
                    Ok(Err((
                        KafkaError::MessageProduction(RDKafkaErrorCode::MessageSizeTooLarge),
                        _,
                    ))) => {
                        return Err(FaucetError::Sink(
                            "kafka send: record exceeds broker max.message.bytes".into(),
                        ));
                    }
                    Ok(Err((e, _msg))) => {
                        return Err(FaucetError::Sink(format!("kafka send: {e}")));
                    }
                    Err(_canceled) => {
                        return Err(FaucetError::Sink(
                            "kafka send: delivery future canceled (producer dropped)".into(),
                        ));
                    }
                }
            }
            Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _record)) => {
                if attempts >= max_retries {
                    return Err(FaucetError::Sink(format!(
                        "kafka send: QueueFull after {max_retries} retries"
                    )));
                }
                tracing::warn!(attempts, "kafka send: QueueFull, backing off");
                tokio::time::sleep(backoff).await;
                attempts += 1;
            }
            Err((e, _record)) => {
                return Err(FaucetError::Sink(format!("kafka send: {e}")));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    // dataset_uri test is skipped: KafkaSink::new() requires a live Kafka
    // broker (creates a FutureProducer in new()), and no offline constructor
    // exists.

    #[cfg(feature = "schema-registry")]
    mod sr_client {
        use crate::sink::build_sr_client;
        use faucet_common_kafka::{KafkaValueFormat, SchemaRegistryConfig};

        #[test]
        fn build_sr_client_none_for_plain_formats() {
            assert!(
                build_sr_client(&KafkaValueFormat::Json, None)
                    .unwrap()
                    .is_none()
            );
            assert!(
                build_sr_client(&KafkaValueFormat::RawString, Some(&KafkaValueFormat::Bytes))
                    .unwrap()
                    .is_none()
            );
        }

        #[test]
        fn build_sr_client_some_for_confluent_avro_value() {
            let format = KafkaValueFormat::ConfluentAvro {
                schema_registry: SchemaRegistryConfig::new("http://localhost:8081"),
            };
            assert!(build_sr_client(&format, None).unwrap().is_some());
        }

        #[test]
        fn build_sr_client_some_for_confluent_protobuf_value() {
            let format = KafkaValueFormat::ConfluentProtobuf {
                schema_registry: SchemaRegistryConfig::new("http://localhost:8081"),
            };
            assert!(build_sr_client(&format, None).unwrap().is_some());
        }

        #[test]
        fn build_sr_client_some_for_confluent_json_schema_value() {
            let format = KafkaValueFormat::ConfluentJsonSchema {
                schema_registry: SchemaRegistryConfig::new("http://localhost:8081"),
                validate: false,
            };
            assert!(build_sr_client(&format, None).unwrap().is_some());
        }

        #[test]
        fn build_sr_client_falls_back_to_key_format() {
            let key = KafkaValueFormat::ConfluentProtobuf {
                schema_registry: SchemaRegistryConfig::new("http://localhost:8081"),
            };
            assert!(
                build_sr_client(&KafkaValueFormat::Json, Some(&key))
                    .unwrap()
                    .is_some()
            );
        }

        #[test]
        fn build_sr_client_propagates_invalid_url() {
            let format = KafkaValueFormat::ConfluentJsonSchema {
                schema_registry: SchemaRegistryConfig::new("not-a-url"),
                validate: false,
            };
            assert!(build_sr_client(&format, None).is_err());
        }
    }
}
