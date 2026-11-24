//! KafkaSink: the producer implementation.
//!
//! Built on the core sink driver (RFC 0014): `KafkaDriver` implements
//! `SinkDriver` (produce a page, flush) and `CommitTokenDriver` (a page and its
//! commit token in one producer transaction, the token read back from the
//! compacted side-topic); `DriverSink` derives the capabilities from them.

use crate::config::{KafkaSinkConfig, KafkaSinkTopic};
use crate::encode;
use crate::extract;
use async_trait::async_trait;
#[cfg(feature = "schema-registry")]
use faucet_common_kafka::KafkaValueFormat;
use faucet_common_kafka::OnKeyError;
use faucet_core::driver::{CommitTokenDriver, DriverSink, PageWrite, RowPolicy, SinkDriver};
use faucet_core::{CallSlot, FaucetError, Sink};
use futures::stream::{FuturesUnordered, StreamExt};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::future_producer::OwnedDeliveryResult;
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord, Producer};
use serde_json::Value;
use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "schema-registry")]
use faucet_common_kafka::schema_registry::client::SchemaRegistryClient;

pub struct KafkaSink {
    inner: DriverSink<KafkaDriver>,
}

/// The config's JSONPaths, parsed once.
struct Paths {
    topic: Option<extract::CompiledPath>,
    key: Option<extract::CompiledPath>,
    partition: Option<extract::CompiledPath>,
    headers: Option<extract::CompiledPath>,
}

impl Paths {
    fn new(config: &KafkaSinkConfig) -> Self {
        let compile = |path: &Option<String>| path.as_deref().map(extract::CompiledPath::new);
        Self {
            topic: match &config.topic {
                KafkaSinkTopic::Fixed { .. } => None,
                KafkaSinkTopic::FromPath { path, .. } => Some(extract::CompiledPath::new(path)),
            },
            key: compile(&config.key_path),
            partition: compile(&config.partition_path),
            headers: compile(&config.headers_path),
        }
    }
}

/// The producer primitives the sink runs on.
pub(crate) struct KafkaDriver {
    config: KafkaSinkConfig,
    producer: Arc<FutureProducer>,
    /// Lazily-built transactional producer for exactly-once writes. Built on
    /// the first `write_batch_idempotent` call (needs the scope-derived
    /// `transactional.id`, unknown at `new()`).
    txn: tokio::sync::OnceCell<Arc<FutureProducer>>,
    paths: Paths,
    #[cfg(feature = "schema-registry")]
    sr_client: Option<SchemaRegistryClient>,
    /// The registry the key format names (it may differ from the value's),
    /// falling back to the value's (#789 MSG-95).
    #[cfg(feature = "schema-registry")]
    key_sr_client: Option<SchemaRegistryClient>,
    #[cfg(feature = "schema-registry")]
    schema_contexts: SchemaContexts,
}

/// The value and key schema contexts of a topic.
#[cfg(feature = "schema-registry")]
type TopicContexts = Arc<(encode::SchemaContext, encode::SchemaContext)>;

/// Topics whose schema contexts are kept; past this they are built per record.
#[cfg(feature = "schema-registry")]
const SCHEMA_CONTEXT_TOPICS: usize = 1024;

/// Each topic's schema contexts, built on first use instead of per record.
#[cfg(feature = "schema-registry")]
#[derive(Default)]
struct SchemaContexts(std::sync::Mutex<std::collections::HashMap<String, TopicContexts>>);

#[cfg(feature = "schema-registry")]
impl SchemaContexts {
    fn for_topic(&self, topic: &str, config: &KafkaSinkConfig) -> TopicContexts {
        let mut cache = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(hit) = cache.get(topic) {
            return hit.clone();
        }
        let built = Arc::new(schema_contexts(topic, config));
        if cache.len() < SCHEMA_CONTEXT_TOPICS {
            cache.insert(topic.to_string(), built.clone());
        }
        built
    }
}

#[cfg(feature = "schema-registry")]
fn schema_contexts(
    topic: &str,
    config: &KafkaSinkConfig,
) -> (encode::SchemaContext, encode::SchemaContext) {
    (
        encode::SchemaContext {
            subject: format!("{topic}-value"),
            schema_text: config.value_schema.clone(),
        },
        encode::SchemaContext {
            subject: format!("{topic}-key"),
            schema_text: config.key_schema.clone(),
        },
    )
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
        #[cfg(feature = "schema-registry")]
        let key_sr_client = match &config.key_format {
            Some(kf) => build_sr_client(kf, Some(&config.value_format))?,
            None => None,
        };

        Ok(Self {
            inner: DriverSink::new(KafkaDriver {
                paths: Paths::new(&config),
                config,
                producer: Arc::new(producer),
                txn: tokio::sync::OnceCell::new(),
                #[cfg(feature = "schema-registry")]
                sr_client,
                #[cfg(feature = "schema-registry")]
                key_sr_client,
                #[cfg(feature = "schema-registry")]
                schema_contexts: SchemaContexts::default(),
            }),
        })
    }
}

#[async_trait]
impl Sink for KafkaSink {
    faucet_core::forward_sink!(inner);
}

impl KafkaDriver {
    fn resolve_topic<'a>(&'a self, record: &Value) -> Result<Cow<'a, str>, FaucetError> {
        match (&self.config.topic, &self.paths.topic) {
            (KafkaSinkTopic::Fixed { name }, _) => Ok(Cow::Borrowed(name)),
            (KafkaSinkTopic::FromPath { path, .. }, compiled) => {
                let found = match compiled {
                    Some(compiled) => compiled.string(record)?,
                    None => extract::string_at(record, path)?,
                };
                let topic = found.ok_or_else(|| {
                    FaucetError::Sink(format!("topic.path '{path}' did not resolve for record"))
                })?;
                self.config.check_record_topic(&topic)?;
                Ok(Cow::Owned(topic))
            }
        }
    }

    async fn build_record_bytes(
        &self,
        record: &Value,
        topic: &str,
    ) -> Result<(Vec<u8>, Option<Vec<u8>>), FaucetError> {
        #[cfg(feature = "schema-registry")]
        let contexts = self.schema_contexts.for_topic(topic, &self.config);
        #[cfg(feature = "schema-registry")]
        let (value_ctx, key_ctx) = (&contexts.0, &contexts.1);

        let value_bytes = encode::encode(
            record,
            &self.config.value_format,
            #[cfg(feature = "schema-registry")]
            self.sr_client.as_ref(),
            #[cfg(feature = "schema-registry")]
            value_ctx,
        )
        .await?;

        let key_bytes = match (&self.paths.key, &self.config.key_format) {
            (Some(path), Some(fmt)) => {
                // Extract the JSON sub-value at key_path and encode per key_format.
                let sub = path.value(record)?;
                match self.handle_key_extract(sub).await? {
                    Some(v) => Some(
                        encode::encode(
                            &v,
                            fmt,
                            #[cfg(feature = "schema-registry")]
                            self.key_sr_client.as_ref(),
                            #[cfg(feature = "schema-registry")]
                            key_ctx,
                        )
                        .await?,
                    ),
                    None => None,
                }
            }
            (Some(path), None) => {
                // Extract a string at key_path and use it directly as the key bytes.
                match path.string(record)? {
                    Some(s) => Some(s.into_bytes()),
                    None => match self.config.on_key_error {
                        OnKeyError::Fail => {
                            return Err(FaucetError::Sink(format!(
                                "key_path '{}' did not resolve and on_key_error=fail",
                                path.path()
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
            Some(v) if !v.is_null() => Ok(Some(v)),
            _ => match self.config.on_key_error {
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
    async fn txn_producer(
        &self,
        calls: &CallSlot,
        scope: &str,
    ) -> Result<Arc<FutureProducer>, FaucetError> {
        self.txn
            .get_or_try_init(|| async {
                let eo = self.config.exactly_once_spec();
                let prefix = eo.transactional_id_prefix.as_deref().unwrap_or("faucet");
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
                if let Some(cap) = crate::idempotent::txn_queue_capacity(&self.config) {
                    cfg.set("queue.buffering.max.messages", cap);
                }

                let producer: FutureProducer = cfg
                    .create()
                    .map_err(|e| FaucetError::Sink(format!("kafka txn producer init: {e}")))?;
                let producer = Arc::new(producer);

                // init_transactions is a blocking FFI call.
                let p = producer.clone();
                let timeout = self.config.message_timeout;
                let init = tokio::task::spawn_blocking(move || p.init_transactions(timeout));
                calls
                    .call_once("init", async {
                        init.await
                            .map_err(|e| {
                                FaucetError::Sink(format!("kafka init_transactions task: {e}"))
                            })?
                            .map_err(|e| FaucetError::Sink(format!("kafka init_transactions: {e}")))
                    })
                    .await?;

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
            KafkaValueFormat::ConfluentAvro {
                schema_registry, ..
            }
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
impl SinkDriver for KafkaDriver {
    fn connector_name(&self) -> &'static str {
        "kafka"
    }

    fn config_schema(&self) -> Value {
        let schema = schemars::schema_for!(KafkaSinkConfig);
        serde_json::to_value(&schema).unwrap_or(Value::Null)
    }

    fn set_call_context(&self, ctx: faucet_core::CallContext) {
        #[cfg(feature = "schema-registry")]
        for client in [&self.sr_client, &self.key_sr_client].into_iter().flatten() {
            client.set_call_context(ctx.clone());
        }
        let _ = ctx;
    }

    fn set_roundtrip_recorder(
        &self,
        recorder: std::sync::Arc<faucet_core::observability::RoundtripRecorder>,
    ) {
        #[cfg(feature = "schema-registry")]
        for client in [&self.sr_client, &self.key_sr_client].into_iter().flatten() {
            client.set_roundtrip_recorder(std::sync::Arc::clone(&recorder));
        }
        let _ = recorder;
    }

    fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        self.config.batch_atomicity()
    }

    fn dataset_uri(&self) -> String {
        use crate::config::KafkaSinkTopic;
        let topic = match &self.config.topic {
            KafkaSinkTopic::Fixed { name } => name.clone(),
            KafkaSinkTopic::FromPath { path, .. } => format!("(from_path:{path})"),
        };
        format!("kafka://{}?topic={}", self.config.brokers, topic)
    }

    /// Records dropped by `on_key_error: skip` are reported as skipped, so row
    /// accounting (#879) does not take them for a silent loss.
    ///
    /// Every policy writes the whole page or fails it: a record that cannot be
    /// encoded or routed fails the page.
    async fn write(
        &self,
        calls: &CallSlot,
        records: &[Value],
        _policy: RowPolicy,
    ) -> Result<PageWrite, FaucetError> {
        let calls = calls.get();
        let mut in_flight: FuturesUnordered<_> = FuturesUnordered::new();
        let mut produced = 0usize;
        let mut skipped = 0usize;
        let in_flight_cap = self.config.in_flight_window();

        for record in records {
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

            let partition = match &self.paths.partition {
                Some(p) => p.partition(record)?,
                None => None,
            };

            // Kafka headers (#657). A path that resolves to nothing yields no
            // headers; one that resolves to a non-object fails the batch, which
            // matches `partition_path`'s strictness — a header map the operator
            // asked for and did not get is not something to paper over.
            let headers = match &self.paths.headers {
                Some(p) => p.headers(record)?,
                None => None,
            };

            let delivery = enqueue_with_queue_full_retry(
                &calls,
                &self.producer,
                &topic,
                &value_bytes,
                &RecordRouting {
                    key: key_bytes,
                    partition,
                    headers,
                },
                self.config.queue_full_max_retries,
                self.config.queue_full_backoff,
            )
            .await?;
            in_flight.push(confirm_delivery(calls.clone(), delivery));
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
        Ok(PageWrite::reported(
            faucet_core::WriteReport::accepted(produced).with_skipped(skipped),
        ))
    }

    async fn flush(&self, calls: &CallSlot) -> Result<(), FaucetError> {
        // rd_kafka_flush is a blocking FFI call.
        let producer = self.producer.clone();
        let timeout = self.config.message_timeout;
        let flush = tokio::task::spawn_blocking(move || producer.flush(timeout));
        calls
            .call_once("flush", async {
                flush
                    .await
                    .map_err(|e| FaucetError::Sink(format!("kafka producer flush task: {e}")))?
                    .map_err(|e| FaucetError::Sink(format!("kafka producer flush: {e}")))
            })
            .await
    }

    fn commit_tokens(&self) -> Option<&dyn CommitTokenDriver> {
        Some(self)
    }

    fn capabilities(&self, derived: faucet_core::Capabilities) -> faucet_core::Capabilities {
        #[cfg(feature = "schema-registry")]
        let avro = matches!(
            self.config.value_format,
            KafkaValueFormat::ConfluentAvro { .. }
        );
        #[cfg(not(feature = "schema-registry"))]
        let avro = false;
        derived
            .to_builder()
            .schema_report(crate::descriptor::schema_report(avro))
            .build()
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
        _calls: &CallSlot,
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

#[async_trait]
impl CommitTokenDriver for KafkaDriver {
    async fn last_token(
        &self,
        calls: &CallSlot,
        scope: &str,
    ) -> Result<Option<String>, FaucetError> {
        let base = crate::idempotent::client_config_base(&self.config)?;
        calls
            .call_once(
                "metadata",
                crate::idempotent::ensure_commit_topic(&self.config, &base),
            )
            .await?;
        // init_transactions fences and aborts this scope's own transaction
        // left open by a crash, which would otherwise hold the side-topic's
        // stable offset until the broker's timeout sweep.
        self.txn_producer(calls, scope).await?;
        calls
            .call_once(
                "read_token",
                crate::idempotent::read_last_token(&self.config, &base, scope),
            )
            .await
    }

    /// Records skipped under `on_key_error: skip` are reported as skipped, so
    /// the exactly-once page accounts for every row it was offered (#879).
    async fn write_with_token(
        &self,
        calls: &CallSlot,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<faucet_core::WriteReport, FaucetError> {
        let producer = self.txn_producer(calls, scope).await?;
        let calls = calls.get();

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

            let partition = match &self.paths.partition {
                Some(p) => match p.partition(record) {
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

            let headers = match &self.paths.headers {
                Some(p) => match p.headers(record) {
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
                &calls,
                &producer,
                &topic,
                &value_bytes,
                &RecordRouting {
                    key: key_bytes,
                    partition,
                    headers,
                },
                self.config.queue_full_backoff,
                self.config.message_timeout,
            )
            .await
            {
                let _ = crate::idempotent::abort_txn(producer.clone(), self.config.message_timeout)
                    .await;
                return Err(e);
            }
            produced += 1;
        }

        // Enqueue the commit-token record (key = `token_key(prefix, scope)`,
        // value = token). No headers: this is faucet's own watermark on its own
        // side-topic, not a user record, so `headers_path` must not reach it.
        let eo = self.config.exactly_once_spec();
        let token_key = crate::idempotent::token_key(eo.transactional_id_prefix.as_deref(), scope);
        if let Err(e) = crate::idempotent::enqueue_in_txn(
            &calls,
            &producer,
            &eo.commit_token_topic,
            token.as_bytes(),
            &RecordRouting {
                key: Some(token_key.into_bytes()),
                ..RecordRouting::default()
            },
            self.config.queue_full_backoff,
            self.config.message_timeout,
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
        let commit = tokio::task::spawn_blocking(move || p.commit_transaction(timeout));
        let commit = match calls
            .call_once("commit", async {
                commit
                    .await
                    .map_err(|e| FaucetError::Sink(format!("kafka commit task: {e}")))
            })
            .await
        {
            Ok(commit) => commit,
            // The run's deadline cut the commit: its outcome is unknown, and
            // aborting fails harmlessly when it already committed.
            Err(e) => {
                let _ = crate::idempotent::abort_txn(producer.clone(), self.config.message_timeout)
                    .await;
                return Err(e);
            }
        };
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
        Ok(faucet_core::WriteReport::accepted(produced).with_skipped(skipped))
    }
}

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

/// Hand one record to the producer's queue without waiting for its delivery.
/// `Ok(None)` means the queue is full; any other refusal is the send error.
pub(crate) fn try_enqueue(
    producer: &FutureProducer,
    topic: &str,
    value_bytes: &[u8],
    routing: &RecordRouting,
) -> Result<Option<DeliveryFuture>, FaucetError> {
    let mut record: FutureRecord<'_, [u8], [u8]> = FutureRecord::to(topic).payload(value_bytes);
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
        Ok(delivery) => Ok(Some(delivery)),
        Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _record)) => Ok(None),
        Err((e, _record)) => Err(FaucetError::Sink(format!("kafka send: {e}"))),
    }
}

/// Enqueue one record, retrying a full queue up to `max_retries` times with
/// `backoff` between attempts. Records are enqueued in page order.
async fn enqueue_with_queue_full_retry(
    calls: &faucet_core::CallContext,
    producer: &FutureProducer,
    topic: &str,
    value_bytes: &[u8],
    routing: &RecordRouting,
    max_retries: u32,
    backoff: Duration,
) -> Result<DeliveryFuture, FaucetError> {
    let mut attempts: u32 = 0;
    loop {
        if let Some(delivery) = try_enqueue(producer, topic, value_bytes, routing)? {
            return Ok(delivery);
        }
        if attempts >= max_retries {
            return Err(FaucetError::Sink(format!(
                "kafka send: QueueFull after {max_retries} retries"
            )));
        }
        tracing::warn!(attempts, "kafka send: QueueFull, backing off");
        calls.wait(backoff).await?;
        attempts += 1;
    }
}

/// Wait for one enqueued record's delivery report.
async fn confirm_delivery(
    calls: faucet_core::CallContext,
    delivery: DeliveryFuture,
) -> Result<(), FaucetError> {
    let delivered = calls
        .call_once("produce", async { Ok(delivery.await) })
        .await?;
    delivery_outcome(delivered)
}

fn delivery_outcome(
    delivered: Result<OwnedDeliveryResult, futures::channel::oneshot::Canceled>,
) -> Result<(), FaucetError> {
    match delivered {
        Ok(Ok(_delivery)) => Ok(()),
        Ok(Err((KafkaError::MessageProduction(RDKafkaErrorCode::MessageSizeTooLarge), _))) => Err(
            FaucetError::Sink("kafka send: record exceeds broker max.message.bytes".into()),
        ),
        Ok(Err((e, _msg))) => Err(FaucetError::Sink(format!("kafka send: {e}"))),
        Err(_canceled) => Err(FaucetError::Sink(
            "kafka send: delivery future canceled (producer dropped)".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use faucet_core::Sink as _;

    #[test]
    fn a_failed_delivery_report_names_its_cause() {
        use rdkafka::error::{KafkaError, RDKafkaErrorCode};
        use rdkafka::message::{OwnedMessage, Timestamp};
        let failed = |code| {
            let msg =
                OwnedMessage::new(None, None, "t".into(), Timestamp::NotAvailable, 0, 0, None);
            super::delivery_outcome(Ok(Err((KafkaError::MessageProduction(code), msg))))
                .unwrap_err()
                .to_string()
        };
        assert!(
            failed(RDKafkaErrorCode::MessageSizeTooLarge)
                .contains("kafka send: record exceeds broker max.message.bytes")
        );
        let other = failed(RDKafkaErrorCode::UnknownTopic);
        assert!(
            other.contains("kafka send: Message production error"),
            "{other}"
        );
        let dropped = super::delivery_outcome(Err(futures::channel::oneshot::Canceled))
            .unwrap_err()
            .to_string();
        assert!(
            dropped.contains("kafka send: delivery future canceled (producer dropped)"),
            "{dropped}"
        );
    }

    /// The sink builds offline (the producer connects lazily) and describes
    /// its config.
    #[tokio::test]
    async fn the_config_schema_describes_the_sink_config() {
        let cfg: crate::KafkaSinkConfig = serde_json::from_value(serde_json::json!({
            "brokers": "127.0.0.1:1",
            "topic": {"type": "fixed", "name": "t"}
        }))
        .unwrap();
        let sink = crate::KafkaSink::new(cfg).await.unwrap();
        assert!(sink.config_schema()["properties"]["brokers"].is_object());
        assert_eq!(sink.connector_name(), "kafka");
    }

    // dataset_uri test is skipped: KafkaSink::new() requires a live Kafka
    // broker (creates a FutureProducer in new()), and no offline constructor
    // exists.

    #[cfg(feature = "schema-registry")]
    mod schema_contexts {
        use crate::sink::{SCHEMA_CONTEXT_TOPICS, SchemaContexts, schema_contexts};

        fn config() -> crate::KafkaSinkConfig {
            serde_json::from_value(serde_json::json!({
                "brokers": "127.0.0.1:1",
                "topic": {"type": "from_path", "path": "$.t"},
                "value_schema": "{\"type\":\"string\"}",
                "key_schema": "\"long\""
            }))
            .unwrap()
        }

        #[test]
        fn a_topic_gets_the_contexts_built_per_record_before() {
            let cfg = config();
            let cache = SchemaContexts::default();
            for topic in ["orders", "orders", "events", ""] {
                let got = cache.for_topic(topic, &cfg);
                assert_eq!(got.0.subject, format!("{topic}-value"));
                assert_eq!(got.0.schema_text, cfg.value_schema);
                assert_eq!(got.1.subject, format!("{topic}-key"));
                assert_eq!(got.1.schema_text, cfg.key_schema);
            }
            let first = cache.for_topic("orders", &cfg);
            assert!(std::sync::Arc::ptr_eq(
                &first,
                &cache.for_topic("orders", &cfg)
            ));
        }

        #[test]
        fn topics_past_the_cap_are_built_and_not_kept() {
            let cfg = config();
            let cache = SchemaContexts::default();
            for i in 0..SCHEMA_CONTEXT_TOPICS {
                cache.for_topic(&format!("t{i}"), &cfg);
            }
            let late = cache.for_topic("late", &cfg);
            assert_eq!(late.0.subject, "late-value");
            assert_eq!(cache.0.lock().unwrap().len(), SCHEMA_CONTEXT_TOPICS);
            let direct = schema_contexts("late", &cfg);
            assert_eq!(late.1.subject, direct.1.subject);
            assert_eq!(late.1.schema_text, direct.1.schema_text);
        }
    }

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
                bytes_encoding: Default::default(),
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
