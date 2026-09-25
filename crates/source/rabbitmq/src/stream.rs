//! `RabbitMqSource` — the AMQP consumer (the one module that does I/O).
//!
//! Each `stream_pages` call opens its own connection and channel, declares the
//! queue and bindings, and consumes until `max_messages` or
//! `idle_timeout_secs` fires, buffering up to `batch_size` records per
//! [`StreamPage`].
//!
//! **Deferred acks (`ack_mode: on_sink_confirm`).** A page's deliveries are
//! acknowledged (one `basic.ack` with `multiple`) only when the generator is
//! resumed after yielding it. The pipeline polls the next page only after it
//! has written the previous one and — because every page carries a bookmark —
//! flushed the sink, so resuming means the page is durable. If the run fails or
//! is dropped first, the channel closes with the deliveries unacknowledged and
//! the broker requeues them (at-least-once).

use crate::config::{AckMode, OnDecodeError, RabbitMqSourceConfig};
use async_trait::async_trait;
use faucet_common_rabbitmq::{amqp_error, declare_exchange, decode_payload, field_table_to_json};
use faucet_core::{FaucetError, Source, Stream, StreamPage};
use futures::StreamExt;
use lapin::message::Delivery;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicQosOptions, BasicRejectOptions, QueueBindOptions,
    QueueDeclareOptions,
};
use lapin::types::FieldTable;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::pin::Pin;
use std::time::{Duration, Instant};

/// A source that consumes a RabbitMQ queue and emits each message as a record.
///
/// Construction validates the config but does not connect; the connection is
/// opened on the first poll, so an unreachable broker fails there.
pub struct RabbitMqSource {
    config: RabbitMqSourceConfig,
}

impl RabbitMqSource {
    /// Create a new RabbitMQ source. Validates the config; does not connect.
    pub async fn new(config: RabbitMqSourceConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        Ok(Self { config })
    }

    /// Open a channel, apply QoS, declare the queue/bindings, and start the
    /// consumer.
    async fn open(
        &self,
    ) -> Result<(lapin::Connection, lapin::Channel, lapin::Consumer), FaucetError> {
        let cfg = &self.config;
        let conn = faucet_common_rabbitmq::connect(&cfg.connection).await?;
        let channel = conn
            .create_channel()
            .await
            .map_err(|e| amqp_error("open channel", e))?;
        if cfg.ack_mode == AckMode::OnSinkConfirm {
            channel
                .basic_qos(cfg.effective_prefetch(), BasicQosOptions::default())
                .await
                .map_err(|e| amqp_error("set prefetch", e))?;
        }
        if cfg.declare_queue {
            channel
                .queue_declare(
                    cfg.queue.as_str().into(),
                    QueueDeclareOptions {
                        durable: cfg.queue_durable,
                        ..QueueDeclareOptions::default()
                    },
                    FieldTable::default(),
                )
                .await
                .map_err(|e| amqp_error(&format!("declare queue '{}'", cfg.queue), e))?;
        }
        for b in &cfg.bindings {
            if let Some(kind) = b.exchange_kind {
                declare_exchange(&channel, &b.exchange, kind).await?;
            }
            channel
                .queue_bind(
                    cfg.queue.as_str().into(),
                    b.exchange.as_str().into(),
                    b.routing_key.as_str().into(),
                    QueueBindOptions::default(),
                    FieldTable::default(),
                )
                .await
                .map_err(|e| {
                    amqp_error(
                        &format!("bind queue '{}' to exchange '{}'", cfg.queue, b.exchange),
                        e,
                    )
                })?;
        }
        let consumer = channel
            .basic_consume(
                cfg.queue.as_str().into(),
                cfg.consumer_tag.clone().unwrap_or_default().into(),
                BasicConsumeOptions {
                    no_ack: cfg.ack_mode == AckMode::Auto,
                    ..BasicConsumeOptions::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| amqp_error(&format!("consume queue '{}'", cfg.queue), e))?;
        Ok((conn, channel, consumer))
    }
}

/// Delivery metadata carried onto a record when `include_metadata` is set.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DeliveryMeta {
    pub exchange: String,
    pub routing_key: String,
    pub delivery_tag: u64,
    pub redelivered: bool,
    pub headers: Value,
    pub content_type: Option<String>,
    pub message_id: Option<String>,
    pub correlation_id: Option<String>,
    pub timestamp: Option<u64>,
}

impl DeliveryMeta {
    pub(crate) fn from_delivery(d: &Delivery) -> Self {
        let p = &d.properties;
        Self {
            exchange: d.exchange.as_str().to_string(),
            routing_key: d.routing_key.as_str().to_string(),
            delivery_tag: d.delivery_tag,
            redelivered: d.redelivered,
            headers: p
                .headers()
                .as_ref()
                .map(field_table_to_json)
                .unwrap_or_else(|| Value::Object(Default::default())),
            content_type: p.content_type().as_ref().map(|s| s.as_str().to_string()),
            message_id: p.message_id().as_ref().map(|s| s.as_str().to_string()),
            correlation_id: p.correlation_id().as_ref().map(|s| s.as_str().to_string()),
            timestamp: *p.timestamp(),
        }
    }
}

/// Assemble the emitted record: the decoded body alone, or wrapped with
/// delivery metadata. Pure.
pub(crate) fn build_record(payload: Value, meta: &DeliveryMeta, include_metadata: bool) -> Value {
    if !include_metadata {
        return payload;
    }
    json!({
        "data": payload,
        "exchange": meta.exchange,
        "routing_key": meta.routing_key,
        "delivery_tag": meta.delivery_tag,
        "redelivered": meta.redelivered,
        "headers": meta.headers,
        "content_type": meta.content_type,
        "message_id": meta.message_id,
        "correlation_id": meta.correlation_id,
        "timestamp": meta.timestamp,
    })
}

/// Deferred-ack bookkeeping. Delivery tags increase monotonically on a channel,
/// so a page is acknowledged with one `basic.ack(multiple)` on its highest
/// outstanding tag. Rejected (skipped) deliveries are never recorded: acking a
/// tag the broker no longer tracks would close the channel.
#[derive(Debug, Default)]
pub(crate) struct AckTracker {
    buffered: Option<u64>,
    sealed: Option<u64>,
}

impl AckTracker {
    /// Record an outstanding delivery in the page being buffered.
    pub(crate) fn record(&mut self, tag: u64) {
        self.buffered = Some(self.buffered.map_or(tag, |t| t.max(tag)));
    }

    /// The buffered page was yielded: its tags await durability.
    pub(crate) fn seal(&mut self) {
        if let Some(tag) = self.buffered.take() {
            self.sealed = Some(self.sealed.map_or(tag, |t| t.max(tag)));
        }
    }

    /// Take the tag to acknowledge now that the sealed page is durable.
    pub(crate) fn take_durable(&mut self) -> Option<u64> {
        self.sealed.take()
    }
}

/// The per-poll outcome of the drain loop.
enum Polled {
    Delivery(Box<Delivery>),
    Closed,
    Idle,
    Interrupted,
    Failed(FaucetError),
}

#[async_trait]
impl Source for RabbitMqSource {
    async fn fetch_with_context(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let mut pages = self.stream_pages(context, self.config.batch_size);
        let mut out = Vec::new();
        while let Some(page) = pages.next().await {
            out.extend(page?.records);
        }
        Ok(out)
    }

    /// Stream messages page-by-page. The trait-level `batch_size` is ignored in
    /// favour of the config field.
    ///
    /// Under `on_sink_confirm` every page carries a small bookmark
    /// (`{"queue", "consumed"}`) so the pipeline flushes the sink before asking
    /// for the next page — that flush is what makes the deferred ack safe. The
    /// bookmark is informational: the broker, not faucet, tracks the queue
    /// position, so the source is not resumable from it.
    fn stream_pages<'a>(
        &'a self,
        _context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let cfg = &self.config;
        let batch_size = cfg.batch_size;
        let page_chunk = if batch_size == 0 {
            usize::MAX
        } else {
            batch_size
        };
        let cap = if batch_size == 0 { 1024 } else { batch_size };
        let max_messages = cfg.max_messages.unwrap_or(usize::MAX);
        let idle = cfg.idle_timeout_secs.map(Duration::from_secs);
        let poll_fallback = Duration::from_millis(500);
        let deferred = cfg.ack_mode == AckMode::OnSinkConfirm;

        Box::pin(async_stream::try_stream! {
            let (conn, channel, mut consumer) = self.open().await?;
            let mut acks = AckTracker::default();
            let mut buffer: Vec<Value> = Vec::with_capacity(cap);
            let mut total = 0usize;
            let mut last_at = Instant::now();

            loop {
                if let Some(tag) = acks.take_durable() {
                    ack(&channel, tag).await?;
                }

                let (budget, deadline) = poll_budget(idle, last_at, poll_fallback);
                let polled = tokio::select! {
                    biased;
                    _ = tokio::signal::ctrl_c() => Polled::Interrupted,
                    next = tokio::time::timeout(budget, consumer.next()) => match next {
                        Ok(Some(Ok(delivery))) => Polled::Delivery(Box::new(delivery)),
                        Ok(Some(Err(e))) => Polled::Failed(amqp_error("consume", e)),
                        Ok(None) => Polled::Closed,
                        Err(_elapsed) => Polled::Idle,
                    }
                };

                let mut stop = false;
                match polled {
                    Polled::Delivery(delivery) => {
                        last_at = Instant::now();
                        total += 1;
                        if total >= max_messages {
                            stop = true;
                        }
                        match decode_payload(&delivery.data, cfg.value_format) {
                            Ok(payload) => {
                                let meta = DeliveryMeta::from_delivery(&delivery);
                                buffer.push(build_record(payload, &meta, cfg.include_metadata));
                                if deferred {
                                    acks.record(delivery.delivery_tag);
                                }
                            }
                            Err(e) => match cfg.on_decode_error {
                                OnDecodeError::Fail => Err(FaucetError::Source(format!(
                                    "{e} (queue '{}', delivery tag {})",
                                    cfg.queue, delivery.delivery_tag
                                )))?,
                                OnDecodeError::Skip => {
                                    tracing::warn!(
                                        error = %e,
                                        queue = %cfg.queue,
                                        delivery_tag = delivery.delivery_tag,
                                        "rabbitmq source: undecodable message rejected"
                                    );
                                    if deferred {
                                        channel
                                            .basic_reject(
                                                delivery.delivery_tag,
                                                BasicRejectOptions { requeue: false },
                                            )
                                            .await
                                            .map_err(|e| amqp_error("reject", e))?;
                                    }
                                }
                            },
                        }
                    }
                    Polled::Closed => Err(FaucetError::Source(format!(
                        "rabbitmq source: the broker cancelled the consumer on queue '{}' \
                         (queue deleted or connection lost); unacknowledged messages are requeued",
                        cfg.queue
                    )))?,
                    Polled::Failed(e) => Err(e)?,
                    Polled::Interrupted => {
                        tracing::info!("rabbitmq source: ctrl_c received, stopping");
                        stop = true;
                    }
                    Polled::Idle => {
                        if idle_expired(deadline) {
                            stop = true;
                        }
                    }
                }

                if !buffer.is_empty() && buffer.len() >= page_chunk {
                    let records = std::mem::replace(&mut buffer, Vec::with_capacity(cap));
                    acks.seal();
                    yield StreamPage { records, bookmark: page_bookmark(deferred, &cfg.queue, total) };
                }

                if stop {
                    break;
                }
            }

            if let Some(tag) = acks.take_durable() {
                ack(&channel, tag).await?;
            }
            if !buffer.is_empty() {
                acks.seal();
                yield StreamPage { records: buffer, bookmark: page_bookmark(deferred, &cfg.queue, total) };
                if let Some(tag) = acks.take_durable() {
                    ack(&channel, tag).await?;
                }
            }
            if let Err(e) = channel.close(200, "faucet run complete".into()).await {
                tracing::debug!(error = %e, "rabbitmq source: channel close failed");
            }
            if let Err(e) = conn.close(200, "faucet run complete".into()).await {
                tracing::debug!(error = %e, "rabbitmq source: connection close failed");
            }
            tracing::info!(messages = total, queue = %cfg.queue, "rabbitmq source: stream complete");
        })
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(RabbitMqSourceConfig)).unwrap_or(Value::Null)
    }

    fn connector_name(&self) -> &'static str {
        "rabbitmq"
    }

    fn dataset_uri(&self) -> String {
        format!(
            "{}?queue={}",
            self.config.connection.display_address(),
            self.config.queue
        )
    }

    /// Connect, then passively check the queue — no message is consumed.
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};
        let start = Instant::now();
        let connected = tokio::time::timeout(
            ctx.timeout,
            faucet_common_rabbitmq::connect(&self.config.connection),
        )
        .await;
        let conn = match connected {
            Ok(Ok(conn)) => conn,
            Ok(Err(e)) => {
                return Ok(CheckReport::single(Probe::fail_hint(
                    "connect",
                    start.elapsed(),
                    e.to_string(),
                    "verify the broker address, vhost, credentials and TLS settings",
                )));
            }
            Err(_) => {
                return Ok(CheckReport::single(Probe::fail_hint(
                    "connect",
                    start.elapsed(),
                    "connect timed out",
                    "no broker responded within the check timeout",
                )));
            }
        };
        let mut report = CheckReport::single(Probe::pass("connect", start.elapsed()));
        let queue_start = Instant::now();
        let queue_probe = match conn.create_channel().await {
            Err(e) => Probe::fail("queue", queue_start.elapsed(), e.to_string()),
            Ok(channel) => match channel
                .queue_declare(
                    self.config.queue.as_str().into(),
                    QueueDeclareOptions {
                        passive: true,
                        ..QueueDeclareOptions::default()
                    },
                    FieldTable::default(),
                )
                .await
            {
                Ok(_) => Probe::pass("queue", queue_start.elapsed()),
                Err(_) if self.config.declare_queue => Probe::skip(
                    "queue",
                    format!(
                        "queue '{}' does not exist yet; it is declared on the first run",
                        self.config.queue
                    ),
                ),
                Err(e) => Probe::fail_hint(
                    "queue",
                    queue_start.elapsed(),
                    e.to_string(),
                    "the queue must exist when `declare_queue: false`",
                ),
            },
        };
        report.probes.push(queue_probe);
        let _ = conn.close(200, "faucet check complete".into()).await;
        Ok(report)
    }
}

fn page_bookmark(deferred: bool, queue: &str, consumed: usize) -> Option<Value> {
    deferred.then(|| json!({ "queue": queue, "consumed": consumed }))
}

async fn ack(channel: &lapin::Channel, tag: u64) -> Result<(), FaucetError> {
    channel
        .basic_ack(tag, BasicAckOptions { multiple: true })
        .await
        .map_err(|e| amqp_error("acknowledge page (its messages will be redelivered)", e))
}

/// The poll timeout for this iteration and the idle deadline, if any. Without
/// an idle timeout, poll in short bursts so ctrl_c stays responsive.
fn poll_budget(
    idle: Option<Duration>,
    last_at: Instant,
    fallback: Duration,
) -> (Duration, Option<Instant>) {
    match idle {
        Some(t) => {
            let deadline = last_at + t;
            let budget = deadline
                .checked_duration_since(Instant::now())
                .unwrap_or(Duration::ZERO);
            (budget, Some(deadline))
        }
        None => (fallback, None),
    }
}

fn idle_expired(deadline: Option<Instant>) -> bool {
    deadline.is_some_and(|d| Instant::now() >= d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use lapin::BasicProperties;
    use lapin::types::AMQPValue;

    fn meta() -> DeliveryMeta {
        DeliveryMeta {
            exchange: "ex".into(),
            routing_key: "rk".into(),
            delivery_tag: 7,
            redelivered: true,
            headers: json!({"h": 1}),
            content_type: Some("application/json".into()),
            message_id: Some("m1".into()),
            correlation_id: None,
            timestamp: Some(1_700_000_000),
        }
    }

    #[test]
    fn record_without_metadata_is_the_payload() {
        let r = build_record(json!({"id": 1}), &meta(), false);
        assert_eq!(r, json!({"id": 1}));
    }

    #[test]
    fn record_with_metadata_wraps_payload() {
        let r = build_record(json!({"id": 1}), &meta(), true);
        assert_eq!(r["data"], json!({"id": 1}));
        assert_eq!(r["exchange"], "ex");
        assert_eq!(r["routing_key"], "rk");
        assert_eq!(r["delivery_tag"], 7);
        assert_eq!(r["redelivered"], true);
        assert_eq!(r["headers"], json!({"h": 1}));
        assert_eq!(r["content_type"], "application/json");
        assert_eq!(r["message_id"], "m1");
        assert_eq!(r["correlation_id"], Value::Null);
        assert_eq!(r["timestamp"], 1_700_000_000);
    }

    #[test]
    fn meta_from_delivery_reads_properties() {
        let mut d = Delivery::mock(3, "ex".into(), "rk".into(), false, b"{}".to_vec());
        let mut headers = FieldTable::default();
        headers.insert("tenant".into(), AMQPValue::LongString("acme".into()));
        d.properties = BasicProperties::default()
            .with_headers(headers)
            .with_content_type("text/plain".into())
            .with_message_id("mid".into())
            .with_correlation_id("cid".into())
            .with_timestamp(42);
        let m = DeliveryMeta::from_delivery(&d);
        assert_eq!(m.delivery_tag, 3);
        assert_eq!(m.exchange, "ex");
        assert_eq!(m.headers, json!({"tenant": "acme"}));
        assert_eq!(m.content_type.as_deref(), Some("text/plain"));
        assert_eq!(m.message_id.as_deref(), Some("mid"));
        assert_eq!(m.correlation_id.as_deref(), Some("cid"));
        assert_eq!(m.timestamp, Some(42));

        let bare = Delivery::mock(1, "".into(), "q".into(), true, Vec::new());
        let m = DeliveryMeta::from_delivery(&bare);
        assert_eq!(m.headers, json!({}));
        assert!(m.redelivered);
        assert!(m.content_type.is_none() && m.timestamp.is_none());
    }

    #[test]
    fn ack_tracker_acks_highest_tag_per_page() {
        let mut t = AckTracker::default();
        assert_eq!(t.take_durable(), None);
        t.record(1);
        t.record(3);
        t.record(2);
        assert_eq!(t.take_durable(), None, "unsealed page is not durable");
        t.seal();
        assert_eq!(t.take_durable(), Some(3));
        assert_eq!(t.take_durable(), None);
        t.seal();
        assert_eq!(t.take_durable(), None, "empty page seals nothing");
    }

    #[test]
    fn ack_tracker_merges_consecutive_seals() {
        let mut t = AckTracker::default();
        t.record(5);
        t.seal();
        t.record(9);
        t.seal();
        assert_eq!(t.take_durable(), Some(9));
    }

    #[test]
    fn bookmark_only_under_deferred_acks() {
        assert_eq!(
            page_bookmark(true, "q", 4),
            Some(json!({"queue": "q", "consumed": 4}))
        );
        assert_eq!(page_bookmark(false, "q", 4), None);
    }

    #[test]
    fn poll_budget_and_idle() {
        let (b, d) = poll_budget(None, Instant::now(), Duration::from_millis(500));
        assert_eq!(b, Duration::from_millis(500));
        assert!(d.is_none());
        let (_, d) = poll_budget(Some(Duration::from_secs(5)), Instant::now(), Duration::ZERO);
        assert!(d.is_some());
        assert!(!idle_expired(d));
        assert!(!idle_expired(None));
        let past = Instant::now() - Duration::from_secs(10);
        let (b, d) = poll_budget(Some(Duration::from_secs(1)), past, Duration::ZERO);
        assert_eq!(b, Duration::ZERO);
        assert!(idle_expired(d));
    }

    #[tokio::test]
    async fn new_validates() {
        let mut c = RabbitMqSourceConfig::new("q");
        c.idle_timeout_secs = None;
        assert!(RabbitMqSource::new(c).await.is_err());
    }

    #[tokio::test]
    async fn name_uri_schema() {
        let s = RabbitMqSource::new(RabbitMqSourceConfig::new("orders"))
            .await
            .unwrap();
        assert_eq!(s.connector_name(), "rabbitmq");
        assert_eq!(s.dataset_uri(), "amqp://127.0.0.1:5672/%2f?queue=orders");
        assert!(s.config_schema().is_object());
    }

    fn unreachable() -> RabbitMqSourceConfig {
        let mut c = RabbitMqSourceConfig::new("q");
        c.connection.port = Some(1);
        c.connection.connect_timeout_secs = 5;
        c.idle_timeout_secs = Some(1);
        c
    }

    #[tokio::test]
    async fn unreachable_broker_errors_on_first_poll() {
        let s = RabbitMqSource::new(unreachable()).await.unwrap();
        let ctx = HashMap::new();
        let mut pages = s.stream_pages(&ctx, 10);
        assert!(matches!(
            pages.next().await,
            Some(Err(FaucetError::Source(_)))
        ));
        drop(pages);
        assert!(s.fetch_with_context(&ctx).await.is_err());
    }

    #[tokio::test]
    async fn check_reports_connect_failure() {
        let s = RabbitMqSource::new(unreachable()).await.unwrap();
        let report = s
            .check(&faucet_core::check::CheckContext::default())
            .await
            .unwrap();
        assert_eq!(report.failed_count(), 1);
        assert_eq!(report.probes[0].name, "connect");
    }

    #[tokio::test]
    async fn check_reports_connect_timeout() {
        let mut c = unreachable();
        c.connection.port = None;
        c.connection.host = Some("10.255.255.1".into());
        let s = RabbitMqSource::new(c).await.unwrap();
        let ctx = faucet_core::check::CheckContext {
            timeout: Duration::from_millis(200),
        };
        let report = s.check(&ctx).await.unwrap();
        assert_eq!(report.failed_count(), 1);
    }
}
