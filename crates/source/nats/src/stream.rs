//! `NatsSource` — the NATS consumer implementation (the one module that does I/O).
//!
//! Two modes, selected by config:
//! - **Core NATS** — `client.subscribe(subject)` (optionally a queue group).
//!   Fire-and-forget (at-most-once) delivery: no bookmark, not resumable.
//! - **JetStream** — pull from a durable consumer bound to an existing stream.
//!   Each page's messages are acked *after* the page is yielded (i.e. after the
//!   pipeline has written the previous page to the sink), giving at-least-once
//!   delivery without claiming exactly-once.
//!
//! Both drain until `max_messages` or `idle_timeout_secs` fires, buffering up to
//! `batch_size` records per [`StreamPage`] so memory stays bounded.

use crate::config::{NatsSourceConfig, NatsValueFormat};
use async_trait::async_trait;
use faucet_core::lease::LeaseExtender;
use faucet_core::{FaucetError, Source, Stream, StreamPage};
use futures::StreamExt;
use serde_json::Value;
use std::collections::HashMap;
use std::pin::Pin;
use std::time::{Duration, Instant};
use tokio::sync::OnceCell;

/// A source that drains messages from a NATS subject (or a JetStream durable
/// consumer) and emits each payload as a JSON record.
///
/// The client is built lazily on the first fetch/stream (see
/// [`NatsSource::new`]), so an unreachable server fails on the first poll rather
/// than at construction time.
pub struct NatsSource {
    config: NatsSourceConfig,
    client: OnceCell<async_nats::Client>,
}

impl NatsSource {
    /// Create a new NATS source. Validates the config but does **not** connect —
    /// the client is built lazily on the first fetch/stream.
    pub async fn new(config: NatsSourceConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        Ok(Self {
            config,
            client: OnceCell::new(),
        })
    }

    /// Lazily build (once) and return the shared NATS client.
    async fn client(&self) -> Result<async_nats::Client, FaucetError> {
        self.client
            .get_or_try_init(|| faucet_common_nats::connect(&self.config.connection))
            .await
            .cloned()
    }
}

/// Decode a raw NATS payload per `format`. Never lossy: a payload the format
/// cannot represent is an error naming the subject (#789 MSG-39).
fn payload_to_value(
    payload: &[u8],
    format: NatsValueFormat,
    subject: &str,
) -> Result<Value, FaucetError> {
    let not_utf8 = |e: std::str::Utf8Error| {
        FaucetError::Source(format!(
            "nats: a message on '{subject}' is not valid UTF-8 ({e}); set `value_format: bytes` \
             to receive binary payloads base64-encoded"
        ))
    };
    match format {
        NatsValueFormat::Auto => match serde_json::from_slice::<Value>(payload) {
            Ok(v) => Ok(v),
            Err(_) => std::str::from_utf8(payload)
                .map(|s| Value::String(s.to_string()))
                .map_err(not_utf8),
        },
        NatsValueFormat::Json => serde_json::from_slice(payload).map_err(|e| {
            FaucetError::Source(format!(
                "nats: a message on '{subject}' is not valid JSON: {e}"
            ))
        }),
        NatsValueFormat::String => std::str::from_utf8(payload)
            .map(|s| Value::String(s.to_string()))
            .map_err(not_utf8),
        NatsValueFormat::Bytes => {
            use base64::Engine as _;
            Ok(Value::String(
                base64::engine::general_purpose::STANDARD.encode(payload),
            ))
        }
    }
}

/// The per-message poll outcome fed to the shared drain loop.
enum Polled {
    /// A decoded record (JetStream carries the message for a deferred ack).
    Record(Value),
    /// The underlying subscription/stream closed.
    Closed,
    /// The poll budget elapsed with no message.
    Idle,
}

#[async_trait]
impl Source for NatsSource {
    async fn fetch_with_context(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        // Reuse the streaming path so there is a single drain implementation.
        let mut pages = self.stream_pages(context, self.config.batch_size);
        let mut out = Vec::new();
        while let Some(page) = pages.next().await {
            out.extend(page?.records);
        }
        Ok(out)
    }

    /// Stream messages page-by-page. The trait-level `batch_size` argument is
    /// ignored in favour of the config field (the user-facing knob).
    ///
    /// No page carries a bookmark: core NATS is fire-and-forget and the
    /// JetStream path acks rather than persisting a resumable position, so this
    /// source is not resumable / exactly-once (the defaults hold).
    fn stream_pages<'a>(
        &'a self,
        _context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let batch_size = self.config.batch_size;
        let page_chunk = if batch_size == 0 {
            usize::MAX
        } else {
            batch_size
        };
        let cap = if batch_size == 0 { 1024 } else { batch_size };
        let max_messages = self.config.max_messages.unwrap_or(usize::MAX);
        let idle = self.config.idle_timeout_secs.map(Duration::from_secs);
        let format = self.config.value_format;
        let poll_fallback = Duration::from_millis(500);

        let with_meta = self.config.include_metadata;

        Box::pin(async_stream::try_stream! {
            let client = self.client().await?;

            if self.config.is_jetstream() {
                // ── JetStream pull-consumer mode ────────────────────────────
                let stream_name = self
                    .config
                    .jetstream_stream
                    .as_deref()
                    .expect("validated: jetstream_stream is Some in JetStream mode");
                let consumer_name = self
                    .config
                    .jetstream_consumer
                    .as_deref()
                    .expect("validated: jetstream_consumer is Some in JetStream mode");

                let js = async_nats::jetstream::new(client.clone());
                let js_stream = js
                    .get_stream(stream_name)
                    .await
                    .map_err(|e| FaucetError::Source(format!("nats jetstream get_stream '{stream_name}': {e}")))?;
                let consumer: async_nats::jetstream::consumer::PullConsumer = js_stream
                    .get_consumer(consumer_name)
                    .await
                    .map_err(|e| FaucetError::Source(format!("nats jetstream get_consumer '{consumer_name}': {e}")))?;
                // Acks happen only after a page is written, so a page larger
                // than the consumer's `max_ack_pending` would stall: the server
                // stops delivering and later redelivers (#789 MSG-47).
                let page_cap = page_capacity(page_chunk, consumer.cached_info().config.max_ack_pending);

                let mut buffer: Vec<Value> = Vec::with_capacity(cap);
                // Messages for the page currently being buffered, acked after
                // the page is yielded (i.e. once the pipeline has written it).
                let mut page_msgs: Vec<async_nats::jetstream::Message> = Vec::with_capacity(cap);
                let mut to_ack: Vec<async_nats::jetstream::Message> = Vec::new();
                let mut total = 0usize;
                let mut last_at = Instant::now();
                let progress = self.config.progress_interval_secs;
                let lease = (progress > 0).then(|| {
                    LeaseExtender::spawn(Duration::from_secs(progress), mark_in_progress)
                });
                let hold = |page: &[async_nats::jetstream::Message], yielded: &[async_nats::jetstream::Message]| {
                    if let Some(l) = &lease {
                        l.hold(page.iter().chain(yielded).cloned().collect());
                    }
                };

                loop {
                    if !to_ack.is_empty() {
                        ack_all(&client, std::mem::take(&mut to_ack)).await;
                        hold(&page_msgs, &to_ack);
                    }

                    // Pull exactly what the page and the run still need — never
                    // a prefetch past `max_messages` that would sit leased,
                    // unacked, until `ack_wait` (#789 MSG-45).
                    let want = pull_size(page_cap, buffer.len(), max_messages, total);
                    let (budget, deadline) = poll_budget(idle, last_at, poll_fallback);
                    let mut batch = Box::pin(
                        consumer
                            .batch()
                            .max_messages(want)
                            .expires(budget.max(Duration::from_millis(100)))
                            .messages()
                            .await
                            .map_err(|e| FaucetError::Source(format!("nats jetstream pull: {e}")))?,
                    );
                    let mut got = 0usize;
                    while let Some(item) = batch.next().await {
                        let msg = item.map_err(|e| FaucetError::Source(format!("nats jetstream recv: {e}")))?;
                        let mut record = payload_to_value(&msg.payload, format, &msg.subject)?;
                        if with_meta {
                            let seq = msg.info().ok().map(|i| i.stream_sequence);
                            record = with_metadata(record, &msg.subject, seq, message_id(msg.headers.as_ref()));
                        }
                        page_msgs.push(msg);
                        hold(&page_msgs, &to_ack);
                        buffer.push(record);
                        total += 1;
                        got += 1;
                    }
                    if got > 0 {
                        last_at = Instant::now();
                    }
                    let stop = total >= max_messages || (got == 0 && idle_expired(deadline));

                    if !buffer.is_empty() && buffer.len() >= page_cap {
                        let records = std::mem::replace(&mut buffer, Vec::with_capacity(cap));
                        to_ack = std::mem::take(&mut page_msgs);
                        // The bookmark makes the pipeline flush the sink before
                        // it resumes us — and resuming is when this page is acked.
                        yield StreamPage {
                            records,
                            bookmark: Some(page_bookmark(stream_name, consumer_name, total)),
                        };
                    }

                    if stop {
                        break;
                    }
                }

                // Flush any acks pending from the last full page, then the
                // trailing partial page (and its acks).
                ack_all(&client, std::mem::take(&mut to_ack)).await;
                if !buffer.is_empty() {
                    yield StreamPage {
                        records: buffer,
                        bookmark: Some(page_bookmark(stream_name, consumer_name, total)),
                    };
                    ack_all(&client, std::mem::take(&mut page_msgs)).await;
                }
                drop(lease);
                tracing::info!(messages = total, "nats source: jetstream stream complete");
            } else {
                // ── Core NATS subscription mode ─────────────────────────────
                let mut sub: Pin<Box<async_nats::Subscriber>> = Box::pin(match &self.config.queue_group {
                    Some(group) => client
                        .queue_subscribe(self.config.subject.clone(), group.clone())
                        .await
                        .map_err(|e| FaucetError::Source(format!("nats queue_subscribe '{}': {e}", self.config.subject)))?,
                    None => client
                        .subscribe(self.config.subject.clone())
                        .await
                        .map_err(|e| FaucetError::Source(format!("nats subscribe '{}': {e}", self.config.subject)))?,
                });

                let mut buffer: Vec<Value> = Vec::with_capacity(cap);
                let mut total = 0usize;
                let mut last_at = Instant::now();

                // Shutdown is the pipeline's cancel token, not a process-wide
                // signal handler a library must not install (#789 MSG-89).
                loop {
                    let (budget, deadline) = poll_budget(idle, last_at, poll_fallback);
                    let polled = match tokio::time::timeout(budget, sub.next()).await {
                        Ok(Some(msg)) => {
                            last_at = Instant::now();
                            let mut record = payload_to_value(&msg.payload, format, &msg.subject)?;
                            if with_meta {
                                record = with_metadata(record, &msg.subject, None, message_id(msg.headers.as_ref()));
                            }
                            Polled::Record(record)
                        }
                        Ok(None) => Polled::Closed,
                        Err(_elapsed) => Polled::Idle,
                    };

                    let mut stop = false;
                    match polled {
                        Polled::Record(record) => {
                            buffer.push(record);
                            total += 1;
                            if total >= max_messages {
                                stop = true;
                            }
                        }
                        Polled::Closed => stop = true,
                        Polled::Idle => {
                            if idle_expired(deadline) {
                                stop = true;
                            }
                        }
                    }

                    if !buffer.is_empty() && buffer.len() >= page_chunk {
                        let records = std::mem::replace(&mut buffer, Vec::with_capacity(cap));
                        yield StreamPage { records, bookmark: None };
                    }

                    if stop {
                        break;
                    }
                }

                if !buffer.is_empty() {
                    yield StreamPage { records: buffer, bookmark: None };
                }
                tracing::info!(messages = total, "nats source: core stream complete");
            }
        })
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(NatsSourceConfig)).unwrap_or(Value::Null)
    }

    fn consumes_destructively(&self) -> bool {
        self.config.is_jetstream()
    }

    fn connector_name(&self) -> &'static str {
        "nats"
    }

    fn dataset_uri(&self) -> String {
        faucet_common_nats::dataset_uri(&self.config.connection.servers, &self.config.subject)
    }
}

/// Compute the poll timeout for this iteration and the idle deadline (if any).
/// With no idle timeout configured we poll in short bursts so `ctrl_c` and
/// `max_messages` termination stay responsive.
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

/// Whether the idle deadline (if set) has passed.
fn idle_expired(deadline: Option<Instant>) -> bool {
    deadline.is_some_and(|d| Instant::now() >= d)
}

/// The informational bookmark every JetStream page carries: the consumer, not
/// faucet, owns the position; the bookmark exists so the pipeline flushes the
/// sink before resuming the generator, which is when the page is acked
/// (MSG-07). Pure.
pub(crate) fn page_bookmark(stream: &str, consumer: &str, consumed: usize) -> Value {
    serde_json::json!({ "stream": stream, "consumer": consumer, "consumed": consumed })
}

/// Send an in-progress ack for every held message, resetting its `ack_wait`.
/// Best-effort.
async fn mark_in_progress(messages: Vec<async_nats::jetstream::Message>) {
    for msg in messages {
        if let Err(e) = msg.ack_with(async_nats::jetstream::AckKind::Progress).await {
            tracing::warn!(error = %e, "nats source: in-progress ack failed");
        }
    }
}

/// Ack a page's JetStream messages best-effort — a failed ack triggers at most
/// a redelivery (at-least-once), never data loss, so it is logged not fatal.
/// `ack` only queues the publish, so the client is flushed: the final page's
/// acks would otherwise be lost when the run exits right after (#789 MSG-81).
async fn ack_all(client: &async_nats::Client, messages: Vec<async_nats::jetstream::Message>) {
    if messages.is_empty() {
        return;
    }
    for msg in messages {
        if let Err(e) = msg.ack().await {
            tracing::warn!(error = %e, "nats source: jetstream ack failed (message may be redelivered)");
        }
    }
    if let Err(e) = client.flush().await {
        tracing::warn!(error = %e, "nats source: flushing acks failed (messages may be redelivered)");
    }
}

/// The page size that never exceeds the consumer's `max_ack_pending`
/// (`<= 0` = unlimited).
fn page_capacity(page_chunk: usize, max_ack_pending: i64) -> usize {
    if max_ack_pending > 0 {
        page_chunk.min(max_ack_pending as usize)
    } else {
        page_chunk
    }
}

/// How many messages one pull requests: what the page and the run still need,
/// capped at the server's per-request batch limit.
fn pull_size(page_cap: usize, buffered: usize, max_messages: usize, total: usize) -> usize {
    page_cap
        .saturating_sub(buffered)
        .min(max_messages.saturating_sub(total))
        .clamp(1, 10_000)
}

/// The `Nats-Msg-Id` header, the publisher's deduplication id.
fn message_id(headers: Option<&async_nats::HeaderMap>) -> Option<String> {
    headers
        .and_then(|h| h.get("Nats-Msg-Id"))
        .map(|v| v.as_str().to_string())
}

/// `{ subject, sequence, message_id, payload }` (`include_metadata: true`).
fn with_metadata(
    payload: Value,
    subject: &str,
    sequence: Option<u64>,
    message_id: Option<String>,
) -> Value {
    serde_json::json!({
        "subject": subject,
        "sequence": sequence,
        "message_id": message_id,
        "payload": payload,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pulls_never_exceed_the_page_the_run_or_max_ack_pending() {
        assert_eq!(page_capacity(1000, 200), 200);
        assert_eq!(page_capacity(1000, -1), 1000);
        assert_eq!(page_capacity(50, 200), 50);
        assert_eq!(pull_size(200, 150, 10_000, 0), 50);
        assert_eq!(pull_size(200, 0, 30, 25), 5);
        assert_eq!(pull_size(usize::MAX, 0, usize::MAX, 0), 10_000);
        assert_eq!(pull_size(10, 10, 100, 0), 1);
    }

    #[test]
    fn metadata_wraps_the_payload() {
        let mut h = async_nats::HeaderMap::new();
        h.insert("Nats-Msg-Id", "m-1");
        assert_eq!(message_id(Some(&h)).as_deref(), Some("m-1"));
        assert_eq!(message_id(None), None);
        assert_eq!(
            with_metadata(
                serde_json::json!({"a": 1}),
                "s.x",
                Some(7),
                Some("m-1".into())
            ),
            serde_json::json!({"subject": "s.x", "sequence": 7, "message_id": "m-1", "payload": {"a": 1}})
        );
    }

    #[test]
    fn every_jetstream_page_carries_an_informational_bookmark() {
        assert_eq!(
            page_bookmark("ORDERS", "faucet", 3),
            serde_json::json!({"stream": "ORDERS", "consumer": "faucet", "consumed": 3})
        );
    }

    fn decode(payload: &[u8], format: NatsValueFormat) -> Result<Value, FaucetError> {
        payload_to_value(payload, format, "s.x")
    }

    #[test]
    fn payload_json_passthrough() {
        let v = decode(br#"{"id":1,"name":"a"}"#, NatsValueFormat::Auto).unwrap();
        assert_eq!(v["id"], 1);
        assert_eq!(v["name"], "a");
    }

    #[test]
    fn payload_non_json_becomes_string() {
        let v = decode(b"hello world", NatsValueFormat::Auto).unwrap();
        assert_eq!(v, Value::String("hello world".into()));
    }

    #[test]
    fn binary_payloads_are_never_mangled() {
        // #789 MSG-39: invalid UTF-8 used to become U+FFFD text.
        let bin = [0xff, 0xfe, 0x00];
        let err = decode(&bin, NatsValueFormat::Auto).unwrap_err().to_string();
        assert!(
            err.contains("value_format: bytes") && err.contains("s.x"),
            "{err}"
        );
        assert!(decode(&bin, NatsValueFormat::String).is_err());
        assert_eq!(
            decode(&bin, NatsValueFormat::Bytes).unwrap(),
            Value::String("//4A".into())
        );
        assert_eq!(
            decode(b"{\"a\":1}", NatsValueFormat::String).unwrap(),
            Value::String("{\"a\":1}".into())
        );
        assert!(decode(b"plain", NatsValueFormat::Json).is_err());
        assert_eq!(
            decode(b"[1]", NatsValueFormat::Json).unwrap(),
            serde_json::json!([1])
        );
    }

    #[test]
    fn poll_budget_none_uses_fallback() {
        let (budget, deadline) = poll_budget(None, Instant::now(), Duration::from_millis(500));
        assert_eq!(budget, Duration::from_millis(500));
        assert!(deadline.is_none());
    }

    #[test]
    fn poll_budget_idle_sets_deadline() {
        let (_budget, deadline) = poll_budget(
            Some(Duration::from_secs(5)),
            Instant::now(),
            Duration::from_millis(500),
        );
        assert!(deadline.is_some());
    }

    #[test]
    fn idle_expired_true_when_past() {
        let past = Instant::now() - Duration::from_secs(1);
        assert!(idle_expired(Some(past)));
    }

    #[test]
    fn idle_expired_false_when_none() {
        assert!(!idle_expired(None));
    }

    #[tokio::test]
    async fn new_validates_config() {
        let mut cfg = NatsSourceConfig::new("x");
        cfg.idle_timeout_secs = None;
        cfg.max_messages = None;
        assert!(NatsSource::new(cfg).await.is_err());
    }

    #[tokio::test]
    async fn connector_name_and_uri() {
        let source = NatsSource::new(NatsSourceConfig::new("events.>"))
            .await
            .unwrap();
        assert_eq!(source.connector_name(), "nats");
        assert!(source.dataset_uri().contains("subject=events.>"));
    }

    #[tokio::test]
    async fn unreachable_server_errors_on_first_poll() {
        let mut cfg = NatsSourceConfig::new("events.>");
        cfg.connection.servers = vec!["nats://127.0.0.1:1".into()];
        cfg.idle_timeout_secs = Some(1);
        let source = NatsSource::new(cfg)
            .await
            .expect("lazy construction succeeds");
        // First poll connects and must surface a typed error, not panic.
        let ctx = HashMap::new();
        let mut pages = source.stream_pages(&ctx, 10);
        let first = pages.next().await;
        assert!(matches!(first, Some(Err(FaucetError::Custom(_)))));
    }
}
