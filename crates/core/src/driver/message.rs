//! The messaging family (RFC 0014): produce with confirmations, transactions
//! for exactly-once, consume with acknowledgements.
//!
//! A [`MessageDriver`] encodes one record into a message and publishes a
//! group of messages; [`MessageSinkDriver`] drives everything around that —
//! grouping, the per-group deadline, which rows a timeout or a cancel leaves
//! unconfirmed, per-row outcomes, and (with [`MessageTransactions`]) the
//! exactly-once commit. A [`MessageConsumer`] receives and acknowledges;
//! [`MessageSourceDriver`] acknowledges a page only once the pipeline has
//! written it.

use super::sink::{CommitTokenDriver, PageWrite, RowPolicy, SinkDriver};
use super::source::{PageStream, SourceDriver};
use crate::accounting::WriteReport;
use crate::check::{CheckContext, CheckReport, Probe};
use crate::dlq::BatchAtomicity;
use crate::error::FaucetError;
use crate::io::{CallContext, CallSlot};
use crate::pipeline::StreamPage;
use crate::traits::RowOutcome;
use async_trait::async_trait;
use futures::StreamExt;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

/// One message to publish.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct OutboundMessage {
    /// Where it goes: a subject, topic, queue or routing key.
    pub destination: String,
    /// The partition / ordering key, when the backend has one.
    pub key: Option<Vec<u8>>,
    /// The body.
    pub payload: Vec<u8>,
    /// Headers, in order.
    pub headers: Vec<(String, String)>,
}

impl OutboundMessage {
    /// A message with a body and no key or headers.
    pub fn new(destination: impl Into<String>, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            destination: destination.into(),
            key: None,
            payload: payload.into(),
            headers: Vec::new(),
        }
    }

    /// The same message with an ordering key.
    pub fn with_key(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// The same message with one more header.
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }
}

/// A producer's primitives (RFC 0014). Wrap it in a [`MessageSinkDriver`],
/// then a [`DriverSink`](super::DriverSink).
#[async_trait]
pub trait MessageDriver: Send + Sync {
    /// The connector's name.
    fn connector_name(&self) -> &'static str;

    /// The JSON Schema of the connector's config.
    fn config_schema(&self) -> Value {
        serde_json::json!({"type": "object", "properties": {}})
    }

    /// The destination, for lineage (credential-free).
    fn dataset_uri(&self) -> String {
        format!("{}://unknown", self.connector_name())
    }

    /// The connector's own call defaults.
    fn call_context(&self) -> CallContext {
        CallContext::new().without_timeout()
    }

    /// A non-mutating preflight probe.
    async fn check(
        &self,
        calls: &CallSlot,
        ctx: &CheckContext,
    ) -> Result<CheckReport, FaucetError> {
        let _ = (calls, ctx);
        Ok(CheckReport::not_implemented())
    }

    /// What a failed write leaves behind.
    fn batch_atomicity(&self) -> BatchAtomicity {
        BatchAtomicity::BestEffort
    }

    /// The message for `record`. An error fails that row alone.
    fn encode(&self, record: &Value) -> Result<OutboundMessage, FaucetError>;

    /// Records per publish group; `0` publishes the whole page as one.
    fn group_size(&self) -> usize {
        0
    }

    /// How long one group may take. On expiry its unconfirmed rows, and
    /// every row after them, fail with a retriable timeout. `None`: only the
    /// run's call deadline applies.
    fn group_timeout(&self) -> Option<Duration> {
        None
    }

    /// Open the connection, once; called before the first group of every
    /// page and before a flush. Default: nothing to open.
    async fn connect(&self, calls: &CallSlot) -> Result<(), FaucetError> {
        let _ = calls;
        Ok(())
    }

    /// Publish a group, returning one outcome per message (`Ok` once the
    /// backend confirmed it). An `Err` is a transport failure of the whole
    /// page.
    async fn publish(
        &self,
        calls: &CallSlot,
        group: Vec<OutboundMessage>,
    ) -> Result<Vec<RowOutcome>, FaucetError>;

    /// Make published messages durable (wait for the client's buffer).
    async fn flush(&self, calls: &CallSlot) -> Result<(), FaucetError> {
        let _ = calls;
        Ok(())
    }

    /// The transaction primitives, for exactly-once.
    fn transactions(&self) -> Option<&dyn MessageTransactions> {
        None
    }

    /// Whether the adapter runs each [`publish`](Self::publish) and
    /// [`flush`](Self::flush) as one backend call (op `publish` / `flush`,
    /// bounded by [`group_timeout`](Self::group_timeout) and the run's call
    /// context). Return `false` for a driver whose `publish` makes its own
    /// calls through the slot — its own op labels, request chunking, retries
    /// and bounds; the adapter then awaits it directly. Default: `true`.
    fn publish_is_one_call(&self) -> bool {
        true
    }

    /// The error a whole-page write (the plain and reported paths) fails
    /// with when rows of `offered` failed, given `(index, error)` for each
    /// in index order. `None` (the default): the first failed row's own
    /// error.
    fn page_error(&self, offered: usize, failed: &[(usize, FaucetError)]) -> Option<FaucetError> {
        let _ = (offered, failed);
        None
    }
}

/// A producer's transaction primitives. With them, [`MessageSinkDriver`]
/// commits a page's messages and its commit token in one transaction.
#[async_trait]
pub trait MessageTransactions: Send + Sync {
    /// Start a transaction for `scope`.
    async fn begin(&self, calls: &CallSlot, scope: &str) -> Result<(), FaucetError>;

    /// Record `token` for `scope` inside the open transaction and commit
    /// everything published since [`begin`](Self::begin).
    async fn commit(&self, calls: &CallSlot, scope: &str, token: &str) -> Result<(), FaucetError>;

    /// Abandon the open transaction; nothing published in it becomes visible.
    async fn abort(&self, calls: &CallSlot, scope: &str);

    /// The last token committed for `scope`.
    async fn last_token(
        &self,
        calls: &CallSlot,
        scope: &str,
    ) -> Result<Option<String>, FaucetError>;
}

/// A [`MessageDriver`] as a [`SinkDriver`].
pub struct MessageSinkDriver<D> {
    driver: D,
}

impl<D: MessageDriver> MessageSinkDriver<D> {
    /// The sink driver over `driver`.
    pub fn new(driver: D) -> Self {
        Self { driver }
    }

    /// The message driver.
    pub fn driver(&self) -> &D {
        &self.driver
    }

    /// The message driver, mutably.
    pub fn driver_mut(&mut self) -> &mut D {
        &mut self.driver
    }

    fn timeout(&self, what: &str, after: Duration) -> FaucetError {
        FaucetError::Timeout {
            operation: format!("{} {what}", self.driver.connector_name()),
            after,
        }
    }

    /// Run `fut` as one backend call named `what`, bounded by the group
    /// timeout. The outer error is the call's own (its timeout, the run's
    /// deadline or cancel); the inner one is the driver's.
    async fn bounded<T: Send>(
        &self,
        calls: &CallSlot,
        what: &'static str,
        fut: impl std::future::Future<Output = Result<T, FaucetError>> + Send,
    ) -> Result<Result<T, FaucetError>, FaucetError> {
        let timeout = self.driver.group_timeout();
        if !self.driver.publish_is_one_call() {
            return match timeout {
                Some(after) => tokio::time::timeout(after, fut)
                    .await
                    .map_err(|_| self.timeout(what, after)),
                None => Ok(fut.await),
            };
        }
        calls
            .call_once(what, async {
                match timeout {
                    Some(after) => tokio::time::timeout(after, fut)
                        .await
                        .map_err(|_| self.timeout(what, after)),
                    None => Ok(fut.await),
                }
            })
            .await
    }

    /// Publish every record in groups; one outcome per record.
    async fn publish_all(
        &self,
        calls: &CallSlot,
        rows: &[Value],
    ) -> Result<Vec<RowOutcome>, FaucetError> {
        let mut outcomes: Vec<RowOutcome> = rows.iter().map(|_| Ok(())).collect();
        if rows.is_empty() {
            return Ok(outcomes);
        }
        self.driver.connect(calls).await?;
        let size = match self.driver.group_size() {
            0 => rows.len(),
            n => n,
        };
        for (g, chunk) in rows.chunks(size).enumerate() {
            let base = g * size;
            let mut positions = Vec::with_capacity(chunk.len());
            let mut group = Vec::with_capacity(chunk.len());
            for (i, record) in chunk.iter().enumerate() {
                match self.driver.encode(record) {
                    Ok(m) => {
                        positions.push(base + i);
                        group.push(m);
                    }
                    Err(e) => outcomes[base + i] = Err(e),
                }
            }
            if group.is_empty() {
                continue;
            }
            let sent = group.len();
            match self
                .bounded(calls, "publish", self.driver.publish(calls, group))
                .await
            {
                Ok(Ok(results)) if results.len() == sent => {
                    for (pos, r) in positions.into_iter().zip(results) {
                        outcomes[pos] = r;
                    }
                }
                Ok(Ok(results)) => {
                    return Err(FaucetError::Sink(format!(
                        "{}: {} outcomes for a group of {sent} messages",
                        self.driver.connector_name(),
                        results.len()
                    )));
                }
                Ok(Err(e)) if g == 0 => return Err(e),
                Ok(Err(e)) => {
                    let name = self.driver.connector_name();
                    fail_open(&mut outcomes[base..], || cut(name, &e));
                    return Ok(outcomes);
                }
                Err(FaucetError::Timeout { after, .. }) => {
                    let after = self.driver.group_timeout().unwrap_or(after);
                    fail_open(&mut outcomes[base..], || self.timeout("publish", after));
                    return Ok(outcomes);
                }
                Err(cause) => {
                    let name = self.driver.connector_name();
                    fail_open(&mut outcomes[base..], || cut(name, &cause));
                    return Ok(outcomes);
                }
            }
        }
        Ok(outcomes)
    }
}

/// The error a row gets when its group's call was cut short by `cause` (a
/// cancel, or the call itself failing).
fn cut(connector: &str, cause: &FaucetError) -> FaucetError {
    match cause {
        FaucetError::Cancelled { operation } => FaucetError::Cancelled {
            operation: operation.clone(),
        },
        other => FaucetError::Sink(format!("{connector} publish: {other}")),
    }
}

/// Fail every row of `outcomes` not already decided.
fn fail_open(outcomes: &mut [RowOutcome], err: impl Fn() -> FaucetError) {
    for o in outcomes.iter_mut().filter(|o| o.is_ok()) {
        *o = Err(err());
    }
}

#[async_trait]
impl<D: MessageDriver> SinkDriver for MessageSinkDriver<D> {
    fn connector_name(&self) -> &'static str {
        self.driver.connector_name()
    }

    fn config_schema(&self) -> Value {
        self.driver.config_schema()
    }

    fn dataset_uri(&self) -> String {
        self.driver.dataset_uri()
    }

    fn call_context(&self) -> CallContext {
        self.driver.call_context()
    }

    async fn check(
        &self,
        calls: &CallSlot,
        ctx: &CheckContext,
    ) -> Result<CheckReport, FaucetError> {
        self.driver.check(calls, ctx).await
    }

    fn batch_atomicity(&self) -> BatchAtomicity {
        self.driver.batch_atomicity()
    }

    async fn write(
        &self,
        calls: &CallSlot,
        rows: &[Value],
        policy: RowPolicy,
    ) -> Result<PageWrite, FaucetError> {
        let write = PageWrite::from_outcomes(self.publish_all(calls, rows).await?);
        if policy == RowPolicy::WholePage
            && !write.failed.is_empty()
            && let Some(e) = self.driver.page_error(rows.len(), &write.failed)
        {
            return Err(e);
        }
        Ok(write)
    }

    async fn flush(&self, calls: &CallSlot) -> Result<(), FaucetError> {
        self.driver.connect(calls).await?;
        self.bounded(calls, "flush", self.driver.flush(calls))
            .await?
    }

    fn commit_tokens(&self) -> Option<&dyn CommitTokenDriver> {
        self.driver
            .transactions()
            .map(|_| self as &dyn CommitTokenDriver)
    }
}

#[async_trait]
impl<D: MessageDriver> CommitTokenDriver for MessageSinkDriver<D> {
    async fn last_token(
        &self,
        calls: &CallSlot,
        scope: &str,
    ) -> Result<Option<String>, FaucetError> {
        match self.driver.transactions() {
            Some(t) => t.last_token(calls, scope).await,
            None => Ok(None),
        }
    }

    async fn write_with_token(
        &self,
        calls: &CallSlot,
        rows: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<WriteReport, FaucetError> {
        let tx = self.driver.transactions().ok_or_else(|| {
            FaucetError::Sink(format!(
                "{}: exactly-once needs a transactional producer",
                self.driver.connector_name()
            ))
        })?;
        tx.begin(calls, scope).await?;
        let outcomes = match self.publish_all(calls, rows).await {
            Ok(o) => o,
            Err(e) => {
                tx.abort(calls, scope).await;
                return Err(e);
            }
        };
        if let Some(e) = outcomes.into_iter().find_map(Result::err) {
            tx.abort(calls, scope).await;
            return Err(e);
        }
        if let Err(e) = tx.commit(calls, scope, token).await {
            tx.abort(calls, scope).await;
            return Err(e);
        }
        Ok(WriteReport::accepted(rows.len()))
    }
}

/// An opaque handle a consumer acknowledges a message by.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct AckToken(pub String);

/// One received message.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct InboundMessage {
    /// Where it came from: a subject, topic or queue.
    pub source: String,
    /// The body.
    pub payload: Vec<u8>,
    /// How to acknowledge it.
    pub ack: AckToken,
}

impl InboundMessage {
    /// A received message.
    pub fn new(source: impl Into<String>, payload: impl Into<Vec<u8>>, ack: AckToken) -> Self {
        Self {
            source: source.into(),
            payload: payload.into(),
            ack,
        }
    }
}

/// A consumer's primitives (RFC 0014). Wrap it in a [`MessageSourceDriver`],
/// then a [`DriverSource`](super::DriverSource).
#[async_trait]
pub trait MessageConsumer: Send + Sync {
    /// The connector's name.
    fn connector_name(&self) -> &'static str;

    /// The JSON Schema of the connector's config.
    fn config_schema(&self) -> Value {
        serde_json::json!({"type": "object", "properties": {}})
    }

    /// The source, for lineage (credential-free).
    fn dataset_uri(&self) -> String {
        format!("{}://unknown", self.connector_name())
    }

    /// The connector's own call defaults.
    fn call_context(&self) -> CallContext {
        CallContext::new()
    }

    /// Receive up to `max` messages, waiting at most the consumer's own idle
    /// wait. An empty result means nothing arrived in that wait.
    async fn receive(
        &self,
        calls: &CallSlot,
        max: usize,
    ) -> Result<Vec<InboundMessage>, FaucetError>;

    /// Acknowledge messages so the broker does not redeliver them.
    async fn ack(&self, calls: &CallSlot, tokens: Vec<AckToken>) -> Result<(), FaucetError>;

    /// The record for a received message.
    fn decode(&self, message: &InboundMessage) -> Result<Value, FaucetError>;

    /// Stop after this many messages; `None`: only idleness stops the read.
    fn max_messages(&self) -> Option<usize> {
        None
    }

    /// Consecutive empty receives that end the read.
    fn idle_receives_to_stop(&self) -> u32 {
        1
    }

    /// A preflight probe that consumes nothing (connect, look up the queue).
    /// `None` (the default): the probe is skipped, because reading a page
    /// would take messages off the queue.
    async fn check(
        &self,
        calls: &CallSlot,
        ctx: &CheckContext,
    ) -> Result<Option<CheckReport>, FaucetError> {
        let _ = (calls, ctx);
        Ok(None)
    }
}

/// A [`MessageConsumer`] as a [`SourceDriver`].
///
/// Each page carries a bookmark, so the pipeline flushes the sink after
/// writing it; the page's messages are acknowledged when the next page is
/// requested (or the stream is polled to its end) — that is, only once the
/// pipeline has written and flushed them. A run that fails before then
/// acknowledges nothing it did not write, and the broker redelivers it.
pub struct MessageSourceDriver<C> {
    consumer: C,
    pending: Mutex<Vec<AckToken>>,
}

impl<C: MessageConsumer> MessageSourceDriver<C> {
    /// The source driver over `consumer`.
    pub fn new(consumer: C) -> Self {
        Self {
            consumer,
            pending: Mutex::new(Vec::new()),
        }
    }

    /// The consumer.
    pub fn consumer(&self) -> &C {
        &self.consumer
    }

    async fn ack_pending(&self, calls: &CallSlot) -> Result<(), FaucetError> {
        let tokens = std::mem::take(&mut *self.pending.lock().unwrap_or_else(|e| e.into_inner()));
        if tokens.is_empty() {
            return Ok(());
        }
        self.consumer.ack(calls, tokens).await
    }
}

#[async_trait]
impl<C: MessageConsumer> SourceDriver for MessageSourceDriver<C> {
    fn connector_name(&self) -> &'static str {
        self.consumer.connector_name()
    }

    fn config_schema(&self) -> Value {
        self.consumer.config_schema()
    }

    fn dataset_uri(&self) -> String {
        self.consumer.dataset_uri()
    }

    fn call_context(&self) -> CallContext {
        self.consumer.call_context()
    }

    fn consumes_destructively(&self) -> bool {
        true
    }

    fn read<'a>(
        &'a self,
        calls: &'a CallSlot,
        _context: &'a HashMap<String, Value>,
        batch_size: usize,
    ) -> PageStream<'a> {
        self.pages(calls, batch_size, true, self.consumer.max_messages())
    }

    async fn fetch(
        &self,
        calls: &CallSlot,
        _context: &HashMap<String, Value>,
    ) -> Option<Result<(Vec<Value>, Option<Value>), FaucetError>> {
        let cap = self
            .consumer
            .max_messages()
            .unwrap_or(crate::pipeline::DEFAULT_BATCH_SIZE);
        let mut pages = self.pages(calls, 0, false, Some(cap));
        let mut rows = Vec::new();
        let mut bookmark = None;
        while let Some(page) = pages.next().await {
            match page {
                Ok(page) => {
                    rows.extend(page.records);
                    bookmark = page.bookmark;
                }
                Err(e) => return Some(Err(e)),
            }
        }
        Some(Ok((rows, bookmark)))
    }

    async fn check(
        &self,
        calls: &CallSlot,
        ctx: &CheckContext,
    ) -> Result<Option<CheckReport>, FaucetError> {
        if let Some(report) = self.consumer.check(calls, ctx).await? {
            return Ok(Some(report));
        }
        Ok(Some(CheckReport::single(Probe::skip(
            "read",
            "reading a page would consume messages; no consumer probe is defined",
        ))))
    }
}

impl<C: MessageConsumer> MessageSourceDriver<C> {
    /// The page stream, ending after `limit` messages. A queue has no end, so
    /// `batch_size` 0 pages by the default size rather than meaning one page.
    /// `ack: false` never acknowledges, so a buffered read leaves every message
    /// for the broker to redeliver; its `limit` keeps a redelivered message
    /// from extending the read forever.
    fn pages<'a>(
        &'a self,
        calls: &'a CallSlot,
        batch_size: usize,
        ack: bool,
        limit: Option<usize>,
    ) -> PageStream<'a> {
        let per_page = if batch_size == 0 {
            crate::pipeline::DEFAULT_BATCH_SIZE
        } else {
            batch_size
        };
        Box::pin(async_stream::try_stream! {
            let mut consumed = 0usize;
            let mut idle = 0u32;
            loop {
                if ack {
                    self.ack_pending(calls).await?;
                }
                let room = match limit {
                    Some(max) if consumed >= max => break,
                    Some(max) => per_page.min(max - consumed),
                    None => per_page,
                };
                let messages = self.consumer.receive(calls, room).await?;
                if messages.is_empty() {
                    idle += 1;
                    if idle >= self.consumer.idle_receives_to_stop() {
                        break;
                    }
                    continue;
                }
                idle = 0;
                let mut records = Vec::with_capacity(messages.len());
                let mut tokens = Vec::with_capacity(messages.len());
                for m in &messages {
                    records.push(self.consumer.decode(m)?);
                    tokens.push(m.ack.clone());
                }
                consumed += records.len();
                if ack {
                    *self.pending.lock().unwrap_or_else(|e| e.into_inner()) = tokens;
                }
                yield StreamPage {
                    records,
                    bookmark: Some(serde_json::json!({ "consumed": consumed })),
                };
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cut_short_group_keeps_a_cancel_and_words_anything_else() {
        let cancel = FaucetError::Cancelled {
            operation: "publish call".into(),
        };
        assert!(matches!(
            cut("broker", &cancel),
            FaucetError::Cancelled { operation } if operation == "publish call"
        ));
        let other = cut("broker", &FaucetError::Source("x".into()));
        assert!(
            matches!(&other, FaucetError::Sink(m) if m.starts_with("broker publish: ")),
            "{other}"
        );
        let mut outcomes: Vec<RowOutcome> = vec![Err(FaucetError::Sink("own".into())), Ok(())];
        fail_open(&mut outcomes, || cut("broker", &cancel));
        assert!(matches!(&outcomes[0], Err(FaucetError::Sink(m)) if m == "own"));
        assert!(matches!(&outcomes[1], Err(FaucetError::Cancelled { .. })));
    }
}
