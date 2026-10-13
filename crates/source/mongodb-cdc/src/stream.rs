//! MongoDB change-stream executor.

use crate::config::{MongoCdcSourceConfig, StartFrom};
use crate::envelope::to_envelope;
use crate::state::{Bookmark, state_key};
use async_trait::async_trait;
use faucet_core::{FaucetError, Source, Stream, StreamPage};
use mongodb::Client;
use mongodb::bson::{self, Document, Timestamp};
use mongodb::change_stream::event::ResumeToken;
use mongodb::options::{FullDocumentBeforeChangeType, FullDocumentType};
use serde_json::Value;
use std::collections::HashMap;
use std::pin::Pin;
use tokio::sync::Mutex;

/// Effective start position resolved for a given fetch cycle.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum StartPosition {
    /// Server default (current cluster time).
    Now,
    /// `start_at_operation_time`.
    AtOperationTime(Timestamp),
    /// `resume_after(token)`.
    ResumeAfter(ResumeToken),
    /// `start_after(token)` — required (not `resume_after`) when resuming from an
    /// **invalidate** token, which MongoDB rejects for `resumeAfter` (#321 M3).
    StartAfter(ResumeToken),
    /// The oldest entry still in the oplog, resolved when the stream opens.
    Earliest,
}

/// Resolve the effective start position from config + any persisted bookmark.
///
/// A persisted bookmark always wins: `start_from` only places a **fresh** run.
/// An explicit `resume_token` / `timestamp` that overrode the bookmark replayed
/// from the same point every run — rewriting committed pages under new tokens
/// in exactly-once mode, ignoring a mirror's captured handoff position, and
/// failing for good once the point left the oplog (#789 MSG-32).
pub(crate) fn resolve_start(
    start_from: &StartFrom,
    pending: Option<&Bookmark>,
) -> Result<StartPosition, FaucetError> {
    if let Some(b) = pending {
        if matches!(
            start_from,
            StartFrom::ResumeToken { .. } | StartFrom::Timestamp { .. }
        ) {
            tracing::info!(
                connector = "mongodb-cdc",
                "the explicit start_from is ignored because a bookmark exists; reset the row's \
                 state to start from it"
            );
        }
        // A bookmark captured from an invalidate event must resume with
        // `start_after` — `resume_after` on an invalidate token is rejected by
        // MongoDB and would wedge the pipeline (#321 M3).
        return if b.invalidate {
            Ok(StartPosition::StartAfter(b.to_token()?))
        } else {
            Ok(StartPosition::ResumeAfter(b.to_token()?))
        };
    }
    match start_from {
        StartFrom::ResumeToken { token } => {
            let b = Bookmark {
                resume_token: token.clone(),
                ..Default::default()
            };
            Ok(StartPosition::ResumeAfter(b.to_token()?))
        }
        StartFrom::Timestamp { timestamp_secs } => Ok(StartPosition::AtOperationTime(Timestamp {
            time: *timestamp_secs,
            increment: 0,
        })),
        StartFrom::Earliest => Ok(StartPosition::Earliest),
        StartFrom::Now => Ok(StartPosition::Now),
    }
}

/// Where `start_from: earliest` opens when the oldest oplog entry cannot be
/// read: the beginning of time, which works until the oplog first rolls over.
const EARLIEST_FALLBACK: Timestamp = Timestamp {
    time: 1,
    increment: 1,
};

/// The identity of the transaction an event belongs to (`lsid` +
/// `txnNumber`), or `None` for an event outside a multi-document transaction.
fn txn_key(lsid: Option<&Document>, txn_number: Option<i64>) -> Option<(String, i64)> {
    match (lsid, txn_number) {
        (Some(l), Some(n)) => Some((l.to_string(), n)),
        _ => None,
    }
}

/// Enforce the in-memory staging cap before buffering one more change record.
///
/// `current_len` is the number of records already buffered this fetch cycle;
/// `max_staged` is the configured cap (`None` = unbounded). Returns a typed
/// [`FaucetError::Source`] when adding one more record would exceed the cap,
/// so the run aborts cleanly instead of risking an OOM-kill on an unbounded
/// `batch_size: 0` (or `fetch_all`) drain. Mirrors `postgres-cdc` /
/// `mysql-cdc`'s `push_staged` guard.
pub(crate) fn check_staging_cap(
    current_len: usize,
    max_staged: Option<usize>,
) -> Result<(), FaucetError> {
    if let Some(max) = max_staged
        && current_len >= max
    {
        return Err(FaucetError::Source(format!(
            "mongodb-cdc: in-memory change buffer exceeded max_staged_records ({max}); \
             aborting to avoid unbounded memory growth. Raise max_staged_records or \
             reduce batch_size so pages flush more often."
        )));
    }
    Ok(())
}

/// A configured MongoDB CDC source.
pub struct MongoCdcSource {
    config: MongoCdcSourceConfig,
    client: Client,
    state_key_value: String,
    pending_bookmark: Mutex<Option<Bookmark>>,
    /// Cluster time (seconds) of the furthest point read this run: the last
    /// event, or the stream's post-batch token when it went idle. 0 = none
    /// yet. Where [`Source::lag`] measures from (#733).
    position_secs: std::sync::atomic::AtomicU64,
    /// Backend-call context (#880): the run's deadline, cancel token and
    /// round-trip recorder for the watch opens, queries and stream polls.
    calls: faucet_core::CallSlot,
}

impl MongoCdcSource {
    /// Connect, validate config + topology, and build the source.
    pub async fn new(config: MongoCdcSourceConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let client = Client::with_uri_str(&config.connection_uri)
            .await
            .map_err(|e| FaucetError::Source(format!("MongoDB connection failed: {e}")))?;
        ensure_changestream_capable(&client).await?;
        if config.full_document_before_change != crate::config::FullDocumentBeforeChange::Off {
            tracing::warn!(
                "mongodb-cdc: full_document_before_change requires MongoDB 6.0+ and the \
                 collection's changeStreamPreAndPostImages enabled; the server will error \
                 at stream open if unavailable"
            );
        }
        let state_key_value = state_key(&config.scope);
        Ok(Self {
            config,
            client,
            state_key_value,
            pending_bookmark: Mutex::new(None),
            position_secs: std::sync::atomic::AtomicU64::new(0),
            calls: faucet_core::CallSlot::new(faucet_core::CallContext::new().without_timeout()),
        })
    }
}

/// Run `hello` and assert the deployment supports change streams.
async fn ensure_changestream_capable(client: &Client) -> Result<(), FaucetError> {
    let hello = client
        .database("admin")
        .run_command(bson::doc! { "hello": 1 })
        .await
        .map_err(|e| FaucetError::Source(format!("mongodb-cdc hello failed: {e}")))?;
    if is_changestream_topology(&hello) {
        Ok(())
    } else {
        Err(FaucetError::Source(
            "mongodb-cdc: Change Streams require a replica set or sharded cluster; \
             the target appears to be a standalone mongod"
                .into(),
        ))
    }
}

/// Classify a `hello` response: replica-set (`setName`) or sharded (`msg=isdbgrid`).
pub(crate) fn is_changestream_topology(hello: &Document) -> bool {
    hello.get_str("setName").is_ok()
        || hello
            .get_str("msg")
            .map(|m| m == "isdbgrid")
            .unwrap_or(false)
}

/// Map the config post-image mode to the driver type (`None` = don't request).
pub(crate) fn full_document_type(fd: crate::config::FullDocument) -> Option<FullDocumentType> {
    use crate::config::FullDocument as F;
    match fd {
        F::Off => None,
        F::WhenAvailable => Some(FullDocumentType::WhenAvailable),
        F::Required => Some(FullDocumentType::Required),
        F::UpdateLookup => Some(FullDocumentType::UpdateLookup),
    }
}

/// Map the config pre-image mode to the driver type (`None` = don't request).
pub(crate) fn full_document_before_type(
    fd: crate::config::FullDocumentBeforeChange,
) -> Option<FullDocumentBeforeChangeType> {
    use crate::config::FullDocumentBeforeChange as F;
    match fd {
        F::Off => None,
        F::WhenAvailable => Some(FullDocumentBeforeChangeType::WhenAvailable),
        F::Required => Some(FullDocumentBeforeChangeType::Required),
    }
}

/// Whether `invalidate` events are delivered as records. The server-side
/// `$match` always lets them through, because the stream ends at one and its
/// token is the only way to resume past it (#789 SQL-27).
pub(crate) fn reports_invalidate(config: &MongoCdcSourceConfig) -> bool {
    config.operation_types.is_empty() || config.operation_types.iter().any(|t| t == "invalidate")
}

/// Build the server-side pipeline: operation-type `$match` + user stages.
pub(crate) fn build_pipeline(config: &MongoCdcSourceConfig) -> Result<Vec<Document>, FaucetError> {
    let mut pipeline: Vec<Document> = Vec::new();
    if !config.operation_types.is_empty() {
        let mut types = config.operation_types.clone();
        if !reports_invalidate(config) {
            types.push("invalidate".to_string());
        }
        pipeline.push(bson::doc! {
            "$match": { "operationType": { "$in": types } }
        });
    }
    for (i, stage) in config.aggregation_pipeline.iter().enumerate() {
        let b = bson::to_bson(stage)
            .map_err(|e| FaucetError::Config(format!("mongodb-cdc: pipeline stage {i}: {e}")))?;
        match b {
            bson::Bson::Document(d) => pipeline.push(d),
            other => {
                return Err(FaucetError::Config(format!(
                    "mongodb-cdc: aggregation_pipeline[{i}] must be an object, got {other:?}"
                )));
            }
        }
    }
    Ok(pipeline)
}

#[async_trait]
impl Source for MongoCdcSource {
    fn set_call_context(&self, ctx: faucet_core::CallContext) {
        self.calls.install(ctx);
    }

    fn set_roundtrip_recorder(
        &self,
        recorder: std::sync::Arc<faucet_core::observability::RoundtripRecorder>,
    ) {
        self.calls.install_recorder(recorder);
    }

    async fn fetch_with_context(
        &self,
        ctx: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        use futures::StreamExt;
        let mut pages = self.stream_pages(ctx, 0);
        let mut all = Vec::new();
        while let Some(page) = pages.next().await {
            all.extend(page?.records);
        }
        Ok(all)
    }

    fn stream_pages<'a>(
        &'a self,
        ctx: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        self.stream_pages_impl(ctx, self.config.batch_size)
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(schemars::schema_for!(MongoCdcSourceConfig)).unwrap_or(Value::Null)
    }

    fn state_key(&self) -> Option<String> {
        Some(self.state_key_value.clone())
    }

    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        let b = Bookmark::from_value(bookmark)?;
        // Round-trip-validate the token now so a corrupt bookmark fails fast.
        b.to_token()?;
        *self.pending_bookmark.lock().await = Some(b);
        Ok(())
    }

    async fn capture_resume_position(&self) -> Result<Option<Value>, FaucetError> {
        let token = self
            .calls
            .call_once("watch", self.capture_now_token())
            .await?;
        Ok(Some(Bookmark::from_token(&token)?.to_value()?))
    }

    fn capabilities(&self) -> faucet_core::Capabilities {
        faucet_core::Capabilities::of_source(self)
            .to_builder()
            .reports_lag(true)
            .table_routing(true)
            .read_side_effects(true)
            .build()
    }

    /// Seconds between the cluster's current time and the oldest change this
    /// source has not delivered (#733).
    async fn lag(&self) -> Result<Option<faucet_core::SourceLag>, FaucetError> {
        let mut position = self
            .position_secs
            .load(std::sync::atomic::Ordering::Relaxed);
        if position == 0 {
            let pending = self.pending_bookmark.lock().await.clone();
            let Some(bm) = pending else {
                return Ok(None);
            };
            match self
                .calls
                .call_once("watch", self.first_unread_secs(&bm))
                .await?
            {
                Some(secs) => position = secs,
                None => return Ok(None),
            }
        }
        let hello = self
            .calls
            .call_once("query", async {
                self.client
                    .database("admin")
                    .run_command(bson::doc! { "hello": 1 })
                    .await
                    .map_err(|e| FaucetError::Source(format!("mongodb-cdc lag: hello failed: {e}")))
            })
            .await?;
        let Ok(now) = hello.get_timestamp("operationTime") else {
            return Ok(None);
        };
        Ok(Some(faucet_core::SourceLag::seconds(
            u64::from(now.time).saturating_sub(position) as f64,
        )))
    }

    fn state_schema(&self) -> u32 {
        crate::state::STATE_SCHEMA
    }

    fn migrate_state(&self, from: u32, data: Value) -> Result<Value, FaucetError> {
        crate::state::migrate_state(from, data)
    }

    fn supports_exactly_once(&self) -> bool {
        // Durable resumeToken + deterministic replay from it + per-event
        // (per-page) bookmarks — the requirements for exactly-once delivery.
        true
    }

    fn record_table(&self, record: &Value) -> Option<String> {
        namespace_table(&self.config.scope, record)
    }

    fn position_le(&self, a: &Value, b: &Value) -> Option<bool> {
        token_le(a, b)
    }

    fn record_covered(&self, record: &Value, position: &Value) -> Option<bool> {
        token_le(record, position)
    }

    fn connector_name(&self) -> &'static str {
        "mongodb-cdc"
    }

    fn dataset_uri(&self) -> String {
        dataset_uri_for(&self.config)
    }

    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};
        let start = std::time::Instant::now();

        // Run `hello` once under the probe timeout, then derive two probes from
        // the outcome: `connection` (could we reach + auth to the server?) and
        // `topology` (does it support change streams?). On a connection
        // failure/timeout the topology probe cannot run, so only `connection`
        // is reported.
        let hello = tokio::time::timeout(
            ctx.timeout,
            self.client
                .database("admin")
                .run_command(bson::doc! { "hello": 1 }),
        )
        .await;

        let hello = match hello {
            Ok(Ok(doc)) => doc,
            Ok(Err(e)) => {
                return Ok(CheckReport::single(Probe::fail_hint(
                    "connection",
                    start.elapsed(),
                    format!("could not run hello: {e}"),
                    "verify the URI is reachable and credentials are valid",
                )));
            }
            Err(_elapsed) => {
                return Ok(CheckReport::single(Probe::fail_hint(
                    "connection",
                    start.elapsed(),
                    "connection timed out",
                    "the server did not respond within the check timeout",
                )));
            }
        };

        let connection = Probe::pass("connection", start.elapsed());
        let topology = if is_changestream_topology(&hello) {
            Probe::pass("topology", start.elapsed())
        } else {
            Probe::fail_hint(
                "topology",
                start.elapsed(),
                "target is a standalone mongod",
                "Change Streams require a replica set or sharded cluster",
            )
        };
        Ok(CheckReport {
            probes: vec![connection, topology],
        })
    }
}

fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// The cluster time (seconds) encoded in a resume token's `_data`: a KeyString
/// whose first byte `0x82` tags a BSON timestamp, seconds first, big-endian.
fn token_cluster_secs(token: &Value) -> Option<u64> {
    let data = token.get("_data")?.as_str()?;
    if data.len() < 10 || !data.starts_with("82") {
        return None;
    }
    u32::from_str_radix(&data[2..10], 16).ok().map(u64::from)
}

impl MongoCdcSource {
    fn note_position(&self, secs: u64) {
        self.position_secs
            .fetch_max(secs, std::sync::atomic::Ordering::Relaxed);
    }

    /// Where a stream resumed after `bm` stands: the cluster time of the first
    /// change not yet delivered, or — when there is none — the stream's
    /// post-batch token (the present). Opens and closes one change stream.
    async fn first_unread_secs(&self, bm: &Bookmark) -> Result<Option<u64>, FaucetError> {
        let token = bm.to_token()?;
        let max_await = std::time::Duration::from_millis(self.config.max_await_time_ms.min(1000));
        let watch_err = |e: mongodb::error::Error| {
            FaucetError::Source(format!("mongodb-cdc lag watch failed: {e}"))
        };
        let cs: mongodb::change_stream::ChangeStream<
            mongodb::change_stream::event::ChangeStreamEvent<Document>,
        > = match &self.config.scope {
            crate::config::Scope::Collection {
                database,
                collection,
            } => {
                let coll = self
                    .client
                    .database(database)
                    .collection::<Document>(collection);
                let w = coll.watch().max_await_time(max_await);
                (if bm.invalidate {
                    w.start_after(token)
                } else {
                    w.resume_after(token)
                })
                .await
                .map_err(watch_err)?
            }
            crate::config::Scope::Database { database } => {
                let db = self.client.database(database);
                let w = db.watch().max_await_time(max_await);
                (if bm.invalidate {
                    w.start_after(token)
                } else {
                    w.resume_after(token)
                })
                .await
                .map_err(watch_err)?
            }
            crate::config::Scope::Cluster => {
                let w = self.client.watch().max_await_time(max_await);
                (if bm.invalidate {
                    w.start_after(token)
                } else {
                    w.resume_after(token)
                })
                .await
                .map_err(watch_err)?
            }
        };
        let mut cs = cs;
        match tokio::time::timeout(max_await * 4, cs.next_if_any()).await {
            Ok(Ok(Some(event))) => Ok(event.cluster_time.map(|ts| u64::from(ts.time))),
            // An empty batch: nothing is waiting, so the post-batch token is
            // the present.
            Ok(Ok(None)) => Ok(cs
                .resume_token()
                .and_then(|t| Bookmark::from_token(&t).ok())
                .and_then(|b| token_cluster_secs(&b.resume_token))),
            Ok(Err(e)) => Err(FaucetError::Source(format!(
                "mongodb-cdc lag: change stream error: {e}"
            ))),
            Err(_) => Ok(None),
        }
    }

    /// Open the change stream and drain it with an idle-timeout terminator.
    fn stream_pages_impl<'a>(
        &'a self,
        _ctx: &'a HashMap<String, Value>,
        batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let idle_timeout = self.config.idle_timeout;
        let max_await = std::time::Duration::from_millis(self.config.max_await_time_ms);
        let per_batch = batch_size != 0;
        let max_staged = self.config.max_staged_records;

        Box::pin(async_stream::try_stream! {

            let pending = self.pending_bookmark.lock().await.take();
            let mut start = resolve_start(&self.config.start_from, pending.as_ref())?;
            // A fresh `now` start opens the real stream at the present and
            // anchors at its initial post-batch token; a separate capture
            // stream could consume the first event (#862 L-070).
            let anchor_now = start == StartPosition::Now;
            let calls = self.calls.get();
            if start == StartPosition::Earliest {
                let oldest = calls
                    .call_once("query", async { Ok(self.oldest_oplog_time().await) })
                    .await?;
                start = StartPosition::AtOperationTime(oldest);
            }
            let pipeline = build_pipeline(&self.config)?;
            let report_invalidate = reports_invalidate(&self.config);

            // Open the change stream scoped to collection, database, or cluster.
            // Each arm resolves to a ChangeStream<ChangeStreamEvent<Document>>.
            let change_stream: mongodb::change_stream::ChangeStream<
                mongodb::change_stream::event::ChangeStreamEvent<Document>,
            > = calls.call_once("watch", async { Ok(match &self.config.scope {
                crate::config::Scope::Collection { database, collection } => {
                    let coll: mongodb::Collection<Document> =
                        self.client.database(database).collection(collection);
                    let mut w = coll
                        .watch()
                        .pipeline(pipeline)
                        .max_await_time(max_await);
                    if let Some(fd) = full_document_type(self.config.full_document) {
                        w = w.full_document(fd);
                    }
                    if let Some(fb) =
                        full_document_before_type(self.config.full_document_before_change)
                    {
                        w = w.full_document_before_change(fb);
                    }
                    w = match &start {
                        StartPosition::Now | StartPosition::Earliest => w,
                        StartPosition::AtOperationTime(ts) => w.start_at_operation_time(*ts),
                        StartPosition::ResumeAfter(tok) => w.resume_after(tok.clone()),
                        StartPosition::StartAfter(tok) => w.start_after(tok.clone()),
                    };
                    w.await
                        .map_err(|e| FaucetError::Source(format!("mongodb-cdc watch failed: {e}")))?
                }
                crate::config::Scope::Database { database } => {
                    let db = self.client.database(database);
                    let mut w = db
                        .watch()
                        .pipeline(pipeline)
                        .max_await_time(max_await);
                    if let Some(fd) = full_document_type(self.config.full_document) {
                        w = w.full_document(fd);
                    }
                    if let Some(fb) =
                        full_document_before_type(self.config.full_document_before_change)
                    {
                        w = w.full_document_before_change(fb);
                    }
                    w = match &start {
                        StartPosition::Now | StartPosition::Earliest => w,
                        StartPosition::AtOperationTime(ts) => w.start_at_operation_time(*ts),
                        StartPosition::ResumeAfter(tok) => w.resume_after(tok.clone()),
                        StartPosition::StartAfter(tok) => w.start_after(tok.clone()),
                    };
                    w.await
                        .map_err(|e| FaucetError::Source(format!("mongodb-cdc watch failed: {e}")))?
                }
                crate::config::Scope::Cluster => {
                    let mut w = self.client
                        .watch()
                        .pipeline(pipeline)
                        .max_await_time(max_await);
                    if let Some(fd) = full_document_type(self.config.full_document) {
                        w = w.full_document(fd);
                    }
                    if let Some(fb) =
                        full_document_before_type(self.config.full_document_before_change)
                    {
                        w = w.full_document_before_change(fb);
                    }
                    w = match &start {
                        StartPosition::Now | StartPosition::Earliest => w,
                        StartPosition::AtOperationTime(ts) => w.start_at_operation_time(*ts),
                        StartPosition::ResumeAfter(tok) => w.resume_after(tok.clone()),
                        StartPosition::StartAfter(tok) => w.start_after(tok.clone()),
                    };
                    w.await
                        .map_err(|e| FaucetError::Source(format!("mongodb-cdc watch failed: {e}")))?
                }
            }) }).await?;
            let mut change_stream = change_stream;
            // A poll waits on the server; it is not a round trip of its own.
            let steps = calls.with_roundtrip_metering(false);
            if let Some(tok) = change_stream.resume_token().filter(|_| anchor_now) {
                yield StreamPage {
                    records: Vec::new(),
                    bookmark: Some(Bookmark::from_token(&tok)?.to_value()?),
                };
            }

            let chunk = if per_batch { batch_size } else { usize::MAX };
            let mut buffer: Vec<Value> = Vec::new();
            // Bookmark of the last record currently in `buffer`. `None` only when
            // the buffer is empty (no events seen this cycle).
            let mut last_bookmark: Option<Value> = None;
            // Transaction of the last buffered event: a full page is held back
            // until that transaction's events are all buffered, so a reader
            // never sees half of a source transaction (#789 MSG-92).
            let mut buffered_txn: Option<(String, i64)> = None;
            // Where the buffered transaction began in `buffer`, and the
            // bookmark before it: a cycle that ends abruptly flushes only what
            // precedes it (#862 L-062).
            let mut open_txn: Option<(usize, Option<Value>)> = None;
            let mut last_event_at = std::time::Instant::now();
            // A getMore waits at most `max_await` server-side; the client-side
            // bound only guards a dead connection.
            let poll_bound = max_await * 4 + std::time::Duration::from_secs(5);

            // Graceful shutdown is handled one layer up: `run_stream`'s page loop
            // `biased`-`select!`s the pipeline cancel token (Pipeline::with_cancel,
            // #146) against polling this stream for its next page, so a cancel
            // stops at the page boundary and flushes the sinks. Nothing is
            // persisted until a page yields, so a dropped in-flight buffer is
            // simply re-fetched (the resume token has not advanced).
            loop {
                if !change_stream.is_alive() {
                    tracing::warn!(
                        connector = "mongodb-cdc",
                        "the server closed the change stream; ending the fetch cycle"
                    );
                    if let Some(page) = complete_prefix(&mut buffer, last_bookmark.take(), open_txn.take()) {
                        yield page;
                    }
                    break;
                }
                // The run's deadline and cancel token end the stream with an
                // error (#880); the pipeline has already stopped reading.
                let polled = steps
                    .call_once("poll", async {
                        Ok(tokio::time::timeout(poll_bound, change_stream.next_if_any()).await)
                    })
                    .await?;
                match polled {
                    Ok(Ok(Some(event))) => {
                        last_event_at = std::time::Instant::now();
                        if let Some(ts) = &event.cluster_time {
                            self.note_position(u64::from(ts.time));
                        }
                        let txn = txn_key(event.lsid.as_ref(), event.txn_number);
                        if per_batch && buffer.len() >= chunk && txn != buffered_txn {
                            open_txn = None;
                            yield StreamPage {
                                records: std::mem::take(&mut buffer),
                                bookmark: last_bookmark.take(),
                            };
                        }
                        if txn.is_none() {
                            open_txn = None;
                        } else if txn != buffered_txn {
                            open_txn = Some((buffer.len(), last_bookmark.clone()));
                        }
                        let bookmark = Bookmark::from_token(&event.id)?;
                        let is_invalidate = matches!(
                            event.operation_type,
                            mongodb::change_stream::event::OperationType::Invalidate
                        );
                        // OOM safety valve: with `batch_size: 0` (or `fetch_all`)
                        // the buffer is never flushed mid-cycle, so a
                        // high-throughput stream would grow it without bound.
                        if report_invalidate || !is_invalidate {
                            check_staging_cap(buffer.len(), max_staged)?;
                            buffer.push(to_envelope(&event, &bookmark)?);
                        }
                        // The persisted bookmark records whether it is an
                        // invalidate token, so resume uses `start_after` not
                        // `resume_after` (#321 M3).
                        let persisted = Bookmark {
                            invalidate: is_invalidate,
                            ..bookmark
                        };
                        last_bookmark = Some(persisted.to_value()?);
                        buffered_txn = txn;

                        // An invalidate closes the stream server-side: flush the
                        // page (including the invalidate event) and end.
                        if is_invalidate {
                            yield StreamPage {
                                records: std::mem::take(&mut buffer),
                                bookmark: last_bookmark.take(),
                            };
                            break;
                        }
                        if per_batch && buffer.len() >= chunk && buffered_txn.is_none() {
                            open_txn = None;
                            yield StreamPage {
                                records: std::mem::take(&mut buffer),
                                bookmark: last_bookmark.take(),
                            };
                        }
                    }
                    Ok(Err(e)) => {
                        Err(FaucetError::Source(format!("mongodb-cdc stream error: {e}")))?;
                    }
                    // An empty batch: every event up to the present is delivered.
                    Ok(Ok(None)) => {
                        if last_event_at.elapsed() < idle_timeout {
                            // A held-back page's transaction is complete.
                            if per_batch && buffer.len() >= chunk {
                                buffered_txn = None;
                                open_txn = None;
                                yield StreamPage {
                                    records: std::mem::take(&mut buffer),
                                    bookmark: last_bookmark.take(),
                                };
                            }
                            continue;
                        }
                        self.note_position(unix_now_secs());
                        // The post-batch resume token covers everything
                        // delivered *and* everything the pipeline filtered out,
                        // so a quiet or filtered stream keeps its bookmark
                        // inside the oplog window (#789 MSG-33).
                        let bookmark = match change_stream.resume_token() {
                            Some(tok) => Some(Bookmark::from_token(&tok)?.to_value()?),
                            None => last_bookmark.take(),
                        };
                        if !buffer.is_empty() || bookmark.is_some() {
                            yield StreamPage {
                                records: std::mem::take(&mut buffer),
                                bookmark,
                            };
                        }
                        break;
                    }
                    // The poll outlived its bound (a dead connection): flush
                    // what was read with its own bookmark. The driver's resume
                    // token must not be read here — the poll is mid-flight.
                    Err(_) => {
                        if let Some(page) = complete_prefix(&mut buffer, last_bookmark.take(), open_txn.take()) {
                            yield page;
                        }
                        break;
                    }
                }
            }

            tracing::info!(connector = "mongodb-cdc", "change stream fetch cycle complete");
        })
    }

    /// The timestamp of the oldest entry still in the oplog, where
    /// `start_from: earliest` opens: `{t: 1, i: 1}` is rejected once the
    /// oplog has rolled over (#789 MSG-74). Without read access to
    /// `local.oplog.rs` it falls back to `{1, 1}` with a warning.
    async fn oldest_oplog_time(&self) -> Timestamp {
        let sharded = self
            .client
            .database("admin")
            .run_command(bson::doc! { "hello": 1 })
            .await
            .ok()
            .is_some_and(|hello| hello.get_str("msg") == Ok("isdbgrid"));
        let found = if sharded {
            Ok(None)
        } else {
            self.client
                .database("local")
                .collection::<Document>("oplog.rs")
                .find_one(Document::new())
                .sort(bson::doc! { "$natural": 1 })
                .projection(bson::doc! { "ts": 1 })
                .await
                .map_err(|e| e.to_string())
        };
        let (ts, warning) = earliest_from(found, sharded);
        if let Some(warning) = warning {
            tracing::warn!(connector = "mongodb-cdc", "{warning}");
        }
        ts
    }

    /// Open a change stream at "now", read its `postBatchResumeToken` without
    /// consuming any events, then drop the stream. Used by the replication
    /// snapshot→CDC handoff to anchor the stream before the bulk snapshot.
    async fn capture_now_token(&self) -> Result<ResumeToken, FaucetError> {
        use crate::config::Scope;
        let max_await = std::time::Duration::from_millis(self.config.max_await_time_ms);
        let cs: mongodb::change_stream::ChangeStream<
            mongodb::change_stream::event::ChangeStreamEvent<Document>,
        > = match &self.config.scope {
            Scope::Collection {
                database,
                collection,
            } => self
                .client
                .database(database)
                .collection::<Document>(collection)
                .watch()
                .max_await_time(max_await)
                .await
                .map_err(|e| {
                    FaucetError::Source(format!("mongodb-cdc capture watch failed: {e}"))
                })?,
            Scope::Database { database } => self
                .client
                .database(database)
                .watch()
                .max_await_time(max_await)
                .await
                .map_err(|e| {
                    FaucetError::Source(format!("mongodb-cdc capture watch failed: {e}"))
                })?,
            Scope::Cluster => self
                .client
                .watch()
                .max_await_time(max_await)
                .await
                .map_err(|e| {
                    FaucetError::Source(format!("mongodb-cdc capture watch failed: {e}"))
                })?,
        };
        let mut cs = cs;
        // The initial aggregate response usually carries a postBatchResumeToken.
        // If the driver hasn't cached one yet, one getMore returns it. That
        // poll must complete (`next_if_any` returns on an empty batch): reading
        // the token while a timed-out `next()` is mid-poll panics the driver.
        if let Some(tok) = cs.resume_token() {
            return Ok(tok);
        }
        cs.next_if_any()
            .await
            .map_err(|e| FaucetError::Source(format!("mongodb-cdc capture poll failed: {e}")))?;
        cs.resume_token().ok_or_else(|| {
            FaucetError::Source(
                "mongodb-cdc: could not capture a resume token (no postBatchResumeToken returned)"
                    .into(),
            )
        })
    }
}

/// The collection a change envelope belongs to: the bare collection name for a
/// database or collection scope (what the `mongodb` source's discovery lists),
/// `db.collection` for a cluster scope where names can collide across databases.
fn namespace_table(scope: &crate::config::Scope, record: &Value) -> Option<String> {
    let ns = record.get("namespace")?;
    let coll = ns.get("coll")?.as_str()?;
    match scope {
        crate::config::Scope::Cluster => Some(format!("{}.{coll}", ns.get("db")?.as_str()?)),
        _ => Some(coll.to_string()),
    }
}

/// Resume tokens order by their hex-encoded `_data` key string, which MongoDB
/// encodes so that byte order is event order.
fn token_le(a: &Value, b: &Value) -> Option<bool> {
    let data = |v: &Value| {
        v.get("resume_token")?
            .get("_data")?
            .as_str()
            .map(str::to_ascii_uppercase)
    };
    Some(data(a)? <= data(b)?)
}

/// The dataset URI for a config: the host-only connection base (no
/// credentials, path or options) followed by the scope's database/collection.
fn dataset_uri_for(config: &MongoCdcSourceConfig) -> String {
    use crate::config::Scope;
    let base = faucet_core::redacted_uri_authority(&config.connection_uri);
    match &config.scope {
        Scope::Collection {
            database,
            collection,
        } => format!("{base}/{database}/{collection}"),
        Scope::Database { database } => format!("{base}/{database}"),
        Scope::Cluster => base,
    }
}

/// Where `start_from: earliest` opens given the oldest-oplog lookup, and the
/// warning to log when it has to fall back to the beginning of time.
fn earliest_from(
    found: Result<Option<Document>, String>,
    sharded: bool,
) -> (Timestamp, Option<String>) {
    const TAIL: &str = "starting at the beginning of time, which fails once the oplog has rolled";
    let reason = match found {
        Ok(Some(doc)) => match doc.get_timestamp("ts") {
            Ok(ts) => return (ts, None),
            Err(_) => "the oldest oplog entry has no timestamp `ts`".to_string(),
        },
        Ok(None) if sharded => "through mongos each shard keeps its own oplog, so `earliest` \
                                cannot be resolved on a sharded cluster"
            .to_string(),
        Ok(None) => "the oplog is empty".to_string(),
        Err(e) => format!("could not read the oldest oplog entry ({e})"),
    };
    (
        EARLIEST_FALLBACK,
        Some(format!(
            "mongodb-cdc: start_from: earliest — {reason}; {TAIL}"
        )),
    )
}

/// The page to flush when a cycle ends before the buffered transaction is
/// known to be complete: everything before that transaction with the bookmark
/// preceding it. The transaction's own events are left for the next run.
fn complete_prefix(
    buffer: &mut Vec<Value>,
    last_bookmark: Option<Value>,
    open_txn: Option<(usize, Option<Value>)>,
) -> Option<StreamPage> {
    let bookmark = match open_txn {
        Some((start, before)) => {
            buffer.truncate(start);
            before
        }
        None => last_bookmark,
    };
    (!buffer.is_empty()).then(|| StreamPage {
        records: std::mem::take(buffer),
        bookmark,
    })
}

#[cfg(test)]
mod tests {

    #[test]
    fn routes_by_collection_and_orders_resume_tokens() {
        use crate::config::Scope;
        let rec = json!({"namespace": {"db": "shop", "coll": "orders"}});
        let db = Scope::Database {
            database: "shop".into(),
        };
        assert_eq!(namespace_table(&db, &rec), Some("orders".into()));
        assert_eq!(
            namespace_table(&Scope::Cluster, &rec),
            Some("shop.orders".into())
        );
        assert_eq!(namespace_table(&db, &json!({"namespace": null})), None);
        let t = |d: &str| json!({"resume_token": {"_data": d}, "invalidate": false});
        assert_eq!(token_le(&t("8264A1"), &t("8264b2")), Some(true));
        assert_eq!(token_le(&t("8264B2"), &t("8264A1")), Some(false));
        assert_eq!(token_le(&t("8264A1"), &json!({})), None);
        let event = |d: &str| json!({"op": "c", "resume_token": {"_data": d}});
        assert_eq!(token_le(&event("8264A1"), &t("8264A1")), Some(true));
        assert_eq!(token_le(&event("8264B2"), &t("8264A1")), Some(false));
    }

    #[test]
    fn token_cluster_secs_decodes_the_leading_timestamp() {
        assert_eq!(
            token_cluster_secs(&json!({"_data": "8266543210000000012B0229296E04"})),
            Some(0x6654_3210)
        );
        assert_eq!(token_cluster_secs(&json!({"_data": "81abc"})), None);
        assert_eq!(token_cluster_secs(&json!({"_data": "82zzzzzzzz00"})), None);
        assert_eq!(token_cluster_secs(&json!({})), None);
    }

    use super::*;
    use serde_json::json;

    // These two pinned the old precedence (an explicit start_from beat the
    // bookmark on every run, #789 MSG-32): the bookmark now wins, and the
    // explicit position applies only to a fresh run.
    #[test]
    fn a_bookmark_beats_an_explicit_timestamp() {
        let pending = Bookmark {
            resume_token: json!({ "_data": "AA" }),
            ..Default::default()
        };
        let ts = StartFrom::Timestamp {
            timestamp_secs: 100,
        };
        assert!(matches!(
            resolve_start(&ts, Some(&pending)).unwrap(),
            StartPosition::ResumeAfter(_)
        ));
        assert_eq!(
            resolve_start(&ts, None).unwrap(),
            StartPosition::AtOperationTime(Timestamp {
                time: 100,
                increment: 0
            })
        );
    }

    #[test]
    fn a_bookmark_beats_an_explicit_resume_token() {
        let pending = Bookmark {
            resume_token: json!({ "_data": "PENDING" }),
            ..Default::default()
        };
        let explicit = StartFrom::ResumeToken {
            token: json!({ "_data": "8264AB00" }),
        };
        let data = |pos: StartPosition| match pos {
            StartPosition::ResumeAfter(tok) => {
                Bookmark::from_token(&tok).unwrap().resume_token["_data"].clone()
            }
            other => panic!("expected ResumeAfter, got {other:?}"),
        };
        assert_eq!(
            data(resolve_start(&explicit, Some(&pending)).unwrap()),
            json!("PENDING")
        );
        assert_eq!(
            data(resolve_start(&explicit, None).unwrap()),
            json!("8264AB00")
        );
    }

    #[test]
    fn transaction_keys_need_both_session_and_number() {
        let lsid = bson::doc! { "id": 1 };
        assert!(txn_key(Some(&lsid), Some(3)).is_some());
        assert_ne!(txn_key(Some(&lsid), Some(3)), txn_key(Some(&lsid), Some(4)));
        assert_eq!(txn_key(None, Some(3)), None);
        assert_eq!(txn_key(Some(&lsid), None), None);
    }

    #[test]
    fn now_yields_to_bookmark() {
        let pending = Bookmark {
            resume_token: json!({ "_data": "8264AB00" }),
            ..Default::default()
        };
        let pos = resolve_start(&StartFrom::Now, Some(&pending)).unwrap();
        assert!(matches!(pos, StartPosition::ResumeAfter(_)));
    }

    #[test]
    fn invalidate_bookmark_resumes_with_start_after() {
        // #321 M3: a bookmark captured from an invalidate event must resolve to
        // StartAfter (MongoDB rejects resumeAfter on an invalidate token).
        let pending = Bookmark {
            resume_token: json!({ "_data": "8264AB00" }),
            invalidate: true,
        };
        let pos = resolve_start(&StartFrom::Now, Some(&pending)).unwrap();
        assert!(
            matches!(pos, StartPosition::StartAfter(_)),
            "invalidate bookmark must use start_after, got {pos:?}"
        );
        // A non-invalidate bookmark still uses resume_after.
        let normal = Bookmark {
            resume_token: json!({ "_data": "8264AB00" }),
            invalidate: false,
        };
        assert!(matches!(
            resolve_start(&StartFrom::Earliest, Some(&normal)).unwrap(),
            StartPosition::ResumeAfter(_)
        ));
    }

    #[test]
    fn now_without_bookmark_is_now() {
        assert_eq!(
            resolve_start(&StartFrom::Now, None).unwrap(),
            StartPosition::Now
        );
    }

    #[test]
    fn earliest_without_bookmark_resolves_at_open() {
        assert_eq!(
            resolve_start(&StartFrom::Earliest, None).unwrap(),
            StartPosition::Earliest
        );
    }

    #[test]
    fn staging_cap_none_is_unbounded() {
        // With no cap, an arbitrarily large buffer never aborts.
        assert!(check_staging_cap(0, None).is_ok());
        assert!(check_staging_cap(1_000_000, None).is_ok());
    }

    #[test]
    fn staging_cap_allows_up_to_limit() {
        // Buffering record N is allowed while the current length is below the
        // cap (i.e. up to `max` records total).
        assert!(check_staging_cap(0, Some(2)).is_ok());
        assert!(check_staging_cap(1, Some(2)).is_ok());
    }

    #[test]
    fn staging_cap_aborts_at_limit() {
        // Once `current_len` reaches the cap, staging one more record must
        // abort with a typed FaucetError::Source naming max_staged_records.
        let err = check_staging_cap(2, Some(2)).unwrap_err();
        assert!(matches!(err, FaucetError::Source(_)));
        assert!(format!("{err}").contains("max_staged_records"));

        // And it stays an error for any length beyond the cap.
        assert!(check_staging_cap(3, Some(2)).is_err());
    }

    #[test]
    fn staging_cap_drives_accumulation_past_limit() {
        // Simulate the buffer-accumulation loop with batch_size: 0 (no
        // mid-cycle flush) and a cap of 3: the 4th record must abort.
        let mut buffer: Vec<u8> = Vec::new();
        let max = Some(3usize);
        let mut last: Result<(), FaucetError> = Ok(());
        for i in 0..10u8 {
            if let Err(e) = check_staging_cap(buffer.len(), max) {
                last = Err(e);
                break;
            }
            buffer.push(i);
        }
        assert_eq!(buffer.len(), 3, "buffer stops growing at the cap");
        let err = last.unwrap_err();
        assert!(matches!(err, FaucetError::Source(_)));
        assert!(format!("{err}").contains("max_staged_records"));
    }

    #[test]
    fn topology_classification() {
        assert!(is_changestream_topology(&bson::doc! { "setName": "rs0" }));
        assert!(is_changestream_topology(&bson::doc! { "msg": "isdbgrid" }));
        assert!(!is_changestream_topology(&bson::doc! { "ok": 1.0 }));
    }

    #[test]
    fn pipeline_prepends_operation_match() {
        let c: MongoCdcSourceConfig = serde_json::from_value(json!({
            "connection_uri": "mongodb://h/?replicaSet=rs0",
            "scope": { "type": "cluster" },
            "operation_types": ["insert", "delete"]
        }))
        .unwrap();
        let p = build_pipeline(&c).unwrap();
        assert_eq!(p.len(), 1);
        assert!(p[0].contains_key("$match"));
    }

    #[test]
    fn the_operation_match_always_lets_invalidate_through() {
        let cfg = |types: Value| -> MongoCdcSourceConfig {
            serde_json::from_value(json!({
                "connection_uri": "mongodb://h/?replicaSet=rs0",
                "scope": { "type": "cluster" },
                "operation_types": types
            }))
            .unwrap()
        };
        let types = |c: &MongoCdcSourceConfig| {
            build_pipeline(c).unwrap()[0]
                .get_document("$match")
                .unwrap()
                .get_document("operationType")
                .unwrap()
                .get_array("$in")
                .unwrap()
                .iter()
                .map(|b| b.as_str().unwrap().to_string())
                .collect::<Vec<_>>()
        };
        let filtered = cfg(json!(["insert"]));
        assert_eq!(types(&filtered), ["insert", "invalidate"]);
        assert!(!reports_invalidate(&filtered));
        let listed = cfg(json!(["invalidate", "insert"]));
        assert_eq!(types(&listed), ["invalidate", "insert"]);
        assert!(reports_invalidate(&listed));
        assert!(reports_invalidate(&cfg(json!([]))));
    }

    #[test]
    fn pipeline_rejects_non_object_stage() {
        let c: MongoCdcSourceConfig = serde_json::from_value(json!({
            "connection_uri": "mongodb://h/?replicaSet=rs0",
            "scope": { "type": "cluster" },
            "aggregation_pipeline": [[1, 2, 3]]
        }))
        .unwrap();
        assert!(matches!(build_pipeline(&c), Err(FaucetError::Config(_))));
    }

    #[test]
    fn pipeline_appends_object_stages_after_match() {
        // operation_types prepends a $match; each aggregation_pipeline object
        // stage is appended in order.
        let c: MongoCdcSourceConfig = serde_json::from_value(json!({
            "connection_uri": "mongodb://h/?replicaSet=rs0",
            "scope": { "type": "cluster" },
            "operation_types": ["insert"],
            "aggregation_pipeline": [
                { "$match": { "fullDocument.tier": "gold" } },
                { "$project": { "fullDocument._id": 1 } }
            ]
        }))
        .unwrap();
        let p = build_pipeline(&c).unwrap();
        assert_eq!(p.len(), 3, "operation $match + two user stages");
        // First is the operation-type $match.
        assert!(
            p[0].get_document("$match")
                .unwrap()
                .contains_key("operationType")
        );
        // User object stages preserved verbatim, in order.
        assert!(p[1].contains_key("$match"));
        assert!(p[2].contains_key("$project"));
    }

    #[test]
    fn pipeline_object_stages_without_operation_match() {
        // No operation_types → no leading $match; only the user object stage.
        let c: MongoCdcSourceConfig = serde_json::from_value(json!({
            "connection_uri": "mongodb://h/?replicaSet=rs0",
            "scope": { "type": "cluster" },
            "aggregation_pipeline": [ { "$project": { "_id": 1 } } ]
        }))
        .unwrap();
        let p = build_pipeline(&c).unwrap();
        assert_eq!(p.len(), 1);
        assert!(p[0].contains_key("$project"));
    }

    #[test]
    fn full_document_type_mapping() {
        use crate::config::FullDocument as F;
        assert!(full_document_type(F::Off).is_none());
        assert!(matches!(
            full_document_type(F::WhenAvailable),
            Some(FullDocumentType::WhenAvailable)
        ));
        assert!(matches!(
            full_document_type(F::Required),
            Some(FullDocumentType::Required)
        ));
        assert!(matches!(
            full_document_type(F::UpdateLookup),
            Some(FullDocumentType::UpdateLookup)
        ));
    }

    #[test]
    fn full_document_before_type_mapping() {
        use crate::config::FullDocumentBeforeChange as F;
        assert!(full_document_before_type(F::Off).is_none());
        assert!(matches!(
            full_document_before_type(F::WhenAvailable),
            Some(FullDocumentBeforeChangeType::WhenAvailable)
        ));
        assert!(matches!(
            full_document_before_type(F::Required),
            Some(FullDocumentBeforeChangeType::Required)
        ));
    }

    // dataset_uri is a pure-config method; MongoCdcSource requires a live
    // MongoDB async constructor, so we verify the logic directly using config
    // deserialization (which is sync).
    #[test]
    fn an_abrupt_end_flushes_only_what_precedes_the_open_transaction() {
        let mut buffer = vec![json!(1), json!(2), json!(3)];
        let page =
            complete_prefix(&mut buffer, Some(json!("b3")), Some((1, Some(json!("b1"))))).unwrap();
        assert_eq!(page.records, vec![json!(1)]);
        assert_eq!(page.bookmark, Some(json!("b1")));
        assert!(buffer.is_empty());

        let mut buffer = vec![json!(1)];
        assert!(complete_prefix(&mut buffer, Some(json!("b1")), Some((0, None))).is_none());

        let mut buffer = vec![json!(1), json!(2)];
        let page = complete_prefix(&mut buffer, Some(json!("b2")), None).unwrap();
        assert_eq!(page.records.len(), 2);
        assert_eq!(page.bookmark, Some(json!("b2")));

        assert!(complete_prefix(&mut Vec::new(), None, None).is_none());
    }

    #[test]
    fn earliest_falls_back_with_a_warning_for_each_reason() {
        let ts = Timestamp {
            time: 7,
            increment: 2,
        };
        assert_eq!(
            earliest_from(Ok(Some(bson::doc! { "ts": ts })), false),
            (ts, None)
        );
        for (found, sharded, reason) in [
            (Ok(Some(bson::doc! { "ts": 1 })), false, "has no timestamp"),
            (Ok(None), true, "sharded cluster"),
            (Ok(None), false, "oplog is empty"),
            (Err("denied".to_string()), false, "(denied)"),
        ] {
            let (got, warning) = earliest_from(found, sharded);
            assert_eq!(got, EARLIEST_FALLBACK);
            let warning = warning.unwrap();
            assert!(warning.contains(reason), "{warning}");
        }
    }

    #[test]
    fn dataset_uri_cluster_scope() {
        let c: MongoCdcSourceConfig = serde_json::from_value(json!({
            "connection_uri": "mongodb://u:p@h:27017/?replicaSet=rs0",
            "scope": { "type": "cluster" }
        }))
        .unwrap();
        assert_eq!(dataset_uri_for(&c), "mongodb://h:27017");
    }

    #[test]
    fn dataset_uri_collection_and_database_scope() {
        let mut c: MongoCdcSourceConfig = serde_json::from_value(json!({
            "connection_uri": "mongodb://u:p@h:27017/admin?replicaSet=rs0",
            "scope": { "type": "collection", "database": "mydb", "collection": "orders" }
        }))
        .unwrap();
        assert_eq!(dataset_uri_for(&c), "mongodb://h:27017/mydb/orders");
        c.scope = crate::config::Scope::Database {
            database: "mydb".into(),
        };
        assert_eq!(dataset_uri_for(&c), "mongodb://h:27017/mydb");
    }
}
