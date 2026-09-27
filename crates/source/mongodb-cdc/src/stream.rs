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
}

/// Resolve the effective start position from config + any persisted bookmark.
///
/// Precedence: explicit `resume_token`/`timestamp` win over a persisted
/// bookmark; `now`/`earliest` yield to a persisted bookmark.
pub(crate) fn resolve_start(
    start_from: &StartFrom,
    pending: Option<&Bookmark>,
) -> Result<StartPosition, FaucetError> {
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
        StartFrom::Now | StartFrom::Earliest => {
            if let Some(b) = pending {
                // A bookmark captured from an invalidate event must resume with
                // `start_after` — `resume_after` on an invalidate token is
                // rejected by MongoDB and would wedge the pipeline (#321 M3).
                if b.invalidate {
                    Ok(StartPosition::StartAfter(b.to_token()?))
                } else {
                    Ok(StartPosition::ResumeAfter(b.to_token()?))
                }
            } else if matches!(start_from, StartFrom::Earliest) {
                // Earliest retained oplog entry (errors at open if rolled).
                Ok(StartPosition::AtOperationTime(Timestamp {
                    time: 1,
                    increment: 1,
                }))
            } else {
                Ok(StartPosition::Now)
            }
        }
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

/// Build the server-side pipeline: operation-type `$match` + user stages.
pub(crate) fn build_pipeline(config: &MongoCdcSourceConfig) -> Result<Vec<Document>, FaucetError> {
    let mut pipeline: Vec<Document> = Vec::new();
    if !config.operation_types.is_empty() {
        pipeline.push(bson::doc! {
            "$match": { "operationType": { "$in": config.operation_types.clone() } }
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
        let token = self.capture_now_token().await?;
        Ok(Some(Bookmark::from_token(&token)?.to_value()?))
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
            match self.first_unread_secs(&bm).await? {
                Some(secs) => position = secs,
                None => return Ok(None),
            }
        }
        let hello = self
            .client
            .database("admin")
            .run_command(bson::doc! { "hello": 1 })
            .await
            .map_err(|e| FaucetError::Source(format!("mongodb-cdc lag: hello failed: {e}")))?;
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

    fn connector_name(&self) -> &'static str {
        "mongodb-cdc"
    }

    fn dataset_uri(&self) -> String {
        use crate::config::Scope;
        let base = faucet_core::redact_uri_credentials(&self.config.connection_uri);
        match &self.config.scope {
            Scope::Collection {
                database,
                collection,
            } => {
                format!("{base}/{database}/{collection}")
            }
            Scope::Database { database } => format!("{base}/{database}"),
            Scope::Cluster => base,
        }
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
            use futures::StreamExt;

            let pending = self.pending_bookmark.lock().await.take();
            let start = resolve_start(&self.config.start_from, pending.as_ref())?;
            let pipeline = build_pipeline(&self.config)?;

            // Open the change stream scoped to collection, database, or cluster.
            // Each arm resolves to a ChangeStream<ChangeStreamEvent<Document>>.
            let change_stream: mongodb::change_stream::ChangeStream<
                mongodb::change_stream::event::ChangeStreamEvent<Document>,
            > = match &self.config.scope {
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
                        StartPosition::Now => w,
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
                        StartPosition::Now => w,
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
                        StartPosition::Now => w,
                        StartPosition::AtOperationTime(ts) => w.start_at_operation_time(*ts),
                        StartPosition::ResumeAfter(tok) => w.resume_after(tok.clone()),
                        StartPosition::StartAfter(tok) => w.start_after(tok.clone()),
                    };
                    w.await
                        .map_err(|e| FaucetError::Source(format!("mongodb-cdc watch failed: {e}")))?
                }
            };
            let mut change_stream = std::pin::pin!(change_stream);

            let chunk = if per_batch { batch_size } else { usize::MAX };
            let mut buffer: Vec<Value> = Vec::new();
            // Bookmark of the last record currently in `buffer`. `None` only when
            // the buffer is empty (no events seen this cycle).
            let mut last_bookmark: Option<Value> = None;

            // Graceful shutdown is handled one layer up: `run_stream`'s page loop
            // `biased`-`select!`s the pipeline cancel token (Pipeline::with_cancel,
            // #146) against polling this stream for its next page, so a cancel
            // mid-idle-wait stops at the page boundary and flushes the sinks — no
            // source-side signal handling needed. Nothing is persisted until a
            // page yields, so a dropped in-flight buffer is simply re-fetched
            // (the resume token has not advanced).
            loop {
                match tokio::time::timeout(idle_timeout, change_stream.next()).await {
                    Ok(Some(Ok(event))) => {
                        if let Some(ts) = &event.cluster_time {
                            self.note_position(u64::from(ts.time));
                        }
                        let bookmark = Bookmark::from_token(&event.id)?;
                        let is_invalidate = matches!(
                            event.operation_type,
                            mongodb::change_stream::event::OperationType::Invalidate
                        );
                        // OOM safety valve: with `batch_size: 0` (or `fetch_all`)
                        // the buffer is never flushed mid-cycle, so a
                        // high-throughput stream would grow it without bound.
                        // Abort with a typed error before staging one more record
                        // past the cap rather than risk an OOM-kill.
                        check_staging_cap(buffer.len(), max_staged)?;
                        buffer.push(to_envelope(&event, &bookmark)?);
                        // The persisted bookmark records whether it is an
                        // invalidate token, so resume uses `start_after` not
                        // `resume_after` (#321 M3). The envelope above keeps the
                        // plain token — only the durable state carries the flag.
                        let persisted = Bookmark {
                            invalidate: is_invalidate,
                            ..bookmark
                        };
                        last_bookmark = Some(persisted.to_value()?);

                        // An invalidate closes the stream server-side: flush the
                        // page (including the invalidate event) and end.
                        if is_invalidate {
                            yield StreamPage {
                                records: std::mem::take(&mut buffer),
                                bookmark: last_bookmark.take(),
                            };
                            break;
                        }
                        if per_batch && buffer.len() >= chunk {
                            yield StreamPage {
                                records: std::mem::take(&mut buffer),
                                bookmark: last_bookmark.take(),
                            };
                        }
                    }
                    Ok(Some(Err(e))) => {
                        Err(FaucetError::Source(format!("mongodb-cdc stream error: {e}")))?;
                    }
                    // Idle (timeout) or cursor closed: flush whatever we have and
                    // end this fetch cycle.
                    Ok(None) | Err(_) => {
                        // Nothing arrived for the idle window: the stream has
                        // caught up to the present. (The driver's resume token
                        // must not be read here — a timed-out `next()` leaves
                        // the stream mid-poll.)
                        self.note_position(unix_now_secs());
                        if !buffer.is_empty() {
                            yield StreamPage {
                                records: std::mem::take(&mut buffer),
                                bookmark: last_bookmark.take(),
                            };
                        }
                        break;
                    }
                }
            }

            tracing::info!(connector = "mongodb-cdc", "change stream fetch cycle complete");
        })
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

#[cfg(test)]
mod tests {

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

    #[test]
    fn explicit_timestamp_overrides_bookmark() {
        let pending = Bookmark {
            resume_token: json!({ "_data": "AA" }),
            ..Default::default()
        };
        let pos = resolve_start(
            &StartFrom::Timestamp {
                timestamp_secs: 100,
            },
            Some(&pending),
        )
        .unwrap();
        assert_eq!(
            pos,
            StartPosition::AtOperationTime(Timestamp {
                time: 100,
                increment: 0
            })
        );
    }

    #[test]
    fn explicit_resume_token_overrides_bookmark() {
        // An explicit ResumeToken in config must win over any persisted
        // bookmark and resolve to ResumeAfter(token).
        let pending = Bookmark {
            resume_token: json!({ "_data": "PENDING" }),
            ..Default::default()
        };
        let pos = resolve_start(
            &StartFrom::ResumeToken {
                token: json!({ "_data": "8264AB00" }),
            },
            Some(&pending),
        )
        .unwrap();
        // The resolved position must round-trip back to the configured token,
        // not the pending bookmark's token.
        match pos {
            StartPosition::ResumeAfter(tok) => {
                let b = Bookmark::from_token(&tok).unwrap();
                assert_eq!(b.resume_token["_data"], json!("8264AB00"));
            }
            other => panic!("expected ResumeAfter, got {other:?}"),
        }
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
    fn earliest_without_bookmark_uses_operation_time() {
        assert_eq!(
            resolve_start(&StartFrom::Earliest, None).unwrap(),
            StartPosition::AtOperationTime(Timestamp {
                time: 1,
                increment: 1
            })
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
    fn dataset_uri_cluster_scope() {
        let c: MongoCdcSourceConfig = serde_json::from_value(json!({
            "connection_uri": "mongodb://u:p@h:27017/?replicaSet=rs0",
            "scope": { "type": "cluster" }
        }))
        .unwrap();
        use crate::config::Scope;
        let base = faucet_core::redact_uri_credentials(&c.connection_uri);
        let uri = match &c.scope {
            Scope::Collection {
                database,
                collection,
            } => format!("{base}/{database}/{collection}"),
            Scope::Database { database } => format!("{base}/{database}"),
            Scope::Cluster => base,
        };
        assert_eq!(uri, "mongodb://h:27017/?replicaSet=rs0");
    }

    #[test]
    fn dataset_uri_collection_scope() {
        let c: MongoCdcSourceConfig = serde_json::from_value(json!({
            "connection_uri": "mongodb://u:p@h:27017/?replicaSet=rs0",
            "scope": { "type": "collection", "database": "mydb", "collection": "orders" }
        }))
        .unwrap();
        use crate::config::Scope;
        let base = faucet_core::redact_uri_credentials(&c.connection_uri);
        let uri = match &c.scope {
            Scope::Collection {
                database,
                collection,
            } => format!("{base}/{database}/{collection}"),
            Scope::Database { database } => format!("{base}/{database}"),
            Scope::Cluster => base,
        };
        assert_eq!(uri, "mongodb://h:27017/?replicaSet=rs0/mydb/orders");
    }
}
