//! Spanner change-stream executor.
//!
//! One fetch cycle is **bounded**, like the other CDC sources: it captures
//! `run_end = now`, reads every unfinished partition from its stored position
//! up to `run_end` (several partitions in parallel), and ends. Each partition
//! reader runs as a task and sends *complete source transactions* over a
//! bounded channel; this module's single consumer loop owns the
//! [`Scheduler`], buffers records, and cuts pages. A page's bookmark is the
//! scheduler snapshot taken at the moment the page is yielded, so it always
//! agrees with the records delivered so far.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use faucet_common_spanner::decode::row_to_json;
use faucet_core::{FaucetError, Source, Stream, StreamPage};
use gcloud_spanner::client::Client;
use gcloud_spanner::statement::Statement;
use gcloud_spanner::transaction::QueryOptions;
use serde_json::Value;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinSet;

use crate::config::{SpannerCdcSourceConfig, StartFrom, parse_timestamp};
use crate::envelope::to_envelopes;
use crate::record::{ChangeRecord, ChildPartition, parse_googlesql_row, parse_pg_row};
use crate::state::{Bookmark, STATE_SCHEMA, Scheduler};
use faucet_common_spanner::SpannerDialect;

/// Events a partition reader may have in flight before it blocks (back-pressure
/// against a slow sink).
const EVENT_CHANNEL: usize = 64;

/// What a partition reader reports to the consumer loop.
#[derive(Debug)]
enum Event {
    /// A complete source transaction (all of its records in this partition).
    Txn {
        token: Option<String>,
        commit: DateTime<Utc>,
        envelopes: Vec<Value>,
    },
    /// Everything in the partition up to `ts` has been delivered.
    Heartbeat {
        token: Option<String>,
        ts: DateTime<Utc>,
    },
    /// The partition ended and announced its children.
    Ended {
        token: Option<String>,
        children: Vec<ChildPartition>,
    },
    /// The read reached the end of the run.
    CaughtUp { token: Option<String> },
    /// The read produced nothing for `idle_timeout`; resume it next run.
    Stalled { token: Option<String> },
    /// The read failed.
    Failed { error: FaucetError },
}

/// What the consumer loop woke up for.
enum Next {
    Event(Option<Event>),
    ReaderDone,
    ReaderDied(String),
    NoReaders,
}

/// Everything a partition reader needs (cheap to share).
struct ReadCtx {
    client: Client,
    dialect: SpannerDialect,
    stream: String,
    heartbeat_ms: i64,
    idle_timeout: Duration,
    run_end: DateTime<Utc>,
}

/// A configured Spanner change-stream source.
pub struct SpannerCdcSource {
    config: SpannerCdcSourceConfig,
    client: Client,
    state_key_value: String,
    pending_bookmark: Mutex<Option<Bookmark>>,
    /// Microseconds since the epoch of the oldest unread position (the
    /// bookmark watermark). `i64::MIN` = unknown. Where [`Source::lag`]
    /// measures from.
    watermark_micros: AtomicI64,
}

impl SpannerCdcSource {
    /// Connect, validate the config and the target stream, and build the source.
    pub async fn new(config: SpannerCdcSourceConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let client = config.connection.connect().await?;
        verify_stream(&client, config.connection.dialect, &config.change_stream).await?;
        let state_key_value = config.effective_state_key();
        Ok(Self {
            config,
            client,
            state_key_value,
            pending_bookmark: Mutex::new(None),
            watermark_micros: AtomicI64::new(i64::MIN),
        })
    }

    fn note_watermark(&self, at: DateTime<Utc>) {
        self.watermark_micros
            .store(at.timestamp_micros(), Ordering::Relaxed);
    }
}

// ── helpers ─────────────────────────────────────────────────────────────────

fn to_prost(ts: DateTime<Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: ts.timestamp(),
        nanos: ts.timestamp_subsec_nanos() as i32,
    }
}

/// `now`, truncated to Spanner's microsecond precision.
fn now_micros() -> DateTime<Utc> {
    DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap_or_else(Utc::now)
}

/// The text of the change-stream query for `dialect`.
///
/// GoogleSQL exposes the stream as the table-valued function
/// `READ_<stream>(...)` returning one `ChangeRecord` column. The PostgreSQL
/// dialect exposes `spanner.read_json_<stream>(...)`, which returns one JSONB
/// record per row; its fifth argument is the (unused) read options.
fn change_stream_sql(dialect: SpannerDialect, stream: &str) -> String {
    match dialect {
        SpannerDialect::GoogleStandardSql => format!(
            "SELECT ChangeRecord FROM READ_{stream}(@start_timestamp, @end_timestamp, \
             @partition_token, @heartbeat_milliseconds)"
        ),
        SpannerDialect::Postgresql => {
            format!("SELECT * FROM spanner.read_json_{stream}($1, $2, $3, $4, null)")
        }
    }
}

/// The change-stream query for one partition. `stream` has already been
/// validated as a plain identifier, so interpolating it is safe.
fn change_stream_statement(
    dialect: SpannerDialect,
    stream: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    token: &Option<String>,
    heartbeat_ms: i64,
) -> Statement {
    let mut st = Statement::new(change_stream_sql(dialect, stream));
    // GoogleSQL binds by name; PostgreSQL binds `$1..$4` as `p1..p4`.
    let name = |n: &str, i: usize| dialect.bind_name(n, i);
    st.add_param(&name("start_timestamp", 1), &to_prost(from));
    st.add_param(&name("end_timestamp", 2), &to_prost(to));
    st.add_param(&name("partition_token", 3), token);
    st.add_param(&name("heartbeat_milliseconds", 4), &heartbeat_ms);
    st
}

fn spanner_err(what: &str, e: impl std::fmt::Display) -> FaucetError {
    FaucetError::Source(format!("spanner-cdc: {what}: {e}"))
}

/// Map the server's "start is too old" rejection to an actionable message.
fn explain_query_error(e: &gcloud_gax::grpc::Status, from: DateTime<Utc>) -> FaucetError {
    if e.code() == gcloud_gax::grpc::Code::OutOfRange {
        return FaucetError::Source(format!(
            "spanner-cdc: the change stream cannot be read from {from}: {}. The position is \
             older than the stream's retention period (or before the stream was created). \
             Re-seed the pipeline with `start_from` and a fresh state.",
            e.message()
        ));
    }
    spanner_err("change stream query failed", e)
}

/// Run a small metadata query and decode its rows.
async fn query_rows(client: &Client, stmt: Statement) -> Result<Vec<Value>, FaucetError> {
    let mut tx = client
        .single()
        .await
        .map_err(|e| spanner_err("begin read-only transaction", e))?;
    let mut iter = tx
        .query(stmt)
        .await
        .map_err(|e| spanner_err("metadata query failed", e))?;
    let mut rows = Vec::new();
    while let Some(row) = iter
        .next()
        .await
        .map_err(|e| spanner_err("metadata row stream failed", e))?
    {
        let fields = iter.columns_metadata().clone();
        rows.push(
            row_to_json(&row, &fields).map_err(|e| spanner_err("metadata row decode failed", e))?,
        );
    }
    Ok(rows)
}

/// Refuse stream options this release cannot read correctly.
///
/// `MUTABLE_KEY_RANGE` streams re-partition by key range at runtime and need a
/// different (token-merging) read protocol; the default `IMMUTABLE_KEY_RANGE`
/// mode is what this source implements.
pub(crate) fn check_stream_options(
    stream: &str,
    options: &[(String, String)],
) -> Result<(), FaucetError> {
    for (name, value) in options {
        if name.eq_ignore_ascii_case("partition_mode")
            && value
                .trim_matches('\'')
                .eq_ignore_ascii_case("MUTABLE_KEY_RANGE")
        {
            return Err(FaucetError::Config(format!(
                "spanner-cdc: change stream `{stream}` uses partition_mode = \
                 MUTABLE_KEY_RANGE, which this source does not support yet. Recreate the \
                 stream with the default IMMUTABLE_KEY_RANGE mode."
            )));
        }
    }
    Ok(())
}

/// Fail fast on a wrong dialect, a missing stream, or an unsupported mode.
async fn verify_stream(
    client: &Client,
    dialect: SpannerDialect,
    stream: &str,
) -> Result<(), FaucetError> {
    // Plain SQL with no parameters parses in both dialects, so a dialect
    // mismatch yields a clear error rather than a syntax error from the first
    // dialect-specific query.
    let actual = query_rows(
        client,
        Statement::new(
            "SELECT option_value AS v FROM information_schema.database_options \
             WHERE option_name = 'database_dialect'",
        ),
    )
    .await?;
    let is_pg = actual.iter().any(|r| {
        r["v"]
            .as_str()
            .is_some_and(|v| v.eq_ignore_ascii_case("POSTGRESQL"))
    });
    if is_pg && !dialect.is_postgresql() {
        return Err(FaucetError::Config(
            "spanner-cdc: this database uses the PostgreSQL dialect; set `dialect: postgresql` \
             in the source config"
                .into(),
        ));
    }
    if !is_pg && dialect.is_postgresql() {
        return Err(FaucetError::Config(
            "spanner-cdc: `dialect: postgresql` is set but this database uses GoogleSQL; remove \
             `dialect` or point at a PostgreSQL-dialect database"
                .into(),
        ));
    }

    let name_param = dialect.placeholder("name", 1);
    let mut st = Statement::new(format!(
        "SELECT change_stream_name AS n FROM information_schema.change_streams \
         WHERE change_stream_name = {name_param}"
    ));
    st.add_param(&dialect.bind_name("name", 1), &stream.to_string());
    if query_rows(client, st).await?.is_empty() {
        return Err(FaucetError::Config(format!(
            "spanner-cdc: change stream `{stream}` does not exist in this database \
             (create it with `CREATE CHANGE STREAM {stream} FOR ...`)"
        )));
    }

    let mut st = Statement::new(format!(
        "SELECT option_name AS k, option_value AS v FROM information_schema.change_stream_options \
         WHERE change_stream_name = {name_param}"
    ));
    st.add_param(&dialect.bind_name("name", 1), &stream.to_string());
    let options: Vec<(String, String)> = query_rows(client, st)
        .await?
        .iter()
        .filter_map(|r| Some((r["k"].as_str()?.to_string(), r["v"].as_str()?.to_string())))
        .collect();
    check_stream_options(stream, &options)
}

/// Enforce the in-memory staging cap before buffering `incoming` more records.
pub(crate) fn check_staging_cap(
    current_len: usize,
    incoming: usize,
    max_staged: Option<usize>,
) -> Result<(), FaucetError> {
    if let Some(max) = max_staged
        && current_len + incoming > max
    {
        return Err(FaucetError::Source(format!(
            "spanner-cdc: in-memory change buffer exceeded max_staged_records ({max}); \
             aborting to avoid unbounded memory growth. Raise max_staged_records or \
             reduce batch_size so pages flush more often."
        )));
    }
    Ok(())
}

// ── partition reader ────────────────────────────────────────────────────────

/// Read one partition to the end of the run, reporting through `tx`.
async fn read_partition(
    ctx: Arc<ReadCtx>,
    tx: mpsc::Sender<Event>,
    token: Option<String>,
    from: DateTime<Utc>,
) {
    let outcome = read_partition_inner(&ctx, &tx, &token, from).await;
    let event = match outcome {
        Ok(Some(ev)) => ev,
        // The receiver is gone (run cancelled): nothing left to report.
        Ok(None) => return,
        Err(error) => Event::Failed { error },
    };
    let _ = tx.send(event).await;
}

/// Returns the terminal event, or `None` when the consumer has gone away.
async fn read_partition_inner(
    ctx: &ReadCtx,
    tx: &mpsc::Sender<Event>,
    token: &Option<String>,
    from: DateTime<Utc>,
) -> Result<Option<Event>, FaucetError> {
    let stmt = change_stream_statement(
        ctx.dialect,
        &ctx.stream,
        from,
        ctx.run_end,
        token,
        ctx.heartbeat_ms,
    );
    let mut txn = ctx
        .client
        .single()
        .await
        .map_err(|e| spanner_err("begin read-only transaction", e))?;
    // `next()` is raced against the idle timeout below, so transparent stream
    // resumption (which makes `next()` non-cancel-safe) is off, as in the
    // Spanner query source.
    let opts = QueryOptions {
        enable_resume: false,
        ..Default::default()
    };
    let mut iter = txn
        .query_with_option(stmt, opts)
        .await
        .map_err(|e| explain_query_error(&e, from))?;

    // Records of the transaction being read; sent only once it is complete, so
    // the consumer never sees (and never bookmarks past) half a transaction.
    let mut pending: Vec<Value> = Vec::new();
    loop {
        let row = match tokio::time::timeout(ctx.idle_timeout, iter.next()).await {
            Err(_elapsed) => {
                return Ok(Some(Event::Stalled {
                    token: token.clone(),
                }));
            }
            Ok(Err(e)) => return Err(explain_query_error(&e, from)),
            Ok(Ok(None)) => {
                return Ok(Some(Event::CaughtUp {
                    token: token.clone(),
                }));
            }
            Ok(Ok(Some(row))) => row,
        };
        let fields = iter.columns_metadata().clone();
        let json = row_to_json(&row, &fields)
            .map_err(|e| spanner_err("change record decode failed", e))?;
        let records = if ctx.dialect.is_postgresql() {
            parse_pg_row(&json)?
        } else {
            parse_googlesql_row(&json)?
        };
        for record in records {
            match record {
                ChangeRecord::Data(change) => {
                    pending.extend(to_envelopes(&change));
                    if change.is_last_in_transaction {
                        let ev = Event::Txn {
                            token: token.clone(),
                            commit: change.commit_timestamp,
                            envelopes: std::mem::take(&mut pending),
                        };
                        if tx.send(ev).await.is_err() {
                            return Ok(None);
                        }
                    }
                }
                ChangeRecord::Heartbeat(ts) => {
                    let ev = Event::Heartbeat {
                        token: token.clone(),
                        ts,
                    };
                    if tx.send(ev).await.is_err() {
                        return Ok(None);
                    }
                }
                ChangeRecord::Children(children) => {
                    return Ok(Some(Event::Ended {
                        token: token.clone(),
                        children,
                    }));
                }
            }
        }
    }
}

// ── Source ──────────────────────────────────────────────────────────────────

#[async_trait]
impl Source for SpannerCdcSource {
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
        _ctx: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        self.stream_pages_impl(self.config.batch_size)
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(schemars::schema_for!(SpannerCdcSourceConfig)).unwrap_or(Value::Null)
    }

    fn state_key(&self) -> Option<String> {
        Some(self.state_key_value.clone())
    }

    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        let bm = Bookmark::from_value(bookmark)?;
        self.note_watermark(bm.watermark);
        *self.pending_bookmark.lock().await = Some(bm);
        Ok(())
    }

    /// Seconds between now and the oldest position not yet delivered.
    async fn lag(&self) -> Result<Option<faucet_core::SourceLag>, FaucetError> {
        let micros = self.watermark_micros.load(Ordering::Relaxed);
        if micros == i64::MIN {
            return Ok(None);
        }
        let behind = (Utc::now().timestamp_micros() - micros).max(0) as f64 / 1_000_000.0;
        Ok(Some(faucet_core::SourceLag::seconds(behind)))
    }

    fn state_schema(&self) -> u32 {
        STATE_SCHEMA
    }

    fn migrate_state(&self, from: u32, data: Value) -> Result<Value, FaucetError> {
        crate::state::migrate_state(from, data)
    }

    fn record_table(&self, record: &Value) -> Option<String> {
        record.get("table")?.as_str().map(str::to_string)
    }

    fn connector_name(&self) -> &'static str {
        "spanner-cdc"
    }

    fn dataset_uri(&self) -> String {
        format!(
            "spanner://{}/{}/{}/change_streams/{}",
            self.config.connection.project_id,
            self.config.connection.instance,
            self.config.connection.database,
            self.config.change_stream
        )
    }

    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};
        let start = std::time::Instant::now();
        let result = tokio::time::timeout(
            ctx.timeout,
            verify_stream(
                &self.client,
                self.config.connection.dialect,
                &self.config.change_stream,
            ),
        )
        .await;
        Ok(match result {
            Ok(Ok(())) => CheckReport {
                probes: vec![
                    Probe::pass("connection", start.elapsed()),
                    Probe::pass("change_stream", start.elapsed()),
                ],
            },
            Ok(Err(FaucetError::Config(msg))) => CheckReport {
                probes: vec![
                    Probe::pass("connection", start.elapsed()),
                    Probe::fail_hint(
                        "change_stream",
                        start.elapsed(),
                        msg,
                        "check the stream name, its partition mode, and the database dialect",
                    ),
                ],
            },
            Ok(Err(e)) => CheckReport::single(Probe::fail_hint(
                "connection",
                start.elapsed(),
                e.to_string(),
                "verify the project / instance / database and the credentials",
            )),
            Err(_) => CheckReport::single(Probe::fail_hint(
                "connection",
                start.elapsed(),
                "connection timed out",
                "Spanner did not respond within the check timeout",
            )),
        })
    }
}

impl SpannerCdcSource {
    fn stream_pages_impl<'a>(
        &'a self,
        batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let max_staged = self.config.max_staged_records;
        let max_concurrent = self.config.max_concurrent_partitions;
        let per_batch = batch_size != 0;
        let chunk = if per_batch { batch_size } else { usize::MAX };

        Box::pin(async_stream::try_stream! {
            let run_end = now_micros();
            let pending = self.pending_bookmark.lock().await.take();
            let start_bookmark = match pending {
                // A persisted bookmark always wins over `start_from`.
                Some(bm) => bm,
                None => match &self.config.start_from {
                    StartFrom::Timestamp { timestamp } => {
                        let at = parse_timestamp(timestamp).map_err(|e| {
                            FaucetError::Config(format!(
                                "spanner-cdc: start_from.timestamp {timestamp:?}: {e}"
                            ))
                        })?;
                        Bookmark::root_at(at)
                    }
                    StartFrom::Now => {
                        // Nothing to read yet: persist "start here" so the next
                        // run picks up from this instant, then end the cycle.
                        let anchor = Bookmark::root_at(run_end);
                        self.note_watermark(anchor.watermark);
                        yield StreamPage {
                            records: Vec::new(),
                            bookmark: Some(anchor.to_value()?),
                        };
                        return;
                    }
                },
            };

            let mut sched = Scheduler::from_bookmark(&start_bookmark, run_end);
            let ctx = Arc::new(ReadCtx {
                dialect: self.config.connection.dialect,
                client: self.client.clone(),
                stream: self.config.change_stream.clone(),
                heartbeat_ms: i64::from(self.config.heartbeat_ms),
                idle_timeout: self.config.idle_timeout,
                run_end,
            });
            let (tx, mut rx) = mpsc::channel::<Event>(EVENT_CHANNEL);
            // Dropping the set (run cancelled, error, or finished) aborts any
            // reader still in flight.
            let mut tasks: JoinSet<()> = JoinSet::new();
            let mut buffer: Vec<Value> = Vec::new();

            loop {
                while sched.running() < max_concurrent {
                    match sched.next_ready() {
                        Some(a) => {
                            tasks.spawn(read_partition(ctx.clone(), tx.clone(), a.token, a.read_from));
                        }
                        None => break,
                    }
                }
                if sched.running() == 0 {
                    break;
                }

                let next = tokio::select! {
                    biased;
                    ev = rx.recv() => Next::Event(ev),
                    joined = tasks.join_next() => match joined {
                        Some(Ok(())) => Next::ReaderDone,
                        Some(Err(e)) => Next::ReaderDied(e.to_string()),
                        None => Next::NoReaders,
                    },
                };
                let event = match next {
                    Next::Event(ev) => ev,
                    // A reader finished; its terminal event is already (or
                    // about to be) in the channel.
                    Next::ReaderDone => continue,
                    Next::ReaderDied(why) => {
                        Err(FaucetError::Source(format!(
                            "spanner-cdc: partition reader died: {why}"
                        )))?;
                        break;
                    }
                    Next::NoReaders => {
                        Err(FaucetError::Source(
                            "spanner-cdc: partition readers exited without reporting".into(),
                        ))?;
                        break;
                    }
                };
                let Some(event) = event else { break };

                match event {
                    Event::Txn { token, commit, envelopes } => {
                        check_staging_cap(buffer.len(), envelopes.len(), max_staged)?;
                        // A page is cut only *between* transactions. Flush what
                        // is buffered first when this transaction would push the
                        // page past `batch_size`, so a page exceeds the cap only
                        // when a single transaction alone is larger than it.
                        if per_batch && !buffer.is_empty() && buffer.len() + envelopes.len() > chunk {
                            let snap = sched.snapshot();
                            self.note_watermark(snap.watermark);
                            yield StreamPage {
                                records: std::mem::take(&mut buffer),
                                bookmark: Some(snap.to_value()?),
                            };
                        }
                        buffer.extend(envelopes);
                        sched.advance(&token, commit);
                        if per_batch && buffer.len() >= chunk {
                            let snap = sched.snapshot();
                            self.note_watermark(snap.watermark);
                            yield StreamPage {
                                records: std::mem::take(&mut buffer),
                                bookmark: Some(snap.to_value()?),
                            };
                        }
                    }
                    Event::Heartbeat { token, ts } => sched.advance(&token, ts),
                    Event::Ended { token, children } => sched.ended(&token, &children),
                    Event::CaughtUp { token } => sched.caught_up(&token),
                    Event::Stalled { token } => {
                        tracing::warn!(
                            connector = "spanner-cdc",
                            partition = token.as_deref().unwrap_or("<root>"),
                            "partition read idle past idle_timeout; it resumes next run"
                        );
                        sched.stalled(&token);
                    }
                    Event::Failed { error, .. } => {
                        Err(error)?;
                    }
                }
            }

            // Final page: always carries the bookmark so progress (heartbeats,
            // partition ends) is persisted even on a quiet run.
            let snap = sched.snapshot();
            self.note_watermark(snap.watermark);
            yield StreamPage {
                records: std::mem::take(&mut buffer),
                bookmark: Some(snap.to_value()?),
            };
            tracing::info!(connector = "spanner-cdc", "change stream fetch cycle complete");
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn default_stream_options_are_accepted() {
        assert!(check_stream_options("s", &[]).is_ok());
        assert!(
            check_stream_options(
                "s",
                &opts(&[
                    ("retention_period", "'7d'"),
                    ("value_capture_type", "'NEW_ROW'")
                ])
            )
            .is_ok()
        );
        assert!(
            check_stream_options("s", &opts(&[("partition_mode", "IMMUTABLE_KEY_RANGE")])).is_ok()
        );
    }

    #[test]
    fn mutable_key_range_is_refused_with_a_config_error() {
        for v in [
            "MUTABLE_KEY_RANGE",
            "'MUTABLE_KEY_RANGE'",
            "mutable_key_range",
        ] {
            let err = check_stream_options("s", &opts(&[("PARTITION_MODE", v)])).unwrap_err();
            assert!(matches!(err, FaucetError::Config(_)), "{v}");
            assert!(err.to_string().contains("MUTABLE_KEY_RANGE"));
        }
    }

    #[test]
    fn staging_cap_none_is_unbounded() {
        assert!(check_staging_cap(usize::MAX / 2, 1, None).is_ok());
    }

    #[test]
    fn staging_cap_allows_up_to_the_limit_and_aborts_beyond() {
        assert!(check_staging_cap(5, 5, Some(10)).is_ok());
        let err = check_staging_cap(5, 6, Some(10)).unwrap_err();
        assert!(matches!(err, FaucetError::Source(_)));
        assert!(err.to_string().contains("max_staged_records"));
    }

    #[test]
    fn timestamps_convert_to_prost_without_losing_micros() {
        let ts = parse_timestamp("2026-10-12T08:05:15.454853Z").unwrap();
        let p = to_prost(ts);
        assert_eq!(p.seconds, ts.timestamp());
        assert_eq!(p.nanos, 454_853_000);
    }

    #[test]
    fn now_micros_has_no_sub_microsecond_part() {
        assert_eq!(now_micros().timestamp_subsec_nanos() % 1_000, 0);
    }

    #[test]
    fn change_stream_query_text_names_the_stream_function() {
        let sql = change_stream_sql(SpannerDialect::GoogleStandardSql, "orders_cs");
        assert!(sql.starts_with("SELECT ChangeRecord FROM READ_orders_cs("));
        assert!(sql.contains("@partition_token"));
        // Building the statement with every param type must not panic.
        let ts = parse_timestamp("2026-10-12T08:05:15Z").unwrap();
        let _ = change_stream_statement(
            SpannerDialect::GoogleStandardSql,
            "orders_cs",
            ts,
            ts,
            &Some("tok".into()),
            5000,
        );
        let _ = change_stream_statement(
            SpannerDialect::GoogleStandardSql,
            "orders_cs",
            ts,
            ts,
            &None,
            5000,
        );
    }

    #[test]
    fn postgresql_query_calls_the_json_function_with_positional_params() {
        assert_eq!(
            change_stream_sql(SpannerDialect::Postgresql, "orders_cs"),
            "SELECT * FROM spanner.read_json_orders_cs($1, $2, $3, $4, null)"
        );
        let ts = Utc::now();
        // Building the statement must not panic for either partition kind.
        let _ =
            change_stream_statement(SpannerDialect::Postgresql, "orders_cs", ts, ts, &None, 5000);
        let _ = change_stream_statement(
            SpannerDialect::Postgresql,
            "orders_cs",
            ts,
            ts,
            &Some("tok".into()),
            5000,
        );
    }
}
