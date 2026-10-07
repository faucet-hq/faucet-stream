//! `MysqlCdcSource` — public `Source` implementation.
//!
//! Tails the MySQL binary log via `mysql_async`'s async [`BinlogStream`] and
//! emits per-row change events as CDC envelopes.  Transactions are buffered
//! in memory (BEGIN → ROWS → COMMIT) and committed transactions are coalesced
//! into pages of up to `batch_size` records — a transaction is never split —
//! each carrying the bookmark of its last commit. XA transactions are held
//! until their outcome; savepoints never split a transaction.
//!
//! **Target engine:** primarily InnoDB / transactional tables, where commits
//! arrive as `XidEvent`.  Explicit `COMMIT` statements (emitted by
//! non-transactional / mixed-engine workloads as `QueryEvent("COMMIT")`) are
//! also handled as commit boundaries with identical durability semantics.
//!
//! **Bookmark strategy:** all persisted bookmarks use `{file, pos}` (the
//! end-position of the commit event).  Even when `start_position` is
//! `GtidSet`, the session resume is via file/pos after the first commit.
//! Assembling the full executed-GTID set from raw `GtidEvent` messages
//! across multiple sessions is fiddly (needs SID→interval accumulation
//! across runs), whereas file/pos is always available, unambiguous, and
//! fully resumable — the server still honours `gtid_mode` ordering
//! guarantees on the other side.  This choice is documented in the crate
//! README.

use crate::config::{CdcTls, MysqlCdcSourceConfig, StartPosition};
use crate::batch::PageBuilder;
use crate::convert::{binlog_row_to_json_hinted, column_hints, primary_key_columns};
use crate::query::{QueryKind, classify_query, parse_xa_prepare};
use crate::state::{Bookmark, state_key};
use async_trait::async_trait;
use faucet_core::{FaucetError, Source, Stream, StreamPage};
use mysql_async::binlog::events::{EventData, RowsEventData};
use mysql_async::prelude::Queryable;
use mysql_async::{BinlogStreamRequest, Conn, Opts, OptsBuilder, Row, SslOpts};
use serde_json::{Map, Value, json};
use std::collections::HashMap;
use std::path::PathBuf;
use std::pin::Pin;
use tokio::sync::Mutex;

/// A configured MySQL CDC (binlog replication) source.
///
/// Bookmarks are file/pos coordinates — see module-level note for the
/// rationale behind the always-file/pos bookmark strategy.
pub struct MysqlCdcSource {
    config: MysqlCdcSourceConfig,
    opts: Opts,
    state_key_value: String,
    /// Bookmark provided by `apply_start_bookmark`, applied at the start of
    /// the next fetch cycle to skip already-consumed events.
    pending_bookmark: Mutex<Option<Bookmark>>,
    /// Last binlog position handed to the pipeline on a page bookmark this
    /// run — where [`Source::lag`] measures from (#733).
    emitted: std::sync::Mutex<Option<(String, u64)>>,
}

impl MysqlCdcSource {
    fn note_emitted(&self, bm: &Bookmark) {
        if let (Bookmark::FilePos { file, pos }, Ok(mut g)) = (bm, self.emitted.lock()) {
            *g = Some((file.clone(), *pos));
        }
    }

    /// Build and preflight-check the source.
    ///
    /// Runs `config.validate()`, builds TLS-aware `Opts`, then opens a
    /// throwaway connection to verify binlog variables and user grants.
    pub async fn new(config: MysqlCdcSourceConfig) -> Result<Self, FaucetError> {
        config.validate()?;

        let opts = build_opts(&config)?;
        if let Some(warning) = tls_warning(&opts) {
            tracing::warn!(server_id = config.server_id, "mysql-cdc: {warning}");
        }
        let key = state_key(config.server_id);

        // Preflight: open + query + drop a throwaway connection.
        let mut conn = Conn::new(opts.clone())
            .await
            .map_err(|e| FaucetError::Source(format!("mysql-cdc: cannot connect: {e}")))?;

        run_preflight(&mut conn, &config).await?;
        drop(conn);

        Ok(Self {
            config,
            opts,
            state_key_value: key,
            pending_bookmark: Mutex::new(None),
            emitted: std::sync::Mutex::new(None),
        })
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Source impl
// ──────────────────────────────────────────────────────────────────────────────

#[async_trait]
impl Source for MysqlCdcSource {
    /// Drain the stream using the per-source `batch_size = 0` sentinel so
    /// all transactions are accumulated into a single trailing page —
    /// matching the historical convenience API contract.
    async fn fetch_with_context(
        &self,
        ctx: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        use futures::StreamExt;
        let mut pages = self.stream_pages_impl(ctx, 0);
        let mut all = Vec::new();
        while let Some(page) = pages.next().await {
            all.extend(page?.records);
        }
        Ok(all)
    }

    /// Streaming: committed transactions are grouped into pages of up to the
    /// config's `batch_size` records (never splitting a transaction) or after
    /// one second, each with `bookmark = Some(file_pos)` of its last commit.
    /// The trait-level `batch_size` argument is ignored in favour of the config
    /// field.
    fn stream_pages<'a>(
        &'a self,
        ctx: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        self.stream_pages_impl(ctx, self.config.batch_size)
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(schemars::schema_for!(MysqlCdcSourceConfig)).unwrap_or(Value::Null)
    }

    fn state_key(&self) -> Option<String> {
        Some(self.state_key_value.clone())
    }

    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        let b = Bookmark::from_value(bookmark)?;
        *self.pending_bookmark.lock().await = Some(b);
        Ok(())
    }

    async fn capture_resume_position(&self) -> Result<Option<Value>, FaucetError> {
        let mut conn = Conn::new(self.opts.clone()).await.map_err(|e| {
            FaucetError::Source(format!("mysql-cdc: capture_position connect: {e}"))
        })?;
        let (file, pos) = current_binlog_position(&mut conn).await?;
        drop(conn);
        Ok(Some(Bookmark::FilePos { file, pos }.to_value()?))
    }

    async fn lag(&self) -> Result<Option<faucet_core::SourceLag>, FaucetError> {
        let emitted = self.emitted.lock().ok().and_then(|g| g.clone());
        let from = match emitted {
            Some(p) => Some(p),
            None => match &*self.pending_bookmark.lock().await {
                Some(Bookmark::FilePos { file, pos }) => Some((file.clone(), *pos)),
                _ => None,
            },
        };
        let Some((file, pos)) = from else {
            return Ok(None);
        };
        let mut conn = Conn::new(self.opts.clone())
            .await
            .map_err(|e| FaucetError::Source(format!("mysql-cdc: lag connect: {e}")))?;
        let head = current_binlog_position(&mut conn).await?;
        let logs: Vec<Row> = conn
            .query("SHOW BINARY LOGS")
            .await
            .map_err(|e| FaucetError::Source(format!("mysql-cdc: SHOW BINARY LOGS: {e}")))?;
        drop(conn);
        let logs: Vec<(String, u64)> = logs
            .into_iter()
            .filter_map(|r| Some((r.get::<String, _>(0)?, r.get::<u64, _>(1)?)))
            .collect();
        Ok(binlog_distance(&logs, (&file, pos), (&head.0, head.1))
            .map(faucet_core::SourceLag::bytes))
    }

    fn supports_exactly_once(&self) -> bool {
        // Durable monotonic binlog file/pos + deterministic replay from it +
        // per-transaction (per-page) bookmarks — the requirements for
        // exactly-once delivery.
        true
    }

    fn connector_name(&self) -> &'static str {
        "mysql-cdc"
    }

    fn record_table(&self, record: &Value) -> Option<String> {
        schema_table(record)
    }

    fn position_le(&self, a: &Value, b: &Value) -> Option<bool> {
        bookmark_le(a, b)
    }

    fn dataset_uri(&self) -> String {
        let base = faucet_core::redact_uri_credentials(&self.config.connection_url);
        if self.config.include_tables.is_empty() {
            base
        } else {
            format!("{base}?tables={}", self.config.include_tables.join(","))
        }
    }

    /// Preflight probe that does **not** open the binlog stream.
    ///
    /// Runs two probes bounded by `ctx.timeout`:
    /// - `connection`: can we connect + authenticate?
    /// - `binlog-config`: are the required server variables set?
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};
        let start = std::time::Instant::now();

        let probe_result = tokio::time::timeout(ctx.timeout, async {
            let mut conn = Conn::new(self.opts.clone()).await.map_err(|e| {
                Probe::fail_hint(
                    "connection",
                    start.elapsed(),
                    format!("could not connect: {e}"),
                    "verify the host is reachable and credentials are valid",
                )
            })?;

            let connection = Probe::pass("connection", start.elapsed());

            let binlog_config = match run_preflight_probes(&mut conn, &self.config).await {
                Ok(_summary) => Probe::pass("binlog-config", start.elapsed()),
                Err(msg) => Probe::fail_hint(
                    "binlog-config",
                    start.elapsed(),
                    msg,
                    "Set binlog_format=ROW, binlog_row_image=FULL, binlog_row_metadata=FULL \
                     and grant REPLICATION SLAVE + REPLICATION CLIENT",
                ),
            };

            Ok::<(Probe, Probe), Probe>((connection, binlog_config))
        })
        .await;

        match probe_result {
            Ok(Ok((conn_probe, cfg_probe))) => Ok(CheckReport {
                probes: vec![conn_probe, cfg_probe],
            }),
            Ok(Err(probe)) => Ok(CheckReport::single(probe)),
            Err(_elapsed) => Ok(CheckReport::single(Probe::fail_hint(
                "connection",
                start.elapsed(),
                "connection timed out",
                "the database did not respond within the check timeout",
            ))),
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Stream loop
// ──────────────────────────────────────────────────────────────────────────────

impl MysqlCdcSource {
    fn stream_pages_impl<'a>(
        &'a self,
        _ctx: &'a HashMap<String, Value>,
        batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let idle_timeout = self.config.idle_timeout;

        Box::pin(async_stream::try_stream! {
            use futures::StreamExt;

            // 1. Resolve start position for this fetch cycle.
            let pending = self.pending_bookmark.lock().await.take();
            let resolved = resolve_start(&self.config.start_position, pending.as_ref());
            let anchor_start = pending.is_none() && matches!(resolved, ResolvedStart::Current { .. });

            // 2. Open a connection and build the binlog stream request.
            let mut conn = Conn::new(self.opts.clone())
                .await
                .map_err(|e| FaucetError::Source(format!("mysql-cdc: connect failed: {e}")))?;

            // Resolve Current → FilePos by querying the server's current binlog
            // position (fills in the actual file/pos before we build the request
            // that borrows from `resolved`).
            let resolved = resolve_current(resolved, &mut conn).await?;
            let req = build_request(self.config.server_id, &resolved)?;

            // 3. Start the binlog stream.
            let binlog_stream = conn
                .get_binlog_stream(req)
                .await
                .map_err(|e| FaucetError::Source(format!("mysql-cdc: get_binlog_stream: {e}")))?;
            let mut stream = std::pin::pin!(binlog_stream);

            // A start with no persisted bookmark persists where it opened
            // right away; otherwise a cycle that sees no commit persists
            // nothing and the next run opens at a later "current", losing
            // the changes in between (#789 SQL-24).
            if anchor_start && let ResolvedStart::FilePos { file, pos } = &resolved {
                let bm = Bookmark::FilePos { file: file.clone(), pos: *pos };
                yield StreamPage { records: Vec::new(), bookmark: Some(bm.to_value()?) };
            }

            // 4. Per-event tracking state.
            let mut current_file = match &resolved {
                ResolvedStart::FilePos { file, .. } => file.clone(),
                _ => String::new(),
            };
            // Rows of the transaction in progress.
            let mut buffer: Vec<Value> = Vec::new();
            let mut payload_end: Option<u64> = None;
            let mut txid: u64 = 0;
            // Committed transactions waiting to become pages (#789 SQL-118).
            let mut pages = PageBuilder::new(batch_size, PAGE_MAX_AGE, self.config.max_staged_records);
            let mut last_event = std::time::Instant::now();

            // emit!(ready) — yield each finished page with its bookmark.
            macro_rules! emit {
                ($ready:expr) => {{
                    for page in $ready {
                        self.note_emitted(&page.bookmark);
                        yield StreamPage {
                            records: page.records,
                            bookmark: Some(page.bookmark.to_value()?),
                        };
                    }
                }};
            }
            // commit!(pos) — the transaction in `buffer` committed at `pos`.
            macro_rules! commit {
                ($pos:expr) => {{
                    let bm = Bookmark::FilePos { file: current_file.clone(), pos: $pos };
                    let ready =
                        pages.commit(std::mem::take(&mut buffer), bm, std::time::Instant::now());
                    txid = txid.wrapping_add(1);
                    emit!(ready);
                }};
            }

            // 5. Drain loop.
            loop {
                let now = std::time::Instant::now();
                let idle_left = idle_timeout.saturating_sub(now.duration_since(last_event));
                let wait = pages.time_left(now).map_or(idle_left, |t| t.min(idle_left));
                match tokio::time::timeout(wait, stream.next()).await {
                    Ok(Some(Ok(event))) => {
                        last_event = std::time::Instant::now();
                        let header = event.header();
                        let ts_ms = u64::from(header.timestamp()) * 1_000;
                        // Events decompressed from a Transaction_payload carry
                        // log_pos 0; they sit at the payload's end position
                        // (#789 SQL-28).
                        let raw_pos = u64::from(header.log_pos());
                        if header.event_type_raw()
                            == mysql_async::binlog::EventType::TRANSACTION_PAYLOAD_EVENT as u8
                        {
                            payload_end = Some(raw_pos);
                        } else if raw_pos != 0 {
                            payload_end = None;
                        }
                        let log_pos = effective_log_pos(raw_pos, payload_end);

                        let event_data = event
                            .read_data()
                            .map_err(|e| FaucetError::Source(format!(
                                "mysql-cdc: read_data failed: {e}"
                            )))?;

                        match event_data {
                            Some(EventData::RotateEvent(re)) => {
                                current_file = re.name().into_owned();
                            }
                            Some(EventData::GtidEvent(_g)) => {
                                // A GtidEvent precedes BEGIN in GTID mode.
                                // Desync guard: rows still buffered when a new
                                // transaction starts belong to one that ended
                                // without a boundary event — commit them now
                                // rather than conflate them with the next txid.
                                if !buffer.is_empty() {
                                    commit!(log_pos);
                                }
                            }
                            Some(EventData::QueryEvent(qe)) => {
                                let default_schema = qe.schema().into_owned();
                                match classify_query(&qe.query(), &default_schema) {
                                    QueryKind::Begin => {
                                        if !buffer.is_empty() {
                                            commit!(log_pos);
                                        }
                                    }
                                    // Non-transactional / mixed-engine explicit
                                    // COMMIT (or ROLLBACK of one): MySQL logs a
                                    // QueryEvent instead of an XidEvent.
                                    QueryKind::Commit => commit!(log_pos),
                                    // SAVEPOINT / ROLLBACK TO / RELEASE / XA END
                                    // sit inside the transaction (#789 SQL-59).
                                    QueryKind::InTransaction => {}
                                    QueryKind::XaOutcome { xid, commit: commit_it } => {
                                        if !buffer.is_empty() {
                                            commit!(log_pos);
                                        }
                                        let bm = Bookmark::FilePos {
                                            file: current_file.clone(),
                                            pos: log_pos,
                                        };
                                        let (known, ready) = pages.decide(
                                            &xid,
                                            commit_it,
                                            bm,
                                            std::time::Instant::now(),
                                        );
                                        if !known && commit_it {
                                            tracing::warn!(
                                                connector = "mysql-cdc",
                                                "an XA transaction prepared before this stream \
                                                 started was committed; its rows were not captured"
                                            );
                                        }
                                        emit!(ready);
                                    }
                                    QueryKind::Truncate { schema, table } => {
                                        // TRUNCATE commits implicitly, like DDL.
                                        if !buffer.is_empty() {
                                            commit!(log_pos);
                                        }
                                        if self.config.table_included(&schema, &table) {
                                            buffer.push(build_envelope(
                                                "truncate",
                                                ts_ms,
                                                &schema,
                                                &table,
                                                Value::Null,
                                                Value::Null,
                                                json!({ "file": &current_file, "pos": log_pos }),
                                                txid,
                                            ));
                                        }
                                        commit!(log_pos);
                                    }
                                    QueryKind::Ddl => {
                                        // A DDL statement commits any open
                                        // transaction before it runs (F36): emit
                                        // those rows first, so the DDL's bookmark
                                        // never passes un-emitted rows.
                                        if !buffer.is_empty() {
                                            commit!(log_pos);
                                        }
                                        if self.config.emit_schema_changes {
                                            buffer.push(build_ddl_envelope(
                                                qe.query().as_ref(),
                                                ts_ms,
                                                &current_file,
                                                log_pos,
                                            ));
                                        }
                                        commit!(log_pos);
                                    }
                                }
                            }
                            Some(EventData::XaPrepareLogEvent(body)) => {
                                let (one_phase, xid) = parse_xa_prepare(&body).ok_or_else(|| {
                                    FaucetError::Source(
                                        "mysql-cdc: malformed XA_PREPARE_LOG_EVENT".into(),
                                    )
                                })?;
                                if one_phase {
                                    // XA COMMIT … ONE PHASE: the prepare is the commit.
                                    commit!(log_pos);
                                } else {
                                    // The outcome arrives later as XA COMMIT /
                                    // XA ROLLBACK; until then nothing after it
                                    // may be bookmarked (#789 SQL-59).
                                    pages
                                        .prepare(xid, std::mem::take(&mut buffer))
                                        .map_err(FaucetError::Source)?;
                                    txid = txid.wrapping_add(1);
                                }
                            }
                            Some(EventData::RowsEvent(re)) => {
                                let table_id = re.table_id();
                                let tme = stream
                                    .get_tme(table_id)
                                    .ok_or_else(|| FaucetError::Source(format!(
                                        "mysql-cdc: missing TableMapEvent for table_id={table_id}"
                                    )))?;

                                let db = tme.database_name().into_owned();
                                let table = tme.table_name().into_owned();

                                if !self.config.table_included(&db, &table) {
                                    continue;
                                }

                                let op = op_from_rows_event(&re);
                                let lsn = json!({ "file": &current_file, "pos": log_pos });
                                let hints = column_hints(tme);
                                let key_columns = if self.config.include_columns {
                                    Vec::new()
                                } else {
                                    primary_key_columns(tme)
                                };

                                for row_result in re.rows(tme) {
                                    let (before_row, after_row) = row_result.map_err(|e| {
                                        FaucetError::Source(format!(
                                            "mysql-cdc: row decode error: {e}"
                                        ))
                                    })?;

                                    let before_json = match &before_row {
                                        Some(r) if self.config.include_columns => {
                                            binlog_row_to_json_hinted(r, &hints)?
                                        }
                                        // Without before-images a delete still
                                        // needs its key, or nothing downstream can
                                        // apply it (#789 SQL-117).
                                        Some(r) if after_row.is_none() => key_image(
                                            binlog_row_to_json_hinted(r, &hints)?,
                                            r,
                                            &key_columns,
                                        ),
                                        _ => Value::Null,
                                    };
                                    let after_json = match &after_row {
                                        Some(r) => binlog_row_to_json_hinted(r, &hints)?,
                                        None => Value::Null,
                                    };

                                    let envelope = build_envelope(
                                        op, ts_ms, &db, &table,
                                        before_json, after_json,
                                        lsn.clone(), txid,
                                    );

                                    if let Some(max) = self.config.max_staged_records
                                        && buffer.len() >= max
                                    {
                                        Err(FaucetError::Source(format!(
                                            "mysql-cdc: in-progress transaction exceeded \
                                             max_staged_records ({max}); aborting to avoid \
                                             unbounded memory growth. Raise \
                                             max_staged_records or split the source transaction."
                                        )))?;
                                    }
                                    buffer.push(envelope);
                                }
                            }
                            Some(EventData::XidEvent(_xid)) => {
                                // InnoDB COMMIT.
                                commit!(log_pos);
                            }
                            _ => {
                                // FormatDescriptionEvent, PreviousGtidsEvent, etc. — ignored.
                            }
                        }
                        if let Some(e) = pages.take_error() {
                            Err(FaucetError::Source(e))?;
                        }
                        if pages.due(std::time::Instant::now()) {
                            emit!(pages.take());
                        }
                    }
                    Ok(Some(Err(e))) => {
                        Err(FaucetError::Source(format!("mysql-cdc: stream error: {e}")))?;
                    }
                    Err(_) if pages.due(std::time::Instant::now()) => {
                        emit!(pages.take());
                    }
                    // Idle timeout or stream closed. A transaction still open
                    // (or an XA one still undecided) is dropped: the server
                    // redelivers it from the last persisted bookmark next run.
                    Ok(None) | Err(_) => {
                        emit!(pages.take());
                        break;
                    }
                }
            }

            tracing::info!(
                connector = "mysql-cdc",
                server_id = self.config.server_id,
                "binlog fetch cycle complete",
            );
        })
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Helpers (pure, unit-testable)
// ──────────────────────────────────────────────────────────────────────────────

/// Resolved start position for a single fetch cycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ResolvedStart {
    /// Start at the server's current position (fresh run, no history).
    Current { file: String, pos: u64 },
    /// Start at the oldest available binlog (errors if purged).
    Earliest,
    /// Resume from a persisted file/pos bookmark.
    FilePos { file: String, pos: u64 },
    /// Start after a specific GTID set.  The string is parsed into `Sid`s
    /// for the `BinlogStreamRequest`.
    GtidSet { value: String },
}

/// The binlog position an event sits at: its own `log_pos`, or — for an
/// event decompressed from a `Transaction_payload` (whose `log_pos` is `0`) —
/// the enclosing payload's end position.
fn effective_log_pos(raw: u64, payload_end: Option<u64>) -> u64 {
    if raw == 0 {
        payload_end.unwrap_or(0)
    } else {
        raw
    }
}

/// Determine the effective start for this fetch cycle.
///
/// **Precedence:** a persisted bookmark (set via `apply_start_bookmark`) always
/// wins over the config's `start_position` — this is the CDC durability
/// invariant: we only advance past a position once the pipeline has persisted
/// the bookmark downstream.
///
/// - `FilePos` bookmark → `ResolvedStart::FilePos`
/// - `GtidSet` bookmark (from a previous session that used GTID start) →
///   treated as `FilePos` since all our bookmarks are file/pos after the first commit.
///
/// Note: all persisted bookmarks are `Bookmark::FilePos` (see module-level
/// note on the bookmark strategy), so the `GtidSet` arm is defensive.
pub(crate) fn resolve_start(
    start_position: &StartPosition,
    pending: Option<&Bookmark>,
) -> ResolvedStart {
    if let Some(bm) = pending {
        // A persisted bookmark always wins.
        return match bm {
            Bookmark::FilePos { file, pos } => ResolvedStart::FilePos {
                file: file.clone(),
                pos: *pos,
            },
            // Defensive: a GtidSet bookmark from a previous session that
            // DID persist GTID coordinates — start from the GTID set.
            Bookmark::GtidSet { gtid_set } => ResolvedStart::GtidSet {
                value: gtid_set.clone(),
            },
        };
    }

    // No persisted bookmark — use the config.
    match start_position {
        StartPosition::Current => {
            // Placeholder; real file/pos filled in later by `current_binlog_position`.
            ResolvedStart::Current {
                file: String::new(),
                pos: 0,
            }
        }
        StartPosition::Earliest => ResolvedStart::Earliest,
        StartPosition::FilePos { file, pos } => ResolvedStart::FilePos {
            file: file.clone(),
            pos: *pos,
        },
        StartPosition::GtidSet { value } => ResolvedStart::GtidSet {
            value: value.clone(),
        },
    }
}

/// If `resolved` is `Current`, query the server for its current binlog
/// position and return a `FilePos` variant with the real coordinates.
/// All other variants pass through unchanged.
///
/// Splitting this out of `build_request` ensures `resolved` is fully owned
/// before we build a request that borrows from it, avoiding the need to leak
/// any byte buffers.
async fn resolve_current(
    resolved: ResolvedStart,
    conn: &mut Conn,
) -> Result<ResolvedStart, FaucetError> {
    if !matches!(resolved, ResolvedStart::Current { .. }) {
        return Ok(resolved);
    }
    let (file, pos) = current_binlog_position(conn).await?;
    Ok(ResolvedStart::FilePos { file, pos })
}

/// Bytes of binlog between `from` and `head`, across files: the rest of
/// `from`'s file, every file in between, and `head`'s offset. `None` when
/// `from`'s file is no longer listed (purged) or sits after the head.
fn binlog_distance(logs: &[(String, u64)], from: (&str, u64), head: (&str, u64)) -> Option<u64> {
    if from.0 == head.0 {
        return Some(head.1.saturating_sub(from.1));
    }
    let start = logs.iter().position(|(n, _)| n == from.0)?;
    let end = logs.iter().position(|(n, _)| n == head.0)?;
    if end < start {
        return None;
    }
    let rest_of_first = logs[start].1.saturating_sub(from.1);
    let middle: u64 = logs[start + 1..end].iter().map(|(_, size)| *size).sum();
    Some(rest_of_first + middle + head.1)
}

/// Read the server's current binlog coordinates.
///
/// MySQL 8.4 removed `SHOW MASTER STATUS` in favour of `SHOW BINARY LOG STATUS`
/// (same `File` / `Position` columns). We try the 8.4+ spelling first and fall
/// back to the legacy statement when the server rejects it (5.7 / 8.0 raise a
/// parse error), so one code path works across 5.7 / 8.0 / 8.4+ without version
/// parsing. Only invoked at start/capture time, never on the per-event hot path.
///
/// A statement that *runs* but returns no rows means binary logging is disabled
/// — that is a definitive answer, so we surface it rather than falling back.
async fn current_binlog_position(conn: &mut Conn) -> Result<(String, u64), FaucetError> {
    let (row, stmt): (Option<Row>, &str) = match conn.query_first("SHOW BINARY LOG STATUS").await {
        Ok(row) => (row, "SHOW BINARY LOG STATUS"),
        // The 8.4+ spelling was rejected (older server) — fall back.
        Err(new_err) => match conn.query_first("SHOW MASTER STATUS").await {
            Ok(row) => (row, "SHOW MASTER STATUS"),
            Err(old_err) => return Err(binlog_position_error(new_err, old_err)),
        },
    };
    // Pull the raw `File` / `Position` columns out of the row (the only
    // server-dependent step), then hand the pure optionals to a unit-testable
    // decoder so the no-rows / missing-column branches need no live server.
    let extracted = row.map(|r| (r.get::<String, _>(0), r.get::<u64, _>(1)));
    finalize_binlog_position(extracted, stmt)
}

/// Error returned when *both* binlog-status statements are rejected — which
/// should never happen on a connected server, since at least one spelling is
/// valid for any supported version. Pulled out so its message is unit-testable.
fn binlog_position_error(
    new_err: impl std::fmt::Display,
    old_err: impl std::fmt::Display,
) -> FaucetError {
    FaucetError::Source(format!(
        "mysql-cdc: reading current binlog position failed — \
         `SHOW BINARY LOG STATUS`: {new_err}; `SHOW MASTER STATUS`: {old_err}"
    ))
}

/// Turn the raw `(File, Position)` columns of a binlog-status row into binlog
/// coordinates. `extracted` is `None` when the statement returned no rows
/// (binary logging disabled); the inner options are `None` when a column is
/// absent. Pure (no I/O) so every branch — no-rows, missing-column, success —
/// is unit-testable without a live server. `stmt` names the statement for errors.
fn finalize_binlog_position(
    extracted: Option<(Option<String>, Option<u64>)>,
    stmt: &str,
) -> Result<(String, u64), FaucetError> {
    let (file, pos) = extracted.ok_or_else(|| {
        FaucetError::Source(format!(
            "mysql-cdc: {stmt} returned no rows; is binary logging enabled?"
        ))
    })?;
    let file =
        file.ok_or_else(|| FaucetError::Source(format!("mysql-cdc: {stmt}: missing File column")))?;
    let pos = pos.ok_or_else(|| {
        FaucetError::Source(format!("mysql-cdc: {stmt}: missing Position column"))
    })?;
    Ok((file, pos))
}

/// Build a `BinlogStreamRequest` from the resolved start position.
///
/// No heap memory is leaked: filenames borrow from `resolved` (which the
/// caller holds for the duration of the call), and GTID SIDs are parsed into
/// fully-owned data structures — `Sid::from_str` produces `Sid<'static>`
/// because all internal fields (`Seq` / `Tag`) are stored as `Cow::Owned`.
fn build_request<'r>(
    server_id: u32,
    resolved: &'r ResolvedStart,
) -> Result<BinlogStreamRequest<'r>, FaucetError> {
    use mysql_async::Sid;
    use std::str::FromStr;

    match resolved {
        // resolve_current() converts Current → FilePos before we get here;
        // this arm is only hit if a caller skips that step (defensive).
        ResolvedStart::Current { .. } => Ok(BinlogStreamRequest::new(server_id)),
        ResolvedStart::Earliest => {
            // No filename/pos — server starts from the oldest available binlog.
            Ok(BinlogStreamRequest::new(server_id))
        }
        ResolvedStart::FilePos { file, pos } => {
            // Borrow the filename bytes directly from the owned `String` in
            // `resolved` — no copy, no leak.
            Ok(BinlogStreamRequest::new(server_id)
                .with_filename(file.as_bytes())
                .with_pos(*pos))
        }
        ResolvedStart::GtidSet { value } => {
            // `Sid::from_str` parses into fully-owned data (intervals as
            // `Cow::Owned`, tag as `Tag<'static>` via `to_owned()`), so the
            // resulting `Sid<'static>` does not borrow the input string.
            // No leaking required.
            let sids: Vec<Sid<'static>> = value
                .split(',')
                .map(|part| {
                    let trimmed = part.trim();
                    Sid::from_str(trimmed).map_err(|e| {
                        FaucetError::Source(format!("mysql-cdc: invalid GTID set '{trimmed}': {e}"))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;

            Ok(BinlogStreamRequest::new(server_id)
                .with_gtid()
                .with_gtid_set(sids))
        }
    }
}

/// How long a committed transaction may wait for its page to fill.
const PAGE_MAX_AGE: std::time::Duration = std::time::Duration::from_secs(1);

/// The key columns of a delete's before-image, for `include_columns: false`
/// (the whole image when the table declares no primary key).
fn key_image(full: Value, row: &mysql_async::binlog::row::BinlogRow, key_columns: &[usize]) -> Value {
    if key_columns.is_empty() {
        return full;
    }
    let names: Vec<String> = key_columns
        .iter()
        .filter_map(|i| row.columns_ref().get(*i).map(|c| c.name_str().into_owned()))
        .collect();
    match full {
        Value::Object(mut obj) => {
            obj.retain(|k, _| names.contains(k));
            Value::Object(obj)
        }
        other => other,
    }
}

/// Map a `RowsEventData` variant to its CDC operation string.
pub(crate) fn op_from_rows_event(re: &RowsEventData<'_>) -> &'static str {
    match re {
        RowsEventData::WriteRowsEvent(_) | RowsEventData::WriteRowsEventV1(_) => "c",
        RowsEventData::UpdateRowsEvent(_)
        | RowsEventData::UpdateRowsEventV1(_)
        | RowsEventData::PartialUpdateRowsEvent(_) => "u",
        RowsEventData::DeleteRowsEvent(_) | RowsEventData::DeleteRowsEventV1(_) => "d",
    }
}

/// Build a CDC change-event envelope.
///
/// ```json
/// { "op": "c", "ts_ms": 1234, "schema": "mydb", "table": "users",
///   "before": null, "after": {"id": 1, "name": "alice"},
///   "lsn": {"file": "binlog.000001", "pos": 4567}, "txid": 0 }
/// ```
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_envelope(
    op: &str,
    ts_ms: u64,
    schema: &str,
    table: &str,
    before: Value,
    after: Value,
    lsn: Value,
    txid: u64,
) -> Value {
    let mut obj = Map::new();
    obj.insert("op".into(), json!(op));
    obj.insert("ts_ms".into(), json!(ts_ms));
    obj.insert("schema".into(), json!(schema));
    obj.insert("table".into(), json!(table));
    obj.insert("before".into(), before);
    obj.insert("after".into(), after);
    obj.insert("lsn".into(), lsn);
    obj.insert("txid".into(), json!(txid));
    Value::Object(obj)
}

/// Build a DDL change-event envelope.
fn build_ddl_envelope(statement: &str, ts_ms: u64, file: &str, pos: u64) -> Value {
    json!({
        "op": "ddl",
        "ts_ms": ts_ms,
        "statement": statement,
        "lsn": { "file": file, "pos": pos },
    })
}

// ──────────────────────────────────────────────────────────────────────────────
// TLS + Opts construction
// ──────────────────────────────────────────────────────────────────────────────

fn build_opts(config: &MysqlCdcSourceConfig) -> Result<Opts, FaucetError> {
    let base = Opts::from_url(&config.connection_url)
        .map_err(|e| FaucetError::Config(format!("mysql-cdc: invalid connection URL: {e}")))?;

    let ssl = match &config.tls {
        CdcTls::Disable => return Ok(base),
        CdcTls::Require => SslOpts::default()
            .with_danger_accept_invalid_certs(true)
            .with_danger_skip_domain_validation(true),
        CdcTls::VerifyCa { ca_path } => {
            let mut s = SslOpts::default().with_danger_skip_domain_validation(true);
            if let Some(p) = ca_path {
                s = s.with_root_certs(vec![PathBuf::from(p).into()]);
            }
            s
        }
        CdcTls::VerifyFull { ca_path } => {
            let mut s = SslOpts::default();
            if let Some(p) = ca_path {
                s = s.with_root_certs(vec![PathBuf::from(p).into()]);
            }
            s
        }
    };

    Ok(OptsBuilder::from_opts(base).ssl_opts(ssl).into())
}

/// What is weak about the replication connection's TLS, if anything
/// (#789 SUPPLY-06).
fn tls_warning(opts: &Opts) -> Option<&'static str> {
    match opts.ssl_opts() {
        None => Some(
            "the replication connection is plaintext — credentials and every row change travel \
             unencrypted; set `tls.mode` or `require_ssl=true` in connection_url",
        ),
        Some(ssl) if ssl.accept_invalid_certs() => Some(
            "TLS without certificate verification (`require`) — use `verify_full` to \
             authenticate the server",
        ),
        Some(_) => None,
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Preflight helpers
// ──────────────────────────────────────────────────────────────────────────────

/// Decide whether a `binlog_row_value_options` value is safe for CDC.
///
/// Only an empty value (full-value JSON logging) is acceptable. Any non-empty
/// setting — `PARTIAL_JSON` is the only documented value today — means UPDATEs to
/// JSON columns may emit partial diffs that cannot be reconstructed without the
/// prior row, so it is rejected. The check is case-insensitive and ignores
/// surrounding whitespace. Pure seam, unit-tested without a server.
fn binlog_row_value_options_ok(value: &str) -> bool {
    value.trim().is_empty()
}

/// Run preflight checks and return a human-readable summary on success, or an
/// error message on the first failing check.
async fn run_preflight_probes(
    conn: &mut Conn,
    config: &MysqlCdcSourceConfig,
) -> Result<String, String> {
    // Check binlog_format = ROW
    let fmt: Option<(String, String)> = conn
        .query_first("SHOW VARIABLES LIKE 'binlog_format'")
        .await
        .map_err(|e| format!("SHOW VARIABLES LIKE 'binlog_format' failed: {e}"))?;
    match fmt.as_ref() {
        Some((_, v)) if !v.eq_ignore_ascii_case("ROW") => {
            return Err(format!(
                "binlog_format is '{v}', must be ROW. \
                 Set binlog_format=ROW in your MySQL config."
            ));
        }
        None => {
            return Err("binlog_format variable not found. Is binary logging enabled?".into());
        }
        _ => {}
    }

    // Check binlog_row_image = FULL
    let img: Option<(String, String)> = conn
        .query_first("SHOW VARIABLES LIKE 'binlog_row_image'")
        .await
        .map_err(|e| format!("SHOW VARIABLES LIKE 'binlog_row_image' failed: {e}"))?;
    match img.as_ref() {
        Some((_, v)) if !v.eq_ignore_ascii_case("FULL") => {
            return Err(format!(
                "binlog_row_image is '{v}', must be FULL. \
                 Set binlog_row_image=FULL in your MySQL config."
            ));
        }
        None => {
            return Err("binlog_row_image variable not found.".into());
        }
        _ => {}
    }

    // Check binlog_row_metadata = FULL (required for column names)
    let meta: Option<(String, String)> = conn
        .query_first("SHOW VARIABLES LIKE 'binlog_row_metadata'")
        .await
        .map_err(|e| format!("SHOW VARIABLES LIKE 'binlog_row_metadata' failed: {e}"))?;
    match meta.as_ref() {
        Some((_, v)) if !v.eq_ignore_ascii_case("FULL") => {
            return Err(format!(
                "binlog_row_metadata is '{v}', must be FULL. \
                 Set binlog_row_metadata=FULL in your MySQL config."
            ));
        }
        None => {
            return Err("binlog_row_metadata variable not found.".into());
        }
        _ => {}
    }

    // Check binlog_row_value_options is not PARTIAL_JSON. Under PARTIAL_JSON an
    // UPDATE that touches a JSON column emits a partial diff (BinlogValue::JsonDiff)
    // rather than the full document; faucet-stream cannot reconstruct it without the
    // prior row, so we reject it here rather than corrupt the column at runtime.
    let value_opts: Option<(String, String)> = conn
        .query_first("SHOW VARIABLES LIKE 'binlog_row_value_options'")
        .await
        .map_err(|e| format!("SHOW VARIABLES LIKE 'binlog_row_value_options' failed: {e}"))?;
    // A missing variable (older servers) means full-value logging — acceptable.
    if let Some((_, v)) = value_opts.as_ref()
        && !binlog_row_value_options_ok(v)
    {
        return Err(format!(
            "binlog_row_value_options is '{v}', must be empty (full JSON). \
             Partial JSON diffs cannot be reconstructed for CDC. \
             Set binlog_row_value_options='' on the MySQL server."
        ));
    }

    // Check REPLICATION grants
    let grants: Vec<String> = conn
        .query("SHOW GRANTS FOR CURRENT_USER()")
        .await
        .map_err(|e| format!("SHOW GRANTS failed: {e}"))?;
    let grants_combined = grants.join(" ").to_uppercase();
    let has_replication = grants_combined.contains("ALL PRIVILEGES")
        || (grants_combined.contains("REPLICATION SLAVE")
            && grants_combined.contains("REPLICATION CLIENT"));
    if !has_replication {
        return Err(
            "user lacks REPLICATION SLAVE and/or REPLICATION CLIENT privileges. \
             Grant them with: GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user'@'host';"
                .into(),
        );
    }

    // If start_position is GtidSet, gtid_mode must be ON. Checked here (rather
    // than only in `new()`) so `faucet doctor`'s binlog-config probe catches it
    // too.
    if matches!(config.start_position, StartPosition::GtidSet { .. }) {
        let gtid: Option<(String, String)> = conn
            .query_first("SHOW VARIABLES LIKE 'gtid_mode'")
            .await
            .map_err(|e| format!("SHOW VARIABLES LIKE 'gtid_mode' failed: {e}"))?;
        match gtid.as_ref() {
            Some((_, v)) if !v.eq_ignore_ascii_case("ON") => {
                return Err(format!(
                    "start_position is GtidSet but gtid_mode is '{v}' (must be ON). \
                     Enable GTID mode: --gtid-mode=ON --enforce-gtid-consistency=ON"
                ));
            }
            None => {
                return Err("gtid_mode variable not found".into());
            }
            _ => {}
        }
    }

    Ok(
        "binlog_format=ROW, binlog_row_image=FULL, binlog_row_metadata=FULL, \
         binlog_row_value_options=full, grants OK"
            .into(),
    )
}

/// Run preflight checks, mapping a failure to a typed `FaucetError::Source`.
async fn run_preflight(conn: &mut Conn, config: &MysqlCdcSourceConfig) -> Result<(), FaucetError> {
    run_preflight_probes(conn, config)
        .await
        .map(|_| ())
        .map_err(|m| FaucetError::Source(format!("mysql-cdc: {m}")))
}

// ──────────────────────────────────────────────────────────────────────────────
// Unit tests
// ──────────────────────────────────────────────────────────────────────────────

/// `database.table` of a change envelope, the name the `mysql` source's
/// discovery reports for the same table.
fn schema_table(record: &Value) -> Option<String> {
    let schema = record.get("schema")?.as_str()?;
    let table = record.get("table")?.as_str()?;
    Some(format!("{schema}.{table}"))
}

/// Order two bookmarks: binlog coordinates by (file sequence, position) within
/// one binlog base name; GTID sets by containment. Mixed shapes are unrelated.
fn bookmark_le(a: &Value, b: &Value) -> Option<bool> {
    match (
        Bookmark::from_value(a.clone()).ok()?,
        Bookmark::from_value(b.clone()).ok()?,
    ) {
        (Bookmark::FilePos { file: fa, pos: pa }, Bookmark::FilePos { file: fb, pos: pb }) => {
            let (base_a, seq_a) = binlog_seq(&fa)?;
            let (base_b, seq_b) = binlog_seq(&fb)?;
            (base_a == base_b).then_some((seq_a, pa) <= (seq_b, pb))
        }
        (Bookmark::GtidSet { gtid_set: ga }, Bookmark::GtidSet { gtid_set: gb }) => {
            let (sa, sb) = (parse_gtid_set(&ga)?, parse_gtid_set(&gb)?);
            Some(sa.iter().all(|(source, intervals)| {
                intervals.iter().all(|&(lo, hi)| {
                    sb.get(source)
                        .is_some_and(|have| interval_covered(have, lo, hi))
                })
            }))
        }
        _ => None,
    }
}

fn binlog_seq(file: &str) -> Option<(&str, u64)> {
    let (base, seq) = file.rsplit_once('.')?;
    Some((base, seq.parse().ok()?))
}

type GtidSet = std::collections::BTreeMap<String, Vec<(u64, u64)>>;

/// Parse `uuid[:tag]:1-5:7,uuid2:1-3` into merged, sorted intervals per source.
fn parse_gtid_set(set: &str) -> Option<GtidSet> {
    let mut out = GtidSet::new();
    for part in set.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let mut pieces = part.split(':');
        let uuid = pieces.next()?.trim().to_ascii_lowercase();
        let mut source = uuid.clone();
        for piece in pieces {
            let piece = piece.trim();
            let interval = match piece.split_once('-') {
                Some((lo, hi)) => lo.parse().ok().zip(hi.parse().ok()),
                None => piece.parse().ok().map(|n| (n, n)),
            };
            match interval {
                Some((lo, hi)) if lo <= hi => out.entry(source.clone()).or_default().push((lo, hi)),
                Some(_) => return None,
                None => source = format!("{uuid}:{piece}"),
            }
        }
    }
    for intervals in out.values_mut() {
        intervals.sort_unstable();
        let mut merged: Vec<(u64, u64)> = Vec::with_capacity(intervals.len());
        for &(lo, hi) in intervals.iter() {
            match merged.last_mut() {
                Some(last) if lo <= last.1.saturating_add(1) => last.1 = last.1.max(hi),
                _ => merged.push((lo, hi)),
            }
        }
        *intervals = merged;
    }
    Some(out)
}

fn interval_covered(have: &[(u64, u64)], lo: u64, hi: u64) -> bool {
    have.iter().any(|&(a, b)| a <= lo && hi <= b)
}

#[cfg(test)]
mod tests {

    #[test]
    fn routes_by_schema_table() {
        assert_eq!(
            schema_table(&json!({"schema": "shop", "table": "orders"})),
            Some("shop.orders".into())
        );
        assert_eq!(schema_table(&json!({"op": "ddl"})), None);
    }

    #[test]
    fn orders_binlog_coordinates_and_gtid_sets() {
        let fp = |f: &str, p: u64| json!({"file": f, "pos": p});
        assert_eq!(
            bookmark_le(&fp("binlog.000009", 900), &fp("binlog.000010", 4)),
            Some(true)
        );
        assert_eq!(
            bookmark_le(&fp("binlog.000010", 5), &fp("binlog.000010", 4)),
            Some(false)
        );
        assert_eq!(
            bookmark_le(&fp("binlog.000010", 4), &fp("binlog.000010", 4)),
            Some(true)
        );
        assert_eq!(bookmark_le(&fp("a.000001", 1), &fp("b.000001", 1)), None);
        assert_eq!(bookmark_le(&fp("noseq", 1), &fp("noseq", 1)), None);
        let g = |s: &str| json!({"gtid_set": s});
        let u = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
        assert_eq!(
            bookmark_le(&g(&format!("{u}:1-5")), &g(&format!("{u}:1-3:4-9"))),
            Some(true)
        );
        assert_eq!(
            bookmark_le(&g(&format!("{u}:1-9")), &g(&format!("{u}:1-5"))),
            Some(false)
        );
        assert_eq!(
            bookmark_le(&g(&format!("{u}:1-2,other:1")), &g(&format!("{u}:1-5"))),
            Some(false)
        );
        assert_eq!(
            bookmark_le(&g(&format!("{u}:tag:3")), &g(&format!("{u}:1-2:tag:1-4"))),
            Some(true)
        );
        assert_eq!(bookmark_le(&g(&format!("{u}:5-1")), &g(u)), None);
        assert_eq!(bookmark_le(&g(u), &fp("binlog.000001", 1)), None);
        assert_eq!(bookmark_le(&json!("junk"), &g(u)), None);
    }

    #[test]
    fn binlog_distance_spans_files() {
        let logs = vec![
            ("binlog.000001".to_string(), 1000),
            ("binlog.000002".to_string(), 500),
            ("binlog.000003".to_string(), 300),
        ];
        assert_eq!(
            binlog_distance(&logs, ("binlog.000003", 100), ("binlog.000003", 300)),
            Some(200)
        );
        assert_eq!(
            binlog_distance(&logs, ("binlog.000001", 900), ("binlog.000003", 50)),
            Some(100 + 500 + 50)
        );
        assert_eq!(
            binlog_distance(&logs, ("binlog.000000", 4), ("binlog.000003", 50)),
            None
        );
        assert_eq!(
            binlog_distance(&logs, ("binlog.000003", 4), ("binlog.000001", 50)),
            None
        );
        assert_eq!(
            binlog_distance(&logs, ("binlog.000002", 600), ("binlog.000002", 10)),
            Some(0)
        );
    }

    use super::*;

    #[test]
    fn events_inside_a_compressed_transaction_take_the_payload_end() {
        assert_eq!(effective_log_pos(1234, None), 1234);
        assert_eq!(effective_log_pos(1234, Some(99)), 1234);
        assert_eq!(effective_log_pos(0, Some(99)), 99);
        assert_eq!(effective_log_pos(0, None), 0);
    }
    use crate::state::Bookmark;
    use serde_json::json;

    // ── binlog_row_value_options preflight seam (PARTIAL_JSON, F17) ───────────

    #[test]
    fn binlog_row_value_options_empty_is_ok() {
        assert!(binlog_row_value_options_ok(""));
        assert!(binlog_row_value_options_ok("   "));
    }

    #[test]
    fn binlog_row_value_options_partial_json_rejected() {
        assert!(!binlog_row_value_options_ok("PARTIAL_JSON"));
        assert!(!binlog_row_value_options_ok("partial_json"));
        assert!(!binlog_row_value_options_ok("  PARTIAL_JSON  "));
    }

    // ── binlog position decoding (MySQL 8.4 fallback, #242) ───────────────────

    #[test]
    fn finalize_binlog_position_returns_file_and_pos() {
        let got = finalize_binlog_position(Some((Some("mysql-bin.000007".into()), Some(154))), "S")
            .expect("valid row decodes");
        assert_eq!(got, ("mysql-bin.000007".to_string(), 154));
    }

    #[test]
    fn finalize_binlog_position_no_rows_means_binlogging_disabled() {
        let err = finalize_binlog_position(None, "SHOW BINARY LOG STATUS")
            .expect_err("no rows must error");
        let msg = err.to_string();
        assert!(msg.contains("returned no rows"), "{msg}");
        assert!(msg.contains("SHOW BINARY LOG STATUS"), "{msg}");
    }

    #[test]
    fn finalize_binlog_position_missing_file_column() {
        let err = finalize_binlog_position(Some((None, Some(4))), "SHOW MASTER STATUS")
            .expect_err("missing File must error");
        assert!(err.to_string().contains("missing File column"), "{err}");
    }

    #[test]
    fn finalize_binlog_position_missing_position_column() {
        let err = finalize_binlog_position(
            Some((Some("mysql-bin.1".into()), None)),
            "SHOW MASTER STATUS",
        )
        .expect_err("missing Position must error");
        assert!(err.to_string().contains("missing Position column"), "{err}");
    }

    #[test]
    fn binlog_position_error_names_both_statements() {
        let msg = binlog_position_error("parse error NEW", "parse error OLD").to_string();
        assert!(
            msg.contains("SHOW BINARY LOG STATUS") && msg.contains("parse error NEW"),
            "{msg}"
        );
        assert!(
            msg.contains("SHOW MASTER STATUS") && msg.contains("parse error OLD"),
            "{msg}"
        );
    }

    // ── resolve_start precedence ──────────────────────────────────────────────

    #[test]
    fn file_pos_bookmark_overrides_current() {
        let bm = Bookmark::FilePos {
            file: "binlog.000003".into(),
            pos: 4567,
        };
        let resolved = resolve_start(&StartPosition::Current, Some(&bm));
        assert_eq!(
            resolved,
            ResolvedStart::FilePos {
                file: "binlog.000003".into(),
                pos: 4567
            }
        );
    }

    #[test]
    fn file_pos_bookmark_overrides_gtid_config() {
        let bm = Bookmark::FilePos {
            file: "binlog.000010".into(),
            pos: 123,
        };
        let resolved = resolve_start(
            &StartPosition::GtidSet {
                value: "abc:1-100".into(),
            },
            Some(&bm),
        );
        assert_eq!(
            resolved,
            ResolvedStart::FilePos {
                file: "binlog.000010".into(),
                pos: 123
            }
        );
    }

    #[test]
    fn gtid_bookmark_overrides_current() {
        let bm = Bookmark::GtidSet {
            gtid_set: "abc:1-100".into(),
        };
        let resolved = resolve_start(&StartPosition::Current, Some(&bm));
        assert_eq!(
            resolved,
            ResolvedStart::GtidSet {
                value: "abc:1-100".into()
            }
        );
    }

    #[test]
    fn no_bookmark_current_yields_current() {
        let resolved = resolve_start(&StartPosition::Current, None);
        assert!(matches!(resolved, ResolvedStart::Current { .. }));
    }

    #[test]
    fn no_bookmark_earliest_yields_earliest() {
        let resolved = resolve_start(&StartPosition::Earliest, None);
        assert_eq!(resolved, ResolvedStart::Earliest);
    }

    #[test]
    fn no_bookmark_file_pos_config_passes_through() {
        let resolved = resolve_start(
            &StartPosition::FilePos {
                file: "binlog.000001".into(),
                pos: 4,
            },
            None,
        );
        assert_eq!(
            resolved,
            ResolvedStart::FilePos {
                file: "binlog.000001".into(),
                pos: 4
            }
        );
    }

    // ── op_from_rows_event ────────────────────────────────────────────────────
    //
    // `op_from_rows_event` maps a `RowsEventData` variant → "c"/"u"/"d". A
    // `RowsEventData` cannot be constructed without raw binlog bytes, so this
    // mapping is exercised end-to-end by the Docker integration test
    // (`tests/integration.rs`), which asserts an INSERT/UPDATE/DELETE produce
    // ops c/u/d. A unit test asserting string literals against themselves would
    // give false confidence, so it is intentionally omitted here.

    // ── envelope assembly ─────────────────────────────────────────────────────

    #[test]
    fn envelope_shape_insert() {
        let lsn = json!({ "file": "binlog.000001", "pos": 4567_u64 });
        let after = json!({ "id": 1, "name": "alice" });
        let env = build_envelope(
            "c",
            1_000,
            "mydb",
            "users",
            Value::Null,
            after.clone(),
            lsn.clone(),
            0,
        );

        assert_eq!(env["op"], "c");
        assert_eq!(env["ts_ms"], 1_000_u64);
        assert_eq!(env["schema"], "mydb");
        assert_eq!(env["table"], "users");
        assert_eq!(env["before"], Value::Null);
        assert_eq!(env["after"], after);
        assert_eq!(env["lsn"], lsn);
        assert_eq!(env["txid"], 0_u64);
    }

    #[test]
    fn envelope_shape_update() {
        let before = json!({ "id": 1, "name": "alice" });
        let after = json!({ "id": 1, "name": "bob" });
        let lsn = json!({ "file": "binlog.000002", "pos": 9999_u64 });
        let env = build_envelope(
            "u",
            2_000,
            "db",
            "tbl",
            before.clone(),
            after.clone(),
            lsn,
            3,
        );

        assert_eq!(env["op"], "u");
        assert_eq!(env["before"], before);
        assert_eq!(env["after"], after);
        assert_eq!(env["txid"], 3_u64);
    }

    #[test]
    fn envelope_shape_delete() {
        let before = json!({ "id": 42 });
        let lsn = json!({ "file": "binlog.000003", "pos": 100_u64 });
        let env = build_envelope("d", 3_000, "db", "tbl", before.clone(), Value::Null, lsn, 7);

        assert_eq!(env["op"], "d");
        assert_eq!(env["before"], before);
        assert_eq!(env["after"], Value::Null);
    }

    #[test]
    fn envelope_has_all_expected_keys() {
        let env = build_envelope(
            "c",
            0,
            "s",
            "t",
            Value::Null,
            Value::Null,
            json!({ "file": "f", "pos": 0_u64 }),
            0,
        );
        let obj = env.as_object().unwrap();
        for key in &[
            "op", "ts_ms", "schema", "table", "before", "after", "lsn", "txid",
        ] {
            assert!(obj.contains_key(*key), "missing key: {key}");
        }
    }

    // ── build_opts TLS ────────────────────────────────────────────────────────

    #[test]
    fn build_opts_disable_succeeds() {
        let config: MysqlCdcSourceConfig = serde_json::from_value(json!({
            "connection_url": "mysql://repl:pass@localhost:3306/db",
            "server_id": 1001
        }))
        .unwrap();
        assert!(build_opts(&config).is_ok());
    }

    #[test]
    fn build_opts_require_tls_succeeds() {
        let config: MysqlCdcSourceConfig = serde_json::from_value(json!({
            "connection_url": "mysql://repl:pass@localhost:3306/db",
            "server_id": 1002,
            "tls": { "mode": "require" }
        }))
        .unwrap();
        assert!(build_opts(&config).is_ok());
    }

    #[test]
    fn build_opts_verify_ca_no_path() {
        let config: MysqlCdcSourceConfig = serde_json::from_value(json!({
            "connection_url": "mysql://repl:pass@localhost:3306/db",
            "server_id": 1003,
            "tls": { "mode": "verify_ca" }
        }))
        .unwrap();
        assert!(build_opts(&config).is_ok());
    }

    #[test]
    fn weak_tls_is_reported_and_the_url_ssl_mode_is_kept() {
        let opts = |url: &str, tls: Value| {
            build_opts(
                &serde_json::from_value(json!({
                    "connection_url": url, "server_id": 7, "tls": tls
                }))
                .unwrap(),
            )
            .unwrap()
        };
        let plain = opts("mysql://r:p@h:3306/db", json!({"mode": "disable"}));
        assert!(tls_warning(&plain).unwrap().contains("plaintext"));
        let required = opts("mysql://r:p@h:3306/db", json!({"mode": "require"}));
        assert!(tls_warning(&required).unwrap().contains("verification"));
        let verified = opts("mysql://r:p@h:3306/db", json!({"mode": "verify_full"}));
        assert!(tls_warning(&verified).is_none());
        // An omitted `tls:` keeps what the URL asks for.
        let from_url = opts(
            "mysql://r:p@h:3306/db?require_ssl=true",
            json!({"mode": "disable"}),
        );
        assert!(tls_warning(&from_url).is_none());
    }

    #[test]
    fn build_opts_invalid_url_errors() {
        let config: MysqlCdcSourceConfig = serde_json::from_value(json!({
            "connection_url": "not-a-valid-url",
            "server_id": 1
        }))
        .unwrap();
        assert!(build_opts(&config).is_err());
    }

    // dataset_uri is a pure-config method; the source requires a live DB to
    // construct so we verify the logic directly using config deserialization.
    #[test]
    fn dataset_uri_strips_credentials_no_tables() {
        let config: MysqlCdcSourceConfig = serde_json::from_value(json!({
            "connection_url": "mysql://repl:pass@h:3306/db",
            "server_id": 1
        }))
        .unwrap();
        let redacted = faucet_core::redact_uri_credentials(&config.connection_url);
        assert_eq!(redacted, "mysql://h:3306/db");
        // No include_tables → base URI only.
        let uri = if config.include_tables.is_empty() {
            redacted
        } else {
            format!("{redacted}?tables={}", config.include_tables.join(","))
        };
        assert_eq!(uri, "mysql://h:3306/db");
    }

    #[test]
    fn dataset_uri_appends_tables_when_present() {
        let config: MysqlCdcSourceConfig = serde_json::from_value(json!({
            "connection_url": "mysql://repl:pass@h:3306/db",
            "server_id": 1,
            "include_tables": ["db.orders", "db.users"]
        }))
        .unwrap();
        let redacted = faucet_core::redact_uri_credentials(&config.connection_url);
        let uri = if config.include_tables.is_empty() {
            redacted
        } else {
            format!("{redacted}?tables={}", config.include_tables.join(","))
        };
        assert_eq!(uri, "mysql://h:3306/db?tables=db.orders,db.users");
    }

    // ── build_request ─────────────────────────────────────────────────────────
    //
    // `BinlogStreamRequest` exposes no public getters and does not derive
    // `Debug`, so the only observable outcome of `build_request` for the
    // non-erroring arms is `Ok` vs `Err` (the builder is side-effect-free).
    // The GtidSet arm additionally has an observable error path, which is
    // asserted exactly below.

    #[test]
    fn build_request_current_arm_succeeds() {
        // Defensive arm: `resolve_current` normally converts Current → FilePos
        // before `build_request`, but a caller that skips it must still get a
        // plain request rather than an error.
        let resolved = ResolvedStart::Current {
            file: String::new(),
            pos: 0,
        };
        assert!(build_request(42, &resolved).is_ok());
    }

    #[test]
    fn build_request_earliest_arm_succeeds() {
        let resolved = ResolvedStart::Earliest;
        assert!(build_request(7, &resolved).is_ok());
    }

    #[test]
    fn build_request_file_pos_arm_succeeds() {
        let resolved = ResolvedStart::FilePos {
            file: "binlog.000007".into(),
            pos: 8192,
        };
        assert!(build_request(1001, &resolved).is_ok());
    }

    #[test]
    fn build_request_valid_gtid_set_succeeds() {
        // A well-formed `uuid:interval` GTID parses into `Sid`s; multiple
        // comma-separated entries are each trimmed and parsed.
        let resolved = ResolvedStart::GtidSet {
            value: "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5, \
                    8a1d3a7c-71ca-11e1-9e33-c80aa9429562:1-10"
                .into(),
        };
        assert!(build_request(1001, &resolved).is_ok());
    }

    #[test]
    fn build_request_invalid_gtid_set_errors_with_source_variant() {
        // A malformed GTID set surfaces as a typed `FaucetError::Source` whose
        // message names the offending fragment.
        let resolved = ResolvedStart::GtidSet {
            value: "totally-not-a-gtid".into(),
        };
        // `BinlogStreamRequest` is not `Debug`, so match the `Result` directly
        // rather than via `expect_err` (which would require `Ok: Debug`).
        match build_request(1001, &resolved) {
            Err(FaucetError::Source(msg)) => {
                assert!(
                    msg.contains("invalid GTID set 'totally-not-a-gtid'"),
                    "message must name the bad fragment; got: {msg}"
                );
            }
            Err(other) => panic!("expected FaucetError::Source, got {other:?}"),
            Ok(_) => panic!("invalid GTID must error"),
        }
    }

    // ── build_ddl_envelope ────────────────────────────────────────────────────

    #[test]
    fn ddl_envelope_shape() {
        let env = build_ddl_envelope("ALTER TABLE t ADD c INT", 1_700, "binlog.000004", 512);
        assert_eq!(env["op"], "ddl");
        assert_eq!(env["ts_ms"], 1_700_u64);
        assert_eq!(env["statement"], "ALTER TABLE t ADD c INT");
        assert_eq!(
            env["lsn"],
            json!({ "file": "binlog.000004", "pos": 512_u64 })
        );
        // DDL envelopes carry no before/after/schema/table keys.
        let obj = env.as_object().unwrap();
        assert!(!obj.contains_key("before"));
        assert!(!obj.contains_key("after"));
        assert!(!obj.contains_key("schema"));
        assert!(!obj.contains_key("table"));
    }
}
