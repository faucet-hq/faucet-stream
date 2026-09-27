//! `MysqlCdcSource` — public `Source` implementation.
//!
//! Tails the MySQL binary log via `mysql_async`'s async [`BinlogStream`] and
//! emits per-row change events as CDC envelopes.  Transactions are buffered
//! in memory (BEGIN → ROWS → COMMIT) and emitted atomically as one
//! [`StreamPage`] per commit, matching the postgres-cdc durability contract.
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
use crate::convert::binlog_row_to_json;
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

    /// Per-transaction streaming.  Each committed transaction is emitted as
    /// its own [`StreamPage`] with `bookmark = Some(file_pos)`.  The
    /// trait-level `batch_size` argument is ignored in favour of the config
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
        let per_transaction = batch_size != 0;

        Box::pin(async_stream::try_stream! {
            use futures::StreamExt;

            // 1. Resolve start position for this fetch cycle.
            let pending = self.pending_bookmark.lock().await.take();
            let resolved = resolve_start(&self.config.start_position, pending.as_ref());

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

            // 4. Per-event tracking state.
            let mut current_file = match &resolved {
                ResolvedStart::FilePos { file, .. } => file.clone(),
                _ => String::new(),
            };
            let mut buffer: Vec<Value> = Vec::new();
            let mut in_txn = false;
            let mut txid: u64 = 0;
            // Bookmark of the last successfully committed transaction.
            let mut last_commit_bookmark: Option<Bookmark> = None;
            // Aggregate buffer used when batch_size == 0.
            let mut agg_records: Vec<Value> = Vec::new();

            // commit_buffer!($bm) — factors out the emit/accumulate logic shared by
            // XidEvent (InnoDB), QueryEvent("COMMIT") (non-transactional/mixed), and
            // the desync-guard flush that runs when a new transaction starts while the
            // buffer is unexpectedly non-empty.
            //
            // The macro does NOT update `in_txn` or `txid`; the caller is responsible
            // for those so that desync-guard callers (which immediately set `in_txn =
            // true` afterward) don't trigger a dead-assignment lint.
            //
            // Behaviour:
            //  - per_transaction mode: yields one StreamPage per commit (even if the
            //    buffer is empty, so the bookmark still advances).
            //  - aggregate mode (batch_size == 0): extends agg_records and records the
            //    bookmark for the trailing flush at stream end; skips empty buffers.
            //
            // Must be a macro (not a closure) because it needs to `yield` inside the
            // async_stream::try_stream! block and capture locals by mutable reference.
            macro_rules! commit_buffer {
                ($bm:expr) => {{
                    let bm: Bookmark = $bm;
                    if per_transaction {
                        // Always yield — even an empty page advances the bookmark.
                        self.note_emitted(&bm);
                        yield StreamPage {
                            records: std::mem::take(&mut buffer),
                            bookmark: Some(bm.to_value()?),
                        };
                    } else if !buffer.is_empty() {
                        // Aggregate mode: accumulate; last_commit_bookmark records
                        // the bookmark for the trailing flush at stream end.
                        // Only guard assignment in aggregate mode — per_transaction
                        // already yields the bookmark directly.
                        last_commit_bookmark = Some(bm);
                        agg_records.extend(std::mem::take(&mut buffer));
                    }
                }};
            }

            // 5. Drain loop.
            loop {
                match tokio::time::timeout(idle_timeout, stream.next()).await {
                    Ok(Some(Ok(event))) => {
                        let header = event.header();
                        let ts_ms = u64::from(header.timestamp()) * 1_000;
                        let log_pos = u64::from(header.log_pos());

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
                                // We don't need the SID/GNO here because we
                                // bookmark with file/pos on commit (see module note).
                                //
                                // Desync guard: if the buffer is non-empty when a new
                                // transaction starts, the previous transaction ended
                                // without an explicit commit boundary event — flush it
                                // now to prevent silent conflation into the next txid.
                                if !buffer.is_empty() {
                                    let bm = Bookmark::FilePos {
                                        file: current_file.clone(),
                                        pos: log_pos,
                                    };
                                    commit_buffer!(bm);
                                    txid = txid.wrapping_add(1);
                                }
                                in_txn = true;
                            }
                            Some(EventData::QueryEvent(qe)) => {
                                let q = qe.query();
                                let q_upper = q.trim().to_ascii_uppercase();
                                if q_upper == "BEGIN" {
                                    // Desync guard: same reasoning as GtidEvent.
                                    if !buffer.is_empty() {
                                        let bm = Bookmark::FilePos {
                                            file: current_file.clone(),
                                            pos: log_pos,
                                        };
                                        commit_buffer!(bm);
                                        txid = txid.wrapping_add(1);
                                    }
                                    in_txn = true;
                                } else if q_upper == "COMMIT" {
                                    // Non-transactional / mixed-engine explicit COMMIT.
                                    // MySQL emits QueryEvent("COMMIT") instead of XidEvent
                                    // for non-InnoDB engines; treat it identically.
                                    let bm = Bookmark::FilePos {
                                        file: current_file.clone(),
                                        pos: log_pos,
                                    };
                                    commit_buffer!(bm);
                                    in_txn = false;
                                    txid = txid.wrapping_add(1);
                                } else {
                                    // DDL statement — auto-commits implicitly (F36).
                                    //
                                    // A DDL statement in MySQL forces an implicit commit
                                    // of any in-progress transaction *before* it runs.
                                    // If `buffer` still holds rows here, they belong to
                                    // that just-committed transaction. We MUST flush them
                                    // (advancing the bookmark atomically with their emit)
                                    // before touching the DDL — otherwise the DDL's own
                                    // bookmark below would advance past those un-emitted
                                    // rows and they would be silently lost on resume.
                                    // Mirror the BEGIN/COMMIT/desync flush exactly.
                                    if should_flush_buffer_before_ddl(buffer.is_empty()) {
                                        let bm = Bookmark::FilePos {
                                            file: current_file.clone(),
                                            pos: log_pos,
                                        };
                                        commit_buffer!(bm);
                                        txid = txid.wrapping_add(1);
                                    }

                                    if self.config.emit_schema_changes {
                                        let envelope = build_ddl_envelope(
                                            q.as_ref(),
                                            ts_ms,
                                            &current_file,
                                            log_pos,
                                        );
                                        let bm = Bookmark::FilePos {
                                            file: current_file.clone(),
                                            pos: log_pos,
                                        };
                                        if per_transaction {
                                            self.note_emitted(&bm);
                                            yield StreamPage {
                                                records: vec![envelope],
                                                bookmark: Some(bm.to_value()?),
                                            };
                                        } else {
                                            last_commit_bookmark = Some(bm);
                                            agg_records.push(envelope);
                                        }
                                    }
                                    in_txn = false;
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
                                    // Skip filtered tables but still mark as in_txn.
                                    continue;
                                }

                                let op = op_from_rows_event(&re);
                                let lsn = json!({ "file": &current_file, "pos": log_pos });

                                for row_result in re.rows(tme) {
                                    let (before_row, after_row) = row_result.map_err(|e| {
                                        FaucetError::Source(format!(
                                            "mysql-cdc: row decode error: {e}"
                                        ))
                                    })?;

                                    let before_json = if self.config.include_columns {
                                        match &before_row {
                                            Some(r) => binlog_row_to_json(r)?,
                                            None => Value::Null,
                                        }
                                    } else {
                                        Value::Null
                                    };
                                    let after_json = match &after_row {
                                        Some(r) => binlog_row_to_json(r)?,
                                        None => Value::Null,
                                    };

                                    let envelope = build_envelope(
                                        op, ts_ms, &db, &table,
                                        before_json, after_json,
                                        lsn.clone(), txid,
                                    );

                                    // Fix 4: use let-chain (stable in edition 2024).
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
                                // InnoDB COMMIT — emit the buffered transaction.
                                let bm = Bookmark::FilePos {
                                    file: current_file.clone(),
                                    pos: log_pos,
                                };
                                commit_buffer!(bm);
                                in_txn = false;
                                txid = txid.wrapping_add(1);
                            }
                            _ => {
                                // FormatDescriptionEvent, PreviousGtidsEvent, etc. — ignored.
                            }
                        }
                    }
                    Ok(Some(Err(e))) => {
                        Err(FaucetError::Source(format!("mysql-cdc: stream error: {e}")))?;
                    }
                    // Idle timeout or stream closed: flush remaining buffer and end.
                    Ok(None) | Err(_) => {
                        // Drop any uncommitted partial transaction — the server will
                        // redeliver it from the last persisted bookmark on the next run.
                        let _ = in_txn;

                        // Aggregate mode: emit one trailing page with all accumulated
                        // records across the fetch window.  If every transaction was
                        // filtered (all rows excluded by table_included), agg_records
                        // stays empty and last_commit_bookmark is None, so the bookmark
                        // is not advanced — those transactions are harmlessly re-scanned
                        // on the next cycle (aggregate mode is for test/snapshot use only).
                        if !per_transaction
                            && let Some(bm) = last_commit_bookmark.take()
                            && !agg_records.is_empty()
                        {
                            self.note_emitted(&bm);
                            yield StreamPage {
                                records: std::mem::take(&mut agg_records),
                                bookmark: Some(bm.to_value()?),
                            };
                        }
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

/// Whether a non-empty in-progress row buffer must be flushed before handling a
/// DDL `QueryEvent` (F36).
///
/// A DDL statement implicitly auto-commits any open transaction in MySQL, so rows
/// staged in `buffer` when a DDL arrives belong to a transaction that has already
/// committed on the server. They must be emitted (and their bookmark advanced)
/// *before* the DDL is processed — otherwise the DDL event's own bookmark advances
/// past those rows and they are silently lost on resume. This mirrors the
/// BEGIN/COMMIT/desync flush contract. Returns `true` iff the buffer is non-empty.
///
/// Pure seam so the data-loss-prevention decision is unit-testable without a live
/// binlog stream.
pub(crate) fn should_flush_buffer_before_ddl(buffer_is_empty: bool) -> bool {
    !buffer_is_empty
}

/// A single page the DDL branch emits, modelled purely for testing (F36).
///
/// `bookmark_pos` is the `Some(pos)` the page advances the bookmark to, or `None`
/// when no bookmark is attached (which never happens in the DDL branch but keeps
/// the model general).
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlannedPage {
    /// Number of records on the page (DDL envelope counts as 1; buffered rows as N).
    pub record_count: usize,
    /// Whether this page carries the DDL envelope (vs. flushed buffered rows).
    pub is_ddl: bool,
    pub bookmark_pos: Option<u64>,
}

/// Pure model of the per-transaction (`batch_size != 0`) DDL-branch emit sequence,
/// faithful to the real branch in `stream_pages_impl` (F36 regression guard).
///
/// Given the count of rows currently staged in `buffer`, whether the source is
/// configured to `emit_schema_changes`, and the DDL event's `log_pos`, it returns the
/// ordered pages the branch emits. The invariant under test: **any buffered rows are
/// flushed (as their own page, with the pre-DDL bookmark) before the DDL envelope** —
/// so the DDL's bookmark can never advance past un-emitted rows.
#[cfg(test)]
pub(crate) fn plan_ddl_pages(
    buffered_rows: usize,
    emit_schema_changes: bool,
    log_pos: u64,
) -> Vec<PlannedPage> {
    let mut pages = Vec::new();
    // Flush staged rows first, exactly as the real branch's `commit_buffer!` does.
    if should_flush_buffer_before_ddl(buffered_rows == 0) {
        pages.push(PlannedPage {
            record_count: buffered_rows,
            is_ddl: false,
            bookmark_pos: Some(log_pos),
        });
    }
    if emit_schema_changes {
        pages.push(PlannedPage {
            record_count: 1,
            is_ddl: true,
            bookmark_pos: Some(log_pos),
        });
    }
    pages
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

#[cfg(test)]
mod tests {

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

    // ── F36: DDL implicit-commit must flush buffered rows before advancing ────────

    #[test]
    fn should_flush_buffer_before_ddl_decision() {
        // Non-empty buffer → must flush; empty buffer → nothing to flush.
        assert!(should_flush_buffer_before_ddl(false));
        assert!(!should_flush_buffer_before_ddl(true));
    }

    #[test]
    fn ddl_with_buffered_rows_flushes_them_first() {
        // A DDL arriving with 3 staged rows must emit those rows FIRST (their own page,
        // bookmarked at the pre-DDL position) and only then the DDL envelope. This is
        // the F36 fix: without the flush, the rows are silently lost on resume.
        let pages = plan_ddl_pages(3, true, 4567);
        assert_eq!(
            pages.len(),
            2,
            "expected a buffer-flush page then a DDL page"
        );

        // Page 1: the flushed buffered rows.
        assert_eq!(pages[0].record_count, 3);
        assert!(
            !pages[0].is_ddl,
            "first page must be the buffered rows, not the DDL"
        );
        assert_eq!(
            pages[0].bookmark_pos,
            Some(4567),
            "buffered rows must advance the bookmark — no silent loss"
        );

        // Page 2: the DDL envelope.
        assert!(pages[1].is_ddl);
        assert_eq!(pages[1].record_count, 1);
        assert_eq!(pages[1].bookmark_pos, Some(4567));
    }

    #[test]
    fn ddl_with_empty_buffer_emits_only_the_ddl() {
        // No staged rows → no flush page; just the DDL envelope.
        let pages = plan_ddl_pages(0, true, 100);
        assert_eq!(pages.len(), 1);
        assert!(pages[0].is_ddl);
        assert_eq!(pages[0].bookmark_pos, Some(100));
    }

    #[test]
    fn ddl_with_buffered_rows_flushes_even_when_schema_changes_disabled() {
        // The critical safety property: even when `emit_schema_changes` is OFF (so the
        // DDL envelope itself is dropped), buffered rows MUST still be flushed before
        // the DDL implicitly auto-commits — otherwise those rows vanish on resume.
        let pages = plan_ddl_pages(2, false, 888);
        assert_eq!(pages.len(), 1, "buffered rows must still be flushed");
        assert!(!pages[0].is_ddl);
        assert_eq!(pages[0].record_count, 2);
        assert_eq!(pages[0].bookmark_pos, Some(888));
    }

    #[test]
    fn ddl_with_empty_buffer_and_no_schema_changes_emits_nothing() {
        let pages = plan_ddl_pages(0, false, 12);
        assert!(
            pages.is_empty(),
            "nothing to emit: empty buffer + schema changes off"
        );
    }
}
