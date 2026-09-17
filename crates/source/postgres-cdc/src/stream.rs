//! `PostgresCdcSource` — public `Source` implementation.

use crate::config::PostgresCdcSourceConfig;
use crate::pgoutput::decoder::decode_message;
use crate::pgoutput::messages::{
    Delete, Insert, Message, Relation, Truncate, TupleCell, TupleData, Update,
};
use crate::pgoutput::registry::RelationRegistry;
use crate::pgoutput::values::text_to_json;
use crate::replication::{
    self, ReplicationEvent, ReplicationParams, postgres_clock_to_unix_ms, recv, send_status_update,
};
use crate::state::{Bookmark, format_lsn, parse_lsn, state_key};
use async_trait::async_trait;
use faucet_core::{FaucetError, Source, Stream, StreamPage};
use serde_json::{Map, Value, json};
use std::collections::HashMap;
use std::pin::Pin;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Field added to a `before`/`after` image listing the columns Postgres elided
/// because their out-of-line (TOAST) value wasn't rewritten by the change.
///
/// Reserved, and **part of the emitted record** — it is an ordinary key in the
/// row image, so a downstream sink in `auto_map` mode sees it as a column and a
/// schema-drift policy sees it as an addition. Named here rather than inlined at
/// the insertion site so the reserved key has one definition and a consumer can
/// filter on it (#654 L27). Postgres-specific (TOAST is a Postgres storage
/// concept), so it lives in this crate rather than `faucet-core`.
pub const UNCHANGED_TOAST_FIELD: &str = "__unchanged_toast__";

pub struct PostgresCdcSource {
    config: PostgresCdcSourceConfig,
    state_key_value: String,
    /// Bookmark provided by `apply_start_bookmark`, applied at the start of
    /// the next fetch cycle. Becomes the new "confirmed_flush_lsn" we
    /// advertise to Postgres.
    pending_bookmark: Mutex<Option<Bookmark>>,
    /// Last LSN we have told Postgres is durable (advertised as the slot's
    /// `confirmed_flush_lsn`, which authorises WAL recycling up to it).
    ///
    /// Advanced **only** by `apply_start_bookmark` — i.e. after the pipeline
    /// has durably persisted a bookmark — or seeded from `start_lsn` config.
    /// It is never advanced from decoded WAL (commit-decode time) or from
    /// keepalive `wal_end`, because those positions are not yet durable
    /// downstream; doing so would let Postgres discard WAL for unwritten
    /// changes and lose data on a crash (#78/#1).
    confirmed_lsn: Mutex<u64>,
}

impl PostgresCdcSource {
    pub async fn new(config: PostgresCdcSourceConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let key = state_key(&config.slot_name);
        let initial_lsn = match config.start_lsn.as_deref() {
            Some(s) => parse_lsn(s)?,
            None => 0,
        };
        Ok(Self {
            config,
            state_key_value: key,
            pending_bookmark: Mutex::new(None),
            confirmed_lsn: Mutex::new(initial_lsn),
        })
    }

    /// Drop this source's replication slot on the server, freeing the WAL it
    /// pins. A no-op if the slot doesn't exist; errors if the slot is still
    /// active (in use by a live replication connection). Call this when
    /// decommissioning a `permanent` slot so it doesn't leak WAL (#78/#12).
    pub async fn drop_slot(&self) -> Result<(), FaucetError> {
        replication::drop_slot(
            &self.config.connection_url,
            &self.config.slot_name,
            &self.config.tls,
        )
        .await
    }
}

#[async_trait]
impl Source for PostgresCdcSource {
    async fn fetch_with_context(
        &self,
        ctx: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let (records, _bookmark) = self.fetch_with_context_incremental(ctx).await?;
        Ok(records)
    }

    /// Drain the replication stream into a single `Vec<Value>` plus the
    /// bookmark of the most recent COMMIT.
    ///
    /// Implemented by collecting [`Source::stream_pages`] with the
    /// `batch_size = 0` sentinel — that sentinel coalesces every transaction
    /// in the run window into a single trailing page, which exactly matches
    /// the historical `fetch_with_context_incremental` contract (one
    /// aggregated buffer, one max-LSN bookmark). The streaming pipeline
    /// (`Pipeline::run` / `run_stream`) drives `stream_pages` directly with
    /// the per-source `batch_size` config field instead so it gets
    /// per-transaction durability.
    async fn fetch_with_context_incremental(
        &self,
        ctx: &HashMap<String, Value>,
    ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        use futures::StreamExt;
        let mut pages = self.stream_pages_with_batch_size(ctx, 0);
        let mut all: Vec<Value> = Vec::new();
        let mut bookmark: Option<Value> = None;
        while let Some(page) = pages.next().await {
            let page = page?;
            all.extend(page.records);
            if page.bookmark.is_some() {
                bookmark = page.bookmark;
            }
        }
        Ok((all, bookmark))
    }

    /// Per-transaction streaming.
    ///
    /// Each committed transaction is emitted as its own
    /// [`StreamPage`] with `bookmark = Some(commit_lsn)`. Because the
    /// pipeline flushes the sink and persists the bookmark on every page
    /// that carries one, a mid-stream crash recovers from the last fully-
    /// committed transaction with no partial-transaction leakage.
    ///
    /// **Atomic transactions.** A transaction is never split across pages.
    /// If a single transaction's record count exceeds
    /// [`PostgresCdcSourceConfig::batch_size`] it is still emitted as one
    /// page; `batch_size` is advisory.
    ///
    /// **`batch_size = 0`** is the "no batching" sentinel: every committed
    /// transaction during the run window is accumulated into a single
    /// trailing page with `bookmark = max(commit_lsn)`. This negates
    /// per-transaction durability and is only useful for tests / initial
    /// snapshot runs.
    ///
    /// The trait-level `batch_size` argument is intentionally ignored in
    /// favour of the config field (matches the convention used by the
    /// query-mode postgres source and the rest source).
    fn stream_pages<'a>(
        &'a self,
        ctx: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        self.stream_pages_with_batch_size(ctx, self.config.batch_size)
    }

    fn config_schema(&self) -> Value {
        let schema = schemars::schema_for!(PostgresCdcSourceConfig);
        serde_json::to_value(&schema).unwrap_or(Value::Null)
    }

    fn state_key(&self) -> Option<String> {
        Some(self.state_key_value.clone())
    }

    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        let parsed = Bookmark::from_value(bookmark)?;
        // Update confirmed_lsn so the next initial status update (in the next
        // fetch cycle) advertises the correct position.
        *self.confirmed_lsn.lock().await = parsed.as_u64()?;
        *self.pending_bookmark.lock().await = Some(parsed);
        Ok(())
    }

    async fn capture_resume_position(&self) -> Result<Option<Value>, FaucetError> {
        // A temporary slot is dropped when the capture connection closes, so it
        // cannot retain WAL across the snapshot — require a permanent slot.
        if matches!(self.config.slot_type, crate::config::SlotType::Temporary) {
            return Err(FaucetError::Config(
                "postgres-cdc: replication snapshot handoff requires a permanent slot \
                 (slot_type: permanent) so WAL is retained across the snapshot — a \
                 temporary slot is dropped when the capture connection closes"
                    .into(),
            ));
        }
        let lsn = replication::ensure_slot_and_current_lsn(
            &self.config.connection_url,
            &self.config.slot_name,
            self.config.create_slot_if_missing,
            self.config.slot_type,
            &self.config.tls,
        )
        .await?;
        Ok(Some(crate::state::Bookmark::from_u64(lsn).to_value()?))
    }

    fn supports_exactly_once(&self) -> bool {
        // Durable monotonic LSN + deterministic replay from it + per-transaction
        // (per-page) bookmarks persisted only after the sink confirms — the
        // requirements for exactly-once delivery.
        true
    }

    fn connector_name(&self) -> &'static str {
        "postgres-cdc"
    }

    fn dataset_uri(&self) -> String {
        format!(
            "{}?publication={}",
            faucet_core::redact_uri_credentials(&self.config.connection_url),
            self.config.publication_name
        )
    }

    /// Preflight probe that does **not** start replication.
    ///
    /// The default [`Source::check`] would call `stream_pages`, which opens the
    /// replication stream and consumes/holds WAL (a side effect that pins server
    /// resources) — unacceptable as a preflight. Instead we open a *normal*
    /// (non-replication) SQL connection — the same connection style
    /// `ensure_slot` uses — and inspect the slot catalog without touching the
    /// replication protocol:
    ///
    /// - connection fails → `auth` probe `Fail` (could not connect / authenticate),
    /// - connected but the slot row is absent → `slot` probe `Skip`
    ///   (faucet can create it on the first run),
    /// - slot present → `slot` probe `Pass`.
    ///
    /// The whole call is bounded by `ctx.timeout`.
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};
        use sqlx::ConnectOptions as _;
        use sqlx::postgres::{PgConnectOptions, PgConnection};

        let start = std::time::Instant::now();

        // A bad connection URL is a config error, not an unreachable server.
        let opts: PgConnectOptions = match self.config.connection_url.parse() {
            Ok(o) => o,
            Err(e) => {
                return Ok(CheckReport::single(Probe::fail_hint(
                    "auth",
                    start.elapsed(),
                    format!("invalid connection URL: {e}"),
                    "connection_url must be a valid postgres:// URL",
                )));
            }
        };

        // Bound the whole connect+query under ctx.timeout so an unreachable
        // host doesn't hang the probe.
        let probe = async {
            let mut conn: PgConnection = opts.connect().await.map_err(|e| {
                Probe::fail_hint(
                    "auth",
                    start.elapsed(),
                    format!("could not connect: {e}"),
                    "verify the host is reachable and credentials are valid",
                )
            })?;

            let row: Option<(String,)> = sqlx::query_as(
                "SELECT slot_name::text FROM pg_replication_slots WHERE slot_name = $1",
            )
            .bind(&self.config.slot_name)
            .fetch_optional(&mut conn)
            .await
            .map_err(|e| {
                Probe::fail(
                    "slot",
                    start.elapsed(),
                    format!("could not query pg_replication_slots: {e}"),
                )
            })?;

            Ok::<Probe, Probe>(match row {
                Some(_) => Probe::pass("slot", start.elapsed()),
                None => Probe::skip(
                    "slot",
                    format!(
                        "replication slot {} does not exist yet (faucet run can create it)",
                        self.config.slot_name
                    ),
                ),
            })
        };

        let probe = match tokio::time::timeout(ctx.timeout, probe).await {
            Ok(Ok(p)) | Ok(Err(p)) => p,
            Err(_elapsed) => Probe::fail_hint(
                "auth",
                start.elapsed(),
                "connection timed out",
                "the database did not respond within the check timeout",
            ),
        };
        Ok(CheckReport::single(probe))
    }
}

impl PostgresCdcSource {
    /// Shared streaming implementation used by both `stream_pages` (per-
    /// transaction page emission when `batch_size > 0`) and the legacy
    /// `fetch_with_context_incremental` (single-page aggregation when
    /// `batch_size == 0`).
    ///
    /// The slot lifecycle (`connect` → `ensure_slot` → `start_replication`)
    /// and Standby Status Update bootstrap are identical to the pre-Plan-15
    /// behaviour. The only difference is when records cross the page
    /// boundary: on every COMMIT (`batch_size > 0`) or once at the end of
    /// the run window (`batch_size == 0`).
    fn stream_pages_with_batch_size<'a>(
        &'a self,
        _ctx: &'a HashMap<String, Value>,
        batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let max_messages = self.config.max_messages.unwrap_or(usize::MAX);
        let idle_timeout = self.config.idle_timeout;
        let per_transaction = batch_size != 0;

        Box::pin(async_stream::try_stream! {
            // 1. Resolve start_lsn for THIS fetch cycle.
            let pending = {
                let mut g = self.pending_bookmark.lock().await;
                g.take()
            };
            let start_lsn = if let Some(b) = pending.as_ref() {
                let lsn = b.as_u64()?;
                *self.confirmed_lsn.lock().await = lsn;
                Some(lsn)
            } else {
                self.config
                    .start_lsn
                    .as_deref()
                    .map(parse_lsn)
                    .transpose()?
            };

            // 2. Open replication connection + ensure slot + START_REPLICATION.
            let params = ReplicationParams {
                connection_url: &self.config.connection_url,
                slot_name: &self.config.slot_name,
                publication_name: &self.config.publication_name,
                proto_version: self.config.proto_version,
                create_slot_if_missing: self.config.create_slot_if_missing,
                start_lsn,
                status_update_interval: self.config.status_update_interval,
                tcp_keepalive: self.config.tcp_keepalive,
                slot_type: self.config.slot_type,
                tls: &self.config.tls,
            };
            let client = replication::connect(&params).await?;
            replication::ensure_slot(
                &client,
                &self.config.connection_url,
                &self.config.slot_name,
                self.config.create_slot_if_missing,
                self.config.slot_type,
                &self.config.tls,
            )
            .await?;

            // Advance the slot's confirmed_flush_lsn to the durable resume
            // point BEFORE opening the stream. For a logical slot,
            // START_REPLICATION resumes decoding from confirmed_flush_lsn (the
            // client-supplied start LSN does not skip already-committed
            // transactions), so this is the only way to skip changes we have
            // already consumed and durably persisted. `start_lsn` is set only
            // from a durable bookmark (apply_start_bookmark) or the start_lsn
            // config override; when it is `None` (no durable resume point) we
            // do NOT advance, so an interrupted run with no persisted bookmark
            // redelivers rather than loses data (#78/#1).
            if let Some(lsn) = start_lsn {
                // Both the slot advance and START_REPLICATION require the slot
                // to be inactive; on a rapid re-run the prior connection may not
                // have released it yet ("slot is active"). Retry with bounded
                // backoff instead of failing the whole run (#146 M12).
                replication::retry_on_slot_active(self.config.slot_acquire_retries, || {
                    replication::advance_slot(
                        &self.config.connection_url,
                        &self.config.slot_name,
                        lsn,
                        &self.config.tls,
                    )
                })
                .await?;
            }

            let mut duplex = replication::retry_on_slot_active(
                self.config.slot_acquire_retries,
                || replication::start_replication(&client, &params),
            )
            .await?;

            // Send an initial Standby Status Update advertising the same
            // durable position so the server's bookkeeping is consistent.
            let initial_confirmed = *self.confirmed_lsn.lock().await;
            send_status_update(&mut duplex, initial_confirmed, false).await?;

            // 4. Drain the replication stream until idle_timeout, ctrl_c, or
            //    max_messages. Per-transaction mode (`batch_size > 0`) emits
            //    one page per COMMIT carrying `bookmark = Some(commit_lsn)`.
            //    Aggregated mode (`batch_size == 0`) accumulates every
            //    transaction's records into one buffer and emits a single
            //    trailing page with `bookmark = max(commit_lsn)`.
            let mut registry = RelationRegistry::new();
            let mut state = TxnState {
                max_staged_records: self.config.max_staged_records,
                ..TxnState::default()
            };
            let mut agg_records: Vec<Value> = Vec::new();
            let mut total_records: usize = 0;
            let mut last_message_at = Instant::now();

            loop {
                let idle_deadline = last_message_at + idle_timeout;
                let budget = idle_deadline
                    .checked_duration_since(Instant::now())
                    .unwrap_or(Duration::ZERO);

                // Tracks per-iteration outcomes that we cannot propagate
                // directly from inside `tokio::select!` arms while remaining
                // inside `try_stream!`.
                let mut stop = false;
                let mut just_committed: Option<(u64, Vec<Value>)> = None;
                let mut fatal: Option<FaucetError> = None;
                let mut unexpected_end = false;
                tokio::select! {
                    biased;
                    _ = tokio::signal::ctrl_c() => {
                        tracing::info!("postgres-cdc: ctrl_c received, stopping cleanly");
                        stop = true;
                    }
                    ev = tokio::time::timeout(budget, recv(&mut duplex)) => {
                        match ev {
                            Ok(Ok(Some(event))) => {
                                // Reset on any server activity, not just committed
                                // output, so idle_timeout never fires mid-transaction
                                // while WAL is still flowing.
                                last_message_at = Instant::now();
                                let was_in_txn = state.in_txn;
                                let pre_commit_count = state.last_committed;
                                let mut committed_records: Vec<Value> = Vec::new();
                                if let Err(e) = handle_event(
                                    event,
                                    &mut registry,
                                    &mut state,
                                    &mut committed_records,
                                ) {
                                    fatal = Some(e);
                                } else if was_in_txn
                                    && !state.in_txn
                                    && state.last_committed != pre_commit_count
                                {
                                    // A COMMIT was just processed and
                                    // `committed_records` holds the drained
                                    // staged records.
                                    let lsn = state.last_committed
                                        .expect("last_committed set on commit");
                                    total_records += committed_records.len();
                                    just_committed = Some((lsn, committed_records));
                                }
                            }
                            Ok(Ok(None)) => {
                                unexpected_end = true;
                            }
                            Ok(Err(e)) => {
                                fatal = Some(e);
                            }
                            Err(_timeout) => {
                                tracing::debug!(
                                    "postgres-cdc: idle_timeout reached, stopping"
                                );
                                stop = true;
                            }
                        }
                    }
                }

                if let Some(e) = fatal {
                    Err(e)?;
                }
                if unexpected_end {
                    Err(FaucetError::Source(
                        "postgres-cdc: replication stream ended unexpectedly".into(),
                    ))?;
                }
                if let Some((lsn, drained)) = just_committed {
                    // NOTE: we deliberately do NOT advance `confirmed_lsn` here.
                    // `confirmed_lsn` is the position advertised to Postgres as
                    // `confirmed_flush_lsn` (which lets the server recycle WAL),
                    // and it must only ever reflect data the consumer has
                    // *durably* persisted. The only durable signal is the
                    // bookmark the pipeline persists after the sink flush, which
                    // arrives back via `apply_start_bookmark` at the start of the
                    // next run. Advancing here at commit-decode time would tell
                    // Postgres to discard WAL for changes that were never written
                    // downstream — a crash in that window loses data (#78/#1).
                    if per_transaction {
                        let bookmark = Some(Bookmark::from_u64(lsn).to_value()?);
                        yield StreamPage {
                            records: drained,
                            bookmark,
                        };
                    } else {
                        agg_records.extend(drained);
                    }
                    if total_records >= max_messages {
                        stop = true;
                    }
                }

                if stop {
                    break;
                }
            }

            // 5. In aggregated mode emit the single trailing page (carrying
            //    the max LSN seen). In per-transaction mode the trailing
            //    `state.staged` is an *uncommitted* partial transaction and
            //    must be dropped — Postgres will redeliver after the next
            //    START_REPLICATION.
            if !per_transaction
                && let Some(lsn) = state.last_committed
            {
                let bookmark = Some(Bookmark::from_u64(lsn).to_value()?);
                yield StreamPage {
                    records: agg_records,
                    bookmark,
                };
            }

            tracing::info!(
                records = total_records,
                batch_size,
                "postgres-cdc: stream complete",
            );
        })
    }
}

/// One decoded tuple, split into its values and the names of any unchanged
/// (large/TOAST) columns whose value the server didn't re-send.
struct TupleRow {
    values: Map<String, Value>,
    unchanged_toast: Vec<String>,
}

/// In-flight transaction state while draining the replication stream.
#[derive(Default)]
struct TxnState {
    /// Records produced inside the current BEGIN..COMMIT, buffered until
    /// COMMIT is seen so partial transactions never leak into the output.
    /// On COMMIT, `handle_event` drains this into the caller-supplied
    /// `out: &mut Vec<Value>`.
    staged: Vec<Value>,
    /// commit_lsn of the most recently fully-applied transaction.
    last_committed: Option<u64>,
    /// commit_ts (Postgres epoch micros) of the in-progress transaction,
    /// set by BEGIN.
    in_progress_ts: i64,
    /// commit_lsn announced by the in-progress BEGIN (== final_lsn).
    in_progress_lsn: u64,
    /// Whether we are currently inside a BEGIN..COMMIT pair.
    in_txn: bool,
    /// Optional cap on `staged.len()` for a single transaction. `None` means
    /// unbounded. Exceeding it aborts the run with a typed error instead of
    /// risking an OOM-kill on a huge bulk transaction (see
    /// [`PostgresCdcSourceConfig::max_staged_records`]).
    max_staged_records: Option<usize>,
}

impl TxnState {
    /// Stage one decoded change record, enforcing
    /// [`max_staged_records`](Self::max_staged_records).
    fn push_staged(&mut self, record: Value) -> Result<(), FaucetError> {
        if let Some(max) = self.max_staged_records
            && self.staged.len() >= max
        {
            return Err(FaucetError::Source(format!(
                "postgres-cdc: in-progress transaction exceeded max_staged_records ({max}); \
                 aborting to avoid unbounded memory growth. Raise max_staged_records or \
                 reduce the size of the source transaction."
            )));
        }
        self.staged.push(record);
        Ok(())
    }
}

fn handle_event(
    event: ReplicationEvent,
    registry: &mut RelationRegistry,
    state: &mut TxnState,
    out: &mut Vec<Value>,
) -> Result<(), FaucetError> {
    match event {
        ReplicationEvent::Begin {
            final_lsn,
            commit_time_micros,
            xid: _,
        } => {
            if state.in_txn {
                // A second BEGIN without an intervening COMMIT is a protocol
                // desync: silently discarding the staged records would drop
                // committed-but-unemitted changes. Fail fast so the run
                // restarts cleanly from the durable bookmark (#78/#46).
                return Err(FaucetError::Source(format!(
                    "postgres-cdc: BEGIN received while a previous transaction was still \
                     in progress ({} records staged) — replication stream desync",
                    state.staged.len()
                )));
            }
            state.in_txn = true;
            state.in_progress_lsn = final_lsn.as_u64();
            state.in_progress_ts = commit_time_micros;
            state.staged.clear();
        }
        ReplicationEvent::Commit {
            lsn: _,
            commit_time_micros: _,
            end_lsn,
        } => {
            if !state.in_txn {
                return Err(FaucetError::Source(
                    "postgres-cdc: COMMIT without BEGIN".into(),
                ));
            }
            // Drain staged records into the caller-supplied output buffer.
            // The caller is responsible for emitting a StreamPage with these
            // records and the bookmark.
            //
            // The bookmark is the commit's `end_lsn` — the WAL position
            // immediately AFTER the commit record — not the commit_lsn. To
            // resume *past* a consumed transaction the slot's
            // confirmed_flush_lsn must be set to a position beyond the commit
            // record; advancing only to commit_lsn leaves the commit record
            // unconfirmed and Postgres redelivers the whole transaction
            // (#78/#1). `end_lsn` is the position a standby reports as flushed.
            //
            // The exactly-once-across-resume boundary (a transaction whose
            // commit lands exactly at the persisted bookmark is delivered once,
            // not skipped or duplicated) is exercised by the Docker integration
            // tests `resume_from_bookmark_skips_already_consumed` and
            // `lsn_not_advanced_without_durable_bookmark_redelivers` (#78 LOW).
            out.append(&mut state.staged);
            state.last_committed = Some(end_lsn.as_u64());
            state.in_txn = false;
        }
        ReplicationEvent::XLogData { data, .. } => {
            let msg = decode_message(&data)?;
            handle_pgoutput(msg, registry, state)?;
        }
        ReplicationEvent::Message { .. } => {
            // pg_logical_emit_message — a user-emitted logical message, never a
            // table change. Intentionally ignored by this row-oriented source.
        }
        // KeepAlive and StoppedAt are filtered inside recv(). Any other variant
        // is one this decoder doesn't understand — it may carry table data, so
        // dropping it silently risks data loss. Fail fast instead (#78/#46).
        other => {
            return Err(FaucetError::Source(format!(
                "postgres-cdc: unhandled ReplicationEvent variant {other:?} — refusing to \
                 continue rather than risk silently dropping change data"
            )));
        }
    }
    Ok(())
}

fn handle_pgoutput(
    msg: Message,
    registry: &mut RelationRegistry,
    state: &mut TxnState,
) -> Result<(), FaucetError> {
    match msg {
        Message::Relation(r) => registry.insert(r),
        Message::Origin | Message::Type => {} // ignored
        Message::Insert(i) => stage_insert(state, registry, i)?,
        Message::Update(u) => stage_update(state, registry, u)?,
        Message::Delete(d) => stage_delete(state, registry, d)?,
        Message::Truncate(t) => stage_truncate(state, registry, t)?,
        // Begin/Commit pgoutput messages should never arrive here — the
        // pgwire-replication library decodes them into structured
        // ReplicationEvent::Begin / Commit variants, handled in handle_event.
        // If we see one, log a warning and ignore.
        Message::Begin(_) | Message::Commit(_) => {
            tracing::warn!(
                "postgres-cdc: pgoutput Begin/Commit reached pgoutput decoder; \
                 pgwire-replication should have intercepted it"
            );
        }
    }
    Ok(())
}

fn stage_insert(
    state: &mut TxnState,
    registry: &RelationRegistry,
    i: Insert,
) -> Result<(), FaucetError> {
    let rel = registry.get(i.relation_oid)?;
    let after = tuple_to_object(rel, &i.new)?;
    let r = record(rel, "insert", state, None, Some(after));
    state.push_staged(r)
}

fn stage_update(
    state: &mut TxnState,
    registry: &RelationRegistry,
    u: Update,
) -> Result<(), FaucetError> {
    let rel = registry.get(u.relation_oid)?;
    let before = match &u.old {
        Some(t) => Some(tuple_to_object(rel, t)?),
        None => None,
    };
    let after = tuple_to_object(rel, &u.new)?;
    let r = record(rel, "update", state, before, Some(after));
    state.push_staged(r)
}

fn stage_delete(
    state: &mut TxnState,
    registry: &RelationRegistry,
    d: Delete,
) -> Result<(), FaucetError> {
    let rel = registry.get(d.relation_oid)?;
    let before = Some(tuple_to_object(rel, &d.old)?);
    let r = record(rel, "delete", state, before, None);
    state.push_staged(r)
}

fn stage_truncate(
    state: &mut TxnState,
    registry: &RelationRegistry,
    t: Truncate,
) -> Result<(), FaucetError> {
    for oid in &t.relation_oids {
        let rel = registry.get(*oid)?;
        let r = record(rel, "truncate", state, None, None);
        state.push_staged(r)?;
    }
    Ok(())
}

fn record(
    rel: &Relation,
    op: &str,
    state: &TxnState,
    before: Option<TupleRow>,
    after: Option<TupleRow>,
) -> Value {
    fn to_value(row: TupleRow) -> Value {
        let mut o = row.values;
        if !row.unchanged_toast.is_empty() {
            o.insert(UNCHANGED_TOAST_FIELD.into(), json!(row.unchanged_toast));
        }
        Value::Object(o)
    }
    let mut obj = Map::new();
    obj.insert("op".into(), json!(op));
    obj.insert("schema".into(), json!(rel.namespace));
    obj.insert("table".into(), json!(rel.name));
    obj.insert("lsn".into(), json!(format_lsn(state.in_progress_lsn)));
    obj.insert(
        "ts_ms".into(),
        json!(postgres_clock_to_unix_ms(state.in_progress_ts)),
    );
    obj.insert("before".into(), before.map(to_value).unwrap_or(Value::Null));
    obj.insert("after".into(), after.map(to_value).unwrap_or(Value::Null));
    Value::Object(obj)
}

/// Convert a tuple's text cells to a [`TupleRow`].
fn tuple_to_object(rel: &Relation, tup: &TupleData) -> Result<TupleRow, FaucetError> {
    if tup.cells.len() != rel.columns.len() {
        return Err(FaucetError::Source(format!(
            "postgres-cdc: tuple has {} cells but relation {}.{} has {} columns",
            tup.cells.len(),
            rel.namespace,
            rel.name,
            rel.columns.len()
        )));
    }
    let mut values = Map::with_capacity(rel.columns.len());
    let mut unchanged_toast = Vec::new();
    for (col, cell) in rel.columns.iter().zip(&tup.cells) {
        match cell {
            TupleCell::Null => {
                values.insert(col.name.clone(), Value::Null);
            }
            TupleCell::UnchangedToast => {
                unchanged_toast.push(col.name.clone());
            }
            TupleCell::Text(s) => {
                values.insert(col.name.clone(), text_to_json(col.type_oid, s)?);
            }
        }
    }
    Ok(TupleRow {
        values,
        unchanged_toast,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pgoutput::messages::{ColumnDesc, ReplicaIdentity};
    use crate::replication::ReplicationEvent;
    use pgwire_replication::Lsn;

    fn rel_users() -> Relation {
        Relation {
            oid: 16384,
            namespace: "public".into(),
            name: "users".into(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![
                ColumnDesc {
                    flags: 1,
                    name: "id".into(),
                    type_oid: 23,
                    type_modifier: -1,
                },
                ColumnDesc {
                    flags: 0,
                    name: "name".into(),
                    type_oid: 25,
                    type_modifier: -1,
                },
            ],
        }
    }

    fn xlogdata(payload: Vec<u8>) -> ReplicationEvent {
        ReplicationEvent::XLogData {
            wal_start: Lsn::from_u64(0),
            wal_end: Lsn::from_u64(0x16A_4F88),
            server_time_micros: 0,
            data: bytes::Bytes::from(payload),
        }
    }

    fn insert_payload(relation_oid: u32, cells: &[(&str, &str)]) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();
        buf.push(b'I');
        buf.extend_from_slice(&relation_oid.to_be_bytes());
        buf.push(b'N');
        buf.extend_from_slice(&(cells.len() as u16).to_be_bytes());
        for (_, val) in cells {
            text_cell(&mut buf, val);
        }
        buf
    }

    /// 'U' relation 'O' fullold 'N' new — exercises REPLICA IDENTITY FULL.
    fn update_full_payload(
        relation_oid: u32,
        old_cells: &[(&str, &str)],
        new_cells: &[(&str, &str)],
    ) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();
        buf.push(b'U');
        buf.extend_from_slice(&relation_oid.to_be_bytes());
        buf.push(b'O');
        buf.extend_from_slice(&(old_cells.len() as u16).to_be_bytes());
        for (_, val) in old_cells {
            text_cell(&mut buf, val);
        }
        buf.push(b'N');
        buf.extend_from_slice(&(new_cells.len() as u16).to_be_bytes());
        for (_, val) in new_cells {
            text_cell(&mut buf, val);
        }
        buf
    }

    /// 'D' relation 'O' fullold — REPLICA IDENTITY FULL delete.
    fn delete_full_payload(relation_oid: u32, old_cells: &[(&str, &str)]) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();
        buf.push(b'D');
        buf.extend_from_slice(&relation_oid.to_be_bytes());
        buf.push(b'O');
        buf.extend_from_slice(&(old_cells.len() as u16).to_be_bytes());
        for (_, val) in old_cells {
            text_cell(&mut buf, val);
        }
        buf
    }

    /// 'T' flags=0 oids... — truncate without cascade/restart_identity.
    fn truncate_payload(relation_oids: &[u32]) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();
        buf.push(b'T');
        buf.extend_from_slice(&(relation_oids.len() as u32).to_be_bytes());
        buf.push(0u8); // flags
        for oid in relation_oids {
            buf.extend_from_slice(&oid.to_be_bytes());
        }
        buf
    }

    fn text_cell(buf: &mut Vec<u8>, val: &str) {
        buf.push(b't');
        buf.extend_from_slice(&(val.len() as u32).to_be_bytes());
        buf.extend_from_slice(val.as_bytes());
    }

    fn begin_event(final_lsn: u64) -> ReplicationEvent {
        ReplicationEvent::Begin {
            final_lsn: Lsn::from_u64(final_lsn),
            xid: 1,
            commit_time_micros: 0,
        }
    }

    fn commit_event(lsn: u64) -> ReplicationEvent {
        ReplicationEvent::Commit {
            lsn: Lsn::from_u64(lsn),
            end_lsn: Lsn::from_u64(lsn + 0x10),
            commit_time_micros: 0,
        }
    }

    #[test]
    fn full_transaction_promotes_to_output_on_commit() {
        let mut registry = RelationRegistry::new();
        registry.insert(rel_users());
        let mut state = TxnState::default();
        let mut out = vec![];

        handle_event(begin_event(0x16A_4F88), &mut registry, &mut state, &mut out).unwrap();
        assert!(out.is_empty());

        handle_event(
            xlogdata(insert_payload(16384, &[("id", "1"), ("name", "alice")])),
            &mut registry,
            &mut state,
            &mut out,
        )
        .unwrap();
        assert!(out.is_empty(), "records stay staged until COMMIT");

        handle_event(
            commit_event(0x16A_4F88),
            &mut registry,
            &mut state,
            &mut out,
        )
        .unwrap();

        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["op"], "insert");
        assert_eq!(out[0]["schema"], "public");
        assert_eq!(out[0]["table"], "users");
        assert_eq!(out[0]["lsn"], "0/16A4F88");
        assert_eq!(out[0]["after"]["id"], 1);
        assert_eq!(out[0]["after"]["name"], "alice");
        assert_eq!(out[0]["before"], Value::Null);

        // The bookmark is the commit's `end_lsn` (the resume position just
        // past the commit record), not the commit_lsn. `commit_event` sets
        // end_lsn = commit_lsn + 0x10. The record's own "lsn" field above is
        // still the commit_lsn (display/identity), which is intentional.
        assert_eq!(state.last_committed, Some(0x16A_4F88 + 0x10));
    }

    #[test]
    fn staging_beyond_max_staged_records_aborts() {
        // Regression for #78/#2: a single transaction that stages more records
        // than `max_staged_records` must abort with a typed error rather than
        // buffering unboundedly (OOM risk).
        let mut registry = RelationRegistry::new();
        registry.insert(rel_users());
        let mut state = TxnState {
            max_staged_records: Some(2),
            ..TxnState::default()
        };
        let mut out = vec![];

        handle_event(begin_event(0x16A_4F88), &mut registry, &mut state, &mut out).unwrap();
        // First two inserts stage fine.
        for id in ["1", "2"] {
            handle_event(
                xlogdata(insert_payload(16384, &[("id", id), ("name", "x")])),
                &mut registry,
                &mut state,
                &mut out,
            )
            .unwrap();
        }
        // The third exceeds the cap and must error.
        let err = handle_event(
            xlogdata(insert_payload(16384, &[("id", "3"), ("name", "x")])),
            &mut registry,
            &mut state,
            &mut out,
        )
        .unwrap_err();
        assert!(
            format!("{err}").contains("max_staged_records"),
            "error must name the cap: {err}"
        );
        assert!(matches!(err, FaucetError::Source(_)));
    }

    #[test]
    fn no_cap_allows_large_transactions() {
        // With max_staged_records = None (default) an arbitrarily large
        // transaction stages without error.
        let mut registry = RelationRegistry::new();
        registry.insert(rel_users());
        let mut state = TxnState::default();
        let mut out = vec![];

        handle_event(begin_event(0x16A_4F88), &mut registry, &mut state, &mut out).unwrap();
        for id in 0..50 {
            handle_event(
                xlogdata(insert_payload(
                    16384,
                    &[("id", &id.to_string()), ("name", "x")],
                )),
                &mut registry,
                &mut state,
                &mut out,
            )
            .unwrap();
        }
        handle_event(
            commit_event(0x16A_4F88),
            &mut registry,
            &mut state,
            &mut out,
        )
        .unwrap();
        assert_eq!(out.len(), 50);
    }

    #[test]
    fn commit_without_begin_errors() {
        let mut registry = RelationRegistry::new();
        let mut state = TxnState::default();
        let mut out = vec![];

        let err = handle_event(
            ReplicationEvent::Commit {
                lsn: Lsn::from_u64(1),
                end_lsn: Lsn::from_u64(2),
                commit_time_micros: 0,
            },
            &mut registry,
            &mut state,
            &mut out,
        )
        .unwrap_err();
        assert!(format!("{err}").contains("COMMIT without BEGIN"));
    }

    #[test]
    fn double_begin_errors() {
        // Regression for #78/#46: a second BEGIN without an intervening COMMIT
        // is a stream desync and must abort, not silently discard staged rows.
        let mut registry = RelationRegistry::new();
        registry.insert(rel_users());
        let mut state = TxnState::default();
        let mut out = vec![];

        handle_event(begin_event(0x100), &mut registry, &mut state, &mut out).unwrap();
        handle_event(
            xlogdata(insert_payload(16384, &[("id", "1"), ("name", "alice")])),
            &mut registry,
            &mut state,
            &mut out,
        )
        .unwrap();

        let err =
            handle_event(begin_event(0x200), &mut registry, &mut state, &mut out).unwrap_err();
        assert!(format!("{err}").contains("desync"), "{err}");
    }

    #[test]
    fn unknown_relation_in_insert_errors() {
        let mut registry = RelationRegistry::new();
        let mut state = TxnState::default();
        let mut out = vec![];

        handle_event(begin_event(1), &mut registry, &mut state, &mut out).unwrap();
        // Insert references relation 99999 which is not in the registry.
        let err = handle_event(
            xlogdata(insert_payload(99999, &[("id", "1"), ("name", "alice")])),
            &mut registry,
            &mut state,
            &mut out,
        )
        .unwrap_err();
        assert!(format!("{err}").contains("99999"));
    }

    #[test]
    fn update_with_replica_identity_full_emits_before_and_after() {
        let mut registry = RelationRegistry::new();
        registry.insert(rel_users());
        let mut state = TxnState::default();
        let mut out = vec![];

        handle_event(begin_event(0x16A_4F88), &mut registry, &mut state, &mut out).unwrap();
        handle_event(
            xlogdata(update_full_payload(
                16384,
                &[("id", "1"), ("name", "alice")],
                &[("id", "1"), ("name", "alice2")],
            )),
            &mut registry,
            &mut state,
            &mut out,
        )
        .unwrap();
        handle_event(
            commit_event(0x16A_4F88),
            &mut registry,
            &mut state,
            &mut out,
        )
        .unwrap();

        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["op"], "update");
        assert_eq!(out[0]["before"]["id"], 1);
        assert_eq!(out[0]["before"]["name"], "alice");
        assert_eq!(out[0]["after"]["name"], "alice2");
    }

    #[test]
    fn delete_with_replica_identity_full_emits_before_only() {
        let mut registry = RelationRegistry::new();
        registry.insert(rel_users());
        let mut state = TxnState::default();
        let mut out = vec![];

        handle_event(begin_event(0x16A_4F88), &mut registry, &mut state, &mut out).unwrap();
        handle_event(
            xlogdata(delete_full_payload(
                16384,
                &[("id", "1"), ("name", "alice")],
            )),
            &mut registry,
            &mut state,
            &mut out,
        )
        .unwrap();
        handle_event(
            commit_event(0x16A_4F88),
            &mut registry,
            &mut state,
            &mut out,
        )
        .unwrap();

        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["op"], "delete");
        assert_eq!(out[0]["before"]["id"], 1);
        assert_eq!(out[0]["before"]["name"], "alice");
        assert_eq!(out[0]["after"], Value::Null);
    }

    #[test]
    fn truncate_emits_one_record_per_relation() {
        let mut registry = RelationRegistry::new();
        registry.insert(rel_users());
        // Build a second relation so the truncate-list has two known OIDs.
        let mut second = rel_users();
        second.oid = 16385;
        second.name = "orders".into();
        registry.insert(second);

        let mut state = TxnState::default();
        let mut out = vec![];

        handle_event(begin_event(0x16A_4F88), &mut registry, &mut state, &mut out).unwrap();
        handle_event(
            xlogdata(truncate_payload(&[16384, 16385])),
            &mut registry,
            &mut state,
            &mut out,
        )
        .unwrap();
        handle_event(
            commit_event(0x16A_4F88),
            &mut registry,
            &mut state,
            &mut out,
        )
        .unwrap();

        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|r| r["op"] == "truncate"));
        let tables: Vec<_> = out.iter().map(|r| r["table"].as_str().unwrap()).collect();
        assert!(tables.contains(&"users"));
        assert!(tables.contains(&"orders"));
    }

    #[test]
    fn unchanged_toast_in_before_surfaces_via_metadata() {
        // Exercise Fix 1: a REPLICA IDENTITY FULL update with an UnchangedToast
        // cell in the old tuple must record the column name in
        // before.__unchanged_toast__.
        let mut registry = RelationRegistry::new();
        registry.insert(rel_users());
        let mut state = TxnState::default();
        let mut out = vec![];

        handle_event(begin_event(0x16A_4F88), &mut registry, &mut state, &mut out).unwrap();
        // Hand-build an UPDATE where the OLD tuple's `name` cell is 'u' (unchanged TOAST).
        let mut buf: Vec<u8> = Vec::new();
        buf.push(b'U');
        buf.extend_from_slice(&16384u32.to_be_bytes());
        buf.push(b'O');
        buf.extend_from_slice(&2u16.to_be_bytes());
        // id = text "1"
        text_cell(&mut buf, "1");
        // name = unchanged TOAST
        buf.push(b'u');
        // New tuple: id=1, name="alice2"
        buf.push(b'N');
        buf.extend_from_slice(&2u16.to_be_bytes());
        text_cell(&mut buf, "1");
        text_cell(&mut buf, "alice2");
        handle_event(xlogdata(buf), &mut registry, &mut state, &mut out).unwrap();
        handle_event(
            commit_event(0x16A_4F88),
            &mut registry,
            &mut state,
            &mut out,
        )
        .unwrap();

        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["before"]["__unchanged_toast__"], json!(["name"]));
        assert!(out[0]["before"].get("name").is_none());
        assert_eq!(out[0]["before"]["id"], 1);
        assert_eq!(out[0]["after"]["name"], "alice2");
    }

    #[test]
    fn unchanged_toast_field_name_is_pinned() {
        // The name is part of the emitted record, so downstream configs (a sink
        // column list, a contract, a drift allowlist) are written against this
        // literal — renaming it is a wire-format change, not a refactor.
        assert_eq!(UNCHANGED_TOAST_FIELD, "__unchanged_toast__");
    }

    // dataset_uri is a pure-config method; the source requires a live DB to
    // construct (async + real PG connection), so we verify the logic directly.
    #[test]
    fn dataset_uri_strips_credentials() {
        let redacted = faucet_core::redact_uri_credentials("postgres://u:p@h:5432/db");
        let uri = format!("{}?publication={}", redacted, "my_pub");
        assert_eq!(uri, "postgres://h:5432/db?publication=my_pub");
    }
}
