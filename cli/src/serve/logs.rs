//! Per-run log capture for SSE streaming (`GET /v1/runs/{id}/logs`, spec §12).
//!
//! [`RunLogLayer`] is a `tracing` `Layer` added to serve's global subscriber. It
//! tags every span that carries a `serve_run_id` field (the
//! `faucet.serve.run` span each run executes inside — see `runner.rs`) and, for
//! every event in such a span's scope, formats a redacted line and pushes it into
//! that run's per-run buffer: a bounded ring (for backfill) plus a `broadcast`
//! channel (for the live tail). The `/logs` handler replays the ring, then
//! streams the live tail via [`log_events`].
//!
//! **Ephemeral lifecycle.** Buffers live while a run is active plus a short drain
//! window ([`LOG_DRAIN`]) for late fetchers, then are dropped regardless of
//! `--retain-terminal-runs-secs` — only `RunRecord` metadata honours that
//! retention. Bulk/historic logs belong in the centralized tracing sink.

use crate::serve::history::{RUN_LOG_TRUNCATED_SEQ, RunHistory, RunLogLine};
use dashmap::DashMap;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};

/// Per-run ring-buffer capacity (lines). Past this the oldest line is evicted and
/// late `/logs` subscribers see a `truncated` event.
pub const RING_CAPACITY: usize = 10_000;

/// Live-tail broadcast channel depth. A `/logs` reader that falls this far behind
/// gets a `truncated` event rather than blocking producers.
pub const BROADCAST_CAPACITY: usize = 1024;

/// How long a run's log buffer survives after the run reaches a terminal state,
/// so a late `/logs` fetcher can still replay it. Independent of run-record
/// retention (spec §12).
pub const LOG_DRAIN: Duration = Duration::from_secs(60);

/// Bound on the persistence channel between capture and the writer task (#529).
/// A log storm past this drops lines (recorded via `faucet_serve_run_logs_dropped_total`)
/// rather than blocking the pipeline.
const PERSIST_CHANNEL_CAPACITY: usize = 16_384;

/// Flush a run's pending persisted lines once its buffer reaches this size (the
/// rest flush at run end).
const PERSIST_BATCH: usize = 256;

/// A message on the persistence channel (#529): a captured line, or a run-end
/// signal telling the writer to flush that run's remaining buffer.
enum PersistMsg {
    Line {
        run_id: String,
        seq: u64,
        ts: String,
        level: String,
        line: String,
        attrs: std::collections::BTreeMap<String, String>,
    },
    End {
        run_id: String,
    },
    /// Flush every run's pending lines and stop (server shutdown).
    Shutdown,
}

/// A single captured log line, tagged with a monotonic sequence number so a late
/// `/logs` subscriber can de-duplicate ring backfill against the live tail.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogLine {
    pub seq: u64,
    pub line: String,
}

/// A message on a run's live-tail broadcast channel.
#[derive(Clone, Debug)]
pub enum LogMsg {
    /// A newly captured log line.
    Line(LogLine),
    /// The run reached a terminal state; no further lines will arrive.
    End,
}

/// One run's bounded log buffer: a ring for backfill + a broadcast for live tail.
struct RunBuffer {
    ring: Mutex<VecDeque<LogLine>>,
    seq: AtomicU64,
    tx: broadcast::Sender<LogMsg>,
    ended: AtomicBool,
}

impl RunBuffer {
    fn new() -> Self {
        let (tx, _rx) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            ring: Mutex::new(VecDeque::with_capacity(64)),
            seq: AtomicU64::new(0),
            tx,
            ended: AtomicBool::new(false),
        }
    }

    /// Append a line: assign a sequence, push to the ring (evicting the oldest
    /// past the cap), and best-effort broadcast to live subscribers. Returns the
    /// assigned sequence (used as the durable-log ordering key, #529).
    fn push(&self, line: String) -> u64 {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        let entry = LogLine { seq, line };
        {
            let mut ring = self.ring.lock().expect("log ring poisoned");
            if ring.len() == RING_CAPACITY {
                ring.pop_front();
            }
            ring.push_back(entry.clone());
        }
        // No live subscribers → Err; the ring still holds the line for backfill.
        let _ = self.tx.send(LogMsg::Line(entry));
        seq
    }

    fn snapshot(&self) -> Vec<LogLine> {
        self.ring
            .lock()
            .expect("log ring poisoned")
            .iter()
            .cloned()
            .collect()
    }

    /// Mark the run terminal and notify live subscribers. `ended` is stored
    /// **before** the broadcast so a reader that misses the `End` message always
    /// observes `is_ended() == true` (see [`LogHub::reader`]).
    fn finish(&self) {
        self.ended.store(true, Ordering::SeqCst);
        let _ = self.tx.send(LogMsg::End);
    }
}

/// Shared, cheaply-cloneable registry of per-run log buffers. One instance is
/// created at subscriber install and shared between [`RunLogLayer`] and the
/// `/logs` handler via `ServerState`.
#[derive(Clone, Default)]
pub struct LogHub {
    inner: Arc<DashMap<String, Arc<RunBuffer>>>,
    /// Set once (via [`enable_persistence`](LogHub::enable_persistence)) when a
    /// durable history backend is configured (#529). `None` → ephemeral-only
    /// behavior, unchanged.
    persist: Arc<OnceLock<mpsc::Sender<PersistMsg>>>,
    /// The persistence writer task, awaited by [`LogHub::shutdown_persistence`].
    writer: Arc<std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
    /// Lines per run the full persistence queue turned away, not yet counted
    /// on the run's delivery row (the writer folds them in).
    queue_dropped: Arc<DashMap<String, u64>>,
}

impl LogHub {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get-or-create the buffer for a run (lazily, on first captured event).
    fn buffer(&self, run_id: &str) -> Arc<RunBuffer> {
        if let Some(b) = self.inner.get(run_id) {
            return Arc::clone(b.value());
        }
        Arc::clone(
            self.inner
                .entry(run_id.to_string())
                .or_insert_with(|| Arc::new(RunBuffer::new()))
                .value(),
        )
    }

    /// Turn on durable persistence of captured logs (#529): spawn a background
    /// writer task that batches lines into `history`, and route captured lines to
    /// it. `max_lines_per_run` caps how many lines are persisted per run (past it,
    /// a truncation marker is recorded). Idempotent — a second call is a no-op.
    pub fn enable_persistence(&self, history: Arc<dyn RunHistory>, max_lines_per_run: usize) {
        let (tx, rx) = mpsc::channel(PERSIST_CHANNEL_CAPACITY);
        if self.persist.set(tx).is_err() {
            return; // already enabled
        }
        let handle = tokio::spawn(persist_writer(
            rx,
            history,
            max_lines_per_run.max(1),
            Arc::clone(&self.queue_dropped),
        ));
        *self.writer.lock().expect("log writer lock poisoned") = Some(handle);
    }

    /// Flush every run's buffered log lines to the durable store and stop the
    /// writer, waiting at most `grace` (#789 SERVE-49). Called when the server
    /// shuts down, so the tail of runs that ended during the drain is kept.
    pub async fn shutdown_persistence(&self, grace: std::time::Duration) {
        let Some(tx) = self.persist.get() else {
            return;
        };
        if tx.send(PersistMsg::Shutdown).await.is_err() {
            return;
        }
        let handle = self.writer.lock().expect("log writer lock poisoned").take();
        if let Some(h) = handle
            && tokio::time::timeout(grace, h).await.is_err()
        {
            tracing::warn!("persisting the last run-log lines timed out at shutdown");
        }
    }

    /// Capture a line for a run: push to the ephemeral ring (SSE) and, when
    /// persistence is enabled, enqueue it for durable storage (#529). Called by
    /// [`RunLogLayer::on_event`] with the pre-redacted line.
    pub fn capture(&self, run_id: &str, level: &str, ts: String, line: String) {
        self.capture_with_attrs(run_id, level, ts, line, Default::default());
    }

    /// [`capture`](Self::capture) with the line's attributes (#806), which
    /// are persisted and shipped with it.
    pub fn capture_with_attrs(
        &self,
        run_id: &str,
        level: &str,
        ts: String,
        line: String,
        attrs: std::collections::BTreeMap<String, String>,
    ) {
        let seq = self.buffer(run_id).push(line.clone());
        if let Some(tx) = self.persist.get()
            && tx
                .try_send(PersistMsg::Line {
                    run_id: run_id.to_string(),
                    seq: persist_seq(seq),
                    ts,
                    level: level.to_string(),
                    line,
                    attrs,
                })
                .is_err()
        {
            metrics::counter!("faucet_serve_run_logs_dropped_total", "reason" => "queue_full")
                .increment(1);
            *self.queue_dropped.entry(run_id.to_string()).or_default() += 1;
        }
    }

    /// Append a captured line without level/timestamp metadata (ephemeral-only;
    /// used by tests and any caller that doesn't persist).
    pub fn append(&self, run_id: &str, line: String) {
        self.buffer(run_id).push(line);
    }

    /// Open a `/logs` reader: returns the ring snapshot, a live-tail receiver, and
    /// whether the run has already ended. `None` means no buffer exists for the
    /// run (never logged, or already dropped after the drain window).
    ///
    /// Subscribe-then-snapshot-then-load-`ended` ordering is deliberate: a reader
    /// that misses the `End` broadcast still observes `ended == true`, so the
    /// stream can close without hanging. Backfill/live duplicates are removed by
    /// sequence number in [`log_events`].
    pub fn reader(
        &self,
        run_id: &str,
    ) -> Option<(Vec<LogLine>, broadcast::Receiver<LogMsg>, bool)> {
        let buf = Arc::clone(self.inner.get(run_id)?.value());
        let rx = buf.tx.subscribe();
        let snapshot = buf.snapshot();
        let ended = buf.ended.load(Ordering::SeqCst);
        Some((snapshot, rx, ended))
    }

    /// Mark a run terminal: broadcast `End` so live readers can close, and flush
    /// the run's durable-log buffer (#529).
    pub fn finish(&self, run_id: &str) {
        if let Some(buf) = self.inner.get(run_id) {
            buf.finish();
        }
        if let Some(tx) = self.persist.get() {
            let _ = tx.try_send(PersistMsg::End {
                run_id: run_id.to_string(),
            });
        }
    }

    /// Drop a run's buffer, freeing its ring (called after the drain window).
    pub fn drop_run(&self, run_id: &str) {
        self.inner.remove(run_id);
    }
}

/// The durable ordering key of a captured line (#789 SERVE-35): capture time
/// in microseconds, with the in-process sequence in the low 12 bits. Several
/// instances log one run (shards, failover re-runs), and a per-process
/// counter alone collided across them; a time-based key interleaves their
/// lines in order, and the insert moves past any key still taken.
pub fn persist_seq(local: u64) -> u64 {
    let micros = chrono::Utc::now().timestamp_micros().max(0) as u64;
    (micros << 12) | (local & 0xFFF)
}

/// Per-run persistence state held by the writer task.
#[derive(Default)]
struct RunPersistState {
    /// Lines buffered for the next batch insert.
    pending: Vec<RunLogLine>,
    /// Total lines persisted for this run so far (against the per-run cap).
    persisted: u64,
    /// Whether the per-run cap has been hit (→ a truncation marker at End).
    truncated: bool,
    /// Lines dropped by the per-run cap, not yet counted on the run's delivery row.
    capped: u64,
    /// When this instance last advanced its persisted watermark while quiet.
    marked_at: Option<std::time::Instant>,
}

/// How often the writer flushes every run's pending lines, so a long run's
/// lines reach the durable store (and the log shipper) while it runs.
const PERSIST_FLUSH_EVERY: std::time::Duration = std::time::Duration::from_secs(1);

/// How often a quiet run's persisted watermark is advanced (#862 M-099).
const WRITER_MARK_EVERY: std::time::Duration = std::time::Duration::from_secs(5);

/// The watermark an idle writer may claim at `now`: every line captured a
/// second or more before it has reached the store (the queue is empty), and a
/// line being enqueued right now carries a later key.
fn idle_watermark(now: chrono::DateTime<chrono::Utc>) -> u64 {
    let at = now - chrono::Duration::seconds(1);
    ((at.timestamp_micros().max(0) as u64) << 12) | 0xFFF
}

/// Background task draining the persistence channel (#529): batches captured
/// lines per run into `history`, enforces the per-run cap, and flushes at run
/// end. All failures are logged, never fatal — persistence must never affect a
/// run.
async fn persist_writer(
    mut rx: mpsc::Receiver<PersistMsg>,
    history: Arc<dyn RunHistory>,
    max_lines_per_run: usize,
    queue_dropped: Arc<DashMap<String, u64>>,
) {
    let cap = max_lines_per_run as u64;
    let mut runs: HashMap<String, RunPersistState> = HashMap::new();

    async fn flush(history: &Arc<dyn RunHistory>, run_id: &str, st: &mut RunPersistState) {
        if st.pending.is_empty() {
            return;
        }
        let batch = std::mem::take(&mut st.pending);
        let n = batch.len() as u64;
        if let Err(e) = history.record_run_logs(run_id, &batch).await {
            tracing::warn!(run_id, error = %e, "persisting run logs failed");
            metrics::counter!("faucet_serve_run_logs_dropped_total", "reason" => "backend_error")
                .increment(n);
            count_dropped(history, run_id, n).await;
        } else {
            metrics::counter!("faucet_serve_run_log_lines_total").increment(n);
        }
    }

    /// Count lines lost before they reached the store on the run's delivery
    /// row, so its export status never claims they were shipped.
    async fn count_dropped(history: &Arc<dyn RunHistory>, run_id: &str, n: u64) {
        if let Err(e) = history.log_ship_add_dropped(run_id, n).await {
            tracing::warn!(run_id, error = %e, "recording dropped run-log lines failed");
        }
    }

    async fn fold_queue_drops(history: &Arc<dyn RunHistory>, drops: &DashMap<String, u64>) {
        let runs: Vec<String> = drops.iter().map(|e| e.key().clone()).collect();
        for run_id in runs {
            if let Some((run_id, n)) = drops.remove(&run_id)
                && n > 0
            {
                count_dropped(history, &run_id, n).await;
            }
        }
    }

    /// Advance the watermark of each run this instance writes, so peers'
    /// lines keep shipping while it has nothing to log.
    async fn mark_quiet(
        history: &Arc<dyn RunHistory>,
        runs: &mut HashMap<String, RunPersistState>,
    ) {
        let through = idle_watermark(chrono::Utc::now());
        for (run_id, st) in runs.iter_mut() {
            if st
                .marked_at
                .is_some_and(|t| t.elapsed() < WRITER_MARK_EVERY)
            {
                continue;
            }
            st.marked_at = Some(std::time::Instant::now());
            if let Err(e) = history.log_ship_writer_mark(run_id, through).await {
                tracing::warn!(run_id, error = %e, "advancing the run-log watermark failed");
            }
        }
    }

    let mut tick = tokio::time::interval(PERSIST_FLUSH_EVERY);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let msg = tokio::select! {
            m = rx.recv() => match m {
                Some(m) => m,
                None => break,
            },
            _ = tick.tick() => {
                for (run_id, st) in runs.iter_mut() {
                    flush(&history, run_id, st).await;
                }
                if rx.is_empty() {
                    mark_quiet(&history, &mut runs).await;
                }
                fold_queue_drops(&history, &queue_dropped).await;
                continue;
            }
        };
        match msg {
            PersistMsg::Line {
                run_id,
                seq,
                ts,
                level,
                line,
                attrs,
            } => {
                if !runs.contains_key(&run_id)
                    && let Err(e) = history
                        .log_ship_writer_mark(&run_id, seq.saturating_sub(1))
                        .await
                {
                    tracing::warn!(run_id, error = %e, "registering the run-log writer failed");
                }
                let st = runs.entry(run_id.clone()).or_default();
                if st.persisted >= cap {
                    if !st.truncated {
                        st.truncated = true;
                        metrics::counter!(
                            "faucet_serve_run_logs_dropped_total", "reason" => "per_run_cap"
                        )
                        .increment(1);
                    }
                    st.capped += 1;
                    crate::logship::metrics::dropped(
                        crate::logship::metrics::DropReason::MaxLines,
                        1,
                    );
                    continue;
                }
                st.persisted += 1;
                st.pending.push(RunLogLine {
                    seq,
                    ts,
                    level,
                    line,
                    attrs,
                });
                if st.pending.len() >= PERSIST_BATCH {
                    flush(&history, &run_id, st).await;
                }
            }
            PersistMsg::End { run_id } => {
                if let Some(mut st) = runs.remove(&run_id) {
                    close(&history, &run_id, &mut st).await;
                }
                fold_queue_drops(&history, &queue_dropped).await;
            }
            PersistMsg::Shutdown => {
                for (run_id, mut st) in runs.drain() {
                    close(&history, &run_id, &mut st).await;
                }
                fold_queue_drops(&history, &queue_dropped).await;
                break;
            }
        }
    }

    async fn close(history: &Arc<dyn RunHistory>, run_id: &str, st: &mut RunPersistState) {
        flush(history, run_id, st).await;
        if st.truncated {
            // Record a single sentinel so `list_run_logs` reports the gap.
            let marker = [RunLogLine {
                seq: RUN_LOG_TRUNCATED_SEQ,
                ts: String::new(),
                level: "WARN".to_string(),
                line: "log truncated: per-run cap reached".to_string(),
                attrs: Default::default(),
            }];
            if let Err(e) = history.record_run_logs(run_id, &marker).await {
                tracing::warn!(run_id, error = %e, "persisting run-log truncation marker failed");
            }
        }
        if st.capped > 0 {
            if let Err(e) = history.log_ship_add_dropped(run_id, st.capped).await {
                tracing::warn!(run_id, error = %e, "recording capped run-log lines failed");
            }
            st.capped = 0;
        }
        if let Err(e) = history.log_ship_writer_done(run_id).await {
            tracing::warn!(run_id, error = %e, "releasing the run-log watermark failed");
        }
    }
}

/// An SSE-bound log event, decoupled from `axum`'s `Event` so the streaming logic
/// (ring replay → live tail, de-dup, lag handling) is unit-testable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogEvent {
    /// A log line.
    Log(String),
    /// `n` live-tail lines were dropped because the reader fell behind.
    Truncated(u64),
    /// Terminal: the run finished and the stream should close.
    End,
}

/// Build the ordered event stream for a `/logs` request: replay the ring
/// snapshot, then forward the live tail, de-duplicating against the snapshot by
/// sequence number and mapping `broadcast` lag to [`LogEvent::Truncated`].
pub fn log_events(
    snapshot: Vec<LogLine>,
    mut rx: broadcast::Receiver<LogMsg>,
    ended: bool,
) -> impl futures::Stream<Item = LogEvent> {
    use tokio::sync::broadcast::error::RecvError;
    async_stream::stream! {
        let mut last_seq: Option<u64> = None;
        for entry in snapshot {
            last_seq = Some(entry.seq);
            yield LogEvent::Log(entry.line);
        }
        // The run already ended before we subscribed: the ring is the whole story.
        if ended {
            yield LogEvent::End;
            return;
        }
        loop {
            match rx.recv().await {
                Ok(LogMsg::Line(entry)) => {
                    if last_seq.is_none_or(|s| entry.seq > s) {
                        last_seq = Some(entry.seq);
                        yield LogEvent::Log(entry.line);
                    }
                }
                Ok(LogMsg::End) => {
                    yield LogEvent::End;
                    break;
                }
                Err(RecvError::Lagged(n)) => {
                    yield LogEvent::Truncated(n);
                }
                Err(RecvError::Closed) => {
                    yield LogEvent::End;
                    break;
                }
            }
        }
    }
}

// ── tracing layer ───────────────────────────────────────────────────────────

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

/// Span extension marking a span (and, via scope walking, its descendants) as
/// belonging to a serve run.
#[derive(Clone)]
struct RunIdExt(String);

/// Extracts a `serve_run_id` field value from span attributes.
#[derive(Default)]
struct RunIdVisitor(Option<String>);

impl Visit for RunIdVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "serve_run_id" {
            self.0 = Some(value.to_string());
        }
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        // `%run_id` records via Display → Debug-of-DisplayValue, i.e. unquoted.
        if field.name() == "serve_run_id" && self.0.is_none() {
            self.0 = Some(format!("{value:?}"));
        }
    }
}

/// Tracing layer that captures events tagged with a `serve_run_id` into the
/// [`LogHub`] for SSE streaming. Added to serve's global subscriber alongside the
/// redacting fmt layer (`observability.rs`).
pub struct RunLogLayer {
    hub: LogHub,
}

impl RunLogLayer {
    pub fn new(hub: LogHub) -> Self {
        Self { hub }
    }
}

impl<S> Layer<S> for RunLogLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_record(&self, id: &Id, values: &tracing::span::Record<'_>, ctx: Context<'_, S>) {
        crate::logship::capture::on_record(id, values, &ctx);
    }

    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        crate::logship::capture::on_new_span(attrs, id, &ctx);
        let mut visitor = RunIdVisitor::default();
        attrs.record(&mut visitor);
        if let Some(run_id) = visitor.0
            && let Some(span) = ctx.span(id)
        {
            span.extensions_mut().insert(RunIdExt(run_id));
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let Some(run_id) = ctx.event_scope(event).and_then(|scope| {
            scope
                .from_root()
                .find_map(|span| span.extensions().get::<RunIdExt>().map(|ext| ext.0.clone()))
        }) else {
            return;
        };
        if crate::logship::capture::is_shipping_noise(event.metadata().target()) {
            return;
        }
        // Render the line self-describing: `<ts> <LEVEL> <target>: <msg>`, mirroring
        // the stderr fmt layer's format. The ephemeral ring / SSE / console only ever
        // see `line`, so the timestamp must live in the line text itself; the separate
        // `ts` field and the span attributes travel with it to the durable store and
        // the log shipper (#806). Body and attributes are redacted at capture.
        let ev = crate::logship::capture::capture(event, &ctx);
        let line = ev.rendered();
        let mut attrs = ev.attrs;
        attrs.remove("serve_run_id");
        self.hub
            .capture_with_attrs(&run_id, ev.level, ev.ts, line, attrs);
    }
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)] // tests drive time and raw clients directly
mod tests {
    use super::*;
    use futures::StreamExt;

    #[test]
    fn later_recorded_fields_and_noise_targets() {
        use tracing_subscriber::layer::SubscriberExt;
        let hub = LogHub::new();
        let subscriber = tracing_subscriber::registry().with(RunLogLayer::new(hub.clone()));
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!(
                "faucet.serve.run",
                serve_run_id = "run-rec",
                shard = tracing::field::Empty
            );
            span.record("shard", "s-7");
            let _g = span.enter();
            tracing::info!("kept");
            tracing::info!(target: "hyper::proto", "transport noise");
        });
        let (lines, _rx, _ended) = hub.reader("run-rec").unwrap();
        assert_eq!(lines.len(), 1);
        assert!(lines[0].line.ends_with(": kept"));
    }

    /// Records drop counts and fails every log insert.
    #[derive(Default)]
    struct DropCounter {
        fail_inserts: bool,
        dropped: std::sync::Mutex<HashMap<String, u64>>,
        marks: std::sync::Mutex<Vec<(String, u64)>>,
    }

    #[async_trait::async_trait]
    impl RunHistory for DropCounter {
        async fn claim_idempotency(
            &self,
            _key: &str,
            _fingerprint: &str,
            _run_id: &str,
            _window: std::time::Duration,
        ) -> Result<crate::serve::history::Claim, crate::serve::history::HistoryError> {
            Ok(crate::serve::history::Claim::Fresh)
        }
        async fn upsert(
            &self,
            _rec: &crate::serve::history::RunRecord,
        ) -> Result<(), crate::serve::history::HistoryError> {
            Ok(())
        }
        async fn get(
            &self,
            _id: &str,
        ) -> Result<Option<crate::serve::history::RunRecord>, crate::serve::history::HistoryError>
        {
            Ok(None)
        }
        async fn list(
            &self,
            _filter: &crate::serve::history::ListFilter,
        ) -> Result<crate::serve::history::ListPage, crate::serve::history::HistoryError> {
            Ok(crate::serve::history::ListPage {
                runs: Vec::new(),
                next_cursor: None,
            })
        }
        async fn delete(
            &self,
            _id: &str,
        ) -> Result<crate::serve::history::DeleteOutcome, crate::serve::history::HistoryError>
        {
            Ok(crate::serve::history::DeleteOutcome::NotFound)
        }
        async fn purge_expired(
            &self,
            _retain_for: std::time::Duration,
        ) -> Result<usize, crate::serve::history::HistoryError> {
            Ok(0)
        }
        async fn recover_orphans(&self) -> Result<usize, crate::serve::history::HistoryError> {
            Ok(0)
        }
        async fn record_run_logs(
            &self,
            _run_id: &str,
            _lines: &[RunLogLine],
        ) -> Result<(), crate::serve::history::HistoryError> {
            if self.fail_inserts {
                return Err(crate::serve::history::HistoryError::Backend("down".into()));
            }
            Ok(())
        }
        async fn log_ship_add_dropped(
            &self,
            run_id: &str,
            n: u64,
        ) -> Result<(), crate::serve::history::HistoryError> {
            *self
                .dropped
                .lock()
                .unwrap()
                .entry(run_id.into())
                .or_default() += n;
            Ok(())
        }
        async fn log_ship_writer_mark(
            &self,
            run_id: &str,
            through: u64,
        ) -> Result<(), crate::serve::history::HistoryError> {
            self.marks.lock().unwrap().push((run_id.into(), through));
            Ok(())
        }
        fn degraded(&self) -> bool {
            false
        }
    }

    /// Review of #868 (cli F5): a writer registers its watermark when it first
    /// sees a run, before its first batch reaches the store, so a first batch
    /// that lands late is still ahead of what the shipper acknowledges.
    #[tokio::test]
    async fn a_writer_registers_its_watermark_before_its_first_batch() {
        let history = Arc::new(DropCounter {
            fail_inserts: true,
            ..Default::default()
        });
        let hub = LogHub::new();
        hub.enable_persistence(history.clone(), 100);
        let before = (chrono::Utc::now().timestamp_micros() as u64) << 12;
        hub.capture("r1", "INFO", "t".into(), "first".into());
        let mut first = None;
        for _ in 0..100 {
            first = history.marks.lock().unwrap().first().cloned();
            if first.is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let (run, through) = first.expect("the writer registered the run");
        assert_eq!(run, "r1");
        assert!(
            through + 1 >= before,
            "registered just below the first line"
        );
        hub.shutdown_persistence(std::time::Duration::from_secs(5))
            .await;
    }

    #[test]
    fn the_idle_watermark_trails_now_by_a_second() {
        let now = chrono::Utc::now();
        let w = idle_watermark(now);
        let behind = ((now - chrono::Duration::seconds(1)).timestamp_micros() as u64) << 12;
        assert_eq!(w & !0xFFF, behind);
        assert!(w < (now.timestamp_micros() as u64) << 12);
    }

    /// #862 M-099: the writer advances a quiet run's watermark and releases it
    /// when the run ends here, so the shipper is held back only while this
    /// instance may still persist a line.
    #[tokio::test]
    async fn the_writer_advances_a_quiet_runs_watermark_and_releases_it_at_the_end() {
        let history = Arc::new(crate::serve::history::memory::MemoryHistory::new(
            std::time::Duration::from_secs(60),
        ));
        let hub = LogHub::new();
        hub.enable_persistence(history.clone(), 100);
        let before = idle_watermark(chrono::Utc::now());
        hub.capture("r1", "INFO", "t".into(), "a".into());
        let stale = std::time::Duration::from_secs(60);
        let mut floor = None;
        for _ in 0..60 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            floor = history.log_ship_writer_floor("r1", stale).await.unwrap();
            if floor.is_some_and(|f| f > before) {
                break;
            }
        }
        assert!(
            floor.is_some_and(|f| f > before),
            "the quiet run's watermark moved past its last line: {floor:?}"
        );
        hub.finish("r1");
        for _ in 0..50 {
            if history
                .log_ship_writer_floor("r1", stale)
                .await
                .unwrap()
                .is_none()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert_eq!(
            history.log_ship_writer_floor("r1", stale).await.unwrap(),
            None
        );
        hub.shutdown_persistence(std::time::Duration::from_secs(5))
            .await;
    }

    /// #862 M-099: lines lost before the store — a full persistence queue or
    /// a failed insert — are counted on the run's delivery row, so its export
    /// status cannot read "every line acknowledged".
    #[tokio::test]
    async fn lines_lost_before_the_store_are_counted_as_dropped() {
        let failing = Arc::new(DropCounter {
            fail_inserts: true,
            ..Default::default()
        });
        let hub = LogHub::new();
        hub.enable_persistence(failing.clone(), 100);
        hub.capture("r1", "INFO", "t".into(), "a".into());
        hub.capture("r1", "INFO", "t".into(), "b".into());
        hub.finish("r1");
        hub.shutdown_persistence(std::time::Duration::from_secs(5))
            .await;
        assert_eq!(failing.dropped.lock().unwrap().get("r1"), Some(&2));

        let ok = Arc::new(DropCounter::default());
        let hub = LogHub::new();
        hub.enable_persistence(ok.clone(), 100);
        *hub.queue_dropped.entry("r2".into()).or_default() += 3;
        hub.shutdown_persistence(std::time::Duration::from_secs(5))
            .await;
        assert_eq!(ok.dropped.lock().unwrap().get("r2"), Some(&3));
        assert!(hub.queue_dropped.is_empty());
    }

    #[test]
    fn ring_caps_and_orders_by_seq() {
        let hub = LogHub::new();
        for i in 0..(RING_CAPACITY + 5) {
            hub.append("r", format!("line-{i}"));
        }
        let (snapshot, _rx, _ended) = hub.reader("r").unwrap();
        assert_eq!(snapshot.len(), RING_CAPACITY, "ring must cap at capacity");
        // The five oldest lines were evicted; line-5 is now the front.
        assert_eq!(snapshot.first().unwrap().line, "line-5");
        assert!(
            snapshot.windows(2).all(|w| w[0].seq < w[1].seq),
            "sequence numbers must be strictly increasing"
        );
    }

    #[test]
    fn reader_none_for_unknown_run() {
        let hub = LogHub::new();
        assert!(hub.reader("nope").is_none());
    }

    #[test]
    fn finish_sets_ended_flag() {
        let hub = LogHub::new();
        hub.append("r", "x".into());
        hub.finish("r");
        let (snapshot, _rx, ended) = hub.reader("r").unwrap();
        assert!(ended, "reader must observe ended after finish");
        assert_eq!(snapshot.len(), 1);
    }

    #[test]
    fn drop_run_frees_buffer() {
        let hub = LogHub::new();
        hub.append("r", "x".into());
        assert!(hub.reader("r").is_some());
        hub.drop_run("r");
        assert!(hub.reader("r").is_none());
    }

    #[tokio::test]
    async fn ended_buffer_streams_snapshot_then_end() {
        let hub = LogHub::new();
        hub.append("r", "a".into());
        hub.append("r", "b".into());
        hub.finish("r");
        let (snapshot, rx, ended) = hub.reader("r").unwrap();
        let events: Vec<LogEvent> = log_events(snapshot, rx, ended).collect().await;
        assert_eq!(
            events,
            vec![
                LogEvent::Log("a".into()),
                LogEvent::Log("b".into()),
                LogEvent::End
            ]
        );
    }

    #[tokio::test]
    async fn snapshot_then_live_dedups_by_seq() {
        let (tx, rx) = broadcast::channel(8);
        // seq 0 lands in BOTH the snapshot and the broadcast — must not duplicate.
        let _ = tx.send(LogMsg::Line(LogLine {
            seq: 0,
            line: "a".into(),
        }));
        let _ = tx.send(LogMsg::Line(LogLine {
            seq: 1,
            line: "b".into(),
        }));
        let _ = tx.send(LogMsg::End);
        let snapshot = vec![LogLine {
            seq: 0,
            line: "a".into(),
        }];
        let events: Vec<LogEvent> = log_events(snapshot, rx, false).collect().await;
        assert_eq!(
            events,
            vec![
                LogEvent::Log("a".into()),
                LogEvent::Log("b".into()),
                LogEvent::End
            ]
        );
    }

    #[tokio::test]
    async fn truncated_emitted_on_broadcast_lag() {
        // Fill the channel past capacity before the receiver reads → Lagged.
        let (tx, rx) = broadcast::channel(2);
        for i in 0..5u64 {
            let _ = tx.send(LogMsg::Line(LogLine {
                seq: i,
                line: format!("l{i}"),
            }));
        }
        let _ = tx.send(LogMsg::End);
        let events: Vec<LogEvent> = log_events(vec![], rx, false).collect().await;
        assert!(
            events.iter().any(|e| matches!(e, LogEvent::Truncated(_))),
            "a lagging reader must get a Truncated event: {events:?}"
        );
        assert_eq!(events.last(), Some(&LogEvent::End));
    }

    #[test]
    fn layer_captures_events_in_run_span_only() {
        use tracing_subscriber::layer::SubscriberExt;
        let hub = LogHub::new();
        let subscriber = tracing_subscriber::registry().with(RunLogLayer::new(hub.clone()));
        tracing::subscriber::with_default(subscriber, || {
            // An event outside any serve-run span is ignored.
            tracing::info!("orphan event");
            let span = tracing::info_span!("faucet.serve.run", serve_run_id = "run-xyz");
            let _g = span.enter();
            tracing::info!("run-span event 7f3a");
        });
        let (snapshot, _rx, _ended) = hub.reader("run-xyz").expect("buffer for the run exists");
        let captured = snapshot
            .iter()
            .find(|l| l.line.contains("run-span event 7f3a"))
            .unwrap_or_else(|| panic!("in-span event must be captured: {snapshot:?}"));
        // The rendered line is self-describing: it leads with an RFC3339 timestamp,
        // then the level and target, mirroring the stderr fmt format.
        assert!(
            captured.line.starts_with(char::is_numeric)
                && captured.line.contains('T')
                && captured.line.contains("INFO "),
            "captured line must lead with a timestamp then level: {:?}",
            captured.line
        );
        assert!(
            snapshot.iter().all(|l| !l.line.contains("orphan")),
            "events outside the run span must not be captured"
        );
        // No buffer is created for events that never had a serve_run_id span.
        assert!(hub.reader("nonexistent").is_none());
    }

    #[test]
    fn a_root_run_span_created_inside_another_runs_span_keeps_its_own_log() {
        use tracing_subscriber::layer::SubscriberExt;
        let hub = LogHub::new();
        let subscriber = tracing_subscriber::registry().with(RunLogLayer::new(hub.clone()));
        tracing::subscriber::with_default(subscriber, || {
            let x = tracing::info_span!("faucet.serve.run", serve_run_id = "run-x");
            let _x = x.enter();
            let stale = tracing::info_span!("faucet.dlq.route");
            let _stale = stale.enter();
            let y = tracing::info_span!(parent: None, "faucet.serve.run", serve_run_id = "run-y");
            let _y = y.enter();
            tracing::info!("from run y");
        });
        let (y_lines, _rx, _ended) = hub.reader("run-y").expect("run-y has a buffer");
        assert!(y_lines.iter().any(|l| l.line.contains("from run y")));
        let x_lines = hub
            .reader("run-x")
            .map(|(lines, _, _)| lines)
            .unwrap_or_default();
        assert!(
            x_lines.iter().all(|l| !l.line.contains("from run y")),
            "run y's line leaked into run x's log"
        );
    }
}
