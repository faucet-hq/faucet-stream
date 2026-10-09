//! `faucet serve` log shipping (#806): the run-history log store (#529) is
//! the durable buffer; this module ships it over OTLP, keeps each run's
//! delivery watermark, and applies delivery-aware retention and the buffer
//! bounds.
//!
//! In a cluster every instance runs the shipper; a run is shipped by the
//! instance holding its delivery lease (`log_ship_claim`), so one run is never
//! shipped by two instances at once, and a peer picks up a run whose shipper
//! died once its lease lapses.

use crate::logship::metrics::{self, BufferGauges, DropReason};
use crate::logship::{
    DeliveryState, LinkVars, LogExportView, ShipLine, derive_view, record::failing_past,
    render_link,
};
use crate::serve::config::ServeConfig;
use crate::serve::history::{HistoryError, LogShipRow, RunHistory, RunRecord};
use chrono::{DateTime, Utc};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

/// Lines per export request from the serve store.
pub const SHIP_BATCH: usize = 512;
/// Batches one run may send per pass before the shipper moves on.
const MAX_BATCHES_PER_RUN: usize = 16;
/// Lines younger than this are left for the next pass, so a line another
/// instance captured a moment earlier (and is still flushing) is not skipped.
pub const SETTLE: Duration = Duration::from_secs(2);
/// Shipping cadence while exports succeed.
const SHIP_EVERY: Duration = Duration::from_secs(2);
/// How often retention + bounds run.
const MAINTAIN_EVERY: Duration = Duration::from_secs(60);

/// The durable-store ordering key of an instant: the high bits of
/// [`crate::serve::logs::persist_seq`].
pub fn seq_at(t: DateTime<Utc>) -> u64 {
    ((t.timestamp_micros().max(0) as u64) << 12) | 0xFFF
}

fn seq_time(seq: u64) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp_micros((seq >> 12) as i64)
}

/// The server's log-export settings and runtime.
pub struct LogExport {
    configured: bool,
    #[cfg(feature = "otel")]
    otel: Option<faucet_core::OtelConfig>,
    #[cfg(feature = "otel")]
    exporter: tokio::sync::OnceCell<Option<crate::logship::otlp::OtlpLogExporter>>,
    retention: Duration,
    max_age: Duration,
    max_bytes: u64,
    link_template: Option<String>,
    notify_after: Duration,
    lease_ttl: Duration,
    #[cfg(feature = "notify")]
    notifiers: dashmap::DashMap<String, Arc<crate::notify::Notifier>>,
}

/// What one shipping pass did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ShipSummary {
    pub shipped: u64,
    pub failed: bool,
    /// Runs another instance is shipping.
    pub skipped: usize,
}

/// What one retention pass did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MaintainSummary {
    pub purged: usize,
    pub dropped_max_age: u64,
    pub dropped_max_bytes: u64,
    pub gauges: BufferGauges,
}

impl LogExport {
    /// From the server config: shipping is on when `--otel-config` exports
    /// `logs` (and the binary has the `otel` feature).
    pub fn from_config(cfg: &ServeConfig) -> Self {
        let wants = cfg
            .otel
            .as_ref()
            .is_some_and(|o| o.exports(faucet_core::OtelSignal::Logs));
        Self {
            configured: wants && cfg!(feature = "otel"),
            #[cfg(feature = "otel")]
            otel: cfg.otel.clone(),
            #[cfg(feature = "otel")]
            exporter: tokio::sync::OnceCell::new(),
            retention: cfg.log_retention,
            max_age: cfg.log_buffer.max_age,
            max_bytes: cfg.log_buffer.max_bytes,
            link_template: cfg.log_buffer.link_template.clone(),
            notify_after: cfg.log_buffer.notify_after,
            lease_ttl: cfg.lease_ttl,
            #[cfg(feature = "notify")]
            notifiers: dashmap::DashMap::new(),
        }
    }

    /// Whether log shipping is on.
    pub fn configured(&self) -> bool {
        self.configured
    }

    /// The `--otel-config` asked for logs but this binary cannot ship them.
    pub fn unsupported(cfg: &ServeConfig) -> bool {
        !cfg!(feature = "otel")
            && cfg
                .otel
                .as_ref()
                .is_some_and(|o| o.exports(faucet_core::OtelSignal::Logs))
    }

    /// Remember a run's notifier so a later export failure reaches the
    /// channels of the run's own `notifications:` block.
    #[cfg(feature = "notify")]
    pub fn register_notifier(&self, run_id: &str, notifier: Arc<crate::notify::Notifier>) {
        if self.configured {
            self.notifiers.insert(run_id.to_string(), notifier);
        }
    }

    #[cfg(feature = "notify")]
    fn notifier_for(
        &self,
        rec: Option<&RunRecord>,
        run_id: &str,
    ) -> Option<Arc<crate::notify::Notifier>> {
        if let Some(n) = self.notifiers.get(run_id) {
            return Some(n.clone());
        }
        let n = notifier_from_body(rec?.config_body.as_deref()?)?;
        self.notifiers.insert(run_id.to_string(), n.clone());
        Some(n)
    }

    /// The link to a run's logs in the log service.
    pub fn link(&self, rec: &RunRecord) -> Option<String> {
        let t = self.link_template.as_deref()?;
        Some(render_link(
            t,
            &LinkVars {
                run_id: rec.run_id.clone(),
                pipeline: rec.name.clone().unwrap_or_default(),
                row: String::new(),
                tenant: rec.tenant.clone().unwrap_or_default(),
                started_at: rec.started_at.or(Some(rec.submitted_at)),
                ended_at: rec.finished_at,
            },
        ))
    }

    /// A run's delivery state.
    pub async fn delivery(
        history: &dyn RunHistory,
        run_id: &str,
    ) -> Result<(Option<LogShipRow>, DeliveryState), HistoryError> {
        let row = history.log_ship_row(run_id).await?;
        let after = row.as_ref().and_then(|r| r.delivered_seq);
        let pending = history.run_log_stats(run_id, after, None).await?;
        let st = DeliveryState {
            delivered_seq: after,
            total_seq: row.as_ref().map(|r| r.total_seq).filter(|t| *t > 0),
            pending_lines: pending.lines,
            dropped_lines: row.as_ref().map(|r| r.dropped).unwrap_or(0),
            last_error: row.as_ref().and_then(|r| r.last_error.clone()),
            last_attempt_at: row.as_ref().and_then(|r| r.last_attempt_at),
            failing_since: row.as_ref().and_then(|r| r.failing_since),
            delivered_at: row.as_ref().and_then(|r| r.delivered_at),
        };
        Ok((row, st))
    }

    /// The `log_export` object for `GET /v1/runs/{id}`.
    pub async fn view(&self, history: &dyn RunHistory, rec: &RunRecord) -> LogExportView {
        let st = match Self::delivery(history, &rec.run_id).await {
            Ok((_, st)) => st,
            Err(e) => {
                tracing::warn!(run_id = %rec.run_id, error = %e, "reading the log-delivery state failed");
                DeliveryState::default()
            }
        };
        let mut v = derive_view(self.configured, &st);
        v.link = self.link(rec);
        v
    }

    #[cfg(feature = "otel")]
    async fn exporter(&self) -> Option<&crate::logship::otlp::OtlpLogExporter> {
        self.exporter
            .get_or_init(|| async {
                let cfg = self.otel.as_ref()?;
                match crate::logship::otlp::OtlpLogExporter::new(cfg) {
                    Ok(e) => Some(e),
                    Err(e) => {
                        tracing::error!("log shipping disabled: {e}");
                        None
                    }
                }
            })
            .await
            .as_ref()
    }

    /// One shipping pass over every run with undelivered lines.
    pub async fn ship_once(&self, history: &dyn RunHistory) -> Result<ShipSummary, HistoryError> {
        self.ship_with(history, SETTLE).await
    }

    /// [`ship_once`](Self::ship_once), leaving lines younger than `settle`.
    pub async fn ship_with(
        &self,
        history: &dyn RunHistory,
        settle: Duration,
    ) -> Result<ShipSummary, HistoryError> {
        let mut summary = ShipSummary::default();
        if !self.configured || !self.exporter_ready().await {
            return Ok(summary);
        }
        for row in history.log_ship_rows(true).await? {
            if !history.log_ship_claim(&row.run_id, self.lease_ttl).await? {
                summary.skipped += 1;
                continue;
            }
            let res = self.ship_run(history, &row, settle, &mut summary).await;
            if let Err(e) = history.log_ship_release(&row.run_id).await {
                tracing::warn!(run_id = %row.run_id, error = %e, "releasing the log-delivery lease failed");
            }
            res?;
        }
        Ok(summary)
    }

    async fn exporter_ready(&self) -> bool {
        #[cfg(feature = "otel")]
        {
            self.exporter().await.is_some()
        }
        #[cfg(not(feature = "otel"))]
        {
            false
        }
    }

    async fn export(
        &self,
        run_attrs: &BTreeMap<String, String>,
        lines: &[ShipLine],
    ) -> Result<u64, String> {
        #[cfg(feature = "otel")]
        {
            match self.exporter().await {
                Some(e) => e.export(run_attrs, lines).await,
                None => Err("the OTLP log exporter could not be built".into()),
            }
        }
        #[cfg(not(feature = "otel"))]
        {
            let _ = (run_attrs, lines);
            Err("this binary was built without the `otel` feature".into())
        }
    }

    async fn ship_run(
        &self,
        history: &dyn RunHistory,
        row: &LogShipRow,
        settle: Duration,
        summary: &mut ShipSummary,
    ) -> Result<(), HistoryError> {
        let rec = history.get(&row.run_id).await?;
        let run_attrs = run_attrs(&row.run_id, rec.as_ref());
        let cap = seq_at(Utc::now() - chrono::Duration::from_std(settle).unwrap_or_default());
        let mut after = row.delivered_seq;
        for _ in 0..MAX_BATCHES_PER_RUN {
            let page = history
                .list_run_logs(&row.run_id, after, SHIP_BATCH)
                .await?;
            let full = page.lines.len() == SHIP_BATCH;
            let lines: Vec<ShipLine> = page
                .lines
                .into_iter()
                .filter(|l| l.seq <= cap)
                .map(|l| ShipLine::from_rendered(l.seq, &l.ts, &l.level, &l.line, l.attrs))
                .collect();
            let Some(last) = lines.last().map(|l| l.seq) else {
                break;
            };
            match self.export(&run_attrs, &lines).await {
                Ok(_) => {
                    metrics::shipped(lines.len() as u64);
                    summary.shipped += lines.len() as u64;
                    if !history.log_ship_ack(&row.run_id, last).await? {
                        tracing::warn!(run_id = %row.run_id, "lost the log-delivery lease mid-pass");
                        return Ok(());
                    }
                    after = Some(last);
                }
                Err(e) => {
                    metrics::export_error();
                    summary.failed = true;
                    tracing::warn!(run_id = %row.run_id, error = %e, "shipping run logs failed");
                    history.log_ship_fail(&row.run_id, &e).await?;
                    break;
                }
            }
            if !full {
                break;
            }
        }
        self.after_pass(history, rec.as_ref(), &row.run_id).await
    }

    /// Notify a run whose export has been failing past `notify_after`, and
    /// forget its notifier once everything is delivered.
    async fn after_pass(
        &self,
        history: &dyn RunHistory,
        rec: Option<&RunRecord>,
        run_id: &str,
    ) -> Result<(), HistoryError> {
        let (row, st) = Self::delivery(history, run_id).await?;
        let Some(row) = row else {
            return Ok(());
        };
        if !row.notified_failure && failing_past(&st, self.notify_after, Utc::now()) {
            self.notify(rec, run_id, &st, "OTLP log export keeps failing")
                .await;
            history
                .log_ship_mark_notified(run_id, true, row.notified_drop)
                .await?;
        }
        #[cfg(feature = "notify")]
        if st.pending_lines == 0 && rec.is_some_and(|r| r.status.is_terminal()) {
            self.notifiers.remove(run_id);
        }
        Ok(())
    }

    async fn notify(
        &self,
        rec: Option<&RunRecord>,
        run_id: &str,
        st: &DeliveryState,
        reason: &str,
    ) {
        #[cfg(feature = "notify")]
        if let Some(n) = self.notifier_for(rec, run_id) {
            let pipeline = rec.and_then(|r| r.name.clone()).unwrap_or_default();
            n.emit(crate::notify::NotifyEvent::log_export_failed(
                &pipeline,
                "",
                run_id,
                st.pending_lines,
                st.dropped_lines,
                st.last_error.as_deref().unwrap_or(reason),
            ))
            .await;
        }
        let _ = (rec, run_id, st, reason);
    }

    /// Retention and bounds. With shipping on, delivered lines are kept for
    /// `retention` after delivery and undelivered ones until delivered,
    /// bounded by `max_age` / `max_bytes` (oldest dropped first, counted on
    /// the run). Without it, plain time-based retention.
    pub async fn maintain(
        &self,
        history: &dyn RunHistory,
    ) -> Result<MaintainSummary, HistoryError> {
        let mut s = MaintainSummary::default();
        let now = Utc::now();
        if !self.configured {
            if !self.retention.is_zero() {
                s.purged += history.purge_run_logs(self.retention).await?;
            }
            for row in history.log_ship_rows(false).await? {
                if history.run_log_stats(&row.run_id, None, None).await?.lines == 0 {
                    history.log_ship_forget(&row.run_id).await?;
                }
            }
            return Ok(s);
        }
        let age_cut = seq_at(now - chrono::Duration::from_std(self.max_age).unwrap_or_default());
        let mut pending: Vec<(LogShipRow, u64, u64)> = Vec::new();
        let mut dropped_runs: Vec<String> = Vec::new();
        for row in history.log_ship_rows(false).await? {
            if let (Some(d), Some(at)) = (row.delivered_seq, row.delivered_at)
                && now - at >= chrono::Duration::from_std(self.retention).unwrap_or_default()
            {
                s.purged += history.delete_run_logs_through(&row.run_id, d).await?;
            }
            let old = history
                .run_log_stats(&row.run_id, row.delivered_seq, Some(age_cut))
                .await?;
            if old.lines > 0 {
                s.purged += history
                    .delete_run_logs_through(&row.run_id, age_cut)
                    .await?;
                history.log_ship_add_dropped(&row.run_id, old.lines).await?;
                metrics::dropped(DropReason::MaxAge, old.lines);
                s.dropped_max_age += old.lines;
                dropped_runs.push(row.run_id.clone());
            }
            let left = history
                .run_log_stats(&row.run_id, row.delivered_seq, None)
                .await?;
            if left.lines > 0 {
                pending.push((row, left.bytes, left.min_seq.unwrap_or(u64::MAX)));
            } else if history.get(&row.run_id).await?.is_none()
                && history.run_log_stats(&row.run_id, None, None).await?.lines == 0
            {
                history.log_ship_forget(&row.run_id).await?;
            }
        }
        let total: u64 = pending.iter().map(|p| p.1).sum();
        if total > self.max_bytes {
            let mut excess = total - self.max_bytes;
            pending.sort_by_key(|p| p.2);
            for (row, bytes, _) in pending.iter_mut() {
                if excess == 0 {
                    break;
                }
                let (n, freed, through) = self.oldest_lines(history, row, excess).await?;
                if n == 0 {
                    continue;
                }
                s.purged += history
                    .delete_run_logs_through(&row.run_id, through)
                    .await?;
                history.log_ship_add_dropped(&row.run_id, n).await?;
                metrics::dropped(DropReason::MaxBytes, n);
                s.dropped_max_bytes += n;
                excess = excess.saturating_sub(freed);
                *bytes = bytes.saturating_sub(freed);
                dropped_runs.push(row.run_id.clone());
            }
        }
        for (row, bytes, min_seq) in &pending {
            let left = history
                .run_log_stats(&row.run_id, row.delivered_seq, None)
                .await?;
            s.gauges.lines += left.lines;
            s.gauges.bytes += (*bytes).min(left.bytes);
            if let Some(t) = left.min_seq.or(Some(*min_seq)).and_then(seq_time) {
                s.gauges.oldest_secs = s
                    .gauges
                    .oldest_secs
                    .max((now - t).num_seconds().max(0) as u64);
            }
        }
        metrics::set_buffer(s.gauges);
        dropped_runs.sort();
        dropped_runs.dedup();
        for run_id in dropped_runs {
            let (row, st) = Self::delivery(history, &run_id).await?;
            if row.is_some_and(|r| !r.notified_drop) {
                let rec = history.get(&run_id).await?;
                self.notify(
                    rec.as_ref(),
                    &run_id,
                    &st,
                    "buffered log lines were dropped before delivery",
                )
                .await;
                history
                    .log_ship_mark_notified(&run_id, st.failing_since.is_some(), true)
                    .await?;
            }
        }
        Ok(s)
    }

    /// The oldest undelivered lines of a run covering `excess` bytes: their
    /// count, bytes and last sequence.
    async fn oldest_lines(
        &self,
        history: &dyn RunHistory,
        row: &LogShipRow,
        excess: u64,
    ) -> Result<(u64, u64, u64), HistoryError> {
        let (mut n, mut freed, mut through) = (0u64, 0u64, 0u64);
        let mut after = row.delivered_seq;
        while freed < excess {
            let page = history.list_run_logs(&row.run_id, after, 1000).await?;
            if page.lines.is_empty() {
                break;
            }
            for l in &page.lines {
                n += 1;
                freed += l.line.len() as u64;
                through = l.seq;
                if freed >= excess {
                    break;
                }
            }
            after = Some(through);
        }
        Ok((n, freed, through))
    }
}

/// The attributes put on every record of a run: its serve id, pipeline and
/// tenant.
pub fn run_attrs(run_id: &str, rec: Option<&RunRecord>) -> BTreeMap<String, String> {
    let mut a = BTreeMap::new();
    a.insert("serve_run_id".to_string(), run_id.to_string());
    if let Some(r) = rec {
        if let Some(n) = &r.name {
            a.insert("pipeline".to_string(), n.clone());
        }
        if let Some(t) = &r.tenant {
            a.insert("tenant".to_string(), t.clone());
        }
    }
    a
}

/// A notifier from a stored cluster run's config body (`notifications:`),
/// for a run shipped by an instance that did not execute it.
#[cfg(feature = "notify")]
pub fn notifier_from_body(body: &str) -> Option<Arc<crate::notify::Notifier>> {
    let mut v: serde_json::Value = serde_yaml::from_str(body).ok()?;
    let mut n = v.get_mut("notifications")?.take();
    crate::interpolate::interpolate_value(&mut n).ok()?;
    let specs: Vec<crate::notify::NotificationSpec> = serde_json::from_value(n).ok()?;
    crate::notify::Notifier::from_specs(&specs).ok().flatten()
}

/// The shipper task: a pass every couple of seconds (backing off while the
/// collector fails) and retention + bounds every minute.
pub async fn run_loop(
    export: Arc<LogExport>,
    history: Arc<dyn RunHistory>,
    shutdown: tokio_util::sync::CancellationToken,
) {
    metrics::describe();
    let mut wait = SHIP_EVERY;
    let mut last_maintain: Option<std::time::Instant> = None;
    loop {
        if last_maintain.is_none_or(|t| t.elapsed() >= MAINTAIN_EVERY) {
            match export.maintain(history.as_ref()).await {
                Ok(s) if s.purged > 0 => {
                    crate::serve::metrics::inc_run_logs_purged(s.purged);
                    tracing::info!(purged = s.purged, "purged run logs past retention");
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "run-log retention pass failed"),
            }
            last_maintain = Some(std::time::Instant::now());
        }
        wait = match export.ship_once(history.as_ref()).await {
            Ok(s) if !s.failed => SHIP_EVERY,
            Ok(_) => backoff(wait),
            Err(e) => {
                tracing::warn!(error = %e, "run-log shipping pass failed");
                backoff(wait)
            }
        };
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tokio::time::sleep(wait) => {}
        }
    }
}

fn backoff(prev: Duration) -> Duration {
    let next = (prev * 2).min(Duration::from_secs(60));
    let jitter =
        u64::from(Utc::now().timestamp_subsec_millis()) % (next.as_millis() as u64 / 5 + 1);
    next + Duration::from_millis(jitter)
}

/// One last bounded pass at shutdown so the tail of the last runs ships.
pub async fn final_flush(export: &LogExport, history: &dyn RunHistory, grace: Duration) {
    if export.configured() {
        let _ = tokio::time::timeout(grace, export.ship_with(history, Duration::ZERO)).await;
    }
}
