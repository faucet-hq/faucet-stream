//! Log shipping for `faucet run` / `faucet schedule` (#806): the capture layer
//! that writes run-log lines into the spool, and the session that ships them.
//!
//! The capture side is a [`SpoolLayer`] in the CLI's tracing subscriber; it
//! is inert until a session starts. Lines are handed to one writer thread
//! through a bounded queue, so a pipeline never waits on log I/O — a full
//! queue drops the line and counts it.

use crate::config::PipelineConfig;
use crate::logship::capture;
use crate::logship::otlp::OtlpLogExporter;
use crate::logship::spool::{PassReport, RunMeta, ShipOptions, Spool, SpoolWriter, ship_pass};
use crate::logship::{LogExportView, LogsSpec, ShipLine};
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant};
use tracing::span::{Attributes, Id};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

/// Capture queue depth (lines).
const QUEUE: usize = 65_536;
/// How often the writer pushes buffered lines to the OS.
const WRITER_TICK: Duration = Duration::from_secs(1);
/// Shipping cadence while exports succeed, and the backoff ceiling.
const SHIP_EVERY: Duration = Duration::from_secs(2);
const MAX_BACKOFF: Duration = Duration::from_secs(60);

enum Msg {
    Begin(Spool, RunMeta, u64),
    Line(String, ShipLine),
    End(String),
    Flush(SyncSender<()>),
}

struct Capture {
    tx: SyncSender<Msg>,
    default_run: RwLock<Option<String>>,
    queue_drops: Mutex<HashMap<String, u64>>,
}

static CAPTURE: OnceLock<Capture> = OnceLock::new();

fn capture_sink() -> &'static Capture {
    CAPTURE.get_or_init(|| {
        let (tx, rx) = std::sync::mpsc::sync_channel(QUEUE);
        std::thread::Builder::new()
            .name("faucet-log-spool".into())
            .spawn(move || writer_loop(rx))
            .map_err(|e| eprintln!("faucet: starting the log-spool writer failed: {e}"))
            .ok();
        Capture {
            tx,
            default_run: RwLock::new(None),
            queue_drops: Mutex::new(HashMap::new()),
        }
    })
}

fn writer_loop(rx: Receiver<Msg>) {
    let mut writers: HashMap<PathBuf, SpoolWriter> = HashMap::new();
    let mut run_dir: HashMap<String, PathBuf> = HashMap::new();
    let mut last_flush = Instant::now();
    loop {
        let msg = rx.recv_timeout(WRITER_TICK);
        match msg {
            Ok(Msg::Begin(spool, meta, max)) => {
                let dir = spool.dir().to_path_buf();
                run_dir.insert(meta.run_id.clone(), dir.clone());
                writers
                    .entry(dir)
                    .or_insert_with(|| SpoolWriter::new(spool))
                    .begin(meta, max);
            }
            Ok(Msg::Line(run, line)) => {
                if let Some(w) = run_dir.get(&run).and_then(|d| writers.get_mut(d)) {
                    w.append(&run, line);
                }
            }
            Ok(Msg::End(run)) => {
                apply_queue_drops(&mut writers, &run_dir);
                if let Some(d) = run_dir.remove(&run)
                    && let Some(w) = writers.get_mut(&d)
                {
                    w.end(&run);
                }
            }
            Ok(Msg::Flush(ack)) => {
                apply_queue_drops(&mut writers, &run_dir);
                for w in writers.values_mut() {
                    w.flush(true);
                }
                last_flush = Instant::now();
                let _ = ack.send(());
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
        if last_flush.elapsed() >= WRITER_TICK {
            apply_queue_drops(&mut writers, &run_dir);
            for w in writers.values_mut() {
                w.flush(false);
            }
            last_flush = Instant::now();
        }
    }
}

fn apply_queue_drops(writers: &mut HashMap<PathBuf, SpoolWriter>, run_dir: &HashMap<String, PathBuf>) {
    let Some(c) = CAPTURE.get() else {
        return;
    };
    let drops: Vec<(String, u64)> = c
        .queue_drops
        .lock()
        .map(|mut m| m.drain().collect())
        .unwrap_or_default();
    for (run, n) in drops {
        if let Some(w) = run_dir.get(&run).and_then(|d| writers.get_mut(d)) {
            w.add_queue_dropped(&run, n);
        }
    }
}

fn send(msg: Msg) -> bool {
    let c = capture_sink();
    match c.tx.try_send(msg) {
        Ok(()) => true,
        Err(TrySendError::Full(Msg::Line(run, _))) => {
            if let Ok(mut m) = c.queue_drops.lock() {
                *m.entry(run).or_default() += 1;
            }
            crate::logship::metrics::dropped(crate::logship::metrics::DropReason::QueueFull, 1);
            false
        }
        Err(TrySendError::Full(other)) => c.tx.send(other).is_ok(),
        Err(TrySendError::Disconnected(_)) => false,
    }
}

/// The tracing layer that feeds the spool. Inert until a session starts.
pub struct SpoolLayer;

impl<S> Layer<S> for SpoolLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        if CAPTURE.get().is_some() {
            capture::on_new_span(attrs, id, &ctx);
        }
    }

    fn on_record(&self, id: &Id, values: &tracing::span::Record<'_>, ctx: Context<'_, S>) {
        if CAPTURE.get().is_some() {
            capture::on_record(id, values, &ctx);
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let Some(c) = CAPTURE.get() else {
            return;
        };
        if capture::is_shipping_noise(event.metadata().target()) {
            return;
        }
        let ev = capture::capture(event, &ctx);
        let run = match ev.attr("log_run_id") {
            Some(r) => r.to_string(),
            None => match c.default_run.read().ok().and_then(|d| d.clone()) {
                Some(r) => r,
                None => return,
            },
        };
        let mut attrs = ev.attrs;
        attrs.remove("log_run_id");
        send(Msg::Line(
            run,
            ShipLine {
                seq: 0,
                ts: ev.ts,
                level: ev.level.to_string(),
                body: ev.body,
                attrs,
            },
        ));
    }
}

/// Whether `cfg` turns log shipping on (`logs` in `observability.otel.export`).
pub fn logs_enabled(cfg: &PipelineConfig) -> bool {
    crate::logship::session_wanted(cfg)
}

/// A fresh, time-ordered run id.
pub fn new_run_id() -> String {
    uuid::Uuid::now_v7().to_string()
}

/// Shipping for one `faucet run` / `faucet schedule` process.
pub struct LogShipSession {
    spool: Spool,
    exporter: Option<OtlpLogExporter>,
    spec: LogsSpec,
    pipeline: String,
    #[cfg(feature = "notify")]
    notifier: Option<Arc<crate::notify::Notifier>>,
    pass_lock: Arc<tokio::sync::Mutex<()>>,
    stop: faucet_core::CancellationToken,
    task: Option<tokio::task::JoinHandle<()>>,
    open: Mutex<Vec<String>>,
}

/// Everything the background task needs, cloneable.
#[derive(Clone)]
struct Shipper {
    spool: Spool,
    exporter: Option<OtlpLogExporter>,
    opts: ShipOptions,
    pipeline: String,
    #[cfg(feature = "notify")]
    notifier: Option<Arc<crate::notify::Notifier>>,
    pass_lock: Arc<tokio::sync::Mutex<()>>,
}

impl Shipper {
    async fn pass(&self) -> PassReport {
        let _g = self.pass_lock.lock().await;
        let report = ship_pass(
            &self.spool,
            self.exporter.as_ref(),
            self.opts,
            chrono::Utc::now(),
        )
        .await;
        #[cfg(feature = "notify")]
        if let Some(n) = &self.notifier {
            notify_report(n, &self.pipeline, &report).await;
        }
        let _ = &self.pipeline;
        report
    }
}

/// Emit `log_export_failed` for every run the pass flagged.
#[cfg(feature = "notify")]
pub async fn notify_report(n: &crate::notify::Notifier, pipeline: &str, report: &PassReport) {
    for r in &report.runs {
        let row = r.meta.attrs.get("row").cloned().unwrap_or_default();
        let pipe = r
            .meta
            .attrs
            .get("pipeline")
            .cloned()
            .unwrap_or_else(|| pipeline.to_string());
        if r.notify_failure {
            n.emit(crate::notify::NotifyEvent::log_export_failed(
                &pipe,
                &row,
                &r.run_id,
                r.view.pending_lines,
                r.view.dropped_lines,
                r.view.last_error.as_deref().unwrap_or("export failing"),
            ))
            .await;
        }
        if r.notify_drop {
            n.emit(crate::notify::NotifyEvent::log_export_failed(
                &pipe,
                &row,
                &r.run_id,
                r.view.pending_lines,
                r.view.dropped_lines,
                "buffered log lines were dropped before delivery",
            ))
            .await;
        }
    }
}

impl LogShipSession {
    /// Start shipping when `cfg` exports logs. `None` when it does not, or
    /// when the spool cannot be opened (logged; a run is never failed by it).
    pub fn start(cfg: &PipelineConfig, pipeline: &str) -> Option<Self> {
        if !logs_enabled(cfg) {
            return None;
        }
        let spec = cfg
            .observability
            .as_ref()
            .and_then(|o| o.logs.clone())
            .unwrap_or_default();
        let otel = cfg.observability.as_ref().and_then(|o| o.otel.as_ref())?;
        let exporter = match otel.to_core().and_then(|c| OtlpLogExporter::new(&c)) {
            Ok(e) => Some(e),
            Err(e) => {
                tracing::warn!("log shipping disabled: {e}");
                return None;
            }
        };
        Self::with_exporter(spec, exporter, pipeline, cfg)
    }

    /// A session over an explicit exporter (or none: buffer only).
    pub fn with_exporter(
        spec: LogsSpec,
        exporter: Option<OtlpLogExporter>,
        pipeline: &str,
        cfg: &PipelineConfig,
    ) -> Option<Self> {
        let _ = cfg;
        let dir = spec.resolved_spool_dir();
        let spool = match Spool::open(&dir) {
            Ok(s) => s,
            Err(e) => {
                eprintln!(
                    "faucet: log shipping disabled — cannot open the spool {}: {e}",
                    dir.display()
                );
                crate::logship::metrics::local_write_failure(1);
                return None;
            }
        };
        crate::logship::metrics::describe();
        #[cfg(feature = "notify")]
        let notifier = crate::notify::Notifier::from_specs(&cfg.notifications)
            .ok()
            .flatten();
        let pass_lock = Arc::new(tokio::sync::Mutex::new(()));
        let stop = faucet_core::CancellationToken::new();
        let shipper = Shipper {
            spool: spool.clone(),
            exporter: exporter.clone(),
            opts: ShipOptions::from(&spec),
            pipeline: pipeline.to_string(),
            #[cfg(feature = "notify")]
            notifier: notifier.clone(),
            pass_lock: pass_lock.clone(),
        };
        capture_sink();
        let task = tokio::spawn(background(shipper, stop.clone()));
        Some(Self {
            spool,
            exporter,
            spec,
            pipeline: pipeline.to_string(),
            #[cfg(feature = "notify")]
            notifier,
            pass_lock,
            stop,
            task: Some(task),
            open: Mutex::new(Vec::new()),
        })
    }

    pub fn spool(&self) -> &Spool {
        &self.spool
    }

    fn shipper(&self) -> Shipper {
        Shipper {
            spool: self.spool.clone(),
            exporter: self.exporter.clone(),
            opts: ShipOptions::from(&self.spec),
            pipeline: self.pipeline.clone(),
            #[cfg(feature = "notify")]
            notifier: self.notifier.clone(),
            pass_lock: self.pass_lock.clone(),
        }
    }

    /// Open a run in the spool. `attrs` go on every exported record.
    pub fn begin_run(&self, run_id: &str, attrs: BTreeMap<String, String>) {
        let mut attrs = attrs;
        attrs.insert("run_id".into(), run_id.to_string());
        attrs
            .entry("pipeline".into())
            .or_insert_with(|| self.pipeline.clone());
        if let Ok(mut o) = self.open.lock() {
            o.push(run_id.to_string());
        }
        send(Msg::Begin(
            self.spool.clone(),
            RunMeta {
                run_id: run_id.to_string(),
                attrs,
                started_at: Some(chrono::Utc::now()),
                ..Default::default()
            },
            self.spec.max_lines_per_run,
        ));
    }

    /// Route events outside any run span to `run_id` (the whole `faucet run`
    /// process is one run).
    pub fn set_default_run(&self, run_id: Option<&str>) {
        if let Ok(mut d) = capture_sink().default_run.write() {
            *d = run_id.map(str::to_string);
        }
    }

    /// Close a run; its lines keep shipping in the background.
    pub fn end_run(&self, run_id: &str) {
        if let Ok(mut d) = capture_sink().default_run.write()
            && d.as_deref() == Some(run_id)
        {
            *d = None;
        }
        if let Ok(mut o) = self.open.lock() {
            o.retain(|r| r != run_id);
        }
        send(Msg::End(run_id.to_string()));
    }

    /// Wait (up to `timeout`) for the writer to fsync everything queued so far.
    pub async fn flush_writer(&self, timeout: Duration) {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        if !send(Msg::Flush(tx)) {
            return;
        }
        let _ = tokio::task::spawn_blocking(move || rx.recv_timeout(timeout)).await;
    }

    /// Close `run_id` and ship its lines for at most `flush_timeout_secs`.
    /// Returns the run's export status.
    pub async fn finish_run(&self, run_id: &str) -> LogExportView {
        self.end_run(run_id);
        let timeout = self.spec.flush_timeout();
        let deadline = Instant::now() + timeout;
        self.flush_writer(timeout).await;
        let shipper = self.shipper();
        let mut last = None;
        loop {
            let report = shipper.pass().await;
            if let Some(r) = report.run(run_id) {
                let done = r.view.pending_lines == 0 && !r.locked_elsewhere;
                last = Some(r.view.clone());
                if done {
                    break;
                }
            }
            if Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200).min(deadline - Instant::now())).await;
        }
        last.or_else(|| crate::logship::spool::run_view(&self.spool, run_id))
            .unwrap_or_else(|| crate::logship::derive_view(true, &Default::default()))
    }

    /// Stop the background shipper after one last bounded flush.
    pub async fn shutdown(mut self) {
        let timeout = self.spec.flush_timeout();
        self.flush_writer(timeout).await;
        let shipper = self.shipper();
        let _ = tokio::time::timeout(timeout, shipper.pass()).await;
        self.stop.cancel();
        if let Some(t) = self.task.take() {
            let _ = tokio::time::timeout(Duration::from_secs(1), t).await;
        }
    }
}

impl Drop for LogShipSession {
    fn drop(&mut self) {
        let open: Vec<String> = self
            .open
            .lock()
            .map(|mut o| std::mem::take(&mut *o))
            .unwrap_or_default();
        for run in open {
            self.end_run(&run);
        }
        self.stop.cancel();
    }
}

async fn background(shipper: Shipper, stop: faucet_core::CancellationToken) {
    let mut wait = SHIP_EVERY;
    loop {
        tokio::select! {
            _ = stop.cancelled() => break,
            _ = tokio::time::sleep(wait) => {}
        }
        let report = shipper.pass().await;
        wait = if report.failed {
            backoff(wait)
        } else {
            SHIP_EVERY
        };
    }
}

/// Double the wait (capped) with up to 20 % jitter.
pub fn backoff(prev: Duration) -> Duration {
    let next = (prev * 2).min(MAX_BACKOFF);
    let jitter = (std::process::id() as u64 ^ chrono::Utc::now().timestamp_subsec_nanos() as u64)
        % (next.as_millis() as u64 / 5 + 1);
    next + Duration::from_millis(jitter)
}

/// `faucet logs ship`: one bounded drain of a spool. Returns the pass report.
pub async fn ship_spool(
    spool: &Spool,
    exporter: &OtlpLogExporter,
    spec: &LogsSpec,
    timeout: Duration,
) -> PassReport {
    let deadline = Instant::now() + timeout;
    let opts = ShipOptions::from(spec);
    loop {
        let report = ship_pass(spool, Some(exporter), opts, chrono::Utc::now()).await;
        if report.undelivered() == 0 || report.failed || Instant::now() >= deadline {
            return report;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_to_a_cap() {
        let b = backoff(Duration::from_secs(2));
        assert!(b >= Duration::from_secs(4) && b <= Duration::from_millis(4800));
        assert!(backoff(Duration::from_secs(50)) <= MAX_BACKOFF + MAX_BACKOFF / 5);
        assert!(!new_run_id().is_empty());
    }

    #[test]
    fn logs_enabled_reads_the_export_list() {
        let yaml = |export: &str| {
            format!(
                "version: 1\npipeline:\n  source: {{ type: rest, config: {{ base_url: \"http://x\" }} }}\n  sink: {{ type: stdout, config: {{}} }}\nobservability:\n  otel: {{ export: [{export}] }}\n"
            )
        };
        let on = crate::config::parse_with_extension(&yaml("traces, logs"), "yaml").unwrap();
        assert!(logs_enabled(&on));
        let off = crate::config::parse_with_extension(&yaml("traces"), "yaml").unwrap();
        assert!(!logs_enabled(&off));
        assert!(LogShipSession::start(&off, "p").is_none());
    }
}
