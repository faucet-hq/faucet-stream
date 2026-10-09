//! The local spool `faucet run` / `faucet schedule` write run logs to before
//! shipping them (#806).
//!
//! Per run, in the spool directory:
//!
//! - `<run_id>.jsonl` — append-only, one [`ShipLine`] per line, fsync'd every
//!   [`SYNC_EVERY`] lines and at run end;
//! - `<run_id>.meta.json` — run identity + attributes, end time, cap drops
//!   (written by the capturing process);
//! - `<run_id>.cursor.json` — the delivery watermark (byte offset + sequence)
//!   and export status (written by whichever process ships the run, always
//!   temp + rename);
//! - `<run_id>.lock` — an advisory lock held while a process writes or ships
//!   the run, so two processes sharing a spool never ship one run twice.

use crate::logship::metrics::{self, BufferGauges, DropReason};
use crate::logship::otlp::{OtlpLogExporter, batches};
use crate::logship::{DeliveryState, LogExportView, ShipLine, derive_view};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

/// fsync the segment after this many lines.
pub const SYNC_EVERY: u64 = 1024;
/// Most lines one pass reads for a run before moving on.
const MAX_LINES_PER_PASS: usize = 8 * crate::logship::otlp::BATCH_LINES;

/// Run identity and capture-side counters.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunMeta {
    pub run_id: String,
    /// Attributes put on every exported record (`run_id`, `pipeline`, …).
    #[serde(default)]
    pub attrs: BTreeMap<String, String>,
    pub started_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub ended_at: Option<DateTime<Utc>>,
    /// Highest sequence written.
    #[serde(default)]
    pub last_seq: u64,
    /// Lines dropped by the per-run cap.
    #[serde(default)]
    pub capped: u64,
    /// Lines dropped because the capture queue was full.
    #[serde(default)]
    pub queue_dropped: u64,
}

/// The delivery watermark and export status of one run.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
    /// Byte offset in the segment just past the last delivered (or dropped) line.
    pub offset: u64,
    pub delivered_seq: Option<u64>,
    pub delivered_lines: u64,
    /// Lines dropped by a buffer bound.
    pub dropped: u64,
    pub last_error: Option<String>,
    pub last_attempt_at: Option<DateTime<Utc>>,
    pub failing_since: Option<DateTime<Utc>>,
    pub delivered_at: Option<DateTime<Utc>>,
    /// When the run ended with nothing left to ship (retention counts from here).
    pub settled_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub notified_failure: bool,
    #[serde(default)]
    pub notified_drop: bool,
}

/// One spool directory.
#[derive(Debug, Clone)]
pub struct Spool {
    dir: PathBuf,
}

/// Write `value` as JSON to `path` atomically (temp + rename).
fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> std::io::Result<()> {
    let tmp = path.with_extension(format!(
        "{}.tmp-{}",
        path.extension().and_then(|e| e.to_str()).unwrap_or("json"),
        std::process::id()
    ));
    {
        let mut f = File::create(&tmp)?;
        f.write_all(&serde_json::to_vec(value).map_err(std::io::Error::other)?)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Option<T> {
    let bytes = std::fs::read(path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

impl Spool {
    /// Open (creating) `dir`.
    pub fn open(dir: impl Into<PathBuf>) -> std::io::Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn segment(&self, run_id: &str) -> PathBuf {
        self.dir.join(format!("{run_id}.jsonl"))
    }
    pub fn meta_path(&self, run_id: &str) -> PathBuf {
        self.dir.join(format!("{run_id}.meta.json"))
    }
    pub fn cursor_path(&self, run_id: &str) -> PathBuf {
        self.dir.join(format!("{run_id}.cursor.json"))
    }
    pub fn lock_path(&self, run_id: &str) -> PathBuf {
        self.dir.join(format!("{run_id}.lock"))
    }

    pub fn read_meta(&self, run_id: &str) -> Option<RunMeta> {
        read_json(&self.meta_path(run_id))
    }
    pub fn write_meta(&self, meta: &RunMeta) -> std::io::Result<()> {
        write_json_atomic(&self.meta_path(&meta.run_id), meta)
    }
    pub fn read_cursor(&self, run_id: &str) -> Cursor {
        read_json(&self.cursor_path(run_id)).unwrap_or_default()
    }
    pub fn write_cursor(&self, run_id: &str, c: &Cursor) -> std::io::Result<()> {
        write_json_atomic(&self.cursor_path(run_id), c)
    }

    /// Every run the spool knows (a meta or a segment), oldest first.
    pub fn run_ids(&self) -> Vec<String> {
        let mut ids: BTreeMap<String, std::time::SystemTime> = BTreeMap::new();
        let Ok(rd) = std::fs::read_dir(&self.dir) else {
            return Vec::new();
        };
        for e in rd.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            let id = name
                .strip_suffix(".meta.json")
                .or_else(|| name.strip_suffix(".jsonl"));
            if let Some(id) = id {
                let mtime = e
                    .metadata()
                    .and_then(|m| m.modified())
                    .unwrap_or(std::time::UNIX_EPOCH);
                let slot = ids.entry(id.to_string()).or_insert(mtime);
                if mtime < *slot {
                    *slot = mtime;
                }
            }
        }
        let mut v: Vec<(String, std::time::SystemTime)> = ids.into_iter().collect();
        v.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        v.into_iter().map(|(id, _)| id).collect()
    }

    fn segment_len(&self, run_id: &str) -> u64 {
        std::fs::metadata(self.segment(run_id))
            .map(|m| m.len())
            .unwrap_or(0)
    }

    /// Remove every file of a run.
    pub fn remove_run(&self, run_id: &str) {
        for p in [
            self.segment(run_id),
            self.meta_path(run_id),
            self.cursor_path(run_id),
            self.lock_path(run_id),
        ] {
            let _ = std::fs::remove_file(p);
        }
    }

    /// Complete lines of a run's segment from `offset`, each with the byte
    /// offset just past it. A partial last line (a writer mid-append) is left.
    pub fn read_lines(
        &self,
        run_id: &str,
        offset: u64,
        max: usize,
    ) -> std::io::Result<Vec<(ShipLine, u64)>> {
        let mut f = match File::open(self.segment(run_id)) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        f.seek(SeekFrom::Start(offset))?;
        let mut r = BufReader::new(f);
        let mut out = Vec::new();
        let mut pos = offset;
        let mut buf = Vec::new();
        while out.len() < max {
            buf.clear();
            let n = r.read_until(b'\n', &mut buf)?;
            if n == 0 || buf.last() != Some(&b'\n') {
                break;
            }
            pos += n as u64;
            match serde_json::from_slice::<ShipLine>(&buf) {
                Ok(l) => out.push((l, pos)),
                Err(_) => out.push((
                    ShipLine {
                        seq: 0,
                        ts: String::new(),
                        level: "WARN".into(),
                        body: String::from_utf8_lossy(&buf).trim_end().to_string(),
                        attrs: BTreeMap::new(),
                    },
                    pos,
                )),
            }
        }
        Ok(out)
    }

    /// Complete lines after `offset` and the first one's timestamp.
    pub fn count_from(&self, run_id: &str, offset: u64) -> (u64, Option<String>) {
        let Ok(mut f) = File::open(self.segment(run_id)) else {
            return (0, None);
        };
        if f.seek(SeekFrom::Start(offset)).is_err() {
            return (0, None);
        }
        let mut r = BufReader::new(f);
        let mut first = Vec::new();
        if r.read_until(b'\n', &mut first).unwrap_or(0) == 0 || first.last() != Some(&b'\n') {
            return (0, None);
        }
        let ts = serde_json::from_slice::<ShipLine>(&first)
            .ok()
            .map(|l| l.ts);
        let mut rest = Vec::new();
        let _ = r.read_to_end(&mut rest);
        let n = 1 + rest.iter().filter(|b| **b == b'\n').count() as u64;
        (n, ts)
    }
}

// ── advisory locks ───────────────────────────────────────────────────────────

static HELD: Mutex<Option<HashMap<PathBuf, (File, usize)>>> = Mutex::new(None);

/// An advisory lock on one run. Shared within the process (the writer and the
/// in-process shipper hold it together); exclusive across processes.
#[derive(Debug)]
pub struct RunLock {
    path: PathBuf,
}

impl RunLock {
    /// Take the lock, or `None` when another process holds it.
    pub fn acquire(path: PathBuf) -> Option<Self> {
        Self::acquire_detect(path).map(|(l, _)| l)
    }

    /// [`acquire`](Self::acquire), also saying whether this process already
    /// held it (a writer here has the run open).
    pub fn acquire_detect(path: PathBuf) -> Option<(Self, bool)> {
        let mut guard = HELD.lock().unwrap_or_else(|e| e.into_inner());
        let held = guard.get_or_insert_with(HashMap::new);
        if let Some(entry) = held.get_mut(&path) {
            entry.1 += 1;
            return Some((Self { path }, true));
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)
            .ok()?;
        file.try_lock().ok()?;
        held.insert(path.clone(), (file, 1));
        Some((Self { path }, false))
    }
}

impl Drop for RunLock {
    fn drop(&mut self) {
        let mut guard = HELD.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(held) = guard.as_mut()
            && let Some(entry) = held.get_mut(&self.path)
        {
            entry.1 -= 1;
            if entry.1 == 0 {
                held.remove(&self.path);
            }
        }
    }
}

// ── writer ───────────────────────────────────────────────────────────────────

/// A run being written.
struct OpenRun {
    file: BufWriter<File>,
    meta: RunMeta,
    since_sync: u64,
    meta_dirty: bool,
    max_lines: u64,
    _lock: Option<RunLock>,
}

/// The capture side of a spool: appends lines to open runs. Not thread-safe
/// by itself; [`crate::logship::session`] drives it from one thread.
pub struct SpoolWriter {
    spool: Spool,
    runs: HashMap<String, OpenRun>,
    write_failures: u64,
}

impl SpoolWriter {
    pub fn new(spool: Spool) -> Self {
        Self {
            spool,
            runs: HashMap::new(),
            write_failures: 0,
        }
    }

    fn failed(&mut self, n: u64, what: &str, e: &std::io::Error) {
        if self.write_failures == 0 {
            eprintln!(
                "faucet: writing run logs to the spool {} failed ({what}): {e}",
                self.spool.dir().display()
            );
        }
        self.write_failures += n;
        metrics::local_write_failure(n);
    }

    /// Start (or reopen) a run.
    pub fn begin(&mut self, mut meta: RunMeta, max_lines: u64) {
        if self.runs.contains_key(&meta.run_id) {
            return;
        }
        if let Some(prev) = self.spool.read_meta(&meta.run_id) {
            meta.last_seq = prev.last_seq;
            meta.capped = prev.capped;
            meta.queue_dropped = prev.queue_dropped;
            meta.started_at = prev.started_at.or(meta.started_at);
            meta.ended_at = None;
        }
        let lock = RunLock::acquire(self.spool.lock_path(&meta.run_id));
        if let Err(e) = self.spool.write_meta(&meta) {
            self.failed(0, "run metadata", &e);
        }
        let file = match OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.spool.segment(&meta.run_id))
        {
            Ok(f) => f,
            Err(e) => {
                self.failed(0, "opening the segment", &e);
                return;
            }
        };
        self.runs.insert(
            meta.run_id.clone(),
            OpenRun {
                file: BufWriter::new(file),
                meta,
                since_sync: 0,
                meta_dirty: false,
                max_lines,
                _lock: lock,
            },
        );
    }

    /// Append one line to `run_id`. Lines for an unknown run are ignored.
    pub fn append(&mut self, run_id: &str, mut line: ShipLine) {
        let Some(run) = self.runs.get_mut(run_id) else {
            return;
        };
        if run.meta.last_seq >= run.max_lines {
            run.meta.capped += 1;
            run.meta_dirty = true;
            metrics::dropped(DropReason::MaxLines, 1);
            return;
        }
        line.seq = run.meta.last_seq + 1;
        let mut bytes = match serde_json::to_vec(&line) {
            Ok(b) => b,
            Err(_) => return,
        };
        bytes.push(b'\n');
        let res = run.file.write_all(&bytes).and_then(|_| {
            run.since_sync += 1;
            if run.since_sync >= SYNC_EVERY {
                run.since_sync = 0;
                run.file.flush()?;
                run.file.get_ref().sync_data()?;
            }
            Ok(())
        });
        match res {
            Ok(()) => {
                run.meta.last_seq = line.seq;
                run.meta_dirty = true;
            }
            Err(e) => self.failed(1, "appending a line", &e),
        }
    }

    /// Record lines the capture queue dropped for `run_id`.
    pub fn add_queue_dropped(&mut self, run_id: &str, n: u64) {
        if let Some(run) = self.runs.get_mut(run_id) {
            run.meta.queue_dropped += n;
            run.meta_dirty = true;
        }
    }

    /// Push buffered lines to the OS and persist changed metadata; with
    /// `sync`, fsync every segment too.
    pub fn flush(&mut self, sync: bool) {
        let mut errs = Vec::new();
        for run in self.runs.values_mut() {
            if run._lock.is_none() {
                run._lock = RunLock::acquire(self.spool.lock_path(&run.meta.run_id));
            }
            if let Err(e) = run.file.flush() {
                errs.push(e);
                continue;
            }
            if sync && let Err(e) = run.file.get_ref().sync_data() {
                errs.push(e);
            }
            if run.meta_dirty {
                run.meta_dirty = false;
                if let Err(e) = self.spool.write_meta(&run.meta) {
                    errs.push(e);
                }
            }
        }
        for e in errs {
            self.failed(0, "flushing", &e);
        }
    }

    /// Close a run: fsync its segment, stamp the end time, release the lock.
    pub fn end(&mut self, run_id: &str) {
        let Some(mut run) = self.runs.remove(run_id) else {
            return;
        };
        let res = run.file.flush().and_then(|_| run.file.get_ref().sync_all());
        if let Err(e) = res {
            self.failed(0, "closing the segment", &e);
        }
        run.meta.ended_at = Some(Utc::now());
        if let Err(e) = self.spool.write_meta(&run.meta) {
            self.failed(0, "run metadata", &e);
        }
    }

    pub fn is_open(&self, run_id: &str) -> bool {
        self.runs.contains_key(run_id)
    }

    pub fn write_failures(&self) -> u64 {
        self.write_failures
    }
}

// ── shipping ─────────────────────────────────────────────────────────────────

/// Retention + bounds for a shipping pass.
#[derive(Debug, Clone, Copy)]
pub struct ShipOptions {
    pub retention: Duration,
    pub max_age: Duration,
    pub max_bytes: u64,
    pub notify_after: Duration,
}

impl From<&crate::logship::LogsSpec> for ShipOptions {
    fn from(s: &crate::logship::LogsSpec) -> Self {
        Self {
            retention: s.retention(),
            max_age: s.max_age(),
            max_bytes: s.buffer_max_bytes,
            notify_after: s.notify_after(),
        }
    }
}

/// What a pass learned about one run.
#[derive(Debug, Clone)]
pub struct RunReport {
    pub run_id: String,
    pub meta: RunMeta,
    pub view: LogExportView,
    /// The run is being shipped by another process.
    pub locked_elsewhere: bool,
    /// The run's files were removed (retention).
    pub removed: bool,
    /// Its export has been failing past `notify_after` (first time).
    pub notify_failure: bool,
    /// Lines were dropped (first time).
    pub notify_drop: bool,
}

/// What one pass over the spool did.
#[derive(Debug, Clone, Default)]
pub struct PassReport {
    pub runs: Vec<RunReport>,
    pub shipped: u64,
    /// At least one export failed.
    pub failed: bool,
    pub gauges: BufferGauges,
}

impl PassReport {
    pub fn run(&self, run_id: &str) -> Option<&RunReport> {
        self.runs.iter().find(|r| r.run_id == run_id)
    }

    /// Runs with lines still undelivered.
    pub fn undelivered(&self) -> usize {
        self.runs
            .iter()
            .filter(|r| !r.removed && r.view.pending_lines > 0)
            .count()
    }
}

fn delivery_state(meta: &RunMeta, c: &Cursor, pending: u64) -> DeliveryState {
    DeliveryState {
        delivered_seq: c.delivered_seq,
        total_seq: (meta.last_seq > 0).then_some(meta.last_seq),
        pending_lines: pending,
        dropped_lines: c.dropped + meta.capped + meta.queue_dropped,
        last_error: c.last_error.clone(),
        last_attempt_at: c.last_attempt_at,
        failing_since: c.failing_since,
        delivered_at: c.delivered_at,
    }
}

/// The current status of one run without shipping it.
pub fn run_view(spool: &Spool, run_id: &str) -> Option<LogExportView> {
    let meta = spool.read_meta(run_id)?;
    let c = spool.read_cursor(run_id);
    let (pending, _) = spool.count_from(run_id, c.offset);
    Some(derive_view(true, &delivery_state(&meta, &c, pending)))
}

/// One shipping pass: apply the bounds, export what is pending (when an
/// exporter is given), advance each run's watermark per acknowledged batch,
/// and remove runs past retention.
pub async fn ship_pass(
    spool: &Spool,
    exporter: Option<&OtlpLogExporter>,
    opts: ShipOptions,
    now: DateTime<Utc>,
) -> PassReport {
    let mut report = PassReport::default();
    let ids = spool.run_ids();
    let bytes_drop = plan_bytes_drops(spool, &ids, opts.max_bytes);
    for id in ids {
        if let Some(r) =
            process_run(spool, &id, exporter, opts, now, &bytes_drop, &mut report).await
        {
            report.runs.push(r);
        }
    }
    metrics::set_buffer(report.gauges);
    report
}

/// Which runs to drop to get back under `max_bytes`: settled runs first, then
/// the oldest runs' undelivered lines.
fn plan_bytes_drops(spool: &Spool, ids: &[String], max_bytes: u64) -> HashMap<String, bool> {
    let sizes: Vec<(String, u64, bool)> = ids
        .iter()
        .map(|id| {
            let settled = spool.read_cursor(id).settled_at.is_some();
            (id.clone(), spool.segment_len(id), settled)
        })
        .collect();
    let total: u64 = sizes.iter().map(|s| s.1).sum();
    let mut out = HashMap::new();
    if total <= max_bytes {
        return out;
    }
    let mut excess = total - max_bytes;
    for pass_settled in [true, false] {
        for (id, len, settled) in &sizes {
            if excess == 0 {
                break;
            }
            if *settled == pass_settled && *len > 0 {
                out.insert(id.clone(), *settled);
                excess = excess.saturating_sub(*len);
            }
        }
    }
    out
}

async fn process_run(
    spool: &Spool,
    id: &str,
    exporter: Option<&OtlpLogExporter>,
    opts: ShipOptions,
    now: DateTime<Utc>,
    bytes_drop: &HashMap<String, bool>,
    report: &mut PassReport,
) -> Option<RunReport> {
    let mut meta = spool.read_meta(id).unwrap_or_else(|| RunMeta {
        run_id: id.to_string(),
        ended_at: Some(now),
        ..Default::default()
    });
    let Some((_lock, shared)) = RunLock::acquire_detect(spool.lock_path(id)) else {
        let c = spool.read_cursor(id);
        let (pending, _) = spool.count_from(id, c.offset);
        return Some(RunReport {
            run_id: id.to_string(),
            view: derive_view(true, &delivery_state(&meta, &c, pending)),
            meta,
            locked_elsewhere: true,
            removed: false,
            notify_failure: false,
            notify_drop: false,
        });
    };
    if !shared && meta.ended_at.is_none() {
        // Nobody holds the run's lock, so the process writing it is gone.
        meta.ended_at = Some(now);
        let _ = spool.write_meta(&meta);
    }
    let mut c = spool.read_cursor(id);
    let len = spool.segment_len(id);
    let ended = meta.ended_at.is_some();

    if let Some(settled) = bytes_drop.get(id) {
        if *settled {
            let _ = std::fs::remove_file(spool.segment(id));
        } else {
            let (n, _) = spool.count_from(id, c.offset);
            c.dropped += n;
            c.offset = len;
            metrics::dropped(DropReason::MaxBytes, n);
            if ended {
                let _ = std::fs::remove_file(spool.segment(id));
                c.offset = 0;
            }
        }
    }

    let cutoff = now - chrono::Duration::from_std(opts.max_age).unwrap_or_default();
    let mut aged = 0u64;
    loop {
        let lines = match spool.read_lines(id, c.offset, MAX_LINES_PER_PASS) {
            Ok(l) => l,
            Err(_) => break,
        };
        let mut advanced = false;
        for (l, end) in &lines {
            let old = DateTime::parse_from_rfc3339(&l.ts)
                .map(|t| t.with_timezone(&Utc) < cutoff)
                .unwrap_or(false);
            if !old {
                break;
            }
            c.offset = *end;
            aged += 1;
            advanced = true;
        }
        if !advanced || lines.len() < MAX_LINES_PER_PASS {
            break;
        }
    }
    if aged > 0 {
        c.dropped += aged;
        metrics::dropped(DropReason::MaxAge, aged);
    }

    if let Some(exporter) = exporter {
        let pending: Vec<(ShipLine, u64)> = spool
            .read_lines(id, c.offset, MAX_LINES_PER_PASS)
            .unwrap_or_default();
        let ends: HashMap<u64, u64> = pending.iter().map(|(l, e)| (l.seq, *e)).collect();
        let lines: Vec<ShipLine> = pending.into_iter().map(|(l, _)| l).collect();
        for batch in batches(lines) {
            c.last_attempt_at = Some(now);
            match exporter.export(&meta.attrs, &batch).await {
                Ok(_) => {
                    let last = batch.last().map(|l| l.seq).unwrap_or(0);
                    if let Some(end) = ends.get(&last) {
                        c.offset = *end;
                    }
                    c.delivered_seq = Some(last);
                    c.delivered_lines += batch.len() as u64;
                    c.delivered_at = Some(now);
                    c.last_error = None;
                    c.failing_since = None;
                    c.notified_failure = false;
                    report.shipped += batch.len() as u64;
                    metrics::shipped(batch.len() as u64);
                    if let Err(e) = spool.write_cursor(id, &c) {
                        tracing::warn!(run_id = id, error = %e, "writing the log-delivery cursor failed");
                    }
                }
                Err(e) => {
                    c.last_error = Some(e);
                    c.failing_since.get_or_insert(now);
                    report.failed = true;
                    metrics::export_error();
                    break;
                }
            }
        }
    }

    let (pending, first_ts) = spool.count_from(id, c.offset);
    if ended && pending == 0 {
        c.settled_at.get_or_insert(now);
    } else {
        c.settled_at = None;
    }
    let st = delivery_state(&meta, &c, pending);
    let notify_failure =
        !c.notified_failure && crate::logship::record::failing_past(&st, opts.notify_after, now);
    let notify_drop = !c.notified_drop && st.dropped_lines > 0;
    c.notified_failure |= notify_failure;
    c.notified_drop |= notify_drop;

    let removed = c
        .settled_at
        .is_some_and(|t| now - t >= chrono::Duration::from_std(opts.retention).unwrap_or_default());
    if removed {
        spool.remove_run(id);
    } else if let Err(e) = spool.write_cursor(id, &c) {
        tracing::warn!(run_id = id, error = %e, "writing the log-delivery cursor failed");
    }
    if pending > 0 {
        report.gauges.lines += pending;
        report.gauges.bytes += spool.segment_len(id).saturating_sub(c.offset);
        if let Some(ts) = first_ts
            && let Ok(t) = DateTime::parse_from_rfc3339(&ts)
        {
            let age = (now - t.with_timezone(&Utc)).num_seconds().max(0) as u64;
            report.gauges.oldest_secs = report.gauges.oldest_secs.max(age);
        }
    }
    Some(RunReport {
        run_id: id.to_string(),
        view: derive_view(true, &st),
        meta,
        locked_elsewhere: false,
        removed,
        notify_failure,
        notify_drop,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(body: &str, ts: &str) -> ShipLine {
        ShipLine {
            seq: 0,
            ts: ts.into(),
            level: "INFO".into(),
            body: body.into(),
            attrs: BTreeMap::new(),
        }
    }

    fn now_ts() -> String {
        Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
    }

    fn opts() -> ShipOptions {
        ShipOptions::from(&crate::logship::LogsSpec::default())
    }

    #[tokio::test]
    async fn writer_assigns_sequences_caps_and_records_the_end() {
        let dir = tempfile::tempdir().unwrap();
        let spool = Spool::open(dir.path()).unwrap();
        let mut w = SpoolWriter::new(spool.clone());
        w.begin(
            RunMeta {
                run_id: "r1".into(),
                started_at: Some(Utc::now()),
                ..Default::default()
            },
            3,
        );
        w.begin(
            RunMeta {
                run_id: "r1".into(),
                ..Default::default()
            },
            3,
        );
        assert!(w.is_open("r1"));
        for i in 0..5 {
            w.append("r1", line(&format!("l{i}"), &now_ts()));
        }
        w.append("nope", line("x", &now_ts()));
        w.add_queue_dropped("r1", 2);
        w.add_queue_dropped("nope", 2);
        w.flush(true);
        let lines = spool.read_lines("r1", 0, 100).unwrap();
        assert_eq!(
            lines.iter().map(|(l, _)| l.seq).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        w.end("r1");
        w.end("r1");
        let meta = spool.read_meta("r1").unwrap();
        assert_eq!(meta.capped, 2);
        assert_eq!(meta.queue_dropped, 2);
        assert!(meta.ended_at.is_some());
        let v = run_view(&spool, "r1").unwrap();
        assert_eq!(v.pending_lines, 3);
        assert_eq!(v.dropped_lines, 4);
        assert_eq!(w.write_failures(), 0);
        // Reopening continues the sequence.
        w.begin(
            RunMeta {
                run_id: "r1".into(),
                ..Default::default()
            },
            10,
        );
        w.append("r1", line("again", &now_ts()));
        w.end("r1");
        let seqs: Vec<u64> = spool
            .read_lines("r1", 0, 100)
            .unwrap()
            .iter()
            .map(|(l, _)| l.seq)
            .collect();
        assert_eq!(seqs, vec![1, 2, 3, 4]);
        assert!(run_view(&spool, "missing").is_none());
    }

    #[test]
    fn partial_and_malformed_lines() {
        let dir = tempfile::tempdir().unwrap();
        let spool = Spool::open(dir.path()).unwrap();
        std::fs::write(spool.segment("r"), b"not json\n{\"seq\":1").unwrap();
        let lines = spool.read_lines("r", 0, 10).unwrap();
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0].0.body, "not json");
        assert_eq!(spool.count_from("r", 0).0, 1);
        assert_eq!(spool.count_from("r", 9).0, 0);
        assert_eq!(spool.count_from("absent", 0).0, 0);
        assert!(spool.read_lines("absent", 0, 10).unwrap().is_empty());
        assert_eq!(spool.read_cursor("r"), Cursor::default());
    }

    #[test]
    fn locks_are_shared_in_process() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("x.lock");
        let a = RunLock::acquire(p.clone()).unwrap();
        let b = RunLock::acquire(p.clone()).unwrap();
        drop(a);
        drop(b);
        let c = RunLock::acquire(p.clone()).unwrap();
        // Another open file description conflicts (as another process would).
        let other = OpenOptions::new().write(true).open(&p).unwrap();
        assert!(other.try_lock().is_err());
        drop(c);
        assert!(other.try_lock().is_ok());
        assert!(RunLock::acquire(dir.path().join("no/such/dir.lock")).is_none());
    }

    #[tokio::test]
    async fn run_locked_by_another_process_is_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let spool = Spool::open(dir.path()).unwrap();
        let mut w = SpoolWriter::new(spool.clone());
        w.begin(
            RunMeta {
                run_id: "held".into(),
                ..Default::default()
            },
            10,
        );
        w.append("held", line("a", &now_ts()));
        w.end("held");
        let other = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(spool.lock_path("held"))
            .unwrap();
        other.try_lock().unwrap();
        let r = ship_pass(&spool, None, opts(), Utc::now()).await;
        let run = r.run("held").unwrap();
        assert!(run.locked_elsewhere);
        assert_eq!(run.view.pending_lines, 1);
    }

    #[tokio::test]
    async fn max_age_drops_the_oldest_undelivered_lines() {
        let dir = tempfile::tempdir().unwrap();
        let spool = Spool::open(dir.path()).unwrap();
        let mut w = SpoolWriter::new(spool.clone());
        w.begin(
            RunMeta {
                run_id: "old".into(),
                ..Default::default()
            },
            100,
        );
        let old = (Utc::now() - chrono::Duration::days(30))
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        w.append("old", line("a", &old));
        w.append("old", line("b", &old));
        w.append("old", line("c", &now_ts()));
        w.end("old");
        let r = ship_pass(&spool, None, opts(), Utc::now()).await;
        let run = r.run("old").unwrap();
        assert_eq!(run.view.dropped_lines, 2);
        assert_eq!(run.view.pending_lines, 1);
        assert_eq!(
            run.view.status,
            crate::logship::LogExportStatus::PartiallyDropped
        );
        assert!(run.notify_drop);
        assert_eq!(r.undelivered(), 1);
        assert_eq!(r.gauges.lines, 1);
        let again = ship_pass(&spool, None, opts(), Utc::now()).await;
        assert!(!again.run("old").unwrap().notify_drop, "notified once");
    }

    #[tokio::test]
    async fn max_bytes_drops_settled_then_oldest_runs() {
        let dir = tempfile::tempdir().unwrap();
        let spool = Spool::open(dir.path()).unwrap();
        let mut w = SpoolWriter::new(spool.clone());
        for id in ["a", "b", "c"] {
            w.begin(
                RunMeta {
                    run_id: id.into(),
                    ..Default::default()
                },
                100,
            );
            for _ in 0..10 {
                w.append(id, line(&"x".repeat(100), &now_ts()));
            }
            w.end(id);
            std::thread::sleep(Duration::from_millis(15));
        }
        let mut c = spool.read_cursor("a");
        c.settled_at = Some(Utc::now());
        c.offset = spool.segment_len("a");
        spool.write_cursor("a", &c).unwrap();
        let one = spool.segment_len("b");
        let o = ShipOptions {
            max_bytes: one + 10,
            ..opts()
        };
        let r = ship_pass(&spool, None, o, Utc::now()).await;
        assert!(!spool.segment("a").exists(), "settled run reclaimed first");
        assert_eq!(r.run("b").unwrap().view.dropped_lines, 10);
        assert!(!spool.segment("b").exists());
        assert_eq!(r.run("c").unwrap().view.dropped_lines, 0);
        assert_eq!(r.run("c").unwrap().view.pending_lines, 10);
    }

    #[tokio::test]
    async fn settled_runs_are_removed_after_retention() {
        let dir = tempfile::tempdir().unwrap();
        let spool = Spool::open(dir.path()).unwrap();
        std::fs::write(spool.segment("orphan"), b"").unwrap();
        let o = ShipOptions {
            retention: Duration::ZERO,
            ..opts()
        };
        let r = ship_pass(&spool, None, o, Utc::now()).await;
        assert!(r.run("orphan").unwrap().removed);
        assert!(spool.run_ids().is_empty());
        let missing = Spool {
            dir: dir.path().join("gone"),
        };
        assert!(missing.run_ids().is_empty());
    }
}
