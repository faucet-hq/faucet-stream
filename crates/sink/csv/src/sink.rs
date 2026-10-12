//! CSV file sink.

use crate::config::CsvSinkConfig;
use async_trait::async_trait;
use faucet_core::FaucetError;
use serde_json::Value;
use std::fs::OpenOptions;
use std::sync::Mutex;

/// The inner writer the CSV serializer writes into. With compression enabled
/// it is a [`SyncCompressWriter`](faucet_core::compression::SyncCompressWriter)
/// that retains the concrete encoder so `finish()` errors surface on flush
/// (#78/#41); otherwise it is the raw file.
#[cfg(feature = "compression")]
type SinkWriter = faucet_core::compression::SyncCompressWriter<std::fs::File>;
#[cfg(not(feature = "compression"))]
type SinkWriter = std::fs::File;

/// Create or truncate the output to an empty file (a valid empty compressed
/// stream when a codec applies).
fn truncate_blocking(config: &CsvSinkConfig) -> Result<(), FaucetError> {
    if let Some(parent) = std::path::Path::new(&config.path).parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent).map_err(|e| {
            FaucetError::Sink(format!(
                "failed to create parent directory '{}': {e}",
                parent.display()
            ))
        })?;
    }
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&config.path)
        .map_err(|e| {
            FaucetError::Sink(format!("failed to open CSV file '{}': {e}", config.path))
        })?;
    #[cfg(feature = "compression")]
    {
        let codec = config.compression.resolve(&config.path);
        faucet_core::compression::sync_compress_writer(file, codec)
            .finish()
            .map_err(|e| FaucetError::Sink(format!("CSV compression finalise failed: {e}")))?;
    }
    #[cfg(not(feature = "compression"))]
    drop(file);
    Ok(())
}

/// State for the CSV writer, including the determined column order.
struct WriterState {
    writer: csv::Writer<SinkWriter>,
    columns: Vec<String>,
}

/// A sink that writes JSON records to a CSV file.
///
/// Column order is the union of keys across the records of the first
/// `write_batch` call, in first-seen order (so a field present only in a later
/// record of that batch is still captured). Subsequent records use the same
/// column order; missing fields are written as empty strings.
///
/// [`Sink::flush`](faucet_core::Sink::flush) finalises the encoder (writes the trailer) and clears the
/// writer slot — a subsequent `write_batch` reopens the file in append mode
/// (independent of `config.append`) and starts a fresh encoder. This makes
/// the per-page `flush` the pipeline emits for bookmarked pages safe for CDC
/// sources — every transaction appends rather than truncates.
#[deprecated(
    since = "1.3.0",
    note = "use faucet-sink-file: FileSinkConfig with `format: csv`, CSV options under `csv:`"
)]
pub struct CsvSink {
    config: CsvSinkConfig,
    state: Mutex<Option<WriterState>>,
    /// The column order frozen at the first open, retained **across `flush()`**.
    /// `flush()` drops `state` (and its `columns`), so without this a re-open
    /// after flush would re-derive columns from the *current* batch — a
    /// different order or subset than the already-written header — silently
    /// misaligning later rows and defeating the `on_unknown_field` guard (audit
    /// #321 H2). Set once on the first write; every re-open reuses it.
    frozen_columns: Mutex<Option<Vec<String>>>,
    /// Tracks whether the file has been opened at least once.
    /// On re-opens (after `flush()` clears the writer), we always use
    /// append mode regardless of `config.append` so the new gzip / zstd
    /// member appends instead of truncating the file. Without this, the
    /// pipeline's per-bookmark flush would silently lose data when
    /// `config.append = false` (the default).
    opened_once: std::sync::atomic::AtomicBool,
    /// One-shot guard for the "dropping unknown field" warning. The CSV header
    /// is frozen from the first batch, so a field that first appears later
    /// cannot be added to the header and its value is dropped. We warn at most
    /// once per run (like the parquet sink's `warn_on_unknown_fields`) so the
    /// loss is visible rather than silent, without flooding the log.
    warned_unknown: std::sync::atomic::AtomicBool,
    /// The file this sink opened, for the local-output retention GC (#587).
    /// Recorded at the first open — before the file is created — so a path that
    /// already held someone else's data is flagged `pre_existing` and never
    /// collected. See `faucet_core::local_outputs`.
    outputs: faucet_core::LocalOutputLog,
    /// Set when a write failed part-way and the output could not be repaired:
    /// rows already reported written may be lost, so every later call fails.
    poisoned: Mutex<Option<String>>,
}

impl CsvSink {
    /// Create a new CSV sink. The file is opened on the first `write_batch` call.
    pub fn new(config: CsvSinkConfig) -> Self {
        Self {
            config,
            state: Mutex::new(None),
            frozen_columns: Mutex::new(None),
            opened_once: std::sync::atomic::AtomicBool::new(false),
            warned_unknown: std::sync::atomic::AtomicBool::new(false),
            outputs: faucet_core::LocalOutputLog::new(),
            poisoned: Mutex::new(None),
        }
    }

    fn check_poisoned(&self) -> Result<(), FaucetError> {
        match self.poisoned.lock() {
            Ok(guard) => match guard.as_ref() {
                Some(msg) => Err(FaucetError::Sink(msg.clone())),
                None => Ok(()),
            },
            Err(e) => Err(FaucetError::Sink(format!("CSV sink lock poisoned: {e}"))),
        }
    }

    /// Convert a JSON value to a string suitable for a CSV field.
    fn value_to_csv_field(value: &Value) -> String {
        match value {
            Value::Null => String::new(),
            Value::String(s) => s.clone(),
            Value::Bool(b) => b.to_string(),
            Value::Number(n) => n.to_string(),
            // For nested objects/arrays, serialize as JSON.
            other => other.to_string(),
        }
    }
}

#[async_trait]
impl faucet_core::Sink for CsvSink {
    fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        self.config.batch_atomicity()
    }

    fn connector_name(&self) -> &'static str {
        "csv"
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(CsvSinkConfig)).expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        format!("file://{}", self.config.path)
    }

    async fn local_outputs(&self) -> Vec<faucet_core::LocalOutput> {
        self.outputs.snapshot()
    }

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        self.check_poisoned()?;
        if records.is_empty() {
            return Ok(0);
        }

        if records.iter().any(|r| !r.is_object()) {
            return Err(FaucetError::Sink(
                "CSV sink expects JSON objects, got non-object record".into(),
            ));
        }

        let config = self.config.clone();
        let records: Vec<Value> = records.to_vec();

        // Extract state from the mutex before entering the blocking task.
        // This avoids holding the MutexGuard across an await point.
        let current_state = {
            let mut guard = self
                .state
                .lock()
                .map_err(|e| FaucetError::Sink(format!("CSV sink lock poisoned: {e}")))?;
            guard.take()
        };

        let opened_before = self.opened_once.load(std::sync::atomic::Ordering::Relaxed);
        let already_warned = self
            .warned_unknown
            .load(std::sync::atomic::Ordering::Relaxed);
        // The frozen header, if a prior open already established it. On a re-open
        // after `flush()` this is reused verbatim so the column order never
        // drifts from the written header (#321 H2).
        let frozen_columns = {
            let guard = self
                .frozen_columns
                .lock()
                .map_err(|e| FaucetError::Sink(format!("CSV sink lock poisoned: {e}")))?;
            guard.clone()
        };

        // Provenance for the retention GC (#587). Probed here — on the async side,
        // before the blocking task can create the file — so a path that already
        // held someone else's data is never mistaken for one faucet created.
        // Idempotent and first-open-wins, so the flush→reopen cycle cannot
        // reclassify it.
        self.outputs.record_open_probing_with(
            std::path::PathBuf::from(&self.config.path),
            !opened_before && !self.config.append,
        );

        let result = tokio::task::spawn_blocking(move || {
            write_csv_blocking(
                config,
                current_state,
                &records,
                opened_before,
                already_warned,
                frozen_columns,
            )
        })
        .await
        .map_err(|e| FaucetError::Sink(format!("CSV write task failed: {e}")))?;

        let (state, result) = result;
        if let Err(Failure::Lost(msg)) = &result
            && let Ok(mut guard) = self.poisoned.lock()
        {
            *guard = Some(msg.clone());
        }
        let result = result.map_err(Failure::into_error);
        // After a clean failure the writer goes back, so earlier rows (and a
        // compressed stream's open frame) are still finished by `flush()`.
        if let Some(state) = state {
            let mut guard = self
                .state
                .lock()
                .map_err(|e| FaucetError::Sink(format!("CSV sink lock poisoned: {e}")))?;
            *guard = Some(state);
        }
        let WriteOutcome {
            columns,
            count,
            warned_unknown,
        } = result?;

        // Mark opened. From now on, re-opens (after flush) use append mode.
        self.opened_once
            .store(true, std::sync::atomic::Ordering::Relaxed);

        // Latch the frozen column order on the first open so it survives future
        // `flush()`-then-write cycles (set once; never changes thereafter).
        {
            let mut guard = self
                .frozen_columns
                .lock()
                .map_err(|e| FaucetError::Sink(format!("CSV sink lock poisoned: {e}")))?;
            if guard.is_none() {
                *guard = Some(columns);
            }
        }

        // Latch the one-shot unknown-field warning so it fires once per run.
        if warned_unknown {
            self.warned_unknown
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }

        Ok(count)
    }

    async fn flush(&self) -> Result<(), FaucetError> {
        self.check_poisoned()?;
        // Take the state out of the mutex so we can move it into a blocking
        // task. Replacing it with None means the next write_batch reopens
        // the file in append mode — for compressed output this starts a
        // fresh gzip/zstd member, which decoders read back transparently.
        let state = {
            let mut guard = self
                .state
                .lock()
                .map_err(|e| FaucetError::Sink(format!("CSV sink lock poisoned: {e}")))?;
            guard.take()
        };
        if let Some(state) = state {
            tokio::task::spawn_blocking(move || -> Result<(), FaucetError> {
                let WriterState { writer, .. } = state;
                // Flush the csv serializer's buffer and recover the inner
                // writer so the compression encoder can be finalised with its
                // error captured, rather than swallowed on drop (#78/#41).
                let inner = writer
                    .into_inner()
                    .map_err(|e| FaucetError::Sink(format!("CSV flush failed: {e}")))?;
                #[cfg(feature = "compression")]
                let file = inner.finish().map_err(|e| {
                    FaucetError::Sink(format!("CSV compression finalise failed: {e}"))
                })?;
                #[cfg(not(feature = "compression"))]
                let file = inner;
                // Durable before the pipeline records the bookmark.
                file.sync_all()
                    .map_err(|e| FaucetError::Sink(format!("CSV fsync failed: {e}")))?;
                Ok(())
            })
            .await
            .map_err(|e| FaucetError::Sink(format!("CSV flush task failed: {e}")))??;
        }
        Ok(())
    }

    /// With `append: false` the file must hold exactly this run's output, so a
    /// successful run that wrote no record still truncates it (#753). There is
    /// no record to derive a header from, so the result is an empty file (an
    /// empty gzip / zstd stream under compression). Only reached after a
    /// successful, uncancelled run.
    async fn complete_run(&self) -> Result<(), FaucetError> {
        if self.config.append || self.opened_once.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(());
        }
        self.outputs
            .record_open_probing_with(std::path::PathBuf::from(&self.config.path), true);
        let config = self.config.clone();
        tokio::task::spawn_blocking(move || truncate_blocking(&config))
            .await
            .map_err(|e| FaucetError::Sink(format!("CSV truncate task failed: {e}")))??;
        self.opened_once
            .store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /// Preflight probe for `faucet doctor`. Verifies the configured output
    /// path's parent directory exists and is writable by creating, then
    /// immediately removing, a uniquely-named temp file there. Never touches
    /// the user's actual output file, so it is fully idempotent.
    async fn check(
        &self,
        _ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::CheckReport;
        let path = self.config.path.clone();
        // The filesystem probe is synchronous; run it on a blocking thread to
        // stay off the async runtime, matching how the sink does its I/O.
        let probe = tokio::task::spawn_blocking(move || {
            crate::probe::probe_parent_writable(&path, std::time::Instant::now())
        })
        .await
        .map_err(|e| FaucetError::Sink(format!("CSV check task failed: {e}")))?;
        Ok(CheckReport::single(probe))
    }
}

/// Result of one blocking CSV write: the (returned) writer state, the number
/// of records written, and whether this batch emitted the one-shot
/// "dropping unknown field" warning (so the caller can latch its atomic flag).
struct WriteOutcome {
    columns: Vec<String>,
    count: usize,
    warned_unknown: bool,
}

/// Identify record keys that are absent from the frozen `columns` set, in
/// first-seen order across `records`, deduplicated. These fields cannot be
/// written (the header is already fixed) and would otherwise be silently
/// dropped. Pure — no I/O — so it is unit-testable.
fn unknown_fields(columns: &[String], records: &[Value]) -> Vec<String> {
    let known: std::collections::HashSet<&str> = columns.iter().map(String::as_str).collect();
    let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut out: Vec<String> = Vec::new();
    for record in records {
        if let Value::Object(map) = record {
            for k in map.keys() {
                if !known.contains(k.as_str()) && seen.insert(k.as_str()) {
                    out.push(k.clone());
                }
            }
        }
    }
    out
}

/// Cut a torn last row (an interrupted write) from an uncompressed CSV file
/// before appending to it: everything after the last newline that ends a
/// record, i.e. one outside a quoted field. A file that ends in a newline is
/// left alone without being read.
fn cut_torn_row(path: &std::path::Path) -> std::io::Result<()> {
    use std::io::{Read, Seek, SeekFrom};
    // Anything but a regular file this process can read and write is left to
    // the header inspection and the open that follow, which report it.
    if !std::fs::metadata(path).is_ok_and(|m| m.is_file()) {
        return Ok(());
    }
    let Ok(mut file) = OpenOptions::new().read(true).write(true).open(path) else {
        return Ok(());
    };
    let len = file.metadata()?.len();
    if len == 0 {
        return Ok(());
    }
    let mut last = [0u8; 1];
    file.seek(SeekFrom::Start(len - 1))?;
    file.read_exact(&mut last)?;
    if last[0] == b'\n' {
        return Ok(());
    }
    file.seek(SeekFrom::Start(0))?;
    let mut reader = std::io::BufReader::with_capacity(64 * 1024, &file);
    let (mut quoted, mut keep, mut pos) = (false, 0u64, 0u64);
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        for &b in &buf[..n] {
            pos += 1;
            match b {
                b'"' => quoted = !quoted,
                b'\n' if !quoted => keep = pos,
                _ => {}
            }
        }
    }
    drop(reader);
    if keep < len {
        tracing::warn!(
            path = %path.display(),
            bytes = len - keep,
            "csv sink: cutting a partial last row left by an interrupted write"
        );
        file.set_len(keep)?;
        file.sync_all()?;
    }
    Ok(())
}

/// The header row of the file an `append: true` run continues, or `None`
/// when the file is missing or empty (the run then writes one).
fn read_existing_header(config: &CsvSinkConfig) -> Result<Option<Vec<String>>, FaucetError> {
    let path = std::path::Path::new(&config.path);
    match std::fs::metadata(path) {
        Ok(m) if m.len() > 0 => {}
        Ok(_) => return Ok(None),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(FaucetError::Sink(format!(
                "failed to inspect CSV file '{}': {e}",
                config.path
            )));
        }
    }
    let file = std::fs::File::open(path).map_err(|e| {
        FaucetError::Sink(format!("failed to open CSV file '{}': {e}", config.path))
    })?;
    #[cfg(feature = "compression")]
    let input =
        faucet_core::compression::wrap_sync_reader(file, config.compression.resolve(&config.path));
    #[cfg(not(feature = "compression"))]
    let input = file;
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(config.delimiter)
        .has_headers(false)
        .flexible(true)
        .from_reader(input);
    let mut record = csv::StringRecord::new();
    let found = reader.read_record(&mut record).map_err(|e| {
        FaucetError::Sink(format!(
            "failed to read the header of CSV file '{}' to append to it: {e}",
            config.path
        ))
    })?;
    Ok(found.then(|| record.iter().map(str::to_string).collect()))
}

/// Make a newly created file's directory entry durable.
fn sync_parent_dir(path: &std::path::Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(std::path::Path::new("."));
        std::fs::File::open(parent)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}

/// Synchronous CSV writing logic, run inside `spawn_blocking`. The writer
/// state comes back whatever happened, so a failed batch never drops it.
/// How a blocking write failed.
#[derive(Debug)]
enum Failure {
    /// Nothing reached the writer; it can be reused.
    Clean(FaucetError),
    /// A row was cut part-way, but the file was flushed and cut back to its
    /// last complete row; the writer was dropped, the next write reopens it.
    Repaired(FaucetError),
    /// A row was cut part-way and the file could not be repaired.
    Lost(String),
}

impl Failure {
    fn into_error(self) -> FaucetError {
        match self {
            Failure::Clean(e) | Failure::Repaired(e) => e,
            Failure::Lost(msg) => FaucetError::Sink(msg),
        }
    }
}

fn write_csv_blocking(
    config: CsvSinkConfig,
    existing_state: Option<WriterState>,
    records: &[Value],
    opened_before: bool,
    already_warned: bool,
    frozen_columns: Option<Vec<String>>,
) -> (Option<WriterState>, Result<WriteOutcome, Failure>) {
    let mut state = match existing_state {
        Some(s) => s,
        None => match open_writer(&config, records, opened_before, frozen_columns) {
            Ok(s) => s,
            Err(e) => return (None, Err(Failure::Clean(e))),
        },
    };
    let mark = match batch_start(&config, &mut state) {
        Ok(mark) => mark,
        Err(e) => return (None, Err(repair(&config, state, e, None))),
    };
    match write_rows(&config, &mut state, records, already_warned) {
        Ok(outcome) => (Some(state), Ok(outcome)),
        Err(RowsError::Clean(e)) => (Some(state), Err(Failure::Clean(e))),
        Err(RowsError::Dirty(e)) => (None, Err(repair(&config, state, e, mark))),
    }
}

/// The file length before this batch's first row, with every earlier row
/// flushed to it — the point a failed batch is cut back to. `None` for a
/// compressed stream, which cannot be cut.
fn batch_start(
    config: &CsvSinkConfig,
    state: &mut WriterState,
) -> Result<Option<u64>, FaucetError> {
    #[cfg(feature = "compression")]
    if config.compression.resolve(&config.path) != faucet_core::Compression::None {
        return Ok(None);
    }
    state
        .writer
        .flush()
        .map_err(|e| FaucetError::Sink(format!("CSV flush error: {e}")))?;
    std::fs::metadata(&config.path)
        .map(|m| Some(m.len()))
        .map_err(|e| FaucetError::Sink(format!("CSV sink: reading {}: {e}", config.path)))
}

/// Recover from a write cut part-way: flush what the writer holds and cut the
/// file back to `mark`, its length before the failed batch — a newline search
/// could stop inside a quoted multi-line field. A compressed stream cannot be
/// cut, so it is lost.
fn repair(
    config: &CsvSinkConfig,
    state: WriterState,
    cause: FaucetError,
    mark: Option<u64>,
) -> Failure {
    let lost = |why: String| {
        Failure::Lost(format!(
            "CSV sink: a write failed part-way ({cause}) and {why}; rows reported written may \
             be lost, so the sink refuses further writes"
        ))
    };
    #[cfg(feature = "compression")]
    if config.compression.resolve(&config.path) != faucet_core::Compression::None {
        return lost("a compressed stream cannot be cut back".into());
    }
    let inner = match state.writer.into_inner() {
        Ok(inner) => inner,
        Err(e) => return lost(format!("flushing the rows before it failed: {}", e.error())),
    };
    #[cfg(feature = "compression")]
    let file = match inner.finish() {
        Ok(f) => f,
        Err(e) => return lost(format!("flushing the rows before it failed: {e}")),
    };
    #[cfg(not(feature = "compression"))]
    let file = inner;
    if let Err(e) = file.sync_all() {
        return lost(format!("syncing the file: {e}"));
    }
    drop(file);
    let Some(mark) = mark else {
        return lost("the length before the batch is unknown".into());
    };
    match truncate_to(std::path::Path::new(&config.path), mark) {
        Ok(()) => Failure::Repaired(cause),
        Err(e) => lost(format!("cutting the partial batch: {e}")),
    }
}

/// Truncate `path` to `len` bytes.
fn truncate_to(path: &std::path::Path, len: u64) -> std::io::Result<()> {
    let f = OpenOptions::new().write(true).open(path)?;
    f.set_len(len)?;
    f.sync_all()
}

/// How [`write_rows`] failed.
#[derive(Debug)]
enum RowsError {
    /// Refused before any byte was written.
    Clean(FaucetError),
    /// A row failed part-way through.
    Dirty(FaucetError),
}

/// Open (or reopen) the output and write the header when one is due.
fn open_writer(
    config: &CsvSinkConfig,
    records: &[Value],
    opened_before: bool,
    frozen_columns: Option<Vec<String>>,
) -> Result<WriterState, FaucetError> {
    let config = config.clone();
    {
        {
            // Column order. On a re-open after `flush()` reuse the header frozen
            // at the first open (#321 H2) so later rows never drift out of
            // alignment with the already-written header. Only on the very first
            // open do we derive columns from the UNION of keys across the first
            // batch's records, in first-seen order — not just `records[0]`.
            // Otherwise a field present only in a later record of the first batch
            // would be absent from the header and silently dropped from every row
            // (audit #146 H2). (A later flush-segment cannot change the
            // already-written header — that is a separate, documented limitation.)
            let mut columns: Vec<String> = Vec::new();
            #[cfg(feature = "compression")]
            let plain = config.compression.resolve(&config.path)
                == faucet_core::compression::Compression::None;
            #[cfg(not(feature = "compression"))]
            let plain = true;
            if (opened_before || config.append) && plain {
                cut_torn_row(std::path::Path::new(&config.path)).map_err(|e| {
                    FaucetError::Sink(format!(
                        "failed to repair CSV file '{}' before appending: {e}",
                        config.path
                    ))
                })?;
            }
            let existing_header = if opened_before || !config.append || !config.write_headers {
                None
            } else {
                read_existing_header(&config)?
            };
            match frozen_columns.or_else(|| existing_header.clone()) {
                Some(frozen) => columns = frozen,
                None => {
                    let mut seen: std::collections::HashSet<&str> =
                        std::collections::HashSet::new();
                    for record in records {
                        match record {
                            Value::Object(map) => {
                                for k in map.keys() {
                                    if seen.insert(k.as_str()) {
                                        columns.push(k.clone());
                                    }
                                }
                            }
                            _ => {
                                return Err(FaucetError::Sink(
                                    "CSV sink expects JSON objects, got non-object record".into(),
                                ));
                            }
                        }
                    }
                }
            }

            // First open obeys `config.append`. Re-opens (after flush()
            // cleared the writer) always append, so flush-then-write
            // sequences do not truncate previously-written data.
            let (append, truncate) = if opened_before {
                (true, false)
            } else {
                (config.append, !config.append)
            };

            if let Some(parent) = std::path::Path::new(&config.path).parent()
                && !parent.as_os_str().is_empty()
            {
                std::fs::create_dir_all(parent).map_err(|e| {
                    FaucetError::Sink(format!(
                        "failed to create parent directory '{}': {e}",
                        parent.display()
                    ))
                })?;
            }
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .append(append)
                .truncate(truncate)
                .open(&config.path)
                .map_err(|e| {
                    FaucetError::Sink(format!("failed to open CSV file '{}': {e}", config.path))
                })?;
            if !opened_before {
                sync_parent_dir(std::path::Path::new(&config.path))
                    .map_err(|e| FaucetError::Sink(format!("CSV directory fsync failed: {e}")))?;
            }

            #[cfg(feature = "compression")]
            let inner: SinkWriter = {
                let codec = config.compression.resolve(&config.path);
                faucet_core::compression::warn_mismatch(&config.path, codec);
                faucet_core::compression::sync_compress_writer(file, codec)
            };
            #[cfg(not(feature = "compression"))]
            let inner: SinkWriter = file;

            let mut writer = csv::WriterBuilder::new()
                .delimiter(config.delimiter)
                .from_writer(inner);

            // Write the header on the first open unless the file being appended
            // to already has one.
            if config.write_headers && !opened_before && existing_header.is_none() {
                writer
                    .write_record(&columns)
                    .map_err(|e| FaucetError::Sink(format!("failed to write CSV headers: {e}")))?;
            }

            Ok(WriterState { writer, columns })
        }
    }
}

/// Write `records` through an open writer.
fn write_rows(
    config: &CsvSinkConfig,
    state: &mut WriterState,
    records: &[Value],
    already_warned: bool,
) -> Result<WriteOutcome, RowsError> {
    write_rows_to(
        config,
        &mut state.writer,
        &state.columns,
        records,
        already_warned,
    )
}

fn write_rows_to<W: std::io::Write>(
    config: &CsvSinkConfig,
    writer: &mut csv::Writer<W>,
    columns: &[String],
    records: &[Value],
    already_warned: bool,
) -> Result<WriteOutcome, RowsError> {
    // The header is now frozen (either just written, or carried over from a
    // prior batch). Detect any record key that is not a known column — it
    // cannot be added to the header and would be dropped from the output.
    // Under `error` this aborts the write before any row is written; under
    // `warn` (default) it emits a single warning per run and continues.
    let unknown = unknown_fields(columns, records);
    let mut warned_unknown = false;
    if !unknown.is_empty() {
        match config.on_unknown_field {
            crate::config::OnUnknownField::Error => {
                return Err(RowsError::Clean(FaucetError::Sink(format!(
                    "CSV sink received record field(s) not in the frozen column set \
                     and on_unknown_field=error: [{}]. The CSV header is fixed from the \
                     first batch and cannot be extended; these values would be dropped.",
                    unknown.join(", ")
                ))));
            }
            crate::config::OnUnknownField::Warn => {
                if !already_warned {
                    warned_unknown = true;
                    tracing::warn!(
                        fields = %unknown.join(", "),
                        path = %config.path,
                        "dropping field(s) not in the frozen CSV column set — the header is \
                         fixed from the first batch and cannot be extended; set \
                         on_unknown_field=error to fail instead"
                    );
                }
            }
        }
    }

    let mut count = 0;
    for record in records {
        let row: Vec<String> = columns
            .iter()
            .map(|col| {
                record
                    .get(col)
                    .map(CsvSink::value_to_csv_field)
                    .unwrap_or_default()
            })
            .collect();

        writer
            .write_record(&row)
            .map_err(|e| RowsError::Dirty(FaucetError::Sink(format!("CSV write error: {e}"))))?;
        count += 1;
    }

    tracing::debug!(records = count, path = %config.path, "CSV batch written");

    Ok(WriteOutcome {
        columns: columns.to_vec(),
        count,
        warned_unknown,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::Sink;
    use serde_json::json;
    use tempfile::NamedTempFile;

    /// #862 L-155: a torn last row is cut back to the last record boundary,
    /// which a newline inside a quoted field is not.
    #[test]
    fn cut_torn_row_respects_quoted_newlines() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("t.csv");
        let cases: [(&str, &str); 5] = [
            ("a,b\n1,2\n3,\"x\ny", "a,b\n1,2\n"),
            ("a,b\n1,\"p\nq\"\n4,", "a,b\n1,\"p\nq\"\n"),
            ("a,b\n1,2\n", "a,b\n1,2\n"),
            ("a,b", ""),
            ("", ""),
        ];
        for (before, after) in cases {
            std::fs::write(&p, before).unwrap();
            cut_torn_row(&p).unwrap();
            assert_eq!(std::fs::read_to_string(&p).unwrap(), after, "{before:?}");
        }
        cut_torn_row(&dir.path().join("missing.csv")).unwrap();
    }

    #[tokio::test]
    async fn an_append_run_cuts_a_torn_last_row_first() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("out.csv");
        std::fs::write(&p, "id,name\n1,a\n2,\"tor").unwrap();
        let mut cfg = CsvSinkConfig::new(p.to_str().unwrap());
        cfg.append = true;
        let sink = CsvSink::new(cfg);
        sink.write_batch(&[json!({"id": 3, "name": "c"})])
            .await
            .unwrap();
        sink.flush().await.unwrap();
        assert_eq!(std::fs::read_to_string(&p).unwrap(), "id,name\n1,a\n3,c\n");
    }

    struct FailAfter(usize);

    impl std::io::Write for FailAfter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if self.0 == 0 {
                return Err(std::io::Error::other("disk full"));
            }
            let n = buf.len().min(self.0);
            self.0 -= n;
            Ok(n)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn a_row_cut_part_way_is_a_dirty_failure() {
        let config = CsvSinkConfig::new("unused.csv");
        let mut writer = csv::WriterBuilder::new()
            .buffer_capacity(1)
            .from_writer(FailAfter(5));
        let columns = vec!["a".to_string()];
        let rows = vec![json!({"a": "0123456789"})];
        assert!(matches!(
            write_rows_to(&config, &mut writer, &columns, &rows, false),
            Err(RowsError::Dirty(_))
        ));
        let mut strict = config.clone();
        strict.on_unknown_field = crate::config::OnUnknownField::Error;
        assert!(matches!(
            write_rows_to(&strict, &mut writer, &columns, &[json!({"b": 1})], false),
            Err(RowsError::Clean(_))
        ));
    }

    fn state_over(path: &std::path::Path) -> WriterState {
        let file = OpenOptions::new().append(true).open(path).unwrap();
        #[cfg(feature = "compression")]
        let file =
            faucet_core::compression::sync_compress_writer(file, faucet_core::Compression::None);
        WriterState {
            writer: csv::WriterBuilder::new().from_writer(file),
            columns: vec!["a".into()],
        }
    }

    #[test]
    fn a_dirty_failure_cuts_the_partial_row() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.csv");
        std::fs::write(&path, "a\n1\n\"line one\nline").unwrap();
        let config = CsvSinkConfig::new(path.to_str().unwrap());
        let failure = repair(
            &config,
            state_over(&path),
            FaucetError::Sink("io".into()),
            Some(4),
        );
        assert!(matches!(failure, Failure::Repaired(_)), "{failure:?}");
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "a\n1\n",
            "cut to the batch start, not into the open quoted field"
        );
        assert!(failure.into_error().to_string().contains("io"));
        assert!(truncate_to(&dir.path().join("missing.csv"), 0).is_err());
        let failure = repair(
            &config,
            state_over(&path),
            FaucetError::Sink("io".into()),
            None,
        );
        assert!(matches!(failure, Failure::Lost(_)), "{failure:?}");
    }

    #[test]
    fn batch_start_flushes_earlier_rows_and_reads_the_length() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.csv");
        std::fs::write(&path, "a\n").unwrap();
        let config = CsvSinkConfig::new(path.to_str().unwrap());
        let mut state = state_over(&path);
        state.writer.write_record(["1"]).unwrap();
        assert_eq!(batch_start(&config, &mut state).unwrap(), Some(4));
        std::fs::remove_file(&path).unwrap();
        assert!(batch_start(&config, &mut state).is_err());
    }

    #[cfg(feature = "compression")]
    #[test]
    fn a_dirty_failure_in_a_compressed_stream_is_lost() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.csv.gz");
        std::fs::write(&path, "").unwrap();
        let config = CsvSinkConfig::new(path.to_str().unwrap());
        let mut state = state_over(&path);
        assert_eq!(batch_start(&config, &mut state).unwrap(), None);
        let failure = repair(&config, state, FaucetError::Sink("io".into()), None);
        let Failure::Lost(msg) = failure else {
            panic!("{failure:?}")
        };
        assert!(msg.contains("compressed"), "{msg}");
    }

    #[tokio::test]
    async fn a_poisoned_sink_refuses_writes_and_flushes() {
        let tmp = NamedTempFile::new().unwrap();
        let sink = CsvSink::new(CsvSinkConfig::new(tmp.path().to_str().unwrap()));
        *sink.poisoned.lock().unwrap() = Some("lost rows".into());
        let err = sink.write_batch(&[json!({"a": 1})]).await.unwrap_err();
        assert!(err.to_string().contains("lost rows"), "{err}");
        assert!(sink.flush().await.is_err());
    }

    #[tokio::test]
    async fn a_non_object_record_is_refused_in_every_batch() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path));
        sink.write_batch(&[json!({"a": 1})]).await.unwrap();
        let err = sink.write_batch(&[json!(7)]).await.unwrap_err();
        assert!(err.to_string().contains("non-object"), "{err}");
        sink.write_batch(&[json!({"a": 2})]).await.unwrap();
        sink.flush().await.unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "a\n1\n2\n");
    }

    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn a_failed_batch_keeps_the_compressed_stream_finishable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.csv.zst");
        let mut config = CsvSinkConfig::new(path.to_str().unwrap());
        config.on_unknown_field = crate::config::OnUnknownField::Error;
        let sink = CsvSink::new(config);
        sink.write_batch(&[json!({"a": 1})]).await.unwrap();
        assert!(sink.write_batch(&[json!({"a": 2, "b": 3})]).await.is_err());
        sink.write_batch(&[json!({"a": 4})]).await.unwrap();
        sink.flush().await.unwrap();
        let mut text = String::new();
        std::io::Read::read_to_string(
            &mut faucet_core::compression::wrap_sync_reader(
                std::fs::File::open(&path).unwrap(),
                faucet_core::compression::Compression::Zstd,
            ),
            &mut text,
        )
        .unwrap();
        assert_eq!(text, "a\n1\n4\n");
    }

    #[test]
    fn dataset_uri_returns_file_scheme() {
        let sink = CsvSink::new(CsvSinkConfig::new("/tmp/output.csv"));
        assert_eq!(sink.dataset_uri(), "file:///tmp/output.csv");
    }

    #[tokio::test]
    async fn writes_csv_records() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path));

        let records = vec![
            json!({"id": 1, "name": "Alice"}),
            json!({"id": 2, "name": "Bob"}),
        ];
        let count = sink.write_batch(&records).await.unwrap();
        sink.flush().await.unwrap();

        assert_eq!(count, 2);

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = content.trim().split('\n').collect();
        // Header + 2 data rows.
        assert_eq!(lines.len(), 3);
    }

    #[tokio::test]
    async fn columns_union_across_first_batch_not_just_first_record() {
        // H2 (audit #146): column order is the union of keys across the first
        // batch's records. The first record lacks `email`; before the fix the
        // header was fixed from record 0 and `email` was silently dropped from
        // every row. After the fix `email` is a column and the second record's
        // value appears (the first row leaves it empty).
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path));

        let records = vec![
            json!({ "id": 1, "name": "Alice" }),
            json!({ "id": 2, "name": "Bob", "email": "bob@x.y" }),
        ];
        sink.write_batch(&records).await.unwrap();
        sink.flush().await.unwrap();

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = content.trim().split('\n').collect();
        assert_eq!(lines.len(), 3, "header + 2 rows");
        assert!(
            lines[0].contains("email"),
            "header must include the later-record-only column: {}",
            lines[0]
        );
        // Row 2 carries the email value; row 1 leaves it empty.
        assert!(
            lines[2].contains("bob@x.y"),
            "second row must carry the unioned column value: {}",
            lines[2]
        );
    }

    #[test]
    fn unknown_fields_detects_late_keys_in_first_seen_order() {
        let columns = vec!["id".to_string(), "name".to_string()];
        let records = vec![
            json!({ "id": 1, "name": "Alice" }),
            json!({ "id": 2, "email": "b@x.y", "name": "Bob", "phone": "555" }),
            json!({ "id": 3, "email": "c@x.y" }), // email is a dup, skipped
            json!("not-an-object"),               // non-objects are ignored here
        ];
        let unknown = unknown_fields(&columns, &records);
        assert_eq!(unknown, vec!["email".to_string(), "phone".to_string()]);
    }

    #[test]
    fn unknown_fields_empty_when_all_known() {
        let columns = vec!["a".to_string(), "b".to_string()];
        let records = vec![json!({ "a": 1 }), json!({ "b": 2, "a": 3 })];
        assert!(unknown_fields(&columns, &records).is_empty());
    }

    #[tokio::test]
    async fn later_page_new_field_is_dropped_and_warns() {
        // F31: the column set is frozen from the first batch. A field that
        // first appears in a later batch (page 2) cannot be added to the
        // already-written header, so its value is dropped — but the loss is
        // now visible via a one-shot warning (default on_unknown_field=warn).
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path));

        // Page 1 freezes columns to {id, name}.
        sink.write_batch(&[json!({ "id": 1, "name": "Alice" })])
            .await
            .unwrap();
        // Page 2 introduces a new field `email` absent from page 1.
        let count = sink
            .write_batch(&[json!({ "id": 2, "name": "Bob", "email": "bob@x.y" })])
            .await
            .unwrap();
        sink.flush().await.unwrap();

        assert_eq!(count, 1);
        // The one-shot warn flag must have latched (warning path exercised).
        assert!(
            sink.warned_unknown
                .load(std::sync::atomic::Ordering::Relaxed),
            "the unknown-field warning must have fired"
        );

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = content.trim().split('\n').collect();
        assert_eq!(lines.len(), 3, "header + 2 rows");
        // The header carries only the first-page columns.
        assert!(
            !lines[0].contains("email"),
            "header must not gain the late field: {}",
            lines[0]
        );
        // The dropped field's value must NOT appear anywhere in the output.
        assert!(
            !content.contains("bob@x.y"),
            "late field value must be dropped from output: {content}"
        );
    }

    #[tokio::test]
    async fn on_unknown_field_error_aborts_with_sink_error() {
        use crate::config::OnUnknownField;
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path).on_unknown_field(OnUnknownField::Error));

        sink.write_batch(&[json!({ "id": 1, "name": "Alice" })])
            .await
            .unwrap();
        let err = sink
            .write_batch(&[json!({ "id": 2, "name": "Bob", "email": "bob@x.y" })])
            .await
            .expect_err("a late field must abort under on_unknown_field=error");
        match err {
            FaucetError::Sink(msg) => {
                assert!(msg.contains("email"), "error must name the field: {msg}");
                assert!(
                    msg.contains("on_unknown_field=error"),
                    "error must explain: {msg}"
                );
            }
            other => panic!("expected FaucetError::Sink, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn first_batch_union_does_not_trigger_unknown_warning() {
        // The first batch's column union already covers all its keys, so a
        // later-record-only field within the first batch must NOT warn.
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path));
        sink.write_batch(&[
            json!({ "id": 1, "name": "Alice" }),
            json!({ "id": 2, "name": "Bob", "email": "bob@x.y" }),
        ])
        .await
        .unwrap();
        sink.flush().await.unwrap();
        assert!(
            !sink
                .warned_unknown
                .load(std::sync::atomic::Ordering::Relaxed),
            "first-batch union must not trigger the unknown-field warning"
        );
        // And the email value IS present (it was part of the frozen union).
        let content = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(content.contains("bob@x.y"));
    }

    #[tokio::test]
    async fn writes_csv_without_headers() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path).write_headers(false));

        let records = vec![json!({"a": "1", "b": "2"})];
        sink.write_batch(&records).await.unwrap();
        sink.flush().await.unwrap();

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = content.trim().split('\n').collect();
        // No header, just 1 data row.
        assert_eq!(lines.len(), 1);
    }

    #[tokio::test]
    async fn empty_batch_returns_zero() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path));
        let count = sink.write_batch(&[]).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn multiple_batches_accumulate() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path));

        sink.write_batch(&[json!({"x": "1"})]).await.unwrap();
        sink.write_batch(&[json!({"x": "2"}), json!({"x": "3"})])
            .await
            .unwrap();
        sink.flush().await.unwrap();

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = content.trim().split('\n').collect();
        // Header + 3 data rows.
        assert_eq!(lines.len(), 4);
    }

    #[tokio::test]
    async fn missing_fields_written_as_empty() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path));

        let records = vec![
            json!({"a": "1", "b": "2"}),
            json!({"a": "3"}), // missing "b"
        ];
        sink.write_batch(&records).await.unwrap();
        sink.flush().await.unwrap();

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = content.trim().split('\n').collect();
        assert_eq!(lines.len(), 3); // header + 2 rows
    }

    #[tokio::test]
    async fn value_to_csv_field_handles_types() {
        assert_eq!(CsvSink::value_to_csv_field(&json!(null)), "");
        assert_eq!(CsvSink::value_to_csv_field(&json!("hello")), "hello");
        assert_eq!(CsvSink::value_to_csv_field(&json!(42)), "42");
        assert_eq!(CsvSink::value_to_csv_field(&json!(true)), "true");
        assert_eq!(CsvSink::value_to_csv_field(&json!(2.72)), "2.72");
    }

    #[tokio::test]
    async fn flush_without_write_is_noop() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path));
        assert!(sink.flush().await.is_ok());
    }

    #[tokio::test]
    async fn check_passes_when_parent_dir_exists() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.csv");
        let path_str = path.to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path_str));
        let report = sink
            .check(&faucet_core::check::CheckContext::default())
            .await
            .unwrap();
        assert_eq!(report.failed_count(), 0);
        assert_eq!(report.probes[0].name, "io");
        // The probe must not have created the user's output file.
        assert!(!path.exists(), "check() must not create the output file");
    }

    #[tokio::test]
    async fn check_passes_when_a_missing_parent_can_be_created() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nope").join("deeper").join("out.csv");
        let path_str = path.to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path_str));
        let report = sink
            .check(&faucet_core::check::CheckContext::default())
            .await
            .unwrap();
        assert_eq!(report.failed_count(), 0, "the sink creates the directory");
        assert!(!dir.path().join("nope").exists(), "check() creates nothing");

        let file = dir.path().join("a-file");
        std::fs::write(&file, "x").unwrap();
        let under_file = file.join("out.csv");
        let sink = CsvSink::new(CsvSinkConfig::new(under_file.to_str().unwrap()));
        let report = sink
            .check(&faucet_core::check::CheckContext::default())
            .await
            .unwrap();
        assert_eq!(report.failed_count(), 1);
        assert_eq!(report.probes[0].name, "io");
    }

    #[tokio::test]
    async fn creates_missing_parent_directories() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("a").join("b").join("out.csv");
        let path_str = nested.to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path_str));

        let records = vec![json!({"id": "1", "name": "Alice"})];
        let count = sink.write_batch(&records).await.unwrap();
        sink.flush().await.unwrap();

        assert_eq!(count, 1);
        assert!(nested.exists(), "output file must exist after write");
        let content = tokio::fs::read_to_string(&nested).await.unwrap();
        let lines: Vec<&str> = content.trim().split('\n').collect();
        // Header + 1 data row.
        assert_eq!(lines.len(), 2);
    }

    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn roundtrip_gzip() {
        use faucet_core::CompressionConfig;
        let tmp = NamedTempFile::with_suffix(".csv.gz").unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path).compression(CompressionConfig::Auto));

        let records = vec![
            json!({"id": "1", "name": "Alice"}),
            json!({"id": "2", "name": "Bob"}),
        ];
        sink.write_batch(&records).await.unwrap();
        sink.flush().await.unwrap();

        let bytes = tokio::fs::read(&path).await.unwrap();
        use std::io::Read;
        let mut r =
            faucet_core::compression::wrap_sync_reader(&bytes[..], faucet_core::Compression::Gzip);
        let mut text = String::new();
        r.read_to_string(&mut text).unwrap();
        let lines: Vec<&str> = text.trim().split('\n').collect();
        // Header + 2 rows.
        assert_eq!(lines.len(), 3);
    }

    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn roundtrip_zstd() {
        use faucet_core::CompressionConfig;
        let tmp = NamedTempFile::with_suffix(".csv.zst").unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path).compression(CompressionConfig::Auto));

        sink.write_batch(&[json!({"x": "42"})]).await.unwrap();
        sink.flush().await.unwrap();

        let bytes = tokio::fs::read(&path).await.unwrap();
        use std::io::Read;
        let mut r =
            faucet_core::compression::wrap_sync_reader(&bytes[..], faucet_core::Compression::Zstd);
        let mut text = String::new();
        r.read_to_string(&mut text).unwrap();
        let lines: Vec<&str> = text.trim().split('\n').collect();
        // Header + 1 row.
        assert_eq!(lines.len(), 2);
    }

    #[tokio::test]
    async fn write_flush_write_does_not_truncate() {
        // Regression: flush() clears the writer; the next write_batch
        // must reopen in append mode regardless of config.append (which
        // defaults to false). Without the opened_once guard, the second
        // open would truncate and lose the first batch's records.
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path));

        sink.write_batch(&[json!({"id": "1"})]).await.unwrap();
        sink.flush().await.unwrap();
        sink.write_batch(&[json!({"id": "2"})]).await.unwrap();
        sink.flush().await.unwrap();

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = content.trim().split('\n').collect();
        // Header + 2 data rows (header is written only on the first open).
        assert_eq!(
            lines.len(),
            3,
            "both batches must survive the mid-stream flush"
        );
    }

    #[tokio::test]
    async fn column_order_frozen_across_flush() {
        // #321 H2: flush() drops the writer state; the next write_batch reopens
        // in append mode. The reopened batch must reuse the header frozen at the
        // first open rather than re-deriving columns from its own keys —
        // otherwise a batch with a different key set writes rows misaligned with
        // the already-written header. Here page 2 has only `name`; before the fix
        // it re-derived columns=[name] and wrote "Bob" under the `id` column.
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path));

        sink.write_batch(&[json!({ "id": "1", "name": "Alice" })])
            .await
            .unwrap();
        sink.flush().await.unwrap();
        // Page 2: a subset of the frozen columns, in a different shape.
        sink.write_batch(&[json!({ "name": "Bob" })]).await.unwrap();
        sink.flush().await.unwrap();

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = content.trim().split('\n').collect();
        assert_eq!(lines.len(), 3, "header + 2 rows");
        assert_eq!(lines[0], "id,name", "header frozen from first open");
        assert_eq!(lines[1], "1,Alice");
        // Bob must land in the `name` column (id empty), not the first column.
        assert_eq!(
            lines[2], ",Bob",
            "reopened batch must align with the frozen header"
        );
    }

    #[tokio::test]
    async fn on_unknown_field_error_guard_holds_across_flush() {
        // #321 H2: the on_unknown_field guard compares against the frozen header,
        // not a per-batch re-derived set. A new field appearing in a post-flush
        // batch must still trip the guard.
        use crate::config::OnUnknownField;
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path).on_unknown_field(OnUnknownField::Error));

        sink.write_batch(&[json!({ "id": 1, "name": "Alice" })])
            .await
            .unwrap();
        sink.flush().await.unwrap();
        let err = sink
            .write_batch(&[json!({ "id": 2, "name": "Bob", "email": "b@x.y" })])
            .await
            .expect_err("a new field after flush must still abort under error mode");
        match err {
            FaucetError::Sink(msg) => assert!(msg.contains("email"), "must name the field: {msg}"),
            other => panic!("expected FaucetError::Sink, got {other:?}"),
        }
    }

    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn write_flush_write_produces_multi_member_gzip_csv() {
        // With compression, flush() finalises one gzip member; the
        // next write_batch starts a fresh member appended after it.
        // The decoder reads both members back correctly.
        use faucet_core::CompressionConfig;
        let tmp = NamedTempFile::with_suffix(".csv.gz").unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path).compression(CompressionConfig::Auto));
        sink.write_batch(&[json!({"id": "1"})]).await.unwrap();
        sink.flush().await.unwrap();
        sink.write_batch(&[json!({"id": "2"})]).await.unwrap();
        sink.flush().await.unwrap();

        let bytes = tokio::fs::read(&path).await.unwrap();
        use std::io::Read;
        let mut r =
            faucet_core::compression::wrap_sync_reader(&bytes[..], faucet_core::Compression::Gzip);
        let mut text = String::new();
        r.read_to_string(&mut text).unwrap();
        let lines: Vec<&str> = text.trim().split('\n').collect();
        // Header (from first open) + 2 data rows. The re-open uses
        // append=true so no second header is written.
        assert_eq!(lines.len(), 3);
    }
    #[tokio::test]
    async fn local_outputs_reports_a_file_faucet_created() {
        // Retention GC provenance (#587).
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.csv");
        let sink = CsvSink::new(CsvSinkConfig::new(path.to_str().unwrap()));
        assert!(sink.local_outputs().await.is_empty());

        sink.write_batch(&[json!({"id": "1"})]).await.unwrap();
        sink.flush().await.unwrap();

        let outs = sink.local_outputs().await;
        assert_eq!(outs.len(), 1);
        assert_eq!(outs[0].path, path);
        assert!(!outs[0].pre_existing);
    }

    #[tokio::test]
    async fn local_outputs_flags_a_file_faucet_did_not_create() {
        let tmp = NamedTempFile::with_suffix(".csv").unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path));
        sink.write_batch(&[json!({"id": "1"})]).await.unwrap();

        let outs = sink.local_outputs().await;
        assert_eq!(outs.len(), 1);
        assert!(outs[0].pre_existing);
    }

    #[tokio::test]
    async fn local_outputs_stays_one_entry_across_the_flush_reopen_cycle() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.csv");
        let sink = CsvSink::new(CsvSinkConfig::new(path.to_str().unwrap()));
        sink.write_batch(&[json!({"id": "1"})]).await.unwrap();
        sink.flush().await.unwrap();
        sink.write_batch(&[json!({"id": "2"})]).await.unwrap();
        sink.flush().await.unwrap();

        let outs = sink.local_outputs().await;
        assert_eq!(outs.len(), 1);
        assert!(!outs[0].pre_existing);
    }
    #[tokio::test]
    async fn local_outputs_marks_a_truncated_existing_file_replaced_and_an_appended_one_not() {
        // Truncating a file faucet did not create leaves only faucet's bytes in
        // it — safe to preview, still never collected. Appending keeps the
        // original owner's content, so it stays plain pre-existing.
        for (append, replaced) in [(false, true), (true, false)] {
            let tmp = NamedTempFile::with_suffix(".csv").unwrap();
            let sink =
                CsvSink::new(CsvSinkConfig::new(tmp.path().to_str().unwrap()).append(append));
            sink.write_batch(&[json!({"id": "1"})]).await.unwrap();
            let outs = sink.local_outputs().await;
            assert!(outs[0].pre_existing);
            assert_eq!(outs[0].replaced, replaced, "append = {append}");
        }
    }
}
