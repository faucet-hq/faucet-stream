//! Parquet sink executor.

use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use faucet_core::FaucetError;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, aws::AmazonS3Builder, local::LocalFileSystem};
use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::async_writer::{AsyncFileWriter, ParquetObjectWriter};
use parquet::file::properties::WriterProperties;
use serde_json::Value;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::config::{ParquetDestination, ParquetSinkConfig, SchemaSource};
use crate::schema::infer_schema;

/// A sink that writes JSON records as Apache Parquet files.
///
/// Lazily opens the first writer on the initial `write_batch` call so the
/// schema can be inferred from real records.
///
/// # Single-file vs. rollover finalization
///
/// A Parquet file is only readable once its footer is written, which happens
/// when the writer is *closed*. How that interacts with `flush()` differs by
/// mode:
///
/// * **Rollover / directory / S3 mode** (`max_rows_per_file` or
///   `max_bytes_per_file` set, or an S3 destination): each `flush()` closes the
///   in-flight writer (writing the footer) and the next page opens a fresh,
///   uniquely-named file. Callers that skip the final `flush()` leave the last
///   file's footer unwritten — and on S3 abort the multipart upload — so the
///   trailing file is unreadable.
/// * **Single-file mode** (a fixed `*.parquet` local path with no rollover):
///   the sink keeps **one** writer open for the whole run. The pipeline calls
///   `flush()` after every bookmark-carrying page, so closing there would
///   footer-write the file and the next page would reopen the same path and
///   *truncate* it — silently losing every page but the last (a critical
///   data-loss bug for any multi-bookmark source, e.g. all CDC pipelines).
///   Instead, an intermediate `flush()` only flushes buffered Arrow row groups
///   to the open writer (no footer, bounding memory) and the footer is written
///   exactly once when the sink is dropped at end of run.
pub struct ParquetSink {
    config: ParquetSinkConfig,
    store: Arc<dyn ObjectStore>,
    /// The directory portion of a `LocalPath` destination (created on `new`),
    /// or `None` for S3.
    local_root: Option<PathBuf>,
    /// Computed once at construction: a fixed `*.parquet` local path with no
    /// rollover thresholds. In this mode one writer stays open for the whole
    /// run and the footer is written on `Drop`, never on a per-page `flush()`.
    single_file: bool,
    state: Mutex<WriterState>,
    /// Every local file this sink opened, for the local-output retention GC
    /// (#587). Rollover means one run can create many UUID-named parts, and each
    /// is recorded individually — the GC deletes recorded files, never the
    /// directory holding them. Empty for an S3 destination (nothing local to
    /// collect). See `faucet_core::local_outputs`.
    outputs: faucet_core::LocalOutputLog,
}

/// Bookkeeping that mutates as we write.
struct WriterState {
    /// `None` until the first batch of the *current* file arrives. Re-inferred
    /// per file: cleared on every rollover (`close_current`) so a file that is
    /// opened *after* the schema widens (e.g. CDC following an
    /// `ALTER TABLE ADD COLUMN`) re-infers from its own first batch and writes
    /// the new columns. A Parquet file's schema is immutable once the first
    /// batch is written, so widening that appears *within* a single file cannot
    /// be accommodated — only at a file boundary (see `warned_fields`).
    schema: Option<SchemaRef>,
    /// `None` between rollovers and at construction.
    writer: Option<AsyncArrowWriter<Box<dyn AsyncFileWriter>>>,
    /// Rows accepted by the current writer.
    rows_in_current_file: usize,
    /// Total files closed successfully (for diagnostics).
    files_written: usize,
    /// Fields already warned-about as dropped from the *current* file (present
    /// in a record but absent from this file's locked schema). Cleared on every
    /// rollover so a new file re-warns if it still drops fields. Persisting this
    /// across chunks (rather than resetting per `encode_batch`) keeps the
    /// warning to one line per dropped field per file instead of one per page.
    warned_fields: std::collections::HashSet<String>,
}

impl WriterState {
    fn new() -> Self {
        Self {
            schema: None,
            writer: None,
            rows_in_current_file: 0,
            files_written: 0,
            warned_fields: std::collections::HashSet::new(),
        }
    }
}

impl ParquetSink {
    /// Create a new Parquet sink. The underlying object store is built eagerly
    /// so that bad configuration fails fast rather than on first write.
    pub async fn new(config: ParquetSinkConfig) -> Result<Self, FaucetError> {
        config
            .validate()
            .map_err(|e| FaucetError::Config(format!("invalid parquet sink config: {e}")))?;

        // A fixed `foo.parquet` local path combined with a rollover threshold
        // is contradictory: rollover needs multiple files, so the sink falls
        // back to UUID-named files in the *parent* directory and the fixed
        // filename is silently ignored. Warn so the surprise is visible
        // (#78 LOW).
        if let ParquetDestination::LocalPath { path } = &config.destination
            && FsPath::new(path).extension().and_then(|s| s.to_str()) == Some("parquet")
            && (config.max_rows_per_file.is_some() || config.max_bytes_per_file.is_some())
        {
            tracing::warn!(
                path = %path,
                "parquet sink: a fixed '.parquet' path with max_rows_per_file / \
                 max_bytes_per_file set cannot be honoured — rollover writes UUID-named \
                 files into the parent directory and the fixed filename is ignored. Use a \
                 directory destination, or drop the rollover thresholds for a single file."
            );
        }

        let (store, local_root) = build_store(&config.destination).await?;
        let single_file = compute_single_file_mode(&config);

        Ok(Self {
            config,
            store,
            local_root,
            single_file,
            state: Mutex::new(WriterState::new()),
            outputs: faucet_core::LocalOutputLog::new(),
        })
    }

    /// Whether the configured destination writes one file per "key" prefix +
    /// uuid (S3 or directory-style local) or to a single fixed file.
    fn single_file_mode(&self) -> bool {
        self.single_file
    }

    /// Build the object_store `Path` for the next file. Each new file gets a
    /// unique UUID-suffixed name unless we're in single-file mode.
    ///
    /// Uses `Path::from_absolute_path` (not `from_filesystem_path`) because
    /// the target file does not exist yet — canonicalize would fail.
    /// Also returns the concrete filesystem path for a local destination (the
    /// `object_store` `Path` is percent-encoded and root-relative, so it is not
    /// a usable filesystem path), which `open_writer` records for the retention
    /// GC. `None` for S3 — there is no local file to collect.
    fn next_object_path(&self) -> Result<(ObjPath, Option<PathBuf>), FaucetError> {
        match &self.config.destination {
            ParquetDestination::LocalPath { path } => {
                let pb = if self.single_file_mode() {
                    PathBuf::from(path)
                } else {
                    let root = self
                        .local_root
                        .as_ref()
                        .expect("local_root set on local dest");
                    root.join(format!("{}.parquet", Uuid::new_v4()))
                };
                let absolute = if pb.is_absolute() {
                    pb.clone()
                } else {
                    std::env::current_dir()
                        .map_err(|e| FaucetError::Sink(format!("could not read cwd: {e}")))?
                        .join(&pb)
                };
                let obj = ObjPath::from_absolute_path(&absolute).map_err(|e| {
                    FaucetError::Sink(format!(
                        "could not encode local path {}: {e}",
                        absolute.display()
                    ))
                })?;
                Ok((obj, Some(absolute)))
            }
            ParquetDestination::S3(s3) => {
                let key = format!("{}{}.parquet", s3.prefix, Uuid::new_v4());
                let obj = ObjPath::parse(&key).map_err(|e| {
                    FaucetError::Sink(format!("invalid s3 prefix/key '{key}': {e}"))
                })?;
                Ok((obj, None))
            }
        }
    }

    fn writer_properties(&self) -> WriterProperties {
        WriterProperties::builder()
            .set_compression(self.config.compression.as_parquet())
            .set_max_row_group_row_count(Some(self.config.row_group_size))
            .build()
    }

    async fn open_writer(
        &self,
        schema: SchemaRef,
    ) -> Result<AsyncArrowWriter<Box<dyn AsyncFileWriter>>, FaucetError> {
        let (obj_path, local_path) = self.next_object_path()?;
        // Provenance for the retention GC (#587), recorded before the writer
        // creates the file so a destination that already held a file of this name
        // is flagged `pre_existing` and never collected. In rollover mode each
        // part lands here as its own entry.
        if let Some(local) = &local_path {
            self.outputs.record_open_probing(local.clone());
        }
        let writer = ParquetObjectWriter::new(self.store.clone(), obj_path);
        let boxed: Box<dyn AsyncFileWriter> = Box::new(writer);
        AsyncArrowWriter::try_new(boxed, schema, Some(self.writer_properties()))
            .map_err(|e| FaucetError::Sink(format!("could not open parquet writer: {e}")))
    }

    /// Build a `RecordBatch` for `records` against the locked-in schema.
    ///
    /// Unknown fields (present in records but not in schema) are silently
    /// dropped by `arrow_json::Decoder` because we leave `strict_mode` at its
    /// default `false`; we still emit a `tracing::warn!` for visibility. The
    /// schema is re-inferred per file (see `WriterState::schema`), so a field
    /// added between files is *written* in the later file; a field added
    /// *within* a single file genuinely cannot be accommodated (a Parquet
    /// file's schema is immutable mid-stream) and is the case this warning
    /// makes visible. `warned_fields` dedupes the warning to one line per
    /// dropped field per file.
    ///
    /// Type drift (a field whose JSON type disagrees with the schema's
    /// `DataType`) is surfaced as a `FaucetError::Sink` naming the field and
    /// both sides of the mismatch.
    fn encode_batch(
        &self,
        warned_fields: &mut std::collections::HashSet<String>,
        schema: SchemaRef,
        records: &[Value],
    ) -> Result<RecordBatch, FaucetError> {
        warn_on_unknown_fields(warned_fields, &schema, records);

        let mut decoder = arrow_json::ReaderBuilder::new(schema.clone())
            .build_decoder()
            .map_err(|e| FaucetError::Sink(format!("could not build json decoder: {e}")))?;

        decoder
            .serialize(records)
            .map_err(|e| classify_decoder_error(&schema, records, e))?;

        let batch = decoder
            .flush()
            .map_err(|e| classify_decoder_error(&schema, records, e))?
            .ok_or_else(|| {
                FaucetError::Sink("json decoder produced no record batch".to_string())
            })?;
        Ok(batch)
    }

    /// Encode + write a single chunk of records, applying row/byte rollover
    /// after the write. Skips entirely on an empty chunk so callers do not
    /// have to guard.
    ///
    /// Splitting this out of `write_batch` lets the public entry point chunk
    /// large pages by `config.batch_size` while keeping the schema /
    /// writer-init / rollover invariants in one place.
    async fn write_chunk(
        &self,
        state: &mut WriterState,
        records: &[Value],
    ) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        if state.schema.is_none() {
            let schema = match &self.config.schema {
                Some(SchemaSource::Explicit {}) => {
                    return Err(FaucetError::Config(
                        "explicit parquet schemas are not supported yet; use inferred".to_string(),
                    ));
                }
                _ => infer_schema(records, self.config.effective_sample_size())?,
            };
            state.schema = Some(schema);
        }
        let schema = state.schema.clone().expect("schema set above");
        let batch = self.encode_batch(&mut state.warned_fields, schema, records)?;
        self.write_record_batch(state, &batch).await
    }

    /// Write an already-built Arrow `RecordBatch` to the current file, opening
    /// the writer lazily on first use and applying row/byte rollover after the
    /// write.
    ///
    /// This is the shared writer tail behind both the JSON path
    /// ([`write_chunk`](Self::write_chunk), which builds the batch from
    /// `serde_json::Value` records via [`encode_batch`](Self::encode_batch))
    /// and the columnar fast path (`write_batch_columnar`, which feeds an
    /// Arrow batch straight in). When `state.schema` is not yet locked in it
    /// is taken from the batch's own schema — the JSON path always sets it
    /// first (from `infer_schema`), so this only fires for the columnar path.
    async fn write_record_batch(
        &self,
        state: &mut WriterState,
        batch: &RecordBatch,
    ) -> Result<usize, FaucetError> {
        if state.schema.is_none() {
            state.schema = Some(batch.schema());
        }
        let schema = state.schema.clone().expect("schema set above");

        if state.writer.is_none() {
            state.writer = Some(self.open_writer(schema).await?);
        }

        let batch_rows = batch.num_rows();

        let estimated_size = {
            let writer = state.writer.as_mut().expect("writer set above");
            writer
                .write(batch)
                .await
                .map_err(|e| FaucetError::Sink(format!("parquet write failed: {e}")))?;
            // `bytes_written` only counts data already flushed to the
            // underlying sink; `in_progress_size` counts what is still
            // buffered in the active row group. We must sum both to know when
            // a file has reached the user's byte cap, otherwise large
            // partially-buffered row groups can hide indefinitely.
            writer.bytes_written() + writer.in_progress_size()
        };
        state.rows_in_current_file += batch_rows;

        if should_rollover(&self.config, state, estimated_size) {
            tracing::debug!(
                rows = state.rows_in_current_file,
                bytes = estimated_size,
                "Rolling over parquet file"
            );
            self.close_current(state).await?;
        }

        Ok(batch_rows)
    }

    /// Close the current writer (writing the parquet footer) and reset the
    /// per-file state so the *next* file re-infers its own schema.
    ///
    /// Clearing `state.schema` here is the fix for F34 (silent column-level
    /// data loss): the schema must not be locked-in from the very first batch
    /// of the run and reused for every subsequent file. Re-inferring per file
    /// means a file opened *after* the record shape widens (CDC after an
    /// `ALTER TABLE ADD COLUMN`, or a non-homogeneous first page) picks up the
    /// new columns at the next file boundary instead of dropping them forever.
    /// This is safe because a Parquet file's schema is immutable once its first
    /// batch is written, so re-inference only ever happens between files, never
    /// mid-file. `warned_fields` is reset too so a new file re-warns about any
    /// fields it still drops.
    async fn close_current(&self, state: &mut WriterState) -> Result<(), FaucetError> {
        if let Some(writer) = state.writer.take() {
            writer
                .close()
                .await
                .map_err(|e| FaucetError::Sink(format!("could not close parquet writer: {e}")))?;
            state.files_written += 1;
            state.rows_in_current_file = 0;
            state.schema = None;
            state.warned_fields.clear();
        }
        Ok(())
    }

    /// Single-file intermediate flush: push buffered Arrow row groups to the
    /// open writer **without** writing the footer, so the one writer stays open
    /// across pages. Bounds memory between bookmark-carrying pages without
    /// truncating the file. No-op until the first write has opened a writer.
    async fn flush_open_writer(&self, state: &mut WriterState) -> Result<(), FaucetError> {
        if let Some(writer) = state.writer.as_mut() {
            writer
                .flush()
                .await
                .map_err(|e| FaucetError::Sink(format!("could not flush parquet writer: {e}")))?;
        }
        Ok(())
    }
}

impl Drop for ParquetSink {
    /// Finalize a still-open single-file writer by writing its footer exactly
    /// once at end of run. This is the *only* place the single-file footer is
    /// written — per-page `flush()` deliberately leaves the writer open (see
    /// the type-level docs) so the file is never reopened/truncated mid-stream.
    ///
    /// Rollover / directory / S3 modes close their writer on every `flush()`,
    /// so by the time the sink is dropped `state.writer` is already `None` and
    /// this is a no-op for them.
    fn drop(&mut self) {
        // Only single-file mode can leave a writer open past the final flush.
        if !self.single_file {
            return;
        }
        // Take the writer out under the lock without awaiting (the lock is
        // uncontended at drop — no other handle to `self` exists).
        let Some(writer) = self.state.get_mut().writer.take() else {
            return;
        };

        // Closing is async (the object_store local writer offloads its final
        // write to a blocking task), so we need a Tokio runtime context. The
        // pipeline runs the sink on a multi-thread runtime, where
        // `block_in_place` lets us drive the close to completion on the current
        // thread. If we are not inside a runtime (or on a single-threaded one
        // where `block_in_place` would panic), fall back to a transient
        // runtime so the footer is still written rather than lost.
        let close = async move {
            writer
                .close()
                .await
                .map(|_meta| ())
                .map_err(|e| FaucetError::Sink(format!("could not close parquet writer: {e}")))
        };

        let result = match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                match handle.runtime_flavor() {
                    tokio::runtime::RuntimeFlavor::MultiThread => {
                        // Safe: a multi-thread runtime tolerates a blocking
                        // section on a worker thread.
                        tokio::task::block_in_place(|| handle.block_on(close))
                    }
                    // current-thread (or any non-multi-thread) runtime:
                    // `block_in_place` would panic, and we cannot re-enter the
                    // current runtime with `block_on`. Drive the close on a
                    // dedicated thread with its own minimal runtime.
                    _ => close_on_dedicated_thread(close),
                }
            }
            // Dropped outside any runtime: spin up a transient one.
            Err(_) => close_on_dedicated_thread(close),
        };

        if let Err(e) = result {
            tracing::error!(
                error = %e,
                "parquet sink: failed to finalize single-file output on drop; the file may be unreadable"
            );
        }
    }
}

/// Drive a future to completion on a freshly-spawned OS thread with its own
/// single-threaded Tokio runtime. Used by `Drop` when the current context
/// cannot host a blocking close (no runtime, or a current-thread runtime that
/// `block_in_place` cannot enter).
fn close_on_dedicated_thread<F>(fut: F) -> Result<(), FaucetError>
where
    F: std::future::Future<Output = Result<(), FaucetError>> + Send + 'static,
{
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| {
                        FaucetError::Sink(format!(
                            "could not build runtime to finalize parquet file: {e}"
                        ))
                    })?;
                rt.block_on(fut)
            })
            .join()
            .unwrap_or_else(|_| {
                Err(FaucetError::Sink(
                    "parquet finalize thread panicked".to_string(),
                ))
            })
    })
}

#[async_trait]
impl faucet_core::Sink for ParquetSink {
    fn connector_name(&self) -> &'static str {
        "parquet"
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(ParquetSinkConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        use crate::config::ParquetDestination;
        match &self.config.destination {
            ParquetDestination::LocalPath { path } => format!("file://{path}"),
            ParquetDestination::S3(s3) => format!("s3://{}/{}", s3.bucket, s3.prefix),
        }
    }

    async fn local_outputs(&self) -> Vec<faucet_core::LocalOutput> {
        self.outputs.snapshot()
    }

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        let mut state = self.state.lock().await;

        // Re-chunk the incoming page when the config asks for it. `batch_size
        // = 0` is the "no batching" sentinel: pass the page straight through.
        // Otherwise we slice into `batch_size`-sized windows and run the
        // existing write path once per chunk. Row/byte rollover logic is
        // applied per chunk and remains independent of `batch_size`.
        let chunk_size = self.config.batch_size;
        let mut total_rows = 0;
        if chunk_size == 0 || records.len() <= chunk_size {
            total_rows += self.write_chunk(&mut state, records).await?;
        } else {
            for chunk in records.chunks(chunk_size) {
                total_rows += self.write_chunk(&mut state, chunk).await?;
            }
        }

        Ok(total_rows)
    }

    /// The Parquet sink writes Arrow `RecordBatch`es natively, so it opts into
    /// the columnar fast path (RFC 0002 / #375): a `parquet -> parquet` chain
    /// can hand batches straight through without materializing
    /// `serde_json::Value` rows.
    #[cfg(feature = "arrow")]
    fn supports_columnar(&self) -> bool {
        true
    }

    /// Write an Arrow `RecordBatch` directly, bypassing the
    /// `Value` → `RecordBatch` conversion `write_batch` performs. Routes
    /// through the same `write_record_batch` tail (the private writer path)
    /// so schema inference (from the batch's own schema on first use), lazy
    /// writer open, and row/byte rollover all behave identically to the JSON
    /// path. Returns the number of rows written.
    #[cfg(feature = "arrow")]
    async fn write_batch_columnar(
        &self,
        batch: &arrow::record_batch::RecordBatch,
    ) -> Result<usize, FaucetError> {
        if batch.num_rows() == 0 {
            return Ok(0);
        }
        let mut state = self.state.lock().await;
        self.write_record_batch(&mut state, batch).await
    }

    /// Make buffered output durable.
    ///
    /// In **single-file mode** this only flushes buffered Arrow row groups to
    /// the open writer (no footer) so the file is never reopened/truncated
    /// between the per-page flushes the pipeline issues; the footer is written
    /// once on `Drop` at end of run. In **rollover / directory / S3 mode** this
    /// closes the in-flight writer (writing the footer / completing the S3
    /// multipart) — the next page opens a fresh file. Files left without a
    /// final `flush()`/drop in those modes are unreadable.
    async fn flush(&self) -> Result<(), FaucetError> {
        let mut state = self.state.lock().await;
        if self.single_file {
            self.flush_open_writer(&mut state).await?;
            tracing::debug!("Parquet single-file sink flushed (writer kept open)");
        } else {
            self.close_current(&mut state).await?;
            tracing::debug!(files = state.files_written, "Parquet sink flushed");
        }
        Ok(())
    }

    /// Preflight probe for `faucet doctor`.
    ///
    /// For a local destination, verifies the target's parent directory exists
    /// and is writable by creating and immediately removing a uniquely-named
    /// temp file there — never touching the user's actual output. For an S3
    /// destination the probe is skipped: object-store targets are not probed by
    /// the doctor.
    async fn check(
        &self,
        _ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};

        let path = match &self.config.destination {
            ParquetDestination::S3(_) => {
                return Ok(CheckReport::single(Probe::skip(
                    "io",
                    "object-store target not probed by doctor",
                )));
            }
            ParquetDestination::LocalPath { path } => path.clone(),
        };

        // Guard against object-store URLs that slipped into a LocalPath
        // destination (e.g. `s3://` / `gs://`): treat them as object-store
        // targets and skip rather than mis-probing the local filesystem.
        let lower = path.to_ascii_lowercase();
        if lower.starts_with("s3://") || lower.starts_with("gs://") {
            return Ok(CheckReport::single(Probe::skip(
                "io",
                "object-store target not probed by doctor",
            )));
        }

        let start = std::time::Instant::now();

        // Mirror `build_store`'s parent-directory derivation: a `.parquet`
        // path is a single file whose parent is the target directory; any
        // other path is itself the (directory) destination.
        let target = FsPath::new(&path);
        let parent = if target.extension().and_then(|e| e.to_str()) == Some("parquet") {
            target.parent().map(|p| p.to_path_buf())
        } else {
            Some(PathBuf::from(&path))
        }
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| PathBuf::from("."));

        if !tokio::fs::try_exists(&parent).await.unwrap_or(false) {
            return Ok(CheckReport::single(Probe::fail_hint(
                "io",
                start.elapsed(),
                format!("parent directory {} does not exist", parent.display()),
                format!(
                    "create the directory {} before running the pipeline",
                    parent.display()
                ),
            )));
        }

        let probe_path = parent.join(format!(".faucet_doctor_probe-{}", Uuid::new_v4()));
        let probe = match tokio::fs::write(&probe_path, b"").await {
            Ok(()) => {
                let _ = tokio::fs::remove_file(&probe_path).await;
                Probe::pass("io", start.elapsed())
            }
            Err(e) => Probe::fail_hint(
                "io",
                start.elapsed(),
                format!("cannot write to directory {}: {e}", parent.display()),
                "ensure the directory is writable by the current user",
            ),
        };
        Ok(CheckReport::single(probe))
    }
}

/// Decide whether to start a new file before the current chunk.
///
/// **`max_bytes_per_file` is approximate.** `bytes_written` is an estimate of
/// the *in-memory Arrow* size, checked after a chunk is appended; the actual
/// on-disk Parquet file is typically much smaller (and varies) once column
/// encoding + compression (`compression = Zstd/Snappy/…`) are applied, and
/// rollover happens at chunk granularity rather than mid-chunk. Treat the
/// threshold as a soft target, not a hard byte cap (#78 LOW).
fn should_rollover(cfg: &ParquetSinkConfig, state: &WriterState, bytes_written: usize) -> bool {
    if let Some(max_rows) = cfg.max_rows_per_file
        && state.rows_in_current_file >= max_rows
    {
        return true;
    }
    if let Some(max_bytes) = cfg.max_bytes_per_file
        && bytes_written >= max_bytes
    {
        return true;
    }
    false
}

/// Warn (once per field per file) about fields present in `records` but absent
/// from the current file's locked `schema` — these are silently dropped by the
/// `arrow_json` decoder (`strict_mode = false`).
///
/// `already_warned` is the per-file dedupe set carried in `WriterState`; it is
/// cleared on rollover so a new file re-warns. This makes the *unavoidable*
/// within-a-single-file widening (a field that appears after this file's schema
/// is locked, which cannot be added mid-file) visible rather than silent.
fn warn_on_unknown_fields(
    already_warned: &mut std::collections::HashSet<String>,
    schema: &SchemaRef,
    records: &[Value],
) {
    if records.is_empty() {
        return;
    }
    let known: std::collections::HashSet<&str> =
        schema.fields().iter().map(|f| f.name().as_str()).collect();
    for r in records {
        if let Value::Object(map) = r {
            for k in map.keys() {
                if !known.contains(k.as_str()) && already_warned.insert(k.clone()) {
                    tracing::warn!(
                        field = %k,
                        "parquet sink: dropping field absent from this file's schema; \
                         it was added after the file's first batch and a Parquet file's \
                         schema is immutable mid-file. It will be captured in the next \
                         file on rollover, or in a fresh run."
                    );
                }
            }
        }
    }
}

/// Whether this decoder error can be a JSON-value ↔ Arrow-schema type
/// conflict. Classified from the error's *variant*, never its rendered text: a
/// substring probe like `msg.contains("type")` matches nearly every arrow
/// message, so an unrelated I/O or memory failure used to be reported as type
/// drift (#654 L28).
fn is_type_conflict(err: &arrow::error::ArrowError) -> bool {
    use arrow::error::ArrowError as E;
    matches!(
        err,
        // `JsonError` is what the arrow-json decoder raises for a value that
        // does not fit the locked field type ("whilst decoding field 'a':
        // expected string got 1"); the other three cover the same conflict
        // reported by the schema/cast/parse layers underneath it.
        E::JsonError(_) | E::SchemaError(_) | E::CastError(_) | E::ParseError(_)
    )
}

/// Map a decoder error to `FaucetError::Sink`. For a type-conflict variant we
/// re-shape it to name both sides of the drift so the user can diagnose it
/// without reading arrow internals; anything else keeps the raw message.
fn classify_decoder_error(
    schema: &SchemaRef,
    records: &[Value],
    err: arrow::error::ArrowError,
) -> FaucetError {
    let msg = err.to_string();
    if is_type_conflict(&err)
        && let Some(field) = guess_drifting_field(schema, records)
    {
        let schema_type = schema
            .field_with_name(&field)
            .map(|f| f.data_type().to_string())
            .unwrap_or_else(|_| "<unknown>".to_string());
        let record_type = sample_field_type(records, &field).unwrap_or("<absent>".to_string());
        return FaucetError::Sink(format!(
            "parquet type drift for field '{field}': schema declares {schema_type}, batch contains {record_type} (raw: {msg})"
        ));
    }
    FaucetError::Sink(format!("parquet encode failed: {msg}"))
}

fn guess_drifting_field(schema: &SchemaRef, records: &[Value]) -> Option<String> {
    for r in records {
        if let Value::Object(map) = r {
            for (k, v) in map {
                let Ok(field) = schema.field_with_name(k) else {
                    continue;
                };
                if !matches_data_type(field.data_type(), v) && !matches!(v, Value::Null) {
                    return Some(k.clone());
                }
            }
        }
    }
    None
}

fn matches_data_type(dt: &arrow::datatypes::DataType, value: &Value) -> bool {
    use arrow::datatypes::DataType as DT;
    match (dt, value) {
        (_, Value::Null) => true,
        (DT::Boolean, Value::Bool(_)) => true,
        (DT::Int64 | DT::Int32 | DT::Int16 | DT::Int8, Value::Number(n)) => n.is_i64(),
        (DT::UInt64 | DT::UInt32 | DT::UInt16 | DT::UInt8, Value::Number(n)) => n.is_u64(),
        (DT::Float64 | DT::Float32, Value::Number(_)) => true,
        (DT::Utf8 | DT::LargeUtf8, Value::String(_)) => true,
        (DT::List(_) | DT::LargeList(_), Value::Array(_)) => true,
        (DT::Struct(_), Value::Object(_)) => true,
        _ => false,
    }
}

fn sample_field_type(records: &[Value], field: &str) -> Option<String> {
    for r in records {
        if let Value::Object(map) = r
            && let Some(v) = map.get(field)
            && !v.is_null()
        {
            return Some(json_value_type_name(v).to_string());
        }
    }
    None
}

fn json_value_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(n) if n.is_i64() => "integer",
        Value::Number(n) if n.is_u64() => "unsigned integer",
        Value::Number(_) => "float",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Determine single-file mode from the config: a fixed `*.parquet` local path
/// with neither rollover threshold set. Pure helper so the value can be
/// computed once at construction and reused without `&self`.
fn compute_single_file_mode(config: &ParquetSinkConfig) -> bool {
    match &config.destination {
        ParquetDestination::LocalPath { path } => {
            let p = FsPath::new(path);
            p.extension().and_then(|s| s.to_str()) == Some("parquet")
                && config.max_rows_per_file.is_none()
                && config.max_bytes_per_file.is_none()
        }
        ParquetDestination::S3(_) => false,
    }
}

async fn build_store(
    destination: &ParquetDestination,
) -> Result<(Arc<dyn ObjectStore>, Option<PathBuf>), FaucetError> {
    match destination {
        ParquetDestination::LocalPath { path } => {
            let target = FsPath::new(path);
            let parent = if target.extension().and_then(|e| e.to_str()) == Some("parquet") {
                target.parent().map(|p| p.to_path_buf())
            } else {
                Some(target.to_path_buf())
            }
            .unwrap_or_else(|| PathBuf::from("."));

            tokio::fs::create_dir_all(&parent).await.map_err(|e| {
                FaucetError::Sink(format!(
                    "could not create directory {}: {e}",
                    parent.display()
                ))
            })?;

            let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
            Ok((store, Some(parent)))
        }
        ParquetDestination::S3(s3) => {
            let mut builder = AmazonS3Builder::from_env().with_bucket_name(&s3.bucket);
            if let Some(region) = &s3.region {
                builder = builder.with_region(region);
            }
            if let Some(endpoint) = &s3.endpoint_url {
                builder = builder.with_endpoint(endpoint);
            }
            if s3.allow_http {
                builder = builder.with_allow_http(true);
            }
            let store = builder
                .build()
                .map_err(|e| FaucetError::Config(format!("could not build S3 client: {e}")))?;
            Ok((Arc::new(store), None))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ParquetCompression;
    use faucet_core::Sink;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use serde_json::json;

    fn cfg(path: &std::path::Path) -> ParquetSinkConfig {
        ParquetSinkConfig::local(path.to_string_lossy().to_string())
    }

    /// Read every row group back from a Parquet file and collect the `id`
    /// column (Int64) into a Vec. Panics on any read error — tests want the
    /// loud failure.
    fn read_ids(path: &std::path::Path) -> Vec<i64> {
        let file = std::fs::File::open(path).expect("open parquet file");
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .expect("parquet reader builder")
            .build()
            .expect("parquet reader");
        let mut ids = Vec::new();
        for batch in reader {
            let batch = batch.expect("record batch");
            let col = batch
                .column(batch.schema().index_of("id").expect("id column"))
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("id is Int64");
            for i in 0..col.len() {
                ids.push(col.value(i));
            }
        }
        ids
    }

    #[tokio::test]
    async fn dataset_uri_local_path() {
        let tmp = tempfile::tempdir().unwrap();
        let sink = ParquetSink::new(cfg(tmp.path())).await.unwrap();
        let uri = sink.dataset_uri();
        assert!(
            uri.starts_with("file://"),
            "expected file:// URI, got: {uri}"
        );
        assert!(
            uri.contains(tmp.path().to_str().unwrap()),
            "URI should contain the path"
        );
    }

    #[tokio::test]
    async fn new_validates_config() {
        let cfg = ParquetSinkConfig::local("");
        match ParquetSink::new(cfg).await {
            Ok(_) => panic!("expected Config error"),
            Err(err) => assert!(matches!(err, FaucetError::Config(_))),
        }
    }

    #[tokio::test]
    async fn empty_batch_is_noop() {
        let tmp = tempfile::tempdir().unwrap();
        let sink = ParquetSink::new(cfg(tmp.path())).await.unwrap();
        let count = sink.write_batch(&[]).await.unwrap();
        assert_eq!(count, 0);
        sink.flush().await.unwrap();
    }

    #[tokio::test]
    async fn flush_without_write_is_noop() {
        let tmp = tempfile::tempdir().unwrap();
        let sink = ParquetSink::new(cfg(tmp.path())).await.unwrap();
        assert!(sink.flush().await.is_ok());
    }

    #[tokio::test]
    async fn type_drift_returns_sink_error() {
        let tmp = tempfile::tempdir().unwrap();
        let sink = ParquetSink::new(cfg(tmp.path())).await.unwrap();
        sink.write_batch(&[json!({"x": 1})]).await.unwrap();
        let err = sink
            .write_batch(&[json!({"x": "not an int"})])
            .await
            .unwrap_err();
        match err {
            FaucetError::Sink(msg) => {
                assert!(
                    msg.contains("'x'") || msg.contains("x"),
                    "error must name the drifting field: {msg}"
                );
            }
            other => panic!("expected Sink error, got {other:?}"),
        }
        sink.flush().await.unwrap();
    }

    #[tokio::test]
    async fn unknown_fields_are_silently_dropped() {
        let tmp = tempfile::tempdir().unwrap();
        let sink = ParquetSink::new(cfg(tmp.path())).await.unwrap();
        sink.write_batch(&[json!({"id": 1})]).await.unwrap();
        let count = sink
            .write_batch(&[json!({"id": 2, "ghost": "value"})])
            .await
            .unwrap();
        assert_eq!(count, 1);
        sink.flush().await.unwrap();
    }

    #[tokio::test]
    async fn writer_properties_apply_compression() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = cfg(tmp.path()).compression(ParquetCompression::Zstd);
        let sink = ParquetSink::new(cfg).await.unwrap();
        let props = sink.writer_properties();
        assert!(matches!(
            props.compression(&parquet::schema::types::ColumnPath::new(vec![
                "any".to_string(),
            ])),
            parquet::basic::Compression::ZSTD(_)
        ));
    }

    #[test]
    fn should_rollover_rows_threshold() {
        let cfg = ParquetSinkConfig::local("/tmp/x").max_rows_per_file(10);
        let mut state = WriterState::new();
        state.rows_in_current_file = 9;
        assert!(!should_rollover(&cfg, &state, 0));
        state.rows_in_current_file = 10;
        assert!(should_rollover(&cfg, &state, 0));
        state.rows_in_current_file = 11;
        assert!(should_rollover(&cfg, &state, 0));
    }

    #[test]
    fn should_rollover_bytes_threshold() {
        let cfg = ParquetSinkConfig::local("/tmp/x").max_bytes_per_file(1024);
        let state = WriterState::new();
        assert!(!should_rollover(&cfg, &state, 1023));
        assert!(should_rollover(&cfg, &state, 1024));
        assert!(should_rollover(&cfg, &state, 4096));
    }

    #[test]
    fn should_rollover_no_thresholds_means_never() {
        let cfg = ParquetSinkConfig::local("/tmp/x");
        let mut state = WriterState::new();
        state.rows_in_current_file = 1_000_000;
        assert!(!should_rollover(&cfg, &state, usize::MAX / 2));
    }

    #[test]
    fn matches_data_type_for_primitives() {
        use arrow::datatypes::DataType as DT;
        assert!(matches_data_type(&DT::Boolean, &json!(true)));
        assert!(matches_data_type(&DT::Int64, &json!(1)));
        assert!(!matches_data_type(&DT::Int64, &json!(1.5)));
        assert!(matches_data_type(&DT::Float64, &json!(1.5)));
        assert!(matches_data_type(&DT::Float64, &json!(1)));
        assert!(matches_data_type(&DT::Utf8, &json!("hi")));
        assert!(!matches_data_type(&DT::Utf8, &json!(1)));
        assert!(matches_data_type(&DT::Boolean, &Value::Null));
    }

    #[test]
    fn json_value_type_name_covers_variants() {
        assert_eq!(json_value_type_name(&json!(null)), "null");
        assert_eq!(json_value_type_name(&json!(true)), "boolean");
        assert_eq!(json_value_type_name(&json!(1)), "integer");
        assert_eq!(json_value_type_name(&json!(1.5)), "float");
        assert_eq!(json_value_type_name(&json!("s")), "string");
        assert_eq!(json_value_type_name(&json!([1])), "array");
        assert_eq!(json_value_type_name(&json!({"a": 1})), "object");
    }

    #[test]
    fn single_file_mode_only_for_fixed_parquet_path_without_rollover() {
        // Fixed *.parquet path, no rollover → single-file.
        assert!(compute_single_file_mode(&ParquetSinkConfig::local(
            "/tmp/out.parquet"
        )));
        // Rollover thresholds disable single-file mode.
        assert!(!compute_single_file_mode(
            &ParquetSinkConfig::local("/tmp/out.parquet").max_rows_per_file(10)
        ));
        assert!(!compute_single_file_mode(
            &ParquetSinkConfig::local("/tmp/out.parquet").max_bytes_per_file(1024)
        ));
        // A directory path is never single-file.
        assert!(!compute_single_file_mode(&ParquetSinkConfig::local(
            "/tmp/outdir"
        )));
    }

    /// Regression test for F2 (audit #264): single-file mode must accumulate
    /// ALL pages across the per-page `flush()` calls the pipeline issues — the
    /// file must not be reopened/truncated mid-stream, so only the final page
    /// would survive. Runs on a multi-thread runtime so the production Drop
    /// finalize path (`block_in_place`) is exercised.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn single_file_accumulates_all_pages_across_flushes() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("out.parquet");
        let cfg = ParquetSinkConfig::local(path.to_string_lossy().to_string());
        assert!(
            compute_single_file_mode(&cfg),
            "test setup must be single-file"
        );

        {
            let sink = ParquetSink::new(cfg).await.unwrap();

            // Page 1 + bookmark flush.
            sink.write_batch(&[json!({"id": 1}), json!({"id": 2})])
                .await
                .unwrap();
            sink.flush().await.unwrap();

            // Page 2 + bookmark flush — under the bug this would truncate the
            // file written by page 1.
            sink.write_batch(&[json!({"id": 3})]).await.unwrap();
            sink.flush().await.unwrap();

            // Page 3 + final flush (pipeline calls flush once more at end).
            sink.write_batch(&[json!({"id": 4}), json!({"id": 5})])
                .await
                .unwrap();
            sink.flush().await.unwrap();

            // Drop here writes the footer exactly once.
        }

        let mut ids = read_ids(&path);
        ids.sort_unstable();
        assert_eq!(
            ids,
            vec![1, 2, 3, 4, 5],
            "all rows from every page must be present in the single file"
        );
    }

    /// Even without any intermediate `flush()`, a single-file sink must produce
    /// a readable file once dropped (footer written on Drop).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn single_file_finalizes_on_drop_without_explicit_flush() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("out.parquet");
        {
            let sink = ParquetSink::new(cfg(&path)).await.unwrap();
            sink.write_batch(&[json!({"id": 10}), json!({"id": 11})])
                .await
                .unwrap();
            // No flush() — rely on Drop to write the footer.
        }
        let ids = read_ids(&path);
        assert_eq!(ids, vec![10, 11]);
    }

    /// Columnar fast path (feature `arrow`): a `RecordBatch` written via
    /// `write_batch_columnar` produces the same readable single-file Parquet as
    /// the `Value` path — it goes through the shared `write_record_batch` tail,
    /// so schema inference (from the batch), lazy writer open, and Drop-finalize
    /// all behave identically. Proves the parquet sink's half of the
    /// `parquet -> parquet` columnar chain (RFC 0002 / #375).
    #[cfg(feature = "arrow")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn write_batch_columnar_round_trips() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("out.parquet");
        let rows = vec![json!({"id": 1}), json!({"id": 2}), json!({"id": 3})];
        let batch = faucet_core::columnar::values_to_record_batch_inferred(&rows).unwrap();
        {
            let sink = ParquetSink::new(cfg(&path)).await.unwrap();
            assert!(sink.supports_columnar(), "parquet sink is columnar-capable");
            let n = sink.write_batch_columnar(&batch).await.unwrap();
            assert_eq!(n, 3, "all rows written via the columnar path");
            sink.flush().await.unwrap();
            // Drop writes the footer.
        }
        let mut ids = read_ids(&path);
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 2, 3], "columnar-written rows read back intact");
    }

    /// Rollover (directory) mode must keep its per-page file-rolling behavior:
    /// each `flush()` closes a file and the next page opens a new one. Assert
    /// the expected file count and total rows.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rollover_mode_still_rolls_files_per_page() {
        let tmp = tempfile::tempdir().unwrap();
        // Directory destination + a row threshold → rollover/multi-file mode.
        let cfg =
            ParquetSinkConfig::local(tmp.path().to_string_lossy().to_string()).max_rows_per_file(2);
        assert!(!compute_single_file_mode(&cfg));

        let mut all_ids = Vec::new();
        {
            let sink = ParquetSink::new(cfg).await.unwrap();
            // Two pages; max_rows_per_file=2 rolls within page 1.
            sink.write_batch(&[json!({"id": 1}), json!({"id": 2}), json!({"id": 3})])
                .await
                .unwrap();
            sink.flush().await.unwrap();
            sink.write_batch(&[json!({"id": 4})]).await.unwrap();
            sink.flush().await.unwrap();
        }

        // Read back every .parquet file in the directory.
        let mut files: Vec<_> = std::fs::read_dir(tmp.path())
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("parquet"))
            .collect();
        files.sort();
        assert!(
            files.len() >= 2,
            "rollover mode should produce multiple files, got {}",
            files.len()
        );
        for f in &files {
            all_ids.extend(read_ids(f));
        }
        all_ids.sort_unstable();
        assert_eq!(
            all_ids,
            vec![1, 2, 3, 4],
            "no rows lost across rolled files"
        );
    }

    /// Read the column names of a Parquet file's schema.
    fn read_columns(path: &std::path::Path) -> Vec<String> {
        let file = std::fs::File::open(path).expect("open parquet file");
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(file).expect("parquet reader builder");
        builder
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect()
    }

    /// Regression test for F34 (audit #264): the Parquet schema must be
    /// re-inferred per file. When a later file's first batch carries a *wider*
    /// schema (a new column added mid-run, e.g. CDC after ALTER TABLE ADD
    /// COLUMN), the new column must be written in that later file rather than
    /// silently dropped because the schema was locked from the very first batch
    /// of the run.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rollover_reinfers_widened_schema_in_later_file() {
        let tmp = tempfile::tempdir().unwrap();
        // Directory destination + row threshold → multi-file rollover mode.
        let cfg =
            ParquetSinkConfig::local(tmp.path().to_string_lossy().to_string()).max_rows_per_file(2);
        assert!(!compute_single_file_mode(&cfg));

        {
            let sink = ParquetSink::new(cfg).await.unwrap();
            // File 1: narrow schema {id}. Two rows hits the rollover threshold,
            // closing file 1 and clearing the locked schema.
            sink.write_batch(&[json!({"id": 1}), json!({"id": 2})])
                .await
                .unwrap();
            sink.flush().await.unwrap();
            // File 2: widened schema {id, extra}. Under the bug the schema
            // stayed locked to {id} and `extra` would be silently dropped.
            sink.write_batch(&[json!({"id": 3, "extra": "new-column"})])
                .await
                .unwrap();
            sink.flush().await.unwrap();
        }

        let mut files: Vec<_> = std::fs::read_dir(tmp.path())
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("parquet"))
            .collect();
        files.sort();
        assert_eq!(files.len(), 2, "expected exactly two rolled files");

        // The file holding id=3 must include the widened `extra` column.
        let mut found_extra = false;
        for f in &files {
            let cols = read_columns(f);
            if cols.iter().any(|c| c == "extra") {
                found_extra = true;
                assert!(
                    cols.iter().any(|c| c == "id"),
                    "widened file must still carry the original `id` column: {cols:?}"
                );
            }
        }
        assert!(
            found_extra,
            "a later file must re-infer and write the widened `extra` column"
        );
    }

    /// `close_current` must reset the per-file state so the next file
    /// re-infers its own schema: `state.schema` and `warned_fields` cleared,
    /// `rows_in_current_file` reset, `files_written` incremented.
    #[tokio::test]
    async fn close_current_resets_per_file_state() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = ParquetSinkConfig::local(tmp.path().to_string_lossy().to_string());
        let sink = ParquetSink::new(cfg).await.unwrap();

        {
            let mut state = sink.state.lock().await;
            // Simulate a written-into file: schema locked, a row counted, and a
            // dropped field warned-about.
            sink.write_chunk(&mut state, &[json!({"id": 1})])
                .await
                .unwrap();
            assert!(
                state.schema.is_some(),
                "schema should be locked after write"
            );
            state.warned_fields.insert("ghost".to_string());
            assert_eq!(state.rows_in_current_file, 1);

            sink.close_current(&mut state).await.unwrap();

            assert!(
                state.schema.is_none(),
                "schema must be cleared on rollover so the next file re-infers"
            );
            assert!(
                state.warned_fields.is_empty(),
                "warned_fields must be cleared so a new file re-warns"
            );
            assert_eq!(state.rows_in_current_file, 0);
            assert_eq!(state.files_written, 1);
        }
    }

    /// The unknown-field warning de-dupe set lives in `WriterState` and is
    /// cleared on rollover, so a field dropped within one file warns once, and
    /// a fresh file re-warns. We assert the de-dupe set behavior directly
    /// (tracing output is not asserted here) since the warn path itself is
    /// exercised by `unknown_fields_are_silently_dropped`.
    #[test]
    fn warn_on_unknown_fields_dedupes_per_file() {
        let records = vec![json!({"id": 1, "ghost": "x"})];
        let schema = infer_schema(&[json!({"id": 1})], 10).unwrap();
        let mut warned = std::collections::HashSet::new();

        warn_on_unknown_fields(&mut warned, &schema, &records);
        assert!(warned.contains("ghost"), "dropped field must be recorded");

        // Second call with the same set must not re-record (one warn per file).
        let before = warned.clone();
        warn_on_unknown_fields(&mut warned, &schema, &records);
        assert_eq!(warned, before, "already-warned field must not re-record");

        // Clearing (as close_current does) lets a new file re-warn.
        warned.clear();
        warn_on_unknown_fields(&mut warned, &schema, &records);
        assert!(warned.contains("ghost"), "a fresh file must re-warn");
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_outputs_records_every_rolled_part_individually() {
        // Retention GC provenance (#587). Rollover names each part with a fresh
        // UUID, so nothing outside the sink could enumerate them without
        // globbing the directory — which the GC is forbidden to do. Every part
        // must therefore arrive as its own recorded entry.
        let tmp = tempfile::tempdir().unwrap();
        let cfg =
            ParquetSinkConfig::local(tmp.path().to_string_lossy().to_string()).max_rows_per_file(2);
        let sink = ParquetSink::new(cfg).await.unwrap();
        assert!(sink.local_outputs().await.is_empty());

        sink.write_batch(&[json!({"id": 1}), json!({"id": 2}), json!({"id": 3})])
            .await
            .unwrap();
        sink.flush().await.unwrap();
        sink.write_batch(&[json!({"id": 4})]).await.unwrap();
        sink.flush().await.unwrap();

        let outs = sink.local_outputs().await;
        let on_disk: std::collections::BTreeSet<PathBuf> = std::fs::read_dir(tmp.path())
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("parquet"))
            .collect();
        assert!(
            on_disk.len() >= 2,
            "expected rollover to write several parts"
        );
        let recorded: std::collections::BTreeSet<PathBuf> =
            outs.iter().map(|o| o.path.clone()).collect();
        assert_eq!(
            recorded, on_disk,
            "every part faucet wrote must be recorded, and nothing else"
        );
        assert!(
            outs.iter().all(|o| !o.pre_existing),
            "UUID-named parts are always faucet's own files"
        );
        assert!(
            !recorded.contains(&PathBuf::from(tmp.path())),
            "the directory itself is never an output — only the files in it"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_outputs_reports_the_single_file_destination() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("out.parquet");
        let sink = ParquetSink::new(cfg(&path)).await.unwrap();
        assert!(compute_single_file_mode(&cfg(&path)));
        sink.write_batch(&[json!({"id": 1})]).await.unwrap();
        sink.flush().await.unwrap();

        let outs = sink.local_outputs().await;
        assert_eq!(outs.len(), 1);
        assert_eq!(outs[0].path, path);
        assert!(!outs[0].pre_existing);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_outputs_flags_a_pre_existing_single_file_destination() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("out.parquet");
        std::fs::write(&path, b"not really parquet").unwrap();
        let sink = ParquetSink::new(cfg(&path)).await.unwrap();
        sink.write_batch(&[json!({"id": 1})]).await.unwrap();
        sink.flush().await.unwrap();

        let outs = sink.local_outputs().await;
        assert_eq!(outs.len(), 1);
        assert!(
            outs[0].pre_existing,
            "faucet overwrote a file it did not create — never collectable"
        );
    }

    /// One `id: Int64` column — the locked file schema the drifting batch below
    /// contradicts.
    fn int_schema() -> SchemaRef {
        use arrow::datatypes::{DataType, Field, Schema};
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]))
    }

    #[test]
    fn classify_decoder_error_reshapes_a_type_conflict_variant() {
        use arrow::error::ArrowError;
        let records = vec![json!({"id": "not-an-int"})];
        let err = classify_decoder_error(
            &int_schema(),
            &records,
            // The arrow-json decoder's real shape for a value that does not fit
            // the locked field type.
            ArrowError::JsonError("whilst decoding field 'id': expected i64 got string".into()),
        );
        let msg = err.to_string();
        assert!(
            msg.contains("parquet type drift for field 'id'")
                && msg.contains("schema declares Int64")
                && msg.contains("batch contains string"),
            "both sides of the drift must be named: {msg}"
        );
    }

    #[test]
    fn classify_decoder_error_leaves_a_non_type_variant_alone() {
        use arrow::error::ArrowError;
        // Same drifting batch, so the field-guessing half would happily find a
        // mismatch — only the typed variant keeps an unrelated allocation
        // failure from being reported as type drift (#654 L28).
        let records = vec![json!({"id": "not-an-int"})];
        let err = classify_decoder_error(
            &int_schema(),
            &records,
            ArrowError::MemoryError("allocation of 8 bytes failed".into()),
        );
        let msg = err.to_string();
        assert!(
            msg.contains("parquet encode failed") && msg.contains("allocation of 8 bytes failed"),
            "a non-type failure must keep its raw message: {msg}"
        );
        assert!(
            !msg.contains("type drift"),
            "an unrelated failure must not be reported as type drift: {msg}"
        );
    }

    #[test]
    fn classify_decoder_error_falls_back_when_no_field_drifts() {
        use arrow::error::ArrowError;
        // A type-conflict variant whose batch contains nothing contradicting the
        // schema (e.g. the conflict is inside a nested value we don't inspect):
        // there is no field to name, so the raw message is preserved.
        let records = vec![json!({"id": 7})];
        let err = classify_decoder_error(
            &int_schema(),
            &records,
            ArrowError::JsonError("offset overflow decoding ListArray".into()),
        );
        assert_eq!(
            err.to_string(),
            "Sink error: parquet encode failed: Json error: offset overflow decoding ListArray"
        );
    }

    #[test]
    fn is_type_conflict_covers_the_decoder_variants_only() {
        use arrow::error::ArrowError as E;
        for err in [
            E::JsonError("x".into()),
            E::SchemaError("x".into()),
            E::CastError("x".into()),
            E::ParseError("x".into()),
        ] {
            assert!(is_type_conflict(&err), "{err} must classify as a conflict");
        }
        for err in [
            E::MemoryError("x".into()),
            E::ComputeError("x".into()),
            E::NotYetImplemented("x".into()),
            E::InvalidArgumentError("x".into()),
            E::IoError("x".into(), std::io::Error::other("x")),
            E::DivideByZero,
        ] {
            assert!(!is_type_conflict(&err), "{err} must not be a conflict");
        }
    }
}
