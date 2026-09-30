//! [`WriteSettings`] and [`FileWriter`]: the part of a file sink that does not
//! depend on where files are stored.

use super::backend::{Area, StorageBackend};
use super::encode::{Closed, Ctx, Failure, OpenFile, remove_fetched};
use super::layout::{NameTemplate, OLD_ROLE, tmp_path};
use super::options::{FileWriteMode, IfExists, JsonLinesOptions, ParquetCodec, ParquetOptions};
use faucet_core::{Compression, FaucetError, FileFormat, FormatOptions};
use futures::stream::{self, StreamExt};
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// The marker file that says an overwrite run's swap area exists.
pub const SWAP_MARKER: &str = ".faucet-swap";
/// The marker file that says an overwrite run started moving its files into
/// place. It lists them, one name per line, so an interrupted move can be
/// finished.
pub const COMMIT_MARKER: &str = ".faucet-commit";
/// Files moved or deleted at once when an overwrite run's files are moved
/// into place.
const SWAP_CONCURRENCY: usize = 8;

/// Everything the writer needs to know about the output, independent of
/// storage. Build it from a sink config with
/// [`WriteConfig`](super::WriteConfig) and check it with
/// [`validate`](Self::validate).
///
/// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
/// minor release; any change is called out in the changelog.
#[derive(Debug, Clone)]
pub struct WriteSettings {
    /// The resolved format.
    pub format: FileFormat,
    /// The resolved compression codec.
    pub codec: Compression,
    /// Per-format encoder options (CSV, Excel, XML, Avro).
    pub opts: FormatOptions,
    /// Parquet options.
    pub parquet: ParquetOptions,
    /// The Parquet codec when `parquet.compression` is unset.
    pub default_parquet_codec: ParquetCodec,
    /// JSON Lines options.
    pub json_lines: JsonLinesOptions,
    /// What happens when a file the run writes already exists.
    pub if_exists: IfExists,
    /// `overwrite` writes the output into a swap area and moves it into place
    /// when the run succeeds.
    pub write_mode: FileWriteMode,
    /// Roll to a new file after this many records.
    pub max_records_per_file: Option<usize>,
    /// Roll to a new file after about this many bytes.
    pub max_bytes_per_file: Option<usize>,
    /// Encrypt the output at rest.
    #[cfg(feature = "encryption")]
    pub encryption: Option<faucet_core::EncryptionSpec>,
    /// Close the open file at every [`flush`](FileWriter::flush) and start
    /// the next part, instead of keeping it open to extend later. For remote
    /// stores, where extending a published object means uploading it whole
    /// again. Only takes effect with a numbered (`{part}`) template.
    pub object_per_flush: bool,
    /// Close the open file at the end of every batch write, so each
    /// `write_batch` call becomes its own file(s). The object-store sinks'
    /// `batch_size: 0` ("no re-chunking") for Parquet. Only takes effect with
    /// a numbered (`{part}`) template.
    pub object_per_write: bool,
    /// At the end of a successful `if_exists: replace` run, delete this
    /// template's files the run did not write (parts left by a longer
    /// earlier run). Off for a template whose names are unique to the run,
    /// where there is nothing to find and listing would only cost requests.
    pub prune_stale: bool,
}

impl WriteSettings {
    /// Settings for `format` with `codec` and every other option at its
    /// default.
    pub fn new(format: FileFormat, codec: Compression) -> Self {
        Self {
            format,
            codec,
            opts: FormatOptions::default(),
            parquet: ParquetOptions::default(),
            default_parquet_codec: ParquetCodec::Snappy,
            json_lines: JsonLinesOptions::default(),
            if_exists: IfExists::default(),
            write_mode: FileWriteMode::default(),
            max_records_per_file: None,
            max_bytes_per_file: None,
            #[cfg(feature = "encryption")]
            encryption: None,
            object_per_flush: false,
            object_per_write: false,
            prune_stale: true,
        }
    }

    /// Whether a rollover cap is set.
    pub fn rolls_over(&self) -> bool {
        self.max_records_per_file.is_some() || self.max_bytes_per_file.is_some()
    }

    /// Whether encryption is configured.
    pub fn encrypted(&self) -> bool {
        #[cfg(feature = "encryption")]
        return self.encryption.is_some();
        #[cfg(not(feature = "encryption"))]
        false
    }

    /// Whether the format writes one record per line, so an encrypted file
    /// seals each line rather than the whole file.
    pub fn line_based(&self) -> bool {
        matches!(self.format, FileFormat::JsonLines | FileFormat::RawText)
    }

    /// The Parquet codec the output is written with.
    pub fn parquet_codec(&self) -> ParquetCodec {
        self.parquet.codec_or(self.default_parquet_codec)
    }

    /// What a failed batch write leaves behind (#737): a page lands in a
    /// scratch file and becomes visible only when a flush publishes it, so a
    /// failed write never exposes part of a page — but a page can span a
    /// rollover, and files closed by an earlier rollover stay.
    pub fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        if self.rolls_over() {
            faucet_core::BatchAtomicity::BestEffort
        } else {
            faucet_core::BatchAtomicity::Atomic
        }
    }

    /// Refuse every combination that would otherwise fail mid-run.
    pub fn validate(&self) -> Result<(), FaucetError> {
        let format = self.format;
        require_feature(format)?;
        if format == FileFormat::Orc {
            return Err(FaucetError::Config(
                "file sink: `orc` is read-only — there is no ORC writer; write Parquet for a \
                 columnar output"
                    .into(),
            ));
        }
        if matches!(self.max_records_per_file, Some(0)) {
            return Err(FaucetError::Config(
                "file sink: `max_records_per_file` must be at least 1".into(),
            ));
        }
        if matches!(self.max_bytes_per_file, Some(0)) {
            return Err(FaucetError::Config(
                "file sink: `max_bytes_per_file` must be at least 1".into(),
            ));
        }
        if self.write_mode == FileWriteMode::Overwrite && self.if_exists != IfExists::Replace {
            return Err(FaucetError::Config(
                "file sink: `write_mode: overwrite` replaces the whole output set, so \
                 `if_exists` must be `replace`"
                    .into(),
            ));
        }
        if format == FileFormat::Csv {
            self.opts.csv.validate()?;
        }
        if format == FileFormat::Parquet {
            super::options::validate_parquet(&self.parquet)
                .map_err(|e| FaucetError::Config(format!("file sink: {}", config_text(e))))?;
        }
        #[cfg(feature = "encryption")]
        if let Some(spec) = &self.encryption {
            faucet_core::CompiledEncryption::compile(spec)?;
        }
        if format == FileFormat::Avro
            && let Some(schema) = &self.opts.avro.schema
        {
            validate_avro_schema(schema)?;
        }
        Ok(())
    }

    /// [`validate`](Self::validate), plus the checks that depend on the
    /// output's names: `if_exists: append` to a whole-document format is
    /// refused unless the template is numbered (each run then adds new
    /// parts).
    pub fn validate_for(&self, template: &NameTemplate) -> Result<(), FaucetError> {
        self.validate()?;
        if self.if_exists == IfExists::Append
            && !crate::appendable(self.format)
            && !template.numbered()
        {
            return Err(FaucetError::Config(format!(
                "file sink: `if_exists: append` cannot add to a {} file without rewriting it — \
                 use JSON Lines, CSV or raw text, `if_exists: replace`, or a `{{part}}` template \
                 (or a rollover cap) so each run adds new files",
                self.format.as_str()
            )));
        }
        Ok(())
    }
}

fn config_text(e: FaucetError) -> String {
    match e {
        FaucetError::Config(m) => m,
        other => other.to_string(),
    }
}

fn require_feature(format: FileFormat) -> Result<(), FaucetError> {
    let (feature, built) = match format {
        FileFormat::Csv => ("file-format-csv", cfg!(feature = "file-format-csv")),
        FileFormat::Xml => ("file-format-xml", cfg!(feature = "file-format-xml")),
        FileFormat::Xlsx => ("file-format-excel", cfg!(feature = "file-format-excel")),
        FileFormat::Avro => ("file-format-avro", cfg!(feature = "file-format-avro")),
        FileFormat::Parquet => ("file-format-parquet", cfg!(feature = "file-format-parquet")),
        _ => return Ok(()),
    };
    built
        .then_some(())
        .ok_or_else(|| missing_feature(format, feature))
}

fn missing_feature(format: FileFormat, feature: &str) -> FaucetError {
    FaucetError::Config(format!(
        "file sink: `{}` needs the `{feature}` build feature",
        format.as_str()
    ))
}

#[cfg(feature = "file-format-avro")]
fn validate_avro_schema(schema: &Value) -> Result<(), FaucetError> {
    faucet_core::file_format::avro::parse_schema(schema).map(|_| ())
}

#[cfg(not(feature = "file-format-avro"))]
fn validate_avro_schema(_: &Value) -> Result<(), FaucetError> {
    Ok(())
}

/// Run CPU-bound encoding from async code: `block_in_place` on a
/// multi-thread runtime, inline otherwise. Storage I/O never runs here.
fn blocking<T>(f: impl FnOnce() -> T) -> T {
    match tokio::runtime::Handle::try_current() {
        Ok(h) if h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(f)
        }
        _ => f(),
    }
}

#[derive(Default)]
struct State {
    current: Option<OpenFile>,
    next_part: u64,
    /// Write calls so far; each record carries the call that wrote it.
    call: u64,
    /// Why the writer can no longer be used.
    poisoned: Option<String>,
    /// Whether a publish not yet settled carries data a caller was already
    /// told was written.
    pending_at_risk: bool,
}

impl State {
    fn check(&self) -> Result<(), FaucetError> {
        match &self.poisoned {
            Some(why) => Err(FaucetError::Sink(why.clone())),
            None => Ok(()),
        }
    }

    fn poison(&mut self, what: &str, e: &FaucetError) {
        if self.poisoned.is_none() {
            self.poisoned = Some(format!(
                "file sink: records already reported as written were lost ({what}: {e}), so \
                 this run cannot continue — every later write and flush fails"
            ));
        }
    }
}

/// One output set: the open file, its part number, and the modes, over a
/// [`StorageBackend`].
///
/// A failure that loses records an earlier call reported as written — a
/// failed rollover, flush or background upload of a file that holds them —
/// poisons the writer: every later write and flush fails, so the run fails
/// and its bookmark never passes them.
///
/// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
/// minor release; any change is called out in the changelog.
pub struct FileWriter {
    settings: WriteSettings,
    template: NameTemplate,
    backend: Arc<dyn StorageBackend>,
    #[cfg(feature = "encryption")]
    encryption: Option<faucet_core::CompiledEncryption>,
    state: tokio::sync::Mutex<State>,
    outputs: faucet_core::LocalOutputLog,
}

impl FileWriter {
    /// A writer for `template` files on `backend`. Validates `settings`.
    pub fn new(
        settings: WriteSettings,
        template: NameTemplate,
        backend: Arc<dyn StorageBackend>,
    ) -> Result<Self, FaucetError> {
        settings.validate_for(&template)?;
        Ok(Self {
            #[cfg(feature = "encryption")]
            encryption: settings
                .encryption
                .as_ref()
                .map(faucet_core::CompiledEncryption::compile)
                .transpose()?,
            settings,
            template,
            backend,
            state: tokio::sync::Mutex::new(State::default()),
            outputs: faucet_core::LocalOutputLog::new(),
        })
    }

    /// The settings the writer was built with.
    pub fn settings(&self) -> &WriteSettings {
        &self.settings
    }

    /// The file-name template.
    pub fn template(&self) -> &NameTemplate {
        &self.template
    }

    /// The storage backend.
    pub fn backend(&self) -> &Arc<dyn StorageBackend> {
        &self.backend
    }

    /// Local files this writer opened, for previews.
    pub fn local_outputs(&self) -> Vec<faucet_core::LocalOutput> {
        self.outputs.snapshot()
    }

    fn ctx(&self) -> Ctx<'_> {
        Ctx {
            format: self.settings.format,
            codec: self.settings.codec,
            opts: &self.settings.opts,
            parquet: &self.settings.parquet,
            parquet_codec: self.settings.parquet_codec(),
            json_lines: &self.settings.json_lines,
            #[cfg(feature = "encryption")]
            encryption: self.encryption.as_ref(),
        }
    }

    /// Whether the run writes into the swap area for an overwrite.
    pub fn overwriting(&self) -> bool {
        self.settings.write_mode == FileWriteMode::Overwrite
    }

    fn area(&self) -> Area {
        if self.overwriting() {
            Area::Swap
        } else {
            Area::Destination
        }
    }

    /// Whether a new file can go up in parts while it is written: a line
    /// format that is not sealed whole.
    fn can_stream(&self) -> bool {
        self.settings.line_based()
            && !(self.settings.encrypted() && self.settings.codec != Compression::None)
    }

    /// `(part, name)` of this template's files in `area`, by part.
    pub async fn existing(&self, area: Area) -> Result<Vec<(u64, String)>, FaucetError> {
        Ok(self.template.select(self.backend.list(area).await?))
    }

    async fn open_next(&self, st: &mut State, call: u64) -> Result<(), FaucetError> {
        let area = self.area();
        let append = self.settings.if_exists == IfExists::Append;
        if st.next_part == 0 {
            self.backend.prepare(Area::Destination).await?;
            if self.overwriting() {
                self.backend.prepare(Area::Swap).await?;
            }
            st.next_part = if append && self.template.numbered() {
                self.existing(area).await?.last().map_or(1, |(n, _)| n + 1)
            } else {
                1
            };
        }
        let name = self.template.file_name(st.next_part);
        let exists = match self.settings.if_exists {
            IfExists::Replace => false,
            IfExists::Append | IfExists::Error => self.backend.exists(area, &name).await?,
        };
        if exists && self.settings.if_exists == IfExists::Error {
            return Err(FaucetError::Sink(format!(
                "file sink: '{}' already exists and `if_exists` is `error`",
                self.backend.describe(area, &name)
            )));
        }
        if let Some(path) = self.backend.local_path(Area::Destination, &name) {
            self.outputs.record_open_probing_with(path, !append);
        }
        let tmp = self.backend.scratch_path(area, &name);
        let label = self.backend.describe(area, &name);
        let existing = if exists && append {
            let to = tmp_path(&tmp, OLD_ROLE);
            self.backend.fetch(area, &name, &to).await?;
            Some(to)
        } else {
            None
        };
        let part_size = if self.can_stream() && existing.is_none() {
            self.backend.part_size()
        } else {
            None
        };
        let ctx = self.ctx();
        let opened = blocking(|| {
            OpenFile::create(&ctx, area, name, label, tmp, existing.as_deref(), part_size)
        });
        if let Some(p) = &existing {
            remove_fetched(p);
        }
        let mut file = opened?;
        file.first_call = call;
        st.current = Some(file);
        Ok(())
    }

    /// A local copy of the published file when the next write continues it.
    async fn existing_copy(&self, need: Existing) -> Result<Option<PathBuf>, FaucetError> {
        match need {
            Existing::Unneeded => Ok(None),
            Existing::Kept(prev) => Ok(Some(prev)),
            Existing::Fetch {
                area,
                name,
                label,
                to,
            } => self
                .fetch_published(area, &name, &label, to)
                .await
                .map(Some),
        }
    }

    async fn fetch_published(
        &self,
        area: Area,
        name: &str,
        label: &str,
        to: PathBuf,
    ) -> Result<PathBuf, FaucetError> {
        if !self.backend.exists(area, name).await? {
            return Err(FaucetError::Sink(format!(
                "file sink: '{label}' was published earlier in this run but is gone, so it \
                 cannot be continued"
            )));
        }
        self.backend.fetch(area, name, &to).await?;
        Ok(to)
    }

    /// Run one encoding step on the open file, then upload any full parts.
    async fn write_chunk(
        &self,
        st: &mut State,
        call: u64,
        op: impl FnOnce(&mut OpenFile, &Ctx<'_>, Option<&Path>) -> Result<(), Failure>,
    ) -> Result<(), FaucetError> {
        let need = Existing::of(st.current.as_ref().expect("a file is open"));
        let existing = self.existing_copy(need).await?;
        let cur = st.current.as_mut().expect("a file is open");
        let ctx = self.ctx();
        let result = blocking(|| op(cur, &ctx, existing.as_deref()));
        if let Some(p) = &existing {
            remove_fetched(p);
        }
        match result {
            Ok(()) => {}
            Err(Failure::Clean(e)) => return Err(e),
            Err(Failure::Dirty(e)) => return Err(self.lose(st, call, e).await),
        }
        let cur = st.current.as_mut().expect("a file is open");
        let parts = cur.take_parts();
        if !parts.is_empty()
            && let Err(e) = self.put_parts(cur, parts).await
        {
            return Err(self.lose(st, call, e).await);
        }
        Ok(())
    }

    async fn put_parts(&self, f: &mut OpenFile, parts: Vec<Vec<u8>>) -> Result<(), FaucetError> {
        if f.stream.is_none() {
            f.stream = Some(self.backend.open_stream(f.area, &f.name).await?);
        }
        let stream = f.stream.as_mut().expect("opened above");
        for p in parts {
            stream.put(p).await?;
        }
        Ok(())
    }

    /// Publish `f` so it holds everything written so far. `keep` keeps a
    /// local copy to continue from.
    async fn publish_file(&self, f: &mut OpenFile, keep: bool) -> Result<(), FaucetError> {
        let ctx = self.ctx();
        match blocking(|| f.close(&ctx))? {
            Closed::Nothing => {}
            Closed::File => {
                if keep {
                    f.keep_copy();
                }
                self.backend.publish(&f.tmp, f.area, &f.name).await?;
            }
            Closed::Parts(parts) if f.stream.is_none() && parts.len() <= 1 => {
                f.spill(parts.first().map_or(&[][..], Vec::as_slice))?;
                self.backend.publish(&f.tmp, f.area, &f.name).await?;
            }
            Closed::Parts(parts) => {
                self.put_parts(f, parts).await?;
                let stream = f.stream.take().expect("opened by put_parts");
                stream.finish().await?;
            }
        }
        f.mark_published(&ctx);
        Ok(())
    }

    /// Drop the open file after a failure, poisoning the writer when that
    /// loses data a caller was already told was written.
    async fn lose(&self, st: &mut State, call: u64, e: FaucetError) -> FaucetError {
        if let Some(mut f) = st.current.take() {
            if f.at_risk(call) {
                st.poison(&f.label, &e);
            }
            if let Some(s) = f.stream.take() {
                s.abort().await;
            }
            f.discard();
            if self.template.numbered() {
                st.next_part += 1;
            }
        }
        e
    }

    fn cap_reached(&self, records: usize, bytes: usize) -> bool {
        self.settings
            .max_records_per_file
            .is_some_and(|m| records >= m)
            || self.settings.max_bytes_per_file.is_some_and(|m| bytes >= m)
    }

    /// Publish the open file and move to the next part.
    async fn roll(&self, st: &mut State, call: u64) -> Result<(), FaucetError> {
        if let Some(mut f) = st.current.take() {
            let at_risk = f.at_risk(call);
            match self.publish_file(&mut f, false).await {
                Ok(()) => {
                    st.pending_at_risk |= at_risk;
                    f.discard();
                }
                Err(e) => {
                    st.current = Some(f);
                    return Err(self.lose(st, call, e).await);
                }
            }
        }
        st.next_part += 1;
        Ok(())
    }
}

impl FileWriter {
    /// Write `rows`, rolling to a new file whenever a cap is reached.
    pub async fn write_rows(&self, rows: &[Value]) -> Result<usize, FaucetError> {
        let mut guard = self.state.lock().await;
        let st = &mut *guard;
        st.check()?;
        st.call += 1;
        let call = st.call;
        let track_bytes = self.settings.max_bytes_per_file.is_some();
        let mut i = 0;
        while i < rows.len() {
            if st.current.is_none() {
                self.open_next(st, call).await?;
            }
            let cur = st.current.as_ref().expect("opened above");
            let (mut records, mut bytes) = (cur.records, cur.bytes);
            let mut end = i;
            while end < rows.len() && !(records > 0 && self.cap_reached(records, bytes)) {
                if track_bytes {
                    bytes += estimate(&rows[end]);
                }
                records += 1;
                end += 1;
            }
            if end > i {
                let chunk = &rows[i..end];
                self.write_chunk(st, call, |f, ctx, existing| f.write(ctx, chunk, existing))
                    .await?;
                st.current.as_mut().expect("still open").bytes = bytes;
            }
            i = end;
            let cur = st.current.as_ref().expect("still open");
            if self.cap_reached(cur.records, cur.bytes) {
                self.roll(st, call).await?;
            }
        }
        self.end_of_write(st, call).await?;
        Ok(rows.len())
    }

    /// Write an Arrow batch on the columnar path (Parquet). The byte cap
    /// counts the batch's in-memory Arrow size, shared evenly by its rows.
    #[cfg(feature = "file-format-parquet")]
    pub async fn write_batch(
        &self,
        batch: &arrow::array::RecordBatch,
    ) -> Result<usize, FaucetError> {
        let rows = batch.num_rows();
        if rows == 0 {
            return Ok(0);
        }
        let per_row = batch.get_array_memory_size().div_ceil(rows);
        let mut guard = self.state.lock().await;
        let st = &mut *guard;
        st.check()?;
        st.call += 1;
        let call = st.call;
        let mut offset = 0;
        while offset < rows {
            if st.current.is_none() {
                self.open_next(st, call).await?;
            }
            let cur = st.current.as_ref().expect("opened above");
            let mut room = self
                .settings
                .max_records_per_file
                .map_or(rows - offset, |m| m.saturating_sub(cur.records).max(1));
            if let Some(max) = self.settings.max_bytes_per_file {
                room = room.min(
                    max.saturating_sub(cur.bytes)
                        .div_ceil(per_row.max(1))
                        .max(1),
                );
            }
            let len = room.min(rows - offset);
            let slice = batch.slice(offset, len);
            self.write_chunk(st, call, |f, ctx, existing| {
                f.write_batch(ctx, &slice, existing)
            })
            .await?;
            let cur = st.current.as_mut().expect("still open");
            if self.settings.max_bytes_per_file.is_some() {
                cur.bytes += per_row * len;
            }
            offset += len;
            if self.cap_reached(cur.records, cur.bytes) {
                self.roll(st, call).await?;
            }
        }
        self.end_of_write(st, call).await?;
        Ok(rows)
    }

    /// End of a batch write: close the file when each write is its own
    /// object, then wait for every upload the batch started, so a failed
    /// upload fails the batch that wrote it (and a DLQ receives the right
    /// rows) rather than a later one.
    async fn end_of_write(&self, st: &mut State, call: u64) -> Result<(), FaucetError> {
        if self.settings.object_per_write && self.template.numbered() && st.current.is_some() {
            self.roll(st, call).await?;
        }
        let at_risk = std::mem::take(&mut st.pending_at_risk);
        if let Err(e) = self.backend.settle().await {
            if at_risk {
                st.poison("an upload", &e);
            }
            return Err(e);
        }
        Ok(())
    }

    /// Publish the open file so it holds everything written so far, and wait
    /// until every file closed earlier has landed too. A failure poisons the
    /// writer: everything unpublished was already reported as written.
    pub async fn flush(&self) -> Result<(), FaucetError> {
        let mut guard = self.state.lock().await;
        let st = &mut *guard;
        st.check()?;
        let call = st.call + 1;
        if self.settings.object_per_flush && self.template.numbered() {
            if st.current.is_some() {
                self.roll(st, call).await?;
            }
        } else if let Some(mut f) = st.current.take() {
            let keep = self.backend.local_path(f.area, &f.name).is_none();
            let published = self.publish_file(&mut f, keep).await;
            st.current = Some(f);
            if let Err(e) = published {
                return Err(self.lose(st, call, e).await);
            }
        }
        st.pending_at_risk = false;
        if let Err(e) = self.backend.settle().await {
            st.poison("an upload", &e);
            return Err(e);
        }
        Ok(())
    }

    /// End of a successful run: with `if_exists: replace`, delete this
    /// template's files the run did not write (parts left by a longer
    /// earlier run). Skipped when the template's names are unique to the run.
    pub async fn complete(&self) -> Result<(), FaucetError> {
        if self.settings.if_exists != IfExists::Replace
            || self.overwriting()
            || !self.settings.prune_stale
        {
            return Ok(());
        }
        let st = self.state.lock().await;
        let first_unwritten = match (st.next_part, st.current.is_some()) {
            (0, _) => 1,
            (n, true) => n + 1,
            (n, false) => n,
        };
        drop(st);
        self.backend.settle().await?;
        let stale: Vec<String> = self
            .existing(Area::Destination)
            .await?
            .into_iter()
            .filter(|(n, _)| *n >= first_unwritten)
            .map(|(_, name)| name)
            .collect();
        self.delete_all(Area::Destination, stale).await
    }

    /// `write_mode: overwrite`: finish an interrupted move of an earlier run,
    /// then start with an empty swap area.
    pub async fn begin_overwrite(&self) -> Result<(), FaucetError> {
        self.backend.prepare(Area::Destination).await?;
        self.finish_interrupted_move().await?;
        self.clear_swap().await?;
        self.backend.prepare(Area::Swap).await?;
        self.put_marker(SWAP_MARKER, "").await
    }

    /// `write_mode: overwrite`: move every file in the swap area into place,
    /// delete this template's files the run did not write, and drop the swap
    /// area. The files to move are recorded first, so a move that stops
    /// half-way is finished by the next commit, abort or run.
    pub async fn commit_overwrite(&self) -> Result<(), FaucetError> {
        self.backend.settle().await?;
        if self.finish_interrupted_move().await? {
            return Ok(());
        }
        if !self.backend.exists(Area::Swap, SWAP_MARKER).await? {
            return Err(FaucetError::Sink(format!(
                "file sink: overwrite swap area '{}' is missing, so there is nothing to move \
                 into place; the destination is unchanged",
                self.backend.describe(Area::Swap, "")
            )));
        }
        let names: Vec<String> = self
            .existing(Area::Swap)
            .await?
            .into_iter()
            .map(|(_, n)| n)
            .collect();
        self.put_marker(COMMIT_MARKER, &names.join("\n")).await?;
        self.move_into_place(names).await
    }

    /// `write_mode: overwrite`: discard the open file and the swap area. A
    /// move into place that already started is finished instead, so the
    /// destination is never left half old, half new.
    pub async fn abort_overwrite(&self) -> Result<(), FaucetError> {
        self.discard().await;
        self.backend.cancel().await;
        if self.finish_interrupted_move().await? {
            return Ok(());
        }
        self.clear_swap().await
    }

    /// Whether an overwrite swap area exists right now.
    pub async fn swap_area_exists(&self) -> Result<bool, FaucetError> {
        Ok(self.backend.exists(Area::Swap, SWAP_MARKER).await?
            || self.backend.exists(Area::Swap, COMMIT_MARKER).await?)
    }

    /// Drop the open file's scratch files without publishing it.
    pub async fn discard(&self) {
        let mut st = self.state.lock().await;
        if let Some(mut f) = st.current.take() {
            if let Some(s) = f.stream.take() {
                s.abort().await;
            }
            f.discard();
        }
    }

    async fn put_marker(&self, name: &str, body: &str) -> Result<(), FaucetError> {
        let scratch = self.backend.scratch_path(Area::Swap, name);
        std::fs::write(&scratch, body).map_err(|e| {
            FaucetError::Sink(format!(
                "file sink: writing the overwrite marker '{}': {e}",
                scratch.display()
            ))
        })?;
        self.backend.publish(&scratch, Area::Swap, name).await?;
        self.backend.settle().await
    }

    /// Finish the move a commit started, when its marker is there.
    async fn finish_interrupted_move(&self) -> Result<bool, FaucetError> {
        if !self.backend.exists(Area::Swap, COMMIT_MARKER).await? {
            return Ok(false);
        }
        let to = self.backend.scratch_path(Area::Swap, COMMIT_MARKER);
        self.backend.fetch(Area::Swap, COMMIT_MARKER, &to).await?;
        let text = std::fs::read_to_string(&to);
        let _ = std::fs::remove_file(&to);
        let text = text.map_err(|e| {
            FaucetError::Sink(format!("file sink: reading the overwrite marker: {e}"))
        })?;
        let names: Vec<String> = text
            .lines()
            .filter(|l| !l.is_empty())
            .map(String::from)
            .collect();
        tracing::warn!(
            area = %self.backend.describe(Area::Swap, ""),
            files = names.len(),
            "file sink: finishing an overwrite that stopped while moving its files into place"
        );
        self.move_into_place(names).await?;
        Ok(true)
    }

    async fn move_into_place(&self, names: Vec<String>) -> Result<(), FaucetError> {
        let staged: std::collections::HashSet<String> =
            self.backend.list(Area::Swap).await?.into_iter().collect();
        let to_move: Vec<String> = names
            .iter()
            .filter(|n| staged.contains(*n))
            .cloned()
            .collect();
        let moves = stream::iter(to_move)
            .map(|name| async move { self.backend.promote(&name).await })
            .buffer_unordered(SWAP_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;
        moves.into_iter().collect::<Result<Vec<()>, _>>()?;
        let kept: std::collections::HashSet<&str> = names.iter().map(String::as_str).collect();
        let stale: Vec<String> = self
            .existing(Area::Destination)
            .await?
            .into_iter()
            .map(|(_, n)| n)
            .filter(|n| !kept.contains(n.as_str()))
            .collect();
        self.delete_all(Area::Destination, stale).await?;
        self.clear_swap().await?;
        self.backend.delete(Area::Swap, COMMIT_MARKER).await
    }

    /// Delete everything in the swap area but the commit marker, the swap
    /// marker last.
    async fn clear_swap(&self) -> Result<(), FaucetError> {
        let files: Vec<String> = self
            .backend
            .list(Area::Swap)
            .await?
            .into_iter()
            .filter(|n| n != SWAP_MARKER && n != COMMIT_MARKER)
            .collect();
        self.delete_all(Area::Swap, files).await?;
        self.backend.delete(Area::Swap, SWAP_MARKER).await
    }

    async fn delete_all(&self, area: Area, names: Vec<String>) -> Result<(), FaucetError> {
        stream::iter(names)
            .map(|name| async move { self.backend.delete(area, &name).await })
            .buffer_unordered(SWAP_CONCURRENCY)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<()>, _>>()
            .map(|_| ())
    }
}

/// What the next write needs of the published file.
enum Existing {
    Unneeded,
    Kept(PathBuf),
    Fetch {
        area: Area,
        name: String,
        label: String,
        to: PathBuf,
    },
}

impl Existing {
    fn of(f: &OpenFile) -> Self {
        if !f.needs_existing() {
            return Self::Unneeded;
        }
        match &f.prev {
            Some(prev) if prev.exists() => Self::Kept(prev.clone()),
            _ => Self::Fetch {
                area: f.area,
                name: f.name.clone(),
                label: f.label.clone(),
                to: f.fetch_path(),
            },
        }
    }
}

impl Drop for FileWriter {
    fn drop(&mut self) {
        if let Some(mut f) = self.state.get_mut().current.take() {
            if let Some(s) = f.stream.take()
                && let Ok(h) = tokio::runtime::Handle::try_current()
            {
                h.spawn(s.abort());
            }
            f.discard();
        }
    }
}

fn estimate(v: &Value) -> usize {
    serde_json::to_vec(v).map_or(0, |b| b.len()) + 1
}

#[cfg(test)]
#[path = "writer_tests.rs"]
mod tests;
