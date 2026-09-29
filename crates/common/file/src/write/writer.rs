//! [`WriteSettings`] and [`FileWriter`]: the part of a file sink that does not
//! depend on where files are stored.

use super::backend::{Area, StorageBackend};
use super::encode::{Ctx, OpenFile};
use super::layout::NameTemplate;
use super::options::{FileMode, FileWriteMode, JsonLinesOptions, ParquetOptions};
use faucet_core::{Compression, FaucetError, FileFormat, FormatOptions};
use serde_json::Value;
use std::sync::{Arc, Mutex};

/// Everything the writer needs to know about the output, independent of
/// storage. Build it from a sink config and check it with
/// [`validate`](Self::validate).
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
    /// JSON Lines options.
    pub json_lines: JsonLinesOptions,
    /// What happens when a file the run writes already exists.
    pub mode: FileMode,
    /// `overwrite` stages the output and swaps it in on commit.
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
    /// stores, where extending a published object means downloading it
    /// again. Only takes effect with a numbered (`{part}`) template.
    pub object_per_flush: bool,
    /// Close the open file at the end of every batch write, so each
    /// `write_batch` call becomes its own file(s). The object-store sinks'
    /// `batch_size: 0` ("no re-chunking") for Parquet. Only takes effect with
    /// a numbered (`{part}`) template.
    pub object_per_write: bool,
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
            json_lines: JsonLinesOptions::default(),
            mode: FileMode::default(),
            write_mode: FileWriteMode::default(),
            max_records_per_file: None,
            max_bytes_per_file: None,
            #[cfg(feature = "encryption")]
            encryption: None,
            object_per_flush: false,
            object_per_write: false,
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

    /// What a failed batch write leaves behind (#737): a page lands in a
    /// scratch file and becomes visible only when a flush publishes it, so a
    /// failed write never exposes part of a page — but a page can span a
    /// rollover, and files finalised by an earlier rollover stay.
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
        if self.write_mode == FileWriteMode::Overwrite && self.mode != FileMode::Overwrite {
            return Err(FaucetError::Config(
                "file sink: `write_mode: overwrite` replaces the whole output set, so `mode` \
                 must be `overwrite`"
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
}

impl WriteSettings {
    /// [`validate`](Self::validate), plus the checks that depend on the
    /// output's names: `mode: append` to a whole-document format is refused
    /// unless the template is numbered (each run then adds new parts).
    pub fn validate_for(&self, template: &NameTemplate) -> Result<(), FaucetError> {
        self.validate()?;
        if self.mode == FileMode::Append && !crate::appendable(self.format) && !template.numbered()
        {
            return Err(FaucetError::Config(format!(
                "file sink: `mode: append` cannot add to a {} file without rewriting it — use \
                 JSON Lines, CSV or raw text, `mode: overwrite`, or a `{{part}}` template (or a \
                 rollover cap) so each run adds new files",
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
    let missing = match format {
        FileFormat::Csv if !cfg!(feature = "file-format-csv") => Some("file-format-csv"),
        FileFormat::Xml if !cfg!(feature = "file-format-xml") => Some("file-format-xml"),
        FileFormat::Xlsx if !cfg!(feature = "file-format-excel") => Some("file-format-excel"),
        FileFormat::Avro if !cfg!(feature = "file-format-avro") => Some("file-format-avro"),
        FileFormat::Parquet if !cfg!(feature = "file-format-parquet") => {
            Some("file-format-parquet")
        }
        _ => None,
    };
    match missing {
        Some(feature) => Err(FaucetError::Config(format!(
            "file sink: `{}` needs the `{feature}` build feature",
            format.as_str()
        ))),
        None => Ok(()),
    }
}

#[cfg(feature = "file-format-avro")]
fn validate_avro_schema(schema: &Value) -> Result<(), FaucetError> {
    faucet_core::file_format::avro::parse_schema(schema).map(|_| ())
}

#[cfg(not(feature = "file-format-avro"))]
fn validate_avro_schema(_: &Value) -> Result<(), FaucetError> {
    Ok(())
}

/// Run blocking file I/O from async code: `block_in_place` on a multi-thread
/// runtime, inline otherwise.
pub fn blocking<T>(f: impl FnOnce() -> T) -> T {
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
}

/// One output set: the open file, its part number, and the modes, over a
/// [`StorageBackend`]. Every method blocks; see [`blocking`].
pub struct FileWriter {
    settings: WriteSettings,
    template: NameTemplate,
    backend: Arc<dyn StorageBackend>,
    #[cfg(feature = "encryption")]
    encryption: Option<faucet_core::CompiledEncryption>,
    state: Mutex<State>,
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
            state: Mutex::new(State::default()),
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
            json_lines: &self.settings.json_lines,
            backend: self.backend.as_ref(),
            sync: self.backend.sync_scratch(),
            #[cfg(feature = "encryption")]
            encryption: self.encryption.as_ref(),
        }
    }

    /// Whether the run stages its output for an overwrite commit.
    pub fn overwriting(&self) -> bool {
        self.settings.write_mode == FileWriteMode::Overwrite
    }

    fn area(&self) -> Area {
        if self.overwriting() {
            Area::Staging
        } else {
            Area::Destination
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// `(part, name)` of this template's files in `area`, by part.
    pub fn existing(&self, area: Area) -> Result<Vec<(u64, String)>, FaucetError> {
        Ok(self.template.select(self.backend.list(area)?))
    }

    fn open_next(&self, st: &mut State) -> Result<(), FaucetError> {
        let area = self.area();
        let append = self.settings.mode == FileMode::Append;
        if st.next_part == 0 {
            self.backend.prepare(Area::Destination)?;
            if self.overwriting() {
                self.backend.prepare(Area::Staging)?;
            }
            let template = &self.template;
            self.backend
                .remove_stale_scratch(area, &|n| template.owns_scratch(n));
            st.next_part = if append && self.template.numbered() {
                self.existing(area)?.last().map_or(1, |(n, _)| n + 1)
            } else {
                1
            };
        } else {
            self.backend.prepare(area)?;
        }
        let name = self.template.file_name(st.next_part);
        let exists = match self.settings.mode {
            FileMode::Overwrite => false,
            FileMode::Append | FileMode::ErrorIfExists => self.backend.exists(area, &name)?,
        };
        if exists && self.settings.mode == FileMode::ErrorIfExists {
            return Err(FaucetError::Sink(format!(
                "file sink: '{}' already exists and `mode` is `error_if_exists`",
                self.backend.describe(area, &name)
            )));
        }
        if let Some(path) = self.backend.local_path(Area::Destination, &name) {
            self.outputs.record_open_probing_with(path, !append);
        }
        st.current = Some(OpenFile::create(&self.ctx(), area, name, exists && append)?);
        Ok(())
    }

    fn cap_reached(&self, records: usize, bytes: usize) -> bool {
        self.settings
            .max_records_per_file
            .is_some_and(|m| records >= m)
            || self.settings.max_bytes_per_file.is_some_and(|m| bytes >= m)
    }

    fn roll(&self, st: &mut State) -> Result<(), FaucetError> {
        if let Some(mut f) = st.current.take() {
            let r = f.finalize(&self.ctx());
            f.discard();
            r?;
        }
        st.next_part += 1;
        Ok(())
    }
}

impl FileWriter {
    /// Write `rows`, rolling to a new file whenever a cap is reached.
    pub fn write_rows(&self, rows: &[Value]) -> Result<usize, FaucetError> {
        let mut st = self.lock();
        let track_bytes = self.settings.max_bytes_per_file.is_some();
        let mut i = 0;
        while i < rows.len() {
            if st.current.is_none() {
                self.open_next(&mut st)?;
            }
            let cur = st.current.as_mut().expect("opened above");
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
                cur.write(&self.ctx(), &rows[i..end])?;
                cur.bytes = bytes;
            }
            i = end;
            if self.cap_reached(cur.records, cur.bytes) {
                self.roll(&mut st)?;
            }
        }
        self.end_of_write(&mut st)?;
        Ok(rows.len())
    }

    /// Write an Arrow batch on the columnar path (Parquet).
    #[cfg(feature = "file-format-parquet")]
    pub fn write_batch(&self, batch: &arrow::array::RecordBatch) -> Result<usize, FaucetError> {
        let rows = batch.num_rows();
        if rows == 0 {
            return Ok(0);
        }
        let mut st = self.lock();
        let mut offset = 0;
        while offset < rows {
            if st.current.is_none() {
                self.open_next(&mut st)?;
            }
            let cur = st.current.as_mut().expect("opened above");
            let room = self
                .settings
                .max_records_per_file
                .map_or(rows - offset, |m| m.saturating_sub(cur.records).max(1));
            let len = room.min(rows - offset);
            let slice = batch.slice(offset, len);
            cur.write_batch(&self.ctx(), &slice)?;
            if self.settings.max_bytes_per_file.is_some() {
                cur.bytes += slice.get_array_memory_size();
            }
            offset += len;
            if self.cap_reached(cur.records, cur.bytes) {
                self.roll(&mut st)?;
            }
        }
        self.end_of_write(&mut st)?;
        Ok(rows)
    }

    /// End of a batch write: close the file when each write is its own
    /// object, then wait for every upload the batch started, so a failed
    /// upload fails the batch that wrote it (and a DLQ receives the right
    /// rows) rather than a later one.
    fn end_of_write(&self, st: &mut State) -> Result<(), FaucetError> {
        if self.settings.object_per_write && self.template.numbered() && st.current.is_some() {
            self.roll(st)?;
        }
        self.backend.settle()
    }

    /// Publish the open file so it holds everything written so far, and wait
    /// until every file closed earlier has landed too.
    pub fn flush(&self) -> Result<(), FaucetError> {
        let mut st = self.lock();
        if self.settings.object_per_flush && self.template.numbered() {
            if st.current.is_some() {
                self.roll(&mut st)?;
            }
        } else if let Some(f) = st.current.as_mut() {
            f.finalize(&self.ctx())?;
        }
        self.backend.settle()
    }

    /// End of a successful run: in plain `mode: overwrite`, delete this
    /// template's files the run did not write (parts left by a longer
    /// earlier run).
    pub fn complete(&self) -> Result<(), FaucetError> {
        if self.settings.mode != FileMode::Overwrite || self.overwriting() {
            return Ok(());
        }
        let st = self.lock();
        let first_unwritten = match (st.next_part, st.current.is_some()) {
            (0, _) => 1,
            (n, true) => n + 1,
            (n, false) => n,
        };
        drop(st);
        self.backend.settle()?;
        for (n, name) in self.existing(Area::Destination)? {
            if n >= first_unwritten {
                self.backend.delete(Area::Destination, &name)?;
            }
        }
        Ok(())
    }

    /// `write_mode: overwrite`: start with an empty staging area.
    pub fn begin_overwrite(&self) -> Result<(), FaucetError> {
        self.backend.begin_staging()
    }

    /// `write_mode: overwrite`: move every staged file into place, delete this
    /// template's files the run did not write, and drop the staging area.
    pub fn commit_overwrite(&self) -> Result<(), FaucetError> {
        self.backend.settle()?;
        if !self.backend.staging_ready()? {
            return Err(FaucetError::Sink(format!(
                "file sink: overwrite staging area '{}' is missing, so there is nothing to swap \
                 in; the destination is unchanged",
                self.backend.describe(Area::Staging, "")
            )));
        }
        let mut kept = std::collections::HashSet::new();
        for (_, name) in self.existing(Area::Staging)? {
            self.backend.promote(&name)?;
            kept.insert(name);
        }
        for (_, name) in self.existing(Area::Destination)? {
            if !kept.contains(&name) {
                self.backend.delete(Area::Destination, &name)?;
            }
        }
        self.backend.clear_staging()
    }

    /// `write_mode: overwrite`: discard the open file and the staging area.
    pub fn abort_overwrite(&self) -> Result<(), FaucetError> {
        self.discard();
        self.backend.cancel();
        self.backend.clear_staging()
    }

    /// Drop the open file's scratch files without publishing it.
    pub fn discard(&self) {
        if let Some(mut f) = self.lock().current.take() {
            f.discard();
        }
    }
}

impl Drop for FileWriter {
    fn drop(&mut self) {
        self.discard();
    }
}

fn estimate(v: &Value) -> usize {
    serde_json::to_vec(v).map_or(0, |b| b.len()) + 1
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn estimate_is_the_json_length_plus_a_newline() {
        assert_eq!(estimate(&json!({"a": 1})), 8);
    }
}
