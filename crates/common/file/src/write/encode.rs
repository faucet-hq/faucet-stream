//! One output file: encoded into a local scratch file and published by the
//! backend on [`OpenFile::finalize`], so a reader sees a complete file or
//! none at all.

use super::backend::{Area, StorageBackend};
use super::layout::{io_err, tmp_path};
use faucet_core::compression::{SyncCompressWriter, sync_compress_writer};
use faucet_core::{Compression, FaucetError, FileFormat, FormatOptions};
use serde_json::Value;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

/// The per-sink constants every file needs.
pub(crate) struct Ctx<'a> {
    pub format: FileFormat,
    pub codec: Compression,
    pub opts: &'a FormatOptions,
    #[cfg_attr(not(feature = "file-format-parquet"), allow(dead_code))]
    pub parquet: &'a super::options::ParquetOptions,
    pub json_lines: &'a super::options::JsonLinesOptions,
    pub backend: &'a dyn StorageBackend,
    /// Whether finished scratch files are fsynced (see
    /// [`StorageBackend::sync_scratch`]).
    pub sync: bool,
    #[cfg(feature = "encryption")]
    pub encryption: Option<&'a faucet_core::CompiledEncryption>,
}

impl Ctx<'_> {
    /// The bytes of `records` as the line formats write them: pretty or
    /// compact JSON, and sealed per record when encryption is on.
    fn line_bytes(&self, records: &[Value]) -> Result<Vec<u8>, FaucetError> {
        #[cfg(feature = "encryption")]
        if let Some(enc) = self.encryption
            && self.seals_lines()
        {
            use base64::Engine as _;
            let mut out = Vec::new();
            for r in records {
                let mut line = self.plain_lines(std::slice::from_ref(r))?;
                line.pop();
                out.extend_from_slice(
                    base64::engine::general_purpose::STANDARD
                        .encode(enc.encrypt(&line))
                        .as_bytes(),
                );
                out.push(b'\n');
            }
            return Ok(out);
        }
        self.plain_lines(records)
    }

    fn plain_lines(&self, records: &[Value]) -> Result<Vec<u8>, FaucetError> {
        if self.format != FileFormat::JsonLines || !self.json_lines.pretty {
            return faucet_core::file_format::encode(records, self.format, self.opts);
        }
        let mut out = Vec::new();
        for r in records {
            serde_json::to_writer_pretty(&mut out, r)
                .map_err(|e| FaucetError::Sink(format!("json_lines: {e}")))?;
            out.push(b'\n');
        }
        Ok(out)
    }

    /// Whether a `mode: append` file can be extended in place: each writer
    /// adds only its new bytes under the backend's lock, so concurrent
    /// appenders never lose each other's records. Line formats (sealed per
    /// line when encrypted) and unencrypted CSV; everything else rewrites the
    /// whole file.
    pub fn appends_in_place(&self) -> bool {
        if !self.backend.supports_append() {
            return false;
        }
        match self.format {
            FileFormat::JsonLines | FileFormat::RawText => !self.encrypted() || self.seals_lines(),
            FileFormat::Csv => cfg!(feature = "file-format-csv") && !self.encrypted(),
            _ => false,
        }
    }

    /// Whether an encryption key is configured.
    fn encrypted(&self) -> bool {
        #[cfg(feature = "encryption")]
        return self.encryption.is_some();
        #[cfg(not(feature = "encryption"))]
        false
    }

    /// Whether encryption seals each line (uncompressed line formats: the
    /// jsonl sink's layout, which stays appendable) rather than the whole
    /// file after compression.
    fn seals_lines(&self) -> bool {
        matches!(self.format, FileFormat::JsonLines | FileFormat::RawText)
            && self.codec == Compression::None
    }

    /// A fetched file's stored (compressed) bytes: whole-file sealing is
    /// undone; a per-line sealed file is returned as it is.
    fn unseal(&self, raw: Vec<u8>, area: Area, name: &str) -> Result<Vec<u8>, FaucetError> {
        #[cfg(feature = "encryption")]
        if let Some(enc) = self.encryption
            && faucet_core::encryption::is_encrypted(&raw)
        {
            return enc.decrypt(&raw).map_err(|e| {
                FaucetError::Sink(format!(
                    "file sink: decrypting '{}': {e}",
                    self.backend.describe(area, name)
                ))
            });
        }
        let _ = (area, name);
        Ok(raw)
    }

    /// Compress a finished body in place, for an encoder that does not write
    /// through the codec.
    #[cfg_attr(not(feature = "file-format-parquet"), allow(dead_code))]
    fn compress_file(&self, path: &Path) -> Result<(), FaucetError> {
        if self.codec == Compression::None {
            return Ok(());
        }
        let plain = std::fs::read(path).map_err(|e| io_err("reading", path, e))?;
        write_synced(
            path,
            &faucet_core::compress_buf(&plain, self.codec)?,
            self.sync,
        )
    }

    /// Undo the file-level codec on a fetched body.
    #[cfg_attr(not(feature = "file-format-parquet"), allow(dead_code))]
    pub fn decompress(&self, bytes: Vec<u8>) -> Result<Vec<u8>, FaucetError> {
        if self.codec == Compression::None {
            return Ok(bytes);
        }
        let mut out = Vec::new();
        std::io::Read::read_to_end(
            &mut faucet_core::compression::wrap_sync_reader(
                std::io::Cursor::new(bytes),
                self.codec,
            ),
            &mut out,
        )
        .map_err(|e| FaucetError::Sink(format!("file sink: decompressing: {e}")))?;
        Ok(out)
    }

    /// Seal a finished whole-file body in place.
    fn seal_file(&self, path: &Path) -> Result<(), FaucetError> {
        #[cfg(feature = "encryption")]
        if let Some(enc) = self.encryption
            && !self.seals_lines()
        {
            let plain = std::fs::read(path).map_err(|e| io_err("reading", path, e))?;
            write_synced(path, &enc.encrypt(&plain), self.sync)?;
        }
        let _ = path;
        Ok(())
    }

    /// A finalised file's bytes as plaintext (decrypting a sealed one),
    /// fetched through the backend via the scratch file `via`.
    #[cfg_attr(
        not(any(feature = "file-format-csv", feature = "file-format-parquet")),
        allow(dead_code)
    )]
    pub fn read_existing(
        &self,
        area: Area,
        name: &str,
        via: &Path,
    ) -> Result<Vec<u8>, FaucetError> {
        self.backend.fetch(area, name, via)?;
        let raw = std::fs::read(via).map_err(|e| io_err("opening", via, e));
        let _ = std::fs::remove_file(via);
        self.unseal(raw?, area, name)
    }
}

type LineWriter = SyncCompressWriter<BufWriter<File>>;

/// Format-specific state.
enum Enc {
    /// JSON Lines and raw text: encoded bytes streamed through the codec.
    /// `None` once finalised — the data then lives in the final file.
    Lines(Option<Box<LineWriter>>),
    /// CSV: rows in an uncompressed body file; the header is prepended when
    /// the file is finalised, so a later record can still add a column.
    #[cfg(feature = "file-format-csv")]
    Csv(Box<CsvState>),
    /// CSV of an encrypted output after a finalisation: the plaintext body is
    /// gone, and the next write reopens the sealed file.
    #[cfg(feature = "file-format-csv")]
    CsvSealed,
    /// Whole-document formats (JSON array, XML, Excel, Avro): the file's
    /// records, encoded together at each finalisation.
    Doc(Vec<Value>),
    /// Parquet row groups streamed to the temporary file.
    #[cfg(feature = "file-format-parquet")]
    Parquet(Box<super::parquet::ParquetState>),
}

/// One output file in progress.
pub(crate) struct OpenFile {
    /// The file's name in `area`.
    pub name: String,
    area: Area,
    tmp: PathBuf,
    /// Records in the file, including any carried over from an existing file.
    pub records: usize,
    /// Estimated size: the records' JSON length, before compression.
    pub bytes: usize,
    enc: Enc,
    /// Set by an I/O failure: the file can no longer be completed.
    poisoned: bool,
    /// Whether the final file holds everything written so far.
    finalized: bool,
    /// `mode: append` in place: the scratch file holds only this writer's new
    /// bytes, which a finalisation appends to the file under a lock.
    in_place: bool,
    /// A lock on the scratch file, held while the writer is alive so another
    /// writer's stale-scratch cleanup leaves it alone.
    _guard: Option<File>,
}

/// Create `tmp` if missing and hold an exclusive lock on it. Unix only: on
/// Windows a lock would also block this writer's own other handles.
fn lock_scratch(tmp: &Path) -> Option<File> {
    #[cfg(unix)]
    {
        let f = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(tmp)
            .ok()?;
        f.try_lock().ok().map(|()| f)
    }
    #[cfg(not(unix))]
    {
        let _ = tmp;
        None
    }
}

impl OpenFile {
    /// Start `name` in `area`, continuing from its current contents when
    /// `resume`.
    pub fn create(
        ctx: &Ctx<'_>,
        area: Area,
        name: String,
        resume: bool,
        in_place: bool,
    ) -> Result<Self, FaucetError> {
        let tmp = if in_place {
            ctx.backend.unique_scratch_path(area, &name)?
        } else {
            ctx.backend.scratch_path(area, &name)?
        };
        let guard = lock_scratch(&tmp);
        let carry = resume && !in_place;
        let mut file = Self {
            enc: Enc::Doc(Vec::new()),
            name,
            area,
            tmp,
            records: 0,
            bytes: 0,
            poisoned: false,
            finalized: false,
            in_place,
            _guard: guard,
        };
        file.enc = match ctx.format {
            FileFormat::JsonLines | FileFormat::RawText => {
                Enc::Lines(Some(open_lines(ctx, area, &file.name, &file.tmp, carry)?))
            }
            #[cfg(feature = "file-format-csv")]
            FileFormat::Csv => {
                let (state, carried) =
                    CsvState::create(ctx, area, &file.name, &file.tmp, carry, resume && in_place)?;
                file.records = carried;
                Enc::Csv(Box::new(state))
            }
            #[cfg(feature = "file-format-parquet")]
            FileFormat::Parquet => Enc::Parquet(Box::new(super::parquet::ParquetState::new())),
            _ => Enc::Doc(Vec::new()),
        };
        Ok(file)
    }

    /// Append `records`. Encoding happens before any byte reaches the disk, so
    /// an encoding error leaves the file as it was.
    pub fn write(&mut self, ctx: &Ctx<'_>, records: &[Value]) -> Result<(), FaucetError> {
        if self.poisoned {
            return Err(self.poison_error());
        }
        let result = self.write_inner(ctx, records);
        match &result {
            Ok(()) => {
                self.records += records.len();
                self.finalized = false;
            }
            Err(FaucetError::Sink(m)) if m.starts_with("file sink: ") => self.poisoned = true,
            Err(_) => {}
        }
        result
    }

    fn write_inner(&mut self, ctx: &Ctx<'_>, records: &[Value]) -> Result<(), FaucetError> {
        let tmp = self.tmp.clone();
        #[cfg(feature = "file-format-csv")]
        if matches!(self.enc, Enc::CsvSealed) {
            let (state, _) = CsvState::create(ctx, self.area, &self.name, &tmp, true, false)?;
            self.enc = Enc::Csv(Box::new(state));
        }
        match &mut self.enc {
            Enc::Lines(slot) => {
                let buf = ctx.line_bytes(records)?;
                if slot.is_none() {
                    *slot = Some(open_lines(ctx, self.area, &self.name, &tmp, !self.in_place)?);
                    if self._guard.is_none() {
                        self._guard = lock_scratch(&tmp);
                    }
                }
                let w = slot.as_mut().expect("opened above");
                w.write_all(&buf).map_err(|e| io_err("writing", &tmp, e))
            }
            #[cfg(feature = "file-format-csv")]
            Enc::Csv(state) => state.write(records, ctx.opts.csv.on_unknown_field),
            #[cfg(feature = "file-format-csv")]
            Enc::CsvSealed => Err(FaucetError::Sink(
                "csv: the sealed file was not reopened for writing".into(),
            )),
            Enc::Doc(buf) => {
                buf.extend_from_slice(records);
                Ok(())
            }
            #[cfg(feature = "file-format-parquet")]
            Enc::Parquet(state) => {
                let batch = state.batch_for(ctx.parquet, records)?;
                state.write(ctx, self.area, &self.name, &tmp, self.finalized, &batch)
            }
        }
    }

    /// Append an Arrow batch (Parquet only; other formats go through rows).
    #[cfg(feature = "file-format-parquet")]
    pub fn write_batch(
        &mut self,
        ctx: &Ctx<'_>,
        batch: &arrow::array::RecordBatch,
    ) -> Result<(), FaucetError> {
        if self.poisoned {
            return Err(self.poison_error());
        }
        let tmp = self.tmp.clone();
        let Enc::Parquet(state) = &mut self.enc else {
            return self.write(ctx, &faucet_core::columnar::record_batch_to_values(batch)?);
        };
        let result = state.write(ctx, self.area, &self.name, &tmp, self.finalized, batch);
        match &result {
            Ok(()) => {
                self.records += batch.num_rows();
                self.finalized = false;
            }
            Err(FaucetError::Sink(m)) if m.starts_with("file sink: ") => self.poisoned = true,
            Err(_) => {}
        }
        result
    }

    /// Make the final file hold everything written so far: write the
    /// temporary file completely, sync it, rename it over the final name.
    pub fn finalize(&mut self, ctx: &Ctx<'_>) -> Result<(), FaucetError> {
        if self.poisoned {
            return Err(self.poison_error());
        }
        if self.finalized {
            return Ok(());
        }
        let result = self.finalize_inner(ctx);
        match &result {
            Ok(()) => self.finalized = true,
            Err(_) => self.poisoned = true,
        }
        result
    }

    fn finalize_inner(&mut self, ctx: &Ctx<'_>) -> Result<(), FaucetError> {
        let tmp = self.tmp.clone();
        match &mut self.enc {
            Enc::Lines(slot) => {
                let Some(w) = slot.take() else {
                    return Ok(());
                };
                let buffered = w.finish().map_err(|e| io_err("finishing", &tmp, e))?;
                let file = buffered
                    .into_inner()
                    .map_err(|e| io_err("flushing", &tmp, e.into_error()))?;
                if self.in_place {
                    drop(file);
                    return append_file(ctx, self.area, &self.name, &tmp);
                }
                sync_if(&file, &tmp, ctx.sync)?;
            }
            #[cfg(feature = "file-format-csv")]
            Enc::Csv(state) if self.in_place => {
                return state.append_to(ctx, self.area, &self.name, &tmp);
            }
            #[cfg(feature = "file-format-csv")]
            Enc::Csv(state) => state.finish_into(ctx.codec, &tmp, ctx.sync)?,
            #[cfg(feature = "file-format-csv")]
            Enc::CsvSealed => return Ok(()),
            Enc::Doc(records) => {
                let body = faucet_core::file_format::encode(records, ctx.format, ctx.opts)?;
                let body = faucet_core::compress_buf(&body, ctx.codec)?;
                write_synced(&tmp, &body, ctx.sync)?;
            }
            #[cfg(feature = "file-format-parquet")]
            Enc::Parquet(state) => {
                if !state.close(&tmp, ctx.sync)? {
                    return Ok(());
                }
                ctx.compress_file(&tmp)?;
            }
        }
        ctx.seal_file(&tmp)?;
        ctx.backend.commit(&tmp, self.area, &self.name)?;
        self._guard = None;
        #[cfg(feature = "file-format-csv")]
        if ctx.encrypted() && matches!(self.enc, Enc::Csv(_)) {
            self.enc = Enc::CsvSealed;
            let _ = std::fs::remove_file(body_path(&tmp));
        }
        Ok(())
    }

    /// Remove the temporary files. The final file, if any, is untouched.
    pub fn discard(&mut self) {
        if let Enc::Lines(slot) = &mut self.enc {
            slot.take();
        }
        #[cfg(feature = "file-format-parquet")]
        if let Enc::Parquet(state) = &mut self.enc {
            state.abandon();
        }
        let _ = std::fs::remove_file(&self.tmp);
        let _ = std::fs::remove_file(body_path(&self.tmp));
    }

    fn poison_error(&self) -> FaucetError {
        FaucetError::Sink(format!(
            "file sink: an earlier write to '{}' failed, so it cannot be completed",
            self.name
        ))
    }
}

/// Append the finished scratch file `tmp` to `name` under the backend's
/// lock. A failed write is cut back off, so the file never keeps half of it.
fn append_file(ctx: &Ctx<'_>, area: Area, name: &str, tmp: &Path) -> Result<(), FaucetError> {
    let mut from = File::open(tmp).map_err(|e| io_err("opening", tmp, e))?;
    ctx.backend.append(area, name, &mut |f, dest| {
        use std::io::{Seek, SeekFrom};
        from.seek(SeekFrom::Start(0))
            .map_err(|e| io_err("reading", tmp, e))?;
        append_with(f, dest, |f| std::io::copy(&mut from, f).map(|_| ()))
    })
}

/// Run `write` at the end of the locked file `f`; on failure, truncate `f`
/// back to its length before, so a partial addition never stays.
pub(crate) fn append_with(
    f: &mut File,
    dest: &Path,
    write: impl FnOnce(&mut File) -> std::io::Result<()>,
) -> Result<super::backend::Appended, FaucetError> {
    use std::io::{Seek, SeekFrom};
    let len = f
        .seek(SeekFrom::End(0))
        .map_err(|e| io_err("seeking", dest, e))?;
    if let Err(e) = write(f).and_then(|()| f.flush()) {
        let _ = f.set_len(len);
        return Err(io_err("appending to", dest, e));
    }
    Ok(super::backend::Appended::InPlace)
}

/// The CSV body scratch file beside a scratch file.
pub(crate) fn body_path(tmp: &Path) -> PathBuf {
    tmp_path(tmp, "-body")
}

/// Open the temporary file for line output, copying the final file in first
/// when continuing it (a compressed file gains a new member).
fn open_lines(
    ctx: &Ctx<'_>,
    area: Area,
    name: &str,
    tmp: &Path,
    resume: bool,
) -> Result<Box<LineWriter>, FaucetError> {
    let carry = resume && ctx.backend.exists(area, name)?;
    if carry {
        ctx.backend.fetch(area, name, tmp)?;
        if ctx.encrypted() && !ctx.seals_lines() {
            let raw = std::fs::read(tmp).map_err(|e| io_err("reading", tmp, e))?;
            write_synced(tmp, &ctx.unseal(raw, area, name)?, ctx.sync)?;
        }
    }
    let codec = ctx.codec;
    let f = OpenOptions::new()
        .create(true)
        .write(true)
        .append(carry)
        .truncate(!carry)
        .open(tmp)
        .map_err(|e| io_err("creating", tmp, e))?;
    Ok(Box::new(sync_compress_writer(BufWriter::new(f), codec)))
}

/// Write `bytes` to `path`, and sync it when `sync`.
pub(crate) fn write_synced(path: &Path, bytes: &[u8], sync: bool) -> Result<(), FaucetError> {
    let mut f = File::create(path).map_err(|e| io_err("creating", path, e))?;
    f.write_all(bytes).map_err(|e| io_err("writing", path, e))?;
    sync_if(&f, path, sync)
}

/// Sync `f` to disk when `sync`.
pub(crate) fn sync_if(f: &File, path: &Path, sync: bool) -> Result<(), FaucetError> {
    if sync {
        f.sync_all().map_err(|e| io_err("syncing", path, e))?;
    }
    Ok(())
}

/// CSV rows waiting for their header.
#[cfg(feature = "file-format-csv")]
pub(crate) struct CsvState {
    body_path: PathBuf,
    body: csv::Writer<BufWriter<File>>,
    header: Vec<String>,
    known: std::collections::HashSet<String>,
    delimiter: u8,
    quote: u8,
    has_headers: bool,
    warned: std::collections::HashSet<String>,
    narrowest: usize,
}

#[cfg(feature = "file-format-csv")]
impl CsvState {
    /// Start a body file; with `resume`, carry the existing file's header and
    /// rows over; with `seed`, take only its header (an in-place appender).
    /// Returns the state and the number of carried rows.
    fn create(
        ctx: &Ctx<'_>,
        area: Area,
        name: &str,
        tmp: &Path,
        resume: bool,
        seed: bool,
    ) -> Result<(Self, usize), FaucetError> {
        let delimiter = ctx.opts.csv.delimiter_byte()?;
        let quote = ctx.opts.csv.quote_byte()?;
        let has_headers = ctx.opts.csv.has_headers;
        let body_path = body_path(tmp);
        let f = File::create(&body_path).map_err(|e| io_err("creating", &body_path, e))?;
        let body = csv::WriterBuilder::new()
            .delimiter(delimiter)
            .quote(quote)
            .flexible(true)
            .from_writer(BufWriter::new(f));
        let mut state = Self {
            body_path,
            body,
            header: Vec::new(),
            known: Default::default(),
            delimiter,
            quote,
            has_headers,
            warned: Default::default(),
            narrowest: usize::MAX,
        };
        let mut carried = 0;
        if seed
            && has_headers
            && let Some(path) = ctx.backend.local_path(area, name)
            && let Ok(f) = File::open(&path)
            && let Some(h) = first_record(f, ctx.codec, delimiter, quote, &path)?
        {
            for c in &h {
                state.add_column(c);
            }
        }
        if resume && ctx.backend.exists(area, name)? {
            let existing =
                std::io::Cursor::new(ctx.read_existing(area, name, &tmp_path(tmp, "-old"))?);
            let reader = faucet_core::compression::wrap_sync_reader(existing, ctx.codec);
            let mut rdr = csv::ReaderBuilder::new()
                .has_headers(false)
                .flexible(true)
                .delimiter(delimiter)
                .quote(quote)
                .from_reader(reader);
            let mut first = has_headers;
            for rec in rdr.records() {
                let rec = rec.map_err(|e| {
                    FaucetError::Sink(format!(
                        "file sink: reading existing '{}': {e}",
                        ctx.backend.describe(area, name)
                    ))
                })?;
                if first {
                    first = false;
                    for h in &rec {
                        state.add_column(h);
                    }
                    continue;
                }
                state
                    .body
                    .write_record(&rec)
                    .map_err(|e| csv_err(&state.body_path, e))?;
                state.narrowest = state.narrowest.min(rec.len());
                carried += 1;
            }
        }
        Ok((state, carried))
    }

    fn add_column(&mut self, name: &str) {
        if self.known.insert(name.to_string()) {
            self.header.push(name.to_string());
        }
    }

    fn write(
        &mut self,
        records: &[Value],
        on_unknown: faucet_core::CsvUnknownField,
    ) -> Result<(), FaucetError> {
        use faucet_core::CsvUnknownField;
        for (i, r) in records.iter().enumerate() {
            if !r.is_object() {
                return Err(FaucetError::Sink(format!(
                    "csv: record {i} of the page is not an object, so it has no columns"
                )));
            }
        }
        let frozen = on_unknown != CsvUnknownField::Widen && !self.header.is_empty();
        let unknown: Vec<String> = faucet_core::file_format::header_union(records)
            .into_iter()
            .filter(|k| !self.known.contains(k))
            .collect();
        if frozen && !unknown.is_empty() {
            if on_unknown == CsvUnknownField::Error {
                return Err(FaucetError::Sink(format!(
                    "csv: record field(s) not in the header and `csv.on_unknown_field: error`: \
                     [{}] — the header is fixed from the first page, so these values would be \
                     dropped",
                    unknown.join(", ")
                )));
            }
            for k in unknown.iter().filter(|k| self.warned.insert((*k).clone())) {
                tracing::warn!(
                    field = %k,
                    "file sink: csv dropping a field that is not in the header \
                     (`csv.on_unknown_field: warn`)"
                );
            }
        } else {
            for key in unknown {
                self.add_column(&key);
            }
        }
        for r in records {
            let row: Vec<String> = self
                .header
                .iter()
                .map(|h| {
                    r.get(h)
                        .map(faucet_core::file_format::cell_text)
                        .unwrap_or_default()
                })
                .collect();
            self.body
                .write_record(&row)
                .map_err(|e| csv_err(&self.body_path, e))?;
            self.narrowest = self.narrowest.min(row.len());
        }
        Ok(())
    }

    /// Write header + body to `tmp` through the codec.
    fn finish_into(
        &mut self,
        codec: Compression,
        tmp: &Path,
        sync: bool,
    ) -> Result<(), FaucetError> {
        self.body
            .flush()
            .map_err(|e| io_err("flushing", &self.body_path, e))?;
        let f = File::create(tmp).map_err(|e| io_err("creating", tmp, e))?;
        let mut out = sync_compress_writer(BufWriter::new(f), codec);
        if self.has_headers && !self.header.is_empty() {
            let mut w = csv::WriterBuilder::new()
                .delimiter(self.delimiter)
                .quote(self.quote)
                .from_writer(Vec::new());
            w.write_record(&self.header).map_err(|e| csv_err(tmp, e))?;
            let line = w
                .into_inner()
                .map_err(|e| FaucetError::Sink(format!("file sink: csv header: {e}")))?;
            out.write_all(&line)
                .map_err(|e| io_err("writing", tmp, e))?;
        }
        let mut body =
            File::open(&self.body_path).map_err(|e| io_err("opening", &self.body_path, e))?;
        if self.narrowest < self.header.len() {
            self.pad_rows(body, &mut out, tmp)?;
        } else {
            std::io::copy(&mut body, &mut out).map_err(|e| io_err("writing", tmp, e))?;
        }
        let buffered = out.finish().map_err(|e| io_err("finishing", tmp, e))?;
        let file = buffered
            .into_inner()
            .map_err(|e| io_err("flushing", tmp, e.into_error()))?;
        sync_if(&file, tmp, sync)
    }
}

#[cfg(feature = "file-format-csv")]
impl CsvState {
    /// Append the rows written so far to `name` under the backend's lock and
    /// start an empty body. The header is decided under the lock: the first
    /// writer of an empty or missing file writes it; later writers map their
    /// rows onto the file's header, and a column it lacks is dropped
    /// (`on_unknown_field: warn`), refused (`error`), or added by rewriting
    /// the file (`widen`).
    fn append_to(
        &mut self,
        ctx: &Ctx<'_>,
        area: Area,
        name: &str,
        tmp: &Path,
    ) -> Result<(), FaucetError> {
        use faucet_core::CsvUnknownField;
        use std::io::{Seek, SeekFrom};
        self.body
            .flush()
            .map_err(|e| io_err("flushing", &self.body_path, e))?;
        let codec = ctx.codec;
        let on_unknown = ctx.opts.csv.on_unknown_field;
        let mut final_header: Option<Vec<String>> = None;
        let mut dropped: Vec<String> = Vec::new();
        let this = &*self;
        ctx.backend.append(area, name, &mut |f, dest| {
            let len = f.metadata().map_err(|e| io_err("reading", dest, e))?.len();
            let existing = if this.has_headers && len > 0 {
                f.seek(SeekFrom::Start(0))
                    .map_err(|e| io_err("reading", dest, e))?;
                first_record(&mut *f, codec, this.delimiter, this.quote, dest)?
            } else {
                None
            };
            let Some(file_header) = existing else {
                let with_header = this.has_headers && len == 0;
                final_header = Some(this.header.clone());
                return append_with(f, dest, |f| {
                    this.write_member(f, codec, with_header, &this.header)
                });
            };
            let missing: Vec<String> = this
                .header
                .iter()
                .filter(|c| !file_header.contains(c))
                .cloned()
                .collect();
            if missing.is_empty() || on_unknown == CsvUnknownField::Warn {
                dropped = missing;
                final_header = Some(file_header.clone());
                return append_with(f, dest, |f| {
                    this.write_member(f, codec, false, &file_header)
                });
            }
            if on_unknown == CsvUnknownField::Error {
                return Err(FaucetError::Sink(format!(
                    "csv: record field(s) not in the header of '{}' and \
                     `csv.on_unknown_field: error`: [{}]",
                    dest.display(),
                    missing.join(", ")
                )));
            }
            let mut wide = file_header.clone();
            wide.extend(missing.iter().cloned());
            let out = tmp_path(tmp, "-wide");
            this.rewrite_widened(f, dest, codec, &wide, &out)?;
            final_header = Some(wide);
            Ok(super::backend::Appended::Replace(out))
        })?;
        for k in dropped {
            if self.warned.insert(k.clone()) {
                tracing::warn!(
                    field = %k,
                    "file sink: csv dropping a field that is not in the appended file's \
                     header (`csv.on_unknown_field: warn`)"
                );
            }
        }
        if let Some(h) = final_header {
            self.known = h.iter().cloned().collect();
            self.header = h;
        }
        let f = File::create(&self.body_path).map_err(|e| io_err("creating", &self.body_path, e))?;
        self.body = csv::WriterBuilder::new()
            .delimiter(self.delimiter)
            .quote(self.quote)
            .flexible(true)
            .from_writer(BufWriter::new(f));
        self.narrowest = usize::MAX;
        Ok(())
    }

    /// Write the body's rows to `f` as one compression member, mapped onto
    /// the columns `target` (a column the body lacks is empty), with the
    /// header first when `with_header`.
    fn write_member(
        &self,
        f: &mut File,
        codec: Compression,
        with_header: bool,
        target: &[String],
    ) -> std::io::Result<()> {
        let mut out = sync_compress_writer(BufWriter::new(&mut *f), codec);
        self.write_rows(&mut out, with_header, target)?;
        out.finish()?.flush()
    }

    fn write_rows(&self, out: &mut impl Write, with_header: bool, target: &[String]) -> std::io::Result<()> {
        let mut w = csv::WriterBuilder::new()
            .delimiter(self.delimiter)
            .quote(self.quote)
            .from_writer(out);
        if with_header && !target.is_empty() {
            w.write_record(target)?;
        }
        let index: Vec<Option<usize>> = target
            .iter()
            .map(|c| self.header.iter().position(|h| h == c))
            .collect();
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .delimiter(self.delimiter)
            .quote(self.quote)
            .from_reader(std::io::BufReader::new(File::open(&self.body_path)?));
        for rec in rdr.records() {
            let rec = rec?;
            let row: Vec<&str> = index
                .iter()
                .map(|i| i.and_then(|i| rec.get(i)).unwrap_or(""))
                .collect();
            w.write_record(&row)?;
        }
        w.flush()
    }

    /// The locked file's rows padded to `wide`, then the body's, all under
    /// the `wide` header, written to `out`.
    fn rewrite_widened(
        &self,
        f: &mut File,
        dest: &Path,
        codec: Compression,
        wide: &[String],
        out: &Path,
    ) -> Result<(), FaucetError> {
        use std::io::{Seek, SeekFrom};
        f.seek(SeekFrom::Start(0))
            .map_err(|e| io_err("reading", dest, e))?;
        let reader = faucet_core::compression::wrap_sync_reader(&mut *f, codec);
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(true)
            .flexible(true)
            .delimiter(self.delimiter)
            .quote(self.quote)
            .from_reader(reader);
        let file = File::create(out).map_err(|e| io_err("creating", out, e))?;
        let mut enc = sync_compress_writer(BufWriter::new(file), codec);
        {
            let mut w = csv::WriterBuilder::new()
                .delimiter(self.delimiter)
                .quote(self.quote)
                .from_writer(&mut enc);
            w.write_record(wide).map_err(|e| csv_err(out, e))?;
            for rec in rdr.records() {
                let rec = rec.map_err(|e| csv_err(dest, e))?;
                let mut row: Vec<&str> = rec.iter().collect();
                row.resize(wide.len().max(row.len()), "");
                w.write_record(&row).map_err(|e| csv_err(out, e))?;
            }
            w.flush().map_err(|e| io_err("writing", out, e))?;
        }
        self.write_rows(&mut enc, false, wide)
            .map_err(|e| io_err("writing", out, e))?;
        let file = enc
            .finish()
            .map_err(|e| io_err("finishing", out, e))?
            .into_inner()
            .map_err(|e| io_err("flushing", out, e.into_error()))?;
        sync_if(&file, out, true)
    }

    /// Copy the body, padding rows written before the header widened with
    /// empty cells, so every row has one cell per column.
    fn pad_rows(&self, body: File, out: &mut impl Write, tmp: &Path) -> Result<(), FaucetError> {
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .delimiter(self.delimiter)
            .quote(self.quote)
            .from_reader(std::io::BufReader::new(body));
        let mut w = csv::WriterBuilder::new()
            .delimiter(self.delimiter)
            .quote(self.quote)
            .from_writer(out);
        let width = self.header.len();
        for rec in rdr.records() {
            let rec = rec.map_err(|e| csv_err(&self.body_path, e))?;
            let mut row: Vec<&str> = rec.iter().collect();
            row.resize(width.max(row.len()), "");
            w.write_record(&row).map_err(|e| csv_err(tmp, e))?;
        }
        w.flush().map_err(|e| io_err("writing", tmp, e))
    }
}

/// The first record of a CSV stream (its header), through the codec.
#[cfg(feature = "file-format-csv")]
fn first_record(
    r: impl std::io::Read + Send,
    codec: Compression,
    delimiter: u8,
    quote: u8,
    path: &Path,
) -> Result<Option<Vec<String>>, FaucetError> {
    let reader = faucet_core::compression::wrap_sync_reader(r, codec);
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .delimiter(delimiter)
        .quote(quote)
        .from_reader(reader);
    match rdr.records().next() {
        None => Ok(None),
        Some(rec) => Ok(Some(
            rec.map_err(|e| csv_err(path, e))?
                .iter()
                .map(str::to_string)
                .collect(),
        )),
    }
}

#[cfg(feature = "file-format-csv")]
fn csv_err(path: &Path, e: csv::Error) -> FaucetError {
    FaucetError::Sink(format!("file sink: writing '{}': {e}", path.display()))
}
