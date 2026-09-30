//! One output file: encoded into a local scratch file (or, for a line format
//! on a store that takes parts, into parts uploaded as it grows) and
//! published by the backend when it is closed, so a reader sees a complete
//! file or none at all.
//!
//! Everything here is synchronous: the writer calls it from a blocking
//! context and does the storage I/O (fetching an existing file, publishing,
//! uploading parts) itself, as futures.

use super::backend::{Area, PartStream};
use super::layout::{BODY_ROLE, OLD_ROLE, PREV_ROLE, SEAL_ROLE, io_err, tmp_path};
use super::options::{JsonLinesOptions, ParquetCodec, ParquetOptions};
use faucet_core::compression::{SyncCompressWriter, sync_compress_writer};
use faucet_core::{Compression, FaucetError, FileFormat, FormatOptions};
use serde_json::Value;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// A failed write, by what it left behind.
#[derive(Debug)]
pub(crate) enum Failure {
    /// Nothing reached the file: it is exactly as it was before the call.
    Clean(FaucetError),
    /// The file may hold part of the call's data and cannot be completed.
    Dirty(FaucetError),
}

fn dirty(e: FaucetError) -> Failure {
    Failure::Dirty(e)
}

fn clean(e: FaucetError) -> Failure {
    Failure::Clean(e)
}

/// The per-output constants every file needs.
pub(crate) struct Ctx<'a> {
    pub format: FileFormat,
    pub codec: Compression,
    pub opts: &'a FormatOptions,
    #[cfg_attr(not(feature = "file-format-parquet"), allow(dead_code))]
    pub parquet: &'a ParquetOptions,
    #[cfg_attr(not(feature = "file-format-parquet"), allow(dead_code))]
    pub parquet_codec: ParquetCodec,
    pub json_lines: &'a JsonLinesOptions,
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

    /// Whether an encryption key is configured.
    pub fn encrypted(&self) -> bool {
        #[cfg(feature = "encryption")]
        return self.encryption.is_some();
        #[cfg(not(feature = "encryption"))]
        false
    }

    /// Whether encryption seals each line (uncompressed line formats: the
    /// jsonl sink's layout, which stays appendable) rather than the whole
    /// file after compression.
    pub fn seals_lines(&self) -> bool {
        matches!(self.format, FileFormat::JsonLines | FileFormat::RawText)
            && self.codec == Compression::None
    }

    /// Whether scratch files hold plaintext of an encrypted output, so they
    /// are created readable by the owner only.
    pub(crate) fn private(&self) -> bool {
        self.encrypted() && !self.seals_lines()
    }

    /// A stored file's bytes (as compressed) with whole-file sealing undone.
    /// Refuses a file whose sealing does not match the config, which would
    /// otherwise be mixed with differently sealed data.
    fn unseal(&self, raw: Vec<u8>, label: &str) -> Result<Vec<u8>, FaucetError> {
        #[cfg(feature = "encryption")]
        {
            let sealed = faucet_core::encryption::is_encrypted(&raw);
            match self.encryption {
                Some(enc) if sealed => {
                    return enc.decrypt(&raw).map_err(|e| {
                        FaucetError::Sink(format!("file sink: decrypting '{label}': {e}"))
                    });
                }
                Some(_) if !raw.is_empty() => return Err(mismatch(label, false)),
                None if sealed => return Err(mismatch(label, true)),
                _ => {}
            }
        }
        let _ = label;
        Ok(raw)
    }

    /// Refuse to add per-line sealed records to a file whose lines are not
    /// sealed, and plaintext records to one whose lines are.
    fn check_line_sealing(&self, existing: &Path, label: &str) -> Result<(), FaucetError> {
        #[cfg(feature = "encryption")]
        {
            use std::io::BufRead as _;
            let file = File::open(existing).map_err(|e| io_err("opening", existing, e))?;
            let mut first = String::new();
            std::io::BufReader::new(file)
                .read_line(&mut first)
                .map_err(|e| io_err("reading", existing, e))?;
            let first = first.trim_end();
            if first.is_empty() {
                return Ok(());
            }
            let sealed = line_is_sealed(first);
            if self.encryption.is_some() != sealed {
                return Err(mismatch(label, sealed));
            }
        }
        let _ = (existing, label);
        Ok(())
    }

    /// Refuse a stored file that is sealed whole when no `encryption` block
    /// is set. Reads only the file's first bytes.
    fn check_unsealed(&self, existing: &Path, label: &str) -> Result<(), FaucetError> {
        #[cfg(feature = "encryption")]
        {
            use std::io::Read as _;
            let mut head = Vec::with_capacity(64);
            File::open(existing)
                .and_then(|f| f.take(64).read_to_end(&mut head))
                .map_err(|e| io_err("reading", existing, e))?;
            if faucet_core::encryption::is_encrypted(&head) {
                return Err(mismatch(label, true));
            }
        }
        let _ = (existing, label);
        Ok(())
    }

    /// Read a stored file: `(compressed) bytes`, whole-file sealing undone.
    pub(crate) fn read_stored(&self, path: &Path, label: &str) -> Result<Vec<u8>, FaucetError> {
        let raw = std::fs::read(path).map_err(|e| io_err("reading", path, e))?;
        self.unseal(raw, label)
    }

    /// Compress a finished body in place, for an encoder that does not write
    /// through the codec.
    #[cfg_attr(not(feature = "file-format-parquet"), allow(dead_code))]
    fn compress_file(&self, path: &Path) -> Result<(), FaucetError> {
        if self.codec == Compression::None {
            return Ok(());
        }
        let plain = std::fs::read(path).map_err(|e| io_err("reading", path, e))?;
        let packed = faucet_core::compress_buf(&plain, self.codec)?;
        write_new(&tmp_path(path, SEAL_ROLE), &packed, self.private())?;
        rename(&tmp_path(path, SEAL_ROLE), path)
    }

    /// Undo the file-level codec on stored bytes.
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

    /// Seal a finished whole-file body. The sealed copy is written to a new
    /// file with the default permissions and renamed over the plaintext one.
    fn seal_file(&self, path: &Path) -> Result<(), FaucetError> {
        #[cfg(feature = "encryption")]
        if let Some(enc) = self.encryption
            && !self.seals_lines()
        {
            let plain = std::fs::read(path).map_err(|e| io_err("reading", path, e))?;
            let sealed = tmp_path(path, SEAL_ROLE);
            write_new(&sealed, &enc.encrypt(&plain), false)?;
            rename(&sealed, path)?;
        }
        let _ = path;
        Ok(())
    }
}

#[cfg(feature = "encryption")]
fn line_is_sealed(line: &str) -> bool {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(line)
        .is_ok_and(|b| faucet_core::encryption::is_encrypted(&b))
}

#[cfg(feature = "encryption")]
fn mismatch(label: &str, stored_sealed: bool) -> FaucetError {
    let (is, set) = if stored_sealed {
        ("encrypted", "not set")
    } else {
        ("not encrypted", "set")
    };
    FaucetError::Sink(format!(
        "file sink: '{label}' is {is} but `encryption` is {set}, so appending to it would mix \
         sealed and plaintext data — write to a new file, or match the `encryption` block to \
         the existing file"
    ))
}

/// Create (truncating) a scratch file, readable by the owner only when
/// `private`.
pub(crate) fn create_scratch(path: &Path, private: bool) -> Result<File, FaucetError> {
    open_scratch(path, private, false)
}

fn open_scratch(path: &Path, private: bool, append: bool) -> Result<File, FaucetError> {
    let mut o = OpenOptions::new();
    o.create(true).write(true).append(append).truncate(!append);
    #[cfg(unix)]
    if private {
        use std::os::unix::fs::OpenOptionsExt;
        o.mode(0o600);
    }
    #[cfg(not(unix))]
    let _ = private;
    o.open(path).map_err(|e| io_err("creating", path, e))
}

/// Write `bytes` to a new file at `path`.
fn write_new(path: &Path, bytes: &[u8], private: bool) -> Result<(), FaucetError> {
    create_scratch(path, private)?
        .write_all(bytes)
        .map_err(|e| io_err("writing", path, e))
}

fn rename(from: &Path, to: &Path) -> Result<(), FaucetError> {
    std::fs::rename(from, to).map_err(|e| io_err("renaming", from, e))
}

/// Parts of a streamed file waiting to be uploaded.
pub(crate) type Parts = Arc<Mutex<Vec<Vec<u8>>>>;

/// Where a line file's bytes go: a scratch file, or parts of `size` bytes.
enum LineOut {
    File(File),
    Spool {
        size: usize,
        buf: Vec<u8>,
        parts: Parts,
    },
}

impl LineOut {
    /// Hand the last, possibly short, part over.
    fn close(self) {
        if let Self::Spool { buf, parts, .. } = self
            && !buf.is_empty()
        {
            parts.lock().unwrap_or_else(|p| p.into_inner()).push(buf);
        }
    }
}

impl Write for LineOut {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::File(f) => f.write(data),
            Self::Spool { size, buf, parts } => {
                buf.extend_from_slice(data);
                if buf.len() >= *size {
                    let mut full = Vec::new();
                    while buf.len() >= *size {
                        let rest = buf.split_off(*size);
                        full.push(std::mem::replace(buf, rest));
                    }
                    parts.lock().unwrap_or_else(|p| p.into_inner()).extend(full);
                }
                Ok(data.len())
            }
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::File(f) => f.flush(),
            Self::Spool { .. } => Ok(()),
        }
    }
}

type LineWriter = SyncCompressWriter<BufWriter<LineOut>>;

/// Format-specific state.
enum Enc {
    /// JSON Lines and raw text: encoded bytes streamed through the codec.
    /// `None` once closed — the data then lives in the published file.
    Lines(Option<Box<LineWriter>>),
    /// CSV: rows in an uncompressed body file; the header is prepended when
    /// the file is closed, so a later record can still add a column.
    #[cfg(feature = "file-format-csv")]
    Csv(Box<CsvState>),
    /// CSV of an encrypted output after it was published: the plaintext body
    /// is gone, and the next write reopens the sealed file.
    #[cfg(feature = "file-format-csv")]
    CsvSealed,
    /// Whole-document formats (JSON array, XML, Excel, Avro): the file's
    /// records, encoded together each time the file is closed.
    Doc(Vec<Value>),
    /// Parquet row groups streamed to the scratch file.
    #[cfg(feature = "file-format-parquet")]
    Parquet(Box<super::parquet::ParquetState>),
}

/// What closing a file left to publish.
pub(crate) enum Closed {
    /// Nothing changed since it was last published.
    Nothing,
    /// The finished file at the scratch path.
    File,
    /// The parts of a streamed file not yet uploaded.
    Parts(Vec<Vec<u8>>),
}

/// One output file in progress.
pub(crate) struct OpenFile {
    /// The file's name in `area`.
    pub name: String,
    pub area: Area,
    /// Where the file lives, for messages.
    pub label: String,
    pub tmp: PathBuf,
    /// Records in the file, including any carried over from an existing file.
    pub records: usize,
    /// Estimated size: the records' JSON length, before compression.
    pub bytes: usize,
    enc: Enc,
    /// Whether the published file holds everything written so far.
    pub published: bool,
    /// A local copy of the last published version, to continue from.
    pub prev: Option<PathBuf>,
    /// The writer call that opened the file.
    pub first_call: u64,
    /// Whether the file holds data from before it was opened in this call:
    /// carried over from an existing file, or published earlier in the run.
    pub continued: bool,
    /// Parts of a streamed file, filled as it is encoded.
    parts: Option<Parts>,
    /// The upload a streamed file's parts go to, once the first part is full.
    pub stream: Option<Box<dyn PartStream>>,
}

impl OpenFile {
    /// Start `name` in `area`, carrying over the stored file at `existing`
    /// (a local copy) when given. With `part_size`, a line format is
    /// encoded into parts instead of a scratch file.
    pub fn create(
        ctx: &Ctx<'_>,
        area: Area,
        name: String,
        label: String,
        tmp: PathBuf,
        existing: Option<&Path>,
        part_size: Option<usize>,
    ) -> Result<Self, FaucetError> {
        let mut file = Self {
            enc: Enc::Doc(Vec::new()),
            name,
            area,
            label,
            tmp,
            records: 0,
            bytes: 0,
            published: false,
            prev: None,
            first_call: 0,
            continued: existing.is_some(),
            parts: None,
            stream: None,
        };
        file.enc = match ctx.format {
            FileFormat::JsonLines | FileFormat::RawText => {
                let spool = match (existing, part_size) {
                    (None, Some(size)) => {
                        let parts = Parts::default();
                        file.parts = Some(parts.clone());
                        Some((size, parts))
                    }
                    _ => None,
                };
                Enc::Lines(Some(open_lines(
                    ctx,
                    &file.label,
                    &file.tmp,
                    existing,
                    spool,
                )?))
            }
            #[cfg(feature = "file-format-csv")]
            FileFormat::Csv => {
                let (state, carried) = CsvState::create(ctx, &file.label, &file.tmp, existing)?;
                file.records = carried;
                Enc::Csv(Box::new(state))
            }
            #[cfg(feature = "file-format-parquet")]
            FileFormat::Parquet => Enc::Parquet(Box::new(super::parquet::ParquetState::new())),
            _ => Enc::Doc(Vec::new()),
        };
        Ok(file)
    }

    /// Whether the next write continues from the published file, so the
    /// caller must pass a local copy of it.
    pub fn needs_existing(&self) -> bool {
        if !self.published {
            return false;
        }
        match &self.enc {
            Enc::Lines(slot) => slot.is_none(),
            #[cfg(feature = "file-format-csv")]
            Enc::CsvSealed => true,
            #[cfg(feature = "file-format-parquet")]
            Enc::Parquet(state) => !state.is_open(),
            _ => false,
        }
    }

    /// Whether losing the file now would lose data from before `call`, which
    /// a caller has already been told was written.
    pub fn at_risk(&self, call: u64) -> bool {
        self.continued || self.first_call < call
    }

    /// Take the full parts encoded so far.
    pub fn take_parts(&self) -> Vec<Vec<u8>> {
        self.parts.as_ref().map_or_else(Vec::new, |p| {
            std::mem::take(&mut *p.lock().unwrap_or_else(|p| p.into_inner()))
        })
    }

    /// Append `records`. Encoding happens before any byte reaches the file,
    /// so an encoding error is [`Failure::Clean`]. `existing` is a local copy
    /// of the published file when [`needs_existing`](Self::needs_existing).
    pub fn write(
        &mut self,
        ctx: &Ctx<'_>,
        records: &[Value],
        existing: Option<&Path>,
    ) -> Result<(), Failure> {
        let tmp = self.tmp.clone();
        match &mut self.enc {
            Enc::Lines(slot) => {
                let buf = ctx.line_bytes(records).map_err(clean)?;
                if slot.is_none() {
                    *slot =
                        Some(open_lines(ctx, &self.label, &tmp, existing, None).map_err(clean)?);
                }
                let w = slot.as_mut().expect("opened above");
                w.write_all(&buf)
                    .map_err(|e| dirty(io_err("writing", &tmp, e)))?;
            }
            #[cfg(feature = "file-format-csv")]
            Enc::Csv(state) => state.write(records, ctx.opts.csv.on_unknown_field)?,
            #[cfg(feature = "file-format-csv")]
            Enc::CsvSealed => {
                let (mut state, _) =
                    CsvState::create(ctx, &self.label, &tmp, existing).map_err(clean)?;
                let written = state.write(records, ctx.opts.csv.on_unknown_field);
                self.enc = Enc::Csv(Box::new(state));
                written?;
            }
            Enc::Doc(buf) => buf.extend_from_slice(records),
            #[cfg(feature = "file-format-parquet")]
            Enc::Parquet(state) => {
                let batch = state.batch_for(ctx.parquet, records).map_err(clean)?;
                state.write(ctx, &self.label, &tmp, existing, &batch)?;
            }
        }
        self.records += records.len();
        self.published = false;
        Ok(())
    }

    /// Append an Arrow batch (Parquet only; other formats go through rows).
    #[cfg(feature = "file-format-parquet")]
    pub fn write_batch(
        &mut self,
        ctx: &Ctx<'_>,
        batch: &arrow::array::RecordBatch,
        existing: Option<&Path>,
    ) -> Result<(), Failure> {
        let tmp = self.tmp.clone();
        let Enc::Parquet(state) = &mut self.enc else {
            let rows = faucet_core::columnar::record_batch_to_values(batch).map_err(clean)?;
            return self.write(ctx, &rows, existing);
        };
        state.write(ctx, &self.label, &tmp, existing, batch)?;
        self.records += batch.num_rows();
        self.published = false;
        Ok(())
    }

    /// Finish the file so it holds everything written so far: the complete
    /// scratch file, compressed and sealed, or the remaining parts.
    pub fn close(&mut self, ctx: &Ctx<'_>) -> Result<Closed, FaucetError> {
        if self.published {
            return Ok(Closed::Nothing);
        }
        let tmp = self.tmp.clone();
        match &mut self.enc {
            Enc::Lines(slot) => {
                let Some(w) = slot.take() else {
                    return Ok(Closed::Nothing);
                };
                let buffered = w.finish().map_err(|e| io_err("finishing", &tmp, e))?;
                let out = buffered
                    .into_inner()
                    .map_err(|e| io_err("flushing", &tmp, e.into_error()))?;
                out.close();
                if self.parts.is_some() {
                    return Ok(Closed::Parts(self.take_parts()));
                }
            }
            #[cfg(feature = "file-format-csv")]
            Enc::Csv(state) => state.finish_into(ctx.codec, &tmp, ctx.private())?,
            #[cfg(feature = "file-format-csv")]
            Enc::CsvSealed => return Ok(Closed::Nothing),
            Enc::Doc(records) => {
                let body = faucet_core::file_format::encode(records, ctx.format, ctx.opts)?;
                let body = faucet_core::compress_buf(&body, ctx.codec)?;
                write_new(&tmp, &body, ctx.private())?;
            }
            #[cfg(feature = "file-format-parquet")]
            Enc::Parquet(state) => {
                if !state.close(&tmp)? {
                    return Ok(Closed::Nothing);
                }
                ctx.compress_file(&tmp)?;
            }
        }
        ctx.seal_file(&tmp)?;
        Ok(Closed::File)
    }

    /// Write a streamed file's only part to the scratch file, to publish it
    /// whole.
    pub fn spill(&self, part: &[u8]) -> Result<(), FaucetError> {
        write_new(&self.tmp, part, false)
    }

    /// Keep a local copy of the scratch file before it is published, to
    /// continue from without downloading it. Best effort: without it the
    /// next write fetches the published file.
    pub fn keep_copy(&mut self) {
        let prev = tmp_path(&self.tmp, PREV_ROLE);
        let _ = std::fs::remove_file(&prev);
        self.prev = std::fs::hard_link(&self.tmp, &prev).ok().map(|()| prev);
    }

    /// The file was published: it holds everything written so far.
    pub fn mark_published(&mut self, ctx: &Ctx<'_>) {
        self.published = true;
        self.continued = true;
        self.parts = None;
        #[cfg(feature = "file-format-csv")]
        if ctx.encrypted() && matches!(self.enc, Enc::Csv(_)) {
            self.enc = Enc::CsvSealed;
            let _ = std::fs::remove_file(tmp_path(&self.tmp, BODY_ROLE));
        }
        let _ = ctx;
    }

    /// The local file `fetch` copies the published file into.
    pub fn fetch_path(&self) -> PathBuf {
        tmp_path(&self.tmp, OLD_ROLE)
    }

    /// Remove the scratch files. The published file, if any, is untouched.
    /// A part upload still open is left for the caller to abort.
    pub fn discard(&mut self) {
        if let Enc::Lines(slot) = &mut self.enc {
            slot.take();
        }
        #[cfg(feature = "file-format-parquet")]
        if let Enc::Parquet(state) = &mut self.enc {
            state.abandon();
        }
        let _ = std::fs::remove_file(&self.tmp);
        for role in [BODY_ROLE, OLD_ROLE, SEAL_ROLE, PREV_ROLE] {
            let _ = std::fs::remove_file(tmp_path(&self.tmp, role));
        }
    }
}

/// Open the scratch output for a line file, carrying the stored file at
/// `existing` over first (a compressed file gains a new member). With
/// `spool`, bytes go to parts instead.
fn open_lines(
    ctx: &Ctx<'_>,
    label: &str,
    tmp: &Path,
    existing: Option<&Path>,
    spool: Option<(usize, Parts)>,
) -> Result<Box<LineWriter>, FaucetError> {
    if let Some((size, parts)) = spool {
        let out = LineOut::Spool {
            size,
            buf: Vec::new(),
            parts,
        };
        return Ok(Box::new(sync_compress_writer(
            BufWriter::new(out),
            ctx.codec,
        )));
    }
    if let Some(existing) = existing {
        if ctx.seals_lines() {
            ctx.check_line_sealing(existing, label)?;
            copy_or_move(existing, tmp)?;
        } else if ctx.encrypted() {
            let stored = ctx.read_stored(existing, label);
            remove_fetched(existing);
            write_new(tmp, &stored?, ctx.private())?;
        } else {
            ctx.check_unsealed(existing, label)?;
            copy_or_move(existing, tmp)?;
        }
    }
    let f = open_scratch(tmp, ctx.private(), existing.is_some())?;
    Ok(Box::new(sync_compress_writer(
        BufWriter::new(LineOut::File(f)),
        ctx.codec,
    )))
}

/// Make `to` a copy of `from`: a rename when `from` is a fetched copy, a
/// copy when it is the kept copy of the published file.
fn copy_or_move(from: &Path, to: &Path) -> Result<(), FaucetError> {
    if is_fetched(from) {
        rename(from, to)
    } else {
        std::fs::copy(from, to)
            .map(|_| ())
            .map_err(|e| io_err("copying", from, e))
    }
}

fn is_fetched(path: &Path) -> bool {
    path.to_string_lossy().ends_with(OLD_ROLE)
}

/// Remove `path` if it is a fetched copy (the kept copy stays).
pub(crate) fn remove_fetched(path: &Path) {
    if is_fetched(path) {
        let _ = std::fs::remove_file(path);
    }
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
    /// Start a body file; with `existing`, carry the stored file's header and
    /// rows over. Returns the state and the number of carried rows.
    fn create(
        ctx: &Ctx<'_>,
        label: &str,
        tmp: &Path,
        existing: Option<&Path>,
    ) -> Result<(Self, usize), FaucetError> {
        let delimiter = ctx.opts.csv.delimiter_byte()?;
        let quote = ctx.opts.csv.quote_byte()?;
        let has_headers = ctx.opts.csv.has_headers;
        let body_path = tmp_path(tmp, BODY_ROLE);
        let f = create_scratch(&body_path, ctx.private())?;
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
        if let Some(existing) = existing {
            let stored = ctx.read_stored(existing, label);
            remove_fetched(existing);
            let reader = faucet_core::compression::wrap_sync_reader(
                std::io::Cursor::new(stored?),
                ctx.codec,
            );
            let mut rdr = csv::ReaderBuilder::new()
                .has_headers(false)
                .flexible(true)
                .delimiter(delimiter)
                .quote(quote)
                .from_reader(reader);
            let mut first = has_headers;
            for rec in rdr.records() {
                let rec = rec.map_err(|e| {
                    FaucetError::Sink(format!("file sink: reading existing '{label}': {e}"))
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
    ) -> Result<(), Failure> {
        use faucet_core::CsvUnknownField;
        for (i, r) in records.iter().enumerate() {
            if !r.is_object() {
                return Err(clean(FaucetError::Sink(format!(
                    "csv: record {i} of the page is not an object, so it has no columns"
                ))));
            }
        }
        let frozen = on_unknown != CsvUnknownField::Widen && !self.header.is_empty();
        let unknown: Vec<String> = faucet_core::file_format::header_union(records)
            .into_iter()
            .filter(|k| !self.known.contains(k))
            .collect();
        if frozen && !unknown.is_empty() {
            if on_unknown == CsvUnknownField::Error {
                return Err(clean(FaucetError::Sink(format!(
                    "csv: record field(s) not in the header and `csv.on_unknown_field: error`: \
                     [{}] — the header is fixed from the first page, so these values would be \
                     dropped",
                    unknown.join(", ")
                ))));
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
                .map_err(|e| dirty(csv_err(&self.body_path, e)))?;
            self.narrowest = self.narrowest.min(row.len());
        }
        Ok(())
    }

    /// Write header + body to `tmp` through the codec.
    fn finish_into(
        &mut self,
        codec: Compression,
        tmp: &Path,
        private: bool,
    ) -> Result<(), FaucetError> {
        self.body
            .flush()
            .map_err(|e| io_err("flushing", &self.body_path, e))?;
        let f = create_scratch(tmp, private)?;
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
        buffered
            .into_inner()
            .map_err(|e| io_err("flushing", tmp, e.into_error()))?;
        Ok(())
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

#[cfg(feature = "file-format-csv")]
fn csv_err(path: &Path, e: csv::Error) -> FaucetError {
    FaucetError::Sink(format!("file sink: writing '{}': {e}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_spool_cuts_exact_parts_and_keeps_the_rest() {
        let parts = Parts::default();
        let mut out = LineOut::Spool {
            size: 4,
            buf: Vec::new(),
            parts: parts.clone(),
        };
        out.write_all(b"abcdefghij").unwrap();
        out.flush().unwrap();
        assert_eq!(
            *parts.lock().unwrap(),
            vec![b"abcd".to_vec(), b"efgh".to_vec()]
        );
        out.close();
        assert_eq!(parts.lock().unwrap().last().unwrap(), b"ij");
        let empty = LineOut::Spool {
            size: 4,
            buf: Vec::new(),
            parts: parts.clone(),
        };
        empty.close();
        assert_eq!(parts.lock().unwrap().len(), 3);
    }

    #[cfg(unix)]
    #[test]
    fn private_scratch_files_are_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("x");
        create_scratch(&p, true).unwrap();
        let mode = std::fs::metadata(&p).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
        let q = dir.path().join("y");
        write_new(&q, b"1", false).unwrap();
        let mode = std::fs::metadata(&q).unwrap().permissions().mode();
        assert_ne!(
            mode & 0o077,
            0,
            "default permissions for a non-private file"
        );
    }
}
