//! One output file: written to a temporary sibling and renamed into place on
//! [`OpenFile::finalize`], so a reader sees a complete file or none at all.

use crate::layout::{BODY_SUFFIX, TMP_SUFFIX, io_err, tmp_path};
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
    pub parquet: &'a crate::config::ParquetOptions,
    pub json_lines: &'a crate::config::JsonLinesOptions,
    #[cfg(feature = "encryption")]
    pub encryption: Option<&'a faucet_core::CompiledEncryption>,
}

impl Ctx<'_> {
    /// The bytes of `records` as the line formats write them: pretty or
    /// compact JSON, and sealed per record when encryption is on.
    fn line_bytes(&self, records: &[Value]) -> Result<Vec<u8>, FaucetError> {
        #[cfg(feature = "encryption")]
        if let Some(enc) = self.encryption {
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

    /// Seal a finished whole-file body in place.
    fn seal_file(&self, path: &Path) -> Result<(), FaucetError> {
        #[cfg(feature = "encryption")]
        if let Some(enc) = self.encryption
            && !matches!(self.format, FileFormat::JsonLines | FileFormat::RawText)
        {
            let plain = std::fs::read(path).map_err(|e| io_err("reading", path, e))?;
            write_synced(path, &enc.encrypt(&plain))?;
        }
        let _ = path;
        Ok(())
    }

    /// Read a finalised file back as plaintext bytes (decrypting a sealed one).
    #[cfg_attr(
        not(any(feature = "file-format-csv", feature = "file-format-parquet")),
        allow(dead_code)
    )]
    pub fn read_existing(&self, path: &Path) -> Result<Vec<u8>, FaucetError> {
        let raw = std::fs::read(path).map_err(|e| io_err("opening", path, e))?;
        #[cfg(feature = "encryption")]
        if let Some(enc) = self.encryption
            && faucet_core::encryption::is_encrypted(&raw)
        {
            return enc.decrypt(&raw).map_err(|e| {
                FaucetError::Sink(format!("file sink: decrypting '{}': {e}", path.display()))
            });
        }
        Ok(raw)
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
    /// Whole-document formats (JSON array, XML, Excel, Avro): the file's
    /// records, encoded together at each finalisation.
    Doc(Vec<Value>),
    /// Parquet row groups streamed to the temporary file.
    #[cfg(feature = "file-format-parquet")]
    Parquet(Box<crate::parquet::ParquetState>),
}

/// One output file in progress.
pub(crate) struct OpenFile {
    /// Where the file appears when finalised.
    pub final_path: PathBuf,
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
}

impl OpenFile {
    /// Start `final_path`, continuing from its current contents when `resume`.
    pub fn create(ctx: &Ctx<'_>, final_path: PathBuf, resume: bool) -> Result<Self, FaucetError> {
        let tmp = tmp_path(&final_path, TMP_SUFFIX);
        let mut file = Self {
            enc: Enc::Doc(Vec::new()),
            final_path,
            tmp,
            records: 0,
            bytes: 0,
            poisoned: false,
            finalized: false,
        };
        file.enc = match ctx.format {
            FileFormat::JsonLines | FileFormat::RawText => Enc::Lines(Some(open_lines(
                &file.final_path,
                &file.tmp,
                ctx.codec,
                resume,
            )?)),
            #[cfg(feature = "file-format-csv")]
            FileFormat::Csv => {
                let (state, carried) = CsvState::create(ctx, &file.final_path, resume)?;
                file.records = carried;
                Enc::Csv(Box::new(state))
            }
            #[cfg(feature = "file-format-parquet")]
            FileFormat::Parquet => Enc::Parquet(Box::new(crate::parquet::ParquetState::new())),
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
        match &mut self.enc {
            Enc::Lines(slot) => {
                let buf = ctx.line_bytes(records)?;
                if slot.is_none() {
                    *slot = Some(open_lines(&self.final_path, &tmp, ctx.codec, true)?);
                }
                let w = slot.as_mut().expect("opened above");
                w.write_all(&buf).map_err(|e| io_err("writing", &tmp, e))
            }
            #[cfg(feature = "file-format-csv")]
            Enc::Csv(state) => state.write(records, ctx.opts.csv.on_unknown_field),
            Enc::Doc(buf) => {
                buf.extend_from_slice(records);
                Ok(())
            }
            #[cfg(feature = "file-format-parquet")]
            Enc::Parquet(state) => {
                let batch = state.batch_for(ctx.parquet, records)?;
                state.write(ctx, &self.final_path, &tmp, self.finalized, &batch)
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
        let result = state.write(ctx, &self.final_path, &tmp, self.finalized, batch);
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
                file.sync_all().map_err(|e| io_err("syncing", &tmp, e))?;
            }
            #[cfg(feature = "file-format-csv")]
            Enc::Csv(state) => state.finish_into(ctx.codec, &tmp)?,
            Enc::Doc(records) => {
                let body = faucet_core::file_format::encode(records, ctx.format, ctx.opts)?;
                let body = faucet_core::compress_buf(&body, ctx.codec)?;
                write_synced(&tmp, &body)?;
            }
            #[cfg(feature = "file-format-parquet")]
            Enc::Parquet(state) => {
                if !state.close(&tmp)? {
                    return Ok(());
                }
            }
        }
        ctx.seal_file(&tmp)?;
        std::fs::rename(&tmp, &self.final_path)
            .map_err(|e| io_err("renaming into place", &self.final_path, e))?;
        sync_dir(&self.final_path);
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
        let _ = std::fs::remove_file(tmp_path(&self.final_path, BODY_SUFFIX));
    }

    fn poison_error(&self) -> FaucetError {
        FaucetError::Sink(format!(
            "file sink: an earlier write to '{}' failed, so it cannot be completed",
            self.final_path.display()
        ))
    }
}

/// Open the temporary file for line output, copying the final file in first
/// when continuing it (a compressed file gains a new member).
fn open_lines(
    final_path: &Path,
    tmp: &Path,
    codec: Compression,
    resume: bool,
) -> Result<Box<LineWriter>, FaucetError> {
    let carry = resume && final_path.exists();
    if carry {
        std::fs::copy(final_path, tmp).map_err(|e| io_err("copying", final_path, e))?;
    }
    let f = OpenOptions::new()
        .create(true)
        .write(true)
        .append(carry)
        .truncate(!carry)
        .open(tmp)
        .map_err(|e| io_err("creating", tmp, e))?;
    Ok(Box::new(sync_compress_writer(BufWriter::new(f), codec)))
}

/// Write `bytes` to `path` and sync it.
pub(crate) fn write_synced(path: &Path, bytes: &[u8]) -> Result<(), FaucetError> {
    let mut f = File::create(path).map_err(|e| io_err("creating", path, e))?;
    f.write_all(bytes).map_err(|e| io_err("writing", path, e))?;
    f.sync_all().map_err(|e| io_err("syncing", path, e))
}

/// Best-effort fsync of the directory holding `path`, so a rename survives a
/// crash.
pub(crate) fn sync_dir(path: &Path) {
    #[cfg(unix)]
    if let Some(dir) = path.parent()
        && let Ok(d) = File::open(if dir.as_os_str().is_empty() {
            Path::new(".")
        } else {
            dir
        })
    {
        let _ = d.sync_all();
    }
    #[cfg(not(unix))]
    let _ = path;
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
    /// rows over. Returns the state and the number of carried rows.
    fn create(
        ctx: &Ctx<'_>,
        final_path: &Path,
        resume: bool,
    ) -> Result<(Self, usize), FaucetError> {
        let delimiter = ctx.opts.csv.delimiter_byte()?;
        let quote = ctx.opts.csv.quote_byte()?;
        let has_headers = ctx.opts.csv.has_headers;
        let body_path = tmp_path(final_path, BODY_SUFFIX);
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
        if resume && final_path.exists() {
            let existing = std::io::Cursor::new(ctx.read_existing(final_path)?);
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
                        final_path.display()
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
    fn finish_into(&mut self, codec: Compression, tmp: &Path) -> Result<(), FaucetError> {
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
        file.sync_all().map_err(|e| io_err("syncing", tmp, e))
    }
}

#[cfg(feature = "file-format-csv")]
impl CsvState {
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
