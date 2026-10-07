//! File source executor — the one module that performs I/O.

use crate::config::{FileSourceConfig, IncrementalBy};
use crate::http::HttpFetcher;
use crate::plan::{self, Bookmark, Candidate};
use async_trait::async_trait;
use faucet_common_file::{resolution_name, url_file_name};
use faucet_core::compression::Compression;
use faucet_core::observability::RecorderSlot;
use faucet_core::shard::{HashShard, ShardSpec, parse_hash_shard, plan_hash_shards};
use faucet_core::{FaucetError, FileFormat, FileInput, FormatOptions, Stream, StreamPage};
use futures::stream::StreamExt;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncBufReadExt;

type Reader = Pin<Box<dyn tokio::io::AsyncBufRead + Send + Unpin>>;

/// One file, opened in the shape its format decodes from. Prefetched with an
/// ordered look-ahead of `concurrency`: a JSON Lines file holds only an open
/// reader, a local uncompressed binary container only an open handle, and
/// every other shape the file's bytes.
enum Opened {
    Lines(Reader),
    #[cfg(feature = "file-format-csv")]
    Csv(Reader),
    Bytes(Vec<u8>),
    Local(std::fs::File),
    Skip,
}

/// Per-run decode state shared with the blocking decoders: the Avro / ORC
/// decoders anchored on the first file of their format, and the first Parquet
/// file's schema.
pub(crate) struct Decoders {
    opts: FormatOptions,
    containers: HashMap<&'static str, faucet_core::ContainerDecoder>,
    #[cfg(feature = "file-format-parquet")]
    parquet: Option<(String, arrow::datatypes::SchemaRef)>,
    #[cfg_attr(not(feature = "file-format-parquet"), allow(dead_code))]
    parquet_columns: Option<Vec<String>>,
}

impl Decoders {
    fn new(opts: FormatOptions, parquet_columns: Option<Vec<String>>) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            opts,
            parquet_columns,
            containers: HashMap::new(),
            #[cfg(feature = "file-format-parquet")]
            parquet: None,
        }))
    }

    fn container(
        &mut self,
        format: FileFormat,
    ) -> Result<&mut faucet_core::ContainerDecoder, FaucetError> {
        let key = format.as_str();
        if !self.containers.contains_key(key) {
            let d = faucet_core::ContainerDecoder::new(format, &self.opts)?;
            self.containers.insert(key, d);
        }
        Ok(self.containers.get_mut(key).expect("inserted above"))
    }
}

/// A local-filesystem (or single `http(s)://` file) source.
pub struct FileSource {
    config: FileSourceConfig,
    http: Option<HttpFetcher>,
    start: Mutex<Option<Bookmark>>,
    shard: Mutex<Option<HashShard>>,
    roundtrips: RecorderSlot,
    #[cfg(feature = "encryption")]
    encryption: Option<faucet_core::CompiledEncryption>,
}

impl FileSource {
    /// Validate the config and build the HTTP client when `path` is a URL.
    /// Performs no filesystem I/O.
    pub fn new(config: FileSourceConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let http = if config.is_http() {
            Some(HttpFetcher::with_timeouts(
                &config.headers,
                config.http_retries,
                std::time::Duration::from_secs(config.http_connect_timeout_secs),
                std::time::Duration::from_secs(config.http_read_timeout_secs),
            )?)
        } else {
            None
        };
        Ok(Self {
            http,
            start: Mutex::new(None),
            shard: Mutex::new(None),
            roundtrips: RecorderSlot::new(),
            #[cfg(feature = "encryption")]
            encryption: config
                .encryption
                .as_ref()
                .map(faucet_core::CompiledEncryption::compile)
                .transpose()?,
            config,
        })
    }

    fn by(&self) -> Option<IncrementalBy> {
        self.config.incremental.map(|i| i.by)
    }

    /// The format of `path`, or `None` when auto-detection finds none and
    /// `strict` is off (the file is skipped with a warning).
    fn format_of(&self, path: &str) -> Result<Option<FileFormat>, FaucetError> {
        let found = self
            .config
            .format
            .resolve(resolution_name(path), self.config.strict)
            .map_err(|e| FaucetError::Source(format!("file source: {e}")))?;
        if found.is_none() {
            tracing::warn!(path = %path, "file source: skipping a file with no recognised extension");
        }
        Ok(found)
    }

    fn codec_of(&self, path: &str) -> Compression {
        let codec = faucet_common_file::resolve_compression(self.config.compression, path);
        faucet_core::compression::warn_mismatch(resolution_name(path), codec);
        codec
    }

    /// The files this run reads, in order.
    async fn candidates(&self, apply_bookmark: bool) -> Result<Vec<Candidate>, FaucetError> {
        self.candidates_at(&self.config.path, apply_bookmark).await
    }

    async fn candidates_at(
        &self,
        root: &str,
        apply_bookmark: bool,
    ) -> Result<Vec<Candidate>, FaucetError> {
        let mut files = match &self.http {
            Some(http) => {
                let needs_mtime = matches!(self.by(), Some(IncrementalBy::Mtime))
                    || self.config.stable_for_secs.is_some();
                let mtime_ns = if needs_mtime {
                    let meta = http.head(root, &self.roundtrips).await?;
                    Some(meta.last_modified_ns.ok_or_else(|| {
                        FaucetError::Source(format!(
                            "file source: {} sends no Last-Modified header, which `incremental: \
                             {{by: mtime}}` / `stable_for_secs` need",
                            root
                        ))
                    })?)
                } else {
                    None
                };
                vec![Candidate {
                    path: root.to_string(),
                    mtime_ns,
                    ctime_ns: None,
                }]
            }
            None => {
                let path = root.to_string();
                let recursive = self.config.recursive;
                tokio::task::spawn_blocking(move || plan::list_local(&path, recursive))
                    .await
                    .map_err(|e| {
                        FaucetError::Source(format!("file source: listing task failed: {e}"))
                    })??
            }
        };
        if let Some(member) = *self.shard.lock().expect("shard mutex") {
            files.retain(|f| member.contains(&f.path));
        }
        let bookmark = if apply_bookmark {
            self.start.lock().expect("bookmark mutex").clone()
        } else {
            None
        };
        let mut files = plan::select(
            files,
            if apply_bookmark { self.by() } else { None },
            bookmark.as_ref(),
            self.config.stable_for_secs,
            now_ns(),
        )?;
        if let Some(max) = self.config.max_files {
            files.truncate(max);
        }
        Ok(files)
    }

    async fn open_reader(&self, path: &str) -> Result<Reader, FaucetError> {
        let raw = self.open_raw(path).await?;
        Ok(faucet_core::compression::wrap_async_reader(
            raw,
            self.codec_of(path),
        ))
    }

    async fn open_raw(&self, path: &str) -> Result<Reader, FaucetError> {
        Ok(match &self.http {
            Some(http) => {
                let resp = http.get(path, &self.roundtrips).await?;
                let body = resp
                    .bytes_stream()
                    .map(|r| r.map_err(std::io::Error::other));
                Box::pin(tokio_util::io::StreamReader::new(body))
            }
            None => Box::pin(tokio::io::BufReader::new(
                tokio::fs::File::open(path)
                    .await
                    .map_err(|e| read_err(path, e))?,
            )),
        })
    }

    async fn open(&self, path: &str) -> Result<Opened, FaucetError> {
        let Some(format) = self.format_of(path)? else {
            return Ok(Opened::Skip);
        };
        #[cfg(feature = "encryption")]
        if let Some(enc) = &self.encryption {
            let plain = self.read_decrypted(path, format, enc).await?;
            return Ok(match format {
                FileFormat::JsonLines => Opened::Lines(Box::pin(std::io::Cursor::new(plain))),
                #[cfg(feature = "file-format-csv")]
                FileFormat::Csv => Opened::Csv(Box::pin(std::io::Cursor::new(plain))),
                _ => Opened::Bytes(plain),
            });
        }
        if format == FileFormat::JsonLines {
            return Ok(Opened::Lines(self.open_reader(path).await?));
        }
        #[cfg(feature = "file-format-csv")]
        if format == FileFormat::Csv {
            return Ok(Opened::Csv(self.open_reader(path).await?));
        }
        let binary = format.is_container() || format == FileFormat::Parquet;
        if binary && self.http.is_none() && self.codec_of(path) == Compression::None {
            let file = std::fs::File::open(path).map_err(|e| read_err(path, e))?;
            return Ok(Opened::Local(file));
        }
        let reader = self.open_reader(path).await?;
        let bytes = faucet_core::file_format::read_to_end_capped(
            reader,
            self.config.max_object_bytes,
            path,
        )
        .await?;
        Ok(Opened::Bytes(bytes))
    }

    #[cfg(feature = "encryption")]
    async fn read_decrypted(
        &self,
        path: &str,
        format: FileFormat,
        enc: &faucet_core::CompiledEncryption,
    ) -> Result<Vec<u8>, FaucetError> {
        let max = self.config.max_object_bytes;
        let raw =
            faucet_core::file_format::read_to_end_capped(self.open_raw(path).await?, max, path)
                .await?;
        let codec = self.codec_of(path);
        let fail = |e: FaucetError| FaucetError::Source(format!("file source: '{path}': {e}"));
        if faucet_core::encryption::is_encrypted(&raw) {
            let sealed = enc.decrypt(&raw).map_err(fail)?;
            let mut plain = Vec::new();
            std::io::Read::read_to_end(
                &mut std::io::Read::take(
                    faucet_core::compression::wrap_sync_reader(std::io::Cursor::new(sealed), codec),
                    max.saturating_add(1),
                ),
                &mut plain,
            )
            .map_err(|e| read_err(path, e))?;
            faucet_core::file_format::check_object_size(plain.len() as u64, max, path)?;
            return Ok(plain);
        }
        if matches!(format, FileFormat::JsonLines | FileFormat::RawText)
            && codec == Compression::None
        {
            return crate::decrypt::lines(&raw, enc).map_err(fail);
        }
        Err(FaucetError::Source(format!(
            "file source: '{path}' is not encrypted, but `encryption` is set — refusing to read \
             plaintext as if it were authenticated"
        )))
    }

    fn root(&self, context: &HashMap<String, Value>) -> String {
        if context.is_empty() {
            self.config.path.clone()
        } else {
            faucet_core::util::substitute_context(&self.config.path, context)
        }
    }

    async fn check_parquet_schemas(&self, files: &[Candidate]) -> Result<(), FaucetError> {
        #[cfg(feature = "file-format-parquet")]
        {
            if self.http.is_some() || self.config.encryption_set() {
                return Ok(());
            }
            let mut local = Vec::new();
            for f in files {
                if self.format_of(&f.path)? == Some(FileFormat::Parquet)
                    && self.codec_of(&f.path) == Compression::None
                {
                    local.push(f.path.clone());
                }
            }
            if local.len() < 2 {
                return Ok(());
            }
            let columns = self.config.parquet.columns.clone();
            tokio::task::spawn_blocking(move || {
                crate::parquet::check_schemas(&local, columns.as_deref())
            })
            .await
            .map_err(|e| FaucetError::Source(format!("file source: schema check failed: {e}")))??;
        }
        #[cfg(not(feature = "file-format-parquet"))]
        let _ = files;
        Ok(())
    }

    fn prefetch<'a>(
        &'a self,
        files: &'a [Candidate],
    ) -> impl futures::Stream<Item = (&'a Candidate, Result<Opened, FaucetError>)> + 'a {
        futures::stream::iter(files.iter())
            .map(move |c| async move { (c, self.open(&c.path).await) })
            .buffered(self.config.concurrency.max(1))
    }
}

fn now_ns() -> i64 {
    chrono::Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX)
}

fn read_err(path: &str, e: impl std::fmt::Display) -> FaucetError {
    FaucetError::Source(format!("file source: read '{path}': {e}"))
}

/// Decode the bytes of a whole-object format into records.
async fn decode_whole(
    path: &str,
    format: FileFormat,
    bytes: Vec<u8>,
    opts: &FormatOptions,
) -> Result<Vec<Value>, FaucetError> {
    if format == FileFormat::RawText {
        let content = String::from_utf8(bytes)
            .map_err(|e| FaucetError::Source(format!("file source: '{path}' is not UTF-8: {e}")))?;
        return Ok(vec![json!({"path": path, "content": content})]);
    }
    faucet_core::file_format::decode(&bytes, format, opts)
        .await
        .map_err(|e| FaucetError::Source(format!("file source: '{path}': {e}")))
}

/// Run a binary decoder (Avro / ORC / Parquet) on a blocking thread, handing
/// back chunks of `T` through a small channel so memory stays at a couple of
/// chunks however large the file is.
fn spawn_blocking_decode<T: Send + 'static>(
    work: impl FnOnce(&mut dyn FnMut(T) -> Result<(), FaucetError>) -> Result<(), FaucetError>
    + Send
    + 'static,
) -> (
    tokio::sync::mpsc::Receiver<T>,
    tokio::task::JoinHandle<Result<(), FaucetError>>,
) {
    let (tx, rx) = tokio::sync::mpsc::channel(2);
    let handle = tokio::task::spawn_blocking(move || {
        work(&mut |item| {
            tx.blocking_send(item)
                .map_err(|_| FaucetError::Source("file source: decode consumer went away".into()))
        })
    });
    (rx, handle)
}

async fn join(handle: tokio::task::JoinHandle<Result<(), FaucetError>>) -> Result<(), FaucetError> {
    handle
        .await
        .map_err(|e| FaucetError::Source(format!("file source: decode task failed: {e}")))?
}

fn input_of(opened: Opened) -> FileInput {
    match opened {
        Opened::Local(f) => FileInput::File(f),
        Opened::Bytes(b) => FileInput::Bytes(b),
        _ => unreachable!("only binary formats are decoded here"),
    }
}

/// Decode one binary file's records on a blocking thread.
fn records_task(
    decoders: Arc<Mutex<Decoders>>,
    path: String,
    format: FileFormat,
    input: FileInput,
    chunk: usize,
) -> (
    tokio::sync::mpsc::Receiver<Vec<Value>>,
    tokio::task::JoinHandle<Result<(), FaucetError>>,
) {
    spawn_blocking_decode(move |send| {
        let mut d = decoders.lock().expect("decoder mutex");
        match format {
            #[cfg(feature = "file-format-parquet")]
            FileFormat::Parquet => crate::parquet::read(&mut d, &path, input, chunk, &mut |b| {
                send(faucet_core::columnar::record_batch_to_values(&b)?)
            }),
            #[cfg(not(feature = "file-format-parquet"))]
            FileFormat::Parquet => Err(crate::parquet_missing()),
            _ => d.container(format)?.records(&path, input, chunk, send),
        }
    })
}

/// Decode one binary file's Arrow batches on a blocking thread.
#[cfg(feature = "arrow")]
fn batches_task(
    decoders: Arc<Mutex<Decoders>>,
    path: String,
    format: FileFormat,
    input: FileInput,
    batch_size: usize,
) -> (
    tokio::sync::mpsc::Receiver<arrow::array::RecordBatch>,
    tokio::task::JoinHandle<Result<(), FaucetError>>,
) {
    spawn_blocking_decode(move |send| {
        let mut d = decoders.lock().expect("decoder mutex");
        match format {
            #[cfg(feature = "file-format-parquet")]
            FileFormat::Parquet => crate::parquet::read(&mut d, &path, input, batch_size, send),
            #[cfg(not(feature = "file-format-parquet"))]
            FileFormat::Parquet => Err(crate::parquet_missing()),
            _ => d
                .container(format)?
                .batches(&path, input, batch_size, send)
                .map(|_| ()),
        }
    })
}

#[cfg(feature = "file-format-parquet")]
impl Decoders {
    pub(crate) fn parquet_reference(
        &mut self,
    ) -> &mut Option<(String, arrow::datatypes::SchemaRef)> {
        &mut self.parquet
    }

    pub(crate) fn parquet_columns(&self) -> Option<&[String]> {
        self.parquet_columns.as_deref()
    }
}

#[async_trait]
impl faucet_core::Source for FileSource {
    async fn fetch_with_context(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let mut out = Vec::new();
        let mut pages = self.stream_pages(context, self.config.batch_size);
        while let Some(page) = pages.next().await {
            out.extend(page?.records);
        }
        Ok(out)
    }

    /// Stream every selected file in order. Pages carry up to `batch_size`
    /// records; in incremental mode a page never spans two files and the last
    /// page of each file carries the bookmark past it, so an interrupted run
    /// resumes at the next unread file.
    fn stream_pages<'a>(
        &'a self,
        context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let batch_size = self.config.batch_size;
        Box::pin(async_stream::try_stream! {
            let root = self.root(context);
            let files = self.candidates_at(&root, true).await?;
            tracing::info!(path = %root, files = files.len(), "file source listed files");
            self.check_parquet_schemas(&files).await?;
            let by = self.by();
            let mut bookmark = self.start.lock().expect("bookmark mutex").clone();
            let decoders = Decoders::new(self.config.format_options(), self.config.parquet.columns.clone());
            let opts = self.config.format_options();
            let chunk = if batch_size == 0 { usize::MAX } else { batch_size };
            let mut buffer: Vec<Value> = Vec::new();
            let mut opened_files = self.prefetch(&files);

            while let Some((file, opened)) = opened_files.next().await {
                let path = file.path.as_str();
                let opened = opened?;
                let format = match &opened {
                    Opened::Skip => None,
                    _ => self.format_of(path)?,
                };
                if let Some(format) = format {
                    match opened {
                        Opened::Lines(reader) => {
                            let mut lines = reader.lines();
                            let mut n = 0usize;
                            while let Some(line) = lines.next_line().await.map_err(|e| read_err(path, e))? {
                                n += 1;
                                let line = line.trim();
                                if line.is_empty() {
                                    continue;
                                }
                                buffer.push(serde_json::from_str(line).map_err(|e| {
                                    FaucetError::Source(format!("file source: '{path}' line {n}: {e}"))
                                })?);
                                if buffer.len() >= chunk {
                                    yield StreamPage { records: std::mem::take(&mut buffer), bookmark: None };
                                }
                            }
                        }
                        #[cfg(feature = "file-format-csv")]
                        Opened::Csv(reader) => {
                            let mut rows = faucet_core::file_format::csv::CsvRowReader::new(reader, &opts.csv, false)?;
                            while let Some(r) = rows.next_record().await.map_err(|e| {
                                FaucetError::Source(format!("file source: '{path}': {e}"))
                            })? {
                                buffer.push(r);
                                if buffer.len() >= chunk {
                                    yield StreamPage { records: std::mem::take(&mut buffer), bookmark: None };
                                }
                            }
                        }
                        other if format.is_container() || format == FileFormat::Parquet => {
                            let (mut rx, handle) = records_task(decoders.clone(), path.to_string(), format, input_of(other), chunk);
                            while let Some(rows) = rx.recv().await {
                                for r in rows {
                                    buffer.push(r);
                                    if buffer.len() >= chunk {
                                        yield StreamPage { records: std::mem::take(&mut buffer), bookmark: None };
                                    }
                                }
                            }
                            join(handle).await?;
                        }
                        Opened::Bytes(bytes) => {
                            for r in decode_whole(path, format, bytes, &opts).await? {
                                buffer.push(r);
                                if buffer.len() >= chunk {
                                    yield StreamPage { records: std::mem::take(&mut buffer), bookmark: None };
                                }
                            }
                        }
                        Opened::Local(_) | Opened::Skip => unreachable!("binary and skipped files handled above"),
                    }
                    if batch_size == 0 && !buffer.is_empty() && by.is_none() {
                        yield StreamPage { records: std::mem::take(&mut buffer), bookmark: None };
                    }
                }
                if let Some(by) = by {
                    bookmark = Some(plan::advance(bookmark, by, file)?);
                    let mark = bookmark.as_ref().map(Bookmark::to_value);
                    yield StreamPage { records: std::mem::take(&mut buffer), bookmark: mark };
                }
            }
            if !buffer.is_empty() {
                yield StreamPage { records: buffer, bookmark: None };
            }
        })
    }

    /// The columnar path is taken for an explicit `avro`, `orc` or `parquet`
    /// format; `auto` cannot promise every file decodes to Arrow.
    #[cfg(feature = "arrow")]
    fn supports_columnar(&self) -> bool {
        self.config
            .format
            .explicit()
            .is_some_and(|f| f.is_container() || f == FileFormat::Parquet)
    }

    #[cfg(feature = "arrow")]
    fn stream_batches<'a>(
        &'a self,
        context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<
        Box<
            dyn Stream<Item = Result<faucet_core::columnar::ColumnarPage, FaucetError>> + Send + 'a,
        >,
    > {
        Box::pin(async_stream::try_stream! {
            if !self.supports_columnar() {
                Err(FaucetError::Source(
                    "file source: stream_batches needs format avro, orc or parquet".into(),
                ))?;
            }
            let format = self.config.format.explicit().expect("checked by supports_columnar");
            let root = self.root(context);
            let files = self.candidates_at(&root, true).await?;
            self.check_parquet_schemas(&files).await?;
            let by = self.by();
            let mut bookmark = self.start.lock().expect("bookmark mutex").clone();
            let decoders = Decoders::new(self.config.format_options(), self.config.parquet.columns.clone());
            let mut opened_files = self.prefetch(&files);
            while let Some((file, opened)) = opened_files.next().await {
                let (mut rx, handle) = batches_task(decoders.clone(), file.path.clone(), format, input_of(opened?), self.config.batch_size);
                let mut last: Option<arrow::array::RecordBatch> = None;
                while let Some(batch) = rx.recv().await {
                    if let Some(prev) = last.replace(batch) {
                        yield faucet_core::columnar::ColumnarPage::new(prev, None);
                    }
                }
                join(handle).await?;
                let mark = match by {
                    Some(by) => {
                        bookmark = Some(plan::advance(bookmark, by, file)?);
                        bookmark.as_ref().map(Bookmark::to_value)
                    }
                    None => None,
                };
                match last {
                    Some(batch) => yield faucet_core::columnar::ColumnarPage::new(batch, mark),
                    None if mark.is_some() => {
                        let schema = std::sync::Arc::new(arrow::datatypes::Schema::empty());
                        yield faucet_core::columnar::ColumnarPage::new(arrow::array::RecordBatch::new_empty(schema), mark);
                    }
                    None => {}
                }
            }
        })
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(FileSourceConfig))
            .expect("schema serialization")
    }

    fn connector_name(&self) -> &'static str {
        "file"
    }

    fn set_roundtrip_recorder(&self, recorder: Arc<faucet_core::observability::RoundtripRecorder>) {
        self.roundtrips.install(recorder);
    }

    fn dataset_uri(&self) -> String {
        if self.http.is_some() {
            return faucet_core::util::redact_uri_credentials(&self.config.path);
        }
        let p = std::path::Path::new(&self.config.path);
        let abs = if p.is_absolute() {
            p.to_path_buf()
        } else {
            std::env::current_dir()
                .map(|d| d.join(p))
                .unwrap_or_else(|_| p.to_path_buf())
        };
        format!("file://{}", abs.display())
    }

    /// The bookmark lives under a key derived from `path`, so two sources
    /// reading different paths never share a position.
    fn state_key(&self) -> Option<String> {
        self.config.incremental.map(|_| {
            format!(
                "file:{:016x}",
                faucet_core::shard::shard_hash(&self.config.path)
            )
        })
    }

    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        let Some(by) = self.by() else {
            return Ok(());
        };
        if bookmark.is_null() {
            return Ok(());
        }
        *self.start.lock().expect("bookmark mutex") = Some(Bookmark::from_value(&bookmark, by)?);
        Ok(())
    }

    fn is_shardable(&self) -> bool {
        self.http.is_none()
    }

    async fn enumerate_shards(&self, target: usize) -> Result<Vec<ShardSpec>, FaucetError> {
        Ok(if self.http.is_some() {
            plan_hash_shards(1)
        } else {
            plan_hash_shards(target)
        })
    }

    async fn apply_shard(&self, shard: &ShardSpec) -> Result<(), FaucetError> {
        *self.shard.lock().expect("shard mutex") = parse_hash_shard(shard, "file")?;
        Ok(())
    }

    fn supports_discover(&self) -> bool {
        true
    }

    /// One dataset per readable file under `path` — no file is opened.
    async fn discover(&self) -> Result<Vec<faucet_core::DatasetDescriptor>, FaucetError> {
        let files = self.candidates(false).await?;
        let mut out = Vec::new();
        for f in files {
            let Some(format) = self.format_of(&f.path)? else {
                continue;
            };
            let name = if self.http.is_some() {
                url_file_name(&f.path).to_string()
            } else {
                std::path::Path::new(&f.path)
                    .strip_prefix(&self.config.path)
                    .ok()
                    .map(|p| p.to_string_lossy().into_owned())
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| f.path.clone())
            };
            out.push(faucet_core::DatasetDescriptor::new(
                name,
                "file",
                json!({"path": f.path, "format": format.as_str()}),
            ));
        }
        Ok(out)
    }

    /// Reachability without reading data: `HEAD` for a URL, a listing for a
    /// local path.
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};
        let started = std::time::Instant::now();
        if let Some(http) = &self.http {
            let probe = match tokio::time::timeout(
                ctx.timeout,
                http.head(&self.config.path, &self.roundtrips),
            )
            .await
            {
                Err(_) => Probe::fail("head", started.elapsed(), "timed out"),
                Ok(Ok(_)) => Probe::pass("head", started.elapsed()),
                Ok(Err(e)) => Probe::fail("head", started.elapsed(), e.to_string()),
            };
            return Ok(CheckReport::single(probe));
        }
        let probe = match tokio::time::timeout(ctx.timeout, self.candidates(false)).await {
            Err(_) => Probe::fail("list", started.elapsed(), "timed out"),
            Ok(Ok(files)) if files.is_empty() => Probe::fail_hint(
                "list",
                started.elapsed(),
                format!("no files match {}", self.config.path),
                "check `path` (and `recursive` for nested directories)",
            ),
            Ok(Ok(_)) => Probe::pass("list", started.elapsed()),
            Ok(Err(e)) => Probe::fail("list", started.elapsed(), e.to_string()),
        };
        Ok(CheckReport::single(probe))
    }
}
