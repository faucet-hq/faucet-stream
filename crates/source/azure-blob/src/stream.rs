//! Azure Blob source stream executor.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use faucet_common_azure::build_store;
use faucet_core::{FaucetError, Stream, StreamPage};
use futures::stream::{self, StreamExt, TryStreamExt};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use serde_json::Value;
use tokio::io::AsyncBufReadExt;

use crate::config::{AzureBlobSourceConfig, AzureFileFormat};

/// One blob's payload, fetched in the shape its `file_format` decodes from.
///
/// Prefetching these with a bounded look-ahead is what makes `concurrency`
/// real on the streaming path (#619). The bound it promises differs by format
/// and that difference is the point: `JsonLines` overlaps only the request
/// round-trip — `open_object_reader` does not download the body, so the decode
/// still streams line-by-line at O(batch) memory — while the whole-blob
/// formats hold at most `concurrency` bodies at once.
enum Fetched {
    Lines(Pin<Box<dyn tokio::io::AsyncBufRead + Send + Unpin>>),
    RawText(String),
    JsonArray(String),
    /// A whole object already decoded into records by
    /// [`faucet_core::file_format`] (#604) — CSV, XML and Excel. Decoding at
    /// fetch time keeps the per-format work in one place; the page loop then
    /// chunks these exactly as it chunks a JSON array's.
    #[cfg(any(
        feature = "file-format-csv",
        feature = "file-format-xml",
        feature = "file-format-excel",
        feature = "file-format-avro",
        feature = "file-format-orc"
    ))]
    Records(Vec<Value>),
    /// A whole Avro or ORC blob (#719). Decoded in listing order by the page
    /// loop's [`ContainerDecoder`](faucet_core::ContainerDecoder), not at
    /// fetch time, because every blob is resolved against the first one's
    /// schema and prefetch completes out of order.
    #[cfg(any(feature = "file-format-avro", feature = "file-format-orc"))]
    Container(Vec<u8>),
    /// A Parquet blob as Arrow batches — read over byte ranges one row group
    /// at a time, or decoded whole when a compression codec makes it
    /// unaddressable (#783).
    #[cfg(feature = "arrow")]
    Parquet(faucet_core::file_format::parquet_io::BatchStream),
}

/// Byte ranges of one blob, for the shared ranged Parquet reader (#783).
#[cfg(feature = "arrow")]
struct BlobRange {
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
}

#[cfg(feature = "arrow")]
impl faucet_core::file_format::parquet_io::RangeRead for BlobRange {
    fn read_range(
        &mut self,
        range: std::ops::Range<u64>,
    ) -> futures::future::BoxFuture<
        '_,
        Result<faucet_core::file_format::parquet_io::Bytes, FaucetError>,
    > {
        use futures::FutureExt as _;
        async move {
            self.store
                .get_range(&self.path, range)
                .await
                .map_err(|e| FaucetError::Source(format!("azure get range error: {e}")))
        }
        .boxed()
    }
}

#[cfg(feature = "arrow")]
type ColumnarStream<'a> = Pin<
    Box<dyn Stream<Item = Result<faucet_core::columnar::ColumnarPage, FaucetError>> + Send + 'a>,
>;

/// An Azure Blob source that lists and reads objects from a container.
pub struct AzureBlobSource {
    config: AzureBlobSourceConfig,
    store: Arc<dyn ObjectStore>,
    filter: faucet_common_file::ObjectFilter,
}

impl AzureBlobSource {
    /// Construct the source, building the object store eagerly so it is reused
    /// across calls.
    pub async fn new(config: AzureBlobSourceConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let filter = faucet_common_file::ObjectFilter::new(config.include.as_deref())
            .map_err(|e| faucet_common_file::config_context("azure source", e))?;
        let store = build_store(&config.connection)?;
        Ok(Self {
            config,
            store,
            filter,
        })
    }

    /// List object names under the configured (or override) prefix, capped at
    /// `max_objects` when set. When `object_keys` is configured, listing is
    /// skipped and those keys are used directly.
    async fn list_object_names(
        &self,
        prefix_override: Option<&str>,
    ) -> Result<Vec<String>, FaucetError> {
        if let Some(keys) = &self.config.object_keys {
            return Ok(cap_keys(keys.clone(), self.config.max_objects));
        }

        let effective_prefix = prefix_override
            .or(self.config.prefix.as_deref())
            .unwrap_or_default();
        let mut listing = self.store.list(list_root(effective_prefix).as_ref());
        let mut names: Vec<String> = Vec::new();
        while let Some(item) = listing.next().await {
            let meta = item.map_err(|e| {
                FaucetError::Source(format!(
                    "azure list error for container '{}': {e}",
                    self.config.container()
                ))
            })?;
            let name = meta.location.to_string();
            if name.is_empty()
                || !name.starts_with(effective_prefix)
                || faucet_common_file::write::is_unfinished_output_key(&name)
                || !self.filter.keep(&name, Some(meta.size), effective_prefix)
            {
                continue;
            }
            names.push(name);
            if let Some(max) = self.config.max_objects
                && names.len() >= max
            {
                break;
            }
        }
        Ok(names)
    }

    /// Fetch one blob in the shape its configured `file_format` needs.
    async fn fetch(&self, key: &str) -> Result<Fetched, FaucetError> {
        Ok(match self.config.file_format {
            AzureFileFormat::JsonLines => Fetched::Lines(self.open_object_reader(key).await?),
            AzureFileFormat::RawText => Fetched::RawText(self.read_object_text(key).await?),
            AzureFileFormat::JsonArray => Fetched::JsonArray(self.read_object_text(key).await?),
            #[cfg(feature = "file-format-csv")]
            AzureFileFormat::Csv => self.fetch_decoded(key).await?,
            #[cfg(feature = "file-format-xml")]
            AzureFileFormat::Xml => self.fetch_decoded(key).await?,
            #[cfg(feature = "file-format-excel")]
            AzureFileFormat::Xlsx => self.fetch_decoded(key).await?,
            #[cfg(feature = "file-format-avro")]
            AzureFileFormat::Avro => Fetched::Container(self.read_object_all(key).await?),
            #[cfg(feature = "file-format-orc")]
            AzureFileFormat::Orc => Fetched::Container(self.read_object_all(key).await?),
            #[cfg(feature = "arrow")]
            AzureFileFormat::Parquet => {
                Fetched::Parquet(self.open_parquet(key, self.config.batch_size).await?.1)
            }
        })
    }

    /// A blob's whole body, after any configured decompression.
    #[cfg(any(
        feature = "file-format-csv",
        feature = "file-format-xml",
        feature = "file-format-excel",
        feature = "file-format-avro",
        feature = "file-format-orc",
        all(feature = "arrow", feature = "compression")
    ))]
    async fn read_object_all(&self, key: &str) -> Result<Vec<u8>, FaucetError> {
        let reader = self.open_object_reader(key).await?;
        faucet_core::file_format::read_to_end_capped(reader, self.config.max_object_bytes, key)
            .await
    }

    /// Open one Parquet blob, projected by `parquet.columns`, as its Arrow
    /// schema and a stream of batches of at most `batch_size` rows. The blob
    /// is read over byte ranges, one row group at a time, so memory stays
    /// bounded by a row group rather than the blob (#783). A blob the
    /// configured `compression` resolves to a codec for is not randomly
    /// addressable, so it is decoded whole on a blocking thread instead.
    #[cfg(feature = "arrow")]
    async fn open_parquet(
        &self,
        key: &str,
        batch_size: usize,
    ) -> Result<
        (
            arrow::datatypes::SchemaRef,
            faucet_core::file_format::parquet_io::BatchStream,
        ),
        FaucetError,
    > {
        #[cfg(feature = "compression")]
        if self.config.compression.resolve(key) != faucet_core::compression::Compression::None {
            let (schema, batches) = self.read_parquet(key, batch_size).await?;
            return Ok((schema, stream::iter(batches.into_iter().map(Ok)).boxed()));
        }
        let path = object_path(key);
        let meta = self.store.head(&path).await.map_err(|e| {
            FaucetError::Source(format!(
                "azure head error for container '{}' key '{key}': {e}",
                self.config.container()
            ))
        })?;
        let reader = faucet_core::file_format::parquet_io::RangedParquetReader::new(
            BlobRange {
                store: self.store.clone(),
                path,
            },
            meta.size,
            key,
        );
        faucet_core::file_format::parquet_io::open_ranged(reader, &self.config.parquet, batch_size)
            .await
    }

    #[cfg(all(feature = "arrow", feature = "compression"))]
    async fn read_parquet(
        &self,
        key: &str,
        batch_size: usize,
    ) -> Result<(arrow::datatypes::SchemaRef, Vec<arrow::array::RecordBatch>), FaucetError> {
        let bytes = self.read_object_all(key).await?;
        let opts = self.config.parquet.clone();
        let display = key.to_string();
        tokio::task::spawn_blocking(move || {
            faucet_core::file_format::parquet_io::read_bytes(
                bytes.into(),
                &opts,
                batch_size,
                &display,
            )
        })
        .await
        .map_err(|e| {
            FaucetError::Source(format!("azure parquet decode for '{key}' panicked: {e}"))
        })?
    }

    /// A decoder for the configured container format, or `None` for every
    /// other format.
    #[cfg(any(feature = "file-format-avro", feature = "file-format-orc"))]
    fn container_decoder(&self) -> Result<Option<faucet_core::ContainerDecoder>, FaucetError> {
        match self.config.file_format.shared() {
            Some(f) if f.is_container() => Ok(Some(faucet_core::ContainerDecoder::new(
                f,
                &self.config.format_options(),
            )?)),
            _ => Ok(None),
        }
    }

    /// Avro / ORC blobs as Arrow batches, each resolved against the first
    /// blob's schema.
    #[cfg(all(
        feature = "arrow",
        any(feature = "file-format-avro", feature = "file-format-orc")
    ))]
    fn container_batches<'a>(
        &'a self,
        keys: Vec<String>,
    ) -> Result<ColumnarStream<'a>, FaucetError> {
        let decoder = self.container_decoder()?.ok_or_else(|| {
            FaucetError::Source(
                "azure source: stream_batches needs file_format avro, orc or parquet".into(),
            )
        })?;
        Ok(Box::pin(
            faucet_core::file_format::container::columnar_pages(
                keys,
                self.config.concurrency,
                decoder,
                self.config.batch_size,
                move |key| async move { self.read_object_all(&key).await },
            ),
        ))
    }

    #[cfg(all(
        feature = "arrow",
        not(any(feature = "file-format-avro", feature = "file-format-orc"))
    ))]
    fn container_batches<'a>(
        &'a self,
        _keys: Vec<String>,
    ) -> Result<ColumnarStream<'a>, FaucetError> {
        Err(FaucetError::Source(
            "azure source: stream_batches needs file_format avro, orc or parquet".into(),
        ))
    }

    /// Whether the configured format is Avro or ORC.
    #[cfg(feature = "arrow")]
    fn is_container(&self) -> bool {
        #[cfg(any(feature = "file-format-avro", feature = "file-format-orc"))]
        if let Some(f) = self.config.file_format.shared() {
            return f.is_container();
        }
        false
    }

    /// Read one blob whole and decode it through the shared format layer.
    ///
    /// Whole-object for all three: a workbook's directory sits at the end of a
    /// zip container, an XML document is a tree, and a CSV's records are
    /// chunked by the same page loop either way.
    #[cfg(any(
        feature = "file-format-csv",
        feature = "file-format-xml",
        feature = "file-format-excel"
    ))]
    async fn fetch_decoded(&self, key: &str) -> Result<Fetched, FaucetError> {
        let bytes = self.read_object_all(key).await?;
        let format =
            self.config.file_format.shared().ok_or_else(|| {
                FaucetError::Source(format!("azure '{key}': format has no decoder"))
            })?;
        let records =
            faucet_core::file_format::decode_owned(bytes, format, &self.config.format_options())
                .await
                .map_err(|e| FaucetError::Source(format!("azure '{key}': {e}")))?;
        Ok(Fetched::Records(records))
    }

    /// Read the full body of a single object into a UTF-8 `String`.
    async fn read_object_text(&self, key: &str) -> Result<String, FaucetError> {
        let reader = self.open_object_reader(key).await?;
        faucet_core::file_format::read_to_string_capped(reader, self.config.max_object_bytes, key)
            .await
    }

    /// Open an object as an `AsyncBufRead` over its (optionally decompressed)
    /// body so callers can decode line-by-line without buffering the whole
    /// object.
    async fn open_object_reader(
        &self,
        key: &str,
    ) -> Result<Pin<Box<dyn tokio::io::AsyncBufRead + Send + Unpin>>, FaucetError> {
        let path = object_path(key);
        let result = self.store.get(&path).await.map_err(|e| {
            FaucetError::Source(format!(
                "azure get error for container '{}' key '{key}': {e}",
                self.config.container()
            ))
        })?;

        // Read the metadata BEFORE consuming the stream (which moves `result`),
        // so a cleanly-truncated transfer is rejected rather than silently
        // parsed as a complete object (#161).
        let checks = length_checks(
            result.meta.size,
            content_encoding(&result.attributes),
            self.config.verify_length,
        );
        if self.config.verify_length && checks.is_empty() {
            tracing::debug!(
                key = %key,
                "azure object length verification skipped (transcoded Content-Encoding)"
            );
        }

        let byte_stream = result
            .into_stream()
            .map_err(|e| std::io::Error::other(e.to_string()));
        let reader = tokio_util::io::StreamReader::new(byte_stream);
        // Wrap the RAW byte stream in the verifier first so the byte count
        // covers the stored bytes (below any client-side decompression).
        let verified = faucet_core::VerifyingReader::new(reader, checks);
        let buffered = tokio::io::BufReader::new(verified);
        #[cfg(feature = "compression")]
        {
            let codec = self.config.compression.resolve(key);
            faucet_core::compression::warn_mismatch(key, codec);
            Ok(faucet_core::compression::wrap_async_reader(buffered, codec))
        }
        #[cfg(not(feature = "compression"))]
        {
            Ok(Box::pin(buffered))
        }
    }

    /// Parse file content into records based on the configured file format.
    fn parse_content(&self, key: &str, text: &str) -> Result<Vec<Value>, FaucetError> {
        parse_file_content(&self.config.file_format, key, text)
    }
}

/// Parse object content into records for a given format. Free function (vs. an
/// `AzureBlobSource` method) so it is unit-testable without an Azure client —
/// the parsing logic is pure.
/// The shared-format decoders run at fetch time, not through the text parser.
#[cfg(any(
    feature = "file-format-csv",
    feature = "file-format-xml",
    feature = "file-format-excel",
    feature = "file-format-avro",
    feature = "file-format-orc",
    feature = "arrow"
))]
fn shared_format_via_text(key: &str, format: &str) -> FaucetError {
    FaucetError::Source(format!(
        "azure {format} object '{key}' reached the text parser (internal error: {format} is \
         decoded at fetch time)"
    ))
}

pub(crate) fn parse_file_content(
    format: &AzureFileFormat,
    key: &str,
    text: &str,
) -> Result<Vec<Value>, FaucetError> {
    match format {
        AzureFileFormat::JsonLines => {
            let mut records = Vec::new();
            for (line_num, line) in text.lines().enumerate() {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let value: Value = serde_json::from_str(trimmed).map_err(|e| {
                    FaucetError::Source(format!(
                        "azure JSON parse error in '{key}' at line {}: {e}",
                        line_num + 1
                    ))
                })?;
                records.push(value);
            }
            Ok(records)
        }
        AzureFileFormat::JsonArray => {
            let value: Value = serde_json::from_str(text).map_err(|e| {
                FaucetError::Source(format!("azure JSON parse error in '{key}': {e}"))
            })?;
            match value {
                Value::Array(arr) => Ok(arr),
                other => Err(FaucetError::Source(format!(
                    "azure expected JSON array in '{key}', got {}",
                    value_type_name(&other)
                ))),
            }
        }
        // The shared-format objects (#604) are decoded at fetch time by
        // `fetch_decoded`, so this synchronous text parser is never on their
        // path — an arm here is an internal invariant violation.
        #[cfg(feature = "file-format-csv")]
        AzureFileFormat::Csv => Err(shared_format_via_text(key, "csv")),
        #[cfg(feature = "file-format-xml")]
        AzureFileFormat::Xml => Err(shared_format_via_text(key, "xml")),
        #[cfg(feature = "file-format-excel")]
        AzureFileFormat::Xlsx => Err(shared_format_via_text(key, "xlsx")),
        #[cfg(feature = "file-format-avro")]
        AzureFileFormat::Avro => Err(shared_format_via_text(key, "avro")),
        #[cfg(feature = "file-format-orc")]
        AzureFileFormat::Orc => Err(shared_format_via_text(key, "orc")),
        #[cfg(feature = "arrow")]
        AzureFileFormat::Parquet => Err(shared_format_via_text(key, "parquet")),
        AzureFileFormat::RawText => Ok(vec![serde_json::json!({
            "key": key,
            "content": text,
        })]),
    }
}

/// The [`IntegrityCheck`](faucet_core::IntegrityCheck) set for one blob read
/// (#161). Pure so the decision is unit-testable without an Azure endpoint.
///
/// Empty when verification is off, or when the blob is served with a non-empty
/// `Content-Encoding` — a store may decompressively transcode on read, so the
/// received byte count would not match the stored `size`. Azure Blob exposes no
/// body checksum through `object_store`, so the length check is the only one
/// available here (`verify_checksum` is refused at config load).
fn length_checks(
    size: u64,
    content_encoding: Option<&str>,
    verify_length: bool,
) -> Vec<Box<dyn faucet_core::IntegrityCheck>> {
    if verify_length && content_encoding.is_none_or(str::is_empty) {
        vec![Box::new(faucet_core::LengthCheck::new(size))]
    } else {
        Vec::new()
    }
}

/// The `Content-Encoding` an object-store `get` reported, if any.
fn content_encoding(attributes: &object_store::Attributes) -> Option<&str> {
    attributes
        .get(&object_store::Attribute::ContentEncoding)
        .map(AsRef::as_ref)
}

/// Truncate an explicit object-key list to the `max_objects` cap. `None` leaves
/// the list untouched.
/// The whole-segment directory to list for `prefix`: object_store lists by
/// path segment, so `logs/2026-05-` is listed as `logs/` and filtered.
fn list_root(prefix: &str) -> Option<ObjectPath> {
    prefix
        .rfind('/')
        .map(|i| object_path(&prefix[..i]))
        .filter(|p| !p.as_ref().is_empty())
}

/// The object_store path for a blob name exactly as listed or configured.
/// `Path::from` would percent-encode characters such as `%`, `[` and `#` a
/// second time and address a different blob.
fn object_path(key: &str) -> ObjectPath {
    ObjectPath::parse(key).unwrap_or_else(|_| ObjectPath::from(key))
}

fn cap_keys(mut keys: Vec<String>, max: Option<usize>) -> Vec<String> {
    if let Some(n) = max {
        keys.truncate(n);
    }
    keys
}

fn value_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[async_trait]
impl faucet_core::Source for AzureBlobSource {
    async fn fetch_with_context(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        // Every format decoded at fetch time (CSV / XML / Excel / Parquet) and
        // the containers (Avro / ORC, decoded in listing order against the
        // first blob's schema) go through the ordered page stream; only the
        // three text formats are parsed here.
        if !matches!(
            self.config.file_format,
            AzureFileFormat::JsonLines | AzureFileFormat::JsonArray | AzureFileFormat::RawText
        ) {
            let mut out = Vec::new();
            let mut pages = self.stream_pages(context, 0);
            while let Some(page) = pages.next().await {
                out.extend(page?.records);
            }
            return Ok(out);
        }
        let substituted_prefix: Option<String> = if !context.is_empty() {
            self.config
                .prefix
                .as_ref()
                .map(|p| faucet_core::util::substitute_context(p, context))
        } else {
            None
        };

        let keys = self
            .list_object_names(substituted_prefix.as_deref())
            .await?;
        tracing::info!(
            container = %self.config.container(),
            objects = keys.len(),
            "Listed Azure objects",
        );

        let concurrency = self.config.concurrency.max(1);
        let results: Vec<Vec<Value>> = stream::iter(keys)
            .map(|key| async move {
                let text = self.read_object_text(&key).await?;
                let records = self.parse_content(&key, &text)?;
                tracing::debug!(key = %key, records = records.len(), "Read Azure object");
                Ok::<Vec<Value>, FaucetError>(records)
            })
            .buffer_unordered(concurrency)
            .try_collect()
            .await?;

        let all_records: Vec<Value> = results.into_iter().flatten().collect();
        tracing::info!(total_records = all_records.len(), "Azure fetch complete");
        Ok(all_records)
    }

    /// Stream records from listed Azure objects without buffering the full
    /// scan. Mirrors the S3/GCS object sources — see those for the per-format
    /// reasoning. `batch_size = 0` emits one page per object.
    fn stream_pages<'a>(
        &'a self,
        context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let batch_size = self.config.batch_size;

        Box::pin(async_stream::try_stream! {
            let substituted_prefix: Option<String> = if !context.is_empty() {
                self.config
                    .prefix
                    .as_ref()
                    .map(|p| faucet_core::util::substitute_context(p, context))
            } else {
                None
            };

            let keys = self.list_object_names(substituted_prefix.as_deref()).await?;
            tracing::info!(
                container = %self.config.container(),
                objects = keys.len(),
                "Listed Azure objects (stream)",
            );

            let chunk = if batch_size == 0 { usize::MAX } else { batch_size };
            let initial_capacity = if batch_size == 0 { 1024 } else { batch_size };
            let mut buffer: Vec<Value> = Vec::with_capacity(initial_capacity);
            let mut total = 0usize;

            // Overlap the blob reads (#619). `buffered` keeps listing order,
            // so a blob that fails surfaces at exactly the point it would have
            // serially — concurrency changes throughput, not semantics. The key
            // is moved into each future rather than borrowed: a closure
            // returning a borrow-capturing async block is not higher-ranked
            // enough for `buffered`.
            let concurrency = self.config.concurrency.max(1);
            let mut fetched = stream::iter(keys.iter().cloned())
                .map(|key| async move {
                    let payload = self.fetch(&key).await;
                    (key, payload)
                })
                .buffered(concurrency);
            #[cfg(any(feature = "file-format-avro", feature = "file-format-orc"))]
            let mut container = self.container_decoder()?;

            while let Some((key, payload)) = fetched.next().await {
                let key = &key;
                let payload = payload?;
                #[cfg(any(feature = "file-format-avro", feature = "file-format-orc"))]
                let payload = match payload {
                    Fetched::Container(bytes) => {
                        let (d, rows) = container
                            .take()
                            .expect("a container object implies a container format")
                            .decode_all_offloaded(key.to_string(), faucet_core::FileInput::Bytes(bytes))
                            .await?;
                        container = Some(d);
                        Fetched::Records(rows)
                    }
                    other => other,
                };
                match payload {
                    Fetched::Lines(reader) => {
                        let mut lines = reader.lines();
                        let mut line_num: usize = 0;
                        while let Some(line) = lines
                            .next_line()
                            .await
                            .map_err(|e| FaucetError::Source(format!(
                                "azure read body error for key '{key}': {e}"
                            )))?
                        {
                            line_num += 1;
                            let trimmed = line.trim();
                            if trimmed.is_empty() { continue; }
                            let value: Value = serde_json::from_str(trimmed).map_err(|e| {
                                FaucetError::Source(format!(
                                    "azure JSON parse error in '{key}' at line {line_num}: {e}",
                                ))
                            })?;
                            buffer.push(value);
                            if batch_size != 0 && buffer.len() >= chunk {
                                let page = std::mem::replace(
                                    &mut buffer,
                                    Vec::with_capacity(initial_capacity),
                                );
                                total += page.len();
                                yield StreamPage { records: page, bookmark: None };
                            }
                        }
                        if batch_size == 0 && !buffer.is_empty() {
                            let page = std::mem::take(&mut buffer);
                            total += page.len();
                            yield StreamPage { records: page, bookmark: None };
                        }
                    }
                    Fetched::RawText(text) => {
                        let record = serde_json::json!({ "key": key, "content": text });
                        buffer.push(record);
                        if batch_size == 0 {
                            let page = std::mem::take(&mut buffer);
                            total += page.len();
                            yield StreamPage { records: page, bookmark: None };
                        } else if buffer.len() >= chunk {
                            let page = std::mem::replace(
                                &mut buffer,
                                Vec::with_capacity(initial_capacity),
                            );
                            total += page.len();
                            yield StreamPage { records: page, bookmark: None };
                        }
                    }
                    Fetched::JsonArray(text) => {
                        let value: Value = serde_json::from_str(&text).map_err(|e| {
                            FaucetError::Source(format!("azure JSON parse error in '{key}': {e}"))
                        })?;
                        let array = match value {
                            Value::Array(arr) => arr,
                            other => Err(FaucetError::Source(format!(
                                "azure expected JSON array in '{key}', got {}",
                                value_type_name(&other)
                            )))?,
                        };
                        if batch_size == 0 {
                            if !buffer.is_empty() {
                                let page = std::mem::take(&mut buffer);
                                total += page.len();
                                yield StreamPage { records: page, bookmark: None };
                            }
                            total += array.len();
                            yield StreamPage { records: array, bookmark: None };
                        } else {
                            for record in array {
                                buffer.push(record);
                                if buffer.len() >= chunk {
                                    let page = std::mem::replace(
                                        &mut buffer,
                                        Vec::with_capacity(initial_capacity),
                                    );
                                    total += page.len();
                                    yield StreamPage { records: page, bookmark: None };
                                }
                            }
                        }
                    }
                    #[cfg(any(feature = "file-format-avro", feature = "file-format-orc"))]
                    Fetched::Container(_) => unreachable!("decoded above"),
                    #[cfg(feature = "arrow")]
                    Fetched::Parquet(mut batches) => {
                        while let Some(batch) = batches.next().await {
                            for record in faucet_core::columnar::record_batch_to_values(&batch?)? {
                                buffer.push(record);
                                if batch_size != 0 && buffer.len() >= chunk {
                                    let page = std::mem::replace(
                                        &mut buffer,
                                        Vec::with_capacity(initial_capacity),
                                    );
                                    total += page.len();
                                    yield StreamPage { records: page, bookmark: None };
                                }
                            }
                        }
                        if batch_size == 0 && !buffer.is_empty() {
                            let page = std::mem::take(&mut buffer);
                            total += page.len();
                            yield StreamPage { records: page, bookmark: None };
                        }
                    }
                    #[cfg(any(
                        feature = "file-format-csv",
                        feature = "file-format-xml",
                        feature = "file-format-excel",
                        feature = "file-format-avro",
                        feature = "file-format-orc"
                    ))]
                    Fetched::Records(records) => {
                        // CSV / XML / Excel (#604): already decoded at fetch
                        // time, so chunk exactly as a JSON array is chunked.
                        if batch_size == 0 {
                            if !buffer.is_empty() {
                                let page = std::mem::take(&mut buffer);
                                total += page.len();
                                yield StreamPage { records: page, bookmark: None };
                            }
                            total += records.len();
                            yield StreamPage { records, bookmark: None };
                        } else {
                            for record in records {
                                buffer.push(record);
                                if buffer.len() >= chunk {
                                    let page = std::mem::replace(
                                        &mut buffer,
                                        Vec::with_capacity(initial_capacity),
                                    );
                                    total += page.len();
                                    yield StreamPage { records: page, bookmark: None };
                                }
                            }
                        }
                    }
                }
            }

            if !buffer.is_empty() {
                let page = std::mem::take(&mut buffer);
                total += page.len();
                yield StreamPage { records: page, bookmark: None };
            }

            tracing::info!(
                total_records = total,
                batch_size,
                objects = keys.len(),
                "Azure source stream complete",
            );
        })
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(AzureBlobSourceConfig))
            .expect("schema serialization")
    }

    fn connector_name(&self) -> &'static str {
        "azure-blob"
    }

    /// Avro, ORC and Parquet blobs decode straight to Arrow, so they take the
    /// columnar path; every other format stays on the row path (#719, #777).
    #[cfg(feature = "arrow")]
    fn supports_columnar(&self) -> bool {
        self.is_container() || matches!(self.config.file_format, AzureFileFormat::Parquet)
    }

    /// Stream Avro / ORC blobs (each resolved against the first one's schema)
    /// or Parquet blobs (projected by `parquet.columns`) as Arrow batches, in
    /// listing order.
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
            let prefix = if context.is_empty() {
                None
            } else {
                self.config
                    .prefix
                    .as_ref()
                    .map(|p| faucet_core::util::substitute_context(p, context))
            };
            if matches!(self.config.file_format, AzureFileFormat::Parquet) {
                let keys = self.list_object_names(prefix.as_deref()).await?;
                let mut first: Option<arrow::datatypes::SchemaRef> = None;
                // Prefetch the next blobs' footers while this one's row
                // groups decode; bodies are never buffered (#783).
                let batch_size = self.config.batch_size;
                let mut opened = stream::iter(keys)
                    .map(|key| async move {
                        let opened = self.open_parquet(&key, batch_size).await;
                        (key, opened)
                    })
                    .buffered(self.config.concurrency.max(1));
                while let Some((key, opened)) = opened.next().await {
                    let (schema, mut batches) = opened?;
                    match &first {
                        Some(f) if !faucet_core::columnar::schema_eq(f, &schema) => {
                            Err(FaucetError::Source(format!(
                                "azure parquet blob '{key}' has a different schema from the \
                                 first blob in the listing"
                            )))?;
                        }
                        Some(_) => {}
                        None => first = Some(schema),
                    }
                    while let Some(batch) = batches.next().await {
                        let batch = batch?;
                        if batch.num_rows() > 0 {
                            yield faucet_core::columnar::ColumnarPage::new(batch, None);
                        }
                    }
                }
                return;
            }
            let keys = self.list_object_names(prefix.as_deref()).await?;
            let mut pages = self.container_batches(keys)?;
            while let Some(page) = pages.next().await {
                yield page?;
            }
        })
    }

    fn dataset_uri(&self) -> String {
        match &self.config.prefix {
            Some(p) => format!("az://{}/{}", self.config.container(), p),
            None => format!("az://{}", self.config.container()),
        }
    }

    /// Preflight probe: confirm the container is reachable and the credentials
    /// work via a non-mutating listing capped at a single item. Reads no object
    /// bodies.
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};

        let started = std::time::Instant::now();
        let probe = match tokio::time::timeout(ctx.timeout, async {
            let mut listing = self.store.list(None);
            listing.next().await
        })
        .await
        {
            // Reachable — an empty container (None) is still a pass.
            Ok(None) | Ok(Some(Ok(_))) => Probe::pass("auth", started.elapsed()),
            Ok(Some(Err(e))) => Probe::fail_hint(
                "auth",
                started.elapsed(),
                e.to_string(),
                "check account, container, credentials, and network",
            ),
            Err(_) => Probe::fail("network", started.elapsed(), "timed out"),
        };
        Ok(CheckReport::single(probe))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::Source as _;
    use serde_json::json;

    #[test]
    fn value_type_name_covers_all_json_variants() {
        assert_eq!(value_type_name(&Value::Null), "null");
        assert_eq!(value_type_name(&json!(true)), "boolean");
        assert_eq!(value_type_name(&json!(7)), "number");
        assert_eq!(value_type_name(&json!("s")), "string");
        assert_eq!(value_type_name(&json!([1, 2])), "array");
        assert_eq!(value_type_name(&json!({"k": 1})), "object");
    }

    #[test]
    fn parse_json_lines() {
        let r = parse_file_content(&AzureFileFormat::JsonLines, "t", "{\"id\":1}\n{\"id\":2}\n")
            .unwrap();
        assert_eq!(r.len(), 2);
        assert_eq!(r[0]["id"], 1);
    }

    #[test]
    fn parse_json_lines_skips_blanks() {
        let r = parse_file_content(
            &AzureFileFormat::JsonLines,
            "t",
            "{\"id\":1}\n\n{\"id\":2}\n\n",
        )
        .unwrap();
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn parse_json_lines_reports_line_number() {
        let err = parse_file_content(&AzureFileFormat::JsonLines, "t", "{\"id\":1}\nbad-line\n")
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("line 2"), "unexpected: {msg}");
    }

    #[test]
    fn parse_json_array() {
        let r = parse_file_content(
            &AzureFileFormat::JsonArray,
            "t.json",
            "[{\"id\":1},{\"id\":2}]",
        )
        .unwrap();
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn parse_json_array_rejects_non_array() {
        let err =
            parse_file_content(&AzureFileFormat::JsonArray, "t.json", "{\"id\":1}").unwrap_err();
        assert!(err.to_string().contains("expected JSON array"));
    }

    #[test]
    fn parse_json_array_rejects_malformed_json() {
        let err =
            parse_file_content(&AzureFileFormat::JsonArray, "t.json", "[not json").unwrap_err();
        assert!(matches!(err, FaucetError::Source(_)));
    }

    #[test]
    fn parse_raw_text_yields_single_record() {
        let r = parse_file_content(&AzureFileFormat::RawText, "p/f.txt", "hello").unwrap();
        assert_eq!(r, vec![json!({"key": "p/f.txt", "content": "hello"})]);
    }

    #[test]
    fn listing_roots_are_whole_segments_and_names_are_not_reencoded() {
        assert_eq!(list_root("logs/2026-05-").unwrap().as_ref(), "logs");
        assert_eq!(list_root("a/b/").unwrap().as_ref(), "a/b");
        assert!(list_root("plain").is_none());
        assert!(list_root("").is_none());
        for name in ["data[2024].csv", "a%20b.json", "~$x.xlsx", "a#1.json"] {
            assert_eq!(object_path(name).as_ref(), name);
        }
    }

    #[test]
    fn cap_keys_truncates_explicit_list_to_max_objects() {
        let keys = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        assert_eq!(
            cap_keys(keys, Some(2)),
            vec!["a".to_string(), "b".to_string()]
        );
    }

    #[test]
    fn cap_keys_passes_through_when_no_max() {
        let keys = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        assert_eq!(cap_keys(keys.clone(), None), keys);
    }

    #[test]
    fn cap_keys_noop_when_max_exceeds_len() {
        let keys = vec!["a".to_string(), "b".to_string()];
        assert_eq!(cap_keys(keys.clone(), Some(10)), keys);
    }

    // dataset_uri logic mirrors the built source without needing an Azure
    // client (construction builds the object store).
    #[test]
    fn dataset_uri_no_prefix_logic() {
        let config = AzureBlobSourceConfig::new("my-container");
        let uri = match &config.prefix {
            Some(p) => format!("az://{}/{}", config.container(), p),
            None => format!("az://{}", config.container()),
        };
        assert_eq!(uri, "az://my-container");
    }

    #[test]
    fn dataset_uri_with_prefix_logic() {
        let config = AzureBlobSourceConfig::new("my-container").prefix("data/2026/");
        let uri = match &config.prefix {
            Some(p) => format!("az://{}/{}", config.container(), p),
            None => format!("az://{}", config.container()),
        };
        assert_eq!(uri, "az://my-container/data/2026/");
    }

    // ---- read-integrity verification (#161) ----

    /// Read `body` through the verifier with the checks `length_checks` picks
    /// for `size`/`content_encoding`, returning whether the read succeeded.
    async fn reads_ok(size: u64, content_encoding: Option<&str>, body: &[u8]) -> bool {
        use tokio::io::AsyncReadExt as _;
        let checks = length_checks(size, content_encoding, true);
        let mut reader = faucet_core::VerifyingReader::new(body, checks);
        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.is_ok()
    }

    #[tokio::test]
    async fn length_check_accepts_a_complete_body() {
        assert!(reads_ok(5, None, b"hello").await);
    }

    #[tokio::test]
    async fn length_check_rejects_a_truncated_body() {
        assert!(
            !reads_ok(10, None, b"hello").await,
            "a cleanly-truncated transfer must fail, not parse as complete"
        );
    }

    #[tokio::test]
    async fn length_check_rejects_an_overlong_body() {
        assert!(!reads_ok(3, None, b"hello").await);
    }

    #[tokio::test]
    async fn length_check_skipped_for_a_transcoded_blob() {
        // A non-empty Content-Encoding means the received bytes need not match
        // the stored size, so no check is installed and the read passes.
        assert!(reads_ok(999, Some("gzip"), b"hello").await);
    }

    #[test]
    fn length_checks_empty_when_disabled() {
        assert!(length_checks(5, None, false).is_empty());
    }

    #[test]
    fn length_checks_installed_for_empty_content_encoding() {
        // An explicitly empty header is equivalent to absent.
        assert_eq!(length_checks(5, Some(""), true).len(), 1);
        assert_eq!(length_checks(5, None, true).len(), 1);
    }

    #[test]
    fn content_encoding_reads_the_attribute() {
        let mut attrs = object_store::Attributes::new();
        assert_eq!(content_encoding(&attrs), None);
        attrs.insert(object_store::Attribute::ContentEncoding, "gzip".into());
        assert_eq!(content_encoding(&attrs), Some("gzip"));
    }

    #[tokio::test]
    async fn new_rejects_verify_checksum() {
        // Azure exposes no body checksum through object_store, so the switch is
        // refused at load time rather than accepted and silently ignored.
        let mut config = AzureBlobSourceConfig::new("c");
        config.verify_checksum = true;
        match AzureBlobSource::new(config).await {
            Err(FaucetError::Config(m)) => {
                assert!(m.contains("verify_checksum"), "got: {m}")
            }
            Ok(_) => panic!("expected a verify_checksum Config error, got Ok(source)"),
            Err(e) => panic!("expected a verify_checksum Config error, got {e:?}"),
        }
    }

    #[tokio::test]
    async fn new_rejects_empty_container() {
        match AzureBlobSource::new(AzureBlobSourceConfig::new("   ")).await {
            Err(FaucetError::Config(m)) => assert!(m.contains("container"), "got: {m}"),
            Ok(_) => panic!("expected a container Config error, got Ok(source)"),
            Err(e) => panic!("expected a container Config error, got {e:?}"),
        }
    }

    #[tokio::test]
    async fn new_rejects_out_of_range_batch_size() {
        let config =
            AzureBlobSourceConfig::new("c").with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        match AzureBlobSource::new(config).await {
            Err(FaucetError::Config(m)) => assert!(m.contains("batch_size"), "got: {m}"),
            Ok(_) => panic!("expected a batch_size Config error, got Ok(source)"),
            Err(e) => panic!("expected a batch_size Config error, got {e:?}"),
        }
    }

    #[tokio::test]
    async fn new_builds_lazily_with_emulator() {
        // The object-store builder is lazy — no I/O — so a well-formed emulator
        // config constructs a source without a reachable backend.
        let config = AzureBlobSourceConfig::new("c")
            .use_emulator(true)
            .allow_http(true);
        let source = AzureBlobSource::new(config).await.unwrap();
        assert_eq!(source.connector_name(), "azure-blob");
        assert_eq!(source.dataset_uri(), "az://c");
    }

    #[cfg(all(feature = "file-format-avro", feature = "file-format-orc"))]
    #[test]
    fn container_formats_never_reach_the_text_parser() {
        for fmt in [AzureFileFormat::Avro, AzureFileFormat::Orc] {
            let err = parse_file_content(&fmt, "k", "").unwrap_err();
            assert!(err.to_string().contains("internal error"), "{err}");
        }
    }

    #[cfg(feature = "arrow")]
    #[test]
    fn a_parquet_object_never_reaches_the_text_parser() {
        let e = parse_file_content(&AzureFileFormat::Parquet, "k.parquet", "")
            .unwrap_err()
            .to_string();
        assert!(
            e.contains("azure parquet object 'k.parquet' reached the text parser"),
            "{e}"
        );
    }
}
