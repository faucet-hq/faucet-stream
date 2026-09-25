//! GCS source stream executor.

use crate::config::{GcsFileFormat, GcsSourceConfig};
use async_trait::async_trait;
use faucet_common_gcs::{build_storage, build_storage_control};
use faucet_core::shard::{HashShard, ShardSpec, parse_hash_shard, plan_hash_shards};
use faucet_core::{FaucetError, Stream, StreamPage};
use futures::stream::{self, StreamExt, TryStreamExt};
use google_cloud_gax::paginator::ItemPaginator;
use google_cloud_storage::client::{Storage, StorageControl};
use serde_json::Value;
use std::pin::Pin;
use std::sync::Mutex;
use tokio::io::AsyncBufReadExt;

/// One object's payload, fetched in the shape its `file_format` decodes from.
///
/// Prefetching these with a bounded look-ahead is what makes `concurrency`
/// real on the streaming path (#619). The bound it promises differs by format
/// and that difference is the point: `JsonLines` overlaps only the request
/// round-trip — `open_object_reader` does not download the body, so the decode
/// still streams line-by-line at O(batch) memory — while the whole-object
/// formats hold at most `concurrency` bodies at once.
enum Fetched {
    Lines(Pin<Box<dyn tokio::io::AsyncBufRead + Send + Unpin>>),
    RawText(String),
    JsonArray(String),
    /// A Parquet object being read row group at a time over byte ranges — the
    /// memory-bounded path (#619).
    #[cfg(feature = "arrow")]
    ParquetStream(
        Box<
            parquet::arrow::async_reader::ParquetRecordBatchStream<
                crate::parquet_range::GcsRangeReader,
            >,
        >,
    ),
    /// A Parquet object buffered whole, for the configurations a ranged read
    /// cannot serve: a compressed object is not randomly addressable, and a
    /// whole-object checksum can only be verified by reading the whole object.
    #[cfg(feature = "arrow")]
    Parquet(bytes::Bytes),
    /// A whole object already decoded into records by
    /// [`faucet_core::file_format`] (#604) — CSV, XML and Excel. Decoding at
    /// fetch time keeps the per-format work in one place; the page loop then
    /// chunks these exactly as it chunks a JSON array's.
    #[cfg(any(
        feature = "file-format-csv",
        feature = "file-format-xml",
        feature = "file-format-excel"
    ))]
    Records(Vec<Value>),
}

/// A GCS source that lists and reads objects from a bucket.
pub struct GcsSource {
    config: GcsSourceConfig,
    storage: Storage,
    control: StorageControl,
    /// Shard applied by the cluster coordinator (Mode B). `None` (or a
    /// degenerate single-shard set) reads every listed object. Stored behind a
    /// `Mutex` so `apply_shard(&self, …)` can record it before streaming.
    applied_shard: Mutex<Option<HashShard>>,
}

impl GcsSource {
    /// Construct the source. Builds both clients eagerly so they are
    /// reused across calls.
    pub async fn new(config: GcsSourceConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let storage = build_storage(&config.auth, config.storage_host.as_deref()).await?;
        let control = build_storage_control(&config.auth, config.storage_host.as_deref()).await?;
        Ok(Self {
            config,
            storage,
            control,
            applied_shard: Mutex::new(None),
        })
    }

    /// Retain only the keys belonging to the applied shard (hash-of-key modulo
    /// `shards`). A no-op when no shard is applied.
    fn shard_filter(&self, keys: Vec<String>) -> Vec<String> {
        filter_shard_keys(
            keys,
            *self.applied_shard.lock().expect("shard mutex poisoned"),
        )
    }

    /// Bucket as a GCS resource path: `projects/_/buckets/{bucket}`.
    fn bucket_path(&self) -> String {
        format!("projects/_/buckets/{}", self.config.bucket)
    }

    /// List object names under the configured (or override) prefix,
    /// capped at `max_objects` if set.
    async fn list_object_names(
        &self,
        prefix_override: Option<&str>,
    ) -> Result<Vec<String>, FaucetError> {
        if let Some(ref keys) = self.config.object_keys {
            return Ok(self.shard_filter(cap_keys(keys.clone(), self.config.max_objects)));
        }

        let effective_prefix = prefix_override.or(self.config.prefix.as_deref());
        let mut req = self.control.list_objects().set_parent(self.bucket_path());
        if let Some(p) = effective_prefix {
            req = req.set_prefix(p.to_string());
        }
        req = req.set_page_size(1000_i32);

        let mut paginator = req.by_item();
        let mut names: Vec<String> = Vec::new();
        while let Some(item) = paginator.next().await {
            let object = item.map_err(|e| {
                FaucetError::Source(format!(
                    "GCS list error for bucket '{}': {e}",
                    self.config.bucket
                ))
            })?;
            if object.name.is_empty() {
                continue;
            }
            names.push(object.name);
            if let Some(max) = self.config.max_objects
                && names.len() >= max
            {
                break;
            }
        }
        // Shard-filter AFTER the max_objects cap so the cap bounds the run's
        // total object set (matching single-worker semantics) rather than
        // multiplying by the shard count.
        Ok(self.shard_filter(names))
    }

    /// Fetch one object in the shape its configured `file_format` needs.
    async fn fetch(&self, key: &str) -> Result<Fetched, FaucetError> {
        Ok(match self.config.file_format {
            GcsFileFormat::JsonLines => Fetched::Lines(self.open_object_reader(key).await?),
            GcsFileFormat::RawText => Fetched::RawText(self.read_object_text(key).await?),
            GcsFileFormat::JsonArray => Fetched::JsonArray(self.read_object_text(key).await?),
            #[cfg(feature = "arrow")]
            GcsFileFormat::Parquet => match self.parquet_row_group_stream(key).await? {
                Some(stream) => Fetched::ParquetStream(Box::new(stream)),
                None => Fetched::Parquet(self.read_object_bytes(key).await?),
            },
            #[cfg(feature = "file-format-csv")]
            GcsFileFormat::Csv => self.fetch_decoded(key).await?,
            #[cfg(feature = "file-format-xml")]
            GcsFileFormat::Xml => self.fetch_decoded(key).await?,
            #[cfg(feature = "file-format-excel")]
            GcsFileFormat::Xlsx => self.fetch_decoded(key).await?,
        })
    }

    /// Read one object whole and decode it through the shared format layer.
    ///
    /// Whole-object for all three: a workbook's directory sits at the end of a
    /// zip container, an XML document is a tree, and a CSV's records are
    /// chunked by the same page loop either way — so one buffered path keeps
    /// the decode in one place rather than three.
    #[cfg(any(
        feature = "file-format-csv",
        feature = "file-format-xml",
        feature = "file-format-excel"
    ))]
    async fn fetch_decoded(&self, key: &str) -> Result<Fetched, FaucetError> {
        let bytes = self.read_object_all(key).await?;
        let format =
            self.config.file_format.shared().ok_or_else(|| {
                FaucetError::Source(format!("GCS '{key}': format has no decoder"))
            })?;
        let records =
            faucet_core::file_format::decode(&bytes, format, &self.config.format_options())
                .await
                .map_err(|e| FaucetError::Source(format!("GCS '{key}': {e}")))?;
        Ok(Fetched::Records(records))
    }

    /// Whether this object can be read row group at a time, and a reader for it
    /// if so (#619).
    ///
    /// Two configurations rule it out, and both are guarantees worth more than
    /// the memory saving:
    ///
    /// - `verify_checksum` — GCS's CRC32C/MD5 covers the whole object, so
    ///   honouring it means reading the object as one stream;
    /// - `compression` — a gzip / zstd member is not randomly addressable, so
    ///   the Parquet footer cannot be located without inflating everything.
    ///
    /// In either case the whole-object path still runs, unchanged.
    /// `verify_length` needs no exception: it guards a truncated *transfer*,
    /// and the ranged reader enforces exactly that per fetch.
    #[cfg(feature = "arrow")]
    async fn parquet_row_group_stream(
        &self,
        key: &str,
    ) -> Result<
        Option<
            parquet::arrow::async_reader::ParquetRecordBatchStream<
                crate::parquet_range::GcsRangeReader,
            >,
        >,
        FaucetError,
    > {
        use parquet::arrow::ParquetRecordBatchStreamBuilder;

        if self.config.verify_checksum {
            return Ok(None);
        }
        #[cfg(feature = "compression")]
        if self.config.compression.resolve(key) != faucet_core::compression::Compression::None {
            return Ok(None);
        }

        // The footer's position is only knowable from the object's size, which
        // the control plane reports without transferring any data.
        let meta = self
            .control
            .get_object()
            .set_bucket(self.bucket_path())
            .set_object(key.to_string())
            .send()
            .await
            .map_err(|e| {
                FaucetError::Source(format!("GCS get object metadata for '{key}' failed: {e}"))
            })?;
        let len = u64::try_from(meta.size).map_err(|_| {
            FaucetError::Source(format!(
                "GCS object '{key}' reports a negative size ({})",
                meta.size
            ))
        })?;

        let reader = crate::parquet_range::GcsRangeReader::open(
            &self.storage,
            &self.bucket_path(),
            key,
            len,
        )
        .await?;
        let object_len = reader.len();
        let mut builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .map_err(|e| {
                FaucetError::Source(format!("failed to read parquet metadata for '{key}': {e}"))
            })?;
        // Cap the Arrow batch size to the page size so a single huge row group
        // is still decoded in bounded steps — the row-group bound alone is not
        // enough when one row group holds millions of rows.
        if self.config.batch_size > 0 {
            builder = builder.with_batch_size(self.config.batch_size);
        }
        let stream = builder.build().map_err(|e| {
            FaucetError::Source(format!("failed to build parquet reader for '{key}': {e}"))
        })?;
        tracing::debug!(
            key = %key,
            object_bytes = object_len,
            "streaming parquet object by row group"
        );
        Ok(Some(stream))
    }

    /// Decode already-fetched Parquet bytes on a blocking thread.
    #[cfg(feature = "arrow")]
    async fn decode_parquet(
        data: bytes::Bytes,
        key: &str,
    ) -> Result<(arrow::datatypes::SchemaRef, Vec<arrow::array::RecordBatch>), FaucetError> {
        let key_owned = key.to_string();
        tokio::task::spawn_blocking(move || decode_parquet_bytes(data, &key_owned))
            .await
            .map_err(|e| {
                FaucetError::Source(format!("parquet decode task for '{key}' panicked: {e}"))
            })?
    }

    /// Read the full body of a single GCS object into a UTF-8 `String`.
    async fn read_object_text(&self, key: &str) -> Result<String, FaucetError> {
        // Stream the (optionally decompressed) body straight into one String
        // via the same reader the line-streaming path uses, instead of holding
        // the raw bytes AND the decompressed bytes AND the String at once
        // (#78/#25). For JsonArray / RawText the whole object is still one
        // unit, but peak memory is now ~1× the decoded size rather than ~3×.
        use tokio::io::AsyncReadExt as _;
        let mut reader = self.open_object_reader(key).await?;
        let mut text = String::new();
        reader.read_to_string(&mut text).await.map_err(|e| {
            FaucetError::Source(format!(
                "GCS read/decode error for key '{key}' (not valid UTF-8?): {e}"
            ))
        })?;
        Ok(text)
    }

    /// Open a GCS object as an `AsyncBufRead` over its body so callers can
    /// decode line-by-line without buffering the entire object.
    ///
    /// Requires the `unstable-stream` feature on `google-cloud-storage`.
    async fn open_object_reader(
        &self,
        key: &str,
    ) -> Result<std::pin::Pin<Box<dyn tokio::io::AsyncBufRead + Send + Unpin>>, FaucetError> {
        let resp = self
            .storage
            .read_object(self.bucket_path(), key.to_string())
            .send()
            .await
            .map_err(|e| {
                FaucetError::Source(format!(
                    "GCS get error for bucket '{}' key '{key}': {e}",
                    self.config.bucket
                ))
            })?;
        // Read the object metadata (size, content-encoding, checksums) BEFORE
        // consuming the stream, so a cleanly-truncated/corrupted transfer is
        // rejected rather than silently parsed as a complete object (#161).
        let highlights = resp.object();
        let mut checks: Vec<Box<dyn faucet_core::IntegrityCheck>> = Vec::new();
        match crate::verify::length_check(
            highlights.size,
            &highlights.content_encoding,
            self.config.verify_length,
        ) {
            Some(check) => checks.push(check),
            None if self.config.verify_length => tracing::debug!(
                key = %key,
                size = highlights.size,
                content_encoding = %highlights.content_encoding,
                "GCS object length verification skipped (no size or transcoded encoding)"
            ),
            None => {}
        }
        if self.config.verify_checksum {
            let (crc32c, md5) = match &highlights.checksums {
                Some(c) => (c.crc32c, c.md5_hash.clone()),
                None => (None, bytes::Bytes::new()),
            };
            match crate::verify::checksum_check(crc32c, &md5, &highlights.content_encoding) {
                Some(check) => checks.push(check),
                None if highlights.content_encoding.is_empty() => tracing::warn!(
                    key = %key,
                    "verify_checksum is enabled but GCS advertised no verifiable checksum for \
                     this object; relying on the length check only"
                ),
                None => {}
            }
        }

        let bytes_stream = resp
            .into_stream()
            .map_err(|e| std::io::Error::other(e.to_string()));
        // Wrap the RAW byte stream in the verifier first so length/checksum
        // cover the stored bytes (below any client-side decompression).
        let verified = faucet_core::VerifyingReader::new(
            tokio_util::io::StreamReader::new(bytes_stream),
            checks,
        );
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

    /// Download a single GCS object's full body into an in-memory
    /// [`bytes::Bytes`], reusing [`open_object_reader`](Self::open_object_reader)
    /// so length/checksum verification (and any configured decompression)
    /// still apply. Used only by the Parquet path (Parquet is binary, so it
    /// cannot go through [`read_object_text`](Self::read_object_text)).
    #[cfg(feature = "arrow")]
    async fn read_object_bytes(&self, key: &str) -> Result<bytes::Bytes, FaucetError> {
        Ok(bytes::Bytes::from(self.read_object_all(key).await?))
    }

    /// The same whole-body read, without the `bytes` dependency the Parquet
    /// path brings — the shared formats (#604) need the bytes but not Arrow.
    #[cfg(any(
        feature = "arrow",
        feature = "file-format-csv",
        feature = "file-format-xml",
        feature = "file-format-excel"
    ))]
    async fn read_object_all(&self, key: &str) -> Result<Vec<u8>, FaucetError> {
        use tokio::io::AsyncReadExt as _;
        let mut reader = self.open_object_reader(key).await?;
        let mut buf = Vec::new();
        reader
            .read_to_end(&mut buf)
            .await
            .map_err(|e| FaucetError::Source(format!("GCS read error for key '{key}': {e}")))?;
        Ok(buf)
    }

    /// Decode a single Parquet object into its Arrow schema and batches. The
    /// whole object is buffered (matching the `JsonArray` model) and decoded on
    /// a blocking thread so the CPU-bound Parquet decode does not stall the
    /// async runtime.
    #[cfg(feature = "arrow")]
    async fn read_object_parquet(
        &self,
        key: &str,
    ) -> Result<(arrow::datatypes::SchemaRef, Vec<arrow::array::RecordBatch>), FaucetError> {
        Self::decode_parquet(self.read_object_bytes(key).await?, key).await
    }
}

/// The shared-format decoders run at fetch time, not through the text parser.
#[cfg(any(
    feature = "file-format-csv",
    feature = "file-format-xml",
    feature = "file-format-excel"
))]
fn shared_format_via_text(key: &str, format: &str) -> FaucetError {
    FaucetError::Source(format!(
        "GCS {format} object '{key}' reached the text parser (internal error: {format} is \
         decoded at fetch time)"
    ))
}

/// Parse file content into records for a given format. Free function (vs. a
/// `GcsSource` method) so it is unit-testable without a GCS client — the
/// parsing logic is pure. Previously this logic lived only inside the
/// `parse_content` method and was duplicated by a copy in the test module,
/// which could silently drift from production.
pub(crate) fn parse_file_content(
    format: &GcsFileFormat,
    key: &str,
    text: &str,
) -> Result<Vec<Value>, FaucetError> {
    match format {
        GcsFileFormat::JsonLines => {
            let mut records = Vec::new();
            for (line_num, line) in text.lines().enumerate() {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let value: Value = serde_json::from_str(trimmed).map_err(|e| {
                    FaucetError::Source(format!(
                        "GCS JSON parse error in '{key}' at line {}: {e}",
                        line_num + 1
                    ))
                })?;
                records.push(value);
            }
            Ok(records)
        }
        GcsFileFormat::JsonArray => {
            let value: Value = serde_json::from_str(text).map_err(|e| {
                FaucetError::Source(format!("GCS JSON parse error in '{key}': {e}"))
            })?;
            match value {
                Value::Array(arr) => Ok(arr),
                other => Err(FaucetError::Source(format!(
                    "GCS expected JSON array in '{key}', got {}",
                    value_type_name(&other)
                ))),
            }
        }
        GcsFileFormat::RawText => Ok(vec![serde_json::json!({
            "key": key,
            "content": text,
        })]),
        // The shared-format objects (#604) are decoded at fetch time by
        // `fetch_decoded`, and Parquet via `read_object_parquet`; neither
        // reaches this synchronous text parser, so an arm here is an internal
        // invariant violation rather than a user error.
        #[cfg(feature = "file-format-csv")]
        GcsFileFormat::Csv => Err(shared_format_via_text(key, "csv")),
        #[cfg(feature = "file-format-xml")]
        GcsFileFormat::Xml => Err(shared_format_via_text(key, "xml")),
        #[cfg(feature = "file-format-excel")]
        GcsFileFormat::Xlsx => Err(shared_format_via_text(key, "xlsx")),
        #[cfg(feature = "arrow")]
        GcsFileFormat::Parquet => Err(FaucetError::Source(format!(
            "GCS parquet object '{key}' cannot be parsed as text (internal error: \
             parquet must use the binary decode path)"
        ))),
    }
}

/// Hold every object in a columnar scan to the first object's schema.
///
/// A divergent *later* object aborts after earlier objects' pages have already
/// been written — the same non-atomic multi-object semantics the row path has.
#[cfg(feature = "arrow")]
fn check_schema(
    reference: &mut Option<arrow::datatypes::SchemaRef>,
    schema: &arrow::datatypes::SchemaRef,
    key: &str,
) -> Result<(), FaucetError> {
    match reference {
        Some(first) if first != schema => Err(FaucetError::Source(format!(
            "GCS source: parquet schema mismatch — object '{key}' diverges from the first \
             object's schema"
        ))),
        None => {
            *reference = Some(schema.clone());
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Decode a fully-buffered Parquet object into its Arrow schema and batches.
///
/// Synchronous (runs inside `spawn_blocking`). `bytes::Bytes` implements
/// `parquet`'s `ChunkReader`, so the in-memory reader needs no temp file. The
/// schema is captured before the reader is consumed so an object with zero
/// row-groups still reports a schema for cross-object consistency checks.
#[cfg(feature = "arrow")]
fn decode_parquet_bytes(
    data: bytes::Bytes,
    key: &str,
) -> Result<(arrow::datatypes::SchemaRef, Vec<arrow::array::RecordBatch>), FaucetError> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let builder = ParquetRecordBatchReaderBuilder::try_new(data).map_err(|e| {
        FaucetError::Source(format!("failed to read parquet metadata for '{key}': {e}"))
    })?;
    let schema = builder.schema().clone();
    let reader = builder.build().map_err(|e| {
        FaucetError::Source(format!("failed to build parquet reader for '{key}': {e}"))
    })?;

    let mut batches = Vec::new();
    for batch in reader {
        batches.push(
            batch.map_err(|e| {
                FaucetError::Source(format!("parquet decode error in '{key}': {e}"))
            })?,
        );
    }
    Ok((schema, batches))
}

#[async_trait]
impl faucet_core::Source for GcsSource {
    async fn fetch_with_context(
        &self,
        context: &std::collections::HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
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
            bucket = %self.config.bucket,
            objects = keys.len(),
            "Listed GCS objects",
        );

        let concurrency = self.config.concurrency.max(1);
        let results: Vec<Vec<Value>> = stream::iter(keys)
            .map(|key| async move {
                #[cfg(feature = "arrow")]
                if matches!(self.config.file_format, GcsFileFormat::Parquet) {
                    let (_schema, batches) = self.read_object_parquet(&key).await?;
                    let mut records = Vec::new();
                    for batch in &batches {
                        records.extend(faucet_core::columnar::record_batch_to_values(batch)?);
                    }
                    tracing::debug!(key = %key, records = records.len(), "Read GCS parquet object");
                    return Ok::<Vec<Value>, FaucetError>(records);
                }
                let text = self.read_object_text(&key).await?;
                let records = self.parse_content(&key, &text)?;
                tracing::debug!(key = %key, records = records.len(), "Read GCS object");
                Ok::<Vec<Value>, FaucetError>(records)
            })
            .buffer_unordered(concurrency)
            .try_collect()
            .await?;

        let all_records: Vec<Value> = results.into_iter().flatten().collect();
        tracing::info!(total_records = all_records.len(), "GCS fetch complete");
        Ok(all_records)
    }

    /// Stream records from listed GCS objects without buffering the full
    /// scan. Mirrors `S3Source::stream_pages` — see that implementation
    /// for the per-format reasoning.
    fn stream_pages<'a>(
        &'a self,
        context: &'a std::collections::HashMap<String, Value>,
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
                bucket = %self.config.bucket,
                objects = keys.len(),
                "Listed GCS objects (stream)",
            );

            let chunk = if batch_size == 0 { usize::MAX } else { batch_size };
            let initial_capacity = if batch_size == 0 { 1024 } else { batch_size };
            let mut buffer: Vec<Value> = Vec::with_capacity(initial_capacity);
            let mut total = 0usize;

            // Overlap the object reads (#619). `buffered` keeps listing order,
            // so an object that fails surfaces at exactly the point it would
            // have serially — concurrency changes throughput, not semantics.
            // The key is moved into each future rather than borrowed: a closure
            // returning a borrow-capturing async block is not higher-ranked
            // enough for `buffered`.
            let concurrency = self.config.concurrency.max(1);
            let mut fetched = stream::iter(keys.iter().cloned())
                .map(|key| async move {
                    let payload = self.fetch(&key).await;
                    (key, payload)
                })
                .buffered(concurrency);

            while let Some((key, payload)) = fetched.next().await {
                let key = &key;
                match payload? {
                    Fetched::Lines(reader) => {
                        let mut lines = reader.lines();
                        let mut line_num: usize = 0;
                        while let Some(line) = lines
                            .next_line()
                            .await
                            .map_err(|e| FaucetError::Source(format!(
                                "GCS read body error for key '{key}': {e}"
                            )))?
                        {
                            line_num += 1;
                            let trimmed = line.trim();
                            if trimmed.is_empty() { continue; }
                            let value: Value = serde_json::from_str(trimmed).map_err(|e| {
                                FaucetError::Source(format!(
                                    "GCS JSON parse error in '{key}' at line {line_num}: {e}",
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
                    #[cfg(feature = "arrow")]
                    Fetched::ParquetStream(mut batches) => {
                        // Row groups arrive one at a time, so peak memory is
                        // one batch rather than the whole object (#619).
                        while let Some(batch) = batches.next().await {
                            let batch = batch.map_err(|e| {
                                FaucetError::Source(format!(
                                    "parquet decode error in '{key}': {e}"
                                ))
                            })?;
                            for record in faucet_core::columnar::record_batch_to_values(&batch)? {
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
                    #[cfg(feature = "arrow")]
                    Fetched::Parquet(data) => {
                        // Parquet objects are buffered and decoded to Arrow
                        // `RecordBatch`es, then converted to JSON rows for the
                        // row path. Rows accumulate across objects and chunk at
                        // `batch_size`; `batch_size == 0` emits one page per
                        // object.
                        let (_schema, batches) = Self::decode_parquet(data, key).await?;
                        for batch in &batches {
                            let rows = faucet_core::columnar::record_batch_to_values(batch)?;
                            for record in rows {
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
                    Fetched::JsonArray(text) => {
                        let value: Value = serde_json::from_str(&text).map_err(|e| {
                            FaucetError::Source(format!("GCS JSON parse error in '{key}': {e}"))
                        })?;
                        let array = match value {
                            Value::Array(arr) => arr,
                            other => Err(FaucetError::Source(format!(
                                "GCS expected JSON array in '{key}', got {}",
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
                    #[cfg(any(
                        feature = "file-format-csv",
                        feature = "file-format-xml",
                        feature = "file-format-excel"
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
                "GCS source stream complete",
            );
        })
    }

    /// The GCS source advertises the columnar fast path **only** when
    /// configured for the [`Parquet`](GcsFileFormat::Parquet) format — the
    /// text formats have no native Arrow representation and stay on the row
    /// path (RFC 0002 / #375).
    #[cfg(feature = "arrow")]
    fn supports_columnar(&self) -> bool {
        matches!(self.config.file_format, GcsFileFormat::Parquet)
    }

    /// Stream Parquet objects natively as Arrow `RecordBatch`es — one
    /// [`ColumnarPage`](faucet_core::columnar::ColumnarPage) per batch — so a
    /// `gcs(parquet) → parquet`/`delta`/`sql` chain never materializes
    /// `serde_json::Value`. The first object's schema is the reference; a later
    /// divergent object aborts after earlier objects' pages have been written
    /// (the same non-atomic multi-object semantics the row path has). Empty
    /// batches are skipped; every page carries `bookmark: None`.
    #[cfg(feature = "arrow")]
    fn stream_batches<'a>(
        &'a self,
        context: &'a std::collections::HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<
        Box<
            dyn Stream<Item = Result<faucet_core::columnar::ColumnarPage, FaucetError>> + Send + 'a,
        >,
    > {
        Box::pin(async_stream::try_stream! {
            if !matches!(self.config.file_format, GcsFileFormat::Parquet) {
                Err(FaucetError::Source(
                    "GCS source: stream_batches invoked for a non-parquet file_format".into(),
                ))?;
            }

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
                bucket = %self.config.bucket,
                objects = keys.len(),
                "Listed GCS objects (columnar stream)",
            );

            let mut reference: Option<arrow::datatypes::SchemaRef> = None;
            let mut total_records = 0usize;
            let mut total_pages = 0usize;
            // Prefetch readers, not bodies: opening one costs a metadata lookup
            // plus a footer read, so `concurrency` overlaps those round trips
            // while the row groups of the object in hand are still decoding
            // (#619).
            let concurrency = self.config.concurrency.max(1);
            let mut fetched = stream::iter(keys.iter().cloned())
                .map(|key| async move {
                    let opened = self.fetch(&key).await;
                    (key, opened)
                })
                .buffered(concurrency);

            while let Some((key, opened)) = fetched.next().await {
                let key = &key;
                let mut pending: Vec<arrow::array::RecordBatch> = Vec::new();
                let mut streaming = None;
                match opened? {
                    Fetched::ParquetStream(s) => streaming = Some(s),
                    Fetched::Parquet(data) => {
                        let (schema, batches) = Self::decode_parquet(data, key).await?;
                        check_schema(&mut reference, &schema, key)?;
                        pending = batches;
                    }
                    _ => Err(FaucetError::Source(format!(
                        "GCS source: stream_batches reached a non-parquet payload for '{key}'"
                    )))?,
                }

                if let Some(mut batches) = streaming {
                    check_schema(&mut reference, &batches.schema().clone(), key)?;
                    while let Some(batch) = batches.next().await {
                        let batch = batch.map_err(|e| {
                            FaucetError::Source(format!("parquet decode error in '{key}': {e}"))
                        })?;
                        if batch.num_rows() == 0 {
                            continue;
                        }
                        total_records += batch.num_rows();
                        total_pages += 1;
                        yield faucet_core::columnar::ColumnarPage { batch, bookmark: None };
                    }
                }

                for batch in pending {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    total_records += batch.num_rows();
                    total_pages += 1;
                    yield faucet_core::columnar::ColumnarPage { batch, bookmark: None };
                }
            }

            tracing::info!(
                pages = total_pages,
                total_records,
                objects = keys.len(),
                "GCS source columnar stream complete",
            );
        })
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(GcsSourceConfig))
            .expect("schema serialization")
    }

    fn connector_name(&self) -> &'static str {
        "gcs"
    }

    fn dataset_uri(&self) -> String {
        match &self.config.prefix {
            Some(p) => format!("gs://{}/{}", self.config.bucket, p),
            None => format!("gs://{}", self.config.bucket),
        }
    }

    /// The GCS source is always shardable: any object set can be split by
    /// hash-of-key. Sharding only takes effect when the cluster coordinator
    /// calls `apply_shard`; a plain `faucet run` reads every object.
    fn is_shardable(&self) -> bool {
        true
    }

    /// Enumerate `target` hash-modulo shards. Each shard `i` will read the
    /// objects whose key hashes to `i (mod target)`. No I/O: the partition is
    /// defined by the hash function, so enumeration is cheap and stable as new
    /// objects appear. `target <= 1` yields a single whole-dataset shard.
    async fn enumerate_shards(&self, target: usize) -> Result<Vec<ShardSpec>, FaucetError> {
        Ok(plan_hash_shards(target))
    }

    /// Narrow this source to one hash-modulo shard. The whole-dataset shard
    /// clears any filter (reads every object).
    async fn apply_shard(&self, shard: &ShardSpec) -> Result<(), FaucetError> {
        *self.applied_shard.lock().expect("shard mutex poisoned") = parse_hash_shard(shard, "gcs")?;
        Ok(())
    }

    fn supports_discover(&self) -> bool {
        true
    }

    /// Enumerate the "directories" directly under the configured prefix via
    /// **one** delimiter (`/`) listing page — each common prefix becomes a
    /// `prefix` dataset. When the listing returns no common prefixes but does
    /// return objects directly under the prefix, each object (first page
    /// only, ≤ `DISCOVER_MAX_OBJECTS`) becomes an `object` dataset instead,
    /// selected via the exact-match `object_keys` config field. No recursion
    /// and no data scan — object counts would require paging the whole
    /// listing, so `estimated_rows` is never set.
    async fn discover(&self) -> Result<Vec<faucet_core::DatasetDescriptor>, FaucetError> {
        let mut req = self
            .control
            .list_objects()
            .set_parent(self.bucket_path())
            .set_delimiter("/")
            .set_page_size(DISCOVER_MAX_OBJECTS as i32);
        if let Some(p) = self.config.prefix.as_deref() {
            req = req.set_prefix(p.to_string());
        }
        let response = req
            .send()
            .await
            .map_err(|e| FaucetError::Source(format!("gcs: catalog discovery failed: {e}")))?;

        let objects: Vec<String> = response
            .objects
            .into_iter()
            .map(|o| o.name)
            .filter(|n| !n.is_empty())
            .collect();
        Ok(descriptors_from_listing(response.prefixes, objects))
    }
}

/// Cap on object-fallback descriptors — one delimiter-listing page, matching
/// the `page_size` requested from GCS.
const DISCOVER_MAX_OBJECTS: usize = 1000;

/// Build one [`DatasetDescriptor`](faucet_core::DatasetDescriptor) per common
/// prefix from a single delimiter listing; when the listing yielded no common
/// prefixes, fall back to one descriptor per object (capped at
/// `DISCOVER_MAX_OBJECTS`). Prefix datasets patch the source's `prefix`
/// config field; object datasets patch `object_keys` (which selects exactly
/// that object and makes `prefix` inert). Pure — unit-testable without a GCS
/// client.
fn descriptors_from_listing(
    prefixes: Vec<String>,
    objects: Vec<String>,
) -> Vec<faucet_core::DatasetDescriptor> {
    let prefixes: Vec<String> = prefixes.into_iter().filter(|p| !p.is_empty()).collect();
    if !prefixes.is_empty() {
        return prefixes
            .into_iter()
            .map(|p| {
                let patch = serde_json::json!({ "prefix": p });
                faucet_core::DatasetDescriptor::new(p, "prefix", patch)
            })
            .collect();
    }
    objects
        .into_iter()
        .take(DISCOVER_MAX_OBJECTS)
        .map(|k| {
            let patch = serde_json::json!({ "object_keys": [k] });
            faucet_core::DatasetDescriptor::new(k, "object", patch)
        })
        .collect()
}

/// Truncate an explicit object-key list to the `max_objects` cap.
///
/// `None` leaves the list untouched; `Some(n)` keeps at most the first `n`
/// keys. This mirrors the cap the listing path applies while paginating, so
/// `max_objects` is honoured whether keys come from `object_keys` or a live
/// `list_objects` scan.
fn cap_keys(mut keys: Vec<String>, max: Option<usize>) -> Vec<String> {
    if let Some(n) = max {
        keys.truncate(n);
    }
    keys
}

/// Retain only the keys owned by `shard` (hash-of-key modulo `shards`). Free
/// function (vs. a `GcsSource` method) so the partitioning logic is
/// unit-testable without a GCS client.
fn filter_shard_keys(keys: Vec<String>, shard: Option<HashShard>) -> Vec<String> {
    match shard {
        Some(member) => keys.into_iter().filter(|k| member.contains(k)).collect(),
        None => keys,
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[cfg(feature = "compression")]
    #[test]
    fn compression_default_is_auto() {
        let cfg = GcsSourceConfig::new("bucket");
        assert_eq!(cfg.compression, faucet_core::CompressionConfig::Auto);
    }

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
        let r =
            parse_file_content(&GcsFileFormat::JsonLines, "t", "{\"id\":1}\n{\"id\":2}\n").unwrap();
        assert_eq!(r.len(), 2);
        assert_eq!(r[0]["id"], 1);
    }

    #[test]
    fn parse_json_lines_skips_blanks() {
        let r = parse_file_content(
            &GcsFileFormat::JsonLines,
            "t",
            "{\"id\":1}\n\n{\"id\":2}\n\n",
        )
        .unwrap();
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn parse_json_lines_reports_line_number() {
        let err = parse_file_content(&GcsFileFormat::JsonLines, "t", "{\"id\":1}\nbad-line\n")
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("line 2"), "unexpected: {msg}");
    }

    #[test]
    fn parse_json_array() {
        let r = parse_file_content(
            &GcsFileFormat::JsonArray,
            "t.json",
            "[{\"id\":1},{\"id\":2}]",
        )
        .unwrap();
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn parse_json_array_rejects_non_array() {
        let err =
            parse_file_content(&GcsFileFormat::JsonArray, "t.json", "{\"id\":1}").unwrap_err();
        assert!(err.to_string().contains("expected JSON array"));
    }

    #[test]
    fn parse_raw_text_yields_single_record() {
        let r = parse_file_content(&GcsFileFormat::RawText, "p/f.txt", "hello").unwrap();
        assert_eq!(r, vec![json!({"key": "p/f.txt", "content": "hello"})]);
    }

    #[test]
    fn cap_keys_truncates_explicit_list_to_max_objects() {
        let keys = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let capped = cap_keys(keys, Some(2));
        assert_eq!(capped, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn cap_keys_passes_through_when_no_max() {
        let keys = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let capped = cap_keys(keys.clone(), None);
        assert_eq!(capped, keys);
    }

    #[test]
    fn cap_keys_noop_when_max_exceeds_len() {
        let keys = vec!["a".to_string(), "b".to_string()];
        let capped = cap_keys(keys.clone(), Some(10));
        assert_eq!(capped, keys);
    }

    // ── Hash-modulo sharding (Mode B, #262) ──────────────────────────────────

    // The union of every shard's filtered key set equals the full set, with no
    // key in two shards — the core no-dup / no-loss guarantee.
    #[test]
    fn shard_filter_partitions_keys_disjointly_and_completely() {
        let keys: Vec<String> = (0..200).map(|i| format!("data/obj-{i}.jsonl")).collect();
        let members: Vec<HashShard> = plan_hash_shards(4)
            .iter()
            .map(|s| HashShard::from_spec(s).expect("descriptor parses"))
            .collect();
        let mut union: Vec<String> = Vec::new();
        for member in members {
            union.extend(filter_shard_keys(keys.clone(), Some(member)));
        }
        union.sort();
        let mut expected = keys.clone();
        expected.sort();
        assert_eq!(
            union, expected,
            "shards must union to the full key set, disjointly"
        );
    }

    #[test]
    fn no_applied_shard_reads_everything() {
        let keys: Vec<String> = (0..20).map(|i| format!("k{i}")).collect();
        assert_eq!(filter_shard_keys(keys.clone(), None), keys);
    }

    // ── discover: pure listing → descriptor mapping ─────────────────────────

    #[test]
    fn descriptors_from_listing_maps_common_prefixes() {
        let out = descriptors_from_listing(
            vec!["raw/orders/".to_string(), "raw/users/".to_string()],
            vec![],
        );
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].name, "raw/orders/");
        assert_eq!(out[0].kind, "prefix");
        assert_eq!(out[0].config_patch, json!({ "prefix": "raw/orders/" }));
        assert!(out[0].schema.is_none());
        assert!(out[0].estimated_rows.is_none());
        assert_eq!(out[1].name, "raw/users/");
        assert_eq!(out[1].config_patch, json!({ "prefix": "raw/users/" }));
    }

    // Prefixes win: objects sitting alongside common prefixes are not
    // enumerated as datasets (they'd be a mixed listing at the same level).
    #[test]
    fn descriptors_from_listing_prefers_prefixes_over_objects() {
        let out = descriptors_from_listing(
            vec!["raw/orders/".to_string()],
            vec!["raw/readme.txt".to_string()],
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].kind, "prefix");
        assert_eq!(out[0].name, "raw/orders/");
    }

    #[test]
    fn descriptors_from_listing_falls_back_to_objects_via_object_keys() {
        let out = descriptors_from_listing(
            vec![],
            vec!["raw/a.jsonl".to_string(), "raw/b.jsonl".to_string()],
        );
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].name, "raw/a.jsonl");
        assert_eq!(out[0].kind, "object");
        assert_eq!(
            out[0].config_patch,
            json!({ "object_keys": ["raw/a.jsonl"] })
        );
        assert!(out[0].schema.is_none());
        assert!(out[0].estimated_rows.is_none());
    }

    #[test]
    fn descriptors_from_listing_empty_listing_yields_no_datasets() {
        assert!(descriptors_from_listing(vec![], vec![]).is_empty());
    }

    #[test]
    fn descriptors_from_listing_skips_empty_prefixes() {
        let out = descriptors_from_listing(vec![String::new(), "raw/orders/".to_string()], vec![]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].name, "raw/orders/");
    }

    #[test]
    fn descriptors_from_listing_caps_object_fallback() {
        let objects: Vec<String> = (0..DISCOVER_MAX_OBJECTS + 500)
            .map(|i| format!("obj-{i}.jsonl"))
            .collect();
        let out = descriptors_from_listing(vec![], objects);
        assert_eq!(out.len(), DISCOVER_MAX_OBJECTS);
    }

    // GcsSource requires an async constructor that tries to connect to GCS,
    // so we verify the dataset_uri() logic directly via the config fields.
    #[test]
    fn dataset_uri_no_prefix_logic() {
        let config = GcsSourceConfig::new("my-bucket");
        let uri = match &config.prefix {
            Some(p) => format!("gs://{}/{}", config.bucket, p),
            None => format!("gs://{}", config.bucket),
        };
        assert_eq!(uri, "gs://my-bucket");
    }

    #[test]
    fn dataset_uri_with_prefix_logic() {
        let config = GcsSourceConfig::new("my-bucket").prefix("data/2026/");
        let uri = match &config.prefix {
            Some(p) => format!("gs://{}/{}", config.bucket, p),
            None => format!("gs://{}", config.bucket),
        };
        assert_eq!(uri, "gs://my-bucket/data/2026/");
    }

    // ── Parquet columnar path (feature `arrow`) ──────────────────────────────

    #[cfg(feature = "arrow")]
    fn sample_parquet_bytes() -> bytes::Bytes {
        use arrow::array::{Int32Array, RecordBatch, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![10, 20])),
                Arc::new(StringArray::from(vec![Some("x"), None])),
            ],
        )
        .unwrap();
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = parquet::arrow::ArrowWriter::try_new(&mut buf, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        bytes::Bytes::from(buf)
    }

    #[cfg(feature = "arrow")]
    #[test]
    fn decode_parquet_bytes_yields_schema_and_batches() {
        let (schema, batches) = decode_parquet_bytes(sample_parquet_bytes(), "t.parquet").unwrap();
        assert_eq!(schema.fields().len(), 2);
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);
    }

    #[cfg(feature = "arrow")]
    #[test]
    fn parquet_batches_convert_to_rows_with_explicit_nulls() {
        let (_schema, batches) = decode_parquet_bytes(sample_parquet_bytes(), "t.parquet").unwrap();
        let mut rows = Vec::new();
        for b in &batches {
            rows.extend(faucet_core::columnar::record_batch_to_values(b).unwrap());
        }
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["id"], 10);
        assert_eq!(rows[0]["name"], "x");
        assert!(rows[1].as_object().unwrap().contains_key("name"));
        assert!(rows[1]["name"].is_null());
    }

    #[cfg(feature = "arrow")]
    #[test]
    fn corrupt_parquet_bytes_error() {
        let err =
            decode_parquet_bytes(bytes::Bytes::from_static(b"nope"), "bad.parquet").unwrap_err();
        assert!(matches!(err, FaucetError::Source(_)));
    }
}
