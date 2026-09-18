//! S3 source stream executor.

use crate::config::{S3FileFormat, S3SourceConfig};
use async_trait::async_trait;
use aws_sdk_s3::Client;
use faucet_core::shard::{HashShard, ShardSpec, parse_hash_shard, plan_hash_shards};
use faucet_core::{FaucetError, Stream, StreamPage};
use futures::stream::{self, StreamExt, TryStreamExt};
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
                crate::parquet_range::S3RangeReader,
            >,
        >,
    ),
    /// A Parquet object buffered whole, for the configurations a ranged read
    /// cannot serve: a compressed object is not randomly addressable, and a
    /// whole-object checksum can only be verified by reading the whole object.
    #[cfg(feature = "arrow")]
    Parquet(bytes::Bytes),
}

/// An S3 source that lists and reads objects from a bucket.
pub struct S3Source {
    config: S3SourceConfig,
    client: Client,
    /// Shard applied by the cluster coordinator (Mode B). `None` (or a
    /// degenerate single-shard set) reads every listed object. Stored behind a
    /// `Mutex` so `apply_shard(&self, …)` can record it before streaming.
    applied_shard: Mutex<Option<HashShard>>,
}

impl S3Source {
    /// Create a new S3 source from the given configuration.
    ///
    /// Builds the S3 client eagerly so it is reused across calls.
    pub async fn new(config: S3SourceConfig) -> Result<Self, FaucetError> {
        let client = Self::build_client(&config).await?;
        Ok(Self {
            config,
            client,
            applied_shard: Mutex::new(None),
        })
    }

    /// Retain only the keys belonging to the applied shard (hash-of-key modulo
    /// `shards`). A no-op when no shard is applied or `shards <= 1`.
    fn shard_filter(&self, keys: Vec<String>) -> Vec<String> {
        match *self.applied_shard.lock().expect("shard mutex poisoned") {
            Some(member) => keys.into_iter().filter(|k| member.contains(k)).collect(),
            None => keys,
        }
    }

    /// Build an S3 client from the configuration.
    async fn build_client(config: &S3SourceConfig) -> Result<Client, FaucetError> {
        let mut config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest());

        if let Some(ref region) = config.region {
            config_loader = config_loader.region(aws_config::Region::new(region.clone()));
        }

        if let Some(ref endpoint) = config.endpoint_url {
            config_loader = config_loader.endpoint_url(endpoint);
        }

        let sdk_config = config_loader.load().await;
        let client = Client::new(&sdk_config);
        Ok(client)
    }

    /// List object keys matching the configured bucket and prefix.
    ///
    /// When `prefix_override` is `Some`, it is used instead of `self.config.prefix`
    /// (used for parent-context substitution).
    async fn list_object_keys(
        &self,
        prefix_override: Option<&str>,
    ) -> Result<Vec<String>, FaucetError> {
        let mut keys = Vec::new();
        let mut continuation_token: Option<String> = None;

        let effective_prefix = prefix_override.or(self.config.prefix.as_deref());

        loop {
            let mut req = self.client.list_objects_v2().bucket(&self.config.bucket);

            if let Some(prefix) = effective_prefix {
                req = req.prefix(prefix);
            }

            if let Some(ref token) = continuation_token {
                req = req.continuation_token(token);
            }

            let response = req.send().await.map_err(|e| {
                FaucetError::Source(format!(
                    "S3 list objects error for bucket '{}': {e}",
                    self.config.bucket
                ))
            })?;

            for object in response.contents() {
                let key: &str = object.key().unwrap_or_default();
                if key.is_empty() {
                    continue;
                }
                keys.push(key.to_string());

                if let Some(max) = self.config.max_objects
                    && keys.len() >= max
                {
                    return Ok(self.shard_filter(keys));
                }
            }

            if response.is_truncated() == Some(true) {
                continuation_token = response.next_continuation_token().map(String::from);
            } else {
                break;
            }
        }

        Ok(self.shard_filter(keys))
    }

    /// Read and parse a single S3 object into records.
    async fn read_object(&self, key: &str) -> Result<Vec<Value>, FaucetError> {
        #[cfg(feature = "arrow")]
        if matches!(self.config.file_format, S3FileFormat::Parquet) {
            let (_schema, batches) = self.read_object_parquet(key).await?;
            let mut rows = Vec::new();
            for batch in &batches {
                rows.extend(faucet_core::columnar::record_batch_to_values(batch)?);
            }
            return Ok(rows);
        }
        let text = self.read_object_text(key).await?;
        self.parse_content(key, &text)
    }

    /// Download a single S3 object's full body into an in-memory
    /// [`bytes::Bytes`], reusing [`open_object_reader`](Self::open_object_reader)
    /// so length/checksum verification (and any configured decompression)
    /// still apply. Used only by the Parquet path, which needs the raw bytes
    /// (Parquet is binary, so it cannot go through
    /// [`read_object_text`](Self::read_object_text)).
    #[cfg(feature = "arrow")]
    async fn read_object_bytes(&self, key: &str) -> Result<bytes::Bytes, FaucetError> {
        use tokio::io::AsyncReadExt as _;
        let mut reader = self.open_object_reader(key).await?;
        let mut buf = Vec::new();
        reader
            .read_to_end(&mut buf)
            .await
            .map_err(|e| FaucetError::Source(format!("S3 read error for key '{key}': {e}")))?;
        Ok(bytes::Bytes::from(buf))
    }

    /// Decode a single Parquet object into its Arrow schema and the list of
    /// `RecordBatch`es it contains. The whole object is buffered (matching the
    /// connector's `JsonArray` model) and decoded on a blocking thread so the
    /// CPU-bound Parquet decode does not stall the async runtime.
    #[cfg(feature = "arrow")]
    async fn read_object_parquet(
        &self,
        key: &str,
    ) -> Result<(arrow::datatypes::SchemaRef, Vec<arrow::array::RecordBatch>), FaucetError> {
        Self::decode_parquet(self.read_object_bytes(key).await?, key).await
    }

    /// Fetch one object in the shape its configured `file_format` needs.
    async fn fetch(&self, key: &str) -> Result<Fetched, FaucetError> {
        Ok(match self.config.file_format {
            S3FileFormat::JsonLines => Fetched::Lines(self.open_object_reader(key).await?),
            S3FileFormat::RawText => Fetched::RawText(self.read_object_text(key).await?),
            S3FileFormat::JsonArray => Fetched::JsonArray(self.read_object_text(key).await?),
            #[cfg(feature = "arrow")]
            S3FileFormat::Parquet => match self.parquet_row_group_stream(key).await? {
                Some(stream) => Fetched::ParquetStream(Box::new(stream)),
                None => Fetched::Parquet(self.read_object_bytes(key).await?),
            },
        })
    }

    /// Whether this object can be read row group at a time, and a reader for it
    /// if so (#619).
    ///
    /// Two configurations rule it out, and both are guarantees worth more than
    /// the memory saving:
    ///
    /// - `verify_checksum` — S3's checksum covers the whole object (a ranged
    ///   response carries at most a checksum for that range), so honouring it
    ///   means reading the object as one stream;
    /// - `compression` — a gzip / zstd member is not randomly addressable, so
    ///   the Parquet footer cannot be located without inflating everything.
    ///
    /// In either case the whole-object path still runs, unchanged. `verify_length`
    /// needs no exception: it guards a truncated *transfer*, and the ranged
    /// reader enforces exactly that per fetch.
    #[cfg(feature = "arrow")]
    async fn parquet_row_group_stream(
        &self,
        key: &str,
    ) -> Result<
        Option<
            parquet::arrow::async_reader::ParquetRecordBatchStream<
                crate::parquet_range::S3RangeReader,
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

        let reader =
            crate::parquet_range::S3RangeReader::open(&self.client, &self.config.bucket, key)
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

    /// Read the full body of a single S3 object into a UTF-8 `String`.
    ///
    /// Streams the (optionally decompressed) body straight into one `String`
    /// via [`open_object_reader`](Self::open_object_reader) rather than
    /// buffering the raw bytes AND the decompressed bytes AND the `String`
    /// at once (#78/#25). The whole object is still one unit for
    /// `JsonArray` / `RawText`, but peak memory is now ~1× the decoded size.
    async fn read_object_text(&self, key: &str) -> Result<String, FaucetError> {
        use tokio::io::AsyncReadExt as _;
        let mut reader = self.open_object_reader(key).await?;
        let mut text = String::new();
        reader.read_to_string(&mut text).await.map_err(|e| {
            FaucetError::Source(format!(
                "S3 read/decode error for key '{key}' (not valid UTF-8?): {e}"
            ))
        })?;
        Ok(text)
    }

    /// Open an S3 object as an [`AsyncBufRead`](tokio::io::AsyncBufRead) over
    /// its body. Used by [`Source::stream_pages`](faucet_core::Source::stream_pages)
    /// to decode `JsonLines` objects line-by-line without buffering the
    /// whole file.
    async fn open_object_reader(
        &self,
        key: &str,
    ) -> Result<std::pin::Pin<Box<dyn tokio::io::AsyncBufRead + Send + Unpin>>, FaucetError> {
        let mut request = self
            .client
            .get_object()
            .bucket(&self.config.bucket)
            .key(key);
        // Ask S3 to return its stored checksum so we can verify the body (#161).
        if self.config.verify_checksum {
            request = request.checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled);
        }
        let response = request.send().await.map_err(|e| {
            FaucetError::Source(format!("S3 get object error for key '{key}': {e}"))
        })?;

        // Read all metadata BEFORE consuming `body` (which partially moves
        // `response`), so a cleanly-truncated/corrupted transfer is rejected
        // rather than silently parsed as a complete object (#161).
        let mut checks: Vec<Box<dyn faucet_core::IntegrityCheck>> = Vec::new();
        match crate::verify::length_check(response.content_length(), self.config.verify_length) {
            Some(check) => checks.push(check),
            None if self.config.verify_length => tracing::debug!(
                key = %key,
                "S3 object reports no Content-Length; length verification skipped"
            ),
            None => {}
        }
        if self.config.verify_checksum {
            let advertised = crate::verify::S3Checksums {
                crc32: response.checksum_crc32().map(str::to_string),
                crc32c: response.checksum_crc32_c().map(str::to_string),
                crc64nvme: response.checksum_crc64_nvme().map(str::to_string),
                sha256: response.checksum_sha256().map(str::to_string),
                etag: response.e_tag().map(str::to_string),
            };
            match crate::verify::checksum_check(&advertised) {
                Some(check) => checks.push(check),
                None => tracing::warn!(
                    key = %key,
                    "verify_checksum is enabled but S3 advertised no verifiable checksum for \
                     this object; relying on the length check only"
                ),
            }
        }

        // `ByteStream::into_async_read` returns `impl AsyncRead`. Wrap the RAW
        // body in the verifier first so length/checksum cover the stored bytes
        // (below any decompression), then `BufReader` so `.lines()` is usable
        // and ownership is `Unpin`.
        let verified = faucet_core::VerifyingReader::new(response.body.into_async_read(), checks);
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
        match self.config.file_format {
            S3FileFormat::JsonLines => {
                let mut records = Vec::new();
                for (line_num, line) in text.lines().enumerate() {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    let value: Value = serde_json::from_str(trimmed).map_err(|e| {
                        FaucetError::Source(format!(
                            "S3 JSON parse error in '{key}' at line {}: {e}",
                            line_num + 1
                        ))
                    })?;
                    records.push(value);
                }
                Ok(records)
            }
            S3FileFormat::JsonArray => {
                let value: Value = serde_json::from_str(text).map_err(|e| {
                    FaucetError::Source(format!("S3 JSON parse error in '{key}': {e}"))
                })?;
                match value {
                    Value::Array(arr) => Ok(arr),
                    _ => Err(FaucetError::Source(format!(
                        "S3 expected JSON array in '{key}', got {}",
                        value_type_name(&value)
                    ))),
                }
            }
            S3FileFormat::RawText => {
                let record = serde_json::json!({
                    "key": key,
                    "content": text,
                });
                Ok(vec![record])
            }
            // Parquet is binary and is decoded via `read_object_parquet`, which
            // never routes through this text parser — reaching here is an
            // internal invariant violation.
            #[cfg(feature = "arrow")]
            S3FileFormat::Parquet => Err(FaucetError::Source(format!(
                "S3 parquet object '{key}' cannot be parsed as text (internal error: \
                 parquet must use the binary decode path)"
            ))),
        }
    }
}

#[async_trait]
impl faucet_core::Source for S3Source {
    async fn fetch_with_context(
        &self,
        context: &std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        // Substitute context into prefix when parent context is provided.
        let substituted_prefix: Option<String> = if !context.is_empty() {
            self.config
                .prefix
                .as_ref()
                .map(|p| faucet_core::util::substitute_context(p, context))
        } else {
            None
        };

        let keys = self.list_object_keys(substituted_prefix.as_deref()).await?;

        tracing::info!(
            bucket = %self.config.bucket,
            objects = keys.len(),
            "Listed S3 objects"
        );

        let concurrency = self.config.concurrency.max(1);

        let results: Vec<Vec<Value>> = stream::iter(keys)
            .map(|key| async move {
                let records = self.read_object(&key).await?;
                tracing::debug!(key = %key, records = records.len(), "Read S3 object");
                Ok::<Vec<Value>, FaucetError>(records)
            })
            .buffer_unordered(concurrency)
            .try_collect()
            .await?;

        let all_records: Vec<Value> = results.into_iter().flatten().collect();

        tracing::info!(total_records = all_records.len(), "S3 fetch complete");
        Ok(all_records)
    }

    /// Stream records from listed S3 objects without buffering the full
    /// scan. Each emitted [`StreamPage`] holds up to
    /// [`S3SourceConfig::batch_size`] records.
    ///
    /// The trait-level `batch_size` argument is ignored in favour of the
    /// config field — the config is the user-facing knob the README
    /// documents, and routing the pipeline-supplied hint through it would
    /// silently override an explicit config value.
    ///
    /// Behaviour by format:
    ///
    /// - `JsonLines` / `RawText`: the object body is decoded line-by-line
    ///   via [`tokio::io::AsyncBufReadExt::lines`] so client-side memory is
    ///   bounded at `O(batch_size)` per object. Multi-object scans are
    ///   flattened — a single page may carry lines drawn from any object.
    /// - `JsonArray`: each object is buffered fully (the JSON value can
    ///   only be parsed once the array is complete) and then its records
    ///   are chunked into pages of `batch_size`. See the README "Streaming
    ///   and batching" section for the caveat.
    ///
    /// `batch_size = 0` is the "no batching" sentinel: one [`StreamPage`]
    /// is emitted per S3 object (no within-object chunking and no
    /// cross-object accumulation). The S3 source has no
    /// incremental-replication mode today, so every emitted page carries
    /// `bookmark: None`.
    fn stream_pages<'a>(
        &'a self,
        context: &'a std::collections::HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let batch_size = self.config.batch_size;

        Box::pin(async_stream::try_stream! {
            // Substitute context into prefix when parent context is provided.
            let substituted_prefix: Option<String> = if !context.is_empty() {
                self.config
                    .prefix
                    .as_ref()
                    .map(|p| faucet_core::util::substitute_context(p, context))
            } else {
                None
            };

            let keys = self.list_object_keys(substituted_prefix.as_deref()).await?;
            tracing::info!(
                bucket = %self.config.bucket,
                objects = keys.len(),
                "Listed S3 objects (stream)",
            );

            let chunk = if batch_size == 0 { usize::MAX } else { batch_size };
            let initial_capacity = if batch_size == 0 { 1024 } else { batch_size };
            let mut buffer: Vec<Value> = Vec::with_capacity(initial_capacity);
            let mut total = 0usize;

            // Overlap the object reads (#619). `buffered` keeps listing order,
            // so an object that fails surfaces at exactly the point it would
            // have serially — concurrency changes throughput, not semantics.
            let concurrency = self.config.concurrency.max(1);
            // The key is moved into each future rather than borrowed: a
            // closure returning a borrow-capturing async block is not
            // higher-ranked enough for `buffered`.
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
                                "S3 read body error for key '{key}': {e}"
                            )))?
                        {
                            line_num += 1;
                            let trimmed = line.trim();
                            if trimmed.is_empty() {
                                continue;
                            }
                            let value: Value =
                                serde_json::from_str(trimmed).map_err(|e| {
                                    FaucetError::Source(format!(
                                        "S3 JSON parse error in '{key}' at line {line_num}: {e}",
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
                        // RawText emits a single record per object; the
                        // `key` + `content` shape is unchanged so we
                        // continue to buffer the body fully. This still
                        // streams *across* objects.
                        let record = serde_json::json!({
                            "key": key,
                            "content": text,
                        });
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
                        // JSON-array files cannot be parsed incrementally
                        // (the closing `]` is required to validate the
                        // structure), so each object is buffered fully and
                        // then chunked. The caveat is documented in the
                        // crate README.
                        let value: Value = serde_json::from_str(&text).map_err(|e| {
                            FaucetError::Source(format!("S3 JSON parse error in '{key}': {e}"))
                        })?;
                        let array = match value {
                            Value::Array(arr) => arr,
                            other => Err(FaucetError::Source(format!(
                                "S3 expected JSON array in '{key}', got {}",
                                value_type_name(&other)
                            )))?,
                        };
                        if batch_size == 0 {
                            // Flush any cross-object buffer first (none
                            // here because each iteration completes its
                            // own object — but keep symmetric with the
                            // line-shaped branches).
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
                "S3 source stream complete",
            );
        })
    }

    /// The S3 source advertises the columnar fast path **only** when configured
    /// for the [`Parquet`](S3FileFormat::Parquet) format — the text formats
    /// (`JsonLines` / `JsonArray` / `RawText`) have no native Arrow
    /// representation and stay on the row path (RFC 0002 / #375).
    #[cfg(feature = "arrow")]
    fn supports_columnar(&self) -> bool {
        matches!(self.config.file_format, S3FileFormat::Parquet)
    }

    /// Stream Parquet objects natively as Arrow `RecordBatch`es — one
    /// [`ColumnarPage`](faucet_core::columnar::ColumnarPage) per batch — so an
    /// `s3(parquet) → parquet`/`delta`/`sql` chain never materializes
    /// `serde_json::Value`.
    ///
    /// Objects are read in listing order. The first object's Arrow schema is
    /// the reference; a later object whose schema diverges surfaces as
    /// [`FaucetError::Source`]. Because each object is buffered and decoded as
    /// it is reached (not probed up front), a divergent *later* object aborts
    /// after earlier objects' pages have already been written — the same
    /// non-atomic multi-object semantics the row path already has. Empty
    /// batches are skipped; every page carries `bookmark: None` (the S3 source
    /// has no incremental-replication mode).
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
            if !matches!(self.config.file_format, S3FileFormat::Parquet) {
                Err(FaucetError::Source(
                    "S3 source: stream_batches invoked for a non-parquet file_format".into(),
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

            let keys = self.list_object_keys(substituted_prefix.as_deref()).await?;
            tracing::info!(
                bucket = %self.config.bucket,
                objects = keys.len(),
                "Listed S3 objects (columnar stream)",
            );

            let mut reference: Option<arrow::datatypes::SchemaRef> = None;
            let mut total_records = 0usize;
            let mut total_pages = 0usize;
            // Prefetch readers, not bodies: opening one costs a HeadObject plus
            // a footer read, so `concurrency` overlaps those round trips while
            // the row groups of the object in hand are still decoding (#619).
            let concurrency = self.config.concurrency.max(1);
            let mut fetched = stream::iter(keys.iter().cloned())
                .map(|key| async move {
                    let opened = self.fetch(&key).await;
                    (key, opened)
                })
                .buffered(concurrency);

            while let Some((key, opened)) = fetched.next().await {
                let key = &key;
                // One object at a time, but never the whole object at once:
                // each row group is fetched, decoded, and yielded before the
                // next is requested.
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
                        "S3 source: stream_batches reached a non-parquet payload for '{key}'"
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
                "S3 source columnar stream complete",
            );
        })
    }

    fn connector_name(&self) -> &'static str {
        "s3"
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(S3SourceConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        match &self.config.prefix {
            Some(p) => format!("s3://{}/{}", self.config.bucket, p),
            None => format!("s3://{}", self.config.bucket),
        }
    }

    /// The S3 source is always shardable: any object set can be split by
    /// hash-of-key. Sharding only takes effect when the cluster coordinator
    /// calls `apply_shard`; a plain `faucet run` reads
    /// every object.
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
        *self.applied_shard.lock().expect("shard mutex poisoned") = parse_hash_shard(shard, "s3")?;
        Ok(())
    }

    fn supports_discover(&self) -> bool {
        true
    }

    /// Enumerate the "directories" directly under the configured prefix via
    /// **one** `ListObjectsV2` delimiter (`/`) listing — each common prefix
    /// becomes a `prefix` dataset. When the listing returns no common
    /// prefixes but does return objects directly under the prefix, each
    /// object (first page only, capped at `DISCOVER_MAX_OBJECTS` = 1000) becomes an
    /// `object` dataset instead. No recursion and no data scan — object
    /// counts would require paging the whole listing, so `estimated_rows`
    /// is never set.
    async fn discover(&self) -> Result<Vec<faucet_core::DatasetDescriptor>, FaucetError> {
        let mut req = self
            .client
            .list_objects_v2()
            .bucket(&self.config.bucket)
            .delimiter("/")
            .max_keys(DISCOVER_MAX_OBJECTS as i32);
        if let Some(prefix) = self.config.prefix.as_deref() {
            req = req.prefix(prefix);
        }
        let response = req
            .send()
            .await
            .map_err(|e| FaucetError::Source(format!("s3: catalog discovery failed: {e}")))?;

        let prefixes: Vec<String> = response
            .common_prefixes()
            .iter()
            .filter_map(|p| p.prefix())
            .filter(|p| !p.is_empty())
            .map(str::to_string)
            .collect();
        let objects: Vec<String> = response
            .contents()
            .iter()
            .filter_map(|o| o.key())
            .filter(|k| !k.is_empty())
            .map(str::to_string)
            .collect();

        Ok(descriptors_from_listing(prefixes, objects))
    }
}

/// Cap on object-fallback descriptors — one delimiter-listing page, matching
/// the `max_keys` requested from S3.
const DISCOVER_MAX_OBJECTS: usize = 1000;

/// Build one [`DatasetDescriptor`](faucet_core::DatasetDescriptor) per common
/// prefix from a single delimiter listing; when the listing yielded no common
/// prefixes, fall back to one descriptor per object (capped at
/// `DISCOVER_MAX_OBJECTS`). Each patch selects the dataset via the source's
/// `prefix` config field — a full object key used as a prefix selects exactly
/// that object. Pure — unit-testable without an S3 client.
fn descriptors_from_listing(
    prefixes: Vec<String>,
    objects: Vec<String>,
) -> Vec<faucet_core::DatasetDescriptor> {
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
            let patch = serde_json::json!({ "prefix": k });
            faucet_core::DatasetDescriptor::new(k, "object", patch)
        })
        .collect()
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
            "S3 source: parquet schema mismatch — object '{key}' diverges from the first \
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

/// Return a human-readable name for a JSON value type.
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
    use crate::config::S3SourceConfig;
    use faucet_core::Source;
    use serde_json::json;

    /// Helper to build an S3Source synchronously for parse-only tests.
    /// We construct it directly to avoid needing an async runtime for unit tests
    /// that only exercise `parse_content`.
    fn test_source(config: S3SourceConfig) -> S3Source {
        // Build a dummy client — these tests never make network calls.
        let sdk_config = aws_config::SdkConfig::builder()
            .behavior_version(aws_config::BehaviorVersion::latest())
            .build();
        let client = Client::new(&sdk_config);
        S3Source {
            config,
            client,
            applied_shard: Mutex::new(None),
        }
    }

    #[test]
    fn parse_json_lines() {
        let source = test_source(S3SourceConfig::new("test"));
        let text = r#"{"id":1,"name":"Alice"}
{"id":2,"name":"Bob"}
"#;
        let records = source.parse_content("test.jsonl", text).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["id"], 1);
        assert_eq!(records[1]["name"], "Bob");
    }

    #[test]
    fn parse_json_lines_skips_empty() {
        let source = test_source(S3SourceConfig::new("test"));
        let text = r#"{"id":1}

{"id":2}

"#;
        let records = source.parse_content("test.jsonl", text).unwrap();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn parse_json_lines_invalid() {
        let source = test_source(S3SourceConfig::new("test"));
        let text = "not json\n";
        let result = source.parse_content("test.jsonl", text);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("JSON parse error"));
        assert!(err.contains("line 1"));
    }

    #[test]
    fn parse_json_array() {
        let source = test_source(S3SourceConfig::new("test").file_format(S3FileFormat::JsonArray));
        let text = r#"[{"id":1},{"id":2}]"#;
        let records = source.parse_content("test.json", text).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["id"], 1);
    }

    #[test]
    fn parse_json_array_not_array() {
        let source = test_source(S3SourceConfig::new("test").file_format(S3FileFormat::JsonArray));
        let text = r#"{"id":1}"#;
        let result = source.parse_content("test.json", text);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("expected JSON array"));
    }

    #[test]
    fn parse_raw_text() {
        let source = test_source(S3SourceConfig::new("test").file_format(S3FileFormat::RawText));
        let text = "hello world\nline two";
        let records = source.parse_content("data/file.txt", text).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0],
            json!({"key": "data/file.txt", "content": "hello world\nline two"})
        );
    }

    #[cfg(feature = "compression")]
    #[test]
    fn compression_default_is_auto() {
        let cfg = S3SourceConfig::new("bucket");
        assert_eq!(cfg.compression, faucet_core::CompressionConfig::Auto);
    }

    // ── Hash-modulo sharding ────────────────────────────────────────────────

    #[test]
    fn shard_hash_is_deterministic() {
        use faucet_core::shard::shard_hash;
        assert_eq!(
            shard_hash("data/part-001.jsonl"),
            shard_hash("data/part-001.jsonl")
        );
        assert_ne!(shard_hash("a"), shard_hash("b"));
    }

    #[tokio::test]
    async fn enumerate_shards_returns_target_disjoint_shards() {
        let source = test_source(S3SourceConfig::new("b"));
        assert!(source.is_shardable());
        let shards = source.enumerate_shards(3).await.unwrap();
        assert_eq!(shards.len(), 3);
        for (i, s) in shards.iter().enumerate() {
            assert_eq!(s.descriptor["shards"], 3);
            assert_eq!(s.descriptor["index"], i);
        }
    }

    #[tokio::test]
    async fn enumerate_shards_target_one_is_whole() {
        let source = test_source(S3SourceConfig::new("b"));
        let shards = source.enumerate_shards(1).await.unwrap();
        assert_eq!(shards.len(), 1);
        assert!(shards[0].is_whole());
    }

    // The union of every shard's filtered key set equals the full set, with no
    // key in two shards — the core no-dup / no-loss guarantee.
    #[tokio::test]
    async fn shard_filter_partitions_keys_disjointly_and_completely() {
        let keys: Vec<String> = (0..200).map(|i| format!("data/obj-{i}.jsonl")).collect();
        let n = 4;
        let mut union: Vec<String> = Vec::new();
        for index in 0..n {
            let source = test_source(S3SourceConfig::new("b"));
            source
                .apply_shard(&ShardSpec::new(
                    index.to_string(),
                    serde_json::json!({ "shards": n, "index": index }),
                ))
                .await
                .unwrap();
            let got = source.shard_filter(keys.clone());
            union.extend(got);
        }
        union.sort();
        let mut expected = keys.clone();
        expected.sort();
        assert_eq!(
            union, expected,
            "shards must union to the full key set, disjointly"
        );
    }

    #[tokio::test]
    async fn apply_whole_shard_reads_everything() {
        let keys: Vec<String> = (0..20).map(|i| format!("k{i}")).collect();
        let source = test_source(S3SourceConfig::new("b"));
        source.apply_shard(&ShardSpec::whole()).await.unwrap();
        assert_eq!(source.shard_filter(keys.clone()).len(), keys.len());
    }

    #[tokio::test]
    async fn apply_shard_rejects_malformed_descriptor() {
        let source = test_source(S3SourceConfig::new("b"));
        let err = source
            .apply_shard(&ShardSpec::new("0", serde_json::json!({ "index": 0 })))
            .await
            .unwrap_err();
        assert!(matches!(err, FaucetError::Source(_)));
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
    fn descriptors_from_listing_falls_back_to_objects() {
        let out = descriptors_from_listing(
            vec![],
            vec!["raw/a.jsonl".to_string(), "raw/b.jsonl".to_string()],
        );
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].name, "raw/a.jsonl");
        assert_eq!(out[0].kind, "object");
        assert_eq!(out[0].config_patch, json!({ "prefix": "raw/a.jsonl" }));
        assert!(out[0].schema.is_none());
        assert!(out[0].estimated_rows.is_none());
    }

    #[test]
    fn descriptors_from_listing_empty_listing_yields_no_datasets() {
        assert!(descriptors_from_listing(vec![], vec![]).is_empty());
    }

    #[test]
    fn descriptors_from_listing_caps_object_fallback() {
        let objects: Vec<String> = (0..DISCOVER_MAX_OBJECTS + 500)
            .map(|i| format!("obj-{i}.jsonl"))
            .collect();
        let out = descriptors_from_listing(vec![], objects);
        assert_eq!(out.len(), DISCOVER_MAX_OBJECTS);
    }

    #[test]
    fn source_advertises_discover() {
        let source = test_source(S3SourceConfig::new("my-bucket"));
        assert!(source.supports_discover());
    }

    #[test]
    fn dataset_uri_no_prefix() {
        let source = test_source(S3SourceConfig::new("my-bucket"));
        assert_eq!(source.dataset_uri(), "s3://my-bucket");
    }

    #[test]
    fn dataset_uri_with_prefix() {
        let source = test_source(S3SourceConfig::new("my-bucket").prefix("data/2026/"));
        assert_eq!(source.dataset_uri(), "s3://my-bucket/data/2026/");
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
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("Alice"), None])),
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
        assert_eq!(schema.field(0).name(), "id");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);
    }

    #[cfg(feature = "arrow")]
    #[test]
    fn parquet_batches_convert_to_rows() {
        let (_schema, batches) = decode_parquet_bytes(sample_parquet_bytes(), "t.parquet").unwrap();
        let mut rows = Vec::new();
        for b in &batches {
            rows.extend(faucet_core::columnar::record_batch_to_values(b).unwrap());
        }
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["id"], 1);
        assert_eq!(rows[0]["name"], "Alice");
        // Explicit-null field survives the round-trip (#321 H6).
        assert!(rows[1].as_object().unwrap().contains_key("name"));
        assert!(rows[1]["name"].is_null());
    }

    #[cfg(feature = "arrow")]
    #[test]
    fn corrupt_parquet_bytes_error() {
        let err = decode_parquet_bytes(bytes::Bytes::from_static(b"not parquet"), "bad.parquet")
            .unwrap_err();
        assert!(matches!(err, FaucetError::Source(_)));
    }

    #[cfg(feature = "arrow")]
    #[test]
    fn supports_columnar_only_for_parquet_format() {
        let parquet_src = test_source(S3SourceConfig::new("b").file_format(S3FileFormat::Parquet));
        assert!(faucet_core::Source::supports_columnar(&parquet_src));

        let json_src = test_source(S3SourceConfig::new("b"));
        assert!(!faucet_core::Source::supports_columnar(&json_src));
    }
}
