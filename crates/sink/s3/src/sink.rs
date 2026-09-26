//! S3 sink executor.

use crate::config::S3SinkConfig;
#[cfg(any(feature = "arrow", test))]
use crate::config::S3SinkFormat;
use async_trait::async_trait;
use aws_sdk_s3::Client;
use faucet_core::FaucetError;
#[cfg(feature = "arrow")]
use futures::stream::{StreamExt, TryStreamExt};
use serde_json::Value;

/// A sink that writes JSON records to S3 as JSON Lines files (or, with the
/// `arrow` feature, self-contained Parquet objects).
pub struct S3Sink {
    config: S3SinkConfig,
    client: Client,
    /// Rows accumulated across `write_batch` calls for the open object (#618).
    ///
    /// Without this the sink wrote one object per upstream page, so a small
    /// `batch_size` produced a swarm of tiny objects — the small-files problem
    /// that dominates read time on S3/Athena/Spark. `Mutex` rather than an
    /// atomic because the buffer and its counters must move together.
    open: tokio::sync::Mutex<OpenObject>,
    /// Round-trip recorder installed by the pipeline (#638 / #704). Op: `put`
    /// (one per `PutObject`).
    roundtrips: faucet_core::observability::RecorderSlot,
}

/// Minimum S3 multipart part size. Every part except the last must be at least
/// 5 MiB, so this is the floor the SDK accepts, not a tuning choice.
const MIN_PART_BYTES: usize = 5 * 1024 * 1024;

/// The in-flight object: the accumulator plus, once a part has been uploaded,
/// the multipart upload it belongs to.
///
/// A multipart upload is started **lazily**, on the first full part — an object
/// that fits in one part stays a plain `put_object`, which is cheaper and
/// leaves nothing abandoned if the run dies before the object is finished.
struct OpenObject {
    acc: faucet_core::ObjectAccumulator,
    /// Records held for a **whole-object** format (#604). CSV has a header,
    /// XML a document element, a workbook a container index and a JSON array
    /// its brackets — none can be appended a record at a time, so their
    /// records are buffered and encoded together at the rollover. Unused (and
    /// always empty) for JSON Lines, which streams through `acc`.
    pending: faucet_core::object_rollover::PageAccumulator,
    /// Key and upload id of the multipart upload, once one has been started.
    upload: Option<(String, String)>,
    parts: Vec<aws_sdk_s3::types::CompletedPart>,
}

impl OpenObject {
    fn new(config: &S3SinkConfig) -> Self {
        Self {
            // `batch_size` keeps sizing objects when no explicit
            // `max_records_per_file` is given, so an existing config still
            // gets objects of the size it asked for — the change is that a
            // page *smaller* than the cap now joins the open object instead
            // of becoming a file of its own (#618).
            acc: faucet_core::ObjectAccumulator::new(
                config.effective_chunk_cap(),
                config.max_bytes_per_file,
            )
            .with_part_size(MIN_PART_BYTES),
            pending: faucet_core::object_rollover::PageAccumulator::new(
                config.effective_chunk_cap(),
                config.max_bytes_per_file,
            ),
            upload: None,
            parts: Vec::new(),
        }
    }
}

impl S3Sink {
    /// Create a new S3 sink from the given configuration.
    ///
    /// Builds the S3 client eagerly so it is reused across calls.
    pub async fn new(config: S3SinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let client = Self::build_client(&config).await?;
        let open = tokio::sync::Mutex::new(OpenObject::new(&config));
        Ok(Self {
            config,
            client,
            open,
            roundtrips: faucet_core::observability::RecorderSlot::new(),
        })
    }

    /// Build an S3 client from the configuration.
    async fn build_client(config: &S3SinkConfig) -> Result<Client, FaucetError> {
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

    /// Generate a unique S3 key for a file.
    fn generate_key(&self) -> String {
        let id = uuid::Uuid::new_v4();
        format!("{}{}{}", self.config.prefix, id, self.config.file_extension)
    }

    /// Apply the configured codec to a body (or a multipart part).
    ///
    /// Applied per part on the multipart path: gzip and zstd both concatenate,
    /// so a multi-member object decodes transparently — the same property the
    /// file sinks already rely on when they reopen a compressed file to append.
    /// Compressing the whole object instead would mean buffering it, which is
    /// the memory bound multipart exists to remove.
    fn encode_body(&self, body: Vec<u8>) -> Result<Vec<u8>, FaucetError> {
        #[cfg(feature = "compression")]
        {
            let codec = self.config.compression.resolve(&self.config.file_extension);
            faucet_core::compression::warn_mismatch(&self.config.file_extension, codec);
            faucet_core::compression::compress_buf(&body, codec)
        }
        #[cfg(not(feature = "compression"))]
        {
            Ok(body)
        }
    }

    /// Upload one multipart part, starting the upload if this is the first.
    ///
    /// Started lazily so an object that fits in a single part stays a plain
    /// `put_object` — cheaper, and it leaves nothing behind if the run dies
    /// before the object is finished.
    async fn upload_part(&self, open: &mut OpenObject, body: Vec<u8>) -> Result<(), FaucetError> {
        if open.upload.is_none() {
            let key = self.generate_key();
            let out = self
                .client
                .create_multipart_upload()
                .bucket(&self.config.bucket)
                .key(&key)
                .content_type("application/x-ndjson")
                .send()
                .await
                .map_err(|e| {
                    FaucetError::Sink(format!("S3 create multipart upload for '{key}': {e}"))
                })?;
            let id = out.upload_id().ok_or_else(|| {
                FaucetError::Sink(format!(
                    "S3 create multipart upload for '{key}': no upload id"
                ))
            })?;
            open.upload = Some((key, id.to_string()));
        }
        let (key, upload_id) = open.upload.as_ref().expect("just set");
        // Part numbers are 1-based and must be contiguous in the completion
        // request, which is why they come from the parts already recorded
        // rather than from a counter that could drift.
        let part_number = open.parts.len() as i32 + 1;
        let body = self.encode_body(body)?;
        let out = self
            .client
            .upload_part()
            .bucket(&self.config.bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(body.into())
            .send()
            .await
            .map_err(|e| {
                FaucetError::Sink(format!("S3 upload part {part_number} for '{key}': {e}"))
            })?;
        open.parts.push(
            aws_sdk_s3::types::CompletedPart::builder()
                .set_e_tag(out.e_tag().map(str::to_string))
                .part_number(part_number)
                .build(),
        );
        Ok(())
    }

    /// Finish an object: complete its multipart upload (after sending the
    /// trailing tail as the last part) or, when no part was ever uploaded,
    /// write it in one `put_object`.
    async fn finish_object(
        &self,
        open: &mut OpenObject,
        obj: faucet_core::CompletedObject,
    ) -> Result<(), FaucetError> {
        if open.upload.is_none() {
            let key = self.generate_key();
            self.upload_file(&key, obj.body).await?;
            tracing::info!(key = %key, records = obj.rows, "S3 object written");
            return Ok(());
        }
        // A trailing tail may be empty when the object rolled exactly on a
        // part boundary; S3 rejects a zero-byte part, so skip it.
        if !obj.body.is_empty() {
            self.upload_part(open, obj.body).await?;
        }
        let (key, upload_id) = open.upload.take().expect("checked above");
        let parts = std::mem::take(&mut open.parts);
        let completed = aws_sdk_s3::types::CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();
        self.client
            .complete_multipart_upload()
            .bucket(&self.config.bucket)
            .key(&key)
            .upload_id(&upload_id)
            .multipart_upload(completed)
            .send()
            .await
            .map_err(|e| {
                FaucetError::Sink(format!("S3 complete multipart upload for '{key}': {e}"))
            })?;
        tracing::info!(key = %key, records = obj.rows, "S3 multipart object written");
        Ok(())
    }

    /// Encode one buffered group in the configured whole-object format and
    /// upload it as a single object (#604).
    ///
    /// A plain `put_object`, never multipart: these formats are encoded from a
    /// bounded in-memory group, so there is no stream to split into parts —
    /// and a header or container index written across parts would not be
    /// readable anyway.
    async fn write_encoded_object(&self, group: Vec<Value>) -> Result<(), FaucetError> {
        if group.is_empty() {
            return Ok(());
        }
        let format = self.config.format.shared().ok_or_else(|| {
            FaucetError::Sink(
                "S3 sink: parquet is written by the Arrow path, not the record encoder".into(),
            )
        })?;
        let rows = group.len();
        let body = faucet_core::file_format::encode(&group, format, &self.config.format_options())?;
        let key = self.generate_key();
        self.upload_file(&key, body).await?;
        tracing::info!(key = %key, records = rows, format = format.as_str(), "S3 object written");
        Ok(())
    }

    /// Upload a single JSONL file to S3.
    async fn upload_file(&self, key: &str, body: Vec<u8>) -> Result<(), FaucetError> {
        let body = self.encode_body(body)?;

        self.roundtrips.record("put");
        self.client
            .put_object()
            .bucket(&self.config.bucket)
            .key(key)
            .body(body.into())
            .content_type("application/x-ndjson")
            .send()
            .await
            .map_err(|e| FaucetError::Sink(format!("S3 put object error for key '{key}': {e}")))?;

        tracing::debug!(key = %key, "Uploaded S3 object");
        Ok(())
    }

    /// Upload pre-encoded Parquet objects concurrently. Parquet carries its own
    /// internal compression, so the crate-local `compression` wrapper is
    /// deliberately **not** applied here; the content type advertises Parquet.
    #[cfg(feature = "arrow")]
    async fn upload_parquet_objects(
        &self,
        prepared: Vec<(String, Vec<u8>)>,
    ) -> Result<(), FaucetError> {
        let concurrency = self.config.concurrency.max(1);
        futures::stream::iter(prepared)
            .map(|(key, body)| async move {
                self.roundtrips.record("put");
                self.client
                    .put_object()
                    .bucket(&self.config.bucket)
                    .key(&key)
                    .body(body.into())
                    .content_type("application/vnd.apache.parquet")
                    .send()
                    .await
                    .map_err(|e| {
                        FaucetError::Sink(format!("S3 put object error for key '{key}': {e}"))
                    })?;
                tracing::debug!(key = %key, "Uploaded S3 parquet object");
                Ok::<(), FaucetError>(())
            })
            .buffer_unordered(concurrency)
            .try_collect::<Vec<()>>()
            .await?;
        Ok(())
    }
}

#[async_trait]
impl faucet_core::Sink for S3Sink {
    fn set_roundtrip_recorder(
        &self,
        recorder: std::sync::Arc<faucet_core::observability::RoundtripRecorder>,
    ) {
        self.roundtrips.install(recorder);
    }
    /// Close the open object (#618).
    ///
    /// The pipeline calls `flush` at every bookmark-carrying page and once at
    /// the end, so the accumulated remainder is uploaded before the bookmark
    /// advances — an object left unfinished after a "successful" run is data
    /// loss with a green exit code.
    async fn flush(&self) -> Result<(), FaucetError> {
        let mut open = self.open.lock().await;
        if let Some(obj) = open.acc.finish() {
            self.finish_object(&mut open, obj).await?;
        }
        if let Some(group) = open.pending.finish() {
            self.write_encoded_object(group).await?;
        }
        Ok(())
    }

    fn connector_name(&self) -> &'static str {
        "s3"
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(S3SinkConfig)).expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        format!("s3://{}/{}", self.config.bucket, self.config.prefix)
    }

    /// Preflight probe: confirm the configured bucket is reachable and the
    /// credentials work via a non-mutating `HeadBucket` call. Uploads nothing.
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};

        let started = std::time::Instant::now();
        let probe = match tokio::time::timeout(
            ctx.timeout,
            self.client.head_bucket().bucket(&self.config.bucket).send(),
        )
        .await
        {
            Ok(Ok(_)) => Probe::pass("auth", started.elapsed()),
            Ok(Err(e)) => Probe::fail_hint(
                "auth",
                started.elapsed(),
                e.to_string(),
                "check bucket name, credentials, and network",
            ),
            Err(_) => Probe::fail("network", started.elapsed(), "timed out"),
        };
        Ok(CheckReport::single(probe))
    }

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        let chunks: Vec<&[Value]> = match self.config.effective_chunk_cap() {
            Some(cap) => records.chunks(cap).collect(),
            None => vec![records],
        };

        // Parquet path: encode each chunk as a self-contained Parquet object.
        #[cfg(feature = "arrow")]
        if matches!(self.config.format, S3SinkFormat::Parquet) {
            let prepared: Vec<(String, Vec<u8>)> = chunks
                .iter()
                .map(|chunk| {
                    let batch = faucet_core::columnar::values_to_record_batch_inferred(chunk)?;
                    let body = encode_parquet(&batch)?;
                    Ok((self.generate_key(), body))
                })
                .collect::<Result<Vec<_>, FaucetError>>()?;
            self.upload_parquet_objects(prepared).await?;
            tracing::info!(
                records = records.len(),
                files = chunks.len(),
                "S3 parquet batch write complete"
            );
            return Ok(records.len());
        }

        // Whole-object formats (#604): a CSV header, an XML document element,
        // a workbook index and a JSON array's brackets all need every record
        // before any byte is final, so records are buffered and encoded
        // together. The same row/byte caps decide the rollover, so object
        // sizing means the same thing whatever the format.
        if !self.config.format.appends_per_record() {
            let mut open = self.open.lock().await;
            if let Some(group) = open.pending.push_page(records) {
                self.write_encoded_object(group).await?;
            }
            return Ok(records.len());
        }

        // JSONL path: accumulate across calls and roll on the record/byte cap
        // (#618). `batch_size` still bounds how much is held before a rollover
        // check, but it no longer *forces* an object boundary — a page smaller
        // than the cap now joins the open object instead of becoming a file.
        let mut open = self.open.lock().await;
        for chunk in &chunks {
            for record in *chunk {
                match open.acc.push_record(record)? {
                    faucet_core::object_rollover::Emit::Nothing => {}
                    faucet_core::object_rollover::Emit::Part(body) => {
                        self.upload_part(&mut open, body).await?;
                    }
                    faucet_core::object_rollover::Emit::Object(obj) => {
                        self.finish_object(&mut open, obj).await?;
                    }
                }
            }
        }
        tracing::debug!(
            records = records.len(),
            open_rows = open.acc.rows(),
            "S3 batch accumulated (objects roll on the record/byte cap)"
        );
        Ok(records.len())
    }

    /// The S3 sink consumes Arrow `RecordBatch`es natively **only** when
    /// configured for the [`Parquet`](S3SinkFormat::Parquet) format; the JSONL
    /// format has no columnar representation and stays on the row path
    /// (RFC 0002 / #375).
    #[cfg(feature = "arrow")]
    fn supports_columnar(&self) -> bool {
        matches!(self.config.format, S3SinkFormat::Parquet)
    }

    /// Write an Arrow `RecordBatch` as one or more self-contained Parquet
    /// objects (sliced by the effective per-object cap), skipping the
    /// `Value` round-trip. Falls back to the row path for a non-Parquet
    /// format (which `supports_columnar` prevents the pipeline from reaching,
    /// but a direct caller might).
    #[cfg(feature = "arrow")]
    async fn write_batch_columnar(
        &self,
        batch: &arrow::array::RecordBatch,
    ) -> Result<usize, FaucetError> {
        if batch.num_rows() == 0 {
            return Ok(0);
        }
        if !matches!(self.config.format, S3SinkFormat::Parquet) {
            let rows = faucet_core::columnar::record_batch_to_values(batch)?;
            return self.write_batch(&rows).await;
        }

        let n = batch.num_rows();
        let cap = self.config.effective_chunk_cap().unwrap_or(n).max(1);
        let mut prepared: Vec<(String, Vec<u8>)> = Vec::new();
        let mut offset = 0usize;
        while offset < n {
            let len = cap.min(n - offset);
            let slice = batch.slice(offset, len);
            let body = encode_parquet(&slice)?;
            prepared.push((self.generate_key(), body));
            offset += len;
        }

        let files = prepared.len();
        self.upload_parquet_objects(prepared).await?;
        tracing::info!(records = n, files, "S3 parquet columnar write complete");
        Ok(n)
    }
}

/// Encode an Arrow `RecordBatch` into a complete, self-contained Parquet file
/// (ZSTD-compressed) in memory.
#[cfg(feature = "arrow")]
fn encode_parquet(batch: &arrow::array::RecordBatch) -> Result<Vec<u8>, FaucetError> {
    use parquet::arrow::ArrowWriter;
    use parquet::basic::{Compression, ZstdLevel};
    use parquet::file::properties::WriterProperties;

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))
            .map_err(|e| FaucetError::Sink(format!("parquet writer init failed: {e}")))?;
        writer
            .write(batch)
            .map_err(|e| FaucetError::Sink(format!("parquet write failed: {e}")))?;
        writer
            .close()
            .map_err(|e| FaucetError::Sink(format!("parquet finalize failed: {e}")))?;
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::S3SinkConfig;
    use faucet_core::Sink as _;
    use serde_json::json;

    /// Helper to build an S3Sink synchronously for tests that never make network calls.
    fn test_sink(config: S3SinkConfig) -> S3Sink {
        let sdk_config = aws_config::SdkConfig::builder()
            .behavior_version(aws_config::BehaviorVersion::latest())
            .build();
        let client = Client::new(&sdk_config);
        let open = tokio::sync::Mutex::new(OpenObject::new(&config));
        S3Sink {
            config,
            client,
            open,
            roundtrips: faucet_core::observability::RecorderSlot::new(),
        }
    }

    /// #604 — a whole-object format buffers records and encodes them together
    /// at the rollover, rather than appending a record at a time.
    #[tokio::test]
    async fn a_whole_object_format_buffers_until_the_cap() {
        let cfg = S3SinkConfig::new("b")
            .prefix("out/")
            .format(S3SinkFormat::Csv)
            .file_extension(".csv")
            .max_records_per_file(3);
        assert!(!cfg.format.appends_per_record());
        let sink = test_sink(cfg);
        let _ = &sink;
        // Two pages of two: the first rollover happens on the fourth record,
        // so nothing is emitted by the first page alone.
        {
            let mut open = sink.open.lock().await;
            assert!(
                open.pending
                    .push_page(&[json!({"a": 1}), json!({"a": 2})])
                    .is_none(),
                "under the cap, nothing rolls"
            );
            let group = open
                .pending
                .push_page(&[json!({"a": 3}), json!({"a": 4})])
                .expect("the cap is reached");
            assert_eq!(group.len(), 4, "a group overshoots by at most one page");
        }
    }

    #[cfg(feature = "file-format-csv")]
    #[test]
    fn a_csv_object_carries_a_header_and_the_configured_delimiter() {
        let cfg = S3SinkConfig::new("b")
            .format(S3SinkFormat::Csv)
            .csv(faucet_core::CsvOptions {
                delimiter: ";".into(),
                has_headers: true,
            });
        let body = faucet_core::file_format::encode(
            &[json!({"a": 1, "b": 2})],
            cfg.format
                .shared()
                .expect("csv maps onto the shared format"),
            &cfg.format_options(),
        )
        .expect("encode");
        assert_eq!(String::from_utf8(body).unwrap(), "a;b\n1;2\n");
    }

    #[test]
    fn json_array_is_one_array_per_object_not_one_per_line() {
        let cfg = S3SinkConfig::new("b").format(S3SinkFormat::JsonArray);
        assert!(!cfg.format.appends_per_record());
        let body = faucet_core::file_format::encode(
            &[json!({"a": 1}), json!({"a": 2})],
            cfg.format
                .shared()
                .expect("json_array maps onto the shared format"),
            &cfg.format_options(),
        )
        .expect("encode");
        assert_eq!(String::from_utf8(body).unwrap(), r#"[{"a":1},{"a":2}]"#);
    }

    #[test]
    fn json_lines_is_the_one_format_that_still_streams() {
        assert!(S3SinkFormat::JsonLines.appends_per_record());
        assert!(!S3SinkFormat::JsonArray.appends_per_record());
    }

    #[test]
    fn dataset_uri_includes_bucket_and_prefix() {
        let sink = test_sink(S3SinkConfig::new("my-bucket").prefix("data/events/"));
        assert_eq!(sink.dataset_uri(), "s3://my-bucket/data/events/");
    }

    #[test]
    fn records_encode_as_ndjson() {
        let records = vec![
            json!({"id": 1, "name": "Alice"}),
            json!({"id": 2, "name": "Bob"}),
        ];
        // The encoding moved into `faucet_core::ObjectAccumulator` with the
        // cross-page accumulation (#618) — pinned here too, because this is
        // what lands in the bucket.
        let mut acc = faucet_core::ObjectAccumulator::new(Some(records.len()), None);
        let mut body = Vec::new();
        for r in &records {
            if let faucet_core::object_rollover::Emit::Object(obj) = acc.push_record(r).unwrap() {
                body = obj.body;
            }
        }
        let result = body;
        let text = String::from_utf8(result).unwrap();
        let lines: Vec<&str> = text.trim().split('\n').collect();
        assert_eq!(lines.len(), 2);

        let first: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["id"], 1);
    }

    #[test]
    fn an_empty_accumulator_writes_no_object() {
        // An empty page must not mint a zero-byte object.
        let mut acc = faucet_core::ObjectAccumulator::new(Some(2), None);
        assert!(acc.finish().is_none());
        let result: Vec<u8> = Vec::new();
        assert!(result.is_empty());
    }

    #[test]
    fn generate_key_uses_prefix_and_extension() {
        let sink = test_sink(
            S3SinkConfig::new("bucket")
                .prefix("data/")
                .file_extension(".jsonl"),
        );
        let key = sink.generate_key();
        assert!(key.starts_with("data/"));
        assert!(key.ends_with(".jsonl"));
        // UUID is 36 chars
        assert!(key.len() > "data/".len() + ".jsonl".len());
    }

    #[test]
    fn generate_key_no_prefix() {
        let sink = test_sink(S3SinkConfig::new("bucket"));
        let key = sink.generate_key();
        assert!(key.ends_with(".jsonl"));
        // No prefix means key starts with UUID
        assert!(!key.starts_with('/'));
    }

    #[tokio::test]
    async fn new_rejects_out_of_range_batch_size() {
        let mut config = S3SinkConfig::new("bucket");
        config.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        match S3Sink::new(config).await {
            Err(faucet_core::FaucetError::Config(m)) => {
                assert!(m.contains("batch_size"), "got: {m}")
            }
            _ => panic!("expected a batch_size Config error"),
        }
    }

    // ── Parquet columnar path (feature `arrow`) ──────────────────────────────

    #[cfg(feature = "arrow")]
    #[test]
    fn supports_columnar_only_for_parquet_format() {
        let parquet_sink = test_sink(S3SinkConfig::new("b").format(S3SinkFormat::Parquet));
        assert!(faucet_core::Sink::supports_columnar(&parquet_sink));
        let json_sink = test_sink(S3SinkConfig::new("b"));
        assert!(!faucet_core::Sink::supports_columnar(&json_sink));
    }

    #[cfg(feature = "arrow")]
    #[test]
    fn encode_parquet_round_trips_via_reader() {
        use arrow::array::{Int32Array, RecordBatch, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        )
        .unwrap();

        let bytes = encode_parquet(&batch).unwrap();
        // Parquet magic header/footer.
        assert_eq!(&bytes[..4], b"PAR1");

        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
            .unwrap()
            .build()
            .unwrap();
        let total: usize = reader.map(|b| b.unwrap().num_rows()).sum();
        assert_eq!(total, 3);
    }

    #[cfg(feature = "compression")]
    #[test]
    fn compress_buf_used_for_gzip_extension() {
        // White-box: confirm the codec resolved from file_extension is Gzip
        // and that compress_buf produces a gzip-magic-prefixed buffer.
        let cfg = S3SinkConfig::new("bucket").file_extension(".jsonl.gz");
        let codec = cfg.compression.resolve(&cfg.file_extension);
        assert_eq!(codec, faucet_core::Compression::Gzip);
        let compressed = faucet_core::compression::compress_buf(b"hello\n", codec).unwrap();
        // gzip magic bytes.
        assert_eq!(&compressed[..2], b"\x1f\x8b");
    }
}
