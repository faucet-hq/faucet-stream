//! GCS sink executor.

use crate::config::GcsSinkConfig;
#[cfg(any(feature = "arrow", test))]
use crate::config::GcsSinkFormat;
use async_trait::async_trait;
use faucet_common_gcs::{build_storage, build_storage_control};
use faucet_core::FaucetError;
use futures::stream::{self, StreamExt, TryStreamExt};
use google_cloud_storage::client::Storage;
use serde_json::Value;

/// A sink that writes JSON records to GCS as JSON Lines files (or, with the
/// `arrow` feature, self-contained Parquet objects).
pub struct GcsSink {
    config: GcsSinkConfig,
    storage: Storage,
    /// Rows accumulated across `write_batch` calls for the open object (#618).
    ///
    /// Without this the sink wrote one object per upstream page, so a small
    /// `batch_size` produced a swarm of tiny objects — the small-files problem
    /// that dominates read time on a data lake.
    open: tokio::sync::Mutex<faucet_core::ObjectAccumulator>,
    /// Records held for a **whole-object** format (#604). CSV has a header,
    /// XML a document element, a workbook a container index and a JSON array
    /// its brackets — none can be appended a record at a time, so their
    /// records are buffered and encoded together at the rollover. Always empty
    /// for JSON Lines, which streams through `open`.
    pending: tokio::sync::Mutex<faucet_core::object_rollover::PageAccumulator>,
    /// Round-trip recorder installed by the pipeline (#638 / #704). Op: `put`
    /// (one per object upload).
    roundtrips: faucet_core::observability::RecorderSlot,
}

impl GcsSink {
    pub async fn new(config: GcsSinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let storage = build_storage(&config.auth, config.storage_host.as_deref()).await?;
        let open = tokio::sync::Mutex::new(faucet_core::ObjectAccumulator::new(
            Some(resolve_effective_chunk_size(&config)),
            config.max_bytes_per_file,
        ));
        let pending = tokio::sync::Mutex::new(faucet_core::object_rollover::PageAccumulator::new(
            Some(resolve_effective_chunk_size(&config)),
            config.max_bytes_per_file,
        ));
        Ok(Self {
            config,
            storage,
            open,
            pending,
            roundtrips: faucet_core::observability::RecorderSlot::new(),
        })
    }

    /// Encode one buffered group in the configured whole-object format and
    /// upload it as a single object (#604).
    async fn write_encoded_object(&self, group: Vec<Value>) -> Result<(), FaucetError> {
        if group.is_empty() {
            return Ok(());
        }
        let format = self.config.format.shared().ok_or_else(|| {
            FaucetError::Sink(
                "GCS sink: parquet is written by the Arrow path, not the record encoder".into(),
            )
        })?;
        let rows = group.len();
        let body = faucet_core::file_format::encode(&group, format, &self.config.format_options())?;
        let key = self.generate_key();
        self.upload_file(&key, body).await?;
        tracing::info!(key = %key, records = rows, format = format.as_str(), "GCS object written");
        Ok(())
    }

    /// Bucket as a GCS resource path: `projects/_/buckets/{bucket}`.
    fn bucket_path(&self) -> String {
        format!("projects/_/buckets/{}", self.config.bucket)
    }

    /// Generate a time-sortable UUIDv7 object name.
    fn generate_key(&self) -> String {
        generate_object_key(&self.config.prefix, &self.config.file_extension)
    }

    /// Upload a single JSONL file to GCS.
    async fn upload_file(&self, key: &str, body: Vec<u8>) -> Result<(), FaucetError> {
        #[cfg(feature = "compression")]
        let body = {
            let codec = self.config.compression.resolve(&self.config.file_extension);
            faucet_core::compression::warn_mismatch(&self.config.file_extension, codec);
            faucet_core::compression::compress_buf(&body, codec)?
        };

        let payload = bytes::Bytes::from(body);
        self.roundtrips.record("put");
        self.storage
            .write_object(self.bucket_path(), key.to_string(), payload)
            .set_content_type("application/x-ndjson")
            .send_unbuffered()
            .await
            .map_err(|e| FaucetError::Sink(format!("GCS put object error for key '{key}': {e}")))?;
        tracing::debug!(key = %key, "Uploaded GCS object");
        Ok(())
    }

    /// Upload a pre-encoded Parquet object. Parquet carries its own internal
    /// compression, so the crate-local `compression` wrapper is deliberately
    /// **not** applied; the content type advertises Parquet.
    #[cfg(feature = "arrow")]
    async fn upload_parquet_object(&self, key: &str, body: Vec<u8>) -> Result<(), FaucetError> {
        let payload = bytes::Bytes::from(body);
        self.roundtrips.record("put");
        self.storage
            .write_object(self.bucket_path(), key.to_string(), payload)
            .set_content_type("application/vnd.apache.parquet")
            .send_unbuffered()
            .await
            .map_err(|e| FaucetError::Sink(format!("GCS put object error for key '{key}': {e}")))?;
        tracing::debug!(key = %key, "Uploaded GCS parquet object");
        Ok(())
    }
}

#[async_trait]
impl faucet_core::Sink for GcsSink {
    fn set_roundtrip_recorder(
        &self,
        recorder: std::sync::Arc<faucet_core::observability::RoundtripRecorder>,
    ) {
        self.roundtrips.install(recorder);
    }
    fn dataset_uri(&self) -> String {
        format!("gs://{}/{}", self.config.bucket, self.config.prefix)
    }

    /// Close the open object (#618).
    ///
    /// The pipeline calls `flush` at every bookmark-carrying page and once at
    /// the end, so the accumulated remainder is uploaded before the bookmark
    /// advances — an object left unfinished after a "successful" run is data
    /// loss with a green exit code.
    async fn flush(&self) -> Result<(), FaucetError> {
        let finished = {
            let mut open = self.open.lock().await;
            open.finish()
        };
        if let Some(obj) = finished {
            let key = self.generate_key();
            self.upload_file(&key, obj.body).await?;
            tracing::info!(key = %key, records = obj.rows, "GCS object closed");
        }
        let group = {
            let mut pending = self.pending.lock().await;
            pending.finish()
        };
        if let Some(group) = group {
            self.write_encoded_object(group).await?;
        }
        Ok(())
    }

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        let concurrency = self.config.concurrency.max(1);
        let written = records.len();

        // Parquet path: encode each chunk as a self-contained Parquet object.
        // Parquet objects are self-describing and carry their own footer, so
        // they are not accumulated across pages — a rolled-over half-file
        // would not be readable.
        #[cfg(feature = "arrow")]
        if matches!(self.config.format, GcsSinkFormat::Parquet) {
            let chunk = resolve_effective_chunk_size(&self.config);
            let uploads: Vec<(String, Vec<u8>)> = records
                .chunks(chunk)
                .map(|slice| {
                    let batch = faucet_core::columnar::values_to_record_batch_inferred(slice)?;
                    let body = encode_parquet(&batch)?;
                    Ok::<(String, Vec<u8>), FaucetError>((self.generate_key(), body))
                })
                .collect::<Result<_, _>>()?;
            stream::iter(uploads)
                .map(|(key, body)| async move { self.upload_parquet_object(&key, body).await })
                .buffer_unordered(concurrency)
                .try_collect::<Vec<()>>()
                .await?;
            return Ok(written);
        }

        // Whole-object formats (#604): a CSV header, an XML document element,
        // a workbook index and a JSON array's brackets all need every record
        // before any byte is final, so records are buffered and encoded
        // together. The same row/byte caps decide the rollover, so object
        // sizing means the same thing whatever the format.
        if !self.config.format.appends_per_record() {
            let group = {
                let mut pending = self.pending.lock().await;
                pending.push_page(records)
            };
            if let Some(group) = group {
                self.write_encoded_object(group).await?;
            }
            return Ok(written);
        }

        // JSONL path: accumulate across calls and roll on the record/byte cap
        // (#618). A page smaller than the cap now joins the open object
        // instead of becoming an object of its own.
        let mut uploads: Vec<(String, Vec<u8>)> = Vec::new();
        {
            let mut open = self.open.lock().await;
            for record in records {
                if let faucet_core::object_rollover::Emit::Object(obj) = open.push_record(record)? {
                    uploads.push((self.generate_key(), obj.body));
                }
            }
        }
        stream::iter(uploads)
            .map(|(key, body)| async move { self.upload_file(&key, body).await })
            .buffer_unordered(concurrency)
            .try_collect::<Vec<()>>()
            .await?;

        Ok(written)
    }

    /// The GCS sink consumes Arrow `RecordBatch`es natively **only** when
    /// configured for the [`Parquet`](GcsSinkFormat::Parquet) format; the JSONL
    /// format stays on the row path (RFC 0002 / #375).
    #[cfg(feature = "arrow")]
    fn supports_columnar(&self) -> bool {
        matches!(self.config.format, GcsSinkFormat::Parquet)
    }

    /// Write an Arrow `RecordBatch` as one or more self-contained Parquet
    /// objects (sliced by the effective per-object chunk size), skipping the
    /// `Value` round-trip. Falls back to the row path for a non-Parquet format.
    #[cfg(feature = "arrow")]
    async fn write_batch_columnar(
        &self,
        batch: &arrow::array::RecordBatch,
    ) -> Result<usize, FaucetError> {
        if batch.num_rows() == 0 {
            return Ok(0);
        }
        if !matches!(self.config.format, GcsSinkFormat::Parquet) {
            let rows = faucet_core::columnar::record_batch_to_values(batch)?;
            return self.write_batch(&rows).await;
        }

        let n = batch.num_rows();
        let cap = resolve_effective_chunk_size(&self.config).min(n).max(1);
        let concurrency = self.config.concurrency.max(1);
        let mut uploads: Vec<(String, Vec<u8>)> = Vec::new();
        let mut offset = 0usize;
        while offset < n {
            let len = cap.min(n - offset);
            let slice = batch.slice(offset, len);
            uploads.push((self.generate_key(), encode_parquet(&slice)?));
            offset += len;
        }
        stream::iter(uploads)
            .map(|(key, body)| async move { self.upload_parquet_object(&key, body).await })
            .buffer_unordered(concurrency)
            .try_collect::<Vec<()>>()
            .await?;
        Ok(n)
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(GcsSinkConfig)).expect("schema serialization")
    }

    fn connector_name(&self) -> &'static str {
        "gcs"
    }

    /// Preflight probe: confirm the configured bucket is reachable and the
    /// credentials work via a non-mutating `list_objects` call capped at a
    /// single result. Writes nothing.
    ///
    /// The sink only holds a data-plane [`Storage`] client (which exposes no
    /// list/get-bucket call), so the probe builds a control-plane
    /// `StorageControl` client on demand using the same credentials.
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};

        let started = std::time::Instant::now();

        // Build a control-plane client (the data-plane Storage client has no
        // read-only list/get-bucket call). Credential/client-build failures
        // surface as a failed probe rather than an Err.
        let control =
            match build_storage_control(&self.config.auth, self.config.storage_host.as_deref())
                .await
            {
                Ok(c) => c,
                Err(e) => {
                    return Ok(CheckReport::single(Probe::fail_hint(
                        "auth",
                        started.elapsed(),
                        e.to_string(),
                        "check bucket name, credentials, and network",
                    )));
                }
            };

        let probe = match tokio::time::timeout(
            ctx.timeout,
            control
                .list_objects()
                .set_parent(self.bucket_path())
                .set_page_size(1_i32)
                .send(),
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
}

/// Pure helper for chunk-size resolution — used by `write_batch` and unit
/// tested directly so the test surface doesn't need a `Storage` stub.
fn resolve_effective_chunk_size(config: &GcsSinkConfig) -> usize {
    let bs = if config.batch_size == 0 {
        usize::MAX
    } else {
        config.batch_size
    };
    let mr = config.max_records_per_file.unwrap_or(usize::MAX);
    bs.min(mr)
}

/// Pure helper for object-key generation — used by `write_batch` and unit
/// tested directly. UUIDv7 makes keys time-sortable so a listing of the
/// destination bucket returns objects in write order.
fn generate_object_key(prefix: &str, file_extension: &str) -> String {
    format!("{prefix}{}{file_extension}", uuid::Uuid::now_v7())
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

    // dataset_uri test is skipped: GcsSink::new() requires Google Cloud
    // credentials (build_storage errors without auth), and no offline
    // constructor exists.

    /// #604 — only JSON Lines can be appended a record at a time; every other
    /// format has a header, a wrapper or a container index, so its records are
    /// buffered and encoded together.
    #[test]
    fn json_lines_is_the_one_format_that_still_streams() {
        assert!(GcsSinkFormat::JsonLines.appends_per_record());
        assert!(!GcsSinkFormat::JsonArray.appends_per_record());
        assert_eq!(
            GcsSinkFormat::JsonArray.shared(),
            Some(faucet_core::FileFormat::JsonArray)
        );
    }

    #[cfg(feature = "file-format-csv")]
    #[test]
    fn a_csv_object_carries_a_header_and_the_configured_delimiter() {
        let cfg = GcsSinkConfig::new("b")
            .format(GcsSinkFormat::Csv)
            .csv(faucet_core::CsvOptions {
                delimiter: ";".into(),
                has_headers: true,
            });
        assert!(!cfg.format.appends_per_record());
        let body = faucet_core::file_format::encode(
            &[serde_json::json!({"a": 1, "b": 2})],
            cfg.format
                .shared()
                .expect("csv maps onto the shared format"),
            &cfg.format_options(),
        )
        .expect("encode");
        assert_eq!(String::from_utf8(body).unwrap(), "a;b\n1;2\n");
    }

    #[tokio::test]
    async fn new_rejects_out_of_range_batch_size() {
        // Validation runs before any GCS client setup, so this needs no backend.
        let mut config = GcsSinkConfig::new("bucket");
        config.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        match GcsSink::new(config).await {
            Err(FaucetError::Config(m)) => assert!(m.contains("batch_size"), "got: {m}"),
            _ => panic!("expected a batch_size Config error"),
        }
    }
    use serde_json::json;

    #[test]
    fn records_encode_as_ndjson() {
        // The encoding moved into `faucet_core::ObjectAccumulator` with the
        // cross-page accumulation (#618) — pinned here too, because this is
        // what lands in the bucket.
        let mut acc = faucet_core::ObjectAccumulator::new(Some(2), None);
        acc.push_record(&json!({"a": 1})).unwrap();
        let faucet_core::object_rollover::Emit::Object(obj) =
            acc.push_record(&json!({"b": 2})).unwrap()
        else {
            panic!("rolled at 2 records");
        };
        let body = obj.body;
        assert_eq!(
            std::str::from_utf8(&body).unwrap(),
            "{\"a\":1}\n{\"b\":2}\n"
        );
    }

    #[test]
    fn an_empty_accumulator_writes_no_object() {
        // An empty page must not mint a zero-byte object — a listing full of
        // those is the small-files problem in its purest form.
        let mut acc = faucet_core::ObjectAccumulator::new(Some(2), None);
        assert!(acc.finish().is_none());
    }

    #[test]
    fn effective_chunk_size_unlimited_when_both_unset() {
        let cfg = GcsSinkConfig::new("b").with_batch_size(0);
        assert_eq!(resolve_effective_chunk_size(&cfg), usize::MAX);
    }

    #[test]
    fn effective_chunk_size_takes_smaller_limit() {
        let cfg = GcsSinkConfig::new("b")
            .with_batch_size(500)
            .max_records_per_file(100);
        assert_eq!(resolve_effective_chunk_size(&cfg), 100);
    }

    #[test]
    fn effective_chunk_size_uses_batch_size_when_smaller() {
        let cfg = GcsSinkConfig::new("b")
            .with_batch_size(50)
            .max_records_per_file(500);
        assert_eq!(resolve_effective_chunk_size(&cfg), 50);
    }

    #[test]
    fn generate_key_uses_prefix_and_extension() {
        let key = generate_object_key("out/", ".ndjson");
        assert!(key.starts_with("out/"));
        assert!(key.ends_with(".ndjson"));
    }

    #[test]
    fn generate_key_yields_distinct_time_ordered_keys() {
        let a = generate_object_key("p/", ".jsonl");
        let b = generate_object_key("p/", ".jsonl");
        assert_ne!(a, b);
        // UUIDv7 keys are lexically comparable by time within the same
        // process: the second key generated should compare greater.
        assert!(a < b, "expected UUIDv7 keys to sort by generation order");
    }

    // ── Parquet columnar path (feature `arrow`) ──────────────────────────────

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
    fn compress_buf_used_for_zstd_extension() {
        let cfg = GcsSinkConfig::new("bucket").file_extension(".jsonl.zst");
        let codec = cfg.compression.resolve(&cfg.file_extension);
        assert_eq!(codec, faucet_core::Compression::Zstd);
        let compressed = faucet_core::compression::compress_buf(b"hello\n", codec).unwrap();
        // zstd magic bytes: 0x28 B5 2F FD.
        assert_eq!(&compressed[..4], b"\x28\xb5\x2f\xfd");
    }
}
