//! Azure Blob sink executor.

use std::sync::Arc;

use async_trait::async_trait;
use faucet_common_azure::build_store;
use faucet_core::FaucetError;
use futures::stream::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use serde_json::Value;

use crate::config::AzureBlobSinkConfig;

/// A sink that writes JSON records to Azure Blob as JSON Lines objects.
pub struct AzureBlobSink {
    config: AzureBlobSinkConfig,
    store: Arc<dyn ObjectStore>,
    /// Rows accumulated across `write_batch` calls for the open object (#618).
    open: tokio::sync::Mutex<OpenObject>,
}

/// The in-flight object: the accumulator plus, once a part has been uploaded,
/// the `object_store` multipart upload it belongs to.
///
/// Started **lazily**, on the first full part — an object that fits in one
/// part stays a plain `put`, which is cheaper and leaves nothing abandoned if
/// the run dies before the object is finished.
struct OpenObject {
    acc: faucet_core::ObjectAccumulator,
    /// Records held for a **whole-object** format (#604). CSV has a header,
    /// XML a document element, a workbook a container index and a JSON array
    /// its brackets — none can be appended a record at a time, so their
    /// records are buffered and encoded together at the rollover. Always empty
    /// for JSON Lines, which streams through `acc`.
    pending: faucet_core::object_rollover::PageAccumulator,
    upload: Option<(String, Box<dyn object_store::MultipartUpload>)>,
}

/// Part size for the streaming multipart upload (#618).
///
/// Azure block blobs cap a single `put` at 5000 MiB but the practical reason
/// to stream is memory: without parts the whole object is held before upload,
/// so output size is bounded by RAM. 8 MiB is the `object_store` default
/// block size and a good trade between request count and buffer size.
const PART_BYTES: usize = 8 * 1024 * 1024;

impl AzureBlobSink {
    /// Construct the sink, building the object store eagerly so it is reused
    /// across calls.
    pub async fn new(config: AzureBlobSinkConfig) -> Result<Self, FaucetError> {
        faucet_core::validate_batch_size(config.batch_size)?;
        let store = build_store(&config.connection)?;
        let open = tokio::sync::Mutex::new(OpenObject {
            pending: faucet_core::object_rollover::PageAccumulator::new(
                Some(resolve_effective_chunk_size(&config)),
                config.max_bytes_per_file,
            ),
            acc: faucet_core::ObjectAccumulator::new(
                Some(resolve_effective_chunk_size(&config)),
                config.max_bytes_per_file,
            )
            .with_part_size(PART_BYTES),
            upload: None,
        });
        Ok(Self {
            config,
            store,
            open,
        })
    }

    /// Generate a time-sortable UUIDv7 object name.
    fn generate_key(&self) -> String {
        generate_object_key(&self.config.prefix, &self.config.file_extension)
    }

    /// Upload one multipart part, starting the upload if this is the first.
    ///
    /// This is the memory bound: the accumulator hands over a full part and
    /// drops its buffer, so peak stays at O(part size) instead of O(object
    /// size). Before #618 the sink held the whole object and issued a single
    /// `put`, so output size was capped by the process's memory.
    async fn upload_part(&self, open: &mut OpenObject, body: Vec<u8>) -> Result<(), FaucetError> {
        if open.upload.is_none() {
            let key = self.generate_key();
            let path = ObjectPath::from(key.clone());
            let upload = self.store.put_multipart(&path).await.map_err(|e| {
                FaucetError::Sink(format!("azure start multipart for '{key}': {e}"))
            })?;
            open.upload = Some((key, upload));
        }
        let (key, upload) = open.upload.as_mut().expect("just set");
        let body = self.encode_body(body)?;
        upload
            .put_part(bytes::Bytes::from(body).into())
            .await
            .map_err(|e| FaucetError::Sink(format!("azure put part for '{key}': {e}")))?;
        Ok(())
    }

    /// Finish an object: complete its multipart upload (after sending the
    /// trailing tail as the last part) or, when no part was ever uploaded,
    /// write it in one `put`.
    async fn finish_object(
        &self,
        open: &mut OpenObject,
        obj: faucet_core::CompletedObject,
    ) -> Result<(), FaucetError> {
        if open.upload.is_none() {
            let key = self.generate_key();
            self.upload_file(&key, obj.body).await?;
            tracing::info!(key = %key, records = obj.rows, "Azure object written");
            return Ok(());
        }
        // The tail can be empty when the object rolled exactly on a part
        // boundary; a zero-byte part is pointless and some stores reject it.
        if !obj.body.is_empty() {
            self.upload_part(open, obj.body).await?;
        }
        let (key, mut upload) = open.upload.take().expect("checked above");
        upload
            .complete()
            .await
            .map_err(|e| FaucetError::Sink(format!("azure complete multipart for '{key}': {e}")))?;
        tracing::info!(key = %key, records = obj.rows, "Azure multipart object written");
        Ok(())
    }

    /// Apply the configured codec to a body (or a multipart part).
    ///
    /// Applied per part on the multipart path: gzip and zstd both concatenate,
    /// so a multi-member object decodes transparently — the same property the
    /// file sinks already rely on. Compressing the whole object instead would
    /// mean buffering it, which is the bound multipart exists to remove.
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

    /// Upload a single JSONL object.
    /// Encode one buffered group in the configured whole-object format and
    /// upload it as a single object (#604).
    async fn write_encoded_object(&self, group: Vec<Value>) -> Result<(), FaucetError> {
        if group.is_empty() {
            return Ok(());
        }
        let format = self.config.format.shared();
        let rows = group.len();
        let body = faucet_core::file_format::encode(&group, format, &self.config.format_options())?;
        let key = self.generate_key();
        self.upload_file(&key, body).await?;
        tracing::info!(key = %key, records = rows, format = format.as_str(), "Azure object written");
        Ok(())
    }

    async fn upload_file(&self, key: &str, body: Vec<u8>) -> Result<(), FaucetError> {
        let body = self.encode_body(body)?;
        let path = ObjectPath::from(key);
        let payload = bytes::Bytes::from(body);
        self.store.put(&path, payload.into()).await.map_err(|e| {
            FaucetError::Sink(format!("azure put object error for key '{key}': {e}"))
        })?;
        tracing::debug!(key = %key, "Uploaded Azure object");
        Ok(())
    }
}

#[async_trait]
impl faucet_core::Sink for AzureBlobSink {
    fn dataset_uri(&self) -> String {
        format!("az://{}/{}", self.config.container(), self.config.prefix)
    }

    /// Close the open object (#618).
    ///
    /// The pipeline calls `flush` at every bookmark-carrying page and once at
    /// the end, so the remainder lands before the bookmark advances — an
    /// object left unfinished after a "successful" run is data loss with a
    /// green exit code.
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

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        // Accumulate across calls and roll on the record/byte cap (#618): a
        // page smaller than the cap joins the open object rather than
        // becoming an object of its own.
        //
        // Parts are collected and uploaded with the closing body rather than
        // streamed into a `put_multipart` handle: holding an open handle
        // across `write_batch` calls would mean carrying a `!Send`-shaped
        // writer in the sink's mutex, and the memory win is already had — the
        // accumulator hands the buffer over at `PART_BYTES` and drops it.
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

        let mut files = 0usize;
        let mut open = self.open.lock().await;
        for record in records {
            match open.acc.push_record(record)? {
                faucet_core::object_rollover::Emit::Nothing => {}
                faucet_core::object_rollover::Emit::Part(body) => {
                    self.upload_part(&mut open, body).await?;
                }
                faucet_core::object_rollover::Emit::Object(obj) => {
                    self.finish_object(&mut open, obj).await?;
                    files += 1;
                }
            }
        }
        let written = records.len();

        tracing::info!(records = written, files, "Azure batch write complete");
        Ok(written)
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(AzureBlobSinkConfig))
            .expect("schema serialization")
    }

    fn connector_name(&self) -> &'static str {
        "azure-blob"
    }

    /// Preflight probe: confirm the container is reachable and the credentials
    /// work via a non-mutating listing capped at a single item. Uploads
    /// nothing.
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

/// Pure chunk-size resolution — unit tested directly so the test surface needs
/// no object store.
fn resolve_effective_chunk_size(config: &AzureBlobSinkConfig) -> usize {
    let bs = if config.batch_size == 0 {
        usize::MAX
    } else {
        config.batch_size
    };
    let mr = config.max_records_per_file.unwrap_or(usize::MAX);
    bs.min(mr)
}

/// Pure object-key generation — UUIDv7 makes keys time-sortable so a listing of
/// the destination returns objects in write order.
fn generate_object_key(prefix: &str, file_extension: &str) -> String {
    format!("{prefix}{}{file_extension}", uuid::Uuid::now_v7())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn new_rejects_out_of_range_batch_size() {
        let mut config = AzureBlobSinkConfig::new("cont");
        config.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        match AzureBlobSink::new(config).await {
            Err(FaucetError::Config(m)) => assert!(m.contains("batch_size"), "got: {m}"),
            Ok(_) => panic!("expected a batch_size Config error, got Ok(sink)"),
            Err(e) => panic!("expected a batch_size Config error, got {e:?}"),
        }
    }

    #[tokio::test]
    async fn new_rejects_empty_container() {
        let config = AzureBlobSinkConfig::new("   ");
        match AzureBlobSink::new(config).await {
            Err(FaucetError::Config(m)) => assert!(m.contains("container"), "got: {m}"),
            Ok(_) => panic!("expected a container Config error, got Ok(sink)"),
            Err(e) => panic!("expected a container Config error, got {e:?}"),
        }
    }

    #[tokio::test]
    async fn new_builds_lazily_with_emulator() {
        use faucet_core::Sink as _;
        let config = AzureBlobSinkConfig::new("cont")
            .prefix("out/")
            .use_emulator(true)
            .allow_http(true);
        let sink = AzureBlobSink::new(config).await.unwrap();
        assert_eq!(sink.connector_name(), "azure-blob");
        assert_eq!(sink.dataset_uri(), "az://cont/out/");
    }

    /// The NDJSON encoding moved into `faucet_core::ObjectAccumulator` with
    /// the cross-page accumulation (#618) — pinned here too, because this sink
    /// is what an operator reads back and a drift in either direction would
    /// change the bytes in the blob.
    #[test]
    fn records_encode_as_ndjson() {
        let mut acc = faucet_core::ObjectAccumulator::new(Some(2), None);
        acc.push_record(&json!({"a": 1})).unwrap();
        let faucet_core::object_rollover::Emit::Object(obj) =
            acc.push_record(&json!({"b": 2})).unwrap()
        else {
            panic!("rolled at 2 records");
        };
        assert_eq!(
            std::str::from_utf8(&obj.body).unwrap(),
            "{\"a\":1}\n{\"b\":2}\n"
        );
    }

    #[test]
    fn an_empty_accumulator_writes_no_object() {
        // An empty page must not mint an empty blob — a listing full of
        // zero-byte objects is the small-files problem in its purest form.
        let mut acc = faucet_core::ObjectAccumulator::new(Some(2), None);
        assert!(acc.finish().is_none());
    }

    #[test]
    fn effective_chunk_size_unlimited_when_both_unset() {
        let cfg = AzureBlobSinkConfig::new("c").with_batch_size(0);
        assert_eq!(resolve_effective_chunk_size(&cfg), usize::MAX);
    }

    #[test]
    fn effective_chunk_size_takes_smaller_limit() {
        let cfg = AzureBlobSinkConfig::new("c")
            .with_batch_size(500)
            .max_records_per_file(100);
        assert_eq!(resolve_effective_chunk_size(&cfg), 100);
    }

    #[test]
    fn effective_chunk_size_uses_batch_size_when_smaller() {
        let cfg = AzureBlobSinkConfig::new("c")
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
        assert!(a < b, "expected UUIDv7 keys to sort by generation order");
    }

    #[cfg(feature = "compression")]
    #[test]
    fn compress_buf_used_for_gzip_extension() {
        let cfg = AzureBlobSinkConfig::new("c").file_extension(".jsonl.gz");
        let codec = cfg.compression.resolve(&cfg.file_extension);
        assert_eq!(codec, faucet_core::Compression::Gzip);
        let compressed = faucet_core::compression::compress_buf(b"hello\n", codec).unwrap();
        assert_eq!(&compressed[..2], b"\x1f\x8b");
    }
}
