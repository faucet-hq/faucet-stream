//! S3 sink executor: the shared file writer (#777) over an S3 backend.

use crate::config::S3SinkConfig;
use crate::object::{PART_BYTES, S3Objects};
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use faucet_common_file::write::{
    BoxFuture, FileWriter, MultipartClient, MultipartUpload, RemoteBackend, SinkIdentity,
    WriterSink, content_type,
};
use faucet_core::{FaucetError, FileFormat};
use serde_json::Value;
use std::sync::{Arc, Mutex};

/// A sink that writes records to S3 objects in any format the local file
/// sink writes — JSON Lines, JSON, CSV, XML, Excel, Avro, Parquet or raw
/// text — through the shared file writer.
///
/// JSON Lines and raw text go up as a multipart upload while they are
/// written, so neither memory nor local disk grows with the object; every
/// other format is built in a local scratch file and published with one
/// upload (multipart past 8 MiB) when it closes: at the row / byte cap or at
/// `flush`. Up to `concurrency` uploads (objects and parts) run at once,
/// while the next data is encoded; `flush` waits for all of them, so a
/// bookmark never advances past an object that is not in the bucket.
pub struct S3Sink {
    inner: WriterSink,
}

use faucet_core::CallExt as _;

impl S3Sink {
    /// Create a new S3 sink from the given configuration.
    ///
    /// Builds the S3 client eagerly so it is reused across calls.
    pub async fn new(config: S3SinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let client = Self::build_client(&config).await?;
        Self::with_client(config, client)
    }

    fn with_client(config: S3SinkConfig, client: Client) -> Result<Self, FaucetError> {
        let write = config.write_config();
        let settings = write.settings()?;
        let (base, template) = write.object_layout(&settings)?;
        let roundtrips = Arc::new(faucet_core::observability::RecorderSlot::new());
        let calls = Arc::new(faucet_core::CallSlot::new(
            faucet_core::CallContext::new()
                .without_timeout()
                .with_roundtrip_metering(false),
        ));
        let objects = Arc::new(S3Objects {
            client: client.clone(),
            bucket: config.bucket.clone(),
            concurrency: config.concurrency,
            part_bytes: PART_BYTES,
            roundtrips: roundtrips.clone(),
            calls: calls.clone(),
            part_slots: Arc::new(tokio::sync::Semaphore::new(config.concurrency.max(1))),
        });
        let backend = RemoteBackend::new(
            objects.clone(),
            base,
            &template,
            config.scratch_dir.as_deref().map(std::path::Path::new),
        )?
        .with_upload_concurrency(config.concurrency)
        .with_multipart(Arc::new(S3Parts(objects)));
        let writer = FileWriter::new(settings, template, Arc::new(backend))?;
        let identity = S3Identity {
            client,
            bucket: config.bucket.clone(),
            prefix: config.prefix.clone(),
            roundtrips,
            calls,
        };
        Ok(Self {
            inner: WriterSink::new(writer, identity),
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
        Ok(Client::new(&sdk_config))
    }

    /// The format objects are written in.
    pub fn format(&self) -> FileFormat {
        self.inner.format()
    }
}

faucet_common_file::delegate_sink!(S3Sink, inner);

/// What the S3 sink supplies to the shared sink.
struct S3Identity {
    client: Client,
    bucket: String,
    prefix: String,
    roundtrips: Arc<faucet_core::observability::RecorderSlot>,
    calls: Arc<faucet_core::CallSlot>,
}

#[faucet_core::async_trait]
impl SinkIdentity for S3Identity {
    fn connector_name(&self) -> &'static str {
        "s3"
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(S3SinkConfig)).expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.prefix)
    }

    fn set_roundtrip_recorder(&self, recorder: Arc<faucet_core::observability::RoundtripRecorder>) {
        self.roundtrips.install(recorder.clone());
        self.calls.install_recorder(recorder);
    }

    fn set_call_context(&self, ctx: faucet_core::CallContext) {
        self.calls.install(ctx);
    }

    /// Preflight probe: confirm the configured bucket is reachable and the
    /// credentials work via a non-mutating `HeadBucket` call. Uploads nothing.
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};
        let started = std::time::Instant::now();
        let request = self.client.head_bucket().bucket(&self.bucket);
        let sent = request.send().within(&self.calls, "head");
        let probe = match tokio::time::timeout(ctx.timeout, sent).await {
            Ok(Err(e)) => Probe::fail("auth", started.elapsed(), e.to_string()),
            Ok(Ok(Ok(_))) => Probe::pass("auth", started.elapsed()),
            Ok(Ok(Err(e))) => Probe::fail_hint(
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

/// S3 multipart uploads for objects written in parts (#783).
struct S3Parts(Arc<S3Objects>);

fn s3_err(what: &str, key: &str, e: impl std::fmt::Display) -> FaucetError {
    FaucetError::Sink(format!("S3 {what} error for key '{key}': {e}"))
}

#[faucet_core::async_trait]
impl MultipartClient for S3Parts {
    fn part_size(&self) -> usize {
        self.0.part_bytes
    }

    async fn start(&self, key: &str) -> Result<Box<dyn MultipartUpload>, FaucetError> {
        let o = &self.0;
        o.roundtrips.record("put");
        let created = o
            .client
            .create_multipart_upload()
            .bucket(&o.bucket)
            .key(key)
            .content_type(content_type(key))
            .send()
            .within(&o.calls, "put")
            .await?
            .map_err(|e| o.sdk_err("start multipart", key, e))?;
        let upload_id = created
            .upload_id()
            .ok_or_else(|| s3_err("start multipart", key, "no upload id"))?
            .to_string();
        Ok(Box::new(S3Upload {
            objects: o.clone(),
            key: key.to_string(),
            upload_id,
            parts: Arc::new(Mutex::new(Vec::new())),
        }))
    }
}

/// The S3 part number of part `number`, refusing one past S3's limit of
/// [`MAX_PARTS`](crate::object::MAX_PARTS) parts per upload.
fn part_number(number: u32, key: &str) -> Result<i32, FaucetError> {
    if u64::from(number) > crate::object::MAX_PARTS {
        return Err(s3_err(
            "upload part",
            key,
            format!(
                "the object needs more than S3's {} parts; set `max_bytes_per_file` so files \
                 roll over before that",
                crate::object::MAX_PARTS
            ),
        ));
    }
    i32::try_from(number).map_err(|_| s3_err("upload part", key, "part number out of range"))
}

struct S3Upload {
    objects: Arc<S3Objects>,
    key: String,
    upload_id: String,
    parts: Arc<Mutex<Vec<CompletedPart>>>,
}

impl MultipartUpload for S3Upload {
    fn put_part(&self, number: u32, body: Vec<u8>) -> BoxFuture<Result<(), FaucetError>> {
        let (o, key, id, parts) = (
            self.objects.clone(),
            self.key.clone(),
            self.upload_id.clone(),
            self.parts.clone(),
        );
        Box::pin(async move {
            let number = part_number(number, &key)?;
            let _slot = o
                .part_slots
                .acquire()
                .await
                .map_err(|e| s3_err("upload part", &key, e))?;
            o.roundtrips.record("put");
            let out = o
                .client
                .upload_part()
                .bucket(&o.bucket)
                .key(&key)
                .upload_id(&id)
                .part_number(number)
                .body(ByteStream::from(body))
                .send()
                .within(&o.calls, "put")
                .await?
                .map_err(|e| o.sdk_err("upload part", &key, e))?;
            parts.lock().unwrap_or_else(|p| p.into_inner()).push(
                CompletedPart::builder()
                    .part_number(number)
                    .set_e_tag(out.e_tag().map(str::to_string))
                    .build(),
            );
            Ok(())
        })
    }

    fn complete(self: Box<Self>) -> BoxFuture<Result<(), FaucetError>> {
        Box::pin(async move {
            let mut parts =
                std::mem::take(&mut *self.parts.lock().unwrap_or_else(|p| p.into_inner()));
            parts.sort_by_key(|p| p.part_number());
            let o = &self.objects;
            o.roundtrips.record("put");
            o.client
                .complete_multipart_upload()
                .bucket(&o.bucket)
                .key(&self.key)
                .upload_id(&self.upload_id)
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .set_parts(Some(parts))
                        .build(),
                )
                .send()
                .within(&o.calls, "put")
                .await?
                .map_err(|e| o.sdk_err("complete multipart", &self.key, e))?;
            Ok(())
        })
    }

    fn abort(self: Box<Self>) -> BoxFuture<Result<(), FaucetError>> {
        Box::pin(async move {
            let o = &self.objects;
            o.client
                .abort_multipart_upload()
                .bucket(&o.bucket)
                .key(&self.key)
                .upload_id(&self.upload_id)
                .send()
                .within(&o.calls, "put")
                .await?
                .map_err(|e| o.sdk_err("abort multipart", &self.key, e))?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #862 L-163: a streamed object stops with a clear error at S3's
    /// 10,000-part limit instead of an opaque rejection from the store.
    #[test]
    fn part_numbers_stop_at_the_s3_limit() {
        assert_eq!(part_number(1, "k").unwrap(), 1);
        assert_eq!(part_number(10_000, "k").unwrap(), 10_000);
        let err = part_number(10_001, "k").unwrap_err().to_string();
        assert!(
            err.contains("10000 parts") && err.contains("max_bytes_per_file"),
            "{err}"
        );
    }
}
