//! Azure Blob sink executor: the shared file writer (#777) over an Azure
//! Blob backend.

use std::sync::{Arc, Mutex};

use faucet_common_azure::build_store;
use faucet_common_file::write::{
    BoxFuture, FileWriter, MultipartClient, MultipartUpload, RemoteBackend, SinkIdentity,
    WriterSink,
};
use faucet_core::{FaucetError, FileFormat};
use futures::stream::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use serde_json::Value;

use crate::config::AzureBlobSinkConfig;
use crate::object::{AzureObjects, PART_BYTES};

/// A sink that writes records to Azure blobs in any format the local file
/// sink writes — JSON Lines, JSON, CSV, XML, Excel, Avro, Parquet or raw
/// text — through the shared file writer.
///
/// JSON Lines and raw text go up as blocks while they are written, so
/// neither memory nor local disk grows with the blob; every other format is
/// built in a local scratch file and published with one upload (a committed
/// block list past 8 MiB) when it closes: at the row / byte cap or at
/// `flush`. Up to `concurrency` uploads (blobs and blocks) run at once,
/// while the next data is encoded; `flush` waits for all of them, so a
/// bookmark never advances past a blob that is not in the container.
pub struct AzureBlobSink {
    inner: WriterSink,
}

impl AzureBlobSink {
    /// Construct the sink, building the object store eagerly so it is reused
    /// across calls.
    pub async fn new(config: AzureBlobSinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let store = build_store(&config.connection)?;
        let write = config.write_config();
        let settings = write.settings()?;
        let (base, template) = write.object_layout(&settings)?;
        let objects = Arc::new(AzureObjects {
            store: store.clone(),
            container: config.container().to_string(),
            part_bytes: PART_BYTES,
            concurrency: config.concurrency,
        });
        let backend = RemoteBackend::new(
            objects,
            base,
            &template,
            config.scratch_dir.as_deref().map(std::path::Path::new),
        )?
        .with_upload_concurrency(config.concurrency)
        .with_multipart(Arc::new(AzureBlocks {
            store: store.clone(),
            part_bytes: PART_BYTES,
        }));
        let writer = FileWriter::new(settings, template, Arc::new(backend))?;
        let identity = AzureIdentity {
            store,
            container: config.container().to_string(),
            prefix: config.prefix.clone(),
        };
        Ok(Self {
            inner: WriterSink::new(writer, identity),
        })
    }

    /// The format blobs are written in.
    pub fn format(&self) -> FileFormat {
        self.inner.format()
    }
}

faucet_common_file::delegate_sink!(AzureBlobSink, inner);

/// What the Azure Blob sink supplies to the shared sink.
struct AzureIdentity {
    store: Arc<dyn ObjectStore>,
    container: String,
    prefix: String,
}

#[faucet_core::async_trait]
impl SinkIdentity for AzureIdentity {
    fn connector_name(&self) -> &'static str {
        "azure-blob"
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(AzureBlobSinkConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        format!("az://{}/{}", self.container, self.prefix)
    }

    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};
        let started = std::time::Instant::now();
        let listed = tokio::time::timeout(ctx.timeout, async {
            let mut listing = self.store.list(None);
            listing.next().await
        })
        .await;
        let probe = match listed {
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

/// Azure block uploads for blobs written in parts (#783).
struct AzureBlocks {
    store: Arc<dyn ObjectStore>,
    part_bytes: usize,
}

fn azure_err(what: &str, key: &str, e: impl std::fmt::Display) -> FaucetError {
    FaucetError::Sink(format!("azure {what} error for key '{key}': {e}"))
}

#[faucet_core::async_trait]
impl MultipartClient for AzureBlocks {
    fn part_size(&self) -> usize {
        self.part_bytes
    }

    async fn start(&self, key: &str) -> Result<Box<dyn MultipartUpload>, FaucetError> {
        let upload = self
            .store
            .put_multipart(&ObjectPath::from(key))
            .await
            .map_err(|e| azure_err("start multipart", key, e))?;
        Ok(Box::new(AzureUpload {
            key: key.to_string(),
            upload: Arc::new(Mutex::new(upload)),
        }))
    }
}

struct AzureUpload {
    key: String,
    upload: Arc<Mutex<Box<dyn object_store::MultipartUpload>>>,
}

impl MultipartUpload for AzureUpload {
    fn put_part(&self, _number: u32, body: Vec<u8>) -> BoxFuture<Result<(), FaucetError>> {
        let part = self
            .upload
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .put_part(bytes::Bytes::from(body).into());
        let key = self.key.clone();
        Box::pin(async move { part.await.map_err(|e| azure_err("put part", &key, e)) })
    }

    fn complete(self: Box<Self>) -> BoxFuture<Result<(), FaucetError>> {
        Box::pin(async move {
            let mut upload = Arc::try_unwrap(self.upload)
                .map_err(|_| azure_err("complete multipart", &self.key, "a part is still open"))?
                .into_inner()
                .unwrap_or_else(|p| p.into_inner());
            upload
                .complete()
                .await
                .map(|_| ())
                .map_err(|e| azure_err("complete multipart", &self.key, e))
        })
    }

    fn abort(self: Box<Self>) -> BoxFuture<Result<(), FaucetError>> {
        Box::pin(async move {
            let Ok(upload) = Arc::try_unwrap(self.upload) else {
                return Ok(());
            };
            let mut upload = upload.into_inner().unwrap_or_else(|p| p.into_inner());
            upload
                .abort()
                .await
                .map_err(|e| azure_err("abort multipart", &self.key, e))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(sink.format(), FileFormat::JsonLines);
        assert_eq!(sink.dataset_uri(), "az://cont/out/");
    }
}
