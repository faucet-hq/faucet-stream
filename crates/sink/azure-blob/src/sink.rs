//! Azure Blob sink executor: the shared file writer (#777) over an Azure
//! Blob backend.

use std::sync::Arc;

use async_trait::async_trait;
use faucet_common_azure::build_store;
use faucet_common_file::write::{FileWriter, RemoteBackend, blocking, object_layout, run};
use faucet_core::{FaucetError, FileFormat, WriteMode};
use futures::stream::StreamExt;
use object_store::ObjectStore;
use serde_json::Value;

use crate::config::AzureBlobSinkConfig;
use crate::object::{AzureObjects, PART_BYTES};

/// A sink that writes records to Azure blobs in any format the local file
/// sink writes — JSON Lines, JSON, CSV, XML, Excel, Avro, Parquet or raw
/// text — through the shared file writer.
///
/// Each blob is built in a local scratch file and published with one upload
/// (a committed block list past 8 MiB) when it closes: at the row / byte cap
/// or at `flush`. Uploads run in the background, up to `concurrency` at a
/// time, while the next blob is encoded; `flush` waits for all of them, so a
/// bookmark never advances past a blob that is not in the container.
pub struct AzureBlobSink {
    config: AzureBlobSinkConfig,
    store: Arc<dyn ObjectStore>,
    writer: FileWriter,
}

impl AzureBlobSink {
    /// Construct the sink, building the object store eagerly so it is reused
    /// across calls.
    pub async fn new(config: AzureBlobSinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let store = build_store(&config.connection)?;
        let settings = config.settings()?;
        let (base, template) = object_layout(
            &config.prefix,
            config.path.as_deref(),
            &config.file_extension,
            settings.format,
            settings.codec,
            settings.rolls_over(),
        )
        .map_err(|e| faucet_common_file::config_context("azure-blob sink", e))?;
        let objects = Arc::new(AzureObjects {
            store: store.clone(),
            container: config.container().to_string(),
            part_bytes: PART_BYTES,
            concurrency: config.concurrency,
        });
        let backend = RemoteBackend::new(objects, base, &template.staging_name())?
            .with_upload_concurrency(config.concurrency);
        let writer = FileWriter::new(settings, template, Arc::new(backend))?;
        Ok(Self {
            config,
            store,
            writer,
        })
    }

    /// The format blobs are written in.
    pub fn format(&self) -> FileFormat {
        self.writer.settings().format
    }
}

#[async_trait]
impl faucet_core::Sink for AzureBlobSink {
    fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        self.writer.settings().batch_atomicity()
    }

    fn dataset_uri(&self) -> String {
        format!("az://{}/{}", self.config.container(), self.config.prefix)
    }

    /// Publish the open blob (#618) before the bookmark advances.
    async fn flush(&self) -> Result<(), FaucetError> {
        blocking(|| self.writer.flush())
    }

    fn supported_write_modes(&self) -> &'static [WriteMode] {
        &[WriteMode::Append, WriteMode::Overwrite]
    }

    fn is_overwrite(&self) -> bool {
        self.config.write_mode == faucet_common_file::write::FileWriteMode::Overwrite
    }

    async fn begin_overwrite(&self) -> Result<(), FaucetError> {
        blocking(|| self.writer.begin_overwrite())
    }

    async fn commit_overwrite(&self) -> Result<(), FaucetError> {
        blocking(|| self.writer.commit_overwrite())
    }

    async fn abort_overwrite(&self) -> Result<(), FaucetError> {
        blocking(|| self.writer.abort_overwrite())
    }

    async fn complete_run(&self) -> Result<(), FaucetError> {
        blocking(|| self.writer.complete())
    }

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        blocking(|| self.writer.write_rows(records))
    }

    /// Arrow `RecordBatch`es go straight into a Parquet blob; other formats
    /// take the row path.
    #[cfg(feature = "arrow")]
    fn supports_columnar(&self) -> bool {
        self.format() == FileFormat::Parquet
    }

    #[cfg(feature = "arrow")]
    async fn write_batch_columnar(
        &self,
        batch: &arrow::array::RecordBatch,
    ) -> Result<usize, FaucetError> {
        if batch.num_rows() == 0 {
            return Ok(0);
        }
        if self.format() == FileFormat::Parquet {
            return blocking(|| self.writer.write_batch(batch));
        }
        let rows = faucet_core::columnar::record_batch_to_values(batch)?;
        self.write_batch(&rows).await
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(AzureBlobSinkConfig))
            .expect("schema serialization")
    }

    fn connector_name(&self) -> &'static str {
        "azure-blob"
    }

    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};

        let started = std::time::Instant::now();
        let store = self.store.clone();
        let timeout = ctx.timeout;
        let probe = blocking(|| {
            run(async move {
                let listed = tokio::time::timeout(timeout, async {
                    let mut listing = store.list(None);
                    listing.next().await
                })
                .await;
                Ok(match listed {
                    Ok(None) | Ok(Some(Ok(_))) => Probe::pass("auth", started.elapsed()),
                    Ok(Some(Err(e))) => Probe::fail_hint(
                        "auth",
                        started.elapsed(),
                        e.to_string(),
                        "check account, container, credentials, and network",
                    ),
                    Err(_) => Probe::fail("network", started.elapsed(), "timed out"),
                })
            })
        })?;
        Ok(CheckReport::single(probe))
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
        assert_eq!(sink.dataset_uri(), "az://cont/out/");
    }
}
