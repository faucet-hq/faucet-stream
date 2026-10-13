//! Azure Blob sink executor: the shared [`FileSink`] over an
//! [`AzureBackend`] (RFC 0013, #886).

use std::sync::Arc;

use faucet_common_azure::{AzureBackend, build_store};
use faucet_common_file::write::{FileSink, SinkIdentity};
use faucet_core::{FaucetError, FileFormat};
use serde_json::Value;

use crate::config::AzureBlobSinkConfig;

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
    inner: FileSink<AzureBackend>,
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
        let backend = AzureBackend::builder(store, config.container())
            .concurrency(config.concurrency)
            .scratch_dir(config.scratch_dir.as_deref().map(std::path::Path::new))
            .build(base, &template)?;
        let identity = AzureIdentity {
            container: config.container().to_string(),
            prefix: config.prefix.clone(),
        };
        Ok(Self {
            inner: FileSink::build(settings, template, Arc::new(backend), identity)?,
        })
    }

    /// The format blobs are written in.
    pub fn format(&self) -> FileFormat {
        self.inner.format()
    }
}

faucet_common_file::delegate_sink!(AzureBlobSink, inner);

/// What the Azure Blob sink supplies to the shared sink; requests, metering
/// and the preflight probe are the backend's.
struct AzureIdentity {
    container: String,
    prefix: String,
}

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
        assert_eq!(sink.inner.backend().container(), "cont");
        assert!(sink.config_schema().is_object());
    }
}
