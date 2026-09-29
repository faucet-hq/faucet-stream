//! GCS sink executor: the shared file writer (#777) over a GCS backend.

use crate::config::GcsSinkConfig;
use crate::object::GcsObjects;
use async_trait::async_trait;
use faucet_common_file::write::{FileWriter, RemoteBackend, blocking, object_layout, run};
use faucet_common_gcs::{build_storage, build_storage_control};
use faucet_core::{FaucetError, FileFormat, WriteMode};
use google_cloud_storage::client::StorageControl;
use serde_json::Value;
use std::sync::Arc;

/// A sink that writes records to GCS objects in any format the local file
/// sink writes — JSON Lines, JSON, CSV, XML, Excel, Avro, Parquet or raw
/// text — through the shared file writer.
///
/// Each object is built in a local scratch file and published with one
/// upload (resumable past the client's threshold) when it closes: at the row
/// / byte cap or at `flush`. Uploads run in the background, up to
/// `concurrency` at a time, while the next object is encoded; `flush` waits
/// for all of them, so a bookmark never advances past an object that is not
/// in the bucket.
pub struct GcsSink {
    config: GcsSinkConfig,
    control: StorageControl,
    writer: FileWriter,
    roundtrips: Arc<faucet_core::observability::RecorderSlot>,
}

impl GcsSink {
    /// Create a new GCS sink, building the clients eagerly.
    pub async fn new(config: GcsSinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let storage = build_storage(&config.auth, config.storage_host.as_deref()).await?;
        let control = build_storage_control(&config.auth, config.storage_host.as_deref()).await?;
        let settings = config.settings()?;
        let (base, template) = object_layout(
            &config.prefix,
            config.path.as_deref(),
            &config.file_extension,
            settings.format,
            settings.codec,
            settings.rolls_over(),
        )
        .map_err(|e| match e {
            FaucetError::Config(m) => FaucetError::Config(format!("GCS sink: {m}")),
            other => other,
        })?;
        let roundtrips = Arc::new(faucet_core::observability::RecorderSlot::new());
        let objects = Arc::new(GcsObjects {
            storage,
            control: control.clone(),
            bucket: config.bucket.clone(),
            roundtrips: roundtrips.clone(),
        });
        let backend = RemoteBackend::new(objects, base, &template.staging_name())?
            .with_upload_concurrency(config.concurrency);
        let writer = FileWriter::new(settings, template, Arc::new(backend))?;
        Ok(Self {
            config,
            control,
            writer,
            roundtrips,
        })
    }

    /// The format objects are written in.
    pub fn format(&self) -> FileFormat {
        self.writer.settings().format
    }

    fn bucket_path(&self) -> String {
        format!("projects/_/buckets/{}", self.config.bucket)
    }
}

#[async_trait]
impl faucet_core::Sink for GcsSink {
    fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        self.writer.settings().batch_atomicity()
    }

    fn set_roundtrip_recorder(
        &self,
        recorder: std::sync::Arc<faucet_core::observability::RoundtripRecorder>,
    ) {
        self.roundtrips.install(recorder);
    }

    fn dataset_uri(&self) -> String {
        format!("gs://{}/{}", self.config.bucket, self.config.prefix)
    }

    /// Publish the open object (#618) before the bookmark advances.
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

    /// Arrow `RecordBatch`es go straight into a Parquet object; other formats
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
        serde_json::to_value(faucet_core::schema_for!(GcsSinkConfig)).expect("schema serialization")
    }

    fn connector_name(&self) -> &'static str {
        "gcs"
    }

    /// Preflight probe: a non-mutating `list_objects` call capped at one
    /// result confirms the bucket is reachable and the credentials work.
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};
        let started = std::time::Instant::now();
        let request = self
            .control
            .list_objects()
            .set_parent(self.bucket_path())
            .set_page_size(1_i32);
        let timeout = ctx.timeout;
        let probe = blocking(|| {
            run(async move {
                Ok(match tokio::time::timeout(timeout, request.send()).await {
                    Ok(Ok(_)) => Probe::pass("auth", started.elapsed()),
                    Ok(Err(e)) => Probe::fail_hint(
                        "auth",
                        started.elapsed(),
                        e.to_string(),
                        "check bucket name, credentials, and network",
                    ),
                    Err(_) => Probe::fail("network", started.elapsed(), "timed out"),
                })
            })
        })?;
        Ok(CheckReport::single(probe))
    }
}
