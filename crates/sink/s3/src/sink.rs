//! S3 sink executor: the shared file writer (#777) over an S3 backend.

use crate::config::S3SinkConfig;
use crate::object::{PART_BYTES, S3Objects};
use async_trait::async_trait;
use aws_sdk_s3::Client;
use faucet_common_file::write::{FileWriter, RemoteBackend, blocking, object_layout, run};
use faucet_core::{FaucetError, FileFormat, WriteMode};
use serde_json::Value;
use std::sync::Arc;

/// A sink that writes records to S3 objects in any format the local file
/// sink writes — JSON Lines, JSON, CSV, XML, Excel, Avro, Parquet or raw
/// text — through the shared file writer.
///
/// Each object is built in a local scratch file and published with one
/// upload (multipart past 8 MiB) when it closes: at the row / byte cap or at
/// `flush`. Uploads run in the background, up to `concurrency` at a time,
/// while the next object is encoded; `flush` waits for all of them, so a
/// bookmark never advances past an object that is not in the bucket.
pub struct S3Sink {
    config: S3SinkConfig,
    client: Client,
    writer: FileWriter,
    roundtrips: Arc<faucet_core::observability::RecorderSlot>,
    name: &'static str,
}

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
            FaucetError::Config(m) => FaucetError::Config(format!("S3 sink: {m}")),
            other => other,
        })?;
        let roundtrips = Arc::new(faucet_core::observability::RecorderSlot::new());
        let objects = Arc::new(S3Objects {
            client: client.clone(),
            bucket: config.bucket.clone(),
            concurrency: config.concurrency,
            part_bytes: PART_BYTES,
            roundtrips: roundtrips.clone(),
        });
        let backend = RemoteBackend::new(objects, base, &template.staging_name())?
            .with_upload_concurrency(config.concurrency);
        let writer = FileWriter::new(settings, template, Arc::new(backend))?;
        Ok(Self {
            config,
            client,
            writer,
            roundtrips,
            name: "s3",
        })
    }

    /// Report `name` as the connector (metrics, logs) instead of `s3` — for a
    /// deprecated kind built as this sink.
    pub fn with_connector_name(mut self, name: &'static str) -> Self {
        self.name = name;
        self
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
        let conf = aws_sdk_s3::config::Builder::from(&sdk_config)
            .force_path_style(config.force_path_style)
            .build();
        Ok(Client::from_conf(conf))
    }

    /// The format objects are written in.
    pub fn format(&self) -> FileFormat {
        self.writer.settings().format
    }

    fn overwriting(&self) -> bool {
        self.config.write_mode == faucet_common_file::write::FileWriteMode::Overwrite
    }
}

#[async_trait]
impl faucet_core::Sink for S3Sink {
    fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        self.writer.settings().batch_atomicity()
    }

    fn set_roundtrip_recorder(
        &self,
        recorder: std::sync::Arc<faucet_core::observability::RoundtripRecorder>,
    ) {
        self.roundtrips.install(recorder);
    }

    /// Publish the open object (#618): the pipeline calls `flush` at every
    /// bookmark-carrying page and at the end, so a bookmark never advances
    /// past records that are not in the bucket.
    async fn flush(&self) -> Result<(), FaucetError> {
        blocking(|| self.writer.flush())
    }

    fn connector_name(&self) -> &'static str {
        self.name
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(S3SinkConfig)).expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        format!("s3://{}/{}", self.config.bucket, self.config.prefix)
    }

    fn supported_write_modes(&self) -> &'static [WriteMode] {
        &[WriteMode::Append, WriteMode::Overwrite]
    }

    fn is_overwrite(&self) -> bool {
        self.overwriting()
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

    /// Preflight probe: confirm the configured bucket is reachable and the
    /// credentials work via a non-mutating `HeadBucket` call. Uploads nothing.
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};

        let started = std::time::Instant::now();
        let request = self.client.head_bucket().bucket(&self.config.bucket);
        let timeout = ctx.timeout;
        // Through the same runtime as every upload, so a pooled connection is
        // never driven by a runtime the writer later blocks.
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::Sink;

    #[tokio::test]
    async fn the_connector_name_and_path_style_are_configurable() {
        let config = S3SinkConfig::new("b")
            .region("us-east-1")
            .endpoint_url("http://minio:9000")
            .force_path_style(true);
        assert!(config.force_path_style);
        let client = S3Sink::build_client(&config).await.unwrap();
        let sink = S3Sink::with_client(config, client).unwrap();
        assert_eq!(sink.connector_name(), "s3");
        let sink = sink.with_connector_name("parquet");
        assert_eq!(sink.connector_name(), "parquet");
        assert_eq!(
            sink.writer
                .backend()
                .describe(faucet_common_file::write::Area::Destination, "k"),
            "s3://b/k"
        );
    }
}
