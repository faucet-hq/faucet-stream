//! SFTP sink executor: the shared file writer (#777) over an SFTP backend.
//!
//! Files are built locally and published by uploading to a hidden temporary
//! name and renaming it into place, so a consumer watching the directory
//! never sees a partial file. Construction is lazy — [`SftpSink::new`]
//! performs no I/O; the SSH transport is opened on the first write and
//! reused for the life of the sink.

use crate::config::SftpSinkConfig;
use crate::object::SftpObjects;
use async_trait::async_trait;
use faucet_common_file::write::{FileWriter, RemoteBackend, blocking, object_layout};
use faucet_core::{FaucetError, FileFormat, WriteMode};
use serde_json::Value;
use std::sync::Arc;

/// A sink that writes records to files on an SFTP server in any format the
/// local file sink writes — JSON Lines, JSON, CSV, XML, Excel, Avro, Parquet
/// or raw text.
pub struct SftpSink {
    config: SftpSinkConfig,
    writer: FileWriter,
}

impl SftpSink {
    /// Build the sink. Validates the config; opens no connection.
    pub fn new(config: SftpSinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let settings = config.settings()?;
        let (base, template) = object_layout(
            &config.dir_prefix(),
            config.file_name.as_deref(),
            &config.file_extension,
            settings.format,
            settings.codec,
            settings.rolls_over(),
        )
        .map_err(|e| faucet_common_file::config_context("SFTP sink", e))?;
        let base = if config.path.starts_with('/') && !base.starts_with('/') {
            format!("/{base}")
        } else {
            base
        };
        let objects = Arc::new(SftpObjects::new(config.connection.clone()));
        let backend = RemoteBackend::new(objects, base, &template.staging_name())?
            .with_upload_concurrency(config.concurrency);
        let writer = FileWriter::new(settings, template, Arc::new(backend))?;
        Ok(Self { config, writer })
    }

    /// The format files are written in.
    pub fn format(&self) -> FileFormat {
        self.writer.settings().format
    }
}

#[async_trait]
impl faucet_core::Sink for SftpSink {
    fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        self.writer.settings().batch_atomicity()
    }

    /// Publish the open file before the bookmark advances.
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

    /// Arrow `RecordBatch`es go straight into a Parquet file; other formats
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
        serde_json::to_value(faucet_core::schema_for!(SftpSinkConfig))
            .expect("schema serialization")
    }

    fn connector_name(&self) -> &'static str {
        "sftp"
    }

    fn dataset_uri(&self) -> String {
        format!(
            "sftp://{}:{}/{}",
            self.config.connection.host,
            self.config.connection.port,
            self.config.path.trim_start_matches('/')
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_common_sftp::SftpConnectionConfig;
    use faucet_core::Sink;

    fn cfg() -> SftpSinkConfig {
        SftpSinkConfig::new(SftpConnectionConfig::with_password("h", "u", "p"), "/out")
    }

    #[test]
    fn new_is_lazy_and_validates_batch_size() {
        assert!(SftpSink::new(cfg()).is_ok());
        let bad = cfg().with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(matches!(SftpSink::new(bad), Err(FaucetError::Config(_))));
    }

    #[test]
    fn files_land_under_the_directory() {
        let sink = SftpSink::new(cfg()).unwrap();
        let key = sink
            .writer
            .backend()
            .describe(faucet_common_file::write::Area::Destination, "f.jsonl");
        assert_eq!(key, "sftp://h:22/out/f.jsonl");
        let mut c = cfg();
        c.file_name = Some("dt/part-{part}.csv".into());
        c.format = crate::config::SftpSinkFormat::Auto;
        #[cfg(feature = "file-format-csv")]
        {
            let sink = SftpSink::new(c).unwrap();
            assert_eq!(sink.format(), FileFormat::Csv);
            let d = sink
                .writer
                .backend()
                .describe(faucet_common_file::write::Area::Destination, "x");
            assert_eq!(d, "sftp://h:22/out/dt/x");
        }
        #[cfg(not(feature = "file-format-csv"))]
        assert!(SftpSink::new(c).is_err());
    }

    #[test]
    fn connector_name_is_sftp() {
        let sink = SftpSink::new(cfg()).unwrap();
        assert_eq!(sink.connector_name(), "sftp");
    }

    #[test]
    fn dataset_uri_has_no_credentials() {
        let sink = SftpSink::new(cfg()).unwrap();
        assert_eq!(sink.dataset_uri(), "sftp://h:22/out");
    }

    #[test]
    fn write_modes_and_capabilities() {
        let sink = SftpSink::new(cfg()).unwrap();
        assert!(!sink.supports_idempotent_writes());
        assert!(!sink.dedups_by_key());
        assert!(sink.supported_write_modes().contains(&WriteMode::Overwrite));
        assert!(!sink.is_overwrite());
    }
}
