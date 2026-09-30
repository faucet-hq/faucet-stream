//! SFTP sink executor: the shared file writer (#777) over an SFTP backend.
//!
//! Files are built locally and published by uploading to a hidden temporary
//! name and renaming it into place, so a consumer watching the directory
//! never sees a partial file. Construction is lazy — [`SftpSink::new`]
//! performs no I/O; the SSH transport is opened on the first write and
//! reused for the life of the sink.

use crate::config::SftpSinkConfig;
use crate::object::SftpObjects;
use faucet_common_file::write::{FileWriter, RemoteBackend, SinkIdentity, WriterSink};
use faucet_core::{FaucetError, FileFormat};
use serde_json::Value;
use std::sync::Arc;

/// A sink that writes records to files on an SFTP server in any format the
/// local file sink writes — JSON Lines, JSON, CSV, XML, Excel, Avro, Parquet
/// or raw text. Each file is built in a local scratch file (see
/// `scratch_dir`) before it is uploaded.
pub struct SftpSink {
    inner: WriterSink,
}

impl SftpSink {
    /// Build the sink. Validates the config; opens no connection.
    pub fn new(config: SftpSinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let write = config.write_config();
        let settings = write.settings()?;
        let (base, template) = write.object_layout(&settings)?;
        let base = if config.path.starts_with('/') && !base.starts_with('/') {
            format!("/{base}")
        } else {
            base
        };
        let objects = Arc::new(SftpObjects::new(config.connection.clone()));
        let backend = RemoteBackend::new(
            objects,
            base,
            &template,
            config.scratch_dir.as_deref().map(std::path::Path::new),
        )?
        .with_upload_concurrency(config.concurrency);
        let writer = FileWriter::new(settings, template, Arc::new(backend))?;
        let identity = SftpIdentity {
            dataset_uri: format!(
                "sftp://{}:{}/{}",
                config.connection.host,
                config.connection.port,
                config.path.trim_start_matches('/')
            ),
        };
        Ok(Self {
            inner: WriterSink::new(writer, identity),
        })
    }

    /// The format files are written in.
    pub fn format(&self) -> FileFormat {
        self.inner.format()
    }
}

faucet_common_file::delegate_sink!(SftpSink, inner);

/// What the SFTP sink supplies to the shared sink.
struct SftpIdentity {
    dataset_uri: String,
}

#[faucet_core::async_trait]
impl SinkIdentity for SftpIdentity {
    fn connector_name(&self) -> &'static str {
        "sftp"
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(SftpSinkConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        self.dataset_uri.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_common_sftp::SftpConnectionConfig;
    use faucet_core::{Sink, WriteMode};

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
            .inner
            .writer()
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
                .inner
                .writer()
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
