//! SFTP sink executor: the shared [`FileSink`] over an [`SftpBackend`]
//! (RFC 0013, #886).
//!
//! Files are built locally and published by uploading to a hidden
//! temporary name and renaming it into place, so a consumer watching the
//! directory never sees a partial file. Construction is lazy —
//! [`SftpSink::new`] performs no I/O; the SSH transport is opened on the
//! first write and reused for the life of the sink.

use crate::config::SftpSinkConfig;
use faucet_common_file::write::{FileSink, SinkIdentity};
use faucet_common_sftp::SftpBackend;
use faucet_core::{FaucetError, FileFormat};
use serde_json::Value;
use std::sync::Arc;

/// A sink that writes records to files on an SFTP server in any format the
/// local file sink writes — JSON Lines, JSON, CSV, XML, Excel, Avro, Parquet
/// or raw text. Each file is built in a local scratch file (see
/// `scratch_dir`) before it is uploaded.
pub struct SftpSink {
    inner: FileSink<SftpBackend>,
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
        let backend = SftpBackend::builder(config.connection.clone())
            .concurrency(config.concurrency)
            .scratch_dir(config.scratch_dir.as_deref().map(std::path::Path::new))
            .build(base, &template)?;
        let identity = SftpIdentity {
            dataset_uri: format!(
                "sftp://{}:{}/{}",
                config.connection.host,
                config.connection.port,
                config.path.trim_start_matches('/')
            ),
        };
        Ok(Self {
            inner: FileSink::build(settings, template, Arc::new(backend), identity)?,
        })
    }

    /// The format files are written in.
    pub fn format(&self) -> FileFormat {
        self.inner.format()
    }
}

impl SftpSink {
    /// Join the overwrite run `token`
    /// ([`faucet_core::idempotency::OVERWRITE_RUN_KEY`]): every instance of
    /// one run (begin, writers, commit) stages in that run's own swap area,
    /// so a concurrent overwrite run of the same output neither drops nor
    /// moves in the other's files.
    pub fn with_overwrite_run(mut self, token: &str) -> Result<Self, FaucetError> {
        self.inner = self.inner.with_overwrite_run(token)?;
        Ok(self)
    }
}

faucet_common_file::delegate_sink!(SftpSink, inner);

/// What the SFTP sink supplies to the shared sink; requests, metering and
/// the preflight probe are the backend's.
struct SftpIdentity {
    dataset_uri: String,
}

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
mod tests;
