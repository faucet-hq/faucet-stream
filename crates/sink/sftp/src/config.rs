//! SFTP sink configuration.

use faucet_common_sftp::SftpConnectionConfig;
use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Configuration for the SFTP sink connector.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SftpSinkConfig {
    /// Shared SFTP connection settings (host, port, username, auth, host-key
    /// policy). Flattened, so its fields sit at the top level of the config.
    #[serde(flatten)]
    pub connection: SftpConnectionConfig,
    /// Remote directory prefix under which JSON Lines objects are written.
    pub path: String,
    /// File extension for written objects (default: `.jsonl`).
    #[serde(default = "default_file_extension")]
    pub file_extension: String,
    /// Records per written object. When a `write_batch` call hands the sink
    /// `N` records with `batch_size = M > 0`, the sink writes `ceil(N / M)`
    /// objects. `batch_size = 0` writes whatever upstream hands it as a single
    /// object. Defaults to [`DEFAULT_BATCH_SIZE`].
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Maximum records per file before rolling to a new one (#618). `None`
    /// removes the record cap; the sink accumulates across `write_batch`
    /// calls, so a small upstream page no longer means a small file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_records_per_file: Option<usize>,
    /// Maximum **bytes** per file before rolling to a new one (#618).
    ///
    /// Rows are a poor proxy for file size, so a rows-only cap either writes
    /// tiny files for narrow data or unbounded ones for wide data. This is
    /// also what bounds peak memory: the open file's body is buffered until it
    /// rolls. `None` (the default) removes the byte cap; a single record
    /// larger than the cap still gets its own file rather than being split.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes_per_file: Option<usize>,
}

fn default_file_extension() -> String {
    ".jsonl".to_string()
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

impl SftpSinkConfig {
    /// Build a sink config from a connection and a remote directory prefix.
    pub fn new(connection: SftpConnectionConfig, path: impl Into<String>) -> Self {
        Self {
            connection,
            path: path.into(),
            file_extension: default_file_extension(),
            batch_size: DEFAULT_BATCH_SIZE,
            max_records_per_file: None,
            max_bytes_per_file: None,
        }
    }

    /// Set the file extension for written objects.
    pub fn file_extension(mut self, ext: impl Into<String>) -> Self {
        self.file_extension = ext.into();
        self
    }

    /// Set the per-object record count.
    pub fn max_records_per_file(mut self, n: usize) -> Self {
        self.max_records_per_file = Some(n);
        self
    }

    /// Set the per-file byte cap (#618).
    pub fn max_bytes_per_file(mut self, n: usize) -> Self {
        self.max_bytes_per_file = Some(n);
        self
    }

    /// Set the per-call record chunk size.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_common_sftp::SftpConnectionConfig;

    fn conn() -> SftpConnectionConfig {
        SftpConnectionConfig::with_password("h", "u", "p")
    }

    #[test]
    fn defaults() {
        let cfg = SftpSinkConfig::new(conn(), "/out");
        assert_eq!(cfg.path, "/out");
        assert_eq!(cfg.file_extension, ".jsonl");
        assert_eq!(cfg.batch_size, DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn deserializes_flat_shape() {
        let json = r#"{
            "host": "sftp.example.com",
            "username": "user",
            "type": "password",
            "config": { "password": "secret" },
            "path": "/upload",
            "batch_size": 0
        }"#;
        let cfg: SftpSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.connection.host, "sftp.example.com");
        assert_eq!(cfg.path, "/upload");
        assert_eq!(cfg.batch_size, 0);
        assert_eq!(cfg.file_extension, ".jsonl");
    }

    #[test]
    fn batch_size_zero_is_valid_sentinel() {
        let cfg = SftpSinkConfig::new(conn(), "/o").with_batch_size(0);
        assert!(faucet_core::validate_batch_size(cfg.batch_size).is_ok());
    }
}
