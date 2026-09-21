//! JSON Lines sink configuration.

use std::path::PathBuf;

use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Configuration for the JSON Lines file sink.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JsonlSinkConfig {
    /// Path to the output file.
    pub path: PathBuf,
    /// Whether to append to an existing file (default: false, truncates).
    #[serde(default)]
    pub append: bool,
    /// Whether to pretty-print each JSON record (default: false, compact).
    #[serde(default)]
    pub pretty: bool,
    /// Records per upstream [`StreamPage`](faucet_core::StreamPage). The JSONL
    /// sink writes records to disk one at a time through a buffered async
    /// writer, so this field has **no behavioural impact** at the sink — it is
    /// exposed purely for config parity across every sink in the workspace.
    /// Defaults to [`DEFAULT_BATCH_SIZE`].
    ///
    /// `batch_size = 0` (the "no batching" sentinel) and any positive value
    /// produce byte-for-byte identical output for this sink: each record is
    /// serialised and appended individually regardless of how upstream chunked
    /// the page.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Compression codec for the output file. Defaults to
    /// [`CompressionConfig::Auto`](faucet_core::CompressionConfig::Auto) —
    /// `.gz` / `.zst` suffix selects gzip / zstd, anything else writes
    /// uncompressed. Requires the crate-local `compression` feature.
    #[cfg(feature = "compression")]
    #[serde(default)]
    pub compression: faucet_core::CompressionConfig,
    /// Encryption at rest (#207): seal every record line with AES-256-GCM and
    /// write it base64-encoded (one sealed record per line, append-safe).
    /// The natural fit for a file-backed DLQ holding sensitive failed rows;
    /// `faucet dlq inspect/replay/discard` decrypt transparently given the
    /// key. Mutually exclusive with `compression`. Requires the crate-local
    /// `encryption` feature.
    #[cfg(feature = "encryption")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encryption: Option<faucet_core::EncryptionSpec>,
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

impl JsonlSinkConfig {
    /// Create a new config with the required file path and sensible defaults.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            append: false,
            pretty: false,
            batch_size: DEFAULT_BATCH_SIZE,
            #[cfg(feature = "compression")]
            compression: faucet_core::CompressionConfig::Auto,
            #[cfg(feature = "encryption")]
            encryption: None,
        }
    }

    /// Append to an existing file instead of truncating.
    pub fn append(mut self, append: bool) -> Self {
        self.append = append;
        self
    }

    /// Pretty-print each JSON record (one record still per logical entry,
    /// but with indentation). Note: this breaks strict JSONL format.
    pub fn pretty(mut self, pretty: bool) -> Self {
        self.pretty = pretty;
        self
    }

    /// Set the compression codec. Available only with the `compression` feature.
    #[cfg(feature = "compression")]
    pub fn compression(mut self, c: faucet_core::CompressionConfig) -> Self {
        self.compression = c;
        self
    }

    /// Seal every record line at rest (#207). Available only with the
    /// `encryption` feature; mutually exclusive with `compression`.
    #[cfg(feature = "encryption")]
    pub fn encryption(mut self, e: faucet_core::EncryptionSpec) -> Self {
        self.encryption = Some(e);
        self
    }

    /// Set the per-page record count hint reported alongside other sink
    /// configs.
    ///
    /// This sink writes per-record through a buffered writer, so the value is
    /// observably a no-op: `0` (the "no batching" sentinel) and any positive
    /// value produce the same on-disk output. Present for symmetry with sinks
    /// whose `batch_size` does drive I/O sizing (e.g. SQL multi-row inserts,
    /// BigQuery streaming inserts).
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = JsonlSinkConfig::new("/tmp/out.jsonl");
        assert_eq!(config.path, PathBuf::from("/tmp/out.jsonl"));
        assert!(!config.append);
        assert!(!config.pretty);
    }

    #[test]
    fn builder_methods() {
        let config = JsonlSinkConfig::new("/tmp/out.jsonl")
            .append(true)
            .pretty(true);
        assert!(config.append);
        assert!(config.pretty);
    }

    #[test]
    fn batch_size_defaults_to_default_batch_size() {
        let config = JsonlSinkConfig::new("/tmp/out.jsonl");
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let config = JsonlSinkConfig::new("/tmp/out.jsonl").with_batch_size(250);
        assert_eq!(config.batch_size, 250);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config = JsonlSinkConfig::new("/tmp/out.jsonl").with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let config =
            JsonlSinkConfig::new("/tmp/out.jsonl").with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_err());
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let json = r#"{
            "path": "/tmp/out.jsonl",
            "append": false,
            "pretty": false,
            "batch_size": 500
        }"#;
        let config: JsonlSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, 500);
    }

    #[test]
    fn batch_size_defaults_when_missing_in_json() {
        let json = r#"{"path": "/tmp/out.jsonl"}"#;
        let config: JsonlSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }
}
