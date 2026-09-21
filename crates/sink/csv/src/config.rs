//! CSV sink configuration.

use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Configuration for the CSV file sink.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CsvSinkConfig {
    /// Path to the output CSV file.
    pub path: String,
    /// Field delimiter byte. Defaults to `b','`.
    #[serde(default = "default_delimiter")]
    pub delimiter: u8,
    /// Whether to write a header row. Defaults to `true`.
    #[serde(default = "default_true")]
    pub write_headers: bool,
    /// Whether to append to an existing file. Defaults to `false` (truncates).
    #[serde(default)]
    pub append: bool,
    /// Records per upstream [`StreamPage`](faucet_core::StreamPage). The CSV
    /// sink writes rows to disk one at a time inside a `spawn_blocking` task
    /// fronted by a buffered `csv::Writer`, so this field has **no
    /// behavioural impact** at the sink — it is exposed purely for config
    /// parity across every sink in the workspace. Defaults to
    /// [`DEFAULT_BATCH_SIZE`].
    ///
    /// `batch_size = 0` (the "no batching" sentinel) and any positive value
    /// produce byte-for-byte identical output for this sink: each record is
    /// serialised and written individually regardless of how upstream chunked
    /// the page.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// What to do when a record contains a field that is not part of the
    /// frozen column set (the CSV header is fixed from the first batch and
    /// cannot be rewritten without rewriting the whole file). A field that
    /// first appears in a *later* batch — or in a later record once the header
    /// is already written — cannot be added to the header, so its value is
    /// dropped from the output. Defaults to [`OnUnknownField::Warn`].
    #[serde(default)]
    pub on_unknown_field: OnUnknownField,
    /// Compression codec for the output file. Defaults to
    /// [`CompressionConfig::Auto`](faucet_core::CompressionConfig::Auto) —
    /// `.gz` / `.zst` suffix selects gzip / zstd. Requires the crate-local
    /// `compression` feature.
    #[cfg(feature = "compression")]
    #[serde(default)]
    pub compression: faucet_core::CompressionConfig,
}

/// Policy for a record key that is not in the sink's frozen column set.
///
/// The CSV header is fixed from the first non-empty batch and cannot be
/// rewritten in place. A field that first appears after the header is written
/// (in a later batch, or a later record of the first batch once the header is
/// determined) therefore cannot become a column, so its value would be lost.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OnUnknownField {
    /// Emit a one-shot `tracing::warn!` naming the dropped field(s) and keep
    /// writing — the unknown field's value is dropped from the output. This is
    /// the default: it preserves the historical streaming/append behaviour
    /// while making the column-level loss visible instead of silent.
    #[default]
    Warn,
    /// Abort the write with a [`FaucetError::Sink`](faucet_core::FaucetError::Sink)
    /// the first time any record carries a field outside the frozen column set.
    /// Opt into this when silent column-level data loss is unacceptable.
    Error,
}

fn default_delimiter() -> u8 {
    b','
}

fn default_true() -> bool {
    true
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

impl CsvSinkConfig {
    /// Create a new config with the required file path and sensible defaults.
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            delimiter: b',',
            write_headers: true,
            append: false,
            batch_size: DEFAULT_BATCH_SIZE,
            on_unknown_field: OnUnknownField::Warn,
            #[cfg(feature = "compression")]
            compression: faucet_core::CompressionConfig::Auto,
        }
    }

    /// Set the field delimiter byte.
    pub fn delimiter(mut self, d: u8) -> Self {
        self.delimiter = d;
        self
    }

    /// Set whether to write a header row.
    pub fn write_headers(mut self, v: bool) -> Self {
        self.write_headers = v;
        self
    }

    /// Set whether to append to an existing file.
    pub fn append(mut self, v: bool) -> Self {
        self.append = v;
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

    /// Set the policy for record keys outside the frozen column set.
    pub fn on_unknown_field(mut self, policy: OnUnknownField) -> Self {
        self.on_unknown_field = policy;
        self
    }

    /// Set the compression codec. Available only with the `compression` feature.
    #[cfg(feature = "compression")]
    pub fn compression(mut self, c: faucet_core::CompressionConfig) -> Self {
        self.compression = c;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = CsvSinkConfig::new("/tmp/out.csv");
        assert_eq!(config.path, "/tmp/out.csv");
        assert_eq!(config.delimiter, b',');
        assert!(config.write_headers);
        assert!(!config.append);
    }

    #[test]
    fn builder_methods() {
        let config = CsvSinkConfig::new("/tmp/out.tsv")
            .delimiter(b'\t')
            .write_headers(false)
            .append(true);
        assert_eq!(config.delimiter, b'\t');
        assert!(!config.write_headers);
        assert!(config.append);
    }

    #[test]
    fn batch_size_defaults_to_default_batch_size() {
        let config = CsvSinkConfig::new("/tmp/out.csv");
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let config = CsvSinkConfig::new("/tmp/out.csv").with_batch_size(250);
        assert_eq!(config.batch_size, 250);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config = CsvSinkConfig::new("/tmp/out.csv").with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let config =
            CsvSinkConfig::new("/tmp/out.csv").with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_err());
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let json = r#"{
            "path": "/tmp/out.csv",
            "delimiter": 44,
            "write_headers": true,
            "append": false,
            "batch_size": 500
        }"#;
        let config: CsvSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, 500);
    }

    #[test]
    fn batch_size_defaults_when_missing_in_json() {
        let json = r#"{"path": "/tmp/out.csv"}"#;
        let config: CsvSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn on_unknown_field_defaults_to_warn() {
        let config = CsvSinkConfig::new("/tmp/out.csv");
        assert_eq!(config.on_unknown_field, OnUnknownField::Warn);
        let json = r#"{"path": "/tmp/out.csv"}"#;
        let config: CsvSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.on_unknown_field, OnUnknownField::Warn);
    }

    #[test]
    fn on_unknown_field_deserializes_snake_case() {
        let json = r#"{"path": "/tmp/out.csv", "on_unknown_field": "error"}"#;
        let config: CsvSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.on_unknown_field, OnUnknownField::Error);
    }

    #[test]
    fn on_unknown_field_builder_sets_policy() {
        let config = CsvSinkConfig::new("/tmp/out.csv").on_unknown_field(OnUnknownField::Error);
        assert_eq!(config.on_unknown_field, OnUnknownField::Error);
    }
}
