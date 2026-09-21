//! Stdout/stderr sink configuration.

use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Which standard stream to write records to.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum StdStream {
    /// Standard output (default). Honors shell redirection.
    #[default]
    Stdout,
    /// Standard error. Useful when stdout is reserved for piping pipeline output.
    Stderr,
}

/// How each record should be serialized before writing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StdoutFormat {
    /// One compact JSON object per line (default — matches JSONL format).
    #[default]
    JsonLines,
    /// Indented JSON, separated by newlines. Easier to read but not a single-line format.
    PrettyJson,
    /// Tab-separated values, with each record's keys sorted alphabetically.
    /// Scalars are emitted as-is; nested objects/arrays are emitted as compact JSON.
    Tsv,
    /// RFC-4180 comma-separated values, with each record's keys sorted
    /// alphabetically. Same column-resolution as [`StdoutFormat::Tsv`] (one
    /// line per record, keys sorted, no header row); values containing commas,
    /// quotes, or newlines are properly quoted. Nested objects/arrays are
    /// emitted as compact JSON.
    Csv,
}

/// Configuration for the stdout/stderr sink.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StdoutSinkConfig {
    /// Which standard stream to write to.
    #[serde(default)]
    pub destination: StdStream,
    /// Output format.
    #[serde(default)]
    pub format: StdoutFormat,
    /// Flush the underlying writer after every record instead of at batch boundaries.
    /// Tradeoff: lower latency for live preview, slightly lower throughput.
    #[serde(default)]
    pub flush_per_record: bool,
    /// Stop writing after this many records have been emitted. Subsequent
    /// `write_batch` calls become no-ops. `None` means unlimited.
    #[serde(default)]
    pub max_records: Option<usize>,
    /// Records per upstream [`StreamPage`](faucet_core::StreamPage). The
    /// stdout sink writes records to the chosen standard stream one at a time
    /// through a buffered writer, so this field has **no behavioural impact**
    /// at the sink — it is exposed purely for config parity across every sink
    /// in the workspace. Defaults to [`DEFAULT_BATCH_SIZE`].
    ///
    /// `batch_size = 0` (the "no batching" sentinel) and any positive value
    /// produce byte-for-byte identical output for this sink: each record is
    /// serialised and written individually regardless of how upstream chunked
    /// the page.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

impl Default for StdoutSinkConfig {
    fn default() -> Self {
        Self {
            destination: StdStream::default(),
            format: StdoutFormat::default(),
            flush_per_record: false,
            max_records: None,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }
}

impl StdoutSinkConfig {
    /// Create a new config with all defaults (stdout, JSON Lines, no limit).
    pub fn new() -> Self {
        Self::default()
    }

    /// Send records to the given standard stream.
    pub fn destination(mut self, destination: StdStream) -> Self {
        self.destination = destination;
        self
    }

    /// Choose the output format.
    pub fn format(mut self, format: StdoutFormat) -> Self {
        self.format = format;
        self
    }

    /// Flush after every record.
    pub fn flush_per_record(mut self, flush_per_record: bool) -> Self {
        self.flush_per_record = flush_per_record;
        self
    }

    /// Stop after writing `n` records total.
    pub fn max_records(mut self, max_records: usize) -> Self {
        self.max_records = Some(max_records);
        self
    }

    /// Set the per-page record count hint reported alongside other sink
    /// configs.
    ///
    /// This sink writes per-record through a buffered writer, so the value is
    /// observably a no-op: `0` (the "no batching" sentinel) and any positive
    /// value produce the same stdout/stderr output. Present for symmetry with
    /// sinks whose `batch_size` does drive I/O sizing (e.g. SQL multi-row
    /// inserts, BigQuery streaming inserts).
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults() {
        let c = StdoutSinkConfig::new();
        assert_eq!(c.destination, StdStream::Stdout);
        assert_eq!(c.format, StdoutFormat::JsonLines);
        assert!(!c.flush_per_record);
        assert!(c.max_records.is_none());
    }

    #[test]
    fn builder_chains() {
        let c = StdoutSinkConfig::new()
            .destination(StdStream::Stderr)
            .format(StdoutFormat::PrettyJson)
            .flush_per_record(true)
            .max_records(10);
        assert_eq!(c.destination, StdStream::Stderr);
        assert_eq!(c.format, StdoutFormat::PrettyJson);
        assert!(c.flush_per_record);
        assert_eq!(c.max_records, Some(10));
    }

    #[test]
    fn serde_round_trip() {
        let c = StdoutSinkConfig::new()
            .destination(StdStream::Stderr)
            .format(StdoutFormat::Tsv);
        let json = serde_json::to_string(&c).unwrap();
        let back: StdoutSinkConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.destination, StdStream::Stderr);
        assert_eq!(back.format, StdoutFormat::Tsv);
    }

    #[test]
    fn deserialize_from_minimal_json() {
        let c: StdoutSinkConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(c.destination, StdStream::Stdout);
        assert_eq!(c.format, StdoutFormat::JsonLines);
    }

    #[test]
    fn batch_size_defaults_to_default_batch_size() {
        let c = StdoutSinkConfig::new();
        assert_eq!(c.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let c = StdoutSinkConfig::new().with_batch_size(250);
        assert_eq!(c.batch_size, 250);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let c = StdoutSinkConfig::new().with_batch_size(0);
        assert_eq!(c.batch_size, 0);
        assert!(faucet_core::validate_batch_size(c.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let c = StdoutSinkConfig::new().with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(c.batch_size).is_err());
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let json = r#"{
            "destination": "stdout",
            "format": "json_lines",
            "batch_size": 500
        }"#;
        let c: StdoutSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(c.batch_size, 500);
    }

    #[test]
    fn batch_size_defaults_when_missing_in_json() {
        let c: StdoutSinkConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(c.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }
}
