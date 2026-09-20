//! Parquet source configuration.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Default Arrow `RecordBatch` size used when decoding Parquet row groups.
///
/// Aliases [`faucet_core::DEFAULT_BATCH_SIZE`] so the parquet source's
/// per-page size matches the rest of the streaming pipeline by default.
pub const DEFAULT_BATCH_SIZE: usize = faucet_core::DEFAULT_BATCH_SIZE;

/// Default parallel-file-read concurrency for glob / S3 prefix dispatch.
pub const DEFAULT_CONCURRENCY: usize = 4;

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

fn default_concurrency() -> usize {
    DEFAULT_CONCURRENCY
}

/// Configuration for the Parquet source connector.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ParquetSourceConfig {
    /// Where to read Parquet from — a local file, a local glob pattern, or S3.
    pub source: ParquetLocation,

    /// Arrow `RecordBatch` size used as the per-page hint when streaming.
    ///
    /// Passed verbatim to
    /// `ParquetRecordBatchStreamBuilder::with_batch_size`, which is itself
    /// only a hint — Arrow may emit smaller batches at row-group boundaries,
    /// so a single emitted [`faucet_core::StreamPage`] can hold fewer rows
    /// than this number. Larger values improve throughput at the cost of
    /// memory.
    ///
    /// `batch_size = 0` is the "no batching" sentinel: the call to
    /// `with_batch_size` is skipped and the underlying file's native
    /// row-group size drives the page cadence (one page per row-group).
    /// Useful for sinks like SQL `COPY` / BigQuery load jobs that prefer one
    /// large request to many small ones.
    ///
    /// Defaults to [`DEFAULT_BATCH_SIZE`].
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Optional column projection (column pruning).
    ///
    /// When set, only the named columns are decoded — non-projected columns
    /// are skipped entirely at the Parquet level, reducing I/O. Errors out
    /// if a name does not exist in the file's schema.
    #[serde(default)]
    pub columns: Option<Vec<String>>,

    /// Maximum number of files to read concurrently for `Glob` / S3 prefix
    /// modes. Ignored for single-file modes.
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
}

impl ParquetSourceConfig {
    /// Build a config from a `ParquetLocation` with sensible defaults.
    pub fn new(source: ParquetLocation) -> Self {
        Self {
            source,
            batch_size: DEFAULT_BATCH_SIZE,
            columns: None,
            concurrency: DEFAULT_CONCURRENCY,
        }
    }

    /// Convenience: build a config that reads one local file path.
    pub fn local(path: impl Into<String>) -> Self {
        Self::new(ParquetLocation::LocalPath { path: path.into() })
    }

    /// Convenience: build a config that reads files matching a local glob.
    pub fn glob(pattern: impl Into<String>) -> Self {
        Self::new(ParquetLocation::Glob {
            pattern: pattern.into(),
        })
    }

    /// Convenience: build a config that reads from an S3 location.
    pub fn s3(s3: ParquetS3Config) -> Self {
        Self::new(ParquetLocation::S3(s3))
    }

    /// Override the Arrow `RecordBatch` size.
    ///
    /// Pass `0` to opt out of batching — the underlying file's native
    /// row-group size drives the page cadence instead.
    pub fn batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Set the per-page row-count hint for
    /// [`Source::stream_pages`](faucet_core::Source::stream_pages).
    ///
    /// Alias for [`Self::batch_size`] — provided so the builder reads the
    /// same as every other faucet source.
    pub fn with_batch_size(self, batch_size: usize) -> Self {
        self.batch_size(batch_size)
    }

    /// Restrict decoding to the named columns.
    pub fn columns<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.columns = Some(columns.into_iter().map(Into::into).collect());
        self
    }

    /// Override the parallel-file-read concurrency.
    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }
}

/// Where to read Parquet data from.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ParquetLocation {
    /// A single local file path.
    LocalPath { path: String },

    /// A local glob pattern that expands to zero or more files. All matched
    /// files must share the same Arrow schema.
    Glob { pattern: String },

    /// An S3 location — either a single object key, or a prefix that
    /// expands to multiple objects.
    S3(ParquetS3Config),
}

/// S3 location parameters.
///
/// Exactly one of `key` or `prefix` must be set.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ParquetS3Config {
    /// S3 bucket name.
    pub bucket: String,

    /// A single object key (mutually exclusive with `prefix`).
    #[serde(default)]
    pub key: Option<String>,

    /// An object key prefix to list and read (mutually exclusive with `key`).
    #[serde(default)]
    pub prefix: Option<String>,

    /// AWS region. `None` uses the default-region resolution chain.
    #[serde(default)]
    pub region: Option<String>,

    /// Custom endpoint URL for S3-compatible services (MinIO, LocalStack, …).
    #[serde(default)]
    pub endpoint_url: Option<String>,
}

impl ParquetS3Config {
    /// New S3 config targeting a single object key.
    pub fn object(bucket: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            key: Some(key.into()),
            prefix: None,
            region: None,
            endpoint_url: None,
        }
    }

    /// New S3 config targeting all objects under a prefix.
    pub fn prefix(bucket: impl Into<String>, prefix: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            key: None,
            prefix: Some(prefix.into()),
            region: None,
            endpoint_url: None,
        }
    }

    /// Set the AWS region.
    pub fn region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Set a custom endpoint URL for S3-compatible services.
    pub fn endpoint_url(mut self, url: impl Into<String>) -> Self {
        self.endpoint_url = Some(url.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_sensible() {
        let cfg = ParquetSourceConfig::local("/tmp/data.parquet");
        assert_eq!(cfg.batch_size, DEFAULT_BATCH_SIZE);
        assert_eq!(cfg.concurrency, DEFAULT_CONCURRENCY);
        assert!(cfg.columns.is_none());
        assert!(matches!(cfg.source, ParquetLocation::LocalPath { .. }));
    }

    #[test]
    fn builder_methods_compose() {
        let cfg = ParquetSourceConfig::glob("/tmp/*.parquet")
            .batch_size(2048)
            .concurrency(8)
            .columns(["id", "name"]);
        assert_eq!(cfg.batch_size, 2048);
        assert_eq!(cfg.concurrency, 8);
        assert_eq!(
            cfg.columns.as_deref(),
            Some(&["id".to_string(), "name".to_string()][..])
        );
    }

    #[test]
    fn s3_object_and_prefix_variants() {
        let by_key = ParquetS3Config::object("my-bucket", "events/2024/data.parquet");
        assert_eq!(by_key.key.as_deref(), Some("events/2024/data.parquet"));
        assert!(by_key.prefix.is_none());

        let by_prefix = ParquetS3Config::prefix("my-bucket", "events/2024/")
            .region("us-east-1")
            .endpoint_url("http://localhost:9000");
        assert!(by_prefix.key.is_none());
        assert_eq!(by_prefix.prefix.as_deref(), Some("events/2024/"));
        assert_eq!(by_prefix.region.as_deref(), Some("us-east-1"));
        assert_eq!(
            by_prefix.endpoint_url.as_deref(),
            Some("http://localhost:9000")
        );
    }

    #[test]
    fn batch_size_default_via_serde() {
        let cfg: ParquetSourceConfig = serde_json::from_value(serde_json::json!({
            "source": { "type": "local_path", "path": "/tmp/x.parquet" }
        }))
        .unwrap();
        assert_eq!(cfg.batch_size, DEFAULT_BATCH_SIZE);
        assert_eq!(cfg.concurrency, DEFAULT_CONCURRENCY);
        assert!(cfg.columns.is_none());
    }

    #[test]
    fn schema_generates_without_panicking() {
        let _ = faucet_core::schema_for!(ParquetSourceConfig);
    }

    #[test]
    fn batch_size_defaults_to_faucet_core_default_batch_size() {
        let cfg = ParquetSourceConfig::local("/tmp/x.parquet");
        assert_eq!(cfg.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let cfg = ParquetSourceConfig::local("/tmp/x.parquet").with_batch_size(500);
        assert_eq!(cfg.batch_size, 500);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let cfg = ParquetSourceConfig::local("/tmp/x.parquet").with_batch_size(0);
        assert_eq!(cfg.batch_size, 0);
        assert!(faucet_core::validate_batch_size(cfg.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let cfg = ParquetSourceConfig::local("/tmp/x.parquet")
            .with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(cfg.batch_size).is_err());
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let json = r#"{
            "source": { "type": "local_path", "path": "/tmp/x.parquet" },
            "batch_size": 250
        }"#;
        let cfg: ParquetSourceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.batch_size, 250);
    }

    #[test]
    fn batch_size_zero_deserializes_from_json() {
        let json = r#"{
            "source": { "type": "local_path", "path": "/tmp/x.parquet" },
            "batch_size": 0
        }"#;
        let cfg: ParquetSourceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.batch_size, 0);
    }
}
