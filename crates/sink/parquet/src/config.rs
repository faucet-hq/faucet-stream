//! Parquet sink configuration.

use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Default sample size used when schema is inferred.
pub const DEFAULT_SAMPLE_SIZE: usize = 100;

/// Default row group size (matches parquet's `DEFAULT_MAX_ROW_GROUP_ROW_COUNT`).
pub const DEFAULT_ROW_GROUP_SIZE: usize = 1024 * 1024;

/// Configuration for the Parquet sink connector.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ParquetSinkConfig {
    /// Where to write the Parquet files (local filesystem or S3).
    pub destination: ParquetDestination,

    /// How the Arrow schema is determined. `None` means infer from the first
    /// batch using `DEFAULT_SAMPLE_SIZE` records.
    #[serde(default)]
    pub schema: Option<SchemaSource>,

    /// Compression codec for column data.
    #[serde(default)]
    pub compression: ParquetCompression,

    /// Maximum rows per Parquet row group. Defaults to `DEFAULT_ROW_GROUP_SIZE`.
    #[serde(default = "default_row_group_size")]
    pub row_group_size: usize,

    /// Roll over to a new file once the current writer has accepted this many
    /// rows. `None` writes everything to a single file (until `flush()`).
    #[serde(default)]
    pub max_rows_per_file: Option<usize>,

    /// Roll over to a new file once the writer's `bytes_written()` exceeds this
    /// threshold. Compared after each batch — so the actual file size may
    /// slightly exceed the limit by one batch worth of data.
    #[serde(default)]
    pub max_bytes_per_file: Option<usize>,

    /// Re-chunk size for [`Sink::write_batch`](faucet_core::Sink::write_batch).
    /// When `batch_size > 0` and an incoming page exceeds this many records,
    /// the sink slices the page into `batch_size`-sized chunks and runs the
    /// internal write path once per chunk. When `batch_size = 0` (the "no
    /// batching" sentinel) the page is written through as-is. Defaults to
    /// [`DEFAULT_BATCH_SIZE`].
    ///
    /// This is purely a knob for downstream callers that want tighter control
    /// than the upstream source provides; for Parquet/S3 the source-defined
    /// page size is usually optimal, so leaving this at the default (or even
    /// at `0`) is recommended. The independent row/byte rollover thresholds
    /// (`max_rows_per_file`, `max_bytes_per_file`) still apply regardless of
    /// `batch_size`.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_row_group_size() -> usize {
    DEFAULT_ROW_GROUP_SIZE
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

impl ParquetSinkConfig {
    /// Create a new config with sensible defaults for the given destination.
    pub fn new(destination: ParquetDestination) -> Self {
        Self {
            destination,
            schema: None,
            compression: ParquetCompression::default(),
            row_group_size: DEFAULT_ROW_GROUP_SIZE,
            max_rows_per_file: None,
            max_bytes_per_file: None,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    /// Convenience: a local-path destination.
    pub fn local(path: impl Into<String>) -> Self {
        Self::new(ParquetDestination::LocalPath { path: path.into() })
    }

    /// Override the schema source.
    pub fn schema(mut self, schema: SchemaSource) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Override the compression codec.
    pub fn compression(mut self, compression: ParquetCompression) -> Self {
        self.compression = compression;
        self
    }

    /// Override the row group size.
    pub fn row_group_size(mut self, rows: usize) -> Self {
        self.row_group_size = rows;
        self
    }

    /// Roll files over once they reach `rows` rows.
    pub fn max_rows_per_file(mut self, rows: usize) -> Self {
        self.max_rows_per_file = Some(rows);
        self
    }

    /// Roll files over once they reach `bytes` bytes (measured after each batch).
    pub fn max_bytes_per_file(mut self, bytes: usize) -> Self {
        self.max_bytes_per_file = Some(bytes);
        self
    }

    /// Set the re-chunk size for [`Sink::write_batch`](faucet_core::Sink::write_batch).
    ///
    /// Pass `0` to opt out of batching — incoming pages are written through
    /// to the underlying Arrow writer as-is. For Parquet/S3 the source-defined
    /// page size is usually optimal, so leaving this at the default (or at
    /// `0`) is recommended.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Validate that the config makes sense; returns an error if not.
    pub fn validate(&self) -> Result<(), String> {
        if self.row_group_size == 0 {
            return Err("row_group_size must be greater than 0".to_string());
        }
        if matches!(self.max_rows_per_file, Some(0)) {
            return Err("max_rows_per_file must be greater than 0 when set".to_string());
        }
        if matches!(self.max_bytes_per_file, Some(0)) {
            return Err("max_bytes_per_file must be greater than 0 when set".to_string());
        }
        if let Some(SchemaSource::Inferred { sample_size }) = self.schema
            && sample_size == 0
        {
            return Err("schema.sample_size must be greater than 0 when set".to_string());
        }
        faucet_core::validate_batch_size(self.batch_size).map_err(|e| e.to_string())?;
        match &self.destination {
            ParquetDestination::LocalPath { path } if path.is_empty() => {
                Err("destination.path must not be empty".to_string())
            }
            ParquetDestination::S3(s3) if s3.bucket.is_empty() => {
                Err("destination.bucket must not be empty".to_string())
            }
            _ => Ok(()),
        }
    }

    /// Resolve the effective sample size for inference.
    pub fn effective_sample_size(&self) -> usize {
        match &self.schema {
            Some(SchemaSource::Inferred { sample_size }) => *sample_size,
            _ => DEFAULT_SAMPLE_SIZE,
        }
    }
}

/// Where the sink writes Parquet files.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ParquetDestination {
    /// Local filesystem. `path` is either a file path ending in `.parquet`
    /// (single-file mode — only valid without rollover) or a directory; in
    /// directory mode each rolled-over file is named `<uuid>.parquet`.
    LocalPath { path: String },
    /// S3 bucket. Each written object is `<prefix><uuid>.parquet`.
    S3(ParquetS3Destination),
}

/// S3 destination configuration.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ParquetS3Destination {
    /// S3 bucket name.
    pub bucket: String,
    /// Key prefix for written objects. Empty string writes to the bucket root.
    #[serde(default)]
    pub prefix: String,
    /// AWS region. `None` uses the SDK default.
    #[serde(default)]
    pub region: Option<String>,
    /// Custom endpoint URL for S3-compatible services (e.g. MinIO, LocalStack).
    #[serde(default)]
    pub endpoint_url: Option<String>,
    /// Allow non-HTTPS endpoints. Required when `endpoint_url` is an `http://`
    /// URL (e.g. for LocalStack).
    #[serde(default)]
    pub allow_http: bool,
}

/// How the sink obtains its Arrow schema.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SchemaSource {
    /// Infer from the first batch using up to `sample_size` records.
    Inferred { sample_size: usize },
    /// Use an explicit Arrow schema. Reserved for a future revision.
    Explicit {},
}

/// Parquet compression codec.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ParquetCompression {
    Uncompressed,
    #[default]
    Snappy,
    Gzip,
    Zstd,
    Lz4,
}

impl ParquetCompression {
    /// Map to the parquet crate's compression enum.
    ///
    /// We pick conservative compression levels (defaults) because every
    /// percent of CPU we spend compressing is a percent the pipeline loses on
    /// throughput; users wanting maximum compression can post-process.
    pub fn as_parquet(&self) -> parquet::basic::Compression {
        use parquet::basic::{Compression as PC, GzipLevel, ZstdLevel};
        match self {
            ParquetCompression::Uncompressed => PC::UNCOMPRESSED,
            ParquetCompression::Snappy => PC::SNAPPY,
            ParquetCompression::Gzip => PC::GZIP(GzipLevel::default()),
            ParquetCompression::Zstd => PC::ZSTD(ZstdLevel::default()),
            ParquetCompression::Lz4 => PC::LZ4_RAW,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_helper_builds_default_config() {
        let cfg = ParquetSinkConfig::local("/tmp/out.parquet");
        assert!(matches!(
            cfg.destination,
            ParquetDestination::LocalPath { ref path } if path == "/tmp/out.parquet"
        ));
        assert_eq!(cfg.compression, ParquetCompression::Snappy);
        assert_eq!(cfg.row_group_size, DEFAULT_ROW_GROUP_SIZE);
        assert!(cfg.schema.is_none());
        assert!(cfg.max_rows_per_file.is_none());
        assert!(cfg.max_bytes_per_file.is_none());
    }

    #[test]
    fn builder_overrides_apply() {
        let cfg = ParquetSinkConfig::local("/tmp/out")
            .compression(ParquetCompression::Zstd)
            .row_group_size(5000)
            .max_rows_per_file(1000)
            .max_bytes_per_file(1_000_000)
            .schema(SchemaSource::Inferred { sample_size: 25 });

        assert_eq!(cfg.compression, ParquetCompression::Zstd);
        assert_eq!(cfg.row_group_size, 5000);
        assert_eq!(cfg.max_rows_per_file, Some(1000));
        assert_eq!(cfg.max_bytes_per_file, Some(1_000_000));
        assert_eq!(cfg.effective_sample_size(), 25);
    }

    #[test]
    fn effective_sample_size_defaults_when_unset() {
        let cfg = ParquetSinkConfig::local("/tmp/out");
        assert_eq!(cfg.effective_sample_size(), DEFAULT_SAMPLE_SIZE);
    }

    #[test]
    fn validate_rejects_zero_row_group() {
        let mut cfg = ParquetSinkConfig::local("/tmp/out");
        cfg.row_group_size = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_max_rows() {
        let cfg = ParquetSinkConfig::local("/tmp/out").max_rows_per_file(0);
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_max_bytes() {
        let cfg = ParquetSinkConfig::local("/tmp/out").max_bytes_per_file(0);
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_empty_path() {
        let cfg = ParquetSinkConfig::local("");
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_empty_bucket() {
        let cfg = ParquetSinkConfig::new(ParquetDestination::S3(ParquetS3Destination {
            bucket: String::new(),
            prefix: String::new(),
            region: None,
            endpoint_url: None,
            allow_http: false,
        }));
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn compression_maps_to_parquet() {
        use parquet::basic::Compression as PC;
        assert!(matches!(
            ParquetCompression::Uncompressed.as_parquet(),
            PC::UNCOMPRESSED
        ));
        assert!(matches!(
            ParquetCompression::Snappy.as_parquet(),
            PC::SNAPPY
        ));
        assert!(matches!(ParquetCompression::Gzip.as_parquet(), PC::GZIP(_)));
        assert!(matches!(ParquetCompression::Zstd.as_parquet(), PC::ZSTD(_)));
        assert!(matches!(ParquetCompression::Lz4.as_parquet(), PC::LZ4_RAW));
    }

    #[test]
    fn config_serializes_and_round_trips() {
        let cfg = ParquetSinkConfig::local("/tmp/out")
            .compression(ParquetCompression::Gzip)
            .max_rows_per_file(500);
        let json = serde_json::to_value(&cfg).unwrap();
        let parsed: ParquetSinkConfig = serde_json::from_value(json).unwrap();
        assert_eq!(parsed.compression, ParquetCompression::Gzip);
        assert_eq!(parsed.max_rows_per_file, Some(500));
    }

    #[test]
    fn batch_size_defaults_to_default_batch_size() {
        let cfg = ParquetSinkConfig::local("/tmp/out");
        assert_eq!(cfg.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let cfg = ParquetSinkConfig::local("/tmp/out").with_batch_size(500);
        assert_eq!(cfg.batch_size, 500);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let cfg = ParquetSinkConfig::local("/tmp/out").with_batch_size(0);
        assert_eq!(cfg.batch_size, 0);
        assert!(faucet_core::validate_batch_size(cfg.batch_size).is_ok());
        // Sentinel also passes the sink-level validate().
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate() {
        let cfg =
            ParquetSinkConfig::local("/tmp/out").with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(cfg.batch_size).is_err());
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let json = r#"{
            "destination": {"type": "local_path", "path": "/tmp/out"},
            "batch_size": 250
        }"#;
        let cfg: ParquetSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.batch_size, 250);
    }

    #[test]
    fn batch_size_default_via_serde_when_omitted() {
        let json = r#"{
            "destination": {"type": "local_path", "path": "/tmp/out"}
        }"#;
        let cfg: ParquetSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }
}
