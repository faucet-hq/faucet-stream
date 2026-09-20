//! S3 sink configuration.

use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// On-the-wire format of objects written by the S3 sink.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum S3SinkFormat {
    /// Newline-delimited JSON — one JSON record per line (the default).
    #[default]
    JsonLines,
    /// Apache Parquet. Each written object is a complete, self-contained
    /// Parquet file. Enables the **columnar** fast path
    /// ([`Sink::write_batch_columnar`](faucet_core::Sink::write_batch_columnar))
    /// so a `parquet`/`delta` → `s3(parquet)` chain never materializes
    /// `serde_json::Value`. Requires the crate-local `arrow` feature
    /// (RFC 0002 / #375).
    #[cfg(feature = "arrow")]
    Parquet,
}

/// Configuration for the S3 sink connector.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct S3SinkConfig {
    /// S3 bucket name.
    pub bucket: String,
    /// Key prefix for written objects.
    pub prefix: String,
    /// Object format (default: `json_lines`). Set to `parquet` (with the
    /// `arrow` feature) to write Parquet objects and enable the columnar
    /// fast path.
    #[serde(default)]
    pub format: S3SinkFormat,
    /// AWS region. `None` uses the SDK default.
    pub region: Option<String>,
    /// Custom endpoint URL for S3-compatible services (e.g. MinIO).
    pub endpoint_url: Option<String>,
    /// File extension for written objects (default: `.jsonl`).
    pub file_extension: String,
    /// Maximum records per object. Since #618 the sink **accumulates across
    /// `write_batch` calls** and rolls to a new object when this (or
    /// [`max_bytes_per_file`](Self::max_bytes_per_file)) is reached, so a
    /// small upstream page no longer means a small object. `None` removes the
    /// record cap; with neither cap set the whole run lands in one object,
    /// closed at `flush`.
    pub max_records_per_file: Option<usize>,
    /// Maximum **bytes** per object before rolling to a new one (#618).
    ///
    /// Rows are a poor proxy for object size — 10k wide rows and 10k
    /// `{"id":1}` rows differ by orders of magnitude — so a rows-only cap
    /// either writes tiny objects for narrow data or unbounded ones for wide
    /// data. This is also what bounds peak memory: the open object's body is
    /// buffered until it rolls. Counted on the **uncompressed** body, before
    /// any `compression` codec, so the threshold means the same thing whatever
    /// the codec. `None` (the default) removes the byte cap.
    ///
    /// A single record larger than the cap still gets its own object rather
    /// than being split (which would corrupt it) or dropped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes_per_file: Option<usize>,
    /// Maximum number of concurrent file uploads (default: 10).
    pub concurrency: usize,
    /// Records per S3 object written by a single
    /// [`Sink::write_batch`](faucet_core::Sink::write_batch) call. When a call
    /// hands the sink `N` records with `batch_size = M > 0`, the sink writes
    /// `ceil(N / M)` objects, each containing at most `M` records (the final
    /// object holds the remainder). Defaults to [`DEFAULT_BATCH_SIZE`].
    ///
    /// `batch_size = 0` is the "no batching" sentinel: the sink writes
    /// whatever upstream hands it without re-chunking (still honouring
    /// `max_records_per_file` if set). Recommended for S3 — most callers
    /// should leave this at `0` and let the source's `batch_size` drive
    /// object sizing, because many tiny S3 objects are a well-known
    /// anti-pattern (per-request overhead, slower downstream reads,
    /// LIST/PUT cost).
    ///
    /// When both `batch_size > 0` and `max_records_per_file` are set, the
    /// effective per-object cap is `min(batch_size, max_records_per_file)`.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Compression codec applied to each uploaded object body. Defaults to
    /// [`CompressionConfig::Auto`](faucet_core::CompressionConfig::Auto) —
    /// resolves against `file_extension` (so `.jsonl.gz` triggers gzip).
    /// Requires the crate-local `compression` feature. Note: this sink does
    /// **not** set the S3 `Content-Encoding` header, so consumers must
    /// decompress explicitly.
    #[cfg(feature = "compression")]
    #[serde(default)]
    pub compression: faucet_core::CompressionConfig,
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

impl S3SinkConfig {
    /// Create a new config with the required bucket name and sensible defaults.
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            prefix: String::new(),
            format: S3SinkFormat::default(),
            region: None,
            endpoint_url: None,
            file_extension: ".jsonl".to_string(),
            max_records_per_file: None,
            max_bytes_per_file: None,
            concurrency: 10,
            batch_size: DEFAULT_BATCH_SIZE,
            #[cfg(feature = "compression")]
            compression: faucet_core::CompressionConfig::Auto,
        }
    }

    /// Set the key prefix for written objects.
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// Set the object format (`json_lines` or, with the `arrow` feature,
    /// `parquet`).
    pub fn format(mut self, format: S3SinkFormat) -> Self {
        self.format = format;
        self
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

    /// Set the file extension for written objects.
    pub fn file_extension(mut self, ext: impl Into<String>) -> Self {
        self.file_extension = ext.into();
        self
    }

    /// Set the maximum number of records per file.
    /// The effective per-object record cap, combining `batch_size` (write-side
    /// re-chunking) and `max_records_per_file`. `None` means "no record cap".
    ///
    /// Lives on the config rather than the sink because the cross-page
    /// accumulator (#618) needs it at construction time, and the Parquet path
    /// needs the same number — two definitions would be one drift away from
    /// objects of different sizes depending on the format.
    pub fn effective_chunk_cap(&self) -> Option<usize> {
        match (self.batch_size, self.max_records_per_file) {
            (0, None) => None,
            (0, Some(0)) => None,
            (0, Some(max)) => Some(max),
            (bs, None) => Some(bs),
            (bs, Some(0)) => Some(bs),
            (bs, Some(max)) => Some(bs.min(max)),
        }
    }

    pub fn max_bytes_per_file(mut self, max: usize) -> Self {
        self.max_bytes_per_file = Some(max);
        self
    }

    /// Set the per-object record cap.
    pub fn max_records_per_file(mut self, max: usize) -> Self {
        self.max_records_per_file = Some(max);
        self
    }

    /// Set the maximum number of concurrent file uploads.
    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Set the per-object record count for
    /// [`Sink::write_batch`](faucet_core::Sink::write_batch).
    ///
    /// Pass `0` to opt out of write-side re-chunking — the sink writes
    /// whatever upstream hands it as a single object (still honouring
    /// `max_records_per_file` if set). `0` is the recommended value for S3
    /// because writing many small objects is an anti-pattern.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the compression codec. Available only with the `compression` feature.
    #[cfg(feature = "compression")]
    pub fn compression(mut self, c: faucet_core::CompressionConfig) -> Self {
        self.compression = c;
        self
    }

    /// Validate the config at construction time. Rejects an empty `bucket`
    /// (a typo or an unset `${env:…}`) with a typed `FaucetError::Config`
    /// rather than letting it surface as an opaque cloud-API failure on the
    /// first upload, and validates `batch_size`.
    pub fn validate(&self) -> Result<(), faucet_core::FaucetError> {
        if self.bucket.trim().is_empty() {
            return Err(faucet_core::FaucetError::Config(
                "S3 sink `bucket` must not be empty".to_owned(),
            ));
        }
        faucet_core::validate_batch_size(self.batch_size)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = S3SinkConfig::new("my-bucket");
        assert_eq!(config.bucket, "my-bucket");
        assert_eq!(config.prefix, "");
        assert!(config.region.is_none());
        assert!(config.endpoint_url.is_none());
        assert_eq!(config.file_extension, ".jsonl");
        assert!(config.max_records_per_file.is_none());
    }

    #[test]
    fn builder_methods() {
        let config = S3SinkConfig::new("my-bucket")
            .prefix("output/")
            .region("eu-west-1")
            .endpoint_url("http://localhost:9000")
            .file_extension(".json")
            .max_records_per_file(1000);

        assert_eq!(config.bucket, "my-bucket");
        assert_eq!(config.prefix, "output/");
        assert_eq!(config.region.as_deref(), Some("eu-west-1"));
        assert_eq!(
            config.endpoint_url.as_deref(),
            Some("http://localhost:9000")
        );
        assert_eq!(config.file_extension, ".json");
        assert_eq!(config.max_records_per_file, Some(1000));
    }

    #[test]
    fn batch_size_defaults_to_default_batch_size() {
        let config = S3SinkConfig::new("my-bucket");
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn validate_accepts_a_normal_config() {
        assert!(S3SinkConfig::new("my-bucket").validate().is_ok());
    }

    #[test]
    fn validate_rejects_empty_bucket() {
        for bucket in ["", "   "] {
            let err = S3SinkConfig::new(bucket).validate().unwrap_err();
            assert!(
                matches!(err, faucet_core::FaucetError::Config(msg) if msg.contains("bucket")),
                "expected a Config error naming `bucket` for {bucket:?}"
            );
        }
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let config = S3SinkConfig::new("my-bucket").with_batch_size(500);
        assert_eq!(config.batch_size, 500);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config = S3SinkConfig::new("my-bucket").with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let config =
            S3SinkConfig::new("my-bucket").with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_err());
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let json = r#"{
            "bucket": "my-bucket",
            "prefix": "",
            "region": null,
            "endpoint_url": null,
            "file_extension": ".jsonl",
            "max_records_per_file": null,
            "concurrency": 10,
            "batch_size": 250
        }"#;
        let config: S3SinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, 250);
    }

    #[test]
    fn batch_size_defaults_when_omitted_from_json() {
        let json = r#"{
            "bucket": "my-bucket",
            "prefix": "",
            "region": null,
            "endpoint_url": null,
            "file_extension": ".jsonl",
            "max_records_per_file": null,
            "concurrency": 10
        }"#;
        let config: S3SinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[cfg(feature = "compression")]
    #[test]
    fn compression_config_round_trips() {
        let json = r#"{
            "bucket": "b",
            "prefix": "",
            "region": null,
            "endpoint_url": null,
            "file_extension": ".jsonl.gz",
            "max_records_per_file": null,
            "concurrency": 1,
            "batch_size": 0,
            "compression": "gzip"
        }"#;
        let config: S3SinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.compression, faucet_core::CompressionConfig::Gzip);
    }

    #[cfg(feature = "compression")]
    #[test]
    fn compression_default_is_auto() {
        let cfg = S3SinkConfig::new("bucket");
        assert_eq!(cfg.compression, faucet_core::CompressionConfig::Auto);
    }
}
