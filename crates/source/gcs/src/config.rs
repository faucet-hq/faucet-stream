//! GCS source configuration.

use faucet_common_gcs::GcsCredentials;
use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Format of files stored in GCS.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum GcsFileFormat {
    /// Each line in the file is a separate JSON record.
    #[default]
    JsonLines,
    /// The entire file is a JSON array of records.
    JsonArray,
    /// Each file becomes a single record with `"key"` and `"content"` fields.
    RawText,
    /// Apache Parquet objects. Decoded via the Arrow Parquet reader; the
    /// resulting `RecordBatch`es feed both the row path (converted to JSON)
    /// and the **columnar** fast path
    /// ([`Source::stream_batches`](faucet_core::Source::stream_batches)) so a
    /// `gcs(parquet) → parquet`/`delta` chain never materializes
    /// `serde_json::Value`. Requires the crate-local `arrow` feature
    /// (RFC 0002 / #375).
    #[cfg(feature = "arrow")]
    Parquet,
    /// Delimited text, decoded through [`faucet_core::file_format`] so the
    /// records match what every other connector produces for the same file.
    /// Dialect from [`csv`](GcsSourceConfig::csv). Requires
    /// `file-format-csv` (#604).
    #[cfg(feature = "file-format-csv")]
    Csv,
    /// XML, decoded to the compact element→object mapping. The repeated
    /// element is named by [`xml`](GcsSourceConfig::xml). Requires
    /// `file-format-xml` (#604).
    #[cfg(feature = "file-format-xml")]
    Xml,
    /// An Excel workbook. Sheet and header row from
    /// [`excel`](GcsSourceConfig::excel). **Buffered whole** — a workbook is a
    /// zip container whose directory sits at the end. Requires
    /// `file-format-excel` (#604).
    #[cfg(feature = "file-format-excel")]
    Xlsx,
}

impl GcsFileFormat {
    /// The shared format this variant maps onto, or `None` for the two the
    /// connector decodes itself (`RawText`'s `{key, content}` envelope is the
    /// connector's own shape, and Parquet is columnar).
    #[cfg(any(
        feature = "file-format-csv",
        feature = "file-format-xml",
        feature = "file-format-excel"
    ))]
    pub(crate) fn shared(&self) -> Option<faucet_core::FileFormat> {
        match self {
            Self::JsonLines => Some(faucet_core::FileFormat::JsonLines),
            Self::JsonArray => Some(faucet_core::FileFormat::JsonArray),
            Self::RawText => None,
            #[cfg(feature = "arrow")]
            Self::Parquet => None,
            #[cfg(feature = "file-format-csv")]
            Self::Csv => Some(faucet_core::FileFormat::Csv),
            #[cfg(feature = "file-format-xml")]
            Self::Xml => Some(faucet_core::FileFormat::Xml),
            #[cfg(feature = "file-format-excel")]
            Self::Xlsx => Some(faucet_core::FileFormat::Xlsx),
        }
    }
}

/// Configuration for the GCS source connector.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct GcsSourceConfig {
    /// GCS bucket name.
    pub bucket: String,
    /// Object name prefix filter. Ignored when `object_keys` is set.
    pub prefix: Option<String>,
    /// Explicit object names. When set, listing is skipped and `prefix`
    /// is ignored.
    pub object_keys: Option<Vec<String>>,
    /// Credential source.
    #[serde(default)]
    pub auth: GcsCredentials,
    /// File format.
    #[serde(default)]
    pub file_format: GcsFileFormat,
    /// Hard cap on the number of objects read (after listing).
    pub max_objects: Option<usize>,
    /// Maximum concurrent object reads (default: 10).
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    /// Records per emitted `StreamPage`. See "Streaming and batching"
    /// in the README. `batch_size = 0` is the "no batching" sentinel and
    /// emits one page per object.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Verify each object's byte length against the length the store
    /// advertises, failing the read with
    /// [`FaucetError::Source`](faucet_core::FaucetError::Source) on a short
    /// (truncated) or over-long transfer (#161). Cheap — a byte counter over
    /// a body that is read anyway — so it defaults to `true`.
    ///
    /// Skipped (with a debug log, not a failure) when the store advertises no
    /// length, or when it reports a non-empty `Content-Encoding`: the body on
    /// the wire is then transcoded and its length legitimately differs from
    /// the stored object's.
    #[serde(default = "default_true")]
    pub verify_length: bool,
    /// Verify each object's body against the checksum the store advertises
    /// (#161). Stronger than the length check but costs a hash over the full
    /// body, so it defaults to `false`. When the store advertises no usable
    /// checksum for an object, verification is skipped for that object (a
    /// debug log notes it); the length check still applies.
    #[serde(default)]
    pub verify_checksum: bool,
    /// Optional storage-host override (e.g. `http://localhost:4443` for
    /// fake-gcs-server). Production users should leave this unset.
    pub storage_host: Option<String>,
    /// Compression codec applied to each downloaded object. Defaults to
    /// [`CompressionConfig::Auto`](faucet_core::CompressionConfig::Auto) —
    /// the codec is resolved per-object-key, so a single source can read a
    /// mix of compressed and uncompressed objects. Requires the
    /// crate-local `compression` feature.
    #[cfg(feature = "compression")]
    #[serde(default)]
    pub compression: faucet_core::CompressionConfig,
    /// CSV dialect, used when `file_format: csv` (#604).
    #[serde(default)]
    pub csv: faucet_core::CsvOptions,
    /// Worksheet selection, used when `file_format: xlsx` (#604).
    #[serde(default)]
    pub excel: faucet_core::ExcelOptions,
    /// Record framing, used when `file_format: xml` (#604).
    #[serde(default)]
    pub xml: faucet_core::XmlOptions,
}

/// Serde default for the integrity flags that default on.
fn default_true() -> bool {
    true
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}
fn default_concurrency() -> usize {
    10
}

impl GcsSourceConfig {
    /// Create a new config with the required bucket name and sensible defaults.
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            prefix: None,
            object_keys: None,
            auth: GcsCredentials::default(),
            file_format: GcsFileFormat::default(),
            max_objects: None,
            concurrency: default_concurrency(),
            batch_size: default_batch_size(),
            verify_length: true,
            verify_checksum: false,
            storage_host: None,
            #[cfg(feature = "compression")]
            compression: faucet_core::CompressionConfig::default(),
            csv: faucet_core::CsvOptions::default(),
            excel: faucet_core::ExcelOptions::default(),
            xml: faucet_core::XmlOptions::default(),
        }
    }

    /// The per-format option blocks in the shape
    /// [`faucet_core::file_format::decode`] wants.
    #[cfg(any(
        feature = "file-format-csv",
        feature = "file-format-xml",
        feature = "file-format-excel"
    ))]
    pub(crate) fn format_options(&self) -> faucet_core::FormatOptions {
        faucet_core::FormatOptions {
            csv: self.csv.clone(),
            excel: self.excel.clone(),
            xml: self.xml.clone(),
        }
    }

    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    pub fn object_keys(mut self, keys: Vec<String>) -> Self {
        self.object_keys = Some(keys);
        self
    }

    pub fn auth(mut self, creds: GcsCredentials) -> Self {
        self.auth = creds;
        self
    }

    pub fn file_format(mut self, format: GcsFileFormat) -> Self {
        self.file_format = format;
        self
    }

    pub fn max_objects(mut self, max: usize) -> Self {
        self.max_objects = Some(max);
        self
    }

    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn storage_host(mut self, host: impl Into<String>) -> Self {
        self.storage_host = Some(host.into());
        self
    }

    /// Enable or disable the per-object length verification (default `true`).
    /// Sets [`verify_length`](Self::verify_length).
    pub fn verify_length(mut self, verify: bool) -> Self {
        self.verify_length = verify;
        self
    }

    /// Enable or disable per-object checksum verification (default `false`).
    /// Sets [`verify_checksum`](Self::verify_checksum).
    pub fn verify_checksum(mut self, verify: bool) -> Self {
        self.verify_checksum = verify;
        self
    }

    /// Set the compression codec. Available only with the `compression` feature.
    #[cfg(feature = "compression")]
    pub fn compression(mut self, c: faucet_core::CompressionConfig) -> Self {
        self.compression = c;
        self
    }

    /// Validate the config at load time so a bad config fails fast with a typed
    /// `FaucetError::Config` instead of surfacing deep in a run: rejects an
    /// out-of-range `batch_size` (`> MAX_BATCH_SIZE`) and an empty `bucket`.
    ///
    /// `faucet_core` is referenced by full path here (rather than imported) so
    /// the field-doc links above keep their explicit targets and the committed
    /// config JSON Schema stays byte-identical.
    pub fn validate(&self) -> Result<(), faucet_core::FaucetError> {
        if self.bucket.trim().is_empty() {
            return Err(faucet_core::FaucetError::Config(
                "GCS source requires a non-empty `bucket`".into(),
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
        let config = GcsSourceConfig::new("my-bucket");
        assert_eq!(config.bucket, "my-bucket");
        assert!(config.prefix.is_none());
        assert!(config.object_keys.is_none());
        assert!(matches!(config.auth, GcsCredentials::ApplicationDefault));
        assert!(matches!(config.file_format, GcsFileFormat::JsonLines));
        assert!(config.max_objects.is_none());
        assert_eq!(config.concurrency, 10);
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
        assert!(config.storage_host.is_none());
    }

    #[test]
    fn builder_methods() {
        let config = GcsSourceConfig::new("my-bucket")
            .prefix("data/")
            .file_format(GcsFileFormat::JsonArray)
            .max_objects(5)
            .concurrency(20)
            .with_batch_size(250)
            .storage_host("http://localhost:4443");

        assert_eq!(config.bucket, "my-bucket");
        assert_eq!(config.prefix.as_deref(), Some("data/"));
        assert!(matches!(config.file_format, GcsFileFormat::JsonArray));
        assert_eq!(config.max_objects, Some(5));
        assert_eq!(config.concurrency, 20);
        assert_eq!(config.batch_size, 250);
        assert_eq!(
            config.storage_host.as_deref(),
            Some("http://localhost:4443")
        );
    }

    #[test]
    fn file_format_default_is_json_lines() {
        assert!(matches!(GcsFileFormat::default(), GcsFileFormat::JsonLines));
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config = GcsSourceConfig::new("b").with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected() {
        let config = GcsSourceConfig::new("b").with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_err());
    }

    #[cfg(feature = "compression")]
    #[test]
    fn compression_default_is_auto() {
        let cfg = GcsSourceConfig::new("bucket");
        assert_eq!(cfg.compression, faucet_core::CompressionConfig::Auto);
    }

    #[test]
    fn verify_defaults_length_on_checksum_off() {
        let cfg = GcsSourceConfig::new("b");
        assert!(cfg.verify_length);
        assert!(!cfg.verify_checksum);
    }

    #[test]
    fn verify_builders_override() {
        let cfg = GcsSourceConfig::new("b")
            .verify_length(false)
            .verify_checksum(true);
        assert!(!cfg.verify_length);
        assert!(cfg.verify_checksum);
    }

    #[test]
    fn verify_fields_default_when_absent_from_json() {
        let json = r#"{
            "bucket": "my-bucket",
            "prefix": null,
            "object_keys": null,
            "file_format": "json_lines",
            "max_objects": null,
            "concurrency": 10,
            "storage_host": null
        }"#;
        let config: GcsSourceConfig = serde_json::from_str(json).unwrap();
        assert!(config.verify_length);
        assert!(!config.verify_checksum);
    }

    /// The credentials block is `#[serde(flatten)]`ed, so the wire shape must stay
    /// exactly the two top-level sibling keys it always was.
    #[test]
    fn verify_keys_stay_top_level_on_the_wire() {
        let json = r#"{
            "bucket": "my-bucket",
            "verify_length": false,
            "verify_checksum": true
        }"#;
        let config: GcsSourceConfig = serde_json::from_str(json).unwrap();
        assert!(!config.verify_length);
        assert!(config.verify_checksum);

        let out = serde_json::to_value(&config).unwrap();
        assert_eq!(out["verify_length"], serde_json::json!(false));
        assert_eq!(out["verify_checksum"], serde_json::json!(true));
        assert!(out.get("verify").is_none(), "no nested block: {out}");
    }

    #[test]
    fn batch_size_defaults_when_omitted_from_json() {
        let json = r#"{
            "bucket": "my-bucket",
            "prefix": null,
            "object_keys": null,
            "file_format": "json_lines",
            "max_objects": null,
            "concurrency": 10,
            "storage_host": null
        }"#;
        let config: GcsSourceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn validate_accepts_valid_config() {
        assert!(GcsSourceConfig::new("my-bucket").validate().is_ok());
    }

    #[test]
    fn validate_rejects_oversized_batch_size() {
        let config =
            GcsSourceConfig::new("my-bucket").with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(matches!(
            config.validate(),
            Err(faucet_core::FaucetError::Config(_))
        ));
    }

    #[test]
    fn validate_rejects_empty_bucket() {
        assert!(matches!(
            GcsSourceConfig::new("   ").validate(),
            Err(faucet_core::FaucetError::Config(_))
        ));
    }
}
