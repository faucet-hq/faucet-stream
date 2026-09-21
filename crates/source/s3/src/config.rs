//! S3 source configuration.

use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Format of files stored in S3.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum S3FileFormat {
    /// Each line in the file is a separate JSON record.
    #[default]
    JsonLines,
    /// The entire file is a JSON array of records.
    JsonArray,
    /// Each file becomes a single record with `"key"` and `"content"` fields.
    RawText,
    /// Apache Parquet objects. Each object is decoded via the Arrow Parquet
    /// reader; its `RecordBatch`es feed both the row path (converted to JSON
    /// records) and the **columnar** fast path
    /// ([`Source::stream_batches`](faucet_core::Source::stream_batches)) so an
    /// `s3(parquet) → parquet`/`delta` chain never materializes
    /// `serde_json::Value`. Requires the crate-local `arrow` feature
    /// (RFC 0002 / #375).
    #[cfg(feature = "arrow")]
    Parquet,
    /// Delimited text, decoded through
    /// [`faucet_core::file_format`] so the records match what every other
    /// connector produces for the same file. Dialect from
    /// [`csv`](S3SourceConfig::csv). Requires `file-format-csv` (#604).
    #[cfg(feature = "file-format-csv")]
    Csv,
    /// XML, decoded to the compact element→object mapping. The repeated
    /// element is named by [`xml`](S3SourceConfig::xml). Requires
    /// `file-format-xml` (#604).
    #[cfg(feature = "file-format-xml")]
    Xml,
    /// An Excel workbook. Sheet and header row from
    /// [`excel`](S3SourceConfig::excel). **Buffered whole** — a workbook is a
    /// zip container whose directory sits at the end, so peak memory is the
    /// object, not the page. Requires `file-format-excel` (#604).
    #[cfg(feature = "file-format-excel")]
    Xlsx,
}

impl S3FileFormat {
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

fn default_concurrency() -> usize {
    10
}

/// Configuration for the S3 source connector.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct S3SourceConfig {
    /// S3 bucket name.
    pub bucket: String,
    /// Object key prefix filter.
    #[serde(default)]
    pub prefix: Option<String>,
    /// AWS region. `None` uses the SDK default.
    #[serde(default)]
    pub region: Option<String>,
    /// Custom endpoint URL for S3-compatible services (e.g. MinIO).
    #[serde(default)]
    pub endpoint_url: Option<String>,
    /// Format of the files to read. Defaults to `json_lines`.
    #[serde(default)]
    pub file_format: S3FileFormat,
    /// Maximum number of objects to read.
    #[serde(default)]
    pub max_objects: Option<usize>,
    /// Maximum number of concurrent object reads (default: 10).
    ///
    /// These `#[serde(default)]`s are the same oversight the REST source
    /// carried: without them every field here was **required**, so a config
    /// naming only `bucket` could not deserialize at all (#609). `bucket` is
    /// the one genuinely required field.
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    /// Records per emitted [`StreamPage`](faucet_core::StreamPage). For
    /// `JsonLines` and `RawText` formats, the object body is decoded
    /// line-by-line via [`tokio::io::AsyncBufReadExt`] and a page is yielded
    /// whenever the buffer reaches this size; multi-object scans flatten so
    /// a single page may contain lines from any object. For `JsonArray`,
    /// each object is buffered fully before its records are chunked into
    /// pages of this size (see the README "Streaming and batching" section
    /// for the caveat). Defaults to [`DEFAULT_BATCH_SIZE`].
    ///
    /// `batch_size = 0` is the "no batching" sentinel: every page is one
    /// complete object — no within-object chunking. Useful for small
    /// lookup files, or for sinks (e.g. SQL `COPY`, BigQuery load jobs)
    /// that prefer one large request per file to many small ones.
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

impl S3SourceConfig {
    /// Create a new config with the required bucket name and sensible defaults.
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            prefix: None,
            region: None,
            endpoint_url: None,
            file_format: S3FileFormat::default(),
            max_objects: None,
            concurrency: 10,
            batch_size: DEFAULT_BATCH_SIZE,
            verify_length: true,
            verify_checksum: false,
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

    /// Set the object key prefix filter.
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
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

    /// Set the file format.
    pub fn file_format(mut self, format: S3FileFormat) -> Self {
        self.file_format = format;
        self
    }

    /// Set the maximum number of objects to read.
    pub fn max_objects(mut self, max: usize) -> Self {
        self.max_objects = Some(max);
        self
    }

    /// Set the maximum number of concurrent object reads.
    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Set the per-page record count for [`Source::stream_pages`](faucet_core::Source::stream_pages).
    ///
    /// Pass `0` to opt out of within-object chunking — every emitted
    /// [`StreamPage`](faucet_core::StreamPage) corresponds to exactly one
    /// S3 object.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Enable or disable the per-object `Content-Length` verification
    /// (default `true`). Sets [`verify_length`](Self::verify_length).
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = S3SourceConfig::new("my-bucket");
        assert_eq!(config.bucket, "my-bucket");
        assert!(config.prefix.is_none());
        assert!(config.region.is_none());
        assert!(config.endpoint_url.is_none());
        assert!(matches!(config.file_format, S3FileFormat::JsonLines));
        assert!(config.max_objects.is_none());
    }

    #[test]
    fn builder_methods() {
        let config = S3SourceConfig::new("my-bucket")
            .prefix("data/")
            .region("us-west-2")
            .endpoint_url("http://localhost:9000")
            .file_format(S3FileFormat::JsonArray)
            .max_objects(10);

        assert_eq!(config.bucket, "my-bucket");
        assert_eq!(config.prefix.as_deref(), Some("data/"));
        assert_eq!(config.region.as_deref(), Some("us-west-2"));
        assert_eq!(
            config.endpoint_url.as_deref(),
            Some("http://localhost:9000")
        );
        assert!(matches!(config.file_format, S3FileFormat::JsonArray));
        assert_eq!(config.max_objects, Some(10));
    }

    #[test]
    fn file_format_default_is_json_lines() {
        let format = S3FileFormat::default();
        assert!(matches!(format, S3FileFormat::JsonLines));
    }

    #[test]
    fn batch_size_defaults_to_default_batch_size() {
        let config = S3SourceConfig::new("my-bucket");
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let config = S3SourceConfig::new("my-bucket").with_batch_size(500);
        assert_eq!(config.batch_size, 500);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config = S3SourceConfig::new("my-bucket").with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let config =
            S3SourceConfig::new("my-bucket").with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_err());
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let json = r#"{
            "bucket": "my-bucket",
            "prefix": null,
            "region": null,
            "endpoint_url": null,
            "file_format": "json_lines",
            "max_objects": null,
            "concurrency": 10,
            "batch_size": 250
        }"#;
        let config: S3SourceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, 250);
    }

    #[test]
    fn verify_defaults_length_on_checksum_off() {
        let cfg = S3SourceConfig::new("b");
        assert!(cfg.verify_length, "length verification defaults on");
        assert!(!cfg.verify_checksum, "checksum verification defaults off");
    }

    #[test]
    fn verify_fields_default_when_absent_from_json() {
        // An existing config that predates these fields must still parse, with
        // length verification on and checksum off.
        let json = r#"{
            "bucket": "my-bucket",
            "prefix": null,
            "region": null,
            "endpoint_url": null,
            "file_format": "json_lines",
            "max_objects": null,
            "concurrency": 10,
            "batch_size": 250
        }"#;
        let config: S3SourceConfig = serde_json::from_str(json).unwrap();
        assert!(config.verify_length);
        assert!(!config.verify_checksum);
    }

    #[test]
    fn verify_builders_override() {
        let cfg = S3SourceConfig::new("b")
            .verify_length(false)
            .verify_checksum(true);
        assert!(!cfg.verify_length);
        assert!(cfg.verify_checksum);
    }

    /// The credentials block is `#[serde(flatten)]`ed, so the YAML/JSON surface must
    /// be byte-identical to the pre-refactor two-sibling-keys shape — no
    /// nested `verify:` block on the way in or out.
    #[test]
    fn verify_keys_stay_top_level_on_the_wire() {
        let json = r#"{
            "bucket": "my-bucket",
            "file_format": "json_lines",
            "concurrency": 10,
            "verify_length": false,
            "verify_checksum": true
        }"#;
        let config: S3SourceConfig = serde_json::from_str(json).unwrap();
        assert!(!config.verify_length);
        assert!(config.verify_checksum);

        let out = serde_json::to_value(&config).unwrap();
        assert_eq!(out["verify_length"], serde_json::json!(false));
        assert_eq!(out["verify_checksum"], serde_json::json!(true));
        assert!(out.get("verify").is_none(), "no nested block: {out}");
    }

    #[cfg(feature = "compression")]
    #[test]
    fn compression_default_is_auto() {
        let cfg = S3SourceConfig::new("bucket");
        assert_eq!(cfg.compression, faucet_core::CompressionConfig::Auto);
    }

    /// Only `bucket` is required (#609) — every other field defaults, so a
    /// hand-written `s3` entry deserializes and `faucet validate` can read it.
    #[test]
    fn a_minimal_config_deserializes_and_omitted_fields_take_their_defaults() {
        let cfg: S3SourceConfig =
            serde_json::from_value(serde_json::json!({ "bucket": "b" })).expect("bucket suffices");
        assert_eq!(cfg.bucket, "b");
        assert!(cfg.prefix.is_none());
        assert!(cfg.region.is_none());
        assert!(cfg.endpoint_url.is_none());
        assert!(cfg.max_objects.is_none());
        assert_eq!(cfg.concurrency, 10);
        assert!(matches!(cfg.file_format, S3FileFormat::JsonLines));
    }

    /// #604 — the shared formats deserialize with their option blocks, and
    /// each maps onto the one `faucet_core::FileFormat` every other file
    /// connector uses for the same bytes.
    #[test]
    #[cfg(feature = "file-format-csv")]
    fn csv_carries_its_dialect_and_maps_onto_the_shared_format() {
        let cfg: S3SourceConfig = serde_json::from_value(serde_json::json!({
            "bucket": "b",
            "file_format": "csv",
            "csv": { "delimiter": ";", "has_headers": false }
        }))
        .expect("csv config");
        assert_eq!(cfg.file_format, S3FileFormat::Csv);
        assert_eq!(cfg.csv.delimiter, ";");
        assert!(!cfg.csv.has_headers);
        assert_eq!(cfg.file_format.shared(), Some(faucet_core::FileFormat::Csv));
        assert_eq!(cfg.format_options().csv.delimiter, ";");
    }

    #[test]
    #[cfg(feature = "file-format-xml")]
    fn xml_carries_its_record_element() {
        let cfg: S3SourceConfig = serde_json::from_value(serde_json::json!({
            "bucket": "b",
            "file_format": "xml",
            "xml": { "record_element": "row" }
        }))
        .expect("xml config");
        assert_eq!(cfg.file_format.shared(), Some(faucet_core::FileFormat::Xml));
        assert_eq!(cfg.format_options().xml.record_element, "row");
    }

    #[test]
    #[cfg(feature = "file-format-excel")]
    fn xlsx_carries_its_sheet_selection() {
        let cfg: S3SourceConfig = serde_json::from_value(serde_json::json!({
            "bucket": "b",
            "file_format": "xlsx",
            "excel": { "sheet": "Data", "header_row": 2 }
        }))
        .expect("xlsx config");
        assert_eq!(
            cfg.file_format.shared(),
            Some(faucet_core::FileFormat::Xlsx)
        );
        assert_eq!(cfg.format_options().excel.sheet.as_deref(), Some("Data"));
        assert_eq!(cfg.format_options().excel.header_row, 2);
    }

    /// The two the connector decodes itself keep their own shapes: `raw_text`
    /// yields `{key, content}` (not core's `{text}`) and parquet is columnar.
    #[test]
    fn the_connector_owned_formats_are_not_routed_through_the_shared_decoder() {
        assert_eq!(S3FileFormat::RawText.shared(), None);
        #[cfg(feature = "arrow")]
        assert_eq!(S3FileFormat::Parquet.shared(), None);
    }
}
