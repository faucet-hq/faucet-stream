//! GCS sink configuration.

use faucet_common_gcs::GcsCredentials;
use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// On-the-wire format of objects written by the GCS sink.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GcsSinkFormat {
    /// Newline-delimited JSON — one JSON record per line (the default).
    #[default]
    JsonLines,
    /// Apache Parquet. Each written object is a complete, self-contained
    /// Parquet file. Enables the **columnar** fast path
    /// ([`Sink::write_batch_columnar`](faucet_core::Sink::write_batch_columnar))
    /// so a `parquet`/`delta` → `gcs(parquet)` chain never materializes
    /// `serde_json::Value`. Requires the crate-local `arrow` feature
    /// (RFC 0002 / #375).
    #[cfg(feature = "arrow")]
    Parquet,
    /// A single JSON array per object.
    JsonArray,
    /// Delimited text. Columns are the union of every record's keys; dialect
    /// from [`csv`](GcsSinkConfig::csv). Requires `file-format-csv` (#604).
    #[cfg(feature = "file-format-csv")]
    Csv,
    /// XML, one element per record. Framing from
    /// [`xml`](GcsSinkConfig::xml). Requires `file-format-xml` (#604).
    #[cfg(feature = "file-format-xml")]
    Xml,
    /// An Excel workbook. Sheet name from [`excel`](GcsSinkConfig::excel).
    /// Requires `file-format-excel` (#604).
    #[cfg(feature = "file-format-excel")]
    Xlsx,
}

impl GcsSinkFormat {
    /// The shared format this variant maps onto, or `None` for Parquet, which
    /// is columnar and has its own Arrow writer.
    pub(crate) fn shared(self) -> Option<faucet_core::FileFormat> {
        match self {
            Self::JsonLines => Some(faucet_core::FileFormat::JsonLines),
            Self::JsonArray => Some(faucet_core::FileFormat::JsonArray),
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

    /// Whether an object of this format can be built one record at a time.
    ///
    /// Only JSON Lines can: every other format has a header, a wrapper, or a
    /// container index, so its records must be buffered and encoded together.
    pub(crate) fn appends_per_record(self) -> bool {
        matches!(self, Self::JsonLines)
    }
}

/// Configuration for the GCS sink connector.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GcsSinkConfig {
    /// GCS bucket name.
    pub bucket: String,
    /// Object-name prefix for written files.
    pub prefix: String,
    /// Object format (default: `json_lines`). Set to `parquet` (with the
    /// `arrow` feature) to write Parquet objects and enable the columnar
    /// fast path.
    #[serde(default)]
    pub format: GcsSinkFormat,
    /// Credential source.
    #[serde(default)]
    pub auth: GcsCredentials,
    /// File extension for written objects (default `.jsonl`).
    #[serde(default = "default_file_extension")]
    pub file_extension: String,
    /// Hard cap on records per uploaded object. `None` means a single
    /// object per `write_batch` call (still subject to `batch_size`).
    pub max_records_per_file: Option<usize>,
    /// Maximum **bytes** per object before rolling to a new one (#618).
    ///
    /// Rows are a poor proxy for object size, so a rows-only cap either writes
    /// tiny objects for narrow data or unbounded ones for wide data. This is
    /// also what bounds peak memory: the open object's body is buffered until
    /// it rolls. Counted on the uncompressed body, before any `compression`
    /// codec, so the threshold means the same thing whatever the codec.
    /// `None` (the default) removes the byte cap. A single record larger than
    /// the cap still gets its own object rather than being split or dropped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes_per_file: Option<usize>,
    /// Maximum number of concurrent uploads (default 10).
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    /// Records per uploaded object from a single `write_batch` call.
    /// `batch_size = 0` writes whatever upstream hands the sink as one
    /// object. Recommended value for GCS is `0` — many tiny objects is
    /// a well-known anti-pattern.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Optional storage-host override (integration-test escape hatch).
    pub storage_host: Option<String>,
    /// Compression codec applied to each uploaded object body. Defaults to
    /// [`CompressionConfig::Auto`](faucet_core::CompressionConfig::Auto) —
    /// resolves against `file_extension` (so `.jsonl.gz` triggers gzip).
    /// Requires the crate-local `compression` feature. Note: this sink does
    /// **not** set the GCS `Content-Encoding` metadata, so consumers must
    /// decompress explicitly.
    #[cfg(feature = "compression")]
    #[serde(default)]
    pub compression: faucet_core::CompressionConfig,
    /// CSV dialect, used when `format: csv` (#604).
    #[serde(default)]
    pub csv: faucet_core::CsvOptions,
    /// Worksheet name, used when `format: xlsx` (#604).
    #[serde(default)]
    pub excel: faucet_core::ExcelOptions,
    /// Record framing, used when `format: xml` (#604).
    #[serde(default)]
    pub xml: faucet_core::XmlOptions,
}

fn default_file_extension() -> String {
    ".jsonl".to_string()
}
fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}
fn default_concurrency() -> usize {
    10
}

impl GcsSinkConfig {
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            prefix: String::new(),
            format: GcsSinkFormat::default(),
            auth: GcsCredentials::default(),
            file_extension: default_file_extension(),
            max_records_per_file: None,
            max_bytes_per_file: None,
            concurrency: default_concurrency(),
            batch_size: default_batch_size(),
            storage_host: None,
            #[cfg(feature = "compression")]
            compression: faucet_core::CompressionConfig::Auto,
            csv: faucet_core::CsvOptions::default(),
            excel: faucet_core::ExcelOptions::default(),
            xml: faucet_core::XmlOptions::default(),
        }
    }

    /// The per-format option blocks in the shape
    /// [`faucet_core::file_format::encode`] wants.
    pub(crate) fn format_options(&self) -> faucet_core::FormatOptions {
        faucet_core::FormatOptions {
            csv: self.csv.clone(),
            excel: self.excel.clone(),
            xml: self.xml.clone(),
        }
    }

    /// Set the CSV dialect used when `format: csv` (#604).
    pub fn csv(mut self, csv: faucet_core::CsvOptions) -> Self {
        self.csv = csv;
        self
    }

    /// Set the worksheet name used when `format: xlsx` (#604).
    pub fn excel(mut self, excel: faucet_core::ExcelOptions) -> Self {
        self.excel = excel;
        self
    }

    /// Set the record framing used when `format: xml` (#604).
    pub fn xml(mut self, xml: faucet_core::XmlOptions) -> Self {
        self.xml = xml;
        self
    }

    pub fn prefix(mut self, p: impl Into<String>) -> Self {
        self.prefix = p.into();
        self
    }
    /// Set the object format (`json_lines` or, with the `arrow` feature,
    /// `parquet`).
    pub fn format(mut self, format: GcsSinkFormat) -> Self {
        self.format = format;
        self
    }
    pub fn auth(mut self, c: GcsCredentials) -> Self {
        self.auth = c;
        self
    }
    pub fn file_extension(mut self, ext: impl Into<String>) -> Self {
        self.file_extension = ext.into();
        self
    }
    pub fn max_bytes_per_file(mut self, n: usize) -> Self {
        self.max_bytes_per_file = Some(n);
        self
    }

    /// Set the per-object record cap.
    pub fn max_records_per_file(mut self, n: usize) -> Self {
        self.max_records_per_file = Some(n);
        self
    }
    pub fn concurrency(mut self, n: usize) -> Self {
        self.concurrency = n;
        self
    }
    pub fn with_batch_size(mut self, n: usize) -> Self {
        self.batch_size = n;
        self
    }
    pub fn storage_host(mut self, h: impl Into<String>) -> Self {
        self.storage_host = Some(h.into());
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
                "GCS sink `bucket` must not be empty".to_owned(),
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
    fn defaults() {
        let c = GcsSinkConfig::new("b");
        assert_eq!(c.bucket, "b");
        assert_eq!(c.prefix, "");
        assert!(matches!(c.auth, GcsCredentials::ApplicationDefault));
        assert_eq!(c.file_extension, ".jsonl");
        assert!(c.max_records_per_file.is_none());
        assert_eq!(c.concurrency, 10);
        assert_eq!(c.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
        assert!(c.storage_host.is_none());
    }

    #[test]
    fn validate_accepts_a_normal_config() {
        assert!(GcsSinkConfig::new("b").validate().is_ok());
    }

    #[test]
    fn validate_rejects_empty_bucket() {
        for bucket in ["", "   "] {
            let err = GcsSinkConfig::new(bucket).validate().unwrap_err();
            assert!(
                matches!(err, faucet_core::FaucetError::Config(msg) if msg.contains("bucket")),
                "expected a Config error naming `bucket` for {bucket:?}"
            );
        }
    }

    #[test]
    fn builder_methods() {
        let c = GcsSinkConfig::new("b")
            .prefix("out/")
            .file_extension(".ndjson")
            .max_records_per_file(500)
            .concurrency(4)
            .with_batch_size(0)
            .storage_host("http://localhost:4443");
        assert_eq!(c.prefix, "out/");
        assert_eq!(c.file_extension, ".ndjson");
        assert_eq!(c.max_records_per_file, Some(500));
        assert_eq!(c.concurrency, 4);
        assert_eq!(c.batch_size, 0);
        assert_eq!(c.storage_host.as_deref(), Some("http://localhost:4443"));
    }

    #[test]
    fn batch_size_sentinel_accepted_and_above_max_rejected() {
        assert!(faucet_core::validate_batch_size(0).is_ok());
        assert!(faucet_core::validate_batch_size(faucet_core::MAX_BATCH_SIZE + 1).is_err());
    }

    #[test]
    fn batch_size_defaults_when_omitted_from_json() {
        let json = r#"{
            "bucket": "b",
            "prefix": "p/",
            "max_records_per_file": null,
            "concurrency": 10,
            "storage_host": null
        }"#;
        let c: GcsSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(c.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[cfg(feature = "compression")]
    #[test]
    fn compression_default_is_auto() {
        let cfg = GcsSinkConfig::new("bucket");
        assert_eq!(cfg.compression, faucet_core::CompressionConfig::Auto);
    }

    #[cfg(feature = "compression")]
    #[test]
    fn compression_config_round_trips() {
        let json = r#"{
            "bucket": "b",
            "prefix": "",
            "file_extension": ".jsonl.gz",
            "max_records_per_file": null,
            "concurrency": 1,
            "batch_size": 0,
            "storage_host": null,
            "compression": "gzip"
        }"#;
        let cfg: GcsSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.compression, faucet_core::CompressionConfig::Gzip);
    }

    #[cfg(feature = "arrow")]
    #[test]
    fn format_defaults_json_lines_and_builder_sets_parquet() {
        assert_eq!(GcsSinkConfig::new("b").format, GcsSinkFormat::JsonLines);
        let cfg = GcsSinkConfig::new("b").format(GcsSinkFormat::Parquet);
        assert_eq!(cfg.format, GcsSinkFormat::Parquet);
    }

    // ── file formats (#604) ───────────────────────────────────────────────

    /// GCS is excluded from coverage on its I/O files (no gRPC-compatible
    /// emulator, #220), which makes its *pure* logic the only part that can
    /// be verified at all — so it is verified here rather than left to the
    /// exclusion to hide.
    #[test]
    fn every_format_maps_onto_the_shared_vocabulary() {
        assert_eq!(
            GcsSinkFormat::JsonLines.shared(),
            Some(faucet_core::FileFormat::JsonLines)
        );
        assert_eq!(
            GcsSinkFormat::JsonArray.shared(),
            Some(faucet_core::FileFormat::JsonArray)
        );
        #[cfg(feature = "file-format-csv")]
        assert_eq!(
            GcsSinkFormat::Csv.shared(),
            Some(faucet_core::FileFormat::Csv)
        );
        #[cfg(feature = "file-format-xml")]
        assert_eq!(
            GcsSinkFormat::Xml.shared(),
            Some(faucet_core::FileFormat::Xml)
        );
        #[cfg(feature = "file-format-excel")]
        assert_eq!(
            GcsSinkFormat::Xlsx.shared(),
            Some(faucet_core::FileFormat::Xlsx)
        );
        // Parquet is columnar and owns its own Arrow writer, so it opts out
        // of the record encoder entirely.
        #[cfg(feature = "arrow")]
        assert_eq!(GcsSinkFormat::Parquet.shared(), None);
    }

    #[test]
    fn only_json_lines_appends_per_record() {
        assert!(GcsSinkFormat::JsonLines.appends_per_record());
        assert!(!GcsSinkFormat::JsonArray.appends_per_record());
        assert_eq!(GcsSinkFormat::default(), GcsSinkFormat::JsonLines);
        #[cfg(feature = "file-format-csv")]
        assert!(!GcsSinkFormat::Csv.appends_per_record());
        #[cfg(feature = "file-format-xml")]
        assert!(!GcsSinkFormat::Xml.appends_per_record());
        #[cfg(feature = "file-format-excel")]
        assert!(!GcsSinkFormat::Xlsx.appends_per_record());
    }

    #[test]
    fn the_format_option_blocks_survive_the_builders() {
        let cfg = GcsSinkConfig::new("b")
            .format(GcsSinkFormat::JsonArray)
            .csv(faucet_core::CsvOptions {
                delimiter: ";".into(),
                has_headers: false,
            })
            .excel(faucet_core::ExcelOptions {
                sheet: Some("Data".into()),
                header_row: 2,
            })
            .xml(faucet_core::XmlOptions {
                record_element: "row".into(),
                root_element: "rows".into(),
            });
        assert_eq!(cfg.format, GcsSinkFormat::JsonArray);
        let opts = cfg.format_options();
        assert_eq!(opts.csv.delimiter, ";");
        assert!(!opts.csv.has_headers);
        assert_eq!(opts.excel.sheet.as_deref(), Some("Data"));
        assert_eq!(opts.excel.header_row, 2);
        assert_eq!(opts.xml.record_element, "row");
        assert_eq!(opts.xml.root_element, "rows");
    }

    /// Both rollover caps are opt-in and independent (#618): setting one must
    /// not disturb the other, or an operator asking for a byte cap silently
    /// gets a record cap too.
    #[test]
    fn the_rollover_caps_are_independent_and_default_to_unset() {
        let base = GcsSinkConfig::new("b");
        assert_eq!(base.max_bytes_per_file, None);
        assert_eq!(base.max_records_per_file, None);

        let bytes_only = GcsSinkConfig::new("b").max_bytes_per_file(4096);
        assert_eq!(bytes_only.max_bytes_per_file, Some(4096));
        assert_eq!(bytes_only.max_records_per_file, None);

        let both = GcsSinkConfig::new("b")
            .max_bytes_per_file(4096)
            .max_records_per_file(10);
        assert_eq!(both.max_bytes_per_file, Some(4096));
        assert_eq!(both.max_records_per_file, Some(10));
    }
}
