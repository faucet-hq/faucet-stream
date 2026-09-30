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
    /// A single JSON array per object.
    JsonArray,
    /// Delimited text. Columns are the union of every record's keys; dialect
    /// from [`csv`](S3SinkConfig::csv). Requires `file-format-csv` (#604).
    #[cfg(feature = "file-format-csv")]
    Csv,
    /// XML, one element per record. Framing from
    /// [`xml`](S3SinkConfig::xml). Requires `file-format-xml` (#604).
    #[cfg(feature = "file-format-xml")]
    Xml,
    /// An Excel workbook. Sheet name from [`excel`](S3SinkConfig::excel).
    /// Requires `file-format-excel` (#604).
    #[cfg(feature = "file-format-excel")]
    Xlsx,
    /// An Apache Avro Object Container File, against `avro.schema` or a
    /// schema inferred from the object's records; block codec from
    /// `avro.codec`. Requires `file-format-avro` (#719). There is no ORC
    /// variant: ORC is read-only.
    #[cfg(feature = "file-format-avro")]
    Avro,
    /// Unparsed text: each record's `content` field (or the record as JSON)
    /// on its own line (#777).
    RawText,
    /// Take the format from the object name's extension — `path`'s, else
    /// `file_extension` — looking through a compression suffix (#777).
    Auto,
}

impl S3SinkFormat {
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
            #[cfg(feature = "file-format-avro")]
            Self::Avro => Some(faucet_core::FileFormat::Avro),
            Self::RawText => Some(faucet_core::FileFormat::RawText),
            Self::Auto => None,
        }
    }

    /// The explicit format, or `None` for `auto` (taken from the name's
    /// extension).
    pub(crate) fn explicit(self) -> Option<faucet_core::FileFormat> {
        match self {
            #[cfg(feature = "arrow")]
            Self::Parquet => Some(faucet_core::FileFormat::Parquet),
            other => other.shared(),
        }
    }
}

/// Configuration for the S3 sink connector.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(extend("x-faucet-aliases" = ["mode"]))]
pub struct S3SinkConfig {
    /// S3 bucket name.
    pub bucket: String,
    /// Key prefix for written objects.
    #[serde(default)]
    pub prefix: String,
    /// Object key template inside the bucket, after `prefix` — the file
    /// sink's `path` (#777). May contain `{part}` (numbered objects) and
    /// `${now.*}` tokens; a trailing `/` is a directory of
    /// `part-{part}<extension>` objects. When set, `file_extension` is not
    /// used. Unset: every run writes new objects named
    /// `<prefix><run id>-<part><file_extension>` (`<run id>` is a fresh
    /// time-ordered UUID, `<part>` is `00001`, `00002`, …).
    ///
    /// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
    /// minor release; any change is called out in the changelog.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// What to do when an object of the same name already exists
    /// (`if_exists`): `replace` it (default), `append` to it (JSON Lines, CSV,
    /// raw text) or fail with `error` (#777). Needs `path`. `mode` is
    /// accepted as another name for this key, and `overwrite` /
    /// `error_if_exists` for its values.
    ///
    /// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
    /// minor release; any change is called out in the changelog.
    #[serde(default, rename = "if_exists", alias = "mode")]
    pub mode: faucet_common_file::write::IfExists,
    /// `append` (default) or `overwrite`: write the run's objects into a
    /// hidden swap area and move them into place only after a successful
    /// run, then remove objects of an earlier run that match `path` and
    /// were not rewritten (#777). The move is one object at a time, so a
    /// reader listing the destination while it runs can see new objects beside
    /// old ones; a move that was interrupted is finished by the next run.
    /// Needs `path`.
    ///
    /// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
    /// minor release; any change is called out in the changelog.
    #[serde(default)]
    pub write_mode: faucet_common_file::write::FileWriteMode,
    /// Object format (default: `json_lines`). Set to `parquet` (with the
    /// `arrow` feature) to write Parquet objects and enable the columnar
    /// fast path.
    #[serde(default)]
    pub format: S3SinkFormat,
    /// AWS region. `None` uses the SDK default.
    pub region: Option<String>,
    /// Custom endpoint URL for S3-compatible services (e.g. MinIO).
    pub endpoint_url: Option<String>,
    /// File extension for written objects (default: `.jsonl`). Not used when
    /// `path` is set.
    #[serde(default = "default_file_extension")]
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
    /// data. It also bounds the scratch disk each object in a format other
    /// than JSON Lines or raw text needs while it is built. Counted on the
    /// records' JSON length before any `compression` codec (on the columnar
    /// Parquet path, their in-memory Arrow size), so the threshold means the
    /// same thing whatever the codec. `None` (the default) removes the byte
    /// cap.
    ///
    /// A single record larger than the cap still gets its own object rather
    /// than being split (which would corrupt it) or dropped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes_per_file: Option<usize>,
    /// Maximum number of uploads in flight (default: 10). A page that rolls
    /// into several objects uploads them concurrently, up to this many, while
    /// the next object is encoded; a large object's multipart parts are also
    /// uploaded up to this many at a time. `flush` returns only after every
    /// upload has landed.
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    /// Records per S3 object when `max_records_per_file` is not set (without
    /// `path`). Records accumulate across
    /// [`Sink::write_batch`](faucet_core::Sink::write_batch) calls and an
    /// object closes once it holds `batch_size` records, or at `flush`.
    /// Defaults to [`DEFAULT_BATCH_SIZE`].
    ///
    /// `batch_size = 0` is the "no re-chunking" sentinel: no record cap
    /// (still honouring `max_records_per_file` / `max_bytes_per_file` if
    /// set). JSON Lines and the whole-object formats then write one object
    /// per `flush`; Parquet writes one object per `write_batch` call, as the
    /// sink always has. Many tiny S3 objects are a well-known anti-pattern
    /// (per-request overhead, slower downstream reads, LIST/PUT cost), so
    /// size objects with the caps rather than a small `batch_size`.
    ///
    /// When both `batch_size > 0` and `max_records_per_file` are set, the
    /// effective per-object cap is `min(batch_size, max_records_per_file)`.
    /// Ignored when `path` is set.
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
    /// CSV dialect, used when `format: csv` (#604).
    #[serde(default)]
    pub csv: faucet_core::CsvOptions,
    /// Worksheet name, used when `format: xlsx` (#604).
    #[serde(default)]
    pub excel: faucet_core::ExcelOptions,
    /// Record framing, used when `format: xml` (#604).
    #[serde(default)]
    pub xml: faucet_core::XmlOptions,
    /// Writer schema and block codec, used when `format: avro` (#719).
    #[serde(default)]
    pub avro: faucet_core::AvroOptions,
    /// Parquet writer options, used when `format: parquet` (#777):
    /// `compression` (default `zstd`), `row_group_size`, explicit `schema`.
    ///
    /// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
    /// minor release; any change is called out in the changelog.
    #[serde(default)]
    pub parquet: faucet_common_file::write::ParquetOptions,
    /// JSON Lines writer options (`pretty`), used when `format: json_lines`.
    ///
    /// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
    /// minor release; any change is called out in the changelog.
    #[serde(default)]
    pub json_lines: faucet_common_file::write::JsonLinesOptions,
    /// Encrypt objects at rest (#777; the `encryption` feature). Scratch
    /// files are not encrypted while the run is in progress; those holding
    /// plaintext are kept in a private directory.
    ///
    /// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
    /// minor release; any change is called out in the changelog.
    #[cfg(feature = "encryption")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encryption: Option<faucet_core::EncryptionSpec>,
    /// Directory the sink builds each object in before uploading it
    /// (default: the system temporary directory). JSON Lines and raw text
    /// go up in parts as they are written and need no scratch space; every
    /// other format needs room for each object being built (up to
    /// `concurrency` of them while uploads are in flight). A private
    /// subdirectory is created in it for each sink.
    ///
    /// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
    /// minor release; any change is called out in the changelog.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scratch_dir: Option<String>,
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

fn default_file_extension() -> String {
    ".jsonl".to_string()
}

fn default_concurrency() -> usize {
    10
}

impl S3SinkConfig {
    /// The per-format option blocks in the shape
    /// [`faucet_core::file_format::encode`] wants.
    pub(crate) fn format_options(&self) -> faucet_core::FormatOptions {
        faucet_core::FormatOptions {
            csv: self.csv.clone(),
            excel: self.excel.clone(),
            xml: self.xml.clone(),
            avro: self.avro.clone(),
            orc: faucet_core::OrcOptions::default(),
        }
    }

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
            csv: faucet_core::CsvOptions::default(),
            excel: faucet_core::ExcelOptions::default(),
            xml: faucet_core::XmlOptions::default(),
            avro: faucet_core::AvroOptions::default(),
            path: None,
            mode: faucet_common_file::write::IfExists::default(),
            scratch_dir: None,
            write_mode: faucet_common_file::write::FileWriteMode::default(),
            parquet: faucet_common_file::write::ParquetOptions::default(),
            json_lines: faucet_common_file::write::JsonLinesOptions::default(),
            #[cfg(feature = "encryption")]
            encryption: None,
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

    /// Set the Avro writer schema and codec used when `format: avro` (#719).
    pub fn avro(mut self, avro: faucet_core::AvroOptions) -> Self {
        self.avro = avro;
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

    /// Validate the config at construction time. An empty `bucket` (a typo
    /// or an unset `${env:…}`) is refused with a typed `FaucetError::Config`
    /// rather than surfacing as an opaque cloud-API failure on the first
    /// upload; `batch_size` and every write option are checked by the shared
    /// writer's rules.
    pub fn validate(&self) -> Result<(), faucet_core::FaucetError> {
        if self.bucket.trim().is_empty() {
            return Err(faucet_core::FaucetError::Config(
                "S3 sink `bucket` must not be empty".to_owned(),
            ));
        }
        faucet_core::validate_batch_size(self.batch_size)?;
        self.write_config().validate()
    }

    /// This config's write fields in the shared writer's shape, mapped by
    /// the same rules as every other file-writing sink (#783).
    #[allow(clippy::needless_update)]
    pub fn write_config(&self) -> faucet_common_file::write::WriteConfig {
        faucet_common_file::write::WriteConfig {
            connector: "S3 sink",
            path_field: "path",
            prefix: self.prefix.clone(),
            path: self.path.clone(),
            file_extension: self.file_extension.clone(),
            format: self.format.explicit(),
            #[cfg(feature = "compression")]
            compression: self.compression,
            opts: self.format_options(),
            parquet: self.parquet.clone(),
            default_parquet_codec: faucet_common_file::write::ParquetCodec::Zstd,
            json_lines: self.json_lines.clone(),
            if_exists: self.mode,
            write_mode: self.write_mode,
            max_records_per_file: self.max_records_per_file,
            max_bytes_per_file: self.max_bytes_per_file,
            batch_size: Some(self.batch_size),
            object_per_flush: true,
            #[cfg(feature = "encryption")]
            encryption: self.encryption.clone(),
            ..Default::default()
        }
    }

    /// The shared writer's settings for this config (#777).
    pub fn settings(
        &self,
    ) -> Result<faucet_common_file::write::WriteSettings, faucet_core::FaucetError> {
        self.write_config().settings()
    }

    /// What a failed batch write leaves behind (#737): a page is encoded
    /// locally and published only at a rollover or flush, so without a cap a
    /// failed write publishes nothing; with one, objects closed earlier stay.
    pub fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        self.write_config().batch_atomicity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_writer_settings_follow_path_and_mode_rules() {
        let mut c = S3SinkConfig::new("b");
        let s = c.settings().unwrap();
        assert_eq!(s.format, faucet_core::FileFormat::JsonLines);
        assert!(s.object_per_flush);
        c.path = Some("d/part-{part}.txt".into());
        c.format = S3SinkFormat::Auto;
        c.max_records_per_file = Some(7);
        let s = c.settings().unwrap();
        assert_eq!(s.format, faucet_core::FileFormat::RawText);
        assert_eq!(s.max_records_per_file, Some(7));
        assert!(c.validate().is_ok());
        c.path = Some("{part}-{part}.jsonl".into());
        assert!(
            c.validate()
                .unwrap_err()
                .to_string()
                .contains("more than one")
        );
        c.path = Some("x.unknownext".into());
        assert!(c.settings().is_err());
        c.path = None;
        c.format = S3SinkFormat::JsonLines;
        c.write_mode = faucet_common_file::write::FileWriteMode::Overwrite;
        assert!(c.settings().unwrap_err().to_string().contains("need"));
        c.write_mode = faucet_common_file::write::FileWriteMode::Append;
        c.mode = faucet_common_file::write::IfExists::Append;
        assert!(c.validate().is_err());
        c.mode = faucet_common_file::write::IfExists::Replace;
        c.batch_size = 0;
        c.max_records_per_file = None;
        assert_eq!(c.settings().unwrap().max_records_per_file, None);
        assert_eq!(c.batch_atomicity(), faucet_core::BatchAtomicity::Atomic);
        c.max_records_per_file = Some(3);
        assert_eq!(c.batch_atomicity(), faucet_core::BatchAtomicity::BestEffort);
        let v: serde_json::Value = serde_json::to_value(&c).unwrap();
        assert!(v.get("parquet").is_some() && v.get("json_lines").is_some());
    }

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

    // ── file formats (#604) ───────────────────────────────────────────────

    /// Only JSON Lines can be appended a record at a time. That predicate
    /// routes a write between the streaming byte accumulator and the buffered
    /// record one, so a wrong answer silently changes how objects are built.
    /// Every variant maps onto exactly one shared format, so what this sink
    /// writes is what the file sources read back.
    #[test]
    fn every_format_maps_onto_the_shared_vocabulary() {
        assert_eq!(
            S3SinkFormat::JsonLines.shared(),
            Some(faucet_core::FileFormat::JsonLines)
        );
        assert_eq!(
            S3SinkFormat::JsonArray.shared(),
            Some(faucet_core::FileFormat::JsonArray)
        );
        #[cfg(feature = "file-format-csv")]
        assert_eq!(
            S3SinkFormat::Csv.shared(),
            Some(faucet_core::FileFormat::Csv)
        );
        #[cfg(feature = "file-format-xml")]
        assert_eq!(
            S3SinkFormat::Xml.shared(),
            Some(faucet_core::FileFormat::Xml)
        );
        #[cfg(feature = "file-format-excel")]
        assert_eq!(
            S3SinkFormat::Xlsx.shared(),
            Some(faucet_core::FileFormat::Xlsx)
        );
        // Parquet is the one variant that is NOT the shared encoder's: it has
        // its own Arrow writer, and routing it through `encode` would produce
        // a JSON body under a `.parquet` key.
        #[cfg(feature = "arrow")]
        assert_eq!(S3SinkFormat::Parquet.shared(), None);
    }

    #[test]
    fn the_format_option_blocks_survive_the_builders() {
        let cfg = S3SinkConfig::new("b")
            .format(S3SinkFormat::JsonArray)
            .csv(faucet_core::CsvOptions {
                delimiter: ";".into(),
                has_headers: false,
                ..Default::default()
            })
            .excel(faucet_core::ExcelOptions {
                sheet: Some("Data".into()),
                header_row: 2,
            })
            .xml(faucet_core::XmlOptions {
                record_element: "row".into(),
                root_element: "rows".into(),
            });
        assert_eq!(cfg.format, S3SinkFormat::JsonArray);
        let opts = cfg.format_options();
        assert_eq!(opts.csv.delimiter, ";");
        assert!(!opts.csv.has_headers);
        assert_eq!(opts.excel.sheet.as_deref(), Some("Data"));
        assert_eq!(opts.excel.header_row, 2);
        assert_eq!(opts.xml.record_element, "row");
        assert_eq!(opts.xml.root_element, "rows");
    }

    /// `effective_chunk_cap` resolves `batch_size` against
    /// `max_records_per_file`; `0` means "no limit on this axis" on both, so
    /// the four combinations are genuinely different answers and a wrong one
    /// silently changes object size.
    #[test]
    fn the_effective_chunk_cap_covers_the_whole_lattice() {
        let base = S3SinkConfig::new("b");
        let with = |bs: usize, max: Option<usize>| {
            let mut c = base.clone();
            c.batch_size = bs;
            c.max_records_per_file = max;
            c.effective_chunk_cap()
        };
        assert_eq!(with(0, None), None, "neither axis caps: one object");
        assert_eq!(with(0, Some(0)), None, "an explicit zero cap is no cap");
        assert_eq!(with(0, Some(500)), Some(500), "the record cap alone");
        assert_eq!(with(100, None), Some(100), "batch_size alone");
        assert_eq!(with(100, Some(0)), Some(100), "a zero record cap defers");
        // Both are caps, so the tighter one binds — in either direction.
        assert_eq!(with(100, Some(500)), Some(100));
        assert_eq!(with(500, Some(100)), Some(100));
    }

    #[test]
    fn batch_atomicity_matches_the_write_path() {
        #[cfg(feature = "arrow")]
        {
            let c: S3SinkConfig = serde_json::from_value(serde_json::json!({"bucket": "b", "prefix": "p/", "file_extension": ".parquet", "concurrency": 10, "format": "parquet", "batch_size": 0})).unwrap();
            assert_eq!(c.batch_atomicity(), faucet_core::BatchAtomicity::Atomic);
        }
        let c: S3SinkConfig =
            serde_json::from_value(serde_json::json!({"bucket": "b", "prefix": "", "file_extension": ".jsonl", "concurrency": 10})).unwrap();
        assert_eq!(c.batch_atomicity(), faucet_core::BatchAtomicity::BestEffort);
    }

    #[cfg(feature = "file-format-avro")]
    #[test]
    fn the_avro_block_reaches_the_encoder() {
        let cfg =
            S3SinkConfig::new("b")
                .format(S3SinkFormat::Avro)
                .avro(faucet_core::AvroOptions {
                    schema: None,
                    codec: faucet_core::AvroCodec::Snappy,
                });
        assert_eq!(cfg.format.shared(), Some(faucet_core::FileFormat::Avro));
        assert_eq!(
            cfg.format_options().avro.codec,
            faucet_core::AvroCodec::Snappy
        );
    }
}

#[cfg(test)]
mod object_rules_tests {
    use super::*;
    use faucet_common_file::write::ParquetCodec;

    #[test]
    fn parquet_defaults_to_zstd_even_when_other_options_are_given() {
        let c: S3SinkConfig = serde_json::from_value(serde_json::json!({"bucket":"b"})).unwrap();
        assert_eq!(c.parquet.compression, None);
        assert_eq!(c.settings().unwrap().parquet_codec(), ParquetCodec::Zstd);
        let c: S3SinkConfig = serde_json::from_value(
            serde_json::json!({"bucket":"b","parquet":{"row_group_size":5}}),
        )
        .unwrap();
        assert_eq!(c.parquet.row_group_size, 5);
        let s = c.settings().unwrap();
        assert_eq!(s.parquet_codec(), ParquetCodec::Zstd);
        assert_eq!(s.parquet.row_group_size, 5);
    }

    #[test]
    fn batch_size_zero_writes_a_parquet_object_per_batch_write() {
        let mut c = S3SinkConfig::new("b");
        c.format = S3SinkFormat::Parquet;
        c.batch_size = 0;
        assert!(c.settings().unwrap().object_per_write);
        c.format = S3SinkFormat::JsonLines;
        assert!(
            !c.settings().unwrap().object_per_write,
            "json lines: per flush"
        );
        c.format = S3SinkFormat::Parquet;
        c.batch_size = 10;
        assert!(!c.settings().unwrap().object_per_write, "a record cap");
        c.batch_size = 0;
        c.max_bytes_per_file = Some(10);
        assert!(!c.settings().unwrap().object_per_write, "a byte cap");
        c.max_bytes_per_file = None;
        c.max_records_per_file = Some(3);
        assert!(!c.settings().unwrap().object_per_write, "a record cap");
        c.max_records_per_file = None;
        c.path = Some("d/part-{part}.parquet".into());
        assert!(!c.settings().unwrap().object_per_write, "path: per part");
    }

    #[test]
    fn auto_has_no_shared_format_and_a_bad_config_is_best_effort() {
        assert_eq!(S3SinkFormat::Auto.shared(), None);
        let mut c = S3SinkConfig::new("b");
        c.write_mode = faucet_common_file::write::FileWriteMode::Overwrite;
        assert!(c.settings().is_err());
        assert_eq!(c.batch_atomicity(), faucet_core::BatchAtomicity::BestEffort);
    }
}
