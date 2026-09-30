//! Azure Blob sink configuration.

use faucet_common_azure::{AzureConnection, AzureCredentials};
use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// On-the-wire format of objects written by the Azure Blob sink (#604).
///
/// Only [`JsonLines`](Self::JsonLines) can be built a record at a time; every
/// other format has a header, a wrapper, or a container index, so its records
/// are buffered and encoded together at the rollover.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AzureSinkFormat {
    /// Newline-delimited JSON — one JSON record per line (the default).
    #[default]
    JsonLines,
    /// A single JSON array per object.
    JsonArray,
    /// Delimited text. Columns are the union of every record's keys; dialect
    /// from `csv`. Requires `file-format-csv`.
    #[cfg(feature = "file-format-csv")]
    Csv,
    /// XML, one element per record. Framing from `xml`. Requires
    /// `file-format-xml`.
    #[cfg(feature = "file-format-xml")]
    Xml,
    /// An Excel workbook. Sheet name from `excel`. Requires
    /// `file-format-excel`.
    #[cfg(feature = "file-format-excel")]
    Xlsx,
    /// An Apache Avro Object Container File, against `avro.schema` or a
    /// schema inferred from the object's records; block codec from
    /// `avro.codec`. Requires `file-format-avro` (#719). There is no ORC
    /// variant: ORC is read-only.
    #[cfg(feature = "file-format-avro")]
    Avro,
    /// Apache Parquet: each object is a complete, self-contained Parquet
    /// file, written with the `parquet` options; enables the columnar fast
    /// path. Requires the `arrow` feature (#777).
    #[cfg(feature = "arrow")]
    Parquet,
    /// Unparsed text: each record's `content` field (or the record as JSON)
    /// on its own line (#777).
    RawText,
    /// Take the format from the object name's extension — `path`'s, else
    /// `file_extension` — looking through a compression suffix (#777).
    Auto,
}

impl AzureSinkFormat {
    /// The shared format this variant maps onto.
    pub(crate) fn shared(self) -> faucet_core::FileFormat {
        match self {
            Self::JsonLines => faucet_core::FileFormat::JsonLines,
            Self::JsonArray => faucet_core::FileFormat::JsonArray,
            #[cfg(feature = "file-format-csv")]
            Self::Csv => faucet_core::FileFormat::Csv,
            #[cfg(feature = "file-format-xml")]
            Self::Xml => faucet_core::FileFormat::Xml,
            #[cfg(feature = "file-format-excel")]
            Self::Xlsx => faucet_core::FileFormat::Xlsx,
            #[cfg(feature = "file-format-avro")]
            Self::Avro => faucet_core::FileFormat::Avro,
            #[cfg(feature = "arrow")]
            Self::Parquet => faucet_core::FileFormat::Parquet,
            Self::RawText => faucet_core::FileFormat::RawText,
            Self::Auto => faucet_core::FileFormat::JsonLines,
        }
    }

    /// The explicit format, or `None` for `auto` (taken from the name's
    /// extension).
    pub(crate) fn explicit(self) -> Option<faucet_core::FileFormat> {
        match self {
            Self::Auto => None,
            other => Some(other.shared()),
        }
    }
}

/// Configuration for the Azure Blob sink connector.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(extend("x-faucet-aliases" = ["mode"]))]
pub struct AzureBlobSinkConfig {
    /// Azure connection (container, account, credentials, endpoint, …).
    #[serde(flatten)]
    pub connection: AzureConnection,
    /// Object-name prefix for written objects.
    #[serde(default)]
    pub prefix: String,
    /// Object key template inside the container, after `prefix` — the file
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
    /// What to do when a blob of the same name already exists
    /// (`if_exists`): `replace` it (default), `append` to it (JSON Lines, CSV,
    /// raw text) or fail with `error` (#777). Needs `path`. `mode` is
    /// accepted as another name for this key, and `overwrite` /
    /// `error_if_exists` for its values.
    ///
    /// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
    /// minor release; any change is called out in the changelog.
    #[serde(default, rename = "if_exists", alias = "mode")]
    pub mode: faucet_common_file::write::IfExists,
    /// `append` (default) or `overwrite`: write the run's blobs into a
    /// hidden swap area and move them into place only after a successful
    /// run, then remove blobs of an earlier run that match `path` and
    /// were not rewritten (#777). The move is one blob at a time, so a
    /// reader listing the destination while it runs can see new blobs beside
    /// old ones; a move that was interrupted is finished by the next run.
    /// Needs `path`.
    ///
    /// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
    /// minor release; any change is called out in the changelog.
    #[serde(default)]
    pub write_mode: faucet_common_file::write::FileWriteMode,
    /// Object format (default `json_lines`) (#604).
    #[serde(default)]
    pub format: AzureSinkFormat,
    /// File extension for written objects (default `.jsonl`).
    #[serde(default = "default_file_extension")]
    pub file_extension: String,
    /// Hard cap on records per uploaded object. `None` means a single object
    /// per `write_batch` call (still subject to `batch_size`).
    pub max_records_per_file: Option<usize>,
    /// Maximum **bytes** per object before rolling to a new one (#618).
    ///
    /// Rows are a poor proxy for object size, so a rows-only cap either writes
    /// tiny objects for narrow data or unbounded ones for wide data. Counted
    /// on the records' JSON length before any `compression` codec (on the
    /// columnar Parquet path, their in-memory Arrow size). `None` (the
    /// default) removes the byte cap; a single record larger than the cap
    /// still gets its own object rather than being split or dropped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes_per_file: Option<usize>,
    /// Maximum number of uploads in flight (default 10): a page that rolls
    /// into several objects uploads them concurrently while the next object
    /// is encoded, and a large blob's blocks go up this many at a time.
    /// `flush` returns only after every upload has landed.
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    /// Records per object when `max_records_per_file` is not set (without
    /// `path`). Records accumulate across `write_batch` calls and an object
    /// closes once it holds this many records, or at `flush`. `batch_size =
    /// 0` is the "no re-chunking" sentinel: no record cap, so JSON Lines and
    /// the whole-object formats write one object per `flush`, and Parquet one
    /// object per `write_batch` call. Ignored when `path` is set.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Compression codec applied to each uploaded object body. Defaults to
    /// [`CompressionConfig::Auto`](faucet_core::CompressionConfig::Auto) —
    /// resolves against `file_extension` (so `.jsonl.gz` triggers gzip).
    /// Requires the crate-local `compression` feature. Note: this sink does
    /// **not** set any `Content-Encoding` metadata, so consumers must
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
    /// Encrypt blobs at rest (#777; the `encryption` feature). Scratch
    /// files are not encrypted while the run is in progress; those holding
    /// plaintext are kept in a private directory.
    ///
    /// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
    /// minor release; any change is called out in the changelog.
    #[cfg(feature = "encryption")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encryption: Option<faucet_core::EncryptionSpec>,
    /// Directory the sink builds each blob in before uploading it
    /// (default: the system temporary directory). JSON Lines and raw text
    /// go up in parts as they are written and need no scratch space; every
    /// other format needs room for each blob being built (up to
    /// `concurrency` of them while uploads are in flight). A private
    /// subdirectory is created in it for each sink.
    ///
    /// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
    /// minor release; any change is called out in the changelog.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scratch_dir: Option<String>,
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

impl AzureBlobSinkConfig {
    /// Create a new config for `container` with sensible defaults.
    pub fn new(container: impl Into<String>) -> Self {
        Self {
            connection: AzureConnection::new(container),
            prefix: String::new(),
            format: AzureSinkFormat::default(),
            file_extension: default_file_extension(),
            max_records_per_file: None,
            max_bytes_per_file: None,
            concurrency: default_concurrency(),
            batch_size: default_batch_size(),
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

    /// Set the object format (#604).
    pub fn format(mut self, format: AzureSinkFormat) -> Self {
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

    /// Set the storage-account name.
    pub fn account(mut self, account: impl Into<String>) -> Self {
        self.connection = self.connection.account(account);
        self
    }

    /// Set the credential source.
    pub fn auth(mut self, creds: AzureCredentials) -> Self {
        self.connection = self.connection.auth(creds);
        self
    }

    /// Set a custom blob endpoint (emulator / sovereign cloud).
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.connection = self.connection.endpoint(endpoint);
        self
    }

    /// Permit plaintext HTTP (required for the Azurite emulator).
    pub fn allow_http(mut self, allow: bool) -> Self {
        self.connection = self.connection.allow_http(allow);
        self
    }

    /// Target the Azurite emulator.
    pub fn use_emulator(mut self, use_emulator: bool) -> Self {
        self.connection = self.connection.use_emulator(use_emulator);
        self
    }

    /// Set the written-object name prefix.
    pub fn prefix(mut self, p: impl Into<String>) -> Self {
        self.prefix = p.into();
        self
    }

    /// Set the written-object file extension.
    pub fn file_extension(mut self, ext: impl Into<String>) -> Self {
        self.file_extension = ext.into();
        self
    }

    /// Cap records per uploaded object.
    pub fn max_bytes_per_file(mut self, n: usize) -> Self {
        self.max_bytes_per_file = Some(n);
        self
    }

    /// Set the per-object record cap.
    pub fn max_records_per_file(mut self, n: usize) -> Self {
        self.max_records_per_file = Some(n);
        self
    }

    /// Set the maximum concurrent uploads.
    pub fn concurrency(mut self, n: usize) -> Self {
        self.concurrency = n;
        self
    }

    /// Set the records-per-object `batch_size`.
    pub fn with_batch_size(mut self, n: usize) -> Self {
        self.batch_size = n;
        self
    }

    /// Set the compression codec. Available only with the `compression` feature.
    #[cfg(feature = "compression")]
    pub fn compression(mut self, c: faucet_core::CompressionConfig) -> Self {
        self.compression = c;
        self
    }

    /// The container name.
    pub fn container(&self) -> &str {
        &self.connection.container
    }
}

impl AzureBlobSinkConfig {
    /// Validate the config at construction time: `batch_size` and every
    /// write option are checked by the shared writer's rules.
    pub fn validate(&self) -> Result<(), faucet_core::FaucetError> {
        faucet_core::validate_batch_size(self.batch_size)?;
        self.write_config().validate()
    }

    /// This config's write fields in the shared writer's shape, mapped by
    /// the same rules as every other file-writing sink (#783).
    pub fn write_config(&self) -> faucet_common_file::write::WriteConfig {
        faucet_common_file::write::WriteConfig {
            connector: "azure-blob sink",
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
    /// failed write publishes nothing; with one, blobs closed earlier stay.
    pub fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        self.write_config().batch_atomicity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_writer_settings_follow_path_and_mode_rules() {
        let mut c = AzureBlobSinkConfig::new("c");
        let s = c.settings().unwrap();
        assert_eq!(s.format, faucet_core::FileFormat::JsonLines);
        assert!(s.object_per_flush);
        c.path = Some("d/part-{part}.txt".into());
        c.format = AzureSinkFormat::Auto;
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
        c.format = AzureSinkFormat::JsonLines;
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
    fn defaults() {
        let c = AzureBlobSinkConfig::new("cont");
        assert_eq!(c.container(), "cont");
        assert_eq!(c.prefix, "");
        assert_eq!(c.connection.auth, AzureCredentials::Default);
        assert_eq!(c.file_extension, ".jsonl");
        assert!(c.max_records_per_file.is_none());
        assert_eq!(c.concurrency, 10);
        assert_eq!(c.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn builder_methods() {
        let c = AzureBlobSinkConfig::new("cont")
            .account("acct")
            .prefix("out/")
            .file_extension(".ndjson")
            .max_records_per_file(500)
            .concurrency(4)
            .with_batch_size(0)
            .auth(AzureCredentials::SasToken {
                sas_token: "sv=x".into(),
            });
        assert_eq!(c.connection.account.as_deref(), Some("acct"));
        assert_eq!(c.prefix, "out/");
        assert_eq!(c.file_extension, ".ndjson");
        assert_eq!(c.max_records_per_file, Some(500));
        assert_eq!(c.concurrency, 4);
        assert_eq!(c.batch_size, 0);
        assert!(matches!(
            c.connection.auth,
            AzureCredentials::SasToken { .. }
        ));
    }

    #[test]
    fn batch_size_sentinel_accepted_and_above_max_rejected() {
        assert!(faucet_core::validate_batch_size(0).is_ok());
        assert!(faucet_core::validate_batch_size(faucet_core::MAX_BATCH_SIZE + 1).is_err());
    }

    #[test]
    fn deserializes_flattened_connection() {
        let json = r#"{
            "container": "cont",
            "account": "acct",
            "prefix": "out/",
            "auth": { "type": "sas_token", "config": { "sas_token": "sv=x" } }
        }"#;
        let c: AzureBlobSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(c.container(), "cont");
        assert_eq!(c.prefix, "out/");
        assert_eq!(c.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
        assert!(matches!(
            c.connection.auth,
            AzureCredentials::SasToken { .. }
        ));
    }

    #[test]
    fn schema_generates_without_panicking() {
        let _ = faucet_core::schema_for!(AzureBlobSinkConfig);
    }

    #[cfg(feature = "compression")]
    #[test]
    fn compression_default_is_auto() {
        let cfg = AzureBlobSinkConfig::new("cont");
        assert_eq!(cfg.compression, faucet_core::CompressionConfig::Auto);
    }

    // ── file formats (#604) ───────────────────────────────────────────────

    /// Every variant maps onto exactly one shared format, so what this sink
    /// writes is what the file sources read back.
    #[test]
    fn every_format_maps_onto_the_shared_vocabulary() {
        assert_eq!(
            AzureSinkFormat::JsonLines.shared(),
            faucet_core::FileFormat::JsonLines
        );
        assert_eq!(
            AzureSinkFormat::JsonArray.shared(),
            faucet_core::FileFormat::JsonArray
        );
        #[cfg(feature = "file-format-csv")]
        assert_eq!(AzureSinkFormat::Csv.shared(), faucet_core::FileFormat::Csv);
        #[cfg(feature = "file-format-xml")]
        assert_eq!(AzureSinkFormat::Xml.shared(), faucet_core::FileFormat::Xml);
        #[cfg(feature = "file-format-excel")]
        assert_eq!(
            AzureSinkFormat::Xlsx.shared(),
            faucet_core::FileFormat::Xlsx
        );
    }

    #[test]
    fn the_format_option_blocks_survive_the_builders() {
        let cfg = AzureBlobSinkConfig::new("c")
            .format(AzureSinkFormat::JsonArray)
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
        assert_eq!(cfg.format, AzureSinkFormat::JsonArray);
        let opts = cfg.format_options();
        assert_eq!(opts.csv.delimiter, ";");
        assert!(!opts.csv.has_headers);
        assert_eq!(opts.excel.sheet.as_deref(), Some("Data"));
        assert_eq!(opts.excel.header_row, 2);
        assert_eq!(opts.xml.record_element, "row");
        assert_eq!(opts.xml.root_element, "rows");
    }

    #[test]
    fn batch_atomicity_matches_the_write_path() {
        let c: AzureBlobSinkConfig = serde_json::from_value(serde_json::json!({"container": "c", "account": "a", "prefix": "p/", "auth": {"type": "sas_token", "config": {"sas_token": "sv=x"}}})).unwrap();
        assert_eq!(c.batch_atomicity(), faucet_core::BatchAtomicity::BestEffort);
    }

    #[cfg(feature = "file-format-avro")]
    #[test]
    fn the_avro_block_reaches_the_encoder() {
        let cfg = AzureBlobSinkConfig::new("c")
            .format(AzureSinkFormat::Avro)
            .avro(faucet_core::AvroOptions {
                schema: None,
                codec: faucet_core::AvroCodec::Snappy,
            });
        assert_eq!(cfg.format.shared(), faucet_core::FileFormat::Avro);
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
        let c: AzureBlobSinkConfig = serde_json::from_value(serde_json::json!({"container":"c","account":"a","auth":{"type":"sas_token","config":{"sas_token":"sv=x"}}})).unwrap();
        assert_eq!(c.parquet.compression, None);
        assert_eq!(c.settings().unwrap().parquet_codec(), ParquetCodec::Zstd);
        let c: AzureBlobSinkConfig = serde_json::from_value(
            serde_json::json!({"container":"c","account":"a","auth":{"type":"sas_token","config":{"sas_token":"sv=x"}},"parquet":{"row_group_size":5}}),
        )
        .unwrap();
        assert_eq!(c.parquet.row_group_size, 5);
        let s = c.settings().unwrap();
        assert_eq!(s.parquet_codec(), ParquetCodec::Zstd);
        assert_eq!(s.parquet.row_group_size, 5);
    }

    #[test]
    fn batch_size_zero_writes_a_parquet_object_per_batch_write() {
        let mut c = AzureBlobSinkConfig::new("c");
        c.format = AzureSinkFormat::Parquet;
        c.batch_size = 0;
        assert!(c.settings().unwrap().object_per_write);
        c.format = AzureSinkFormat::JsonLines;
        assert!(
            !c.settings().unwrap().object_per_write,
            "json lines: per flush"
        );
        c.format = AzureSinkFormat::Parquet;
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
    fn every_variant_has_a_shared_format_and_caps_combine() {
        #[cfg(feature = "arrow")]
        assert_eq!(
            AzureSinkFormat::Parquet.shared(),
            faucet_core::FileFormat::Parquet
        );
        assert_eq!(
            AzureSinkFormat::Auto.shared(),
            faucet_core::FileFormat::JsonLines
        );
        let mut c = AzureBlobSinkConfig::new("c");
        c.batch_size = 10;
        c.max_records_per_file = Some(4);
        assert_eq!(c.settings().unwrap().max_records_per_file, Some(4));
        c.write_mode = faucet_common_file::write::FileWriteMode::Overwrite;
        assert!(c.settings().is_err());
        assert_eq!(c.batch_atomicity(), faucet_core::BatchAtomicity::BestEffort);
    }
}
