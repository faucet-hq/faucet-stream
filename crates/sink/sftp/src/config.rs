//! SFTP sink configuration.

use faucet_common_sftp::SftpConnectionConfig;
use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// On-the-wire format of files written by the SFTP sink (#604).
///
/// Only [`JsonLines`](Self::JsonLines) can be built a record at a time; every
/// other format has a header, a wrapper, or a container index, so its records
/// are buffered and encoded together at the rollover.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SftpSinkFormat {
    /// Newline-delimited JSON — one JSON record per line (the default).
    #[default]
    JsonLines,
    /// A single JSON array per file.
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
    /// Apache Parquet, written with the `parquet` options; enables the
    /// columnar fast path. Requires the `arrow` feature (#777).
    #[cfg(feature = "arrow")]
    Parquet,
    /// Unparsed text: each record's `content` field (or the record as JSON)
    /// on its own line (#777).
    RawText,
    /// Take the format from the file name's extension — `file_name`'s, else
    /// `file_extension` — looking through a compression suffix (#777).
    Auto,
}

impl SftpSinkFormat {
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

/// Configuration for the SFTP sink connector.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(extend("x-faucet-aliases" = ["mode"]))]
pub struct SftpSinkConfig {
    /// Shared SFTP connection settings (host, port, username, auth, host-key
    /// policy). Flattened, so its fields sit at the top level of the config.
    #[serde(flatten)]
    pub connection: SftpConnectionConfig,
    /// Remote directory the files are written in.
    pub path: String,
    /// File-name template inside `path` — the file sink's `path` file name
    /// (#777). May contain `{part}` (numbered files) and `${now.*}` tokens; a
    /// trailing `/` is a subdirectory of `part-{part}<extension>` files. When
    /// set, `file_extension` is not used. Unset: every run writes new files
    /// named `<run id>-<part><file_extension>` (`<run id>` is a fresh
    /// time-ordered UUID, `<part>` is `00001`, `00002`, …).
    ///
    /// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
    /// minor release; any change is called out in the changelog.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_name: Option<String>,
    /// What to do when a file of the same name already exists
    /// (`if_exists`): `replace` it (default), `append` to it (JSON Lines, CSV,
    /// raw text) or fail with `error` (#777). Needs `file_name`. `mode` is
    /// accepted as another name for this key, and `overwrite` /
    /// `error_if_exists` for its values.
    ///
    /// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
    /// minor release; any change is called out in the changelog.
    #[serde(default, rename = "if_exists", alias = "mode")]
    pub mode: faucet_common_file::write::IfExists,
    /// `append` (default) or `overwrite`: write the run's files into a
    /// hidden swap area and move them into place only after a successful
    /// run, then remove files of an earlier run that match `file_name` and
    /// were not rewritten (#777). The move is one file at a time, so a
    /// reader listing the destination while it runs can see new files beside
    /// old ones; a move that was interrupted is finished by the next run.
    /// Needs `file_name`.
    ///
    /// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
    /// minor release; any change is called out in the changelog.
    #[serde(default)]
    pub write_mode: faucet_common_file::write::FileWriteMode,
    /// Compression codec (default `auto`: from the file name's extension,
    /// `.gz` / `.zst`). The `compression` feature (#777).
    #[cfg(feature = "compression")]
    #[serde(default)]
    pub compression: faucet_core::CompressionConfig,
    /// File format (default `json_lines`) (#604).
    #[serde(default)]
    pub format: SftpSinkFormat,
    /// File extension for written objects (default: `.jsonl`).
    #[serde(default = "default_file_extension")]
    pub file_extension: String,
    /// Records per file when `max_records_per_file` is not set (without
    /// `file_name`). Records accumulate across `write_batch` calls and a file
    /// closes once it holds this many records, or at `flush`. `batch_size =
    /// 0` is the "no re-chunking" sentinel: no record cap, so JSON Lines and
    /// the whole-file formats write one file per `flush`, and Parquet one
    /// file per `write_batch` call. Ignored when `file_name` is set.
    /// Defaults to [`DEFAULT_BATCH_SIZE`].
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Maximum number of file uploads in flight over the SSH session
    /// (default 4): a page that rolls into several files uploads them
    /// concurrently while the next file is encoded. `flush` returns only
    /// after every upload has landed.
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    /// Maximum records per file before rolling to a new one (#618). `None`
    /// removes the record cap; the sink accumulates across `write_batch`
    /// calls, so a small upstream page no longer means a small file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_records_per_file: Option<usize>,
    /// Maximum **bytes** per file before rolling to a new one (#618).
    ///
    /// Rows are a poor proxy for file size, so a rows-only cap either writes
    /// tiny files for narrow data or unbounded ones for wide data. It also
    /// bounds the scratch disk each file needs while it is built. Counted on
    /// the records' JSON length before any `compression` codec. `None` (the
    /// default) removes the byte cap; a single record larger than the cap
    /// still gets its own file rather than being split.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes_per_file: Option<usize>,
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
    /// Encrypt files at rest (#777; the `encryption` feature). Scratch
    /// files are not encrypted while the run is in progress; those holding
    /// plaintext are kept in a private directory.
    ///
    /// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
    /// minor release; any change is called out in the changelog.
    #[cfg(feature = "encryption")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encryption: Option<faucet_core::EncryptionSpec>,
    /// Directory the sink builds each file in before uploading it
    /// (default: the system temporary directory). JSON Lines and raw text
    /// need scratch space for the whole file; every
    /// other format needs room for each file being built (up to
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

fn default_concurrency() -> usize {
    4
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

impl SftpSinkConfig {
    /// Build a sink config from a connection and a remote directory prefix.
    pub fn new(connection: SftpConnectionConfig, path: impl Into<String>) -> Self {
        Self {
            connection,
            path: path.into(),
            format: SftpSinkFormat::default(),
            file_extension: default_file_extension(),
            batch_size: DEFAULT_BATCH_SIZE,
            concurrency: default_concurrency(),
            max_records_per_file: None,
            max_bytes_per_file: None,
            csv: faucet_core::CsvOptions::default(),
            excel: faucet_core::ExcelOptions::default(),
            xml: faucet_core::XmlOptions::default(),
            avro: faucet_core::AvroOptions::default(),
            file_name: None,
            mode: faucet_common_file::write::IfExists::default(),
            scratch_dir: None,
            write_mode: faucet_common_file::write::FileWriteMode::default(),
            #[cfg(feature = "compression")]
            compression: faucet_core::CompressionConfig::default(),
            parquet: faucet_common_file::write::ParquetOptions::default(),
            json_lines: faucet_common_file::write::JsonLinesOptions::default(),
            #[cfg(feature = "encryption")]
            encryption: None,
        }
    }

    /// Set the file format (#604).
    pub fn format(mut self, format: SftpSinkFormat) -> Self {
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

    /// Set the file extension for written objects.
    pub fn file_extension(mut self, ext: impl Into<String>) -> Self {
        self.file_extension = ext.into();
        self
    }

    /// Set the per-object record count.
    pub fn max_records_per_file(mut self, n: usize) -> Self {
        self.max_records_per_file = Some(n);
        self
    }

    /// Set the per-file byte cap (#618).
    pub fn max_bytes_per_file(mut self, n: usize) -> Self {
        self.max_bytes_per_file = Some(n);
        self
    }

    /// Set the most file uploads in flight.
    pub fn concurrency(mut self, n: usize) -> Self {
        self.concurrency = n;
        self
    }

    /// Set the per-call record chunk size.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

impl SftpSinkConfig {
    /// The remote directory as a key prefix (`path` with a trailing `/`).
    pub(crate) fn dir_prefix(&self) -> String {
        if self.path.is_empty() || self.path.ends_with('/') {
            self.path.clone()
        } else {
            format!("{}/", self.path)
        }
    }

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
            connector: "SFTP sink",
            path_field: "file_name",
            prefix: self.dir_prefix(),
            path: self.file_name.clone(),
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
    /// failed write publishes nothing; with one, files closed earlier stay.
    pub fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        self.write_config().batch_atomicity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_writer_settings_follow_path_and_mode_rules() {
        let mut c = SftpSinkConfig::new(
            faucet_common_sftp::SftpConnectionConfig::with_password("h", "u", "p"),
            "/out",
        );
        let s = c.settings().unwrap();
        assert_eq!(s.format, faucet_core::FileFormat::JsonLines);
        assert!(s.object_per_flush);
        c.file_name = Some("d/part-{part}.txt".into());
        c.format = SftpSinkFormat::Auto;
        c.max_records_per_file = Some(7);
        let s = c.settings().unwrap();
        assert_eq!(s.format, faucet_core::FileFormat::RawText);
        assert_eq!(s.max_records_per_file, Some(7));
        assert!(c.validate().is_ok());
        c.file_name = Some("{part}-{part}.jsonl".into());
        assert!(
            c.validate()
                .unwrap_err()
                .to_string()
                .contains("more than one")
        );
        c.file_name = Some("x.unknownext".into());
        assert!(c.settings().is_err());
        c.file_name = None;
        c.format = SftpSinkFormat::JsonLines;
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
    use faucet_common_sftp::SftpConnectionConfig;

    fn conn() -> SftpConnectionConfig {
        SftpConnectionConfig::with_password("h", "u", "p")
    }

    #[test]
    fn defaults() {
        let cfg = SftpSinkConfig::new(conn(), "/out");
        assert_eq!(cfg.path, "/out");
        assert_eq!(cfg.file_extension, ".jsonl");
        assert_eq!(cfg.batch_size, DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn deserializes_flat_shape() {
        let json = r#"{
            "host": "sftp.example.com",
            "username": "user",
            "type": "password",
            "config": { "password": "secret" },
            "path": "/upload",
            "batch_size": 0
        }"#;
        let cfg: SftpSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.connection.host, "sftp.example.com");
        assert_eq!(cfg.path, "/upload");
        assert_eq!(cfg.batch_size, 0);
        assert_eq!(cfg.file_extension, ".jsonl");
    }

    #[test]
    fn batch_size_zero_is_valid_sentinel() {
        let cfg = SftpSinkConfig::new(conn(), "/o").with_batch_size(0);
        assert!(faucet_core::validate_batch_size(cfg.batch_size).is_ok());
    }

    // ── file formats (#604) ───────────────────────────────────────────────

    /// Every variant maps onto exactly one shared format, so what this sink
    /// writes is what the file sources read back.
    #[test]
    fn every_format_maps_onto_the_shared_vocabulary() {
        assert_eq!(
            SftpSinkFormat::JsonLines.shared(),
            faucet_core::FileFormat::JsonLines
        );
        assert_eq!(
            SftpSinkFormat::JsonArray.shared(),
            faucet_core::FileFormat::JsonArray
        );
        #[cfg(feature = "file-format-csv")]
        assert_eq!(SftpSinkFormat::Csv.shared(), faucet_core::FileFormat::Csv);
        #[cfg(feature = "file-format-xml")]
        assert_eq!(SftpSinkFormat::Xml.shared(), faucet_core::FileFormat::Xml);
        #[cfg(feature = "file-format-excel")]
        assert_eq!(SftpSinkFormat::Xlsx.shared(), faucet_core::FileFormat::Xlsx);
    }

    #[test]
    fn the_format_option_blocks_survive_the_builders() {
        let cfg = SftpSinkConfig::new(SftpConnectionConfig::with_password("h", "u", "p"), "/p")
            .format(SftpSinkFormat::JsonArray)
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
        assert_eq!(cfg.format, SftpSinkFormat::JsonArray);
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
        let base = SftpSinkConfig::new(conn(), "/out");
        assert_eq!(base.max_records_per_file, None);
        assert_eq!(base.max_bytes_per_file, None);

        let records_only = SftpSinkConfig::new(conn(), "/out").max_records_per_file(10);
        assert_eq!(records_only.max_records_per_file, Some(10));
        assert_eq!(records_only.max_bytes_per_file, None);

        let both = SftpSinkConfig::new(conn(), "/out")
            .max_records_per_file(10)
            .max_bytes_per_file(4096)
            .file_extension(".csv");
        assert_eq!(both.max_records_per_file, Some(10));
        assert_eq!(both.max_bytes_per_file, Some(4096));
        assert_eq!(both.file_extension, ".csv");
    }

    #[test]
    fn batch_atomicity_matches_the_write_path() {
        let c: SftpSinkConfig = serde_json::from_value(serde_json::json!({"host": "sftp.example.com", "username": "user", "type": "password", "config": {"password": "secret"}, "path": "/upload"})).unwrap();
        assert_eq!(c.batch_atomicity(), faucet_core::BatchAtomicity::BestEffort);
    }

    #[cfg(feature = "file-format-avro")]
    #[test]
    fn the_avro_block_reaches_the_encoder() {
        let cfg = SftpSinkConfig::new(
            faucet_common_sftp::SftpConnectionConfig::with_password("h", "u", "p"),
            "/d",
        )
        .format(SftpSinkFormat::Avro)
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
        let c: SftpSinkConfig = serde_json::from_value(serde_json::json!({"host":"h","username":"u","type":"password","config":{"password":"p"},"path":"/o"})).unwrap();
        assert_eq!(c.parquet.compression, None);
        assert_eq!(c.settings().unwrap().parquet_codec(), ParquetCodec::Zstd);
        let c: SftpSinkConfig = serde_json::from_value(
            serde_json::json!({"host":"h","username":"u","type":"password","config":{"password":"p"},"path":"/o","parquet":{"row_group_size":5}}),
        )
        .unwrap();
        assert_eq!(c.parquet.row_group_size, 5);
        let s = c.settings().unwrap();
        assert_eq!(s.parquet_codec(), ParquetCodec::Zstd);
        assert_eq!(s.parquet.row_group_size, 5);
    }

    #[test]
    fn batch_size_zero_writes_a_parquet_object_per_batch_write() {
        let mut c = SftpSinkConfig::new(SftpConnectionConfig::with_password("h", "u", "p"), "/o");
        c.format = SftpSinkFormat::Parquet;
        c.batch_size = 0;
        assert!(c.settings().unwrap().object_per_write);
        c.format = SftpSinkFormat::JsonLines;
        assert!(
            !c.settings().unwrap().object_per_write,
            "json lines: per flush"
        );
        c.format = SftpSinkFormat::Parquet;
        c.batch_size = 10;
        assert!(!c.settings().unwrap().object_per_write, "a record cap");
        c.batch_size = 0;
        c.max_bytes_per_file = Some(10);
        assert!(!c.settings().unwrap().object_per_write, "a byte cap");
        c.max_bytes_per_file = None;
        c.max_records_per_file = Some(3);
        assert!(!c.settings().unwrap().object_per_write, "a record cap");
        c.max_records_per_file = None;
        c.file_name = Some("part-{part}.parquet".into());
        assert!(!c.settings().unwrap().object_per_write, "path: per part");
    }

    #[test]
    fn auto_maps_onto_json_lines_and_a_bad_config_is_best_effort() {
        assert_eq!(
            SftpSinkFormat::Auto.shared(),
            faucet_core::FileFormat::JsonLines
        );
        let mut c = SftpSinkConfig::new(
            faucet_common_sftp::SftpConnectionConfig::with_password("h", "u", "p"),
            "/d",
        )
        .concurrency(3);
        assert_eq!(c.concurrency, 3);
        c.max_records_per_file = Some(0);
        c.write_mode = faucet_common_file::write::FileWriteMode::Overwrite;
        c.mode = faucet_common_file::write::IfExists::Append;
        assert!(c.settings().and_then(|s| s.validate()).is_err());
        assert_eq!(c.batch_atomicity(), faucet_core::BatchAtomicity::BestEffort);
    }
}
