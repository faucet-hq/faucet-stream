//! Local file sink configuration.

use faucet_core::{
    AvroOptions, Compression, CompressionConfig, CsvOptions, DEFAULT_BATCH_SIZE, ExcelOptions,
    FaucetError, FileFormat, FormatOptions, OrcOptions, XmlOptions,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use faucet_common_file::{
    appendable as is_appendable, compresses_internally as is_self_compressed,
};

pub use faucet_common_file::FileFormatChoice as FileSinkFormat;

/// The placeholder a path template uses for the rollover part number.
pub const PART_TOKEN: &str = "{part}";

/// What happens when a file this run writes already exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FileMode {
    /// Replace it: the new file is written beside it and renamed over it, so
    /// the old contents stay readable until the new ones are complete.
    #[default]
    Overwrite,
    /// Add to it. JSON Lines, CSV and raw text only — a JSON array, XML,
    /// Excel, Avro or Parquet file cannot be appended to without rewriting.
    /// With rollover, numbering continues after the highest existing part.
    Append,
    /// Fail the run instead of touching an existing file.
    ErrorIfExists,
}

/// Pipeline-level write mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FileWriteMode {
    /// Write this run's files; leave everything else in place.
    #[default]
    Append,
    /// Replace the destination's whole output set: files are staged in a
    /// hidden directory beside the destination and moved into place only when
    /// the run succeeds, then files of an earlier run that match the path
    /// template and were not rewritten are removed. A failed or cancelled run
    /// leaves the previous output untouched.
    Overwrite,
}

/// Parquet column-chunk compression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ParquetCodec {
    /// Uncompressed pages. `uncompressed` is accepted as another name.
    #[serde(alias = "uncompressed")]
    None,
    /// Snappy (the default): fast and universally readable.
    #[default]
    Snappy,
    /// Gzip.
    Gzip,
    /// Zstandard.
    Zstd,
    /// LZ4 (the `LZ4_RAW` codec).
    Lz4,
}

/// Rows per Parquet row group unless `parquet.row_group_size` says otherwise.
pub const DEFAULT_ROW_GROUP_SIZE: usize = 1024 * 1024;

fn default_row_group_size() -> usize {
    DEFAULT_ROW_GROUP_SIZE
}

/// Parquet write options.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ParquetOptions {
    /// Column-chunk compression (default `snappy`). File-level `compression`
    /// does not apply to Parquet.
    #[serde(default)]
    pub compression: ParquetCodec,
    /// Maximum rows per row group (default 1,048,576). Smaller groups let
    /// readers skip more data and bound the writer's memory.
    #[serde(default = "default_row_group_size")]
    pub row_group_size: usize,
    /// An explicit schema, in column order. Without it the schema is
    /// inferred from the records and widened when a later page adds a field.
    /// With it the file has exactly these columns: a record field the schema
    /// does not name fails the write, and a value that does not fit its
    /// column's type fails naming the column.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<Vec<ParquetField>>,
}

impl Default for ParquetOptions {
    fn default() -> Self {
        Self {
            compression: ParquetCodec::default(),
            row_group_size: DEFAULT_ROW_GROUP_SIZE,
            schema: None,
        }
    }
}

/// One column of an explicit Parquet schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ParquetField {
    /// Column name — the record field it is read from.
    pub name: String,
    /// Column type.
    #[serde(rename = "type")]
    pub data_type: ParquetType,
    /// Whether the column may be null or missing (default `true`).
    #[serde(default = "default_true")]
    pub nullable: bool,
}

/// A column type of an explicit Parquet schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ParquetType {
    /// `true` / `false`.
    Boolean,
    /// 32-bit signed integer.
    Int32,
    /// 64-bit signed integer.
    Int64,
    /// 64-bit unsigned integer.
    Uint64,
    /// 32-bit float.
    Float32,
    /// 64-bit float.
    Float64,
    /// UTF-8 text.
    String,
    /// A calendar date, from `YYYY-MM-DD` text.
    Date,
    /// A UTC timestamp in milliseconds, from RFC 3339 text or epoch numbers.
    TimestampMs,
    /// A UTC timestamp in microseconds.
    TimestampUs,
    /// A UTC timestamp in nanoseconds.
    TimestampNs,
    /// A fixed-point decimal with the given precision and scale, from
    /// numbers or numeric text.
    Decimal {
        /// Total digits (1–38).
        precision: u8,
        /// Digits after the point.
        scale: i8,
    },
}

/// JSON Lines write options.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JsonLinesOptions {
    /// Pretty-print each record across several lines. The output is then a
    /// stream of JSON documents rather than one record per line.
    #[serde(default)]
    pub pretty: bool,
}

fn validate_parquet(p: &ParquetOptions) -> Result<(), FaucetError> {
    if p.row_group_size == 0 {
        return Err(FaucetError::Config(
            "file sink: `parquet.row_group_size` must be at least 1".into(),
        ));
    }
    let Some(fields) = &p.schema else {
        return Ok(());
    };
    if fields.is_empty() {
        return Err(FaucetError::Config(
            "file sink: `parquet.schema` must name at least one column".into(),
        ));
    }
    let mut seen = std::collections::HashSet::new();
    for f in fields {
        if f.name.is_empty() || !seen.insert(f.name.as_str()) {
            return Err(FaucetError::Config(format!(
                "file sink: `parquet.schema` column names must be unique and non-empty, got {:?}",
                f.name
            )));
        }
        if let ParquetType::Decimal { precision, scale } = f.data_type
            && (!(1..=38).contains(&precision)
                || i16::from(scale).unsigned_abs() > u16::from(precision))
        {
            return Err(FaucetError::Config(format!(
                "file sink: `parquet.schema` column '{}': decimal precision must be 1–38 and \
                 the scale no larger than the precision",
                f.name
            )));
        }
    }
    Ok(())
}

/// Configuration for the local file sink.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(extend("x-faucet-aliases" = ["max_rows_per_file"]))]
pub struct FileSinkConfig {
    /// The file to write, as a template. `{part}` is replaced by the rollover
    /// part number (`00001`, `00002`, …); with a rollover cap and no `{part}`,
    /// `-{part}` is inserted before the extension. A path ending in `/` is a
    /// directory: files are named `part-{part}` plus the format's extension,
    /// so `format` must be set. `${now.*}` tokens are resolved by the CLI
    /// before the sink sees the path.
    pub path: String,
    /// File format: `auto` (default) resolves from the path's extension,
    /// looking through a compression suffix (`.csv.gz` is CSV). ORC is
    /// read-only and refused.
    #[serde(default)]
    pub format: FileSinkFormat,
    /// Compression: `auto` (default) resolves from the suffix (`.gz`, `.zst`).
    /// Not applicable to Parquet, Avro or Excel, which compress internally.
    #[serde(default)]
    pub compression: CompressionConfig,
    /// What to do with a file that already exists (default `overwrite`).
    #[serde(default)]
    pub mode: FileMode,
    /// `append` (default) or `overwrite` — replace the destination's output
    /// set atomically when the run succeeds.
    #[serde(default)]
    pub write_mode: FileWriteMode,
    /// Roll to a new file after this many records. `max_rows_per_file` is
    /// accepted as another name.
    #[serde(
        default,
        alias = "max_rows_per_file",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_records_per_file: Option<usize>,
    /// Roll to a new file once the current one reaches this many bytes. A
    /// single page larger than the cap still lands in one file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes_per_file: Option<usize>,
    /// Create missing parent directories (default `true`).
    #[serde(default = "default_true")]
    pub create_dirs: bool,
    /// Records per upstream page. Informational: the sink writes each page it
    /// is handed. Defaults to [`DEFAULT_BATCH_SIZE`].
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// CSV dialect, used when the format is CSV.
    #[serde(default)]
    pub csv: CsvOptions,
    /// Worksheet name, used when the format is Excel.
    #[serde(default)]
    pub excel: ExcelOptions,
    /// Record framing, used when the format is XML.
    #[serde(default)]
    pub xml: XmlOptions,
    /// Writer schema and block codec, used when the format is Avro.
    #[serde(default)]
    pub avro: AvroOptions,
    /// Compression, row groups and an optional explicit schema, used when
    /// the format is Parquet.
    #[serde(default)]
    pub parquet: ParquetOptions,
    /// Pretty-printing, used when the format is JSON Lines.
    #[serde(default)]
    pub json_lines: JsonLinesOptions,
    /// Encrypt the output at rest (AES-256-GCM). JSON Lines and raw text
    /// seal each record on its own line (base64), exactly as the jsonl sink
    /// does, so the file stays appendable — and, like the jsonl sink, cannot
    /// also be compressed. Every other format is sealed as a whole file when
    /// it is finalised, after any `compression`. The file source's
    /// `encryption` block reads both back.
    #[cfg(feature = "encryption")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encryption: Option<faucet_core::EncryptionSpec>,
}

fn default_true() -> bool {
    true
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

impl FileSinkConfig {
    /// A config writing to `path` with every default.
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            format: FileSinkFormat::Auto,
            compression: CompressionConfig::Auto,
            mode: FileMode::Overwrite,
            write_mode: FileWriteMode::Append,
            max_records_per_file: None,
            max_bytes_per_file: None,
            create_dirs: true,
            batch_size: DEFAULT_BATCH_SIZE,
            csv: CsvOptions::default(),
            excel: ExcelOptions::default(),
            xml: XmlOptions::default(),
            avro: AvroOptions::default(),
            parquet: ParquetOptions::default(),
            json_lines: JsonLinesOptions::default(),
            #[cfg(feature = "encryption")]
            encryption: None,
        }
    }

    /// Set the JSON Lines options.
    pub fn json_lines(mut self, json_lines: JsonLinesOptions) -> Self {
        self.json_lines = json_lines;
        self
    }

    /// Encrypt the output at rest.
    #[cfg(feature = "encryption")]
    pub fn encryption(mut self, encryption: faucet_core::EncryptionSpec) -> Self {
        self.encryption = Some(encryption);
        self
    }

    /// Whether the format seals each record on its own line when encrypted.
    pub fn encrypts_per_line(&self) -> bool {
        matches!(
            self.resolved_format(),
            Ok(FileFormat::JsonLines | FileFormat::RawText)
        )
    }

    /// Set the format.
    pub fn format(mut self, format: FileSinkFormat) -> Self {
        self.format = format;
        self
    }

    /// Set the compression codec.
    pub fn compression(mut self, compression: CompressionConfig) -> Self {
        self.compression = compression;
        self
    }

    /// Set the existing-file mode.
    pub fn mode(mut self, mode: FileMode) -> Self {
        self.mode = mode;
        self
    }

    /// Set the pipeline write mode.
    pub fn write_mode(mut self, write_mode: FileWriteMode) -> Self {
        self.write_mode = write_mode;
        self
    }

    /// Roll after `n` records.
    pub fn max_records_per_file(mut self, n: usize) -> Self {
        self.max_records_per_file = Some(n);
        self
    }

    /// Roll after `n` bytes.
    pub fn max_bytes_per_file(mut self, n: usize) -> Self {
        self.max_bytes_per_file = Some(n);
        self
    }

    /// Set whether missing directories are created.
    pub fn create_dirs(mut self, create: bool) -> Self {
        self.create_dirs = create;
        self
    }

    /// Set the page-size hint.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the CSV dialect.
    pub fn csv(mut self, csv: CsvOptions) -> Self {
        self.csv = csv;
        self
    }

    /// Set the Avro writer schema and codec.
    pub fn avro(mut self, avro: AvroOptions) -> Self {
        self.avro = avro;
        self
    }

    /// Set the Parquet options.
    pub fn parquet(mut self, parquet: ParquetOptions) -> Self {
        self.parquet = parquet;
        self
    }

    /// Whether the path names a directory rather than a file.
    pub fn is_directory(&self) -> bool {
        faucet_common_file::is_directory_path(&self.path)
    }

    /// Whether a rollover cap is set.
    pub fn rolls_over(&self) -> bool {
        self.max_records_per_file.is_some() || self.max_bytes_per_file.is_some()
    }

    /// The format this config writes. Refuses ORC, a path whose extension
    /// names no format, and a directory path without an explicit format.
    pub fn resolved_format(&self) -> Result<FileFormat, FaucetError> {
        if self.is_directory() && self.format.explicit().is_none() {
            return Err(FaucetError::Config(format!(
                "file sink: '{}' is a directory, so there is no extension to take the format \
                 from — set `format`",
                self.path
            )));
        }
        let name = self.path.replace(PART_TOKEN, "");
        let format = self
            .format
            .resolve_writable(&name)
            .map_err(|e| FaucetError::Config(format!("file sink: {e}")))?;
        Ok(format)
    }

    /// The compression codec for the resolved path.
    pub fn resolved_compression(&self) -> Compression {
        faucet_common_file::resolve_compression(
            self.compression,
            &self.path.replace(PART_TOKEN, "00001"),
        )
    }

    /// Validate every combination that would otherwise fail mid-run.
    pub fn validate(&self) -> Result<(), FaucetError> {
        faucet_common_file::require_path("file sink", &self.path)?;
        faucet_core::validate_batch_size(self.batch_size)?;
        let format = self.resolved_format()?;
        require_feature(format)?;
        if matches!(self.max_records_per_file, Some(0)) {
            return Err(FaucetError::Config(
                "file sink: `max_records_per_file` must be at least 1".into(),
            ));
        }
        if matches!(self.max_bytes_per_file, Some(0)) {
            return Err(FaucetError::Config(
                "file sink: `max_bytes_per_file` must be at least 1".into(),
            ));
        }
        if self.path.matches(PART_TOKEN).count() > 1 {
            return Err(FaucetError::Config(format!(
                "file sink: '{}' has more than one `{{part}}`",
                self.path
            )));
        }
        let codec = self.resolved_compression();
        if codec != Compression::None && is_self_compressed(format) {
            return Err(FaucetError::Config(format!(
                "file sink: `compression` does not apply to {} files, which compress \
                 internally{} — remove the compression suffix or setting",
                format.as_str(),
                match format {
                    FileFormat::Parquet => " (see `parquet.compression`)",
                    FileFormat::Avro => " (see `avro.codec`)",
                    _ => "",
                }
            )));
        }
        if self.mode == FileMode::Append && !is_appendable(format) {
            return Err(FaucetError::Config(format!(
                "file sink: `mode: append` cannot add to a {} file without rewriting it — use \
                 JSON Lines or CSV, `mode: overwrite`, or rollover with a `{{part}}` template",
                format.as_str()
            )));
        }
        if self.write_mode == FileWriteMode::Overwrite && self.mode != FileMode::Overwrite {
            return Err(FaucetError::Config(
                "file sink: `write_mode: overwrite` replaces the whole output set, so `mode` \
                 must be `overwrite`"
                    .into(),
            ));
        }
        if format == FileFormat::Csv {
            self.csv.validate()?;
        }
        if format == FileFormat::Parquet {
            validate_parquet(&self.parquet)?;
        }
        #[cfg(feature = "encryption")]
        if let Some(spec) = &self.encryption {
            faucet_core::CompiledEncryption::compile(spec)?;
            if codec != Compression::None && self.encrypts_per_line() {
                return Err(FaucetError::Config(
                    "file sink: `encryption` and `compression` are mutually exclusive for JSON \
                     Lines and raw text — each record is sealed on its own line, and sealed \
                     lines cannot form a gzip/zstd stream"
                        .into(),
                ));
            }
        }
        if format == FileFormat::Avro
            && let Some(schema) = &self.avro.schema
        {
            validate_avro_schema(schema)?;
        }
        Ok(())
    }

    /// The per-format option blocks in the shape the shared encoders want.
    pub fn format_options(&self) -> FormatOptions {
        FormatOptions {
            csv: self.csv.clone(),
            excel: self.excel.clone(),
            xml: self.xml.clone(),
            avro: self.avro.clone(),
            orc: OrcOptions::default(),
        }
    }

    /// What a failed batch write leaves behind (#737): a page lands in a
    /// temporary file and becomes visible only when a flush renames it, so a
    /// failed write never exposes part of a page — but a page can span a
    /// rollover, and files finalised by an earlier rollover stay.
    pub fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        if self.rolls_over() {
            faucet_core::BatchAtomicity::BestEffort
        } else {
            faucet_core::BatchAtomicity::Atomic
        }
    }
}

fn require_feature(format: FileFormat) -> Result<(), FaucetError> {
    let missing = match format {
        FileFormat::Csv if !cfg!(feature = "file-format-csv") => Some("file-format-csv"),
        FileFormat::Xml if !cfg!(feature = "file-format-xml") => Some("file-format-xml"),
        FileFormat::Xlsx if !cfg!(feature = "file-format-excel") => Some("file-format-excel"),
        FileFormat::Avro if !cfg!(feature = "file-format-avro") => Some("file-format-avro"),
        FileFormat::Parquet if !cfg!(feature = "file-format-parquet") => {
            Some("file-format-parquet")
        }
        _ => None,
    };
    match missing {
        Some(feature) => Err(FaucetError::Config(format!(
            "file sink: `{}` needs the `{feature}` build feature",
            format.as_str()
        ))),
        None => Ok(()),
    }
}

#[cfg(feature = "file-format-avro")]
fn validate_avro_schema(schema: &serde_json::Value) -> Result<(), FaucetError> {
    faucet_core::file_format::avro::parse_schema(schema).map(|_| ())
}

#[cfg(not(feature = "file-format-avro"))]
fn validate_avro_schema(_: &serde_json::Value) -> Result<(), FaucetError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cfg(v: serde_json::Value) -> FileSinkConfig {
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn defaults_and_builders() {
        let c = FileSinkConfig::new("out/x.jsonl")
            .format(FileSinkFormat::JsonLines)
            .compression(CompressionConfig::None)
            .mode(FileMode::Append)
            .write_mode(FileWriteMode::Append)
            .max_records_per_file(5)
            .max_bytes_per_file(10)
            .create_dirs(false)
            .with_batch_size(7)
            .csv(CsvOptions::default())
            .avro(AvroOptions::default())
            .parquet(ParquetOptions::default());
        assert_eq!(c.max_records_per_file, Some(5));
        assert_eq!(c.max_bytes_per_file, Some(10));
        assert!(!c.create_dirs);
        assert_eq!(c.batch_size, 7);
        assert!(c.rolls_over());
        let d = cfg(json!({"path": "a.jsonl"}));
        assert!(d.create_dirs);
        assert_eq!(d.batch_size, DEFAULT_BATCH_SIZE);
        assert_eq!(d.mode, FileMode::Overwrite);
        assert_eq!(d.write_mode, FileWriteMode::Append);
        assert_eq!(d.parquet.compression, ParquetCodec::Snappy);
        assert!(!d.rolls_over());
    }

    #[test]
    fn unknown_keys_are_refused() {
        let r: Result<FileSinkConfig, _> =
            serde_json::from_value(json!({"path": "a.jsonl", "formt": "csv"}));
        assert!(r.is_err());
    }

    #[test]
    fn formats_resolve_by_extension_and_explicitly() {
        assert_eq!(
            cfg(json!({"path": "a.jsonl"})).resolved_format().unwrap(),
            FileFormat::JsonLines
        );
        assert_eq!(
            cfg(json!({"path": "a-{part}.csv.gz"}))
                .resolved_format()
                .unwrap(),
            FileFormat::Csv
        );
        assert_eq!(
            cfg(json!({"path": "a.dat", "format": "xml"}))
                .resolved_format()
                .unwrap(),
            FileFormat::Xml
        );
        let e = cfg(json!({"path": "a.dat"})).resolved_format().unwrap_err();
        assert!(e.to_string().contains("a.dat"), "{e}");
        let e = cfg(json!({"path": "a.orc"})).resolved_format().unwrap_err();
        assert!(e.to_string().contains("read-only"), "{e}");
        let e = cfg(json!({"path": "out/"})).resolved_format().unwrap_err();
        assert!(e.to_string().contains("directory"), "{e}");
        assert!(cfg(json!({"path": "out/"})).is_directory());
        assert_eq!(
            cfg(json!({"path": "out\\", "format": "json_array"}))
                .resolved_format()
                .unwrap(),
            FileFormat::JsonArray
        );
    }

    #[test]
    fn validate_refuses_each_bad_combination() {
        let bad = [
            (json!({"path": " "}), "empty"),
            (json!({"path": "a.jsonl", "batch_size": 2_000_000}), "batch"),
            (
                json!({"path": "a.jsonl", "max_records_per_file": 0}),
                "max_records",
            ),
            (
                json!({"path": "a.jsonl", "max_bytes_per_file": 0}),
                "max_bytes",
            ),
            (json!({"path": "{part}-{part}.jsonl"}), "more than one"),
            (json!({"path": "a.json", "mode": "append"}), "append"),
            (
                json!({"path": "a.jsonl", "mode": "append", "write_mode": "overwrite"}),
                "must be `overwrite`",
            ),
            (
                json!({"path": "a.jsonl", "mode": "error_if_exists", "write_mode": "overwrite"}),
                "must be `overwrite`",
            ),
        ];
        for (v, needle) in bad {
            let e = cfg(v.clone()).validate().unwrap_err().to_string();
            assert!(
                e.to_lowercase().contains(&needle.to_lowercase()),
                "{v}: {e}"
            );
        }
        cfg(json!({"path": "a.jsonl", "mode": "append"}))
            .validate()
            .unwrap();
        cfg(json!({"path": "a.txt", "mode": "error_if_exists"}))
            .validate()
            .unwrap();
        cfg(json!({"path": "a.jsonl.zst", "write_mode": "overwrite"}))
            .validate()
            .unwrap();
    }

    #[cfg(feature = "file-formats")]
    #[test]
    fn validate_format_specific_rules() {
        let e = cfg(json!({"path": "a.parquet.gz"}))
            .validate()
            .unwrap_err()
            .to_string();
        assert!(e.contains("parquet.compression"), "{e}");
        let e = cfg(json!({"path": "a.avro", "compression": "zstd"}))
            .validate()
            .unwrap_err()
            .to_string();
        assert!(e.contains("avro.codec"), "{e}");
        let e = cfg(json!({"path": "a.xlsx", "compression": "gzip"}))
            .validate()
            .unwrap_err()
            .to_string();
        assert!(e.contains("xlsx"), "{e}");
        let e = cfg(json!({"path": "a.csv", "csv": {"delimiter": "ab"}}))
            .validate()
            .unwrap_err()
            .to_string();
        assert!(e.contains("delimiter"), "{e}");
        let e = cfg(json!({"path": "a.avro", "avro": {"schema": {"type": "nope"}}}))
            .validate()
            .unwrap_err()
            .to_string();
        assert!(!e.is_empty());
        cfg(json!({"path": "a.avro", "avro": {"schema": {"type": "record", "name": "r", "fields": []}}}))
            .validate()
            .unwrap();
        cfg(json!({"path": "a.csv.gz", "mode": "append"}))
            .validate()
            .unwrap();
        for p in ["a.xml", "a.xlsx", "a.parquet", "a.avro"] {
            cfg(json!({ "path": p })).validate().unwrap();
        }
    }

    #[test]
    fn require_feature_reports_missing_builds() {
        for (f, has) in [
            (FileFormat::Csv, cfg!(feature = "file-format-csv")),
            (FileFormat::Xml, cfg!(feature = "file-format-xml")),
            (FileFormat::Xlsx, cfg!(feature = "file-format-excel")),
            (FileFormat::Avro, cfg!(feature = "file-format-avro")),
            (FileFormat::Parquet, cfg!(feature = "file-format-parquet")),
            (FileFormat::JsonLines, true),
        ] {
            let r = require_feature(f);
            assert_eq!(r.is_ok(), has, "{f:?}");
        }
    }

    #[test]
    fn atomicity_depends_on_rollover() {
        assert_eq!(
            cfg(json!({"path": "a.jsonl"})).batch_atomicity(),
            faucet_core::BatchAtomicity::Atomic
        );
        assert_eq!(
            cfg(json!({"path": "a.jsonl", "max_records_per_file": 3})).batch_atomicity(),
            faucet_core::BatchAtomicity::BestEffort
        );
    }

    #[test]
    fn format_options_carry_every_block() {
        let c =
            cfg(json!({"path": "a.csv", "csv": {"delimiter": ";"}, "xml": {"root_element": "r"}}));
        let o = c.format_options();
        assert_eq!(o.csv.delimiter, ";");
        assert_eq!(o.xml.root_element, "r");
        assert_eq!(c.resolved_compression(), Compression::None);
        assert_eq!(
            cfg(json!({"path": "a-{part}.jsonl.gz"})).resolved_compression(),
            Compression::Gzip
        );
    }
}
