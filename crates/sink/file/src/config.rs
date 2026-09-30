//! Local file sink configuration.

use faucet_core::{
    AvroOptions, Compression, CompressionConfig, CsvOptions, DEFAULT_BATCH_SIZE, ExcelOptions,
    FaucetError, FileFormat, FormatOptions, OrcOptions, XmlOptions,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub use faucet_common_file::write::{
    DEFAULT_ROW_GROUP_SIZE, FileWriteMode, IfExists, JsonLinesOptions, PART_TOKEN, ParquetCodec,
    ParquetField, ParquetOptions, ParquetType,
};
use faucet_common_file::write::{WriteConfig, WriteSettings};

pub use faucet_common_file::FileFormatChoice as FileSinkFormat;

/// Configuration for the local file sink.
///
/// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
/// minor release; any change is called out in the changelog.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(extend("x-faucet-aliases" = ["mode"]))]
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
    /// Compression of the finished file: `auto` (default) resolves from the
    /// suffix (`.gz`, `.zst`). It applies to every format; Parquet, Avro and
    /// Excel already compress internally, so a codec on them compresses the
    /// file a second time and most readers then cannot open it without
    /// decompressing first.
    #[serde(default)]
    pub compression: CompressionConfig,
    /// What to do with a file that already exists (default `replace`):
    /// `replace` it, `append` to it (JSON Lines, CSV, raw text), or fail
    /// with `error`. `mode` is accepted as another name for this field, and
    /// `overwrite` / `error_if_exists` for its values.
    #[serde(default, alias = "mode")]
    pub if_exists: IfExists,
    /// `append` (default) or `overwrite` — replace the destination's whole
    /// output set when the run succeeds.
    #[serde(default)]
    pub write_mode: FileWriteMode,
    /// Roll to a new file after this many records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_records_per_file: Option<usize>,
    /// Roll to a new file once the current one reaches about this many
    /// bytes, counted on the records' JSON length before compression (on the
    /// columnar Parquet path, their in-memory Arrow size). A single page
    /// larger than the cap still lands in one file.
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
    /// Compression (default `snappy`), row groups and an optional explicit
    /// schema, used when the format is Parquet.
    #[serde(default)]
    pub parquet: ParquetOptions,
    /// Pretty-printing, used when the format is JSON Lines.
    #[serde(default)]
    pub json_lines: JsonLinesOptions,
    /// Encrypt the output at rest (AES-256-GCM). Uncompressed JSON Lines and
    /// raw text seal each record on its own line (base64), exactly as the
    /// jsonl sink does, so the file stays appendable. Every other file —
    /// compressed JSON Lines included — is sealed whole when it is
    /// published, after any `compression`; appending to one decrypts it
    /// first. Appending to an existing file that is not sealed the same way
    /// is refused. The file source's `encryption` block reads both back.
    /// Scratch files are not encrypted while the run is in progress; those
    /// holding plaintext are readable by their owner only.
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
            if_exists: IfExists::Replace,
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

    /// Set what happens to an existing file.
    pub fn if_exists(mut self, if_exists: IfExists) -> Self {
        self.if_exists = if_exists;
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

    /// The config's write fields in the shared writer's shape.
    pub fn write_config(&self) -> WriteConfig {
        WriteConfig {
            connector: "file sink",
            path: Some(self.path.clone()),
            format: self.format.explicit(),
            compression: self.compression,
            opts: self.format_options(),
            parquet: self.parquet.clone(),
            default_parquet_codec: ParquetCodec::Snappy,
            json_lines: self.json_lines.clone(),
            if_exists: self.if_exists,
            write_mode: self.write_mode,
            max_records_per_file: self.max_records_per_file,
            max_bytes_per_file: self.max_bytes_per_file,
            #[cfg(feature = "encryption")]
            encryption: self.encryption.clone(),
            ..WriteConfig::default()
        }
    }

    /// The format this config writes. Refuses ORC, a path whose extension
    /// names no format, and a directory path without an explicit format.
    pub fn resolved_format(&self) -> Result<FileFormat, FaucetError> {
        Ok(self.write_config().settings()?.format)
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
        faucet_core::validate_batch_size(self.batch_size)?;
        self.write_config().validate()
    }

    /// The storage-independent write settings for the shared writer.
    pub fn settings(&self) -> Result<WriteSettings, FaucetError> {
        self.write_config().settings()
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
    /// scratch file and becomes visible only when a flush publishes it, so a
    /// failed write never exposes part of a page — but a page can span a
    /// rollover, and files closed by an earlier rollover stay.
    pub fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        self.write_config().batch_atomicity()
    }
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
            .if_exists(IfExists::Append)
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
        assert_eq!(d.if_exists, IfExists::Replace);
        assert_eq!(d.write_mode, FileWriteMode::Append);
        assert_eq!(d.parquet.compression, None);
        assert_eq!(d.settings().unwrap().parquet_codec(), ParquetCodec::Snappy);
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
                "must be `replace`",
            ),
            (
                json!({"path": "a.jsonl", "if_exists": "error", "write_mode": "overwrite"}),
                "must be `replace`",
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
        for p in ["a.parquet.gz", "a.avro.zst", "a.xlsx.gz"] {
            cfg(json!({ "path": p })).validate().unwrap();
        }
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
            let r = WriteSettings::new(f, Compression::None).validate();
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

    #[test]
    fn json_lines_and_encryption_builders_and_a_part_token_directory() {
        let c = FileSinkConfig::new("out/x.jsonl").json_lines(JsonLinesOptions { pretty: true });
        assert!(c.json_lines.pretty);
        #[cfg(feature = "encryption")]
        {
            let spec: faucet_core::EncryptionSpec =
                serde_json::from_value(json!({"key": "k"})).unwrap();
            assert!(c.clone().encryption(spec).encryption.is_some());
        }
        let e = cfg(json!({"path": "out-{part}/x.jsonl"}))
            .validate()
            .unwrap_err()
            .to_string();
        assert!(e.contains("file sink: "), "{e}");
        assert!(e.contains("may appear only in the file name"), "{e}");
    }
}
