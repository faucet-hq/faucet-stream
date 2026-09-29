//! Local file sink configuration.

use faucet_core::{
    AvroOptions, Compression, CompressionConfig, CsvOptions, DEFAULT_BATCH_SIZE, ExcelOptions,
    FaucetError, FileFormat, FormatOptions, OrcOptions, XmlOptions,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use faucet_common_file::write::WriteSettings;
pub use faucet_common_file::write::{
    DEFAULT_ROW_GROUP_SIZE, FileMode, FileWriteMode, JsonLinesOptions, PART_TOKEN, ParquetCodec,
    ParquetField, ParquetOptions, ParquetType,
};

pub use faucet_common_file::FileFormatChoice as FileSinkFormat;

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
    /// Encrypt the output at rest (AES-256-GCM). Uncompressed JSON Lines and
    /// raw text seal each record on its own line (base64), exactly as the
    /// jsonl sink does, so the file stays appendable. Every other file —
    /// compressed JSON Lines included — is sealed whole when it is
    /// finalised, after any `compression`; appending to one decrypts it
    /// first. The file source's `encryption` block reads both back.
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
        if self.path.matches(PART_TOKEN).count() > 1 {
            return Err(FaucetError::Config(format!(
                "file sink: '{}' has more than one `{{part}}`",
                self.path
            )));
        }
        let settings = self.settings()?;
        let (_, template) = faucet_common_file::write::NameTemplate::from_path(
            &self.path,
            settings.format,
            settings.codec,
            settings.rolls_over(),
        )
        .map_err(|e| match e {
            FaucetError::Config(m) => FaucetError::Config(format!("file sink: {m}")),
            other => other,
        })?;
        settings.validate_for(&template)
    }

    /// The storage-independent write settings for the shared writer.
    pub fn settings(&self) -> Result<WriteSettings, FaucetError> {
        let mut s = WriteSettings::new(self.resolved_format()?, self.resolved_compression());
        s.opts = self.format_options();
        s.parquet = self.parquet.clone();
        s.json_lines = self.json_lines.clone();
        s.mode = self.mode;
        s.write_mode = self.write_mode;
        s.max_records_per_file = self.max_records_per_file;
        s.max_bytes_per_file = self.max_bytes_per_file;
        #[cfg(feature = "encryption")]
        {
            s.encryption = self.encryption.clone();
        }
        Ok(s)
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
}
