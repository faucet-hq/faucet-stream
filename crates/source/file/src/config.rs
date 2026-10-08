//! File source configuration.

use faucet_core::{
    AvroOptions, CompressionConfig, CsvOptions, DEFAULT_BATCH_SIZE, ExcelOptions, FaucetError,
    FormatOptions, OrcOptions, XmlOptions,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The format choice — `auto` by extension, per file, or one fixed format.
/// Shared with the file sink through `faucet-common-file`.
pub use faucet_common_file::FileFormatChoice as FileSourceFormat;

/// What makes a file "new" in incremental mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum IncrementalBy {
    /// Modification time: a run reads files modified after the newest one the
    /// previous run read (ties broken by path), so a rewritten file is read
    /// again. Over HTTP the `Last-Modified` header is the modification time.
    Mtime,
    /// Name: a run reads files whose path sorts after the last one read —
    /// the right choice for dated names (`export-2026-09-27.csv`) that are
    /// never rewritten.
    Name,
}

/// Incremental mode: re-runs pick up only new files.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct IncrementalSpec {
    /// What makes a file new.
    pub by: IncrementalBy,
}

fn default_concurrency() -> usize {
    4
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

fn default_http_retries() -> u32 {
    3
}

fn default_max_object_bytes() -> u64 {
    faucet_core::file_format::DEFAULT_MAX_OBJECT_BYTES
}

fn default_http_connect_timeout_secs() -> u64 {
    30
}

fn default_http_read_timeout_secs() -> u64 {
    300
}

/// Configuration for the local (and `http(s)://`) file source.
///
/// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
/// minor release; any change is called out in the changelog.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FileSourceConfig {
    /// A file, a directory, a glob (`data/**/*.jsonl`), or one `http://` /
    /// `https://` URL. A directory lists its regular files (descending into
    /// subdirectories with `recursive: true`); every listing is sorted by path.
    pub path: String,
    /// File format, `auto` (by extension, per file) by default. Under `auto`
    /// a file with no recognised extension is skipped with a warning, or
    /// refused when `strict: true`. JSON Lines, Avro, ORC and Parquet stream;
    /// JSON arrays, CSV, XML and Excel are read whole per file.
    #[serde(default)]
    pub format: FileSourceFormat,
    /// Compression codec. `auto` (default) resolves per file from its suffix
    /// (`.gz`, `.zst`).
    #[serde(default)]
    pub compression: CompressionConfig,
    /// Descend into subdirectories when `path` is a directory.
    #[serde(default)]
    pub recursive: bool,
    /// Files opened ahead of the one being decoded (ordered prefetch). `0`
    /// reads serially.
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    /// Records per emitted page. `0` emits one page per file.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Read only files that are new since the previous run. Needs a
    /// pipeline `state:` block; the bookmark is kept under `file:<path>`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incremental: Option<IncrementalSpec>,
    /// Skip files modified less than this many seconds ago — files still
    /// being written. They are read by a later run once they settle.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stable_for_secs: Option<u64>,
    /// Refuse (instead of skipping with a warning) a file whose extension
    /// names no format under `format: auto`.
    #[serde(default)]
    pub strict: bool,
    /// Largest file, once decompressed, that a format read whole (JSON array,
    /// XML, Excel, compressed or remote Avro/ORC/Parquet, encrypted files) may
    /// reach; a larger one fails with an error instead of exhausting memory.
    /// Default 2 GiB.
    #[serde(default = "default_max_object_bytes")]
    pub max_object_bytes: u64,
    /// Read at most this many files (after sorting and filtering).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_files: Option<usize>,
    /// Request headers for an `http(s)://` path (e.g. `Authorization`).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,
    /// Retries for a failed HTTP request (connection error, 429, 5xx), with
    /// exponential backoff that honours `Retry-After`.
    #[serde(default = "default_http_retries")]
    pub http_retries: u32,
    /// Seconds to wait for an HTTP connection to open. A timeout is retried.
    #[serde(default = "default_http_connect_timeout_secs")]
    pub http_connect_timeout_secs: u64,
    /// Seconds an HTTP response may go without sending a byte — waiting for
    /// the headers or mid-body. A timeout before the body starts is retried;
    /// one mid-body fails the file.
    #[serde(default = "default_http_read_timeout_secs")]
    pub http_read_timeout_secs: u64,
    /// CSV dialect, used for CSV files.
    #[serde(default)]
    pub csv: CsvOptions,
    /// Worksheet selection, used for Excel files.
    #[serde(default)]
    pub excel: ExcelOptions,
    /// Record framing, used for XML files.
    #[serde(default)]
    pub xml: XmlOptions,
    /// Reader schema, used for Avro files.
    #[serde(default)]
    pub avro: AvroOptions,
    /// Column projection, used for ORC files.
    #[serde(default)]
    pub orc: OrcOptions,
    /// Column projection, used for Parquet files.
    #[serde(default)]
    pub parquet: ParquetReadOptions,
    /// Decrypt files sealed by the file or jsonl sink's `encryption` block
    /// (AES-256-GCM). An encrypted file is read into memory whole: JSON Lines
    /// and raw text written line by line are then opened one line at a time,
    /// every other file as one sealed body. A file that is not sealed fails
    /// the read.
    #[cfg(feature = "encryption")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encryption: Option<faucet_core::EncryptionSpec>,
}

/// Parquet read options: the same `parquet:` block as the object-store
/// sources.
pub use faucet_core::ParquetReadOptions;

impl FileSourceConfig {
    /// A config reading `path` with every default.
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            format: FileSourceFormat::Auto,
            compression: CompressionConfig::Auto,
            recursive: false,
            concurrency: default_concurrency(),
            batch_size: DEFAULT_BATCH_SIZE,
            incremental: None,
            stable_for_secs: None,
            strict: false,
            max_files: None,
            max_object_bytes: default_max_object_bytes(),
            headers: BTreeMap::new(),
            http_retries: default_http_retries(),
            http_connect_timeout_secs: default_http_connect_timeout_secs(),
            http_read_timeout_secs: default_http_read_timeout_secs(),
            csv: CsvOptions::default(),
            excel: ExcelOptions::default(),
            xml: XmlOptions::default(),
            avro: AvroOptions::default(),
            orc: OrcOptions::default(),
            parquet: ParquetReadOptions::default(),
            #[cfg(feature = "encryption")]
            encryption: None,
        }
    }

    /// Set the format.
    pub fn format(mut self, format: FileSourceFormat) -> Self {
        self.format = format;
        self
    }

    /// Descend into subdirectories.
    pub fn recursive(mut self, recursive: bool) -> Self {
        self.recursive = recursive;
        self
    }

    /// Set the records per page.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Turn on incremental mode.
    pub fn incremental(mut self, by: IncrementalBy) -> Self {
        self.incremental = Some(IncrementalSpec { by });
        self
    }

    /// Whether `path` is an `http(s)://` URL.
    pub fn is_http(&self) -> bool {
        faucet_common_file::is_http_path(&self.path)
    }

    /// Project Parquet files to `columns`.
    pub fn parquet_columns<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.parquet.columns = Some(columns.into_iter().map(Into::into).collect());
        self
    }

    /// Whether an `encryption` block is set.
    pub fn encryption_set(&self) -> bool {
        #[cfg(feature = "encryption")]
        return self.encryption.is_some();
        #[cfg(not(feature = "encryption"))]
        false
    }

    /// The per-format option blocks. CSV is strict about ragged rows unless
    /// `csv.flexible` says otherwise.
    pub fn format_options(&self) -> FormatOptions {
        let mut csv = self.csv.clone();
        csv.flexible.get_or_insert(false);
        FormatOptions {
            csv,
            excel: self.excel.clone(),
            xml: self.xml.clone(),
            avro: self.avro.clone(),
            orc: self.orc.clone(),
        }
    }

    /// Load-time checks: batch size bounds, a non-empty path, a parseable
    /// Avro reader schema, and header names/values HTTP can carry.
    pub fn validate(&self) -> Result<(), FaucetError> {
        faucet_core::validate_batch_size(self.batch_size)?;
        faucet_common_file::require_path("file source", &self.path)?;
        if !self.headers.is_empty() && !self.is_http() {
            return Err(FaucetError::Config(
                "file source: `headers` apply only to an http(s):// path".into(),
            ));
        }
        if self.max_object_bytes == 0 {
            return Err(FaucetError::Config(
                "file source: `max_object_bytes` must be at least 1".into(),
            ));
        }
        if self.http_connect_timeout_secs == 0 || self.http_read_timeout_secs == 0 {
            return Err(FaucetError::Config(
                "file source: `http_connect_timeout_secs` and `http_read_timeout_secs` must be \
                 at least 1"
                    .into(),
            ));
        }
        for (k, v) in &self.headers {
            reqwest::header::HeaderName::from_bytes(k.as_bytes())
                .map_err(|e| FaucetError::Config(format!("file source: header name {k:?}: {e}")))?;
            reqwest::header::HeaderValue::from_str(v)
                .map_err(|e| FaucetError::Config(format!("file source: header {k:?}: {e}")))?;
        }
        #[cfg(feature = "file-format-avro")]
        self.avro.parsed_schema()?;
        self.csv.validate()?;
        self.parquet.validate()?;
        #[cfg(feature = "encryption")]
        if let Some(spec) = &self.encryption {
            faucet_core::CompiledEncryption::compile(spec)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn defaults_and_builders() {
        let c = FileSourceConfig::new("data/")
            .format(FileSourceFormat::Csv)
            .recursive(true)
            .with_batch_size(10)
            .incremental(IncrementalBy::Name);
        assert_eq!(c.format.explicit(), Some(faucet_core::FileFormat::Csv));
        assert!(c.recursive);
        assert_eq!(c.batch_size, 10);
        assert_eq!(c.incremental.unwrap().by, IncrementalBy::Name);
        assert!(!c.is_http());
        assert!(FileSourceConfig::new("HTTPS://x/y.csv").is_http());
        c.validate().expect("valid");
    }

    #[test]
    fn deserializes_with_option_blocks_and_refuses_unknown_keys() {
        let c: FileSourceConfig = serde_json::from_value(json!({
            "path": "in/*.csv",
            "csv": {"delimiter": ";"},
            "orc": {"columns": ["a"]},
            "incremental": {"by": "mtime"},
            "stable_for_secs": 5
        }))
        .expect("config");
        assert_eq!(c.format_options().csv.delimiter, ";");
        assert_eq!(c.format_options().orc.columns, Some(vec!["a".to_string()]));
        assert_eq!(c.incremental.unwrap().by, IncrementalBy::Mtime);
        assert!(
            serde_json::from_value::<FileSourceConfig>(json!({"path": "x", "nope": 1})).is_err()
        );
    }

    #[test]
    fn validation_refuses_bad_configs() {
        assert!(FileSourceConfig::new(" ").validate().is_err());
        assert!(
            FileSourceConfig::new("x")
                .with_batch_size(usize::MAX)
                .validate()
                .is_err()
        );
        let mut c = FileSourceConfig::new("local.csv");
        c.headers.insert("Authorization".into(), "x".into());
        assert!(c.validate().is_err(), "headers need an http path");
        let mut c = FileSourceConfig::new("https://h/x.csv");
        c.headers.insert("bad header".into(), "x".into());
        assert!(c.validate().is_err());
        let mut c = FileSourceConfig::new("https://h/x.csv");
        c.headers.insert("X-Ok".into(), "line\nbreak".into());
        assert!(c.validate().is_err());
        #[cfg(feature = "file-format-avro")]
        {
            let mut c = FileSourceConfig::new("x.avro");
            c.avro.schema = Some(json!({"type": "nope"}));
            assert!(c.validate().is_err());
        }
    }
}
