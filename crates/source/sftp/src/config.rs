//! SFTP source configuration.

use faucet_common_sftp::SftpConnectionConfig;
use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Format of the remote files read by the SFTP source.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SftpFormat {
    /// Each line in the file is a separate JSON record (the default).
    /// Streamed line-by-line with bounded memory.
    #[default]
    Jsonl,
    /// The entire file is a JSON array of records. Buffered fully per file
    /// (the closing `]` is required to validate the structure), then chunked.
    JsonArray,
    /// Each file becomes a single record with `"path"` and `"content"` fields.
    RawText,
    /// Delimited text, decoded through [`faucet_core::file_format`] so the
    /// records match what every other connector produces for the same file.
    /// Dialect from [`csv`](SftpSourceConfig::csv). Requires
    /// `file-format-csv` (#604).
    #[cfg(feature = "file-format-csv")]
    Csv,
    /// XML, decoded to the compact element→object mapping. The repeated
    /// element is named by [`xml`](SftpSourceConfig::xml). Requires
    /// `file-format-xml` (#604).
    #[cfg(feature = "file-format-xml")]
    Xml,
    /// An Excel workbook. Sheet and header row from
    /// [`excel`](SftpSourceConfig::excel). **Buffered whole** — a workbook is
    /// a zip container whose directory sits at the end. Requires
    /// `file-format-excel` (#604).
    #[cfg(feature = "file-format-excel")]
    Xlsx,
    /// Apache Avro Object Container Files. Each file's embedded writer
    /// schema is resolved against [`avro.schema`](SftpSourceConfig::avro) when set, else
    /// against the first file's schema, so the directory reads as one shape.
    /// Joins the columnar path with the `arrow` feature. Requires
    /// `file-format-avro` (#719).
    #[cfg(feature = "file-format-avro")]
    Avro,
    /// Apache ORC, projected by [`orc.columns`](SftpSourceConfig::orc). **Buffered
    /// whole** (the footer sits at the end) and decoded stripe by stripe;
    /// joins the columnar path. Requires `file-format-orc` (#719).
    #[cfg(feature = "file-format-orc")]
    Orc,
    /// Apache Parquet, projected by [`parquet.columns`](SftpSourceConfig::parquet).
    /// **Buffered whole** (the footer sits at the end); joins the columnar
    /// path. Requires the `arrow` feature (#777).
    #[cfg(feature = "arrow")]
    Parquet,
}

impl SftpFormat {
    /// The shared format this variant maps onto, or `None` for `RawText`,
    /// whose `{path, content}` envelope is the connector's own shape.
    #[cfg(any(
        feature = "file-format-csv",
        feature = "file-format-xml",
        feature = "file-format-excel",
        feature = "file-format-avro",
        feature = "file-format-orc"
    ))]
    pub(crate) fn shared(self) -> Option<faucet_core::FileFormat> {
        match self {
            Self::Jsonl => Some(faucet_core::FileFormat::JsonLines),
            Self::JsonArray => Some(faucet_core::FileFormat::JsonArray),
            Self::RawText => None,
            #[cfg(feature = "file-format-csv")]
            Self::Csv => Some(faucet_core::FileFormat::Csv),
            #[cfg(feature = "file-format-xml")]
            Self::Xml => Some(faucet_core::FileFormat::Xml),
            #[cfg(feature = "file-format-excel")]
            Self::Xlsx => Some(faucet_core::FileFormat::Xlsx),
            #[cfg(feature = "file-format-avro")]
            Self::Avro => Some(faucet_core::FileFormat::Avro),
            #[cfg(feature = "file-format-orc")]
            Self::Orc => Some(faucet_core::FileFormat::Orc),
            #[cfg(feature = "arrow")]
            Self::Parquet => Some(faucet_core::FileFormat::Parquet),
        }
    }
}

/// Configuration for the SFTP source connector.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SftpSourceConfig {
    /// Shared SFTP connection settings (host, port, username, auth, host-key
    /// policy). Flattened, so its fields sit at the top level of the config.
    #[serde(flatten)]
    pub connection: SftpConnectionConfig,
    /// Remote path to read: a directory whose files are listed and streamed,
    /// or a single file.
    pub path: String,
    /// Optional filename glob (`*` / `?`) applied to the basenames of files in
    /// a directory listing. Ignored when `path` points at a single file.
    #[serde(default)]
    pub glob: Option<String>,
    /// Format of the files to read (default: `jsonl`).
    #[serde(default)]
    pub format: SftpFormat,
    /// Records per emitted [`StreamPage`](faucet_core::StreamPage). For
    /// `jsonl` / `raw_text`, files are decoded incrementally and a page is
    /// yielded whenever the buffer reaches this size (bounded memory). For
    /// `json_array`, each file is buffered fully, then its records are chunked
    /// into pages of this size. Defaults to [`DEFAULT_BATCH_SIZE`].
    ///
    /// `batch_size = 0` is the "no batching" sentinel: one page is emitted per
    /// file.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Maximum number of files read concurrently (default: 4).
    ///
    /// Reads are overlapped with a bounded look-ahead in listing order, so
    /// throughput improves without changing which file an error is attributed
    /// to. Lower than the object-store sources' default because every read
    /// shares one SSH channel, and many servers cap concurrent open handles.
    /// `0` is treated as `1`.
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    /// CSV dialect, used when `format: csv` (#604).
    #[serde(default)]
    pub csv: faucet_core::CsvOptions,
    /// Worksheet selection, used when `format: xlsx` (#604).
    #[serde(default)]
    pub excel: faucet_core::ExcelOptions,
    /// Record framing, used when `format: xml` (#604).
    #[serde(default)]
    pub xml: faucet_core::XmlOptions,
    /// Reader schema, used when `format: avro` (#719).
    #[serde(default)]
    pub avro: faucet_core::AvroOptions,
    /// Column projection, used when `format: orc` (#719).
    #[serde(default)]
    pub orc: faucet_core::OrcOptions,
    /// Column projection, used when `format: parquet` (#777).
    #[serde(default)]
    pub parquet: faucet_core::ParquetReadOptions,
    /// Largest file, in bytes, a format read whole into memory (JSON array,
    /// raw text, CSV, XML, Excel, Avro, ORC) may reach; a larger one fails
    /// with an error naming it instead of exhausting memory. It also bounds one
    /// line of a streamed `jsonl` file, so a file with no newline fails the
    /// same way; `parquet` streams and is not capped. Default 2 GiB.
    #[serde(default = "default_max_object_bytes")]
    pub max_object_bytes: u64,
}

fn default_max_object_bytes() -> u64 {
    faucet_core::file_format::DEFAULT_MAX_OBJECT_BYTES
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

fn default_concurrency() -> usize {
    4
}

impl SftpSourceConfig {
    /// Build a source config from a connection and a remote path, with default
    /// format (`jsonl`) and batch size.
    pub fn new(connection: SftpConnectionConfig, path: impl Into<String>) -> Self {
        Self {
            connection,
            path: path.into(),
            glob: None,
            format: SftpFormat::default(),
            batch_size: DEFAULT_BATCH_SIZE,
            concurrency: default_concurrency(),
            csv: faucet_core::CsvOptions::default(),
            excel: faucet_core::ExcelOptions::default(),
            xml: faucet_core::XmlOptions::default(),
            avro: faucet_core::AvroOptions::default(),
            orc: faucet_core::OrcOptions::default(),
            parquet: faucet_core::ParquetReadOptions::default(),
            max_object_bytes: default_max_object_bytes(),
        }
    }

    /// The per-format option blocks in the shape
    /// [`faucet_core::file_format::decode`] wants.
    ///
    /// Not feature-gated: `fetch` takes it on every path, so it is built even
    /// when no format feature is on (where it is simply unused).
    pub(crate) fn format_options(&self) -> faucet_core::FormatOptions {
        faucet_core::FormatOptions {
            csv: self.csv.clone(),
            excel: self.excel.clone(),
            xml: self.xml.clone(),
            avro: self.avro.clone(),
            orc: self.orc.clone(),
        }
    }

    /// Set the filename glob filter.
    pub fn glob(mut self, glob: impl Into<String>) -> Self {
        self.glob = Some(glob.into());
        self
    }

    /// Set the file format.
    pub fn format(mut self, format: SftpFormat) -> Self {
        self.format = format;
        self
    }

    /// Set the per-page record count.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the maximum number of files read concurrently.
    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }
}

/// Match `name` against a `*` / `?` glob `pattern`.
///
/// `*` matches any run of characters (including empty); `?` matches exactly one
/// character. All other characters match literally. Pure and allocation-light
/// (linear backtracking), used to filter directory listings by basename.
pub fn glob_match(pattern: &str, name: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let n: Vec<char> = name.chars().collect();
    let (mut pi, mut ni) = (0usize, 0usize);
    // Position to backtrack to on a `*` mismatch.
    let (mut star, mut star_n) = (None, 0usize);

    while ni < n.len() {
        if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            star_n = ni;
            pi += 1;
        } else if pi < p.len() && (p[pi] == '?' || p[pi] == n[ni]) {
            pi += 1;
            ni += 1;
        } else if let Some(s) = star {
            pi = s + 1;
            star_n += 1;
            ni = star_n;
        } else {
            return false;
        }
    }
    // Consume trailing `*`s.
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

#[cfg(test)]
mod tests;
