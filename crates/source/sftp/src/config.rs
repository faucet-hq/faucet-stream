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
}

impl SftpFormat {
    /// The shared format this variant maps onto, or `None` for `RawText`,
    /// whose `{path, content}` envelope is the connector's own shape.
    #[cfg(any(
        feature = "file-format-csv",
        feature = "file-format-xml",
        feature = "file-format-excel"
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
        if pi < p.len() && (p[pi] == '?' || p[pi] == n[ni]) {
            pi += 1;
            ni += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            star_n = ni;
            pi += 1;
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
mod tests {
    use super::*;
    use faucet_common_sftp::SftpConnectionConfig;

    fn conn() -> SftpConnectionConfig {
        SftpConnectionConfig::with_password("h", "u", "p")
    }

    #[test]
    fn defaults() {
        let cfg = SftpSourceConfig::new(conn(), "/data");
        assert_eq!(cfg.path, "/data");
        assert!(cfg.glob.is_none());
        assert_eq!(cfg.format, SftpFormat::Jsonl);
        assert_eq!(cfg.batch_size, DEFAULT_BATCH_SIZE);
        assert_eq!(cfg.concurrency, 4);
    }

    #[test]
    fn concurrency_is_settable_and_defaults_when_omitted() {
        // The field is new (#619); an existing config that does not mention it
        // must keep working and pick up the default rather than landing at 0,
        // which would mean "no reads in flight".
        let cfg = SftpSourceConfig::new(conn(), "/data").concurrency(12);
        assert_eq!(cfg.concurrency, 12);

        let parsed: SftpSourceConfig = serde_json::from_str(
            r#"{"host":"h","username":"u","type":"password",
                "config":{"password":"p"},"path":"/data"}"#,
        )
        .expect("a config without `concurrency` must still deserialize");
        assert_eq!(parsed.concurrency, 4);
    }

    #[test]
    fn format_default_is_jsonl() {
        assert_eq!(SftpFormat::default(), SftpFormat::Jsonl);
    }

    #[test]
    fn deserializes_flat_shape() {
        let json = r#"{
            "host": "sftp.example.com",
            "port": 2222,
            "username": "user",
            "type": "password",
            "config": { "password": "secret" },
            "path": "/incoming",
            "glob": "*.jsonl",
            "format": "jsonl",
            "batch_size": 250
        }"#;
        let cfg: SftpSourceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.connection.host, "sftp.example.com");
        assert_eq!(cfg.connection.port, 2222);
        assert_eq!(cfg.path, "/incoming");
        assert_eq!(cfg.glob.as_deref(), Some("*.jsonl"));
        assert_eq!(cfg.batch_size, 250);
    }

    #[test]
    fn batch_size_zero_is_valid_sentinel() {
        let cfg = SftpSourceConfig::new(conn(), "/d").with_batch_size(0);
        assert_eq!(cfg.batch_size, 0);
        assert!(faucet_core::validate_batch_size(cfg.batch_size).is_ok());
    }

    #[test]
    fn format_parses_all_variants() {
        for (s, want) in [
            ("jsonl", SftpFormat::Jsonl),
            ("json_array", SftpFormat::JsonArray),
            ("raw_text", SftpFormat::RawText),
        ] {
            let got: SftpFormat = serde_json::from_str(&format!("\"{s}\"")).unwrap();
            assert_eq!(got, want);
        }
    }

    #[test]
    fn glob_literal_and_wildcards() {
        assert!(glob_match("*.jsonl", "orders.jsonl"));
        assert!(glob_match("*.jsonl", ".jsonl"));
        assert!(!glob_match("*.jsonl", "orders.json"));
        assert!(glob_match("data-?.csv", "data-1.csv"));
        assert!(!glob_match("data-?.csv", "data-12.csv"));
        assert!(glob_match("*", "anything"));
        assert!(glob_match("exact.txt", "exact.txt"));
        assert!(!glob_match("exact.txt", "other.txt"));
        assert!(glob_match("a*b*c", "axxbyyc"));
        assert!(!glob_match("a*b*c", "axxbyy"));
    }

    /// Off-by-one boundary cases a backtracking matcher classically gets wrong
    /// (empty inputs, exhausted input against a trailing `?`, pattern longer
    /// than the name). Pure test hardening — the current matcher already passes.
    #[test]
    fn glob_boundary_cases() {
        assert!(glob_match("", ""), "empty pattern matches empty name");
        assert!(
            !glob_match("", "x"),
            "empty pattern must not match a non-empty name"
        );
        assert!(!glob_match("a?", "a"), "trailing `?` needs one more char");
        assert!(
            !glob_match("abc", "ab"),
            "pattern longer than name, no star"
        );
        assert!(glob_match("*", ""), "a lone star matches the empty name");
        assert!(glob_match("a*", "a"), "trailing star may match nothing");
        assert!(
            glob_match("**", ""),
            "consecutive stars collapse and match empty"
        );
        assert!(
            glob_match("?", "x"),
            "a single `?` matches exactly one char"
        );
        assert!(!glob_match("?", ""), "a single `?` needs a char");
    }
}
