#![cfg_attr(docsrs, feature(doc_cfg))]
//! Config types shared by the local file source and sink, and the shared
//! file-writing layer ([`write`](mod@write)) every file-writing sink builds on.
//!
//! **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
//! minor release; any change is called out in the changelog.

use faucet_core::compression::{Compression, CompressionConfig};
use faucet_core::{FaucetError, FileFormat};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[cfg(feature = "encryption")]
pub mod sealed_lines;
pub mod write;

#[doc(hidden)]
pub mod __private {
    pub use faucet_core::async_trait;
    pub use faucet_core::check::{CheckContext, CheckReport};
    pub use faucet_core::observability::RoundtripRecorder;
    pub use faucet_core::{BatchAtomicity, FaucetError, LocalOutput, Sink, WriteMode};
    pub use serde_json::Value;
}

/// Which format a file is in: resolved from its extension (`auto`) or fixed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FileFormatChoice {
    /// Resolve per file from its extension, looking through a compression
    /// suffix (`.csv.gz` is CSV): `.jsonl`/`.ndjson`, `.json`, `.csv`, `.xml`,
    /// `.xlsx`, `.parquet`, `.avro`, `.orc`, `.txt`.
    #[default]
    Auto,
    /// One JSON value per line.
    JsonLines,
    /// A JSON array (a lone object is one record).
    JsonArray,
    /// Delimited text; dialect from `csv`.
    Csv,
    /// XML; record framing from `xml`.
    Xml,
    /// An Excel workbook; sheet from `excel`.
    Xlsx,
    /// Apache Parquet.
    Parquet,
    /// Unparsed text, one record per file.
    RawText,
    /// Apache Avro Object Container File; schema and codec from `avro`.
    Avro,
    /// Apache ORC (read-only); projection from `orc`.
    Orc,
}

impl FileFormatChoice {
    /// The explicit format, or `None` for `auto`.
    pub fn explicit(self) -> Option<FileFormat> {
        Some(match self {
            Self::Auto => return None,
            Self::JsonLines => FileFormat::JsonLines,
            Self::JsonArray => FileFormat::JsonArray,
            Self::Csv => FileFormat::Csv,
            Self::Xml => FileFormat::Xml,
            Self::Xlsx => FileFormat::Xlsx,
            Self::Parquet => FileFormat::Parquet,
            Self::RawText => FileFormat::RawText,
            Self::Avro => FileFormat::Avro,
            Self::Orc => FileFormat::Orc,
        })
    }

    /// The format of the file called `name` (a path or a URL's last segment).
    ///
    /// `Ok(None)` when `auto` finds no format and `strict` is off — the caller
    /// skips the file; with `strict` it is an error naming the file.
    pub fn resolve(self, name: &str, strict: bool) -> Result<Option<FileFormat>, FaucetError> {
        if let Some(f) = self.explicit() {
            return Ok(Some(f));
        }
        match FileFormat::from_path(name) {
            Some(f) => Ok(Some(f)),
            None if strict => Err(FaucetError::Config(format!(
                "'{name}' has no recognised extension and `strict` is on; set `format`"
            ))),
            None => Ok(None),
        }
    }
}

/// Extensions that name a format a file sink can write, for error messages.
pub const WRITABLE_EXTENSIONS: &str = ".jsonl .json .csv .xml .xlsx .avro .parquet .txt";

impl FileFormatChoice {
    /// The format a file called `name` is **written** in: the explicit format,
    /// or the one its extension names. Refuses a name with no recognised
    /// extension and ORC, which has no writer.
    pub fn resolve_writable(self, name: &str) -> Result<FileFormat, FaucetError> {
        let format = self.resolve(name, false)?.ok_or_else(|| {
            FaucetError::Config(format!(
                "'{name}' has no extension naming a writable format — use one of \
                 {WRITABLE_EXTENSIONS} (optionally + .gz/.zst) or set `format`"
            ))
        })?;
        if !format.is_writable() && format != FileFormat::Parquet {
            return Err(FaucetError::Config(format!(
                "`{}` is read-only — there is no {} writer; write Parquet for a columnar output",
                format.as_str(),
                format.as_str().to_ascii_uppercase()
            )));
        }
        Ok(format)
    }
}

/// The compression codec for the file at `path` (a local path or a URL),
/// resolved from its suffix under `auto`.
pub fn resolve_compression(config: CompressionConfig, path: &str) -> Compression {
    config.resolve(resolution_name(path))
}

/// Formats that compress internally (Parquet column chunks, Avro blocks, the
/// xlsx zip container), so file-level compression does not apply on write.
pub fn compresses_internally(format: FileFormat) -> bool {
    matches!(
        format,
        FileFormat::Parquet | FileFormat::Avro | FileFormat::Xlsx
    )
}

/// Formats a writer can add records to without rewriting the file.
pub fn appendable(format: FileFormat) -> bool {
    matches!(
        format,
        FileFormat::JsonLines | FileFormat::Csv | FileFormat::RawText
    )
}

/// Whether `path` names a directory (ends in a separator) rather than a file.
pub fn is_directory_path(path: &str) -> bool {
    path.ends_with('/') || path.ends_with('\\')
}

/// Refuse an empty or blank `path`, naming the connector.
pub fn require_path(connector: &str, path: &str) -> Result<(), FaucetError> {
    if path.trim().is_empty() {
        return Err(FaucetError::Config(format!("{connector}: `path` is empty")));
    }
    Ok(())
}

/// Prefix a [`FaucetError::Config`] message with `connector`; any other
/// error passes through unchanged.
pub fn config_context(connector: &str, e: FaucetError) -> FaucetError {
    match e {
        FaucetError::Config(m) => FaucetError::Config(format!("{connector}: {m}")),
        other => other,
    }
}

/// Whether `path` is an `http://` or `https://` URL.
pub fn is_http_path(path: &str) -> bool {
    let p = path.trim_start().to_ascii_lowercase();
    p.starts_with("http://") || p.starts_with("https://")
}

/// The last path segment of a URL, without its query or fragment — the part
/// that names the file for format and compression resolution.
pub fn url_file_name(url: &str) -> &str {
    let no_query = url.split(['?', '#']).next().unwrap_or(url);
    no_query.rsplit('/').next().unwrap_or(no_query)
}

/// The name format and compression are resolved from: the URL's file name
/// for an `http(s)://` path, the path itself otherwise.
pub fn resolution_name(path: &str) -> &str {
    if is_http_path(path) {
        url_file_name(path)
    } else {
        path
    }
}

/// Which listed objects an object-store source reads.
///
/// A folder marker (a zero-byte key ending in `/`) is never data. Without an
/// `include` glob, a key with a path segment below the prefix that starts
/// with `_` or `.` — `_SUCCESS`, `_temporary/…`, `.crc` files — is skipped
/// too; with one, exactly the keys the glob matches are read.
#[derive(Debug, Clone, Default)]
pub struct ObjectFilter {
    include: Option<glob::Pattern>,
}

impl ObjectFilter {
    /// Compile the optional `include` glob (matched against the whole key).
    pub fn new(include: Option<&str>) -> Result<Self, FaucetError> {
        let include = include
            .map(|g| {
                glob::Pattern::new(g)
                    .map_err(|e| FaucetError::Config(format!("`include` glob {g:?}: {e}")))
            })
            .transpose()?;
        Ok(Self { include })
    }

    /// Whether to read `key` (`size` in bytes, when the listing reports it)
    /// listed under `prefix`.
    pub fn keep(&self, key: &str, size: Option<u64>, prefix: &str) -> bool {
        if key.ends_with('/') && size.unwrap_or(0) == 0 {
            return false;
        }
        match &self.include {
            Some(p) => p.matches_with(
                key,
                glob::MatchOptions {
                    case_sensitive: true,
                    require_literal_separator: false,
                    require_literal_leading_dot: false,
                },
            ),
            None => !is_hidden_object(key, prefix),
        }
    }
}

/// Whether a path segment of `key` below `prefix` starts with `_` or `.`.
pub fn is_hidden_object(key: &str, prefix: &str) -> bool {
    let dir = &prefix[..prefix.rfind('/').map_or(0, |i| i + 1)];
    key.strip_prefix(dir)
        .unwrap_or(key)
        .split('/')
        .any(|s| s.starts_with('_') || s.starts_with('.'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn markers_and_hidden_objects_are_skipped_unless_included() {
        let f = ObjectFilter::default();
        assert!(f.keep("out/part-0001.parquet", Some(10), "out/"));
        assert!(!f.keep("out/_SUCCESS", Some(0), "out/"));
        assert!(!f.keep("out/.part-0001.parquet.crc", Some(8), "out/"));
        assert!(!f.keep("out/_temporary/0/part-1.json", Some(8), "out/"));
        assert!(!f.keep("out/sub/", Some(0), "out/"));
        assert!(!f.keep("out/sub/", None, "out/"));
        assert!(
            f.keep("_raw/data.json", Some(4), "_raw/"),
            "the prefix itself is not judged"
        );
        assert!(f.keep("_raw/part-1.json", Some(4), "_raw/part"));
        assert!(!f.keep("data/_x.json", Some(4), ""));
        let inc = ObjectFilter::new(Some("out/_*")).unwrap();
        assert!(inc.keep("out/_metadata", Some(4), "out/"));
        assert!(!inc.keep("out/part-1.parquet", Some(4), "out/"));
        assert!(ObjectFilter::new(Some("[")).is_err());
    }

    #[test]
    fn every_choice_maps_onto_the_shared_vocabulary() {
        let all = [
            (FileFormatChoice::JsonLines, FileFormat::JsonLines),
            (FileFormatChoice::JsonArray, FileFormat::JsonArray),
            (FileFormatChoice::Csv, FileFormat::Csv),
            (FileFormatChoice::Xml, FileFormat::Xml),
            (FileFormatChoice::Xlsx, FileFormat::Xlsx),
            (FileFormatChoice::Parquet, FileFormat::Parquet),
            (FileFormatChoice::RawText, FileFormat::RawText),
            (FileFormatChoice::Avro, FileFormat::Avro),
            (FileFormatChoice::Orc, FileFormat::Orc),
        ];
        for (c, f) in all {
            assert_eq!(c.explicit(), Some(f));
            assert_eq!(c.resolve("x.unknown", true).unwrap(), Some(f));
        }
        assert_eq!(FileFormatChoice::default().explicit(), None);
    }

    #[test]
    fn auto_resolves_by_extension_and_strict_refuses_unknowns() {
        let auto = FileFormatChoice::Auto;
        assert_eq!(
            auto.resolve("a/b.csv.gz", false).unwrap(),
            Some(FileFormat::Csv)
        );
        assert_eq!(auto.resolve("notes.md", false).unwrap(), None);
        let err = auto.resolve("notes.md", true).unwrap_err();
        assert!(err.to_string().contains("notes.md"), "{err}");
    }

    #[test]
    fn urls_and_paths() {
        assert!(is_http_path("HTTPS://h/x") && is_http_path("http://h"));
        assert!(!is_http_path("/tmp/x") && !is_http_path("ftp://h"));
        assert_eq!(url_file_name("https://h/a/b.csv.gz?sig=1#x"), "b.csv.gz");
        assert_eq!(url_file_name("https://h/"), "");
        assert_eq!(resolution_name("https://h/a.avro?x=1"), "a.avro");
        assert_eq!(resolution_name("dir/a.avro"), "dir/a.avro");
    }

    #[test]
    fn writable_resolution_refuses_unknown_extensions_and_orc() {
        let auto = FileFormatChoice::Auto;
        assert_eq!(auto.resolve_writable("a.csv.gz").unwrap(), FileFormat::Csv);
        assert_eq!(
            auto.resolve_writable("a.parquet").unwrap(),
            FileFormat::Parquet
        );
        let e = auto.resolve_writable("a.dat").unwrap_err().to_string();
        assert!(e.contains("a.dat") && e.contains(".jsonl"), "{e}");
        let e = auto.resolve_writable("a.orc").unwrap_err().to_string();
        assert!(e.contains("read-only"), "{e}");
        let e = FileFormatChoice::Orc
            .resolve_writable("x")
            .unwrap_err()
            .to_string();
        assert!(e.contains("ORC"), "{e}");
        assert_eq!(
            FileFormatChoice::Xml.resolve_writable("a.dat").unwrap(),
            FileFormat::Xml
        );
    }

    #[test]
    fn compression_and_format_properties() {
        assert_eq!(
            resolve_compression(CompressionConfig::Auto, "a.jsonl.gz"),
            Compression::Gzip
        );
        assert_eq!(
            resolve_compression(CompressionConfig::Auto, "https://h/a.csv.zst?x=1"),
            Compression::Zstd
        );
        assert_eq!(
            resolve_compression(CompressionConfig::Auto, "a.csv"),
            Compression::None
        );
        assert_eq!(
            resolve_compression(CompressionConfig::Gzip, "a.csv"),
            Compression::Gzip
        );
        for f in [FileFormat::Parquet, FileFormat::Avro, FileFormat::Xlsx] {
            assert!(compresses_internally(f) && !appendable(f), "{f:?}");
        }
        for f in [FileFormat::JsonLines, FileFormat::Csv, FileFormat::RawText] {
            assert!(appendable(f) && !compresses_internally(f), "{f:?}");
        }
        assert!(!appendable(FileFormat::JsonArray) && !compresses_internally(FileFormat::Xml));
    }

    #[test]
    fn paths_directories_and_emptiness() {
        assert!(is_directory_path("out/") && is_directory_path("out\\"));
        assert!(!is_directory_path("out/a.jsonl"));
        assert!(require_path("file sink", "a").is_ok());
        let e = require_path("file sink", "  ").unwrap_err().to_string();
        assert!(e.contains("file sink") && e.contains("empty"), "{e}");
    }

    #[test]
    fn serde_names_are_snake_case() {
        let c: FileFormatChoice = serde_json::from_str("\"json_lines\"").unwrap();
        assert_eq!(c, FileFormatChoice::JsonLines);
        assert_eq!(
            serde_json::to_string(&FileFormatChoice::Auto).unwrap(),
            "\"auto\""
        );
    }

    #[test]
    fn config_context_prefixes_only_config_errors() {
        assert_eq!(
            config_context("S3 sink", FaucetError::Config("bad".into())).to_string(),
            FaucetError::Config("S3 sink: bad".into()).to_string()
        );
        assert!(matches!(
            config_context("S3 sink", FaucetError::Sink("io".into())),
            FaucetError::Sink(m) if m == "io"
        ));
    }
}
