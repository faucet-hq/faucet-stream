#![cfg_attr(docsrs, feature(doc_cfg))]
//! Config types shared by the local file source and sink.

use faucet_core::{FaucetError, FileFormat};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

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

#[cfg(test)]
mod tests {
    use super::*;

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
    fn serde_names_are_snake_case() {
        let c: FileFormatChoice = serde_json::from_str("\"json_lines\"").unwrap();
        assert_eq!(c, FileFormatChoice::JsonLines);
        assert_eq!(
            serde_json::to_string(&FileFormatChoice::Auto).unwrap(),
            "\"auto\""
        );
    }
}
