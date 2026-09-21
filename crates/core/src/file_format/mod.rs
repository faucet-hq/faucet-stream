//! One file-format vocabulary for every file and object-store connector (#604).
//!
//! S3, GCS, Azure Blob and SFTP all move *files*, and the files they move are
//! CSV exports, JSON dumps, XML feeds and Excel workbooks as often as they are
//! JSON Lines. Before this module each connector carried its own small format
//! enum — `JsonLines | JsonArray | RawText | Parquet` on the source side, and
//! only `JsonLines | Parquet` on the sink side — so what you could read depended
//! on which bucket it was in, and what you could write was a strict subset of
//! what you could read. The gap was filled by pre- and post-processing outside
//! the pipeline, which is exactly the work a movement engine exists to absorb.
//!
//! The parsers themselves are not new: the REST source has decoded
//! `json | csv | xlsx | xml` since #497/#515. This module is where they move so
//! that every connector — including a third-party one, which depends only on
//! `faucet-core` — gets the same set, with the same option names and the same
//! edge-case behaviour.
//!
//! # Shape
//!
//! ```yaml
//! source:
//!   type: s3
//!   config:
//!     bucket: exports
//!     prefix: daily/
//!     format: csv                  # FileFormat
//!     compression: auto            # faucet_core::compression
//!     csv: { has_headers: true, delimiter: "," }
//! sink:
//!   type: s3
//!   config:
//!     bucket: reports
//!     format: xlsx
//!     compression: gzip
//! ```
//!
//! # What is deliberately not here
//!
//! **Parquet.** It is columnar, self-describing, and already has a dedicated
//! Arrow read/write path per connector that must not be routed through a
//! `Vec<Value>`. [`FileFormat::Parquet`] therefore names the format for config
//! purposes but [`decode`]/[`encode`] refuse it, so a caller cannot silently
//! lose the columnar fast path by going through the generic helper.
//!
//! **Compression.** [`crate::compression`] already owns codec selection,
//! extension-based `auto` resolution, and the mismatch warning. Format and
//! codec compose; neither needs to know about the other.

use crate::error::FaucetError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[cfg(feature = "file-format-csv")]
pub mod csv;
#[cfg(feature = "file-format-excel")]
pub mod excel;
pub mod json;
#[cfg(feature = "file-format-xml")]
pub mod xml;

/// How the bytes of one object are laid out.
///
/// The variants are the union of what the file connectors could previously read
/// *or* write, so adopting this enum never removes a format from a connector
/// that had it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FileFormat {
    /// One JSON value per line (NDJSON). The default everywhere, because it is
    /// the only format that both streams and round-trips arbitrary JSON.
    #[default]
    JsonLines,
    /// A single JSON array holding every record.
    JsonArray,
    /// Delimited text. See [`CsvOptions`].
    Csv,
    /// XML. See [`XmlOptions`].
    Xml,
    /// An Excel workbook (`.xlsx`). See [`ExcelOptions`].
    Xlsx,
    /// Apache Parquet — named for config, handled by each connector's own
    /// Arrow path, never by [`decode`]/[`encode`].
    Parquet,
    /// Unparsed text: one record per object, `{"text": "<whole body>"}`.
    RawText,
}

impl FileFormat {
    /// The conventional file extension, used to name objects a sink creates
    /// when the user did not pick one.
    pub fn extension(self) -> &'static str {
        match self {
            Self::JsonLines => ".jsonl",
            Self::JsonArray => ".json",
            Self::Csv => ".csv",
            Self::Xml => ".xml",
            Self::Xlsx => ".xlsx",
            Self::Parquet => ".parquet",
            Self::RawText => ".txt",
        }
    }

    /// Whether the whole object must be in memory to produce the first record.
    ///
    /// Callers use this to decide between a streaming read and a buffered one.
    /// It is a property of the format, not of the source: a JSON array has no
    /// record boundary until it is parsed, a zip-container workbook has its
    /// directory at the end, and an XML document is a tree.
    pub fn requires_whole_object(self) -> bool {
        matches!(self, Self::JsonArray | Self::Xml | Self::Xlsx)
    }

    /// The name used in config and error messages.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::JsonLines => "json_lines",
            Self::JsonArray => "json_array",
            Self::Csv => "csv",
            Self::Xml => "xml",
            Self::Xlsx => "xlsx",
            Self::Parquet => "parquet",
            Self::RawText => "raw_text",
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_delimiter() -> String {
    ",".into()
}

/// CSV dialect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CsvOptions {
    /// Field separator. A single character; `"\t"` is accepted for tabs.
    #[serde(default = "default_delimiter")]
    pub delimiter: String,
    /// Whether the first row names the fields. When false, fields are named
    /// `column_0`, `column_1`, … — the same fallback the REST source and the
    /// `csv` connector already use.
    #[serde(default = "default_true")]
    pub has_headers: bool,
}

impl Default for CsvOptions {
    fn default() -> Self {
        Self {
            delimiter: default_delimiter(),
            has_headers: true,
        }
    }
}

impl CsvOptions {
    /// The delimiter as the single byte the CSV readers want.
    ///
    /// Rejected rather than truncated: a multi-byte delimiter silently becoming
    /// its first byte would split every row in the wrong place and produce
    /// plausible-looking garbage.
    pub fn delimiter_byte(&self) -> Result<u8, FaucetError> {
        let d = match self.delimiter.as_str() {
            "\\t" => "\t",
            other => other,
        };
        let bytes = d.as_bytes();
        match bytes.len() {
            1 => Ok(bytes[0]),
            _ => Err(FaucetError::Config(format!(
                "csv.delimiter must be exactly one byte, got {:?}",
                self.delimiter
            ))),
        }
    }
}

/// Excel worksheet selection.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExcelOptions {
    /// Worksheet name, or an index as a string. Default: the first sheet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sheet: Option<String>,
    /// 0-based row supplying the field names.
    #[serde(default)]
    pub header_row: usize,
}

fn default_record_element() -> String {
    "record".into()
}

fn default_root_element() -> String {
    "records".into()
}

/// XML record framing.
///
/// XML has no canonical record boundary, so one must be declared. On read,
/// `record_element` names the repeated element; without it the decoder falls
/// back to "the document's root children", which is right for the common
/// `<rows><row/>…</rows>` shape and wrong often enough that naming the element
/// is recommended in the docs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct XmlOptions {
    /// Element that delimits one record. Read: select these. Write: wrap each
    /// record in one.
    #[serde(default = "default_record_element")]
    pub record_element: String,
    /// Document element wrapping the records. Write only.
    #[serde(default = "default_root_element")]
    pub root_element: String,
}

impl Default for XmlOptions {
    fn default() -> Self {
        Self {
            record_element: default_record_element(),
            root_element: default_root_element(),
        }
    }
}

/// The per-format option blocks, flattened into a connector's config so they
/// appear at its top level (`csv: {...}`, `excel: {...}`, `xml: {...}`).
///
/// Every block is defaulted, so a connector that adopts this struct adds no
/// required field and the change stays a minor bump under the project's
/// [API-evolution policy](https://github.com/faucet-hq/faucet-stream).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FormatOptions {
    /// CSV dialect, used when `format: csv`.
    #[serde(default)]
    pub csv: CsvOptions,
    /// Worksheet selection, used when `format: xlsx`.
    #[serde(default)]
    pub excel: ExcelOptions,
    /// Record framing, used when `format: xml`.
    #[serde(default)]
    pub xml: XmlOptions,
}

/// Decode one object's bytes into records.
///
/// Async because the CSV reader is (`csv-async`), and because keeping one
/// signature for every format is worth more than saving an `.await` on the
/// three that are synchronous.
pub async fn decode(
    bytes: &[u8],
    format: FileFormat,
    opts: &FormatOptions,
) -> Result<Vec<Value>, FaucetError> {
    match format {
        FileFormat::JsonLines => json::decode_lines(bytes),
        FileFormat::JsonArray => json::decode_array(bytes),
        FileFormat::RawText => json::decode_raw_text(bytes),
        FileFormat::Csv => decode_csv(bytes, opts).await,
        FileFormat::Xml => decode_xml(bytes, opts),
        FileFormat::Xlsx => decode_xlsx(bytes, opts),
        FileFormat::Parquet => Err(columnar_refusal("decode")),
    }
}

/// Encode records into one object's bytes.
pub fn encode(
    records: &[Value],
    format: FileFormat,
    opts: &FormatOptions,
) -> Result<Vec<u8>, FaucetError> {
    match format {
        FileFormat::JsonLines => json::encode_lines(records),
        FileFormat::JsonArray => json::encode_array(records),
        FileFormat::RawText => json::encode_raw_text(records),
        FileFormat::Csv => encode_csv(records, opts),
        FileFormat::Xml => encode_xml(records, opts),
        FileFormat::Xlsx => encode_xlsx(records, opts),
        FileFormat::Parquet => Err(columnar_refusal("encode")),
    }
}

/// Parquet never goes through the generic helper.
///
/// Routing it here would work and would silently cost the Arrow fast path and
/// the columnar memory profile, so it is refused rather than accommodated.
fn columnar_refusal(verb: &str) -> FaucetError {
    FaucetError::Config(format!(
        "file_format::{verb}: `parquet` is columnar and is handled by each connector's Arrow \
         path, not the generic record helper"
    ))
}

/// The error a build without the feature reports.
///
/// Named rather than mis-parsed: decoding an Excel workbook as CSV produces
/// records, which is the worst possible outcome.
#[cfg_attr(
    all(
        feature = "file-format-csv",
        feature = "file-format-xml",
        feature = "file-format-excel"
    ),
    allow(dead_code)
)]
fn missing_feature(format: FileFormat, feature: &str) -> FaucetError {
    FaucetError::Config(format!(
        "`format: {}` requires the `{feature}` build feature — rebuild with \
         `--features {feature}`",
        format.as_str()
    ))
}

#[cfg(feature = "file-format-csv")]
async fn decode_csv(bytes: &[u8], opts: &FormatOptions) -> Result<Vec<Value>, FaucetError> {
    csv::decode(bytes, opts.csv.delimiter_byte()?, opts.csv.has_headers).await
}

#[cfg(not(feature = "file-format-csv"))]
async fn decode_csv(_: &[u8], _: &FormatOptions) -> Result<Vec<Value>, FaucetError> {
    Err(missing_feature(FileFormat::Csv, "file-format-csv"))
}

#[cfg(feature = "file-format-csv")]
fn encode_csv(records: &[Value], opts: &FormatOptions) -> Result<Vec<u8>, FaucetError> {
    csv::encode(records, opts.csv.delimiter_byte()?, opts.csv.has_headers)
}

#[cfg(not(feature = "file-format-csv"))]
fn encode_csv(_: &[Value], _: &FormatOptions) -> Result<Vec<u8>, FaucetError> {
    Err(missing_feature(FileFormat::Csv, "file-format-csv"))
}

#[cfg(feature = "file-format-xml")]
fn decode_xml(bytes: &[u8], opts: &FormatOptions) -> Result<Vec<Value>, FaucetError> {
    xml::decode(bytes, &opts.xml.record_element)
}

#[cfg(not(feature = "file-format-xml"))]
fn decode_xml(_: &[u8], _: &FormatOptions) -> Result<Vec<Value>, FaucetError> {
    Err(missing_feature(FileFormat::Xml, "file-format-xml"))
}

#[cfg(feature = "file-format-xml")]
fn encode_xml(records: &[Value], opts: &FormatOptions) -> Result<Vec<u8>, FaucetError> {
    xml::encode(records, &opts.xml.root_element, &opts.xml.record_element)
}

#[cfg(not(feature = "file-format-xml"))]
fn encode_xml(_: &[Value], _: &FormatOptions) -> Result<Vec<u8>, FaucetError> {
    Err(missing_feature(FileFormat::Xml, "file-format-xml"))
}

#[cfg(feature = "file-format-excel")]
fn decode_xlsx(bytes: &[u8], opts: &FormatOptions) -> Result<Vec<Value>, FaucetError> {
    excel::decode(bytes, opts.excel.sheet.as_deref(), opts.excel.header_row)
}

#[cfg(not(feature = "file-format-excel"))]
fn decode_xlsx(_: &[u8], _: &FormatOptions) -> Result<Vec<Value>, FaucetError> {
    Err(missing_feature(FileFormat::Xlsx, "file-format-excel"))
}

#[cfg(feature = "file-format-excel")]
fn encode_xlsx(records: &[Value], opts: &FormatOptions) -> Result<Vec<u8>, FaucetError> {
    excel::encode(records, opts.excel.sheet.as_deref())
}

#[cfg(not(feature = "file-format-excel"))]
fn encode_xlsx(_: &[Value], _: &FormatOptions) -> Result<Vec<u8>, FaucetError> {
    Err(missing_feature(FileFormat::Xlsx, "file-format-excel"))
}

/// Field names across `records`, in first-seen order.
///
/// A union rather than the first record's keys, so a later record carrying an
/// extra field widens the file instead of losing the field silently.
///
/// "First-seen" orders *records* deterministically; within one record the order
/// is whatever `serde_json::Map` iterates, which is insertion order only when
/// the `preserve_order` feature is unified into the build and sorted otherwise.
/// Column order is therefore a build-time property, not a guarantee — do not
/// write a test that pins it.
pub fn header_union(records: &[Value]) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for r in records {
        if let Value::Object(map) = r {
            for k in map.keys() {
                if seen.insert(k.clone()) {
                    out.push(k.clone());
                }
            }
        }
    }
    out
}

/// One cell's text for the tabular formats.
///
/// A nested object or array is re-serialized as JSON rather than dropped or
/// `Debug`-printed: a spreadsheet cell cannot hold structure, and the round
/// trip back through `json_parse` is at least lossless.
pub fn cell_text(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Every variant's extension and wire name, so adding a format without
    /// giving it both is a test failure rather than a `.txt` file called
    /// "raw_text".
    #[test]
    fn every_variant_has_an_extension_and_a_wire_name() {
        let all = [
            (FileFormat::JsonLines, ".jsonl", "json_lines"),
            (FileFormat::JsonArray, ".json", "json_array"),
            (FileFormat::Csv, ".csv", "csv"),
            (FileFormat::Xml, ".xml", "xml"),
            (FileFormat::Xlsx, ".xlsx", "xlsx"),
            (FileFormat::Parquet, ".parquet", "parquet"),
            (FileFormat::RawText, ".txt", "raw_text"),
        ];
        for (f, ext, name) in all {
            assert_eq!(f.extension(), ext, "{f:?}");
            assert_eq!(f.as_str(), name, "{f:?}");
        }
    }

    /// `raw_text` is source-only in the connectors, but the shared helper
    /// still round-trips it — the sink side is what `encode_raw_text` exists
    /// for, and routing it through the top-level dispatch is how a connector
    /// reaches it.
    #[tokio::test]
    async fn raw_text_round_trips_through_the_top_level_dispatch() {
        let opts = FormatOptions::default();
        let recs = decode(b"hello", FileFormat::RawText, &opts)
            .await
            .expect("decode");
        assert_eq!(recs, vec![json!({"text": "hello"})]);
        let bytes = encode(&recs, FileFormat::RawText, &opts).expect("encode");
        assert_eq!(bytes, b"hello\n");
    }

    /// The refusal a build without the feature emits — it must name the
    /// format and the feature, because mis-parsing an Excel blob as CSV is
    /// the outcome this exists to prevent.
    #[test]
    fn a_missing_feature_is_named_not_guessed() {
        let err = missing_feature(FileFormat::Xlsx, "file-format-excel");
        let msg = err.to_string();
        assert!(msg.contains("xlsx"), "{msg}");
        assert!(msg.contains("file-format-excel"), "{msg}");
    }

    #[test]
    fn extensions_and_names_are_stable() {
        assert_eq!(FileFormat::default(), FileFormat::JsonLines);
        assert_eq!(FileFormat::Csv.extension(), ".csv");
        assert_eq!(FileFormat::Xlsx.as_str(), "xlsx");
    }

    #[test]
    fn whole_object_formats_are_the_ones_without_a_record_boundary() {
        for f in [FileFormat::JsonArray, FileFormat::Xml, FileFormat::Xlsx] {
            assert!(f.requires_whole_object(), "{f:?}");
        }
        for f in [
            FileFormat::JsonLines,
            FileFormat::Csv,
            FileFormat::RawText,
            FileFormat::Parquet,
        ] {
            assert!(!f.requires_whole_object(), "{f:?}");
        }
    }

    #[tokio::test]
    async fn parquet_is_refused_on_both_directions() {
        // Going through the generic helper would work and would silently cost
        // the Arrow path, so it must be an error, not a fallback.
        let err = decode(b"", FileFormat::Parquet, &FormatOptions::default())
            .await
            .expect_err("decode refuses parquet");
        assert!(err.to_string().contains("columnar"), "{err}");
        let err = encode(&[], FileFormat::Parquet, &FormatOptions::default())
            .expect_err("encode refuses parquet");
        assert!(err.to_string().contains("columnar"), "{err}");
    }

    #[test]
    fn delimiter_must_be_one_byte() {
        let mut o = CsvOptions::default();
        assert_eq!(o.delimiter_byte().expect("default"), b',');
        o.delimiter = "\\t".into();
        assert_eq!(o.delimiter_byte().expect("tab"), b'\t');
        o.delimiter = "||".into();
        let err = o.delimiter_byte().expect_err("multi-byte");
        assert!(err.to_string().contains("exactly one byte"), "{err}");
        // A multi-byte character is rejected too — truncating it to its first
        // UTF-8 byte would split rows at a byte that never appears alone.
        o.delimiter = "§".into();
        assert!(o.delimiter_byte().is_err());
    }

    #[test]
    fn header_union_widens_across_records_and_ignores_non_objects() {
        // Records are visited in order, so a key only this record has lands
        // after the earlier ones. Intra-record order is a build-time property
        // of `serde_json::Map`, so it is deliberately not asserted.
        let recs = vec![json!({"a": 1}), json!({"b": 2}), json!("not an object")];
        assert_eq!(header_union(&recs), vec!["a", "b"]);
        // A key seen twice appears once.
        assert_eq!(header_union(&[json!({"a": 1}), json!({"a": 2})]), vec!["a"]);
        assert!(header_union(&[json!([1, 2])]).is_empty());
    }

    #[test]
    fn cells_render_scalars_plainly_and_structure_as_json() {
        assert_eq!(cell_text(&Value::Null), "");
        assert_eq!(cell_text(&json!("x")), "x");
        assert_eq!(cell_text(&json!(3)), "3");
        assert_eq!(cell_text(&json!(true)), "true");
        assert_eq!(cell_text(&json!({"k": 1})), r#"{"k":1}"#);
    }
}
