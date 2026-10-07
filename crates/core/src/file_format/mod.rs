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

#[cfg(feature = "file-format-avro")]
pub mod avro;
pub mod container;
#[cfg(feature = "file-format-csv")]
pub mod csv;
#[cfg(feature = "file-format-excel")]
pub mod excel;
pub mod json;
#[cfg(feature = "file-format-orc")]
pub mod orc;
pub mod parquet_io;
#[cfg(feature = "file-format-xml")]
pub mod xml;

pub use container::{ContainerDecoder, FileInput};

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
    /// Apache Avro Object Container File. See [`AvroOptions`]. Read and
    /// write; decodes to records or, with `arrow`, straight to Arrow batches.
    Avro,
    /// Apache ORC. See [`OrcOptions`]. **Read-only** — see the `orc` module
    /// for why there is no writer.
    Orc,
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
            Self::Avro => ".avro",
            Self::Orc => ".orc",
        }
    }

    /// Whether the whole object must be in memory to produce the first record.
    ///
    /// Callers use this to decide between a streaming read and a buffered one.
    /// It is a property of the format, not of the source: a JSON array has no
    /// record boundary until it is parsed, a zip-container workbook has its
    /// directory at the end, and an XML document is a tree.
    pub fn requires_whole_object(self) -> bool {
        matches!(self, Self::JsonArray | Self::Xml | Self::Xlsx | Self::Orc)
    }

    /// Whether the format is a self-describing binary container decoded by
    /// [`ContainerDecoder`] (Avro, ORC) rather than by [`decode`] per object.
    pub fn is_container(self) -> bool {
        matches!(self, Self::Avro | Self::Orc)
    }

    /// Whether a sink can write this format through [`encode`].
    pub fn is_writable(self) -> bool {
        !matches!(self, Self::Orc | Self::Parquet)
    }

    /// The format a file name's extension names, looking through a trailing
    /// compression suffix (`.gz`, `.gzip`, `.zst`, `.zstd`), so
    /// `export.csv.gz` is CSV. `None` for an extension no format claims.
    ///
    /// `.json` is a JSON array (a lone object is one record); `.jsonl` and
    /// `.ndjson` are JSON Lines; `.tsv` is not claimed, because its delimiter
    /// is an option rather than a format.
    pub fn from_path(path: &str) -> Option<Self> {
        let name = path
            .rsplit(['/', '\\'])
            .next()
            .unwrap_or(path)
            .to_ascii_lowercase();
        let stem = ["gz", "gzip", "zst", "zstd"]
            .iter()
            .find_map(|c| name.strip_suffix(&format!(".{c}")))
            .unwrap_or(&name);
        let ext = stem.rsplit_once('.')?.1;
        Some(match ext {
            "jsonl" | "ndjson" => Self::JsonLines,
            "json" => Self::JsonArray,
            "csv" => Self::Csv,
            "xml" => Self::Xml,
            "xlsx" => Self::Xlsx,
            "parquet" => Self::Parquet,
            "txt" => Self::RawText,
            "avro" => Self::Avro,
            "orc" => Self::Orc,
            _ => return None,
        })
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
            Self::Avro => "avro",
            Self::Orc => "orc",
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
    /// `csv` connector already use. On write it decides whether a header row
    /// is written.
    #[serde(default = "default_true")]
    pub has_headers: bool,
    /// Quote character. A single byte; default `"`.
    #[serde(default = "default_quote")]
    pub quote: String,
    /// Whether rows may have a different number of fields than the header.
    /// `false` fails the read on the first ragged row, naming its line; `true`
    /// keeps the fields the row has. Unset, each connector picks: the `file`
    /// source is strict, the object-store sources and the REST source are
    /// lenient.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flexible: Option<bool>,
    /// Cell values read as `null` instead of a string (e.g. `["", "NULL"]`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub null_values: Vec<String>,
    /// What a streaming CSV writer does with a field the header does not
    /// have. See [`CsvUnknownField`]. Whole-object encoders always write the
    /// union of every record's fields, so it only affects the `file` sink.
    #[serde(default)]
    pub on_unknown_field: CsvUnknownField,
}

/// What a CSV writer does with a field that is not in the header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CsvUnknownField {
    /// Add it as a new column; earlier rows get an empty cell for it.
    #[default]
    Widen,
    /// Fix the header from the first page and drop the field, logging a
    /// warning once per field.
    Warn,
    /// Fix the header from the first page and fail the write naming the field.
    Error,
}

fn default_quote() -> String {
    "\"".into()
}

impl Default for CsvOptions {
    fn default() -> Self {
        Self {
            delimiter: default_delimiter(),
            has_headers: true,
            quote: default_quote(),
            flexible: None,
            null_values: Vec::new(),
            on_unknown_field: CsvUnknownField::Widen,
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
        single_byte("delimiter", &self.delimiter)
    }

    /// The quote character as a single byte.
    pub fn quote_byte(&self) -> Result<u8, FaucetError> {
        single_byte("quote", &self.quote)
    }

    /// Whether ragged rows are accepted, with `default` when unset.
    pub fn flexible_or(&self, default: bool) -> bool {
        self.flexible.unwrap_or(default)
    }

    /// Check the single-byte fields, so a bad dialect fails at load time.
    pub fn validate(&self) -> Result<(), FaucetError> {
        self.delimiter_byte()?;
        self.quote_byte()?;
        Ok(())
    }
}

fn single_byte(name: &str, value: &str) -> Result<u8, FaucetError> {
    let d = match value {
        "\\t" => "\t",
        other => other,
    };
    match d.as_bytes() {
        [b] => Ok(*b),
        _ => Err(FaucetError::Config(format!(
            "csv.{name} must be exactly one byte, got {value:?}"
        ))),
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

/// Block compression for written files.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AvroCodec {
    /// Uncompressed blocks — the Avro default and the most portable.
    #[default]
    Null,
    /// Raw deflate (RFC 1951), readable by every Avro implementation.
    Deflate,
    /// Snappy with the per-block CRC-32 the spec requires.
    Snappy,
    /// Zstandard.
    Zstd,
}

/// Avro read/write options.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AvroOptions {
    /// An Avro schema (the JSON form: an object, an array for a union, or a
    /// primitive name). On a **source** it is the *reader* schema every file
    /// is resolved against — projection, aliases, defaults for added fields.
    /// On a **sink** it is the *writer* schema; without it the schema is
    /// inferred from the records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<Value>,
    /// Block codec for written files: `null` (default), `deflate`, `snappy`,
    /// `zstd`. Reading detects the codec from the file header.
    #[serde(default)]
    pub codec: AvroCodec,
}

/// ORC read options.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OrcOptions {
    /// Top-level columns to read, in file order. Default: every column. A
    /// name the file does not have is an error, not an empty column.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<String>>,
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
    /// Reader / writer schema and codec, used when `format: avro`.
    #[serde(default)]
    pub avro: AvroOptions,
    /// Column projection, used when `format: orc`.
    #[serde(default)]
    pub orc: OrcOptions,
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
        FileFormat::Avro => decode_avro(bytes, opts),
        FileFormat::Orc => decode_orc(bytes, opts),
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
        FileFormat::Avro => encode_avro(records, opts),
        FileFormat::Orc => Err(FaucetError::Config(
            "`format: orc` is read-only — there is no ORC writer; write Parquet for a columnar \
             output"
                .into(),
        )),
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

/// Default ceiling on one file's size once decompressed, for formats read
/// whole into memory (2 GiB).
pub const DEFAULT_MAX_OBJECT_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Read `reader` to the end, failing once it yields more than `max` bytes — so
/// a decompression bomb fails the file instead of exhausting memory. `what`
/// names the file in the error.
pub async fn read_to_end_capped<R>(reader: R, max: u64, what: &str) -> Result<Vec<u8>, FaucetError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt as _;
    let mut buf = Vec::new();
    reader
        .take(max.saturating_add(1))
        .read_to_end(&mut buf)
        .await
        .map_err(|e| FaucetError::Source(format!("read error for '{what}': {e}")))?;
    check_object_size(buf.len() as u64, max, what)?;
    Ok(buf)
}

/// [`read_to_end_capped`] for a UTF-8 text body.
pub async fn read_to_string_capped<R>(
    reader: R,
    max: u64,
    what: &str,
) -> Result<String, FaucetError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let bytes = read_to_end_capped(reader, max, what).await?;
    String::from_utf8(bytes)
        .map_err(|e| FaucetError::Source(format!("'{what}' is not valid UTF-8: {e}")))
}

/// The error for a file past `max_object_bytes`.
pub fn check_object_size(len: u64, max: u64, what: &str) -> Result<(), FaucetError> {
    if len > max {
        return Err(FaucetError::Source(format!(
            "'{what}' is larger than `max_object_bytes` ({max} bytes) once decompressed; \
             raise `max_object_bytes` to read it"
        )));
    }
    Ok(())
}

/// The error a build without the feature reports.
///
/// Named rather than mis-parsed: decoding an Excel workbook as CSV produces
/// records, which is the worst possible outcome.
#[cfg_attr(
    all(
        feature = "file-format-csv",
        feature = "file-format-xml",
        feature = "file-format-excel",
        feature = "file-format-avro",
        feature = "file-format-orc"
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
    csv::decode_with(bytes, &opts.csv, true).await
}

#[cfg(not(feature = "file-format-csv"))]
async fn decode_csv(_: &[u8], _: &FormatOptions) -> Result<Vec<Value>, FaucetError> {
    Err(missing_feature(FileFormat::Csv, "file-format-csv"))
}

#[cfg(feature = "file-format-csv")]
fn encode_csv(records: &[Value], opts: &FormatOptions) -> Result<Vec<u8>, FaucetError> {
    csv::encode_with(records, &opts.csv)
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

#[cfg(feature = "file-format-avro")]
fn decode_avro(bytes: &[u8], opts: &FormatOptions) -> Result<Vec<Value>, FaucetError> {
    avro::decode(bytes, &opts.avro)
}

#[cfg(not(feature = "file-format-avro"))]
fn decode_avro(_: &[u8], _: &FormatOptions) -> Result<Vec<Value>, FaucetError> {
    Err(missing_feature(FileFormat::Avro, "file-format-avro"))
}

#[cfg(feature = "file-format-avro")]
fn encode_avro(records: &[Value], opts: &FormatOptions) -> Result<Vec<u8>, FaucetError> {
    avro::encode(records, &opts.avro)
}

#[cfg(not(feature = "file-format-avro"))]
fn encode_avro(_: &[Value], _: &FormatOptions) -> Result<Vec<u8>, FaucetError> {
    Err(missing_feature(FileFormat::Avro, "file-format-avro"))
}

#[cfg(feature = "file-format-orc")]
fn decode_orc(bytes: &[u8], opts: &FormatOptions) -> Result<Vec<Value>, FaucetError> {
    orc::decode(bytes, &opts.orc)
}

#[cfg(not(feature = "file-format-orc"))]
fn decode_orc(_: &[u8], _: &FormatOptions) -> Result<Vec<Value>, FaucetError> {
    Err(missing_feature(FileFormat::Orc, "file-format-orc"))
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
mod capped_read_tests {
    use super::*;

    #[tokio::test]
    async fn a_body_past_the_cap_fails_and_one_at_the_cap_reads() {
        let body = vec![b'x'; 64];
        let ok = read_to_end_capped(&body[..], 64, "f").await.unwrap();
        assert_eq!(ok.len(), 64);
        let err = read_to_end_capped(&body[..], 63, "big.json.gz")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("big.json.gz"), "{err}");
        assert!(err.to_string().contains("max_object_bytes"), "{err}");
        assert_eq!(
            read_to_string_capped(&b"hi"[..], 8, "t").await.unwrap(),
            "hi"
        );
        assert!(
            read_to_string_capped(&[0xff, 0xfe][..], 8, "t")
                .await
                .is_err()
        );
        assert!(check_object_size(5, 4, "x").is_err());
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
            (FileFormat::Avro, ".avro", "avro"),
            (FileFormat::Orc, ".orc", "orc"),
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
        for f in [
            FileFormat::JsonArray,
            FileFormat::Xml,
            FileFormat::Xlsx,
            FileFormat::Orc,
        ] {
            assert!(f.requires_whole_object(), "{f:?}");
        }
        for f in [
            FileFormat::JsonLines,
            FileFormat::Csv,
            FileFormat::RawText,
            FileFormat::Parquet,
            FileFormat::Avro,
        ] {
            assert!(!f.requires_whole_object(), "{f:?}");
        }
    }

    #[test]
    fn extensions_resolve_through_compression_suffixes() {
        let cases = [
            ("a.jsonl", Some(FileFormat::JsonLines)),
            ("dir/b.NDJSON", Some(FileFormat::JsonLines)),
            ("c.json.gz", Some(FileFormat::JsonArray)),
            ("d.csv.gz", Some(FileFormat::Csv)),
            ("e.xml.zst", Some(FileFormat::Xml)),
            ("f.xlsx", Some(FileFormat::Xlsx)),
            ("g.parquet", Some(FileFormat::Parquet)),
            ("h.txt", Some(FileFormat::RawText)),
            ("i.avro", Some(FileFormat::Avro)),
            ("j.orc", Some(FileFormat::Orc)),
            ("k.csv.gzip", Some(FileFormat::Csv)),
            ("l.jsonl.zstd", Some(FileFormat::JsonLines)),
            ("m.tsv", None),
            ("noext", None),
            ("n.gz", None),
        ];
        for (path, want) in cases {
            assert_eq!(FileFormat::from_path(path), want, "{path}");
        }
    }

    #[test]
    fn container_and_writable_classification() {
        assert!(FileFormat::Avro.is_container() && FileFormat::Orc.is_container());
        assert!(!FileFormat::Csv.is_container());
        assert!(FileFormat::Avro.is_writable());
        assert!(!FileFormat::Orc.is_writable() && !FileFormat::Parquet.is_writable());
    }

    #[tokio::test]
    async fn orc_is_refused_for_writing() {
        let err = encode(&[], FileFormat::Orc, &FormatOptions::default()).expect_err("orc");
        assert!(err.to_string().contains("read-only"), "{err}");
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
