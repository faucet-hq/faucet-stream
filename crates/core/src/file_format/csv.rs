//! CSV read/write for the file connectors (#604).
//!
//! The reader is the one the REST source has used since #497 — streaming
//! RFC-4180 via `csv-async`, `flexible(true)`, header-derived keys with a
//! `column_<i>` fallback, all-`String` values. Keeping those semantics exactly
//! is the point of moving it here rather than writing a second one: a pipeline
//! that reads a CSV through the REST source and one that reads the same file
//! from S3 must produce the same records.

use super::CsvOptions;
use crate::error::FaucetError;
use serde_json::{Map, Value};

/// Parse CSV bytes into records (lenient about ragged rows).
pub async fn decode(
    bytes: &[u8],
    delimiter: u8,
    has_headers: bool,
) -> Result<Vec<Value>, FaucetError> {
    let opts = CsvOptions {
        delimiter: (delimiter as char).to_string(),
        has_headers,
        ..CsvOptions::default()
    };
    let mut rdr = CsvRowReader::with_bytes(bytes, delimiter, &opts, true)?;
    let mut out = Vec::new();
    while let Some(r) = rdr.next_record().await? {
        out.push(r);
    }
    Ok(out)
}

/// Parse CSV bytes into records with the full dialect in `opts`;
/// `default_flexible` applies when `opts.flexible` is unset.
pub async fn decode_with(
    bytes: &[u8],
    opts: &CsvOptions,
    default_flexible: bool,
) -> Result<Vec<Value>, FaucetError> {
    let mut rdr = CsvRowReader::new(bytes, opts, default_flexible)?;
    let mut out = Vec::new();
    while let Some(r) = rdr.next_record().await? {
        out.push(r);
    }
    Ok(out)
}

/// A streaming CSV reader yielding one JSON object per row.
///
/// Header names must be unique: rows are keyed by header, so a repeated name
/// would silently drop a column. A ragged row fails naming its line unless
/// the dialect is flexible.
pub struct CsvRowReader<R> {
    inner: csv_async::AsyncReader<R>,
    record: csv_async::StringRecord,
    headers: Option<Vec<String>>,
    has_headers: bool,
    flexible: bool,
    null_values: Vec<String>,
    records: usize,
}

impl<R: tokio::io::AsyncRead + Unpin + Send> CsvRowReader<R> {
    /// Build a reader over `reader` with the dialect in `opts`.
    pub fn new(reader: R, opts: &CsvOptions, default_flexible: bool) -> Result<Self, FaucetError> {
        Self::with_bytes(reader, opts.delimiter_byte()?, opts, default_flexible)
    }

    fn with_bytes(
        reader: R,
        delimiter: u8,
        opts: &CsvOptions,
        default_flexible: bool,
    ) -> Result<Self, FaucetError> {
        let flexible = opts.flexible_or(default_flexible);
        let inner = csv_async::AsyncReaderBuilder::new()
            .has_headers(false)
            .delimiter(delimiter)
            .quote(opts.quote_byte()?)
            .flexible(flexible)
            .create_reader(reader);
        Ok(Self {
            inner,
            record: csv_async::StringRecord::new(),
            headers: None,
            has_headers: opts.has_headers,
            flexible,
            null_values: opts.null_values.clone(),
            records: 0,
        })
    }

    /// The next row, or `None` at the end of the input.
    pub async fn next_record(&mut self) -> Result<Option<Value>, FaucetError> {
        loop {
            self.records += 1;
            let more = self
                .inner
                .read_record(&mut self.record)
                .await
                .map_err(|e| self.parse_error(e))?;
            if !more {
                return Ok(None);
            }
            if self.has_headers && self.headers.is_none() {
                self.headers = Some(unique_headers(&self.record)?);
                continue;
            }
            return Ok(Some(Value::Object(record_to_object(
                &self.record,
                self.headers.as_deref(),
                &self.null_values,
            ))));
        }
    }

    /// Where a failed read was: the physical line when the parser knows it
    /// (a quoted field can span lines, so records are not lines), else the
    /// record number.
    fn location(&self, e: &csv_async::Error) -> String {
        match e.position() {
            Some(p) => format!("line {}", p.line()),
            None => format!("record {}", self.records),
        }
    }

    fn parse_error(&self, e: csv_async::Error) -> FaucetError {
        let at = self.location(&e);
        if !self.flexible && matches!(e.kind(), csv_async::ErrorKind::UnequalLengths { .. }) {
            FaucetError::Source(format!(
                "csv: ragged row at {at}: {e} — a short or long row is a structural defect \
                 that would silently misalign fields; fix the file or set `csv.flexible: true` \
                 to accept uneven rows"
            ))
        } else {
            FaucetError::Source(format!("csv: parse error at {at}: {e}"))
        }
    }
}

fn unique_headers(rec: &csv_async::StringRecord) -> Result<Vec<String>, FaucetError> {
    let headers: Vec<String> = rec.iter().map(str::to_string).collect();
    let mut seen = std::collections::HashMap::with_capacity(headers.len());
    for (i, name) in headers.iter().enumerate() {
        if let Some(first) = seen.insert(name.as_str(), i) {
            let shown = if name.is_empty() { "(empty)" } else { name };
            return Err(FaucetError::Source(format!(
                "csv: duplicate header {shown} at columns {first} and {i}; rows are keyed by \
                 header name, so a repeated header would silently drop a column — rename it \
                 or set `csv.has_headers: false`"
            )));
        }
    }
    Ok(headers)
}

/// How one CSV record becomes a JSON object — the single definition, so the
/// `Value` path and any byte path can never drift apart.
fn record_to_object(
    rec: &csv_async::StringRecord,
    headers: Option<&[String]>,
    null_values: &[String],
) -> Map<String, Value> {
    let mut obj = Map::new();
    for (i, field) in rec.iter().enumerate() {
        let key = headers
            .and_then(|h| h.get(i).cloned())
            .unwrap_or_else(|| format!("column_{i}"));
        let value = if null_values.iter().any(|n| n == field) {
            Value::Null
        } else {
            Value::String(field.to_string())
        };
        obj.insert(key, value);
    }
    obj
}

/// Write records as CSV.
///
/// Columns are the first-seen union of every record's keys
/// ([`header_union`](super::header_union)), so a record that gains a field
/// mid-page widens the file instead of losing the field. A record missing a
/// column writes an empty cell.
pub fn encode(records: &[Value], delimiter: u8, has_headers: bool) -> Result<Vec<u8>, FaucetError> {
    encode_rows(records, delimiter, b'"', has_headers)
}

/// [`encode`] with the full dialect in `opts`.
pub fn encode_with(records: &[Value], opts: &CsvOptions) -> Result<Vec<u8>, FaucetError> {
    encode_rows(
        records,
        opts.delimiter_byte()?,
        opts.quote_byte()?,
        opts.has_headers,
    )
}

fn encode_rows(
    records: &[Value],
    delimiter: u8,
    quote: u8,
    has_headers: bool,
) -> Result<Vec<u8>, FaucetError> {
    let headers = super::header_union(records);
    let mut wtr = csv::WriterBuilder::new()
        .delimiter(delimiter)
        .quote(quote)
        .from_writer(Vec::new());
    if has_headers && !headers.is_empty() {
        wtr.write_record(&headers)
            .map_err(|e| FaucetError::Sink(format!("csv: writing header: {e}")))?;
    }
    for r in records {
        let row: Vec<String> = headers
            .iter()
            .map(|h| r.get(h).map(super::cell_text).unwrap_or_default())
            .collect();
        wtr.write_record(&row)
            .map_err(|e| FaucetError::Sink(format!("csv: writing row: {e}")))?;
    }
    wtr.into_inner()
        .map_err(|e| FaucetError::Sink(format!("csv: finishing: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn headers_name_the_fields() {
        let recs = decode(b"a,b\n1,2\n3,4\n", b',', true)
            .await
            .expect("decode");
        assert_eq!(
            recs,
            vec![json!({"a": "1", "b": "2"}), json!({"a": "3", "b": "4"})]
        );
    }

    #[tokio::test]
    async fn without_headers_fields_fall_back_to_column_index() {
        let recs = decode(b"1,2\n", b',', false).await.expect("decode");
        assert_eq!(recs, vec![json!({"column_0": "1", "column_1": "2"})]);
    }

    #[tokio::test]
    async fn a_quoted_embedded_newline_stays_one_record() {
        let recs = decode(b"a\n\"x\ny\"\n", b',', true).await.expect("decode");
        assert_eq!(recs, vec![json!({"a": "x\ny"})]);
    }

    #[tokio::test]
    async fn a_ragged_row_is_kept_not_rejected() {
        // `flexible(true)`: a short row yields the fields it has rather than
        // failing the whole object, which is what the REST source does today.
        let recs = decode(b"a,b\n1\n", b',', true).await.expect("decode");
        assert_eq!(recs, vec![json!({"a": "1"})]);
    }

    #[tokio::test]
    async fn header_only_and_empty_bodies_yield_nothing() {
        assert!(
            decode(b"a,b\n", b',', true)
                .await
                .expect("header")
                .is_empty()
        );
        assert!(decode(b"", b',', true).await.expect("empty").is_empty());
    }

    #[test]
    fn encode_uses_the_union_of_keys_and_blanks_the_missing_ones() {
        let out = encode(
            &[json!({"a": 1, "b": 2}), json!({"a": 3, "c": 4})],
            b',',
            true,
        )
        .expect("encode");
        assert_eq!(String::from_utf8(out).unwrap(), "a,b,c\n1,2,\n3,,4\n");
    }

    #[test]
    fn encode_can_omit_the_header() {
        let out = encode(&[json!({"a": 1})], b',', false).expect("encode");
        assert_eq!(String::from_utf8(out).unwrap(), "1\n");
    }

    #[tokio::test]
    async fn a_tab_delimiter_round_trips() {
        let out = encode(&[json!({"a": "x", "b": "y"})], b'\t', true).expect("encode");
        assert_eq!(String::from_utf8(out.clone()).unwrap(), "a\tb\nx\ty\n");
        let back = decode(&out, b'\t', true).await.expect("decode");
        assert_eq!(back, vec![json!({"a": "x", "b": "y"})]);
    }

    #[test]
    fn nested_structure_survives_as_json_rather_than_being_dropped() {
        let out = encode(&[json!({"a": {"k": 1}})], b',', true).expect("encode");
        assert_eq!(String::from_utf8(out).unwrap(), "a\n\"{\"\"k\"\":1}\"\n");
    }
    fn opts(f: impl FnOnce(&mut CsvOptions)) -> CsvOptions {
        let mut o = CsvOptions::default();
        f(&mut o);
        o
    }

    #[tokio::test]
    async fn a_custom_quote_character_is_honoured_on_read_and_write() {
        let o = opts(|o| o.quote = "'".into());
        let recs = decode_with(b"a,b\n'x,y',2\n", &o, false)
            .await
            .expect("decode");
        assert_eq!(recs, vec![json!({"a": "x,y", "b": "2"})]);
        let out = encode_with(&recs, &o).expect("encode");
        assert_eq!(String::from_utf8(out).unwrap(), "a,b\n'x,y',2\n");
    }

    #[tokio::test]
    async fn a_strict_dialect_rejects_a_ragged_row_naming_its_line() {
        let o = opts(|o| o.flexible = Some(false));
        let err = decode_with(b"a,b\n1,2\n3\n", &o, true).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("ragged row at line 3"), "{msg}");
        let multi = decode_with(b"a,b\n\"x\ny\",2\n3\n", &o, true)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            multi.contains("ragged row at line 4"),
            "the physical line: {multi}"
        );
        assert!(msg.contains("csv.flexible"), "{msg}");
        let lenient = decode_with(b"a,b\n1,2\n3\n", &CsvOptions::default(), false)
            .await
            .unwrap_err();
        assert!(lenient.to_string().contains("ragged"));
        let ok = decode_with(b"a,b\n3\n", &CsvOptions::default(), true)
            .await
            .expect("default flexible");
        assert_eq!(ok, vec![json!({"a": "3"})]);
    }

    #[tokio::test]
    async fn null_values_read_as_null() {
        let o = opts(|o| o.null_values = vec!["".into(), "NULL".into()]);
        let recs = decode_with(b"a,b,c\n,NULL,x\n", &o, false)
            .await
            .expect("decode");
        assert_eq!(recs, vec![json!({"a": null, "b": null, "c": "x"})]);
    }

    #[tokio::test]
    async fn a_duplicate_header_is_refused_rather_than_dropping_a_column() {
        let err = decode(b"a,a\n1,2\n", b',', true).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("duplicate header a at columns 0 and 1"),
            "{msg}"
        );
        let err = decode(b",\n1,2\n", b',', true).await.unwrap_err();
        assert!(err.to_string().contains("(empty)"));
        let ok = decode(b"a,a\n1,2\n", b',', false)
            .await
            .expect("no headers");
        assert_eq!(
            ok,
            vec![
                json!({"column_0": "a", "column_1": "a"}),
                json!({"column_0": "1", "column_1": "2"})
            ]
        );
    }

    #[tokio::test]
    async fn a_malformed_quote_is_a_typed_parse_error() {
        let o = opts(|o| o.flexible = Some(false));
        let bad: &[u8] = b"a\n\xff\xfe\n";
        let err = decode_with(bad, &o, false).await.unwrap_err();
        assert!(err.to_string().contains("parse error at line"), "{err}");
    }

    #[test]
    fn dialect_bytes_are_validated() {
        assert!(opts(|o| o.quote = "''".into()).validate().is_err());
        assert!(opts(|o| o.delimiter = ";;".into()).validate().is_err());
        assert!(opts(|o| o.delimiter = "\\t".into()).validate().is_ok());
        assert!(encode_with(&[json!({"a": 1})], &opts(|o| o.quote = "".into())).is_err());
    }

    #[test]
    fn has_headers_has_one_name() {
        let o: CsvOptions = serde_json::from_value(json!({"has_headers": false})).unwrap();
        assert!(!o.has_headers);
        assert_eq!(o.on_unknown_field, super::super::CsvUnknownField::Widen);
        assert!(serde_json::from_value::<CsvOptions>(json!({"write_headers": false})).is_err());
    }
}
