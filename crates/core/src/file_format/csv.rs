//! CSV read/write for the file connectors (#604).
//!
//! The reader is the one the REST source has used since #497 — streaming
//! RFC-4180 via `csv-async`, `flexible(true)`, header-derived keys with a
//! `column_<i>` fallback, all-`String` values. Keeping those semantics exactly
//! is the point of moving it here rather than writing a second one: a pipeline
//! that reads a CSV through the REST source and one that reads the same file
//! from S3 must produce the same records.

use crate::error::FaucetError;
use serde_json::{Map, Value};

/// Parse CSV bytes into records.
pub async fn decode(
    bytes: &[u8],
    delimiter: u8,
    has_headers: bool,
) -> Result<Vec<Value>, FaucetError> {
    use futures::StreamExt as _;
    let mut rdr = csv_async::AsyncReaderBuilder::new()
        .has_headers(false)
        .delimiter(delimiter)
        .flexible(true)
        .create_reader(bytes);
    let mut records = rdr.records();
    let mut headers: Option<Vec<String>> = None;
    let mut out = Vec::new();
    while let Some(rec) = records.next().await {
        let rec = rec.map_err(|e| FaucetError::Source(format!("csv: parse error: {e}")))?;
        if has_headers && headers.is_none() {
            headers = Some(rec.iter().map(str::to_string).collect());
            continue;
        }
        out.push(Value::Object(record_to_object(&rec, headers.as_deref())));
    }
    Ok(out)
}

/// How one CSV record becomes a JSON object — the single definition, so the
/// `Value` path and any byte path can never drift apart.
fn record_to_object(
    rec: &csv_async::StringRecord,
    headers: Option<&[String]>,
) -> Map<String, Value> {
    let mut obj = Map::new();
    for (i, field) in rec.iter().enumerate() {
        let key = headers
            .and_then(|h| h.get(i).cloned())
            .unwrap_or_else(|| format!("column_{i}"));
        obj.insert(key, Value::String(field.to_string()));
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
    let headers = super::header_union(records);
    let mut wtr = csv::WriterBuilder::new()
        .delimiter(delimiter)
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
}
