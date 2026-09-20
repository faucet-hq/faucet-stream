//! JSON-shaped file formats: JSON Lines, a single JSON array, and raw text.
//!
//! These need no extra dependency, so they are always compiled — which also
//! makes them the formats a connector can fall back to when no feature is on.

use crate::error::FaucetError;
use serde_json::Value;

/// The key a [`RawText`](super::FileFormat::RawText) body lands under.
pub const RAW_TEXT_FIELD: &str = "text";

/// One JSON value per line.
///
/// Blank lines are skipped rather than reported: a trailing newline is the
/// normal end of an NDJSON file, and a writer that emits `\r\n` should not fail
/// a reader.
pub fn decode_lines(bytes: &[u8]) -> Result<Vec<Value>, FaucetError> {
    let text = as_utf8(bytes, "json_lines")?;
    let mut out = Vec::new();
    for (i, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let v = serde_json::from_str(line)
            .map_err(|e| FaucetError::Source(format!("json_lines: line {}: {e}", i + 1)))?;
        out.push(v);
    }
    Ok(out)
}

/// A single JSON array (or a single object, which becomes one record).
pub fn decode_array(bytes: &[u8]) -> Result<Vec<Value>, FaucetError> {
    let v: Value = serde_json::from_slice(bytes)
        .map_err(|e| FaucetError::Source(format!("json_array: {e}")))?;
    Ok(match v {
        Value::Array(a) => a,
        other => vec![other],
    })
}

/// The whole body as one record.
pub fn decode_raw_text(bytes: &[u8]) -> Result<Vec<Value>, FaucetError> {
    let text = as_utf8(bytes, "raw_text")?;
    Ok(vec![serde_json::json!({ RAW_TEXT_FIELD: text })])
}

/// One JSON value per line, newline-terminated.
pub fn encode_lines(records: &[Value]) -> Result<Vec<u8>, FaucetError> {
    let mut out = Vec::new();
    for r in records {
        serde_json::to_writer(&mut out, r)
            .map_err(|e| FaucetError::Sink(format!("json_lines: {e}")))?;
        out.push(b'\n');
    }
    Ok(out)
}

/// A single JSON array.
pub fn encode_array(records: &[Value]) -> Result<Vec<u8>, FaucetError> {
    serde_json::to_vec(records).map_err(|e| FaucetError::Sink(format!("json_array: {e}")))
}

/// The inverse of [`decode_raw_text`]: each record's `text` field, one per line.
///
/// A record without a `text` field is written as its JSON form rather than
/// skipped — dropping a record because it does not match the expected shape is
/// the silent-data-loss failure this project treats as the worst class of bug.
pub fn encode_raw_text(records: &[Value]) -> Result<Vec<u8>, FaucetError> {
    let mut out = Vec::new();
    for r in records {
        let line = match r.get(RAW_TEXT_FIELD) {
            Some(Value::String(s)) => s.clone(),
            _ => super::cell_text(r),
        };
        out.extend_from_slice(line.as_bytes());
        out.push(b'\n');
    }
    Ok(out)
}

fn as_utf8<'a>(bytes: &'a [u8], what: &str) -> Result<&'a str, FaucetError> {
    std::str::from_utf8(bytes)
        .map_err(|e| FaucetError::Source(format!("{what}: body is not UTF-8: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn json_lines_round_trips_and_tolerates_blank_lines() {
        let recs = vec![json!({"a": 1}), json!({"a": 2})];
        let bytes = encode_lines(&recs).expect("encode");
        assert_eq!(bytes, b"{\"a\":1}\n{\"a\":2}\n");
        assert_eq!(decode_lines(&bytes).expect("decode"), recs);
        // A trailing newline and a \r\n writer must both read back cleanly.
        assert_eq!(
            decode_lines(b"{\"a\":1}\r\n\n{\"a\":2}\n").expect("decode"),
            recs
        );
    }

    #[test]
    fn a_bad_line_names_its_line_number() {
        let err = decode_lines(b"{\"a\":1}\nnot json\n").expect_err("bad line");
        assert!(err.to_string().contains("line 2"), "{err}");
    }

    #[test]
    fn json_array_accepts_an_array_or_a_bare_object() {
        assert_eq!(
            decode_array(br#"[{"a":1},{"a":2}]"#).expect("array"),
            vec![json!({"a": 1}), json!({"a": 2})]
        );
        assert_eq!(
            decode_array(br#"{"a":1}"#).expect("object"),
            vec![json!({"a": 1})]
        );
        assert_eq!(
            encode_array(&[json!({"a": 1})]).expect("encode"),
            br#"[{"a":1}]"#
        );
    }

    #[test]
    fn raw_text_round_trips_through_the_text_field() {
        let recs = decode_raw_text(b"hello\nworld").expect("decode");
        assert_eq!(recs, vec![json!({"text": "hello\nworld"})]);
        assert_eq!(encode_raw_text(&recs).expect("encode"), b"hello\nworld\n");
    }

    #[test]
    fn raw_text_encode_never_drops_a_record_of_the_wrong_shape() {
        let out = encode_raw_text(&[json!({"other": 1})]).expect("encode");
        assert_eq!(out, b"{\"other\":1}\n");
    }

    #[test]
    fn non_utf8_is_reported_not_replaced() {
        let err = decode_lines(&[0xff, 0xfe]).expect_err("invalid utf-8");
        assert!(err.to_string().contains("not UTF-8"), "{err}");
    }
}
