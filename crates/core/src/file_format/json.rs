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
        check_numbers_exact(line.as_bytes())
            .map_err(|e| FaucetError::Source(format!("json_lines: line {}: {e}", i + 1)))?;
        out.push(v);
    }
    Ok(out)
}

/// A single JSON array (or a single object, which becomes one record).
pub fn decode_array(bytes: &[u8]) -> Result<Vec<Value>, FaucetError> {
    let v: Value = serde_json::from_slice(bytes)
        .map_err(|e| FaucetError::Source(format!("json_array: {e}")))?;
    check_numbers_exact(bytes).map_err(|e| FaucetError::Source(format!("json_array: {e}")))?;
    Ok(match v {
        Value::Array(a) => a,
        other => vec![other],
    })
}

/// Refuse a number literal the parsed value cannot hold exactly (CORE-69).
///
/// Without serde_json's `arbitrary_precision`, an integer beyond `u64` or a
/// decimal with more than ~17 significant digits is stored as the nearest
/// `f64`, silently changing an id or an amount. Each literal is compared, as a
/// decimal value, with what parsing it yields; a literal of at most 15
/// significant digits always survives and is skipped cheaply. Runs only on
/// input that already parsed, so it never has to report a syntax error.
fn check_numbers_exact(bytes: &[u8]) -> Result<(), String> {
    let mut i = 0;
    let mut in_string = false;
    while i < bytes.len() {
        let b = bytes[i];
        if in_string {
            match b {
                b'\\' => i += 1,
                b'"' => in_string = false,
                _ => {}
            }
            i += 1;
            continue;
        }
        if b == b'"' {
            in_string = true;
            i += 1;
            continue;
        }
        if b == b'-' || b.is_ascii_digit() {
            let start = i;
            while i < bytes.len()
                && matches!(bytes[i], b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E')
            {
                i += 1;
            }
            let token = std::str::from_utf8(&bytes[start..i]).unwrap_or_default();
            if token.bytes().filter(u8::is_ascii_digit).count() > 15 {
                check_literal(token)?;
            }
            continue;
        }
        i += 1;
    }
    Ok(())
}

fn check_literal(token: &str) -> Result<(), String> {
    let Ok(parsed) = serde_json::from_str::<serde_json::Number>(token) else {
        return Ok(());
    };
    let back = parsed.to_string();
    if canonical_decimal(token) == canonical_decimal(&back) {
        Ok(())
    } else {
        Err(format!(
            "number {token} cannot be represented exactly (it would be read as {back}); \
             quote it as a string in the source data to keep every digit"
        ))
    }
}

/// `(negative, significant digits, decimal-point position)` with leading and
/// trailing zeros stripped, so `1.50`, `15e-1` and `0.15e1` compare equal.
fn canonical_decimal(s: &str) -> Option<(bool, String, i64)> {
    let (neg, body) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s),
    };
    let (mantissa, exp) = match body.find(['e', 'E']) {
        Some(k) => (&body[..k], body[k + 1..].parse::<i64>().ok()?),
        None => (body, 0),
    };
    let (int, frac) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits: String = format!("{int}{frac}");
    let mut point = (int.len() as i64).checked_add(exp)?;
    let trimmed = digits.trim_start_matches('0');
    point -= (digits.len() - trimmed.len()) as i64;
    let trimmed = trimmed.trim_end_matches('0');
    if trimmed.is_empty() {
        return Some((false, String::new(), 0));
    }
    Some((neg, trimmed.to_string(), point))
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
    fn numbers_that_would_round_are_refused() {
        // Exact: big u64, i64::MIN, short decimals, strings that look like numbers.
        let ok = br#"[{"a": 18446744073709551615, "b": -9223372036854775808, "c": 0.1,
            "d": 1.50, "e": "123456789012345678901234567890", "f": 1.0000000000000000e-5,
            "g": 0.0000000000000000}]"#;
        assert_eq!(decode_array(ok).unwrap().len(), 1);
        let err = decode_array(br#"[{"id": 18446744073709551616}]"#).unwrap_err();
        assert!(err.to_string().contains("18446744073709551616"), "{err}");
        let err = decode_lines(b"{\"x\":\"\\\"\"}\n{\"amt\": 0.12345678901234567890}\n")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("line 2") && err.contains("0.12345678901234567890"),
            "{err}"
        );
    }

    #[test]
    fn canonical_decimal_normalizes_spellings() {
        assert_eq!(canonical_decimal("1.50"), canonical_decimal("15e-1"));
        assert_eq!(canonical_decimal("0.15e1"), canonical_decimal("1.5"));
        assert_eq!(canonical_decimal("-0.0"), canonical_decimal("0"));
        assert_ne!(canonical_decimal("-1"), canonical_decimal("1"));
        assert_eq!(canonical_decimal("1e99999999999999999999"), None);
        assert!(check_literal("1e99999").is_ok());
    }

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
