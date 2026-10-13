//! Exact handling of numbers inside JSON text read from a database column.
//!
//! A `serde_json::Value` holds every non-integer (and every integer beyond
//! `u64`/`i64`) as an `f64`, so a JSON document with a number of more than
//! about 17 significant digits would silently lose digits on the way through.
//! [`parse_json_exact`] checks every number token of the *source text* and
//! either refuses the document or keeps such a number as an exact string,
//! per [`JsonBigNumbers`].

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// What to do with a number inside a JSON / JSONB column that a 64-bit float
/// cannot hold exactly.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum JsonBigNumbers {
    /// Fail the read with an error naming the column (default): no silent
    /// rounding.
    #[default]
    Fail,
    /// Emit that number as a JSON string holding its exact digits.
    String,
}

/// An inexact number found in a JSON document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InexactNumber {
    /// The number's text as it appears in the document.
    pub text: String,
}

impl InexactNumber {
    /// The leading digits, for an error message.
    pub fn preview(&self) -> String {
        let mut p: String = self.text.chars().take(24).collect();
        if self.text.chars().count() > 24 {
            p.push('…');
        }
        p
    }
}

/// `(negative, significant digits, exponent)` with `value = digits × 10^exp`,
/// digits stripped of leading and trailing zeros (`("", 0)` for zero).
fn normalize(text: &str) -> Option<(bool, String, i64)> {
    let (neg, rest) = match text.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, text),
    };
    let (mantissa, exp) = match rest.find(['e', 'E']) {
        Some(i) => (&rest[..i], rest[i + 1..].parse::<i64>().ok()?),
        None => (rest, 0),
    };
    let (int, frac) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if int.is_empty() || !int.bytes().chain(frac.bytes()).all(|b| b.is_ascii_digit()) {
        return None;
    }
    let mut digits = format!("{int}{frac}");
    let mut exp = exp.checked_sub(frac.len() as i64)?;
    let trimmed = digits.trim_start_matches('0').len();
    digits.drain(..digits.len() - trimmed);
    while digits.ends_with('0') {
        digits.pop();
        exp += 1;
    }
    if digits.is_empty() {
        return Some((false, String::new(), 0));
    }
    Some((neg, digits, exp))
}

/// Whether the number `text` survives conversion to a JSON `Value` exactly:
/// an `i64` / `u64`, or an `f64` whose shortest form has the same value.
///
/// The value is the one `serde_json` itself produces, not `str::parse`'s:
/// without its `float_roundtrip` feature serde_json rounds some 16–17 digit
/// numbers one ULP away from the correctly rounded float (#862 M-008).
pub fn is_exact(text: &str) -> bool {
    if short_plain_decimal(text) {
        return true;
    }
    if text.parse::<i64>().is_ok() || text.parse::<u64>().is_ok() {
        return true;
    }
    let Ok(Value::Number(n)) = serde_json::from_str::<Value>(text) else {
        return false;
    };
    let exact = normalize(text);
    exact.is_some() && exact == normalize(&n.to_string())
}

/// `-?d+(.d+)?` with at most 15 digits: every such decimal round-trips through
/// an `f64`, so the common case skips the parse.
fn short_plain_decimal(text: &str) -> bool {
    let body = text.strip_prefix('-').unwrap_or(text);
    let (int, frac) = body.split_once('.').unwrap_or((body, "1"));
    !int.is_empty()
        && !frac.is_empty()
        && int.len() + frac.len() <= 15
        && int.bytes().chain(frac.bytes()).all(|b| b.is_ascii_digit())
}

/// Byte ranges of the number tokens in JSON `text` (outside strings).
fn number_tokens(text: &str) -> Vec<std::ops::Range<usize>> {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => {
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' {
                    i += if bytes[i] == b'\\' { 2 } else { 1 };
                }
                i += 1;
            }
            b'-' | b'0'..=b'9' => {
                let start = i;
                while i < bytes.len()
                    && matches!(bytes[i], b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9')
                {
                    i += 1;
                }
                out.push(start..i);
            }
            _ => i += 1,
        }
    }
    out
}

/// Parse JSON `text`, applying `mode` to every number a `Value` cannot hold
/// exactly. Returns the value and the inexact numbers found; under
/// [`JsonBigNumbers::Fail`] a document with any is an `Err` carrying them.
pub fn parse_json_exact(
    text: &str,
    mode: JsonBigNumbers,
) -> Result<(Value, Vec<InexactNumber>), JsonNumberError> {
    let inexact: Vec<_> = number_tokens(text)
        .into_iter()
        .filter(|r| !is_exact(&text[r.clone()]))
        .collect();
    if inexact.is_empty() {
        return serde_json::from_str(text)
            .map(|v| (v, Vec::new()))
            .map_err(|e| JsonNumberError::Invalid(e.to_string()));
    }
    let found: Vec<InexactNumber> = inexact
        .iter()
        .map(|r| InexactNumber {
            text: text[r.clone()].to_string(),
        })
        .collect();
    if mode == JsonBigNumbers::Fail {
        return Err(JsonNumberError::Inexact(found));
    }
    let mut rewritten = String::with_capacity(text.len() + 2 * inexact.len());
    let mut last = 0;
    for r in &inexact {
        rewritten.push_str(&text[last..r.start]);
        rewritten.push('"');
        rewritten.push_str(&text[r.clone()]);
        rewritten.push('"');
        last = r.end;
    }
    rewritten.push_str(&text[last..]);
    serde_json::from_str(&rewritten)
        .map(|v| (v, found))
        .map_err(|e| JsonNumberError::Invalid(e.to_string()))
}

/// Why [`parse_json_exact`] refused a document.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum JsonNumberError {
    /// Numbers a `Value` cannot hold exactly, under [`JsonBigNumbers::Fail`].
    #[error(
        "{} number(s) cannot be held exactly, first {}",
        .0.len(),
        .0.first().map(InexactNumber::preview).unwrap_or_default()
    )]
    Inexact(Vec<InexactNumber>),
    /// The text is not valid JSON.
    #[error("invalid JSON: {0}")]
    Invalid(String),
}

impl std::fmt::Display for InexactNumber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} cannot be held exactly", self.preview())
    }
}

impl std::error::Error for InexactNumber {}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn errors_render_a_message() {
        let n = InexactNumber {
            text: "123456789012345678901234567890".into(),
        };
        assert_eq!(
            n.to_string(),
            "123456789012345678901234… cannot be held exactly"
        );
        let e = JsonNumberError::Inexact(vec![n.clone(), n]);
        assert!(
            e.to_string()
                .starts_with("2 number(s) cannot be held exactly, first 1234"),
            "{e}"
        );
        assert_eq!(
            JsonNumberError::Invalid("eof".into()).to_string(),
            "invalid JSON: eof"
        );
        let _: &dyn std::error::Error = &e;
    }

    #[test]
    fn exactness_follows_the_float_serde_json_produces() {
        for text in [
            "1.0715660391465826e-75",
            "-1.81996730402717e-179",
            "-1.603964615428183e143",
            "-9.643915712060552e-234",
            "1.5860846119992697e-265",
            "2.2250738585072011e-308",
        ] {
            let parsed = parse_json_exact(text, JsonBigNumbers::String).unwrap().0;
            match &parsed {
                Value::Number(n) => {
                    assert!(is_exact(text), "{text}");
                    assert_eq!(normalize(&n.to_string()), normalize(text), "{text}");
                }
                Value::String(s) => {
                    assert!(!is_exact(text), "{text}");
                    assert_eq!(s, text);
                }
                other => panic!("{text} parsed to {other}"),
            }
        }
    }

    #[test]
    fn exactness_compares_decimal_values() {
        for exact in [
            "0",
            "-0",
            "1",
            "1.50",
            "0.1",
            "1e2",
            "1E-2",
            "9007199254740993",
            "18446744073709551615",
            "-9223372036854775808",
            "123.456",
            "1e300",
        ] {
            assert!(is_exact(exact), "{exact}");
        }
        for inexact in [
            "12345678901234567890.123",
            "18446744073709551616",
            "0.12345678901234567891",
            "1e400",
            "-",
        ] {
            assert!(!is_exact(inexact), "{inexact}");
        }
        assert!(short_plain_decimal("-123456789.12345"));
        assert!(!short_plain_decimal("1234567890.123456"));
        assert!(!short_plain_decimal("1."));
        assert!(!short_plain_decimal("-"));
        assert!(!short_plain_decimal("1e5"));
        assert_eq!(normalize("1.2e"), None);
        assert_eq!(normalize("."), None);
        assert_eq!(normalize("0.000"), Some((false, String::new(), 0)));
        assert_eq!(normalize("-1.50"), Some((true, "15".to_string(), -1)));
    }

    #[test]
    fn documents_fail_or_keep_big_numbers_as_strings() {
        let doc = r#"{"a": 12345678901234567890.123, "s": "9.99999999999999999999 in a string", "b": [1, 2.5], "e": "\"q\\" }"#;
        let failed = parse_json_exact(doc, JsonBigNumbers::Fail);
        assert!(matches!(
            &failed,
            Err(JsonNumberError::Inexact(found))
                if found.len() == 1 && found[0].preview() == "12345678901234567890.123"
        ));
        let (v, found) = parse_json_exact(doc, JsonBigNumbers::String).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(v["a"], json!("12345678901234567890.123"));
        assert_eq!(v["b"], json!([1, 2.5]));
        let (v, found) = parse_json_exact("[1.5]", JsonBigNumbers::Fail).unwrap();
        assert!(found.is_empty());
        assert_eq!(v, json!([1.5]));
        assert!(matches!(
            parse_json_exact("{", JsonBigNumbers::Fail),
            Err(JsonNumberError::Invalid(_))
        ));
        assert!(matches!(
            parse_json_exact("[1e400,", JsonBigNumbers::String),
            Err(JsonNumberError::Invalid(_))
        ));
        let long = InexactNumber {
            text: "1".repeat(30),
        };
        assert!(long.preview().ends_with('…'));
    }
}
