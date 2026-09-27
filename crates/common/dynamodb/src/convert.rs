//! Lossless DynamoDB `AttributeValue` ⇄ plain JSON conversion.
//!
//! Reading (`AttributeValue` → JSON):
//!
//! | DynamoDB | JSON |
//! |----------|------|
//! | `S` | string |
//! | `N` | number when the decimal value is exactly representable (an `i64`/`u64`, or an `f64` whose shortest decimal form equals the stored value); otherwise the original decimal **string** — precision is never lost |
//! | `BOOL` | boolean |
//! | `NULL` | `null` |
//! | `B` | base64 string (standard alphabet, padded) |
//! | `M` / `L` | object / array (recursive) |
//! | `SS` / `NS` / `BS` | array of strings / numbers (same `N` rule) / base64 strings |
//!
//! Writing (JSON → `AttributeValue`): string → `S`, number → `N`, boolean →
//! `BOOL`, `null` → `NULL`, array → `L`, object → `M`. JSON has no set or
//! binary type, so sets and binaries read from DynamoDB come back as lists
//! and strings respectively.

use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::AttributeValue;
use base64::Engine as _;
use faucet_core::FaucetError;
use serde_json::{Map, Number, Value};
use std::collections::HashMap;

type StreamsAttr = aws_sdk_dynamodbstreams::types::AttributeValue;

/// A DynamoDB decimal in canonical form: `digits × 10^exp` with no leading or
/// trailing zeros in `digits` (zero is the empty digit string).
#[derive(Debug, PartialEq, Eq)]
struct Decimal {
    negative: bool,
    digits: String,
    exp: i64,
}

fn canonical_decimal(raw: &str) -> Option<Decimal> {
    let s = raw.trim();
    let (negative, s) = match s.as_bytes().first()? {
        b'-' => (true, &s[1..]),
        b'+' => (false, &s[1..]),
        _ => (false, s),
    };
    let (mantissa, exp_part) = match s.find(['e', 'E']) {
        Some(i) => (&s[..i], Some(&s[i + 1..])),
        None => (s, None),
    };
    let (int_part, frac_part) = match mantissa.find('.') {
        Some(i) => (&mantissa[..i], &mantissa[i + 1..]),
        None => (mantissa, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let mut exp: i64 = match exp_part {
        Some(e) => e.parse().ok()?,
        None => 0,
    };
    exp = exp.checked_sub(frac_part.len() as i64)?;
    let joined = format!("{int_part}{frac_part}");
    let trimmed_leading = joined.trim_start_matches('0');
    let digits = trimmed_leading.trim_end_matches('0');
    exp = exp.checked_add((trimmed_leading.len() - digits.len()) as i64)?;
    if digits.is_empty() {
        return Some(Decimal {
            negative: false,
            digits: String::new(),
            exp: 0,
        });
    }
    Some(Decimal {
        negative,
        digits: digits.to_string(),
        exp,
    })
}

/// Convert a DynamoDB `N` decimal string to JSON: a JSON number when the value
/// is exactly representable, otherwise the original string (never lossy).
pub fn number_to_json(raw: &str) -> Value {
    let Some(dec) = canonical_decimal(raw) else {
        return Value::String(raw.to_string());
    };
    if dec.digits.is_empty() {
        return Value::Number(0.into());
    }
    if dec.exp >= 0 && dec.digits.len() as i64 + dec.exp <= 20 {
        let integer = format!("{}{}", dec.digits, "0".repeat(dec.exp as usize));
        if dec.negative {
            if let Ok(i) = format!("-{integer}").parse::<i64>() {
                return Value::Number(i.into());
            }
        } else if let Ok(u) = integer.parse::<u64>() {
            return Value::Number(u.into());
        }
    }
    if let Ok(f) = raw.trim().parse::<f64>()
        && f.is_finite()
        && canonical_decimal(&format!("{f}")).as_ref() == Some(&dec)
        && let Some(n) = Number::from_f64(f)
    {
        return Value::Number(n);
    }
    Value::String(raw.to_string())
}

/// Convert one DynamoDB attribute value to plain JSON (see the module docs).
/// An attribute type this SDK version does not know is an error rather than
/// a silent `null`.
pub fn attribute_to_json(av: &AttributeValue) -> Result<Value, FaucetError> {
    Ok(match av {
        AttributeValue::S(s) => Value::String(s.clone()),
        AttributeValue::N(n) => number_to_json(n),
        AttributeValue::Bool(b) => Value::Bool(*b),
        AttributeValue::Null(_) => Value::Null,
        AttributeValue::B(b) => b64(b),
        AttributeValue::Ss(v) => Value::Array(v.iter().cloned().map(Value::String).collect()),
        AttributeValue::Ns(v) => Value::Array(v.iter().map(|n| number_to_json(n)).collect()),
        AttributeValue::Bs(v) => Value::Array(v.iter().map(b64).collect()),
        AttributeValue::L(v) => Value::Array(
            v.iter()
                .map(attribute_to_json)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        AttributeValue::M(m) => item_to_json(m)?,
        other => {
            return Err(FaucetError::Source(format!(
                "dynamodb: unsupported attribute value type {other:?}"
            )));
        }
    })
}

/// Convert a DynamoDB item (attribute map) to a JSON object.
pub fn item_to_json(item: &HashMap<String, AttributeValue>) -> Result<Value, FaucetError> {
    let mut obj = Map::with_capacity(item.len());
    for (k, v) in item {
        obj.insert(k.clone(), attribute_to_json(v)?);
    }
    Ok(Value::Object(obj))
}

/// Render a JSON number as a DynamoDB `N` string.
fn json_number_to_n(n: &Number) -> String {
    if let Some(i) = n.as_i64() {
        i.to_string()
    } else if let Some(u) = n.as_u64() {
        u.to_string()
    } else {
        format!("{}", n.as_f64().unwrap_or_default())
    }
}

/// Convert a JSON value to a DynamoDB attribute value.
pub fn json_to_attribute(v: &Value) -> AttributeValue {
    match v {
        Value::Null => AttributeValue::Null(true),
        Value::Bool(b) => AttributeValue::Bool(*b),
        Value::Number(n) => AttributeValue::N(json_number_to_n(n)),
        Value::String(s) => AttributeValue::S(s.clone()),
        Value::Array(a) => AttributeValue::L(a.iter().map(json_to_attribute).collect()),
        Value::Object(o) => AttributeValue::M(
            o.iter()
                .map(|(k, v)| (k.clone(), json_to_attribute(v)))
                .collect(),
        ),
    }
}

/// Convert a JSON object record to a DynamoDB item. Non-objects are rejected.
pub fn json_to_item(v: &Value) -> Result<HashMap<String, AttributeValue>, FaucetError> {
    match v {
        Value::Object(o) => Ok(o
            .iter()
            .map(|(k, v)| (k.clone(), json_to_attribute(v)))
            .collect()),
        other => Err(FaucetError::Sink(format!(
            "dynamodb: record must be a JSON object, got {}",
            json_kind(other)
        ))),
    }
}

fn json_kind(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Re-type a DynamoDB Streams attribute value as a DynamoDB one (the two SDKs
/// define structurally identical but distinct enums).
pub fn streams_attribute_to_dynamodb(av: &StreamsAttr) -> Result<AttributeValue, FaucetError> {
    Ok(match av {
        StreamsAttr::S(s) => AttributeValue::S(s.clone()),
        StreamsAttr::N(n) => AttributeValue::N(n.clone()),
        StreamsAttr::Bool(b) => AttributeValue::Bool(*b),
        StreamsAttr::Null(b) => AttributeValue::Null(*b),
        StreamsAttr::B(b) => AttributeValue::B(Blob::new(b.as_ref().to_vec())),
        StreamsAttr::Ss(v) => AttributeValue::Ss(v.clone()),
        StreamsAttr::Ns(v) => AttributeValue::Ns(v.clone()),
        StreamsAttr::Bs(v) => {
            AttributeValue::Bs(v.iter().map(|b| Blob::new(b.as_ref().to_vec())).collect())
        }
        StreamsAttr::L(v) => AttributeValue::L(
            v.iter()
                .map(streams_attribute_to_dynamodb)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        StreamsAttr::M(m) => AttributeValue::M(
            m.iter()
                .map(|(k, v)| Ok((k.clone(), streams_attribute_to_dynamodb(v)?)))
                .collect::<Result<HashMap<_, _>, FaucetError>>()?,
        ),
        other => {
            return Err(FaucetError::Source(format!(
                "dynamodb streams: unsupported attribute value type {other:?}"
            )));
        }
    })
}

/// Convert a DynamoDB Streams image (attribute map) to a JSON object.
pub fn streams_item_to_json(item: &HashMap<String, StreamsAttr>) -> Result<Value, FaucetError> {
    let mut obj = Map::with_capacity(item.len());
    for (k, v) in item {
        obj.insert(
            k.clone(),
            attribute_to_json(&streams_attribute_to_dynamodb(v)?)?,
        );
    }
    Ok(Value::Object(obj))
}

fn b64(b: &Blob) -> Value {
    Value::String(base64::engine::general_purpose::STANDARD.encode(b.as_ref()))
}

fn unb64(v: &Value) -> Result<Blob, FaucetError> {
    let s = v
        .as_str()
        .ok_or_else(|| FaucetError::Source("dynamodb: typed B value must be a string".into()))?;
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .map(Blob::new)
        .map_err(|e| FaucetError::Source(format!("dynamodb: invalid base64 in typed B value: {e}")))
}

fn attribute_to_typed(av: &AttributeValue) -> Value {
    use serde_json::json;
    match av {
        AttributeValue::S(s) => json!({ "S": s }),
        AttributeValue::N(n) => json!({ "N": n }),
        AttributeValue::B(b) => json!({ "B": b64(b) }),
        AttributeValue::Bool(b) => json!({ "BOOL": b }),
        AttributeValue::Null(b) => json!({ "NULL": b }),
        AttributeValue::Ss(v) => json!({ "SS": v }),
        AttributeValue::Ns(v) => json!({ "NS": v }),
        AttributeValue::Bs(v) => json!({ "BS": v.iter().map(b64).collect::<Vec<_>>() }),
        AttributeValue::L(v) => {
            json!({ "L": v.iter().map(attribute_to_typed).collect::<Vec<_>>() })
        }
        AttributeValue::M(m) => json!({ "M": item_to_typed_json(m) }),
        _ => Value::Null,
    }
}

/// Serialize an item in DynamoDB's typed JSON form (`{"pk": {"S": "a"}}`) —
/// lossless, used to persist `LastEvaluatedKey` cursors in bookmarks.
pub fn item_to_typed_json(item: &HashMap<String, AttributeValue>) -> Value {
    Value::Object(
        item.iter()
            .map(|(k, v)| (k.clone(), attribute_to_typed(v)))
            .collect(),
    )
}

fn strings(v: &Value) -> Result<Vec<String>, FaucetError> {
    v.as_array()
        .and_then(|a| a.iter().map(|s| s.as_str().map(str::to_string)).collect())
        .ok_or_else(|| {
            FaucetError::Source("dynamodb: typed set must be an array of strings".into())
        })
}

fn typed_to_attribute(v: &Value) -> Result<AttributeValue, FaucetError> {
    let bad = || FaucetError::Source(format!("dynamodb: malformed typed attribute value {v}"));
    let obj = v.as_object().filter(|o| o.len() == 1).ok_or_else(bad)?;
    let (tag, inner) = obj.iter().next().ok_or_else(bad)?;
    Ok(match tag.as_str() {
        "S" => AttributeValue::S(inner.as_str().ok_or_else(bad)?.to_string()),
        "N" => AttributeValue::N(inner.as_str().ok_or_else(bad)?.to_string()),
        "B" => AttributeValue::B(unb64(inner)?),
        "BOOL" => AttributeValue::Bool(inner.as_bool().ok_or_else(bad)?),
        "NULL" => AttributeValue::Null(inner.as_bool().ok_or_else(bad)?),
        "SS" => AttributeValue::Ss(strings(inner)?),
        "NS" => AttributeValue::Ns(strings(inner)?),
        "BS" => AttributeValue::Bs(
            inner
                .as_array()
                .ok_or_else(bad)?
                .iter()
                .map(unb64)
                .collect::<Result<_, _>>()?,
        ),
        "L" => AttributeValue::L(
            inner
                .as_array()
                .ok_or_else(bad)?
                .iter()
                .map(typed_to_attribute)
                .collect::<Result<_, _>>()?,
        ),
        "M" => AttributeValue::M(typed_json_to_item(inner)?),
        _ => return Err(bad()),
    })
}

/// Parse DynamoDB typed JSON back into an item (inverse of
/// [`item_to_typed_json`]).
pub fn typed_json_to_item(v: &Value) -> Result<HashMap<String, AttributeValue>, FaucetError> {
    let obj = v.as_object().ok_or_else(|| {
        FaucetError::Source(format!("dynamodb: typed item must be an object, got {v}"))
    })?;
    obj.iter()
        .map(|(k, v)| Ok((k.clone(), typed_to_attribute(v)?)))
        .collect()
}

/// DynamoDB's size of a number attribute: roughly one byte per two
/// significant digits plus one (capped at 21 bytes).
fn number_size(n: &str) -> usize {
    let digits = n.bytes().filter(u8::is_ascii_digit).count();
    (digits.div_ceil(2) + 1).min(21)
}

fn attribute_size(av: &AttributeValue) -> usize {
    match av {
        AttributeValue::S(s) => s.len(),
        AttributeValue::N(n) => number_size(n),
        AttributeValue::B(b) => b.as_ref().len(),
        AttributeValue::Bool(_) | AttributeValue::Null(_) => 1,
        AttributeValue::Ss(v) => v.iter().map(String::len).sum(),
        AttributeValue::Ns(v) => v.iter().map(|n| number_size(n)).sum(),
        AttributeValue::Bs(v) => v.iter().map(|b| b.as_ref().len()).sum(),
        AttributeValue::L(v) => 3 + v.iter().map(|a| 1 + attribute_size(a)).sum::<usize>(),
        AttributeValue::M(m) => {
            3 + m
                .iter()
                .map(|(k, a)| 1 + k.len() + attribute_size(a))
                .sum::<usize>()
        }
        _ => 0,
    }
}

/// Approximate DynamoDB item size in bytes (attribute names + values), per
/// the documented sizing rules — used to enforce the 400 KB item and 16 MB
/// `BatchWriteItem` request ceilings before sending.
pub fn item_size(item: &HashMap<String, AttributeValue>) -> usize {
    item.iter().map(|(k, v)| k.len() + attribute_size(v)).sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn numbers_become_json_numbers_only_when_exact() {
        assert_eq!(number_to_json("42"), json!(42));
        assert_eq!(number_to_json("-42"), json!(-42));
        assert_eq!(number_to_json("+7"), json!(7));
        assert_eq!(number_to_json("0"), json!(0));
        assert_eq!(number_to_json("-0.000"), json!(0));
        assert_eq!(number_to_json("1.50"), json!(1.5));
        assert_eq!(number_to_json("1.0"), json!(1));
        assert_eq!(number_to_json("1E+3"), json!(1000));
        assert_eq!(number_to_json("2.5e-3"), json!(0.0025));
        assert_eq!(number_to_json("0.1"), json!(0.1));
        assert_eq!(number_to_json("18446744073709551615"), json!(u64::MAX));
        assert_eq!(number_to_json("-9223372036854775808"), json!(i64::MIN));
        // Beyond the integer types and not an exact f64: kept as the string.
        assert_eq!(
            number_to_json("12345678901234567890123"),
            json!("12345678901234567890123")
        );
        assert_eq!(
            number_to_json("-9223372036854775809"),
            json!("-9223372036854775809")
        );
        assert_eq!(
            number_to_json("3.14159265358979323846264338327950288"),
            json!("3.14159265358979323846264338327950288")
        );
        // Very large but f64-exact in shortest form: a number.
        assert_eq!(number_to_json("1E+30"), json!(1e30));
        // Malformed: passed through verbatim.
        assert_eq!(number_to_json("abc"), json!("abc"));
        assert_eq!(number_to_json(""), json!(""));
        assert_eq!(number_to_json("."), json!("."));
        assert_eq!(number_to_json("1.2.3"), json!("1.2.3"));
        assert_eq!(number_to_json("1ex"), json!("1ex"));
        assert_eq!(
            number_to_json("1e99999999999999999999"),
            json!("1e99999999999999999999")
        );
    }

    #[test]
    fn canonical_decimal_normalizes() {
        let d = canonical_decimal("001.2300e2").unwrap();
        assert_eq!((d.negative, d.digits.as_str(), d.exp), (false, "123", 0));
        let d = canonical_decimal("-.5").unwrap();
        assert_eq!((d.negative, d.digits.as_str(), d.exp), (true, "5", -1));
        let d = canonical_decimal("5.").unwrap();
        assert_eq!((d.digits.as_str(), d.exp), ("5", 0));
        assert!(canonical_decimal("-").is_none());
    }

    #[test]
    fn attribute_to_json_covers_every_type() {
        let mut inner = HashMap::new();
        inner.insert(
            "n".to_string(),
            AttributeValue::N("99999999999999999999999".into()),
        );
        let item: HashMap<String, AttributeValue> = [
            ("s", AttributeValue::S("x".into())),
            ("n", AttributeValue::N("3".into())),
            ("b", AttributeValue::Bool(true)),
            ("nul", AttributeValue::Null(true)),
            ("bin", AttributeValue::B(Blob::new(vec![1, 2, 3]))),
            ("ss", AttributeValue::Ss(vec!["a".into(), "b".into()])),
            ("ns", AttributeValue::Ns(vec!["1".into(), "1.5".into()])),
            ("bs", AttributeValue::Bs(vec![Blob::new(vec![0xff])])),
            (
                "l",
                AttributeValue::L(vec![
                    AttributeValue::S("y".into()),
                    AttributeValue::N("2".into()),
                ]),
            ),
            ("m", AttributeValue::M(inner)),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        let v = item_to_json(&item).unwrap();
        assert_eq!(
            v,
            json!({
                "s": "x", "n": 3, "b": true, "nul": null, "bin": "AQID",
                "ss": ["a", "b"], "ns": [1, 1.5], "bs": ["/w=="],
                "l": ["y", 2], "m": {"n": "99999999999999999999999"}
            })
        );
    }

    #[test]
    fn json_round_trips_through_attributes() {
        let rec = json!({
            "id": "a", "n": 7, "neg": -3, "big": 18446744073709551615u64, "f": 2.5,
            "flag": false, "none": null, "list": [1, "x", {"deep": true}], "obj": {"k": "v"}
        });
        let item = json_to_item(&rec).unwrap();
        assert!(matches!(&item["n"], AttributeValue::N(n) if n == "7"));
        assert!(matches!(&item["big"], AttributeValue::N(n) if n == "18446744073709551615"));
        assert!(matches!(&item["f"], AttributeValue::N(n) if n == "2.5"));
        assert!(matches!(&item["none"], AttributeValue::Null(true)));
        assert_eq!(item_to_json(&item).unwrap(), rec);
        let err = json_to_item(&json!([1])).unwrap_err().to_string();
        assert!(err.contains("array"), "{err}");
        for (v, kind) in [
            (json!(null), "null"),
            (json!(true), "boolean"),
            (json!(1), "number"),
            (json!("s"), "string"),
        ] {
            assert!(json_to_item(&v).unwrap_err().to_string().contains(kind));
        }
        assert_eq!(json_kind(&json!({})), "object");
    }

    #[test]
    fn streams_attributes_convert() {
        let mut m = HashMap::new();
        m.insert("x".to_string(), StreamsAttr::N("1".into()));
        let image: HashMap<String, StreamsAttr> = [
            ("s", StreamsAttr::S("v".into())),
            ("n", StreamsAttr::N("12345678901234567890123".into())),
            ("b", StreamsAttr::Bool(false)),
            ("z", StreamsAttr::Null(true)),
            (
                "bin",
                StreamsAttr::B(aws_sdk_dynamodbstreams::primitives::Blob::new(vec![1])),
            ),
            ("ss", StreamsAttr::Ss(vec!["a".into()])),
            ("ns", StreamsAttr::Ns(vec!["2".into()])),
            (
                "bs",
                StreamsAttr::Bs(vec![aws_sdk_dynamodbstreams::primitives::Blob::new(vec![
                    2,
                ])]),
            ),
            ("l", StreamsAttr::L(vec![StreamsAttr::S("q".into())])),
            ("m", StreamsAttr::M(m)),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        let v = streams_item_to_json(&image).unwrap();
        assert_eq!(
            v,
            json!({
                "s": "v", "n": "12345678901234567890123", "b": false, "z": null,
                "bin": "AQ==", "ss": ["a"], "ns": [2], "bs": ["Ag=="], "l": ["q"], "m": {"x": 1}
            })
        );
    }

    #[test]
    fn typed_json_round_trips_losslessly() {
        let mut inner = HashMap::new();
        inner.insert("q".to_string(), AttributeValue::Bool(true));
        let item: HashMap<String, AttributeValue> = [
            ("s", AttributeValue::S("x".into())),
            (
                "n",
                AttributeValue::N("123456789012345678901234567890".into()),
            ),
            ("b", AttributeValue::B(Blob::new(vec![9, 8]))),
            ("t", AttributeValue::Bool(false)),
            ("z", AttributeValue::Null(true)),
            ("ss", AttributeValue::Ss(vec!["a".into()])),
            ("ns", AttributeValue::Ns(vec!["1".into()])),
            ("bs", AttributeValue::Bs(vec![Blob::new(vec![1])])),
            ("l", AttributeValue::L(vec![AttributeValue::N("2".into())])),
            ("m", AttributeValue::M(inner)),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        let typed = item_to_typed_json(&item);
        assert_eq!(typed["n"], json!({"N": "123456789012345678901234567890"}));
        assert_eq!(typed["b"], json!({"B": "CQg="}));
        assert_eq!(typed_json_to_item(&typed).unwrap(), item);
    }

    #[test]
    fn typed_json_rejects_malformed_input() {
        for bad in [
            json!([1]),
            json!({"a": "plain"}),
            json!({"a": {"S": 1}}),
            json!({"a": {"N": 1}}),
            json!({"a": {"B": 1}}),
            json!({"a": {"B": "!!"}}),
            json!({"a": {"BOOL": "x"}}),
            json!({"a": {"NULL": 1}}),
            json!({"a": {"SS": [1]}}),
            json!({"a": {"BS": "x"}}),
            json!({"a": {"L": "x"}}),
            json!({"a": {"M": []}}),
            json!({"a": {"X": 1}}),
            json!({"a": {"S": "x", "N": "1"}}),
        ] {
            assert!(typed_json_to_item(&bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn item_size_follows_sizing_rules() {
        let item = json_to_item(&json!({
            "pk": "abcd", "n": 12345, "flag": true, "l": ["ab"], "m": {"k": "v"}
        }))
        .unwrap();
        // pk: 2+4, n: 1+(3+1), flag: 4+1, l: 1+3+1+2, m: 1+3+(1+1+1)
        assert_eq!(item_size(&item), 6 + 5 + 5 + 7 + 7);
        let sets: HashMap<String, AttributeValue> = [
            ("a", AttributeValue::Ss(vec!["xy".into(), "z".into()])),
            ("b", AttributeValue::Ns(vec!["1".into(), "22".into()])),
            ("c", AttributeValue::Bs(vec![Blob::new(vec![0; 4])])),
            ("d", AttributeValue::B(Blob::new(vec![0; 10]))),
            ("e", AttributeValue::Null(true)),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        assert_eq!(
            item_size(&sets),
            (1 + 3) + (1 + 4) + (1 + 4) + (1 + 10) + (1 + 1)
        );
        assert_eq!(number_size(&"9".repeat(60)), 21);
    }
}
