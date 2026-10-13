//! Postgres type OID -> JSON Value mapping for text-mode tuple cells.
//!
//! Reference: <https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat>
//! Only the OIDs that ship with every Postgres install are special-cased;
//! anything else falls back to a JSON string.

use base64::Engine;
use faucet_core::FaucetError;
use serde_json::Value;

// Selected pg_type built-in OIDs.
const OID_BOOL: u32 = 16;
const OID_BYTEA: u32 = 17;
const OID_INT2: u32 = 21;
const OID_INT4: u32 = 23;
const OID_INT8: u32 = 20;
const OID_FLOAT4: u32 = 700;
const OID_FLOAT8: u32 = 701;
const OID_NUMERIC: u32 = 1700;
const OID_MONEY: u32 = 790;
const OID_JSON: u32 = 114;
const OID_JSONB: u32 = 3802;
const OID_DATE: u32 = 1082;
const OID_TIME: u32 = 1083;
const OID_TIMESTAMP: u32 = 1114;
const OID_TIMESTAMPTZ: u32 = 1184;
// uuid/interval and other text types pass through verbatim.

/// Map a built-in Postgres *array* type OID to its element type OID, so a
/// `{...}` array literal can be decoded element-by-element instead of being
/// emitted as one opaque string (e.g. `int4[]` → `"{1,2,3}"`) (#78/#45).
/// Returns `None` for non-array OIDs.
fn array_element_oid(array_oid: u32) -> Option<u32> {
    Some(match array_oid {
        1000 => OID_BOOL,    // _bool
        1001 => OID_BYTEA,   // _bytea
        1005 => OID_INT2,    // _int2
        1007 => OID_INT4,    // _int4
        1016 => OID_INT8,    // _int8
        1021 => OID_FLOAT4,  // _float4
        1022 => OID_FLOAT8,  // _float8
        1231 => OID_NUMERIC, // _numeric
        791 => OID_MONEY,    // _money
        199 => OID_JSON,     // _json
        3807 => OID_JSONB,   // _jsonb
        1009 => 25,          // _text → text
        1015 => 1043,        // _varchar → varchar
        1014 => 1042,        // _bpchar → bpchar
        2951 => 2950,        // _uuid → uuid
        1115 => 1114,        // _timestamp
        1185 => 1184,        // _timestamptz
        1182 => 1082,        // _date
        1183 => 1083,        // _time
        _ => return None,
    })
}

/// Decode a text-encoded value with the given column type OID into JSON.
///
/// Unknown OIDs and decode failures both fall back to wrapping the raw text
/// in a JSON string — this is the safest default for a generic CDC
/// connector. We never panic on bad data; the only way this returns `Err` is
/// if a structurally promised invariant is violated (e.g. `OID_BYTEA` text
/// that doesn't start with `\x`).
pub fn text_to_json(type_oid: u32, text: &str) -> Result<Value, FaucetError> {
    Ok(match type_oid {
        OID_BOOL => match text {
            "t" => Value::Bool(true),
            "f" => Value::Bool(false),
            other => {
                return Err(FaucetError::Source(format!(
                    "pgoutput: bool column has non-t/f text {other:?}"
                )));
            }
        },
        OID_INT2 | OID_INT4 | OID_INT8 => {
            let n: i64 = text.parse().map_err(|e| {
                FaucetError::Source(format!(
                    "pgoutput: int (oid={type_oid}) parse {text:?}: {e}"
                ))
            })?;
            Value::from(n)
        }
        // JSON has no NaN/Inf: they become a counted `null` (#885).
        OID_FLOAT4 | OID_FLOAT8 => {
            let n: f64 = text
                .parse()
                .map_err(|e| FaucetError::Source(format!("pgoutput: float parse {text:?}: {e}")))?;
            faucet_core::fidelity::float64(n)
        }
        OID_NUMERIC => faucet_core::fidelity::decimal_text(text)
            .map_err(|e| FaucetError::Source(format!("pgoutput: numeric {e}")))?,
        OID_MONEY => money_json(text)?,
        OID_BYTEA => {
            let stripped = text.strip_prefix("\\x").ok_or_else(|| {
                FaucetError::Source(format!(
                    "pgoutput: bytea text {text:?} missing '\\x' prefix"
                ))
            })?;
            let bytes = hex_decode(stripped)?;
            Value::String(base64::engine::general_purpose::STANDARD.encode(bytes))
        }
        OID_DATE | OID_TIME | OID_TIMESTAMP | OID_TIMESTAMPTZ => temporal_json(type_oid, text)?,
        OID_JSON | OID_JSONB => {
            faucet_core::fidelity::parse_json(text.as_bytes()).map_err(|e| {
                FaucetError::Source(format!("pgoutput: json/jsonb parse {text:?}: {e}"))
            })?
        }
        other => {
            // One-dimensional arrays of a known scalar element type decode
            // into a JSON array; everything else (incl. multi-dimensional
            // arrays, ranges, composites, enums, and unmapped scalars) falls
            // back to the raw Postgres text string (#78/#45).
            if let Some(elem_oid) = array_element_oid(other)
                && let Some(elements) = parse_pg_array(text)
            {
                let mut out = Vec::with_capacity(elements.len());
                for elem in elements {
                    match elem {
                        Some(s) => out.push(text_to_json(elem_oid, &s)?),
                        None => out.push(Value::Null),
                    }
                }
                Value::Array(out)
            } else {
                Value::String(text.into())
            }
        }
    })
}

/// A `money` value, which the server prints in its `lc_monetary` locale
/// (`$1,234.50`, `-$5.00`, `1.234,50 €`, `($5.00)`), as the exact decimal the
/// query source reads through `money::numeric`. The decimal separator is the
/// last `.` or `,` unless exactly three digits follow it (a locale with no
/// fractional digits prints `￥1,235`).
fn money_json(text: &str) -> Result<Value, FaucetError> {
    let negative = text.contains('-') || (text.contains('(') && text.contains(')'));
    let kept: String = text
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == '.' || *c == ',')
        .collect();
    let decimal_at = kept.rfind(['.', ',']).filter(|&i| kept.len() - i - 1 != 3);
    let mut digits = String::with_capacity(kept.len() + 1);
    if negative {
        digits.push('-');
    }
    for (i, c) in kept.char_indices() {
        if c.is_ascii_digit() {
            digits.push(c);
        } else if Some(i) == decimal_at {
            digits.push('.');
        }
    }
    faucet_core::fidelity::decimal_text(&digits)
        .map_err(|e| FaucetError::Source(format!("pgoutput: money {text:?}: {e}")))
}

/// Re-render a temporal value the way the `postgres` query source does —
/// `timestamptz` as RFC 3339 in UTC, `timestamp` / `date` / `time` in chrono's
/// ISO form — so a snapshot and the change stream spell one value one way.
///
/// The server renders these in the session's `DateStyle`, which the
/// replication connection cannot pin. Under any style but `ISO` a date is
/// ambiguous (`04/03/2026`) and a `timestamptz` carries a zone abbreviation, so
/// a value that is not ISO fails the run instead of being passed on as text a
/// sink would misread (#789 SQL-119). `infinity`, `-infinity` and BC dates
/// pass through as the server spells them.
fn temporal_json(type_oid: u32, text: &str) -> Result<Value, FaucetError> {
    use chrono::{DateTime, NaiveDate, NaiveDateTime, SecondsFormat, Utc};
    if matches!(text, "infinity" | "-infinity") || text.ends_with(" BC") {
        return Ok(Value::String(text.into()));
    }
    if type_oid == OID_TIME {
        return if is_iso_time(text) {
            Ok(Value::String(text.into()))
        } else {
            Err(FaucetError::Source(format!(
                "pgoutput: time value {text:?} (oid={type_oid}) is not HH:MM:SS[.ffffff]"
            )))
        };
    }
    let year_digits = iso_year_digits(text);
    if year_digits.is_some_and(|n| n > 4) {
        return Ok(Value::String(text.into()));
    }
    let rendered = match type_oid {
        OID_DATE => NaiveDate::parse_from_str(text, "%Y-%m-%d")
            .ok()
            .map(|d| d.to_string()),
        OID_TIMESTAMP => NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")
            .ok()
            .map(|t| t.to_string()),
        _ => DateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f%#z")
            .ok()
            .map(|t| {
                t.with_timezone(&Utc)
                    .to_rfc3339_opts(SecondsFormat::AutoSi, false)
            }),
    };
    // Still ISO but outside these patterns (a seconds offset on a historic
    // timestamptz): keep the server's spelling.
    if rendered.is_none() && year_digits.is_some() {
        return Ok(Value::String(text.into()));
    }
    rendered.map(Value::String).ok_or_else(|| {
        FaucetError::Source(format!(
            "pgoutput: date/time value {text:?} (oid={type_oid}) is not in ISO format — set \
             DateStyle to ISO for the replication role or database \
             (ALTER ROLE … SET datestyle = 'ISO, MDY')"
        ))
    })
}

/// `HH:MM:SS[.f]` as the server renders `time` in every `DateStyle`, kept as
/// text like the `postgres` query source does (`24:00:00` is a legal value).
fn is_iso_time(text: &str) -> bool {
    let (whole, frac) = match text.split_once('.') {
        Some((w, f)) => (w, Some(f)),
        None => (text, None),
    };
    let b = whole.as_bytes();
    b.len() == 8
        && b[2] == b':'
        && b[5] == b':'
        && [0, 1, 3, 4, 6, 7].iter().all(|&i| b[i].is_ascii_digit())
        && frac.is_none_or(|f| !f.is_empty() && f.bytes().all(|c| c.is_ascii_digit()))
}

/// The number of year digits when `text` starts with an ISO date
/// (`YYYY-MM-DD`, where PostgreSQL allows years past 9999).
fn iso_year_digits(text: &str) -> Option<usize> {
    let b = text.as_bytes();
    let n = b.iter().take_while(|c| c.is_ascii_digit()).count();
    let rest = &b[n..];
    (n >= 4
        && rest.len() >= 6
        && rest[0] == b'-'
        && rest[3] == b'-'
        && [1, 2, 4, 5].iter().all(|&i| rest[i].is_ascii_digit()))
    .then_some(n)
}

/// Parse a one-dimensional Postgres array literal (`{a,b,"c,d",NULL}`) into a
/// vector of element texts, where `None` marks an unquoted `NULL`. Returns
/// `None` (caller falls back to the raw string) for anything this simple
/// parser doesn't handle: a non-`{...}`-delimited input or a nested
/// (multi-dimensional) array.
fn parse_pg_array(text: &str) -> Option<Vec<Option<String>>> {
    let bytes = text.as_bytes();
    if bytes.first() != Some(&b'{') || bytes.last() != Some(&b'}') {
        return None;
    }
    let inner = &text[1..text.len() - 1];
    if inner.is_empty() {
        return Some(Vec::new());
    }

    let mut out: Vec<Option<String>> = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    let mut quoted = false; // current element was quoted (so "NULL" != NULL)
    let mut chars = inner.chars().peekable();

    while let Some(c) = chars.next() {
        if in_quotes {
            match c {
                '\\' => {
                    // Escaped next char (\" or \\) is taken literally.
                    if let Some(next) = chars.next() {
                        cur.push(next);
                    }
                }
                '"' => in_quotes = false,
                _ => cur.push(c),
            }
            continue;
        }
        match c {
            '"' => {
                in_quotes = true;
                quoted = true;
            }
            // A nested array — bail out, let the caller keep the raw string.
            '{' => return None,
            ',' => {
                out.push(finish_array_element(&cur, quoted));
                cur.clear();
                quoted = false;
            }
            _ => cur.push(c),
        }
    }
    if in_quotes {
        return None; // unterminated quote — malformed, fall back to string
    }
    out.push(finish_array_element(&cur, quoted));
    Some(out)
}

/// An unquoted `NULL` token is a SQL NULL; anything else (incl. a quoted
/// `"NULL"`) is the literal text.
fn finish_array_element(raw: &str, quoted: bool) -> Option<String> {
    if !quoted && raw == "NULL" {
        None
    } else {
        Some(raw.to_owned())
    }
}

fn hex_decode(s: &str) -> Result<Vec<u8>, FaucetError> {
    if !s.len().is_multiple_of(2) {
        return Err(FaucetError::Source(format!(
            "pgoutput: bytea hex has odd length: {s:?}"
        )));
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    for i in (0..s.len()).step_by(2) {
        out.push(
            u8::from_str_radix(&s[i..i + 2], 16)
                .map_err(|e| FaucetError::Source(format!("pgoutput: bytea hex {s:?}: {e}")))?,
        );
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn bool_t_and_f() {
        assert_eq!(text_to_json(OID_BOOL, "t").unwrap(), json!(true));
        assert_eq!(text_to_json(OID_BOOL, "f").unwrap(), json!(false));
        assert!(text_to_json(OID_BOOL, "yes").is_err());
    }

    #[test]
    fn integer_types() {
        assert_eq!(text_to_json(OID_INT2, "32000").unwrap(), json!(32000));
        assert_eq!(text_to_json(OID_INT4, "-1").unwrap(), json!(-1));
        assert_eq!(
            text_to_json(OID_INT8, "9223372036854775807").unwrap(),
            json!(9223372036854775807_i64)
        );
        assert!(text_to_json(OID_INT4, "abc").is_err());
    }

    #[test]
    fn floats() {
        assert_eq!(text_to_json(OID_FLOAT8, "3.5").unwrap(), json!(3.5));
        // NaN/Inf are a counted null (#885).
        assert_eq!(text_to_json(OID_FLOAT8, "NaN").unwrap(), Value::Null);
        assert_eq!(text_to_json(OID_FLOAT4, "Infinity").unwrap(), Value::Null);
        assert_eq!(text_to_json(OID_FLOAT8, "-Infinity").unwrap(), Value::Null);
        assert_eq!(text_to_json(OID_NUMERIC, "NaN").unwrap(), Value::Null);
        assert!(text_to_json(OID_NUMERIC, "x").is_err());
        for (text, exact) in [
            ("$1,234.50", "1234.50"),
            ("-$5.00", "-5.00"),
            ("($5.00)", "-5.00"),
            ("1.234,50 €", "1234.50"),
            ("￥1,235", "1235"),
            ("$0.07", "0.07"),
        ] {
            assert_eq!(
                text_to_json(OID_MONEY, text).unwrap(),
                json!(exact),
                "{text}"
            );
        }
        assert!(text_to_json(OID_MONEY, "n/a").is_err());
        assert_eq!(
            text_to_json(791, "{\"$1.00\",\"$2.50\"}").unwrap(),
            json!(["1.00", "2.50"])
        );
        assert!(text_to_json(OID_FLOAT8, "x").is_err());
        assert_eq!(
            text_to_json(OID_JSONB, r#"{"n": 12345678901234567890.5}"#).unwrap(),
            json!({"n": "12345678901234567890.5"})
        );
    }

    #[test]
    fn int_array_decodes_to_json_array() {
        assert_eq!(text_to_json(1007, "{1,2,3}").unwrap(), json!([1, 2, 3]));
        assert_eq!(text_to_json(1007, "{}").unwrap(), json!([]));
        assert_eq!(text_to_json(1016, "{-9,0,9}").unwrap(), json!([-9, 0, 9]));
    }

    #[test]
    fn text_array_handles_quotes_nulls_and_commas() {
        assert_eq!(
            text_to_json(1009, r#"{a,"b,c",NULL,"NULL"}"#).unwrap(),
            json!(["a", "b,c", null, "NULL"])
        );
        // Escaped quote inside a quoted element.
        assert_eq!(
            text_to_json(1009, r#"{"he said \"hi\""}"#).unwrap(),
            json!(["he said \"hi\""])
        );
    }

    #[test]
    fn bool_array_decodes() {
        assert_eq!(
            text_to_json(1000, "{t,f,t}").unwrap(),
            json!([true, false, true])
        );
    }

    #[test]
    fn multidimensional_array_falls_back_to_string() {
        assert_eq!(
            text_to_json(1007, "{{1,2},{3,4}}").unwrap(),
            json!("{{1,2},{3,4}}")
        );
    }

    #[test]
    fn numeric_kept_as_string() {
        assert_eq!(
            text_to_json(OID_NUMERIC, "12345.67890").unwrap(),
            json!("12345.67890")
        );
    }

    #[test]
    fn bytea_base64() {
        // \xDEADBEEF -> base64 "3q2+7w=="
        assert_eq!(
            text_to_json(OID_BYTEA, "\\xDEADBEEF").unwrap(),
            json!("3q2+7w==")
        );
        assert!(text_to_json(OID_BYTEA, "deadbeef").is_err()); // missing \x
        assert!(text_to_json(OID_BYTEA, "\\xZZ").is_err()); // not hex
    }

    #[test]
    fn json_columns_parsed() {
        assert_eq!(
            text_to_json(OID_JSON, r#"{"a":1}"#).unwrap(),
            json!({"a": 1})
        );
        assert_eq!(
            text_to_json(OID_JSONB, r#"[1,2,3]"#).unwrap(),
            json!([1, 2, 3])
        );
    }

    #[test]
    fn unknown_oid_falls_back_to_string() {
        assert_eq!(
            text_to_json(99999, "2026-05-17 12:34:56+00").unwrap(),
            json!("2026-05-17 12:34:56+00")
        );
    }

    #[test]
    fn int2_and_int8_arrays_decode() {
        // _int2 (1005) → int2 elements.
        assert_eq!(text_to_json(1005, "{1,2,3}").unwrap(), json!([1, 2, 3]));
        // _int8 (1016) is already covered elsewhere; assert _int2 negative too.
        assert_eq!(text_to_json(1005, "{-7,7}").unwrap(), json!([-7, 7]));
    }

    #[test]
    fn float_arrays_decode() {
        // _float4 (1021) and _float8 (1022) → JSON numbers.
        assert_eq!(text_to_json(1021, "{1.5,2.5}").unwrap(), json!([1.5, 2.5]));
        assert_eq!(
            text_to_json(1022, "{3.25,-4.75}").unwrap(),
            json!([3.25, -4.75])
        );
    }

    #[test]
    fn numeric_array_keeps_elements_as_strings() {
        // _numeric (1231) → each element stays a string (NUMERIC is OID_NUMERIC).
        assert_eq!(
            text_to_json(1231, "{1.10,2.20}").unwrap(),
            json!(["1.10", "2.20"])
        );
    }

    #[test]
    fn bytea_array_decodes_to_base64_elements() {
        // _bytea (1001) → element OID_BYTEA → base64. \xDEADBEEF → "3q2+7w==".
        assert_eq!(
            text_to_json(1001, "{\\xDEADBEEF}").unwrap(),
            json!(["3q2+7w=="])
        );
    }

    #[test]
    fn json_and_jsonb_arrays_decode_elements() {
        // _json (199) and _jsonb (3807) → element OID_JSON/JSONB → parsed JSON.
        // The element values are quoted so the embedded braces are not treated
        // as a nested array by the array parser.
        assert_eq!(
            text_to_json(199, r#"{"{\"a\":1}"}"#).unwrap(),
            json!([{"a": 1}])
        );
        assert_eq!(text_to_json(3807, r#"{"[1,2]"}"#).unwrap(), json!([[1, 2]]));
    }

    #[test]
    fn varchar_bpchar_uuid_arrays_decode_to_strings() {
        // _varchar (1015), _bpchar (1014), _uuid (2951) all map to string
        // element types and pass through verbatim.
        assert_eq!(text_to_json(1015, "{a,b}").unwrap(), json!(["a", "b"]));
        assert_eq!(text_to_json(1014, "{x,y}").unwrap(), json!(["x", "y"]));
        assert_eq!(
            text_to_json(2951, "{11111111-1111-1111-1111-111111111111}").unwrap(),
            json!(["11111111-1111-1111-1111-111111111111"])
        );
    }

    #[test]
    fn temporal_values_are_normalised_or_refused() {
        assert_eq!(
            text_to_json(1184, "2026-03-04 10:00:00.12+05:30").unwrap(),
            json!("2026-03-04T04:30:00.120+00:00")
        );
        assert_eq!(
            text_to_json(1184, "2026-03-04 10:00:00-03").unwrap(),
            json!("2026-03-04T13:00:00+00:00")
        );
        assert_eq!(
            text_to_json(1114, "2026-03-04 10:00:00.5").unwrap(),
            json!("2026-03-04 10:00:00.500")
        );
        assert_eq!(
            text_to_json(1082, "2026-03-04").unwrap(),
            json!("2026-03-04")
        );
        assert_eq!(text_to_json(1083, "23:59:59").unwrap(), json!("23:59:59"));
        for raw in ["infinity", "-infinity", "0044-03-15 BC"] {
            assert_eq!(text_to_json(1082, raw).unwrap(), json!(raw));
        }
        assert_eq!(
            text_to_json(1184, "1890-01-01 00:00:00+05:53:28").unwrap(),
            json!("1890-01-01 00:00:00+05:53:28"),
            "ISO with a seconds offset passes through"
        );
        for (oid, raw) in [
            (1082, "04/03/2026"),
            (1184, "Wed Mar 04 10:00:00 2026 IST"),
            (1114, "04.03.2026 10:00:00"),
        ] {
            let err = text_to_json(oid, raw).unwrap_err().to_string();
            assert!(err.contains("DateStyle"), "{err}");
        }
    }

    #[test]
    fn time_keeps_the_server_text_and_extended_years_pass_through() {
        for raw in ["24:00:00", "10:00:00.5", "00:00:00.000001"] {
            assert_eq!(text_to_json(1083, raw).unwrap(), json!(raw));
        }
        for raw in ["1:00:00", "10:00", "10:00:00.", "10:00:0x", "ab:cd:ef"] {
            let err = text_to_json(1083, raw).unwrap_err().to_string();
            assert!(err.contains("HH:MM:SS"), "{raw}: {err}");
        }
        for (oid, raw) in [
            (1082, "10000-01-01"),
            (1114, "12345-06-07 08:09:10"),
            (1184, "10000-01-01 00:00:00+00"),
        ] {
            assert_eq!(text_to_json(oid, raw).unwrap(), json!(raw));
        }
        assert_eq!(super::iso_year_digits("2026-03-04"), Some(4));
        assert_eq!(super::iso_year_digits("226-03-04"), None);
        assert_eq!(super::iso_year_digits("2026-3-04"), None);
        assert_eq!(super::iso_year_digits("2026-03"), None);
    }

    #[test]
    fn datetime_arrays_decode_to_strings() {
        // _timestamp (1115), _timestamptz (1185), _date (1182), _time (1183)
        // all map to text-passthrough element types. Commas-free literals so
        // the simple parser splits cleanly.
        assert_eq!(
            text_to_json(1115, r#"{"2026-05-17 12:34:56"}"#).unwrap(),
            json!(["2026-05-17 12:34:56"])
        );
        assert_eq!(
            text_to_json(1185, r#"{"2026-05-17 12:34:56+00"}"#).unwrap(),
            json!(["2026-05-17T12:34:56+00:00"])
        );
        assert_eq!(
            text_to_json(1182, "{2026-05-17}").unwrap(),
            json!(["2026-05-17"])
        );
        assert_eq!(
            text_to_json(1183, "{12:34:56}").unwrap(),
            json!(["12:34:56"])
        );
    }

    #[test]
    fn float_array_element_parse_error_propagates() {
        // A float array whose element is not parseable surfaces the float
        // parse error (lines 90-92) rather than falling back to a string.
        let err = text_to_json(1022, "{not_a_float}").unwrap_err();
        assert!(err.to_string().contains("float parse"), "{err}");
    }

    #[test]
    fn json_array_element_parse_error_propagates() {
        // A _json array whose element is not valid JSON surfaces the json
        // parse error (lines 108-110).
        let err = text_to_json(199, "{notjson}").unwrap_err();
        assert!(err.to_string().contains("json/jsonb parse"), "{err}");
    }

    #[test]
    fn array_oid_with_non_brace_text_falls_back_to_string() {
        // An array OID (1007 _int4) whose text is not `{...}`-delimited makes
        // parse_pg_array return None (line 142), so text_to_json falls back to
        // wrapping the raw text in a string.
        assert_eq!(
            text_to_json(1007, "not-an-array").unwrap(),
            json!("not-an-array")
        );
    }

    #[test]
    fn array_with_unterminated_quote_falls_back_to_string() {
        // An unterminated quote makes parse_pg_array return None (line 185),
        // so the whole literal is kept as a raw string.
        let raw = r#"{"unterminated}"#;
        assert_eq!(text_to_json(1009, raw).unwrap(), json!(raw));
    }

    #[test]
    fn bytea_odd_length_hex_errors() {
        // \x followed by an odd number of hex digits is rejected (lines 202-205).
        let err = text_to_json(OID_BYTEA, "\\xABC").unwrap_err();
        assert!(err.to_string().contains("odd length"), "{err}");
    }
}
