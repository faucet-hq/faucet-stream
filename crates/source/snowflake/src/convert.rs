//! Type-aware conversion from Snowflake SQL REST API rows to JSON objects.
//!
//! The Snowflake SQL REST API returns every cell as a string, regardless of
//! its column type, and ships a schema (`resultSetMetaData.rowType`) alongside
//! the rows. This module pairs each cell with its column metadata and
//! produces a `serde_json::Value::Object` per row.
//!
//! Type coverage (Snowflake's `type` field is lowercase in the JSON v2 format):
//!
//! | Snowflake type          | JSON output                              |
//! |-------------------------|------------------------------------------|
//! | `fixed`, scale 0        | `Number` (i64/u64 if it fits, else `String` — full precision) |
//! | `fixed`, scale > 0      | `String` (exact decimal — full precision preserved) |
//! | `real`                  | `Number` (f64)                           |
//! | `boolean`               | `Bool`                                   |
//! | `text` / `binary`       | `String`                                 |
//! | `date`                  | `String` `YYYY-MM-DD`                    |
//! | `time`                  | `String` `HH:MM:SS[.fff…]`               |
//! | `timestamp_ntz`         | `String` `YYYY-MM-DDTHH:MM:SS[.fff…]` (no offset) |
//! | `timestamp_ltz`         | `String` UTC, `…Z`                       |
//! | `timestamp_tz`          | `String` wall time with its `+HH:MM` offset |
//! | `variant` / `object` / `array` | parsed JSON value (falls back to `String`) |
//! | anything else           | `String` (raw cell)                      |
//!
//! `null` cells map to `Value::Null` regardless of declared type.

use serde::Deserialize;
use serde_json::{Map, Value};

/// One entry in `resultSetMetaData.rowType`. Only the fields we actually use
/// are deserialised; everything else is ignored.
#[derive(Debug, Clone, Deserialize)]
pub struct ColumnMeta {
    /// Column name as Snowflake reports it (upper-case for unquoted
    /// identifiers); the casing is kept untouched as the row's JSON key.
    pub name: String,
    /// Low-level Snowflake type — lowercased identifiers like `"fixed"`,
    /// `"text"`, `"boolean"`, `"variant"`, `"timestamp_ntz"`. See the
    /// Snowflake [JSON v2 result format
    /// docs](https://docs.snowflake.com/en/developer-guide/sql-api/handling-responses).
    #[serde(rename = "type")]
    pub ty: String,
    /// Scale of a `fixed` (`NUMBER`/`DECIMAL`/`NUMERIC`) column — the number of
    /// digits after the decimal point. Snowflake reports `0` for integer-typed
    /// fixed columns and `> 0` for fractional ones. Absent for non-fixed types
    /// (defaults to `0`). A non-zero scale means the cell carries a fractional
    /// decimal whose exact value is preserved losslessly as a JSON string,
    /// matching how the BigQuery source treats `NUMERIC`/`BIGNUMERIC`.
    #[serde(default)]
    pub scale: i64,
}

/// Build a JSON object out of one Snowflake row (an array of string cells).
///
/// `row` is expected to have the same length as `columns`. If the row is
/// shorter, missing trailing cells are treated as `null`; if it is longer,
/// the extras are silently ignored.
pub fn row_to_json(row: &[Value], columns: &[ColumnMeta]) -> Value {
    let mut obj = Map::with_capacity(columns.len());
    for (i, col) in columns.iter().enumerate() {
        let cell = row.get(i);
        obj.insert(col.name.clone(), cell_to_json(cell, &col.ty, col.scale));
    }
    Value::Object(obj)
}

/// Convert a single Snowflake cell to a typed `serde_json::Value`.
///
/// `scale` is the column's reported decimal scale (only meaningful for `fixed`
/// columns; `0` otherwise).
fn cell_to_json(cell: Option<&Value>, ty: &str, scale: i64) -> Value {
    let Some(cell) = cell else {
        return Value::Null;
    };
    // SQL REST API ships `null` as a JSON null in the array.
    if cell.is_null() {
        return Value::Null;
    }
    // Pre-typed values (already a JSON number / bool / object) are passed
    // through verbatim. This is defensive — Snowflake's documented JSON v2
    // format always wraps cells as strings, but tolerating already-typed
    // values keeps the converter robust against future format tweaks.
    let s = match cell {
        Value::String(s) => s.as_str(),
        other => return other.clone(),
    };

    let ty = ty.to_ascii_lowercase();
    match ty.as_str() {
        "fixed" => parse_number(s, scale),
        "real" => parse_real(s),
        "boolean" => parse_bool(s),
        "variant" | "object" | "array" => {
            serde_json::from_str(s).unwrap_or_else(|_| Value::String(s.to_owned()))
        }
        "date" | "time" | "timestamp_ntz" | "timestamp_ltz" | "timestamp_tz" => {
            temporal_to_iso(s, &ty)
                .map(Value::String)
                .unwrap_or_else(|| Value::String(s.to_owned()))
        }
        _ => Value::String(s.to_owned()),
    }
}

/// Render a SQL API temporal cell as ISO 8601. The JSON v2 format sends a
/// DATE as days since the epoch, a TIME as seconds since midnight, NTZ/LTZ
/// timestamps as epoch seconds, and TZ as `<epoch seconds> <offset + 1440>`
/// (offset in minutes). Seconds are split as text so the fraction is exact.
/// `None` when the cell is not in that shape.
fn temporal_to_iso(s: &str, ty: &str) -> Option<String> {
    let s = s.trim();
    match ty {
        "date" => {
            let days: i64 = s.parse().ok()?;
            let (y, m, d) = civil_from_days(days);
            Some(format!("{y:04}-{m:02}-{d:02}"))
        }
        "time" => {
            let (secs, frac) = split_seconds(s)?;
            Some(format!("{}{frac}", clock(secs.rem_euclid(86_400))))
        }
        "timestamp_ntz" => {
            let (secs, frac) = split_seconds(s)?;
            Some(format!("{}{frac}", datetime(secs)))
        }
        "timestamp_ltz" => {
            let (secs, frac) = split_seconds(s)?;
            Some(format!("{}{frac}Z", datetime(secs)))
        }
        "timestamp_tz" => {
            let (epoch, tz) = s.split_once(' ')?;
            let (secs, frac) = split_seconds(epoch)?;
            let offset_min = tz.trim().parse::<i64>().ok()? - 1440;
            let sign = if offset_min < 0 { '-' } else { '+' };
            let abs = offset_min.abs();
            Some(format!(
                "{}{frac}{sign}{:02}:{:02}",
                datetime(secs + offset_min * 60),
                abs / 60,
                abs % 60
            ))
        }
        _ => None,
    }
}

/// Split a decimal seconds value into whole seconds (floored) and the exact
/// fraction text (`.ddd`, empty when there is none).
fn split_seconds(s: &str) -> Option<(i64, String)> {
    let (neg, body) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s),
    };
    let (int, frac) = body.split_once('.').unwrap_or((body, ""));
    if int.is_empty() || !int.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if !frac.bytes().all(|b| b.is_ascii_digit()) || frac.len() > 18 {
        return None;
    }
    let whole: i64 = int.parse().ok()?;
    let has_frac = frac.bytes().any(|b| b != b'0');
    if !neg {
        let f = if frac.is_empty() {
            String::new()
        } else {
            format!(".{frac}")
        };
        return Some((whole, f));
    }
    if !has_frac {
        let f = if frac.is_empty() {
            String::new()
        } else {
            format!(".{frac}")
        };
        return Some((-whole, f));
    }
    let scale = 10u64.pow(frac.len() as u32);
    let complement = scale - frac.parse::<u64>().ok()?;
    Some((
        -whole - 1,
        format!(".{complement:0width$}", width = frac.len()),
    ))
}

fn clock(secs_of_day: i64) -> String {
    format!(
        "{:02}:{:02}:{:02}",
        secs_of_day / 3600,
        (secs_of_day / 60) % 60,
        secs_of_day % 60
    )
}

fn datetime(epoch_secs: i64) -> String {
    let (y, m, d) = civil_from_days(epoch_secs.div_euclid(86_400));
    format!(
        "{y:04}-{m:02}-{d:02}T{}",
        clock(epoch_secs.rem_euclid(86_400))
    )
}

/// Proleptic Gregorian (year, month, day) for a day count since 1970-01-01.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

/// Parse a `FIXED` (`NUMBER`/`DECIMAL`/`NUMERIC`) column value losslessly.
///
/// Snowflake reports a `scale` for fixed-point columns:
///
/// - **scale > 0** (any `NUMBER(p,s)`/`DECIMAL`/`NUMERIC` with `s > 0`, i.e.
///   every monetary/decimal column): the cell is a fractional decimal whose
///   exact value cannot in general be represented by an `f64`
///   (`serde_json` is built here *without* `arbitrary_precision`, so a JSON
///   number is always an `f64`). To honor the connector's documented
///   full-precision contract we keep the **exact decimal text as a JSON
///   string**, matching how the BigQuery source preserves `NUMERIC`/
///   `BIGNUMERIC`. We only fall back to numeric parsing when the value is not
///   a well-formed finite decimal (defensive — never observed from Snowflake).
/// - **scale 0** (integer-typed `NUMBER(p,0)`): parsed as a JSON integer when
///   it fits `i64`/`u64`; a value beyond `u64` (e.g. `NUMBER(38,0)`) is kept
///   as a lossless string rather than dropping precision through `f64`.
///
/// This is also robust when the scale metadata is missing/unreliable: a `fixed`
/// cell whose text is a fractional decimal is preserved as a string regardless
/// of the reported scale.
fn parse_number(s: &str, scale: i64) -> Value {
    let trimmed = s.trim();

    if scale > 0 || is_fractional_decimal(trimmed) {
        // Fractional fixed value — preserve the exact decimal text losslessly
        // as a string when it is a well-formed finite decimal. A non-decimal /
        // non-finite token (shouldn't occur for `fixed`) falls back to numeric
        // parsing for round-trip safety.
        if is_finite_decimal(trimmed) {
            return Value::String(s.to_owned());
        }
        return parse_real(s);
    }

    if let Ok(i) = s.parse::<i64>() {
        return Value::Number(i.into());
    }
    if let Ok(u) = s.parse::<u64>() {
        return Value::Number(u.into());
    }
    // Past u64: only an integer literal stays a (lossless) string; a decimal or
    // scientific value is genuinely floating-point, so let `parse_real` handle it.
    let digits = trimmed.strip_prefix(['+', '-']).unwrap_or(trimmed);
    if !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()) {
        return Value::String(s.to_owned());
    }
    parse_real(s)
}

/// True when `s` is a finite decimal literal containing a fractional component
/// (a `.` with digits, or a scientific exponent) — i.e. not a plain integer.
fn is_fractional_decimal(s: &str) -> bool {
    is_finite_decimal(s) && (s.contains('.') || s.contains(['e', 'E']))
}

/// True when `s` is a well-formed finite decimal literal: an optional sign,
/// decimal digits with at most one decimal point, and an optional scientific
/// exponent. Rejects `NaN`/`Infinity` and any non-numeric token, so they fall
/// through to `parse_real`'s round-trip-safe handling.
fn is_finite_decimal(s: &str) -> bool {
    let body = s.strip_prefix(['+', '-']).unwrap_or(s);
    if body.is_empty() {
        return false;
    }

    // Split off an optional scientific exponent (`e`/`E` + signed integer).
    let (mantissa, exponent) = match body.split_once(['e', 'E']) {
        Some((m, e)) => (m, Some(e)),
        None => (body, None),
    };

    // Mantissa: digits with at most one decimal point, and at least one digit.
    let mut seen_dot = false;
    let mut seen_digit = false;
    for b in mantissa.bytes() {
        match b {
            b'0'..=b'9' => seen_digit = true,
            b'.' if !seen_dot => seen_dot = true,
            _ => return false,
        }
    }
    if !seen_digit {
        return false;
    }

    // Exponent (if present): optional sign + at least one digit.
    if let Some(exp) = exponent {
        let exp_digits = exp.strip_prefix(['+', '-']).unwrap_or(exp);
        if exp_digits.is_empty() || !exp_digits.bytes().all(|b| b.is_ascii_digit()) {
            return false;
        }
    }

    true
}

/// Parse a floating-point column. Non-finite values (`Infinity`, `NaN`) are
/// not representable in JSON and fall back to a string for round-trip safety.
fn parse_real(s: &str) -> Value {
    match s.parse::<f64>() {
        Ok(f) => serde_json::Number::from_f64(f)
            .map(Value::Number)
            .unwrap_or_else(|| Value::String(s.to_owned())),
        Err(_) => Value::String(s.to_owned()),
    }
}

/// Parse a boolean column. Snowflake ships booleans as `"true"`/`"false"`
/// (lowercase) in the JSON v2 format, but we accept the canonical Snowflake
/// SQL forms (`"TRUE"`/`"FALSE"`, `"1"`/`"0"`) too.
fn parse_bool(s: &str) -> Value {
    match s {
        "true" | "TRUE" | "1" => Value::Bool(true),
        "false" | "FALSE" | "0" => Value::Bool(false),
        other => Value::String(other.to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn col(name: &str, ty: &str) -> ColumnMeta {
        ColumnMeta {
            name: name.into(),
            ty: ty.into(),
            scale: 0,
        }
    }

    /// A `fixed` column with an explicit (non-zero) decimal scale.
    fn col_scaled(name: &str, scale: i64) -> ColumnMeta {
        ColumnMeta {
            name: name.into(),
            ty: "fixed".into(),
            scale,
        }
    }

    #[test]
    fn fixed_parses_as_integer() {
        let row = [json!("42")];
        let cols = [col("ID", "fixed")];
        assert_eq!(row_to_json(&row, &cols), json!({"ID": 42}));
    }

    #[test]
    fn fixed_fractional_preserves_exact_decimal_as_string() {
        // A `fixed`/NUMBER column with non-zero scale ships a fractional
        // decimal. Decoding it as f64 (the previous behavior) silently lost
        // precision; the exact decimal text is preserved as a string instead,
        // matching the BigQuery source's NUMERIC handling.
        let row = [json!("2.5")];
        let cols = [col_scaled("RATIO", 1)];
        assert_eq!(row_to_json(&row, &cols), json!({"RATIO": "2.5"}));
    }

    #[test]
    fn fixed_high_precision_decimal_preserves_all_digits() {
        // 29 significant digits + 9 fractional — far beyond f64's ~15–17
        // significant digits. Round-tripping through f64 would corrupt this.
        let row = [json!("12345678901234567890.123456789")];
        let cols = [col_scaled("AMOUNT", 9)];
        let out = row_to_json(&row, &cols);
        assert_eq!(out, json!({"AMOUNT": "12345678901234567890.123456789"}));
        // Assert the serialized JSON is byte-exact — no digit dropped, no
        // float rounding, value emitted as a JSON string.
        assert_eq!(
            serde_json::to_string(&out).unwrap(),
            r#"{"AMOUNT":"12345678901234567890.123456789"}"#
        );
    }

    #[test]
    fn fixed_monetary_amount_preserves_exact_value() {
        let row = [json!("1234.56")];
        let cols = [col_scaled("PRICE", 2)];
        let out = row_to_json(&row, &cols);
        assert_eq!(out, json!({"PRICE": "1234.56"}));
        assert_eq!(
            serde_json::to_string(&out).unwrap(),
            r#"{"PRICE":"1234.56"}"#
        );
    }

    #[test]
    fn fixed_negative_fractional_preserves_exact_value() {
        let row = [json!("-0.0000000001")];
        let cols = [col_scaled("DELTA", 10)];
        assert_eq!(row_to_json(&row, &cols), json!({"DELTA": "-0.0000000001"}));
    }

    #[test]
    fn fixed_scale_zero_integer_stays_json_integer() {
        // Scale-0 fixed values that fit i64 remain JSON integers.
        let row = [json!("100"), json!("-7")];
        let cols = [col_scaled("A", 0), col_scaled("B", 0)];
        assert_eq!(row_to_json(&row, &cols), json!({"A": 100, "B": -7}));
        let out = row_to_json(&row, &cols);
        assert_eq!(serde_json::to_string(&out).unwrap(), r#"{"A":100,"B":-7}"#);
    }

    #[test]
    fn fixed_fractional_detected_without_scale_metadata() {
        // Defensive: even if scale metadata is missing/zero, a `fixed` cell
        // whose text is fractional is preserved losslessly as a string rather
        // than dropping precision through f64.
        let row = [json!("3.141592653589793238462643383279")];
        let cols = [col("PI", "fixed")]; // scale defaults to 0
        assert_eq!(
            row_to_json(&row, &cols),
            json!({"PI": "3.141592653589793238462643383279"})
        );
    }

    #[test]
    fn fixed_scientific_notation_preserved_as_string() {
        // A scaled fixed value rendered in scientific notation is still a
        // fractional decimal — keep its exact text.
        let row = [json!("1.5e10")];
        let cols = [col_scaled("X", 4)];
        assert_eq!(row_to_json(&row, &cols), json!({"X": "1.5e10"}));
    }

    #[test]
    fn fixed_scale_present_in_column_metadata_deserializes() {
        // The `scale` field is read from Snowflake's rowType metadata.
        let meta: ColumnMeta =
            serde_json::from_value(json!({"name": "AMT", "type": "fixed", "scale": 2})).unwrap();
        assert_eq!(meta.scale, 2);
        // Absent scale defaults to 0.
        let meta0: ColumnMeta =
            serde_json::from_value(json!({"name": "ID", "type": "fixed"})).unwrap();
        assert_eq!(meta0.scale, 0);
    }

    #[test]
    fn fixed_non_decimal_token_falls_back_to_real() {
        // Defensive guard: a `fixed` column reporting scale>0 but carrying a
        // non-finite/garbage token must not be emitted as a misleading numeric
        // string. It falls back to parse_real (string for non-finite).
        let row = [json!("NaN")];
        let cols = [col_scaled("X", 2)];
        assert_eq!(row_to_json(&row, &cols), json!({"X": "NaN"}));
    }

    #[test]
    fn fixed_past_i64_within_u64_stays_numeric() {
        // A FIXED/NUMBER(20,0) value above i64::MAX but within u64 range is
        // still losslessly representable as a JSON integer — keep it numeric
        // instead of dropping precision through f64.
        let row = [json!("18446744073709551615")]; // u64::MAX
        let cols = [col("ID", "fixed")];
        assert_eq!(
            row_to_json(&row, &cols),
            json!({"ID": 18446744073709551615u64})
        );
    }

    #[test]
    fn fixed_beyond_u64_kept_as_string_lossless() {
        // A NUMBER(38,0) value beyond u64 can't be a JSON integer; keep it as a
        // string (lossless) rather than a lossy f64.
        let row = [json!("123456789012345678901234567890")];
        let cols = [col("ID", "fixed")];
        assert_eq!(
            row_to_json(&row, &cols),
            json!({"ID": "123456789012345678901234567890"})
        );
    }

    #[test]
    fn real_parses_as_float() {
        let row = [json!("0.5")];
        let cols = [col("X", "real")];
        assert_eq!(row_to_json(&row, &cols), json!({"X": 0.5}));
    }

    #[test]
    fn boolean_parses_lowercase() {
        let row = [json!("true"), json!("false")];
        let cols = [col("A", "boolean"), col("B", "boolean")];
        assert_eq!(row_to_json(&row, &cols), json!({"A": true, "B": false}));
    }

    #[test]
    fn variant_parses_inner_json() {
        let row = [json!(r#"{"k":1}"#)];
        let cols = [col("DATA", "variant")];
        assert_eq!(row_to_json(&row, &cols), json!({"DATA": {"k": 1}}));
    }

    #[test]
    fn variant_falls_back_to_string_on_invalid_json() {
        let row = [json!("not-json")];
        let cols = [col("DATA", "variant")];
        assert_eq!(row_to_json(&row, &cols), json!({"DATA": "not-json"}));
    }

    #[test]
    fn text_passes_through() {
        let row = [json!("hello")];
        let cols = [col("NAME", "text")];
        assert_eq!(row_to_json(&row, &cols), json!({"NAME": "hello"}));
    }

    #[test]
    fn null_cell_maps_to_null() {
        let row = [json!(null)];
        let cols = [col("X", "fixed")];
        assert_eq!(row_to_json(&row, &cols), json!({"X": null}));
    }

    #[test]
    fn missing_trailing_cell_maps_to_null() {
        let row: [Value; 0] = [];
        let cols = [col("X", "text")];
        assert_eq!(row_to_json(&row, &cols), json!({"X": null}));
    }

    #[test]
    fn temporal_cells_render_as_iso_8601() {
        let row = [
            json!("19675"),
            json!("45296.123456789"),
            json!("1700000000.000000000"),
            json!("1700000000.5"),
            json!("1700000000.250 1920"),
            json!("1700000000 1380"),
        ];
        let cols = [
            col("D", "date"),
            col("T", "time"),
            col("NTZ", "timestamp_ntz"),
            col("LTZ", "TIMESTAMP_LTZ"),
            col("TZ", "timestamp_tz"),
            col("TZW", "timestamp_tz"),
        ];
        assert_eq!(
            row_to_json(&row, &cols),
            json!({
                "D": "2023-11-14",
                "T": "12:34:56.123456789",
                "NTZ": "2023-11-14T22:13:20.000000000",
                "LTZ": "2023-11-14T22:13:20.5Z",
                "TZ": "2023-11-15T06:13:20.250+08:00",
                "TZW": "2023-11-14T21:13:20-01:00",
            })
        );
    }

    #[test]
    fn temporal_cells_before_the_epoch_floor_the_seconds() {
        let row = [json!("-1"), json!("-1.250"), json!("-86400"), json!("-0.5")];
        let cols = [
            col("D", "date"),
            col("A", "timestamp_ntz"),
            col("B", "timestamp_ntz"),
            col("C", "timestamp_ntz"),
        ];
        assert_eq!(
            row_to_json(&row, &cols),
            json!({
                "D": "1969-12-31",
                "A": "1969-12-31T23:59:58.750",
                "B": "1969-12-31T00:00:00",
                "C": "1969-12-31T23:59:59.5",
            })
        );
        assert_eq!(civil_from_days(-719_468), (0, 3, 1));
        assert_eq!(civil_from_days(11_016), (2000, 2, 29));
    }

    #[test]
    fn malformed_temporal_cells_pass_through() {
        for (raw, ty) in [
            ("x", "date"),
            ("1.2.3", "timestamp_ntz"),
            ("1.x", "time"),
            ("", "timestamp_ltz"),
            ("17 zz", "timestamp_tz"),
            ("17", "timestamp_tz"),
            ("1.1234567890123456789", "timestamp_ntz"),
        ] {
            let row = [json!(raw)];
            assert_eq!(
                row_to_json(&row, &[col("C", ty)]),
                json!({"C": raw}),
                "{ty} {raw}"
            );
        }
        assert_eq!(temporal_to_iso("1", "text"), None);
    }

    #[test]
    fn unknown_type_passes_through_as_string() {
        let row = [json!("0xDEADBEEF")];
        let cols = [col("BLOB", "binary")];
        assert_eq!(row_to_json(&row, &cols), json!({"BLOB": "0xDEADBEEF"}));
    }

    #[test]
    fn non_finite_real_falls_back_to_string() {
        let row = [json!("NaN")];
        let cols = [col("X", "real")];
        // NaN is not representable in JSON; round-trip-safe fallback.
        assert_eq!(row_to_json(&row, &cols), json!({"X": "NaN"}));
    }
}
