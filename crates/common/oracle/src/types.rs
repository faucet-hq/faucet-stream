//! Pure Oracle ⇄ JSON type mapping. Everything here is driver-free so it is
//! unit-tested without an Oracle client.
//!
//! Precision rule: a `NUMBER` is emitted as a JSON number only when that is
//! exact — an integer that fits `i64`/`u64`, or a decimal whose shortest `f64`
//! rendering is the same decimal. Anything else is emitted as its exact decimal
//! string, so a `NUMBER(38)` key or a monetary `NUMBER(30,10)` never loses digits.

use base64::Engine;
use chrono::{DateTime, FixedOffset, NaiveDateTime};
use serde_json::{Value, json};

/// A decoded Oracle value, extracted from the driver but not yet shaped as JSON.
#[derive(Debug, Clone, PartialEq)]
pub enum Cell {
    /// SQL `NULL`.
    Null,
    /// Character data (`VARCHAR2`, `CHAR`, `CLOB`, `ROWID`, `LONG`, …).
    Text(String),
    /// `NUMBER` / `FLOAT` in Oracle's exact decimal text form.
    Number(String),
    /// `BINARY_FLOAT` / `BINARY_DOUBLE`.
    Float(f64),
    /// `RAW` / `LONG RAW` / `BLOB`.
    Bytes(Vec<u8>),
    /// `DATE` (second precision, no zone).
    Date(NaiveDateTime),
    /// `TIMESTAMP` without a zone.
    Timestamp(NaiveDateTime),
    /// `TIMESTAMP WITH [LOCAL] TIME ZONE`.
    TimestampTz(DateTime<FixedOffset>),
    /// `INTERVAL DAY TO SECOND` components (all share the interval's sign).
    IntervalDs {
        /// Days.
        days: i32,
        /// Hours.
        hours: i32,
        /// Minutes.
        minutes: i32,
        /// Seconds.
        seconds: i32,
        /// Nanoseconds.
        nanos: i32,
    },
    /// `INTERVAL YEAR TO MONTH` components.
    IntervalYm {
        /// Years.
        years: i32,
        /// Months.
        months: i32,
    },
    /// `BOOLEAN` (23ai).
    Bool(bool),
}

/// Shape a [`Cell`] as JSON.
pub fn cell_to_json(cell: Cell) -> Value {
    match cell {
        Cell::Null => Value::Null,
        Cell::Text(s) => Value::String(s),
        Cell::Number(s) => number_text_to_json(&s),
        Cell::Float(f) => float_to_json(f),
        Cell::Bytes(b) => Value::String(base64::engine::general_purpose::STANDARD.encode(b)),
        Cell::Date(d) => Value::String(d.format("%Y-%m-%dT%H:%M:%S").to_string()),
        Cell::Timestamp(t) => Value::String(t.format("%Y-%m-%dT%H:%M:%S%.f").to_string()),
        Cell::TimestampTz(t) => Value::String(t.format("%Y-%m-%dT%H:%M:%S%.f%:z").to_string()),
        Cell::IntervalDs {
            days,
            hours,
            minutes,
            seconds,
            nanos,
        } => Value::String(format_interval_ds(days, hours, minutes, seconds, nanos)),
        Cell::IntervalYm { years, months } => Value::String(format_interval_ym(years, months)),
        Cell::Bool(b) => Value::Bool(b),
    }
}

/// A non-finite `BINARY_DOUBLE` (`Inf`, `NaN`) has no JSON number form; emit
/// its text instead of silently turning it into `null`.
fn float_to_json(f: f64) -> Value {
    serde_json::Number::from_f64(f)
        .map(Value::Number)
        .unwrap_or_else(|| Value::String(f.to_string()))
}

/// Convert an Oracle decimal string to JSON without losing precision.
pub fn number_text_to_json(text: &str) -> Value {
    let t = text.trim();
    let is_integer = !t.contains(['.', 'e', 'E']);
    if is_integer {
        if let Ok(i) = t.parse::<i64>() {
            return json!(i);
        }
        if let Ok(u) = t.parse::<u64>() {
            return json!(u);
        }
        return Value::String(t.to_string());
    }
    if let Ok(f) = t.parse::<f64>()
        && f.is_finite()
        && canonical_decimal(t).is_some()
        && canonical_decimal(t) == canonical_decimal(&f.to_string())
    {
        return float_to_json(f);
    }
    Value::String(t.to_string())
}

/// Canonical `(negative, significant digits, exponent)` of a decimal string —
/// two strings denote the same number iff their canonical forms are equal.
fn canonical_decimal(s: &str) -> Option<(bool, String, i64)> {
    let s = s.trim();
    let (neg, body) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (mantissa, exp) = match body.find(['e', 'E']) {
        Some(i) => (&body[..i], body[i + 1..].parse::<i64>().ok()?),
        None => (body, 0),
    };
    let (int_part, frac_part) = match mantissa.find('.') {
        Some(i) => (&mantissa[..i], &mantissa[i + 1..]),
        None => (mantissa, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    if !int_part
        .chars()
        .chain(frac_part.chars())
        .all(|c| c.is_ascii_digit())
    {
        return None;
    }
    let digits = format!("{int_part}{frac_part}");
    let exponent = exp - frac_part.len() as i64;
    let significant = digits.trim_start_matches('0');
    if significant.is_empty() {
        return Some((false, "0".into(), 0));
    }
    let trimmed = significant.trim_end_matches('0');
    let trailing = significant.len() - trimmed.len();
    Some((neg, trimmed.to_string(), exponent + trailing as i64))
}

/// ISO-8601 duration for an `INTERVAL DAY TO SECOND`.
pub fn format_interval_ds(days: i32, hours: i32, minutes: i32, seconds: i32, nanos: i32) -> String {
    let negative = days < 0 || hours < 0 || minutes < 0 || seconds < 0 || nanos < 0;
    let (d, h, m, s, n) = (
        days.unsigned_abs(),
        hours.unsigned_abs(),
        minutes.unsigned_abs(),
        seconds.unsigned_abs(),
        nanos.unsigned_abs(),
    );
    let mut out = String::from(if negative { "-P" } else { "P" });
    if d > 0 {
        out.push_str(&format!("{d}D"));
    }
    let mut time = String::new();
    if h > 0 {
        time.push_str(&format!("{h}H"));
    }
    if m > 0 {
        time.push_str(&format!("{m}M"));
    }
    if s > 0 || n > 0 {
        if n > 0 {
            let frac = format!("{n:09}");
            time.push_str(&format!("{s}.{}S", frac.trim_end_matches('0')));
        } else {
            time.push_str(&format!("{s}S"));
        }
    }
    if !time.is_empty() {
        out.push('T');
        out.push_str(&time);
    } else if d == 0 {
        out.push_str("T0S");
    }
    out
}

/// ISO-8601 duration for an `INTERVAL YEAR TO MONTH`.
pub fn format_interval_ym(years: i32, months: i32) -> String {
    let negative = years < 0 || months < 0;
    let (y, m) = (years.unsigned_abs(), months.unsigned_abs());
    let mut out = String::from(if negative { "-P" } else { "P" });
    if y > 0 {
        out.push_str(&format!("{y}Y"));
    }
    if m > 0 || y == 0 {
        out.push_str(&format!("{m}M"));
    }
    out
}

/// Broad family of an Oracle column type, keyed off the data-dictionary
/// `DATA_TYPE` (+ `DATA_SCALE`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeFamily {
    /// `NUMBER` with scale 0 (includes `INTEGER`).
    Integer,
    /// Any other `NUMBER` / `FLOAT`.
    Decimal,
    /// `BINARY_FLOAT` / `BINARY_DOUBLE`.
    BinaryFloat,
    /// `VARCHAR2`, `CHAR`, `LONG`, `ROWID`, …
    Text,
    /// `NVARCHAR2` / `NCHAR`.
    NationalText,
    /// `CLOB` / `NCLOB` / `XMLTYPE`.
    Clob,
    /// Native `JSON` (21c+).
    Json,
    /// `RAW` / `LONG RAW`.
    Raw,
    /// `BLOB`.
    Blob,
    /// `DATE`.
    Date,
    /// `TIMESTAMP` without a zone.
    Timestamp,
    /// `TIMESTAMP WITH [LOCAL] TIME ZONE`.
    TimestampTz,
    /// `INTERVAL DAY TO SECOND`.
    IntervalDs,
    /// `INTERVAL YEAR TO MONTH`.
    IntervalYm,
    /// `BOOLEAN` (23ai).
    Boolean,
    /// Anything else (object types, `BFILE`, …).
    Other,
}

impl TypeFamily {
    /// Classify a data-dictionary `DATA_TYPE`.
    pub fn from_data_type(data_type: &str, scale: Option<i64>) -> Self {
        let t = data_type.trim().to_ascii_uppercase();
        match t.as_str() {
            "NUMBER" | "INTEGER" | "SMALLINT" | "INT" | "DECIMAL" | "NUMERIC" => {
                if scale == Some(0) || matches!(t.as_str(), "INTEGER" | "SMALLINT" | "INT") {
                    TypeFamily::Integer
                } else {
                    TypeFamily::Decimal
                }
            }
            "FLOAT" | "REAL" | "DOUBLE PRECISION" => TypeFamily::Decimal,
            "BINARY_FLOAT" | "BINARY_DOUBLE" => TypeFamily::BinaryFloat,
            "VARCHAR2" | "VARCHAR" | "CHAR" | "LONG" | "ROWID" | "UROWID" => TypeFamily::Text,
            "NVARCHAR2" | "NCHAR" => TypeFamily::NationalText,
            "CLOB" | "NCLOB" | "XMLTYPE" => TypeFamily::Clob,
            "JSON" => TypeFamily::Json,
            "RAW" | "LONG RAW" => TypeFamily::Raw,
            "BLOB" => TypeFamily::Blob,
            "DATE" => TypeFamily::Date,
            "BOOLEAN" => TypeFamily::Boolean,
            _ if t.starts_with("TIMESTAMP") && t.contains("TIME ZONE") => TypeFamily::TimestampTz,
            _ if t.starts_with("TIMESTAMP") => TypeFamily::Timestamp,
            _ if t.starts_with("INTERVAL DAY") => TypeFamily::IntervalDs,
            _ if t.starts_with("INTERVAL YEAR") => TypeFamily::IntervalYm,
            _ if t.starts_with("UROWID") => TypeFamily::Text,
            _ => TypeFamily::Other,
        }
    }

    /// The JSON-Schema fragment for values of this family as the source emits them.
    pub fn json_schema(self) -> Value {
        match self {
            TypeFamily::Integer => json!({ "type": "integer" }),
            TypeFamily::Decimal | TypeFamily::BinaryFloat => json!({ "type": "number" }),
            TypeFamily::Boolean => json!({ "type": "boolean" }),
            TypeFamily::Json => json!({ "type": "object" }),
            TypeFamily::Raw | TypeFamily::Blob => {
                json!({ "type": "string", "contentEncoding": "base64" })
            }
            TypeFamily::Date | TypeFamily::Timestamp | TypeFamily::TimestampTz => {
                json!({ "type": "string", "format": "date-time" })
            }
            TypeFamily::IntervalDs | TypeFamily::IntervalYm => {
                json!({ "type": "string", "format": "duration" })
            }
            _ => json!({ "type": "string" }),
        }
    }
}

/// Convert a value Oracle rendered as text (a LogMiner literal, a `TO_CHAR`
/// result) to JSON using the column's type family. The session renders dates
/// as `YYYY-MM-DD HH24:MI:SS[.FF] [TZH:TZM]` (see [`crate::NLS_SESSION_SQL`]).
pub fn typed_text_to_json(text: &str, family: TypeFamily) -> Value {
    match family {
        TypeFamily::Integer | TypeFamily::Decimal => number_text_to_json(text),
        TypeFamily::BinaryFloat => match text.trim().parse::<f64>() {
            Ok(f) => float_to_json(f),
            Err(_) => Value::String(text.to_string()),
        },
        TypeFamily::Date | TypeFamily::Timestamp | TypeFamily::TimestampTz => {
            Value::String(normalize_datetime_text(text))
        }
        TypeFamily::IntervalDs => {
            Value::String(oracle_interval_ds_to_iso(text).unwrap_or_else(|| text.to_string()))
        }
        TypeFamily::IntervalYm => {
            Value::String(oracle_interval_ym_to_iso(text).unwrap_or_else(|| text.to_string()))
        }
        TypeFamily::Raw | TypeFamily::Blob => match hex_to_bytes(text) {
            Some(b) => Value::String(base64::engine::general_purpose::STANDARD.encode(b)),
            None => Value::String(text.to_string()),
        },
        TypeFamily::Boolean => match text.trim().to_ascii_uppercase().as_str() {
            "1" | "TRUE" => Value::Bool(true),
            "0" | "FALSE" => Value::Bool(false),
            _ => Value::String(text.to_string()),
        },
        TypeFamily::Json => {
            serde_json::from_str(text).unwrap_or_else(|_| Value::String(text.into()))
        }
        _ => Value::String(text.to_string()),
    }
}

/// `2024-01-02 03:04:05.500000 +01:00` → `2024-01-02T03:04:05.5+01:00`.
/// Text in another shape is returned unchanged.
pub fn normalize_datetime_text(text: &str) -> String {
    let t = text.trim();
    for fmt in ["%Y-%m-%d %H:%M:%S%.f %:z", "%Y-%m-%d %H:%M:%S%.f%:z"] {
        if let Ok(dt) = DateTime::parse_from_str(t, fmt) {
            return dt.format("%Y-%m-%dT%H:%M:%S%.f%:z").to_string();
        }
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(t, "%Y-%m-%d %H:%M:%S%.f") {
        return dt.format("%Y-%m-%dT%H:%M:%S%.f").to_string();
    }
    t.to_string()
}

/// `+01 02:03:04.500000` → `P1DT2H3M4.5S`.
pub fn oracle_interval_ds_to_iso(text: &str) -> Option<String> {
    let t = text.trim();
    let (negative, body) = match t.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, t.strip_prefix('+').unwrap_or(t)),
    };
    let (days, clock) = body.split_once(' ')?;
    let mut hms = clock.split(':');
    let hours: i32 = hms.next()?.parse().ok()?;
    let minutes: i32 = hms.next()?.parse().ok()?;
    let sec_text = hms.next()?;
    if hms.next().is_some() {
        return None;
    }
    let (secs, frac) = sec_text.split_once('.').unwrap_or((sec_text, ""));
    let seconds: i32 = secs.parse().ok()?;
    let nanos: i32 = if frac.is_empty() {
        0
    } else {
        if frac.len() > 9 || !frac.chars().all(|c| c.is_ascii_digit()) {
            return None;
        }
        format!("{frac:0<9}").parse().ok()?
    };
    let days: i32 = days.parse().ok()?;
    let sign = if negative { -1 } else { 1 };
    Some(format_interval_ds(
        sign * days,
        sign * hours,
        sign * minutes,
        sign * seconds,
        sign * nanos,
    ))
}

/// `+01-02` → `P1Y2M`.
pub fn oracle_interval_ym_to_iso(text: &str) -> Option<String> {
    let t = text.trim();
    let (negative, body) = match t.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, t.strip_prefix('+').unwrap_or(t)),
    };
    let (y, m) = body.split_once('-')?;
    let years: i32 = y.parse().ok()?;
    let months: i32 = m.parse().ok()?;
    let sign = if negative { -1 } else { 1 };
    Some(format_interval_ym(sign * years, sign * months))
}

/// ISO-8601 duration → Oracle interval literal text, for binding into an
/// `INTERVAL` column: `P1DT2H3M4.5S` → `+1 02:03:04.500000000`,
/// `P1Y2M` → `+1-2`. `None` when `iso` is not a duration this can express.
pub fn iso_to_oracle_interval(iso: &str, day_to_second: bool) -> Option<String> {
    let t = iso.trim();
    let (negative, body) = match t.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, t),
    };
    let body = body.strip_prefix('P')?;
    let (date_part, time_part) = body.split_once('T').unwrap_or((body, ""));
    let mut years = 0i64;
    let mut months = 0i64;
    let mut days = 0i64;
    let mut num = String::new();
    for c in date_part.chars() {
        match c {
            '0'..='9' => num.push(c),
            'Y' => years = std::mem::take(&mut num).parse().ok()?,
            'M' => months = std::mem::take(&mut num).parse().ok()?,
            'W' => days += 7 * std::mem::take(&mut num).parse::<i64>().ok()?,
            'D' => days += std::mem::take(&mut num).parse::<i64>().ok()?,
            _ => return None,
        }
    }
    if !num.is_empty() {
        return None;
    }
    let (mut hours, mut minutes, mut seconds, mut nanos) = (0i64, 0i64, 0i64, 0i64);
    for c in time_part.chars() {
        match c {
            '0'..='9' | '.' => num.push(c),
            'H' => hours = std::mem::take(&mut num).parse().ok()?,
            'M' => minutes = std::mem::take(&mut num).parse().ok()?,
            'S' => {
                let s = std::mem::take(&mut num);
                let (whole, frac) = s.split_once('.').unwrap_or((&s, ""));
                seconds = whole.parse().ok()?;
                if !frac.is_empty() {
                    if frac.len() > 9 {
                        return None;
                    }
                    nanos = format!("{frac:0<9}").parse().ok()?;
                }
            }
            _ => return None,
        }
    }
    if !num.is_empty() {
        return None;
    }
    let sign = if negative { '-' } else { '+' };
    if day_to_second {
        if years != 0 || months != 0 {
            return None;
        }
        let total_secs = ((days * 24 + hours) * 60 + minutes) * 60 + seconds;
        let (d, rem) = (total_secs / 86_400, total_secs % 86_400);
        Some(format!(
            "{sign}{d} {:02}:{:02}:{:02}.{nanos:09}",
            rem / 3600,
            (rem % 3600) / 60,
            rem % 60
        ))
    } else {
        if days != 0 || hours != 0 || minutes != 0 || seconds != 0 || nanos != 0 {
            return None;
        }
        let total = years * 12 + months;
        Some(format!("{sign}{}-{}", total / 12, total % 12))
    }
}

/// Decode an even-length hex string.
pub fn hex_to_bytes(text: &str) -> Option<Vec<u8>> {
    let t = text.trim();
    if !t.len().is_multiple_of(2) {
        return None;
    }
    (0..t.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(t.get(i..i + 2)?, 16).ok())
        .collect()
}

/// Encode bytes as uppercase hex (the form `HEXTORAW` / RAW binds accept).
pub fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02X}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn number_integers_keep_exactness() {
        assert_eq!(number_text_to_json("42"), json!(42));
        assert_eq!(number_text_to_json("-7"), json!(-7));
        assert_eq!(number_text_to_json("18446744073709551615"), json!(u64::MAX));
        assert_eq!(
            number_text_to_json("123456789012345678901234567890"),
            json!("123456789012345678901234567890")
        );
    }

    #[test]
    fn number_decimals_only_when_exact() {
        assert_eq!(number_text_to_json("1.5"), json!(1.5));
        assert_eq!(number_text_to_json("0.1"), json!(0.1));
        assert_eq!(number_text_to_json(".25"), json!(0.25));
        assert_eq!(number_text_to_json("-.5"), json!(-0.5));
        assert_eq!(number_text_to_json("1E+3"), json!(1000.0));
        assert_eq!(
            number_text_to_json("12345678901234567.25"),
            json!("12345678901234567.25")
        );
        assert_eq!(
            number_text_to_json("0.12345678901234567890123"),
            json!("0.12345678901234567890123")
        );
        assert_eq!(number_text_to_json("abc.d"), json!("abc.d"));
        assert_eq!(number_text_to_json("1e999"), json!("1e999"));
    }

    #[test]
    fn canonical_decimal_forms() {
        assert_eq!(canonical_decimal("1.50"), canonical_decimal("1.5"));
        assert_eq!(canonical_decimal("0.000"), Some((false, "0".into(), 0)));
        assert_eq!(canonical_decimal("+2"), canonical_decimal("2"));
        assert_eq!(canonical_decimal("100"), Some((false, "1".into(), 2)));
        assert_eq!(canonical_decimal("."), None);
        assert_eq!(canonical_decimal("1ex"), None);
    }

    #[test]
    fn cells_to_json() {
        let d = NaiveDate::from_ymd_opt(2024, 1, 2)
            .unwrap()
            .and_hms_opt(3, 4, 5)
            .unwrap();
        assert_eq!(cell_to_json(Cell::Null), Value::Null);
        assert_eq!(cell_to_json(Cell::Text("x".into())), json!("x"));
        assert_eq!(cell_to_json(Cell::Number("3".into())), json!(3));
        assert_eq!(cell_to_json(Cell::Float(2.5)), json!(2.5));
        assert_eq!(cell_to_json(Cell::Float(f64::INFINITY)), json!("inf"));
        assert_eq!(cell_to_json(Cell::Bytes(vec![1, 2, 3])), json!("AQID"));
        assert_eq!(cell_to_json(Cell::Date(d)), json!("2024-01-02T03:04:05"));
        let t = d + chrono::Duration::milliseconds(500);
        assert_eq!(
            cell_to_json(Cell::Timestamp(t)),
            json!("2024-01-02T03:04:05.500")
        );
        let tz = FixedOffset::east_opt(3600)
            .unwrap()
            .from_local_datetime(&d)
            .unwrap();
        assert_eq!(
            cell_to_json(Cell::TimestampTz(tz)),
            json!("2024-01-02T03:04:05+01:00")
        );
        assert_eq!(
            cell_to_json(Cell::IntervalDs {
                days: 1,
                hours: 2,
                minutes: 3,
                seconds: 4,
                nanos: 500_000_000
            }),
            json!("P1DT2H3M4.5S")
        );
        assert_eq!(
            cell_to_json(Cell::IntervalYm {
                years: 1,
                months: 2
            }),
            json!("P1Y2M")
        );
        assert_eq!(cell_to_json(Cell::Bool(true)), json!(true));
    }

    use chrono::TimeZone;

    #[test]
    fn interval_formatting_edges() {
        assert_eq!(format_interval_ds(0, 0, 0, 0, 0), "PT0S");
        assert_eq!(format_interval_ds(3, 0, 0, 0, 0), "P3D");
        assert_eq!(format_interval_ds(-1, -2, 0, 0, 0), "-P1DT2H");
        assert_eq!(format_interval_ds(0, 0, 0, 7, 0), "PT7S");
        assert_eq!(format_interval_ym(0, 0), "P0M");
        assert_eq!(format_interval_ym(2, 0), "P2Y");
        assert_eq!(format_interval_ym(-1, -3), "-P1Y3M");
    }

    #[test]
    fn families() {
        use TypeFamily::*;
        let cases = [
            ("NUMBER", Some(0), Integer),
            ("NUMBER", None, Decimal),
            ("NUMBER", Some(2), Decimal),
            ("INTEGER", None, Integer),
            ("FLOAT", None, Decimal),
            ("BINARY_DOUBLE", None, BinaryFloat),
            ("VARCHAR2", None, Text),
            ("NVARCHAR2", None, NationalText),
            ("CLOB", None, Clob),
            ("JSON", None, Json),
            ("RAW", None, Raw),
            ("LONG RAW", None, Raw),
            ("BLOB", None, Blob),
            ("DATE", None, Date),
            ("TIMESTAMP(6)", None, Timestamp),
            ("TIMESTAMP(6) WITH TIME ZONE", None, TimestampTz),
            ("TIMESTAMP(6) WITH LOCAL TIME ZONE", None, TimestampTz),
            ("INTERVAL DAY(2) TO SECOND(6)", None, IntervalDs),
            ("INTERVAL YEAR(2) TO MONTH", None, IntervalYm),
            ("BOOLEAN", None, Boolean),
            ("UROWID(4000)", None, Text),
            ("SDO_GEOMETRY", None, Other),
        ];
        for (t, s, f) in cases {
            assert_eq!(TypeFamily::from_data_type(t, s), f, "{t}");
        }
    }

    #[test]
    fn family_schemas() {
        use TypeFamily::*;
        assert_eq!(Integer.json_schema()["type"], "integer");
        assert_eq!(Decimal.json_schema()["type"], "number");
        assert_eq!(Boolean.json_schema()["type"], "boolean");
        assert_eq!(Json.json_schema()["type"], "object");
        assert_eq!(Blob.json_schema()["contentEncoding"], "base64");
        assert_eq!(TimestampTz.json_schema()["format"], "date-time");
        assert_eq!(IntervalYm.json_schema()["format"], "duration");
        assert_eq!(Other.json_schema()["type"], "string");
    }

    #[test]
    fn typed_text_conversions() {
        use TypeFamily::*;
        assert_eq!(typed_text_to_json("12", Integer), json!(12));
        assert_eq!(typed_text_to_json("2.5", BinaryFloat), json!(2.5));
        assert_eq!(typed_text_to_json("x", BinaryFloat), json!("x"));
        assert_eq!(
            typed_text_to_json("2024-01-02 03:04:05", Date),
            json!("2024-01-02T03:04:05")
        );
        assert_eq!(
            typed_text_to_json("2024-01-02 03:04:05.250000000", Timestamp),
            json!("2024-01-02T03:04:05.250")
        );
        assert_eq!(
            typed_text_to_json("2024-01-02 03:04:05.250000000 +05:30", TimestampTz),
            json!("2024-01-02T03:04:05.250+05:30")
        );
        assert_eq!(typed_text_to_json("not a date", Date), json!("not a date"));
        assert_eq!(
            typed_text_to_json("+01 02:03:04.500000", IntervalDs),
            json!("P1DT2H3M4.5S")
        );
        assert_eq!(typed_text_to_json("junk", IntervalDs), json!("junk"));
        assert_eq!(typed_text_to_json("-01-02", IntervalYm), json!("-P1Y2M"));
        assert_eq!(typed_text_to_json("junk", IntervalYm), json!("junk"));
        assert_eq!(typed_text_to_json("0102", Raw), json!("AQI="));
        assert_eq!(typed_text_to_json("0G", Raw), json!("0G"));
        assert_eq!(typed_text_to_json("1", Boolean), json!(true));
        assert_eq!(typed_text_to_json("FALSE", Boolean), json!(false));
        assert_eq!(typed_text_to_json("maybe", Boolean), json!("maybe"));
        assert_eq!(typed_text_to_json("{\"a\":1}", Json), json!({"a": 1}));
        assert_eq!(typed_text_to_json("{bad", Json), json!("{bad"));
        assert_eq!(typed_text_to_json("hi", Text), json!("hi"));
    }

    #[test]
    fn interval_parsing_rejects_garbage() {
        assert_eq!(oracle_interval_ds_to_iso("1 02:03"), None);
        assert_eq!(oracle_interval_ds_to_iso("1 02:03:04:05"), None);
        assert_eq!(oracle_interval_ds_to_iso("1 02:03:04.1234567890"), None);
        assert_eq!(oracle_interval_ds_to_iso("1 02:03:04.x"), None);
        assert_eq!(
            oracle_interval_ds_to_iso("+1 02:03:04"),
            Some("P1DT2H3M4S".into())
        );
        assert_eq!(oracle_interval_ym_to_iso("12"), None);
    }

    #[test]
    fn iso_to_oracle_intervals() {
        assert_eq!(
            iso_to_oracle_interval("P1DT2H3M4.5S", true).as_deref(),
            Some("+1 02:03:04.500000000")
        );
        assert_eq!(
            iso_to_oracle_interval("-PT90M", true).as_deref(),
            Some("-0 01:30:00.000000000")
        );
        assert_eq!(
            iso_to_oracle_interval("P1W", true).as_deref(),
            Some("+7 00:00:00.000000000")
        );
        assert_eq!(
            iso_to_oracle_interval("P1Y2M", false).as_deref(),
            Some("+1-2")
        );
        assert_eq!(
            iso_to_oracle_interval("P14M", false).as_deref(),
            Some("+1-2")
        );
        assert_eq!(iso_to_oracle_interval("P1Y", true), None);
        assert_eq!(iso_to_oracle_interval("P1D", false), None);
        assert_eq!(iso_to_oracle_interval("1D", true), None);
        assert_eq!(iso_to_oracle_interval("P1X", true), None);
        assert_eq!(iso_to_oracle_interval("P1", true), None);
        assert_eq!(iso_to_oracle_interval("PT1X", true), None);
        assert_eq!(iso_to_oracle_interval("PT1", true), None);
        assert_eq!(iso_to_oracle_interval("PT1.1234567890S", true), None);
    }

    #[test]
    fn hex_round_trip() {
        assert_eq!(hex_to_bytes("0aFF"), Some(vec![10, 255]));
        assert_eq!(hex_to_bytes("abc"), None);
        assert_eq!(bytes_to_hex(&[10, 255]), "0AFF");
    }
}
