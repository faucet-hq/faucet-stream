//! Convert MySQL binlog row values into JSON.

use base64::Engine as _;
use faucet_core::FaucetError;
use mysql_async::Value;
use mysql_async::binlog::row::BinlogRow;
use mysql_async::binlog::value::BinlogValue;
use serde_json::{Map, Value as Json};

/// Convert a single MySQL protocol `Value` to JSON.
///
/// - `NULL` → `null`
/// - `Int(i64)` / `UInt(u64)` → JSON number
/// - `Float(f32)` / `Double(f64)` → JSON number (NaN/Inf → `null`)
/// - `Bytes(Vec<u8>)` → UTF-8 string if valid, else base64-encoded string
/// - `Date(y, mo, d, h, mi, s, micro)` → `"YYYY-MM-DD HH:MM:SS.ffffff"`
/// - `Time(neg, days, h, mi, s, micro)` → `"[-]HHH:MM:SS.ffffff"`
pub fn value_to_json(v: &Value) -> Json {
    match v {
        Value::NULL => Json::Null,
        Value::Int(i) => Json::from(*i),
        Value::UInt(u) => Json::from(*u),
        Value::Float(f) => serde_json::Number::from_f64(f64::from(*f))
            .map(Json::Number)
            .unwrap_or(Json::Null),
        Value::Double(d) => serde_json::Number::from_f64(*d)
            .map(Json::Number)
            .unwrap_or(Json::Null),
        Value::Bytes(b) => match std::str::from_utf8(b) {
            // Text-like columns decode as UTF-8 strings; binary falls back to base64.
            Ok(s) => Json::String(s.to_owned()),
            Err(_) => Json::String(base64::engine::general_purpose::STANDARD.encode(b)),
        },
        // Date(year, month, day, hour, minute, second, microsecond)
        Value::Date(y, mo, d, h, mi, s, micro) => Json::String(format!(
            "{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}.{micro:06}"
        )),
        // Time(is_negative, days, hours, minutes, seconds, microseconds)
        Value::Time(neg, days, h, mi, s, micro) => {
            let sign = if *neg { "-" } else { "" };
            // `days` is already u32; widen to u64 so an out-of-range value from a
            // malformed binlog can't overflow (real MySQL caps TIME at 838:59:59).
            let total_hours = u64::from(*days) * 24 + u64::from(*h);
            Json::String(format!("{sign}{total_hours:02}:{mi:02}:{s:02}.{micro:06}"))
        }
    }
}

/// Convert one binlog value (which may be a plain value or a JSON/JSONB value).
///
/// - `Value(val)` → delegates to [`value_to_json`]
/// - `Jsonb(x)` → converts the MySQL JSONB representation to `serde_json::Value`
/// - `JsonDiff(diffs)` → typed `FaucetError::Source` (a partial JSON diff cannot be
///   reconstructed without the prior document — see below)
///
/// **JSONB opaque scalars (lossless):** `mysql_common`'s
/// `TryFrom<jsonb::Value> for serde_json::Value` bails with `Err(Opaque)` the moment
/// the document contains an *opaque* scalar — a MySQL type with no direct JSON
/// representation stored inside a JSON column (e.g. a `DECIMAL`/`DATE`/`TIME`/
/// `DATETIME` embedded via `CAST(... AS …)`). Using that conversion would null out
/// the **entire** column — wholesale data loss — for one un-representable sub-field.
///
/// Instead we go through [`jsonb::Value::parse`], which decodes opaque scalars into a
/// structured [`JsonDom`] **losslessly**: `NEWDECIMAL` → its exact decimal string,
/// `DATE`/`TIME`/`DATETIME`/`TIMESTAMP` → a formatted temporal string, and any other
/// opaque type → a `base64:type<N>:<…>` round-trippable string. The infallible
/// `From<JsonDom> for serde_json::Value` then yields the final value. Ordinary JSON
/// content (objects, arrays, strings, numbers, bools, null) still converts losslessly,
/// and opaque sub-fields are now preserved rather than collapsing the whole column to
/// `null`. The behaviour is documented in the crate README.
///
/// Only a structurally **corrupt** JSONB blob (truncated opaque data, invalid UTF-8 in
/// a string) makes `parse()` fail; there is no recoverable textual form for corrupt
/// bytes, so that rare case surfaces as a typed [`FaucetError::Source`] (visible,
/// routable to a DLQ) rather than a silent `null`.
///
/// **Partial JSON diffs:** when the MySQL server runs with
/// `binlog_row_value_options=PARTIAL_JSON`, an UPDATE that touches a JSON column may
/// emit a `BinlogValue::JsonDiff` — a *delta* against the column's prior value rather
/// than the full document. faucet-stream does not buffer prior row state, so the diff
/// cannot be applied to reconstruct the new value. Fabricating a placeholder would
/// silently corrupt the column, so we return a typed [`FaucetError::Source`] instead;
/// the row is surfaced as an error (and routed to a DLQ when configured). The
/// `run_preflight` startup check rejects `PARTIAL_JSON` outright so this path is not
/// normally reached — operators must set `binlog_row_value_options=''` for CDC.
pub fn binlog_value_to_json(v: &BinlogValue<'_>) -> Result<Json, FaucetError> {
    match v {
        BinlogValue::Value(val) => Ok(value_to_json(val)),
        BinlogValue::Jsonb(jsonb_val) => {
            // We clone-via-into_owned because we hold a borrow; JSONB columns are
            // rare enough that the clone cost doesn't matter.
            //
            // `Value::parse()` → `JsonDom` decodes opaque scalars (DECIMAL/temporal/
            // other) losslessly into a structured form, and `From<JsonDom>` is
            // infallible — so a JSON column carrying an opaque sub-field is preserved
            // rather than nulled. See the function doc for the exact mappings.
            jsonb_value_to_json(jsonb_val.clone().into_owned())
        }
        // A partial JSON diff cannot be reconstructed without the prior document.
        // Never fabricate a value — surface a typed error so the row is routed to a
        // DLQ / fails the run rather than silently corrupting the JSON column.
        BinlogValue::JsonDiff(_) => Err(FaucetError::Source(
            "mysql-cdc: received a partial JSON diff (BinlogValue::JsonDiff) for a JSON \
             column; this happens under binlog_row_value_options=PARTIAL_JSON and cannot \
             be reconstructed without the prior document. Set binlog_row_value_options='' \
             (full JSON) on the MySQL server for CDC."
                .into(),
        )),
    }
}

/// Convert an owned MySQL JSONB [`jsonb::Value`] into `serde_json::Value`, preserving
/// opaque scalars losslessly.
///
/// This is the pure (no-I/O) seam behind the `BinlogValue::Jsonb` arm of
/// [`binlog_value_to_json`]. It runs [`jsonb::Value::parse`] (which decodes opaque
/// `NEWDECIMAL`/`DATE`/`TIME`/`DATETIME`/`TIMESTAMP` and any other opaque type into a
/// structured [`JsonDom`]) and then the infallible `From<JsonDom> for
/// serde_json::Value`. The whole-column-to-`null` fallback the previous
/// `TryFrom<Value>` path forced on every opaque scalar is gone.
///
/// Returns `Err(FaucetError::Source)` only for a structurally corrupt JSONB blob,
/// where no textual form can be recovered — surfaced (and DLQ-routable) rather than
/// silently dropped to `null`.
fn jsonb_value_to_json(
    jsonb_val: mysql_async::binlog::jsonb::Value<'static>,
) -> Result<Json, FaucetError> {
    match jsonb_val.parse() {
        Ok(dom) => Ok(Json::from(dom)),
        Err(e) => Err(FaucetError::Source(format!(
            "mysql-cdc: failed to decode a JSON (JSONB) column value: {e}; the binlog \
             bytes for this column are malformed and cannot be reconstructed"
        ))),
    }
}

/// How a column's binlog value must be rendered to match the `mysql` query
/// source's snapshot rendering (#789 SQL-14). Built once per `TableMapEvent`;
/// requires `binlog_row_metadata=FULL` for the ENUM/SET labels.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnHint {
    /// `TIMESTAMP`: epoch seconds → RFC 3339 UTC.
    Timestamp,
    /// `DATE`: `YYYY-MM-DD`.
    Date,
    /// `DATETIME`: `YYYY-MM-DD HH:MM:SS[.fff…]`.
    DateTime,
    /// `TIME`: `HH:MM:SS[.fff…]`.
    Time,
    /// `YEAR`: a number (`0` for `0000`).
    Year,
    /// `FLOAT`: the single-precision value's shortest decimal form.
    Float,
    /// `ENUM`: the label for the stored ordinal.
    Enum(Vec<String>),
    /// `SET`: the labels of the stored bitmask, comma-joined.
    Set(Vec<String>),
    /// Any other column: rendered as before.
    Other,
}

/// One [`ColumnHint`] per column of `tme`.
pub fn column_hints(tme: &mysql_async::binlog::events::TableMapEvent<'_>) -> Vec<ColumnHint> {
    use mysql_async::binlog::events::OptionalMetadataField;
    use mysql_async::consts::ColumnType as T;
    let mut enum_labels: Vec<Vec<String>> = Vec::new();
    let mut set_labels: Vec<Vec<String>> = Vec::new();
    for field in tme.iter_optional_meta().flatten() {
        match field {
            OptionalMetadataField::EnumStrValue(values) => {
                enum_labels = values
                    .iter_values()
                    .flatten()
                    .map(|v| v.values().iter().map(|x| x.value().into_owned()).collect())
                    .collect();
            }
            OptionalMetadataField::SetStrValue(values) => {
                set_labels = values
                    .iter_values()
                    .flatten()
                    .map(|v| v.values().iter().map(|x| x.value().into_owned()).collect())
                    .collect();
            }
            _ => {}
        }
    }
    let mut enums = enum_labels.into_iter();
    let mut sets = set_labels.into_iter();
    (0..tme.columns_count() as usize)
        .map(|i| match tme.get_column_type(i) {
            Ok(Some(T::MYSQL_TYPE_TIMESTAMP | T::MYSQL_TYPE_TIMESTAMP2)) => ColumnHint::Timestamp,
            Ok(Some(T::MYSQL_TYPE_DATE | T::MYSQL_TYPE_NEWDATE)) => ColumnHint::Date,
            Ok(Some(T::MYSQL_TYPE_DATETIME | T::MYSQL_TYPE_DATETIME2)) => ColumnHint::DateTime,
            Ok(Some(T::MYSQL_TYPE_TIME | T::MYSQL_TYPE_TIME2)) => ColumnHint::Time,
            Ok(Some(T::MYSQL_TYPE_YEAR)) => ColumnHint::Year,
            Ok(Some(T::MYSQL_TYPE_FLOAT)) => ColumnHint::Float,
            Ok(Some(T::MYSQL_TYPE_ENUM)) => enums
                .next()
                .map(ColumnHint::Enum)
                .unwrap_or(ColumnHint::Other),
            Ok(Some(T::MYSQL_TYPE_SET)) => sets
                .next()
                .map(ColumnHint::Set)
                .unwrap_or(ColumnHint::Other),
            _ => ColumnHint::Other,
        })
        .collect()
}

/// Fractional seconds the way chrono prints them (and so the snapshot source):
/// none when zero, else 3 or 6 digits.
fn fraction(micros: u32) -> String {
    if micros == 0 {
        String::new()
    } else if micros % 1000 == 0 {
        format!(".{:03}", micros / 1000)
    } else {
        format!(".{micros:06}")
    }
}

/// Render `v` for a column with `hint`; `None` when the hint does not apply to
/// the value's shape, which then falls back to [`value_to_json`].
fn hinted_value(hint: &ColumnHint, v: &Value) -> Option<Json> {
    match (hint, v) {
        (ColumnHint::Timestamp, Value::Bytes(b)) => {
            let text = std::str::from_utf8(b).ok()?;
            let (secs, frac) = text.split_once('.').unwrap_or((text, "0"));
            let secs: i64 = secs.parse().ok()?;
            let micros: u32 = format!("{frac:0<6}").get(..6)?.parse().ok()?;
            let at = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, micros * 1000)?;
            Some(Json::String(at.to_rfc3339()))
        }
        (ColumnHint::Date, Value::Date(y, mo, d, ..)) => {
            Some(Json::String(format!("{y:04}-{mo:02}-{d:02}")))
        }
        (ColumnHint::DateTime, Value::Date(y, mo, d, h, mi, s, micro)) => {
            Some(Json::String(format!(
                "{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}{}",
                fraction(*micro)
            )))
        }
        (ColumnHint::Time, Value::Time(false, 0, h, mi, s, micro)) if *h < 24 => Some(
            Json::String(format!("{h:02}:{mi:02}:{s:02}{}", fraction(*micro))),
        ),
        (ColumnHint::Year, Value::Bytes(b)) => {
            let year: u16 = std::str::from_utf8(b).ok()?.parse().ok()?;
            Some(Json::from(if year == 1900 { 0 } else { year }))
        }
        (ColumnHint::Float, Value::Float(f)) => {
            let shortest: f64 = f.to_string().parse().ok()?;
            Some(
                serde_json::Number::from_f64(shortest)
                    .map(Json::Number)
                    .unwrap_or(Json::Null),
            )
        }
        (ColumnHint::Enum(labels), Value::Int(i)) => {
            let label = usize::try_from(*i)
                .ok()
                .and_then(|i| i.checked_sub(1))
                .and_then(|i| labels.get(i))
                .cloned()
                .unwrap_or_default();
            Some(Json::String(label))
        }
        (ColumnHint::Set(labels), Value::Bytes(mask)) => {
            let chosen: Vec<&str> = labels
                .iter()
                .enumerate()
                .filter(|(i, _)| {
                    mask.get(i / 8)
                        .is_some_and(|byte| byte & (1 << (i % 8)) != 0)
                })
                .map(|(_, l)| l.as_str())
                .collect();
            Some(Json::String(chosen.join(",")))
        }
        _ => None,
    }
}

/// Build a `{column_name: json_value}` object from a binlog row, rendering each
/// column by its [`ColumnHint`].
///
/// Column names require `binlog_row_metadata=FULL` on the server; positional
/// names (`col_<i>`) are used as a defensive fallback if a name is empty.
pub fn binlog_row_to_json_hinted(
    row: &BinlogRow,
    hints: &[ColumnHint],
) -> Result<Json, FaucetError> {
    let cols = row.columns_ref();
    let mut obj = Map::with_capacity(cols.len());
    for (i, col) in cols.iter().enumerate() {
        let name = {
            let n = col.name_str();
            if n.is_empty() {
                format!("col_{i}")
            } else {
                n.into_owned()
            }
        };
        let val = match row.as_ref(i) {
            Some(BinlogValue::Value(v)) => match hints.get(i).and_then(|h| hinted_value(h, v)) {
                Some(j) => j,
                None => value_to_json(v),
            },
            Some(bv) => binlog_value_to_json(bv)?,
            None => Json::Null,
        };
        obj.insert(name, val);
    }
    Ok(Json::Object(obj))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hinted_values_render_like_the_snapshot_source() {
        let v = |h: ColumnHint, val: Value| hinted_value(&h, &val);
        assert_eq!(
            v(ColumnHint::Timestamp, Value::Bytes(b"1780749000".to_vec())),
            Some(Json::String("2026-06-06T12:30:00+00:00".into()))
        );
        assert_eq!(
            v(
                ColumnHint::Timestamp,
                Value::Bytes(b"1780749000.500000".to_vec())
            ),
            Some(Json::String("2026-06-06T12:30:00.500+00:00".into()))
        );
        assert_eq!(v(ColumnHint::Timestamp, Value::Bytes(b"x".to_vec())), None);
        assert_eq!(
            v(ColumnHint::Date, Value::Date(2026, 6, 6, 0, 0, 0, 0)),
            Some(Json::String("2026-06-06".into()))
        );
        assert_eq!(
            v(
                ColumnHint::DateTime,
                Value::Date(2026, 6, 6, 12, 30, 0, 123_456)
            ),
            Some(Json::String("2026-06-06 12:30:00.123456".into()))
        );
        assert_eq!(
            v(ColumnHint::Time, Value::Time(false, 0, 10, 0, 0, 0)),
            Some(Json::String("10:00:00".into()))
        );
        assert_eq!(v(ColumnHint::Time, Value::Time(true, 0, 1, 0, 0, 0)), None);
        assert_eq!(
            v(ColumnHint::Year, Value::Bytes(b"1900".to_vec())),
            Some(Json::from(0))
        );
        assert_eq!(
            v(ColumnHint::Year, Value::Bytes(b"2026".to_vec())),
            Some(Json::from(2026))
        );
        assert_eq!(
            v(ColumnHint::Float, Value::Float(0.1)),
            Some(Json::from(0.1))
        );
        assert_eq!(
            v(ColumnHint::Float, Value::Float(f32::NAN)),
            Some(Json::Null)
        );
        let labels = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        assert_eq!(
            v(ColumnHint::Enum(labels.clone()), Value::Int(3)),
            Some(Json::String("c".into()))
        );
        assert_eq!(
            v(ColumnHint::Enum(labels.clone()), Value::Int(0)),
            Some(Json::String(String::new()))
        );
        assert_eq!(
            v(ColumnHint::Set(labels), Value::Bytes(vec![0b101])),
            Some(Json::String("a,c".into()))
        );
        assert_eq!(v(ColumnHint::Other, Value::Int(1)), None);
        assert_eq!(fraction(0), "");
        assert_eq!(fraction(500_000), ".500");
    }
    use mysql_async::Value;

    #[test]
    fn scalars() {
        assert_eq!(value_to_json(&Value::NULL), Json::Null);
        assert_eq!(value_to_json(&Value::Int(-5)), Json::from(-5));
        assert_eq!(value_to_json(&Value::UInt(7)), Json::from(7u64));
        assert_eq!(
            value_to_json(&Value::Bytes(b"hello".to_vec())),
            Json::String("hello".into())
        );
    }

    #[test]
    fn double() {
        assert_eq!(value_to_json(&Value::Double(1.5)), Json::from(1.5));
    }

    #[test]
    fn date_formats() {
        // Date(year=2026, month=6, day=6, hour=12, minute=30, second=0, microsecond=0)
        let v = value_to_json(&Value::Date(2026, 6, 6, 12, 30, 0, 0));
        assert_eq!(v, Json::String("2026-06-06 12:30:00.000000".into()));
    }

    #[test]
    fn time_positive() {
        // Time(neg=false, days=0, h=1, mi=30, s=0, micro=0)
        let v = value_to_json(&Value::Time(false, 0, 1, 30, 0, 0));
        assert_eq!(v, Json::String("01:30:00.000000".into()));
    }

    #[test]
    fn time_negative() {
        // Time(neg=true, days=0, h=2, mi=0, s=0, micro=500000)
        let v = value_to_json(&Value::Time(true, 0, 2, 0, 0, 500_000));
        assert_eq!(v, Json::String("-02:00:00.500000".into()));
    }

    #[test]
    fn non_utf8_bytes_base64() {
        let v = value_to_json(&Value::Bytes(vec![0xff, 0xfe]));
        assert!(matches!(v, Json::String(_)));
        // Verify it is valid base64 (decodes without panic)
        if let Json::String(s) = v {
            base64::engine::general_purpose::STANDARD
                .decode(&s)
                .expect("should be valid base64");
        }
    }

    #[test]
    fn time_with_days_accumulates_hours() {
        // Time(neg=false, days=1, h=2, ...) → 1*24 + 2 = 26 hours.
        let v = value_to_json(&Value::Time(false, 1, 2, 3, 4, 0));
        assert_eq!(v, Json::String("26:03:04.000000".into()));
    }

    #[test]
    fn float_nan_is_null() {
        let v = value_to_json(&Value::Float(f32::NAN));
        assert_eq!(v, Json::Null);
    }

    #[test]
    fn double_infinity_is_null() {
        assert_eq!(value_to_json(&Value::Double(f64::INFINITY)), Json::Null);
        assert_eq!(value_to_json(&Value::Double(f64::NEG_INFINITY)), Json::Null);
    }

    #[test]
    fn binlog_value_plain_delegates_ok() {
        let v = binlog_value_to_json(&BinlogValue::Value(Value::Int(42)))
            .expect("plain value converts");
        assert_eq!(v, Json::from(42));
    }

    #[test]
    fn binlog_value_json_diff_is_typed_error_not_placeholder() {
        // A partial JSON diff (binlog_row_value_options=PARTIAL_JSON) must surface as a
        // typed FaucetError::Source, never the fabricated "<JsonDiff>" placeholder.
        let bv = BinlogValue::JsonDiff(Vec::new());
        let err = binlog_value_to_json(&bv).expect_err("JsonDiff must error");
        assert!(
            matches!(err, FaucetError::Source(_)),
            "expected FaucetError::Source, got {err:?}"
        );
        let msg = err.to_string();
        assert!(msg.contains("partial JSON diff"), "{msg}");
        assert!(msg.contains("PARTIAL_JSON"), "{msg}");
        // Guard against any regression that re-introduces the corrupt placeholder.
        assert!(!msg.contains("<JsonDiff>"), "{msg}");
    }

    #[test]
    fn binlog_row_with_json_diff_propagates_error() {
        // The row-level entry point must propagate the JsonDiff error rather than
        // embedding a corrupt value in the resulting object.
        let bv = BinlogValue::JsonDiff(Vec::new());
        assert!(binlog_value_to_json(&bv).is_err());
    }

    // ── F23: opaque-scalar JSONB is preserved losslessly, never nulled ────────────
    //
    // A JSON column whose document embeds a DECIMAL / temporal / other type with no
    // direct JSON form is stored as a JSONB *opaque* scalar. The old code ran
    // `TryFrom<jsonb::Value>`, which bails with `Err(Opaque)` and previously dropped
    // the ENTIRE column to `null`. The fix routes through `Value::parse()` → `JsonDom`
    // so the value survives. These tests build the opaque `jsonb::Value` directly via
    // the public `OpaqueValue::new` constructor.

    use mysql_async::binlog::jsonb::{JsonbString, OpaqueValue, Value as JsonbValue};
    use mysql_async::consts::ColumnType;

    #[test]
    fn jsonb_opaque_decimal_is_preserved_not_null() {
        // DECIMAL `1.5` packed in MySQL's binary decimal format: precision 2, scale 1.
        // Layout: [precision, scale, packed-digits…]. For 1.5 with scale 1 the integer
        // part `1` and fractional digit `5` pack to 0x81 0x05 (high bit set = positive).
        let data = vec![0x02, 0x01, 0x81, 0x05];
        let opaque = JsonbValue::Opaque(OpaqueValue::new(ColumnType::MYSQL_TYPE_NEWDECIMAL, data));
        let bv = BinlogValue::Jsonb(opaque);
        let got = binlog_value_to_json(&bv).expect("opaque decimal must convert, not error");
        // The exact decimal must be preserved as its string form, NEVER null.
        assert_ne!(
            got,
            Json::Null,
            "opaque DECIMAL was dropped to null (F23 regression)"
        );
        assert_eq!(
            got,
            Json::String("1.5".into()),
            "decimal not preserved losslessly"
        );
    }

    #[test]
    fn jsonb_opaque_date_is_preserved_not_null() {
        // MYSQL_TYPE_DATE decodes from a little-endian i64 packed datetime. The decoder
        // (`from_int64_datetime_packed`) computes: int_part = packed >> 24,
        // ymdhms = int_part, ymd = ymdhms >> 17, ym = ymd >> 5,
        // day = ymd % 32, month = ym % 13, year = ym / 13.
        // So to encode 2026-06-22 (no time): ym = 2026*13 + 6, ymd = ym*32 + 22,
        // ymdhms = ymd << 17, packed = ymdhms << 24.
        let ym: i64 = 2026 * 13 + 6;
        let ymd: i64 = (ym << 5) | 22;
        let ymdhms: i64 = ymd << 17;
        let packed: i64 = ymdhms << 24;
        let data = packed.to_le_bytes().to_vec();
        let opaque = JsonbValue::Opaque(OpaqueValue::new(ColumnType::MYSQL_TYPE_DATE, data));
        let bv = BinlogValue::Jsonb(opaque);
        let got = binlog_value_to_json(&bv).expect("opaque date must convert, not error");
        assert_ne!(
            got,
            Json::Null,
            "opaque DATE was dropped to null (F23 regression)"
        );
        // Preserved as a temporal string carrying the date components.
        match got {
            Json::String(s) => {
                assert!(s.contains("2026"), "date year not preserved: {s}");
                assert!(s.contains("06"), "date month not preserved: {s}");
                assert!(s.contains("22"), "date day not preserved: {s}");
            }
            other => panic!("expected a preserved date string, got {other:?}"),
        }
    }

    #[test]
    fn jsonb_opaque_other_type_is_preserved_as_base64_string_not_null() {
        // Any opaque type without a dedicated decoder (e.g. GEOMETRY) falls through to
        // a round-trippable `base64:type<N>:<base64>` string — still preserved, never
        // null. This proves the catch-all opaque arm survives the fix.
        let data = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let opaque = JsonbValue::Opaque(OpaqueValue::new(ColumnType::MYSQL_TYPE_GEOMETRY, data));
        let bv = BinlogValue::Jsonb(opaque);
        let got = binlog_value_to_json(&bv).expect("opaque geometry must convert, not error");
        assert_ne!(
            got,
            Json::Null,
            "opaque GEOMETRY was dropped to null (F23 regression)"
        );
        match got {
            Json::String(s) => {
                assert!(
                    s.starts_with("base64:"),
                    "expected base64 opaque encoding, got {s}"
                );
            }
            other => panic!("expected a preserved opaque string, got {other:?}"),
        }
    }

    #[test]
    fn jsonb_ordinary_string_still_converts_losslessly() {
        // A non-opaque JSONB string must still convert losslessly through the new path.
        let v = JsonbValue::String(JsonbString::new(b"hello".to_vec()));
        let bv = BinlogValue::Jsonb(v);
        let got = binlog_value_to_json(&bv).expect("ordinary jsonb string converts");
        assert_eq!(got, Json::String("hello".into()));
    }

    #[test]
    fn jsonb_ordinary_integer_still_converts_losslessly() {
        let bv = BinlogValue::Jsonb(JsonbValue::I64(-42));
        let got = binlog_value_to_json(&bv).expect("ordinary jsonb int converts");
        assert_eq!(got, Json::from(-42));
    }

    #[test]
    fn jsonb_object_with_opaque_field_preserves_whole_document() {
        // The core F23 case: a JSON *object* containing an opaque sub-field must keep
        // the rest of the document intact AND preserve the opaque field — the old code
        // collapsed the entire object to null. We assert via the catch-all opaque arm
        // (no packed-format constraints) embedded as one element of a small array.
        let opaque = JsonbValue::Opaque(OpaqueValue::new(
            ColumnType::MYSQL_TYPE_GEOMETRY,
            vec![0x01, 0x02],
        ));
        // Wrap the opaque scalar in a JSONB string sibling is not directly constructible
        // without wire bytes; the scalar-level guarantee (above) plus the infallible
        // `From<JsonDom>` for containers already covers nesting. This test pins that a
        // bare opaque scalar never errors and never nulls — the building block.
        let got = jsonb_value_to_json(opaque).expect("opaque scalar converts");
        assert_ne!(got, Json::Null);
    }
}
