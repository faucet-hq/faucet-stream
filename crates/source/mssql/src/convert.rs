//! Decode a `tiberius` [`Row`] into a `serde_json` object.
//!
//! Non-temporal columns are decoded directly from their [`ColumnData`] variant
//! (the variant carries the exact width, so there's no integer-size guessing).
//! Temporal columns are decoded through [`Row::try_get`] with `chrono` target
//! types so the conversion uses `tiberius`' own (correct) epoch math rather than
//! a re-implementation here.

use base64::Engine;
use chrono::{FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use faucet_core::FaucetError;
use serde_json::{Map, Value, json};
use tiberius::numeric::Numeric;
use tiberius::{ColumnData, Row};

/// Decode every column of `row` into a JSON object keyed by column name.
pub fn row_to_json(row: &Row) -> Result<Value, FaucetError> {
    let mut map = Map::with_capacity(row.columns().len());
    // Collect names up front (cells borrows the row immutably for the iterator;
    // we also call try_get — another shared borrow — for temporal columns).
    let names: Vec<String> = row.columns().iter().map(|c| c.name().to_string()).collect();
    for (i, (_col, data)) in row.cells().enumerate() {
        let value = match scalar_to_json(data) {
            Some(v) => v,
            None => decode_temporal(row, i, data)?,
        };
        map.insert(names[i].clone(), value);
    }
    Ok(Value::Object(map))
}

/// Convert a non-temporal [`ColumnData`] to JSON.
///
/// Returns `Some(Value::Null)` for a SQL NULL in a non-temporal column, and
/// `None` for temporal variants — the caller decodes those via
/// [`decode_temporal`] (which needs the [`Row`] for `try_get`).
fn scalar_to_json(data: &ColumnData<'_>) -> Option<Value> {
    let v = match data {
        ColumnData::U8(o) => o.map(|n| json!(n)),
        ColumnData::I16(o) => o.map(|n| json!(n)),
        ColumnData::I32(o) => o.map(|n| json!(n)),
        ColumnData::I64(o) => o.map(|n| json!(n)),
        ColumnData::F32(o) => o.map(|f| json!(f)),
        ColumnData::F64(o) => o.map(|f| json!(f)),
        ColumnData::Bit(o) => o.map(|b| json!(b)),
        ColumnData::String(o) => o.as_ref().map(|s| json!(s.as_ref())),
        ColumnData::Guid(o) => o.map(|g| json!(g.to_string())),
        ColumnData::Binary(o) => o
            .as_ref()
            .map(|b| json!(base64::engine::general_purpose::STANDARD.encode(b.as_ref()))),
        ColumnData::Numeric(o) => o.map(|n| json!(numeric_to_string(n))),
        ColumnData::Xml(o) => o.as_ref().map(|x| json!(x.as_ref().clone().into_string())),
        // Temporal variants are decoded by the caller via try_get.
        ColumnData::DateTime(_)
        | ColumnData::SmallDateTime(_)
        | ColumnData::Date(_)
        | ColumnData::Time(_)
        | ColumnData::DateTime2(_)
        | ColumnData::DateTimeOffset(_) => return None,
    };
    // A present non-temporal column yields its value; a NULL yields JSON null.
    Some(v.unwrap_or(Value::Null))
}

fn decode_temporal(row: &Row, idx: usize, data: &ColumnData<'_>) -> Result<Value, FaucetError> {
    let conv = |e: tiberius::error::Error| {
        FaucetError::Source(format!("MSSQL column {idx} temporal decode failed: {e}"))
    };
    let value = match data {
        ColumnData::Date(_) => row
            .try_get::<NaiveDate, _>(idx)
            .map_err(conv)?
            .map(|d| json!(d.to_string())),
        ColumnData::Time(_) => row
            .try_get::<NaiveTime, _>(idx)
            .map_err(conv)?
            .map(|t| json!(t.to_string())),
        ColumnData::DateTime(_) | ColumnData::SmallDateTime(_) => row
            .try_get::<NaiveDateTime, _>(idx)
            .map_err(conv)?
            .map(|dt| json!(format_legacy_datetime(dt))),
        ColumnData::DateTime2(_) => row
            .try_get::<NaiveDateTime, _>(idx)
            .map_err(conv)?
            .map(|dt| json!(dt.format("%Y-%m-%dT%H:%M:%S%.f").to_string())),
        ColumnData::DateTimeOffset(_) => row
            .try_get::<chrono::DateTime<FixedOffset>, _>(idx)
            .map_err(conv)?
            .map(|dt| json!(dt.to_rfc3339())),
        _ => unreachable!("decode_temporal called on a non-temporal column"),
    };
    Ok(value.unwrap_or(Value::Null))
}

/// Render a `DATETIME` / `SMALLDATETIME` value with millisecond precision, as
/// SQL Server itself displays it.
///
/// `DATETIME` stores 1/300-second ticks, which decode to nine-digit fractions
/// such as `.003333333`; SQL Server refuses to convert that string back to
/// `DATETIME`, so a bookmark rendered with `%.f` failed every run after the
/// first (SQL-40). The fraction is rounded to the nearest millisecond — never
/// past `.997`, the last tick of a second.
pub(crate) fn format_legacy_datetime(dt: NaiveDateTime) -> String {
    let ms = ((dt.and_utc().timestamp_subsec_nanos() + 500_000) / 1_000_000).min(999);
    format!("{}.{ms:03}", dt.format("%Y-%m-%dT%H:%M:%S"))
}

/// Format an MSSQL DECIMAL/NUMERIC value as a precision-preserving string
/// (JSON numbers can't represent arbitrary-precision decimals — mirrors the
/// postgres source's NUMERIC-as-string behaviour).
pub(crate) fn numeric_to_string(n: Numeric) -> String {
    let scale = n.scale() as usize;
    let mantissa = n.value();
    if scale == 0 {
        return mantissa.to_string();
    }
    let negative = mantissa < 0;
    // unsigned_abs avoids i128::MIN overflow.
    let mut digits = mantissa.unsigned_abs().to_string();
    if digits.len() <= scale {
        // pad with leading zeros so there is at least one integer digit.
        digits = format!("{:0>width$}", digits, width = scale + 1);
    }
    let split = digits.len() - scale;
    let (int_part, frac_part) = digits.split_at(split);
    format!(
        "{}{}.{}",
        if negative { "-" } else { "" },
        int_part,
        frac_part
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;

    #[test]
    fn legacy_datetime_renders_milliseconds_sql_server_can_parse() {
        let at = |nanos: u32| {
            NaiveDate::from_ymd_opt(2024, 3, 1)
                .unwrap()
                .and_hms_nano_opt(12, 30, 45, nanos)
                .unwrap()
        };
        assert_eq!(
            format_legacy_datetime(at(3_333_333)),
            "2024-03-01T12:30:45.003"
        );
        assert_eq!(
            format_legacy_datetime(at(6_666_667)),
            "2024-03-01T12:30:45.007"
        );
        assert_eq!(
            format_legacy_datetime(at(996_666_667)),
            "2024-03-01T12:30:45.997"
        );
        assert_eq!(format_legacy_datetime(at(0)), "2024-03-01T12:30:45.000");
        assert_eq!(
            format_legacy_datetime(at(999_999_999)),
            "2024-03-01T12:30:45.999"
        );
    }

    #[test]
    fn numeric_formats_with_scale() {
        assert_eq!(
            numeric_to_string(Numeric::new_with_scale(12345, 2)),
            "123.45"
        );
        assert_eq!(numeric_to_string(Numeric::new_with_scale(100, 0)), "100");
        // value < 1 must keep a leading zero
        assert_eq!(numeric_to_string(Numeric::new_with_scale(5, 2)), "0.05");
        // negatives must not corrupt the fractional part
        assert_eq!(numeric_to_string(Numeric::new_with_scale(-150, 2)), "-1.50");
        assert_eq!(numeric_to_string(Numeric::new_with_scale(-5, 2)), "-0.05");
    }

    #[test]
    fn scalar_integers_and_floats() {
        assert_eq!(scalar_to_json(&ColumnData::I32(Some(42))), Some(json!(42)));
        assert_eq!(scalar_to_json(&ColumnData::U8(Some(7))), Some(json!(7)));
        assert_eq!(scalar_to_json(&ColumnData::I16(Some(-3))), Some(json!(-3)));
        assert_eq!(
            scalar_to_json(&ColumnData::I64(Some(9_000_000_000))),
            Some(json!(9_000_000_000i64))
        );
        assert_eq!(
            scalar_to_json(&ColumnData::F64(Some(1.5))),
            Some(json!(1.5))
        );
    }

    #[test]
    fn scalar_bool_string_guid() {
        assert_eq!(
            scalar_to_json(&ColumnData::Bit(Some(true))),
            Some(json!(true))
        );
        assert_eq!(
            scalar_to_json(&ColumnData::String(Some(Cow::Borrowed("hi")))),
            Some(json!("hi"))
        );
        let id = tiberius::Uuid::nil();
        assert_eq!(
            scalar_to_json(&ColumnData::Guid(Some(id))),
            Some(json!("00000000-0000-0000-0000-000000000000"))
        );
    }

    #[test]
    fn scalar_binary_is_base64() {
        // bytes [1,2,3] -> base64 "AQID"
        let data = ColumnData::Binary(Some(Cow::Borrowed(&[1u8, 2, 3][..])));
        assert_eq!(scalar_to_json(&data), Some(json!("AQID")));
    }

    #[test]
    fn scalar_numeric_is_string() {
        let data = ColumnData::Numeric(Some(Numeric::new_with_scale(12345, 2)));
        assert_eq!(scalar_to_json(&data), Some(json!("123.45")));
    }

    #[test]
    fn scalar_null_is_json_null_not_temporal_none() {
        assert_eq!(scalar_to_json(&ColumnData::I32(None)), Some(Value::Null));
        assert_eq!(scalar_to_json(&ColumnData::String(None)), Some(Value::Null));
    }

    #[test]
    fn temporal_variants_defer_to_caller() {
        assert_eq!(scalar_to_json(&ColumnData::Date(None)), None);
        assert_eq!(scalar_to_json(&ColumnData::DateTime2(None)), None);
        assert_eq!(scalar_to_json(&ColumnData::DateTimeOffset(None)), None);
    }
}
