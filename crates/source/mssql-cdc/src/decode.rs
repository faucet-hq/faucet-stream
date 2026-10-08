//! Decode a `tiberius` [`Row`] into a `serde_json` object.
//!
//! Mirrors the query source's decoder: non-temporal columns are read directly
//! from their [`ColumnData`] variant (the variant carries the exact width, so
//! there is no integer-size guessing); temporal columns go through
//! [`Row::try_get`] with `chrono` target types so the conversion uses
//! `tiberius`' own epoch math. CDC metadata columns (`__$start_lsn`,
//! `__$update_mask`, …) come back as `binary` and are decoded to base64 here
//! (they are stripped from the change envelope downstream — see
//! [`crate::change`]).

use base64::Engine;
use chrono::{FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use faucet_core::FaucetError;
use serde_json::{Map, Value, json};
use tiberius::numeric::Numeric;
use tiberius::{ColumnData, Row};

/// Decode every column of `row` into a JSON object keyed by column name.
pub fn row_to_json(row: &Row) -> Result<Value, FaucetError> {
    let mut map = Map::with_capacity(row.columns().len());
    let names: Vec<String> = row.columns().iter().map(|c| c.name().to_string()).collect();
    for (i, (col, data)) in row.cells().enumerate() {
        let value = match cell_to_json(is_money(&col.column_type()), data) {
            Some(v) => v,
            None => decode_temporal(row, i, data)?,
        };
        map.insert(names[i].clone(), value);
    }
    Ok(Value::Object(map))
}

/// Whether a column is `MONEY` / `SMALLMONEY`.
fn is_money(ty: &tiberius::ColumnType) -> bool {
    matches!(
        ty,
        tiberius::ColumnType::Money | tiberius::ColumnType::Money4
    )
}

/// A non-temporal cell as JSON (a money column as exact decimal text), or
/// `None` for a temporal cell.
fn cell_to_json(money: bool, data: &ColumnData<'_>) -> Option<Value> {
    match (money, data) {
        (true, ColumnData::F64(Some(f))) => Some(Value::String(money_text(*f))),
        _ => scalar_to_json(data),
    }
}

/// A `MONEY` / `SMALLMONEY` value as an exact 4-decimal string. The driver
/// hands MONEY over as `f64`; every MONEY value of magnitude below 2^53 / 10^4
/// (about 900 billion) round-trips exactly through `value × 10^4`, which is
/// rendered as integer digits rather than through float formatting.
pub(crate) fn money_text(f: f64) -> String {
    let units = (f * 1e4).round() as i128;
    let sign = if units < 0 { "-" } else { "" };
    let abs = units.unsigned_abs();
    format!("{sign}{}.{:04}", abs / 10_000, abs % 10_000)
}

/// Convert a non-temporal [`ColumnData`] to JSON. Returns `Some(Value::Null)`
/// for a SQL NULL in a non-temporal column, and `None` for temporal variants
/// (decoded by [`decode_temporal`], which needs the [`Row`] for `try_get`).
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
        ColumnData::DateTime(_)
        | ColumnData::SmallDateTime(_)
        | ColumnData::Date(_)
        | ColumnData::Time(_)
        | ColumnData::DateTime2(_)
        | ColumnData::DateTimeOffset(_) => return None,
    };
    Some(v.unwrap_or(Value::Null))
}

fn decode_temporal(row: &Row, idx: usize, data: &ColumnData<'_>) -> Result<Value, FaucetError> {
    let conv = |e: tiberius::error::Error| {
        FaucetError::Source(format!(
            "mssql-cdc column {idx} temporal decode failed: {e}"
        ))
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
        ColumnData::DateTime(_) | ColumnData::SmallDateTime(_) | ColumnData::DateTime2(_) => row
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

/// Format an MSSQL DECIMAL/NUMERIC value as a precision-preserving string (JSON
/// numbers cannot represent arbitrary-precision decimals — mirrors the query
/// source's NUMERIC-as-string behaviour).
pub(crate) fn numeric_to_string(n: Numeric) -> String {
    let scale = n.scale() as usize;
    let mantissa = n.value();
    if scale == 0 {
        return mantissa.to_string();
    }
    let negative = mantissa < 0;
    let mut digits = mantissa.unsigned_abs().to_string();
    if digits.len() <= scale {
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
    #[test]
    fn money_columns_decode_as_exact_text() {
        use tiberius::{ColumnData, ColumnType};
        assert!(super::is_money(&ColumnType::Money));
        assert!(super::is_money(&ColumnType::Money4));
        assert!(!super::is_money(&ColumnType::Float8));
        assert_eq!(
            super::cell_to_json(true, &ColumnData::F64(Some(12.3456))),
            Some(serde_json::json!("12.3456"))
        );
        assert_eq!(
            super::cell_to_json(true, &ColumnData::F64(None)),
            Some(serde_json::Value::Null)
        );
        assert_eq!(
            super::cell_to_json(false, &ColumnData::F64(Some(1.5))),
            Some(serde_json::json!(1.5))
        );
    }

    #[test]
    fn money_renders_exact_decimal_text() {
        assert_eq!(super::money_text(12.3456), "12.3456");
        assert_eq!(super::money_text(-0.5), "-0.5000");
        assert_eq!(super::money_text(0.0), "0.0000");
        assert_eq!(super::money_text(900000000000.1234), "900000000000.1234");
        assert_eq!(super::money_text(-214748.3648), "-214748.3648");
    }

    use super::*;
    use std::borrow::Cow;

    #[test]
    fn numeric_formats_with_scale() {
        assert_eq!(
            numeric_to_string(Numeric::new_with_scale(12345, 2)),
            "123.45"
        );
        assert_eq!(numeric_to_string(Numeric::new_with_scale(100, 0)), "100");
        assert_eq!(numeric_to_string(Numeric::new_with_scale(5, 2)), "0.05");
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
    fn scalar_bool_string_guid_binary_numeric() {
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
        // bytes [1,2,3] -> base64 "AQID" (the shape of an LSN metadata column).
        let data = ColumnData::Binary(Some(Cow::Borrowed(&[1u8, 2, 3][..])));
        assert_eq!(scalar_to_json(&data), Some(json!("AQID")));
        let num = ColumnData::Numeric(Some(Numeric::new_with_scale(12345, 2)));
        assert_eq!(scalar_to_json(&num), Some(json!("123.45")));
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
