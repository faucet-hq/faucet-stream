//! Arrow → JSON conversion for DuckDB results.
//!
//! DuckDB hands results over as Arrow record batches. Every column is
//! converted by its Arrow type, using the DuckDB type name (from `DESCRIBE`)
//! where Arrow alone is ambiguous:
//!
//! - integers are JSON numbers; `HUGEINT` (exported as `Decimal128(38, 0)`) is
//!   a number when it fits `i64`, else its exact decimal text; other decimals
//!   are exact strings;
//! - `REAL` goes through its shortest decimal form (`0.1`, not
//!   `0.10000000149011612`); NaN / ±Inf are `"NaN"` / `"Infinity"` /
//!   `"-Infinity"`, as in every other Arrow-backed path;
//! - temporal values are ISO-8601 at their own precision (`TIMESTAMPTZ` with a
//!   `Z`), `±infinity` sentinels as `"infinity"` / `"-infinity"`;
//! - `LIST` / `ARRAY` are arrays, `STRUCT` and string-keyed `MAP` objects, other
//!   `MAP`s arrays of `{key, value}`, `ENUM` its label, `UNION` its active value;
//! - `BLOB` is base64; `UUID` and the types the source casts to `VARCHAR`
//!   (`UHUGEINT`, `BIT`, `BIGNUM`, `TIMETZ`) are their text.

use base64::Engine as _;
use duckdb::arrow::array::{Array, ArrayRef, AsArray};
use duckdb::arrow::datatypes::{
    DataType, Date32Type, Date64Type, Decimal128Type, Decimal256Type, Float16Type, Float32Type,
    Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, IntervalDayTimeType,
    IntervalMonthDayNanoType, IntervalUnit, IntervalYearMonthType, Time32MillisecondType,
    Time32SecondType, Time64MicrosecondType, Time64NanosecondType, TimeUnit,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::arrow::temporal_conversions as tc;
use faucet_core::FaucetError;
use serde_json::{Map, Value, json};

/// Convert one batch into JSON objects. `duck_types[i]` is the DuckDB type
/// name of column `i` when `DESCRIBE` reported it.
pub(crate) fn batch_to_values(
    batch: &RecordBatch,
    duck_types: &[String],
) -> Result<Vec<Value>, FaucetError> {
    let schema = batch.schema();
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (i, (field, array)) in schema.fields().iter().zip(batch.columns()).enumerate() {
        let hint = duck_types.get(i).map(String::as_str).unwrap_or("");
        let values = array_to_values(array, hint).map_err(|e| {
            FaucetError::Source(format!("DuckDB column {} read failed: {e}", field.name()))
        })?;
        columns.push(values.into_iter());
    }
    let mut rows = Vec::with_capacity(batch.num_rows());
    for _ in 0..batch.num_rows() {
        let mut row = Map::with_capacity(columns.len());
        for (field, column) in schema.fields().iter().zip(columns.iter_mut()) {
            row.insert(field.name().clone(), column.next().unwrap_or(Value::Null));
        }
        rows.push(Value::Object(row));
    }
    Ok(rows)
}

fn non_finite(v: f64) -> Value {
    Value::String(
        if v.is_nan() {
            "NaN"
        } else if v > 0.0 {
            "Infinity"
        } else {
            "-Infinity"
        }
        .into(),
    )
}

fn f64_value(v: f64) -> Value {
    serde_json::Number::from_f64(v).map_or_else(|| non_finite(v), Value::Number)
}

fn f32_value(v: f32) -> Value {
    if !v.is_finite() {
        return non_finite(f64::from(v));
    }
    v.to_string()
        .parse::<f64>()
        .ok()
        .and_then(serde_json::Number::from_f64)
        .map_or(Value::Null, Value::Number)
}

fn infinity(v: i64) -> Value {
    Value::String(if v > 0 { "infinity" } else { "-infinity" }.into())
}

fn naive_ts(dt: Option<chrono::NaiveDateTime>, v: i64, tz: bool) -> Value {
    match dt {
        Some(dt) => {
            let text = dt.format("%Y-%m-%dT%H:%M:%S%.f").to_string();
            Value::String(if tz { format!("{text}Z") } else { text })
        }
        None => infinity(v),
    }
}

fn time_text(t: Option<chrono::NaiveTime>) -> Value {
    t.map_or(Value::Null, |t| Value::String(t.format("%H:%M:%S%.f").to_string()))
}

/// Convert one column into one JSON value per row (nulls included).
pub(crate) fn array_to_values(array: &ArrayRef, duck_type: &str) -> Result<Vec<Value>, String> {
    let n = array.len();
    let mut out: Vec<Value> = match array.data_type() {
        DataType::Null => vec![Value::Null; n],
        DataType::Boolean => {
            let a = array.as_boolean();
            (0..n).map(|i| Value::Bool(a.value(i))).collect()
        }
        DataType::Int8 => prim::<Int8Type, _>(array, |v| json!(v)),
        DataType::Int16 => prim::<Int16Type, _>(array, |v| json!(v)),
        DataType::Int32 => prim::<Int32Type, _>(array, |v| json!(v)),
        DataType::Int64 => prim::<Int64Type, _>(array, |v| json!(v)),
        DataType::UInt8 => prim::<UInt8Type, _>(array, |v| json!(v)),
        DataType::UInt16 => prim::<UInt16Type, _>(array, |v| json!(v)),
        DataType::UInt32 => prim::<UInt32Type, _>(array, |v| json!(v)),
        DataType::UInt64 => prim::<UInt64Type, _>(array, |v| json!(v)),
        DataType::Float16 => prim::<Float16Type, _>(array, |v| f32_value(f32::from(v))),
        DataType::Float32 => prim::<Float32Type, _>(array, f32_value),
        DataType::Float64 => prim::<Float64Type, _>(array, f64_value),
        DataType::Decimal128(_, 0) if duck_type.eq_ignore_ascii_case("HUGEINT") => {
            prim::<Decimal128Type, _>(array, |v| {
                i64::try_from(v).map_or_else(|_| Value::String(v.to_string()), Value::from)
            })
        }
        DataType::Decimal128(..) => {
            let a = array.as_primitive::<Decimal128Type>();
            (0..n).map(|i| Value::String(a.value_as_string(i))).collect()
        }
        DataType::Decimal256(..) => {
            let a = array.as_primitive::<Decimal256Type>();
            (0..n).map(|i| Value::String(a.value_as_string(i))).collect()
        }
        DataType::Utf8 => {
            let a = array.as_string::<i32>();
            (0..n).map(|i| Value::String(a.value(i).to_owned())).collect()
        }
        DataType::LargeUtf8 => {
            let a = array.as_string::<i64>();
            (0..n).map(|i| Value::String(a.value(i).to_owned())).collect()
        }
        DataType::Utf8View => {
            let a = array.as_string_view();
            (0..n).map(|i| Value::String(a.value(i).to_owned())).collect()
        }
        DataType::Binary => {
            let a = array.as_binary::<i32>();
            (0..n).map(|i| base64(a.value(i))).collect()
        }
        DataType::LargeBinary => {
            let a = array.as_binary::<i64>();
            (0..n).map(|i| base64(a.value(i))).collect()
        }
        DataType::BinaryView => {
            let a = array.as_binary_view();
            (0..n).map(|i| base64(a.value(i))).collect()
        }
        DataType::FixedSizeBinary(_) => {
            let a = array.as_fixed_size_binary();
            (0..n).map(|i| base64(a.value(i))).collect()
        }
        DataType::Date32 => prim::<Date32Type, _>(array, |v| {
            tc::date32_to_datetime(v).map_or_else(
                || infinity(i64::from(v)),
                |d| Value::String(d.format("%Y-%m-%d").to_string()),
            )
        }),
        DataType::Date64 => prim::<Date64Type, _>(array, |v| {
            tc::date64_to_datetime(v).map_or_else(
                || infinity(v),
                |d| Value::String(d.format("%Y-%m-%d").to_string()),
            )
        }),
        DataType::Timestamp(unit, tz) => {
            let tz = tz.is_some();
            match unit {
                TimeUnit::Second => prim::<TimestampSecondType, _>(array, |v| {
                    naive_ts(tc::timestamp_s_to_datetime(v), v, tz)
                }),
                TimeUnit::Millisecond => prim::<TimestampMillisecondType, _>(array, |v| {
                    naive_ts(tc::timestamp_ms_to_datetime(v), v, tz)
                }),
                TimeUnit::Microsecond => prim::<TimestampMicrosecondType, _>(array, |v| {
                    naive_ts(tc::timestamp_us_to_datetime(v), v, tz)
                }),
                TimeUnit::Nanosecond => prim::<TimestampNanosecondType, _>(array, |v| {
                    naive_ts(tc::timestamp_ns_to_datetime(v), v, tz)
                }),
            }
        }
        DataType::Time32(TimeUnit::Second) => {
            prim::<Time32SecondType, _>(array, |v| time_text(tc::time32s_to_time(v)))
        }
        DataType::Time32(_) => {
            prim::<Time32MillisecondType, _>(array, |v| time_text(tc::time32ms_to_time(v)))
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            prim::<Time64NanosecondType, _>(array, |v| time_text(tc::time64ns_to_time(v)))
        }
        DataType::Time64(_) => {
            prim::<Time64MicrosecondType, _>(array, |v| time_text(tc::time64us_to_time(v)))
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            prim::<IntervalMonthDayNanoType, _>(array, |v| {
                json!({ "months": v.months, "days": v.days, "nanos": v.nanoseconds })
            })
        }
        DataType::Interval(IntervalUnit::DayTime) => prim::<IntervalDayTimeType, _>(array, |v| {
            json!({ "months": 0, "days": v.days, "nanos": i64::from(v.milliseconds) * 1_000_000 })
        }),
        DataType::Interval(IntervalUnit::YearMonth) => {
            prim::<IntervalYearMonthType, _>(array, |v| json!({ "months": v, "days": 0, "nanos": 0 }))
        }
        DataType::List(_) => {
            let a = array.as_list::<i32>();
            let values = array_to_values(a.values(), "")?;
            let offsets = a.value_offsets();
            (0..n)
                .map(|i| slice(&values, offsets[i] as usize, offsets[i + 1] as usize))
                .collect()
        }
        DataType::LargeList(_) => {
            let a = array.as_list::<i64>();
            let values = array_to_values(a.values(), "")?;
            let offsets = a.value_offsets();
            (0..n)
                .map(|i| slice(&values, offsets[i] as usize, offsets[i + 1] as usize))
                .collect()
        }
        DataType::FixedSizeList(_, size) => {
            let a = array.as_fixed_size_list();
            let values = array_to_values(a.values(), "")?;
            let size = *size as usize;
            let base = a.offset() * size;
            (0..n)
                .map(|i| slice(&values, base + i * size, base + (i + 1) * size))
                .collect()
        }
        DataType::Struct(fields) => {
            let a = array.as_struct();
            let mut children = Vec::with_capacity(fields.len());
            for child in a.columns() {
                children.push(array_to_values(child, "")?);
            }
            (0..n)
                .map(|i| {
                    let mut obj = Map::with_capacity(fields.len());
                    for (f, child) in fields.iter().zip(&children) {
                        obj.insert(f.name().clone(), child[i].clone());
                    }
                    Value::Object(obj)
                })
                .collect()
        }
        DataType::Map(..) => {
            let a = array.as_map();
            let keys = array_to_values(a.keys(), "")?;
            let vals = array_to_values(a.values(), "")?;
            let offsets = a.value_offsets();
            let string_keys = keys.iter().all(Value::is_string);
            (0..n)
                .map(|i| {
                    let range = offsets[i] as usize..offsets[i + 1] as usize;
                    if string_keys {
                        Value::Object(
                            range
                                .map(|j| {
                                    let k = keys[j].as_str().unwrap_or_default().to_owned();
                                    (k, vals[j].clone())
                                })
                                .collect(),
                        )
                    } else {
                        Value::Array(
                            range
                                .map(|j| json!({ "key": keys[j], "value": vals[j] }))
                                .collect(),
                        )
                    }
                })
                .collect()
        }
        DataType::Dictionary(_, value_type) => {
            let decoded = duckdb::arrow::compute::cast(array, value_type)
                .map_err(|e| format!("decoding a dictionary column: {e}"))?;
            array_to_values(&decoded, "")?
        }
        DataType::Union(..) => {
            let a = array.as_union();
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                let one = a.value(i);
                out.push(array_to_values(&one, "")?.pop().unwrap_or(Value::Null));
            }
            out
        }
        other => {
            return Err(format!(
                "the Arrow type {other} has no JSON form here; CAST it to VARCHAR in the query"
            ));
        }
    };
    if array.null_count() > 0 && !matches!(array.data_type(), DataType::Union(..)) {
        for (i, v) in out.iter_mut().enumerate() {
            if array.is_null(i) {
                *v = Value::Null;
            }
        }
    }
    Ok(out)
}

fn prim<T, F>(array: &ArrayRef, f: F) -> Vec<Value>
where
    T: duckdb::arrow::datatypes::ArrowPrimitiveType,
    F: Fn(T::Native) -> Value,
{
    array.as_primitive::<T>().values().iter().map(|v| f(*v)).collect()
}

fn slice(values: &[Value], start: usize, end: usize) -> Value {
    Value::Array(values.get(start..end).map(<[Value]>::to_vec).unwrap_or_default())
}

fn base64(bytes: &[u8]) -> Value {
    Value::String(base64::engine::general_purpose::STANDARD.encode(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use duckdb::arrow::array::{
        BinaryViewArray, Date64Array, Decimal256Array, FixedSizeBinaryArray,
        IntervalDayTimeArray, IntervalYearMonthArray, LargeBinaryArray, LargeListArray,
        LargeStringArray, NullArray, StringViewArray, Time32MillisecondArray, Time32SecondArray,
        Time64NanosecondArray, TimestampMillisecondArray, DurationSecondArray,
    };
    use duckdb::arrow::datatypes::{Int32Type as I32, IntervalDayTime, i256};
    use std::sync::Arc;

    fn conv(a: ArrayRef) -> Vec<Value> {
        array_to_values(&a, "").unwrap()
    }

    #[test]
    fn arrow_types_duckdb_does_not_emit_still_convert() {
        assert_eq!(conv(Arc::new(NullArray::new(2))), vec![Value::Null, Value::Null]);
        assert_eq!(
            conv(
                duckdb::arrow::compute::cast(
                    &duckdb::arrow::array::Float32Array::from(vec![0.5]),
                    &DataType::Float16
                )
                .unwrap()
            ),
            vec![json!(0.5)]
        );
        assert_eq!(
            conv(Arc::new(LargeStringArray::from(vec!["a"]))),
            vec![json!("a")]
        );
        assert_eq!(
            conv(Arc::new(StringViewArray::from(vec!["v"]))),
            vec![json!("v")]
        );
        assert_eq!(
            conv(Arc::new(LargeBinaryArray::from(vec![&b"\x00"[..]]))),
            vec![json!("AA==")]
        );
        assert_eq!(
            conv(Arc::new(BinaryViewArray::from(vec![&b"\x01"[..]]))),
            vec![json!("AQ==")]
        );
        assert_eq!(
            conv(Arc::new(
                FixedSizeBinaryArray::try_from_iter(vec![vec![1u8, 2]].into_iter()).unwrap()
            )),
            vec![json!("AQI=")]
        );
        assert_eq!(
            conv(Arc::new(Date64Array::from(vec![86_400_000]))),
            vec![json!("1970-01-02")]
        );
        assert_eq!(
            conv(Arc::new(TimestampMillisecondArray::from(vec![1_500]))),
            vec![json!("1970-01-01T00:00:01.500")]
        );
        assert_eq!(
            conv(Arc::new(Time32SecondArray::from(vec![61]))),
            vec![json!("00:01:01")]
        );
        assert_eq!(
            conv(Arc::new(Time32MillisecondArray::from(vec![1_250]))),
            vec![json!("00:00:01.250")]
        );
        assert_eq!(
            conv(Arc::new(Time64NanosecondArray::from(vec![1]))),
            vec![json!("00:00:00.000000001")]
        );
        assert_eq!(
            conv(Arc::new(IntervalDayTimeArray::from(vec![IntervalDayTime::new(2, 3)]))),
            vec![json!({"months": 0, "days": 2, "nanos": 3_000_000})]
        );
        assert_eq!(
            conv(Arc::new(IntervalYearMonthArray::from(vec![14]))),
            vec![json!({"months": 14, "days": 0, "nanos": 0})]
        );
        assert_eq!(
            conv(Arc::new(
                Decimal256Array::from(vec![i256::from_i128(12345)])
                    .with_precision_and_scale(40, 2)
                    .unwrap()
            )),
            vec![json!("123.45")]
        );
        let large = LargeListArray::from_iter_primitive::<I32, _, _>(vec![
            Some(vec![Some(1), None]),
            None,
        ]);
        assert_eq!(conv(Arc::new(large)), vec![json!([1, null]), Value::Null]);
        let err = array_to_values(&(Arc::new(DurationSecondArray::from(vec![1])) as ArrayRef), "")
            .unwrap_err();
        assert!(err.contains("CAST it to VARCHAR"), "{err}");
    }

    #[test]
    fn floats_and_out_of_range_dates() {
        assert_eq!(f32_value(f32::INFINITY), json!("Infinity"));
        assert_eq!(f64_value(f64::NEG_INFINITY), json!("-Infinity"));
        assert_eq!(f64_value(f64::NAN), json!("NaN"));
        assert_eq!(f32_value(0.1), json!(0.1));
        assert_eq!(
            conv(Arc::new(Date64Array::from(vec![i64::MAX]))),
            vec![json!("infinity")]
        );
        assert_eq!(time_text(None), Value::Null);
        assert_eq!(slice(&[json!(1)], 3, 4), json!([]));
    }
}
