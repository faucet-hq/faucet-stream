//! Type-aware conversion from BigQuery `TableRow`s to JSON objects.
//!
//! BigQuery returns every scalar cell as a string regardless of declared
//! type, with non-trivial structure (`RECORD`/`STRUCT`/`REPEATED`) reflected
//! in nested cell shapes:
//!
//! - A `RECORD` cell carries `{"f": [...nested cells...]}` in its `value`
//!   field.
//! - A `REPEATED` cell carries `[{"v": ...cell...}, ...]` in its `value`
//!   field — one element per array entry.
//!
//! This module walks the schema in lockstep with each row's cells and emits
//! a `serde_json::Value::Object` keyed by column name, with each cell typed
//! according to its `TableFieldSchema`.
//!
//! ## Type mapping
//!
//! | BigQuery type                          | JSON output                      |
//! |----------------------------------------|----------------------------------|
//! | `INTEGER` / `INT64`                    | `Number` (i64 if it fits, else f64) |
//! | `FLOAT` / `FLOAT64`                    | `Number` (f64)                   |
//! | `NUMERIC` / `BIGNUMERIC`               | `String` (full precision preserved) |
//! | `BOOLEAN` / `BOOL`                     | `Bool`                           |
//! | `RECORD` / `STRUCT`                    | nested `Object`                  |
//! | `REPEATED <T>` mode                    | `Array<T>`                       |
//! | `JSON`                                 | parsed `Value` (falls back to `String`) |
//! | `STRING` / `BYTES` / `TIMESTAMP` / `DATE` / `TIME` / `DATETIME` / `GEOGRAPHY` / `INTERVAL` | `String` |
//! | `NULL` cell                            | `Null`                           |

use gcp_bigquery_client::model::field_type::FieldType;
use gcp_bigquery_client::model::table_field_schema::TableFieldSchema;
use gcp_bigquery_client::model::table_row::TableRow;
use serde::Deserialize;
use serde_json::{Map, Value};

/// Convert a BigQuery `TableRow` into a JSON object using its schema.
///
/// `fields` and the row's `columns` are expected to line up positionally —
/// the field at index `i` describes the cell at index `i`. Missing trailing
/// cells map to `Null`.
pub fn row_to_json(row: &TableRow, fields: &[TableFieldSchema]) -> Value {
    let cells = row.columns.as_deref().unwrap_or(&[]);
    let mut obj = Map::with_capacity(fields.len());
    for (i, field) in fields.iter().enumerate() {
        let raw = cells.get(i).and_then(|c| c.value.as_ref());
        obj.insert(field.name.clone(), cell_to_json(raw, field));
    }
    Value::Object(obj)
}

/// Convert one `TableCell.value` (or `None` for a missing cell) to JSON,
/// honouring `REPEATED` mode and `RECORD`/`STRUCT` nesting.
fn cell_to_json(value: Option<&Value>, field: &TableFieldSchema) -> Value {
    let Some(value) = value else {
        return Value::Null;
    };
    if value.is_null() {
        return Value::Null;
    }

    let repeated = field
        .mode
        .as_deref()
        .map(|m| m.eq_ignore_ascii_case("REPEATED"))
        .unwrap_or(false);

    if repeated {
        // Repeated cells are arrays of `{v: ...}` wrappers; recurse into
        // each element as if it were a single non-repeated cell of the same
        // field type.
        let Some(array) = value.as_array() else {
            // Shape we didn't expect — fall back to a single-element array
            // carrying the raw value rather than dropping data on the floor.
            return Value::Array(vec![scalar_or_record(value, field)]);
        };
        let items: Vec<Value> = array
            .iter()
            .map(|elem| {
                // Each element is a `TableCell`-shaped wrapper; unwrap the
                // `{"v": ...}` envelope if present, otherwise treat the
                // element itself as the payload.
                match elem.get("v") {
                    Some(inner) => scalar_or_record(inner, field),
                    None => scalar_or_record(elem, field),
                }
            })
            .collect();
        return Value::Array(items);
    }

    scalar_or_record(value, field)
}

/// Convert one already-unwrapped cell payload (no `REPEATED` wrapper) to JSON.
fn scalar_or_record(payload: &Value, field: &TableFieldSchema) -> Value {
    if matches!(field.r#type, FieldType::Record | FieldType::Struct) {
        return record_to_json(payload, field);
    }

    let Some(s) = payload.as_str() else {
        // BigQuery may already have typed numeric/bool values for certain
        // formats; pass through unchanged.
        return payload.clone();
    };

    match field.r#type {
        FieldType::Integer | FieldType::Int64 => parse_integer(s),
        FieldType::Float | FieldType::Float64 => parse_float(s),
        FieldType::Boolean | FieldType::Bool => parse_bool(s),
        FieldType::Json => serde_json::from_str(s).unwrap_or_else(|_| Value::String(s.to_owned())),
        FieldType::Timestamp => {
            Value::String(timestamp_to_rfc3339(s).unwrap_or_else(|| s.to_owned()))
        }
        // STRING, BYTES, DATE, TIME, DATETIME, NUMERIC,
        // BIGNUMERIC, GEOGRAPHY, INTERVAL — keep as string for
        // round-trip safety.
        _ => Value::String(s.to_owned()),
    }
}

/// A REST-API TIMESTAMP cell — epoch seconds as decimal text, often in
/// scientific notation (`"1.7019216E9"`) — as RFC 3339 UTC with the
/// microseconds BigQuery stores. The text is shifted to microseconds
/// digit by digit, never through `f64`. `None` when it is not a decimal.
pub(crate) fn timestamp_to_rfc3339(s: &str) -> Option<String> {
    let s = s.trim();
    let (neg, body) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (mantissa, exp) = match body.split_once(['e', 'E']) {
        Some((m, e)) => (m, e.parse::<i32>().ok()?),
        None => (body, 0),
    };
    let (int, frac) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if int.is_empty() && frac.is_empty()
        || !int.bytes().chain(frac.bytes()).all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let digits = format!("{int}{frac}");
    // value = digits × 10^(exp − frac.len()); micros = value × 10^6.
    let shift = exp + 6 - i32::try_from(frac.len()).ok()?;
    let mut micros: i128 = digits.parse().ok()?;
    if shift >= 0 {
        micros = micros.checked_mul(10i128.checked_pow(shift.unsigned_abs())?)?;
    } else {
        micros /= 10i128.checked_pow(shift.unsigned_abs())?;
    }
    if neg {
        micros = -micros;
    }
    let dt = chrono::DateTime::from_timestamp_micros(i64::try_from(micros).ok()?)?;
    Some(dt.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
}

/// Decode a RECORD/STRUCT cell. The payload is itself a `TableRow`-shaped
/// object `{"f": [...]}`, so we deserialise it into a `TableRow` and recurse.
fn record_to_json(payload: &Value, field: &TableFieldSchema) -> Value {
    let row: TableRow = match TableRow::deserialize(payload.clone()) {
        Ok(row) => row,
        Err(_) => return payload.clone(),
    };
    let nested_fields = field.fields.as_deref().unwrap_or(&[]);
    row_to_json(&row, nested_fields)
}

fn parse_integer(s: &str) -> Value {
    if let Ok(i) = s.parse::<i64>() {
        return Value::Number(i.into());
    }
    parse_float(s)
}

fn parse_float(s: &str) -> Value {
    match s.parse::<f64>() {
        Ok(f) => serde_json::Number::from_f64(f)
            .map(Value::Number)
            .unwrap_or_else(|| Value::String(s.to_owned())),
        Err(_) => Value::String(s.to_owned()),
    }
}

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
    use gcp_bigquery_client::model::field_type::FieldType;
    use gcp_bigquery_client::model::table_cell::TableCell;
    use serde_json::json;

    fn field(name: &str, ty: FieldType) -> TableFieldSchema {
        TableFieldSchema::new(name, ty)
    }

    fn repeated(mut f: TableFieldSchema) -> TableFieldSchema {
        f.mode = Some("REPEATED".to_string());
        f
    }

    fn cells(values: Vec<Value>) -> TableRow {
        TableRow {
            columns: Some(
                values
                    .into_iter()
                    .map(|v| TableCell { value: Some(v) })
                    .collect(),
            ),
        }
    }

    #[test]
    fn integer_parses_as_number() {
        let row = cells(vec![json!("42")]);
        let fields = vec![field("id", FieldType::Integer)];
        assert_eq!(row_to_json(&row, &fields), json!({"id": 42}));
    }

    #[test]
    fn float_parses_as_number() {
        let row = cells(vec![json!("2.5")]);
        let fields = vec![field("ratio", FieldType::Float)];
        assert_eq!(row_to_json(&row, &fields), json!({"ratio": 2.5}));
    }

    #[test]
    fn boolean_parses_lowercase() {
        let row = cells(vec![json!("true"), json!("false")]);
        let fields = vec![field("a", FieldType::Boolean), field("b", FieldType::Bool)];
        assert_eq!(row_to_json(&row, &fields), json!({"a": true, "b": false}));
    }

    #[test]
    fn numeric_preserves_string_for_precision() {
        let row = cells(vec![json!("1234567890123456789012345.6789")]);
        let fields = vec![field("amount", FieldType::Numeric)];
        assert_eq!(
            row_to_json(&row, &fields),
            json!({"amount": "1234567890123456789012345.6789"})
        );
    }

    #[test]
    fn timestamp_renders_as_rfc3339() {
        let row = cells(vec![json!("1.7e9")]);
        let fields = vec![field("ts", FieldType::Timestamp)];
        assert_eq!(
            row_to_json(&row, &fields),
            json!({"ts": "2023-11-14T22:13:20Z"})
        );
        assert_eq!(
            timestamp_to_rfc3339("1.701921612345678E9").as_deref(),
            Some("2023-12-07T04:00:12.345678Z")
        );
        assert_eq!(
            timestamp_to_rfc3339("1700000000.5").as_deref(),
            Some("2023-11-14T22:13:20.500Z")
        );
        assert_eq!(
            timestamp_to_rfc3339("-1.5").as_deref(),
            Some("1969-12-31T23:59:58.500Z")
        );
        assert_eq!(
            timestamp_to_rfc3339("+0").as_deref(),
            Some("1970-01-01T00:00:00Z")
        );
        for bad in ["", ".", "x", "1e", "1.2.3", "1e999999"] {
            assert_eq!(timestamp_to_rfc3339(bad), None, "{bad}");
        }
        let row = cells(vec![json!("not-a-time")]);
        assert_eq!(row_to_json(&row, &fields), json!({"ts": "not-a-time"}));
    }

    #[test]
    fn null_cell_maps_to_null() {
        let row = cells(vec![json!(null)]);
        let fields = vec![field("x", FieldType::Integer)];
        assert_eq!(row_to_json(&row, &fields), json!({"x": null}));
    }

    #[test]
    fn missing_cell_maps_to_null() {
        // Row has zero cells but the schema declares one field.
        let row = TableRow {
            columns: Some(vec![]),
        };
        let fields = vec![field("x", FieldType::String)];
        assert_eq!(row_to_json(&row, &fields), json!({"x": null}));
    }

    #[test]
    fn repeated_integers_become_array() {
        let row = cells(vec![json!([{"v": "1"}, {"v": "2"}, {"v": "3"}])]);
        let fields = vec![repeated(field("ids", FieldType::Integer))];
        assert_eq!(row_to_json(&row, &fields), json!({"ids": [1, 2, 3]}));
    }

    #[test]
    fn record_decodes_nested_object() {
        let row = cells(vec![json!({"f": [{"v": "1"}, {"v": "alice"}]})]);
        let mut record_field = field("user", FieldType::Record);
        record_field.fields = Some(vec![
            field("id", FieldType::Integer),
            field("name", FieldType::String),
        ]);
        let fields = vec![record_field];
        assert_eq!(
            row_to_json(&row, &fields),
            json!({"user": {"id": 1, "name": "alice"}})
        );
    }

    #[test]
    fn json_field_parses_inner_json() {
        let row = cells(vec![json!(r#"{"k": 1}"#)]);
        let fields = vec![field("payload", FieldType::Json)];
        assert_eq!(row_to_json(&row, &fields), json!({"payload": {"k": 1}}));
    }

    #[test]
    fn json_field_falls_back_to_string_on_invalid_json() {
        let row = cells(vec![json!("not-json")]);
        let fields = vec![field("payload", FieldType::Json)];
        assert_eq!(row_to_json(&row, &fields), json!({"payload": "not-json"}));
    }

    #[test]
    fn unparseable_integer_falls_back_to_string() {
        let row = cells(vec![json!("abc")]);
        let fields = vec![field("id", FieldType::Integer)];
        assert_eq!(row_to_json(&row, &fields), json!({"id": "abc"}));
    }
}
