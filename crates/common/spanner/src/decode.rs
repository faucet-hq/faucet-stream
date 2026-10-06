//! Generic Spanner row → `serde_json::Value` decoding.
//!
//! faucet sources run arbitrary user SQL, so rows must decode without a
//! compile-time schema. [`SpannerJson`] implements the client's
//! `TryFromValue` hook, which hands us the raw protobuf value *plus* the
//! column's full [`Type`] metadata — enough to decode every Spanner type
//! into faithful JSON:
//!
//! | Spanner | JSON |
//! |---|---|
//! | INT64 / ENUM | integer (lossless — arrives string-encoded) |
//! | FLOAT64 / FLOAT32 | number (`NaN`/`Infinity` → string) |
//! | BOOL | boolean |
//! | STRING / TIMESTAMP / DATE / UUID / INTERVAL | string |
//! | BYTES / PROTO | base64 string (as transmitted) |
//! | NUMERIC | string (precision preserved) |
//! | JSON | parsed object/array/scalar |
//! | ARRAY | array (element type recursed) |
//! | STRUCT | object keyed by field name |

use gcloud_googleapis::spanner::v1::struct_type::Field;
use gcloud_googleapis::spanner::v1::{Type, TypeCode};
use gcloud_spanner::row::{Error as RowError, Row, TryFromValue};
use prost_types::value::Kind;
use serde_json::Value;

/// Newtype decoding one Spanner column into a [`serde_json::Value`].
pub struct SpannerJson(pub Value);

impl TryFromValue for SpannerJson {
    fn try_from(value: &prost_types::Value, field: &Field) -> Result<Self, RowError> {
        decode_value(value, field.r#type.as_ref(), &field.name).map(SpannerJson)
    }
}

/// Decode a whole [`Row`] into a JSON object keyed by column name, using the
/// stream's column metadata (`RowIterator::columns_metadata`).
pub fn row_to_json(row: &Row, fields: &[Field]) -> Result<Value, RowError> {
    let mut obj = serde_json::Map::with_capacity(fields.len());
    for (idx, field) in fields.iter().enumerate() {
        let SpannerJson(v) = row.column::<SpannerJson>(idx)?;
        obj.insert(field.name.clone(), v);
    }
    Ok(Value::Object(obj))
}

fn type_code(ty: Option<&Type>) -> TypeCode {
    ty.and_then(|t| TypeCode::try_from(t.code).ok())
        .unwrap_or(TypeCode::Unspecified)
}

/// Whether the named column in a result's metadata is Spanner `NUMERIC`.
///
/// `NUMERIC` decodes to a JSON *string* (to preserve precision) and `TIMESTAMP`
/// to Spanner's variable-width RFC 3339 text (trailing fractional zeros are
/// trimmed, so text order is not time order); the replication comparison orders
/// both by value, so either works as an incremental cursor.
pub fn column_is_numeric(fields: &[Field], name: &str) -> bool {
    fields
        .iter()
        .find(|f| f.name == name)
        .map(|f| type_code(f.r#type.as_ref()) == TypeCode::Numeric)
        .unwrap_or(false)
}

/// Decode one protobuf value with its (possibly absent) Spanner type.
pub fn decode_value(
    value: &prost_types::Value,
    ty: Option<&Type>,
    field_name: &str,
) -> Result<Value, RowError> {
    let kind = match &value.kind {
        None | Some(Kind::NullValue(_)) => return Ok(Value::Null),
        Some(kind) => kind,
    };
    let code = type_code(ty);
    match (code, kind) {
        // INT64 (and ENUM numbers) arrive string-encoded to survive f64
        // precision loss; keep them lossless as JSON integers.
        (TypeCode::Int64 | TypeCode::Enum, Kind::StringValue(s)) => {
            let n: i64 = s.parse().map_err(|_| {
                RowError::CustomParseError(format!("{field_name}: non-integer INT64 `{s}`"))
            })?;
            Ok(Value::Number(n.into()))
        }
        (TypeCode::Float64 | TypeCode::Float32, Kind::NumberValue(f)) => {
            Ok(serde_json::Number::from_f64(*f).map_or(Value::Null, Value::Number))
        }
        // FLOAT64 NaN / Infinity / -Infinity arrive as strings; JSON has no
        // representation for them, so keep the string form.
        (TypeCode::Float64 | TypeCode::Float32, Kind::StringValue(s)) => {
            Ok(Value::String(s.clone()))
        }
        (TypeCode::Bool, Kind::BoolValue(b)) => Ok(Value::Bool(*b)),
        // JSON columns carry their RFC 7159 text in a string value.
        (TypeCode::Json, Kind::StringValue(s)) => serde_json::from_str(s).map_err(|e| {
            RowError::CustomParseError(format!("{field_name}: invalid JSON column value: {e}"))
        }),
        // Strings and every string-encoded scalar (timestamps, dates,
        // NUMERIC, base64 bytes, UUID, INTERVAL, PROTO) pass through.
        (_, Kind::StringValue(s)) => Ok(Value::String(s.clone())),
        (TypeCode::Array, Kind::ListValue(list)) => {
            let elem_ty = ty.and_then(|t| t.array_element_type.as_deref());
            let items: Result<Vec<Value>, RowError> = list
                .values
                .iter()
                .map(|v| decode_value(v, elem_ty, field_name))
                .collect();
            Ok(Value::Array(items?))
        }
        // STRUCT rows are encoded as a list whose i-th element matches
        // `struct_type.fields[i]`.
        (TypeCode::Struct, Kind::ListValue(list)) => {
            let fields = ty
                .and_then(|t| t.struct_type.as_ref())
                .map(|st| st.fields.as_slice())
                .unwrap_or(&[]);
            let mut obj = serde_json::Map::with_capacity(list.values.len());
            for (idx, v) in list.values.iter().enumerate() {
                let (name, elem_ty) = fields
                    .get(idx)
                    .map(|f| (f.name.clone(), f.r#type.as_ref()))
                    .unwrap_or_else(|| (format!("_{idx}"), None));
                obj.insert(name, decode_value(v, elem_ty, field_name)?);
            }
            Ok(Value::Object(obj))
        }
        // Untyped / unexpected pairings: decode the raw protobuf value
        // structurally so nothing is silently dropped.
        (_, kind) => Ok(kind_to_json(kind)),
    }
}

/// Structural protobuf → JSON fallback for values whose Spanner type is
/// absent or does not match the wire kind. Infallible: every protobuf value
/// kind has a structural JSON form.
fn kind_to_json(kind: &Kind) -> Value {
    match kind {
        Kind::NullValue(_) => Value::Null,
        Kind::BoolValue(b) => Value::Bool(*b),
        Kind::NumberValue(f) => serde_json::Number::from_f64(*f).map_or(Value::Null, Value::Number),
        Kind::StringValue(s) => Value::String(s.clone()),
        Kind::ListValue(list) => Value::Array(
            list.values
                .iter()
                .map(|v| match &v.kind {
                    None => Value::Null,
                    Some(k) => kind_to_json(k),
                })
                .collect(),
        ),
        Kind::StructValue(st) => {
            let mut obj = serde_json::Map::with_capacity(st.fields.len());
            for (k, v) in &st.fields {
                let decoded = match &v.kind {
                    None => Value::Null,
                    Some(kind) => kind_to_json(kind),
                };
                obj.insert(k.clone(), decoded);
            }
            Value::Object(obj)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gcloud_googleapis::spanner::v1::StructType;
    use serde_json::json;

    fn pv(kind: Kind) -> prost_types::Value {
        prost_types::Value { kind: Some(kind) }
    }

    fn ty(code: TypeCode) -> Type {
        Type {
            code: code.into(),
            array_element_type: None,
            struct_type: None,
            ..Default::default()
        }
    }

    #[test]
    fn column_is_numeric_only_flags_numeric_columns() {
        let field = |name: &str, code: TypeCode| Field {
            name: name.to_string(),
            r#type: Some(ty(code)),
        };
        let fields = vec![
            field("id", TypeCode::Int64),
            field("amount", TypeCode::Numeric),
            field("updated_at", TypeCode::Timestamp),
        ];
        // Only the NUMERIC column is flagged (the #466 L3 cursor guard).
        assert!(column_is_numeric(&fields, "amount"));
        assert!(!column_is_numeric(&fields, "id"));
        assert!(!column_is_numeric(&fields, "updated_at"));
        // An unknown column is not numeric (and must not panic).
        assert!(!column_is_numeric(&fields, "nope"));
    }

    #[test]
    fn int64_decodes_losslessly_from_string() {
        let big = 9_007_199_254_740_993_i64; // 2^53 + 1 — breaks under f64
        let v = decode_value(
            &pv(Kind::StringValue(big.to_string())),
            Some(&ty(TypeCode::Int64)),
            "n",
        )
        .unwrap();
        assert_eq!(v, json!(big));
    }

    #[test]
    fn malformed_int64_is_a_typed_error() {
        let err = decode_value(
            &pv(Kind::StringValue("abc".into())),
            Some(&ty(TypeCode::Int64)),
            "n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("non-integer INT64"));
    }

    #[test]
    fn floats_bools_strings_and_nulls() {
        assert_eq!(
            decode_value(
                &pv(Kind::NumberValue(1.5)),
                Some(&ty(TypeCode::Float64)),
                "f"
            )
            .unwrap(),
            json!(1.5)
        );
        assert_eq!(
            decode_value(
                &pv(Kind::StringValue("NaN".into())),
                Some(&ty(TypeCode::Float64)),
                "f"
            )
            .unwrap(),
            json!("NaN")
        );
        assert_eq!(
            decode_value(&pv(Kind::BoolValue(true)), Some(&ty(TypeCode::Bool)), "b").unwrap(),
            json!(true)
        );
        assert_eq!(
            decode_value(
                &pv(Kind::StringValue("hi".into())),
                Some(&ty(TypeCode::String)),
                "s"
            )
            .unwrap(),
            json!("hi")
        );
        assert_eq!(
            decode_value(&pv(Kind::NullValue(0)), Some(&ty(TypeCode::String)), "s").unwrap(),
            Value::Null
        );
        assert_eq!(
            decode_value(&prost_types::Value { kind: None }, None, "x").unwrap(),
            Value::Null
        );
    }

    #[test]
    fn numeric_timestamp_date_bytes_stay_strings() {
        for code in [
            TypeCode::Numeric,
            TypeCode::Timestamp,
            TypeCode::Date,
            TypeCode::Bytes,
        ] {
            let v =
                decode_value(&pv(Kind::StringValue("raw".into())), Some(&ty(code)), "c").unwrap();
            assert_eq!(v, json!("raw"));
        }
    }

    #[test]
    fn json_columns_parse_to_structured_values() {
        let v = decode_value(
            &pv(Kind::StringValue(r#"{"a": [1, 2]}"#.into())),
            Some(&ty(TypeCode::Json)),
            "j",
        )
        .unwrap();
        assert_eq!(v, json!({"a": [1, 2]}));
        let err = decode_value(
            &pv(Kind::StringValue("{not json".into())),
            Some(&ty(TypeCode::Json)),
            "j",
        )
        .unwrap_err();
        assert!(err.to_string().contains("invalid JSON column value"));
    }

    #[test]
    fn arrays_recurse_with_element_type() {
        let mut arr_ty = ty(TypeCode::Array);
        arr_ty.array_element_type = Some(Box::new(ty(TypeCode::Int64)));
        let v = decode_value(
            &pv(Kind::ListValue(prost_types::ListValue {
                values: vec![pv(Kind::StringValue("1".into())), pv(Kind::NullValue(0))],
            })),
            Some(&arr_ty),
            "a",
        )
        .unwrap();
        assert_eq!(v, json!([1, null]));
    }

    #[test]
    fn structs_become_objects_keyed_by_field_name() {
        let mut st_ty = ty(TypeCode::Struct);
        st_ty.struct_type = Some(StructType {
            fields: vec![
                Field {
                    name: "id".into(),
                    r#type: Some(ty(TypeCode::Int64)),
                },
                Field {
                    name: "name".into(),
                    r#type: Some(ty(TypeCode::String)),
                },
            ],
        });
        let v = decode_value(
            &pv(Kind::ListValue(prost_types::ListValue {
                values: vec![
                    pv(Kind::StringValue("7".into())),
                    pv(Kind::StringValue("x".into())),
                ],
            })),
            Some(&st_ty),
            "s",
        )
        .unwrap();
        assert_eq!(v, json!({"id": 7, "name": "x"}));
    }

    #[test]
    fn untyped_values_fall_back_to_structural_decode() {
        let v = decode_value(&pv(Kind::NumberValue(2.0)), None, "u").unwrap();
        assert_eq!(v, json!(2.0));
        let v = decode_value(
            &pv(Kind::ListValue(prost_types::ListValue {
                values: vec![pv(Kind::BoolValue(false))],
            })),
            None,
            "u",
        )
        .unwrap();
        assert_eq!(v, json!([false]));
    }
}
