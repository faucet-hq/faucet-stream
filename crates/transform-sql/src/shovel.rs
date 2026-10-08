//! JSON ↔ Arrow conversion. `arrow-json` for both directions; `arrow` for
//! concatenation. All errors surface as `FaucetError::Transform` (or
//! `FaucetError::Config` for arity mismatches in the inline `values` relation).

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use faucet_core::FaucetError;
use serde_json::{Map, Value};
use std::sync::Arc;

/// Map any display-able error into a `FaucetError::Transform` with context.
fn te<E: std::fmt::Display>(ctx: &str, e: E) -> FaucetError {
    FaucetError::Transform(format!("sql transform: {ctx}: {e}"))
}

/// Infer an Arrow [`Schema`] from a slice of JSON records.
///
/// Each element must be a JSON object. Returns the inferred schema wrapped in
/// an [`Arc`]. On an empty slice the result is a schema with no fields.
///
/// Delegates to [`faucet_core::columnar::infer_arrow_schema`] rather than calling
/// `arrow-json` directly. The two used to be independent copies of the same call,
/// which is how the wide-integer bug (#460) existed in both: an integer above
/// `i64::MAX` was inferred as `Float64` and silently lost its exact value. One
/// implementation means one place to fix.
pub fn infer_schema(records: &[Value]) -> Result<SchemaRef, FaucetError> {
    // Re-wrap so the message still names the thing the operator configured (a
    // `sql` transform) rather than only the shared shim it delegates to.
    faucet_core::columnar::infer_arrow_schema(records)
        .map_err(|e| FaucetError::Transform(format!("sql transform: {e}")))
}

/// Encode a slice of JSON records into a single [`RecordBatch`] against `schema`.
///
/// Uses the `arrow-json` decoder (`ReaderBuilder → build_decoder → serialize →
/// flush`). Returns an empty batch if `records` is empty.
pub fn json_to_record_batch(
    records: &[Value],
    schema: SchemaRef,
) -> Result<RecordBatch, FaucetError> {
    let mut decoder = arrow_json::ReaderBuilder::new(schema.clone())
        .with_coerce_primitive(true)
        .build_decoder()
        .map_err(|e| te("decoder build", e))?;
    decoder.serialize(records).map_err(|e| te("encode", e))?;
    let mut batches = Vec::new();
    while let Some(b) = decoder.flush().map_err(|e| te("flush", e))? {
        batches.push(b);
    }
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }
    if batches.len() == 1 {
        return Ok(batches.pop().unwrap());
    }
    arrow::compute::concat_batches(&schema, &batches).map_err(|e| te("concat", e))
}

/// Decode one or more [`RecordBatch`]es into JSON objects (one per row).
///
/// Explicit nulls are kept (`"key": null`), so `SELECT *` never deletes an
/// explicit-null field (audit #321 H6). Decimals are exact: a `DECIMAL` whose
/// precision is at most [`EXACT_F64_DIGITS`] stays a JSON number, an integral
/// `DECIMAL`/`HUGEINT` value that fits a 64-bit integer becomes a JSON
/// integer, and anything wider is an exact decimal string — never an `f64`
/// rounding. An empty input returns an empty `Vec`.
pub fn record_batches_to_json(batches: &[RecordBatch]) -> Result<Vec<Value>, FaucetError> {
    let mut rows = Vec::new();
    for batch in batches {
        let decimals = decimal_columns(batch.schema_ref());
        let batch = integral_decimals_as_text(batch)?;
        let mut part = faucet_core::columnar::record_batch_to_values(&batch)
            .map_err(|e| te("json write", e))?;
        if !decimals.is_empty() {
            for row in &mut part {
                for (name, precision, scale) in &decimals {
                    if let Some(v) = row.get_mut(name.as_str()) {
                        renumber_decimal(v, *precision, *scale);
                    }
                }
            }
        }
        rows.append(&mut part);
    }
    Ok(rows)
}

/// Render integral (`scale <= 0`) 128/256-bit decimal columns — `HUGEINT`
/// among them — as text from their raw integers: a `HUGEINT` can carry 39
/// digits, one more than `DECIMAL(38, 0)` formatting keeps.
fn integral_decimals_as_text(batch: &RecordBatch) -> Result<RecordBatch, FaucetError> {
    use arrow::array::{Array, ArrayRef, AsArray, StringArray};
    use arrow::datatypes::{Decimal128Type, Decimal256Type};
    let schema = batch.schema();
    if !schema.fields().iter().any(|f| {
        matches!(f.data_type(), DataType::Decimal128(_, s) | DataType::Decimal256(_, s) if *s <= 0)
    }) {
        return Ok(batch.clone());
    }
    let scaled = |digits: String, scale: i8| {
        if scale < 0 && digits != "0" {
            format!("{digits}{}", "0".repeat(scale.unsigned_abs() as usize))
        } else {
            digits
        }
    };
    let mut fields = Vec::with_capacity(schema.fields().len());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    for (f, col) in schema.fields().iter().zip(batch.columns()) {
        let text: Option<StringArray> = match f.data_type() {
            DataType::Decimal128(_, s) if *s <= 0 => Some(
                col.as_primitive::<Decimal128Type>()
                    .iter()
                    .map(|v| v.map(|x| scaled(x.to_string(), *s)))
                    .collect(),
            ),
            DataType::Decimal256(_, s) if *s <= 0 => Some(
                col.as_primitive::<Decimal256Type>()
                    .iter()
                    .map(|v| v.map(|x| scaled(x.to_string(), *s)))
                    .collect(),
            ),
            _ => None,
        };
        match text {
            Some(t) => {
                fields.push(Field::new(
                    f.name(),
                    DataType::Utf8,
                    f.is_nullable() || t.null_count() > 0,
                ));
                columns.push(Arc::new(t));
            }
            None => {
                fields.push(f.as_ref().clone());
                columns.push(col.clone());
            }
        }
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(|e| te("decimal text", e))
}

/// Significant digits every `f64` round-trips exactly through its shortest text.
pub const EXACT_F64_DIGITS: u8 = 15;

fn decimal_columns(schema: &Schema) -> Vec<(String, u8, i8)> {
    schema
        .fields()
        .iter()
        .filter_map(|f| match f.data_type() {
            DataType::Decimal32(p, s)
            | DataType::Decimal64(p, s)
            | DataType::Decimal128(p, s)
            | DataType::Decimal256(p, s) => Some((f.name().clone(), *p, *s)),
            _ => None,
        })
        .collect()
}

/// Turn an exact decimal string back into a JSON number when that is lossless.
fn renumber_decimal(v: &mut Value, precision: u8, scale: i8) {
    let Value::String(text) = v else { return };
    let number = if scale <= 0 {
        text.parse::<i64>()
            .map(Value::from)
            .or_else(|_| text.parse::<u64>().map(Value::from))
            .ok()
    } else if precision <= EXACT_F64_DIGITS {
        text.parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number)
    } else {
        None
    };
    if let Some(n) = number {
        *v = n;
    }
}

/// Refuse field names that differ only by letter case: DuckDB identifiers are
/// case-insensitive, so it would silently rename the second one (`A` → `A_1`).
pub fn refuse_case_collisions(schema: &Schema) -> Result<(), FaucetError> {
    check_case_collisions(schema.fields(), "")
}

fn check_case_collisions(fields: &Fields, path: &str) -> Result<(), FaucetError> {
    let mut seen: std::collections::HashMap<String, &str> = std::collections::HashMap::new();
    for f in fields {
        if let Some(prev) = seen.insert(f.name().to_lowercase(), f.name()) {
            return Err(FaucetError::Transform(format!(
                "sql transform: fields '{path}{prev}' and '{path}{}' differ only by letter \
                 case; SQL identifiers are case-insensitive, so DuckDB would rename one of \
                 them — rename one upstream (e.g. with a rename_field transform)",
                f.name()
            )));
        }
        let child = format!("{path}{}.", f.name());
        check_nested(f.data_type(), &child)?;
    }
    Ok(())
}

fn check_nested(dt: &DataType, path: &str) -> Result<(), FaucetError> {
    match dt {
        DataType::Struct(fields) => check_case_collisions(fields, path),
        DataType::List(item) | DataType::LargeList(item) => check_nested(item.data_type(), path),
        _ => Ok(()),
    }
}

/// Replace every zero-field struct type (inferred from a field that is `{}` in
/// every record) with `Utf8`, rewriting those values to the string `"{}"` —
/// DuckDB cannot register a struct with no fields. Returns the rewritten
/// schema, or `None` when the schema has none.
pub fn stringify_empty_structs(schema: &Schema, records: &mut [Value]) -> Option<SchemaRef> {
    if !schema
        .fields()
        .iter()
        .any(|f| has_empty_struct(f.data_type()))
    {
        return None;
    }
    for r in records.iter_mut() {
        if let Some(o) = r.as_object_mut() {
            for f in schema.fields() {
                if let Some(v) = o.get_mut(f.name().as_str()) {
                    rewrite_empty(f.data_type(), v, false);
                }
            }
        }
    }
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| {
            f.as_ref()
                .clone()
                .with_data_type(without_empty(f.data_type()))
        })
        .collect();
    Some(Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}

/// Restore `{}` in output columns that [`stringify_empty_structs`] rewrote,
/// matched by top-level name against the original (pre-rewrite) schema.
pub fn restore_empty_structs(original: &Schema, rows: &mut [Value]) {
    let affected: Vec<&Field> = original
        .fields()
        .iter()
        .filter(|f| has_empty_struct(f.data_type()))
        .map(|f| f.as_ref())
        .collect();
    if affected.is_empty() {
        return;
    }
    for r in rows.iter_mut() {
        if let Some(o) = r.as_object_mut() {
            for f in &affected {
                if let Some(v) = o.get_mut(f.name().as_str()) {
                    rewrite_empty(f.data_type(), v, true);
                }
            }
        }
    }
}

fn is_empty_struct(dt: &DataType) -> bool {
    matches!(dt, DataType::Struct(f) if f.is_empty())
}

fn has_empty_struct(dt: &DataType) -> bool {
    match dt {
        DataType::Struct(fields) => {
            fields.is_empty() || fields.iter().any(|f| has_empty_struct(f.data_type()))
        }
        DataType::List(item) | DataType::LargeList(item) => has_empty_struct(item.data_type()),
        _ => false,
    }
}

fn without_empty(dt: &DataType) -> DataType {
    match dt {
        d if is_empty_struct(d) => DataType::Utf8,
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|f| {
                    f.as_ref()
                        .clone()
                        .with_data_type(without_empty(f.data_type()))
                })
                .collect::<Vec<_>>()
                .into(),
        ),
        DataType::List(item) => DataType::List(Arc::new(
            item.as_ref()
                .clone()
                .with_data_type(without_empty(item.data_type())),
        )),
        DataType::LargeList(item) => DataType::LargeList(Arc::new(
            item.as_ref()
                .clone()
                .with_data_type(without_empty(item.data_type())),
        )),
        other => other.clone(),
    }
}

/// Walk `v` along `dt`: forward turns `{}` into `"{}"`, `restore` turns it back.
fn rewrite_empty(dt: &DataType, v: &mut Value, restore: bool) {
    match (dt, &mut *v) {
        (d, Value::Object(o)) if is_empty_struct(d) && !restore && o.is_empty() => {
            *v = Value::String("{}".into());
        }
        (d, Value::String(s)) if is_empty_struct(d) && restore && s == "{}" => {
            *v = Value::Object(Map::new());
        }
        (DataType::Struct(fields), Value::Object(o)) => {
            for f in fields {
                if let Some(child) = o.get_mut(f.name().as_str()) {
                    rewrite_empty(f.data_type(), child, restore);
                }
            }
        }
        (DataType::List(item) | DataType::LargeList(item), Value::Array(items)) => {
            for child in items {
                rewrite_empty(item.data_type(), child, restore);
            }
        }
        _ => {}
    }
}

/// Hidden column listing the fields a record did not carry, so output rows can
/// drop the nulls Arrow had to invent for them (keys a sibling record had).
pub const ABSENT_MARKER: &str = "__faucet_absent";

/// Mark each record with the schema fields it lacks. Returns `false` (and
/// leaves the page untouched) when every record carries every field.
pub fn mark_absent_fields(records: &mut [Value], schema: &Schema) -> bool {
    let absent: Vec<Vec<&str>> = records
        .iter()
        .map(|r| match r.as_object() {
            Some(o) => schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .filter(|n| !o.contains_key(*n))
                .collect(),
            None => Vec::new(),
        })
        .collect();
    if absent.iter().all(Vec::is_empty) {
        return false;
    }
    for (r, missing) in records.iter_mut().zip(absent) {
        if let Some(o) = r.as_object_mut() {
            let v = if missing.is_empty() {
                Value::Null
            } else {
                Value::String(missing.join("\u{1f}"))
            };
            o.insert(ABSENT_MARKER.to_string(), v);
        }
    }
    true
}

/// Remove the [`ABSENT_MARKER`] column from output rows, dropping each listed
/// field that came back `null` (a value the query computed is kept).
pub fn strip_absent_fields(rows: &mut [Value]) {
    for r in rows {
        let Some(o) = r.as_object_mut() else { continue };
        if let Some(Value::String(list)) = o.remove(ABSENT_MARKER) {
            for name in list.split('\u{1f}') {
                if o.get(name).is_some_and(Value::is_null) {
                    o.remove(name);
                }
            }
        }
    }
}

/// Build a [`RecordBatch`] from inline `columns` + `rows` (the `values` relation).
///
/// Each row is zipped with `columns` to produce a JSON object, and the resulting
/// objects are passed through [`infer_schema`] + [`json_to_record_batch`]. A row
/// whose length differs from `columns` is rejected with [`FaucetError::Config`].
pub fn values_to_record_batch(
    columns: &[String],
    rows: &[Vec<Value>],
) -> Result<RecordBatch, FaucetError> {
    let mut objs = Vec::with_capacity(rows.len());
    for (i, row) in rows.iter().enumerate() {
        if row.len() != columns.len() {
            return Err(FaucetError::Config(format!(
                "sql transform: values relation row {i} has {} cells, expected {}",
                row.len(),
                columns.len()
            )));
        }
        let mut m = Map::new();
        for (c, v) in columns.iter().zip(row.iter()) {
            m.insert(c.clone(), v.clone());
        }
        objs.push(Value::Object(m));
    }
    let schema = infer_schema(&objs)?;
    json_to_record_batch(&objs, schema)
}

/// Build a [`RecordBatch`] from a slice of JSON objects (one row each).
///
/// Infers the schema from the records, then encodes them. Used by the `http`
/// reference relation, whose fetched rows are already JSON objects. Every
/// element must be a JSON object; a non-object surfaces as a `FaucetError`.
pub fn records_to_record_batch(records: &[Value]) -> Result<RecordBatch, FaucetError> {
    let schema = infer_schema(records)?;
    json_to_record_batch(records, schema)
}

/// Compare two schemas for field-level equality (name + data-type + nullability).
///
/// Used by the per-page schema cache in the runtime to detect schema drift
/// between pages.
pub fn schema_eq(a: &Schema, b: &Schema) -> bool {
    a.fields() == b.fields()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn round_trip_scalars_nulls_nested() {
        let recs = vec![
            json!({"id": 1, "name": "a", "score": 1.5, "ok": true, "tags": ["x", "y"]}),
            json!({"id": 2, "name": null, "score": null, "ok": false, "tags": []}),
        ];
        let schema = infer_schema(&recs).unwrap();
        let batch = json_to_record_batch(&recs, schema).unwrap();
        assert_eq!(batch.num_rows(), 2);
        let back = record_batches_to_json(&[batch]).unwrap();
        assert_eq!(back[0]["id"], json!(1));
        assert_eq!(back[0]["tags"], json!(["x", "y"]));
        assert_eq!(back[1]["name"], json!(null));
    }

    #[test]
    fn integral_decimals_keep_every_digit_and_negative_scales_expand() {
        use arrow::array::{Array, Decimal128Array, Decimal256Array};
        use arrow::datatypes::i256;
        let huge = Decimal128Array::from(vec![Some(i128::MAX), None, Some(-5)])
            .with_precision_and_scale(38, 0)
            .unwrap();
        let shifted = Decimal128Array::from(vec![Some(12), Some(0), Some(7)])
            .with_precision_and_scale(10, -2)
            .unwrap();
        let wide = Decimal256Array::from(vec![Some(i256::from_i128(3)), None, Some(i256::MAX)])
            .with_precision_and_scale(76, 0)
            .unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("huge", huge.data_type().clone(), true),
            Field::new("shifted", shifted.data_type().clone(), false),
            Field::new("wide", wide.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(huge), Arc::new(shifted), Arc::new(wide)],
        )
        .unwrap();
        let rows = record_batches_to_json(&[batch]).unwrap();
        assert_eq!(rows[0]["huge"], json!(i128::MAX.to_string()));
        assert_eq!(rows[1]["huge"], json!(null));
        assert_eq!(rows[2]["huge"], json!(-5));
        assert_eq!(rows[0]["shifted"], json!(1200));
        assert_eq!(rows[1]["shifted"], json!(0));
        assert_eq!(rows[0]["wide"], json!(3));
        assert_eq!(rows[2]["wide"], json!(i256::MAX.to_string()));
    }

    #[test]
    fn values_to_batch_builds_named_columns() {
        let batch = values_to_record_batch(
            &["id".to_string(), "label".to_string()],
            &[vec![json!(1), json!("NA")], vec![json!(2), json!("EU")]],
        )
        .unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);
        let back = record_batches_to_json(&[batch]).unwrap();
        assert_eq!(back[1]["label"], json!("EU"));
    }

    #[test]
    fn schema_drift_changes_inferred_schema() {
        let a = infer_schema(&[json!({"a": 1})]).unwrap();
        let b = infer_schema(&[json!({"a": 1, "b": "x"})]).unwrap();
        assert_ne!(a.fields().len(), b.fields().len());
    }

    #[test]
    fn round_trip_all_json_value_types() {
        // null, bool, int, float, string, nested object, nested array.
        let recs = vec![json!({
            "n": null,
            "flag": true,
            "count": 7,
            "ratio": 2.5,
            "label": "hello",
            "nested": {"inner": 1, "deep": {"x": "y"}},
            "list": [1, 2, 3]
        })];
        let schema = infer_schema(&recs).unwrap();
        let batch = json_to_record_batch(&recs, schema).unwrap();
        assert_eq!(batch.num_rows(), 1);
        let back = record_batches_to_json(&[batch]).unwrap();
        assert_eq!(back[0]["flag"], json!(true));
        assert_eq!(back[0]["count"], json!(7));
        assert_eq!(back[0]["ratio"], json!(2.5));
        assert_eq!(back[0]["label"], json!("hello"));
        assert_eq!(back[0]["nested"], json!({"inner": 1, "deep": {"x": "y"}}));
        assert_eq!(back[0]["list"], json!([1, 2, 3]));
        // #321 H6: an explicit null-valued field is preserved (not dropped) so
        // `SELECT *` is a true identity transform.
        assert!(
            back[0].as_object().unwrap().contains_key("n"),
            "explicit-null field must be present: {:?}",
            back[0]
        );
        assert_eq!(back[0]["n"], json!(null));
    }

    #[test]
    fn explicit_null_fields_survive_round_trip() {
        // #321 H6: a mixed page where some rows carry a null value. The null
        // must round-trip as `"key": null`, not be silently deleted — otherwise
        // a CDC → upsert pipeline never nulls the column and the mirror diverges.
        let recs = vec![
            json!({"id": 1, "email": "a@x.y"}),
            json!({"id": 2, "email": null}),
        ];
        let schema = infer_schema(&recs).unwrap();
        let batch = json_to_record_batch(&recs, schema).unwrap();
        let back = record_batches_to_json(&[batch]).unwrap();
        assert_eq!(back.len(), 2);
        let row1 = back[1].as_object().unwrap();
        assert!(row1.contains_key("email"), "null email must be present");
        assert_eq!(back[1]["email"], json!(null));
    }

    #[test]
    fn json_to_record_batch_empty_records_yields_empty_batch() {
        // No records → the decoder flushes nothing → empty batch built from schema.
        let schema = infer_schema(&[json!({"a": 1})]).unwrap();
        let batch = json_to_record_batch(&[], schema).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 1);
    }

    #[test]
    fn record_batches_to_json_empty_input_is_empty_vec() {
        let rows = record_batches_to_json(&[]).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn values_relation_row_arity_mismatch_is_config_error() {
        // Two columns declared, second row has only one cell → Config error naming
        // the offending row index and the expected/actual counts.
        let err = values_to_record_batch(
            &["id".to_string(), "label".to_string()],
            &[vec![json!(1), json!("ok")], vec![json!(2)]],
        )
        .unwrap_err();
        assert!(matches!(err, FaucetError::Config(_)), "got: {err:?}");
        let msg = format!("{err}");
        assert!(msg.contains("row 1"), "got: {msg}");
        assert!(msg.contains("1 cells, expected 2"), "got: {msg}");
    }

    #[test]
    fn infer_schema_on_non_object_is_transform_error() {
        // A bare scalar is not a JSON object; arrow-json schema inference rejects
        // it and the `te` helper wraps it as a Transform error.
        let err = infer_schema(&[json!(42)]).unwrap_err();
        assert!(matches!(err, FaucetError::Transform(_)), "got: {err:?}");
        assert!(format!("{err}").contains("sql transform"), "got: {err}");
    }
}
