//! JSON ↔ Arrow conversion. `arrow-json` for both directions; `arrow` for
//! concatenation. All errors surface as `FaucetError::Transform` (or
//! `FaucetError::Config` for arity mismatches in the inline `values` relation).

use arrow::array::RecordBatch;
use arrow::datatypes::{Schema, SchemaRef};
use faucet_core::FaucetError;
use serde_json::{Map, Value};

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
/// Uses `arrow-json`'s array writer with **explicit nulls enabled**, so a
/// null-valued column is emitted as `"key": null` rather than omitted from the
/// object. Without this, `SELECT * FROM batch` silently deletes every
/// explicit-null field — e.g. a CDC update `{"id":1,"email":null}` loses
/// `email`, so a downstream upsert never nulls the column and the mirror
/// diverges from the source (audit #321 H6). An empty input returns an empty
/// `Vec`.
pub fn record_batches_to_json(batches: &[RecordBatch]) -> Result<Vec<Value>, FaucetError> {
    let mut buf = Vec::new();
    {
        let mut writer = arrow_json::writer::WriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, arrow_json::writer::JsonArray>(&mut buf);
        for b in batches {
            writer.write(b).map_err(|e| te("json write", e))?;
        }
        writer.finish().map_err(|e| te("json finish", e))?;
    }
    let rows: Vec<Value> = serde_json::from_slice(&buf).map_err(|e| te("json parse", e))?;
    Ok(rows)
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
