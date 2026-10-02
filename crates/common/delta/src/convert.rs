//! Arrow ⇆ JSON conversion shared by the Delta source and sink.
//!
//! * [`record_batch_to_json`] — Arrow `RecordBatch` → `Vec<serde_json::Value>`
//!   (the read path). Delegates to `arrow_json::ArrayWriter`, which already
//!   encodes every Arrow logical type.
//! * [`infer_arrow_schema`] — a nullable-everywhere Arrow schema inferred from
//!   a sample of JSON records (the write path, when creating a table / decoding
//!   a batch).

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow_json::ArrayWriter;
use faucet_core::FaucetError;
use serde_json::Value;

/// Encode a single Arrow `RecordBatch` as `Vec<serde_json::Value>`, one JSON
/// object per row. An empty batch yields an empty vec.
pub fn record_batch_to_json(batch: &RecordBatch) -> Result<Vec<Value>, FaucetError> {
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }

    let mut buf: Vec<u8> = Vec::with_capacity(batch.num_rows() * 64);
    {
        let mut writer = ArrayWriter::new(&mut buf);
        writer
            .write(batch)
            .map_err(|e| FaucetError::Source(format!("delta: arrow_json encode error: {e}")))?;
        writer
            .finish()
            .map_err(|e| FaucetError::Source(format!("delta: arrow_json finish error: {e}")))?;
    }

    let parsed: Value = serde_json::from_slice(&buf)
        .map_err(|e| FaucetError::Source(format!("delta: arrow_json output parse error: {e}")))?;

    match parsed {
        Value::Array(rows) => Ok(rows),
        other => Err(FaucetError::Source(format!(
            "delta: arrow_json produced non-array output: {other}"
        ))),
    }
}

/// Infer a fully-nullable Arrow schema from up to `sample_size` JSON records.
///
/// Every field (including nested struct/list fields) is forced nullable —
/// the sink is intentionally forgiving about missing keys, and Delta append
/// semantics require the physical parquet schema to tolerate nulls.
pub fn infer_arrow_schema(records: &[Value], sample_size: usize) -> Result<SchemaRef, FaucetError> {
    if records.is_empty() {
        return Err(FaucetError::Sink(
            "delta: cannot infer schema — record sample is empty".to_string(),
        ));
    }

    let take = sample_size.min(records.len());
    let iter = records
        .iter()
        .take(take)
        .filter(|v| v.is_object())
        .map(Ok::<&Value, ArrowError>);

    let raw = arrow_json::reader::infer_json_schema_from_iterator(iter)
        .map_err(|e| FaucetError::Sink(format!("delta: schema inference failed: {e}")))?;

    if raw.fields().is_empty() {
        return Err(FaucetError::Sink(
            "delta: cannot infer schema — no object records in sample".to_string(),
        ));
    }

    Ok(Arc::new(force_nullable(raw)))
}

/// [`infer_arrow_schema`] in the Arrow version `deltalake` writes with — the
/// schema the Delta sink creates tables and decodes batches against.
pub fn infer_delta_schema(
    records: &[Value],
    sample_size: usize,
) -> Result<deltalake::arrow::datatypes::SchemaRef, FaucetError> {
    crate::arrow_bridge::schema_to_delta(infer_arrow_schema(records, sample_size)?.as_ref())
}

/// Recursively force every field in the schema to be nullable.
fn force_nullable(schema: Schema) -> Schema {
    let metadata = schema.metadata.clone();
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| make_nullable(f.as_ref()))
        .collect();
    Schema::new_with_metadata(fields, metadata)
}

fn make_nullable(field: &Field) -> Field {
    let data_type = match field.data_type() {
        DataType::Struct(fields) => {
            let nullable_fields: Vec<Field> =
                fields.iter().map(|f| make_nullable(f.as_ref())).collect();
            DataType::Struct(nullable_fields.into())
        }
        DataType::List(inner) => DataType::List(Arc::new(make_nullable(inner.as_ref()))),
        DataType::LargeList(inner) => DataType::LargeList(Arc::new(make_nullable(inner.as_ref()))),
        other => other.clone(),
    };
    Field::new(field.name(), data_type, true).with_metadata(field.metadata().clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use serde_json::json;

    #[test]
    fn empty_batch_is_empty_vec() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(Vec::<i32>::new()))])
                .unwrap();
        assert!(record_batch_to_json(&batch).unwrap().is_empty());
    }

    #[test]
    fn batch_round_trips_to_objects() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("Alice"), None])),
            ],
        )
        .unwrap();
        let rows = record_batch_to_json(&batch).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["id"], 1);
        assert_eq!(rows[0]["name"], "Alice");
        assert_eq!(rows[1]["id"], 2);
        assert!(rows[1].get("name").is_none() || rows[1]["name"].is_null());
    }

    #[test]
    fn infers_primitives_all_nullable() {
        let records = vec![
            json!({"id": 1, "name": "a", "ok": true, "score": 1.5}),
            json!({"id": 2, "name": "b", "ok": false, "score": 2.0}),
        ];
        let schema = infer_arrow_schema(&records, 10).unwrap();
        assert_eq!(
            schema.field_with_name("id").unwrap().data_type(),
            &DataType::Int64
        );
        assert_eq!(
            schema.field_with_name("score").unwrap().data_type(),
            &DataType::Float64
        );
        for f in schema.fields() {
            assert!(f.is_nullable(), "{} must be nullable", f.name());
        }
    }

    #[test]
    fn infer_promotes_int_to_float() {
        let records = vec![json!({"x": 1}), json!({"x": 1.5})];
        let schema = infer_arrow_schema(&records, 10).unwrap();
        assert_eq!(
            schema.field_with_name("x").unwrap().data_type(),
            &DataType::Float64
        );
    }

    #[test]
    fn delta_schema_matches_workspace_inference() {
        let records = vec![json!({"id": 1, "meta": {"a": "z"}, "tags": [1.5]})];
        let delta = infer_delta_schema(&records, 10).unwrap();
        let workspace = infer_arrow_schema(&records, 10).unwrap();
        assert_eq!(
            crate::arrow_bridge::schema_from_delta(&delta).unwrap(),
            workspace
        );
        assert!(delta.fields().iter().all(|f| f.is_nullable()));
        assert!(infer_delta_schema(&[], 10).is_err());
    }

    #[test]
    fn infer_empty_sample_errors() {
        assert!(infer_arrow_schema(&[], 10).is_err());
        assert!(infer_arrow_schema(&[json!("scalar")], 10).is_err());
    }

    #[test]
    fn infer_nested_struct_and_list_forced_nullable() {
        let records = vec![json!({"meta": {"a": 1, "b": "z"}, "tags": ["x", "y"]})];
        let schema = infer_arrow_schema(&records, 10).unwrap();
        match schema.field_with_name("meta").unwrap().data_type() {
            DataType::Struct(fs) => assert!(fs.iter().all(|f| f.is_nullable())),
            other => panic!("expected struct, got {other:?}"),
        }
        let tags = schema.field_with_name("tags").unwrap();
        assert!(matches!(tags.data_type(), DataType::List(_)));
        assert!(tags.is_nullable());
    }
}
