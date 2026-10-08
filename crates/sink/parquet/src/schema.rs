//! Arrow schema inference for the Parquet sink.
//!
//! Wraps `arrow_json::reader::infer_json_schema_from_iterator` so callers
//! never have to construct an `Iterator<Item = Result<&Value, ArrowError>>`
//! by hand, and forces every inferred field to be nullable — the parquet
//! sink is intentionally forgiving about missing keys.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use faucet_core::FaucetError;
use serde_json::Value;

/// Infer an Arrow schema from up to `sample_size` JSON records.
///
/// Returns an error if no usable records are available or if inference fails.
/// Non-object values in the sample are skipped (we don't have a record-shaped
/// thing to learn from).
#[deprecated(
    since = "1.4.0",
    note = "use faucet-sink-file (FileSinkConfig with `format: parquet`), or faucet-sink-s3 for S3 locations"
)]
pub fn infer_schema(records: &[Value], sample_size: usize) -> Result<SchemaRef, FaucetError> {
    if records.is_empty() {
        return Err(FaucetError::Sink(
            "cannot infer parquet schema: sample is empty".to_string(),
        ));
    }

    let take = sample_size.min(records.len());
    let iter = records
        .iter()
        .take(take)
        .filter(|v| v.is_object())
        .map(Ok::<&Value, ArrowError>);

    let raw = arrow_json::reader::infer_json_schema_from_iterator(iter)
        .map_err(|e| FaucetError::Sink(format!("schema inference failed: {e}")))?;

    if raw.fields().is_empty() {
        return Err(FaucetError::Sink(
            "cannot infer parquet schema: no object records in sample".to_string(),
        ));
    }

    Ok(Arc::new(force_nullable(raw)))
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
        // Null in every sampled record: a nullable string column (no format
        // can store a Null-typed one).
        DataType::Null => DataType::Utf8,
        other => other.clone(),
    };
    Field::new(field.name(), data_type, true).with_metadata(field.metadata().clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn infers_primitive_fields() {
        let records = vec![
            json!({"id": 1, "name": "alice", "active": true, "score": 1.5}),
            json!({"id": 2, "name": "bob", "active": false, "score": 2.0}),
        ];
        let schema = infer_schema(&records, 10).unwrap();
        let fields_by_name: std::collections::HashMap<_, _> = schema
            .fields()
            .iter()
            .map(|f| (f.name().to_string(), f.data_type().clone()))
            .collect();

        assert_eq!(fields_by_name.get("id"), Some(&DataType::Int64));
        assert_eq!(fields_by_name.get("name"), Some(&DataType::Utf8));
        assert_eq!(fields_by_name.get("active"), Some(&DataType::Boolean));
        assert_eq!(fields_by_name.get("score"), Some(&DataType::Float64));
        for f in schema.fields() {
            assert!(f.is_nullable(), "{} should be nullable", f.name());
        }
    }

    #[test]
    fn promotes_int_to_float_when_mixed() {
        let records = vec![json!({"x": 1}), json!({"x": 1.5})];
        let schema = infer_schema(&records, 10).unwrap();
        let dt = schema.field_with_name("x").unwrap().data_type();
        assert_eq!(dt, &DataType::Float64);
    }

    #[test]
    fn infers_nested_struct() {
        let records = vec![json!({"meta": {"a": 1, "b": "z"}})];
        let schema = infer_schema(&records, 10).unwrap();
        let meta = schema.field_with_name("meta").unwrap();
        match meta.data_type() {
            DataType::Struct(fields) => {
                assert_eq!(fields.len(), 2);
                for f in fields {
                    assert!(f.is_nullable(), "nested {} must be nullable", f.name());
                }
            }
            other => panic!("expected struct, got {other:?}"),
        }
    }

    #[test]
    fn infers_list_of_primitive() {
        let records = vec![json!({"tags": ["a", "b"]}), json!({"tags": ["c"]})];
        let schema = infer_schema(&records, 10).unwrap();
        let tags = schema.field_with_name("tags").unwrap();
        match tags.data_type() {
            DataType::List(inner) => {
                assert_eq!(inner.data_type(), &DataType::Utf8);
                assert!(inner.is_nullable());
            }
            other => panic!("expected list, got {other:?}"),
        }
    }

    #[test]
    fn empty_sample_errors() {
        let err = infer_schema(&[], 5).unwrap_err();
        assert!(matches!(err, FaucetError::Sink(_)));
    }

    #[test]
    fn sample_with_only_non_objects_errors() {
        let records = vec![json!(1), json!("foo")];
        let err = infer_schema(&records, 5).unwrap_err();
        assert!(matches!(err, FaucetError::Sink(_)));
    }

    #[test]
    fn sample_size_caps_records_considered() {
        let records: Vec<Value> = (0..10)
            .map(|i| {
                if i < 2 {
                    json!({"only_in_early": i})
                } else {
                    json!({"only_late": "ignored"})
                }
            })
            .collect();
        let schema = infer_schema(&records, 2).unwrap();
        assert!(schema.field_with_name("only_in_early").is_ok());
        assert!(schema.field_with_name("only_late").is_err());
    }
}
