//! Arrow ↔ Iceberg schema mapping and JSON → Arrow conversion for the Iceberg sink.
//!
//! ## Arrow version note
//!
//! This crate uses **arrow 58**, matching both iceberg-rust 0.10.0 and the
//! workspace — so there is no longer a dual-arrow pin (it was 57 under
//! iceberg-rust 0.10.0). The pipeline still hands the sink `serde_json::Value`
//! records only.
//!
//! ## Function overview
//!
//! | Function | Purpose |
//! |---|---|
//! | [`infer_arrow_schema`] | JSON records → Arrow `SchemaRef` (forced-nullable) |
//! | [`json_to_record_batch`] | `Vec<Value>` + `SchemaRef` → `RecordBatch` |
//! | [`arrow_to_iceberg_schema`] | Arrow `SchemaRef` → `iceberg::spec::Schema` (auto field IDs) |
//! | [`iceberg_to_arrow_schema`] | `iceberg::spec::Schema` → Arrow `SchemaRef` |

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow_json::ReaderBuilder;
use arrow_json::reader::infer_json_schema_from_iterator;
use faucet_core::FaucetError;
use iceberg::spec::{PrimitiveType, Type};
use serde_json::Value;

// ── Public API ────────────────────────────────────────────────────────────────

/// Map a faucet JSON-Schema type fragment (as produced by `infer_schema`, e.g.
/// `{"type":"integer"}` or `{"type":["string","null"]}`) to an Iceberg
/// primitive [`Type`], for additive schema evolution (#255). Only the scalar
/// types faucet infers are supported; `object` / `array` / unknown fragments
/// error rather than guess a nested Iceberg type.
pub(crate) fn json_fragment_to_iceberg_type(fragment: &Value) -> Result<Type, FaucetError> {
    let base = match fragment.get("type") {
        Some(Value::String(s)) => Some(s.as_str()),
        // A nullable union like `["integer","null"]` — take the non-null member.
        Some(Value::Array(arr)) => arr.iter().filter_map(|v| v.as_str()).find(|s| *s != "null"),
        _ => None,
    };
    let prim = match base {
        Some("string") => PrimitiveType::String,
        Some("integer") => PrimitiveType::Long,
        Some("number") => PrimitiveType::Double,
        Some("boolean") => PrimitiveType::Boolean,
        other => {
            return Err(FaucetError::Sink(format!(
                "iceberg: cannot add a column of JSON-Schema type {other:?} — additive schema \
                 evolution supports string / integer / number / boolean columns"
            )));
        }
    };
    Ok(Type::Primitive(prim))
}

/// Infer an Arrow schema from up to `sample` JSON records.
///
/// Every inferred field is forced to be nullable so missing keys in later
/// batches don't cause type errors. Non-object values in the sample are skipped.
/// Returns `FaucetError::Sink` when the sample is empty or yields no object records.
pub fn infer_arrow_schema(records: &[Value], sample: usize) -> Result<SchemaRef, FaucetError> {
    if records.is_empty() {
        return Err(FaucetError::Sink(
            "iceberg: cannot infer schema — sample is empty".to_string(),
        ));
    }

    let take = sample.min(records.len());
    let iter = records
        .iter()
        .take(take)
        .filter(|v| v.is_object())
        .map(Ok::<&Value, ArrowError>);

    let raw = infer_json_schema_from_iterator(iter)
        .map_err(|e| FaucetError::Sink(format!("iceberg: schema inference failed: {e}")))?;

    if raw.fields().is_empty() {
        return Err(FaucetError::Sink(
            "iceberg: cannot infer schema — no object records in sample".to_string(),
        ));
    }

    Ok(Arc::new(force_nullable(raw)))
}

/// Decode `records` into an Arrow `RecordBatch` using the provided schema.
///
/// Uses the arrow-json 57 `Decoder` API:
/// 1. `ReaderBuilder::new(schema).build_decoder()?`
/// 2. `decoder.serialize(records)?`
/// 3. `decoder.flush()?.ok_or(...)`
///
/// Returns `FaucetError::Sink` on any conversion failure.
pub fn json_to_record_batch(
    records: &[Value],
    schema: &SchemaRef,
) -> Result<RecordBatch, FaucetError> {
    let mut decoder = ReaderBuilder::new(schema.clone())
        .build_decoder()
        .map_err(|e| FaucetError::Sink(format!("iceberg: failed to build JSON decoder: {e}")))?;

    decoder.serialize(records).map_err(|e| {
        FaucetError::Sink(format!(
            "iceberg: failed to serialize records to Arrow: {e}"
        ))
    })?;

    decoder
        .flush()
        .map_err(|e| FaucetError::Sink(format!("iceberg: Arrow decoder flush failed: {e}")))?
        .ok_or_else(|| {
            FaucetError::Sink(
                "iceberg: Arrow decoder returned no batch after serializing non-empty records"
                    .to_string(),
            )
        })
}

/// Convert an Arrow `SchemaRef` to an Iceberg `Schema`, auto-assigning field IDs.
///
/// Uses `iceberg::arrow::arrow_schema_to_schema_auto_assign_ids`, which assigns
/// stable integer field IDs starting from 1. This is the correct function for
/// inferred schemas that don't originate from an existing Iceberg table.
///
/// Returns `FaucetError::Config` on type-mapping failures (e.g. unsupported
/// Arrow types).
pub fn arrow_to_iceberg_schema(schema: &SchemaRef) -> Result<iceberg::spec::Schema, FaucetError> {
    iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(schema).map_err(|e| {
        FaucetError::Config(format!(
            "iceberg: Arrow→Iceberg schema conversion failed: {e}"
        ))
    })
}

/// Convert an Iceberg `Schema` to an Arrow `SchemaRef`.
///
/// Used when building `RecordBatch`es against an existing table so the Arrow
/// types align with the table's stored schema.
///
/// Returns `FaucetError::Config` on mapping failures.
pub fn iceberg_to_arrow_schema(schema: &iceberg::spec::Schema) -> Result<SchemaRef, FaucetError> {
    iceberg::arrow::schema_to_arrow_schema(schema)
        .map(Arc::new)
        .map_err(|e| {
            FaucetError::Config(format!(
                "iceberg: Iceberg→Arrow schema conversion failed: {e}"
            ))
        })
}

/// Convert an Arrow `Schema` to an `infer_schema`-shaped JSON object
/// (`{"type":"object","properties":{ <col>: <type-fragment>, … }}`).
///
/// Used by the schema-drift policy (issue #194) to diff a page's shape against
/// the live Iceberg table schema. Each top-level field maps to a JSON base type;
/// nullable fields carry `{"type":[base,"null"]}`, non-nullable fields
/// `{"type":base}`. Only the top level is described — nested struct/list fields
/// collapse to `object` / `array`.
pub fn arrow_to_json_schema(schema: &Schema) -> Value {
    let mut props = serde_json::Map::new();
    for field in schema.fields() {
        let base = arrow_base_type(field.data_type());
        let fragment = if field.is_nullable() {
            serde_json::json!({ "type": [base, "null"] })
        } else {
            serde_json::json!({ "type": base })
        };
        props.insert(field.name().clone(), fragment);
    }
    serde_json::json!({ "type": "object", "properties": Value::Object(props) })
}

/// Map a top-level Arrow `DataType` to its `infer_schema` JSON base keyword.
fn arrow_base_type(dt: &DataType) -> &'static str {
    match dt {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => "integer",
        DataType::Float16 | DataType::Float32 | DataType::Float64 => "number",
        DataType::Boolean => "boolean",
        DataType::Utf8 | DataType::LargeUtf8 => "string",
        DataType::Struct(_) => "object",
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => "array",
        _ => "string",
    }
}

// ── Internal helpers ──────────────────────────────────────────────────────────

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

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use serde_json::json;

    // ── json_fragment_to_iceberg_type (additive schema evolution, #255) ─────────

    fn prim(frag: serde_json::Value) -> PrimitiveType {
        match json_fragment_to_iceberg_type(&frag).unwrap() {
            Type::Primitive(p) => p,
            other => panic!("expected primitive, got {other:?}"),
        }
    }

    #[test]
    fn json_fragment_maps_scalar_types() {
        assert_eq!(prim(json!({"type": "string"})), PrimitiveType::String);
        assert_eq!(prim(json!({"type": "integer"})), PrimitiveType::Long);
        assert_eq!(prim(json!({"type": "number"})), PrimitiveType::Double);
        assert_eq!(prim(json!({"type": "boolean"})), PrimitiveType::Boolean);
    }

    #[test]
    fn json_fragment_unwraps_nullable_union() {
        assert_eq!(
            prim(json!({"type": ["integer", "null"]})),
            PrimitiveType::Long
        );
        assert_eq!(
            prim(json!({"type": ["null", "string"]})),
            PrimitiveType::String
        );
    }

    #[test]
    fn json_fragment_rejects_unsupported_types() {
        for frag in [
            json!({"type": "object"}),
            json!({"type": "array"}),
            json!({"type": "null"}),
            json!({}),
        ] {
            let err = json_fragment_to_iceberg_type(&frag).unwrap_err();
            assert!(matches!(err, FaucetError::Sink(_)), "{frag}: {err:?}");
        }
    }

    // ── infer_arrow_schema ────────────────────────────────────────────────────

    #[test]
    fn infers_primitive_fields() {
        let records = vec![
            json!({"id": 1, "name": "alice", "active": true, "score": 1.5}),
            json!({"id": 2, "name": "bob",   "active": false, "score": 2.0}),
        ];
        let schema = infer_arrow_schema(&records, 10).unwrap();
        let fields: std::collections::HashMap<_, _> = schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type().clone()))
            .collect();
        assert_eq!(fields.get("id"), Some(&DataType::Int64));
        assert_eq!(fields.get("name"), Some(&DataType::Utf8));
        assert_eq!(fields.get("active"), Some(&DataType::Boolean));
        assert_eq!(fields.get("score"), Some(&DataType::Float64));
        for f in schema.fields() {
            assert!(f.is_nullable(), "{} should be nullable", f.name());
        }
    }

    #[test]
    fn infers_nested_struct() {
        let records = vec![json!({"meta": {"a": 1, "b": "z"}})];
        let schema = infer_arrow_schema(&records, 10).unwrap();
        let meta = schema.field_with_name("meta").unwrap();
        match meta.data_type() {
            DataType::Struct(fields) => {
                assert_eq!(fields.len(), 2);
                for f in fields {
                    assert!(f.is_nullable(), "nested {} must be nullable", f.name());
                }
            }
            other => panic!("expected Struct, got {other:?}"),
        }
    }

    #[test]
    fn infers_list_with_nullable_element() {
        let records = vec![json!({"tags": ["a", "b"]})];
        let schema = infer_arrow_schema(&records, 10).unwrap();
        let tags = schema.field_with_name("tags").unwrap();
        assert!(tags.is_nullable(), "list field must be nullable");
        match tags.data_type() {
            DataType::List(inner) | DataType::LargeList(inner) => {
                assert!(inner.is_nullable(), "list element must be forced nullable");
            }
            other => panic!("expected List, got {other:?}"),
        }
    }

    #[test]
    fn empty_sample_errors() {
        let err = infer_arrow_schema(&[], 10).unwrap_err();
        assert!(matches!(err, FaucetError::Sink(_)));
    }

    #[test]
    fn non_object_only_sample_errors() {
        let records = vec![json!(1), json!("foo")];
        let err = infer_arrow_schema(&records, 10).unwrap_err();
        assert!(matches!(err, FaucetError::Sink(_)));
    }

    // ── json_to_record_batch ──────────────────────────────────────────────────

    #[test]
    fn json_to_record_batch_right_row_count() {
        let records = vec![
            json!({"id": 1, "name": "alice"}),
            json!({"id": 2, "name": "bob"}),
            json!({"id": 3, "name": "carol"}),
        ];
        let schema = infer_arrow_schema(&records, 10).unwrap();
        let batch = json_to_record_batch(&records, &schema).unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn json_to_record_batch_single_record() {
        let records = vec![json!({"x": 42})];
        let schema = infer_arrow_schema(&records, 10).unwrap();
        let batch = json_to_record_batch(&records, &schema).unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    // ── arrow_to_iceberg_schema ───────────────────────────────────────────────

    #[test]
    fn arrow_to_iceberg_assigns_field_ids() {
        let records = vec![json!({"id": 1, "name": "alice"})];
        let arrow_schema = infer_arrow_schema(&records, 10).unwrap();
        let iceberg_schema = arrow_to_iceberg_schema(&arrow_schema).unwrap();

        // All fields should have been assigned positive IDs.
        let fields = iceberg_schema.as_struct().fields();
        assert!(!fields.is_empty(), "should have at least one field");
        for field in fields {
            assert!(
                field.id >= 1,
                "field '{}' should have field_id >= 1, got {}",
                field.name,
                field.id
            );
        }
    }

    #[test]
    fn arrow_to_iceberg_field_ids_are_unique() {
        let records = vec![json!({"a": 1, "b": "x", "c": true})];
        let arrow_schema = infer_arrow_schema(&records, 10).unwrap();
        let iceberg_schema = arrow_to_iceberg_schema(&arrow_schema).unwrap();

        let mut ids: Vec<_> = iceberg_schema
            .as_struct()
            .fields()
            .iter()
            .map(|f| f.id)
            .collect();
        let before = ids.len();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), before, "field IDs must be unique");
    }

    // ── iceberg_to_arrow_schema ───────────────────────────────────────────────

    #[test]
    fn iceberg_to_arrow_round_trip_field_names() {
        let records = vec![json!({"col_a": 1, "col_b": "hello"})];
        let arrow_schema = infer_arrow_schema(&records, 10).unwrap();
        let iceberg_schema = arrow_to_iceberg_schema(&arrow_schema).unwrap();
        let back = iceberg_to_arrow_schema(&iceberg_schema).unwrap();

        let names: std::collections::HashSet<_> =
            back.fields().iter().map(|f| f.name().clone()).collect();
        assert!(names.contains("col_a"), "col_a must survive round-trip");
        assert!(names.contains("col_b"), "col_b must survive round-trip");
    }

    // ── arrow_to_json_schema ──────────────────────────────────────────────────

    #[test]
    fn arrow_schema_to_json_schema_top_level() {
        let fields = vec![
            Field::new("i32", DataType::Int32, false),
            Field::new("i64", DataType::Int64, true),
            Field::new("u16", DataType::UInt16, true),
            Field::new("f32", DataType::Float32, true),
            Field::new("f64", DataType::Float64, false),
            Field::new("flag", DataType::Boolean, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("big_name", DataType::LargeUtf8, false),
            Field::new(
                "meta",
                DataType::Struct(vec![Field::new("a", DataType::Int64, true)].into()),
                true,
            ),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            // Unknown/unmapped type falls through to "string".
            Field::new("ts", DataType::Date32, true),
        ];
        let schema = Schema::new(fields);
        let json = arrow_to_json_schema(&schema);

        assert_eq!(json["type"], "object");
        let props = json["properties"].as_object().unwrap();

        // Non-nullable → bare base type.
        assert_eq!(props["i32"]["type"], "integer");
        assert_eq!(props["f64"]["type"], "number");
        assert_eq!(props["big_name"]["type"], "string");
        // Nullable → [base, "null"].
        assert_eq!(props["i64"]["type"], serde_json::json!(["integer", "null"]));
        assert_eq!(props["u16"]["type"], serde_json::json!(["integer", "null"]));
        assert_eq!(props["f32"]["type"], serde_json::json!(["number", "null"]));
        assert_eq!(
            props["flag"]["type"],
            serde_json::json!(["boolean", "null"])
        );
        assert_eq!(props["name"]["type"], serde_json::json!(["string", "null"]));
        assert_eq!(props["meta"]["type"], serde_json::json!(["object", "null"]));
        assert_eq!(props["tags"]["type"], serde_json::json!(["array", "null"]));
        // Unmapped Arrow type → "string".
        assert_eq!(props["ts"]["type"], serde_json::json!(["string", "null"]));
    }
}
