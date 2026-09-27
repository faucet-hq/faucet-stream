//! Pure conversions: Arrow batches to JSON rows, Iceberg schemas to JSON
//! Schema, discovery descriptors, and the delete-file support check.

use std::collections::HashSet;

use arrow::array::RecordBatch;
use faucet_core::FaucetError;
use faucet_core::discover::{DatasetDescriptor, columns_to_schema, nullable_type};
use faucet_core::shard::HashShard;
use iceberg::scan::FileScanTask;
use iceberg::spec::{DataContentType, PrimitiveType, Schema, Type};
use serde_json::{Value, json};

/// Decode a batch into one JSON object per row, keeping explicit nulls.
pub fn batch_to_rows(batch: &RecordBatch) -> Result<Vec<Value>, FaucetError> {
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }
    let mut buf = Vec::new();
    let mut writer = arrow_json::writer::WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, arrow_json::writer::JsonArray>(&mut buf);
    writer
        .write(batch)
        .and_then(|_| writer.finish())
        .map_err(|e| FaucetError::Source(format!("iceberg: encoding rows as JSON failed: {e}")))?;
    drop(writer);
    serde_json::from_slice(&buf)
        .map_err(|e| FaucetError::Source(format!("iceberg: decoding JSON rows failed: {e}")))
}

/// JSON Schema fragment for an Iceberg type (matching the row encoding).
pub fn type_to_json_schema(ty: &Type) -> Value {
    match ty {
        Type::Primitive(p) => match p {
            PrimitiveType::Boolean => json!({ "type": "boolean" }),
            PrimitiveType::Int | PrimitiveType::Long => json!({ "type": "integer" }),
            PrimitiveType::Float | PrimitiveType::Double | PrimitiveType::Decimal { .. } => {
                json!({ "type": "number" })
            }
            PrimitiveType::Date => json!({ "type": "string", "format": "date" }),
            PrimitiveType::Time => json!({ "type": "string", "format": "time" }),
            PrimitiveType::Timestamp
            | PrimitiveType::Timestamptz
            | PrimitiveType::TimestampNs
            | PrimitiveType::TimestamptzNs => json!({ "type": "string", "format": "date-time" }),
            PrimitiveType::Uuid => json!({ "type": "string", "format": "uuid" }),
            _ => json!({ "type": "string" }),
        },
        Type::Struct(s) => columns_to_schema(
            s.fields()
                .iter()
                .map(|f| (f.name.clone(), field_schema(f.required, &f.field_type))),
        ),
        Type::List(l) => json!({
            "type": "array",
            "items": field_schema(l.element_field.required, &l.element_field.field_type),
        }),
        Type::Map(m) => json!({
            "type": "object",
            "additionalProperties": field_schema(m.value_field.required, &m.value_field.field_type),
        }),
    }
}

fn field_schema(required: bool, ty: &Type) -> Value {
    let fragment = type_to_json_schema(ty);
    if required {
        fragment
    } else {
        nullable_type(fragment)
    }
}

/// `infer_schema`-shaped JSON Schema for a table schema.
pub fn schema_to_json_schema(schema: &Schema) -> Value {
    columns_to_schema(
        schema
            .as_struct()
            .fields()
            .iter()
            .map(|f| (f.name.clone(), field_schema(f.required, &f.field_type))),
    )
}

/// One discovery descriptor for a table.
pub fn descriptor(
    namespace: &[String],
    table: &str,
    schema: &Schema,
    total_records: Option<u64>,
) -> DatasetDescriptor {
    let name = format!("{}.{table}", namespace.join("."));
    let mut d = DatasetDescriptor::new(name.clone(), "table", json!({ "table": name }))
        .with_schema(schema_to_json_schema(schema));
    if let Some(rows) = total_records {
        d = d.with_estimated_rows(rows);
    }
    d
}

/// Refuse a scan task whose delete files iceberg-rust cannot apply, so a read
/// never returns rows that were deleted.
pub fn ensure_deletes_supported(task: &FileScanTask) -> Result<(), FaucetError> {
    for d in &task.deletes {
        let parquet = d.file_path.to_ascii_lowercase().ends_with(".parquet");
        let problem = match d.file_type {
            _ if !parquet => Some("is not a Parquet delete file (e.g. a deletion vector)"),
            DataContentType::EqualityDeletes
                if d.equality_ids.as_ref().is_none_or(Vec::is_empty) =>
            {
                Some("is an equality delete without equality field ids")
            }
            DataContentType::Data => Some("is a data file listed as a delete file"),
            _ => None,
        };
        if let Some(problem) = problem {
            return Err(FaucetError::Source(format!(
                "iceberg: data file '{}' has delete file '{}' which {problem}; the source cannot \
                 apply it and refuses to read rather than return deleted rows",
                task.data_file_path, d.file_path
            )));
        }
    }
    Ok(())
}

/// Keep the tasks this read should process: only files added by the snapshot
/// being read incrementally (`added`), only this shard's files.
pub fn select_tasks(
    tasks: Vec<FileScanTask>,
    added: Option<&HashSet<String>>,
    shard: Option<HashShard>,
) -> Result<Vec<FileScanTask>, FaucetError> {
    let mut out = Vec::new();
    for task in tasks {
        if added.is_some_and(|a| !a.contains(&task.data_file_path)) {
            continue;
        }
        if shard.is_some_and(|s| !s.contains(&task.data_file_path)) {
            continue;
        }
        ensure_deletes_supported(&task)?;
        out.push(task);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use iceberg::scan::FileScanTaskDeleteFile;
    use iceberg::spec::{DataFileFormat, ListType, MapType, NestedField, StructType};
    use std::sync::Arc;

    #[test]
    fn rows_keep_explicit_nulls() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("a"), None])),
            ],
        )
        .unwrap();
        assert_eq!(
            batch_to_rows(&batch).unwrap(),
            vec![
                json!({"id": 1, "name": "a"}),
                json!({"id": 2, "name": null})
            ]
        );
        assert!(
            batch_to_rows(&RecordBatch::new_empty(schema))
                .unwrap()
                .is_empty()
        );
    }

    fn table_schema() -> Schema {
        let p = |t| Type::Primitive(t);
        Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(1, "id", p(PrimitiveType::Long))),
                Arc::new(NestedField::optional(2, "flag", p(PrimitiveType::Boolean))),
                Arc::new(NestedField::optional(3, "n", p(PrimitiveType::Int))),
                Arc::new(NestedField::optional(4, "x", p(PrimitiveType::Double))),
                Arc::new(NestedField::optional(
                    5,
                    "dec",
                    p(PrimitiveType::Decimal {
                        precision: 9,
                        scale: 2,
                    }),
                )),
                Arc::new(NestedField::optional(6, "d", p(PrimitiveType::Date))),
                Arc::new(NestedField::optional(7, "t", p(PrimitiveType::Time))),
                Arc::new(NestedField::optional(
                    8,
                    "ts",
                    p(PrimitiveType::Timestamptz),
                )),
                Arc::new(NestedField::optional(9, "u", p(PrimitiveType::Uuid))),
                Arc::new(NestedField::optional(10, "s", p(PrimitiveType::String))),
                Arc::new(NestedField::optional(11, "b", p(PrimitiveType::Binary))),
                Arc::new(NestedField::optional(
                    12,
                    "st",
                    Type::Struct(StructType::new(vec![Arc::new(NestedField::required(
                        13,
                        "k",
                        p(PrimitiveType::String),
                    ))])),
                )),
                Arc::new(NestedField::optional(
                    14,
                    "l",
                    Type::List(ListType::new(Arc::new(NestedField::list_element(
                        15,
                        p(PrimitiveType::Long),
                        false,
                    )))),
                )),
                Arc::new(NestedField::optional(
                    16,
                    "m",
                    Type::Map(MapType::new(
                        Arc::new(NestedField::map_key_element(17, p(PrimitiveType::String))),
                        Arc::new(NestedField::map_value_element(
                            18,
                            p(PrimitiveType::Long),
                            true,
                        )),
                    )),
                )),
            ])
            .build()
            .unwrap()
    }

    #[test]
    fn schema_maps_every_type() {
        let s = schema_to_json_schema(&table_schema());
        let p = &s["properties"];
        assert_eq!(s["type"], "object");
        assert_eq!(p["id"], json!({"type": "integer"}));
        assert_eq!(p["flag"]["type"], json!(["boolean", "null"]));
        assert_eq!(p["n"]["type"], json!(["integer", "null"]));
        assert_eq!(p["x"]["type"], json!(["number", "null"]));
        assert_eq!(p["dec"]["type"], json!(["number", "null"]));
        assert_eq!(p["d"]["format"], "date");
        assert_eq!(p["t"]["format"], "time");
        assert_eq!(p["ts"]["format"], "date-time");
        assert_eq!(p["u"]["format"], "uuid");
        assert_eq!(p["s"]["type"], json!(["string", "null"]));
        assert_eq!(p["b"]["type"], json!(["string", "null"]));
        assert_eq!(p["st"]["properties"]["k"], json!({"type": "string"}));
        assert_eq!(p["l"]["items"]["type"], json!(["integer", "null"]));
        assert_eq!(p["m"]["additionalProperties"], json!({"type": "integer"}));
    }

    #[test]
    fn descriptor_carries_table_patch_and_rows() {
        let d = descriptor(
            &["lake".into(), "db".into()],
            "events",
            &table_schema(),
            Some(12),
        );
        assert_eq!(d.name, "lake.db.events");
        assert_eq!(d.kind, "table");
        assert_eq!(d.config_patch, json!({"table": "lake.db.events"}));
        assert_eq!(d.estimated_rows, Some(12));
        assert!(
            descriptor(&["a".into()], "t", &table_schema(), None)
                .estimated_rows
                .is_none()
        );
    }

    fn task(path: &str, deletes: Vec<FileScanTaskDeleteFile>) -> FileScanTask {
        FileScanTask::builder()
            .with_file_size_in_bytes(1)
            .with_start(0)
            .with_length(1)
            .with_data_file_path(path.to_string())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(Arc::new(table_schema()))
            .with_project_field_ids(vec![1])
            .with_deletes(deletes)
            .with_case_sensitive(true)
            .build()
    }

    fn del(path: &str, ty: DataContentType, ids: Option<Vec<i32>>) -> FileScanTaskDeleteFile {
        FileScanTaskDeleteFile::builder()
            .with_file_path(path.to_string())
            .with_file_size_in_bytes(1)
            .with_file_type(ty)
            .with_partition_spec_id(0)
            .with_equality_ids(ids)
            .build()
    }

    #[test]
    fn delete_support_check() {
        ensure_deletes_supported(&task("a.parquet", vec![])).unwrap();
        ensure_deletes_supported(&task(
            "a.parquet",
            vec![
                del("p.PARQUET", DataContentType::PositionDeletes, None),
                del("e.parquet", DataContentType::EqualityDeletes, Some(vec![1])),
            ],
        ))
        .unwrap();
        for (d, needle) in [
            (
                del("dv.puffin", DataContentType::PositionDeletes, None),
                "not a Parquet",
            ),
            (
                del("e.parquet", DataContentType::EqualityDeletes, None),
                "without equality",
            ),
            (
                del("e.parquet", DataContentType::EqualityDeletes, Some(vec![])),
                "without equality",
            ),
            (
                del("x.parquet", DataContentType::Data, None),
                "data file listed",
            ),
        ] {
            let e = ensure_deletes_supported(&task("a.parquet", vec![d])).unwrap_err();
            assert!(e.to_string().contains(needle), "{e}");
            assert!(e.to_string().contains("refuses"), "{e}");
        }
    }

    #[test]
    fn select_tasks_filters_by_added_set_and_shard() {
        let paths: Vec<String> = (0..20).map(|i| format!("s3://b/f{i}.parquet")).collect();
        let tasks = || paths.iter().map(|p| task(p, vec![])).collect::<Vec<_>>();
        assert_eq!(select_tasks(tasks(), None, None).unwrap().len(), 20);

        let added: HashSet<String> = paths[..3].iter().cloned().collect();
        let kept = select_tasks(tasks(), Some(&added), None).unwrap();
        assert_eq!(kept.len(), 3);

        let shards = 3;
        let mut total = 0;
        for index in 0..shards {
            let got = select_tasks(tasks(), None, Some(HashShard { shards, index })).unwrap();
            total += got.len();
        }
        assert_eq!(total, 20, "hash shards partition the files");

        let bad = vec![task(
            "x.parquet",
            vec![del("dv.puffin", DataContentType::PositionDeletes, None)],
        )];
        assert!(select_tasks(bad, None, None).is_err());
    }
}
