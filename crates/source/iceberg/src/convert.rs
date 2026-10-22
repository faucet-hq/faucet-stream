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

/// Decode a batch into one JSON object per row, keeping explicit nulls and
/// every digit of a decimal (as a string).
pub fn batch_to_rows(batch: &RecordBatch) -> Result<Vec<Value>, FaucetError> {
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }
    faucet_core::columnar::record_batch_to_values(batch)
        .map_err(|e| FaucetError::Source(format!("iceberg: encoding rows as JSON failed: {e}")))
}

/// The Arrow field metadata key iceberg-rust stamps every scanned field with.
const FIELD_ID_KEY: &str = "PARQUET:field_id";

/// The `uuid` columns of a table schema. iceberg-rust reads a `uuid` as a
/// 16-byte `FixedSizeBinary`, which a `fixed(16)` column also becomes, so the
/// Iceberg field ids (or, for a top-level field without id metadata, its
/// name) tell them apart.
#[derive(Debug, Default, Clone)]
pub struct UuidColumns {
    ids: HashSet<i32>,
    top_level: HashSet<String>,
}

impl UuidColumns {
    pub fn of(schema: &Schema) -> Self {
        fn walk(ty: &Type, id: i32, out: &mut HashSet<i32>) {
            match ty {
                Type::Primitive(PrimitiveType::Uuid) => {
                    out.insert(id);
                }
                Type::Primitive(_) => {}
                Type::Struct(s) => {
                    for f in s.fields() {
                        walk(&f.field_type, f.id, out);
                    }
                }
                Type::List(l) => walk(&l.element_field.field_type, l.element_field.id, out),
                Type::Map(m) => {
                    walk(&m.key_field.field_type, m.key_field.id, out);
                    walk(&m.value_field.field_type, m.value_field.id, out);
                }
            }
        }
        let mut ids = HashSet::new();
        let mut top_level = HashSet::new();
        for f in schema.as_struct().fields() {
            if matches!(*f.field_type, Type::Primitive(PrimitiveType::Uuid)) {
                top_level.insert(f.name.clone());
            }
            walk(&f.field_type, f.id, &mut ids);
        }
        Self { ids, top_level }
    }

    fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    fn matches(&self, field: &arrow::datatypes::Field, top: bool) -> bool {
        match field
            .metadata()
            .get(FIELD_ID_KEY)
            .and_then(|v| v.parse::<i32>().ok())
        {
            Some(id) => self.ids.contains(&id),
            None => top && self.top_level.contains(field.name()),
        }
    }

    /// Rewrite every `uuid` column (nested ones too) as hyphenated UUID text,
    /// so the row and columnar paths carry what the schema advertises.
    pub fn apply(&self, batch: RecordBatch) -> Result<RecordBatch, FaucetError> {
        if self.is_empty() {
            return Ok(batch);
        }
        let schema = batch.schema();
        let mut changed = false;
        let mut fields = Vec::with_capacity(schema.fields().len());
        let mut columns = Vec::with_capacity(schema.fields().len());
        for (field, column) in schema.fields().iter().zip(batch.columns()) {
            match self.convert(field, column, true)? {
                Some((f, c)) => {
                    changed = true;
                    fields.push(f);
                    columns.push(c);
                }
                None => {
                    fields.push(field.clone());
                    columns.push(column.clone());
                }
            }
        }
        if !changed {
            return Ok(batch);
        }
        let schema = arrow::datatypes::Schema::new_with_metadata(fields, schema.metadata().clone());
        RecordBatch::try_new(std::sync::Arc::new(schema), columns).map_err(|e| {
            FaucetError::Source(format!("iceberg: rendering uuid columns failed: {e}"))
        })
    }

    fn convert(
        &self,
        field: &arrow::datatypes::FieldRef,
        array: &arrow::array::ArrayRef,
        top: bool,
    ) -> Result<Option<(arrow::datatypes::FieldRef, arrow::array::ArrayRef)>, FaucetError> {
        use arrow::array::{
            Array, AsArray, LargeListArray, ListArray, MapArray, StringArray, StructArray,
        };
        use arrow::datatypes::{DataType, Field};
        use std::sync::Arc;
        let err = |e: arrow::error::ArrowError| {
            FaucetError::Source(format!("iceberg: rendering uuid columns failed: {e}"))
        };
        Ok(match field.data_type() {
            DataType::FixedSizeBinary(16) if self.matches(field, top) => {
                let bin = array.as_fixed_size_binary();
                let text: StringArray = (0..bin.len())
                    .map(|i| (!bin.is_null(i)).then(|| uuid_text(bin.value(i))))
                    .collect();
                let f = Field::new(field.name(), DataType::Utf8, field.is_nullable())
                    .with_metadata(field.metadata().clone());
                Some((Arc::new(f), Arc::new(text)))
            }
            DataType::Struct(children) => {
                let st = array.as_struct();
                let mut changed = false;
                let mut fields = Vec::with_capacity(children.len());
                let mut cols = Vec::with_capacity(children.len());
                for (child, col) in children.iter().zip(st.columns()) {
                    match self.convert(child, col, false)? {
                        Some((f, c)) => {
                            changed = true;
                            fields.push(f);
                            cols.push(c);
                        }
                        None => {
                            fields.push(child.clone());
                            cols.push(col.clone());
                        }
                    }
                }
                if !changed {
                    return Ok(None);
                }
                let st = StructArray::try_new(fields.clone().into(), cols, st.nulls().cloned())
                    .map_err(err)?;
                let f = field
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::Struct(fields.into()));
                Some((Arc::new(f), Arc::new(st)))
            }
            DataType::List(item) => {
                let list = array.as_list::<i32>();
                let Some((item, values)) = self.convert(item, list.values(), false)? else {
                    return Ok(None);
                };
                let out = ListArray::try_new(
                    item.clone(),
                    list.offsets().clone(),
                    values,
                    list.nulls().cloned(),
                )
                .map_err(err)?;
                let f = field.as_ref().clone().with_data_type(DataType::List(item));
                Some((Arc::new(f), Arc::new(out)))
            }
            DataType::LargeList(item) => {
                let list = array.as_list::<i64>();
                let Some((item, values)) = self.convert(item, list.values(), false)? else {
                    return Ok(None);
                };
                let out = LargeListArray::try_new(
                    item.clone(),
                    list.offsets().clone(),
                    values,
                    list.nulls().cloned(),
                )
                .map_err(err)?;
                let f = field
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::LargeList(item));
                Some((Arc::new(f), Arc::new(out)))
            }
            DataType::Map(entries_field, sorted) => {
                let map = array.as_map();
                let entries: arrow::array::ArrayRef = Arc::new(map.entries().clone());
                let Some((entries_field, entries)) =
                    self.convert(entries_field, &entries, false)?
                else {
                    return Ok(None);
                };
                let out = MapArray::try_new(
                    entries_field.clone(),
                    map.offsets().clone(),
                    entries.as_struct().clone(),
                    map.nulls().cloned(),
                    *sorted,
                )
                .map_err(err)?;
                let f = field
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::Map(entries_field, *sorted));
                Some((Arc::new(f), Arc::new(out)))
            }
            _ => None,
        })
    }
}

/// `8-4-4-4-12` lower-case hex of a 16-byte UUID.
fn uuid_text(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(36);
    for (i, b) in bytes.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            out.push('-');
        }
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// JSON Schema fragment for an Iceberg type (matching the row encoding).
pub fn type_to_json_schema(ty: &Type) -> Value {
    match ty {
        Type::Primitive(p) => match p {
            PrimitiveType::Boolean => json!({ "type": "boolean" }),
            PrimitiveType::Int | PrimitiveType::Long => json!({ "type": "integer" }),
            // Rows carry NaN / ±Infinity as strings and every digit of a
            // decimal as a string, so the schema says so (#862 M-062).
            PrimitiveType::Float | PrimitiveType::Double => json!({ "type": ["number", "string"] }),
            PrimitiveType::Decimal { .. } => json!({ "type": "string", "format": "decimal" }),
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

    #[test]
    fn decimal_rows_keep_every_digit() {
        use arrow::datatypes::{DataType, Field};
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "d",
            DataType::Decimal128(38, 4),
            true,
        )]));
        let d = arrow::array::Decimal128Array::from(vec![Some(
            123_456_789_012_345_678_901_234_567_890i128,
        )])
        .with_precision_and_scale(38, 4)
        .unwrap();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(d)]).unwrap();
        assert_eq!(
            batch_to_rows(&batch).unwrap()[0]["d"],
            "12345678901234567890123456.7890"
        );
    }

    /// A `uuid` column (top level, in a struct and in a list) arrives as
    /// hyphenated text; a `fixed(16)` column with the same Arrow type stays
    /// binary.
    #[test]
    fn uuid_columns_render_as_uuid_text() {
        use arrow::array::{FixedSizeBinaryArray, ListArray, StructArray};
        let p = |t| Type::Primitive(t);
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::optional(1, "id", p(PrimitiveType::Uuid))),
                Arc::new(NestedField::optional(2, "raw", p(PrimitiveType::Fixed(16)))),
                Arc::new(NestedField::optional(
                    3,
                    "s",
                    Type::Struct(StructType::new(vec![Arc::new(NestedField::optional(
                        4,
                        "owner",
                        p(PrimitiveType::Uuid),
                    ))])),
                )),
                Arc::new(NestedField::optional(
                    5,
                    "tags",
                    Type::List(ListType::new(Arc::new(NestedField::list_element(
                        6,
                        p(PrimitiveType::Uuid),
                        false,
                    )))),
                )),
            ])
            .build()
            .unwrap();
        let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(&schema).unwrap());
        let bytes: [u8; 16] = [
            0x12, 0x34, 0x56, 0x78, 0x90, 0x12, 0x34, 0x56, 0x78, 0x90, 0x12, 0x34, 0x56, 0x78,
            0x90, 0x12,
        ];
        let bin = || {
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    vec![Some(bytes), None].into_iter(),
                    16,
                )
                .unwrap(),
            ) as arrow::array::ArrayRef
        };
        let DataType::Struct(sf) = arrow_schema.field(2).data_type().clone() else {
            panic!("struct")
        };
        let st = StructArray::new(sf, vec![bin()], None);
        let DataType::List(item) = arrow_schema.field(3).data_type().clone() else {
            panic!("list")
        };
        let list = ListArray::new(
            item,
            arrow::buffer::OffsetBuffer::from_lengths([1, 1]),
            bin(),
            None,
        );
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![bin(), bin(), Arc::new(st), Arc::new(list)],
        )
        .unwrap();
        let out = UuidColumns::of(&schema).apply(batch).unwrap();
        assert_eq!(out.schema().field(0).data_type(), &DataType::Utf8);
        let rows = batch_to_rows(&out).unwrap();
        let text = "12345678-9012-3456-7890-123456789012";
        assert_eq!(rows[0]["id"], text);
        assert_eq!(rows[0]["s"]["owner"], text);
        assert_eq!(rows[0]["tags"], json!([text]));
        assert_eq!(rows[1]["id"], Value::Null);
        assert_eq!(rows[1]["tags"], json!([null]));
        assert_eq!(rows[0]["raw"], "EjRWeJASNFZ4kBI0VniQEg==");
    }

    /// A `uuid` map value and a `uuid` large-list item render as text; a
    /// batch with no `uuid` column, or none the schema marks, is untouched.
    /// A batch that breaks its own schema (a null in a required uuid column,
    /// top level or nested) is refused with a typed error, not rebuilt.
    #[test]
    #[allow(unsafe_code)]
    fn uuid_rendering_refuses_a_batch_that_breaks_its_schema() {
        use arrow::array::{ArrayRef, FixedSizeBinaryArray, StructArray};
        let p = |t| Type::Primitive(t);
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(1, "id", p(PrimitiveType::Uuid))),
                Arc::new(NestedField::required(
                    2,
                    "s",
                    Type::Struct(StructType::new(vec![Arc::new(NestedField::required(
                        3,
                        "owner",
                        p(PrimitiveType::Uuid),
                    ))])),
                )),
            ])
            .build()
            .unwrap();
        let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(&schema).unwrap());
        let bin = |v: Vec<Option<[u8; 16]>>| {
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(v.into_iter(), 16).unwrap(),
            ) as ArrayRef
        };
        let arrow::datatypes::DataType::Struct(children) =
            arrow_schema.field(1).data_type().clone()
        else {
            panic!("struct");
        };
        // SAFETY: deliberately invalid (nulls in required fields) to exercise
        // the rebuild's validation; only read by the code under test.
        let st = |owner: ArrayRef| -> ArrayRef {
            Arc::new(unsafe { StructArray::new_unchecked(children.clone(), vec![owner], None) })
        };
        let cols = UuidColumns::of(&schema);

        let top = unsafe {
            RecordBatch::new_unchecked(
                arrow_schema.clone(),
                vec![bin(vec![None]), st(bin(vec![Some([1; 16])]))],
                1,
            )
        };
        let e = cols.apply(top).unwrap_err().to_string();
        assert!(e.contains("rendering uuid columns failed"), "{e}");

        let nested = unsafe {
            RecordBatch::new_unchecked(
                arrow_schema,
                vec![bin(vec![Some([1; 16])]), st(bin(vec![None]))],
                1,
            )
        };
        let e = cols.apply(nested).unwrap_err().to_string();
        assert!(e.contains("rendering uuid columns failed"), "{e}");
    }

    #[test]
    fn uuid_in_maps_and_large_lists_renders_as_text_and_others_are_untouched() {
        use arrow::array::{
            ArrayRef, FixedSizeBinaryArray, LargeListArray, ListArray, MapArray, StructArray,
        };
        use arrow::buffer::OffsetBuffer;
        let p = |t| Type::Primitive(t);
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::optional(
                    1,
                    "m",
                    Type::Map(MapType::new(
                        Arc::new(NestedField::map_key_element(2, p(PrimitiveType::String))),
                        Arc::new(NestedField::map_value_element(
                            3,
                            p(PrimitiveType::Uuid),
                            false,
                        )),
                    )),
                )),
                Arc::new(NestedField::optional(
                    4,
                    "big",
                    Type::List(ListType::new(Arc::new(NestedField::list_element(
                        5,
                        p(PrimitiveType::Uuid),
                        false,
                    )))),
                )),
            ])
            .build()
            .unwrap();
        let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(&schema).unwrap());
        let bytes: [u8; 16] = [0x12; 16];
        let bin = || {
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    vec![Some(bytes), None].into_iter(),
                    16,
                )
                .unwrap(),
            ) as ArrayRef
        };
        let DataType::Map(entries_field, sorted) = arrow_schema.field(0).data_type().clone() else {
            panic!("map")
        };
        let DataType::Struct(ef) = entries_field.data_type().clone() else {
            panic!("entries")
        };
        let keys = Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef;
        let entries = StructArray::new(ef, vec![keys, bin()], None);
        let map = MapArray::new(
            entries_field,
            OffsetBuffer::from_lengths([1, 1]),
            entries,
            None,
            sorted,
        );
        let DataType::List(item) = arrow_schema.field(1).data_type().clone() else {
            panic!("list")
        };
        let big = LargeListArray::new(
            item.clone(),
            OffsetBuffer::<i64>::from_lengths([1, 1]),
            bin(),
            None,
        );
        let big_field = arrow_schema
            .field(1)
            .clone()
            .with_data_type(DataType::LargeList(item));
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                arrow_schema.field(0).clone(),
                big_field,
            ])),
            vec![Arc::new(map), Arc::new(big)],
        )
        .unwrap();
        let out = UuidColumns::of(&schema).apply(batch).unwrap();
        let DataType::Map(entries, _) = out.schema().field(0).data_type().clone() else {
            panic!("map")
        };
        let DataType::Struct(ef) = entries.data_type().clone() else {
            panic!("entries")
        };
        assert_eq!(ef[1].data_type(), &DataType::Utf8);
        let DataType::LargeList(item) = out.schema().field(1).data_type().clone() else {
            panic!("large list")
        };
        assert_eq!(item.data_type(), &DataType::Utf8);
        let text = "12121212-1212-1212-1212-121212121212";
        let rows = batch_to_rows(&out).unwrap();
        assert!(rows[0].to_string().contains(text), "{}", rows[0]);
        assert_eq!(rows[0]["big"], json!([text]));

        let plain = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "n",
                p(PrimitiveType::Long),
            ))])
            .build()
            .unwrap();
        let ints = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![Field::new(
                "n",
                DataType::Int64,
                true,
            )])),
            vec![Arc::new(Int64Array::from(vec![1, 2]))],
        )
        .unwrap();
        let same = UuidColumns::of(&plain).apply(ints.clone()).unwrap();
        assert!(Arc::ptr_eq(&same.schema(), &ints.schema()));

        let mixed = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::optional(1, "u", p(PrimitiveType::Uuid))),
                Arc::new(NestedField::optional(
                    2,
                    "st",
                    Type::Struct(StructType::new(vec![Arc::new(NestedField::optional(
                        3,
                        "n",
                        p(PrimitiveType::Long),
                    ))])),
                )),
                Arc::new(NestedField::optional(
                    4,
                    "ns",
                    Type::List(ListType::new(Arc::new(NestedField::list_element(
                        5,
                        p(PrimitiveType::Long),
                        false,
                    )))),
                )),
            ])
            .build()
            .unwrap();
        let mixed_arrow = Arc::new(iceberg::arrow::schema_to_arrow_schema(&mixed).unwrap());
        let DataType::Struct(sf) = mixed_arrow.field(1).data_type().clone() else {
            panic!("struct")
        };
        let st = StructArray::new(sf, vec![Arc::new(Int64Array::from(vec![1, 2]))], None);
        let DataType::List(li) = mixed_arrow.field(2).data_type().clone() else {
            panic!("list")
        };
        let ns = ListArray::new(
            li,
            OffsetBuffer::from_lengths([1, 1]),
            Arc::new(Int64Array::from(vec![3, 4])),
            None,
        );
        let only_nested = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                mixed_arrow.field(1).clone(),
                mixed_arrow.field(2).clone(),
            ])),
            vec![Arc::new(st), Arc::new(ns)],
        )
        .unwrap();
        let untouched = UuidColumns::of(&mixed).apply(only_nested.clone()).unwrap();
        assert!(Arc::ptr_eq(&untouched.schema(), &only_nested.schema()));
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
        assert_eq!(p["x"]["type"], json!(["number", "string", "null"]));
        assert_eq!(p["dec"]["type"], json!(["string", "null"]));
        assert_eq!(p["dec"]["format"], "decimal");
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
