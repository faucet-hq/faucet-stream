//! Parquet output through the Arrow writer.
//!
//! The schema comes from the first page (every field nullable). A later page
//! that adds a column widens it: the row groups written so far are rewritten
//! under the wider schema, with the new column null. A field that changes type
//! is an error naming the field.

use super::encode::{Ctx, Failure, create_scratch, remove_fetched};
use super::layout::{OLD_ROLE, io_err, tmp_path};
use super::options::{ParquetCodec, ParquetField, ParquetOptions, ParquetType};
use arrow::array::{RecordBatch, new_null_array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use faucet_core::FaucetError;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, GzipLevel, ZstdLevel};
use parquet::file::properties::WriterProperties;
use serde_json::Value;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

/// Parquet state of one output file.
pub(crate) struct ParquetState {
    schema: Option<SchemaRef>,
    writer: Option<ArrowWriter<File>>,
    /// Integer columns written so far that hold a value `Float64` cannot
    /// represent exactly, so widening them would round rows already written.
    inexact_ints: std::collections::HashSet<String>,
}

/// The largest integer magnitude a `Float64` holds exactly.
const F64_EXACT: u64 = 1 << 53;

fn inexact_as_f64(array: &dyn arrow::array::Array) -> bool {
    use arrow::array::AsArray;
    use arrow::datatypes::{Int64Type, UInt64Type};
    match array.data_type() {
        DataType::Int64 => array
            .as_primitive::<Int64Type>()
            .iter()
            .flatten()
            .any(|v| v.unsigned_abs() > F64_EXACT),
        DataType::UInt64 => array
            .as_primitive::<UInt64Type>()
            .iter()
            .flatten()
            .any(|v| v > F64_EXACT),
        _ => false,
    }
}

fn rounding_err(column: &str) -> FaucetError {
    FaucetError::Sink(format!(
        "parquet: column '{column}' holds whole numbers beyond 2^53 and a fraction on another \
         page; widening it to a float would round those integers — cast it to one type (a \
         `cast` transform to string or number) or roll to a new file"
    ))
}

impl ParquetState {
    pub fn new() -> Self {
        Self {
            schema: None,
            writer: None,
            inexact_ints: std::collections::HashSet::new(),
        }
    }

    /// Encode `records` against the file's schema, widened by whatever new
    /// fields they carry.
    pub fn batch_for(
        &self,
        opts: &ParquetOptions,
        records: &[Value],
    ) -> Result<RecordBatch, FaucetError> {
        if let Some(fields) = &opts.schema {
            let schema = explicit_schema(fields);
            reject_unknown(
                &schema,
                records
                    .iter()
                    .filter_map(Value::as_object)
                    .flat_map(|m| m.keys()),
            )?;
            return faucet_core::columnar::values_to_record_batch(records, schema).map_err(|e| {
                FaucetError::Sink(format!(
                    "parquet: a record does not fit `parquet.schema`: {e}"
                ))
            });
        }
        let inferred = faucet_core::columnar::infer_arrow_schema(records)?;
        let schema = match &self.schema {
            Some(current) => merge(current, &inferred)?,
            None => nullable(&inferred),
        };
        for field in inferred.fields() {
            let to_float = schema
                .field_with_name(field.name())
                .is_ok_and(|f| f.data_type() == &DataType::Float64);
            if to_float && field.data_type().is_integer() {
                let inexact = records.iter().any(|r| {
                    r.get(field.name()).is_some_and(|v| {
                        v.as_i64().map_or_else(
                            || v.as_u64().is_some_and(|u| u > F64_EXACT),
                            |i| i.unsigned_abs() > F64_EXACT,
                        )
                    })
                });
                if inexact {
                    return Err(rounding_err(field.name()));
                }
            }
        }
        faucet_core::columnar::values_to_record_batch(records, schema)
    }

    /// Whether a writer is open (the file has rows not yet closed).
    pub fn is_open(&self) -> bool {
        self.writer.is_some()
    }

    /// Append `batch`. When the file's schema must widen, or the file was
    /// published and is being continued from the local copy at `existing`,
    /// the rows already written are copied into a new writer first. A batch
    /// that does not fit the schema is refused before anything is written.
    pub fn write(
        &mut self,
        ctx: &Ctx<'_>,
        label: &str,
        tmp: &Path,
        existing: Option<&Path>,
        batch: &RecordBatch,
    ) -> Result<(), Failure> {
        let target = match (&ctx.parquet.schema, &self.schema) {
            (Some(fields), _) => {
                let schema = explicit_schema(fields);
                reject_unknown(&schema, batch.schema().fields().iter().map(|f| f.name()))
                    .map_err(Failure::Clean)?;
                schema
            }
            (None, Some(current)) => merge(current, batch.schema_ref()).map_err(Failure::Clean)?,
            (None, None) => nullable(batch.schema_ref()),
        };
        if let Some(current) = &self.schema {
            for f in target.fields() {
                let was_int = current
                    .field_with_name(f.name())
                    .is_ok_and(|c| c.data_type().is_integer());
                if was_int && f.data_type().is_floating() && self.inexact_ints.contains(f.name()) {
                    return Err(Failure::Clean(rounding_err(f.name())));
                }
            }
        }
        let batch = project(batch, &target).map_err(Failure::Clean)?;
        let widened = self
            .schema
            .as_ref()
            .is_some_and(|s| s.fields() != target.fields());
        if self.writer.is_none() || widened {
            self.reopen(ctx, label, tmp, existing, &target)
                .map_err(Failure::Dirty)?;
        }
        self.writer
            .as_mut()
            .expect("writer opened above")
            .write(&batch)
            .map_err(|e| Failure::Dirty(pq_err(tmp, e)))?;
        for (f, column) in batch.schema().fields().iter().zip(batch.columns()) {
            if inexact_as_f64(column.as_ref()) {
                self.inexact_ints.insert(f.name().clone());
            }
        }
        Ok(())
    }

    /// Open a writer for `target` at `tmp`, carrying over the rows of the
    /// open writer or of the stored file at `existing`.
    fn reopen(
        &mut self,
        ctx: &Ctx<'_>,
        label: &str,
        tmp: &Path,
        existing: Option<&Path>,
        target: &SchemaRef,
    ) -> Result<(), FaucetError> {
        let old = tmp_path(tmp, OLD_ROLE);
        let carried = match self.writer.take() {
            Some(w) => {
                w.close().map_err(|e| pq_err(tmp, e))?;
                std::fs::rename(tmp, &old).map_err(|e| io_err("renaming", tmp, e))?;
                Carry::File(old.clone())
            }
            None => match existing {
                Some(path) => {
                    let stored = ctx.read_stored(path, label);
                    remove_fetched(path);
                    Carry::Bytes(ctx.decompress(stored?)?)
                }
                None => Carry::Nothing,
            },
        };
        let result = (|| -> Result<ArrowWriter<File>, FaucetError> {
            let mut writer = open_writer(tmp, target, ctx)?;
            match carried {
                Carry::File(path) => {
                    let file = File::open(&path).map_err(|e| io_err("opening", &path, e))?;
                    copy_into(&path, file, target, &mut writer)?;
                }
                Carry::Bytes(bytes) => {
                    copy_into(tmp, bytes::Bytes::from(bytes), target, &mut writer)?
                }
                Carry::Nothing => {}
            }
            Ok(writer)
        })();
        let _ = std::fs::remove_file(&old);
        self.writer = Some(result?);
        self.schema = Some(target.clone());
        Ok(())
    }

    /// Finish the file at `tmp`. `false` when nothing is open.
    pub fn close(&mut self, tmp: &Path) -> Result<bool, FaucetError> {
        let Some(writer) = self.writer.take() else {
            return Ok(false);
        };
        writer.into_inner().map_err(|e| pq_err(tmp, e))?;
        Ok(true)
    }

    /// Drop the writer without finishing the file.
    pub fn abandon(&mut self) {
        self.writer.take();
    }
}

/// Rows a new writer starts from.
enum Carry {
    Nothing,
    File(std::path::PathBuf),
    Bytes(Vec<u8>),
}

fn open_writer(
    tmp: &Path,
    schema: &SchemaRef,
    ctx: &Ctx<'_>,
) -> Result<ArrowWriter<File>, FaucetError> {
    let compression = match ctx.parquet_codec {
        ParquetCodec::None => Compression::UNCOMPRESSED,
        ParquetCodec::Snappy => Compression::SNAPPY,
        ParquetCodec::Gzip => Compression::GZIP(GzipLevel::default()),
        ParquetCodec::Zstd => Compression::ZSTD(ZstdLevel::default()),
        ParquetCodec::Lz4 => Compression::LZ4_RAW,
    };
    let props = WriterProperties::builder()
        .set_compression(compression)
        .set_max_row_group_row_count(Some(ctx.parquet.row_group_size))
        .build();
    let file = create_scratch(tmp, ctx.private())?;
    ArrowWriter::try_new(file, schema.clone(), Some(props)).map_err(|e| pq_err(tmp, e))
}

/// Copy every row of the Parquet file at `path` into `writer` under `schema`.
fn copy_into<R: parquet::file::reader::ChunkReader + 'static>(
    path: &Path,
    input: R,
    schema: &SchemaRef,
    writer: &mut ArrowWriter<File>,
) -> Result<(), FaucetError> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(input)
        .and_then(|b| b.build())
        .map_err(|e| pq_err(path, e))?;
    for batch in reader {
        let batch = batch.map_err(|e| pq_err(path, e.into()))?;
        writer
            .write(&project(&batch, schema)?)
            .map_err(|e| pq_err(path, e))?;
    }
    Ok(())
}

/// `batch` reshaped to `schema`: columns by name, cast where the type widened,
/// null where the batch lacks the column.
fn project(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch, FaucetError> {
    let columns = schema
        .fields()
        .iter()
        .map(|f| match batch.column_by_name(f.name()) {
            Some(c) if c.data_type() == f.data_type() => Ok(c.clone()),
            Some(c) if f.data_type().is_floating() && inexact_as_f64(c.as_ref()) => {
                Err(rounding_err(f.name()))
            }
            Some(c) => arrow::compute::cast_with_options(
                c,
                f.data_type(),
                &arrow::compute::CastOptions {
                    safe: false,
                    ..Default::default()
                },
            )
            .map_err(|e| {
                FaucetError::Sink(format!(
                    "parquet: column '{}' cannot be converted to {}: {e}",
                    f.name(),
                    f.data_type()
                ))
            }),
            None => Ok(new_null_array(f.data_type(), batch.num_rows())),
        })
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| FaucetError::Sink(format!("parquet: building a batch: {e}")))
}

/// The type both of two numeric types widen to without refusing a value:
/// any integer with a float, or two floats, is `Float64`; two signed
/// integers the wider one. `Int64` with `UInt64` has no common type.
fn widened_number(a: &DataType, b: &DataType) -> Option<DataType> {
    use DataType::*;
    let signed = |t: &DataType| matches!(t, Int8 | Int16 | Int32 | Int64);
    let unsigned = |t: &DataType| matches!(t, UInt8 | UInt16 | UInt32 | UInt64);
    let float = |t: &DataType| matches!(t, Float16 | Float32 | Float64);
    let int = |t: &DataType| signed(t) || unsigned(t);
    match (a, b) {
        _ if a == b => None,
        (x, y) if (float(x) && (float(y) || int(y))) || (int(x) && float(y)) => Some(Float64),
        (x, y) if signed(x) && signed(y) => Some(Int64),
        _ => None,
    }
}

/// `schema` with each field `other` holds as a different numeric type
/// widened to their common type.
fn unify_numbers(schema: &Schema, other: &Schema) -> Schema {
    Schema::new(
        schema
            .fields()
            .iter()
            .map(|f| {
                let common = other
                    .field_with_name(f.name())
                    .ok()
                    .and_then(|o| widened_number(f.data_type(), o.data_type()));
                match common {
                    Some(dt) => Arc::new(f.as_ref().clone().with_data_type(dt)),
                    None => f.clone(),
                }
            })
            .collect::<Vec<_>>(),
    )
    .with_metadata(schema.metadata().clone())
}

/// `current` widened by `incoming`'s fields. A column that holds whole
/// numbers on one page and fractions on another is widened to `Float64`, and
/// the rows already written are cast to it.
fn merge(current: &SchemaRef, incoming: &Schema) -> Result<SchemaRef, FaucetError> {
    let incoming = nullable(incoming);
    let merged = Schema::try_merge([
        unify_numbers(current, &incoming),
        unify_numbers(&incoming, current),
    ])
    .map_err(|e| {
        FaucetError::Sink(format!(
            "parquet: a field changed type between pages, which one Parquet file cannot \
                 hold — cast it to one type (a `cast` transform) or roll to a new file: {e}"
        ))
    })?;
    Ok(nullable(&merged))
}

/// The Arrow schema an explicit `parquet.schema` declares.
pub(crate) fn explicit_schema(fields: &[ParquetField]) -> SchemaRef {
    use arrow::datatypes::TimeUnit;
    let utc = || Some(Arc::from("UTC"));
    Arc::new(Schema::new(
        fields
            .iter()
            .map(|f| {
                let dt = match f.data_type {
                    ParquetType::Boolean => DataType::Boolean,
                    ParquetType::Int32 => DataType::Int32,
                    ParquetType::Int64 => DataType::Int64,
                    ParquetType::Uint64 => DataType::UInt64,
                    ParquetType::Float32 => DataType::Float32,
                    ParquetType::Float64 => DataType::Float64,
                    ParquetType::String => DataType::Utf8,
                    ParquetType::Date => DataType::Date32,
                    ParquetType::TimestampMs => DataType::Timestamp(TimeUnit::Millisecond, utc()),
                    ParquetType::TimestampUs => DataType::Timestamp(TimeUnit::Microsecond, utc()),
                    ParquetType::TimestampNs => DataType::Timestamp(TimeUnit::Nanosecond, utc()),
                    ParquetType::Decimal { precision, scale } => {
                        DataType::Decimal128(precision, scale)
                    }
                };
                Field::new(&f.name, dt, f.nullable)
            })
            .collect::<Vec<_>>(),
    ))
}

fn reject_unknown<'a>(
    schema: &Schema,
    names: impl Iterator<Item = &'a String>,
) -> Result<(), FaucetError> {
    let mut unknown: Vec<&str> = Vec::new();
    for n in names {
        if schema.field_with_name(n).is_err() && !unknown.contains(&n.as_str()) {
            unknown.push(n);
        }
    }
    if unknown.is_empty() {
        return Ok(());
    }
    Err(FaucetError::Sink(format!(
        "parquet: field(s) not in `parquet.schema`: [{}] — add them to the schema or drop them \
         with a `drop` transform",
        unknown.join(", ")
    )))
}

/// Every field, recursively, made nullable.
fn nullable(schema: &Schema) -> SchemaRef {
    fn field(f: &Field) -> Field {
        let dt = match f.data_type() {
            DataType::Struct(children) => {
                DataType::Struct(children.iter().map(|c| field(c)).collect())
            }
            DataType::List(inner) => DataType::List(Arc::new(field(inner))),
            DataType::LargeList(inner) => DataType::LargeList(Arc::new(field(inner))),
            other => other.clone(),
        };
        Field::new(f.name(), dt, true).with_metadata(f.metadata().clone())
    }
    Arc::new(Schema::new(
        schema.fields().iter().map(|f| field(f)).collect::<Vec<_>>(),
    ))
}

fn pq_err(path: &Path, e: parquet::errors::ParquetError) -> FaucetError {
    FaucetError::Sink(format!("file sink: parquet '{}': {e}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn merge_widens_and_refuses_type_changes() {
        let a = faucet_core::columnar::infer_arrow_schema(&[json!({"a": 1})]).unwrap();
        let b = faucet_core::columnar::infer_arrow_schema(&[json!({"b": "x"})]).unwrap();
        let m = merge(&nullable(&a), &b).unwrap();
        assert_eq!(m.fields().len(), 2);
        assert!(m.fields().iter().all(|f| f.is_nullable()));
        let c = faucet_core::columnar::infer_arrow_schema(&[json!({"a": "s"})]).unwrap();
        let e = merge(&nullable(&a), &c).unwrap_err();
        assert!(e.to_string().contains("changed type"), "{e}");
    }

    #[test]
    fn a_number_column_widens_to_float_whichever_page_comes_first() {
        let ints = faucet_core::columnar::infer_arrow_schema(&[json!({"amount": 10})]).unwrap();
        let floats = faucet_core::columnar::infer_arrow_schema(&[json!({"amount": 10.5})]).unwrap();
        for (first, then) in [(&ints, &floats), (&floats, &ints)] {
            let m = merge(&nullable(first), then).unwrap();
            assert_eq!(
                m.field_with_name("amount").unwrap().data_type(),
                &DataType::Float64
            );
        }
        assert_eq!(
            widened_number(&DataType::Int32, &DataType::Int64),
            Some(DataType::Int64)
        );
        assert_eq!(widened_number(&DataType::Int64, &DataType::UInt64), None);
        assert_eq!(widened_number(&DataType::Utf8, &DataType::Int64), None);
        assert_eq!(widened_number(&DataType::Int64, &DataType::Int64), None);
        let wide = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Float64,
            true,
        )]));
        let batch = faucet_core::columnar::values_to_record_batch(
            &[json!({"amount": 7})],
            Arc::new(Schema::new(vec![Field::new(
                "amount",
                DataType::Int64,
                true,
            )])),
        )
        .unwrap();
        let cast = project(&batch, &wide).unwrap();
        assert_eq!(cast.column(0).data_type(), &DataType::Float64);
    }

    #[test]
    fn nullable_recurses_into_structs_and_lists() {
        let s = faucet_core::columnar::infer_arrow_schema(&[
            json!({"o": {"x": 1}, "l": [1, 2], "n": null}),
        ])
        .unwrap();
        let n = nullable(&s);
        let o = n.field_with_name("o").unwrap();
        let DataType::Struct(children) = o.data_type() else {
            panic!("struct")
        };
        assert!(children.iter().all(|c| c.is_nullable()));
        let l = n.field_with_name("l").unwrap();
        let DataType::List(inner) = l.data_type() else {
            panic!("list")
        };
        assert!(inner.is_nullable());
        let large = Schema::new(vec![Field::new(
            "ll",
            DataType::LargeList(Arc::new(Field::new("item", DataType::Int64, false))),
            false,
        )]);
        let DataType::LargeList(inner) = nullable(&large).field(0).data_type().clone() else {
            panic!("large list")
        };
        assert!(inner.is_nullable());
    }

    #[test]
    fn project_casts_fills_and_reports_impossible_casts() {
        let batch =
            faucet_core::columnar::values_to_record_batch_inferred(&[json!({"a": 1})]).unwrap();
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let p = project(&batch, &schema).unwrap();
        assert_eq!(p.num_columns(), 2);
        assert_eq!(p.column(1).null_count(), 1);
        let bad: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Struct(vec![Field::new("x", DataType::Int64, true)].into()),
            true,
        )]));
        let e = project(&batch, &bad).unwrap_err();
        assert!(e.to_string().contains("cannot be converted"), "{e}");
    }

    #[test]
    fn project_refuses_a_value_the_target_type_cannot_hold() {
        let batch = faucet_core::columnar::values_to_record_batch_inferred(&[
            json!({"n": 3_000_000_000_i64, "t": "not a time"}),
        ])
        .unwrap();
        for (field, ty) in [
            ("n", DataType::Int32),
            (
                "t",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            ),
        ] {
            let target: SchemaRef = Arc::new(Schema::new(vec![Field::new(field, ty, true)]));
            let e = project(&batch, &target).unwrap_err();
            assert!(e.to_string().contains(&format!("'{field}'")), "{e}");
        }
    }

    #[test]
    fn project_refuses_to_round_a_large_integer_into_a_float() {
        let float: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("n", DataType::Float64, true)]));
        for (ty, value) in [
            (DataType::Int64, json!(-9_007_199_254_740_993_i64)),
            (DataType::UInt64, json!(18_000_000_000_000_000_000_u64)),
        ] {
            let batch = faucet_core::columnar::values_to_record_batch(
                &[json!({ "n": value })],
                Arc::new(Schema::new(vec![Field::new("n", ty, true)])),
            )
            .unwrap();
            let e = project(&batch, &float).unwrap_err();
            assert!(e.to_string().contains("2^53"), "{e}");
        }
        let exact = faucet_core::columnar::values_to_record_batch(
            &[json!({"n": 9_007_199_254_740_992_i64})],
            Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)])),
        )
        .unwrap();
        assert!(project(&exact, &float).is_ok());
        assert!(!inexact_as_f64(&arrow::array::Int32Array::from(vec![1])));
    }

    #[test]
    fn batch_for_refuses_a_large_unsigned_integer_into_a_float_column() {
        let mut state = ParquetState::new();
        state.schema = Some(nullable(&Schema::new(vec![Field::new(
            "n",
            DataType::Float64,
            true,
        )])));
        let opts: ParquetOptions = serde_json::from_value(json!({})).unwrap();
        let e = state
            .batch_for(&opts, &[json!({"n": 18_000_000_000_000_000_000_u64})])
            .unwrap_err();
        assert!(e.to_string().contains("2^53"), "{e}");
        assert!(state.batch_for(&opts, &[json!({"n": 3})]).is_ok());
    }

    #[test]
    fn an_explicit_integer_column_refuses_a_fractional_value() {
        let opts: ParquetOptions = serde_json::from_value(json!({
            "schema": [{"name": "n", "type": "int32"}]
        }))
        .unwrap();
        let state = ParquetState::new();
        state.batch_for(&opts, &[json!({"n": 3.0})]).unwrap();
        let e = state.batch_for(&opts, &[json!({"n": 3.7})]).unwrap_err();
        assert!(e.to_string().contains("'n'"), "{e}");
    }
}
