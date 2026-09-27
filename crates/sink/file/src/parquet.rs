//! Parquet output through the Arrow writer.
//!
//! The schema comes from the first page (every field nullable). A later page
//! that adds a column widens it: the row groups written so far are rewritten
//! under the wider schema, with the new column null. A field that changes type
//! is an error naming the field.

use crate::config::{ParquetCodec, ParquetOptions};
use crate::layout::{io_err, tmp_path};
use crate::writer::Ctx;
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
}

impl ParquetState {
    pub fn new() -> Self {
        Self {
            schema: None,
            writer: None,
        }
    }

    /// Encode `records` against the file's schema, widened by whatever new
    /// fields they carry.
    pub fn batch_for(&self, records: &[Value]) -> Result<RecordBatch, FaucetError> {
        let inferred = faucet_core::columnar::infer_arrow_schema(records)?;
        let schema = match &self.schema {
            Some(current) => merge(current, &inferred)?,
            None => nullable(&inferred),
        };
        faucet_core::columnar::values_to_record_batch(records, schema)
    }

    /// Append `batch`. When the file's schema must widen, or the file was
    /// finalised and is being continued, the rows already written are copied
    /// into a new writer first.
    pub fn write(
        &mut self,
        ctx: &Ctx<'_>,
        final_path: &Path,
        tmp: &Path,
        finalized: bool,
        batch: &RecordBatch,
    ) -> Result<(), FaucetError> {
        let target = match &self.schema {
            Some(current) => merge(current, batch.schema_ref())?,
            None => nullable(batch.schema_ref()),
        };
        let widened = self
            .schema
            .as_ref()
            .is_some_and(|s| s.fields() != target.fields());
        if self.writer.is_none() || widened {
            let old = match self.writer.take() {
                Some(w) => {
                    w.close().map_err(|e| pq_err(tmp, e))?;
                    let old = tmp_path(tmp, ".old");
                    std::fs::rename(tmp, &old).map_err(|e| io_err("renaming", tmp, e))?;
                    Some((old, true))
                }
                None if finalized && final_path.exists() => Some((final_path.to_path_buf(), false)),
                None => None,
            };
            let mut writer = open_writer(tmp, &target, ctx.parquet)?;
            if let Some((path, remove)) = old {
                copy_into(&path, &target, &mut writer)?;
                if remove {
                    let _ = std::fs::remove_file(&path);
                }
            }
            self.writer = Some(writer);
            self.schema = Some(target.clone());
        }
        let batch = project(batch, &target)?;
        self.writer
            .as_mut()
            .expect("writer opened above")
            .write(&batch)
            .map_err(|e| pq_err(tmp, e))
    }

    /// Finish the file at `tmp` and sync it. `false` when nothing is open.
    pub fn close(&mut self, tmp: &Path) -> Result<bool, FaucetError> {
        let Some(writer) = self.writer.take() else {
            return Ok(false);
        };
        let file = writer.into_inner().map_err(|e| pq_err(tmp, e))?;
        file.sync_all().map_err(|e| io_err("syncing", tmp, e))?;
        Ok(true)
    }

    /// Drop the writer without finishing the file.
    pub fn abandon(&mut self) {
        self.writer.take();
    }
}

fn open_writer(
    tmp: &Path,
    schema: &SchemaRef,
    opts: &ParquetOptions,
) -> Result<ArrowWriter<File>, FaucetError> {
    let compression = match opts.compression {
        ParquetCodec::None => Compression::UNCOMPRESSED,
        ParquetCodec::Snappy => Compression::SNAPPY,
        ParquetCodec::Gzip => Compression::GZIP(GzipLevel::default()),
        ParquetCodec::Zstd => Compression::ZSTD(ZstdLevel::default()),
    };
    let props = WriterProperties::builder()
        .set_compression(compression)
        .build();
    let file = File::create(tmp).map_err(|e| io_err("creating", tmp, e))?;
    ArrowWriter::try_new(file, schema.clone(), Some(props)).map_err(|e| pq_err(tmp, e))
}

/// Copy every row of the Parquet file at `path` into `writer` under `schema`.
fn copy_into(
    path: &Path,
    schema: &SchemaRef,
    writer: &mut ArrowWriter<File>,
) -> Result<(), FaucetError> {
    let file = File::open(path).map_err(|e| io_err("opening", path, e))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
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
            Some(c) => arrow::compute::cast(c, f.data_type()).map_err(|e| {
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

/// `current` widened by `incoming`'s fields.
fn merge(current: &SchemaRef, incoming: &Schema) -> Result<SchemaRef, FaucetError> {
    let merged = Schema::try_merge([
        current.as_ref().clone(),
        nullable(incoming).as_ref().clone(),
    ])
    .map_err(|e| {
        FaucetError::Sink(format!(
            "parquet: a field changed type between pages, which one Parquet file cannot \
                 hold — cast it to one type (a `cast` transform) or roll to a new file: {e}"
        ))
    })?;
    Ok(nullable(&merged))
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
}
