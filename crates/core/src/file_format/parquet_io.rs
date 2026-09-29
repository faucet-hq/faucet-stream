//! Apache Parquet read helpers for the object-store sources (#777).
//!
//! [`ParquetReadOptions`] is always compiled so a source config can carry a
//! `parquet:` block without cfg-gating it; [`projection_mask`] and
//! [`read_bytes`] need the `file-format-parquet` feature. `parquet.columns`
//! projects top-level columns before any row group is decoded, on both the
//! ranged (row group at a time) and the buffered path.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Parquet reader options (the `parquet:` block of an object-store source).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ParquetReadOptions {
    /// Decode only these top-level columns. A name missing from an object is
    /// an error naming the object and its columns. Unset: every column.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<String>>,
}

#[cfg(feature = "file-format-parquet")]
pub use imp::{projection_mask, read_bytes};

#[cfg(feature = "file-format-parquet")]
mod imp {
    use super::ParquetReadOptions;
    use crate::FaucetError;
    use ::parquet::arrow::ProjectionMask;
    use ::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use ::parquet::schema::types::SchemaDescriptor;
    use arrow::array::RecordBatch;
    use arrow::datatypes::SchemaRef;

    /// The projection `opts.columns` selects in an object with `schema`, or
    /// `None` to read every column. A requested name the object lacks is an
    /// error naming the object (`display`) and its columns.
    pub fn projection_mask(
        opts: &ParquetReadOptions,
        schema: &SchemaDescriptor,
        display: &str,
    ) -> Result<Option<ProjectionMask>, FaucetError> {
        let Some(columns) = opts.columns.as_deref() else {
            return Ok(None);
        };
        let roots = schema.root_schema().get_fields();
        let mut indices = Vec::with_capacity(columns.len());
        for name in columns {
            match roots.iter().position(|f| f.name() == name) {
                Some(i) => indices.push(i),
                None => {
                    let available: Vec<&str> = roots.iter().map(|f| f.name()).collect();
                    return Err(FaucetError::Source(format!(
                        "parquet: column `{name}` is not in '{display}' (columns: {})",
                        available.join(", ")
                    )));
                }
            }
        }
        Ok(Some(ProjectionMask::roots(schema, indices)))
    }

    /// Decode a whole in-memory Parquet object (projected by `opts`) into its
    /// Arrow schema and batches of at most `batch_size` rows (`0`: the
    /// reader's default).
    pub fn read_bytes(
        data: bytes::Bytes,
        opts: &ParquetReadOptions,
        batch_size: usize,
        display: &str,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), FaucetError> {
        let src = |e: &dyn std::fmt::Display| {
            FaucetError::Source(format!("parquet: failed to read '{display}': {e}"))
        };
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(data).map_err(|e| src(&e))?;
        if let Some(mask) = projection_mask(opts, builder.parquet_schema(), display)? {
            builder = builder.with_projection(mask);
        }
        if batch_size > 0 {
            builder = builder.with_batch_size(batch_size);
        }
        let reader = builder.build().map_err(|e| src(&e))?;
        let schema = arrow::array::RecordBatchReader::schema(&reader);
        let batches = reader.collect::<Result<Vec<_>, _>>().map_err(|e| src(&e))?;
        Ok((schema, batches))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_options_serde() {
        let r: ParquetReadOptions =
            serde_json::from_value(serde_json::json!({"columns": ["a"]})).unwrap();
        assert_eq!(r.columns.as_deref(), Some(&["a".to_string()][..]));
        let empty: ParquetReadOptions = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(empty, ParquetReadOptions::default());
        assert!(
            serde_json::from_value::<ParquetReadOptions>(serde_json::json!({"nope": 1})).is_err()
        );
    }

    #[cfg(feature = "file-format-parquet")]
    fn object() -> bytes::Bytes {
        use arrow::array::{Int64Array, RecordBatch, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("a.b", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["x", "y", "z"])),
            ],
        )
        .unwrap();
        let mut buf = Vec::new();
        let mut w = ::parquet::arrow::ArrowWriter::try_new(&mut buf, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        buf.into()
    }

    #[cfg(feature = "file-format-parquet")]
    #[test]
    fn projection_selects_top_level_columns_even_with_dots() {
        let (schema, batches) =
            read_bytes(object(), &ParquetReadOptions::default(), 2, "o").unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(batches.len(), 2, "batch_size caps each batch");
        let opts = ParquetReadOptions {
            columns: Some(vec!["a.b".into()]),
        };
        let (schema, batches) = read_bytes(object(), &opts, 0, "o").unwrap();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "a.b");
        assert_eq!(batches[0].num_columns(), 1);
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
    }

    #[cfg(feature = "file-format-parquet")]
    #[test]
    fn a_missing_column_or_a_bad_object_is_named() {
        let bad = ParquetReadOptions {
            columns: Some(vec!["nope".into()]),
        };
        let err = read_bytes(object(), &bad, 0, "obj")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("`nope`") && err.contains("'obj'") && err.contains("id, a.b"),
            "{err}"
        );
        let err = read_bytes(bytes::Bytes::from_static(b"nope"), &bad, 0, "obj").unwrap_err();
        assert!(matches!(err, crate::FaucetError::Source(ref m) if m.contains("'obj'")));
    }
}
