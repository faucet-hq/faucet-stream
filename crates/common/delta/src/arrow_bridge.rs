//! Conversion between the workspace's Arrow and the Arrow `deltalake` is built on.
//!
//! `deltalake` 1.x depends on a newer Arrow major than the rest of the workspace
//! (and than `faucet-core`'s columnar API), so a Delta schema or batch cannot be
//! handed across as-is. Values cross through the Arrow IPC stream format, which
//! is stable across Arrow versions: schema metadata, nested types, dictionaries
//! and null buffers survive unchanged. A batch crossing costs one buffer copy;
//! a schema crossing happens once per run.

use std::io::Cursor;

use deltalake::arrow as delta_arrow;
use faucet_core::FaucetError;

/// The table schema `deltalake` reports, as a workspace Arrow schema.
pub fn schema_from_delta(
    schema: &delta_arrow::datatypes::Schema,
) -> Result<arrow::datatypes::SchemaRef, FaucetError> {
    let mut buf = Vec::new();
    {
        let mut writer = delta_arrow::ipc::writer::StreamWriter::try_new(&mut buf, schema)
            .map_err(|e| bridge_error("encode Delta schema", &e))?;
        writer
            .finish()
            .map_err(|e| bridge_error("encode Delta schema", &e))?;
    }
    let reader = arrow::ipc::reader::StreamReader::try_new(Cursor::new(buf), None)
        .map_err(|e| bridge_error("decode Delta schema", &e))?;
    Ok(reader.schema())
}

/// A workspace Arrow schema as the Arrow schema `deltalake` accepts.
pub fn schema_to_delta(
    schema: &arrow::datatypes::Schema,
) -> Result<delta_arrow::datatypes::SchemaRef, FaucetError> {
    let mut buf = Vec::new();
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buf, schema)
            .map_err(|e| bridge_error("encode schema", &e))?;
        writer
            .finish()
            .map_err(|e| bridge_error("encode schema", &e))?;
    }
    let reader = delta_arrow::ipc::reader::StreamReader::try_new(Cursor::new(buf), None)
        .map_err(|e| bridge_error("decode schema", &e))?;
    Ok(reader.schema())
}

/// A workspace Arrow batch as a batch `deltalake`'s writers accept.
pub fn batch_to_delta(
    batch: &arrow::record_batch::RecordBatch,
) -> Result<delta_arrow::record_batch::RecordBatch, FaucetError> {
    let mut buf = Vec::new();
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema())
            .map_err(|e| bridge_error("encode batch", &e))?;
        writer
            .write(batch)
            .map_err(|e| bridge_error("encode batch", &e))?;
        writer
            .finish()
            .map_err(|e| bridge_error("encode batch", &e))?;
    }
    let mut reader = delta_arrow::ipc::reader::StreamReader::try_new(Cursor::new(buf), None)
        .map_err(|e| bridge_error("decode batch", &e))?;
    first_batch(reader.next())
}

/// The single batch a one-batch IPC stream decodes to.
fn first_batch(
    next: Option<Result<delta_arrow::record_batch::RecordBatch, delta_arrow::error::ArrowError>>,
) -> Result<delta_arrow::record_batch::RecordBatch, FaucetError> {
    match next {
        Some(batch) => batch.map_err(|e| bridge_error("decode batch", &e)),
        None => Err(bridge_error("decode batch", &"the stream held no batch")),
    }
}

fn bridge_error(what: &str, detail: &dyn std::fmt::Display) -> FaucetError {
    FaucetError::Custom(format!("delta: arrow bridge could not {what}: {detail}").into())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{
        Array, DictionaryArray, Int32Array, ListArray, StringArray, StructArray,
        TimestampMicrosecondArray,
    };
    use arrow::datatypes::{DataType, Field, Int32Type, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use delta_arrow::array::Array as _;

    use super::*;

    fn nested_schema() -> Schema {
        let item = Field::new("item", DataType::Int32, true);
        Schema::new_with_metadata(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, true)
                    .with_metadata(HashMap::from([("k".to_string(), "v".to_string())])),
                Field::new("tags", DataType::List(Arc::new(item)), true),
                Field::new(
                    "meta",
                    DataType::Struct(vec![Field::new("a", DataType::Int32, true)].into()),
                    true,
                ),
                Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    true,
                ),
                Field::new(
                    "kind",
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                    true,
                ),
            ],
            HashMap::from([("table".to_string(), "t".to_string())]),
        )
    }

    fn nested_batch() -> RecordBatch {
        let schema = Arc::new(nested_schema());
        let tags = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), None]),
            None,
        ]);
        let meta = StructArray::from(vec![(
            Arc::new(Field::new("a", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![Some(7), None])) as Arc<dyn Array>,
        )]);
        let ts = TimestampMicrosecondArray::from(vec![Some(1_700_000_000_000_000), None])
            .with_timezone("UTC");
        let kind: DictionaryArray<Int32Type> = vec![Some("a"), None].into_iter().collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("x"), None])),
                Arc::new(tags),
                Arc::new(meta),
                Arc::new(ts),
                Arc::new(kind),
            ],
        )
        .unwrap()
    }

    #[test]
    fn schema_round_trips_both_ways() {
        let schema = nested_schema();
        let delta = schema_to_delta(&schema).unwrap();
        assert_eq!(delta.fields().len(), schema.fields().len());
        assert_eq!(delta.metadata().get("table").map(String::as_str), Some("t"));
        let back = schema_from_delta(&delta).unwrap();
        assert_eq!(back.as_ref(), &schema);
    }

    #[test]
    fn batch_crosses_with_values_and_nulls_intact() {
        let batch = nested_batch();
        let delta = batch_to_delta(&batch).unwrap();
        assert_eq!(delta.num_rows(), 2);
        assert_eq!(delta.num_columns(), batch.num_columns());
        assert_eq!(schema_from_delta(&delta.schema()).unwrap(), batch.schema());

        let ids = delta
            .column(0)
            .as_any()
            .downcast_ref::<delta_arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(ids.values(), &[1, 2]);
        let names = delta
            .column(1)
            .as_any()
            .downcast_ref::<delta_arrow::array::StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "x");
        assert!(names.is_null(1));
        assert!(delta.column(2).is_null(1));
        assert!(delta.column(4).is_null(1));
        assert!(delta.column(5).is_null(1));
    }

    #[test]
    fn empty_batch_crosses() {
        let batch = nested_batch().slice(0, 0);
        let delta = batch_to_delta(&batch).unwrap();
        assert_eq!(delta.num_rows(), 0);
        assert_eq!(delta.num_columns(), batch.num_columns());
    }

    #[test]
    fn a_stream_without_a_batch_is_an_error() {
        let err = first_batch(None).unwrap_err().to_string();
        assert!(
            err.contains("arrow bridge could not decode batch: the stream held no batch"),
            "{err}"
        );
    }

    #[test]
    fn a_batch_that_fails_to_decode_is_an_error() {
        let decode = delta_arrow::error::ArrowError::IpcError("truncated".into());
        let err = first_batch(Some(Err(decode))).unwrap_err().to_string();
        assert!(err.contains("arrow bridge could not decode batch"), "{err}");
        assert!(err.contains("truncated"), "{err}");
    }
}
