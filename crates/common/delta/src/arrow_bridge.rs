//! Conversion between the workspace's Arrow and the Arrow `deltalake` is built on.
//!
//! `deltalake` 1.x depends on a newer Arrow major than the rest of the workspace
//! (and than `faucet-core`'s columnar API), so a Delta schema or batch cannot be
//! handed across as-is. Values cross through the Arrow IPC stream format, which
//! is stable across Arrow versions: schema metadata, nested types, dictionaries
//! and null buffers survive unchanged. A batch crosses through the Arrow C Data
//! Interface instead — the same ABI in every Arrow major — so its buffers are
//! shared, not copied.

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
            .map_err(|e| source_bridge_error("encode Delta schema", &e))?;
        writer
            .finish()
            .map_err(|e| source_bridge_error("encode Delta schema", &e))?;
    }
    let reader = arrow::ipc::reader::StreamReader::try_new(Cursor::new(buf), None)
        .map_err(|e| source_bridge_error("decode Delta schema", &e))?;
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
///
/// The buffers cross through the Arrow C Data Interface, which is the same
/// ABI in every Arrow major: the batch is exported as one struct array and
/// imported on the other side without copying a buffer (#862 M-068). Only the
/// schema (a few hundred bytes) is re-encoded, to keep its metadata.
pub fn batch_to_delta(
    batch: &arrow::record_batch::RecordBatch,
) -> Result<delta_arrow::record_batch::RecordBatch, FaucetError> {
    let exported = arrow::array::Array::into_data(arrow::array::StructArray::from(batch.clone()));
    let (array, schema) =
        arrow::ffi::to_ffi(&exported).map_err(|e| bridge_error("export batch", &e))?;
    let (array, schema) = into_delta_ffi(array, schema);
    // SAFETY: `array` / `schema` were produced by `to_ffi` just above, describe
    // one valid struct array, and are consumed exactly once here.
    let imported = unsafe { delta_arrow::ffi::from_ffi(array, &schema) }
        .map_err(|e| bridge_error("import batch", &e))?;
    let columns = delta_arrow::array::StructArray::from(imported)
        .columns()
        .to_vec();
    let schema = schema_to_delta(&batch.schema())?;
    delta_arrow::record_batch::RecordBatch::try_new_with_options(
        schema,
        columns,
        &delta_arrow::record_batch::RecordBatchOptions::new()
            .with_row_count(Some(batch.num_rows())),
    )
    .map_err(|e| bridge_error("import batch", &e))
}

const _: () = {
    use std::mem::{align_of, size_of};
    assert!(
        size_of::<arrow::ffi::FFI_ArrowArray>() == size_of::<delta_arrow::ffi::FFI_ArrowArray>()
    );
    assert!(
        align_of::<arrow::ffi::FFI_ArrowArray>() == align_of::<delta_arrow::ffi::FFI_ArrowArray>()
    );
    assert!(
        size_of::<arrow::ffi::FFI_ArrowSchema>() == size_of::<delta_arrow::ffi::FFI_ArrowSchema>()
    );
    assert!(
        align_of::<arrow::ffi::FFI_ArrowSchema>()
            == align_of::<delta_arrow::ffi::FFI_ArrowSchema>()
    );
};

/// Re-type the C Data Interface structs one Arrow major exported as the other
/// major's. Both are `#[repr(C)]` renderings of the same C structs (sizes are
/// checked at compile time above); ownership — the `release` callback — moves
/// with the bits, so the originals are not dropped.
fn into_delta_ffi(
    array: arrow::ffi::FFI_ArrowArray,
    schema: arrow::ffi::FFI_ArrowSchema,
) -> (
    delta_arrow::ffi::FFI_ArrowArray,
    delta_arrow::ffi::FFI_ArrowSchema,
) {
    let array = std::mem::ManuallyDrop::new(array);
    let schema = std::mem::ManuallyDrop::new(schema);
    // SAFETY: identical `#[repr(C)]` layouts (asserted above); the sources are
    // never used or dropped again, so the release callbacks run exactly once.
    unsafe {
        (
            std::ptr::read(&*array as *const _ as *const delta_arrow::ffi::FFI_ArrowArray),
            std::ptr::read(&*schema as *const _ as *const delta_arrow::ffi::FFI_ArrowSchema),
        )
    }
}

/// A bridge failure on the write side (`schema_to_delta`, `batch_to_delta`).
fn bridge_error(what: &str, detail: &dyn std::fmt::Display) -> FaucetError {
    FaucetError::Sink(format!("delta: arrow bridge could not {what}: {detail}"))
}

/// A bridge failure on the read side (`schema_from_delta`).
fn source_bridge_error(what: &str, detail: &dyn std::fmt::Display) -> FaucetError {
    FaucetError::Source(format!("delta: arrow bridge could not {what}: {detail}"))
}

#[cfg(test)]
mod tests {
    #[test]
    fn bridge_errors_are_typed_by_side() {
        assert!(matches!(
            super::bridge_error("x", &"y"),
            faucet_core::FaucetError::Sink(m) if m.contains("could not x: y")
        ));
        assert!(matches!(
            super::source_bridge_error("x", &"y"),
            faucet_core::FaucetError::Source(_)
        ));
    }

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
    fn batch_buffers_are_shared_not_copied() {
        let batch = nested_batch();
        let delta = batch_to_delta(&batch).unwrap();
        let ours = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .as_ptr();
        let theirs = delta
            .column(0)
            .as_any()
            .downcast_ref::<delta_arrow::array::Int32Array>()
            .unwrap()
            .values()
            .as_ptr();
        assert_eq!(ours, theirs, "the value buffer crossed without a copy");
    }

    #[test]
    fn a_sliced_batch_keeps_its_offset() {
        let batch = nested_batch().slice(1, 1);
        let delta = batch_to_delta(&batch).unwrap();
        assert_eq!(delta.num_rows(), 1);
        let ids = delta
            .column(0)
            .as_any()
            .downcast_ref::<delta_arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(ids.values(), &[2]);
        assert!(delta.column(1).is_null(0));
    }
}
