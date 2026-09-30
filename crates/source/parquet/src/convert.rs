//! Arrow `RecordBatch` → `serde_json::Value` conversion.
//!
//! `arrow_json::ArrayWriter` already knows how to encode every Arrow logical
//! type (structs, lists, maps, decimals, dates, timestamps) as JSON. We let it
//! do the heavy lifting: write each batch as a JSON array into an in-memory
//! buffer and parse the result back into `Vec<Value>`.

use arrow::array::RecordBatch;
use arrow_json::ArrayWriter;
use faucet_core::FaucetError;
use serde_json::Value;

/// Encode a single Arrow `RecordBatch` as a `Vec<serde_json::Value>` where
/// each element is the JSON object representation of one row.
#[deprecated(
    since = "1.5.0",
    note = "use faucet-source-file (FileSourceConfig with `format: parquet`), or faucet-source-s3 for S3 locations"
)]
pub fn record_batch_to_json(batch: &RecordBatch) -> Result<Vec<Value>, FaucetError> {
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }

    let mut buf: Vec<u8> = Vec::with_capacity(batch.num_rows() * 64);
    {
        let mut writer = ArrayWriter::new(&mut buf);
        writer
            .write(batch)
            .map_err(|e| FaucetError::Source(format!("arrow_json encode error: {e}")))?;
        writer
            .finish()
            .map_err(|e| FaucetError::Source(format!("arrow_json finish error: {e}")))?;
    }

    let parsed: Value = serde_json::from_slice(&buf)
        .map_err(|e| FaucetError::Source(format!("arrow_json output parse error: {e}")))?;

    match parsed {
        Value::Array(rows) => Ok(rows),
        other => Err(FaucetError::Source(format!(
            "arrow_json produced non-array output: {}",
            other
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn empty_batch_returns_empty_vec() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(Vec::<i32>::new()))])
                .unwrap();
        let rows = record_batch_to_json(&batch).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn simple_batch_round_trips_to_objects() {
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
        // Null fields are omitted by arrow_json's default writer.
        assert!(rows[1].get("name").is_none() || rows[1]["name"].is_null());
    }
}
