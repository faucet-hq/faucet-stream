//! Arrow `RecordBatch` → `serde_json::Value` conversion.
//!
//! Delegates to [`faucet_core::columnar::record_batch_to_values`]: null
//! columns stay present as `null`, decimals keep every digit as a string, and
//! NaN / ±Infinity come out as the strings `"NaN"` / `"Infinity"` /
//! `"-Infinity"` rather than `null`.

use arrow::array::RecordBatch;
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

    faucet_core::columnar::record_batch_to_values(batch)
        .map_err(|e| FaucetError::Source(format!("parquet: encoding rows as JSON failed: {e}")))
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
        assert_eq!(
            rows[1].get("name"),
            Some(&Value::Null),
            "a null stays present"
        );
    }

    #[test]
    fn decimals_keep_every_digit_and_non_finite_floats_survive() {
        use arrow::array::{Decimal128Array, Float64Array};
        let schema = Arc::new(Schema::new(vec![
            Field::new("d", DataType::Decimal128(38, 10), true),
            Field::new("f", DataType::Float64, true),
        ]));
        let d = Decimal128Array::from(vec![
            Some(12_345_678_901_234_567_890_123_456_789_012_345_678i128),
            None,
        ])
        .with_precision_and_scale(38, 10)
        .unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(d),
                Arc::new(Float64Array::from(vec![
                    Some(f64::NAN),
                    Some(f64::NEG_INFINITY),
                ])),
            ],
        )
        .unwrap();
        let rows = record_batch_to_json(&batch).unwrap();
        assert_eq!(rows[0]["d"], "1234567890123456789012345678.9012345678");
        assert_eq!(rows[1]["d"], Value::Null);
        assert_eq!(rows[0]["f"], "NaN");
        assert_eq!(rows[1]["f"], "-Infinity");
    }
}
