//! Apache ORC (#719) — **read-only**.
//!
//! Decoded with `orc-rust`, which reads straight into Arrow `RecordBatch`es,
//! stripe by stripe: memory is bounded by one stripe's decoded batch (capped at
//! the configured batch size), not by the file. `orc.columns` projects
//! top-level columns before any stripe is decoded.
//!
//! There is no ORC writer here by design. `orc-rust`'s writer is experimental:
//! it covers primitive columns only, panics on nested and temporal types, and
//! writes no compression — not something to put behind a sink a pipeline
//! trusts. A sink asked for `format: orc` refuses at encode time; Parquet is
//! the columnar format to write.
//!
//! On the record path the batches go through
//! [`record_batch_to_values`](crate::columnar::record_batch_to_values), so an
//! ORC file and a Parquet file with the same content produce the same records.

use super::OrcOptions;
use crate::error::FaucetError;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use orc_rust::projection::ProjectionMask;
use orc_rust::reader::ChunkReader;
use serde_json::Value;

/// Where an ORC file's bytes come from.
///
/// A closed set rather than a generic reader so `orc-rust`'s trait never
/// appears in this crate's public API.
#[derive(Debug)]
pub enum OrcInput {
    /// A local file, read by stripe with positioned reads.
    File(std::fs::File),
    /// A whole object already in memory.
    Bytes(bytes::Bytes),
}

/// Decode a whole ORC object into records.
pub fn decode(bytes: &[u8], opts: &OrcOptions) -> Result<Vec<Value>, FaucetError> {
    let mut out = Vec::new();
    read_batches(
        OrcInput::Bytes(bytes::Bytes::copy_from_slice(bytes)),
        opts,
        0,
        &mut |b| {
            out.extend(crate::columnar::record_batch_to_values(&b)?);
            Ok(())
        },
    )?;
    Ok(out)
}

/// Stream an ORC file's batches (at most `batch_size` rows each; `0` means the
/// reader's own stripe-sized batches). Returns the (projected) Arrow schema,
/// known even for a file with no rows.
pub fn read_batches(
    input: OrcInput,
    opts: &OrcOptions,
    batch_size: usize,
    f: &mut dyn FnMut(RecordBatch) -> Result<(), FaucetError>,
) -> Result<SchemaRef, FaucetError> {
    read_batches_checked(input, opts, batch_size, &mut |_| Ok(()), f)
}

/// [`read_batches`], calling `check` with the file's (projected) schema
/// before any stripe is decoded — so a caller can refuse a file whose shape
/// it cannot accept without having emitted any of its rows.
pub fn read_batches_checked(
    input: OrcInput,
    opts: &OrcOptions,
    batch_size: usize,
    check: &mut dyn FnMut(&SchemaRef) -> Result<(), FaucetError>,
    f: &mut dyn FnMut(RecordBatch) -> Result<(), FaucetError>,
) -> Result<SchemaRef, FaucetError> {
    match input {
        OrcInput::File(file) => read_from(file, opts, batch_size, check, f),
        OrcInput::Bytes(b) => read_from(b, opts, batch_size, check, f),
    }
}

fn read_from<R: ChunkReader>(
    reader: R,
    opts: &OrcOptions,
    batch_size: usize,
    check: &mut dyn FnMut(&SchemaRef) -> Result<(), FaucetError>,
    f: &mut dyn FnMut(RecordBatch) -> Result<(), FaucetError>,
) -> Result<SchemaRef, FaucetError> {
    let mut builder = orc_rust::ArrowReaderBuilder::try_new(reader)
        .map_err(|e| FaucetError::Source(format!("orc: not a readable ORC file: {e}")))?;
    if let Some(cols) = &opts.columns {
        let root = builder.file_metadata().root_data_type();
        let known: Vec<&str> = root.children().iter().map(|c| c.name()).collect();
        if let Some(missing) = cols.iter().find(|c| !known.contains(&c.as_str())) {
            return Err(FaucetError::Source(format!(
                "orc: column {missing:?} is not in the file (columns: {})",
                known.join(", ")
            )));
        }
        let mask = ProjectionMask::named_roots(root, cols);
        builder = builder.with_projection(mask);
    }
    if batch_size > 0 {
        builder = builder.with_batch_size(batch_size);
    }
    let schema = builder.schema();
    check(&schema)?;
    for batch in builder.build() {
        let batch = batch.map_err(|e| FaucetError::Source(format!("orc: decode error: {e}")))?;
        if batch.num_rows() > 0 {
            f(batch)?;
        }
    }
    Ok(schema)
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE: &[u8] = include_bytes!("../../tests/fixtures/orc/people.orc");

    #[test]
    fn reads_every_column_and_row() {
        let rows = decode(FIXTURE, &OrcOptions::default()).expect("decode");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0]["id"], serde_json::json!(1));
        assert_eq!(rows[0]["name"], serde_json::json!("ada"));
        assert!(rows[0].get("score").is_some());
    }

    #[test]
    fn projection_keeps_only_the_named_columns() {
        let opts = OrcOptions {
            columns: Some(vec!["name".into()]),
        };
        let mut schemas = Vec::new();
        let mut rows = Vec::new();
        let schema = read_batches(
            OrcInput::Bytes(bytes::Bytes::from_static(FIXTURE)),
            &opts,
            2,
            &mut |b| {
                schemas.push(b.schema());
                rows.extend(crate::columnar::record_batch_to_values(&b)?);
                Ok(())
            },
        )
        .expect("read");
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schemas.len(), 2, "batch size 2 over 3 rows");
        assert_eq!(rows[2], serde_json::json!({"name": "grace"}));
    }

    #[test]
    fn unknown_columns_and_garbage_are_errors() {
        let opts = OrcOptions {
            columns: Some(vec!["nope".into()]),
        };
        let err = decode(FIXTURE, &opts).expect_err("unknown column");
        assert!(err.to_string().contains("nope"), "{err}");
        let err = decode(b"definitely not orc", &OrcOptions::default()).expect_err("garbage");
        assert!(err.to_string().contains("orc"), "{err}");
    }

    #[test]
    fn reads_from_a_local_file() {
        let dir = tempfile::tempdir().expect("tmp");
        let path = dir.path().join("p.orc");
        std::fs::write(&path, FIXTURE).expect("write");
        let mut n = 0;
        read_batches(
            OrcInput::File(std::fs::File::open(&path).expect("open")),
            &OrcOptions::default(),
            0,
            &mut |b| {
                n += b.num_rows();
                Ok(())
            },
        )
        .expect("read");
        assert_eq!(n, 3);
    }
}
