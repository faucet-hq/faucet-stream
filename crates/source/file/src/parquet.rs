//! Parquet files, read row group by row group with the Arrow reader.

use crate::stream::Decoders;
use arrow::array::RecordBatch;
use faucet_core::{FaucetError, FileInput};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

/// Stream one Parquet file's batches (at most `batch_size` rows; `0` = the
/// reader default). The first Parquet file's schema is the reference; a file
/// that differs fails naming both.
pub(crate) fn read(
    decoders: &mut Decoders,
    path: &str,
    input: FileInput,
    batch_size: usize,
    f: &mut dyn FnMut(RecordBatch) -> Result<(), FaucetError>,
) -> Result<(), FaucetError> {
    match input {
        FileInput::File(file) => read_from(decoders, path, file, batch_size, f),
        FileInput::Bytes(b) => read_from(decoders, path, bytes::Bytes::from(b), batch_size, f),
    }
}

fn read_from<R: parquet::file::reader::ChunkReader + 'static>(
    decoders: &mut Decoders,
    path: &str,
    reader: R,
    batch_size: usize,
    f: &mut dyn FnMut(RecordBatch) -> Result<(), FaucetError>,
) -> Result<(), FaucetError> {
    let err = |e: &dyn std::fmt::Display| {
        FaucetError::Source(format!("file source: parquet '{path}': {e}"))
    };
    let mut builder = open(path, reader, decoders.parquet_columns())?;
    if batch_size > 0 {
        builder = builder.with_batch_size(batch_size);
    }
    let schema = builder.schema().clone();
    let reference = decoders.parquet_reference();
    match reference {
        Some((first, s)) if !faucet_core::file_format::container::same_shape(s, &schema) => {
            return Err(faucet_core::file_format::container::schema_conflict(
                path, first, s, &schema,
            ));
        }
        Some(_) => {}
        None => *reference = Some((path.to_string(), schema)),
    }
    for batch in builder.build().map_err(|e| err(&e))? {
        let batch = batch.map_err(|e| err(&e))?;
        if batch.num_rows() > 0 {
            f(batch)?;
        }
    }
    Ok(())
}

/// A reader builder over `reader`, projected to `columns` when set. Only the
/// projected column chunks are decoded.
fn open<R: parquet::file::reader::ChunkReader + 'static>(
    path: &str,
    reader: R,
    columns: Option<&[String]>,
) -> Result<ParquetRecordBatchReaderBuilder<R>, FaucetError> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(reader)
        .map_err(|e| FaucetError::Source(format!("file source: parquet '{path}': {e}")))?;
    let Some(columns) = columns else {
        return Ok(builder);
    };
    let available: Vec<&str> = builder
        .parquet_schema()
        .root_schema()
        .get_fields()
        .iter()
        .map(|f| f.name())
        .collect();
    if let Some(missing) = columns.iter().find(|c| !available.contains(&c.as_str())) {
        return Err(FaucetError::Source(format!(
            "file source: parquet '{path}': column '{missing}' in `parquet.columns` is not in the \
             file (available: {})",
            available.join(", ")
        )));
    }
    let mask =
        ProjectionMask::columns(builder.parquet_schema(), columns.iter().map(String::as_str));
    Ok(builder.with_projection(mask))
}

/// Read every file's footer and fail, naming both files, when two schemas
/// differ — before any row is read, so a mismatch in a late file never
/// leaves earlier files half-delivered.
pub(crate) fn check_schemas(
    paths: &[String],
    columns: Option<&[String]>,
) -> Result<(), FaucetError> {
    let mut first: Option<(&str, arrow::datatypes::SchemaRef)> = None;
    for path in paths {
        let file = std::fs::File::open(path)
            .map_err(|e| FaucetError::Source(format!("file source: read '{path}': {e}")))?;
        let builder = open(path, file, columns)?;
        let schema = builder.schema().clone();
        match &first {
            Some((p, s)) if !faucet_core::file_format::container::same_shape(s, &schema) => {
                return Err(faucet_core::file_format::container::schema_conflict(
                    path, p, s, &schema,
                ));
            }
            Some(_) => {}
            None => first = Some((path, schema)),
        }
    }
    Ok(())
}
