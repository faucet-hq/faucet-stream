//! Parquet files, read row group by row group with the Arrow reader.

use crate::stream::Decoders;
use arrow::array::RecordBatch;
use faucet_core::{FaucetError, FileInput};
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
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(reader).map_err(|e| err(&e))?;
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
