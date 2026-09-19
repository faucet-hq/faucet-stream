//! Iceberg writer pipeline: `RecordBatch` → Parquet data files → `Vec<DataFile>`.
//!
//! `TableWriter` wraps a single `DataFileWriter` for a loaded Iceberg table.
//! File rollover at `target_file_size_mb` is handled internally by the
//! `RollingFileWriterBuilder`; callers just call `write(batch).await` and
//! `close().await` when done.
//!
//! ## Compression
//!
//! `compression_from_str` maps the user-facing string (`"snappy"`, `"zstd"`,
//! `"gzip"`, `"lz4"`, `"none"`) to `parquet::basic::Compression`. An
//! unrecognised string returns `FaucetError::Config`.

use arrow::record_batch::RecordBatch;
use faucet_core::FaucetError;
use iceberg::spec::{DataFile, DataFileFormat};
use iceberg::table::Table;
use iceberg::writer::IcebergWriter;
use iceberg::writer::IcebergWriterBuilder;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

// ── Compression mapping ───────────────────────────────────────────────────────

/// Map a compression string to a `parquet::basic::Compression` variant.
///
/// Recognised values (case-insensitive): `"snappy"`, `"zstd"`, `"gzip"`,
/// `"lz4"`, `"none"`. Any unrecognised value returns
/// `FaucetError::Config` with a clear message.
pub(crate) fn compression_from_str(s: &str) -> Result<Compression, FaucetError> {
    match s.to_ascii_lowercase().as_str() {
        "snappy" => Ok(Compression::SNAPPY),
        "zstd" => Ok(Compression::ZSTD(Default::default())),
        "gzip" => Ok(Compression::GZIP(Default::default())),
        "lz4" => Ok(Compression::LZ4),
        "none" | "uncompressed" => Ok(Compression::UNCOMPRESSED),
        other => Err(FaucetError::Config(format!(
            "iceberg: unknown parquet compression codec {other:?}; \
             expected one of: snappy, zstd, gzip, lz4, none"
        ))),
    }
}

// ── TableWriter ───────────────────────────────────────────────────────────────

/// Wraps an Iceberg `DataFileWriter` for a loaded table.
///
/// File rollover at `target_file_size_mb` is handled by the
/// `RollingFileWriterBuilder` passed at construction time; callers write
/// one `RecordBatch` per `write()` call and collect all `DataFile`s from
/// `close()`.
pub(crate) struct TableWriter {
    inner: Box<dyn IcebergWriter>,
}

impl TableWriter {
    /// Build a writer for `table` using the given compression + target size.
    pub(crate) async fn new(
        table: &Table,
        compression: Compression,
        target_file_size_mb: u64,
    ) -> Result<Self, FaucetError> {
        let loc_gen = DefaultLocationGenerator::new(table.metadata())
            .map_err(|e| FaucetError::Sink(format!("iceberg: location generator failed: {e}")))?;

        // Include a per-writer UUID suffix so that each `TableWriter` instance
        // produces uniquely-named files (e.g. `part-00000-<uuid>.parquet`).
        // Without this, successive writers all generate `part-00000.parquet`
        // and the second `fast_append` commit is rejected by the iceberg SDK
        // with "Cannot add files that are already referenced by table".
        let writer_id = Uuid::new_v4();
        let name_gen = DefaultFileNameGenerator::new(
            "part".to_string(),
            Some(writer_id.to_string()),
            DataFileFormat::Parquet,
        );

        let props = WriterProperties::builder()
            .set_compression(compression)
            .build();

        // `ParquetWriterBuilder::new` takes an iceberg `SchemaRef`, NOT an
        // Arrow `SchemaRef`. The iceberg 0.10.0 API uses the table's current
        // schema as returned by `Table::metadata().current_schema()`.
        let parquet_builder =
            ParquetWriterBuilder::new(props, table.metadata().current_schema().clone());

        let target_bytes = (target_file_size_mb as usize).saturating_mul(1024 * 1024);

        let rolling = RollingFileWriterBuilder::new(
            parquet_builder,
            target_bytes,
            table.file_io().clone(),
            loc_gen,
            name_gen,
        );

        let inner = DataFileWriterBuilder::new(rolling)
            .build(None)
            .await
            .map_err(|e| FaucetError::Sink(format!("iceberg: failed to open writer: {e}")))?;

        Ok(Self {
            // `DataFileWriterBuilder::build` returns `impl IcebergWriter`, which
            // we box to erase the concrete type (keeps the public API stable and
            // avoids a complex generic type parameter on `IcebergSink`).
            inner: Box::new(inner),
        })
    }

    /// Write a single `RecordBatch` to the underlying Parquet data file(s).
    ///
    /// File rollover at the configured target size is handled internally by the
    /// `RollingFileWriterBuilder`; callers do not need to manage it.
    pub(crate) async fn write(&mut self, batch: RecordBatch) -> Result<(), FaucetError> {
        self.inner
            .write(batch)
            .await
            .map_err(|e| FaucetError::Sink(format!("iceberg: write failed: {e}")))
    }

    /// Finalise all open Parquet files and return the accumulated `DataFile`s
    /// that must be committed in a `fast_append` transaction.
    pub(crate) async fn close(mut self) -> Result<Vec<DataFile>, FaucetError> {
        self.inner
            .close()
            .await
            .map_err(|e| FaucetError::Sink(format!("iceberg: close failed: {e}")))
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compression_snappy() {
        assert!(matches!(
            compression_from_str("snappy").unwrap(),
            Compression::SNAPPY
        ));
    }

    #[test]
    fn compression_zstd() {
        assert!(matches!(
            compression_from_str("zstd").unwrap(),
            Compression::ZSTD(_)
        ));
    }

    #[test]
    fn compression_gzip() {
        assert!(matches!(
            compression_from_str("gzip").unwrap(),
            Compression::GZIP(_)
        ));
    }

    #[test]
    fn compression_lz4() {
        assert!(matches!(
            compression_from_str("lz4").unwrap(),
            Compression::LZ4
        ));
    }

    #[test]
    fn compression_none() {
        assert!(matches!(
            compression_from_str("none").unwrap(),
            Compression::UNCOMPRESSED
        ));
    }

    #[test]
    fn compression_uncompressed_alias() {
        assert!(matches!(
            compression_from_str("uncompressed").unwrap(),
            Compression::UNCOMPRESSED
        ));
    }

    #[test]
    fn compression_case_insensitive() {
        assert!(matches!(
            compression_from_str("SNAPPY").unwrap(),
            Compression::SNAPPY
        ));
        assert!(matches!(
            compression_from_str("Zstd").unwrap(),
            Compression::ZSTD(_)
        ));
    }

    #[test]
    fn compression_unknown_returns_config_error() {
        let err = compression_from_str("brotli").unwrap_err();
        assert!(matches!(err, FaucetError::Config(_)));
        let msg = err.to_string();
        assert!(
            msg.contains("brotli"),
            "error should name the unknown codec: {msg}"
        );
    }

    #[test]
    fn compression_empty_returns_config_error() {
        let err = compression_from_str("").unwrap_err();
        assert!(matches!(err, FaucetError::Config(_)));
    }
}
