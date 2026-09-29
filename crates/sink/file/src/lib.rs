#![cfg_attr(docsrs, feature(doc_cfg))]
//! Local file sink for faucet-stream: JSON Lines, JSON arrays, CSV, XML,
//! Excel, Avro, Parquet and raw text to one file or a directory of rolled
//! files — the write-side counterpart of `faucet-source-file`.
//!
//! The format comes from the path's extension unless one is set; `.gz` /
//! `.zst` select compression. Every file is written beside its final name and
//! renamed into place on flush, so a crashed run never leaves a
//! complete-looking partial file, and `write_mode: overwrite` replaces the
//! whole output set only when the run succeeds.

pub mod config;
mod sink;

pub use config::{
    FileMode, FileSinkConfig, FileSinkFormat, FileWriteMode, JsonLinesOptions, PART_TOKEN,
    ParquetCodec, ParquetField, ParquetOptions, ParquetType,
};
pub use faucet_core::{FaucetError, Sink};
pub use sink::FileSink;
