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
//!
//! **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
//! minor release; any change is called out in the changelog.

pub mod config;
mod sink;

pub use config::{
    FileSinkConfig, FileSinkFormat, FileWriteMode, IfExists, JsonLinesOptions, PART_TOKEN,
    ParquetCodec, ParquetField, ParquetOptions, ParquetType,
};
pub use faucet_core::{FaucetError, Sink};
pub use sink::FileSink;
