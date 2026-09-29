#![cfg_attr(docsrs, feature(doc_cfg))]
//! Local file source for faucet-stream: JSON Lines, JSON, CSV, Excel, XML,
//! Parquet, Avro and ORC from a path, a directory, a glob, or one
//! `http(s)://` URL — the local counterpart of the object-store sources.
//!
//! Formats resolve per file from the extension (through a `.gz` / `.zst`
//! suffix) unless one is set; incremental mode reads only files that are new
//! since the previous run, by modification time or by name.

pub mod config;
#[cfg(feature = "encryption")]
mod decrypt;
pub mod http;
#[cfg(feature = "file-format-parquet")]
mod parquet;
pub mod plan;
mod stream;

pub use config::{
    FileSourceConfig, FileSourceFormat, IncrementalBy, IncrementalSpec, ParquetReadOptions,
};
pub use stream::FileSource;

#[cfg(not(feature = "file-format-parquet"))]
pub(crate) fn parquet_missing() -> faucet_core::FaucetError {
    faucet_core::FaucetError::Config(
        "file source: Parquet files need the `file-format-parquet` build feature".into(),
    )
}

/// Read every record at `path` with every default — format resolved per file
/// from its extension, compression from its suffix. The one-call read-back a
/// test or a script wants; build a [`FileSource`] for anything more.
pub async fn read_records(
    path: impl Into<String>,
) -> Result<Vec<faucet_core::Value>, faucet_core::FaucetError> {
    use faucet_core::Source;
    FileSource::new(FileSourceConfig::new(path))?
        .fetch_all()
        .await
}
