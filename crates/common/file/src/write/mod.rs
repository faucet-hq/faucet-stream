//! The shared file-writing layer (#777): every format, compression,
//! encryption, `{part}` rollover, append / overwrite modes and the
//! `write_mode: overwrite` stage-and-swap, over a pluggable
//! [`StorageBackend`].
//!
//! A sink builds a [`WriteSettings`] from its config, a [`NameTemplate`] from
//! its path or key, and a backend for its storage, then forwards its `Sink`
//! calls to a [`FileWriter`]. The local `file` sink is exactly that over
//! [`LocalBackend`].
//!
//! [`FileWriter`]'s methods do blocking I/O. Call them through
//! [`blocking`], which uses `block_in_place` on a multi-thread runtime.

mod backend;
mod encode;
mod layout;
mod local;
mod options;
#[cfg(feature = "file-format-parquet")]
mod parquet;
mod remote;
mod writer;

pub use backend::{Area, StorageBackend};
pub use layout::{BODY_SUFFIX, NameTemplate, STAGING_PREFIX, TMP_SUFFIX, io_err, tmp_path};
pub use local::{LocalBackend, sync_dir};
pub use options::{
    DEFAULT_ROW_GROUP_SIZE, FileMode, FileWriteMode, JsonLinesOptions, PART_TOKEN, ParquetCodec,
    ParquetField, ParquetOptions, ParquetType, validate_parquet,
};
pub use remote::{ObjectClient, RemoteBackend, STAGING_MARKER, content_type, object_layout, run};
pub use writer::{FileWriter, WriteSettings, blocking};
