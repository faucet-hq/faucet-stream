//! The shared file-writing layer (#777): every format, compression,
//! encryption, `{part}` rollover, the `if_exists` modes and the
//! `write_mode: overwrite` swap, over a pluggable [`StorageBackend`].
//!
//! A sink maps its config onto a [`WriteConfig`], which gives it the
//! [`WriteSettings`] and the output's [`NameTemplate`]; it builds a backend
//! for its storage, wraps a [`FileWriter`] in a [`WriterSink`], and forwards
//! its `Sink` impl with [`delegate_sink!`](crate::delegate_sink). The local
//! `file` sink is exactly that over [`LocalBackend`].
//!
//! Every [`FileWriter`] method is `async`. Encoding runs on the calling task
//! (moved off the async worker with `block_in_place` on a multi-thread
//! runtime); storage I/O is awaited, so dropping a sink's future — a
//! cancelled run — stops waiting on a hung upload.
//!
//! Scratch files are not encrypted while a run is in progress: an encrypted
//! output is sealed when each file is published. Scratch files that hold
//! plaintext of an encrypted output are created readable by their owner
//! only, and a remote backend keeps its scratch files in a private
//! directory.
//!
//! **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
//! minor release; any change is called out in the changelog.

mod backend;
mod config;
mod encode;
mod layout;
mod local;
mod options;
#[cfg(feature = "file-format-parquet")]
mod parquet;
mod remote;
mod sink;
mod writer;

pub use backend::{Area, PartStream, StorageBackend};
pub use config::WriteConfig;
pub use layout::{
    BODY_SUFFIX, NameTemplate, PART_WIDTH, SCRATCH_ROLES, SWAP_PREFIX, TMP_SUFFIX, is_scratch_name,
    is_swap_dir_name,
};
pub use local::LocalBackend;
pub use options::{
    DEFAULT_ROW_GROUP_SIZE, FileWriteMode, IfExists, JsonLinesOptions, PART_TOKEN, ParquetCodec,
    ParquetField, ParquetOptions, ParquetType, validate_parquet,
};
pub use remote::{
    BoxFuture, MultipartClient, MultipartUpload, ObjectClient, RemoteBackend, content_type,
    object_layout,
};
pub use sink::{SinkIdentity, WriterSink};
pub use writer::{COMMIT_MARKER, FileWriter, SWAP_MARKER, WriteSettings};
