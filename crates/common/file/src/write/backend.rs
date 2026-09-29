//! Where finished files go.
//!
//! The shared writer ([`FileWriter`](super::FileWriter)) encodes every file
//! into a local scratch file and hands the finished file to a
//! [`StorageBackend`], which publishes it. A backend therefore never sees a
//! record or a format — only names and finished bytes — so adding a storage
//! target is a matter of implementing the dozen methods below.
//!
//! Names are file names relative to one of two [`Area`]s: the destination
//! (the directory or key prefix the output lands in) and the staging area a
//! `write_mode: overwrite` run writes into before its commit swaps it in.
//!
//! Every method is called from a blocking context (never directly on an
//! async worker thread; see `FileWriter`'s notes on `block_in_place`), so a
//! backend over an async client may block on it.

use faucet_core::FaucetError;
use std::path::{Path, PathBuf};

/// One of the two places a backend holds files.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Area {
    /// Where the output lands.
    Destination,
    /// Where an overwrite run stages its files until the commit.
    Staging,
}

/// Storage for finished files. Object-safe: the writer holds an
/// `Arc<dyn StorageBackend>`.
pub trait StorageBackend: Send + Sync {
    /// A human-readable location of `name` in `area` (a path or URL), used in
    /// errors and logs.
    fn describe(&self, area: Area, name: &str) -> String;

    /// The local path of `name`, when the backend stores files locally. Used
    /// to record local outputs for previews. Default: `None`.
    fn local_path(&self, _area: Area, _name: &str) -> Option<PathBuf> {
        None
    }

    /// A local path where the writer may build `name` before [`commit`](Self::commit).
    /// The writer also creates siblings of it by appending suffixes. A local
    /// backend puts it beside the destination so the commit is a rename; a
    /// remote one uses a temporary directory.
    fn scratch_path(&self, area: Area, name: &str) -> Result<PathBuf, FaucetError>;

    /// Remove scratch files a crashed run left in `area`, for the names
    /// `ours` accepts. Best effort. Default: nothing.
    fn remove_stale_scratch(&self, _area: Area, _ours: &dyn Fn(&str) -> bool) {}

    /// Make `area` ready for writes (create the directory, check the
    /// bucket). Called before the first file of a run is opened in it.
    fn prepare(&self, area: Area) -> Result<(), FaucetError>;

    /// Names of the files directly in `area` (not recursive).
    fn list(&self, area: Area) -> Result<Vec<String>, FaucetError>;

    /// Whether `name` exists in `area`.
    fn exists(&self, area: Area, name: &str) -> Result<bool, FaucetError>;

    /// Copy `name` from `area` into the local file `to` (for appending to it).
    fn fetch(&self, area: Area, name: &str, to: &Path) -> Result<(), FaucetError>;

    /// Publish the finished local file `scratch` as `name` in `area`,
    /// replacing any existing file atomically: a reader sees the old file or
    /// the new one, never a partial one. `scratch` is consumed.
    fn commit(&self, scratch: &Path, area: Area, name: &str) -> Result<(), FaucetError>;

    /// Delete `name` from `area`. A missing file is not an error.
    fn delete(&self, area: Area, name: &str) -> Result<(), FaucetError>;

    /// Start an overwrite run: create an empty staging area, discarding any
    /// left by an earlier run.
    fn begin_staging(&self) -> Result<(), FaucetError>;

    /// Whether [`begin_staging`](Self::begin_staging) ran and the staging area
    /// still exists. The overwrite lifecycle runs on different sink instances,
    /// so this must be answered from storage, not memory.
    fn staging_ready(&self) -> Result<bool, FaucetError>;

    /// Move `name` from the staging area to the destination, replacing any
    /// file of that name.
    fn promote(&self, name: &str) -> Result<(), FaucetError>;

    /// Remove the staging area and everything in it. A missing area is not
    /// an error.
    fn clear_staging(&self) -> Result<(), FaucetError>;
}
