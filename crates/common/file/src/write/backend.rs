//! Where finished files go.
//!
//! The shared writer ([`FileWriter`](super::FileWriter)) encodes every file
//! into a local scratch file and hands the finished file to a
//! [`StorageBackend`], which publishes it. A backend therefore never sees a
//! record or a format — only names and finished bytes — so adding a storage
//! target is a matter of implementing the methods below.
//!
//! Names are file names relative to one of two [`Area`]s: the destination
//! (the directory or key prefix the output lands in) and the swap area a
//! `write_mode: overwrite` run writes into before its files are moved into
//! place.
//!
//! Every method that touches storage is `async`, so dropping the caller's
//! future (a cancelled run) stops waiting on it.
//!
//! **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
//! minor release; any change is called out in the changelog.

use faucet_core::FaucetError;
use std::path::{Path, PathBuf};

/// One of the two places a backend holds files.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Area {
    /// Where the output lands.
    Destination,
    /// Where an overwrite run keeps its files until they are moved into
    /// place.
    Swap,
}

/// Parts of one object uploaded while it is still being written, for a
/// backend whose store can assemble an object from parts.
///
/// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
/// minor release; any change is called out in the changelog.
#[faucet_core::async_trait]
pub trait PartStream: Send {
    /// Upload the next part. It may return before the part has landed (the
    /// upload continues in the background); an error from it is reported by a
    /// later `put` or by [`finish`](Self::finish).
    async fn put(&mut self, part: Vec<u8>) -> Result<(), FaucetError>;
    /// Wait for every part and publish the object. On an error nothing is
    /// published.
    async fn finish(self: Box<Self>) -> Result<(), FaucetError>;
    /// Stop: nothing is published and the parts are discarded.
    async fn abort(self: Box<Self>);
}

/// Storage for finished files. Object-safe: the writer holds an
/// `Arc<dyn StorageBackend>`.
///
/// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
/// minor release; any change is called out in the changelog.
#[faucet_core::async_trait]
pub trait StorageBackend: Send + Sync {
    /// A human-readable location of `name` in `area` (a path or URL), used in
    /// errors and logs.
    fn describe(&self, area: Area, name: &str) -> String;

    /// The local path of `name`, when the backend stores files locally. Used
    /// to record local outputs for previews. Default: `None`.
    fn local_path(&self, _area: Area, _name: &str) -> Option<PathBuf> {
        None
    }

    /// A local path where the writer may build `name` before
    /// [`publish`](Self::publish). The writer also creates siblings of it by
    /// appending one of [`SCRATCH_ROLES`](super::SCRATCH_ROLES). A local
    /// backend puts it beside the destination so publishing is a rename; a
    /// remote one uses a private scratch directory.
    fn scratch_path(&self, area: Area, name: &str) -> PathBuf;

    /// Make `area` ready for writes (create the directory, remove scratch
    /// files a crashed run of this output left behind). Called before the
    /// first file of a run is opened in it.
    async fn prepare(&self, area: Area) -> Result<(), FaucetError>;

    /// Names of the files directly in `area` (not recursive). A missing area
    /// lists as empty.
    async fn list(&self, area: Area) -> Result<Vec<String>, FaucetError>;

    /// Whether `name` exists in `area`.
    async fn exists(&self, area: Area, name: &str) -> Result<bool, FaucetError>;

    /// Copy `name` from `area` into the local file `to`.
    async fn fetch(&self, area: Area, name: &str, to: &Path) -> Result<(), FaucetError>;

    /// Publish the finished local file `scratch` as `name` in `area`,
    /// replacing any existing file atomically: a reader sees the old file or
    /// the new one, never a partial one. `scratch` is consumed.
    ///
    /// A backend may return before the file is published — an upload left
    /// running in the background — provided [`settle`](Self::settle) waits
    /// for it and reports its error.
    async fn publish(&self, scratch: &Path, area: Area, name: &str) -> Result<(), FaucetError>;

    /// Wait for every [`publish`](Self::publish) this backend has accepted to
    /// land, and return the first error among them. The writer calls it
    /// before a flush returns and before it lists, moves or deletes files.
    /// Default: publishing is synchronous, so there is nothing to wait for.
    async fn settle(&self) -> Result<(), FaucetError> {
        Ok(())
    }

    /// Give up on every publish still in flight: wait for each to end, then
    /// delete whatever it published, so none of them has an effect. Errors
    /// are ignored. Default: publishing is synchronous, so there is nothing
    /// in flight.
    async fn cancel(&self) {}

    /// Delete `name` from `area`. A missing file is not an error. Deleting
    /// the last file of the swap area may remove the area itself.
    async fn delete(&self, area: Area, name: &str) -> Result<(), FaucetError>;

    /// Delete every file in `names` from `area`. Default: up to eight
    /// [`delete`](Self::delete)s at a time; a backend over a store with a
    /// batch delete overrides it.
    async fn delete_many(&self, area: Area, names: &[String]) -> Result<(), FaucetError> {
        use futures::stream::{self, StreamExt, TryStreamExt};
        stream::iter(names.iter().cloned())
            .map(|name| async move { self.delete(area, &name).await })
            .buffer_unordered(8)
            .try_collect::<Vec<()>>()
            .await
            .map(|_| ())
    }

    /// Move `name` from the swap area to the destination, replacing any file
    /// of that name.
    async fn promote(&self, name: &str) -> Result<(), FaucetError>;

    /// The part size of [`open_stream`](Self::open_stream), when the backend
    /// can upload an object in parts while it is being written. Default:
    /// `None` — every file is built whole before it is published.
    fn part_size(&self) -> Option<usize> {
        None
    }

    /// Start uploading `name` in `area` part by part. Only called when
    /// [`part_size`](Self::part_size) is `Some`.
    async fn open_stream(
        &self,
        _area: Area,
        name: &str,
    ) -> Result<Box<dyn PartStream>, FaucetError> {
        Err(FaucetError::Sink(format!(
            "file sink: '{name}' cannot be uploaded in parts by this backend"
        )))
    }
}
