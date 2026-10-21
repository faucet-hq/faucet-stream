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
use faucet_core::check::{CheckContext, CheckReport};
use faucet_core::io::CallContext;
use faucet_core::observability::RoundtripRecorder;
use std::path::{Path, PathBuf};
use std::sync::Arc;

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

    /// Install the run's round-trip recorder the backend's requests report
    /// to. Default: the backend counts nothing.
    fn set_roundtrip_recorder(&self, _recorder: Arc<RoundtripRecorder>) {}

    /// Install the run's call context (#880) every request of the backend
    /// runs through. Default: the backend makes no remote calls.
    fn set_call_context(&self, _ctx: CallContext) {}

    /// Join overwrite run `token`
    /// ([`faucet_core::idempotency::OVERWRITE_RUN_KEY`]): from now on
    /// [`Area::Swap`] is that run's own area
    /// ([`NameTemplate::swap_run_dir_name`](super::NameTemplate::swap_run_dir_name)),
    /// so concurrent overwrite runs of one output never share one. Called
    /// once, before the first use. Default: refused — the backend has one
    /// fixed swap area.
    fn join_overwrite_run(&self, token: &str) -> Result<(), FaucetError> {
        let _ = token;
        Err(FaucetError::Config(
            "file sink: this storage backend cannot stage overwrite runs apart".into(),
        ))
    }

    /// The run tokens of every tokened swap area of this output that exists
    /// (any run's, this one's included). Default: none.
    async fn swap_runs(&self) -> Result<Vec<String>, FaucetError> {
        Ok(Vec::new())
    }

    /// Delete the swap area of overwrite run `run` (`None`: the untokened
    /// area) and everything in it. A missing area is not an error. Default:
    /// nothing to delete.
    async fn drop_swap_run(&self, run: Option<&str>) -> Result<(), FaucetError> {
        let _ = run;
        Ok(())
    }

    /// A non-mutating preflight probe of the storage (reachability,
    /// credentials). `None` when the backend has none, so the sink's own
    /// probe answers. Default: `None`.
    async fn check(&self, _ctx: &CheckContext) -> Result<Option<CheckReport>, FaucetError> {
        Ok(None)
    }
}

/// Implement [`StorageBackend`] for a type by forwarding every method to the
/// backend in one of its fields: `delegate_storage_backend!(S3Backend, remote);`.
/// A store's common crate wraps a [`RemoteBackend`](super::RemoteBackend) in
/// its own named type this way.
#[macro_export]
macro_rules! delegate_storage_backend {
    ($ty:ty, $field:ident) => {
        #[$crate::__private::async_trait]
        impl $crate::write::StorageBackend for $ty {
            fn describe(&self, area: $crate::write::Area, name: &str) -> ::std::string::String {
                $crate::write::StorageBackend::describe(&self.$field, area, name)
            }
            fn local_path(
                &self,
                area: $crate::write::Area,
                name: &str,
            ) -> ::std::option::Option<::std::path::PathBuf> {
                $crate::write::StorageBackend::local_path(&self.$field, area, name)
            }
            fn scratch_path(&self, area: $crate::write::Area, name: &str) -> ::std::path::PathBuf {
                $crate::write::StorageBackend::scratch_path(&self.$field, area, name)
            }
            async fn prepare(
                &self,
                area: $crate::write::Area,
            ) -> ::std::result::Result<(), $crate::__private::FaucetError> {
                $crate::write::StorageBackend::prepare(&self.$field, area).await
            }
            async fn list(
                &self,
                area: $crate::write::Area,
            ) -> ::std::result::Result<
                ::std::vec::Vec<::std::string::String>,
                $crate::__private::FaucetError,
            > {
                $crate::write::StorageBackend::list(&self.$field, area).await
            }
            async fn exists(
                &self,
                area: $crate::write::Area,
                name: &str,
            ) -> ::std::result::Result<bool, $crate::__private::FaucetError> {
                $crate::write::StorageBackend::exists(&self.$field, area, name).await
            }
            async fn fetch(
                &self,
                area: $crate::write::Area,
                name: &str,
                to: &::std::path::Path,
            ) -> ::std::result::Result<(), $crate::__private::FaucetError> {
                $crate::write::StorageBackend::fetch(&self.$field, area, name, to).await
            }
            async fn publish(
                &self,
                scratch: &::std::path::Path,
                area: $crate::write::Area,
                name: &str,
            ) -> ::std::result::Result<(), $crate::__private::FaucetError> {
                $crate::write::StorageBackend::publish(&self.$field, scratch, area, name).await
            }
            async fn settle(&self) -> ::std::result::Result<(), $crate::__private::FaucetError> {
                $crate::write::StorageBackend::settle(&self.$field).await
            }
            async fn cancel(&self) {
                $crate::write::StorageBackend::cancel(&self.$field).await
            }
            async fn delete(
                &self,
                area: $crate::write::Area,
                name: &str,
            ) -> ::std::result::Result<(), $crate::__private::FaucetError> {
                $crate::write::StorageBackend::delete(&self.$field, area, name).await
            }
            async fn delete_many(
                &self,
                area: $crate::write::Area,
                names: &[::std::string::String],
            ) -> ::std::result::Result<(), $crate::__private::FaucetError> {
                $crate::write::StorageBackend::delete_many(&self.$field, area, names).await
            }
            async fn promote(
                &self,
                name: &str,
            ) -> ::std::result::Result<(), $crate::__private::FaucetError> {
                $crate::write::StorageBackend::promote(&self.$field, name).await
            }
            fn part_size(&self) -> ::std::option::Option<usize> {
                $crate::write::StorageBackend::part_size(&self.$field)
            }
            async fn open_stream(
                &self,
                area: $crate::write::Area,
                name: &str,
            ) -> ::std::result::Result<
                ::std::boxed::Box<dyn $crate::write::PartStream>,
                $crate::__private::FaucetError,
            > {
                $crate::write::StorageBackend::open_stream(&self.$field, area, name).await
            }
            fn set_roundtrip_recorder(
                &self,
                recorder: ::std::sync::Arc<$crate::__private::RoundtripRecorder>,
            ) {
                $crate::write::StorageBackend::set_roundtrip_recorder(&self.$field, recorder)
            }
            fn set_call_context(&self, ctx: $crate::__private::CallContext) {
                $crate::write::StorageBackend::set_call_context(&self.$field, ctx)
            }
            fn join_overwrite_run(
                &self,
                token: &str,
            ) -> ::std::result::Result<(), $crate::__private::FaucetError> {
                $crate::write::StorageBackend::join_overwrite_run(&self.$field, token)
            }
            async fn swap_runs(
                &self,
            ) -> ::std::result::Result<
                ::std::vec::Vec<::std::string::String>,
                $crate::__private::FaucetError,
            > {
                $crate::write::StorageBackend::swap_runs(&self.$field).await
            }
            async fn drop_swap_run(
                &self,
                run: ::std::option::Option<&str>,
            ) -> ::std::result::Result<(), $crate::__private::FaucetError> {
                $crate::write::StorageBackend::drop_swap_run(&self.$field, run).await
            }
            async fn check(
                &self,
                ctx: &$crate::__private::CheckContext,
            ) -> ::std::result::Result<
                ::std::option::Option<$crate::__private::CheckReport>,
                $crate::__private::FaucetError,
            > {
                $crate::write::StorageBackend::check(&self.$field, ctx).await
            }
        }
    };
}
