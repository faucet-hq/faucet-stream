//! A [`StorageBackend`] over a remote object store or file server (#777).
//!
//! Each connector implements the small async [`ObjectClient`] with its own
//! client (S3, GCS, Azure Blob, SFTP); [`RemoteBackend`] turns it into a
//! backend: files are built in a private local scratch directory, and a
//! publish is one upload that the store makes visible atomically (a single
//! `PUT`, a completed multipart / resumable upload, or a temp-name-then-rename).
//! A client that can also take an object in parts ([`MultipartClient`]) lets
//! line formats upload while they are written, so neither memory nor scratch
//! disk grows with the object.
//!
//! Keys are the backend's `base` (a key prefix, concatenated as is) followed
//! by the file name. An overwrite run keeps its files under
//! `<base><swap name>/` until they are moved into place.
//!
//! **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
//! minor release; any change is called out in the changelog.

use super::backend::{Area, PartStream, StorageBackend};
use super::layout::NameTemplate;
use faucet_core::{Compression, FaucetError, FileFormat};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

/// A boxed, `'static` future.
pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// The operations a remote store must offer. Keys are full object keys (or
/// remote paths).
///
/// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
/// minor release; any change is called out in the changelog.
#[faucet_core::async_trait]
pub trait ObjectClient: Send + Sync {
    /// A readable location of `key` (`s3://bucket/key`, …).
    fn describe(&self, key: &str) -> String;
    /// Every key that starts with `prefix` (recursive is fine; the backend
    /// keeps only direct children).
    async fn list(&self, prefix: &str) -> Result<Vec<String>, FaucetError>;
    /// Whether `key` exists.
    async fn exists(&self, key: &str) -> Result<bool, FaucetError>;
    /// Download `key` into the local file `to`.
    async fn download(&self, key: &str, to: &Path) -> Result<(), FaucetError>;
    /// Publish the local file `from` as `key`, replacing any object of that
    /// name atomically: readers see the old object or the whole new one.
    /// Large files go up in parts; a failed upload is aborted.
    async fn upload(&self, from: &Path, key: &str) -> Result<(), FaucetError>;
    /// Delete `key`. A missing key is not an error.
    async fn delete(&self, key: &str) -> Result<(), FaucetError>;
    /// Delete every key in `keys`; a missing key is not an error. Default:
    /// one [`delete`](Self::delete) each; a store with a batch delete
    /// overrides it.
    async fn delete_many(&self, keys: &[String]) -> Result<(), FaucetError> {
        for key in keys {
            self.delete(key).await?;
        }
        Ok(())
    }
    /// Whether [`upload`](Self::upload) writes a temporary object named by
    /// [`upload_scratch_key`](super::upload_scratch_key) and renames it into
    /// place, so a crash can leave one behind for the next run to remove.
    /// Default `false` (an atomic put leaves nothing).
    fn leaves_upload_scratch(&self) -> bool {
        false
    }
    /// Move `from` to `to`, replacing `to`. Default: a server-side copy is
    /// not assumed, so the object is downloaded and re-uploaded, then
    /// `from` deleted; stores with a copy or rename override it.
    async fn rename(&self, from: &str, to: &str) -> Result<(), FaucetError> {
        let dir = tempfile::tempdir()
            .map_err(sink_io("remote rename: creating a temporary directory"))?;
        let tmp = dir.path().join("object");
        self.download(from, &tmp).await?;
        self.upload(&tmp, to).await?;
        self.delete(from).await
    }
}

/// A store that can assemble an object from parts uploaded one by one, so an
/// object can be uploaded while it is still being written.
///
/// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
/// minor release; any change is called out in the changelog.
#[faucet_core::async_trait]
pub trait MultipartClient: Send + Sync {
    /// The size of every part but the last.
    fn part_size(&self) -> usize;
    /// Start an upload of `key`. Nothing is visible until it completes.
    async fn start(&self, key: &str) -> Result<Box<dyn MultipartUpload>, FaucetError>;
}

/// One upload started by a [`MultipartClient`].
///
/// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
/// minor release; any change is called out in the changelog.
pub trait MultipartUpload: Send + Sync {
    /// Queue part `number` (from 1, called in increasing order) and return
    /// the future that uploads it. Several may run at once.
    fn put_part(&self, number: u32, body: Vec<u8>) -> BoxFuture<Result<(), FaucetError>>;
    /// Publish the object from every part put. Called once all of them
    /// have finished.
    fn complete(self: Box<Self>) -> BoxFuture<Result<(), FaucetError>>;
    /// Discard the upload and its parts.
    fn abort(self: Box<Self>) -> BoxFuture<Result<(), FaucetError>>;
}

/// A [`FaucetError::Sink`] for an I/O failure while `what`.
fn sink_io(what: impl std::fmt::Display) -> impl FnOnce(std::io::Error) -> FaucetError {
    move |e| FaucetError::Sink(format!("{what}: {e}"))
}

type Upload = tokio::task::JoinHandle<Result<(), FaucetError>>;

/// An upload running in the background, and what it publishes.
struct InFlight {
    key: String,
    handle: Upload,
}

/// Await `uploads` and return the first error; every one is awaited even
/// after a failure, so none is left running unobserved. The keys of the
/// uploads that succeeded come back too.
async fn join_all(uploads: Vec<InFlight>) -> (Result<(), FaucetError>, Vec<String>) {
    let mut first = Ok(());
    let mut landed = Vec::new();
    for u in uploads {
        let r = match u.handle.await {
            Ok(r) => r,
            Err(e) => Err(FaucetError::Sink(format!(
                "remote file sink: the upload of '{}' did not finish: {e}",
                u.key
            ))),
        };
        match r {
            Ok(()) => landed.push(u.key),
            Err(e) if first.is_ok() => first = Err(e),
            Err(_) => {}
        }
    }
    (first, landed)
}

/// A [`StorageBackend`] over an [`ObjectClient`].
///
/// With [`with_upload_concurrency`](Self::with_upload_concurrency) above 1, a
/// [`publish`](StorageBackend::publish) hands the finished file to a
/// background upload and returns, so the writer encodes the next file while
/// earlier ones are still going up; at most that many uploads (whole files
/// and parts together) are in flight, and one past the limit waits for a
/// slot. [`settle`](StorageBackend::settle) awaits them all and reports the
/// first error.
///
/// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
/// minor release; any change is called out in the changelog.
pub struct RemoteBackend {
    client: Arc<dyn ObjectClient>,
    multipart: Option<Arc<dyn MultipartClient>>,
    base: String,
    swap: String,
    template: NameTemplate,
    scratch: tempfile::TempDir,
    /// Held locked for the backend's life, so a later run can tell this
    /// scratch directory from one a crashed run left behind.
    _owner: std::fs::File,
    uploads: usize,
    slots: Arc<tokio::sync::Semaphore>,
    in_flight: std::sync::Mutex<Vec<InFlight>>,
    seq: std::sync::atomic::AtomicU64,
    scrubbed: tokio::sync::Mutex<[bool; 2]>,
}

const SCRATCH_PREFIX: &str = "faucet-remote-";
const OWNER_FILE: &str = ".owner";
/// Age past which a scratch directory without an owner lock (left by a
/// faucet that predates the lock) is treated as abandoned.
const UNOWNED_MAX_AGE: std::time::Duration = std::time::Duration::from_secs(24 * 60 * 60);

fn lock_owner(dir: &Path) -> std::io::Result<std::fs::File> {
    let file = std::fs::File::create(dir.join(OWNER_FILE))?;
    file.try_lock().map_err(std::io::Error::other)?;
    Ok(file)
}

/// Remove the scratch directories in `parent` whose run is gone: a crashed
/// (SIGKILL, OOM) run of an encrypted sink leaves plaintext there. A live
/// run holds its directory's owner lock; one whose lock can be taken is
/// dead. Best effort — a directory that can not be inspected is left.
fn sweep_orphaned_scratch(parent: &Path) {
    let Ok(entries) = std::fs::read_dir(parent) else {
        return;
    };
    for entry in entries.flatten() {
        if !entry
            .file_name()
            .to_string_lossy()
            .starts_with(SCRATCH_PREFIX)
            || !entry.file_type().is_ok_and(|t| t.is_dir())
        {
            continue;
        }
        let dir = entry.path();
        if scratch_is_orphaned(&dir) && std::fs::remove_dir_all(&dir).is_ok() {
            tracing::info!(
                path = %dir.display(),
                "remote file sink: removed the scratch directory of a run that is gone"
            );
        }
    }
}

fn scratch_is_orphaned(dir: &Path) -> bool {
    match std::fs::File::open(dir.join(OWNER_FILE)) {
        Ok(owner) => owner.try_lock().is_ok(),
        Err(_) => std::fs::metadata(dir)
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.elapsed().ok())
            .is_some_and(|age| age > UNOWNED_MAX_AGE),
    }
}

impl std::fmt::Debug for RemoteBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteBackend")
            .field("base", &self.base)
            .field("swap", &self.swap)
            .finish_non_exhaustive()
    }
}

impl RemoteBackend {
    /// A backend writing `base` + file name, keeping overwrite runs under
    /// `base` + `template.swap_dir_name()` + `/`. Scratch files go in a
    /// private directory created in `scratch_dir` (default: the system
    /// temporary directory).
    pub fn new(
        client: Arc<dyn ObjectClient>,
        base: impl Into<String>,
        template: &NameTemplate,
        scratch_dir: Option<&Path>,
    ) -> Result<Self, FaucetError> {
        let base = base.into();
        let parent = scratch_dir.map_or_else(std::env::temp_dir, Path::to_path_buf);
        sweep_orphaned_scratch(&parent);
        let mut builder = tempfile::Builder::new();
        builder.prefix(SCRATCH_PREFIX);
        let scratch = match scratch_dir {
            Some(dir) => builder.tempdir_in(dir),
            None => builder.tempdir(),
        }
        .map_err(sink_io(format!(
            "remote file sink: creating a scratch directory in '{}'",
            scratch_dir.map_or_else(
                || std::env::temp_dir().display().to_string(),
                |d| d.display().to_string()
            )
        )))?;
        let owner = lock_owner(scratch.path()).map_err(sink_io(format!(
            "remote file sink: locking the scratch directory '{}'",
            scratch.path().display()
        )))?;
        Ok(Self {
            _owner: owner,
            swap: format!("{base}{}/", template.swap_dir_name()),
            template: template.clone(),
            base,
            client,
            multipart: None,
            scratch,
            uploads: 1,
            slots: Arc::new(tokio::sync::Semaphore::new(1)),
            in_flight: std::sync::Mutex::new(Vec::new()),
            seq: std::sync::atomic::AtomicU64::new(0),
            scrubbed: tokio::sync::Mutex::new([false; 2]),
        })
    }

    /// Remove the upload scratch a crashed run of this output left in `area`,
    /// once per area, before the first upload into it — not when the area is
    /// prepared, so a run needs the store only when it publishes.
    async fn scrub_upload_scratch(&self, area: Area) -> Result<(), FaucetError> {
        if !self.client.leaves_upload_scratch() {
            return Ok(());
        }
        let mut done = self.scrubbed.lock().await;
        let slot = &mut done[usize::from(area == Area::Swap)];
        if *slot {
            return Ok(());
        }
        let prefix = self.prefix(area).to_string();
        for name in direct_children(&prefix, self.client.list(&prefix).await?) {
            if self.template.owns_scratch(&name) {
                self.client.delete(&format!("{prefix}{name}")).await?;
            }
        }
        *slot = true;
        Ok(())
    }

    /// Keep up to `n` uploads in flight (at least 1). With 1 (the default)
    /// every publish uploads before it returns.
    pub fn with_upload_concurrency(mut self, n: usize) -> Self {
        self.uploads = n.max(1);
        self.slots = Arc::new(tokio::sync::Semaphore::new(self.uploads));
        self
    }

    /// Upload line-format objects in parts as they are written.
    pub fn with_multipart(mut self, client: Arc<dyn MultipartClient>) -> Self {
        self.multipart = Some(client);
        self
    }

    /// The most uploads kept in flight.
    pub fn upload_concurrency(&self) -> usize {
        self.uploads
    }

    /// The private directory scratch files are built in.
    pub fn scratch_dir(&self) -> &Path {
        self.scratch.path()
    }

    fn pending(&self) -> std::sync::MutexGuard<'_, Vec<InFlight>> {
        self.in_flight.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Collect finished uploads, returning the first error among them, and
    /// wait for an in-flight upload of `key` so two versions never race. On
    /// an error every other upload is awaited too, so none is still running
    /// when the failure reaches the caller.
    async fn reap(&self, key: &str) -> Result<(), FaucetError> {
        let done: Vec<InFlight> = {
            let mut pending = self.pending();
            let (done, running): (Vec<_>, Vec<_>) = pending
                .drain(..)
                .partition(|u| u.handle.is_finished() || u.key == key);
            *pending = running;
            done
        };
        let (result, _) = join_all(done).await;
        if result.is_err() {
            let _ = self.settle().await;
        }
        result
    }

    async fn permit(&self) -> Result<tokio::sync::OwnedSemaphorePermit, FaucetError> {
        self.slots.clone().acquire_owned().await.map_err(|e| {
            FaucetError::Sink(format!("remote file sink: waiting for an upload slot: {e}"))
        })
    }

    /// Move `scratch` aside and upload it in the background. On an error the
    /// caller still owns `scratch`.
    async fn spawn_upload(&self, scratch: &Path, key: String) -> Result<(), FaucetError> {
        let permit = self.permit().await?;
        let n = self.seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let moved = self.scratch.path().join(format!("inflight-{n}"));
        std::fs::rename(scratch, &moved).map_err(sink_io(format!(
            "remote file sink: preparing '{}' for upload",
            self.client.describe(&key)
        )))?;
        let client = self.client.clone();
        let k = key.clone();
        let task = tokio::spawn(async move {
            let r = client.upload(&moved, &k).await;
            let _ = tokio::fs::remove_file(&moved).await;
            drop(permit);
            r
        });
        self.pending().push(InFlight { key, handle: task });
        Ok(())
    }

    /// The key prefix files land under.
    pub fn base(&self) -> &str {
        &self.base
    }

    /// The key prefix an overwrite run keeps its files under.
    pub fn swap_prefix(&self) -> &str {
        &self.swap
    }

    fn prefix(&self, area: Area) -> &str {
        match area {
            Area::Destination => &self.base,
            Area::Swap => &self.swap,
        }
    }

    /// The full key of `name` in `area`.
    pub fn key(&self, area: Area, name: &str) -> String {
        format!("{}{name}", self.prefix(area))
    }
}

#[faucet_core::async_trait]
impl StorageBackend for RemoteBackend {
    fn describe(&self, area: Area, name: &str) -> String {
        self.client.describe(&self.key(area, name))
    }

    fn scratch_path(&self, area: Area, name: &str) -> PathBuf {
        let tag = match area {
            Area::Destination => "d",
            Area::Swap => "s",
        };
        let safe: String = name
            .chars()
            .map(|c| if c == '/' || c == '\\' { '_' } else { c })
            .collect();
        self.scratch.path().join(format!("{tag}-{safe}"))
    }

    async fn prepare(&self, _area: Area) -> Result<(), FaucetError> {
        Ok(())
    }

    async fn list(&self, area: Area) -> Result<Vec<String>, FaucetError> {
        let prefix = self.prefix(area).to_string();
        let keys = self.client.list(&prefix).await?;
        Ok(direct_children(&prefix, keys))
    }

    async fn exists(&self, area: Area, name: &str) -> Result<bool, FaucetError> {
        self.client.exists(&self.key(area, name)).await
    }

    async fn fetch(&self, area: Area, name: &str, to: &Path) -> Result<(), FaucetError> {
        self.client.download(&self.key(area, name), to).await
    }

    async fn publish(&self, scratch: &Path, area: Area, name: &str) -> Result<(), FaucetError> {
        if let Err(e) = self.scrub_upload_scratch(area).await {
            let _ = std::fs::remove_file(scratch);
            return Err(e);
        }
        let key = self.key(area, name);
        if self.uploads <= 1 {
            let result = self.client.upload(scratch, &key).await;
            let _ = std::fs::remove_file(scratch);
            return result;
        }
        let result = match self.reap(&key).await {
            Ok(()) => self.spawn_upload(scratch, key).await,
            Err(e) => Err(e),
        };
        if result.is_err() {
            let _ = std::fs::remove_file(scratch);
        }
        result
    }

    async fn settle(&self) -> Result<(), FaucetError> {
        let all = std::mem::take(&mut *self.pending());
        join_all(all).await.0
    }

    async fn cancel(&self) {
        let all = std::mem::take(&mut *self.pending());
        let (_, landed) = join_all(all).await;
        if !landed.is_empty() {
            let _ = self.client.delete_many(&landed).await;
        }
    }

    async fn delete(&self, area: Area, name: &str) -> Result<(), FaucetError> {
        self.client.delete(&self.key(area, name)).await
    }

    async fn delete_many(&self, area: Area, names: &[String]) -> Result<(), FaucetError> {
        let keys: Vec<String> = names.iter().map(|n| self.key(area, n)).collect();
        self.client.delete_many(&keys).await
    }

    async fn promote(&self, name: &str) -> Result<(), FaucetError> {
        self.client
            .rename(
                &self.key(Area::Swap, name),
                &self.key(Area::Destination, name),
            )
            .await
    }

    fn part_size(&self) -> Option<usize> {
        self.multipart.as_ref().map(|m| m.part_size())
    }

    async fn open_stream(
        &self,
        area: Area,
        name: &str,
    ) -> Result<Box<dyn PartStream>, FaucetError> {
        let key = self.key(area, name);
        let multipart = self.multipart.as_ref().ok_or_else(|| {
            FaucetError::Sink(format!(
                "remote file sink: '{}' cannot be uploaded in parts",
                self.client.describe(&key)
            ))
        })?;
        let upload = multipart.start(&key).await?;
        Ok(Box::new(RemoteStream {
            upload: Some(upload),
            slots: self.slots.clone(),
            next: 1,
            parts: Vec::new(),
        }))
    }
}

/// The parts of one object going up through a [`MultipartUpload`].
struct RemoteStream {
    upload: Option<Box<dyn MultipartUpload>>,
    slots: Arc<tokio::sync::Semaphore>,
    next: u32,
    parts: Vec<Upload>,
}

impl RemoteStream {
    /// Await every part started so far; the first error, after all ended.
    async fn drain(&mut self) -> Result<(), FaucetError> {
        let parts = std::mem::take(&mut self.parts);
        let mut first = Ok(());
        for p in parts {
            let r = p.await.unwrap_or_else(|e| {
                Err(FaucetError::Sink(format!(
                    "remote file sink: a part upload did not finish: {e}"
                )))
            });
            if first.is_ok() {
                first = r;
            }
        }
        first
    }

    /// The first error among the parts that already ended.
    async fn reap(&mut self) -> Result<(), FaucetError> {
        let (done, running): (Vec<_>, Vec<_>) = std::mem::take(&mut self.parts)
            .into_iter()
            .partition(|p| p.is_finished());
        self.parts = running;
        for p in done {
            p.await.unwrap_or_else(|e| {
                Err(FaucetError::Sink(format!(
                    "remote file sink: a part upload did not finish: {e}"
                )))
            })?;
        }
        Ok(())
    }
}

#[faucet_core::async_trait]
impl PartStream for RemoteStream {
    async fn put(&mut self, part: Vec<u8>) -> Result<(), FaucetError> {
        self.reap().await?;
        let permit = self.slots.clone().acquire_owned().await.map_err(|e| {
            FaucetError::Sink(format!("remote file sink: waiting for an upload slot: {e}"))
        })?;
        let upload = self
            .upload
            .as_ref()
            .expect("an upload is open until it finishes or aborts");
        let fut = upload.put_part(self.next, part);
        self.next += 1;
        self.parts.push(tokio::spawn(async move {
            let r = fut.await;
            drop(permit);
            r
        }));
        Ok(())
    }

    async fn finish(mut self: Box<Self>) -> Result<(), FaucetError> {
        let upload = self
            .upload
            .take()
            .expect("an upload is open until it finishes or aborts");
        match self.drain().await {
            Ok(()) => upload.complete().await,
            Err(e) => {
                let _ = upload.abort().await;
                Err(e)
            }
        }
    }

    async fn abort(mut self: Box<Self>) {
        let _ = self.drain().await;
        if let Some(upload) = self.upload.take() {
            let _ = upload.abort().await;
        }
    }
}

/// The content type an object named `name` is served with, from its
/// extension (looking through `.gz` / `.zst`).
pub fn content_type(name: &str) -> &'static str {
    let name = name.trim_end_matches(".gz").trim_end_matches(".zst");
    match name.rsplit('.').next().unwrap_or("") {
        "parquet" => "application/vnd.apache.parquet",
        "csv" => "text/csv",
        "json" => "application/json",
        "xml" => "application/xml",
        "xlsx" => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "avro" => "application/avro",
        "txt" => "text/plain",
        _ => "application/x-ndjson",
    }
}

/// The names directly under `prefix` among `keys` (no deeper `/`).
fn direct_children(prefix: &str, keys: Vec<String>) -> Vec<String> {
    let mut out: Vec<String> = keys
        .into_iter()
        .filter_map(|k| k.strip_prefix(prefix).map(str::to_string))
        .filter(|n| !n.is_empty() && !n.contains('/'))
        .collect();
    out.sort();
    out.dedup();
    out
}

/// Where an object-store sink's files go: the key prefix (`base`) and the
/// file-name template.
///
/// With `path` set, the key template is `prefix + path`, split like the file
/// sink's `path` (a trailing `/` is a directory of `part-{part}` files).
/// Without it every run gets a fresh time-ordered id, numbered per object,
/// with `file_extension` — `<prefix><id>-00001.jsonl`,
/// `<prefix><id>-00002.jsonl`, … — so objects are never overwritten.
pub fn object_layout(
    prefix: &str,
    path: Option<&str>,
    file_extension: &str,
    format: FileFormat,
    codec: Compression,
    rolls_over: bool,
) -> Result<(String, NameTemplate), FaucetError> {
    match path {
        Some(p) => {
            let full = format!("{prefix}{p}");
            let (dir, template) = NameTemplate::from_path(&full, format, codec, rolls_over)?;
            let dir = dir.trim_matches('/');
            let base = if dir.is_empty() {
                String::new()
            } else {
                format!("{dir}/")
            };
            Ok((base, template))
        }
        None => {
            let name = format!(
                "{}-{}{file_extension}",
                uuid::Uuid::now_v7(),
                super::options::PART_TOKEN
            );
            Ok((prefix.to_string(), NameTemplate { name }))
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};

    fn io(e: std::io::Error) -> FaucetError {
        FaucetError::Sink(e.to_string())
    }

    /// An in-memory store; optionally slow, failing or taking parts.
    #[derive(Default)]
    pub(crate) struct Mem {
        pub objects: Mutex<BTreeMap<String, Vec<u8>>>,
        pub fail_upload: AtomicBool,
        pub fail_key: Mutex<Option<String>>,
        pub fail_rename_key: Mutex<Option<String>>,
        pub panic_key: Mutex<Option<String>>,
        pub delay_ms: std::sync::atomic::AtomicU64,
        pub now: AtomicUsize,
        pub peak: AtomicUsize,
        pub parts_put: AtomicUsize,
        pub renames: AtomicUsize,
        pub lists: AtomicUsize,
        pub upload_scratch: AtomicBool,
    }

    impl Mem {
        pub fn keys(&self) -> Vec<String> {
            self.objects.lock().unwrap().keys().cloned().collect()
        }
        pub fn text(&self, key: &str) -> String {
            String::from_utf8(self.objects.lock().unwrap()[key].clone()).unwrap()
        }
        async fn slow(&self, key: &str) -> Result<(), FaucetError> {
            if self.panic_key.lock().unwrap().as_deref() == Some(key) {
                panic!("upload of {key} panicked");
            }
            let n = self.now.fetch_add(1, SeqCst) + 1;
            self.peak.fetch_max(n, SeqCst);
            let ms = self.delay_ms.load(SeqCst);
            if ms > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
            }
            self.now.fetch_sub(1, SeqCst);
            if self.fail_upload.load(SeqCst) {
                return Err(FaucetError::Sink("upload refused".into()));
            }
            if self.fail_key.lock().unwrap().as_deref() == Some(key) {
                return Err(FaucetError::Sink(format!("refused {key}")));
            }
            Ok(())
        }
    }

    #[faucet_core::async_trait]
    impl ObjectClient for Mem {
        fn describe(&self, key: &str) -> String {
            format!("mem://{key}")
        }
        fn leaves_upload_scratch(&self) -> bool {
            self.upload_scratch.load(SeqCst)
        }
        async fn list(&self, prefix: &str) -> Result<Vec<String>, FaucetError> {
            self.lists.fetch_add(1, SeqCst);
            let o = self.objects.lock().unwrap();
            Ok(o.keys()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect())
        }
        async fn exists(&self, key: &str) -> Result<bool, FaucetError> {
            Ok(self.objects.lock().unwrap().contains_key(key))
        }
        async fn download(&self, key: &str, to: &Path) -> Result<(), FaucetError> {
            let body = self.objects.lock().unwrap().get(key).cloned();
            let body = body.ok_or_else(|| FaucetError::Sink(format!("missing {key}")))?;
            std::fs::write(to, body).map_err(io)
        }
        async fn upload(&self, from: &Path, key: &str) -> Result<(), FaucetError> {
            self.slow(key).await?;
            let body = std::fs::read(from).map_err(io)?;
            self.objects.lock().unwrap().insert(key.to_string(), body);
            Ok(())
        }
        async fn delete(&self, key: &str) -> Result<(), FaucetError> {
            self.objects.lock().unwrap().remove(key);
            Ok(())
        }
        async fn rename(&self, from: &str, to: &str) -> Result<(), FaucetError> {
            self.renames.fetch_add(1, SeqCst);
            if self.fail_rename_key.lock().unwrap().as_deref() == Some(from) {
                return Err(FaucetError::Sink(format!("rename of {from} refused")));
            }
            let body = self.objects.lock().unwrap().remove(from);
            let body = body.ok_or_else(|| FaucetError::Sink(format!("missing {from}")))?;
            self.objects.lock().unwrap().insert(to.to_string(), body);
            Ok(())
        }
    }

    /// A [`MultipartClient`] over [`Mem`] with a tiny part size.
    pub(crate) struct MemParts {
        pub mem: Arc<Mem>,
        pub size: usize,
    }

    struct MemUpload {
        mem: Arc<Mem>,
        key: String,
        parts: Arc<Mutex<BTreeMap<u32, Vec<u8>>>>,
    }

    #[faucet_core::async_trait]
    impl MultipartClient for MemParts {
        fn part_size(&self) -> usize {
            self.size
        }
        async fn start(&self, key: &str) -> Result<Box<dyn MultipartUpload>, FaucetError> {
            Ok(Box::new(MemUpload {
                mem: self.mem.clone(),
                key: key.to_string(),
                parts: Default::default(),
            }))
        }
    }

    impl MultipartUpload for MemUpload {
        fn put_part(&self, number: u32, body: Vec<u8>) -> BoxFuture<Result<(), FaucetError>> {
            let (mem, parts, key) = (self.mem.clone(), self.parts.clone(), self.key.clone());
            Box::pin(async move {
                mem.slow(&format!("{key}#{number}")).await?;
                mem.parts_put.fetch_add(1, SeqCst);
                parts.lock().unwrap().insert(number, body);
                Ok(())
            })
        }
        fn complete(self: Box<Self>) -> BoxFuture<Result<(), FaucetError>> {
            Box::pin(async move {
                let body: Vec<u8> = self
                    .parts
                    .lock()
                    .unwrap()
                    .values()
                    .flatten()
                    .copied()
                    .collect();
                self.mem.objects.lock().unwrap().insert(self.key, body);
                Ok(())
            })
        }
        fn abort(self: Box<Self>) -> BoxFuture<Result<(), FaucetError>> {
            Box::pin(async { Ok(()) })
        }
    }

    fn template(name: &str) -> NameTemplate {
        NameTemplate { name: name.into() }
    }

    #[test]
    fn a_crashed_runs_scratch_is_swept_and_a_live_ones_kept() {
        let parent = tempfile::tempdir().unwrap();
        let mem = Arc::new(Mem::default());
        let live =
            RemoteBackend::new(mem.clone(), "a/", &template("x"), Some(parent.path())).unwrap();
        let crashed = parent.path().join("faucet-remote-crashed");
        std::fs::create_dir(&crashed).unwrap();
        std::fs::write(crashed.join(OWNER_FILE), b"").unwrap();
        std::fs::write(crashed.join("plain.jsonl"), b"{\"ssn\":1}").unwrap();
        let young = parent.path().join("faucet-remote-young");
        std::fs::create_dir(&young).unwrap();
        let other = parent.path().join("not-ours");
        std::fs::create_dir(&other).unwrap();
        std::fs::write(crashed.with_extension("file"), b"").unwrap();

        let next = RemoteBackend::new(mem, "b/", &template("x"), Some(parent.path())).unwrap();
        assert!(
            !crashed.exists(),
            "a dead run's plaintext scratch is removed"
        );
        assert!(live.scratch_dir().exists(), "a live run's scratch is kept");
        assert!(young.exists() && other.exists());
        assert!(!scratch_is_orphaned(next.scratch_dir()));
        assert!(!scratch_is_orphaned(&young));
        drop(live);
    }

    #[tokio::test]
    async fn publish_list_fetch_promote_and_delete() {
        let mem = Arc::new(Mem::default());
        let b = RemoteBackend::new(mem.clone(), "b/", &template("x"), None).unwrap();
        assert_eq!(b.base(), "b/");
        assert_eq!(b.swap_prefix(), "b/.faucet-overwrite-x/");
        assert!(format!("{b:?}").contains(".faucet-overwrite-x"));
        assert!(b.scratch_path(Area::Swap, "x/y").ends_with("s-x_y"));
        b.prepare(Area::Destination).await.unwrap();
        let s = b.scratch_path(Area::Swap, "a");
        std::fs::write(&s, b"body").unwrap();
        b.publish(&s, Area::Swap, "a").await.unwrap();
        assert!(!s.exists(), "the scratch file is consumed");
        assert_eq!(b.list(Area::Swap).await.unwrap(), ["a"]);
        assert!(b.exists(Area::Swap, "a").await.unwrap());
        let to = b.scratch_path(Area::Destination, "copy");
        b.fetch(Area::Swap, "a", &to).await.unwrap();
        assert_eq!(std::fs::read(&to).unwrap(), b"body");
        b.promote("a").await.unwrap();
        assert_eq!(mem.keys(), ["b/a"]);
        b.delete(Area::Destination, "a").await.unwrap();
        assert!(mem.keys().is_empty());
        let e = b.fetch(Area::Destination, "b", &to).await.unwrap_err();
        assert!(e.to_string().contains("missing b/b"), "{e}");
        assert_eq!(b.describe(Area::Destination, "z"), "mem://b/z");
        assert_eq!(b.part_size(), None);
        assert!(b.open_stream(Area::Destination, "z").await.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn background_uploads_overlap_up_to_the_limit_and_settle_reports_errors() {
        let mem = Arc::new(Mem::default());
        mem.delay_ms.store(100, SeqCst);
        let b = RemoteBackend::new(mem.clone(), "", &template("x"), None)
            .unwrap()
            .with_upload_concurrency(3);
        assert_eq!(b.upload_concurrency(), 3);
        *mem.fail_key.lock().unwrap() = Some("o4".into());
        let started = std::time::Instant::now();
        for i in 0..6 {
            let s = b.scratch_path(Area::Destination, &format!("o{i}"));
            std::fs::write(&s, b"1").unwrap();
            b.publish(&s, Area::Destination, &format!("o{i}"))
                .await
                .unwrap();
        }
        let e = b.settle().await.unwrap_err();
        assert!(e.to_string().contains("refused o4"), "{e}");
        assert_eq!(mem.peak.load(SeqCst), 3);
        assert_eq!(mem.now.load(SeqCst), 0, "nothing is left running");
        assert!(started.elapsed() < std::time::Duration::from_millis(500));
        assert_eq!(mem.keys().len(), 5);
        b.settle().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cancel_waits_and_removes_what_landed() {
        let mem = Arc::new(Mem::default());
        mem.delay_ms.store(50, SeqCst);
        let b = RemoteBackend::new(mem.clone(), "", &template("x"), None)
            .unwrap()
            .with_upload_concurrency(4);
        for i in 0..3 {
            let s = b.scratch_path(Area::Swap, &format!("o{i}"));
            std::fs::write(&s, b"1").unwrap();
            b.publish(&s, Area::Swap, &format!("o{i}")).await.unwrap();
        }
        b.cancel().await;
        assert_eq!(mem.now.load(SeqCst), 0);
        assert!(mem.keys().is_empty(), "{:?}", mem.keys());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_panicking_upload_is_an_error() {
        let mem = Arc::new(Mem::default());
        *mem.panic_key.lock().unwrap() = Some("p".into());
        let b = RemoteBackend::new(mem.clone(), "", &template("x"), None)
            .unwrap()
            .with_upload_concurrency(2);
        let s = b.scratch_path(Area::Destination, "p");
        std::fs::write(&s, b"1").unwrap();
        b.publish(&s, Area::Destination, "p").await.unwrap();
        let e = b.settle().await.unwrap_err();
        assert!(
            e.to_string().contains("the upload of 'p' did not finish"),
            "{e}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_publish_that_cannot_start_removes_its_scratch_file() {
        let mem = Arc::new(Mem::default());
        let b = RemoteBackend::new(mem.clone(), "p/", &template("x"), None)
            .unwrap()
            .with_upload_concurrency(2);
        let missing = b.scratch_path(Area::Destination, "gone");
        let e = b
            .publish(&missing, Area::Destination, "gone")
            .await
            .unwrap_err();
        assert!(
            e.to_string()
                .contains("preparing 'mem://p/gone' for upload"),
            "{e}"
        );
        let scratch = b.scratch_path(Area::Destination, "x");
        std::fs::write(&scratch, b"1").unwrap();
        b.slots.close();
        let e = b
            .publish(&scratch, Area::Destination, "x")
            .await
            .unwrap_err();
        assert!(e.to_string().contains("waiting for an upload slot"), "{e}");
        assert!(!scratch.exists());
        let direct = RemoteBackend::new(mem.clone(), "p/", &template("x"), None).unwrap();
        let missing = direct.scratch_path(Area::Destination, "gone");
        assert!(
            direct
                .publish(&missing, Area::Destination, "gone")
                .await
                .is_err()
        );
        assert!(mem.keys().is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_stream_puts_parts_concurrently_and_completes_in_order() {
        let mem = Arc::new(Mem::default());
        mem.delay_ms.store(50, SeqCst);
        let b = RemoteBackend::new(mem.clone(), "", &template("x"), None)
            .unwrap()
            .with_upload_concurrency(3)
            .with_multipart(Arc::new(MemParts {
                mem: mem.clone(),
                size: 2,
            }));
        assert_eq!(b.part_size(), Some(2));
        let mut s = b.open_stream(Area::Destination, "o").await.unwrap();
        for p in ["ab", "cd", "ef", "gh", "i"] {
            s.put(p.as_bytes().to_vec()).await.unwrap();
        }
        s.finish().await.unwrap();
        assert_eq!(mem.text("o"), "abcdefghi");
        assert_eq!(mem.peak.load(SeqCst), 3);

        *mem.fail_key.lock().unwrap() = Some("q#2".into());
        let mut s = b.open_stream(Area::Destination, "q").await.unwrap();
        for p in ["ab", "cd", "ef"] {
            let _ = s.put(p.as_bytes().to_vec()).await;
        }
        let e = s.finish().await.unwrap_err();
        assert!(e.to_string().contains("refused q#2"), "{e}");
        assert!(!mem.keys().contains(&"q".to_string()));
        let mut s = b.open_stream(Area::Destination, "r").await.unwrap();
        s.put(b"ab".to_vec()).await.unwrap();
        s.abort().await;
        assert!(!mem.keys().contains(&"r".to_string()));
    }

    #[tokio::test]
    async fn the_default_rename_copies_through_a_local_file() {
        struct Plain(Mem);
        #[faucet_core::async_trait]
        impl ObjectClient for Plain {
            fn describe(&self, key: &str) -> String {
                self.0.describe(key)
            }
            async fn list(&self, prefix: &str) -> Result<Vec<String>, FaucetError> {
                self.0.list(prefix).await
            }
            async fn exists(&self, key: &str) -> Result<bool, FaucetError> {
                self.0.exists(key).await
            }
            async fn download(&self, key: &str, to: &Path) -> Result<(), FaucetError> {
                self.0.download(key, to).await
            }
            async fn upload(&self, from: &Path, key: &str) -> Result<(), FaucetError> {
                self.0.upload(from, key).await
            }
            async fn delete(&self, key: &str) -> Result<(), FaucetError> {
                self.0.delete(key).await
            }
        }
        let p = Plain(Mem::default());
        p.0.objects
            .lock()
            .unwrap()
            .insert("a".into(), b"x".to_vec());
        p.rename("a", "b").await.unwrap();
        assert_eq!(p.0.text("b"), "x");
        assert!(!p.exists("a").await.unwrap());
    }

    #[test]
    fn content_types_children_and_layouts() {
        assert_eq!(
            content_type("a/b.parquet"),
            "application/vnd.apache.parquet"
        );
        assert_eq!(content_type("x.csv.gz"), "text/csv");
        assert_eq!(content_type("x.json"), "application/json");
        assert_eq!(content_type("x.xml"), "application/xml");
        assert!(content_type("x.xlsx").contains("spreadsheet"));
        assert_eq!(content_type("x.avro"), "application/avro");
        assert_eq!(content_type("x.txt"), "text/plain");
        assert_eq!(content_type("x.jsonl.zst"), "application/x-ndjson");
        let keys = vec![
            "p/a".into(),
            "p/b/c".into(),
            "q/x".into(),
            "p/".into(),
            "p/a".into(),
        ];
        assert_eq!(direct_children("p/", keys), ["a"]);
        let (base, t) =
            object_layout("", Some("/"), "", FileFormat::Csv, Compression::Gzip, false).unwrap();
        assert_eq!(base, "");
        assert_eq!(t.name, "part-{part}.csv.gz");
        let (base, t) = object_layout(
            "pre/",
            None,
            ".jsonl",
            FileFormat::JsonLines,
            Compression::None,
            false,
        )
        .unwrap();
        assert_eq!(base, "pre/");
        assert!(t.numbered() && t.name.ends_with("-{part}.jsonl"), "{t:?}");
    }

    #[test]
    fn a_scratch_directory_can_be_chosen() {
        let dir = tempfile::tempdir().unwrap();
        let b = RemoteBackend::new(
            Arc::new(Mem::default()),
            "",
            &template("x"),
            Some(dir.path()),
        )
        .unwrap();
        assert!(b.scratch_dir().starts_with(dir.path()));
        let e = RemoteBackend::new(
            Arc::new(Mem::default()),
            "",
            &template("x"),
            Some(&dir.path().join("missing")),
        )
        .unwrap_err();
        assert!(
            e.to_string().contains("creating a scratch directory in"),
            "{e}"
        );
    }
}
