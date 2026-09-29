//! A [`StorageBackend`] over a remote object store or file server (#777).
//!
//! Each connector implements the small async [`ObjectClient`] with its own
//! client (S3, GCS, Azure Blob, SFTP); [`RemoteBackend`] turns it into a
//! backend: files are built in a local temporary directory, and a commit is
//! one upload that the store publishes atomically (a single `PUT`, a
//! completed multipart / resumable upload, or a temp-name-then-rename).
//!
//! Keys are the backend's `base` (a key prefix, concatenated as is) followed
//! by the file name. An overwrite run stages under
//! `<base><staging name>/`, marked by an empty `.faucet-staging` object so
//! that [`staging_ready`](StorageBackend::staging_ready) is answered from the
//! store rather than from memory.
//!
//! Backend calls are blocking; [`run`] drives the client's futures on the
//! caller's multi-threaded Tokio runtime from inside `block_in_place`, or on
//! a process-wide I/O runtime when the caller has none (or a current-thread
//! one).

use super::backend::{Area, StorageBackend};
use super::layout::NameTemplate;
use faucet_core::{Compression, FaucetError, FileFormat};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// The marker object that says an overwrite run's staging area exists.
pub const STAGING_MARKER: &str = ".faucet-staging";

/// The operations a remote store must offer. Keys are full object keys (or
/// remote paths); every method is called through [`run`].
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
    /// Move `from` to `to`, replacing `to`. Default: a server-side copy is
    /// not assumed, so the object is downloaded and re-uploaded, then
    /// `from` deleted; stores with a copy or rename override it.
    async fn rename(&self, from: &str, to: &str) -> Result<(), FaucetError> {
        let dir = tempfile::tempdir().map_err(|e| {
            FaucetError::Sink(format!(
                "remote rename: creating a temporary directory: {e}"
            ))
        })?;
        let tmp = dir.path().join("object");
        self.download(from, &tmp).await?;
        self.upload(&tmp, to).await?;
        self.delete(from).await
    }
}

/// Drive `fut` to completion from a blocking context.
///
/// On a multi-threaded Tokio runtime the future runs on that runtime inside
/// `block_in_place`. Anywhere else — a current-thread runtime, or no runtime
/// — it runs on a small process-wide runtime from a scoped helper thread, so
/// the caller's single thread is never asked to drive I/O it is blocked on.
pub fn run<T: Send>(
    fut: impl Future<Output = Result<T, FaucetError>> + Send,
) -> Result<T, FaucetError> {
    if let Ok(handle) = tokio::runtime::Handle::try_current()
        && handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread
    {
        return tokio::task::block_in_place(|| handle.block_on(fut));
    }
    let rt = fallback_runtime()?;
    std::thread::scope(|s| {
        s.spawn(|| rt.block_on(fut)).join().unwrap_or_else(|_| {
            Err(FaucetError::Sink(
                "remote file sink: an I/O task panicked".into(),
            ))
        })
    })
}

fn fallback_runtime() -> Result<&'static tokio::runtime::Runtime, FaucetError> {
    static RT: std::sync::OnceLock<Result<tokio::runtime::Runtime, String>> =
        std::sync::OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("faucet-remote-io")
            .enable_all()
            .build()
            .map_err(|e| e.to_string())
    })
    .as_ref()
    .map_err(|e| FaucetError::Sink(format!("remote file sink: starting an I/O runtime: {e}")))
}

/// The runtime background uploads are spawned on: the caller's
/// multi-threaded runtime, else the process-wide I/O runtime.
fn io_handle() -> Result<tokio::runtime::Handle, FaucetError> {
    if let Ok(handle) = tokio::runtime::Handle::try_current()
        && handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread
    {
        return Ok(handle);
    }
    Ok(fallback_runtime()?.handle().clone())
}

type Upload = tokio::task::JoinHandle<Result<(), FaucetError>>;

/// An upload running in the background, and the key it publishes.
struct InFlight {
    key: String,
    handle: Upload,
}

/// Await `uploads` and return the first error; every one is awaited even
/// after a failure, so none is left running unobserved.
fn join_all(uploads: Vec<InFlight>) -> Result<(), FaucetError> {
    if uploads.is_empty() {
        return Ok(());
    }
    run(async move {
        let mut first = Ok(());
        for u in uploads {
            let r = match u.handle.await {
                Ok(r) => r,
                Err(e) => Err(FaucetError::Sink(format!(
                    "remote file sink: the upload of '{}' did not finish: {e}",
                    u.key
                ))),
            };
            if first.is_ok() {
                first = r;
            }
        }
        first
    })
}

/// A [`StorageBackend`] over an [`ObjectClient`].
///
/// With [`with_upload_concurrency`](Self::with_upload_concurrency) above 1, a
/// [`commit`](StorageBackend::commit) hands the finished file to a background
/// upload and returns, so the writer encodes the next file while earlier ones
/// are still going up; at most that many uploads are in flight, and a commit
/// past the limit waits for a slot. [`settle`](StorageBackend::settle) awaits
/// them all and reports the first error.
pub struct RemoteBackend {
    client: Arc<dyn ObjectClient>,
    base: String,
    staging: String,
    scratch: Arc<tempfile::TempDir>,
    uploads: usize,
    slots: Arc<tokio::sync::Semaphore>,
    in_flight: std::sync::Mutex<Vec<InFlight>>,
    seq: std::sync::atomic::AtomicU64,
}

impl std::fmt::Debug for RemoteBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteBackend")
            .field("base", &self.base)
            .field("staging", &self.staging)
            .finish_non_exhaustive()
    }
}

impl RemoteBackend {
    /// A backend writing `base` + file name, staging overwrite runs under
    /// `base` + `staging_name` + `/`.
    pub fn new(
        client: Arc<dyn ObjectClient>,
        base: impl Into<String>,
        staging_name: &str,
    ) -> Result<Self, FaucetError> {
        let base = base.into();
        let scratch = tempfile::Builder::new()
            .prefix("faucet-remote-")
            .tempdir()
            .map_err(|e| {
                FaucetError::Sink(format!(
                    "remote file sink: creating a scratch directory: {e}"
                ))
            })?;
        Ok(Self {
            staging: format!("{base}{staging_name}/"),
            base,
            client,
            scratch: Arc::new(scratch),
            uploads: 1,
            slots: Arc::new(tokio::sync::Semaphore::new(1)),
            in_flight: std::sync::Mutex::new(Vec::new()),
            seq: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// Keep up to `n` uploads in flight (at least 1). With 1 (the default)
    /// every commit uploads before it returns.
    pub fn with_upload_concurrency(mut self, n: usize) -> Self {
        self.uploads = n.max(1);
        self.slots = Arc::new(tokio::sync::Semaphore::new(self.uploads));
        self
    }

    /// The most uploads kept in flight.
    pub fn upload_concurrency(&self) -> usize {
        self.uploads
    }

    fn pending(&self) -> std::sync::MutexGuard<'_, Vec<InFlight>> {
        self.in_flight.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Collect finished uploads, returning the first error among them, and
    /// wait for an in-flight upload of `key` so two versions never race.
    fn reap(&self, key: &str) -> Result<(), FaucetError> {
        let done: Vec<InFlight> = {
            let mut pending = self.pending();
            let (done, running): (Vec<_>, Vec<_>) = pending
                .drain(..)
                .partition(|u| u.handle.is_finished() || u.key == key);
            *pending = running;
            done
        };
        join_all(done)
    }

    fn spawn_upload(&self, scratch: &Path, key: String) -> Result<(), FaucetError> {
        let n = self.seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let moved = self.scratch.path().join(format!("inflight-{n}"));
        if let Err(e) = std::fs::rename(scratch, &moved) {
            let _ = std::fs::remove_file(scratch);
            return Err(FaucetError::Sink(format!(
                "remote file sink: preparing '{}' for upload: {e}",
                self.client.describe(&key)
            )));
        }
        let slots = self.slots.clone();
        let permit = match run(async move {
            slots.acquire_owned().await.map_err(|e| {
                FaucetError::Sink(format!("remote file sink: waiting for an upload slot: {e}"))
            })
        }) {
            Ok(p) => p,
            Err(e) => {
                let _ = std::fs::remove_file(&moved);
                return Err(e);
            }
        };
        let handle = match io_handle() {
            Ok(h) => h,
            Err(e) => {
                let _ = std::fs::remove_file(&moved);
                return Err(e);
            }
        };
        let client = self.client.clone();
        let dir = self.scratch.clone();
        let k = key.clone();
        let task = handle.spawn(async move {
            let r = client.upload(&moved, &k).await;
            let _ = tokio::fs::remove_file(&moved).await;
            drop(permit);
            drop(dir);
            r
        });
        self.pending().push(InFlight { key, handle: task });
        Ok(())
    }

    /// The key prefix files land under.
    pub fn base(&self) -> &str {
        &self.base
    }

    /// The key prefix an overwrite run stages under.
    pub fn staging_prefix(&self) -> &str {
        &self.staging
    }

    fn prefix(&self, area: Area) -> &str {
        match area {
            Area::Destination => &self.base,
            Area::Staging => &self.staging,
        }
    }

    /// The full key of `name` in `area`.
    pub fn key(&self, area: Area, name: &str) -> String {
        format!("{}{name}", self.prefix(area))
    }
}

impl StorageBackend for RemoteBackend {
    fn describe(&self, area: Area, name: &str) -> String {
        self.client.describe(&self.key(area, name))
    }

    fn scratch_path(&self, area: Area, name: &str) -> Result<PathBuf, FaucetError> {
        let tag = match area {
            Area::Destination => "d",
            Area::Staging => "s",
        };
        let safe: String = name
            .chars()
            .map(|c| if c == '/' || c == '\\' { '_' } else { c })
            .collect();
        Ok(self.scratch.path().join(format!("{tag}-{safe}")))
    }

    fn sync_scratch(&self) -> bool {
        false
    }

    fn prepare(&self, _area: Area) -> Result<(), FaucetError> {
        Ok(())
    }

    fn list(&self, area: Area) -> Result<Vec<String>, FaucetError> {
        let prefix = self.prefix(area).to_string();
        let keys = run(self.client.list(&prefix))?;
        Ok(direct_children(&prefix, keys))
    }

    fn exists(&self, area: Area, name: &str) -> Result<bool, FaucetError> {
        run(self.client.exists(&self.key(area, name)))
    }

    fn fetch(&self, area: Area, name: &str, to: &Path) -> Result<(), FaucetError> {
        run(self.client.download(&self.key(area, name), to))
    }

    fn commit(&self, scratch: &Path, area: Area, name: &str) -> Result<(), FaucetError> {
        let key = self.key(area, name);
        if self.uploads <= 1 {
            let result = run(self.client.upload(scratch, &key));
            let _ = std::fs::remove_file(scratch);
            return result;
        }
        if let Err(e) = self.reap(&key) {
            let _ = std::fs::remove_file(scratch);
            return Err(e);
        }
        self.spawn_upload(scratch, key)
    }

    fn settle(&self) -> Result<(), FaucetError> {
        let all = std::mem::take(&mut *self.pending());
        join_all(all)
    }

    fn cancel(&self) {
        let _ = self.settle();
    }

    fn delete(&self, area: Area, name: &str) -> Result<(), FaucetError> {
        run(self.client.delete(&self.key(area, name)))
    }

    fn begin_staging(&self) -> Result<(), FaucetError> {
        self.clear_staging()?;
        let marker = self.scratch.path().join("marker");
        std::fs::write(&marker, b"").map_err(|e| {
            FaucetError::Sink(format!("remote file sink: writing the staging marker: {e}"))
        })?;
        let result = run(self
            .client
            .upload(&marker, &self.key(Area::Staging, STAGING_MARKER)));
        let _ = std::fs::remove_file(&marker);
        result
    }

    fn staging_ready(&self) -> Result<bool, FaucetError> {
        run(self.client.exists(&self.key(Area::Staging, STAGING_MARKER)))
    }

    fn promote(&self, name: &str) -> Result<(), FaucetError> {
        run(self.client.rename(
            &self.key(Area::Staging, name),
            &self.key(Area::Destination, name),
        ))
    }

    fn clear_staging(&self) -> Result<(), FaucetError> {
        let staging = self.staging.clone();
        run(async {
            let keys = self.client.list(&staging).await?;
            for key in keys
                .iter()
                .filter(|k| k.as_str() != self.key(Area::Staging, STAGING_MARKER))
            {
                self.client.delete(key).await?;
            }
            self.client
                .delete(&self.key(Area::Staging, STAGING_MARKER))
                .await
        })
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
/// Without it the sink keeps its original naming: a fresh time-ordered id per
/// run, numbered per object, with `file_extension` —
/// `<prefix><id>-00001.jsonl`, `<prefix><id>-00002.jsonl`, … — so objects are
/// never overwritten.
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
mod tests {
    use super::*;
    use crate::write::{FileWriter, WriteSettings};
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Mem {
        objects: Mutex<BTreeMap<String, Vec<u8>>>,
        fail_upload: std::sync::atomic::AtomicBool,
    }

    #[faucet_core::async_trait]
    impl ObjectClient for Mem {
        fn describe(&self, key: &str) -> String {
            format!("mem://{key}")
        }
        async fn list(&self, prefix: &str) -> Result<Vec<String>, FaucetError> {
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
            std::fs::write(to, body).map_err(|e| FaucetError::Sink(e.to_string()))
        }
        async fn upload(&self, from: &Path, key: &str) -> Result<(), FaucetError> {
            if self.fail_upload.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(FaucetError::Sink("upload refused".into()));
            }
            let body = std::fs::read(from).map_err(|e| FaucetError::Sink(e.to_string()))?;
            self.objects.lock().unwrap().insert(key.to_string(), body);
            Ok(())
        }
        async fn delete(&self, key: &str) -> Result<(), FaucetError> {
            self.objects.lock().unwrap().remove(key);
            Ok(())
        }
    }

    fn text(mem: &Mem, key: &str) -> String {
        String::from_utf8(mem.objects.lock().unwrap()[key].clone()).unwrap()
    }

    fn writer(mem: &Arc<Mem>, path: Option<&str>, per_flush: bool) -> FileWriter {
        let mut s = WriteSettings::new(FileFormat::JsonLines, Compression::None);
        s.object_per_flush = per_flush;
        let (base, t) = object_layout("pre/", path, ".jsonl", s.format, s.codec, false).unwrap();
        let b = RemoteBackend::new(mem.clone(), base, &t.staging_name()).unwrap();
        FileWriter::new(s, t, Arc::new(b)).unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_numbered_template_publishes_one_object_per_flush() {
        let mem = Arc::new(Mem::default());
        let w = writer(&mem, Some("out/part-{part}.jsonl"), true);
        w.write_rows(&[serde_json::json!({"a": 1})]).unwrap();
        assert!(
            mem.objects.lock().unwrap().is_empty(),
            "nothing before a flush"
        );
        w.flush().unwrap();
        w.flush().unwrap();
        w.write_rows(&[serde_json::json!({"a": 2})]).unwrap();
        w.flush().unwrap();
        let keys: Vec<String> = mem.objects.lock().unwrap().keys().cloned().collect();
        assert_eq!(
            keys,
            ["pre/out/part-00001.jsonl", "pre/out/part-00002.jsonl"]
        );
        assert_eq!(text(&mem, "pre/out/part-00002.jsonl"), "{\"a\":2}\n");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_single_object_is_extended_by_downloading_it() {
        let mem = Arc::new(Mem::default());
        let w = writer(&mem, Some("one.jsonl"), true);
        w.write_rows(&[serde_json::json!({"a": 1})]).unwrap();
        w.flush().unwrap();
        w.write_rows(&[serde_json::json!({"a": 2})]).unwrap();
        w.flush().unwrap();
        assert_eq!(text(&mem, "pre/one.jsonl"), "{\"a\":1}\n{\"a\":2}\n");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn legacy_naming_is_a_run_id_and_a_part_number() {
        let mem = Arc::new(Mem::default());
        let w = writer(&mem, None, true);
        w.write_rows(&[serde_json::json!({"a": 1})]).unwrap();
        w.flush().unwrap();
        let keys: Vec<String> = mem.objects.lock().unwrap().keys().cloned().collect();
        assert_eq!(keys.len(), 1);
        assert!(
            keys[0].starts_with("pre/") && keys[0].ends_with("-00001.jsonl"),
            "{keys:?}"
        );
        assert!(
            w.backend()
                .describe(Area::Destination, "x")
                .starts_with("mem://pre/")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn overwrite_stages_then_swaps_and_removes_stale_parts() {
        let mem = Arc::new(Mem::default());
        mem.objects
            .lock()
            .unwrap()
            .insert("pre/d/part-00009.jsonl".into(), b"old\n".to_vec());
        let mut s = WriteSettings::new(FileFormat::JsonLines, Compression::None);
        s.write_mode = crate::write::FileWriteMode::Overwrite;
        s.mode = crate::write::FileMode::Overwrite;
        let (base, t) = object_layout("pre/", Some("d/"), "", s.format, s.codec, false).unwrap();
        let backend = Arc::new(RemoteBackend::new(mem.clone(), base, &t.staging_name()).unwrap());
        assert!(!backend.staging_ready().unwrap());
        let w = FileWriter::new(s, t, backend.clone()).unwrap();
        w.begin_overwrite().unwrap();
        assert!(backend.staging_ready().unwrap());
        w.write_rows(&[serde_json::json!({"a": 1})]).unwrap();
        w.flush().unwrap();
        assert!(
            mem.objects
                .lock()
                .unwrap()
                .contains_key("pre/d/part-00009.jsonl")
        );
        w.commit_overwrite().unwrap();
        let keys: Vec<String> = mem.objects.lock().unwrap().keys().cloned().collect();
        assert_eq!(keys, ["pre/d/part-00001.jsonl"]);
        assert!(!backend.staging_ready().unwrap());
        w.abort_overwrite().unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_failed_upload_is_an_error_and_publishes_nothing() {
        let mem = Arc::new(Mem::default());
        let w = writer(&mem, Some("x.jsonl"), false);
        mem.fail_upload
            .store(true, std::sync::atomic::Ordering::SeqCst);
        w.write_rows(&[serde_json::json!({"a": 1})]).unwrap();
        assert!(
            w.flush()
                .unwrap_err()
                .to_string()
                .contains("upload refused")
        );
        assert!(mem.objects.lock().unwrap().is_empty());
    }

    const DELAY_MS: u64 = 150;

    /// Records how many uploads overlap.
    #[derive(Default)]
    struct Slow {
        mem: Mem,
        now: std::sync::atomic::AtomicUsize,
        peak: std::sync::atomic::AtomicUsize,
        fail_key: Mutex<Option<String>>,
    }

    #[faucet_core::async_trait]
    impl ObjectClient for Slow {
        fn describe(&self, key: &str) -> String {
            self.mem.describe(key)
        }
        async fn list(&self, prefix: &str) -> Result<Vec<String>, FaucetError> {
            self.mem.list(prefix).await
        }
        async fn exists(&self, key: &str) -> Result<bool, FaucetError> {
            self.mem.exists(key).await
        }
        async fn download(&self, key: &str, to: &Path) -> Result<(), FaucetError> {
            self.mem.download(key, to).await
        }
        async fn upload(&self, from: &Path, key: &str) -> Result<(), FaucetError> {
            use std::sync::atomic::Ordering::SeqCst;
            let body = std::fs::read(from).map_err(|e| FaucetError::Sink(e.to_string()))?;
            let n = self.now.fetch_add(1, SeqCst) + 1;
            self.peak.fetch_max(n, SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(DELAY_MS)).await;
            self.now.fetch_sub(1, SeqCst);
            if self.fail_key.lock().unwrap().as_deref() == Some(key) {
                return Err(FaucetError::Sink(format!("refused {key}")));
            }
            self.mem.objects.lock().unwrap().insert(key.into(), body);
            Ok(())
        }
        async fn delete(&self, key: &str) -> Result<(), FaucetError> {
            self.mem.delete(key).await
        }
    }

    fn pipelined(client: &Arc<Slow>, uploads: usize, path: &str) -> FileWriter {
        let mut s = WriteSettings::new(FileFormat::JsonLines, Compression::None);
        s.object_per_flush = true;
        s.max_records_per_file = Some(1);
        let (base, t) = object_layout("", Some(path), "", s.format, s.codec, true).unwrap();
        let b = RemoteBackend::new(client.clone(), base, &t.staging_name())
            .unwrap()
            .with_upload_concurrency(uploads);
        assert_eq!(b.upload_concurrency(), uploads);
        FileWriter::new(s, t, Arc::new(b)).unwrap()
    }

    fn rows(n: usize) -> Vec<serde_json::Value> {
        (0..n).map(|i| serde_json::json!({ "i": i })).collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_page_that_rolls_into_many_objects_uploads_them_concurrently() {
        let client = Arc::new(Slow::default());
        let w = pipelined(&client, 5, "o/");
        let started = std::time::Instant::now();
        w.write_rows(&rows(20)).unwrap();
        w.flush().unwrap();
        let elapsed = started.elapsed();
        assert_eq!(client.mem.objects.lock().unwrap().len(), 20);
        assert_eq!(
            client.peak.load(std::sync::atomic::Ordering::SeqCst),
            5,
            "uploads overlap up to the limit and never beyond it"
        );
        assert_eq!(client.now.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert!(
            elapsed < std::time::Duration::from_millis(20 * DELAY_MS / 2),
            "20 uploads of {DELAY_MS} ms took {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn one_upload_in_flight_is_sequential() {
        let client = Arc::new(Slow::default());
        let w = pipelined(&client, 1, "o/");
        w.write_rows(&rows(4)).unwrap();
        w.flush().unwrap();
        assert_eq!(client.peak.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(client.mem.objects.lock().unwrap().len(), 4);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_background_upload_failure_fails_the_flush() {
        let client = Arc::new(Slow::default());
        *client.fail_key.lock().unwrap() = Some("o/part-00002.jsonl".into());
        let w = pipelined(&client, 4, "o/");
        let e = w
            .write_rows(&rows(6))
            .and_then(|_| w.flush())
            .unwrap_err()
            .to_string();
        assert!(e.contains("refused o/part-00002.jsonl"), "{e}");
        assert_eq!(client.now.load(std::sync::atomic::Ordering::SeqCst), 0);
        w.flush().unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_failed_upload_fails_the_batch_that_wrote_it() {
        let client = Arc::new(Slow::default());
        *client.fail_key.lock().unwrap() = Some("o/part-00001.jsonl".into());
        let w = pipelined(&client, 2, "o/");
        let e = w.write_rows(&rows(1)).unwrap_err().to_string();
        assert!(e.contains("refused"), "{e}");
        w.write_rows(&rows(1)).unwrap();
        w.flush().unwrap();
        assert_eq!(client.mem.objects.lock().unwrap().len(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn overwrite_waits_for_staged_uploads_and_abort_clears_them() {
        let client = Arc::new(Slow::default());
        let mut s = WriteSettings::new(FileFormat::JsonLines, Compression::None);
        s.write_mode = crate::write::FileWriteMode::Overwrite;
        s.object_per_flush = true;
        s.max_records_per_file = Some(1);
        let (base, t) = object_layout("", Some("d/"), "", s.format, s.codec, true).unwrap();
        let b = RemoteBackend::new(client.clone(), base, &t.staging_name())
            .unwrap()
            .with_upload_concurrency(3);
        let w = FileWriter::new(s, t, Arc::new(b)).unwrap();
        w.begin_overwrite().unwrap();
        w.write_rows(&rows(5)).unwrap();
        w.commit_overwrite().unwrap();
        let keys: Vec<String> = client.mem.objects.lock().unwrap().keys().cloned().collect();
        assert_eq!(keys.len(), 5, "{keys:?}");
        assert!(keys.iter().all(|k| k.starts_with("d/part-")), "{keys:?}");

        w.begin_overwrite().unwrap();
        w.write_rows(&rows(4)).unwrap();
        w.abort_overwrite().unwrap();
        assert_eq!(client.now.load(std::sync::atomic::Ordering::SeqCst), 0);
        let keys: Vec<String> = client.mem.objects.lock().unwrap().keys().cloned().collect();
        assert!(keys.iter().all(|k| !k.contains("staging")), "{keys:?}");
        assert_eq!(keys.len(), 5, "{keys:?}");
    }

    #[test]
    fn pipelined_uploads_run_outside_a_runtime() {
        let client = Arc::new(Slow::default());
        let w = pipelined(&client, 3, "o/");
        w.write_rows(&rows(6)).unwrap();
        w.flush().unwrap();
        assert_eq!(client.mem.objects.lock().unwrap().len(), 6);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn object_per_write_closes_an_object_at_the_end_of_each_batch_write() {
        let mem = Arc::new(Mem::default());
        let mut s = WriteSettings::new(FileFormat::JsonLines, Compression::None);
        s.object_per_flush = true;
        s.object_per_write = true;
        let (base, t) = object_layout("pre/", None, ".jsonl", s.format, s.codec, false).unwrap();
        let b = RemoteBackend::new(mem.clone(), base, &t.staging_name()).unwrap();
        assert!(!b.sync_scratch());
        let w = FileWriter::new(s, t, Arc::new(b)).unwrap();
        w.write_rows(&rows(3)).unwrap();
        assert_eq!(
            mem.objects.lock().unwrap().len(),
            1,
            "published at write end"
        );
        w.write_rows(&rows(2)).unwrap();
        w.flush().unwrap();
        let o = mem.objects.lock().unwrap();
        let bodies: Vec<usize> = o
            .values()
            .map(|b| b.iter().filter(|c| **c == b'\n').count())
            .collect();
        assert_eq!(bodies, [3, 2]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_default_rename_copies_through_a_local_file() {
        let mem = Mem::default();
        mem.objects
            .lock()
            .unwrap()
            .insert("a".into(), b"x".to_vec());
        mem.rename("a", "b").await.unwrap();
        assert_eq!(text(&mem, "b"), "x");
        assert!(!mem.exists("a").await.unwrap());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_current_thread_runtime_uses_the_io_runtime() {
        let mem = Arc::new(Mem::default());
        let w = writer(&mem, Some("ct.jsonl"), false);
        w.write_rows(&[serde_json::json!({"a": 1})]).unwrap();
        w.flush().unwrap();
        assert_eq!(text(&mem, "pre/ct.jsonl"), "{\"a\":1}\n");
    }

    #[test]
    fn outside_a_runtime_runs_and_children_are_direct() {
        assert_eq!(run(async { Ok(7) }).unwrap(), 7);
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
        let mem = Arc::new(Mem::default());
        let b = RemoteBackend::new(mem, "b/", "stage").unwrap();
        assert_eq!(b.base(), "b/");
        assert_eq!(b.staging_prefix(), "b/stage/");
        assert!(format!("{b:?}").contains("b/stage/"));
        assert!(
            b.scratch_path(Area::Staging, "x/y")
                .unwrap()
                .ends_with("s-x_y")
        );
    }
}
