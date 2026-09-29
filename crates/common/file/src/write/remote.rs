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

/// A [`StorageBackend`] over an [`ObjectClient`].
pub struct RemoteBackend {
    client: Arc<dyn ObjectClient>,
    base: String,
    staging: String,
    scratch: tempfile::TempDir,
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
            scratch,
        })
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
        let result = run(self.client.upload(scratch, &self.key(area, name)));
        let _ = std::fs::remove_file(scratch);
        result
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
