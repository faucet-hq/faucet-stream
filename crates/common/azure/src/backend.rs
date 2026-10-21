//! [`AzureBackend`]: the shared file writer's storage on an Azure Blob
//! container (#777, #886), over the Azure Blob [`ObjectClient`].
//!
//! A blob up to [`PART_BYTES`] is one `Put Blob`; a larger one is uploaded
//! as blocks (up to `concurrency` in flight) and committed with one
//! `Put Block List`, which is what makes it visible — a failed upload is
//! aborted and leaves nothing behind.

use async_trait::async_trait;
use faucet_common_file::write::{
    BoxFuture, MultipartClient, MultipartUpload, NameTemplate, ObjectClient, RemoteBackend,
};
use faucet_core::CallExt as _;
use faucet_core::FaucetError;
use faucet_core::check::{CheckContext, CheckReport, Probe};
use faucet_core::observability::RoundtripRecorder;
use futures::StreamExt;
use object_store::list::{PaginatedListOptions, PaginatedListStore};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Block size for large blobs; also the single-`put` limit.
pub const PART_BYTES: usize = 8 * 1024 * 1024;

struct AzureObjects {
    store: Arc<dyn ObjectStore>,
    /// Lists by a raw key prefix, a page per request, when the store has it.
    paginated: Option<Arc<dyn PaginatedListStore>>,
    container: String,
    part_bytes: usize,
    concurrency: usize,
    roundtrips: Arc<faucet_core::observability::RecorderSlot>,
    /// The run's call context (#880) every request runs through, once.
    calls: Arc<faucet_core::CallSlot>,
}

fn err(what: &str, key: &str, e: impl std::fmt::Display) -> FaucetError {
    FaucetError::Sink(format!("azure {what} error for key '{key}': {e}"))
}

/// The directory part of a key prefix: `object_store` lists whole path
/// segments, so `a/b-` lists `a/` and is filtered client-side.
fn list_root(prefix: &str) -> Option<ObjectPath> {
    prefix
        .rfind('/')
        .map(|i| ObjectPath::from(&prefix[..i]))
        .filter(|p| !p.as_ref().is_empty())
}

impl AzureObjects {
    /// List the keys starting with `prefix` server-side (`prefix=` on List
    /// Blobs), one request per page, rather than every blob under its
    /// directory.
    async fn list_by_prefix(
        &self,
        store: &dyn PaginatedListStore,
        prefix: &str,
    ) -> Result<Vec<String>, FaucetError> {
        let raw = (!prefix.is_empty()).then_some(prefix);
        let mut keys = Vec::new();
        let mut token = None;
        loop {
            self.roundtrips.record("list");
            let opts = PaginatedListOptions {
                page_token: token.take(),
                ..Default::default()
            };
            let page = store
                .list_paginated(raw, opts)
                .within(&self.calls, "list")
                .await?
                .map_err(|e| err("list", prefix, e))?;
            keys.extend(
                page.result
                    .objects
                    .into_iter()
                    .map(|m| m.location.to_string())
                    .filter(|k| k.starts_with(prefix)),
            );
            match page.page_token {
                Some(next) if !next.is_empty() => token = Some(next),
                _ => return Ok(keys),
            }
        }
    }
}

#[async_trait]
impl ObjectClient for AzureObjects {
    fn describe(&self, key: &str) -> String {
        format!("az://{}/{key}", self.container)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, FaucetError> {
        if let Some(paginated) = &self.paginated {
            return self.list_by_prefix(paginated.as_ref(), prefix).await;
        }
        let root = list_root(prefix);
        self.roundtrips.record("list");
        let mut listing = self.store.list(root.as_ref());
        let mut keys = Vec::new();
        while let Some(meta) = listing.next().within(&self.calls, "list").await? {
            let meta = meta.map_err(|e| err("list", prefix, e))?;
            let key = meta.location.to_string();
            if key.starts_with(prefix) {
                keys.push(key);
            }
        }
        Ok(keys)
    }

    async fn exists(&self, key: &str) -> Result<bool, FaucetError> {
        self.roundtrips.record("head");
        match self
            .store
            .head(&ObjectPath::from(key))
            .within(&self.calls, "head")
            .await?
        {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(err("head", key, e)),
        }
    }

    async fn download(&self, key: &str, to: &Path) -> Result<(), FaucetError> {
        use tokio::io::AsyncWriteExt as _;
        self.roundtrips.record("get");
        let got = self
            .store
            .get(&ObjectPath::from(key))
            .within(&self.calls, "get")
            .await?
            .map_err(|e| err("get", key, e))?;
        let mut stream = got.into_stream();
        let mut file = tokio::fs::File::create(to)
            .await
            .map_err(|e| err("create local copy", key, e))?;
        while let Some(chunk) = stream.next().within(&self.calls, "get").await? {
            let chunk = chunk.map_err(|e| err("download", key, e))?;
            file.write_all(&chunk)
                .await
                .map_err(|e| err("download", key, e))?;
        }
        file.flush().await.map_err(|e| err("download", key, e))
    }

    async fn upload(&self, from: &Path, key: &str) -> Result<(), FaucetError> {
        use tokio::io::AsyncReadExt as _;
        let path = ObjectPath::from(key);
        let len = tokio::fs::metadata(from)
            .await
            .map_err(|e| err("stat local file", key, e))?
            .len() as usize;
        if len <= self.part_bytes {
            let body = tokio::fs::read(from)
                .await
                .map_err(|e| err("read local file", key, e))?;
            self.roundtrips.record("put");
            self.store
                .put(&path, bytes::Bytes::from(body).into())
                .within(&self.calls, "put")
                .await?
                .map_err(|e| err("put object", key, e))?;
            return Ok(());
        }
        let mut file = tokio::fs::File::open(from)
            .await
            .map_err(|e| err("open local file", key, e))?;
        let upload = self
            .store
            .put_multipart(&path)
            .within(&self.calls, "put")
            .await?
            .map_err(|e| err("start multipart", key, e))?;
        let mut writer = object_store::WriteMultipart::new_with_chunk_size(upload, self.part_bytes);
        let mut result = Ok(());
        let mut sent = 0;
        while sent < len {
            match writer
                .wait_for_capacity(self.concurrency.max(1))
                .within(&self.calls, "put")
                .await
            {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    result = Err(err("put part", key, e));
                    break;
                }
                Err(spent) => {
                    result = Err(spent);
                    break;
                }
            }
            let mut part = vec![0u8; self.part_bytes.min(len - sent)];
            if let Err(e) = file.read_exact(&mut part).await {
                result = Err(err("read local file", key, e));
                break;
            }
            sent += part.len();
            self.roundtrips.record("put");
            writer.put(bytes::Bytes::from(part));
        }
        if result.is_ok() {
            self.roundtrips.record("put");
            return match writer.finish().within(&self.calls, "put").await? {
                Ok(_) => Ok(()),
                Err(e) => Err(err("complete multipart", key, e)),
            };
        }
        let _ = writer.abort().await;
        result
    }

    async fn delete(&self, key: &str) -> Result<(), FaucetError> {
        self.roundtrips.record("delete");
        match self
            .store
            .delete(&ObjectPath::from(key))
            .within(&self.calls, "delete")
            .await?
        {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(err("delete", key, e)),
        }
    }

    async fn rename(&self, from: &str, to: &str) -> Result<(), FaucetError> {
        self.roundtrips.record("copy");
        self.store
            .copy(&ObjectPath::from(from), &ObjectPath::from(to))
            .within(&self.calls, "copy")
            .await?
            .map_err(|e| err("copy", to, e))?;
        self.delete(from).await
    }

    fn set_roundtrip_recorder(&self, recorder: Arc<RoundtripRecorder>) {
        self.roundtrips.install(recorder.clone());
        self.calls.install_recorder(recorder);
    }

    fn set_call_context(&self, ctx: faucet_core::CallContext) {
        self.calls.install(ctx);
    }

    /// A non-mutating listing of the container's first blob: the account
    /// and container are reachable and the credentials work.
    async fn check(&self, ctx: &CheckContext) -> Result<Option<CheckReport>, FaucetError> {
        let started = std::time::Instant::now();
        let listed = tokio::time::timeout(ctx.timeout, async {
            let mut listing = self.store.list(None);
            listing.next().within(&self.calls, "list").await
        })
        .await;
        let probe = match listed {
            Ok(Err(e)) => Probe::fail("auth", started.elapsed(), e.to_string()),
            Ok(Ok(None | Some(Ok(_)))) => Probe::pass("auth", started.elapsed()),
            Ok(Ok(Some(Err(e)))) => Probe::fail_hint(
                "auth",
                started.elapsed(),
                e.to_string(),
                "check account, container, credentials, and network",
            ),
            Err(_) => Probe::fail("network", started.elapsed(), "timed out"),
        };
        Ok(Some(CheckReport::single(probe)))
    }
}

/// Azure block uploads for blobs written in parts (#783).
struct AzureBlocks {
    store: Arc<dyn ObjectStore>,
    part_bytes: usize,
    roundtrips: Arc<faucet_core::observability::RecorderSlot>,
    calls: Arc<faucet_core::CallSlot>,
}

fn azure_err(what: &str, key: &str, e: impl std::fmt::Display) -> FaucetError {
    FaucetError::Sink(format!("azure {what} error for key '{key}': {e}"))
}

#[faucet_core::async_trait]
impl MultipartClient for AzureBlocks {
    fn part_size(&self) -> usize {
        self.part_bytes
    }

    async fn start(&self, key: &str) -> Result<Box<dyn MultipartUpload>, FaucetError> {
        let upload = faucet_core::CallExt::within(
            self.store.put_multipart(&ObjectPath::from(key)),
            &self.calls,
            "put",
        )
        .await?
        .map_err(|e| azure_err("start multipart", key, e))?;
        Ok(Box::new(AzureUpload {
            key: key.to_string(),
            upload: Arc::new(Mutex::new(upload)),
            roundtrips: self.roundtrips.clone(),
            calls: self.calls.clone(),
        }))
    }
}

struct AzureUpload {
    key: String,
    upload: Arc<Mutex<Box<dyn object_store::MultipartUpload>>>,
    roundtrips: Arc<faucet_core::observability::RecorderSlot>,
    calls: Arc<faucet_core::CallSlot>,
}

impl MultipartUpload for AzureUpload {
    fn put_part(&self, _number: u32, body: Vec<u8>) -> BoxFuture<Result<(), FaucetError>> {
        self.roundtrips.record("put");
        let part = self
            .upload
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .put_part(bytes::Bytes::from(body).into());
        let key = self.key.clone();
        let calls = self.calls.clone();
        Box::pin(async move {
            faucet_core::CallExt::within(part, &calls, "put")
                .await?
                .map_err(|e| azure_err("put part", &key, e))
        })
    }

    fn complete(self: Box<Self>) -> BoxFuture<Result<(), FaucetError>> {
        self.roundtrips.record("put");
        Box::pin(async move {
            let mut upload = Arc::try_unwrap(self.upload)
                .map_err(|_| azure_err("complete multipart", &self.key, "a part is still open"))?
                .into_inner()
                .unwrap_or_else(|p| p.into_inner());
            faucet_core::CallExt::within(upload.complete(), &self.calls, "put")
                .await?
                .map(|_| ())
                .map_err(|e| azure_err("complete multipart", &self.key, e))
        })
    }

    fn abort(self: Box<Self>) -> BoxFuture<Result<(), FaucetError>> {
        Box::pin(async move {
            let Ok(upload) = Arc::try_unwrap(self.upload) else {
                return Ok(());
            };
            let mut upload = upload.into_inner().unwrap_or_else(|p| p.into_inner());
            faucet_core::CallExt::within(upload.abort(), &self.calls, "abort")
                .await?
                .map_err(|e| azure_err("abort multipart", &self.key, e))
        })
    }
}

/// A [`StorageBackend`](faucet_common_file::write::StorageBackend) on an
/// Azure Blob container (or ADLS Gen2 filesystem).
///
/// A blob up to [`PART_BYTES`] is one `Put Blob`; a larger one is uploaded as
/// blocks and committed with one `Put Block List`, which is what makes it
/// visible. Line formats go up block by block while they are written. A
/// move into place is a server-side copy and a delete.
///
/// Every request runs through the run's call context (#880) and reports to
/// its round-trip recorder; the preflight probe lists the container.
///
/// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
/// minor release; any change is called out in the changelog.
pub struct AzureBackend {
    remote: RemoteBackend,
    objects: Arc<AzureObjects>,
}

faucet_common_file::delegate_storage_backend!(AzureBackend, remote);

impl AzureBackend {
    /// Start building a backend on `container` through `store`
    /// ([`build_store`](crate::build_store)).
    pub fn builder(
        store: Arc<dyn ObjectStore>,
        container: impl Into<String>,
    ) -> AzureBackendBuilder {
        AzureBackendBuilder {
            store,
            paginated: None,
            container: container.into(),
            concurrency: 1,
            part_bytes: PART_BYTES,
            scratch_dir: None,
        }
    }

    /// Start building a backend on an Azure store
    /// ([`build_azure`](crate::build_azure)): keys are listed by their exact
    /// prefix on the server, a page per request.
    pub fn builder_for(
        store: Arc<object_store::azure::MicrosoftAzure>,
        container: impl Into<String>,
    ) -> AzureBackendBuilder {
        Self::builder(store.clone(), container).paginated_list(store)
    }

    /// The object store.
    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.objects.store
    }

    /// The container.
    pub fn container(&self) -> &str {
        &self.objects.container
    }

    /// The generic remote backend this one is built on.
    pub fn remote(&self) -> &RemoteBackend {
        &self.remote
    }
}

impl std::fmt::Debug for AzureBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzureBackend")
            .field("container", &self.objects.container)
            .field("base", &self.remote.base())
            .finish()
    }
}

/// Builds an [`AzureBackend`].
///
/// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
/// minor release; any change is called out in the changelog.
#[must_use]
pub struct AzureBackendBuilder {
    store: Arc<dyn ObjectStore>,
    paginated: Option<Arc<dyn PaginatedListStore>>,
    container: String,
    concurrency: usize,
    part_bytes: usize,
    scratch_dir: Option<PathBuf>,
}

impl AzureBackendBuilder {
    /// List keys through `store` by their exact prefix, a page per request,
    /// instead of walking the store's directory listing and filtering it.
    pub fn paginated_list(mut self, store: Arc<dyn PaginatedListStore>) -> Self {
        self.paginated = Some(store);
        self
    }

    /// Blobs (and blocks of one blob) uploaded at once. Default 1.
    pub fn concurrency(mut self, n: usize) -> Self {
        self.concurrency = n;
        self
    }

    /// The block size, and the largest blob sent as one `Put Blob`. Default
    /// [`PART_BYTES`].
    pub fn part_size(mut self, bytes: usize) -> Self {
        self.part_bytes = bytes.max(1);
        self
    }

    /// Where files are built before they are uploaded. Default: the system
    /// temporary directory.
    pub fn scratch_dir(mut self, dir: Option<&Path>) -> Self {
        self.scratch_dir = dir.map(Path::to_path_buf);
        self
    }

    /// The backend for keys `base` + a name `template` produces.
    pub fn build(
        self,
        base: impl Into<String>,
        template: &NameTemplate,
    ) -> Result<AzureBackend, FaucetError> {
        let roundtrips = Arc::new(faucet_core::observability::RecorderSlot::new());
        let calls = Arc::new(faucet_core::CallSlot::new(
            faucet_core::CallContext::new()
                .without_timeout()
                .with_roundtrip_metering(false),
        ));
        let objects = Arc::new(AzureObjects {
            store: self.store.clone(),
            paginated: self.paginated,
            container: self.container,
            part_bytes: self.part_bytes,
            concurrency: self.concurrency,
            roundtrips: roundtrips.clone(),
            calls: calls.clone(),
        });
        let remote =
            RemoteBackend::new(objects.clone(), base, template, self.scratch_dir.as_deref())?
                .with_upload_concurrency(self.concurrency)
                .with_multipart(Arc::new(AzureBlocks {
                    store: self.store,
                    part_bytes: self.part_bytes,
                    roundtrips,
                    calls,
                }));
        Ok(AzureBackend { remote, objects })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn the_backend_streams_blocks_and_probes_the_container() {
        use faucet_common_file::write::{Area, StorageBackend};
        let template = NameTemplate::from_path(
            "x.jsonl",
            faucet_core::FileFormat::JsonLines,
            faucet_core::Compression::None,
            false,
        )
        .unwrap()
        .1;
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let backend = AzureBackend::builder(store.clone(), "c")
            .concurrency(2)
            .part_size(4)
            .scratch_dir(None)
            .build("pre/", &template)
            .unwrap();
        assert_eq!(backend.container(), "c");
        assert_eq!(
            backend.describe(Area::Destination, "x.jsonl"),
            "az://c/pre/x.jsonl"
        );
        assert_eq!(backend.part_size(), Some(4));
        assert_eq!(backend.remote().upload_concurrency(), 2);
        assert!(Arc::ptr_eq(backend.store(), &store));
        assert!(format!("{backend:?}").contains("AzureBackend"));
        backend.set_call_context(faucet_core::CallContext::new());
        backend.set_roundtrip_recorder(Arc::new(
            faucet_core::observability::RoundtripRecorder::new(
                faucet_core::observability::RoundtripSide::Sink,
                "p",
                "r",
                "azure-blob",
            ),
        ));
        let mut stream = backend
            .open_stream(Area::Destination, "x.jsonl")
            .await
            .unwrap();
        stream.put(b"0123".to_vec()).await.unwrap();
        stream.put(b"45".to_vec()).await.unwrap();
        stream.finish().await.unwrap();
        assert!(backend.exists(Area::Destination, "x.jsonl").await.unwrap());
        let report = backend
            .check(&CheckContext::default())
            .await
            .unwrap()
            .expect("a probe");
        assert_eq!(report.failed_count(), 0, "{report:?}");
    }

    /// A paginated listing that records the prefixes it was asked for and
    /// serves two pages.
    #[derive(Default)]
    struct TwoPages(Mutex<Vec<(Option<String>, Option<String>)>>);

    #[async_trait]
    impl PaginatedListStore for TwoPages {
        async fn list_paginated(
            &self,
            prefix: Option<&str>,
            opts: PaginatedListOptions,
        ) -> object_store::Result<object_store::list::PaginatedListResult> {
            self.0
                .lock()
                .unwrap()
                .push((prefix.map(str::to_string), opts.page_token.clone()));
            let meta = |k: &str| object_store::ObjectMeta {
                location: ObjectPath::from(k),
                last_modified: chrono::DateTime::<chrono::Utc>::UNIX_EPOCH,
                size: 1,
                e_tag: None,
                version: None,
            };
            let (keys, next) = match opts.page_token.as_deref() {
                None => (vec!["exports-1.jsonl"], Some("t2".to_string())),
                _ => (vec!["exports-2.jsonl"], None),
            };
            Ok(object_store::list::PaginatedListResult {
                result: object_store::ListResult {
                    common_prefixes: Vec::new(),
                    objects: keys.into_iter().map(meta).collect(),
                },
                page_token: next,
            })
        }
    }

    #[tokio::test]
    async fn a_paginated_store_lists_by_the_exact_prefix_page_by_page() {
        let pages = Arc::new(TwoPages::default());
        let objects = AzureObjects {
            store: Arc::new(object_store::memory::InMemory::new()),
            paginated: Some(pages.clone()),
            container: "c".into(),
            part_bytes: 4,
            concurrency: 1,
            roundtrips: Arc::new(faucet_core::observability::RecorderSlot::new()),
            calls: Arc::new(faucet_core::CallSlot::default()),
        };
        let keys = objects.list("exports-").await.unwrap();
        assert_eq!(keys, ["exports-1.jsonl", "exports-2.jsonl"]);
        assert_eq!(
            *pages.0.lock().unwrap(),
            [
                (Some("exports-".to_string()), None),
                (Some("exports-".to_string()), Some("t2".to_string()))
            ],
            "the root-level prefix goes to the server, not the whole container"
        );
        pages.0.lock().unwrap().clear();
        objects.list("").await.unwrap();
        assert_eq!(pages.0.lock().unwrap()[0].0, None);
    }

    #[test]
    fn list_roots_are_whole_segments() {
        assert_eq!(list_root("a/b-").unwrap().as_ref(), "a");
        assert_eq!(list_root("a/b/").unwrap().as_ref(), "a/b");
        assert!(list_root("plain").is_none());
        assert!(list_root("/").is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn round_trips_against_an_in_memory_store() {
        let objects = AzureObjects {
            store: Arc::new(object_store::memory::InMemory::new()),
            paginated: None,
            container: "c".into(),
            part_bytes: 4,
            concurrency: 2,
            roundtrips: Arc::new(faucet_core::observability::RecorderSlot::new()),
            calls: Arc::new(faucet_core::CallSlot::default()),
        };
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        std::fs::write(&src, b"0123456789").unwrap();
        objects.upload(&src, "p/a-1").await.unwrap();
        std::fs::write(&src, b"xy").unwrap();
        objects.upload(&src, "p/b").await.unwrap();
        assert!(objects.exists("p/a-1").await.unwrap());
        assert!(!objects.exists("p/zz").await.unwrap());
        let mut keys = objects.list("p/a").await.unwrap();
        keys.sort();
        assert_eq!(keys, ["p/a-1"]);
        let out = dir.path().join("out");
        objects.download("p/a-1", &out).await.unwrap();
        assert_eq!(std::fs::read(&out).unwrap(), b"0123456789");
        objects.rename("p/b", "p/c").await.unwrap();
        assert!(!objects.exists("p/b").await.unwrap());
        objects.delete("p/c").await.unwrap();
        objects.delete("p/c").await.unwrap();
        assert_eq!(objects.describe("k"), "az://c/k");
        assert!(objects.download("p/none", &out).await.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn every_call_is_counted_as_a_round_trip() {
        use faucet_core::observability::{RoundtripRecorder, RoundtripSide};
        let meter = Arc::new(faucet_core::UsageMeter::new());
        let objects = objects(Arc::new(object_store::memory::InMemory::new()), 4);
        objects.roundtrips.install(Arc::new(
            RoundtripRecorder::new(RoundtripSide::Sink, "p", "", "azure-blob")
                .with_meter(meter.clone()),
        ));
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        std::fs::write(&src, b"0123456789").unwrap();
        objects.upload(&src, "p/big").await.unwrap();
        std::fs::write(&src, b"xy").unwrap();
        objects.upload(&src, "p/small").await.unwrap();
        objects.exists("p/small").await.unwrap();
        objects.list("p/").await.unwrap();
        objects
            .download("p/small", &dir.path().join("o"))
            .await
            .unwrap();
        objects.rename("p/small", "p/moved").await.unwrap();
        let ops = meter.snapshot().sink_roundtrips;
        assert_eq!(
            ops["put"], 5,
            "three blocks, the block list and one Put Blob: {ops:?}"
        );
        assert_eq!(ops["head"], 1);
        assert_eq!(ops["list"], 1);
        assert_eq!(ops["get"], 1);
        assert_eq!(ops["copy"], 1);
        assert_eq!(ops["delete"], 1);
    }

    fn objects(store: Arc<dyn ObjectStore>, part_bytes: usize) -> AzureObjects {
        AzureObjects {
            store,
            paginated: None,
            container: "c".into(),
            part_bytes,
            concurrency: 1,
            roundtrips: Arc::new(faucet_core::observability::RecorderSlot::new()),
            calls: Arc::new(faucet_core::CallSlot::default()),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn local_file_failures_name_the_step_and_the_key() {
        let mem: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let whole = objects(mem.clone(), 1 << 20);
        let parts = objects(mem, 1);
        let dir = tempfile::tempdir().unwrap();
        let e = whole.upload(&dir.path().join("missing"), "k").await;
        assert!(
            e.unwrap_err()
                .to_string()
                .contains("azure stat local file error for key 'k'")
        );
        let e = whole.upload(dir.path(), "k").await.unwrap_err().to_string();
        assert!(e.contains("azure read local file error for key 'k'"), "{e}");
        let e = parts.upload(dir.path(), "k").await.unwrap_err().to_string();
        assert!(e.contains("azure read local file error for key 'k'"), "{e}");
        assert!(
            !parts.exists("k").await.unwrap(),
            "an aborted upload publishes nothing"
        );

        let src = dir.path().join("src");
        std::fs::write(&src, b"ab").unwrap();
        whole.upload(&src, "o").await.unwrap();
        let e = whole
            .download("o", &dir.path().join("no/such/file"))
            .await
            .unwrap_err()
            .to_string();
        assert!(
            e.contains("azure create local copy error for key 'o'"),
            "{e}"
        );
        let e = whole.rename("absent", "to").await.unwrap_err().to_string();
        assert!(e.contains("azure copy error for key 'to'"), "{e}");

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&src, std::fs::Permissions::from_mode(0o000)).unwrap();
            if std::fs::File::open(&src).is_err() {
                let e = parts.upload(&src, "p").await.unwrap_err().to_string();
                assert!(e.contains("azure open local file error for key 'p'"), "{e}");
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn store_failures_name_the_call_and_the_key() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("f"), b"").unwrap();
        let fs: Arc<dyn ObjectStore> =
            Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&root).unwrap());
        let src = dir.path().join("src");
        std::fs::write(&src, b"abc").unwrap();
        let e = objects(fs.clone(), 1 << 20)
            .upload(&src, "f/x")
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("azure put object error for key 'f/x'"), "{e}");
        let e = objects(fs, 1)
            .upload(&src, "f/y")
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("error for key 'f/y'"), "{e}");
    }
}
