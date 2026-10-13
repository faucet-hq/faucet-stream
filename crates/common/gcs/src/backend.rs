//! [`GcsBackend`]: the shared file writer's storage on a GCS bucket (#777,
//! #886), over the GCS [`ObjectClient`].
//!
//! Uploads stream the finished local file; past the client's resumable
//! threshold that is a resumable upload, which GCS finalises only once every
//! byte arrived, so an object is either wholly published or not at all.
//!
//! A failure that carries a retryable status (HTTP 429 / 5xx, or the gRPC
//! `RESOURCE_EXHAUSTED` / `UNAVAILABLE` / `INTERNAL` codes) is a typed
//! [`FaucetError::HttpStatus`], so the pipeline's resilience policy retries
//! it (#783).

use async_trait::async_trait;
use faucet_common_file::write::{NameTemplate, ObjectClient, RemoteBackend, content_type};
use faucet_core::CallExt as _;
use faucet_core::FaucetError;
use faucet_core::check::{CheckContext, CheckReport, Probe};
use faucet_core::observability::RoundtripRecorder;
use google_cloud_gax::paginator::ItemPaginator as _;
use google_cloud_storage::client::{Storage, StorageControl};
use std::path::{Path, PathBuf};
use std::sync::Arc;

struct GcsObjects {
    storage: Storage,
    control: StorageControl,
    bucket: String,
    roundtrips: Arc<faucet_core::observability::RecorderSlot>,
    /// The run's call context (#880) every request runs through, once.
    calls: Arc<faucet_core::CallSlot>,
}

fn err(what: &str, key: &str, e: impl std::fmt::Display) -> FaucetError {
    FaucetError::Sink(format!("GCS {what} error for key '{key}': {e}"))
}

/// A rename's rewrite is resumed at most this many times before it fails:
/// a server that never reports `done` would otherwise loop forever.
pub const MAX_REWRITE_CALLS: usize = 1000;

/// The HTTP status a client error stands for: its own, or the equivalent of
/// a retryable gRPC code.
fn status_of(e: &google_cloud_storage::Error) -> Option<u16> {
    use google_cloud_gax::error::rpc::Code;
    e.http_status_code().or_else(|| {
        e.status().and_then(|s| match s.code {
            Code::ResourceExhausted => Some(429),
            Code::Unavailable => Some(503),
            Code::Internal => Some(500),
            _ => None,
        })
    })
}

/// Whether a client error means "no such object".
fn is_not_found(e: &google_cloud_storage::Error) -> bool {
    e.http_status_code() == Some(404)
        || e.status()
            .is_some_and(|s| s.code == google_cloud_gax::error::rpc::Code::NotFound)
}

impl GcsObjects {
    /// A client failure, typed by its status (see the module docs).
    fn gcs_err(&self, what: &str, key: &str, e: google_cloud_storage::Error) -> FaucetError {
        FaucetError::sink_status(
            status_of(&e),
            self.describe(key),
            format!("GCS {what} error for key '{key}': {e}"),
        )
    }

    fn bucket_path(&self) -> String {
        format!("projects/_/buckets/{}", self.bucket)
    }
}

#[async_trait]
impl ObjectClient for GcsObjects {
    fn describe(&self, key: &str) -> String {
        format!("gs://{}/{key}", self.bucket)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, FaucetError> {
        self.roundtrips.record("list");
        let mut items = self
            .control
            .list_objects()
            .set_parent(self.bucket_path())
            .set_prefix(prefix.to_string())
            .set_page_size(1000_i32)
            .by_item();
        let mut names = Vec::new();
        while let Some(item) = items.next().within(&self.calls, "list").await? {
            let object = item.map_err(|e| self.gcs_err("list", prefix, e))?;
            if !object.name.is_empty() {
                names.push(object.name);
            }
        }
        Ok(names)
    }

    async fn exists(&self, key: &str) -> Result<bool, FaucetError> {
        self.roundtrips.record("head");
        match self
            .control
            .get_object()
            .set_bucket(self.bucket_path())
            .set_object(key.to_string())
            .send()
            .within(&self.calls, "head")
            .await?
        {
            Ok(_) => Ok(true),
            Err(e) if is_not_found(&e) => Ok(false),
            Err(e) => Err(self.gcs_err("get metadata", key, e)),
        }
    }

    async fn download(&self, key: &str, to: &Path) -> Result<(), FaucetError> {
        use tokio::io::AsyncWriteExt as _;
        self.roundtrips.record("get");
        let mut resp = self
            .storage
            .read_object(self.bucket_path(), key.to_string())
            .send()
            .within(&self.calls, "get")
            .await?
            .map_err(|e| self.gcs_err("get", key, e))?;
        let mut file = tokio::fs::File::create(to)
            .await
            .map_err(|e| err("create local copy", key, e))?;
        while let Some(chunk) = resp.next().within(&self.calls, "get").await? {
            let chunk = chunk.map_err(|e| self.gcs_err("download", key, e))?;
            file.write_all(&chunk)
                .await
                .map_err(|e| err("download", key, e))?;
        }
        file.flush().await.map_err(|e| err("download", key, e))
    }

    async fn upload(&self, from: &Path, key: &str) -> Result<(), FaucetError> {
        let (file, crc) = crate::open_with_crc32c(from)
            .await
            .map_err(|e| err("open local file", key, e))?;
        self.roundtrips.record("put");
        self.storage
            .write_object(self.bucket_path(), key.to_string(), file)
            .set_content_type(content_type(key))
            .with_known_crc32c(crc)
            .send_unbuffered()
            .within(&self.calls, "put")
            .await?
            .map_err(|e| self.gcs_err("put object", key, e))?;
        tracing::debug!(key = %key, "Uploaded GCS object");
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), FaucetError> {
        self.roundtrips.record("delete");
        match self
            .control
            .delete_object()
            .set_bucket(self.bucket_path())
            .set_object(key.to_string())
            .send()
            .within(&self.calls, "delete")
            .await?
        {
            Ok(_) => Ok(()),
            Err(e) if is_not_found(&e) => Ok(()),
            Err(e) => Err(self.gcs_err("delete", key, e)),
        }
    }

    async fn rename(&self, from: &str, to: &str) -> Result<(), FaucetError> {
        let mut token = String::new();
        let mut calls = 0;
        loop {
            calls += 1;
            if calls > MAX_REWRITE_CALLS {
                return Err(err(
                    "copy",
                    to,
                    format!("the rewrite did not finish after {MAX_REWRITE_CALLS} calls"),
                ));
            }
            self.roundtrips.record("copy");
            let resp = self
                .control
                .rewrite_object()
                .set_source_bucket(self.bucket_path())
                .set_source_object(from.to_string())
                .set_destination_bucket(self.bucket_path())
                .set_destination_name(to.to_string())
                .set_rewrite_token(token.clone())
                .send()
                .within(&self.calls, "copy")
                .await?
                .map_err(|e| self.gcs_err("copy", to, e))?;
            if resp.done {
                break;
            }
            token = resp.rewrite_token;
        }
        self.delete(from).await
    }

    fn set_roundtrip_recorder(&self, recorder: Arc<RoundtripRecorder>) {
        self.roundtrips.install(recorder.clone());
        self.calls.install_recorder(recorder);
    }

    fn set_call_context(&self, ctx: faucet_core::CallContext) {
        self.calls.install(ctx);
    }

    /// A non-mutating `list_objects` capped at one result: the bucket is
    /// reachable and the credentials work.
    async fn check(&self, ctx: &CheckContext) -> Result<Option<CheckReport>, FaucetError> {
        let started = std::time::Instant::now();
        let request = self
            .control
            .list_objects()
            .set_parent(self.bucket_path())
            .set_page_size(1_i32);
        let sent = request.send().within(&self.calls, "list");
        let probe = match tokio::time::timeout(ctx.timeout, sent).await {
            Ok(Err(e)) => Probe::fail("auth", started.elapsed(), e.to_string()),
            Ok(Ok(Ok(_))) => Probe::pass("auth", started.elapsed()),
            Ok(Ok(Err(e))) => Probe::fail_hint(
                "auth",
                started.elapsed(),
                e.to_string(),
                "check bucket name, credentials, and network",
            ),
            Err(_) => Probe::fail("network", started.elapsed(), "timed out"),
        };
        Ok(Some(CheckReport::single(probe)))
    }
}

/// A [`StorageBackend`](faucet_common_file::write::StorageBackend) on a GCS
/// bucket.
///
/// Each file is built in a local scratch file and published with one
/// upload — resumable past the client's threshold, finalised only once
/// every byte arrived — carrying its CRC32C so the store verifies it. A move
/// into place is a `rewrite` (resumed until done, at most
/// [`MAX_REWRITE_CALLS`] calls) and a delete.
///
/// Every request runs through the run's call context (#880) and reports to
/// its round-trip recorder; the preflight probe lists one object.
///
/// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
/// minor release; any change is called out in the changelog.
pub struct GcsBackend {
    remote: RemoteBackend,
    objects: Arc<GcsObjects>,
}

faucet_common_file::delegate_storage_backend!(GcsBackend, remote);

impl GcsBackend {
    /// Start building a backend on `bucket` through the two clients
    /// ([`build_storage`](crate::build_storage) and
    /// [`build_storage_control`](crate::build_storage_control)).
    pub fn builder(
        storage: Storage,
        control: StorageControl,
        bucket: impl Into<String>,
    ) -> GcsBackendBuilder {
        GcsBackendBuilder {
            storage,
            control,
            bucket: bucket.into(),
            concurrency: 1,
            scratch_dir: None,
        }
    }

    /// The control-plane client.
    pub fn control(&self) -> &StorageControl {
        &self.objects.control
    }

    /// The bucket.
    pub fn bucket(&self) -> &str {
        &self.objects.bucket
    }

    /// The generic remote backend this one is built on.
    pub fn remote(&self) -> &RemoteBackend {
        &self.remote
    }
}

impl std::fmt::Debug for GcsBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcsBackend")
            .field("bucket", &self.objects.bucket)
            .field("base", &self.remote.base())
            .finish()
    }
}

/// Builds a [`GcsBackend`].
///
/// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
/// minor release; any change is called out in the changelog.
#[must_use]
pub struct GcsBackendBuilder {
    storage: Storage,
    control: StorageControl,
    bucket: String,
    concurrency: usize,
    scratch_dir: Option<PathBuf>,
}

impl GcsBackendBuilder {
    /// Objects uploaded at once. Default 1.
    pub fn concurrency(mut self, n: usize) -> Self {
        self.concurrency = n;
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
    ) -> Result<GcsBackend, FaucetError> {
        let objects = Arc::new(GcsObjects {
            storage: self.storage,
            control: self.control,
            bucket: self.bucket,
            roundtrips: Arc::new(faucet_core::observability::RecorderSlot::new()),
            calls: Arc::new(faucet_core::CallSlot::new(
                faucet_core::CallContext::new()
                    .without_timeout()
                    .with_roundtrip_metering(false),
            )),
        });
        let remote =
            RemoteBackend::new(objects.clone(), base, template, self.scratch_dir.as_deref())?
                .with_upload_concurrency(self.concurrency);
        Ok(GcsBackend { remote, objects })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn objects(host: &str) -> GcsObjects {
        let creds = crate::GcsCredentials::Anonymous;
        GcsObjects {
            storage: crate::build_storage(&creds, Some(host)).await.unwrap(),
            control: crate::build_storage_control(&creds, Some(host))
                .await
                .unwrap(),
            bucket: "b".into(),
            roundtrips: Arc::new(faucet_core::observability::RecorderSlot::new()),
            calls: Arc::new(faucet_core::CallSlot::default()),
        }
    }

    /// L6 (#783): a rename resumes its rewrite with the returned token, and a
    /// rewrite that never reports `done` fails after a bounded number of
    /// calls instead of looping forever.
    #[tokio::test]
    async fn renames_resume_rewrites_and_give_up_after_a_bound() {
        use wiremock::matchers::{method, path, query_param, query_param_is_missing};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/storage/v1/b/b/o/s/rewriteTo/b/b/o/d"))
            .and(query_param_is_missing("rewriteToken"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"done": false, "rewriteToken": "t1"})),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/storage/v1/b/b/o/s/rewriteTo/b/b/o/d"))
            .and(query_param("rewriteToken", "t1"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"done": true})),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/storage/v1/b/b/o/s"))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/storage/v1/b/b/o/stuck/rewriteTo/b/b/o/d"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"done": false, "rewriteToken": "t"})),
            )
            .expect(MAX_REWRITE_CALLS as u64)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/storage/v1/b/b/o/busy/rewriteTo/b/b/o/d"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        let o = objects(&server.uri()).await;
        o.rename("s", "d").await.unwrap();
        let e = o.rename("stuck", "d").await.unwrap_err().to_string();
        assert!(e.contains("did not finish after 1000 calls"), "{e}");
        let e = o.rename("busy", "d").await.unwrap_err();
        assert!(
            matches!(e, FaucetError::HttpStatus { status: 503, ref url, .. } if url == "gs://b/d"),
            "{e:?}"
        );
        assert!(faucet_core::FaucetError::is_retriable(&e));
    }

    /// #803: the upload's CRC32C travels in the object metadata, and the
    /// request carries exactly the metadata and media parts — no trailing
    /// checksum part a two-part server would store as content.
    #[tokio::test]
    async fn uploads_send_the_crc32c_in_the_metadata_part() {
        use base64::Engine as _;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        let body = b"{\"a\":1}\n";
        let encoded = base64::engine::general_purpose::STANDARD
            .encode(crate::crc32c_of_bytes(body).to_be_bytes());
        Mock::given(method("POST"))
            .and(path("/upload/storage/v1/b/b/o"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "bucket": "b", "name": "k.jsonl", "crc32c": encoded,
            })))
            .expect(1)
            .mount(&server)
            .await;
        let o = objects(&server.uri()).await;
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("k.jsonl");
        std::fs::write(&file, body).unwrap();
        o.upload(&file, "k.jsonl").await.unwrap();

        let requests = server.received_requests().await.unwrap();
        let sent = String::from_utf8_lossy(&requests[0].body).into_owned();
        let content_type = requests[0].headers["content-type"].to_str().unwrap();
        let boundary = content_type.split("boundary=").nth(1).unwrap();
        let parts = sent.matches(&format!("--{boundary}\r\n")).count();
        assert_eq!(parts, 2, "metadata + media only: {sent}");
        assert!(
            sent.contains(&format!("\"crc32c\":\"{encoded}\"")),
            "the checksum is declared up front: {sent}"
        );
    }

    #[tokio::test]
    async fn the_backend_names_keys_and_probes_with_one_listing() {
        use faucet_common_file::write::{Area, StorageBackend};
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/storage/v1/b/b/o"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;
        let creds = crate::GcsCredentials::Anonymous;
        let host = Some(server.uri());
        let template = NameTemplate::from_path(
            "x.jsonl",
            faucet_core::FileFormat::JsonLines,
            faucet_core::Compression::None,
            false,
        )
        .unwrap()
        .1;
        let backend = GcsBackend::builder(
            crate::build_storage(&creds, host.as_deref()).await.unwrap(),
            crate::build_storage_control(&creds, host.as_deref())
                .await
                .unwrap(),
            "b",
        )
        .concurrency(2)
        .scratch_dir(None)
        .build("pre/", &template)
        .unwrap();
        assert_eq!(backend.bucket(), "b");
        assert_eq!(
            backend.describe(Area::Destination, "x.jsonl"),
            "gs://b/pre/x.jsonl"
        );
        assert_eq!(backend.remote().upload_concurrency(), 2);
        assert!(format!("{backend:?}").contains("GcsBackend"));
        let _ = backend.control();
        backend.set_call_context(faucet_core::CallContext::new());
        backend.set_roundtrip_recorder(Arc::new(
            faucet_core::observability::RoundtripRecorder::new(
                faucet_core::observability::RoundtripSide::Sink,
                "p",
                "r",
                "gcs",
            ),
        ));
        let report = backend
            .check(&CheckContext::default())
            .await
            .unwrap()
            .expect("a probe");
        assert_eq!(report.failed_count(), 0, "{report:?}");
    }

    #[test]
    fn grpc_codes_map_to_their_http_status() {
        use google_cloud_gax::error::rpc::{Code, Status};
        let e = |c| google_cloud_storage::Error::service(Status::default().set_code(c));
        assert_eq!(status_of(&e(Code::ResourceExhausted)), Some(429));
        assert_eq!(status_of(&e(Code::Unavailable)), Some(503));
        assert_eq!(status_of(&e(Code::Internal)), Some(500));
        assert_eq!(status_of(&e(Code::PermissionDenied)), None);
    }

    #[tokio::test]
    async fn uploading_a_missing_local_file_names_the_step_and_the_key() {
        let creds = crate::GcsCredentials::Anonymous;
        let host = Some("http://127.0.0.1:9");
        let o = GcsObjects {
            storage: crate::build_storage(&creds, host).await.unwrap(),
            control: crate::build_storage_control(&creds, host).await.unwrap(),
            bucket: "b".into(),
            roundtrips: Arc::new(faucet_core::observability::RecorderSlot::new()),
            calls: Arc::new(faucet_core::CallSlot::default()),
        };
        let dir = tempfile::tempdir().unwrap();
        let e = o
            .upload(&dir.path().join("missing"), "k")
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("GCS open local file error for key 'k'"), "{e}");
        assert_eq!(o.describe("k"), "gs://b/k");
    }
}
