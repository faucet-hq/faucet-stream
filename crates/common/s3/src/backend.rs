//! [`S3Backend`]: the shared file writer's storage on an S3 bucket.

use crate::objects::{PART_BYTES, S3Objects};
use crate::parts::S3Parts;
use aws_sdk_s3::Client;
use faucet_common_file::write::{NameTemplate, RemoteBackend};
use faucet_core::FaucetError;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// A [`StorageBackend`](faucet_common_file::write::StorageBackend) on an S3
/// bucket (or any S3-compatible store).
///
/// A file up to [`PART_BYTES`] is one `PutObject`; a larger one a multipart
/// upload, completed only after every part landed and aborted on any
/// failure, so an object is either wholly published or not at all. Line
/// formats go up part by part while they are written. A move into place is
/// a server-side copy (`UploadPartCopy` past 5 GiB) and a delete; deletes
/// are batched 1,000 keys per `DeleteObjects`.
///
/// Every request runs through the run's call context (#880) and reports to
/// its round-trip recorder; the preflight probe is a `HeadBucket`.
///
/// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
/// minor release; any change is called out in the changelog.
pub struct S3Backend {
    remote: RemoteBackend,
    objects: Arc<S3Objects>,
}

faucet_common_file::delegate_storage_backend!(S3Backend, remote);

impl S3Backend {
    /// Start building a backend on `bucket` through `client`.
    pub fn builder(client: Client, bucket: impl Into<String>) -> S3BackendBuilder {
        S3BackendBuilder {
            client,
            bucket: bucket.into(),
            concurrency: 1,
            part_bytes: PART_BYTES,
            scratch_dir: None,
        }
    }

    /// The S3 client.
    pub fn client(&self) -> &Client {
        &self.objects.client
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

impl std::fmt::Debug for S3Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Backend")
            .field("bucket", &self.objects.bucket)
            .field("base", &self.remote.base())
            .finish()
    }
}

/// Builds an [`S3Backend`].
///
/// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
/// minor release; any change is called out in the changelog.
#[must_use]
pub struct S3BackendBuilder {
    client: Client,
    bucket: String,
    concurrency: usize,
    part_bytes: usize,
    scratch_dir: Option<PathBuf>,
}

impl S3BackendBuilder {
    /// Objects (and parts of one object) uploaded at once. Default 1.
    pub fn concurrency(mut self, n: usize) -> Self {
        self.concurrency = n;
        self
    }

    /// The multipart part size, and the largest object sent as one
    /// `PutObject`. Default [`PART_BYTES`]; S3 needs at least 5 MiB for every
    /// part but the last.
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
    ) -> Result<S3Backend, FaucetError> {
        let slots = self.concurrency.max(1);
        let objects = Arc::new(S3Objects {
            client: self.client,
            bucket: self.bucket,
            concurrency: self.concurrency,
            part_bytes: self.part_bytes,
            roundtrips: Arc::new(faucet_core::observability::RecorderSlot::new()),
            calls: Arc::new(faucet_core::CallSlot::new(
                faucet_core::CallContext::new()
                    .without_timeout()
                    .with_roundtrip_metering(false),
            )),
            part_slots: Arc::new(tokio::sync::Semaphore::new(slots)),
        });
        let remote =
            RemoteBackend::new(objects.clone(), base, template, self.scratch_dir.as_deref())?
                .with_upload_concurrency(self.concurrency)
                .with_multipart(Arc::new(S3Parts(objects.clone())));
        Ok(S3Backend { remote, objects })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_common_file::write::{Area, StorageBackend};
    use faucet_core::FileFormat;

    fn client(endpoint: &str) -> Client {
        let conf = aws_sdk_s3::Config::builder()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .endpoint_url(endpoint)
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "k", "s", None, None, "test",
            ))
            .force_path_style(true)
            .retry_config(aws_sdk_s3::config::retry::RetryConfig::disabled())
            .build();
        Client::from_conf(conf)
    }

    fn template() -> NameTemplate {
        NameTemplate::from_path(
            "x.jsonl",
            FileFormat::JsonLines,
            faucet_core::Compression::None,
            false,
        )
        .unwrap()
        .1
    }

    #[test]
    fn keys_are_the_base_and_the_name() {
        let b = S3Backend::builder(client("http://127.0.0.1:9"), "bkt")
            .concurrency(3)
            .part_size(0)
            .scratch_dir(None)
            .build("pre/", &template())
            .unwrap();
        assert_eq!(b.bucket(), "bkt");
        assert_eq!(
            b.describe(Area::Destination, "x.jsonl"),
            "s3://bkt/pre/x.jsonl"
        );
        assert_eq!(b.part_size(), Some(1));
        assert_eq!(b.remote().upload_concurrency(), 3);
        assert!(b.client().config().region().is_some());
        assert!(format!("{b:?}").contains("bkt"));
    }

    #[tokio::test]
    async fn the_probe_is_a_head_bucket() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path("/bkt/"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("HEAD"))
            .and(path("/gone/"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        let ctx = faucet_core::check::CheckContext::default();
        let ok = S3Backend::builder(client(&server.uri()), "bkt")
            .build("", &template())
            .unwrap();
        let report = ok.check(&ctx).await.unwrap().expect("a probe");
        assert_eq!(report.failed_count(), 0, "{report:?}");
        let gone = S3Backend::builder(client(&server.uri()), "gone")
            .build("", &template())
            .unwrap();
        let report = gone.check(&ctx).await.unwrap().expect("a probe");
        assert_eq!(report.failed_count(), 1);
    }
}
