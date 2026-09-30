//! The S3 [`ObjectClient`] behind the shared file writer (#777).
//!
//! A file up to [`PART_BYTES`] is one `PutObject`; a larger one is a
//! multipart upload of `PART_BYTES` parts (up to `concurrency` in flight per
//! object), completed only after every part landed and aborted on any
//! failure, so an object is either wholly published or not at all. The
//! writer uploads up to `concurrency` objects at once, so at most
//! `concurrency × concurrency` parts are in flight.
//!
//! A rename is a server-side copy and a delete: one `CopyObject` up to
//! [`MAX_COPY_BYTES`], a multipart `UploadPartCopy` above it (#783).
//!
//! A failure that carries an HTTP status of 429 or 5xx (`SlowDown`,
//! `ServiceUnavailable`, …) is a typed [`FaucetError::HttpStatus`], so the
//! pipeline's resilience policy retries it.

use async_trait::async_trait;
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use faucet_common_file::write::{ObjectClient, content_type};
use faucet_core::FaucetError;
use futures::stream::{StreamExt, TryStreamExt};
use std::path::Path;
use std::sync::Arc;

/// Part size for multipart uploads; also the single-`PutObject` limit.
/// S3 requires every part but the last to be at least 5 MiB.
pub const PART_BYTES: usize = 8 * 1024 * 1024;

/// The largest object one `CopyObject` copies; larger ones are copied in
/// parts.
pub const MAX_COPY_BYTES: u64 = 5 * 1024 * 1024 * 1024;

/// Part size of a multipart copy, grown for very large objects so a copy
/// never needs more than [`MAX_PARTS`] parts.
const COPY_PART_BYTES: u64 = 512 * 1024 * 1024;

/// S3's limit on the parts of one multipart upload.
const MAX_PARTS: u64 = 10_000;

type SdkError<E> = aws_sdk_s3::error::SdkError<E, aws_sdk_s3::config::http::HttpResponse>;

pub(crate) struct S3Objects {
    pub client: Client,
    pub bucket: String,
    pub concurrency: usize,
    pub part_bytes: usize,
    pub roundtrips: Arc<faucet_core::observability::RecorderSlot>,
}

fn err(what: &str, key: &str, e: impl std::fmt::Display) -> FaucetError {
    FaucetError::Sink(format!("S3 {what} error for key '{key}': {e}"))
}

/// The byte ranges (`start..end`) a multipart copy of `len` bytes copies.
fn copy_ranges(len: u64) -> Vec<std::ops::Range<u64>> {
    let part = COPY_PART_BYTES.max(len.div_ceil(MAX_PARTS));
    (0..len.div_ceil(part))
        .map(|i| i * part..((i + 1) * part).min(len))
        .collect()
}

/// `bucket/key` for `CopySource`, percent-encoding everything but the
/// unreserved characters and `/`.
fn copy_source(bucket: &str, key: &str) -> String {
    let mut out = format!("{bucket}/");
    for b in key.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.~/".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

impl S3Objects {
    /// An SDK failure, typed by its HTTP status (see the module docs).
    fn sdk_err<E>(&self, what: &str, key: &str, e: SdkError<E>) -> FaucetError
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        let status = e.raw_response().map(|r| r.status().as_u16());
        FaucetError::sink_status(
            status,
            self.describe(key),
            format!(
                "S3 {what} error for key '{key}': {}",
                aws_sdk_s3::error::DisplayErrorContext(&e)
            ),
        )
    }

    async fn put_single(&self, body: Vec<u8>, key: &str) -> Result<(), FaucetError> {
        self.roundtrips.record("put");
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .content_type(content_type(key))
            .body(ByteStream::from(body))
            .send()
            .await
            .map_err(|e| self.sdk_err("put object", key, e))?;
        Ok(())
    }

    async fn put_multipart(&self, from: &Path, key: &str, len: usize) -> Result<(), FaucetError> {
        self.roundtrips.record("put");
        let created = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .content_type(content_type(key))
            .send()
            .await
            .map_err(|e| self.sdk_err("start multipart", key, e))?;
        let upload_id = created
            .upload_id()
            .ok_or_else(|| err("start multipart", key, "no upload id"))?
            .to_string();
        let parts = self.upload_parts(from, key, &upload_id, len).await;
        self.finish_multipart(key, &upload_id, parts).await
    }

    async fn upload_parts(
        &self,
        from: &Path,
        key: &str,
        upload_id: &str,
        len: usize,
    ) -> Result<Vec<CompletedPart>, FaucetError> {
        let count = len.div_ceil(self.part_bytes);
        let parts: Vec<CompletedPart> = futures::stream::iter(0..count)
            .map(|i| async move {
                let offset = i * self.part_bytes;
                let size = self.part_bytes.min(len - offset);
                let body = ByteStream::read_from()
                    .path(from)
                    .offset(offset as u64)
                    .length(aws_sdk_s3::primitives::Length::Exact(size as u64))
                    .build()
                    .await
                    .map_err(|e| err("read part", key, e))?;
                let number = i as i32 + 1;
                self.roundtrips.record("put");
                let out = self
                    .client
                    .upload_part()
                    .bucket(&self.bucket)
                    .key(key)
                    .upload_id(upload_id)
                    .part_number(number)
                    .body(body)
                    .send()
                    .await
                    .map_err(|e| self.sdk_err("upload part", key, e))?;
                Ok::<_, FaucetError>(
                    CompletedPart::builder()
                        .part_number(number)
                        .set_e_tag(out.e_tag().map(str::to_string))
                        .build(),
                )
            })
            .buffer_unordered(self.concurrency.max(1))
            .try_collect()
            .await?;
        Ok(parts)
    }
}

#[async_trait]
impl ObjectClient for S3Objects {
    fn describe(&self, key: &str) -> String {
        format!("s3://{}/{key}", self.bucket)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, FaucetError> {
        let mut keys = Vec::new();
        let mut token: Option<String> = None;
        loop {
            self.roundtrips.record("list");
            let page = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix)
                .set_continuation_token(token.take())
                .send()
                .await
                .map_err(|e| self.sdk_err("list", prefix, e))?;
            keys.extend(
                page.contents()
                    .iter()
                    .filter_map(|o| o.key().map(str::to_string)),
            );
            match page.next_continuation_token() {
                Some(t) if page.is_truncated() == Some(true) => token = Some(t.to_string()),
                _ => return Ok(keys),
            }
        }
    }

    async fn exists(&self, key: &str) -> Result<bool, FaucetError> {
        self.roundtrips.record("head");
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(e) => match e.as_service_error() {
                Some(se) if se.is_not_found() => Ok(false),
                _ => Err(self.sdk_err("head object", key, e)),
            },
        }
    }

    async fn download(&self, key: &str, to: &Path) -> Result<(), FaucetError> {
        self.roundtrips.record("get");
        let out = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| self.sdk_err("get object", key, e))?;
        let mut reader = out.body.into_async_read();
        let mut file = tokio::fs::File::create(to)
            .await
            .map_err(|e| err("create local copy", key, e))?;
        tokio::io::copy(&mut reader, &mut file)
            .await
            .map_err(|e| err("download", key, e))?;
        Ok(())
    }

    async fn upload(&self, from: &Path, key: &str) -> Result<(), FaucetError> {
        let len = tokio::fs::metadata(from)
            .await
            .map_err(|e| err("stat local file", key, e))?
            .len() as usize;
        if len <= self.part_bytes {
            let body = tokio::fs::read(from)
                .await
                .map_err(|e| err("read local file", key, e))?;
            self.put_single(body, key).await?;
        } else {
            self.put_multipart(from, key, len).await?;
        }
        tracing::debug!(key = %key, bytes = len, "Uploaded S3 object");
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), FaucetError> {
        self.roundtrips.record("delete");
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| self.sdk_err("delete object", key, e))?;
        Ok(())
    }

    async fn rename(&self, from: &str, to: &str) -> Result<(), FaucetError> {
        self.roundtrips.record("head");
        let head = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(from)
            .send()
            .await
            .map_err(|e| self.sdk_err("head object", from, e))?;
        let len = head
            .content_length()
            .and_then(|l| u64::try_from(l).ok())
            .ok_or_else(|| err("copy object", from, "no Content-Length on the source"))?;
        if len > MAX_COPY_BYTES {
            self.copy_multipart(from, to, len).await?;
        } else {
            self.roundtrips.record("copy");
            self.client
                .copy_object()
                .bucket(&self.bucket)
                .key(to)
                .copy_source(copy_source(&self.bucket, from))
                .send()
                .await
                .map_err(|e| self.sdk_err("copy object", to, e))?;
        }
        self.delete(from).await
    }
}

impl S3Objects {
    /// Copy an object above [`MAX_COPY_BYTES`] with `UploadPartCopy`, up to
    /// `concurrency` parts at once; aborted on any failure, so `to` is either
    /// the whole copy or untouched.
    async fn copy_multipart(&self, from: &str, to: &str, len: u64) -> Result<(), FaucetError> {
        self.roundtrips.record("copy");
        let created = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(to)
            .content_type(content_type(to))
            .send()
            .await
            .map_err(|e| self.sdk_err("start multipart copy", to, e))?;
        let upload_id = created
            .upload_id()
            .ok_or_else(|| err("start multipart copy", to, "no upload id"))?
            .to_string();
        let source = copy_source(&self.bucket, from);
        let parts: Result<Vec<CompletedPart>, FaucetError> =
            futures::stream::iter(copy_ranges(len).into_iter().enumerate())
                .map(|(i, range)| {
                    let (source, upload_id) = (&source, &upload_id);
                    async move {
                        let number = i as i32 + 1;
                        self.roundtrips.record("copy");
                        let out = self
                            .client
                            .upload_part_copy()
                            .bucket(&self.bucket)
                            .key(to)
                            .upload_id(upload_id)
                            .part_number(number)
                            .copy_source(source)
                            .copy_source_range(format!("bytes={}-{}", range.start, range.end - 1))
                            .send()
                            .await
                            .map_err(|e| self.sdk_err("copy part", to, e))?;
                        Ok(CompletedPart::builder()
                            .part_number(number)
                            .set_e_tag(
                                out.copy_part_result()
                                    .and_then(|r| r.e_tag())
                                    .map(str::to_string),
                            )
                            .build())
                    }
                })
                .buffer_unordered(self.concurrency.max(1))
                .try_collect()
                .await;
        self.finish_multipart(to, &upload_id, parts).await
    }

    /// Complete a multipart upload with `parts`, or abort it when they
    /// failed.
    async fn finish_multipart(
        &self,
        key: &str,
        upload_id: &str,
        parts: Result<Vec<CompletedPart>, FaucetError>,
    ) -> Result<(), FaucetError> {
        match parts {
            Ok(mut parts) => {
                parts.sort_by_key(|p| p.part_number());
                self.roundtrips.record("put");
                self.client
                    .complete_multipart_upload()
                    .bucket(&self.bucket)
                    .key(key)
                    .upload_id(upload_id)
                    .multipart_upload(
                        CompletedMultipartUpload::builder()
                            .set_parts(Some(parts))
                            .build(),
                    )
                    .send()
                    .await
                    .map_err(|e| self.sdk_err("complete multipart", key, e))?;
                Ok(())
            }
            Err(e) => {
                let _ = self
                    .client
                    .abort_multipart_upload()
                    .bucket(&self.bucket)
                    .key(key)
                    .upload_id(upload_id)
                    .send()
                    .await;
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_sources_are_percent_encoded() {
        assert_eq!(copy_source("b", "d/a b+c.jsonl"), "b/d/a%20b%2Bc.jsonl");
    }

    fn objects(endpoint: &str) -> S3Objects {
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
        S3Objects {
            client: Client::from_conf(conf),
            bucket: "b".into(),
            concurrency: 1,
            part_bytes: PART_BYTES,
            roundtrips: Arc::new(faucet_core::observability::RecorderSlot::new()),
        }
    }

    #[tokio::test]
    async fn local_file_failures_name_the_step_and_the_key() {
        let o = objects("http://127.0.0.1:9");
        let dir = tempfile::tempdir().unwrap();
        let e = o
            .upload(&dir.path().join("missing"), "k1")
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("S3 stat local file error for key 'k1'"), "{e}");
        let e = o.upload(dir.path(), "k2").await.unwrap_err().to_string();
        assert!(e.contains("S3 read local file error for key 'k2'"), "{e}");
    }

    #[tokio::test]
    async fn a_download_into_a_missing_directory_is_an_error() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_string("x"))
            .mount(&server)
            .await;
        let o = objects(&server.uri());
        let dir = tempfile::tempdir().unwrap();
        let e = o
            .download("k", &dir.path().join("no/such/file"))
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("S3 create local copy error for key 'k'"), "{e}");
    }

    #[test]
    fn copy_ranges_are_contiguous_and_bounded() {
        let six = 6 * 1024 * 1024 * 1024_u64;
        let r = copy_ranges(six);
        assert_eq!(r.len(), 12);
        assert_eq!(r[0], 0..COPY_PART_BYTES);
        assert_eq!(r.last().unwrap().end, six);
        assert!(r.windows(2).all(|w| w[0].end == w[1].start));
        let huge = 50 * 1024 * 1024 * 1024 * 1024_u64;
        let r = copy_ranges(huge);
        assert!(r.len() as u64 <= MAX_PARTS);
        assert_eq!(r.last().unwrap().end, huge);
    }

    fn xml(body: &str) -> wiremock::ResponseTemplate {
        wiremock::ResponseTemplate::new(200)
            .insert_header("content-type", "application/xml")
            .set_body_string(body.to_string())
    }

    /// C5 (#783): a promote of an object above 5 GiB — which one `CopyObject`
    /// refuses — is a multipart `UploadPartCopy` of 512 MiB ranges.
    #[tokio::test]
    async fn an_object_above_five_gib_is_copied_in_parts() {
        use wiremock::matchers::{header_exists, method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path("/b/stage/big"))
            .respond_with(ResponseTemplate::new(200).insert_header("content-length", "6442450944"))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/b/out/big"))
            .and(query_param("uploads", ""))
            .respond_with(xml(
                "<InitiateMultipartUploadResult><Bucket>b</Bucket><Key>out/big</Key>\
                 <UploadId>u1</UploadId></InitiateMultipartUploadResult>",
            ))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/b/out/big"))
            .and(query_param("uploadId", "u1"))
            .and(header_exists("x-amz-copy-source-range"))
            .respond_with(xml("<CopyPartResult><ETag>\"e\"</ETag></CopyPartResult>"))
            .expect(12)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/b/out/big"))
            .and(query_param("uploadId", "u1"))
            .respond_with(xml(
                "<CompleteMultipartUploadResult><Key>out/big</Key></CompleteMultipartUploadResult>",
            ))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/b/stage/big"))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&server)
            .await;
        let o = objects(&server.uri());
        o.rename("stage/big", "out/big").await.unwrap();
        let ranges: Vec<String> = server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter_map(|r| r.headers.get("x-amz-copy-source-range"))
            .map(|v| v.to_str().unwrap().to_string())
            .collect();
        assert!(ranges.contains(&"bytes=0-536870911".to_string()), "{ranges:?}");
        assert!(ranges.contains(&"bytes=5905580032-6442450943".to_string()), "{ranges:?}");
    }

    #[tokio::test]
    async fn a_failed_part_copy_aborts_and_small_objects_use_one_copy() {
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path("/b/big"))
            .respond_with(ResponseTemplate::new(200).insert_header("content-length", "6442450944"))
            .mount(&server)
            .await;
        Mock::given(method("HEAD"))
            .and(path("/b/small"))
            .respond_with(ResponseTemplate::new(200).insert_header("content-length", "10"))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/b/dst"))
            .and(query_param("uploads", ""))
            .respond_with(xml(
                "<InitiateMultipartUploadResult><UploadId>u2</UploadId></InitiateMultipartUploadResult>",
            ))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/b/dst"))
            .and(query_param("uploadId", "u2"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/b/dst"))
            .and(query_param("uploadId", "u2"))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/b/dst2"))
            .respond_with(xml("<CopyObjectResult><ETag>\"e\"</ETag></CopyObjectResult>"))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/b/small"))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&server)
            .await;
        let o = objects(&server.uri());
        let e = o.rename("big", "dst").await.unwrap_err();
        assert!(matches!(e, FaucetError::Sink(ref m) if m.contains("copy part")), "{e}");
        o.rename("small", "dst2").await.unwrap();
    }

    /// M5 (#783): a 503 `SlowDown` is a typed `HttpStatus`, which the
    /// resilience policy retries; a 403 stays a plain sink error.
    #[tokio::test]
    async fn throttling_is_a_typed_retryable_status() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/b/slow"))
            .respond_with(ResponseTemplate::new(503).set_body_string(
                "<Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message></Error>",
            ))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/b/denied"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;
        let o = objects(&server.uri());
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("f");
        std::fs::write(&src, b"x").unwrap();
        let e = o.upload(&src, "slow").await.unwrap_err();
        match &e {
            FaucetError::HttpStatus { status, url, body } => {
                assert_eq!(*status, 503);
                assert_eq!(url, "s3://b/slow");
                assert!(body.contains("S3 put object error for key 'slow'"), "{body}");
            }
            other => panic!("expected HttpStatus, got {other:?}"),
        }
        assert_eq!(
            faucet_core::resilience::classify(&e),
            Some(faucet_core::resilience::RetryClass::Http5xx)
        );
        let e = o.upload(&src, "denied").await.unwrap_err();
        assert!(matches!(e, FaucetError::Sink(_)), "{e:?}");
    }
}
