//! The S3 [`ObjectClient`] behind the shared file writer (#777).
//!
//! A file up to [`PART_BYTES`] is one `PutObject`; a larger one is a
//! multipart upload of `PART_BYTES` parts (up to `concurrency` in flight),
//! completed only after every part landed and aborted on any failure, so an
//! object is either wholly published or not at all.

use async_trait::async_trait;
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use faucet_common_file::write::ObjectClient;
use faucet_core::FaucetError;
use futures::stream::{StreamExt, TryStreamExt};
use std::path::Path;
use std::sync::Arc;

/// Part size for multipart uploads; also the single-`PutObject` limit.
/// S3 requires every part but the last to be at least 5 MiB.
pub const PART_BYTES: usize = 8 * 1024 * 1024;

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

/// The content type an object named `key` is served with.
pub(crate) fn content_type(key: &str) -> &'static str {
    let name = key.trim_end_matches(".gz").trim_end_matches(".zst");
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
            .map_err(|e| err("put object", key, aws_sdk_s3::error::DisplayErrorContext(e)))?;
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
            .map_err(|e| {
                err(
                    "start multipart",
                    key,
                    aws_sdk_s3::error::DisplayErrorContext(e),
                )
            })?;
        let upload_id = created
            .upload_id()
            .ok_or_else(|| err("start multipart", key, "no upload id"))?
            .to_string();
        let result = self.upload_parts(from, key, &upload_id, len).await;
        match result {
            Ok(parts) => {
                self.roundtrips.record("put");
                self.client
                    .complete_multipart_upload()
                    .bucket(&self.bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .multipart_upload(
                        CompletedMultipartUpload::builder()
                            .set_parts(Some(parts))
                            .build(),
                    )
                    .send()
                    .await
                    .map_err(|e| {
                        err(
                            "complete multipart",
                            key,
                            aws_sdk_s3::error::DisplayErrorContext(e),
                        )
                    })?;
                Ok(())
            }
            Err(e) => {
                let _ = self
                    .client
                    .abort_multipart_upload()
                    .bucket(&self.bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .send()
                    .await;
                Err(e)
            }
        }
    }

    async fn upload_parts(
        &self,
        from: &Path,
        key: &str,
        upload_id: &str,
        len: usize,
    ) -> Result<Vec<CompletedPart>, FaucetError> {
        let count = len.div_ceil(self.part_bytes);
        let mut parts: Vec<CompletedPart> = futures::stream::iter(0..count)
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
                    .map_err(|e| {
                        err(
                            "upload part",
                            key,
                            aws_sdk_s3::error::DisplayErrorContext(e),
                        )
                    })?;
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
        parts.sort_by_key(|p| p.part_number());
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
                .map_err(|e| err("list", prefix, aws_sdk_s3::error::DisplayErrorContext(e)))?;
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
                _ => Err(err(
                    "head object",
                    key,
                    aws_sdk_s3::error::DisplayErrorContext(e),
                )),
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
            .map_err(|e| err("get object", key, aws_sdk_s3::error::DisplayErrorContext(e)))?;
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
            .map_err(|e| {
                err(
                    "delete object",
                    key,
                    aws_sdk_s3::error::DisplayErrorContext(e),
                )
            })?;
        Ok(())
    }

    async fn rename(&self, from: &str, to: &str) -> Result<(), FaucetError> {
        self.roundtrips.record("copy");
        self.client
            .copy_object()
            .bucket(&self.bucket)
            .key(to)
            .copy_source(copy_source(&self.bucket, from))
            .send()
            .await
            .map_err(|e| err("copy object", to, aws_sdk_s3::error::DisplayErrorContext(e)))?;
        self.delete(from).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_types_and_copy_sources() {
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
        assert_eq!(copy_source("b", "d/a b+c.jsonl"), "b/d/a%20b%2Bc.jsonl");
    }
}
