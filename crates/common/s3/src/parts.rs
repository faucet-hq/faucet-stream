//! S3 multipart uploads for objects written while they are encoded (#783).

use crate::objects::S3Objects;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use faucet_common_file::write::{BoxFuture, MultipartClient, MultipartUpload, content_type};
use faucet_core::CallExt as _;
use faucet_core::FaucetError;
use std::sync::{Arc, Mutex};

/// S3 multipart uploads for objects written in parts (#783).
pub(crate) struct S3Parts(pub(crate) Arc<S3Objects>);

fn s3_err(what: &str, key: &str, e: impl std::fmt::Display) -> FaucetError {
    FaucetError::Sink(format!("S3 {what} error for key '{key}': {e}"))
}

#[faucet_core::async_trait]
impl MultipartClient for S3Parts {
    fn part_size(&self) -> usize {
        self.0.part_bytes
    }

    async fn start(&self, key: &str) -> Result<Box<dyn MultipartUpload>, FaucetError> {
        let o = &self.0;
        o.roundtrips.record("put");
        let created = o
            .client
            .create_multipart_upload()
            .bucket(&o.bucket)
            .key(key)
            .content_type(content_type(key))
            .send()
            .within(&o.calls, "put")
            .await?
            .map_err(|e| o.sdk_err("start multipart", key, e))?;
        let upload_id = created
            .upload_id()
            .ok_or_else(|| s3_err("start multipart", key, "no upload id"))?
            .to_string();
        Ok(Box::new(S3Upload {
            objects: o.clone(),
            key: key.to_string(),
            upload_id,
            parts: Arc::new(Mutex::new(Vec::new())),
        }))
    }
}

/// The S3 part number of part `number`, refusing one past S3's limit of
/// [`MAX_PARTS`](crate::objects::MAX_PARTS) parts per upload.
fn part_number(number: u32, key: &str) -> Result<i32, FaucetError> {
    if u64::from(number) > crate::objects::MAX_PARTS {
        return Err(s3_err(
            "upload part",
            key,
            format!(
                "the object needs more than S3's {} parts; set `max_bytes_per_file` so files \
                 roll over before that",
                crate::objects::MAX_PARTS
            ),
        ));
    }
    i32::try_from(number).map_err(|_| s3_err("upload part", key, "part number out of range"))
}

struct S3Upload {
    objects: Arc<S3Objects>,
    key: String,
    upload_id: String,
    parts: Arc<Mutex<Vec<CompletedPart>>>,
}

impl MultipartUpload for S3Upload {
    fn put_part(&self, number: u32, body: Vec<u8>) -> BoxFuture<Result<(), FaucetError>> {
        let (o, key, id, parts) = (
            self.objects.clone(),
            self.key.clone(),
            self.upload_id.clone(),
            self.parts.clone(),
        );
        Box::pin(async move {
            let number = part_number(number, &key)?;
            let _slot = o
                .part_slots
                .acquire()
                .await
                .map_err(|e| s3_err("upload part", &key, e))?;
            o.roundtrips.record("put");
            let out = o
                .client
                .upload_part()
                .bucket(&o.bucket)
                .key(&key)
                .upload_id(&id)
                .part_number(number)
                .body(ByteStream::from(body))
                .send()
                .within(&o.calls, "put")
                .await?
                .map_err(|e| o.sdk_err("upload part", &key, e))?;
            parts.lock().unwrap_or_else(|p| p.into_inner()).push(
                CompletedPart::builder()
                    .part_number(number)
                    .set_e_tag(out.e_tag().map(str::to_string))
                    .build(),
            );
            Ok(())
        })
    }

    fn complete(self: Box<Self>) -> BoxFuture<Result<(), FaucetError>> {
        Box::pin(async move {
            let mut parts =
                std::mem::take(&mut *self.parts.lock().unwrap_or_else(|p| p.into_inner()));
            parts.sort_by_key(|p| p.part_number());
            let o = &self.objects;
            o.roundtrips.record("put");
            o.client
                .complete_multipart_upload()
                .bucket(&o.bucket)
                .key(&self.key)
                .upload_id(&self.upload_id)
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .set_parts(Some(parts))
                        .build(),
                )
                .send()
                .within(&o.calls, "put")
                .await?
                .map_err(|e| o.sdk_err("complete multipart", &self.key, e))?;
            Ok(())
        })
    }

    fn abort(self: Box<Self>) -> BoxFuture<Result<(), FaucetError>> {
        Box::pin(async move {
            let o = &self.objects;
            o.client
                .abort_multipart_upload()
                .bucket(&o.bucket)
                .key(&self.key)
                .upload_id(&self.upload_id)
                .send()
                .within(&o.calls, "put")
                .await?
                .map_err(|e| o.sdk_err("abort multipart", &self.key, e))?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #862 L-163: a streamed object stops with a clear error at S3's
    /// 10,000-part limit instead of an opaque rejection from the store.
    #[test]
    fn part_numbers_stop_at_the_s3_limit() {
        assert_eq!(part_number(1, "k").unwrap(), 1);
        assert_eq!(part_number(10_000, "k").unwrap(), 10_000);
        let err = part_number(10_001, "k").unwrap_err().to_string();
        assert!(
            err.contains("10000 parts") && err.contains("max_bytes_per_file"),
            "{err}"
        );
    }
}
