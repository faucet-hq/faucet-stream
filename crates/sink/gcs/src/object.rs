//! The GCS [`ObjectClient`] behind the shared file writer (#777).
//!
//! Uploads stream the finished local file; past the client's resumable
//! threshold that is a resumable upload, which GCS finalises only once every
//! byte arrived, so an object is either wholly published or not at all.

use async_trait::async_trait;
use faucet_common_file::write::{ObjectClient, content_type};
use faucet_core::FaucetError;
use google_cloud_gax::paginator::ItemPaginator as _;
use google_cloud_storage::client::{Storage, StorageControl};
use std::path::Path;
use std::sync::Arc;

pub(crate) struct GcsObjects {
    pub storage: Storage,
    pub control: StorageControl,
    pub bucket: String,
    pub roundtrips: Arc<faucet_core::observability::RecorderSlot>,
}

fn err(what: &str, key: &str, e: impl std::fmt::Display) -> FaucetError {
    FaucetError::Sink(format!("GCS {what} error for key '{key}': {e}"))
}

/// Whether a client error means "no such object".
pub(crate) fn is_not_found(e: &google_cloud_storage::Error) -> bool {
    e.http_status_code() == Some(404)
        || e.status()
            .is_some_and(|s| s.code == google_cloud_gax::error::rpc::Code::NotFound)
}

impl GcsObjects {
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
        while let Some(item) = items.next().await {
            let object = item.map_err(|e| err("list", prefix, e))?;
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
            .await
        {
            Ok(_) => Ok(true),
            Err(e) if is_not_found(&e) => Ok(false),
            Err(e) => Err(err("get metadata", key, e)),
        }
    }

    async fn download(&self, key: &str, to: &Path) -> Result<(), FaucetError> {
        use tokio::io::AsyncWriteExt as _;
        self.roundtrips.record("get");
        let mut resp = self
            .storage
            .read_object(self.bucket_path(), key.to_string())
            .send()
            .await
            .map_err(|e| err("get", key, e))?;
        let mut file = tokio::fs::File::create(to)
            .await
            .map_err(|e| err("create local copy", key, e))?;
        while let Some(chunk) = resp.next().await {
            let chunk = chunk.map_err(|e| err("download", key, e))?;
            file.write_all(&chunk)
                .await
                .map_err(|e| err("download", key, e))?;
        }
        file.flush().await.map_err(|e| err("download", key, e))
    }

    async fn upload(&self, from: &Path, key: &str) -> Result<(), FaucetError> {
        let file = tokio::fs::File::open(from)
            .await
            .map_err(|e| err("open local file", key, e))?;
        self.roundtrips.record("put");
        self.storage
            .write_object(self.bucket_path(), key.to_string(), file)
            .set_content_type(content_type(key))
            .send_unbuffered()
            .await
            .map_err(|e| err("put object", key, e))?;
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
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if is_not_found(&e) => Ok(()),
            Err(e) => Err(err("delete", key, e)),
        }
    }

    async fn rename(&self, from: &str, to: &str) -> Result<(), FaucetError> {
        let mut token = String::new();
        loop {
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
                .await
                .map_err(|e| err("copy", to, e))?;
            if resp.done {
                break;
            }
            token = resp.rewrite_token;
        }
        self.delete(from).await
    }
}
