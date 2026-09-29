//! The Azure Blob [`ObjectClient`] behind the shared file writer (#777).
//!
//! A blob up to [`PART_BYTES`] is one `Put Blob`; a larger one is uploaded
//! as blocks (up to `concurrency` in flight) and committed with one
//! `Put Block List`, which is what makes it visible — a failed upload is
//! aborted and leaves nothing behind.

use async_trait::async_trait;
use faucet_common_file::write::ObjectClient;
use faucet_core::FaucetError;
use futures::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use std::path::Path;
use std::sync::Arc;

/// Block size for large blobs; also the single-`put` limit.
pub const PART_BYTES: usize = 8 * 1024 * 1024;

pub(crate) struct AzureObjects {
    pub store: Arc<dyn ObjectStore>,
    pub container: String,
    pub part_bytes: usize,
    pub concurrency: usize,
}

fn err(what: &str, key: &str, e: impl std::fmt::Display) -> FaucetError {
    FaucetError::Sink(format!("azure {what} error for key '{key}': {e}"))
}

/// The directory part of a key prefix: `object_store` lists whole path
/// segments, so `a/b-` lists `a/` and is filtered client-side.
pub(crate) fn list_root(prefix: &str) -> Option<ObjectPath> {
    prefix
        .rfind('/')
        .map(|i| ObjectPath::from(&prefix[..i]))
        .filter(|p| !p.as_ref().is_empty())
}

#[async_trait]
impl ObjectClient for AzureObjects {
    fn describe(&self, key: &str) -> String {
        format!("az://{}/{key}", self.container)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, FaucetError> {
        let root = list_root(prefix);
        let mut listing = self.store.list(root.as_ref());
        let mut keys = Vec::new();
        while let Some(meta) = listing.next().await {
            let meta = meta.map_err(|e| err("list", prefix, e))?;
            let key = meta.location.to_string();
            if key.starts_with(prefix) {
                keys.push(key);
            }
        }
        Ok(keys)
    }

    async fn exists(&self, key: &str) -> Result<bool, FaucetError> {
        match self.store.head(&ObjectPath::from(key)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(err("head", key, e)),
        }
    }

    async fn download(&self, key: &str, to: &Path) -> Result<(), FaucetError> {
        use tokio::io::AsyncWriteExt as _;
        let got = self
            .store
            .get(&ObjectPath::from(key))
            .await
            .map_err(|e| err("get", key, e))?;
        let mut stream = got.into_stream();
        let mut file = tokio::fs::File::create(to)
            .await
            .map_err(|e| err("create local copy", key, e))?;
        while let Some(chunk) = stream.next().await {
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
            self.store
                .put(&path, bytes::Bytes::from(body).into())
                .await
                .map_err(|e| err("put object", key, e))?;
            return Ok(());
        }
        let mut file = tokio::fs::File::open(from)
            .await
            .map_err(|e| err("open local file", key, e))?;
        let upload = self
            .store
            .put_multipart(&path)
            .await
            .map_err(|e| err("start multipart", key, e))?;
        let mut writer = object_store::WriteMultipart::new_with_chunk_size(upload, self.part_bytes);
        let mut result = Ok(());
        let mut sent = 0;
        while sent < len {
            if let Err(e) = writer.wait_for_capacity(self.concurrency.max(1)).await {
                result = Err(err("put part", key, e));
                break;
            }
            let mut part = vec![0u8; self.part_bytes.min(len - sent)];
            if let Err(e) = file.read_exact(&mut part).await {
                result = Err(err("read local file", key, e));
                break;
            }
            sent += part.len();
            writer.put(bytes::Bytes::from(part));
        }
        if result.is_ok() {
            return match writer.finish().await {
                Ok(_) => Ok(()),
                Err(e) => Err(err("complete multipart", key, e)),
            };
        }
        let _ = writer.abort().await;
        result
    }

    async fn delete(&self, key: &str) -> Result<(), FaucetError> {
        match self.store.delete(&ObjectPath::from(key)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(err("delete", key, e)),
        }
    }

    async fn rename(&self, from: &str, to: &str) -> Result<(), FaucetError> {
        self.store
            .copy(&ObjectPath::from(from), &ObjectPath::from(to))
            .await
            .map_err(|e| err("copy", to, e))?;
        self.delete(from).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            container: "c".into(),
            part_bytes: 4,
            concurrency: 2,
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

    fn objects(store: Arc<dyn ObjectStore>, part_bytes: usize) -> AzureObjects {
        AzureObjects {
            store,
            container: "c".into(),
            part_bytes,
            concurrency: 1,
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
