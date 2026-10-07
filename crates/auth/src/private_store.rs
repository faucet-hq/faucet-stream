//! A file-backed [`StateStore`] for credentials: the same on-disk layout as
//! [`FileStateStore`] (one `<key>.json` per key, read through it), but every
//! write lands in a file only its owner can read, inside a directory only its
//! owner can enter when this store creates it (#789 API-20).

use async_trait::async_trait;
use faucet_core::{FaucetError, FileStateStore, StateStore};
use serde_json::Value;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) struct PrivateFileStore {
    root: PathBuf,
    reader: FileStateStore,
}

impl PrivateFileStore {
    pub(crate) fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        Self {
            reader: FileStateStore::new(root.clone()),
            root,
        }
    }

    fn entry_path(&self, key: &str) -> PathBuf {
        self.root
            .join(format!("{}.json", key.replace(':', "%3A").replace('/', "%2F")))
    }

    async fn ensure_root(&self) -> Result<(), FaucetError> {
        if tokio::fs::metadata(&self.root).await.is_ok() {
            return Ok(());
        }
        tokio::fs::create_dir_all(&self.root)
            .await
            .map_err(|e| io_error("create", &self.root, e))?;
        restrict(&self.root, 0o700).await
    }
}

fn io_error(what: &str, path: &std::path::Path, e: std::io::Error) -> FaucetError {
    FaucetError::State(format!("cannot {what} '{}': {e}", path.display()))
}

#[cfg(unix)]
async fn restrict(path: &std::path::Path, mode: u32) -> Result<(), FaucetError> {
    use std::os::unix::fs::PermissionsExt as _;
    tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
        .await
        .map_err(|e| io_error("restrict permissions of", path, e))
}

#[cfg(not(unix))]
async fn restrict(_path: &std::path::Path, _mode: u32) -> Result<(), FaucetError> {
    Ok(())
}

#[async_trait]
impl StateStore for PrivateFileStore {
    async fn get(&self, key: &str) -> Result<Option<Value>, FaucetError> {
        self.reader.get(key).await
    }

    async fn put(&self, key: &str, value: &Value) -> Result<(), FaucetError> {
        use tokio::io::AsyncWriteExt as _;
        faucet_core::state::validate_state_key(key)?;
        self.ensure_root().await?;
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let target = self.entry_path(key);
        let tmp = target.with_extension(format!(
            "{}.{}.tmp",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let bytes = serde_json::to_vec(value)?;
        let mut opts = tokio::fs::OpenOptions::new();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        opts.mode(0o600);
        let written = async {
            let mut file = opts.open(&tmp).await?;
            file.write_all(&bytes).await?;
            file.sync_all().await?;
            tokio::fs::rename(&tmp, &target).await
        }
        .await;
        if let Err(e) = written {
            let _ = tokio::fs::remove_file(&tmp).await;
            return Err(io_error("write", &target, e));
        }
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), FaucetError> {
        self.reader.delete(key).await
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt as _;

    #[tokio::test]
    async fn writes_owner_only_files_in_an_owner_only_directory() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("tokens");
        let store = PrivateFileStore::new(&root);
        store
            .put("oauth2:grant", &serde_json::json!({"refresh_token": "rt"}))
            .await
            .unwrap();
        let mode = |p: &std::path::Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&root), 0o700);
        let file = root.join("oauth2%3Agrant.json");
        assert_eq!(mode(&file), 0o600);
        assert_eq!(
            store.get("oauth2:grant").await.unwrap(),
            Some(serde_json::json!({"refresh_token": "rt"}))
        );
        store
            .put("oauth2:grant", &serde_json::json!({"refresh_token": "rt2"}))
            .await
            .unwrap();
        assert_eq!(mode(&file), 0o600);
        store.delete("oauth2:grant").await.unwrap();
        assert_eq!(store.get("oauth2:grant").await.unwrap(), None);
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 0, "no temp left");
    }

    #[tokio::test]
    async fn a_failed_write_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let blocker = dir.path().join("file");
        std::fs::write(&blocker, b"x").unwrap();
        let store = PrivateFileStore::new(blocker.join("sub"));
        let err = store.put("k", &serde_json::json!(1)).await.unwrap_err();
        assert!(err.to_string().contains("cannot create"), "{err}");
        let err = store.put("bad key", &serde_json::json!(1)).await.unwrap_err();
        assert!(err.to_string().contains("key"), "{err}");

        let ro = dir.path().join("ro");
        std::fs::create_dir(&ro).unwrap();
        std::fs::set_permissions(&ro, std::fs::Permissions::from_mode(0o500)).unwrap();
        let err = PrivateFileStore::new(&ro)
            .put("k", &serde_json::json!(1))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("cannot write"), "{err}");
    }
}
