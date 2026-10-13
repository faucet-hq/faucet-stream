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
        self.reader.path_for(key)
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

    fn supports_compare_and_put(&self) -> bool {
        true
    }

    /// Atomic across processes sharing the directory: the read-compare-write
    /// runs under an exclusive OS lock on `<entry>.lock` (the lock file
    /// [`FileStateStore`] uses), released by the OS if the writer dies.
    async fn compare_and_put(
        &self,
        key: &str,
        expected: Option<&Value>,
        value: &Value,
    ) -> Result<bool, FaucetError> {
        faucet_core::state::validate_state_key(key)?;
        self.ensure_root().await?;
        let mut lock = self.entry_path(key).into_os_string();
        lock.push(".lock");
        let _held = lock_exclusive(PathBuf::from(lock)).await?;
        if self.get(key).await?.as_ref() != expected {
            return Ok(false);
        }
        self.put(key, value).await?;
        Ok(true)
    }
}

/// How long a writer waits for another process's rotation to finish.
const LOCK_WAIT: std::time::Duration = std::time::Duration::from_secs(30);

/// Hold an exclusive OS lock on `path` until the returned file is dropped.
#[allow(clippy::disallowed_methods)] // lock-file polling, not a backend call
async fn lock_exclusive(path: PathBuf) -> Result<std::fs::File, FaucetError> {
    let mut opts = tokio::fs::OpenOptions::new();
    opts.create(true).truncate(false).write(true);
    #[cfg(unix)]
    opts.mode(0o600);
    let file = opts
        .open(&path)
        .await
        .map_err(|e| io_error("create", &path, e))?
        .into_std()
        .await;
    let started = tokio::time::Instant::now();
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(file),
            Err(std::fs::TryLockError::WouldBlock) if started.elapsed() < LOCK_WAIT => {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            Err(std::fs::TryLockError::WouldBlock) => {
                return Err(FaucetError::State(format!(
                    "'{}' is held by another writer",
                    path.display()
                )));
            }
            Err(std::fs::TryLockError::Error(e)) => return Err(io_error("lock", &path, e)),
        }
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
        let err = store
            .put("bad key", &serde_json::json!(1))
            .await
            .unwrap_err();
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

#[cfg(test)]
mod key_tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn compare_and_put_lets_exactly_one_concurrent_rotation_win() {
        let dir = tempfile::tempdir().unwrap();
        let store = std::sync::Arc::new(PrivateFileStore::new(dir.path().join("auth")));
        assert!(store.supports_compare_and_put());
        store.put("grant", &json!({"rt": "old"})).await.unwrap();
        let tasks: Vec<_> = (0..8)
            .map(|i| {
                let store = store.clone();
                tokio::spawn(async move {
                    store
                        .compare_and_put("grant", Some(&json!({"rt": "old"})), &json!({"rt": i}))
                        .await
                        .unwrap()
                })
            })
            .collect();
        let mut won = 0;
        for t in tasks {
            won += usize::from(t.await.unwrap());
        }
        assert_eq!(won, 1);
        assert!(
            !store
                .compare_and_put("grant", None, &json!({"rt": "x"}))
                .await
                .unwrap()
        );
        assert!(
            store
                .compare_and_put("fresh", None, &json!({"rt": "new"}))
                .await
                .unwrap()
        );
        assert!(
            store
                .compare_and_put("bad key", None, &json!(1))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn a_lock_that_cannot_be_created_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let err = lock_exclusive(dir.path().join("missing").join("x.lock"))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("cannot create"), "{err}");
    }

    #[tokio::test]
    async fn uppercase_and_long_keys_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let store = PrivateFileStore::new(dir.path().join("auth"));
        let long = "k".repeat(200);
        for key in ["CrmProd", long.as_str(), "oauth2_refresh_abc"] {
            store.put(key, &json!({"t": key})).await.unwrap();
            assert_eq!(
                store.get(key).await.unwrap(),
                Some(json!({"t": key})),
                "{key}"
            );
            assert!(store.entry_path(key).exists(), "{key}");
        }
    }
}
