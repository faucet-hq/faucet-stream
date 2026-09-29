//! The SFTP [`ObjectClient`] behind the shared file writer (#777).
//!
//! A file is uploaded to a hidden temporary name beside its destination and
//! renamed into place, so a reader never sees a partial file. SFTP v3 has no
//! replacing rename, so replacing an existing file removes it first; a new
//! name (the default naming never reuses one) is published atomically.
//!
//! The SSH session is opened lazily on first use, inside the writer's I/O
//! runtime, and reused for the life of the sink.

use async_trait::async_trait;
use faucet_common_file::write::ObjectClient;
use faucet_common_sftp::{SftpConnectionConfig, SftpSession, connect};
use faucet_core::FaucetError;
use russh_sftp::protocol::OpenFlags;
use std::path::Path;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

pub(crate) struct SftpObjects {
    pub connection: SftpConnectionConfig,
    pub session: Mutex<Option<Arc<SftpSession>>>,
}

fn err(what: &str, path: &str, e: impl std::fmt::Display) -> FaucetError {
    FaucetError::Sink(format!("SFTP {what} '{path}' failed: {e}"))
}

/// The directory and file-name prefix of a key prefix.
pub(crate) fn split_prefix(prefix: &str) -> (String, &str) {
    match prefix.rfind('/') {
        Some(0) => ("/".to_string(), &prefix[1..]),
        Some(i) => (prefix[..i].to_string(), &prefix[i + 1..]),
        None => (".".to_string(), prefix),
    }
}

fn parent(path: &str) -> Option<&str> {
    path.rfind('/')
        .map(|i| &path[..i])
        .filter(|p| !p.is_empty())
}

impl SftpObjects {
    pub(crate) fn new(connection: SftpConnectionConfig) -> Self {
        Self {
            connection,
            session: Mutex::new(None),
        }
    }

    async fn session(&self) -> Result<Arc<SftpSession>, FaucetError> {
        let mut guard = self.session.lock().await;
        if let Some(s) = guard.as_ref() {
            return Ok(s.clone());
        }
        let s = Arc::new(connect(&self.connection).await?);
        *guard = Some(s.clone());
        Ok(s)
    }

    /// Create `dir` and its parents, ignoring ones that already exist.
    async fn create_dirs(&self, sftp: &SftpSession, dir: &str) {
        let mut at = String::new();
        for seg in dir.split('/') {
            if seg.is_empty() {
                at.push('/');
                continue;
            }
            if !at.is_empty() && !at.ends_with('/') {
                at.push('/');
            }
            at.push_str(seg);
            if !matches!(sftp.try_exists(at.as_str()).await, Ok(true)) {
                let _ = sftp.create_dir(at.as_str()).await;
            }
        }
    }
}

#[async_trait]
impl ObjectClient for SftpObjects {
    fn describe(&self, key: &str) -> String {
        format!(
            "sftp://{}:{}/{}",
            self.connection.host,
            self.connection.port,
            key.trim_start_matches('/')
        )
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, FaucetError> {
        let sftp = self.session().await?;
        let (dir, start) = split_prefix(prefix);
        if !sftp.try_exists(dir.as_str()).await.unwrap_or(false) {
            return Ok(Vec::new());
        }
        let entries = sftp
            .read_dir(dir.as_str())
            .await
            .map_err(|e| err("list", &dir, e))?;
        let base = &prefix[..prefix.len() - start.len()];
        Ok(entries
            .filter(|e| e.file_type().is_file())
            .map(|e| e.file_name())
            .filter(|n| n.starts_with(start))
            .map(|n| format!("{base}{n}"))
            .collect())
    }

    async fn exists(&self, key: &str) -> Result<bool, FaucetError> {
        let sftp = self.session().await?;
        sftp.try_exists(key).await.map_err(|e| err("stat", key, e))
    }

    async fn download(&self, key: &str, to: &Path) -> Result<(), FaucetError> {
        let sftp = self.session().await?;
        let mut remote = sftp.open(key).await.map_err(|e| err("open", key, e))?;
        let mut local = tokio::fs::File::create(to)
            .await
            .map_err(|e| err("create local copy of", key, e))?;
        tokio::io::copy(&mut remote, &mut local)
            .await
            .map_err(|e| err("download", key, e))?;
        Ok(())
    }

    async fn upload(&self, from: &Path, key: &str) -> Result<(), FaucetError> {
        let sftp = self.session().await?;
        if let Some(dir) = parent(key) {
            self.create_dirs(&sftp, dir).await;
        }
        let tmp = format!("{key}.faucet-tmp-{}", uuid::Uuid::new_v4().simple());
        let result = async {
            let mut local = tokio::fs::File::open(from)
                .await
                .map_err(|e| err("open local file for", key, e))?;
            let mut remote = sftp
                .open_with_flags(
                    tmp.as_str(),
                    OpenFlags::CREATE | OpenFlags::WRITE | OpenFlags::TRUNCATE,
                )
                .await
                .map_err(|e| err("open for write", &tmp, e))?;
            tokio::io::copy(&mut local, &mut remote)
                .await
                .map_err(|e| err("write", &tmp, e))?;
            remote.flush().await.map_err(|e| err("flush", &tmp, e))?;
            remote.shutdown().await.map_err(|e| err("close", &tmp, e))?;
            if sftp.try_exists(key).await.unwrap_or(false) {
                sftp.remove_file(key)
                    .await
                    .map_err(|e| err("replace", key, e))?;
            }
            sftp.rename(tmp.as_str(), key)
                .await
                .map_err(|e| err("rename into place", key, e))
        }
        .await;
        if result.is_err() {
            let _ = sftp.remove_file(tmp.as_str()).await;
        }
        result
    }

    async fn delete(&self, key: &str) -> Result<(), FaucetError> {
        let sftp = self.session().await?;
        if !sftp.try_exists(key).await.unwrap_or(false) {
            return Ok(());
        }
        sftp.remove_file(key)
            .await
            .map_err(|e| err("delete", key, e))
    }

    async fn rename(&self, from: &str, to: &str) -> Result<(), FaucetError> {
        let sftp = self.session().await?;
        if let Some(dir) = parent(to) {
            self.create_dirs(&sftp, dir).await;
        }
        if sftp.try_exists(to).await.unwrap_or(false) {
            sftp.remove_file(to)
                .await
                .map_err(|e| err("replace", to, e))?;
        }
        sftp.rename(from, to)
            .await
            .map_err(|e| err("rename", to, e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefixes_split_into_directory_and_name_start() {
        assert_eq!(split_prefix("/out/"), ("/out".to_string(), ""));
        assert_eq!(split_prefix("/out/run-"), ("/out".to_string(), "run-"));
        assert_eq!(split_prefix("/x"), ("/".to_string(), "x"));
        assert_eq!(split_prefix("rel"), (".".to_string(), "rel"));
        assert_eq!(parent("/a/b/c"), Some("/a/b"));
        assert_eq!(parent("c"), None);
        let o = SftpObjects::new(SftpConnectionConfig::with_password("h", "u", "p"));
        assert_eq!(o.describe("/out/a.jsonl"), "sftp://h:22/out/a.jsonl");
    }
}
