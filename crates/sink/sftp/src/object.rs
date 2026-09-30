//! The SFTP [`ObjectClient`] behind the shared file writer (#777).
//!
//! A file is uploaded to a hidden temporary name beside its destination and
//! renamed into place, so a reader never sees a partial file. A server with
//! `posix-rename@openssh.com` replaces an existing file atomically; plain
//! SFTP v3 has no replacing rename, so there an existing file is removed
//! first. A new name (the default naming never reuses one) is published
//! atomically either way.
//!
//! The SSH session is opened lazily on first use and reused for the life of
//! the sink. Only the server's typed "no such file" status means "missing";
//! every other stat or remove failure is an error (#783).

use async_trait::async_trait;
use faucet_common_file::write::ObjectClient;
use faucet_common_sftp::{
    SftpConnection, SftpConnectionConfig, SftpError, connect_with_extensions, is_no_such_file,
};
use faucet_core::FaucetError;
use russh_sftp::protocol::OpenFlags;
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

/// Copy buffer for uploads and downloads. Larger than any server's
/// `max_write_len` (the protocol default packet is 256 KiB), so each `WRITE`
/// carries a full packet instead of an 8 KiB slice.
const COPY_BUF: usize = 1024 * 1024;

pub(crate) struct SftpObjects {
    pub connection: SftpConnectionConfig,
    session: Mutex<Option<Arc<SftpConnection>>>,
    /// Directories known to exist, so an upload does not stat its parents
    /// again.
    dirs: Mutex<HashSet<String>>,
    /// Use `posix-rename` when the server has it (off only in tests, to
    /// cover the plain-`RENAME` fallback against a server that has it).
    posix_rename: bool,
}

fn err(what: &str, path: &str, e: impl std::fmt::Display) -> FaucetError {
    FaucetError::Sink(format!("SFTP {what} '{path}' failed: {e}"))
}

/// `Ok(false)` for the server's typed "no such file", the error otherwise.
fn missing(what: &str, path: &str, e: SftpError) -> Result<bool, FaucetError> {
    if is_no_such_file(&e) {
        Ok(false)
    } else {
        Err(err(what, path, e))
    }
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

/// Every ancestor of `dir`, outermost first, ending with `dir` itself.
fn ancestors(dir: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut at = String::new();
    for seg in dir.split('/') {
        if seg.is_empty() {
            if at.is_empty() {
                at.push('/');
            }
            continue;
        }
        if !at.is_empty() && !at.ends_with('/') {
            at.push('/');
        }
        at.push_str(seg);
        out.push(at.clone());
    }
    out
}

impl SftpObjects {
    pub(crate) fn new(connection: SftpConnectionConfig) -> Self {
        Self {
            connection,
            session: Mutex::new(None),
            dirs: Mutex::new(HashSet::new()),
            posix_rename: true,
        }
    }

    async fn connection(&self) -> Result<Arc<SftpConnection>, FaucetError> {
        let mut guard = self.session.lock().await;
        if let Some(s) = guard.as_ref() {
            return Ok(s.clone());
        }
        let s = Arc::new(connect_with_extensions(&self.connection).await?);
        *guard = Some(s.clone());
        Ok(s)
    }

    /// Whether `path` is an existing directory; `false` when it is missing.
    async fn is_dir(conn: &SftpConnection, path: &str) -> Result<bool, FaucetError> {
        match conn.session().metadata(path).await {
            Ok(m) => Ok(m.file_type().is_dir()),
            Err(e) => missing("stat", path, e),
        }
    }

    /// Create `dir` and its parents. A directory created or seen once is
    /// cached, so later uploads into it cost no round trip.
    async fn create_dirs(&self, conn: &SftpConnection, dir: &str) -> Result<(), FaucetError> {
        if self.dirs.lock().await.contains(dir) {
            return Ok(());
        }
        if !Self::is_dir(conn, dir).await? {
            for at in ancestors(dir) {
                if self.dirs.lock().await.contains(&at) {
                    continue;
                }
                if let Err(e) = conn.session().create_dir(at.as_str()).await
                    && !Self::is_dir(conn, &at).await?
                {
                    return Err(err("create directory", &at, e));
                }
                self.dirs.lock().await.insert(at);
            }
        }
        self.dirs.lock().await.insert(dir.to_string());
        Ok(())
    }

    /// Move `from` over `to`: one atomic `posix-rename` when the server has
    /// it, else remove `to` (a missing one is fine) and `RENAME`.
    async fn replace(&self, conn: &SftpConnection, from: &str, to: &str) -> Result<(), FaucetError> {
        if self.posix_rename && conn.supports_posix_rename() {
            return conn
                .posix_rename(from, to)
                .await
                .map_err(|e| err("replace", to, e));
        }
        if let Err(e) = conn.session().remove_file(to).await {
            missing("replace", to, e)?;
        }
        conn.session()
            .rename(from, to)
            .await
            .map_err(|e| err("rename into place", to, e))
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
        let conn = self.connection().await?;
        let (dir, start) = split_prefix(prefix);
        let entries = match conn.session().read_dir(dir.as_str()).await {
            Ok(entries) => entries,
            Err(e) => {
                missing("list", &dir, e)?;
                return Ok(Vec::new());
            }
        };
        let base = &prefix[..prefix.len() - start.len()];
        Ok(entries
            .filter(|e| e.file_type().is_file())
            .map(|e| e.file_name())
            .filter(|n| n.starts_with(start))
            .map(|n| format!("{base}{n}"))
            .collect())
    }

    async fn exists(&self, key: &str) -> Result<bool, FaucetError> {
        let conn = self.connection().await?;
        conn.session()
            .try_exists(key)
            .await
            .map_err(|e| err("stat", key, e))
    }

    async fn download(&self, key: &str, to: &Path) -> Result<(), FaucetError> {
        let conn = self.connection().await?;
        let remote = conn
            .session()
            .open(key)
            .await
            .map_err(|e| err("open", key, e))?;
        let mut local = tokio::fs::File::create(to)
            .await
            .map_err(|e| err("create local copy of", key, e))?;
        let mut remote = tokio::io::BufReader::with_capacity(COPY_BUF, remote);
        tokio::io::copy_buf(&mut remote, &mut local)
            .await
            .map_err(|e| err("download", key, e))?;
        local.flush().await.map_err(|e| err("download", key, e))
    }

    async fn upload(&self, from: &Path, key: &str) -> Result<(), FaucetError> {
        let conn = self.connection().await?;
        if let Some(dir) = parent(key) {
            self.create_dirs(&conn, dir).await?;
        }
        let tmp = format!("{key}.faucet-tmp-{}", uuid::Uuid::new_v4().simple());
        let sftp = conn.session();
        let result = async {
            let local = tokio::fs::File::open(from)
                .await
                .map_err(|e| err("open local file for", key, e))?;
            let mut local = tokio::io::BufReader::with_capacity(COPY_BUF, local);
            let mut remote = sftp
                .open_with_flags(
                    tmp.as_str(),
                    OpenFlags::CREATE | OpenFlags::WRITE | OpenFlags::TRUNCATE,
                )
                .await
                .map_err(|e| err("open for write", &tmp, e))?;
            tokio::io::copy_buf(&mut local, &mut remote)
                .await
                .map_err(|e| err("write", &tmp, e))?;
            remote.flush().await.map_err(|e| err("flush", &tmp, e))?;
            remote.shutdown().await.map_err(|e| err("close", &tmp, e))?;
            self.replace(&conn, &tmp, key).await
        }
        .await;
        if result.is_err() {
            let _ = sftp.remove_file(tmp.as_str()).await;
        }
        result
    }

    async fn delete(&self, key: &str) -> Result<(), FaucetError> {
        let conn = self.connection().await?;
        match conn.session().remove_file(key).await {
            Ok(()) => Ok(()),
            Err(e) => missing("delete", key, e).map(|_| ()),
        }
    }

    async fn rename(&self, from: &str, to: &str) -> Result<(), FaucetError> {
        let conn = self.connection().await?;
        if let Some(dir) = parent(to) {
            self.create_dirs(&conn, dir).await?;
        }
        self.replace(&conn, from, to).await
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
        assert_eq!(ancestors("/a/b"), ["/a", "/a/b"]);
        assert_eq!(ancestors("a/b"), ["a", "a/b"]);
        assert_eq!(ancestors("/"), Vec::<String>::new());
        let o = SftpObjects::new(SftpConnectionConfig::with_password("h", "u", "p"));
        assert_eq!(o.describe("/out/a.jsonl"), "sftp://h:22/out/a.jsonl");
    }

    fn status(code: faucet_common_sftp::StatusCode) -> SftpError {
        SftpError::Status(russh_sftp::protocol::Status {
            id: 1,
            status_code: code,
            error_message: "m".into(),
            language_tag: "en".into(),
        })
    }

    #[test]
    fn only_no_such_file_means_missing() {
        use faucet_common_sftp::StatusCode;
        assert!(!missing("stat", "/p", status(StatusCode::NoSuchFile)).unwrap());
        let e = missing("stat", "/p", status(StatusCode::PermissionDenied))
            .unwrap_err()
            .to_string();
        assert!(e.contains("SFTP stat '/p' failed"), "{e}");
        assert!(missing("stat", "/p", status(StatusCode::Failure)).is_err());
        assert!(missing("stat", "/p", SftpError::Timeout).is_err());
    }

    async fn server() -> Option<(
        testcontainers::ContainerAsync<testcontainers::GenericImage>,
        u16,
    )> {
        use testcontainers::ImageExt;
        use testcontainers::core::{IntoContainerPort, WaitFor};
        use testcontainers::runners::AsyncRunner;
        let image = testcontainers::GenericImage::new("atmoz/sftp", "alpine")
            .with_exposed_port(22.tcp())
            .with_wait_for(WaitFor::message_on_stderr("Server listening on"))
            .with_cmd(vec!["faucet:secret:::data".to_string()]);
        match image.start().await {
            Ok(c) => {
                let port = c.get_host_port_ipv4(22).await.expect("port");
                Some((c, port))
            }
            Err(e) => {
                eprintln!("Skipping: Docker not available ({e})");
                None
            }
        }
    }

    /// C3 (#783): a stat the server refuses (here: a directory the user cannot
    /// traverse) is an error, not "missing" — a listing that silently came
    /// back empty let an overwrite commit promote nothing and prune the
    /// destination, and a delete that silently did nothing left files behind.
    #[tokio::test(flavor = "multi_thread")]
    async fn refused_stats_are_errors_not_missing_files() {
        let Some((_c, port)) = server().await else {
            return;
        };
        let cfg = SftpConnectionConfig::with_password("127.0.0.1", "faucet", "secret").port(port);
        let o = SftpObjects::new(cfg);
        let conn = o.connection().await.unwrap();
        let sftp = conn.session();
        sftp.create_dir("/data/locked").await.unwrap();
        sftp.create_dir("/data/locked/sub").await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("f");
        std::fs::write(&src, b"x\n").unwrap();
        o.upload(&src, "/data/locked/sub/f").await.unwrap();
        assert_eq!(o.list("/data/locked/sub/").await.unwrap(), ["/data/locked/sub/f"]);
        assert!(o.list("/data/nope/").await.unwrap().is_empty());
        o.delete("/data/nope/f").await.unwrap();

        let mut attrs = russh_sftp::protocol::FileAttributes::empty();
        attrs.permissions = Some(0o040000);
        sftp.set_metadata("/data/locked", attrs).await.unwrap();

        let e = o.list("/data/locked/sub/").await.unwrap_err().to_string();
        assert!(e.contains("SFTP list '/data/locked/sub' failed"), "{e}");
        let e = o.delete("/data/locked/sub/f").await.unwrap_err().to_string();
        assert!(e.contains("SFTP delete '/data/locked/sub/f' failed"), "{e}");
        assert!(o.exists("/data/locked/sub/f").await.is_err());
        let e = o
            .upload(&src, "/data/locked/sub/new/g")
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("/data/locked/sub"), "{e}");

        let mut attrs = russh_sftp::protocol::FileAttributes::empty();
        attrs.permissions = Some(0o040755);
        sftp.set_metadata("/data/locked", attrs).await.unwrap();
        o.upload(&src, "/data/locked/sub/f").await.unwrap();
        o.rename("/data/locked/sub/f", "/data/moved/deep/f").await.unwrap();
        assert!(o.exists("/data/moved/deep/f").await.unwrap());
        assert!(!o.exists("/data/locked/sub/f").await.unwrap());
        o.upload(&src, "/data/moved/deep/g").await.unwrap();
        o.rename("/data/moved/deep/g", "/data/moved/deep/f").await.unwrap();
        let out = dir.path().join("out");
        o.download("/data/moved/deep/f", &out).await.unwrap();
        assert_eq!(std::fs::read(&out).unwrap(), b"x\n");
        assert!(conn.supports_posix_rename());

        let mut plain = SftpObjects::new(o.connection.clone());
        plain.posix_rename = false;
        std::fs::write(&src, b"y\n").unwrap();
        plain.upload(&src, "/data/moved/deep/f").await.unwrap();
        plain.upload(&src, "/data/moved/deep/h").await.unwrap();
        plain.rename("/data/moved/deep/h", "/data/moved/deep/f").await.unwrap();
        plain.download("/data/moved/deep/f", &out).await.unwrap();
        assert_eq!(std::fs::read(&out).unwrap(), b"y\n");
        let e = plain
            .rename("/data/moved/deep/none", "/data/moved/deep/f")
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("rename into place"), "{e}");
    }
}
