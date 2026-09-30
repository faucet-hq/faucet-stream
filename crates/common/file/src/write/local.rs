//! The local-filesystem backend: scratch files beside the destination,
//! fsync, and an atomic rename into place. File-system calls run on Tokio's
//! blocking pool, so a hung disk never blocks an async worker and dropping
//! the caller's future stops waiting on it.

use super::backend::{Area, StorageBackend};
use super::layout::{NameTemplate, TMP_SUFFIX, io_err, tmp_path};
use faucet_core::FaucetError;
use std::path::{Path, PathBuf};

/// Files in one local directory; overwrite runs keep their files in a hidden
/// swap directory inside it.
///
/// **Experimental** (PRINCIPLES.md §3): this block's shape may change in a
/// minor release; any change is called out in the changelog.
#[derive(Debug, Clone)]
pub struct LocalBackend {
    dir: PathBuf,
    swap: PathBuf,
    template: NameTemplate,
    create_dirs: bool,
}

impl LocalBackend {
    /// Write `template`'s files into `dir` (`""` = the working directory),
    /// keeping overwrite runs in `dir/<template.swap_dir_name()>`. With
    /// `create_dirs` false a missing `dir` is an error rather than created.
    pub fn new(dir: &str, template: &NameTemplate, create_dirs: bool) -> Self {
        let dir = if dir.is_empty() {
            PathBuf::from(".")
        } else {
            PathBuf::from(dir)
        };
        Self {
            swap: dir.join(template.swap_dir_name()),
            dir,
            template: template.clone(),
            create_dirs,
        }
    }

    /// The destination directory.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The overwrite swap directory.
    pub fn swap_dir(&self) -> &Path {
        &self.swap
    }

    fn area(&self, area: Area) -> &Path {
        match area {
            Area::Destination => &self.dir,
            Area::Swap => &self.swap,
        }
    }

    fn path(&self, area: Area, name: &str) -> PathBuf {
        self.area(area).join(name)
    }
}

/// Run `f` on the blocking pool.
async fn fs<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, FaucetError> + Send + 'static,
) -> Result<T, FaucetError> {
    tokio::task::spawn_blocking(f).await.map_err(|e| {
        FaucetError::Sink(format!("file sink: a file-system task did not finish: {e}"))
    })?
}

/// Fsync `dir`, so a rename or delete in it survives a crash. Best effort; a
/// no-op off Unix.
fn sync_directory(dir: &Path) {
    #[cfg(unix)]
    if let Ok(d) = std::fs::File::open(if dir.as_os_str().is_empty() {
        Path::new(".")
    } else {
        dir
    }) {
        let _ = d.sync_all();
    }
    #[cfg(not(unix))]
    let _ = dir;
}

/// Fsync the directory holding `path`.
fn sync_parent(path: &Path) {
    if let Some(dir) = path.parent() {
        sync_directory(dir);
    }
}

/// Remove the scratch files of `template` a crashed run left in `dir`.
fn remove_stale_scratch(dir: &Path, template: &NameTemplate) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        if entry
            .file_name()
            .to_str()
            .is_some_and(|n| template.owns_scratch(n))
        {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

#[faucet_core::async_trait]
impl StorageBackend for LocalBackend {
    fn describe(&self, area: Area, name: &str) -> String {
        self.path(area, name).display().to_string()
    }

    fn local_path(&self, area: Area, name: &str) -> Option<PathBuf> {
        Some(self.path(area, name))
    }

    fn scratch_path(&self, area: Area, name: &str) -> PathBuf {
        tmp_path(&self.path(area, name), TMP_SUFFIX)
    }

    async fn prepare(&self, area: Area) -> Result<(), FaucetError> {
        let dir = self.area(area).to_path_buf();
        let template = self.template.clone();
        let create = area == Area::Swap || self.create_dirs;
        fs(move || {
            if dir.is_dir() {
                remove_stale_scratch(&dir, &template);
                return Ok(());
            }
            if !create {
                return Err(FaucetError::Sink(format!(
                    "file sink: directory '{}' does not exist and `create_dirs` is off",
                    dir.display()
                )));
            }
            std::fs::create_dir_all(&dir).map_err(|e| io_err("creating directory", &dir, e))
        })
        .await
    }

    async fn list(&self, area: Area) -> Result<Vec<String>, FaucetError> {
        let dir = self.area(area).to_path_buf();
        fs(move || {
            let entries = match std::fs::read_dir(&dir) {
                Ok(e) => e,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
                Err(e) => return Err(io_err("listing", &dir, e)),
            };
            let mut out = Vec::new();
            for entry in entries {
                let entry = entry.map_err(|e| io_err("listing", &dir, e))?;
                let is_file = entry
                    .file_type()
                    .map_err(|e| io_err("listing", &entry.path(), e))?
                    .is_file();
                if is_file && let Some(name) = entry.file_name().to_str() {
                    out.push(name.to_string());
                }
            }
            Ok(out)
        })
        .await
    }

    async fn exists(&self, area: Area, name: &str) -> Result<bool, FaucetError> {
        let path = self.path(area, name);
        fs(move || path.try_exists().map_err(|e| io_err("checking", &path, e))).await
    }

    async fn fetch(&self, area: Area, name: &str, to: &Path) -> Result<(), FaucetError> {
        let from = self.path(area, name);
        let to = to.to_path_buf();
        fs(move || {
            std::fs::copy(&from, &to)
                .map(|_| ())
                .map_err(|e| io_err("copying", &from, e))
        })
        .await
    }

    async fn publish(&self, scratch: &Path, area: Area, name: &str) -> Result<(), FaucetError> {
        let dest = self.path(area, name);
        let scratch = scratch.to_path_buf();
        fs(move || {
            std::fs::File::open(&scratch)
                .and_then(|f| f.sync_all())
                .map_err(|e| io_err("syncing", &scratch, e))?;
            std::fs::rename(&scratch, &dest)
                .map_err(|e| io_err("renaming into place", &dest, e))?;
            sync_parent(&dest);
            Ok(())
        })
        .await
    }

    async fn delete(&self, area: Area, name: &str) -> Result<(), FaucetError> {
        let path = self.path(area, name);
        let swap = (area == Area::Swap).then(|| self.swap.clone());
        fs(move || {
            match std::fs::remove_file(&path) {
                Ok(()) => sync_parent(&path),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(io_err("removing earlier output", &path, e)),
            }
            if let Some(dir) = swap
                && std::fs::remove_dir(&dir).is_ok()
            {
                sync_parent(&dir);
            }
            Ok(())
        })
        .await
    }

    async fn promote(&self, name: &str) -> Result<(), FaucetError> {
        let from = self.path(Area::Swap, name);
        let dest = self.path(Area::Destination, name);
        fs(move || {
            std::fs::rename(&from, &dest).map_err(|e| io_err("moving into place", &dest, e))?;
            sync_parent(&dest);
            Ok(())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn template() -> NameTemplate {
        NameTemplate {
            name: "f-{part}".into(),
        }
    }

    fn backend(dir: &Path, create: bool) -> LocalBackend {
        LocalBackend::new(&dir.to_string_lossy(), &template(), create)
    }

    #[tokio::test]
    async fn lists_files_only_and_tolerates_a_missing_directory() {
        let dir = tempfile::tempdir().unwrap();
        let b = backend(dir.path(), false);
        assert!(b.list(Area::Swap).await.unwrap().is_empty());
        std::fs::write(dir.path().join("a"), b"").unwrap();
        std::fs::create_dir(dir.path().join("sub")).unwrap();
        assert_eq!(b.list(Area::Destination).await.unwrap(), vec!["a"]);
        let file = backend(&dir.path().join("a"), false);
        let e = file.list(Area::Destination).await.unwrap_err();
        assert!(e.to_string().contains("listing"), "{e}");
        assert!(b.exists(Area::Destination, "a").await.unwrap());
        assert_eq!(
            b.local_path(Area::Destination, "a"),
            Some(dir.path().join("a"))
        );
        assert!(b.describe(Area::Swap, "x").contains(".faucet-overwrite-"));
        assert_eq!(
            LocalBackend::new("", &template(), true).dir(),
            Path::new(".")
        );
    }

    #[tokio::test]
    async fn prepare_respects_create_dirs_and_clears_only_our_scratch() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("m");
        let strict = backend(&missing, false);
        let e = strict.prepare(Area::Destination).await.unwrap_err();
        assert!(e.to_string().contains("create_dirs"), "{e}");
        strict.prepare(Area::Swap).await.unwrap();
        assert!(strict.swap_dir().is_dir());
        let lax = backend(&dir.path().join("n"), true);
        lax.prepare(Area::Destination).await.unwrap();
        for n in [
            "f-00001.faucet-tmp",
            "f-00002.faucet-tmp-old",
            "g.faucet-tmp",
        ] {
            std::fs::write(dir.path().join("n").join(n), b"").unwrap();
        }
        lax.prepare(Area::Destination).await.unwrap();
        let mut left = lax.list(Area::Destination).await.unwrap();
        left.sort();
        assert_eq!(left, ["g.faucet-tmp"]);
    }

    #[tokio::test]
    async fn publish_fetch_delete_and_promote() {
        let dir = tempfile::tempdir().unwrap();
        let b = backend(dir.path(), true);
        let scratch = b.scratch_path(Area::Destination, "f");
        assert!(scratch.ends_with("f.faucet-tmp"));
        std::fs::write(&scratch, b"one").unwrap();
        b.publish(&scratch, Area::Destination, "f").await.unwrap();
        let copy = dir.path().join("copy");
        b.fetch(Area::Destination, "f", &copy).await.unwrap();
        assert_eq!(std::fs::read(&copy).unwrap(), b"one");
        assert!(b.fetch(Area::Destination, "nope", &copy).await.is_err());
        let gone = b.scratch_path(Area::Destination, "gone");
        assert!(b.publish(&gone, Area::Destination, "gone").await.is_err());
        b.delete(Area::Destination, "f").await.unwrap();
        b.delete(Area::Destination, "f").await.unwrap();

        b.prepare(Area::Swap).await.unwrap();
        std::fs::write(b.swap_dir().join("g"), b"two").unwrap();
        std::fs::write(b.swap_dir().join("h"), b"three").unwrap();
        b.promote("g").await.unwrap();
        assert!(b.promote("g").await.is_err());
        assert_eq!(std::fs::read(dir.path().join("g")).unwrap(), b"two");
        b.delete(Area::Swap, "h").await.unwrap();
        assert!(!b.swap_dir().exists(), "the empty swap area is removed");
        assert!(!b.exists(Area::Swap, "h").await.unwrap());
    }

    #[tokio::test]
    async fn deleting_a_directory_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("d")).unwrap();
        let b = backend(dir.path(), false);
        let e = b.delete(Area::Destination, "d").await.unwrap_err();
        assert!(e.to_string().contains("removing earlier output"), "{e}");
        assert!(dir.path().join("d").is_dir());
        sync_parent(Path::new("no-directory-part"));
        sync_directory(Path::new(""));
    }
}
