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
    /// The joined overwrite run's own swap directory (see
    /// [`StorageBackend::join_overwrite_run`]).
    run_swap: std::sync::OnceLock<PathBuf>,
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
            run_swap: std::sync::OnceLock::new(),
            dir,
            template: template.clone(),
            create_dirs,
        }
    }

    /// The destination directory.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The overwrite swap directory (the joined run's own, once joined).
    pub fn swap_dir(&self) -> &Path {
        self.run_swap.get().unwrap_or(&self.swap)
    }

    fn area(&self, area: Area) -> &Path {
        match area {
            Area::Destination => &self.dir,
            Area::Swap => self.swap_dir(),
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

/// Write `bytes` to a new file at `path`, fsynced. Whatever is at `path`
/// already (a stale file or a link) is removed, never followed.
pub(crate) fn write_fresh(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write as _;
    match std::fs::remove_file(path) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e),
        _ => {}
    }
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    f.write_all(bytes)?;
    f.sync_all()
}

/// Refuse a directory that is a link: side files are only written in real
/// directories of the output.
fn refuse_linked_dir(dir: &Path) -> Result<(), FaucetError> {
    match std::fs::symlink_metadata(dir) {
        Ok(m) if m.file_type().is_symlink() => Err(FaucetError::Sink(format!(
            "file sink: '{}' is a link, not a directory",
            dir.display()
        ))),
        _ => Ok(()),
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
            faucet_core::path_guard::check(&dir)?;
            if area == Area::Swap {
                refuse_linked_dir(&dir)?;
            }
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
            faucet_core::path_guard::check(&dest)?;
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
        let swap = (area == Area::Swap).then(|| self.swap_dir().to_path_buf());
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

    fn join_overwrite_run(&self, token: &str) -> Result<(), FaucetError> {
        faucet_core::idempotency::check_overwrite_run_token(token)?;
        let dir = self.dir.join(self.template.swap_run_dir_name(Some(token)));
        if self.run_swap.get_or_init(|| dir.clone()) != &dir {
            return Err(FaucetError::Config(
                "file sink: already joined another overwrite run".into(),
            ));
        }
        Ok(())
    }

    async fn swap_runs(&self) -> Result<Vec<String>, FaucetError> {
        let dir = self.dir.clone();
        let template = self.template.clone();
        fs(move || {
            let entries = match std::fs::read_dir(&dir) {
                Ok(e) => e,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
                Err(e) => return Err(io_err("listing", &dir, e)),
            };
            Ok(entries
                .flatten()
                .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
                .filter_map(|e| {
                    e.file_name()
                        .to_str()
                        .and_then(|n| template.swap_run_token(n))
                })
                .collect())
        })
        .await
    }

    async fn drop_swap_run(&self, run: Option<&str>) -> Result<(), FaucetError> {
        let dir = self.dir.join(self.template.swap_run_dir_name(run));
        fs(move || match std::fs::remove_dir_all(&dir) {
            Ok(()) => {
                sync_parent(&dir);
                Ok(())
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(io_err("removing an overwrite swap area", &dir, e)),
        })
        .await
    }

    async fn read_swap_run_marker(
        &self,
        run: Option<&str>,
        name: &str,
    ) -> Result<Option<String>, FaucetError> {
        let path = self
            .dir
            .join(self.template.swap_run_dir_name(run))
            .join(name);
        fs(move || match std::fs::read_to_string(&path) {
            Ok(text) => Ok(Some(text)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(io_err("reading the overwrite marker", &path, e)),
        })
        .await
    }

    async fn list_swap_run(&self, run: Option<&str>) -> Result<Vec<String>, FaucetError> {
        let view = Self {
            run_swap: std::sync::OnceLock::from(
                self.dir.join(self.template.swap_run_dir_name(run)),
            ),
            ..self.clone()
        };
        view.list(Area::Swap).await
    }

    async fn promote_from_swap_run(
        &self,
        run: Option<&str>,
        name: &str,
    ) -> Result<(), FaucetError> {
        let from = self
            .dir
            .join(self.template.swap_run_dir_name(run))
            .join(name);
        let dest = self.path(Area::Destination, name);
        fs(move || {
            faucet_core::path_guard::check(&dest)?;
            match std::fs::rename(&from, &dest) {
                Ok(()) => {
                    sync_parent(&dest);
                    Ok(())
                }
                // Another begin finished this move first.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound && dest.exists() => Ok(()),
                Err(e) => Err(io_err("moving into place", &dest, e)),
            }
        })
        .await
    }

    async fn read_commit_record(&self) -> Result<Option<String>, FaucetError> {
        let path = self
            .dir
            .join(self.template.commit_ledger_dir_name())
            .join(super::layout::COMMIT_LEDGER_FILE);
        fs(move || match std::fs::read_to_string(&path) {
            Ok(text) => Ok(Some(text)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(io_err("reading the overwrite commit ledger", &path, e)),
        })
        .await
    }

    async fn write_commit_record(&self, text: &str) -> Result<(), FaucetError> {
        let dir = self.dir.join(self.template.commit_ledger_dir_name());
        let text = text.to_string();
        fs(move || {
            faucet_core::path_guard::check(&dir)?;
            std::fs::create_dir_all(&dir)
                .map_err(|e| io_err("creating the overwrite commit ledger", &dir, e))?;
            refuse_linked_dir(&dir)?;
            let path = dir.join(super::layout::COMMIT_LEDGER_FILE);
            let tmp = dir.join(format!("{}.tmp", super::layout::COMMIT_LEDGER_FILE));
            faucet_core::path_guard::check(&tmp)?;
            faucet_core::path_guard::check(&path)?;
            write_fresh(&tmp, text.as_bytes())
                .map_err(|e| io_err("writing the overwrite commit ledger", &tmp, e))?;
            std::fs::rename(&tmp, &path)
                .map_err(|e| io_err("writing the overwrite commit ledger", &path, e))?;
            sync_parent(&path);
            Ok(())
        })
        .await
    }

    async fn promote(&self, name: &str) -> Result<(), FaucetError> {
        let from = self.path(Area::Swap, name);
        let dest = self.path(Area::Destination, name);
        fs(move || {
            faucet_core::path_guard::check(&dest)?;
            std::fs::rename(&from, &dest).map_err(|e| io_err("moving into place", &dest, e))?;
            sync_parent(&dest);
            Ok(())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn overwrite_runs_join_once_and_surface_listing_and_removal_errors() {
        let dir = tempfile::tempdir().unwrap();
        let b = backend(dir.path(), true);
        let a = faucet_core::idempotency::new_overwrite_run_token();
        let other = faucet_core::idempotency::new_overwrite_run_token();
        b.join_overwrite_run(&a).unwrap();
        b.join_overwrite_run(&a).unwrap();
        let e = b.join_overwrite_run(&other).unwrap_err();
        assert!(e.to_string().contains("already joined"), "{e}");
        // A missing output directory has no swap runs.
        let missing = backend(&dir.path().join("nope"), false);
        assert!(missing.swap_runs().await.unwrap().is_empty());
        // A file where the directory should be fails the listing.
        std::fs::write(dir.path().join("f"), b"").unwrap();
        let file = backend(&dir.path().join("f"), false);
        let e = file.swap_runs().await.unwrap_err();
        assert!(e.to_string().contains("listing"), "{e}");
        // A file where a swap area should be fails its removal.
        let area = dir.path().join(template().swap_run_dir_name(Some(&a)));
        std::fs::write(&area, b"").unwrap();
        let e = b.drop_swap_run(Some(&a)).await.unwrap_err();
        assert!(e.to_string().contains("overwrite swap area"), "{e}");
    }
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

    #[cfg(unix)]
    #[tokio::test]
    async fn a_planted_symlink_at_the_ledger_is_not_followed() {
        let dir = tempfile::tempdir().unwrap();
        let victim = dir.path().join("victim");
        std::fs::write(&victim, b"keep").unwrap();
        let b = backend(&dir.path().join("out"), true);
        let ledger = dir
            .path()
            .join("out")
            .join(template().commit_ledger_dir_name());
        std::fs::create_dir_all(&ledger).unwrap();
        std::fs::write(ledger.join(super::super::layout::COMMIT_LEDGER_FILE), b"0").unwrap();
        let tmp = ledger.join(format!("{}.tmp", super::super::layout::COMMIT_LEDGER_FILE));
        std::os::unix::fs::symlink(&victim, &tmp).unwrap();
        b.write_commit_record("7").await.unwrap();
        assert_eq!(std::fs::read(&victim).unwrap(), b"keep");
        assert_eq!(b.read_commit_record().await.unwrap().as_deref(), Some("7"));
        assert!(std::fs::symlink_metadata(&tmp).is_err(), "the link is gone");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn side_files_replace_links_and_linked_swap_areas_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let victim = dir.path().join("victim");
        std::fs::write(&victim, b"keep").unwrap();
        let marker = dir.path().join("marker.faucet-tmp");
        std::os::unix::fs::symlink(&victim, &marker).unwrap();
        write_fresh(&marker, b"m").unwrap();
        assert_eq!(std::fs::read(&victim).unwrap(), b"keep");
        assert_eq!(std::fs::read(&marker).unwrap(), b"m");
        write_fresh(&marker, b"n").unwrap();
        assert_eq!(std::fs::read(&marker).unwrap(), b"n");
        assert!(write_fresh(&dir.path().join("no/such"), b"x").is_err());
        let as_dir = dir.path().join("d");
        std::fs::create_dir_all(as_dir.join("x")).unwrap();
        assert!(write_fresh(&as_dir, b"x").is_err());

        let out = dir.path().join("out");
        std::fs::create_dir(&out).unwrap();
        let b = backend(&out, true);
        std::os::unix::fs::symlink(dir.path(), b.swap_dir()).unwrap();
        let e = b.prepare(Area::Swap).await.unwrap_err();
        assert!(e.to_string().contains("is a link"), "{e}");
        b.prepare(Area::Destination).await.unwrap();

        let other = dir.path().join("other");
        std::fs::create_dir(&other).unwrap();
        let ledger = other.join(template().commit_ledger_dir_name());
        std::os::unix::fs::symlink(dir.path(), &ledger).unwrap();
        let e = backend(&other, true)
            .write_commit_record("1")
            .await
            .unwrap_err();
        assert!(e.to_string().contains("is a link"), "{e}");
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
