//! The local-filesystem backend: scratch files beside the destination,
//! fsync, and an atomic rename into place.

use super::backend::{Area, StorageBackend};
use super::layout::io_err;
use faucet_core::FaucetError;
use std::path::{Path, PathBuf};

/// Files in one local directory; overwrite runs stage in a hidden sibling
/// directory inside it.
#[derive(Debug, Clone)]
pub struct LocalBackend {
    dir: PathBuf,
    staging: PathBuf,
    create_dirs: bool,
}

impl LocalBackend {
    /// Write into `dir` (`""` = the working directory), staging overwrite
    /// runs in `dir/<staging_name>`. With `create_dirs` false a missing
    /// `dir` is an error rather than created.
    pub fn new(dir: &str, staging_name: &str, create_dirs: bool) -> Self {
        let dir = if dir.is_empty() {
            PathBuf::from(".")
        } else {
            PathBuf::from(dir)
        };
        Self {
            staging: dir.join(staging_name),
            dir,
            create_dirs,
        }
    }

    /// The destination directory.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The overwrite staging directory.
    pub fn staging_dir(&self) -> &Path {
        &self.staging
    }

    fn area(&self, area: Area) -> &Path {
        match area {
            Area::Destination => &self.dir,
            Area::Staging => &self.staging,
        }
    }

    fn path(&self, area: Area, name: &str) -> PathBuf {
        self.area(area).join(name)
    }
}

/// Fsync the directory holding `path`, so a rename or delete survives a
/// crash. Best effort; a no-op off Unix.
pub fn sync_dir(path: &Path) {
    #[cfg(unix)]
    if let Some(dir) = path.parent()
        && let Ok(d) = std::fs::File::open(if dir.as_os_str().is_empty() {
            Path::new(".")
        } else {
            dir
        })
    {
        let _ = d.sync_all();
    }
    #[cfg(not(unix))]
    let _ = path;
}

impl StorageBackend for LocalBackend {
    fn describe(&self, area: Area, name: &str) -> String {
        self.path(area, name).display().to_string()
    }

    fn local_path(&self, area: Area, name: &str) -> Option<PathBuf> {
        Some(self.path(area, name))
    }

    fn scratch_path(&self, area: Area, name: &str) -> Result<PathBuf, FaucetError> {
        Ok(super::layout::tmp_path(
            &self.path(area, name),
            super::layout::TMP_SUFFIX,
        ))
    }

    fn remove_stale_scratch(&self, area: Area, ours: &dyn Fn(&str) -> bool) {
        let Ok(entries) = std::fs::read_dir(self.area(area)) else {
            return;
        };
        for entry in entries.flatten() {
            if entry.file_name().to_str().is_some_and(ours) {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }

    fn prepare(&self, area: Area) -> Result<(), FaucetError> {
        let dir = self.area(area);
        if dir.is_dir() {
            return Ok(());
        }
        if area == Area::Destination && !self.create_dirs {
            return Err(FaucetError::Sink(format!(
                "file sink: directory '{}' does not exist and `create_dirs` is off",
                dir.display()
            )));
        }
        std::fs::create_dir_all(dir).map_err(|e| io_err("creating directory", dir, e))
    }

    fn list(&self, area: Area) -> Result<Vec<String>, FaucetError> {
        let dir = self.area(area);
        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(io_err("listing", dir, e)),
        };
        let mut out = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| io_err("listing", dir, e))?;
            if entry.file_type().map(|t| t.is_file()).unwrap_or(false)
                && let Some(name) = entry.file_name().to_str()
            {
                out.push(name.to_string());
            }
        }
        Ok(out)
    }

    fn exists(&self, area: Area, name: &str) -> Result<bool, FaucetError> {
        Ok(self.path(area, name).exists())
    }

    fn fetch(&self, area: Area, name: &str, to: &Path) -> Result<(), FaucetError> {
        let from = self.path(area, name);
        std::fs::copy(&from, to)
            .map(|_| ())
            .map_err(|e| io_err("copying", &from, e))
    }

    fn commit(&self, scratch: &Path, area: Area, name: &str) -> Result<(), FaucetError> {
        let dest = self.path(area, name);
        std::fs::rename(scratch, &dest).map_err(|e| io_err("renaming into place", &dest, e))?;
        sync_dir(&dest);
        Ok(())
    }

    fn delete(&self, area: Area, name: &str) -> Result<(), FaucetError> {
        let path = self.path(area, name);
        match std::fs::remove_file(&path) {
            Ok(()) => {
                sync_dir(&path);
                Ok(())
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(io_err("removing earlier output", &path, e)),
        }
    }

    fn begin_staging(&self) -> Result<(), FaucetError> {
        self.prepare(Area::Destination)?;
        if self.staging.exists() {
            std::fs::remove_dir_all(&self.staging)
                .map_err(|e| io_err("clearing staging directory", &self.staging, e))?;
        }
        std::fs::create_dir_all(&self.staging)
            .map_err(|e| io_err("creating staging directory", &self.staging, e))
    }

    fn staging_ready(&self) -> Result<bool, FaucetError> {
        Ok(self.staging.is_dir())
    }

    fn promote(&self, name: &str) -> Result<(), FaucetError> {
        let dest = self.path(Area::Destination, name);
        std::fs::rename(self.path(Area::Staging, name), &dest)
            .map_err(|e| io_err("moving into place", &dest, e))
    }

    fn clear_staging(&self) -> Result<(), FaucetError> {
        if !self.staging.exists() {
            return Ok(());
        }
        std::fs::remove_dir_all(&self.staging)
            .map_err(|e| io_err("removing staging directory", &self.staging, e))?;
        sync_dir(&self.dir.join("x"));
        Ok(())
    }
}
