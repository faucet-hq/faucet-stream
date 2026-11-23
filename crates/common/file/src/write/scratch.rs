//! Private scratch directories owned by one live writer: an owner file held
//! locked for the writer's life tells a live directory from one a crashed
//! run left behind.

use std::path::Path;

/// The owner file inside a scratch directory.
pub(crate) const OWNER_FILE: &str = ".owner";
/// Age past which a scratch directory whose owner cannot be told by a lock
/// (left by a faucet that predates the lock, or on a file system without
/// file locks) is treated as abandoned once nothing in it has changed.
pub(crate) const UNOWNED_MAX_AGE: std::time::Duration =
    std::time::Duration::from_secs(24 * 60 * 60);

/// Lock the owner file under a private name and only then rename it into
/// place, so a concurrent sweep never sees an unlocked `.owner` in a live
/// directory and removes it. On a file system without file locks the
/// directory is left without an owner file (`None`): the lock only guards
/// cleanup, and an unowned directory is swept by age instead.
pub(crate) fn lock_owner(dir: &Path) -> std::io::Result<Option<std::fs::File>> {
    let pending = dir.join(".owner.pending");
    let file = std::fs::File::create(&pending)?;
    match file.try_lock() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => {
            return Err(std::io::Error::other(
                "the new owner file is locked by another process",
            ));
        }
        Err(std::fs::TryLockError::Error(e)) => {
            tracing::warn!(
                path = %dir.display(),
                error = %e,
                "file sink: the scratch file system does not support file locks; \
                 an abandoned scratch directory is removed only once nothing in it changed for a day"
            );
            drop(file);
            std::fs::remove_file(&pending)?;
            return Ok(None);
        }
    }
    std::fs::rename(&pending, dir.join(OWNER_FILE))?;
    Ok(Some(file))
}

/// Remove the directories in `parent` named with `prefix` whose writer is
/// gone. Best effort — a directory that can not be inspected is left.
pub(crate) fn sweep_orphaned(parent: &Path, prefix: &str, keep: Option<&Path>) {
    let Ok(entries) = std::fs::read_dir(parent) else {
        return;
    };
    for entry in entries.flatten() {
        if !entry.file_name().to_string_lossy().starts_with(prefix)
            || !entry.file_type().is_ok_and(|t| t.is_dir())
        {
            continue;
        }
        let dir = entry.path();
        if keep.is_some_and(|k| k == dir) {
            continue;
        }
        if is_orphaned(&dir) && std::fs::remove_dir_all(&dir).is_ok() {
            tracing::info!(
                path = %dir.display(),
                "file sink: removed the scratch directory of a writer that is gone"
            );
        }
    }
}

/// Whether the writer that owns `dir` is gone: its owner lock can be taken,
/// or — when locks tell nothing — nothing in it changed for
/// [`UNOWNED_MAX_AGE`]. The newest file is what counts, not the directory,
/// whose time does not move while a file in it is written.
pub(crate) fn is_orphaned(dir: &Path) -> bool {
    match std::fs::File::open(dir.join(OWNER_FILE)) {
        Ok(owner) => match owner.try_lock() {
            Ok(()) => true,
            Err(std::fs::TryLockError::WouldBlock) => false,
            Err(std::fs::TryLockError::Error(_)) => idle(dir),
        },
        Err(_) => idle(dir),
    }
}

fn idle(dir: &Path) -> bool {
    let age = |m: std::fs::Metadata| m.modified().ok().and_then(|t| t.elapsed().ok());
    let mut youngest = std::fs::metadata(dir).ok().and_then(age);
    if let Ok(entries) = std::fs::read_dir(dir) {
        for e in entries.flatten() {
            if let Some(a) = e.metadata().ok().and_then(age) {
                youngest = Some(youngest.map_or(a, |y: std::time::Duration| y.min(a)));
            }
        }
    }
    youngest.is_some_and(|a| a > UNOWNED_MAX_AGE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_locked_pending_owner_file_refuses_a_second_owner() {
        let dir = tempfile::tempdir().unwrap();
        let held = std::fs::File::create(dir.path().join(".owner.pending")).unwrap();
        held.lock().unwrap();
        let e = lock_owner(dir.path()).unwrap_err();
        assert!(e.to_string().contains("locked by another process"), "{e}");
    }

    #[test]
    fn the_sweep_keeps_the_callers_own_directory_and_live_owners() {
        let parent = tempfile::tempdir().unwrap();
        let mine = parent.path().join("w.scratch-mine");
        let live = parent.path().join("w.scratch-live");
        let gone = parent.path().join("w.scratch-gone");
        for d in [&mine, &live, &gone] {
            std::fs::create_dir(d).unwrap();
        }
        std::fs::write(parent.path().join("w.scratch-file"), b"").unwrap();
        let _live_owner = lock_owner(&live).unwrap().expect("locks are supported");
        drop(lock_owner(&gone).unwrap());
        sweep_orphaned(parent.path(), "w.scratch-", Some(&mine));
        assert!(mine.exists());
        assert!(live.exists());
        assert!(!gone.exists());
        assert!(parent.path().join("w.scratch-file").exists());
    }

    #[test]
    fn an_unowned_directory_with_a_fresh_file_is_not_orphaned() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("part.faucet-tmp"), b"x").unwrap();
        assert!(!is_orphaned(dir.path()));
    }
}
