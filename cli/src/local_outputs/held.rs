//! Reaching a recorded output through directories held open (unix).
//!
//! The sweep checks a file and then deletes it. Naming the file by path at
//! the delete would resolve every directory on the path a second time, so a
//! directory changed in between could point the delete somewhere else. Here
//! every directory from the root down is opened without following symbolic
//! links, the file is checked relative to the last one, and the delete goes
//! through that same open directory.
//!
//! A row that carries an identity (its file's, its directory's, or both) has
//! its directory resolved first, so an output written through a symlinked
//! directory is still found; the identities then prove it is the same place.
//! A row with neither identity is walked exactly as recorded, so a symbolic
//! link anywhere on its path reads as changed.

use super::ledger::FileIdentity;
use rustix::fs::{AtFlags, FileType, Mode, OFlags, Stat};
use rustix::io::Errno;
use std::ffi::OsString;
use std::os::fd::OwnedFd;
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// What the file at the recorded path is, read through its held directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Leaf {
    pub is_file: bool,
    pub id: FileIdentity,
    pub len: u64,
    pub modified: Option<SystemTime>,
}

/// A recorded output reached through its directory, held open.
#[derive(Debug)]
pub(super) struct Held {
    dir: OwnedFd,
    name: OsString,
    pub leaf: Leaf,
}

/// The result of reaching a recorded output.
#[derive(Debug)]
pub(super) enum Opened {
    Held(Held),
    /// The file, or a directory on its path, is not there.
    Gone,
    /// A directory on the path is not the one recorded: a symbolic link, not a
    /// directory, or a different directory.
    Changed,
    Failed(std::io::Error),
}

/// The result of deleting a held output.
#[derive(Debug)]
pub(super) enum Unlinked {
    Removed,
    Gone,
    /// The name in the held directory no longer refers to the checked file.
    Changed,
    Failed(std::io::Error),
}

/// Reach the output at `path`, checking its directory against `parent_id`.
pub(super) fn open(
    path: &Path,
    parent_id: Option<FileIdentity>,
    file_id: Option<FileIdentity>,
) -> Opened {
    match resolve(path, parent_id.is_some() || file_id.is_some()) {
        Ok(target) => walk(&target, parent_id),
        Err(opened) => opened,
    }
}

/// The path to walk: with its directory resolved when `canonical`, as given
/// otherwise.
pub(super) fn resolve(path: &Path, canonical: bool) -> Result<PathBuf, Opened> {
    let (Some(parent), Some(name)) = (path.parent(), path.file_name()) else {
        return Err(Opened::Changed);
    };
    if !path.is_absolute() {
        return Err(Opened::Changed);
    }
    if !canonical {
        return Ok(path.to_path_buf());
    }
    std::fs::canonicalize(parent)
        .map(|dir| dir.join(name))
        .map_err(|e| from_io(&e).unwrap_or(Opened::Failed(e)))
}

/// Open every directory of `target` from the root without following links,
/// and read the file relative to the last one.
pub(super) fn walk(target: &Path, parent_id: Option<FileIdentity>) -> Opened {
    let (Some(parent), Some(name)) = (target.parent(), target.file_name()) else {
        return Opened::Changed;
    };
    let mut dir = match rustix::fs::open("/", dir_flags(), Mode::empty()) {
        Ok(fd) => fd,
        Err(e) => return from_errno(e),
    };
    for component in parent.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(segment) => {
                dir = match rustix::fs::openat(
                    &dir,
                    segment,
                    dir_flags() | OFlags::NOFOLLOW,
                    Mode::empty(),
                ) {
                    Ok(fd) => fd,
                    Err(e) => return from_errno(e),
                };
            }
            Component::Prefix(_) | Component::CurDir | Component::ParentDir => {
                return Opened::Changed;
            }
        }
    }
    if let Some(want) = parent_id {
        match rustix::fs::fstat(&dir) {
            Ok(st) if identity(&st) == want => {}
            Ok(_) => return Opened::Changed,
            Err(e) => return from_errno(e),
        }
    }
    match rustix::fs::statat(&dir, name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(st) => Opened::Held(Held {
            dir,
            name: name.to_os_string(),
            leaf: leaf(&st),
        }),
        Err(e) => from_errno(e),
    }
}

/// Delete the held output, while its name still refers to the checked file.
pub(super) fn unlink(held: &Held) -> Unlinked {
    match rustix::fs::statat(&held.dir, &held.name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(st) if leaf(&st).is_file && identity(&st) == held.leaf.id => {}
        Ok(_) => return Unlinked::Changed,
        Err(Errno::NOENT) => return Unlinked::Gone,
        Err(e) => return Unlinked::Failed(e.into()),
    }
    match rustix::fs::unlinkat(&held.dir, &held.name, AtFlags::empty()) {
        Ok(()) => Unlinked::Removed,
        Err(Errno::NOENT) => Unlinked::Gone,
        Err(e) => Unlinked::Failed(e.into()),
    }
}

fn from_errno(e: Errno) -> Opened {
    match e {
        Errno::NOENT => Opened::Gone,
        Errno::NOTDIR | Errno::LOOP => Opened::Changed,
        e => Opened::Failed(e.into()),
    }
}

fn from_io(e: &std::io::Error) -> Option<Opened> {
    let errno = Errno::from_io_error(e)?;
    match from_errno(errno) {
        Opened::Failed(_) => None,
        opened => Some(opened),
    }
}

/// Directories are opened for lookup only where the platform allows it, so an
/// ancestor the process may search but not list still works.
fn dir_flags() -> OFlags {
    OFlags::DIRECTORY | OFlags::CLOEXEC | search_flag()
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn search_flag() -> OFlags {
    OFlags::PATH
}

#[cfg(target_vendor = "apple")]
fn search_flag() -> OFlags {
    OFlags::from_bits_retain(libc::O_SEARCH as u32)
}

#[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
fn search_flag() -> OFlags {
    OFlags::RDONLY
}

#[allow(clippy::unnecessary_cast)] // field widths differ by platform; matches std's MetadataExt
fn identity(st: &Stat) -> FileIdentity {
    FileIdentity {
        dev: st.st_dev as u64,
        ino: st.st_ino as u64,
    }
}

#[allow(clippy::unnecessary_cast)] // field widths differ by platform
fn leaf(st: &Stat) -> Leaf {
    let secs = st.st_mtime as i64;
    let modified =
        u32::try_from(st.st_mtime_nsec as i64)
            .ok()
            .and_then(|nanos| match u64::try_from(secs) {
                Ok(secs) => UNIX_EPOCH.checked_add(Duration::new(secs, nanos)),
                Err(_) => UNIX_EPOCH.checked_sub(Duration::from_secs(secs.unsigned_abs())),
            });
    Leaf {
        is_file: FileType::from_raw_mode(st.st_mode) == FileType::RegularFile,
        id: identity(st),
        len: u64::try_from(st.st_size).unwrap_or(0),
        modified,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id_of(path: &Path) -> Option<FileIdentity> {
        FileIdentity::of(&std::fs::metadata(path).unwrap())
    }

    fn root() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = std::fs::canonicalize(dir.path()).unwrap();
        (dir, path)
    }

    fn held(opened: Opened) -> Held {
        match opened {
            Opened::Held(h) => h,
            other => panic!("expected a held output, got {other:?}"),
        }
    }

    #[test]
    fn a_held_file_reports_what_the_path_reports() {
        let (_t, root) = root();
        let path = root.join("out.jsonl");
        std::fs::write(&path, b"hello").unwrap();
        let meta = std::fs::metadata(&path).unwrap();
        let h = held(open(&path, id_of(&root), id_of(&path)));
        assert!(h.leaf.is_file);
        assert_eq!(Some(h.leaf.id), id_of(&path));
        assert_eq!(h.leaf.len, 5);
        assert_eq!(h.leaf.modified, meta.modified().ok());
        assert!(matches!(unlink(&h), Unlinked::Removed));
        assert!(!path.exists());
        assert!(matches!(unlink(&h), Unlinked::Gone));
    }

    #[test]
    fn an_ancestor_replaced_after_the_check_leaves_both_files() {
        let (_t, root) = root();
        let outputs = root.join("outputs");
        std::fs::create_dir(&outputs).unwrap();
        let path = outputs.join("out.jsonl");
        std::fs::write(&path, b"faucet").unwrap();
        let elsewhere = root.join("elsewhere");
        std::fs::create_dir(&elsewhere).unwrap();
        std::fs::write(elsewhere.join("out.jsonl"), b"other").unwrap();

        let target = resolve(&path, true).unwrap();
        std::fs::rename(&outputs, root.join("outputs.moved")).unwrap();
        std::os::unix::fs::symlink(&elsewhere, &outputs).unwrap();

        assert!(matches!(walk(&target, None), Opened::Changed));
        assert_eq!(
            std::fs::read(elsewhere.join("out.jsonl")).unwrap(),
            b"other"
        );
        assert_eq!(
            std::fs::read(root.join("outputs.moved/out.jsonl")).unwrap(),
            b"faucet"
        );
    }

    #[test]
    fn an_ancestor_replaced_after_the_walk_deletes_only_the_checked_file() {
        let (_t, root) = root();
        let outputs = root.join("outputs");
        std::fs::create_dir(&outputs).unwrap();
        let path = outputs.join("out.jsonl");
        std::fs::write(&path, b"faucet").unwrap();
        let elsewhere = root.join("elsewhere");
        std::fs::create_dir(&elsewhere).unwrap();
        std::fs::write(elsewhere.join("out.jsonl"), b"other").unwrap();

        let h = held(open(&path, id_of(&outputs), id_of(&path)));
        std::fs::rename(&outputs, root.join("outputs.moved")).unwrap();
        std::os::unix::fs::symlink(&elsewhere, &outputs).unwrap();

        assert!(matches!(unlink(&h), Unlinked::Removed));
        assert_eq!(
            std::fs::read(elsewhere.join("out.jsonl")).unwrap(),
            b"other"
        );
        assert!(!root.join("outputs.moved/out.jsonl").exists());
    }

    #[test]
    fn a_directory_that_changed_since_it_was_recorded_is_changed() {
        let (_t, root) = root();
        let outputs = root.join("outputs");
        std::fs::create_dir(&outputs).unwrap();
        let path = outputs.join("out.jsonl");
        std::fs::write(&path, b"x").unwrap();
        let recorded = id_of(&outputs);
        std::fs::rename(&outputs, root.join("old")).unwrap();
        std::fs::create_dir(&outputs).unwrap();
        std::fs::write(&path, b"y").unwrap();
        assert!(matches!(open(&path, recorded, None), Opened::Changed));
    }

    #[test]
    fn a_name_replaced_in_the_held_directory_is_not_deleted() {
        let (_t, root) = root();
        let path = root.join("out.jsonl");
        std::fs::write(&path, b"faucet").unwrap();
        let h = held(open(&path, None, id_of(&path)));
        let other = root.join("other");
        std::fs::write(&other, b"other").unwrap();
        std::fs::rename(&other, &path).unwrap();
        assert!(matches!(unlink(&h), Unlinked::Changed));
        assert_eq!(std::fs::read(&path).unwrap(), b"other");
    }

    #[test]
    fn a_path_without_identities_is_walked_without_following_links() {
        let (_t, root) = root();
        let real = root.join("real");
        std::fs::create_dir(&real).unwrap();
        std::fs::write(real.join("out.jsonl"), b"x").unwrap();
        let link = root.join("link");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        assert!(matches!(
            open(&link.join("out.jsonl"), None, None),
            Opened::Changed
        ));
        assert!(matches!(
            open(&real.join("out.jsonl"), None, None),
            Opened::Held(_)
        ));
        let with_id = held(open(&link.join("out.jsonl"), id_of(&real), None));
        assert!(with_id.leaf.is_file);
    }

    #[test]
    fn missing_paths_and_odd_shapes_are_classified() {
        let (_t, root) = root();
        assert!(matches!(
            open(&root.join("none/out.jsonl"), None, None),
            Opened::Gone
        ));
        assert!(matches!(
            open(
                &root.join("none/out.jsonl"),
                Some(FileIdentity { dev: 0, ino: 0 }),
                None
            ),
            Opened::Gone
        ));
        assert!(matches!(
            open(&root.join("out.jsonl"), None, None),
            Opened::Gone
        ));
        let file = root.join("file");
        std::fs::write(&file, b"x").unwrap();
        assert!(matches!(
            open(&file.join("out.jsonl"), None, None),
            Opened::Changed
        ));
        assert!(matches!(
            open(&file.join("out.jsonl"), id_of(&root), None),
            Opened::Changed
        ));
        assert!(matches!(open(Path::new("/"), None, None), Opened::Changed));
        assert!(matches!(walk(Path::new("/"), None), Opened::Changed));
        std::fs::create_dir(root.join("a")).unwrap();
        assert!(matches!(
            open(&root.join("a/../out.jsonl"), None, None),
            Opened::Changed
        ));
        assert!(matches!(
            open(Path::new("relative/out.jsonl"), None, None),
            Opened::Changed
        ));
    }

    #[test]
    fn an_output_under_a_search_only_directory_is_still_collected() {
        use std::os::unix::fs::PermissionsExt;
        let (_t, root) = root();
        let gate = root.join("gate");
        let inner = gate.join("inner");
        std::fs::create_dir_all(&inner).unwrap();
        let path = inner.join("out.jsonl");
        std::fs::write(&path, b"x").unwrap();
        let (dir_id, file_id) = (id_of(&inner), id_of(&path));
        std::fs::set_permissions(&gate, std::fs::Permissions::from_mode(0o100)).unwrap();
        let opened = open(&path, dir_id, file_id);
        let removed = match &opened {
            Opened::Held(h) => Some(matches!(unlink(h), Unlinked::Removed)),
            _ => None,
        };
        std::fs::set_permissions(&gate, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert_eq!(removed, Some(true), "{opened:?}");
        assert!(!path.exists());
    }

    #[test]
    fn an_unreadable_directory_is_a_failure_not_a_change() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let (_t, root) = root();
        if std::fs::metadata(&root).unwrap().uid() == 0 {
            return;
        }
        let gate = root.join("gate");
        std::fs::create_dir(&gate).unwrap();
        let inner = gate.join("inner");
        std::fs::create_dir(&inner).unwrap();
        std::fs::set_permissions(&gate, std::fs::Permissions::from_mode(0o000)).unwrap();
        let without = open(&inner.join("out.jsonl"), None, None);
        let with = open(
            &inner.join("out.jsonl"),
            Some(FileIdentity { dev: 0, ino: 0 }),
            None,
        );
        std::fs::set_permissions(&gate, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(matches!(without, Opened::Failed(_)), "{without:?}");
        assert!(matches!(with, Opened::Failed(_)), "{with:?}");
    }
}
