//! A process-global guard on the local files connectors open.
//!
//! A host that runs configs it did not write (`faucet serve`) checks their
//! paths when it accepts them, but a path is only a pattern until a connector
//! lists or opens files: a glob, a directory, a templated name. A host installs
//! a guard once at startup; file connectors call [`check`] on each concrete
//! path right before they read or replace it. With no guard installed,
//! [`check`] accepts everything and costs one atomic load.

use crate::error::FaucetError;
use std::path::Path;
use std::sync::OnceLock;

/// A guard: `Some(reason)` when `path` may not be opened.
pub type Guard = Box<dyn Fn(&Path) -> Option<String> + Send + Sync>;

fn hook() -> &'static OnceLock<Guard> {
    static HOOK: OnceLock<Guard> = OnceLock::new();
    &HOOK
}

/// Install the process-wide guard. The **first** call wins; later calls are
/// ignored and return `false`.
pub fn install(guard: Guard) -> bool {
    hook().set(guard).is_ok()
}

/// Whether a guard has been installed.
pub fn is_installed() -> bool {
    hook().get().is_some()
}

/// Refuse `path` when the installed guard does.
pub fn check(path: &Path) -> Result<(), FaucetError> {
    match hook().get().and_then(|g| g(path)) {
        Some(reason) => Err(FaucetError::Config(format!(
            "'{}' {reason}",
            path.display()
        ))),
        None => Ok(()),
    }
}

/// Open the file at `path` for reading. Under a `policy` with path roots the
/// file is reached by [`open_unlinked`], its resolved path must sit under a
/// root and pass [`check`], and nothing on the way is a link; otherwise it is
/// opened by name after [`check`].
pub fn open_within(
    path: &Path,
    policy: Option<&crate::resolve::ReferencePolicy>,
) -> Result<std::fs::File, FaucetError> {
    let read_err =
        |e: std::io::Error| FaucetError::Source(format!("reading '{}': {e}", path.display()));
    let Some(policy) = policy.filter(|p| p.path_roots().is_some()) else {
        check(path)?;
        return std::fs::File::open(path).map_err(read_err);
    };
    let (resolved, file) = open_unlinked(path).map_err(|e| {
        if is_link_error(&e) {
            FaucetError::Config(format!(
                "'{}' is reached through a link, which is not followed for this config",
                path.display()
            ))
        } else {
            read_err(e)
        }
    })?;
    policy
        .check_path(&resolved.to_string_lossy())
        .map_err(|r| FaucetError::Config(format!("'{}': {r}", path.display())))?;
    check(&resolved)?;
    Ok(file)
}

fn is_link_error(e: &std::io::Error) -> bool {
    #[cfg(unix)]
    {
        matches!(
            rustix::io::Errno::from_io_error(e),
            Some(rustix::io::Errno::LOOP | rustix::io::Errno::NOTDIR)
        )
    }
    #[cfg(not(unix))]
    {
        let _ = e;
        false
    }
}

/// Open the file at `path` for reading without following a link on the way:
/// the path is resolved first, then every directory of the resolved path from
/// the root and the file itself are opened without following links, so a
/// link put in place after the resolution makes the open fail. Returns the
/// resolved path with the open file.
#[cfg(unix)]
pub fn open_unlinked(path: &Path) -> std::io::Result<(std::path::PathBuf, std::fs::File)> {
    use rustix::fs::{Mode, OFlags};
    let resolved = std::fs::canonicalize(path)?;
    let (Some(parent), Some(name)) = (resolved.parent(), resolved.file_name()) else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "not a file path",
        ));
    };
    let dir = open_dir_unlinked(parent)?;
    let fd = rustix::fs::openat(
        &dir,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )?;
    let file = std::fs::File::from(fd);
    if !file.metadata()?.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "not a regular file",
        ));
    }
    Ok((resolved, file))
}

/// Off unix: the resolved path, opened by name.
#[cfg(not(unix))]
pub fn open_unlinked(path: &Path) -> std::io::Result<(std::path::PathBuf, std::fs::File)> {
    let resolved = std::fs::canonicalize(path)?;
    let file = std::fs::File::open(&resolved)?;
    Ok((resolved, file))
}

/// Open the absolute directory `dir` by opening every directory from the root
/// without following links. `dir` may hold no `.` or `..` components.
#[cfg(unix)]
pub fn open_dir_unlinked(dir: &Path) -> std::io::Result<std::os::fd::OwnedFd> {
    use rustix::fs::{Mode, OFlags};
    use std::path::Component;
    if !dir.is_absolute() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "not an absolute directory",
        ));
    }
    let flags = OFlags::DIRECTORY | OFlags::CLOEXEC | search_flag();
    let mut fd = rustix::fs::open("/", flags, Mode::empty())?;
    for component in dir.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(segment) => {
                fd = rustix::fs::openat(&fd, segment, flags | OFlags::NOFOLLOW, Mode::empty())?;
            }
            Component::Prefix(_) | Component::CurDir | Component::ParentDir => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "a directory with `.` or `..` components",
                ));
            }
        }
    }
    Ok(fd)
}

/// Directories are opened for lookup only where the platform allows it, so an
/// ancestor the process may search but not list still works.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn search_flag() -> rustix::fs::OFlags {
    rustix::fs::OFlags::PATH
}

#[cfg(target_vendor = "apple")]
fn search_flag() -> rustix::fs::OFlags {
    rustix::fs::OFlags::from_bits_retain(libc::O_SEARCH as u32)
}

#[cfg(all(
    unix,
    not(any(target_os = "linux", target_os = "android", target_vendor = "apple"))
))]
fn search_flag() -> rustix::fs::OFlags {
    rustix::fs::OFlags::RDONLY
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn files_are_reached_without_following_links() {
        let dir = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(dir.path()).unwrap();
        std::fs::create_dir(root.join("inside")).unwrap();
        std::fs::write(root.join("inside/a"), b"a").unwrap();
        std::fs::create_dir(root.join("outside")).unwrap();
        std::fs::write(root.join("outside/secret"), b"s").unwrap();
        std::os::unix::fs::symlink(root.join("outside/secret"), root.join("inside/leaf")).unwrap();
        std::os::unix::fs::symlink(root.join("outside"), root.join("inside/sub")).unwrap();

        let (resolved, mut f) = open_unlinked(&root.join("inside/a")).unwrap();
        assert_eq!(resolved, root.join("inside/a"));
        let mut text = String::new();
        std::io::Read::read_to_string(&mut f, &mut text).unwrap();
        assert_eq!(text, "a");

        let (resolved, _) = open_unlinked(&root.join("inside/leaf")).unwrap();
        assert_eq!(resolved, root.join("outside/secret"));
        let (resolved, _) = open_unlinked(&root.join("inside/sub/secret")).unwrap();
        assert_eq!(
            resolved,
            root.join("outside/secret"),
            "resolved, then checked"
        );
        let e = open_dir_unlinked(&root.join("inside/sub")).unwrap_err();
        assert!(is_link_error(&e), "a swapped-in link is not followed: {e}");
        assert!(open_dir_unlinked(Path::new("relative")).is_err());
        assert!(open_dir_unlinked(&root.join("inside/../outside")).is_err());
        assert!(open_unlinked(Path::new("/")).is_err());
        assert!(open_unlinked(&root.join("inside")).is_err(), "a directory");
        assert!(!is_link_error(&std::io::Error::other("x")));
    }

    #[cfg(unix)]
    #[test]
    fn a_swapped_file_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(dir.path()).unwrap();
        std::fs::create_dir_all(root.join("in/d")).unwrap();
        std::fs::write(root.join("in/d/f"), b"f").unwrap();
        std::fs::create_dir(root.join("out")).unwrap();
        std::fs::write(root.join("out/f"), b"s").unwrap();
        let resolved = std::fs::canonicalize(root.join("in/d/f")).unwrap();
        std::fs::rename(root.join("in/d"), root.join("in/old")).unwrap();
        std::os::unix::fs::symlink(root.join("out"), root.join("in/d")).unwrap();
        let e = open_dir_unlinked(resolved.parent().unwrap()).unwrap_err();
        assert!(is_link_error(&e), "{e}");
        std::fs::remove_file(root.join("in/d")).unwrap();
        std::fs::create_dir(root.join("in/d")).unwrap();
        std::os::unix::fs::symlink(root.join("out/f"), root.join("in/d/f")).unwrap();
        let dir = open_dir_unlinked(resolved.parent().unwrap()).unwrap();
        let e = rustix::fs::openat(
            &dir,
            "f",
            rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NOFOLLOW,
            rustix::fs::Mode::empty(),
        )
        .unwrap_err();
        assert!(is_link_error(&e.into()));
    }

    #[cfg(unix)]
    #[test]
    fn open_within_confines_files_to_the_roots() {
        use crate::resolve::ReferencePolicy;
        let dir = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(dir.path()).unwrap();
        std::fs::create_dir(root.join("in")).unwrap();
        std::fs::create_dir(root.join("out")).unwrap();
        std::fs::write(root.join("in/a"), b"a").unwrap();
        std::fs::write(root.join("out/s"), b"s").unwrap();
        std::os::unix::fs::symlink(root.join("out"), root.join("in/link")).unwrap();
        std::os::unix::fs::symlink(root.join("out/s"), root.join("in/leaf")).unwrap();
        let policy = ReferencePolicy::open().with_path_roots(vec![root.join("in")]);

        assert!(open_within(&root.join("in/a"), Some(&policy)).is_ok());
        let e = open_within(&root.join("in/link/s"), Some(&policy)).unwrap_err();
        assert!(
            e.to_string().contains("outside the allowed path roots"),
            "{e}"
        );
        let e = open_within(&root.join("in/leaf"), Some(&policy)).unwrap_err();
        assert!(
            e.to_string().contains("outside the allowed path roots"),
            "{e}"
        );
        std::fs::write(root.join("in/b"), b"b").unwrap();
        std::os::unix::fs::symlink(root.join("in/b"), root.join("in/inner")).unwrap();
        assert!(open_within(&root.join("in/inner"), Some(&policy)).is_ok());
        let e = open_within(&root.join("in/missing"), Some(&policy)).unwrap_err();
        assert!(matches!(e, FaucetError::Source(_)), "{e}");

        assert!(
            open_within(&root.join("in/leaf"), None).is_ok(),
            "trusted runs follow links"
        );
        let open = ReferencePolicy::open();
        assert!(open_within(&root.join("in/link/s"), Some(&open)).is_ok());
        assert!(open_within(&root.join("in/missing"), None).is_err());
    }

    #[test]
    fn an_installed_guard_refuses_what_it_names() {
        install(Box::new(|p: &Path| {
            p.file_name()
                .is_some_and(|n| n == "guarded-by-test")
                .then(|| "is refused by the test guard".to_string())
        }));
        assert!(is_installed());
        assert!(!install(Box::new(|_: &Path| None)), "first install wins");
        let err = check(Path::new("/x/guarded-by-test")).unwrap_err();
        assert!(
            err.to_string().contains("refused by the test guard"),
            "{err}"
        );
        assert!(check(Path::new("/x/other")).is_ok());
    }
}
