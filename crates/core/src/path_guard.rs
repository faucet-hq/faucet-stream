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

#[cfg(test)]
mod tests {
    use super::*;

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
