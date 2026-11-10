//! Resolve a `.env` file path and auto-discover a pipeline config file.
//!
//! Both helpers take an explicit "from" directory so unit tests can run in a
//! tempdir without touching process cwd. Production call-sites pass
//! `std::env::current_dir()?` as the third argument.

use crate::error::{CliError, CliResult};
use std::path::{Path, PathBuf};

/// Resolve the `.env` file to load (if any). Precedence:
///
/// 1. If `no_env_file` is set, return `Ok(None)`.
/// 2. If `explicit` is `Some(path)`, the file must exist — otherwise error.
/// 3. Otherwise, return `Some(dir/.env)` if it exists, else `None`.
pub fn resolve_env_file(
    explicit: Option<&Path>,
    no_env_file: bool,
    dir: &Path,
) -> CliResult<Option<PathBuf>> {
    if no_env_file {
        return Ok(None);
    }
    if let Some(p) = explicit {
        if !p.exists() {
            return Err(CliError::EnvFileNotFound {
                path: p.to_path_buf(),
            });
        }
        return Ok(Some(p.to_path_buf()));
    }
    let candidate = dir.join(".env");
    Ok(candidate.is_file().then_some(candidate))
}

/// Load the resolved `.env` via `dotenvy::from_path`. No-op when `path` is
/// `None`. Process-env values always win over `.env`-supplied ones — that's
/// the dotenvy default and we deliberately don't override it.
pub fn load_env_file_if_present(path: Option<&Path>) -> CliResult<()> {
    if let Some(p) = path {
        dotenvy::from_path(p).map_err(|source| CliError::ReadConfig {
            path: p.to_path_buf(),
            source: std::io::Error::other(source),
        })?;
    }
    Ok(())
}

/// Probe `dir` for `faucet.yaml`, `faucet.yml`, then `faucet.json`. Returns the
/// first match in priority order, or `None`. When more than one candidate is
/// present, emits a `tracing::warn!` naming the runner-ups so a stale
/// `faucet.yml` next to the canonical `faucet.yaml` doesn't go unnoticed.
pub fn discover_config_path(dir: &Path) -> Option<PathBuf> {
    const CANDIDATES: &[&str] = &["faucet.yaml", "faucet.yml", "faucet.json"];
    let mut chosen: Option<PathBuf> = None;
    let mut also: Vec<PathBuf> = Vec::new();
    for name in CANDIDATES {
        let p = dir.join(name);
        if p.exists() {
            if chosen.is_none() {
                chosen = Some(p);
            } else {
                also.push(p);
            }
        }
    }
    if !also.is_empty()
        && let Some(ref c) = chosen
    {
        let extras: Vec<String> = also.iter().map(|p| p.display().to_string()).collect();
        tracing::warn!(
            chosen = %c.display(),
            also_present = ?extras,
            "multiple faucet.* config files in cwd; picking the first in priority order (yaml > yml > json)"
        );
    }
    chosen
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn resolve_env_file_explicit_wins() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("secrets.env");
        std::fs::write(&p, "X=1\n").unwrap();
        let resolved = resolve_env_file(Some(&p), false, dir.path()).unwrap();
        assert_eq!(resolved, Some(p));
    }

    #[test]
    fn resolve_env_file_explicit_missing_errors() {
        let dir = tempdir().unwrap();
        let missing = dir.path().join("nope.env");
        let err = resolve_env_file(Some(&missing), false, dir.path()).unwrap_err();
        assert!(matches!(err, CliError::EnvFileNotFound { .. }));
    }

    #[test]
    fn resolve_env_file_auto_finds_dotenv_in_cwd() {
        let dir = tempdir().unwrap();
        let dotenv = dir.path().join(".env");
        std::fs::write(&dotenv, "X=1\n").unwrap();
        assert_eq!(
            resolve_env_file(None, false, dir.path()).unwrap(),
            Some(dotenv)
        );
    }

    #[test]
    fn resolve_env_file_none_when_missing_and_not_explicit() {
        let dir = tempdir().unwrap();
        assert_eq!(resolve_env_file(None, false, dir.path()).unwrap(), None);
    }

    #[test]
    fn resolve_env_file_skips_a_dotenv_directory() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join(".env")).unwrap();
        assert_eq!(resolve_env_file(None, false, dir.path()).unwrap(), None);
    }

    #[test]
    fn resolve_env_file_no_env_file_skips_auto_discovery() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join(".env"), "X=1\n").unwrap();
        assert_eq!(resolve_env_file(None, true, dir.path()).unwrap(), None);
    }

    #[test]
    fn resolve_env_file_no_env_file_also_skips_explicit_path() {
        // --no-env-file conflicts with --env-file at the clap level, but be
        // defensive at the function boundary too.
        let dir = tempdir().unwrap();
        let p = dir.path().join("explicit.env");
        std::fs::write(&p, "X=1\n").unwrap();
        assert_eq!(resolve_env_file(Some(&p), true, dir.path()).unwrap(), None);
    }

    #[test]
    fn discover_config_path_prefers_yaml() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("faucet.yaml"), "version: 1\n").unwrap();
        std::fs::write(dir.path().join("faucet.yml"), "version: 1\n").unwrap();
        std::fs::write(dir.path().join("faucet.json"), "{}").unwrap();
        let found = discover_config_path(dir.path());
        assert_eq!(found, Some(dir.path().join("faucet.yaml")));
    }

    #[test]
    fn discover_config_path_falls_through_to_yml() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("faucet.yml"), "v: 1\n").unwrap();
        std::fs::write(dir.path().join("faucet.json"), "{}").unwrap();
        assert_eq!(
            discover_config_path(dir.path()),
            Some(dir.path().join("faucet.yml"))
        );
    }

    #[test]
    fn discover_config_path_falls_through_to_json() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("faucet.json"), "{}").unwrap();
        assert_eq!(
            discover_config_path(dir.path()),
            Some(dir.path().join("faucet.json"))
        );
    }

    #[test]
    fn discover_config_path_none_when_missing() {
        let dir = tempdir().unwrap();
        assert_eq!(discover_config_path(dir.path()), None);
    }

    #[test]
    fn load_env_file_if_present_is_noop_for_none() {
        load_env_file_if_present(None).unwrap();
    }

    #[test]
    fn load_env_file_if_present_propagates_parse_failure_as_readconfig() {
        // Write a malformed .env (lines must be KEY=VALUE).
        let dir = tempdir().unwrap();
        let p = dir.path().join("bad.env");
        std::fs::write(&p, "no equals here\n").unwrap();
        let err = load_env_file_if_present(Some(&p)).unwrap_err();
        assert!(matches!(err, CliError::ReadConfig { .. }));
    }
}
