//! Which of faucet's environment variables a Singer subprocess sees.

use serde::{Deserialize, Serialize};
use std::ffi::{OsStr, OsString};

/// Variables a tap or target always receives when the environment is not
/// inherited whole: enough to find executables, a home and a temp directory,
/// and a locale.
#[cfg(not(windows))]
pub const BASELINE_ENV: &[&str] = &["PATH", "HOME", "LANG", "LC_ALL", "TMPDIR"];

/// Variables a tap or target always receives when the environment is not
/// inherited whole: enough to find executables, a home and a temp directory,
/// and a locale.
#[cfg(windows)]
pub const BASELINE_ENV: &[&str] = &[
    "PATH",
    "HOME",
    "LANG",
    "LC_ALL",
    "TMPDIR",
    "SYSTEMROOT",
    "USERPROFILE",
    "TEMP",
    "TMP",
];

/// The `inherit_env` setting of a Singer tap or target.
///
/// `true` (the default) passes faucet's whole environment, as a shell would.
/// `false` passes only [`BASELINE_ENV`], keeping credentials faucet holds in
/// its environment (cloud keys, vault tokens, server secrets) away from the
/// subprocess. A list passes the baseline plus the named variables.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(untagged)]
pub enum InheritEnv {
    /// `true` inherits everything; `false` only the baseline.
    All(bool),
    /// The baseline plus these variable names.
    Only(Vec<String>),
}

impl Default for InheritEnv {
    fn default() -> Self {
        InheritEnv::All(true)
    }
}

impl InheritEnv {
    /// Whether the subprocess inherits the whole parent environment.
    pub fn inherits_all(&self) -> bool {
        matches!(self, InheritEnv::All(true))
    }

    /// The variables the subprocess receives out of `parent`, or `None` when
    /// it inherits everything (so the caller leaves the environment alone).
    pub fn filter<I>(&self, parent: I) -> Option<Vec<(OsString, OsString)>>
    where
        I: IntoIterator<Item = (OsString, OsString)>,
    {
        let extra: &[String] = match self {
            InheritEnv::All(true) => return None,
            InheritEnv::All(false) => &[],
            InheritEnv::Only(names) => names,
        };
        let allowed = |name: &OsStr| {
            BASELINE_ENV.iter().any(|b| same_name(name, b))
                || extra.iter().any(|e| same_name(name, e))
        };
        Some(
            parent
                .into_iter()
                .filter(|(name, _)| allowed(name))
                .collect(),
        )
    }

    /// [`filter`](Self::filter) over this process's environment.
    pub fn from_process(&self) -> Option<Vec<(OsString, OsString)>> {
        self.filter(std::env::vars_os())
    }
}

#[cfg(not(windows))]
fn same_name(name: &OsStr, wanted: &str) -> bool {
    name == wanted
}

#[cfg(windows)]
fn same_name(name: &OsStr, wanted: &str) -> bool {
    name.to_str()
        .is_some_and(|n| n.eq_ignore_ascii_case(wanted))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parent() -> Vec<(OsString, OsString)> {
        [
            ("PATH", "/usr/bin"),
            ("HOME", "/home/f"),
            ("AWS_SECRET_ACCESS_KEY", "s3cr3t"),
            ("FAUCET_VAULT_KEY", "vk"),
            ("TAP_TOKEN", "t"),
        ]
        .into_iter()
        .map(|(k, v)| (OsString::from(k), OsString::from(v)))
        .collect()
    }

    fn names(v: Option<Vec<(OsString, OsString)>>) -> Vec<String> {
        v.unwrap()
            .into_iter()
            .map(|(k, _)| k.into_string().unwrap())
            .collect()
    }

    #[test]
    fn default_inherits_everything() {
        assert!(InheritEnv::default().inherits_all());
        assert!(InheritEnv::default().filter(parent()).is_none());
    }

    #[test]
    fn false_passes_only_the_baseline() {
        let e = InheritEnv::All(false);
        assert!(!e.inherits_all());
        assert_eq!(names(e.filter(parent())), vec!["PATH", "HOME"]);
    }

    #[test]
    fn a_list_adds_named_variables_to_the_baseline() {
        let e = InheritEnv::Only(vec!["TAP_TOKEN".into(), "ABSENT".into()]);
        assert!(!e.inherits_all());
        assert_eq!(names(e.filter(parent())), vec!["PATH", "HOME", "TAP_TOKEN"]);
    }

    #[test]
    fn deserializes_from_a_bool_or_a_list() {
        let t: InheritEnv = serde_json::from_str("true").unwrap();
        assert_eq!(t, InheritEnv::All(true));
        let f: InheritEnv = serde_json::from_str("false").unwrap();
        assert_eq!(f, InheritEnv::All(false));
        let l: InheritEnv = serde_json::from_str(r#"["A","B"]"#).unwrap();
        assert_eq!(l, InheritEnv::Only(vec!["A".into(), "B".into()]));
        assert!(serde_json::from_str::<InheritEnv>("\"yes\"").is_err());
        assert_eq!(serde_json::to_string(&l).unwrap(), r#"["A","B"]"#);
    }

    #[test]
    fn from_process_reads_this_environment() {
        let got = InheritEnv::All(false).from_process().unwrap();
        assert!(
            got.iter()
                .all(|(k, _)| BASELINE_ENV.iter().any(|b| same_name(k, b)))
        );
        assert!(InheritEnv::All(true).from_process().is_none());
    }
}
