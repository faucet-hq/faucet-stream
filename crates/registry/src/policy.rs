//! What a build is trusted to do.

/// What [`crate::build_source_with`] / [`crate::build_sink_with`] may build.
///
/// The default refuses every connector that runs a program on this host — a
/// connector whose config schema declares a field annotated
/// `x-faucet-resource: command`, such as the Singer tap and target bridges —
/// with [`crate::RegistryError::SubprocessRefused`]. A host that trusts the
/// config it builds from (it wrote it, or an operator did) opts in:
///
/// ```
/// let policy = faucet_registry::BuildPolicy::default().allow_subprocess(true);
/// assert!(policy.allows_subprocess());
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct BuildPolicy {
    allow_subprocess: bool,
}

impl BuildPolicy {
    /// Allow (or refuse) connectors that run a program on this host.
    #[must_use]
    pub fn allow_subprocess(mut self, allow: bool) -> Self {
        self.allow_subprocess = allow;
        self
    }

    /// Whether connectors that run a program on this host may be built.
    pub fn allows_subprocess(&self) -> bool {
        self.allow_subprocess
    }

    /// Every connector may be built: for a host whose configs are trusted, as
    /// the `faucet` CLI treats a config file it was pointed at.
    pub fn trusted() -> Self {
        Self::default().allow_subprocess(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_refuses_subprocess_and_trusted_allows_it() {
        assert!(!BuildPolicy::default().allows_subprocess());
        assert!(BuildPolicy::trusted().allows_subprocess());
        assert!(
            !BuildPolicy::trusted()
                .allow_subprocess(false)
                .allows_subprocess()
        );
    }
}
