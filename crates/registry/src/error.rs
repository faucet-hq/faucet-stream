//! The registry's error type.

use faucet_core::FaucetError;

/// `Result` with a [`RegistryError`].
pub type RegistryResult<T> = Result<T, RegistryError>;

/// Why a connector could not be looked up, validated or built.
///
/// The `Display` text of each variant is stable wording the `faucet` CLI
/// prints unchanged. Match with a `_` arm: variants and fields may be added in
/// a minor release.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RegistryError {
    /// The named connector is unknown, or its feature is not enabled in this
    /// build.
    #[error("unknown {kind} '{name}'. Available: {available}")]
    UnknownConnector {
        /// `"source"` or `"sink"`.
        kind: &'static str,
        /// The `type:` that was asked for.
        name: String,
        /// The compiled-in kinds of that role, comma-separated.
        available: String,
    },

    /// A connector config failed to deserialize or failed its own checks.
    #[error("invalid config for {kind} '{name}': {message}")]
    InvalidConnectorConfig {
        /// `"source"`, `"sink"`, or the connector kind for a config check.
        kind: &'static str,
        /// The connector (or caller-chosen) name the config belongs to.
        name: String,
        /// What is wrong. Values from the config are redacted: only key and
        /// type names appear.
        message: String,
        /// Where in the config the deserializer stopped, as a dotted path
        /// (`auth.config.token`), when it is known. Never part of the
        /// rendered message.
        path: Option<String>,
    },

    /// A connector's `auth: { ref }` named a provider the catalog does not
    /// hold.
    #[error(
        "auth references unknown provider '{name}'. Declared providers: {}",
        if known.is_empty() { String::from("(none)") } else { known.join(", ") }
    )]
    UnknownAuthProvider {
        /// The referenced provider name.
        name: String,
        /// The catalog's provider names, sorted.
        known: Vec<String>,
    },

    /// An `auth:` provider spec failed to build.
    #[error("failed to build auth provider '{name}': {message}")]
    AuthProviderBuild {
        /// The provider's name in the catalog.
        name: String,
        /// Why it failed.
        message: String,
    },

    /// The connector runs a program on this host and the build was not
    /// allowed to (see [`crate::BuildPolicy::allow_subprocess`]).
    #[error(
        "{role} '{kind}' runs a program on this host; building it needs an explicit opt-in \
         (BuildPolicy::allow_subprocess)"
    )]
    SubprocessRefused {
        /// `"source"` or `"sink"`.
        role: &'static str,
        /// The connector kind.
        kind: String,
    },

    /// A registry-level failure not covered by a more specific variant (a
    /// plugin name collision, a second [`crate::PluginRegistry::install`]).
    #[error("config error: {0}")]
    Config(String),

    /// A connector's constructor failed.
    #[error(transparent)]
    Faucet(#[from] FaucetError),

    /// An error from a plugin factory that is neither of the above. Its
    /// `Display` is the wrapped error's.
    #[error(transparent)]
    Other(Box<dyn std::error::Error + Send + Sync>),
}

impl RegistryError {
    /// Wrap any error a plugin factory produced.
    pub fn other(e: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self::Other(e.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn variants_render_their_stable_text() {
        let e = RegistryError::UnknownConnector {
            kind: "source",
            name: "x".into(),
            available: "a, b".into(),
        };
        assert_eq!(e.to_string(), "unknown source 'x'. Available: a, b");
        let e = RegistryError::InvalidConnectorConfig {
            kind: "sink",
            name: "out".into(),
            message: "missing field `path`".into(),
            path: Some("path".into()),
        };
        assert_eq!(
            e.to_string(),
            "invalid config for sink 'out': missing field `path`"
        );
        let e = RegistryError::SubprocessRefused {
            role: "sink",
            kind: "singer".into(),
        };
        assert!(e.to_string().starts_with("sink 'singer' runs a program"));
        assert_eq!(
            RegistryError::Config("c".into()).to_string(),
            "config error: c"
        );
        assert_eq!(
            RegistryError::from(FaucetError::Config("bad".into())).to_string(),
            FaucetError::Config("bad".into()).to_string()
        );
        let e = RegistryError::other(std::io::Error::other("plugin failed"));
        assert_eq!(e.to_string(), "plugin failed");
    }
}
