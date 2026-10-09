//! Configuration for the acme source. Pure data and validation, no I/O.

use faucet_core::{FaucetError, JsonSchema};
use serde::{Deserialize, Serialize};

/// Config for the acme source, deserialized from the `source.config:` block of
/// a `faucet.yaml` pipeline.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AcmeSourceConfig {
    /// Base URL of the acme API, e.g. `https://acme.example.com/api`.
    pub base_url: String,
    /// API token. Supply it as `${env:ACME_TOKEN}`; never commit a literal.
    pub token: String,
    /// Collection to read.
    pub collection: String,
    /// Field that orders records and resumes the next run. Must be unique and
    /// strictly increasing.
    #[serde(default = "default_cursor_field")]
    pub cursor_field: String,
    /// Records per request. When set it overrides the pipeline's batch-size
    /// hint; `0` reads everything into a single page.
    #[serde(default)]
    pub page_size: Option<usize>,
    /// Per-request timeout in seconds.
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
    /// Retries for 429, 5xx and connection errors on each request.
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    /// Overrides the default state key (`acme:<collection>`).
    #[serde(default)]
    pub state_key: Option<String>,
}

fn default_cursor_field() -> String {
    "id".into()
}

fn default_timeout_secs() -> u64 {
    30
}

fn default_max_retries() -> u32 {
    3
}

impl AcmeSourceConfig {
    /// Minimal config with every optional field at its default.
    pub fn new(
        base_url: impl Into<String>,
        token: impl Into<String>,
        collection: impl Into<String>,
    ) -> Self {
        Self {
            base_url: base_url.into(),
            token: token.into(),
            collection: collection.into(),
            cursor_field: default_cursor_field(),
            page_size: None,
            timeout_secs: default_timeout_secs(),
            max_retries: default_max_retries(),
            state_key: None,
        }
    }

    pub fn with_page_size(mut self, page_size: usize) -> Self {
        self.page_size = Some(page_size);
        self
    }

    /// Fail fast at construction so `faucet validate` reports bad config
    /// before any data moves.
    pub fn validate(&self) -> Result<(), FaucetError> {
        if self.collection.trim().is_empty() {
            return Err(FaucetError::Config(
                "acme: `collection` must not be empty".into(),
            ));
        }
        if self.cursor_field.trim().is_empty() {
            return Err(FaucetError::Config(
                "acme: `cursor_field` must not be empty".into(),
            ));
        }
        if self.timeout_secs == 0 {
            return Err(FaucetError::Config(
                "acme: `timeout_secs` must be > 0".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn state_key(&self) -> String {
        self.state_key
            .clone()
            .unwrap_or_else(|| format!("acme:{}", self.collection))
    }
}

impl std::fmt::Debug for AcmeSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AcmeSourceConfig")
            .field("base_url", &self.base_url)
            .field("token", &"<redacted>")
            .field("collection", &self.collection)
            .field("cursor_field", &self.cursor_field)
            .field("page_size", &self.page_size)
            .field("timeout_secs", &self.timeout_secs)
            .field("max_retries", &self.max_retries)
            .field("state_key", &self.state_key)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_collection() {
        let err = AcmeSourceConfig::new("http://x", "t", " ")
            .validate()
            .unwrap_err();
        assert!(matches!(err, FaucetError::Config(_)), "{err:?}");
    }

    #[test]
    fn debug_never_prints_the_token() {
        let cfg = AcmeSourceConfig::new("http://x", "s3cr3t", "orders");
        assert!(!format!("{cfg:?}").contains("s3cr3t"));
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let v = faucet_core::json!({"base_url": "http://x", "token": "t", "collection": "c", "typo": 1});
        assert!(faucet_core::serde_json::from_value::<AcmeSourceConfig>(v).is_err());
    }
}
