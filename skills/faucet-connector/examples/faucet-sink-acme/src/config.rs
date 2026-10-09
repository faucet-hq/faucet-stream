//! Configuration for the acme sink. Pure data and validation, no I/O.

use faucet_core::{FaucetError, JsonSchema, WriteMode, WriteSpec};
use serde::{Deserialize, Serialize};

/// Config for the acme sink, deserialized from the `sink.config:` block of a
/// `faucet.yaml` pipeline.
///
/// `write_mode` / `key` come from the flattened [`WriteSpec`], so they sit at
/// the top level of the block like every built-in sink. `deny_unknown_fields`
/// is not used because serde does not support it together with `flatten`.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct AcmeSinkConfig {
    /// Base URL of the acme API, e.g. `https://acme.example.com/api`.
    pub base_url: String,
    /// API token. Supply it as `${env:ACME_TOKEN}`; never commit a literal.
    pub token: String,
    /// Collection to write to.
    pub collection: String,
    /// Most records sent in one request; larger pages are split.
    #[serde(default = "default_max_request_records")]
    pub max_request_records: usize,
    /// Per-request timeout in seconds.
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
    /// `write_mode: append` (default) or `upsert` with a non-empty `key`.
    #[serde(flatten)]
    pub write: WriteSpec,
}

fn default_max_request_records() -> usize {
    500
}

fn default_timeout_secs() -> u64 {
    30
}

impl AcmeSinkConfig {
    pub fn new(
        base_url: impl Into<String>,
        token: impl Into<String>,
        collection: impl Into<String>,
    ) -> Self {
        Self {
            base_url: base_url.into(),
            token: token.into(),
            collection: collection.into(),
            max_request_records: default_max_request_records(),
            timeout_secs: default_timeout_secs(),
            write: WriteSpec::default(),
        }
    }

    /// Switch to keyed upsert on `key`.
    pub fn upsert(mut self, key: &[&str]) -> Self {
        self.write.write_mode = WriteMode::Upsert;
        self.write.key = key.iter().map(|k| k.to_string()).collect();
        self
    }

    pub fn validate(&self) -> Result<(), FaucetError> {
        if self.collection.trim().is_empty() {
            return Err(FaucetError::Config(
                "acme: `collection` must not be empty".into(),
            ));
        }
        if self.max_request_records == 0 {
            return Err(FaucetError::Config(
                "acme: `max_request_records` must be > 0".into(),
            ));
        }
        if self.timeout_secs == 0 {
            return Err(FaucetError::Config(
                "acme: `timeout_secs` must be > 0".into(),
            ));
        }
        self.write.validate()?;
        if !matches!(self.write.write_mode, WriteMode::Append | WriteMode::Upsert) {
            return Err(FaucetError::Config(format!(
                "acme: write_mode `{}` is not supported (use append or upsert)",
                self.write.write_mode.as_str()
            )));
        }
        if self.write.delete_marker.is_some() {
            return Err(FaucetError::Config(
                "acme: `delete_marker` is not supported; the API has no delete endpoint".into(),
            ));
        }
        Ok(())
    }
}

impl std::fmt::Debug for AcmeSinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AcmeSinkConfig")
            .field("base_url", &self.base_url)
            .field("token", &"<redacted>")
            .field("collection", &self.collection)
            .field("max_request_records", &self.max_request_records)
            .field("timeout_secs", &self.timeout_secs)
            .field("write", &self.write)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_without_key_is_a_config_error() {
        let mut cfg = AcmeSinkConfig::new("http://x", "t", "orders");
        cfg.write.write_mode = WriteMode::Upsert;
        assert!(matches!(cfg.validate(), Err(FaucetError::Config(_))));
    }

    #[test]
    fn overwrite_is_rejected() {
        let mut cfg = AcmeSinkConfig::new("http://x", "t", "orders");
        cfg.write.write_mode = WriteMode::Overwrite;
        assert!(matches!(cfg.validate(), Err(FaucetError::Config(_))));
    }

    #[test]
    fn write_mode_is_read_from_the_top_level() {
        let v = faucet_core::json!({
            "base_url": "http://x", "token": "t", "collection": "c",
            "write_mode": "upsert", "key": ["id"]
        });
        let cfg: AcmeSinkConfig = faucet_core::serde_json::from_value(v).unwrap();
        assert!(cfg.write.dedups_by_key());
    }
}
