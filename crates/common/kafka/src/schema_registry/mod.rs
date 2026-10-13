//! Confluent Schema Registry client and wire-envelope handling.
//!
//! All four moving pieces live behind the `schema-registry` Cargo feature:
//!
//! - [`SchemaRegistryConfig`] — connection settings.
//! - [`envelope`] — encode/decode the Confluent wire envelope
//!   `[magic_byte: 0x00][schema_id: be u32][payload bytes...]`.
//! - [`client::SchemaRegistryClient`] — HTTP client with an LRU schema cache.
//! - The per-format codecs (`avro`, `protobuf`, `json_schema`) live in
//!   sibling modules.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::BasicAuth;

pub mod avro;
pub mod client;
pub mod envelope;
pub mod json_schema;
pub mod protobuf;

/// Confluent Schema Registry connection settings.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SchemaRegistryConfig {
    #[schemars(extend("x-faucet-resource" = "url"))]
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<BasicAuth>,
    #[serde(default = "default_cache_capacity")]
    pub cache_capacity: usize,
    #[serde(
        default = "default_request_timeout",
        with = "faucet_core::config::duration_secs"
    )]
    #[schemars(with = "u64")]
    pub request_timeout: Duration,
}

fn default_cache_capacity() -> usize {
    1024
}

fn default_request_timeout() -> Duration {
    Duration::from_secs(10)
}

impl SchemaRegistryConfig {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            auth: None,
            cache_capacity: default_cache_capacity(),
            request_timeout: default_request_timeout(),
        }
    }

    pub fn validate(&self) -> Result<(), faucet_core::FaucetError> {
        let parsed = url::Url::parse(&self.url).map_err(|e| {
            faucet_core::FaucetError::Config(format!(
                "schema_registry.url '{}' is not a valid URL: {e}",
                self.url
            ))
        })?;
        match parsed.scheme() {
            "http" | "https" => {}
            other => {
                return Err(faucet_core::FaucetError::Config(format!(
                    "schema_registry.url scheme must be http or https, got '{other}'"
                )));
            }
        }
        if self.cache_capacity == 0 {
            return Err(faucet_core::FaucetError::Config(
                "schema_registry.cache_capacity must be at least 1".into(),
            ));
        }
        Ok(())
    }
}
