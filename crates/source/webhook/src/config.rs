//! Webhook source configuration.

use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Configuration for the webhook receiver source.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
#[serde(deny_unknown_fields)]
pub struct WebhookSourceConfig {
    /// Address to bind the HTTP server to (default: `"127.0.0.1:8080"`).
    ///
    /// **Security:** the default binds to loopback only. Binding to
    /// `0.0.0.0` exposes the receiver to the whole network — only do so
    /// behind a trusted gateway, and set `auth_token` to require a shared
    /// secret on every request.
    pub listen_addr: String,
    /// Endpoint path for receiving webhooks (default: `"/webhook"`).
    pub path: String,
    /// Stop after receiving this many payloads.
    pub max_payloads: Option<usize>,
    /// How long to listen before returning, in seconds (default: 30).
    pub timeout_secs: u64,
    /// Maximum accepted request body size in bytes (default: 1 MiB). Larger
    /// POSTs are rejected with `413 Payload Too Large` so a single huge
    /// request can't exhaust memory.
    pub max_body_bytes: usize,
    /// Optional shared secret. When set, every request must carry it in the
    /// `Authorization` header (either the raw token or `Bearer <token>`);
    /// requests without it are rejected with `401 Unauthorized`. When `None`
    /// (default) the endpoint is unauthenticated.
    pub auth_token: Option<String>,
    /// Records per emitted [`StreamPage`](faucet_core::StreamPage). The webhook
    /// source has no native streaming primitive — it accumulates incoming POSTs
    /// into an in-memory buffer during the receive window, then the default
    /// [`Source::stream_pages`](faucet_core::Source::stream_pages) impl chunks
    /// that buffer into pages of this size. Defaults to [`DEFAULT_BATCH_SIZE`].
    ///
    /// `batch_size = 0` is the "no batching" sentinel: the entire flush window
    /// is emitted in a single page. For this source it is functionally
    /// equivalent to any positive value larger than the received payload count
    /// — the server-side buffering behaviour does not change.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

impl Default for WebhookSourceConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:8080".into(),
            path: "/webhook".into(),
            max_payloads: None,
            timeout_secs: 30,
            max_body_bytes: 1024 * 1024,
            auth_token: None,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }
}

impl WebhookSourceConfig {
    /// Create a new config with sensible defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the listen address.
    pub fn listen_addr(mut self, addr: impl Into<String>) -> Self {
        self.listen_addr = addr.into();
        self
    }

    /// Set the webhook endpoint path.
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = path.into();
        self
    }

    /// Stop after receiving this many payloads.
    pub fn max_payloads(mut self, max: usize) -> Self {
        self.max_payloads = Some(max);
        self
    }

    /// Set the timeout in seconds.
    pub fn timeout_secs(mut self, secs: u64) -> Self {
        self.timeout_secs = secs;
        self
    }

    /// Set the maximum accepted request body size in bytes.
    pub fn max_body_bytes(mut self, bytes: usize) -> Self {
        self.max_body_bytes = bytes;
        self
    }

    /// Require a shared-secret token in the `Authorization` header.
    pub fn auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    /// Set the per-page record count for
    /// [`Source::stream_pages`](faucet_core::Source::stream_pages).
    ///
    /// Pass `0` to opt out of batching — the entire flush window is emitted in
    /// a single [`StreamPage`](faucet_core::StreamPage). The webhook source's
    /// server-side buffering is unaffected; only the downstream chunking
    /// changes.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = WebhookSourceConfig::new();
        assert_eq!(
            config.listen_addr, "127.0.0.1:8080",
            "default must bind loopback only, not 0.0.0.0"
        );
        assert_eq!(config.path, "/webhook");
        assert!(config.max_payloads.is_none());
        assert_eq!(config.timeout_secs, 30);
        assert_eq!(config.max_body_bytes, 1024 * 1024);
        assert!(config.auth_token.is_none());
    }

    #[test]
    fn builder_methods() {
        let config = WebhookSourceConfig::new()
            .listen_addr("127.0.0.1:9090")
            .path("/hooks/incoming")
            .max_payloads(10)
            .timeout_secs(60);
        assert_eq!(config.listen_addr, "127.0.0.1:9090");
        assert_eq!(config.path, "/hooks/incoming");
        assert_eq!(config.max_payloads, Some(10));
        assert_eq!(config.timeout_secs, 60);
    }

    #[test]
    fn batch_size_defaults_to_default_batch_size() {
        let config = WebhookSourceConfig::new();
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let config = WebhookSourceConfig::new().with_batch_size(250);
        assert_eq!(config.batch_size, 250);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config = WebhookSourceConfig::new().with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let config = WebhookSourceConfig::new().with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_err());
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let json = r#"{
            "listen_addr": "127.0.0.1:8080",
            "path": "/webhook",
            "timeout_secs": 30,
            "batch_size": 500
        }"#;
        let config: WebhookSourceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, 500);
    }
}
