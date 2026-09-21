//! HTTP sink configuration.

use faucet_core::{AuthSpec, DEFAULT_BATCH_SIZE};
use reqwest::header::HeaderMap;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Authentication method for the HTTP sink.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum HttpSinkAuth {
    /// No authentication.
    None,
    /// Bearer token in the Authorization header.
    Bearer {
        /// The token value (sent as `Authorization: Bearer <token>`).
        token: String,
    },
    /// HTTP Basic authentication.
    Basic {
        /// Username.
        username: String,
        /// Password.
        password: String,
    },
    /// Custom headers for authentication (e.g. API keys).
    Custom {
        /// Header name → value map applied to every request.
        headers: HashMap<String, String>,
    },
}

impl std::fmt::Debug for HttpSinkAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "None"),
            Self::Bearer { .. } => f.debug_tuple("Bearer").field(&"***").finish(),
            Self::Basic { username, .. } => f
                .debug_struct("Basic")
                .field("username", username)
                .field("password", &"***")
                .finish(),
            Self::Custom { .. } => f.debug_tuple("Custom").field(&"***").finish(),
        }
    }
}

/// How records are sent in HTTP requests.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type")]
pub enum HttpBatchMode {
    /// Send one HTTP request per record.
    Individual,
    /// Send all records as a JSON array in a single request.
    Array,
}

/// Configuration for the HTTP sink connector.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HttpSinkConfig {
    /// Target endpoint URL.
    pub url: String,
    /// HTTP method (default: POST).
    #[serde(with = "crate::serde_helpers::http_method")]
    #[schemars(with = "String")]
    pub method: reqwest::Method,
    /// Additional request headers.
    #[serde(skip, default)]
    pub headers: HeaderMap,
    /// Authentication: either inline (`{ type, config }`) or a `{ ref: <name> }`
    /// pointer to a shared provider in the CLI's top-level `auth:` catalog.
    pub auth: AuthSpec<HttpSinkAuth>,
    /// How to batch records in requests.
    pub batch_mode: HttpBatchMode,
    /// Number of retries on transient failures (default: 0).
    pub max_retries: usize,
    /// Maximum number of concurrent requests in Individual mode (default: 10).
    pub concurrency: usize,
    /// Maximum number of records sent per outbound HTTP request. Defaults to
    /// [`DEFAULT_BATCH_SIZE`] (1000).
    ///
    /// Interpretation depends on [`batch_mode`](Self::batch_mode):
    ///
    /// - In [`HttpBatchMode::Array`] mode, `write_batch` re-chunks the
    ///   incoming slice into `batch_size`-row chunks and issues one POST
    ///   request per chunk, with each request body a JSON array of up to
    ///   `batch_size` records. `batch_size = 0` is the **"no batching"
    ///   sentinel**: the entire upstream `StreamPage` is forwarded as a
    ///   single JSON array — useful when the source already chunks to a
    ///   size the destination endpoint accepts.
    /// - In [`HttpBatchMode::Individual`] mode the sink already sends one
    ///   request per record, so `batch_size` has no effect on wire framing;
    ///   the field is accepted for config-shape parity with other sinks
    ///   and validated via [`faucet_core::validate_batch_size`] at load
    ///   time.
    ///
    /// Recommended value for HTTP POST endpoints that accept arrays:
    /// match the destination's documented batch limit (commonly 100–1000
    /// records per request).
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

impl std::fmt::Debug for HttpSinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpSinkConfig")
            .field("url", &self.url)
            .field("method", &self.method)
            .field("headers", &self.headers)
            .field("auth", &self.auth)
            .field("batch_mode", &self.batch_mode)
            .field("max_retries", &self.max_retries)
            .field("concurrency", &self.concurrency)
            .field("batch_size", &self.batch_size)
            .finish()
    }
}

impl HttpSinkConfig {
    /// Create a new config with the given URL and sensible defaults.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            method: reqwest::Method::POST,
            headers: HeaderMap::new(),
            auth: AuthSpec::Inline(HttpSinkAuth::None),
            batch_mode: HttpBatchMode::Individual,
            max_retries: 0,
            concurrency: 10,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    /// Set the HTTP method.
    pub fn method(mut self, method: reqwest::Method) -> Self {
        self.method = method;
        self
    }

    /// Set additional request headers.
    pub fn headers(mut self, headers: HeaderMap) -> Self {
        self.headers = headers;
        self
    }

    /// Set the authentication method (inline).
    pub fn auth(mut self, auth: HttpSinkAuth) -> Self {
        self.auth = AuthSpec::Inline(auth);
        self
    }

    /// Set the batch mode.
    pub fn batch_mode(mut self, mode: HttpBatchMode) -> Self {
        self.batch_mode = mode;
        self
    }

    /// Set the maximum number of retries.
    pub fn max_retries(mut self, retries: usize) -> Self {
        self.max_retries = retries;
        self
    }

    /// Set the maximum number of concurrent requests in Individual mode.
    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Set the maximum number of records sent per outbound HTTP request.
    ///
    /// In [`HttpBatchMode::Array`] mode this controls how many records are
    /// packed into each POST body. In [`HttpBatchMode::Individual`] mode it
    /// has no effect on wire framing (one request per record either way)
    /// and is accepted only for parity.
    ///
    /// Pass `0` to opt out of re-chunking in Array mode — the entire
    /// records slice handed to `write_batch` is sent as a single JSON
    /// array, preserving upstream `StreamPage` framing.
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
        let config = HttpSinkConfig::new("https://api.example.com/ingest");
        assert_eq!(config.url, "https://api.example.com/ingest");
        assert_eq!(config.method, reqwest::Method::POST);
        assert_eq!(config.max_retries, 0);
        assert!(matches!(config.auth, AuthSpec::Inline(HttpSinkAuth::None)));
        assert!(matches!(config.batch_mode, HttpBatchMode::Individual));
    }

    #[test]
    fn builder_methods() {
        let config = HttpSinkConfig::new("https://api.example.com/ingest")
            .method(reqwest::Method::PUT)
            .auth(HttpSinkAuth::Bearer {
                token: "token123".into(),
            })
            .batch_mode(HttpBatchMode::Array)
            .max_retries(3);
        assert_eq!(config.method, reqwest::Method::PUT);
        assert_eq!(config.max_retries, 3);
        assert!(matches!(config.batch_mode, HttpBatchMode::Array));
    }

    #[test]
    fn batch_size_defaults_to_default_batch_size() {
        let config = HttpSinkConfig::new("https://api.example.com/ingest");
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let config = HttpSinkConfig::new("https://api.example.com/ingest").with_batch_size(250);
        assert_eq!(config.batch_size, 250);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config = HttpSinkConfig::new("https://api.example.com/ingest").with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let config = HttpSinkConfig::new("https://api.example.com/ingest")
            .with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_err());
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let json = r#"{
            "url": "https://api.example.com/ingest",
            "method": "POST",
            "auth": {"type": "none"},
            "batch_mode": {"type": "Array"},
            "max_retries": 0,
            "concurrency": 10,
            "batch_size": 250
        }"#;
        let config: HttpSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, 250);
    }

    #[test]
    fn batch_size_defaults_when_absent_from_json() {
        let json = r#"{
            "url": "https://api.example.com/ingest",
            "method": "POST",
            "auth": {"type": "none"},
            "batch_mode": {"type": "Array"},
            "max_retries": 0,
            "concurrency": 10
        }"#;
        let config: HttpSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn auth_debug_masks_secrets() {
        let bearer = HttpSinkAuth::Bearer {
            token: "secret-token".into(),
        };
        let debug = format!("{bearer:?}");
        assert!(debug.contains("***"));
        assert!(!debug.contains("secret-token"));

        let basic = HttpSinkAuth::Basic {
            username: "user".into(),
            password: "secret-pass".into(),
        };
        let debug = format!("{basic:?}");
        assert!(debug.contains("user"));
        assert!(debug.contains("***"));
        assert!(!debug.contains("secret-pass"));

        let custom = HttpSinkAuth::Custom {
            headers: HashMap::new(),
        };
        let debug = format!("{custom:?}");
        assert!(debug.contains("***"));
    }
}
