//! Error types for faucet-stream.

use std::time::Duration;
use thiserror::Error;

/// All possible errors returned by faucet-stream.
///
/// `#[non_exhaustive]`: this enum demonstrably grows (`QualityFailure`,
/// `CircuitOpen`, `SchemaDrift`, `ContractViolation` all arrived post-1.0) and
/// it is the one type every third-party connector author touches. Marking it
/// keeps the next variant a **minor** release instead of a forced major for
/// `faucet-core` and everything downstream. Match with a `_` arm.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum FaucetError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    /// An HTTP response with a non-success status code.
    ///
    /// Contains the status code, URL, and (truncated) response body for
    /// debugging.  Whether this error is retriable depends on the status code
    /// — see [`FaucetError::is_retriable`].
    #[error("HTTP {status} from {url}: {body}")]
    HttpStatus {
        status: u16,
        url: String,
        body: String,
    },

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("JSONPath error: {0}")]
    JsonPath(String),

    #[error("Auth error: {0}")]
    Auth(String),

    /// The server responded with HTTP 429 Too Many Requests.
    /// The inner value is the duration to wait before retrying,
    /// parsed from the `Retry-After` response header (default: 60 s).
    #[error("Rate limited: retry after {0:?}")]
    RateLimited(Duration),

    /// A URL could not be constructed or parsed.
    #[error("URL error: {0}")]
    Url(String),

    /// A record transform could not be compiled or applied (e.g. invalid regex).
    #[error("Transform error: {0}")]
    Transform(String),

    /// A configuration or validation error (e.g. invalid endpoint, missing descriptor).
    #[error("Config error: {0}")]
    Config(String),

    /// A source operation failed (e.g. database query error, file read error).
    #[error("Source error: {0}")]
    Source(String),

    /// A sink operation failed (e.g. BigQuery insert error).
    #[error("Sink error: {0}")]
    Sink(String),

    /// A data-quality check failed under an `abort` policy.
    #[error("Quality check '{check}' failed: {message}")]
    QualityFailure { check: String, message: String },

    /// An incoming page's shape diverged from the destination schema under an
    /// `on_drift: fail` (or `on_incompatible: fail`) policy.
    #[error("Schema drift on columns {columns:?}: {message}")]
    SchemaDrift {
        columns: Vec<String>,
        message: String,
    },

    /// A run's learned column profile drifted from its baseline under a
    /// `profiling.on_drift: fail` policy (#708). Raised after the run's data
    /// is written: the failure marks the run, it does not undo it.
    #[error("Profile drift on columns {columns:?}: {message}")]
    ProfileDrift {
        columns: Vec<String>,
        message: String,
    },

    /// A record breached the pipeline's data contract under an
    /// `on_breach: fail` policy. `version` is the contract version the
    /// record was validated against.
    #[error("Contract v{version} violated: {message}")]
    ContractViolation { version: String, message: String },

    /// A state-store operation failed (read/write/delete of a replication
    /// bookmark, checkpoint, or other persisted pipeline state).
    #[error("State error: {0}")]
    State(String),

    /// The resilience circuit breaker opened after repeated failures; the run
    /// is aborted fast. `cooldown` is advisory for the orchestration layer
    /// (e.g. `faucet schedule` delays re-entry by this duration).
    #[error("Circuit open after {failures} consecutive failures; cooldown {cooldown:?}")]
    CircuitOpen { failures: u32, cooldown: Duration },

    /// A custom error from a third-party connector.
    ///
    /// Use this to wrap your own error types without losing the error chain:
    /// ```rust
    /// use faucet_core::FaucetError;
    /// let err = FaucetError::Custom(Box::new(std::io::Error::new(
    ///     std::io::ErrorKind::Other,
    ///     "my connector failed",
    /// )));
    /// ```
    #[error("Connector error: {0}")]
    Custom(#[from] Box<dyn std::error::Error + Send + Sync>),
}

impl FaucetError {
    /// Whether this error is transient and the request should be retried.
    ///
    /// Retriable errors:
    /// - Network / connection errors (`Http` from reqwest)
    /// - Server errors (5xx status codes)
    /// - Rate limiting (429 — handled separately with `Retry-After`)
    ///
    /// Non-retriable errors:
    /// - Client errors (4xx except 429)
    /// - JSON parse / JSONPath / auth / transform errors
    pub fn is_retriable(&self) -> bool {
        match self {
            // reqwest errors: connection timeouts, DNS failures, etc. are retriable.
            FaucetError::Http(e) => {
                // If it's a status error that leaked through, check the code.
                if let Some(status) = e.status() {
                    status.is_server_error()
                } else {
                    // Connection errors, timeouts, etc.
                    true
                }
            }
            // 5xx are retriable; 429 (Too Many Requests) is too — sources that
            // surface a rate-limit as a plain HttpStatus rather than the
            // dedicated RateLimited variant (XML, GraphQL) would otherwise abort
            // on the first 429 (audit #146 H3).
            FaucetError::HttpStatus { status, .. } => *status >= 500 || *status == 429,
            FaucetError::RateLimited(_) => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_status_5xx_is_retriable() {
        let err = FaucetError::HttpStatus {
            status: 500,
            url: "https://example.com".into(),
            body: "Internal Server Error".into(),
        };
        assert!(err.is_retriable());

        let err = FaucetError::HttpStatus {
            status: 503,
            url: "https://example.com".into(),
            body: "".into(),
        };
        assert!(err.is_retriable());
    }

    #[test]
    fn http_status_4xx_is_not_retriable() {
        let err = FaucetError::HttpStatus {
            status: 400,
            url: "https://example.com".into(),
            body: "Bad Request".into(),
        };
        assert!(!err.is_retriable());

        let err = FaucetError::HttpStatus {
            status: 404,
            url: "https://example.com".into(),
            body: "".into(),
        };
        assert!(!err.is_retriable());
    }

    #[test]
    fn http_status_429_is_retriable() {
        // H3 (audit #146): a 429 surfaced as a plain HttpStatus (XML/GraphQL
        // sources) must be retriable, not aborted on the first hit.
        let err = FaucetError::HttpStatus {
            status: 429,
            url: "https://example.com".into(),
            body: "Too Many Requests".into(),
        };
        assert!(err.is_retriable());
    }

    #[test]
    fn rate_limited_is_retriable() {
        let err = FaucetError::RateLimited(Duration::from_secs(30));
        assert!(err.is_retriable());
    }

    #[test]
    fn json_error_is_not_retriable() {
        let serde_err = serde_json::from_str::<serde_json::Value>("not json").unwrap_err();
        let err = FaucetError::Json(serde_err);
        assert!(!err.is_retriable());
    }

    #[test]
    fn jsonpath_error_is_not_retriable() {
        let err = FaucetError::JsonPath("bad path".into());
        assert!(!err.is_retriable());
    }

    #[test]
    fn auth_error_is_not_retriable() {
        let err = FaucetError::Auth("invalid token".into());
        assert!(!err.is_retriable());
    }

    #[test]
    fn url_error_is_not_retriable() {
        let err = FaucetError::Url("bad url".into());
        assert!(!err.is_retriable());
    }

    #[test]
    fn transform_error_is_not_retriable() {
        let err = FaucetError::Transform("bad regex".into());
        assert!(!err.is_retriable());
    }

    #[test]
    fn http_status_display_includes_url_and_body() {
        let err = FaucetError::HttpStatus {
            status: 422,
            url: "https://api.example.com/test".into(),
            body: "Unprocessable Entity".into(),
        };
        let msg = err.to_string();
        assert!(msg.contains("422"));
        assert!(msg.contains("https://api.example.com/test"));
        assert!(msg.contains("Unprocessable Entity"));
    }

    #[test]
    fn config_error_is_not_retriable() {
        let err = FaucetError::Config("bad endpoint".into());
        assert!(!err.is_retriable());
    }

    #[test]
    fn config_error_display() {
        let err = FaucetError::Config("missing descriptor".into());
        assert_eq!(err.to_string(), "Config error: missing descriptor");
    }

    #[test]
    fn source_error_is_not_retriable() {
        let err = FaucetError::Source("query failed".into());
        assert!(!err.is_retriable());
    }

    #[test]
    fn source_error_display() {
        let err = FaucetError::Source("connection refused".into());
        assert_eq!(err.to_string(), "Source error: connection refused");
    }

    #[test]
    fn custom_error_is_not_retriable() {
        let err = FaucetError::Custom(Box::new(std::io::Error::other("custom failure")));
        assert!(!err.is_retriable());
    }

    #[test]
    fn custom_error_display() {
        let err = FaucetError::Custom(Box::new(std::io::Error::other("custom failure")));
        assert_eq!(err.to_string(), "Connector error: custom failure");
    }

    #[test]
    fn custom_error_from_boxed() {
        let io_err = std::io::Error::other("file missing");
        let boxed: Box<dyn std::error::Error + Send + Sync> = Box::new(io_err);
        let err: FaucetError = boxed.into();
        assert!(matches!(err, FaucetError::Custom(_)));
    }

    #[test]
    fn sink_error_is_not_retriable() {
        let err = FaucetError::Sink("BigQuery insert failed".into());
        assert!(!err.is_retriable());
    }

    #[test]
    fn sink_error_display() {
        let err = FaucetError::Sink("connection refused".into());
        assert_eq!(err.to_string(), "Sink error: connection refused");
    }

    #[test]
    fn quality_failure_is_not_retriable_and_displays() {
        let err = FaucetError::QualityFailure {
            check: "not_null".into(),
            message: "field 'user_id' was null".into(),
        };
        assert!(!err.is_retriable());
        let s = err.to_string();
        assert!(s.contains("not_null"));
        assert!(s.contains("user_id"));
    }

    #[test]
    fn schema_drift_is_not_retriable_and_displays() {
        let err = FaucetError::SchemaDrift {
            columns: vec!["email".into(), "score".into()],
            message: "2 new columns".into(),
        };
        assert!(!err.is_retriable());
        let s = err.to_string();
        assert!(s.contains("email"));
        assert!(s.contains("2 new columns"));
    }

    #[test]
    fn circuit_open_is_not_retriable_and_displays() {
        let err = FaucetError::CircuitOpen {
            failures: 5,
            cooldown: std::time::Duration::from_secs(60),
        };
        assert!(!err.is_retriable());
        let s = err.to_string();
        assert!(s.contains("5"));
        assert!(s.contains("Circuit open"));
    }
}
