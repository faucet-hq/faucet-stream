#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-common-elasticsearch
//!
//! Shared configuration types for the [`faucet-stream`](https://crates.io/crates/faucet-stream)
//! Elasticsearch source and sink connectors.
//!
//! - [`ElasticsearchAuth`] — authentication modes (None, Basic, Bearer, ApiKey)
//! - [`ElasticsearchTls`] — the `tls:` block (private CA, client certificate,
//!   `insecure_skip_verify`) and [`build_client`], which applies it
//!
//! The enum derives `Serialize`, `Deserialize`, and `JsonSchema` so it round-trips
//! through YAML/JSON configs and CLI introspection. Its `Debug` impl masks
//! credentials (`password`, `token`, `key`) as `"***"` so accidental logging of a
//! config value never leaks secrets.

use faucet_core::{Credential, FaucetError};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The `tls:` block of the Elasticsearch source and sink: a CA that signed
/// the cluster's certificate (`ca_cert_path` / `ca_cert_pem`, used in place
/// of the platform roots — self-managed Elasticsearch 8 ships a self-signed
/// CA), a client certificate for mutual TLS, and `insecure_skip_verify`.
pub type ElasticsearchTls = faucet_core::TlsOptions;

/// Build the HTTP client both connectors use: bounded connect and request
/// timeouts plus `tls`. `connector` prefixes errors (`"elasticsearch source"`).
pub fn build_client(
    connector: &str,
    connect_timeout: std::time::Duration,
    request_timeout: std::time::Duration,
    tls: &ElasticsearchTls,
) -> Result<faucet_core::http::Client, FaucetError> {
    let loaded = tls.load(connector)?;
    faucet_core::http::Client::builder()
        .connect_timeout(connect_timeout)
        .timeout(request_timeout)
        .tls(&loaded)
        .build()
        .map_err(|e| match e {
            FaucetError::Config(m) => FaucetError::Config(format!("{connector}: {m}")),
            other => FaucetError::Config(format!("{connector} HTTP client: {other}")),
        })
}

/// Authentication method for Elasticsearch.
///
/// Serializes as `{ type: <method>, config: { … } }` (adjacent tagging,
/// snake_case discriminators) — the consistent auth wire shape shared by
/// every faucet connector.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum ElasticsearchAuth {
    /// No authentication.
    None,
    /// HTTP Basic authentication.
    Basic { username: String, password: String },
    /// Bearer token authentication.
    Bearer { token: String },
    /// API key authentication (sent as `ApiKey` in the `Authorization` header).
    ApiKey { key: String },
}

/// Map a [`Credential`] from a shared [`faucet_core::AuthProvider`] onto an
/// [`ElasticsearchAuth`] variant.
///
/// Elasticsearch supports `Bearer` and `Basic` credentials. The `Header` and
/// `Token` credential variants have no equivalent Elasticsearch auth mode, so
/// they return [`FaucetError::Auth`].
pub fn credential_to_auth(cred: Credential) -> Result<ElasticsearchAuth, FaucetError> {
    match cred {
        Credential::Bearer(token) => Ok(ElasticsearchAuth::Bearer { token }),
        Credential::Basic { username, password } => {
            Ok(ElasticsearchAuth::Basic { username, password })
        }
        other => Err(FaucetError::Auth(format!(
            "Elasticsearch auth provider must yield a bearer or basic credential, got {other:?}"
        ))),
    }
}

impl std::fmt::Debug for ElasticsearchAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "None"),
            Self::Basic { username, .. } => f
                .debug_struct("Basic")
                .field("username", username)
                .field("password", &"***")
                .finish(),
            Self::Bearer { .. } => write!(f, "Bearer(***)"),
            Self::ApiKey { .. } => write!(f, "ApiKey(***)"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_masks_basic_password() {
        let auth = ElasticsearchAuth::Basic {
            username: "user".into(),
            password: "secret".into(),
        };
        let debug = format!("{auth:?}");
        assert!(debug.contains("user"));
        assert!(debug.contains("***"));
        assert!(!debug.contains("secret"));
    }

    #[test]
    fn debug_masks_bearer_token() {
        let auth = ElasticsearchAuth::Bearer {
            token: "my-token".into(),
        };
        let debug = format!("{auth:?}");
        assert!(debug.contains("***"));
        assert!(!debug.contains("my-token"));
    }

    #[test]
    fn debug_masks_api_key() {
        let auth = ElasticsearchAuth::ApiKey {
            key: "my-key".into(),
        };
        let debug = format!("{auth:?}");
        assert!(debug.contains("***"));
        assert!(!debug.contains("my-key"));
    }

    #[test]
    fn debug_none_renders_unit() {
        let auth = ElasticsearchAuth::None;
        assert_eq!(format!("{auth:?}"), "None");
    }

    #[test]
    fn serde_round_trip_basic() {
        let auth = ElasticsearchAuth::Basic {
            username: "u".into(),
            password: "p".into(),
        };
        let json = serde_json::to_string(&auth).unwrap();
        assert_eq!(
            json,
            r#"{"type":"basic","config":{"username":"u","password":"p"}}"#
        );
        let parsed: ElasticsearchAuth = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, ElasticsearchAuth::Basic { .. }));
    }

    #[test]
    fn serde_round_trip_none() {
        let json = serde_json::to_string(&ElasticsearchAuth::None).unwrap();
        assert_eq!(json, r#"{"type":"none"}"#);
        let parsed: ElasticsearchAuth = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, ElasticsearchAuth::None));
    }

    #[test]
    fn serde_round_trip_bearer() {
        let auth = ElasticsearchAuth::Bearer { token: "t".into() };
        let json = serde_json::to_string(&auth).unwrap();
        assert_eq!(json, r#"{"type":"bearer","config":{"token":"t"}}"#);
        let parsed: ElasticsearchAuth = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, ElasticsearchAuth::Bearer { .. }));
    }

    #[test]
    fn serde_round_trip_api_key() {
        let auth = ElasticsearchAuth::ApiKey { key: "k".into() };
        let json = serde_json::to_string(&auth).unwrap();
        assert_eq!(json, r#"{"type":"api_key","config":{"key":"k"}}"#);
        let parsed: ElasticsearchAuth = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, ElasticsearchAuth::ApiKey { .. }));
    }

    #[test]
    fn build_client_applies_and_reports_tls() {
        let d = std::time::Duration::from_secs(1);
        assert!(build_client("elasticsearch source", d, d, &ElasticsearchTls::default()).is_ok());
        let missing = ElasticsearchTls {
            ca_cert_path: Some("/nonexistent/ca.pem".into()),
            ..Default::default()
        };
        let e = build_client("elasticsearch source", d, d, &missing)
            .unwrap_err()
            .to_string();
        assert!(e.contains("elasticsearch source: cannot read"), "{e}");
        let bad = ElasticsearchTls {
            ca_cert_pem: Some(
                "-----BEGIN CERTIFICATE-----\n!!\n-----END CERTIFICATE-----\n".into(),
            ),
            ..Default::default()
        };
        let e = build_client("elasticsearch sink", d, d, &bad)
            .unwrap_err()
            .to_string();
        assert!(e.contains("elasticsearch sink: tls:"), "{e}");
    }
}
