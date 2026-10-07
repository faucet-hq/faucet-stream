#![cfg_attr(docsrs, feature(doc_cfg))]
//! Shared, single-flight authentication providers for faucet-stream.
//!
//! These implement [`faucet_core::AuthProvider`] — a live entity that owns a
//! token cache and refresh lifecycle. One instance, wrapped in an [`Arc`], is
//! shared across every connector that references it (via the CLI `auth:` catalog
//! and `auth: { ref }`, or by a library caller cloning the `Arc`), so N
//! connectors hitting one identity provider share a single token with
//! single-flight refresh instead of racing.
//!
//! Providers:
//! - [`StaticProvider`] — a fixed, pre-minted credential.
//! - [`OAuth2ClientCredentialsProvider`] — OAuth2 `client_credentials` grant.
//! - [`OAuth2RefreshProvider`] — OAuth2 `refresh_token` grant with rotation
//!   capture (the headline: a single active access token + rotating refresh
//!   token, shared safely).
//! - [`TokenEndpointProvider`] — fetch a token from an arbitrary HTTP endpoint
//!   and extract it via JSONPath.
//!
//! [`build_provider`] constructs one from a `{ type, config }` spec (the shape
//! used by the CLI's top-level `auth:` block).
//!
//! [`Arc`]: std::sync::Arc

mod flow;
#[cfg(feature = "google-sa")]
mod google_sa;
#[cfg(feature = "oauth1")]
mod oauth1;
mod oauth2;
mod private_store;
mod retry;
mod static_provider;
mod token_endpoint;

use std::sync::Arc;
use std::time::Duration;

use faucet_core::{FaucetError, SharedAuthProvider};
use serde_json::Value;

/// Build the HTTP client the auth providers use, with a bounded request timeout.
///
/// Providers hold a single-flight mutex across the token-fetch network call, so
/// a hung or unreachable IdP with no timeout would wedge that mutex — and thus
/// every connector sharing the provider — indefinitely. A bounded timeout lets
/// the fetch fail and release the lock so callers can retry (audit #146 H11).
pub(crate) fn auth_http_client() -> reqwest::Client {
    const AUTH_HTTP_TIMEOUT: Duration = Duration::from_secs(30);
    reqwest::Client::builder()
        .timeout(AUTH_HTTP_TIMEOUT)
        .build()
        .unwrap_or_else(|_| reqwest::Client::new())
}

pub use flow::{FLOW_SESSION_KEY, FlowProvider};
#[cfg(feature = "google-sa")]
pub use google_sa::{GoogleServiceAccountProvider, JWT_BEARER_GRANT};
#[cfg(feature = "oauth1")]
pub use oauth1::OAuth1Provider;
pub use oauth2::{OAuth2ClientCredentialsProvider, OAuth2RefreshProvider};
pub use static_provider::StaticProvider;
pub use token_endpoint::TokenEndpointProvider;

/// Default fraction of `expires_in` after which a token is proactively
/// refreshed. A token with `expires_in = 3600` is refreshed after 3240 s.
pub const DEFAULT_EXPIRY_RATIO: f64 = 0.9;

/// Build a shared [`AuthProvider`](faucet_core::AuthProvider) from a
/// `{ type, config }` spec — the shape used by the CLI's top-level `auth:`
/// catalog.
///
/// Supported `type` values: `flow` (composable multi-step, #511), `static`,
/// `oauth2` (client-credentials), `oauth2_refresh`, `token_endpoint`,
/// `google_service_account` (RFC 7523 JWT-bearer, `google-sa` feature), `oauth1`.
pub fn build_provider(spec: &Value) -> Result<SharedAuthProvider, FaucetError> {
    let kind = spec
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| FaucetError::Config("auth provider: missing `type`".into()))?;
    let config = spec.get("config").cloned().unwrap_or(Value::Null);

    match kind {
        "flow" => Ok(Arc::new(FlowProvider::from_config(&config)?)),
        "static" => Ok(Arc::new(StaticProvider::from_config(&config)?)),
        "oauth2" => Ok(Arc::new(OAuth2ClientCredentialsProvider::from_config(
            &config,
        )?)),
        "oauth2_refresh" => Ok(Arc::new(OAuth2RefreshProvider::from_config(&config)?)),
        "token_endpoint" => Ok(Arc::new(TokenEndpointProvider::from_config(&config)?)),
        "google_service_account" => {
            #[cfg(feature = "google-sa")]
            {
                Ok(Arc::new(GoogleServiceAccountProvider::from_config(
                    &config,
                )?))
            }
            #[cfg(not(feature = "google-sa"))]
            {
                Err(FaucetError::Config(
                    "auth provider: `google_service_account` requires the `google-sa` feature — \
                     rebuild with `--features google-sa`"
                        .into(),
                ))
            }
        }
        "oauth1" => {
            #[cfg(feature = "oauth1")]
            {
                Ok(Arc::new(OAuth1Provider::from_config(&config)?))
            }
            #[cfg(not(feature = "oauth1"))]
            {
                Err(FaucetError::Config(
                    "auth provider: `oauth1` requires the `oauth1` feature — rebuild with \
                     `--features oauth1` (e.g. `cargo install faucet-cli --features oauth1`)"
                        .into(),
                ))
            }
        }
        other => Err(FaucetError::Config(format!(
            "auth provider: unknown type `{other}` (expected one of: flow, static, oauth2, oauth2_refresh, token_endpoint, google_service_account, oauth1)"
        ))),
    }
}

/// Compute the instant at which a token fetched now (with the given
/// server-reported `expires_in`, in seconds) should be treated as expired,
/// applying `expiry_ratio`. Returns `None` when the server gave no expiry.
pub(crate) fn expiry_instant(
    expires_in: Option<u64>,
    expiry_ratio: f64,
) -> Option<tokio::time::Instant> {
    expires_in.and_then(|secs| {
        let effective = (secs as f64 * expiry_ratio) as u64;
        // An absurd lifetime overflows `Instant`; treat it as "no expiry".
        tokio::time::Instant::now().checked_add(std::time::Duration::from_secs(effective))
    })
}

/// Parse and validate the optional `expiry_ratio` config field, shared by every
/// provider that caches a token. Must be a finite number in `(0, 1]`; defaults
/// to [`DEFAULT_EXPIRY_RATIO`] when absent or null.
///
/// Out-of-range values silently break token caching (#146 M16): `≤ 0` or `NaN`
/// makes the effective expiry `0`, so every call refetches (defeating the cache
/// and single-flight refresh); `> 1` treats the token as valid past its real
/// expiry, causing 401s mid-use. Rejecting at construction surfaces the mistake
/// at config-load time instead.
pub(crate) fn parse_expiry_ratio(config: &Value) -> Result<f64, FaucetError> {
    match config.get("expiry_ratio") {
        None | Some(Value::Null) => Ok(DEFAULT_EXPIRY_RATIO),
        Some(v) => {
            let r = v.as_f64().ok_or_else(|| {
                FaucetError::Config(format!(
                    "auth provider: `expiry_ratio` must be a number in (0, 1], got {v}"
                ))
            })?;
            if !r.is_finite() || r <= 0.0 || r > 1.0 {
                return Err(FaucetError::Config(format!(
                    "auth provider: `expiry_ratio` must be a finite number in (0, 1], got {r}"
                )));
            }
            Ok(r)
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn an_absurd_expiry_means_no_expiry_rather_than_a_panic() {
        assert!(super::expiry_instant(Some(u64::MAX), 1.0).is_none());
        assert!(super::expiry_instant(Some(3600), 0.9).is_some());
        assert!(super::expiry_instant(None, 0.9).is_none());
    }

    use super::*;

    #[test]
    fn build_provider_static() {
        let spec = serde_json::json!({
            "type": "static",
            "config": { "token": "abc" }
        });
        let p = build_provider(&spec).unwrap();
        assert_eq!(p.provider_name(), "static");
    }

    #[test]
    fn build_provider_unknown_type_errors() {
        let spec = serde_json::json!({ "type": "magic", "config": {} });
        let err = build_provider(&spec).unwrap_err();
        assert!(matches!(err, FaucetError::Config(_)));
    }

    #[test]
    fn build_provider_missing_type_errors() {
        let spec = serde_json::json!({ "config": {} });
        assert!(build_provider(&spec).is_err());
    }

    #[test]
    fn parse_expiry_ratio_validates_range() {
        use serde_json::json;
        // Absent / null → default.
        assert_eq!(
            parse_expiry_ratio(&json!({})).unwrap(),
            DEFAULT_EXPIRY_RATIO
        );
        assert_eq!(
            parse_expiry_ratio(&json!({ "expiry_ratio": null })).unwrap(),
            DEFAULT_EXPIRY_RATIO
        );
        // In-range values pass.
        assert_eq!(
            parse_expiry_ratio(&json!({ "expiry_ratio": 0.5 })).unwrap(),
            0.5
        );
        assert_eq!(
            parse_expiry_ratio(&json!({ "expiry_ratio": 1.0 })).unwrap(),
            1.0
        );
        // Out-of-range / non-numeric are rejected (#146 M16).
        assert!(parse_expiry_ratio(&json!({ "expiry_ratio": 0 })).is_err());
        assert!(parse_expiry_ratio(&json!({ "expiry_ratio": -0.5 })).is_err());
        assert!(parse_expiry_ratio(&json!({ "expiry_ratio": 1.5 })).is_err());
        assert!(parse_expiry_ratio(&json!({ "expiry_ratio": "0.5" })).is_err());
    }

    #[test]
    fn build_provider_rejects_out_of_range_expiry_ratio() {
        let spec = serde_json::json!({
            "type": "oauth2",
            "config": {
                "token_url": "http://x", "client_id": "id",
                "client_secret": "sec", "expiry_ratio": 2.0
            }
        });
        assert!(build_provider(&spec).is_err());
    }
}
