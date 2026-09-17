//! Authentication strategies for REST APIs.

pub mod api_key;
pub mod basic;
pub mod bearer;
pub mod custom;
pub mod oauth2;
pub mod token_endpoint;

use faucet_core::FaucetError;
use reqwest::header::HeaderMap;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
pub use token_endpoint::ResponseValidator;

/// Body encoding for the token-endpoint request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum TokenBodyEncoding {
    /// JSON body (`application/json`) — the default (back-compat).
    #[default]
    Json,
    /// Form-urlencoded body (`application/x-www-form-urlencoded`) — required by
    /// RFC-6749 OAuth token endpoints (Salesforce, Google, most OAuth servers),
    /// which reject a JSON body with `unsupported_grant_type`.
    Form,
}

/// Supported authentication methods.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum Auth {
    None,
    /// Bearer token in the `Authorization` header.
    Bearer {
        token: String,
    },
    Basic {
        username: String,
        password: String,
    },
    /// API key sent in a request header.
    ApiKey {
        header: String,
        value: String,
    },
    /// API key sent as a query parameter (e.g. `?api_key=secret`).
    ///
    /// Some APIs require the key in the URL rather than a header. The `param`
    /// field is the query parameter name, and `value` is the key itself.
    ApiKeyQuery {
        param: String,
        value: String,
    },
    #[serde(rename = "oauth2")]
    OAuth2 {
        token_url: String,
        client_id: String,
        client_secret: String,
        scopes: Vec<String>,
        /// Fraction of `expires_in` after which the cached token is considered
        /// expired and a new one is fetched. Must be in `(0.0, 1.0]`.
        /// Defaults to `0.9` (refresh after 90 % of the token lifetime).
        expiry_ratio: f64,
    },
    /// Fetch a token from an arbitrary HTTP endpoint.
    ///
    /// The endpoint is called, the token is extracted from the JSON response
    /// using `token_path` (a JSONPath expression), and then used as a Bearer
    /// token (or in a custom header if `header_name` is set).
    ///
    /// Tokens are cached and refreshed automatically when `expiry_path`
    /// is provided and the server returns an expiry value.
    TokenEndpoint {
        /// URL of the token endpoint.
        url: String,
        /// HTTP method for the token request (e.g. GET, POST).
        #[serde(with = "crate::serde_helpers::http_method")]
        #[schemars(with = "String")]
        method: reqwest::Method,
        /// Headers to send with the token request (e.g. API keys, content type).
        #[serde(skip, default)]
        headers: HeaderMap,
        /// Optional JSON body for the token request.
        body: Option<serde_json::Value>,
        /// JSONPath expression to extract the token string from the response.
        token_path: String,
        /// Optional JSONPath expression to extract the expiry (in seconds)
        /// from the response. When absent, the token is cached indefinitely.
        expiry_path: Option<String>,
        /// Fraction of the expiry after which the token is proactively refreshed.
        /// Must be in `(0.0, 1.0]`. Defaults to `0.9`.
        expiry_ratio: f64,
        /// Body encoding: `json` (default) or `form`
        /// (`application/x-www-form-urlencoded`, required by RFC-6749 OAuth
        /// token endpoints like Salesforce/Google).
        #[serde(default)]
        encoding: TokenBodyEncoding,
        /// Optional callback to decide whether the token endpoint response is
        /// successful. Receives the HTTP status code. When `None`, defaults to
        /// `status.is_success()` (2xx).
        #[serde(skip, default)]
        response_validator: Option<ResponseValidator>,
    },
    /// Arbitrary headers attached to every request (e.g. multi-tenant routing,
    /// API keys split across several headers).
    Custom {
        headers: HashMap<String, String>,
    },
}

impl Auth {
    /// Apply header-based auth to the request headers.
    ///
    /// `ApiKeyQuery` is a no-op here — it is applied as a query parameter by
    /// `RestStream::execute_request` instead.
    pub fn apply(&self, headers: &mut HeaderMap) -> Result<(), FaucetError> {
        match self {
            Auth::None | Auth::ApiKeyQuery { .. } => Ok(()),
            Auth::Bearer { token } => bearer::apply(headers, token),
            Auth::Basic { username, password } => basic::apply(headers, username, password),
            Auth::ApiKey { header, value } => api_key::apply(headers, header, value),
            // OAuth2 is resolved to Auth::Bearer by RestStream before apply() is called.
            // If apply() is reached with an OAuth2 variant, it means the caller bypassed
            // RestStream — return a clear error rather than silently sending no auth.
            Auth::OAuth2 { .. } => Err(FaucetError::Auth(
                "OAuth2 auth must be resolved to a bearer token before applying; \
                 use RestStream (which resolves it automatically) or call \
                 fetch_oauth2_token() and construct Auth::Bearer { token } directly"
                    .into(),
            )),
            // TokenEndpoint is resolved to Auth::Bearer by RestStream before apply().
            Auth::TokenEndpoint { .. } => Err(FaucetError::Auth(
                "TokenEndpoint auth must be resolved to a bearer token before applying; \
                 use RestStream (which resolves it automatically) or call \
                 fetch_token_from_endpoint() and construct Auth::Bearer { token } directly"
                    .into(),
            )),
            Auth::Custom { headers: extra } => custom::apply(headers, extra),
        }
    }
}

pub use oauth2::fetch_oauth2_token;
pub use token_endpoint::fetch_token_from_endpoint;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_serializes_as_type_config() {
        let a = Auth::Bearer { token: "t".into() };
        let v = serde_json::to_value(&a).unwrap();
        assert_eq!(
            v,
            serde_json::json!({"type": "bearer", "config": {"token": "t"}})
        );
        let back: Auth = serde_json::from_value(v).unwrap();
        assert!(matches!(back, Auth::Bearer { token } if token == "t"));
    }

    #[test]
    fn auth_unit_variant_has_no_config() {
        let v = serde_json::to_value(Auth::None).unwrap();
        assert_eq!(v, serde_json::json!({"type": "none"}));
    }

    #[test]
    fn auth_snake_case_discriminators() {
        let a = Auth::ApiKey {
            header: "X-Key".into(),
            value: "v".into(),
        };
        let v = serde_json::to_value(&a).unwrap();
        assert_eq!(v["type"], "api_key");
        assert_eq!(v["config"]["header"], "X-Key");
    }

    #[test]
    fn auth_none_is_noop() {
        let mut headers = HeaderMap::new();
        Auth::None.apply(&mut headers).unwrap();
        assert!(headers.is_empty());
    }

    #[test]
    fn auth_bearer_sets_authorization_header() {
        let mut headers = HeaderMap::new();
        Auth::Bearer {
            token: "my-token".into(),
        }
        .apply(&mut headers)
        .unwrap();
        assert_eq!(headers.get("authorization").unwrap(), "Bearer my-token");
    }

    #[test]
    fn auth_basic_sets_authorization_header() {
        let mut headers = HeaderMap::new();
        Auth::Basic {
            username: "user".into(),
            password: "pass".into(),
        }
        .apply(&mut headers)
        .unwrap();
        let value = headers.get("authorization").unwrap().to_str().unwrap();
        assert!(value.starts_with("Basic "));
    }

    #[test]
    fn auth_api_key_sets_custom_header() {
        let mut headers = HeaderMap::new();
        Auth::ApiKey {
            header: "X-Api-Key".into(),
            value: "secret".into(),
        }
        .apply(&mut headers)
        .unwrap();
        assert_eq!(headers.get("x-api-key").unwrap(), "secret");
    }

    #[test]
    fn auth_api_key_query_is_noop_on_apply() {
        let mut headers = HeaderMap::new();
        Auth::ApiKeyQuery {
            param: "api_key".into(),
            value: "secret".into(),
        }
        .apply(&mut headers)
        .unwrap();
        assert!(headers.is_empty());
    }

    #[test]
    fn auth_oauth2_errors_on_direct_apply() {
        let mut headers = HeaderMap::new();
        let result = Auth::OAuth2 {
            token_url: "https://auth.example.com/token".into(),
            client_id: "id".into(),
            client_secret: "secret".into(),
            scopes: vec![],
            expiry_ratio: 0.9,
        }
        .apply(&mut headers);
        assert!(result.is_err());
        assert!(matches!(result, Err(FaucetError::Auth(_))));
    }

    #[test]
    fn auth_token_endpoint_errors_on_direct_apply() {
        let mut headers = HeaderMap::new();
        let result = Auth::TokenEndpoint {
            url: "https://auth.example.com/token".into(),
            method: reqwest::Method::POST,
            headers: HeaderMap::new(),
            body: None,
            token_path: "$.token".into(),
            expiry_path: None,
            expiry_ratio: 0.9,
            encoding: TokenBodyEncoding::default(),
            response_validator: None,
        }
        .apply(&mut headers);
        assert!(result.is_err());
        assert!(matches!(result, Err(FaucetError::Auth(_))));
    }

    #[test]
    fn auth_custom_headers() {
        let mut headers = HeaderMap::new();
        let custom = Auth::Custom {
            headers: [("x-custom".to_string(), "value".to_string())]
                .into_iter()
                .collect(),
        };
        custom.apply(&mut headers).unwrap();
        assert_eq!(headers.get("x-custom").unwrap(), "value");
    }

    #[test]
    fn auth_custom_round_trips_through_json() {
        let auth = Auth::Custom {
            headers: [
                ("x-tenant".to_string(), "acme".to_string()),
                ("x-region".to_string(), "us".to_string()),
            ]
            .into_iter()
            .collect(),
        };
        let json = serde_json::to_value(&auth).unwrap();
        let restored: Auth = serde_json::from_value(json).unwrap();
        let mut headers = HeaderMap::new();
        restored.apply(&mut headers).unwrap();
        assert_eq!(headers.get("x-tenant").unwrap(), "acme");
        assert_eq!(headers.get("x-region").unwrap(), "us");
    }

    #[test]
    fn auth_bearer_round_trips_through_json() {
        let auth = Auth::Bearer {
            token: "tok".into(),
        };
        let json = serde_json::to_value(&auth).unwrap();
        let restored: Auth = serde_json::from_value(json).unwrap();
        let mut headers = HeaderMap::new();
        restored.apply(&mut headers).unwrap();
        assert_eq!(headers.get("authorization").unwrap(), "Bearer tok");
    }

    #[test]
    fn auth_debug_format() {
        let auth = Auth::None;
        assert_eq!(format!("{auth:?}"), "None");

        let auth = Auth::Bearer {
            token: "tok".into(),
        };
        let debug = format!("{auth:?}");
        assert!(debug.contains("Bearer"));
    }

    #[test]
    fn auth_clone() {
        let auth = Auth::Bearer {
            token: "token".into(),
        };
        let cloned = auth.clone();
        let mut h = HeaderMap::new();
        cloned.apply(&mut h).unwrap();
        assert_eq!(h.get("authorization").unwrap(), "Bearer token");
    }
}
