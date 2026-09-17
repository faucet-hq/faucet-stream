use faucet_source_rest::{
    Auth, DEFAULT_EXPIRY_RATIO, DEFAULT_TOKEN_ENDPOINT_EXPIRY_RATIO, RestStream, RestStreamConfig,
};
use reqwest::header::HeaderMap;

#[test]
fn bearer_auth_sets_header() {
    let mut headers = HeaderMap::new();
    Auth::Bearer {
        token: "test-token".into(),
    }
    .apply(&mut headers)
    .unwrap();
    assert_eq!(headers.get("authorization").unwrap(), "Bearer test-token");
}

#[test]
fn basic_auth_sets_header() {
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
fn api_key_sets_custom_header() {
    let mut headers = HeaderMap::new();
    Auth::ApiKey {
        header: "X-Api-Key".into(),
        value: "secret-123".into(),
    }
    .apply(&mut headers)
    .unwrap();
    assert_eq!(headers.get("x-api-key").unwrap(), "secret-123");
}

#[test]
fn no_auth_leaves_headers_empty() {
    let mut headers = HeaderMap::new();
    Auth::None.apply(&mut headers).unwrap();
    assert!(headers.is_empty());
}

#[test]
fn api_key_query_is_noop_on_headers() {
    let mut headers = HeaderMap::new();
    Auth::ApiKeyQuery {
        param: "api_key".into(),
        value: "secret".into(),
    }
    .apply(&mut headers)
    .unwrap();
    // ApiKeyQuery applies via query params, not headers.
    assert!(headers.is_empty());
}

#[test]
fn custom_auth_merges_headers() {
    let mut headers = HeaderMap::new();
    let custom: std::collections::HashMap<String, String> = [
        ("x-custom-auth".to_string(), "token-123".to_string()),
        ("x-tenant".to_string(), "acme".to_string()),
    ]
    .into_iter()
    .collect();
    Auth::Custom { headers: custom }
        .apply(&mut headers)
        .unwrap();
    assert_eq!(headers.get("x-custom-auth").unwrap(), "token-123");
    assert_eq!(headers.get("x-tenant").unwrap(), "acme");
}

#[test]
fn oauth2_apply_without_resolution_returns_error() {
    let mut headers = HeaderMap::new();
    let result = Auth::OAuth2 {
        token_url: "https://example.com/token".into(),
        client_id: "id".into(),
        client_secret: "secret".into(),
        scopes: vec![],
        expiry_ratio: DEFAULT_EXPIRY_RATIO,
    }
    .apply(&mut headers);
    assert!(result.is_err());
}

#[test]
fn expiry_ratio_zero_rejected() {
    let result = RestStream::new(RestStreamConfig::new("https://example.com", "/api").auth(
        Auth::OAuth2 {
            token_url: "https://example.com/token".into(),
            client_id: "id".into(),
            client_secret: "secret".into(),
            scopes: vec![],
            expiry_ratio: 0.0,
        },
    ));
    assert!(result.is_err());
}

#[test]
fn expiry_ratio_negative_rejected() {
    let result = RestStream::new(RestStreamConfig::new("https://example.com", "/api").auth(
        Auth::OAuth2 {
            token_url: "https://example.com/token".into(),
            client_id: "id".into(),
            client_secret: "secret".into(),
            scopes: vec![],
            expiry_ratio: -0.5,
        },
    ));
    assert!(result.is_err());
}

#[test]
fn expiry_ratio_greater_than_one_rejected() {
    let result = RestStream::new(RestStreamConfig::new("https://example.com", "/api").auth(
        Auth::OAuth2 {
            token_url: "https://example.com/token".into(),
            client_id: "id".into(),
            client_secret: "secret".into(),
            scopes: vec![],
            expiry_ratio: 1.5,
        },
    ));
    assert!(result.is_err());
}

#[test]
fn expiry_ratio_one_accepted() {
    let result = RestStream::new(RestStreamConfig::new("https://example.com", "/api").auth(
        Auth::OAuth2 {
            token_url: "https://example.com/token".into(),
            client_id: "id".into(),
            client_secret: "secret".into(),
            scopes: vec![],
            expiry_ratio: 1.0,
        },
    ));
    assert!(result.is_ok());
}

#[test]
fn token_endpoint_apply_without_resolution_returns_error() {
    let mut headers = HeaderMap::new();
    let result = Auth::TokenEndpoint {
        encoding: Default::default(),
        url: "https://example.com/auth".into(),
        method: reqwest::Method::POST,
        headers: HeaderMap::new(),
        body: None,
        token_path: "$.token".into(),
        expiry_path: None,
        expiry_ratio: DEFAULT_TOKEN_ENDPOINT_EXPIRY_RATIO,
        response_validator: None,
    }
    .apply(&mut headers);
    assert!(result.is_err());
}

#[test]
fn token_endpoint_expiry_ratio_zero_rejected() {
    let result = RestStream::new(RestStreamConfig::new("https://example.com", "/api").auth(
        Auth::TokenEndpoint {
            encoding: Default::default(),
            url: "https://example.com/auth".into(),
            method: reqwest::Method::POST,
            headers: HeaderMap::new(),
            body: None,
            token_path: "$.token".into(),
            expiry_path: None,
            expiry_ratio: 0.0,
            response_validator: None,
        },
    ));
    assert!(result.is_err());
}

#[test]
fn token_endpoint_expiry_ratio_valid_accepted() {
    let result = RestStream::new(RestStreamConfig::new("https://example.com", "/api").auth(
        Auth::TokenEndpoint {
            encoding: Default::default(),
            url: "https://example.com/auth".into(),
            method: reqwest::Method::POST,
            headers: HeaderMap::new(),
            body: None,
            token_path: "$.token".into(),
            expiry_path: None,
            expiry_ratio: 0.9,
            response_validator: None,
        },
    ));
    assert!(result.is_ok());
}
