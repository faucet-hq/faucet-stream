//! Integration tests for the shared `AuthProvider` injection path on the
//! Snowflake source.
//!
//! A `FixedBearer` provider is injected via `.with_auth_provider()`; the mock
//! server asserts that the correct OAuth header arrives, proving that the
//! provider's credential — not the config's inline auth — was used.

use std::sync::Arc;

use faucet_core::{AuthProvider, AuthReference, AuthSpec, Credential, FaucetError, Source};
use faucet_source_snowflake::{SnowflakeAuth, SnowflakeSource, SnowflakeSourceConfig};
use serde_json::json;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// A trivial provider that always returns the same bearer token.
#[derive(Debug)]
struct FixedBearer(&'static str);

#[async_trait::async_trait]
impl AuthProvider for FixedBearer {
    async fn credential(&self) -> Result<Credential, FaucetError> {
        Ok(Credential::Bearer(self.0.to_string()))
    }
    fn provider_name(&self) -> &'static str {
        "fixed-bearer"
    }
}

fn minimal_response() -> serde_json::Value {
    json!({
        "code": "090001",
        "statementHandle": "h",
        "resultSetMetaData": {
            "rowType": [{"name": "ID", "type": "fixed"}],
            "partitionInfo": [{"rowCount": 1}]
        },
        "data": [["42"]]
    })
}

// Build a source config whose inline auth would send a DIFFERENT token so any
// test that accidentally uses the inline auth will fail the mock assertion.
fn base_cfg() -> SnowflakeSourceConfig {
    SnowflakeSourceConfig::new(
        "xy12345",
        "WH",
        "DB",
        "PUBLIC",
        SnowflakeAuth::OAuth {
            token: "INLINE-TOKEN".into(),
        },
        "SELECT 1",
    )
}

/// The provider's bearer token must be forwarded as `Bearer INJECTED`.
/// The mock will only respond to that exact header; any other value ⇒ no mock
/// match ⇒ wiremock returns 404 ⇒ test fails.
#[tokio::test]
async fn injected_provider_supplies_oauth_token() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(header("Authorization", "Bearer INJECTED"))
        .and(header("X-Snowflake-Authorization-Token-Type", "OAUTH"))
        .respond_with(ResponseTemplate::new(200).set_body_json(minimal_response()))
        .mount(&server)
        .await;

    let provider = Arc::new(FixedBearer("INJECTED"));
    let src = SnowflakeSource::new(base_cfg())
        .unwrap()
        .with_endpoint_base(server.uri())
        .with_auth_provider(provider);

    let rows = src.fetch_all().await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["ID"], 42);
}

/// A `Token` credential maps the same way as `Bearer` (both produce
/// `SnowflakeAuth::OAuth`).
#[tokio::test]
async fn token_credential_maps_to_oauth() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(header("Authorization", "Bearer TOKEN-CRED"))
        .respond_with(ResponseTemplate::new(200).set_body_json(minimal_response()))
        .mount(&server)
        .await;

    #[derive(Debug)]
    struct FixedToken(&'static str);
    #[async_trait::async_trait]
    impl AuthProvider for FixedToken {
        async fn credential(&self) -> Result<Credential, FaucetError> {
            Ok(Credential::Token(self.0.to_string()))
        }
        fn provider_name(&self) -> &'static str {
            "fixed-token"
        }
    }

    let src = SnowflakeSource::new(base_cfg())
        .unwrap()
        .with_endpoint_base(server.uri())
        .with_auth_provider(Arc::new(FixedToken("TOKEN-CRED")));

    let rows = src.fetch_all().await.unwrap();
    assert_eq!(rows.len(), 1);
}

/// An `AuthSpec::Reference` with no provider injected must surface a typed
/// `FaucetError::Auth` at request time (not at construction time).
#[tokio::test]
async fn unresolved_auth_reference_errors() {
    let server = MockServer::start().await;
    let mut config = base_cfg();
    config.auth = AuthSpec::Reference(AuthReference {
        name: "missing-provider".into(),
    });
    let src = SnowflakeSource::new(config)
        .unwrap()
        .with_endpoint_base(server.uri());

    let err = src.fetch_all().await.unwrap_err();
    assert!(
        matches!(err, FaucetError::Auth(_)),
        "expected Auth error, got {err:?}"
    );
}

/// The `credential_to_auth` helper in `faucet-common-snowflake` must reject
/// credential variants that Snowflake has no concept of.
#[test]
fn credential_to_auth_rejects_basic() {
    let cred = Credential::Basic {
        username: "u".into(),
        password: "p".into(),
    };
    let err = faucet_common_snowflake::credential_to_auth(cred).unwrap_err();
    assert!(
        matches!(err, FaucetError::Auth(_)),
        "expected Auth error, got {err:?}"
    );
}

/// Two sources can share one provider without each spawning an independent
/// token-refresh cycle.
#[tokio::test]
async fn one_provider_shared_across_two_sources() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(header("Authorization", "Bearer SHARED"))
        .respond_with(ResponseTemplate::new(200).set_body_json(minimal_response()))
        .mount(&server)
        .await;

    let provider = Arc::new(FixedBearer("SHARED"));
    let a = SnowflakeSource::new(base_cfg())
        .unwrap()
        .with_endpoint_base(server.uri())
        .with_auth_provider(provider.clone());
    let b = SnowflakeSource::new(base_cfg())
        .unwrap()
        .with_endpoint_base(server.uri())
        .with_auth_provider(provider.clone());

    assert_eq!(a.fetch_all().await.unwrap().len(), 1);
    assert_eq!(b.fetch_all().await.unwrap().len(), 1);
}
