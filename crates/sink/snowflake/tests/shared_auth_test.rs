//! Integration tests for the shared `AuthProvider` injection path on the
//! Snowflake sink.
//!
//! A `FixedBearer` provider is injected via `.with_auth_provider()`; the mock
//! server asserts that the correct OAuth header arrives, proving that the
//! provider's credential — not the config's inline auth — was used.

use std::sync::Arc;

use faucet_core::{AuthProvider, AuthReference, AuthSpec, Credential, FaucetError, Sink};
use faucet_sink_snowflake::{SnowflakeAuth, SnowflakeSink, SnowflakeSinkConfig};
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

fn success_response() -> serde_json::Value {
    json!({ "code": "090001", "message": "Statement executed successfully." })
}

// Build a sink config whose inline auth would send a DIFFERENT token so any
// test that accidentally uses the inline auth will fail the mock assertion.
fn base_cfg() -> SnowflakeSinkConfig {
    SnowflakeSinkConfig::new(
        "xy12345",
        "WH",
        "DB",
        "PUBLIC",
        "events",
        SnowflakeAuth::OAuth {
            token: "INLINE-TOKEN".into(),
        },
    )
}

/// The provider's bearer token must be forwarded as `Bearer INJECTED`.
/// The mock only responds to that exact header; any other value ⇒ no mock
/// match ⇒ wiremock returns 404 ⇒ test fails.
#[tokio::test]
async fn injected_provider_supplies_oauth_token() {
    let server = MockServer::start().await;
    let endpoint = format!("{}/api/v2/statements", server.uri());

    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(header("Authorization", "Bearer INJECTED"))
        .and(header("X-Snowflake-Authorization-Token-Type", "OAUTH"))
        .respond_with(ResponseTemplate::new(200).set_body_json(success_response()))
        .mount(&server)
        .await;

    let provider = Arc::new(FixedBearer("INJECTED"));
    let sink = SnowflakeSink::new(base_cfg())
        .unwrap()
        .with_endpoint(endpoint)
        .with_auth_provider(provider);

    let n = sink.write_batch(&[json!({"id": 1})]).await.unwrap();
    assert_eq!(n, 1);
}

/// A `Token` credential maps the same way as `Bearer`.
#[tokio::test]
async fn token_credential_maps_to_oauth() {
    let server = MockServer::start().await;
    let endpoint = format!("{}/api/v2/statements", server.uri());

    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(header("Authorization", "Bearer TOKEN-CRED"))
        .respond_with(ResponseTemplate::new(200).set_body_json(success_response()))
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

    let sink = SnowflakeSink::new(base_cfg())
        .unwrap()
        .with_endpoint(endpoint)
        .with_auth_provider(Arc::new(FixedToken("TOKEN-CRED")));

    let n = sink.write_batch(&[json!({"id": 1})]).await.unwrap();
    assert_eq!(n, 1);
}

/// An `AuthSpec::Reference` with no provider injected must surface a typed
/// `FaucetError::Auth` at request time (not at construction time).
#[tokio::test]
async fn unresolved_auth_reference_errors() {
    let server = MockServer::start().await;
    let endpoint = format!("{}/api/v2/statements", server.uri());
    let mut config = base_cfg();
    config.auth = AuthSpec::Reference(AuthReference {
        name: "missing-provider".into(),
    });
    let sink = SnowflakeSink::new(config).unwrap().with_endpoint(endpoint);

    let err = sink.write_batch(&[json!({"id": 1})]).await.unwrap_err();
    assert!(
        matches!(err, FaucetError::Auth(_)),
        "expected Auth error, got {err:?}"
    );
}

/// Two sinks sharing one provider verify the Arc-clone path compiles and runs.
#[tokio::test]
async fn one_provider_shared_across_two_sinks() {
    let server = MockServer::start().await;
    let endpoint = format!("{}/api/v2/statements", server.uri());

    Mock::given(method("POST"))
        .and(header("Authorization", "Bearer SHARED"))
        .respond_with(ResponseTemplate::new(200).set_body_json(success_response()))
        .mount(&server)
        .await;

    let provider = Arc::new(FixedBearer("SHARED"));
    let a = SnowflakeSink::new(base_cfg())
        .unwrap()
        .with_endpoint(endpoint.clone())
        .with_auth_provider(provider.clone());
    let b = SnowflakeSink::new(base_cfg())
        .unwrap()
        .with_endpoint(endpoint)
        .with_auth_provider(provider.clone());

    assert_eq!(a.write_batch(&[json!({"id": 1})]).await.unwrap(), 1);
    assert_eq!(b.write_batch(&[json!({"id": 2})]).await.unwrap(), 1);
}
