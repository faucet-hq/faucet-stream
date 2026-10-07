//! HashiCorp Vault KV v2 resolver (`${vault:<path>[#field]}`).
//!
//! Auth: `VAULT_ADDR` + `VAULT_TOKEN`, optional `VAULT_NAMESPACE`. Pure HTTP
//! via `reqwest` — no Vault client dependency.

use super::{SecretResolver, extract_field, split_field};
use crate::error::{CliError, CliResult};
use async_trait::async_trait;
use serde_json::Value;

pub struct VaultResolver {
    addr: String,
    token: String,
    namespace: Option<String>,
    client: reqwest::Client,
}

impl VaultResolver {
    /// Build from `VAULT_ADDR` / `VAULT_TOKEN` (+ optional `VAULT_NAMESPACE`).
    pub fn from_env() -> CliResult<Self> {
        let addr = std::env::var("VAULT_ADDR").map_err(|_| CliError::SecretAuthFailed {
            scheme: "vault".into(),
            hint: "set VAULT_ADDR (e.g. https://vault.example.com:8200)".into(),
        })?;
        let token = std::env::var("VAULT_TOKEN").map_err(|_| CliError::SecretAuthFailed {
            scheme: "vault".into(),
            hint: "set VAULT_TOKEN".into(),
        })?;
        Ok(Self {
            addr: addr.trim_end_matches('/').to_owned(),
            token,
            namespace: std::env::var("VAULT_NAMESPACE").ok(),
            client: crate::secrets::secret_http_client(),
        })
    }
}

#[async_trait]
impl SecretResolver for VaultResolver {
    fn scheme(&self) -> &'static str {
        "vault"
    }

    async fn resolve(&self, reference: &str) -> CliResult<String> {
        let (path, field) = split_field(reference);
        let url = format!("{}/v1/{}", self.addr, path);
        let mut req = self.client.get(&url).header("X-Vault-Token", &self.token);
        if let Some(ns) = &self.namespace {
            req = req.header("X-Vault-Namespace", ns);
        }
        let resp = req
            .send()
            .await
            .map_err(|source| CliError::SecretFetchFailed {
                scheme: "vault".into(),
                reference: reference.into(),
                source: Box::new(source),
            })?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(CliError::SecretNotFound {
                scheme: "vault".into(),
                reference: reference.into(),
            });
        }
        if resp.status() == reqwest::StatusCode::FORBIDDEN
            || resp.status() == reqwest::StatusCode::UNAUTHORIZED
        {
            return Err(CliError::SecretAuthFailed {
                scheme: "vault".into(),
                hint: "VAULT_TOKEN was rejected (403/401) — check the token and its policy".into(),
            });
        }
        let resp = resp
            .error_for_status()
            .map_err(|source| CliError::SecretFetchFailed {
                scheme: "vault".into(),
                reference: reference.into(),
                source: Box::new(source),
            })?;
        let body: Value = resp
            .json()
            .await
            .map_err(|source| CliError::SecretFetchFailed {
                scheme: "vault".into(),
                reference: reference.into(),
                source: Box::new(source),
            })?;
        // KV v2: secret map lives at .data.data
        let data = &body["data"]["data"];
        // A KV v1 mount (or any 200 whose secret map is at `.data`, not
        // `.data.data`) leaves this path absent → `Value::Null`. Without this
        // guard the no-field branch would return the literal string "null" as a
        // silently-wrong credential (audit #321 H4). Fail loudly instead.
        if data.is_null() {
            return Err(CliError::SecretNotFound {
                scheme: "vault".into(),
                reference: reference.into(),
            });
        }
        match field {
            Some(f) => extract_field("vault", reference, &data.to_string(), f),
            None => Ok(data.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn reads_kv_v2_field() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/secret/data/app"))
            .and(header("X-Vault-Token", "test-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": { "data": { "token": "s3cr3t-value", "other": "x" } }
            })))
            .mount(&server)
            .await;

        let resolver = VaultResolver {
            addr: server.uri(),
            token: "test-token".into(),
            namespace: None,
            client: crate::secrets::secret_http_client(),
        };
        let v = resolver.resolve("secret/data/app#token").await.unwrap();
        assert_eq!(v, "s3cr3t-value");
    }

    #[tokio::test]
    async fn missing_secret_is_not_found() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/secret/data/nope"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        let resolver = VaultResolver {
            addr: server.uri(),
            token: "t".into(),
            namespace: None,
            client: crate::secrets::secret_http_client(),
        };
        match resolver.resolve("secret/data/nope#x").await.unwrap_err() {
            CliError::SecretNotFound { .. } => {}
            other => panic!("expected SecretNotFound, got {other:?}"),
        }
    }

    #[test]
    fn scheme_is_vault() {
        let r = VaultResolver {
            addr: "http://x".into(),
            token: "t".into(),
            namespace: None,
            client: crate::secrets::secret_http_client(),
        };
        assert_eq!(r.scheme(), "vault");
    }

    #[tokio::test]
    async fn forbidden_status_maps_to_auth_failed() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/secret/data/secured"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;
        let resolver = VaultResolver {
            addr: server.uri(),
            token: "bad".into(),
            namespace: None,
            client: crate::secrets::secret_http_client(),
        };
        match resolver.resolve("secret/data/secured#x").await.unwrap_err() {
            CliError::SecretAuthFailed { scheme, .. } => assert_eq!(scheme, "vault"),
            other => panic!("expected SecretAuthFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn unauthorized_status_maps_to_auth_failed() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/secret/data/secured"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;
        let resolver = VaultResolver {
            addr: server.uri(),
            token: "bad".into(),
            namespace: None,
            client: crate::secrets::secret_http_client(),
        };
        match resolver.resolve("secret/data/secured").await.unwrap_err() {
            CliError::SecretAuthFailed { .. } => {}
            other => panic!("expected SecretAuthFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn no_field_returns_whole_kv_v2_map_as_json() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/secret/data/all"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": { "data": { "a": "1", "b": "2" } }
            })))
            .mount(&server)
            .await;
        let resolver = VaultResolver {
            addr: server.uri(),
            token: "t".into(),
            namespace: None,
            client: crate::secrets::secret_http_client(),
        };
        // No `#field` → the whole `.data.data` map, JSON-serialized.
        let v = resolver.resolve("secret/data/all").await.unwrap();
        let parsed: Value = serde_json::from_str(&v).unwrap();
        assert_eq!(parsed["a"], "1");
        assert_eq!(parsed["b"], "2");
    }

    #[tokio::test]
    async fn namespace_header_is_sent_when_configured() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/secret/data/ns"))
            .and(header("X-Vault-Namespace", "team-a"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": { "data": { "k": "v" } }
            })))
            .mount(&server)
            .await;
        let resolver = VaultResolver {
            addr: server.uri(),
            token: "t".into(),
            namespace: Some("team-a".into()),
            client: crate::secrets::secret_http_client(),
        };
        // The mock only matches when the namespace header is present.
        let v = resolver.resolve("secret/data/ns#k").await.unwrap();
        assert_eq!(v, "v");
    }

    #[test]
    #[serial_test::serial(vault_env)]
    fn from_env_reads_addr_token_namespace() {
        unsafe {
            std::env::set_var("VAULT_ADDR", "https://vault.example.com:8200/");
            std::env::set_var("VAULT_TOKEN", "tok-123");
            std::env::set_var("VAULT_NAMESPACE", "ns1");
        }
        let r = VaultResolver::from_env().unwrap();
        // Trailing slash on the addr must be trimmed.
        assert_eq!(r.addr, "https://vault.example.com:8200");
        assert_eq!(r.token, "tok-123");
        assert_eq!(r.namespace.as_deref(), Some("ns1"));
        unsafe {
            std::env::remove_var("VAULT_ADDR");
            std::env::remove_var("VAULT_TOKEN");
            std::env::remove_var("VAULT_NAMESPACE");
        }
    }

    #[test]
    #[serial_test::serial(vault_env)]
    fn from_env_errors_without_addr() {
        unsafe {
            std::env::remove_var("VAULT_ADDR");
            std::env::remove_var("VAULT_TOKEN");
        }
        match VaultResolver::from_env() {
            Err(CliError::SecretAuthFailed { scheme, hint }) => {
                assert_eq!(scheme, "vault");
                assert!(hint.contains("VAULT_ADDR"), "{hint}");
            }
            Err(other) => panic!("expected SecretAuthFailed, got {other:?}"),
            Ok(_) => panic!("expected an error when VAULT_ADDR is unset"),
        }
    }

    #[test]
    #[serial_test::serial(vault_env)]
    fn from_env_errors_without_token() {
        unsafe {
            std::env::set_var("VAULT_ADDR", "https://v");
            std::env::remove_var("VAULT_TOKEN");
        }
        match VaultResolver::from_env() {
            Err(CliError::SecretAuthFailed { scheme, hint }) => {
                assert_eq!(scheme, "vault");
                assert!(hint.contains("VAULT_TOKEN"), "{hint}");
            }
            Err(other) => panic!("expected SecretAuthFailed, got {other:?}"),
            Ok(_) => panic!("expected an error when VAULT_TOKEN is unset"),
        }
        unsafe {
            std::env::remove_var("VAULT_ADDR");
        }
    }
    #[tokio::test]
    async fn kv_v1_shape_is_not_found_not_literal_null() {
        // #321 H4: a mount whose secret map is at `.data` (KV v1), not
        // `.data.data`, must error rather than return the literal string "null".
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/secret/app"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": { "token": "s3cr3t" }
            })))
            .mount(&server)
            .await;
        let resolver = VaultResolver {
            addr: server.uri(),
            token: "t".into(),
            namespace: None,
            client: crate::secrets::secret_http_client(),
        };
        // No `#field`: the missing `.data.data` must surface as SecretNotFound,
        // never Ok("null").
        match resolver.resolve("secret/app").await.unwrap_err() {
            CliError::SecretNotFound { .. } => {}
            other => panic!("expected SecretNotFound, got {other:?}"),
        }
    }
}
