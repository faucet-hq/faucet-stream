#![cfg_attr(docsrs, feature(doc_cfg))]

//! Shared Azure Blob Storage / ADLS Gen2 credential and client construction for
//! the faucet source and sink connectors.
//!
//! Both `faucet-source-azure-blob` and `faucet-sink-azure-blob` build a single
//! [`object_store`]-backed Azure store from an [`AzureConnection`] (account +
//! container + credentials), so end users see one consistent config surface for
//! both directions. ADLS Gen2 and classic Blob share the same
//! `MicrosoftAzureBuilder`, so a single code path serves both.

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use faucet_core::FaucetError;
use object_store::ObjectStore;
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Credential source for an Azure storage client.
///
/// Serializes as `{ type: <method>, config: { … } }` (adjacent tagging,
/// snake_case discriminators) — the consistent auth wire shape shared by every
/// faucet connector, e.g.
/// `{ type: account_key, config: { account_key: "…" } }`.
#[derive(Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum AzureCredentials {
    /// Shared storage-account access key (the primary/secondary key).
    AccountKey {
        /// Base64 account key.
        account_key: String,
    },
    /// Shared-access-signature token (with or without a leading `?`).
    SasToken {
        /// SAS token string.
        sas_token: String,
    },
    /// Full storage connection string
    /// (`DefaultEndpointsProtocol=…;AccountName=…;AccountKey=…;…`).
    ConnectionString {
        /// Connection string.
        connection_string: String,
    },
    /// Azure Managed Identity (IMDS). For a user-assigned identity, set
    /// `client_id`; leave it unset for the system-assigned identity.
    ManagedIdentity {
        /// Optional user-assigned managed-identity client id.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        client_id: Option<String>,
    },
    /// Azure AD service principal (client credentials).
    ServicePrincipal {
        /// Application (client) id.
        client_id: String,
        /// Client secret.
        client_secret: String,
        /// Directory (tenant) id.
        tenant_id: String,
    },
    /// `DefaultAzureCredential`-style resolution: the object-store builder's
    /// default chain (environment variables, workload identity, managed
    /// identity, Azure CLI), honouring `AZURE_*` env vars. This is the default.
    #[default]
    Default,
}

impl std::fmt::Debug for AzureCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        faucet_core::util::fmt_redacted(
            f,
            "AzureCredentials",
            self,
            &[
                "account_key",
                "sas_token",
                "connection_string",
                "client_secret",
            ],
        )
    }
}

impl AzureCredentials {
    /// Map this credential to the set of `object_store` Azure config
    /// key/value pairs that select it.
    ///
    /// Keys are the canonical `azure_storage_*` alias strings that
    /// [`AzureConfigKey::from_str`] accepts; returning them as plain strings
    /// keeps this mapping unit-testable without constructing a live store.
    /// [`Default`](AzureCredentials::Default) returns an empty set — the
    /// builder's default credential chain is used unchanged.
    pub fn config_entries(&self) -> Vec<(&'static str, String)> {
        match self {
            AzureCredentials::AccountKey { account_key } => {
                vec![("azure_storage_access_key", account_key.clone())]
            }
            AzureCredentials::SasToken { sas_token } => {
                vec![("azure_storage_sas_key", sas_token.clone())]
            }
            // `object_store` has no single connection-string config key, so we
            // parse the string into the account/key/sas/endpoint config keys it
            // does understand.
            AzureCredentials::ConnectionString { connection_string } => {
                parse_connection_string(connection_string)
            }
            // Setting the client id selects the user-assigned managed identity;
            // a system-assigned identity (no client id) is picked up by the
            // builder's default credential chain (IMDS), so no key is needed.
            AzureCredentials::ManagedIdentity { client_id } => match client_id {
                Some(id) => vec![("azure_storage_client_id", id.clone())],
                None => Vec::new(),
            },
            AzureCredentials::ServicePrincipal {
                client_id,
                client_secret,
                tenant_id,
            } => vec![
                ("azure_storage_client_id", client_id.clone()),
                ("azure_storage_client_secret", client_secret.clone()),
                ("azure_storage_tenant_id", tenant_id.clone()),
            ],
            AzureCredentials::Default => Vec::new(),
        }
    }
}

/// Parse an Azure storage connection string into the `object_store` config
/// key/value pairs it understands.
///
/// A connection string is a `;`-separated list of `Key=Value` segments, e.g.
/// `DefaultEndpointsProtocol=https;AccountName=x;AccountKey=y;EndpointSuffix=core.windows.net`.
/// Only the segments that map to an authentication/endpoint config key are
/// emitted (`AccountName`, `AccountKey`, `SharedAccessSignature`,
/// `BlobEndpoint`); protocol/suffix hints are ignored, so the default public-cloud
/// endpoint is used. Pure — unit-tested without a live store.
fn parse_connection_string(cs: &str) -> Vec<(&'static str, String)> {
    let mut entries: Vec<(&'static str, String)> = Vec::new();
    for segment in cs.split(';') {
        let segment = segment.trim();
        if segment.is_empty() {
            continue;
        }
        let Some((key, value)) = segment.split_once('=') else {
            continue;
        };
        let value = value.trim().to_string();
        if value.is_empty() {
            continue;
        }
        // `AccountKey` values may themselves contain `=` (base64 padding); the
        // `split_once` above keeps everything after the first `=` intact.
        match key.trim() {
            "AccountName" => entries.push(("azure_storage_account_name", value)),
            "AccountKey" => entries.push(("azure_storage_access_key", value)),
            "SharedAccessSignature" => entries.push(("azure_storage_sas_key", value)),
            "BlobEndpoint" => entries.push(("azure_storage_endpoint", value)),
            _ => {}
        }
    }
    entries
}

/// Connection parameters shared by the Azure source and sink.
///
/// Flattened into each connector's config via `#[serde(flatten)]`, so
/// `container`, `account`, `auth`, … appear at the top level of the source /
/// sink config.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AzureConnection {
    /// Blob container / ADLS Gen2 filesystem name. Required.
    pub container: String,
    /// Storage-account name. Optional when a connection string or the
    /// emulator supplies it, otherwise required.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account: Option<String>,
    /// Credential source. Defaults to the environment / managed-identity
    /// credential chain.
    #[serde(default)]
    pub auth: AzureCredentials,
    /// Custom blob endpoint (e.g. an Azurite emulator or a sovereign cloud).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// Permit plaintext HTTP (required for a local Azurite emulator).
    #[serde(default)]
    pub allow_http: bool,
    /// Target the Azurite storage emulator with its well-known
    /// `devstoreaccount1` credentials.
    #[serde(default)]
    pub use_emulator: bool,
    /// Seconds one request (including reading its body) may take. Unset (the
    /// default) means no limit, since a blob body is read at the pace of the
    /// sink and a total deadline would cut long reads off mid-body.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
    /// Seconds to wait for a connection to the storage endpoint. Defaults to 10.
    #[serde(default = "default_connect_timeout_secs")]
    pub connect_timeout_secs: u64,
    /// Retries of a failed request, including resuming an interrupted body
    /// read. Defaults to 10.
    #[serde(default = "default_max_retries")]
    pub max_retries: usize,
    /// Seconds after a request first went out during which it may still be
    /// retried or its body resumed. Defaults to 600. Keep it within the
    /// lifetime of the credential (a SAS token or bearer token), since a
    /// retry reuses the original request's credentials.
    #[serde(default = "default_retry_timeout_secs")]
    pub retry_timeout_secs: u64,
}

fn default_connect_timeout_secs() -> u64 {
    10
}

fn default_max_retries() -> usize {
    10
}

fn default_retry_timeout_secs() -> u64 {
    600
}

impl AzureConnection {
    /// New connection targeting `container` with default credentials.
    pub fn new(container: impl Into<String>) -> Self {
        Self {
            container: container.into(),
            account: None,
            auth: AzureCredentials::default(),
            endpoint: None,
            allow_http: false,
            use_emulator: false,
            timeout_secs: None,
            connect_timeout_secs: default_connect_timeout_secs(),
            max_retries: default_max_retries(),
            retry_timeout_secs: default_retry_timeout_secs(),
        }
    }

    /// The HTTP client options and retry policy [`build_store`] applies.
    pub fn client_settings(&self) -> (object_store::ClientOptions, object_store::RetryConfig) {
        let mut options = object_store::ClientOptions::new()
            .with_connect_timeout(Duration::from_secs(self.connect_timeout_secs));
        options = match self.timeout_secs {
            Some(secs) => options.with_timeout(Duration::from_secs(secs)),
            None => options.with_timeout_disabled(),
        };
        let retry = object_store::RetryConfig {
            max_retries: self.max_retries,
            retry_timeout: Duration::from_secs(self.retry_timeout_secs),
            ..Default::default()
        };
        (options, retry)
    }

    /// Set the storage-account name.
    pub fn account(mut self, account: impl Into<String>) -> Self {
        self.account = Some(account.into());
        self
    }

    /// Set the credential source.
    pub fn auth(mut self, auth: AzureCredentials) -> Self {
        self.auth = auth;
        self
    }

    /// Set a custom blob endpoint (emulator / sovereign cloud).
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Permit plaintext HTTP.
    pub fn allow_http(mut self, allow: bool) -> Self {
        self.allow_http = allow;
        self
    }

    /// Target the Azurite emulator.
    pub fn use_emulator(mut self, use_emulator: bool) -> Self {
        self.use_emulator = use_emulator;
        self
    }
}

/// Build an [`object_store`] Azure store from an [`AzureConnection`].
///
/// The builder starts from [`MicrosoftAzureBuilder::from_env`] so `AZURE_*`
/// environment variables act as a fallback; explicit config overrides them.
/// All build failures map to [`FaucetError::Config`].
pub fn build_store(conn: &AzureConnection) -> Result<Arc<dyn ObjectStore>, FaucetError> {
    if conn.container.trim().is_empty() {
        return Err(FaucetError::Config(
            "azure: container name must not be empty".into(),
        ));
    }

    let (options, retry) = conn.client_settings();
    let mut builder = MicrosoftAzureBuilder::from_env()
        .with_container_name(&conn.container)
        .with_client_options(options)
        .with_retry(retry);

    if let Some(account) = &conn.account {
        builder = builder.with_account(account);
    }
    if conn.use_emulator {
        builder = builder.with_use_emulator(true);
    }
    if let Some(endpoint) = &conn.endpoint {
        builder = builder.with_endpoint(endpoint.clone());
    }
    if conn.allow_http {
        builder = builder.with_allow_http(true);
    }

    for (key, value) in conn.auth.config_entries() {
        let config_key = AzureConfigKey::from_str(key)
            .map_err(|e| FaucetError::Config(format!("azure: unknown config key '{key}': {e}")))?;
        builder = builder.with_config(config_key, value);
    }

    let store = builder
        .build()
        .map_err(|e| FaucetError::Config(format!("azure: failed to build client: {e}")))?;
    Ok(Arc::new(store))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn credentials_default_is_default_variant() {
        assert_eq!(AzureCredentials::default(), AzureCredentials::Default);
    }

    #[test]
    fn default_credential_has_no_config_entries() {
        assert!(AzureCredentials::Default.config_entries().is_empty());
    }

    #[test]
    fn account_key_sets_access_key() {
        let creds = AzureCredentials::AccountKey {
            account_key: "abc123".into(),
        };
        assert_eq!(
            creds.config_entries(),
            vec![("azure_storage_access_key", "abc123".to_string())]
        );
    }

    #[test]
    fn sas_token_sets_sas_key() {
        let creds = AzureCredentials::SasToken {
            sas_token: "sv=2021".into(),
        };
        assert_eq!(
            creds.config_entries(),
            vec![("azure_storage_sas_key", "sv=2021".to_string())]
        );
    }

    #[test]
    fn connection_string_parses_into_account_and_key() {
        let creds = AzureCredentials::ConnectionString {
            connection_string:
                "DefaultEndpointsProtocol=https;AccountName=acct;AccountKey=a2V5==;EndpointSuffix=core.windows.net"
                    .into(),
        };
        let entries = creds.config_entries();
        assert!(entries.contains(&("azure_storage_account_name", "acct".to_string())));
        // AccountKey value retains its base64 padding (`=`).
        assert!(entries.contains(&("azure_storage_access_key", "a2V5==".to_string())));
        // Protocol / suffix hints are ignored.
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn connection_string_parses_sas_and_endpoint() {
        let creds = AzureCredentials::ConnectionString {
            connection_string:
                "SharedAccessSignature=sv=2021&sig=abc;BlobEndpoint=http://127.0.0.1:10000/acct"
                    .into(),
        };
        let entries = creds.config_entries();
        assert!(entries.contains(&("azure_storage_sas_key", "sv=2021&sig=abc".to_string())));
        assert!(entries.contains(&(
            "azure_storage_endpoint",
            "http://127.0.0.1:10000/acct".to_string()
        )));
    }

    #[test]
    fn connection_string_ignores_blank_and_unknown_segments() {
        assert!(parse_connection_string("").is_empty());
        assert!(parse_connection_string(";;Foo=bar;").is_empty());
        assert!(parse_connection_string("AccountKey=").is_empty());
    }

    #[test]
    fn managed_identity_user_assigned_sets_client_id() {
        let creds = AzureCredentials::ManagedIdentity {
            client_id: Some("mi-client".into()),
        };
        assert_eq!(
            creds.config_entries(),
            vec![("azure_storage_client_id", "mi-client".to_string())]
        );
    }

    #[test]
    fn managed_identity_system_assigned_sets_no_config() {
        let creds = AzureCredentials::ManagedIdentity { client_id: None };
        assert!(creds.config_entries().is_empty());
    }

    #[test]
    fn service_principal_sets_all_three_fields() {
        let creds = AzureCredentials::ServicePrincipal {
            client_id: "cid".into(),
            client_secret: "secret".into(),
            tenant_id: "tid".into(),
        };
        let entries = creds.config_entries();
        assert!(entries.contains(&("azure_storage_client_id", "cid".to_string())));
        assert!(entries.contains(&("azure_storage_client_secret", "secret".to_string())));
        assert!(entries.contains(&("azure_storage_tenant_id", "tid".to_string())));
    }

    #[test]
    fn credentials_serde_account_key_round_trip() {
        let creds = AzureCredentials::AccountKey {
            account_key: "k".into(),
        };
        let v = serde_json::to_value(&creds).unwrap();
        assert_eq!(
            v,
            json!({"type": "account_key", "config": {"account_key": "k"}})
        );
        let back: AzureCredentials = serde_json::from_value(v).unwrap();
        assert_eq!(back, creds);
    }

    #[test]
    fn credentials_serde_default_round_trip() {
        let v = serde_json::to_value(AzureCredentials::Default).unwrap();
        assert_eq!(v, json!({"type": "default"}));
        let back: AzureCredentials = serde_json::from_value(v).unwrap();
        assert_eq!(back, AzureCredentials::Default);
    }

    #[test]
    fn credentials_serde_service_principal_round_trip() {
        let creds = AzureCredentials::ServicePrincipal {
            client_id: "cid".into(),
            client_secret: "sec".into(),
            tenant_id: "tid".into(),
        };
        let v = serde_json::to_value(&creds).unwrap();
        assert_eq!(v["type"], "service_principal");
        let back: AzureCredentials = serde_json::from_value(v).unwrap();
        assert_eq!(back, creds);
    }

    #[test]
    fn connection_builder_sets_fields() {
        let conn = AzureConnection::new("data")
            .account("acct")
            .auth(AzureCredentials::AccountKey {
                account_key: "k".into(),
            })
            .endpoint("http://127.0.0.1:10000/devstoreaccount1")
            .allow_http(true)
            .use_emulator(true);
        assert_eq!(conn.container, "data");
        assert_eq!(conn.account.as_deref(), Some("acct"));
        assert!(conn.allow_http);
        assert!(conn.use_emulator);
        assert_eq!(
            conn.endpoint.as_deref(),
            Some("http://127.0.0.1:10000/devstoreaccount1")
        );
    }

    #[test]
    fn build_store_rejects_empty_container() {
        let conn = AzureConnection::new("   ");
        let err = build_store(&conn).unwrap_err();
        assert!(matches!(err, FaucetError::Config(_)));
    }

    #[test]
    fn build_store_succeeds_lazily_with_account_key() {
        // The builder is lazy (no I/O at build time), so a well-formed config
        // constructs a store even without a reachable backend. This exercises
        // the full config-entry → with_config wiring for the account-key path.
        let conn = AzureConnection::new("data")
            .account("devstoreaccount1")
            .auth(AzureCredentials::AccountKey {
                account_key: "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==".into(),
            })
            .endpoint("http://127.0.0.1:10000/devstoreaccount1")
            .allow_http(true);
        assert!(build_store(&conn).is_ok());
    }

    #[test]
    fn build_store_succeeds_lazily_with_emulator_and_default_creds() {
        let conn = AzureConnection::new("data")
            .use_emulator(true)
            .allow_http(true);
        assert!(build_store(&conn).is_ok());
    }

    #[test]
    fn build_store_succeeds_lazily_with_service_principal() {
        let conn =
            AzureConnection::new("data")
                .account("acct")
                .auth(AzureCredentials::ServicePrincipal {
                    client_id: "cid".into(),
                    client_secret: "sec".into(),
                    tenant_id: "tid".into(),
                });
        assert!(build_store(&conn).is_ok());
    }

    #[test]
    fn client_settings_default_to_no_total_timeout_and_a_long_retry_window() {
        let conn: AzureConnection = serde_json::from_value(json!({"container": "c"})).unwrap();
        assert_eq!(conn.timeout_secs, None);
        assert_eq!(conn.connect_timeout_secs, 10);
        let (_, retry) = conn.client_settings();
        assert_eq!(retry.max_retries, 10);
        assert_eq!(retry.retry_timeout, Duration::from_secs(600));

        let tuned: AzureConnection = serde_json::from_value(json!({
            "container": "c", "timeout_secs": 60, "max_retries": 2, "retry_timeout_secs": 30
        }))
        .unwrap();
        let (_, retry) = tuned.client_settings();
        assert_eq!(retry.max_retries, 2);
        assert_eq!(retry.retry_timeout, Duration::from_secs(30));
        build_store(&tuned.clone().account("acct")).unwrap();
        build_store(&AzureConnection::new("c").account("acct")).unwrap();
    }
}

#[cfg(test)]
mod debug_redaction_tests {
    use super::*;

    #[test]
    fn debug_never_prints_secrets() {
        let v: AzureCredentials =
            serde_json::from_str(r#"{"type":"account_key","config":{"account_key":"S3CRET-1"}}"#)
                .unwrap();
        let dbg = format!("{v:?}");
        assert!(!dbg.contains("S3CRET") && dbg.contains("***"), "{dbg}");
        let v: AzureCredentials = serde_json::from_str(r#"{"type":"service_principal","config":{"client_id":"c","client_secret":"S3CRET-2","tenant_id":"t"}}"#).unwrap();
        let dbg = format!("{v:?}");
        assert!(!dbg.contains("S3CRET") && dbg.contains("***"), "{dbg}");
    }
}
