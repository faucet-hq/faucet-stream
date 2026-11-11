#![cfg_attr(docsrs, feature(doc_cfg))]

//! Shared Azure Blob Storage / ADLS Gen2 credential and client construction for
//! the faucet source and sink connectors.
//!
//! Both `faucet-source-azure-blob` and `faucet-sink-azure-blob` build a single
//! [`object_store`]-backed Azure store from an [`AzureConnection`] (account +
//! container + credentials), so end users see one consistent config surface for
//! both directions. ADLS Gen2 and classic Blob share the same
//! `MicrosoftAzureBuilder`, so a single code path serves both.

mod backend;
#[cfg(feature = "testing")]
#[doc(hidden)]
pub mod testing;

pub use backend::{AzureBackend, AzureBackendBuilder, PART_BYTES};

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
        /// Connection string. Its endpoints (`BlobEndpoint=`, or the account
        /// host built from `AccountName=` / `EndpointSuffix=`) are checked
        /// like a URL by a host that confines configs.
        #[schemars(extend("x-faucet-resource" = "url", "x-faucet-url-syntax" = "azure-conn"))]
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
        /// Azure AD authority host the token request goes to, for a sovereign
        /// or private cloud (`https://login.microsoftonline.us`,
        /// `https://login.chinacloudapi.cn`). Unset uses `AZURE_AUTHORITY_HOST`
        /// from the environment, else the public cloud
        /// (`https://login.microsoftonline.com`). Must be `https://`; plain
        /// `http://` is accepted only for a loopback host.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[schemars(extend("x-faucet-resource" = "url"))]
        authority_host: Option<String>,
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
                authority_host,
            } => {
                let mut entries = vec![
                    ("azure_storage_client_id", client_id.clone()),
                    ("azure_storage_client_secret", client_secret.clone()),
                    ("azure_storage_tenant_id", tenant_id.clone()),
                ];
                if let Some(host) = authority_host {
                    entries.push((
                        "azure_storage_authority_host",
                        host.trim().trim_end_matches('/').to_string(),
                    ));
                }
                entries
            }
            AzureCredentials::Default => Vec::new(),
        }
    }

    /// Check the parts of this credential that can be wrong before any
    /// request: a service principal's `authority_host` must be an `https://`
    /// URL (or `http://` to a loopback host) with no credentials, query or
    /// fragment; a connection string's `AccountName` must be a storage
    /// account name and its `DefaultEndpointsProtocol` `http` or `https`.
    pub fn validate(&self) -> Result<(), FaucetError> {
        match self {
            AzureCredentials::ServicePrincipal {
                authority_host: Some(host),
                ..
            } => validate_authority_host(host)?,
            AzureCredentials::ConnectionString { connection_string } => {
                if let Some(problem) =
                    faucet_core::resolve::AzureConnSettings::read(connection_string).problem()
                {
                    return Err(FaucetError::Config(format!(
                        "azure: connection_string: {problem}"
                    )));
                }
            }
            _ => {}
        }
        Ok(())
    }
}

fn validate_authority_host(raw: &str) -> Result<(), FaucetError> {
    let bad = |why: &str| {
        FaucetError::Config(format!(
            "azure: service_principal authority_host '{raw}' {why}"
        ))
    };
    let url = url::Url::parse(raw.trim()).map_err(|e| bad(&format!("is not a URL: {e}")))?;
    let loopback = match url.host() {
        Some(url::Host::Domain(d)) => d.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        None => return Err(bad("has no host")),
    };
    match url.scheme() {
        "https" => {}
        "http" if loopback => {}
        "http" => {
            return Err(bad(
                "must use https:// (http:// is allowed only for a loopback host)",
            ));
        }
        other => return Err(bad(&format!("must use https://, not {other}://"))),
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(bad("must not carry credentials"));
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(bad("must not have a query or fragment"));
    }
    Ok(())
}

/// Parse an Azure storage connection string into the `object_store` config
/// key/value pairs it understands.
///
/// A connection string is a `;`-separated list of `Key=Value` segments, e.g.
/// `DefaultEndpointsProtocol=https;AccountName=x;AccountKey=y;EndpointSuffix=core.windows.net`.
/// `AccountName`, `AccountKey`, `SharedAccessSignature` and `BlobEndpoint` map
/// straight across; without a `BlobEndpoint`, `EndpointSuffix` (a sovereign
/// cloud such as `core.chinacloudapi.cn`) and `DefaultEndpointsProtocol`
/// build `<protocol>://<account>.blob.<suffix>`; an `http` protocol permits
/// plaintext HTTP; and `UseDevelopmentStorage=true` targets the Azurite
/// emulator. Pure — unit-tested without a live store.
fn parse_connection_string(cs: &str) -> Vec<(&'static str, String)> {
    use faucet_core::resolve::{AzureConnKey, AzureConnSettings};
    let settings = AzureConnSettings::read(cs);
    let mut entries: Vec<(&'static str, String)> = Vec::new();
    let mut blob_endpoint = false;
    for (key, value) in settings.entries() {
        let value = value.clone();
        match key {
            AzureConnKey::AccountName => entries.push(("azure_storage_account_name", value)),
            AzureConnKey::AccountKey => entries.push(("azure_storage_access_key", value)),
            AzureConnKey::SharedAccessSignature => entries.push(("azure_storage_sas_key", value)),
            AzureConnKey::BlobEndpoint => {
                blob_endpoint = true;
                if value.to_ascii_lowercase().starts_with("http://") {
                    entries.push(("allow_http", "true".into()));
                }
                entries.push(("azure_storage_endpoint", value));
            }
            AzureConnKey::UseDevelopmentStorage => {
                entries.push(("azure_storage_use_emulator", "true".into()));
            }
            _ => {}
        }
    }
    if !blob_endpoint && let Some(endpoint) = settings.endpoint() {
        if settings.protocol().as_deref() == Some("http") {
            entries.push(("allow_http", "true".into()));
        }
        entries.push(("azure_storage_endpoint", endpoint));
    }
    entries
}

/// The `AZURE_*` environment settings an explicitly configured credential
/// still takes: the account, endpoint and HTTP-client options, plus the
/// identity endpoint for a managed identity and the authority host for a
/// service principal — never another credential, so
/// a key, secret or workload identity in the environment cannot replace the
/// one the config names.
fn env_fallback(
    auth: &AzureCredentials,
    vars: impl IntoIterator<Item = (String, String)>,
) -> Vec<(AzureConfigKey, String)> {
    let managed = matches!(auth, AzureCredentials::ManagedIdentity { .. });
    let principal = matches!(auth, AzureCredentials::ServicePrincipal { .. });
    let mut out = Vec::new();
    for (name, value) in vars {
        if name == "IDENTITY_ENDPOINT" {
            if managed {
                out.push((AzureConfigKey::MsiEndpoint, value));
            }
            continue;
        }
        if !name.starts_with("AZURE_") {
            continue;
        }
        let Ok(key) = AzureConfigKey::from_str(&name.to_ascii_lowercase()) else {
            continue;
        };
        let keep = match key {
            AzureConfigKey::AccountName
            | AzureConfigKey::Endpoint
            | AzureConfigKey::UseEmulator
            | AzureConfigKey::ContainerName
            | AzureConfigKey::DisableTagging
            | AzureConfigKey::Client(_) => true,
            AzureConfigKey::MsiEndpoint
            | AzureConfigKey::ObjectId
            | AzureConfigKey::MsiResourceId => managed,
            AzureConfigKey::AuthorityHost => principal,
            _ => false,
        };
        if keep {
            out.push((key, value));
        }
    }
    out
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
    #[schemars(extend("x-faucet-resource" = "url"))]
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

    /// Check the connection before any request: `account` must be a storage
    /// account name (3 to 24 lowercase letters and digits), and the
    /// credential must pass [`AzureCredentials::validate`].
    pub fn validate(&self) -> Result<(), FaucetError> {
        if let Some(account) = &self.account
            && !faucet_core::resolve::is_valid_azure_account_name(account)
        {
            return Err(FaucetError::Config(
                "azure: `account` must be an Azure storage account name: 3 to 24 lowercase \
                 letters and digits"
                    .into(),
            ));
        }
        self.auth.validate()
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
/// With `auth: default` the builder starts from
/// [`MicrosoftAzureBuilder::from_env`], so the environment's credential chain
/// applies. With any other `auth` it starts empty and takes only the
/// non-credential `AZURE_*` settings from the environment, so the configured
/// credential is the one used. All build failures map to
/// [`FaucetError::Config`].
pub fn build_store(conn: &AzureConnection) -> Result<Arc<dyn ObjectStore>, FaucetError> {
    Ok(build_azure(conn)?)
}

/// [`build_store`] as the concrete Azure store, which also lists by a raw key
/// prefix ([`PaginatedListStore`](object_store::list::PaginatedListStore)).
pub fn build_azure(
    conn: &AzureConnection,
) -> Result<Arc<object_store::azure::MicrosoftAzure>, FaucetError> {
    let store = configured_builder(conn)?
        .build()
        .map_err(|e| FaucetError::Config(format!("azure: failed to build client: {e}")))?;
    Ok(Arc::new(store))
}

/// The process environment, its UTF-8 entries only (as
/// [`MicrosoftAzureBuilder::from_env`] reads it).
fn process_env() -> Vec<(String, String)> {
    std::env::vars_os()
        .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
        .collect()
}

/// The settings the builder takes from `vars`: with `auth: default`, every
/// `AZURE_*` key it understands plus `IDENTITY_ENDPOINT` (what
/// [`MicrosoftAzureBuilder::from_env`] reads); otherwise [`env_fallback`].
fn env_layer(
    auth: &AzureCredentials,
    vars: impl IntoIterator<Item = (String, String)>,
) -> Vec<(AzureConfigKey, String)> {
    if auth != &AzureCredentials::Default {
        return env_fallback(auth, vars);
    }
    let vars: Vec<(String, String)> = vars.into_iter().collect();
    let mut out: Vec<(AzureConfigKey, String)> = vars
        .iter()
        .filter(|(k, _)| k.starts_with("AZURE_"))
        .filter_map(|(k, v)| {
            AzureConfigKey::from_str(&k.to_ascii_lowercase())
                .ok()
                .map(|key| (key, v.clone()))
        })
        .collect();
    if let Some((_, v)) = vars.iter().find(|(k, _)| k == "IDENTITY_ENDPOINT") {
        out.push((AzureConfigKey::MsiEndpoint, v.clone()));
    }
    out
}

/// Whether a connection string names its own credential: an account key, a
/// shared access signature, or the emulator.
fn connection_string_names_a_credential(cs: &str) -> bool {
    parse_connection_string(cs).iter().any(|(k, v)| {
        matches!(
            *k,
            "azure_storage_access_key" | "azure_storage_sas_key" | "azure_storage_use_emulator"
        ) && !v.trim().is_empty()
    })
}

/// The config fields that make an Azure client built from `config`
/// authenticate with the identity of the host it runs on: `auth` when it
/// selects `default` (also when absent) without `use_emulator`,
/// `managed_identity`, or a `connection_string` with neither an account key,
/// a shared access signature nor the emulator.
pub fn host_identity(config: &faucet_core::Value) -> Vec<String> {
    host_identity_at(config, "")
}

/// [`host_identity`] for an Azure connection block at another JSON pointer
/// (`""` for the config root).
pub fn host_identity_at(config: &faucet_core::Value, base: &str) -> Vec<String> {
    use faucet_core::capabilities::config::{bool_at, str_at, tag_at};
    let auth = format!("{base}/auth");
    let host = match tag_at(config, &auth).unwrap_or("default") {
        "default" => !bool_at(config, &format!("{base}/use_emulator"), false),
        "managed_identity" => true,
        "connection_string" => !str_at(config, &format!("{auth}/config/connection_string"))
            .is_some_and(connection_string_names_a_credential),
        _ => false,
    };
    if host {
        vec![auth.trim_start_matches('/').replace('/', ".")]
    } else {
        Vec::new()
    }
}

/// Refuse a connection that would authenticate with the host's identity
/// where the task may not use it. `vars` is the environment the builder
/// reads (its emulator switch counts for `auth: default`).
fn check_host_identity(
    conn: &AzureConnection,
    vars: &[(String, String)],
) -> Result<(), FaucetError> {
    let host = match &conn.auth {
        AzureCredentials::Default => {
            !conn.use_emulator
                && !vars
                    .iter()
                    .any(|(k, v)| k == "AZURE_STORAGE_USE_EMULATOR" && truthy(v))
        }
        AzureCredentials::ManagedIdentity { .. } => true,
        AzureCredentials::ConnectionString { connection_string } => {
            !connection_string_names_a_credential(connection_string)
        }
        _ => false,
    };
    if host {
        faucet_core::resolve::require_host_credentials("azure", "auth")?;
    }
    Ok(())
}

fn configured_builder(conn: &AzureConnection) -> Result<MicrosoftAzureBuilder, FaucetError> {
    configured_builder_with(conn, process_env())
}

fn configured_builder_with(
    conn: &AzureConnection,
    vars: impl IntoIterator<Item = (String, String)>,
) -> Result<MicrosoftAzureBuilder, FaucetError> {
    conn.validate()?;
    let vars: Vec<(String, String)> = vars.into_iter().collect();
    check_host_identity(conn, &vars)?;
    if conn.container.trim().is_empty() {
        return Err(FaucetError::Config(
            "azure: container name must not be empty".into(),
        ));
    }

    let (options, retry) = conn.client_settings();
    let base = env_layer(&conn.auth, vars)
        .into_iter()
        .fold(MicrosoftAzureBuilder::new(), |b, (k, v)| {
            b.with_config(k, v)
        });
    let mut builder = base
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
    Ok(builder)
}

/// A boolean setting as the builder reads it (`true`, `1`, `on`, `yes`, `y`).
fn truthy(v: &str) -> bool {
    matches!(
        v.to_ascii_lowercase().as_str(),
        "1" | "true" | "on" | "yes" | "y"
    )
}

/// The emulator URL used when `AZURITE_BLOB_STORAGE_URL` is not set.
const EMULATOR_URL: &str = "http://127.0.0.1:10000";

/// Where an [`AzureConnection`] leads the client: the account, endpoint and
/// emulator settings the store is built with, after the environment, the
/// typed fields and the connection string are applied in
/// [`build_store`]'s order (the string's entries last).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct EffectiveSettings {
    /// The storage account.
    pub account: Option<String>,
    /// The blob endpoint URL, when one is set.
    pub endpoint: Option<String>,
    /// Whether the store targets the storage emulator.
    pub use_emulator: bool,
    /// Whether the default endpoint is the Fabric one.
    pub use_fabric_endpoint: bool,
    /// The emulator URL from `AZURITE_BLOB_STORAGE_URL`, if set.
    pub emulator_url: Option<String>,
}

impl EffectiveSettings {
    /// The URL the store sends its requests to, if it can be built: the
    /// emulator's, else the endpoint, else the account's default.
    pub fn url(&self) -> Option<String> {
        if self.use_emulator {
            return Some(
                self.emulator_url
                    .clone()
                    .unwrap_or_else(|| EMULATOR_URL.to_string()),
            );
        }
        if let Some(endpoint) = &self.endpoint {
            return Some(endpoint.clone());
        }
        let account = self.account.as_ref()?;
        Some(if self.use_fabric_endpoint {
            format!("https://{account}.blob.fabric.microsoft.com")
        } else {
            format!("https://{account}.blob.core.windows.net")
        })
    }

    /// The hosts the store connects to, canonical
    /// ([`faucet_core::resolve::url_hosts`]).
    pub fn hosts(&self) -> Vec<String> {
        self.url()
            .map(|u| faucet_core::resolve::url_hosts(&u))
            .unwrap_or_default()
    }
}

/// [`EffectiveSettings`] of `conn` under the process environment.
pub fn effective_settings(conn: &AzureConnection) -> EffectiveSettings {
    effective_settings_with(conn, process_env())
}

/// [`EffectiveSettings`] of `conn` under the environment `vars`. Pure.
pub fn effective_settings_with(
    conn: &AzureConnection,
    vars: impl IntoIterator<Item = (String, String)>,
) -> EffectiveSettings {
    let vars: Vec<(String, String)> = vars.into_iter().collect();
    let mut out = EffectiveSettings {
        emulator_url: vars
            .iter()
            .find(|(k, _)| k == "AZURITE_BLOB_STORAGE_URL")
            .map(|(_, v)| v.clone()),
        ..Default::default()
    };
    let mut apply = |key: AzureConfigKey, value: &str| match key {
        AzureConfigKey::AccountName => out.account = Some(value.to_string()),
        AzureConfigKey::Endpoint => out.endpoint = Some(value.to_string()),
        AzureConfigKey::UseEmulator => out.use_emulator = truthy(value),
        AzureConfigKey::UseFabricEndpoint => out.use_fabric_endpoint = truthy(value),
        _ => {}
    };
    for (key, value) in env_layer(&conn.auth, vars) {
        apply(key, &value);
    }
    if let Some(account) = &conn.account {
        apply(AzureConfigKey::AccountName, account);
    }
    if conn.use_emulator {
        apply(AzureConfigKey::UseEmulator, "true");
    }
    if let Some(endpoint) = &conn.endpoint {
        apply(AzureConfigKey::Endpoint, endpoint);
    }
    for (key, value) in conn.auth.config_entries() {
        if let Ok(key) = AzureConfigKey::from_str(key) {
            apply(key, &value);
        }
    }
    out
}

/// The hosts `conn` connects to under the process environment
/// ([`EffectiveSettings::hosts`]).
pub fn effective_hosts(conn: &AzureConnection) -> Vec<String> {
    effective_settings(conn).hosts()
}

/// [`EffectiveSettings`] as the configured builder itself holds them, for
/// checking [`effective_settings_with`] against the client.
#[doc(hidden)]
pub fn builder_settings_with(
    conn: &AzureConnection,
    vars: impl IntoIterator<Item = (String, String)>,
) -> Result<EffectiveSettings, FaucetError> {
    let vars: Vec<(String, String)> = vars.into_iter().collect();
    let b = configured_builder_with(conn, vars.clone())?;
    Ok(EffectiveSettings {
        account: b.get_config_value(&AzureConfigKey::AccountName),
        endpoint: b.get_config_value(&AzureConfigKey::Endpoint),
        use_emulator: b
            .get_config_value(&AzureConfigKey::UseEmulator)
            .is_some_and(|v| truthy(&v)),
        use_fabric_endpoint: b
            .get_config_value(&AzureConfigKey::UseFabricEndpoint)
            .is_some_and(|v| truthy(&v)),
        emulator_url: vars
            .into_iter()
            .find(|(k, _)| k == "AZURITE_BLOB_STORAGE_URL")
            .map(|(_, v)| v),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn host_identity_follows_the_selected_credentials() {
        let cs = |s: &str| json!({"auth": {"type": "connection_string", "config": {"connection_string": s}}});
        assert_eq!(host_identity(&json!({})), vec!["auth"]);
        assert!(host_identity(&json!({"use_emulator": true})).is_empty());
        assert_eq!(
            host_identity(&json!({"auth": {"type": "managed_identity"}})),
            vec!["auth"]
        );
        assert_eq!(
            host_identity(&cs("AccountName=a;BlobEndpoint=https://a.example")),
            vec!["auth"]
        );
        assert!(host_identity(&cs("AccountName=a;AccountKey=k")).is_empty());
        assert!(
            host_identity(&cs(
                "BlobEndpoint=https://a.example;SharedAccessSignature=sv=1"
            ))
            .is_empty()
        );
        assert!(host_identity(&cs("UseDevelopmentStorage=true")).is_empty());
        for t in ["account_key", "sas_token", "service_principal"] {
            assert!(
                host_identity(&json!({"auth": {"type": t}})).is_empty(),
                "{t}"
            );
        }
        assert_eq!(
            host_identity_at(&json!({"staging": {}}), "/staging"),
            vec!["staging.auth"]
        );
    }

    #[tokio::test]
    async fn ambient_clients_are_refused_inside_a_caller_run() {
        let with = |auth: AzureCredentials| AzureConnection::new("c").account("acct").auth(auth);
        faucet_core::resolve::with_host_credentials(false, async {
            for auth in [
                AzureCredentials::Default,
                AzureCredentials::ManagedIdentity { client_id: None },
                AzureCredentials::ConnectionString {
                    connection_string: "AccountName=acct;BlobEndpoint=https://acct.example".into(),
                },
            ] {
                let err = configured_builder_with(&with(auth), Vec::new())
                    .expect_err("refused")
                    .to_string();
                assert!(err.contains("azure: `auth`"), "{err}");
            }
            assert!(
                configured_builder_with(
                    &with(AzureCredentials::Default).use_emulator(true),
                    Vec::new()
                )
                .is_ok()
            );
            assert!(
                configured_builder_with(
                    &with(AzureCredentials::Default),
                    vec![("AZURE_STORAGE_USE_EMULATOR".into(), "true".into())]
                )
                .is_ok()
            );
            let key = AzureCredentials::AccountKey {
                account_key: "a2V5".into(),
            };
            assert!(configured_builder_with(&with(key), Vec::new()).is_ok());
        })
        .await;
        assert!(configured_builder_with(&with(AzureCredentials::Default), Vec::new()).is_ok());
    }

    fn env(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn effective_settings_match_the_builder() {
        let strings = [
            "AccountName=acct;AccountKey=a2V5;EndpointSuffix=core.windows.net",
            "AccountName=acct;EndpointSuffix=evil.example;accountname=ok;endpointsuffix=core.windows.net",
            "AccountName=acct;EndpointSuffix=x@evil.example",
            "AccountName=acct;DefaultEndpointsProtocol=http;EndpointSuffix=core.windows.net",
            "BlobEndpoint=http://10.0.0.5:10000/acct;AccountName=acct",
            "UseDevelopmentStorage=true",
            "UseDevelopmentStorage=false;AccountName=acct",
            "AccountKey=a2V5",
            "AccountName=acct;AccountName=other;BlobEndpoint=https://a/;BlobEndpoint=https://b/",
        ];
        let mut conns = vec![
            AzureConnection::new("c"),
            AzureConnection::new("c").account("acct"),
            AzureConnection::new("c").use_emulator(true).account("acct"),
            AzureConnection::new("c").endpoint("https://private.example/"),
        ];
        for s in strings {
            for base in [
                AzureConnection::new("c"),
                AzureConnection::new("c").endpoint("https://private.example/"),
                AzureConnection::new("c").account("acct").use_emulator(true),
            ] {
                conns.push(base.auth(AzureCredentials::ConnectionString {
                    connection_string: s.into(),
                }));
            }
        }
        let envs = [
            env(&[]),
            env(&[
                ("AZURE_STORAGE_ACCOUNT_NAME", "envacct"),
                ("AZURE_STORAGE_ENDPOINT", "https://env.example/"),
                ("AZURE_STORAGE_USE_EMULATOR", "yes"),
                ("AZURITE_BLOB_STORAGE_URL", "http://azurite:10000"),
            ]),
            env(&[
                ("AZURE_USE_FABRIC_ENDPOINT", "true"),
                ("AZURE_STORAGE_ACCOUNT_NAME", "envacct"),
            ]),
        ];
        for conn in &conns {
            for vars in &envs {
                let ours = effective_settings_with(conn, vars.clone());
                match builder_settings_with(conn, vars.clone()) {
                    Ok(client) => assert_eq!(ours, client, "{conn:?} {vars:?}"),
                    Err(e) => panic!("{conn:?}: {e}"),
                }
            }
        }
        let azurite =
            effective_settings_with(&AzureConnection::new("c").use_emulator(true), env(&[]));
        assert_eq!(azurite.hosts(), vec!["127.0.0.1"]);
        let private = AzureConnection::new("c")
            .account("acct")
            .endpoint("https://10.1.2.3/acct")
            .auth(AzureCredentials::ConnectionString {
                connection_string: "AccountName=acct;AccountKey=a2V5".into(),
            });
        assert_eq!(
            effective_settings_with(&private, env(&[])).hosts(),
            vec!["10.1.2.3"]
        );
        let fabric = effective_settings_with(
            &AzureConnection::new("c").account("acct"),
            env(&[("AZURE_USE_FABRIC_ENDPOINT", "true")]),
        );
        assert_eq!(fabric.hosts(), vec!["acct.blob.fabric.microsoft.com"]);
        assert!(
            effective_settings_with(&AzureConnection::new("c"), env(&[]))
                .hosts()
                .is_empty()
        );
        assert!(effective_hosts(&AzureConnection::new("c").account("acct")).len() <= 1);
    }

    #[test]
    fn account_names_follow_the_service_rule() {
        for bad in ["evil.example#", "Acct", "ab"] {
            let e = build_store(&AzureConnection::new("c").account(bad))
                .expect_err(bad)
                .to_string();
            assert!(e.contains("`account`") && !e.contains("evil"), "{e}");
        }
        for (cs, needle) in [
            ("AccountName=evil.example#;AccountKey=a2V5", "AccountName"),
            (
                "AccountName=acct;DefaultEndpointsProtocol=https://evil.example/?;EndpointSuffix=x",
                "DefaultEndpointsProtocol",
            ),
        ] {
            let creds = AzureCredentials::ConnectionString {
                connection_string: cs.into(),
            };
            let conn = AzureConnection::new("c").auth(creds.clone());
            assert!(builder_settings_with(&conn, env(&[])).is_err());
            let e = creds.validate().unwrap_err().to_string();
            assert!(e.contains(needle) && !e.contains("evil"), "{e}");
        }
    }

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
        assert!(entries.contains(&(
            "azure_storage_endpoint",
            "https://acct.blob.core.windows.net".to_string()
        )));
        assert_eq!(entries.len(), 3);
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
    fn connection_string_honours_suffix_protocol_and_development_storage() {
        let sovereign = parse_connection_string(
            "DefaultEndpointsProtocol=http;AccountName=acct;AccountKey=k;EndpointSuffix=core.chinacloudapi.cn",
        );
        assert!(sovereign.contains(&(
            "azure_storage_endpoint",
            "http://acct.blob.core.chinacloudapi.cn".to_string()
        )));
        assert!(sovereign.contains(&("allow_http", "true".to_string())));
        let azurite = parse_connection_string("UseDevelopmentStorage=true");
        assert_eq!(
            azurite,
            vec![("azure_storage_use_emulator", "true".to_string())]
        );
        let explicit = parse_connection_string(
            "AccountName=a;BlobEndpoint=http://127.0.0.1:10000/a;EndpointSuffix=x",
        );
        assert!(explicit.contains(&("allow_http", "true".to_string())));
        assert_eq!(
            explicit
                .iter()
                .filter(|(k, _)| *k == "azure_storage_endpoint")
                .count(),
            1
        );
        for (k, _) in sovereign.iter().chain(&azurite).chain(&explicit) {
            AzureConfigKey::from_str(k).expect("a key object_store understands");
        }
    }

    #[test]
    fn an_explicit_credential_ignores_credentials_in_the_environment() {
        let vars = |pairs: &[(&str, &str)]| {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<Vec<_>>()
        };
        let env = vars(&[
            ("AZURE_STORAGE_ACCOUNT_KEY", "server-key"),
            ("AZURE_CLIENT_SECRET", "s"),
            ("AZURE_FEDERATED_TOKEN_FILE", "/t"),
            ("AZURE_STORAGE_ACCOUNT_NAME", "acct"),
            ("AZURE_ALLOW_HTTP", "true"),
            ("AZURE_MSI_RESOURCE_ID", "r"),
            ("IDENTITY_ENDPOINT", "http://169.254.169.254"),
            ("PATH", "/bin"),
        ]);
        let sas = AzureCredentials::SasToken {
            sas_token: "sv=2021&sig=abc".into(),
        };
        let kept: Vec<_> = env_fallback(&sas, env.clone())
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(
            kept,
            vec![
                AzureConfigKey::AccountName,
                AzureConfigKey::Client(object_store::ClientConfigKey::AllowHttp)
            ]
        );
        let mi = AzureCredentials::ManagedIdentity { client_id: None };
        let kept: Vec<_> = env_fallback(&mi, env).into_iter().map(|(k, _)| k).collect();
        assert!(kept.contains(&AzureConfigKey::MsiResourceId));
        assert!(kept.contains(&AzureConfigKey::MsiEndpoint));
        assert!(!kept.contains(&AzureConfigKey::AccessKey));

        let conn = AzureConnection::new("c").account("acct").auth(sas);
        assert!(build_store(&conn).is_ok());
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
            authority_host: None,
        };
        let entries = creds.config_entries();
        assert!(entries.contains(&("azure_storage_client_id", "cid".to_string())));
        assert!(entries.contains(&("azure_storage_client_secret", "secret".to_string())));
        assert!(entries.contains(&("azure_storage_tenant_id", "tid".to_string())));
    }

    fn principal(authority_host: Option<&str>) -> AzureCredentials {
        AzureCredentials::ServicePrincipal {
            client_id: "cid".into(),
            client_secret: "sec".into(),
            tenant_id: "tid".into(),
            authority_host: authority_host.map(str::to_string),
        }
    }

    fn resolved_authority_host(auth: AzureCredentials) -> Option<String> {
        let conn = AzureConnection::new("c").account("acct").auth(auth);
        configured_builder(&conn)
            .unwrap()
            .get_config_value(&AzureConfigKey::AuthorityHost)
    }

    #[test]
    fn service_principal_defaults_to_the_public_cloud_authority() {
        let creds = principal(None);
        assert!(
            !creds
                .config_entries()
                .iter()
                .any(|(k, _)| *k == "azure_storage_authority_host")
        );
        assert_eq!(resolved_authority_host(creds), None);
        let v: AzureCredentials = serde_json::from_value(json!({
            "type": "service_principal",
            "config": {"client_id": "c", "client_secret": "s", "tenant_id": "t"}
        }))
        .unwrap();
        assert!(matches!(
            v,
            AzureCredentials::ServicePrincipal {
                authority_host: None,
                ..
            }
        ));
    }

    #[test]
    fn service_principal_authority_host_reaches_the_builder() {
        assert_eq!(
            resolved_authority_host(principal(Some("https://login.microsoftonline.us/"))),
            Some("https://login.microsoftonline.us".to_string())
        );
        assert_eq!(
            resolved_authority_host(principal(Some("http://127.0.0.1:8080"))),
            Some("http://127.0.0.1:8080".to_string())
        );
        assert_eq!(
            resolved_authority_host(principal(Some("http://[::1]:8080"))),
            Some("http://[::1]:8080".to_string())
        );
        assert_eq!(
            resolved_authority_host(principal(Some("http://localhost:8080"))),
            Some("http://localhost:8080".to_string())
        );
    }

    #[test]
    fn service_principal_rejects_a_bad_authority_host() {
        for (host, why) in [
            ("login.microsoftonline.us", "is not a URL"),
            ("http://login.microsoftonline.us", "must use https://"),
            ("ftp://login.microsoftonline.us", "not ftp://"),
            ("https://user:pw@login.microsoftonline.us", "credentials"),
            ("https://login.microsoftonline.us/?x=1", "query or fragment"),
            ("https://login.microsoftonline.us/#f", "query or fragment"),
            ("unix:/run/sock", "has no host"),
        ] {
            let conn = AzureConnection::new("c")
                .account("acct")
                .auth(principal(Some(host)));
            match build_store(&conn) {
                Err(FaucetError::Config(msg)) => {
                    assert!(msg.contains(why) && msg.contains(host), "{host}: {msg}")
                }
                other => panic!("{host}: expected a Config error, got {:?}", other.err()),
            }
        }
        assert!(AzureCredentials::Default.validate().is_ok());
    }

    #[test]
    fn service_principal_keeps_the_authority_host_from_the_environment() {
        let env = vec![(
            "AZURE_AUTHORITY_HOST".to_string(),
            "https://login.chinacloudapi.cn".to_string(),
        )];
        let kept: Vec<_> = env_fallback(&principal(None), env.clone())
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(kept, vec![AzureConfigKey::AuthorityHost]);
        let sas = AzureCredentials::SasToken {
            sas_token: "sv=1".into(),
        };
        assert!(env_fallback(&sas, env).is_empty());
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
        let creds = principal(Some("https://login.microsoftonline.us"));
        let v = serde_json::to_value(&creds).unwrap();
        assert_eq!(v["type"], "service_principal");
        assert_eq!(
            v["config"]["authority_host"],
            "https://login.microsoftonline.us"
        );
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
                    authority_host: None,
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
