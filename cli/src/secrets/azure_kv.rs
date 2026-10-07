//! Azure Key Vault resolver (`${azure-kv:<vault>/<secret>[/<version>]}`).
//!
//! Auth: chained credential that tries, in order:
//!   1. `ClientSecretCredential` if AZURE_TENANT_ID + AZURE_CLIENT_ID +
//!      AZURE_CLIENT_SECRET are all set.
//!   2. `ManagedIdentityCredential` (Azure VM / App Service / AKS pod identity).
//!   3. `DeveloperToolsCredential` (Azure CLI / Azure Developer CLI).
//!
//! No emulator exists; a live test gated on `AZURE_TEST=1` may be added later.

use super::SecretResolver;
use crate::error::{CliError, CliResult};
use async_trait::async_trait;
use azure_core::credentials::{AccessToken, Secret, TokenCredential, TokenRequestOptions};
use azure_identity::{ClientSecretCredential, DeveloperToolsCredential, ManagedIdentityCredential};
use std::sync::Arc;
use tokio::sync::OnceCell;

// ── Chained credential ────────────────────────────────────────────────────────

/// A lightweight credential chain: tries each source in order, returning the
/// first successful token or aggregating errors if all fail.
struct ChainedCredential {
    sources: Vec<Arc<dyn TokenCredential>>,
}

impl ChainedCredential {
    fn new(sources: Vec<Arc<dyn TokenCredential>>) -> Arc<Self> {
        Arc::new(Self { sources })
    }
}

impl std::fmt::Debug for ChainedCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChainedCredential")
            .field("sources_len", &self.sources.len())
            .finish()
    }
}

#[async_trait]
impl TokenCredential for ChainedCredential {
    async fn get_token(
        &self,
        scopes: &[&str],
        options: Option<TokenRequestOptions<'_>>,
    ) -> azure_core::Result<AccessToken> {
        let mut errors = Vec::new();
        for source in &self.sources {
            match source.get_token(scopes, options.clone()).await {
                Ok(token) => return Ok(token),
                Err(e) => errors.push(e.to_string()),
            }
        }
        Err(azure_core::Error::with_message(
            azure_core::error::ErrorKind::Credential,
            format!(
                "All Azure credential sources failed:\n{}",
                errors.join("\n")
            ),
        ))
    }
}

// ── Credential builder ────────────────────────────────────────────────────────

/// Build the default Azure credential chain for this resolver.
///
/// Returns `CliError::SecretAuthFailed` if even the chain cannot be constructed
/// (which should only happen if the Azure identity library itself is broken).
fn build_credential() -> CliResult<Arc<dyn TokenCredential>> {
    let mut sources: Vec<Arc<dyn TokenCredential>> = Vec::new();

    // 1. Service-principal via env vars (AZURE_TENANT_ID + AZURE_CLIENT_ID +
    //    AZURE_CLIENT_SECRET). Try only when all three are present.
    if let (Ok(tenant_id), Ok(client_id), Ok(client_secret)) = (
        std::env::var("AZURE_TENANT_ID"),
        std::env::var("AZURE_CLIENT_ID"),
        std::env::var("AZURE_CLIENT_SECRET"),
    ) {
        match ClientSecretCredential::new(&tenant_id, client_id, Secret::new(client_secret), None) {
            Ok(cred) => sources.push(cred),
            Err(e) => {
                tracing::debug!("Azure ClientSecretCredential build failed: {e}");
            }
        }
    }

    // 2. Managed identity (VM / App Service / AKS).
    match ManagedIdentityCredential::new(None) {
        Ok(cred) => sources.push(cred),
        Err(e) => tracing::debug!("Azure ManagedIdentityCredential build failed: {e}"),
    }

    // 3. Developer tools (Azure CLI / Azure Developer CLI).
    match DeveloperToolsCredential::new(None) {
        Ok(cred) => sources.push(cred),
        Err(e) => tracing::debug!("Azure DeveloperToolsCredential build failed: {e}"),
    }

    if sources.is_empty() {
        return Err(CliError::SecretAuthFailed {
            scheme: "azure-kv".into(),
            hint: "no Azure credentials available — set AZURE_TENANT_ID / AZURE_CLIENT_ID / \
                   AZURE_CLIENT_SECRET, use a managed identity, or run `az login`"
                .into(),
        });
    }

    Ok(ChainedCredential::new(sources))
}

// ── Resolver ──────────────────────────────────────────────────────────────────

pub struct AzureKvResolver {
    /// The Azure credential chain, built lazily on first resolve and reused
    /// thereafter so the SDK's inner `TokenCache` survives across references
    /// (#135). Errors are not cached — a failed build is retried next call.
    credential: OnceCell<Arc<dyn TokenCredential>>,
}

impl AzureKvResolver {
    pub fn new() -> Self {
        Self {
            credential: OnceCell::new(),
        }
    }

    /// Build the Azure credential chain once and reuse it. The chain (and its
    /// inner `TokenCache`) is constructed lazily on first use, so a config that
    /// never references `azure-kv` pays nothing and auth failures are deferred
    /// to the first fetch.
    async fn credential(&self) -> CliResult<Arc<dyn TokenCredential>> {
        let cred = self
            .credential
            .get_or_try_init(|| async { build_credential() })
            .await?;
        Ok(Arc::clone(cred))
    }
}

impl Default for AzureKvResolver {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SecretResolver for AzureKvResolver {
    fn scheme(&self) -> &'static str {
        "azure-kv"
    }

    async fn resolve(&self, reference: &str) -> CliResult<String> {
        super::reject_field("azure-kv", reference)?;
        // reference = "<vault>/<secret>[/<version>]"
        let mut parts = reference.splitn(3, '/');

        let vault =
            parts
                .next()
                .filter(|s| !s.is_empty())
                .ok_or_else(|| CliError::SecretFetchFailed {
                    scheme: "azure-kv".into(),
                    reference: reference.into(),
                    source: "expected '<vault>/<secret>[/<version>]'".into(),
                })?;
        let secret_name =
            parts
                .next()
                .filter(|s| !s.is_empty())
                .ok_or_else(|| CliError::SecretFetchFailed {
                    scheme: "azure-kv".into(),
                    reference: reference.into(),
                    source: "expected '<vault>/<secret>[/<version>]'".into(),
                })?;
        let version = parts.next().filter(|s| !s.is_empty());

        let credential = self.credential().await?;

        let vault_url = format!("https://{vault}.vault.azure.net/");
        let client =
            azure_security_keyvault_secrets::SecretClient::new(&vault_url, credential, None)
                .map_err(|e| CliError::SecretFetchFailed {
                    scheme: "azure-kv".into(),
                    reference: reference.into(),
                    source: Box::new(e),
                })?;

        // Version goes in the options struct, NOT a positional arg.
        let options = version.map(|v| {
            azure_security_keyvault_secrets::models::SecretClientGetSecretOptions {
                secret_version: Some(v.to_string()),
                ..Default::default()
            }
        });

        let resp = client.get_secret(secret_name, options).await.map_err(|e| {
            CliError::SecretFetchFailed {
                scheme: "azure-kv".into(),
                reference: reference.into(),
                source: Box::new(e),
            }
        })?;

        // `Response<Secret>::into_model()` is sync and deserializes the body
        // into the typed `Secret` model (the JSON format is inferred from F).
        let secret = resp.into_model().map_err(|e| CliError::SecretFetchFailed {
            scheme: "azure-kv".into(),
            reference: reference.into(),
            source: Box::new(e),
        })?;

        secret.value.ok_or_else(|| CliError::SecretNotFound {
            scheme: "azure-kv".into(),
            reference: reference.into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn credential_chain_is_built_once() {
        // #135: the credential chain (and its inner TokenCache) must be reused
        // across resolve() calls, not rebuilt — and discarded — each time.
        // Constructing the chain does no network I/O, so this is safe offline.
        let resolver = AzureKvResolver::new();
        let first = resolver.credential().await.expect("chain builds");
        let second = resolver.credential().await.expect("chain builds");
        assert!(
            Arc::ptr_eq(&first, &second),
            "credential chain should be built at most once and reused"
        );
    }

    #[test]
    fn scheme_is_azure_kv() {
        assert_eq!(AzureKvResolver::new().scheme(), "azure-kv");
    }

    #[test]
    fn default_constructs_a_resolver() {
        // Exercises the `Default` impl (no panic, fresh OnceCell).
        let _r = AzureKvResolver::default();
    }

    #[tokio::test]
    async fn empty_reference_is_a_parse_error_before_any_network() {
        // An empty reference yields an empty vault segment → fetch-failed with
        // the format hint, with no credential build / network call.
        let resolver = AzureKvResolver::new();
        match resolver.resolve("").await.unwrap_err() {
            CliError::SecretFetchFailed {
                scheme, reference, ..
            } => {
                assert_eq!(scheme, "azure-kv");
                assert_eq!(reference, "");
            }
            other => panic!("expected SecretFetchFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn missing_secret_name_segment_is_a_parse_error() {
        // "<vault>" with no "/<secret>" → the secret-name segment is missing.
        let resolver = AzureKvResolver::new();
        match resolver.resolve("myvault").await.unwrap_err() {
            CliError::SecretFetchFailed {
                scheme, reference, ..
            } => {
                assert_eq!(scheme, "azure-kv");
                assert_eq!(reference, "myvault");
            }
            other => panic!("expected SecretFetchFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn empty_secret_name_segment_is_a_parse_error() {
        // "<vault>/" → the secret-name segment is present but empty.
        let resolver = AzureKvResolver::new();
        match resolver.resolve("myvault/").await.unwrap_err() {
            CliError::SecretFetchFailed { reference, .. } => {
                assert_eq!(reference, "myvault/");
            }
            other => panic!("expected SecretFetchFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn chained_credential_aggregates_errors_when_all_fail() {
        // A chain of zero sources fails get_token with the aggregate message.
        let chain = ChainedCredential::new(Vec::new());
        let err = chain.get_token(&["scope"], None).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("All Azure credential sources failed"),
            "{err}"
        );
    }

    #[test]
    fn chained_credential_debug_reports_source_count() {
        let chain = ChainedCredential::new(Vec::new());
        let dbg = format!("{chain:?}");
        assert!(dbg.contains("ChainedCredential"), "{dbg}");
        assert!(dbg.contains("sources_len"), "{dbg}");
    }
}
