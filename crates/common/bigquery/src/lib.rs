#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-common-bigquery
//!
//! Shared credential configuration and client construction for the
//! [`faucet-stream`](https://crates.io/crates/faucet-stream) BigQuery source
//! and sink connectors.
//!
//! - [`BigQueryCredentials`] — service-account key file, inline service-account
//!   JSON, or Application Default Credentials.
//! - [`build_client`] — async helper that turns a [`BigQueryCredentials`] into
//!   a ready-to-use [`gcp_bigquery_client::Client`].
//!
//! `BigQueryCredentials` derives `Serialize`, `Deserialize`, and `JsonSchema`
//! so it round-trips through YAML/JSON configs and CLI introspection. Its
//! `Debug` impl masks inline JSON as `"***"` while leaving the key path
//! visible.

use faucet_core::FaucetError;
use gcp_bigquery_client::Client;
use gcp_bigquery_client::error::BQError;

pub mod raw;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// How to authenticate with Google BigQuery.
///
/// Serializes as `{ type: <method>, config: { … } }` (adjacent tagging,
/// snake_case discriminators) — the consistent auth wire shape shared by
/// every faucet connector.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum BigQueryCredentials {
    /// Path to a service account JSON key file.
    ServiceAccountKeyPath {
        /// Filesystem path to the service-account JSON key.
        path: String,
    },
    /// Inline service account JSON key content.
    ServiceAccountKey {
        /// Service-account JSON key as an inline string.
        json: String,
    },
    /// Use application default credentials (e.g. workload identity, `gcloud auth`).
    ApplicationDefault,
}

impl std::fmt::Debug for BigQueryCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ServiceAccountKeyPath { path } => f
                .debug_struct("ServiceAccountKeyPath")
                .field("path", path)
                .finish(),
            Self::ServiceAccountKey { .. } => write!(f, "ServiceAccountKey(***)"),
            Self::ApplicationDefault => write!(f, "ApplicationDefault"),
        }
    }
}

/// Build a [`gcp_bigquery_client::Client`] from a faucet credential spec.
///
/// Returns [`FaucetError::Auth`] on authentication failures and on inline
/// service-account JSON that fails to parse.
pub async fn build_client(creds: &BigQueryCredentials) -> Result<Client, FaucetError> {
    let mut builder = gcp_bigquery_client::client_builder::ClientBuilder::new();
    builder.with_client(http_client()?);
    match creds {
        BigQueryCredentials::ServiceAccountKeyPath { path } => builder
            .build_from_service_account_key_file(path)
            .await
            .map_err(|e| FaucetError::Auth(format!("BigQuery auth failed: {e}"))),
        BigQueryCredentials::ServiceAccountKey { json } => {
            let sa_key = serde_json::from_str(json)
                .map_err(|e| FaucetError::Auth(format!("invalid service account JSON: {e}")))?;
            builder
                .build_from_service_account_key(sa_key, false)
                .await
                .map_err(|e| FaucetError::Auth(format!("BigQuery auth failed: {e}")))
        }
        BigQueryCredentials::ApplicationDefault => builder
            .build_from_application_default_credentials()
            .await
            .map_err(|e| FaucetError::Auth(format!("BigQuery auth failed: {e}"))),
    }
}

/// TCP connect timeout of the REST client [`build_client`] creates.
pub const HTTP_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Idle-read timeout of that client: a response that sends nothing for this
/// long fails instead of hanging the run on a half-open connection.
pub const HTTP_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

/// OAuth scope for BigQuery API calls made outside the typed client.
pub const BQ_OAUTH_SCOPE: &str = "https://www.googleapis.com/auth/bigquery";

/// Mint a BigQuery access token from `creds` for a request the typed client
/// cannot make (media uploads, schema-tolerant fallbacks).
pub async fn access_token(creds: &BigQueryCredentials) -> Result<String, FaucetError> {
    access_token_bq(creds).await.map_err(auth_error)
}

/// A token provider for `creds`, built once: it caches the access token and
/// refreshes it shortly before expiry, so every request made through it reuses
/// one token instead of minting a new one.
pub async fn token_provider(creds: &BigQueryCredentials) -> Result<TokenProvider, FaucetError> {
    token_provider_bq(creds).await.map_err(auth_error)
}

/// Build a [`gcp_bigquery_client::Client`] together with the token provider it
/// authenticates with, so requests the typed client cannot make share its
/// cached token.
pub async fn build_client_with_tokens(
    creds: &BigQueryCredentials,
) -> Result<(Client, TokenProvider), FaucetError> {
    let tokens = token_provider(creds).await?;
    let mut builder = gcp_bigquery_client::client_builder::ClientBuilder::new();
    builder.with_client(http_client()?);
    let client = builder
        .build_from_authenticator(tokens.clone())
        .await
        .map_err(|e| FaucetError::Auth(format!("BigQuery auth failed: {e}")))?;
    Ok((client, tokens))
}

/// A shared, cloneable BigQuery access-token provider.
pub type TokenProvider = std::sync::Arc<dyn gcp_bigquery_client::auth::Authenticator>;

fn auth_error(e: BQError) -> FaucetError {
    match e {
        BQError::InvalidServiceAccountKey(e) => {
            FaucetError::Auth(format!("invalid service account JSON: {e}"))
        }
        BQError::InvalidAuthorizedUserAuthenticator(e) => {
            FaucetError::Auth(format!("invalid authorized_user ADC file: {e}"))
        }
        other => FaucetError::Auth(format!("BigQuery token mint failed: {other}")),
    }
}

#[derive(Clone)]
struct CachedTokens {
    auth: gcp_bigquery_client::yup_oauth2::authenticator::DefaultAuthenticator,
}

#[faucet_core::async_trait]
impl gcp_bigquery_client::auth::Authenticator for CachedTokens {
    async fn access_token(&self) -> Result<String, BQError> {
        self.auth
            .token(&[BQ_OAUTH_SCOPE])
            .await?
            .token()
            .map(str::to_string)
            .ok_or(BQError::NoToken)
    }
}

pub(crate) async fn access_token_bq(creds: &BigQueryCredentials) -> Result<String, BQError> {
    token_provider_bq(creds).await?.access_token().await
}

pub(crate) async fn token_provider_bq(
    creds: &BigQueryCredentials,
) -> Result<TokenProvider, BQError> {
    use gcp_bigquery_client::yup_oauth2::{
        self, ApplicationDefaultCredentialsAuthenticator, ApplicationDefaultCredentialsFlowOpts,
        ServiceAccountAuthenticator, authenticator::ApplicationDefaultCredentialsTypes,
    };
    let auth = match creds {
        BigQueryCredentials::ServiceAccountKey { json } => {
            let key = yup_oauth2::parse_service_account_key(json)
                .map_err(BQError::InvalidServiceAccountKey)?;
            ServiceAccountAuthenticator::builder(key)
                .build()
                .await
                .map_err(BQError::InvalidServiceAccountAuthenticator)?
        }
        BigQueryCredentials::ServiceAccountKeyPath { path } => {
            let key = yup_oauth2::read_service_account_key(path).await?;
            ServiceAccountAuthenticator::builder(key)
                .build()
                .await
                .map_err(BQError::InvalidServiceAccountAuthenticator)?
        }
        BigQueryCredentials::ApplicationDefault => match authorized_user_adc().await? {
            Some(secret) => yup_oauth2::AuthorizedUserAuthenticator::builder(secret)
                .build()
                .await
                .map_err(BQError::InvalidAuthorizedUserAuthenticator)?,
            None => {
                let opts = ApplicationDefaultCredentialsFlowOpts::default();
                match ApplicationDefaultCredentialsAuthenticator::builder(opts).await {
                    ApplicationDefaultCredentialsTypes::ServiceAccount(b) => b.build().await,
                    ApplicationDefaultCredentialsTypes::InstanceMetadata(b) => b.build().await,
                }
                .map_err(BQError::InvalidApplicationDefaultCredentialsAuthenticator)?
            }
        },
    };
    Ok(std::sync::Arc::new(CachedTokens { auth }))
}

/// The application-default credentials file: `GOOGLE_APPLICATION_CREDENTIALS`,
/// else gcloud's well-known location under `home`.
fn adc_path(gac: Option<String>, home: Option<String>) -> Option<std::path::PathBuf> {
    gac.map(std::path::PathBuf::from).or_else(|| {
        home.map(|h| {
            std::path::PathBuf::from(h).join(".config/gcloud/application_default_credentials.json")
        })
    })
}

/// The `authorized_user` secret in the ADC file, if that is what it holds
/// (`gcloud auth application-default login`). yup-oauth2's ADC flow only
/// knows service accounts and instance metadata.
async fn authorized_user_adc()
-> Result<Option<gcp_bigquery_client::yup_oauth2::authorized_user::AuthorizedUserSecret>, BQError> {
    let Some(path) = adc_path(
        std::env::var("GOOGLE_APPLICATION_CREDENTIALS").ok(),
        std::env::var("HOME").ok(),
    ) else {
        return Ok(None);
    };
    let Ok(contents) = tokio::fs::read_to_string(&path).await else {
        return Ok(None);
    };
    parse_authorized_user(&contents)
}

fn parse_authorized_user(
    contents: &str,
) -> Result<Option<gcp_bigquery_client::yup_oauth2::authorized_user::AuthorizedUserSecret>, BQError>
{
    let kind = serde_json::from_str::<serde_json::Value>(contents)
        .ok()
        .and_then(|v| v.get("type")?.as_str().map(str::to_string));
    match kind.as_deref() {
        Some("authorized_user") => {}
        None | Some("service_account") => return Ok(None),
        Some(other) => {
            return Err(BQError::InvalidApplicationDefaultCredentialsAuthenticator(
                std::io::Error::other(format!(
                    "the application-default credentials file has type {other:?}, which is \
                     not supported; use a service-account key or \
                     `gcloud auth application-default login`"
                )),
            ));
        }
    }
    serde_json::from_str(contents)
        .map(Some)
        .map_err(|e| BQError::InvalidAuthorizedUserAuthenticator(e.into()))
}

pub(crate) fn http_client_bq() -> Result<reqwest_bq::Client, BQError> {
    http_client().map_err(|e| BQError::ConnectionPoolError(e.to_string()))
}

fn http_client() -> Result<reqwest_bq::Client, FaucetError> {
    reqwest_bq::Client::builder()
        .connect_timeout(HTTP_CONNECT_TIMEOUT)
        .read_timeout(HTTP_READ_TIMEOUT)
        .build()
        .map_err(|e| FaucetError::Config(format!("BigQuery: cannot build HTTP client: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adc_file_location_and_authorized_user_detection() {
        assert_eq!(
            adc_path(Some("/k.json".into()), Some("/h".into())),
            Some(std::path::PathBuf::from("/k.json"))
        );
        assert_eq!(
            adc_path(None, Some("/h".into())),
            Some(std::path::PathBuf::from(
                "/h/.config/gcloud/application_default_credentials.json"
            ))
        );
        assert_eq!(adc_path(None, None), None);
        let user =
            r#"{"type":"authorized_user","client_id":"c","client_secret":"s","refresh_token":"r"}"#;
        assert!(parse_authorized_user(user).unwrap().is_some());
        assert!(
            parse_authorized_user(r#"{"type":"service_account"}"#)
                .unwrap()
                .is_none()
        );
        assert!(parse_authorized_user("not json").unwrap().is_none());
        assert!(parse_authorized_user(r#"{"type":"authorized_user"}"#).is_err());
        let external = parse_authorized_user(r#"{"type":"external_account"}"#)
            .unwrap_err()
            .to_string();
        assert!(external.contains("\"external_account\""), "{external}");
    }

    #[tokio::test]
    async fn build_client_with_tokens_shares_one_provider() {
        let json = serde_json::json!({
            "type": "service_account",
            "project_id": "dummy",
            "private_key_id": "dummy",
            "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDNk6cKkWP/4NMu\nWb3s24YHfM639IXzPtTev06PUVVQnyHmT1bZgQ/XB6BvIRaReqAqnQd61PAGtX3e\n8XocTw+u/ZfiPJOf+jrXMkRBpiBh9mbyEIqBy8BC20OmsUc+O/YYh/qRccvRfPI7\n3XMabQ8eFWhI6z/t35oRpvEVFJnSIgyV4JR/L/cjtoKnxaFwjBzEnxPiwtdy4olU\nKO/1maklXexvlO7onC7CNmPAjuEZKzdMLzFszikCDnoKJC8k6+2GZh0/JDMAcAF4\nwxlKNQ89MpHVRXZ566uKZg0MqZqkq5RXPn6u7yvNHwZ0oahHT+8ixPPrAEjuPEKM\nUPzVRz71AgMBAAECggEAfdbVWLW5Befkvam3hea2+5xdmeN3n3elrJhkiXxbAhf3\nE1kbq9bCEHmdrokNnI34vz0SWBFCwIiWfUNJ4UxQKGkZcSZto270V8hwWdNMXUsM\npz6S2nMTxJkdp0s7dhAUS93o9uE2x4x5Z0XecJ2ztFGcXY6Lupu2XvnW93V9109h\nkY3uICLdbovJq7wS/fO/AL97QStfEVRWW2agIXGvoQG5jOwfPh86GZZRYP9b8VNw\ntkAUJe4qpzNbWs9AItXOzL+50/wsFkD/iWMGWFuU8DY5ZwsL434N+uzFlaD13wtZ\n63D+tNAxCSRBfZGQbd7WxJVFfZe/2vgjykKWsdyNAQKBgQDnEBgSI836HGSRk0Ub\nDwiEtdfh2TosV+z6xtyU7j/NwjugTOJEGj1VO/TMlZCEfpkYPLZt3ek2LdNL66n8\nDyxwzTT5Q3D/D0n5yE3mmxy13Qyya6qBYvqqyeWNwyotGM7hNNOix1v9lEMtH5Rd\nUT0gkThvJhtrV663bcAWCALmtQKBgQDjw2rYlMUp2TUIa2/E7904WOnSEG85d+nc\norhzthX8EWmPgw1Bbfo6NzH4HhebTw03j3NjZdW2a8TG/uEmZFWhK4eDvkx+rxAa\n6EwamS6cmQ4+vdep2Ac4QCSaTZj02YjHb06Be3gptvpFaFrotH2jnpXxggdiv8ul\n6x+ooCffQQKBgQCR3ykzGoOI6K/c75prELyR+7MEk/0TzZaAY1cSdq61GXBHLQKT\nd/VMgAN1vN51pu7DzGBnT/dRCvEgNvEjffjSZdqRmrAVdfN/y6LSeQ5RCfJgGXSV\nJoWVmMxhCNrxiX3h01Xgp/c9SYJ3VD54AzeR/dwg32/j/oEAsDraLciXGQKBgQDF\nMNc8k/DvfmJv27R06Ma6liA6AoiJVMxgfXD8nVUDW3/tBCVh1HmkFU1p54PArvxe\nchAQqoYQ3dUMBHeh6ZRJaYp2ATfxJlfnM99P1/eHFOxEXdBt996oUMBf53bZ5cyJ\n/lAVwnQSiZy8otCyUDHGivJ+mXkTgcIq8BoEwERFAQKBgQDmImBaFqoMSVihqHIf\nDa4WZqwM7ODqOx0JnBKrKO8UOc51J5e1vpwP/qRpNhUipoILvIWJzu4efZY7GN5C\nImF9sN3PP6Sy044fkVPyw4SYEisxbvp9tfw8Xmpj/pbmugkB2ut6lz5frmEBoJSN\n3osZlZTgx+pM3sO6ITV6U4ID2Q==\n-----END PRIVATE KEY-----\n",
            "client_email": "dummy@developer.gserviceaccount.com",
            "client_id": "dummy",
            "auth_uri": "http://127.0.0.1:1/auth",
            "token_uri": "http://127.0.0.1:1/token",
        })
        .to_string();
        let (_client, tokens) =
            build_client_with_tokens(&BigQueryCredentials::ServiceAccountKey { json })
                .await
                .expect("no network until a token is needed");
        assert!(
            tokens.access_token().await.is_err(),
            "token endpoint is unreachable"
        );
    }

    #[tokio::test]
    async fn token_provider_and_client_report_a_bad_inline_key() {
        let creds = BigQueryCredentials::ServiceAccountKey {
            json: "{not json".into(),
        };
        let err = token_provider(&creds).await.err().expect("bad key");
        assert!(
            matches!(&err, FaucetError::Auth(m) if m.contains("invalid service account JSON")),
            "{err:?}"
        );
        let err = build_client_with_tokens(&creds)
            .await
            .err()
            .expect("bad key");
        assert!(matches!(err, FaucetError::Auth(_)), "{err:?}");
    }

    #[tokio::test]
    async fn access_token_reports_a_bad_inline_key() {
        let err = access_token(&BigQueryCredentials::ServiceAccountKey { json: "{}".into() })
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("invalid service account JSON"),
            "{err}"
        );
    }

    #[test]
    fn debug_masks_inline_service_account_key() {
        let creds = BigQueryCredentials::ServiceAccountKey {
            json: "secret-json".into(),
        };
        let debug = format!("{creds:?}");
        assert!(debug.contains("***"));
        assert!(!debug.contains("secret-json"));
    }

    #[test]
    fn debug_does_not_mask_service_account_key_path() {
        let creds = BigQueryCredentials::ServiceAccountKeyPath {
            path: "/path/to/key.json".into(),
        };
        let debug = format!("{creds:?}");
        assert!(debug.contains("/path/to/key.json"));
    }

    #[test]
    fn debug_application_default_is_plain() {
        let creds = BigQueryCredentials::ApplicationDefault;
        assert_eq!(format!("{creds:?}"), "ApplicationDefault");
    }

    #[test]
    fn serde_round_trip_application_default() {
        let json = serde_json::to_string(&BigQueryCredentials::ApplicationDefault).unwrap();
        let parsed: BigQueryCredentials = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, BigQueryCredentials::ApplicationDefault));
    }

    #[test]
    fn serde_round_trip_service_account_key_path() {
        let creds = BigQueryCredentials::ServiceAccountKeyPath {
            path: "/k.json".into(),
        };
        let json = serde_json::to_string(&creds).unwrap();
        assert_eq!(
            json,
            r#"{"type":"service_account_key_path","config":{"path":"/k.json"}}"#
        );
        let parsed: BigQueryCredentials = serde_json::from_str(&json).unwrap();
        match parsed {
            BigQueryCredentials::ServiceAccountKeyPath { path } => assert_eq!(path, "/k.json"),
            _ => panic!("expected ServiceAccountKeyPath"),
        }
    }

    #[tokio::test]
    async fn build_client_with_invalid_inline_json_surfaces_auth_error() {
        let creds = BigQueryCredentials::ServiceAccountKey {
            json: "not-json".into(),
        };
        match build_client(&creds).await {
            Ok(_) => panic!("expected auth error"),
            Err(FaucetError::Auth(_)) => {}
            Err(other) => panic!("expected Auth error, got {other:?}"),
        }
    }
}
