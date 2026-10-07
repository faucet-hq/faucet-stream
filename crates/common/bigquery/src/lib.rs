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
    access_token_bq(creds).await.map_err(|e| match e {
        BQError::InvalidServiceAccountKey(e) => {
            FaucetError::Auth(format!("invalid service account JSON: {e}"))
        }
        BQError::InvalidAuthorizedUserAuthenticator(e) => {
            FaucetError::Auth(format!("invalid authorized_user ADC file: {e}"))
        }
        other => FaucetError::Auth(format!("BigQuery token mint failed: {other}")),
    })
}

pub(crate) async fn access_token_bq(creds: &BigQueryCredentials) -> Result<String, BQError> {
    use gcp_bigquery_client::yup_oauth2::{
        self, ApplicationDefaultCredentialsAuthenticator, ApplicationDefaultCredentialsFlowOpts,
        ServiceAccountAuthenticator, authenticator::ApplicationDefaultCredentialsTypes,
    };
    let scopes = [BQ_OAUTH_SCOPE];
    let token = match creds {
        BigQueryCredentials::ServiceAccountKey { json } => {
            let key = yup_oauth2::parse_service_account_key(json)
                .map_err(BQError::InvalidServiceAccountKey)?;
            ServiceAccountAuthenticator::builder(key)
                .build()
                .await
                .map_err(BQError::InvalidServiceAccountAuthenticator)?
                .token(&scopes)
                .await?
        }
        BigQueryCredentials::ServiceAccountKeyPath { path } => {
            let key = yup_oauth2::read_service_account_key(path).await?;
            ServiceAccountAuthenticator::builder(key)
                .build()
                .await
                .map_err(BQError::InvalidServiceAccountAuthenticator)?
                .token(&scopes)
                .await?
        }
        BigQueryCredentials::ApplicationDefault => match authorized_user_adc()? {
            Some(secret) => {
                yup_oauth2::AuthorizedUserAuthenticator::builder(secret)
                    .build()
                    .await
                    .map_err(BQError::InvalidAuthorizedUserAuthenticator)?
                    .token(&scopes)
                    .await?
            }
            None => {
                let opts = ApplicationDefaultCredentialsFlowOpts::default();
                match ApplicationDefaultCredentialsAuthenticator::builder(opts).await {
                    ApplicationDefaultCredentialsTypes::ServiceAccount(b) => b.build().await,
                    ApplicationDefaultCredentialsTypes::InstanceMetadata(b) => b.build().await,
                }
                .map_err(BQError::InvalidApplicationDefaultCredentialsAuthenticator)?
                .token(&scopes)
                .await?
            }
        },
    };
    token.token().map(str::to_string).ok_or(BQError::NoToken)
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
fn authorized_user_adc()
-> Result<Option<gcp_bigquery_client::yup_oauth2::authorized_user::AuthorizedUserSecret>, BQError> {
    let Some(path) = adc_path(
        std::env::var("GOOGLE_APPLICATION_CREDENTIALS").ok(),
        std::env::var("HOME").ok(),
    ) else {
        return Ok(None);
    };
    let Ok(contents) = std::fs::read_to_string(&path) else {
        return Ok(None);
    };
    parse_authorized_user(&contents)
}

fn parse_authorized_user(
    contents: &str,
) -> Result<Option<gcp_bigquery_client::yup_oauth2::authorized_user::AuthorizedUserSecret>, BQError>
{
    let is_user = serde_json::from_str::<serde_json::Value>(contents)
        .ok()
        .and_then(|v| {
            v.get("type")
                .and_then(serde_json::Value::as_str)
                .map(|t| t == "authorized_user")
        })
        .unwrap_or(false);
    if !is_user {
        return Ok(None);
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
