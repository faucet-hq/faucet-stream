#![cfg_attr(docsrs, feature(doc_cfg))]
//! Shared Google Cloud Spanner types for the faucet-stream ecosystem.
//!
//! This crate holds everything the `faucet-source-spanner` and
//! `faucet-sink-spanner` connectors have in common:
//!
//! - [`SpannerCredentials`] — the `{ type, config }` credentials enum shared
//!   by both connectors (mirrors `faucet-common-bigquery`).
//! - [`SpannerConnection`] — the flattened connection block (`project_id` /
//!   `instance` / `database` / `auth` / `max_sessions` / `emulator_host`)
//!   plus [`SpannerConnection::connect`] / [`SpannerConnection::connect_admin`]
//!   client builders.
//! - [`types`] — `INFORMATION_SCHEMA` type-string parsing and the mapping to
//!   JSON-Schema fragments.
//! - [`decode`] — generic Spanner row → `serde_json::Value` decoding for
//!   arbitrary queries (no compile-time schema).
//! - [`encode`] — `serde_json::Value` → Spanner mutation value encoding keyed
//!   by the destination column type.

pub mod decode;
pub mod encode;
pub mod types;

use faucet_core::FaucetError;
use gcloud_gax::conn::Environment;
use gcloud_spanner::admin::AdminClientConfig;
use gcloud_spanner::admin::client::Client as AdminClient;
use gcloud_spanner::client::google_cloud_auth::credentials::CredentialsFile;
use gcloud_spanner::client::{Client, ClientConfig};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Sessions Spanner allows per gRPC channel (a hard server-side limit the
/// client enforces at construction time).
const SESSIONS_PER_CHANNEL: usize = 100;

/// How long a session acquisition may wait for a pooled session before
/// failing. The crate default (1 s) is too tight for bursty page writes
/// against a small pool; 30 s matches the channel connect/request timeouts.
const SESSION_GET_TIMEOUT: Duration = Duration::from_secs(30);

/// Credentials used to authenticate against Cloud Spanner.
///
/// Serialized in the project-wide adjacently-tagged shape:
///
/// ```yaml
/// auth:
///   type: service_account_key_path
///   config:
///     path: /secrets/sa.json
/// ```
#[derive(Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum SpannerCredentials {
    /// Load a service-account key from a JSON file on disk.
    ServiceAccountKeyPath {
        /// Filesystem path to the service-account key JSON.
        #[schemars(extend("x-faucet-resource" = "path"))]
        path: String,
    },
    /// Inline service-account key JSON (prefer `${vault:…}` / `${env:…}`
    /// interpolation over literal keys in config files).
    ServiceAccountKey {
        /// The full service-account key JSON document.
        json: String,
    },
    /// Application Default Credentials: `GOOGLE_APPLICATION_CREDENTIALS`,
    /// `gcloud auth application-default login`, or the GCE/GKE metadata
    /// server.
    #[default]
    ApplicationDefault,
}

impl std::fmt::Debug for SpannerCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ServiceAccountKeyPath { path } => f
                .debug_struct("ServiceAccountKeyPath")
                .field("path", path)
                .finish(),
            // Never print inline key material.
            Self::ServiceAccountKey { .. } => f
                .debug_struct("ServiceAccountKey")
                .field("json", &"***")
                .finish(),
            Self::ApplicationDefault => f.debug_struct("ApplicationDefault").finish(),
        }
    }
}

fn default_max_sessions() -> usize {
    100
}

/// Connection block shared by the Spanner source and sink configs (flattened
/// into each connector config via `#[serde(flatten)]`).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SpannerConnection {
    /// GCP project ID that owns the Spanner instance.
    pub project_id: String,
    /// Spanner instance ID.
    pub instance: String,
    /// Database name within the instance.
    pub database: String,
    /// Credentials. Defaults to Application Default Credentials.
    #[serde(default)]
    pub auth: SpannerCredentials,
    /// Upper bound on pooled Spanner sessions (server-side resources).
    /// Channels are sized automatically at one per 100 sessions.
    #[serde(default = "default_max_sessions")]
    pub max_sessions: usize,
    /// Emulator endpoint override (`host:port`, e.g. `localhost:9010`).
    /// When set, the connection is plaintext and unauthenticated — exactly
    /// what `SPANNER_EMULATOR_HOST` does, but scoped to this connector so
    /// parallel pipelines/tests don't race on a process-global env var.
    /// The env var is still honored when this field is unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-faucet-resource" = "url"))]
    pub emulator_host: Option<String>,
}

/// The config fields that make a Spanner client built from `config`
/// authenticate with the identity of the host it runs on: `auth` when it
/// selects `application_default` (also when absent) and no `emulator_host`
/// is set.
pub fn host_identity(config: &faucet_core::Value) -> Vec<String> {
    use faucet_core::capabilities::config::{non_empty_str_at, tag_at};
    let adc = tag_at(config, "/auth").unwrap_or("application_default") == "application_default";
    if adc && !non_empty_str_at(config, "/emulator_host") {
        vec!["auth".to_string()]
    } else {
        Vec::new()
    }
}

impl SpannerConnection {
    /// Refuse Application Default Credentials outside an emulator where the
    /// task may not use the host's identity.
    fn check_host_identity(&self) -> Result<(), FaucetError> {
        let emulated = self.emulator_host.is_some()
            || std::env::var_os("SPANNER_EMULATOR_HOST").is_some_and(|v| !v.is_empty());
        if matches!(self.auth, SpannerCredentials::ApplicationDefault) && !emulated {
            faucet_core::resolve::require_host_credentials("spanner", "auth")?;
        }
        Ok(())
    }

    /// Fully-qualified database path:
    /// `projects/{project}/instances/{instance}/databases/{database}`.
    pub fn database_path(&self) -> String {
        format!(
            "projects/{}/instances/{}/databases/{}",
            self.project_id, self.instance, self.database
        )
    }

    /// Validate the connection block at config-load time.
    pub fn validate(&self) -> Result<(), FaucetError> {
        for (field, value) in [
            ("project_id", &self.project_id),
            ("instance", &self.instance),
            ("database", &self.database),
        ] {
            if value.trim().is_empty() {
                return Err(FaucetError::Config(format!(
                    "spanner: `{field}` must not be empty"
                )));
            }
        }
        if self.max_sessions == 0 {
            return Err(FaucetError::Config(
                "spanner: `max_sessions` must be at least 1".into(),
            ));
        }
        Ok(())
    }

    /// Build the data-plane [`Client`]. Honors `emulator_host` (and, when
    /// unset, the `SPANNER_EMULATOR_HOST` env var via the client default).
    pub async fn connect(&self) -> Result<Client, FaucetError> {
        self.validate()?;
        self.check_host_identity()?;
        let mut config = ClientConfig::default();
        config.channel_config.num_channels = self.max_sessions.div_ceil(SESSIONS_PER_CHANNEL);
        config.session_config.min_opened = 1;
        config.session_config.max_idle = self.max_sessions;
        config.session_config.max_opened = self.max_sessions;
        config.session_config.session_get_timeout = SESSION_GET_TIMEOUT;
        if let Some(host) = &self.emulator_host {
            config.environment = Environment::Emulator(host.clone());
        }
        // Credential application is a no-op under an emulator environment,
        // so it is safe to apply unconditionally.
        let config = self.apply_data_credentials(config).await?;
        Client::new(self.database_path(), config)
            .await
            .map_err(|e| FaucetError::Custom(Box::new(e)))
    }

    /// Build the admin (DDL) client. Same credential and emulator handling
    /// as [`connect`](Self::connect).
    pub async fn connect_admin(&self) -> Result<AdminClient, FaucetError> {
        self.validate()?;
        self.check_host_identity()?;
        let mut config = AdminClientConfig::default();
        if let Some(host) = &self.emulator_host {
            config.environment = Environment::Emulator(host.clone());
        }
        let config = match &self.auth {
            SpannerCredentials::ApplicationDefault => config
                .with_auth()
                .await
                .map_err(|e| FaucetError::Auth(format!("spanner ADC: {e}")))?,
            SpannerCredentials::ServiceAccountKeyPath { path } => config
                .with_credentials(credentials_from_file(path).await?)
                .await
                .map_err(|e| FaucetError::Auth(format!("spanner key file {path}: {e}")))?,
            SpannerCredentials::ServiceAccountKey { json } => config
                .with_credentials(credentials_from_str(json).await?)
                .await
                .map_err(|e| FaucetError::Auth(format!("spanner inline key: {e}")))?,
        };
        AdminClient::new(config)
            .await
            .map_err(|e| FaucetError::Custom(Box::new(e)))
    }

    async fn apply_data_credentials(
        &self,
        config: ClientConfig,
    ) -> Result<ClientConfig, FaucetError> {
        match &self.auth {
            SpannerCredentials::ApplicationDefault => config
                .with_auth()
                .await
                .map_err(|e| FaucetError::Auth(format!("spanner ADC: {e}"))),
            SpannerCredentials::ServiceAccountKeyPath { path } => config
                .with_credentials(credentials_from_file(path).await?)
                .await
                .map_err(|e| FaucetError::Auth(format!("spanner key file {path}: {e}"))),
            SpannerCredentials::ServiceAccountKey { json } => config
                .with_credentials(credentials_from_str(json).await?)
                .await
                .map_err(|e| FaucetError::Auth(format!("spanner inline key: {e}"))),
        }
    }
}

async fn credentials_from_file(path: &str) -> Result<CredentialsFile, FaucetError> {
    CredentialsFile::new_from_file(path.to_string())
        .await
        .map_err(|e| FaucetError::Auth(format!("spanner key file {path}: {e}")))
}

async fn credentials_from_str(json: &str) -> Result<CredentialsFile, FaucetError> {
    CredentialsFile::new_from_str(json)
        .await
        .map_err(|e| FaucetError::Auth(format!("spanner inline key: {e}")))
}

/// Quote a Spanner identifier with backticks. GoogleSQL quoted identifiers
/// take string-literal escapes, so an embedded backslash or backtick is
/// backslash-escaped (a doubled backtick would end the identifier).
pub fn quote_ident_spanner(ident: &str) -> String {
    format!("`{}`", ident.replace('\\', "\\\\").replace('`', "\\`"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn host_identity_follows_the_selected_credentials() {
        assert_eq!(host_identity(&json!({})), vec!["auth"]);
        assert!(host_identity(&json!({"emulator_host": "localhost:9010"})).is_empty());
        for t in ["service_account_key_path", "service_account_key"] {
            assert!(
                host_identity(&json!({"auth": {"type": t}})).is_empty(),
                "{t}"
            );
        }
    }

    #[tokio::test]
    async fn ambient_clients_are_refused_inside_a_caller_run() {
        if std::env::var_os("SPANNER_EMULATOR_HOST").is_some() {
            return;
        }
        let conn: SpannerConnection = serde_json::from_value(json!({
            "project_id": "p", "instance": "i", "database": "d"
        }))
        .unwrap();
        faucet_core::resolve::with_host_credentials(false, async {
            let err = conn.connect().await.err().expect("refused").to_string();
            assert!(err.contains("spanner: `auth`"), "{err}");
            let err = conn
                .connect_admin()
                .await
                .err()
                .expect("refused")
                .to_string();
            assert!(err.contains("spanner: `auth`"), "{err}");
            let emulated = SpannerConnection {
                emulator_host: Some("127.0.0.1:1".into()),
                ..conn.clone()
            };
            assert!(emulated.check_host_identity().is_ok());
        })
        .await;
    }

    #[test]
    fn credentials_serde_shape_is_adjacently_tagged() {
        let c: SpannerCredentials = serde_json::from_value(json!({
            "type": "service_account_key_path",
            "config": {"path": "/tmp/sa.json"}
        }))
        .unwrap();
        assert!(matches!(
            c,
            SpannerCredentials::ServiceAccountKeyPath { ref path } if path == "/tmp/sa.json"
        ));
        let adc: SpannerCredentials =
            serde_json::from_value(json!({"type": "application_default"})).unwrap();
        assert!(matches!(adc, SpannerCredentials::ApplicationDefault));
        assert!(matches!(
            SpannerCredentials::default(),
            SpannerCredentials::ApplicationDefault
        ));
    }

    #[test]
    fn debug_masks_inline_key_material() {
        let c = SpannerCredentials::ServiceAccountKey {
            json: "{\"private_key\": \"SECRET\"}".into(),
        };
        let rendered = format!("{c:?}");
        assert!(!rendered.contains("SECRET"));
        assert!(rendered.contains("***"));
    }

    fn conn() -> SpannerConnection {
        SpannerConnection {
            project_id: "p".into(),
            instance: "i".into(),
            database: "d".into(),
            auth: SpannerCredentials::default(),
            max_sessions: default_max_sessions(),
            emulator_host: None,
        }
    }

    #[test]
    fn database_path_shape() {
        assert_eq!(conn().database_path(), "projects/p/instances/i/databases/d");
    }

    #[test]
    fn validate_rejects_empty_fields_and_zero_sessions() {
        let mut c = conn();
        c.project_id = " ".into();
        assert!(matches!(c.validate(), Err(FaucetError::Config(_))));
        let mut c = conn();
        c.instance = String::new();
        assert!(c.validate().is_err());
        let mut c = conn();
        c.database = String::new();
        assert!(c.validate().is_err());
        let mut c = conn();
        c.max_sessions = 0;
        assert!(c.validate().is_err());
        assert!(conn().validate().is_ok());
    }

    #[test]
    fn connection_defaults_from_minimal_config() {
        let c: SpannerConnection = serde_json::from_value(json!({
            "project_id": "p", "instance": "i", "database": "d"
        }))
        .unwrap();
        assert_eq!(c.max_sessions, 100);
        assert!(c.emulator_host.is_none());
        assert!(matches!(c.auth, SpannerCredentials::ApplicationDefault));
    }

    #[test]
    fn quote_ident_backslash_escapes() {
        assert_eq!(quote_ident_spanner("plain"), "`plain`");
        assert_eq!(quote_ident_spanner("we`ird"), "`we\\`ird`");
        assert_eq!(quote_ident_spanner("a\\b"), "`a\\\\b`");
    }

    #[tokio::test]
    async fn credentials_from_missing_file_is_a_typed_auth_error() {
        let Err(err) = credentials_from_file("/definitely/not/here.json").await else {
            panic!("missing key file unexpectedly loaded");
        };
        assert!(matches!(err, FaucetError::Auth(_)));
        assert!(err.to_string().contains("/definitely/not/here.json"));
    }

    #[tokio::test]
    async fn credentials_from_invalid_json_is_a_typed_auth_error() {
        let Err(err) = credentials_from_str("{not json").await else {
            panic!("invalid key json unexpectedly parsed");
        };
        assert!(matches!(err, FaucetError::Auth(_)));
        assert!(err.to_string().contains("inline key"));
    }

    /// A syntactically valid (but fake) service-account document — every
    /// field except `type` is optional in the credentials-file schema.
    const FAKE_SA: &str = r#"{"type": "service_account", "project_id": "p"}"#;

    /// Every credential variant must build under an emulator environment
    /// (where token providers are inert) and then fail fast at client
    /// construction against a dead endpoint — proving the credential arms
    /// never panic and map errors into `FaucetError`.
    #[tokio::test]
    async fn connect_exercises_every_credential_variant_against_dead_emulator() {
        let key_path = std::env::temp_dir().join("faucet-spanner-test-sa.json");
        std::fs::write(&key_path, FAKE_SA).expect("write fake key");
        let variants = [
            SpannerCredentials::ApplicationDefault,
            SpannerCredentials::ServiceAccountKey {
                json: FAKE_SA.to_string(),
            },
            SpannerCredentials::ServiceAccountKeyPath {
                path: key_path.to_string_lossy().into_owned(),
            },
        ];
        for auth in variants {
            let mut c = conn();
            // Port 1 is never a Spanner emulator: connection is refused
            // immediately, after the credential arm has already run.
            c.emulator_host = Some("127.0.0.1:1".into());
            c.auth = auth.clone();
            let Err(err) = c.connect().await else {
                panic!("data-plane connect unexpectedly succeeded for {auth:?}");
            };
            assert!(
                matches!(err, FaucetError::Custom(_)),
                "data-plane connect: unexpected error {err:?} for {auth:?}"
            );
            let Err(err) = c.connect_admin().await else {
                panic!("admin connect unexpectedly succeeded for {auth:?}");
            };
            assert!(
                matches!(err, FaucetError::Custom(_)),
                "admin connect: unexpected error {err:?} for {auth:?}"
            );
        }
    }
}
