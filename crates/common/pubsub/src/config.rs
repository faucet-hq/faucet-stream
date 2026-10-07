//! Credential + connection configuration shared by the Pub/Sub source and
//! sink. No I/O here — the client builder lives in `client.rs`.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// How to authenticate with Google Cloud Pub/Sub.
///
/// Serializes as `{ type: <method>, config: { … } }` (adjacent tagging,
/// snake_case discriminators) — the consistent auth wire shape shared by
/// every faucet connector:
/// `{ type: service_account_json_file, config: { path: "/run/secrets/sa.json" } }`.
#[derive(Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum PubsubCredentials {
    /// Application Default Credentials — honours
    /// `GOOGLE_APPLICATION_CREDENTIALS`, `GOOGLE_APPLICATION_CREDENTIALS_JSON`,
    /// gcloud user creds, and the GCE/GKE metadata server, in that order.
    #[default]
    ApplicationDefault,
    /// Path to a service-account JSON key file on disk.
    ServiceAccountJsonFile {
        /// Filesystem path to the service-account key JSON.
        path: String,
    },
    /// Service-account JSON key as an inline string. Pair with
    /// `${env:GCP_SA_JSON}` / `${secret:…}` interpolation in CLI configs so
    /// the key never sits in the config file verbatim.
    ServiceAccountJsonInline {
        /// The service-account key JSON document.
        json: String,
    },
    /// No credentials. Use with the Pub/Sub emulator, which does not validate
    /// bearer tokens — the SDK otherwise tries to fetch ADC tokens at startup
    /// and fails in environments without GCP credentials.
    Anonymous,
}

/// Never prints an inline service-account key (#789 MSG-89).
impl std::fmt::Debug for PubsubCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ApplicationDefault => f.write_str("ApplicationDefault"),
            Self::ServiceAccountJsonFile { path } => f
                .debug_struct("ServiceAccountJsonFile")
                .field("path", path)
                .finish(),
            Self::ServiceAccountJsonInline { .. } => f
                .debug_struct("ServiceAccountJsonInline")
                .field("json", &"***")
                .finish(),
            Self::Anonymous => f.write_str("Anonymous"),
        }
    }
}

/// Connection settings shared by the Pub/Sub source and sink. Flattened into
/// each connector config via `#[serde(flatten)]`, so `project_id` /
/// `endpoint` / `emulator_host` / `credentials` appear at the config top
/// level.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PubsubConnection {
    /// GCP project id that owns the topic / subscription. Required for real
    /// Pub/Sub; the emulator infers it from `PUBSUB_PROJECT_ID` when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    /// Override the Pub/Sub API endpoint (host:port or URL). Rarely needed —
    /// prefer `emulator_host` for the emulator.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// Point the client at a Pub/Sub emulator (`host:port`). When set, auth is
    /// skipped and the endpoint is taken from this value — mirrors the
    /// `PUBSUB_EMULATOR_HOST` environment variable the SDK honours.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub emulator_host: Option<String>,
    /// How to authenticate. Defaults to Application Default Credentials.
    #[serde(default)]
    pub credentials: PubsubCredentials,
}

impl PubsubConnection {
    /// Effective emulator host: the explicit config value (which wins), else
    /// the `PUBSUB_EMULATOR_HOST` environment variable. `None` = real Pub/Sub.
    pub fn effective_emulator_host(&self) -> Option<String> {
        self.explicit_emulator_host().or_else(|| {
            std::env::var("PUBSUB_EMULATOR_HOST")
                .ok()
                .filter(|h| !h.trim().is_empty())
        })
    }

    /// The configured `emulator_host`, ignoring the environment.
    pub fn explicit_emulator_host(&self) -> Option<String> {
        self.emulator_host.clone().filter(|h| !h.trim().is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn debug_never_prints_an_inline_key() {
        let c = PubsubCredentials::ServiceAccountJsonInline {
            json: "{\"private_key\":\"SECRET\"}".into(),
        };
        let d = format!("{c:?}");
        assert!(!d.contains("SECRET") && d.contains("***"), "{d}");
        assert_eq!(format!("{:?}", PubsubCredentials::Anonymous), "Anonymous");
        assert_eq!(
            format!("{:?}", PubsubCredentials::ApplicationDefault),
            "ApplicationDefault"
        );
        assert!(
            format!(
                "{:?}",
                PubsubCredentials::ServiceAccountJsonFile { path: "/p".into() }
            )
            .contains("/p")
        );
        let conn = PubsubConnection {
            emulator_host: Some(" ".into()),
            ..Default::default()
        };
        assert_eq!(conn.explicit_emulator_host(), None);
    }

    #[test]
    fn credentials_default_is_adc() {
        assert_eq!(
            PubsubCredentials::default(),
            PubsubCredentials::ApplicationDefault
        );
    }

    #[test]
    fn credentials_serde_application_default() {
        let v = serde_json::to_value(PubsubCredentials::ApplicationDefault).unwrap();
        assert_eq!(v, json!({"type": "application_default"}));
        let back: PubsubCredentials = serde_json::from_value(v).unwrap();
        assert_eq!(back, PubsubCredentials::ApplicationDefault);
    }

    #[test]
    fn credentials_serde_service_account_file_and_inline() {
        let file = PubsubCredentials::ServiceAccountJsonFile {
            path: "/run/secrets/sa.json".into(),
        };
        let v = serde_json::to_value(&file).unwrap();
        assert_eq!(
            v,
            json!({"type": "service_account_json_file", "config": {"path": "/run/secrets/sa.json"}})
        );
        assert_eq!(
            serde_json::from_value::<PubsubCredentials>(v).unwrap(),
            file
        );

        let inline = PubsubCredentials::ServiceAccountJsonInline {
            json: "{\"client_email\":\"x@y\"}".into(),
        };
        let v = serde_json::to_value(&inline).unwrap();
        assert_eq!(v["type"], "service_account_json_inline");
        assert_eq!(
            serde_json::from_value::<PubsubCredentials>(v).unwrap(),
            inline
        );
    }

    #[test]
    fn credentials_serde_anonymous() {
        let v = serde_json::to_value(PubsubCredentials::Anonymous).unwrap();
        assert_eq!(v, json!({"type": "anonymous"}));
        assert_eq!(
            serde_json::from_value::<PubsubCredentials>(v).unwrap(),
            PubsubCredentials::Anonymous
        );
    }

    #[test]
    fn connection_flatten_shape_parses() {
        let yaml = r#"
project_id: my-proj
emulator_host: "localhost:8085"
credentials: { type: anonymous }
"#;
        let c: PubsubConnection = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(c.project_id.as_deref(), Some("my-proj"));
        assert_eq!(c.emulator_host.as_deref(), Some("localhost:8085"));
        assert_eq!(c.credentials, PubsubCredentials::Anonymous);
    }

    #[test]
    fn connection_defaults() {
        let c = PubsubConnection::default();
        assert!(c.project_id.is_none());
        assert!(c.endpoint.is_none());
        assert!(c.emulator_host.is_none());
        assert_eq!(c.credentials, PubsubCredentials::ApplicationDefault);
    }

    #[test]
    fn effective_emulator_host_prefers_explicit() {
        let c = PubsubConnection {
            emulator_host: Some("localhost:8085".into()),
            ..Default::default()
        };
        assert_eq!(
            c.effective_emulator_host().as_deref(),
            Some("localhost:8085")
        );

        // Blank explicit value is ignored.
        let c = PubsubConnection {
            emulator_host: Some("   ".into()),
            ..Default::default()
        };
        // Only asserts the blank-explicit path; env-var state is process-wide
        // so we don't assert on its presence/absence here.
        let got = c.effective_emulator_host();
        assert!(got.as_deref() != Some("   "));
    }
}
