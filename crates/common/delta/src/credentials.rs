//! Object-store credentials for Delta tables.
//!
//! delta-rs authenticates through a `storage_options: HashMap<String,String>`
//! map whose keys mirror the underlying `object_store` environment variables
//! (`AWS_ACCESS_KEY_ID`, `AZURE_STORAGE_ACCOUNT_NAME`, `GOOGLE_SERVICE_ACCOUNT`,
//! …). [`DeltaCredentials`] is a typed, self-documenting front for the common
//! cases; anything it does not cover can still be set verbatim via the
//! connection's `storage_options`. Explicit `storage_options` win on key
//! collision (see [`DeltaCredentials::apply`]).

use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Object-store credentials for reading/writing a Delta table.
///
/// Uses the project-wide adjacently-tagged `{ type, config }` shape
/// (snake_case discriminators): e.g. `credentials: { type: aws, config: { … } }`.
/// The default is [`DeltaCredentials::Default`], which resolves credentials from
/// the ambient environment / instance metadata via the object-store default
/// provider chain — nothing is injected into `storage_options`.
#[derive(Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum DeltaCredentials {
    /// Resolve credentials from the ambient environment / default provider
    /// chain (AWS instance profile, `AZURE_*` env, GCP ADC, …). Injects no
    /// keys of its own.
    #[default]
    Default,
    /// Static AWS credentials (or an S3-compatible endpoint such as MinIO).
    Aws(AwsCredentials),
    /// Azure Blob / ADLS Gen2 credentials.
    Azure(AzureCredentials),
    /// Google Cloud Storage credentials.
    Gcp(GcpCredentials),
}

impl std::fmt::Debug for DeltaCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        faucet_core::util::fmt_redacted(
            f,
            "DeltaCredentials",
            self,
            &[
                "secret_access_key",
                "session_token",
                "access_key",
                "sas_token",
                "service_account_key",
            ],
        )
    }
}

impl DeltaCredentials {
    /// Merge the credential-derived keys into `options`, **without** overwriting
    /// any key the user set explicitly. This makes explicit `storage_options`
    /// authoritative — an escape hatch for keys this enum does not model.
    pub fn apply(&self, options: &mut HashMap<String, String>) {
        let derived = self.storage_options();
        for (k, v) in derived {
            options.entry(k).or_insert(v);
        }
    }

    /// The `storage_options` entries implied by these credentials.
    pub fn storage_options(&self) -> HashMap<String, String> {
        let mut m = HashMap::new();
        match self {
            DeltaCredentials::Default => {}
            DeltaCredentials::Aws(a) => {
                if let Some(v) = &a.access_key_id {
                    m.insert("AWS_ACCESS_KEY_ID".into(), v.clone());
                }
                if let Some(v) = &a.secret_access_key {
                    m.insert("AWS_SECRET_ACCESS_KEY".into(), v.clone());
                }
                if let Some(v) = &a.session_token {
                    m.insert("AWS_SESSION_TOKEN".into(), v.clone());
                }
                if let Some(v) = &a.region {
                    m.insert("AWS_REGION".into(), v.clone());
                }
                if let Some(v) = &a.endpoint_url {
                    m.insert("AWS_ENDPOINT_URL".into(), v.clone());
                }
                if let Some(v) = a.allow_http {
                    m.insert("AWS_ALLOW_HTTP".into(), v.to_string());
                }
            }
            DeltaCredentials::Azure(a) => {
                if let Some(v) = &a.account_name {
                    m.insert("AZURE_STORAGE_ACCOUNT_NAME".into(), v.clone());
                }
                if let Some(v) = &a.access_key {
                    m.insert("AZURE_STORAGE_ACCESS_KEY".into(), v.clone());
                }
                if let Some(v) = &a.sas_token {
                    m.insert("AZURE_STORAGE_SAS_KEY".into(), v.clone());
                }
            }
            DeltaCredentials::Gcp(g) => {
                if let Some(v) = &g.service_account_path {
                    m.insert("GOOGLE_SERVICE_ACCOUNT".into(), v.clone());
                }
                if let Some(v) = &g.service_account_key {
                    m.insert("GOOGLE_SERVICE_ACCOUNT_KEY".into(), v.clone());
                }
            }
        }
        m
    }
}

/// Static AWS credentials and optional S3-compatible endpoint overrides.
///
/// Leaving a field unset lets the object-store default chain resolve it (so
/// `region` alone with an instance profile is valid). `endpoint_url` +
/// `allow_http: true` targets MinIO / LocalStack.
#[derive(Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
pub struct AwsCredentials {
    /// `AWS_ACCESS_KEY_ID`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_key_id: Option<String>,
    /// `AWS_SECRET_ACCESS_KEY`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_access_key: Option<String>,
    /// `AWS_SESSION_TOKEN` (temporary credentials).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_token: Option<String>,
    /// `AWS_REGION`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    /// `AWS_ENDPOINT_URL` — override for S3-compatible stores (MinIO, etc.).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_url: Option<String>,
    /// `AWS_ALLOW_HTTP` — permit plaintext HTTP to the endpoint (MinIO in dev).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow_http: Option<bool>,
}

impl std::fmt::Debug for AwsCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        faucet_core::util::fmt_redacted(
            f,
            "AwsCredentials",
            self,
            &["secret_access_key", "session_token"],
        )
    }
}

/// Azure Blob / ADLS Gen2 credentials.
#[derive(Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
pub struct AzureCredentials {
    /// `AZURE_STORAGE_ACCOUNT_NAME`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_name: Option<String>,
    /// `AZURE_STORAGE_ACCESS_KEY` (shared-key auth).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_key: Option<String>,
    /// `AZURE_STORAGE_SAS_KEY` (SAS-token auth).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sas_token: Option<String>,
}

impl std::fmt::Debug for AzureCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        faucet_core::util::fmt_redacted(f, "AzureCredentials", self, &["access_key", "sas_token"])
    }
}

/// Google Cloud Storage credentials.
#[derive(Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
pub struct GcpCredentials {
    /// `GOOGLE_SERVICE_ACCOUNT` — path to a service-account JSON key file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_account_path: Option<String>,
    /// `GOOGLE_SERVICE_ACCOUNT_KEY` — inline service-account JSON key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_account_key: Option<String>,
}

impl std::fmt::Debug for GcpCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        faucet_core::util::fmt_redacted(f, "GcpCredentials", self, &["service_account_key"])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn default_injects_nothing() {
        let c = DeltaCredentials::default();
        assert!(c.storage_options().is_empty());
    }

    #[test]
    fn aws_maps_all_fields() {
        let c = DeltaCredentials::Aws(AwsCredentials {
            access_key_id: Some("AK".into()),
            secret_access_key: Some("SK".into()),
            session_token: Some("ST".into()),
            region: Some("us-east-1".into()),
            endpoint_url: Some("http://localhost:9000".into()),
            allow_http: Some(true),
        });
        let m = c.storage_options();
        assert_eq!(m["AWS_ACCESS_KEY_ID"], "AK");
        assert_eq!(m["AWS_SECRET_ACCESS_KEY"], "SK");
        assert_eq!(m["AWS_SESSION_TOKEN"], "ST");
        assert_eq!(m["AWS_REGION"], "us-east-1");
        assert_eq!(m["AWS_ENDPOINT_URL"], "http://localhost:9000");
        assert_eq!(m["AWS_ALLOW_HTTP"], "true");
    }

    #[test]
    fn azure_and_gcp_partial() {
        let az = DeltaCredentials::Azure(AzureCredentials {
            account_name: Some("acct".into()),
            access_key: None,
            sas_token: Some("sas".into()),
        });
        let m = az.storage_options();
        assert_eq!(m["AZURE_STORAGE_ACCOUNT_NAME"], "acct");
        assert_eq!(m["AZURE_STORAGE_SAS_KEY"], "sas");
        assert!(!m.contains_key("AZURE_STORAGE_ACCESS_KEY"));

        let gcp = DeltaCredentials::Gcp(GcpCredentials {
            service_account_path: Some("/keys/sa.json".into()),
            service_account_key: None,
        });
        assert_eq!(
            gcp.storage_options()["GOOGLE_SERVICE_ACCOUNT"],
            "/keys/sa.json"
        );

        // Azure shared-key and GCP inline-key branches.
        let az_key = DeltaCredentials::Azure(AzureCredentials {
            account_name: None,
            access_key: Some("shared-key".into()),
            sas_token: None,
        });
        assert_eq!(
            az_key.storage_options()["AZURE_STORAGE_ACCESS_KEY"],
            "shared-key"
        );
        let gcp_key = DeltaCredentials::Gcp(GcpCredentials {
            service_account_path: None,
            service_account_key: Some("{\"type\":\"service_account\"}".into()),
        });
        assert_eq!(
            gcp_key.storage_options()["GOOGLE_SERVICE_ACCOUNT_KEY"],
            "{\"type\":\"service_account\"}"
        );
    }

    #[test]
    fn explicit_options_win_over_credentials() {
        let c = DeltaCredentials::Aws(AwsCredentials {
            region: Some("us-east-1".into()),
            ..Default::default()
        });
        let mut opts = HashMap::new();
        opts.insert("AWS_REGION".to_string(), "eu-west-1".to_string());
        c.apply(&mut opts);
        // Explicit value is preserved.
        assert_eq!(opts["AWS_REGION"], "eu-west-1");
    }

    #[test]
    fn tagged_shape_round_trips() {
        let v = json!({ "type": "aws", "config": { "region": "us-east-1" } });
        let c: DeltaCredentials = serde_json::from_value(v).unwrap();
        assert_eq!(
            c,
            DeltaCredentials::Aws(AwsCredentials {
                region: Some("us-east-1".into()),
                ..Default::default()
            })
        );
        // Default variant is just `{ "type": "default" }`.
        let d: DeltaCredentials = serde_json::from_value(json!({ "type": "default" })).unwrap();
        assert_eq!(d, DeltaCredentials::Default);
    }
}

#[cfg(test)]
mod debug_redaction_tests {
    use super::*;

    #[test]
    fn debug_never_prints_secrets() {
        let v: DeltaCredentials = serde_json::from_str(r#"{"type":"aws","config":{"access_key_id":"AK","secret_access_key":"S3CRET-1","session_token":"S3CRET-2"}}"#).unwrap();
        let dbg = format!("{v:?}");
        assert!(!dbg.contains("S3CRET") && dbg.contains("***"), "{dbg}");
        let v: DeltaCredentials = serde_json::from_str(
            r#"{"type":"azure","config":{"account_name":"a","access_key":"S3CRET-3"}}"#,
        )
        .unwrap();
        let dbg = format!("{v:?}");
        assert!(!dbg.contains("S3CRET") && dbg.contains("***"), "{dbg}");
        let v: DeltaCredentials =
            serde_json::from_str(r#"{"type":"gcp","config":{"service_account_key":"S3CRET-4"}}"#)
                .unwrap();
        let dbg = format!("{v:?}");
        assert!(!dbg.contains("S3CRET") && dbg.contains("***"), "{dbg}");
    }
}
