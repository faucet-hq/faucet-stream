#![cfg_attr(docsrs, feature(doc_cfg))]

//! Shared GCS credential and client construction for faucet source and
//! sink connectors.

mod json_control;

use faucet_core::FaucetError;
use google_cloud_storage::client::{Storage, StorageControl};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Credential source for a GCS client.
///
/// Serializes as `{ type: <method>, config: { … } }` (adjacent tagging,
/// snake_case discriminators) — the consistent auth wire shape shared by
/// every faucet connector:
/// `{ type: service_account_json_file, config: { path: "/run/secrets/sa.json" } }`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum GcsCredentials {
    /// Path to a service-account JSON key file on disk.
    ServiceAccountJsonFile { path: String },
    /// Service-account JSON key as an inline string. Useful for
    /// environment-variable injection via `${env:GCP_SA_JSON}` in CLI
    /// configs.
    ServiceAccountJsonInline { json: String },
    /// Application Default Credentials — honours
    /// `GOOGLE_APPLICATION_CREDENTIALS`, gcloud user creds, and the
    /// GCE/GKE metadata server, in that order.
    #[default]
    ApplicationDefault,
    /// Anonymous credentials. Use this with emulators (e.g.
    /// `fake-gcs-server`) that do not validate bearer tokens — the SDK
    /// otherwise tries to fetch ADC tokens at request time and fails in
    /// environments without GCP credentials.
    Anonymous,
}

/// Build a `google_cloud_auth::credentials::Credentials` from a faucet
/// credential spec. All failures map to `FaucetError::Auth`.
pub async fn build_credentials(
    creds: &GcsCredentials,
) -> Result<google_cloud_auth::credentials::Credentials, FaucetError> {
    match creds {
        GcsCredentials::ApplicationDefault => google_cloud_auth::credentials::Builder::default()
            .build()
            .map_err(|e| FaucetError::Auth(format!("GCS auth (ADC): {e}"))),
        GcsCredentials::Anonymous => {
            Ok(google_cloud_auth::credentials::anonymous::Builder::new().build())
        }
        GcsCredentials::ServiceAccountJsonFile { path } => {
            let bytes = tokio::fs::read(path).await.map_err(|e| {
                FaucetError::Auth(format!(
                    "GCS auth: could not read service-account key from '{path}': {e}"
                ))
            })?;
            let value: serde_json::Value = serde_json::from_slice(&bytes).map_err(|e| {
                FaucetError::Auth(format!(
                    "GCS auth: service-account key at '{path}' is not valid JSON: {e}"
                ))
            })?;
            google_cloud_auth::credentials::service_account::Builder::new(value)
                .build()
                .map_err(|e| FaucetError::Auth(format!("GCS auth (service account): {e}")))
        }
        GcsCredentials::ServiceAccountJsonInline { json } => {
            let value: serde_json::Value = serde_json::from_str(json).map_err(|e| {
                FaucetError::Auth(format!(
                    "GCS auth: inline service-account key is not valid JSON: {e}"
                ))
            })?;
            google_cloud_auth::credentials::service_account::Builder::new(value)
                .build()
                .map_err(|e| FaucetError::Auth(format!("GCS auth (service account): {e}")))
        }
    }
}

/// Build a data-plane [`Storage`] client. Accepts an optional storage-host
/// override for integration tests (e.g. fake-gcs-server at
/// `http://localhost:4443`).
pub async fn build_storage(
    creds: &GcsCredentials,
    storage_host: Option<&str>,
) -> Result<Storage, FaucetError> {
    let credentials = build_credentials(creds).await?;
    let mut builder = Storage::builder().with_credentials(credentials);
    if let Some(host) = storage_host {
        builder = builder.with_endpoint(host.to_string());
    }
    builder
        .build()
        .await
        .map_err(|e| FaucetError::Auth(format!("GCS client build failed: {e}")))
}

/// Build a control-plane [`StorageControl`] client.
///
/// A plaintext (`http://`) storage host is an emulator: it gets a client whose
/// `list_objects` and `get_object` go over the JSON API, since gRPC needs
/// HTTP/2 and emulators serve that only behind TLS. Every other host uses the
/// SDK's gRPC client.
pub async fn build_storage_control(
    creds: &GcsCredentials,
    storage_host: Option<&str>,
) -> Result<StorageControl, FaucetError> {
    if let Some(host) = storage_host.filter(|h| json_control::is_plaintext_endpoint(h)) {
        return Ok(StorageControl::from_stub(json_control::JsonApiControl::new(host)));
    }
    let credentials = build_credentials(creds).await?;
    let mut builder = StorageControl::builder().with_credentials(credentials);
    if let Some(host) = storage_host {
        builder = builder.with_endpoint(host.to_string());
    }
    builder
        .build()
        .await
        .map_err(|e| FaucetError::Auth(format!("GCS control client build failed: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn credentials_serde_application_default() {
        let creds = GcsCredentials::ApplicationDefault;
        let v = serde_json::to_value(&creds).unwrap();
        assert_eq!(v, json!({"type": "application_default"}));
        let back: GcsCredentials = serde_json::from_value(v).unwrap();
        assert!(matches!(back, GcsCredentials::ApplicationDefault));
    }

    #[test]
    fn credentials_serde_service_account_json_file() {
        let creds = GcsCredentials::ServiceAccountJsonFile {
            path: "/run/secrets/sa.json".into(),
        };
        let v = serde_json::to_value(&creds).unwrap();
        assert_eq!(
            v,
            json!({"type": "service_account_json_file", "config": {"path": "/run/secrets/sa.json"}})
        );
        let back: GcsCredentials = serde_json::from_value(v).unwrap();
        assert!(
            matches!(back, GcsCredentials::ServiceAccountJsonFile { path } if path == "/run/secrets/sa.json")
        );
    }

    #[test]
    fn credentials_serde_service_account_json_inline() {
        let creds = GcsCredentials::ServiceAccountJsonInline {
            json: "{\"client_email\":\"x@y\"}".into(),
        };
        let v = serde_json::to_value(&creds).unwrap();
        assert_eq!(v["type"], "service_account_json_inline");
        assert!(
            v["config"]["json"]
                .as_str()
                .unwrap()
                .contains("client_email")
        );
        let back: GcsCredentials = serde_json::from_value(v).unwrap();
        assert!(matches!(
            back,
            GcsCredentials::ServiceAccountJsonInline { .. }
        ));
    }

    #[test]
    fn credentials_default_is_application_default() {
        let creds = GcsCredentials::default();
        assert!(matches!(creds, GcsCredentials::ApplicationDefault));
    }

    #[test]
    fn credentials_serde_anonymous() {
        let creds = GcsCredentials::Anonymous;
        let v = serde_json::to_value(&creds).unwrap();
        assert_eq!(v, json!({"type": "anonymous"}));
        let back: GcsCredentials = serde_json::from_value(v).unwrap();
        assert!(matches!(back, GcsCredentials::Anonymous));
    }

    #[tokio::test]
    async fn build_credentials_anonymous_succeeds() {
        let creds = build_credentials(&GcsCredentials::Anonymous).await.unwrap();
        // Smoke check: the call returns a real Credentials handle. We
        // can't easily assert on its internal type, but the fact that
        // it returned `Ok` (vs. needing GCP creds in the environment)
        // is the point of the variant.
        let _ = creds;
    }

    #[tokio::test]
    async fn build_credentials_rejects_missing_file() {
        let creds = GcsCredentials::ServiceAccountJsonFile {
            path: "/definitely/does/not/exist/sa.json".into(),
        };
        let err = build_credentials(&creds).await.unwrap_err();
        assert!(matches!(err, FaucetError::Auth(_)));
        let msg = err.to_string();
        assert!(
            msg.contains("could not read") || msg.contains("No such file"),
            "unexpected error: {msg}"
        );
    }

    #[tokio::test]
    async fn build_credentials_rejects_invalid_inline_json() {
        let creds = GcsCredentials::ServiceAccountJsonInline {
            json: "not-json".into(),
        };
        let err = build_credentials(&creds).await.unwrap_err();
        assert!(matches!(err, FaucetError::Auth(_)));
    }

    #[tokio::test]
    async fn build_credentials_reads_file_and_gets_past_parse() {
        // A real, readable file containing parseable JSON exercises the
        // file-read + JSON-parse branch (the missing-file test only covers the
        // read error). The JSON is a structurally-valid-but-incomplete service
        // account, so the SDK build may fail — but if it does, the error must
        // be the service-account build error, proving the read + parse lines
        // ran successfully first.
        let path = std::env::temp_dir().join(format!("faucet_gcs_sa_{}.json", std::process::id()));
        std::fs::write(
            &path,
            br#"{"type":"service_account","client_email":"x@y.iam"}"#,
        )
        .expect("write temp sa file");
        let creds = GcsCredentials::ServiceAccountJsonFile {
            path: path.to_string_lossy().into_owned(),
        };
        let result = build_credentials(&creds).await;
        let _ = std::fs::remove_file(&path);
        match result {
            Ok(_) => {} // built a (lazy) credential — read + parse path ran
            Err(FaucetError::Auth(msg)) => assert!(
                !msg.contains("could not read") && !msg.contains("not valid JSON"),
                "read + parse should have succeeded, got: {msg}"
            ),
            Err(other) => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[tokio::test]
    async fn build_credentials_inline_valid_json_gets_past_parse() {
        // Valid JSON (vs. the rejects_invalid_inline_json case) exercises the
        // inline parse-success + build branch.
        let creds = GcsCredentials::ServiceAccountJsonInline {
            json: r#"{"type":"service_account","client_email":"x@y.iam"}"#.into(),
        };
        match build_credentials(&creds).await {
            Ok(_) => {}
            Err(FaucetError::Auth(msg)) => assert!(
                !msg.contains("not valid JSON"),
                "inline JSON should have parsed, got: {msg}"
            ),
            Err(other) => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[tokio::test]
    async fn build_credentials_application_default_does_not_panic() {
        // ADC resolution either yields a lazy credential handle or an Auth
        // error depending on the environment; either is acceptable. The point
        // is that the ApplicationDefault arm executes and maps any failure to
        // FaucetError::Auth rather than panicking.
        match build_credentials(&GcsCredentials::ApplicationDefault).await {
            Ok(_) | Err(FaucetError::Auth(_)) => {}
            Err(other) => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[tokio::test]
    async fn build_storage_constructs_client_with_endpoint_override() {
        // Anonymous credentials + an endpoint override exercise the data-plane
        // client builder (including the with_endpoint branch) without needing
        // GCP credentials or a live connection. The client is constructed
        // lazily, so success is expected; any failure must still map to Auth.
        match build_storage(&GcsCredentials::Anonymous, Some("http://localhost:4443")).await {
            Ok(_) | Err(FaucetError::Auth(_)) => {}
            Err(other) => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[tokio::test]
    async fn build_storage_control_constructs_client_with_endpoint_override() {
        // Same as above for the control-plane client.
        match build_storage_control(&GcsCredentials::Anonymous, Some("http://localhost:4443")).await
        {
            Ok(_) | Err(FaucetError::Auth(_)) => {}
            Err(other) => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[tokio::test]
    async fn only_plaintext_hosts_take_the_json_listing_path() {
        let plain = build_storage_control(&GcsCredentials::Anonymous, Some("http://127.0.0.1:9"))
            .await
            .unwrap();
        let err = plain
            .list_objects()
            .set_parent("projects/_/buckets/b")
            .send()
            .await
            .unwrap_err();
        assert!(err.is_io(), "plaintext host must list over HTTP/1.1: {err}");
    }
}
