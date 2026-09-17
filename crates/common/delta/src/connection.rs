//! The shared Delta connection block: `table_uri` + `credentials` +
//! `storage_options`, and the open-table / handler-registration helpers both
//! the source and sink rely on.

use std::collections::HashMap;

use faucet_core::FaucetError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::credentials::DeltaCredentials;

/// Location + credentials for a Delta table. Flattened (`#[serde(flatten)]`)
/// into both the source and sink config so the same three keys appear at the
/// connector-config top level.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DeltaConnection {
    /// Delta table location URI: `file:///abs/path`, `s3://bucket/key`,
    /// `abfss://…`, `gs://bucket/key`. A bare local path is accepted and
    /// treated as `file://`.
    pub table_uri: String,

    /// Object-store credentials. Defaults to the ambient provider chain.
    #[serde(default)]
    pub credentials: DeltaCredentials,

    /// Extra `storage_options` passed verbatim to delta-rs (e.g. a MinIO
    /// endpoint override, or any object-store key the [`DeltaCredentials`]
    /// enum does not model). Keys set here **win** over credential-derived
    /// keys.
    #[serde(default)]
    pub storage_options: HashMap<String, String>,
}

impl DeltaConnection {
    /// Build the effective `storage_options` map: credential-derived keys with
    /// the user's explicit `storage_options` layered on top (explicit wins).
    pub fn merged_storage_options(&self) -> HashMap<String, String> {
        let mut opts = self.storage_options.clone();
        self.credentials.apply(&mut opts);
        opts
    }

    /// The table URI with any embedded credentials scrubbed — safe for logs,
    /// lineage, and `dataset_uri()`.
    pub fn redacted_uri(&self) -> String {
        faucet_core::util::redact_uri_credentials(&self.table_uri)
    }

    /// Register the object-store handlers delta-rs needs for this URI's scheme.
    ///
    /// delta-rs does not auto-register the cloud object stores; the relevant
    /// `register_handlers` must run once before opening/creating an `s3://` /
    /// `abfss://` / `gs://` table. Idempotent and cheap — safe to call before
    /// every open.
    pub fn register_handlers(&self) {
        register_all_handlers();
    }

    /// Resolve `table_uri` to a `Url`, turning bare local paths into `file://`
    /// (delta-rs's `open_table*` functions take a `Url`, not a string).
    fn table_url(&self) -> Result<url::Url, FaucetError> {
        deltalake::ensure_table_uri(&self.table_uri).map_err(|e| {
            FaucetError::Config(format!(
                "delta: invalid table_uri '{}': {e}",
                self.redacted_uri()
            ))
        })
    }

    /// The resolved table location as a string — for delta-rs builders that
    /// take a location string (e.g. `CreateBuilder::with_location`).
    pub fn location_string(&self) -> Result<String, FaucetError> {
        Ok(self.table_url()?.to_string())
    }

    /// Open the table at its latest version. Registers handlers first.
    pub async fn open(&self) -> Result<deltalake::DeltaTable, FaucetError> {
        self.register_handlers();
        let url = self.table_url()?;
        deltalake::open_table_with_storage_options(url, self.merged_storage_options())
            .await
            .map_err(|e| self.map_open_error(e))
    }

    /// Open the table if it exists; return `Ok(None)` when the URI is not (yet)
    /// a Delta table. Other errors (auth, I/O) propagate. Used by the sink's
    /// `create_if_not_missing` path.
    pub async fn open_optional(&self) -> Result<Option<deltalake::DeltaTable>, FaucetError> {
        self.register_handlers();
        let url = self.table_url()?;
        match deltalake::open_table_with_storage_options(url, self.merged_storage_options()).await {
            Ok(t) => Ok(Some(t)),
            Err(e) if is_missing_table(&e) => Ok(None),
            Err(e) => Err(self.map_open_error(e)),
        }
    }

    /// Open the table pinned to a specific version (time travel).
    pub async fn open_at_version(
        &self,
        version: u64,
    ) -> Result<deltalake::DeltaTable, FaucetError> {
        let mut table = self.open().await?;
        table
            .load_version(version)
            .await
            .map_err(|e| self.map_open_error(e))?;
        Ok(table)
    }

    /// Open the table as of a timestamp (RFC 3339). Time travel.
    pub async fn open_at_timestamp(
        &self,
        timestamp: &str,
    ) -> Result<deltalake::DeltaTable, FaucetError> {
        let ts = chrono::DateTime::parse_from_rfc3339(timestamp)
            .map_err(|e| {
                FaucetError::Config(format!(
                    "delta: invalid `timestamp` '{timestamp}' (expected RFC 3339): {e}"
                ))
            })?
            .with_timezone(&chrono::Utc);
        let mut table = self.open().await?;
        table
            .load_with_datetime(ts)
            .await
            .map_err(|e| self.map_open_error(e))?;
        Ok(table)
    }

    fn map_open_error(&self, e: deltalake::DeltaTableError) -> FaucetError {
        FaucetError::Source(format!(
            "delta: failed to open table '{}': {e}",
            self.redacted_uri()
        ))
    }
}

/// Whether a delta-rs error means "there is no Delta table here (yet)" —
/// either an explicit `NotATable` or an underlying object-store "not found".
fn is_missing_table(e: &deltalake::DeltaTableError) -> bool {
    if matches!(e, deltalake::DeltaTableError::NotATable(_)) {
        return true;
    }
    // A brand-new location surfaces as an object-store NotFound wrapped in a
    // generic error. Classify it **typed**: matching the rendered message
    // instead would also swallow a 403 AccessDenied, an expired credential, or
    // a transient listing failure on `_delta_log/` — every one of which
    // renders "not found"-shaped text for object stores. Reading an *existing*
    // table as absent makes the sink write a fresh `_delta_log/…0.json`,
    // replacing the table's identity and orphaning its data files, so this is
    // a data-loss boundary, not a diagnostic one.
    matches!(
        e,
        deltalake::DeltaTableError::ObjectStore {
            source: deltalake::ObjectStoreError::NotFound { .. }
        }
    )
}

/// Register every compiled-in cloud object-store handler exactly once.
///
/// Each backend is gated on its cargo feature; with no cloud feature this is a
/// no-op (local filesystem needs no registration).
fn register_all_handlers() {
    #[cfg(feature = "s3")]
    {
        use std::sync::Once;
        static S3: Once = Once::new();
        S3.call_once(|| deltalake::aws::register_handlers(None));
    }
    #[cfg(feature = "azure")]
    {
        use std::sync::Once;
        static AZURE: Once = Once::new();
        AZURE.call_once(|| deltalake::azure::register_handlers(None));
    }
    #[cfg(feature = "gcs")]
    {
        use std::sync::Once;
        static GCS: Once = Once::new();
        GCS.call_once(|| deltalake::gcp::register_handlers(None));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn conn(uri: &str) -> DeltaConnection {
        DeltaConnection {
            table_uri: uri.into(),
            credentials: DeltaCredentials::default(),
            storage_options: HashMap::new(),
        }
    }

    #[test]
    fn flatten_shape_round_trips() {
        let v = json!({
            "table_uri": "s3://bucket/tbl",
            "credentials": { "type": "aws", "config": { "region": "us-east-1" } },
            "storage_options": { "AWS_ALLOW_HTTP": "true" }
        });
        let c: DeltaConnection = serde_json::from_value(v).unwrap();
        assert_eq!(c.table_uri, "s3://bucket/tbl");
        let opts = c.merged_storage_options();
        assert_eq!(opts["AWS_REGION"], "us-east-1");
        assert_eq!(opts["AWS_ALLOW_HTTP"], "true");
    }

    #[test]
    fn explicit_storage_option_overrides_credential() {
        let v = json!({
            "table_uri": "s3://bucket/tbl",
            "credentials": { "type": "aws", "config": { "region": "us-east-1" } },
            "storage_options": { "AWS_REGION": "eu-west-1" }
        });
        let c: DeltaConnection = serde_json::from_value(v).unwrap();
        assert_eq!(c.merged_storage_options()["AWS_REGION"], "eu-west-1");
    }

    #[test]
    fn defaults_omit_credentials_and_options() {
        let c: DeltaConnection =
            serde_json::from_value(json!({ "table_uri": "file:///tmp/t" })).unwrap();
        assert_eq!(c.credentials, DeltaCredentials::Default);
        assert!(c.merged_storage_options().is_empty());
    }

    #[test]
    fn redacted_uri_passes_plain_uri_through() {
        assert_eq!(conn("s3://bucket/tbl").redacted_uri(), "s3://bucket/tbl");
    }

    #[test]
    fn register_handlers_is_idempotent() {
        // No cloud feature in the default test build → pure no-op, but must not
        // panic when called repeatedly.
        let c = conn("file:///tmp/t");
        c.register_handlers();
        c.register_handlers();
    }
}
