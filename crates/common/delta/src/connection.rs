//! The shared Delta connection block: `table_uri` + `credentials` +
//! `storage_options`, and the open-table / handler-registration helpers both
//! the source and sink rely on.

use std::collections::HashMap;

use faucet_core::FaucetError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::credentials::DeltaCredentials;

/// Storage options naming the Azure storage account (any case).
const AZURE_ACCOUNT_KEYS: &[&str] = &["azure_storage_account_name", "account_name"];

/// Location + credentials for a Delta table. Flattened (`#[serde(flatten)]`)
/// into both the source and sink config so the same three keys appear at the
/// connector-config top level.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct DeltaConnection {
    /// Delta table location URI: `file:///abs/path`, `s3://bucket/key`,
    /// `abfss://…`, `gs://bucket/key`. A bare local path is accepted and
    /// treated as `file://`.
    #[schemars(extend("x-faucet-resource" = "path"))]
    pub table_uri: String,

    /// Object-store credentials. Defaults to the ambient provider chain.
    #[serde(default)]
    pub credentials: DeltaCredentials,

    /// Extra `storage_options` passed verbatim to delta-rs (e.g. a MinIO
    /// endpoint override, or any object-store key the [`DeltaCredentials`]
    /// enum does not model). Keys set here **win** over credential-derived
    /// keys. Keys that name an endpoint (`*endpoint*`, `*_url`, `*_uri`,
    /// `*_host`, a proxy, an authority host) or a local file (`*_file`,
    /// `*_path`, a service-account or application-credentials file) are checked
    /// like the typed fields by a host that confines configs (`faucet serve`).
    #[serde(default)]
    #[schemars(extend("x-faucet-resource-keys" = {
        "*endpoint*": "url",
        "*_url": "url",
        "*_uri": "url",
        "*_host": "url",
        "url": "url",
        "*proxy": "url",
        "*authority_host": "url",
        "*_file": "path",
        "*_path": "path",
        "*application_credentials": "path",
        "google_service_account": "path",
        "service_account": "path"
    }))]
    pub storage_options: HashMap<String, String>,
}

impl std::fmt::Debug for DeltaConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        faucet_core::util::fmt_redacted(
            f,
            "DeltaConnection",
            self,
            &[
                "secret_access_key",
                "session_token",
                "access_key",
                "sas_token",
                "service_account_key",
                "storage_options",
            ],
        )
    }
}

/// The fields of a Delta source or sink config (the connection block is
/// flattened at its top level) that make it use the host's identity
/// ([`DeltaConnection::host_identity`]).
pub fn host_identity(config: &faucet_core::Value) -> Vec<String> {
    serde_json::from_value::<DeltaConnection>(config.clone())
        .ok()
        .and_then(|c| c.host_identity())
        .into_iter()
        .collect()
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

    /// Check the connection before any request: an Azure storage account
    /// name (`credentials.account_name`, or an `azure_storage_account_name` /
    /// `account_name` storage option in any case) must be 3 to 24 lowercase
    /// letters and digits, the service's own rule.
    pub fn validate(&self) -> Result<(), FaucetError> {
        for (key, value) in self.merged_storage_options() {
            let key = key.to_ascii_lowercase();
            if AZURE_ACCOUNT_KEYS.contains(&key.as_str())
                && !faucet_core::resolve::is_valid_azure_account_name(&value)
            {
                return Err(FaucetError::Config(format!(
                    "delta: `{key}` must be an Azure storage account name: 3 to 24 lowercase \
                     letters and digits"
                )));
            }
        }
        Ok(())
    }

    /// Resolve `table_uri` to a `Url`, turning bare local paths into `file://`
    /// (delta-rs's `open_table*` functions take a `Url`, not a string).
    fn table_url(&self) -> Result<url::Url, FaucetError> {
        self.validate()?;
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

    /// The field that makes this connection authenticate with the identity
    /// of the host faucet runs on, if any: `storage_options.<key>` for a key
    /// naming a role, profile, metadata service or workload identity;
    /// `credentials` when the effective options name no credential of the
    /// table's store (an S3 key id needs its secret). A task that may not use
    /// the host's identity builds its store from these options alone, so the
    /// process environment does not count. A local table uses none.
    pub fn host_identity(&self) -> Option<String> {
        use faucet_core::resolve::{StoreKind, store_options_host_identity};
        let kind = StoreKind::from_url(&self.table_uri)?;
        match store_options_host_identity(kind, self.merged_storage_options())? {
            key if key != "credentials" => Some(format!("storage_options.{key}")),
            _ => Some("credentials".into()),
        }
    }

    /// Refuse a connection that would authenticate with the host's identity
    /// where the task may not use it ([`DeltaConnection::host_identity`]).
    pub fn check_host_identity(&self) -> Result<(), FaucetError> {
        match self.host_identity() {
            Some(field) => faucet_core::resolve::require_host_credentials("delta", &field),
            None => Ok(()),
        }
    }

    /// Open the table at its latest version. Registers handlers first.
    pub async fn open(&self) -> Result<deltalake::DeltaTable, FaucetError> {
        self.check_host_identity()?;
        self.register_handlers();
        let url = self.table_url()?;
        self.load(url).await.map_err(|e| self.map_open_error(e))
    }

    /// Load the table at `url` ([`DeltaConnection::table_builder`]).
    async fn load(
        &self,
        url: url::Url,
    ) -> Result<deltalake::DeltaTable, deltalake::DeltaTableError> {
        match self.table_builder(url.clone())? {
            Some(builder) => builder.load().await,
            None => {
                deltalake::open_table_with_storage_options(url, self.merged_storage_options()).await
            }
        }
    }

    /// The builder for `url` when the task needs its own store, else `None`
    /// (delta-rs builds it from the options). Where the task may not use the
    /// host's identity the store is built from the connection's options alone,
    /// never the process environment; under a reference policy with path roots
    /// (a config the host does not trust) it refuses keys outside the table.
    fn table_builder(
        &self,
        url: url::Url,
    ) -> Result<Option<deltalake::DeltaTableBuilder>, deltalake::DeltaTableError> {
        let options = self.merged_storage_options();
        let confined =
            faucet_core::http::current_reference_policy().is_some_and(|p| p.path_roots().is_some());
        let own = !faucet_core::resolve::host_credentials_allowed();
        if !confined && !own {
            return Ok(None);
        }
        let builder = deltalake::DeltaTableBuilder::from_url(url.clone())?
            .with_storage_options(options.clone());
        let built = if own {
            crate::own_store::build(&url, &options)?
        } else {
            None
        };
        let mut root = match built {
            Some(store) => store,
            None => builder.build_storage()?.root_object_store(None),
        };
        if confined {
            let prefix = deltalake::Path::from_url_path(url.path()).map_err(|e| {
                deltalake::DeltaTableError::InvalidTableLocation(format!("{url}: {e}"))
            })?;
            root = std::sync::Arc::new(crate::confined::Confined::new(root, prefix));
        }
        Ok(Some(builder.with_storage_backend(root, url)))
    }

    /// The table's log store, built as [`DeltaConnection::open`] builds it —
    /// for delta-rs operations that create the table (`CreateBuilder`).
    pub fn log_store(&self) -> Result<deltalake::logstore::LogStoreRef, FaucetError> {
        self.check_host_identity()?;
        self.register_handlers();
        let url = self.table_url()?;
        let builder = match self.table_builder(url.clone()) {
            Ok(Some(b)) => b,
            Ok(None) => deltalake::DeltaTableBuilder::from_url(url)
                .map(|b| b.with_storage_options(self.merged_storage_options()))
                .map_err(|e| self.map_open_error(e))?,
            Err(e) => return Err(self.map_open_error(e)),
        };
        builder.build_storage().map_err(|e| self.map_open_error(e))
    }

    /// Open the table if it exists; return `Ok(None)` when the URI is not (yet)
    /// a Delta table. Other errors (auth, I/O) propagate. Used by the sink's
    /// `create_if_not_missing` path.
    pub async fn open_optional(&self) -> Result<Option<deltalake::DeltaTable>, FaucetError> {
        self.check_host_identity()?;
        self.register_handlers();
        let url = self.table_url()?;
        match self.load(url).await {
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
        if e.to_string().contains(crate::confined::OUTSIDE) {
            return FaucetError::Config(format!(
                "delta: table '{}' names a file outside its `table_uri`: {e}",
                self.redacted_uri()
            ));
        }
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

    #[test]
    fn delta_role_options_are_refused_for_callers() {
        let keyed = json!({"table_uri": "s3://b/t", "credentials": {"type": "aws", "config": {
            "access_key_id": "a", "secret_access_key": "s", "region": "us-east-1"}}});
        assert!(
            host_identity(&keyed).is_empty(),
            "static keys are the config's own"
        );
        assert_eq!(
            host_identity(&json!({"table_uri": "s3://b/t"})),
            vec!["credentials"]
        );
        let id_only = json!({"table_uri": "s3://b/t", "credentials": {"type": "aws", "config": {
            "access_key_id": "a"}}});
        assert_eq!(host_identity(&id_only), vec!["credentials"]);
        let role = json!({"table_uri": "s3://b/t", "credentials": {"type": "aws", "config": {
            "access_key_id": "a", "secret_access_key": "s"}},
            "storage_options": {"AWS_IAM_ROLE_ARN": "arn:aws:iam::1:role/r"}});
        assert_eq!(
            host_identity(&role),
            vec!["storage_options.AWS_IAM_ROLE_ARN"]
        );
        assert!(host_identity(&json!({"table_uri": "/tmp/t"})).is_empty());
        assert!(host_identity(&json!({"table_uri": "file:///tmp/t"})).is_empty());
        let az = json!({"table_uri": "abfss://c@a.dfs.core.windows.net/t", "credentials": {
            "type": "azure", "config": {"account_name": "a", "access_key": "k"}}});
        assert!(host_identity(&az).is_empty());
        assert_eq!(
            host_identity(&json!({"table_uri": "abfss://c@a.dfs.core.windows.net/t"})),
            vec!["credentials"]
        );
        let gcs = json!({"table_uri": "gs://b/t", "credentials": {"type": "gcp", "config": {
            "service_account_key": "{}"}}});
        assert!(host_identity(&gcs).is_empty());
        assert_eq!(
            host_identity(&json!({"table_uri": "gs://b/t"})),
            vec!["credentials"]
        );
    }

    #[tokio::test]
    async fn ambient_clients_are_refused_inside_a_caller_run() {
        let conn: DeltaConnection =
            serde_json::from_value(json!({"table_uri": "s3://b/t"})).unwrap();
        faucet_core::resolve::with_host_credentials(false, async {
            let err = conn.open().await.expect_err("refused").to_string();
            assert!(err.contains("delta: `credentials`"), "{err}");
            let err = conn.open_optional().await.expect_err("refused").to_string();
            assert!(err.contains("delta: `credentials`"), "{err}");
        })
        .await;
        let local: DeltaConnection =
            serde_json::from_value(json!({"table_uri": "/nonexistent/faucet/t"})).unwrap();
        assert!(local.check_host_identity().is_ok());
    }

    #[tokio::test]
    async fn a_caller_run_builds_its_log_store_from_its_own_options() {
        let dir = tempfile::tempdir().unwrap();
        let local = conn(dir.path().to_str().unwrap());
        let trusted = local.log_store().unwrap();
        let caller =
            faucet_core::resolve::with_host_credentials(false, async { local.log_store() })
                .await
                .unwrap();
        assert_eq!(caller.root_url(), trusted.root_url());
    }

    fn conn(uri: &str) -> DeltaConnection {
        DeltaConnection {
            table_uri: uri.into(),
            credentials: DeltaCredentials::default(),
            storage_options: HashMap::new(),
        }
    }

    #[test]
    fn azure_account_names_follow_the_service_rule() {
        let mut c = conn("abfss://c@acct.dfs.core.windows.net/t");
        assert!(c.validate().is_ok());
        c.storage_options
            .insert("AZURE_STORAGE_ACCOUNT_NAME".into(), "evil.example#".into());
        let e = c.location_string().unwrap_err().to_string();
        assert!(
            e.contains("azure_storage_account_name") && !e.contains("evil"),
            "{e}"
        );
        c.storage_options.clear();
        c.storage_options
            .insert("account_name".into(), "acct".into());
        assert!(c.validate().is_ok());
        c.credentials = serde_json::from_value(
            json!({"type": "azure", "config": {"account_name": "Bad.Name"}}),
        )
        .unwrap();
        c.storage_options.clear();
        assert!(c.validate().is_err());
    }

    #[test]
    fn endpoint_and_key_file_storage_options_are_resources() {
        use faucet_core::resolve::{ReferencePolicy, ResourceKind, annotated_resources};
        let schema = serde_json::to_value(schemars::schema_for!(DeltaConnection)).unwrap();
        let config = json!({
            "table_uri": "s3://bucket/tbl",
            "storage_options": {
                "AWS_ENDPOINT_URL": "http://169.254.169.254",
                "AWS_ALLOW_HTTP": "true",
                "GOOGLE_SERVICE_ACCOUNT": "/var/run/secrets/other.json",
                "azure_storage_endpoint": "https://evil.example",
                "AZURE_FEDERATED_TOKEN_FILE": "/etc/token"
            }
        });
        let mut found: Vec<(String, ResourceKind)> = annotated_resources(&schema, &config)
            .into_iter()
            .map(|r| (r.pointer, r.kind))
            .collect();
        found.sort();
        assert_eq!(
            found,
            vec![
                (
                    "/storage_options/AWS_ENDPOINT_URL".into(),
                    ResourceKind::Url
                ),
                (
                    "/storage_options/AZURE_FEDERATED_TOKEN_FILE".into(),
                    ResourceKind::Path
                ),
                (
                    "/storage_options/GOOGLE_SERVICE_ACCOUNT".into(),
                    ResourceKind::Path
                ),
                (
                    "/storage_options/azure_storage_endpoint".into(),
                    ResourceKind::Url
                ),
                ("/table_uri".into(), ResourceKind::Path),
            ]
        );
        let policy = ReferencePolicy::open().with_url_hosts(["bucket"]);
        let refused = annotated_resources(&schema, &config)
            .iter()
            .filter(|r| policy.check(r).is_err())
            .count();
        assert_eq!(refused, 2);
    }

    #[test]
    fn credential_uri_and_host_storage_options_are_resources() {
        use faucet_core::resolve::{ReferencePolicy, ResourceKind, annotated_resources};
        let schema = serde_json::to_value(schemars::schema_for!(DeltaConnection)).unwrap();
        let config = json!({
            "table_uri": "s3://bucket/tbl",
            "storage_options": {
                "AWS_CONTAINER_CREDENTIALS_FULL_URI": "http://10.0.0.5:8500/v1/kv/x",
                "aws_container_credentials_relative_uri": "/v2/credentials",
                "azure_fabric_workload_host": "10.0.0.6",
                "AWS_REGION": "us-east-1"
            }
        });
        let mut found: Vec<(String, ResourceKind)> = annotated_resources(&schema, &config)
            .into_iter()
            .filter(|r| r.pointer != "/table_uri")
            .map(|r| (r.pointer, r.kind))
            .collect();
        found.sort();
        assert_eq!(
            found,
            vec![
                (
                    "/storage_options/AWS_CONTAINER_CREDENTIALS_FULL_URI".into(),
                    ResourceKind::Url
                ),
                (
                    "/storage_options/aws_container_credentials_relative_uri".into(),
                    ResourceKind::Url
                ),
                (
                    "/storage_options/azure_fabric_workload_host".into(),
                    ResourceKind::Url
                ),
            ]
        );
        let policy = ReferencePolicy::open().with_url_hosts(["bucket"]);
        let refused: Vec<String> = annotated_resources(&schema, &config)
            .into_iter()
            .filter(|r| policy.check(r).is_err())
            .map(|r| r.pointer)
            .collect();
        assert!(
            refused.contains(&"/storage_options/AWS_CONTAINER_CREDENTIALS_FULL_URI".to_string())
                && refused.contains(&"/storage_options/azure_fabric_workload_host".to_string()),
            "{refused:?}"
        );
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

#[cfg(test)]
mod confined_tests {
    use super::*;
    use deltalake::logstore::object_store::ObjectStoreExt as _;
    use faucet_core::resolve::ReferencePolicy;
    use std::sync::Arc;

    #[tokio::test(flavor = "multi_thread")]
    async fn delta_log_paths_outside_the_table_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(dir.path()).unwrap();
        let table = root.join("t");
        std::fs::create_dir(&table).unwrap();
        std::fs::create_dir(root.join("other")).unwrap();
        std::fs::write(root.join("other/x.parquet"), b"PAR1").unwrap();
        let conn: DeltaConnection = serde_json::from_value(serde_json::json!({
            "table_uri": table.display().to_string()
        }))
        .unwrap();
        deltalake::operations::create::CreateBuilder::new()
            .with_location(conn.location_string().unwrap())
            .with_columns([deltalake::kernel::StructField::new(
                "id",
                deltalake::kernel::DataType::LONG,
                true,
            )])
            .await
            .unwrap();

        let trusted = conn.open().await.unwrap();
        let store = trusted.log_store().root_object_store(None);
        assert!(!store.to_string().contains("Confined"));

        let policy = Arc::new(ReferencePolicy::deny_all().with_path_roots(vec![root.clone()]));
        let caller = faucet_core::http::with_reference_policy(Arc::clone(&policy), conn.open())
            .await
            .unwrap();
        let store = caller.log_store().root_object_store(None);
        assert!(store.to_string().contains("Confined"), "{store}");
        let outside = deltalake::Path::from_filesystem_path(root.join("other/x.parquet")).unwrap();
        let e = store.get(&outside).await.unwrap_err().to_string();
        assert!(e.contains(crate::confined::OUTSIDE), "{e}");
        let log = deltalake::Path::from_filesystem_path(
            table.join("_delta_log/00000000000000000000.json"),
        )
        .unwrap();
        assert!(store.get(&log).await.is_ok());
        let missing = faucet_core::http::with_reference_policy(policy, async {
            let conn: DeltaConnection = serde_json::from_value(serde_json::json!({
                "table_uri": root.join("nope").display().to_string()
            }))
            .unwrap();
            conn.open_optional().await
        })
        .await
        .unwrap();
        assert!(missing.is_none());

        let refused = conn.map_open_error(deltalake::DeltaTableError::Generic(format!(
            "'x' {}",
            crate::confined::OUTSIDE
        )));
        assert!(matches!(refused, FaucetError::Config(_)), "{refused}");
    }
}

#[cfg(test)]
mod missing_table_tests {
    use super::is_missing_table;

    #[test]
    fn only_a_typed_object_store_not_found_means_no_table_here() {
        // A genuinely absent table: object-store NotFound.
        let absent = deltalake::DeltaTableError::ObjectStore {
            source: deltalake::ObjectStoreError::NotFound {
                path: "s3://b/t/_delta_log".into(),
                source: "no such key".into(),
            },
        };
        assert!(is_missing_table(&absent));

        // The regression this pins (#654 C2): a permission failure whose
        // rendered text contains "not found"-shaped wording must NOT be read
        // as "no table here" — doing so made the sink write a fresh
        // `_delta_log/…0.json` over an EXISTING table and orphan its data
        // files. Only the typed variant counts now.
        let denied = deltalake::DeltaTableError::ObjectStore {
            source: deltalake::ObjectStoreError::Generic {
                store: "S3",
                source: "403 AccessDenied: The specified key does not exist".into(),
            },
        };
        assert!(
            !is_missing_table(&denied),
            "a 403 must never be mistaken for an absent table"
        );

        // An explicit NotATable is still the other legitimate signal.
        assert!(is_missing_table(&deltalake::DeltaTableError::NotATable(
            "no log".into()
        )));
    }
}

#[cfg(test)]
mod debug_redaction_tests {
    use super::*;

    #[test]
    fn debug_never_prints_secrets() {
        let v: DeltaConnection = serde_json::from_str(r#"{"table_uri":"s3://b/t","credentials":{"type":"aws","config":{"secret_access_key":"S3CRET-1"}},"storage_options":{"AWS_SECRET_ACCESS_KEY":"S3CRET-2"}}"#).unwrap();
        let dbg = format!("{v:?}");
        assert!(!dbg.contains("S3CRET") && dbg.contains("***"), "{dbg}");
    }
}
