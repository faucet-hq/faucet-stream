//! Databricks SQL warehouse sink configuration.

use faucet_core::staging::StagingCleanup;
use faucet_core::{AuthSpec, DEFAULT_BATCH_SIZE, FaucetError, WriteMode, WriteSpec};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub use faucet_common_databricks::DatabricksAuth;

/// How a page is loaded into the table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DatabricksLoadMethod {
    /// Stage the page when `staging` is configured and the page is at least
    /// `copy_threshold_bytes`; otherwise a multi-row `INSERT` (the default).
    #[default]
    Auto,
    /// Always a multi-row `INSERT … SELECT … FROM VALUES` through the
    /// Statement Execution API. No staging location needed.
    Insert,
    /// Always stage the page as Parquet, then `COPY INTO` (append) or read it
    /// with `read_files` (upsert / delete / exactly-once). Requires `staging`.
    CopyInto,
}

/// Where staged Parquet files are written before the warehouse loads them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DatabricksStagingConfig {
    /// Staging root. Either a Unity Catalog volume path
    /// (`/Volumes/<catalog>/<schema>/<volume>[/prefix]`, uploaded through the
    /// Files API with the sink's own credentials) or a cloud location the
    /// warehouse can read through an external location: `s3://bucket/prefix`,
    /// `gs://bucket/prefix`, or
    /// `abfss://container@account.dfs.core.windows.net/prefix`. Cloud locations
    /// need the crate's `staging` feature and ambient cloud credentials for the
    /// upload.
    pub location: String,
    /// When staged files are deleted after a load. Default `always`.
    #[serde(default)]
    pub cleanup: StagingCleanup,
    /// Extra `COPY_OPTIONS` entries appended verbatim to the append path's
    /// `COPY INTO`, e.g. `'mergeSchema' = 'false'`. The operator owns their
    /// correctness.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub copy_options: Option<String>,
}

fn default_true() -> bool {
    true
}
fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}
fn default_copy_threshold() -> usize {
    1024 * 1024
}
fn default_max_statement_bytes() -> usize {
    8 * 1024 * 1024
}
fn default_wait_timeout() -> u64 {
    50
}
fn default_poll_interval_ms() -> u64 {
    1000
}
fn default_statement_timeout() -> u64 {
    3600
}
fn default_max_retries() -> u32 {
    5
}
fn default_retry_backoff_ms() -> u64 {
    1000
}

/// Hard ceiling on one Statement Execution API request's statement text.
pub const STATEMENT_TEXT_LIMIT: usize = 16 * 1024 * 1024;

/// Configuration for the Databricks SQL warehouse sink.
///
/// Writes into a Delta table through a SQL warehouse's
/// [Statement Execution API](https://docs.databricks.com/api/workspace/statementexecution).
/// `write_mode` / `key` / `delete_marker` are flattened in from
/// [`WriteSpec`](faucet_core::WriteSpec).
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct DatabricksSinkConfig {
    /// Workspace base URL, e.g. `https://dbc-abc123.cloud.databricks.com`.
    pub workspace_url: String,
    /// Target SQL warehouse id.
    pub warehouse_id: String,
    /// Authentication (PAT / OAuth bearer), inline or via a shared `auth: { ref }`
    /// (use an `oauth2` client-credentials provider for an M2M service principal).
    pub auth: AuthSpec<DatabricksAuth>,
    /// Unity Catalog catalog. When unset, the warehouse's default catalog.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,
    /// Target schema.
    pub schema: String,
    /// Target table.
    pub table: String,
    /// Create the table from the first page's inferred schema when it does not
    /// exist. Default `true`.
    #[serde(default = "default_true")]
    pub create_table: bool,
    /// Load path. Default `auto`.
    #[serde(default)]
    pub load_method: DatabricksLoadMethod,
    /// Staging location for `copy_into` / `auto`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staging: Option<DatabricksStagingConfig>,
    /// `auto` stages a page whose estimated size is at least this many bytes.
    /// Default 1 MiB.
    #[serde(default = "default_copy_threshold")]
    pub copy_threshold_bytes: usize,
    /// Rows per `INSERT` / `MERGE` statement on the insert path (`0` = no row
    /// limit — statements are then bounded only by `max_statement_bytes`).
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Upper bound on one statement's SQL text on the insert path; a page is
    /// split into several statements above it. Default 8 MiB (the API accepts
    /// up to 16 MiB).
    #[serde(default = "default_max_statement_bytes")]
    pub max_statement_bytes: usize,
    /// Server-side wait before a statement goes async (`0` or `5`–`50` s).
    #[serde(default = "default_wait_timeout")]
    pub wait_timeout_secs: u64,
    /// Poll cadence while a statement is queued or running (a serverless
    /// warehouse may take a minute to start). Default 1000 ms.
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    /// Client deadline for one statement; the statement is cancelled when it
    /// is exceeded. `0` waits indefinitely. Default 3600 s.
    #[serde(default = "default_statement_timeout")]
    pub statement_timeout_secs: u64,
    /// Retries on `429` / `503` and on Delta concurrent-write conflicts of
    /// idempotent statements. Default 5.
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    /// Base of the exponential retry backoff. Default 1000 ms.
    #[serde(default = "default_retry_backoff_ms")]
    pub retry_backoff_ms: u64,
    /// `write_mode` (`append` / `upsert` / `delete` / `overwrite`), `key`,
    /// `delete_marker`.
    #[serde(flatten)]
    pub write: WriteSpec,
}

impl std::fmt::Debug for DatabricksSinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabricksSinkConfig")
            .field("workspace_url", &self.workspace_url)
            .field("warehouse_id", &self.warehouse_id)
            .field("catalog", &self.catalog)
            .field("schema", &self.schema)
            .field("table", &self.table)
            .field("load_method", &self.load_method)
            .field("staging", &self.staging)
            .field("write", &self.write)
            .finish_non_exhaustive()
    }
}

impl DatabricksSinkConfig {
    /// A config with every optional field at its default.
    pub fn new(
        workspace_url: impl Into<String>,
        warehouse_id: impl Into<String>,
        schema: impl Into<String>,
        table: impl Into<String>,
        auth: DatabricksAuth,
    ) -> Self {
        Self {
            workspace_url: workspace_url.into(),
            warehouse_id: warehouse_id.into(),
            auth: AuthSpec::Inline(auth),
            catalog: None,
            schema: schema.into(),
            table: table.into(),
            create_table: true,
            load_method: DatabricksLoadMethod::Auto,
            staging: None,
            copy_threshold_bytes: default_copy_threshold(),
            batch_size: default_batch_size(),
            max_statement_bytes: default_max_statement_bytes(),
            wait_timeout_secs: default_wait_timeout(),
            poll_interval_ms: default_poll_interval_ms(),
            statement_timeout_secs: default_statement_timeout(),
            max_retries: default_max_retries(),
            retry_backoff_ms: default_retry_backoff_ms(),
            write: WriteSpec::default(),
        }
    }

    /// Validate the config at load time.
    pub fn validate(&self) -> Result<(), FaucetError> {
        for (name, v) in [
            ("workspace_url", &self.workspace_url),
            ("warehouse_id", &self.warehouse_id),
            ("schema", &self.schema),
            ("table", &self.table),
        ] {
            if v.trim().is_empty() {
                return Err(FaucetError::Config(format!(
                    "databricks sink: `{name}` must not be empty"
                )));
            }
        }
        if self.catalog.as_deref().is_some_and(|c| c.trim().is_empty()) {
            return Err(FaucetError::Config(
                "databricks sink: `catalog` must not be empty when set".into(),
            ));
        }
        if self.wait_timeout_secs != 0 && !(5..=50).contains(&self.wait_timeout_secs) {
            return Err(FaucetError::Config(format!(
                "databricks sink: `wait_timeout_secs` must be 0 or between 5 and 50 (got {})",
                self.wait_timeout_secs
            )));
        }
        faucet_core::validate_batch_size(self.batch_size)?;
        if self.max_statement_bytes == 0 || self.max_statement_bytes > STATEMENT_TEXT_LIMIT {
            return Err(FaucetError::Config(format!(
                "databricks sink: `max_statement_bytes` must be between 1 and {STATEMENT_TEXT_LIMIT}"
            )));
        }
        if self.load_method == DatabricksLoadMethod::CopyInto && self.staging.is_none() {
            return Err(FaucetError::Config(
                "databricks sink: `load_method: copy_into` requires a `staging` block".into(),
            ));
        }
        if let Some(s) = &self.staging {
            crate::stage::StageLocation::parse(&s.location)?;
        }
        self.write.validate()?;
        if self.write.write_mode == WriteMode::Overwrite && self.table.ends_with(OVW_SUFFIX) {
            return Err(FaucetError::Config(format!(
                "databricks sink: table names ending in `{OVW_SUFFIX}` are reserved for overwrite staging"
            )));
        }
        Ok(())
    }
}

/// Overwrite staging-table suffix.
pub(crate) const OVW_SUFFIX: &str = faucet_core::idempotency::OVERWRITE_STAGING_SUFFIX;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn base() -> DatabricksSinkConfig {
        DatabricksSinkConfig::new(
            "https://x.cloud.databricks.com",
            "wh",
            "sales",
            "orders",
            DatabricksAuth::Pat { token: "t".into() },
        )
    }

    #[test]
    fn defaults_validate() {
        let c = base();
        c.validate().unwrap();
        assert!(c.create_table);
        assert_eq!(c.load_method, DatabricksLoadMethod::Auto);
        assert_eq!(c.batch_size, DEFAULT_BATCH_SIZE);
        let d = format!("{c:?}");
        assert!(d.contains("orders") && !d.contains("\"t\""));
    }

    #[test]
    fn deserializes_full_shape_with_flattened_write_spec() {
        let c: DatabricksSinkConfig = serde_json::from_value(json!({
            "workspace_url": "https://x",
            "warehouse_id": "wh",
            "auth": {"type": "pat", "config": {"token": "t"}},
            "catalog": "main",
            "schema": "s",
            "table": "t",
            "load_method": "copy_into",
            "staging": {"location": "/Volumes/main/s/stage/faucet", "cleanup": "on_success"},
            "write_mode": "upsert",
            "key": ["id"]
        }))
        .unwrap();
        c.validate().unwrap();
        assert_eq!(c.write.write_mode, WriteMode::Upsert);
        assert_eq!(c.staging.unwrap().cleanup, StagingCleanup::OnSuccess);
    }

    #[test]
    fn rejects_invalid_configs() {
        type Mutate = Box<dyn Fn(&mut DatabricksSinkConfig)>;
        let cases: Vec<Mutate> = vec![
            Box::new(|c| c.workspace_url = " ".into()),
            Box::new(|c| c.warehouse_id = "".into()),
            Box::new(|c| c.schema = "".into()),
            Box::new(|c| c.table = "".into()),
            Box::new(|c| c.catalog = Some(" ".into())),
            Box::new(|c| c.wait_timeout_secs = 3),
            Box::new(|c| c.batch_size = faucet_core::MAX_BATCH_SIZE + 1),
            Box::new(|c| c.max_statement_bytes = 0),
            Box::new(|c| c.max_statement_bytes = STATEMENT_TEXT_LIMIT + 1),
            Box::new(|c| c.load_method = DatabricksLoadMethod::CopyInto),
            Box::new(|c| {
                c.staging = Some(DatabricksStagingConfig {
                    location: "ftp://nope".into(),
                    cleanup: StagingCleanup::Always,
                    copy_options: None,
                })
            }),
            Box::new(|c| c.write.write_mode = WriteMode::Upsert),
            Box::new(|c| {
                c.write.write_mode = WriteMode::Overwrite;
                c.table = "t__faucet_ovw".into();
            }),
        ];
        for (i, mutate) in cases.iter().enumerate() {
            let mut c = base();
            mutate(&mut c);
            assert!(c.validate().is_err(), "case {i} should be rejected");
        }
        let mut ok = base();
        ok.wait_timeout_secs = 0;
        ok.validate().unwrap();
    }
}
