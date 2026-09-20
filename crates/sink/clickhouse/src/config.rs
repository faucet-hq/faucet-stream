//! Configuration for the ClickHouse sink.

use faucet_common_clickhouse::ClickHouseConnection;
use faucet_core::{DEFAULT_BATCH_SIZE, FaucetError, validate_batch_size};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

fn default_wait_for_async_insert() -> bool {
    true
}

/// Configuration for [`ClickHouseSink`](crate::ClickHouseSink).
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct ClickHouseSinkConfig {
    /// Connection settings (`url` or `host`, `database`, credentials).
    #[serde(flatten)]
    pub connection: ClickHouseConnection,
    /// Target table. May be schema-qualified (`db.table`); each identifier
    /// segment is quoted before use.
    pub table: String,
    /// Records per `INSERT` request. When the upstream `StreamPage` carries more
    /// records than `batch_size`, the sink splits it into `batch_size`-row
    /// chunks and issues one `INSERT … FORMAT JSONEachRow` per chunk. `0`
    /// forwards the whole page as a single request. Defaults to
    /// [`DEFAULT_BATCH_SIZE`].
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Create the target table if it does not exist, inferring the columns
    /// from the first written page (#580). Enabled by default: a first-ever
    /// sync cannot assume the destination already exists. The table is created
    /// with the `MergeTree` engine and `ORDER BY tuple()` — the neutral choice
    /// when faucet has no key to sort by; define the table yourself and set
    /// `create_table: false` when the sort key matters, which it does for any
    /// table you intend to query at scale.
    ///
    /// Every inferred column is `Nullable(…)`: a column present in page 1 is
    /// not required forever, and ClickHouse rejects a null into a non-nullable
    /// column, so inferring non-nullability would fail page 2.
    #[serde(default = "default_create_table")]
    pub create_table: bool,
    /// Enable ClickHouse [asynchronous inserts](https://clickhouse.com/docs/en/optimize/asynchronous-inserts)
    /// (`async_insert=1`): the server buffers rows and flushes them in the
    /// background, which greatly improves throughput for many small inserts.
    /// Defaults to `false`.
    #[serde(default)]
    pub async_insert: bool,
    /// When [`async_insert`](Self::async_insert) is enabled, whether the server
    /// waits for the buffered rows to be flushed before acknowledging the
    /// request (`wait_for_async_insert=1`). Keeping this `true` (the default)
    /// preserves faucet's at-least-once durability contract — the bookmark only
    /// advances after ClickHouse has durably accepted the batch. Ignored when
    /// `async_insert` is `false`.
    #[serde(default = "default_wait_for_async_insert")]
    pub wait_for_async_insert: bool,

    /// Staged bulk load (#528): stage each page to S3/GCS and have the
    /// ClickHouse server pull it with `s3()` / `gcs()` instead of an
    /// `INSERT … FORMAT JSONEachRow` body. Absent ⇒ the ordinary insert path.
    /// Requires the crate's `staging` feature at build time to execute.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staging: Option<ClickHouseStagingConfig>,
}

/// Staged-load configuration for the ClickHouse sink (#528).
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ClickHouseStagingConfig {
    /// Shared object-store staging block (`location`, `format`, `compression`,
    /// `cleanup`). `location` must be `s3://…` or `gs://…`.
    #[serde(flatten)]
    pub spec: faucet_core::staging::StagingSpec,
    /// AWS region for the derived `s3()` URL (virtual-hosted). Ignored for `gs://`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    /// Explicit S3-compatible endpoint (`host[:port]`, path-style) — for MinIO /
    /// non-AWS stores. Overrides `region`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// Access key the ClickHouse server uses to **read** the staged objects
    /// (the `s3()`/`gcs()` credentials). Omit to rely on the server's own IAM /
    /// configured access.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_key: Option<String>,
    /// Secret paired with [`access_key`](Self::access_key).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_key: Option<String>,
}

impl std::fmt::Debug for ClickHouseStagingConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseStagingConfig")
            .field("spec", &self.spec)
            .field("region", &self.region)
            .field("endpoint", &self.endpoint)
            // Never print the object-store credentials.
            .field("access_key", &self.access_key.as_ref().map(|_| "***"))
            .field("secret_key", &self.secret_key.as_ref().map(|_| "***"))
            .finish()
    }
}

impl std::fmt::Debug for ClickHouseSinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("ClickHouseSinkConfig");
        ds.field("connection", &self.connection)
            .field("table", &self.table)
            .field("batch_size", &self.batch_size)
            .field("async_insert", &self.async_insert)
            .field("wait_for_async_insert", &self.wait_for_async_insert);
        ds.field("staging", &self.staging);
        ds.finish()
    }
}

fn default_create_table() -> bool {
    true
}

impl ClickHouseSinkConfig {
    /// Build a config from a base URL and table, with defaults elsewhere.
    pub fn new(url: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            connection: ClickHouseConnection::from_url(url),
            table: table.into(),
            batch_size: default_batch_size(),
            create_table: default_create_table(),
            async_insert: false,
            wait_for_async_insert: default_wait_for_async_insert(),
            staging: None,
        }
    }

    /// Opt out of auto-creating a missing target table (#580).
    pub fn with_create_table(mut self, create: bool) -> Self {
        self.create_table = create;
        self
    }

    /// Set the per-request record count.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Enable ClickHouse asynchronous inserts.
    pub fn with_async_insert(mut self, async_insert: bool) -> Self {
        self.async_insert = async_insert;
        self
    }

    /// Validate connection, table, and batch size.
    pub fn validate(&self) -> Result<(), FaucetError> {
        self.connection.validate()?;
        validate_batch_size(self.batch_size)?;
        if self.table.trim().is_empty() {
            return Err(FaucetError::Config(
                "ClickHouse sink requires a non-empty `table`".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn staging_debug_redacts_credentials() {
        let staging: ClickHouseStagingConfig = serde_json::from_value(json!({
            "location": "s3://bucket/stage",
            "format": "jsonl",
            "region": "us-east-1",
            "access_key": "AKIAEXAMPLE",
            "secret_key": "super-secret-value",
        }))
        .unwrap();
        let dbg = format!("{staging:?}");
        assert!(dbg.contains("***"), "creds should be masked: {dbg}");
        assert!(!dbg.contains("AKIAEXAMPLE"), "access_key leaked: {dbg}");
        assert!(
            !dbg.contains("super-secret-value"),
            "secret_key leaked: {dbg}"
        );
        assert!(dbg.contains("us-east-1"), "non-secret fields kept: {dbg}");
    }

    #[test]
    fn config_flattens_connection_and_defaults() {
        let cfg: ClickHouseSinkConfig = serde_json::from_value(json!({
            "url": "http://localhost:8123",
            "table": "events",
        }))
        .unwrap();
        assert_eq!(cfg.table, "events");
        assert_eq!(cfg.batch_size, DEFAULT_BATCH_SIZE);
        assert!(!cfg.async_insert);
        assert!(cfg.wait_for_async_insert, "wait defaults to true");
    }

    #[test]
    fn async_insert_parses() {
        let cfg: ClickHouseSinkConfig = serde_json::from_value(json!({
            "url": "http://h:8123",
            "table": "t",
            "async_insert": true,
            "wait_for_async_insert": false,
        }))
        .unwrap();
        assert!(cfg.async_insert);
        assert!(!cfg.wait_for_async_insert);
    }

    #[test]
    fn validate_rejects_empty_table() {
        let cfg = ClickHouseSinkConfig::new("http://h:8123", "   ");
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_batch_size() {
        let cfg = ClickHouseSinkConfig::new("http://h:8123", "t")
            .with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_missing_endpoint() {
        let mut cfg = ClickHouseSinkConfig::new("http://h:8123", "t");
        cfg.connection.url = None;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn debug_masks_password() {
        let mut cfg = ClickHouseSinkConfig::new("http://h:8123", "t");
        cfg.connection.password = Some("s3cret".into());
        let dbg = format!("{cfg:?}");
        assert!(dbg.contains("***"));
        assert!(!dbg.contains("s3cret"));
    }
}
