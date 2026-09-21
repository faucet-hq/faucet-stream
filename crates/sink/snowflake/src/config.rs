//! Snowflake sink configuration.

use faucet_core::{AuthSpec, DEFAULT_BATCH_SIZE};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

// Re-export the shared auth types so end-user imports remain stable
// (`use faucet_sink_snowflake::SnowflakeAuth;` keeps working).
pub use faucet_common_snowflake::SnowflakeAuth;

/// Configuration for the Snowflake sink.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SnowflakeSinkConfig {
    /// Snowflake account identifier (e.g. `"xy12345.us-east-1"`).
    pub account: String,
    /// Warehouse to use for the session.
    pub warehouse: String,
    /// Database name.
    pub database: String,
    /// Schema name.
    pub schema: String,
    /// Target table name.
    pub table: String,
    /// Create the target table if it does not exist, inferring the column
    /// **names** from the first written page (#580). Enabled by default: a
    /// first-ever sync cannot assume the destination already exists.
    ///
    /// Columns are created as nullable `STRING`, not as inferred types,
    /// because the insert path projects every value with `::string` — a
    /// `NUMBER` column would reject its own writer's cast. Define the table
    /// yourself and set `create_table: false` when you want typed columns
    /// (then use `schema:` drift to keep them aligned).
    #[serde(default = "default_create_table")]
    pub create_table: bool,
    /// Commit-group size for the cross-page accumulator (#617).
    ///
    /// Warehouse loads are dominated by per-operation overhead, and
    /// `batch_size` can only ever *split* a page — it can never merge
    /// undersized ones, so a small source page meant one expensive warehouse
    /// operation per small page. Records now accumulate across `write_batch`
    /// calls and commit once per threshold, plus once at `flush`.
    ///
    /// `None` (the default) accumulates the **whole run** into one commit.
    /// Set it to bound how much is buffered, or to commit progressively on a
    /// long run. `0` means the same as `None`.
    ///
    /// Only the append path accumulates: `delivery: exactly_once` and the DLQ
    /// path commit per page, because a watermark must land with its own page
    /// and a DLQ must report which rows of *this* page failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_rows: Option<usize>,
    /// Estimated-bytes counterpart of [`commit_rows`](Self::commit_rows)
    /// (#617). Rows are a poor proxy for how much work a warehouse commit is;
    /// this bounds the buffered size. `None` (the default) removes the byte
    /// threshold.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_bytes: Option<usize>,
    /// Authentication: either inline (`{ type, config }`) or a `{ ref: <name> }`
    /// pointer to a shared provider in the CLI's top-level `auth:` catalog.
    /// A shared provider must yield a `Bearer` or `Token` credential, which
    /// maps onto [`SnowflakeAuth::OAuth`]; key-pair JWT is always inline.
    pub auth: AuthSpec<SnowflakeAuth>,
    /// Maximum number of records sent per Snowflake SQL REST API request.
    /// Defaults to [`DEFAULT_BATCH_SIZE`] (1000), which matches the
    /// documented sweet spot for the SQL REST API.
    ///
    /// When `write_batch` is handed a slice larger than `batch_size`, the
    /// sink re-chunks it into `batch_size` slices and issues one INSERT per
    /// chunk. `batch_size = 0` is the **"no batching" sentinel** — the
    /// records slice is forwarded as a single INSERT, no matter how large,
    /// so upstream `StreamPage` framing flows through untouched.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Maximum wall-clock time to wait for an asynchronously-executed
    /// INSERT to finish. Snowflake's SQL REST API answers an accepted but
    /// not-yet-finished statement with HTTP 202 and a `statementHandle`;
    /// the sink polls `GET /api/v2/statements/{handle}` until the statement
    /// reports success before counting the rows as written. Without this
    /// the sink would report success the moment Snowflake *accepted* the
    /// statement, losing durability. Defaults to 300 seconds. Set to `0`
    /// to poll indefinitely.
    #[serde(
        default = "default_poll_timeout",
        with = "faucet_core::config::duration_secs"
    )]
    #[schemars(with = "u64")]
    pub poll_timeout: Duration,
    /// Arrow columnar **bulk-load** mode (#381): buffer Arrow `RecordBatch`es
    /// to Parquet, upload to an external cloud stage's backing storage, then
    /// `COPY INTO <table> FROM @stage FILE_FORMAT=(TYPE=PARQUET)` over the SQL
    /// REST API. Requires a binary built with this crate's `arrow` feature; a
    /// config that sets it on an `arrow`-off build is rejected at construction.
    ///
    /// This only drives the Arrow fast path the pipeline negotiates when the
    /// source *and* sink are both columnar and no `Value`-shaped stage
    /// (transforms, DLQ, exactly-once, masking, …) is configured. The regular
    /// row path (`INSERT … PARSE_JSON`) and the exactly-once watermark MERGE
    /// are unaffected — bulk-load is append-only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bulk_load: Option<SnowflakeStageConfig>,
}

/// External-stage Parquet bulk-load configuration for the Arrow columnar path.
///
/// The named `stage` must already exist in Snowflake and point at the same
/// cloud location as `url`; the sink uploads Parquet files to `url` (via
/// `object_store`) and then references them as `@stage/<file>` in `COPY INTO`.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SnowflakeStageConfig {
    /// Named **external** stage in Snowflake — `MY_DB.MY_SCHEMA.MY_STAGE` or a
    /// schema-relative `MY_STAGE`. Must already exist and reference `url`.
    /// (Internal named stages use the `PUT` driver command, which the SQL REST
    /// API does not support, so only external stages work here.)
    pub stage: String,
    /// Object-store URL of the stage's backing location, e.g.
    /// `s3://bucket/prefix/`, `gs://bucket/prefix/`, or
    /// `azure://container/prefix/`. Uploaded Parquet files land here and are
    /// then loaded via `@stage/<file>`.
    pub url: String,
    /// Extra `object_store` config keys for the upload client (credentials,
    /// region, endpoint, …), applied verbatim — e.g. `aws_access_key_id`,
    /// `aws_secret_access_key`, `aws_region`, `google_service_account_key`.
    /// Prefer `${secret:…}` / `${env:…}` interpolation for secret values.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub storage_options: HashMap<String, String>,
    /// `MATCH_BY_COLUMN_NAME` COPY option — defaults to `CASE_INSENSITIVE` so
    /// Parquet columns map to table columns by name. Set to `NONE` for
    /// positional loading (rarely wanted for Parquet).
    #[serde(default = "default_match_by_column_name")]
    pub match_by_column_name: String,
    /// Append `PURGE = TRUE` so Snowflake removes staged files after a
    /// successful load. Default `false` (leave files for audit / debugging).
    #[serde(default)]
    pub purge: bool,
}

/// Manual `Debug` so `storage_options` values (which may carry cloud
/// credentials) are never printed — only the key names are shown.
impl std::fmt::Debug for SnowflakeStageConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnowflakeStageConfig")
            .field("stage", &self.stage)
            .field("url", &self.url)
            .field(
                "storage_options",
                &self.storage_options.keys().collect::<Vec<_>>(),
            )
            .field("match_by_column_name", &self.match_by_column_name)
            .field("purge", &self.purge)
            .finish()
    }
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

fn default_match_by_column_name() -> String {
    "CASE_INSENSITIVE".to_string()
}

fn default_poll_timeout() -> Duration {
    Duration::from_secs(300)
}

fn default_create_table() -> bool {
    true
}

impl SnowflakeSinkConfig {
    /// Create a new config with required fields and sensible defaults.
    pub fn new(
        account: impl Into<String>,
        warehouse: impl Into<String>,
        database: impl Into<String>,
        schema: impl Into<String>,
        table: impl Into<String>,
        auth: SnowflakeAuth,
    ) -> Self {
        Self {
            account: account.into(),
            warehouse: warehouse.into(),
            database: database.into(),
            schema: schema.into(),
            table: table.into(),
            create_table: default_create_table(),
            commit_rows: None,
            commit_bytes: None,
            auth: AuthSpec::Inline(auth),
            batch_size: DEFAULT_BATCH_SIZE,
            poll_timeout: default_poll_timeout(),
            bulk_load: None,
        }
    }

    /// Opt out of auto-creating a missing target table (#580).
    pub fn with_create_table(mut self, create: bool) -> Self {
        self.create_table = create;
        self
    }

    /// Enable Arrow columnar bulk-load via an external Parquet stage (#381).
    pub fn with_bulk_load(mut self, stage: SnowflakeStageConfig) -> Self {
        self.bulk_load = Some(stage);
        self
    }

    /// Set the maximum wall-clock time spent polling an asynchronously
    /// executed INSERT for completion. Pass `Duration::ZERO` to poll forever.
    pub fn with_poll_timeout(mut self, timeout: Duration) -> Self {
        self.poll_timeout = timeout;
        self
    }

    /// Set the maximum number of records per Snowflake SQL REST API request.
    ///
    /// Pass `0` to opt out of re-chunking — the entire records slice handed
    /// to `write_batch` is sent in a single INSERT request, preserving
    /// upstream `StreamPage` framing.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_auth() -> SnowflakeAuth {
        SnowflakeAuth::OAuth {
            token: "tok".into(),
        }
    }

    fn sample_config() -> SnowflakeSinkConfig {
        SnowflakeSinkConfig::new(
            "xy12345",
            "COMPUTE_WH",
            "MY_DB",
            "PUBLIC",
            "events",
            sample_auth(),
        )
    }

    #[test]
    fn default_config() {
        let config = sample_config();
        assert_eq!(config.account, "xy12345");
        assert_eq!(config.warehouse, "COMPUTE_WH");
        assert_eq!(config.database, "MY_DB");
        assert_eq!(config.schema, "PUBLIC");
        assert_eq!(config.table, "events");
        assert_eq!(config.poll_timeout, Duration::from_secs(300));
    }

    #[test]
    fn batch_size_defaults_to_default_batch_size() {
        let config = sample_config();
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let config = sample_config().with_batch_size(250);
        assert_eq!(config.batch_size, 250);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config = sample_config().with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let config = sample_config().with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_err());
    }

    #[test]
    fn with_bulk_load_sets_stage_and_defaults_deserialize() {
        let cfg = sample_config().with_bulk_load(SnowflakeStageConfig {
            stage: "STG".into(),
            url: "s3://b/p/".into(),
            storage_options: std::collections::HashMap::new(),
            match_by_column_name: default_match_by_column_name(),
            purge: false,
        });
        let stage = cfg.bulk_load.expect("bulk_load set");
        assert_eq!(stage.stage, "STG");
        assert_eq!(stage.match_by_column_name, "CASE_INSENSITIVE");
        assert!(!stage.purge);

        // JSON defaults: match_by_column_name + purge fall back sensibly.
        let json = r#"{ "stage": "S", "url": "gs://b/" }"#;
        let s: SnowflakeStageConfig = serde_json::from_str(json).unwrap();
        assert_eq!(s.match_by_column_name, "CASE_INSENSITIVE");
        assert!(!s.purge);
        assert!(s.storage_options.is_empty());
    }

    #[test]
    fn stage_debug_masks_storage_option_values() {
        let mut opts = std::collections::HashMap::new();
        opts.insert(
            "aws_secret_access_key".to_string(),
            "SUPER_SECRET".to_string(),
        );
        let stage = SnowflakeStageConfig {
            stage: "STG".into(),
            url: "s3://b/".into(),
            storage_options: opts,
            match_by_column_name: default_match_by_column_name(),
            purge: true,
        };
        let dbg = format!("{stage:?}");
        assert!(!dbg.contains("SUPER_SECRET"), "secret leaked: {dbg}");
        assert!(
            dbg.contains("aws_secret_access_key"),
            "key name shown: {dbg}"
        );
        assert!(dbg.contains("purge: true"), "{dbg}");
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let json = r#"{
            "account": "xy12345",
            "warehouse": "COMPUTE_WH",
            "database": "MY_DB",
            "schema": "PUBLIC",
            "table": "events",
            "auth": {"type": "oauth", "config": {"token": "tok"}},
            "batch_size": 250
        }"#;
        let config: SnowflakeSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, 250);
    }

    #[test]
    fn batch_size_defaults_when_absent_from_json() {
        let json = r#"{
            "account": "xy12345",
            "warehouse": "COMPUTE_WH",
            "database": "MY_DB",
            "schema": "PUBLIC",
            "table": "events",
            "auth": {"type": "oauth", "config": {"token": "tok"}}
        }"#;
        let config: SnowflakeSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }
}
