//! Snowflake source configuration.

use faucet_core::{AuthSpec, DEFAULT_BATCH_SIZE};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;

// Re-export the shared auth types so end-user imports remain stable.
pub use faucet_common_snowflake::SnowflakeAuth;

fn default_statement_timeout() -> Duration {
    Duration::from_secs(60)
}

fn default_poll_timeout() -> Duration {
    Duration::from_secs(300)
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

/// Configuration for the Snowflake query source.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SnowflakeSourceConfig {
    /// Snowflake account identifier (e.g. `"xy12345.us-east-1"`).
    pub account: String,
    /// Warehouse to use for the session.
    pub warehouse: String,
    /// Database name.
    pub database: String,
    /// Schema name.
    pub schema: String,
    /// Optional role to assume for the session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    /// Authentication: either inline (`{ type, config }`) or a `{ ref: <name> }`
    /// pointer to a shared provider in the CLI's top-level `auth:` catalog.
    /// A shared provider must yield a `Bearer` or `Token` credential, which
    /// maps onto [`SnowflakeAuth::OAuth`]; key-pair JWT is always inline.
    pub auth: AuthSpec<SnowflakeAuth>,
    /// SQL statement to execute. May contain `${field.path}` placeholders that
    /// are resolved against the parent-record context at runtime; resolved
    /// values are sent as positional bind parameters appended after
    /// [`params`](Self::params).
    pub query: String,
    /// Positional bind parameters for the query, applied in order before any
    /// context-derived values. Snowflake's SQL REST API uses 1-based positional
    /// binds in the JSON request body (see the `bindings` field in the
    /// [SQL API docs](https://docs.snowflake.com/en/developer-guide/sql-api/submitting-requests#using-bind-variables-in-a-statement)).
    #[serde(default)]
    pub params: Vec<Value>,
    /// Per-statement server-side timeout. Defaults to 60 seconds. Passed
    /// through as the `timeout` field on the `POST /api/v2/statements` request
    /// body. The HTTP-level timeout for each individual request is configured
    /// separately by the source via the underlying `reqwest` client defaults.
    #[serde(
        default = "default_statement_timeout",
        with = "faucet_core::config::duration_secs"
    )]
    #[schemars(with = "u64")]
    pub statement_timeout: Duration,
    /// Maximum wall-clock time the source will spend polling an asynchronous
    /// statement (one for which the initial `POST /api/v2/statements`
    /// returns HTTP 202) before giving up with
    /// [`FaucetError::Source`](faucet_core::FaucetError::Source).
    /// Without this cap a statement that never finishes would loop forever.
    /// Defaults to 300 seconds. Set to `0` to disable the cap and poll
    /// indefinitely.
    #[serde(
        default = "default_poll_timeout",
        with = "faucet_core::config::duration_secs"
    )]
    #[schemars(with = "u64")]
    pub poll_timeout: Duration,
    /// Records per emitted [`StreamPage`](faucet_core::StreamPage).
    ///
    /// Snowflake's SQL REST API splits large result sets into *partitions* (one
    /// chunk of rows per HTTP response). The source re-frames partitions into
    /// `batch_size`-sized pages: rows accumulate in a buffer and are yielded as
    /// soon as the buffer reaches `batch_size`. Defaults to
    /// [`DEFAULT_BATCH_SIZE`].
    ///
    /// `batch_size = 0` is the **"no batching" sentinel**: the entire result
    /// set is buffered and emitted in a single page. Useful for small lookup
    /// tables, or for sinks (e.g. SQL `COPY`, BigQuery load jobs) that prefer
    /// one large request to many small ones.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

impl std::fmt::Debug for SnowflakeSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnowflakeSourceConfig")
            .field("account", &self.account)
            .field("warehouse", &self.warehouse)
            .field("database", &self.database)
            .field("schema", &self.schema)
            .field("role", &self.role)
            .field("auth", &self.auth)
            .field("query", &self.query)
            .field("params", &self.params)
            .field("statement_timeout", &self.statement_timeout)
            .field("poll_timeout", &self.poll_timeout)
            .field("batch_size", &self.batch_size)
            .finish()
    }
}

impl SnowflakeSourceConfig {
    /// Create a new config with required fields and sensible defaults.
    pub fn new(
        account: impl Into<String>,
        warehouse: impl Into<String>,
        database: impl Into<String>,
        schema: impl Into<String>,
        auth: SnowflakeAuth,
        query: impl Into<String>,
    ) -> Self {
        Self {
            account: account.into(),
            warehouse: warehouse.into(),
            database: database.into(),
            schema: schema.into(),
            role: None,
            auth: AuthSpec::Inline(auth),
            query: query.into(),
            params: Vec::new(),
            statement_timeout: default_statement_timeout(),
            poll_timeout: default_poll_timeout(),
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    /// Set the session role.
    pub fn with_role(mut self, role: impl Into<String>) -> Self {
        self.role = Some(role.into());
        self
    }

    /// Set positional bind parameters for the query.
    pub fn with_params(mut self, params: Vec<Value>) -> Self {
        self.params = params;
        self
    }

    /// Set the per-statement server-side timeout.
    pub fn with_statement_timeout(mut self, timeout: Duration) -> Self {
        self.statement_timeout = timeout;
        self
    }

    /// Set the maximum wall-clock time spent polling an asynchronous
    /// statement before giving up. Pass `Duration::ZERO` to poll forever.
    pub fn with_poll_timeout(mut self, timeout: Duration) -> Self {
        self.poll_timeout = timeout;
        self
    }

    /// Set the records-per-page hint for [`Source::stream_pages`](faucet_core::Source::stream_pages).
    ///
    /// Pass `0` to opt out of batching — the entire result set is emitted in
    /// a single page.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample_config() -> SnowflakeSourceConfig {
        SnowflakeSourceConfig::new(
            "xy12345",
            "COMPUTE_WH",
            "MY_DB",
            "PUBLIC",
            SnowflakeAuth::OAuth { token: "t".into() },
            "SELECT * FROM events",
        )
    }

    #[test]
    fn default_config() {
        let cfg = sample_config();
        assert_eq!(cfg.account, "xy12345");
        assert_eq!(cfg.warehouse, "COMPUTE_WH");
        assert_eq!(cfg.database, "MY_DB");
        assert_eq!(cfg.schema, "PUBLIC");
        assert!(cfg.role.is_none());
        assert!(cfg.params.is_empty());
        assert_eq!(cfg.statement_timeout, Duration::from_secs(60));
        assert_eq!(cfg.poll_timeout, Duration::from_secs(300));
        assert_eq!(cfg.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn builders_compose() {
        let cfg = sample_config()
            .with_role("ANALYST")
            .with_params(vec![json!(42)])
            .with_statement_timeout(Duration::from_secs(15))
            .with_batch_size(500);
        assert_eq!(cfg.role.as_deref(), Some("ANALYST"));
        assert_eq!(cfg.params, vec![json!(42)]);
        assert_eq!(cfg.statement_timeout, Duration::from_secs(15));
        assert_eq!(cfg.batch_size, 500);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let cfg = sample_config().with_batch_size(0);
        assert_eq!(cfg.batch_size, 0);
        assert!(faucet_core::validate_batch_size(cfg.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let cfg = sample_config().with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(cfg.batch_size).is_err());
    }

    #[test]
    fn deserializes_minimal_json() {
        let json = r#"{
            "account": "xy12345",
            "warehouse": "WH",
            "database": "DB",
            "schema": "PUBLIC",
            "auth": {"type": "oauth", "config": {"token": "t"}},
            "query": "SELECT 1"
        }"#;
        let cfg: SnowflakeSourceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
        assert_eq!(cfg.statement_timeout, Duration::from_secs(60));
        assert!(cfg.role.is_none());
        assert!(cfg.params.is_empty());
    }

    #[test]
    fn deserializes_all_optional_fields() {
        let json = r#"{
            "account": "xy12345",
            "warehouse": "WH",
            "database": "DB",
            "schema": "PUBLIC",
            "role": "ANALYST",
            "auth": {"type": "oauth", "config": {"token": "t"}},
            "query": "SELECT $1",
            "params": [42],
            "statement_timeout": 15,
            "batch_size": 250
        }"#;
        let cfg: SnowflakeSourceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.role.as_deref(), Some("ANALYST"));
        assert_eq!(cfg.params, vec![json!(42)]);
        assert_eq!(cfg.statement_timeout, Duration::from_secs(15));
        assert_eq!(cfg.batch_size, 250);
    }

    #[test]
    fn debug_masks_auth_secrets() {
        let cfg = SnowflakeSourceConfig::new(
            "acct",
            "wh",
            "db",
            "schema",
            SnowflakeAuth::KeyPair {
                user: "alice".into(),
                private_key_pem: "PRIVATE-KEY-DATA".into(),
            },
            "SELECT 1",
        );
        let dbg = format!("{cfg:?}");
        assert!(dbg.contains("***"));
        assert!(!dbg.contains("PRIVATE-KEY-DATA"));
    }
}
