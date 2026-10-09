//! PostgreSQL source configuration.

use faucet_core::{DEFAULT_BATCH_SIZE, FaucetError};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Configuration for the PostgreSQL query source.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PostgresSourceConfig {
    /// PostgreSQL connection URL (e.g. `postgres://user:pass@host/db`).
    pub connection_url: String,
    /// SQL query to execute.
    pub query: String,
    /// Bind parameters for the query. Defaults to empty.
    #[serde(default)]
    pub params: Vec<Value>,
    /// Maximum number of connections in the pool. Defaults to 10.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    /// Records per emitted [`StreamPage`](faucet_core::StreamPage). Rows are
    /// drained from the sqlx cursor and yielded whenever the buffer reaches
    /// this size. Defaults to [`DEFAULT_BATCH_SIZE`].
    ///
    /// `batch_size = 0` is the "no batching" sentinel: the cursor is fully
    /// drained and the entire result set is emitted in a single page. Useful
    /// for small lookup tables or for sinks (e.g. SQL `COPY`, BigQuery load
    /// jobs) that prefer one large request to many small ones.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Optional primary-key range sharding for clustered (Mode B) execution.
    ///
    /// When set, the source advertises itself as shardable: the cluster
    /// coordinator splits the query's `key` range into contiguous slices that
    /// different workers process concurrently. Has **no effect** outside the
    /// cluster coordinator (a plain `faucet run` streams the whole query), so
    /// it is fully backward compatible. See [`ShardConfig`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard: Option<ShardConfig>,
    /// Longest the source waits on the server for the next row (or for a
    /// whole result in `fetch_all`), in seconds, before the read fails.
    /// Defaults to 3600; `0` waits forever.
    ///
    /// A peer that disappears without closing the connection (a failover, an
    /// idle eviction by a NAT or load balancer) otherwise leaves the read
    /// waiting forever, and a scheduled or served run stuck in "running".
    #[serde(default = "default_read_timeout_secs")]
    pub read_timeout_secs: u64,
    /// What to do with a number inside a JSON column that a 64-bit float
    /// cannot represent exactly (more than about 17 significant digits, or
    /// beyond the `u64` / `i64` range): `fail` (default) fails the read with
    /// an error naming the column, `string` emits the number as a JSON string
    /// holding its exact digits (with one warning per column).
    #[serde(default)]
    pub json_big_numbers: faucet_core::JsonBigNumbers,
    /// Replication mode. Defaults to [`PostgresReplication::Full`].
    #[serde(default)]
    pub replication: PostgresReplication,
    /// Explicit state-store key for the incremental bookmark. When unset, a
    /// key is derived from the connection host and a fingerprint of the
    /// database URL and query.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_key: Option<String>,
}

/// The token a query uses to read the incremental bookmark
/// (`WHERE updated_at >= ${bookmark}`).
pub const BOOKMARK_TOKEN: &str = "${bookmark}";

/// How the source replicates rows across runs.
///
/// Serializes as `{ type: full }` or
/// `{ type: incremental, column: "...", initial_value: ... }`.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, Default, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum PostgresReplication {
    /// Every run fetches the full result set (default).
    #[default]
    Full,
    /// Only rows not yet read, in `column` order, resuming from the stored
    /// bookmark (or `initial_value` on the first run).
    ///
    /// The source wraps the query as
    /// `SELECT * FROM (<query>) WHERE column >= <bookmark> ORDER BY column`
    /// and drops the rows it already emitted at the bookmark value, so rows
    /// that share a cursor value are never skipped. Every page carries a
    /// bookmark, so a crash replays at most one page. Write `${bookmark}` in
    /// the query (with `>=`) to apply the cursor further inside it, e.g. in a
    /// CTE. Rows whose `column` is NULL are never read.
    Incremental {
        /// Output column holding the replication cursor (e.g. `updated_at`).
        column: String,
        /// Lower bound (inclusive) used on the first run, before any bookmark
        /// is stored.
        initial_value: Value,
    },
}

/// Primary-key range sharding settings for the PostgreSQL source.
///
/// The source is split by contiguous ranges of an **integer-typed** column:
/// each shard runs `SELECT * FROM (<query>) WHERE <key> >= lo AND <key> < hi`.
/// The column must be present in the query's output and orderable as a 64-bit
/// integer (e.g. a `bigint`/`int`/`serial` primary key).
///
/// **Nullable keys:** if the key column contains NULLs, those rows are not
/// visible to the `MIN`/`MAX` range enumeration. They are still read — exactly
/// one shard (the last) additionally matches `<key> IS NULL`, so NULL-key rows
/// are covered by precisely one shard with no loss and no duplication.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ShardConfig {
    /// Integer column to range-partition on. Quoted as an identifier before use,
    /// so it is safe against injection but must name a real output column.
    pub key: String,
}

fn default_read_timeout_secs() -> u64 {
    3600
}

fn default_max_connections() -> u32 {
    10
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

impl std::fmt::Debug for PostgresSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresSourceConfig")
            .field("connection_url", &"***")
            .field("query", &self.query)
            .field("params", &self.params)
            .field("max_connections", &self.max_connections)
            .field("batch_size", &self.batch_size)
            .field("replication", &self.replication)
            .field("state_key", &self.state_key)
            .finish()
    }
}

impl PostgresSourceConfig {
    /// Create a new config with the required connection URL and query.
    pub fn new(connection_url: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            connection_url: connection_url.into(),
            query: query.into(),
            params: Vec::new(),
            max_connections: 10,
            batch_size: DEFAULT_BATCH_SIZE,
            shard: None,
            read_timeout_secs: default_read_timeout_secs(),
            json_big_numbers: faucet_core::JsonBigNumbers::Fail,
            replication: PostgresReplication::Full,
            state_key: None,
        }
    }

    /// Read incrementally on `column`, starting at `initial_value`.
    pub fn incremental(mut self, column: impl Into<String>, initial_value: Value) -> Self {
        self.replication = PostgresReplication::Incremental {
            column: column.into(),
            initial_value,
        };
        self
    }

    /// Validate the batch size and the replication settings.
    pub fn validate(&self) -> Result<(), FaucetError> {
        faucet_core::validate_batch_size(self.batch_size)?;
        if let PostgresReplication::Incremental {
            column,
            initial_value,
        } = &self.replication
        {
            validate_incremental(&self.query, column, initial_value)?;
        }
        Ok(())
    }

    /// Set bind parameters for the query.
    pub fn params(mut self, params: Vec<Value>) -> Self {
        self.params = params;
        self
    }

    /// Set the maximum number of connections in the pool.
    pub fn with_max_connections(mut self, max_connections: u32) -> Self {
        self.max_connections = max_connections;
        self
    }

    /// Set the per-page row count for [`Source::stream_pages`](faucet_core::Source::stream_pages).
    ///
    /// Pass `0` to opt out of batching — the entire result set is emitted in
    /// a single [`StreamPage`](faucet_core::StreamPage).
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

fn validate_incremental(
    query: &str,
    column: &str,
    initial_value: &Value,
) -> Result<(), FaucetError> {
    if column.trim().is_empty() {
        return Err(FaucetError::Config(
            "postgres: incremental replication requires a non-empty `column`".into(),
        ));
    }
    if initial_value.is_null() {
        return Err(FaucetError::Config(
            "postgres: incremental replication requires a non-null `initial_value`".into(),
        ));
    }
    if faucet_core::replication::cursor::strict_comparison(query, BOOKMARK_TOKEN) {
        return Err(FaucetError::Config(format!(
            "postgres: compare `{BOOKMARK_TOKEN}` with `>=`, not `>`: the source re-reads the \
             bookmark value and drops the rows it already emitted, and a strict comparison \
             skips rows that share it"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn default_config() {
        let config = PostgresSourceConfig::new("postgres://localhost/test", "SELECT * FROM events");
        assert_eq!(config.query, "SELECT * FROM events");
        assert!(config.params.is_empty());
    }

    #[test]
    fn builder_with_params() {
        let config = PostgresSourceConfig::new(
            "postgres://localhost/test",
            "SELECT * FROM events WHERE id = $1",
        )
        .params(vec![json!(42)]);
        assert_eq!(config.params.len(), 1);
        assert_eq!(config.params[0], json!(42));
    }

    #[test]
    fn debug_masks_connection_url() {
        let config = PostgresSourceConfig::new("postgres://secret:pass@host/db", "SELECT 1");
        let debug = format!("{config:?}");
        assert!(debug.contains("***"));
        assert!(!debug.contains("secret"));
        assert!(!debug.contains("pass"));
    }

    #[test]
    fn batch_size_defaults_to_default_batch_size() {
        let config = PostgresSourceConfig::new("postgres://localhost/test", "SELECT 1");
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let config =
            PostgresSourceConfig::new("postgres://localhost/test", "SELECT 1").with_batch_size(500);
        assert_eq!(config.batch_size, 500);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config =
            PostgresSourceConfig::new("postgres://localhost/test", "SELECT 1").with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let config = PostgresSourceConfig::new("postgres://localhost/test", "SELECT 1")
            .with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_err());
    }

    #[test]
    fn replication_defaults_to_full_and_parses_incremental() {
        let cfg: PostgresSourceConfig = serde_json::from_value(json!({
            "connection_url": "postgres://h/db",
            "query": "SELECT * FROM t",
        }))
        .unwrap();
        assert_eq!(cfg.replication, PostgresReplication::Full);
        assert!(cfg.validate().is_ok());

        let cfg: PostgresSourceConfig = serde_json::from_value(json!({
            "connection_url": "postgres://h/db",
            "query": "SELECT * FROM t",
            "replication": {"type": "incremental", "column": "updated_at", "initial_value": "2024-01-01T00:00:00Z"},
            "state_key": "orders",
        }))
        .unwrap();
        assert_eq!(
            cfg.replication,
            PostgresReplication::Incremental {
                column: "updated_at".into(),
                initial_value: json!("2024-01-01T00:00:00Z"),
            }
        );
        assert_eq!(cfg.state_key.as_deref(), Some("orders"));
        assert!(cfg.validate().is_ok());
        assert!(format!("{cfg:?}").contains("updated_at"));
    }

    #[test]
    fn replication_rejects_unknown_fields() {
        let err = serde_json::from_value::<PostgresReplication>(
            json!({"type": "incremental", "column": "c", "initial_value": 0, "key": ["id"]}),
        )
        .unwrap_err();
        assert!(err.to_string().contains("key"), "{err}");
    }

    #[test]
    fn validate_rejects_bad_incremental_settings() {
        let base = PostgresSourceConfig::new("postgres://h/db", "SELECT * FROM t");
        for (query, column, initial, needle) in [
            ("SELECT * FROM t", " ", json!(0), "column"),
            ("SELECT * FROM t", "c", json!(null), "initial_value"),
            ("SELECT * FROM t WHERE c > ${bookmark}", "c", json!(0), ">="),
        ] {
            let mut cfg = base.clone().incremental(column, initial);
            cfg.query = query.into();
            let err = cfg.validate().unwrap_err().to_string();
            assert!(err.contains(needle), "{err}");
        }
        let ok =
            PostgresSourceConfig::new("postgres://h/db", "SELECT * FROM t WHERE c >= ${bookmark}")
                .incremental("c", json!(0));
        assert!(ok.validate().is_ok());
        assert!(
            base.with_batch_size(faucet_core::MAX_BATCH_SIZE + 1)
                .validate()
                .is_err()
        );
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let json = r#"{
            "connection_url": "postgres://localhost/test",
            "query": "SELECT 1",
            "batch_size": 250
        }"#;
        let config: PostgresSourceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, 250);
    }
}
