//! SQLite source configuration.

use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Configuration for the SQLite query source.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SqliteSourceConfig {
    /// SQLite database URL (file path or `sqlite::memory:`).
    pub database_url: String,
    /// SQL query to execute.
    pub query: String,
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
    ///
    /// Note: sharding a SQLite source across cluster workers requires every
    /// worker to reach the same database file (e.g. a shared volume).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard: Option<ShardConfig>,
}

/// Primary-key range sharding settings for the SQLite source.
///
/// The source is split by contiguous ranges of an **integer-typed** column:
/// each shard runs `SELECT * FROM (<query>) WHERE "key" >= lo AND "key" < hi`.
/// The column must be present in the query's output and orderable as a 64-bit
/// integer (e.g. an `INTEGER PRIMARY KEY` rowid alias).
///
/// **Nullable keys:** if the key column contains NULLs, those rows are not
/// visible to the `MIN`/`MAX` range enumeration. They are still read — exactly
/// one shard (the last) additionally matches `"key" IS NULL`, so NULL-key rows
/// are covered by precisely one shard with no loss and no duplication.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ShardConfig {
    /// Integer column to range-partition on. Quoted as an identifier before use,
    /// so it is safe against injection but must name a real output column.
    pub key: String,
}

fn default_max_connections() -> u32 {
    10
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

impl SqliteSourceConfig {
    /// Create a new config with the required database URL and query.
    pub fn new(database_url: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            database_url: database_url.into(),
            query: query.into(),
            max_connections: 10,
            batch_size: DEFAULT_BATCH_SIZE,
            shard: None,
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = SqliteSourceConfig::new("sqlite:test.db", "SELECT * FROM events");
        assert_eq!(config.database_url, "sqlite:test.db");
        assert_eq!(config.query, "SELECT * FROM events");
    }

    #[test]
    fn memory_database() {
        let config = SqliteSourceConfig::new("sqlite::memory:", "SELECT 1");
        assert_eq!(config.database_url, "sqlite::memory:");
    }

    #[test]
    fn batch_size_defaults_to_default_batch_size() {
        let config = SqliteSourceConfig::new("sqlite::memory:", "SELECT 1");
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let config = SqliteSourceConfig::new("sqlite::memory:", "SELECT 1").with_batch_size(500);
        assert_eq!(config.batch_size, 500);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config = SqliteSourceConfig::new("sqlite::memory:", "SELECT 1").with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let config = SqliteSourceConfig::new("sqlite::memory:", "SELECT 1")
            .with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_err());
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let json = r#"{
            "database_url": "sqlite::memory:",
            "query": "SELECT 1",
            "batch_size": 250
        }"#;
        let config: SqliteSourceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, 250);
    }
}
