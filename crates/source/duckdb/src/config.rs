//! DuckDB source configuration.

use faucet_core::{DEFAULT_BATCH_SIZE, FaucetError, validate_batch_size};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Configuration for the DuckDB query source.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DuckdbSourceConfig {
    /// Path to the DuckDB database file, or `:memory:` for an in-memory
    /// database. A `duckdb://` / `duckdb:` scheme prefix is accepted and
    /// stripped.
    pub database: String,
    /// SQL query to execute.
    pub query: String,
    /// Open the database read-only. Defaults to `false` (read-write).
    ///
    /// Set `true` to attach to a database file another process holds open —
    /// DuckDB permits multiple read-only connections to one file but only a
    /// single read-write connection.
    #[serde(default)]
    pub read_only: bool,
    /// Records per emitted [`StreamPage`](faucet_core::StreamPage). Rows are
    /// drained from the DuckDB result and yielded whenever the buffer reaches
    /// this size. Defaults to [`DEFAULT_BATCH_SIZE`].
    ///
    /// `batch_size = 0` is the "no batching" sentinel: the entire result set is
    /// emitted in a single page. Useful for small lookup tables.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

impl DuckdbSourceConfig {
    /// Create a new config with the required database path and query.
    pub fn new(database: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            database: database.into(),
            query: query.into(),
            read_only: false,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    /// Set the per-page row count for
    /// [`Source::stream_pages`](faucet_core::Source::stream_pages).
    ///
    /// Pass `0` to opt out of batching — the entire result set is emitted in a
    /// single [`StreamPage`](faucet_core::StreamPage).
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Open the database read-only.
    pub fn read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }

    /// The filesystem path with any `duckdb://` / `duckdb:` scheme stripped.
    pub(crate) fn resolved_path(&self) -> &str {
        self.database
            .trim_start_matches("duckdb://")
            .trim_start_matches("duckdb:")
    }

    /// Validate the config at load time so a bad config fails fast with a typed
    /// [`FaucetError::Config`] instead of surfacing deep in a run: rejects an
    /// out-of-range `batch_size` (`> MAX_BATCH_SIZE`) and an empty `database` or
    /// `query`.
    pub fn validate(&self) -> Result<(), FaucetError> {
        if self.database.trim().is_empty() {
            return Err(FaucetError::Config(
                "DuckDB source requires a non-empty `database` (a file path or `:memory:`)".into(),
            ));
        }
        if self.query.trim().is_empty() {
            return Err(FaucetError::Config(
                "DuckDB source requires a non-empty `query`".into(),
            ));
        }
        validate_batch_size(self.batch_size)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = DuckdbSourceConfig::new("data.duckdb", "SELECT * FROM events");
        assert_eq!(config.database, "data.duckdb");
        assert_eq!(config.query, "SELECT * FROM events");
        assert_eq!(config.batch_size, DEFAULT_BATCH_SIZE);
        assert!(!config.read_only);
    }

    #[test]
    fn resolved_path_strips_scheme() {
        assert_eq!(
            DuckdbSourceConfig::new("duckdb:///tmp/a.duckdb", "SELECT 1").resolved_path(),
            "/tmp/a.duckdb"
        );
        assert_eq!(
            DuckdbSourceConfig::new("duckdb::memory:", "SELECT 1").resolved_path(),
            ":memory:"
        );
        assert_eq!(
            DuckdbSourceConfig::new(":memory:", "SELECT 1").resolved_path(),
            ":memory:"
        );
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let config = DuckdbSourceConfig::new(":memory:", "SELECT 1").with_batch_size(500);
        assert_eq!(config.batch_size, 500);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config = DuckdbSourceConfig::new(":memory:", "SELECT 1").with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let json = r#"{
            "database": ":memory:",
            "query": "SELECT 1",
            "batch_size": 250
        }"#;
        let config: DuckdbSourceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, 250);
        assert!(!config.read_only);
    }

    #[test]
    fn validate_accepts_valid_config() {
        assert!(
            DuckdbSourceConfig::new(":memory:", "SELECT 1")
                .validate()
                .is_ok()
        );
    }

    #[test]
    fn validate_rejects_oversized_batch_size() {
        let config = DuckdbSourceConfig::new(":memory:", "SELECT 1")
            .with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(matches!(config.validate(), Err(FaucetError::Config(_))));
    }

    #[test]
    fn validate_rejects_empty_database() {
        assert!(matches!(
            DuckdbSourceConfig::new("  ", "SELECT 1").validate(),
            Err(FaucetError::Config(_))
        ));
    }

    #[test]
    fn validate_rejects_empty_query() {
        assert!(matches!(
            DuckdbSourceConfig::new(":memory:", "").validate(),
            Err(FaucetError::Config(_))
        ));
    }
}
