//! Configuration for the ClickHouse query source.

use faucet_common_clickhouse::ClickHouseConnection;
use faucet_core::{DEFAULT_BATCH_SIZE, FaucetError, validate_batch_size};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

/// How the source replicates rows across runs.
///
/// Serializes as `{ type: full }` or
/// `{ type: incremental, column: "...", initial_value: ... }`.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, Default, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClickHouseReplication {
    /// Every run fetches the full result set (default).
    #[default]
    Full,
    /// Only rows whose `column` is strictly greater than the stored bookmark
    /// (or `initial_value` on the first run) are emitted.
    ///
    /// The bookmark is applied two ways: if the query contains the literal
    /// token `@bookmark`, it is substituted as an injection-safe SQL literal so
    /// the server filters (efficient pushdown); the source *also* filters
    /// client-side as a correctness backstop. The new maximum of `column` is
    /// persisted on the final page.
    Incremental {
        /// Column whose value is the replication cursor (e.g. `updated_at`).
        column: String,
        /// Lower bound used on the first run, before any bookmark is stored.
        initial_value: Value,
    },
}

/// Configuration for [`ClickHouseSource`](crate::ClickHouseSource).
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct ClickHouseSourceConfig {
    /// Connection settings (`url` or `host`, `database`, credentials).
    #[serde(flatten)]
    pub connection: ClickHouseConnection,
    /// SQL `SELECT` query to run. The output format is set via the request
    /// settings (`JSONCompactEachRowWithNamesAndTypes`, decoded by column
    /// type), so **do not** append a `FORMAT` clause. Use
    /// the literal `@bookmark` token to push the incremental cursor down into
    /// the `WHERE` clause; use `{key}` tokens to inject parent-context values in
    /// a matrix child.
    pub query: String,
    /// Records per emitted [`StreamPage`](faucet_core::StreamPage). `0` emits
    /// the whole result set as a single page. Defaults to
    /// [`DEFAULT_BATCH_SIZE`].
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Replication mode. Defaults to [`ClickHouseReplication::Full`].
    #[serde(default)]
    pub replication: ClickHouseReplication,
    /// Explicit state-store key for the bookmark. When unset, a key is derived
    /// from the connection host and a query fingerprint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_key: Option<String>,
}

impl std::fmt::Debug for ClickHouseSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseSourceConfig")
            .field("connection", &self.connection)
            .field("query", &self.query)
            .field("batch_size", &self.batch_size)
            .field("replication", &self.replication)
            .field("state_key", &self.state_key)
            .finish()
    }
}

impl ClickHouseSourceConfig {
    /// Build a config from a base URL and query, with defaults elsewhere.
    pub fn new(url: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            connection: ClickHouseConnection::from_url(url),
            query: query.into(),
            batch_size: default_batch_size(),
            replication: ClickHouseReplication::Full,
            state_key: None,
        }
    }

    /// Set the per-page record count.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Configure incremental replication on `column`, starting at `initial`.
    pub fn incremental(mut self, column: impl Into<String>, initial: Value) -> Self {
        self.replication = ClickHouseReplication::Incremental {
            column: column.into(),
            initial_value: initial,
        };
        self
    }

    /// Validate connection, batch size, and replication settings.
    pub fn validate(&self) -> Result<(), FaucetError> {
        self.connection.validate()?;
        validate_batch_size(self.batch_size)?;
        if let ClickHouseReplication::Incremental { column, .. } = &self.replication
            && column.trim().is_empty()
        {
            return Err(FaucetError::Config(
                "ClickHouse incremental replication requires a non-empty `column`".into(),
            ));
        }
        if self.incremental_without_bookmark_pushdown() {
            tracing::warn!(
                "ClickHouse incremental replication query has no `@bookmark` token: the \
                 cursor is applied client-side only, so the server returns the ENTIRE \
                 result set on every run (correctness is preserved, but it is a full \
                 re-scan). Add `@bookmark` to the WHERE clause to push the cursor down, \
                 e.g. `... WHERE {column} > @bookmark`",
                column = match &self.replication {
                    ClickHouseReplication::Incremental { column, .. } => column.as_str(),
                    _ => "<column>",
                }
            );
        }
        Ok(())
    }

    /// `true` when replication is `Incremental` but the query omits the
    /// `@bookmark` token, so the cursor cannot be pushed down and every run
    /// re-scans the whole result set. Pure predicate so the load-time warning's
    /// condition is unit-testable.
    pub(crate) fn incremental_without_bookmark_pushdown(&self) -> bool {
        matches!(self.replication, ClickHouseReplication::Incremental { .. })
            && !self.query.contains("@bookmark")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn config_flattens_connection_fields() {
        let cfg: ClickHouseSourceConfig = serde_json::from_value(json!({
            "url": "http://localhost:8123",
            "database": "analytics",
            "query": "SELECT 1",
        }))
        .unwrap();
        assert_eq!(cfg.connection.url.as_deref(), Some("http://localhost:8123"));
        assert_eq!(cfg.connection.database, "analytics");
        assert_eq!(cfg.batch_size, DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn replication_full_is_default() {
        let cfg = ClickHouseSourceConfig::new("http://h:8123", "SELECT 1");
        assert_eq!(cfg.replication, ClickHouseReplication::Full);
    }

    #[test]
    fn replication_incremental_parses() {
        let r: ClickHouseReplication = serde_json::from_value(json!({
            "type": "incremental",
            "column": "updated_at",
            "initial_value": "1970-01-01",
        }))
        .unwrap();
        assert_eq!(
            r,
            ClickHouseReplication::Incremental {
                column: "updated_at".into(),
                initial_value: json!("1970-01-01"),
            }
        );
    }

    #[test]
    fn validate_rejects_incremental_without_column() {
        let cfg = ClickHouseSourceConfig {
            replication: ClickHouseReplication::Incremental {
                column: "  ".into(),
                initial_value: json!(0),
            },
            ..ClickHouseSourceConfig::new("http://h:8123", "SELECT 1")
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_batch_size() {
        let cfg = ClickHouseSourceConfig::new("http://h:8123", "SELECT 1")
            .with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_missing_endpoint() {
        let mut cfg = ClickHouseSourceConfig::new("http://h:8123", "SELECT 1");
        cfg.connection.url = None;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn incremental_without_bookmark_pushdown_flags_missing_token() {
        let missing = ClickHouseSourceConfig::new("http://h:8123", "SELECT * FROM t")
            .incremental("updated_at", json!("1970-01-01"));
        assert!(missing.incremental_without_bookmark_pushdown());
        assert!(missing.validate().is_ok(), "warn, not hard error");

        let with_token = ClickHouseSourceConfig::new(
            "http://h:8123",
            "SELECT * FROM t WHERE updated_at > @bookmark",
        )
        .incremental("updated_at", json!("1970-01-01"));
        assert!(!with_token.incremental_without_bookmark_pushdown());

        let full = ClickHouseSourceConfig::new("http://h:8123", "SELECT * FROM t");
        assert!(!full.incremental_without_bookmark_pushdown());
    }

    #[test]
    fn debug_masks_password() {
        let mut cfg = ClickHouseSourceConfig::new("http://h:8123", "SELECT 1");
        cfg.connection.password = Some("s3cret".into());
        let dbg = format!("{cfg:?}");
        assert!(dbg.contains("***"));
        assert!(!dbg.contains("s3cret"));
    }
}
