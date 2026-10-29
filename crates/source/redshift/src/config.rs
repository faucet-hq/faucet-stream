//! Amazon Redshift source configuration.

use faucet_common_redshift::RedshiftConnection;
use faucet_core::{DEFAULT_BATCH_SIZE, FaucetError};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// How the Redshift source replicates rows across runs.
///
/// Serializes as `{ type: full }` or
/// `{ type: incremental, column: "...", initial_value: ... }`.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, Default, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RedshiftReplication {
    /// Every run re-fetches the full result set (default).
    #[default]
    Full,
    /// Only rows not yet read, in `column` order, resuming from the stored
    /// bookmark (or `initial_value` on the first run).
    ///
    /// The source wraps the query as
    /// `SELECT * FROM (<query>) AS q WHERE q."column" >= CAST($n AS <type>) ORDER BY q."column"`
    /// (the column's type as the server describes it) and drops the rows it
    /// already emitted at the bookmark value, so rows that share a cursor
    /// value (batch loads stamped with one `updated_at`, COPY jobs landing in
    /// several commits) are never skipped. Every page carries a bookmark, so
    /// a crash replays at most one page. Write `${bookmark}` in the query
    /// (with `>=`) to apply the cursor further inside it; a strict `>` is
    /// refused. Rows whose `column` is NULL are never read.
    Incremental {
        /// Column whose value is the replication cursor (e.g. `updated_at`).
        column: String,
        /// Lower bound (inclusive) used on the first run, before any bookmark
        /// is stored.
        initial_value: Value,
    },
}

fn default_max_connections() -> u32 {
    10
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

/// Configuration for the Amazon Redshift query source.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct RedshiftSourceConfig {
    /// Connection block (host / port / database / user / credentials / tls),
    /// flattened to the config top level.
    #[serde(flatten)]
    pub connection: RedshiftConnection,
    /// SQL query to execute. May contain `${field.path}` parent-context tokens
    /// (resolved to positional binds at runtime) and, for incremental
    /// replication, a `${bookmark}` token.
    #[schemars(extend("x-faucet-sql" = true))]
    pub query: String,
    /// Positional bind parameters for the query, applied in `$1, $2, …` order
    /// before any context- or bookmark-derived values. Defaults to empty.
    #[serde(default)]
    pub params: Vec<Value>,
    /// Maximum number of connections in the pool. Defaults to 10.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    /// Records per emitted [`StreamPage`](faucet_core::StreamPage). Rows are
    /// drained from the `sqlx` cursor and yielded whenever the buffer reaches
    /// this size. Defaults to [`DEFAULT_BATCH_SIZE`].
    ///
    /// `batch_size = 0` is the "no batching" sentinel: the cursor is fully
    /// drained and the entire result set is emitted in a single page.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Replication mode. Defaults to [`RedshiftReplication::Full`].
    #[serde(default)]
    pub replication: RedshiftReplication,
    /// Explicit state-store key for the incremental bookmark. When unset, a key
    /// is derived from the host / database and a query fingerprint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_key: Option<String>,
}

impl RedshiftSourceConfig {
    /// Validate the config; returns a human-readable error if invalid.
    pub fn validate(&self) -> Result<(), FaucetError> {
        if self.query.trim().is_empty() {
            return Err(FaucetError::Config(
                "redshift: `query` must not be empty".into(),
            ));
        }
        faucet_core::validate_batch_size(self.batch_size)?;
        let RedshiftReplication::Incremental {
            column,
            initial_value,
        } = &self.replication
        else {
            return Ok(());
        };
        if column.trim().is_empty() {
            return Err(FaucetError::Config(
                "redshift: incremental replication `column` must not be empty".into(),
            ));
        }
        if initial_value.is_null() {
            return Err(FaucetError::Config(
                "redshift: incremental replication requires a non-null `initial_value`".into(),
            ));
        }
        if faucet_core::replication::cursor::strict_comparison(&self.query, BOOKMARK_TOKEN) {
            return Err(FaucetError::Config(format!(
                "redshift: compare `{BOOKMARK_TOKEN}` with `>=`, not `>`: the source re-reads \
                 the bookmark value and drops the rows it already emitted, and a strict \
                 comparison skips rows that share it"
            )));
        }
        Ok(())
    }
}

/// The token a query uses to read the incremental bookmark
/// (`WHERE updated_at >= ${bookmark}`).
pub const BOOKMARK_TOKEN: &str = "${bookmark}";

#[cfg(test)]
mod tests;
