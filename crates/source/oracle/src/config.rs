//! Configuration for the Oracle query source.

use faucet_common_oracle::OracleConnectionConfig;
use faucet_core::{DEFAULT_BATCH_SIZE, FaucetError, validate_batch_size};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

fn default_max_connections() -> u32 {
    10
}
fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}
fn default_statement_timeout_secs() -> u64 {
    300
}

/// How the source replicates rows across runs.
///
/// Serializes as `{ type: full }` or
/// `{ type: incremental, column: "UPDATED_AT", initial_value: ... }`.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, Default, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OracleReplication {
    /// Every run fetches the full result set (default).
    #[default]
    Full,
    /// Only rows not yet read, in `column` order, resuming from the stored
    /// bookmark (or `initial_value` on the first run).
    ///
    /// The source wraps the query as
    /// `SELECT * FROM (<query>) q WHERE q."COLUMN" >= :bookmark ORDER BY q."COLUMN"`
    /// and drops the rows it already emitted at the bookmark value, so rows
    /// that share a cursor value (ties, late commits, `DATE`'s one-second
    /// resolution) are never skipped. Every page carries a bookmark, so a
    /// crash replays at most one page. Write `:bookmark` in the query (with
    /// `>=`) to apply the cursor further inside it; a strict `>` is refused.
    /// `column` names the key exactly as it appears in the output — Oracle
    /// upper-cases unquoted names. Rows whose `column` is NULL are never read.
    Incremental {
        /// Output column holding the replication cursor (e.g. `UPDATED_AT`).
        column: String,
        /// Lower bound (inclusive) used on the first run, before any bookmark
        /// is stored.
        initial_value: Value,
    },
}

/// Primary-key range sharding for clustered (Mode B) execution: each shard runs
/// `SELECT * FROM (<query>) WHERE "<key>" >= lo AND "<key>" < hi`. The key must
/// be an integer-valued output column, named exactly as Oracle reports it.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ShardConfig {
    /// Integer output column to range-partition on (quoted as an identifier).
    pub key: String,
}

/// Configuration for [`OracleSource`](crate::OracleSource).
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct OracleSourceConfig {
    /// Connection settings (`connect_string` or `host` + `service_name`/`sid`).
    #[serde(flatten)]
    pub connection: OracleConnectionConfig,
    /// SQL query. Bind [`params`](Self::params) with `:1`, `:2`, …, and the
    /// incremental cursor with `:bookmark`.
    #[schemars(extend("x-faucet-sql" = true))]
    pub query: String,
    /// Positional bind values for `:1`…`:n`.
    #[serde(default)]
    pub params: Vec<Value>,
    /// Maximum pooled sessions. Defaults to 10.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    /// Records per emitted page, also used as the fetch array size. `0` emits
    /// the whole result set as one page. Defaults to [`DEFAULT_BATCH_SIZE`].
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Per-round-trip call timeout in seconds (`0` disables). Defaults to 300.
    #[serde(default = "default_statement_timeout_secs")]
    pub statement_timeout_secs: u64,
    /// Replication mode. Defaults to [`OracleReplication::Full`].
    #[serde(default)]
    pub replication: OracleReplication,
    /// Explicit state-store key for the bookmark. Derived from the database and
    /// a query fingerprint when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_key: Option<String>,
    /// Optional PK-range sharding for clustered execution. Inert outside the
    /// cluster coordinator.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard: Option<ShardConfig>,
    /// Output columns whose text holds JSON (a `JSON_SERIALIZE(...)` or an
    /// `IS JSON` `VARCHAR2`/`CLOB`); their values are emitted as parsed JSON.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub json_columns: Vec<String>,
}

impl std::fmt::Debug for OracleSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OracleSourceConfig")
            .field("connection", &self.connection)
            .field("query", &self.query)
            .field("params", &self.params)
            .field("max_connections", &self.max_connections)
            .field("batch_size", &self.batch_size)
            .field("statement_timeout_secs", &self.statement_timeout_secs)
            .field("replication", &self.replication)
            .field("state_key", &self.state_key)
            .field("shard", &self.shard)
            .field("json_columns", &self.json_columns)
            .finish()
    }
}

impl OracleSourceConfig {
    /// A full-replication config with defaults elsewhere.
    pub fn new(connection: OracleConnectionConfig, query: impl Into<String>) -> Self {
        Self {
            connection,
            query: query.into(),
            params: Vec::new(),
            max_connections: default_max_connections(),
            batch_size: default_batch_size(),
            statement_timeout_secs: default_statement_timeout_secs(),
            replication: OracleReplication::Full,
            state_key: None,
            shard: None,
            json_columns: Vec::new(),
        }
    }

    /// Validate the fail-fast invariants.
    pub fn validate(&self) -> Result<(), FaucetError> {
        self.connection.validate()?;
        validate_batch_size(self.batch_size)?;
        if self.query.trim().is_empty() {
            return Err(FaucetError::Config(
                "oracle source requires a `query`".into(),
            ));
        }
        if let OracleReplication::Incremental {
            column,
            initial_value,
        } = &self.replication
        {
            if column.trim().is_empty() {
                return Err(FaucetError::Config(
                    "oracle incremental replication requires a non-empty `column`".into(),
                ));
            }
            faucet_common_oracle::quote_ident_oracle(column)?;
            if initial_value.is_null() {
                return Err(FaucetError::Config(
                    "oracle incremental replication requires a non-null `initial_value`".into(),
                ));
            }
            if faucet_core::replication::cursor::strict_comparison(
                &self.query.to_ascii_lowercase(),
                BOOKMARK_TOKEN,
            ) {
                return Err(FaucetError::Config(format!(
                    "oracle: compare `{BOOKMARK_TOKEN}` with `>=`, not `>`: the source re-reads \
                     the bookmark value and drops the rows it already emitted, and a strict \
                     comparison skips rows that share it"
                )));
            }
        }
        if let Some(shard) = &self.shard {
            faucet_common_oracle::quote_ident_oracle(&shard.key)?;
        }
        if let Some(key) = &self.state_key {
            faucet_core::state::validate_state_key(key)?;
        }
        Ok(())
    }
}

/// The placeholder a query uses to read the incremental bookmark
/// (`WHERE UPDATED_AT >= :bookmark`, any case).
pub const BOOKMARK_TOKEN: &str = ":bookmark";

#[cfg(test)]
mod tests;
