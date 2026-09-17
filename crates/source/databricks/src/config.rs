//! Databricks SQL query source configuration.

use faucet_core::{AuthSpec, DEFAULT_BATCH_SIZE, FaucetError};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// How the source replicates rows across runs.
///
/// Serializes as `{ type: full }` or
/// `{ type: incremental, column: "...", initial_value: ... }`.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, Default, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DatabricksReplication {
    /// Every run fetches the full result set (default).
    #[default]
    Full,
    /// Only rows whose `column` is strictly greater than the stored bookmark
    /// (or `initial_value` on the first run) are emitted. If the SQL contains
    /// the literal token `${bookmark}`, it is bound as a `:_faucet_bookmark`
    /// named parameter so the warehouse filters server-side (efficient); the
    /// source also filters client-side as a correctness backstop. The new
    /// maximum of `column` is persisted on the final page.
    Incremental {
        /// Column whose value is the replication cursor (e.g. `updated_at`).
        column: String,
        /// Lower bound used on the first run, before any bookmark is stored.
        initial_value: Value,
    },
}

fn default_wait_timeout() -> u64 {
    50
}

fn default_poll_interval() -> u64 {
    1
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

/// Authentication for the Databricks SQL Statement Execution API.
///
/// Both variants send `Authorization: Bearer <token>` — Databricks accepts a
/// Personal Access Token (PAT) or an OAuth machine-to-machine (M2M) access
/// token in the same header. Uses the project-wide adjacently-tagged
/// `{ type, config }` shape, e.g. `auth: { type: pat, config: { token: … } }`.
/// A shared `auth: { ref: <name> }` provider that yields a `Bearer`/`Token`
/// credential maps onto [`DatabricksAuth::Token`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum DatabricksAuth {
    /// Databricks Personal Access Token.
    Pat {
        /// The token string (use `${env:…}` / `${vault:…}` to inject).
        token: String,
    },
    /// A pre-obtained OAuth (M2M) bearer token.
    Token {
        /// The bearer token string.
        token: String,
    },
}

impl DatabricksAuth {
    /// The `Authorization` header value (`Bearer <token>`).
    pub fn authorization_value(&self) -> String {
        match self {
            DatabricksAuth::Pat { token } | DatabricksAuth::Token { token } => {
                format!("Bearer {token}")
            }
        }
    }
}

/// A named SQL parameter passed to the statement (`:name` markers in the SQL).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DatabricksParam {
    /// Parameter name (the `:name` marker in the SQL, without the colon).
    pub name: String,
    /// Parameter value. Sent to Databricks as its string form; `null` binds a
    /// typed NULL.
    #[serde(default)]
    pub value: Value,
    /// Optional Databricks SQL type (e.g. `INT`, `STRING`, `DATE`, `TIMESTAMP`).
    /// When omitted, Databricks treats the value as `STRING`.
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub param_type: Option<String>,
}

/// Configuration for the Databricks SQL query source.
///
/// Runs `sql` against a Databricks SQL Warehouse via the Statement Execution
/// REST API and streams the result rows as typed JSON objects.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct DatabricksSourceConfig {
    /// Workspace base URL, e.g. `https://dbc-abc123.cloud.databricks.com`.
    pub workspace_url: String,
    /// Target SQL Warehouse id.
    pub warehouse_id: String,
    /// The SQL statement to run. May contain `:name` parameter markers (see
    /// [`parameters`](Self::parameters)) and a `${bookmark}` token for
    /// incremental replication.
    pub sql: String,
    /// Authentication (PAT / OAuth bearer), inline or via a shared `auth: { ref }`.
    pub auth: AuthSpec<DatabricksAuth>,
    /// Default Unity Catalog catalog for the statement.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,
    /// Default schema for the statement.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    /// Named SQL parameters (`:name` markers).
    #[serde(default)]
    pub parameters: Vec<DatabricksParam>,
    /// Server-side wait before the statement goes async (seconds). Databricks
    /// accepts `0` (fully async) or `5`–`50`. Defaults to `50`.
    #[serde(default = "default_wait_timeout")]
    pub wait_timeout_secs: u64,
    /// Client poll cadence while the statement is `PENDING`/`RUNNING` (seconds).
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: u64,
    /// Page size — rows accumulated before a `StreamPage` is emitted.
    /// Defaults to [`DEFAULT_BATCH_SIZE`](faucet_core::DEFAULT_BATCH_SIZE).
    ///
    /// `batch_size = 0` is the "no batching" sentinel: result chunks are fully
    /// drained and the entire result set is emitted in a single page. Useful
    /// for small lookup tables or for sinks (e.g. SQL `COPY`, BigQuery load
    /// jobs) that prefer one large request to many small ones.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Fetch results as Apache Arrow instead of JSON. When `true`, the
    /// statement is submitted with `EXTERNAL_LINKS` disposition +
    /// `ARROW_STREAM` format; each chunk's presigned link is fetched and
    /// decoded as an Arrow IPC stream. This enables the **columnar** fast path
    /// ([`Source::stream_batches`](faucet_core::Source::stream_batches)) so a
    /// `databricks → parquet`/`delta` chain skips the per-cell JSON decode.
    ///
    /// Requires the crate-local `arrow` feature, and (in this release) only
    /// [`DatabricksReplication::Full`] — the columnar path does not run the
    /// per-row client-side incremental filter. Defaults to `false` (the
    /// `INLINE` + `JSON_ARRAY` row path, unchanged). RFC 0002 / #375.
    #[serde(default)]
    pub arrow_native: bool,
    /// Replication mode. Defaults to [`DatabricksReplication::Full`].
    #[serde(default)]
    pub replication: DatabricksReplication,
    /// Explicit state-store key for the incremental bookmark. When unset, a key
    /// is derived from the workspace/warehouse and a query fingerprint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_key: Option<String>,
}

impl DatabricksSourceConfig {
    /// Validate the config; returns a human-readable error if invalid.
    pub fn validate(&self) -> Result<(), FaucetError> {
        if self.workspace_url.trim().is_empty() {
            return Err(FaucetError::Config(
                "databricks: `workspace_url` must not be empty".into(),
            ));
        }
        if self.warehouse_id.trim().is_empty() {
            return Err(FaucetError::Config(
                "databricks: `warehouse_id` must not be empty".into(),
            ));
        }
        if self.sql.trim().is_empty() {
            return Err(FaucetError::Config(
                "databricks: `sql` must not be empty".into(),
            ));
        }
        // Databricks accepts wait_timeout of 0 (async) or 5..=50 seconds.
        if self.wait_timeout_secs != 0 && !(5..=50).contains(&self.wait_timeout_secs) {
            return Err(FaucetError::Config(format!(
                "databricks: `wait_timeout_secs` must be 0 or between 5 and 50 (got {})",
                self.wait_timeout_secs
            )));
        }
        faucet_core::validate_batch_size(self.batch_size)?;
        if self.arrow_native {
            if !cfg!(feature = "arrow") {
                return Err(FaucetError::Config(
                    "databricks: `arrow_native` requires the crate-local `arrow` feature to be \
                     enabled"
                        .into(),
                ));
            }
            if !matches!(self.replication, DatabricksReplication::Full) {
                return Err(FaucetError::Config(
                    "databricks: `arrow_native` currently supports only `replication: full` — the \
                     columnar path does not run the client-side incremental filter"
                        .into(),
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn base() -> DatabricksSourceConfig {
        DatabricksSourceConfig {
            workspace_url: "https://x.cloud.databricks.com".into(),
            warehouse_id: "wh1".into(),
            sql: "SELECT 1".into(),
            auth: AuthSpec::Inline(DatabricksAuth::Pat { token: "t".into() }),
            catalog: None,
            schema: None,
            parameters: Vec::new(),
            wait_timeout_secs: default_wait_timeout(),
            poll_interval_secs: default_poll_interval(),
            batch_size: DEFAULT_BATCH_SIZE,
            arrow_native: false,
            replication: DatabricksReplication::Full,
            state_key: None,
        }
    }

    #[test]
    fn valid_config_passes() {
        base().validate().unwrap();
    }

    #[cfg(feature = "arrow")]
    #[test]
    fn arrow_native_full_passes_but_incremental_rejected() {
        let mut c = base();
        c.arrow_native = true;
        c.validate().unwrap();
        c.replication = DatabricksReplication::Incremental {
            column: "ts".into(),
            initial_value: json!("2026-01-01"),
        };
        let err = c.validate().unwrap_err();
        assert!(err.to_string().contains("arrow_native"));
    }

    #[cfg(not(feature = "arrow"))]
    #[test]
    fn arrow_native_requires_feature() {
        let mut c = base();
        c.arrow_native = true;
        let err = c.validate().unwrap_err();
        assert!(err.to_string().contains("arrow"));
    }

    #[test]
    fn auth_is_bearer_for_both_variants() {
        assert_eq!(
            DatabricksAuth::Pat {
                token: "abc".into()
            }
            .authorization_value(),
            "Bearer abc"
        );
        assert_eq!(
            DatabricksAuth::Token {
                token: "xyz".into()
            }
            .authorization_value(),
            "Bearer xyz"
        );
    }

    #[test]
    fn rejects_empty_required_fields() {
        let mut c = base();
        c.workspace_url = "  ".into();
        assert!(c.validate().is_err());
        let mut c = base();
        c.warehouse_id = "".into();
        assert!(c.validate().is_err());
        let mut c = base();
        c.sql = "".into();
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_bad_wait_timeout() {
        let mut c = base();
        c.wait_timeout_secs = 3; // not 0 and not in 5..=50
        assert!(c.validate().is_err());
        c.wait_timeout_secs = 51;
        assert!(c.validate().is_err());
        c.wait_timeout_secs = 0; // allowed (fully async)
        assert!(c.validate().is_ok());
    }

    #[test]
    fn rejects_oversized_batch() {
        let mut c = base();
        c.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        assert!(c.validate().is_err());
    }

    #[test]
    fn deserializes_full_shape() {
        let v = json!({
            "workspace_url": "https://x.cloud.databricks.com",
            "warehouse_id": "wh1",
            "sql": "SELECT * FROM t WHERE id > :min",
            "auth": { "type": "pat", "config": { "token": "tok" } },
            "catalog": "main",
            "schema": "sales",
            "parameters": [{ "name": "min", "value": 10, "type": "INT" }],
            "wait_timeout_secs": 30,
            "batch_size": 500
        });
        let c: DatabricksSourceConfig = serde_json::from_value(v).unwrap();
        assert_eq!(c.warehouse_id, "wh1");
        assert_eq!(c.catalog.as_deref(), Some("main"));
        assert_eq!(c.parameters.len(), 1);
        assert_eq!(c.parameters[0].name, "min");
        assert_eq!(c.parameters[0].param_type.as_deref(), Some("INT"));
        c.validate().unwrap();
    }
}
