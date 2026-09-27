//! Serde config types for the `mirror:` block (`faucet mirror`; the pre-#670
//! spellings `replication:` / `faucet replicate` are still accepted).
//!
//! The main `pipeline` is the CDC pipeline (its `source` is a CDC connector,
//! its `sink` the destination). `replication:` adds the one-time bulk-read
//! snapshot source used to back-fill before CDC starts. Consumed only by
//! `faucet replicate`; ignored by `faucet run` (like `schedule:`).

use crate::config::ConnectorSpec;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

fn default_true() -> bool {
    true
}

fn default_snapshot_concurrency() -> usize {
    4
}

fn default_discover_interval_secs() -> u64 {
    300
}

fn default_max_table_failures() -> u32 {
    3
}

fn default_retry_paused_secs() -> u64 {
    300
}

fn default_lag_warning_secs() -> u64 {
    300
}

fn default_include() -> Vec<String> {
    vec!["*".to_string()]
}

/// Top-level `mirror:` block (alias `replication:`).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ReplicationSpec {
    /// Replication strategy. Only `snapshot_then_cdc` is available in v1.
    pub mode: ReplicationMode,
    /// One-time bulk-read source used to back-fill the destination before CDC.
    pub snapshot: SnapshotSpec,
    /// After the snapshot completes, keep streaming CDC until SIGTERM/SIGINT.
    /// When `false`, drain CDC once and exit (useful for tests / batch runs).
    #[serde(default = "default_true")]
    pub continuous: bool,
    /// Mirror a **set of tables** over one change stream (#731) instead of the
    /// single table `pipeline.source` / `snapshot.source` describe. The table
    /// set comes from the snapshot source's `discover()`, filtered by these
    /// globs; each table gets its own snapshot → CDC handoff, state key and
    /// destination.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tables: Option<TablesSpec>,
    /// Per-table overrides keyed by the discovered table name (e.g.
    /// `public.orders`). Requires `tables`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub per_table: BTreeMap<String, TableOverride>,
}

/// The table set of a multi-table mirror (`mirror.tables`, #731).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TablesSpec {
    /// Globs (`*` any run, `?` one character) over the discovered table names.
    /// Default `["*"]` — every table the snapshot source discovers.
    #[serde(default = "default_include")]
    pub include: Vec<String>,
    /// Globs removing tables from `include`.
    #[serde(default)]
    pub exclude: Vec<String>,
    /// What to do with a matching table created after the mirror started.
    #[serde(default)]
    pub new_tables: NewTables,
    /// How often (seconds) to re-run discovery to notice created and dropped
    /// tables. A change record for an unknown matching table also triggers
    /// it. `0` disables the periodic check.
    #[serde(default = "default_discover_interval_secs")]
    pub discover_interval_secs: u64,
    /// A table with no primary key: `refuse` it (reported in status, never
    /// mirrored) or mirror it `append`-only.
    #[serde(default)]
    pub without_primary_key: WithoutPrimaryKey,
    /// Sink-config patch that points the destination at one table, merged over
    /// `pipeline.sink.config`. String values may use `{table}` (the full
    /// discovered name), `{table_name}` (its last segment) and `{schema}`
    /// (everything before the last `.`). Defaults per sink kind
    /// (`table_name: "{table_name}"` for SQL sinks, `table_id` for BigQuery,
    /// `collection` for MongoDB, `index` for Elasticsearch).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination: Option<Value>,
    /// A table whose pipeline keeps failing: `pause` it (explicit status, the
    /// rest of the stream continues, re-snapshotted after
    /// `retry_paused_secs`) or `fail` the whole mirror.
    #[serde(default)]
    pub on_table_error: OnTableError,
    /// Consecutive failed stream cycles before a table is paused.
    #[serde(default = "default_max_table_failures")]
    pub max_table_failures: u32,
    /// Seconds before a paused table is re-snapshotted and rejoins. `0` keeps
    /// it paused until the mirror restarts.
    #[serde(default = "default_retry_paused_secs")]
    pub retry_paused_secs: u64,
    /// A table whose last applied change is older than this (seconds) while
    /// others advance is reported as lagging and logged as a warning — it is
    /// holding the shared stream's resume position back.
    #[serde(default = "default_lag_warning_secs")]
    pub lag_warning_secs: u64,
}

/// `mirror.tables.new_tables`.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NewTables {
    /// Snapshot a newly created matching table at the current position, then
    /// route its changes (default).
    #[default]
    Follow,
    /// Mirror only the tables that matched when the mirror first started.
    Ignore,
}

/// `mirror.tables.without_primary_key`.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WithoutPrimaryKey {
    /// Do not mirror the table; report it as `refused` (default).
    #[default]
    Refuse,
    /// Mirror it with `write_mode: append` (updates and deletes append rows).
    Append,
}

/// `mirror.tables.on_table_error`.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OnTableError {
    /// Pause the failing table and keep mirroring the rest (default).
    #[default]
    Pause,
    /// Stop the whole mirror with the table's error.
    Fail,
}

/// One entry of `mirror.per_table`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TableOverride {
    /// Upsert key; defaults to the discovered primary key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<Vec<String>>,
    /// Destination write mode (`append` / `upsert` / `delete`); defaults to
    /// the sink template's, else `upsert` on a keyed table.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_mode: Option<String>,
    /// Extra sink-config patch for this table (merged after `destination`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sink: Option<Value>,
    /// Snapshot-source config patch for this table (merged after the
    /// discovered selection, e.g. a narrower `query`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<Value>,
    /// Schema-drift policy for this table, replacing the top-level `schema:`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_drift: Option<faucet_core::SchemaDriftSpec>,
}

/// Replication strategy discriminator.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReplicationMode {
    /// Capture the CDC position, bulk-snapshot the table, then stream CDC from
    /// that position. Pair with `write_mode: upsert` for a true mirror.
    SnapshotThenCdc,
}

/// The one-time snapshot source (a non-CDC bulk reader of the same upstream DB).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SnapshotSpec {
    /// Bulk-read source connector (e.g. `postgres` running `SELECT * FROM t`).
    /// In a multi-table mirror this is the connection template: discovery
    /// runs against it and each table's selection is merged over it.
    pub source: ConnectorSpec,
    /// Tables snapshotted in parallel (multi-table mirror only).
    #[serde(default = "default_snapshot_concurrency")]
    pub concurrency: usize,
    /// Split each table's snapshot into this many primary-key ranges read in
    /// parallel (multi-table mirror only; needs a single-column integer key
    /// and a source that supports `shard:`). `0` / `1` reads the table whole.
    #[serde(default)]
    pub shards: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_replication_block() {
        let yaml = r#"
mode: snapshot_then_cdc
snapshot:
  source:
    type: postgres
    config: { connection_url: "postgres://x", query: "SELECT * FROM orders" }
"#;
        let spec: ReplicationSpec = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(spec.mode, ReplicationMode::SnapshotThenCdc);
        assert_eq!(spec.snapshot.source.kind, "postgres");
        assert!(spec.continuous, "continuous defaults to true");
    }

    #[test]
    fn continuous_false_parses() {
        let yaml = r#"
mode: snapshot_then_cdc
continuous: false
snapshot:
  source: { type: postgres, config: {} }
"#;
        let spec: ReplicationSpec = serde_yaml::from_str(yaml).unwrap();
        assert!(!spec.continuous);
    }

    #[test]
    fn rejects_unknown_field() {
        let yaml = r#"
mode: snapshot_then_cdc
snapshot: { source: { type: postgres, config: {} } }
bogus: true
"#;
        assert!(serde_yaml::from_str::<ReplicationSpec>(yaml).is_err());
    }
}
