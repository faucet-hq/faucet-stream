//! MySQL sink configuration.

use faucet_core::{DEFAULT_BATCH_SIZE, WriteSpec};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// How to map JSON records to table columns.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MysqlColumnMapping {
    /// Insert each record as a single JSON column. The column name
    /// defaults to `"data"` but can be overridden.
    Json { column: String },
    /// Map top-level JSON keys directly to table columns.
    /// Only keys that match existing columns are inserted; extra keys are ignored.
    AutoMap,
}

impl Default for MysqlColumnMapping {
    fn default() -> Self {
        Self::Json {
            column: "data".into(),
        }
    }
}

/// Configuration for the MySQL sink.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct MysqlSinkConfig {
    /// MySQL connection URL (e.g. `mysql://user:pass@host/db`).
    pub connection_url: String,
    /// Target table name.
    pub table_name: String,
    /// How to map JSON records to columns. Defaults to a single JSON column.
    #[serde(default)]
    pub column_mapping: MysqlColumnMapping,
    /// Maximum rows per multi-row `INSERT` statement. Defaults to
    /// [`DEFAULT_BATCH_SIZE`].
    ///
    /// When the upstream `StreamPage` carries more records than `batch_size`,
    /// the sink slices the page into `batch_size`-row chunks and issues one
    /// multi-row `INSERT INTO ... VALUES (...), (...), ...` statement per
    /// chunk. When `batch_size = 0`, the entire upstream page is forwarded
    /// in a single multi-row `INSERT` — useful when the source already
    /// chunks to a size tuned for MySQL.
    ///
    /// `batch_size = 0` is the "no batching" sentinel: the full upstream
    /// page is forwarded as one `INSERT`, subject to MySQL's
    /// `max_allowed_packet` limit (default 64MB). Keep the default unless
    /// the upstream `StreamPage` size is already tuned for MySQL.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Maximum number of connections in the pool. Defaults to 5.
    ///
    /// Bounded on purpose: a finite pool keeps a wide fan-out from exhausting
    /// the server's own connection limit. There is no "unlimited" setting.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    /// Create the target table if it does not exist, inferring the columns
    /// from the first written page (#580). Enabled by default: a first-ever
    /// sync cannot assume the destination already exists.
    ///
    /// Every inferred column is created **nullable** — a column that happened
    /// to be present in page 1 is not required forever, and a `NOT NULL`
    /// inferred from one page turns page 2 into a hard failure the first time
    /// a record omits the field. Narrowing is the `schema:` drift policy's job.
    ///
    /// Set `false` to require the table to pre-exist and fail fast when it is
    /// missing (the schema is managed externally, and a missing table means a
    /// typo rather than a first run).
    #[serde(default = "default_create_table")]
    pub create_table: bool,
    /// Write mode: `append` (default), `upsert`, or `delete`.
    ///
    /// `upsert` and `delete` require `column_mapping: auto_map` (key columns
    /// must be real table columns, not packed inside a JSON blob) and a
    /// non-empty `key` list. The table must already have a PRIMARY or UNIQUE
    /// index on the key columns; MySQL's `ON DUPLICATE KEY UPDATE` uses that
    /// index to detect conflicts.
    #[serde(flatten)]
    pub write: WriteSpec,
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

fn default_create_table() -> bool {
    true
}

fn default_max_connections() -> u32 {
    5
}

impl std::fmt::Debug for MysqlSinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MysqlSinkConfig")
            .field("connection_url", &"***")
            .field("table_name", &self.table_name)
            .field("column_mapping", &self.column_mapping)
            .field("batch_size", &self.batch_size)
            .field("max_connections", &self.max_connections)
            .finish()
    }
}

impl MysqlSinkConfig {
    /// Create a new config with required fields and sensible defaults.
    pub fn new(connection_url: impl Into<String>, table_name: impl Into<String>) -> Self {
        Self {
            connection_url: connection_url.into(),
            table_name: table_name.into(),
            column_mapping: MysqlColumnMapping::default(),
            batch_size: DEFAULT_BATCH_SIZE,
            max_connections: 5,
            create_table: default_create_table(),
            write: WriteSpec::default(),
        }
    }

    /// Set the column mapping strategy.
    /// Opt out of auto-creating a missing target table (#580).
    pub fn with_create_table(mut self, create: bool) -> Self {
        self.create_table = create;
        self
    }

    pub fn column_mapping(mut self, mapping: MysqlColumnMapping) -> Self {
        self.column_mapping = mapping;
        self
    }

    /// Set the per-statement row count for the multi-row `INSERT`.
    ///
    /// Pass `0` to opt out of re-chunking — the sink forwards each upstream
    /// [`StreamPage`](faucet_core::StreamPage) as a single multi-row
    /// `INSERT`. MySQL's multi-row insert sweet spot is ~1000 rows.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the maximum number of connections in the pool.
    pub fn max_connections(mut self, n: u32) -> Self {
        self.max_connections = n;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = MysqlSinkConfig::new("mysql://localhost/test", "events");
        assert_eq!(config.table_name, "events");
        assert_eq!(config.batch_size, DEFAULT_BATCH_SIZE);
        assert!(matches!(
            config.column_mapping,
            MysqlColumnMapping::Json { ref column } if column == "data"
        ));
    }

    #[test]
    fn builder_methods() {
        let config = MysqlSinkConfig::new("mysql://localhost/test", "events")
            .column_mapping(MysqlColumnMapping::AutoMap)
            .with_batch_size(100);
        assert_eq!(config.batch_size, 100);
        assert!(matches!(config.column_mapping, MysqlColumnMapping::AutoMap));
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let config = MysqlSinkConfig::new("mysql://localhost/test", "events").with_batch_size(250);
        assert_eq!(config.batch_size, 250);
    }

    #[test]
    fn json_custom_column() {
        let config = MysqlSinkConfig::new("mysql://localhost/test", "events").column_mapping(
            MysqlColumnMapping::Json {
                column: "payload".into(),
            },
        );
        assert!(matches!(
            config.column_mapping,
            MysqlColumnMapping::Json { ref column } if column == "payload"
        ));
    }

    #[test]
    fn debug_masks_connection_url() {
        let config = MysqlSinkConfig::new("mysql://secret:pass@host/db", "events");
        let debug = format!("{config:?}");
        assert!(debug.contains("***"));
        assert!(!debug.contains("secret"));
        assert!(!debug.contains("pass"));
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config = MysqlSinkConfig::new("mysql://localhost/test", "events").with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let config = MysqlSinkConfig::new("mysql://localhost/test", "events")
            .with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_err());
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let json = r#"{
            "connection_url": "mysql://localhost/test",
            "table_name": "events",
            "column_mapping": {"json": {"column": "data"}},
            "batch_size": 250,
            "max_connections": 5
        }"#;
        let config: MysqlSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, 250);
    }

    #[test]
    fn batch_size_defaults_when_absent_in_json() {
        let json = r#"{
            "connection_url": "mysql://localhost/test",
            "table_name": "events",
            "column_mapping": {"json": {"column": "data"}},
            "max_connections": 5
        }"#;
        let config: MysqlSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn with_batch_size_chaining() {
        let config = MysqlSinkConfig::new("mysql://localhost/test", "events")
            .with_batch_size(100)
            .with_batch_size(2_000);
        assert_eq!(config.batch_size, 2_000);
    }

    #[test]
    fn max_connections_and_column_mapping_default_when_absent_in_json() {
        // Only the two genuinely-required fields are supplied; pool size and
        // column mapping must fall back to their documented defaults.
        let json = r#"{
            "connection_url": "mysql://localhost/test",
            "table_name": "events"
        }"#;
        let config: MysqlSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.max_connections, 5);
        assert!(matches!(
            config.column_mapping,
            MysqlColumnMapping::Json { .. }
        ));
        assert_eq!(config.batch_size, DEFAULT_BATCH_SIZE);
    }
}
