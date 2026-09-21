//! SQLite sink configuration.

use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// How to map JSON records to table columns.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SqliteColumnMapping {
    /// Insert each record as a single JSON text column. The column name
    /// defaults to `"data"` but can be overridden.
    Json { column: String },
    /// Map top-level JSON keys directly to table columns.
    /// Only keys that match existing columns are inserted; extra keys are ignored.
    AutoMap,
}

impl Default for SqliteColumnMapping {
    fn default() -> Self {
        Self::Json {
            column: "data".into(),
        }
    }
}

/// Configuration for the SQLite sink.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SqliteSinkConfig {
    /// SQLite database URL (file path or `:memory:`).
    pub database_url: String,
    /// Target table name.
    pub table_name: String,
    /// How to map JSON records to columns.
    pub column_mapping: SqliteColumnMapping,
    /// Maximum number of rows per multi-row INSERT. Defaults to
    /// [`DEFAULT_BATCH_SIZE`] (1000); the recommended value for SQLite.
    ///
    /// When the upstream `StreamPage` carries more records than `batch_size`,
    /// the sink slices the page into `batch_size`-row chunks and issues one
    /// multi-row INSERT per chunk, each wrapped in its own `BEGIN`/`COMMIT`
    /// transaction. When `batch_size = 0`, the entire slice is written as a
    /// single multi-row INSERT inside one transaction — useful when the
    /// source already emits pages sized for SQLite (e.g. a Postgres source
    /// configured with `batch_size: 1000`).
    ///
    /// `batch_size = 0` is the "no batching" sentinel: upstream framing is
    /// preserved exactly, no internal re-chunking is performed.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Maximum number of connections in the pool. Defaults to `1`.
    ///
    /// SQLite serializes writers at the file level, so a single SQLite file
    /// can only ever have one writer at a time. A multi-connection pool against
    /// one file therefore races for the write lock and risks `SQLITE_BUSY`
    /// errors under concurrency; one writer is the safe default. Raise this only
    /// if your workload is read-heavy against a WAL database (the sink opens the
    /// pool in WAL mode with a `busy_timeout`, so extra connections can still
    /// read concurrently with the single writer).
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
    /// Write mode, key columns, and optional delete marker. `write_mode`
    /// defaults to `append`. Upsert/delete require `column_mapping: auto_map`
    /// and a UNIQUE/PRIMARY KEY constraint on `key`.
    #[serde(flatten)]
    pub write: faucet_core::WriteSpec,
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

fn default_create_table() -> bool {
    true
}

fn default_max_connections() -> u32 {
    1
}

impl SqliteSinkConfig {
    /// Create a new config with required fields and sensible defaults.
    pub fn new(database_url: impl Into<String>, table_name: impl Into<String>) -> Self {
        Self {
            database_url: database_url.into(),
            table_name: table_name.into(),
            column_mapping: SqliteColumnMapping::default(),
            batch_size: DEFAULT_BATCH_SIZE,
            max_connections: default_max_connections(),
            create_table: default_create_table(),
            write: faucet_core::WriteSpec::default(),
        }
    }

    /// Set the column mapping strategy.
    /// Opt out of auto-creating a missing target table (#580).
    pub fn with_create_table(mut self, create: bool) -> Self {
        self.create_table = create;
        self
    }

    pub fn column_mapping(mut self, mapping: SqliteColumnMapping) -> Self {
        self.column_mapping = mapping;
        self
    }

    /// Set the maximum number of rows per multi-row INSERT.
    ///
    /// Pass `0` to opt out of re-chunking — the entire records slice handed
    /// to `write_batch` is written in a single multi-row INSERT inside one
    /// transaction, preserving upstream `StreamPage` framing.
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
        let config = SqliteSinkConfig::new("sqlite::memory:", "events");
        assert_eq!(config.table_name, "events");
        assert_eq!(config.batch_size, DEFAULT_BATCH_SIZE);
        assert!(matches!(
            config.column_mapping,
            SqliteColumnMapping::Json { ref column } if column == "data"
        ));
    }

    #[test]
    fn default_max_connections_is_one() {
        // SQLite serializes writers at the file level, so a single-writer pool
        // is the safe default — a multi-connection pool against one SQLite file
        // races for the write lock and risks SQLITE_BUSY (audit #146 LOW).
        let config = SqliteSinkConfig::new("sqlite::memory:", "events");
        assert_eq!(config.max_connections, 1);
    }

    #[test]
    fn default_max_connections_deserializes_to_one_when_absent() {
        let json = r#"{
            "database_url": "sqlite::memory:",
            "table_name": "events",
            "column_mapping": {"json": {"column": "data"}}
        }"#;
        let config: SqliteSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.max_connections, 1);
    }

    #[test]
    fn builder_methods() {
        let config = SqliteSinkConfig::new("sqlite::memory:", "events")
            .column_mapping(SqliteColumnMapping::AutoMap)
            .with_batch_size(100);
        assert_eq!(config.batch_size, 100);
        assert!(matches!(
            config.column_mapping,
            SqliteColumnMapping::AutoMap
        ));
    }

    #[test]
    fn json_custom_column() {
        let config = SqliteSinkConfig::new("sqlite::memory:", "events").column_mapping(
            SqliteColumnMapping::Json {
                column: "payload".into(),
            },
        );
        assert!(matches!(
            config.column_mapping,
            SqliteColumnMapping::Json { ref column } if column == "payload"
        ));
    }

    #[test]
    fn config_with_file_path() {
        let config = SqliteSinkConfig::new("/tmp/test.db", "events");
        assert_eq!(config.database_url, "/tmp/test.db");
    }

    #[test]
    fn batch_size_defaults_to_default_batch_size() {
        let config = SqliteSinkConfig::new("sqlite::memory:", "events");
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let config = SqliteSinkConfig::new("sqlite::memory:", "events").with_batch_size(250);
        assert_eq!(config.batch_size, 250);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config = SqliteSinkConfig::new("sqlite::memory:", "events").with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let config = SqliteSinkConfig::new("sqlite::memory:", "events")
            .with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_err());
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let json = r#"{
            "database_url": "sqlite::memory:",
            "table_name": "events",
            "column_mapping": {"json": {"column": "data"}},
            "batch_size": 250,
            "max_connections": 5
        }"#;
        let config: SqliteSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, 250);
    }

    #[test]
    fn batch_size_defaults_when_absent_in_json() {
        let json = r#"{
            "database_url": "sqlite::memory:",
            "table_name": "events",
            "column_mapping": {"json": {"column": "data"}},
            "max_connections": 5
        }"#;
        let config: SqliteSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    /// `create_table` defaults on since #580; this is the opt-out that
    /// selects the refusal path the integration tests assert against.
    #[test]
    fn create_table_can_be_turned_off() {
        let cfg = SqliteSinkConfig::new("sqlite::memory:", "t");
        assert!(cfg.create_table, "auto-create is the default (#580)");
        assert!(
            !SqliteSinkConfig::new("sqlite::memory:", "t")
                .with_create_table(false)
                .create_table
        );
    }
}
