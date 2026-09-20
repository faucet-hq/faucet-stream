//! DuckDB sink configuration.

use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// How to map JSON records to table columns.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DuckdbColumnMapping {
    /// Insert each record as a single JSON text column.
    Json { column: String },
    /// Map top-level JSON keys directly to table columns. Only keys that match
    /// existing columns are inserted; extra keys are ignored.
    AutoMap,
}

impl Default for DuckdbColumnMapping {
    fn default() -> Self {
        Self::Json {
            column: "data".into(),
        }
    }
}

/// Configuration for the DuckDB sink.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DuckdbSinkConfig {
    /// Path to the DuckDB database file, or `:memory:`. A `duckdb://` /
    /// `duckdb:` scheme prefix is accepted and stripped. The target table must
    /// already exist.
    pub database: String,
    /// Target table name.
    pub table_name: String,
    /// How to map JSON records to columns.
    #[serde(default)]
    pub column_mapping: DuckdbColumnMapping,
    /// Maximum number of rows per multi-row INSERT. Defaults to
    /// [`DEFAULT_BATCH_SIZE`] (1000). `batch_size = 0` writes the entire
    /// upstream slice as a single multi-row INSERT (no re-chunking).
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Create the target table if it does not exist, inferring the columns
    /// from the first written page (#580). Enabled by default: a first-ever
    /// sync cannot assume the destination already exists. Every inferred
    /// column is created nullable. Set `false` to require the table to
    /// pre-exist and fail fast when it is missing.
    #[serde(default = "default_create_table")]
    pub create_table: bool,
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

fn default_create_table() -> bool {
    true
}

impl DuckdbSinkConfig {
    /// Create a new config with required fields and sensible defaults.
    pub fn new(database: impl Into<String>, table_name: impl Into<String>) -> Self {
        Self {
            database: database.into(),
            table_name: table_name.into(),
            column_mapping: DuckdbColumnMapping::default(),
            batch_size: DEFAULT_BATCH_SIZE,
            create_table: default_create_table(),
        }
    }

    /// Opt out of auto-creating a missing target table (#580).
    pub fn with_create_table(mut self, create: bool) -> Self {
        self.create_table = create;
        self
    }

    /// Set the column mapping strategy.
    pub fn column_mapping(mut self, mapping: DuckdbColumnMapping) -> Self {
        self.column_mapping = mapping;
        self
    }

    /// Set the maximum number of rows per multi-row INSERT. `0` disables
    /// re-chunking.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// The filesystem path with any `duckdb://` / `duckdb:` scheme stripped.
    pub(crate) fn resolved_path(&self) -> &str {
        self.database
            .trim_start_matches("duckdb://")
            .trim_start_matches("duckdb:")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = DuckdbSinkConfig::new(":memory:", "events");
        assert_eq!(config.table_name, "events");
        assert_eq!(config.batch_size, DEFAULT_BATCH_SIZE);
        assert!(matches!(
            config.column_mapping,
            DuckdbColumnMapping::Json { ref column } if column == "data"
        ));
    }

    #[test]
    fn builder_methods() {
        let config = DuckdbSinkConfig::new(":memory:", "events")
            .column_mapping(DuckdbColumnMapping::AutoMap)
            .with_batch_size(100);
        assert_eq!(config.batch_size, 100);
        assert!(matches!(
            config.column_mapping,
            DuckdbColumnMapping::AutoMap
        ));
    }

    #[test]
    fn batch_size_deserializes_and_defaults() {
        let json = r#"{ "database": ":memory:", "table_name": "events" }"#;
        let config: DuckdbSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, DEFAULT_BATCH_SIZE);
        let json2 = r#"{ "database": ":memory:", "table_name": "e", "batch_size": 250 }"#;
        let config2: DuckdbSinkConfig = serde_json::from_str(json2).unwrap();
        assert_eq!(config2.batch_size, 250);
    }
}
