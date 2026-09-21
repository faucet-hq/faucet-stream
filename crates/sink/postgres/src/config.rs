//! PostgreSQL sink configuration.

use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// How to map JSON records to table columns.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PostgresColumnMapping {
    /// Insert each record as a single `jsonb` column. The column name
    /// defaults to `"data"` but can be overridden.
    Jsonb { column: String },
    /// Map top-level JSON keys directly to table columns.
    /// Only keys that match existing columns are inserted; extra keys are ignored.
    AutoMap,
}

impl Default for PostgresColumnMapping {
    fn default() -> Self {
        Self::Jsonb {
            column: "data".into(),
        }
    }
}

/// How rows are shipped to PostgreSQL in append mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PostgresWriteMethod {
    /// Multi-row `INSERT INTO … VALUES …` — the default. Works with every
    /// write mode and the exactly-once transaction path.
    #[default]
    Insert,
    /// `COPY … FROM STDIN (FORMAT text)` bulk load — typically **5–10×
    /// faster** than multi-row `INSERT` for bulk append (issue #308).
    ///
    /// **Append-only**: `COPY` has no `ON CONFLICT`, so combining
    /// `write_method: copy` with `write_mode: upsert|delete` is rejected at
    /// config load. The exactly-once path (`write_batch_idempotent`) always
    /// uses the `INSERT`/transaction path regardless of this setting — the
    /// watermark token must commit atomically with the page's data.
    ///
    /// Error semantics are all-or-nothing per batch, the same as a failed
    /// multi-row `INSERT`: one bad row fails the whole `COPY` and the DLQ
    /// router's `on_batch_error` policy applies.
    Copy,
}

/// Configuration for the PostgreSQL sink.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct PostgresSinkConfig {
    /// PostgreSQL connection URL (e.g. `postgres://user:pass@host/db`).
    pub connection_url: String,
    /// Target table name.
    pub table_name: String,
    /// Optional schema (namespace) qualifying [`table_name`](Self::table_name).
    ///
    /// When set, both the AutoMap column-discovery probe and the `INSERT`
    /// target `schema.table_name` explicitly. When unset (the default), the
    /// table resolves against the connection's `search_path`, and column
    /// discovery is scoped to whichever schema the `INSERT` actually resolves
    /// to — so a same-named table in another schema no longer pollutes the
    /// AutoMap column set (#146 M13).
    #[serde(default)]
    pub schema: Option<String>,
    /// How to map JSON records to columns. Defaults to a single `jsonb`
    /// column named `data`.
    #[serde(default)]
    pub column_mapping: PostgresColumnMapping,
    /// Maximum rows per multi-row `INSERT` statement. Defaults to
    /// [`DEFAULT_BATCH_SIZE`].
    ///
    /// When the upstream `StreamPage` carries more records than `batch_size`,
    /// the sink slices the page into `batch_size`-row chunks and issues one
    /// multi-row `INSERT` per chunk. When `batch_size = 0`, the entire slice
    /// is sent in a single `INSERT` — useful when the source already chunks
    /// to a Postgres-friendly size.
    ///
    /// `batch_size = 0` is the "no batching" sentinel: the entire upstream
    /// page is forwarded in one statement, subject to Postgres' natural
    /// per-statement bind-parameter limit of 65 535. AutoMap mode binds one
    /// parameter per column per row, so the safe ceiling is roughly
    /// `65_535 / num_columns` rows per call; JSONB mode binds a single
    /// array parameter and has no such ceiling. Keep the default unless the
    /// upstream page size is already tuned for Postgres.
    ///
    /// **Recommended value: ~1000** — Postgres' multi-row `INSERT` sweet
    /// spot. Larger chunks rarely add throughput and risk hitting the
    /// 65 535-parameter ceiling in AutoMap mode.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Maximum number of connections in the pool. Defaults to 5.
    ///
    /// Bounded on purpose: the pool must be finite so a wide fan-out (many
    /// matrix rows / shards) cannot exhaust the server's own `max_connections`.
    /// There is no "unlimited" setting — raise this explicitly if you need more.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    /// Create the target table (and its schema, when `schema:` is set) if it
    /// does not exist, inferring the columns from the first written page
    /// (#580). Enabled by default: a first-ever sync cannot assume the
    /// destination already exists.
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
    /// How append-mode rows are shipped: multi-row `insert` (default) or the
    /// `copy` bulk-load fast-path (`COPY … FROM STDIN`, typically 5–10×
    /// faster for bulk append). `copy` is append-only — it is rejected with
    /// `write_mode: upsert|delete` — and the exactly-once path always stays
    /// on `insert` so data + watermark commit in one transaction.
    #[serde(default)]
    pub write_method: PostgresWriteMethod,
    /// Scoped/windowed overwrite (#518): with `write_mode: overwrite`, replace
    /// only the rows matching this scope (e.g. a date window) instead of
    /// truncating the whole table. The prior out-of-scope rows are preserved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<faucet_core::OverwriteScope>,
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

impl std::fmt::Debug for PostgresSinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresSinkConfig")
            .field("connection_url", &"***")
            .field("table_name", &self.table_name)
            .field("schema", &self.schema)
            .field("column_mapping", &self.column_mapping)
            .field("batch_size", &self.batch_size)
            .field("max_connections", &self.max_connections)
            .field("write_method", &self.write_method)
            .finish()
    }
}

impl PostgresSinkConfig {
    /// Create a new config with required fields and sensible defaults.
    pub fn new(connection_url: impl Into<String>, table_name: impl Into<String>) -> Self {
        Self {
            connection_url: connection_url.into(),
            table_name: table_name.into(),
            schema: None,
            column_mapping: PostgresColumnMapping::default(),
            batch_size: DEFAULT_BATCH_SIZE,
            max_connections: 5,
            create_table: default_create_table(),
            write: faucet_core::WriteSpec::default(),
            write_method: PostgresWriteMethod::default(),
            scope: None,
        }
    }

    /// Opt out of auto-creating a missing target table (#580).
    pub fn with_create_table(mut self, create: bool) -> Self {
        self.create_table = create;
        self
    }

    /// Set the schema (namespace) that qualifies the table. When unset, the
    /// table resolves against the connection's `search_path`.
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Set the column mapping strategy.
    pub fn column_mapping(mut self, mapping: PostgresColumnMapping) -> Self {
        self.column_mapping = mapping;
        self
    }

    /// Set the per-statement row count for multi-row `INSERT`.
    ///
    /// Pass `0` to opt out of re-chunking — the sink forwards each upstream
    /// [`StreamPage`](faucet_core::StreamPage) as a single `INSERT`
    /// statement. Postgres' multi-row `INSERT` sweet spot is ~1000 rows.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the maximum number of connections in the pool.
    pub fn max_connections(mut self, n: u32) -> Self {
        self.max_connections = n;
        self
    }

    /// Choose how append-mode rows are shipped (`insert` vs the `copy`
    /// bulk-load fast-path). See [`PostgresWriteMethod`].
    pub fn with_write_method(mut self, method: PostgresWriteMethod) -> Self {
        self.write_method = method;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = PostgresSinkConfig::new("postgres://localhost/test", "events");
        assert_eq!(config.table_name, "events");
        assert_eq!(config.batch_size, DEFAULT_BATCH_SIZE);
        assert!(matches!(
            config.column_mapping,
            PostgresColumnMapping::Jsonb { ref column } if column == "data"
        ));
    }

    #[test]
    fn builder_methods() {
        let config = PostgresSinkConfig::new("postgres://localhost/test", "events")
            .column_mapping(PostgresColumnMapping::AutoMap)
            .with_batch_size(100);
        assert_eq!(config.batch_size, 100);
        assert!(matches!(
            config.column_mapping,
            PostgresColumnMapping::AutoMap
        ));
    }

    #[test]
    fn jsonb_custom_column() {
        let config = PostgresSinkConfig::new("postgres://localhost/test", "events").column_mapping(
            PostgresColumnMapping::Jsonb {
                column: "payload".into(),
            },
        );
        assert!(matches!(
            config.column_mapping,
            PostgresColumnMapping::Jsonb { ref column } if column == "payload"
        ));
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let config =
            PostgresSinkConfig::new("postgres://localhost/test", "events").with_batch_size(250);
        assert_eq!(config.batch_size, 250);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config =
            PostgresSinkConfig::new("postgres://localhost/test", "events").with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let config = PostgresSinkConfig::new("postgres://localhost/test", "events")
            .with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_err());
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let json = r#"{
            "connection_url": "postgres://localhost/test",
            "table_name": "events",
            "column_mapping": {"jsonb": {"column": "data"}},
            "batch_size": 250,
            "max_connections": 5
        }"#;
        let config: PostgresSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, 250);
    }

    #[test]
    fn batch_size_defaults_when_absent_in_json() {
        let json = r#"{
            "connection_url": "postgres://localhost/test",
            "table_name": "events",
            "column_mapping": {"jsonb": {"column": "data"}},
            "max_connections": 5
        }"#;
        let config: PostgresSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn config_builder_chaining() {
        let config = PostgresSinkConfig::new("postgres://localhost/test", "events")
            .with_batch_size(100)
            .with_batch_size(250);
        assert_eq!(config.batch_size, 250);
    }

    #[test]
    fn max_connections_and_column_mapping_default_when_absent_in_json() {
        // Only the two genuinely-required fields are supplied; the pool size and
        // column mapping must fall back to their documented defaults rather than
        // failing to deserialize.
        let json = r#"{
            "connection_url": "postgres://localhost/test",
            "table_name": "events"
        }"#;
        let config: PostgresSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.max_connections, 5);
        assert!(matches!(
            config.column_mapping,
            PostgresColumnMapping::Jsonb { .. }
        ));
        assert_eq!(config.batch_size, DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn max_connections_deserializes_when_present() {
        let json = r#"{
            "connection_url": "postgres://localhost/test",
            "table_name": "events",
            "max_connections": 20
        }"#;
        let config: PostgresSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.max_connections, 20);
    }

    #[test]
    fn write_method_defaults_to_insert() {
        let config = PostgresSinkConfig::new("postgres://localhost/test", "events");
        assert_eq!(config.write_method, PostgresWriteMethod::Insert);

        // Absent in JSON → insert (back-compat: existing configs unchanged).
        let json = r#"{
            "connection_url": "postgres://localhost/test",
            "table_name": "events"
        }"#;
        let config: PostgresSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.write_method, PostgresWriteMethod::Insert);
    }

    #[test]
    fn write_method_serde_round_trips() {
        let json = r#"{
            "connection_url": "postgres://localhost/test",
            "table_name": "events",
            "write_method": "copy"
        }"#;
        let config: PostgresSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.write_method, PostgresWriteMethod::Copy);
        let text = serde_json::to_string(&config).unwrap();
        assert!(text.contains("\"write_method\":\"copy\""));
    }

    #[test]
    fn with_write_method_builder() {
        let config = PostgresSinkConfig::new("postgres://localhost/test", "events")
            .with_write_method(PostgresWriteMethod::Copy);
        assert_eq!(config.write_method, PostgresWriteMethod::Copy);
    }
}
