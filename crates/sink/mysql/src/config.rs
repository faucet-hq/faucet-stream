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
    /// Longest a page write may wait on the server, in seconds, before it
    /// fails. Defaults to 3600; `0` waits forever. The same bound applies to
    /// the commit-token read, schema reads and evolution, and the overwrite
    /// staging setup, `RENAME` swap and abort; the bulk statements of a
    /// scoped cleanup and a rollback are not bounded, because they can
    /// legitimately run longer.
    ///
    /// A peer that disappears without closing the connection (a failover, an
    /// idle-eviction by a NAT or load balancer) otherwise leaves the write
    /// waiting forever, and a scheduled or served run stuck in "running".
    #[serde(default = "default_write_timeout_secs")]
    pub write_timeout_secs: u64,
    /// How a string written to a binary column (`BLOB`/`VARBINARY`/`bytea`)
    /// becomes bytes: `base64` (default) decodes every value — the form every
    /// faucet source emits — and fails the row when one is not base64; `text`
    /// stores every string as its text; `auto` decodes canonical base64 and
    /// stores any other string as text, deciding per value, so a text value
    /// that happens to be valid base64 (`"user"`, a hex digest) is decoded.
    /// Set `text` when the column is fed plain text.
    #[serde(default)]
    pub binary_encoding: BinaryEncoding,
}

fn default_write_timeout_secs() -> u64 {
    3600
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
            .field("write_timeout_secs", &self.write_timeout_secs)
            .field("binary_encoding", &self.binary_encoding)
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
            write_timeout_secs: default_write_timeout_secs(),
            binary_encoding: BinaryEncoding::default(),
        }
    }

    /// Set the column mapping strategy.
    /// How strings bound to binary columns become bytes (#862 M-064).
    pub fn with_binary_encoding(mut self, encoding: BinaryEncoding) -> Self {
        self.binary_encoding = encoding;
        self
    }

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

impl MysqlSinkConfig {
    /// What a failed batch write leaves behind (#737): keyed writes run in one transaction; appends are autocommit statements.
    pub fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        if self.write.dedups_by_key() {
            faucet_core::BatchAtomicity::Atomic
        } else {
            faucet_core::BatchAtomicity::BestEffort
        }
    }
}

/// How a JSON string bound to a binary column becomes bytes (#862 M-064).
///
/// Every faucet source emits binary values as base64 text, but a string from
/// a text origin (a CSV token, a hex digest) can also happen to be valid
/// base64. `auto` cannot tell the two apart: it decodes any canonical, padded
/// base64 string and stores anything else as its text. The default, `base64`,
/// treats every value the same way and refuses one that is not base64; pick
/// `text` for a column fed plain text.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum BinaryEncoding {
    /// Decode a string that is canonical padded base64; store any other
    /// string as its text.
    Auto,
    /// Every string is base64 (line breaks ignored); one that is not fails the
    /// row instead of being stored as text. The default.
    #[default]
    Base64,
    /// Every string is stored as its text, never decoded.
    Text,
}

/// What a binary-column string resolves to under a [`BinaryEncoding`].
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum BinaryValue<'a> {
    /// Decoded bytes.
    Bytes(Vec<u8>),
    /// The string itself, stored as text.
    Text(&'a str),
}

impl BinaryEncoding {
    /// Resolve `s` for a binary column. `Err` only under `base64`, naming why
    /// the value is not base64.
    pub(crate) fn resolve(self, s: &str) -> Result<BinaryValue<'_>, String> {
        use base64::Engine as _;
        let engine = &base64::engine::general_purpose::STANDARD;
        match self {
            Self::Text => Ok(BinaryValue::Text(s)),
            Self::Auto => Ok(match engine.decode(s) {
                Ok(bytes) => BinaryValue::Bytes(bytes),
                Err(_) => BinaryValue::Text(s),
            }),
            Self::Base64 => {
                let unwrapped: String = s.chars().filter(|c| *c != '\n' && *c != '\r').collect();
                engine
                    .decode(unwrapped)
                    .map(BinaryValue::Bytes)
                    .map_err(|e| format!("binary_encoding: base64 — value is not base64: {e}"))
            }
        }
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

    #[test]
    fn batch_atomicity_matches_the_write_path() {
        let c: MysqlSinkConfig = serde_json::from_value(
            serde_json::json!({"connection_url": "mysql://localhost/test", "table_name": "events"}),
        )
        .unwrap();
        assert_eq!(c.batch_atomicity(), faucet_core::BatchAtomicity::BestEffort);
        let c: MysqlSinkConfig = serde_json::from_value(serde_json::json!({"connection_url": "mysql://localhost/test", "table_name": "events", "write_mode": "delete", "key": ["id"]})).unwrap();
        assert_eq!(c.batch_atomicity(), faucet_core::BatchAtomicity::Atomic);
    }
}

#[cfg(test)]
mod binary_encoding_tests {
    use super::*;

    #[test]
    fn each_mode_resolves_deterministically() {
        assert_eq!(
            BinaryEncoding::Auto.resolve("SGVsbG8="),
            Ok(BinaryValue::Bytes(b"Hello".to_vec()))
        );
        assert_eq!(
            BinaryEncoding::Auto.resolve("not base64!"),
            Ok(BinaryValue::Text("not base64!"))
        );
        assert_eq!(
            BinaryEncoding::Text.resolve("abcd"),
            Ok(BinaryValue::Text("abcd"))
        );
        assert_eq!(
            BinaryEncoding::Base64.resolve("SGVs\nbG8=\r\n"),
            Ok(BinaryValue::Bytes(b"Hello".to_vec()))
        );
        let err = BinaryEncoding::Base64.resolve("abc").unwrap_err();
        assert!(err.contains("not base64"), "{err}");
        assert_eq!(BinaryEncoding::default(), BinaryEncoding::Base64);
        let parsed: BinaryEncoding = serde_json::from_str("\"text\"").unwrap();
        assert_eq!(parsed, BinaryEncoding::Text);
    }
}
