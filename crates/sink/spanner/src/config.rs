//! Cloud Spanner sink configuration.

use faucet_common_spanner::SpannerConnection;
use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Configuration for the Cloud Spanner sink.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SpannerSinkConfig {
    /// Connection block (`project_id` / `instance` / `database` / `auth` /
    /// `max_sessions` / `emulator_host`), flattened to the config top level.
    #[serde(flatten)]
    pub connection: SpannerConnection,
    /// Target table name.
    pub table_name: String,
    /// Create the target table if it does not exist, inferring the columns
    /// from the first written page (#580). Enabled by default: a first-ever
    /// sync cannot assume the destination already exists.
    ///
    /// **Spanner needs a primary key on every table**, and faucet has no basis
    /// to invent one — so auto-create applies only when `key:` is set (which
    /// `write_mode: upsert`/`delete` already require, and which Spanner
    /// requires to equal the PK). Without a `key`, a missing table is an error
    /// naming that requirement rather than a table with a made-up key column.
    ///
    /// Inferred columns are `STRING(MAX)` apart from the key columns, because
    /// the writer serialises values through the shared JSON→mutation encoder;
    /// define the table yourself and set `create_table: false` for typed
    /// columns.
    #[serde(default = "default_create_table")]
    pub create_table: bool,
    /// Maximum rows per Spanner commit. Defaults to [`DEFAULT_BATCH_SIZE`].
    ///
    /// Each chunk is applied as one atomic commit. Spanner additionally caps
    /// a commit at ~80,000 mutated *cells* (rows × columns, plus secondary
    /// index amplification), so the sink also splits chunks to stay under a
    /// conservative 60,000-cell budget regardless of `batch_size`.
    ///
    /// `batch_size = 0` is the "no batching" sentinel: the entire upstream
    /// page is written in as few commits as the cell budget allows.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Upper bound (seconds) on waiting for a schema-change operation
    /// (`ALTER TABLE …` long-running operation) to complete. Defaults to 300.
    #[serde(default = "default_ddl_timeout_secs")]
    pub ddl_timeout_secs: u64,
    /// Write mode, key columns, and optional delete marker. `write_mode`
    /// defaults to `append`. Upsert/delete require `key` to equal the target
    /// table's PRIMARY KEY columns (Spanner mutations always key on the PK).
    #[serde(flatten)]
    pub write: faucet_core::WriteSpec,
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

fn default_ddl_timeout_secs() -> u64 {
    300
}

fn default_create_table() -> bool {
    true
}

impl SpannerSinkConfig {
    /// Create a new config with required fields and sensible defaults.
    pub fn new(
        project_id: impl Into<String>,
        instance: impl Into<String>,
        database: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Self {
        Self {
            connection: SpannerConnection {
                project_id: project_id.into(),
                instance: instance.into(),
                database: database.into(),
                auth: Default::default(),
                max_sessions: 100,
                emulator_host: None,
            },
            table_name: table_name.into(),
            create_table: default_create_table(),
            batch_size: DEFAULT_BATCH_SIZE,
            ddl_timeout_secs: default_ddl_timeout_secs(),
            write: faucet_core::WriteSpec::default(),
        }
    }

    /// Opt out of auto-creating a missing target table (#580).
    pub fn with_create_table(mut self, create: bool) -> Self {
        self.create_table = create;
        self
    }

    /// Set the per-commit row count. Pass `0` to opt out of row-count
    /// re-chunking (the ~60,000-cell budget still applies).
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the emulator endpoint (`host:port`), for tests and local runs.
    pub fn with_emulator_host(mut self, host: impl Into<String>) -> Self {
        self.connection.emulator_host = Some(host.into());
        self
    }

    /// Set the bound on schema-change LRO waits.
    pub fn with_ddl_timeout_secs(mut self, secs: u64) -> Self {
        self.ddl_timeout_secs = secs;
        self
    }

    /// Validate the config at load time: connection block, batch size, and
    /// write spec (`key` non-empty for upsert/delete).
    pub fn validate(&self) -> Result<(), faucet_core::FaucetError> {
        self.connection.validate()?;
        faucet_core::validate_batch_size(self.batch_size)?;
        if self.table_name.trim().is_empty() {
            return Err(faucet_core::FaucetError::Config(
                "spanner sink: `table_name` must not be empty".into(),
            ));
        }
        self.write.validate()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::{WriteMode, WriteSpec};
    use serde_json::json;

    fn base() -> SpannerSinkConfig {
        SpannerSinkConfig::new("p", "i", "d", "events")
    }

    #[test]
    fn default_config() {
        let config = base();
        assert_eq!(config.table_name, "events");
        assert_eq!(config.batch_size, DEFAULT_BATCH_SIZE);
        assert_eq!(config.ddl_timeout_secs, 300);
        assert_eq!(config.connection.max_sessions, 100);
        assert!(matches!(config.write.write_mode, WriteMode::Append));
        assert!(config.validate().is_ok());
    }

    #[test]
    fn builder_methods() {
        let config = base()
            .with_batch_size(50)
            .with_emulator_host("localhost:9010")
            .with_ddl_timeout_secs(10);
        assert_eq!(config.batch_size, 50);
        assert_eq!(
            config.connection.emulator_host.as_deref(),
            Some("localhost:9010")
        );
        assert_eq!(config.ddl_timeout_secs, 10);
    }

    #[test]
    fn deserializes_flattened_connection_and_write_spec() {
        let config: SpannerSinkConfig = serde_json::from_value(json!({
            "project_id": "p",
            "instance": "i",
            "database": "d",
            "table_name": "t",
            "write_mode": "upsert",
            "key": ["id"]
        }))
        .unwrap();
        assert_eq!(config.connection.project_id, "p");
        assert!(matches!(config.write.write_mode, WriteMode::Upsert));
        assert_eq!(config.write.key, vec!["id".to_string()]);
        assert_eq!(config.batch_size, DEFAULT_BATCH_SIZE);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn validate_rejects_empty_table_name() {
        let mut config = base();
        config.table_name = "  ".into();
        assert!(config.validate().is_err());
    }

    #[test]
    fn validate_rejects_upsert_without_key() {
        let mut config = base();
        config.write = WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec![],
            delete_marker: None,
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn validate_rejects_oversized_batch() {
        let config = base().with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(config.validate().is_err());
    }

    #[test]
    fn batch_size_zero_is_accepted_as_sentinel() {
        let config = base().with_batch_size(0);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn debug_does_not_leak_inline_key() {
        let mut config = base();
        config.connection.auth = faucet_common_spanner::SpannerCredentials::ServiceAccountKey {
            json: "TOPSECRET".into(),
        };
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("TOPSECRET"));
    }
}
