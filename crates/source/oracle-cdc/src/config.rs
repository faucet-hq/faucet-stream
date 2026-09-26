//! Configuration for [`OracleCdcSource`](crate::OracleCdcSource).

use std::time::Duration;

use faucet_common_oracle::{OracleConnectionConfig, quote_ident_oracle, split_table};
use faucet_core::{DEFAULT_BATCH_SIZE, FaucetError, validate_batch_size};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

fn default_poll_interval() -> Duration {
    Duration::from_secs(1)
}
fn default_idle_timeout() -> Duration {
    Duration::from_secs(30)
}
fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}
fn default_max_connections() -> u32 {
    2
}
fn default_statement_timeout_secs() -> u64 {
    600
}
fn default_max_scn_window() -> u64 {
    500_000
}
fn default_flush_table() -> String {
    "FAUCET_LOGMNR_FLUSH".into()
}

/// Where to start mining on a fresh run (no persisted bookmark).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StartPosition {
    /// The database's current SCN — only changes committed from now on
    /// (transactions already open are captured from their start). Default.
    #[default]
    Current,
    /// The oldest SCN still present in the available redo (online + archived).
    Earliest,
}

/// What to do with a change LogMiner cannot render as SQL for a tracked table
/// (`UNSUPPORTED` rows, or a *dictionary mismatch* after the table's structure
/// changed since the redo was written).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OnUnsupported {
    /// Fail the run (default) — never silently drop a change.
    #[default]
    Fail,
    /// Log a warning and skip the change.
    Skip,
}

/// Configuration for the Oracle LogMiner CDC source.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct OracleCdcSourceConfig {
    /// Connection settings. Connect to the PDB that owns the tables (Oracle
    /// 21c+ mines per PDB), or to a non-CDB / the CDB root.
    #[serde(flatten)]
    pub connection: OracleConnectionConfig,
    /// Tables to capture as `OWNER.TABLE`, in data-dictionary case (usually
    /// upper case). **Required and non-empty.**
    pub tables: Vec<String>,
    /// Start position on a fresh run. Default `current`.
    #[serde(default)]
    pub start_position: StartPosition,
    /// Seconds to wait between polls that find nothing new. Default 1s.
    #[serde(
        default = "default_poll_interval",
        with = "faucet_core::config::duration_secs"
    )]
    #[schemars(with = "u64")]
    pub poll_interval: Duration,
    /// End the fetch cycle after this long without a captured change. Default
    /// 30s. A long-running runtime re-invokes the source to keep tailing.
    #[serde(
        default = "default_idle_timeout",
        with = "faucet_core::config::duration_secs"
    )]
    #[schemars(with = "u64")]
    pub idle_timeout: Duration,
    /// Largest SCN range mined per LogMiner session. Default 500000.
    #[serde(default = "default_max_scn_window")]
    pub max_scn_window: u64,
    /// Abort when one in-flight transaction buffers more than this many
    /// changes (bounded memory). `None` = unbounded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_staged_records: Option<usize>,
    /// `0` accumulates every change into one trailing page; otherwise each
    /// committed transaction is its own page. Default [`DEFAULT_BATCH_SIZE`].
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Maximum pooled sessions. Default 2.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    /// Per-round-trip call timeout in seconds (`0` disables). Default 600.
    #[serde(default = "default_statement_timeout_secs")]
    pub statement_timeout_secs: u64,
    /// Table the source commits to before each mining window, forcing the log
    /// writer to flush redo up to the window's end SCN. Created in the
    /// connecting user's schema when missing. Default `FAUCET_LOGMNR_FLUSH`.
    #[serde(default = "default_flush_table")]
    pub flush_table: String,
    /// Handling of changes LogMiner cannot render. Default `fail`.
    #[serde(default)]
    pub on_unsupported: OnUnsupported,
    /// Explicit state-store key. Derived as `oracle-cdc:<database>:<table>`
    /// (or a digest of several tables) when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_key: Option<String>,
}

impl std::fmt::Debug for OracleCdcSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OracleCdcSourceConfig")
            .field("connection", &self.connection)
            .field("tables", &self.tables)
            .field("start_position", &self.start_position)
            .field("poll_interval", &self.poll_interval)
            .field("idle_timeout", &self.idle_timeout)
            .field("max_scn_window", &self.max_scn_window)
            .field("max_staged_records", &self.max_staged_records)
            .field("batch_size", &self.batch_size)
            .field("max_connections", &self.max_connections)
            .field("statement_timeout_secs", &self.statement_timeout_secs)
            .field("flush_table", &self.flush_table)
            .field("on_unsupported", &self.on_unsupported)
            .field("state_key", &self.state_key)
            .finish()
    }
}

impl OracleCdcSourceConfig {
    /// A config capturing `tables` with defaults elsewhere.
    pub fn new(connection: OracleConnectionConfig, tables: Vec<String>) -> Self {
        Self {
            connection,
            tables,
            start_position: StartPosition::default(),
            poll_interval: default_poll_interval(),
            idle_timeout: default_idle_timeout(),
            max_scn_window: default_max_scn_window(),
            max_staged_records: None,
            batch_size: default_batch_size(),
            max_connections: default_max_connections(),
            statement_timeout_secs: default_statement_timeout_secs(),
            flush_table: default_flush_table(),
            on_unsupported: OnUnsupported::default(),
            state_key: None,
        }
    }

    /// Validate the fail-fast invariants.
    pub fn validate(&self) -> Result<(), FaucetError> {
        self.connection.validate()?;
        if self.tables.is_empty() {
            return Err(FaucetError::Config(
                "oracle-cdc: `tables` must list at least one OWNER.TABLE".into(),
            ));
        }
        let mut seen = std::collections::BTreeSet::new();
        for t in &self.tables {
            let (owner, name) = split_table(t)?;
            let Some(owner) = owner else {
                return Err(FaucetError::Config(format!(
                    "oracle-cdc: table {t:?} must be owner-qualified (OWNER.TABLE)"
                )));
            };
            quote_ident_oracle(&owner)?;
            quote_ident_oracle(&name)?;
            if !seen.insert(t) {
                return Err(FaucetError::Config(format!(
                    "oracle-cdc: duplicate table {t:?} in `tables`"
                )));
            }
        }
        quote_ident_oracle(&self.flush_table)?;
        if self.poll_interval.is_zero() || self.idle_timeout.is_zero() {
            return Err(FaucetError::Config(
                "oracle-cdc: poll_interval and idle_timeout must be > 0".into(),
            ));
        }
        if self.max_scn_window == 0 {
            return Err(FaucetError::Config(
                "oracle-cdc: max_scn_window must be > 0".into(),
            ));
        }
        validate_batch_size(self.batch_size)?;
        faucet_core::state::validate_state_key(&self.resolved_state_key())?;
        Ok(())
    }

    /// `(owner, table)` pairs of the captured tables.
    pub fn table_pairs(&self) -> Vec<(String, String)> {
        self.tables
            .iter()
            .filter_map(|t| match split_table(t) {
                Ok((Some(o), n)) => Some((o, n)),
                _ => None,
            })
            .collect()
    }

    /// The state-store key for the SCN bookmark.
    pub fn resolved_state_key(&self) -> String {
        if let Some(k) = &self.state_key {
            return k.clone();
        }
        let scope = self.connection.scope_label();
        let scope = if scope.is_empty() {
            "oracle".into()
        } else {
            scope
        };
        if self.tables.len() == 1 {
            let t: String = self.tables[0]
                .chars()
                .map(|c| {
                    if c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.') {
                        c
                    } else {
                        '_'
                    }
                })
                .collect();
            return format!("oracle-cdc:{scope}:{t}");
        }
        let mut sorted: Vec<&str> = self.tables.iter().map(String::as_str).collect();
        sorted.sort_unstable();
        format!("oracle-cdc:{scope}:{}", fnv1a_hex(&sorted.join(",")))
    }
}

fn fnv1a_hex(s: &str) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.bytes() {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn conn() -> OracleConnectionConfig {
        OracleConnectionConfig::new("h", 1521, "FREEPDB1", "u", "p")
    }

    #[test]
    fn serde_defaults() {
        let cfg: OracleCdcSourceConfig = serde_json::from_value(json!({
            "host": "h", "service_name": "FREEPDB1", "username": "u", "password": "p",
            "tables": ["APP.ORDERS"]
        }))
        .unwrap();
        assert_eq!(cfg.start_position, StartPosition::Current);
        assert_eq!(cfg.poll_interval, Duration::from_secs(1));
        assert_eq!(cfg.idle_timeout, Duration::from_secs(30));
        assert_eq!(cfg.max_scn_window, 500_000);
        assert_eq!(cfg.flush_table, "FAUCET_LOGMNR_FLUSH");
        assert_eq!(cfg.on_unsupported, OnUnsupported::Fail);
        cfg.validate().unwrap();
        assert_eq!(cfg.table_pairs(), vec![("APP".into(), "ORDERS".into())]);
        let s: StartPosition = serde_json::from_value(json!({"type": "earliest"})).unwrap();
        assert_eq!(s, StartPosition::Earliest);
    }

    #[test]
    fn validate_rejects_bad_configs() {
        let mut cfg = OracleCdcSourceConfig::new(conn(), vec![]);
        assert!(cfg.validate().is_err());
        cfg.tables = vec!["ORDERS".into()];
        assert!(cfg.validate().is_err(), "owner required");
        cfg.tables = vec!["A.B".into(), "A.B".into()];
        assert!(cfg.validate().is_err(), "duplicates");
        cfg.tables = vec!["A\".B".into()];
        assert!(cfg.validate().is_err());
        cfg.tables = vec!["A.B\"".into()];
        assert!(cfg.validate().is_err());
        cfg.tables = vec!["A.B".into()];
        cfg.flush_table = "x\"".into();
        assert!(cfg.validate().is_err());
        cfg.flush_table = "F".into();
        cfg.poll_interval = Duration::ZERO;
        assert!(cfg.validate().is_err());
        cfg.poll_interval = Duration::from_secs(1);
        cfg.max_scn_window = 0;
        assert!(cfg.validate().is_err());
        cfg.max_scn_window = 1;
        cfg.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        assert!(cfg.validate().is_err());
        cfg.batch_size = 10;
        cfg.validate().unwrap();
    }

    #[test]
    fn state_keys() {
        let one = OracleCdcSourceConfig::new(conn(), vec!["APP.ORDERS".into()]);
        assert_eq!(one.resolved_state_key(), "oracle-cdc:FREEPDB1:APP.ORDERS");
        let a = OracleCdcSourceConfig::new(conn(), vec!["A.X".into(), "A.Y".into()]);
        let b = OracleCdcSourceConfig::new(conn(), vec!["A.Y".into(), "A.X".into()]);
        assert_eq!(a.resolved_state_key(), b.resolved_state_key());
        assert!(a.resolved_state_key().starts_with("oracle-cdc:FREEPDB1:"));
        let explicit = OracleCdcSourceConfig {
            state_key: Some("mine".into()),
            ..one
        };
        assert_eq!(explicit.resolved_state_key(), "mine");
        let odd = OracleCdcSourceConfig::new(conn(), vec!["A.T$1".into()]);
        faucet_core::state::validate_state_key(&odd.resolved_state_key()).unwrap();
    }

    #[test]
    fn debug_masks_password() {
        let cfg = OracleCdcSourceConfig::new(
            OracleConnectionConfig::new("h", 1, "S", "u", "pw123"),
            vec![],
        );
        assert!(!format!("{cfg:?}").contains("pw123"));
    }
}
