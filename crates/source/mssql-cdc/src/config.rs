//! Configuration for [`MssqlCdcSource`](crate::MssqlCdcSource).

use std::fmt;
use std::time::Duration;

use faucet_common_mssql::MssqlConnectionConfig;
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
    5
}
fn default_statement_timeout_secs() -> u64 {
    300
}

/// Where to start reading changes on a fresh run (no persisted bookmark).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StartPosition {
    /// Start at the database's current maximum LSN — skip all pre-existing
    /// change history and only capture changes committed after the source
    /// starts. Default.
    #[default]
    Current,
    /// Start at the earliest LSN still retained by the CDC capture instance
    /// (`sys.fn_cdc_get_min_lsn`), replaying whatever history the cleanup job
    /// has not yet purged.
    Earliest,
}

/// What to do when a run resumes behind the capture instance's retained
/// minimum LSN — the CDC cleanup job purged changes before they were read.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OnGap {
    /// Fail the run with a typed error. Default: the purged changes are lost,
    /// and a destination that silently missed them is corrupt.
    #[default]
    Fail,
    /// Log a warning and resume from the earliest retained change, accepting
    /// the loss (e.g. after re-snapshotting the destination).
    Skip,
}

/// Configuration for the Microsoft SQL Server CDC source.
///
/// The source polls native SQL Server change data capture: `sys.fn_cdc_get_max_lsn()`
/// for the high-water LSN, then `cdc.fn_cdc_get_all_changes_<capture_instance>()`
/// per configured capture instance, advancing a durable per-instance LSN bookmark.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct MssqlCdcSourceConfig {
    /// Connection + TLS settings (`connection_url` or `connection_string`).
    #[serde(flatten)]
    pub connection: MssqlConnectionConfig,
    /// Capture instances to poll (e.g. `dbo_Orders`). **Required and non-empty.**
    ///
    /// A capture instance is created by `sys.sp_cdc_enable_table` and defaults to
    /// `<schema>_<table>`. Names are used verbatim to build the
    /// `cdc.fn_cdc_get_all_changes_<name>` table-valued function call, so v1 only
    /// accepts the SQL Server identifier characters `[A-Za-z0-9_]` (validated at
    /// load time) to prevent injection into the function name.
    pub capture_instances: Vec<String>,
    /// Start position on a fresh run (ignored once a bookmark exists). Default
    /// `current` (skip existing history).
    #[serde(default)]
    pub start_position: StartPosition,
    /// Seconds to wait between empty polls (no new changes). Default 1s.
    #[serde(
        default = "default_poll_interval",
        with = "faucet_core::config::duration_secs"
    )]
    #[schemars(with = "u64")]
    pub poll_interval: Duration,
    /// Terminator: end the fetch cycle after this much continuous quiet (no
    /// change rows across any capture instance). Default 30s. A long-running
    /// runtime (`faucet schedule` / `faucet serve`) re-invokes the source to
    /// keep tailing.
    #[serde(
        default = "default_idle_timeout",
        with = "faucet_core::config::duration_secs"
    )]
    #[schemars(with = "u64")]
    pub idle_timeout: Duration,
    /// Max records buffered for a single in-progress transaction before the run
    /// aborts with a typed error (rather than risking unbounded memory growth).
    /// `None` = unbounded.
    #[serde(default)]
    pub max_staged_records: Option<usize>,
    /// Advisory per-page record count. `0` accumulates every change into a
    /// single trailing page (snapshot/test convenience). Default
    /// [`DEFAULT_BATCH_SIZE`].
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Maximum pooled connections. Defaults to 5; raised to the number of
    /// capture instances, since a poll reads every instance's changes at once
    /// to merge them in commit order.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    /// Per-query timeout in seconds (`0` disables). Defaults to 300.
    #[serde(default = "default_statement_timeout_secs")]
    pub statement_timeout_secs: u64,
    /// Explicit state-store key for the LSN bookmark. When unset, a key is
    /// derived from the database (or host) and the sorted capture-instance list.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_key: Option<String>,
    /// What to do when the bookmark predates the retained change history
    /// (downtime longer than the CDC cleanup retention): `fail` (default) or
    /// `skip`.
    #[serde(default)]
    pub on_gap: OnGap,
}

impl MssqlCdcSourceConfig {
    /// Validate fail-fast invariants. Called from [`MssqlCdcSource::new`](crate::MssqlCdcSource::new).
    pub fn validate(&self) -> Result<(), FaucetError> {
        self.connection.validate()?;
        if self.capture_instances.is_empty() {
            return Err(FaucetError::Config(
                "mssql-cdc: `capture_instances` must list at least one CDC capture instance".into(),
            ));
        }
        for ci in &self.capture_instances {
            validate_capture_instance(ci)?;
        }
        // Duplicate capture instances would double-emit and fight over the same
        // bookmark map entry — reject them up front.
        let mut seen = std::collections::BTreeSet::new();
        for ci in &self.capture_instances {
            if !seen.insert(ci) {
                return Err(FaucetError::Config(format!(
                    "mssql-cdc: duplicate capture instance {ci:?} in `capture_instances`"
                )));
            }
        }
        if self.poll_interval.is_zero() {
            return Err(FaucetError::Config(
                "mssql-cdc: poll_interval must be > 0".into(),
            ));
        }
        if self.idle_timeout.is_zero() {
            return Err(FaucetError::Config(
                "mssql-cdc: idle_timeout must be > 0".into(),
            ));
        }
        validate_batch_size(self.batch_size)?;
        let key = self.resolved_state_key();
        faucet_core::state::validate_state_key(&key)?;
        Ok(())
    }

    /// The pool size: `max_connections`, at least one connection per capture
    /// instance.
    pub fn pool_size(&self) -> u32 {
        self.max_connections
            .max(u32::try_from(self.capture_instances.len()).unwrap_or(u32::MAX))
    }

    /// The state-store key for this source's LSN bookmark map. Uses the explicit
    /// `state_key` override when set, otherwise a derived, stable key.
    pub fn resolved_state_key(&self) -> String {
        self.state_key
            .clone()
            .unwrap_or_else(|| derive_state_key(self))
    }
}

/// Validate a capture-instance name: non-empty, ≤128 chars, and only the SQL
/// Server identifier characters `[A-Za-z0-9_]`. The name is interpolated into
/// the `cdc.fn_cdc_get_all_changes_<name>` function identifier (which cannot be
/// bracket-quoted mid-name), so this is the injection guard.
pub(crate) fn validate_capture_instance(ci: &str) -> Result<(), FaucetError> {
    if ci.is_empty() {
        return Err(FaucetError::Config(
            "mssql-cdc: capture instance name must not be empty".into(),
        ));
    }
    if ci.len() > 128 {
        return Err(FaucetError::Config(format!(
            "mssql-cdc: capture instance name {ci:?} exceeds 128 characters"
        )));
    }
    if !ci.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(FaucetError::Config(format!(
            "mssql-cdc: capture instance name {ci:?} may only contain letters, digits, and \
             underscores (SQL Server identifier characters); other characters are unsupported in v1"
        )));
    }
    Ok(())
}

/// Derive a stable state key from the target database (or connection host) plus
/// a fingerprint of the sorted capture-instance list. Pure so it is unit-testable.
///
/// - Single instance: `mssql-cdc:<db-or-host>:<capture_instance>` (readable).
/// - Multiple instances: `mssql-cdc:<db-or-host>:<fnv1a-hex>` (stable digest).
pub(crate) fn derive_state_key(config: &MssqlCdcSourceConfig) -> String {
    let scope = database_or_host(config);

    if config.capture_instances.len() == 1 {
        return format!("mssql-cdc:{scope}:{}", config.capture_instances[0]);
    }

    let mut sorted: Vec<&str> = config
        .capture_instances
        .iter()
        .map(String::as_str)
        .collect();
    sorted.sort_unstable();
    let digest = fnv1a_hex(&sorted.join(","));
    format!("mssql-cdc:{scope}:{digest}")
}

/// Extract the target database name (preferred) or connection host for the state
/// key scope, sanitising to key-safe characters. Falls back to `mssql`.
fn database_or_host(config: &MssqlCdcSourceConfig) -> String {
    let raw = config
        .connection
        .connection_url
        .as_deref()
        .and_then(|u| url::Url::parse(u).ok())
        .and_then(|u| {
            let db = u.path().trim_start_matches('/').to_string();
            if !db.is_empty() {
                Some(db)
            } else {
                u.host_str().map(str::to_string)
            }
        })
        .or_else(|| {
            config
                .connection
                .connection_string
                .as_deref()
                .and_then(database_from_ado_string)
        })
        .unwrap_or_else(|| "mssql".to_string());

    let sanitised: String = raw
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect();
    if sanitised.is_empty() {
        "mssql".to_string()
    } else {
        sanitised
    }
}

/// Pull the `Database=`/`Initial Catalog=` value out of an ADO.NET connection
/// string (case-insensitive key match). Pure.
fn database_from_ado_string(s: &str) -> Option<String> {
    for part in s.split(';') {
        let mut kv = part.splitn(2, '=');
        let key = kv.next()?.trim();
        let val = kv.next().unwrap_or("").trim();
        if key.eq_ignore_ascii_case("database") || key.eq_ignore_ascii_case("initial catalog") {
            if val.is_empty() {
                return None;
            }
            return Some(val.to_string());
        }
    }
    None
}

/// 64-bit FNV-1a of `s`, rendered as 16 lowercase hex digits. Deterministic and
/// dependency-free (used only for a stable state-key digest, not security).
fn fnv1a_hex(s: &str) -> String {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET;
    for b in s.as_bytes() {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(PRIME);
    }
    format!("{hash:016x}")
}

impl fmt::Debug for MssqlCdcSourceConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MssqlCdcSourceConfig")
            .field("connection", &"***")
            .field("capture_instances", &self.capture_instances)
            .field("start_position", &self.start_position)
            .field("poll_interval", &self.poll_interval)
            .field("idle_timeout", &self.idle_timeout)
            .field("max_staged_records", &self.max_staged_records)
            .field("batch_size", &self.batch_size)
            .field("max_connections", &self.max_connections)
            .field("statement_timeout_secs", &self.statement_timeout_secs)
            .field("state_key", &self.state_key)
            .field("on_gap", &self.on_gap)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn minimal() -> MssqlCdcSourceConfig {
        serde_json::from_value(json!({
            "connection_url": "mssql://sa:pw@localhost:1433/sales",
            "capture_instances": ["dbo_Orders"]
        }))
        .unwrap()
    }

    #[test]
    fn defaults_via_serde() {
        let c = minimal();
        assert_eq!(c.poll_interval.as_secs(), 1);
        assert_eq!(c.idle_timeout.as_secs(), 30);
        assert_eq!(c.batch_size, DEFAULT_BATCH_SIZE);
        assert_eq!(c.max_connections, 5);
        assert_eq!(c.statement_timeout_secs, 300);
        assert_eq!(c.start_position, StartPosition::Current);
        assert!(c.max_staged_records.is_none());
    }

    #[test]
    fn start_position_tagged_enum() {
        let c: MssqlCdcSourceConfig = serde_json::from_value(json!({
            "connection_url": "mssql://sa:pw@h/db",
            "capture_instances": ["dbo_t"],
            "start_position": { "type": "earliest" }
        }))
        .unwrap();
        assert_eq!(c.start_position, StartPosition::Earliest);
    }

    #[test]
    fn accepts_minimal() {
        assert!(minimal().validate().is_ok());
    }

    #[test]
    fn pool_has_a_connection_per_capture_instance() {
        let mut c: MssqlCdcSourceConfig = serde_json::from_value(json!({
            "connection_url": "mssql://sa:pw@h/db",
            "capture_instances": ["a", "b"]
        }))
        .unwrap();
        assert_eq!(c.pool_size(), 5);
        c.max_connections = 1;
        assert_eq!(c.pool_size(), 2);
    }

    #[test]
    fn rejects_empty_capture_instances() {
        let c: MssqlCdcSourceConfig = serde_json::from_value(json!({
            "connection_url": "mssql://sa:pw@h/db",
            "capture_instances": []
        }))
        .unwrap();
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_duplicate_capture_instances() {
        let mut c = minimal();
        c.capture_instances = vec!["dbo_t".into(), "dbo_t".into()];
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_injection_in_capture_instance() {
        assert!(validate_capture_instance("dbo_Orders").is_ok());
        assert!(validate_capture_instance("Orders123").is_ok());
        // Hostile names that would break out of the function identifier.
        assert!(validate_capture_instance("dbo.Orders").is_err());
        assert!(validate_capture_instance("x(1,2,'all')--").is_err());
        assert!(validate_capture_instance("a b").is_err());
        assert!(validate_capture_instance("").is_err());
        assert!(validate_capture_instance(&"a".repeat(129)).is_err());
    }

    #[test]
    fn rejects_zero_intervals() {
        let mut c = minimal();
        c.poll_interval = Duration::ZERO;
        assert!(c.validate().is_err());

        let mut c = minimal();
        c.idle_timeout = Duration::ZERO;
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_bad_batch_size() {
        let mut c = minimal();
        c.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        assert!(c.validate().is_err());
    }

    #[test]
    fn requires_a_connection_source() {
        let c: MssqlCdcSourceConfig = serde_json::from_value(json!({
            "capture_instances": ["dbo_t"]
        }))
        .unwrap();
        // No connection_url / connection_string -> connection.validate() fails.
        assert!(c.validate().is_err());
    }

    #[test]
    fn state_key_single_instance_is_readable() {
        let c = minimal();
        let key = c.resolved_state_key();
        assert_eq!(key, "mssql-cdc:sales:dbo_Orders");
        faucet_core::state::validate_state_key(&key).expect("derived key must be valid");
    }

    #[test]
    fn state_key_multi_instance_is_digest_and_order_independent() {
        let mut a = minimal();
        a.capture_instances = vec!["dbo_Orders".into(), "dbo_Items".into()];
        let mut b = minimal();
        b.capture_instances = vec!["dbo_Items".into(), "dbo_Orders".into()];
        let ka = a.resolved_state_key();
        let kb = b.resolved_state_key();
        assert_eq!(ka, kb, "sorted digest is order-independent");
        assert!(ka.starts_with("mssql-cdc:sales:"));
        assert!(!ka.ends_with("dbo_Orders"));
        faucet_core::state::validate_state_key(&ka).expect("digest key must be valid");
    }

    #[test]
    fn state_key_explicit_override_wins() {
        let mut c = minimal();
        c.state_key = Some("custom:key".into());
        assert_eq!(c.resolved_state_key(), "custom:key");
    }

    #[test]
    fn state_key_falls_back_to_host_then_mssql() {
        // connection_string with a database.
        let c: MssqlCdcSourceConfig = serde_json::from_value(json!({
            "connection_string": "Server=tcp:h,1433;Initial Catalog=warehouse;User Id=sa;Password=p",
            "capture_instances": ["dbo_t"]
        }))
        .unwrap();
        assert_eq!(c.resolved_state_key(), "mssql-cdc:warehouse:dbo_t");

        // connection_string without a database -> "mssql".
        let c: MssqlCdcSourceConfig = serde_json::from_value(json!({
            "connection_string": "Server=tcp:h,1433;User Id=sa;Password=p",
            "capture_instances": ["dbo_t"]
        }))
        .unwrap();
        assert_eq!(c.resolved_state_key(), "mssql-cdc:mssql:dbo_t");
    }

    #[test]
    fn debug_redacts_connection() {
        let c: MssqlCdcSourceConfig = serde_json::from_value(json!({
            "connection_url": "mssql://sa:secret@h/db",
            "capture_instances": ["dbo_t"]
        }))
        .unwrap();
        let dbg = format!("{c:?}");
        assert!(dbg.contains("***"));
        assert!(!dbg.contains("secret"));
    }
}
