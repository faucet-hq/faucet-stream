//! Configuration for `PostgresCdcSource`.

use faucet_core::{DEFAULT_BATCH_SIZE, FaucetError};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::time::Duration;

fn default_true() -> bool {
    true
}
fn default_proto_version() -> u32 {
    1
}
fn default_idle_timeout() -> Duration {
    Duration::from_secs(30)
}
fn default_status_update_interval() -> Duration {
    Duration::from_secs(10)
}
fn default_tcp_keepalive() -> Duration {
    Duration::from_secs(60)
}
fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}
fn default_slot_acquire_retries() -> u32 {
    10
}

/// Configuration for [`PostgresCdcSource`](crate::PostgresCdcSource).
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PostgresCdcSourceConfig {
    /// Connection URL pointing at the database whose WAL we want to read.
    /// The crate internally upgrades the connection to `replication=database`
    /// — callers do **not** need to add it themselves.
    pub connection_url: String,

    /// Logical replication slot name. Must match the Postgres naming rules:
    /// 1–63 chars, lowercase letters / digits / underscores only.
    pub slot_name: String,

    /// Publication name on the server. Must already exist (faucet does not
    /// create publications — they're a DBA-level concern that determines
    /// which tables are replicated).
    pub publication_name: String,

    /// If the slot does not exist, create it as a logical/`pgoutput` slot
    /// at connection time. Default: `true`.
    #[serde(default = "default_true")]
    pub create_slot_if_missing: bool,

    /// Whether a newly-created slot is `permanent` (survives disconnect) or
    /// `temporary` (auto-dropped when the replication connection closes).
    ///
    /// Default `permanent` (back-compatible). **A permanent slot pins WAL on
    /// the server until it is consumed or dropped** — an abandoned permanent
    /// slot fills `pg_wal` and can take the whole instance down. Use
    /// `temporary` for ephemeral / test runs (note: a temporary slot resets on
    /// reconnect, so bookmark-based resume across runs requires a permanent
    /// slot). Drop an unused permanent slot explicitly with
    /// [`PostgresCdcSource::drop_slot`](crate::PostgresCdcSource::drop_slot).
    #[serde(default)]
    pub slot_type: SlotType,

    /// Number of times to retry acquiring the replication slot when the server
    /// reports it is still **active** (held by a not-yet-released prior
    /// connection). On a rapid restart — a scheduler or `serve` re-running the
    /// pipeline before the previous backend has dropped the slot — both the
    /// pre-stream `pg_replication_slot_advance` and `START_REPLICATION` fail
    /// with *"replication slot … is active for PID …"*. Each retry waits an
    /// exponentially increasing backoff (250 ms, doubling, capped at 4 s).
    /// `0` disables retries (fail fast). Defaults to 10.
    #[serde(default = "default_slot_acquire_retries")]
    pub slot_acquire_retries: u32,

    /// TLS settings for the replication connection. Default `disable`
    /// (plaintext) for back-compatibility, but credentials and all WAL data
    /// then travel unencrypted — set `require`/`verify_ca`/`verify_full` in
    /// production.
    #[serde(default)]
    pub tls: CdcTls,

    /// Optional starting LSN override (e.g. `"0/16A4F88"`). Ignored when a
    /// state-store-managed bookmark is present (that bookmark wins).
    /// When neither is set, replication starts from the slot's
    /// `confirmed_flush_lsn`.
    #[serde(default)]
    pub start_lsn: Option<String>,

    /// pgoutput protocol version. Only `1` is fully exercised in v1; `2` is
    /// accepted but streaming-transaction messages (S/E/c/A) are not yet
    /// decoded. Default: `1`.
    #[serde(default = "default_proto_version")]
    pub proto_version: u32,

    /// Maximum time to wait for new replication messages before returning
    /// the current batch. Default: 30 s.
    #[serde(
        default = "default_idle_timeout",
        with = "faucet_core::config::duration_secs"
    )]
    #[schemars(with = "u64")]
    pub idle_timeout: Duration,

    /// Optional cap on the number of change events drained per fetch call.
    /// Acts as a safety bound — `idle_timeout` is the primary terminator.
    ///
    /// **Note:** the cap is checked **after each COMMIT**, never mid-
    /// transaction. A single transaction larger than `max_messages` will
    /// still be emitted atomically (the fetch returns only after that
    /// transaction's COMMIT and may produce more records than `max_messages`).
    /// To bound the memory a *single* in-progress transaction can consume,
    /// use [`max_staged_records`](Self::max_staged_records) instead.
    #[serde(default)]
    pub max_messages: Option<usize>,

    /// Maximum number of change records buffered in memory for a *single*
    /// in-progress transaction before it is aborted.
    ///
    /// Logical replication requires a transaction to be buffered until its
    /// COMMIT so it can be emitted atomically (partial transactions must
    /// never leak downstream). A single bulk `UPDATE`/`DELETE`/`COPY` of
    /// millions of rows therefore buffers every decoded row as a
    /// `serde_json::Value` in RAM, which can OOM the process. This bound is a
    /// safety valve: when an in-progress transaction's staged record count
    /// exceeds it, the source aborts with a typed
    /// [`FaucetError::Source`] rather than
    /// being OOM-killed.
    ///
    /// `None` (the default) means unbounded — atomic delivery of arbitrarily
    /// large transactions at the cost of unbounded memory. Set a value sized
    /// to your available memory if you replicate tables subject to large
    /// bulk writes.
    #[serde(default)]
    pub max_staged_records: Option<usize>,

    /// Interval at which Standby Status Update keepalives are sent to the
    /// server. Must be shorter than `idle_timeout` and well under the
    /// server's `wal_sender_timeout` (default 60 s). Default: 10 s.
    #[serde(
        default = "default_status_update_interval",
        with = "faucet_core::config::duration_secs"
    )]
    #[schemars(with = "u64")]
    pub status_update_interval: Duration,

    /// TCP keepalive for the replication connection. Default: 60 s.
    #[serde(
        default = "default_tcp_keepalive",
        with = "faucet_core::config::duration_secs"
    )]
    #[schemars(with = "u64")]
    pub tcp_keepalive: Duration,

    /// Advisory page size for
    /// [`Source::stream_pages`](faucet_core::Source::stream_pages). The CDC
    /// source emits **one `StreamPage` per committed transaction** so the
    /// pipeline gets per-transaction durability via its per-page bookmark
    /// persist. Because transactions are atomic units they are never split
    /// across pages — a single transaction whose record count exceeds
    /// `batch_size` still emits as one page. Defaults to
    /// [`DEFAULT_BATCH_SIZE`].
    ///
    /// `batch_size = 0` is the "no batching" sentinel: every committed
    /// transaction during the run window is accumulated into a single page
    /// that is emitted at the end with `bookmark = max(commit_lsn)`. This
    /// negates per-transaction durability and is only useful for tests or
    /// initial-snapshot style runs.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

/// Lifetime of a newly-created replication slot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SlotType {
    /// Survives disconnect; pins WAL until consumed or dropped. Default.
    #[default]
    Permanent,
    /// Auto-dropped by the server when the replication connection closes.
    Temporary,
}

/// TLS configuration for the CDC replication connection.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum CdcTls {
    /// No TLS — plaintext (default, back-compatible).
    #[default]
    Disable,
    /// Require TLS but do not verify the server certificate.
    Require,
    /// Require TLS and verify the certificate chain against `ca_path` (or the
    /// system roots when `None`).
    VerifyCa {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ca_path: Option<String>,
    },
    /// Require TLS and verify both the certificate chain and the hostname.
    VerifyFull {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ca_path: Option<String>,
    },
}

impl std::fmt::Debug for PostgresCdcSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresCdcSourceConfig")
            .field("connection_url", &"***")
            .field("slot_name", &self.slot_name)
            .field("publication_name", &self.publication_name)
            .field("create_slot_if_missing", &self.create_slot_if_missing)
            .field("slot_type", &self.slot_type)
            .field("tls", &self.tls)
            .field("start_lsn", &self.start_lsn)
            .field("proto_version", &self.proto_version)
            .field("idle_timeout", &self.idle_timeout)
            .field("max_messages", &self.max_messages)
            .field("max_staged_records", &self.max_staged_records)
            .field("status_update_interval", &self.status_update_interval)
            .field("tcp_keepalive", &self.tcp_keepalive)
            .field("batch_size", &self.batch_size)
            .field("slot_acquire_retries", &self.slot_acquire_retries)
            .finish()
    }
}

impl PostgresCdcSourceConfig {
    /// Override the advisory per-page record count emitted by
    /// [`Source::stream_pages`](faucet_core::Source::stream_pages).
    ///
    /// Pass `0` to disable per-transaction emission — every transaction in
    /// the run window will be accumulated into a single trailing page with
    /// `bookmark = max(commit_lsn)`. Transactions are never split regardless
    /// of `batch_size`.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Validate fail-fast invariants. Called from `PostgresCdcSource::new`.
    pub fn validate(&self) -> Result<(), FaucetError> {
        if self.connection_url.trim().is_empty() {
            return Err(FaucetError::Config(
                "postgres-cdc: connection_url must not be empty".into(),
            ));
        }
        validate_slot_name(&self.slot_name)?;
        if self.publication_name.is_empty() {
            return Err(FaucetError::Config(
                "postgres-cdc: publication_name must not be empty".into(),
            ));
        }
        if self.proto_version != 1 {
            return Err(FaucetError::Config(format!(
                "postgres-cdc: proto_version must be 1 (v2 streaming-transaction \
                 support is not yet available via pgwire-replication), got {}",
                self.proto_version
            )));
        }
        if self.idle_timeout.is_zero() {
            return Err(FaucetError::Config(
                "postgres-cdc: idle_timeout must be > 0".into(),
            ));
        }
        if self.status_update_interval >= self.idle_timeout {
            return Err(FaucetError::Config(format!(
                "postgres-cdc: status_update_interval ({}s) must be \
                 strictly less than idle_timeout ({}s)",
                self.status_update_interval.as_secs(),
                self.idle_timeout.as_secs()
            )));
        }
        Ok(())
    }
}

fn validate_slot_name(name: &str) -> Result<(), FaucetError> {
    if name.is_empty() {
        return Err(FaucetError::Config(
            "postgres-cdc: slot_name must not be empty".into(),
        ));
    }
    if name.len() > 63 {
        return Err(FaucetError::Config(format!(
            "postgres-cdc: slot_name '{name}' exceeds Postgres' 63-char limit"
        )));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
    {
        return Err(FaucetError::Config(format!(
            "postgres-cdc: slot_name '{name}' must contain only \
             [a-z0-9_]"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal() -> PostgresCdcSourceConfig {
        PostgresCdcSourceConfig {
            connection_url: "postgres://u:p@localhost/db".into(),
            slot_name: "faucet_slot".into(),
            publication_name: "faucet_pub".into(),
            create_slot_if_missing: true,
            slot_type: SlotType::Permanent,
            tls: CdcTls::Disable,
            start_lsn: None,
            proto_version: 1,
            idle_timeout: std::time::Duration::from_secs(30),
            max_messages: None,
            max_staged_records: None,
            status_update_interval: std::time::Duration::from_secs(10),
            tcp_keepalive: std::time::Duration::from_secs(60),
            batch_size: DEFAULT_BATCH_SIZE,
            slot_acquire_retries: default_slot_acquire_retries(),
        }
    }

    #[test]
    fn defaults_via_serde() {
        let value: PostgresCdcSourceConfig = serde_json::from_value(serde_json::json!({
            "connection_url": "postgres://u:p@localhost/db",
            "slot_name": "faucet_slot",
            "publication_name": "faucet_pub",
        }))
        .unwrap();
        assert!(value.create_slot_if_missing);
        assert_eq!(value.proto_version, 1);
        assert_eq!(value.idle_timeout.as_secs(), 30);
        assert_eq!(value.status_update_interval.as_secs(), 10);
        assert_eq!(value.tcp_keepalive.as_secs(), 60);
        assert!(value.start_lsn.is_none());
        assert!(value.max_messages.is_none());
        assert_eq!(value.batch_size, DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn batch_size_defaults_to_default_batch_size() {
        let c = minimal();
        assert_eq!(c.batch_size, DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let c = minimal().with_batch_size(64);
        assert_eq!(c.batch_size, 64);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let c = minimal().with_batch_size(0);
        assert_eq!(c.batch_size, 0);
        assert!(faucet_core::validate_batch_size(c.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let c = minimal().with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(c.batch_size).is_err());
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let v: PostgresCdcSourceConfig = serde_json::from_value(serde_json::json!({
            "connection_url": "postgres://u:p@localhost/db",
            "slot_name": "faucet_slot",
            "publication_name": "faucet_pub",
            "batch_size": 256,
        }))
        .unwrap();
        assert_eq!(v.batch_size, 256);
    }

    #[test]
    fn rejects_empty_slot_name() {
        let mut c = minimal();
        c.slot_name = String::new();
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_invalid_slot_name_chars() {
        let mut c = minimal();
        c.slot_name = "Faucet-Slot".into(); // uppercase + dash both disallowed
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_slot_name_over_63_chars() {
        let mut c = minimal();
        c.slot_name = "a".repeat(64);
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_empty_publication_name() {
        let mut c = minimal();
        c.publication_name = String::new();
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_zero_idle_timeout() {
        let mut c = minimal();
        c.idle_timeout = std::time::Duration::from_secs(0);
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_status_update_interval_longer_than_idle_timeout() {
        // Keepalives must fire before idle_timeout would terminate the loop.
        let mut c = minimal();
        c.status_update_interval = std::time::Duration::from_secs(60);
        c.idle_timeout = std::time::Duration::from_secs(30);
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_invalid_proto_version() {
        // 0, 2, and 3 are all rejected — only 1 is supported.
        let mut c = minimal();
        c.proto_version = 0;
        assert!(c.validate().is_err());
        c.proto_version = 2;
        assert!(c.validate().is_err());
        c.proto_version = 3;
        assert!(c.validate().is_err());
    }

    #[test]
    fn accepts_proto_version_one() {
        let mut c = minimal();
        c.proto_version = 1;
        assert!(c.validate().is_ok());
    }

    #[test]
    fn rejects_empty_connection_url() {
        let mut c = minimal();
        c.connection_url = String::new();
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_whitespace_connection_url() {
        let mut c = minimal();
        c.connection_url = "   ".into();
        assert!(c.validate().is_err());
    }

    #[test]
    fn debug_redacts_connection_url() {
        let cfg = minimal();
        let dbg = format!("{cfg:?}");
        assert!(dbg.contains("connection_url: \"***\""));
        assert!(!dbg.contains("u:p@localhost"));
    }

    #[test]
    fn schema_for_config_includes_required_fields() {
        let schema = schemars::schema_for!(PostgresCdcSourceConfig);
        let json = serde_json::to_value(&schema).unwrap();
        let required = json["required"].as_array().expect("required array");
        let names: Vec<_> = required.iter().filter_map(|v| v.as_str()).collect();
        assert!(names.contains(&"connection_url"));
        assert!(names.contains(&"slot_name"));
        assert!(names.contains(&"publication_name"));
    }
}
