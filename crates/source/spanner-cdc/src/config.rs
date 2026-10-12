//! Configuration for [`SpannerCdcSource`](crate::SpannerCdcSource).

use chrono::{DateTime, Utc};
use faucet_common_spanner::SpannerConnection;
use faucet_core::{DEFAULT_BATCH_SIZE, FaucetError};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Smallest accepted `heartbeat_ms` (Spanner's own lower bound).
pub const MIN_HEARTBEAT_MS: u32 = 1_000;
/// Largest accepted `heartbeat_ms` (Spanner's own upper bound).
pub const MAX_HEARTBEAT_MS: u32 = 300_000;
/// Upper bound for `max_concurrent_partitions`. Every reader holds one pooled
/// Spanner session for the length of its query.
pub const MAX_CONCURRENT_PARTITIONS: usize = 64;

fn default_heartbeat_ms() -> u32 {
    10_000
}
fn default_idle_timeout() -> Duration {
    Duration::from_secs(60)
}
fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}
fn default_max_concurrent_partitions() -> usize {
    4
}

/// Where to start reading on a fresh run (no persisted bookmark).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StartFrom {
    /// Start at the current time: history is not replayed (default).
    #[default]
    Now,
    /// Start at an RFC 3339 timestamp. Must be inside the change stream's
    /// retention period and not before the stream was created.
    Timestamp {
        /// RFC 3339 timestamp, e.g. `2026-01-31T10:00:00Z`.
        timestamp: String,
    },
}

/// Configuration for [`SpannerCdcSource`](crate::SpannerCdcSource).
///
/// The connection block (`project_id`, `instance`, `database`, `auth`,
/// `max_sessions`, `emulator_host`) is flattened in from the shared
/// [`SpannerConnection`].
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct SpannerCdcSourceConfig {
    /// Spanner connection (shared with the Spanner query source and sink).
    #[serde(flatten)]
    pub connection: SpannerConnection,
    /// Name of the change stream to read (`CREATE CHANGE STREAM <name> ...`).
    pub change_stream: String,
    /// Start position on a fresh run; ignored once a bookmark exists.
    #[serde(default)]
    pub start_from: StartFrom,
    /// How often Spanner emits a heartbeat on a quiet partition, in
    /// milliseconds (1000-300000). A bounded run ends about one heartbeat after
    /// the run's end time, so a smaller value finishes quiet runs sooner.
    /// Default 10000.
    #[serde(default = "default_heartbeat_ms")]
    pub heartbeat_ms: u32,
    /// Give up on one partition read that yields nothing (not even a
    /// heartbeat) for this long. The partition keeps its progress and is
    /// resumed on the next run. Must exceed `heartbeat_ms`. Default 60s.
    #[serde(
        default = "default_idle_timeout",
        with = "faucet_core::config::duration_secs"
    )]
    #[schemars(with = "u64")]
    pub idle_timeout: Duration,
    /// Records per emitted `StreamPage`. `0` = drain the run into one page.
    /// A page is only cut between source transactions.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Partitions read in parallel (1-64). Each holds one pooled session, so
    /// keep it well below `max_sessions`. Default 4.
    #[serde(default = "default_max_concurrent_partitions")]
    pub max_concurrent_partitions: usize,
    /// Max change records buffered in memory before the run aborts with a
    /// typed error. `None` = unbounded. The OOM safety valve for
    /// `batch_size: 0`.
    #[serde(default)]
    pub max_staged_records: Option<usize>,
    /// Override the durable state key. Default
    /// `spanner-cdc:<project>.<instance>.<database>:<change_stream>`.
    #[serde(default)]
    pub state_key: Option<String>,
}

impl SpannerCdcSourceConfig {
    /// Validate fail-fast invariants. Called from `SpannerCdcSource::new`.
    pub fn validate(&self) -> Result<(), FaucetError> {
        self.connection.validate()?;
        validate_stream_name(&self.change_stream)?;
        if !(MIN_HEARTBEAT_MS..=MAX_HEARTBEAT_MS).contains(&self.heartbeat_ms) {
            return Err(FaucetError::Config(format!(
                "spanner-cdc: heartbeat_ms must be between {MIN_HEARTBEAT_MS} and \
                 {MAX_HEARTBEAT_MS}, got {}",
                self.heartbeat_ms
            )));
        }
        if self.idle_timeout <= Duration::from_millis(u64::from(self.heartbeat_ms)) {
            return Err(FaucetError::Config(format!(
                "spanner-cdc: idle_timeout ({}ms) must be greater than heartbeat_ms ({})",
                self.idle_timeout.as_millis(),
                self.heartbeat_ms
            )));
        }
        if !(1..=MAX_CONCURRENT_PARTITIONS).contains(&self.max_concurrent_partitions) {
            return Err(FaucetError::Config(format!(
                "spanner-cdc: max_concurrent_partitions must be between 1 and \
                 {MAX_CONCURRENT_PARTITIONS}, got {}",
                self.max_concurrent_partitions
            )));
        }
        if self.max_concurrent_partitions > self.connection.max_sessions {
            return Err(FaucetError::Config(format!(
                "spanner-cdc: max_concurrent_partitions ({}) exceeds max_sessions ({}); \
                 every partition reader holds one session",
                self.max_concurrent_partitions, self.connection.max_sessions
            )));
        }
        faucet_core::validate_batch_size(self.batch_size)?;
        if let StartFrom::Timestamp { timestamp } = &self.start_from {
            parse_timestamp(timestamp).map_err(|e| {
                FaucetError::Config(format!(
                    "spanner-cdc: start_from.timestamp {timestamp:?} is not RFC 3339: {e}"
                ))
            })?;
        }
        faucet_core::state::validate_state_key(&self.effective_state_key())?;
        Ok(())
    }

    /// The durable state key: the override, or a stable derived default.
    pub fn effective_state_key(&self) -> String {
        match &self.state_key {
            Some(k) => k.clone(),
            None => default_state_key(&self.connection, &self.change_stream),
        }
    }
}

/// Derived state key. Project / instance / database ids are `[a-z0-9-]` by
/// Spanner's naming rules; sanitise defensively anyway (`:` separates segments).
pub(crate) fn default_state_key(conn: &SpannerConnection, stream: &str) -> String {
    let path: String = format!("{}.{}.{}", conn.project_id, conn.instance, conn.database)
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("spanner-cdc:{path}:{stream}")
}

/// Change-stream names are plain identifiers; interpolated into
/// `READ_<name>` / `read_json_<name>`, so they must be validated, not quoted.
pub(crate) fn validate_stream_name(name: &str) -> Result<(), FaucetError> {
    let mut chars = name.chars();
    let ok = matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        && name.len() <= 128;
    if ok {
        Ok(())
    } else {
        Err(FaucetError::Config(format!(
            "spanner-cdc: change_stream {name:?} is not a valid identifier \
             (letters, digits and underscores, starting with a letter or underscore)"
        )))
    }
}

/// Parse an RFC 3339 timestamp into UTC.
pub(crate) fn parse_timestamp(s: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
    DateTime::parse_from_rfc3339(s).map(|d| d.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn minimal() -> SpannerCdcSourceConfig {
        serde_json::from_value(json!({
            "project_id": "p", "instance": "i", "database": "d",
            "change_stream": "orders_stream"
        }))
        .unwrap()
    }

    #[test]
    fn defaults_via_serde() {
        let c = minimal();
        assert_eq!(c.heartbeat_ms, 10_000);
        assert_eq!(c.idle_timeout, Duration::from_secs(60));
        assert_eq!(c.batch_size, DEFAULT_BATCH_SIZE);
        assert_eq!(c.max_concurrent_partitions, 4);
        assert_eq!(c.start_from, StartFrom::Now);
        assert_eq!(c.max_staged_records, None);
        assert_eq!(c.state_key, None);
        assert!(c.validate().is_ok());
    }

    #[test]
    fn start_from_timestamp_round_trips_and_validates() {
        let mut c: SpannerCdcSourceConfig = serde_json::from_value(json!({
            "project_id": "p", "instance": "i", "database": "d",
            "change_stream": "s",
            "start_from": {"type": "timestamp", "timestamp": "2026-01-31T10:00:00Z"}
        }))
        .unwrap();
        assert!(c.validate().is_ok());
        c.start_from = StartFrom::Timestamp {
            timestamp: "yesterday".into(),
        };
        assert!(matches!(c.validate(), Err(FaucetError::Config(_))));
    }

    #[test]
    fn rejects_bad_stream_names() {
        for bad in ["", "1abc", "has space", "semi;colon", "dash-ed", "tick`"] {
            let mut c = minimal();
            c.change_stream = bad.into();
            assert!(
                matches!(c.validate(), Err(FaucetError::Config(_))),
                "{bad:?} must be rejected"
            );
        }
        for good in ["s", "_s", "Orders_2", "a1_b2"] {
            assert!(validate_stream_name(good).is_ok(), "{good:?}");
        }
    }

    #[test]
    fn heartbeat_bounds() {
        let mut c = minimal();
        c.heartbeat_ms = 999;
        assert!(c.validate().is_err());
        c.heartbeat_ms = 300_001;
        assert!(c.validate().is_err());
        c.heartbeat_ms = 1_000;
        assert!(c.validate().is_ok());
        c.heartbeat_ms = 300_000;
        c.idle_timeout = Duration::from_secs(600);
        assert!(c.validate().is_ok());
    }

    #[test]
    fn idle_timeout_must_exceed_heartbeat() {
        let mut c = minimal();
        c.idle_timeout = Duration::from_millis(10_000);
        assert!(c.validate().is_err());
        c.idle_timeout = Duration::from_millis(10_001);
        assert!(c.validate().is_ok());
    }

    #[test]
    fn concurrency_bounds() {
        let mut c = minimal();
        c.max_concurrent_partitions = 0;
        assert!(c.validate().is_err());
        c.max_concurrent_partitions = 65;
        assert!(c.validate().is_err());
        c.max_concurrent_partitions = 8;
        c.connection.max_sessions = 4;
        assert!(c.validate().is_err());
        c.connection.max_sessions = 8;
        assert!(c.validate().is_ok());
    }

    #[test]
    fn batch_size_zero_is_the_drain_sentinel() {
        let mut c = minimal();
        c.batch_size = 0;
        assert!(c.validate().is_ok());
    }

    #[test]
    fn default_state_key_is_stable_and_valid() {
        let c = minimal();
        let k = c.effective_state_key();
        assert_eq!(k, "spanner-cdc:p.i.d:orders_stream");
        faucet_core::state::validate_state_key(&k).unwrap();
    }

    #[test]
    fn state_key_override_wins_and_is_validated() {
        let mut c = minimal();
        c.state_key = Some("custom:key".into());
        assert_eq!(c.effective_state_key(), "custom:key");
        assert!(c.validate().is_ok());
        c.state_key = Some("bad key!".into());
        assert!(c.validate().is_err());
    }

    #[test]
    fn debug_never_prints_inline_key_material() {
        let c: SpannerCdcSourceConfig = serde_json::from_value(json!({
            "project_id": "p", "instance": "i", "database": "d",
            "change_stream": "s",
            "auth": {"type": "service_account_key", "config": {"json": "{\"private_key\":\"SECRET\"}"}}
        }))
        .unwrap();
        let dbg = format!("{c:?}");
        assert!(!dbg.contains("SECRET"));
    }

    #[test]
    fn schema_requires_change_stream() {
        let schema = schemars::schema_for!(SpannerCdcSourceConfig);
        let v = serde_json::to_value(&schema).unwrap();
        let req: Vec<_> = v["required"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|x| x.as_str())
            .collect();
        assert!(req.contains(&"change_stream"));
        assert!(req.contains(&"project_id"));
    }
}
