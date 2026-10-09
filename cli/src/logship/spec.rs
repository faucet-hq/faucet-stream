//! The `observability.logs:` block (#806).

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

/// Durable log-shipping settings. Shipping itself is switched on by adding
/// `logs` to `observability.otel.export`; this block tunes the local buffer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LogsSpec {
    /// Directory holding the local spool `faucet run` / `faucet schedule`
    /// write every line to before it is shipped. Default:
    /// `$XDG_STATE_HOME/faucet/logs` (`~/.local/state/faucet/logs`).
    #[serde(default)]
    pub spool_dir: Option<PathBuf>,
    /// Seconds a run's lines stay in the local buffer after they were
    /// delivered. Default: 86400 (24 h).
    #[serde(default = "default_retention_secs")]
    pub retention_secs: u64,
    /// Undelivered lines older than this many seconds are dropped (counted,
    /// and the run is marked `partially_dropped`). Default: 604800 (7 days).
    #[serde(default = "default_buffer_max_age_secs")]
    pub buffer_max_age_secs: u64,
    /// Upper bound on the spool size in bytes; past it the oldest undelivered
    /// lines are dropped. Default: 1073741824 (1 GiB).
    #[serde(default = "default_buffer_max_bytes")]
    pub buffer_max_bytes: u64,
    /// How long `faucet run` waits at exit for the run's lines to ship.
    /// Anything still undelivered stays in the spool for the next faucet
    /// process or `faucet logs ship`. Default: 10.
    #[serde(default = "default_flush_timeout_secs")]
    pub flush_timeout_secs: u64,
    /// Per-run line cap; lines past it are dropped and counted. Default: 100000.
    #[serde(default = "default_max_lines_per_run")]
    pub max_lines_per_run: u64,
    /// Emit a `log_export_failed` notification once a run's export has been
    /// failing for this many seconds. Default: 300.
    #[serde(default = "default_notify_after_secs")]
    pub notify_after_secs: u64,
    /// A link to the run's logs in the log service, shown once the local copy
    /// has aged out. Placeholders: `{run_id} {pipeline} {row} {tenant}
    /// {started_at} {ended_at}` (URL-encoded).
    #[serde(default)]
    pub link_template: Option<String>,
}

pub(crate) fn default_retention_secs() -> u64 {
    86_400
}
pub(crate) fn default_buffer_max_age_secs() -> u64 {
    604_800
}
pub(crate) fn default_buffer_max_bytes() -> u64 {
    1_073_741_824
}
fn default_flush_timeout_secs() -> u64 {
    10
}
pub(crate) fn default_max_lines_per_run() -> u64 {
    100_000
}
pub(crate) fn default_notify_after_secs() -> u64 {
    300
}

impl Default for LogsSpec {
    fn default() -> Self {
        Self {
            spool_dir: None,
            retention_secs: default_retention_secs(),
            buffer_max_age_secs: default_buffer_max_age_secs(),
            buffer_max_bytes: default_buffer_max_bytes(),
            flush_timeout_secs: default_flush_timeout_secs(),
            max_lines_per_run: default_max_lines_per_run(),
            notify_after_secs: default_notify_after_secs(),
            link_template: None,
        }
    }
}

impl LogsSpec {
    /// Reject values that would make the buffer useless.
    pub fn validate(&self) -> Result<(), String> {
        if self.buffer_max_age_secs == 0 {
            return Err("observability.logs.buffer_max_age_secs must be > 0".into());
        }
        if self.buffer_max_bytes == 0 {
            return Err("observability.logs.buffer_max_bytes must be > 0".into());
        }
        if self.max_lines_per_run == 0 {
            return Err("observability.logs.max_lines_per_run must be > 0".into());
        }
        if let Some(t) = &self.link_template
            && t.trim().is_empty()
        {
            return Err("observability.logs.link_template must not be empty".into());
        }
        Ok(())
    }

    /// The spool directory, falling back to the XDG state directory.
    pub fn resolved_spool_dir(&self) -> PathBuf {
        self.spool_dir.clone().unwrap_or_else(|| {
            default_spool_dir(
                std::env::var_os("XDG_STATE_HOME").map(PathBuf::from),
                std::env::var_os("HOME").map(PathBuf::from),
            )
        })
    }

    pub fn retention(&self) -> Duration {
        Duration::from_secs(self.retention_secs)
    }
    pub fn max_age(&self) -> Duration {
        Duration::from_secs(self.buffer_max_age_secs)
    }
    pub fn flush_timeout(&self) -> Duration {
        Duration::from_secs(self.flush_timeout_secs)
    }
    pub fn notify_after(&self) -> Duration {
        Duration::from_secs(self.notify_after_secs)
    }
}

/// `$XDG_STATE_HOME/faucet/logs`, else `$HOME/.local/state/faucet/logs`, else
/// `./.faucet/logs`.
pub fn default_spool_dir(xdg_state: Option<PathBuf>, home: Option<PathBuf>) -> PathBuf {
    if let Some(x) = xdg_state.filter(|p| p.is_absolute()) {
        return x.join("faucet").join("logs");
    }
    if let Some(h) = home {
        return h.join(".local").join("state").join("faucet").join("logs");
    }
    PathBuf::from(".faucet").join("logs")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_from_an_empty_block() {
        let s: LogsSpec = serde_json::from_str("{}").unwrap();
        assert_eq!(s, LogsSpec::default());
        assert_eq!(s.retention(), Duration::from_secs(86_400));
        assert_eq!(s.max_age(), Duration::from_secs(604_800));
        assert_eq!(s.buffer_max_bytes, 1 << 30);
        assert_eq!(s.flush_timeout(), Duration::from_secs(10));
        assert_eq!(s.notify_after(), Duration::from_secs(300));
        assert!(s.validate().is_ok());
    }

    #[test]
    fn validate_rejects_zero_bounds_and_blank_link() {
        for bad in [
            LogsSpec {
                buffer_max_age_secs: 0,
                ..Default::default()
            },
            LogsSpec {
                buffer_max_bytes: 0,
                ..Default::default()
            },
            LogsSpec {
                max_lines_per_run: 0,
                ..Default::default()
            },
            LogsSpec {
                link_template: Some("  ".into()),
                ..Default::default()
            },
        ] {
            assert!(bad.validate().is_err(), "{bad:?}");
        }
        assert!(serde_json::from_str::<LogsSpec>(r#"{"nope":1}"#).is_err());
    }

    #[test]
    fn spool_dir_prefers_explicit_then_xdg_then_home() {
        let s = LogsSpec {
            spool_dir: Some("/var/lib/faucet/logs".into()),
            ..Default::default()
        };
        assert_eq!(
            s.resolved_spool_dir(),
            PathBuf::from("/var/lib/faucet/logs")
        );
        assert_eq!(
            default_spool_dir(Some("/x".into()), Some("/h".into())),
            PathBuf::from("/x/faucet/logs")
        );
        assert_eq!(
            default_spool_dir(Some("rel".into()), Some("/h".into())),
            PathBuf::from("/h/.local/state/faucet/logs")
        );
        assert_eq!(default_spool_dir(None, None), PathBuf::from(".faucet/logs"));
        let _ = LogsSpec::default().resolved_spool_dir();
    }
}
