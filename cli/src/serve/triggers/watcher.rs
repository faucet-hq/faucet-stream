//! The `Watcher` trait + the supervised polling loop. A watcher never dies on a
//! transient error: it records health, backs off, and retries until shutdown.

use super::health::TriggersHandle;
use super::metrics;
use crate::serve::state::ServerState;
use async_trait::async_trait;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Consecutive poll failures before a watcher is reported unhealthy on /readyz.
pub const UNHEALTHY_THRESHOLD: u64 = 3;
/// Backoff ceiling so a persistently-failing watcher still retries periodically.
pub const MAX_BACKOFF: Duration = Duration::from_secs(60);
/// Shortest default time one poll (or, for `object_arrival`, one listing
/// page) may take before it counts as failed.
pub const POLL_TIMEOUT_FLOOR: Duration = Duration::from_secs(60);

/// The default stall limit for a watcher polling every `interval`.
pub fn default_poll_timeout(interval: Duration) -> Duration {
    interval.max(POLL_TIMEOUT_FLOOR)
}

/// Run one poll, bounded by `limit` (when set) and stopped by `shutdown`
/// (`None`). A poll that hangs on an unresponsive backend becomes an error
/// the supervisor records, instead of freezing the watcher while it reads
/// healthy.
pub async fn poll_bounded<W: Watcher + ?Sized>(
    watcher: &mut W,
    state: &ServerState,
    limit: Option<Duration>,
    shutdown: &CancellationToken,
) -> Option<Result<bool, PollError>> {
    let poll = async {
        match limit {
            Some(limit) => match tokio::time::timeout(limit, watcher.poll(state)).await {
                Ok(r) => r,
                Err(_) => Err(PollError::from(format!(
                    "poll did not finish within {}s (raise the trigger's poll_timeout_secs if \
                     a poll legitimately takes longer)",
                    limit.as_secs()
                ))),
            },
            None => watcher.poll(state).await,
        }
    };
    tokio::select! {
        biased;
        _ = shutdown.cancelled() => None,
        r = poll => Some(r),
    }
}

/// Why a poll failed. `counted` is set when the failure is a fire the fire
/// path already counted in `faucet_serve_trigger_errors_total`, so the
/// supervisor does not count it again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PollError {
    pub message: String,
    pub counted: bool,
}

impl PollError {
    /// A failed fire, already counted by the fire path.
    pub fn fired(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            counted: true,
        }
    }
}

impl From<String> for PollError {
    fn from(message: String) -> Self {
        Self {
            message,
            counted: false,
        }
    }
}

impl std::fmt::Display for PollError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

/// One pollable watcher (object_arrival / queue_depth). Webhook is push, not
/// polled, so it does not implement this.
#[async_trait]
pub trait Watcher: Send + Sync {
    fn name(&self) -> &str;
    fn kind(&self) -> &'static str;
    fn poll_interval(&self) -> Duration;
    /// Longest one whole poll may take before it counts as failed; `None`
    /// leaves the whole poll unbounded (the watcher bounds its own I/O).
    /// Default: [`default_poll_timeout`] of the poll interval.
    fn poll_timeout(&self) -> Option<Duration> {
        Some(default_poll_timeout(self.poll_interval()))
    }
    /// Do one cycle. `Ok(true)` = something fired; `Ok(false)` = idle.
    async fn poll(&mut self, state: &ServerState) -> Result<bool, PollError>;
}

/// Exponential backoff doubling from `base` (the poll interval) to `MAX_BACKOFF`.
pub fn backoff(base: Duration, consecutive_failures: u64) -> Duration {
    if consecutive_failures == 0 {
        return base;
    }
    let shift = consecutive_failures.min(20) as u32;
    let scaled = base.saturating_mul(1u32 << shift.min(16));
    scaled.min(MAX_BACKOFF).max(base)
}

/// Run a watcher until `shutdown` fires. Never returns an error (logs instead).
#[allow(clippy::disallowed_methods)] // a runtime timer, not a connector backend call
pub async fn run_supervised<W: Watcher>(
    mut watcher: W,
    state: ServerState,
    health: TriggersHandle,
    shutdown: CancellationToken,
) {
    let base = watcher.poll_interval();
    let mut failures: u64 = 0;
    metrics::healthy(watcher.name(), true);
    loop {
        let wait = backoff(base, failures);
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                tracing::info!(trigger = watcher.name(), "trigger watcher stopping");
                break;
            }
            _ = tokio::time::sleep(wait) => {
                let limit = watcher.poll_timeout();
                let Some(outcome) = poll_bounded(&mut watcher, &state, limit, &shutdown).await else {
                    tracing::info!(trigger = watcher.name(), "trigger watcher stopping");
                    break;
                };
                match outcome {
                    Ok(fired) => {
                        failures = 0;
                        let stamp = if fired {
                            Some(chrono::Utc::now().to_rfc3339())
                        } else {
                            None
                        };
                        if fired {
                            metrics::last_fire(watcher.name(), chrono::Utc::now().timestamp());
                        }
                        health.record_ok(watcher.name(), stamp);
                        metrics::healthy(watcher.name(), true);
                    }
                    Err(e) => {
                        failures += 1;
                        tracing::warn!(trigger = watcher.name(), error = %e, "trigger poll failed");
                        if !e.counted {
                            metrics::error(watcher.name(), watcher.kind());
                            health.note_error(watcher.name());
                        }
                        health.record_err(watcher.name(), e.message, UNHEALTHY_THRESHOLD);
                        if failures >= UNHEALTHY_THRESHOLD {
                            metrics::healthy(watcher.name(), false);
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Hangs;

    #[async_trait]
    impl Watcher for Hangs {
        fn name(&self) -> &str {
            "hangs"
        }
        fn kind(&self) -> &'static str {
            "queue_depth"
        }
        fn poll_interval(&self) -> Duration {
            Duration::from_secs(1)
        }
        async fn poll(&mut self, _state: &ServerState) -> Result<bool, PollError> {
            std::future::pending().await
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_hung_poll_times_out_and_shutdown_stops_it() {
        let state = crate::serve::test_support::test_state();
        let token = CancellationToken::new();
        let out = poll_bounded(&mut Hangs, &state, Some(POLL_TIMEOUT_FLOOR), &token).await;
        let err = out.expect("not shut down").unwrap_err();
        assert!(err.message.contains("did not finish within 60s"), "{err}");
        assert!(!err.counted);
        token.cancel();
        assert!(
            poll_bounded(&mut Hangs, &state, Some(POLL_TIMEOUT_FLOOR), &token)
                .await
                .is_none()
        );
    }

    #[test]
    fn a_fired_poll_error_is_marked_counted() {
        assert!(PollError::fired("x").counted);
        assert!(!PollError::from("x".to_string()).counted);
    }

    #[test]
    fn backoff_starts_at_base_and_caps() {
        let base = Duration::from_secs(10);
        assert_eq!(backoff(base, 0), base);
        assert_eq!(backoff(base, 1), Duration::from_secs(20));
        assert_eq!(backoff(base, 2), Duration::from_secs(40));
        // Cap at MAX_BACKOFF.
        assert_eq!(backoff(base, 50), MAX_BACKOFF);
    }
}
