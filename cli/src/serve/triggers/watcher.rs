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

/// One pollable watcher (object_arrival / queue_depth). Webhook is push, not
/// polled, so it does not implement this.
#[async_trait]
pub trait Watcher: Send + Sync {
    fn name(&self) -> &str;
    fn kind(&self) -> &'static str;
    fn poll_interval(&self) -> Duration;
    /// Do one cycle. `Ok(true)` = something fired; `Ok(false)` = idle.
    async fn poll(&mut self, state: &ServerState) -> Result<bool, String>;
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
                match watcher.poll(&state).await {
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
                        metrics::error(watcher.name(), watcher.kind());
                        health.record_err(watcher.name(), e, UNHEALTHY_THRESHOLD);
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
