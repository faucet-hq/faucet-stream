//! Clustered execution (#197, Mode A): when `--cluster` is set, every instance
//! runs a claim loop that pulls `Pending` runs from the shared SQL history DB,
//! so submissions pull-balance across instances and a crashed instance's runs
//! are re-run by a survivor. Inert unless enabled.

use crate::serve::config::ServeConfig;
use crate::serve::state::ServerState;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

/// Validated cluster settings, derived from `--cluster*` args.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub enabled: bool,
    /// Claim-loop poll interval (also the cross-instance cancel-propagation lag).
    pub poll: Duration,
    /// Max failover re-runs before an orphan is marked Failed (poison).
    pub max_attempts: u32,
}

impl ClusterConfig {
    /// A disabled cluster (single-instance default).
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            poll: Duration::from_secs(2),
            max_attempts: 3,
        }
    }
}

/// Cheaply-cloneable runtime handle for cluster coordination, held in
/// `ServerState`. Carries the kick signal (so `submit` can wake the local claim
/// loop immediately) and the cached live-member count (so `/readyz` need not hit
/// the DB per probe).
#[derive(Clone)]
pub struct ClusterHandle {
    inner: Arc<ClusterInner>,
}

struct ClusterInner {
    cfg: ClusterConfig,
    listen: String,
    max_concurrent: u32,
    started_at: chrono::DateTime<chrono::Utc>,
    kick: Notify,
    members: AtomicUsize,
}

impl ClusterHandle {
    /// Build from the validated server config (reads `cluster`, `listen`,
    /// `max_concurrent_runs`). Captures the instance start time for membership.
    pub fn from_config(config: &ServeConfig) -> Self {
        Self {
            inner: Arc::new(ClusterInner {
                cfg: config.cluster.clone(),
                listen: config.listen.to_string(),
                max_concurrent: config.max_concurrent_runs as u32,
                started_at: chrono::Utc::now(),
                kick: Notify::new(),
                members: AtomicUsize::new(0),
            }),
        }
    }

    pub fn enabled(&self) -> bool {
        self.inner.cfg.enabled
    }
    pub fn poll(&self) -> Duration {
        self.inner.cfg.poll
    }
    pub fn max_attempts(&self) -> u32 {
        self.inner.cfg.max_attempts
    }
    pub fn listen(&self) -> &str {
        &self.inner.listen
    }
    pub fn max_concurrent(&self) -> u32 {
        self.inner.max_concurrent
    }
    pub fn started_at(&self) -> chrono::DateTime<chrono::Utc> {
        self.inner.started_at
    }

    /// Wake the local claim loop now (called by `submit` after writing Pending).
    pub fn kick(&self) {
        self.inner.kick.notify_one();
    }
    /// Await the next kick (used by the claim loop).
    pub async fn kicked(&self) {
        self.inner.kick.notified().await;
    }

    /// Cached count of live cluster members (updated by the lease loop).
    pub fn members(&self) -> usize {
        self.inner.members.load(Ordering::Acquire)
    }
    pub fn set_members(&self, n: usize) {
        self.inner.members.store(n, Ordering::Release);
    }
}

/// Background claim + cancel-propagation loop (cluster mode only). Each wake:
/// 1. fire local cancels for any of this instance's runs flagged remotely;
/// 2. claim up to `available_permits()` Pending runs and dispatch each to the
///    existing execution path.
///
/// Wakes every `poll` interval or immediately on a `kick()` from `submit`.
///
/// Safe to claim exactly `available_permits()`: in cluster mode this loop is the
/// SOLE consumer of the execution semaphore (submit writes Pending + kicks but
/// never spawns locally), so claimed runs queue on the semaphore and drain at
/// `max_concurrent` — never over-running local capacity. Claimed runs flip to
/// `Running` in the shared DB immediately (the lease keeps them owned) even while
/// locally queued on the semaphore.
pub async fn claim_loop(state: ServerState, shutdown: CancellationToken) {
    let handle = state.cluster().clone();
    let mut tick = tokio::time::interval(handle.poll());
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            _ = tick.tick() => {}
            _ = handle.kicked() => {}
        }

        // 1. Cross-instance cancel: fire local tokens for flagged runs.
        match state.history().pending_cancellations().await {
            Ok(ids) => {
                for id in ids {
                    state.registry().cancel(&id);
                }
            }
            Err(e) => tracing::warn!(error = %e, "cluster: pending_cancellations failed"),
        }
        // 1b. Mode B (#230 / F10): a Sharded parent flagged for cancel — fire the
        //     local coop tokens of any of this instance's running shards under it
        //     so they stop + flush, instead of running to completion.
        match state.history().pending_shard_cancellations().await {
            Ok(ids) => {
                for id in ids {
                    let fired = state.registry().cancel_run_shards(&id);
                    if fired > 0 {
                        tracing::info!(
                            run_id = %id,
                            shards = fired,
                            "cluster: cancelling local shards of a flagged sharded run"
                        );
                    }
                }
            }
            Err(e) => tracing::warn!(error = %e, "cluster: pending_shard_cancellations failed"),
        }

        // 2. Claim up to our free capacity and dispatch.
        let free = state.semaphore().available_permits();
        if free == 0 {
            continue;
        }
        let mut claimed_count = 0usize;
        match state.history().claim_pending(free).await {
            Ok(claimed) => {
                if !claimed.is_empty() {
                    crate::serve::metrics::record_runs_claimed(claimed.len());
                    claimed_count = claimed.len();
                    for rec in claimed {
                        crate::serve::runner::resume_claimed_run(state.clone(), rec);
                    }
                }
            }
            Err(e) => tracing::warn!(error = %e, "cluster: claim_pending failed"),
        }

        // 3. Mode B (#230): claim source shards with the remaining budget and
        //    dispatch each to a per-shard executor. A claimed shard flips to
        //    Running (leased) in the shared DB; if it over-subscribes local
        //    permits it simply queues on the semaphore, like a claimed run.
        let shard_budget = free.saturating_sub(claimed_count);
        if shard_budget > 0 {
            match state.history().claim_shards(shard_budget).await {
                Ok(shards) => {
                    if !shards.is_empty() {
                        crate::serve::metrics::record_shards_claimed(shards.len());
                        for shard in shards {
                            crate::serve::runner::resume_claimed_shard(state.clone(), shard);
                        }
                    }
                }
                Err(e) => tracing::warn!(error = %e, "cluster: claim_shards failed"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_handle_reports_disabled() {
        let cfg = ClusterConfig::disabled();
        assert!(!cfg.enabled);
        assert_eq!(cfg.max_attempts, 3);
    }

    #[tokio::test]
    async fn kick_wakes_a_waiter() {
        // A kick issued before `kicked()` is awaited is still delivered
        // (Notify::notify_one stores one permit).
        use crate::serve::config::{AuthMode, HistoryBackendSpec, ServeConfig};
        let cfg = ServeConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            log_format: crate::cli::LogFormat::Text,
            auth: AuthMode::None,
            max_concurrent_runs: 4,
            max_queued_runs: 4,
            default_config_path: None,
            history: HistoryBackendSpec::Memory,
            cors_origins: vec![],
            body_limit_bytes: 1_048_576,
            shutdown_grace: std::time::Duration::from_secs(60),
            retain_terminal_runs: std::time::Duration::from_secs(60),
            idempotency_retention: std::time::Duration::from_secs(60),
            log_retention: std::time::Duration::from_secs(0),
            log_max_lines_per_run: 100_000,
            local_output_retention_days: 7,
            local_output_in_flight_grace: Duration::from_secs(60),
            preview: crate::serve::preview::PreviewConfig::default(),
            lease_ttl: std::time::Duration::from_secs(30),
            probe_timeout: std::time::Duration::from_secs(10),
            env_file: None,
            no_env_file: false,
            log_level: "info".into(),
            ui_enabled: true,
            cluster: ClusterConfig::disabled(),
            triggers_path: None,
            templates_sync_path: None,
            callback_allow_hosts: Vec::new(),
        };
        let h = ClusterHandle::from_config(&cfg);
        h.kick();
        tokio::time::timeout(std::time::Duration::from_secs(1), h.kicked())
            .await
            .expect("kick must wake the waiter");
        h.set_members(2);
        assert_eq!(h.members(), 2);
    }
}
