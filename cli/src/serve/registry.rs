//! In-flight run registry. Tracks per-run cancellation tokens and the queue /
//! in-flight counters that drive backpressure (429), the `faucet_serve_runs_*`
//! gauges, `/readyz`, and the shutdown drain. A "queued" run is one that has been
//! spawned but has not yet acquired an execution permit.

use dashmap::DashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

/// The unique registry key for a shard's cancel token. Keeping it distinct from
/// the parent run's key (which is the bare run id) lets `cancel_run_shards` fire
/// all of a run's shard tokens via a `{run_id}::` prefix scan without colliding
/// with the run's own token.
fn shard_key(run_id: &str, shard_id: &str) -> String {
    format!("{run_id}::{shard_id}")
}

pub struct Registry {
    tokens: DashMap<String, CancellationToken>,
    queued: AtomicUsize,
    in_flight: AtomicUsize,
    max_queued: usize,
    drained: Notify,
}

impl Registry {
    pub fn new(max_queued: usize) -> Self {
        Self {
            tokens: DashMap::new(),
            queued: AtomicUsize::new(0),
            in_flight: AtomicUsize::new(0),
            max_queued: max_queued.max(1),
            drained: Notify::new(),
        }
    }

    /// Reserve a queue slot. Returns `true` if a slot was reserved, or `false`
    /// if the queue is full. Atomic against concurrent submits via a CAS loop
    /// (`AtomicUsize::try_update`, stabilized in Rust 1.95).
    pub fn try_reserve(&self) -> bool {
        self.queued
            .try_update(Ordering::AcqRel, Ordering::Acquire, |cur| {
                (cur < self.max_queued).then_some(cur + 1)
            })
            .is_ok()
    }

    /// Release a slot reserved by `try_reserve` that will not be spawned
    /// (idempotency replay/conflict).
    pub fn release_reservation(&self) {
        self.dec_queued();
    }

    pub fn register(&self, run_id: String, token: CancellationToken) {
        self.tokens.insert(run_id, token);
    }

    /// Queued → running: the run acquired its execution permit.
    pub fn mark_running(&self) {
        self.dec_queued();
        self.in_flight.fetch_add(1, Ordering::AcqRel);
    }

    /// Running transition for a run that never occupied a local queue slot (the
    /// cluster claim path: `submit` writes Pending + releases its reservation, so
    /// no queued slot exists to consume). Only bumps `in_flight`.
    pub fn mark_running_unqueued(&self) {
        self.in_flight.fetch_add(1, Ordering::AcqRel);
    }

    /// Running → terminal: drop the token and wake any shutdown drain waiter.
    pub fn mark_finished(&self, run_id: &str) {
        self.dec_in_flight();
        self.tokens.remove(run_id);
        self.drained.notify_waiters();
    }

    /// Queued → terminal: a run was cancelled (or the server shut down) while
    /// still waiting for an execution permit, so it never became in-flight.
    /// Release its **queue** slot (not `in_flight`), drop the token, and wake any
    /// drain waiter. Distinct from [`Self::mark_finished`], which decrements
    /// `in_flight` (#146 R: a cancel on a queued run now takes effect at once).
    pub fn mark_queued_cancelled(&self, run_id: &str) {
        self.dec_queued();
        self.tokens.remove(run_id);
        self.drained.notify_waiters();
    }

    /// Saturating decrement of `queued` (never wraps below 0 — a wrap to
    /// usize::MAX would permanently fail `try_reserve` and wedge the server).
    fn dec_queued(&self) {
        let _ = self
            .queued
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |q| {
                Some(q.saturating_sub(1))
            });
    }

    /// Saturating decrement of `in_flight`.
    fn dec_in_flight(&self) {
        let _ = self
            .in_flight
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                Some(n.saturating_sub(1))
            });
    }

    /// Cancel a live run. Returns `true` if a live token existed.
    pub fn cancel(&self, run_id: &str) -> bool {
        if let Some(t) = self.tokens.get(run_id) {
            t.cancel();
            true
        } else {
            false
        }
    }

    /// Register a shard's cancel token under a per-shard key (`{run_id}::{shard_id}`).
    /// Separate from a run's token so a sharded run's shards each get their own
    /// cooperative-cancel signal (Mode B, #230 / F10).
    pub fn register_shard(&self, run_id: &str, shard_id: &str, token: CancellationToken) {
        self.tokens.insert(shard_key(run_id, shard_id), token);
    }

    /// Drop a token by key without touching the queue/in-flight counters. Used to
    /// remove a finished shard's token (shard accounting is separate from the
    /// parent run's `in_flight`, so [`Self::mark_finished`] is not appropriate).
    pub fn deregister_shard(&self, run_id: &str, shard_id: &str) {
        self.tokens.remove(&shard_key(run_id, shard_id));
    }

    /// A claimed shard began executing: bump `in_flight` so the shutdown drain
    /// (`wait_drained` + `shutdown.cancel()`) accounts for it. Without this,
    /// running Mode-B shards are invisible to `wait_drained`, so SIGTERM sees
    /// `in_flight == 0`, `shutdown.cancel()` never fires, and the detached shard
    /// tasks are hard-dropped mid-write with no cooperative flush (audit #321
    /// H5). Pairs with [`Self::mark_shard_finished`].
    pub fn mark_shard_running(&self) {
        self.in_flight.fetch_add(1, Ordering::AcqRel);
    }

    /// A shard reached a terminal state: decrement `in_flight`, drop its token,
    /// and wake any drain waiter. The shard-specific analogue of
    /// [`Self::mark_finished`].
    pub fn mark_shard_finished(&self, run_id: &str, shard_id: &str) {
        self.dec_in_flight();
        self.deregister_shard(run_id, shard_id);
        self.drained.notify_waiters();
    }

    /// Fire every registered shard token whose key belongs to `run_id` (key
    /// prefix `{run_id}::`). Returns how many tokens were fired. Drives a
    /// cross-instance cancel of a sharded run: the claim loop calls this for each
    /// run id returned by `pending_shard_cancellations` (F10).
    pub fn cancel_run_shards(&self, run_id: &str) -> usize {
        let prefix = format!("{run_id}::");
        let mut fired = 0usize;
        for entry in self.tokens.iter() {
            if entry.key().starts_with(&prefix) {
                entry.value().cancel();
                fired += 1;
            }
        }
        fired
    }

    /// Fire every live run and shard token. Returns how many were fired.
    pub fn cancel_all(&self) -> usize {
        let mut fired = 0usize;
        for entry in self.tokens.iter() {
            entry.value().cancel();
            fired += 1;
        }
        fired
    }

    /// Run ids with a live cancellation token — i.e. runs this instance has
    /// registered and not yet finished.
    ///
    /// Used by the local-output GC (#587) to skip files a run is still writing:
    /// deleting an output mid-write would corrupt it. Shard entries are keyed
    /// `{run_id}::{shard_id}`, so the prefix before `::` is taken — a shard being
    /// live makes its parent run live for this purpose, which is what the
    /// sweeper needs.
    pub fn live_run_ids(&self) -> std::collections::BTreeSet<String> {
        self.tokens
            .iter()
            .map(|e| {
                let key = e.key();
                match key.split_once("::") {
                    Some((run_id, _shard)) => run_id.to_string(),
                    None => key.clone(),
                }
            })
            .collect()
    }

    pub fn queued(&self) -> usize {
        self.queued.load(Ordering::Acquire)
    }

    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Acquire)
    }

    pub fn is_full(&self) -> bool {
        self.queued() >= self.max_queued
    }

    /// Resolve once no run is queued or in flight. Arms the notification *before*
    /// re-checking so a transition can't be missed.
    pub async fn wait_drained(&self) {
        loop {
            if self.queued() == 0 && self.in_flight() == 0 {
                return;
            }
            let notified = self.drained.notified();
            if self.queued() == 0 && self.in_flight() == 0 {
                return;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancel_all_fires_every_run_and_shard_token() {
        let r = Registry::new(4);
        let run = CancellationToken::new();
        let shard = CancellationToken::new();
        r.register("a".into(), run.clone());
        r.register_shard("b", "0", shard.clone());
        assert_eq!(r.cancel_all(), 2);
        assert!(run.is_cancelled() && shard.is_cancelled());
    }

    #[test]
    fn reserve_respects_capacity() {
        let r = Registry::new(2);
        assert!(r.try_reserve());
        assert!(r.try_reserve());
        assert!(!r.try_reserve());
        assert!(r.is_full());
        r.release_reservation();
        assert!(r.try_reserve());
    }

    #[test]
    fn running_transition_moves_counters() {
        let r = Registry::new(4);
        r.try_reserve();
        assert_eq!(r.queued(), 1);
        r.mark_running();
        assert_eq!(r.queued(), 0);
        assert_eq!(r.in_flight(), 1);
        r.mark_finished("x");
        assert_eq!(r.in_flight(), 0);
    }

    #[test]
    fn mark_running_unqueued_only_bumps_in_flight() {
        let r = Registry::new(4);
        // No reservation taken (cluster claim path).
        r.mark_running_unqueued();
        assert_eq!(
            r.queued(),
            0,
            "queued must NOT be decremented (no slot was held)"
        );
        assert_eq!(r.in_flight(), 1);
        r.mark_finished("x");
        assert_eq!(r.in_flight(), 0);
        assert_eq!(r.queued(), 0);
    }

    #[test]
    fn queued_decrement_saturates_at_zero() {
        let r = Registry::new(4);
        // A spurious decrement at 0 must NOT wrap to usize::MAX (that would
        // permanently fail try_reserve and wedge backpressure — #228).
        r.mark_running(); // dec_queued() at 0 + in_flight++
        assert_eq!(r.queued(), 0, "saturating: stays 0, never usize::MAX");
        assert!(
            r.try_reserve(),
            "try_reserve still works (queued not wrapped)"
        );
    }

    #[test]
    fn cancel_reports_presence() {
        let r = Registry::new(4);
        let token = CancellationToken::new();
        r.register("run1".into(), token.clone());
        assert!(r.cancel("run1"));
        assert!(token.is_cancelled());
        assert!(!r.cancel("missing"));
    }

    #[test]
    fn cancel_run_shards_fires_only_matching_run_tokens() {
        let r = Registry::new(8);
        // Two shards of run "A", one shard of run "B", plus a whole-run token for
        // "A" (registered under the bare run id — must NOT be fired by the shard
        // sweep, which keys on the "A::" prefix).
        let a0 = CancellationToken::new();
        let a1 = CancellationToken::new();
        let b0 = CancellationToken::new();
        let a_run = CancellationToken::new();
        r.register_shard("A", "0", a0.clone());
        r.register_shard("A", "1", a1.clone());
        r.register_shard("B", "0", b0.clone());
        r.register("A".into(), a_run.clone());

        let fired = r.cancel_run_shards("A");
        assert_eq!(fired, 2, "both A shards fired");
        assert!(a0.is_cancelled());
        assert!(a1.is_cancelled());
        assert!(!b0.is_cancelled(), "B's shard untouched");
        assert!(
            !a_run.is_cancelled(),
            "A's whole-run token (bare id, no '::') untouched"
        );

        // No shards for a run → fires nothing.
        assert_eq!(r.cancel_run_shards("C"), 0);
    }

    #[test]
    fn shard_running_counts_toward_in_flight_and_drain() {
        // #321 H5: a running shard must be visible to the shutdown drain so
        // `wait_drained` blocks on it and `shutdown.cancel()` gets a chance to
        // fire the cooperative flush.
        let r = Registry::new(4);
        r.register_shard("A", "0", CancellationToken::new());
        r.mark_shard_running();
        assert_eq!(r.in_flight(), 1, "a running shard bumps in_flight");
        // Finishing drops the token, decrements in_flight, and wakes the drain.
        r.mark_shard_finished("A", "0");
        assert_eq!(r.in_flight(), 0);
        assert_eq!(
            r.cancel_run_shards("A"),
            0,
            "the shard token was removed on finish"
        );
    }

    #[test]
    fn deregister_shard_removes_the_token() {
        let r = Registry::new(4);
        r.register_shard("A", "0", CancellationToken::new());
        r.register_shard("A", "1", CancellationToken::new());
        r.deregister_shard("A", "0");
        // Only "A::1" remains → exactly one token fired.
        assert_eq!(r.cancel_run_shards("A"), 1, "deregistered token not fired");
        r.deregister_shard("A", "1");
        assert_eq!(r.cancel_run_shards("A"), 0, "all shard tokens removed");
    }

    #[test]
    fn is_not_idle_while_queued() {
        let r = Registry::new(4);
        r.try_reserve();
        // queued=1, in_flight=0 — must NOT be considered drained.
        assert_eq!(r.queued(), 1);
        assert_eq!(r.in_flight(), 0);
    }

    #[tokio::test]
    async fn wait_drained_returns_when_idle() {
        let r = Registry::new(4);
        // No in-flight work → resolves immediately.
        r.wait_drained().await;
    }
}
