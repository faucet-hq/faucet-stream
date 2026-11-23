//! What every server knows about a delivery between runs (#1034): its
//! adaptive batch size, the claim it is working on, its recent failures and
//! whether it is failing, stalled, held or paused. One document per delivery
//! in the run history's settings, changed only by compare-and-set, so it
//! survives restarts and every replica reads the same one.

use crate::serve::history::RunHistory;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Consecutive failed runs after which a delivery is reported as failing.
pub const FAILING_AFTER: u32 = 3;
/// The first back-off after a failure, in seconds; it doubles up to
/// [`MAX_BACKOFF_SECS`].
pub const FIRST_BACKOFF_SECS: u64 = 5;
pub const MAX_BACKOFF_SECS: u64 = 60;
const ATTEMPTS: usize = 5;
/// How long one event must keep failing, with events waiting behind it,
/// before its delivery stalls: an outage shorter than this never stalls a
/// delivery, a poison event still does within minutes.
pub const MIN_STALL: std::time::Duration = std::time::Duration::from_secs(600);

/// The floor in force: [`MIN_STALL`]. Debug builds (the test suite) may
/// lower it with `FAUCET_FEED_TEST_MIN_STALL_SECS`; release binaries never
/// read it.
pub fn min_stall() -> std::time::Duration {
    #[cfg(debug_assertions)]
    if let Some(secs) = std::env::var(TEST_MIN_STALL_ENV)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
    {
        return std::time::Duration::from_secs(secs);
    }
    MIN_STALL
}

/// The debug-build override of [`min_stall`] (tests only).
pub const TEST_MIN_STALL_ENV: &str = "FAUCET_FEED_TEST_MIN_STALL_SECS";

/// The settings key of a delivery's status.
pub fn status_key(feed_uid: &str, delivery_uid: &str) -> String {
    format!("feed_status:{feed_uid}:{delivery_uid}")
}

/// How a delivery is doing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Health {
    #[default]
    Ok,
    /// Three or more runs in a row failed; it keeps retrying.
    Failing,
    /// One event kept failing: no run starts until an admin skips it,
    /// replays, or resumes the delivery.
    Stalled,
    /// The store and the delivery's bookmark disagree: no run starts until
    /// an admin recovers it.
    Held,
}

impl Health {
    pub fn as_str(self) -> &'static str {
        match self {
            Health::Ok => "ok",
            Health::Failing => "failing",
            Health::Stalled => "stalled",
            Health::Held => "held",
        }
    }

    /// Whether the scheduler starts runs in this state.
    pub fn runs(self) -> bool {
        matches!(self, Health::Ok | Health::Failing)
    }
}

/// The claim a delivery is working on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveClaim {
    pub id: String,
    pub from: u64,
    pub to: u64,
    /// Set once the run is submitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    pub claimed_at: DateTime<Utc>,
    /// Events were waiting past the claimed range when it was claimed: a
    /// failure of a one-event claim then holds them back.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub blocking: bool,
}

/// How a delivery run ended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LastRun {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    pub claim: String,
    pub from: u64,
    pub to: u64,
    /// `delivered`, `failed`, `refused`, `cancelled` or `claim_lost`.
    pub outcome: String,
    pub at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// A pause set at run time (a cancelled or rolled-back run, an operator's
/// pause of a `--feeds` file delivery).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pause {
    pub at: DateTime<Utc>,
    pub by: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// One delivery's status document.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryStatus {
    /// The current batch size in events; unset = the delivery's `max_batch`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_events: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claim: Option<LiveClaim>,
    #[serde(default)]
    pub health: Health,
    /// Failed runs in a row.
    #[serde(default)]
    pub consecutive_failures: u32,
    /// Failed runs in a row of a single event that held others back.
    #[serde(default)]
    pub failures_at_one: u32,
    /// When the first of those failures happened.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_failure_at_one: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_run: Option<LastRun>,
    /// When a run last moved the delivery's position.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_delivered_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_submit_at: Option<DateTime<Utc>>,
    /// No run starts before this (a back-off after a failure or a refusal).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backoff_until: Option<DateTime<Utc>>,
    #[serde(default)]
    pub backoff_secs: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paused: Option<Pause>,
    /// Why runs wait for an approval, when they do.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub needs_approval: Option<String>,
    /// An operator asked for a run now: the next claim does not wait for the
    /// flush policy.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub deliver_now: bool,
}

/// How a claimed range ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The cursor moved: the events are delivered, whatever the run said
    /// afterwards.
    Delivered,
    /// The run failed, or was refused for a reason a retry cannot fix,
    /// without moving the cursor.
    Failed(String),
    /// The bookmark and the store disagree.
    Held(String),
    /// The submit was refused for a reason that passes with time.
    Transient(String),
    /// The run's claim was replaced or expired; not a delivery failure.
    ClaimLost,
    /// The run was cancelled.
    Cancelled,
}

impl Outcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            Outcome::Delivered => "delivered",
            Outcome::Failed(_) => "failed",
            Outcome::Held(_) => "held",
            Outcome::Transient(_) => "refused",
            Outcome::ClaimLost => "claim_lost",
            Outcome::Cancelled => "cancelled",
        }
    }
}

/// The batch size after a failure: half, never below one event.
pub fn halved(current: u64) -> u64 {
    (current / 2).max(1)
}

/// The batch size after a success: double, never above `max`.
pub fn doubled(current: u64, max: u64) -> u64 {
    current.saturating_mul(2).min(max.max(1))
}

impl DeliveryStatus {
    /// The batch size to claim next, within `max` events.
    pub fn batch(&self, max: u64) -> u64 {
        self.batch_events.unwrap_or(max).clamp(1, max.max(1))
    }

    /// Whether a back-off is still running at `now`.
    pub fn backing_off(&self, now: DateTime<Utc>) -> bool {
        self.backoff_until.is_some_and(|t| t > now)
    }

    fn back_off(&mut self, now: DateTime<Utc>) {
        self.backoff_secs = match self.backoff_secs {
            0 => FIRST_BACKOFF_SECS,
            n => n.saturating_mul(2).min(MAX_BACKOFF_SECS),
        };
        self.backoff_until = Some(now + chrono::Duration::seconds(self.backoff_secs as i64));
    }

    /// Record that `claim` was taken.
    pub fn claimed(&mut self, claim: LiveClaim) {
        self.claim = Some(claim);
        self.deliver_now = false;
    }

    /// Record the run submitted for `claim`.
    pub fn submitted(&mut self, claim: &str, run_id: &str, now: DateTime<Utc>) {
        if let Some(c) = self.claim.as_mut().filter(|c| c.id == claim) {
            c.run_id = Some(run_id.to_string());
        }
        self.last_submit_at = Some(now);
        self.needs_approval = None;
    }

    /// Fold the end of `claim` in: batch size, failure counts, health and
    /// back-off. `max_events` is the delivery's `max_batch.events`, `retries`
    /// its stall threshold. A claim already recorded changes nothing.
    pub fn finish(
        &mut self,
        claim: &LiveClaim,
        outcome: &Outcome,
        max_events: u64,
        retries: u32,
        now: DateTime<Utc>,
    ) -> bool {
        if self.last_run.as_ref().is_some_and(|r| r.claim == claim.id) {
            if self.claim.as_ref().is_some_and(|c| c.id == claim.id) {
                self.claim = None;
                return true;
            }
            return false;
        }
        let size = claim.to.saturating_sub(claim.from).max(1);
        let error = match outcome {
            Outcome::Failed(e) | Outcome::Held(e) | Outcome::Transient(e) => Some(e.clone()),
            _ => None,
        };
        self.last_run = Some(LastRun {
            run_id: claim.run_id.clone(),
            claim: claim.id.clone(),
            from: claim.from,
            to: claim.to,
            outcome: outcome.as_str().to_string(),
            at: now,
            error: error.clone(),
        });
        if self.claim.as_ref().is_some_and(|c| c.id == claim.id) {
            self.claim = None;
        }
        match outcome {
            Outcome::Delivered => {
                self.batch_events = Some(doubled(size.max(self.batch(max_events)), max_events));
                if self.batch_events == Some(max_events.max(1)) {
                    self.batch_events = None;
                }
                self.consecutive_failures = 0;
                self.failures_at_one = 0;
                self.first_failure_at_one = None;
                self.health = Health::Ok;
                self.last_error = None;
                self.backoff_secs = 0;
                self.backoff_until = None;
                self.last_delivered_at = Some(now);
            }
            Outcome::Failed(e) => {
                self.consecutive_failures = self.consecutive_failures.saturating_add(1);
                if size == 1 && claim.blocking {
                    self.failures_at_one = self.failures_at_one.saturating_add(1);
                    self.first_failure_at_one.get_or_insert(now);
                } else if size > 1 {
                    self.failures_at_one = 0;
                    self.first_failure_at_one = None;
                }
                self.batch_events = Some(halved(size));
                self.last_error = Some(e.clone());
                let long_enough = self
                    .first_failure_at_one
                    .is_some_and(|first| (now - first).to_std().unwrap_or_default() >= min_stall());
                self.health = if self.failures_at_one >= retries.max(1) && long_enough {
                    Health::Stalled
                } else if self.consecutive_failures >= FAILING_AFTER {
                    Health::Failing
                } else {
                    self.health
                };
                self.back_off(now);
            }
            Outcome::Held(e) => {
                self.health = Health::Held;
                self.last_error = Some(e.clone());
            }
            Outcome::Transient(e) => {
                self.last_error = Some(e.clone());
                self.back_off(now);
            }
            Outcome::ClaimLost | Outcome::Cancelled => {}
        }
        true
    }

    /// Start over after an operator acted (resume, deliver now, a cursor
    /// move): failure counts, back-off and the stalled or held state go.
    pub fn reset(&mut self) {
        self.health = Health::Ok;
        self.consecutive_failures = 0;
        self.failures_at_one = 0;
        self.first_failure_at_one = None;
        self.last_error = None;
        self.backoff_secs = 0;
        self.backoff_until = None;
    }
}

/// Read a delivery's status (`Default` when absent). `Err` when the history
/// cannot be read: the caller holds.
pub async fn read(
    history: &dyn RunHistory,
    feed_uid: &str,
    delivery_uid: &str,
) -> Result<DeliveryStatus, String> {
    let stored = history
        .meta_get(&status_key(feed_uid, delivery_uid))
        .await
        .map_err(|e| e.to_string())?;
    Ok(stored
        .as_deref()
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or_default())
}

/// Apply `change` with a compare-and-set loop and return the stored result.
/// `change` returns whether it changed anything worth writing.
pub async fn update(
    history: &dyn RunHistory,
    feed_uid: &str,
    delivery_uid: &str,
    change: impl Fn(&mut DeliveryStatus) -> bool,
) -> Result<DeliveryStatus, String> {
    let key = status_key(feed_uid, delivery_uid);
    for _ in 0..ATTEMPTS {
        let current = history.meta_get(&key).await.map_err(|e| e.to_string())?;
        let mut doc: DeliveryStatus = current
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default();
        if !change(&mut doc) {
            return Ok(doc);
        }
        let encoded = serde_json::to_string(&doc).map_err(|e| e.to_string())?;
        if current.as_deref() == Some(encoded.as_str()) {
            return Ok(doc);
        }
        if history
            .meta_compare_and_set(&key, current.as_deref(), &encoded)
            .await
            .map_err(|e| e.to_string())?
        {
            return Ok(doc);
        }
    }
    Err("the delivery's status kept changing".into())
}

/// Clear a deleted delivery's status. `Err`: the history refused the write.
pub async fn clear(
    history: &dyn RunHistory,
    feed_uid: &str,
    delivery_uid: &str,
) -> Result<(), String> {
    history
        .meta_put(&status_key(feed_uid, delivery_uid), "")
        .await
        .map_err(|e| e.to_string())
}

/// Forget a deleted delivery's status.
pub async fn forget(history: &dyn RunHistory, feed_uid: &str, delivery_uid: &str) {
    if let Err(e) = clear(history, feed_uid, delivery_uid).await {
        tracing::warn!(feed = feed_uid, delivery = delivery_uid, error = %e, "could not clear a deleted delivery's status");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::history::memory::MemoryHistory;

    fn claim(id: &str, from: u64, to: u64) -> LiveClaim {
        LiveClaim {
            id: id.into(),
            from,
            to,
            run_id: Some(format!("run-{id}")),
            claimed_at: Utc::now(),
            blocking: true,
        }
    }

    fn lone(id: &str) -> LiveClaim {
        LiveClaim {
            blocking: false,
            ..claim(id, 1, 2)
        }
    }

    #[test]
    fn a_lone_failing_event_retries_and_never_stalls() {
        let start = Utc::now();
        let mut s = DeliveryStatus::default();
        for i in 0..20 {
            s.finish(
                &lone(&format!("l{i}")),
                &Outcome::Failed("destination down".into()),
                8,
                3,
                start + chrono::Duration::minutes(i),
            );
            assert_ne!(s.health, Health::Stalled, "attempt {i}");
        }
        assert_eq!(s.health, Health::Failing);
        assert_eq!((s.failures_at_one, s.first_failure_at_one), (0, None));
        assert!(s.health.runs());
    }

    #[test]
    fn a_blocking_event_stalls_only_after_failing_for_the_minimum_time() {
        let start = Utc::now();
        let mut s = DeliveryStatus::default();
        let fail = |s: &mut DeliveryStatus, id: &str, at: DateTime<Utc>| {
            s.finish(&claim(id, 1, 2), &Outcome::Failed("x".into()), 8, 3, at);
        };
        for (i, minutes) in [0, 1, 2, 5, 9].into_iter().enumerate() {
            fail(
                &mut s,
                &format!("b{i}"),
                start + chrono::Duration::minutes(minutes),
            );
            assert_ne!(s.health, Health::Stalled, "{minutes} min: a short outage");
        }
        assert_eq!(s.failures_at_one, 5);
        assert_eq!(s.first_failure_at_one, Some(start));
        // A lone failure in between neither counts nor restarts the clock.
        s.finish(&lone("gap"), &Outcome::Failed("x".into()), 8, 3, start);
        assert_eq!(s.failures_at_one, 5);
        fail(&mut s, "late", start + chrono::Duration::minutes(10));
        assert_eq!(s.health, Health::Stalled);
        // Delivering, or a larger batch failing, starts the count again.
        s.reset();
        assert_eq!((s.failures_at_one, s.first_failure_at_one), (0, None));
        fail(&mut s, "again", start + chrono::Duration::minutes(30));
        s.finish(
            &claim("big", 1, 5),
            &Outcome::Failed("x".into()),
            8,
            3,
            start,
        );
        assert_eq!((s.failures_at_one, s.first_failure_at_one), (0, None));
    }

    #[test]
    fn the_batch_halves_on_failure_and_doubles_back_on_success() {
        assert_eq!(halved(10_000), 5_000);
        assert_eq!(halved(1), 1);
        assert_eq!(doubled(4, 10), 8);
        assert_eq!(doubled(8, 10), 10);
        assert_eq!(doubled(u64::MAX, u64::MAX), u64::MAX);
        let now = Utc::now();
        let mut s = DeliveryStatus::default();
        assert_eq!(s.batch(1_000), 1_000);
        assert!(s.finish(
            &claim("a", 1, 1_001),
            &Outcome::Failed("boom".into()),
            1_000,
            5,
            now
        ));
        assert_eq!(s.batch(1_000), 500);
        assert_eq!((s.consecutive_failures, s.failures_at_one), (1, 0));
        assert_eq!(s.health, Health::Ok);
        assert!(s.backing_off(now));
        assert_eq!(s.backoff_secs, FIRST_BACKOFF_SECS);
        assert!(!s.backing_off(now + chrono::Duration::seconds(6)));
        assert_eq!(s.last_run.as_ref().unwrap().outcome, "failed");
        // The same claim reported twice (two replicas) counts once.
        assert!(!s.finish(
            &claim("a", 1, 1_001),
            &Outcome::Failed("boom".into()),
            1_000,
            5,
            now
        ));
        assert_eq!(s.consecutive_failures, 1);
        s.finish(
            &claim("b", 1, 501),
            &Outcome::Failed("boom".into()),
            1_000,
            5,
            now,
        );
        s.finish(
            &claim("c", 1, 251),
            &Outcome::Failed("boom".into()),
            1_000,
            5,
            now,
        );
        assert_eq!(s.health, Health::Failing);
        assert_eq!(s.batch(1_000), 125);
        assert_eq!(s.backoff_secs, 20);
        assert!(s.health.runs());
        // A success doubles back toward the maximum and clears the failures.
        s.finish(&claim("d", 1, 126), &Outcome::Delivered, 1_000, 5, now);
        assert_eq!(s.batch(1_000), 250);
        assert_eq!((s.health, s.consecutive_failures), (Health::Ok, 0));
        assert!(!s.backing_off(now));
        s.finish(&claim("e", 126, 376), &Outcome::Delivered, 1_000, 5, now);
        s.finish(&claim("f", 376, 876), &Outcome::Delivered, 1_000, 5, now);
        assert_eq!(s.batch_events, None, "back at the maximum");
        assert_eq!(s.batch(1_000), 1_000);
    }

    #[test]
    fn a_delivery_stalls_only_after_retries_failures_of_one_event() {
        let now = Utc::now();
        let mut s = DeliveryStatus::default();
        let mut n = 0;
        let mut size = 8;
        while size > 1 {
            n += 1;
            s.finish(
                &claim(&format!("h{n}"), 1, 1 + size),
                &Outcome::Failed("x".into()),
                8,
                3,
                now,
            );
            size = s.batch(8);
            assert_ne!(s.health, Health::Stalled);
        }
        assert_eq!(s.batch(8), 1);
        for i in 0..2 {
            s.finish(
                &claim(&format!("one{i}"), 1, 2),
                &Outcome::Failed("x".into()),
                8,
                3,
                now,
            );
            assert_eq!(s.health, Health::Failing);
        }
        s.finish(
            &claim("one2", 1, 2),
            &Outcome::Failed("poison".into()),
            8,
            3,
            now,
        );
        assert_eq!(s.health, Health::Failing, "not failing long enough yet");
        s.finish(
            &claim("one3", 1, 2),
            &Outcome::Failed("poison".into()),
            8,
            3,
            now + chrono::Duration::from_std(MIN_STALL).unwrap(),
        );
        assert_eq!(s.health, Health::Stalled);
        assert!(!s.health.runs());
        assert_eq!(s.failures_at_one, 4);
        assert_eq!(s.last_error.as_deref(), Some("poison"));
        assert_eq!(s.backoff_secs, MAX_BACKOFF_SECS);
        s.reset();
        assert_eq!(
            (s.health, s.failures_at_one, s.backoff_secs),
            (Health::Ok, 0, 0)
        );
        assert_eq!(s.batch(8), 1, "the batch size stays");
    }

    #[test]
    fn refusals_lost_claims_cancels_and_holds_do_not_count_as_failures() {
        let now = Utc::now();
        let mut s = DeliveryStatus::default();
        s.claimed(claim("t", 1, 11));
        s.submitted("t", "run-9", now);
        assert_eq!(s.claim.as_ref().unwrap().run_id.as_deref(), Some("run-9"));
        assert_eq!(s.last_submit_at, Some(now));
        s.finish(
            &claim("t", 1, 11),
            &Outcome::Transient("queue full".into()),
            100,
            5,
            now,
        );
        assert_eq!((s.consecutive_failures, s.batch(100)), (0, 100));
        assert!(s.backing_off(now) && s.claim.is_none());
        assert_eq!(s.last_run.as_ref().unwrap().outcome, "refused");
        s.finish(&claim("u", 1, 11), &Outcome::ClaimLost, 100, 5, now);
        s.finish(&claim("v", 1, 11), &Outcome::Cancelled, 100, 5, now);
        assert_eq!((s.consecutive_failures, s.health), (0, Health::Ok));
        assert_eq!(s.last_run.as_ref().unwrap().outcome, "cancelled");
        s.finish(
            &claim("w", 1, 11),
            &Outcome::Held("is held".into()),
            100,
            5,
            now,
        );
        assert_eq!(s.health, Health::Held);
        assert!(!s.health.runs());
        for (h, text) in [
            (Health::Ok, "ok"),
            (Health::Failing, "failing"),
            (Health::Stalled, "stalled"),
            (Health::Held, "held"),
        ] {
            assert_eq!(h.as_str(), text);
        }
        assert_eq!(Outcome::ClaimLost.as_str(), "claim_lost");
        assert_eq!(Outcome::Held(String::new()).as_str(), "held");
        // A recorded claim that is still listed as live is only cleared.
        let mut again = DeliveryStatus::default();
        again.finish(&claim("z", 1, 3), &Outcome::Delivered, 10, 5, now);
        again.claimed(claim("z", 1, 3));
        assert!(again.finish(
            &claim("z", 1, 3),
            &Outcome::Failed("late".into()),
            10,
            5,
            now
        ));
        assert!(again.claim.is_none());
        assert_eq!(again.consecutive_failures, 0);
    }

    #[tokio::test]
    async fn the_status_round_trips_through_the_history() {
        let h = MemoryHistory::new(std::time::Duration::from_secs(60));
        assert_eq!(status_key("f", "d"), "feed_status:f:d");
        assert_eq!(read(&h, "f", "d").await.unwrap(), DeliveryStatus::default());
        let now = Utc::now();
        let stored = update(&h, "f", "d", |s| {
            s.claimed(claim("a", 5, 9));
            true
        })
        .await
        .unwrap();
        assert_eq!(stored.claim.as_ref().unwrap().to, 9);
        let same = update(&h, "f", "d", |_| false).await.unwrap();
        assert_eq!(same, stored);
        let unchanged = update(&h, "f", "d", |_| true).await.unwrap();
        assert_eq!(unchanged, stored);
        update(&h, "f", "d", |s| {
            s.paused = Some(Pause {
                at: now,
                by: "ops".into(),
                reason: Some("why".into()),
            });
            true
        })
        .await
        .unwrap();
        let back = read(&h, "f", "d").await.unwrap();
        assert_eq!(back.paused.as_ref().unwrap().by, "ops");
        forget(&h, "f", "d").await;
        assert_eq!(read(&h, "f", "d").await.unwrap(), DeliveryStatus::default());
    }
}
