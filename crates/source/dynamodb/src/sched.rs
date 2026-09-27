//! Rotating shard scheduler. Pure.
//!
//! At most `concurrency` shards are read at once, but every readable shard
//! gets turns: a worker reads one bounded slice of a shard, hands the shard
//! back (with its iterator and position) and the shard rejoins the back of
//! the queue. A shard that was idle or throttled rejoins with a
//! `not_before` so it doesn't spin. Parent-before-child ordering comes from
//! the [`Planner`]: a child is admitted only once its parent has drained.

use crate::lineage::{Planner, StartAt};
use std::collections::{BTreeSet, VecDeque};
use std::time::Instant;

/// A shard handed to a worker for one slice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    /// Shard id.
    pub shard_id: String,
    /// Iterator to continue from; `None` means acquire one at `position`.
    pub iterator: Option<String>,
    /// Where to (re)acquire an iterator: after the last emitted sequence, or
    /// the shard's start position.
    pub position: StartAt,
    /// Consecutive throttled reads (drives the backoff).
    pub throttles: u32,
}

impl Lease {
    /// A shard that has not been read yet.
    pub fn new(shard_id: impl Into<String>, position: StartAt) -> Self {
        Self {
            shard_id: shard_id.into(),
            iterator: None,
            position,
            throttles: 0,
        }
    }
}

/// Work queue over the readable shards.
#[derive(Debug)]
pub struct Scheduler {
    planner: Planner,
    queue: VecDeque<(Lease, Instant)>,
    active: BTreeSet<String>,
    concurrency: usize,
}

impl Scheduler {
    /// A scheduler running at most `concurrency` slices at once.
    pub fn new(planner: Planner, concurrency: usize) -> Self {
        Self {
            planner,
            queue: VecDeque::new(),
            active: BTreeSet::new(),
            concurrency: concurrency.max(1),
        }
    }

    /// The lineage planner (for `parent_known`, `has_children`, `merge`).
    pub fn planner(&mut self) -> &mut Planner {
        &mut self.planner
    }

    /// Shards whose parents have drained and that were never admitted.
    pub fn ready(&self) -> Vec<String> {
        self.planner.ready()
    }

    /// Admit a ready shard to the queue.
    pub fn admit(&mut self, shard_id: &str, start: StartAt, now: Instant) {
        self.planner.start(shard_id);
        self.queue.push_back((Lease::new(shard_id, start), now));
    }

    /// Hand out leases while slots are free, in queue order, skipping shards
    /// that are not due yet.
    pub fn dispatch(&mut self, now: Instant) -> Vec<Lease> {
        let mut out = Vec::new();
        while self.active.len() < self.concurrency {
            let Some(pos) = self.queue.iter().position(|(_, due)| *due <= now) else {
                break;
            };
            let (lease, _) = self.queue.remove(pos).expect("position is in range");
            self.active.insert(lease.shard_id.clone());
            out.push(lease);
        }
        out
    }

    /// A slice ended with the shard still open: requeue it at the back.
    pub fn yielded(&mut self, lease: Lease, not_before: Instant) {
        self.active.remove(&lease.shard_id);
        self.queue.push_back((lease, not_before));
    }

    /// A closed shard drained: release its slot and unblock its children.
    pub fn finished(&mut self, shard_id: &str) {
        self.active.remove(shard_id);
        self.planner.finish(shard_id);
    }

    /// When the next queued shard becomes due, if a slot is free for it.
    pub fn next_wakeup(&self) -> Option<Instant> {
        if self.active.len() >= self.concurrency {
            return None;
        }
        self.queue.iter().map(|(_, due)| *due).min()
    }

    /// Nothing running and nothing queued.
    pub fn is_idle(&self) -> bool {
        self.active.is_empty() && self.queue.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lineage::ShardInfo;
    use std::collections::BTreeMap;
    use std::time::Duration;

    fn shard(id: &str, parent: Option<&str>) -> ShardInfo {
        ShardInfo {
            id: id.into(),
            parent: parent.map(str::to_string),
        }
    }

    fn admit_ready(s: &mut Scheduler, now: Instant) {
        for id in s.ready() {
            s.admit(&id, StartAt::TrimHorizon, now);
        }
    }

    #[test]
    fn more_shards_than_workers_all_make_progress() {
        let planner = Planner::new(
            (0..5).map(|i| shard(&format!("s{i}"), None)).collect(),
            &BTreeSet::new(),
        );
        let mut s = Scheduler::new(planner, 2);
        let now = Instant::now();
        admit_ready(&mut s, now);
        let mut turns: BTreeMap<String, usize> = BTreeMap::new();
        for _ in 0..10 {
            let leases = s.dispatch(now);
            assert!(leases.len() <= 2);
            for mut l in leases {
                *turns.entry(l.shard_id.clone()).or_default() += 1;
                l.iterator = Some("it".into());
                s.yielded(l, now);
            }
        }
        assert_eq!(turns.len(), 5, "every shard got a turn: {turns:?}");
        assert!(turns.values().all(|&n| n == 4), "turns are even: {turns:?}");
        assert!(!s.is_idle());
    }

    #[test]
    fn children_are_admitted_only_after_parents_drain() {
        let planner = Planner::new(
            vec![
                shard("p", None),
                shard("c1", Some("p")),
                shard("c2", Some("p")),
            ],
            &BTreeSet::new(),
        );
        let mut s = Scheduler::new(planner, 1);
        let now = Instant::now();
        admit_ready(&mut s, now);
        assert!(s.ready().is_empty());
        let lease = s.dispatch(now).pop().unwrap();
        assert_eq!(lease.shard_id, "p");
        assert!(s.planner().has_children("p"));
        s.yielded(lease, now);
        assert!(
            s.ready().is_empty(),
            "an open parent keeps children waiting"
        );
        let lease = s.dispatch(now).pop().unwrap();
        s.finished(&lease.shard_id);
        assert_eq!(s.ready(), vec!["c1", "c2"]);
        admit_ready(&mut s, now);
        assert_eq!(s.dispatch(now)[0].shard_id, "c1");
    }

    #[test]
    fn deferred_shards_wait_and_wakeups_follow_free_slots() {
        let planner = Planner::new(vec![shard("a", None), shard("b", None)], &BTreeSet::new());
        let mut s = Scheduler::new(planner, 1);
        let now = Instant::now();
        let later = now + Duration::from_secs(1);
        s.admit("a", StartAt::Latest, later);
        s.admit("b", StartAt::Latest, now);
        let got = s.dispatch(now);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].shard_id, "b", "a is not due yet");
        assert_eq!(s.next_wakeup(), None, "no free slot");
        s.finished("b");
        assert_eq!(s.next_wakeup(), Some(later));
        assert!(s.dispatch(now).is_empty());
        let a = s.dispatch(later).pop().unwrap();
        assert_eq!(a, Lease::new("a", StartAt::Latest));
        s.finished("a");
        assert!(s.is_idle());
        assert_eq!(s.next_wakeup(), None);
        assert_eq!(Scheduler::new(Planner::default(), 0).concurrency, 1);
    }
}
