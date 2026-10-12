//! Durable bookmark and the partition scheduler.
//!
//! A Spanner change stream is a tree of *partitions*. A partition ends by
//! announcing child partitions (a split gives one parent several children, a
//! merge gives one child several parents). To preserve per-key ordering, a
//! child must not be read until **every** parent has been read to its end.
//!
//! [`Scheduler`] holds that frontier: the partitions that have not ended yet,
//! the position each one is read from, and which parents a not-yet-started
//! child still waits for. It is pure (no I/O), so the ordering rules are unit
//! tested here. The bookmark is a snapshot of the frontier:
//!
//! ```json
//! {
//!   "watermark": "2026-10-12T08:05:25Z",
//!   "partitions": [
//!     { "token": "AbC...", "read_from": "2026-10-12T08:05:25Z" },
//!     { "token": "DeF...", "read_from": "2026-10-12T08:05:20Z", "waiting_on": ["AbC..."] }
//!   ]
//! }
//! ```
//!
//! The synthetic root partition (`token: null`) is the query with no partition
//! token; it only returns the initial children and appears in a bookmark solely
//! on a fresh run that has not read it yet.

use chrono::{DateTime, Utc};
use faucet_core::FaucetError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeSet, HashSet};

use crate::record::ChildPartition;

/// The bookmark shape version.
pub const STATE_SCHEMA: u32 = 1;

/// Bring a bookmark stored at shape version `from` up to [`STATE_SCHEMA`].
pub fn migrate_state(from: u32, data: Value) -> Result<Value, FaucetError> {
    match from {
        STATE_SCHEMA => Ok(data),
        other => Err(FaucetError::State(format!(
            "spanner-cdc has no bookmark schema {other} (this release reads up to {STATE_SCHEMA})"
        ))),
    }
}

/// One unfinished partition in a [`Bookmark`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PartitionState {
    /// Partition token; `None` is the synthetic root.
    pub token: Option<String>,
    /// Resume position (inclusive): everything earlier has been delivered.
    pub read_from: DateTime<Utc>,
    /// Parent partitions that must finish before this one starts.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub waiting_on: Vec<String>,
}

/// Durable resume state: the frontier of unfinished partitions.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Bookmark {
    /// Everything before this time has been delivered on every partition
    /// (the oldest `read_from` on the frontier). Drives `Source::lag`.
    pub watermark: DateTime<Utc>,
    /// The unfinished partitions.
    pub partitions: Vec<PartitionState>,
}

impl Bookmark {
    /// Parse a bookmark previously emitted by [`to_value`](Self::to_value).
    pub fn from_value(v: Value) -> Result<Self, FaucetError> {
        serde_json::from_value(v)
            .map_err(|e| FaucetError::State(format!("spanner-cdc bookmark parse: {e}")))
    }

    /// Serialize for the state store.
    pub fn to_value(&self) -> Result<Value, FaucetError> {
        serde_json::to_value(self)
            .map_err(|e| FaucetError::State(format!("spanner-cdc bookmark serialize: {e}")))
    }

    /// A bookmark holding only the unread root partition, starting at `start`.
    pub fn root_at(start: DateTime<Utc>) -> Self {
        Self {
            watermark: start,
            partitions: vec![PartitionState {
                token: None,
                read_from: start,
                waiting_on: Vec::new(),
            }],
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    /// Not read yet this run.
    Idle,
    /// A reader is running.
    Running,
    /// Read up to the end of this run; nothing more to do until the next run.
    Caught,
}

#[derive(Clone, Debug)]
struct Part {
    token: Option<String>,
    read_from: DateTime<Utc>,
    waiting_on: BTreeSet<String>,
    phase: Phase,
}

/// A partition handed out for reading.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Assignment {
    pub token: Option<String>,
    pub read_from: DateTime<Utc>,
}

/// The frontier of unfinished partitions for one run.
#[derive(Debug)]
pub(crate) struct Scheduler {
    parts: Vec<Part>,
    /// Partitions that ended during this run (so a child listing them as a
    /// parent does not wait for them).
    finished: HashSet<String>,
    run_end: DateTime<Utc>,
}

impl Scheduler {
    /// Rebuild the frontier from a persisted bookmark.
    pub fn from_bookmark(bm: &Bookmark, run_end: DateTime<Utc>) -> Self {
        let parts = bm
            .partitions
            .iter()
            .map(|p| Part {
                token: p.token.clone(),
                read_from: p.read_from,
                waiting_on: p.waiting_on.iter().cloned().collect(),
                phase: Phase::Idle,
            })
            .collect();
        Self {
            parts,
            finished: HashSet::new(),
            run_end,
        }
    }

    fn find(&mut self, token: &Option<String>) -> Option<&mut Part> {
        self.parts.iter_mut().find(|p| &p.token == token)
    }

    /// Next partition that may be read now: not yet read this run, not waiting
    /// on a parent, and with something left to read. A partition already at
    /// (or past) the end of the run is marked caught up without a read.
    pub fn next_ready(&mut self) -> Option<Assignment> {
        let run_end = self.run_end;
        for p in &mut self.parts {
            if p.phase != Phase::Idle || !p.waiting_on.is_empty() {
                continue;
            }
            if p.read_from >= run_end {
                p.phase = Phase::Caught;
                continue;
            }
            p.phase = Phase::Running;
            return Some(Assignment {
                token: p.token.clone(),
                read_from: p.read_from,
            });
        }
        None
    }

    /// Number of readers currently running.
    pub fn running(&self) -> usize {
        self.parts
            .iter()
            .filter(|p| p.phase == Phase::Running)
            .count()
    }

    /// Advance a partition's resume position (never backwards, never past the
    /// end of this run).
    pub fn advance(&mut self, token: &Option<String>, to: DateTime<Utc>) {
        let run_end = self.run_end;
        if let Some(p) = self.find(token) {
            let to = to.min(run_end);
            if to > p.read_from {
                p.read_from = to;
            }
        }
    }

    /// The partition's read ended cleanly at the end of the run.
    pub fn caught_up(&mut self, token: &Option<String>) {
        let run_end = self.run_end;
        if let Some(p) = self.find(token) {
            p.read_from = p.read_from.max(run_end);
            p.phase = Phase::Caught;
        }
    }

    /// The partition's read stopped early (idle timeout): keep its progress and
    /// resume it next run.
    pub fn stalled(&mut self, token: &Option<String>) {
        if let Some(p) = self.find(token) {
            p.phase = Phase::Caught;
        }
    }

    /// The partition ended and announced `children`. The partition leaves the
    /// frontier; each child joins it (or, if already known, stops waiting for
    /// this parent).
    pub fn ended(&mut self, token: &Option<String>, children: &[ChildPartition]) {
        self.parts.retain(|p| &p.token != token);
        if let Some(t) = token {
            self.finished.insert(t.clone());
            for p in &mut self.parts {
                p.waiting_on.remove(t);
            }
        }
        for c in children {
            let already = self
                .parts
                .iter()
                .any(|p| p.token.as_deref() == Some(&c.token));
            if already {
                continue;
            }
            let waiting_on: BTreeSet<String> = c
                .parents
                .iter()
                .filter(|p| !self.finished.contains(*p))
                .cloned()
                .collect();
            self.parts.push(Part {
                token: Some(c.token.clone()),
                read_from: c.start,
                waiting_on,
                phase: Phase::Idle,
            });
        }
    }

    /// Snapshot of the frontier for persistence. Partitions are sorted by token
    /// so the same frontier always serializes the same way.
    pub fn snapshot(&self) -> Bookmark {
        let mut partitions: Vec<PartitionState> = self
            .parts
            .iter()
            .map(|p| PartitionState {
                token: p.token.clone(),
                read_from: p.read_from,
                waiting_on: p.waiting_on.iter().cloned().collect(),
            })
            .collect();
        partitions.sort_by(|a, b| a.token.cmp(&b.token));
        let active_min = self
            .parts
            .iter()
            .filter(|p| p.waiting_on.is_empty())
            .map(|p| p.read_from)
            .min();
        let any_min = self.parts.iter().map(|p| p.read_from).min();
        Bookmark {
            watermark: active_min.or(any_min).unwrap_or(self.run_end),
            partitions,
        }
    }

    /// Whether the frontier holds a partition with this token.
    #[cfg(test)]
    pub fn contains(&self, token: &str) -> bool {
        self.parts.iter().any(|p| p.token.as_deref() == Some(token))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn t(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000 + secs, 0).unwrap()
    }

    fn tok(s: &str) -> Option<String> {
        Some(s.to_string())
    }

    fn child(token: &str, parents: &[&str], start: i64) -> ChildPartition {
        ChildPartition {
            token: token.into(),
            parents: parents.iter().map(|s| s.to_string()).collect(),
            start: t(start),
        }
    }

    fn fresh(run_end: i64) -> Scheduler {
        Scheduler::from_bookmark(&Bookmark::root_at(t(0)), t(run_end))
    }

    #[test]
    fn fresh_run_reads_only_the_root_first() {
        let mut s = fresh(100);
        let a = s.next_ready().unwrap();
        assert_eq!(a.token, None);
        assert_eq!(a.read_from, t(0));
        assert!(s.next_ready().is_none());
        assert_eq!(s.running(), 1);
    }

    #[test]
    fn root_children_are_ready_at_once() {
        let mut s = fresh(100);
        s.next_ready();
        s.ended(&None, &[child("A", &[], 0), child("B", &[], 0)]);
        assert_eq!(s.running(), 0);
        let a = s.next_ready().unwrap();
        let b = s.next_ready().unwrap();
        assert_eq!(
            [a.token, b.token]
                .into_iter()
                .flatten()
                .collect::<BTreeSet<_>>(),
            ["A", "B"].iter().map(|s| s.to_string()).collect()
        );
        assert!(s.next_ready().is_none());
    }

    #[test]
    fn a_split_child_waits_for_nothing_else_than_its_parent() {
        let mut s = Scheduler::from_bookmark(
            &Bookmark {
                watermark: t(0),
                partitions: vec![PartitionState {
                    token: tok("P"),
                    read_from: t(0),
                    waiting_on: vec![],
                }],
            },
            t(100),
        );
        s.next_ready();
        s.ended(
            &tok("P"),
            &[child("C1", &["P"], 50), child("C2", &["P"], 50)],
        );
        assert!(!s.contains("P"));
        assert_eq!(s.next_ready().unwrap().read_from, t(50));
        assert_eq!(s.next_ready().unwrap().read_from, t(50));
    }

    #[test]
    fn a_merged_child_waits_for_every_parent() {
        let bm = Bookmark {
            watermark: t(0),
            partitions: vec![
                PartitionState {
                    token: tok("P"),
                    read_from: t(0),
                    waiting_on: vec![],
                },
                PartitionState {
                    token: tok("Q"),
                    read_from: t(0),
                    waiting_on: vec![],
                },
            ],
        };
        let mut s = Scheduler::from_bookmark(&bm, t(100));
        s.next_ready();
        s.next_ready();
        // P ends first and announces the merge child M (parents P and Q).
        s.ended(&tok("P"), &[child("M", &["P", "Q"], 60)]);
        assert!(s.contains("M"));
        assert!(s.next_ready().is_none(), "M must wait for Q");
        // Q ends and announces the same child: it is now free.
        s.ended(&tok("Q"), &[child("M", &["P", "Q"], 60)]);
        let a = s.next_ready().unwrap();
        assert_eq!(a.token, tok("M"));
        assert_eq!(a.read_from, t(60));
    }

    #[test]
    fn a_child_of_a_not_yet_discovered_parent_waits_for_it() {
        // M lists Q as a parent although Q has not even been registered yet
        // (Q's own parent G is still being read): M must wait, not start.
        let bm = Bookmark {
            watermark: t(0),
            partitions: vec![
                PartitionState {
                    token: tok("P"),
                    read_from: t(0),
                    waiting_on: vec![],
                },
                PartitionState {
                    token: tok("G"),
                    read_from: t(0),
                    waiting_on: vec![],
                },
            ],
        };
        let mut s = Scheduler::from_bookmark(&bm, t(100));
        s.next_ready();
        s.next_ready();
        s.ended(&tok("P"), &[child("M", &["P", "Q"], 60)]);
        assert!(s.next_ready().is_none());
        // G splits into Q (and another); Q then ends in a merge into M.
        s.ended(&tok("G"), &[child("Q", &["G"], 40)]);
        assert_eq!(s.next_ready().unwrap().token, tok("Q"));
        s.ended(&tok("Q"), &[child("M", &["P", "Q"], 60)]);
        assert_eq!(s.next_ready().unwrap().token, tok("M"));
    }

    #[test]
    fn waiting_children_survive_a_bookmark_round_trip() {
        let bm = Bookmark {
            watermark: t(0),
            partitions: vec![
                PartitionState {
                    token: tok("Q"),
                    read_from: t(10),
                    waiting_on: vec![],
                },
                PartitionState {
                    token: tok("M"),
                    read_from: t(60),
                    waiting_on: vec!["Q".into()],
                },
            ],
        };
        let json = bm.to_value().unwrap();
        let back = Bookmark::from_value(json).unwrap();
        assert_eq!(back, bm);
        let mut s = Scheduler::from_bookmark(&back, t(100));
        assert_eq!(s.next_ready().unwrap().token, tok("Q"));
        assert!(s.next_ready().is_none());
        s.ended(&tok("Q"), &[child("M", &["P", "Q"], 60)]);
        assert_eq!(s.next_ready().unwrap().token, tok("M"));
    }

    #[test]
    fn advance_only_moves_forward_and_stops_at_run_end() {
        let mut s = fresh(100);
        s.next_ready();
        s.advance(&None, t(30));
        s.advance(&None, t(10));
        assert_eq!(s.snapshot().partitions[0].read_from, t(30));
        s.advance(&None, t(500));
        assert_eq!(s.snapshot().partitions[0].read_from, t(100));
    }

    #[test]
    fn caught_up_moves_to_run_end_but_stalled_keeps_progress() {
        let bm = Bookmark {
            watermark: t(0),
            partitions: vec![
                PartitionState {
                    token: tok("A"),
                    read_from: t(0),
                    waiting_on: vec![],
                },
                PartitionState {
                    token: tok("B"),
                    read_from: t(0),
                    waiting_on: vec![],
                },
            ],
        };
        let mut s = Scheduler::from_bookmark(&bm, t(100));
        s.next_ready();
        s.next_ready();
        s.advance(&tok("B"), t(20));
        s.caught_up(&tok("A"));
        s.stalled(&tok("B"));
        let snap = s.snapshot();
        let a = snap
            .partitions
            .iter()
            .find(|p| p.token == tok("A"))
            .unwrap();
        let b = snap
            .partitions
            .iter()
            .find(|p| p.token == tok("B"))
            .unwrap();
        assert_eq!(a.read_from, t(100));
        assert_eq!(b.read_from, t(20));
        assert_eq!(snap.watermark, t(20));
        assert!(
            s.next_ready().is_none(),
            "no partition is read twice per run"
        );
        assert_eq!(s.running(), 0);
    }

    #[test]
    fn a_partition_already_at_run_end_is_not_read() {
        let bm = Bookmark::root_at(t(100));
        let mut s = Scheduler::from_bookmark(&bm, t(100));
        assert!(s.next_ready().is_none());
        assert_eq!(s.running(), 0);
    }

    #[test]
    fn watermark_ignores_waiting_children_unless_nothing_else_is_active() {
        let bm = Bookmark {
            watermark: t(0),
            partitions: vec![
                PartitionState {
                    token: tok("Q"),
                    read_from: t(90),
                    waiting_on: vec![],
                },
                PartitionState {
                    token: tok("M"),
                    read_from: t(60),
                    waiting_on: vec!["Q".into()],
                },
            ],
        };
        let s = Scheduler::from_bookmark(&bm, t(100));
        assert_eq!(s.snapshot().watermark, t(90));
    }

    #[test]
    fn an_empty_frontier_snapshots_at_run_end() {
        let mut s = fresh(100);
        s.next_ready();
        s.ended(&None, &[]);
        let snap = s.snapshot();
        assert!(snap.partitions.is_empty());
        assert_eq!(snap.watermark, t(100));
    }

    #[test]
    fn snapshot_is_deterministic() {
        let mut s = fresh(100);
        s.next_ready();
        s.ended(&None, &[child("B", &[], 0), child("A", &[], 0)]);
        let a = s.snapshot().to_value().unwrap();
        let b = s.snapshot().to_value().unwrap();
        assert_eq!(a, b);
        assert_eq!(s.snapshot().partitions[0].token, tok("A"));
    }

    #[test]
    fn bookmark_json_shape_is_stable() {
        let bm = Bookmark {
            watermark: t(0),
            partitions: vec![PartitionState {
                token: tok("A"),
                read_from: t(5),
                waiting_on: vec![],
            }],
        };
        let v = bm.to_value().unwrap();
        assert_eq!(v["partitions"][0]["token"], "A");
        assert!(v["partitions"][0].get("waiting_on").is_none());
        assert!(v["watermark"].as_str().unwrap().ends_with('Z'));
    }

    #[test]
    fn migrate_state_accepts_current_and_rejects_unknown() {
        let v = serde_json::json!({"watermark": "2026-01-01T00:00:00Z", "partitions": []});
        assert_eq!(migrate_state(STATE_SCHEMA, v.clone()).unwrap(), v);
        assert!(matches!(
            migrate_state(0, v.clone()),
            Err(FaucetError::State(_))
        ));
        assert!(migrate_state(STATE_SCHEMA + 1, v).is_err());
    }

    #[test]
    fn corrupt_bookmark_is_a_typed_state_error() {
        let err = Bookmark::from_value(serde_json::json!({"nope": 1})).unwrap_err();
        assert!(matches!(err, FaucetError::State(_)));
    }
}
