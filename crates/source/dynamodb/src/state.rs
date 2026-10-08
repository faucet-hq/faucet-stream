//! Bookmark formats.
//!
//! **Scan / query** — `{ "total_segments": N, "segments": { "<i>": {"after": <typed key>} | "done" } }`.
//! Mid-run pages carry the cumulative per-segment `LastEvaluatedKey`
//! cursors, so a crash resumes each segment where it stopped. The final page
//! carries an empty map, so the next run re-reads the table from the start (a
//! scan is a snapshot, not an incremental feed).
//!
//! **Streams** — `{ "stream_arn": "…", "shards": { "<shard>": "<seq>" }, "finished": [ … ] }`.
//! Every page carries the cumulative map. An empty sequence string marks a
//! shard that was opened but yielded nothing yet (resumes from its trim
//! horizon, never skipping). `finished` lists closed shards fully drained.
//! `verified_at` holds, for each such empty shard, the unix time it was last
//! known to hold nothing unread (opened, or read to its end at the trim
//! horizon), so a resume after the 24 h retention is reported as a gap.

use crate::config::ReadMode;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

/// Where one scan segment (or the single query cursor) stands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SegmentCursor {
    /// Resume after this `LastEvaluatedKey` (DynamoDB typed JSON).
    After(Value),
    /// Segment fully read.
    Done,
}

/// Scan / query bookmark.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanBookmark {
    /// `TotalSegments` the cursors belong to.
    #[serde(default)]
    pub total_segments: u32,
    /// Segment index → cursor.
    #[serde(default)]
    pub segments: BTreeMap<u32, SegmentCursor>,
}

impl ScanBookmark {
    /// Parse; malformed values are treated as absent (a fresh scan is
    /// at-least-once-safe).
    pub fn from_value(v: &Value) -> Self {
        serde_json::from_value(v.clone()).unwrap_or_default()
    }

    /// Serialize.
    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }

    /// The cursors to resume with for a scan of `total` segments. Cursors from
    /// a different segment count are unusable and dropped (fresh scan).
    pub fn for_total(&self, total: u32) -> BTreeMap<u32, SegmentCursor> {
        if self.total_segments == total {
            self.segments.clone()
        } else {
            if !self.segments.is_empty() {
                tracing::warn!(
                    bookmarked = self.total_segments,
                    configured = total,
                    "dynamodb: scan bookmark was taken with a different segment count; \
                     restarting the scan"
                );
            }
            BTreeMap::new()
        }
    }
}

/// Streams bookmark.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamBookmark {
    /// Stream the positions belong to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_arn: Option<String>,
    /// Shard id → last emitted sequence number (`""` = opened, nothing read).
    #[serde(default)]
    pub shards: BTreeMap<String, String>,
    /// Closed shards fully drained.
    #[serde(default)]
    pub finished: BTreeSet<String>,
    /// Empty-position shard → unix seconds it was last known to hold nothing unread.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub verified_at: BTreeMap<String, i64>,
}

/// Seconds since the unix epoch.
pub(crate) fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

impl StreamBookmark {
    /// Parse; malformed values are treated as absent.
    pub fn from_value(v: &Value) -> Self {
        serde_json::from_value(v.clone()).unwrap_or_default()
    }

    /// Serialize.
    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }

    /// Whether this bookmark records any progress.
    pub fn is_empty(&self) -> bool {
        self.shards.is_empty() && self.finished.is_empty()
    }

    /// Record that a shard was opened (keeps an existing position).
    pub fn open(&mut self, shard: &str) {
        self.open_at(shard, unix_now());
    }

    /// [`open`](Self::open) at an explicit time.
    pub fn open_at(&mut self, shard: &str, now: i64) {
        let seq = self.shards.entry(shard.to_string()).or_default();
        if seq.is_empty() {
            self.verified_at.entry(shard.to_string()).or_insert(now);
        }
    }

    /// An empty-position shard was read to its end: nothing retained is unread.
    pub fn verify_empty(&mut self, shard: &str, now: i64) {
        if self.shards.get(shard).is_some_and(String::is_empty) {
            self.verified_at.insert(shard.to_string(), now);
        }
    }

    /// Record a shard's newest emitted sequence.
    pub fn advance(&mut self, shard: &str, sequence: &str) {
        self.shards.insert(shard.to_string(), sequence.to_string());
        self.verified_at.remove(shard);
    }

    /// Record a closed shard as fully drained.
    pub fn finish(&mut self, shard: &str) {
        self.shards.remove(shard);
        self.verified_at.remove(shard);
        self.finished.insert(shard.to_string());
    }

    /// Drop finished entries no longer needed: keep a finished shard while it
    /// still exists or is the parent of one that does.
    pub fn prune_finished(&mut self, existing: &BTreeSet<String>, parents: &BTreeSet<String>) {
        self.finished
            .retain(|s| existing.contains(s) || parents.contains(s));
    }
}

/// The source's stable state key for a mode.
pub fn state_key(mode: ReadMode, table: &str) -> String {
    match mode {
        ReadMode::Streams => format!("dynamodb-streams:{table}"),
        ReadMode::Scan | ReadMode::Query => format!("dynamodb:{table}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn scan_bookmark_round_trips() {
        let mut b = ScanBookmark {
            total_segments: 2,
            ..Default::default()
        };
        b.segments
            .insert(0, SegmentCursor::After(json!({"pk": {"S": "a"}})));
        b.segments.insert(1, SegmentCursor::Done);
        let v = b.to_value();
        assert_eq!(v["segments"]["1"], "done");
        assert_eq!(v["segments"]["0"]["after"]["pk"]["S"], "a");
        assert_eq!(ScanBookmark::from_value(&v), b);
        assert_eq!(b.for_total(2).len(), 2);
        assert!(b.for_total(4).is_empty());
        assert!(ScanBookmark::default().for_total(3).is_empty());
        assert_eq!(
            ScanBookmark::from_value(&json!("x")),
            ScanBookmark::default()
        );
    }

    #[test]
    fn stream_bookmark_tracks_positions() {
        let mut b = StreamBookmark::default();
        assert!(b.is_empty());
        b.open("s1");
        assert_eq!(b.shards["s1"], "");
        b.advance("s1", "100");
        b.open("s1");
        assert_eq!(b.shards["s1"], "100", "open keeps a position");
        b.finish("s1");
        assert!(b.shards.is_empty() && b.finished.contains("s1"));
        assert!(!b.is_empty());
        b.finish("old");
        b.finish("parent");
        let existing: BTreeSet<String> = ["s1".to_string()].into();
        let parents: BTreeSet<String> = ["parent".to_string()].into();
        b.prune_finished(&existing, &parents);
        assert_eq!(
            b.finished.iter().cloned().collect::<Vec<_>>(),
            vec!["parent", "s1"]
        );
        b.stream_arn = Some("arn:x".into());
        let v = b.to_value();
        assert_eq!(StreamBookmark::from_value(&v), b);
        assert_eq!(
            StreamBookmark::from_value(&json!(3)),
            StreamBookmark::default()
        );
    }

    #[test]
    fn empty_shards_carry_their_verification_time() {
        let mut b = StreamBookmark::default();
        b.open_at("s1", 100);
        b.open_at("s1", 200);
        assert_eq!(b.verified_at["s1"], 100, "re-opening keeps the first time");
        b.verify_empty("s1", 300);
        assert_eq!(b.verified_at["s1"], 300);
        b.verify_empty("unknown", 300);
        assert!(!b.verified_at.contains_key("unknown"));
        let v = b.to_value();
        assert_eq!(v["verified_at"]["s1"], 300);
        assert_eq!(StreamBookmark::from_value(&v), b);
        b.advance("s1", "7");
        assert!(b.verified_at.is_empty());
        b.open_at("s1", 400);
        assert!(b.verified_at.is_empty(), "a positioned shard needs no time");
        b.verify_empty("s1", 500);
        assert!(b.verified_at.is_empty());
        b.open_at("s2", 1);
        b.finish("s2");
        assert!(b.verified_at.is_empty());
        assert!(
            StreamBookmark::default()
                .to_value()
                .get("verified_at")
                .is_none()
        );
    }

    #[test]
    fn state_keys_are_valid() {
        for mode in [ReadMode::Scan, ReadMode::Query, ReadMode::Streams] {
            faucet_core::state::validate_state_key(&state_key(mode, "orders")).unwrap();
        }
        assert_eq!(state_key(ReadMode::Streams, "t"), "dynamodb-streams:t");
        assert_eq!(state_key(ReadMode::Query, "t"), "dynamodb:t");
    }
}
