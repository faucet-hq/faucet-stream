//! Stream shard lineage: parent-before-child scheduling, start positions and
//! trim-horizon gap detection. Pure.

use crate::config::StreamStart;
use crate::state::StreamBookmark;
use std::collections::{BTreeMap, BTreeSet};

/// One shard as reported by `DescribeStream`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardInfo {
    /// Shard id.
    pub id: String,
    /// Parent shard id (split lineage).
    pub parent: Option<String>,
}

/// Where a shard iterator starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartAt {
    /// Just after this sequence number.
    After(String),
    /// Oldest retained record.
    TrimHorizon,
    /// Only new records.
    Latest,
}

/// Choose a shard's start: a recorded sequence wins; an opened-but-empty
/// shard, or a child of a shard we read, starts at its trim horizon (never
/// skipping); otherwise the configured start position applies.
pub fn start_for(bookmarked: Option<&str>, parent_known: bool, start: StreamStart) -> StartAt {
    match bookmarked {
        Some(seq) if !seq.is_empty() => StartAt::After(seq.to_string()),
        Some(_) => StartAt::TrimHorizon,
        None if parent_known => StartAt::TrimHorizon,
        None => match start {
            StreamStart::TrimHorizon => StartAt::TrimHorizon,
            StreamStart::Latest => StartAt::Latest,
        },
    }
}

/// Schedules shards so a child is read only after its parent is drained.
#[derive(Debug, Default)]
pub struct Planner {
    shards: BTreeMap<String, ShardInfo>,
    started: BTreeSet<String>,
    finished: BTreeSet<String>,
}

impl Planner {
    /// Build from described shards and the shards already finished.
    pub fn new(shards: Vec<ShardInfo>, finished: &BTreeSet<String>) -> Self {
        let mut p = Self {
            finished: finished.clone(),
            ..Default::default()
        };
        p.merge(shards);
        p
    }

    /// Add shards discovered by a later `DescribeStream`.
    pub fn merge(&mut self, shards: Vec<ShardInfo>) {
        for s in shards {
            self.shards.insert(s.id.clone(), s);
        }
    }

    /// Whether the shard's parent is one this consumer reads (or has read).
    pub fn parent_known(&self, id: &str) -> bool {
        self.shards
            .get(id)
            .and_then(|s| s.parent.as_deref())
            .is_some_and(|p| self.shards.contains_key(p) || self.finished.contains(p))
    }

    /// Shards ready to start: not started, not finished, parent absent or done.
    pub fn ready(&self) -> Vec<String> {
        self.shards
            .values()
            .filter(|s| !self.started.contains(&s.id) && !self.finished.contains(&s.id))
            .filter(|s| match &s.parent {
                None => true,
                Some(p) => !self.shards.contains_key(p) || self.finished.contains(p),
            })
            .map(|s| s.id.clone())
            .collect()
    }

    /// Mark a shard as being read.
    pub fn start(&mut self, id: &str) {
        self.started.insert(id.to_string());
    }

    /// Mark a shard fully drained.
    pub fn finish(&mut self, id: &str) {
        self.finished.insert(id.to_string());
    }

    /// Whether any known shard lists `id` as its parent.
    pub fn has_children(&self, id: &str) -> bool {
        self.shards
            .values()
            .any(|s| s.parent.as_deref() == Some(id))
    }
}

/// Ids + parent ids of described shards (for bookmark pruning).
pub fn ids_and_parents(shards: &[ShardInfo]) -> (BTreeSet<String>, BTreeSet<String>) {
    let ids = shards.iter().map(|s| s.id.clone()).collect();
    let parents = shards.iter().filter_map(|s| s.parent.clone()).collect();
    (ids, parents)
}

/// Reasons the bookmark can no longer be resumed without losing changes.
/// Empty when resuming is safe (or there is nothing to resume).
pub fn detect_gaps(bm: &StreamBookmark, stream_arn: &str, described: &[ShardInfo]) -> Vec<String> {
    if bm.is_empty() {
        return Vec::new();
    }
    if let Some(prev) = &bm.stream_arn
        && prev != stream_arn
    {
        return vec![format!(
            "the bookmark belongs to stream {prev}, but the table's stream is now {stream_arn}"
        )];
    }
    let (ids, _) = ids_and_parents(described);
    let mut gaps = Vec::new();
    for id in bm.shards.keys() {
        if !ids.contains(id) {
            gaps.push(format!("shard {id} expired before it was fully read"));
        }
    }
    for s in described {
        let known = bm.shards.contains_key(&s.id) || bm.finished.contains(&s.id);
        if known {
            continue;
        }
        if let Some(p) = &s.parent
            && !ids.contains(p)
            && !bm.shards.contains_key(p)
            && !bm.finished.contains(p)
        {
            gaps.push(format!("shard {}'s parent {p} expired unread", s.id));
        }
    }
    gaps
}

/// The resume position `capture_resume_position` returns: every current
/// shard opened at its trim horizon. Replaying the retained window after a
/// snapshot converges on an upsert sink.
pub fn capture_bookmark(stream_arn: &str, described: &[ShardInfo]) -> StreamBookmark {
    let mut bm = StreamBookmark {
        stream_arn: Some(stream_arn.to_string()),
        ..Default::default()
    };
    for s in described {
        bm.open(&s.id);
    }
    bm
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shard(id: &str, parent: Option<&str>, _closed: bool) -> ShardInfo {
        ShardInfo {
            id: id.into(),
            parent: parent.map(str::to_string),
        }
    }

    #[test]
    fn start_positions() {
        assert_eq!(
            start_for(Some("9"), false, StreamStart::Latest),
            StartAt::After("9".into())
        );
        assert_eq!(
            start_for(Some(""), false, StreamStart::Latest),
            StartAt::TrimHorizon
        );
        assert_eq!(
            start_for(None, true, StreamStart::Latest),
            StartAt::TrimHorizon
        );
        assert_eq!(start_for(None, false, StreamStart::Latest), StartAt::Latest);
        assert_eq!(
            start_for(None, false, StreamStart::TrimHorizon),
            StartAt::TrimHorizon
        );
    }

    #[test]
    fn parents_run_before_children() {
        let mut p = Planner::new(
            vec![
                shard("a", None, true),
                shard("b", Some("a"), false),
                shard("c", Some("a"), false),
                shard("orphan", Some("gone"), false),
            ],
            &BTreeSet::new(),
        );
        assert_eq!(p.ready(), vec!["a", "orphan"]);
        assert!(p.parent_known("b"));
        assert!(!p.parent_known("orphan"));
        assert!(!p.parent_known("a"));
        assert!(!p.parent_known("missing"));
        p.start("a");
        p.start("orphan");
        assert!(p.ready().is_empty());
        assert!(p.has_children("a"));
        assert!(!p.has_children("b"));
        p.finish("a");
        assert_eq!(p.ready(), vec!["b", "c"]);
        p.merge(vec![shard("d", Some("b"), false)]);
        assert_eq!(p.ready(), vec!["b", "c"]);
    }

    #[test]
    fn finished_shards_from_the_bookmark_are_skipped() {
        let finished: BTreeSet<String> = ["a".to_string()].into();
        let p = Planner::new(
            vec![shard("a", None, true), shard("b", Some("a"), false)],
            &finished,
        );
        assert_eq!(p.ready(), vec!["b"]);
        assert!(p.parent_known("b"));
    }

    #[test]
    fn gap_detection() {
        let described = vec![shard("a", None, true), shard("b", Some("a"), false)];
        assert!(detect_gaps(&StreamBookmark::default(), "arn", &described).is_empty());

        let mut bm = StreamBookmark {
            stream_arn: Some("arn".into()),
            ..Default::default()
        };
        bm.advance("a", "5");
        assert!(detect_gaps(&bm, "arn", &described).is_empty());

        let other = detect_gaps(&bm, "arn2", &described);
        assert!(other[0].contains("now arn2"), "{other:?}");

        let mut expired = bm.clone();
        expired.advance("old", "1");
        let g = detect_gaps(&expired, "arn", &described);
        assert!(g.iter().any(|m| m.contains("shard old expired")), "{g:?}");

        let mut orphan_bm = StreamBookmark::default();
        orphan_bm.advance("x", "1");
        let described = vec![shard("x", None, false), shard("c", Some("p"), false)];
        let g = detect_gaps(&orphan_bm, "arn", &described);
        assert_eq!(g, vec!["shard c's parent p expired unread"]);

        orphan_bm.finish("p");
        assert!(detect_gaps(&orphan_bm, "arn", &described).is_empty());
    }

    #[test]
    fn capture_opens_every_shard() {
        let bm = capture_bookmark(
            "arn",
            &[shard("a", None, true), shard("b", Some("a"), false)],
        );
        assert_eq!(bm.stream_arn.as_deref(), Some("arn"));
        assert_eq!(bm.shards.len(), 2);
        assert!(bm.shards.values().all(String::is_empty));
        let (ids, parents) = ids_and_parents(&[shard("b", Some("a"), false)]);
        assert!(ids.contains("b") && parents.contains("a"));
    }
}
