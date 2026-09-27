//! Streams-mode source lag (#733). While a run reads, the age of the newest
//! record each open shard has handed to the pipeline; before that (a probe,
//! or the start of a run), the age of the oldest unconsumed record at the
//! bookmark, read with one `Limit: 1` `GetRecords` per open shard. A shard
//! with nothing to read counts as caught up (zero).

use std::collections::HashMap;

use faucet_core::SourceLag;

use crate::config::StreamStart;
use crate::lineage::{ShardInfo, StartAt, start_for};
use crate::state::StreamBookmark;

/// Where to peek for each open shard when probing from a bookmark: finished
/// shards are skipped, the rest start where a run would. Pure.
pub(crate) fn probe_starts(
    bm: &StreamBookmark,
    described: &[ShardInfo],
    start: StreamStart,
) -> Vec<(String, StartAt)> {
    let known = |id: &str| bm.finished.contains(id) || described.iter().any(|s| s.id == id);
    described
        .iter()
        .filter(|s| !bm.finished.contains(&s.id))
        .map(|s| {
            let parent_known = s.parent.as_deref().is_some_and(known);
            let at = start_for(
                bm.shards.get(&s.id).map(String::as_str),
                parent_known,
                start,
            );
            (s.id.clone(), at)
        })
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum ShardLag {
    Behind(i64),
    CaughtUp,
}

/// Per-shard lag state for one streams run. Pure.
#[derive(Debug, Default)]
pub(crate) struct LagTracker {
    shards: HashMap<String, ShardLag>,
}

impl LagTracker {
    /// A record created at `ts_ms` was read from `shard`.
    pub(crate) fn observe(&mut self, shard: &str, ts_ms: i64) {
        let next = match self.shards.get(shard) {
            Some(ShardLag::Behind(prev)) => ShardLag::Behind((*prev).max(ts_ms)),
            _ => ShardLag::Behind(ts_ms),
        };
        self.shards.insert(shard.to_string(), next);
    }

    /// `shard` returned no records: nothing is waiting on it.
    pub(crate) fn caught_up(&mut self, shard: &str) {
        self.shards.insert(shard.to_string(), ShardLag::CaughtUp);
    }

    /// `shard` is closed and drained.
    pub(crate) fn close(&mut self, shard: &str) {
        self.shards.remove(shard);
    }

    /// The largest lag across open shards at `now_ms`, or `None` before any
    /// shard has reported.
    pub(crate) fn lag(&self, now_ms: i64) -> Option<SourceLag> {
        self.shards
            .values()
            .map(|s| match s {
                ShardLag::Behind(ts) => (now_ms - ts) as f64 / 1000.0,
                ShardLag::CaughtUp => 0.0,
            })
            .reduce(f64::max)
            .map(SourceLag::seconds)
    }
}

/// Milliseconds since the Unix epoch.
pub(crate) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_the_largest_open_shard_lag() {
        let mut t = LagTracker::default();
        assert_eq!(t.lag(10_000), None);
        t.observe("a", 4_000);
        t.observe("a", 2_000);
        t.observe("b", 1_000);
        assert_eq!(t.lag(10_000), Some(SourceLag::seconds(9.0)));
        t.caught_up("b");
        assert_eq!(t.lag(10_000), Some(SourceLag::seconds(6.0)));
        t.close("a");
        assert_eq!(t.lag(10_000), Some(SourceLag::seconds(0.0)));
        t.observe("b", 9_500);
        assert_eq!(t.lag(10_000), Some(SourceLag::seconds(0.5)));
        t.close("b");
        assert_eq!(t.lag(10_000), None);
    }

    #[test]
    fn probe_starts_follow_the_bookmark() {
        let shard = |id: &str, parent: Option<&str>| ShardInfo {
            id: id.into(),
            parent: parent.map(str::to_string),
        };
        let described = vec![
            shard("done", None),
            shard("read", None),
            shard("opened", None),
            shard("child", Some("done")),
            shard("new", None),
        ];
        let mut bm = StreamBookmark::default();
        bm.finish("done");
        bm.advance("read", "42");
        bm.open("opened");
        let starts = probe_starts(&bm, &described, StreamStart::Latest);
        assert_eq!(
            starts,
            vec![
                ("read".to_string(), StartAt::After("42".into())),
                ("opened".to_string(), StartAt::TrimHorizon),
                ("child".to_string(), StartAt::TrimHorizon),
                ("new".to_string(), StartAt::Latest),
            ]
        );
        let fresh = probe_starts(
            &StreamBookmark::default(),
            &[shard("a", None)],
            StreamStart::TrimHorizon,
        );
        assert_eq!(fresh, vec![("a".to_string(), StartAt::TrimHorizon)]);
    }

    #[test]
    fn clock_skew_clamps_to_zero() {
        let mut t = LagTracker::default();
        t.observe("a", 20_000);
        assert_eq!(t.lag(10_000), Some(SourceLag::seconds(0.0)));
        assert!(now_ms() > 1_700_000_000_000);
    }
}
