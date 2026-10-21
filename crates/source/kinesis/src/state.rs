//! Per-shard bookmark format: `{ "shards": { "<shard-id>": "<sequence>" },
//! "resume_at": { "<shard-id>": <unix-secs> } }`.
//!
//! Every emitted page carries the **cumulative** map, so any page's bookmark
//! is a valid resume point. On resume, a bookmarked shard restarts at
//! `AFTER_SEQUENCE_NUMBER(<persisted>)`. A shard that delivered no record yet
//! but was read up to its tip has a `resume_at` time instead and restarts at
//! `AT_TIMESTAMP(<persisted>)` — without it a quiet shard would restart at the
//! configured start position, which under `latest` skips everything written
//! between runs. Shards absent from both maps use the start position.
//! `resume_at` is omitted when empty, so a bookmark without it keeps the
//! original shape (and an older release reading the new one ignores it).

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// The persisted bookmark value.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardBookmarks {
    /// Shard id → last durably-emitted sequence number.
    #[serde(default)]
    pub shards: BTreeMap<String, String>,
    /// Shard id → Unix seconds a shard with no sequence was read up to its
    /// tip: no record of it arrived before then.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub resume_at: BTreeMap<String, i64>,
}

impl ShardBookmarks {
    /// Record a shard's newest sequence number.
    pub fn advance(&mut self, shard_id: &str, sequence: &str) {
        self.shards
            .insert(shard_id.to_string(), sequence.to_string());
        self.resume_at.remove(shard_id);
    }

    /// Record that a shard with no sequence was read up to its tip at
    /// `unix_secs`. Ignored once the shard has a sequence (the more exact
    /// position); never moves an earlier time later than a newer one.
    pub fn caught_up(&mut self, shard_id: &str, unix_secs: i64) {
        if self.shards.contains_key(shard_id) {
            return;
        }
        let at = self
            .resume_at
            .entry(shard_id.to_string())
            .or_insert(unix_secs);
        *at = (*at).max(unix_secs);
    }

    /// The time a sequence-less shard resumes at, if any.
    pub fn resume_time(&self, shard_id: &str) -> Option<i64> {
        self.resume_at.get(shard_id).copied()
    }

    /// Whether the bookmark records no shard at all.
    pub fn is_empty(&self) -> bool {
        self.shards.is_empty() && self.resume_at.is_empty()
    }

    /// The persisted sequence for a shard, if any.
    pub fn get(&self, shard_id: &str) -> Option<&str> {
        self.shards.get(shard_id).map(String::as_str)
    }

    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }

    /// Parse a bookmark `Value`; a malformed value is treated as absent.
    /// Prefer [`try_from_value`](Self::try_from_value): under a `latest`
    /// start position "absent" skips everything written since the bookmark.
    pub fn from_value(v: &Value) -> Self {
        Self::try_from_value(v).unwrap_or_default()
    }

    /// Parse a bookmark `Value`, refusing a malformed one rather than
    /// silently restarting every shard at the start position (#789 MSG-95).
    pub fn try_from_value(v: &Value) -> Result<Self, faucet_core::FaucetError> {
        serde_json::from_value(v.clone()).map_err(|e| {
            faucet_core::FaucetError::State(format!(
                "kinesis: the stored bookmark is malformed ({e}); refusing to resume from it — \
                 reset the state to start over"
            ))
        })
    }

    /// Drop bookmarks of shards the stream no longer lists (expired after a
    /// reshard), so the map does not grow forever (#789 MSG-91).
    pub fn retain_listed(&mut self, listed: &std::collections::HashSet<String>) {
        self.shards.retain(|id, _| listed.contains(id));
        self.resume_at.retain(|id, _| listed.contains(id));
    }
}

/// The source's stable state key.
pub fn state_key(stream_name: &str) -> String {
    format!("kinesis:{stream_name}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn bookmark_round_trips_and_advances() {
        let mut b = ShardBookmarks::default();
        b.advance("shardId-000000000000", "491");
        b.advance("shardId-000000000001", "530");
        b.advance("shardId-000000000000", "495"); // newer overwrites
        let v = b.to_value();
        assert_eq!(v["shards"]["shardId-000000000000"], "495");
        let back = ShardBookmarks::from_value(&v);
        assert_eq!(back, b);
        assert_eq!(back.get("shardId-000000000001"), Some("530"));
        assert_eq!(back.get("missing"), None);
    }

    #[test]
    fn malformed_bookmark_is_treated_as_fresh() {
        assert_eq!(
            ShardBookmarks::from_value(&json!("not-a-map")),
            ShardBookmarks::default()
        );
        assert_eq!(
            ShardBookmarks::from_value(&json!(null)),
            ShardBookmarks::default()
        );
    }

    #[test]
    fn malformed_bookmarks_are_refused_and_expired_shards_pruned() {
        assert!(ShardBookmarks::try_from_value(&json!("not-a-map")).is_err());
        let mut b = ShardBookmarks::default();
        b.advance("a", "1");
        b.advance("gone", "2");
        b.retain_listed(&["a".to_string()].into());
        assert_eq!(b.shards.len(), 1);
        assert_eq!(b.get("a"), Some("1"));
    }

    #[test]
    fn a_quiet_shard_resumes_at_the_time_it_was_caught_up() {
        let mut b = ShardBookmarks::default();
        b.caught_up("quiet", 100);
        b.caught_up("quiet", 90);
        assert_eq!(b.resume_time("quiet"), Some(100));
        assert!(!b.is_empty());
        let v = b.to_value();
        assert_eq!(v, json!({"shards": {}, "resume_at": {"quiet": 100}}));
        assert_eq!(ShardBookmarks::from_value(&v), b);
        // A sequence supersedes the time, and a time never replaces it.
        b.advance("quiet", "7");
        b.caught_up("quiet", 200);
        assert_eq!(b.resume_time("quiet"), None);
        assert_eq!(b.to_value(), json!({"shards": {"quiet": "7"}}));
        b.caught_up("gone", 5);
        b.retain_listed(&["quiet".to_string()].into());
        assert_eq!(b.resume_time("gone"), None);
        assert!(ShardBookmarks::default().is_empty());
    }

    #[test]
    fn state_key_shape_is_valid() {
        assert_eq!(state_key("events"), "kinesis:events");
        faucet_core::state::validate_state_key(&state_key("events")).unwrap();
    }
}
