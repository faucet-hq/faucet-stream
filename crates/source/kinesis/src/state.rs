//! Per-shard bookmark format: `{ "shards": { "<shard-id>": "<sequence>" } }`.
//!
//! Every emitted page carries the **cumulative** map, so any page's bookmark
//! is a valid resume point. On resume, a bookmarked shard restarts at
//! `AFTER_SEQUENCE_NUMBER(<persisted>)`; shards absent from the map use the
//! configured start position.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// The persisted bookmark value.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardBookmarks {
    /// Shard id → last durably-emitted sequence number.
    #[serde(default)]
    pub shards: BTreeMap<String, String>,
}

impl ShardBookmarks {
    /// Record a shard's newest sequence number.
    pub fn advance(&mut self, shard_id: &str, sequence: &str) {
        self.shards
            .insert(shard_id.to_string(), sequence.to_string());
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
    fn state_key_shape_is_valid() {
        assert_eq!(state_key("events"), "kinesis:events");
        faucet_core::state::validate_state_key(&state_key("events")).unwrap();
    }
}
