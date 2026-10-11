//! Bookmark <-> JSON serialization for Kafka offset progress.
//!
//! Bookmark shape (round-trips through `serde_json::Value`):
//!
//! ```json
//! {
//!   "partition_offsets": [
//!     {"topic": "orders", "partition": 0, "offset": 1234},
//!     {"topic": "orders", "partition": 1, "offset":  987}
//!   ]
//! }
//! ```
//!
//! `offset` is the next offset to read (i.e. one past the highest offset
//! whose value has been written to the sink).

use faucet_core::FaucetError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionOffset {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Bookmark {
    #[serde(default)]
    pub partition_offsets: Vec<PartitionOffset>,
}

impl Bookmark {
    pub fn from_value(v: Value) -> Result<Self, FaucetError> {
        serde_json::from_value(v)
            .map_err(|e| FaucetError::State(format!("kafka bookmark parse: {e}")))
    }

    pub fn to_value(&self) -> Result<Value, FaucetError> {
        serde_json::to_value(self)
            .map_err(|e| FaucetError::State(format!("kafka bookmark serialize: {e}")))
    }

    pub fn from_map(map: HashMap<(String, i32), i64>) -> Self {
        let mut entries: Vec<PartitionOffset> = map
            .into_iter()
            .map(|((topic, partition), offset)| PartitionOffset {
                topic,
                partition,
                offset,
            })
            .collect();
        // Deterministic order makes diffs in state-store files reviewable.
        entries
            .sort_by(|a, b| (a.topic.as_str(), a.partition).cmp(&(b.topic.as_str(), b.partition)));
        Self {
            partition_offsets: entries,
        }
    }

    /// Build the bookmark to persist for a run/page, merging three offset
    /// sources in **increasing precedence** so no assigned partition is ever
    /// silently dropped from the bookmark (the H9 data-loss fix):
    ///
    /// 1. `prior` — offsets from the bookmark applied at run start
    ///    (carry-forward). A partition consumed in an earlier run but
    ///    assigned-yet-empty in this one keeps its last-known offset instead
    ///    of vanishing. Acts as the safety net when librdkafka has not yet
    ///    resolved a position for an assigned partition.
    /// 2. `positions` — the consumer's current position for every *assigned*
    ///    partition (from `rdkafka::consumer::Consumer::position`). This is
    ///    the crux of the fix: a partition that produced **no** message this
    ///    run still records where the consumer actually sits, so the next
    ///    resume seeks it to that offset instead of leaving it absent — an
    ///    absent partition resets to `auto.offset.reset` (default `latest`)
    ///    and silently skips any records that arrived in the meantime.
    /// 3. `consumed` — the next offset after each message actually delivered
    ///    this run (authoritative: we definitely read up to here, so this
    ///    overrides a position/prior value for the same partition).
    pub fn merged(
        prior: Option<&Bookmark>,
        positions: &[PartitionOffset],
        consumed: &HashMap<(String, i32), i64>,
    ) -> Self {
        Self::merged_with_floors(prior, &[], positions, consumed)
    }

    /// [`Bookmark::merged`] plus `floors`: watermark offsets for assigned
    /// partitions whose consumer position is not yet concrete. A floor has the
    /// **lowest** precedence — below the prior bookmark — because librdkafka
    /// reports no position for a resumed partition until it delivers, so a
    /// floor over a bookmarked partition would move it to the watermark and
    /// skip (or, under `earliest`, replay) its backlog. A floor only seeds a
    /// partition nothing else knows about.
    pub fn merged_with_floors(
        prior: Option<&Bookmark>,
        floors: &[PartitionOffset],
        positions: &[PartitionOffset],
        consumed: &HashMap<(String, i32), i64>,
    ) -> Self {
        let mut map: HashMap<(String, i32), i64> = HashMap::new();
        for p in floors {
            map.insert((p.topic.clone(), p.partition), p.offset);
        }
        if let Some(prior) = prior {
            for p in &prior.partition_offsets {
                map.insert((p.topic.clone(), p.partition), p.offset);
            }
        }
        for p in positions {
            map.insert((p.topic.clone(), p.partition), p.offset);
        }
        for (&(ref topic, partition), &offset) in consumed {
            map.insert((topic.clone(), partition), offset);
        }
        Self::from_map(map)
    }
}

/// Offsets by partition, or `None` for a value that is not a bookmark.
fn offsets(v: &Value) -> Option<HashMap<(String, i32), i64>> {
    let b = Bookmark::from_value(v.clone()).ok()?;
    let mut map = HashMap::with_capacity(b.partition_offsets.len());
    for p in b.partition_offsets {
        map.insert((p.topic, p.partition), p.offset);
    }
    Some(map)
}

/// Whether bookmark `a` is at or before `b`: both cover the same partitions
/// and every offset in `a` is at or below `b`'s. `None` when either is not a
/// bookmark or they cover different partitions, since an absent partition
/// resumes from `auto.offset.reset`, which has no place in the order.
pub fn position_le(a: &Value, b: &Value) -> Option<bool> {
    let (a, b) = (offsets(a)?, offsets(b)?);
    if a.len() != b.len() {
        return None;
    }
    let mut le = true;
    for (k, off) in &a {
        le &= off <= b.get(k)?;
    }
    Some(le)
}

/// The per-partition minimum of `positions`, when they all cover the same
/// partitions.
pub fn position_min(positions: &[Value]) -> Option<Value> {
    let mut iter = positions.iter();
    let mut min = offsets(iter.next()?)?;
    for v in iter {
        let next = offsets(v)?;
        if next.len() != min.len() {
            return None;
        }
        for (k, off) in min.iter_mut() {
            *off = (*off).min(*next.get(k)?);
        }
    }
    Bookmark::from_map(min).to_value().ok()
}

/// Whether a record (`topic` / `partition` / `offset`) was delivered at
/// `position`: its partition's next offset there is past the record's.
pub fn record_covered(record: &Value, position: &Value) -> Option<bool> {
    let topic = record.get("topic")?.as_str()?;
    let partition = i32::try_from(record.get("partition")?.as_i64()?).ok()?;
    let offset = record.get("offset")?.as_i64()?;
    let positions = offsets(position)?;
    Some(
        positions
            .get(&(topic.to_string(), partition))
            .is_some_and(|next| offset < *next),
    )
}

/// Generate the `state_key` for a `(group_id, topics)` pair.
///
/// Topics are sorted before joining so the key is stable regardless of config
/// ordering. Topics are joined with `:` rather than `.`: a Kafka topic name may
/// legally contain `.` (e.g. `orders.eu`), so a `.` join made `["a.b","c"]` and
/// `["a","b.c"]` collide on a shared `group_id`. `:` is **not** a legal Kafka
/// topic character, so it is an unambiguous separator — and it is permitted in a
/// state key per [`faucet_core::state::validate_state_key`] (`[A-Za-z0-9_:.-]`).
pub fn state_key(group_id: &str, topics: &[String]) -> String {
    let mut sorted = topics.to_vec();
    sorted.sort();
    format!("kafka:{group_id}:{}", sorted.join(":"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn round_trip_via_value() {
        let bookmark = Bookmark {
            partition_offsets: vec![
                PartitionOffset {
                    topic: "t".into(),
                    partition: 0,
                    offset: 5,
                },
                PartitionOffset {
                    topic: "t".into(),
                    partition: 1,
                    offset: 9,
                },
            ],
        };
        let v = bookmark.to_value().unwrap();
        let parsed = Bookmark::from_value(v).unwrap();
        assert_eq!(parsed.partition_offsets, bookmark.partition_offsets);
    }

    #[test]
    fn from_map_is_deterministic() {
        let mut a: HashMap<(String, i32), i64> = HashMap::new();
        a.insert(("z".into(), 0), 1);
        a.insert(("a".into(), 1), 2);
        a.insert(("a".into(), 0), 3);
        let b = Bookmark::from_map(a);
        let topics: Vec<_> = b
            .partition_offsets
            .iter()
            .map(|p| (p.topic.as_str(), p.partition))
            .collect();
        assert_eq!(topics, vec![("a", 0), ("a", 1), ("z", 0)]);
    }

    fn bm(offsets: &[(i32, i64)]) -> Value {
        json!({ "partition_offsets": offsets
            .iter()
            .map(|(p, o)| json!({ "topic": "t", "partition": p, "offset": o }))
            .collect::<Vec<_>>() })
    }

    #[test]
    fn positions_order_per_partition() {
        let a = bm(&[(0, 5), (1, 5)]);
        assert_eq!(position_le(&a, &bm(&[(0, 5), (1, 7)])), Some(true));
        assert_eq!(position_le(&a, &bm(&[(0, 9), (1, 3)])), Some(false));
        assert_eq!(position_le(&bm(&[(0, 9), (1, 3)]), &a), Some(false));
        assert_eq!(
            position_le(&a, &bm(&[(0, 5)])),
            None,
            "different partitions"
        );
        assert_eq!(position_le(&a, &bm(&[(0, 5), (2, 5)])), None);
        assert_eq!(position_le(&a, &json!("x")), None);
    }

    #[test]
    fn position_min_is_per_partition() {
        let min = position_min(&[bm(&[(0, 5), (1, 9)]), bm(&[(0, 8), (1, 2)])]).unwrap();
        assert_eq!(min, bm(&[(0, 5), (1, 2)]));
        assert_eq!(position_min(&[bm(&[(0, 5)]), bm(&[(0, 5), (1, 2)])]), None);
        assert_eq!(position_min(&[bm(&[(0, 5)]), bm(&[(1, 5)])]), None);
        assert_eq!(position_min(&[]), None);
        assert_eq!(position_min(&[json!(1)]), None);
    }

    #[test]
    fn record_covered_compares_the_records_partition() {
        let at = bm(&[(0, 5), (1, 9)]);
        let rec = |p: i64, o: i64| json!({ "topic": "t", "partition": p, "offset": o });
        assert_eq!(record_covered(&rec(0, 4), &at), Some(true));
        assert_eq!(record_covered(&rec(0, 5), &at), Some(false));
        assert_eq!(
            record_covered(&rec(2, 0), &at),
            Some(false),
            "partition never read"
        );
        assert_eq!(record_covered(&json!({ "value": 1 }), &at), None);
        assert_eq!(record_covered(&rec(0, 4), &json!(null)), None);
    }

    #[test]
    fn state_key_sorts_topics() {
        assert_eq!(
            state_key("g1", &["beta".into(), "alpha".into()]),
            "kafka:g1:alpha:beta"
        );
    }

    #[test]
    fn state_key_single_topic() {
        assert_eq!(state_key("g1", &["only".into()]), "kafka:g1:only");
    }

    #[test]
    fn state_key_distinguishes_topic_sets_containing_dots() {
        // Topic names legally contain dots. Joining with '.' made
        // ["a.b","c"] and ["a","b.c"] collide on a shared group_id, so two
        // distinct subscriptions would clobber each other's bookmark.
        let k1 = state_key("g", &["a.b".into(), "c".into()]);
        let k2 = state_key("g", &["a".into(), "b.c".into()]);
        assert_ne!(k1, k2, "topic sets with dots must not produce the same key");
    }

    #[test]
    fn from_value_rejects_garbage() {
        let v = json!({"partition_offsets": "not an array"});
        assert!(Bookmark::from_value(v).is_err());
    }

    #[test]
    fn empty_bookmark_round_trips() {
        let b = Bookmark::default();
        let v = b.to_value().unwrap();
        let parsed = Bookmark::from_value(v).unwrap();
        assert!(parsed.partition_offsets.is_empty());
    }

    fn po(topic: &str, partition: i32, offset: i64) -> PartitionOffset {
        PartitionOffset {
            topic: topic.into(),
            partition,
            offset,
        }
    }

    fn offsets_of(b: &Bookmark) -> Vec<(&str, i32, i64)> {
        b.partition_offsets
            .iter()
            .map(|p| (p.topic.as_str(), p.partition, p.offset))
            .collect()
    }

    #[test]
    fn merged_consumed_overrides_position_and_prior() {
        // A partition that delivered messages this run: the per-message
        // next-offset is authoritative and wins over both the consumer
        // position and the carry-forward value.
        let prior = Bookmark {
            partition_offsets: vec![po("orders", 0, 50)],
        };
        let positions = vec![po("orders", 0, 90)];
        let mut consumed = HashMap::new();
        consumed.insert(("orders".to_string(), 0), 100);

        let merged = Bookmark::merged(Some(&prior), &positions, &consumed);
        assert_eq!(offsets_of(&merged), vec![("orders", 0, 100)]);
    }

    #[test]
    fn merged_seeds_empty_assigned_partition_from_position() {
        // The H9 case: partition 1 produced no message this run (absent from
        // `consumed`) but is assigned, so its current position must be
        // recorded — otherwise it would be missing from the bookmark and
        // reset to `auto.offset.reset` on the next resume, skipping records.
        let positions = vec![po("orders", 0, 100), po("orders", 1, 0)];
        let mut consumed = HashMap::new();
        consumed.insert(("orders".to_string(), 0), 100);

        let merged = Bookmark::merged(None, &positions, &consumed);
        assert_eq!(
            offsets_of(&merged),
            vec![("orders", 0, 100), ("orders", 1, 0)],
            "the empty-this-run partition must be seeded from its position"
        );
    }

    #[test]
    fn merged_carries_forward_prior_when_no_position_or_message() {
        // A previously-known partition with neither a fresh position nor a
        // delivered message this run keeps its prior offset rather than being
        // dropped (safety net for an unresolved position).
        let prior = Bookmark {
            partition_offsets: vec![po("orders", 2, 777)],
        };
        let merged = Bookmark::merged(Some(&prior), &[], &HashMap::new());
        assert_eq!(offsets_of(&merged), vec![("orders", 2, 777)]);
    }

    #[test]
    fn merged_position_overrides_prior_but_not_consumed() {
        // position > prior (we trust where the consumer currently sits over a
        // stale carry-forward), but consumed still wins over both.
        let prior = Bookmark {
            partition_offsets: vec![po("t", 0, 10), po("t", 1, 20)],
        };
        let positions = vec![po("t", 0, 15), po("t", 1, 25)];
        let mut consumed = HashMap::new();
        consumed.insert(("t".to_string(), 1), 30);

        let merged = Bookmark::merged(Some(&prior), &positions, &consumed);
        assert_eq!(
            offsets_of(&merged),
            vec![("t", 0, 15), ("t", 1, 30)],
            "p0 takes the position; p1 takes the consumed next-offset"
        );
    }

    #[test]
    fn a_watermark_floor_never_overrides_a_bookmarked_partition() {
        // Resumed partition 1 has not delivered yet, so only a floor (the
        // high watermark) is known for it: the prior bookmark must win.
        let prior = Bookmark {
            partition_offsets: vec![po("t", 0, 10), po("t", 1, 20)],
        };
        let floors = vec![po("t", 1, 500), po("t", 2, 900)];
        let mut consumed = HashMap::new();
        consumed.insert(("t".to_string(), 0), 12);

        let merged = Bookmark::merged_with_floors(Some(&prior), &floors, &[], &consumed);
        assert_eq!(
            offsets_of(&merged),
            vec![("t", 0, 12), ("t", 1, 20), ("t", 2, 900)],
            "p1 keeps its bookmark; only the never-seen p2 takes its floor"
        );

        let positions = vec![po("t", 1, 25)];
        let merged = Bookmark::merged_with_floors(Some(&prior), &floors, &positions, &consumed);
        assert_eq!(offsets_of(&merged)[1], ("t", 1, 25));
    }

    #[test]
    fn merged_all_empty_yields_empty_bookmark() {
        let merged = Bookmark::merged(None, &[], &HashMap::new());
        assert!(merged.partition_offsets.is_empty());
    }
}
