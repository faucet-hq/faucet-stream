//! Group-member sharding for clustered (Mode B) execution — issue #261.
//!
//! Kafka already solves work distribution inside the broker: consumers that
//! share a `group.id` have the topic's partitions assigned across them by the
//! consumer-group protocol, and a member failure triggers a rebalance onto the
//! survivors. So the Kafka source does **not** enumerate disjoint data slices
//! the way the PK-range / hash-of-key sharders do — each "shard" is simply a
//! *membership slot*: "run one more consumer in the shared group". N claimed
//! member shards ⇒ N cooperating group members ⇒ the broker splits partitions
//! among them with no duplication and rebalances on membership change.
//!
//! Offset continuity across members is delegated to the group's **committed
//! offsets**: in member mode the source commits each page's offsets to the
//! broker once that page is durable (see
//! [`KafkaSource::stream_pages`](crate::stream::KafkaSource)), so a partition
//! that migrates to another member (rebalance, worker death, shard reclaim)
//! resumes from the last durably-written position. The state-store bookmark
//! remains the per-member safety net for the crash window between a durable
//! write and its commit — the rebalance callback seeks to the bookmark only
//! when it is *ahead* of the committed offset (never behind, which would
//! re-read work another member already completed).
//!
//! Everything in this module is pure (no I/O) so it is unit-testable without
//! a broker.

use faucet_core::{FaucetError, shard::ShardSpec};
use std::collections::{HashMap, HashSet};

/// One membership slot of the shared consumer group, parsed from a
/// [`ShardSpec`] descriptor of the shape `{"members": N, "member": i}`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MemberShard {
    /// Total membership slots in this run's shard set.
    pub members: usize,
    /// This slot's index in `0..members`. Identification only — Kafka, not
    /// faucet, decides which partitions this member consumes.
    pub member: usize,
}

impl MemberShard {
    /// Parse from a [`ShardSpec`] descriptor produced by
    /// [`plan_member_shards`]. Returns `None` for a malformed descriptor —
    /// including the whole-dataset shard's `null` descriptor, so callers must
    /// check [`ShardSpec::is_whole`] first.
    pub fn from_spec(spec: &ShardSpec) -> Option<Self> {
        let d = &spec.descriptor;
        Some(Self {
            members: d.get("members")?.as_u64()? as usize,
            member: d.get("member")?.as_u64()? as usize,
        })
    }
}

/// Most membership slots one subscription is split into (the same ceiling as
/// `faucet_core::shard::MAX_SHARDS`, kept local so this crate builds against
/// any `faucet-core` 1.x).
pub const MAX_MEMBERS: usize = 4096;

/// Enumerate `target` group-membership slots, optionally capped at the
/// subscription's total partition count (a member beyond the partition count
/// would sit idle — Kafka never assigns one partition to two members of a
/// group). `partition_cap` is `None` when the partition count could not be
/// resolved (broker unreachable at enumeration time); the plan then trusts
/// `target`. `target <= 1` yields a single whole-dataset shard (run it as one
/// plain consumer).
pub fn plan_member_shards(target: usize, partition_cap: Option<usize>) -> Vec<ShardSpec> {
    let target = target.min(MAX_MEMBERS);
    let n = match partition_cap {
        Some(cap) => target.min(cap.max(1)),
        None => target,
    };
    if n <= 1 {
        return vec![ShardSpec::whole()];
    }
    (0..n)
        .map(|i| {
            ShardSpec::new(
                i.to_string(),
                serde_json::json!({ "members": n, "member": i }),
            )
        })
        .collect()
}

/// Parse + validate an applied shard for the Kafka source.
///
/// Returns `Ok(None)` for the whole-dataset shard (plain single-consumer run)
/// and a typed [`FaucetError::Source`] for a malformed descriptor.
pub fn parse_member_shard(shard: &ShardSpec) -> Result<Option<MemberShard>, FaucetError> {
    if shard.is_whole() {
        return Ok(None);
    }
    MemberShard::from_spec(shard).map(Some).ok_or_else(|| {
        FaucetError::Source(format!(
            "kafka: invalid shard descriptor: {}",
            shard.descriptor
        ))
    })
}

/// Decide whether a bookmarked partition needs an explicit seek, given the
/// group's committed offset for it (`None` = no committed offset / lookup
/// failed / invalid).
///
/// - Committed **at or ahead of** the bookmark → no seek: the consumer's
///   default start (the committed offset) already covers everything the
///   bookmark does, and seeking backwards would re-read messages another
///   member durably wrote after this bookmark was taken.
/// - Committed behind the bookmark, or absent → seek to the bookmark. This is
///   the crash window between a durable page write (bookmark persisted) and
///   its broker commit: the bookmark is the furthest durable position, so
///   starting below it merely duplicates (at-least-once), while ignoring it
///   under `auto.offset.reset: latest` would silently skip records.
pub fn member_seek_offset(bookmark: i64, committed: Option<i64>) -> Option<i64> {
    match committed {
        Some(c) if c >= bookmark => None,
        _ => Some(bookmark),
    }
}

/// Filter a cumulative `(topic, partition) -> next_offset` map down to the
/// partitions this member currently owns, as a deterministic list ready to be
/// committed. Committing only owned partitions avoids regressing another
/// member's committed offset after a partition migrated away mid-page.
pub fn commit_list(
    offsets: &HashMap<(String, i32), i64>,
    assigned: &HashSet<(String, i32)>,
) -> Vec<(String, i32, i64)> {
    let mut out: Vec<(String, i32, i64)> = offsets
        .iter()
        .filter(|(tp, _)| assigned.contains(*tp))
        .map(|((t, p), &o)| (t.clone(), *p, o))
        .collect();
    out.sort();
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn plan_member_shards_is_capped() {
        assert_eq!(plan_member_shards(usize::MAX, None).len(), MAX_MEMBERS);
    }

    #[test]
    fn plan_member_shards_returns_target_slots() {
        let shards = plan_member_shards(3, None);
        assert_eq!(shards.len(), 3);
        for (i, s) in shards.iter().enumerate() {
            assert_eq!(s.id, i.to_string());
            assert_eq!(s.descriptor["members"], 3);
            assert_eq!(s.descriptor["member"], i);
            assert_eq!(s.size_estimate, None, "member slots carry no size");
        }
    }

    #[test]
    fn plan_member_shards_caps_at_partition_count() {
        // 8 requested members over a 3-partition subscription → 3 slots; the
        // extra 5 would never be assigned a partition.
        let shards = plan_member_shards(8, Some(3));
        assert_eq!(shards.len(), 3);
        assert_eq!(shards[0].descriptor["members"], 3);
    }

    #[test]
    fn plan_member_shards_uncapped_when_partition_count_unknown() {
        let shards = plan_member_shards(4, None);
        assert_eq!(shards.len(), 4);
    }

    #[test]
    fn plan_member_shards_target_one_is_whole() {
        for (target, cap) in [(1, None), (0, None), (5, Some(1)), (5, Some(0))] {
            let shards = plan_member_shards(target, cap);
            assert_eq!(shards.len(), 1, "target={target} cap={cap:?}");
            assert!(shards[0].is_whole());
        }
    }

    #[test]
    fn member_shard_round_trips_plan_descriptors() {
        for spec in plan_member_shards(5, None) {
            let m = MemberShard::from_spec(&spec).unwrap();
            assert_eq!(m.members, 5);
            assert_eq!(m.member, spec.id.parse::<usize>().unwrap());
        }
    }

    #[test]
    fn parse_member_shard_whole_clears_and_real_parses() {
        assert!(
            parse_member_shard(&ShardSpec::whole())
                .expect("whole parses")
                .is_none()
        );
        let spec = &plan_member_shards(3, None)[2];
        let m = parse_member_shard(spec).unwrap().unwrap();
        assert_eq!((m.members, m.member), (3, 2));
    }

    #[test]
    fn parse_member_shard_rejects_malformed_descriptor() {
        for bad in [
            ShardSpec::new("0", json!({ "member": 0 })),
            ShardSpec::new("0", json!({ "members": 4 })),
            ShardSpec::new("0", json!({ "members": "four", "member": 0 })),
        ] {
            let err = parse_member_shard(&bad).unwrap_err();
            assert!(err.to_string().contains("kafka"), "got: {err}");
        }
    }

    // ── committed-aware seek decision ────────────────────────────────────────

    #[test]
    fn seek_when_committed_absent() {
        assert_eq!(member_seek_offset(42, None), Some(42));
    }

    #[test]
    fn seek_when_bookmark_ahead_of_committed() {
        // The durable-write-before-commit crash window: bookmark is the
        // furthest durable position, so it wins.
        assert_eq!(member_seek_offset(100, Some(90)), Some(100));
    }

    #[test]
    fn no_seek_when_committed_at_or_ahead() {
        // A partition that migrated to another member and advanced: seeking
        // back to our stale bookmark would duplicate that member's work.
        assert_eq!(member_seek_offset(100, Some(100)), None);
        assert_eq!(member_seek_offset(100, Some(250)), None);
    }

    // ── commit list ─────────────────────────────────────────────────────────

    fn tp(t: &str, p: i32) -> (String, i32) {
        (t.to_string(), p)
    }

    #[test]
    fn commit_list_filters_to_assigned_and_sorts() {
        let mut offsets = HashMap::new();
        offsets.insert(tp("t", 1), 20);
        offsets.insert(tp("t", 0), 10);
        offsets.insert(tp("t", 2), 30); // migrated away — must not be committed
        let assigned: HashSet<_> = [tp("t", 0), tp("t", 1)].into();
        assert_eq!(
            commit_list(&offsets, &assigned),
            vec![("t".to_string(), 0, 10), ("t".to_string(), 1, 20)]
        );
    }

    #[test]
    fn commit_list_empty_when_nothing_assigned() {
        let mut offsets = HashMap::new();
        offsets.insert(tp("t", 0), 10);
        assert!(commit_list(&offsets, &HashSet::new()).is_empty());
    }
}
