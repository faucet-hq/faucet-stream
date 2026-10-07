//! Custom rdkafka `ConsumerContext` that seeks to bookmarked offsets during
//! partition assignment — before any message is polled — so a restart with
//! a bookmark applied produces no duplicate records.
//!
//! The poll-then-seek path (the old `maybe_apply_seek`) emits one duplicate
//! per assigned partition because partition assignment is not final until
//! the first message arrives. By overriding `rebalance` and modifying the
//! `TopicPartitionList` offsets before calling `rd_kafka_assign`, the
//! bookmarked starting positions are applied atomically with the partition
//! assignment — no messages from before the bookmark are ever fetched.

use crate::state::Bookmark;
use faucet_core::FaucetError;
use rdkafka::Offset;
use rdkafka::TopicPartitionList;
use rdkafka::client::ClientContext;
use rdkafka::consumer::base_consumer::BaseConsumer;
use rdkafka::consumer::{Consumer, ConsumerContext, Rebalance, RebalanceProtocol};
use rdkafka::error::KafkaError;
use rdkafka::types::RDKafkaRespErr;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Broker-round-trip budget for the committed-offsets lookup performed inside
/// the rebalance callback in group-member mode. Matches the watermark-lookup
/// budget used elsewhere in this crate.
const COMMITTED_LOOKUP_TIMEOUT: Duration = Duration::from_secs(5);

/// Shared state between [`KafkaSource`](crate::stream::KafkaSource) and the
/// rdkafka consumer background thread. Cloning a `BookmarkContext` clones the
/// inner `Arc`s, not the underlying data — both clones see the same bookmark
/// and the same error slot.
#[derive(Clone, Default)]
pub(crate) struct BookmarkContext {
    /// Bookmark to apply on the first `Rebalance::Assign`. Taken (not peeked)
    /// when the assign fires; later assigns use [`Self::delivered`].
    pub(crate) pending_bookmark: Arc<Mutex<Option<Bookmark>>>,
    /// A retained copy of the start bookmark (never taken). Read at
    /// bookmark-build time so previously-known partitions that are assigned
    /// yet produce no message this run carry their offset forward instead of
    /// being dropped (part of the H9 fix). Distinct from `pending_bookmark`,
    /// which is consumed by the rebalance callback.
    pub(crate) start_offsets: Arc<Mutex<Option<Bookmark>>>,
    /// First error raised inside the rebalance callback (assign failures).
    /// The poll loop drains this between iterations and surfaces it to the
    /// caller.
    pub(crate) callback_error: Arc<Mutex<Option<FaucetError>>>,
    /// Group-member (Mode B, #261) mode: this consumer is one of N cooperating
    /// members of the group, so bookmark seeks must defer to the group's
    /// committed offsets whenever those are ahead (another member may have
    /// durably advanced a partition past our bookmark). Set by
    /// [`KafkaSource::apply_shard`](crate::stream::KafkaSource).
    pub(crate) member_mode: Arc<AtomicBool>,
    /// The next offset after every message delivered this run, by partition.
    /// Outside member mode nothing is committed to the group, so a later
    /// Assign (a rebalance after the first) re-seeks from the start bookmark
    /// plus these, instead of falling back to `auto.offset.reset`.
    pub(crate) delivered: Arc<Mutex<HashMap<(String, i32), i64>>>,
    /// Whether `auto.offset.reset` is `earliest` — where a member-mode
    /// partition the group has no offset for starts, so that position can be
    /// seeded into the group before anything is consumed (#789 MSG-72).
    pub(crate) earliest: Arc<AtomicBool>,
}

/// Where a bookmarked partition actually resumes: never below the log start.
/// Returns the offset and how many bookmarked records retention already
/// deleted. librdkafka resets an out-of-range offset to `auto.offset.reset`
/// (default `latest`), which would skip the still-retained backlog (#789
/// MSG-24); starting at the low watermark keeps it.
pub(crate) fn clamp_to_log_start(bookmark: i64, low: Option<i64>) -> (i64, u64) {
    match low {
        Some(low) if bookmark < low => (low, (low - bookmark) as u64),
        _ => (bookmark, 0),
    }
}

/// The group offsets a member-mode consumer seeds for partitions the group has
/// never committed: the bookmarked position when there is one, else where
/// `auto.offset.reset` starts it. Without the seed, a partition that migrates
/// to another member before the first durable commit falls back to that
/// member's `auto.offset.reset` and skips what this member had started
/// reading from (#789 MSG-72).
pub(crate) fn member_seeds(
    assigned: &[(String, i32)],
    committed: &HashMap<(String, i32), Option<i64>>,
    seeks: &HashMap<(String, i32), i64>,
    watermarks: &HashMap<(String, i32), (i64, i64)>,
    earliest: bool,
) -> Vec<(String, i32, i64)> {
    let mut out = Vec::new();
    for tp in assigned {
        if committed.get(tp).copied().flatten().is_some() {
            continue;
        }
        let offset = match seeks.get(tp) {
            Some(o) => Some(*o),
            None => watermarks
                .get(tp)
                .map(|(low, high)| if earliest { *low } else { *high }),
        };
        if let Some(o) = offset {
            out.push((tp.0.clone(), tp.1, o));
        }
    }
    out.sort();
    out
}

/// Where a re-assigned partition resumes outside member mode: the start
/// bookmark, advanced by what this run already delivered. `None` when neither
/// knows any partition.
pub(crate) fn resume_bookmark(
    start: Option<&Bookmark>,
    delivered: &HashMap<(String, i32), i64>,
) -> Option<Bookmark> {
    let merged = Bookmark::merged(start, &[], delivered);
    (!merged.partition_offsets.is_empty()).then_some(merged)
}

impl BookmarkContext {
    /// Record the next offset after a delivered message.
    pub(crate) fn note_delivered(&self, topic: &str, partition: i32, next_offset: i64) {
        if let Ok(mut map) = self.delivered.lock() {
            map.insert((topic.to_string(), partition), next_offset);
        }
    }

    fn reassign_bookmark(&self) -> Option<Bookmark> {
        let start = self.start_offsets.lock().ok().and_then(|g| g.clone());
        if self.member_mode.load(Ordering::Acquire) {
            // Only durable positions apply in member mode: the start bookmark,
            // filtered against the group's committed offsets at assign time.
            return start.filter(|b| !b.partition_offsets.is_empty());
        }
        let delivered = self.delivered.lock().ok()?.clone();
        resume_bookmark(start.as_ref(), &delivered)
    }

    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Record the first error from a callback. Subsequent errors are
    /// dropped so the original cause survives, matching the pattern used
    /// elsewhere in faucet-stream for background-thread error capture.
    fn record_error(&self, err: FaucetError) {
        let Ok(mut guard) = self.callback_error.lock() else {
            // Mutex poisoned and we are already in an error path — nothing
            // useful to do but swallow.
            return;
        };
        if guard.is_none() {
            *guard = Some(err);
        }
    }
}

impl ClientContext for BookmarkContext {}

impl ConsumerContext for BookmarkContext {
    /// Override `rebalance` to inject bookmark offsets into the
    /// `TopicPartitionList` *before* `rd_kafka_assign` is called.
    ///
    /// The default `ConsumerContext::rebalance` fires `pre_rebalance`, then
    /// calls `rd_kafka_assign`, then fires `post_rebalance`. Offsets set on
    /// the TPL before `rd_kafka_assign` become the initial fetch positions for
    /// those partitions — no seek call is needed and there is no race with
    /// the fetch state machine.
    fn rebalance(
        &self,
        base_consumer: &BaseConsumer<Self>,
        err: RDKafkaRespErr,
        tpl: &mut TopicPartitionList,
    ) {
        match err {
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS => {
                let rebalance = Rebalance::Assign(tpl);
                self.pre_rebalance(base_consumer, &rebalance);
                drop(rebalance);

                // The first Assign takes the start bookmark. A later one
                // (outside member mode, where nothing is committed to the
                // group) resumes from the start bookmark advanced by what this
                // run delivered — otherwise a re-assigned partition restarts
                // at `auto.offset.reset` and skips the gap.
                let bookmark = match self.pending_bookmark.lock() {
                    Ok(mut guard) => guard.take(),
                    Err(poisoned) => {
                        self.record_error(FaucetError::State(format!(
                            "kafka pending_bookmark mutex poisoned: {poisoned}"
                        )));
                        None
                    }
                }
                .or_else(|| self.reassign_bookmark());

                let mut applied: Vec<(String, i32, i64)> = Vec::new();
                if let Some(bookmark) = bookmark {
                    let lookup: HashMap<(&str, i32), i64> = bookmark
                        .partition_offsets
                        .iter()
                        .map(|p| ((p.topic.as_str(), p.partition), p.offset))
                        .collect();

                    // Collect (topic, partition, offset) triples first to avoid
                    // holding an immutable borrow on `tpl` while mutating it.
                    let mut seeks: Vec<(String, i32, i64)> = tpl
                        .elements()
                        .into_iter()
                        .filter_map(|elem| {
                            let topic = elem.topic().to_owned();
                            let partition = elem.partition();
                            lookup
                                .get(&(topic.as_str(), partition))
                                .copied()
                                .map(|offset| (topic, partition, offset))
                        })
                        .collect();

                    // Group-member mode (#261): the group's committed offsets
                    // are the shared source of truth across members — another
                    // member may have durably advanced a partition past this
                    // member's bookmark, so an unconditional bookmark seek
                    // would re-read its work. Keep a seek only when the
                    // bookmark is AHEAD of the committed offset (the durable
                    // write → commit crash window, where skipping the seek
                    // under `auto.offset.reset: latest` would silently lose
                    // records). The lookup is a bounded broker round-trip,
                    // paid only at assign time; on failure fall back to
                    // seeking every bookmarked partition — that can duplicate
                    // (at-least-once) but never lose.
                    if self.member_mode.load(Ordering::Acquire) && !seeks.is_empty() {
                        seeks = filter_seeks_by_committed(base_consumer, seeks);
                    }

                    for (topic, partition, offset) in &mut seeks {
                        let low = base_consumer
                            .fetch_watermarks(topic, *partition, COMMITTED_LOOKUP_TIMEOUT)
                            .ok()
                            .map(|(low, _)| low);
                        let (clamped, lost) = clamp_to_log_start(*offset, low);
                        if lost > 0 {
                            tracing::warn!(
                                topic = %topic,
                                partition = *partition,
                                bookmark = *offset,
                                log_start = clamped,
                                lost,
                                "kafka source: the bookmark fell out of topic retention; \
                                 resuming at the log start (the deleted records are gone)"
                            );
                            *offset = clamped;
                        }
                    }

                    // Partitions absent from the bookmark are left at their
                    // default offset (earliest/latest per `auto.offset.reset`).
                    // With the assigned-set bookmark seeding in
                    // `Bookmark::merged`, an absent partition here is one that
                    // was never assigned in any prior run (e.g. a partition
                    // added to the topic since the last run) — honouring
                    // `auto.offset.reset` for a genuinely-new partition is
                    // correct. Partitions that were assigned but empty in a
                    // prior run are recorded via their position and so DO
                    // appear in the bookmark and get seeked here.
                    for (topic, partition, offset) in &seeks {
                        if let Err(e) =
                            tpl.set_partition_offset(topic, *partition, Offset::Offset(*offset))
                        {
                            self.record_error(FaucetError::State(format!(
                                "kafka set_partition_offset topic={topic} \
                                 partition={partition} offset={offset}: {e}"
                            )));
                        }
                    }
                    applied = seeks;
                }

                let seeds = if self.member_mode.load(Ordering::Acquire) {
                    self.member_seed_offsets(base_consumer, tpl, &applied)
                } else {
                    Vec::new()
                };
                for (topic, partition, offset) in &seeds {
                    let _ = tpl.set_partition_offset(topic, *partition, Offset::Offset(*offset));
                }

                match base_consumer.rebalance_protocol() {
                    RebalanceProtocol::Cooperative => {
                        if let Err(e) = base_consumer.incremental_assign(tpl) {
                            self.record_error(FaucetError::State(format!(
                                "kafka incremental_assign failed: {e}"
                            )));
                        }
                    }
                    _ => {
                        if let Err(e) = base_consumer.assign(tpl) {
                            self.record_error(FaucetError::State(format!(
                                "kafka assign failed: {e}"
                            )));
                        }
                    }
                }

                if !seeds.is_empty() {
                    commit_seeds(base_consumer, &seeds);
                }

                let rebalance = Rebalance::Assign(tpl);
                self.post_rebalance(base_consumer, &rebalance);
            }

            RDKafkaRespErr::RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS => {
                let rebalance = Rebalance::Revoke(tpl);
                self.pre_rebalance(base_consumer, &rebalance);
                drop(rebalance);

                match base_consumer.rebalance_protocol() {
                    RebalanceProtocol::Cooperative => {
                        if let Err(e) = base_consumer.incremental_unassign(tpl) {
                            self.record_error(FaucetError::State(format!(
                                "kafka incremental_unassign failed: {e}"
                            )));
                        }
                    }
                    _ => {
                        if let Err(e) = base_consumer.unassign() {
                            self.record_error(FaucetError::State(format!(
                                "kafka unassign failed: {e}"
                            )));
                        }
                    }
                }

                let rebalance = Rebalance::Revoke(tpl);
                self.post_rebalance(base_consumer, &rebalance);
            }

            _ => {
                let error_code = rdkafka::error::RDKafkaErrorCode::from(err);
                let rebalance = Rebalance::Error(KafkaError::Rebalance(error_code));
                self.pre_rebalance(base_consumer, &rebalance);
                self.post_rebalance(base_consumer, &rebalance);
            }
        }
    }
}

impl BookmarkContext {
    /// Member mode: the starting offset of every assigned partition the group
    /// has no committed offset for (see [`member_seeds`]). A lookup failure
    /// seeds nothing, which leaves today's `auto.offset.reset` behaviour.
    fn member_seed_offsets(
        &self,
        consumer: &BaseConsumer<BookmarkContext>,
        tpl: &TopicPartitionList,
        applied: &[(String, i32, i64)],
    ) -> Vec<(String, i32, i64)> {
        let assigned: Vec<(String, i32)> = tpl
            .elements()
            .into_iter()
            .map(|e| (e.topic().to_string(), e.partition()))
            .collect();
        let mut query = TopicPartitionList::with_capacity(assigned.len());
        for (t, p) in &assigned {
            query.add_partition(t, *p);
        }
        let committed = match consumer.committed_offsets(query, COMMITTED_LOOKUP_TIMEOUT) {
            Ok(c) => committed_map(&c),
            Err(e) => {
                tracing::warn!(error = %e, "kafka member mode: committed-offsets lookup failed; not seeding");
                return Vec::new();
            }
        };
        let seeks: HashMap<(String, i32), i64> = applied
            .iter()
            .map(|(t, p, o)| ((t.clone(), *p), *o))
            .collect();
        let mut watermarks = HashMap::new();
        for tp in &assigned {
            if committed.get(tp).copied().flatten().is_none()
                && !seeks.contains_key(tp)
                && let Ok(w) = consumer.fetch_watermarks(&tp.0, tp.1, COMMITTED_LOOKUP_TIMEOUT)
            {
                watermarks.insert(tp.clone(), w);
            }
        }
        member_seeds(
            &assigned,
            &committed,
            &seeks,
            &watermarks,
            self.earliest.load(Ordering::Acquire),
        )
    }
}

fn committed_map(tpl: &TopicPartitionList) -> HashMap<(String, i32), Option<i64>> {
    tpl.elements()
        .into_iter()
        .map(|e| {
            let offset = match e.offset() {
                Offset::Offset(n) => Some(n),
                _ => None,
            };
            ((e.topic().to_string(), e.partition()), offset)
        })
        .collect()
}

/// Commit the seeded starting offsets to the group (synchronously, so a member
/// that takes a partition over right after this rebalance finds them).
fn commit_seeds(consumer: &BaseConsumer<BookmarkContext>, seeds: &[(String, i32, i64)]) {
    let mut tpl = TopicPartitionList::with_capacity(seeds.len());
    for (t, p, o) in seeds {
        let _ = tpl.add_partition_offset(t, *p, Offset::Offset(*o));
    }
    if let Err(e) = consumer.commit(&tpl, rdkafka::consumer::CommitMode::Sync) {
        tracing::warn!(error = %e, "kafka member mode: seeding group offsets failed");
    }
}

/// Drop bookmark seeks that the group's committed offsets already cover
/// (member mode only). Queries the committed offset for each seek candidate in
/// one bounded broker round-trip; a lookup failure conservatively keeps every
/// seek (duplicates are possible, loss is not). The per-partition decision is
/// the pure [`member_seek_offset`](crate::shard::member_seek_offset).
fn filter_seeks_by_committed(
    consumer: &BaseConsumer<BookmarkContext>,
    seeks: Vec<(String, i32, i64)>,
) -> Vec<(String, i32, i64)> {
    let mut query = TopicPartitionList::with_capacity(seeks.len());
    for (topic, partition, _) in &seeks {
        query.add_partition(topic, *partition);
    }
    let committed: HashMap<(String, i32), Option<i64>> =
        match consumer.committed_offsets(query, COMMITTED_LOOKUP_TIMEOUT) {
            Ok(tpl) => committed_map(&tpl),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "kafka member mode: committed-offsets lookup failed; \
                     seeking every bookmarked partition (may re-read, never loses)"
                );
                return seeks;
            }
        };
    seeks
        .into_iter()
        .filter(|(topic, partition, bookmark)| {
            let c = committed
                .get(&(topic.clone(), *partition))
                .copied()
                .flatten();
            crate::shard::member_seek_offset(*bookmark, c).is_some()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{Bookmark, PartitionOffset};

    #[test]
    fn shared_state_round_trips_through_clone() {
        let ctx = BookmarkContext::new();
        let clone = ctx.clone();
        let bookmark = Bookmark {
            partition_offsets: vec![PartitionOffset {
                topic: "t".into(),
                partition: 0,
                offset: 42,
            }],
        };
        *clone.pending_bookmark.lock().unwrap() = Some(bookmark.clone());
        let read_back = ctx.pending_bookmark.lock().unwrap().clone();
        assert_eq!(
            read_back.unwrap().partition_offsets,
            bookmark.partition_offsets
        );
    }

    #[test]
    fn record_error_keeps_first_only() {
        let ctx = BookmarkContext::new();
        ctx.record_error(FaucetError::State("first".into()));
        ctx.record_error(FaucetError::State("second".into()));
        let captured = ctx.callback_error.lock().unwrap().take().unwrap();
        match captured {
            FaucetError::State(msg) => assert_eq!(msg, "first"),
            other => panic!("expected State, got {other:?}"),
        }
    }

    #[test]
    fn a_later_assign_resumes_from_the_start_bookmark_advanced_by_deliveries() {
        let ctx = BookmarkContext::new();
        assert!(ctx.reassign_bookmark().is_none(), "nothing known yet");
        *ctx.start_offsets.lock().unwrap() = Some(Bookmark {
            partition_offsets: vec![
                PartitionOffset {
                    topic: "t".into(),
                    partition: 0,
                    offset: 10,
                },
                PartitionOffset {
                    topic: "t".into(),
                    partition: 1,
                    offset: 20,
                },
            ],
        });
        ctx.note_delivered("t", 1, 27);
        ctx.note_delivered("t", 2, 3);
        let b = ctx.reassign_bookmark().unwrap();
        let got: Vec<(&str, i32, i64)> = b
            .partition_offsets
            .iter()
            .map(|p| (p.topic.as_str(), p.partition, p.offset))
            .collect();
        assert_eq!(got, vec![("t", 0, 10), ("t", 1, 27), ("t", 2, 3)]);

        ctx.member_mode.store(true, Ordering::Release);
        let b = ctx.reassign_bookmark().unwrap();
        assert_eq!(
            b.partition_offsets.len(),
            2,
            "member mode re-applies only the durable start bookmark, never this run's deliveries"
        );
        *ctx.start_offsets.lock().unwrap() = None;
        assert!(ctx.reassign_bookmark().is_none());
    }

    #[test]
    fn a_bookmark_below_the_log_start_resumes_at_the_log_start() {
        assert_eq!(clamp_to_log_start(5, Some(20)), (20, 15));
        assert_eq!(clamp_to_log_start(25, Some(20)), (25, 0));
        assert_eq!(clamp_to_log_start(20, Some(20)), (20, 0));
        assert_eq!(clamp_to_log_start(5, None), (5, 0));
    }

    #[test]
    fn member_seeds_cover_only_partitions_the_group_never_committed() {
        let tp = |p: i32| ("t".to_string(), p);
        let assigned = vec![tp(0), tp(1), tp(2), tp(3)];
        let committed: HashMap<_, _> = [
            (tp(0), Some(7)),
            (tp(1), None),
            (tp(2), None),
            (tp(3), None),
        ]
        .into();
        let seeks: HashMap<_, _> = [(tp(1), 40)].into();
        let marks: HashMap<_, _> = [(tp(2), (3, 9))].into();
        assert_eq!(
            member_seeds(&assigned, &committed, &seeks, &marks, false),
            vec![("t".into(), 1, 40), ("t".into(), 2, 9)],
            "committed partition 0 is left alone, 3 has no known position"
        );
        assert_eq!(
            member_seeds(&assigned, &committed, &seeks, &marks, true),
            vec![("t".into(), 1, 40), ("t".into(), 2, 3)]
        );
    }

    #[test]
    fn resume_bookmark_is_none_when_nothing_is_known() {
        assert!(resume_bookmark(None, &HashMap::new()).is_none());
    }
}
