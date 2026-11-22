//! Copies of a matrix row for the engines that run it again on their own:
//! backfill units, mirror snapshots, verify repairs and DLQ replays.
//!
//! A derived run reads a slice (a window, a snapshot, a few repaired keys or
//! dead letters) and runs alone, so the row's state identity, its forward
//! ordering and its forward-sync monitoring must not come with it. [`derive`]
//! builds the copy field by field, so a new [`ExpandedNode`] field does not
//! compile until it is decided here how a derived run treats it.

use crate::expand::{ExpandedNode, NodeRole};
use faucet_core::{DeliveryGuarantee, DeliveryMode, EffectivelyOnceMechanism};

/// The engine a derived run belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubRun {
    /// One window of `faucet backfill` (keeps the state block; the unit has a
    /// namespaced key).
    Backfill,
    /// The bulk snapshot of `faucet mirror` (keeps the state block).
    MirrorSnapshot,
    /// A `verify` repair (no state).
    VerifyRepair,
    /// A `faucet dlq replay` (no state).
    DlqReplay,
}

impl SubRun {
    fn keeps_state(self) -> bool {
        matches!(self, SubRun::Backfill | SubRun::MirrorSnapshot)
    }
}

/// A copy of the root `node` named `id` for a `purpose` run: no adopted
/// bookmark history (`legacy_state_ids`), no `depends_on` (it runs alone), no
/// scoped cleanup (its read is not the complete set), no SLA or profiling
/// (their baselines describe the forward sync), at-least-once delivery (the
/// keyed-upsert guarantee stays), no pre-built source; state kept only for
/// backfill and mirror snapshots. Everything else is copied for the caller to
/// adjust.
pub(crate) fn derive(node: &ExpandedNode, purpose: SubRun, id: String) -> ExpandedNode {
    debug_assert!(
        matches!(node.role, NodeRole::Root),
        "derived runs start from a root row"
    );
    let ExpandedNode {
        id: _,
        opt_outs,
        row_index,
        weight,
        role,
        source,
        sink,
        transforms,
        state,
        dlq,
        sla,
        profiling,
        #[cfg(feature = "policy")]
        policy,
        #[cfg(feature = "quality")]
        quality,
        #[cfg(feature = "contract")]
        contract,
        #[cfg(feature = "masking")]
        masking,
        sink_ref,
        sink_attested,
        dlq_attested,
        schema,
        delivery: _,
        delivery_guarantee,
        depends_on,
        status,
        tags,
        deferred_refs,
        source_override: _,
        legacy_state_ids,
        cleanup_scope,
        metadata_columns,
        #[cfg(feature = "catalog")]
        local_outputs,
    } = node.clone();
    if cleanup_scope.is_some() && purpose == SubRun::Backfill {
        tracing::warn!(
            row = %node.id,
            "backfill: the row's `complete_for` cleanup is skipped — a unit fetches one window, \
             so a sweep would delete every in-scope row outside it"
        );
    }
    tracing::debug!(
        row = %node.id,
        derived = %id,
        ?purpose,
        legacy_state_ids = legacy_state_ids.len(),
        depends_on = depends_on.len(),
        cleanup = cleanup_scope.is_some(),
        sla = sla.is_some(),
        profiling = profiling.is_some(),
        "derived run drops the row's bookmark history, ordering, cleanup and monitoring"
    );
    let delivery_guarantee = match delivery_guarantee {
        g @ DeliveryGuarantee::EffectivelyOnce(EffectivelyOnceMechanism::KeyedUpsert) => g,
        _ => DeliveryGuarantee::AtLeastOnce,
    };
    ExpandedNode {
        id,
        opt_outs,
        row_index,
        weight,
        role,
        source,
        sink,
        transforms,
        state: if purpose.keeps_state() { state } else { None },
        dlq,
        sla: None,
        profiling: None,
        #[cfg(feature = "policy")]
        policy,
        #[cfg(feature = "quality")]
        quality,
        #[cfg(feature = "contract")]
        contract,
        #[cfg(feature = "masking")]
        masking,
        sink_ref,
        sink_attested,
        dlq_attested,
        schema,
        delivery: DeliveryMode::AtLeastOnce,
        delivery_guarantee,
        depends_on: Vec::new(),
        status,
        tags,
        deferred_refs,
        source_override: None,
        legacy_state_ids: Vec::new(),
        cleanup_scope: None,
        metadata_columns,
        #[cfg(feature = "catalog")]
        local_outputs,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root() -> ExpandedNode {
        let cfg = crate::config::parse_with_extension(
            r#"version: 1
name: p
pipeline:
  source: { type: csv, config: { path: in.csv } }
  sink: { type: jsonl, config: { path: out.jsonl } }
  state: { type: memory }
matrix:
  - id: a
    sink: { config: { path: a.jsonl } }
  - id: b
    depends_on: [a]
"#,
            "yaml",
        )
        .unwrap();
        let mut n = crate::expand::expand(&cfg)
            .unwrap()
            .into_iter()
            .find(|n| n.id == "b")
            .unwrap();
        n.legacy_state_ids = vec!["old".into()];
        n.cleanup_scope = Some(Default::default());
        n.sla =
            Some(serde_json::from_value(serde_json::json!({"max_staleness_secs": 60})).unwrap());
        n.profiling = Some(Default::default());
        n.delivery = DeliveryMode::ExactlyOnce;
        n
    }

    #[test]
    fn every_purpose_clears_history_ordering_cleanup_and_monitoring() {
        let node = root();
        assert_eq!(node.depends_on, vec!["a".to_string()]);
        for purpose in [
            SubRun::Backfill,
            SubRun::MirrorSnapshot,
            SubRun::VerifyRepair,
            SubRun::DlqReplay,
        ] {
            let d = derive(&node, purpose, "x".into());
            assert_eq!(d.id, "x");
            assert!(d.legacy_state_ids.is_empty(), "{purpose:?}");
            assert!(d.depends_on.is_empty(), "{purpose:?}");
            assert!(d.cleanup_scope.is_none(), "{purpose:?}");
            assert!(d.sla.is_none() && d.profiling.is_none(), "{purpose:?}");
            assert_eq!(d.delivery, DeliveryMode::AtLeastOnce);
            assert_eq!(d.delivery_guarantee, DeliveryGuarantee::AtLeastOnce);
            assert!(d.source_override.is_none());
            assert_eq!(d.state.is_some(), purpose.keeps_state(), "{purpose:?}");
            assert_eq!(d.sink.config, node.sink.config);
        }
    }

    #[test]
    fn the_keyed_upsert_guarantee_is_kept() {
        let mut node = root();
        let keyed = DeliveryGuarantee::EffectivelyOnce(EffectivelyOnceMechanism::KeyedUpsert);
        node.delivery_guarantee = keyed;
        assert_eq!(
            derive(&node, SubRun::Backfill, "x".into()).delivery_guarantee,
            keyed
        );
    }
}
