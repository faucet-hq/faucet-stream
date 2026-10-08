//! Options struct passed to `run_stream`. Replaces the prior positional
//! argument list so future observability additions don't keep breaking the
//! function signature.

use crate::dlq::DlqConfig;
use crate::state::StateStore;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[derive(Default, Clone)]
pub struct RunStreamOptions {
    pub state_store: Option<Arc<dyn StateStore>>,
    pub state_key: Option<String>,
    pub pipeline_name: Option<String>,
    pub row: Option<String>,
    pub run_id: Option<String>,
    pub dlq: Option<DlqConfig>,
    #[cfg(feature = "quality")]
    pub quality: Option<std::sync::Arc<crate::quality::CompiledQuality>>,
    /// Compiled data contract (issue #204). The pass runs per page after the
    /// quality pass and before the schema-drift pass.
    #[cfg(feature = "contract")]
    pub contract: Option<std::sync::Arc<crate::contract::CompiledContract>>,
    /// Compiled masking policy (issue #206). The pass runs per page *first* —
    /// before quality/contract/drift and every sink write — so PII never
    /// reaches a sink, the DLQ, or a lineage sample unmasked.
    #[cfg(feature = "masking")]
    pub masking: Option<std::sync::Arc<crate::masking::CompiledMasking>>,
    /// Adaptive batch-size controller config; `None` (or `enabled = false`)
    /// leaves the per-page write path unchanged.
    pub adaptive: Option<crate::adaptive::AdaptiveBatchConfig>,
    /// Cooperative cancellation. When set and cancelled mid-run, the streaming
    /// loop stops polling new pages, **flushes the sinks** (so a buffered sink
    /// like Parquet writes its footer / completes its upload rather than
    /// orphaning the file), and returns the partial result. Without this, a
    /// dropped run future loses everything written-but-unflushed (#146 H16).
    pub cancel: Option<CancellationToken>,
    /// Delivery guarantee. `AtLeastOnce` (default) leaves the write path
    /// unchanged. `ExactlyOnce` enables the resume/skip + atomic-token path.
    pub delivery: crate::idempotency::DeliveryMode,
    /// Resume sequence read from the (unwrapped) exactly-once state value.
    /// Ignored unless `delivery == ExactlyOnce`. Defaults to 0.
    pub start_seq: u64,
    /// The source's replay capability, when the caller knows it.
    /// `Some(NonDeterministic)` steers an `ExactlyOnce` run away from the
    /// atomic-watermark skip path (whose correctness depends on positional
    /// replay) and onto the keyed-upsert mechanism when the sink is configured
    /// for it. `None` (default) trusts the caller — today's behaviour for
    /// library users driving `run_stream` directly. `Pipeline::run` always
    /// sets it from `Source::replay_guarantee()`.
    pub replay: Option<crate::idempotency::ReplayGuarantee>,
    /// Optional resilience policy (retry/backoff/circuit-breaker/poison).
    pub resilience: Option<crate::resilience::ResiliencePolicy>,
    /// Compiled schema-drift policy (issue #194). `None` → no drift handling.
    pub schema_drift: Option<crate::drift::SchemaDriftPolicy>,
}

impl RunStreamOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_state(mut self, store: Arc<dyn StateStore>, key: impl Into<String>) -> Self {
        self.state_store = Some(store);
        self.state_key = Some(key.into());
        self
    }

    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.pipeline_name = Some(name.into());
        self
    }

    pub fn with_row(mut self, row: impl Into<String>) -> Self {
        self.row = Some(row.into());
        self
    }

    pub fn with_run_id(mut self, id: impl Into<String>) -> Self {
        self.run_id = Some(id.into());
        self
    }

    pub fn with_dlq(mut self, dlq: DlqConfig) -> Self {
        self.dlq = Some(dlq);
        self
    }

    /// Attach a cancellation token for cooperative, flush-completing cancel.
    pub fn with_cancel(mut self, cancel: CancellationToken) -> Self {
        self.cancel = Some(cancel);
        self
    }

    /// Attach an adaptive batch-size controller config.
    pub fn with_adaptive(mut self, cfg: crate::adaptive::AdaptiveBatchConfig) -> Self {
        self.adaptive = Some(cfg);
        self
    }

    #[cfg(feature = "quality")]
    pub fn with_quality(
        mut self,
        quality: std::sync::Arc<crate::quality::CompiledQuality>,
    ) -> Self {
        self.quality = Some(quality);
        self
    }

    /// Attach a compiled data contract. The pass runs per page after the
    /// quality pass and before the schema-drift pass.
    #[cfg(feature = "contract")]
    pub fn with_contract(
        mut self,
        contract: std::sync::Arc<crate::contract::CompiledContract>,
    ) -> Self {
        self.contract = Some(contract);
        self
    }

    /// Attach a compiled masking policy. The pass runs per page *first* —
    /// before quality/contract/drift and every sink write.
    #[cfg(feature = "masking")]
    pub fn with_masking(
        mut self,
        masking: std::sync::Arc<crate::masking::CompiledMasking>,
    ) -> Self {
        self.masking = Some(masking);
        self
    }

    /// Set the delivery mode.
    pub fn with_delivery(mut self, mode: crate::idempotency::DeliveryMode) -> Self {
        self.delivery = mode;
        self
    }

    /// Declare the source's replay capability (see the `replay` field).
    pub fn with_replay_guarantee(mut self, replay: crate::idempotency::ReplayGuarantee) -> Self {
        self.replay = Some(replay);
        self
    }

    /// Set the resume sequence (exactly-once). Normally derived by
    /// `Pipeline::run` from the unwrapped state value.
    pub fn with_start_seq(mut self, seq: u64) -> Self {
        self.start_seq = seq;
        self
    }

    /// Attach a resilience policy to the run.
    pub fn with_resilience(mut self, policy: crate::resilience::ResiliencePolicy) -> Self {
        self.resilience = Some(policy);
        self
    }

    /// Attach a compiled schema-drift policy. The pass runs per page after the
    /// quality pass and before the sink write.
    pub fn with_schema_drift(mut self, policy: crate::drift::SchemaDriftPolicy) -> Self {
        self.schema_drift = Some(policy);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::RunStreamOptions;
    use crate::idempotency::{DeliveryMode, ReplayGuarantee};
    use crate::state::MemoryStateStore;
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;

    #[test]
    fn default_is_empty_at_least_once() {
        let o = RunStreamOptions::new();
        assert!(o.pipeline_name.is_none());
        assert!(o.row.is_none());
        assert!(o.run_id.is_none());
        assert!(o.state_store.is_none());
        assert!(o.state_key.is_none());
        assert!(o.cancel.is_none());
        assert_eq!(o.delivery, DeliveryMode::AtLeastOnce);
        assert_eq!(o.start_seq, 0);
        assert!(o.replay.is_none());
        assert!(o.resilience.is_none());
    }

    #[test]
    fn each_builder_sets_only_its_own_field() {
        let o = RunStreamOptions::new().with_name("p");
        assert_eq!(o.pipeline_name.as_deref(), Some("p"));
        assert!(o.row.is_none(), "with_name must not touch row");

        let o = RunStreamOptions::new().with_row("r1");
        assert_eq!(o.row.as_deref(), Some("r1"));
        assert!(o.pipeline_name.is_none());

        let o = RunStreamOptions::new().with_run_id("run-42");
        assert_eq!(o.run_id.as_deref(), Some("run-42"));

        let o = RunStreamOptions::new().with_state(Arc::new(MemoryStateStore::new()), "k");
        assert!(o.state_store.is_some());
        assert_eq!(o.state_key.as_deref(), Some("k"));

        let o = RunStreamOptions::new().with_cancel(CancellationToken::new());
        assert!(o.cancel.is_some());

        let o = RunStreamOptions::new().with_delivery(DeliveryMode::ExactlyOnce);
        assert_eq!(o.delivery, DeliveryMode::ExactlyOnce);

        let o = RunStreamOptions::new().with_start_seq(7);
        assert_eq!(o.start_seq, 7);

        let o = RunStreamOptions::new().with_replay_guarantee(ReplayGuarantee::Deterministic);
        assert_eq!(o.replay, Some(ReplayGuarantee::Deterministic));

        let o = RunStreamOptions::new().with_resilience(Default::default());
        assert!(o.resilience.is_some());
    }

    #[test]
    fn chaining_composes_all_set_fields() {
        let o = RunStreamOptions::new()
            .with_name("pipe")
            .with_row("row0")
            .with_run_id("rid")
            .with_state(Arc::new(MemoryStateStore::new()), "state-key")
            .with_delivery(DeliveryMode::ExactlyOnce)
            .with_start_seq(3);
        assert_eq!(o.pipeline_name.as_deref(), Some("pipe"));
        assert_eq!(o.row.as_deref(), Some("row0"));
        assert_eq!(o.run_id.as_deref(), Some("rid"));
        assert!(o.state_store.is_some());
        assert_eq!(o.state_key.as_deref(), Some("state-key"));
        assert_eq!(o.delivery, DeliveryMode::ExactlyOnce);
        assert_eq!(o.start_seq, 3);
    }
}
