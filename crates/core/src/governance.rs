//! Pipeline-owned governance assembly (RFC 0013, #875, #870).
//!
//! A [`Governance`] value carries every guarantee a run is held to — masking,
//! quality, contract, schema drift, data-flow policy, budget, profiling,
//! metadata columns, metering, resilience, cancellation and the DLQ — and one
//! builder assembles them in the canonical order. Every entry point (the CLI's
//! run / schedule / serve executor, topology, DLQ replay, `verify --repair`,
//! preview) builds the same value, so a pass cannot be forgotten by omission:
//! leaving one out is [`Governance::without`] with a reason, which is recorded
//! and logged.
//!
//! # The canonical order
//!
//! Per page, inside [`run_stream`](crate::pipeline::run_stream): masking →
//! quality → contract → schema drift, then the write. Around the sink,
//! outermost first ([`SINK_LAYERS`]):
//!
//! ```text
//! policy → profiling → [outer tap] → metadata → [inner tap] → budget → destination
//! ```
//!
//! - **policy** is outermost, so it classifies exactly the records about to be
//!   written (post-masking) and can quarantine a row before anything below
//!   sees it;
//! - **profiling** observes accepted rows without the `_faucet_*` columns;
//! - **metadata** stamps those columns closest to the destination;
//! - **budget** is innermost, so it counts what the destination accepts.
//!
//! The two taps are where a caller layers its own observers (the CLI's lineage
//! sampler sits at the outer tap, its `--limit` and fan-out capture at the
//! inner tap); they never reorder the governance layers.
//!
//! # The DLQ is a governed destination (#870)
//!
//! Records routed to the DLQ go through their own stack, assembled by
//! [`Pipeline::run`](crate::Pipeline::run) when a governance value is attached:
//!
//! ```text
//! masking (rules naming the DLQ) → policy (DLQ's attributes) → budget (shared) → metering → DLQ sink
//! ```
//!
//! A DLQ write the policy refuses fails the run; DLQ records and bytes count
//! against the same budget as the main sink and appear in the
//! [`UsageSnapshot`](crate::usage::UsageSnapshot); DLQ writes are retried under
//! `resilience:` when the DLQ sink's write is replay-safe.

pub(crate) mod apply;
mod page;
pub(crate) mod write;

#[cfg(feature = "arrow")]
pub(crate) use page::GovernedBatch;
pub(crate) use page::{Clearance, GovernedNative};
pub use page::{GovernedEnvelopes, GovernedPage};

use crate::budget::{BudgetSink, BudgetSpec, BudgetState, BudgetTimer};
use crate::dlq::DlqConfig;
use crate::error::FaucetError;
use crate::metadata::{CompiledMetadata, MetadataContext, MetadataSink};
use crate::profiling::{Profiler, ProfilingSink};
use crate::traits::{RowOutcome, Sink};
use crate::usage::UsageMeter;
use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

/// A governance pass a run is held to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Pass {
    /// PII masking (runs first on every page).
    Masking,
    /// Data-quality checks.
    Quality,
    /// The data contract.
    Contract,
    /// The schema-drift policy.
    SchemaDrift,
    /// The data-flow policy runtime backstop.
    Policy,
    /// Run budget ceilings.
    Budget,
    /// Column profiling.
    Profiling,
    /// `_faucet_*` metadata columns.
    Metadata,
    /// Usage metering.
    Metering,
    /// The resilience (retry / circuit breaker / poison) policy.
    Resilience,
    /// Cooperative cancellation.
    Cancel,
    /// The dead-letter queue.
    Dlq,
}

impl Pass {
    /// Every pass.
    pub const ALL: &'static [Pass] = &[
        Pass::Masking,
        Pass::Quality,
        Pass::Contract,
        Pass::SchemaDrift,
        Pass::Policy,
        Pass::Budget,
        Pass::Profiling,
        Pass::Metadata,
        Pass::Metering,
        Pass::Resilience,
        Pass::Cancel,
        Pass::Dlq,
    ];

    /// The pass's config spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            Pass::Masking => "masking",
            Pass::Quality => "quality",
            Pass::Contract => "contract",
            Pass::SchemaDrift => "schema_drift",
            Pass::Policy => "policy",
            Pass::Budget => "budget",
            Pass::Profiling => "profiling",
            Pass::Metadata => "metadata",
            Pass::Metering => "metering",
            Pass::Resilience => "resilience",
            Pass::Cancel => "cancel",
            Pass::Dlq => "dlq",
        }
    }
}

impl std::fmt::Display for Pass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A sink-side governance layer, in [`SINK_LAYERS`] order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SinkLayer {
    /// The data-flow policy backstop.
    Policy,
    /// Column profiling.
    Profiling,
    /// The caller's outer observer.
    OuterTap,
    /// `_faucet_*` metadata columns.
    Metadata,
    /// The caller's inner observer.
    InnerTap,
    /// Budget ceilings.
    Budget,
}

/// The canonical sink-side order, outermost first. A core invariant: the
/// builder applies the layers in exactly this order.
pub const SINK_LAYERS: &[SinkLayer] = &[
    SinkLayer::Policy,
    SinkLayer::Profiling,
    SinkLayer::OuterTap,
    SinkLayer::Metadata,
    SinkLayer::InnerTap,
    SinkLayer::Budget,
];

/// A pass a caller turned off on purpose, with the reason.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OptOut {
    /// The pass.
    pub pass: Pass,
    /// Why it does not apply to this entry point.
    pub reason: String,
}

/// The data-flow policy runtime backstop for one destination.
#[cfg(feature = "policy")]
#[derive(Clone)]
#[non_exhaustive]
pub struct PolicyGovernance {
    /// The compiled policy.
    pub policy: Arc<crate::policy::CompiledPolicy>,
    /// The destination the policy is evaluated against.
    pub facts: crate::policy::SinkFacts,
    /// Metric labels.
    pub scope: crate::policy::PolicyScope,
    /// The masking pass that ran upstream for this destination.
    pub masking: Option<Arc<crate::masking::CompiledMasking>>,
}

#[cfg(feature = "policy")]
impl PolicyGovernance {
    /// Enforce `policy` for the destination described by `facts`.
    pub fn new(
        policy: Arc<crate::policy::CompiledPolicy>,
        facts: crate::policy::SinkFacts,
        scope: crate::policy::PolicyScope,
    ) -> Self {
        Self {
            policy,
            facts,
            scope,
            masking: None,
        }
    }

    /// The masking pass that ran upstream, so a column it rewrote by name
    /// counts as masked.
    pub fn with_masking(mut self, masking: Arc<crate::masking::CompiledMasking>) -> Self {
        self.masking = Some(masking);
        self
    }

    /// Whether any rule quarantines at run time.
    pub fn quarantines(&self) -> bool {
        self.policy.quarantines()
    }
}

/// The DLQ as a governed destination (#870).
#[derive(Clone)]
#[non_exhaustive]
pub struct DlqGovernance {
    /// The DLQ wiring (sink, `on_batch_error`, failure budgets).
    pub config: DlqConfig,
    /// The DLQ destination as the policy sees it (its own `attributes`).
    #[cfg(feature = "policy")]
    pub facts: Option<crate::policy::SinkFacts>,
    /// Masking rules that name the DLQ (and not the main destination), applied
    /// to each envelope's payload.
    #[cfg(feature = "masking")]
    pub masking: Option<Arc<crate::masking::CompiledMasking>>,
}

impl DlqGovernance {
    /// Govern `config`'s sink.
    pub fn new(config: DlqConfig) -> Self {
        Self {
            config,
            #[cfg(feature = "policy")]
            facts: None,
            #[cfg(feature = "masking")]
            masking: None,
        }
    }

    /// The DLQ destination's identity and attributes for the policy.
    #[cfg(feature = "policy")]
    pub fn with_facts(mut self, facts: crate::policy::SinkFacts) -> Self {
        self.facts = Some(facts);
        self
    }

    /// Masking rules that apply only at the DLQ.
    #[cfg(feature = "masking")]
    pub fn with_masking(mut self, masking: Arc<crate::masking::CompiledMasking>) -> Self {
        if !masking.is_empty() {
            self.masking = Some(masking);
        }
        self
    }
}

/// Every guarantee a run is held to. Build with the `with_*` methods, opt out
/// of a pass with [`Governance::without`], wrap the destination with
/// [`Governance::sink_builder`], and attach with
/// [`Pipeline::with_governance`](crate::Pipeline::with_governance).
#[derive(Clone, Default)]
#[non_exhaustive]
pub struct Governance {
    #[cfg(feature = "masking")]
    masking: Option<Arc<crate::masking::CompiledMasking>>,
    #[cfg(feature = "quality")]
    quality: Option<Arc<crate::quality::CompiledQuality>>,
    #[cfg(feature = "contract")]
    contract: Option<Arc<crate::contract::CompiledContract>>,
    schema_drift: Option<crate::drift::SchemaDriftPolicy>,
    #[cfg(feature = "policy")]
    policy: Option<PolicyGovernance>,
    budget: Option<BudgetSpec>,
    budget_state: Option<Arc<BudgetState>>,
    profiler: Option<Arc<Mutex<Profiler>>>,
    metadata: Option<(CompiledMetadata, MetadataContext)>,
    usage_meter: Option<Arc<UsageMeter>>,
    resilience: Option<crate::resilience::ResiliencePolicy>,
    cancel: Option<CancellationToken>,
    dlq: Option<DlqGovernance>,
    opt_outs: Vec<OptOut>,
    sink_built: Arc<AtomicBool>,
}

impl std::fmt::Debug for Governance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Governance")
            .field("active", &self.active())
            .field("opt_outs", &self.opt_outs)
            .finish()
    }
}

impl Governance {
    /// No pass configured.
    pub fn new() -> Self {
        Self::default()
    }

    /// PII masking, applied first on every page. An empty policy is ignored.
    #[cfg(feature = "masking")]
    pub fn with_masking(mut self, masking: Arc<crate::masking::CompiledMasking>) -> Self {
        self.masking = (!masking.is_empty()).then_some(masking);
        self
    }

    /// Data-quality checks.
    #[cfg(feature = "quality")]
    pub fn with_quality(mut self, quality: Arc<crate::quality::CompiledQuality>) -> Self {
        self.quality = Some(quality);
        self
    }

    /// A data contract.
    #[cfg(feature = "contract")]
    pub fn with_contract(mut self, contract: Arc<crate::contract::CompiledContract>) -> Self {
        self.contract = Some(contract);
        self
    }

    /// A schema-drift policy.
    pub fn with_schema_drift(mut self, policy: crate::drift::SchemaDriftPolicy) -> Self {
        self.schema_drift = Some(policy);
        self
    }

    /// The data-flow policy backstop for the destination.
    #[cfg(feature = "policy")]
    pub fn with_policy(mut self, policy: PolicyGovernance) -> Self {
        self.policy = Some(policy);
        self
    }

    /// Budget ceilings. The main sink and the DLQ count against one shared
    /// state. An empty spec is ignored.
    pub fn with_budget(mut self, spec: BudgetSpec) -> Self {
        if spec.max_records.is_none()
            && spec.max_bytes.is_none()
            && spec.max_duration_secs.is_none()
        {
            self.budget = None;
            self.budget_state = None;
        } else {
            self.budget = Some(spec);
            self.budget_state = Some(Arc::new(BudgetState::default()));
        }
        self
    }

    /// Column profiling into `profiler`.
    pub fn with_profiler(mut self, profiler: Arc<Mutex<Profiler>>) -> Self {
        self.profiler = Some(profiler);
        self
    }

    /// `_faucet_*` metadata columns.
    pub fn with_metadata(mut self, meta: CompiledMetadata, ctx: MetadataContext) -> Self {
        self.metadata = Some((meta, ctx));
        self
    }

    /// Usage metering for the sink, the source and the DLQ.
    pub fn with_usage_meter(mut self, meter: Arc<UsageMeter>) -> Self {
        self.usage_meter = Some(meter);
        self
    }

    /// The resilience policy, for sink and DLQ writes.
    pub fn with_resilience(mut self, policy: crate::resilience::ResiliencePolicy) -> Self {
        self.resilience = Some(policy);
        self
    }

    /// The run's cooperative cancellation token (a duration budget cancels it
    /// too).
    pub fn with_cancel(mut self, cancel: CancellationToken) -> Self {
        self.cancel = Some(cancel);
        self
    }

    /// The DLQ, governed as a destination of its own.
    pub fn with_dlq(mut self, dlq: DlqGovernance) -> Self {
        self.dlq = Some(dlq);
        self
    }

    /// Turn `pass` off for this entry point, saying why. The opt-out is
    /// recorded ([`Governance::opt_outs`]) and logged.
    pub fn without(mut self, pass: Pass, reason: impl Into<String>) -> Self {
        let reason = reason.into();
        match pass {
            #[cfg(feature = "masking")]
            Pass::Masking => self.masking = None,
            #[cfg(feature = "quality")]
            Pass::Quality => self.quality = None,
            #[cfg(feature = "contract")]
            Pass::Contract => self.contract = None,
            Pass::SchemaDrift => self.schema_drift = None,
            #[cfg(feature = "policy")]
            Pass::Policy => self.policy = None,
            Pass::Budget => {
                self.budget = None;
                self.budget_state = None;
            }
            Pass::Profiling => self.profiler = None,
            Pass::Metadata => self.metadata = None,
            Pass::Metering => self.usage_meter = None,
            Pass::Resilience => self.resilience = None,
            Pass::Cancel => self.cancel = None,
            Pass::Dlq => self.dlq = None,
            #[allow(unreachable_patterns)]
            _ => {}
        }
        tracing::info!(pass = pass.as_str(), reason = %reason, "governance pass opted out");
        self.opt_outs.retain(|o| o.pass != pass);
        self.opt_outs.push(OptOut { pass, reason });
        self
    }

    /// The passes turned off with a reason.
    pub fn opt_outs(&self) -> &[OptOut] {
        &self.opt_outs
    }

    /// The reason `pass` was turned off, if it was.
    pub fn opted_out(&self, pass: Pass) -> Option<&str> {
        self.opt_outs
            .iter()
            .find(|o| o.pass == pass)
            .map(|o| o.reason.as_str())
    }

    /// Whether `pass` is configured.
    pub fn is_active(&self, pass: Pass) -> bool {
        match pass {
            #[cfg(feature = "masking")]
            Pass::Masking => self.masking.is_some(),
            #[cfg(feature = "quality")]
            Pass::Quality => self.quality.is_some(),
            #[cfg(feature = "contract")]
            Pass::Contract => self.contract.is_some(),
            Pass::SchemaDrift => self.schema_drift.is_some(),
            #[cfg(feature = "policy")]
            Pass::Policy => self.policy.is_some(),
            Pass::Budget => self.budget.is_some(),
            Pass::Profiling => self.profiler.is_some(),
            Pass::Metadata => self.metadata.is_some(),
            Pass::Metering => self.usage_meter.is_some(),
            Pass::Resilience => self.resilience.is_some(),
            Pass::Cancel => self.cancel.is_some(),
            Pass::Dlq => self.dlq.is_some(),
            #[allow(unreachable_patterns)]
            _ => false,
        }
    }

    /// The configured passes.
    pub fn active(&self) -> Vec<Pass> {
        Pass::ALL
            .iter()
            .copied()
            .filter(|p| self.is_active(*p))
            .collect()
    }

    /// The budget spec, when one is configured.
    pub fn budget(&self) -> Option<&BudgetSpec> {
        self.budget.as_ref()
    }

    /// The shared budget state the main sink and the DLQ count against.
    pub fn budget_state(&self) -> Option<&Arc<BudgetState>> {
        self.budget_state.as_ref()
    }

    /// The cancellation token.
    pub fn cancel(&self) -> Option<&CancellationToken> {
        self.cancel.as_ref()
    }

    /// The usage meter.
    pub fn usage_meter(&self) -> Option<&Arc<UsageMeter>> {
        self.usage_meter.as_ref()
    }

    /// The resilience policy.
    pub fn resilience(&self) -> Option<&crate::resilience::ResiliencePolicy> {
        self.resilience.as_ref()
    }

    /// The DLQ.
    pub fn dlq(&self) -> Option<&DlqGovernance> {
        self.dlq.as_ref()
    }

    /// The masking policy.
    #[cfg(feature = "masking")]
    pub fn masking(&self) -> Option<&Arc<crate::masking::CompiledMasking>> {
        self.masking.as_ref()
    }

    /// The quality checks.
    #[cfg(feature = "quality")]
    pub fn quality(&self) -> Option<&Arc<crate::quality::CompiledQuality>> {
        self.quality.as_ref()
    }

    /// The data contract.
    #[cfg(feature = "contract")]
    pub fn contract(&self) -> Option<&Arc<crate::contract::CompiledContract>> {
        self.contract.as_ref()
    }

    /// The schema-drift policy.
    pub fn schema_drift(&self) -> Option<crate::drift::SchemaDriftPolicy> {
        self.schema_drift
    }

    /// The policy backstop.
    #[cfg(feature = "policy")]
    pub fn policy(&self) -> Option<&PolicyGovernance> {
        self.policy.as_ref()
    }

    /// Whether a policy rule quarantines at run time — such a run takes the
    /// row path (#873).
    pub fn policy_quarantines(&self) -> bool {
        #[cfg(feature = "policy")]
        {
            self.policy
                .as_ref()
                .is_some_and(PolicyGovernance::quarantines)
        }
        #[cfg(not(feature = "policy"))]
        {
            false
        }
    }

    /// Whether any sink-side layer (policy, profiling, metadata, budget) is
    /// configured, so the destination must come from [`Self::sink_builder`].
    pub fn has_sink_layers(&self) -> bool {
        self.is_active(Pass::Budget)
            || self.is_active(Pass::Profiling)
            || self.is_active(Pass::Metadata)
            || self.policy_active()
    }

    fn policy_active(&self) -> bool {
        #[cfg(feature = "policy")]
        {
            self.policy.is_some()
        }
        #[cfg(not(feature = "policy"))]
        {
            false
        }
    }

    /// Whether [`Self::sink_builder`] built a destination for this value.
    pub fn sink_built(&self) -> bool {
        self.sink_built.load(Ordering::Relaxed)
    }

    /// The builder that wraps the destination in the canonical order.
    pub fn sink_builder(&self) -> GovernedSinkBuilder<'_> {
        GovernedSinkBuilder {
            gov: self,
            inner_tap: None,
            outer_tap: None,
        }
    }

    /// Run the per-page passes — masking → quality → contract → schema drift
    /// (against `sink`'s live schema) — over one page, outside a pipeline run.
    ///
    /// The returned [`GovernedOutput`] holds the governed survivors and the
    /// quarantine envelopes the passes built; write the page with
    /// [`Sink::write_governed`]. A pass that aborts (a quality `abort`, a
    /// contract `fail`, a drift `fail`) is the error. Each call starts from a
    /// fresh destination-schema read, so this suits one-off pages (tests,
    /// previews, conformance), not a page loop — use a
    /// [`Pipeline`](crate::Pipeline) for that.
    pub async fn govern<Si: Sink + ?Sized>(
        &self,
        records: Vec<Value>,
        sink: &Si,
    ) -> Result<GovernedOutput, FaucetError> {
        let specs = apply::GovernanceSpecs {
            #[cfg(feature = "masking")]
            masking: self.masking.as_ref(),
            #[cfg(feature = "quality")]
            quality: self.quality.as_ref(),
            #[cfg(feature = "contract")]
            contract: self.contract.as_ref(),
            schema_drift: self.schema_drift.as_ref(),
            pipeline: "unnamed",
            row: "",
            run_id: "",
            sink_name: sink.connector_name(),
            has_dlq: false,
        };
        let mut state = apply::GovernanceState::default();
        let out = apply::apply_governance(records, &specs, &mut state, sink).await?;
        Ok(GovernedOutput {
            page: out.records,
            envelopes: out.envelopes,
        })
    }

    /// The compat features this governance contributes to path selection.
    pub(crate) fn compat_features(&self) -> Vec<crate::compat::Feature> {
        use crate::compat::Feature as F;
        [
            (self.policy_quarantines(), F::PolicyQuarantine),
            (self.policy_active(), F::Policy),
            (self.is_active(Pass::Budget), F::Budget),
            (self.is_active(Pass::Profiling), F::Profiling),
            (self.is_active(Pass::Metadata), F::MetadataColumns),
        ]
        .into_iter()
        .filter_map(|(on, f)| on.then_some(f))
        .collect()
    }

    /// The DLQ wiring with its sink wrapped in the governed stack (#870):
    /// masking (rules naming the DLQ) → policy (the DLQ's attributes) → budget
    /// (shared with the main sink) → metering. `None` without a DLQ. For a
    /// caller that drives the write loop itself (a topology sink node).
    pub fn dlq_config(&self) -> Option<DlqConfig> {
        self.governed_dlq()
    }

    /// Wrap the DLQ sink in its governed stack (#870). Returns `None` when no
    /// DLQ is configured.
    pub(crate) fn governed_dlq(&self) -> Option<DlqConfig> {
        let dlq = self.dlq.as_ref()?;
        Some(govern_dlq(
            dlq,
            self.usage_meter.as_ref(),
            self.budget.as_ref().zip(self.budget_state.as_ref()),
            self.cancel.clone(),
            #[cfg(feature = "policy")]
            self.policy.as_ref(),
        ))
    }
}

/// What [`Governance::govern`] produced for one page.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct GovernedOutput {
    /// The records that survived every pass.
    pub page: GovernedPage<'static>,
    /// Dead-letter envelopes for the records a pass quarantined.
    pub envelopes: GovernedEnvelopes,
}

/// Wrap a DLQ's sink in the governed stack: masking → policy → budget →
/// metering → the DLQ sink.
pub(crate) fn govern_dlq(
    dlq: &DlqGovernance,
    meter: Option<&Arc<UsageMeter>>,
    budget: Option<(&BudgetSpec, &Arc<BudgetState>)>,
    cancel: Option<CancellationToken>,
    #[cfg(feature = "policy")] policy: Option<&PolicyGovernance>,
) -> DlqConfig {
    let mut sink: Box<dyn Sink> = Box::new(SharedSink(Arc::clone(&dlq.config.sink)));
    if let Some(m) = meter {
        sink = Box::new(DlqMeteredSink {
            inner: sink,
            meter: Arc::clone(m),
        });
    }
    if let Some((spec, state)) = budget {
        sink = Box::new(BudgetSink::wrap_shared(
            sink,
            spec.clone(),
            Arc::clone(state),
            cancel.unwrap_or_default(),
        ));
    }
    #[cfg(feature = "policy")]
    if let (Some(p), Some(facts)) = (policy, dlq.facts.as_ref()) {
        let mut ps = crate::policy::PolicySink::for_dlq(
            sink,
            Arc::clone(&p.policy),
            facts.clone(),
            p.scope.clone(),
        );
        if let Some(m) = &p.masking {
            ps = ps.with_masking(Arc::clone(m));
        }
        sink = Box::new(ps);
    }
    #[cfg(feature = "masking")]
    if let Some(m) = &dlq.masking {
        sink = Box::new(DlqMaskingSink {
            inner: sink,
            masking: Arc::clone(m),
        });
    }
    DlqConfig {
        sink: Arc::from(sink),
        ..dlq.config.clone()
    }
}

type Tap = Box<dyn FnOnce(Box<dyn Sink>) -> Box<dyn Sink> + Send>;

/// Wraps a destination in the canonical [`SINK_LAYERS`] order. Built by
/// [`Governance::sink_builder`].
pub struct GovernedSinkBuilder<'g> {
    gov: &'g Governance,
    inner_tap: Option<Tap>,
    outer_tap: Option<Tap>,
}

impl<'g> GovernedSinkBuilder<'g> {
    /// An observer layered between the budget and the metadata columns.
    pub fn inner_tap(
        mut self,
        tap: impl FnOnce(Box<dyn Sink>) -> Box<dyn Sink> + Send + 'static,
    ) -> Self {
        self.inner_tap = Some(Box::new(tap));
        self
    }

    /// An observer layered between the metadata columns and profiling.
    pub fn outer_tap(
        mut self,
        tap: impl FnOnce(Box<dyn Sink>) -> Box<dyn Sink> + Send + 'static,
    ) -> Self {
        self.outer_tap = Some(Box::new(tap));
        self
    }

    /// Wrap `destination`. Starts the duration-budget timer, which lives as
    /// long as the returned [`GovernedSink`].
    pub fn build(mut self, destination: Box<dyn Sink>) -> GovernedSink {
        let gov = self.gov;
        let mut layers = Vec::new();
        let mut sink = destination;
        let mut timer = None;
        for layer in SINK_LAYERS.iter().rev() {
            match layer {
                SinkLayer::Budget => {
                    if let (Some(spec), Some(state)) = (&gov.budget, &gov.budget_state) {
                        let cancel = gov.cancel.clone().unwrap_or_default();
                        timer = Some(BudgetTimer::start_for(
                            spec.max_duration_secs,
                            Arc::clone(state),
                            cancel.clone(),
                        ));
                        sink = Box::new(BudgetSink::wrap_shared(
                            sink,
                            spec.clone(),
                            Arc::clone(state),
                            cancel,
                        ));
                        layers.push(*layer);
                    }
                }
                SinkLayer::InnerTap => {
                    if let Some(tap) = self.inner_tap.take() {
                        sink = tap(sink);
                        layers.push(*layer);
                    }
                }
                SinkLayer::Metadata => {
                    if let Some((meta, ctx)) = &gov.metadata {
                        sink = Box::new(MetadataSink::new(sink, meta.clone(), ctx.clone()));
                        layers.push(*layer);
                    }
                }
                SinkLayer::OuterTap => {
                    if let Some(tap) = self.outer_tap.take() {
                        sink = tap(sink);
                        layers.push(*layer);
                    }
                }
                SinkLayer::Profiling => {
                    if let Some(p) = &gov.profiler {
                        sink = Box::new(ProfilingSink::new(sink, Arc::clone(p)));
                        layers.push(*layer);
                    }
                }
                SinkLayer::Policy =>
                {
                    #[cfg(feature = "policy")]
                    if let Some(p) = &gov.policy {
                        let mut ps = crate::policy::PolicySink::new(
                            sink,
                            Arc::clone(&p.policy),
                            p.facts.clone(),
                            p.scope.clone(),
                        );
                        if let Some(facts) = gov.dlq.as_ref().and_then(|d| d.facts.clone()) {
                            ps = ps.with_dlq_destination(facts);
                        }
                        if let Some(m) = &p.masking {
                            ps = ps.with_masking(Arc::clone(m));
                        }
                        sink = Box::new(ps);
                        layers.push(*layer);
                    }
                }
            }
        }
        layers.reverse();
        gov.sink_built.store(true, Ordering::Relaxed);
        GovernedSink {
            sink,
            budget: gov.budget_state.clone(),
            _timer: timer,
            layers,
        }
    }
}

/// A destination wrapped in its governance layers. It is a [`Sink`] itself;
/// keep it alive for the run (it owns the duration-budget timer).
pub struct GovernedSink {
    sink: Box<dyn Sink>,
    budget: Option<Arc<BudgetState>>,
    _timer: Option<BudgetTimer>,
    layers: Vec<SinkLayer>,
}

impl GovernedSink {
    /// The applied layers, outermost first.
    pub fn layers(&self) -> &[SinkLayer] {
        &self.layers
    }

    /// The budget state, when a budget is configured.
    pub fn budget_state(&self) -> Option<&Arc<BudgetState>> {
        self.budget.as_ref()
    }

    /// A duration ceiling cancels the run cooperatively, so the pipeline
    /// returns `Ok` with a partial result; turn that into the budget failure.
    pub fn finish<T>(&self, result: Result<T, FaucetError>) -> Result<T, FaucetError> {
        match (&result, self.budget.as_ref().and_then(|s| s.verdict())) {
            (Ok(_), Some(verdict)) => Err(verdict.error()),
            _ => result,
        }
    }

    /// Unwrap into the boxed sink (the duration timer stops).
    pub fn into_inner(self) -> Box<dyn Sink> {
        self.sink
    }
}

#[async_trait]
impl Sink for GovernedSink {
    crate::forward_sink!(sink);
}

/// An `Arc`-shared sink as an owned one.
pub(crate) struct SharedSink(pub(crate) Arc<dyn Sink>);

#[async_trait]
impl Sink for SharedSink {
    crate::forward_sink!(0);
}

/// Feeds DLQ writes into the usage meter.
struct DlqMeteredSink {
    inner: Box<dyn Sink>,
    meter: Arc<UsageMeter>,
}

#[async_trait]
impl Sink for DlqMeteredSink {
    crate::forward_sink!(inner; except write_batch, write_batch_reported, write_batch_partial);

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        let n = self.inner.write_batch(records).await?;
        self.meter
            .add_dlq_written(n as u64, crate::usage::estimate_page_bytes(records));
        Ok(n)
    }

    async fn write_batch_reported(
        &self,
        records: &[Value],
    ) -> Result<crate::accounting::WriteReport, FaucetError> {
        let r = self.inner.write_batch_reported(records).await?;
        self.meter.add_dlq_written(
            r.accepted as u64,
            crate::usage::estimate_page_bytes(records),
        );
        Ok(r)
    }

    async fn write_batch_partial(&self, records: &[Value]) -> Result<Vec<RowOutcome>, FaucetError> {
        let outcomes = self.inner.write_batch_partial(records).await?;
        let ok = outcomes.iter().filter(|o| o.is_ok()).count();
        self.meter
            .add_dlq_written(ok as u64, crate::usage::estimate_page_bytes(records));
        Ok(outcomes)
    }
}

/// Applies DLQ-only masking rules to each envelope's payload.
#[cfg(feature = "masking")]
struct DlqMaskingSink {
    inner: Box<dyn Sink>,
    masking: Arc<crate::masking::CompiledMasking>,
}

#[cfg(feature = "masking")]
impl DlqMaskingSink {
    fn mask(&self, records: &[Value]) -> Vec<Value> {
        let mut out = records.to_vec();
        let payloads: Vec<Value> = out
            .iter_mut()
            .map(|e| match e.get_mut("payload") {
                Some(p) => p.take(),
                None => Value::Null,
            })
            .collect();
        let masked = crate::masking::apply_masking(payloads, &self.masking).records;
        for (envelope, payload) in out.iter_mut().zip(masked) {
            if let Some(slot) = envelope.get_mut("payload") {
                *slot = payload;
            }
        }
        out
    }
}

#[cfg(feature = "masking")]
#[async_trait]
impl Sink for DlqMaskingSink {
    crate::forward_sink!(inner; except write_batch, write_batch_reported, write_batch_partial);

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        self.inner.write_batch(&self.mask(records)).await
    }

    async fn write_batch_reported(
        &self,
        records: &[Value],
    ) -> Result<crate::accounting::WriteReport, FaucetError> {
        self.inner.write_batch_reported(&self.mask(records)).await
    }

    async fn write_batch_partial(&self, records: &[Value]) -> Result<Vec<RowOutcome>, FaucetError> {
        self.inner.write_batch_partial(&self.mask(records)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::Source;
    use serde_json::json;
    use std::collections::HashMap;

    type Seen = Arc<Mutex<Vec<(&'static str, Value)>>>;

    /// Records the first record of every page it sees, then forwards.
    struct Tap {
        name: &'static str,
        inner: Box<dyn Sink>,
        seen: Seen,
    }

    #[async_trait]
    impl Sink for Tap {
        crate::forward_sink!(inner; except write_batch);
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            if let Some(r) = records.first() {
                self.seen.lock().unwrap().push((self.name, r.clone()));
            }
            self.inner.write_batch(records).await
        }
    }

    fn tap(
        name: &'static str,
        seen: &Seen,
    ) -> impl FnOnce(Box<dyn Sink>) -> Box<dyn Sink> + Send + 'static {
        let seen = Arc::clone(seen);
        move |inner| Box::new(Tap { name, inner, seen })
    }

    struct Dest(Seen);

    #[async_trait]
    impl Sink for Dest {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            for r in records {
                self.0.lock().unwrap().push(("dest", r.clone()));
            }
            Ok(records.len())
        }
    }

    fn profiler() -> Arc<Mutex<Profiler>> {
        Arc::new(Mutex::new(Profiler::new(
            serde_json::from_value(json!({})).unwrap(),
        )))
    }

    fn metadata() -> (CompiledMetadata, MetadataContext) {
        (
            CompiledMetadata::compile(&crate::metadata::MetadataColumnsSpec::default())
                .unwrap()
                .unwrap(),
            MetadataContext {
                run_id: "r".into(),
                source: "s".into(),
            },
        )
    }

    fn is_subsequence(layers: &[SinkLayer]) -> bool {
        let mut canon = SINK_LAYERS.iter();
        layers.iter().all(|l| canon.any(|c| c == l))
    }

    /// The core invariant (#875): the builder wraps the destination in exactly
    /// the canonical order — metadata below the outer tap and above the inner
    /// tap, the budget innermost, counting what the destination accepts.
    #[tokio::test]
    async fn layers_wrap_the_destination_in_the_canonical_order() {
        let (meta, ctx) = metadata();
        let g = Governance::new()
            .with_budget(BudgetSpec {
                max_records: Some(10),
                ..Default::default()
            })
            .with_metadata(meta, ctx)
            .with_profiler(profiler());
        let seen: Seen = Arc::default();
        let governed = g
            .sink_builder()
            .inner_tap(tap("inner", &seen))
            .outer_tap(tap("outer", &seen))
            .build(Box::new(Dest(Arc::clone(&seen))));
        assert_eq!(
            governed.layers(),
            &[
                SinkLayer::Profiling,
                SinkLayer::OuterTap,
                SinkLayer::Metadata,
                SinkLayer::InnerTap,
                SinkLayer::Budget,
            ]
        );
        assert!(is_subsequence(governed.layers()));
        assert!(g.sink_built());
        governed.write_batch(&[json!({"a": 1})]).await.unwrap();
        let seen = seen.lock().unwrap().clone();
        let names: Vec<&str> = seen.iter().map(|(n, _)| *n).collect();
        assert_eq!(names, ["outer", "inner", "dest"]);
        assert!(
            seen[0].1.get("_faucet_run_id").is_none(),
            "the outer tap sees the source record"
        );
        assert_eq!(
            seen[1].1["_faucet_run_id"], "r",
            "metadata is stamped above the inner tap"
        );
        assert_eq!(governed.budget_state().unwrap().records(), 1);
        assert!(governed.finish(Ok::<_, FaucetError>(())).is_ok());
        let _ = governed.into_inner();
    }

    #[tokio::test]
    async fn an_empty_governance_builds_the_bare_destination() {
        let g = Governance::new().with_budget(BudgetSpec::default());
        assert!(!g.has_sink_layers());
        assert!(g.budget().is_none());
        let governed = g.sink_builder().build(Box::new(Dest(Arc::default())));
        assert!(governed.layers().is_empty());
        assert!(governed.budget_state().is_none());
        assert_eq!(governed.write_batch(&[json!({})]).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn a_spent_duration_turns_the_partial_result_into_the_verdict() {
        let cancel = CancellationToken::new();
        let g = Governance::new()
            .with_cancel(cancel.clone())
            .with_budget(BudgetSpec {
                max_duration_secs: Some(1),
                ..Default::default()
            });
        let governed = g.sink_builder().build(Box::new(Dest(Arc::default())));
        tokio::time::timeout(std::time::Duration::from_secs(5), cancel.cancelled())
            .await
            .expect("the duration ceiling cancels the run");
        let err = governed.finish(Ok::<_, FaucetError>(())).unwrap_err();
        assert!(matches!(err, FaucetError::BudgetExceeded { .. }), "{err}");
    }

    #[test]
    fn opt_outs_clear_the_pass_and_are_recorded() {
        let (meta, ctx) = metadata();
        let g = Governance::new()
            .with_budget(BudgetSpec {
                max_records: Some(1),
                ..Default::default()
            })
            .with_metadata(meta, ctx)
            .with_profiler(profiler())
            .with_cancel(CancellationToken::new())
            .with_usage_meter(Arc::new(UsageMeter::new()))
            .with_resilience(crate::resilience::ResiliencePolicy::default())
            .with_schema_drift(crate::drift::SchemaDriftPolicy::compile(
                &serde_json::from_value(json!({})).unwrap(),
            ));
        for p in [
            Pass::Budget,
            Pass::Metadata,
            Pass::Profiling,
            Pass::Cancel,
            Pass::Metering,
            Pass::Resilience,
            Pass::SchemaDrift,
        ] {
            assert!(g.is_active(p), "{p}");
        }
        assert!(g.has_sink_layers());
        let g = g
            .without(Pass::Budget, "why")
            .without(Pass::Profiling, "first")
            .without(Pass::Profiling, "second")
            .without(Pass::Dlq, "none");
        assert!(!g.is_active(Pass::Budget));
        assert!(g.budget_state().is_none());
        assert!(!g.is_active(Pass::Profiling));
        assert_eq!(g.opted_out(Pass::Profiling), Some("second"));
        assert_eq!(g.opted_out(Pass::Budget), Some("why"));
        assert_eq!(g.opted_out(Pass::Masking), None);
        assert_eq!(g.opt_outs().len(), 3);
        assert!(g.active().contains(&Pass::Metadata));
        assert!(format!("{g:?}").contains("opt_outs"));
        for p in Pass::ALL {
            assert_eq!(p.to_string(), p.as_str());
        }
        let g = g
            .without(Pass::Metadata, "m")
            .without(Pass::Cancel, "c")
            .without(Pass::Metering, "u")
            .without(Pass::Resilience, "r")
            .without(Pass::SchemaDrift, "d");
        assert!(!g.has_sink_layers());
        assert!(g.cancel().is_none() && g.usage_meter().is_none() && g.resilience().is_none());
        assert!(g.schema_drift().is_none() && g.dlq().is_none());
    }

    struct Rows(Vec<Value>);

    #[async_trait]
    impl Source for Rows {
        async fn fetch_with_context(
            &self,
            _ctx: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(self.0.clone())
        }
    }

    /// A pipeline holding sink-side passes refuses to run a destination that
    /// did not come from the builder: the passes would silently not apply.
    #[tokio::test]
    async fn a_pipeline_refuses_an_unbuilt_destination() {
        let g = Governance::new().with_budget(BudgetSpec {
            max_records: Some(5),
            ..Default::default()
        });
        let source = Rows(vec![json!({"a": 1})]);
        let raw = Dest(Arc::default());
        let err = crate::Pipeline::new(&source, &raw)
            .with_governance(g.clone())
            .run()
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("Governance::sink_builder"),
            "{err}"
        );
        let governed = g.sink_builder().build(Box::new(raw));
        let r = crate::Pipeline::new(&source, &governed)
            .with_governance(g)
            .run()
            .await
            .unwrap();
        assert_eq!(r.records_written, 1);
    }

    /// Counts governed writes; the default forwards to the reported write.
    struct GovernedOnly {
        governed: std::sync::atomic::AtomicUsize,
        rows: Seen,
    }

    #[async_trait]
    impl Sink for GovernedOnly {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            for r in records {
                self.rows.lock().unwrap().push(("plain", r.clone()));
            }
            Ok(records.len())
        }
        async fn write_governed(
            &self,
            page: &GovernedPage<'_>,
        ) -> Result<crate::accounting::WriteReport, FaucetError> {
            self.governed
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.write_batch_reported(page.records()).await
        }
    }

    /// RFC 0014: the engine's plain write path hands the sink a governed page.
    #[tokio::test]
    async fn the_plain_write_path_writes_through_write_governed() {
        let source = Rows(vec![json!({"a": 1}), json!({"a": 2})]);
        let sink = GovernedOnly {
            governed: Default::default(),
            rows: Arc::default(),
        };
        let r = crate::Pipeline::new(&source, &sink).run().await.unwrap();
        assert_eq!(r.records_written, 2);
        assert_eq!(sink.governed.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(sink.rows.lock().unwrap().len(), 2);
    }

    /// A wrapper that changes the plain write keeps its change on the
    /// governed path: `write_governed` is not forwarded past it.
    #[tokio::test]
    async fn a_wrapper_changing_write_batch_keeps_the_governed_write_its_own() {
        let seen: Seen = Arc::default();
        let wrapped = Tap {
            name: "tap",
            inner: Box::new(Dest(Arc::clone(&seen))),
            seen: Arc::clone(&seen),
        };
        let page = Governance::new()
            .govern(vec![json!({"k": 1})], &wrapped)
            .await
            .unwrap();
        assert!(page.envelopes.is_empty());
        let report = wrapped.write_governed(&page.page).await.unwrap();
        assert_eq!(report.accepted, 1);
        let names: Vec<&str> = seen.lock().unwrap().iter().map(|(n, _)| *n).collect();
        assert_eq!(names, ["tap", "dest"]);
    }

    #[cfg(feature = "masking")]
    #[tokio::test]
    async fn govern_runs_the_passes_and_returns_a_governed_page() {
        let spec: crate::masking::MaskingSpec = serde_json::from_value(json!({
            "rules": [{"match": {"fields": ["email"]}, "action": {"type": "redact"}}]
        }))
        .unwrap();
        let g = Governance::new().with_masking(Arc::new(
            crate::masking::CompiledMasking::compile(&spec).unwrap(),
        ));
        let out = g
            .govern(vec![json!({"email": "a@b.co"})], &Dest(Arc::default()))
            .await
            .unwrap();
        assert_eq!(out.page.records()[0]["email"], "***");
    }

    #[cfg(feature = "quality")]
    #[tokio::test]
    async fn govern_fails_on_an_aborting_pass() {
        let spec: crate::quality::QualitySpec = serde_json::from_value(json!({
            "record": [{"type": "not_null", "field": "id", "on_failure": "abort"}]
        }))
        .unwrap();
        let g = Governance::new().with_quality(Arc::new(
            crate::quality::CompiledQuality::compile(&spec).unwrap(),
        ));
        let err = g
            .govern(vec![json!({"id": null})], &Dest(Arc::default()))
            .await
            .unwrap_err();
        assert!(matches!(err, FaucetError::QualityFailure { .. }), "{err}");
    }

    #[test]
    fn compat_features_follow_the_sink_layers() {
        use crate::compat::Feature as F;
        let (meta, ctx) = metadata();
        let g = Governance::new()
            .with_budget(BudgetSpec {
                max_bytes: Some(1),
                ..Default::default()
            })
            .with_metadata(meta, ctx)
            .with_profiler(profiler());
        let f = g.compat_features();
        assert!(f.contains(&F::Budget) && f.contains(&F::MetadataColumns));
        assert!(f.contains(&F::Profiling));
        assert!(!f.contains(&F::PolicyQuarantine));
        assert!(!g.policy_quarantines());
        assert!(!crate::compat::path_allowed(
            crate::compat::WritePath::Native,
            &f
        ));
        assert!(!crate::compat::path_allowed(
            crate::compat::WritePath::Columnar,
            &f
        ));
        assert!(crate::compat::path_allowed(
            crate::compat::WritePath::Row,
            &f
        ));
    }

    /// The reported and partial DLQ writes go through the same masking and
    /// metering as a plain write: the payload is masked and the accepted rows
    /// are counted as dead-lettered.
    #[cfg(feature = "masking")]
    #[tokio::test]
    async fn the_governed_dlq_masks_and_meters_every_write_kind() {
        let spec: crate::masking::MaskingSpec = serde_json::from_value(json!({
            "rules": [{"match": {"fields": ["email"]}, "action": {"type": "redact"}}]
        }))
        .unwrap();
        let masking = Arc::new(crate::masking::CompiledMasking::compile(&spec).unwrap());
        let seen: Seen = Arc::default();
        let meter = Arc::new(UsageMeter::new());
        let dlq = DlqGovernance::new(DlqConfig::new(Arc::new(Dest(Arc::clone(&seen)))))
            .with_masking(masking);
        let config = Governance::new()
            .with_usage_meter(Arc::clone(&meter))
            .with_dlq(dlq)
            .dlq_config()
            .unwrap();
        let envelope = || json!({"payload": {"email": "a@b.co"}});
        let report = config
            .sink
            .write_batch_reported(&[envelope()])
            .await
            .unwrap();
        assert_eq!(report.accepted, 1);
        let outcomes = config
            .sink
            .write_batch_partial(&[envelope(), envelope()])
            .await
            .unwrap();
        assert!(outcomes.iter().all(|o| o.is_ok()));
        let seen = seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 3);
        assert!(seen.iter().all(|(_, r)| r["payload"]["email"] == "***"));
        assert_eq!(meter.snapshot().dlq_records_written, 3);
    }

    /// Opting out of the record passes clears them.
    #[cfg(all(feature = "quality", feature = "contract", feature = "policy"))]
    #[test]
    fn the_record_passes_can_be_opted_out() {
        let quality: crate::quality::QualitySpec = serde_json::from_value(json!({})).unwrap();
        let contract: crate::contract::ContractSpec = serde_json::from_value(json!({
            "version": "1", "fields": [{"name": "id", "type": "integer"}]
        }))
        .unwrap();
        let policy: crate::policy::PolicySpec = serde_json::from_value(json!({
            "classifications": [], "rules": []
        }))
        .unwrap();
        let g = Governance::new()
            .with_quality(Arc::new(
                crate::quality::CompiledQuality::compile(&quality).unwrap(),
            ))
            .with_contract(Arc::new(
                crate::contract::CompiledContract::compile(&contract).unwrap(),
            ))
            .with_policy(PolicyGovernance::new(
                Arc::new(crate::policy::CompiledPolicy::compile(&policy).unwrap()),
                crate::policy::SinkFacts {
                    id: "w".into(),
                    kind: "stdout".into(),
                    attributes: Default::default(),
                },
                crate::policy::PolicyScope {
                    pipeline: "p".into(),
                    row: "w".into(),
                },
            ));
        assert!(g.quality().is_some() && g.contract().is_some() && g.policy().is_some());
        let g = g
            .without(Pass::Masking, "m")
            .without(Pass::Quality, "q")
            .without(Pass::Contract, "c")
            .without(Pass::Policy, "p");
        assert!(g.quality().is_none() && g.contract().is_none() && g.policy().is_none());
        assert_eq!(g.opt_outs().len(), 4);
    }

    #[cfg(feature = "masking")]
    #[tokio::test]
    async fn dlq_only_masking_rewrites_the_envelope_payload() {
        let spec: crate::masking::MaskingSpec = serde_json::from_value(json!({
            "rules": [{"match": {"fields": ["email"]}, "action": {"type": "redact"}}]
        }))
        .unwrap();
        let masking = Arc::new(crate::masking::CompiledMasking::compile(&spec).unwrap());
        let seen: Seen = Arc::default();
        let dlq = DlqGovernance::new(DlqConfig::new(Arc::new(Dest(Arc::clone(&seen)))))
            .with_masking(Arc::clone(&masking));
        let config = Governance::new().with_dlq(dlq).dlq_config().unwrap();
        let envelope = json!({"payload": {"email": "a@b.co"}, "reason": "partial"});
        config
            .sink
            .write_batch(&[envelope, json!({"reason": "no payload"})])
            .await
            .unwrap();
        let seen = seen.lock().unwrap().clone();
        assert_eq!(seen[0].1["payload"]["email"], "***");
        assert!(seen[1].1.get("payload").is_none());
        let elsewhere: crate::masking::MaskingSpec = serde_json::from_value(json!({
            "rules": [{"match": {"fields": ["x"]}, "action": {"type": "redact"},
                       "applies_to": ["elsewhere"]}]
        }))
        .unwrap();
        let empty = DlqGovernance::new(DlqConfig::new(Arc::new(Dest(Arc::default()))))
            .with_masking(Arc::new(
                crate::masking::CompiledMasking::compile_for_sink(&elsewhere, &["dlq"]).unwrap(),
            ));
        assert!(empty.masking.is_none(), "an empty policy is ignored");
        assert!(Governance::new().dlq_config().is_none());
    }
}
