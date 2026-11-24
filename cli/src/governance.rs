//! One place that turns a node + its caller's options into the run's
//! [`faucet_core::Governance`] (RFC 0013, #875).
//!
//! Every entry point that writes through a pipeline — `faucet run` /
//! `schedule` / `serve`, backfill, mirror, DLQ replay, `verify --repair` and
//! topology sink nodes — builds its governance here, so a pass is never left
//! out by omission. A pass that does not apply to a caller is turned off with
//! [`faucet_core::Governance::without`] and a reason: from the node's
//! [`opt_outs`](crate::expand::ExpandedNode::opt_outs) (a replay does not
//! re-mask, a repair does not profile) or from the run's scope (a `--dry-run`
//! / `--limit` / shard run is not profiled).

use crate::config::DlqSpec;
use crate::error::{CliError, CliResult};
use crate::executor::ExecuteOptions;
use crate::expand::{ExpandedNode, NodeRole};
use faucet_core::{DlqGovernance, Governance, Pass};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

/// The id a masking `applies_to` entry (or a policy) uses for the DLQ.
pub const DLQ_ID: &str = "dlq";

/// The per-invocation values governance needs beyond the node and options.
pub(crate) struct GovernanceRequest<'a> {
    pub node: &'a ExpandedNode,
    pub opts: &'a ExecuteOptions,
    pub pipeline: &'a str,
    pub row: &'a str,
    pub run_id: &'a str,
    pub cancel: &'a CancellationToken,
    pub meter: Option<Arc<faucet_core::UsageMeter>>,
}

/// The built governance plus the handle the caller reads after the run.
pub(crate) struct GovernanceBuild {
    pub governance: Governance,
    /// The column profiler, when profiling applies to this invocation.
    pub profiler: Option<Arc<Mutex<faucet_core::Profiler>>>,
}

/// Why profiling does not apply to an invocation, if it does not.
pub(crate) fn profiling_scope_opt_out(
    node: &ExpandedNode,
    opts: &ExecuteOptions,
) -> Option<&'static str> {
    if !matches!(node.role, NodeRole::Root) {
        Some("profiling learns a baseline from root rows only")
    } else if opts.dry_run {
        Some("a --dry-run writes nothing, so it has no data to profile")
    } else if opts.limit.is_some() {
        Some("a --limit run writes a sample, which would skew the baseline")
    } else if opts.shard.is_some() {
        Some("a shard writes a slice of the source, which would skew the baseline")
    } else {
        None
    }
}

/// The quality, contract and masking of one row, compiled.
#[derive(Default)]
struct RecordPasses {
    #[cfg(feature = "quality")]
    quality: Option<Arc<faucet_core::CompiledQuality>>,
    #[cfg(feature = "contract")]
    contract: Option<Arc<faucet_core::CompiledContract>>,
    #[cfg(feature = "masking")]
    masking: Option<Arc<faucet_core::CompiledMasking>>,
}

impl RecordPasses {
    fn compile(node: &ExpandedNode) -> CliResult<Self> {
        #[allow(unused_mut)]
        let mut passes = Self::default();
        #[cfg(feature = "quality")]
        if let Some(spec) = &node.quality {
            passes.quality = Some(Arc::new(
                faucet_core::CompiledQuality::compile(spec)
                    .map_err(|e| CliError::Config(format!("quality: {e}")))?,
            ));
        }
        #[cfg(feature = "contract")]
        if let Some(spec) = &node.contract {
            passes.contract = Some(Arc::new(
                faucet_core::CompiledContract::compile(spec)
                    .map_err(|e| CliError::Config(format!("contract: {e}")))?,
            ));
        }
        #[cfg(feature = "masking")]
        if let Some(spec) = &node.masking {
            passes.masking = Some(Arc::new(
                faucet_core::CompiledMasking::compile_for_sink(
                    spec,
                    &[node.sink_ref.as_str(), node.sink.kind.as_str()],
                )
                .map_err(|e| CliError::Config(format!("masking: {e}")))?,
            ));
        }
        let _ = node;
        Ok(passes)
    }
}

#[cfg(feature = "policy")]
fn compile_policy(
    spec: &faucet_core::PolicySpec,
    node: &ExpandedNode,
) -> CliResult<Arc<faucet_core::CompiledPolicy>> {
    faucet_core::CompiledPolicy::compile(&crate::policy::runtime_spec(spec, node))
        .map(Arc::new)
        .map_err(|e| CliError::Config(format!("policy: {e}")))
}

/// What one row's governance compiles to, kept for a run.
struct CompiledNode {
    /// Holds the row in place: its address is this entry's key.
    _node: Arc<ExpandedNode>,
    passes: tokio::sync::OnceCell<Arc<RecordPasses>>,
    #[cfg(feature = "policy")]
    policy: tokio::sync::OnceCell<Arc<faucet_core::CompiledPolicy>>,
}

/// The compiled governance of a run's rows: every invocation of a row (each
/// child of a fan-out) shares one compile of its quality, contract, masking
/// and policy, which depend on the row alone.
#[derive(Clone, Default)]
pub(crate) struct CompiledGovernance {
    rows: Arc<std::collections::HashMap<usize, CompiledNode>>,
}

impl CompiledGovernance {
    /// A cache for the rows of one run.
    pub(crate) fn for_rows<'a>(rows: impl Iterator<Item = &'a Arc<ExpandedNode>>) -> Self {
        Self {
            rows: Arc::new(
                rows.map(|node| {
                    (
                        Arc::as_ptr(node) as usize,
                        CompiledNode {
                            _node: Arc::clone(node),
                            passes: Default::default(),
                            #[cfg(feature = "policy")]
                            policy: Default::default(),
                        },
                    )
                })
                .collect(),
            ),
        }
    }

    /// Run `work` with this cache in reach of [`governance_for`].
    pub(crate) async fn scope<F: std::future::Future>(self, work: F) -> F::Output {
        COMPILED.scope(self, work).await
    }

    fn row(&self, node: &ExpandedNode) -> Option<&CompiledNode> {
        self.rows.get(&(std::ptr::from_ref(node) as usize))
    }
}

tokio::task_local! {
    static COMPILED: CompiledGovernance;
}

async fn record_passes(node: &ExpandedNode) -> CliResult<Arc<RecordPasses>> {
    let cache = COMPILED.try_with(Clone::clone).ok();
    match cache.as_ref().and_then(|c| c.row(node)) {
        Some(row) => row
            .passes
            .get_or_try_init(|| async { RecordPasses::compile(node).map(Arc::new) })
            .await
            .cloned(),
        None => RecordPasses::compile(node).map(Arc::new),
    }
}

#[cfg(feature = "policy")]
async fn compiled_policy(
    spec: &faucet_core::PolicySpec,
    node: &ExpandedNode,
) -> CliResult<Arc<faucet_core::CompiledPolicy>> {
    let cache = COMPILED.try_with(Clone::clone).ok();
    match cache.as_ref().and_then(|c| c.row(node)) {
        Some(row) => row
            .policy
            .get_or_try_init(|| async { compile_policy(spec, node) })
            .await
            .cloned(),
        None => compile_policy(spec, node),
    }
}

/// Build the governance for one invocation of `node`.
pub(crate) async fn governance_for(req: GovernanceRequest<'_>) -> CliResult<GovernanceBuild> {
    let GovernanceRequest {
        node,
        opts,
        pipeline,
        row,
        run_id,
        cancel,
        meter,
    } = req;
    let _ = (pipeline, row);
    let mut g = Governance::new().with_cancel(cancel.clone());
    if let Some(m) = meter {
        g = g.with_usage_meter(m);
    }
    if let Some(r) = opts.resilience.clone() {
        g = g.with_resilience(r);
    }
    if let Some(spec) = opts.budget.as_ref().filter(|b| !b.is_empty()) {
        g = match &opts.budget_state {
            Some(run) => g.with_budget_state(spec.clone(), Arc::clone(run)),
            None => g.with_budget(spec.clone()),
        };
    }
    #[cfg_attr(
        not(any(feature = "quality", feature = "contract", feature = "masking")),
        allow(unused_variables)
    )]
    let passes = record_passes(node).await?;
    #[cfg(feature = "quality")]
    if let Some(quality) = &passes.quality {
        g = g.with_quality(Arc::clone(quality));
    }
    #[cfg(feature = "contract")]
    if let Some(contract) = &passes.contract {
        g = g.with_contract(Arc::clone(contract));
    }
    #[cfg(feature = "masking")]
    let masking = passes.masking.clone();
    #[cfg(feature = "masking")]
    if let Some(compiled) = &masking {
        g = g.with_masking(Arc::clone(compiled));
    }
    if let Some(sd) = &node.schema {
        g = g.with_schema_drift(faucet_core::SchemaDriftPolicy::compile(sd));
    }
    if let Some(spec) = &node.metadata_columns
        && let Some(meta) = faucet_core::CompiledMetadata::compile(spec)
            .map_err(|e| CliError::Config(format!("metadata_columns: {e}")))?
    {
        g = g.with_metadata(
            meta,
            faucet_core::MetadataContext {
                run_id: run_id.to_string(),
                source: node.source.kind.clone(),
            },
        );
    }
    #[cfg(feature = "policy")]
    let mut classifier = None;
    #[cfg(feature = "policy")]
    if let Some(spec) = &node.policy {
        if crate::policy::quarantines(spec) && node.dlq.is_none() {
            return Err(CliError::Config(format!(
                "row '{}': policy: a rule with `on_runtime: quarantine` needs a `dlq:` block to \
                 route the quarantined records to",
                node.id
            )));
        }
        let compiled = compiled_policy(spec, node).await?;
        classifier = Some(Arc::clone(&compiled));
        let mut p = faucet_core::PolicyGovernance::new(
            compiled,
            crate::policy::sink_facts(node),
            faucet_core::PolicyScope {
                pipeline: pipeline.to_string(),
                row: row.to_string(),
            },
        );
        #[cfg(feature = "masking")]
        if let Some(m) = &masking {
            p = p.with_masking(Arc::clone(m));
        }
        g = g.with_policy(p);
    }
    if let Some(dlq) = &node.dlq {
        let config = if opts.dry_run {
            crate::executor::preview_dlq_config(dlq)
        } else {
            crate::executor::build_dlq_config(dlq).await?
        };
        #[allow(unused_mut)]
        let mut d = dlq_governance(node, dlq, config)?;
        #[cfg(feature = "masking")]
        if let Some(spec) = &node.masking
            && let Some(p) = crate::dlq_replay::proof::signer(
                spec,
                &[node.sink_ref.as_str(), node.sink.kind.as_str()],
                opts.state_scope.namespace.as_deref(),
            )
        {
            d = d.with_masking_proof(Arc::new(p));
        }
        g = g.with_dlq(d);
    }
    let mut profiler = None;
    if let Some(spec) = &node.profiling {
        match profiling_scope_opt_out(node, opts) {
            Some(reason) => g = g.without(Pass::Profiling, reason),
            None => {
                #[allow(unused_mut)]
                let mut profiler_value = faucet_core::Profiler::new(spec.clone());
                #[cfg(feature = "policy")]
                if let Some(c) = classifier.clone() {
                    profiler_value = profiler_value.withhold_classified_values(c);
                }
                let p = Arc::new(Mutex::new(profiler_value));
                g = g.with_profiler(Arc::clone(&p));
                profiler = Some(p);
            }
        }
    }
    for o in &node.opt_outs {
        if o.pass == Pass::Profiling {
            profiler = None;
        }
        g = g.without(o.pass, o.reason.clone());
    }
    Ok(GovernanceBuild {
        governance: g,
        profiler,
    })
}

/// The DLQ as a governed destination (#870): its own `attributes` for the
/// policy, and the masking rules that name it (and not the main destination).
pub(crate) fn dlq_governance(
    node: &ExpandedNode,
    dlq: &DlqSpec,
    config: faucet_core::DlqConfig,
) -> CliResult<DlqGovernance> {
    dlq_governance_for(
        DlqTarget {
            #[cfg(feature = "masking")]
            masking: node.masking.as_ref(),
            #[cfg(feature = "policy")]
            policy: node.policy.is_some(),
            #[cfg(feature = "policy")]
            attested: node.dlq_attested,
            main_ids: &[node.sink_ref.as_str(), node.sink.kind.as_str()],
        },
        dlq,
        config,
    )
}

/// What the DLQ is governed against: the run's masking spec, whether a policy
/// is configured, and the ids of the main destination.
pub(crate) struct DlqTarget<'a> {
    #[cfg(feature = "masking")]
    pub masking: Option<&'a faucet_core::MaskingSpec>,
    #[cfg(feature = "policy")]
    pub policy: bool,
    /// Whether the policy owner trusts the DLQ's `attributes`.
    #[cfg(feature = "policy")]
    pub attested: bool,
    pub main_ids: &'a [&'a str],
}

/// [`dlq_governance`] for any caller (a matrix row or a topology sink node).
pub(crate) fn dlq_governance_for(
    target: DlqTarget<'_>,
    dlq: &DlqSpec,
    config: faucet_core::DlqConfig,
) -> CliResult<DlqGovernance> {
    #[allow(unused_mut)]
    let mut d = DlqGovernance::new(config);
    #[cfg(feature = "policy")]
    if target.policy {
        d = d.with_facts(crate::policy::dlq_facts(dlq, target.attested));
    }
    #[cfg(feature = "masking")]
    if let Some(spec) = target.masking
        && let Some(only) = dlq_only_masking(spec, target.main_ids, dlq)
    {
        d = d.with_masking(Arc::new(
            faucet_core::CompiledMasking::compile(&only)
                .map_err(|e| CliError::Config(format!("masking: {e}")))?,
        ));
    }
    #[cfg(not(any(feature = "policy", feature = "masking")))]
    let _ = (target, dlq);
    Ok(d)
}

/// One topology sink node's governance request.
pub(crate) struct TopologySinkRequest<'a> {
    pub cfg: &'a crate::config::PipelineConfig,
    pub node_id: &'a str,
    pub kind: &'a str,
    pub template: &'a str,
    pub attributes: std::collections::BTreeMap<String, String>,
    pub run_id: &'a str,
    pub source_label: String,
    pub budget: Option<&'a faucet_core::BudgetSpec>,
    /// The graph's run budget, shared by every sink node.
    pub budget_state: Option<Arc<faucet_core::BudgetState>>,
    pub cancel: CancellationToken,
    pub dry_run: bool,
    pub preview: bool,
}

/// The sink-side governance of one topology sink node (#875): budget, metadata
/// columns, profiling and the policy backstop. The page passes (masking,
/// quality, contract, drift, resilience) run in the topology's own sink loop
/// from [`faucet_core::TopologyGovernance`]; the DLQ is attached with
/// [`topology_dlq`] once it is built.
pub(crate) fn topology_sink_governance(req: TopologySinkRequest<'_>) -> CliResult<GovernanceBuild> {
    let cfg = req.cfg;
    let mut g = Governance::new().with_cancel(req.cancel.clone());
    if let Some(spec) = req.budget.filter(|b| !b.is_empty()) {
        g = match &req.budget_state {
            Some(run) => g.with_budget_state(spec.clone(), Arc::clone(run)),
            None => g.with_budget(spec.clone()),
        };
        if req.dry_run {
            g = g.without(Pass::Budget, "a --dry-run writes nothing to count");
        }
    }
    if let Some(spec) = &cfg.metadata_columns
        && let Some(meta) = faucet_core::CompiledMetadata::compile(spec)
            .map_err(|e| CliError::Config(format!("metadata_columns: {e}")))?
    {
        g = g.with_metadata(
            meta,
            faucet_core::MetadataContext {
                run_id: req.run_id.to_string(),
                source: req.source_label.clone(),
            },
        );
    }
    let mut profiler = None;
    if let Some(spec) = &cfg.profiling {
        if req.preview {
            g = g.without(
                Pass::Profiling,
                "a preview's volumes are synthetic, so they would skew the baseline",
            );
        } else {
            let p = Arc::new(Mutex::new(faucet_core::Profiler::new(spec.clone())));
            g = g.with_profiler(Arc::clone(&p));
            profiler = Some(p);
        }
    }
    #[cfg(feature = "policy")]
    if let Some(policy_spec) = cfg.policy.as_ref() {
        if crate::policy::quarantines(policy_spec) && cfg.pipeline.dlq.is_none() && !req.dry_run {
            return Err(CliError::Config(format!(
                "node '{}': policy: a rule with `on_runtime: quarantine` needs a `dlq:` block \
                 to route the quarantined records to",
                req.node_id
            )));
        }
        let compiled = Arc::new(
            faucet_core::CompiledPolicy::compile(&crate::policy::topology_runtime_spec(
                policy_spec,
                cfg,
            ))
            .map_err(|e| CliError::Config(format!("policy: {e}")))?,
        );
        if let Some(prof) = &profiler {
            let mut guard = prof.lock().unwrap_or_else(|e| e.into_inner());
            let taken =
                std::mem::replace(&mut *guard, faucet_core::Profiler::new(Default::default()));
            *guard = taken.withhold_classified_values(Arc::clone(&compiled));
        }
        #[allow(unused_mut)]
        let mut p = faucet_core::PolicyGovernance::new(
            compiled,
            faucet_core::SinkFacts::new(req.node_id, req.kind, req.attributes.clone())
                .with_attested(cfg.topology_destination_trust(req.node_id, req.template).0),
            faucet_core::PolicyScope {
                pipeline: cfg.name.clone().unwrap_or_default(),
                row: req.node_id.to_string(),
            },
        );
        #[cfg(feature = "masking")]
        if let Some(masking) = cfg.pipeline.masking.as_ref() {
            p = p.with_masking(Arc::new(
                faucet_core::CompiledMasking::compile_for_sink(
                    masking,
                    &[req.node_id, req.template, req.kind],
                )
                .map_err(|e| CliError::Config(format!("masking: {e}")))?,
            ));
        }
        g = g.with_policy(p);
        if req.dry_run {
            g = g.without(
                Pass::Policy,
                "a --dry-run writes nothing; the static policy pass still ran",
            );
        }
    }
    Ok(GovernanceBuild {
        governance: g,
        profiler,
    })
}

/// A topology sink node's governed DLQ (#870): the shared DLQ sink wrapped
/// with that node's budget, policy facts and DLQ-only masking.
pub(crate) fn topology_dlq(
    governance: &Governance,
    cfg: &crate::config::PipelineConfig,
    main_ids: &[&str],
    dlq: &DlqSpec,
    config: faucet_core::DlqConfig,
) -> CliResult<faucet_core::DlqConfig> {
    let d = dlq_governance_for(
        DlqTarget {
            #[cfg(feature = "masking")]
            masking: cfg.pipeline.masking.as_ref(),
            #[cfg(feature = "policy")]
            policy: cfg.policy.is_some(),
            #[cfg(feature = "policy")]
            attested: cfg.attribute_trust.attests(&["/pipeline/dlq".to_owned()]),
            main_ids,
        },
        dlq,
        config,
    )?;
    #[cfg(not(any(feature = "policy", feature = "masking")))]
    let _ = cfg;
    Ok(governance
        .clone()
        .with_dlq(d)
        .dlq_config()
        .expect("a DLQ was just attached"))
}

/// The governance of a preview — `faucet preview`, a topology preview and the
/// MCP `preview` / dry-run tools (#875). A preview writes nothing, so only
/// masking applies (every rule: there is no destination to scope by); the
/// rest are opted out by name.
#[cfg(feature = "masking")]
pub fn preview_governance(masking: Option<&faucet_core::MaskingSpec>) -> CliResult<Governance> {
    let mut g = Governance::new();
    if let Some(spec) = masking {
        g = g.with_masking(Arc::new(
            faucet_core::CompiledMasking::compile(spec)
                .map_err(|e| CliError::Config(format!("masking: {e}")))?,
        ));
    }
    Ok(g.without(Pass::Budget, PREVIEW_WRITES_NOTHING)
        .without(Pass::Policy, PREVIEW_WRITES_NOTHING)
        .without(Pass::Dlq, PREVIEW_WRITES_NOTHING)
        .without(Pass::Profiling, PREVIEW_WRITES_NOTHING)
        .without(Pass::Cancel, "a preview is bounded by its own deadline"))
}

/// The reason a preview opts out of the write-side passes.
pub const PREVIEW_WRITES_NOTHING: &str = "a preview writes nothing; it only shows masked records";

/// The ids masking `applies_to` / policy facts use for a node's DLQ.
pub fn dlq_ids(dlq: &DlqSpec) -> [&str; 2] {
    [DLQ_ID, dlq.sink.kind.as_str()]
}

/// The masking rules that apply at the DLQ only: they name the DLQ in
/// `applies_to` and name neither the main sink template nor its kind (those
/// already ran on the page, and masking twice is not idempotent).
#[cfg(feature = "masking")]
pub fn dlq_only_masking(
    spec: &faucet_core::MaskingSpec,
    main: &[&str],
    dlq: &DlqSpec,
) -> Option<faucet_core::MaskingSpec> {
    let at_dlq = dlq_ids(dlq);
    let mut only = spec.clone();
    only.rules.retain(|r| {
        !r.applies_to.is_empty()
            && r.applies_to.iter().any(|t| at_dlq.contains(&t.as_str()))
            && !r.applies_to.iter().any(|t| main.contains(&t.as_str()))
    });
    (!only.rules.is_empty()).then_some(only)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PipelineConfig;
    use crate::executor::ExecuteOptions;

    fn cfg(extra: &str) -> PipelineConfig {
        let dir = std::env::temp_dir();
        let yaml = format!(
            "version: 1\nname: g\npipeline:\n  source: {{ type: rest, config: {{ url: \"http://x\" }} }}\n  sink: {{ type: stdout, config: {{}}, attributes: {{ residency: eu }} }}\n{extra}",
        );
        let _ = dir;
        PipelineConfig::from_text(&yaml, std::path::Path::new("g.yaml")).unwrap()
    }

    fn node(c: &PipelineConfig) -> ExpandedNode {
        crate::expand::expand(c).unwrap().remove(0)
    }

    async fn build(n: &ExpandedNode, opts: &ExecuteOptions) -> CliResult<GovernanceBuild> {
        governance_for(GovernanceRequest {
            node: n,
            opts,
            pipeline: "g",
            row: &n.id,
            run_id: "r",
            cancel: &CancellationToken::new(),
            meter: Some(Arc::new(faucet_core::UsageMeter::new())),
        })
        .await
    }

    const DLQ: &str = "  dlq:\n    sink: { type: jsonl, config: { path: /tmp/faucet-gov-test-dlq.jsonl }, attributes: { residency: us } }\n";

    /// #875: one function builds every pass the node and its caller's options
    /// configure — budget, policy, cancel, metering, resilience, the page
    /// passes, metadata columns, profiling and the governed DLQ.
    #[cfg(feature = "sink-jsonl")]
    #[tokio::test]
    async fn every_configured_pass_is_built() {
        let c = cfg(&format!(
            "{DLQ}  quality:\n    record:\n      - {{ type: not_null, field: id, on_failure: quarantine }}\n  contract:\n    version: \"1\"\n    fields:\n      - {{ name: id, type: integer }}\n  masking:\n    rules:\n      - {{ match: {{ fields: [email] }}, action: {{ type: hash }} }}\n      - {{ match: {{ fields: [note] }}, action: {{ type: redact }}, applies_to: [dlq] }}\n  schema: {{ on_drift: warn }}\n  state: {{ type: memory }}\nmetadata_columns: {{}}\nprofiling: {{}}\npolicy:\n  classifications:\n    - {{ label: pii, fields: [email] }}\n  rules:\n    - {{ name: pii-eu, when: {{ label: pii }}, require: {{ residency: [eu] }}, on_runtime: quarantine }}\n"
        ));
        let n = node(&c);
        let mut opts = ExecuteOptions::for_tests("g");
        opts.budget = Some(faucet_core::BudgetSpec {
            max_records: Some(10),
            ..Default::default()
        });
        opts.resilience = Some(faucet_core::ResiliencePolicy::default());
        let built = build(&n, &opts).await.unwrap();
        let g = &built.governance;
        for p in [
            Pass::Budget,
            Pass::Cancel,
            Pass::Metering,
            Pass::Resilience,
            Pass::Quality,
            Pass::Contract,
            Pass::Masking,
            Pass::SchemaDrift,
            Pass::Metadata,
            Pass::Profiling,
            Pass::Policy,
            Pass::Dlq,
        ] {
            assert!(g.is_active(p), "{p}");
        }
        assert!(g.opt_outs().is_empty(), "{:?}", g.opt_outs());
        assert!(built.profiler.is_some());
        assert!(g.policy_quarantines());
        let dlq = g.dlq().unwrap();
        assert_eq!(dlq.facts.as_ref().unwrap().id, DLQ_ID);
        assert!(dlq.masking.is_some(), "the rule naming the DLQ reaches it");
        assert!(g.dlq_config().is_some());
    }

    /// Every invocation of a row in one run shares one compile of its
    /// passes; a copy of the row, or a call outside a run, compiles its own.
    #[cfg(all(feature = "masking", feature = "quality", feature = "contract"))]
    #[tokio::test]
    async fn a_rows_passes_are_compiled_once_per_run() {
        let c = cfg(
            "  quality:\n    record:\n      - { type: not_null, field: id, on_failure: abort }\n  contract:\n    version: \"1\"\n    fields:\n      - { name: id, type: integer }\n  masking:\n    rules:\n      - { match: { fields: [email] }, action: { type: hash } }\n",
        );
        let shared = Arc::new(node(&c));
        let copy = (*shared).clone();
        let cache = CompiledGovernance::for_rows(std::iter::once(&shared));
        let (first, second, of_copy) = cache
            .clone()
            .scope(async {
                (
                    record_passes(&shared).await.unwrap(),
                    record_passes(&shared).await.unwrap(),
                    record_passes(&copy).await.unwrap(),
                )
            })
            .await;
        assert!(Arc::ptr_eq(&first, &second));
        assert!(!Arc::ptr_eq(&first, &of_copy));
        assert!(first.quality.is_some() && first.contract.is_some() && first.masking.is_some());
        let outside = record_passes(&shared).await.unwrap();
        assert!(!Arc::ptr_eq(&first, &outside));

        let opts = ExecuteOptions::for_tests("g");
        let built = cache
            .scope(async { build(&shared, &opts).await })
            .await
            .unwrap();
        for p in [Pass::Quality, Pass::Contract, Pass::Masking] {
            assert!(built.governance.is_active(p), "{p}");
        }
    }

    /// A pass that fails to compile fails every invocation with the same
    /// error; nothing broken is kept.
    #[cfg(feature = "masking")]
    #[tokio::test]
    async fn a_pass_that_does_not_compile_is_reported_each_time() {
        let c = cfg(
            "  masking:\n    rules:\n      - { match: { fields: [email] }, action: { type: hash } }\n",
        );
        let mut n = node(&c);
        n.masking.as_mut().unwrap().rules[0].matcher.field_pattern = Some("(".into());
        let shared = Arc::new(n);
        let cache = CompiledGovernance::for_rows(std::iter::once(&shared));
        let alone = record_passes(&shared).await.err().unwrap().to_string();
        let (first, second) = cache
            .scope(async {
                (
                    record_passes(&shared).await.err().unwrap().to_string(),
                    record_passes(&shared).await.err().unwrap().to_string(),
                )
            })
            .await;
        assert!(alone.contains("masking: "), "{alone}");
        assert_eq!(first, alone);
        assert_eq!(second, alone);
    }

    #[cfg(feature = "policy")]
    #[tokio::test]
    async fn a_rows_policy_is_compiled_once_per_run() {
        let c = cfg(
            "policy:\n  classifications:\n    - { label: pii, fields: [email] }\n  rules:\n    - { name: pii-eu, when: { label: pii }, require: { residency: [eu] } }\n",
        );
        let shared = Arc::new(node(&c));
        let spec = shared.policy.clone().unwrap();
        let cache = CompiledGovernance::for_rows(std::iter::once(&shared));
        let (first, second) = cache
            .scope(async {
                (
                    compiled_policy(&spec, &shared).await.unwrap(),
                    compiled_policy(&spec, &shared).await.unwrap(),
                )
            })
            .await;
        assert!(Arc::ptr_eq(&first, &second));
        let outside = compiled_policy(&spec, &shared).await.unwrap();
        assert!(!Arc::ptr_eq(&first, &outside));
    }

    /// A pass that does not apply is opted out by name, never left out.
    #[tokio::test]
    async fn out_of_scope_passes_are_opted_out_with_a_reason() {
        let c = cfg("  state: { type: memory }\nprofiling: {}\n");
        let mut n = node(&c);
        n.opt_outs.push(faucet_core::OptOut {
            pass: Pass::Masking,
            reason: "already masked".into(),
        });
        let mut opts = ExecuteOptions::for_tests("g");
        opts.dry_run = true;
        let built = build(&n, &opts).await.unwrap();
        assert!(built.profiler.is_none());
        assert!(
            built
                .governance
                .opted_out(Pass::Profiling)
                .is_some_and(|r| r.contains("--dry-run"))
        );
        assert_eq!(
            built.governance.opted_out(Pass::Masking),
            Some("already masked")
        );
        let mut repair = n.clone();
        repair.opt_outs.push(faucet_core::OptOut {
            pass: Pass::Profiling,
            reason: "repair".into(),
        });
        let built = build(&repair, &ExecuteOptions::for_tests("g"))
            .await
            .unwrap();
        assert!(built.profiler.is_none(), "an opt-out drops the profiler");
        assert_eq!(built.governance.opted_out(Pass::Profiling), Some("repair"));
    }

    #[test]
    fn profiling_scope_names_why_it_does_not_apply() {
        let n = node(&cfg(""));
        let mut opts = ExecuteOptions::for_tests("g");
        assert!(profiling_scope_opt_out(&n, &opts).is_none());
        opts.limit = Some(1);
        assert!(
            profiling_scope_opt_out(&n, &opts)
                .unwrap()
                .contains("--limit")
        );
        opts.limit = None;
        opts.shard = Some(faucet_core::ShardSpec::whole());
        assert!(
            profiling_scope_opt_out(&n, &opts)
                .unwrap()
                .contains("shard")
        );
        let mut child = n.clone();
        child.role = NodeRole::Child {
            parent_id: "p".into(),
            parent_key: "id".into(),
        };
        assert!(
            profiling_scope_opt_out(&child, &ExecuteOptions::for_tests("g"))
                .unwrap()
                .contains("root")
        );
    }

    #[tokio::test]
    async fn a_policy_quarantine_needs_a_dlq() {
        let c = cfg(
            "policy:\n  classifications:\n    - { label: pii, fields: [email] }\n  rules:\n    - { name: r, when: { label: pii }, require: { residency: [eu] }, on_runtime: quarantine }\n",
        );
        let err = build(&node(&c), &ExecuteOptions::for_tests("g"))
            .await
            .err()
            .unwrap();
        assert!(err.to_string().contains("needs a `dlq:` block"), "{err}");
    }

    #[test]
    fn dlq_only_masking_keeps_the_rules_that_name_only_the_dlq() {
        let c = cfg(&format!(
            "{DLQ}  masking:\n    rules:\n      - {{ match: {{ fields: [a] }}, action: {{ type: redact }} }}\n      - {{ match: {{ fields: [b] }}, action: {{ type: redact }}, applies_to: [dlq] }}\n      - {{ match: {{ fields: [c] }}, action: {{ type: redact }}, applies_to: [jsonl] }}\n      - {{ match: {{ fields: [d] }}, action: {{ type: redact }}, applies_to: [dlq, stdout] }}\n"
        ));
        let n = node(&c);
        let dlq = n.dlq.as_ref().unwrap();
        let only = dlq_only_masking(
            n.masking.as_ref().unwrap(),
            &[n.sink_ref.as_str(), n.sink.kind.as_str()],
            dlq,
        )
        .unwrap();
        let fields: Vec<String> = only
            .rules
            .iter()
            .flat_map(|r| r.matcher.fields.clone())
            .collect();
        assert_eq!(fields, ["b", "c"]);
        assert_eq!(dlq_ids(dlq), ["dlq", "jsonl"]);
        let none = cfg(&format!(
            "{DLQ}  masking:\n    rules:\n      - {{ match: {{ fields: [a] }}, action: {{ type: redact }} }}\n"
        ));
        let n = node(&none);
        assert!(
            dlq_only_masking(
                n.masking.as_ref().unwrap(),
                &["default"],
                n.dlq.as_ref().unwrap()
            )
            .is_none()
        );
    }

    /// A topology sink node gets the same sink layers, with the preview opt-outs.
    #[tokio::test]
    async fn a_topology_sink_node_gets_the_same_layers() {
        let c = cfg(
            "metadata_columns: {}\nprofiling: {}\npolicy:\n  classifications:\n    - { label: pii, fields: [email] }\n  rules:\n    - { name: r, when: { label: pii }, require: { residency: [eu] } }\n",
        );
        let budget = faucet_core::BudgetSpec {
            max_records: Some(3),
            ..Default::default()
        };
        let req = |dry_run: bool, preview: bool| TopologySinkRequest {
            cfg: &c,
            node_id: "w",
            kind: "stdout",
            template: "default",
            attributes: Default::default(),
            run_id: "r",
            source_label: "rest".into(),
            budget: Some(&budget),
            budget_state: None,
            cancel: CancellationToken::new(),
            dry_run,
            preview,
        };
        let live = topology_sink_governance(req(false, false)).unwrap();
        for p in [
            Pass::Budget,
            Pass::Policy,
            Pass::Profiling,
            Pass::Metadata,
            Pass::Cancel,
        ] {
            assert!(live.governance.is_active(p), "{p}");
        }
        assert!(live.profiler.is_some());
        let dry = topology_sink_governance(req(true, true)).unwrap();
        assert!(dry.profiler.is_none());
        for p in [Pass::Budget, Pass::Policy, Pass::Profiling] {
            assert!(!dry.governance.is_active(p), "{p}");
            assert!(dry.governance.opted_out(p).is_some(), "{p}");
        }
        let quarantine = cfg(
            "policy:\n  classifications:\n    - { label: pii, fields: [email] }\n  rules:\n    - { name: r, when: { label: pii }, require: { residency: [eu] }, on_runtime: quarantine }\n",
        );
        let err = topology_sink_governance(TopologySinkRequest {
            cfg: &quarantine,
            ..req(false, false)
        })
        .err()
        .unwrap();
        assert!(err.to_string().contains("needs a `dlq:` block"));
    }

    /// The policy backstop of a topology sink node sees the node's masking, so
    /// a column masked for that node is not refused; a bad rule fails the build.
    #[cfg(all(feature = "policy", feature = "masking"))]
    #[test]
    fn a_topology_policy_backstop_carries_the_nodes_masking() {
        let policy = "policy:\n  classifications:\n    - { label: pii, fields: [email] }\n  rules:\n    - { name: r, when: { label: pii }, require: { residency: [eu] }, mask: [hash] }\n";
        let req = |c| TopologySinkRequest {
            cfg: c,
            node_id: "w",
            kind: "stdout",
            template: "default",
            attributes: Default::default(),
            run_id: "r",
            source_label: "rest".into(),
            budget: None,
            budget_state: None,
            cancel: CancellationToken::new(),
            dry_run: false,
            preview: false,
        };
        let masked = cfg(&format!(
            "  masking:\n    rules:\n      - {{ match: {{ fields: [email] }}, action: {{ type: hash }} }}\n{policy}"
        ));
        let built = topology_sink_governance(req(&masked)).unwrap();
        assert!(built.governance.is_active(Pass::Policy));
        let mut bad = masked.clone();
        bad.pipeline.masking = Some(
            serde_json::from_value(serde_json::json!({
                "rules": [{"match": {"field_pattern": "("}, "action": {"type": "hash"}}]
            }))
            .unwrap(),
        );
        let err = topology_sink_governance(req(&bad)).err().unwrap();
        assert!(err.to_string().contains("masking"), "{err}");
    }

    /// A DLQ-only masking rule that does not compile fails the topology DLQ.
    #[cfg(all(feature = "sink-jsonl", feature = "masking"))]
    #[tokio::test]
    async fn a_topology_dlq_with_a_bad_masking_rule_is_refused() {
        let mut c = cfg(DLQ);
        c.pipeline.masking = Some(
            serde_json::from_value(serde_json::json!({
                "rules": [{"match": {"field_pattern": "("}, "action": {"type": "hash"},
                           "applies_to": ["dlq"]}]
            }))
            .unwrap(),
        );
        let dlq = c.pipeline.dlq.clone().unwrap();
        let config = crate::executor::build_dlq_config(&dlq).await.unwrap();
        let Err(err) = topology_dlq(
            &Governance::new(),
            &c,
            &["w", "default", "stdout"],
            &dlq,
            config,
        ) else {
            panic!("a bad rule is refused");
        };
        assert!(err.to_string().contains("masking"), "{err}");
    }

    #[cfg(feature = "sink-jsonl")]
    #[tokio::test]
    async fn a_topology_dlq_is_governed_with_the_nodes_budget() {
        let c = cfg(DLQ);
        let dlq = c.pipeline.dlq.clone().unwrap();
        let g = Governance::new().with_budget(faucet_core::BudgetSpec {
            max_records: Some(1),
            ..Default::default()
        });
        let config = crate::executor::build_dlq_config(&dlq).await.unwrap();
        let governed = topology_dlq(&g, &c, &["w", "default", "stdout"], &dlq, config).unwrap();
        let err = governed
            .sink
            .write_batch(&[serde_json::json!({}), serde_json::json!({})])
            .await
            .unwrap_err();
        assert!(
            matches!(err, faucet_core::FaucetError::BudgetExceeded { .. }),
            "{err}"
        );
    }

    /// A preview writes nothing: only masking applies, the rest is opted out.
    #[test]
    #[cfg(feature = "masking")]
    fn a_preview_masks_and_opts_out_of_the_write_side() {
        let spec: faucet_core::MaskingSpec = serde_json::from_value(serde_json::json!({
            "rules": [{"match": {"fields": ["email"]}, "action": {"type": "redact"},
                       "applies_to": ["elsewhere"]}]
        }))
        .unwrap();
        let g = preview_governance(Some(&spec)).unwrap();
        assert!(
            g.is_active(Pass::Masking),
            "every rule applies to a preview"
        );
        for p in [
            Pass::Budget,
            Pass::Policy,
            Pass::Dlq,
            Pass::Profiling,
            Pass::Cancel,
        ] {
            assert!(g.opted_out(p).is_some(), "{p}");
        }
        assert!(!preview_governance(None).unwrap().is_active(Pass::Masking));
    }
}
