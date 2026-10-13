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
pub(crate) fn profiling_scope_opt_out(node: &ExpandedNode, opts: &ExecuteOptions) -> Option<&'static str> {
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
    let mut g = Governance::new().with_cancel(cancel.clone());
    if let Some(m) = meter {
        g = g.with_usage_meter(m);
    }
    if let Some(r) = opts.resilience.clone() {
        g = g.with_resilience(r);
    }
    if let Some(spec) = opts.budget.as_ref().filter(|b| !b.is_empty()) {
        g = g.with_budget(spec.clone());
    }
    #[cfg(feature = "quality")]
    if let Some(spec) = &node.quality {
        g = g.with_quality(Arc::new(
            faucet_core::CompiledQuality::compile(spec)
                .map_err(|e| CliError::Config(format!("quality: {e}")))?,
        ));
    }
    #[cfg(feature = "contract")]
    if let Some(spec) = &node.contract {
        g = g.with_contract(Arc::new(
            faucet_core::CompiledContract::compile(spec)
                .map_err(|e| CliError::Config(format!("contract: {e}")))?,
        ));
    }
    #[cfg(feature = "masking")]
    let masking = match &node.masking {
        Some(spec) => {
            let compiled = Arc::new(
                faucet_core::CompiledMasking::compile_for_sink(
                    spec,
                    &[node.sink_ref.as_str(), node.sink.kind.as_str()],
                )
                .map_err(|e| CliError::Config(format!("masking: {e}")))?,
            );
            g = g.with_masking(Arc::clone(&compiled));
            Some(compiled)
        }
        None => None,
    };
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
    if let Some(spec) = &node.policy {
        if crate::policy::quarantines(spec) && node.dlq.is_none() {
            return Err(CliError::Config(format!(
                "row '{}': policy: a rule with `on_runtime: quarantine` needs a `dlq:` block to \
                 route the quarantined records to",
                node.id
            )));
        }
        let compiled = faucet_core::CompiledPolicy::compile(&crate::policy::runtime_spec(spec, node))
            .map_err(|e| CliError::Config(format!("policy: {e}")))?;
        let mut p = faucet_core::PolicyGovernance::new(
            Arc::new(compiled),
            faucet_core::SinkFacts {
                id: node.sink_ref.clone(),
                kind: node.sink.kind.clone(),
                attributes: node.sink.attributes.clone(),
            },
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
        g = g.with_dlq(dlq_governance(node, dlq, config)?);
    }
    let mut profiler = None;
    if let Some(spec) = &node.profiling {
        match profiling_scope_opt_out(node, opts) {
            Some(reason) => g = g.without(Pass::Profiling, reason),
            None => {
                let p = Arc::new(Mutex::new(faucet_core::Profiler::new(spec.clone())));
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
        d = d.with_facts(crate::policy::dlq_facts(dlq));
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
        g = g.with_budget(spec.clone());
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
        let compiled = faucet_core::CompiledPolicy::compile(&crate::policy::topology_runtime_spec(
            policy_spec,
            cfg,
        ))
        .map_err(|e| CliError::Config(format!("policy: {e}")))?;
        #[allow(unused_mut)]
        let mut p = faucet_core::PolicyGovernance::new(
            Arc::new(compiled),
            faucet_core::SinkFacts {
                id: req.node_id.to_string(),
                kind: req.kind.to_string(),
                attributes: req.attributes.clone(),
            },
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
pub fn preview_governance(
    #[allow(unused_variables)] masking: Option<&faucet_core::MaskingSpec>,
) -> CliResult<Governance> {
    #[allow(unused_mut)]
    let mut g = Governance::new();
    #[cfg(feature = "masking")]
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
