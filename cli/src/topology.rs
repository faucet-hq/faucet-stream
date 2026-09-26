//! Topology mode (issues #71 / #72): build and run a
//! [`faucet_core::Topology`] from a config's `pipeline.nodes` / `edges` block.
//!
//! When `pipeline.nodes` is non-empty the pipeline runs as an explicit node
//! graph rather than a matrix. This module resolves each node's connector
//! templates, compiles its transforms, wires the edges, and drives the core
//! topology executor — reusing [`crate::registry`] for connector construction
//! and [`crate::state`] / [`crate::executor::build_dlq_config`] for the
//! sink-side plumbing.

use crate::auth_catalog::AuthCatalog;
use crate::config::{ConnectorSpec, NodeSpec, PipelineConfig};
use crate::error::{CliError, CliResult};
use crate::executor::{InvocationErrorKind, InvocationOutcome, RunSummary};
use crate::merge::merge_value;
use crate::registry::{build_sink, build_source};
use crate::transforms::compile_transforms;
use chrono::{DateTime, FixedOffset};
use faucet_core::stage::compile_stage;
use faucet_core::topology::{
    JoinConfig, JoinNode, NodeKind, Topology, TopologyGovernance, TopologyOnError, TopologyOptions,
};
use serde_json::Value;
use std::collections::HashMap;
use tokio_util::sync::CancellationToken;

/// Whether the config selects topology mode (a non-empty `pipeline.nodes`).
pub fn is_topology(cfg: &PipelineConfig) -> bool {
    !cfg.pipeline.nodes.is_empty()
}

/// Per-run knobs for topology mode, mirroring the matrix path's
/// [`crate::executor::ExecuteOptions`] subset that applies to a node graph.
#[derive(Default, Clone)]
pub struct TopologyRunOptions {
    /// External cancellation (serve run-cancel / timeout / shutdown, TUI quit).
    pub cancel: Option<CancellationToken>,
    /// Preview: build no real sinks, count records instead, and never persist a
    /// bookmark (#456 C2).
    pub dry_run: bool,
    /// Preview: stop after this many records per sink, and never persist a
    /// bookmark (#456 C2).
    pub limit: Option<usize>,
    /// Clock backing `${now.*}` in node configs. `None` = process start.
    pub clock: Option<DateTime<FixedOffset>>,
}

impl TopologyRunOptions {
    /// The effective `${now.*}` clock.
    fn clock(&self) -> DateTime<FixedOffset> {
        self.clock
            .unwrap_or_else(|| chrono::Utc::now().fixed_offset())
    }

    /// Whether this is a non-writing preview, which must not persist bookmarks.
    fn is_preview(&self) -> bool {
        self.dry_run || self.limit.is_some()
    }
}

/// Top-level blocks that topology mode parses but does **not** act on.
///
/// Empty — every top-level block is now applied to a node graph: the per-page
/// governance passes and `resilience:` per sink node (#456 C3), and `sla:` /
/// `notifications:` / `lineage:` / `catalog:` per sink node in
/// `post_run_observability` (#459).
///
/// The mechanism is kept deliberately. A declared-but-inert block is the worst
/// kind of silence — the operator believes a guarantee is in force when nothing
/// is enforcing it — so if a future block lands in matrix mode before topology
/// mode, list it here and `faucet validate` will say so out loud rather than
/// printing a clean bill of health.
pub fn inert_blocks(cfg: &PipelineConfig) -> Vec<(&'static str, &'static str)> {
    let _ = cfg;
    Vec::new()
}

/// Config-level graph validation: the checks that need only the `nodes:` /
/// `edges:` spec, no connectors. Run as a fail-fast prelude to
/// [`build_topology`] (so a wiring typo is reported before any client is
/// constructed) and standalone by the template registry, which validates a
/// config it must not build connectors for (#444).
///
/// Node **arity** (a tee's fan-out, a join's labelled inputs, …) is validated by
/// [`faucet_core::topology::Topology::validate`] once the graph is built —
/// deliberately not re-implemented here, so the two can never disagree.
pub fn validate_topology_spec(cfg: &PipelineConfig) -> CliResult<()> {
    if !cfg.matrix.is_empty() {
        return Err(CliError::MatrixAndNodesBothPresent);
    }
    // Exactly-once in a node graph (#458). Each sink node commits under its own
    // scope, so the requirements are the matrix ones applied per node — plus a
    // single-source restriction, because nothing records which source a given
    // sink's bookmark came from. Checked here, at config-load time, so an
    // unsupported combination never runs *as if* it were exactly-once (#456 H2).
    if cfg.delivery == faucet_core::DeliveryMode::ExactlyOnce {
        validate_exactly_once(cfg)?;
    }
    let spec = &cfg.pipeline;
    let mut known: Vec<String> = spec.nodes.keys().cloned().collect();
    known.sort_unstable();
    for edge in &spec.edges {
        for endpoint in [&edge.from, &edge.to] {
            if !spec.nodes.contains_key(endpoint) {
                return Err(CliError::EdgeEndpointMissing {
                    name: endpoint.clone(),
                    known: known.clone(),
                });
            }
        }
    }
    Ok(())
}

/// The connector kind a source/sink node resolves to, without building anything.
///
/// Mirrors [`resolve_connector`]'s kind precedence (inline `type` override, else
/// the referenced template, else the legacy singular block) so the gate below and
/// the builder can never disagree about what a node *is*.
fn resolved_node_kind(cfg: &PipelineConfig, node: &NodeSpec) -> Option<String> {
    let (template, kind, templates, legacy) = match node {
        NodeSpec::Source { template, kind, .. } => {
            (template, kind, &cfg.pipeline.sources, &cfg.pipeline.source)
        }
        NodeSpec::Sink { template, kind, .. } => {
            (template, kind, &cfg.pipeline.sinks, &cfg.pipeline.sink)
        }
        _ => return None,
    };
    if let Some(k) = kind {
        return Some(k.clone());
    }
    let name = template.as_deref().unwrap_or("default");
    templates
        .get(name)
        .or(if name == "default" {
            legacy.as_ref()
        } else {
            None
        })
        .map(|t| t.kind.clone())
}

/// The four atomic-watermark requirements, per node, plus the single-source rule.
///
/// Ordered so the message names the *limiting* side, and suggests the keyed-upsert
/// alternative when the sinks could do it — the same shape as the matrix gate in
/// `expand`, so an operator moving a pipeline between the two forms reads the same
/// diagnosis.
fn validate_exactly_once(cfg: &PipelineConfig) -> CliResult<()> {
    let nodes = &cfg.pipeline.nodes;
    let sources: Vec<(&String, String)> = nodes
        .iter()
        .filter(|(_, n)| matches!(n, NodeSpec::Source { .. }))
        .map(|(id, n)| (id, resolved_node_kind(cfg, n).unwrap_or_default()))
        .collect();
    let sinks: Vec<(&String, String)> = nodes
        .iter()
        .filter(|(_, n)| matches!(n, NodeSpec::Sink { .. }))
        .map(|(id, n)| (id, resolved_node_kind(cfg, n).unwrap_or_default()))
        .collect();

    // 1. One source. A sink's bookmark records the position of whichever source
    //    fed its pages, and the graph does not record which one — so with several
    //    sources there is no sound resume point to anchor the watermark against.
    if sources.len() != 1 {
        return Err(CliError::Config(format!(
            "`delivery: exactly_once` needs exactly one source node; this graph has {}. A \
             sink's commit watermark is only meaningful against a known source position, and \
             nothing records which source fed a given page. Split the graph into one pipeline \
             per source, or use `write_mode: upsert` + `key` on the sinks for keyed-upsert \
             effectively-once with any number of sources",
            sources.len()
        )));
    }
    // 2. The source must replay deterministically.
    let (src_id, src_kind) = &sources[0];
    if !crate::registry::source_supports_exactly_once(src_kind) {
        return Err(CliError::Config(format!(
            "node '{src_id}': `delivery: exactly_once` is not supported by source '{src_kind}' \
             (deterministic-replay sources only: {})",
            crate::registry::EXACTLY_ONCE_SOURCE_KINDS.join(", ")
        )));
    }
    // 3. Every sink must commit data + token atomically.
    for (id, kind) in &sinks {
        if !crate::registry::sink_supports_idempotent_writes(kind) {
            return Err(CliError::Config(format!(
                "node '{id}': `delivery: exactly_once` is not supported by sink '{kind}' \
                 (sinks that commit a watermark atomically: {}). Every sink node must qualify — \
                 each one keeps its own watermark",
                crate::registry::IDEMPOTENT_SINK_KINDS.join(", ")
            )));
        }
    }
    // 4. Durable state — the per-node sequence has to survive a restart.
    match cfg.pipeline.state.as_ref() {
        None => {
            return Err(CliError::Config(
                "`delivery: exactly_once` requires a durable `state:` block: each sink node \
                 persists its commit sequence there, and without it every restart would \
                 re-commit from zero"
                    .into(),
            ));
        }
        Some(state) if state.kind == "memory" => {
            return Err(CliError::Config(
                "`delivery: exactly_once` requires a durable `state:` block, and `memory` does \
                 not survive the process. Use `file`, `redis`, or `postgres`"
                    .into(),
            ));
        }
        Some(_) => {}
    }
    // 5. No DLQ — routing a row aside breaks the all-or-nothing page commit.
    if cfg.pipeline.dlq.is_some() {
        return Err(CliError::Config(
            "`delivery: exactly_once` is incompatible with a `dlq:` block in this version: a \
             page's rows and its commit token are written as one unit, so a partial page \
             cannot be split off to a dead-letter queue"
                .into(),
        ));
    }
    Ok(())
}

/// Resolve one source/sink node's template `ref` + inline overrides into a
/// concrete `(kind, config)` pair.
fn resolve_connector(
    templates: &HashMap<String, ConnectorSpec>,
    legacy: &Option<ConnectorSpec>,
    template_ref: Option<&str>,
    kind_override: Option<&str>,
    config_override: Option<&Value>,
    node_id: &str,
    kind_label: &'static str,
) -> CliResult<(String, Value)> {
    let name = template_ref.unwrap_or("default");
    let base: ConnectorSpec = if name == "default" {
        templates
            .get("default")
            .cloned()
            .or_else(|| legacy.clone())
            .ok_or(CliError::MissingTemplate {
                kind: kind_label,
                row_id: node_id.to_string(),
            })?
    } else {
        templates
            .get(name)
            .cloned()
            .ok_or_else(|| CliError::UnknownTemplate {
                kind: kind_label,
                name: name.to_string(),
                row_id: node_id.to_string(),
                known: {
                    let mut k: Vec<String> = templates.keys().cloned().collect();
                    if legacy.is_some() {
                        k.push("default".to_string());
                    }
                    k.sort();
                    k
                },
            })?
    };
    let mut kind = base.kind;
    let mut config = base.config;
    if let Some(k) = kind_override {
        kind = k.to_string();
    }
    if let Some(c) = config_override {
        merge_value(&mut config, c.clone());
    }
    Ok((kind, config))
}

/// Build a [`faucet_core::Topology`] from the config's `pipeline.nodes` /
/// `edges` block, with default run options (no preview, process-start clock).
pub async fn build_topology(cfg: &PipelineConfig, auth: &AuthCatalog) -> CliResult<Topology> {
    build_topology_with(cfg, auth, &TopologyRunOptions::default()).await
}

/// Identity of one source/sink node, captured while the graph is built.
///
/// `dataset_uri` is a method on the *built* connector, and the graph is consumed
/// by the run — so lineage and catalog need this recorded up front (#459).
#[derive(Debug, Clone)]
pub struct NodeIdentity {
    /// Connector kind (`"csv"`, `"postgres"`, …).
    pub kind: String,
    /// Raw dataset URI, before canonicalization.
    pub dataset_uri: String,
    /// The node's resolved connector config, for `${now.*}` folding.
    pub config: Value,
}

/// Record one connector node's identity, when a caller asked for the map.
///
/// Skipped when the caller wants no map, or when the connector does not override
/// `dataset_uri()` — the `<kind>://unknown` default names nothing joinable, and
/// recording it would put a placeholder dataset in the catalog and in every
/// OpenLineage event.
fn record_identity(
    identities: &mut Option<&mut NodeIdentities>,
    node_id: &str,
    kind: &str,
    config: Value,
    dataset_uri: String,
) {
    let Some(map) = identities.as_mut() else {
        return;
    };
    if dataset_uri.ends_with("://unknown") {
        tracing::debug!(
            node = %node_id,
            kind = %kind,
            "topology: connector does not expose a dataset_uri; omitted from lineage/catalog"
        );
        return;
    }
    let uri = dataset_uri;
    map.insert(
        node_id.to_string(),
        NodeIdentity {
            kind: kind.to_string(),
            dataset_uri: uri,
            config,
        },
    );
}

/// Per-node identities, keyed by node id. Only source and sink nodes appear.
pub type NodeIdentities = std::collections::HashMap<String, NodeIdentity>;

/// Per-sink-node column profilers (#708), keyed by node id — present for every
/// real (non-preview) sink node when the config has a `profiling:` block.
pub type TopologyProfilers =
    std::collections::HashMap<String, std::sync::Arc<std::sync::Mutex<faucet_core::Profiler>>>;

/// [`build_topology_with`] that also reports each source/sink node's identity.
pub async fn build_topology_meta(
    cfg: &PipelineConfig,
    auth: &AuthCatalog,
    opts: &TopologyRunOptions,
) -> CliResult<(Topology, NodeIdentities)> {
    let mut ids = NodeIdentities::new();
    let topo = build_topology_inner(cfg, auth, opts, Some(&mut ids), None).await?;
    Ok((topo, ids))
}

/// [`build_topology_meta`] that also hands back each sink node's column
/// profiler (#708), so the post-run pass can evaluate drift per sink node.
pub async fn build_topology_full(
    cfg: &PipelineConfig,
    auth: &AuthCatalog,
    opts: &TopologyRunOptions,
) -> CliResult<(Topology, NodeIdentities, TopologyProfilers)> {
    let mut ids = NodeIdentities::new();
    let mut profilers = TopologyProfilers::new();
    let topo = build_topology_inner(cfg, auth, opts, Some(&mut ids), Some(&mut profilers)).await?;
    Ok((topo, ids, profilers))
}

/// Build a [`faucet_core::Topology`], honouring the run options.
///
/// Two things happen here that the matrix path does per invocation in
/// [`crate::executor`], and that topology mode used to skip entirely:
///
/// - **`${now.*}` is resolved** in every node's source/sink config, and a
///   leftover `${backfill.*}` token is rejected. Without this the literal token
///   string reached the connector, so a dated path became a directory named
///   `${now.date}` (#456 H4).
/// - **Preview modes wrap the sinks**: `--dry-run` swaps in a counting sink and
///   `--limit` truncates, so neither performs a real write (#456 C2).
pub async fn build_topology_with(
    cfg: &PipelineConfig,
    auth: &AuthCatalog,
    opts: &TopologyRunOptions,
) -> CliResult<Topology> {
    build_topology_inner(cfg, auth, opts, None, None).await
}

async fn build_topology_inner(
    cfg: &PipelineConfig,
    auth: &AuthCatalog,
    opts: &TopologyRunOptions,
    mut identities: Option<&mut NodeIdentities>,
    mut profilers: Option<&mut TopologyProfilers>,
) -> CliResult<Topology> {
    // Cheap graph checks first, so a wiring typo never costs a connector build.
    validate_topology_spec(cfg)?;

    let clock = opts.clock();
    let spec = &cfg.pipeline;
    let mut builder = Topology::builder();

    // Deterministic node order (sorted by id) so errors/logs are stable.
    let mut node_ids: Vec<&String> = spec.nodes.keys().collect();
    node_ids.sort();

    for id in &node_ids {
        let node = &spec.nodes[*id];
        let kind: NodeKind = match node {
            NodeSpec::Source {
                template,
                kind,
                config,
            } => {
                let (k, mut c) = resolve_connector(
                    &spec.sources,
                    &spec.source,
                    template.as_deref(),
                    kind.as_deref(),
                    config.as_ref(),
                    id,
                    "source",
                )?;
                crate::executor::resolve_now_inplace(&mut c, clock)?;
                crate::executor::reject_unresolved_backfill_tokens(&c, "source")?;
                let source = build_source(&k, c.clone(), auth, None).await?;
                record_identity(&mut identities, id, &k, c, source.dataset_uri());
                NodeKind::Source(source)
            }
            NodeSpec::Sink {
                template,
                kind,
                config,
            } => {
                let (k, mut c) = resolve_connector(
                    &spec.sinks,
                    &spec.sink,
                    template.as_deref(),
                    kind.as_deref(),
                    config.as_ref(),
                    id,
                    "sink",
                )?;
                crate::executor::resolve_now_inplace(&mut c, clock)?;
                crate::executor::reject_unresolved_backfill_tokens(&c, "sink")?;
                // Preview modes must never reach the real destination. The
                // identity is still recorded from the *real* config, so a
                // `--dry-run` report names the destination it would have
                // written rather than the counting stand-in.
                let sink: Box<dyn faucet_core::Sink> = if opts.dry_run {
                    if let Ok(probe) = build_sink(&k, c.clone(), auth).await {
                        record_identity(&mut identities, id, &k, c.clone(), probe.dataset_uri());
                    }
                    Box::new(crate::executor::CountingSink::new())
                } else {
                    let sink = build_sink(&k, c.clone(), auth).await?;
                    record_identity(&mut identities, id, &k, c, sink.dataset_uri());
                    sink
                };
                let sink = match opts.limit {
                    Some(n) => Box::new(crate::executor::LimitedSink::wrap(sink, n)) as Box<_>,
                    None => sink,
                };
                // Column profiling (#708): a real run's sink node profiles what
                // it writes, exactly as a matrix invocation does. Previews are
                // not profiled (their volumes are synthetic).
                let sink = match (cfg.profiling.as_ref(), profilers.as_mut()) {
                    (Some(spec), Some(map)) if !opts.is_preview() => {
                        let profiler = std::sync::Arc::new(std::sync::Mutex::new(
                            faucet_core::Profiler::new(spec.clone()),
                        ));
                        map.insert((*id).clone(), std::sync::Arc::clone(&profiler));
                        Box::new(faucet_core::ProfilingSink::new(sink, profiler)) as Box<_>
                    }
                    _ => sink,
                };
                // Data-flow policy runtime backstop (#702): classify the real
                // records with the same rules the static pass used. Attributes
                // come from the node's resolved sink template.
                #[cfg(feature = "policy")]
                let sink = match cfg.policy.as_ref() {
                    Some(policy_spec) if !opts.dry_run => {
                        if crate::policy::quarantines(policy_spec) && spec.dlq.is_none() {
                            return Err(CliError::Config(format!(
                                "node '{id}': policy: a rule with `on_runtime: quarantine` needs a \
                                 `dlq:` block to route the quarantined records to"
                            )));
                        }
                        let compiled = faucet_core::CompiledPolicy::compile(policy_spec)
                            .map_err(|e| CliError::Config(format!("policy: {e}")))?;
                        let template_name = template.as_deref().unwrap_or("default");
                        let attributes = if template_name == "default" {
                            spec.sinks.get("default").or(spec.sink.as_ref())
                        } else {
                            spec.sinks.get(template_name)
                        }
                        .map(|b| b.attributes.clone())
                        .unwrap_or_default();
                        Box::new(faucet_core::PolicySink::new(
                            sink,
                            std::sync::Arc::new(compiled),
                            faucet_core::SinkFacts {
                                id: (*id).clone(),
                                kind: k.clone(),
                                attributes,
                            },
                            faucet_core::PolicyScope {
                                pipeline: cfg.name.clone().unwrap_or_default(),
                                row: (*id).clone(),
                            },
                        )) as Box<_>
                    }
                    _ => sink,
                };
                NodeKind::Sink(sink)
            }
            NodeSpec::Transform { transforms } => {
                let stages = compile_transforms(transforms)?;
                let compiled = stages
                    .iter()
                    .map(compile_stage)
                    .collect::<Result<Vec<_>, _>>()?;
                NodeKind::Transform(compiled)
            }
            NodeSpec::Tee {
                channel_capacity,
                fanout,
            } => NodeKind::Tee {
                capacity: *channel_capacity,
                fanout: *fanout,
            },
            NodeSpec::Merge => NodeKind::Merge,
            NodeSpec::Join(js) => NodeKind::Join(JoinNode {
                config: JoinConfig {
                    mode: js.mode,
                    build_key: js.build.key.clone(),
                    probe_key: js.probe.key.clone(),
                    projections: js.project.clone(),
                    on_missing: js.on_missing.clone(),
                    on_duplicate: js.on_duplicate,
                    on_collision: js.on_collision,
                    key_normalize: js.key_normalize,
                    max_build_records: js.max_build_records,
                },
                build_edge: js.build.edge.clone(),
                probe_edge: js.probe.edge.clone(),
            }),
        };
        builder = builder.node((*id).clone(), kind);
    }

    // Edge endpoints were already validated by `validate_topology_spec` above —
    // before any connector was constructed — so just wire them.
    for e in &spec.edges {
        builder = match &e.label {
            Some(label) => builder.labelled_edge(e.from.clone(), e.to.clone(), label.clone()),
            None => builder.edge(e.from.clone(), e.to.clone()),
        };
    }

    builder.build().map_err(|e| CliError::InvalidTopology {
        message: e.to_string(),
    })
}

/// Compile the config's governance blocks for a node graph.
///
/// Mirrors the matrix path in [`crate::executor`] so a topology enforces the same
/// policies — before this existed, a config declaring `masking:` ran with no
/// masking at all and PII reached every destination in the clear (#456 C3).
///
/// Masking is destination-scoped, so it is compiled **per sink node** against
/// that node's identifiers (node id, template ref, connector kind) — any of which
/// an `applies_to` rule may name. A sink for which no rule applies gets no entry,
/// so the pass is skipped entirely for it.
fn build_governance(cfg: &PipelineConfig) -> CliResult<TopologyGovernance> {
    #[allow(unused_mut)]
    let mut g = TopologyGovernance::new();

    #[cfg(feature = "quality")]
    if let Some(spec) = &cfg.pipeline.quality {
        g.quality = Some(std::sync::Arc::new(
            faucet_core::CompiledQuality::compile(spec)
                .map_err(|e| CliError::Config(format!("quality: {e}")))?,
        ));
    }
    #[cfg(feature = "contract")]
    if let Some(spec) = &cfg.pipeline.contract {
        g.contract = Some(std::sync::Arc::new(
            faucet_core::CompiledContract::compile(spec)
                .map_err(|e| CliError::Config(format!("contract: {e}")))?,
        ));
    }
    if let Some(spec) = &cfg.pipeline.schema {
        g.schema_drift = Some(faucet_core::SchemaDriftPolicy::compile(spec));
    }
    if let Some(spec) = &cfg.resilience {
        g.resilience = Some(spec.to_policy()?);
    }
    // Delivery guarantee (#458). `validate_topology_spec` has already checked the
    // per-node requirements, so by here `exactly_once` is known to be supportable.
    g.delivery = cfg.delivery;

    #[cfg(feature = "masking")]
    if let Some(spec) = &cfg.pipeline.masking {
        for (node_id, node) in &cfg.pipeline.nodes {
            let NodeSpec::Sink { template, kind, .. } = node else {
                continue;
            };
            let template_ref = template.as_deref().unwrap_or("default");
            // The node's own kind override, else the template's declared kind.
            let resolved_kind = kind.clone().or_else(|| {
                cfg.pipeline
                    .sinks
                    .get(template_ref)
                    .or(cfg.pipeline.sink.as_ref())
                    .map(|t| t.kind.clone())
            });
            let mut ids: Vec<&str> = vec![node_id.as_str(), template_ref];
            if let Some(k) = resolved_kind.as_deref() {
                ids.push(k);
            }
            let compiled = faucet_core::CompiledMasking::compile_for_sink(spec, &ids)
                .map_err(|e| CliError::Config(format!("masking: {e}")))?;
            if !compiled.is_empty() {
                g.masking_by_sink
                    .insert(node_id.clone(), std::sync::Arc::new(compiled));
            }
        }
    }
    Ok(g)
}

/// Collect a bounded preview of each `source` node's records (source side
/// only; downstream nodes are not run). Returns `(node_id, records)` per
/// source node, in sorted node-id order.
pub async fn preview_records(
    cfg: &PipelineConfig,
    auth: &AuthCatalog,
    limit: usize,
) -> CliResult<Vec<(String, Vec<Value>)>> {
    if !cfg.matrix.is_empty() {
        return Err(CliError::MatrixAndNodesBothPresent);
    }
    let spec = &cfg.pipeline;
    let mut ids: Vec<&String> = spec.nodes.keys().collect();
    ids.sort();

    let mut out = Vec::new();
    for id in ids {
        if let NodeSpec::Source {
            template,
            kind,
            config,
        } = &spec.nodes[id]
        {
            let (k, c) = resolve_connector(
                &spec.sources,
                &spec.source,
                template.as_deref(),
                kind.as_deref(),
                config.as_ref(),
                id,
                "source",
            )?;
            let source = build_source(&k, c, auth, None).await?;
            let records = source.fetch_all().await?;
            out.push((
                id.clone(),
                records.into_iter().take(limit).collect::<Vec<_>>(),
            ));
        }
    }
    if out.is_empty() {
        return Err(CliError::InvalidTopology {
            message: "no source nodes to preview".to_string(),
        });
    }
    Ok(out)
}

/// Preview topology mode: build each `source` node and print the first
/// `limit` records per source to stdout as JSON Lines.
pub async fn preview(cfg: &PipelineConfig, auth: &AuthCatalog, limit: usize) -> CliResult<()> {
    for (id, records) in preview_records(cfg, auth, limit).await? {
        tracing::info!(node = %id, "previewing source node");
        for rec in records {
            println!("{}", serde_json::to_string(&rec).unwrap_or_default());
        }
    }
    Ok(())
}

/// Preview topology sources into a JSON string (for the MCP `preview` tool).
pub async fn preview_to_string(
    cfg: &PipelineConfig,
    auth: &AuthCatalog,
    limit: usize,
) -> CliResult<String> {
    let sources = preview_records(cfg, auth, limit).await?;
    let doc: Vec<Value> = sources
        .into_iter()
        .map(|(id, records)| {
            serde_json::json!({ "node": id, "count": records.len(), "records": records })
        })
        .collect();
    Ok(
        serde_json::to_string_pretty(&serde_json::json!({ "sources": doc }))
            .unwrap_or_else(|_| "[]".to_string()),
    )
}

/// Build and run the topology, returning a [`RunSummary`] shaped like a matrix
/// run (one invocation per sink node, plus one per node failure under
/// `on_error: continue`).
pub async fn run_topology(
    cfg: &PipelineConfig,
    auth: &AuthCatalog,
    run: TopologyRunOptions,
) -> CliResult<RunSummary> {
    for (block, consequence) in inert_blocks(cfg) {
        tracing::warn!(
            block,
            "`{block}:` is not applied in topology mode (`pipeline.nodes`) — {consequence}"
        );
    }

    let (topo, identities, profilers) = build_topology_full(cfg, auth, &run).await?;

    let pipeline_name = cfg.name.clone().unwrap_or_else(|| "unnamed".to_string());
    let run_id = uuid::Uuid::now_v7().to_string();

    let on_error = match cfg.execution.as_ref().map(|e| e.on_error) {
        Some(crate::config::OnError::Stop) => TopologyOnError::Propagate,
        _ => TopologyOnError::Continue,
    };

    let mut opts = TopologyOptions::new(pipeline_name.clone()).with_on_error(on_error);
    opts.run_id = run_id.clone();

    if let Some(state) = &cfg.pipeline.state {
        let store = crate::state::build_state_store(state).await?;
        // A preview must not advance a durable bookmark: the counting/truncating
        // sinks return `Ok` without a real write, so a persisted bookmark would
        // make the next real run resume past records nobody wrote (#456 C2,
        // mirroring #321 H1 on the matrix path). Reads still pass through.
        let store = if run.is_preview() {
            std::sync::Arc::new(crate::executor::ReadOnlyStateStore { inner: store })
                as std::sync::Arc<dyn faucet_core::StateStore>
        } else {
            store
        };
        opts = opts.with_state_store(store);
    }
    if let Some(dlq) = &cfg.pipeline.dlq {
        opts = opts.with_dlq(crate::executor::build_dlq_config(dlq).await?);
    }
    if let Some(c) = run.cancel.clone() {
        opts = opts.with_cancel(c);
    }

    // Lineage START, one per sink node — a topology's analogue of an invocation.
    // Built before the run so a crash still leaves a START on record.
    #[cfg(feature = "lineage")]
    let lineage = crate::lineage_glue::build_emitter(cfg.lineage.as_ref())
        .map_err(|e| CliError::Config(format!("lineage: {e}")))?;
    #[cfg(feature = "lineage")]
    let reaching_for_lineage = reaching_sources(cfg);
    #[cfg(feature = "lineage")]
    if let (Some(em), Some(lc)) = (lineage.as_ref(), cfg.lineage.as_ref()) {
        for (node_id, _) in cfg
            .pipeline
            .nodes
            .iter()
            .filter(|(_, n)| matches!(n, NodeSpec::Sink { .. }))
        {
            if let Some(ctx) = lineage_ctx(
                cfg,
                &pipeline_name,
                &run_id,
                node_id,
                &identities,
                &reaching_for_lineage,
                lc,
                0,
                None,
            ) {
                em.emit(faucet_lineage::EventType::Start, &ctx).await;
            }
        }
    }

    // `run_reported` rather than `run_with`: the post-run pass below emits one
    // notification and evaluates one SLA per **sink node**, which needs to know
    // which node failed (#459).
    let state_store = opts.state_store.clone();
    let cancelled = run.cancel.as_ref().is_some_and(|c| c.is_cancelled());
    let reported = topo.run_reported(opts, build_governance(cfg)?).await?;

    // Per-sink-node observability. A sink node is a topology's analogue of a
    // matrix invocation — it owns a state key, a bookmark, and a record count —
    // so the SLA and notification passes key off it, reusing the same standalone
    // functions the executor calls rather than a parallel implementation.
    let profile_failures = if !run.is_preview() && !cancelled {
        post_run_observability(
            cfg,
            PostRun {
                pipeline_name: &pipeline_name,
                reported: &reported,
                state_store: state_store.as_ref(),
                identities: &identities,
                profilers: &profilers,
                reaching: &reaching_sources(cfg),
                clock: run.clock(),
                run_id: &run_id,
                #[cfg(feature = "lineage")]
                lineage: lineage.as_ref(),
            },
        )
        .await
    } else {
        Vec::new()
    };

    let mut failures: Vec<(&str, &str, Option<faucet_core::topology::NodeErrorKind>)> = reported
        .nodes
        .iter()
        .filter_map(|n| {
            n.error
                .as_deref()
                .map(|e| (n.node_id.as_str(), e, n.error_kind))
        })
        .collect();
    // A `profiling.on_drift: fail` finding marks its sink node failed (#708),
    // like the matrix executor does for an invocation.
    for (node_id, message) in &profile_failures {
        failures.push((node_id.as_str(), message.as_str(), None));
    }
    Ok(build_summary(&reported.result.per_sink, &failures))
}

/// Flatten a topology run's per-node attribution into the same
/// [`RunSummary`] shape a matrix run produces, so every caller downstream of
/// `run`/`schedule`/`serve` sees one summary type.
///
/// Takes the two pieces it needs rather than the whole `TopologyRun`, which
/// keeps it pure *and* constructible from a test — `NodeReport` is
/// `#[non_exhaustive]`, so no other crate can build one.
///
/// One row per **sink** node carrying its record count, sorted by node id so
/// the output is deterministic, then one row per **failed** node — attributed
/// to the node that produced the failure rather than a single flat `topology`
/// row (#459). Pure, so the mapping is unit-testable without standing up a
/// graph.
///
/// Each failed node carries its typed [`NodeErrorKind`](faucet_core::topology::NodeErrorKind)
/// through as an [`InvocationErrorKind`], so a topology node that trips the
/// resilience circuit breaker gets the same scheduler cooldown a matrix row
/// does (#658). Before this, the kind was dropped at the reporting boundary and
/// a cron tick re-fired immediately at a destination that had just tripped —
/// the exact behaviour the breaker exists to prevent.
fn build_summary(
    per_sink: &HashMap<String, usize>,
    failures: &[(&str, &str, Option<faucet_core::topology::NodeErrorKind>)],
) -> RunSummary {
    let mut invocations: Vec<InvocationOutcome> = per_sink
        .iter()
        .map(|(node_id, records)| InvocationOutcome {
            row_id: node_id.clone(),
            parent_record_key: None,
            run_id: None,
            records_written: *records,
            error: None,
            error_kind: None,
            metrics: None,
        })
        .collect();
    invocations.sort_by(|a, b| a.row_id.cmp(&b.row_id));

    for (node_id, error, kind) in failures {
        // Classification comes from the typed kind only — never from the text.
        // `NodeErrorKind` is non-exhaustive: an unmapped future class is still
        // a failure, just not one with its own handling; a post-run verdict
        // (profile drift, #708) carries no node kind and stays unclassified.
        let error_kind = kind.map(|k| match k {
            faucet_core::topology::NodeErrorKind::CircuitOpen => InvocationErrorKind::CircuitOpen,
            _ => InvocationErrorKind::Other,
        });
        // A node that wrote its records and was then failed by a post-run pass
        // is one outcome, not a success row plus a failure row.
        if let Some(existing) = invocations.iter_mut().find(|i| i.row_id == *node_id) {
            existing.error = Some((*error).to_string());
            existing.error_kind = error_kind;
            continue;
        }
        invocations.push(InvocationOutcome {
            row_id: (*node_id).to_string(),
            parent_record_key: None,
            run_id: None,
            records_written: 0,
            error: Some((*error).to_string()),
            error_kind,
            metrics: None,
        });
    }

    RunSummary { invocations }
}

/// Build the lineage context for one sink node: every source that reaches it as
/// an input, the sink as the output.
///
/// `None` when the node has no identity or no reaching source — there is nothing
/// truthful to emit in that case, and OpenLineage would rather have no event than
/// one naming a dataset that does not exist.
#[cfg(feature = "lineage")]
#[allow(clippy::too_many_arguments)]
fn lineage_ctx(
    _cfg: &PipelineConfig,
    pipeline_name: &str,
    run_id: &str,
    node_id: &str,
    identities: &NodeIdentities,
    reaching: &std::collections::HashMap<String, Vec<String>>,
    lc: &faucet_lineage::LineageConfig,
    records: u64,
    error: Option<String>,
) -> Option<faucet_lineage::RunLifecycle> {
    let sink = identities.get(node_id)?;
    let inputs: Vec<faucet_lineage::DatasetRef> = reaching
        .get(node_id)?
        .iter()
        .filter_map(|src| {
            let ident = identities.get(src)?;
            Some(faucet_lineage::DatasetRef {
                namespace: lc.namespace.clone(),
                name: ident.dataset_uri.clone(),
            })
        })
        .collect();
    if inputs.is_empty() {
        return None;
    }
    Some(faucet_lineage::RunLifecycle {
        job_namespace: lc.namespace.clone(),
        // One OpenLineage job per sink node, so a graph shows up as several
        // related jobs rather than one opaque run.
        job_name: format!("{pipeline_name}.{node_id}"),
        run_id: run_id.to_string(),
        parent: lc.parent_job.clone(),
        inputs,
        output: faucet_lineage::DatasetRef {
            namespace: lc.namespace.clone(),
            name: sink.dataset_uri.clone(),
        },
        started_at: chrono::Utc::now(),
        finished_at: None,
        records,
        error,
        input_schemas: Vec::new(),
        output_schema: None,
        // Derived from a single transform chain, which a graph's per-node chains
        // are not — omitted rather than fabricated.
        column_lineage: None,
        source_code: None,
    })
}

/// For each **sink** node, the source nodes that reach it, in deterministic order.
///
/// A linear or tee graph gives one source per sink; a merge or join gives several,
/// which is why lineage and catalog model a list of inputs (#459). Pure reverse
/// reachability over the declared edges — no connectors involved.
pub fn reaching_sources(cfg: &PipelineConfig) -> std::collections::HashMap<String, Vec<String>> {
    use std::collections::{HashMap, HashSet};
    let mut rev: HashMap<&str, Vec<&str>> = HashMap::new();
    for e in &cfg.pipeline.edges {
        rev.entry(e.to.as_str()).or_default().push(e.from.as_str());
    }
    let is_source = |id: &str| matches!(cfg.pipeline.nodes.get(id), Some(NodeSpec::Source { .. }));

    let mut out = HashMap::new();
    for (id, node) in &cfg.pipeline.nodes {
        if !matches!(node, NodeSpec::Sink { .. }) {
            continue;
        }
        // Walk upstream; a DAG so a seen-set is enough to terminate.
        let mut found: Vec<String> = Vec::new();
        let mut seen: HashSet<&str> = HashSet::new();
        let mut stack: Vec<&str> = rev.get(id.as_str()).cloned().unwrap_or_default();
        while let Some(n) = stack.pop() {
            if !seen.insert(n) {
                continue;
            }
            if is_source(n) {
                found.push(n.to_string());
            }
            if let Some(ups) = rev.get(n) {
                stack.extend(ups.iter().copied());
            }
        }
        found.sort();
        out.insert(id.clone(), found);
    }
    out
}

/// Freshness/volume SLAs, notifications, lineage, and catalog — once per sink node.
///
/// Deliberately a thin adapter: `sla::evaluate_post_run` and the `NotifyEvent`
/// constructors are already standalone, so topology mode calls exactly what the
/// matrix executor calls. Neither can fail a run — an SLA violation is a signal
/// and a notification is best-effort — so this returns nothing.
struct PostRun<'a> {
    pipeline_name: &'a str,
    reported: &'a faucet_core::topology::TopologyRun,
    state_store: Option<&'a std::sync::Arc<dyn faucet_core::StateStore>>,
    identities: &'a NodeIdentities,
    profilers: &'a TopologyProfilers,
    reaching: &'a std::collections::HashMap<String, Vec<String>>,
    clock: DateTime<FixedOffset>,
    run_id: &'a str,
    #[cfg(feature = "lineage")]
    lineage: Option<&'a std::sync::Arc<faucet_lineage::LineageEmitter>>,
}

/// Returns the `(sink node id, message)` of every node a
/// `profiling.on_drift: fail` finding turned into a failure (#708).
async fn post_run_observability(cfg: &PipelineConfig, ctx: PostRun<'_>) -> Vec<(String, String)> {
    let PostRun {
        pipeline_name,
        reported,
        state_store,
        identities,
        profilers,
        reaching,
        clock,
        run_id,
        #[cfg(feature = "lineage")]
        lineage,
    } = ctx;
    #[cfg(feature = "catalog")]
    let catalog = match cfg.catalog.as_ref() {
        Some(spec) => match crate::catalog::connect_from_spec(spec).await {
            Ok(h) => Some(h),
            Err(e) => {
                // Recording is best-effort; a bad catalog URL must not retro-fail
                // a run whose data is already written.
                tracing::error!(error = %e, "catalog connect failed; not recording");
                None
            }
        },
        None => None,
    };
    #[cfg(feature = "notify")]
    let notifier = match crate::notify::Notifier::from_specs(&cfg.notifications) {
        Ok(n) => n,
        Err(e) => {
            // A malformed block is a config error, but it must not fail a run that
            // has already written its data.
            tracing::error!(error = %e, "notifications config invalid; not notifying");
            None
        }
    };
    let now = chrono::Utc::now().timestamp();
    let mut profile_failures = Vec::new();

    for node in reported.nodes.iter().filter(|n| n.kind == "sink") {
        let row = node.node_id.as_str();

        // ── SLA (#202) ───────────────────────────────────────────────────────
        let violations = match cfg.sla.as_ref() {
            Some(spec) => {
                let base_key = format!("{pipeline_name}::{row}");
                let outcome = match &node.error {
                    None => crate::sla::RunOutcome::Success {
                        rows: node.records as u64,
                    },
                    Some(_) => crate::sla::RunOutcome::Failure,
                };
                let v = crate::sla::evaluate_post_run(
                    spec,
                    state_store,
                    &base_key,
                    pipeline_name,
                    row,
                    outcome,
                    now,
                )
                .await;
                for violation in &v {
                    tracing::warn!(node = %row, kind = violation.kind(), "SLA violation: {violation}");
                }
                v
            }
            None => Vec::new(),
        };

        // ── Column profiling (#708) ──────────────────────────────────────────
        // Same pass as the matrix executor's: finish the node's profile, compare
        // it with the baseline under `{pipeline}::{node}`, and apply `on_drift`.
        let profile_outcome = match (cfg.profiling.as_ref(), profilers.get(row), &node.error) {
            (Some(spec), Some(p), None) => {
                let profile = p.lock().map(|g| g.finish()).unwrap_or_default();
                let outcome = crate::profiling::evaluate_post_run(
                    spec,
                    state_store,
                    &format!("{pipeline_name}::{row}"),
                    pipeline_name,
                    row,
                    run_id,
                    profile,
                    chrono::Utc::now(),
                )
                .await;
                if outcome.fails_run(spec) {
                    profile_failures.push((row.to_string(), outcome.error().to_string()));
                }
                Some(outcome)
            }
            _ => None,
        };
        let node_failed_by_profile = profile_failures.iter().any(|(id, _)| id == row);

        // ── Notifications (#280) ─────────────────────────────────────────────
        #[cfg(feature = "notify")]
        if let Some(notifier) = &notifier {
            use crate::notify::NotifyEvent;
            match (&node.error, node_failed_by_profile) {
                (None, false) => {
                    notifier
                        .emit(NotifyEvent::run_success(
                            pipeline_name,
                            row,
                            node.records as u64,
                        ))
                        .await;
                }
                (None, true) => {
                    let msg = profile_failures
                        .iter()
                        .find(|(id, _)| id == row)
                        .map(|(_, m)| m.clone())
                        .unwrap_or_default();
                    notifier
                        .emit(NotifyEvent::run_failure(
                            pipeline_name,
                            row,
                            "profile_drift",
                            msg,
                        ))
                        .await;
                }
                (Some(msg), _) => {
                    notifier
                        .emit(NotifyEvent::run_failure(pipeline_name, row, "sink", msg))
                        .await;
                }
            }
            for v in &violations {
                notifier
                    .emit(NotifyEvent::sla_breach(
                        pipeline_name,
                        row,
                        v.kind(),
                        v.to_string(),
                    ))
                    .await;
            }
            if let (Some(o), Some(spec)) = (&profile_outcome, cfg.profiling.as_ref())
                && spec.on_drift != faucet_core::OnProfileDrift::Warn
            {
                for d in &o.drift {
                    notifier
                        .emit(NotifyEvent::profile_drift(
                            pipeline_name,
                            row,
                            &d.column,
                            d.metric.as_str(),
                            d.to_string(),
                        ))
                        .await;
                }
            }
        }
        #[cfg(not(feature = "notify"))]
        let _ = (&violations, node_failed_by_profile);
        #[cfg(not(feature = "catalog"))]
        let _ = &profile_outcome;

        // ── OpenLineage terminal event (#459) ────────────────────────────────
        #[cfg(feature = "lineage")]
        if let (Some(em), Some(lc)) = (lineage, cfg.lineage.as_ref())
            && let Some(ctx) = lineage_ctx(
                cfg,
                pipeline_name,
                run_id,
                row,
                identities,
                reaching,
                lc,
                node.records as u64,
                node.error.clone(),
            )
        {
            let ev = match node.error {
                None => faucet_lineage::EventType::Complete,
                Some(_) => faucet_lineage::EventType::Fail,
            };
            em.emit(ev, &ctx).await;
        }

        // ── Data Movement Catalog (#279 / #459) ──────────────────────────────
        // One record per sink node: every source that reaches it as an input,
        // plus the sink itself. A successful node only — a failed one's partial
        // volume is not a signal, matching the matrix path.
        #[cfg(feature = "catalog")]
        if node.error.is_none()
            && let Some(handle) = catalog.as_ref()
            && let Some(sink_id) = identities.get(row)
        {
            use crate::catalog::model::canonicalize_uri;
            use crate::serve::history::catalog::{CatalogUpdate, DatasetObservation, DatasetRole};

            let sources: Vec<DatasetObservation> = reaching
                .get(row)
                .map(Vec::as_slice)
                .unwrap_or_default()
                .iter()
                .filter_map(|src| {
                    let ident = identities.get(src)?;
                    Some(DatasetObservation {
                        uri: canonicalize_uri(&ident.dataset_uri, &ident.config, clock),
                        kind: ident.kind.clone(),
                        role: DatasetRole::Source,
                        // Each input reports what *it* read, so a merge's edge
                        // volumes sum to the sink instead of repeating its total.
                        records: reported
                            .nodes
                            .iter()
                            .find(|n| &n.node_id == src)
                            .map(|n| n.records as u64)
                            .unwrap_or(0),
                        schema: None,
                    })
                })
                .collect();

            if sources.is_empty() {
                tracing::debug!(node = %row, "no source reaches this sink; nothing to catalog");
            } else {
                let update = CatalogUpdate {
                    run_id: run_id.to_string(),
                    pipeline: pipeline_name.to_string(),
                    row: row.to_string(),
                    recorded_at: chrono::Utc::now(),
                    sources,
                    sink: DatasetObservation {
                        uri: canonicalize_uri(&sink_id.dataset_uri, &sink_id.config, clock),
                        kind: sink_id.kind.clone(),
                        role: DatasetRole::Sink,
                        schema: None,
                        records: node.records as u64,
                    },
                    // Column lineage is derived from a single transform chain; a
                    // graph's per-node chains are not that, so it is left absent
                    // rather than fabricated.
                    column_lineage: None,
                };
                crate::catalog::record(handle, &update).await;
            }
            if let Some(o) = &profile_outcome {
                use crate::serve::history::catalog::{CatalogProfileRecord, dataset_id};
                let uri = canonicalize_uri(&sink_id.dataset_uri, &sink_id.config, clock);
                let record = CatalogProfileRecord {
                    run_id: run_id.to_string(),
                    pipeline: pipeline_name.to_string(),
                    row: row.to_string(),
                    recorded_at: chrono::Utc::now(),
                    profile: o.profile.clone(),
                    drift: o.drift.clone(),
                    baseline_runs: o.baseline_runs,
                };
                crate::catalog::record_profile(handle, &dataset_id(&uri), &record).await;
            }
        }
    }
    profile_failures
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::topology::NodeErrorKind;

    fn cfg(yaml: &str) -> PipelineConfig {
        serde_yaml::from_str(yaml).expect("valid config")
    }

    // ─── build_summary ──────────────────────────────────────────────────────

    /// Per-sink counts as the executor sees them (a `HashMap`, hence unordered).
    fn sinks(pairs: &[(&str, usize)]) -> HashMap<String, usize> {
        pairs.iter().map(|(k, v)| ((*k).to_string(), *v)).collect()
    }

    #[test]
    fn summary_rows_are_one_per_sink_node_sorted_by_id() {
        // `per_sink` is a HashMap, so without the explicit sort the row order
        // would vary run to run — and `faucet run --output json` prints it.
        let summary = build_summary(&sinks(&[("zulu", 3), ("alpha", 7), ("mike", 0)]), &[]);
        let rows: Vec<(&str, usize)> = summary
            .invocations
            .iter()
            .map(|i| (i.row_id.as_str(), i.records_written))
            .collect();
        assert_eq!(rows, vec![("alpha", 7), ("mike", 0), ("zulu", 3)]);
        assert!(
            summary.invocations.iter().all(|i| i.error.is_none()),
            "no node failed, so no row carries an error"
        );
    }

    #[test]
    fn summary_appends_one_row_per_failed_node_attributed_to_that_node() {
        // #459: a failure is attributed to the node that produced it, not to a
        // single flat `topology` row.
        let summary = build_summary(
            &sinks(&[("sink_a", 5)]),
            &[
                ("source_x", "connection refused", Some(NodeErrorKind::Other)),
                ("sink_b", "disk full", Some(NodeErrorKind::Other)),
            ],
        );
        // The sink-count row comes first, then the failures in node order.
        assert_eq!(summary.invocations[0].row_id, "sink_a");
        assert_eq!(summary.invocations[0].records_written, 5);

        let failures: Vec<(&str, &str)> = summary
            .invocations
            .iter()
            .filter_map(|i| i.error.as_deref().map(|e| (i.row_id.as_str(), e)))
            .collect();
        assert_eq!(
            failures,
            vec![("source_x", "connection refused"), ("sink_b", "disk full")]
        );
        assert!(
            summary
                .invocations
                .iter()
                .filter(|i| i.error.is_some())
                .all(|i| i.records_written == 0),
            "a failure row reports no records written"
        );
    }

    #[test]
    fn summary_classifies_from_the_typed_kind_never_from_the_text() {
        // #658 landed, so the kind now travels from the node instead of being
        // dropped — but the original guarantee this test was written for still
        // holds and is the half that matters: classification comes from the
        // typed kind, never from what the message happens to say.
        let looks_like_a_trip = build_summary(
            &sinks(&[("s", 1)]),
            &[("s", "Circuit open after 3 consecutive failures", None)],
        );
        assert!(
            looks_like_a_trip
                .invocations
                .iter()
                .all(|i| i.error_kind.is_none()),
            "a message that reads like a breaker trip, with no typed kind behind \
             it, must not be classified as one"
        );

        // And the converse: a node that really did trip carries the kind
        // through, which is what earns the scheduler cooldown.
        let real_trip = build_summary(
            &sinks(&[("s", 1)]),
            &[("s", "anything at all", Some(NodeErrorKind::CircuitOpen))],
        );
        let classified: Vec<_> = real_trip
            .invocations
            .iter()
            .filter_map(|i| i.error_kind)
            .collect();
        assert_eq!(
            classified,
            vec![InvocationErrorKind::CircuitOpen],
            "the typed kind must survive the hop into the run summary"
        );
    }

    #[test]
    fn summary_of_an_empty_run_is_empty() {
        assert!(build_summary(&HashMap::new(), &[]).invocations.is_empty());
    }

    #[cfg(feature = "lineage")]
    fn lineage_cfg() -> faucet_lineage::LineageConfig {
        serde_json::from_value(serde_json::json!({
            "namespace": "ns",
            "transport": { "type": "file", "config": { "path": "/tmp/ol.jsonl" } },
        }))
        .expect("valid lineage config")
    }

    const LINEAR: &str = r#"version: 1
name: p
pipeline:
  sources:
    a: { type: csv, config: { path: /tmp/a.csv } }
  sinks:
    o: { type: jsonl, config: { path: /tmp/o.jsonl } }
  nodes:
    s: { kind: source, ref: a }
    w: { kind: sink, ref: o }
  edges:
    - { from: s, to: w }
"#;

    /// The invariant #459 exists to hold: nothing is parsed-but-ignored, so
    /// `validate` has nothing to warn about. If this fails because a block was
    /// added to the list, wire the block instead of updating the assertion.
    #[test]
    fn no_block_is_inert() {
        let mut c = cfg(LINEAR);
        c.sla =
            Some(serde_json::from_value(serde_json::json!({ "max_staleness_secs": 60 })).unwrap());
        assert!(inert_blocks(&c).is_empty());
    }

    /// `is_topology` is the load-bearing predicate that branches run/validate/
    /// preview between matrix mode and topology mode: a non-empty `pipeline.nodes`
    /// selects topology mode, its absence keeps matrix mode.
    #[test]
    fn is_topology_true_only_with_nodes() {
        assert!(
            is_topology(&cfg(LINEAR)),
            "a config with nodes is topology mode"
        );

        let matrix = cfg(r#"version: 1
name: p
pipeline:
  source: { type: csv, config: { path: /tmp/a.csv } }
  sink:   { type: jsonl, config: { path: /tmp/o.jsonl } }
"#);
        assert!(
            !is_topology(&matrix),
            "a plain source/sink config is matrix mode, not topology"
        );
    }

    /// Kind precedence: an inline `type` on the node wins over its template, so
    /// the exactly-once gate classifies the connector the run will actually build.
    #[test]
    fn resolved_kind_prefers_inline_type_over_template() {
        let c = cfg(r#"version: 1
name: p
pipeline:
  sinks:
    o: { type: jsonl, config: { path: /tmp/o.jsonl } }
  nodes:
    s: { kind: source, type: csv, config: { path: /tmp/a.csv } }
    w: { kind: sink, ref: o, type: stdout, config: {} }
  edges:
    - { from: s, to: w }
"#);
        assert_eq!(
            resolved_node_kind(&c, &c.pipeline.nodes["w"]).as_deref(),
            Some("stdout")
        );
        assert_eq!(
            resolved_node_kind(&c, &c.pipeline.nodes["s"]).as_deref(),
            Some("csv")
        );
    }

    /// A non-connector node has no kind — the gate must skip it rather than
    /// treating an empty string as an unsupported connector.
    #[test]
    fn resolved_kind_is_none_for_a_structural_node() {
        let c = cfg(r#"version: 1
name: p
pipeline:
  sources:
    a: { type: csv, config: { path: /tmp/a.csv } }
  sinks:
    o: { type: jsonl, config: { path: /tmp/o.jsonl } }
  nodes:
    s: { kind: source, ref: a }
    f: { kind: tee, fanout: 1 }
    w: { kind: sink, ref: o }
  edges:
    - { from: s, to: f }
    - { from: f, to: w }
"#);
        assert!(resolved_node_kind(&c, &c.pipeline.nodes["f"]).is_none());
    }

    /// A sink with no reaching source produces no lineage event. Emitting one
    /// would name an input dataset the run never read.
    #[cfg(feature = "lineage")]
    #[test]
    fn lineage_ctx_is_none_without_a_reaching_source() {
        let c = cfg(LINEAR);
        let mut identities = NodeIdentities::new();
        identities.insert(
            "w".to_string(),
            NodeIdentity {
                kind: "jsonl".into(),
                dataset_uri: "file:///tmp/o.jsonl".into(),
                config: Value::Null,
            },
        );
        let lc = lineage_cfg();
        // `reaching` deliberately empty: the sink is in the graph but no source
        // reaches it.
        let ctx = lineage_ctx(
            &c,
            "p",
            "run-1",
            "w",
            &identities,
            &std::collections::HashMap::new(),
            &lc,
            0,
            None,
        );
        assert!(ctx.is_none());
    }

    /// A merge sink's event carries every reaching source as an input, and
    /// deliberately no column lineage — the per-column derivation is not knowable
    /// from the graph, so it is omitted rather than guessed.
    #[cfg(feature = "lineage")]
    #[test]
    fn lineage_ctx_carries_all_inputs_and_no_column_lineage() {
        let c = cfg(LINEAR);
        let mut identities = NodeIdentities::new();
        for (id, uri) in [
            ("sa", "file:///tmp/a.csv"),
            ("sb", "file:///tmp/b.csv"),
            ("w", "file:///tmp/o.jsonl"),
        ] {
            identities.insert(
                id.to_string(),
                NodeIdentity {
                    kind: "csv".into(),
                    dataset_uri: uri.into(),
                    config: Value::Null,
                },
            );
        }
        let lc = lineage_cfg();
        let reaching = std::collections::HashMap::from([(
            "w".to_string(),
            vec!["sa".to_string(), "sb".to_string()],
        )]);
        let ctx = lineage_ctx(&c, "p", "run-1", "w", &identities, &reaching, &lc, 7, None)
            .expect("both sources known");
        assert_eq!(ctx.job_name, "p.w");
        assert_eq!(
            ctx.inputs
                .iter()
                .map(|d| d.name.as_str())
                .collect::<Vec<_>>(),
            vec!["file:///tmp/a.csv", "file:///tmp/b.csv"]
        );
        assert_eq!(ctx.output.name, "file:///tmp/o.jsonl");
        assert_eq!(ctx.records, 7);
        assert!(ctx.column_lineage.is_none());
    }

    /// An input whose identity was never recorded is dropped, not rendered as an
    /// empty dataset name.
    #[cfg(feature = "lineage")]
    #[test]
    fn lineage_ctx_skips_an_unknown_input() {
        let c = cfg(LINEAR);
        let mut identities = NodeIdentities::new();
        for (id, uri) in [("sa", "file:///tmp/a.csv"), ("w", "file:///tmp/o.jsonl")] {
            identities.insert(
                id.to_string(),
                NodeIdentity {
                    kind: "csv".into(),
                    dataset_uri: uri.into(),
                    config: Value::Null,
                },
            );
        }
        let lc = lineage_cfg();
        let reaching = std::collections::HashMap::from([(
            "w".to_string(),
            vec!["sa".to_string(), "ghost".to_string()],
        )]);
        let ctx = lineage_ctx(&c, "p", "run-1", "w", &identities, &reaching, &lc, 1, None)
            .expect("one known source is enough");
        assert_eq!(ctx.inputs.len(), 1);
    }

    /// `reaching_sources` walks through structural nodes, so a join's two labelled
    /// inputs both surface on the downstream sink.
    #[test]
    fn reaching_sources_traverses_a_join() {
        let c = cfg(r#"version: 1
name: p
pipeline:
  sources:
    a: { type: csv, config: { path: /tmp/a.csv } }
    b: { type: csv, config: { path: /tmp/b.csv } }
  sinks:
    o: { type: jsonl, config: { path: /tmp/o.jsonl } }
  nodes:
    probe: { kind: source, ref: a }
    build: { kind: source, ref: b }
    j:
      kind: join
      mode: left
      build: { edge: build_in, key: id }
      probe: { edge: probe_in, key: id }
    w: { kind: sink, ref: o }
  edges:
    - { from: probe, to: j, as: probe_in }
    - { from: build, to: j, as: build_in }
    - { from: j, to: w }
"#);
        let reaching = reaching_sources(&c);
        assert_eq!(
            reaching["w"],
            vec!["build".to_string(), "probe".to_string()]
        );
    }

    /// A cyclic edge set must not hang the traversal. Cycles are rejected by
    /// graph validation, but this helper also runs from `validate`, so it has to
    /// terminate on a config that has not been validated yet.
    #[test]
    fn reaching_sources_terminates_on_a_cycle() {
        let c = cfg(r#"version: 1
name: p
pipeline:
  sources:
    a: { type: csv, config: { path: /tmp/a.csv } }
  sinks:
    o: { type: jsonl, config: { path: /tmp/o.jsonl } }
  nodes:
    s: { kind: source, ref: a }
    t1: { kind: transform, transforms: [ { type: flatten, config: {} } ] }
    t2: { kind: transform, transforms: [ { type: flatten, config: {} } ] }
    w: { kind: sink, ref: o }
  edges:
    - { from: s, to: t1 }
    - { from: t1, to: t2 }
    - { from: t2, to: t1 }
    - { from: t2, to: w }
"#);
        assert_eq!(reaching_sources(&c)["w"], vec!["s".to_string()]);
    }
}
