//! Expand a parsed [`PipelineConfig`] into a flat list of [`ExpandedNode`]s
//! ready for the executor to run.
//!
//! Responsibilities:
//! - Assign synthetic ids to anonymous rows (`row-0`, `row-1`, …).
//! - Reject reserved row ids (`env`, `file`, `secret`, `matrix`, `pipeline`).
//! - Reject duplicate ids.
//! - Validate that every `parent:` references a known row id and that the
//!   parent chain has no cycles.
//! - Deep-merge each row's partial overrides into `pipeline.*`.
//! - Find every `${id.path}` token surviving from load-time interpolation and
//!   record where each one came from. Tokens that reference unknown ids
//!   produce a `CliError::UnknownInterpolationId` here, not at runtime.

use crate::config::{
    ConnectorSpec, MatrixRow, PartialConnector, PipelineConfig, PipelineSpec, StateStoreSpec,
    TransformSpec,
};
use crate::error::{CliError, CliResult};
use crate::interpolate::{Directive, iter_directives};
use crate::merge::merge_value;
use serde_json::Value;
use std::collections::{BTreeSet, HashMap, HashSet};

/// Row ids that callers can never use because they collide with
/// load-time interpolation prefixes or future runtime scopes.
pub const RESERVED_IDS: &[&str] = &[
    "env",
    "file",
    "secret",
    "matrix",
    "pipeline",
    "now",
    "backfill",
    "tenant",
    "param",
    "partition",
    "bookmark",
    "job_id",
    "window",
];

/// One fully-merged matrix row, ready for the executor.
#[derive(Debug, Clone)]
pub struct ExpandedNode {
    pub id: String,
    pub row_index: usize,
    /// Relative dispatch cost for `execution.schedule: lpt` (#644); `None`
    /// means unranked, which sorts after every weighted row.
    pub weight: Option<f64>,
    pub role: NodeRole,
    pub source: ConnectorSpec,
    pub sink: ConnectorSpec,
    pub transforms: Vec<TransformSpec>,
    pub state: Option<StateStoreSpec>,
    /// Resolved DLQ spec for this row, or `None` if no DLQ applies.
    pub dlq: Option<crate::config::DlqSpec>,
    /// This row's SLA override (#679), or `None` to use the top-level `sla:`.
    pub sla: Option<crate::sla::SlaSpec>,
    /// The effective column-profiling spec (#708): the row's own `profiling:`
    /// or the top-level block; `None` when neither is set.
    pub profiling: Option<faucet_core::ProfilingSpec>,
    /// The effective data-flow policy (#702) — the top-level `policy:` block
    /// with any `--policy` file already merged in; `None` when neither is set.
    #[cfg(feature = "policy")]
    pub policy: Option<faucet_core::PolicySpec>,
    /// Pipeline-level quality spec, shared by every node. `quality:` has no
    /// matrix-row override in v1, so this is `cfg.pipeline.quality` verbatim.
    #[cfg(feature = "quality")]
    pub quality: Option<faucet_core::QualitySpec>,
    /// Pipeline-level data contract, shared by every node (`contract:` has no
    /// matrix-row override in v1) — `cfg.pipeline.contract` verbatim.
    #[cfg(feature = "contract")]
    pub contract: Option<faucet_core::ContractSpec>,
    /// Pipeline-level PII masking policy, shared by every node (`masking:` has
    /// no matrix-row override in v1) — `cfg.pipeline.masking` verbatim. The
    /// executor compiles it *scoped to this node's sink* ([`sink_ref`] +
    /// [`sink`].`kind`) so `applies_to` destination-scoping works (#206).
    ///
    /// [`sink_ref`]: ExpandedNode::sink_ref
    /// [`sink`]: ExpandedNode::sink
    #[cfg(feature = "masking")]
    pub masking: Option<faucet_core::MaskingSpec>,
    /// The sink template name this node resolved (`sink.ref`, or `"default"`
    /// for the legacy singular `pipeline.sink`). Used to scope masking
    /// `applies_to` rules per destination.
    pub sink_ref: String,
    /// Compiled schema-drift policy spec (pipeline-level; same for every node).
    pub schema: Option<faucet_core::SchemaDriftSpec>,
    /// Delivery guarantee for this row. Resolved from the row's override or
    /// falls back to the top-level `cfg.delivery`.
    pub delivery: faucet_core::DeliveryMode,
    /// The **derived** end-to-end guarantee this row's source × sink × config
    /// actually provides (issue #292) — computed for every row regardless of
    /// the requested `delivery:` mode, so `faucet validate` / `doctor` report
    /// it truthfully (e.g. a keyed-upsert row is effectively-once even when
    /// the user did not ask for `exactly_once`).
    pub delivery_guarantee: faucet_core::DeliveryGuarantee,
    /// Row ids this node waits for (deduplicated, declaration order). The
    /// executor starts the node only after every listed row's invocations
    /// finish successfully; a failed or skipped dependency skips this node.
    pub depends_on: Vec<String>,
    /// Resolved readiness status for this row's source (#371) — the source
    /// template's `status` overridden by the row's `source.status`, defaulting
    /// to [`SourceStatus::Active`] when neither is set. Drives the runtime
    /// run-set status gate; does not affect the state key.
    ///
    /// [`SourceStatus::Active`]: crate::config::SourceStatus::Active
    pub status: crate::config::SourceStatus,
    /// Effective classification tags (#376) = source-template `tags` ∪ row
    /// `tags` (union, deduped, sorted). Drives the runtime `--tag` narrowing;
    /// does not affect the state key.
    pub tags: Vec<String>,
    /// Every `${id.path}` placeholder that survived load-time interpolation.
    /// Populated by `collect_deferred`; the executor uses this to know
    /// which parent record to feed which row.
    pub deferred_refs: Vec<DeferredRef>,
    /// A pre-built source that replaces the registry-built one for this node.
    /// Set only by `faucet dlq replay` (#281), which injects a
    /// [`DlqReaderSource`](crate::dlq_replay::reader::DlqReaderSource) so the
    /// executor runs it through the normal pipeline path. `None` for every
    /// config-driven node (the executor builds the source from `source.kind`).
    pub source_override: Option<crate::dlq_replay::reader::SourceOverride>,
    /// Scoped-cleanup claim (#478): the source's `complete_for` scope, still
    /// carrying any `${parent.*}` / `${now.*}` tokens — the executor resolves
    /// them per invocation, like the connector configs. `Some` only when the
    /// destination sink also opted in with `cleanup: delete_missing`, so this
    /// being present already means a cleanup is intended.
    pub cleanup_scope: Option<std::collections::BTreeMap<String, serde_json::Value>>,
    /// Pipeline-level `_faucet_*` metadata columns (#510), shared by every node;
    /// the executor wraps the sink in a `MetadataSink` decorator when present.
    pub metadata_columns: Option<faucet_core::MetadataColumnsSpec>,
    /// Pipeline-level local-output retention policy (#587), shared by every
    /// node. The executor reads it after a successful invocation to decide
    /// whether to record the files the sink wrote, and with what window.
    #[cfg(feature = "catalog")]
    pub local_outputs: Option<crate::local_outputs::LocalOutputsSpec>,
}

#[derive(Debug, Clone)]
pub enum NodeRole {
    /// Root node — runs once per pipeline invocation.
    Root,
    /// Child node — runs once per record produced by the parent row.
    Child {
        parent_id: String,
        parent_key: String,
    },
    /// Discovery dimension (#501) — runs its source once, projects `select`,
    /// dedups, and publishes the value-set under `as_alias`. No sink.
    ///
    /// **Chained / collected discovery (#531):** when `dims` is non-empty the
    /// discovery is itself fanned out over the cartesian product of those
    /// upstream discovery dimensions (its `for_each`), running once per tuple
    /// with `${dim}` tokens resolved in its source config; with `collect: true`
    /// each tuple's value-set is published as one list keyed by the tuple
    /// (a [`CollectedDim`](crate::discovery_matrix::CollectedDim)) rather than as
    /// a flat cartesian axis.
    Discovery {
        select: String,
        as_alias: String,
        collect: bool,
        dims: Vec<String>,
    },
    /// Discovery-driven fan-out (#501) — runs once per tuple of the cartesian
    /// product of the named discovery dimensions. `dims` are discovery row ids
    /// (also mirrored into `depends_on` for readiness/skip/cycle reuse).
    /// `collected` (#531) names the collected discovery rows this row references
    /// (`${id.alias}`), whose per-tuple lists are injected into each tuple ctx.
    Product {
        dims: Vec<String>,
        collected: Vec<String>,
    },
}

#[derive(Debug, Clone)]
pub struct DeferredRef {
    pub referenced_id: String,
    pub dotted_path: String,
    pub token: String,
}

/// The metadata-columns policy a node runs with: the config's own, plus the
/// `run_id` column when a `rollback:` block needs it (#706).
fn effective_metadata_columns(cfg: &PipelineConfig) -> Option<faucet_core::MetadataColumnsSpec> {
    match cfg.rollback.as_ref().filter(|r| r.enabled) {
        Some(_) => Some(crate::rollback::ensure_run_id_column(
            cfg.metadata_columns.as_ref(),
        )),
        None => cfg.metadata_columns.clone(),
    }
}

/// In-memory lookup of source / sink templates, built once per `expand()` call.
/// Combines named entries from `pipeline.sources` / `pipeline.sinks` with the
/// legacy singular `pipeline.source` / `pipeline.sink` (registered as `default`).
struct Registry<'a> {
    sources: HashMap<&'a str, &'a ConnectorSpec>,
    sinks: HashMap<&'a str, &'a ConnectorSpec>,
}

impl<'a> Registry<'a> {
    fn build(spec: &'a PipelineSpec) -> CliResult<Self> {
        let mut sources: HashMap<&'a str, &'a ConnectorSpec> = HashMap::new();
        if let Some(default) = spec.source.as_ref() {
            sources.insert("default", default);
        }
        for (name, s) in spec.sources.iter() {
            if sources.contains_key(name.as_str()) {
                return Err(CliError::DuplicateTemplate {
                    kind: "source",
                    name: name.clone(),
                });
            }
            sources.insert(name.as_str(), s);
        }

        let mut sinks: HashMap<&'a str, &'a ConnectorSpec> = HashMap::new();
        if let Some(default) = spec.sink.as_ref() {
            if default.transforms.is_some() {
                return Err(CliError::TransformsOnSink {
                    name: "default".to_string(),
                });
            }
            if !default.inherit_transforms {
                return Err(CliError::InheritTransformsOnSink {
                    name: "default".to_string(),
                });
            }
            sinks.insert("default", default);
        }
        for (name, s) in spec.sinks.iter() {
            if sinks.contains_key(name.as_str()) {
                return Err(CliError::DuplicateTemplate {
                    kind: "sink",
                    name: name.clone(),
                });
            }
            if s.transforms.is_some() {
                return Err(CliError::TransformsOnSink { name: name.clone() });
            }
            if !s.inherit_transforms {
                return Err(CliError::InheritTransformsOnSink { name: name.clone() });
            }
            sinks.insert(name.as_str(), s);
        }
        Ok(Self { sources, sinks })
    }

    fn known(&self, kind: &'static str) -> Vec<String> {
        debug_assert!(
            matches!(kind, "source" | "sink"),
            "Registry::known called with kind = {:?}",
            kind
        );
        let map = if kind == "source" {
            &self.sources
        } else {
            &self.sinks
        };
        let mut out: Vec<String> = map.keys().map(|s| (*s).to_string()).collect();
        out.sort();
        out
    }

    fn resolve(
        &self,
        kind: &'static str,
        row_id: &str,
        overlay: Option<&PartialConnector>,
    ) -> CliResult<ConnectorSpec> {
        debug_assert!(
            matches!(kind, "source" | "sink"),
            "Registry::resolve called with kind = {:?}",
            kind
        );
        let map = if kind == "source" {
            &self.sources
        } else {
            &self.sinks
        };
        let ref_name = overlay
            .and_then(|p| p.r#ref.as_deref())
            .unwrap_or("default");
        let base = map.get(ref_name).ok_or_else(|| {
            if ref_name == "default" {
                CliError::MissingTemplate {
                    kind,
                    row_id: row_id.to_owned(),
                }
            } else {
                CliError::UnknownTemplate {
                    kind,
                    name: ref_name.to_owned(),
                    row_id: row_id.to_owned(),
                    known: self.known(kind),
                }
            }
        })?;
        let mut out = (*base).clone();
        if let Some(p) = overlay {
            if let Some(k) = &p.kind {
                out.kind = k.clone();
            }
            if let Some(c) = &p.config {
                merge_value(&mut out.config, c.clone());
            }
            // Readiness ladder is a scalar: a row `source.status` replaces the
            // template's (#371). `tags` are handled separately (union, not
            // replace) in `expand`, since `PartialConnector` carries no `tags`.
            if p.status.is_some() {
                out.status = p.status;
            }
            if let Some(attrs) = &p.attributes {
                out.attributes.extend(attrs.clone());
            }
        }
        Ok(out)
    }
}

/// Expand `cfg` into a topologically valid list of nodes. Roots come first,
/// then children in BFS order.
pub fn expand(cfg: &PipelineConfig) -> CliResult<Vec<ExpandedNode>> {
    // Fail-fast at config load: validate the execution-level adaptive
    // batch-size controller here (the shared `validate`/`run`/`preview`/
    // `doctor`/`schedule` gate) so `faucet validate` rejects a bad block
    // rather than only surfacing it mid-run in the executor.
    if let Some(ab) = cfg
        .execution
        .as_ref()
        .and_then(|e| e.adaptive_batch_size.as_ref())
    {
        // `validate()` returns `FaucetError::Config` whose message already names
        // the offending field; propagate it directly (CliError: From<FaucetError>).
        ab.validate()?;
    }

    // Implicit single-row case: empty matrix → run pipeline once with no merge.
    let synthetic_row;
    let rows: &[MatrixRow] = if cfg.matrix.is_empty() {
        synthetic_row = [MatrixRow {
            id: None,
            parent: None,
            weight: None,
            depends_on: Vec::new(),
            parent_key: "id".into(),
            source: None,
            sink: None,
            transforms: None,
            inherit_transforms: true,
            state: None,
            dlq: None,
            delivery: None,
            sla: None,
            profiling: None,
            tags: Vec::new(),
            partition: None,
            discover: None,
            for_each: Vec::new(),
        }];
        &synthetic_row
    } else {
        &cfg.matrix
    };

    // 1) Assign / validate ids.
    let mut ids: Vec<String> = Vec::with_capacity(rows.len());
    let mut seen: HashSet<String> = HashSet::new();
    for (i, row) in rows.iter().enumerate() {
        let id = match &row.id {
            Some(s) => s.clone(),
            None => format!("row-{i}"),
        };
        if RESERVED_IDS.contains(&id.as_str()) {
            return Err(CliError::ReservedRowId { id });
        }
        if !seen.insert(id.clone()) {
            return Err(CliError::DuplicateRowId { id });
        }
        ids.push(id);
    }
    // Flow-auth capture names (#567) are runtime-resolved deferred tokens: a
    // source body/header may reference `${session_id}` where `session_id` is
    // captured by a `type: flow` provider's login step and substituted per
    // request by the connector. Treat them like row ids for `${...}` validation.
    let capture_names: Vec<String> = collect_flow_capture_names(cfg);
    let id_set: HashSet<&str> = ids
        .iter()
        .chain(capture_names.iter())
        .map(String::as_str)
        .collect();

    // 1b) Discovery-driven matrix (#501): identify `discover:` rows and validate
    // the `discover:` / `for_each:` shapes before the graph checks below, so
    // `for_each` dims can be folded into the dependency graph.
    let discovery_ids: HashSet<&str> = rows
        .iter()
        .zip(ids.iter())
        .filter(|(row, _)| row.discover.is_some())
        .map(|(_, id)| id.as_str())
        .collect();
    // Collected (list-valued) discovery rows (#531) — referenced via `${id.alias}`
    // (not via `for_each`), so a consuming row depends on them explicitly.
    let collect_discovery_ids: HashSet<&str> = rows
        .iter()
        .zip(ids.iter())
        .filter(|(row, _)| row.discover.as_ref().is_some_and(|d| d.collect))
        .map(|(_, id)| id.as_str())
        .collect();
    for (i, row) in rows.iter().enumerate() {
        let id = ids[i].as_str();
        if let Some(disc) = &row.discover {
            // Chained / two-level discovery (#531): a `discover:` row MAY also
            // declare `for_each:` — it then runs once per upstream tuple and must
            // `collect: true` (publish one list per tuple, not a cartesian axis).
            if !row.for_each.is_empty() && !disc.collect {
                return Err(CliError::Config(format!(
                    "matrix row '{id}': a chained `discover:` row (with `for_each:`) must set \
                     `collect: true` — it publishes one list per upstream tuple"
                )));
            }
            if disc.collect && row.for_each.is_empty() {
                return Err(CliError::Config(format!(
                    "matrix row '{id}': `discover.collect: true` requires `for_each:` — it collects \
                     one list per upstream discovery tuple"
                )));
            }
            if row.parent.is_some() {
                return Err(CliError::Config(format!(
                    "matrix row '{id}': a `discover:` row cannot also declare `parent:`"
                )));
            }
            if row.sink.is_some() {
                return Err(CliError::Config(format!(
                    "matrix row '{id}': a `discover:` row has no sink — remove its `sink:` override"
                )));
            }
            if row.transforms.is_some() {
                return Err(CliError::Config(format!(
                    "matrix row '{id}': a `discover:` row does not run transforms"
                )));
            }
            if disc.select.trim().is_empty() {
                return Err(CliError::Config(format!(
                    "matrix row '{id}': `discover.select` must not be empty"
                )));
            }
            if !is_ident(&disc.as_alias) {
                return Err(CliError::Config(format!(
                    "matrix row '{id}': `discover.as` ('{}') must match ^[a-z0-9][a-z0-9_-]*$",
                    disc.as_alias
                )));
            }
        }
        if !row.for_each.is_empty() {
            if row.parent.is_some() {
                return Err(CliError::Config(format!(
                    "matrix row '{id}': `for_each:` and `parent:` cannot be combined (v1) — a row \
                     fans out over the discovery cross-product OR a parent's records, not both"
                )));
            }
            let mut seen_dims: HashSet<&str> = HashSet::new();
            for dim in &row.for_each {
                if dim.as_str() == id {
                    return Err(CliError::Config(format!(
                        "matrix row '{id}': `for_each` cannot reference itself"
                    )));
                }
                if !id_set.contains(dim.as_str()) {
                    return Err(CliError::Config(format!(
                        "matrix row '{id}': `for_each` references unknown row '{dim}'"
                    )));
                }
                if !discovery_ids.contains(dim.as_str()) {
                    return Err(CliError::Config(format!(
                        "matrix row '{id}': `for_each` row '{dim}' is not a `discover:` row"
                    )));
                }
                if !seen_dims.insert(dim.as_str()) {
                    return Err(CliError::Config(format!(
                        "matrix row '{id}': `for_each` lists '{dim}' more than once"
                    )));
                }
            }
        }
    }

    // 2) Validate parents + detect cycles.
    let mut parents: HashMap<&str, &str> = HashMap::new();
    for (i, row) in rows.iter().enumerate() {
        let id = ids[i].as_str();
        if let Some(parent) = row.parent.as_deref() {
            if !id_set.contains(parent) {
                return Err(CliError::UnknownParent {
                    id: id.to_owned(),
                    parent: parent.to_owned(),
                });
            }
            if parent == id {
                return Err(CliError::ParentCycle {
                    ids: vec![id.to_owned()],
                });
            }
            parents.insert(id, parent);
        }
    }
    detect_cycle(&parents)?;

    // 2b) Validate `depends_on` edges (unknown id, self-dependency) and
    // dedup each row's list while preserving declaration order. Then check
    // the *combined* parent + depends_on graph for cycles — `detect_cycle`
    // above only walks single-parent chains, so a cycle routed through a
    // `depends_on` edge would otherwise deadlock the executor at run time.
    let mut deps_by_row: Vec<Vec<String>> = Vec::with_capacity(rows.len());
    // Collected discovery rows (#531) each row references via `${id.alias}`, in
    // referenced order (deduped). Consumed when building the `Product` role so
    // each tuple ctx gets the collected list injected.
    let mut collected_refs_by_row: Vec<Vec<String>> = Vec::with_capacity(rows.len());
    for (i, row) in rows.iter().enumerate() {
        let id = ids[i].as_str();
        let mut deps: Vec<String> = Vec::with_capacity(row.depends_on.len());
        for dep in &row.depends_on {
            if !id_set.contains(dep.as_str()) {
                return Err(CliError::UnknownDependency {
                    id: id.to_owned(),
                    depends_on: dep.clone(),
                });
            }
            if dep == id {
                return Err(CliError::DependencyCycle {
                    ids: vec![id.to_owned()],
                });
            }
            if !deps.contains(dep) {
                deps.push(dep.clone());
            }
        }
        // A `for_each` row (#501) waits for every discovery dimension it fans
        // out over; model those as `depends_on` edges so readiness, the skip
        // cascade, and cycle detection all reuse the existing machinery.
        for dim in &row.for_each {
            if !deps.contains(dim) {
                deps.push(dim.clone());
            }
        }
        // Collected-discovery references (#531): `${props.name}` in a row's
        // config makes it depend on `props` (which is NOT one of its `for_each`
        // dims), so `props` runs first and its per-tuple lists are available.
        let mut collected_refs: Vec<String> = Vec::new();
        let mut refs = Vec::new();
        if let Some(p) = &row.source
            && let Some(c) = &p.config
        {
            collect_deferred(c, &mut refs);
        }
        if let Some(p) = &row.sink
            && let Some(c) = &p.config
        {
            collect_deferred(c, &mut refs);
        }
        if let Some(disc) = &row.discover
            && let Some(c) = &disc.source.config
        {
            collect_deferred(c, &mut refs);
        }
        for r in &refs {
            if r.referenced_id == id {
                continue;
            }
            if collect_discovery_ids.contains(r.referenced_id.as_str()) {
                if !deps.contains(&r.referenced_id) {
                    deps.push(r.referenced_id.clone());
                }
                if !collected_refs.contains(&r.referenced_id) {
                    collected_refs.push(r.referenced_id.clone());
                }
            }
        }
        deps_by_row.push(deps);
        collected_refs_by_row.push(collected_refs);
    }
    detect_combined_cycle(&ids, &parents, &deps_by_row)?;

    // 3) Validate `${id.path}` references — each `id` must be a known row.
    // We scan the *raw* (pre-merge) row configs because interpolation lives in
    // strings that survive merging unchanged.
    //
    // Discovery-recipe naming tokens (`${name}` etc.) are legal only when some
    // source in the config actually carries a discovery recipe to resolve them
    // — scoping the exemption keeps `${name}` a hard validation error in every
    // ordinary config, where it is almost always a typo'd `${vars.name}`.
    let any_source_recipe = cfg
        .pipeline
        .source
        .as_ref()
        .map(|s| has_discovery_recipe(&s.config))
        .unwrap_or(false)
        || cfg
            .pipeline
            .sources
            .values()
            .any(|s| has_discovery_recipe(&s.config))
        || rows.iter().any(|row| {
            row.discover.is_some()
                || row
                    .source
                    .as_ref()
                    .and_then(|p| p.config.as_ref())
                    .is_some_and(has_discovery_recipe)
        });
    // Connector-level runtime placeholders: bare `${name}` tokens a connector
    // substitutes itself — `${next_token}` in an XML `BodyCursor` request
    // template, and every value a `type: flow` auth provider captures
    // (`${session}` in a header). They are legal only in connector configs,
    // and only the names the config actually declares.
    let connector_tokens = connector_placeholders(cfg);
    for (i, row) in rows.iter().enumerate() {
        let id = ids[i].as_str();
        if let Some(p) = &row.source
            && let Some(c) = &p.config
        {
            check_refs(c, &id_set, id, any_source_recipe, &connector_tokens)?;
        }
        if let Some(p) = &row.sink
            && let Some(c) = &p.config
        {
            check_refs(c, &id_set, id, any_source_recipe, &connector_tokens)?;
        }
        // A chained `discover:` row's source config (#531) references its upstream
        // dimension (`${types.name}`); validate those refs too.
        if let Some(disc) = &row.discover
            && let Some(c) = &disc.source.config
        {
            check_refs(c, &id_set, id, true, &connector_tokens)?;
        }
    }
    if let Some(s) = &cfg.pipeline.source {
        check_refs(
            &s.config,
            &id_set,
            "pipeline.source",
            any_source_recipe,
            &connector_tokens,
        )?;
    }
    if let Some(s) = &cfg.pipeline.sink {
        check_refs(
            &s.config,
            &id_set,
            "pipeline.sink",
            any_source_recipe,
            &connector_tokens,
        )?;
    }
    for (name, s) in &cfg.pipeline.sources {
        check_refs(
            &s.config,
            &id_set,
            &format!("pipeline.sources.{name}"),
            any_source_recipe,
            &connector_tokens,
        )?;
    }
    for (name, s) in &cfg.pipeline.sinks {
        check_refs(
            &s.config,
            &id_set,
            &format!("pipeline.sinks.{name}"),
            any_source_recipe,
            &connector_tokens,
        )?;
    }

    // 4) Build template registry — validates duplicate default conflicts.
    let registry = Registry::build(&cfg.pipeline)?;

    // 5) Build expanded nodes. Order: roots first (in declaration order),
    // then BFS over children — guarantees a parent appears before its children.
    let mut by_parent: HashMap<&str, Vec<usize>> = HashMap::new();
    let mut roots: Vec<usize> = Vec::new();
    for (i, row) in rows.iter().enumerate() {
        match row.parent.as_deref() {
            None => roots.push(i),
            Some(p) => by_parent.entry(p).or_default().push(i),
        }
    }

    let mut order: Vec<usize> = Vec::with_capacity(rows.len());
    let mut queue: std::collections::VecDeque<usize> = roots.into_iter().collect();
    while let Some(idx) = queue.pop_front() {
        order.push(idx);
        if let Some(children) = by_parent.get(ids[idx].as_str()) {
            queue.extend(children.iter().copied());
        }
    }
    debug_assert_eq!(order.len(), rows.len());

    let mut out = Vec::with_capacity(rows.len());
    for &i in &order {
        let row = &rows[i];
        let row_id = ids[i].as_str();

        // Discovery row (#501): a value-enumeration step with no sink. Build a
        // minimal node from `discover.source` and skip the entire source→sink
        // pipeline (templates, write-mode, exactly-once, drift, cleanup) — none
        // of it applies to an enumeration that writes nothing.
        if let Some(disc) = &row.discover {
            // Resolve the discovery source: a `{ ref }` merges over the named
            // `pipeline.sources` template; a standalone `{ type, config }` is
            // used verbatim (NOT merged over `default`, which would pollute the
            // enumeration with the data source's `${dim}` tokens).
            let src = if disc.source.r#ref.is_some() {
                registry.resolve("source", row_id, Some(&disc.source))?
            } else {
                let kind = disc.source.kind.clone().ok_or_else(|| {
                    CliError::Config(format!(
                        "matrix row '{row_id}': `discover.source` needs a `type` (or a `ref` to a \
                         pipeline.sources template)"
                    ))
                })?;
                ConnectorSpec {
                    kind,
                    config: disc
                        .source
                        .config
                        .clone()
                        .unwrap_or_else(|| Value::Object(Default::default())),
                    transforms: None,
                    inherit_transforms: true,
                    status: None,
                    tags: Vec::new(),
                    complete_for: None,
                    attributes: Default::default(),
                }
            };
            out.push(ExpandedNode {
                id: ids[i].clone(),
                row_index: i,
                // A discovery row enumerates a value set rather than moving
                // data, so it carries no dispatch cost to rank by.
                weight: None,
                role: NodeRole::Discovery {
                    select: disc.select.clone(),
                    as_alias: disc.as_alias.clone(),
                    collect: disc.collect,
                    dims: row.for_each.clone(),
                },
                // `sink` is a never-built placeholder (run_discovery ignores it).
                sink: src.clone(),
                source: src,
                transforms: Vec::new(),
                state: None,
                dlq: None,
                sla: None,
                profiling: None,
                #[cfg(feature = "policy")]
                policy: None,
                delivery: faucet_core::DeliveryMode::AtLeastOnce,
                delivery_guarantee: faucet_core::DeliveryGuarantee::AtLeastOnce,
                #[cfg(feature = "quality")]
                quality: None,
                #[cfg(feature = "contract")]
                contract: None,
                #[cfg(feature = "masking")]
                masking: None,
                sink_ref: "default".to_string(),
                schema: None,
                depends_on: deps_by_row[i].clone(),
                status: crate::config::SourceStatus::default(),
                tags: Vec::new(),
                deferred_refs: Vec::new(),
                source_override: None,
                cleanup_scope: None,
                metadata_columns: None,
                #[cfg(feature = "catalog")]
                local_outputs: None,
            });
            continue;
        }

        let merged_source = registry.resolve("source", row_id, row.source.as_ref())?;
        let merged_sink = registry.resolve("sink", row_id, row.sink.as_ref())?;
        // The sink template name this row resolved (or the legacy `default`),
        // used to scope masking `applies_to` per destination.
        let sink_ref = row
            .sink
            .as_ref()
            .and_then(|s| s.r#ref.clone())
            .unwrap_or_else(|| "default".to_string());
        let role = if !row.for_each.is_empty() {
            // Discovery-driven fan-out (#501): runs once per cartesian-product
            // tuple of the named dimensions (also mirrored into `depends_on`).
            // `collected` (#531) names the collected discovery rows this row
            // injects a per-tuple list from.
            NodeRole::Product {
                dims: row.for_each.clone(),
                collected: collected_refs_by_row[i].clone(),
            }
        } else {
            match &row.parent {
                None => NodeRole::Root,
                Some(p) => NodeRole::Child {
                    parent_id: p.clone(),
                    parent_key: row.parent_key.clone(),
                },
            }
        };
        let mut deferred = Vec::new();
        collect_deferred(&merged_source.config, &mut deferred);
        collect_deferred(&merged_sink.config, &mut deferred);

        // Resolved readiness status (#371): `merged_source.status` already
        // carries the template→row `source.status` scalar merge; default to
        // `active` when neither declared it.
        let status = merged_source.status.unwrap_or_default();

        // Effective tags (#376) = source-template `tags` ∪ row `tags`. This is
        // the one deliberate exception to `merge.rs`'s array-replace rule:
        // tags union rather than replace. Validate + dedup + sort so the set is
        // canonical and order-insensitive.
        let tags = resolve_tags(&merged_source.tags, &row.tags, row_id)?;

        // Resolve transforms, state, and DLQ (row overrides win over base).
        // Three-layer additive resolution:
        //   T_pipeline ++ T_source ++ T_row
        // gated on each layer's `inherit_transforms` flag.
        let src_inherit = merged_source.inherit_transforms;
        let row_inherit = row.inherit_transforms;
        let mut transforms: Vec<TransformSpec> = Vec::new();
        if src_inherit && row_inherit {
            transforms.extend(cfg.pipeline.transforms.iter().cloned());
        }
        if row_inherit && let Some(src_ts) = merged_source.transforms.as_ref() {
            transforms.extend(src_ts.iter().cloned());
        }
        if let Some(row_ts) = row.transforms.as_ref() {
            transforms.extend(row_ts.iter().cloned());
        }
        let state = row.state.clone().or_else(|| cfg.pipeline.state.clone());
        // Row override wins; fall back to the top-level delivery mode.
        let delivery = row.delivery.unwrap_or(cfg.delivery);
        // Three-state match: Some(None) = disable, Some(Some(spec)) = replace,
        // None = inherit. The naive `.flatten().or_else()` would conflate
        // disable and absent, silently inheriting on explicit null.
        let dlq = match row.dlq.clone() {
            Some(None) => None,
            Some(Some(spec)) => Some(spec),
            None => cfg.pipeline.dlq.clone(),
        };

        if let Some(ref d) = dlq {
            if matches!(d.max_failures_per_page, Some(0)) {
                return Err(CliError::InvalidDlqBudget {
                    field: "max_failures_per_page",
                });
            }
            if matches!(d.max_failures_total, Some(0)) {
                return Err(CliError::InvalidDlqBudget {
                    field: "max_failures_total",
                });
            }
            if !crate::registry::sink_exists(&d.sink.kind) {
                return Err(CliError::UnknownDlqSinkKind {
                    kind: d.sink.kind.clone(),
                    context: format!("row `{row_id}`"),
                });
            }
        }

        // A transform's config may reference `${now.*}` and `${<parent-row>.*}`
        // — the executor resolves both per invocation, exactly like source/sink
        // configs (#568) — so validate them like source/sink (`check_refs`:
        // `${now.*}` and known row ids pass, an unknown id fails) rather than
        // blanket-rejecting every runtime token. State / dlq configs have no
        // such runtime resolution, so they keep the stricter rejection below.
        for (ti, t) in transforms.iter().enumerate() {
            // Transforms never carry discovery recipes, so the naming tokens
            // stay hard errors here.
            check_refs(
                &t.config,
                &id_set,
                &format!("row `{row_id}` transform[{ti}] (`{}`)", t.kind),
                false,
                &HashSet::new(),
            )?;
        }
        if let Some(ref st) = state {
            reject_runtime_tokens(&st.config, &format!("row `{row_id}` state config"))?;
        }
        if let Some(ref d) = dlq {
            reject_runtime_tokens(&d.sink.config, &format!("row `{row_id}` dlq sink config"))?;
        }

        // `quality:` is pipeline-level only in v1 (no matrix-row override), so
        // every node carries the same spec. Compile it once per node to (a)
        // surface invalid paths/regexes/bounds at expand time, and (b) fail
        // fast when a quarantine check has no DLQ to route to — the core guards
        // this at run start too, but catching it here makes `faucet validate`
        // a friendly, fast failure.
        #[cfg(feature = "quality")]
        let quality = cfg.pipeline.quality.clone();
        #[cfg(feature = "quality")]
        if let Some(ref spec) = quality {
            let compiled = faucet_core::CompiledQuality::compile(spec)
                .map_err(|e| CliError::Config(format!("quality (row `{row_id}`): {e}")))?;
            if compiled.requires_dlq() && dlq.is_none() {
                return Err(CliError::Config(format!(
                    "row `{row_id}`: a quality check uses `on_failure: quarantine` \
                     but no DLQ is configured — add a `dlq:` block (or change the \
                     check's `on_failure` to `abort`)"
                )));
            }
        }

        // `contract:` is pipeline-level only in v1 (like `quality:`). Compile
        // it once per node so a malformed contract (bad regex, duplicate
        // fields, misplaced constraints) surfaces at expand time, and fail
        // fast when `on_breach: quarantine` has no DLQ to route to.
        #[cfg(feature = "contract")]
        let contract = cfg.pipeline.contract.clone();
        #[cfg(feature = "contract")]
        if let Some(ref spec) = contract {
            let compiled = faucet_core::CompiledContract::compile(spec)
                .map_err(|e| CliError::Config(format!("contract (row `{row_id}`): {e}")))?;
            if compiled.requires_dlq() && dlq.is_none() {
                return Err(CliError::Config(format!(
                    "row `{row_id}`: the contract uses `on_breach: quarantine` \
                     but no DLQ is configured — add a `dlq:` block (or change \
                     `on_breach` to `fail` or `warn`)"
                )));
            }
        }

        // `masking:` is pipeline-level only in v1 (like `quality:`/`contract:`).
        // Compile it once per node so a malformed policy (empty rules, empty
        // match, bad regex) surfaces at expand time. No DLQ gate — masking
        // never quarantines; it rewrites matching fields in place.
        #[cfg(feature = "masking")]
        let masking = cfg.pipeline.masking.clone();
        #[cfg(feature = "masking")]
        if let Some(ref spec) = masking {
            faucet_core::CompiledMasking::compile(spec)
                .map_err(|e| CliError::Config(format!("masking (row `{row_id}`): {e}")))?;
        }

        // Resilience poison-pill cross-check: `poison.action: dlq` routes
        // persistently-failing rows to the DLQ, so a DLQ must be configured.
        // Caught here so `faucet validate` reports it before any run starts.
        if let Some(spec) = &cfg.resilience
            && matches!(
                spec.poison.as_ref().map(|p| p.action),
                Some(crate::config::PoisonActionSpec::Dlq)
            )
            && dlq.is_none()
        {
            return Err(CliError::Config(format!(
                "row '{row_id}': resilience.poison.action=dlq requires a dlq: block"
            )));
        }

        // SLA gate (load-time, #202): validate the spec once per row and
        // require a `state:` block when staleness / volume-anomaly checks need
        // persisted history. `min_rows_per_run` alone is stateless and passes
        // without one.
        if let Some(sla) = row.sla.as_ref().or(cfg.sla.as_ref()) {
            sla.validate()
                .map_err(|e| CliError::Config(format!("sla: {e}")))?;
            if sla.needs_state() {
                match state.as_ref() {
                    None => {
                        return Err(CliError::Config(format!(
                            "row '{row_id}': sla.max_staleness_secs / sla.volume_anomaly \
                             need persisted run history — add a `state:` block \
                             (min_rows_per_run alone works without one)"
                        )));
                    }
                    Some(s) if s.kind == "memory" => {
                        tracing::warn!(
                            row = %row_id,
                            "sla: the `memory` state store resets on process exit — \
                             staleness/volume baselines only persist within a single \
                             `faucet schedule`/`serve` process; use `file`, `redis`, \
                             or `postgres` for one-shot runs"
                        );
                    }
                    Some(_) => {}
                }
            }
        }

        // Profiling gate (#708): validate the spec once per row and require a
        // `state:` block — the rolling baseline lives there. A memory store
        // only baselines within one process.
        if let Some(pf) = row.profiling.as_ref().or(cfg.profiling.as_ref()) {
            pf.validate()
                .map_err(|e| CliError::Config(format!("profiling: {e}")))?;
            match state.as_ref() {
                None => {
                    return Err(CliError::Config(format!(
                        "row '{row_id}': profiling: needs a `state:` block — the rolling                          baseline of column profiles is kept there"
                    )));
                }
                Some(s) if s.kind == "memory" => {
                    tracing::warn!(
                        row = %row_id,
                        "profiling: the `memory` state store resets on process exit — the                          profile baseline only persists within a single `faucet                          schedule`/`serve` process; use `file`, `redis`, or `postgres`                          for one-shot runs"
                    );
                }
                Some(_) => {}
            }
        }

        // Rollback gate (#706): an undoable run needs a durable state store
        // (the pre-run marker and bookmark live there), a sink that can undo
        // its own writes, and the run-id column the metadata decorator stamps.
        if let Some(rb) = cfg.rollback.as_ref().filter(|r| r.enabled) {
            rb.validate()
                .map_err(|e| CliError::Config(format!("rollback: {e}")))?;
            match state.as_ref() {
                None => {
                    return Err(CliError::Config(format!(
                        "row '{row_id}': rollback: needs a `state:` block — the pre-run marker \
                         (bookmark + watermark before the run) is kept there so the next run \
                         re-reads what a rollback undid"
                    )));
                }
                Some(s) if s.kind == "memory" => {
                    return Err(CliError::Config(format!(
                        "row '{row_id}': rollback: the `memory` state store resets on process \
                         exit, so a run could never be undone later — use `file`, `redis`, or \
                         `postgres`"
                    )));
                }
                Some(_) => {}
            }
            if !crate::rollback::sink_supports_rollback(&merged_sink.kind) {
                return Err(CliError::Config(format!(
                    "row '{row_id}': rollback: sink '{}' cannot undo a run (supported: {})",
                    merged_sink.kind,
                    crate::rollback::ROLLBACK_SINK_KINDS.join(", ")
                )));
            }
            if cfg.metadata_columns.as_ref().is_some_and(|m| !m.enabled) {
                return Err(CliError::Config(format!(
                    "row '{row_id}': rollback: needs the `run_id` metadata column, but \
                     `metadata_columns.enabled` is false"
                )));
            }
        }
        // Verify gate (#701): validate once per row; the key / destination are
        // resolved at verify time against the built sink.
        if let Some(v) = cfg.verify.as_ref() {
            v.validate()
                .map_err(|e| CliError::Config(format!("verify: {e}")))?;
        }

        // write_mode × sink validation (load-time): reject an unsupported mode
        // for the sink kind, and upsert/delete without a key, before any run.
        // Runs for every row; append rows pass trivially.
        let requested_mode = merged_sink
            .config
            .get("write_mode")
            .and_then(|v| v.as_str())
            .unwrap_or("append");
        let mode = match requested_mode {
            "append" => faucet_core::WriteMode::Append,
            "upsert" => faucet_core::WriteMode::Upsert,
            "delete" => faucet_core::WriteMode::Delete,
            "overwrite" => faucet_core::WriteMode::Overwrite,
            other => {
                return Err(CliError::Config(format!(
                    "row '{}': unknown write_mode '{}' (expected append, upsert, delete, or overwrite)",
                    ids[i], other
                )));
            }
        };
        if !crate::registry::sink_supported_write_modes(&merged_sink.kind).contains(&mode) {
            let sinks = if matches!(mode, faucet_core::WriteMode::Overwrite) {
                format!(
                    "overwrite sinks: {}",
                    crate::registry::OVERWRITE_SINK_KINDS.join(", ")
                )
            } else {
                format!(
                    "upsert/delete sinks: {}",
                    crate::registry::UPSERT_SINK_KINDS.join(", ")
                )
            };
            return Err(CliError::Config(format!(
                "row '{}': write_mode '{}' is not supported by sink '{}' ({})",
                ids[i], requested_mode, merged_sink.kind, sinks
            )));
        }
        if matches!(
            mode,
            faucet_core::WriteMode::Upsert | faucet_core::WriteMode::Delete
        ) {
            let key_present = merged_sink
                .config
                .get("key")
                .and_then(|v| v.as_array())
                .map(|a| !a.is_empty())
                .unwrap_or(false);
            if !key_present {
                return Err(CliError::Config(format!(
                    "row '{}': write_mode '{}' requires a non-empty `key`",
                    ids[i], requested_mode
                )));
            }
        }

        // ── Overwrite gates (load-time, #492) ───────────────────────────
        // Overwrite replaces the whole destination via an atomic begin/commit
        // staging swap. It has no per-page watermark, and its staging table is
        // a pre-run clone of the target — so it cannot compose with
        // exactly-once delivery or with an in-place schema evolution that
        // mutates the target mid-run. (Scoped cleanup is already rejected: it
        // requires `write_mode: upsert`.) Checked before the delivery-guarantee
        // derivation so an EO source + idempotent sink cannot slip overwrite
        // onto the atomic-watermark path.
        if matches!(mode, faucet_core::WriteMode::Overwrite) {
            if delivery == faucet_core::DeliveryMode::ExactlyOnce {
                return Err(CliError::Config(format!(
                    "row '{}': write_mode: overwrite is incompatible with delivery: exactly_once \
                     — a full-destination replace has no per-page watermark to resume from",
                    ids[i]
                )));
            }
            if let Some(ref sd) = cfg.pipeline.schema
                && faucet_core::SchemaDriftPolicy::compile(sd).on_drift
                    == faucet_core::OnDrift::Evolve
            {
                return Err(CliError::Config(format!(
                    "row '{}': write_mode: overwrite is incompatible with schema.on_drift: evolve \
                     — overwrite stages into a pre-run clone of the target, so evolving the \
                     target mid-run would leave the staged data a column short at swap time",
                    ids[i]
                )));
            }
        }

        // ── Scoped/windowed overwrite gate (#518) ───────────────────────────
        // A `scope:` block replaces only the rows matching it (a date window)
        // instead of truncating. Valid only with `write_mode: overwrite` on a
        // sink that implements the scoped begin/delete/insert swap.
        if let Some(scope_val) = merged_sink.config.get("scope") {
            if !matches!(mode, faucet_core::WriteMode::Overwrite) {
                return Err(CliError::Config(format!(
                    "row '{}': `scope` is only valid with `write_mode: overwrite`",
                    ids[i]
                )));
            }
            if !crate::registry::sink_supports_scoped_overwrite(&merged_sink.kind) {
                return Err(CliError::Config(format!(
                    "row '{}': scoped overwrite (`scope`) is not supported by sink '{}' \
                     (scoped-overwrite sinks: {})",
                    ids[i],
                    merged_sink.kind,
                    crate::registry::SCOPED_OVERWRITE_SINK_KINDS.join(", ")
                )));
            }
            let scope: faucet_core::OverwriteScope = serde_json::from_value(scope_val.clone())
                .map_err(|e| CliError::Config(format!("row '{}': invalid `scope`: {e}", ids[i])))?;
            scope
                .validate()
                .map_err(|e| CliError::Config(format!("row '{}': {e}", ids[i])))?;
        }

        // Derived end-to-end delivery guarantee (issue #292): computed for
        // *every* row — regardless of the requested `delivery:` mode — so
        // `faucet validate` / `doctor` report the truth (a keyed-upsert row is
        // effectively-once even when the user did not request `exactly_once`).
        // `keyed_upsert_configured` relies on the write_mode gate above: after
        // it, an upsert/delete mode implies a non-empty `key`.
        let keyed_upsert_configured = matches!(
            mode,
            faucet_core::WriteMode::Upsert | faucet_core::WriteMode::Delete
        );
        let guarantee_inputs = faucet_core::GuaranteeInputs {
            replay: crate::registry::source_replay_guarantee(&merged_source.kind),
            sink_atomic: crate::registry::sink_supports_idempotent_writes(&merged_sink.kind),
            keyed_upsert_configured,
            durable_state: matches!(state.as_ref(), Some(s) if s.kind != "memory"),
            dlq: dlq.is_some(),
        };
        let delivery_guarantee = faucet_core::derive_delivery_guarantee(&guarantee_inputs);

        // Exactly-once delivery gate: `delivery: exactly_once` means "require
        // ≥ effectively-once". Enforced at config-load time so `faucet
        // validate` catches an unsupported topology before any run starts,
        // with the error naming the limiting side. A derived `AtLeastOnce`
        // implies keyed dedup is not configured (the keyed mechanism has no
        // other requirement), so the cascade below walks the atomic-watermark
        // requirements in order.
        if delivery == faucet_core::DeliveryMode::ExactlyOnce
            && delivery_guarantee == faucet_core::DeliveryGuarantee::AtLeastOnce
        {
            if !crate::registry::source_supports_exactly_once(&merged_source.kind) {
                let keyed_hint = if crate::registry::UPSERT_SINK_KINDS.contains(&&*merged_sink.kind)
                {
                    format!(
                        ", or configure `write_mode: upsert` + `key` on sink '{}' for \
                         keyed-upsert effectively-once with any source",
                        merged_sink.kind
                    )
                } else {
                    String::new()
                };
                return Err(CliError::Config(format!(
                    "row '{}': delivery: exactly_once is not supported by source '{}' \
                     (deterministic-replay sources only: {}{})",
                    ids[i],
                    merged_source.kind,
                    crate::registry::EXACTLY_ONCE_SOURCE_KINDS.join(", "),
                    keyed_hint
                )));
            }
            if !crate::registry::sink_supports_idempotent_writes(&merged_sink.kind) {
                let keyed_hint = if crate::registry::UPSERT_SINK_KINDS.contains(&&*merged_sink.kind)
                {
                    format!(
                        "; alternatively configure `write_mode: upsert` + `key` on '{}' for \
                         keyed-upsert effectively-once",
                        merged_sink.kind
                    )
                } else {
                    String::new()
                };
                return Err(CliError::Config(format!(
                    "row '{}': delivery: exactly_once is not supported by sink '{}' \
                     (idempotent sinks only: {}{})",
                    ids[i],
                    merged_sink.kind,
                    crate::registry::IDEMPOTENT_SINK_KINDS.join(", "),
                    keyed_hint
                )));
            }
            // Require a *durable* state store. The atomic-watermark mechanism
            // persists the monotonic page sequence alongside the bookmark
            // (`wrap_state(bookmark, seq)`) and resumes from it across
            // restarts; the in-process `memory` store loses that watermark on
            // exit, so a restart would re-run already-committed pages — exactly
            // the duplication exactly-once exists to prevent (F24). Mirror the
            // `faucet replicate` gate, which already rejects `memory`.
            match state.as_ref() {
                None => {
                    return Err(CliError::Config(format!(
                        "row '{}': delivery: exactly_once requires a state store",
                        ids[i]
                    )));
                }
                Some(s) if s.kind == "memory" => {
                    return Err(CliError::Config(format!(
                        "row '{}': delivery: exactly_once requires a durable state store, \
                         not `memory` — the cross-restart watermark/sequence guarantee \
                         depends on it (use `file`, `redis`, or `postgres`)",
                        ids[i]
                    )));
                }
                Some(_) => {}
            }
            if dlq.is_some() {
                return Err(CliError::Config(format!(
                    "row '{}': delivery: exactly_once is not compatible with a DLQ in this version",
                    ids[i]
                )));
            }
            // The cascade above covers every way the derivation can land on
            // at-least-once; reaching here would mean it diverged from the
            // checks.
            unreachable!("delivery-guarantee derivation and the exactly-once gate diverged");
        }

        // ── Scoped-cleanup gates (load-time, #478) ──────────────────────
        // Cleanup DELETES data, so every precondition is checked before a run
        // starts rather than discovered mid-flight.
        if merged_sink.complete_for.is_some() {
            return Err(CliError::Config(format!(
                "row '{}': `complete_for` belongs on the source, not the sink — only the \
                 source can claim a fetch returned every record for a scope",
                ids[i]
            )));
        }
        let cleanup_scope = match merged_source.complete_for.as_ref() {
            None => None,
            Some(claim) if claim.on_missing == crate::config::OnMissing::Ignore => {
                // A claim with no action is inert by design — it documents the
                // scope without authorising a delete.
                None
            }
            Some(claim) => {
                if claim.scope.is_empty() {
                    return Err(CliError::Config(format!(
                        "row '{}': `complete_for.scope` is empty — an empty scope matches \
                         every row in the destination",
                        ids[i]
                    )));
                }
                if !crate::registry::sink_supports_cleanup(&merged_sink.kind) {
                    return Err(CliError::Config(format!(
                        "row '{}': `complete_for.on_missing: delete` is not supported by sink \
                         '{}' (cleanup-capable sinks: {})",
                        ids[i],
                        merged_sink.kind,
                        crate::registry::CLEANUP_SINK_KINDS.join(", ")
                    )));
                }
                if !matches!(mode, faucet_core::WriteMode::Upsert) {
                    return Err(CliError::Config(format!(
                        "row '{}': `complete_for.on_missing: delete` requires \
                         `write_mode: upsert` (got '{}') — on an append sink there is no key \
                         to tell a written row from a stale one",
                        ids[i], requested_mode
                    )));
                }
                // Cleanup is a second, non-idempotent write outside the
                // commit-token transaction, so it cannot compose with the
                // atomic-watermark path.
                if matches!(delivery, faucet_core::DeliveryMode::ExactlyOnce) {
                    return Err(CliError::Config(format!(
                        "row '{}': `complete_for.on_missing: delete` is incompatible with \
                         `delivery: exactly_once` — the scoped delete happens outside the \
                         commit-token transaction, so it cannot be replayed idempotently",
                        ids[i]
                    )));
                }
                // A quarantined record never reaches the sink, so the cleanup
                // tracker never sees its key — and the delete would then remove
                // its destination row, losing data the source still has. Reject
                // rather than silently delete.
                let mut quarantines: Vec<&str> = Vec::new();
                #[cfg(feature = "quality")]
                if let Some(q) = cfg.pipeline.quality.as_ref()
                    && faucet_core::CompiledQuality::compile(q)
                        .map(|c| c.requires_dlq())
                        .unwrap_or(false)
                {
                    quarantines.push("quality");
                }
                #[cfg(feature = "contract")]
                if let Some(c) = cfg.pipeline.contract.as_ref()
                    && faucet_core::CompiledContract::compile(c)
                        .map(|c| c.requires_dlq())
                        .unwrap_or(false)
                {
                    quarantines.push("contract");
                }
                if let Some(sd) = cfg.pipeline.schema.as_ref()
                    && faucet_core::SchemaDriftPolicy::compile(sd).requires_dlq()
                {
                    quarantines.push("schema");
                }
                if !quarantines.is_empty() {
                    return Err(CliError::Config(format!(
                        "row '{}': `complete_for.on_missing: delete` is incompatible with a \
                         quarantining `{}` policy — a quarantined record never reaches the \
                         sink, so cleanup cannot tell it from a record deleted at the source \
                         and would delete its destination row",
                        ids[i],
                        quarantines.join("`/`")
                    )));
                }
                Some(claim.scope.clone())
            }
        };

        // Schema-drift policy gates (load-time):
        //  - `evolve` requires an evolution-capable sink.
        //  - `quarantine` (drift or incompatible) requires a DLQ, and is
        //    incompatible with exactly-once (which forbids a DLQ).
        if let Some(ref sd) = cfg.pipeline.schema {
            let policy = faucet_core::SchemaDriftPolicy::compile(sd);
            if policy.on_drift == faucet_core::OnDrift::Evolve
                && !crate::registry::sink_supports_schema_evolution(&merged_sink.kind)
            {
                return Err(CliError::Config(format!(
                    "row '{}': schema.on_drift: evolve is not supported by sink '{}' \
                     (evolvable sinks: postgres, mysql, mssql, sqlite, bigquery, elasticsearch)",
                    ids[i], merged_sink.kind
                )));
            }
            if policy.requires_dlq() && dlq.is_none() {
                return Err(CliError::Config(format!(
                    "row '{}': schema.on_drift/on_incompatible 'quarantine' requires a `dlq:` block",
                    ids[i]
                )));
            }
            if policy.requires_dlq() && delivery == faucet_core::DeliveryMode::ExactlyOnce {
                return Err(CliError::Config(format!(
                    "row '{}': schema quarantine is incompatible with delivery: exactly_once \
                     (exactly_once forbids a DLQ)",
                    ids[i]
                )));
            }
        }

        // ── Range partitioning (#479) ───────────────────────────────────
        // A partitioned row becomes N nodes, one per chunk, each with the
        // chunk's `${partition.*}` tokens already substituted into its connector
        // configs. Everything downstream — the executor, state keys, the
        // concurrency semaphore — then treats them as ordinary sibling rows,
        // which is why the fan-out costs no executor changes.
        let partition_spec = row.partition.clone().or_else(|| {
            // The top-level block is a default for *root* rows only. A child
            // fans out per parent record already; combining both would multiply
            // the two fan-outs, which is never what a top-level default meant.
            matches!(role, NodeRole::Root)
                .then(|| cfg.partition.clone())
                .flatten()
        });
        let chunks = match partition_spec.as_ref() {
            None => Vec::new(),
            Some(spec) => {
                // A partitioned row's id gains a chunk suffix, so any row that
                // names it as `parent:` or in `depends_on:` would resolve to a
                // node that no longer exists. Reject rather than silently
                // dropping the dependent's edge.
                let me = ids[i].as_str();
                let dependents: Vec<&str> = rows
                    .iter()
                    .enumerate()
                    .filter(|(j, r)| {
                        *j != i
                            && (r.parent.as_deref() == Some(me)
                                || r.depends_on.iter().any(|d| d == me))
                    })
                    .map(|(j, _)| ids[j].as_str())
                    .collect();
                if !dependents.is_empty() {
                    return Err(CliError::Config(format!(
                        "row '{}': a partitioned row cannot be referenced by another row \
                         (`parent:` or `depends_on:`) — it expands into one node per chunk, \
                         so there is no single node for '{}' to attach to. Partition the \
                         dependent row instead, or drop the reference",
                        me,
                        dependents.join("', '")
                    )));
                }
                let serialized = merged_source.config.to_string();
                if !crate::partition::references_partition(&serialized) {
                    return Err(CliError::Config(format!(
                        "row '{}': a `partition:` block is set but the source config references \
                         no `${{partition.*}}` token — every chunk would run the identical \
                         query. Scope the source to the chunk (e.g. \
                         `?id_from=${{partition.start}}&id_to=${{partition.end}}`). Available \
                         tokens for kind `{}`: {}",
                        ids[i],
                        spec.kind_str(),
                        spec.token_names().join(", ")
                    )));
                }
                crate::partition::plan(spec)
                    .map_err(|e| CliError::Config(format!("row '{}': {e}", ids[i])))?
            }
        };
        if chunks.len() >= crate::chunking::WARN_UNITS {
            tracing::warn!(
                row = %ids[i],
                chunks = chunks.len(),
                "this row plans a very large number of partitions; each is a full pipeline \
                 invocation with its own connector clients"
            );
        }

        let base = ExpandedNode {
            id: ids[i].clone(),
            row_index: i,
            weight: row.weight,
            role,
            source: merged_source,
            sink: merged_sink,
            transforms,
            state,
            dlq,
            sla: row.sla.clone(),
            profiling: row.profiling.clone().or_else(|| cfg.profiling.clone()),
            #[cfg(feature = "policy")]
            policy: cfg.policy.clone(),
            delivery,
            delivery_guarantee,
            #[cfg(feature = "quality")]
            quality,
            #[cfg(feature = "contract")]
            contract,
            #[cfg(feature = "masking")]
            masking,
            sink_ref,
            schema: cfg.pipeline.schema.clone(),
            depends_on: deps_by_row[i].clone(),
            status,
            tags,
            deferred_refs: deferred,
            source_override: None,
            cleanup_scope,
            metadata_columns: effective_metadata_columns(cfg),
            #[cfg(feature = "catalog")]
            local_outputs: cfg.local_outputs.clone(),
        };

        if chunks.is_empty() {
            out.push(base);
        } else {
            // One node per chunk. The id carries the chunk suffix so state keys
            // (`{name}::{row}::partition::{chunk}`) and log lines stay distinct,
            // and `row_index` is kept identical so partitions of one row sort
            // together ahead of the next row.
            for chunk in &chunks {
                let mut n = base.clone();
                n.id = format!("{}::partition::{}", base.id, chunk.id);
                crate::partition::substitute(&mut n.source.config, chunk)
                    .map_err(|e| CliError::Config(format!("row '{}': {e}", ids[i])))?;
                crate::partition::substitute(&mut n.sink.config, chunk)
                    .map_err(|e| CliError::Config(format!("row '{}': {e}", ids[i])))?;
                out.push(n);
            }
        }
    }
    Ok(out)
}

fn detect_cycle(parents: &HashMap<&str, &str>) -> CliResult<()> {
    // Each node has at most one parent ⇒ cycle detection is "walk parents
    // until we hit `None` or revisit a node we've already seen this walk".
    for &start in parents.keys() {
        let mut visited: BTreeSet<&str> = BTreeSet::new();
        let mut cur = start;
        while let Some(&p) = parents.get(cur) {
            if !visited.insert(cur) {
                let chain: Vec<String> = visited.iter().map(|s| (*s).to_string()).collect();
                return Err(CliError::ParentCycle { ids: chain });
            }
            cur = p;
            if cur == start {
                let mut chain: Vec<String> = visited.iter().map(|s| (*s).to_string()).collect();
                chain.push(start.to_string());
                return Err(CliError::ParentCycle { ids: chain });
            }
        }
    }
    Ok(())
}

/// Kahn's algorithm over the combined `parent:` + `depends_on:` edge set.
/// Pure-parent cycles are already caught by [`detect_cycle`] (with its more
/// specific error), so any leftover here necessarily involves a `depends_on`
/// edge. Rows that cannot be topologically ordered are the cycle participants
/// (plus any rows downstream of them — still actionable, since the report
/// names every row that would never become ready).
fn detect_combined_cycle(
    ids: &[String],
    parents: &HashMap<&str, &str>,
    deps_by_row: &[Vec<String>],
) -> CliResult<()> {
    let index_of: HashMap<&str, usize> = ids
        .iter()
        .enumerate()
        .map(|(i, id)| (id.as_str(), i))
        .collect();
    let mut in_degree = vec![0usize; ids.len()];
    let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); ids.len()];
    for (i, id) in ids.iter().enumerate() {
        let mut prereqs: Vec<usize> = Vec::new();
        if let Some(p) = parents.get(id.as_str()) {
            prereqs.push(index_of[p]);
        }
        prereqs.extend(deps_by_row[i].iter().map(|d| index_of[d.as_str()]));
        for p in prereqs {
            in_degree[i] += 1;
            dependents[p].push(i);
        }
    }
    let mut queue: std::collections::VecDeque<usize> =
        (0..ids.len()).filter(|&i| in_degree[i] == 0).collect();
    let mut processed = 0usize;
    while let Some(i) = queue.pop_front() {
        processed += 1;
        for &d in &dependents[i] {
            in_degree[d] -= 1;
            if in_degree[d] == 0 {
                queue.push_back(d);
            }
        }
    }
    if processed < ids.len() {
        let mut stuck: Vec<String> = (0..ids.len())
            .filter(|&i| in_degree[i] > 0)
            .map(|i| ids[i].clone())
            .collect();
        stuck.sort();
        return Err(CliError::DependencyCycle { ids: stuck });
    }
    Ok(())
}

/// Verify that every `${X.path}` token in `value` has `X` listed in `id_set`.
/// Load-time prefixes (`env`, `file`, `secret`) were already handled and are
/// ignored here.
/// Collect the capture names declared by every `type: flow` provider in the
/// top-level `auth:` catalog — the `capture` keys of each login step plus each
/// `apply[].name`. These become valid `${name}` deferred tokens so a source
/// body/header can reference a captured value the connector substitutes per
/// request (#567).
fn collect_flow_capture_names(cfg: &PipelineConfig) -> Vec<String> {
    let mut names = Vec::new();
    let Some(auth) = &cfg.auth else {
        return names;
    };
    for provider in auth.values() {
        if provider.get("type").and_then(Value::as_str) != Some("flow") {
            continue;
        }
        let Some(config) = provider.get("config") else {
            continue;
        };
        if let Some(steps) = config.get("steps").and_then(Value::as_array) {
            for step in steps {
                if let Some(cap) = step.get("capture").and_then(Value::as_object) {
                    names.extend(cap.keys().cloned());
                }
            }
        }
        if let Some(apply) = config.get("apply").and_then(Value::as_array) {
            for a in apply {
                if let Some(n) = a.get("name").and_then(Value::as_str) {
                    names.push(n.to_string());
                }
            }
        }
    }
    names
}

/// Whether a source config carries a discovery recipe whose naming-template
/// tokens (`${name}` / `${name_snake}` / `${name_lower}` / `${field_names}`)
/// are resolved by the source's discovery engine at `discover()` time.
///
/// Recognised by *shape*, not by block name (#654 M22): an explicit
/// `discovery:` recipe, or any connector-config block using the fan-out / emit
/// vocabulary. Naming one protocol's block here made a third discovery-capable
/// source silently fail this check.
fn has_discovery_recipe(config: &Value) -> bool {
    if config.get("discovery").is_some() {
        return true;
    }
    config.as_object().is_some_and(|cfg| {
        cfg.values().filter_map(Value::as_object).any(|o| {
            o.contains_key("fan_out") || o.contains_key("emit") || o.contains_key("objects")
        })
    })
}

/// Bare `${name}` placeholders that connectors resolve themselves, so they are
/// legal in a source/sink config: the XML source's `BodyCursor` pagination
/// substitutes `${next_token}` into its request template, and a `type: flow`
/// auth provider exposes every `capture`d value (plus the signing context
/// `${sig}` / `${ts}` / `${nonce}`) to the connector that references it.
/// Only names the config actually declares are allowed, so a typo'd
/// `${vars.name}` stays a hard error.
fn connector_placeholders(cfg: &PipelineConfig) -> HashSet<String> {
    let mut out: HashSet<String> = HashSet::new();
    out.insert("next_token".into());
    for provider in cfg.auth.iter().flat_map(|m| m.values()) {
        if provider.get("type").and_then(Value::as_str) != Some("flow") {
            continue;
        }
        for name in ["sig", "ts", "nonce"] {
            out.insert(name.into());
        }
        let steps = provider
            .pointer("/config/steps")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for step in steps {
            if let Some(cap) = step.get("capture").and_then(Value::as_object) {
                out.extend(cap.keys().cloned());
            }
        }
    }
    out
}

fn check_refs(
    value: &Value,
    id_set: &HashSet<&str>,
    owner: &str,
    allow_recipe_tokens: bool,
    connector_tokens: &HashSet<String>,
) -> CliResult<()> {
    walk_strings(value, &mut |s| {
        for (token, dir) in iter_directives(s) {
            // Load-time / template directives (`${env:..}`, `${vars.X}`, …) are
            // resolved before expansion; only deferred `${id.path}` references
            // are validated here, against the known row ids.
            // `now` and `backfill` are reserved built-in deferred ids
            // resolved at run time (`backfill` by `faucet backfill`, #282).
            // `${param.*}` is bound *pre-parse* (`params::bind_document`), so a
            // token surviving to expansion means the config was built through a
            // path that skipped binding — e.g. a host calling
            // `PipelineConfig::from_text`/`from_value` directly. Name the cause
            // rather than reporting a generic unknown row id (#444).
            if let Directive::Deferred { id, .. } = dir
                && id == crate::params::PARAM_ID
            {
                return Err(CliError::Config(format!(
                    "interpolation token `{token}` (in {owner}) was never bound — a `${{param.*}}` \
                     reference is resolved when the run is triggered. Load the config through \
                     `PipelineConfig::from_path*` (or supply values with `--param`) so params are \
                     bound before expansion"
                )));
            }
            if let Directive::Deferred { id, path } = dir
                && id != "now"
                && id != "backfill"
                && id != crate::tenant_tokens::TENANT_ID
                && id != "partition"
                && id != "bookmark"
                && id != "job_id"
                && id != "window"
                // Discovery-recipe naming tokens, resolved by the source's
                // discovery engine at `discover()` time (before the executor
                // sees them) — allowed only when the config actually carries a
                // recipe, so a bare `${name}` in an ordinary config stays a
                // hard error (it is almost always a typo'd `${vars.name}`).
                && !(allow_recipe_tokens
                    && matches!(id, "name" | "name_snake" | "name_lower" | "field_names"))
                && !(path.is_empty() && connector_tokens.contains(id))
                && !id_set.contains(id)
            {
                return Err(CliError::UnknownInterpolationId {
                    id: id.to_owned(),
                    token: format!("{token} (in {owner})"),
                });
            }
        }
        Ok(())
    })
}

/// Reject any runtime interpolation token (`${id.path}` parent-record refs and
/// `${now.*}`) found in `value`. These resolve **only** in source/sink configs;
/// elsewhere — transform / state / dlq bodies — they would silently reach the
/// connector as a literal `${...}` string (#146 M2). Load-time directives
/// (`${env:}`, `${vars.X}`, `${sources.X}`, …) are already resolved before
/// expansion, so any deferred token still present here is genuinely
/// unsupported in this location.
fn reject_runtime_tokens(value: &Value, location: &str) -> CliResult<()> {
    walk_strings(value, &mut |s| {
        for (token, dir) in iter_directives(s) {
            if let Directive::Deferred { .. } = dir {
                return Err(CliError::Config(format!(
                    "interpolation token `{token}` in {location} is not supported: \
                     `${{...}}` runtime tokens (parent-record references and `${{now.*}}`) \
                     resolve only in source/sink configs"
                )));
            }
        }
        Ok(())
    })
}

/// Compute a row's effective tag set = `template_tags` ∪ `row_tags` (#376).
/// Union — the deliberate exception to `merge.rs`'s array-replace rule.
/// Validates each tag (charset `^[a-z0-9][a-z0-9_-]*$`, non-empty), dedups, and
/// returns a sorted, canonical list so `--tag` matching is order-insensitive.
fn resolve_tags(
    template_tags: &[String],
    row_tags: &[String],
    row_id: &str,
) -> CliResult<Vec<String>> {
    let mut set: BTreeSet<String> = BTreeSet::new();
    for tag in template_tags.iter().chain(row_tags.iter()) {
        validate_tag(tag, row_id)?;
        set.insert(tag.clone());
    }
    Ok(set.into_iter().collect())
}

/// True when `s` matches `^[a-z0-9][a-z0-9_-]*$` (a discovery alias, #501).
fn is_ident(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_lowercase() || c.is_ascii_digit() => {
            chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
        }
        _ => false,
    }
}

/// A tag must be lowercase kebab/snake: `^[a-z0-9][a-z0-9_-]*$`.
fn validate_tag(tag: &str, row_id: &str) -> CliResult<()> {
    let ok = {
        let mut chars = tag.chars();
        match chars.next() {
            Some(c) if c.is_ascii_lowercase() || c.is_ascii_digit() => {
                chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
            }
            _ => false,
        }
    };
    if !ok {
        return Err(CliError::Config(format!(
            "row '{row_id}': invalid tag '{tag}' — tags must match ^[a-z0-9][a-z0-9_-]*$ \
             (lowercase letters, digits, `_`, `-`; first char alphanumeric)"
        )));
    }
    Ok(())
}

fn collect_deferred(value: &Value, out: &mut Vec<DeferredRef>) {
    let _ = walk_strings(value, &mut |s| {
        for (token, dir) in iter_directives(s) {
            if let Directive::Deferred { id, path } = dir {
                // `now` / `backfill` / `partition` are reserved built-ins
                // resolved at run time, not parent-record dependencies — skip
                // them so the executor doesn't treat them as deferred
                // parent-record refs. `bookmark` is consumed *inside* a source's
                // `replication_bind.template` (#513), `window` inside a source's
                // `window.{lower,upper}.template` (#527) — the connector renders
                // them, so the CLI must pass them through untouched.
                if id == "now"
                    || id == "backfill"
                    || id == crate::tenant_tokens::TENANT_ID
                    || id == "partition"
                    || id == "bookmark"
                    || id == "job_id"
                    || id == "window"
                {
                    continue;
                }
                out.push(DeferredRef {
                    referenced_id: id.to_owned(),
                    dotted_path: path.to_owned(),
                    token: token.to_owned(),
                });
            }
        }
        Ok(())
    });
}

fn walk_strings<F>(value: &Value, f: &mut F) -> CliResult<()>
where
    F: FnMut(&str) -> CliResult<()>,
{
    match value {
        Value::String(s) => f(s),
        Value::Array(a) => a.iter().try_for_each(|v| walk_strings(v, f)),
        Value::Object(m) => m.values().try_for_each(|v| walk_strings(v, f)),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{OnBatchErrorSpec, parse_with_extension};

    fn cfg(yaml: &str) -> PipelineConfig {
        parse_with_extension(yaml, "yaml").unwrap()
    }

    /// #679: a matrix row's `sla:` replaces the top-level one for that row —
    /// it rides on the node, and the load-time state gate applies to it.
    #[test]
    fn a_row_sla_overrides_the_top_level_one() {
        let yaml = |state: &str| {
            format!(
                r#"
version: 1
name: p
sla: {{ min_rows_per_run: 5 }}
pipeline:
  source: {{ type: csv, config: {{ path: in.csv }} }}
  sink: {{ type: stdout, config: {{}} }}
{state}
matrix:
  - id: a
    sla: {{ max_staleness_secs: 60 }}
  - id: b
"#
            )
        };
        let nodes = expand(&cfg(&yaml("  state: { type: memory }"))).expect("expands");
        let a = nodes.iter().find(|n| n.id == "a").unwrap();
        let b = nodes.iter().find(|n| n.id == "b").unwrap();
        assert_eq!(a.sla.as_ref().and_then(|s| s.max_staleness_secs), Some(60));
        assert!(b.sla.is_none(), "b inherits the top-level sla at run time");
        let err = expand(&cfg(&yaml(""))).unwrap_err().to_string();
        assert!(err.contains("row 'a'") && err.contains("state:"), "{err}");
    }

    /// Bare `${name}` tokens are connector placeholders only when the config
    /// declares them: `${next_token}` (XML BodyCursor) always, a `type: flow`
    /// auth provider's captures + signing context when such a provider exists.
    /// Anything else stays the typo'd-`${vars.name}` hard error.
    #[test]
    fn declared_connector_placeholders_pass_ref_validation() {
        let with_flow = |token: &str| {
            cfg(&format!(
                r#"
version: 1
auth:
  login:
    type: flow
    config:
      steps:
        - request: {{ method: POST, url: "https://x/login" }}
          capture: {{ session: "$.Session" }}
pipeline:
  source:
    type: rest
    config: {{ base_url: "https://x", path: /v1, headers: {{ Authorization: "Bearer {token}" }} }}
  sink: {{ type: stdout, config: {{}} }}
"#
            ))
        };
        expand(&with_flow("${session}")).expect("captured name is a legal placeholder");
        expand(&with_flow("${sig}-${ts}")).expect("signing context is legal with a flow");
        let err = expand(&with_flow("${sessoin}")).unwrap_err();
        assert!(
            matches!(err, CliError::UnknownInterpolationId { ref id, .. } if id == "sessoin"),
            "{err}"
        );

        let plain = |token: &str| {
            cfg(&format!(
                r#"
version: 1
pipeline:
  source:
    type: xml
    config: {{ endpoint: "https://x/api", body: "<q>{token}</q>", records_path: r }}
  sink: {{ type: stdout, config: {{}} }}
"#
            ))
        };
        expand(&plain("${next_token}")).expect("the XML BodyCursor token is always legal");
        let err = expand(&plain("${session}")).unwrap_err();
        assert!(
            matches!(err, CliError::UnknownInterpolationId { ref id, .. } if id == "session"),
            "without a flow, a capture-looking token is still an error: {err}"
        );
        assert!(connector_placeholders(&plain("x")).contains("next_token"));
        let names = connector_placeholders(&with_flow("x"));
        for n in ["next_token", "session", "sig", "ts", "nonce"] {
            assert!(names.contains(n), "{n}");
        }
    }

    #[test]
    fn implicit_single_row_when_matrix_absent() {
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: jsonl, config: { path: ./o } }
"#);
        let nodes = expand(&c).unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].id, "row-0");
        assert!(matches!(nodes[0].role, NodeRole::Root));
        assert_eq!(nodes[0].source.kind, "rest");
        assert_eq!(nodes[0].sink.kind, "jsonl");
    }

    #[test]
    fn rejects_runtime_token_in_dlq_config() {
        // M2 (#146): `${now.*}` / `${parent.path}` resolve only in source/sink
        // configs. In a dlq config they would silently pass through as a literal
        // `${...}` string — expand must reject them with a clear error.
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: jsonl, config: { path: ./o } }
  dlq:
    sink: { type: jsonl, config: { path: "dead-${now.date}.jsonl" } }
"#);
        let err = expand(&c).unwrap_err();
        assert!(
            matches!(&err, CliError::Config(m) if m.contains("now.date") && m.contains("dlq")),
            "got: {err:?}"
        );
    }

    #[test]
    fn rejects_runtime_token_in_state_config() {
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: jsonl, config: { path: ./o } }
  state:
    type: file
    config: { path: "state-${now.date}" }
"#);
        let err = expand(&c).unwrap_err();
        assert!(
            matches!(&err, CliError::Config(m) if m.contains("state")),
            "got: {err:?}"
        );
    }

    #[test]
    fn allows_now_token_in_transform_config() {
        // `${now.*}` in a transform is resolved per invocation by the executor
        // (#568), so expand must accept it — like source/sink configs.
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: jsonl, config: { path: ./o } }
  transforms:
    - type: set
      config: { values: { ts: "${now.datetime}" } }
"#);
        assert_eq!(expand(&c).unwrap().len(), 1);
    }

    #[test]
    fn allows_reserved_builtin_tokens_in_transform_config() {
        // Reserved runtime built-ins (`${now.*}`, `${window.*}`, `${backfill.*}`,
        // `${bookmark}`, `${job_id}`, `${partition.*}`) are accepted in a
        // transform's config, exercising the check_refs guard chain (#568).
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: jsonl, config: { path: ./o } }
  transforms:
    - type: set
      config: { values: { a: "${now.date}", b: "${window.from}", c: "${backfill.start}", d: "${bookmark}", e: "${job_id}", f: "${partition.id}" } }
"#);
        assert_eq!(expand(&c).unwrap().len(), 1);
    }

    #[test]
    fn rejects_unknown_id_token_in_transform_config() {
        // An unknown id (not `now`/`backfill`/… and not a declared row) in a
        // transform still fails — it would leak as a literal at runtime.
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: jsonl, config: { path: ./o } }
  transforms:
    - type: set
      config: { values: { who: "${nobody.name}" } }
"#);
        let err = expand(&c).unwrap_err();
        assert!(
            matches!(&err, CliError::UnknownInterpolationId { id, .. } if id == "nobody"),
            "got: {err:?}"
        );
    }

    #[test]
    fn allows_flow_capture_token_in_source_config() {
        // A `type: flow` provider captures `session_id`; a source body may then
        // reference `${session_id}`, substituted per request by the connector
        // (#567). Expand must accept the token rather than rejecting it.
        let c = cfg(r#"
version: 1
auth:
  intacct:
    type: flow
    config:
      steps:
        - request: { url: "https://x/login", method: POST }
          capture: { session_id: "$.sessionid" }
      apply: []
pipeline:
  source:
    type: xml
    config:
      base_url: "https://x"
      path: /gw
      body: "<r><sessionid>${session_id}</sessionid></r>"
      auth: { ref: intacct }
  sink: { type: jsonl, config: { path: ./o } }
"#);
        assert_eq!(expand(&c).unwrap().len(), 1);
    }

    #[test]
    fn rejects_capture_token_without_a_declaring_flow_provider() {
        // The same token with no flow provider declaring it is still an unknown
        // id — the allowance is scoped to declared captures.
        let c = cfg(r#"
version: 1
pipeline:
  source:
    type: xml
    config:
      base_url: "https://x"
      path: /gw
      body: "<r><sessionid>${session_id}</sessionid></r>"
  sink: { type: jsonl, config: { path: ./o } }
"#);
        let err = expand(&c).unwrap_err();
        assert!(
            matches!(&err, CliError::UnknownInterpolationId { id, .. } if id == "session_id"),
            "got: {err:?}"
        );
    }

    #[test]
    fn allows_runtime_token_in_source_and_sink_configs() {
        // The same tokens remain valid in source/sink configs (regression guard
        // that M2's rejection didn't over-reach).
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: "https://x?d=${now.date}" } }
  sink:   { type: jsonl, config: { path: "out-${now.date}.jsonl" } }
"#);
        let nodes = expand(&c).unwrap();
        assert_eq!(nodes.len(), 1);
    }

    #[test]
    fn merges_row_overrides_into_pipeline_source() {
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x, headers: { a: 1 } } }
  sink:   { type: jsonl, config: { path: ./o } }
matrix:
  - id: users
    source: { config: { path: /v1/users, headers: { b: 2 } } }
"#);
        let nodes = expand(&c).unwrap();
        assert_eq!(nodes[0].id, "users");
        assert_eq!(nodes[0].source.config["base_url"], "https://x");
        assert_eq!(nodes[0].source.config["path"], "/v1/users");
        assert_eq!(nodes[0].source.config["headers"]["a"], 1);
        assert_eq!(nodes[0].source.config["headers"]["b"], 2);
    }

    #[test]
    fn errors_on_unknown_parent() {
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o } }
matrix:
  - id: child
    parent: nobody
"#);
        assert!(matches!(
            expand(&c).unwrap_err(),
            CliError::UnknownParent { .. }
        ));
    }

    #[test]
    fn errors_on_duplicate_ids() {
        let c = cfg(r#"
version: 1
pipeline: { source: { type: rest, config: {} }, sink: { type: jsonl, config: { path: ./o } } }
matrix:
  - { id: x }
  - { id: x }
"#);
        assert!(matches!(
            expand(&c).unwrap_err(),
            CliError::DuplicateRowId { .. }
        ));
    }

    #[test]
    fn errors_on_reserved_id() {
        let c = cfg(r#"
version: 1
pipeline: { source: { type: rest, config: {} }, sink: { type: jsonl, config: { path: ./o } } }
matrix:
  - { id: env }
"#);
        assert!(matches!(
            expand(&c).unwrap_err(),
            CliError::ReservedRowId { .. }
        ));
    }

    #[test]
    fn bookmark_token_is_reserved_and_passes_through() {
        // `${bookmark}` is consumed inside a source's `replication_bind.template`
        // (#513); expand must treat it as a reserved deferred id, not reject it
        // as an unknown interpolation reference.
        let c = cfg(r#"
version: 1
pipeline:
  source:
    type: rest
    config:
      base_url: https://x
      replication_bind: { into: query, name: since, template: "gt ${bookmark}" }
  sink: { type: jsonl, config: { path: ./o } }
"#);
        let nodes = expand(&c).unwrap();
        assert_eq!(nodes.len(), 1);
    }

    #[test]
    fn job_id_token_is_reserved_and_passes_through() {
        // `${job_id}` is consumed inside a source's `async_job` block (#514);
        // expand must treat it as a reserved deferred id.
        let c = cfg(r#"
version: 1
pipeline:
  source:
    type: rest
    config:
      base_url: https://x
      async_job: { submit: { url: /jobs }, job_id: "$.id", poll: { url: "/jobs/${job_id}" }, status: { path: "$.s", success: [Done] }, fetch: { url: "/jobs/${job_id}/r" } }
  sink: { type: jsonl, config: { path: ./o } }
"#);
        let nodes = expand(&c).unwrap();
        assert_eq!(nodes.len(), 1);
    }

    #[test]
    fn window_token_is_reserved_and_passes_through() {
        // `${window}` is consumed inside a source's `window.{lower,upper}.template`
        // (#527); expand must treat it as a reserved deferred id, not a ref.
        let c = cfg(r#"
version: 1
pipeline:
  source:
    type: rest
    config:
      base_url: https://x
      path: /report
      replication_method: incremental
      replication_key: updated_at
      start_replication_value: "2024-01-01"
      window:
        step: 30d
        lower: { into: query, name: start_date, template: "${window}", format: date }
        upper: { into: query, name: end_date, template: "${window}", format: date }
  sink: { type: jsonl, config: { path: ./o } }
"#);
        let nodes = expand(&c).unwrap();
        assert_eq!(nodes.len(), 1);
    }

    #[test]
    fn errors_on_self_parent_cycle() {
        let c = cfg(r#"
version: 1
pipeline: { source: { type: rest, config: {} }, sink: { type: jsonl, config: { path: ./o } } }
matrix:
  - { id: a, parent: a }
"#);
        assert!(matches!(
            expand(&c).unwrap_err(),
            CliError::ParentCycle { .. }
        ));
    }

    #[test]
    fn errors_on_two_node_cycle() {
        let c = cfg(r#"
version: 1
pipeline: { source: { type: rest, config: {} }, sink: { type: jsonl, config: { path: ./o } } }
matrix:
  - { id: a, parent: b }
  - { id: b, parent: a }
"#);
        assert!(matches!(
            expand(&c).unwrap_err(),
            CliError::ParentCycle { .. }
        ));
    }

    #[test]
    fn errors_on_unknown_dependency() {
        let c = cfg(r#"
version: 1
pipeline: { source: { type: rest, config: {} }, sink: { type: jsonl, config: { path: ./o } } }
matrix:
  - { id: facts, depends_on: [nobody] }
"#);
        match expand(&c).unwrap_err() {
            CliError::UnknownDependency { id, depends_on } => {
                assert_eq!(id, "facts");
                assert_eq!(depends_on, "nobody");
            }
            other => panic!("expected UnknownDependency, got {other:?}"),
        }
    }

    #[test]
    fn errors_on_self_dependency() {
        let c = cfg(r#"
version: 1
pipeline: { source: { type: rest, config: {} }, sink: { type: jsonl, config: { path: ./o } } }
matrix:
  - { id: a, depends_on: [a] }
"#);
        match expand(&c).unwrap_err() {
            CliError::DependencyCycle { ids } => assert_eq!(ids, vec!["a".to_string()]),
            other => panic!("expected DependencyCycle, got {other:?}"),
        }
    }

    #[test]
    fn errors_on_depends_on_cycle() {
        let c = cfg(r#"
version: 1
pipeline: { source: { type: rest, config: {} }, sink: { type: jsonl, config: { path: ./o } } }
matrix:
  - { id: a, depends_on: [b] }
  - { id: b, depends_on: [a] }
"#);
        match expand(&c).unwrap_err() {
            CliError::DependencyCycle { ids } => {
                assert_eq!(ids, vec!["a".to_string(), "b".to_string()]);
            }
            other => panic!("expected DependencyCycle, got {other:?}"),
        }
    }

    #[test]
    fn errors_on_mixed_parent_depends_on_cycle() {
        // `a` is a child of `b` (parent edge b -> a) while `b` waits for `a`
        // (dependency edge a -> b). Neither the parent-only walk nor a
        // depends_on-only check sees this — only the combined graph does.
        let c = cfg(r#"
version: 1
pipeline: { source: { type: rest, config: {} }, sink: { type: jsonl, config: { path: ./o } } }
matrix:
  - { id: a, parent: b }
  - { id: b, depends_on: [a] }
"#);
        match expand(&c).unwrap_err() {
            CliError::DependencyCycle { ids } => {
                assert_eq!(ids, vec!["a".to_string(), "b".to_string()]);
            }
            other => panic!("expected DependencyCycle, got {other:?}"),
        }
    }

    #[test]
    fn depends_on_is_recorded_and_deduped() {
        let c = cfg(r#"
version: 1
pipeline: { source: { type: rest, config: {} }, sink: { type: jsonl, config: { path: ./o } } }
matrix:
  - { id: dims }
  - { id: staging }
  - { id: facts, depends_on: [dims, staging, dims] }
"#);
        let nodes = expand(&c).unwrap();
        let facts = nodes.iter().find(|n| n.id == "facts").unwrap();
        assert_eq!(
            facts.depends_on,
            vec!["dims".to_string(), "staging".to_string()]
        );
        assert!(matches!(facts.role, NodeRole::Root));
        let dims = nodes.iter().find(|n| n.id == "dims").unwrap();
        assert!(dims.depends_on.is_empty());
    }

    // ── Discovery-driven request matrix (#501) ─────────────────────────────

    fn disc_cfg(matrix: &str) -> String {
        format!(
            r#"
version: 1
pipeline: {{ source: {{ type: rest, config: {{}} }}, sink: {{ type: jsonl, config: {{ path: ./o }} }} }}
matrix:
{matrix}
"#
        )
    }

    #[test]
    fn discovery_and_product_roles_are_assigned() {
        let c = cfg(&disc_cfg(
            r#"  - id: subs
    discover:
      source: { type: rest, config: {} }
      select: "$.id"
      as: subsidiary_id
  - id: report
    for_each: [subs]"#,
        ));
        let nodes = expand(&c).unwrap();
        let subs = nodes.iter().find(|n| n.id == "subs").unwrap();
        match &subs.role {
            NodeRole::Discovery {
                select, as_alias, ..
            } => {
                assert_eq!(select, "$.id");
                assert_eq!(as_alias, "subsidiary_id");
            }
            other => panic!("expected Discovery, got {other:?}"),
        }
        let report = nodes.iter().find(|n| n.id == "report").unwrap();
        match &report.role {
            NodeRole::Product { dims, .. } => assert_eq!(dims, &vec!["subs".to_string()]),
            other => panic!("expected Product, got {other:?}"),
        }
        // The dim is folded into depends_on so readiness/skip/cycle reuse it.
        assert_eq!(report.depends_on, vec!["subs".to_string()]);
    }

    #[test]
    fn chained_discover_without_collect_is_rejected() {
        // #531: a `discover:` row with `for_each:` must set `collect: true`.
        let c = cfg(&disc_cfg(
            r#"  - id: types
    discover: { source: { type: rest, config: {} }, select: "$.name", as: name }
  - id: props
    for_each: [types]
    discover: { source: { type: rest, config: {} }, select: "$.name", as: name }"#,
        ));
        let err = expand(&c).unwrap_err().to_string();
        assert!(err.contains("collect: true"), "{err}");
    }

    #[test]
    fn collect_without_for_each_is_rejected() {
        // #531: `collect: true` is meaningless without an upstream `for_each:`.
        let c = cfg(&disc_cfg(
            r#"  - id: types
    discover: { source: { type: rest, config: {} }, select: "$.name", as: name, collect: true }"#,
        ));
        let err = expand(&c).unwrap_err().to_string();
        assert!(err.contains("requires `for_each:`"), "{err}");
    }

    #[test]
    fn chained_discovery_roles_and_deps_are_wired() {
        // #531: types → props (chained, collected) → records (product injecting props).
        let c = cfg(&disc_cfg(
            r#"  - id: types
    discover: { source: { type: rest, config: {} }, select: "$.name", as: name }
  - id: props
    for_each: [types]
    discover:
      source: { type: rest, config: { path: "/props/${types.name}" } }
      select: "$.name"
      as: name
      collect: true
  - id: records
    for_each: [types]
    source: { type: rest, config: { path: "/obj/${types.name}", query_params: { properties: "${props.name}" } } }"#,
        ));
        let nodes = expand(&c).unwrap();
        // props: a chained Discovery (collect) fanning out over [types].
        let props = nodes.iter().find(|n| n.id == "props").unwrap();
        match &props.role {
            NodeRole::Discovery { collect, dims, .. } => {
                assert!(*collect);
                assert_eq!(dims, &vec!["types".to_string()]);
            }
            other => panic!("expected chained Discovery, got {other:?}"),
        }
        assert_eq!(props.depends_on, vec!["types".to_string()]);
        // records: a Product over [types] that injects the collected `props`.
        let records = nodes.iter().find(|n| n.id == "records").unwrap();
        match &records.role {
            NodeRole::Product { dims, collected } => {
                assert_eq!(dims, &vec!["types".to_string()]);
                assert_eq!(collected, &vec!["props".to_string()]);
            }
            other => panic!("expected Product, got {other:?}"),
        }
        // records depends on both the fan-out dim and the collected discovery.
        assert!(records.depends_on.contains(&"types".to_string()));
        assert!(records.depends_on.contains(&"props".to_string()));
    }

    #[test]
    fn chained_discovery_cycle_is_rejected() {
        // #531: two chained discoveries fanning out over each other form a cycle.
        let c = cfg(&disc_cfg(
            r#"  - id: a
    for_each: [b]
    discover: { source: { type: rest, config: {} }, select: "$.name", as: name, collect: true }
  - id: b
    for_each: [a]
    discover: { source: { type: rest, config: {} }, select: "$.name", as: name, collect: true }"#,
        ));
        let err = expand(&c).unwrap_err().to_string();
        assert!(err.to_lowercase().contains("cycle"), "{err}");
    }

    #[test]
    fn discover_with_sink_is_rejected() {
        let c = cfg(&disc_cfg(
            r#"  - id: a
    discover: { source: { type: rest, config: {} }, select: "$.id", as: x }
    sink: { type: jsonl, config: { path: ./o } }"#,
        ));
        let err = expand(&c).unwrap_err().to_string();
        assert!(err.contains("has no sink"), "{err}");
    }

    #[test]
    fn for_each_on_non_discovery_row_is_rejected() {
        let c = cfg(&disc_cfg(
            r#"  - id: plain
  - id: report
    for_each: [plain]"#,
        ));
        let err = expand(&c).unwrap_err().to_string();
        assert!(err.contains("is not a `discover:` row"), "{err}");
    }

    #[test]
    fn for_each_unknown_row_is_rejected() {
        let c = cfg(&disc_cfg(
            r#"  - id: report
    for_each: [ghost]"#,
        ));
        let err = expand(&c).unwrap_err().to_string();
        assert!(err.contains("unknown row 'ghost'"), "{err}");
    }

    #[test]
    fn for_each_with_parent_is_rejected() {
        let c = cfg(&disc_cfg(
            r#"  - id: subs
    discover: { source: { type: rest, config: {} }, select: "$.id", as: x }
  - id: p
  - id: report
    parent: p
    for_each: [subs]"#,
        ));
        let err = expand(&c).unwrap_err().to_string();
        assert!(err.contains("cannot be combined"), "{err}");
    }

    #[test]
    fn discover_bad_alias_is_rejected() {
        let c = cfg(&disc_cfg(
            r#"  - id: a
    discover: { source: { type: rest, config: {} }, select: "$.id", as: "Bad Alias" }"#,
        ));
        let err = expand(&c).unwrap_err().to_string();
        assert!(err.contains("must match"), "{err}");
    }

    #[test]
    fn discovery_source_ref_resolves_named_template() {
        let c = cfg(r#"
version: 1
pipeline:
  sources:
    api: { type: rest, config: { base_url: https://x, path: /list } }
  sink: { type: jsonl, config: { path: ./o } }
matrix:
  - id: subs
    discover:
      source: { ref: api, config: { path: /subsidiaries } }
      select: "$.id"
      as: sid
  - id: report
    for_each: [subs]
    source: { ref: api }
"#);
        let nodes = expand(&c).unwrap();
        let subs = nodes.iter().find(|n| n.id == "subs").unwrap();
        // The named template resolved (kind rest) and the override applied.
        assert_eq!(subs.source.kind, "rest");
        assert_eq!(subs.source.config["path"], "/subsidiaries");
        assert_eq!(subs.source.config["base_url"], "https://x");
    }

    #[test]
    fn depends_on_may_target_a_child_row() {
        // Waiting on a per-record fan-out row is legal: the dependent starts
        // only after every one of the child's invocations completes.
        let c = cfg(r#"
version: 1
pipeline: { source: { type: rest, config: {} }, sink: { type: jsonl, config: { path: ./o } } }
matrix:
  - { id: users }
  - { id: posts, parent: users }
  - { id: rollup, depends_on: [posts] }
"#);
        let nodes = expand(&c).unwrap();
        let rollup = nodes.iter().find(|n| n.id == "rollup").unwrap();
        assert_eq!(rollup.depends_on, vec!["posts".to_string()]);
    }

    #[test]
    fn errors_on_unknown_interpolation_id() {
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: { url: "https://x/${nobody.id}" } }
  sink:   { type: jsonl, config: { path: ./o } }
"#);
        assert!(matches!(
            expand(&c).unwrap_err(),
            CliError::UnknownInterpolationId { .. }
        ));
    }

    #[test]
    fn dot_form_reserved_prefix_is_validated_as_deferred_id() {
        // Regression for #78/#39: `${env.foo}` has no colon, so it is a
        // deferred reference to id `env`, not a load-time `env:` directive.
        // The validator must reject it (as the runtime would), rather than
        // silently skipping it and letting `run` fail later.
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: { url: "https://x/${env.foo}" } }
  sink:   { type: jsonl, config: { path: ./o } }
"#);
        match expand(&c).unwrap_err() {
            CliError::UnknownInterpolationId { id, .. } => assert_eq!(id, "env"),
            other => panic!("expected UnknownInterpolationId for `env`, got {other:?}"),
        }
    }

    #[test]
    fn accepts_id_path_when_referenced_row_exists() {
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o } }
matrix:
  - id: users
  - id: posts
    parent: users
    source: { config: { path: "/v1/users/${users.id}/posts" } }
"#);
        let nodes = expand(&c).unwrap();
        let posts = nodes.iter().find(|n| n.id == "posts").unwrap();
        assert_eq!(posts.deferred_refs.len(), 1);
        assert_eq!(posts.deferred_refs[0].referenced_id, "users");
        assert_eq!(posts.deferred_refs[0].dotted_path, "id");
    }

    #[test]
    fn nested_referenced_path_resolves() {
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o } }
matrix:
  - id: users
  - id: addrs
    parent: users
    source: { config: { path: "/users/${users.addr.city}/addr" } }
"#);
        let nodes = expand(&c).unwrap();
        let addrs = nodes.iter().find(|n| n.id == "addrs").unwrap();
        assert_eq!(addrs.deferred_refs[0].dotted_path, "addr.city");
    }

    #[test]
    fn roots_come_before_children_in_order() {
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o } }
matrix:
  - id: posts
    parent: users
  - id: users
"#);
        let nodes = expand(&c).unwrap();
        let users_idx = nodes.iter().position(|n| n.id == "users").unwrap();
        let posts_idx = nodes.iter().position(|n| n.id == "posts").unwrap();
        assert!(users_idx < posts_idx, "users must precede posts");
    }

    #[test]
    fn child_node_has_parent_role() {
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o } }
matrix:
  - id: users
  - id: posts
    parent: users
    parent_key: user_id
"#);
        let nodes = expand(&c).unwrap();
        let posts = nodes.iter().find(|n| n.id == "posts").unwrap();
        match &posts.role {
            NodeRole::Child {
                parent_id,
                parent_key,
            } => {
                assert_eq!(parent_id, "users");
                assert_eq!(parent_key, "user_id");
            }
            other => panic!("expected Child, got {other:?}"),
        }
    }

    #[test]
    fn expand_rejects_zero_per_page_budget() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
  dlq:
    sink: { type: jsonl, config: { path: ./dlq.jsonl } }
    max_failures_per_page: 0
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let err = expand(&cfg).unwrap_err();
        assert!(matches!(
            err,
            CliError::InvalidDlqBudget {
                field: "max_failures_per_page"
            }
        ));
    }

    #[test]
    fn expand_rejects_zero_total_budget() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
  dlq:
    sink: { type: jsonl, config: { path: ./dlq.jsonl } }
    max_failures_total: 0
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let err = expand(&cfg).unwrap_err();
        assert!(matches!(
            err,
            CliError::InvalidDlqBudget {
                field: "max_failures_total"
            }
        ));
    }

    #[test]
    fn expand_rejects_unknown_dlq_sink_kind() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
  dlq:
    sink: { type: not_a_sink, config: {} }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let err = expand(&cfg).unwrap_err();
        assert!(matches!(err, CliError::UnknownDlqSinkKind { .. }));
    }

    #[cfg(feature = "quality")]
    #[test]
    fn expand_rejects_quarantine_without_dlq() {
        // A quality check with `on_failure: quarantine` needs a DLQ to route to.
        // `expand` must reject the config so `faucet validate` fails fast.
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
  quality:
    record:
      - { type: not_null, field: id, on_failure: quarantine }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let err = expand(&cfg).unwrap_err();
        match err {
            CliError::Config(msg) => {
                assert!(msg.contains("quarantine"), "{msg}");
                assert!(msg.contains("DLQ") || msg.contains("dlq"), "{msg}");
            }
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[cfg(feature = "quality")]
    #[test]
    fn expand_accepts_quarantine_with_dlq() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
  dlq:
    sink: { type: jsonl, config: { path: ./dlq.jsonl } }
  quality:
    record:
      - { type: not_null, field: id, on_failure: quarantine }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        assert_eq!(nodes.len(), 1);
        let q = nodes[0]
            .quality
            .as_ref()
            .expect("quality threaded onto node");
        assert_eq!(q.record.len(), 1);
    }

    #[cfg(feature = "quality")]
    #[test]
    fn expand_accepts_abort_quality_without_dlq() {
        // `on_failure: abort` does not route to a DLQ, so no DLQ is required.
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
  quality:
    record:
      - { type: not_null, field: id, on_failure: abort }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        assert!(nodes[0].quality.is_some());
    }

    #[cfg(feature = "contract")]
    #[test]
    fn expand_rejects_contract_quarantine_without_dlq() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
  contract:
    version: "1.0.0"
    on_breach: quarantine
    fields:
      - { name: id, type: integer }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let err = expand(&cfg).unwrap_err();
        match err {
            CliError::Config(msg) => {
                assert!(msg.contains("on_breach: quarantine"), "{msg}");
                assert!(msg.contains("dlq"), "{msg}");
            }
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[cfg(feature = "contract")]
    #[test]
    fn expand_accepts_contract_quarantine_with_dlq() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
  dlq:
    sink: { type: jsonl, config: { path: ./dlq.jsonl } }
  contract:
    version: "1.0.0"
    on_breach: quarantine
    fields:
      - { name: id, type: integer }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        assert_eq!(nodes.len(), 1);
        let c = nodes[0]
            .contract
            .as_ref()
            .expect("contract threaded onto node");
        assert_eq!(c.version, "1.0.0");
        assert_eq!(c.fields.len(), 1);
    }

    #[cfg(feature = "contract")]
    #[test]
    fn expand_accepts_contract_fail_without_dlq() {
        // `on_breach: fail` (the default) does not route to a DLQ.
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
  contract:
    version: "1.0.0"
    fields:
      - { name: id, type: integer }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        assert!(nodes[0].contract.is_some());
    }

    #[cfg(feature = "contract")]
    #[test]
    fn expand_rejects_malformed_contract() {
        // A bad regex must surface at expand time (load-time), not mid-run.
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
  contract:
    version: "1.0.0"
    fields:
      - { name: email, type: string, pattern: "[invalid" }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let err = expand(&cfg).unwrap_err();
        match err {
            CliError::Config(msg) => assert!(msg.contains("invalid pattern"), "{msg}"),
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn legacy_singular_source_resolves_as_default_template() {
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: jsonl, config: { path: ./o } }
"#);
        let nodes = expand(&c).unwrap();
        assert_eq!(nodes[0].source.kind, "rest");
        assert_eq!(nodes[0].source.config["base_url"], "https://x");
    }

    #[test]
    fn row_with_ref_picks_named_template() {
        let c = cfg(r#"
version: 1
pipeline:
  sources:
    users_api: { type: rest, config: { base_url: https://x } }
  sinks:
    archive:   { type: jsonl, config: { path: ./out } }
matrix:
  - id: load_users
    source:
      ref: users_api
      config: { path: /v1/users }
    sink:
      ref: archive
      config: { path: ./users.jsonl }
"#);
        let nodes = expand(&c).unwrap();
        assert_eq!(nodes[0].source.kind, "rest");
        assert_eq!(nodes[0].source.config["base_url"], "https://x");
        assert_eq!(nodes[0].source.config["path"], "/v1/users");
        assert_eq!(nodes[0].sink.config["path"], "./users.jsonl");
    }

    #[test]
    fn row_without_ref_falls_back_to_default_template() {
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: jsonl, config: { path: ./o } }
matrix:
  - id: users
    source: { config: { path: /v1/users } }
"#);
        let nodes = expand(&c).unwrap();
        assert_eq!(nodes[0].source.kind, "rest");
        assert_eq!(nodes[0].source.config["path"], "/v1/users");
    }

    #[test]
    fn unknown_template_ref_errors_with_known_list() {
        let c = cfg(r#"
version: 1
pipeline:
  sources:
    a: { type: rest, config: {} }
    b: { type: rest, config: {} }
  sinks:
    s: { type: jsonl, config: { path: ./o } }
matrix:
  - id: x
    source: { ref: c }
    sink: { ref: s }
"#);
        let err = expand(&c).unwrap_err();
        match err {
            CliError::UnknownTemplate {
                kind,
                name,
                row_id,
                known,
            } => {
                assert_eq!(kind, "source");
                assert_eq!(name, "c");
                assert_eq!(row_id, "x");
                assert_eq!(known, vec!["a".to_string(), "b".to_string()]);
            }
            other => panic!("expected UnknownTemplate, got {other:?}"),
        }
    }

    #[test]
    fn missing_default_template_errors() {
        // No singular `source:` and no `sources.default` — a row without a ref
        // has nowhere to go.
        let c = cfg(r#"
version: 1
pipeline:
  sources:
    users_api: { type: rest, config: {} }
  sink: { type: jsonl, config: { path: ./o } }
matrix:
  - id: x
    source: { config: { path: /v1 } }
"#);
        let err = expand(&c).unwrap_err();
        match err {
            CliError::MissingTemplate { kind, row_id } => {
                assert_eq!(kind, "source");
                assert_eq!(row_id, "x");
            }
            other => panic!("expected MissingTemplate, got {other:?}"),
        }
    }

    #[test]
    fn duplicate_default_template_errors() {
        // Defining both legacy `source:` and `sources.default:` is a conflict.
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sources:
    default: { type: rest, config: {} }
  sink: { type: jsonl, config: { path: ./o } }
"#);
        let err = expand(&c).unwrap_err();
        match err {
            CliError::DuplicateTemplate { kind, name } => {
                assert_eq!(kind, "source");
                assert_eq!(name, "default");
            }
            other => panic!("expected DuplicateTemplate, got {other:?}"),
        }
    }

    #[test]
    fn row_can_override_template_kind() {
        let c = cfg(r#"
version: 1
pipeline:
  sources:
    api: { type: rest, config: { base_url: https://x } }
  sinks:
    out: { type: jsonl, config: { path: ./o } }
matrix:
  - id: x
    source: { ref: api, type: graphql, config: { query: "{users{id}}" } }
    sink: { ref: out }
"#);
        let nodes = expand(&c).unwrap();
        assert_eq!(nodes[0].source.kind, "graphql");
        assert_eq!(nodes[0].source.config["base_url"], "https://x");
        assert_eq!(nodes[0].source.config["query"], "{users{id}}");
    }

    #[test]
    fn expand_accepts_inherited_disabled_replaced_dlq_rows() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
  dlq:
    sink: { type: jsonl, config: { path: ./base.jsonl } }
matrix:
  - id: a
  - id: b
    dlq: null
  - id: c
    dlq:
      sink: { type: jsonl, config: { path: ./c.jsonl } }
      on_batch_error: dlq_all
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        assert_eq!(nodes.len(), 3);
        // Row a inherits.
        assert_eq!(nodes[0].dlq.as_ref().unwrap().sink.kind, "jsonl");
        assert_eq!(
            nodes[0]
                .dlq
                .as_ref()
                .unwrap()
                .sink
                .config
                .get("path")
                .unwrap(),
            "./base.jsonl"
        );
        // Row b is disabled.
        assert!(nodes[1].dlq.is_none());
        // Row c is replaced.
        assert_eq!(
            nodes[2].dlq.as_ref().unwrap().on_batch_error,
            OnBatchErrorSpec::DlqAll
        );
        assert_eq!(
            nodes[2]
                .dlq
                .as_ref()
                .unwrap()
                .sink
                .config
                .get("path")
                .unwrap(),
            "./c.jsonl"
        );
    }

    #[test]
    fn multiple_rows_pick_different_templates() {
        let c = cfg(r#"
version: 1
pipeline:
  sources:
    users_api:  { type: rest, config: { base_url: https://users.example } }
    orders_api: { type: rest, config: { base_url: https://orders.example } }
  sinks:
    archive: { type: jsonl, config: { path: ./out } }
matrix:
  - id: load_users
    source: { ref: users_api, config: { path: /v1/users } }
    sink:   { ref: archive,   config: { path: ./users.jsonl } }
  - id: load_orders
    source: { ref: orders_api, config: { path: /v1/orders } }
    sink:   { ref: archive,    config: { path: ./orders.jsonl } }
"#);
        let nodes = expand(&c).unwrap();
        assert_eq!(nodes.len(), 2);
        let users = nodes.iter().find(|n| n.id == "load_users").unwrap();
        let orders = nodes.iter().find(|n| n.id == "load_orders").unwrap();
        assert_eq!(users.source.config["base_url"], "https://users.example");
        assert_eq!(users.source.config["path"], "/v1/users");
        assert_eq!(orders.source.config["base_url"], "https://orders.example");
        assert_eq!(orders.source.config["path"], "/v1/orders");
        // Both rows share the same sink template but pick different output paths.
        assert_eq!(users.sink.config["path"], "./users.jsonl");
        assert_eq!(orders.sink.config["path"], "./orders.jsonl");
    }

    #[test]
    fn sink_template_with_transforms_errors_at_expand() {
        let yaml = r#"
version: 1
pipeline:
  source:
    type: rest
    config: {}
  sinks:
    bad:
      type: jsonl
      config: { destination: /tmp/x.jsonl }
      transforms:
        - { type: flatten, config: { separator: "_" } }
matrix:
  - id: row
    sink: { ref: bad }
"#;
        let cfg = crate::config::PipelineConfig::from_text(yaml, std::path::Path::new("test.yaml"))
            .unwrap();
        let err = crate::expand::expand(&cfg).expect_err("expected TransformsOnSink");
        match err {
            crate::error::CliError::TransformsOnSink { name } => assert_eq!(name, "bad"),
            other => panic!("expected TransformsOnSink, got {other:?}"),
        }
    }

    #[test]
    fn sink_template_with_inherit_transforms_false_errors_at_expand() {
        let yaml = r#"
version: 1
pipeline:
  source:
    type: rest
    config: {}
  sinks:
    bad:
      type: jsonl
      config: { destination: /tmp/x.jsonl }
      inherit_transforms: false
matrix:
  - id: row
    sink: { ref: bad }
"#;
        let cfg = crate::config::PipelineConfig::from_text(yaml, std::path::Path::new("test.yaml"))
            .unwrap();
        let err = crate::expand::expand(&cfg).expect_err("expected InheritTransformsOnSink");
        match err {
            crate::error::CliError::InheritTransformsOnSink { name } => assert_eq!(name, "bad"),
            other => panic!("expected InheritTransformsOnSink, got {other:?}"),
        }
    }

    fn kinds(transforms: &[crate::config::TransformSpec]) -> Vec<String> {
        transforms.iter().map(|t| t.kind.clone()).collect()
    }

    #[test]
    fn three_layer_concat_default_inherit() {
        let yaml = r#"
version: 1
pipeline:
  transforms:
    - { type: flatten, config: { separator: "_" } }
  sources:
    s:
      type: rest
      config: {}
      transforms:
        - { type: keys_case, config: { mode: snake } }
  sink:
    type: jsonl
    config: { destination: /tmp/x.jsonl }
matrix:
  - id: row
    source: { ref: s }
    transforms:
      - { type: select, config: { fields: [id] } }
"#;
        let cfg = crate::config::PipelineConfig::from_text(yaml, std::path::Path::new("test.yaml"))
            .unwrap();
        let nodes = crate::expand::expand(&cfg).unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(
            kinds(&nodes[0].transforms),
            vec!["flatten", "keys_case", "select"]
        );
    }

    #[test]
    fn source_inherit_false_drops_pipeline_layer() {
        let yaml = r#"
version: 1
pipeline:
  transforms:
    - { type: flatten, config: { separator: "_" } }
  sources:
    s:
      type: rest
      config: {}
      inherit_transforms: false
      transforms:
        - { type: keys_case, config: { mode: snake } }
  sink:
    type: jsonl
    config: { destination: /tmp/x.jsonl }
matrix:
  - id: row
    source: { ref: s }
    transforms:
      - { type: select, config: { fields: [id] } }
"#;
        let cfg = crate::config::PipelineConfig::from_text(yaml, std::path::Path::new("test.yaml"))
            .unwrap();
        let nodes = crate::expand::expand(&cfg).unwrap();
        assert_eq!(kinds(&nodes[0].transforms), vec!["keys_case", "select"]);
    }

    #[test]
    fn row_inherit_false_drops_pipeline_and_source_layers() {
        let yaml = r#"
version: 1
pipeline:
  transforms:
    - { type: flatten, config: { separator: "_" } }
  sources:
    s:
      type: rest
      config: {}
      transforms:
        - { type: keys_case, config: { mode: snake } }
  sink:
    type: jsonl
    config: { destination: /tmp/x.jsonl }
matrix:
  - id: row
    source: { ref: s }
    inherit_transforms: false
    transforms:
      - { type: select, config: { fields: [id] } }
"#;
        let cfg = crate::config::PipelineConfig::from_text(yaml, std::path::Path::new("test.yaml"))
            .unwrap();
        let nodes = crate::expand::expand(&cfg).unwrap();
        assert_eq!(kinds(&nodes[0].transforms), vec!["select"]);
    }

    #[test]
    fn both_inherit_false_yields_row_only() {
        let yaml = r#"
version: 1
pipeline:
  transforms:
    - { type: flatten, config: { separator: "_" } }
  sources:
    s:
      type: rest
      config: {}
      inherit_transforms: false
      transforms:
        - { type: keys_case, config: { mode: snake } }
  sink:
    type: jsonl
    config: { destination: /tmp/x.jsonl }
matrix:
  - id: row
    source: { ref: s }
    inherit_transforms: false
    transforms:
      - { type: select, config: { fields: [id] } }
"#;
        let cfg = crate::config::PipelineConfig::from_text(yaml, std::path::Path::new("test.yaml"))
            .unwrap();
        let nodes = crate::expand::expand(&cfg).unwrap();
        assert_eq!(kinds(&nodes[0].transforms), vec!["select"]);
    }

    #[test]
    fn all_layers_omitted_yields_empty_transforms() {
        let yaml = r#"
version: 1
pipeline:
  source:
    type: rest
    config: {}
  sink:
    type: jsonl
    config: { destination: /tmp/x.jsonl }
matrix:
  - id: row
"#;
        let cfg = crate::config::PipelineConfig::from_text(yaml, std::path::Path::new("test.yaml"))
            .unwrap();
        let nodes = crate::expand::expand(&cfg).unwrap();
        assert!(nodes[0].transforms.is_empty());
    }

    #[test]
    fn now_is_a_valid_builtin_ref_not_an_unknown_id() {
        // A root pipeline referencing ${now.date} must pass expand validation.
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: "out-${now.date}.jsonl" } }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        // expand must NOT raise UnknownInterpolationId for `now`.
        assert!(expand(&cfg).is_ok());
    }

    #[test]
    fn now_is_a_reserved_row_id() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
matrix:
  - id: now
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        match expand(&cfg).unwrap_err() {
            CliError::ReservedRowId { id } => assert_eq!(id, "now"),
            other => panic!("expected ReservedRowId, got {other:?}"),
        }
    }

    #[test]
    fn expand_rejects_invalid_adaptive_batch_size_at_load() {
        // Fail-fast: an invalid execution.adaptive_batch_size block must be
        // rejected by `expand` (the gate `faucet validate` uses), not only at
        // run time in the executor.
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
execution:
  adaptive_batch_size:
    enabled: true
    min: 5000
    max: 100
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let err = expand(&cfg).unwrap_err();
        assert!(
            err.to_string().contains("adaptive_batch_size.min"),
            "expected adaptive validation error, got: {err}"
        );
    }

    #[test]
    fn expand_accepts_valid_adaptive_batch_size() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
execution:
  adaptive_batch_size:
    enabled: true
    min: 100
    max: 5000
    target_latency_ms: 500
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        assert!(expand(&cfg).is_ok());
    }

    // --- exactly-once delivery gate tests ---

    #[test]
    fn exactly_once_rejects_non_cdc_source() {
        // rest→stdout with exactly_once must fail: rest is not replay-capable.
        let yaml = r#"
version: 1
delivery: exactly_once
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: stdout, config: {} }
  state:
    type: memory
    config: {}
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let err = expand(&cfg).unwrap_err();
        match &err {
            CliError::Config(msg) => {
                assert!(
                    msg.contains("rest"),
                    "expected source kind in error, got: {msg}"
                );
                assert!(
                    msg.contains("exactly_once") || msg.contains("not supported"),
                    "got: {msg}"
                );
            }
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn exactly_once_rejects_non_idempotent_sink() {
        // postgres-cdc→stdout: source is OK but stdout is not idempotent.
        let yaml = r#"
version: 1
delivery: exactly_once
pipeline:
  source: { type: postgres-cdc, config: {} }
  sink:   { type: stdout, config: {} }
  state:
    type: memory
    config: {}
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let err = expand(&cfg).unwrap_err();
        match &err {
            CliError::Config(msg) => {
                assert!(
                    msg.contains("stdout"),
                    "expected sink kind in error, got: {msg}"
                );
                assert!(
                    msg.contains("exactly_once") || msg.contains("not supported"),
                    "got: {msg}"
                );
            }
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn exactly_once_accepted_with_cdc_source_idempotent_sink_and_state() {
        // postgres-cdc → sqlite + a *durable* state store → must expand
        // successfully. (Must not be `memory`: exactly-once needs cross-restart
        // durability — see `exactly_once_rejects_memory_state`.)
        let yaml = r#"
version: 1
delivery: exactly_once
pipeline:
  source: { type: postgres-cdc, config: {} }
  sink:   { type: sqlite, config: {} }
  state:
    type: file
    config: { path: "/tmp/faucet-eo-state.json" }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].delivery, faucet_core::DeliveryMode::ExactlyOnce);
        assert_eq!(
            nodes[0].delivery_guarantee,
            faucet_core::DeliveryGuarantee::EffectivelyOnce(
                faucet_core::EffectivelyOnceMechanism::AtomicWatermark
            )
        );
    }

    #[test]
    fn exactly_once_accepted_via_keyed_upsert_with_any_source() {
        // rest → postgres with `write_mode: upsert` + `key`: accepted under
        // exactly_once via the keyed-upsert mechanism (#292) — no CDC source,
        // no state store required.
        let yaml = r#"
version: 1
delivery: exactly_once
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:
    type: postgres
    config:
      connection_url: "postgres://localhost/db"
      table_name: t
      column_mapping: auto_map
      write_mode: upsert
      key: [id]
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        assert_eq!(
            nodes[0].delivery_guarantee,
            faucet_core::DeliveryGuarantee::EffectivelyOnce(
                faucet_core::EffectivelyOnceMechanism::KeyedUpsert
            )
        );
    }

    #[test]
    fn exactly_once_kafka_source_accepted_with_atomic_sink() {
        // kafka → sqlite + durable state: the kafka source's offset bookmarks
        // qualify it for the atomic-watermark mechanism (#291).
        let yaml = r#"
version: 1
delivery: exactly_once
pipeline:
  source:
    type: kafka
    config: { brokers: "localhost:9092", topics: [t], group_id: g, max_messages: 10 }
  sink:   { type: sqlite, config: {} }
  state:
    type: file
    config: { path: "/tmp/faucet-eo-kafka-state.json" }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        assert_eq!(
            nodes[0].delivery_guarantee,
            faucet_core::DeliveryGuarantee::EffectivelyOnce(
                faucet_core::EffectivelyOnceMechanism::AtomicWatermark
            )
        );
    }

    #[test]
    fn exactly_once_source_error_hints_keyed_upsert_for_capable_sink() {
        // rest → postgres (no write_mode): the source error should point at
        // the keyed-upsert alternative since postgres is upsert-capable.
        let yaml = r#"
version: 1
delivery: exactly_once
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:
    type: postgres
    config:
      connection_url: "postgres://localhost/db"
      table_name: t
      column_mapping: auto_map
  state:
    type: file
    config: { path: "/tmp/faucet-eo-hint-state.json" }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let err = expand(&cfg).unwrap_err();
        match &err {
            CliError::Config(msg) => assert!(
                msg.contains("write_mode: upsert"),
                "expected keyed-upsert hint, got: {msg}"
            ),
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn derived_guarantee_is_at_least_once_by_default() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: stdout, config: {} }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        assert_eq!(
            nodes[0].delivery_guarantee,
            faucet_core::DeliveryGuarantee::AtLeastOnce
        );
    }

    #[test]
    fn exactly_once_rejects_memory_state() {
        // A non-durable `memory` store defeats the cross-restart watermark
        // guarantee, so it must be rejected at config-load (F24).
        let yaml = r#"
version: 1
delivery: exactly_once
pipeline:
  source: { type: postgres-cdc, config: {} }
  sink:   { type: sqlite, config: {} }
  state:
    type: memory
    config: {}
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let err = expand(&cfg).unwrap_err();
        match &err {
            CliError::Config(msg) => assert!(
                msg.contains("durable") && msg.contains("memory"),
                "expected durable/memory mention, got: {msg}"
            ),
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn exactly_once_rejects_missing_state_store() {
        // Valid CDC pair but no state block → must fail with "requires a state store".
        let yaml = r#"
version: 1
delivery: exactly_once
pipeline:
  source: { type: postgres-cdc, config: {} }
  sink:   { type: sqlite, config: {} }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let err = expand(&cfg).unwrap_err();
        match &err {
            CliError::Config(msg) => {
                assert!(
                    msg.contains("state store") || msg.contains("state"),
                    "expected state-store mention in error, got: {msg}"
                );
            }
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn rejects_upsert_on_unsupported_sink() {
        let c = cfg(r#"
version: 1
name: t
pipeline:
  source: { type: rest, config: { url: "http://x" } }
  sink:   { type: jsonl, config: { path: "out.jsonl", write_mode: upsert, key: [id] } }
"#);
        let err = expand(&c).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("write_mode") && msg.contains("upsert") && msg.contains("jsonl"),
            "{msg}"
        );
    }

    #[test]
    fn rejects_upsert_without_key() {
        let c = cfg(r#"
version: 1
name: t
pipeline:
  source: { type: rest, config: { url: "http://x" } }
  sink:   { type: postgres, config: { connection_url: "postgres://x", table_name: t, column_mapping: auto_map, write_mode: upsert } }
"#);
        let err = expand(&c).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("key"), "{msg}");
    }

    #[test]
    fn accepts_upsert_on_postgres_with_key() {
        let c = cfg(r#"
version: 1
name: t
pipeline:
  source: { type: rest, config: { url: "http://x" } }
  sink:   { type: postgres, config: { connection_url: "postgres://x", table_name: t, column_mapping: auto_map, write_mode: upsert, key: [id] } }
"#);
        assert!(expand(&c).is_ok());
    }

    #[test]
    fn bigquery_upsert_passes_write_mode_gate() {
        let c = cfg(r#"
version: 1
name: t
pipeline:
  source: { type: rest, config: { url: "http://x" } }
  sink:   { type: bigquery, config: { project_id: p, dataset_id: d, table_id: t, auth: { type: application_default }, write_mode: upsert, key: [id] } }
"#);
        assert!(expand(&c).is_ok());
    }

    #[test]
    fn accepts_append_by_default_on_any_sink() {
        let c = cfg(r#"
version: 1
name: t
pipeline:
  source: { type: rest, config: { url: "http://x" } }
  sink:   { type: jsonl, config: { path: "out.jsonl" } }
"#);
        assert!(expand(&c).is_ok());
    }

    #[test]
    fn rejects_delete_without_key() {
        let c = cfg(r#"
version: 1
name: t
pipeline:
  source: { type: rest, config: { url: "http://x" } }
  sink:   { type: mongodb, config: { connection_url: "mongodb://x", database: d, collection: c, write_mode: delete } }
"#);
        let err = expand(&c).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("delete") && msg.contains("key"), "{msg}");
    }

    #[test]
    fn rejects_unknown_write_mode() {
        let c = cfg(r#"
version: 1
name: t
pipeline:
  source: { type: rest, config: { url: "http://x" } }
  sink:   { type: postgres, config: { connection_url: "postgres://x", table_name: t, column_mapping: auto_map, write_mode: replace } }
"#);
        let err = expand(&c).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("unknown write_mode") && msg.contains("replace"),
            "{msg}"
        );
    }

    #[test]
    fn overwrite_passes_on_capable_sink() {
        let c = cfg(r#"
version: 1
name: t
pipeline:
  source: { type: rest, config: { url: "http://x" } }
  sink:   { type: postgres, config: { connection_url: "postgres://x", table_name: t, column_mapping: auto_map, write_mode: overwrite } }
"#);
        assert!(
            expand(&c).is_ok(),
            "overwrite needs no key and postgres supports it"
        );
    }

    #[test]
    fn scoped_overwrite_passes_on_postgres() {
        let c = cfg(r#"
version: 1
name: t
pipeline:
  source: { type: rest, config: { url: "http://x" } }
  sink:
    type: postgres
    config:
      connection_url: "postgres://x"
      table_name: t
      column_mapping: auto_map
      write_mode: overwrite
      scope: { window: { column: posting_date, from: "2024-06-01", to: "2024-07-01" } }
"#);
        assert!(expand(&c).is_ok(), "postgres supports scoped overwrite");
    }

    #[test]
    fn rejects_scope_on_non_scoped_sink() {
        let c = cfg(r#"
version: 1
name: t
pipeline:
  source: { type: rest, config: { url: "http://x" } }
  sink:
    type: sqlite
    config:
      connection_url: "sqlite://x"
      table_name: t
      column_mapping: auto_map
      write_mode: overwrite
      scope: { window: { column: d, from: 1, to: 2 } }
"#);
        let msg = format!("{}", expand(&c).unwrap_err());
        assert!(
            msg.contains("scoped overwrite") && msg.contains("not supported"),
            "{msg}"
        );
    }

    #[test]
    fn rejects_scope_without_overwrite_mode() {
        let c = cfg(r#"
version: 1
name: t
pipeline:
  source: { type: rest, config: { url: "http://x" } }
  sink:
    type: postgres
    config:
      connection_url: "postgres://x"
      table_name: t
      column_mapping: auto_map
      scope: { window: { column: d, from: 1, to: 2 } }
"#);
        let msg = format!("{}", expand(&c).unwrap_err());
        assert!(
            msg.contains("only valid with `write_mode: overwrite`"),
            "{msg}"
        );
    }

    #[test]
    fn rejects_overwrite_on_unsupported_sink() {
        let c = cfg(r#"
version: 1
name: t
pipeline:
  source: { type: rest, config: { url: "http://x" } }
  sink:   { type: jsonl, config: { path: "out.jsonl", write_mode: overwrite } }
"#);
        let err = expand(&c).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("overwrite")
                && msg.contains("not supported")
                && msg.contains("overwrite sinks"),
            "{msg}"
        );
    }

    #[test]
    fn rejects_overwrite_with_exactly_once() {
        let c = cfg(r#"
version: 1
name: t
delivery: exactly_once
pipeline:
  source: { type: rest, config: { url: "http://x" } }
  sink:   { type: postgres, config: { connection_url: "postgres://x", table_name: t, column_mapping: auto_map, write_mode: overwrite } }
  state:  { type: file, config: { path: "./s.json" } }
"#);
        let err = expand(&c).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("overwrite") && msg.contains("exactly_once"),
            "{msg}"
        );
    }

    #[test]
    fn rejects_overwrite_with_schema_evolve() {
        let c = cfg(r#"
version: 1
name: t
pipeline:
  source: { type: rest, config: { url: "http://x" } }
  sink:   { type: postgres, config: { connection_url: "postgres://x", table_name: t, column_mapping: auto_map, write_mode: overwrite } }
  schema: { on_drift: evolve }
"#);
        let err = expand(&c).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("overwrite") && msg.contains("evolve"), "{msg}");
    }

    #[test]
    fn rejects_poison_dlq_action_without_dlq() {
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: jsonl, config: { path: ./o } }
resilience:
  poison: { max_row_attempts: 3, action: dlq }
"#);
        let err = expand(&c).unwrap_err();
        assert!(
            matches!(&err, CliError::Config(m) if m.contains("poison.action=dlq") && m.contains("dlq:")),
            "got: {err:?}"
        );
    }

    #[test]
    fn accepts_poison_dlq_action_with_dlq() {
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: jsonl, config: { path: ./o } }
  dlq:
    sink: { type: jsonl, config: { path: ./dead.jsonl } }
resilience:
  poison: { max_row_attempts: 3, action: dlq }
"#);
        let nodes = expand(&c).expect("poison.action=dlq with a dlq: block should validate");
        assert_eq!(nodes.len(), 1);
    }

    #[test]
    fn accepts_poison_drop_action_without_dlq() {
        // action=drop discards rows in place, so no DLQ is required.
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: jsonl, config: { path: ./o } }
resilience:
  poison: { max_row_attempts: 3, action: drop }
"#);
        let nodes = expand(&c).expect("poison.action=drop needs no dlq");
        assert_eq!(nodes.len(), 1);
    }

    // --- schema-drift composition gate tests ---

    #[test]
    fn evolve_on_non_evolvable_sink_rejected() {
        // jsonl is not evolution-capable; on_drift: evolve must fail.
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
  schema:
    on_drift: evolve
"#);
        let err = expand(&c).unwrap_err();
        match &err {
            CliError::Config(msg) => {
                assert!(
                    msg.contains("evolve"),
                    "expected evolve mention, got: {msg}"
                );
                assert!(msg.contains("jsonl"), "expected sink kind, got: {msg}");
            }
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn quarantine_drift_without_dlq_rejected() {
        // on_drift: quarantine requires a dlq: block.
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: postgres, config: {} }
  schema:
    on_drift: quarantine
"#);
        let err = expand(&c).unwrap_err();
        match &err {
            CliError::Config(msg) => {
                assert!(
                    msg.contains("quarantine"),
                    "expected quarantine mention, got: {msg}"
                );
                assert!(msg.contains("dlq") || msg.contains("DLQ"), "got: {msg}");
            }
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn evolve_on_postgres_passes() {
        // postgres is evolution-capable; on_drift: evolve must expand.
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: postgres, config: {} }
  schema:
    on_drift: evolve
"#);
        assert!(expand(&c).is_ok());
    }
}

#[cfg(test)]
mod partition_tests {
    //! Row fan-out for the `partition:` block (#479).
    use super::*;
    use crate::config::PipelineConfig;

    fn cfg(yaml: &str) -> PipelineConfig {
        PipelineConfig::from_text(yaml, std::path::Path::new("p.yaml")).expect("config parses")
    }

    const SCOPED_SOURCE: &str = r#"
    type: rest
    config:
      base_url: "https://api.example.com"
      path: "/records?id_from=${partition.start}&id_to=${partition.end}""#;

    fn doc(partition: &str, source: &str) -> String {
        format!(
            "version: 1\nname: p\npipeline:\n  source:{source}\n  sink:\n    type: jsonl\n    config:\n      path: ./out.jsonl\n{partition}"
        )
    }

    #[test]
    fn a_partitioned_row_expands_into_one_node_per_chunk() {
        let nodes = expand(&cfg(&doc(
            "partition:\n  kind: integer\n  from: 0\n  to: 24\n  chunk_size: 10\n  bounds: inclusive\n",
            SCOPED_SOURCE,
        )))
        .expect("expand");
        assert_eq!(nodes.len(), 3, "24 values / 10 = 3 chunks");
        // Each node's source carries its own substituted range.
        let urls: Vec<String> = nodes
            .iter()
            .map(|n| n.source.config["path"].as_str().unwrap().to_string())
            .collect();
        assert!(urls[0].contains("id_from=0&id_to=9"), "{:?}", urls[0]);
        assert!(urls[1].contains("id_from=10&id_to=19"), "{:?}", urls[1]);
        assert!(urls[2].contains("id_from=20&id_to=24"), "{:?}", urls[2]);
    }

    #[test]
    fn chunk_ids_are_distinct_and_namespaced_so_state_keys_cannot_collide() {
        let nodes = expand(&cfg(&doc(
            "partition:\n  kind: integer\n  from: 0\n  to: 24\n  chunk_size: 10\n  bounds: inclusive\n",
            SCOPED_SOURCE,
        )))
        .unwrap();
        let ids: std::collections::BTreeSet<&str> = nodes.iter().map(|n| n.id.as_str()).collect();
        assert_eq!(ids.len(), nodes.len(), "ids must be unique");
        assert!(nodes.iter().all(|n| n.id.contains("::partition::")));
    }

    #[test]
    fn an_unpartitioned_config_is_completely_unchanged() {
        let nodes = expand(&cfg(&doc(
            "",
            "\n    type: csv\n    config:\n      path: ./in.csv",
        )))
        .unwrap();
        assert_eq!(nodes.len(), 1);
        assert!(!nodes[0].id.contains("partition"));
    }

    #[test]
    fn a_partition_block_whose_source_ignores_the_tokens_is_rejected() {
        // Otherwise every chunk runs the identical query N times.
        let err = expand(&cfg(&doc(
            "partition:\n  kind: integer\n  from: 0\n  to: 9\n  chunk_size: 5\n  bounds: inclusive\n",
            "\n    type: csv\n    config:\n      path: ./in.csv",
        )))
        .expect_err("must be rejected");
        let msg = err.to_string();
        assert!(msg.contains("no `${partition.*}` token"), "{msg}");
        assert!(msg.contains("start"), "should list available tokens: {msg}");
    }

    #[test]
    fn a_wrong_kind_token_is_rejected_naming_the_real_tokens() {
        let err = expand(&cfg(&doc(
            "partition:\n  kind: offset\n  total: 20\n  chunk_size: 10\n",
            SCOPED_SOURCE,
        )))
        .expect_err("id-range tokens are not offset tokens");
        let msg = err.to_string();
        assert!(msg.contains("start"), "{msg}");
        assert!(msg.contains("offset"), "{msg}");
    }

    #[test]
    fn a_partitioned_row_cannot_be_a_parent_or_a_dependency() {
        // Its id gains a chunk suffix, so a dependent would resolve to nothing.
        for edge in ["parent: a\n    parent_key: id", "depends_on: [a]"] {
            let yaml = format!(
                "version: 1\nname: p\npipeline:\n  source:\n    type: csv\n    config:\n      path: ./in.csv\n  sink:\n    type: jsonl\n    config:\n      path: ./out.jsonl\nmatrix:\n  - id: a\n    partition:\n      kind: integer\n      from: 0\n      to: 9\n      chunk_size: 5\n      bounds: inclusive\n    source:\n      config:\n        path: \"./in-${{partition.start}}.csv\"\n  - id: b\n    {edge}\n"
            );
            let err = expand(&cfg(&yaml)).expect_err("must be rejected");
            assert!(
                err.to_string()
                    .contains("partitioned row cannot be referenced"),
                "{err}"
            );
        }
    }

    #[test]
    fn the_top_level_block_applies_to_root_rows() {
        let nodes = expand(&cfg(&doc(
            "partition:\n  kind: offset\n  total: 25\n  chunk_size: 10\n",
            "\n    type: rest\n    config:\n      base_url: \"https://x\"\n      path: \"/r?offset=${partition.offset}&limit=${partition.limit}\"",
        )))
        .unwrap();
        assert_eq!(nodes.len(), 3);
        let p = nodes[2].source.config["path"].as_str().unwrap();
        assert!(p.contains("offset=20&limit=5"), "{p}");
    }

    #[test]
    fn a_row_level_block_overrides_the_top_level_default() {
        let yaml = format!(
            "version: 1\nname: p\npipeline:\n  source:{SCOPED_SOURCE}\n  sink:\n    type: jsonl\n    config:\n      path: ./out.jsonl\npartition:\n  kind: integer\n  from: 0\n  to: 99\n  chunk_size: 10\n  bounds: inclusive\nmatrix:\n  - id: a\n    partition:\n      kind: integer\n      from: 0\n      to: 4\n      chunk_size: 5\n      bounds: inclusive\n"
        );
        let nodes = expand(&cfg(&yaml)).unwrap();
        assert_eq!(
            nodes.len(),
            1,
            "the row's own 5-wide range wins over 100/10"
        );
    }
}
