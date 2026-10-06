//! Data-flow policies (#702) — the CLI layer over [`faucet_core::policy`].
//!
//! A policy comes from a `--policy <file>`, a top-level `policy:` block (which
//! is where a deployment overlay's `policy:` lands), or both merged. The
//! **static pass** ([`evaluate_nodes`]) runs before any data moves — in
//! `faucet validate` / `plan` / `doctor` (report), `faucet run` and the serve
//! submit path (refuse), template registration (warn) — over each expanded
//! row: the columns the row is known to carry (its data contract, or a
//! schema supplied by the caller: a `plan --sample`, the catalog's recorded
//! source schema) are labelled by name, the labels follow the row's transform
//! chain through the same column-lineage ops OpenLineage emission uses (an
//! opaque transform carries every input label **conservatively**), a column
//! the row's masking policy provably rewrites counts as masked, and the pure
//! [`faucet_core::policy::evaluate()`] decides. The **runtime backstop** is the
//! [`faucet_core::PolicySink`] decorator the executor installs, which
//! classifies the real records (names and value detectors) with the same rules.
//!
//! - [`metrics`] — `faucet_policy_violations_total{pipeline,row,rule,phase,action}`.

pub mod metrics;

use crate::config::PipelineConfig;
use crate::error::{CliError, CliResult};
use crate::expand::ExpandedNode;
use faucet_core::policy::{ColumnFacts, CompiledPolicy, PolicySpec, SinkFacts, Violation};
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;

/// Load a policy file (`.yaml` / `.yml` / `.json`). A file that fails to
/// parse or validate is an error — a policy fails closed.
pub fn load_file(path: &Path) -> CliResult<PolicySpec> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| CliError::Config(format!("policy: cannot read {}: {e}", path.display())))?;
    let spec: PolicySpec = match path.extension().and_then(|e| e.to_str()) {
        Some("json") => serde_json::from_str(&text)
            .map_err(|e| CliError::Config(format!("policy: {}: {e}", path.display())))?,
        _ => serde_yaml::from_str(&text)
            .map_err(|e| CliError::Config(format!("policy: {}: {e}", path.display())))?,
    };
    spec.validate()
        .map_err(|e| CliError::Config(format!("policy: {}: {e}", path.display())))?;
    Ok(spec)
}

/// The effective policy for a config: its own `policy:` block merged with the
/// `--policy` file (both validated; a rule name on both sides is an error).
/// `None` when neither is present.
pub fn resolve(inline: Option<&PolicySpec>, file: Option<&Path>) -> CliResult<Option<PolicySpec>> {
    let from_file = match file {
        Some(p) => Some(load_file(p)?),
        None => None,
    };
    let inline = match inline {
        Some(spec) => {
            spec.validate()
                .map_err(|e| CliError::Config(format!("policy: {e}")))?;
            Some(spec.clone())
        }
        None => None,
    };
    Ok(match (inline, from_file) {
        (Some(a), Some(b)) => Some(
            a.merge(b)
                .map_err(|e| CliError::Config(format!("policy: {e}")))?,
        ),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    })
}

/// Apply `--policy` on top of the config's own block, in place. Used by every
/// command that loads a config, so the executor and the reports see one policy.
pub fn apply_to_config(cfg: &mut PipelineConfig, file: Option<&Path>) -> CliResult<()> {
    cfg.policy = resolve(cfg.policy.as_ref(), file)?;
    Ok(())
}

/// Where the static pass learned a row's columns from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnSource {
    /// A schema supplied by the caller (a plan sample, the catalog).
    Schema,
    /// The row's data contract fields.
    Contract,
    /// Nothing is known statically — the runtime backstop enforces.
    None,
}

/// One row's static verdict.
#[derive(Debug, Clone, Serialize)]
pub struct RowPolicyReport {
    pub row: String,
    pub sink: String,
    pub sink_kind: String,
    pub attributes: BTreeMap<String, String>,
    pub column_source: ColumnSource,
    /// Columns known at the sink (after the transform chain).
    pub known_columns: usize,
    /// The labelled columns heading into the sink.
    pub columns: Vec<ColumnFacts>,
    /// The transform chain hides the column mapping (labels carried
    /// conservatively).
    pub opaque: bool,
    pub violations: Vec<Violation>,
}

/// The static pass over a whole config.
#[derive(Debug, Clone, Serialize)]
pub struct PolicyReport {
    pub rules: usize,
    pub classifications: usize,
    pub value_detectors: bool,
    pub rows: Vec<RowPolicyReport>,
    pub violations: usize,
}

impl PolicyReport {
    pub fn violated(&self) -> bool {
        self.violations > 0
    }

    pub fn all_violations(&self) -> impl Iterator<Item = &Violation> {
        self.rows.iter().flat_map(|r| r.violations.iter())
    }

    /// The typed refusal for `faucet run` / the serve submit path.
    pub fn error(&self) -> CliError {
        CliError::PolicyViolations {
            violations: self.violations,
        }
    }
}

/// Evaluate the policy statically over every expanded row. `schemas` supplies
/// a row's **input** column set when the caller knows it (row id → an
/// `infer_schema`-shaped `{"type":"object","properties":{…}}`); otherwise the
/// row's data contract is used, else nothing is known and only the runtime
/// backstop applies.
pub fn evaluate_nodes(
    spec: &PolicySpec,
    nodes: &[ExpandedNode],
    schemas: &HashMap<String, Value>,
) -> CliResult<PolicyReport> {
    let compiled =
        CompiledPolicy::compile(spec).map_err(|e| CliError::Config(format!("policy: {e}")))?;
    let mut rows = Vec::new();
    let mut total = 0;
    for node in nodes {
        if matches!(node.role, crate::expand::NodeRole::Discovery { .. }) {
            continue;
        }
        let row = evaluate_node(&compiled, node, schemas.get(&node.id));
        total += row.violations.len();
        rows.push(row);
    }
    Ok(PolicyReport {
        rules: spec.rules.len(),
        classifications: spec.classifications.len(),
        value_detectors: spec.has_value_detectors(),
        rows,
        violations: total,
    })
}

/// The static verdict for one row.
pub fn evaluate_node(
    policy: &CompiledPolicy,
    node: &ExpandedNode,
    input_schema: Option<&Value>,
) -> RowPolicyReport {
    let (inputs, column_source) = input_columns(node, input_schema);
    let (columns, known, opaque) = if column_source == ColumnSource::Contract {
        labelled_outputs(policy, node, &inputs)
    } else {
        labelled_columns(policy, node, &inputs)
    };
    let facts = SinkFacts {
        id: node.sink_ref.clone(),
        kind: node.sink.kind.clone(),
        attributes: node.sink.attributes.clone(),
    };
    let violations = faucet_core::policy::evaluate(policy, &facts, &columns);
    RowPolicyReport {
        row: node.id.clone(),
        sink: node.sink_ref.clone(),
        sink_kind: node.sink.kind.clone(),
        attributes: node.sink.attributes.clone(),
        column_source,
        known_columns: known,
        columns,
        opaque,
        violations,
    }
}

/// The row's known columns: the supplied schema's properties (the row's
/// input), else the contract's fields (its **output** — a contract is enforced
/// after the transforms), else nothing.
fn input_columns(node: &ExpandedNode, schema: Option<&Value>) -> (Vec<String>, ColumnSource) {
    if let Some(props) = schema
        .and_then(|s| s.get("properties"))
        .and_then(Value::as_object)
    {
        return (props.keys().cloned().collect(), ColumnSource::Schema);
    }
    #[cfg(feature = "contract")]
    if let Some(c) = &node.contract {
        return (
            c.fields.iter().map(|f| f.name.clone()).collect(),
            ColumnSource::Contract,
        );
    }
    #[cfg(not(feature = "contract"))]
    let _ = node;
    (Vec::new(), ColumnSource::None)
}

/// Push the input columns through the row's transform chain and label the
/// result. Returns `(labelled columns, known column count, opaque)`.
fn labelled_columns(
    policy: &CompiledPolicy,
    node: &ExpandedNode,
    inputs: &[String],
) -> (Vec<ColumnFacts>, usize, bool) {
    let masking_present = masking_present(node);
    let ops = crate::lineage_glue::column_ops(&node.transforms, masking_present);
    label_columns(policy, inputs, Some(&ops), &|column| {
        masked_action(node, column)
    })
}

/// Label a row's contract fields. They are the transforms' **outputs**, so
/// each is traced back through the chain to the input it came from, and a
/// renamed column (`ssn` → `tax_id`) keeps the labels of its input name.
fn labelled_outputs(
    policy: &CompiledPolicy,
    node: &ExpandedNode,
    outputs: &[String],
) -> (Vec<ColumnFacts>, usize, bool) {
    let masking_present = masking_present(node);
    let ops = crate::lineage_glue::column_ops(&node.transforms, masking_present);
    label_outputs(policy, outputs, Some(&ops), &|column| {
        masked_action(node, column)
    })
}

/// Where an output column came from, walking `ops` backwards.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Origin {
    /// This input column (possibly under another name).
    Input(String),
    /// A literal the chain set; no input feeds it.
    Literal,
    /// An opaque step hides it.
    Unknown,
}

fn origin_of(output: &str, ops: &[faucet_lineage::ColumnOp]) -> Origin {
    use faucet_lineage::ColumnOp;
    let mut name = output.to_string();
    for op in ops.iter().rev() {
        match op {
            ColumnOp::Opaque => return Origin::Unknown,
            ColumnOp::Set(keys) if keys.contains(&name) => return Origin::Literal,
            ColumnOp::Rename(pairs) => {
                if let Some((old, _)) = pairs.iter().find(|(_, new)| *new == name) {
                    name = old.clone();
                }
            }
            _ => {}
        }
    }
    Origin::Input(name)
}

/// The pure step behind [`labelled_outputs`]: `ops` is the chain (`None` =
/// unknown); an output whose origin is hidden is labelled by its own name and
/// marked conservative.
fn label_outputs(
    policy: &CompiledPolicy,
    outputs: &[String],
    ops: Option<&[faucet_lineage::ColumnOp]>,
    masked: &dyn Fn(&str) -> Option<String>,
) -> (Vec<ColumnFacts>, usize, bool) {
    let mut out = Vec::new();
    let mut opaque = ops.is_none();
    for output in outputs {
        let origin = match ops {
            Some(ops) => origin_of(output, ops),
            None => Origin::Unknown,
        };
        let mut labels = policy.labels_for_name(output);
        let mut via = "name";
        let mut conservative = false;
        match &origin {
            Origin::Input(input) if input != output => {
                let inherited = policy.labels_for_name(input);
                if !inherited.is_empty() {
                    via = "lineage";
                }
                labels.extend(inherited);
            }
            Origin::Unknown => {
                opaque = true;
                conservative = true;
                via = "conservative";
            }
            Origin::Input(_) | Origin::Literal => {}
        }
        if labels.is_empty() {
            continue;
        }
        out.push(ColumnFacts {
            name: output.clone(),
            labels,
            masked: masked(output),
            conservative,
            via: Some(via.to_string()),
        });
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    (out, outputs.len(), opaque)
}

/// Label a topology's contract fields. The path from a source to a sink is
/// not one chain, so an output inherits the labels of every name any transform
/// node renames into it (transitively) — conservatively — and a graph with
/// transform nodes is reported opaque.
fn label_graph_outputs(
    policy: &CompiledPolicy,
    outputs: &[String],
    renames: &[(String, String)],
    has_transforms: bool,
    masked: &dyn Fn(&str) -> Option<String>,
) -> (Vec<ColumnFacts>, usize, bool) {
    let mut out = Vec::new();
    for output in outputs {
        let mut labels = policy.labels_for_name(output);
        let mut seen = BTreeSet::from([output.clone()]);
        let mut frontier = vec![output.clone()];
        let mut inherited = false;
        while let Some(name) = frontier.pop() {
            for (old, _) in renames.iter().filter(|(_, new)| *new == name) {
                if seen.insert(old.clone()) {
                    let from = policy.labels_for_name(old);
                    inherited |= !from.is_empty();
                    labels.extend(from);
                    frontier.push(old.clone());
                }
            }
        }
        if labels.is_empty() {
            continue;
        }
        let via = if inherited {
            "lineage"
        } else if has_transforms {
            "conservative"
        } else {
            "name"
        };
        out.push(ColumnFacts {
            name: output.clone(),
            labels,
            masked: masked(output),
            conservative: has_transforms,
            via: Some(via.to_string()),
        });
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    (out, outputs.len(), has_transforms)
}

/// The policy the runtime backstop ([`faucet_core::PolicySink`]) enforces for
/// `node`. It classifies records by their **output** names, so every column the
/// row's transforms rename also carries the labels of the name it came from.
pub fn runtime_spec(spec: &PolicySpec, node: &ExpandedNode) -> PolicySpec {
    let Ok(compiled) = CompiledPolicy::compile(spec) else {
        return spec.clone();
    };
    let ops = crate::lineage_glue::column_ops(&node.transforms, masking_present(node));
    let mut out = spec.clone();
    out.classifications.extend(rename_aliases(&compiled, &ops));
    out
}

/// The runtime-backstop policy for a topology graph: every column a transform
/// node renames carries the labels of the name it came from, taken across all
/// transform nodes so a label can only be added, never lost.
pub fn topology_runtime_spec(spec: &PolicySpec, cfg: &PipelineConfig) -> PolicySpec {
    use crate::config::NodeSpec;
    let Ok(compiled) = CompiledPolicy::compile(spec) else {
        return spec.clone();
    };
    let mut ids: Vec<&String> = cfg.pipeline.nodes.keys().collect();
    ids.sort();
    let ops: Vec<faucet_lineage::ColumnOp> = ids
        .into_iter()
        .filter_map(|id| match &cfg.pipeline.nodes[id] {
            NodeSpec::Transform { transforms, .. } => Some(transforms),
            _ => None,
        })
        .flat_map(|t| crate::lineage_glue::column_ops(t, false))
        .collect();
    let mut out = spec.clone();
    out.classifications.extend(rename_aliases(&compiled, &ops));
    out
}

/// One extra classification per (renamed column, inherited label).
fn rename_aliases(
    policy: &CompiledPolicy,
    ops: &[faucet_lineage::ColumnOp],
) -> Vec<faucet_core::policy::Classification> {
    let mut aliases: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (k, op) in ops.iter().enumerate() {
        let faucet_lineage::ColumnOp::Rename(pairs) = op else {
            continue;
        };
        for (old, new) in pairs {
            let mut labels = policy.labels_for_name(old);
            if let Origin::Input(input) = origin_of(new, &ops[..=k]) {
                labels.extend(policy.labels_for_name(&input));
            }
            if !labels.is_empty() {
                aliases.entry(new.clone()).or_default().extend(labels);
            }
        }
    }
    aliases
        .into_iter()
        .flat_map(|(field, labels)| {
            labels
                .into_iter()
                .map(move |label| faucet_core::policy::Classification {
                    label,
                    description: None,
                    fields: vec![field.clone()],
                    field_pattern: None,
                    value_detector: None,
                })
        })
        .collect()
}

/// The pure labelling step: `ops` is the column-lineage chain (`None` when the
/// chain is unknown, so every labelled input is carried conservatively) and
/// `masked` says which mask action provably rewrites a column at this sink.
fn label_columns(
    policy: &CompiledPolicy,
    inputs: &[String],
    ops: Option<&[faucet_lineage::ColumnOp]>,
    masked: &dyn Fn(&str) -> Option<String>,
) -> (Vec<ColumnFacts>, usize, bool) {
    let mut out = Vec::new();
    let lineage = ops.and_then(|ops| faucet_lineage::derive_column_lineage(inputs, ops));
    match lineage {
        Some(lineage) => {
            for (output, sources) in &lineage.edges {
                let mut labels: BTreeSet<String> = policy.labels_for_name(output);
                let mut via = "name";
                for src in sources {
                    let from_src = policy.labels_for_name(src);
                    if !from_src.is_empty() && src != output {
                        via = "lineage";
                    }
                    labels.extend(from_src);
                }
                if labels.is_empty() {
                    continue;
                }
                out.push(ColumnFacts {
                    name: output.clone(),
                    labels,
                    masked: masked(output),
                    conservative: false,
                    via: Some(via.to_string()),
                });
            }
            // The lineage map's iteration order depends on the serde_json
            // feature set; the report is user-facing, so sort it.
            out.sort_by(|a, b| a.name.cmp(&b.name));
            (out, lineage.edges.len(), false)
        }
        None => {
            // Opaque chain: every labelled input may still be present under
            // some name, so each label is carried conservatively.
            for input in inputs {
                let labels = policy.labels_for_name(input);
                if labels.is_empty() {
                    continue;
                }
                out.push(ColumnFacts {
                    name: input.clone(),
                    labels,
                    masked: masked(input),
                    conservative: true,
                    via: Some("conservative".to_string()),
                });
            }
            (out, inputs.len(), true)
        }
    }
}

#[cfg(feature = "masking")]
fn masking_present(node: &ExpandedNode) -> bool {
    node.masking.is_some()
}
#[cfg(not(feature = "masking"))]
fn masking_present(_node: &ExpandedNode) -> bool {
    false
}

/// The mask action the row's masking policy provably applies to `column` at
/// this row's sink (name-matched rules only — a value-detector rule masks
/// values, which the runtime backstop then no longer classifies).
#[cfg(feature = "masking")]
pub fn masked_action(node: &ExpandedNode, column: &str) -> Option<String> {
    masked_action_for(
        node.masking.as_ref(),
        &[node.sink_ref.as_str(), node.sink.kind.as_str()],
        column,
    )
}

/// [`masked_action`] over an explicit masking spec + the sink's ids (its
/// template / node id and connector kind) — shared with topology mode. Uses
/// the same matching as the masking pass itself, so a column is reported
/// masked exactly when the pass rewrites it (directly or through an ancestor).
#[cfg(feature = "masking")]
pub fn masked_action_for(
    spec: Option<&faucet_core::MaskingSpec>,
    sink_ids: &[&str],
    column: &str,
) -> Option<String> {
    faucet_core::CompiledMasking::compile_for_sink(spec?, sink_ids)
        .ok()?
        .name_action_for(column)
        .map(str::to_string)
}
#[cfg(not(feature = "masking"))]
pub fn masked_action(_node: &ExpandedNode, _column: &str) -> Option<String> {
    None
}

/// The static pass over a **topology** config (`pipeline.nodes` / `edges`,
/// #71): one verdict per sink node. Columns come from the pipeline-level data
/// contract; the graph's transform nodes are not column-lineage-analysed, so
/// when any transform node exists every labelled input is carried
/// conservatively (a rename cannot hide a label). Sink attributes come from the
/// node's resolved sink template.
pub fn evaluate_topology(spec: &PolicySpec, cfg: &PipelineConfig) -> CliResult<PolicyReport> {
    use crate::config::NodeSpec;
    let compiled =
        CompiledPolicy::compile(spec).map_err(|e| CliError::Config(format!("policy: {e}")))?;
    let has_transforms = cfg
        .pipeline
        .nodes
        .values()
        .any(|n| matches!(n, NodeSpec::Transform { .. }));
    #[cfg(feature = "contract")]
    let (inputs, column_source): (Vec<String>, ColumnSource) = match &cfg.pipeline.contract {
        Some(c) => (
            c.fields.iter().map(|f| f.name.clone()).collect(),
            ColumnSource::Contract,
        ),
        None => (Vec::new(), ColumnSource::None),
    };
    #[cfg(not(feature = "contract"))]
    let (inputs, column_source): (Vec<String>, ColumnSource) = (Vec::new(), ColumnSource::None);
    let graph_renames: Vec<(String, String)> = cfg
        .pipeline
        .nodes
        .values()
        .filter_map(|n| match n {
            NodeSpec::Transform { transforms, .. } => Some(transforms),
            _ => None,
        })
        .flat_map(|t| crate::lineage_glue::column_ops(t, false))
        .filter_map(|op| match op {
            faucet_lineage::ColumnOp::Rename(pairs) => Some(pairs),
            _ => None,
        })
        .flatten()
        .collect();
    let mut ids: Vec<&String> = cfg.pipeline.nodes.keys().collect();
    ids.sort();
    let mut rows = Vec::new();
    let mut total = 0;
    for id in ids {
        let NodeSpec::Sink { template, kind, .. } = &cfg.pipeline.nodes[id] else {
            continue;
        };
        let template_name = template.as_deref().unwrap_or("default");
        let base = if template_name == "default" {
            cfg.pipeline
                .sinks
                .get("default")
                .or(cfg.pipeline.sink.as_ref())
        } else {
            cfg.pipeline.sinks.get(template_name)
        };
        let sink_kind = kind
            .clone()
            .or_else(|| base.map(|b| b.kind.clone()))
            .unwrap_or_default();
        let attributes = base.map(|b| b.attributes.clone()).unwrap_or_default();
        let sink_ids = [id.as_str(), template_name, sink_kind.as_str()];
        #[cfg(feature = "masking")]
        let masked =
            |column: &str| masked_action_for(cfg.pipeline.masking.as_ref(), &sink_ids, column);
        #[cfg(not(feature = "masking"))]
        let masked = |_column: &str| -> Option<String> {
            let _ = &sink_ids;
            None
        };
        let (columns, known, opaque) = if column_source == ColumnSource::Contract {
            label_graph_outputs(&compiled, &inputs, &graph_renames, has_transforms, &masked)
        } else {
            let ops: Vec<faucet_lineage::ColumnOp> = Vec::new();
            label_columns(
                &compiled,
                &inputs,
                if has_transforms { None } else { Some(&ops) },
                &masked,
            )
        };
        let facts = SinkFacts {
            id: id.clone(),
            kind: sink_kind.clone(),
            attributes: attributes.clone(),
        };
        let violations = faucet_core::policy::evaluate(&compiled, &facts, &columns);
        total += violations.len();
        rows.push(RowPolicyReport {
            row: id.clone(),
            sink: template_name.to_string(),
            sink_kind,
            attributes,
            column_source,
            known_columns: known,
            columns,
            opaque,
            violations,
        });
    }
    Ok(PolicyReport {
        rules: spec.rules.len(),
        classifications: spec.classifications.len(),
        value_detectors: spec.has_value_detectors(),
        rows,
        violations: total,
    })
}

/// The `faucet doctor` probes for one root row (role `policy`): one probe
/// per rule the row violates statically, or a single pass.
pub fn doctor_probes(spec: &PolicySpec, node: &ExpandedNode) -> Vec<faucet_core::Probe> {
    use faucet_core::Probe;
    let t = std::time::Instant::now();
    let compiled = match CompiledPolicy::compile(spec) {
        Ok(c) => c,
        Err(e) => return vec![Probe::fail("policy", t.elapsed(), format!("policy: {e}"))],
    };
    let row = evaluate_node(&compiled, node, None);
    if row.violations.is_empty() {
        return vec![Probe::pass("policy", t.elapsed())];
    }
    row.violations
        .iter()
        .map(|v| Probe::fail("policy", t.elapsed(), v.to_string()))
        .collect()
}

/// Whether any rule quarantines at run time (which needs a `dlq:` block).
pub fn quarantines(spec: &PolicySpec) -> bool {
    spec.rules
        .iter()
        .any(|r| r.on_runtime == faucet_core::policy::RuntimeAction::Quarantine)
}

/// Count the static violations into the shared metric.
pub fn record_metrics(pipeline: &str, report: &PolicyReport) {
    for row in &report.rows {
        for v in &row.violations {
            metrics::record_violation(pipeline, &row.row, &v.rule, "static", "refuse");
        }
    }
}

/// The human report: one line per row, then each violation.
pub fn render_human(report: &PolicyReport) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "policy: {} rule(s), {} classification(s){} — {}\n",
        report.rules,
        report.classifications,
        if report.value_detectors {
            ", value detectors enforced at run time"
        } else {
            ""
        },
        if report.violated() {
            format!("{} violation(s)", report.violations)
        } else {
            "no violations".to_string()
        }
    ));
    for row in &report.rows {
        let attrs = if row.attributes.is_empty() {
            "no attributes".to_string()
        } else {
            row.attributes
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let knowledge = match row.column_source {
            ColumnSource::Schema => format!("{} column(s) from a schema", row.known_columns),
            ColumnSource::Contract => format!("{} column(s) from the contract", row.known_columns),
            ColumnSource::None => {
                "no columns known statically; the runtime backstop enforces".to_string()
            }
        };
        out.push_str(&format!(
            "  row {} → sink {} ({}; {attrs}): {knowledge}{}{}\n",
            row.row,
            row.sink,
            row.sink_kind,
            if row.opaque {
                "; opaque transform, labels carried conservatively"
            } else {
                ""
            },
            if row.columns.is_empty() {
                String::new()
            } else {
                format!(
                    "; labelled: {}",
                    row.columns
                        .iter()
                        .map(|c| format!(
                            "{}[{}]{}",
                            c.name,
                            c.labels.iter().cloned().collect::<Vec<_>>().join(","),
                            c.masked
                                .as_deref()
                                .map(|m| format!(" masked:{m}"))
                                .unwrap_or_default()
                        ))
                        .collect::<Vec<_>>()
                        .join(" ")
                )
            }
        ));
        for v in &row.violations {
            out.push_str(&format!("    ! {v}\n"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expand::expand;
    use serde_json::json;

    fn mk_cfg(extra: &str) -> PipelineConfig {
        PipelineConfig::from_text(
            &format!(
                r#"version: 1
name: p
pipeline:
  source: {{ type: csv, config: {{ path: in.csv }} }}
  sink:
    type: jsonl
    config: {{ path: out.jsonl }}
    attributes: {{ residency: us, environment: prod }}
{extra}
"#
            ),
            Path::new("p.yaml"),
        )
        .unwrap()
    }

    fn policy() -> PolicySpec {
        serde_json::from_value(json!({
            "classifications": [
                {"label": "pii", "fields": ["email"], "value_detector": "email"},
                {"label": "finance", "field_pattern": "^amount"}
            ],
            "rules": [
                {"name": "pii-eu", "when": {"label": "pii"}, "require": {"residency": ["eu"]}, "mask": ["hash"]},
                {"name": "finance-no-prod-files", "when": {"label": "finance", "sink_kind": ["jsonl"], "sink": {"environment": ["prod"]}}, "deny": true, "on_runtime": "quarantine"}
            ]
        }))
        .unwrap()
    }

    #[test]
    fn contract_columns_are_labelled_and_evaluated() {
        let cfg = mk_cfg(
            "  contract:\n    version: \"1\"\n    fields:\n      - { name: id, type: integer }\n      - { name: email, type: string }\n      - { name: amount_cents, type: integer }\n",
        );
        let nodes = expand(&cfg).unwrap();
        let report = evaluate_nodes(&policy(), &nodes, &HashMap::new()).unwrap();
        assert_eq!(report.violations, 2, "{report:?}");
        let row = &report.rows[0];
        assert_eq!(row.column_source, ColumnSource::Contract);
        assert_eq!(row.known_columns, 3);
        assert!(!row.opaque);
        let names: Vec<&str> = row.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["amount_cents", "email"]);
        assert!(
            row.violations
                .iter()
                .any(|v| v.rule == "pii-eu" && v.column == "email")
        );
        assert!(
            row.violations
                .iter()
                .any(|v| v.rule == "finance-no-prod-files")
        );
        let text = render_human(&report);
        assert!(
            text.contains("2 violation(s)") && text.contains("! rule `pii-eu`"),
            "{text}"
        );
        assert!(matches!(
            report.error(),
            CliError::PolicyViolations { violations: 2 }
        ));
        assert!(quarantines(&policy()));
    }

    #[test]
    fn labels_follow_renames_and_masking_satisfies() {
        // A contract describes the transformed output, so it lists the
        // renamed column.
        let cfg = mk_cfg(
            "  contract:\n    version: \"1\"\n    fields:\n      - { name: contact, type: string }\n  transforms:\n    - type: rename_field\n      config: { fields: { email: contact } }\n",
        );
        let nodes = expand(&cfg).unwrap();
        let report = evaluate_nodes(&policy(), &nodes, &HashMap::new()).unwrap();
        let row = &report.rows[0];
        assert_eq!(row.columns[0].name, "contact");
        assert_eq!(row.columns[0].via.as_deref(), Some("lineage"));
        assert!(row.columns[0].labels.contains("pii"));
        assert_eq!(report.violations, 1);

        let masked = mk_cfg(
            "  contract:\n    version: \"1\"\n    fields:\n      - { name: email, type: string }\n  masking:\n    rules:\n      - name: m\n        match: { fields: [email] }\n        action: { type: hash }\n",
        );
        let nodes = expand(&masked).unwrap();
        let report = evaluate_nodes(&policy(), &nodes, &HashMap::new()).unwrap();
        assert_eq!(report.rows[0].columns[0].masked.as_deref(), Some("hash"));
        assert_eq!(report.violations, 0, "{report:?}");
        let redacted = mk_cfg(
            "  contract:\n    version: \"1\"\n    fields:\n      - { name: email, type: string }\n  masking:\n    rules:\n      - name: m\n        match: { field_pattern: '^em' }\n        action: { type: partial }\n        applies_to: [postgres]\n",
        );
        let nodes = expand(&redacted).unwrap();
        let report = evaluate_nodes(&policy(), &nodes, &HashMap::new()).unwrap();
        assert!(
            report.rows[0].columns[0].masked.is_none(),
            "rule scoped to another sink"
        );
        assert_eq!(report.violations, 1);
    }

    #[cfg(feature = "masking")]
    #[test]
    fn masked_action_for_agrees_with_the_masking_pass() {
        let spec: faucet_core::MaskingSpec = serde_json::from_value(json!({
            "rules": [
                { "match": { "fields": ["phones"] }, "action": { "type": "hash" } },
                { "match": { "field_pattern": "^card$" }, "action": { "type": "tokenize" } },
                { "match": { "fields": ["ssn"] }, "action": { "type": "partial" } }
            ]
        }))
        .unwrap();
        let at = |c: &str| masked_action_for(Some(&spec), &["default", "jsonl"], c);
        assert_eq!(at("phones").as_deref(), Some("hash"));
        assert_eq!(at("phones.0").as_deref(), Some("hash"));
        assert_eq!(at("card.number").as_deref(), Some("tokenize"));
        assert_eq!(at("ssn").as_deref(), Some("partial"));
        assert_eq!(
            at("user.ssn"),
            None,
            "the pass matches `fields` on the full path"
        );
        assert_eq!(masked_action_for(None, &["default"], "phones"), None);

        let rows = vec![
            json!({"phones": ["+14155552671"], "card": {"number": "4111111111111111"}, "user": {"ssn": "123-45-6789"}}),
        ];
        let compiled =
            faucet_core::CompiledMasking::compile_for_sink(&spec, &["default", "jsonl"]).unwrap();
        let out = faucet_core::masking::apply_masking(rows, &compiled).records;
        assert_ne!(out[0]["phones"][0], json!("+14155552671"));
        assert_ne!(out[0]["card"]["number"], json!("4111111111111111"));
        assert_eq!(out[0]["user"]["ssn"], json!("123-45-6789"));
    }

    fn topology_cfg(extra_nodes: &str, extra: &str) -> PipelineConfig {
        PipelineConfig::from_text(
            &format!(
                r#"version: 1
name: p
pipeline:
  contract:
    version: "1"
    fields:
      - {{ name: id, type: integer }}
      - {{ name: email, type: string }}
      - {{ name: amount_cents, type: integer }}
  sources:
    a: {{ type: csv, config: {{ path: /tmp/a.csv }} }}
  sinks:
    o:
      type: jsonl
      config: {{ path: /tmp/o.jsonl }}
      attributes: {{ residency: us, environment: prod }}
  nodes:
    s: {{ kind: source, ref: a }}
    w: {{ kind: sink, ref: o }}
    x: {{ kind: sink, type: stdout, config: {{}} }}
{extra_nodes}  edges:
    - {{ from: s, to: w }}
    - {{ from: s, to: x }}
{extra}
"#
            ),
            Path::new("p.yaml"),
        )
        .unwrap()
    }

    /// A node graph gets one verdict per sink node, from the contract's
    /// columns and each node's resolved sink template; an inline-typed sink
    /// node without a template has no attributes.
    #[test]
    fn topology_configs_are_evaluated_per_sink_node() {
        let cfg = topology_cfg("", "");
        let report = evaluate_topology(&policy(), &cfg).unwrap();
        let rows: Vec<(&str, &str, &str, usize)> = report
            .rows
            .iter()
            .map(|r| {
                (
                    r.row.as_str(),
                    r.sink.as_str(),
                    r.sink_kind.as_str(),
                    r.violations.len(),
                )
            })
            .collect();
        // `w` (jsonl, us, prod) trips both rules; `x` (stdout, no attributes)
        // only the residency requirement.
        assert_eq!(
            rows,
            vec![("w", "o", "jsonl", 2), ("x", "default", "stdout", 1)],
            "{report:?}"
        );
        assert_eq!(report.violations, 3);
        assert!(
            report
                .rows
                .iter()
                .all(|r| r.column_source == ColumnSource::Contract)
        );
        assert!(report.rows.iter().all(|r| !r.opaque));
        assert!(report.rows[1].attributes.is_empty());
        let text = render_human(&report);
        assert!(text.contains("no attributes"), "{text}");
        assert!(text.contains("from the contract"), "{text}");

        // A transform node makes the graph opaque: every labelled input is
        // carried conservatively rather than pushed through the chain.
        let cfg = topology_cfg(
            "    t: { kind: transform, transforms: [{ type: flatten, config: {} }] }\n",
            "",
        );
        let report = evaluate_topology(&policy(), &cfg).unwrap();
        assert!(report.rows.iter().all(|r| r.opaque), "{report:?}");
        assert!(report.rows[0].columns.iter().all(|c| c.conservative));

        // A masking rule scoped to the node's sink satisfies the mask option.
        let cfg = topology_cfg(
            "",
            "  masking:\n    rules:\n      - name: m\n        match: { fields: [email] }\n        action: { type: hash }\n        applies_to: [w]\n",
        );
        let report = evaluate_topology(&policy(), &cfg).unwrap();
        let w = &report.rows[0];
        assert_eq!(w.row, "w");
        let email = w.columns.iter().find(|c| c.name == "email").unwrap();
        assert_eq!(email.masked.as_deref(), Some("hash"));
        let x = &report.rows[1];
        assert!(
            x.columns
                .iter()
                .find(|c| c.name == "email")
                .unwrap()
                .masked
                .is_none()
        );

        // An uncompilable policy fails closed.
        let bad: PolicySpec = serde_json::from_value(json!({
            "classifications": [{"label": "pii", "field_pattern": "("}],
            "rules": []
        }))
        .unwrap();
        assert!(evaluate_topology(&bad, &cfg).is_err());
    }

    #[test]
    fn doctor_probes_report_each_static_violation() {
        let cfg = mk_cfg(
            "  contract:\n    version: \"1\"\n    fields:\n      - { name: email, type: string }\n",
        );
        let nodes = expand(&cfg).unwrap();
        let probes = doctor_probes(&policy(), &nodes[0]);
        assert_eq!(probes.len(), 1);
        assert!(matches!(
            probes[0].status,
            faucet_core::ProbeStatus::Fail { .. }
        ));
        let clean: PolicySpec = serde_json::from_value(json!({
            "classifications": [{"label": "pii", "fields": ["email"]}],
            "rules": [{"name": "r", "when": {"label": "pii"}, "require": {"residency": ["us"]}}]
        }))
        .unwrap();
        let probes = doctor_probes(&clean, &nodes[0]);
        assert!(matches!(probes[0].status, faucet_core::ProbeStatus::Pass));
        let bad: PolicySpec = serde_json::from_value(json!({
            "classifications": [{"label": "pii", "field_pattern": "("}],
            "rules": []
        }))
        .unwrap();
        let probes = doctor_probes(&bad, &nodes[0]);
        assert!(matches!(
            probes[0].status,
            faucet_core::ProbeStatus::Fail { .. }
        ));
    }

    #[test]
    fn a_renamed_pii_column_keeps_its_label_against_a_deny_rule() {
        let deny: PolicySpec = serde_json::from_value(json!({
            "classifications": [{"label": "pii", "fields": ["ssn"]}],
            "rules": [{"name": "no-pii-in-files", "when": {"label": "pii"}, "deny": true}]
        }))
        .unwrap();
        let cfg = mk_cfg(
            "  contract:\n    version: \"1\"\n    fields:\n      - { name: id, type: integer }\n      - { name: tax_id, type: string }\n  transforms:\n    - type: rename_field\n      config: { fields: { ssn: tax_id } }\n",
        );
        let nodes = expand(&cfg).unwrap();
        let report = evaluate_nodes(&deny, &nodes, &HashMap::new()).unwrap();
        assert_eq!(report.violations, 1, "{report:?}");
        assert_eq!(report.rows[0].violations[0].column, "tax_id");
        assert!(!report.rows[0].opaque);

        // The runtime backstop classifies records by their output names, so it
        // gets the alias too.
        let runtime = CompiledPolicy::compile(&runtime_spec(&deny, &nodes[0])).unwrap();
        assert!(runtime.labels_for_name("tax_id").contains("pii"));
        assert!(runtime.labels_for_name("id").is_empty());
        // An uncompilable policy is handed back unchanged (the caller's own
        // compile reports it).
        let bad: PolicySpec = serde_json::from_value(json!({
            "classifications": [{"label": "pii", "field_pattern": "("}],
            "rules": []
        }))
        .unwrap();
        assert_eq!(runtime_spec(&bad, &nodes[0]), bad);
    }

    #[test]
    fn origins_follow_renames_backwards_and_stop_at_literals_and_opaque_steps() {
        use faucet_lineage::ColumnOp;
        let ops = vec![
            ColumnOp::Rename(vec![("a".into(), "b".into())]),
            ColumnOp::Identity,
            ColumnOp::Rename(vec![("b".into(), "c".into())]),
            ColumnOp::Set(vec!["lit".into()]),
        ];
        assert_eq!(origin_of("c", &ops), Origin::Input("a".into()));
        assert_eq!(origin_of("z", &ops), Origin::Input("z".into()));
        assert_eq!(origin_of("lit", &ops), Origin::Literal);
        let opaque = vec![ColumnOp::Opaque, ColumnOp::Identity];
        assert_eq!(origin_of("c", &opaque), Origin::Unknown);

        let policy = CompiledPolicy::compile(
            &serde_json::from_value(json!({
                "classifications": [{"label": "pii", "fields": ["a"]}, {"label": "x", "fields": ["lit"]}],
                "rules": []
            }))
            .unwrap(),
        )
        .unwrap();
        let none = |_: &str| None;
        let outputs = vec!["c".to_string(), "lit".to_string(), "plain".to_string()];
        let (cols, known, opaque) = label_outputs(&policy, &outputs, Some(&ops), &none);
        assert_eq!((known, opaque), (3, false));
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "c");
        assert_eq!(cols[0].via.as_deref(), Some("lineage"));
        assert_eq!(cols[1].via.as_deref(), Some("name"));
        let (cols, _, opaque) = label_outputs(&policy, &outputs, None, &none);
        assert!(opaque);
        assert_eq!(cols.len(), 1, "an unknown chain keeps only name labels");
        assert!(cols[0].conservative);

        let aliases = rename_aliases(&policy, &ops);
        let fields: Vec<&str> = aliases.iter().map(|c| c.fields[0].as_str()).collect();
        assert_eq!(
            fields,
            ["b", "c"],
            "every renamed name inherits `a`'s label"
        );
    }

    #[test]
    fn a_topology_renamed_column_inherits_labels_conservatively() {
        let deny: PolicySpec = serde_json::from_value(json!({
            "classifications": [{"label": "pii", "fields": ["ssn"]}],
            "rules": [{"name": "no-pii", "when": {"label": "pii"}, "deny": true}]
        }))
        .unwrap();
        let cfg = PipelineConfig::from_text(
            r#"version: 1
name: p
pipeline:
  contract:
    version: "1"
    fields:
      - { name: tax_id, type: string }
  sources:
    a: { type: csv, config: { path: /tmp/a.csv } }
  sinks:
    o: { type: jsonl, config: { path: /tmp/o.jsonl } }
  nodes:
    s: { kind: source, ref: a }
    t: { kind: transform, transforms: [{ type: rename_field, config: { fields: { ssn: mid } } }] }
    u: { kind: transform, transforms: [{ type: rename_field, config: { fields: { mid: tax_id } } }] }
    w: { kind: sink, ref: o }
  edges:
    - { from: s, to: t }
    - { from: t, to: u }
    - { from: u, to: w }
"#,
            Path::new("p.yaml"),
        )
        .unwrap();
        let report = evaluate_topology(&deny, &cfg).unwrap();
        assert_eq!(report.violations, 1, "{report:?}");
        let col = &report.rows[0].columns[0];
        assert_eq!(col.name, "tax_id");
        assert!(col.conservative);
        assert_eq!(col.via.as_deref(), Some("lineage"));

        let runtime = CompiledPolicy::compile(&topology_runtime_spec(&deny, &cfg)).unwrap();
        assert!(runtime.labels_for_name("tax_id").contains("pii"));
        assert!(runtime.labels_for_name("mid").contains("pii"));
        let unchanged = topology_runtime_spec(&deny, &topology_cfg("", ""));
        assert_eq!(unchanged, deny);

        // Without a contract nothing is known statically.
        let no_contract = PipelineConfig::from_text(
            r#"version: 1
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
"#,
            Path::new("p.yaml"),
        )
        .unwrap();
        let report = evaluate_topology(&deny, &no_contract).unwrap();
        assert_eq!(report.rows[0].column_source, ColumnSource::None);
        assert!(report.rows[0].columns.is_empty());
        assert_eq!(report.violations, 0);
    }

    #[test]
    fn opaque_transforms_carry_labels_conservatively() {
        let cfg = mk_cfg(
            "  contract:\n    version: \"1\"\n    fields:\n      - { name: email, type: string }\n  transforms:\n    - type: flatten\n      config: {}\n",
        );
        let nodes = expand(&cfg).unwrap();
        let report = evaluate_nodes(&policy(), &nodes, &HashMap::new()).unwrap();
        let row = &report.rows[0];
        assert!(row.opaque);
        assert!(row.columns[0].conservative);
        assert!(row.violations[0].conservative);
        assert!(render_human(&report).contains("conservatively"));
    }

    #[test]
    fn a_supplied_schema_wins_and_no_knowledge_defers_to_runtime() {
        let cfg = mk_cfg("");
        let nodes = expand(&cfg).unwrap();
        let none = evaluate_nodes(&policy(), &nodes, &HashMap::new()).unwrap();
        assert_eq!(none.rows[0].column_source, ColumnSource::None);
        assert_eq!(none.violations, 0);
        assert!(render_human(&none).contains("runtime backstop"));
        let mut schemas = HashMap::new();
        schemas.insert(
            "row-0".to_string(),
            json!({"type": "object", "properties": {"email": {"type": "string"}, "id": {"type": "integer"}}}),
        );
        let with = evaluate_nodes(&policy(), &nodes, &schemas).unwrap();
        assert_eq!(with.rows[0].column_source, ColumnSource::Schema);
        assert_eq!(with.violations, 1);
        record_metrics("p", &with);
    }

    #[test]
    fn resolve_merges_file_and_inline_and_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("policy.yaml");
        std::fs::write(&file, "classifications:\n  - { label: health, fields: [diagnosis] }\nrules:\n  - { name: health-deny, when: { label: health }, deny: true }\n").unwrap();
        let merged = resolve(Some(&policy()), Some(&file)).unwrap().unwrap();
        assert_eq!(merged.rules.len(), 3);
        assert!(resolve(None, None).unwrap().is_none());
        assert_eq!(resolve(None, Some(&file)).unwrap().unwrap().rules.len(), 1);
        let bad = dir.path().join("bad.yaml");
        std::fs::write(
            &bad,
            "rules:\n  - { name: r, when: { label: nope }, deny: true }\n",
        )
        .unwrap();
        let err = resolve(None, Some(&bad)).unwrap_err().to_string();
        assert!(err.contains("no classification"), "{err}");
        let missing = dir.path().join("nope.yaml");
        assert!(
            resolve(None, Some(&missing))
                .unwrap_err()
                .to_string()
                .contains("cannot read")
        );
        let json = dir.path().join("policy.json");
        std::fs::write(&json, serde_json::to_string(&policy()).unwrap()).unwrap();
        assert_eq!(load_file(&json).unwrap().rules.len(), 2);
        let mut cfg = mk_cfg("");
        apply_to_config(&mut cfg, Some(&file)).unwrap();
        assert_eq!(cfg.policy.as_ref().unwrap().rules.len(), 1);
    }
}
