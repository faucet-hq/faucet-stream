//! A template's rows (#741): what a caller may select, described from data
//! faucet already computes — the template spec, the composed / expanded rows,
//! the capability registry, the derived delivery guarantee, and (optionally)
//! the `faucet status` report — plus a dry-run resolve of a selection.
//!
//! Rows come from expanding the template with **placeholder-bound params** (the
//! path `template register` validates with), so required params never block a
//! listing. A `source-template` is expanded through a probe composition that
//! keeps every stream; sink-dependent facts (write resolution, delivery
//! guarantee, cleanup) are added only when a sink template is given. The same
//! report backs `GET /v1/templates/{id}/rows`, `faucet template rows`,
//! `faucet hub rows` and the MCP `list_template_rows` tool.

use std::collections::{BTreeSet, HashMap};

use chrono::{DateTime, Utc};
use faucet_core::WriteMode;
use serde::Serialize;
use serde_json::{Value, json};

use super::compose::{StreamIncompatibility, StreamPlan, resolve_mode};
use super::spec::{DEFAULT_SOURCE, SinkTemplate, SourceTemplate, TemplateKind, WriteChoice};
use crate::config::{PipelineConfig, SourceStatus};
use crate::error::CliResult;
use crate::expand::{ExpandedNode, NodeRole};
use crate::params::{BindMode, SuppliedParams};
use crate::select::{Resolution, RowDecision, SelectionRequest};

/// Source kinds that can split one read across workers (`Source::is_shardable`
/// overrides; PK-range for the SQL sources needs `shard: { key }` config).
pub const SHARDABLE_SOURCE_KINDS: &[&str] = &[
    "dynamodb", "gcs", "iceberg", "kafka", "mssql", "mysql", "oracle", "parquet", "postgres", "s3",
    "spanner", "sqlite",
];

/// Transform kinds that change which field names reach the sink.
const RENAMING_TRANSFORMS: &[&str] = &["rename_field", "rename_keys", "keys_case", "flatten"];
/// Transform kinds that remove fields.
const DROPPING_TRANSFORMS: &[&str] = &["drop", "select"];

/// `GET /v1/templates/{id}/rows` and friends.
#[derive(Debug, Clone, Serialize)]
pub struct RowsReport {
    pub template: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<u32>,
    pub kind: String,
    /// `false` for a topology pipeline (`pipeline.nodes`), which has no rows.
    pub selectable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sink: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sink_version: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sink_kind: Option<String>,
    pub rows: Vec<TemplateRow>,
    /// The selection that was resolved (only with a selection).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selection: Option<SelectionRequest>,
    /// The rows a trigger with this selection runs, in execution order.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_set: Option<Vec<String>>,
    /// The error a trigger with this selection would return.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Why an optional group is missing (state unreadable, …).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
}

/// One selectable row.
#[derive(Debug, Clone, Serialize)]
pub struct TemplateRow {
    pub id: String,
    /// `stream` (a source-template stream) or `row` (a pipeline matrix row).
    pub kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub status: SourceStatus,
    pub tags: Vec<String>,
    /// Whether a run with no narrowing selector includes the row (the status
    /// gate: `mandatory` / `active`).
    pub default_selected: bool,
    pub parent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_key: Option<String>,
    pub children: Vec<String>,
    pub depends_on: Vec<String>,
    /// Execution level: 0 without ancestors, else one past the deepest.
    pub depth: usize,
    /// Runs once per record of its parent (a child fan-out).
    pub per_parent_record: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write: Option<RowWrite>,
    pub primary_keys: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delete_marker: Option<Value>,
    pub read: RowRead,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub guarantees: Option<RowGuarantees>,
    pub shape: RowShape,
    pub params_used: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<RowState>,
    /// Picked by the resolved selection itself.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected: Option<bool>,
    /// Added as a required ancestor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pulled_in: Option<PulledIn>,
    /// Needed by the run set but refused — why.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocked: Option<String>,
    /// Not in the run set — why.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub excluded: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PulledIn {
    pub because: String,
}

/// How the row writes.
#[derive(Debug, Clone, Serialize)]
pub struct RowWrite {
    /// The template's preference order.
    pub requested: Vec<WriteMode>,
    /// What the sink runs (with a sink).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolved: Option<WriteMode>,
    /// Whether the row can run against the sink (with a sink).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub supported: Option<bool>,
    /// The modes the sink offers (with a sink).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offered: Option<Vec<WriteMode>>,
    /// A `write_mode_aliases` substitution, `overwrite→append`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alias_applied: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unsupported_reason: Option<String>,
}

/// How the row reads.
#[derive(Debug, Clone, Serialize)]
pub struct RowRead {
    pub source_kind: String,
    pub replication: Replication,
    /// Keeps its position across runs: it bookmarks (incremental, or a CDC /
    /// stream source) and the row has a durable state store.
    pub resumable: bool,
    pub shardable: bool,
    pub supports_discover: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct Replication {
    /// `full` or `incremental`.
    pub method: &'static str,
    /// The bookmark / cursor field, when configured.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
}

/// End-to-end guarantees with the chosen sink.
#[derive(Debug, Clone, Serialize)]
pub struct RowGuarantees {
    /// The `faucet validate` derivation: `at-least-once`, or
    /// `effectively-once (atomic watermark | keyed upsert)`.
    pub delivery_guarantee: String,
    /// Whether scoped cleanup (`complete_for` + `delete_missing`) could run.
    pub cleanup_capable: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct RowShape {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<Value>,
    /// `contract` when `schema` comes from the pipeline's data contract.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_source: Option<&'static str>,
    pub transforms: TransformSummary,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct TransformSummary {
    pub count: usize,
    /// Transform kinds in the order they run (repeats kept once).
    pub kinds: Vec<String>,
    pub renames_fields: bool,
    pub drops_fields: bool,
}

/// What `faucet status` knows about the row.
#[derive(Debug, Clone, Serialize)]
pub struct RowState {
    pub health: crate::status::Health,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_success: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_failure: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bookmark_age_secs: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lag: Option<Value>,
}

/// Per-row facts only the template spec knows.
#[derive(Debug, Clone, Default)]
struct RowMeta {
    description: Option<String>,
    primary_keys: Vec<String>,
    write: Option<RowWrite>,
    params_used: Vec<String>,
    guarantee: Option<RowGuarantees>,
}

/// Bind every declared param to a placeholder and parse — the register-time
/// validation binding.
fn placeholder_config(doc: &Value) -> CliResult<PipelineConfig> {
    let mut probe = doc.clone();
    crate::params::bind_document(&mut probe, &SuppliedParams::new(), BindMode::Placeholder)?;
    PipelineConfig::from_value(probe)
}

/// The config's expanded rows, placeholder-bound. `None` for a topology.
fn placeholder_nodes(doc: &Value) -> CliResult<(PipelineConfig, Option<Vec<ExpandedNode>>)> {
    let cfg = placeholder_config(doc)?;
    if crate::topology::is_topology(&cfg) {
        return Ok((cfg, None));
    }
    let nodes = crate::expand::expand(&cfg)?;
    Ok((cfg, Some(nodes)))
}

fn probe_sink() -> SinkTemplate {
    serde_json::from_value(json!({
        "kind": "sink-template",
        "name": "rows-probe",
        "sink": { "type": "stdout", "config": {} },
        "per_stream": { "stream": "${stream}" },
    }))
    .expect("static probe sink template")
}

/// Every stream of `src` expanded through a probe composition (each stream
/// forced to `append` against a stdout sink), so a listing never depends on a
/// sink the caller has not chosen.
pub fn source_template_nodes(src: &SourceTemplate) -> CliResult<Vec<ExpandedNode>> {
    let mut probe = src.clone();
    for s in &mut probe.streams {
        s.write = WriteChoice::One(WriteMode::Append);
    }
    let c = super::compose::compose_with(&probe, &probe_sink(), &[WriteMode::Append])?;
    let (_, nodes) = placeholder_nodes(&c.document)?;
    Ok(nodes.unwrap_or_default())
}

/// Narrow a source template to the streams a selection runs — how a trigger
/// with a selection composes only those streams, so a stream the chosen sink
/// cannot run does not block the others. Returns the narrowed template and
/// the selection to apply to its composed rows (the run set, by id).
pub fn narrow_source_template(
    src: &SourceTemplate,
    selection: &SelectionRequest,
) -> CliResult<(SourceTemplate, SelectionRequest)> {
    let nodes = source_template_nodes(src)?;
    let kept: BTreeSet<String> =
        crate::select::select_nodes(nodes, &selection.to_run_selection(None), true)?
            .into_iter()
            .map(|n| n.id)
            .collect();
    let mut narrowed = src.clone();
    narrowed.streams.retain(|s| kept.contains(&s.name));
    let effective = SelectionRequest {
        select: narrowed.streams.iter().map(|s| s.name.clone()).collect(),
        include_parents: Some(crate::config::IncludeParents::Off),
        ..Default::default()
    };
    Ok((narrowed, effective))
}

fn replication_of(config: &Value) -> Replication {
    let method = match config.get("replication_method") {
        Some(Value::String(s)) if s.eq_ignore_ascii_case("incremental") => "incremental",
        Some(Value::Object(o))
            if o.get("type")
                .and_then(Value::as_str)
                .is_some_and(|t| t.eq_ignore_ascii_case("incremental")) =>
        {
            "incremental"
        }
        _ => "full",
    };
    let field = ["replication_key", "bookmark_field", "cursor_field"]
        .iter()
        .find_map(|k| config.get(*k).and_then(Value::as_str))
        .or_else(|| {
            config
                .get("replication_method")
                .and_then(|m| m.get("key").or_else(|| m.get("field")))
                .and_then(Value::as_str)
        })
        .map(str::to_string);
    Replication { method, field }
}

fn read_of(node: &ExpandedNode) -> RowRead {
    let kind = node.source.kind.as_str();
    let replication = replication_of(&node.source.config);
    let bookmarks = replication.method == "incremental"
        || crate::registry::EXACTLY_ONCE_SOURCE_KINDS.contains(&kind)
        || crate::registry::source_reports_lag(kind);
    let durable = node.state.as_ref().is_some_and(|s| s.kind != "memory");
    RowRead {
        source_kind: kind.to_string(),
        replication,
        resumable: bookmarks && durable,
        shardable: SHARDABLE_SOURCE_KINDS.contains(&kind),
        supports_discover: crate::registry::source_supports_discover(kind),
    }
}

fn transform_summary(transforms: &[crate::config::TransformSpec]) -> TransformSummary {
    let mut kinds: Vec<String> = Vec::new();
    for t in transforms {
        if !kinds.contains(&t.kind) {
            kinds.push(t.kind.clone());
        }
    }
    TransformSummary {
        count: transforms.len(),
        renames_fields: kinds
            .iter()
            .any(|k| RENAMING_TRANSFORMS.contains(&k.as_str())),
        drops_fields: kinds
            .iter()
            .any(|k| DROPPING_TRANSFORMS.contains(&k.as_str())),
        kinds,
    }
}

#[allow(unused_variables)]
fn shape_of(node: &ExpandedNode) -> RowShape {
    #[allow(unused_mut)]
    let mut shape = RowShape {
        schema: None,
        schema_source: None,
        transforms: transform_summary(&node.transforms),
    };
    #[cfg(feature = "contract")]
    if let Some(c) = &node.contract {
        shape.schema = Some(faucet_core::to_json_schema(c));
        shape.schema_source = Some("contract");
    }
    shape
}

fn guarantees_of(node: &ExpandedNode, mode: WriteMode) -> RowGuarantees {
    RowGuarantees {
        delivery_guarantee: node.delivery_guarantee.to_string(),
        cleanup_capable: crate::registry::sink_supports_cleanup(&node.sink.kind)
            && mode == WriteMode::Upsert,
    }
}

fn sink_write_mode(config: &Value) -> WriteMode {
    config
        .get("write_mode")
        .and_then(Value::as_str)
        .and_then(super::spec::parse_mode)
        .unwrap_or(WriteMode::Append)
}

fn string_list(v: Option<&Value>) -> Vec<String> {
    match v {
        Some(Value::Array(a)) => a
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect(),
        Some(Value::String(s)) => vec![s.clone()],
        _ => Vec::new(),
    }
}

fn sorted_refs(trees: &[&Value]) -> Vec<String> {
    let mut out = Vec::new();
    for t in trees {
        super::spec::param_refs(t, &mut out);
    }
    out.sort();
    out.dedup();
    out
}

fn build_rows(
    nodes: &[ExpandedNode],
    mut meta: HashMap<String, RowMeta>,
    kind: &'static str,
) -> Vec<TemplateRow> {
    let depths = crate::select::execution_depths(nodes);
    let mut children: HashMap<&str, Vec<String>> = HashMap::new();
    for n in nodes {
        if let NodeRole::Child { parent_id, .. } = &n.role {
            children
                .entry(parent_id.as_str())
                .or_default()
                .push(n.id.clone());
        }
    }
    nodes
        .iter()
        .filter(|n| !matches!(n.role, NodeRole::Discovery { .. }))
        .map(|n| {
            let m = meta.remove(&n.id).unwrap_or_default();
            let (parent, parent_key) = match &n.role {
                NodeRole::Child {
                    parent_id,
                    parent_key,
                } => (Some(parent_id.clone()), Some(parent_key.clone())),
                _ => (None, None),
            };
            TemplateRow {
                id: n.id.clone(),
                kind,
                description: m.description,
                status: n.status,
                tags: n.tags.clone(),
                default_selected: n.status.default_eligible(),
                per_parent_record: parent.is_some(),
                parent,
                parent_key,
                children: children.remove(n.id.as_str()).unwrap_or_default(),
                depends_on: n.depends_on.clone(),
                depth: depths.get(n.id.as_str()).copied().unwrap_or(0),
                write: m.write,
                primary_keys: m.primary_keys,
                delete_marker: n
                    .sink
                    .config
                    .get("delete_marker")
                    .filter(|v| !v.is_null())
                    .cloned(),
                read: read_of(n),
                guarantees: m.guarantee,
                shape: shape_of(n),
                params_used: m.params_used,
                state: None,
                selected: None,
                pulled_in: None,
                blocked: None,
                excluded: None,
            }
        })
        .collect()
}

/// A listed pipeline: the report, the placeholder-bound config, and its rows
/// (`None` for a topology).
pub struct PipelineRows {
    pub report: RowsReport,
    pub cfg: PipelineConfig,
    pub nodes: Option<Vec<ExpandedNode>>,
}

/// Rows of a `kind: pipeline` template (or any pipeline config document).
pub fn rows_for_pipeline(doc: &Value) -> CliResult<PipelineRows> {
    let (cfg, nodes) = placeholder_nodes(doc)?;
    let name = cfg.name.clone().unwrap_or_default();
    let Some(nodes) = nodes else {
        return Ok(PipelineRows {
            report: empty_report(name, TemplateKind::Pipeline, false),
            cfg,
            nodes: None,
        });
    };
    let pipeline = doc.get("pipeline").cloned().unwrap_or(Value::Null);
    let matrix = doc
        .get("matrix")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut meta = HashMap::new();
    for n in &nodes {
        let raw_row = matrix.get(n.row_index).cloned().unwrap_or(Value::Null);
        let src_ref = raw_row
            .pointer("/source/ref")
            .and_then(Value::as_str)
            .unwrap_or(DEFAULT_SOURCE);
        let sink_ref = raw_row
            .pointer("/sink/ref")
            .and_then(Value::as_str)
            .unwrap_or(DEFAULT_SOURCE);
        let named_or_single = |plural: &str, single: &str, r: &str| -> Value {
            pipeline
                .get(plural)
                .and_then(|m| m.get(r))
                .cloned()
                .or_else(|| {
                    (r == DEFAULT_SOURCE)
                        .then(|| pipeline.get(single).cloned())
                        .flatten()
                })
                .unwrap_or(Value::Null)
        };
        let src = named_or_single("sources", "source", src_ref);
        let sink = named_or_single("sinks", "sink", sink_ref);
        let shared = pipeline.get("transforms").cloned().unwrap_or(Value::Null);
        let mode = sink_write_mode(&n.sink.config);
        let offered = crate::registry::sink_supported_write_modes(&n.sink.kind).to_vec();
        let supported = offered.contains(&mode);
        meta.insert(
            n.id.clone(),
            RowMeta {
                description: None,
                primary_keys: string_list(n.sink.config.get("key")),
                write: Some(RowWrite {
                    requested: vec![mode],
                    resolved: Some(mode),
                    supported: Some(supported),
                    unsupported_reason: (!supported).then(|| {
                        format!(
                            "sink '{}' does not support write_mode {}",
                            n.sink.kind,
                            mode.as_str()
                        )
                    }),
                    offered: Some(offered),
                    alias_applied: None,
                }),
                params_used: sorted_refs(&[&raw_row, &src, &sink, &shared]),
                guarantee: Some(guarantees_of(n, mode)),
            },
        );
    }
    let rows = build_rows(&nodes, meta, "row");
    let mut report = empty_report(name, TemplateKind::Pipeline, true);
    report.rows = rows;
    Ok(PipelineRows {
        report,
        cfg,
        nodes: Some(nodes),
    })
}

fn empty_report(template: String, kind: TemplateKind, selectable: bool) -> RowsReport {
    RowsReport {
        template,
        version: None,
        kind: kind.as_str().to_string(),
        selectable,
        sink: None,
        sink_version: None,
        sink_kind: None,
        rows: Vec::new(),
        selection: None,
        run_set: None,
        error: None,
        notes: Vec::new(),
    }
}

/// A listed source template: the report, the probe rows a selection resolves
/// over, and — with a sink — the composed document of its runnable streams
/// (the config a state read uses).
pub struct SourceRows {
    pub report: RowsReport,
    pub nodes: Vec<ExpandedNode>,
    pub composed: Option<Value>,
}

/// Rows of a `source-template`, with sink-dependent facts when `sink` is given.
/// An `overlay` is applied over the composition, so the guarantees and the
/// state reflect the deployment that runs it.
pub fn rows_for_source(
    src: &SourceTemplate,
    sink: Option<&SinkTemplate>,
    overlay: Option<&super::spec::DeploymentTemplate>,
) -> CliResult<SourceRows> {
    src.validate()?;
    let nodes = source_template_nodes(src)?;
    let streams: HashMap<&str, &super::spec::Stream> =
        src.streams.iter().map(|s| (s.name.as_str(), s)).collect();

    // Per-stream resolution against the sink, plus the guarantee each runnable
    // stream gets from the real composition.
    let mut plans: HashMap<String, Result<StreamPlan, StreamIncompatibility>> = HashMap::new();
    let mut guarantees: HashMap<String, RowGuarantees> = HashMap::new();
    let mut offered: Vec<WriteMode> = Vec::new();
    let mut composed = None;
    if let Some(sink) = sink {
        sink.validate()?;
        let supported = crate::registry::sink_supported_write_modes(&sink.sink.kind);
        let aliases = sink.aliases();
        offered = supported.to_vec();
        for s in &src.streams {
            plans.insert(
                s.name.clone(),
                resolve_mode(s, &sink.sink.kind, supported, &aliases),
            );
        }
        // A stream runs only when it and every ancestor resolve.
        let runnable = |name: &str| -> Result<(), String> {
            let mut cur = streams.get(name).copied();
            let mut first = true;
            while let Some(s) = cur {
                if let Some(Err(e)) = plans.get(&s.name) {
                    return Err(if first {
                        e.reason.clone()
                    } else {
                        format!(
                            "its parent stream `{}` cannot run on this sink: {}",
                            s.name, e.reason
                        )
                    });
                }
                first = false;
                cur = s.parent.as_deref().and_then(|p| streams.get(p).copied());
            }
            Ok(())
        };
        let run_status: HashMap<String, Result<(), String>> = src
            .streams
            .iter()
            .map(|s| (s.name.clone(), runnable(&s.name)))
            .collect();
        let mut narrowed = src.clone();
        narrowed
            .streams
            .retain(|s| matches!(run_status.get(&s.name), Some(Ok(()))));
        if !narrowed.streams.is_empty() {
            let mut c = super::compose::compose_with(&narrowed, sink, supported)?;
            if let Some(o) = overlay {
                c = c.apply_overlay(o)?;
            }
            let (_, real) = placeholder_nodes(&c.document)?;
            composed = Some(c.document);
            for n in real.unwrap_or_default() {
                let mode = sink_write_mode(&n.sink.config);
                guarantees.insert(n.id.clone(), guarantees_of(&n, mode));
            }
        }
        for s in &src.streams {
            if let Some(Err(reason)) = run_status.get(&s.name).cloned()
                && matches!(plans.get(&s.name), Some(Ok(_)))
            {
                plans.insert(
                    s.name.clone(),
                    Err(StreamIncompatibility {
                        stream: s.name.clone(),
                        reason,
                    }),
                );
            }
        }
    }

    let mut meta = HashMap::new();
    for s in &src.streams {
        let source_spec = match s.source.r#ref.as_deref() {
            None | Some(DEFAULT_SOURCE) => serde_json::to_value(&src.source).unwrap_or(Value::Null),
            Some(r) => src
                .sources
                .get(r)
                .and_then(|c| serde_json::to_value(c).ok())
                .unwrap_or(Value::Null),
        };
        let stream_v = serde_json::to_value(s).unwrap_or(Value::Null);
        let shared = if s.inherit_transforms {
            serde_json::to_value(&src.transforms).unwrap_or(Value::Null)
        } else {
            Value::Null
        };
        let sink_v = sink
            .map(|t| json!({ "sink": t.sink, "per_stream": t.per_stream }))
            .unwrap_or(Value::Null);
        let requested = s.write.candidates();
        let write = match (sink, plans.get(&s.name)) {
            (Some(_), Some(Ok(p))) => RowWrite {
                requested,
                resolved: Some(p.chosen),
                supported: Some(true),
                offered: Some(offered.clone()),
                alias_applied: p
                    .satisfies
                    .map(|from| format!("{}→{}", from.as_str(), p.chosen.as_str())),
                unsupported_reason: None,
            },
            (Some(_), Some(Err(e))) => RowWrite {
                requested,
                resolved: None,
                supported: Some(false),
                offered: Some(offered.clone()),
                alias_applied: None,
                unsupported_reason: Some(e.reason.clone()),
            },
            _ => RowWrite {
                requested,
                resolved: None,
                supported: None,
                offered: None,
                alias_applied: None,
                unsupported_reason: None,
            },
        };
        meta.insert(
            s.name.clone(),
            RowMeta {
                description: s.description.clone(),
                primary_keys: s.primary_keys.clone(),
                write: Some(write),
                params_used: sorted_refs(&[&stream_v, &source_spec, &shared, &sink_v]),
                guarantee: guarantees.remove(&s.name),
            },
        );
    }
    let mut report = empty_report(src.id(), TemplateKind::SourceTemplate, true);
    report.rows = build_rows(&nodes, meta, "stream");
    if let Some(t) = sink {
        report.sink = Some(t.id());
        report.sink_kind = Some(t.sink.kind.clone());
    }
    Ok(SourceRows {
        report,
        nodes,
        composed,
    })
}

/// Attach a dry-run resolve of `selection` over `nodes` to `report`.
pub fn attach_resolve(
    report: &mut RowsReport,
    nodes: &[ExpandedNode],
    selection: &SelectionRequest,
    cfg_selection: Option<&crate::config::SelectionSpec>,
    has_matrix: bool,
) {
    report.selection = Some(selection.clone());
    if !report.selectable {
        report.run_set = Some(Vec::new());
        report.error = Some(crate::select::TOPOLOGY_REFUSAL.to_string());
        return;
    }
    let Resolution {
        run_set,
        decisions,
        error,
    } = crate::select::resolve(
        nodes,
        &selection.to_run_selection(cfg_selection),
        has_matrix,
    );
    for row in &mut report.rows {
        match decisions.get(&row.id) {
            Some(RowDecision::Selected { .. }) => row.selected = Some(true),
            Some(RowDecision::PulledIn { because }) => {
                row.selected = Some(false);
                row.pulled_in = Some(PulledIn {
                    because: because.clone(),
                });
            }
            Some(RowDecision::Blocked { reason }) => {
                row.selected = Some(false);
                row.blocked = Some(reason.clone());
            }
            Some(RowDecision::Excluded { reason }) => {
                row.selected = Some(false);
                row.excluded = Some(reason.clone());
            }
            None => row.selected = Some(false),
        }
    }
    report.run_set = Some(run_set);
    report.error = error.map(|e| e.to_string());
}

/// Attach the `faucet status` view to each row, from a status report.
pub fn attach_state(report: &mut RowsReport, status: &crate::status::StatusReport) {
    let by_row: HashMap<&str, &crate::status::RowStatus> =
        status.rows.iter().map(|r| (r.row.as_str(), r)).collect();
    let children: HashMap<&str, &crate::status::ChildrenStatus> = status
        .rows
        .iter()
        .flat_map(|r| r.children.iter())
        .map(|c| (c.row.as_str(), c))
        .collect();
    for row in &mut report.rows {
        if let Some(r) = by_row.get(row.id.as_str()) {
            row.state = Some(RowState {
                health: r.health,
                last_success: r.last_success.as_ref().map(|x| x.at),
                last_failure: r.last_failure.as_ref().map(|x| x.at),
                last_error: r.last_failure.as_ref().and_then(|x| x.error.clone()),
                bookmark_age_secs: r.bookmark_age_secs,
                lag: r.lag.as_ref().and_then(|l| serde_json::to_value(l).ok()),
            });
        } else if let Some(c) = children.get(row.id.as_str()) {
            row.state = Some(RowState {
                health: c.worst,
                last_success: None,
                last_failure: c.latest_failure.as_ref().map(|x| x.at),
                last_error: c.latest_failure.as_ref().and_then(|x| x.error.clone()),
                bookmark_age_secs: None,
                lag: None,
            });
        }
    }
}

/// Load a runnable (defaults-only) config for reading state, then read the
/// status report. `Err` carries the note explaining why state is omitted.
pub async fn read_state(
    doc: &Value,
    fallback_name: &str,
    history: (Vec<crate::status::HistoryRun>, Vec<String>),
) -> Result<crate::status::StatusReport, String> {
    let mut doc = doc.clone();
    crate::interpolate::interpolate_value(&mut doc).map_err(|e| format!("state omitted: {e}"))?;
    crate::params::bind_document(&mut doc, &SuppliedParams::new(), BindMode::Strict)
        .map_err(|e| format!("state omitted: the config needs params to name its state ({e})"))?;
    let mut cfg = PipelineConfig::from_value(doc).map_err(|e| format!("state omitted: {e}"))?;
    crate::secrets::resolve_secrets(&mut cfg)
        .await
        .map_err(|e| format!("state omitted: {e}"))?;
    let name = cfg
        .name
        .clone()
        .unwrap_or_else(|| fallback_name.to_string());
    let target = crate::pipeline_state::PipelineTarget::resolve(&cfg, &name)
        .map_err(|e| format!("state omitted: {e}"))?;
    let stores = crate::pipeline_state::ops::Stores::build(&target, None)
        .await
        .map_err(|e| {
            format!(
                "state omitted: the state store is unreadable: {}",
                crate::secrets::registry::redact(&e.to_string())
            )
        })?;
    if stores.is_empty() {
        return Err("state omitted: the pipeline has no `state:` store".to_string());
    }
    let auth = crate::auth_catalog::build_auth_catalog(cfg.auth.as_ref())
        .map_err(|e| format!("state omitted: {e}"))?;
    let (runs, active) = history;
    let inputs = crate::status::StatusInputs {
        now: Utc::now(),
        row: None,
        probe: false,
        auth: &auth,
        history: runs,
        active_runs: active,
    };
    crate::status::assemble(&target, Ok(&stores), &inputs)
        .await
        .map_err(|e| format!("state omitted: {e}"))
}

/// What a listing adds beyond the rows.
#[derive(Debug, Default)]
pub struct ListOptions<'a> {
    /// Dry-run resolve this selection.
    pub selection: Option<&'a SelectionRequest>,
    /// Read each row's `faucet status` view.
    pub state: bool,
    /// Runs a run-history store recorded for the pipeline (newest first) and
    /// the ids still in flight — the status report's history input.
    pub history: (Vec<crate::status::HistoryRun>, Vec<String>),
}

/// The full listing of a source template.
pub async fn list_source(
    src: &SourceTemplate,
    sink: Option<&SinkTemplate>,
    overlay: Option<&super::spec::DeploymentTemplate>,
    opts: ListOptions<'_>,
) -> CliResult<RowsReport> {
    let SourceRows {
        mut report,
        nodes,
        composed,
    } = rows_for_source(src, sink, overlay)?;
    if let Some(sel) = opts.selection {
        attach_resolve(&mut report, &nodes, sel, None, true);
    }
    if opts.state {
        match composed {
            Some(doc) => match read_state(&doc, &src.id(), opts.history).await {
                Ok(status) => attach_state(&mut report, &status),
                Err(note) => report.notes.push(note),
            },
            None if sink.is_none() => report.notes.push(
                "state omitted: pass a sink template — the composed run names the state store"
                    .to_string(),
            ),
            None => report
                .notes
                .push("state omitted: no stream can run on this sink".to_string()),
        }
    }
    Ok(report)
}

/// The full listing of a pipeline document.
pub async fn list_pipeline(
    doc: &Value,
    fallback_name: &str,
    opts: ListOptions<'_>,
) -> CliResult<RowsReport> {
    let PipelineRows {
        mut report,
        cfg,
        nodes,
    } = rows_for_pipeline(doc)?;
    if report.template.is_empty() {
        report.template = fallback_name.to_string();
    }
    if let Some(sel) = opts.selection {
        attach_resolve(
            &mut report,
            nodes.as_deref().unwrap_or_default(),
            sel,
            cfg.selection.as_ref(),
            !cfg.matrix.is_empty(),
        );
    }
    if opts.state && report.selectable {
        match read_state(doc, fallback_name, opts.history).await {
            Ok(status) => attach_state(&mut report, &status),
            Err(note) => report.notes.push(note),
        }
    }
    Ok(report)
}

/// The pipeline name a document's state lives under: its `name:`, else
/// `fallback`.
pub fn pipeline_name(doc: &Value, fallback: &str) -> String {
    doc.get("name")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| fallback.to_string())
}

/// The human table `faucet template rows` / `faucet hub rows` print.
pub fn render_human(r: &RowsReport) -> String {
    let mut out = format!(
        "{} {}{} ({})",
        r.kind,
        r.template,
        r.version.map(|v| format!(" v{v}")).unwrap_or_default(),
        if r.selectable {
            format!("{} row(s)", r.rows.len())
        } else {
            "topology — no selectable rows".to_string()
        }
    );
    if let Some(s) = &r.sink {
        out.push_str(&format!(
            " × {s}{}",
            r.sink_version.map(|v| format!(" v{v}")).unwrap_or_default()
        ));
    }
    out.push('\n');
    let resolving = r.run_set.is_some();
    for row in &r.rows {
        let mark = if !resolving {
            if row.default_selected { "•" } else { "·" }
        } else if row.selected == Some(true) {
            "✓"
        } else if row.pulled_in.is_some() {
            "+"
        } else if row.blocked.is_some() {
            "✗"
        } else {
            " "
        };
        let mut facts = vec![row.status.as_str().to_string()];
        if !row.tags.is_empty() {
            facts.push(format!("tags {}", row.tags.join(",")));
        }
        if let Some(p) = &row.parent {
            facts.push(format!("child of {p}"));
        }
        if !row.depends_on.is_empty() {
            facts.push(format!("after {}", row.depends_on.join(",")));
        }
        if let Some(w) = &row.write {
            match (w.resolved, w.supported) {
                (_, Some(false)) => facts.push(format!(
                    "UNSUPPORTED: {}",
                    w.unsupported_reason.as_deref().unwrap_or("")
                )),
                (Some(m), _) => facts.push(match &w.alias_applied {
                    Some(a) => format!("write {a}"),
                    None => format!("write {}", m.as_str()),
                }),
                (None, _) => facts.push(format!(
                    "wants {}",
                    w.requested
                        .iter()
                        .map(|m| m.as_str())
                        .collect::<Vec<_>>()
                        .join("|")
                )),
            }
        }
        if let Some(g) = &row.guarantees {
            facts.push(g.delivery_guarantee.clone());
        }
        if let Some(st) = &row.state {
            facts.push(format!("health {}", st.health.as_str()));
        }
        out.push_str(&format!("  {mark} {:<28} {}\n", row.id, facts.join(" · ")));
        if let Some(p) = &row.pulled_in {
            out.push_str(&format!("      pulled in for {}\n", p.because));
        }
        if let Some(b) = &row.blocked {
            out.push_str(&format!("      blocked: {b}\n"));
        }
    }
    if let Some(set) = &r.run_set {
        if set.is_empty() {
            out.push_str("run set: (none)\n");
        } else {
            out.push_str(&format!("run set: {}\n", set.join(" → ")));
        }
    }
    if let Some(e) = &r.error {
        out.push_str(&format!("error: {e}\n"));
    }
    for n in &r.notes {
        out.push_str(&format!("note: {n}\n"));
    }
    out
}
