//! Change impact analysis (#707) — who breaks if this row's output changes.
//!
//! `faucet plan` knows what a change does to *its own* destination; the Data
//! Movement Catalog (#279) knows the lineage graph across pipelines. This
//! module connects the two: given the row about to run and the schema it
//! would now write, it
//!
//! 1. finds the row's sink dataset in the catalog (by the lineage edge the
//!    row recorded on its last run — a row that has never run has no edge,
//!    and the report says so),
//! 2. computes the **schema delta** against the dataset's last observed
//!    schema — added / removed / retyped columns, with a removed+added pair
//!    reported as a **rename** when the row's own transform chain renames it
//!    (`rename_field` / `rename_keys`),
//! 3. walks the lineage graph **downstream** (source → sink edges, depth
//!    bounded), following each edge's recorded column lineage to say which
//!    downstream columns read an affected column; an edge with no column
//!    lineage (opaque transform, or recorded before lineage existed) makes
//!    everything past it `unknown` — never a false "none",
//! 4. escalates to `breaking` when a downstream pipeline's **data contract**
//!    (its last config snapshot's `contract`) declares an affected column,
//!    naming the contract version, and
//! 5. names the affected datasets' **owners** and declared **consumers**
//!    (a consumer listing `columns` is affected only when one of them is).
//!
//! Severity per item: `breaking` (a removed / retyped / renamed column that
//! something downstream reads), `additive` (only new columns), `unknown`
//! (opaque path), `none`. Everything here is pure over catalog reads —
//! nothing is written and no connector is built.

use crate::error::{CliError, CliResult};
use crate::expand::ExpandedNode;
use crate::serve::history::RunHistory;
use crate::serve::history::catalog::{CatalogConsumer, CatalogDataset, CatalogLineageEdge};
use faucet_lineage::ColumnOp;
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

/// Default downstream hop bound.
pub const DEFAULT_DEPTH: u32 = 5;
/// Hard ceiling on the hop bound.
pub const MAX_DEPTH: u32 = 32;

/// How a change affects an item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    /// Nothing it reads changes.
    None,
    /// Only new columns appear.
    Additive,
    /// The path is opaque — a change may or may not reach it.
    Unknown,
    /// A column it reads is removed, retyped, or renamed.
    Breaking,
}

impl Severity {
    pub fn as_str(self) -> &'static str {
        match self {
            Severity::None => "none",
            Severity::Additive => "additive",
            Severity::Unknown => "unknown",
            Severity::Breaking => "breaking",
        }
    }
}

/// What happened to one column of the changed row's output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ColumnChange {
    Added,
    Removed,
    Retyped {
        from: String,
        to: String,
    },
    /// `from` (the removed name) is now written as `to`.
    Renamed {
        to: String,
    },
}

impl ColumnChange {
    fn severity(&self) -> Severity {
        match self {
            ColumnChange::Added => Severity::Additive,
            _ => Severity::Breaking,
        }
    }
}

/// The schema delta of the changed row's output.
#[derive(Debug, Clone, Default, Serialize)]
pub struct SchemaDelta {
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub retyped: Vec<RetypedColumn>,
    pub renamed: Vec<RenamedColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RetypedColumn {
    pub column: String,
    pub from: String,
    pub to: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RenamedColumn {
    pub from: String,
    pub to: String,
}

impl SchemaDelta {
    pub fn is_empty(&self) -> bool {
        self.added.is_empty()
            && self.removed.is_empty()
            && self.retyped.is_empty()
            && self.renamed.is_empty()
    }

    /// The per-column change map the downstream walk starts from.
    fn changes(&self) -> BTreeMap<String, ColumnChange> {
        let mut m = BTreeMap::new();
        for c in &self.added {
            m.insert(c.clone(), ColumnChange::Added);
        }
        for c in &self.removed {
            m.insert(c.clone(), ColumnChange::Removed);
        }
        for r in &self.retyped {
            m.insert(
                r.column.clone(),
                ColumnChange::Retyped {
                    from: r.from.clone(),
                    to: r.to.clone(),
                },
            );
        }
        for r in &self.renamed {
            m.insert(r.from.clone(), ColumnChange::Renamed { to: r.to.clone() });
        }
        m
    }
}

/// One affected downstream column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AffectedColumn {
    /// The column *in the affected dataset*.
    pub column: String,
    /// The change to the upstream column it reads.
    pub change: ColumnChange,
    /// The upstream column(s) it reads that changed.
    pub reads: Vec<String>,
}

/// A downstream data contract that declares an affected column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContractHit {
    pub pipeline: String,
    pub row: String,
    pub version: String,
    /// The declared fields the change touches.
    pub fields: Vec<String>,
}

/// One declared consumer, with its own severity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AffectedConsumer {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contact: Option<String>,
    pub severity: Severity,
    /// The affected columns it reads (all affected columns when it declared
    /// none).
    pub columns: Vec<String>,
}

/// One affected dataset.
#[derive(Debug, Clone, Serialize)]
pub struct AffectedDataset {
    pub id: String,
    pub uri: String,
    /// The pipeline / row that writes it (the last recorded edge into it).
    pub pipeline: String,
    pub row: String,
    /// Hops downstream of the changed row's sink (0 = the sink itself).
    pub depth: u32,
    pub severity: Severity,
    /// The path is opaque: an edge on the way carries no column lineage.
    pub opaque: bool,
    pub columns: Vec<AffectedColumn>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contract: Option<ContractHit>,
    pub owners: Vec<String>,
    pub consumers: Vec<AffectedConsumer>,
}

/// Where the planned schema came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PlannedFrom {
    /// The plan's sample, run through the row's transforms.
    Sample,
    /// The catalog's last source schema, pushed through the row's transform
    /// chain by column lineage.
    Lineage,
    /// Nothing — the chain is opaque and no sample was given.
    Unknown,
}

/// The dataset the changed row writes.
#[derive(Debug, Clone, Serialize)]
pub struct SinkDataset {
    pub id: String,
    pub uri: String,
    pub last_run_id: String,
}

/// The whole report.
#[derive(Debug, Clone, Serialize)]
pub struct ImpactReport {
    pub pipeline: String,
    pub row: String,
    /// `None` when the row has never recorded a run (no lineage edge).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dataset: Option<SinkDataset>,
    pub planned_from: PlannedFrom,
    pub delta: SchemaDelta,
    /// Overall severity — the maximum over the affected items.
    pub severity: Severity,
    /// Downstream datasets (depth 0 = the row's own sink), nearest first.
    pub affected: Vec<AffectedDataset>,
    /// Every owner of an affected dataset, deduplicated.
    pub owners: Vec<String>,
    pub notes: Vec<String>,
}

impl ImpactReport {
    pub fn is_breaking(&self) -> bool {
        self.severity == Severity::Breaking
    }
}

/// The inputs for one analysis.
pub struct ImpactInputs<'a> {
    pub pipeline: &'a str,
    pub node: &'a ExpandedNode,
    /// The schema the row would now write (`infer_schema` shape) — from a
    /// plan sample, when there is one.
    pub planned_schema: Option<&'a Value>,
    pub depth: u32,
}

/// Run the analysis against `store`.
pub async fn analyze(store: &dyn RunHistory, inputs: ImpactInputs<'_>) -> CliResult<ImpactReport> {
    let read = |e: crate::serve::history::HistoryError| {
        CliError::Config(format!("catalog read failed: {e}"))
    };
    let depth = inputs.depth.clamp(1, MAX_DEPTH);
    let edges = store.catalog_lineage(None, depth).await.map_err(read)?;
    let node = inputs.node;
    let mut notes = Vec::new();

    // 1. The row's sink dataset: the edge this (pipeline, row) last recorded.
    let own_edge = edges
        .iter()
        .find(|e| e.pipeline == inputs.pipeline && e.row == node.id)
        .cloned();
    let Some(own_edge) = own_edge else {
        notes.push(format!(
            "row '{}' of pipeline '{}' has no recorded run in the catalog — no lineage to walk; \
             run it once (with a `catalog:` block) and plan again",
            node.id, inputs.pipeline
        ));
        return Ok(ImpactReport {
            pipeline: inputs.pipeline.to_string(),
            row: node.id.clone(),
            dataset: None,
            planned_from: match inputs.planned_schema {
                Some(_) => PlannedFrom::Sample,
                None => PlannedFrom::Unknown,
            },
            delta: SchemaDelta::default(),
            severity: Severity::None,
            affected: Vec::new(),
            owners: Vec::new(),
            notes,
        });
    };
    let sink = store
        .catalog_get_dataset(&own_edge.dst_id)
        .await
        .map_err(read)?
        .map(|d| d.dataset);
    let source = store
        .catalog_get_dataset(&own_edge.src_id)
        .await
        .map_err(read)?
        .map(|d| d.dataset);

    // 2. Current vs planned schema.
    let current = sink.as_ref().and_then(|d| d.current_schema.clone());
    let ops = crate::lineage_glue::column_ops(&node.transforms, masking_present(node));
    let (planned, planned_from) = match inputs.planned_schema {
        Some(s) => (Some(s.clone()), PlannedFrom::Sample),
        None => match source.as_ref().and_then(|d| d.current_schema.as_ref()) {
            Some(src_schema) => match push_schema(src_schema, &ops) {
                Some(p) => (Some(p), PlannedFrom::Lineage),
                None => {
                    notes.push(
                        "the row's transform chain is opaque (flatten / explode / keys_case / \
                         sql / wasm / custom): pass `--sample` to plan the output schema"
                            .to_string(),
                    );
                    (None, PlannedFrom::Unknown)
                }
            },
            None => {
                notes.push(
                    "the catalog has no schema for the row's source dataset: pass `--sample` \
                     to plan the output schema"
                        .to_string(),
                );
                (None, PlannedFrom::Unknown)
            }
        },
    };
    if current.is_none() {
        notes.push(
            "the catalog has no observed schema for the row's sink dataset yet — the delta \
             cannot be computed"
                .to_string(),
        );
    }
    let delta = match (&current, &planned) {
        (Some(cur), Some(plan)) => schema_delta(cur, plan, &ops),
        _ => SchemaDelta::default(),
    };
    let unknown_delta = planned.is_none() || current.is_none();

    // 3. Downstream walk.
    let datasets = load_datasets(store, &edges, &own_edge.dst_id).await?;
    let mut affected = walk_downstream(&own_edge, &edges, &datasets, &delta, unknown_delta, depth);

    // 4. Contract escalation, per affected dataset's writing pipeline.
    let mut snapshots: HashMap<String, Option<crate::serve::history::catalog::ConfigSnapshot>> =
        HashMap::new();
    for a in affected.iter_mut() {
        if a.depth == 0 {
            // The changed row's own contract is what `plan` already checks.
            continue;
        }
        let snap = match snapshots.get(&a.pipeline) {
            Some(s) => s.clone(),
            None => {
                let s = store
                    .catalog_last_config_snapshot(&a.pipeline)
                    .await
                    .map_err(read)?;
                snapshots.insert(a.pipeline.clone(), s.clone());
                s
            }
        };
        let Some(contract) = snap
            .as_ref()
            .and_then(|s| s.rows.get(&a.row))
            .and_then(|r| r.contract.as_ref())
        else {
            continue;
        };
        let declared: Vec<String> = contract
            .get("fields")
            .and_then(Value::as_array)
            .map(|f| {
                f.iter()
                    .filter_map(|x| x.get("name").and_then(Value::as_str))
                    .map(String::from)
                    .collect()
            })
            .unwrap_or_default();
        let version = contract
            .get("version")
            .and_then(Value::as_str)
            .unwrap_or("?")
            .to_string();
        let hit: Vec<String> = if a.opaque {
            // Every declared field may be affected; the contract is named so a
            // reviewer knows a promise sits on this path.
            declared.clone()
        } else {
            a.columns
                .iter()
                .filter(|c| c.change.severity() == Severity::Breaking)
                .map(|c| c.column.clone())
                .filter(|c| declared.contains(c))
                .collect()
        };
        if hit.is_empty() {
            continue;
        }
        if !a.opaque {
            a.severity = Severity::Breaking;
        }
        a.contract = Some(ContractHit {
            pipeline: a.pipeline.clone(),
            row: a.row.clone(),
            version,
            fields: hit,
        });
    }

    let severity = affected
        .iter()
        .map(|a| a.severity)
        .max()
        .unwrap_or(Severity::None);
    let mut owners: Vec<String> = affected
        .iter()
        .flat_map(|a| a.owners.iter().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    owners.sort();
    Ok(ImpactReport {
        pipeline: inputs.pipeline.to_string(),
        row: node.id.clone(),
        dataset: sink.as_ref().map(|d| SinkDataset {
            id: d.id.clone(),
            uri: d.uri.clone(),
            last_run_id: d.last_run_id.clone(),
        }),
        planned_from,
        delta,
        severity,
        affected,
        owners,
        notes,
    })
}

#[cfg(feature = "masking")]
fn masking_present(node: &ExpandedNode) -> bool {
    node.masking.is_some()
}
#[cfg(not(feature = "masking"))]
fn masking_present(_node: &ExpandedNode) -> bool {
    false
}

/// Load every dataset reachable downstream of `root` (plus `root`) in one map.
async fn load_datasets(
    store: &dyn RunHistory,
    edges: &[CatalogLineageEdge],
    root: &str,
) -> CliResult<HashMap<String, CatalogDataset>> {
    let mut wanted: BTreeSet<String> = BTreeSet::from([root.to_string()]);
    let mut frontier = vec![root.to_string()];
    while let Some(id) = frontier.pop() {
        for e in edges.iter().filter(|e| e.src_id == id) {
            if wanted.insert(e.dst_id.clone()) {
                frontier.push(e.dst_id.clone());
            }
        }
    }
    let mut out = HashMap::new();
    for id in wanted {
        if let Some(d) = store
            .catalog_get_dataset(&id)
            .await
            .map_err(|e| CliError::Config(format!("catalog read failed: {e}")))?
        {
            out.insert(id, d.dataset);
        }
    }
    Ok(out)
}

/// Column names + type strings of an `infer_schema`-shaped object.
fn columns_of(schema: &Value) -> BTreeMap<String, String> {
    schema
        .get("properties")
        .and_then(Value::as_object)
        .map(|p| p.iter().map(|(k, v)| (k.clone(), type_string(v))).collect())
        .unwrap_or_default()
}

/// A stable rendering of a property's type (`"string"`, `"integer|null"`, …).
fn type_string(prop: &Value) -> String {
    match prop.get("type") {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Array(items)) => {
            let mut parts: Vec<String> = items
                .iter()
                .filter_map(Value::as_str)
                .map(String::from)
                .collect();
            parts.sort();
            parts.join("|")
        }
        _ => "unknown".to_string(),
    }
}

/// Push a source schema through the row's column ops: the planned output
/// columns, each typed like its single origin (or `unknown` for a literal /
/// multi-origin column). `None` when the chain is opaque.
pub fn push_schema(source: &Value, ops: &[ColumnOp]) -> Option<Value> {
    let cols = columns_of(source);
    let inputs: Vec<String> = cols.keys().cloned().collect();
    let lineage = faucet_lineage::derive_column_lineage(&inputs, ops)?;
    let mut props = serde_json::Map::new();
    for (out, ins) in &lineage.edges {
        let ty = match ins.as_slice() {
            [one] => cols.get(one).cloned().unwrap_or_else(|| "unknown".into()),
            _ => "unknown".to_string(),
        };
        props.insert(out.clone(), serde_json::json!({ "type": ty }));
    }
    Some(serde_json::json!({ "type": "object", "properties": props }))
}

/// The delta between the current and planned schema, with renames recovered
/// from the row's own `Rename` ops (a rename would otherwise read as a drop
/// plus an add).
pub fn schema_delta(current: &Value, planned: &Value, ops: &[ColumnOp]) -> SchemaDelta {
    let cur = columns_of(current);
    let plan = columns_of(planned);
    let mut added: Vec<String> = plan
        .keys()
        .filter(|k| !cur.contains_key(*k))
        .cloned()
        .collect();
    let mut removed: Vec<String> = cur
        .keys()
        .filter(|k| !plan.contains_key(*k))
        .cloned()
        .collect();
    let mut retyped = Vec::new();
    for (name, cur_ty) in &cur {
        if let Some(plan_ty) = plan.get(name)
            && plan_ty != cur_ty
            && plan_ty != "unknown"
        {
            retyped.push(RetypedColumn {
                column: name.clone(),
                from: cur_ty.clone(),
                to: plan_ty.clone(),
            });
        }
    }
    let mut renamed = Vec::new();
    for op in ops {
        if let ColumnOp::Rename(pairs) = op {
            for (from, to) in pairs {
                if removed.contains(from) && added.contains(to) {
                    removed.retain(|c| c != from);
                    added.retain(|c| c != to);
                    renamed.push(RenamedColumn {
                        from: from.clone(),
                        to: to.clone(),
                    });
                }
            }
        }
    }
    added.sort();
    removed.sort();
    SchemaDelta {
        added,
        removed,
        retyped,
        renamed,
    }
}

/// The recorded column-lineage facet of an edge as `out → ins`.
fn edge_fields(edge: &CatalogLineageEdge) -> Option<BTreeMap<String, Vec<String>>> {
    let fields = edge.column_lineage.as_ref()?.get("fields")?.as_object()?;
    Some(
        fields
            .iter()
            .map(|(out, ins)| {
                (
                    out.clone(),
                    ins.as_array()
                        .map(|a| {
                            a.iter()
                                .filter_map(Value::as_str)
                                .map(String::from)
                                .collect()
                        })
                        .unwrap_or_default(),
                )
            })
            .collect(),
    )
}

/// The affected consumers of a dataset given its affected column set.
fn affected_consumers(
    consumers: &[CatalogConsumer],
    columns: &[AffectedColumn],
    opaque: bool,
) -> Vec<AffectedConsumer> {
    consumers
        .iter()
        .filter_map(|c| {
            let hits: Vec<&AffectedColumn> = if c.columns.is_empty() {
                columns.iter().collect()
            } else {
                columns
                    .iter()
                    .filter(|a| c.columns.contains(&a.column))
                    .collect()
            };
            let severity = if opaque {
                Severity::Unknown
            } else {
                hits.iter()
                    .map(|a| a.change.severity())
                    .max()
                    .unwrap_or(Severity::None)
            };
            if severity == Severity::None {
                return None;
            }
            Some(AffectedConsumer {
                name: c.name.clone(),
                kind: c.kind.clone(),
                contact: c.contact.clone(),
                severity,
                columns: hits.iter().map(|a| a.column.clone()).collect(),
            })
        })
        .collect()
}

/// BFS downstream from the changed row's sink, carrying the affected column
/// set across each edge's column lineage.
fn walk_downstream(
    own_edge: &CatalogLineageEdge,
    edges: &[CatalogLineageEdge],
    datasets: &HashMap<String, CatalogDataset>,
    delta: &SchemaDelta,
    unknown_delta: bool,
    depth: u32,
) -> Vec<AffectedDataset> {
    // Depth 0: the sink itself.
    let root_changes = delta.changes();
    let root_columns: Vec<AffectedColumn> = root_changes
        .iter()
        .map(|(col, change)| AffectedColumn {
            column: col.clone(),
            change: change.clone(),
            reads: vec![col.clone()],
        })
        .collect();
    let root_ds = datasets.get(&own_edge.dst_id);
    let root_severity = if unknown_delta {
        Severity::Unknown
    } else {
        root_columns
            .iter()
            .map(|c| c.change.severity())
            .max()
            .unwrap_or(Severity::None)
    };
    let mut out = vec![AffectedDataset {
        id: own_edge.dst_id.clone(),
        uri: own_edge.dst_uri.clone(),
        pipeline: own_edge.pipeline.clone(),
        row: own_edge.row.clone(),
        depth: 0,
        severity: root_severity,
        opaque: unknown_delta,
        columns: root_columns.clone(),
        contract: None,
        owners: root_ds.map(|d| d.owners.clone()).unwrap_or_default(),
        consumers: root_ds
            .map(|d| affected_consumers(&d.consumers, &root_columns, unknown_delta))
            .unwrap_or_default(),
    }];

    // (dataset id, affected columns at that dataset, opaque so far)
    let mut frontier: Vec<(String, Vec<AffectedColumn>, bool)> =
        vec![(own_edge.dst_id.clone(), root_columns, unknown_delta)];
    let mut seen: HashSet<String> = HashSet::from([own_edge.dst_id.clone()]);
    for hop in 1..=depth {
        let mut next = Vec::new();
        for (id, cols, opaque_so_far) in &frontier {
            for e in edges.iter().filter(|e| &e.src_id == id) {
                if !seen.insert(e.dst_id.clone()) {
                    continue;
                }
                let (affected_cols, opaque) = match edge_fields(e) {
                    Some(fields) if !opaque_so_far => {
                        let mut acc: Vec<AffectedColumn> = Vec::new();
                        for (out_col, ins) in &fields {
                            let reads: Vec<&AffectedColumn> =
                                cols.iter().filter(|c| ins.contains(&c.column)).collect();
                            // A column added upstream is not read by anything
                            // recorded yet, so only non-additive changes flow.
                            let breaking: Vec<&AffectedColumn> = reads
                                .iter()
                                .copied()
                                .filter(|c| c.change != ColumnChange::Added)
                                .collect();
                            if let Some(worst) = breaking.first() {
                                acc.push(AffectedColumn {
                                    column: out_col.clone(),
                                    change: worst.change.clone(),
                                    reads: breaking.iter().map(|c| c.column.clone()).collect(),
                                });
                            }
                        }
                        (acc, false)
                    }
                    _ => (Vec::new(), true),
                };
                let ds = datasets.get(&e.dst_id);
                let severity = if opaque {
                    Severity::Unknown
                } else {
                    affected_cols
                        .iter()
                        .map(|c| c.change.severity())
                        .max()
                        .unwrap_or(Severity::None)
                };
                if severity == Severity::None {
                    // Nothing it reads changes; nothing past it can either.
                    continue;
                }
                out.push(AffectedDataset {
                    id: e.dst_id.clone(),
                    uri: e.dst_uri.clone(),
                    pipeline: e.pipeline.clone(),
                    row: e.row.clone(),
                    depth: hop,
                    severity,
                    opaque,
                    columns: affected_cols.clone(),
                    contract: None,
                    owners: ds.map(|d| d.owners.clone()).unwrap_or_default(),
                    consumers: ds
                        .map(|d| affected_consumers(&d.consumers, &affected_cols, opaque))
                        .unwrap_or_default(),
                });
                next.push((e.dst_id.clone(), affected_cols, opaque));
            }
        }
        if next.is_empty() {
            break;
        }
        frontier = next;
    }
    out
}

/// Human rendering for `faucet plan --impact`.
pub fn render_human(r: &ImpactReport) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "  impact: {} ({} affected dataset(s), planned schema from {})\n",
        r.severity.as_str(),
        r.affected.len(),
        match r.planned_from {
            PlannedFrom::Sample => "the sample",
            PlannedFrom::Lineage => "the catalog's source schema",
            PlannedFrom::Unknown => "nothing — unknown",
        }
    ));
    if let Some(d) = &r.dataset {
        out.push_str(&format!(
            "    sink dataset: {} (last run {})\n",
            d.uri, d.last_run_id
        ));
    }
    let d = &r.delta;
    if d.is_empty() {
        out.push_str("    schema delta: none\n");
    } else {
        out.push_str("    schema delta:");
        if !d.added.is_empty() {
            out.push_str(&format!(" +{}", d.added.join(" +")));
        }
        if !d.removed.is_empty() {
            out.push_str(&format!(" -{}", d.removed.join(" -")));
        }
        for t in &d.retyped {
            out.push_str(&format!(" ~{}:{}→{}", t.column, t.from, t.to));
        }
        for n in &d.renamed {
            out.push_str(&format!(" {}→{}", n.from, n.to));
        }
        out.push('\n');
    }
    for a in &r.affected {
        out.push_str(&format!(
            "    [{}] depth {} {} (written by {} / {}){}\n",
            a.severity.as_str(),
            a.depth,
            a.uri,
            a.pipeline,
            a.row,
            if a.opaque {
                " — opaque path, column lineage unknown"
            } else {
                ""
            }
        ));
        for c in &a.columns {
            let what = match &c.change {
                ColumnChange::Added => "added".to_string(),
                ColumnChange::Removed => "removed".to_string(),
                ColumnChange::Retyped { from, to } => format!("retyped {from}→{to}"),
                ColumnChange::Renamed { to } => format!("renamed to {to}"),
            };
            if a.depth == 0 {
                out.push_str(&format!("      {}: {what}\n", c.column));
            } else {
                out.push_str(&format!(
                    "      {} reads {} ({what})\n",
                    c.column,
                    c.reads.join(", ")
                ));
            }
        }
        if let Some(c) = &a.contract {
            out.push_str(&format!(
                "      contract v{} of {} / {} declares: {}\n",
                c.version,
                c.pipeline,
                c.row,
                c.fields.join(", ")
            ));
        }
        if !a.owners.is_empty() {
            out.push_str(&format!("      owners: {}\n", a.owners.join(", ")));
        }
        for c in &a.consumers {
            out.push_str(&format!(
                "      consumer {} [{}]{}{}\n",
                c.name,
                c.severity.as_str(),
                c.kind
                    .as_deref()
                    .map(|k| format!(" ({k})"))
                    .unwrap_or_default(),
                c.contact
                    .as_deref()
                    .map(|k| format!(" → {k}"))
                    .unwrap_or_default(),
            ));
        }
    }
    if !r.owners.is_empty() {
        out.push_str(&format!("    owners to notify: {}\n", r.owners.join(", ")));
    }
    for n in &r.notes {
        out.push_str(&format!("    note: {n}\n"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::history::catalog::{
        CatalogAnnotation, CatalogUpdate, ConfigSnapshot, DatasetObservation, DatasetRole,
        RowSnapshot, dataset_id,
    };
    use crate::serve::history::memory::MemoryHistory;
    use chrono::Utc;
    use serde_json::json;

    fn schema(cols: &[(&str, &str)]) -> Value {
        let props: serde_json::Map<String, Value> = cols
            .iter()
            .map(|(n, t)| (n.to_string(), json!({ "type": t })))
            .collect();
        json!({ "type": "object", "properties": props })
    }

    fn update(
        pipeline: &str,
        row: &str,
        src: &str,
        src_schema: Value,
        dst: &str,
        dst_schema: Value,
        lineage: Option<Value>,
    ) -> CatalogUpdate {
        CatalogUpdate {
            run_id: format!("run-{pipeline}"),
            pipeline: pipeline.into(),
            row: row.into(),
            recorded_at: Utc::now(),
            sources: vec![DatasetObservation {
                uri: src.into(),
                kind: "csv".into(),
                role: DatasetRole::Source,
                schema: Some(src_schema),
                records: 10,
            }],
            sink: DatasetObservation {
                uri: dst.into(),
                kind: "jsonl".into(),
                role: DatasetRole::Sink,
                schema: Some(dst_schema),
                records: 10,
            },
            column_lineage: lineage,
        }
    }

    fn node(transforms_yaml: &str) -> ExpandedNode {
        let cfg = crate::config::PipelineConfig::from_text(
            &format!(
                "version: 1\nname: a\npipeline:\n  source: {{ type: csv, config: {{ path: ./in.csv }} }}\n  sink: {{ type: jsonl, config: {{ path: ./a.jsonl }} }}\n{transforms_yaml}"
            ),
            std::path::Path::new("a.yaml"),
        )
        .unwrap();
        crate::expand::expand(&cfg).unwrap().remove(0)
    }

    /// A → B → C chain: A writes a.jsonl; B reads a.jsonl (renames email →
    /// contact) into b.jsonl; C reads b.jsonl opaquely into c.jsonl.
    async fn seed() -> MemoryHistory {
        let store = MemoryHistory::new(std::time::Duration::from_secs(60));
        store
            .catalog_record(&update(
                "a",
                "row-0",
                "file:///in.csv",
                schema(&[
                    ("id", "integer"),
                    ("email", "string"),
                    ("amount", "integer"),
                ]),
                "file:///a.jsonl",
                schema(&[
                    ("id", "integer"),
                    ("email", "string"),
                    ("amount", "integer"),
                ]),
                Some(
                    json!({ "fields": { "id": ["id"], "email": ["email"], "amount": ["amount"] } }),
                ),
            ))
            .await
            .unwrap();
        store
            .catalog_record(&update(
                "b",
                "row-0",
                "file:///a.jsonl",
                schema(&[
                    ("id", "integer"),
                    ("email", "string"),
                    ("amount", "integer"),
                ]),
                "file:///b.jsonl",
                schema(&[("id", "integer"), ("contact", "string")]),
                Some(json!({ "fields": { "id": ["id"], "contact": ["email"] } })),
            ))
            .await
            .unwrap();
        store
            .catalog_record(&update(
                "c",
                "row-0",
                "file:///b.jsonl",
                schema(&[("id", "integer"), ("contact", "string")]),
                "file:///c.jsonl",
                schema(&[("blob", "string")]),
                None,
            ))
            .await
            .unwrap();
        store
            .catalog_annotate(
                &dataset_id("file:///b.jsonl"),
                &CatalogAnnotation {
                    owners: Some(vec!["team-b".into()]),
                    consumers: vec![
                        CatalogConsumer {
                            name: "contacts-dashboard".into(),
                            kind: Some("dashboard".into()),
                            contact: Some("#bi".into()),
                            columns: vec!["contact".into()],
                            registered_by: "test".into(),
                            registered_at: Utc::now(),
                        },
                        CatalogConsumer {
                            name: "id-export".into(),
                            kind: None,
                            contact: None,
                            columns: vec!["id".into()],
                            registered_by: "test".into(),
                            registered_at: Utc::now(),
                        },
                    ],
                    replace_consumers: false,
                },
            )
            .await
            .unwrap();
        store
    }

    #[tokio::test]
    async fn dropping_a_read_column_is_breaking_downstream_and_names_owners() {
        let store = seed().await;
        let n = node("  transforms:\n    - type: drop\n      config: { fields: [email] }\n");
        let planned = schema(&[("id", "integer"), ("amount", "integer")]);
        let r = analyze(
            &store,
            ImpactInputs {
                pipeline: "a",
                node: &n,
                planned_schema: Some(&planned),
                depth: 5,
            },
        )
        .await
        .unwrap();
        assert_eq!(r.severity, Severity::Breaking, "{r:?}");
        assert_eq!(r.delta.removed, vec!["email"]);
        assert_eq!(r.planned_from, PlannedFrom::Sample);
        // depth 0 = a.jsonl, depth 1 = b.jsonl (contact reads email), depth 2 = c (opaque).
        assert_eq!(r.affected.len(), 3);
        let b = &r.affected[1];
        assert_eq!(b.uri, "file:///b.jsonl");
        assert_eq!(b.severity, Severity::Breaking);
        assert_eq!(b.columns[0].column, "contact");
        assert_eq!(b.columns[0].reads, vec!["email"]);
        assert_eq!(b.owners, vec!["team-b"]);
        let names: Vec<&str> = b.consumers.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["contacts-dashboard"], "id-export reads only id");
        assert_eq!(b.consumers[0].severity, Severity::Breaking);
        let c = &r.affected[2];
        assert_eq!(c.severity, Severity::Unknown);
        assert!(c.opaque);
        assert_eq!(r.owners, vec!["team-b"]);
        let text = render_human(&r);
        assert!(
            text.contains("[breaking] depth 1 file:///b.jsonl"),
            "{text}"
        );
        assert!(text.contains("contact reads email (removed)"), "{text}");
        assert!(text.contains("owners to notify: team-b"), "{text}");
    }

    #[tokio::test]
    async fn adding_a_column_is_additive_and_does_not_reach_downstream() {
        let store = seed().await;
        let n = node("");
        let planned = schema(&[
            ("id", "integer"),
            ("email", "string"),
            ("amount", "integer"),
            ("country", "string"),
        ]);
        let r = analyze(
            &store,
            ImpactInputs {
                pipeline: "a",
                node: &n,
                planned_schema: Some(&planned),
                depth: 5,
            },
        )
        .await
        .unwrap();
        assert_eq!(r.severity, Severity::Additive);
        assert_eq!(r.delta.added, vec!["country"]);
        assert_eq!(r.affected.len(), 1, "{r:?}");
        assert_eq!(r.affected[0].depth, 0);
    }

    #[tokio::test]
    async fn rename_in_the_chain_is_reported_as_a_rename_and_breaks_readers() {
        let store = seed().await;
        let n = node(
            "  transforms:\n    - type: rename_field\n      config: { fields: { email: mail } }\n",
        );
        // No sample: planned from the source schema through the chain.
        let r = analyze(
            &store,
            ImpactInputs {
                pipeline: "a",
                node: &n,
                planned_schema: None,
                depth: 5,
            },
        )
        .await
        .unwrap();
        assert_eq!(r.planned_from, PlannedFrom::Lineage);
        assert_eq!(
            r.delta.renamed,
            vec![RenamedColumn {
                from: "email".into(),
                to: "mail".into()
            }]
        );
        assert!(r.delta.added.is_empty() && r.delta.removed.is_empty());
        assert_eq!(r.severity, Severity::Breaking);
        assert!(render_human(&r).contains("email→mail"));
    }

    #[tokio::test]
    async fn contract_downstream_escalates_and_names_the_version() {
        let store = seed().await;
        // B declares a contract over `contact`.
        let mut rows = std::collections::BTreeMap::new();
        rows.insert(
            "row-0".to_string(),
            RowSnapshot {
                source: crate::serve::history::catalog::ConnectorSnapshot {
                    kind: "jsonl".into(),
                    config: json!({}),
                },
                sink: crate::serve::history::catalog::ConnectorSnapshot {
                    kind: "jsonl".into(),
                    config: json!({}),
                },
                transforms: vec![],
                state_key: None,
                delivery_guarantee: "x".into(),
                on_error: "stop".into(),
                dlq: false,
                contract: Some(
                    json!({ "version": "2", "fields": [{ "name": "contact", "type": "string" }] }),
                ),
            },
        );
        store
            .catalog_record_config_snapshot(&ConfigSnapshot {
                pipeline: "b".into(),
                recorded_at: Utc::now(),
                faucet_version: "t".into(),
                rows,
            })
            .await
            .unwrap();
        let n = node("");
        let planned = schema(&[
            ("id", "integer"),
            ("email", "integer"),
            ("amount", "integer"),
        ]);
        let r = analyze(
            &store,
            ImpactInputs {
                pipeline: "a",
                node: &n,
                planned_schema: Some(&planned),
                depth: 5,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            r.delta.retyped,
            vec![RetypedColumn {
                column: "email".into(),
                from: "string".into(),
                to: "integer".into()
            }]
        );
        let b = &r.affected[1];
        let hit = b.contract.as_ref().expect("contract hit");
        assert_eq!(hit.version, "2");
        assert_eq!(hit.fields, vec!["contact"]);
        assert!(render_human(&r).contains("contract v2 of b / row-0 declares: contact"));
    }

    #[tokio::test]
    async fn never_run_row_reports_no_lineage() {
        let store = seed().await;
        let n = node("");
        let r = analyze(
            &store,
            ImpactInputs {
                pipeline: "never",
                node: &n,
                planned_schema: None,
                depth: 5,
            },
        )
        .await
        .unwrap();
        assert!(r.dataset.is_none());
        assert!(r.affected.is_empty());
        assert_eq!(r.severity, Severity::None);
        assert!(r.notes[0].contains("no recorded run"));
        assert!(render_human(&r).contains("note: row"));
    }

    #[tokio::test]
    async fn opaque_own_chain_without_sample_is_unknown() {
        let store = seed().await;
        let n = node("  transforms:\n    - type: flatten\n      config: {}\n");
        let r = analyze(
            &store,
            ImpactInputs {
                pipeline: "a",
                node: &n,
                planned_schema: None,
                depth: 5,
            },
        )
        .await
        .unwrap();
        assert_eq!(r.planned_from, PlannedFrom::Unknown);
        assert_eq!(r.severity, Severity::Unknown);
        assert!(r.affected.iter().all(|a| a.opaque));
        assert!(r.notes.iter().any(|n| n.contains("opaque")));
    }

    #[test]
    fn push_schema_types_follow_single_origins() {
        let src = schema(&[("a", "integer"), ("b", "string")]);
        let ops = vec![
            ColumnOp::Rename(vec![("a".into(), "x".into())]),
            ColumnOp::Set(vec!["lit".into()]),
        ];
        let out = push_schema(&src, &ops).unwrap();
        let cols = columns_of(&out);
        assert_eq!(cols["x"], "integer");
        assert_eq!(cols["b"], "string");
        assert_eq!(cols["lit"], "unknown");
        assert!(push_schema(&src, &[ColumnOp::Opaque]).is_none());
        assert_eq!(
            type_string(&json!({ "type": ["null", "string"] })),
            "null|string"
        );
        assert_eq!(type_string(&json!({})), "unknown");
    }
}
