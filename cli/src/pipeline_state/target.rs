//! Resolve a config into the rows whose state `faucet state` / `faucet status`
//! operate on: matrix rows (roots, children, products) or, in topology mode,
//! sink nodes — each with its state store spec and the facts about its sink.

use crate::config::{DlqSpec, PipelineConfig, StateStoreSpec};
use crate::error::{CliError, CliResult};
use crate::expand::{NodeRole, expand};
use serde::Serialize;
use serde_json::Value;

/// How a row runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RowRole {
    /// Runs once per pipeline run.
    Root,
    /// Runs once per parent record; its bookmarks are `{name}::{row}::{key}`.
    Child,
    /// Runs once per discovery tuple.
    Product,
    /// A topology sink node.
    TopologySink,
}

/// One row with everything the state/status tools need.
#[derive(Debug, Clone)]
pub struct RowTarget {
    pub id: String,
    pub role: RowRole,
    pub parent: Option<String>,
    pub state: Option<StateStoreSpec>,
    pub sink_kind: String,
    pub sink_config: Value,
    pub dlq: Option<DlqSpec>,
    pub sla: Option<crate::sla::SlaSpec>,
    pub profiling: bool,
    /// The row commits a per-page watermark with its data (the
    /// atomic-watermark exactly-once mechanism).
    pub atomic_watermark: bool,
    pub overwrite: bool,
}

impl RowTarget {
    /// Whether the row keeps durable state between runs.
    pub fn durable(&self) -> bool {
        self.state.as_ref().is_some_and(|s| s.kind != "memory")
    }
}

/// A config resolved for the state/status tools.
#[derive(Debug, Clone)]
pub struct PipelineTarget {
    pub pipeline: String,
    pub topology: bool,
    /// Source nodes in a topology (1 for a matrix config).
    pub source_count: usize,
    /// The pipeline-level store (`pipeline.state`), where the pipeline markers
    /// (replication, backfill) live.
    pub state: Option<StateStoreSpec>,
    pub rows: Vec<RowTarget>,
}

fn is_overwrite(sink_config: &Value) -> bool {
    sink_config.get("write_mode").and_then(Value::as_str) == Some("overwrite")
}

impl PipelineTarget {
    /// Resolve `cfg` (named `pipeline`) into its rows.
    pub fn resolve(cfg: &PipelineConfig, pipeline: &str) -> CliResult<Self> {
        faucet_core::state::validate_state_key(pipeline).map_err(|e| {
            CliError::Config(format!("pipeline name '{pipeline}' cannot key state: {e}"))
        })?;
        if crate::topology::is_topology(cfg) {
            let atomic = cfg.delivery == faucet_core::DeliveryMode::ExactlyOnce;
            let rows = crate::topology::sink_nodes(cfg)?
                .into_iter()
                .map(|(id, kind, config)| RowTarget {
                    atomic_watermark: atomic
                        && crate::registry::sink_supports_idempotent_writes(&kind),
                    overwrite: is_overwrite(&config),
                    id,
                    role: RowRole::TopologySink,
                    parent: None,
                    state: cfg.pipeline.state.clone(),
                    sink_kind: kind,
                    sink_config: config,
                    dlq: cfg.pipeline.dlq.clone(),
                    sla: cfg.sla.clone(),
                    profiling: cfg.profiling.is_some(),
                })
                .collect();
            return Ok(Self {
                pipeline: pipeline.to_string(),
                topology: true,
                source_count: crate::topology::source_node_count(cfg),
                state: cfg.pipeline.state.clone(),
                rows,
            });
        }
        let mut rows = Vec::new();
        for node in expand(cfg)? {
            let (role, parent) = match &node.role {
                NodeRole::Root => (RowRole::Root, None),
                NodeRole::Child { parent_id, .. } => (RowRole::Child, Some(parent_id.clone())),
                NodeRole::Product { .. } => (RowRole::Product, None),
                NodeRole::Discovery { .. } => continue,
            };
            rows.push(RowTarget {
                atomic_watermark: node.delivery_guarantee
                    == faucet_core::DeliveryGuarantee::EffectivelyOnce(
                        faucet_core::EffectivelyOnceMechanism::AtomicWatermark,
                    ),
                overwrite: is_overwrite(&node.sink.config),
                sla: node.sla.clone().or_else(|| cfg.sla.clone()),
                profiling: node.profiling.is_some(),
                id: node.id,
                role,
                parent,
                state: node.state,
                sink_kind: node.sink.kind,
                sink_config: node.sink.config,
                dlq: node.dlq,
            });
        }
        // `faucet mirror` runs the root as `cdc` after a one-time `snapshot`.
        if cfg.replication.is_some()
            && let Some(root) = rows.iter_mut().find(|r| r.role == RowRole::Root)
        {
            root.id = "cdc".to_string();
            let mut snapshot = root.clone();
            snapshot.id = "snapshot".to_string();
            snapshot.atomic_watermark = false;
            rows.push(snapshot);
        }
        Ok(Self {
            pipeline: pipeline.to_string(),
            topology: false,
            source_count: 1,
            state: cfg.pipeline.state.clone(),
            rows,
        })
    }

    /// The row named `id`, or an error listing the rows there are.
    pub fn row(&self, id: &str) -> CliResult<&RowTarget> {
        self.rows.iter().find(|r| r.id == id).ok_or_else(|| {
            CliError::Config(format!(
                "no row '{id}' in pipeline '{}' (rows: {})",
                self.pipeline,
                self.rows
                    .iter()
                    .map(|r| r.id.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        })
    }

    /// Rows selected by an optional `--row`.
    pub fn select(&self, row: Option<&str>) -> CliResult<Vec<&RowTarget>> {
        match row {
            Some(id) => Ok(vec![self.row(id)?]),
            None => Ok(self.rows.iter().collect()),
        }
    }

    /// The invocation key of a row (`{name}::{row}`).
    pub fn base_key(&self, row: &str) -> String {
        crate::executor::build_state_key(&self.pipeline, row, None)
    }
}

/// The pipeline name a CLI invocation keys state under: the config's `name`,
/// else the config file's stem (what `faucet run` uses).
pub fn cli_pipeline_name(cfg: &PipelineConfig, path: &std::path::Path) -> String {
    cfg.name.clone().unwrap_or_else(|| {
        path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("pipeline")
            .to_owned()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn parse(text: &str) -> PipelineConfig {
        PipelineConfig::from_text(text, Path::new("t.yaml")).unwrap()
    }

    #[test]
    fn resolves_matrix_rows_with_roles_and_facts() {
        let cfg = parse(
            r#"version: 1
name: orders
pipeline:
  source: { type: csv, config: { path: in.csv } }
  sink: { type: jsonl, config: { path: out.jsonl } }
  state: { type: file, config: { path: ./state } }
matrix:
  - id: parent
  - id: kid
    parent: parent
    parent_key: id
    sink: { config: { path: "k-${parent.id}.jsonl" } }
  - id: mem
    state: { type: memory, config: {} }
"#,
        );
        let t = PipelineTarget::resolve(&cfg, "orders").unwrap();
        assert!(!t.topology);
        assert_eq!(t.rows.len(), 3);
        let kid = t.row("kid").unwrap();
        assert_eq!(kid.role, RowRole::Child);
        assert_eq!(kid.parent.as_deref(), Some("parent"));
        assert!(t.row("parent").unwrap().durable());
        assert!(!t.row("mem").unwrap().durable());
        assert!(!t.row("parent").unwrap().atomic_watermark);
        assert_eq!(t.base_key("parent"), "orders::parent");
        let err = t.row("nope").unwrap_err().to_string();
        assert!(err.contains("rows: parent, mem, kid"), "{err}");
        assert_eq!(t.select(None).unwrap().len(), 3);
        assert_eq!(t.select(Some("mem")).unwrap().len(), 1);
    }

    #[test]
    fn resolves_topology_sink_nodes() {
        let cfg = parse(
            r#"version: 1
name: topo
pipeline:
  sources: { src: { type: csv, config: { path: in.csv } } }
  sinks:
    a: { type: jsonl, config: { path: a.jsonl } }
    b: { type: sqlite, config: { database_url: "sqlite://x.db", table_name: t, write_mode: overwrite } }
  state: { type: file, config: { path: ./state } }
  nodes:
    read: { kind: source, ref: src }
    fan: { kind: tee, fanout: 2 }
    left: { kind: sink, ref: a }
    right: { kind: sink, ref: b }
  edges:
    - { from: read, to: fan }
    - { from: fan, to: left }
    - { from: fan, to: right }
"#,
        );
        let t = PipelineTarget::resolve(&cfg, "topo").unwrap();
        assert!(t.topology);
        assert_eq!(t.source_count, 1);
        let ids: Vec<_> = t.rows.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, vec!["left", "right"]);
        assert_eq!(t.rows[0].role, RowRole::TopologySink);
        assert!(t.rows[1].overwrite);
        assert_eq!(t.rows[1].sink_kind, "sqlite");
    }

    #[test]
    fn mirror_configs_name_their_rows_like_the_orchestrator() {
        let cfg = parse(
            r#"version: 1
name: m
pipeline:
  source: { type: postgres-cdc, config: { connection_url: "postgres://u@h/d", slot_name: s, publication: p } }
  sink: { type: jsonl, config: { path: out.jsonl } }
  state: { type: file, config: { path: ./state } }
mirror:
  mode: snapshot_then_cdc
  snapshot:
    source: { type: postgres, config: { connection_url: "postgres://u@h/d", query: "SELECT 1" } }
"#,
        );
        let t = PipelineTarget::resolve(&cfg, "m").unwrap();
        let ids: Vec<_> = t.rows.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, vec!["cdc", "snapshot"]);
    }

    #[test]
    fn rejects_an_unkeyable_pipeline_name_and_names_from_the_file() {
        let cfg = parse(
            r#"version: 1
pipeline:
  source: { type: csv, config: { path: in.csv } }
  sink: { type: jsonl, config: { path: out.jsonl } }
"#,
        );
        assert!(PipelineTarget::resolve(&cfg, "bad name").is_err());
        assert_eq!(
            cli_pipeline_name(&cfg, Path::new("/x/orders.yaml")),
            "orders"
        );
        let named = parse(
            "version: 1\nname: n\npipeline:\n  source: { type: csv, config: { path: a } }\n  sink: { type: jsonl, config: { path: b } }\n",
        );
        assert_eq!(cli_pipeline_name(&named, Path::new("/x/orders.yaml")), "n");
    }
}
