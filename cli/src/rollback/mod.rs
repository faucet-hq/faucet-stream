//! `faucet rollback` — undo a run (#706).
//!
//! A run is undoable when the config carries a `rollback:` block: the
//! executor stamps a run-id column (`metadata_columns`), hands the sink a
//! [`RollbackWriteSpec`] so it
//! journals before-images / keeps the replaced table, and writes a
//! [`RunMarker`] (the bookmark and exactly-once token *before* the run) into
//! the row's state store. `faucet rollback --run <id>` then asks the sink to
//! undo the run per its write mode and rewinds the bookmark and watermark, so
//! the next run re-reads exactly what was undone.
//!
//! Undo is per dataset and all-or-nothing per dataset. A key a later run
//! changed since is a **conflict**: the whole dataset is left untouched unless
//! `--force`.

pub mod spec;
pub mod state;

pub use spec::RollbackSpec;
pub use state::{RunIndex, RunMarker};

use crate::auth_catalog::AuthCatalog;
use crate::config::PipelineConfig;
use crate::error::{CliError, CliResult};
use crate::expand::{ExpandedNode, NodeRole, expand};
use crate::registry::build_sink;
use crate::state::build_state_store;
use chrono::{DateTime, FixedOffset, Utc};
use faucet_core::rollback::{RollbackMode, RollbackOptions, RollbackOutcome, RollbackWriteSpec};
use faucet_core::{DeliveryMode, MetadataColumn, MetadataColumnsSpec, Sink, StateStore, WriteMode};
use serde::Serialize;
use serde_json::Value;
use std::sync::Arc;

/// Sink kinds that implement `Sink::rollback_run` (column-mapping mode).
/// Mirrors each sink's `supports_rollback` override.
pub const ROLLBACK_SINK_KINDS: &[&str] = &["postgres", "sqlite", "mysql"];

/// Whether a sink kind can undo a run.
pub fn sink_supports_rollback(kind: &str) -> bool {
    ROLLBACK_SINK_KINDS.contains(&kind)
}

/// The run-id column the metadata decorator stamps for `spec`
/// (`{prefix}_run_id`, default `_faucet_run_id`).
pub fn run_id_column(meta: Option<&MetadataColumnsSpec>) -> String {
    let prefix = meta
        .map(|m| m.prefix.as_str())
        .filter(|p| !p.trim().is_empty())
        .unwrap_or("_faucet");
    format!("{prefix}_{}", MetadataColumn::RunId.suffix())
}

/// The metadata-columns policy a rollback-enabled run needs: the config's own,
/// with `run_id` added when it is missing (or a `[run_id]`-only block when the
/// config has none). An explicitly disabled block is left alone — the
/// expand-time gate reports that.
pub fn ensure_run_id_column(meta: Option<&MetadataColumnsSpec>) -> MetadataColumnsSpec {
    match meta {
        None => MetadataColumnsSpec {
            columns: vec![MetadataColumn::RunId],
            ..MetadataColumnsSpec::default()
        },
        Some(m) => {
            let mut m = m.clone();
            // An empty list means the default set, which includes run_id.
            if !m.columns.is_empty() && !m.columns.contains(&MetadataColumn::RunId) {
                m.columns.push(MetadataColumn::RunId);
            }
            m
        }
    }
}

/// The write mode a sink config declares (`append` when absent).
pub fn write_mode_of(sink_cfg: &Value) -> WriteMode {
    match sink_cfg.get("write_mode").and_then(Value::as_str) {
        Some("upsert") => WriteMode::Upsert,
        Some("delete") => WriteMode::Delete,
        Some("overwrite") => WriteMode::Overwrite,
        _ => WriteMode::Append,
    }
}

/// Put the per-run rollback settings into a sink config's flattened
/// `WriteSpec` (the `rollback` key).
pub fn inject_write_spec(sink_cfg: &mut Value, spec: &RollbackSpec, run_id: &str, column: &str) {
    if let Value::Object(m) = sink_cfg {
        let ws = RollbackWriteSpec {
            run_id: run_id.to_string(),
            run_id_column: column.to_string(),
            journal: spec.journal,
            keep_previous: spec.keep_previous,
        };
        m.insert(
            "rollback".into(),
            serde_json::to_value(ws).unwrap_or(Value::Null),
        );
    }
}

/// Write the pre-run marker for `marker.run_id` and prune the retained set.
/// Reads the bookmark before the run (and the exactly-once token, when the
/// row is exactly-once) off the live store / sink. Pruned runs lose their
/// journal rows (`Sink::forget_run`) and marker. Never fails the run: a
/// marker that could not be written is logged and the run proceeds — it is
/// then simply not undoable.
pub async fn prepare(
    store: &dyn StateStore,
    sink: &dyn Sink,
    mut marker: RunMarker,
    retain: usize,
) -> CliResult<()> {
    marker.bookmark_before = store.get(&marker.state_key).await?;
    if marker.delivery == DeliveryMode::ExactlyOnce {
        marker.token_before = sink.last_committed_token(&marker.state_key).await?;
    }
    let mk = state::marker_key(&marker.state_key, &marker.run_id);
    store
        .put(
            &mk,
            &serde_json::to_value(&marker)
                .map_err(|e| CliError::Internal(format!("rollback marker: {e}")))?,
        )
        .await?;
    let ik = state::index_key(&marker.state_key);
    let mut index = RunIndex::decode(store.get(&ik).await?.as_ref());
    for old in index.push(&marker.run_id, retain) {
        if let Err(e) = sink.forget_run(&old).await {
            tracing::warn!(run_id = %old, error = %e, "could not drop the pruned run's journal");
        }
        if let Err(e) = store
            .delete(&state::marker_key(&marker.state_key, &old))
            .await
        {
            tracing::warn!(run_id = %old, error = %e, "could not drop the pruned run's marker");
        }
    }
    store
        .put(
            &ik,
            &serde_json::to_value(&index)
                .map_err(|e| CliError::Internal(format!("rollback index: {e}")))?,
        )
        .await?;
    Ok(())
}

/// Inputs to [`rollback`].
pub struct RollbackInputs {
    pub run_id: String,
    /// The row the run wrote (`None` = search every root's state for the
    /// run's marker).
    pub row: Option<String>,
    pub dry_run: bool,
    pub force: bool,
    pub pipeline_name: String,
    pub auth: AuthCatalog,
}

/// What a rollback did (or would do).
#[derive(Debug, Clone, Serialize)]
pub struct RollbackReport {
    pub run_id: String,
    pub row: String,
    pub sink_kind: String,
    pub dataset: String,
    pub mode: RollbackMode,
    pub dry_run: bool,
    #[serde(flatten)]
    pub outcome: RollbackOutcome,
    /// The bookmark was reset to its pre-run value (or cleared).
    pub bookmark_rewound: bool,
    /// The exactly-once watermark was reset to its pre-run token.
    pub token_rewound: bool,
    /// The rollback was refused because a later run changed the keys and
    /// `force` was not set (a dry run reports whether it *would* be).
    pub blocked: bool,
}

impl RollbackReport {
    /// Whether the rollback was refused because a later run changed the keys.
    pub fn blocked(&self) -> bool {
        self.blocked
    }
}

/// Undo `inputs.run_id` on the row that wrote it.
pub async fn rollback(cfg: &PipelineConfig, inputs: RollbackInputs) -> CliResult<RollbackReport> {
    let nodes = expand(cfg)?;
    let (node, store, marker) = locate(
        &nodes,
        &inputs.pipeline_name,
        &inputs.run_id,
        inputs.row.as_deref(),
    )
    .await?;
    rollback_node(&node, store, marker, &inputs).await
}

/// Undo a run against an already-expanded node, given its marker.
pub async fn rollback_node(
    node: &ExpandedNode,
    store: Arc<dyn StateStore>,
    marker: RunMarker,
    inputs: &RollbackInputs,
) -> CliResult<RollbackReport> {
    let mut sink_cfg = node.sink.config.clone();
    crate::executor::resolve_now_inplace(&mut sink_cfg, marker.clock)?;
    if let Value::Object(m) = &mut sink_cfg {
        m.remove("rollback");
    }
    let sink = build_sink(&node.sink.kind, sink_cfg, &inputs.auth).await?;
    if !sink.supports_rollback() {
        return Err(CliError::Config(format!(
            "rollback: sink '{}' cannot undo a run (supported: {}; SQL sinks need column mapping)",
            node.sink.kind,
            ROLLBACK_SINK_KINDS.join(", ")
        )));
    }
    let opts = RollbackOptions {
        run_id_column: marker.run_id_column.clone(),
        mode: marker.mode,
        force: inputs.force,
        dry_run: inputs.dry_run,
    };
    let outcome = sink.rollback_run(&marker.run_id, &opts).await?;
    let mut report = RollbackReport {
        run_id: marker.run_id.clone(),
        row: node.id.clone(),
        sink_kind: node.sink.kind.clone(),
        dataset: marker.sink_uri.clone(),
        mode: marker.mode,
        dry_run: inputs.dry_run,
        blocked: outcome.conflicts > 0 && !inputs.force,
        outcome,
        bookmark_rewound: false,
        token_rewound: false,
    };
    if !report.outcome.applied || inputs.dry_run {
        return Ok(report);
    }
    // The destination is undone; now make the next run re-read what was
    // undone: bookmark, then watermark, then drop what made the run undoable.
    match &marker.bookmark_before {
        Some(b) => store.put(&marker.state_key, b).await?,
        None => store.delete(&marker.state_key).await?,
    }
    report.bookmark_rewound = true;
    if marker.delivery == DeliveryMode::ExactlyOnce {
        sink.rewind_commit_token(&marker.state_key, marker.token_before.as_deref())
            .await?;
        report.token_rewound = true;
    }
    if let Err(e) = sink.forget_run(&marker.run_id).await {
        tracing::warn!(run_id = %marker.run_id, error = %e, "could not drop the run's journal");
    }
    store
        .delete(&state::marker_key(&marker.state_key, &marker.run_id))
        .await?;
    let ik = state::index_key(&marker.state_key);
    let mut index = RunIndex::decode(store.get(&ik).await?.as_ref());
    index.remove(&marker.run_id);
    store
        .put(
            &ik,
            &serde_json::to_value(&index)
                .map_err(|e| CliError::Internal(format!("rollback index: {e}")))?,
        )
        .await?;
    tracing::info!(
        run_id = %marker.run_id,
        row = %node.id,
        deleted = report.outcome.deleted,
        restored = report.outcome.restored,
        "run rolled back"
    );
    Ok(report)
}

/// Find the root node whose state holds `run_id`'s marker (or the given row),
/// its state store, and the marker.
pub async fn locate(
    nodes: &[ExpandedNode],
    pipeline_name: &str,
    run_id: &str,
    row: Option<&str>,
) -> CliResult<(ExpandedNode, Arc<dyn StateStore>, RunMarker)> {
    let roots: Vec<&ExpandedNode> = nodes
        .iter()
        .filter(|n| matches!(n.role, NodeRole::Root) && row.is_none_or(|r| n.id == r))
        .collect();
    if roots.is_empty() {
        return Err(CliError::Config(match row {
            Some(r) => format!("rollback: row '{r}' is not a root row of this config"),
            None => "rollback: config has no root pipeline".to_string(),
        }));
    }
    let mut searched = Vec::new();
    for node in roots {
        let Some(spec) = &node.state else {
            searched.push(node.id.clone());
            continue;
        };
        let store = build_state_store(spec).await?;
        let state_key = crate::executor::build_state_key(pipeline_name, &node.id, None);
        if let Some(v) = store.get(&state::marker_key(&state_key, run_id)).await? {
            let marker: RunMarker = serde_json::from_value(v).map_err(|e| {
                CliError::Config(format!("rollback: malformed marker for run {run_id}: {e}"))
            })?;
            return Ok((node.clone(), store, marker));
        }
        searched.push(node.id.clone());
    }
    Err(CliError::Config(format!(
        "rollback: no undoable run '{run_id}' in the state of row(s) {} — was the run made with a \
         `rollback:` block (and a durable `state:`), and is it within `rollback.retain`? \
         `faucet rollback --list` shows the undoable runs.",
        searched.join(", ")
    )))
}

/// The undoable runs of every root row (newest first per row).
pub async fn list(
    cfg: &PipelineConfig,
    pipeline_name: &str,
    row: Option<&str>,
) -> CliResult<Vec<RunMarker>> {
    let nodes = expand(cfg)?;
    let mut out = Vec::new();
    for node in nodes
        .iter()
        .filter(|n| matches!(n.role, NodeRole::Root) && row.is_none_or(|r| n.id == r))
    {
        let Some(spec) = &node.state else { continue };
        let store = build_state_store(spec).await?;
        let state_key = crate::executor::build_state_key(pipeline_name, &node.id, None);
        let index = RunIndex::decode(store.get(&state::index_key(&state_key)).await?.as_ref());
        for id in index.runs.iter().rev() {
            if let Some(v) = store.get(&state::marker_key(&state_key, id)).await?
                && let Ok(m) = serde_json::from_value::<RunMarker>(v)
            {
                out.push(m);
            }
        }
    }
    Ok(out)
}

/// Build the marker the executor writes before a run.
#[allow(clippy::too_many_arguments)]
pub fn marker_for(
    run_id: &str,
    pipeline: &str,
    node: &ExpandedNode,
    state_key: &str,
    clock: DateTime<FixedOffset>,
    sink_uri: String,
    run_id_column: String,
) -> RunMarker {
    RunMarker {
        run_id: run_id.to_string(),
        pipeline: pipeline.to_string(),
        row: node.id.clone(),
        state_key: state_key.to_string(),
        started_at: Utc::now(),
        clock,
        sink_kind: node.sink.kind.clone(),
        sink_uri,
        mode: RollbackMode::for_write_mode(write_mode_of(&node.sink.config)),
        delivery: node.delivery,
        run_id_column,
        bookmark_before: None,
        token_before: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn run_id_column_follows_the_prefix() {
        assert_eq!(run_id_column(None), "_faucet_run_id");
        let m = MetadataColumnsSpec {
            prefix: "_meta".into(),
            ..MetadataColumnsSpec::default()
        };
        assert_eq!(run_id_column(Some(&m)), "_meta_run_id");
        let blank = MetadataColumnsSpec {
            prefix: "  ".into(),
            ..MetadataColumnsSpec::default()
        };
        assert_eq!(run_id_column(Some(&blank)), "_faucet_run_id");
    }

    #[test]
    fn ensure_run_id_adds_the_column_only_when_missing() {
        let none = ensure_run_id_column(None);
        assert_eq!(none.columns, vec![MetadataColumn::RunId]);
        let default_set = ensure_run_id_column(Some(&MetadataColumnsSpec::default()));
        assert!(
            default_set.columns.is_empty(),
            "empty = default set, which has run_id"
        );
        let partial = MetadataColumnsSpec {
            columns: vec![MetadataColumn::Source],
            ..MetadataColumnsSpec::default()
        };
        let fixed = ensure_run_id_column(Some(&partial));
        assert_eq!(
            fixed.columns,
            vec![MetadataColumn::Source, MetadataColumn::RunId]
        );
        let has = ensure_run_id_column(Some(&fixed));
        assert_eq!(has.columns.len(), 2, "not added twice");
    }

    #[test]
    fn write_spec_injection_and_mode() {
        let mut cfg = json!({"table_name": "t", "write_mode": "upsert", "key": ["id"]});
        assert_eq!(write_mode_of(&cfg), WriteMode::Upsert);
        assert_eq!(write_mode_of(&json!({})), WriteMode::Append);
        assert_eq!(
            write_mode_of(&json!({"write_mode": "delete"})),
            WriteMode::Delete
        );
        assert_eq!(
            write_mode_of(&json!({"write_mode": "overwrite"})),
            WriteMode::Overwrite
        );
        let spec = RollbackSpec {
            keep_previous: false,
            ..RollbackSpec::default()
        };
        inject_write_spec(&mut cfg, &spec, "r1", "_faucet_run_id");
        assert_eq!(
            cfg["rollback"],
            json!({"run_id": "r1", "run_id_column": "_faucet_run_id", "journal": true, "keep_previous": false})
        );
        let mut scalar = json!(1);
        inject_write_spec(&mut scalar, &spec, "r1", "c");
        assert_eq!(scalar, json!(1));
    }

    #[test]
    fn report_blocked_means_conflicts_without_apply() {
        let base = RollbackReport {
            run_id: "r".into(),
            row: "row".into(),
            sink_kind: "sqlite".into(),
            dataset: "d".into(),
            mode: RollbackMode::Upsert,
            dry_run: false,
            outcome: RollbackOutcome::blocked(2),
            bookmark_rewound: false,
            token_rewound: false,
            blocked: true,
        };
        assert!(base.blocked());
        let ok = RollbackReport {
            outcome: RollbackOutcome {
                applied: true,
                ..Default::default()
            },
            blocked: false,
            ..base
        };
        assert!(!ok.blocked());
    }

    #[tokio::test]
    async fn prepare_writes_marker_and_prunes_via_the_sink() {
        use faucet_core::MemoryStateStore;
        struct S(std::sync::Mutex<Vec<String>>);
        #[async_trait::async_trait]
        impl Sink for S {
            async fn write_batch(&self, _r: &[Value]) -> Result<usize, faucet_core::FaucetError> {
                Ok(0)
            }
            fn config_schema(&self) -> Value {
                json!({})
            }
            async fn forget_run(&self, run_id: &str) -> Result<(), faucet_core::FaucetError> {
                self.0.lock().unwrap().push(run_id.to_string());
                Ok(())
            }
            async fn last_committed_token(
                &self,
                _scope: &str,
            ) -> Result<Option<String>, faucet_core::FaucetError> {
                Ok(Some("tok".into()))
            }
        }
        let store = MemoryStateStore::new();
        store
            .put("p::row", &json!({"updated_at": "2026-01-01"}))
            .await
            .unwrap();
        let sink = S(Default::default());
        let node = crate::expand::expand(
            &crate::config::PipelineConfig::from_text(
                "version: 1\nname: p\npipeline:\n  source: {type: csv, config: {path: x.csv}}\n  sink: {type: sqlite, config: {database_url: 'sqlite::memory:', table_name: t, write_mode: upsert, key: [id]}}\n  state: {type: memory}\n",
                std::path::Path::new("p.yaml"),
            )
            .unwrap(),
        )
        .unwrap()
        .remove(0);
        for id in ["a", "b", "c"] {
            let mut m = marker_for(
                "x",
                "p",
                &node,
                "p::row",
                Utc::now().fixed_offset(),
                "u".into(),
                "_faucet_run_id".into(),
            );
            m.run_id = id.into();
            m.delivery = DeliveryMode::ExactlyOnce;
            prepare(&store, &sink, m, 2).await.unwrap();
        }
        // `a` was pruned: journal forgotten, marker gone; `b`/`c` retained.
        assert_eq!(*sink.0.lock().unwrap(), vec!["a".to_string()]);
        assert!(
            store
                .get("p::row::__rollback__::a")
                .await
                .unwrap()
                .is_none()
        );
        let c: RunMarker =
            serde_json::from_value(store.get("p::row::__rollback__::c").await.unwrap().unwrap())
                .unwrap();
        assert_eq!(c.bookmark_before, Some(json!({"updated_at": "2026-01-01"})));
        assert_eq!(c.token_before.as_deref(), Some("tok"));
        assert_eq!(c.mode, RollbackMode::Upsert);
        let idx = RunIndex::decode(store.get("p::row::__rollback__").await.unwrap().as_ref());
        assert_eq!(idx.runs, vec!["b", "c"]);
    }
}
