//! Backfill orchestration: plan units → gate → run each unit through
//! `executor::run_expanded` under bounded concurrency → record every unit's
//! terminal outcome in the durable progress marker.
//!
//! Reuses `expand` (so every config gate applies) and the executor (so
//! transforms / quality / contract / masking / DLQ / flush-completing cancel
//! all behave exactly like `faucet run`). Each unit runs the selected root
//! node with:
//! - `${backfill.*}` tokens substituted in its source + sink configs,
//! - the `${now.*}` clock set to the unit's window start,
//! - a namespaced row id (`backfill::{unit}`) so its state key never touches
//!   the forward-sync bookmark,
//! - delivery forced to at-least-once (pair with `write_mode: upsert` for
//!   idempotent replays).

use crate::auth_catalog::AuthCatalog;
use crate::backfill::plan::{
    BackfillUnit, WARN_UNITS, plan_windows, range_hash, substitute_unit_tokens,
};
use crate::backfill::spec::has_scoping_tokens;
use crate::backfill::state::{
    BackfillState, marker_key, split_remaining, unit_row_id, unit_state_key,
};
use crate::config::{ExecutionSpec, PipelineConfig};
use crate::error::{CliError, CliResult};
use crate::executor::{ExecuteOptions, run_expanded};
use crate::expand::{ExpandedNode, expand};
use chrono::{DateTime, FixedOffset};
use faucet_core::{FaucetError, StateStore, Stream, StreamPage, json_gt};
use serde::Serialize;
use serde_json::Value;
use std::pin::Pin;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// The requested replay range.
#[derive(Debug, Clone)]
pub enum BackfillRange {
    /// Wall-clock window (`--from` / `--to`), chunked by `window`.
    Time {
        from: DateTime<FixedOffset>,
        to: DateTime<FixedOffset>,
        window: Option<crate::backfill::plan::WindowStep>,
        tz: chrono_tz::Tz,
    },
    /// Explicit bookmark range (`--from-bookmark` / `--to-bookmark`): seed
    /// the unit's scoped state with `from`, optionally drop records whose
    /// `field` exceeds `to`. Always a single unit.
    Bookmark {
        from: Value,
        to: Option<Value>,
        field: Option<String>,
    },
}

impl BackfillRange {
    /// Canonical descriptor — the marker hash input and the operator-facing
    /// identity of this backfill.
    fn descriptor(&self, row: &str) -> String {
        match self {
            Self::Time {
                from, to, window, ..
            } => format!(
                "time|{}|{}|{}|{row}",
                from.to_rfc3339(),
                to.to_rfc3339(),
                window
                    .map(|w| w.to_string())
                    .unwrap_or_else(|| "whole".into()),
            ),
            Self::Bookmark { from, to, .. } => format!(
                "bookmark|{from}|{}|{row}",
                to.as_ref().map(Value::to_string).unwrap_or_default()
            ),
        }
    }
}

/// Inputs for one `faucet backfill` invocation.
pub struct BackfillOptions {
    pub pipeline_name: String,
    pub execution: Option<ExecutionSpec>,
    pub auth: AuthCatalog,
    pub resilience: Option<faucet_core::ResiliencePolicy>,
    pub range: BackfillRange,
    /// Max concurrently-running units (≥ 1).
    pub concurrency: usize,
    /// Root row to backfill; `None` = the config's only root.
    pub row: Option<String>,
    /// Redirect writes to this named sink template (`--into`).
    pub into_sink: Option<String>,
    /// Plan and report without running anything.
    pub dry_run: bool,
    /// Continue a previous backfill of the same range (skip done units).
    pub resume: bool,
    /// Discard a previous marker for this range and start over.
    pub restart: bool,
    /// External cancel (serve); `None` installs a SIGTERM/Ctrl-C handler.
    pub cancel: Option<CancellationToken>,
}

/// Per-unit report line.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct UnitReport {
    pub unit: String,
    pub start: String,
    pub end: String,
    /// `pending` (dry-run) | `done` | `failed` | `skipped` (resume).
    pub outcome: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Overall backfill result.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct BackfillOutcome {
    pub descriptor: String,
    pub planned: usize,
    pub skipped: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub dry_run: bool,
    pub units: Vec<UnitReport>,
}

/// Select the root node to backfill: `--row`, or the config's only root.
fn select_root(nodes: Vec<ExpandedNode>, row: Option<&str>) -> CliResult<ExpandedNode> {
    let roots: Vec<ExpandedNode> = nodes
        .into_iter()
        .filter(|n| matches!(n.role, crate::expand::NodeRole::Root))
        .collect();
    match row {
        Some(id) => {
            let available: Vec<String> = roots.iter().map(|n| n.id.clone()).collect();
            roots.into_iter().find(|n| n.id == id).ok_or_else(|| {
                CliError::Config(format!(
                    "no root row named '{id}' — available: {}",
                    available.join(", ")
                ))
            })
        }
        None => {
            if roots.len() > 1 {
                return Err(CliError::Config(format!(
                    "the config has {} root rows — pick one with --row ({})",
                    roots.len(),
                    roots
                        .iter()
                        .map(|n| n.id.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )));
            }
            roots
                .into_iter()
                .next()
                .ok_or_else(|| CliError::Config("the config has no root rows".into()))
        }
    }
}

/// Build one unit's node from the selected root: namespaced id (scoped state
/// key), `${backfill.*}` tokens substituted, delivery forced to
/// at-least-once (mirroring the `replicate` snapshot phase).
fn build_unit_node(
    root: &ExpandedNode,
    unit: &BackfillUnit,
    time_mode: bool,
) -> CliResult<ExpandedNode> {
    let mut n = root.clone();
    n.id = unit_row_id(&unit.id);
    if time_mode {
        substitute_unit_tokens(&mut n.source.config, unit)?;
        substitute_unit_tokens(&mut n.sink.config, unit)?;
    }
    n.delivery = faucet_core::DeliveryMode::AtLeastOnce;
    if n.delivery_guarantee
        != faucet_core::DeliveryGuarantee::EffectivelyOnce(
            faucet_core::EffectivelyOnceMechanism::KeyedUpsert,
        )
    {
        n.delivery_guarantee = faucet_core::DeliveryGuarantee::AtLeastOnce;
    }
    Ok(n)
}

/// Whether the sink dedups replayed rows (`write_mode: upsert` / `delete`).
fn sink_dedups(node: &ExpandedNode) -> bool {
    matches!(
        node.sink.config.get("write_mode").and_then(Value::as_str),
        Some("upsert") | Some("delete")
    )
}

/// A source wrapper that drops records whose `field` orders **after** the
/// `--to-bookmark` upper bound (missing field = kept). Everything else —
/// bookmarks, state identity, schema — delegates to the wrapped source.
struct BoundedSource {
    inner: Box<dyn faucet_core::Source>,
    field: String,
    to: Value,
}

impl BoundedSource {
    fn within_bound(&self, record: &Value) -> bool {
        match record.get(&self.field) {
            Some(v) => !json_gt(v, &self.to),
            None => true,
        }
    }
}

#[faucet_core::async_trait]
impl faucet_core::Source for BoundedSource {
    async fn fetch_with_context(
        &self,
        context: &std::collections::HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let records = self.inner.fetch_with_context(context).await?;
        Ok(records
            .into_iter()
            .filter(|r| self.within_bound(r))
            .collect())
    }

    async fn fetch_with_context_incremental(
        &self,
        context: &std::collections::HashMap<String, Value>,
    ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        let (records, bookmark) = self.inner.fetch_with_context_incremental(context).await?;
        Ok((
            records
                .into_iter()
                .filter(|r| self.within_bound(r))
                .collect(),
            bookmark,
        ))
    }

    fn stream_pages<'a>(
        &'a self,
        context: &'a std::collections::HashMap<String, Value>,
        batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        use futures::StreamExt;
        let inner = self.inner.stream_pages(context, batch_size);
        Box::pin(inner.map(move |page| {
            page.map(|p| StreamPage {
                records: p
                    .records
                    .into_iter()
                    .filter(|r| self.within_bound(r))
                    .collect(),
                bookmark: p.bookmark,
            })
        }))
    }

    fn state_key(&self) -> Option<String> {
        self.inner.state_key()
    }

    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        self.inner.apply_start_bookmark(bookmark).await
    }

    fn config_schema(&self) -> Value {
        self.inner.config_schema()
    }

    fn connector_name(&self) -> &'static str {
        self.inner.connector_name()
    }

    fn dataset_uri(&self) -> String {
        self.inner.dataset_uri()
    }
}

/// Build a fresh `ExecuteOptions` for one unit run.
fn make_opts(
    opts: &BackfillOptions,
    clock: DateTime<FixedOffset>,
    cancel: CancellationToken,
) -> ExecuteOptions {
    ExecuteOptions {
        pipeline_name: opts.pipeline_name.clone(),
        run_id: None,
        execution: opts.execution.clone(),
        concurrency: None,
        dry_run: false,
        limit: None,
        state_path_override: None,
        shard: None,
        auth: opts.auth.clone(),
        clock,
        cancel: Some(cancel),
        resilience: opts.resilience.clone(),
        // SLA history and catalog volumes describe the forward sync, not a
        // historical replay — a backfill must not pollute either.
        sla: None,
        reconcile: None,
        #[cfg(feature = "lineage")]
        lineage: None,
        #[cfg(feature = "lineage")]
        lineage_cfg: None,
        #[cfg(feature = "notify")]
        notifier: None,
        #[cfg(feature = "catalog")]
        catalog: None,
    }
}

/// Run a backfill. Returns the per-unit outcome table; `Err` only for
/// planning/gating/config failures (unit failures are reported in the
/// outcome and via a non-`Ok` summary the caller maps to an exit code).
pub async fn run_backfill(
    cfg: &PipelineConfig,
    opts: BackfillOptions,
) -> CliResult<BackfillOutcome> {
    let nodes = expand(cfg)?;
    let root = select_root(nodes, opts.row.as_deref())?;
    let mut root = root;

    // `--into`: redirect writes at a named sink template (staging-first).
    if let Some(name) = &opts.into_sink {
        let spec = cfg.pipeline.sinks.get(name).ok_or_else(|| {
            let mut available: Vec<&str> = cfg.pipeline.sinks.keys().map(String::as_str).collect();
            available.sort_unstable();
            CliError::Config(format!(
                "--into '{name}' does not name a sink template under pipeline.sinks — \
                 available: {}",
                if available.is_empty() {
                    "none".to_string()
                } else {
                    available.join(", ")
                }
            ))
        })?;
        root.sink = spec.clone();
        root.sink_ref = name.clone();
    }

    let time_mode = matches!(opts.range, BackfillRange::Time { .. });

    // ── Gates ────────────────────────────────────────────────────────────────
    if time_mode {
        let serialized = root.source.config.to_string();
        if !has_scoping_tokens(&serialized) {
            return Err(CliError::Config(format!(
                "source '{}' is not scoped to the backfill window — its config references \
                 no `${{backfill.start}}` / `${{backfill.end}}` / `${{now.*}}` token, so every \
                 window would replay identical data. Add a window predicate (e.g. \
                 `query: … WHERE updated_at >= '${{backfill.start}}' AND updated_at < \
                 '${{backfill.end}}'`), or use --from-bookmark for bookmark-positioned \
                 sources",
                root.source.kind
            )));
        }
    } else if root.state.is_none() {
        return Err(CliError::Config(
            "--from-bookmark requires a `state:` block — the bookmark is seeded into the \
             backfill's scoped state key"
                .into(),
        ));
    }
    if let BackfillRange::Bookmark {
        to: Some(_), field, ..
    } = &opts.range
        && field.is_none()
    {
        return Err(CliError::Config(
            "--to-bookmark requires --bookmark-field naming the record field the bound \
             applies to"
                .into(),
        ));
    }
    if !sink_dedups(&root) {
        tracing::warn!(
            sink = %root.sink.kind,
            "backfill sink is append-only — replaying an overlapping window will duplicate \
             rows. Recommended: `write_mode: upsert` with a `key` (or --into a staging sink)"
        );
    }

    // ── Plan ─────────────────────────────────────────────────────────────────
    let units = match &opts.range {
        BackfillRange::Time {
            from,
            to,
            window,
            tz,
        } => plan_windows(*from, *to, *window, *tz)?,
        BackfillRange::Bookmark { .. } => vec![BackfillUnit {
            id: "bookmark".into(),
            start: chrono::Utc::now().fixed_offset(),
            end: chrono::Utc::now().fixed_offset(),
        }],
    };
    if units.len() > WARN_UNITS {
        tracing::warn!(
            units = units.len(),
            "large backfill plan — consider a bigger --window"
        );
    }
    let descriptor = opts.range.descriptor(&root.id);
    let marker_k = marker_key(&opts.pipeline_name, &range_hash(&descriptor));

    // ── Marker (durable when a state store is configured) ────────────────────
    let store: Arc<dyn StateStore> = match cfg.pipeline.state.as_ref() {
        Some(spec) => crate::state::build_state_store(spec).await?,
        None => {
            tracing::warn!(
                "no `state:` block — backfill progress is not durable and --resume will \
                 not survive a restart"
            );
            Arc::new(faucet_core::MemoryStateStore::new())
        }
    };
    let marker = match store.get(&marker_k).await? {
        Some(v) if opts.restart => {
            let prior = BackfillState::from_value(v)?;
            tracing::warn!(
                done = prior.done_count(),
                failed = prior.failed_count(),
                "--restart: discarding the previous progress marker for this range"
            );
            BackfillState::new(descriptor.clone())
        }
        Some(v) => {
            let prior = BackfillState::from_value(v)?;
            if !opts.resume && !opts.dry_run {
                return Err(CliError::Config(format!(
                    "a previous backfill of this range exists ({} done, {} failed of {} \
                     planned) — pass --resume to continue it or --restart to start over",
                    prior.done_count(),
                    prior.failed_count(),
                    units.len()
                )));
            }
            prior
        }
        None => BackfillState::new(descriptor.clone()),
    };

    let planned = units.len();
    let (todo, skipped) = split_remaining(units.clone(), &marker);
    for _ in 0..skipped {
        super::metrics::record_unit(&opts.pipeline_name, "skipped");
    }

    // ── Dry run: report the plan without executing ───────────────────────────
    if opts.dry_run {
        let reports = units
            .iter()
            .map(|u| UnitReport {
                unit: u.id.clone(),
                start: u.start.to_rfc3339(),
                end: u.end.to_rfc3339(),
                outcome: if marker.is_done(&u.id) {
                    "skipped".into()
                } else {
                    "pending".into()
                },
                error: None,
            })
            .collect();
        return Ok(BackfillOutcome {
            descriptor,
            planned,
            skipped,
            succeeded: 0,
            failed: 0,
            dry_run: true,
            units: reports,
        });
    }

    // ── Execute ──────────────────────────────────────────────────────────────
    let cancel = match &opts.cancel {
        Some(token) => token.clone(),
        None => {
            let token = CancellationToken::new();
            crate::replication::orchestrator::spawn_cancel_on_signal(token.clone());
            token
        }
    };
    // `--restart` means "start over": besides resetting the progress marker
    // (done above), delete every planned unit's scoped state key so the run
    // genuinely re-backfills from the start. Without this a bookmark-mode unit's
    // surviving `{name}::backfill::{unit}` bookmark is kept (the seed is guarded
    // `if is_none()`), silently resuming instead of restarting (audit #321 H3).
    // Done here (execute path only) so a `--restart --dry-run` never mutates
    // state. `units` still reflects the full plan (the marker was just reset).
    if opts.restart {
        clear_scoped_unit_state(&store, &opts.pipeline_name, &units).await?;
    }

    // Persist the (possibly reset) marker up front so an early crash leaves a
    // resumable record of the attempt.
    store.put(&marker_k, &marker.to_value()?).await?;

    let semaphore = Arc::new(tokio::sync::Semaphore::new(opts.concurrency.max(1)));
    let marker_lock = Arc::new(tokio::sync::Mutex::new(marker));
    let mut join = tokio::task::JoinSet::new();
    let opts = Arc::new(opts);
    let root = Arc::new(root);
    let total_todo = todo.len();
    let mut reports: Vec<UnitReport> = Vec::with_capacity(total_todo);

    for unit in todo {
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| CliError::Internal(format!("backfill semaphore closed: {e}")))?;
        if cancel.is_cancelled() {
            drop(permit);
            break;
        }
        let opts = opts.clone();
        let root = root.clone();
        let cfg_range = opts.range.clone();
        let store = store.clone();
        let cancel = cancel.clone();
        join.spawn(async move {
            let _permit = permit;
            let result = run_one_unit(&root, &unit, &cfg_range, &opts, &store, cancel).await;
            (unit, result)
        });
    }

    let mut succeeded = 0usize;
    let mut failed = 0usize;
    while let Some(joined) = join.join_next().await {
        let (unit, result) =
            joined.map_err(|e| CliError::Internal(format!("backfill unit task panicked: {e}")))?;
        let (outcome, error) = match result {
            Ok(()) => {
                succeeded += 1;
                super::metrics::record_unit(&opts.pipeline_name, "ok");
                ("done".to_string(), None)
            }
            Err(e) => {
                failed += 1;
                super::metrics::record_unit(&opts.pipeline_name, "err");
                ("failed".to_string(), Some(e.to_string()))
            }
        };
        // Durable per-unit progress: read-modify-write under the lock so a
        // crash between units never loses a completed unit's outcome.
        {
            let mut m = marker_lock.lock().await;
            match &error {
                None => m.mark_done(&unit.id),
                Some(e) => m.mark_failed(&unit.id, e.clone()),
            }
            store.put(&marker_k, &m.to_value()?).await?;
            super::metrics::set_progress(&opts.pipeline_name, m.done_count(), planned);
            tracing::info!(
                unit = %unit.id,
                outcome = %outcome,
                done = m.done_count(),
                failed = m.failed_count(),
                planned,
                "backfill unit finished"
            );
        }
        reports.push(UnitReport {
            unit: unit.id.clone(),
            start: unit.start.to_rfc3339(),
            end: unit.end.to_rfc3339(),
            outcome,
            error,
        });
    }

    reports.sort_by(|a, b| a.unit.cmp(&b.unit));
    Ok(BackfillOutcome {
        descriptor,
        planned,
        skipped,
        succeeded,
        failed,
        dry_run: false,
        units: reports,
    })
}

/// Delete every planned unit's scoped state key (`{name}::backfill::{unit}`).
/// Called on `--restart` so a re-backfill starts from scratch rather than
/// silently resuming a surviving bookmark (audit #321 H3).
async fn clear_scoped_unit_state(
    store: &Arc<dyn StateStore>,
    pipeline_name: &str,
    units: &[BackfillUnit],
) -> CliResult<()> {
    for unit in units {
        store
            .delete(&unit_state_key(pipeline_name, &unit.id))
            .await?;
    }
    Ok(())
}

/// Run one unit end-to-end through the executor.
async fn run_one_unit(
    root: &ExpandedNode,
    unit: &BackfillUnit,
    range: &BackfillRange,
    opts: &BackfillOptions,
    store: &Arc<dyn StateStore>,
    cancel: CancellationToken,
) -> CliResult<()> {
    let time_mode = matches!(range, BackfillRange::Time { .. });
    let mut node = build_unit_node(root, unit, time_mode)?;

    if let BackfillRange::Bookmark { from, to, field } = range {
        // Seed the scoped bookmark once — a resumed unit keeps its own
        // further-along position.
        let key = unit_state_key(&opts.pipeline_name, &unit.id);
        if store.get(&key).await?.is_none() {
            store.put(&key, from).await?;
        }
        // Upper bound: wrap the pre-built source so records past the bound
        // are dropped before transforms/sink.
        if let (Some(to), Some(field)) = (to, field) {
            let mut source_cfg = node.source.config.clone();
            crate::executor::resolve_now_inplace(&mut source_cfg, unit.start)?;
            let inner = crate::registry::build_source(
                &node.source.kind,
                source_cfg,
                &opts.auth,
                opts.resilience.as_ref().map(|r| &r.retry),
            )
            .await?;
            node.source_override = Some(crate::dlq_replay::reader::SourceOverride::new(Box::new(
                BoundedSource {
                    inner,
                    field: field.clone(),
                    to: to.clone(),
                },
            )));
        }
    }

    let summary = run_expanded(vec![node], make_opts(opts, unit.start, cancel.clone())).await?;
    if summary.had_failures() {
        let detail = summary
            .invocations
            .iter()
            .find_map(|i| i.error.clone())
            .unwrap_or_else(|| "unknown error".to_string());
        return Err(CliError::Internal(format!(
            "unit {} failed: {detail}",
            unit.id
        )));
    }
    if cancel.is_cancelled() {
        // A flush-completing cancel mid-unit wrote a partial window — the
        // unit is NOT complete and must re-run on --resume.
        return Err(CliError::Internal(format!(
            "unit {} interrupted by shutdown before completion",
            unit.id
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parse_cfg(yaml: &str) -> PipelineConfig {
        crate::config::parse_with_extension(yaml, "yaml").unwrap()
    }

    const SCOPED: &str = r#"
version: 1
name: orders
pipeline:
  source:
    type: rest
    config: { url: "https://api.example.com/orders?since=${backfill.start}&until=${backfill.end}" }
  sink:
    type: jsonl
    config: { path: ./out.jsonl }
"#;

    fn time_range(
        from: &str,
        to: &str,
        window: Option<crate::backfill::plan::WindowStep>,
    ) -> BackfillRange {
        let tz: chrono_tz::Tz = "UTC".parse().unwrap();
        BackfillRange::Time {
            from: crate::backfill::plan::parse_boundary(from, tz).unwrap(),
            to: crate::backfill::plan::parse_boundary(to, tz).unwrap(),
            window,
            tz,
        }
    }

    fn base_opts(range: BackfillRange) -> BackfillOptions {
        BackfillOptions {
            pipeline_name: "orders".into(),
            execution: None,
            auth: crate::auth_catalog::AuthCatalog::default(),
            resilience: None,
            range,
            concurrency: 2,
            row: None,
            into_sink: None,
            dry_run: true,
            resume: false,
            restart: false,
            cancel: None,
        }
    }

    #[tokio::test]
    async fn dry_run_plans_31_units_without_running() {
        let cfg = parse_cfg(SCOPED);
        let opts = base_opts(time_range(
            "2026-06-01",
            "2026-07-02",
            Some(crate::backfill::plan::WindowStep::Days(1)),
        ));
        let out = run_backfill(&cfg, opts).await.unwrap();
        assert!(out.dry_run);
        assert_eq!(out.planned, 31);
        assert_eq!(out.units.len(), 31);
        assert!(out.units.iter().all(|u| u.outcome == "pending"));
        assert_eq!(out.succeeded + out.failed, 0);
    }

    #[tokio::test]
    async fn unscoped_source_rejected_with_actionable_error() {
        let cfg = parse_cfg(
            r#"
version: 1
name: orders
pipeline:
  source: { type: rest, config: { url: "https://api.example.com/orders" } }
  sink:   { type: jsonl, config: { path: ./out.jsonl } }
"#,
        );
        let opts = base_opts(time_range("2026-06-01", "2026-06-02", None));
        let err = run_backfill(&cfg, opts).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("${backfill.start}"), "actionable: {msg}");
        assert!(
            msg.contains("--from-bookmark"),
            "suggests alternative: {msg}"
        );
    }

    #[tokio::test]
    async fn bookmark_mode_requires_state_block() {
        let cfg = parse_cfg(SCOPED);
        let opts = base_opts(BackfillRange::Bookmark {
            from: json!("2026-01-01"),
            to: None,
            field: None,
        });
        let err = run_backfill(&cfg, opts).await.unwrap_err();
        assert!(err.to_string().contains("state"), "{err}");
    }

    #[tokio::test]
    async fn to_bookmark_requires_field() {
        let cfg = parse_cfg(&format!(
            "{SCOPED}  state: {{ type: memory, config: {{}} }}\n"
        ));
        let opts = base_opts(BackfillRange::Bookmark {
            from: json!(1),
            to: Some(json!(9)),
            field: None,
        });
        let err = run_backfill(&cfg, opts).await.unwrap_err();
        assert!(err.to_string().contains("--bookmark-field"), "{err}");
    }

    #[tokio::test]
    async fn into_unknown_sink_lists_available() {
        let cfg = parse_cfg(
            r#"
version: 1
name: orders
pipeline:
  sources:
    default:
      type: rest
      config: { url: "https://api.example.com/x?s=${backfill.start}" }
  sinks:
    default: { type: jsonl, config: { path: ./out.jsonl } }
    staging: { type: jsonl, config: { path: ./staging.jsonl } }
"#,
        );
        let mut opts = base_opts(time_range("2026-06-01", "2026-06-02", None));
        opts.into_sink = Some("nope".into());
        let err = run_backfill(&cfg, opts).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("staging"), "lists templates: {msg}");
    }

    #[tokio::test]
    async fn multiple_roots_require_row_selection() {
        let cfg = parse_cfg(
            r#"
version: 1
name: orders
pipeline:
  source:
    type: rest
    config: { url: "https://api.example.com/x?s=${backfill.start}" }
  sink: { type: jsonl, config: { path: ./out.jsonl } }
matrix:
  - id: a
  - id: b
"#,
        );
        let opts = base_opts(time_range("2026-06-01", "2026-06-02", None));
        let err = run_backfill(&cfg, opts).await.unwrap_err();
        assert!(err.to_string().contains("--row"), "{err}");

        let mut opts = base_opts(time_range("2026-06-01", "2026-06-02", None));
        opts.row = Some("b".into());
        let out = run_backfill(&cfg, opts).await.unwrap();
        assert_eq!(out.planned, 1);

        let mut opts = base_opts(time_range("2026-06-01", "2026-06-02", None));
        opts.row = Some("zzz".into());
        let err = run_backfill(&cfg, opts).await.unwrap_err();
        assert!(err.to_string().contains("available: a, b"), "{err}");
    }

    #[test]
    fn unit_node_is_namespaced_and_at_least_once() {
        let cfg = parse_cfg(SCOPED);
        let root = select_root(expand(&cfg).unwrap(), None).unwrap();
        let tz: chrono_tz::Tz = "UTC".parse().unwrap();
        let unit = BackfillUnit {
            id: "20260601T000000Z".into(),
            start: crate::backfill::plan::parse_boundary("2026-06-01", tz).unwrap(),
            end: crate::backfill::plan::parse_boundary("2026-06-02", tz).unwrap(),
        };
        let node = build_unit_node(&root, &unit, true).unwrap();
        assert_eq!(node.id, "backfill::20260601T000000Z");
        assert_eq!(node.delivery, faucet_core::DeliveryMode::AtLeastOnce);
        let url = node.source.config["url"].as_str().unwrap();
        assert!(url.contains("since=2026-06-01T00:00:00+00:00"), "{url}");
        assert!(url.contains("until=2026-06-02T00:00:00+00:00"), "{url}");
    }

    #[tokio::test]
    async fn restart_clears_scoped_unit_state() {
        // #321 H3: --restart must delete each planned unit's scoped bookmark so
        // the run genuinely starts over. A surviving bookmark would otherwise
        // make run_one_unit skip its re-seed and resume mid-range.
        let store: Arc<dyn StateStore> = Arc::new(faucet_core::MemoryStateStore::new());
        let key = unit_state_key("orders", "bookmark");
        store.put(&key, &json!(500)).await.unwrap();

        let tz: chrono_tz::Tz = "UTC".parse().unwrap();
        let unit = BackfillUnit {
            id: "bookmark".into(),
            start: crate::backfill::plan::parse_boundary("2026-06-01", tz).unwrap(),
            end: crate::backfill::plan::parse_boundary("2026-06-02", tz).unwrap(),
        };
        clear_scoped_unit_state(&store, "orders", std::slice::from_ref(&unit))
            .await
            .unwrap();
        assert_eq!(
            store.get(&key).await.unwrap(),
            None,
            "restart must delete the surviving scoped bookmark"
        );
    }

    #[test]
    fn descriptor_distinguishes_ranges_and_rows() {
        let r1 = time_range("2026-06-01", "2026-07-01", None).descriptor("a");
        let r2 = time_range("2026-06-01", "2026-07-01", None).descriptor("b");
        let r3 = time_range("2026-06-01", "2026-07-02", None).descriptor("a");
        assert_ne!(r1, r2);
        assert_ne!(r1, r3);
        let b = BackfillRange::Bookmark {
            from: json!(5),
            to: Some(json!(9)),
            field: Some("id".into()),
        }
        .descriptor("a");
        assert!(b.starts_with("bookmark|"), "{b}");
    }

    // ── BoundedSource ────────────────────────────────────────────────────────

    struct FixtureSource(Vec<Value>);

    #[faucet_core::async_trait]
    impl faucet_core::Source for FixtureSource {
        async fn fetch_with_context(
            &self,
            _c: &std::collections::HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(self.0.clone())
        }
    }

    #[tokio::test]
    async fn bounded_source_drops_records_past_the_bound() {
        use faucet_core::Source as _;
        use futures::StreamExt;
        let inner = FixtureSource(vec![
            json!({"id": 1, "ts": "2026-06-01"}),
            json!({"id": 2, "ts": "2026-06-15"}),
            json!({"id": 3, "ts": "2026-07-05"}),
            json!({"id": 4}), // missing field → kept
        ]);
        let bounded = BoundedSource {
            inner: Box::new(inner),
            field: "ts".into(),
            to: json!("2026-06-30"),
        };
        let ctx = std::collections::HashMap::new();
        let records = bounded.fetch_with_context(&ctx).await.unwrap();
        let ids: Vec<i64> = records.iter().map(|r| r["id"].as_i64().unwrap()).collect();
        assert_eq!(ids, vec![1, 2, 4], "record past the bound dropped");

        let mut pages = bounded.stream_pages(&ctx, 10);
        let page = pages.next().await.unwrap().unwrap();
        assert_eq!(page.records.len(), 3);
    }
}
