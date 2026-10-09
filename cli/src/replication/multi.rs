//! Multi-table mirror orchestration (#731): discover the table set, snapshot
//! each table from a position captured just before it, and stream every
//! snapshotted table from one shared change stream — picking up created
//! tables, retiring dropped ones, and pausing a table whose pipeline keeps
//! failing instead of stalling the rest.

use crate::config::{ConnectorSpec, PipelineConfig};
use crate::error::{CliError, CliResult};
use crate::executor::{RunSummary, build_state_key, run_expanded};
use crate::expand::{ExpandedNode, expand};
use crate::registry::build_source;
use crate::replication::compiled::{CompiledReplication, CompiledTables};
use crate::replication::feed::{self, LiveStats};
use crate::replication::multi_state::{
    self, FailureAction, MirrorState, TablePhase, due_for_snapshot, reconcile_discovery,
};
use crate::replication::orchestrator::{
    ReplicationOptions, build_snapshot_node, make_opts, spawn_cancel_on_signal,
};
use crate::replication::spec::{NewTables, OnTableError};
use crate::replication::state::marker_key;
use crate::replication::tables::{self, Resolution, Router, TablePlan};
use chrono::Utc;
use faucet_core::{DeliveryMode, Source, StateStore};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

/// How often live progress is written to the marker during a stream cycle.
const PROGRESS_INTERVAL: Duration = Duration::from_secs(10);
/// Delay before a failed snapshot is retried.
const SNAPSHOT_RETRY: Duration = Duration::from_secs(30);
/// How often a running stream cycle checks for due snapshots and discovery.
const HOUSEKEEPING: Duration = Duration::from_secs(5);

struct Shared {
    cfg: PipelineConfig,
    tables: CompiledTables,
    snapshot: ConnectorSpec,
    cdc: ConnectorSpec,
    sink_kind: String,
    continuous: bool,
    opts: ReplicationOptions,
    store: Arc<dyn StateStore>,
    marker_key: String,
    state: tokio::sync::Mutex<MirrorState>,
    plans: std::sync::Mutex<BTreeMap<String, TablePlan>>,
    live: LiveStats,
}

impl Shared {
    fn plan(&self, table: &str) -> CliResult<TablePlan> {
        self.plans
            .lock()
            .ok()
            .and_then(|p| p.get(table).cloned())
            .ok_or_else(|| CliError::Internal(format!("mirror: no plan for table '{table}'")))
    }

    fn table_state_key(&self, plan: &TablePlan) -> String {
        build_state_key(&self.opts.pipeline_name, &plan.id, None)
    }

    /// Fold live counters into the marker and write it.
    async fn persist(&self) -> CliResult<()> {
        let mut state = self.state.lock().await;
        let deltas = self
            .live
            .lock()
            .map(|mut g| std::mem::take(&mut *g))
            .unwrap_or_default();
        for (table, delta) in deltas {
            if let Some(t) = state.tables.get_mut(&table) {
                t.changes += delta.changes;
                if delta.last_applied_at > t.last_applied_at {
                    t.last_applied_at = delta.last_applied_at;
                }
            }
        }
        state.updated_at = Utc::now();
        let value = state.to_value()?;
        drop(state);
        self.store.put(&self.marker_key, &value).await?;
        Ok(())
    }

    /// The per-table node: the CDC pipeline expanded against this table's sink
    /// config, so every generic gate (write mode, exactly-once, drift) runs per
    /// table.
    fn table_node(&self, plan: &TablePlan) -> CliResult<ExpandedNode> {
        let mut cfg = self.cfg.clone();
        cfg.replication = None;
        if let Some(sink) = cfg.pipeline.sink.as_mut() {
            sink.config = plan.sink_config.clone();
        }
        if plan.schema_drift.is_some() {
            cfg.pipeline.schema = plan.schema_drift.clone();
        }
        let mut node = expand(&cfg)?
            .into_iter()
            .next()
            .ok_or_else(|| CliError::Internal("mirror: expand produced no node".into()))?;
        node.id = plan.id.clone();
        Ok(node)
    }

    fn snapshot_node(&self, plan: &TablePlan, cdc_node: &ExpandedNode) -> ExpandedNode {
        let mut spec = self.snapshot.clone();
        spec.config = plan.snapshot_config.clone();
        let mut node = build_snapshot_node(cdc_node, spec);
        node.id = format!("{}::snapshot", plan.id);
        node
    }

    /// Whether a snapshot replaces the destination atomically (so a redo or a
    /// re-sync after a pause leaves no row the source no longer has).
    fn snapshot_replaces(&self) -> bool {
        crate::registry::sink_supports_overwrite(&self.sink_kind)
    }

    /// Whether the table's destination already exists (asked of the sinks
    /// whose keyed writes need the destination's own key constraint).
    async fn destination_exists(&self, plan: &TablePlan) -> CliResult<bool> {
        let sink =
            crate::registry::build_sink(&self.sink_kind, plan.sink_config.clone(), &self.opts.auth)
                .await?;
        Ok(sink.current_schema().await?.is_some())
    }

    /// Tables currently sharing the change stream with `table` (for scoping
    /// the capture connection).
    async fn stream_mates(&self, table: &str) -> Vec<String> {
        if self.tables.cdc_kind == "dynamodb" {
            return vec![table.to_string()];
        }
        let state = self.state.lock().await;
        let mut out: BTreeSet<String> = state
            .tables
            .iter()
            .filter(|(_, t)| {
                matches!(
                    t.phase,
                    TablePhase::Active | TablePhase::Snapshotting | TablePhase::Pending
                )
            })
            .map(|(n, _)| n.clone())
            .collect();
        out.insert(table.to_string());
        out.into_iter().collect()
    }

    async fn build_cdc(&self, group: &[String]) -> CliResult<Box<dyn Source>> {
        build_source(
            &self.cdc.kind,
            tables::cdc_config_for(&self.cdc.kind, &self.cdc.config, group),
            &self.opts.auth,
            None,
        )
        .await
    }

    fn router(&self, active: &[String], known: BTreeSet<String>) -> Router {
        Router {
            active: active.iter().cloned().collect(),
            known,
            qualifier: tables::db_qualifier(&self.snapshot.kind, &self.snapshot.config),
            follow: (self.tables.spec.new_tables == NewTables::Follow)
                .then(|| self.tables.spec.clone()),
        }
    }
}

/// Resolve the table set: discover, filter, plan, and validate each table's
/// pipeline. Refusals carry the reason shown in status.
async fn discover(shared: &Shared) -> CliResult<BTreeMap<String, Resolution>> {
    let source = build_source(
        &shared.snapshot.kind,
        shared.snapshot.config.clone(),
        &shared.opts.auth,
        None,
    )
    .await?;
    let descriptors = source.discover().await?;
    let upsert_capable = crate::registry::sink_supported_write_modes(&shared.sink_kind)
        .contains(&faucet_core::WriteMode::Upsert);
    let sink_template = shared
        .cfg
        .pipeline
        .sink
        .as_ref()
        .map(|s| s.config.clone())
        .unwrap_or(Value::Null);
    let mut out: BTreeMap<String, Resolution> = BTreeMap::new();
    let mut plans = Vec::new();
    for d in descriptors
        .iter()
        .filter(|d| tables::selected(&shared.tables.spec, &d.name))
    {
        match tables::resolve(
            &shared.tables.spec,
            &shared.sink_kind,
            &sink_template,
            &shared.snapshot.config,
            upsert_capable,
            shared.tables.per_table.get(&d.name),
            d,
        ) {
            Resolution::Mirror(plan) => plans.push(*plan),
            refused @ Resolution::Refused(_) => {
                out.insert(d.name.clone(), refused);
            }
        }
    }
    for name in shared.tables.per_table.keys() {
        if !descriptors.iter().any(|d| &d.name == name) {
            tracing::warn!(table = %name, "mirror.per_table names a table discovery did not report");
        }
    }
    let incumbents = shared.state.lock().await.incumbents();
    let (kept, collisions) = tables::refuse_collisions(plans, &incumbents);
    for (name, reason) in collisions {
        out.insert(name, Resolution::Refused(reason));
    }
    let mut accepted = BTreeMap::new();
    for plan in kept {
        let verdict = if shared.tables.cdc_kind == "dynamodb" && plan.key.is_empty() {
            Err("DynamoDB Streams replays a retained window; only a keyed upsert converges".into())
        } else {
            shared
                .table_node(&plan)
                .map(|_| ())
                .map_err(|e| e.to_string())
        };
        match verdict {
            Ok(()) => {
                accepted.insert(plan.name.clone(), plan.clone());
                out.insert(plan.name.clone(), Resolution::Mirror(Box::new(plan)));
            }
            Err(reason) => {
                out.insert(plan.name.clone(), Resolution::Refused(reason));
            }
        }
    }
    if let Ok(mut p) = shared.plans.lock() {
        p.extend(accepted);
    }
    Ok(out)
}

/// Seed `plan`'s state key at `position`, keeping the exactly-once sequence so
/// the sink's watermark stays monotonic across a re-snapshot.
async fn seed_position(
    shared: &Shared,
    plan: &TablePlan,
    source: &dyn Source,
    position: &Value,
) -> CliResult<()> {
    let key = shared.table_state_key(plan);
    let data = if shared.cfg.delivery == DeliveryMode::ExactlyOnce {
        let seq = match shared.store.get(&key).await? {
            Some(stored) => faucet_core::unwrap_state(&stored).1,
            None => 0,
        };
        faucet_core::wrap_state(Some(position), seq)
    } else {
        position.clone()
    };
    let stored = faucet_core::state_version::wrap_versioned(
        source.connector_name(),
        source.state_schema(),
        &data,
    );
    shared.store.put(&key, &stored).await?;
    Ok(())
}

fn summary_rows(summary: &RunSummary) -> u64 {
    summary
        .invocations
        .iter()
        .map(|i| i.records_written as u64)
        .sum()
}

fn summary_error(summary: &RunSummary) -> Option<String> {
    summary.had_failures().then(|| {
        summary
            .invocations
            .iter()
            .find_map(|i| i.error.clone())
            .unwrap_or_else(|| "unknown error".to_string())
    })
}

/// Snapshot one table. `Ok(true)` = done and active, `Ok(false)` = interrupted
/// by shutdown (redone on the next run).
/// Capture the stream position `table` will join from and record it. Runs in
/// the driver while no stream cycle is open, so the position is part of the
/// next cycle's resume floor before any cycle can move the stream past it.
async fn prepare_snapshot(shared: &Shared, table: &str) -> CliResult<Vec<ShardRun>> {
    let plan = shared.plan(table)?;
    let mates = shared.stream_mates(table).await;
    let cdc = shared.build_cdc(&mates).await?;
    let position = cdc.capture_resume_position().await?.ok_or_else(|| {
        CliError::Config(format!(
            "mirror: source '{}' does not support position capture",
            shared.cdc.kind
        ))
    })?;
    seed_position(shared, &plan, cdc.as_ref(), &position).await?;
    drop(cdc);
    let cdc_node = shared.table_node(&plan)?;
    let snap = shared.snapshot_node(&plan, &cdc_node);
    let shards = plan_shards(shared, &plan, &snap).await?;
    {
        let mut state = shared.state.lock().await;
        multi_state::mark_snapshot_started(
            &mut state,
            table,
            position,
            shards.len() as u64,
            Utc::now(),
        );
        if multi_state::flag_stale_resnapshot(
            &mut state,
            table,
            shared.snapshot_replaces(),
            &shared.sink_kind,
        ) {
            tracing::warn!(
                pipeline = %shared.opts.pipeline_name,
                table = %table,
                sink = %shared.sink_kind,
                "mirror: re-snapshotting into a destination this sink cannot replace — rows \
                 deleted at the source meanwhile may remain; see `faucet mirror status`"
            );
        }
    }
    shared.persist().await?;
    Ok(shards)
}

/// Snapshot one prepared table. `Ok(true)` = done and active, `Ok(false)` =
/// interrupted by shutdown (redone on the next run).
async fn snapshot_table(
    shared: Arc<Shared>,
    table: String,
    shards: Vec<ShardRun>,
    cancel: CancellationToken,
) -> CliResult<bool> {
    let plan = shared.plan(&table)?;
    let cdc_node = shared.table_node(&plan)?;
    let snap = shared.snapshot_node(&plan, &cdc_node);
    tracing::info!(pipeline = %shared.opts.pipeline_name, table = %table, shards = shards.len(), "mirror: snapshotting table");

    let missing = !plan.key.is_empty()
        && KEY_CONSTRAINT_SINKS.contains(&shared.sink_kind.as_str())
        && !shared.destination_exists(&plan).await?;
    let strategy = snapshot_strategy(shared.snapshot_replaces(), missing, shards.len());
    if strategy.clear_first {
        let summary = run_expanded(
            vec![snapshot_as_overwrite(&snap, true)],
            make_opts(&shared.opts, Some(cancel.clone())),
        )
        .await?;
        if let Some(held) = summary.lease_refusal() {
            return Err(held);
        }
        if let Some(e) = summary_error(&summary) {
            return Err(CliError::Internal(format!(
                "clearing '{table}' before its sharded snapshot failed: {e}"
            )));
        }
    }
    let mut runs = JoinSet::new();
    let mut first_alone = strategy.first_shard_alone;
    for shard in shards {
        let mut node = if strategy.overwrite {
            snapshot_as_overwrite(&snap, false)
        } else {
            snap.clone()
        };
        let mut opts = make_opts(&shared.opts, Some(cancel.clone()));
        if let Some(spec) = shard {
            node.source.config = spec.0;
            opts.shard = Some(spec.1);
        }
        runs.spawn(async move { run_expanded(vec![node], opts).await });
        if first_alone {
            first_alone = false;
            if let Some(joined) = runs.join_next().await {
                let summary =
                    joined.map_err(|e| CliError::Internal(format!("snapshot task: {e}")))??;
                if let Some(held) = summary.lease_refusal() {
                    return Err(held);
                }
                if let Some(e) = summary_error(&summary) {
                    return Err(CliError::Internal(format!(
                        "snapshot of '{table}' failed: {e}"
                    )));
                }
                let mut state = shared.state.lock().await;
                multi_state::mark_shard_done(
                    &mut state,
                    &table,
                    summary_rows(&summary),
                    Utc::now(),
                );
            }
        }
    }
    let mut failure: Option<String> = None;
    while let Some(joined) = runs.join_next().await {
        let outcome = joined.map_err(|e| CliError::Internal(format!("snapshot task: {e}")))?;
        if let Some(held) = outcome.as_ref().ok().and_then(RunSummary::lease_refusal) {
            return Err(held);
        }
        match outcome {
            Ok(summary) => match summary_error(&summary) {
                Some(e) => failure = failure.or(Some(e)),
                None => {
                    let mut state = shared.state.lock().await;
                    multi_state::mark_shard_done(
                        &mut state,
                        &table,
                        summary_rows(&summary),
                        Utc::now(),
                    );
                }
            },
            Err(e) => failure = failure.or(Some(e.to_string())),
        }
    }
    if let Some(e) = failure {
        return Err(CliError::Internal(format!(
            "snapshot of '{table}' failed: {e}"
        )));
    }
    if cancel.is_cancelled() {
        return Ok(false);
    }
    {
        let mut state = shared.state.lock().await;
        multi_state::mark_snapshot_done(&mut state, &table, Utc::now());
    }
    shared.persist().await?;
    tracing::info!(pipeline = %shared.opts.pipeline_name, table = %table, "mirror: snapshot complete; table joins the stream");
    Ok(true)
}

/// The snapshot node rewritten to replace the destination (`write_mode:
/// overwrite`). With `empty`, it reads nothing — an atomic truncate ahead of a
/// sharded snapshot whose ranges then write in the table's own mode.
fn snapshot_as_overwrite(snap: &ExpandedNode, empty: bool) -> ExpandedNode {
    let mut node = snap.clone();
    if let Some(obj) = node.sink.config.as_object_mut() {
        obj.insert("write_mode".into(), json!("overwrite"));
        obj.remove("delete_marker");
        obj.remove("key");
    }
    node.schema = None;
    if empty {
        node.id = format!("{}::clear", node.id);
        let (feed, source) = feed::channel(&node.id, Arc::new(feed::EmptySource));
        drop(feed);
        node.source_override = Some(crate::dlq_replay::reader::SourceOverride::new(Box::new(
            source,
        )));
    }
    node
}

/// Sinks whose keyed upsert relies on a key constraint in the destination
/// table — a snapshot must create their table keyed, never by an overwrite's
/// keyless first-run swap.
const KEY_CONSTRAINT_SINKS: &[&str] = &["postgres", "mysql", "sqlite", "mssql"];

/// How a table's snapshot writes its destination.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct SnapshotStrategy {
    /// Atomically empty the destination first (sharded re-snapshot).
    clear_first: bool,
    /// Write the snapshot as an atomic overwrite.
    overwrite: bool,
    /// Run the first range alone (it creates the destination) before the rest.
    first_shard_alone: bool,
}

/// Pick the snapshot's write strategy. A destination that can be replaced
/// atomically is — so a redo or a re-sync leaves no row the source no longer
/// has — except a keyed destination that does not exist yet, which the
/// table's own keyed mode must create (with its key constraint).
fn snapshot_strategy(replaces: bool, missing_keyed: bool, shards: usize) -> SnapshotStrategy {
    if !replaces || missing_keyed {
        return SnapshotStrategy {
            first_shard_alone: missing_keyed && shards > 1,
            ..Default::default()
        };
    }
    if shards <= 1 {
        SnapshotStrategy {
            overwrite: true,
            ..Default::default()
        }
    } else {
        SnapshotStrategy {
            clear_first: true,
            ..Default::default()
        }
    }
}

type ShardRun = Option<(Value, faucet_core::ShardSpec)>;

/// Split a table's snapshot into primary-key ranges when `snapshot.shards`
/// asks for it and the key is a single column the source can shard on.
async fn plan_shards(
    shared: &Shared,
    plan: &TablePlan,
    snap: &ExpandedNode,
) -> CliResult<Vec<ShardRun>> {
    if shared.tables.shards <= 1 || plan.key.len() != 1 {
        return Ok(vec![None]);
    }
    let mut config = snap.source.config.clone();
    if let Some(obj) = config.as_object_mut() {
        obj.insert("shard".into(), json!({ "key": plan.key[0] }));
    }
    let source = build_source(&snap.source.kind, config.clone(), &shared.opts.auth, None).await?;
    if !source.is_shardable() {
        return Ok(vec![None]);
    }
    let specs = source.enumerate_shards(shared.tables.shards).await?;
    Ok(specs
        .into_iter()
        .map(|s| Some((config.clone(), s)))
        .collect())
}

/// What one stream cycle did.
#[derive(Default)]
struct CycleResult {
    tables: BTreeMap<String, Result<(), String>>,
    new_tables: BTreeSet<String>,
    source_error: Option<String>,
    /// A table's run was refused by another run's live lease: the mirror
    /// fails as a whole rather than recording a table failure.
    lease_held: Option<String>,
}

/// Stream every active table for one cycle (one shared stream per group).
async fn run_cycle(
    shared: &Arc<Shared>,
    active: Vec<String>,
    cancel: CancellationToken,
) -> CycleResult {
    let (known, floors) = {
        let state = shared.state.lock().await;
        let floors: Vec<Value> = if shared.tables.cdc_kind == "dynamodb" {
            Vec::new()
        } else {
            state
                .tables
                .values()
                .filter(|t| t.phase == TablePhase::Snapshotting)
                .filter_map(|t| t.position.clone())
                .collect()
        };
        let known: BTreeSet<String> = state.tables.keys().cloned().collect();
        (known, floors)
    };
    let groups = tables::stream_groups(&shared.tables.cdc_kind, &active);
    let mut runs = JoinSet::new();
    for group in groups {
        let shared = shared.clone();
        let known = known.clone();
        let cancel = cancel.clone();
        let floors = floors.clone();
        runs.spawn(async move { run_group(shared, group, known, floors, cancel).await });
    }
    let mut out = CycleResult::default();
    while let Some(joined) = runs.join_next().await {
        match joined {
            Ok(group) => {
                out.tables.extend(group.tables);
                out.new_tables.extend(group.new_tables);
                out.lease_held = out.lease_held.or(group.lease_held);
                if group.source_error.is_some() {
                    out.source_error = group.source_error;
                }
            }
            Err(e) => out.source_error = Some(format!("stream task: {e}")),
        }
    }
    out
}

async fn run_group(
    shared: Arc<Shared>,
    group: Vec<String>,
    known: BTreeSet<String>,
    floors: Vec<Value>,
    cancel: CancellationToken,
) -> CycleResult {
    let mut out = CycleResult::default();
    let source: Arc<dyn Source> = match shared.build_cdc(&group).await {
        Ok(s) => Arc::from(s),
        Err(e) => {
            out.source_error = Some(e.to_string());
            return out;
        }
    };
    let mut feeds = Vec::new();
    let mut runs: JoinSet<(String, Result<(), String>, Option<String>)> = JoinSet::new();
    let mut streamed = Vec::new();
    for table in &group {
        let node = shared.plan(table).and_then(|plan| shared.table_node(&plan));
        let mut node = match node {
            Ok(n) => n,
            Err(e) => {
                out.tables.insert(table.clone(), Err(e.to_string()));
                continue;
            }
        };
        let (feed, channel) = feed::channel(table, source.clone());
        node.source_override = Some(crate::dlq_replay::reader::SourceOverride::new(Box::new(
            channel,
        )));
        feeds.push(feed);
        streamed.push(table.clone());
        let opts = make_opts(&shared.opts, None);
        let name = table.clone();
        runs.spawn(async move {
            let (result, held) = match run_expanded(vec![node], opts).await {
                Ok(summary) => (
                    summary_error(&summary).map_or(Ok(()), Err),
                    summary.lease_refusal().map(|e| e.to_string()),
                ),
                Err(e @ CliError::LeaseHeld(_)) => (Err(e.to_string()), Some(e.to_string())),
                Err(e) => (Err(e.to_string()), None),
            };
            (name, result, held)
        });
    }
    let router = shared.router(&streamed, known);
    let demux = feed::run_demux(source, feeds, router, floors, cancel, shared.live.clone()).await;
    while let Some(joined) = runs.join_next().await {
        match joined {
            Ok((table, result, held)) => {
                out.tables.insert(table, result);
                out.lease_held = out.lease_held.or(held);
            }
            Err(e) => out.source_error = Some(format!("table task: {e}")),
        }
    }
    match demux {
        Ok(d) => {
            out.new_tables = d.new_tables;
            for table in d.dead {
                out.tables
                    .entry(table)
                    .or_insert_with(|| Err("the table's pipeline stopped".into()));
            }
        }
        Err(e) => out.source_error = Some(e.to_string()),
    }
    out
}

/// Run a multi-table mirror until SIGTERM (continuous) or until every table is
/// snapshotted and the stream has drained once (one-shot).
pub async fn run_multi(
    cfg: &PipelineConfig,
    compiled: &CompiledReplication,
    opts: ReplicationOptions,
) -> CliResult<()> {
    let tables_cfg = compiled
        .tables
        .clone()
        .ok_or_else(|| CliError::Internal("mirror: not a multi-table mirror".into()))?;
    let state_spec = cfg
        .pipeline
        .state
        .as_ref()
        .ok_or_else(|| CliError::Config("mirror requires a state store".into()))?;
    let store = crate::state::build_state_store(state_spec).await?;
    let marker_key = marker_key(&opts.pipeline_name);
    let state = match store.get(&marker_key).await? {
        Some(v) => MirrorState::from_value(v)?,
        None => MirrorState::new(Utc::now()),
    };
    let cdc = cfg
        .pipeline
        .source
        .clone()
        .ok_or_else(|| CliError::Config("mirror requires pipeline.source".into()))?;
    let sink_kind = cfg
        .pipeline
        .sink
        .as_ref()
        .map(|s| s.kind.clone())
        .ok_or_else(|| CliError::Config("mirror requires pipeline.sink".into()))?;
    let shared = Arc::new(Shared {
        cfg: cfg.clone(),
        tables: tables_cfg,
        snapshot: compiled.snapshot_source.clone(),
        cdc,
        sink_kind,
        continuous: compiled.continuous,
        opts,
        store,
        marker_key,
        state: tokio::sync::Mutex::new(state),
        plans: Default::default(),
        live: Default::default(),
    });
    let cancel = CancellationToken::new();
    spawn_cancel_on_signal(cancel.clone());
    drive(shared, cancel).await
}

struct Driver {
    shared: Arc<Shared>,
    cancel: CancellationToken,
    joined: Arc<Notify>,
    snapshots: JoinSet<(String, CliResult<bool>)>,
    running: BTreeSet<String>,
    retry_at: HashMap<String, Instant>,
    warned_lag: BTreeSet<String>,
    /// Tables whose latest snapshot or stream cycle this run failed.
    failed: BTreeMap<String, String>,
}

impl Driver {
    /// Tables due for a snapshot that can start now (capacity, retry delay).
    fn startable(&self, due: &[String]) -> Vec<String> {
        let now = Instant::now();
        let room = self
            .shared
            .tables
            .concurrency
            .saturating_sub(self.running.len());
        due.iter()
            .filter(|t| {
                !self.running.contains(*t) && self.retry_at.get(*t).is_none_or(|at| *at <= now)
            })
            .take(room)
            .cloned()
            .collect()
    }

    /// Capture positions for, then start, every startable due table. Must run
    /// while no stream cycle is open (see [`prepare_snapshot`]).
    async fn spawn_due(&mut self, due: Vec<String>) -> CliResult<()> {
        for table in self.startable(&due) {
            let shards = match prepare_snapshot(&self.shared, &table).await {
                Ok(shards) => shards,
                Err(e) => {
                    self.on_snapshot(table, Err(e)).await?;
                    continue;
                }
            };
            self.running.insert(table.clone());
            let shared = self.shared.clone();
            let cancel = self.cancel.clone();
            self.snapshots.spawn(async move {
                let r = snapshot_table(shared, table.clone(), shards, cancel).await;
                (table, r)
            });
        }
        Ok(())
    }

    async fn on_snapshot(&mut self, table: String, result: CliResult<bool>) -> CliResult<()> {
        self.running.remove(&table);
        match result {
            Ok(true) => {
                self.retry_at.remove(&table);
                self.failed.remove(&table);
                self.joined.notify_one();
            }
            Ok(false) => {}
            Err(e @ CliError::LeaseHeld(_)) => return Err(e),
            Err(e) => {
                let msg = e.to_string();
                self.failed.insert(table.clone(), msg.clone());
                tracing::error!(pipeline = %self.shared.opts.pipeline_name, table = %table, error = %msg, "mirror: table snapshot failed");
                let action = {
                    let mut state = self.shared.state.lock().await;
                    multi_state::record_failure(
                        &mut state,
                        &table,
                        &msg,
                        self.shared.tables.spec.max_table_failures,
                        Utc::now(),
                    )
                };
                self.retry_at
                    .insert(table.clone(), Instant::now() + SNAPSHOT_RETRY);
                self.shared.persist().await?;
                self.check_fail(&table, action, &msg)?;
            }
        }
        Ok(())
    }

    fn check_fail(&self, table: &str, action: FailureAction, msg: &str) -> CliResult<()> {
        if action == FailureAction::Paused {
            if self.shared.tables.spec.on_table_error == OnTableError::Fail {
                return Err(CliError::Internal(format!(
                    "mirror: table '{table}' failed {} times: {msg}",
                    self.shared.tables.spec.max_table_failures
                )));
            }
            tracing::warn!(pipeline = %self.shared.opts.pipeline_name, table = %table, error = %msg, "mirror: table paused; the rest of the stream continues");
        }
        Ok(())
    }

    async fn reap_finished(&mut self) -> CliResult<()> {
        while let Some(joined) = self.snapshots.try_join_next() {
            let (table, result) =
                joined.map_err(|e| CliError::Internal(format!("snapshot task: {e}")))?;
            self.on_snapshot(table, result).await?;
        }
        Ok(())
    }

    async fn wait_snapshots(&mut self) -> CliResult<()> {
        while let Some(joined) = self.snapshots.join_next().await {
            let (table, result) =
                joined.map_err(|e| CliError::Internal(format!("snapshot task: {e}")))?;
            self.on_snapshot(table, result).await?;
        }
        Ok(())
    }

    async fn rediscover(&mut self) -> CliResult<()> {
        match discover(&self.shared).await {
            Ok(resolved) => {
                let follow = self.shared.tables.spec.new_tables == NewTables::Follow;
                let diff = {
                    let mut state = self.shared.state.lock().await;
                    reconcile_discovery(&mut state, &resolved, follow, Utc::now())
                };
                for t in &diff.added {
                    tracing::info!(pipeline = %self.shared.opts.pipeline_name, table = %t, "mirror: table added");
                }
                for t in &diff.dropped {
                    tracing::warn!(pipeline = %self.shared.opts.pipeline_name, table = %t, "mirror: table dropped at the source; no longer routed (destination left untouched)");
                }
                for t in &diff.refused {
                    let reason = match resolved.get(t) {
                        Some(Resolution::Refused(r)) => r.clone(),
                        _ => String::new(),
                    };
                    tracing::error!(pipeline = %self.shared.opts.pipeline_name, table = %t, reason = %reason, "mirror: table refused");
                }
                self.shared.persist().await
            }
            Err(e) if self.shared.state.lock().await.discovered_at.is_none() => Err(e),
            Err(e) => {
                tracing::warn!(pipeline = %self.shared.opts.pipeline_name, error = %e, "mirror: discovery failed; keeping the current table set");
                Ok(())
            }
        }
    }

    async fn apply_cycle(&mut self, result: &CycleResult) -> CliResult<usize> {
        if let Some(held) = &result.lease_held {
            return Err(CliError::LeaseHeld(held.clone()));
        }
        let now = Utc::now();
        let mut ok = 0;
        let mut failures = Vec::new();
        {
            let mut state = self.shared.state.lock().await;
            for (table, r) in &result.tables {
                match r {
                    Ok(()) => {
                        ok += 1;
                        self.failed.remove(table);
                        multi_state::record_success(&mut state, table, now);
                    }
                    Err(msg) => {
                        self.failed.insert(table.clone(), msg.clone());
                        let action = multi_state::record_failure(
                            &mut state,
                            table,
                            msg,
                            self.shared.tables.spec.max_table_failures,
                            now,
                        );
                        failures.push((table.clone(), action, msg.clone()));
                    }
                }
            }
        }
        self.shared.persist().await?;
        for (table, action, msg) in failures {
            tracing::warn!(pipeline = %self.shared.opts.pipeline_name, table = %table, error = %msg, "mirror: table cycle failed");
            self.check_fail(&table, action, &msg)?;
        }
        let lagging = {
            let state = self.shared.state.lock().await;
            multi_state::lagging(&state, self.shared.tables.spec.lag_warning_secs, now)
        };
        let lagging_names: BTreeSet<String> = lagging.iter().map(|(n, _)| n.clone()).collect();
        for (table, secs) in lagging {
            if self.warned_lag.insert(table.clone()) {
                tracing::warn!(pipeline = %self.shared.opts.pipeline_name, table = %table, lag_secs = secs, "mirror: table is lagging and holds the shared stream's resume position back");
            }
        }
        self.warned_lag.retain(|t| lagging_names.contains(t));
        Ok(ok)
    }
}

/// Aborts a helper task when the cycle (or the whole mirror future) ends.
struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// How long a stream cycle gets to stop at a page boundary and flush before
/// the driver gives up on it.
const CYCLE_STOP_GRACE: Duration = Duration::from_secs(30);

/// Stop a running stream cycle cooperatively before the driver propagates an
/// error: cancel its token and wait (bounded) for the table pipelines to
/// flush, instead of dropping them mid-write.
async fn stop_cycle<T>(cancel: &CancellationToken, task: &mut AbortOnDrop<T>) {
    cancel.cancel();
    let _ = tokio::time::timeout(CYCLE_STOP_GRACE, &mut task.0).await;
}

async fn drive(shared: Arc<Shared>, cancel: CancellationToken) -> CliResult<()> {
    let mut d = Driver {
        shared,
        cancel,
        joined: Arc::new(Notify::new()),
        snapshots: JoinSet::new(),
        running: BTreeSet::new(),
        retry_at: HashMap::new(),
        warned_lag: BTreeSet::new(),
        failed: BTreeMap::new(),
    };
    let interval = d.shared.tables.spec.discover_interval_secs;
    let continuous = d.shared.continuous;
    let retry_paused = if continuous {
        d.shared.tables.spec.retry_paused_secs
    } else {
        0
    };
    let mut rediscover = true;
    let mut next_discovery = Instant::now();
    let mut backoff = Duration::from_secs(1);
    const MAX_BACKOFF: Duration = Duration::from_secs(60);

    let result: CliResult<()> = async {
        loop {
            if d.cancel.is_cancelled() {
                break;
            }
            if rediscover || (interval > 0 && Instant::now() >= next_discovery) {
                d.rediscover().await?;
                rediscover = false;
                next_discovery = Instant::now() + Duration::from_secs(interval.max(1));
            }
            d.reap_finished().await?;
            let due = {
                let state = d.shared.state.lock().await;
                due_for_snapshot(&state, retry_paused, Utc::now())
            };
            d.spawn_due(due.clone()).await?;

            if !continuous && (!d.snapshots.is_empty() || !due.is_empty()) {
                if d.snapshots.is_empty() {
                    let wait = d
                        .retry_at
                        .values()
                        .min()
                        .map(|at| at.saturating_duration_since(Instant::now()))
                        .unwrap_or(Duration::from_secs(1));
                    tokio::select! {
                        biased;
                        _ = d.cancel.cancelled() => break,
                        _ = tokio::time::sleep(wait) => {}
                    }
                } else {
                    d.wait_snapshots().await?;
                }
                continue;
            }

            let active = d.shared.state.lock().await.in_phase(TablePhase::Active);
            if !active.is_empty() {
                let cycle_cancel = d.cancel.child_token();
                let ticker = {
                    let shared = d.shared.clone();
                    tokio::spawn(async move {
                        loop {
                            tokio::time::sleep(PROGRESS_INTERVAL).await;
                            if let Err(e) = shared.persist().await {
                                tracing::warn!(error = %e, "mirror: progress write failed");
                            }
                        }
                    })
                };
                let _ticker = AbortOnDrop(ticker);
                let mut cycle_task = AbortOnDrop({
                    let shared = d.shared.clone();
                    let token = cycle_cancel.clone();
                    tokio::spawn(async move { run_cycle(&shared, active, token).await })
                });
                // While the stream runs the driver keeps discovering and
                // snapshotting; a table whose snapshot completes ends the cycle
                // so the next one streams it too.
                let cycle = loop {
                    let tick = if continuous {
                        let at = if interval > 0 {
                            next_discovery.min(Instant::now() + HOUSEKEEPING)
                        } else {
                            Instant::now() + HOUSEKEEPING
                        };
                        Some(tokio::time::Instant::from_std(at))
                    } else {
                        None
                    };
                    tokio::select! {
                        joined = &mut cycle_task.0 => {
                            break joined.map_err(|e| CliError::Internal(format!("stream cycle: {e}")))?;
                        }
                        _ = d.joined.notified() => cycle_cancel.cancel(),
                        Some(joined) = d.snapshots.join_next(), if !d.snapshots.is_empty() => {
                            let handled = match joined {
                                Ok((table, result)) => d.on_snapshot(table, result).await,
                                Err(e) => Err(CliError::Internal(format!("snapshot task: {e}"))),
                            };
                            if let Err(e) = handled {
                                stop_cycle(&cycle_cancel, &mut cycle_task).await;
                                return Err(e);
                            }
                        }
                        _ = async { tokio::time::sleep_until(tick.unwrap_or_else(tokio::time::Instant::now)).await }, if tick.is_some() => {
                            if interval > 0 && Instant::now() >= next_discovery {
                                if let Err(e) = d.rediscover().await {
                                    stop_cycle(&cycle_cancel, &mut cycle_task).await;
                                    return Err(e);
                                }
                                next_discovery = Instant::now() + Duration::from_secs(interval.max(1));
                            }
                            let due = {
                                let state = d.shared.state.lock().await;
                                due_for_snapshot(&state, retry_paused, Utc::now())
                            };
                            if !d.startable(&due).is_empty() {
                                cycle_cancel.cancel();
                            }
                        }
                    }
                };
                let interrupted = cycle_cancel.is_cancelled();
                drop(_ticker);
                let ok = d.apply_cycle(&cycle).await?;
                if !cycle.new_tables.is_empty() {
                    tracing::info!(pipeline = %d.shared.opts.pipeline_name, tables = ?cycle.new_tables, "mirror: new tables seen on the stream; re-discovering");
                    rediscover = true;
                }
                let failed = cycle.source_error.is_some() || (ok == 0 && !cycle.tables.is_empty());
                if let Some(e) = &cycle.source_error {
                    if !continuous {
                        return Err(CliError::Internal(format!("mirror CDC phase failed: {e}")));
                    }
                    tracing::warn!(pipeline = %d.shared.opts.pipeline_name, error = %e, backoff_secs = backoff.as_secs(), "mirror: stream cycle failed; resuming from the tables' positions after backoff");
                }
                if failed && continuous {
                    tokio::select! {
                        biased;
                        _ = d.cancel.cancelled() => break,
                        _ = tokio::time::sleep(backoff) => {}
                    }
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                } else {
                    backoff = Duration::from_secs(1);
                }
                if !continuous && !interrupted && !rediscover && d.snapshots.is_empty() {
                    let due = {
                        let state = d.shared.state.lock().await;
                        due_for_snapshot(&state, retry_paused, Utc::now())
                    };
                    if due.is_empty() {
                        break;
                    }
                }
            } else if !d.snapshots.is_empty() {
                tokio::select! {
                    biased;
                    _ = d.cancel.cancelled() => break,
                    _ = d.joined.notified() => {}
                    Some(joined) = d.snapshots.join_next() => {
                        let (table, result) = joined
                            .map_err(|e| CliError::Internal(format!("snapshot task: {e}")))?;
                        d.on_snapshot(table, result).await?;
                    }
                }
            } else {
                if !continuous {
                    break;
                }
                let wait = if interval > 0 {
                    next_discovery.saturating_duration_since(Instant::now())
                } else {
                    Duration::from_secs(60)
                };
                tokio::select! {
                    biased;
                    _ = d.cancel.cancelled() => break,
                    _ = tokio::time::sleep(wait.max(Duration::from_millis(100))) => {}
                }
            }
        }
        Ok(())
    }
    .await;
    d.cancel.cancel();
    let drained = d.wait_snapshots().await;
    d.shared.persist().await?;
    result.and(drained)?;
    if continuous {
        return Ok(());
    }
    one_shot_outcome(&d.failed)
}

/// A one-shot mirror that ends with a table whose latest snapshot or stream
/// cycle failed has not mirrored it, so it fails rather than exiting clean.
fn one_shot_outcome(failed: &BTreeMap<String, String>) -> CliResult<()> {
    if failed.is_empty() {
        return Ok(());
    }
    let detail: Vec<String> = failed.iter().map(|(t, e)| format!("{t}: {e}")).collect();
    Err(CliError::Internal(format!(
        "mirror: {} table(s) did not complete this run — {}",
        failed.len(),
        detail.join("; ")
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshots_replace_the_destination_and_clearing_reads_nothing() {
        let cfg = crate::config::parse_with_extension(
            r#"
version: 1
name: m
pipeline:
  source: { type: postgres, config: { connection_url: "postgres://x", query: "SELECT 1" } }
  sink:   { type: postgres, config: { connection_url: "postgres://y", table_name: t, column_mapping: auto_map, write_mode: upsert, key: [id], delete_marker: { field: __op, values: [d] } } }
  schema: { on_drift: evolve }
"#,
            "yaml",
        )
        .unwrap();
        let snap = expand(&cfg).unwrap().remove(0);
        let whole = snapshot_as_overwrite(&snap, false);
        assert_eq!(whole.sink.config["write_mode"], "overwrite");
        assert!(whole.sink.config.get("delete_marker").is_none());
        assert!(whole.sink.config.get("key").is_none());
        assert!(whole.schema.is_none());
        assert!(whole.source_override.is_none());
        let clear = snapshot_as_overwrite(&snap, true);
        assert!(clear.id.ends_with("::clear"));
        assert!(clear.source_override.is_some());
    }

    /// A driver error stops the running cycle through its token, giving it
    /// time to flush, before the task is dropped (#789 CLI-87).
    #[test]
    fn a_one_shot_run_fails_while_a_table_did_not_complete() {
        assert!(one_shot_outcome(&BTreeMap::new()).is_ok());
        let failed = BTreeMap::from([
            ("shop.a".to_string(), "boom".to_string()),
            ("shop.b".to_string(), "bang".to_string()),
        ]);
        let err = one_shot_outcome(&failed).unwrap_err().to_string();
        assert!(
            err.contains("2 table(s)")
                && err.contains("shop.a: boom")
                && err.contains("shop.b: bang"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn a_driver_error_cancels_the_cycle_before_dropping_it() {
        let cancel = CancellationToken::new();
        let seen = cancel.clone();
        let flushed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let f = flushed.clone();
        let mut task = AbortOnDrop(tokio::spawn(async move {
            seen.cancelled().await;
            f.store(true, std::sync::atomic::Ordering::SeqCst);
        }));
        stop_cycle(&cancel, &mut task).await;
        assert!(flushed.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[test]
    fn snapshot_strategies() {
        let s = |r, m, n| snapshot_strategy(r, m, n);
        assert_eq!(
            s(true, false, 1),
            SnapshotStrategy {
                overwrite: true,
                ..Default::default()
            }
        );
        assert_eq!(
            s(true, false, 4),
            SnapshotStrategy {
                clear_first: true,
                ..Default::default()
            }
        );
        assert_eq!(s(true, true, 1), SnapshotStrategy::default());
        assert_eq!(
            s(true, true, 4),
            SnapshotStrategy {
                first_shard_alone: true,
                ..Default::default()
            }
        );
        assert_eq!(s(false, false, 4), SnapshotStrategy::default());
        assert_eq!(s(false, true, 1), SnapshotStrategy::default());
    }

    #[test]
    fn summaries_fold_rows_and_errors() {
        let ok = RunSummary {
            invocations: vec![crate::executor::InvocationOutcome {
                row_id: "t".into(),
                parent_record_key: None,
                run_id: None,
                records_written: 4,
                error: None,
                error_kind: None,
                metrics: None,
                usage: None,
            }],
        };
        assert_eq!(summary_rows(&ok), 4);
        assert_eq!(summary_error(&ok), None);
        let mut bad = ok;
        bad.invocations[0].error = Some("boom".into());
        assert_eq!(summary_error(&bad), Some("boom".into()));
    }
}
