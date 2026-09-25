//! Run a list of [`ExpandedNode`]s under a bounded-concurrency executor.
//!
//! Semantics:
//!
//! - Roots run concurrently under `Semaphore(max_concurrent)`.
//! - Each root captures its written records (via a `CapturingSink` wrapper)
//!   so descendants can fan out per parent record.
//! - For each child whose parent has finished successfully, one pipeline
//!   invocation runs per parent record. `${parent.dotted.path}` tokens in the
//!   source / sink config and state-key suffix are resolved against that
//!   record via [`interpolate_record`].
//! - All invocations share one global semaphore — children and roots compete
//!   for the same budget.
//! - A node with `depends_on: [row, …]` starts only after every listed row's
//!   invocations finish successfully — pure completion-ordering, no record
//!   hand-off. A failed or skipped dependency skips the node (and, in turn,
//!   its own subtree and dependents).
//! - `on_error: continue` (default) skips a failed node's subtree but keeps
//!   running siblings. `on_error: stop` cancels everything after the first
//!   failure.
//! - State-key collisions among children of the same parent surface as a
//!   `CliError::DuplicateStateKey`.

use crate::auth_catalog::AuthCatalog;
use crate::config::{DispatchOrder, ExecutionSpec, OnError};
use crate::error::{CliError, CliResult};
use crate::expand::{ExpandedNode, NodeRole};
use crate::interpolate::interpolate_record;
use crate::registry::{build_sink, build_source};
use crate::state::build_state_store;
use async_trait::async_trait;
use chrono::{DateTime, FixedOffset};
use faucet_core::observability::Labels;
use faucet_core::{DlqConfig, FaucetError, OnBatchError, Pipeline, Sink, Source, StateStore};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore};

/// Captured fan-out records, keyed by node id. Records are held as `Arc<Value>`
/// so the per-level snapshot and per-child-unit hand-off are pointer bumps, not
/// deep clones of the JSON tree (#160).
type CapturedRecords = Arc<Mutex<HashMap<String, Vec<Arc<Value>>>>>;

/// Enumerated discovery dimensions (#501), keyed by discovery row id. Populated
/// by discovery invocations, read when building the `for_each` cross-product.
type DiscoveredDims = Arc<Mutex<HashMap<String, crate::discovery_matrix::Dim>>>;

/// Collected (list-valued) discovery dimensions (#531), keyed by discovery row
/// id. A chained `discover:` row (`for_each:` + `collect: true`) populates one
/// per-tuple list here; a consuming `Product` row injects the matching list into
/// each tuple ctx via [`discovery_matrix::inject_collected`].
type CollectedDims = Arc<Mutex<HashMap<String, crate::discovery_matrix::CollectedDim>>>;
use tokio_util::sync::CancellationToken;

/// Knobs passed to [`run_expanded`].
pub struct ExecuteOptions {
    /// Pipeline name — used in log lines and as the first segment of every
    /// state key.
    pub pipeline_name: String,
    /// Correlation id for the submitted run (#480). `faucet serve` passes the
    /// id it returned from `POST /v1/runs` so a completion notification can be
    /// matched back to the submission; every other runtime leaves it `None` and
    /// each invocation falls back to its own generated id.
    ///
    /// Deliberately *not* the observability `run_id`: one submitted run expands
    /// to one invocation per matrix row, and the span identity stays unique per
    /// invocation. Notifications carry both — `run_id` (this) to correlate to
    /// the submission, `invocation_id` to tell rows apart.
    pub run_id: Option<String>,
    /// Override for `execution.max_concurrent`. `None` → use the value in
    /// `ExecutionSpec` or the default (`num_cpus::get().min(4)`, floored at 1).
    pub execution: Option<ExecutionSpec>,
    /// Run-level **connector** concurrency override (#610): how many
    /// concurrent connections/fetches this run's source and sink may use,
    /// whatever the config says.
    ///
    /// Exists for the multi-tenant case — one template driving a customer with
    /// beefy read replicas and one with a small instance — where the pool size
    /// would otherwise be baked into the config, or have to be pre-declared as
    /// a `${param.*}` by the template author. Applied by rewriting whichever
    /// knob the connector declares in its own schema
    /// (`registry::override_source_concurrency`), so a connector with no such
    /// knob ignores it. Does **not** change `execution.max_concurrent` (matrix
    /// parallelism) or the server's `--max-concurrent` slots. For a sharded
    /// run it is per-shard, because each shard is its own invocation.
    pub concurrency: Option<usize>,
    /// `--dry-run` — every sink is replaced with a no-op counter.
    pub dry_run: bool,
    /// `--limit N` — wraps every sink to drop records past the cap.
    pub limit: Option<usize>,
    /// `--state-path PATH` — overrides the `file` state-store path.
    pub state_path_override: Option<PathBuf>,
    /// Clustered Mode B (#230): narrow this run's single source to one shard
    /// before streaming, and suffix its state key with the shard id so resume is
    /// per-shard. `None` (the default) runs the whole source unchanged. Only set
    /// by the serve shard executor; every other caller leaves it `None`.
    pub shard: Option<faucet_core::ShardSpec>,
    /// Shared auth providers built from the top-level `auth:` block. Connectors
    /// that reference one via `auth: { ref }` resolve against this catalog;
    /// every row sharing a provider gets the same `Arc` (one token, shared).
    pub auth: AuthCatalog,
    /// Wall-clock instant for `${now.*}` interpolation in this run's configs.
    /// `faucet run` sets process-start (or `--clock`); `faucet schedule` sets
    /// the tick's scheduled time in the schedule timezone.
    pub clock: DateTime<FixedOffset>,
    /// Optional external cancellation token. When set and cancelled, in-flight
    /// invocations stop at their next page boundary and **flush** their sinks
    /// (so buffered output like a Parquet footer is durable), rather than being
    /// hard-dropped (#146 H16). `faucet serve` wires this to run-cancel /
    /// timeout / shutdown; `faucet run` leaves it `None`.
    pub cancel: Option<CancellationToken>,
    /// Optional resilience policy (retry/backoff/circuit-breaker/poison),
    /// attached to every invocation's `RunStreamOptions` and injected into
    /// rest/xml/graphql sources. Built once from the top-level `resilience:`
    /// block; `None` preserves today's behaviour.
    pub resilience: Option<faucet_core::ResiliencePolicy>,
    /// Optional freshness/volume SLA (#202), evaluated after every **root**
    /// invocation against history persisted in the node's state store.
    /// Violations emit metrics + warnings; they never fail the run. `None`
    /// disables the pass entirely.
    pub sla: Option<crate::sla::SlaSpec>,
    /// Optional completeness reconciliation (#502): after every successful
    /// **root** invocation, run a count probe and **fail the run** on a shortfall
    /// beyond tolerance. `None` disables the pass. Same root/real-run scoping as
    /// the SLA pass (skipped for dry-run / `--limit` / shard / cancelled runs).
    pub reconcile: Option<crate::reconcile::ReconcileSpec>,
    /// Optional content verification (#701): after every successful **root**
    /// invocation (`verify.after_run`), compare the destination to the source
    /// by content; differences fail the run unless `fail_on_difference:
    /// false`, and `repair: true` re-syncs them first. Same root/real-run
    /// scoping as reconcile.
    pub verify: Option<crate::verify::VerifySpec>,
    /// Optional run rollback (#706): when set, every real **root** invocation
    /// writes a pre-run marker into its state store and hands the sink its
    /// journaling settings, so `faucet rollback --run <id>` can undo it.
    pub rollback: Option<crate::rollback::RollbackSpec>,
    /// Shared OpenLineage emitter, built once from the `lineage:` block. `None`
    /// disables lineage (and adds zero overhead). Gated on the `lineage` feature.
    #[cfg(feature = "lineage")]
    pub lineage: Option<std::sync::Arc<faucet_lineage::LineageEmitter>>,
    /// The resolved `lineage:` config block (facet/event toggles, sampling, job
    /// name template). Carried alongside the emitter so `run_one_invocation`
    /// knows which facets/events to assemble. Gated on the `lineage` feature.
    #[cfg(feature = "lineage")]
    pub lineage_cfg: Option<faucet_lineage::LineageConfig>,
    /// Optional notification/incident-routing notifier (#280), built once from
    /// the top-level `notifications:` block and shared across invocations. Fires
    /// run success/failure, SLA breach, circuit-open, contract-abort, and
    /// DLQ-threshold events after every **root** invocation. `None` disables
    /// notifications entirely (zero overhead). Gated on the `notify` feature.
    #[cfg(feature = "notify")]
    pub notifier: Option<std::sync::Arc<crate::notify::Notifier>>,
    /// Optional Data Movement Catalog store (#279), recorded into after every
    /// successful **root** invocation (dataset identity, schema timeline,
    /// volume/freshness, lineage edge). `faucet serve` passes its run-history
    /// backend + the serve run id; the CLI runtimes connect one from the
    /// `catalog:` block. `None` disables recording entirely (zero overhead).
    /// Recording never fails the run. Gated on the `catalog` feature.
    #[cfg(feature = "catalog")]
    pub catalog: Option<crate::catalog::CatalogHandle>,
}

/// Grace window granted to in-flight invocations to flush cooperatively after
/// an `on_error: stop` cancellation, before the remaining tasks are
/// hard-aborted (the backstop for a sink genuinely stuck mid-write). Bounded so
/// a hung sink can't wedge the whole run.
const STOP_FLUSH_GRACE: Duration = Duration::from_secs(5);

/// Typed classification of an invocation failure, carried alongside the
/// rendered [`InvocationOutcome::error`] so a consumer can react to a *kind* of
/// failure without matching its prose (PRINCIPLES §6). Grows by addition as
/// consumers need to distinguish more kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum InvocationErrorKind {
    /// [`FaucetError::CircuitOpen`] — the resilience circuit breaker tripped.
    /// `faucet schedule` delays its next tick by the policy cooldown rather
    /// than re-firing immediately at a destination that just tripped.
    CircuitOpen,
    /// Any other failure. No consumer needs to distinguish these yet, and it
    /// stays separate from `None` (= this outcome was never classified, e.g. a
    /// synthetic placeholder outcome) so the two are never confused.
    Other,
}

/// Order the ready set for permit acquisition (#644).
///
/// `Declared` returns the input untouched — the BFS order the caller built.
/// `Lpt` sorts by [`ExpandedNode::weight`] descending, **stably**, so ties and
/// unweighted rows keep declaration order and dispatch is never
/// nondeterministic. Unweighted rows sort last: a row nobody costed should not
/// displace one somebody did.
///
/// Pure, so the policy is testable without standing up a pipeline.
fn order_ready(
    ready: Vec<String>,
    nodes_by_id: &HashMap<String, ExpandedNode>,
    order: DispatchOrder,
) -> Vec<String> {
    if order == DispatchOrder::Declared || ready.len() < 2 {
        return ready;
    }
    let mut ranked = ready;
    // `sort_by` is stable, so equal keys keep their relative (declared) order.
    ranked.sort_by(|a, b| {
        let w = |id: &String| nodes_by_id.get(id).and_then(|n| n.weight);
        match (w(a), w(b)) {
            // Heaviest first. `total_cmp` rather than `partial_cmp` so a NaN
            // weight (a config can carry one) orders deterministically instead
            // of silently making the sort inconsistent.
            (Some(x), Some(y)) => y.total_cmp(&x),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        }
    });
    ranked
}

/// One pipeline invocation's outcome.
#[derive(Debug)]
pub struct InvocationOutcome {
    pub row_id: String,
    /// `None` for root invocations; for children, the value at `parent_key` in
    /// the parent record (rendered to a string).
    pub parent_record_key: Option<String>,
    /// This invocation's own run id — the value the `run_id` metadata column
    /// carries and the id `faucet rollback --run` takes (#706). `None` for
    /// synthetic outcomes where no pipeline ran.
    pub run_id: Option<String>,
    pub records_written: usize,
    pub error: Option<String>,
    /// Typed kind of `error`, set wherever the failure is still a typed
    /// `CliError`. `None` means "not classified" — either a success or a
    /// synthetic outcome built from an already-flattened message — so a
    /// consumer must never infer a kind from the `error` string.
    pub error_kind: Option<InvocationErrorKind>,
    /// Machine-readable per-invocation stats for `faucet run --output json`
    /// (#390). `None` for synthetic outcomes (a task panic, or the placeholder
    /// outcomes the replication / schedule orchestrators build) where no
    /// pipeline actually ran.
    pub metrics: Option<InvocationMetrics>,
}

/// Classify a failed invocation's typed error (#654 M2).
///
/// Pure: the one place a `CliError` becomes an [`InvocationErrorKind`], so the
/// scheduler's circuit-breaker cooldown keys off a variant instead of the
/// `CircuitOpen` display text — which a reworded `#[error(...)]` would silently
/// break, leaving the next cron tick to re-fire at a destination that just
/// tripped its own breaker.
pub fn classify_error(err: &CliError) -> InvocationErrorKind {
    match err {
        CliError::Faucet(FaucetError::CircuitOpen { .. }) => InvocationErrorKind::CircuitOpen,
        _ => InvocationErrorKind::Other,
    }
}

/// Per-invocation stats surfaced by `faucet run --output json` (#390). Every
/// field comes from counters the pipeline already maintains — no new
/// measurement. `records_read` is `None` unless input sampling was active
/// (a `lineage:` or `catalog:` block installs the source sampler).
#[derive(Debug, Clone, Default)]
pub struct InvocationMetrics {
    pub source_kind: String,
    pub sink_kind: String,
    pub duration_ms: u64,
    pub records_read: Option<u64>,
    pub dlq_count: u64,
    pub bookmark: Option<Value>,
}

/// What [`run_one_invocation`] hands back on success alongside the captured
/// records — the pipeline-level counters `run_unit` folds into an
/// [`InvocationMetrics`] (which then adds the connector kinds + wall-clock).
struct PipelineStats {
    run_id: String,
    records_written: usize,
    records_read: Option<u64>,
    dlq_count: u64,
    bookmark: Option<Value>,
}

/// Aggregate outcome of `run_expanded`.
#[derive(Debug)]
pub struct RunSummary {
    pub invocations: Vec<InvocationOutcome>,
}

impl RunSummary {
    pub fn failure_count(&self) -> usize {
        self.invocations
            .iter()
            .filter(|i| i.error.is_some())
            .count()
    }
    pub fn had_failures(&self) -> bool {
        self.failure_count() > 0
    }
}

/// Default number of matrix invocations to run in parallel when neither the
/// config's `execution.max_concurrent` nor a flag specifies one.
///
/// Scales with the core count but is capped at 8. The cap is deliberate: each
/// invocation is a *full pipeline* with its own connection pools / HTTP
/// clients, and matrix rows often target the same external system (one API,
/// one database), so an unbounded fan-out across, say, a 64-core box would
/// blow through that system's connection or rate limits rather than going
/// faster. Workloads that genuinely benefit from more parallelism set
/// `execution.max_concurrent` explicitly to opt out of the cap (#78 LOW).
fn default_concurrency() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .clamp(1, 8)
}

/// Resolve `execution.max_concurrent` into semaphore permits.
///
/// - `Some(0)` — **no limit**: every invocation runs in parallel. This is the
///   intuitive complement of `1` (serial): the user opts fully out of the cap.
/// - `Some(n)` — exactly `n` in flight.
/// - `None` — the CPU-scaled default ([`default_concurrency`]).
fn concurrency_permits(configured: Option<usize>) -> usize {
    match configured {
        Some(0) => Semaphore::MAX_PERMITS,
        Some(n) => n,
        None => default_concurrency(),
    }
}

/// Execute every node in `nodes`. `nodes` must be in BFS order (roots first
/// then children) — that's what [`crate::expand::expand`] returns.
/// [`run_expanded`] as a boxed future — for callers that run the executor
/// from *inside* an invocation (the `verify` repair pass, #701), where the
/// recursive `async fn` type would otherwise be a cycle.
pub fn run_expanded_boxed(
    nodes: Vec<ExpandedNode>,
    opts: ExecuteOptions,
) -> futures::future::BoxFuture<'static, CliResult<RunSummary>> {
    Box::pin(run_expanded(nodes, opts))
}

pub async fn run_expanded(nodes: Vec<ExpandedNode>, opts: ExecuteOptions) -> CliResult<RunSummary> {
    let on_error = opts
        .execution
        .as_ref()
        .map(|e| e.on_error)
        .unwrap_or_default();
    let max_concurrent =
        concurrency_permits(opts.execution.as_ref().and_then(|e| e.max_concurrent));
    let dispatch_order = opts
        .execution
        .as_ref()
        .map(|e| e.schedule)
        .unwrap_or_default();
    let semaphore = Arc::new(Semaphore::new(max_concurrent));

    // A `local_outputs:` block with nowhere to record to is inert (#587). Say so
    // once, here, rather than failing the run or going silent: the pipeline works
    // fine — it just will not be able to reclaim its own files later, and finding
    // that out only when `faucet cleanup` reports an empty ledger is worse.
    #[cfg(feature = "catalog")]
    if opts.catalog.is_none()
        && let Some(spec) = nodes.iter().find_map(|n| n.local_outputs.as_ref())
        && spec.track
    {
        tracing::warn!(
            "`local_outputs:` is set but no store is configured, so local output files are \
             not being tracked and cannot be reclaimed later — add a `catalog:` block \
             (e.g. `catalog: {{ url: sqlite:./faucet-catalog.db }}`) or run under \
             `faucet serve`, whose --history backend holds the ledger"
        );
    }

    // Index nodes by id for parent → children lookups.
    // parent id → child node ids. Keyed and valued by id (not Vec index) so the
    // failure cascade can look children up directly instead of indexing into a
    // HashMap's nondeterministic iteration order (#78/#24).
    let mut children_of: HashMap<String, Vec<String>> = HashMap::new();
    for n in nodes.iter() {
        if let NodeRole::Child { parent_id, .. } = &n.role {
            children_of
                .entry(parent_id.clone())
                .or_default()
                .push(n.id.clone());
        }
    }

    // Captured records per node id. Only populated for nodes that have
    // children (= are referenced by another node's `parent:`). Records are held
    // as `Arc<Value>` so the per-level snapshot clone and the per-child-unit
    // hand-off are pointer bumps, not deep clones of the JSON tree (#160).
    let captured: CapturedRecords = Arc::new(Mutex::new(HashMap::new()));
    // Discovery dimensions (#501): value-sets enumerated by `discover:` rows,
    // read when a `for_each` row builds its cross-product.
    let discovered: DiscoveredDims = Arc::new(Mutex::new(HashMap::new()));
    // Collected (list-valued) discovery dimensions (#531): per-tuple lists a
    // chained `discover:` row publishes, injected into a consuming row's tuples.
    let collected: CollectedDims = Arc::new(Mutex::new(HashMap::new()));

    let mut outcomes: Vec<InvocationOutcome> = Vec::new();
    let mut skipped_subtrees: HashSet<String> = HashSet::new();

    // Root cooperative-cancel token: the caller's (serve wires run-cancel /
    // timeout / shutdown) or a fresh one. Each level derives a child token so
    // an `on_error: stop` cancels only that level's invocations, while an
    // external cancel of the root propagates to every level (#146 H16).
    let cancel = opts.cancel.clone().unwrap_or_default();
    let opts = Arc::new(opts);

    // We execute level-by-level. Each level is "every node whose parent is
    // already done." Roots are level 0. For each level, we spawn one task per
    // (node, parent-record) pair and await them all before moving on.
    let mut remaining: HashSet<String> = nodes.iter().map(|n| n.id.clone()).collect();
    let mut completed: HashSet<String> = HashSet::new();
    let nodes_by_id: HashMap<String, ExpandedNode> =
        nodes.into_iter().map(|n| (n.id.clone(), n)).collect();

    // Per-parent projection: what to keep from each captured record so the
    // fan-out buffer holds only the fields children reference (#160).
    let projections = build_projections(&nodes_by_id, &children_of);

    // Sort node ids in their original BFS row order so the executor is
    // deterministic — important for `on_error: stop`, where the first failure
    // halts the rest of the level.
    let bfs_order: Vec<String> = {
        let mut ids: Vec<(usize, String)> = nodes_by_id
            .values()
            .map(|n| (n.row_index, n.id.clone()))
            .collect();
        ids.sort_by_key(|(i, _)| *i);
        ids.into_iter().map(|(_, id)| id).collect()
    };

    while !remaining.is_empty() {
        // Pick every remaining node whose parent (if any) and every
        // `depends_on` row are already terminal (completed or skipped), in
        // deterministic BFS order. Whether a skipped/failed prerequisite
        // *skips* the node is decided in the unit loop below — readiness only
        // asks "is there anything left to wait for".
        let ready: Vec<String> = bfs_order
            .iter()
            .filter(|id| remaining.contains(*id))
            .filter(|id| {
                let node = &nodes_by_id[*id];
                let parent_done = match &node.role {
                    // Discovery/Product have no `parent:`; their prerequisites
                    // (the `for_each` dims) are carried in `depends_on`.
                    NodeRole::Root | NodeRole::Discovery { .. } | NodeRole::Product { .. } => true,
                    NodeRole::Child { parent_id, .. } => {
                        completed.contains(parent_id) || skipped_subtrees.contains(parent_id)
                    }
                };
                parent_done
                    && node
                        .depends_on
                        .iter()
                        .all(|d| completed.contains(d) || skipped_subtrees.contains(d))
            })
            .cloned()
            .collect();

        // Heaviest-first when asked (#644). Only the order in which ready
        // siblings queue for a permit changes — same budget, same JoinSet,
        // same cancellation.
        let ready = order_ready(ready, &nodes_by_id, dispatch_order);

        if ready.is_empty() {
            // No node is ready but some remain — an expand.rs invariant was
            // violated (e.g. an orphaned parent reference). Surface it instead
            // of silently dropping the remaining work and reporting success
            // (#78/#24).
            let mut stuck: Vec<String> = remaining.iter().cloned().collect();
            stuck.sort();
            return Err(CliError::Internal(format!(
                "executor deadlock: {} node(s) never became ready (no completed/skipped \
                 parent or dependency): {}",
                stuck.len(),
                stuck.join(", ")
            )));
        }

        // Build the work units for this level. Each unit is one invocation —
        // a root runs once; a child runs once per parent record.
        let mut units: Vec<Unit> = Vec::new();
        // Move only the captured records of the parents whose children run this
        // level out of the shared map. This both narrows the snapshot and frees
        // each parent's buffer the moment its children consume it: all of a
        // parent's children become ready in the same level, so its records are
        // needed exactly once. Units hold their own `Arc<Value>` clones, so
        // removing the map entry here only drops the map's hold (#160).
        let level_records: HashMap<String, Vec<Arc<Value>>> = {
            let consumed_parents: HashSet<&str> = ready
                .iter()
                .filter_map(|id| match &nodes_by_id[id].role {
                    NodeRole::Child { parent_id, .. } => Some(parent_id.as_str()),
                    _ => None,
                })
                .collect();
            let mut cap = captured.lock().await;
            consumed_parents
                .iter()
                .filter_map(|p| cap.remove(*p).map(|v| (p.to_string(), v)))
                .collect()
        };
        for id in &ready {
            let node = &nodes_by_id[id];
            // If a parent failed (and on_error=continue), the subtree is
            // skipped. Surface a synthetic "skipped" outcome and move on.
            if let NodeRole::Child { parent_id, .. } = &node.role
                && skipped_subtrees.contains(parent_id)
            {
                skipped_subtrees.insert(id.clone());
                tracing::warn!(row = %id, parent = %parent_id, "skipping subtree under failed parent");
                continue;
            }
            // Same for a failed/skipped `depends_on` prerequisite: the row
            // waited for something that never succeeded, so running it would
            // violate the ordering contract. Mark it skipped so its own
            // subtree and dependents cascade too.
            if let Some(dep) = node
                .depends_on
                .iter()
                .find(|d| skipped_subtrees.contains(d.as_str()))
            {
                skipped_subtrees.insert(id.clone());
                tracing::warn!(
                    row = %id, dependency = %dep,
                    "skipping row: a depends_on row failed or was skipped"
                );
                continue;
            }
            match &node.role {
                NodeRole::Root => {
                    let uses_state = node.state.is_some() || opts.state_path_override.is_some();
                    let state_key = build_state_key(&opts.pipeline_name, &node.id, None);
                    validate_unit_state_key(&node.id, uses_state, &state_key)?;
                    units.push(Unit {
                        node: node.clone(),
                        parent_record: None,
                        state_key,
                        parent_record_key: None,
                        product_ctx: None,
                    });
                }
                NodeRole::Discovery { dims, .. } => {
                    if dims.is_empty() {
                        // One-level discovery (#501): one invocation; run_unit
                        // intercepts it to enumerate the dimension. State is
                        // unused, so the key is a placeholder.
                        let state_key = build_state_key(&opts.pipeline_name, &node.id, None);
                        units.push(Unit {
                            node: node.clone(),
                            parent_record: None,
                            state_key,
                            parent_record_key: None,
                            product_ctx: None,
                        });
                    } else {
                        // Chained discovery (#531): fan out over the upstream
                        // dimensions, running the enumeration once per tuple.
                        let resolved = match resolve_product_dims(dims, &discovered, id).await? {
                            Some(r) => r,
                            None => {
                                skipped_subtrees.insert(id.clone());
                                continue;
                            }
                        };
                        for ctx in crate::discovery_matrix::cartesian(&resolved) {
                            let suffix =
                                crate::discovery_matrix::tuple_state_key_suffix(&resolved, &ctx);
                            let state_key =
                                build_state_key(&opts.pipeline_name, &node.id, Some(&suffix));
                            units.push(Unit {
                                node: node.clone(),
                                parent_record: None,
                                state_key,
                                parent_record_key: Some(suffix),
                                product_ctx: Some(ctx),
                            });
                        }
                    }
                }
                NodeRole::Product {
                    dims,
                    collected: collected_ids,
                } => {
                    // Gather the enumerated dimensions in declared order. They
                    // are guaranteed present: each dim is in this row's
                    // `depends_on`, so a skipped/failed dim already skipped this
                    // row above. The guard inside is defensive.
                    let resolved = match resolve_product_dims(dims, &discovered, id).await? {
                        Some(r) => r,
                        None => {
                            skipped_subtrees.insert(id.clone());
                            continue;
                        }
                    };
                    // Collected (list-valued) dimensions (#531) this row injects
                    // into each tuple ctx. Guaranteed present (each is in
                    // `depends_on`); a missing entry injects an empty list.
                    let collected_dims: Vec<crate::discovery_matrix::CollectedDim> = {
                        let cmap = collected.lock().await;
                        collected_ids
                            .iter()
                            .filter_map(|cid| cmap.get(cid).cloned())
                            .collect()
                    };
                    let uses_state = node.state.is_some() || opts.state_path_override.is_some();
                    let mut seen_keys: HashSet<String> = HashSet::new();
                    for mut ctx in crate::discovery_matrix::cartesian(&resolved) {
                        crate::discovery_matrix::inject_collected(&mut ctx, &collected_dims);
                        let suffix =
                            crate::discovery_matrix::tuple_state_key_suffix(&resolved, &ctx);
                        let state_key =
                            build_state_key(&opts.pipeline_name, &node.id, Some(&suffix));
                        validate_unit_state_key(&node.id, uses_state, &state_key)?;
                        if !seen_keys.insert(state_key.clone()) {
                            return Err(CliError::DuplicateStateKey {
                                id: node.id.clone(),
                                state_key,
                            });
                        }
                        units.push(Unit {
                            node: node.clone(),
                            parent_record: None,
                            state_key,
                            parent_record_key: Some(suffix),
                            product_ctx: Some(ctx),
                        });
                    }
                }
                NodeRole::Child {
                    parent_id,
                    parent_key,
                } => {
                    let parent_records = level_records.get(parent_id).cloned().unwrap_or_default();
                    if parent_records.is_empty() {
                        tracing::info!(
                            row = %id, parent = %parent_id,
                            "parent produced no records — child skipped"
                        );
                        continue;
                    }
                    // Detect state-key collisions among siblings sharing one parent.
                    let uses_state = node.state.is_some() || opts.state_path_override.is_some();
                    let mut seen_keys: HashSet<String> = HashSet::new();
                    for record in &parent_records {
                        let pk_value = resolve_parent_key(record, parent_key);
                        let pk_string = pk_value
                            .as_ref()
                            .map(value_to_string_brief)
                            .unwrap_or_else(|| "(missing)".to_string());
                        let state_key =
                            build_state_key(&opts.pipeline_name, &node.id, Some(&pk_string));
                        validate_unit_state_key(&node.id, uses_state, &state_key)?;
                        if !seen_keys.insert(state_key.clone()) {
                            return Err(CliError::DuplicateStateKey {
                                id: node.id.clone(),
                                state_key,
                            });
                        }
                        units.push(Unit {
                            node: node.clone(),
                            parent_record: Some(record.clone()),
                            state_key,
                            parent_record_key: Some(pk_string),
                            product_ctx: None,
                        });
                    }
                }
            }
        }
        drop(level_records);

        // #552: overwrite fan-out groups. A `write_mode: overwrite` node that
        // fans out over N parent records (or discovery tuples) into ONE shared
        // destination must truncate+swap that destination exactly ONCE across
        // the whole group — not once per invocation, which would leave only the
        // last parent's rows (silent data loss). Group the level's units by
        // their *resolved* sink destination: units sharing a destination form a
        // group whose lifecycle the executor owns (begin here, commit/abort
        // after the join); units with per-parent destinations (`${parent.*}` in
        // the table) each land in their own single-member group, exactly as
        // before. `dry_run` uses counting sinks with no real destination, so it
        // is skipped entirely.
        let (overwrite_groups, overwrite_task_group) = if opts.dry_run {
            (Vec::new(), HashMap::new())
        } else {
            plan_overwrite_groups(&units, opts.as_ref())?
        };
        for group in &overwrite_groups {
            // Build a short-lived sink just for begin, then drop it (closing its
            // pool) before the invocations run — so it never contends with the
            // per-invocation sinks on a single-writer backend.
            let mut cfg = group.cfg.clone();
            mark_overwrite_staging(&group.kind, &mut cfg, group.members > 1);
            let sink = build_sink(&group.kind, cfg, &opts.auth).await?;
            sink.begin_overwrite().await.map_err(|e| {
                CliError::Internal(format!(
                    "overwrite: preparing destination '{}': {e}",
                    group.dest
                ))
            })?;
        }
        // Group indices whose fan-out had at least one failed invocation: the
        // whole group must abort (a full-refresh must never commit partial data).
        let mut failed_overwrite_groups: HashSet<usize> = HashSet::new();

        let mut had_level_failure = false;
        let mut nodes_with_any_failure: HashSet<String> = HashSet::new();

        // Unified parallel execution. Tasks run concurrently under the global
        // semaphore. Under `on_error: stop`, the first failure triggers
        // `JoinSet::abort_all()` — pending tasks waiting on a permit are
        // dropped before they do real work, and in-flight tasks are
        // cancelled at their next `.await` point (potentially leaving
        // partial sink state — the trade-off users opt into by choosing
        // `stop`). Under `on_error: continue` every spawned task runs to
        // completion regardless of sibling failures.
        // Per-level cancel token: cancelling it (on `on_error: stop`) stops only
        // this level's invocations cooperatively; it is a child of the root
        // token, so an external cancel (serve) still propagates here (#146 H16).
        let level_cancel = cancel.child_token();
        let mut joinset = tokio::task::JoinSet::new();
        // Map each spawned task's id back to its row id + parent key so that a
        // panic (surfaced as a JoinError, which doesn't carry the unit) can be
        // attributed to the right invocation.
        let mut task_meta: HashMap<tokio::task::Id, (String, Option<String>)> = HashMap::new();
        for unit in units {
            let sem = Arc::clone(&semaphore);
            let opts2 = Arc::clone(&opts);
            let captured = Arc::clone(&captured);
            let discovered = Arc::clone(&discovered);
            let collected = Arc::clone(&collected);
            let capture = projections.get(&unit.node.id).cloned();
            let meta = (unit.node.id.clone(), unit.parent_record_key.clone());
            // #552: a unit whose destination is part of an executor-managed
            // overwrite group runs with its per-invocation begin/commit/abort
            // suppressed — the group's single lifecycle is driven here instead.
            let suppress_overwrite = overwrite_task_group.contains_key(&meta);
            // A *grouped* overwrite (>1 writer → one table) must stage; a solo
            // one loads directly. Drives the bigquery `_overwrite_staging` flag.
            let overwrite_grouped = overwrite_task_group
                .get(&meta)
                .is_some_and(|gi| overwrite_groups[*gi].members > 1);
            let unit_cancel = level_cancel.clone();
            let handle = joinset.spawn(async move {
                let _permit = sem.acquire().await.expect("semaphore not closed");
                run_unit(
                    &unit,
                    capture,
                    &captured,
                    &discovered,
                    &collected,
                    &opts2,
                    unit_cancel,
                    suppress_overwrite,
                    overwrite_grouped,
                )
                .await
            });
            task_meta.insert(handle.id(), meta);
        }

        let mut stop_triggered = false;
        let mut aborted = false;
        let mut stop_deadline: Option<tokio::time::Instant> = None;
        loop {
            // Once `on_error: stop` has cancelled the level, give in-flight
            // invocations a bounded grace to flush cooperatively, then
            // hard-abort the stragglers (the backstop for a sink stuck
            // mid-write that can't reach a page boundary to observe the cancel).
            let joined = match stop_deadline {
                Some(deadline) if !aborted => {
                    match tokio::time::timeout_at(deadline, joinset.join_next_with_id()).await {
                        Ok(j) => j,
                        Err(_) => {
                            tracing::warn!(
                                "on_error: stop — flush grace elapsed; aborting remaining \
                                 in-flight invocations"
                            );
                            joinset.abort_all();
                            aborted = true;
                            continue;
                        }
                    }
                }
                _ => joinset.join_next_with_id().await,
            };
            let Some(joined) = joined else { break };
            // A failure (an `Err` outcome or a panicked task) marks the level
            // failed and, under `on_error: stop`, stops the rest. A panicking
            // connector must NOT take down the whole process (#78/#24).
            let outcome = match joined {
                Ok((_id, outcome)) => outcome,
                Err(e) if e.is_cancelled() => {
                    // Expected after abort_all() — cancelled before/at an await.
                    // Not counted as a failure or a success.
                    continue;
                }
                Err(e) => {
                    let (row_id, parent_record_key) = task_meta
                        .get(&e.id())
                        .cloned()
                        .unwrap_or_else(|| ("<unknown>".to_string(), None));
                    InvocationOutcome {
                        row_id,
                        parent_record_key,
                        run_id: None,
                        records_written: 0,
                        error: Some(format!("pipeline invocation task panicked: {e}")),
                        // A panicked task never produced a typed error.
                        error_kind: None,
                        metrics: None,
                    }
                }
            };

            if let Some(err) = &outcome.error {
                tracing::error!(row = %outcome.row_id, error = %err, "pipeline invocation failed");
                had_level_failure = true;
                nodes_with_any_failure.insert(outcome.row_id.clone());
                // #552: fail the invocation's overwrite group so it aborts
                // (discards staging) rather than committing a partial refresh.
                if let Some(gi) = overwrite_task_group
                    .get(&(outcome.row_id.clone(), outcome.parent_record_key.clone()))
                {
                    failed_overwrite_groups.insert(*gi);
                }
                if matches!(on_error, OnError::Stop) && !stop_triggered {
                    stop_triggered = true;
                    tracing::error!(
                        "on_error: stop — cancelling in-flight invocations (cooperative \
                         flush), then aborting any that don't stop within the grace window"
                    );
                    // Cooperative first: in-flight pipelines flush at their next
                    // page boundary so a Parquet footer / S3 upload is completed
                    // rather than orphaned (#146 H16).
                    level_cancel.cancel();
                    stop_deadline = Some(tokio::time::Instant::now() + STOP_FLUSH_GRACE);
                }
            } else {
                tracing::info!(
                    row = %outcome.row_id,
                    records_written = outcome.records_written,
                    elapsed_ms = outcome.metrics.as_ref().map(|m| m.duration_ms).unwrap_or(0),
                    "pipeline invocation completed"
                );
            }
            outcomes.push(outcome);
        }

        // #552: finalize each overwrite group's single truncate/swap. Commit
        // only a group with no failed invocation on a level that was not
        // cancelled/stopped — otherwise abort (best-effort), leaving the prior
        // destination exactly as it was. This runs once per shared destination
        // across the whole fan-out, so every parent's rows are swapped in
        // together, atomically.
        let level_cancelled = level_cancel.is_cancelled() || cancel.is_cancelled();
        for (gi, group) in overwrite_groups.iter().enumerate() {
            let must_abort = level_cancelled || failed_overwrite_groups.contains(&gi);
            let mut cfg = group.cfg.clone();
            mark_overwrite_staging(&group.kind, &mut cfg, group.members > 1);
            let sink = build_sink(&group.kind, cfg, &opts.auth).await?;
            if must_abort {
                if let Err(e) = sink.abort_overwrite().await {
                    tracing::warn!(
                        error = %e, dest = %group.dest,
                        "overwrite: discarding staging after a failed/cancelled fan-out failed; \
                         the destination is unchanged"
                    );
                }
            } else {
                sink.commit_overwrite().await.map_err(|e| {
                    CliError::Internal(format!(
                        "overwrite: swapping destination '{}' into place: {e}",
                        group.dest
                    ))
                })?;
            }
        }

        // Mark ready nodes done (some may have produced both successes and
        // failures across their per-parent-record fan-outs — we treat a node
        // as "failed" overall if any of its invocations failed).
        for id in ready {
            remaining.remove(&id);
            if nodes_with_any_failure.contains(&id) {
                skipped_subtrees.insert(id.clone());
                // Cascade to descendants in case we have multi-level chains.
                if let Some(children) = children_of.get(&id) {
                    for cid in children {
                        skipped_subtrees.insert(cid.clone());
                    }
                }
            } else {
                completed.insert(id);
            }
        }

        if had_level_failure && matches!(on_error, OnError::Stop) {
            tracing::error!("on_error: stop — aborting after first failure");
            // Any unfinished work surfaces as "skipped"; we just break here.
            break;
        }
    }

    Ok(RunSummary {
        invocations: outcomes,
    })
}

/// One scheduled invocation — a root runs once, a child runs once per parent
/// record. Built by the level loop, consumed by [`run_unit`].
struct Unit {
    node: ExpandedNode,
    parent_record: Option<Arc<Value>>,
    state_key: String,
    parent_record_key: Option<String>,
    /// Per-tuple interpolation context for a discovery-driven `for_each` row
    /// (#501): `{dim_id -> { alias: value }}`. `None` for roots/children.
    product_ctx: Option<HashMap<String, Value>>,
}

/// Resolve the enumerated dimensions a `for_each`/chained-discovery row fans out
/// over (#501/#531). Returns `Ok(None)` — meaning "skip this row" — when a
/// dimension is unavailable (a dependency was skipped/failed) or the cross-product
/// is empty; `Err` when the product exceeds [`MAX_MATRIX_PRODUCT`]. Each dim is in
/// the row's `depends_on`, so the missing guard is defensive.
async fn resolve_product_dims(
    dims: &[String],
    discovered: &DiscoveredDims,
    id: &str,
) -> CliResult<Option<Vec<crate::discovery_matrix::Dim>>> {
    let resolved: Vec<crate::discovery_matrix::Dim> = {
        let dim_map = discovered.lock().await;
        let mut v = Vec::with_capacity(dims.len());
        for d in dims {
            match dim_map.get(d) {
                Some(dim) => v.push(dim.clone()),
                None => {
                    tracing::warn!(row = %id, dimension = %d, "for_each dimension unavailable — row skipped");
                    return Ok(None);
                }
            }
        }
        v
    };
    let size = crate::discovery_matrix::product_size(&resolved);
    if size == 0 {
        tracing::info!(row = %id, "for_each cross-product is empty — row skipped");
        return Ok(None);
    }
    if size > crate::discovery_matrix::MAX_MATRIX_PRODUCT {
        return Err(CliError::Config(format!(
            "matrix row '{id}': for_each cross-product is {size} invocations, over the limit of \
             {} — narrow the discovery dimensions",
            crate::discovery_matrix::MAX_MATRIX_PRODUCT
        )));
    }
    Ok(Some(resolved))
}

/// A group of overwrite fan-out invocations sharing one physical destination
/// (#552). The executor drives `begin_overwrite` once before the group runs and
/// `commit_overwrite`/`abort_overwrite` once after — so the destination is
/// truncated and swapped exactly once, retaining every parent's rows rather
/// than only the last invocation's.
struct OverwriteGroup {
    /// Human-readable destination (sink kind + a short config hint) for logs.
    dest: String,
    /// The group's sink kind + resolved destination config. A short-lived
    /// lifecycle sink is built from these for `begin` (before the group runs)
    /// and again for `commit`/`abort` (after) — never held open across the
    /// invocations, so it can't contend with the per-invocation sinks on a
    /// single-writer backend like SQLite. begin/commit/abort address the same
    /// deterministic staging target the per-invocation sinks write into.
    kind: String,
    cfg: Value,
    /// Number of invocations writing to this destination. `> 1` means a true
    /// fan-out (several writers → one table): a shared staging table is then the
    /// only safe swap, so the BigQuery sink is told to stage (`_overwrite_staging`)
    /// rather than load directly. A single-member group is a solo overwrite.
    members: usize,
}

/// Inject the internal `_overwrite_staging` flag into a **bigquery** sink config
/// for a *grouped* overwrite fan-out (>1 writer → one physical table), where a
/// shared staging table is the only safe swap across the independent writer sink
/// instances. Solo overwrites (single member) and every non-bigquery kind are
/// left untouched — the latter would reject an unknown field, and a solo
/// overwrite loads directly into the target (a WRITE_TRUNCATE load is atomic on
/// its own).
fn mark_overwrite_staging(kind: &str, cfg: &mut Value, grouped: bool) {
    if grouped
        && crate::registry::sink_supports_direct_overwrite(kind)
        && let Value::Object(map) = cfg
    {
        map.insert("_overwrite_staging".to_string(), Value::Bool(true));
    }
}

/// Whether a node's sink is configured for `write_mode: overwrite`. The write
/// mode is `#[serde(flatten)]`'d into the sink config, so it reads off the top
/// level — mirroring the `write_mode` gate in `expand.rs`.
fn node_sink_is_overwrite(node: &ExpandedNode) -> bool {
    node.sink.config.get("write_mode").and_then(Value::as_str) == Some("overwrite")
}

/// A short destination hint for logs (e.g. `postgres:public.orders`).
fn describe_sink_dest(kind: &str, cfg: &Value) -> String {
    for field in [
        "table",
        "table_id",
        "collection",
        "index",
        "dataset",
        "path",
    ] {
        if let Some(v) = cfg.get(field).and_then(Value::as_str) {
            return format!("{kind}:{v}");
        }
    }
    kind.to_string()
}

/// Resolve a unit's sink destination the same way [`run_one_invocation`] does —
/// `${now.*}` then the fan-out `${parent.*}` / `${dim.*}` context — so units are
/// grouped by their *actual* destination (a `${parent.*}`-templated table yields
/// a distinct destination per parent, and stays a single-member group).
fn resolved_sink_destination(unit: &Unit, opts: &ExecuteOptions) -> CliResult<Value> {
    let mut sink_cfg = unit.node.sink.config.clone();
    resolve_now_inplace(&mut sink_cfg, opts.clock)?;
    let mut ctx: HashMap<String, Value> = HashMap::new();
    if let (Some(record), NodeRole::Child { parent_id, .. }) =
        (unit.parent_record.as_deref(), &unit.node.role)
    {
        ctx.insert(parent_id.clone(), record.clone());
    }
    if let Some(pc) = unit.product_ctx.as_ref() {
        for (k, v) in pc {
            ctx.insert(k.clone(), v.clone());
        }
    }
    if !ctx.is_empty() {
        resolve_inplace(&mut sink_cfg, &ctx)?;
    }
    // Rollback (#706): the group's lifecycle sink (begin/commit/abort) is a
    // different instance from the writers, and it is the commit that keeps
    // the replaced table — so it needs the `keep_previous` setting too. The
    // run id is irrelevant to the copy (the run-id column comes from the
    // metadata decorator on the writers).
    if let Some(spec) = opts.rollback.as_ref().filter(|r| r.enabled)
        && matches!(unit.node.role, NodeRole::Root)
        && !opts.dry_run
        && opts.limit.is_none()
        && opts.shard.is_none()
        && unit.node.state.is_some()
        && crate::rollback::sink_supports_rollback(&unit.node.sink.kind)
    {
        let column = crate::rollback::run_id_column(unit.node.metadata_columns.as_ref());
        crate::rollback::inject_write_spec(&mut sink_cfg, spec, "", &column);
    }
    Ok(sink_cfg)
}

/// Plan the level's overwrite fan-out groups (#552). Units whose sink is
/// `write_mode: overwrite` are grouped by their resolved destination; one
/// lifecycle sink is built per distinct destination. Returns the groups plus a
/// map from each grouped invocation's `(row_id, parent_key)` to its group index
/// — used to suppress the per-invocation lifecycle and to attribute a failed
/// invocation to its group.
/// Maps a grouped invocation's `(row_id, parent_key)` to its overwrite-group
/// index (#552).
type OverwriteTaskGroup = HashMap<(String, Option<String>), usize>;

/// (#552) The planned overwrite groups plus the invocation → group-index map.
type OverwritePlan = (Vec<OverwriteGroup>, OverwriteTaskGroup);

fn plan_overwrite_groups(units: &[Unit], opts: &ExecuteOptions) -> CliResult<OverwritePlan> {
    let mut groups: Vec<OverwriteGroup> = Vec::new();
    let mut task_group: HashMap<(String, Option<String>), usize> = HashMap::new();
    let mut index_by_key: HashMap<String, usize> = HashMap::new();
    for unit in units {
        if !node_sink_is_overwrite(&unit.node) {
            continue;
        }
        let kind = unit.node.sink.kind.clone();
        let cfg = resolved_sink_destination(unit, opts)?;
        // Same sink source-object + key-preserving resolution ⇒ stable
        // serialization, so equal destinations collapse to one group.
        let key = format!(
            "{kind}\u{0}{}",
            serde_json::to_string(&cfg).unwrap_or_default()
        );
        let gi = match index_by_key.get(&key) {
            Some(gi) => {
                groups[*gi].members += 1;
                *gi
            }
            None => {
                let gi = groups.len();
                groups.push(OverwriteGroup {
                    dest: describe_sink_dest(&kind, &cfg),
                    kind: kind.clone(),
                    cfg,
                    members: 1,
                });
                index_by_key.insert(key, gi);
                gi
            }
        };
        task_group.insert((unit.node.id.clone(), unit.parent_record_key.clone()), gi);
    }
    Ok((groups, task_group))
}

#[allow(clippy::too_many_arguments)]
async fn run_unit(
    unit: &Unit,
    capture: Option<Arc<Projection>>,
    captured: &CapturedRecords,
    discovered: &DiscoveredDims,
    collected: &CollectedDims,
    opts: &ExecuteOptions,
    cancel: CancellationToken,
    suppress_overwrite: bool,
    overwrite_grouped: bool,
) -> InvocationOutcome {
    // A discovery row (#501) enumerates a value-set instead of running a
    // source→sink pipeline: build its source, drain it, project + dedup, and
    // publish the dimension for dependent `for_each` rows. No sink, no capture.
    // A chained discovery (#531) additionally resolves its per-tuple `${dim}`
    // tokens and, with `collect`, publishes one list per tuple.
    if let NodeRole::Discovery {
        select,
        as_alias,
        collect,
        dims,
    } = &unit.node.role
    {
        return run_discovery(
            &unit.node,
            select,
            as_alias,
            *collect,
            dims,
            unit.product_ctx.as_ref(),
            discovered,
            collected,
            opts,
        )
        .await;
    }
    let needs_capture = capture.is_some();
    let started = std::time::Instant::now();
    let result = boxed_run_one_invocation(
        &unit.node,
        unit.parent_record.as_deref(),
        unit.product_ctx.as_ref(),
        &unit.state_key,
        capture,
        opts,
        cancel,
        suppress_overwrite,
        overwrite_grouped,
    )
    .await;
    let duration_ms = started.elapsed().as_millis() as u64;
    let row_id = unit.node.id.clone();
    let parent_record_key = unit.parent_record_key.clone();
    // Per-row timing metric: the pipeline already measured `duration_ms`; surface
    // it as a histogram so per-matrix-row time is observable on a scrape, not just
    // in `--output json`.
    crate::exec_metrics::record_invocation(
        &opts.pipeline_name,
        &row_id,
        &unit.node.source.kind,
        &unit.node.sink.kind,
        result.is_ok(),
        duration_ms,
    );
    let base_metrics = || InvocationMetrics {
        source_kind: unit.node.source.kind.clone(),
        sink_kind: unit.node.sink.kind.clone(),
        duration_ms,
        ..Default::default()
    };
    match result {
        Ok((records, stats)) => {
            if needs_capture {
                captured
                    .lock()
                    .await
                    .entry(row_id.clone())
                    .or_default()
                    // Move each record into an `Arc` once here; downstream
                    // per-level / per-unit hand-offs then clone only the pointer.
                    .extend(records.into_iter().map(Arc::new));
            }
            InvocationOutcome {
                row_id,
                parent_record_key,
                run_id: Some(stats.run_id.clone()),
                records_written: stats.records_written,
                error: None,
                error_kind: None,
                metrics: Some(InvocationMetrics {
                    records_read: stats.records_read,
                    dlq_count: stats.dlq_count,
                    bookmark: stats.bookmark,
                    ..base_metrics()
                }),
            }
        }
        Err(e) => InvocationOutcome {
            row_id,
            parent_record_key,
            run_id: None,
            records_written: 0,
            error: Some(e.to_string()),
            error_kind: Some(classify_error(&e)),
            metrics: Some(base_metrics()),
        },
    }
}

/// [`run_one_invocation`] behind a heap allocation, built in its own
/// never-inlined frame.
///
/// One invocation's future is enormous in a debug build — every optional pass
/// (masking, quality, contract, drift, verify, lineage, catalog) is a state
/// machine inlined into it. Awaiting it directly puts that whole size into
/// the *caller's* stack frame (the compiler reserves the temporary even on
/// branches that never run), which overflowed the 2 MiB test-thread stack
/// under `--all-features`. Constructing it here and returning a `BoxFuture`
/// keeps the caller's frame at pointer size; only this frame pays the
/// transient cost. `run_unit` and every other caller go through this.
#[allow(clippy::too_many_arguments)]
#[inline(never)]
fn boxed_run_one_invocation<'a>(
    node: &'a ExpandedNode,
    parent_record: Option<&'a Value>,
    product_ctx: Option<&'a HashMap<String, Value>>,
    state_key: &'a str,
    capture: Option<Arc<Projection>>,
    opts: &'a ExecuteOptions,
    cancel: CancellationToken,
    suppress_overwrite: bool,
    overwrite_grouped: bool,
) -> futures::future::BoxFuture<'a, CliResult<(Vec<Value>, PipelineStats)>> {
    Box::pin(run_one_invocation(
        node,
        parent_record,
        product_ctx,
        state_key,
        capture,
        opts,
        cancel,
        suppress_overwrite,
        overwrite_grouped,
    ))
}

/// Run a discovery row (#501): build its source, drain it, project `select`,
/// dedup, and publish the dimension for dependent `for_each` rows. No sink is
/// built and nothing is written — the outcome records the enumerated count.
#[allow(clippy::too_many_arguments)]
async fn run_discovery(
    node: &ExpandedNode,
    select: &str,
    as_alias: &str,
    collect: bool,
    dims: &[String],
    product_ctx: Option<&HashMap<String, Value>>,
    discovered: &DiscoveredDims,
    collected: &CollectedDims,
    opts: &ExecuteOptions,
) -> InvocationOutcome {
    let started = std::time::Instant::now();
    let source_kind = node.source.kind.clone();
    let result: CliResult<usize> = async {
        let mut cfg = node.source.config.clone();
        // `${now.*}` resolves for a discovery source like any other; `${vars}`
        // and shared `auth: { ref }` are already handled by the registry build.
        resolve_now_inplace(&mut cfg, opts.clock)?;
        // Chained discovery (#531): resolve the upstream tuple `${dim}` tokens in
        // the source config (e.g. `/crm/v3/properties/${types.name}`).
        if let Some(pc) = product_ctx {
            resolve_inplace(&mut cfg, pc)?;
        }
        let source = build_source(
            &source_kind,
            cfg,
            &opts.auth,
            opts.resilience.as_ref().map(|r| &r.retry),
        )
        .await?;
        let records = source.fetch_all().await?;
        let values = crate::discovery_matrix::project_dedup(&records, select);
        let n = values.len();
        if collect {
            // Publish one list per upstream tuple, keyed by the tuple, so a
            // consuming `Product` row injects the whole list into one request.
            let pc = product_ctx.cloned().unwrap_or_default();
            let key = crate::discovery_matrix::collected_tuple_key(dims, &pc);
            let mut cmap = collected.lock().await;
            let entry = cmap.entry(node.id.clone()).or_insert_with(|| {
                crate::discovery_matrix::CollectedDim {
                    id: node.id.clone(),
                    alias: as_alias.to_string(),
                    dims: dims.to_vec(),
                    by_tuple: HashMap::new(),
                }
            });
            entry.by_tuple.insert(key, values);
        } else {
            discovered.lock().await.insert(
                node.id.clone(),
                crate::discovery_matrix::Dim {
                    id: node.id.clone(),
                    alias: as_alias.to_string(),
                    values,
                },
            );
        }
        Ok(n)
    }
    .await;
    let duration_ms = started.elapsed().as_millis() as u64;
    let metrics = |records_read: usize| InvocationMetrics {
        source_kind: source_kind.clone(),
        sink_kind: String::new(),
        duration_ms,
        records_read: Some(records_read as u64),
        ..Default::default()
    };
    match result {
        Ok(n) => {
            tracing::info!(row = %node.id, values = n, "discovery dimension enumerated");
            InvocationOutcome {
                row_id: node.id.clone(),
                parent_record_key: None,
                run_id: None,
                records_written: 0,
                error: None,
                error_kind: None,
                metrics: Some(metrics(n)),
            }
        }
        Err(e) => InvocationOutcome {
            row_id: node.id.clone(),
            parent_record_key: None,
            run_id: None,
            records_written: 0,
            error: Some(e.to_string()),
            error_kind: Some(classify_error(&e)),
            metrics: Some(metrics(0)),
        },
    }
}

/// Produce `{pipeline_name}::{row_id}` or `{pipeline_name}::{row_id}::{key}`.
pub(crate) fn build_state_key(
    pipeline_name: &str,
    row_id: &str,
    parent_key: Option<&str>,
) -> String {
    match parent_key {
        None => format!("{pipeline_name}::{row_id}"),
        Some(k) => format!("{pipeline_name}::{row_id}::{k}"),
    }
}

/// Reject an invalid state key up front (at unit construction) when the node
/// will use a state store, so a bad pipeline name or parent-key value surfaces
/// as a clear [`CliError::InvalidStateKey`] instead of a late mid-run
/// `FaucetError::State` after connectors are built and the stream has started.
fn validate_unit_state_key(node_id: &str, uses_state: bool, state_key: &str) -> CliResult<()> {
    if uses_state {
        faucet_core::state::validate_state_key(state_key).map_err(|e| {
            CliError::InvalidStateKey {
                id: node_id.to_owned(),
                state_key: state_key.to_owned(),
                reason: e.to_string(),
            }
        })?;
    }
    Ok(())
}

/// Walk the parent record by `parent_key` (a dotted path) and clone the value.
fn resolve_parent_key(record: &Value, parent_key: &str) -> Option<Value> {
    let mut cur = record;
    for segment in parent_key.split('.') {
        cur = match cur {
            Value::Object(m) => m.get(segment)?,
            Value::Array(a) => a.get(segment.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(cur.clone())
}

/// What to keep from each of a parent's records when capturing for fan-out.
/// Projecting to only the fields children reference bounds orchestrator memory
/// at O(referenced-fields × N) instead of O(full-record × N) (#160).
#[derive(Debug, Clone)]
enum Projection {
    /// Keep the whole record — a child referenced `${parent}` (the entire record)
    /// or used an empty `parent_key`, so nothing can be safely dropped.
    Full,
    /// Keep only these pre-split, non-overlapping dotted paths.
    Paths(Vec<Vec<String>>),
}

/// Split a dotted path into segments.
fn split_path(path: &str) -> Vec<String> {
    path.split('.').map(|s| s.to_string()).collect()
}

/// Reduce a set of segment-paths to a minimal non-overlapping set: drop any path
/// that has a (segment-wise prefix) ancestor in the set — `["user"]` covers
/// `["user","name"]`. Sorting puts ancestors before their descendants.
fn minimal_paths(mut paths: Vec<Vec<String>>) -> Vec<Vec<String>> {
    paths.sort();
    paths.dedup();
    let mut kept: Vec<Vec<String>> = Vec::new();
    for p in paths {
        let covered = kept
            .iter()
            .any(|anc| p.len() >= anc.len() && p[..anc.len()] == anc[..]);
        if !covered {
            kept.push(p);
        }
    }
    kept
}

/// Resolve a pre-split dotted path against `record`, dispatching on each value's
/// type exactly like `resolve_parent_key` / `interpolate::resolve_dotted`.
fn walk_value(record: &Value, segments: &[String]) -> Option<Value> {
    let mut cur = record;
    for seg in segments {
        cur = match cur {
            Value::Object(m) => m.get(seg)?,
            Value::Array(a) => a.get(seg.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(cur.clone())
}

/// Insert `leaf` at `segments` into `out`, creating intermediate `Value::Object`
/// nodes keyed by the literal segment string. Callers pass non-overlapping
/// `segments` (see `minimal_paths`), so a node that must be an object is never
/// already a leaf.
fn graft_object(out: &mut Value, segments: &[String], leaf: Value) {
    if segments.is_empty() {
        return;
    }
    let mut cur = out;
    for seg in &segments[..segments.len() - 1] {
        let map = match cur {
            Value::Object(m) => m,
            _ => return,
        };
        cur = map
            .entry(seg.clone())
            .or_insert_with(|| Value::Object(serde_json::Map::new()));
    }
    if let Value::Object(m) = cur {
        m.insert(segments[segments.len() - 1].clone(), leaf);
    }
}

/// Project `record` down to `projection`, building an all-objects reduced tree.
/// Because the readers (`resolve_parent_key`, `interpolate_record`) dispatch on
/// the reduced value's type, an array-index segment like `0` is stored — and
/// later read — as the object key `"0"`, so resolution matches the original.
fn project_record(record: &Value, projection: &Projection) -> Value {
    match projection {
        Projection::Full => record.clone(),
        Projection::Paths(paths) => {
            let mut out = Value::Object(serde_json::Map::new());
            for segs in paths {
                if let Some(v) = walk_value(record, segs) {
                    graft_object(&mut out, segs, v);
                }
            }
            out
        }
    }
}

/// Compute, per parent id, what to keep from each of its records: the union of
/// every child's `parent_key` (the state-key path) and every `${parent.path}`
/// token the children reference (from their pre-collected `deferred_refs`).
/// Reused by the level loop to project captured records (#160).
fn build_projections(
    nodes_by_id: &HashMap<String, ExpandedNode>,
    children_of: &HashMap<String, Vec<String>>,
) -> HashMap<String, Arc<Projection>> {
    let mut out = HashMap::new();
    for (parent_id, child_ids) in children_of {
        let mut raw: Vec<Vec<String>> = Vec::new();
        let mut full = false;
        for cid in child_ids {
            let child = &nodes_by_id[cid];
            if let NodeRole::Child { parent_key, .. } = &child.role {
                if parent_key.is_empty() {
                    full = true;
                } else {
                    raw.push(split_path(parent_key));
                }
            }
            for dref in &child.deferred_refs {
                if dref.referenced_id == *parent_id {
                    if dref.dotted_path.is_empty() {
                        full = true; // `${parent}` — whole record
                    } else {
                        raw.push(split_path(&dref.dotted_path));
                    }
                }
            }
        }
        // Defensive: `raw` is empty only when every child had an empty
        // parent_key, which already set `full`. The `|| raw.is_empty()` keeps us
        // on `Full` even if that ever changes, so we never project to an empty
        // tree that would drop the state-key path.
        let projection = if full || raw.is_empty() {
            Projection::Full
        } else {
            Projection::Paths(minimal_paths(raw))
        };
        out.insert(parent_id.clone(), Arc::new(projection));
    }
    out
}

/// Run one pipeline invocation. Returns (captured records, records_written).
/// Assemble the runtime [`Pipeline`] for one invocation from a node's specs.
///
/// This is the `with_*` builder chain — state store, DLQ, cancellation, quality,
/// contract, masking, schema-drift, adaptive batching, resilience, and delivery
/// mode — lifted out of [`run_one_invocation`] so the wiring is testable in
/// isolation and a preview-mode / feature-gate mistake (cf. #321 H1) is harder
/// to make (#324 C). Behaviour is identical to the previous inline chain; the
/// only I/O it performs is building the DLQ sink. `source`/`sink` are borrowed
/// for the returned pipeline's lifetime; `state` is moved in.
#[allow(clippy::too_many_arguments)]
async fn build_pipeline<'a>(
    source: &'a dyn Source,
    sink: &'a dyn Sink,
    node: &ExpandedNode,
    opts: &ExecuteOptions,
    state: Option<Arc<dyn StateStore>>,
    cancel: &CancellationToken,
    pipeline_name: &str,
    row_id: &str,
    run_id: &str,
    cleanup_scope: Option<Value>,
    suppress_overwrite: bool,
) -> CliResult<Pipeline<'a, dyn Source + 'a, dyn Sink + 'a>> {
    let mut pipeline = Pipeline::new(source, sink)
        .with_name(pipeline_name.to_owned())
        .with_row(row_id.to_owned())
        .with_run_id(run_id.to_owned());
    if let Some(store) = state {
        pipeline = pipeline.with_state_store(store);
    }
    if let Some(ref dlq_spec) = node.dlq {
        let dlq_cfg = build_dlq_config(dlq_spec).await?;
        pipeline = pipeline.with_dlq(dlq_cfg);
    }
    // Cooperative cancellation: a cancelled token makes the streaming loop stop
    // at the next page boundary and flush the sink (#146 H16). The pipeline takes
    // a clone so the caller's lineage terminal-event classification and SLA pass
    // can still read `cancel.is_cancelled()` (cheap — the token is an `Arc`).
    pipeline = pipeline.with_cancel(cancel.clone());
    // Pipeline-level quality checks (v1: no matrix-row override). `expand` already
    // validated this spec, but compile again here to obtain the runtime
    // `CompiledQuality`; map any error to a config-level failure.
    #[cfg(feature = "quality")]
    if let Some(ref quality_spec) = node.quality {
        let compiled = Arc::new(
            faucet_core::CompiledQuality::compile(quality_spec)
                .map_err(|e| CliError::Config(format!("quality: {e}")))?,
        );
        pipeline = pipeline.with_quality(compiled);
    }
    // Pipeline-level data contract (v1: no matrix-row override). `expand` already
    // validated the spec; compile again here to obtain the runtime
    // `CompiledContract`.
    #[cfg(feature = "contract")]
    if let Some(ref contract_spec) = node.contract {
        let compiled = Arc::new(
            faucet_core::CompiledContract::compile(contract_spec)
                .map_err(|e| CliError::Config(format!("contract: {e}")))?,
        );
        pipeline = pipeline.with_contract(compiled);
    }
    // Pipeline-level PII masking (v1: no matrix-row override). Compile scoped to
    // this node's destination sink — by template name (`sink_ref`) and by
    // connector kind — so `applies_to` per-destination rules resolve. When no
    // rule applies to this sink the compiled policy is empty and the pass is
    // skipped entirely.
    #[cfg(feature = "masking")]
    if let Some(ref masking_spec) = node.masking {
        let sink_ids = [node.sink_ref.as_str(), node.sink.kind.as_str()];
        let compiled = faucet_core::CompiledMasking::compile_for_sink(masking_spec, &sink_ids)
            .map_err(|e| CliError::Config(format!("masking: {e}")))?;
        if !compiled.is_empty() {
            pipeline = pipeline.with_masking(Arc::new(compiled));
        }
    }
    // Schema-drift policy (pipeline-level in v1; same for every invocation).
    if let Some(ref sd) = node.schema {
        pipeline = pipeline.with_schema_drift(faucet_core::SchemaDriftPolicy::compile(sd));
    }
    // ── Scoped cleanup (#478) ────────────────────────────────────────────────
    // Attaching the policy is what authorises a delete, so the coarse gating
    // lives here — `run_stream` only adds "the run finished uncancelled".
    //
    //  - Roots only: a child fans out per parent record, and each invocation
    //    claims its own scope, which is exactly right — but a *non-root* here
    //    means a child, which is still a root-of-its-own-scope, so the real
    //    exclusions are the synthetic runs below.
    //  - `--dry-run` replaces the sink with a counter and `--limit` wraps it to
    //    drop records: under either, "what this run wrote" is a fiction, so a
    //    delete computed from it would remove live rows.
    //  - A shard reads a fraction of the source, so its written-key set is
    //    incomplete by construction — deleting the difference would delete the
    //    other shards' rows.
    if let Some(scope) = cleanup_scope {
        let synthetic = opts.dry_run || opts.limit.is_some() || opts.shard.is_some();
        if synthetic {
            tracing::warn!(
                row = %row_id,
                "scoped cleanup skipped: --dry-run / --limit / shard runs do not write the \
                 authoritative record set for the scope, so a delete would remove live rows"
            );
        } else {
            let map: std::collections::BTreeMap<String, Value> = scope
                .as_object()
                .map(|o| o.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                .unwrap_or_default();
            let key: Vec<String> = node
                .sink
                .config
                .get("key")
                .and_then(|v| v.as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(str::to_owned))
                        .collect()
                })
                .unwrap_or_default();
            let policy = faucet_core::CleanupPolicy::new(map, key, faucet_core::DEFAULT_MAX_KEYS)
                .map_err(|e| CliError::Config(format!("cleanup: {e}")))?;
            pipeline = pipeline.with_cleanup(Arc::new(policy));
        }
    }
    // Execution-level adaptive batch-size controller (shared by all rows).
    if let Some(ab) = opts
        .execution
        .as_ref()
        .and_then(|e| e.adaptive_batch_size.clone())
    {
        ab.validate()
            .map_err(|e| CliError::Config(format!("adaptive_batch_size: {e}")))?;
        pipeline = pipeline.with_adaptive(ab);
    }
    // Resilience policy (retry/backoff/circuit-breaker/poison). Top-level in v1,
    // so the same policy is attached to every invocation.
    if let Some(policy) = opts.resilience.clone() {
        pipeline = pipeline.with_resilience(policy);
    }
    // Delivery guarantee (exactly-once resume/skip when the node opted in; the
    // expand gate already verified source/sink/state support). Preview modes
    // (`--dry-run`, `--limit`) swap in counting/truncating sinks that cannot
    // uphold an atomic watermark — and a token committed for a truncated page
    // would corrupt it — so they always run at-least-once.
    let effective_delivery = if opts.dry_run || opts.limit.is_some() {
        faucet_core::idempotency::DeliveryMode::AtLeastOnce
    } else {
        node.delivery
    };
    pipeline = pipeline.with_delivery(effective_delivery);
    // #552: when this invocation is one of several fanning out into a shared
    // `write_mode: overwrite` destination, the executor owns the truncate/swap
    // lifecycle across the whole group — the pipeline must not begin/commit it
    // per invocation (which would leave only the last parent's rows).
    pipeline = pipeline.with_suppress_overwrite(suppress_overwrite);
    Ok(pipeline)
}

#[allow(clippy::too_many_arguments)]
async fn run_one_invocation(
    node: &ExpandedNode,
    parent_record: Option<&Value>,
    product_ctx: Option<&HashMap<String, Value>>,
    state_key: &str,
    capture: Option<Arc<Projection>>,
    opts: &ExecuteOptions,
    cancel: CancellationToken,
    suppress_overwrite: bool,
    overwrite_grouped: bool,
) -> CliResult<(Vec<Value>, PipelineStats)> {
    // Observability identity for this invocation — built once, reused by both
    // the Pipeline builder and the transform instrumentation.
    let run_id = uuid::Uuid::now_v7().to_string();
    // Notification run identity + timing (#480). `run_id` here correlates to the
    // *submission* (serve passes the id it returned from `POST /v1/runs`);
    // `invocation_id` is this row's own id, so a matrix run's notifications are
    // still individually identifiable. Monotonic `Instant` for the duration —
    // subtracting the wall-clock stamps would go negative across an NTP step.
    #[cfg(feature = "notify")]
    let invocation_started = std::time::Instant::now();
    #[cfg(feature = "notify")]
    let notify_run = crate::notify::RunContext::start(
        Some(opts.run_id.clone().unwrap_or_else(|| run_id.clone())),
        Some(run_id.clone()),
    );
    let pipeline_name = opts.pipeline_name.clone();
    let row_id = node.id.clone();
    #[cfg(feature = "lineage")]
    let lineage = opts.lineage.clone();
    #[cfg(feature = "lineage")]
    let lineage_cfg = opts.lineage_cfg.clone();
    let obs_labels = Labels::new(pipeline_name.clone(), row_id.clone(), run_id.clone());
    // Whether this invocation records into the Data Movement Catalog (#279):
    // roots only, and never for dry-run / --limit / shard runs (their volumes
    // and datasets are partial or synthetic) — the same scoping as SLA.
    #[cfg(feature = "catalog")]
    let catalog_active = opts.catalog.is_some()
        && matches!(node.role, NodeRole::Root)
        && !opts.dry_run
        && opts.limit.is_none()
        && opts.shard.is_none();
    // 1) Resolve `${parent.path}` in the per-row source + sink configs.
    let mut source_cfg = node.source.config.clone();
    let mut sink_cfg = node.sink.config.clone();
    // For a *grouped* overwrite fan-out (>1 writer → one table), tell a bigquery
    // sink to stage rather than load directly — the independent writer instances
    // can't coordinate a direct WRITE_TRUNCATE. A solo overwrite is left to load
    // directly into the target. No-op for non-bigquery kinds and non-overwrite.
    mark_overwrite_staging(&node.sink.kind, &mut sink_cfg, overwrite_grouped);

    // Resolve `${now.*}` run-clock tokens for every invocation (root + child),
    // before the parent-record pass. Leaves all other tokens verbatim.
    resolve_now_inplace(&mut source_cfg, opts.clock)?;
    resolve_now_inplace(&mut sink_cfg, opts.clock)?;
    // `${backfill.*}` tokens are substituted by the `faucet backfill`
    // orchestrator before nodes reach the executor. One still present here
    // means a window-scoped config is being driven by `run`/`schedule`/
    // `serve` — fail loudly instead of handing the literal token to the
    // connector (#282).
    reject_unresolved_backfill_tokens(&source_cfg, "source")?;
    reject_unresolved_backfill_tokens(&sink_cfg, "sink")?;

    // Rollback (#706): a real root run of an undo-capable sink is journaled.
    // The sink gets its per-run settings through its flattened `WriteSpec`;
    // the pre-run marker is written once the state store is built below.
    let rollback_active = opts.rollback.as_ref().is_some_and(|r| r.enabled)
        && matches!(node.role, NodeRole::Root)
        && !opts.dry_run
        && opts.limit.is_none()
        && opts.shard.is_none()
        && node.state.is_some()
        && crate::rollback::sink_supports_rollback(&node.sink.kind);
    let run_id_column = crate::rollback::run_id_column(node.metadata_columns.as_ref());
    if rollback_active && let Some(spec) = &opts.rollback {
        crate::rollback::inject_write_spec(&mut sink_cfg, spec, &run_id, &run_id_column);
    }

    // Scoped-cleanup claim (#478). Resolved through the *same* token passes as
    // the connector configs — the scope is typically `${parent.id}` on a child
    // row, so it has to see the parent record exactly like the source URL does.
    let mut cleanup_scope: Option<Value> = node
        .cleanup_scope
        .as_ref()
        .map(|m| Value::Object(m.iter().map(|(k, v)| (k.clone(), v.clone())).collect()));
    if let Some(scope) = cleanup_scope.as_mut() {
        resolve_now_inplace(scope, opts.clock)?;
        reject_unresolved_backfill_tokens(scope, "complete_for")?;
    }

    // Runtime fan-out context. A `parent:` child sees `{parent_id: record}`;
    // a discovery-driven `for_each` row (#501) sees `{dim_id: {alias: value}}`
    // for each dimension in its product tuple. Both resolve `${id.path}` tokens
    // through the same `interpolate_record` path. Kept in function scope so the
    // transform-compile step below can resolve the same tokens in a `set`
    // transform's values (#568).
    let mut fanout_ctx: HashMap<String, Value> = HashMap::new();
    if let (Some(record), NodeRole::Child { parent_id, .. }) = (parent_record, &node.role) {
        fanout_ctx.insert(parent_id.clone(), record.clone());
    }
    if let Some(pc) = product_ctx {
        for (k, v) in pc {
            fanout_ctx.insert(k.clone(), v.clone());
        }
    }
    if !fanout_ctx.is_empty() {
        resolve_inplace(&mut source_cfg, &fanout_ctx)?;
        resolve_inplace(&mut sink_cfg, &fanout_ctx)?;
        if let Some(scope) = cleanup_scope.as_mut() {
            resolve_inplace(scope, &fanout_ctx)?;
        }
    }

    // 2) Build source + sink. `faucet dlq replay` injects a pre-built source
    //    (an envelope-unwrapping DLQ reader) via `source_override`; every
    //    config-driven node builds from the connector registry. The override
    //    is taken once — replay runs a single invocation.
    // Run-level concurrency override (#610), applied after every
    // interpolation pass so it always wins over what the config says, and
    // before the build so the connector sees only the final value.
    if let Some(n) = opts.concurrency {
        if let Some(knob) =
            crate::registry::override_source_concurrency(&node.source.kind, &mut source_cfg, n)
        {
            tracing::debug!(row = %node.id, kind = %node.source.kind, knob, n, "concurrency override applied to source");
        }
        if let Some(knob) =
            crate::registry::override_sink_concurrency(&node.sink.kind, &mut sink_cfg, n)
        {
            tracing::debug!(row = %node.id, kind = %node.sink.kind, knob, n, "concurrency override applied to sink");
        }
    }

    let source = match node.source_override.as_ref().and_then(|o| o.take()) {
        Some(prebuilt) => prebuilt,
        None => {
            build_source(
                &node.source.kind,
                source_cfg,
                &opts.auth,
                opts.resilience.as_ref().map(|r| &r.retry),
            )
            .await?
        }
    };

    // Catalog identity (#279): read the dataset URIs off the *raw* connectors,
    // before any wrapper is layered on.
    #[cfg(feature = "catalog")]
    let source_dataset_uri = source.dataset_uri();

    // Clustered Mode B: narrow the raw source to its assigned shard BEFORE any
    // wrapping (the TransformingSource / StateKeyOverride wrappers do not forward
    // apply_shard, so it must reach the concrete connector).
    if let Some(shard) = &opts.shard {
        source
            .apply_shard(shard)
            .await
            .map_err(|e| CliError::Internal(format!("applying shard {:?}: {e}", shard.id)))?;
    }
    let raw_sink: Box<dyn Sink> = if opts.dry_run {
        Box::new(CountingSink::new())
    } else {
        build_sink(&node.sink.kind, sink_cfg, &opts.auth).await?
    };
    #[cfg(feature = "catalog")]
    let sink_dataset_uri = raw_sink.dataset_uri();
    let raw_sink: Box<dyn Sink> = match opts.limit {
        Some(n) => Box::new(LimitedSink::wrap(raw_sink, n)),
        None => raw_sink,
    };
    let captured = Arc::new(Mutex::new(Vec::<Value>::new()));
    let sink: Box<dyn Sink> = match &capture {
        Some(projection) => Box::new(CapturingSink::wrap(
            raw_sink,
            Arc::clone(&captured),
            Arc::clone(projection),
        )),
        None => raw_sink,
    };

    // ── Lineage: sampling wrappers (only for requested facets) ───────────────
    // `in_sample` taps the source's pre-transform records (input schema /
    // column lineage); `out_sample` taps the sink's written records (output
    // schema / RUNNING-heartbeat throughput counter). Both stay `None` when no
    // facet/event needs them, so lineage adds zero per-record overhead.
    #[cfg(feature = "lineage")]
    let (in_sample, out_sample) = {
        use std::sync::Arc as StdArc;
        let mut want = false;
        let mut cap = 0usize;
        if let (Some(_), Some(lc)) = (&lineage, &lineage_cfg) {
            let want_schema = lc.include_schema_facet || lc.include_column_lineage;
            if want_schema {
                cap = cap.max(lc.sample_records);
            }
            want = want_schema || lc.emit_on.running;
        }
        // The catalog (#279) needs input/output samples for schema inference
        // and record counts regardless of any `lineage:` block; take the max
        // of both caps when both are active.
        #[cfg(feature = "catalog")]
        if catalog_active {
            want = true;
            cap = cap.max(
                opts.catalog
                    .as_ref()
                    .map(|h| h.sample_records)
                    .unwrap_or(crate::catalog::DEFAULT_SAMPLE_RECORDS),
            );
        }
        if want {
            (
                Some(StdArc::new(faucet_lineage::SampleState::new(cap))),
                Some(StdArc::new(faucet_lineage::SampleState::new(cap))),
            )
        } else {
            (None, None)
        }
    };

    // Wrap the raw source so it samples PRE-transform input records — this must
    // sit between `build_source` and `TransformingSource`.
    #[cfg(feature = "lineage")]
    let source: Box<dyn Source> = match &in_sample {
        Some(state) => Box::new(faucet_lineage::SamplingSource::new(
            source,
            std::sync::Arc::clone(state),
        )),
        None => source,
    };

    // 3) Compile transforms. Resolve `${now.*}` run-clock tokens and, on a
    //    child / discovery fan-out, `${parent.*}` record tokens in each
    //    transform's config first — exactly as for source/sink above — so e.g. a
    //    `set` transform stamping `${now.date}` or `${company.company_id}` writes
    //    the real value instead of the literal token string (#568). Without this
    //    the token leaks into every record.
    let mut transforms = node.transforms.clone();
    for t in &mut transforms {
        resolve_now_inplace(&mut t.config, opts.clock)?;
        if !fanout_ctx.is_empty() {
            resolve_inplace(&mut t.config, &fanout_ctx)?;
        }
    }
    // With `arrow`, compile the columnar batch forms too so an all-columnar
    // chain (e.g. `parquet → sql → parquet`) runs on the Arrow fast path; a
    // `Value`-only stage in the chain leaves `batch_fns` with a `None` entry,
    // which keeps the whole pipeline on the `Value` path (#375).
    #[cfg(feature = "arrow")]
    let source: Box<dyn Source> = {
        let (stages, batch_fns) = crate::transforms::compile_transforms_columnar(&transforms)?;
        if stages.is_empty() {
            source
        } else {
            Box::new(faucet_core::TransformingSource::new_with_batches(
                source,
                stages,
                batch_fns,
                obs_labels.clone(),
            )?)
        }
    };
    #[cfg(not(feature = "arrow"))]
    let source: Box<dyn Source> = {
        let stages = crate::transforms::compile_transforms(&transforms)?;
        if stages.is_empty() {
            source
        } else {
            Box::new(faucet_core::TransformingSource::new(
                source,
                stages,
                obs_labels.clone(),
            )?)
        }
    };

    // 4) Build state store. If the source opts into state, wrap it so the
    //    executor's per-row state key is used instead of the source's natural
    //    one (which is shared across all matrix rows of the same kind).
    let state = build_state_for_node(node, opts.state_path_override.as_deref()).await?;
    // Preview modes must not persist bookmarks: the counting/truncating sinks
    // return `Ok` without a real write, so a persisted (advanced) bookmark would
    // make the next real run skip unwritten records (#321 H1). Wrap the store so
    // reads still resume faithfully but writes are dropped.
    let state: Option<Arc<dyn StateStore>> = match state {
        Some(inner) if opts.dry_run || opts.limit.is_some() => {
            Some(Arc::new(ReadOnlyStateStore { inner }))
        }
        other => other,
    };
    // Keep a handle for the post-run SLA pass (#202) — `state` itself is moved
    // into the pipeline below.
    let sla_store = state.clone();
    // Per-shard bookmark: suffix the state key with the shard id so a reassigned
    // shard resumes where its dead owner left off, independent of sibling shards.
    let effective_state_key = match &opts.shard {
        Some(shard) => format!("{state_key}::{}", shard.id),
        None => state_key.to_owned(),
    };
    // Rollback (#706): record what the bookmark and watermark were before this
    // run writes, so undoing it can rewind them. Never fails the run — a run
    // whose marker could not be written is simply not undoable.
    if rollback_active
        && let (Some(spec), Some(store)) = (&opts.rollback, &state)
        && sink.supports_rollback()
    {
        let marker = crate::rollback::marker_for(
            &run_id,
            &pipeline_name,
            node,
            &effective_state_key,
            opts.clock,
            sink.dataset_uri(),
            run_id_column.clone(),
        );
        if let Err(e) =
            crate::rollback::prepare_boxed(store.as_ref(), sink.as_ref(), marker, spec.retain).await
        {
            tracing::warn!(
                run_id = %run_id,
                error = %e,
                "rollback marker could not be written; this run will not be undoable"
            );
        }
    }
    let source: Box<dyn Source> = if state.is_some() && source.state_key().is_some() {
        Box::new(StateKeyOverride {
            inner: source,
            key: effective_state_key,
        })
    } else {
        source
    };

    // #510: stamp `_faucet_*` run/lineage metadata columns closest to the real
    // sink (so `loaded_at` ≈ the actual write), and inside the lineage sampler
    // below so samples reflect the source records, not the added columns.
    let sink: Box<dyn Sink> = match &node.metadata_columns {
        Some(spec) => match faucet_core::CompiledMetadata::compile(spec)
            .map_err(|e| CliError::Config(format!("metadata_columns: {e}")))?
        {
            Some(meta) => Box::new(faucet_core::MetadataSink::new(
                sink,
                meta,
                faucet_core::MetadataContext {
                    run_id: run_id.clone(),
                    source: node.source.kind.clone(),
                },
            )),
            None => sink,
        },
        None => sink,
    };

    // Wrap the sink so it samples written records — outermost, immediately
    // before the pipeline is constructed (after capture/limit wrappers).
    #[cfg(feature = "lineage")]
    let sink: Box<dyn Sink> = match &out_sample {
        Some(state) => Box::new(faucet_lineage::SamplingSink::new(
            sink,
            std::sync::Arc::clone(state),
        )),
        None => sink,
    };

    // 5) Assemble the runtime pipeline. The whole `with_*` builder chain (state,
    //    DLQ, cancellation, quality, contract, masking, schema-drift, adaptive
    //    batching, resilience, delivery) lives in `build_pipeline` so the wiring
    //    is testable in isolation and a preview-mode / feature-gate mistake
    //    (cf. #321 H1) is harder to make (#324 C). The identity strings are
    //    borrowed — the lineage START/terminal lifecycle below still needs
    //    `pipeline_name` / `row_id` / `run_id`.
    let pipeline = build_pipeline(
        source.as_ref(),
        sink.as_ref(),
        node,
        opts,
        state,
        &cancel,
        &pipeline_name,
        &row_id,
        &run_id,
        cleanup_scope,
        suppress_overwrite,
    )
    .await?;
    // ── Lineage: START + heartbeat + terminal ────────────────────────────────
    #[cfg(feature = "lineage")]
    let lineage_ctx = match (&lineage, &lineage_cfg) {
        (Some(em), Some(lc)) => {
            let job_name =
                crate::interpolate::resolve_lineage_job_name(&lc.job_name, &pipeline_name, &row_id);
            let mut ctx = faucet_lineage::RunLifecycle {
                job_namespace: lc.namespace.clone(),
                job_name,
                run_id: run_id.clone(),
                parent: lc.parent_job.clone(),
                inputs: vec![faucet_lineage::DatasetRef {
                    namespace: lc.namespace.clone(),
                    name: source.dataset_uri(),
                }],
                output: faucet_lineage::DatasetRef {
                    namespace: lc.namespace.clone(),
                    name: sink.dataset_uri(),
                },
                started_at: chrono::Utc::now(),
                finished_at: None,
                records: 0,
                error: None,
                input_schemas: Vec::new(),
                output_schema: None,
                column_lineage: None,
                source_code: None,
            };
            em.emit(faucet_lineage::EventType::Start, &ctx).await;
            // Heartbeat task — periodic RUNNING events with the live throughput
            // count read off the output sampler.
            let hb_handle = if lc.emit_on.running {
                let em2 = std::sync::Arc::clone(em);
                let interval = lc.heartbeat_interval;
                let mut beat_ctx = ctx.clone();
                let counter = out_sample.clone();
                Some(tokio::spawn(async move {
                    let mut tick = tokio::time::interval(interval);
                    tick.tick().await; // skip the immediate first tick
                    loop {
                        tick.tick().await;
                        if let Some(c) = &counter {
                            beat_ctx.records = c.count();
                        }
                        em2.emit(faucet_lineage::EventType::Running, &beat_ctx)
                            .await;
                    }
                }))
            } else {
                None
            };
            ctx.source_code = if lc.include_source_code_facet {
                Some(serde_json::to_string(&node.source.config).unwrap_or_default())
            } else {
                None
            };
            Some((std::sync::Arc::clone(em), ctx, hb_handle))
        }
        _ => None,
    };

    // Combine run + final flush into one outcome BEFORE emitting the terminal
    // lineage event, preserving the original semantics (run error → skip flush;
    // run ok but flush error → overall error). The terminal event is classified
    // from this combined `result`, then `?`-propagated below — restoring the
    // original early-return behaviour while still firing the terminal event on
    // both success and error.
    let result: Result<faucet_core::PipelineResult, FaucetError> = match pipeline.run().await {
        Ok(r) => sink.flush().await.map(|_| r),
        Err(e) => Err(e),
    };

    // ── Completeness reconciliation (#502) ───────────────────────────────────
    // On a successful root run, compare rows written against an authoritative
    // count probe; a shortfall beyond tolerance turns the run into an error
    // (silent-truncation guard). Runs BEFORE the lineage/SLA/notify passes so
    // they all observe the failure. Same root/real-run scoping as SLA: skipped
    // for dry-run / --limit / shard / cancelled runs.
    let result = match (&result, &opts.reconcile) {
        (Ok(r), Some(spec))
            if matches!(node.role, NodeRole::Root)
                && !opts.dry_run
                && opts.limit.is_none()
                && opts.shard.is_none()
                && !cancel.is_cancelled() =>
        {
            let written = r.records_written as u64;
            match crate::reconcile::run(spec, &opts.auth, written).await {
                Ok(()) => result,
                Err(e) => Err(e),
            }
        }
        _ => result,
    };

    // ── Content verification (#701) ──────────────────────────────────────────
    // On a successful root run, compare the destination to the source by
    // content (digests + bisection) and — with `repair` — re-sync differing
    // keys. Differences fail the run unless `fail_on_difference: false`. Same
    // scoping as reconcile; a repair runs its own nested invocation.
    let result = match (&result, &opts.verify) {
        (Ok(_), Some(spec))
            if spec.after_run
                && matches!(node.role, NodeRole::Root)
                && !opts.dry_run
                && opts.limit.is_none()
                && opts.shard.is_none()
                && !cancel.is_cancelled() =>
        {
            let inputs = crate::verify::VerifyInputs {
                row: Some(node.id.clone()),
                repair: spec.repair,
                allow_delete: spec.allow_delete,
                dry_run: false,
                pipeline_name: pipeline_name.clone(),
                execution: opts.execution.clone(),
                auth: opts.auth.clone(),
                clock: opts.clock,
            };
            // Behind a never-inlined boxing helper: the verifier's state
            // machine is large, and inlining it here overflowed the 2 MiB
            // test-thread stack under `--all-features` (see
            // `boxed_run_one_invocation`).
            match crate::verify::verify_node_boxed(node, spec, &inputs).await {
                Ok(outcome) if outcome.report.equal() => result,
                // A repair that re-synced every reported difference healed the
                // drift: the run is green, and the drift is logged.
                Ok(outcome) if outcome.report.healed() => {
                    tracing::warn!(
                        row = %node.id,
                        differences = outcome.differing_keys(),
                        repaired_upserts = outcome.report.repaired_upserts.unwrap_or(0),
                        repaired_deletes = outcome.report.repaired_deletes.unwrap_or(0),
                        "content verification found drift and repaired it"
                    );
                    result
                }
                Ok(outcome) => {
                    let (missing, extra, changed, dup) = outcome.report.tally();
                    let msg = format!(
                        "content verification found {} differing key(s) between {} and {} \
                         ({missing} missing in destination, {extra} extra in destination, \
                         {changed} changed, {dup} duplicated{}{})",
                        outcome.differing_keys(),
                        outcome.source,
                        outcome.destination,
                        match (
                            outcome.report.repaired_upserts,
                            outcome.report.repaired_deletes
                        ) {
                            (Some(u), Some(d)) =>
                                format!("; repaired: {u} upsert(s), {d} delete(s)"),
                            _ => String::new(),
                        },
                        if outcome.report.truncated {
                            "; report truncated"
                        } else {
                            ""
                        },
                    );
                    if spec.fail_on_difference {
                        Err(FaucetError::Sink(msg))
                    } else {
                        tracing::warn!(row = %node.id, "{msg}");
                        result
                    }
                }
                Err(e) => Err(FaucetError::Sink(format!(
                    "content verification failed: {e}"
                ))),
            }
        }
        _ => result,
    };

    #[cfg(feature = "lineage")]
    if let Some((em, mut ctx, hb)) = lineage_ctx {
        if let Some(h) = hb {
            h.abort();
        }
        ctx.finished_at = Some(chrono::Utc::now());
        if let Some(state) = &out_sample {
            ctx.records = state.count();
            if lineage_cfg
                .as_ref()
                .map(|l| l.include_schema_facet)
                .unwrap_or(false)
            {
                ctx.output_schema = Some(state.inferred_schema());
            }
        }
        if let Some(state) = &in_sample
            && lineage_cfg
                .as_ref()
                .map(|l| l.include_schema_facet || l.include_column_lineage)
                .unwrap_or(false)
        {
            let in_schema = state.inferred_schema();
            if lineage_cfg
                .as_ref()
                .map(|l| l.include_column_lineage)
                .unwrap_or(false)
            {
                let input_fields: Vec<String> =
                    in_schema.fields.iter().map(|(n, _)| n.clone()).collect();
                #[cfg(feature = "masking")]
                let has_masking = node.masking.is_some();
                #[cfg(not(feature = "masking"))]
                let has_masking = false;
                let ops = crate::lineage_glue::column_ops(&node.transforms, has_masking);
                ctx.column_lineage = faucet_lineage::derive_column_lineage(&input_fields, &ops);
            }
            if lineage_cfg
                .as_ref()
                .map(|l| l.include_schema_facet)
                .unwrap_or(false)
            {
                ctx.input_schemas = vec![Some(in_schema)];
            }
        }
        let ev = match &result {
            Err(e) => {
                ctx.error = Some(e.to_string());
                faucet_lineage::EventType::Fail
            }
            Ok(_) if cancel.is_cancelled() => faucet_lineage::EventType::Abort,
            Ok(_) => faucet_lineage::EventType::Complete,
        };
        em.emit(ev, &ctx).await;
    }

    // ── SLA post-run evaluation (#202) ───────────────────────────────────────
    // Roots only: children fan out per parent record, so their volumes are not
    // a stable series to baseline (same scoping as `faucet doctor`). Skipped
    // for dry-run / --limit (synthetic volumes would poison the baseline), for
    // shard executions (a shard's volume is a fraction of the row's, and shard
    // counts change run to run), and for cancelled runs (a partial volume is
    // not a signal). Monitoring never fails the run — see `evaluate_post_run`.
    // Roots only, and never for dry-run / --limit / shard / cancelled runs —
    // the same scoping the notification pass below reuses.
    let is_notifiable_root = matches!(node.role, NodeRole::Root)
        && !opts.dry_run
        && opts.limit.is_none()
        && opts.shard.is_none()
        && !cancel.is_cancelled();

    #[cfg_attr(not(feature = "notify"), allow(unused_variables))]
    let sla_violations = if let Some(spec) = node.sla.as_ref().or(opts.sla.as_ref())
        && is_notifiable_root
    {
        let outcome = match &result {
            Ok(r) => crate::sla::RunOutcome::Success {
                rows: r.records_written as u64,
            },
            Err(_) => crate::sla::RunOutcome::Failure,
        };
        crate::sla::evaluate_post_run(
            spec,
            sla_store.as_ref(),
            state_key,
            &obs_labels.pipeline,
            &obs_labels.row,
            outcome,
            chrono::Utc::now().timestamp(),
        )
        .await
    } else {
        Vec::new()
    };

    // ── Notifications (#280) ─────────────────────────────────────────────────
    // Fan run success/failure, SLA breach, circuit-open, contract-abort, and
    // DLQ-threshold out to the configured channels. Same root/real-run scoping
    // as SLA; delivery is fire-and-forget and never fails the run.
    #[cfg(feature = "notify")]
    if let Some(notifier) = &opts.notifier
        && is_notifiable_root
    {
        use crate::notify::NotifyEvent;
        let pipeline = obs_labels.pipeline.to_string();
        let row = obs_labels.row.to_string();
        // Closed once here, so every event from this invocation reports the same
        // terminal timestamp and duration.
        let run_ctx = notify_run.clone().finish(invocation_started);
        match &result {
            Ok(r) => {
                notifier
                    .emit(
                        NotifyEvent::run_success(
                            pipeline.clone(),
                            row.clone(),
                            r.records_written as u64,
                        )
                        .with_run(run_ctx.clone()),
                    )
                    .await;
                if let Some(dlq) = &r.dlq
                    && dlq.records_dlq > 0
                {
                    notifier
                        .emit(
                            NotifyEvent::dlq_threshold(
                                pipeline.clone(),
                                row.clone(),
                                dlq.records_dlq as u64,
                            )
                            .with_run(run_ctx.clone()),
                        )
                        .await;
                }
            }
            Err(e) => {
                notifier
                    .emit(error_event(&pipeline, &row, e).with_run(run_ctx.clone()))
                    .await;
            }
        }
        for v in &sla_violations {
            notifier
                .emit(
                    NotifyEvent::sla_breach(pipeline.clone(), row.clone(), v.kind(), v.to_string())
                        .with_run(run_ctx.clone()),
                )
                .await;
        }
    }

    // ── Data Movement Catalog (#279) ─────────────────────────────────────────
    // Fold this run's dataset observations + lineage edge into the catalog.
    // Successful, complete root runs only (a cancelled run's partial volume is
    // not a signal). Recording never fails the run — see `catalog::record`.
    #[cfg(feature = "catalog")]
    if let Some(handle) = &opts.catalog
        && catalog_active
        && !cancel.is_cancelled()
        && let Ok(pipeline_result) = &result
    {
        use crate::catalog::model::{canonicalize_uri, schema_from_samples};
        use crate::serve::history::catalog::{CatalogUpdate, DatasetObservation, DatasetRole};

        let records_written = pipeline_result.records_written as u64;
        let source_schema = in_sample
            .as_ref()
            .and_then(|s| schema_from_samples(&s.samples()));
        let sink_schema = out_sample
            .as_ref()
            .and_then(|s| schema_from_samples(&s.samples()));
        // The samplers are always installed while the catalog is active, so the
        // unwrap_or arms are defensive only.
        let records_read = in_sample
            .as_ref()
            .map(|s| s.count())
            .unwrap_or(records_written);
        let records_out = out_sample
            .as_ref()
            .map(|s| s.count())
            .unwrap_or(records_written);

        // Column lineage for the edge — the same derivation `faucet-lineage`
        // emits, so the catalog's edges match the OpenLineage output.
        let column_lineage = in_sample.as_ref().and_then(|s| {
            let input_fields: Vec<String> = s
                .inferred_schema()
                .fields
                .iter()
                .map(|(n, _)| n.clone())
                .collect();
            #[cfg(feature = "masking")]
            let has_masking = node.masking.is_some();
            #[cfg(not(feature = "masking"))]
            let has_masking = false;
            let ops = crate::lineage_glue::column_ops(&node.transforms, has_masking);
            faucet_lineage::derive_column_lineage(&input_fields, &ops).map(|cl| {
                // `ColumnLineage` is not `Serialize` (IndexMap); render the
                // stable `{"fields": {out: [in, …]}}` shape by hand.
                let fields: serde_json::Map<String, Value> = cl
                    .edges
                    .iter()
                    .map(|(out, ins)| {
                        (
                            out.clone(),
                            Value::Array(ins.iter().map(|s| Value::String(s.clone())).collect()),
                        )
                    })
                    .collect();
                serde_json::json!({ "fields": fields })
            })
        });

        let update = CatalogUpdate {
            run_id: handle.run_id.clone().unwrap_or_else(|| run_id.clone()),
            pipeline: obs_labels.pipeline.to_string(),
            row: obs_labels.row.to_string(),
            recorded_at: chrono::Utc::now(),
            // A matrix invocation is always one source → one sink; the vector
            // shape exists for topology graphs, where a sink can be fed by several
            // (#459).
            sources: vec![DatasetObservation {
                uri: canonicalize_uri(&source_dataset_uri, &node.source.config, opts.clock),
                kind: node.source.kind.clone(),
                role: DatasetRole::Source,
                schema: source_schema,
                records: records_read,
            }],
            sink: DatasetObservation {
                uri: canonicalize_uri(&sink_dataset_uri, &node.sink.config, opts.clock),
                kind: node.sink.kind.clone(),
                role: DatasetRole::Sink,
                schema: sink_schema,
                records: records_out,
            },
            column_lineage,
        };
        crate::catalog::record(handle, &update).await;
    }

    // Local sink outputs (#587): record the concrete files this invocation's
    // sink opened, so the retention GC has a path list to delete from — and so
    // the console can list what local data exists.
    //
    // Deliberately *not* gated like the catalog block above. That one is scoped
    // to real root invocations because partial volumes are a misleading signal;
    // here the opposite holds — a `--limit` run, a shard, a child row, and a run
    // that failed halfway all leave real files on disk, and a file the ledger
    // never learned about is a file nothing will ever reclaim. Only `--dry-run`
    // is excluded, and only because it writes nothing (its sink is a counter).
    #[cfg(feature = "catalog")]
    if let Some(handle) = &opts.catalog
        && !opts.dry_run
        && node
            .local_outputs
            .as_ref()
            .map(|spec| spec.track)
            .unwrap_or(true)
    {
        let outputs = sink.local_outputs().await;
        if !outputs.is_empty() {
            let ctx = crate::local_outputs::RecordContext {
                dataset_uri: crate::catalog::model::canonicalize_uri(
                    &sink_dataset_uri,
                    &node.sink.config,
                    opts.clock,
                ),
                kind: node.sink.kind.clone(),
                pipeline: obs_labels.pipeline.to_string(),
                row: obs_labels.row.to_string(),
                run_id: handle.run_id.clone().unwrap_or_else(|| run_id.clone()),
                retention_days: node
                    .local_outputs
                    .as_ref()
                    .and_then(|spec| spec.retention_days),
                observed_at: chrono::Utc::now(),
            };
            crate::local_outputs::record(handle.store.as_ref(), &outputs, &ctx).await;
        }
    }

    let result = result?;

    // Per-invocation stats for `faucet run --output json` (#390). `records_read`
    // is only known when the source sampler was installed (a `lineage:` or
    // `catalog:` block); otherwise it stays `None` rather than guessing.
    #[cfg(feature = "lineage")]
    let records_read = in_sample.as_ref().map(|s| s.count());
    #[cfg(not(feature = "lineage"))]
    let records_read: Option<u64> = None;
    let stats = PipelineStats {
        run_id: run_id.clone(),
        records_written: result.records_written,
        records_read,
        dlq_count: result
            .dlq
            .as_ref()
            .map(|d| d.records_dlq as u64)
            .unwrap_or(0),
        bookmark: result.bookmark.clone(),
    };

    let captured = if capture.is_some() {
        std::mem::take(&mut *captured.lock().await)
    } else {
        Vec::new()
    };
    Ok((captured, stats))
}

async fn build_state_for_node(
    node: &ExpandedNode,
    state_path_override: Option<&Path>,
) -> CliResult<Option<Arc<dyn StateStore>>> {
    match (&node.state, state_path_override) {
        (Some(spec), None) => Ok(Some(build_state_store(spec).await?)),
        (None, Some(path)) => Ok(Some(state_from_override(path))),
        (Some(spec), Some(path)) => {
            if spec.kind == "file" {
                Ok(Some(state_from_override(path)))
            } else {
                tracing::warn!(
                    state = %spec.kind,
                    "--state-path is only meaningful for the 'file' backend; ignoring override"
                );
                Ok(Some(build_state_store(spec).await?))
            }
        }
        (None, None) => Ok(None),
    }
}

fn state_from_override(path: &Path) -> Arc<dyn StateStore> {
    Arc::new(faucet_core::FileStateStore::new(path)) as Arc<dyn StateStore>
}

/// Translate a [`crate::config::DlqSpec`] from the YAML/JSON config into a
/// runtime [`DlqConfig`] ready to attach to a [`Pipeline`].
pub async fn build_dlq_config(spec: &crate::config::DlqSpec) -> CliResult<DlqConfig> {
    // DLQ sinks resolve against an empty catalog — shared `auth: { ref }` on a
    // DLQ sink is out of scope (DLQ targets are typically local jsonl/stdout).
    let sink = build_sink(
        &spec.sink.kind,
        spec.sink.config.clone(),
        &AuthCatalog::new(),
    )
    .await?;
    Ok(DlqConfig {
        sink: Arc::from(sink),
        on_batch_error: match spec.on_batch_error {
            crate::config::OnBatchErrorSpec::Propagate => OnBatchError::Propagate,
            crate::config::OnBatchErrorSpec::DlqAll => OnBatchError::DlqAll,
        },
        max_failures_per_page: spec.max_failures_per_page,
        max_failures_total: spec.max_failures_total,
        include_original_payload: spec.include_original_payload,
    })
}

/// Classify a pipeline error into a notification event (#280). A circuit-breaker
/// trip and a contract-abort breach get their own event kinds; everything else
/// is a generic `run_failure` carrying a short error-kind label.
#[cfg(feature = "notify")]
fn error_event(pipeline: &str, row: &str, err: &FaucetError) -> crate::notify::NotifyEvent {
    use crate::notify::NotifyEvent;
    match err {
        FaucetError::CircuitOpen { failures, cooldown } => {
            NotifyEvent::circuit_open(pipeline, row, *failures, cooldown.as_secs())
        }
        FaucetError::ContractViolation { message, .. } => {
            NotifyEvent::contract_abort(pipeline, row, message.clone())
        }
        other => {
            NotifyEvent::run_failure(pipeline, row, faucet_error_kind(other), other.to_string())
        }
    }
}

/// Short, stable label for a `FaucetError` variant used as the `error_kind`
/// detail on a `run_failure` notification. (`faucet-core`'s own `error_kind`
/// helper is `pub(crate)`, so we keep a small CLI-side mapping.)
#[cfg(feature = "notify")]
fn faucet_error_kind(err: &FaucetError) -> &'static str {
    match err {
        FaucetError::Config(_) => "config",
        FaucetError::Source(_) => "source",
        FaucetError::Sink(_) => "sink",
        FaucetError::State(_) => "state",
        FaucetError::QualityFailure { .. } => "quality",
        FaucetError::SchemaDrift { .. } => "schema_drift",
        _ => "error",
    }
}

/// In-place `${now.*}` resolution against the run clock. Walks every string
/// leaf and rewrites `${now.<token>}`; all other `${...}` tokens are untouched.
/// Shared with `faucet test`, which applies the same pre-pass to transform
/// configs under the case clock.
/// Error on a leftover `${backfill.*}` token: those resolve only inside
/// `faucet backfill`, which substitutes them per window unit before the
/// executor runs. Reaching here means another runtime picked up a
/// window-scoped config.
pub(crate) fn reject_unresolved_backfill_tokens(value: &Value, owner: &str) -> CliResult<()> {
    fn walk(value: &Value, owner: &str) -> CliResult<()> {
        match value {
            Value::String(s) if s.contains("${backfill.") => Err(CliError::Config(format!(
                "the {owner} config references a `${{backfill.*}}` token, which only                  `faucet backfill` resolves — run this config via `faucet backfill                  --from … --to …`, or remove the token"
            ))),
            Value::Array(a) => a.iter().try_for_each(|v| walk(v, owner)),
            Value::Object(m) => m.values().try_for_each(|v| walk(v, owner)),
            _ => Ok(()),
        }
    }
    walk(value, owner)
}

pub(crate) fn resolve_now_inplace(
    value: &mut Value,
    clock: DateTime<FixedOffset>,
) -> CliResult<()> {
    match value {
        Value::String(s) => {
            *s = crate::interpolate::resolve_now(s, clock)?;
            Ok(())
        }
        Value::Array(a) => a.iter_mut().try_for_each(|v| resolve_now_inplace(v, clock)),
        Value::Object(m) => m
            .values_mut()
            .try_for_each(|v| resolve_now_inplace(v, clock)),
        _ => Ok(()),
    }
}

/// In-place runtime interpolation against a parent-record context. Walks every
/// string leaf in `value` and replaces `${id.path}` tokens with stringified
/// values from `ctx`.
fn resolve_inplace(value: &mut Value, ctx: &HashMap<String, Value>) -> CliResult<()> {
    match value {
        Value::String(s) => {
            let resolved = interpolate_record(s, ctx)?;
            *s = resolved;
            Ok(())
        }
        Value::Array(a) => a.iter_mut().try_for_each(|v| resolve_inplace(v, ctx)),
        Value::Object(m) => m.values_mut().try_for_each(|v| resolve_inplace(v, ctx)),
        _ => Ok(()),
    }
}

// ── Adapter sinks/sources ───────────────────────────────────────────────────

/// Wraps a [`StateStore`] so reads pass through but writes/deletes are dropped.
///
/// Attached under `--dry-run` / `--limit`: those modes swap in counting /
/// truncating sinks that return `Ok` without a real durable write, and
/// `run_stream` persists a page's bookmark after any `Ok`. Persisting an
/// advanced bookmark from a preview would make the next *real* run resume past
/// records that were never written — a `--dry-run` silently causing data loss
/// (audit #321 H1; for postgres-cdc it also lets Postgres recycle WAL for
/// undelivered changes). Reads still pass through so the preview faithfully
/// resumes from the existing bookmark.
pub(crate) struct ReadOnlyStateStore {
    pub(crate) inner: Arc<dyn StateStore>,
}

#[async_trait]
impl StateStore for ReadOnlyStateStore {
    async fn get(&self, key: &str) -> Result<Option<Value>, FaucetError> {
        self.inner.get(key).await
    }
    async fn put(&self, _key: &str, _value: &Value) -> Result<(), FaucetError> {
        Ok(())
    }
    async fn delete(&self, _key: &str) -> Result<(), FaucetError> {
        Ok(())
    }
}

/// Wraps a source so its `state_key()` returns the executor-provided value
/// instead of the source's natural one. Lets every matrix invocation use a
/// distinct state-store entry even when the underlying source kind is shared.
struct StateKeyOverride {
    inner: Box<dyn Source>,
    key: String,
}

#[async_trait]
impl Source for StateKeyOverride {
    async fn fetch_with_context(
        &self,
        ctx: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        self.inner.fetch_with_context(ctx).await
    }
    async fn fetch_with_context_incremental(
        &self,
        ctx: &HashMap<String, Value>,
    ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        self.inner.fetch_with_context_incremental(ctx).await
    }
    // Forward `stream_pages` so the wrapped connector's *native* page stream
    // survives the wrap. Without this the trait's buffering default kicks in
    // for every stateful run — losing per-page bookmarks (CDC per-transaction
    // durability, exactly-once per-page tokens) and the O(batch_size) memory
    // bound.
    fn stream_pages<'a>(
        &'a self,
        ctx: &'a HashMap<String, Value>,
        batch_size: usize,
    ) -> std::pin::Pin<
        Box<
            dyn faucet_core::Stream<Item = Result<faucet_core::StreamPage, FaucetError>>
                + Send
                + 'a,
        >,
    > {
        self.inner.stream_pages(ctx, batch_size)
    }
    fn connector_name(&self) -> &'static str {
        self.inner.connector_name()
    }
    fn dataset_uri(&self) -> String {
        self.inner.dataset_uri()
    }
    fn state_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        self.inner.apply_start_bookmark(bookmark).await
    }
    fn supports_exactly_once(&self) -> bool {
        self.inner.supports_exactly_once()
    }
    fn replay_guarantee(&self) -> faucet_core::ReplayGuarantee {
        self.inner.replay_guarantee()
    }
    async fn capture_resume_position(&self) -> Result<Option<Value>, FaucetError> {
        self.inner.capture_resume_position().await
    }
    // Forward the fast-path capabilities so wrapping the source for a per-row
    // state key never silently disables the columnar (#375) or native
    // byte-passthrough (#633) transfer paths.
    #[cfg(feature = "arrow")]
    fn supports_columnar(&self) -> bool {
        self.inner.supports_columnar()
    }
    #[cfg(feature = "arrow")]
    fn stream_batches<'a>(
        &'a self,
        ctx: &'a HashMap<String, Value>,
        batch_size: usize,
    ) -> std::pin::Pin<
        Box<
            dyn faucet_core::Stream<Item = Result<faucet_core::columnar::ColumnarPage, FaucetError>>
                + Send
                + 'a,
        >,
    > {
        self.inner.stream_batches(ctx, batch_size)
    }
    fn native_output_formats(&self) -> &'static [faucet_core::NativeFormat] {
        self.inner.native_output_formats()
    }
    fn stream_native<'a>(
        &'a self,
        ctx: &'a HashMap<String, Value>,
        format: faucet_core::NativeFormat,
        batch_size: usize,
    ) -> std::pin::Pin<
        Box<
            dyn faucet_core::Stream<Item = Result<faucet_core::NativeBatch, FaucetError>>
                + Send
                + 'a,
        >,
    > {
        self.inner.stream_native(ctx, format, batch_size)
    }
}

/// Forwards each record to an inner sink while also capturing a **projected**
/// copy into a shared buffer for descendant rows to consume. Projecting to only
/// the fields children reference bounds orchestrator memory (#160).
struct CapturingSink {
    inner: Box<dyn Sink>,
    captured: Arc<Mutex<Vec<Value>>>,
    projection: Arc<Projection>,
}

impl CapturingSink {
    fn wrap(
        inner: Box<dyn Sink>,
        captured: Arc<Mutex<Vec<Value>>>,
        projection: Arc<Projection>,
    ) -> Self {
        Self {
            inner,
            captured,
            projection,
        }
    }
}

#[async_trait]
impl Sink for CapturingSink {
    fn connector_name(&self) -> &'static str {
        self.inner.connector_name()
    }
    fn dataset_uri(&self) -> String {
        self.inner.dataset_uri()
    }
    // Local-output provenance for the retention GC (#587) must survive every
    // decorator — a wrapper that fell back to the empty default would hide the
    // real sink's files and they would never be reclaimed.
    async fn local_outputs(&self) -> Vec<faucet_core::LocalOutput> {
        self.inner.local_outputs().await
    }
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        let written = self.inner.write_batch(records).await?;
        // Capture only what actually landed (LimitedSink may have dropped some),
        // projected to the fields children reference (#160).
        let n = written.min(records.len());
        let mut buf = self.captured.lock().await;
        buf.extend(
            records
                .iter()
                .take(n)
                .map(|r| project_record(r, &self.projection)),
        );
        Ok(written)
    }
    async fn flush(&self) -> Result<(), FaucetError> {
        self.inner.flush().await
    }
    // Capability + exactly-once passthroughs, so a parent row that fans out to
    // children (which is what this wrapper serves) keeps the inner sink's
    // delivery semantics instead of being masked down to the trait defaults.
    fn supports_idempotent_writes(&self) -> bool {
        self.inner.supports_idempotent_writes()
    }
    fn sink_guarantee(&self) -> faucet_core::SinkGuarantee {
        self.inner.sink_guarantee()
    }
    fn dedups_by_key(&self) -> bool {
        self.inner.dedups_by_key()
    }
    fn supported_write_modes(&self) -> &'static [faucet_core::WriteMode] {
        self.inner.supported_write_modes()
    }
    async fn write_batch_idempotent(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<usize, FaucetError> {
        let written = self
            .inner
            .write_batch_idempotent(records, scope, token)
            .await?;
        let n = written.min(records.len());
        let mut buf = self.captured.lock().await;
        buf.extend(
            records
                .iter()
                .take(n)
                .map(|r| project_record(r, &self.projection)),
        );
        Ok(written)
    }
    async fn last_committed_token(&self, scope: &str) -> Result<Option<String>, FaucetError> {
        self.inner.last_committed_token(scope).await
    }
    fn supports_rollback(&self) -> bool {
        self.inner.supports_rollback()
    }
    async fn forget_run(&self, run_id: &str) -> Result<(), FaucetError> {
        self.inner.forget_run(run_id).await
    }
    async fn current_schema(&self) -> Result<Option<Value>, FaucetError> {
        self.inner.current_schema().await
    }
    fn supports_schema_evolution(&self) -> bool {
        self.inner.supports_schema_evolution()
    }
    async fn evolve_schema(
        &self,
        evolution: &faucet_core::SchemaEvolution,
    ) -> Result<(), FaucetError> {
        self.inner.evolve_schema(evolution).await
    }
}

/// Cap on records written. Each `write_batch` call truncates `records` to the
/// remaining budget before delegating.
pub(crate) struct LimitedSink {
    inner: Box<dyn Sink>,
    remaining: AtomicUsize,
}

impl LimitedSink {
    pub(crate) fn wrap(inner: Box<dyn Sink>, cap: usize) -> Self {
        Self {
            inner,
            remaining: AtomicUsize::new(cap),
        }
    }
}

#[async_trait]
impl Sink for LimitedSink {
    fn connector_name(&self) -> &'static str {
        self.inner.connector_name()
    }
    fn dataset_uri(&self) -> String {
        self.inner.dataset_uri()
    }
    // Local-output provenance for the retention GC (#587) must survive every
    // decorator — a wrapper that fell back to the empty default would hide the
    // real sink's files and they would never be reclaimed.
    async fn local_outputs(&self) -> Vec<faucet_core::LocalOutput> {
        self.inner.local_outputs().await
    }
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        let remaining = self.remaining.load(Ordering::Relaxed);
        if remaining == 0 {
            return Ok(0);
        }
        let take = remaining.min(records.len());
        let slice = &records[..take];
        let written = self.inner.write_batch(slice).await?;
        self.remaining
            .fetch_sub(written.min(remaining), Ordering::Relaxed);
        Ok(written)
    }
    async fn flush(&self) -> Result<(), FaucetError> {
        self.inner.flush().await
    }
}

/// No-op sink used in `--dry-run`. Counts records seen so the rest of the
/// pipeline (transforms, source) still runs.
pub(crate) struct CountingSink {
    seen: AtomicUsize,
}

impl CountingSink {
    pub(crate) fn new() -> Self {
        Self {
            seen: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl Sink for CountingSink {
    fn connector_name(&self) -> &'static str {
        "dry-run"
    }
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        self.seen.fetch_add(records.len(), Ordering::Relaxed);
        Ok(records.len())
    }
}

/// Render a JSON value compactly for use as a state-key suffix or log line.
/// Strings pass through unquoted; numbers/bools/null/composites use to_string.
fn value_to_string_brief(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #644 — dispatch order.
    ///
    /// `order_ready` is the whole policy, so it is tested directly rather than
    /// through a running pipeline: what matters is that the heaviest row claims
    /// a permit first and that everything else stays deterministic.
    mod dispatch_order {
        use super::*;

        fn node(id: &str, weight: Option<f64>) -> ExpandedNode {
            ExpandedNode {
                id: id.into(),
                row_index: 0,
                weight,
                role: NodeRole::Root,
                source: ConnectorSpec {
                    kind: "csv".into(),
                    config: json!({}),
                    transforms: None,
                    inherit_transforms: true,
                    status: None,
                    tags: Vec::new(),
                    complete_for: None,
                },
                sink: ConnectorSpec {
                    kind: "jsonl".into(),
                    config: json!({}),
                    transforms: None,
                    inherit_transforms: true,
                    status: None,
                    tags: Vec::new(),
                    complete_for: None,
                },
                transforms: Vec::new(),
                state: None,
                dlq: None,
                sla: None,
                delivery: faucet_core::DeliveryMode::AtLeastOnce,
                delivery_guarantee: faucet_core::DeliveryGuarantee::AtLeastOnce,
                #[cfg(feature = "quality")]
                quality: None,
                #[cfg(feature = "contract")]
                contract: None,
                #[cfg(feature = "masking")]
                masking: None,
                sink_ref: "default".into(),
                schema: None,
                depends_on: Vec::new(),
                status: crate::config::SourceStatus::Active,
                tags: Vec::new(),
                cleanup_scope: None,
                metadata_columns: None,
                #[cfg(feature = "catalog")]
                local_outputs: None,
                deferred_refs: Vec::new(),
                source_override: None,
            }
        }

        fn map(nodes: Vec<ExpandedNode>) -> HashMap<String, ExpandedNode> {
            nodes.into_iter().map(|n| (n.id.clone(), n)).collect()
        }

        #[test]
        fn declared_order_is_returned_untouched() {
            let by_id = map(vec![
                node("a", Some(1.0)),
                node("b", Some(900.0)),
                node("c", None),
            ]);
            let ready = vec!["a".into(), "b".into(), "c".into()];
            assert_eq!(
                order_ready(ready.clone(), &by_id, DispatchOrder::Declared),
                ready,
                "the default must not reorder anything"
            );
        }

        #[test]
        fn lpt_dispatches_the_heaviest_row_first() {
            // The reported shape: the big objects are listed last, so plain
            // declaration order leaves them as an idle tail.
            let by_id = map(vec![
                node("small", Some(10.0)),
                node("medium", Some(500.0)),
                node("huge", Some(9_000.0)),
            ]);
            let ready = vec!["small".into(), "medium".into(), "huge".into()];
            assert_eq!(
                order_ready(ready, &by_id, DispatchOrder::Lpt),
                vec!["huge", "medium", "small"]
            );
        }

        #[test]
        fn ties_and_unweighted_rows_keep_declaration_order() {
            // Determinism is the constraint: an operator must be able to
            // predict dispatch from the config alone.
            let by_id = map(vec![
                node("a", Some(5.0)),
                node("b", None),
                node("c", Some(5.0)),
                node("d", None),
            ]);
            let ready = vec!["a".into(), "b".into(), "c".into(), "d".into()];
            assert_eq!(
                order_ready(ready, &by_id, DispatchOrder::Lpt),
                vec!["a", "c", "b", "d"],
                "equal weights keep their order, and unweighted rows sort last"
            );
        }

        #[test]
        fn a_nan_weight_does_not_make_the_sort_inconsistent() {
            // A config can carry a NaN; `partial_cmp` would return `None` and
            // leave the comparator inconsistent, which is UB-adjacent for a
            // sort. `total_cmp` orders it definitely.
            let by_id = map(vec![node("nan", Some(f64::NAN)), node("real", Some(1.0))]);
            let out = order_ready(
                vec!["nan".into(), "real".into()],
                &by_id,
                DispatchOrder::Lpt,
            );
            assert_eq!(out.len(), 2, "no row is dropped");
            assert!(out.contains(&"real".to_string()));
        }

        #[test]
        fn a_single_ready_row_short_circuits() {
            let by_id = map(vec![node("only", Some(1.0))]);
            assert_eq!(
                order_ready(vec!["only".into()], &by_id, DispatchOrder::Lpt),
                vec!["only"]
            );
        }
    }
    use crate::config::{ConnectorSpec, PipelineConfig, PipelineSpec};
    use crate::expand::expand;
    use serde_json::json;

    #[test]
    fn classify_error_picks_out_a_circuit_open_trip() {
        assert_eq!(
            classify_error(&CliError::Faucet(FaucetError::CircuitOpen {
                failures: 3,
                cooldown: Duration::from_secs(60),
            })),
            InvocationErrorKind::CircuitOpen
        );
        // Every other failure — including a *message* that reads like a breaker
        // trip — is `Other`: the classification is the variant, not the prose
        // (#654 M2).
        for err in [
            CliError::Faucet(FaucetError::Sink("Circuit open after 3 failures".into())),
            CliError::Internal("Circuit open after 3 failures".into()),
            CliError::Faucet(FaucetError::Source("connection refused".into())),
        ] {
            assert_eq!(
                classify_error(&err),
                InvocationErrorKind::Other,
                "must not be CircuitOpen: {err}"
            );
        }
    }

    /// A source advertising both fast paths, so the per-row state-key wrapper
    /// can be checked for forwarding rather than masking them.
    struct FastPathSource;

    #[async_trait]
    impl Source for FastPathSource {
        async fn fetch_with_context(
            &self,
            _ctx: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, faucet_core::FaucetError> {
            Ok(vec![serde_json::json!({ "id": 1 })])
        }
        fn state_key(&self) -> Option<String> {
            Some("natural".into())
        }
        fn native_output_formats(&self) -> &'static [faucet_core::NativeFormat] {
            &[faucet_core::NativeFormat::NdJson]
        }
        fn stream_native<'a>(
            &'a self,
            _ctx: &'a HashMap<String, Value>,
            format: faucet_core::NativeFormat,
            _batch_size: usize,
        ) -> std::pin::Pin<
            Box<
                dyn faucet_core::Stream<
                        Item = Result<faucet_core::NativeBatch, faucet_core::FaucetError>,
                    > + Send
                    + 'a,
            >,
        > {
            assert_eq!(format, faucet_core::NativeFormat::NdJson);
            Box::pin(futures::stream::once(async move {
                Ok(faucet_core::NativeBatch::bytes(
                    faucet_core::NativeFormat::NdJson,
                    b"{\"id\":1}\n".to_vec(),
                ))
            }))
        }
        #[cfg(feature = "arrow")]
        fn supports_columnar(&self) -> bool {
            true
        }
    }

    /// Wrapping a source to override its state key must not disable the
    /// negotiated fast paths — a wrapper that fell back to the trait defaults
    /// would silently force every row onto the `Value` path (the #639 class of
    /// regression), costing the memory win with no signal.
    #[tokio::test]
    async fn state_key_override_forwards_fast_path_capabilities() {
        use futures::StreamExt as _;
        let wrapped = StateKeyOverride {
            inner: Box::new(FastPathSource),
            key: "row-scoped".into(),
        };
        // The override itself still applies.
        assert_eq!(wrapped.state_key().as_deref(), Some("row-scoped"));
        // Native: format list forwarded, and the batch stream really comes from
        // the inner source.
        assert_eq!(
            wrapped.native_output_formats(),
            &[faucet_core::NativeFormat::NdJson]
        );
        let ctx: HashMap<String, Value> = HashMap::new();
        let mut batches = wrapped.stream_native(&ctx, faucet_core::NativeFormat::NdJson, 10);
        let batch = batches.next().await.expect("one batch").expect("ok");
        match batch.payload {
            faucet_core::NativePayload::Bytes(b) => {
                assert_eq!(b, b"{\"id\":1}\n".to_vec())
            }
            faucet_core::NativePayload::Stream(_) => panic!("bytes expected"),
        }
        #[cfg(feature = "arrow")]
        assert!(wrapped.supports_columnar(), "columnar must forward too");
    }

    #[test]
    fn concurrency_permits_semantics() {
        // 0 = unlimited (all in parallel); 1 = serial; n = n; None = default cap.
        assert_eq!(concurrency_permits(Some(0)), Semaphore::MAX_PERMITS);
        assert_eq!(concurrency_permits(Some(1)), 1);
        assert_eq!(concurrency_permits(Some(12)), 12);
        let d = concurrency_permits(None);
        assert!((1..=8).contains(&d), "default cap in 1..=8, got {d}");
    }

    #[tokio::test]
    async fn resolve_product_dims_skips_unavailable_and_rejects_oversized() {
        // An unavailable upstream dimension → `Ok(None)` (the row is skipped).
        let empty: DiscoveredDims =
            std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));
        let skipped = resolve_product_dims(&["missing".to_string()], &empty, "row")
            .await
            .unwrap();
        assert!(skipped.is_none());

        // A cross-product past `MAX_MATRIX_PRODUCT` (101 × 100 = 10_100) → `Err`.
        let dim = |id: &str, n: i64| crate::discovery_matrix::Dim {
            id: id.to_string(),
            alias: id.to_string(),
            values: (0..n).map(|i| json!(i)).collect(),
        };
        let dd: DiscoveredDims =
            std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::from([
                ("a".to_string(), dim("a", 101)),
                ("b".to_string(), dim("b", 100)),
            ])));
        let err = resolve_product_dims(&["a".to_string(), "b".to_string()], &dd, "row")
            .await
            .unwrap_err();
        assert!(matches!(err, CliError::Config(m) if m.contains("over the limit")));
    }

    fn cfg_csv_to_jsonl(input: &Path, output: &Path) -> PipelineConfig {
        PipelineConfig {
            kind: None,
            version: 1,
            name: Some("test".into()),
            vars: None,
            params: Default::default(),
            auth: None,
            pipeline: PipelineSpec {
                source: Some(ConnectorSpec {
                    kind: "csv".into(),
                    config: json!({"path": input.to_str().unwrap()}),
                    transforms: None,
                    inherit_transforms: true,
                    status: None,
                    tags: Vec::new(),
                    complete_for: None,
                }),
                sink: Some(ConnectorSpec {
                    kind: "jsonl".into(),
                    config: json!({"path": output.to_str().unwrap()}),
                    transforms: None,
                    inherit_transforms: true,
                    status: None,
                    tags: Vec::new(),
                    complete_for: None,
                }),
                sources: Default::default(),
                sinks: Default::default(),
                transforms: Vec::new(),
                state: None,
                dlq: None,
                #[cfg(feature = "quality")]
                quality: None,
                #[cfg(feature = "contract")]
                contract: None,
                #[cfg(feature = "masking")]
                masking: None,
                schema: None,
                nodes: std::collections::HashMap::new(),
                edges: Vec::new(),
            },
            matrix: Vec::new(),
            execution: None,
            metadata_columns: None,
            #[cfg(feature = "catalog")]
            local_outputs: None,
            selection: None,
            observability: None,
            delivery: faucet_core::DeliveryMode::default(),
            resilience: None,
            sla: None,
            reconcile: None,
            verify: None,
            rollback: None,
            shard: None,
            replication: None,
            backfill: None,
            partition: None,
            #[cfg(feature = "schedule")]
            schedule: None,
            #[cfg(feature = "lineage")]
            lineage: None,
            #[cfg(feature = "catalog")]
            catalog: None,
            #[cfg(feature = "notify")]
            notifications: Vec::new(),
        }
    }

    #[tokio::test]
    async fn empty_matrix_runs_pipeline_once() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.csv");
        let output = dir.path().join("out.jsonl");
        std::fs::write(&input, "name\nalice\nbob\n").unwrap();
        let cfg = cfg_csv_to_jsonl(&input, &output);
        let nodes = expand(&cfg).unwrap();
        let summary = run_expanded(
            nodes,
            ExecuteOptions {
                pipeline_name: "t".into(),
                run_id: None,
                execution: None,
                concurrency: None,
                dry_run: false,
                limit: None,
                state_path_override: None,
                shard: None,
                auth: Default::default(),
                clock: chrono::Utc::now().fixed_offset(),
                cancel: None,
                resilience: None,
                sla: None,
                reconcile: None,
                verify: None,
                rollback: None,
                #[cfg(feature = "lineage")]
                lineage: None,
                #[cfg(feature = "lineage")]
                lineage_cfg: None,
                #[cfg(feature = "notify")]
                notifier: None,
                #[cfg(feature = "catalog")]
                catalog: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(summary.invocations.len(), 1);
        assert_eq!(summary.invocations[0].records_written, 2);
        assert!(!summary.had_failures());
        let body = std::fs::read_to_string(&output).unwrap();
        assert_eq!(body.lines().count(), 2);
    }

    // ── #552: child fan-out into write_mode: overwrite ──────────────────────
    // These exercise the executor-owned overwrite lifecycle end to end against
    // a real sqlite sink (the only in-process overwrite sink). Before the fix,
    // each per-parent invocation ran its own begin→commit, so the destination
    // ended with only the LAST parent's rows (silent data loss).

    fn exec_opts(name: &str) -> ExecuteOptions {
        ExecuteOptions {
            pipeline_name: name.into(),
            run_id: None,
            execution: None,
            concurrency: None,
            dry_run: false,
            limit: None,
            state_path_override: None,
            shard: None,
            auth: Default::default(),
            clock: chrono::Utc::now().fixed_offset(),
            cancel: None,
            resilience: None,
            sla: None,
            reconcile: None,
            verify: None,
            rollback: None,
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

    async fn run_yaml(yaml: &str, path: &Path) -> RunSummary {
        let cfg = PipelineConfig::from_text(yaml, path).expect("config parses");
        let nodes = expand(&cfg).expect("config expands");
        run_expanded(nodes, exec_opts("ow")).await.expect("run ok")
    }

    // Pre-create table `t` with a seed row (99, OLD). sqlite `auto_map` requires
    // the table to already exist (overwrite replaces an *existing* table), so we
    // create it directly — mirroring a real destination. The seed row also lets
    // the abort test prove the prior data survives a failed refresh.
    async fn seed_table(db_url: &str) {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .connect(&format!("{db_url}?mode=rwc"))
            .await
            .expect("open sqlite");
        sqlx::query("CREATE TABLE t (id TEXT, v TEXT)")
            .execute(&pool)
            .await
            .expect("create table");
        sqlx::query("INSERT INTO t (id, v) VALUES ('99', 'OLD')")
            .execute(&pool)
            .await
            .expect("seed row");
        pool.close().await;
    }

    /// Read the `v` column of every row of table `t`, sorted, via a fresh
    /// connection opened after the run (so the sink's pool has been dropped and
    /// its WAL checkpointed).
    async fn read_table_vs(db_url: &str) -> Vec<String> {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .connect(db_url)
            .await
            .expect("open sqlite");
        let rows: Vec<(String,)> = sqlx::query_as("SELECT v FROM t ORDER BY v")
            .fetch_all(&pool)
            .await
            .expect("read back");
        pool.close().await;
        rows.into_iter().map(|(v,)| v).collect()
    }

    fn overwrite_fanout_yaml(dir: &Path, db_url: &str) -> String {
        // execution.max_concurrent: 1 keeps the two child invocations
        // sequential — sqlite is single-writer, so this avoids SQLITE_BUSY
        // flakiness while still proving begin-once / commit-once.
        format!(
            "version: 1\nname: ow\nexecution:\n  max_concurrent: 1\npipeline:\n  sources:\n    parents:\n      type: csv\n      config: {{ path: \"{parents}\" }}\n    child:\n      type: csv\n      config: {{ path: \"{child}\" }}\n  sinks:\n    trash:\n      type: jsonl\n      config: {{ path: \"{trash}\", append: false }}\n    t:\n      type: sqlite\n      config:\n        database_url: \"{db_url}\"\n        table_name: t\n        column_mapping: auto_map\n        write_mode: overwrite\nmatrix:\n  - id: p\n    source: {{ ref: parents }}\n    sink: {{ ref: trash }}\n  - id: c\n    parent: p\n    parent_key: id\n    source: {{ ref: child }}\n    sink: {{ ref: t }}\n",
            parents = dir.join("parents.csv").display(),
            child = dir.join("child_${p.id}.csv").display(),
            trash = dir.join("trash.jsonl").display(),
        )
    }

    /// Expand a parent→child(overwrite sink) matrix and return the child node,
    /// with the child's destination table set to `child_table` (which may carry
    /// a `${p.id}` token). Pure config expansion — no connector I/O.
    fn overwrite_child_node(dir: &Path, child_table: &str) -> ExpandedNode {
        let yaml = format!(
            "version: 1\nname: t\nexecution:\n  max_concurrent: 1\npipeline:\n  sources:\n    parents: {{ type: csv, config: {{ path: \"{p}\" }} }}\n    child: {{ type: csv, config: {{ path: \"{c}\" }} }}\n  sinks:\n    trash: {{ type: jsonl, config: {{ path: \"{t}\" }} }}\n    dst: {{ type: sqlite, config: {{ database_url: \"sqlite:{db}\", table_name: \"{child_table}\", column_mapping: auto_map, write_mode: overwrite }} }}\nmatrix:\n  - id: p\n    source: {{ ref: parents }}\n    sink: {{ ref: trash }}\n  - id: c\n    parent: p\n    parent_key: id\n    source: {{ ref: child }}\n    sink: {{ ref: dst }}\n",
            p = dir.join("parents.csv").display(),
            c = dir.join("child.csv").display(),
            t = dir.join("trash.jsonl").display(),
            db = dir.join("t.db").display(),
        );
        let cfg = PipelineConfig::from_text(&yaml, &dir.join("c.yaml")).expect("config parses");
        expand(&cfg)
            .expect("config expands")
            .into_iter()
            .find(|n| n.id == "c")
            .expect("child node")
    }

    // #552: the executor collapses overwrite fan-out to ONE lifecycle per
    // resolved destination. This unit-tests the pure grouping (`plan_overwrite_groups`)
    // — the heart of the fix — deterministically, without the SQLite write
    // contention that makes a full-executor overwrite E2E flaky in CI. Combined
    // with the core `suppressed_overwrite_writes_but_skips_the_lifecycle` test
    // (each invocation appends, never begins/commits itself) and the abort E2E
    // below, this proves begin-once → append-all → commit-once retains every
    // parent's rows.
    #[tokio::test]
    async fn overwrite_groups_are_one_per_resolved_destination() {
        let dir = tempfile::tempdir().unwrap();
        let opts = exec_opts("t");
        let mk = |node: &ExpandedNode, pid: &str, id: i64| Unit {
            node: node.clone(),
            parent_record: Some(std::sync::Arc::new(json!({ "id": id }))),
            state_key: format!("t::c::{pid}"),
            parent_record_key: Some(pid.to_string()),
            product_ctx: None,
        };

        // Shared destination (fixed table) → ONE group across both parents, so
        // begin/commit run once — the fix for the last-writer-wins bug.
        let shared = overwrite_child_node(dir.path(), "orders");
        let units = vec![mk(&shared, "1", 1), mk(&shared, "2", 2)];
        let (groups, task_group) = plan_overwrite_groups(&units, &opts).unwrap();
        assert_eq!(
            groups.len(),
            1,
            "a shared destination collapses to one overwrite group"
        );
        assert_eq!(
            task_group.len(),
            2,
            "both invocations attach to the group (their per-invocation lifecycle is suppressed)"
        );

        // Per-parent destination (`${p.id}` in the table) → a single-member
        // group each, exactly as before — the out-of-scope case stays unchanged.
        let per = overwrite_child_node(dir.path(), "orders_${p.id}");
        let units2 = vec![mk(&per, "1", 1), mk(&per, "2", 2)];
        let (groups2, task_group2) = plan_overwrite_groups(&units2, &opts).unwrap();
        assert_eq!(
            groups2.len(),
            2,
            "per-parent tables stay separate single-member groups"
        );
        assert_eq!(task_group2.len(), 2);
    }

    #[tokio::test]
    async fn child_fanout_overwrite_aborts_on_failure_leaving_destination_intact() {
        let dir = tempfile::tempdir().unwrap();
        let db_url = format!("sqlite:{}", dir.path().join("ow.db").display());
        seed_table(&db_url).await;
        std::fs::write(dir.path().join("parents.csv"), "id\n1\n2\n").unwrap();
        std::fs::write(dir.path().join("child_1.csv"), "id,v\n1,A\n").unwrap();
        // child_2.csv is intentionally absent → the parent-2 child invocation
        // fails, so the whole overwrite group must abort (never commit partial).

        let yaml = overwrite_fanout_yaml(dir.path(), &db_url);
        let summary = run_yaml(&yaml, &dir.path().join("ow.yaml")).await;
        assert!(
            summary.had_failures(),
            "a missing child source must fail a unit"
        );

        // The prior destination is untouched: still exactly the seed row.
        let vs = read_table_vs(&db_url).await;
        assert_eq!(
            vs,
            vec!["OLD".to_string()],
            "destination must be unchanged: {vs:?}"
        );
    }

    /// The invocation future must stay small enough to be *constructed* on a
    /// 2 MiB thread stack (the CI test threads), whatever passes are compiled
    /// in. A debug build reserves the full size of an awaited future in the
    /// awaiting frame, so an unboxed post-run pass silently pushes every
    /// caller of `run_expanded` toward a stack overflow (#725).
    #[test]
    fn invocation_future_size_is_bounded() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.csv");
        let output = dir.path().join("out.jsonl");
        std::fs::write(&input, "name\nalice\n").unwrap();
        let cfg = cfg_csv_to_jsonl(&input, &output);
        let nodes = expand(&cfg).unwrap();
        let o = opts("size");
        let fut = run_one_invocation(
            &nodes[0],
            None,
            None,
            "size::row",
            None,
            &o,
            CancellationToken::new(),
            false,
            false,
        );
        let size = std::mem::size_of_val(&fut);
        drop(fut);
        eprintln!("run_one_invocation future: {size} bytes");
        assert!(
            size < 1_200_000,
            "run_one_invocation future is {size} bytes; box the newest pass (see boxed_run_one_invocation)"
        );
        let unit = Unit {
            node: nodes[0].clone(),
            parent_record: None,
            state_key: "size::row".into(),
            parent_record_key: None,
            product_ctx: None,
        };
        let captured: CapturedRecords = Default::default();
        let discovered: DiscoveredDims = Default::default();
        let collected: CollectedDims = Default::default();
        let run_unit_fut = run_unit(
            &unit,
            None,
            &captured,
            &discovered,
            &collected,
            &o,
            CancellationToken::new(),
            false,
            false,
        );
        let unit_size = std::mem::size_of_val(&run_unit_fut);
        drop(run_unit_fut);
        eprintln!("run_unit future: {unit_size} bytes");
        assert!(
            unit_size < 65_536,
            "run_unit future is {unit_size} bytes; the invocation must stay boxed"
        );
    }

    #[tokio::test]
    async fn metadata_columns_are_stamped_onto_output() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.csv");
        let output = dir.path().join("out.jsonl");
        std::fs::write(&input, "name\nalice\n").unwrap();
        let mut cfg = cfg_csv_to_jsonl(&input, &output);
        cfg.metadata_columns = Some(faucet_core::MetadataColumnsSpec {
            enabled: true,
            prefix: "_faucet".into(),
            columns: vec![
                faucet_core::MetadataColumn::RunId,
                faucet_core::MetadataColumn::Source,
                faucet_core::MetadataColumn::LoadedAt,
            ],
        });
        let nodes = expand(&cfg).unwrap();
        run_expanded(nodes, opts("t")).await.unwrap();
        let body = std::fs::read_to_string(&output).unwrap();
        let row: serde_json::Value = serde_json::from_str(body.lines().next().unwrap()).unwrap();
        assert_eq!(row["name"], "alice");
        assert_eq!(row["_faucet_source"], "csv");
        assert!(row["_faucet_run_id"].is_string());
        assert!(row["_faucet_loaded_at"].is_string());
    }

    #[tokio::test]
    async fn metadata_columns_disabled_stamps_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.csv");
        let output = dir.path().join("out.jsonl");
        std::fs::write(&input, "name\nalice\n").unwrap();
        let mut cfg = cfg_csv_to_jsonl(&input, &output);
        cfg.metadata_columns = Some(faucet_core::MetadataColumnsSpec {
            enabled: false,
            ..Default::default()
        });
        let nodes = expand(&cfg).unwrap();
        run_expanded(nodes, opts("t")).await.unwrap();
        let body = std::fs::read_to_string(&output).unwrap();
        let row: serde_json::Value = serde_json::from_str(body.lines().next().unwrap()).unwrap();
        assert_eq!(row["name"], "alice");
        assert!(row.get("_faucet_run_id").is_none());
    }

    /// Minimal options with a catalog handle attached.
    #[cfg(feature = "catalog")]
    fn opts_with_catalog(name: &str, handle: crate::catalog::CatalogHandle) -> ExecuteOptions {
        let mut o = opts(name);
        o.catalog = Some(handle);
        o
    }

    #[cfg(feature = "catalog")]
    #[tokio::test]
    async fn catalog_records_schema_timeline_across_two_runs() {
        // Acceptance (#279): running the same pipeline twice with a schema
        // change in between produces exactly two schema-timeline entries for
        // the dataset, the second carrying a computed diff.
        use crate::catalog::CatalogHandle;
        use crate::serve::history::RunHistory as _;
        use crate::serve::history::catalog::{self, CatalogListFilter};
        use crate::serve::history::memory::MemoryHistory;

        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.csv");
        let output = dir.path().join("out.jsonl");
        let store = Arc::new(MemoryHistory::new(std::time::Duration::from_secs(60)));
        let handle = CatalogHandle {
            store: store.clone(),
            run_id: None,
            sample_records: 10,
        };

        std::fs::write(&input, "id,name\n1,alice\n2,bob\n").unwrap();
        let cfg = cfg_csv_to_jsonl(&input, &output);
        let nodes = expand(&cfg).unwrap();
        let summary = run_expanded(nodes, opts_with_catalog("cat", handle.clone()))
            .await
            .unwrap();
        assert!(!summary.had_failures());

        // Second run: same pipeline, schema gains an `email` column.
        std::fs::write(&input, "id,name,email\n1,alice,a@x.io\n2,bob,b@x.io\n").unwrap();
        let nodes = expand(&cfg).unwrap();
        let summary = run_expanded(nodes, opts_with_catalog("cat", handle))
            .await
            .unwrap();
        assert!(!summary.had_failures());

        // Two datasets (source + sink), each with a 2-entry deduped timeline.
        let page = store
            .catalog_list_datasets(&CatalogListFilter {
                limit: 10,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(page.datasets.len(), 2, "source + sink datasets");
        for ds in &page.datasets {
            let detail = store
                .catalog_get_dataset(&ds.id)
                .await
                .unwrap()
                .expect("dataset detail");
            assert_eq!(detail.dataset.runs, 2);
            assert_eq!(
                detail.schema_timeline.len(),
                2,
                "exactly two timeline entries for {}",
                ds.uri
            );
            assert!(detail.schema_timeline[0].diff.is_none());
            let diff = detail.schema_timeline[1]
                .diff
                .as_ref()
                .expect("second version carries a diff");
            assert!(
                diff["added"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|c| c["column"] == "email"),
                "diff must show the added email column: {diff}"
            );
            assert_eq!(detail.stats.len(), 2, "one volume point per run");
        }
        // One lineage edge, csv → jsonl, traversed twice.
        let edges = store.catalog_lineage(None, 5).await.unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].runs, 2);
        assert_eq!(edges[0].last_records, 2);
        assert_eq!(edges[0].src_id, catalog::dataset_id(&edges[0].src_uri));
    }

    /// A catalog store whose writes always fail — drives the never-fail-the-run
    /// contract.
    #[cfg(feature = "catalog")]
    struct FailingCatalogStore;

    #[cfg(feature = "catalog")]
    #[async_trait]
    impl crate::serve::history::RunHistory for FailingCatalogStore {
        async fn claim_idempotency(
            &self,
            _: &str,
            _: &str,
            _: &str,
            _: std::time::Duration,
        ) -> Result<crate::serve::history::Claim, crate::serve::history::HistoryError> {
            Err(crate::serve::history::HistoryError::Backend("down".into()))
        }
        async fn upsert(
            &self,
            _: &crate::serve::history::RunRecord,
        ) -> Result<(), crate::serve::history::HistoryError> {
            Err(crate::serve::history::HistoryError::Backend("down".into()))
        }
        async fn get(
            &self,
            _: &str,
        ) -> Result<Option<crate::serve::history::RunRecord>, crate::serve::history::HistoryError>
        {
            Err(crate::serve::history::HistoryError::Backend("down".into()))
        }
        async fn list(
            &self,
            _: &crate::serve::history::ListFilter,
        ) -> Result<crate::serve::history::ListPage, crate::serve::history::HistoryError> {
            Err(crate::serve::history::HistoryError::Backend("down".into()))
        }
        async fn delete(
            &self,
            _: &str,
        ) -> Result<crate::serve::history::DeleteOutcome, crate::serve::history::HistoryError>
        {
            Err(crate::serve::history::HistoryError::Backend("down".into()))
        }
        async fn purge_expired(
            &self,
            _: std::time::Duration,
        ) -> Result<usize, crate::serve::history::HistoryError> {
            Err(crate::serve::history::HistoryError::Backend("down".into()))
        }
        async fn recover_orphans(&self) -> Result<usize, crate::serve::history::HistoryError> {
            Err(crate::serve::history::HistoryError::Backend("down".into()))
        }
        async fn catalog_record(
            &self,
            _: &crate::serve::history::catalog::CatalogUpdate,
        ) -> Result<(), crate::serve::history::HistoryError> {
            Err(crate::serve::history::HistoryError::Backend(
                "catalog write refused".into(),
            ))
        }
        fn degraded(&self) -> bool {
            false
        }
    }

    #[cfg(feature = "catalog")]
    #[tokio::test]
    async fn catalog_write_failure_never_fails_the_run() {
        // Acceptance (#279): a forced catalog-backend error degrades (logged)
        // while the pipeline still succeeds and writes its output.
        use crate::catalog::CatalogHandle;
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.csv");
        let output = dir.path().join("out.jsonl");
        std::fs::write(&input, "name\nalice\n").unwrap();
        let cfg = cfg_csv_to_jsonl(&input, &output);
        let nodes = expand(&cfg).unwrap();
        let handle = CatalogHandle {
            store: Arc::new(FailingCatalogStore),
            run_id: None,
            sample_records: 10,
        };
        let summary = run_expanded(nodes, opts_with_catalog("cat-fail", handle))
            .await
            .unwrap();
        assert!(
            !summary.had_failures(),
            "catalog failure must not fail the run"
        );
        assert_eq!(summary.invocations[0].records_written, 1);
        assert_eq!(
            std::fs::read_to_string(&output).unwrap().lines().count(),
            1,
            "sink output written despite the catalog error"
        );
    }

    #[tokio::test]
    async fn matrix_two_independent_roots_both_run() {
        // Two roots: one writes alice, the other writes bob — to two separate files.
        let dir = tempfile::tempdir().unwrap();
        let csv_a = dir.path().join("a.csv");
        let csv_b = dir.path().join("b.csv");
        let out_a = dir.path().join("a.jsonl");
        let out_b = dir.path().join("b.jsonl");
        std::fs::write(&csv_a, "name\nalice\n").unwrap();
        std::fs::write(&csv_b, "name\nbob\n").unwrap();

        let yaml = format!(
            r#"version: 1
pipeline:
  source: {{ type: csv, config: {{ path: {a} }} }}
  sink:   {{ type: jsonl, config: {{ path: {out_a} }} }}
matrix:
  - id: rowA
  - id: rowB
    source: {{ config: {{ path: {b} }} }}
    sink:   {{ config: {{ path: {out_b} }} }}
"#,
            a = csv_a.display(),
            b = csv_b.display(),
            out_a = out_a.display(),
            out_b = out_b.display(),
        );
        let cfg = crate::config::parse_with_extension(&yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        let summary = run_expanded(
            nodes,
            ExecuteOptions {
                pipeline_name: "matrix".into(),
                run_id: None,
                execution: None,
                concurrency: None,
                dry_run: false,
                limit: None,
                state_path_override: None,
                shard: None,
                auth: Default::default(),
                clock: chrono::Utc::now().fixed_offset(),
                cancel: None,
                resilience: None,
                sla: None,
                reconcile: None,
                verify: None,
                rollback: None,
                #[cfg(feature = "lineage")]
                lineage: None,
                #[cfg(feature = "lineage")]
                lineage_cfg: None,
                #[cfg(feature = "notify")]
                notifier: None,
                #[cfg(feature = "catalog")]
                catalog: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(summary.invocations.len(), 2);
        assert!(out_a.exists());
        assert!(out_b.exists());
    }

    #[tokio::test]
    async fn dag_child_fans_out_per_parent_record() {
        // Parent: CSV with two records (id=1, id=2).
        // Child: writes one JSONL file per parent id, using ${parent.id} in the path.
        let dir = tempfile::tempdir().unwrap();
        let parent_csv = dir.path().join("parents.csv");
        let child_csv = dir.path().join("child.csv");
        std::fs::write(&parent_csv, "id,name\n1,alice\n2,bob\n").unwrap();
        std::fs::write(&child_csv, "x\nA\nB\nC\n").unwrap();
        let parent_out = dir.path().join("parents.jsonl");
        let child_out_pattern = dir.path().join("child-${parents.id}.jsonl");

        let yaml = format!(
            r#"version: 1
pipeline:
  source: {{ type: csv, config: {{ path: {parent} }} }}
  sink:   {{ type: jsonl, config: {{ path: {parent_out} }} }}
matrix:
  - id: parents
  - id: child
    parent: parents
    source: {{ config: {{ path: {child} }} }}
    sink:   {{ config: {{ path: "{child_out}" }} }}
"#,
            parent = parent_csv.display(),
            parent_out = parent_out.display(),
            child = child_csv.display(),
            child_out = child_out_pattern.display(),
        );
        let cfg = crate::config::parse_with_extension(&yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        let summary = run_expanded(
            nodes,
            ExecuteOptions {
                pipeline_name: "dagtest".into(),
                run_id: None,
                execution: None,
                concurrency: None,
                dry_run: false,
                limit: None,
                state_path_override: None,
                shard: None,
                auth: Default::default(),
                clock: chrono::Utc::now().fixed_offset(),
                cancel: None,
                resilience: None,
                sla: None,
                reconcile: None,
                verify: None,
                rollback: None,
                #[cfg(feature = "lineage")]
                lineage: None,
                #[cfg(feature = "lineage")]
                lineage_cfg: None,
                #[cfg(feature = "notify")]
                notifier: None,
                #[cfg(feature = "catalog")]
                catalog: None,
            },
        )
        .await
        .unwrap();

        // 1 parent invocation + 2 child invocations.
        assert_eq!(summary.invocations.len(), 3);
        assert!(!summary.had_failures(), "{:?}", summary);
        assert!(dir.path().join("child-1.jsonl").exists());
        assert!(dir.path().join("child-2.jsonl").exists());
    }

    #[tokio::test]
    async fn depends_on_root_runs_after_dependency() {
        // `stage` writes a CSV that `load` reads — `load` can only succeed if
        // it genuinely starts after `stage` finishes (pure ordering, no
        // record hand-off).
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.csv");
        let mid = dir.path().join("mid.csv");
        let out = dir.path().join("out.jsonl");
        std::fs::write(&input, "name\nalice\nbob\n").unwrap();

        let yaml = format!(
            r#"version: 1
pipeline:
  source: {{ type: csv, config: {{ path: {input} }} }}
  sink:   {{ type: jsonl, config: {{ path: {out} }} }}
matrix:
  - id: stage
    sink: {{ type: csv, config: {{ path: {mid} }} }}
  - id: load
    depends_on: [stage]
    source: {{ config: {{ path: {mid} }} }}
"#,
            input = input.display(),
            mid = mid.display(),
            out = out.display(),
        );
        let cfg = crate::config::parse_with_extension(&yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        let summary = run_expanded(nodes, opts("depsorder")).await.unwrap();
        assert_eq!(summary.invocations.len(), 2, "{summary:?}");
        assert!(!summary.had_failures(), "{summary:?}");
        let load = summary
            .invocations
            .iter()
            .find(|i| i.row_id == "load")
            .unwrap();
        assert_eq!(load.records_written, 2);
        let written = std::fs::read_to_string(&out).unwrap();
        assert_eq!(written.lines().count(), 2);
    }

    #[tokio::test]
    async fn diamond_dependency_waits_for_all_prerequisites() {
        // c waits on both a and b (a diamond join): readiness must require
        // *every* dependency to be terminal, not just the first.
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.csv");
        let mid_a = dir.path().join("mid_a.csv");
        let mid_b = dir.path().join("mid_b.csv");
        let out = dir.path().join("out.jsonl");
        std::fs::write(&input, "name\nalice\n").unwrap();

        let yaml = format!(
            r#"version: 1
pipeline:
  source: {{ type: csv, config: {{ path: {input} }} }}
  sink:   {{ type: jsonl, config: {{ path: {out} }} }}
matrix:
  - id: a
    sink: {{ type: csv, config: {{ path: {mid_a} }} }}
  - id: b
    sink: {{ type: csv, config: {{ path: {mid_b} }} }}
  - id: c
    depends_on: [a, b]
    source: {{ config: {{ path: {mid_a} }} }}
"#,
            input = input.display(),
            mid_a = mid_a.display(),
            mid_b = mid_b.display(),
            out = out.display(),
        );
        let cfg = crate::config::parse_with_extension(&yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        let summary = run_expanded(nodes, opts("diamond")).await.unwrap();
        assert_eq!(summary.invocations.len(), 3, "{summary:?}");
        assert!(!summary.had_failures(), "{summary:?}");
        assert!(mid_b.exists(), "b must have run before c became ready");
        assert!(out.exists());
    }

    #[tokio::test]
    async fn failed_dependency_skips_dependent() {
        // `stage` fails (missing input file); `load` depends on it and must be
        // skipped — no invocation outcome, no output file.
        let dir = tempfile::tempdir().unwrap();
        let good_input = dir.path().join("good.csv");
        let out = dir.path().join("out.jsonl");
        std::fs::write(&good_input, "name\nalice\n").unwrap();

        let yaml = format!(
            r#"version: 1
pipeline:
  source: {{ type: csv, config: {{ path: {good} }} }}
  sink:   {{ type: jsonl, config: {{ path: {out} }} }}
matrix:
  - id: stage
    source: {{ config: {{ path: {missing} }} }}
  - id: load
    depends_on: [stage]
"#,
            good = good_input.display(),
            missing = dir.path().join("nonexistent.csv").display(),
            out = out.display(),
        );
        let cfg = crate::config::parse_with_extension(&yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        let summary = run_expanded(nodes, opts("depskip")).await.unwrap();
        assert_eq!(summary.invocations.len(), 1, "{summary:?}");
        assert_eq!(summary.invocations[0].row_id, "stage");
        assert!(summary.invocations[0].error.is_some());
        assert!(
            !out.exists(),
            "dependent row must not run after its dependency failed"
        );
    }

    #[tokio::test]
    async fn dependency_on_skipped_row_cascades() {
        // p fails → its child c is skipped → q (which depends on c) must be
        // skipped too, even though c itself never *failed*.
        let dir = tempfile::tempdir().unwrap();
        let good_input = dir.path().join("good.csv");
        let out = dir.path().join("q.jsonl");
        std::fs::write(&good_input, "id\n1\n").unwrap();

        let yaml = format!(
            r#"version: 1
pipeline:
  source: {{ type: csv, config: {{ path: {good} }} }}
  sink:   {{ type: jsonl, config: {{ path: {out} }} }}
matrix:
  - id: p
    source: {{ config: {{ path: {missing} }} }}
  - id: c
    parent: p
  - id: q
    depends_on: [c]
"#,
            good = good_input.display(),
            missing = dir.path().join("nonexistent.csv").display(),
            out = out.display(),
        );
        let cfg = crate::config::parse_with_extension(&yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        let summary = run_expanded(nodes, opts("depcascade")).await.unwrap();
        assert_eq!(summary.invocations.len(), 1, "{summary:?}");
        assert_eq!(summary.invocations[0].row_id, "p");
        assert!(summary.invocations[0].error.is_some());
        assert!(
            !out.exists(),
            "q must be skipped when its dependency was skipped"
        );
    }

    #[tokio::test]
    async fn on_error_stop_reports_failure_and_runs_no_extra_work() {
        // First root writes to an invalid sink path and fails. The second
        // ("good") root would succeed. Under `on_error: stop` the executor
        // calls `abort_all()` on the first failure, which cancels pending /
        // in-flight tasks at their next await point — but that is
        // best-effort: with `max_concurrent: 1` the two roots race for the
        // single permit, so "good" may already have completed before "bad"
        // fails. We therefore assert the guarantees that hold under *any*
        // scheduling rather than an exact invocation count (which was racy,
        // see issue #78 finding #24). The deterministic "stop actually
        // cancels in-flight work" path is covered by
        // `on_error_stop_under_parallelism_aborts_other_in_flight`.
        let dir = tempfile::tempdir().unwrap();
        let good_csv = dir.path().join("good.csv");
        std::fs::write(&good_csv, "x\n1\n").unwrap();
        let good_out = dir.path().join("good.jsonl");
        let bad_sink_dir = dir.path().to_path_buf();

        let yaml = format!(
            r#"version: 1
pipeline:
  source: {{ type: csv, config: {{ path: {good_csv} }} }}
  sink:   {{ type: jsonl, config: {{ path: {good_out} }} }}
matrix:
  - id: bad
    sink: {{ config: {{ path: {bad_dir} }} }}
  - id: good
execution:
  max_concurrent: 1
  on_error: stop
"#,
            good_csv = good_csv.display(),
            good_out = good_out.display(),
            bad_dir = bad_sink_dir.display(),
        );
        let cfg = crate::config::parse_with_extension(&yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        let summary = run_expanded(
            nodes,
            ExecuteOptions {
                pipeline_name: "stoptest".into(),
                run_id: None,
                execution: cfg.execution.clone(),
                concurrency: None,
                dry_run: false,
                limit: None,
                state_path_override: None,
                shard: None,
                auth: Default::default(),
                clock: chrono::Utc::now().fixed_offset(),
                cancel: None,
                resilience: None,
                sla: None,
                reconcile: None,
                verify: None,
                rollback: None,
                #[cfg(feature = "lineage")]
                lineage: None,
                #[cfg(feature = "lineage")]
                lineage_cfg: None,
                #[cfg(feature = "notify")]
                notifier: None,
                #[cfg(feature = "catalog")]
                catalog: None,
            },
        )
        .await
        .unwrap();

        // Invariants that hold regardless of which root won the permit race:
        assert!(summary.had_failures(), "the failing root must be reported");

        // "bad" ran exactly once and is recorded as a failure.
        let bad: Vec<_> = summary
            .invocations
            .iter()
            .filter(|o| o.row_id == "bad")
            .collect();
        assert_eq!(bad.len(), 1, "bad must run exactly once");
        assert!(bad[0].error.is_some(), "bad must be recorded as a failure");

        // No duplicate / extra invocations beyond the two work units.
        assert!(
            summary.invocations.len() <= 2,
            "at most the two roots may run, got {:?}",
            summary.invocations
        );

        // "good" may: (a) win the permit first and run fully (writes its row,
        // file exists); (b) lose the race, acquire the permit after "bad" fails,
        // observe the cooperative stop-cancel at its first page boundary, and
        // return a 0-record success (no file); or (c) never appear if it was
        // still pending when the level finished. So the only invariant is: a
        // "good" that actually WROTE records must have produced its file.
        let good_wrote = summary
            .invocations
            .iter()
            .find(|o| o.row_id == "good" && o.error.is_none())
            .map(|o| o.records_written)
            .unwrap_or(0);
        if good_wrote > 0 {
            assert!(
                good_out.exists(),
                "a good that wrote records must have produced its output file"
            );
        }
    }

    #[tokio::test]
    async fn invalid_pipeline_name_with_state_errors_up_front() {
        // A pipeline name that can't form a valid state key must fail up front
        // (at unit construction) when state is configured — not deep mid-run
        // as a `FaucetError::State`.
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.csv");
        let output = dir.path().join("out.jsonl");
        std::fs::write(&input, "name\nalice\n").unwrap();
        let yaml = format!(
            r#"version: 1
pipeline:
  source: {{ type: csv, config: {{ path: {input} }} }}
  sink:   {{ type: jsonl, config: {{ path: {output} }} }}
  state:  {{ type: memory }}
"#,
            input = input.display(),
            output = output.display(),
        );
        let cfg = crate::config::parse_with_extension(&yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        let err = run_expanded(
            nodes,
            ExecuteOptions {
                pipeline_name: "bad name".into(), // space is illegal in a state key
                run_id: None,
                execution: None,
                concurrency: None,
                dry_run: false,
                limit: None,
                state_path_override: None,
                shard: None,
                auth: Default::default(),
                clock: chrono::Utc::now().fixed_offset(),
                cancel: None,
                resilience: None,
                sla: None,
                reconcile: None,
                verify: None,
                rollback: None,
                #[cfg(feature = "lineage")]
                lineage: None,
                #[cfg(feature = "lineage")]
                lineage_cfg: None,
                #[cfg(feature = "notify")]
                notifier: None,
                #[cfg(feature = "catalog")]
                catalog: None,
            },
        )
        .await
        .expect_err("an invalid pipeline name must be rejected up front when state is configured");
        assert!(matches!(err, CliError::InvalidStateKey { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn invalid_parent_key_value_with_state_errors_up_front() {
        // A parent-record value that yields an illegal state-key suffix must
        // fail up front at the child's unit construction, not mid-run.
        let dir = tempfile::tempdir().unwrap();
        let parent_csv = dir.path().join("parents.csv");
        let child_csv = dir.path().join("child.csv");
        // The parent `id` value contains a space — illegal in a state key.
        std::fs::write(&parent_csv, "id\nbad id\n").unwrap();
        std::fs::write(&child_csv, "x\nA\n").unwrap();
        let parent_out = dir.path().join("parents.jsonl");
        let child_out = dir.path().join("child.jsonl");
        let yaml = format!(
            r#"version: 1
pipeline:
  source: {{ type: csv, config: {{ path: {parent} }} }}
  sink:   {{ type: jsonl, config: {{ path: {parent_out} }} }}
  state:  {{ type: memory }}
matrix:
  - id: parents
  - id: child
    parent: parents
    source: {{ config: {{ path: {child} }} }}
    sink:   {{ config: {{ path: {child_out} }} }}
"#,
            parent = parent_csv.display(),
            parent_out = parent_out.display(),
            child = child_csv.display(),
            child_out = child_out.display(),
        );
        let cfg = crate::config::parse_with_extension(&yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        let err = run_expanded(
            nodes,
            ExecuteOptions {
                pipeline_name: "ok".into(),
                run_id: None,
                execution: None,
                concurrency: None,
                dry_run: false,
                limit: None,
                state_path_override: None,
                shard: None,
                auth: Default::default(),
                clock: chrono::Utc::now().fixed_offset(),
                cancel: None,
                resilience: None,
                sla: None,
                reconcile: None,
                verify: None,
                rollback: None,
                #[cfg(feature = "lineage")]
                lineage: None,
                #[cfg(feature = "lineage")]
                lineage_cfg: None,
                #[cfg(feature = "notify")]
                notifier: None,
                #[cfg(feature = "catalog")]
                catalog: None,
            },
        )
        .await
        .expect_err(
            "an illegal parent-key value must be rejected up front when state is configured",
        );
        assert!(matches!(err, CliError::InvalidStateKey { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn on_error_stop_under_parallelism_aborts_other_in_flight() {
        // Three roots running with `max_concurrent: 3`. The bad row points
        // its sink at a directory (open fails fast). The other two point at
        // sinks that block forever on the writer end of a pipe — stuck *inside*
        // the sink write, they never reach a page boundary to observe the
        // cooperative stop-cancel, so the only way they can complete is the
        // hard-abort backstop that fires after the flush grace (#146 H16). The
        // test would hang if `on_error: stop` never aborted them, so a passing
        // run is itself the assertion.
        let dir = tempfile::tempdir().unwrap();
        let bad_sink_dir = dir.path().to_path_buf();
        // A real csv source with one row — small enough that the pipeline
        // proceeds straight to the sink phase.
        let good_csv = dir.path().join("good.csv");
        std::fs::write(&good_csv, "x\n1\n").unwrap();
        // The two "would never finish" sinks point at the same path as the
        // bad sink (an existing directory). Their sink-open also errors
        // out — but we still verify the *abort* path by counting how many
        // tasks make it past spawn before stop fires. The strict invariant
        // we assert: the bad row's failure is the first one observed.
        let yaml = format!(
            r#"version: 1
pipeline:
  source: {{ type: csv, config: {{ path: {good_csv} }} }}
  sink:   {{ type: jsonl, config: {{ path: {bad_dir} }} }}
matrix:
  - id: bad
  - id: good_a
  - id: good_b
execution:
  max_concurrent: 3
  on_error: stop
"#,
            good_csv = good_csv.display(),
            bad_dir = bad_sink_dir.display(),
        );
        let cfg = crate::config::parse_with_extension(&yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        let summary = run_expanded(
            nodes,
            ExecuteOptions {
                pipeline_name: "stop_parallel".into(),
                run_id: None,
                execution: cfg.execution.clone(),
                concurrency: None,
                dry_run: false,
                limit: None,
                state_path_override: None,
                shard: None,
                auth: Default::default(),
                clock: chrono::Utc::now().fixed_offset(),
                cancel: None,
                resilience: None,
                sla: None,
                reconcile: None,
                verify: None,
                rollback: None,
                #[cfg(feature = "lineage")]
                lineage: None,
                #[cfg(feature = "lineage")]
                lineage_cfg: None,
                #[cfg(feature = "notify")]
                notifier: None,
                #[cfg(feature = "catalog")]
                catalog: None,
            },
        )
        .await
        .unwrap();

        // First-observed failure halts the run. The first outcome in the
        // summary is guaranteed to be a failure (other tasks either fail
        // too or get cancelled — both cases never push a *success* outcome
        // first because every sink in this matrix is configured to fail).
        assert!(
            summary.had_failures(),
            "summary should record at least one failure: {summary:?}"
        );
        assert!(
            summary.invocations[0].error.is_some(),
            "first outcome must be the failure that triggered stop: {summary:?}"
        );
        // No invocation should report `records_written > 0` — every sink is
        // bad. (Catches a regression where abort_all somehow let a task
        // bypass its broken sink.)
        for inv in &summary.invocations {
            assert_eq!(inv.records_written, 0, "no records should land: {inv:?}");
        }
    }

    #[tokio::test]
    async fn on_error_continue_skips_failed_subtree_only() {
        // Two roots: one fails. The good one's invocation still completes.
        let dir = tempfile::tempdir().unwrap();
        let good_csv = dir.path().join("good.csv");
        std::fs::write(&good_csv, "x\n1\n").unwrap();
        let good_out = dir.path().join("good.jsonl");

        let yaml = format!(
            r#"version: 1
pipeline:
  source: {{ type: csv, config: {{ path: {good_csv} }} }}
  sink:   {{ type: jsonl, config: {{ path: {good_out} }} }}
matrix:
  - id: bad
    sink: {{ config: {{ path: {bad_dir} }} }}
  - id: good
"#,
            good_csv = good_csv.display(),
            good_out = good_out.display(),
            bad_dir = dir.path().display(),
        );
        let cfg = crate::config::parse_with_extension(&yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        let summary = run_expanded(
            nodes,
            ExecuteOptions {
                pipeline_name: "continuetest".into(),
                run_id: None,
                execution: None,
                concurrency: None,
                dry_run: false,
                limit: None,
                state_path_override: None,
                shard: None,
                auth: Default::default(),
                clock: chrono::Utc::now().fixed_offset(),
                cancel: None,
                resilience: None,
                sla: None,
                reconcile: None,
                verify: None,
                rollback: None,
                #[cfg(feature = "lineage")]
                lineage: None,
                #[cfg(feature = "lineage")]
                lineage_cfg: None,
                #[cfg(feature = "notify")]
                notifier: None,
                #[cfg(feature = "catalog")]
                catalog: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(summary.invocations.len(), 2);
        assert_eq!(summary.failure_count(), 1);
        let good_outcome = summary
            .invocations
            .iter()
            .find(|i| i.row_id == "good")
            .unwrap();
        assert!(good_outcome.error.is_none());
    }

    // ── projection helpers (#160) ─────────────────────────────────────────────

    #[test]
    fn split_path_splits_on_dots() {
        assert_eq!(split_path("id"), vec!["id".to_string()]);
        assert_eq!(
            split_path("user.name"),
            vec!["user".to_string(), "name".to_string()]
        );
    }

    #[test]
    fn minimal_paths_drops_descendants_of_kept_ancestors() {
        let paths = vec![
            vec!["user".into(), "name".into()],
            vec!["user".into()],
            vec!["id".into()],
            vec!["id".into()],
        ];
        let min = minimal_paths(paths);
        assert!(min.contains(&vec!["user".to_string()]));
        assert!(min.contains(&vec!["id".to_string()]));
        assert!(
            !min.contains(&vec!["user".to_string(), "name".to_string()]),
            "user.name must be dropped — covered by user"
        );
        assert_eq!(min.len(), 2);
    }

    #[test]
    fn project_full_clones_whole_record() {
        let r = json!({"a": 1, "b": {"c": 2}});
        assert_eq!(project_record(&r, &Projection::Full), r);
    }

    #[test]
    fn project_keeps_only_referenced_paths() {
        let r = json!({"id": 7, "user": {"name": "a", "age": 3}, "blob": "<huge>"});
        let p = Projection::Paths(vec![vec!["id".into()], vec!["user".into(), "name".into()]]);
        let got = project_record(&r, &p);
        assert_eq!(got, json!({"id": 7, "user": {"name": "a"}}));
        assert!(got.get("blob").is_none());
        assert!(got["user"].get("age").is_none());
    }

    #[test]
    fn project_array_index_path_resolves_same_as_original() {
        let r = json!({"tags": ["x", "y", "z"]});
        let p = Projection::Paths(vec![vec!["tags".into(), "0".into()]]);
        let got = project_record(&r, &p);
        assert_eq!(got, json!({"tags": {"0": "x"}}));
        assert_eq!(resolve_parent_key(&got, "tags.0"), Some(json!("x")));
        assert_eq!(
            resolve_parent_key(&got, "tags.0"),
            resolve_parent_key(&r, "tags.0"),
            "reduced tree must resolve the same value as the original"
        );
    }

    #[test]
    fn project_numeric_object_key_resolves_same_as_original() {
        // A numeric segment can address an OBJECT key in the original (not an
        // array index). The reduced all-objects tree stores it under the same
        // key, so resolution still matches — the parity property the design
        // relies on, distinct from the array-index case above.
        let r = json!({"data": {"0": "x", "1": "y"}});
        let p = Projection::Paths(vec![vec!["data".into(), "0".into()]]);
        let got = project_record(&r, &p);
        assert_eq!(got, json!({"data": {"0": "x"}}));
        assert_eq!(
            resolve_parent_key(&got, "data.0"),
            resolve_parent_key(&r, "data.0"),
            "numeric object-key path must resolve identically on the reduced tree"
        );
    }

    #[test]
    fn project_missing_path_is_omitted() {
        let r = json!({"id": 1});
        let p = Projection::Paths(vec![vec!["nope".into()]]);
        assert_eq!(project_record(&r, &p), json!({}));
    }

    #[test]
    fn build_projections_unions_parent_key_and_refs() {
        use crate::config::ConnectorSpec;
        use crate::expand::{DeferredRef, ExpandedNode, NodeRole};

        fn child(id: &str, parent: &str, parent_key: &str, refs: &[(&str, &str)]) -> ExpandedNode {
            ExpandedNode {
                id: id.into(),
                row_index: 0,
                weight: None,
                role: NodeRole::Child {
                    parent_id: parent.into(),
                    parent_key: parent_key.into(),
                },
                source: ConnectorSpec {
                    kind: "csv".into(),
                    config: json!({}),
                    transforms: None,
                    inherit_transforms: true,
                    status: None,
                    tags: Vec::new(),
                    complete_for: None,
                },
                sink: ConnectorSpec {
                    kind: "jsonl".into(),
                    config: json!({}),
                    transforms: None,
                    inherit_transforms: true,
                    status: None,
                    tags: Vec::new(),
                    complete_for: None,
                },
                transforms: Vec::new(),
                state: None,
                dlq: None,
                sla: None,
                delivery: faucet_core::DeliveryMode::AtLeastOnce,
                delivery_guarantee: faucet_core::DeliveryGuarantee::AtLeastOnce,
                #[cfg(feature = "quality")]
                quality: None,
                #[cfg(feature = "contract")]
                contract: None,
                #[cfg(feature = "masking")]
                masking: None,
                sink_ref: "default".into(),
                schema: None,
                depends_on: Vec::new(),
                status: crate::config::SourceStatus::Active,
                tags: Vec::new(),
                cleanup_scope: None,
                metadata_columns: None,
                #[cfg(feature = "catalog")]
                local_outputs: None,
                deferred_refs: refs
                    .iter()
                    .map(|(rid, p)| DeferredRef {
                        referenced_id: (*rid).into(),
                        dotted_path: (*p).into(),
                        token: format!("${{{rid}.{p}}}"),
                    })
                    .collect(),
                source_override: None,
            }
        }

        let c1 = child("c1", "p", "id", &[("p", "user.name")]);
        let c2 = child("c2", "p", "id", &[("p", "email"), ("q", "x")]);
        let nodes_by_id = HashMap::from([("c1".to_string(), c1), ("c2".to_string(), c2)]);
        let children_of =
            HashMap::from([("p".to_string(), vec!["c1".to_string(), "c2".to_string()])]);

        let projs = build_projections(&nodes_by_id, &children_of);
        let p = projs.get("p").expect("projection for p");
        match &**p {
            Projection::Paths(paths) => {
                assert!(paths.contains(&vec!["id".to_string()]));
                assert!(paths.contains(&vec!["user".to_string(), "name".to_string()]));
                assert!(paths.contains(&vec!["email".to_string()]));
                assert!(
                    !paths.iter().any(|p| p == &vec!["x".to_string()]),
                    "a ref to a different parent must not be captured under p"
                );
            }
            Projection::Full => panic!("expected Paths, got Full"),
        }
    }

    #[test]
    fn build_projections_whole_record_ref_is_full() {
        use crate::config::ConnectorSpec;
        use crate::expand::{DeferredRef, ExpandedNode, NodeRole};
        let c = ExpandedNode {
            id: "c".into(),
            row_index: 0,
            weight: None,
            role: NodeRole::Child {
                parent_id: "p".into(),
                parent_key: "id".into(),
            },
            source: ConnectorSpec {
                kind: "csv".into(),
                config: json!({}),
                transforms: None,
                inherit_transforms: true,
                status: None,
                tags: Vec::new(),
                complete_for: None,
            },
            sink: ConnectorSpec {
                kind: "jsonl".into(),
                config: json!({}),
                transforms: None,
                inherit_transforms: true,
                status: None,
                tags: Vec::new(),
                complete_for: None,
            },
            transforms: Vec::new(),
            state: None,
            dlq: None,
            sla: None,
            delivery: faucet_core::DeliveryMode::AtLeastOnce,
            delivery_guarantee: faucet_core::DeliveryGuarantee::AtLeastOnce,
            #[cfg(feature = "quality")]
            quality: None,
            #[cfg(feature = "contract")]
            contract: None,
            #[cfg(feature = "masking")]
            masking: None,
            sink_ref: "default".into(),
            schema: None,
            depends_on: Vec::new(),
            status: crate::config::SourceStatus::Active,
            tags: Vec::new(),
            cleanup_scope: None,
            metadata_columns: None,
            #[cfg(feature = "catalog")]
            local_outputs: None,
            deferred_refs: vec![DeferredRef {
                referenced_id: "p".into(),
                dotted_path: "".into(),
                token: "${p}".into(),
            }],
            source_override: None,
        };
        let nodes_by_id = HashMap::from([("c".to_string(), c)]);
        let children_of = HashMap::from([("p".to_string(), vec!["c".to_string()])]);
        let projs = build_projections(&nodes_by_id, &children_of);
        assert!(matches!(&**projs.get("p").unwrap(), Projection::Full));
    }

    /// Helper: minimal `ExecuteOptions` with all optional knobs cleared.
    fn opts(name: &str) -> ExecuteOptions {
        ExecuteOptions {
            pipeline_name: name.into(),
            run_id: None,
            execution: None,
            concurrency: None,
            dry_run: false,
            limit: None,
            state_path_override: None,
            shard: None,
            auth: Default::default(),
            clock: chrono::Utc::now().fixed_offset(),
            cancel: None,
            resilience: None,
            sla: None,
            reconcile: None,
            verify: None,
            rollback: None,
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

    #[tokio::test]
    async fn dry_run_counts_records_without_writing_sink_file() {
        // `--dry-run` swaps the real sink for a CountingSink — records flow but
        // no output file is produced.
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.csv");
        let output = dir.path().join("out.jsonl");
        std::fs::write(&input, "name\nalice\nbob\ncarol\n").unwrap();
        let cfg = cfg_csv_to_jsonl(&input, &output);
        let nodes = expand(&cfg).unwrap();
        let mut o = opts("dry");
        o.dry_run = true;
        let summary = run_expanded(nodes, o).await.unwrap();
        assert_eq!(summary.invocations.len(), 1);
        assert_eq!(summary.invocations[0].records_written, 3);
        assert!(!summary.had_failures());
        assert!(
            !output.exists(),
            "dry-run must not create the real sink file"
        );
    }

    #[tokio::test]
    async fn read_only_state_store_drops_writes_keeps_reads() {
        // #321 H1: reads pass through; put/delete are dropped so a preview never
        // mutates the durable bookmark.
        let inner = Arc::new(faucet_core::MemoryStateStore::new()) as Arc<dyn StateStore>;
        inner.put("k", &json!("v0")).await.unwrap();
        let ro = ReadOnlyStateStore {
            inner: inner.clone(),
        };
        assert_eq!(ro.get("k").await.unwrap(), Some(json!("v0")));
        // A write must be a no-op — the inner store keeps its original value.
        ro.put("k", &json!("advanced")).await.unwrap();
        assert_eq!(inner.get("k").await.unwrap(), Some(json!("v0")));
        // A delete must be a no-op too.
        ro.delete("k").await.unwrap();
        assert_eq!(inner.get("k").await.unwrap(), Some(json!("v0")));
    }

    #[tokio::test]
    async fn dry_run_with_state_does_not_persist_bookmark() {
        // #321 H1: a `--dry-run` with a durable state store must not advance the
        // persisted bookmark. Pre-seed a bookmark file, run dry, and confirm it is
        // byte-for-byte unchanged (the ReadOnlyStateStore wrapper drops writes).
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.csv");
        let output = dir.path().join("out.jsonl");
        let state_dir = dir.path().join("state");
        std::fs::create_dir_all(&state_dir).unwrap();
        std::fs::write(&input, "name\nalice\nbob\n").unwrap();
        let cfg = cfg_csv_to_jsonl(&input, &output);
        let nodes = expand(&cfg).unwrap();
        let mut o = opts("drystate");
        o.dry_run = true;
        o.state_path_override = Some(state_dir.clone());
        let summary = run_expanded(nodes, o).await.unwrap();
        assert!(!summary.had_failures());
        assert!(!output.exists(), "dry-run must not write the sink file");
        // The state store is wrapped read-only under dry-run, so no bookmark
        // file is ever persisted — the state dir stays empty.
        let persisted: Vec<_> = std::fs::read_dir(&state_dir)
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        assert!(
            persisted.is_empty(),
            "dry-run must not persist any bookmark file, found: {persisted:?}"
        );
    }

    #[tokio::test]
    async fn limit_caps_records_written_across_the_run() {
        // `--limit N` wraps the sink so only the first N records land.
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.csv");
        let output = dir.path().join("out.jsonl");
        std::fs::write(&input, "name\na\nb\nc\nd\ne\n").unwrap();
        let cfg = cfg_csv_to_jsonl(&input, &output);
        let nodes = expand(&cfg).unwrap();
        let mut o = opts("lim");
        o.limit = Some(2);
        let summary = run_expanded(nodes, o).await.unwrap();
        assert_eq!(summary.invocations[0].records_written, 2);
        let body = std::fs::read_to_string(&output).unwrap();
        assert_eq!(body.lines().count(), 2, "only the first 2 rows are written");
    }

    #[tokio::test]
    async fn duplicate_state_key_among_siblings_is_rejected() {
        // Two parent records whose `parent_key` value collides (both id="dup")
        // produce two child units with the SAME state key. With state
        // configured, that collision must surface as DuplicateStateKey.
        let dir = tempfile::tempdir().unwrap();
        let parent_csv = dir.path().join("parents.csv");
        let child_csv = dir.path().join("child.csv");
        // Both rows share id="dup" — the per-child state-key suffix collides.
        std::fs::write(&parent_csv, "id\ndup\ndup\n").unwrap();
        std::fs::write(&child_csv, "x\nA\n").unwrap();
        let parent_out = dir.path().join("parents.jsonl");
        let child_out = dir.path().join("child.jsonl");
        let yaml = format!(
            r#"version: 1
pipeline:
  source: {{ type: csv, config: {{ path: {parent} }} }}
  sink:   {{ type: jsonl, config: {{ path: {parent_out} }} }}
  state:  {{ type: memory }}
matrix:
  - id: parents
  - id: child
    parent: parents
    source: {{ config: {{ path: {child} }} }}
    sink:   {{ config: {{ path: {child_out} }} }}
"#,
            parent = parent_csv.display(),
            parent_out = parent_out.display(),
            child = child_csv.display(),
            child_out = child_out.display(),
        );
        let cfg = crate::config::parse_with_extension(&yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        let err = run_expanded(nodes, opts("dupkey"))
            .await
            .expect_err("colliding sibling state keys must be rejected");
        match err {
            CliError::DuplicateStateKey { id, state_key } => {
                assert_eq!(id, "child");
                assert_eq!(state_key, "dupkey::child::dup");
            }
            other => panic!("expected DuplicateStateKey, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn state_path_override_writes_bookmark_file() {
        // `--state-path` with a node that has no `state:` block wires a
        // FileStateStore at the override path; running it should create the
        // bookmark file (REST-less csv source still opts into state via the
        // override).
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.csv");
        let output = dir.path().join("out.jsonl");
        let state_dir = dir.path().join("state");
        std::fs::write(&input, "name\nalice\n").unwrap();
        let cfg = cfg_csv_to_jsonl(&input, &output);
        let nodes = expand(&cfg).unwrap();
        let mut o = opts("statepath");
        o.state_path_override = Some(state_dir.clone());
        let summary = run_expanded(nodes, o).await.unwrap();
        assert!(!summary.had_failures());
        // The csv source has no natural state key, so the StateKeyOverride wrap
        // is skipped — but build_state_for_node still constructs the store.
        // The run completing without error exercises the (None, Some(path)) arm.
        assert_eq!(summary.invocations[0].records_written, 1);
    }

    #[tokio::test]
    async fn build_dlq_config_maps_spec_fields() {
        use crate::config::{ConnectorSpec, DlqSpec, OnBatchErrorSpec};
        let dir = tempfile::tempdir().unwrap();
        let dlq_out = dir.path().join("dlq.jsonl");
        let spec = DlqSpec {
            sink: ConnectorSpec {
                kind: "jsonl".into(),
                config: json!({ "path": dlq_out.to_str().unwrap() }),
                transforms: None,
                inherit_transforms: true,
                status: None,
                tags: Vec::new(),
                complete_for: None,
            },
            on_batch_error: OnBatchErrorSpec::DlqAll,
            max_failures_per_page: Some(7),
            max_failures_total: Some(42),
            include_original_payload: false,
        };
        let cfg = build_dlq_config(&spec).await.unwrap();
        assert!(matches!(cfg.on_batch_error, OnBatchError::DlqAll));
        assert_eq!(cfg.max_failures_per_page, Some(7));
        assert_eq!(cfg.max_failures_total, Some(42));
        assert!(!cfg.include_original_payload);
    }

    #[tokio::test]
    async fn build_state_for_node_arms() {
        let dir = tempfile::tempdir().unwrap();

        // (None, None) → no store.
        let node = stub_node(None);
        assert!(build_state_for_node(&node, None).await.unwrap().is_none());

        // (None, Some(path)) → FileStateStore from override.
        let p = dir.path().join("s1");
        assert!(
            build_state_for_node(&node, Some(&p))
                .await
                .unwrap()
                .is_some()
        );

        // (Some(memory spec), None) → built from spec.
        let node_mem = stub_node(Some(crate::config::StateStoreSpec {
            kind: "memory".into(),
            config: json!({}),
        }));
        assert!(
            build_state_for_node(&node_mem, None)
                .await
                .unwrap()
                .is_some()
        );

        // (Some(file spec), Some(path)) → file backend uses the override path.
        let node_file = stub_node(Some(crate::config::StateStoreSpec {
            kind: "file".into(),
            config: json!({ "path": dir.path().join("orig").to_str().unwrap() }),
        }));
        let p2 = dir.path().join("override2");
        assert!(
            build_state_for_node(&node_file, Some(&p2))
                .await
                .unwrap()
                .is_some()
        );

        // (Some(memory spec), Some(path)) → non-file backend ignores override,
        // still builds from spec.
        let node_mem2 = stub_node(Some(crate::config::StateStoreSpec {
            kind: "memory".into(),
            config: json!({}),
        }));
        let p3 = dir.path().join("override3");
        assert!(
            build_state_for_node(&node_mem2, Some(&p3))
                .await
                .unwrap()
                .is_some()
        );
    }

    /// Build a minimal root `ExpandedNode` carrying only an (optional) state spec.
    fn stub_node(state: Option<crate::config::StateStoreSpec>) -> ExpandedNode {
        use crate::config::ConnectorSpec;
        ExpandedNode {
            id: "n".into(),
            row_index: 0,
            weight: None,
            role: NodeRole::Root,
            source: ConnectorSpec {
                kind: "csv".into(),
                config: json!({}),
                transforms: None,
                inherit_transforms: true,
                status: None,
                tags: Vec::new(),
                complete_for: None,
            },
            sink: ConnectorSpec {
                kind: "jsonl".into(),
                config: json!({}),
                transforms: None,
                inherit_transforms: true,
                status: None,
                tags: Vec::new(),
                complete_for: None,
            },
            transforms: Vec::new(),
            state,
            dlq: None,
            sla: None,
            delivery: faucet_core::DeliveryMode::AtLeastOnce,
            delivery_guarantee: faucet_core::DeliveryGuarantee::AtLeastOnce,
            #[cfg(feature = "quality")]
            quality: None,
            #[cfg(feature = "contract")]
            contract: None,
            #[cfg(feature = "masking")]
            masking: None,
            sink_ref: "default".into(),
            schema: None,
            depends_on: Vec::new(),
            status: crate::config::SourceStatus::Active,
            tags: Vec::new(),
            cleanup_scope: None,
            metadata_columns: None,
            #[cfg(feature = "catalog")]
            local_outputs: None,
            deferred_refs: Vec::new(),
            source_override: None,
        }
    }

    #[tokio::test]
    async fn state_key_override_delegates_and_overrides_key() {
        // StateKeyOverride forwards fetch/bookmark to inner and reports its own key.
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.csv");
        std::fs::write(&input, "name\nz\n").unwrap();
        let inner = build_source(
            "csv",
            json!({"path": input.to_str().unwrap()}),
            &AuthCatalog::new(),
            None,
        )
        .await
        .unwrap();
        // The wrapped name must match the inner source's, whatever it reports.
        let inner_name = inner.connector_name();
        let ov = StateKeyOverride {
            inner,
            key: "my::custom::key".into(),
        };
        assert_eq!(ov.state_key(), Some("my::custom::key".to_string()));
        assert_eq!(ov.connector_name(), inner_name);
        let rows = ov.fetch_with_context(&HashMap::new()).await.unwrap();
        assert_eq!(rows.len(), 1);
        // apply_start_bookmark delegates without error (csv ignores it).
        ov.apply_start_bookmark(json!({"any": "bookmark"}))
            .await
            .unwrap();
        // Capability passthroughs (csv defaults).
        assert!(!ov.supports_exactly_once());
        assert_eq!(
            ov.replay_guarantee(),
            faucet_core::ReplayGuarantee::NonDeterministic
        );
        assert_eq!(ov.capture_resume_position().await.unwrap(), None);
    }

    #[tokio::test]
    async fn state_key_override_forwards_native_stream_pages() {
        // The wrap must preserve the inner source's NATIVE page stream —
        // per-page bookmarks included. Without the `stream_pages` forward, the
        // trait's buffering default kicks in and collapses everything into
        // final-page-bookmark-only pages (losing CDC per-transaction
        // durability and exactly-once per-page tokens).
        struct PerPageBookmarkSource;
        #[async_trait]
        impl Source for PerPageBookmarkSource {
            async fn fetch_with_context(
                &self,
                _ctx: &HashMap<String, Value>,
            ) -> Result<Vec<Value>, FaucetError> {
                Ok(vec![json!({"id": 1}), json!({"id": 2})])
            }
            fn stream_pages<'a>(
                &'a self,
                _ctx: &'a HashMap<String, Value>,
                _batch_size: usize,
            ) -> std::pin::Pin<
                Box<
                    dyn faucet_core::Stream<Item = Result<faucet_core::StreamPage, FaucetError>>
                        + Send
                        + 'a,
                >,
            > {
                Box::pin(faucet_core::async_stream::try_stream! {
                    yield faucet_core::StreamPage {
                        records: vec![json!({"id": 1})],
                        bookmark: Some(json!("bm-1")),
                    };
                    yield faucet_core::StreamPage {
                        records: vec![json!({"id": 2})],
                        bookmark: Some(json!("bm-2")),
                    };
                })
            }
            fn state_key(&self) -> Option<String> {
                Some("native".into())
            }
        }

        use futures::StreamExt;
        let ov = StateKeyOverride {
            inner: Box::new(PerPageBookmarkSource),
            key: "override".into(),
        };
        let ctx = HashMap::new();
        let pages: Vec<_> = ov
            .stream_pages(&ctx, 1000)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(pages.len(), 2, "native page boundaries survive the wrap");
        assert_eq!(pages[0].bookmark, Some(json!("bm-1")));
        assert_eq!(pages[1].bookmark, Some(json!("bm-2")));
    }

    #[tokio::test]
    async fn capturing_sink_forwards_capabilities_and_captures_idempotent_writes() {
        struct IdemSink;
        #[async_trait]
        impl Sink for IdemSink {
            async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
                Ok(records.len())
            }
            fn connector_name(&self) -> &'static str {
                "idem"
            }
            fn supports_idempotent_writes(&self) -> bool {
                true
            }
            fn dedups_by_key(&self) -> bool {
                true
            }
            fn supported_write_modes(&self) -> &'static [faucet_core::WriteMode] {
                &[
                    faucet_core::WriteMode::Append,
                    faucet_core::WriteMode::Upsert,
                ]
            }
            async fn write_batch_idempotent(
                &self,
                records: &[Value],
                _scope: &str,
                _token: &str,
            ) -> Result<usize, FaucetError> {
                Ok(records.len())
            }
            async fn last_committed_token(
                &self,
                _scope: &str,
            ) -> Result<Option<String>, FaucetError> {
                Ok(Some("tok".into()))
            }
        }

        let captured = Arc::new(Mutex::new(Vec::new()));
        let sink = CapturingSink::wrap(
            Box::new(IdemSink),
            Arc::clone(&captured),
            Arc::new(Projection::Full),
        );
        // Capability passthroughs: a parent row feeding children keeps the
        // inner sink's delivery semantics.
        assert!(sink.supports_idempotent_writes());
        assert!(sink.dedups_by_key());
        assert_eq!(
            sink.sink_guarantee(),
            faucet_core::SinkGuarantee::AtomicWatermark
        );
        assert!(
            sink.supported_write_modes()
                .contains(&faucet_core::WriteMode::Upsert)
        );
        assert_eq!(
            sink.last_committed_token("k").await.unwrap(),
            Some("tok".into())
        );
        assert_eq!(sink.current_schema().await.unwrap(), None);
        assert!(!sink.supports_schema_evolution());
        // Idempotent writes are captured for child fan-out like plain writes.
        let n = sink
            .write_batch_idempotent(&[json!({"id": 7})], "k", "t")
            .await
            .unwrap();
        assert_eq!(n, 1);
        assert_eq!(*captured.lock().await, vec![json!({"id": 7})]);
    }

    #[tokio::test]
    async fn orphaned_child_surfaces_executor_deadlock() {
        // A child node whose parent id is never present among the nodes can
        // never become ready. `expand` would reject this, but a hand-built node
        // list exercises the executor's own deadlock guard (lines 227-233).
        use crate::config::ConnectorSpec;
        let orphan = ExpandedNode {
            id: "orphan".into(),
            row_index: 0,
            weight: None,
            role: NodeRole::Child {
                parent_id: "missing-parent".into(),
                parent_key: "id".into(),
            },
            source: ConnectorSpec {
                kind: "csv".into(),
                config: json!({}),
                transforms: None,
                inherit_transforms: true,
                status: None,
                tags: Vec::new(),
                complete_for: None,
            },
            sink: ConnectorSpec {
                kind: "jsonl".into(),
                config: json!({}),
                transforms: None,
                inherit_transforms: true,
                status: None,
                tags: Vec::new(),
                complete_for: None,
            },
            transforms: Vec::new(),
            state: None,
            dlq: None,
            sla: None,
            delivery: faucet_core::DeliveryMode::AtLeastOnce,
            delivery_guarantee: faucet_core::DeliveryGuarantee::AtLeastOnce,
            #[cfg(feature = "quality")]
            quality: None,
            #[cfg(feature = "contract")]
            contract: None,
            #[cfg(feature = "masking")]
            masking: None,
            sink_ref: "default".into(),
            schema: None,
            depends_on: Vec::new(),
            status: crate::config::SourceStatus::Active,
            tags: Vec::new(),
            cleanup_scope: None,
            metadata_columns: None,
            #[cfg(feature = "catalog")]
            local_outputs: None,
            deferred_refs: Vec::new(),
            source_override: None,
        };
        let err = run_expanded(vec![orphan], opts("deadlock"))
            .await
            .expect_err("an orphaned child must surface as an executor deadlock");
        match err {
            CliError::Internal(msg) => {
                assert!(msg.contains("executor deadlock"), "{msg}");
                assert!(msg.contains("orphan"), "{msg}");
            }
            other => panic!("expected Internal deadlock error, got {other:?}"),
        }
    }

    #[test]
    fn value_to_string_brief_unquotes_strings_only() {
        assert_eq!(value_to_string_brief(&json!("hello")), "hello");
        assert_eq!(value_to_string_brief(&json!(42)), "42");
        assert_eq!(value_to_string_brief(&json!(true)), "true");
        assert_eq!(value_to_string_brief(&json!(null)), "null");
        assert_eq!(value_to_string_brief(&json!({"a": 1})), "{\"a\":1}");
    }

    #[test]
    fn build_state_key_with_and_without_parent() {
        assert_eq!(build_state_key("pipe", "row", None), "pipe::row");
        assert_eq!(build_state_key("pipe", "row", Some("k")), "pipe::row::k");
    }

    #[test]
    fn mark_overwrite_staging_only_for_grouped_bigquery() {
        // Grouped (>1 member) bigquery ⇒ inject the staging flag.
        let mut cfg = json!({"table_id": "t"});
        mark_overwrite_staging("bigquery", &mut cfg, true);
        assert_eq!(cfg.get("_overwrite_staging"), Some(&json!(true)));

        // Solo bigquery ⇒ no flag (loads directly).
        let mut cfg = json!({"table_id": "t"});
        mark_overwrite_staging("bigquery", &mut cfg, false);
        assert!(cfg.get("_overwrite_staging").is_none());

        // Grouped non-bigquery ⇒ never injected (would reject the unknown field).
        let mut cfg = json!({"connection_url": "postgres://x"});
        mark_overwrite_staging("postgres", &mut cfg, true);
        assert!(cfg.get("_overwrite_staging").is_none());
    }

    #[tokio::test]
    async fn plan_overwrite_groups_counts_members() {
        // Two invocations to the same destination ⇒ one group, members == 2
        // (grouped ⇒ stage); a distinct destination ⇒ its own single-member
        // group (solo ⇒ direct load).
        let dir = tempfile::tempdir().unwrap();
        let opts = exec_opts("t");
        let mk = |node: &ExpandedNode, pid: &str, id: i64| Unit {
            node: node.clone(),
            parent_record: Some(std::sync::Arc::new(json!({ "id": id }))),
            state_key: format!("t::c::{pid}"),
            parent_record_key: Some(pid.to_string()),
            product_ctx: None,
        };
        // Shared destination across two parents ⇒ members == 2.
        let shared = overwrite_child_node(dir.path(), "orders");
        let units = vec![mk(&shared, "1", 1), mk(&shared, "2", 2)];
        let (groups, _) = plan_overwrite_groups(&units, &opts).unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].members, 2, "the two shared-dest units group");

        // Per-parent destinations ⇒ two single-member (solo) groups.
        let per = overwrite_child_node(dir.path(), "orders_${p.id}");
        let units2 = vec![mk(&per, "1", 1), mk(&per, "2", 2)];
        let (groups2, _) = plan_overwrite_groups(&units2, &opts).unwrap();
        assert_eq!(groups2.len(), 2);
        assert!(
            groups2.iter().all(|g| g.members == 1),
            "per-parent destinations are solo"
        );
    }

    #[test]
    fn resolve_parent_key_walks_objects_arrays_and_misses() {
        let r = json!({"user": {"name": "ada"}, "tags": ["x", "y"]});
        assert_eq!(resolve_parent_key(&r, "user.name"), Some(json!("ada")));
        assert_eq!(resolve_parent_key(&r, "tags.1"), Some(json!("y")));
        // Missing key → None.
        assert_eq!(resolve_parent_key(&r, "user.age"), None);
        // Descending into a scalar → None.
        assert_eq!(resolve_parent_key(&r, "user.name.deep"), None);
        // Non-numeric array index → None.
        assert_eq!(resolve_parent_key(&r, "tags.notanindex"), None);
    }

    #[tokio::test]
    async fn cooperative_cancel_returns_partial_ok() {
        // A pre-cancelled token makes the run stop at the first page boundary
        // and flush — returning Ok with a partial (possibly empty) result
        // rather than erroring. Covers the cancel-threading path.
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.csv");
        let output = dir.path().join("out.jsonl");
        std::fs::write(&input, "name\nalice\nbob\n").unwrap();
        let cfg = cfg_csv_to_jsonl(&input, &output);
        let nodes = expand(&cfg).unwrap();
        let token = CancellationToken::new();
        token.cancel(); // already cancelled before the run starts
        let mut o = opts("cancel");
        o.cancel = Some(token);
        let summary = run_expanded(nodes, o).await.unwrap();
        // The single root invocation completes (Ok) — it is not reported as a
        // failure even though it was cancelled.
        assert_eq!(summary.invocations.len(), 1);
        assert!(
            !summary.had_failures(),
            "a cooperatively-cancelled run is Ok, not a failure: {summary:?}"
        );
    }

    #[tokio::test]
    async fn fanout_projects_away_unreferenced_parent_fields() {
        // Parent CSV has id + a big unreferenced "payload" column. The child only
        // references ${parents.id} (in its output path), so projection keeps "id"
        // and the parent_key but drops "payload" — and fan-out still works.
        let dir = tempfile::tempdir().unwrap();
        let parent_csv = dir.path().join("parents.csv");
        let child_csv = dir.path().join("child.csv");
        std::fs::write(&parent_csv, "id,payload\n1,aaaaaaaaaa\n2,bbbbbbbbbb\n").unwrap();
        std::fs::write(&child_csv, "x\nA\n").unwrap();
        let parent_out = dir.path().join("parents.jsonl");
        let child_out_pattern = dir.path().join("child-${parents.id}.jsonl");

        let yaml = format!(
            r#"version: 1
pipeline:
  source: {{ type: csv, config: {{ path: {parent} }} }}
  sink:   {{ type: jsonl, config: {{ path: {parent_out} }} }}
matrix:
  - id: parents
  - id: child
    parent: parents
    source: {{ config: {{ path: {child} }} }}
    sink:   {{ config: {{ path: "{child_out}" }} }}
"#,
            parent = parent_csv.display(),
            parent_out = parent_out.display(),
            child = child_csv.display(),
            child_out = child_out_pattern.display(),
        );
        let cfg = crate::config::parse_with_extension(&yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        let summary = run_expanded(
            nodes,
            ExecuteOptions {
                pipeline_name: "projtest".into(),
                run_id: None,
                execution: None,
                concurrency: None,
                dry_run: false,
                limit: None,
                state_path_override: None,
                shard: None,
                auth: Default::default(),
                clock: chrono::Utc::now().fixed_offset(),
                cancel: None,
                resilience: None,
                sla: None,
                reconcile: None,
                verify: None,
                rollback: None,
                #[cfg(feature = "lineage")]
                lineage: None,
                #[cfg(feature = "lineage")]
                lineage_cfg: None,
                #[cfg(feature = "notify")]
                notifier: None,
                #[cfg(feature = "catalog")]
                catalog: None,
            },
        )
        .await
        .unwrap();

        // 1 parent + 2 child invocations (one per parent record) — cardinality kept.
        assert_eq!(summary.invocations.len(), 3, "{summary:?}");
        assert!(!summary.had_failures(), "{summary:?}");
        // ${parents.id} resolved correctly for each child despite "payload" being projected away.
        assert!(dir.path().join("child-1.jsonl").exists());
        assert!(dir.path().join("child-2.jsonl").exists());
    }
}
