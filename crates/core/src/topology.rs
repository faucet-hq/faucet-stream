//! Multi-edge pipeline topology — fan-out (tee), fan-in (merge), and
//! hash-join over an explicit node graph (issues #71 and #72).
//!
//! The single-source→single-sink [`Pipeline`](crate::Pipeline) covers the
//! common case. A [`Topology`] generalizes it to a directed acyclic graph of
//! typed nodes connected by edges, so one run can *tee* a source's records to
//! several sinks, *merge* several sources into one sink, or *join* two
//! upstreams by key. It is the in-process primitive behind the CLI's
//! `pipeline.nodes` / `edges` topology mode.
//!
//! ## Node kinds
//!
//! | Kind | In | Out | Semantics |
//! |------|----|-----|-----------|
//! | [`NodeKind::Source`] | 0 | 1 | Drives [`Source::stream_pages`]. |
//! | [`NodeKind::Transform`] | 1 | 1 | Applies compiled transform stages per page. |
//! | [`NodeKind::Tee`] | 1 | N | Clones each page to every downstream edge. |
//! | [`NodeKind::Merge`] | N | 1 | Forwards pages from all inputs in arrival order. |
//! | [`NodeKind::Join`] | 2 | 1 | Hash-join: buffer the build edge, enrich the probe edge. |
//! | [`NodeKind::Sink`] | 1 | 0 | Drives [`run_stream`] (write → flush → persist). |
//!
//! ## Execution
//!
//! Each node runs as a cooperatively-scheduled future; edges are bounded
//! [`tokio::sync::mpsc`] channels so the slowest consumer paces its producer
//! (backpressure). No OS threads are spawned — the topology runs on whatever
//! runtime drives [`Topology::run`], overlapping the nodes' I/O. Sink nodes
//! reuse [`run_stream`], so DLQ routing, bookmark persistence, and the full
//! observability metric set come for free.
//!
//! ## State
//!
//! Each terminal sink owns its bookmark under `{pipeline}::{node_id}`. On
//! restart the source resumes from the **minimum** across every sink's stored
//! bookmark (so the slowest sink catches up), applied only when *every* sink
//! has a stored bookmark; otherwise the source replays in full. Sinks whose
//! bookmarks have diverged must therefore be idempotent — a faster sink will
//! re-see already-written pages.
//!
//! Resuming is deliberately conservative, because the only safe direction to err
//! is *replay* (duplicates) and never *skip* (loss) — see [`start_bookmark`]:
//!
//! - **One source node only.** With two or more sources there is no way to tell
//!   which source a given sink's bookmark came from, so applying one to all of
//!   them would resume a source at a position that is not its own. Multi-source
//!   graphs therefore replay in full.
//! - **Comparable, agreeing bookmarks only.** Sink bookmarks are compared for
//!   equality, not ordered. Resume positions are frequently structured (CDC LSN
//!   maps, Kafka offset maps), and [`json_gt`](crate::replication::json_gt)'s
//!   object arm orders by *serialized
//!   text*, which is not the replication order — so a "minimum" picked that way
//!   can sit ahead of the true minimum and skip records. Divergent bookmarks
//!   therefore replay in full rather than guess.
//!
//! ## Governance passes
//!
//! Sink nodes reuse [`run_stream`], so the masking / quality / contract /
//! schema-drift passes and the resilience policy apply exactly as they do to a
//! single-source pipeline — supply them via
//! [`Topology::run_with`]. Masking is destination-scoped and is
//! therefore keyed by sink node id.

use crate::dlq::DlqConfig;
use crate::error::FaucetError;
use crate::join::HashJoin;
use crate::observability::{Labels, RunStreamOptions, instrumented_apply_stages};
use crate::pipeline::{DEFAULT_BATCH_SIZE, StreamPage, run_stream};
use crate::stage::CompiledStage;
use crate::state::StateStore;
use crate::traits::{Sink, Source};
use futures::StreamExt;
use metrics::{Label, SharedString, counter, histogram};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub use crate::join::{JoinConfig, JoinMode, KeyNormalize, OnCollision, OnDuplicate, Projection};

/// Default bounded-channel capacity for topology edges.
pub const DEFAULT_CHANNEL_CAPACITY: usize = 4;

/// A join node: the pure [`JoinConfig`] plus the labels of its two incoming
/// edges identifying which upstream is the build (right) side and which is the
/// probe (left) side.
#[derive(Debug, Clone)]
pub struct JoinNode {
    /// Pure join logic configuration.
    pub config: JoinConfig,
    /// Label of the incoming edge feeding the build (right) side.
    pub build_edge: String,
    /// Label of the incoming edge feeding the probe (left) side.
    pub probe_edge: String,
}

/// A typed topology node.
pub enum NodeKind {
    /// A data source (0 in, 1 out).
    Source(Box<dyn Source>),
    /// Transform stages applied per page (1 in, 1 out).
    Transform(Vec<CompiledStage>),
    /// Fan-out: clone each page to every downstream edge (1 in, N out).
    Tee {
        /// Bounded-channel capacity for each outgoing edge.
        capacity: usize,
        /// Optional expected fan-out (outgoing edge count) sanity check.
        fanout: Option<usize>,
    },
    /// Fan-in: forward pages from all inputs in arrival order (N in, 1 out).
    Merge,
    /// Hash-join two upstreams by key (2 in, 1 out).
    Join(JoinNode),
    /// A data sink (1 in, 0 out).
    Sink(Box<dyn Sink>),
}

impl std::fmt::Debug for NodeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.kind_str())
    }
}

impl NodeKind {
    /// Short name of this node kind, used in errors and metric labels.
    pub fn kind_str(&self) -> &'static str {
        match self {
            NodeKind::Source(_) => "source",
            NodeKind::Transform(_) => "transform",
            NodeKind::Tee { .. } => "tee",
            NodeKind::Merge => "merge",
            NodeKind::Join(_) => "join",
            NodeKind::Sink(_) => "sink",
        }
    }

    fn is_source(&self) -> bool {
        matches!(self, NodeKind::Source(_))
    }

    fn is_sink(&self) -> bool {
        matches!(self, NodeKind::Sink(_))
    }
}

/// A node in the topology: a stable id plus its typed kind.
#[derive(Debug)]
pub struct Node {
    /// Stable node id (used as the metric `node` label and state-key suffix).
    pub id: String,
    /// The node's kind.
    pub kind: NodeKind,
}

/// A directed edge from one node's output to another's input.
#[derive(Debug, Clone)]
pub struct Edge {
    /// Producer node id.
    pub from: String,
    /// Consumer node id.
    pub to: String,
    /// Optional edge label, used by [`NodeKind::Join`] to distinguish its
    /// build edge from its probe edge.
    pub label: Option<String>,
}

/// What to do when a node fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TopologyOnError {
    /// Abort the whole topology on the first node failure (default).
    #[default]
    Propagate,
    /// Let every node run to completion; collect and report failures without
    /// aborting healthy branches.
    Continue,
}

/// The per-run governance passes applied to every sink node.
///
/// These are the same passes [`crate::Pipeline`] applies in matrix mode; a
/// topology wires them through [`run_stream`] per sink node so a graph pipeline
/// gets identical enforcement. Masking is destination-scoped (a rule may name
/// the sinks it applies to), so it is keyed by **sink node id** and compiled by
/// the caller; the rest are pipeline-wide.
///
/// `#[non_exhaustive]`: construct with [`TopologyGovernance::new`] (or
/// `Default`) and assign the fields you need. This is deliberate — adding a pass
/// later would otherwise be a major-version break for every downstream crate,
/// which is exactly the trap `TopologyOptions` is already in.
#[derive(Clone, Default)]
#[non_exhaustive]
pub struct TopologyGovernance {
    /// Compiled data-quality checks, applied per page in every sink node.
    #[cfg(feature = "quality")]
    pub quality: Option<Arc<crate::quality::CompiledQuality>>,
    /// Compiled data contract, applied per page in every sink node.
    #[cfg(feature = "contract")]
    pub contract: Option<Arc<crate::contract::CompiledContract>>,
    /// Compiled masking policy per sink node id. A sink node with no entry runs
    /// no masking pass (the caller found no rule that applies to it).
    #[cfg(feature = "masking")]
    pub masking_by_sink: HashMap<String, Arc<crate::masking::CompiledMasking>>,
    /// Compiled schema-drift policy, applied per page in every sink node.
    pub schema_drift: Option<crate::drift::SchemaDriftPolicy>,
    /// Resilience policy (retry / circuit breaker / poison) for sink-side
    /// writes, flushes, and state puts.
    pub resilience: Option<crate::resilience::ResiliencePolicy>,
    /// Delivery guarantee for every sink node (#458).
    ///
    /// `ExactlyOnce` gives each sink node its own commit-token scope — its state
    /// key, `{pipeline}::{node_id}` — so a sink that durably committed a page
    /// skips it on resume independently of its siblings. Lives here rather than on
    /// [`TopologyOptions`] because that struct is exhaustively constructible
    /// through the public API, so a new field there is a major break; this one is
    /// `#[non_exhaustive]`. It is a per-sink write policy either way.
    ///
    /// The caller is responsible for the gate (deterministic-replay source,
    /// idempotent sinks, durable state, no DLQ) — `run_stream` re-checks the sink
    /// side and downgrades with a warning rather than pretending.
    pub delivery: crate::idempotency::DeliveryMode,
}

impl TopologyGovernance {
    /// A governance set with no passes configured.
    pub fn new() -> Self {
        Self::default()
    }
}

/// Per-run options for [`Topology::run`].
#[derive(Clone)]
pub struct TopologyOptions {
    /// Pipeline name (metric `pipeline` label).
    pub pipeline_name: String,
    /// Run id (span attribute).
    pub run_id: String,
    /// Batch-size hint passed to source nodes' `stream_pages`.
    pub batch_size: usize,
    /// State store shared by every sink node (each under `{pipeline}::{node_id}`).
    pub state_store: Option<Arc<dyn StateStore>>,
    /// DLQ applied to every sink node.
    pub dlq: Option<DlqConfig>,
    /// Cooperative cancellation.
    pub cancel: Option<CancellationToken>,
    /// Failure policy.
    pub on_error: TopologyOnError,
    /// Default bounded-channel capacity for edges not fed by a tee.
    pub default_channel_capacity: usize,
}

/// How long a node gets to stop at its next page boundary and flush after another
/// node has failed under [`TopologyOnError::Propagate`], before it is aborted.
///
/// Mirrors the CLI executor's `on_error: stop` grace: without it a buffered sink
/// is dropped mid-write, orphaning a multipart upload or leaving a footer-less
/// Parquet file (#146 H16, #456 M1). The window opens only once a failure has
/// cancelled the run, so a healthy run is never bounded by it.
pub const STOP_FLUSH_GRACE: Duration = Duration::from_secs(30);

impl Default for TopologyOptions {
    fn default() -> Self {
        Self {
            pipeline_name: "unnamed".into(),
            run_id: String::new(),
            batch_size: DEFAULT_BATCH_SIZE,
            state_store: None,
            dlq: None,
            cancel: None,
            on_error: TopologyOnError::default(),
            default_channel_capacity: DEFAULT_CHANNEL_CAPACITY,
        }
    }
}

impl TopologyOptions {
    /// New options with the given pipeline name.
    pub fn new(pipeline_name: impl Into<String>) -> Self {
        Self {
            pipeline_name: pipeline_name.into(),
            ..Default::default()
        }
    }

    /// Attach a state store.
    pub fn with_state_store(mut self, store: Arc<dyn StateStore>) -> Self {
        self.state_store = Some(store);
        self
    }

    /// Attach a DLQ applied to every sink node.
    pub fn with_dlq(mut self, dlq: DlqConfig) -> Self {
        self.dlq = Some(dlq);
        self
    }

    /// Attach a cancellation token.
    pub fn with_cancel(mut self, cancel: CancellationToken) -> Self {
        self.cancel = Some(cancel);
        self
    }

    /// Set the failure policy.
    pub fn with_on_error(mut self, on_error: TopologyOnError) -> Self {
        self.on_error = on_error;
        self
    }

    /// Set the batch-size hint.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

/// What one node did, for callers that need per-node attribution (the CLI emits
/// notifications and evaluates SLAs per **sink node**, which needs to know which
/// node failed — [`TopologyResult::errors`] is a flat list of messages).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct NodeReport {
    /// Node id.
    pub node_id: String,
    /// Node kind (`"source"`, `"sink"`, …).
    pub kind: &'static str,
    /// Records written (sink nodes only; 0 elsewhere).
    pub records: usize,
    /// Final bookmark (sink nodes only).
    pub bookmark: Option<Value>,
    /// The node's failure, if it failed.
    pub error: Option<String>,
}

/// A topology run with per-node attribution.
///
/// `#[non_exhaustive]`: this is an output callers read, never construct, so
/// keeping it open means a future per-node field is a minor release rather than
/// a breaking one — the mistake [`TopologyResult`] cannot now undo.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct TopologyRun {
    /// The aggregate result, identical to what [`Topology::run`] returns.
    pub result: TopologyResult,
    /// One entry per node, in the graph's deterministic (sorted-id) order.
    pub nodes: Vec<NodeReport>,
    /// Records emitted per **source** node, keyed by node id (#459). Lives here
    /// rather than on [`TopologyResult`] so adding it stays a minor change.
    pub per_source: HashMap<String, usize>,
}

/// Outcome of a topology run.
#[derive(Debug, Clone, Default)]
pub struct TopologyResult {
    /// Total records written across all sink nodes.
    pub records_written: usize,
    /// Per-sink-node records written, keyed by node id.
    pub per_sink: HashMap<String, usize>,
    /// Per-sink-node final bookmark, keyed by node id.
    pub bookmarks: HashMap<String, Option<Value>>,
    /// Node failures observed under [`TopologyOnError::Continue`] (empty under
    /// `Propagate`, which returns `Err` on the first failure instead).
    pub errors: Vec<String>,
}

/// One incoming edge of a node: its optional label plus the receiving end of
/// the channel.
struct InEdge {
    label: Option<String>,
    rx: mpsc::Receiver<StreamPage>,
}

/// Pop the single input receiver from a one-input node's edge list.
fn take_single(mut ins: Vec<InEdge>) -> Option<mpsc::Receiver<StreamPage>> {
    ins.drain(..).next().map(|ie| ie.rx)
}

/// Remove and return the input receiver whose edge carries `label`.
fn take_by_label(ins: &mut Vec<InEdge>, label: &str) -> Option<mpsc::Receiver<StreamPage>> {
    ins.iter()
        .position(|ie| ie.label.as_deref() == Some(label))
        .map(|pos| ins.remove(pos).rx)
}

/// What a completed node future reports back.
enum NodeOutcome {
    Sink {
        node_id: String,
        records: usize,
        bookmark: Option<Value>,
    },
    /// A source node and how many records it emitted. Needed so a lineage /
    /// catalog edge can report the volume *that input* contributed rather than
    /// the sink's total, which would over-count a merge (#459).
    Source {
        node_id: String,
        records: usize,
    },
    Other,
}

/// A directed acyclic graph of typed nodes.
///
/// Build one with [`Topology::builder`], then drive it with [`Topology::run`].
#[derive(Debug)]
pub struct Topology {
    nodes: Vec<Node>,
    edges: Vec<Edge>,
}

impl Topology {
    /// Start building a topology.
    pub fn builder() -> TopologyBuilder {
        TopologyBuilder::default()
    }

    /// The nodes, in insertion order.
    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    /// The edges, in insertion order.
    pub fn edges(&self) -> &[Edge] {
        &self.edges
    }

    /// Validate the graph: unique ids, existing endpoints, per-kind arity,
    /// tee fan-out, join edge labels, acyclicity, and source→sink
    /// reachability. Returns [`FaucetError::Config`] with a descriptive
    /// message on the first violation.
    pub fn validate(&self) -> Result<(), FaucetError> {
        if self.nodes.is_empty() {
            return Err(cfg("topology has no nodes"));
        }

        // Unique ids.
        let mut seen = HashSet::new();
        for n in &self.nodes {
            if !seen.insert(n.id.as_str()) {
                return Err(cfg(format!("duplicate node id '{}'", n.id)));
            }
        }
        let ids: HashSet<&str> = seen;

        // Edge endpoints exist.
        for e in &self.edges {
            if !ids.contains(e.from.as_str()) {
                return Err(cfg(format!(
                    "edge references unknown 'from' node '{}'",
                    e.from
                )));
            }
            if !ids.contains(e.to.as_str()) {
                return Err(cfg(format!("edge references unknown 'to' node '{}'", e.to)));
            }
        }

        // In/out degrees.
        let mut in_deg: HashMap<&str, usize> = HashMap::new();
        let mut out_deg: HashMap<&str, usize> = HashMap::new();
        for e in &self.edges {
            *out_deg.entry(e.from.as_str()).or_default() += 1;
            *in_deg.entry(e.to.as_str()).or_default() += 1;
        }

        let mut has_source = false;
        let mut has_sink = false;
        for n in &self.nodes {
            let i = in_deg.get(n.id.as_str()).copied().unwrap_or(0);
            let o = out_deg.get(n.id.as_str()).copied().unwrap_or(0);
            match &n.kind {
                NodeKind::Source(_) => {
                    has_source = true;
                    arity(&n.id, "source", i == 0, o == 1, "0 in, exactly 1 out")?;
                }
                NodeKind::Transform(_) => {
                    arity(&n.id, "transform", i == 1, o == 1, "exactly 1 in, 1 out")?;
                }
                NodeKind::Tee { fanout, .. } => {
                    arity(&n.id, "tee", i == 1, o >= 2, "exactly 1 in, 2+ out")?;
                    if let Some(f) = fanout
                        && *f != o
                    {
                        return Err(cfg(format!(
                            "tee '{}' declares fanout {f} but has {o} outgoing edges",
                            n.id
                        )));
                    }
                }
                NodeKind::Merge => {
                    arity(&n.id, "merge", i >= 2, o == 1, "2+ in, exactly 1 out")?;
                }
                NodeKind::Join(j) => {
                    arity(&n.id, "join", i == 2, o == 1, "exactly 2 in, 1 out")?;
                    self.validate_join_edges(&n.id, j)?;
                }
                NodeKind::Sink(_) => {
                    has_sink = true;
                    arity(&n.id, "sink", i == 1, o == 0, "exactly 1 in, 0 out")?;
                }
            }
        }

        if !has_source {
            return Err(cfg("topology has no source node"));
        }
        if !has_sink {
            return Err(cfg("topology has no sink node"));
        }

        self.detect_cycle()?;
        self.check_reachability()?;
        Ok(())
    }

    fn validate_join_edges(&self, node_id: &str, j: &JoinNode) -> Result<(), FaucetError> {
        let labels: Vec<&str> = self
            .edges
            .iter()
            .filter(|e| e.to == node_id)
            .filter_map(|e| e.label.as_deref())
            .collect();
        for want in [j.build_edge.as_str(), j.probe_edge.as_str()] {
            if !labels.contains(&want) {
                return Err(cfg(format!(
                    "join '{node_id}' has no incoming edge labelled '{want}' (known labels: {labels:?})"
                )));
            }
        }
        if j.build_edge == j.probe_edge {
            return Err(cfg(format!(
                "join '{node_id}' build_edge and probe_edge must differ"
            )));
        }
        Ok(())
    }

    /// DFS cycle detection (three-color).
    fn detect_cycle(&self) -> Result<(), FaucetError> {
        let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
        for e in &self.edges {
            adj.entry(e.from.as_str()).or_default().push(e.to.as_str());
        }
        #[derive(Clone, Copy, PartialEq)]
        enum Color {
            White,
            Gray,
            Black,
        }
        let mut color: HashMap<&str, Color> = self
            .nodes
            .iter()
            .map(|n| (n.id.as_str(), Color::White))
            .collect();

        // Iterative DFS to avoid stack overflow on deep graphs.
        for start in self.nodes.iter().map(|n| n.id.as_str()) {
            if color[start] != Color::White {
                continue;
            }
            let mut stack: Vec<(&str, usize)> = vec![(start, 0)];
            *color.get_mut(start).unwrap() = Color::Gray;
            while let Some((node, idx)) = stack.last().copied() {
                let neighbours = adj.get(node).map(|v| v.as_slice()).unwrap_or(&[]);
                if idx < neighbours.len() {
                    stack.last_mut().unwrap().1 += 1;
                    let next = neighbours[idx];
                    match color[next] {
                        Color::Gray => {
                            return Err(cfg(format!("topology has a cycle through node '{next}'")));
                        }
                        Color::White => {
                            *color.get_mut(next).unwrap() = Color::Gray;
                            stack.push((next, 0));
                        }
                        Color::Black => {}
                    }
                } else {
                    *color.get_mut(node).unwrap() = Color::Black;
                    stack.pop();
                }
            }
        }
        Ok(())
    }

    /// Every source must reach at least one sink, and every sink must be
    /// reachable from at least one source.
    fn check_reachability(&self) -> Result<(), FaucetError> {
        let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
        let mut radj: HashMap<&str, Vec<&str>> = HashMap::new();
        for e in &self.edges {
            adj.entry(e.from.as_str()).or_default().push(e.to.as_str());
            radj.entry(e.to.as_str()).or_default().push(e.from.as_str());
        }
        let sink_ids: HashSet<&str> = self
            .nodes
            .iter()
            .filter(|n| n.kind.is_sink())
            .map(|n| n.id.as_str())
            .collect();
        let source_ids: HashSet<&str> = self
            .nodes
            .iter()
            .filter(|n| n.kind.is_source())
            .map(|n| n.id.as_str())
            .collect();

        for src in &source_ids {
            if !reaches_any(src, &adj, &sink_ids) {
                return Err(cfg(format!("source '{src}' does not reach any sink node")));
            }
        }
        for sink in &sink_ids {
            if !reaches_any(sink, &radj, &source_ids) {
                return Err(cfg(format!(
                    "sink '{sink}' is not reachable from any source node"
                )));
            }
        }
        Ok(())
    }

    /// Run the topology to completion with no governance passes.
    ///
    /// Equivalent to [`Topology::run_with`] with a default
    /// [`TopologyGovernance`] — kept as-is so existing callers are unaffected.
    pub async fn run(self, opts: TopologyOptions) -> Result<TopologyResult, FaucetError> {
        self.run_with(opts, TopologyGovernance::default()).await
    }

    /// Run the topology to completion, applying `governance` to every sink node.
    ///
    /// Separate from [`Topology::run`] rather than a field on
    /// [`TopologyOptions`]: that struct is exhaustively constructible through the
    /// public API, so adding a field to it would be a major-version break for
    /// every downstream crate. A new method is additive.
    pub async fn run_with(
        self,
        opts: TopologyOptions,
        governance: TopologyGovernance,
    ) -> Result<TopologyResult, FaucetError> {
        self.run_reported(opts, governance).await.map(|r| r.result)
    }

    /// [`Topology::run_with`] with **per-node attribution**.
    ///
    /// The CLI emits notifications and evaluates SLAs per *sink node*, which needs
    /// to know which node failed; [`TopologyResult::errors`] is only a flat list
    /// of messages. Additive rather than a change to `run_with`'s return type,
    /// which would break every caller (#459).
    pub async fn run_reported(
        self,
        opts: TopologyOptions,
        governance: TopologyGovernance,
    ) -> Result<TopologyRun, FaucetError> {
        self.validate()?;
        let Topology { nodes, edges } = self;

        // Capacity per outgoing edge: a tee's edges use its configured
        // capacity; everything else uses the default.
        let tee_cap: HashMap<&str, usize> = nodes
            .iter()
            .filter_map(|n| match &n.kind {
                NodeKind::Tee { capacity, .. } => Some((n.id.as_str(), *capacity)),
                _ => None,
            })
            .collect();

        // Resume point for the source node(s) — only when provably safe (see
        // `start_bookmark`); otherwise every source replays in full.
        let sink_ids: Vec<String> = nodes
            .iter()
            .filter(|n| n.kind.is_sink())
            .map(|n| n.id.clone())
            .collect();
        let source_count = nodes.iter().filter(|n| n.kind.is_source()).count();
        // The graph's source replay capability, captured before the sources are
        // moved into their futures. Only meaningful with exactly one source —
        // which is also the only shape exactly-once is allowed in (#458).
        let source_replay = if source_count == 1 {
            nodes.iter().find_map(|n| match &n.kind {
                NodeKind::Source(src) => Some(src.replay_guarantee()),
                _ => None,
            })
        } else {
            None
        };
        let start_bookmark =
            compute_start_bookmark(&opts, &sink_ids, source_count, governance.delivery).await;

        // Build channels.
        let mut outs: HashMap<String, Vec<mpsc::Sender<StreamPage>>> = HashMap::new();
        let mut ins: HashMap<String, Vec<InEdge>> = HashMap::new();
        for e in &edges {
            let cap = tee_cap
                .get(e.from.as_str())
                .copied()
                .unwrap_or(opts.default_channel_capacity)
                .max(1);
            let (tx, rx) = mpsc::channel(cap);
            outs.entry(e.from.clone()).or_default().push(tx);
            ins.entry(e.to.clone()).or_default().push(InEdge {
                label: e.label.clone(),
                rx,
            });
        }

        // Build one future per node. `Send + 'static` so each node can own a
        // task (see the spawn below).
        type NodeFut = Pin<Box<dyn Future<Output = Result<NodeOutcome, FaucetError>> + Send>>;
        let mut futs: Vec<NodeFut> = Vec::with_capacity(nodes.len());
        // Parallel to `futs`, so a failure can be attributed to its node.
        let mut order: Vec<(String, &'static str)> = Vec::with_capacity(nodes.len());

        for node in nodes {
            let node_outs = outs.remove(&node.id).unwrap_or_default();
            let mut node_ins = ins.remove(&node.id).unwrap_or_default();
            let pipeline = opts.pipeline_name.clone();
            let cancel = opts.cancel.clone();
            let Node { id, kind } = node;
            order.push((id.clone(), kind.kind_str()));

            let fut: NodeFut = match kind {
                NodeKind::Source(source) => {
                    let sb = start_bookmark.clone();
                    let bs = opts.batch_size;
                    Box::pin(run_source_node(id, source, sb, bs, node_outs, cancel))
                }
                NodeKind::Transform(stages) => {
                    let rx = take_single(node_ins)
                        .ok_or_else(|| cfg(format!("transform '{id}' has no input edge")))?;
                    let labels = Labels::new(pipeline.clone(), id.clone(), opts.run_id.clone());
                    Box::pin(run_transform_node(stages, labels, rx, node_outs, cancel))
                }
                NodeKind::Tee { .. } => {
                    let rx = take_single(node_ins)
                        .ok_or_else(|| cfg(format!("tee '{id}' has no input edge")))?;
                    Box::pin(run_tee_node(id, pipeline, rx, node_outs, cancel))
                }
                NodeKind::Merge => {
                    let rxs: Vec<mpsc::Receiver<StreamPage>> =
                        node_ins.into_iter().map(|ie| ie.rx).collect();
                    Box::pin(run_merge_node(id, pipeline, rxs, node_outs, cancel))
                }
                NodeKind::Join(j) => {
                    let build_rx = take_by_label(&mut node_ins, &j.build_edge);
                    let probe_rx = take_by_label(&mut node_ins, &j.probe_edge);
                    match (build_rx, probe_rx) {
                        (Some(b), Some(p)) => {
                            Box::pin(run_join_node(id, pipeline, j, b, p, node_outs, cancel))
                        }
                        _ => {
                            return Err(cfg(format!(
                                "join '{id}' is missing its build/probe input edges"
                            )));
                        }
                    }
                }
                NodeKind::Sink(sink) => {
                    let rx = take_single(node_ins)
                        .ok_or_else(|| cfg(format!("sink '{id}' has no input edge")))?;
                    let sopts = SinkNodeOpts {
                        pipeline_name: pipeline,
                        run_id: opts.run_id.clone(),
                        state_store: opts.state_store.clone(),
                        dlq: opts.dlq.clone(),
                        cancel: cancel.clone(),
                        // Masking is destination-scoped, so each sink node takes
                        // the policy compiled for it (if any); the rest are
                        // pipeline-wide.
                        #[cfg(feature = "masking")]
                        masking: governance.masking_by_sink.get(&id).cloned(),
                        #[cfg(feature = "quality")]
                        quality: governance.quality.clone(),
                        #[cfg(feature = "contract")]
                        contract: governance.contract.clone(),
                        schema_drift: governance.schema_drift,
                        resilience: governance.resilience.clone(),
                        delivery: governance.delivery,
                        replay: source_replay,
                    };
                    Box::pin(run_sink_node(id, sink, rx, sopts))
                }
            };
            futs.push(fut);
        }

        // Drop the leftover maps so no dangling senders keep channels open.
        drop(outs);
        drop(ins);

        // One task per node, so nodes run on the runtime's whole thread pool
        // instead of sharing a single task. A synchronous stage (the DuckDB `sql`
        // transform, a wasm transform) would otherwise occupy the one task and
        // stall every other node including the sinks (#456 M5). A spawned node
        // also isolates panics: they arrive as a `JoinError` we report, rather
        // than unwinding the caller.
        let handles: Vec<tokio::task::JoinHandle<Result<NodeOutcome, FaucetError>>> =
            futs.into_iter().map(tokio::spawn).collect();
        // Dropping a `JoinHandle` detaches the task rather than cancelling it, so
        // every abandon path below aborts explicitly.
        let aborts: Vec<tokio::task::AbortHandle> =
            handles.iter().map(|h| h.abort_handle()).collect();
        let abort_all = || {
            for a in &aborts {
                a.abort();
            }
        };
        let joined = handles.into_iter().map(|h| async move {
            match h.await {
                Ok(r) => r,
                Err(e) if e.is_panic() => {
                    Err(FaucetError::Source(format!("topology node panicked: {e}")))
                }
                Err(e) => Err(FaucetError::Source(format!("topology node aborted: {e}"))),
            }
        });

        match opts.on_error {
            TopologyOnError::Propagate => {
                // Do NOT `try_join_all`: it returns on the first error and drops
                // the remaining node futures where they stand, so a buffered sink
                // never flushes — orphaning a multipart upload or writing a
                // footer-less Parquet file. Instead signal the shared cancel
                // token and let the siblings stop at their next page boundary and
                // flush, exactly as the CLI executor's `on_error: stop` does
                // (#146 H16, #456 M1). A node that does not stop within the grace
                // window is still dropped, so a sink wedged mid-write cannot hang
                // the run.
                let coop = opts.cancel.clone().unwrap_or_default().child_token();
                let first_err: Arc<std::sync::Mutex<Option<FaucetError>>> =
                    Arc::new(std::sync::Mutex::new(None));
                let failures: Arc<std::sync::Mutex<Vec<(String, String)>>> =
                    Arc::new(std::sync::Mutex::new(Vec::new()));
                let wrapped = joined.zip(order.clone()).map(|(f, (node_id, _))| {
                    let coop = coop.clone();
                    let slot = Arc::clone(&first_err);
                    let failed = Arc::clone(&failures);
                    async move {
                        match f.await {
                            Ok(o) => Some(o),
                            Err(e) => {
                                failed
                                    .lock()
                                    .unwrap_or_else(|p| p.into_inner())
                                    .push((node_id, e.to_string()));
                                tracing::error!(
                                    error = %e,
                                    "topology node failed; cancelling siblings so they flush"
                                );
                                let mut guard = slot.lock().unwrap_or_else(|p| p.into_inner());
                                if guard.is_none() {
                                    *guard = Some(e);
                                }
                                coop.cancel();
                                None
                            }
                        }
                    }
                });
                // The grace window opens only once something has cancelled the
                // token — a healthy run is never bounded by it.
                let all = futures::future::join_all(wrapped);
                let grace = STOP_FLUSH_GRACE;
                let deadline = {
                    let coop = coop.clone();
                    async move {
                        coop.cancelled().await;
                        tokio::time::sleep(grace).await;
                    }
                };
                let outcomes = tokio::select! {
                    biased;
                    v = all => v,
                    () = deadline => {
                        tracing::warn!(
                            grace_secs = grace.as_secs(),
                            "topology: nodes did not stop within the flush grace after a failure; \
                             aborting them"
                        );
                        abort_all();
                        Vec::new()
                    }
                };
                if let Some(e) = first_err.lock().unwrap_or_else(|p| p.into_inner()).take() {
                    return Err(e);
                }
                let (result, per_source) = aggregate(outcomes.into_iter().flatten().collect());
                let failed = failures.lock().unwrap_or_else(|p| p.into_inner()).clone();
                let nodes = reports(&order, &result, &per_source, &failed);
                Ok(TopologyRun {
                    result,
                    nodes,
                    per_source,
                })
            }
            TopologyOnError::Continue => {
                let results = futures::future::join_all(joined).await;
                let mut ok = Vec::new();
                let mut errs = Vec::new();
                let mut failed: Vec<(String, String)> = Vec::new();
                for (r, (node_id, _)) in results.into_iter().zip(order.clone()) {
                    match r {
                        Ok(o) => ok.push(o),
                        Err(e) => {
                            tracing::error!(
                                node = %node_id,
                                error = %e,
                                "topology node failed (on_error: continue)"
                            );
                            errs.push(e.to_string());
                            failed.push((node_id, e.to_string()));
                        }
                    }
                }
                let (mut result, per_source) = aggregate(ok);
                result.errors = errs;
                let nodes = reports(&order, &result, &per_source, &failed);
                Ok(TopologyRun {
                    result,
                    nodes,
                    per_source,
                })
            }
        }
    }
}

/// Build the per-node report list from the node ids/kinds and their outcomes.
fn reports(
    order: &[(String, &'static str)],
    sinks: &TopologyResult,
    per_source: &HashMap<String, usize>,
    errors: &[(String, String)],
) -> Vec<NodeReport> {
    order
        .iter()
        .map(|(id, kind)| NodeReport {
            node_id: id.clone(),
            kind,
            records: sinks
                .per_sink
                .get(id)
                .or_else(|| per_source.get(id))
                .copied()
                .unwrap_or(0),
            bookmark: sinks.bookmarks.get(id).cloned().flatten(),
            error: errors
                .iter()
                .find(|(nid, _)| nid == id)
                .map(|(_, e)| e.clone()),
        })
        .collect()
}

/// Aggregate node outcomes into a [`TopologyResult`] plus the per-source counts
/// (which live on [`TopologyRun`], not the result).
fn aggregate(outcomes: Vec<NodeOutcome>) -> (TopologyResult, HashMap<String, usize>) {
    let mut result = TopologyResult::default();
    let mut per_source = HashMap::new();
    for o in outcomes {
        match o {
            NodeOutcome::Sink {
                node_id,
                records,
                bookmark,
            } => {
                result.records_written += records;
                result.per_sink.insert(node_id.clone(), records);
                result.bookmarks.insert(node_id, bookmark);
            }
            NodeOutcome::Source { node_id, records } => {
                per_source.insert(node_id, records);
            }
            NodeOutcome::Other => {}
        }
    }
    (result, per_source)
}

fn cfg(msg: impl Into<String>) -> FaucetError {
    FaucetError::Config(format!("topology: {}", msg.into()))
}

fn arity(
    node_id: &str,
    kind: &str,
    in_ok: bool,
    out_ok: bool,
    expected: &str,
) -> Result<(), FaucetError> {
    if in_ok && out_ok {
        Ok(())
    } else {
        Err(cfg(format!(
            "{kind} '{node_id}' has the wrong edge arity (expected {expected})"
        )))
    }
}

fn reaches_any(start: &str, adj: &HashMap<&str, Vec<&str>>, targets: &HashSet<&str>) -> bool {
    let mut stack = vec![start];
    let mut seen = HashSet::new();
    while let Some(n) = stack.pop() {
        if targets.contains(n) {
            return true;
        }
        if !seen.insert(n) {
            continue;
        }
        if let Some(ns) = adj.get(n) {
            stack.extend(ns.iter().copied());
        }
    }
    false
}

/// Read every sink node's stored bookmark and decide the source's resume point.
///
/// Returns `Some(bookmark)` only when it is provably safe to resume there;
/// `None` means "replay in full", which costs duplicates on a non-idempotent
/// sink but can never skip a record. See [`start_bookmark`] for the rules.
async fn compute_start_bookmark(
    opts: &TopologyOptions,
    sink_ids: &[String],
    source_count: usize,
    delivery: crate::idempotency::DeliveryMode,
) -> Option<Value> {
    let store = opts.state_store.as_ref()?;
    if sink_ids.is_empty() {
        return None;
    }
    let mut values = Vec::with_capacity(sink_ids.len());
    for id in sink_ids {
        let key = format!("{}::{}", opts.pipeline_name, id);
        match store.get(&key).await {
            Ok(Some(v)) => values.push(v),
            _ => return None, // a sink with no bookmark → full replay.
        }
    }
    if delivery == crate::idempotency::DeliveryMode::ExactlyOnce {
        // Exactly-once stores `(bookmark, seq)`, and `seq` is a monotonic page
        // counter — a real total order, so the sinks can be ranked exactly
        // instead of guessed at. Resume from the *furthest behind* sink; every
        // sink ahead of it skips the pages it already committed, which is
        // precisely what the commit token is for. (#458)
        let ranked: Vec<(u64, Option<Value>)> = values
            .iter()
            .map(|v| {
                let (bm, seq) = crate::idempotency::unwrap_state(v);
                (seq, bm)
            })
            .collect();
        return eo_start_bookmark(&ranked, source_count);
    }
    start_bookmark(&values, source_count)
}

/// Exactly-once resume point: the bookmark of the lowest-`seq` sink.
///
/// Unlike the at-least-once path this *can* order the sinks, because `seq` is a
/// monotonic counter rather than an opaque position — so a diverged set resumes
/// from the laggard instead of replaying from scratch, and the sinks that are
/// ahead skip their already-committed pages via their commit tokens.
///
/// The single-source restriction still applies: nothing records which source a
/// sink's bookmark came from.
pub fn eo_start_bookmark(ranked: &[(u64, Option<Value>)], source_count: usize) -> Option<Value> {
    if ranked.is_empty() || source_count != 1 {
        if source_count > 1 && !ranked.is_empty() {
            tracing::warn!(
                sources = source_count,
                "topology: multi-source graph cannot attribute a sink bookmark to a source; \
                 replaying every source in full"
            );
        }
        return None;
    }
    ranked
        .iter()
        .min_by_key(|(seq, _)| *seq)
        .and_then(|(_, bm)| bm.clone())
}

/// Pure resume-point decision: the bookmark every source node is started from,
/// or `None` to replay in full.
///
/// Deliberately conservative — the only safe way to be wrong is to replay:
///
/// 1. **More than one source node → `None`.** A sink's bookmark records the
///    position of whichever source fed its pages, and nothing in the graph
///    records which one that was. Applying one source's position to another
///    resumes it somewhere it has never been. (#456 H1)
/// 2. **Sink bookmarks that are not all equal → `None`.** Bookmarks are compared
///    for *equality*, never ordered: resume positions are routinely structured
///    (CDC LSN maps, Kafka/Kinesis offset maps) and
///    [`json_gt`](crate::replication::json_gt)'s object arm falls back to
///    comparing serialized text, an order unrelated to replication progress. A
///    "minimum" chosen that way can sit *ahead* of the true minimum and silently
///    skip the lagging sink's records. (#456 H1)
/// 3. **All sinks agree → resume there.** No ordering needed, so no guessing.
pub fn start_bookmark(sink_bookmarks: &[Value], source_count: usize) -> Option<Value> {
    if sink_bookmarks.is_empty() || source_count != 1 {
        if source_count > 1 && !sink_bookmarks.is_empty() {
            tracing::warn!(
                sources = source_count,
                "topology: multi-source graph cannot attribute a sink bookmark to a source; \
                 replaying every source in full. Make the sinks idempotent \
                 (`write_mode: upsert`) or split the graph into one pipeline per source."
            );
        }
        return None;
    }
    let first = &sink_bookmarks[0];
    if sink_bookmarks.iter().any(|v| v != first) {
        tracing::warn!(
            "topology: sink bookmarks have diverged and resume positions are not safely \
             ordered; replaying the source in full so no sink is skipped past. Faster sinks \
             will re-see already-written pages — make them idempotent."
        );
        return None;
    }
    Some(first.clone())
}

/// Send `page` to every live output, moving into the last and cloning for the
/// rest. Closed (dropped-receiver) outputs are removed. Returns `false` once
/// every output has closed.
async fn broadcast(page: StreamPage, outs: &mut Vec<mpsc::Sender<StreamPage>>) -> bool {
    if outs.is_empty() {
        return false;
    }
    let last = outs.len() - 1;
    let mut closed: Vec<usize> = Vec::new();
    for (i, tx) in outs.iter().enumerate().take(last) {
        if tx.send(page.clone()).await.is_err() {
            closed.push(i);
        }
    }
    if outs[last].send(page).await.is_err() {
        closed.push(last);
    }
    for &i in closed.iter().rev() {
        outs.remove(i);
    }
    !outs.is_empty()
}

fn cancelled(cancel: &Option<CancellationToken>) -> bool {
    cancel.as_ref().is_some_and(|c| c.is_cancelled())
}

async fn run_source_node(
    node_id: String,
    source: Box<dyn Source>,
    start_bookmark: Option<Value>,
    batch_size: usize,
    mut outs: Vec<mpsc::Sender<StreamPage>>,
    cancel: Option<CancellationToken>,
) -> Result<NodeOutcome, FaucetError> {
    if let Some(bm) = start_bookmark {
        source.apply_start_bookmark(bm).await?;
    }
    let ctx = std::collections::HashMap::new();
    let mut pages = source.stream_pages(&ctx, batch_size);
    let mut records = 0usize;
    while let Some(item) = pages.next().await {
        if cancelled(&cancel) {
            break;
        }
        let page = item?;
        records += page.records.len();
        if !broadcast(page, &mut outs).await {
            break;
        }
    }
    Ok(NodeOutcome::Source { node_id, records })
}

async fn run_transform_node(
    stages: Vec<CompiledStage>,
    labels: Labels,
    mut rx: mpsc::Receiver<StreamPage>,
    mut outs: Vec<mpsc::Sender<StreamPage>>,
    cancel: Option<CancellationToken>,
) -> Result<NodeOutcome, FaucetError> {
    while let Some(page) = rx.recv().await {
        if cancelled(&cancel) {
            break;
        }
        let records = instrumented_apply_stages(page.records, &stages, &labels)?;
        let out = StreamPage {
            records,
            bookmark: page.bookmark,
        };
        if !broadcast(out, &mut outs).await {
            break;
        }
    }
    Ok(NodeOutcome::Other)
}

fn node_labels(pipeline: &str, node: &str) -> Vec<Label> {
    vec![
        Label::new("pipeline", SharedString::from(pipeline.to_string())),
        Label::new("node", SharedString::from(node.to_string())),
    ]
}

async fn run_tee_node(
    node_id: String,
    pipeline: String,
    mut rx: mpsc::Receiver<StreamPage>,
    mut outs: Vec<mpsc::Sender<StreamPage>>,
    cancel: Option<CancellationToken>,
) -> Result<NodeOutcome, FaucetError> {
    let labels = node_labels(&pipeline, &node_id);
    while let Some(page) = rx.recv().await {
        if cancelled(&cancel) {
            break;
        }
        counter!("faucet_tee_records_total", labels.clone()).increment(page.records.len() as u64);
        if !broadcast(page, &mut outs).await {
            break;
        }
    }
    Ok(NodeOutcome::Other)
}

async fn run_merge_node(
    node_id: String,
    pipeline: String,
    rxs: Vec<mpsc::Receiver<StreamPage>>,
    mut outs: Vec<mpsc::Sender<StreamPage>>,
    cancel: Option<CancellationToken>,
) -> Result<NodeOutcome, FaucetError> {
    let labels = node_labels(&pipeline, &node_id);
    let streams = rxs.into_iter().map(|mut rx| {
        Box::pin(async_stream::stream! {
            while let Some(p) = rx.recv().await {
                yield p;
            }
        }) as Pin<Box<dyn futures::Stream<Item = StreamPage> + Send>>
    });
    let mut sel = futures::stream::select_all(streams);
    while let Some(page) = sel.next().await {
        if cancelled(&cancel) {
            break;
        }
        counter!("faucet_merge_records_total", labels.clone()).increment(page.records.len() as u64);
        if !broadcast(page, &mut outs).await {
            break;
        }
    }
    Ok(NodeOutcome::Other)
}

#[allow(clippy::too_many_arguments)]
async fn run_join_node(
    node_id: String,
    pipeline: String,
    j: JoinNode,
    mut build_rx: mpsc::Receiver<StreamPage>,
    mut probe_rx: mpsc::Receiver<StreamPage>,
    mut outs: Vec<mpsc::Sender<StreamPage>>,
    cancel: Option<CancellationToken>,
) -> Result<NodeOutcome, FaucetError> {
    let mode = j.config.mode;
    let mut join = HashJoin::new(j.config);

    // Build phase: fully drain the build side before probing.
    let build_start = std::time::Instant::now();
    while let Some(page) = build_rx.recv().await {
        if cancelled(&cancel) {
            return Ok(NodeOutcome::Other);
        }
        join.add_build_page(page.records)?;
    }
    let labels = node_labels(&pipeline, &node_id);
    histogram!("faucet_join_build_duration_seconds", labels.clone())
        .record(build_start.elapsed().as_secs_f64());

    // Probe phase.
    while let Some(page) = probe_rx.recv().await {
        if cancelled(&cancel) {
            break;
        }
        let enriched = join.probe_page(page.records)?;
        let out = StreamPage {
            records: enriched,
            bookmark: page.bookmark,
        };
        if !broadcast(out, &mut outs).await {
            break;
        }
    }

    emit_join_metrics(&labels, mode, join.stats());
    Ok(NodeOutcome::Other)
}

fn emit_join_metrics(labels: &[Label], mode: JoinMode, stats: &crate::join::JoinStats) {
    counter!("faucet_join_build_records_total", labels.to_vec()).increment(stats.build_records);
    counter!("faucet_join_build_nulls_total", labels.to_vec()).increment(stats.build_nulls);
    counter!("faucet_join_duplicates_total", labels.to_vec()).increment(stats.duplicates);
    counter!("faucet_join_probe_records_total", labels.to_vec()).increment(stats.probe_records);
    counter!("faucet_join_project_misses_total", labels.to_vec()).increment(stats.project_misses);
    let mut match_labels = labels.to_vec();
    match_labels.push(Label::new("kind", SharedString::from(mode.to_string())));
    counter!("faucet_join_matches_total", match_labels.clone()).increment(stats.matches);
    counter!("faucet_join_misses_total", match_labels).increment(stats.misses);
}

struct SinkNodeOpts {
    pipeline_name: String,
    run_id: String,
    state_store: Option<Arc<dyn StateStore>>,
    dlq: Option<DlqConfig>,
    cancel: Option<CancellationToken>,
    /// Masking policy compiled for *this* sink node (destination-scoped).
    #[cfg(feature = "masking")]
    masking: Option<Arc<crate::masking::CompiledMasking>>,
    #[cfg(feature = "quality")]
    quality: Option<Arc<crate::quality::CompiledQuality>>,
    #[cfg(feature = "contract")]
    contract: Option<Arc<crate::contract::CompiledContract>>,
    schema_drift: Option<crate::drift::SchemaDriftPolicy>,
    resilience: Option<crate::resilience::ResiliencePolicy>,
    /// Delivery guarantee for this sink node.
    delivery: crate::idempotency::DeliveryMode,
    /// The replay capability of the graph's source, so `run_stream` can tell an
    /// atomic-watermark run from a keyed-upsert one. `None` when there is not
    /// exactly one source (in which case exactly-once is gated off anyway).
    replay: Option<crate::idempotency::ReplayGuarantee>,
}

async fn run_sink_node(
    node_id: String,
    sink: Box<dyn Sink>,
    mut rx: mpsc::Receiver<StreamPage>,
    opts: SinkNodeOpts,
) -> Result<NodeOutcome, FaucetError> {
    let pages = Box::pin(async_stream::stream! {
        while let Some(page) = rx.recv().await {
            yield Ok::<StreamPage, FaucetError>(page);
        }
    });

    let mut run_opts = RunStreamOptions::new()
        .with_name(opts.pipeline_name.clone())
        .with_row(node_id.clone())
        .with_run_id(opts.run_id.clone());
    if let Some(store) = opts.state_store {
        let key = format!("{}::{}", opts.pipeline_name, node_id);
        // Exactly-once: this node's state holds `(bookmark, seq)`, and `seq` is
        // where its commit-token sequence resumes. Read it before handing the
        // store to `run_stream`, which owns the writes from here (#458).
        if opts.delivery == crate::idempotency::DeliveryMode::ExactlyOnce {
            let seq = match store.get(&key).await {
                Ok(Some(prior)) => crate::idempotency::unwrap_state(&prior).1,
                Ok(None) => 0,
                Err(e) => return Err(e),
            };
            run_opts = run_opts.with_delivery(opts.delivery).with_start_seq(seq);
            if let Some(replay) = opts.replay {
                run_opts = run_opts.with_replay_guarantee(replay);
            }
        }
        run_opts = run_opts.with_state(store, key);
    }
    if let Some(dlq) = opts.dlq {
        run_opts = run_opts.with_dlq(dlq);
    }
    if let Some(cancel) = opts.cancel {
        run_opts = run_opts.with_cancel(cancel);
    }
    // Governance passes, in the same order `Pipeline` applies them: masking
    // first (so nothing downstream — sink, DLQ, lineage sample — ever sees
    // unmasked PII), then quality, contract, and drift.
    #[cfg(feature = "masking")]
    if let Some(m) = opts.masking {
        run_opts = run_opts.with_masking(m);
    }
    #[cfg(feature = "quality")]
    if let Some(q) = opts.quality {
        run_opts = run_opts.with_quality(q);
    }
    #[cfg(feature = "contract")]
    if let Some(c) = opts.contract {
        run_opts = run_opts.with_contract(c);
    }
    if let Some(d) = opts.schema_drift {
        run_opts.schema_drift = Some(d);
    }
    if let Some(r) = opts.resilience {
        run_opts.resilience = Some(r);
    }

    let result = run_stream(pages, sink.as_ref(), run_opts).await?;
    Ok(NodeOutcome::Sink {
        node_id,
        records: result.records_written,
        bookmark: result.bookmark,
    })
}

// ── Builder ──────────────────────────────────────────────────────────────────

/// Fluent builder for a [`Topology`].
#[derive(Default)]
pub struct TopologyBuilder {
    nodes: Vec<Node>,
    edges: Vec<Edge>,
}

impl TopologyBuilder {
    /// Add a node of any kind.
    pub fn node(mut self, id: impl Into<String>, kind: NodeKind) -> Self {
        self.nodes.push(Node {
            id: id.into(),
            kind,
        });
        self
    }

    /// Add a source node.
    pub fn source(self, id: impl Into<String>, source: Box<dyn Source>) -> Self {
        self.node(id, NodeKind::Source(source))
    }

    /// Add a transform node.
    pub fn transform(self, id: impl Into<String>, stages: Vec<CompiledStage>) -> Self {
        self.node(id, NodeKind::Transform(stages))
    }

    /// Add a tee (fan-out) node.
    pub fn tee(self, id: impl Into<String>, capacity: usize, fanout: Option<usize>) -> Self {
        self.node(id, NodeKind::Tee { capacity, fanout })
    }

    /// Add a merge (fan-in) node.
    pub fn merge(self, id: impl Into<String>) -> Self {
        self.node(id, NodeKind::Merge)
    }

    /// Add a join node.
    pub fn join(self, id: impl Into<String>, join: JoinNode) -> Self {
        self.node(id, NodeKind::Join(join))
    }

    /// Add a sink node.
    pub fn sink(self, id: impl Into<String>, sink: Box<dyn Sink>) -> Self {
        self.node(id, NodeKind::Sink(sink))
    }

    /// Add an unlabelled edge.
    pub fn edge(mut self, from: impl Into<String>, to: impl Into<String>) -> Self {
        self.edges.push(Edge {
            from: from.into(),
            to: to.into(),
            label: None,
        });
        self
    }

    /// Add a labelled edge (used by join build/probe wiring).
    pub fn labelled_edge(
        mut self,
        from: impl Into<String>,
        to: impl Into<String>,
        label: impl Into<String>,
    ) -> Self {
        self.edges.push(Edge {
            from: from.into(),
            to: to.into(),
            label: Some(label.into()),
        });
        self
    }

    /// Finalize and validate the topology.
    pub fn build(self) -> Result<Topology, FaucetError> {
        let t = Topology {
            nodes: self.nodes,
            edges: self.edges,
        };
        t.validate()?;
        Ok(t)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::join::{JoinConfig, JoinMode, Projection};
    use crate::state::MemoryStateStore;
    use async_trait::async_trait;
    use serde_json::json;
    use std::sync::Mutex;

    // ── Mock connectors ───────────────────────────────────────────────────────

    pub(super) struct VecSource {
        records: Vec<Value>,
        bookmark: Option<Value>,
    }
    impl VecSource {
        pub(super) fn boxed(records: Vec<Value>) -> Box<dyn Source> {
            Box::new(VecSource {
                records,
                bookmark: None,
            })
        }
        fn boxed_bm(records: Vec<Value>, bm: Value) -> Box<dyn Source> {
            Box::new(VecSource {
                records,
                bookmark: Some(bm),
            })
        }
    }
    #[async_trait]
    impl Source for VecSource {
        async fn fetch_with_context(
            &self,
            _c: &std::collections::HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(self.records.clone())
        }
        async fn fetch_with_context_incremental(
            &self,
            _c: &std::collections::HashMap<String, Value>,
        ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
            Ok((self.records.clone(), self.bookmark.clone()))
        }
    }

    struct FailingSource;
    #[async_trait]
    impl Source for FailingSource {
        async fn fetch_with_context(
            &self,
            _c: &std::collections::HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Err(FaucetError::Source("boom".into()))
        }
    }

    /// Records the bookmark applied via `apply_start_bookmark`.
    struct RecordingSource {
        records: Vec<Value>,
        applied: Arc<Mutex<Option<Value>>>,
    }
    #[async_trait]
    impl Source for RecordingSource {
        async fn fetch_with_context(
            &self,
            _c: &std::collections::HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(self.records.clone())
        }
        async fn apply_start_bookmark(&self, bm: Value) -> Result<(), FaucetError> {
            *self.applied.lock().unwrap() = Some(bm);
            Ok(())
        }
    }

    #[derive(Clone)]
    pub(super) struct CollectSink {
        store: Arc<Mutex<Vec<Value>>>,
    }
    impl CollectSink {
        pub(super) fn new() -> (Self, Arc<Mutex<Vec<Value>>>) {
            let store = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    store: store.clone(),
                },
                store,
            )
        }
    }
    #[async_trait]
    impl Sink for CollectSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            self.store.lock().unwrap().extend_from_slice(records);
            Ok(records.len())
        }
    }

    pub(super) struct FailingSink;
    #[async_trait]
    impl Sink for FailingSink {
        async fn write_batch(&self, _records: &[Value]) -> Result<usize, FaucetError> {
            Err(FaucetError::Sink("sink boom".into()))
        }
    }

    /// A sink that records whether `flush` was called — the observable proof that
    /// a node was allowed to finish cooperatively rather than being dropped.
    struct FlushTrackingSink {
        flushed: Arc<Mutex<bool>>,
    }
    #[async_trait]
    impl Sink for FlushTrackingSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            Ok(records.len())
        }
        async fn flush(&self) -> Result<(), FaucetError> {
            *self.flushed.lock().unwrap() = true;
            Ok(())
        }
    }

    pub(super) fn recs(n: usize) -> Vec<Value> {
        (0..n).map(|i| json!({ "i": i })).collect()
    }

    // ── Validation ────────────────────────────────────────────────────────────

    #[test]
    fn validate_rejects_empty() {
        let err = Topology {
            nodes: vec![],
            edges: vec![],
        }
        .validate()
        .unwrap_err();
        assert!(err.to_string().contains("no nodes"));
    }

    #[test]
    fn validate_rejects_duplicate_id() {
        let (sink, _) = CollectSink::new();
        let err = Topology::builder()
            .source("a", VecSource::boxed(recs(1)))
            .sink("a", Box::new(sink))
            .edge("a", "a")
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("duplicate node id"));
    }

    #[test]
    fn validate_rejects_unknown_endpoint() {
        let err = Topology::builder()
            .source("s", VecSource::boxed(recs(1)))
            .edge("s", "ghost")
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("unknown 'to' node 'ghost'"));
    }

    #[test]
    fn validate_rejects_unknown_from_endpoint() {
        let (sink, _) = CollectSink::new();
        let err = Topology::builder()
            .sink("k", Box::new(sink))
            .edge("ghost", "k")
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("unknown 'from' node 'ghost'"));
    }

    #[test]
    fn validate_rejects_source_with_incoming_edge() {
        let (sink, _) = CollectSink::new();
        let err = Topology::builder()
            .source("s", VecSource::boxed(recs(1)))
            .sink("k", Box::new(sink))
            .edge("s", "k")
            .edge("k", "s") // sink→source: gives source in=1 and sink out=1
            .build()
            .unwrap_err();
        // Either arity or cycle is caught; both are correct rejections.
        assert!(err.to_string().contains("arity") || err.to_string().contains("cycle"));
    }

    #[test]
    fn validate_rejects_tee_fanout_mismatch() {
        let (s1, _) = CollectSink::new();
        let (s2, _) = CollectSink::new();
        let err = Topology::builder()
            .source("s", VecSource::boxed(recs(1)))
            .tee("t", 4, Some(3))
            .sink("a", Box::new(s1))
            .sink("b", Box::new(s2))
            .edge("s", "t")
            .edge("t", "a")
            .edge("t", "b")
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("fanout 3 but has 2"));
    }

    #[test]
    fn validate_rejects_tee_with_one_output() {
        let (s1, _) = CollectSink::new();
        let err = Topology::builder()
            .source("s", VecSource::boxed(recs(1)))
            .tee("t", 4, None)
            .sink("a", Box::new(s1))
            .edge("s", "t")
            .edge("t", "a")
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("tee 't'"));
    }

    #[test]
    fn validate_rejects_merge_with_one_input() {
        let (s1, _) = CollectSink::new();
        let err = Topology::builder()
            .source("s", VecSource::boxed(recs(1)))
            .merge("m")
            .sink("a", Box::new(s1))
            .edge("s", "m")
            .edge("m", "a")
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("merge 'm'"));
    }

    #[test]
    fn validate_rejects_join_missing_label() {
        let (s1, _) = CollectSink::new();
        let jn = JoinNode {
            config: JoinConfig::default(),
            build_edge: "build".into(),
            probe_edge: "probe".into(),
        };
        let err = Topology::builder()
            .source("b", VecSource::boxed(recs(1)))
            .source("p", VecSource::boxed(recs(1)))
            .join("j", jn)
            .sink("a", Box::new(s1))
            .labelled_edge("b", "j", "build")
            .edge("p", "j") // unlabelled — probe label missing
            .edge("j", "a")
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("labelled 'probe'"));
    }

    #[test]
    fn validate_rejects_join_same_labels() {
        let (s1, _) = CollectSink::new();
        let jn = JoinNode {
            config: JoinConfig::default(),
            build_edge: "x".into(),
            probe_edge: "x".into(),
        };
        let err = Topology::builder()
            .source("b", VecSource::boxed(recs(1)))
            .source("p", VecSource::boxed(recs(1)))
            .join("j", jn)
            .sink("a", Box::new(s1))
            .labelled_edge("b", "j", "x")
            .labelled_edge("p", "j", "x")
            .edge("j", "a")
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("must differ"));
    }

    #[test]
    fn validate_rejects_cycle() {
        // s → m(merge) → t(tee) → {m, k}. The m→t→m loop is a valid-arity
        // cycle (merge absorbs the back-edge, tee provides the second out).
        let (sink, _) = CollectSink::new();
        let err = Topology::builder()
            .source("s", VecSource::boxed(recs(1)))
            .merge("m")
            .tee("t", 4, None)
            .sink("k", Box::new(sink))
            .edge("s", "m")
            .edge("m", "t")
            .edge("t", "m")
            .edge("t", "k")
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("cycle"), "{err}");
    }

    #[test]
    fn validate_rejects_no_source() {
        // Two transforms wired in a ring: valid arity, but no source node.
        let err = Topology::builder()
            .transform("t1", vec![])
            .transform("t2", vec![])
            .edge("t1", "t2")
            .edge("t2", "t1")
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("no source"), "{err}");
    }

    #[test]
    fn validate_rejects_no_sink() {
        // Two sources into a self-looping merge: valid arity, but no sink.
        let err = Topology::builder()
            .source("s1", VecSource::boxed(recs(1)))
            .source("s2", VecSource::boxed(recs(1)))
            .merge("m")
            .edge("s1", "m")
            .edge("s2", "m")
            .edge("m", "m")
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("no sink"), "{err}");
    }

    // ── Execution ─────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn simple_source_to_sink() {
        let (sink, store) = CollectSink::new();
        let topo = Topology::builder()
            .source("s", VecSource::boxed(recs(5)))
            .sink("k", Box::new(sink))
            .edge("s", "k")
            .build()
            .unwrap();
        let result = topo.run(TopologyOptions::new("p")).await.unwrap();
        assert_eq!(result.records_written, 5);
        assert_eq!(store.lock().unwrap().len(), 5);
        assert_eq!(result.per_sink.get("k"), Some(&5));
    }

    #[tokio::test]
    async fn source_transform_sink() {
        let (sink, store) = CollectSink::new();
        let topo = Topology::builder()
            .source("s", VecSource::boxed(recs(3)))
            .transform("t", vec![]) // passthrough
            .sink("k", Box::new(sink))
            .edge("s", "t")
            .edge("t", "k")
            .build()
            .unwrap();
        let result = topo.run(TopologyOptions::new("p")).await.unwrap();
        assert_eq!(result.records_written, 3);
        assert_eq!(store.lock().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn tee_fans_out_to_three_sinks() {
        let (s1, st1) = CollectSink::new();
        let (s2, st2) = CollectSink::new();
        let (s3, st3) = CollectSink::new();
        let topo = Topology::builder()
            .source("s", VecSource::boxed(recs(10)))
            .tee("t", 4, Some(3))
            .sink("a", Box::new(s1))
            .sink("b", Box::new(s2))
            .sink("c", Box::new(s3))
            .edge("s", "t")
            .edge("t", "a")
            .edge("t", "b")
            .edge("t", "c")
            .build()
            .unwrap();
        let result = topo.run(TopologyOptions::new("p")).await.unwrap();
        assert_eq!(st1.lock().unwrap().len(), 10);
        assert_eq!(st2.lock().unwrap().len(), 10);
        assert_eq!(st3.lock().unwrap().len(), 10);
        assert_eq!(result.records_written, 30);
    }

    #[tokio::test]
    async fn merge_fans_in_two_sources() {
        let (sink, store) = CollectSink::new();
        let topo = Topology::builder()
            .source("s1", VecSource::boxed(recs(4)))
            .source("s2", VecSource::boxed(recs(6)))
            .merge("m")
            .sink("k", Box::new(sink))
            .edge("s1", "m")
            .edge("s2", "m")
            .edge("m", "k")
            .build()
            .unwrap();
        let result = topo.run(TopologyOptions::new("p")).await.unwrap();
        assert_eq!(result.records_written, 10);
        assert_eq!(store.lock().unwrap().len(), 10);
    }

    #[tokio::test]
    async fn join_enriches_end_to_end() {
        let (sink, store) = CollectSink::new();
        let customers = vec![
            json!({"id": 1, "tier": "gold"}),
            json!({"id": 2, "tier": "silver"}),
        ];
        let orders = vec![
            json!({"order": "A", "cust": 1}),
            json!({"order": "B", "cust": 2}),
            json!({"order": "C", "cust": 99}),
        ];
        let jn = JoinNode {
            config: JoinConfig {
                mode: JoinMode::Inner,
                build_key: "id".into(),
                probe_key: "cust".into(),
                projections: vec![Projection {
                    from: "tier".into(),
                    as_: "tier".into(),
                }],
                ..Default::default()
            },
            build_edge: "customers".into(),
            probe_edge: "orders".into(),
        };
        let topo = Topology::builder()
            .source("c", VecSource::boxed(customers))
            .source("o", VecSource::boxed(orders))
            .join("j", jn)
            .sink("k", Box::new(sink))
            .labelled_edge("c", "j", "customers")
            .labelled_edge("o", "j", "orders")
            .edge("j", "k")
            .build()
            .unwrap();
        let result = topo.run(TopologyOptions::new("p")).await.unwrap();
        // inner join: C (cust 99) drops → 2 enriched records.
        assert_eq!(result.records_written, 2);
        let written = store.lock().unwrap();
        assert!(
            written
                .iter()
                .any(|r| r["order"] == json!("A") && r["tier"] == json!("gold"))
        );
    }

    #[tokio::test]
    async fn propagate_aborts_on_sink_failure() {
        let topo = Topology::builder()
            .source("s", VecSource::boxed(recs(3)))
            .sink("k", Box::new(FailingSink))
            .edge("s", "k")
            .build()
            .unwrap();
        let err = topo.run(TopologyOptions::new("p")).await.unwrap_err();
        assert!(matches!(err, FaucetError::Sink(_)));
    }

    #[tokio::test]
    async fn propagate_aborts_on_source_failure() {
        let (sink, _) = CollectSink::new();
        let topo = Topology::builder()
            .source("s", Box::new(FailingSource))
            .sink("k", Box::new(sink))
            .edge("s", "k")
            .build()
            .unwrap();
        let err = topo.run(TopologyOptions::new("p")).await.unwrap_err();
        assert!(matches!(err, FaucetError::Source(_)));
    }

    #[tokio::test]
    async fn continue_lets_healthy_branch_finish() {
        // One branch fails, the other still receives every record.
        let (good, store) = CollectSink::new();
        let topo = Topology::builder()
            .source("s", VecSource::boxed(recs(8)))
            .tee("t", 8, Some(2))
            .sink("bad", Box::new(FailingSink))
            .sink("good", Box::new(good))
            .edge("s", "t")
            .edge("t", "bad")
            .edge("t", "good")
            .build()
            .unwrap();
        let opts = TopologyOptions::new("p").with_on_error(TopologyOnError::Continue);
        let result = topo.run(opts).await.unwrap();
        assert_eq!(store.lock().unwrap().len(), 8);
        assert!(!result.errors.is_empty(), "failing sink should be recorded");
    }

    #[tokio::test]
    async fn state_agreeing_bookmarks_resume_the_source() {
        let store = Arc::new(MemoryStateStore::new());
        // Both sinks committed the same page → that position is a safe resume.
        store.put("p::a", &json!(100)).await.unwrap();
        store.put("p::b", &json!(100)).await.unwrap();
        let applied = Arc::new(Mutex::new(None));
        let src = RecordingSource {
            records: recs(1),
            applied: applied.clone(),
        };
        let (s1, _) = CollectSink::new();
        let (s2, _) = CollectSink::new();
        let topo = Topology::builder()
            .source("s", Box::new(src))
            .tee("t", 4, Some(2))
            .sink("a", Box::new(s1))
            .sink("b", Box::new(s2))
            .edge("s", "t")
            .edge("t", "a")
            .edge("t", "b")
            .build()
            .unwrap();
        let opts = TopologyOptions::new("p").with_state_store(store.clone());
        topo.run(opts).await.unwrap();
        assert_eq!(*applied.lock().unwrap(), Some(json!(100)));
    }

    #[tokio::test]
    async fn state_diverged_bookmarks_replay_in_full() {
        // #456 H1: bookmarks are compared for equality, never ordered — an
        // ordered "minimum" over structured positions can sit ahead of the true
        // minimum and skip the lagging sink's records. Diverged → full replay.
        let store = Arc::new(MemoryStateStore::new());
        store.put("p::a", &json!(250)).await.unwrap();
        store.put("p::b", &json!(100)).await.unwrap();
        let applied = Arc::new(Mutex::new(None));
        let src = RecordingSource {
            records: recs(1),
            applied: applied.clone(),
        };
        let (s1, _) = CollectSink::new();
        let (s2, _) = CollectSink::new();
        let topo = Topology::builder()
            .source("s", Box::new(src))
            .tee("t", 4, Some(2))
            .sink("a", Box::new(s1))
            .sink("b", Box::new(s2))
            .edge("s", "t")
            .edge("t", "a")
            .edge("t", "b")
            .build()
            .unwrap();
        let opts = TopologyOptions::new("p").with_state_store(store.clone());
        topo.run(opts).await.unwrap();
        assert_eq!(
            *applied.lock().unwrap(),
            None,
            "diverged bookmarks must replay, never guess an order"
        );
    }

    #[tokio::test]
    async fn state_multi_source_never_cross_applies_a_bookmark() {
        // #456 H1: nothing records which source a sink's bookmark came from, so
        // applying one to every source would resume a source somewhere it has
        // never been. Multi-source graphs replay in full.
        let store = Arc::new(MemoryStateStore::new());
        store.put("p::k", &json!(500)).await.unwrap();
        let a_applied = Arc::new(Mutex::new(None));
        let b_applied = Arc::new(Mutex::new(None));
        let a = RecordingSource {
            records: recs(1),
            applied: a_applied.clone(),
        };
        let b = RecordingSource {
            records: recs(1),
            applied: b_applied.clone(),
        };
        let (sink, _) = CollectSink::new();
        let topo = Topology::builder()
            .source("a", Box::new(a))
            .source("b", Box::new(b))
            .merge("m")
            .sink("k", Box::new(sink))
            .edge("a", "m")
            .edge("b", "m")
            .edge("m", "k")
            .build()
            .unwrap();
        let opts = TopologyOptions::new("p").with_state_store(store.clone());
        topo.run(opts).await.unwrap();
        assert_eq!(*a_applied.lock().unwrap(), None);
        assert_eq!(*b_applied.lock().unwrap(), None);
    }

    #[tokio::test]
    async fn state_no_bookmark_when_a_sink_is_missing() {
        let store = Arc::new(MemoryStateStore::new());
        store.put("p::a", &json!(100)).await.unwrap();
        // sink b has no stored bookmark → full replay (no apply).
        let applied = Arc::new(Mutex::new(None));
        let src = RecordingSource {
            records: recs(1),
            applied: applied.clone(),
        };
        let (s1, _) = CollectSink::new();
        let (s2, _) = CollectSink::new();
        let topo = Topology::builder()
            .source("s", Box::new(src))
            .tee("t", 4, Some(2))
            .sink("a", Box::new(s1))
            .sink("b", Box::new(s2))
            .edge("s", "t")
            .edge("t", "a")
            .edge("t", "b")
            .build()
            .unwrap();
        let opts = TopologyOptions::new("p").with_state_store(store.clone());
        topo.run(opts).await.unwrap();
        assert_eq!(*applied.lock().unwrap(), None);
    }

    #[tokio::test]
    async fn sink_persists_bookmark() {
        let store = Arc::new(MemoryStateStore::new());
        let (sink, _) = CollectSink::new();
        let topo = Topology::builder()
            .source("s", VecSource::boxed_bm(recs(2), json!("v9")))
            .sink("k", Box::new(sink))
            .edge("s", "k")
            .build()
            .unwrap();
        let opts = TopologyOptions::new("p").with_state_store(store.clone());
        let result = topo.run(opts).await.unwrap();
        assert_eq!(result.bookmarks.get("k"), Some(&Some(json!("v9"))));
        assert_eq!(store.get("p::k").await.unwrap(), Some(json!("v9")));
    }

    /// #456 M1: a node failure under `Propagate` must let its siblings stop at a
    /// page boundary and **flush**, not drop them where they stand (which
    /// orphans a multipart upload / writes a footer-less file).
    #[tokio::test]
    async fn propagate_lets_siblings_flush_before_returning_the_error() {
        let flushed = Arc::new(Mutex::new(false));
        let tracker = FlushTrackingSink {
            flushed: flushed.clone(),
        };
        let topo = Topology::builder()
            .source("s", VecSource::boxed(recs(64)))
            .tee("t", 4, Some(2))
            .sink("bad", Box::new(FailingSink))
            .sink("good", Box::new(tracker))
            .edge("s", "t")
            .edge("t", "bad")
            .edge("t", "good")
            .build()
            .unwrap();
        let opts = TopologyOptions::new("p").with_on_error(TopologyOnError::Propagate);
        let err = topo.run(opts).await.unwrap_err();
        assert!(matches!(err, FaucetError::Sink(_)), "{err:?}");
        assert!(
            *flushed.lock().unwrap(),
            "the healthy sink node must be flushed, not dropped mid-write"
        );
    }

    /// #456 C3: the governance passes must apply to a topology's sink nodes, or a
    /// config declaring masking writes PII in the clear.
    #[cfg(feature = "masking")]
    #[tokio::test]
    async fn masking_applies_to_a_sink_node() {
        use crate::masking::{CompiledMasking, MaskingSpec};

        let spec: MaskingSpec = serde_json::from_value(json!({
            "rules": [{
                "name": "hide-email",
                "match": { "fields": ["email"] },
                "action": { "type": "redact", "mask": "***" }
            }]
        }))
        .unwrap();
        let compiled = Arc::new(CompiledMasking::compile(&spec).unwrap());

        let (sink, store) = CollectSink::new();
        let topo = Topology::builder()
            .source(
                "s",
                VecSource::boxed(vec![json!({"id": 1, "email": "a@b.c"})]),
            )
            .sink("k", Box::new(sink))
            .edge("s", "k")
            .build()
            .unwrap();

        let mut governance = TopologyGovernance::new();
        governance.masking_by_sink.insert("k".to_string(), compiled);
        topo.run_with(TopologyOptions::new("p"), governance)
            .await
            .unwrap();

        let written = store.lock().unwrap();
        assert_eq!(written.len(), 1);
        assert_eq!(written[0]["email"], json!("***"), "PII must be masked");
        assert_eq!(written[0]["id"], json!(1));
    }

    #[tokio::test]
    async fn cancellation_stops_the_run() {
        let cancel = CancellationToken::new();
        cancel.cancel(); // pre-cancelled
        let (sink, store) = CollectSink::new();
        let topo = Topology::builder()
            .source("s", VecSource::boxed(recs(1000)))
            .sink("k", Box::new(sink))
            .edge("s", "k")
            .build()
            .unwrap();
        let opts = TopologyOptions::new("p").with_cancel(cancel);
        let result = topo.run(opts).await.unwrap();
        // Cancelled before/early: far fewer than 1000 records written.
        assert!(store.lock().unwrap().len() < 1000);
        let _ = result;
    }

    #[test]
    fn start_bookmark_only_resumes_when_provably_safe() {
        // Every sink agrees, single source → resume there.
        assert_eq!(
            start_bookmark(&[json!(100), json!(100)], 1),
            Some(json!(100))
        );
        // Structured positions that agree are fine too — no ordering needed.
        let lsn = json!({"slot": "s", "lsn": "0/16B3748"});
        assert_eq!(
            start_bookmark(&[lsn.clone(), lsn.clone()], 1),
            Some(lsn.clone())
        );

        // Diverged scalars → replay. The old code returned `min` = 100 here;
        // for the structured case below that "minimum" was text-ordered and
        // could sit ahead of the true minimum (#456 H1).
        assert_eq!(start_bookmark(&[json!(250), json!(100)], 1), None);
        // The exact shape that made a text-ordered minimum unsafe: "0/9…" sorts
        // above "0/10…" lexicographically while being *behind* it numerically.
        assert_eq!(
            start_bookmark(
                &[json!({"lsn": "0/9FFFFFF"}), json!({"lsn": "0/10000000"}),],
                1
            ),
            None
        );

        // More than one source → never cross-apply.
        assert_eq!(start_bookmark(&[json!(100), json!(100)], 2), None);
        // No sinks / no bookmarks → nothing to resume from.
        assert_eq!(start_bookmark(&[], 1), None);
        assert_eq!(start_bookmark(&[], 3), None);
    }

    #[test]
    fn kind_str_matches() {
        assert_eq!(NodeKind::Merge.kind_str(), "merge");
        assert_eq!(
            NodeKind::Tee {
                capacity: 1,
                fanout: None
            }
            .kind_str(),
            "tee"
        );
    }

    #[test]
    fn builder_exposes_nodes_and_edges() {
        let (sink, _) = CollectSink::new();
        let topo = Topology::builder()
            .source("s", VecSource::boxed(recs(1)))
            .sink("k", Box::new(sink))
            .edge("s", "k")
            .build()
            .unwrap();
        assert_eq!(topo.nodes().len(), 2);
        assert_eq!(topo.edges().len(), 1);
    }

    #[cfg(feature = "transform-keys-case")]
    #[tokio::test]
    async fn transform_node_applies_stage() {
        use crate::stage::{TransformStage, compile_stage};
        use crate::transform::{KeyCaseMode, RecordTransform};
        let stage = compile_stage(&TransformStage::Map(RecordTransform::KeysCase {
            mode: KeyCaseMode::Snake,
        }))
        .unwrap();
        let (sink, store) = CollectSink::new();
        let topo = Topology::builder()
            .source("s", VecSource::boxed(vec![json!({"FooBar": 1})]))
            .transform("t", vec![stage])
            .sink("k", Box::new(sink))
            .edge("s", "t")
            .edge("t", "k")
            .build()
            .unwrap();
        topo.run(TopologyOptions::new("p")).await.unwrap();
        let w = store.lock().unwrap();
        assert!(w[0].get("foo_bar").is_some());
    }
}

#[cfg(test)]
mod delivery_and_report_tests {
    use super::tests::{CollectSink, FailingSink, VecSource, recs};
    use super::*;
    use crate::Stream;
    use crate::idempotency::{DeliveryMode, format_token_with_bookmark, wrap_state};
    use crate::state::{MemoryStateStore, StateStore};
    use async_trait::async_trait;
    use serde_json::json;
    use std::sync::Mutex;

    /// A sink that records a commit token per scope, like the SQL sinks do.
    struct TokenSink {
        rows: Arc<Mutex<Vec<Value>>>,
        tokens: Arc<Mutex<std::collections::HashMap<String, String>>>,
    }
    #[async_trait]
    impl Sink for TokenSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            self.rows.lock().unwrap().extend_from_slice(records);
            Ok(records.len())
        }
        fn supports_idempotent_writes(&self) -> bool {
            true
        }
        async fn write_batch_idempotent(
            &self,
            records: &[Value],
            scope: &str,
            token: &str,
        ) -> Result<usize, FaucetError> {
            self.rows.lock().unwrap().extend_from_slice(records);
            self.tokens
                .lock()
                .unwrap()
                .insert(scope.to_string(), token.to_string());
            Ok(records.len())
        }
        async fn last_committed_token(&self, scope: &str) -> Result<Option<String>, FaucetError> {
            Ok(self.tokens.lock().unwrap().get(scope).cloned())
        }
    }

    /// #458: a sink node under `delivery: exactly_once` must commit through the
    /// idempotent write path, under **its own** scope (its state key), so each
    /// sink's watermark is independent of its siblings'.
    #[tokio::test]
    async fn exactly_once_commits_a_token_per_sink_node_scope() {
        let rows = Arc::new(Mutex::new(Vec::new()));
        let tokens = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let sink = TokenSink {
            rows: rows.clone(),
            tokens: tokens.clone(),
        };
        let store = Arc::new(MemoryStateStore::new());
        let topo = Topology::builder()
            .source("s", Box::new(EoSource(recs(3))))
            .sink("k", Box::new(sink))
            .edge("s", "k")
            .build()
            .unwrap();

        let mut gov = TopologyGovernance::new();
        gov.delivery = DeliveryMode::ExactlyOnce;
        let opts = TopologyOptions::new("p").with_state_store(store.clone());
        topo.run_with(opts, gov).await.unwrap();

        assert_eq!(rows.lock().unwrap().len(), 3);
        let committed = tokens.lock().unwrap();
        assert!(
            committed.contains_key("p::k"),
            "token must be scoped to the sink node's own state key, got {:?}",
            committed.keys().collect::<Vec<_>>()
        );
    }

    /// A source that reports deterministic replay and emits a bookmark per page,
    /// which is what the atomic-watermark mechanism requires.
    struct EoSource(Vec<Value>);
    #[async_trait]
    impl Source for EoSource {
        async fn fetch_with_context(
            &self,
            _ctx: &std::collections::HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(self.0.clone())
        }
        fn stream_pages<'a>(
            &'a self,
            _ctx: &'a std::collections::HashMap<String, Value>,
            _batch: usize,
        ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
            let rows = self.0.clone();
            Box::pin(async_stream::try_stream! {
                yield StreamPage { records: rows, bookmark: Some(json!({"pos": 1})) };
            })
        }
        fn replay_guarantee(&self) -> crate::idempotency::ReplayGuarantee {
            crate::idempotency::ReplayGuarantee::Deterministic
        }
        fn supports_exactly_once(&self) -> bool {
            true
        }
    }

    /// #458: exactly-once *can* order sinks, because `seq` is a monotonic counter.
    /// Resume from the furthest-behind sink; the ones ahead skip via their tokens.
    #[test]
    fn eo_resume_picks_the_lowest_sequence() {
        let a = (7u64, Some(json!({"pos": 7})));
        let b = (4u64, Some(json!({"pos": 4})));
        let c = (9u64, Some(json!({"pos": 9})));
        assert_eq!(
            eo_start_bookmark(&[a.clone(), b.clone(), c.clone()], 1),
            Some(json!({"pos": 4})),
            "resume from the laggard, not the leader"
        );
        // Still never cross-applies in a multi-source graph.
        assert_eq!(eo_start_bookmark(&[a, b, c], 2), None);
        assert_eq!(eo_start_bookmark(&[], 1), None);
    }

    /// The EO envelope must be unwrapped on read — a raw `get` would hand the
    /// source `{"__faucet_eo": …}` instead of its bookmark.
    #[tokio::test]
    async fn eo_resume_unwraps_the_state_envelope() {
        let store = Arc::new(MemoryStateStore::new());
        store
            .put("p::k", &wrap_state(Some(&json!({"pos": 5})), 5))
            .await
            .unwrap();
        let opts = TopologyOptions::new("p").with_state_store(store.clone());
        let bm =
            compute_start_bookmark(&opts, &["k".to_string()], 1, DeliveryMode::ExactlyOnce).await;
        assert_eq!(bm, Some(json!({"pos": 5})), "envelope must be unwrapped");
        // A bare token round-trips through parse_token_parts the same way.
        let t = format_token_with_bookmark(5, Some(&json!({"pos": 5})));
        assert!(t.contains('#'), "token embeds the bookmark: {t}");
    }

    /// #459: the CLI needs to know *which* sink node failed to notify per node.
    #[tokio::test]
    async fn run_reported_attributes_failures_to_their_node() {
        let (good, _) = CollectSink::new();
        let topo = Topology::builder()
            .source("s", VecSource::boxed(recs(4)))
            .tee("t", 4, Some(2))
            .sink("bad", Box::new(FailingSink))
            .sink("good", Box::new(good))
            .edge("s", "t")
            .edge("t", "bad")
            .edge("t", "good")
            .build()
            .unwrap();
        let run = topo
            .run_reported(
                TopologyOptions::new("p").with_on_error(TopologyOnError::Continue),
                TopologyGovernance::new(),
            )
            .await
            .unwrap();

        let bad = run.nodes.iter().find(|n| n.node_id == "bad").unwrap();
        assert!(bad.error.is_some(), "the failing sink is attributed");
        let good = run.nodes.iter().find(|n| n.node_id == "good").unwrap();
        assert!(good.error.is_none(), "the healthy sink is not");
        assert_eq!(good.records, 4);
        // Every node appears, with its kind.
        assert_eq!(run.nodes.len(), 4);
        assert!(run.nodes.iter().any(|n| n.kind == "source"));
        assert!(run.nodes.iter().any(|n| n.kind == "tee"));
    }
}
