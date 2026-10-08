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
//! restart the source resumes from the sinks' stored bookmark only when *every*
//! sink has one and they all agree (exactly-once orders committed positions
//! instead — see below); otherwise the source replays in full, so sinks that
//! may diverge must be idempotent — a faster sink will re-see already-written
//! pages. A state-store read error fails the run rather than replaying.
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
//! - **Exactly-once orders by committed position.** Each sink's position comes
//!   from its state, overridden by the bookmark embedded in its commit token
//!   when the token is ahead. The source resumes from the earliest position;
//!   a sink ahead of it skips what it already committed — by position when the
//!   source can order its positions ([`Source::position_le`]), otherwise by its
//!   commit-token sequence, with every sink started at the laggard's sequence so
//!   the sequences keep comparing across sinks.
//! - **A merge that rejoins one source's copies gates the positions.** After a
//!   tee fans a source out and a merge joins the branches back, a position is
//!   forwarded only once every branch has delivered it, so a sink never persists
//!   a position while another branch's copy of that page is still in flight. A
//!   join may not read both of its sides from one upstream node: it drains its
//!   build side first, which would block the shared upstream forever.
//!
//! A sink node in `write_mode: overwrite` stages before its first write and swaps
//! its staging in — then persists its position — only once every node of the
//! graph has succeeded and the run was not cancelled; otherwise it discards the
//! staging. A sink whose input closed because an upstream node failed reports
//! that failure instead of a successful, partial run.
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

/// Typed class of a node's failure, for callers that react to a *kind* of
/// failure rather than report it (PRINCIPLES §6 — never recover a class by
/// grepping a message).
///
/// Non-exhaustive: new classes are additive, so matching must carry a `_` arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum NodeErrorKind {
    /// [`FaucetError::CircuitOpen`] — the resilience circuit breaker tripped.
    /// `faucet schedule` delays its next tick by the policy cooldown rather
    /// than re-firing immediately at a destination that just tripped.
    CircuitOpen,
    /// Any other failure. Deliberately distinct from `None`, which means the
    /// node did not fail at all.
    Other,
}

impl NodeErrorKind {
    /// Classify a node's failure while the typed error is still intact.
    ///
    /// One table, at the boundary where the error is stringified — the whole
    /// point of the field is that no consumer has to re-derive this from prose.
    pub fn classify(error: &FaucetError) -> Self {
        match error {
            FaucetError::CircuitOpen { .. } => Self::CircuitOpen,
            _ => Self::Other,
        }
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
    /// Typed class of [`error`](Self::error), for callers that react to a kind
    /// of failure rather than render it. `None` when the node succeeded.
    ///
    /// Additive on a `#[non_exhaustive]` struct, so a minor bump under the
    /// declared semver contract.
    pub error_kind: Option<NodeErrorKind>,
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
    from: String,
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
        /// Kept so a fully successful graph can finalize it (#753).
        sink: Box<dyn Sink>,
        /// Where an overwrite sink's bookmark goes once its staging is
        /// swapped in (#789 CORE-14).
        deferred: Option<DeferredState>,
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
                NodeKind::Source(source) => {
                    has_source = true;
                    arity(&n.id, "source", i == 0, o == 1, "0 in, exactly 1 out")?;
                    if source.consumes_destructively() {
                        return Err(cfg(format!(
                            "source '{}' ({}) acknowledges messages as it is read past each \
                             page, and a graph polls its sources ahead of its sinks, so a \
                             failed or cancelled sink would lose messages that were already \
                             acked; run this source in a linear pipeline instead",
                            n.id,
                            source.connector_name()
                        )));
                    }
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
        self.check_join_inputs()?;
        Ok(())
    }

    /// A join drains its whole build side before it reads the probe side, so its
    /// two inputs must not share an upstream node. A tee feeding both sides blocks
    /// on the probe send once that channel fills, the build side never closes, and
    /// the run hangs (#789 CORE-13).
    fn check_join_inputs(&self) -> Result<(), FaucetError> {
        let mut rev: HashMap<&str, Vec<&Edge>> = HashMap::new();
        for e in &self.edges {
            rev.entry(e.to.as_str()).or_default().push(e);
        }
        for n in &self.nodes {
            let NodeKind::Join(j) = &n.kind else {
                continue;
            };
            let side = |label: &str| -> Option<HashSet<String>> {
                let from = rev
                    .get(n.id.as_str())?
                    .iter()
                    .find(|e| e.label.as_deref() == Some(label))?
                    .from
                    .as_str();
                let mut set = ancestors_of(from, &rev);
                set.insert(from.to_string());
                Some(set)
            };
            let (Some(build), Some(probe)) = (side(&j.build_edge), side(&j.probe_edge)) else {
                continue;
            };
            let mut shared: Vec<&String> = build.intersection(&probe).collect();
            shared.sort();
            if let Some(node) = shared.first() {
                return Err(cfg(format!(
                    "join '{}' reads both its build and probe sides from node '{node}'; the \
                     join drains its build side before reading the probe side, so a shared \
                     upstream blocks once the probe channel fills and the run never ends. \
                     Feed the two sides from separate source nodes",
                    n.id
                )));
            }
        }
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
        match self.run_attributed(opts, governance).await {
            (_, Some(e)) => Err(e),
            (run, None) => Ok(run),
        }
    }

    /// [`Topology::run_reported`] that keeps the per-node attribution when the
    /// run fails.
    ///
    /// `run_reported` returns only the error under [`TopologyOnError::Propagate`],
    /// so a caller reporting per sink node (notifications, SLA, lineage) had
    /// nothing to report from and skipped the failure entirely (#789 CLI-18).
    /// Here the [`TopologyRun`] is always returned, alongside the error that
    /// `run_reported` would have returned. A sink that a failed node feeds is
    /// reported as failed too: its input closing looked like a normal end of
    /// stream, but it never received its complete input.
    pub async fn run_attributed(
        self,
        opts: TopologyOptions,
        governance: TopologyGovernance,
    ) -> (TopologyRun, Option<FaucetError>) {
        if let Err(e) = self.validate() {
            return (TopologyRun::default(), Some(e));
        }
        let facts = GraphFacts::of(&self.nodes, &self.edges);
        let Topology { nodes, edges } = self;

        let order: Vec<(String, &'static str)> = nodes
            .iter()
            .map(|n| (n.id.clone(), n.kind.kind_str()))
            .collect();

        // Sources are shared (`Arc`) so a sink node can ask the graph's source to
        // order two of its positions when it skips pages it already committed.
        let mut sources: HashMap<String, Arc<dyn Source>> = HashMap::new();
        let mut rest: Vec<Node> = Vec::with_capacity(nodes.len());
        for n in nodes {
            match n.kind {
                NodeKind::Source(s) => {
                    sources.insert(n.id, Arc::from(s));
                }
                kind => rest.push(Node { id: n.id, kind }),
            }
        }
        let source_count = sources.len();
        let only_source: Option<Arc<dyn Source>> = if source_count == 1 {
            sources.values().next().cloned()
        } else {
            None
        };

        // Capacity per outgoing edge: a tee's edges use its configured
        // capacity; everything else uses the default.
        let tee_cap: HashMap<String, usize> = rest
            .iter()
            .filter_map(|n| match &n.kind {
                NodeKind::Tee { capacity, .. } => Some((n.id.clone(), *capacity)),
                _ => None,
            })
            .collect();

        // The graph's source replay capability. Only meaningful with exactly one
        // source — which is also the only shape exactly-once is allowed in (#458).
        let source_replay = only_source.as_ref().map(|s| s.replay_guarantee());
        // How sink nodes store their bookmarks (#736): owned by the graph's
        // only source when there is one; a multi-source graph never resumes
        // from a sink bookmark, so its bookmarks are owned by the graph.
        let codec = only_source
            .as_ref()
            .map(|s| crate::state_version::StateCodec::for_source(s.as_ref(), false))
            .unwrap_or(crate::state_version::StateCodec {
                owner: "topology".into(),
                schema: 0,
                legacy: false,
            });

        let sink_refs: Vec<(String, Option<&dyn Sink>)> = rest
            .iter()
            .filter_map(|n| match &n.kind {
                NodeKind::Sink(s) => Some((n.id.clone(), Some(s.as_ref()))),
                _ => None,
            })
            .collect();
        let plan = match compute_resume(
            &opts,
            &sink_refs,
            source_count,
            governance.delivery,
            only_source.as_deref(),
        )
        .await
        {
            Ok(p) => p,
            Err(e) => return (TopologyRun::default(), Some(e)),
        };
        drop(sink_refs);

        // Every node runs under one cooperative token: a caller's cancel reaches
        // it, and under `Propagate` a node failure cancels it so the siblings stop
        // at their next page boundary — including a node parked on a full or empty
        // channel, which a token checked only after `recv` returns never reaches
        // (#789 CORE-13).
        let coop = opts.cancel.clone().unwrap_or_default().child_token();
        let failed: FailedNodes = Arc::new(std::sync::Mutex::new(HashMap::new()));

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
                from: e.from.clone(),
                rx,
            });
        }

        // Build one future per node. `Send + 'static` so each node can own a
        // task (see the spawn below).
        type NodeFut = Pin<Box<dyn Future<Output = Result<NodeOutcome, FaucetError>> + Send>>;
        let mut by_id: HashMap<String, NodeFut> = HashMap::new();

        for (id, source) in sources {
            let node_outs = outs.remove(&id).unwrap_or_default();
            let keep = node_outs.clone();
            let fut: NodeFut = Box::pin(run_source_node(
                id.clone(),
                source,
                plan.start.clone(),
                opts.batch_size,
                node_outs,
                coop.clone(),
            ));
            by_id.insert(
                id.clone(),
                track_failure(id, fut, keep, Arc::clone(&failed)),
            );
        }

        for node in rest {
            let node_outs = outs.remove(&node.id).unwrap_or_default();
            let keep = node_outs.clone();
            let mut node_ins = ins.remove(&node.id).unwrap_or_default();
            let pipeline = opts.pipeline_name.clone();
            let cancel = coop.clone();
            let Node { id, kind } = node;

            let built: Result<NodeFut, FaucetError> = match kind {
                NodeKind::Source(_) => Err(cfg(format!("source '{id}' was not prepared"))),
                NodeKind::Transform(stages) => take_single(node_ins)
                    .ok_or_else(|| cfg(format!("transform '{id}' has no input edge")))
                    .map(|rx| {
                        let labels = Labels::new(pipeline.clone(), id.clone(), opts.run_id.clone());
                        Box::pin(run_transform_node(stages, labels, rx, node_outs, cancel))
                            as NodeFut
                    }),
                NodeKind::Tee { .. } => take_single(node_ins)
                    .ok_or_else(|| cfg(format!("tee '{id}' has no input edge")))
                    .map(|rx| {
                        Box::pin(run_tee_node(id.clone(), pipeline, rx, node_outs, cancel))
                            as NodeFut
                    }),
                NodeKind::Merge => {
                    let rxs: Vec<mpsc::Receiver<StreamPage>> =
                        node_ins.into_iter().map(|ie| ie.rx).collect();
                    let mode = facts
                        .merge_modes
                        .get(&id)
                        .copied()
                        .unwrap_or(MergeMode::Passthrough);
                    Ok(Box::pin(run_merge_node(
                        id.clone(),
                        pipeline,
                        rxs,
                        node_outs,
                        cancel,
                        mode,
                    )) as NodeFut)
                }
                NodeKind::Join(j) => {
                    // Every node feeding the build side: a failure among them
                    // leaves the hash table incomplete.
                    let build_upstream: HashSet<String> = node_ins
                        .iter()
                        .find(|ie| ie.label.as_deref() == Some(j.build_edge.as_str()))
                        .map(|ie| {
                            let mut set =
                                facts.ancestors.get(&ie.from).cloned().unwrap_or_default();
                            set.insert(ie.from.clone());
                            set
                        })
                        .unwrap_or_default();
                    let build_rx = take_by_label(&mut node_ins, &j.build_edge);
                    let probe_rx = take_by_label(&mut node_ins, &j.probe_edge);
                    match (build_rx, probe_rx) {
                        (Some(b), Some(p)) => Ok(Box::pin(run_join_node(
                            id.clone(),
                            pipeline,
                            j,
                            JoinInputs {
                                build_rx: b,
                                probe_rx: p,
                                build_upstream,
                                failed: Arc::clone(&failed),
                            },
                            node_outs,
                            cancel,
                        )) as NodeFut),
                        _ => Err(cfg(format!(
                            "join '{id}' is missing its build/probe input edges"
                        ))),
                    }
                }
                NodeKind::Sink(sink) => match take_single(node_ins) {
                    None => Err(cfg(format!("sink '{id}' has no input edge"))),
                    Some(rx) => {
                        let resume = plan.sinks.get(&id).cloned();
                        let sopts = SinkNodeOpts {
                            pipeline_name: pipeline,
                            run_id: opts.run_id.clone(),
                            state_store: opts.state_store.clone(),
                            dlq: opts.dlq.clone(),
                            cancel: cancel.clone(),
                            // Masking is destination-scoped, so each sink node
                            // takes the policy compiled for it (if any); the rest
                            // are pipeline-wide.
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
                            codec: codec.clone(),
                            resume,
                            position_source: only_source.clone(),
                            upstream: facts.ancestors.get(&id).cloned().unwrap_or_default(),
                            failed: Arc::clone(&failed),
                        };
                        Ok(Box::pin(run_sink_node(id.clone(), sink, rx, sopts)) as NodeFut)
                    }
                },
            };
            match built {
                Ok(fut) => {
                    by_id.insert(
                        id.clone(),
                        track_failure(id, fut, keep, Arc::clone(&failed)),
                    );
                }
                Err(e) => return (TopologyRun::default(), Some(e)),
            }
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
        let futs: Vec<NodeFut> = order
            .iter()
            .filter_map(|(id, _)| by_id.remove(id))
            .collect();
        let handles: Vec<tokio::task::JoinHandle<Result<NodeOutcome, FaucetError>>> =
            futs.into_iter().map(tokio::spawn).collect();
        // Dropping a `JoinHandle` detaches the task rather than cancelling it, so
        // every abandon path below aborts explicitly.
        let aborts: Vec<tokio::task::AbortHandle> =
            handles.iter().map(|h| h.abort_handle()).collect();
        // A dropped run future (serve cancel / timeout past its flush grace)
        // must not leave node tasks writing and persisting bookmarks for a
        // run already reported as cancelled.
        let _abort_on_drop = AbortOnDrop(aborts.clone());
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

        let externally_cancelled = || cancelled(&opts.cancel);
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
                let first_err: Arc<std::sync::Mutex<Option<FaucetError>>> =
                    Arc::new(std::sync::Mutex::new(None));
                let failures: Arc<std::sync::Mutex<Vec<(String, String, NodeErrorKind)>>> =
                    Arc::new(std::sync::Mutex::new(Vec::new()));
                let wrapped = joined.zip(order.clone()).map(|(f, (node_id, _))| {
                    let coop = coop.clone();
                    let slot = Arc::clone(&first_err);
                    let failed = Arc::clone(&failures);
                    async move {
                        match f.await {
                            Ok(o) => Some(o),
                            Err(e) => {
                                failed.lock().unwrap_or_else(|p| p.into_inner()).push((
                                    node_id,
                                    e.to_string(),
                                    NodeErrorKind::classify(&e),
                                ));
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
                let outcomes: Vec<NodeOutcome> = outcomes.into_iter().flatten().collect();
                let err = first_err.lock().unwrap_or_else(|p| p.into_inner()).take();
                let complete = err.is_none() && !externally_cancelled();
                let finish = finish_sinks(&outcomes, complete).await;
                let (result, per_source) = aggregate(outcomes);
                let failed = failures.lock().unwrap_or_else(|p| p.into_inner()).clone();
                let nodes = reports(&order, &result, &per_source, &failed, &facts.ancestors);
                let run = TopologyRun {
                    result,
                    nodes,
                    per_source,
                };
                (run, err.or(finish.err()))
            }
            TopologyOnError::Continue => {
                let results = futures::future::join_all(joined).await;
                let mut ok = Vec::new();
                let mut errs = Vec::new();
                let mut failed: Vec<(String, String, NodeErrorKind)> = Vec::new();
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
                            failed.push((node_id, e.to_string(), NodeErrorKind::classify(&e)));
                        }
                    }
                }
                let complete = errs.is_empty() && !externally_cancelled();
                let finish = finish_sinks(&ok, complete).await;
                let (mut result, per_source) = aggregate(ok);
                result.errors = errs;
                let nodes = reports(&order, &result, &per_source, &failed, &facts.ancestors);
                let run = TopologyRun {
                    result,
                    nodes,
                    per_source,
                };
                (run, finish.err())
            }
        }
    }
}

/// Node ids that failed, with their error text and class — the shared record a
/// sink node consults when its input closes, so it can tell a finished stream
/// from one cut short by an upstream failure.
type FailedNodes = Arc<std::sync::Mutex<HashMap<String, (String, NodeErrorKind)>>>;

/// Run a node future and record a failure **before** its output channels close.
///
/// `keep` holds clones of the node's senders: the node's own copies drop when
/// its future returns, but a downstream receiver only sees end-of-stream once
/// these clones drop too — after the failure is recorded. Without that ordering a
/// sink could observe a closed input, find no failure yet, and report a partial
/// input as a complete, successful run (#789 CLI-18).
fn track_failure(
    id: String,
    fut: Pin<Box<dyn Future<Output = Result<NodeOutcome, FaucetError>> + Send>>,
    keep: Vec<mpsc::Sender<StreamPage>>,
    failed: FailedNodes,
) -> Pin<Box<dyn Future<Output = Result<NodeOutcome, FaucetError>> + Send>> {
    Box::pin(async move {
        let result = fut.await;
        if let Err(e) = &result {
            failed
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(id, (e.to_string(), NodeErrorKind::classify(e)));
        }
        drop(keep);
        result
    })
}

/// How a merge node treats the bookmarks riding on its input pages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MergeMode {
    /// The inputs carry positions of different sources: forward them as they
    /// arrive. (A multi-source graph never resumes from them anyway.)
    Passthrough,
    /// Every input carries the same single source's positions — a tee fanned the
    /// source out and this merge joins the copies back. A position is released
    /// only once **every** input has delivered it, so no sink can persist a
    /// bookmark while another branch's copy of that page is still in flight
    /// (#789 CORE-12).
    Gated,
    /// Some inputs share a source and some do not, so their positions cannot be
    /// lined up: forward pages without bookmarks. The sink then never persists a
    /// position it cannot prove every branch delivered, and the next run replays
    /// in full — duplicates on a non-idempotent sink, never a skipped record.
    Strip,
}

/// Facts derived from the graph shape alone.
struct GraphFacts {
    /// Every node upstream of each node (transitively), excluding the node.
    ancestors: HashMap<String, HashSet<String>>,
    /// How each merge node treats its inputs' bookmarks.
    merge_modes: HashMap<String, MergeMode>,
}

impl GraphFacts {
    fn of(nodes: &[Node], edges: &[Edge]) -> Self {
        let mut rev: HashMap<&str, Vec<&Edge>> = HashMap::new();
        for e in edges {
            rev.entry(e.to.as_str()).or_default().push(e);
        }
        let ancestors: HashMap<String, HashSet<String>> = nodes
            .iter()
            .map(|n| (n.id.clone(), ancestors_of(&n.id, &rev)))
            .collect();

        // The sources whose positions arrive on a node's output pages: a source
        // emits its own, a transform/tee/sink passes its input's on, a merge
        // carries the union of its inputs', and a join only its probe side's.
        let kinds: HashMap<&str, &NodeKind> =
            nodes.iter().map(|n| (n.id.as_str(), &n.kind)).collect();
        let mut origins: HashMap<String, HashSet<String>> = HashMap::new();
        for n in nodes {
            bookmark_origins(&n.id, &kinds, &rev, &mut origins);
        }

        let mut merge_modes = HashMap::new();
        for n in nodes {
            if !matches!(n.kind, NodeKind::Merge) {
                continue;
            }
            let inputs: Vec<HashSet<String>> = rev
                .get(n.id.as_str())
                .map(|es| {
                    es.iter()
                        .map(|e| origins.get(&e.from).cloned().unwrap_or_default())
                        .collect()
                })
                .unwrap_or_default();
            merge_modes.insert(n.id.clone(), merge_mode(&inputs));
        }
        Self {
            ancestors,
            merge_modes,
        }
    }
}

/// Every node upstream of `id`, excluding `id`.
fn ancestors_of(id: &str, rev: &HashMap<&str, Vec<&Edge>>) -> HashSet<String> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut stack: Vec<&str> = rev
        .get(id)
        .map(|es| es.iter().map(|e| e.from.as_str()).collect())
        .unwrap_or_default();
    while let Some(n) = stack.pop() {
        if !seen.insert(n.to_string()) {
            continue;
        }
        if let Some(es) = rev.get(n) {
            stack.extend(es.iter().map(|e| e.from.as_str()));
        }
    }
    seen
}

fn bookmark_origins(
    id: &str,
    kinds: &HashMap<&str, &NodeKind>,
    rev: &HashMap<&str, Vec<&Edge>>,
    memo: &mut HashMap<String, HashSet<String>>,
) -> HashSet<String> {
    if let Some(o) = memo.get(id) {
        return o.clone();
    }
    let inputs: Vec<&Edge> = rev.get(id).cloned().unwrap_or_default();
    let origins: HashSet<String> = match kinds.get(id) {
        Some(NodeKind::Source(_)) => HashSet::from([id.to_string()]),
        Some(NodeKind::Join(j)) => inputs
            .iter()
            .filter(|e| e.label.as_deref() == Some(j.probe_edge.as_str()))
            .flat_map(|e| bookmark_origins(&e.from, kinds, rev, memo))
            .collect(),
        _ => inputs
            .iter()
            .flat_map(|e| bookmark_origins(&e.from, kinds, rev, memo))
            .collect(),
    };
    memo.insert(id.to_string(), origins.clone());
    origins
}

/// Pure merge-mode decision over the bookmark origins of each input.
fn merge_mode(inputs: &[HashSet<String>]) -> MergeMode {
    let disjoint = inputs
        .iter()
        .enumerate()
        .all(|(i, a)| inputs[i + 1..].iter().all(|b| a.is_disjoint(b)));
    if disjoint {
        return MergeMode::Passthrough;
    }
    let first = &inputs[0];
    if first.len() == 1 && inputs.iter().all(|o| o == first) {
        MergeMode::Gated
    } else {
        MergeMode::Strip
    }
}

/// Releases a source position only once every live input of a gated merge has
/// delivered it.
///
/// The inputs of a [`MergeMode::Gated`] merge are copies of one source's pages,
/// so each sees the same positions in the same order. Counting how many
/// bookmarked pages each input has delivered is therefore enough: position `k`
/// is safe once every live input has delivered `k` pages. A closed input has
/// delivered everything it ever will, so it no longer holds the others back.
#[derive(Debug)]
struct BookmarkGate {
    delivered: Vec<u64>,
    closed: Vec<bool>,
    released: u64,
    /// Positions seen but not yet released; `pending[0]` is position
    /// `released + 1`.
    pending: std::collections::VecDeque<Value>,
}

impl BookmarkGate {
    fn new(inputs: usize) -> Self {
        Self {
            delivered: vec![0; inputs],
            closed: vec![false; inputs],
            released: 0,
            pending: std::collections::VecDeque::new(),
        }
    }

    /// A page carrying `bookmark` arrived on `input`. Returns the bookmark to
    /// attach to the forwarded page, if this delivery released one.
    fn arrive(&mut self, input: usize, bookmark: Value) -> Option<Value> {
        self.delivered[input] += 1;
        let seen = self.released + self.pending.len() as u64;
        if self.delivered[input] > seen {
            self.pending.push_back(bookmark);
        }
        self.release()
    }

    /// `input` has closed. Returns a position released by its absence, if any.
    fn close(&mut self, input: usize) -> Option<Value> {
        self.closed[input] = true;
        self.release()
    }

    fn release(&mut self) -> Option<Value> {
        let live_min = self
            .delivered
            .iter()
            .zip(&self.closed)
            .filter(|(_, closed)| !**closed)
            .map(|(d, _)| *d)
            .min()
            .unwrap_or_else(|| self.released + self.pending.len() as u64);
        let mut out = None;
        while self.released < live_min {
            match self.pending.pop_front() {
                Some(bm) => {
                    self.released += 1;
                    out = Some(bm);
                }
                None => break,
            }
        }
        out
    }
}

/// Build the per-node report list from the node ids/kinds and their outcomes.
///
/// A sink that a failed node feeds is reported as failed even when it returned
/// normally: its input closing looked like the end of the stream, but the run
/// never delivered its complete input (#789 CLI-18).
fn reports(
    order: &[(String, &'static str)],
    sinks: &TopologyResult,
    per_source: &HashMap<String, usize>,
    errors: &[(String, String, NodeErrorKind)],
    ancestors: &HashMap<String, HashSet<String>>,
) -> Vec<NodeReport> {
    order
        .iter()
        .map(|(id, kind)| {
            let own = errors.iter().find(|(nid, _, _)| nid == id);
            let upstream = (own.is_none() && *kind == "sink")
                .then(|| {
                    errors
                        .iter()
                        .find(|(nid, _, _)| ancestors.get(id).is_some_and(|a| a.contains(nid)))
                })
                .flatten();
            let (error, error_kind) = match (own, upstream) {
                (Some((_, e, k)), _) => (Some(e.clone()), Some(*k)),
                (None, Some((nid, e, k))) => (
                    Some(format!(
                        "upstream node '{nid}' failed, so this sink did not receive its \
                         complete input: {e}"
                    )),
                    Some(*k),
                ),
                (None, None) => (None, None),
            };
            NodeReport {
                node_id: id.clone(),
                kind,
                records: sinks
                    .per_sink
                    .get(id)
                    .or_else(|| per_source.get(id))
                    .copied()
                    .unwrap_or(0),
                bookmark: sinks.bookmarks.get(id).cloned().flatten(),
                error,
                error_kind,
            }
        })
        .collect()
}

/// Finalize the sink nodes once every node has stopped.
///
/// `complete` is true only when **no** node failed and the run was not
/// cancelled — the graph analogue of `Pipeline::run`'s decision. A sink node
/// sees only its own input close, which also happens when an upstream node
/// failed, so the decision is graph-wide:
///
/// - **Complete:** every `write_mode: overwrite` sink swaps its staging in
///   (`commit_overwrite`) and only then persists its bookmark, which `run_stream`
///   held back; then every sink's [`Sink::complete_run`] runs (#753). A commit
///   that fails discards the remaining sinks' staging rather than swapping a
///   subset in.
/// - **Otherwise:** every overwrite sink discards its staging, leaving its
///   destination as it was (#789 CORE-14).
async fn finish_sinks(outcomes: &[NodeOutcome], complete: bool) -> Result<(), FaucetError> {
    struct Finish<'a> {
        sink: &'a dyn Sink,
        deferred: Option<&'a DeferredState>,
        bookmark: Option<&'a Value>,
    }
    let sinks: Vec<Finish<'_>> = outcomes
        .iter()
        .filter_map(|o| match o {
            NodeOutcome::Sink {
                sink,
                deferred,
                bookmark,
                ..
            } => Some(Finish {
                sink: sink.as_ref(),
                deferred: deferred.as_ref(),
                bookmark: bookmark.as_ref(),
            }),
            _ => None,
        })
        .collect();
    if !complete {
        for f in &sinks {
            if f.sink.is_overwrite() {
                abort_quietly(f.sink).await;
            }
        }
        return Ok(());
    }
    for (i, f) in sinks.iter().enumerate() {
        if !f.sink.is_overwrite() {
            continue;
        }
        if let Err(e) = f.sink.commit_overwrite().await {
            for rest in &sinks[i + 1..] {
                if rest.sink.is_overwrite() {
                    abort_quietly(rest.sink).await;
                }
            }
            return Err(e);
        }
        if let (Some(d), Some(bm)) = (f.deferred, f.bookmark) {
            d.store.put(&d.key, bm).await?;
        }
    }
    for f in &sinks {
        f.sink.complete_run().await?;
    }
    Ok(())
}

async fn abort_quietly(sink: &dyn Sink) {
    if let Err(e) = sink.abort_overwrite().await {
        tracing::warn!(
            error = %e,
            "failed to discard overwrite staging after an unsuccessful or cancelled \
             topology run; the destination is unchanged"
        );
    }
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
                ..
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

/// Where one sink node resumes under exactly-once delivery.
#[derive(Debug, Clone, PartialEq)]
struct SinkResume {
    /// The commit-token sequence the node's next page follows.
    start_seq: u64,
    /// Skip replayed pages whose position is at or before this one: the node
    /// already committed them. Set only for a node ahead of the replay start,
    /// and only when the source can order its positions.
    skip_through: Option<Value>,
}

/// The source's resume point plus each sink node's resume state.
#[derive(Debug, Default)]
struct ResumePlan {
    start: Option<Value>,
    sinks: HashMap<String, SinkResume>,
}

/// Aborts every node task when dropped; a no-op for tasks that finished.
struct AbortOnDrop(Vec<tokio::task::AbortHandle>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        for a in &self.0 {
            a.abort();
        }
    }
}

/// Read every sink node's stored bookmark and decide the source's resume point.
///
/// Returns `Some(bookmark)` only when it is provably safe to resume there;
/// `None` means "replay in full", which costs duplicates on a non-idempotent
/// sink but can never skip a record. See [`start_bookmark`] for the rules.
#[cfg(test)]
async fn compute_start_bookmark(
    opts: &TopologyOptions,
    sink_ids: &[String],
    source_count: usize,
    delivery: crate::idempotency::DeliveryMode,
    source: Option<&dyn Source>,
) -> Result<Option<Value>, FaucetError> {
    let sinks: Vec<(String, Option<&dyn Sink>)> =
        sink_ids.iter().map(|id| (id.clone(), None)).collect();
    compute_resume(opts, &sinks, source_count, delivery, source)
        .await
        .map(|p| p.start)
}

/// Decide the source's resume point and, under exactly-once delivery, where each
/// sink node resumes.
///
/// At-least-once follows [`start_bookmark`]. Exactly-once is ordered by each
/// sink's committed position, recovered the way `Pipeline::run` does: the state
/// store's `(bookmark, seq)`, overridden by the bookmark embedded in the sink's
/// own commit token when the token is ahead — the crash window between "sink
/// committed" and "state persisted" (#789 CORE-11). Then:
///
/// - **All sinks at one position:** resume there; nothing is replayed.
/// - **Positions differ, and the source orders them** ([`Source::position_le`]):
///   resume from the earliest; each sink ahead of it skips the replayed pages at
///   or before its own committed position and keeps its own sequence. Page
///   boundaries need not replay identically.
/// - **Otherwise:** resume from the lowest sequence, and start **every** sink at
///   that sequence, so a sink ahead skips by its committed token and every
///   sink's sequence keeps counting the same pages. Starting each at its own
///   sequence made a sink ahead re-write the replayed pages and left sequences
///   that no longer compared across sinks (#789 CORE-01).
///
/// A sink with no committed position forces a full replay, with every sink
/// starting from zero (or skipping by position when the source can order them).
async fn compute_resume(
    opts: &TopologyOptions,
    sinks: &[(String, Option<&dyn Sink>)],
    source_count: usize,
    delivery: crate::idempotency::DeliveryMode,
    source: Option<&dyn Source>,
) -> Result<ResumePlan, FaucetError> {
    let Some(store) = opts.state_store.as_ref() else {
        return Ok(ResumePlan::default());
    };
    if sinks.is_empty() {
        return Ok(ResumePlan::default());
    }
    let mut stored: Vec<(String, Option<Value>)> = Vec::with_capacity(sinks.len());
    for (id, _) in sinks {
        let key = format!("{}::{}", opts.pipeline_name, id);
        let value = match store.get(&key).await {
            // Refuse a bookmark the source cannot read; migrate an older one (#736).
            Ok(Some(v)) => Some(match source {
                Some(src) => crate::state_version::resolve_for_source(&key, &v, src)?.data,
                None => crate::state_version::peel_versioned(&v),
            }),
            Ok(None) => None,
            // A transient read error is not "no bookmark": replaying in full
            // on it would be a silent re-sync.
            Err(e) => return Err(e),
        };
        stored.push((id.clone(), value));
    }

    if delivery != crate::idempotency::DeliveryMode::ExactlyOnce {
        if stored.iter().any(|(_, v)| v.is_none()) {
            return Ok(ResumePlan::default());
        }
        let values: Vec<Value> = stored.into_iter().filter_map(|(_, v)| v).collect();
        return Ok(ResumePlan {
            start: start_bookmark(&values, source_count),
            sinks: HashMap::new(),
        });
    }

    let mut positions: Vec<(String, Option<Value>, u64)> = Vec::with_capacity(sinks.len());
    for ((id, sink), (_, value)) in sinks.iter().zip(stored) {
        let (mut bm, mut seq) = value
            .as_ref()
            .map(crate::idempotency::unwrap_state)
            .unwrap_or((None, 0));
        let key = format!("{}::{}", opts.pipeline_name, id);
        if let (Some(sink), Some(src)) = (sink, source)
            && sink.supports_idempotent_writes()
            && src.replay_guarantee() == crate::idempotency::ReplayGuarantee::Deterministic
            && let Some(token) = sink.last_committed_token(&key).await?
            && let Some((token_seq, Some(token_bm))) = crate::idempotency::parse_token_parts(&token)
            && token_seq > seq
        {
            bm = Some(token_bm);
            seq = token_seq;
        }
        positions.push((id.clone(), bm, seq));
    }
    Ok(plan_exactly_once(&positions, source_count, source))
}

/// Pure exactly-once resume decision over each sink's `(id, position, seq)`.
fn plan_exactly_once(
    positions: &[(String, Option<Value>, u64)],
    source_count: usize,
    source: Option<&dyn Source>,
) -> ResumePlan {
    let own = |skip: &dyn Fn(&Option<Value>) -> Option<Value>| -> HashMap<String, SinkResume> {
        positions
            .iter()
            .map(|(id, bm, seq)| {
                (
                    id.clone(),
                    SinkResume {
                        start_seq: *seq,
                        skip_through: skip(bm),
                    },
                )
            })
            .collect()
    };
    if source_count != 1 {
        if source_count > 1 {
            tracing::warn!(
                sources = source_count,
                "topology: multi-source graph cannot attribute a sink bookmark to a source; \
                 replaying every source in full"
            );
        }
        return ResumePlan {
            start: None,
            sinks: own(&|_| None),
        };
    }

    let known: Vec<Value> = positions
        .iter()
        .filter_map(|(_, bm, _)| bm.clone())
        .collect();
    let all_known = known.len() == positions.len();
    if all_known && known.iter().all(|b| b == &known[0]) {
        return ResumePlan {
            start: known.first().cloned(),
            sinks: own(&|_| None),
        };
    }

    let ordering = source.filter(|src| {
        known
            .first()
            .is_some_and(|b| src.position_le(b, b).is_some())
    });
    if let Some(src) = ordering {
        let start = if all_known {
            src.position_min(&known)
        } else {
            None
        };
        if start.is_some() || !all_known {
            let skip = |bm: &Option<Value>| bm.clone().filter(|b| Some(b) != start.as_ref());
            return ResumePlan {
                start: start.clone(),
                sinks: own(&skip),
            };
        }
    }

    let (start, base_seq) = if all_known {
        positions
            .iter()
            .min_by_key(|(_, _, seq)| *seq)
            .map(|(_, bm, seq)| (bm.clone(), *seq))
            .unwrap_or((None, 0))
    } else {
        (None, 0)
    };
    ResumePlan {
        start,
        sinks: positions
            .iter()
            .map(|(id, _, _)| {
                (
                    id.clone(),
                    SinkResume {
                        start_seq: base_seq,
                        skip_through: None,
                    },
                )
            })
            .collect(),
    }
}

/// Exactly-once resume point: the bookmark of the lowest-`seq` sink.
///
/// Unlike the at-least-once path this *can* order the sinks, because `seq` is a
/// monotonic counter rather than an opaque position — so a diverged set resumes
/// from the laggard instead of replaying from scratch. The runner starts every
/// sink at that laggard's sequence, so the sinks ahead skip their
/// already-committed pages via their commit tokens.
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

/// Receive the next page, or `None` once the channel closes or `cancel` fires.
async fn recv_or_cancel(
    rx: &mut mpsc::Receiver<StreamPage>,
    cancel: &CancellationToken,
) -> Option<StreamPage> {
    tokio::select! {
        biased;
        () = cancel.cancelled() => None,
        page = rx.recv() => page,
    }
}

/// Send `page` to every live output, moving into the last and cloning for the
/// rest. Closed (dropped-receiver) outputs are removed. Returns `false` once
/// every output has closed, or when `cancel` fires while a send is waiting on a
/// full channel — a send that waited forever is how a tee feeding both sides of
/// a join wedged the run (#789 CORE-13).
async fn broadcast(
    page: StreamPage,
    outs: &mut Vec<mpsc::Sender<StreamPage>>,
    cancel: &CancellationToken,
) -> bool {
    if outs.is_empty() {
        return false;
    }
    let last = outs.len() - 1;
    let mut page = Some(page);
    let mut closed: Vec<usize> = Vec::new();
    for (i, tx) in outs.iter().enumerate() {
        let item = if i == last { page.take() } else { page.clone() };
        let Some(item) = item else { break };
        tokio::select! {
            biased;
            () = cancel.cancelled() => return false,
            r = tx.send(item) => if r.is_err() { closed.push(i) },
        }
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
    source: Arc<dyn Source>,
    start_bookmark: Option<Value>,
    batch_size: usize,
    mut outs: Vec<mpsc::Sender<StreamPage>>,
    cancel: CancellationToken,
) -> Result<NodeOutcome, FaucetError> {
    if let Some(bm) = start_bookmark {
        source.apply_start_bookmark(bm).await?;
    }
    let ctx = std::collections::HashMap::new();
    let mut pages = source.stream_pages(&ctx, batch_size);
    let mut records = 0usize;
    loop {
        let item = tokio::select! {
            biased;
            () = cancel.cancelled() => break,
            item = pages.next() => item,
        };
        let Some(item) = item else { break };
        let page = item?;
        records += page.records.len();
        if !broadcast(page, &mut outs, &cancel).await {
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
    cancel: CancellationToken,
) -> Result<NodeOutcome, FaucetError> {
    while let Some(page) = recv_or_cancel(&mut rx, &cancel).await {
        let records = instrumented_apply_stages(page.records, &stages, &labels)?;
        let out = StreamPage {
            records,
            bookmark: page.bookmark,
        };
        if !broadcast(out, &mut outs, &cancel).await {
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
    cancel: CancellationToken,
) -> Result<NodeOutcome, FaucetError> {
    let labels = node_labels(&pipeline, &node_id);
    while let Some(page) = recv_or_cancel(&mut rx, &cancel).await {
        counter!("faucet_tee_records_total", labels.clone()).increment(page.records.len() as u64);
        if !broadcast(page, &mut outs, &cancel).await {
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
    cancel: CancellationToken,
    mode: MergeMode,
) -> Result<NodeOutcome, FaucetError> {
    let labels = node_labels(&pipeline, &node_id);
    if mode == MergeMode::Strip {
        tracing::warn!(
            node = %node_id,
            "topology: merge '{node_id}' combines inputs that share a source with inputs that \
             do not, so their positions cannot be lined up; it forwards no bookmarks and the \
             next run replays in full"
        );
    }
    let mut gate = BookmarkGate::new(rxs.len());
    let streams = rxs.into_iter().enumerate().map(|(i, mut rx)| {
        Box::pin(async_stream::stream! {
            while let Some(p) = rx.recv().await {
                yield (i, Some(p));
            }
            yield (i, None);
        }) as Pin<Box<dyn futures::Stream<Item = (usize, Option<StreamPage>)> + Send>>
    });
    let mut sel = futures::stream::select_all(streams);
    loop {
        let next = tokio::select! {
            biased;
            () = cancel.cancelled() => break,
            next = sel.next() => next,
        };
        let Some((input, page)) = next else { break };
        let out = match (mode, page) {
            (MergeMode::Gated, None) => match gate.close(input) {
                Some(bm) => StreamPage {
                    records: Vec::new(),
                    bookmark: Some(bm),
                },
                None => continue,
            },
            (_, None) => continue,
            (MergeMode::Gated, Some(p)) => StreamPage {
                bookmark: p.bookmark.and_then(|bm| gate.arrive(input, bm)),
                records: p.records,
            },
            (MergeMode::Strip, Some(p)) => StreamPage {
                records: p.records,
                bookmark: None,
            },
            (MergeMode::Passthrough, Some(p)) => p,
        };
        counter!("faucet_merge_records_total", labels.clone()).increment(out.records.len() as u64);
        if !broadcast(out, &mut outs, &cancel).await {
            break;
        }
    }
    Ok(NodeOutcome::Other)
}

/// A join node's two inputs, plus what it needs to tell a finished build side
/// from one cut short by a failure.
struct JoinInputs {
    build_rx: mpsc::Receiver<StreamPage>,
    probe_rx: mpsc::Receiver<StreamPage>,
    build_upstream: HashSet<String>,
    failed: FailedNodes,
}

async fn run_join_node(
    node_id: String,
    pipeline: String,
    j: JoinNode,
    inputs: JoinInputs,
    mut outs: Vec<mpsc::Sender<StreamPage>>,
    cancel: CancellationToken,
) -> Result<NodeOutcome, FaucetError> {
    let JoinInputs {
        mut build_rx,
        mut probe_rx,
        build_upstream,
        failed,
    } = inputs;
    let mode = j.config.mode;
    let mut join = HashJoin::new(j.config);

    // Build phase: fully drain the build side before probing.
    let build_start = std::time::Instant::now();
    while let Some(page) = recv_or_cancel(&mut build_rx, &cancel).await {
        join.add_build_page(page.records)?;
    }
    // A closed build channel is also what a failed upstream looks like; never
    // probe against a partial hash table (inner joins would drop rows, left
    // joins emit them unenriched). A failed node is recorded before its
    // senders close, so this check cannot race the close.
    if let Some((node, error)) = failed_upstream(&failed, &build_upstream) {
        return Err(FaucetError::Source(format!(
            "join '{node_id}': build-side node '{node}' failed, so its hash table is \
             incomplete and nothing was probed: {error}"
        )));
    }
    if cancel.is_cancelled() {
        return Ok(NodeOutcome::Other);
    }
    let labels = node_labels(&pipeline, &node_id);
    histogram!("faucet_join_build_duration_seconds", labels.clone())
        .record(build_start.elapsed().as_secs_f64());

    // Probe phase.
    while let Some(page) = recv_or_cancel(&mut probe_rx, &cancel).await {
        let enriched = join.probe_page(page.records)?;
        let out = StreamPage {
            records: enriched,
            bookmark: page.bookmark,
        };
        if !broadcast(out, &mut outs, &cancel).await {
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

/// Where an overwrite sink node's bookmark goes once its staging is swapped in.
struct DeferredState {
    store: Arc<dyn StateStore>,
    key: String,
}

struct SinkNodeOpts {
    pipeline_name: String,
    run_id: String,
    state_store: Option<Arc<dyn StateStore>>,
    dlq: Option<DlqConfig>,
    cancel: CancellationToken,
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
    /// How this node's bookmark is stored (#736).
    codec: crate::state_version::StateCodec,
    /// Where this node resumes under exactly-once delivery.
    resume: Option<SinkResume>,
    /// The graph's only source, which orders positions for `resume.skip_through`.
    position_source: Option<Arc<dyn Source>>,
    /// Every node upstream of this sink.
    upstream: HashSet<String>,
    /// Nodes that have failed so far.
    failed: FailedNodes,
}

/// The first failed node among `upstream`, if any.
fn failed_upstream(failed: &FailedNodes, upstream: &HashSet<String>) -> Option<(String, String)> {
    let failed = failed.lock().unwrap_or_else(|p| p.into_inner());
    let mut hits: Vec<(&String, &(String, NodeErrorKind))> = failed
        .iter()
        .filter(|(id, _)| upstream.contains(*id))
        .collect();
    hits.sort_by(|a, b| a.0.cmp(b.0));
    hits.first()
        .map(|(id, (msg, _))| ((*id).clone(), msg.clone()))
}

async fn run_sink_node(
    node_id: String,
    sink: Box<dyn Sink>,
    mut rx: mpsc::Receiver<StreamPage>,
    opts: SinkNodeOpts,
) -> Result<NodeOutcome, FaucetError> {
    let failed = Arc::clone(&opts.failed);
    let upstream = opts.upstream.clone();
    let position_source = opts.position_source.clone();
    let mut skip_through = opts.resume.as_ref().and_then(|r| r.skip_through.clone());
    let sink_id = node_id.clone();
    let pages = Box::pin(async_stream::stream! {
        loop {
            match rx.recv().await {
                Some(page) => {
                    // Exactly-once: this node already committed every page at or
                    // before `skip_through`, so a replay from an earlier sink's
                    // position must not write them again (#789 CORE-01).
                    if let (Some(through), Some(src)) = (skip_through.as_ref(), position_source.as_ref()) {
                        let committed = page
                            .bookmark
                            .as_ref()
                            .and_then(|bm| src.position_le(bm, through))
                            == Some(true);
                        if committed {
                            continue;
                        }
                        skip_through = None;
                    }
                    yield Ok::<StreamPage, FaucetError>(page);
                }
                None => {
                    if let Some((node, error)) = failed_upstream(&failed, &upstream) {
                        yield Err(FaucetError::Source(format!(
                            "upstream node '{node}' failed, so sink '{sink_id}' did not \
                             receive its complete input: {error}"
                        )));
                    }
                    break;
                }
            }
        }
    });

    let mut run_opts = RunStreamOptions::new()
        .with_name(opts.pipeline_name.clone())
        .with_row(node_id.clone())
        .with_run_id(opts.run_id.clone());
    let overwriting = sink.is_overwrite();
    let mut deferred = None;
    if let Some(store) = opts.state_store {
        let key = format!("{}::{}", opts.pipeline_name, node_id);
        let store: Arc<dyn StateStore> = Arc::new(crate::state_version::VersionedStateStore::new(
            store,
            key.clone(),
            opts.codec,
        ));
        // Exactly-once: this node's commit-token sequence resumes where the
        // graph-wide resume plan placed it (#458, #789 CORE-01).
        if opts.delivery == crate::idempotency::DeliveryMode::ExactlyOnce {
            let seq = match opts.resume.as_ref() {
                Some(r) => r.start_seq,
                None => match store.get(&key).await? {
                    Some(prior) => crate::idempotency::unwrap_state(&prior).1,
                    None => 0,
                },
            };
            run_opts = run_opts.with_delivery(opts.delivery).with_start_seq(seq);
            if let Some(replay) = opts.replay {
                run_opts = run_opts.with_replay_guarantee(replay);
            }
        }
        if overwriting {
            deferred = Some(DeferredState {
                store: Arc::clone(&store),
                key: key.clone(),
            });
        }
        run_opts = run_opts.with_state(store, key);
    }
    if let Some(dlq) = opts.dlq {
        run_opts = run_opts.with_dlq(dlq);
    }
    run_opts = run_opts.with_cancel(opts.cancel);
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

    // `write_mode: overwrite` stages before the first write; the swap happens
    // only once the whole graph has succeeded (`finish_sinks`), and a failure
    // here discards the staging so the destination is left as it was
    // (#789 CORE-14).
    if overwriting {
        sink.begin_overwrite().await?;
    }
    let result = match run_stream(pages, sink.as_ref(), run_opts).await {
        Ok(r) => r,
        Err(e) => {
            if overwriting {
                abort_quietly(sink.as_ref()).await;
            }
            return Err(e);
        }
    };
    Ok(NodeOutcome::Sink {
        node_id,
        records: result.records_written,
        bookmark: result.bookmark,
        sink,
        deferred,
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

    /// A sink that fails the way the resilience circuit breaker does, so a test
    /// can check the *class* survives rather than just the message (#658).
    pub(super) struct BreakerSink;
    #[async_trait]
    impl Sink for BreakerSink {
        async fn write_batch(&self, _records: &[Value]) -> Result<usize, FaucetError> {
            Err(FaucetError::CircuitOpen {
                failures: 3,
                cooldown: std::time::Duration::from_secs(30),
            })
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
    fn validate_rejects_a_source_that_consumes_destructively() {
        struct Queue;
        #[async_trait]
        impl Source for Queue {
            async fn fetch_with_context(
                &self,
                _: &std::collections::HashMap<String, Value>,
            ) -> Result<Vec<Value>, FaucetError> {
                Ok(Vec::new())
            }
            fn consumes_destructively(&self) -> bool {
                true
            }
            fn connector_name(&self) -> &'static str {
                "queue"
            }
        }
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        assert!(rt.block_on(Queue.fetch_all()).unwrap().is_empty());
        let (sink, _) = CollectSink::new();
        let err = Topology::builder()
            .source("q", Box::new(Queue))
            .sink("out", Box::new(sink))
            .edge("q", "out")
            .build()
            .unwrap_err()
            .to_string();
        assert!(err.contains("source 'q' (queue)"), "{err}");
    }

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
    async fn a_state_read_error_fails_the_run_instead_of_replaying() {
        struct Unreadable;
        #[async_trait]
        impl StateStore for Unreadable {
            async fn get(&self, _k: &str) -> Result<Option<Value>, FaucetError> {
                Err(FaucetError::State("store unreachable".into()))
            }
            async fn put(&self, _k: &str, _v: &Value) -> Result<(), FaucetError> {
                Ok(())
            }
            async fn delete(&self, _k: &str) -> Result<(), FaucetError> {
                Ok(())
            }
        }
        let (sink, store) = CollectSink::new();
        let topo = Topology::builder()
            .source("s", VecSource::boxed(recs(2)))
            .sink("k", Box::new(sink))
            .edge("s", "k")
            .build()
            .unwrap();
        let mut opts = TopologyOptions::new("p");
        opts.state_store = Some(Arc::new(Unreadable));
        let err = topo.run(opts).await.unwrap_err();
        assert!(err.to_string().contains("store unreachable"), "{err}");
        assert!(store.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_failed_build_side_is_never_probed() {
        /// One page, then a failure.
        struct HalfSource;
        #[async_trait]
        impl Source for HalfSource {
            async fn fetch_with_context(
                &self,
                _c: &std::collections::HashMap<String, Value>,
            ) -> Result<Vec<Value>, FaucetError> {
                Ok(Vec::new())
            }
            fn stream_pages<'a>(
                &'a self,
                _c: &'a std::collections::HashMap<String, Value>,
                _b: usize,
            ) -> Pin<Box<dyn crate::Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>>
            {
                Box::pin(futures::stream::iter(vec![
                    Ok(StreamPage {
                        records: vec![json!({"id": 1, "tier": "gold"})],
                        bookmark: None,
                    }),
                    Err(FaucetError::Source("build side broke".into())),
                ]))
            }
        }
        for on_error in [TopologyOnError::Continue, TopologyOnError::Propagate] {
            let (sink, store) = CollectSink::new();
            let jn = JoinNode {
                config: JoinConfig {
                    mode: JoinMode::Left,
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
                .source("c", Box::new(HalfSource))
                .source(
                    "o",
                    VecSource::boxed(vec![
                        json!({"order": "A", "cust": 1}),
                        json!({"order": "B", "cust": 2}),
                    ]),
                )
                .join("j", jn)
                .sink("k", Box::new(sink))
                .labelled_edge("c", "j", "customers")
                .labelled_edge("o", "j", "orders")
                .edge("j", "k")
                .build()
                .unwrap();
            let mut opts = TopologyOptions::new("p");
            opts.on_error = on_error;
            let (run, err) = topo
                .run_attributed(opts, TopologyGovernance::default())
                .await;
            let errors = format!("{err:?} {:?}", run.result.errors);
            assert!(errors.contains("build side broke"), "{errors}");
            assert!(
                store.lock().unwrap().is_empty(),
                "{on_error:?}: nothing probed"
            );
        }
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

    struct CompletingSink(Arc<Mutex<u32>>);
    #[async_trait]
    impl Sink for CompletingSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            Ok(records.len())
        }
        async fn complete_run(&self) -> Result<(), FaucetError> {
            *self.0.lock().unwrap() += 1;
            Ok(())
        }
    }

    #[tokio::test]
    async fn complete_run_fires_only_for_a_fully_successful_graph() {
        let done = Arc::new(Mutex::new(0));
        let topo = Topology::builder()
            .source("s", VecSource::boxed(recs(2)))
            .sink("k", Box::new(CompletingSink(done.clone())))
            .edge("s", "k")
            .build()
            .unwrap();
        topo.run(TopologyOptions::new("p")).await.unwrap();
        assert_eq!(*done.lock().unwrap(), 1);

        let failed = Arc::new(Mutex::new(0));
        let topo = Topology::builder()
            .source("s", Box::new(FailingSource))
            .sink("k", Box::new(CompletingSink(failed.clone())))
            .edge("s", "k")
            .build()
            .unwrap();
        assert!(topo.run(TopologyOptions::new("p")).await.is_err());
        assert_eq!(*failed.lock().unwrap(), 0);

        let partial = Arc::new(Mutex::new(0));
        let topo = Topology::builder()
            .source("s", VecSource::boxed(recs(4)))
            .tee("t", 4, Some(2))
            .sink("bad", Box::new(FailingSink))
            .sink("good", Box::new(CompletingSink(partial.clone())))
            .edge("s", "t")
            .edge("t", "bad")
            .edge("t", "good")
            .build()
            .unwrap();
        let opts = TopologyOptions::new("p").with_on_error(TopologyOnError::Continue);
        topo.run(opts).await.unwrap();
        assert_eq!(*partial.lock().unwrap(), 0);

        let cancelled = Arc::new(Mutex::new(0));
        let token = CancellationToken::new();
        token.cancel();
        let topo = Topology::builder()
            .source("s", VecSource::boxed(recs(2)))
            .sink("k", Box::new(CompletingSink(cancelled.clone())))
            .edge("s", "k")
            .build()
            .unwrap();
        let _ = topo.run(TopologyOptions::new("p").with_cancel(token)).await;
        assert_eq!(*cancelled.lock().unwrap(), 0);
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
        assert_eq!(
            store
                .get("p::k")
                .await
                .unwrap()
                .map(|v| crate::state_version::peel_versioned(&v)),
            Some(json!("v9"))
        );
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
            on_collision: Default::default(),
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
    use super::tests::{BreakerSink, CollectSink, FailingSink, VecSource, recs};
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
        let bm = compute_start_bookmark(
            &opts,
            &["k".to_string()],
            1,
            DeliveryMode::ExactlyOnce,
            None,
        )
        .await
        .unwrap();
        assert_eq!(bm, Some(json!({"pos": 5})), "envelope must be unwrapped");
        // A bare token round-trips through parse_token_parts the same way.
        let t = format_token_with_bookmark(5, Some(&json!({"pos": 5})));
        assert!(t.contains('#'), "token embeds the bookmark: {t}");
    }

    /// #658: the class of a node failure travels typed, not as prose.
    #[test]
    fn node_error_kind_classifies_only_the_breaker() {
        // The table is one line, but it is the whole point of the field: the
        // class comes from the typed error, so no consumer has to read prose.
        assert_eq!(
            NodeErrorKind::classify(&FaucetError::CircuitOpen {
                failures: 3,
                cooldown: std::time::Duration::from_secs(30),
            }),
            NodeErrorKind::CircuitOpen
        );
        // A message that merely *reads* like a trip is not one.
        assert_eq!(
            NodeErrorKind::classify(&FaucetError::Sink(
                "Circuit open after 3 consecutive failures".into()
            )),
            NodeErrorKind::Other
        );
    }

    #[tokio::test]
    async fn a_breaker_trip_is_reported_on_the_node_that_raised_it_only() {
        // The edge case #658 names: when one node trips and its siblings are
        // cancelled so they can flush, only the node that actually raised
        // `CircuitOpen` may carry that kind — otherwise the scheduler would
        // apply a breaker cooldown for an unrelated cancellation.
        let (healthy, _) = CollectSink::new();
        let topo = Topology::builder()
            .source("s", VecSource::boxed(recs(4)))
            .tee("t", 4, Some(2))
            .sink("tripped", Box::new(BreakerSink))
            .sink("healthy", Box::new(healthy))
            .edge("s", "t")
            .edge("t", "tripped")
            .edge("t", "healthy")
            .build()
            .unwrap();

        let run = topo
            .run_reported(
                TopologyOptions::new("p").with_on_error(TopologyOnError::Continue),
                TopologyGovernance::new(),
            )
            .await
            .unwrap();

        let tripped = run.nodes.iter().find(|n| n.node_id == "tripped").unwrap();
        assert_eq!(
            tripped.error_kind,
            Some(NodeErrorKind::CircuitOpen),
            "the node that raised the trip must carry the class that earns the cooldown"
        );

        let classified: Vec<&str> = run
            .nodes
            .iter()
            .filter(|n| n.error_kind == Some(NodeErrorKind::CircuitOpen))
            .map(|n| n.node_id.as_str())
            .collect();
        assert_eq!(
            classified,
            vec!["tripped"],
            "exactly one node may be classified as the breaker trip"
        );
        // A node that did not fail carries no kind at all — `None` is
        // "did not fail", never "failed, unclassified".
        let healthy = run.nodes.iter().find(|n| n.node_id == "healthy").unwrap();
        assert_eq!(healthy.error_kind, None);
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
        assert_eq!(
            bad.error_kind,
            Some(NodeErrorKind::Other),
            "an ordinary sink failure is classified, just not as a breaker trip"
        );
        let good = run.nodes.iter().find(|n| n.node_id == "good").unwrap();
        assert!(good.error.is_none(), "the healthy sink is not");
        assert_eq!(good.records, 4);
        // Every node appears, with its kind.
        assert_eq!(run.nodes.len(), 4);
        assert!(run.nodes.iter().any(|n| n.kind == "source"));
        assert!(run.nodes.iter().any(|n| n.kind == "tee"));
    }
}

#[cfg(test)]
mod graph_safety_tests {
    use super::tests::{CollectSink, FailingSink, VecSource, recs};
    use super::*;
    use crate::Stream;
    use crate::idempotency::{DeliveryMode, format_token_with_bookmark, wrap_state};
    use crate::state::MemoryStateStore;
    use async_trait::async_trait;
    use serde_json::json;
    use std::sync::Mutex;

    /// A source over `pos` 1..=`total`, `per_page` records per page, each page
    /// bookmarked `{"pos": <last record>}`. Resuming from `{"pos": p}` continues
    /// at `p + 1`. `ordered` makes it order positions like a CDC source.
    struct LogSource {
        total: u64,
        per_page: u64,
        ordered: bool,
        start: Mutex<u64>,
        applied: Arc<Mutex<Option<Value>>>,
    }
    impl LogSource {
        fn new(total: u64, per_page: u64, ordered: bool) -> (Self, Arc<Mutex<Option<Value>>>) {
            let applied = Arc::new(Mutex::new(None));
            (
                Self {
                    total,
                    per_page,
                    ordered,
                    start: Mutex::new(0),
                    applied: applied.clone(),
                },
                applied,
            )
        }
    }
    #[async_trait]
    impl Source for LogSource {
        async fn fetch_with_context(
            &self,
            _c: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(Vec::new())
        }
        fn stream_pages<'a>(
            &'a self,
            _ctx: &'a HashMap<String, Value>,
            _batch: usize,
        ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
            let mut next = *self.start.lock().unwrap() + 1;
            let (total, per_page) = (self.total, self.per_page);
            Box::pin(async_stream::try_stream! {
                while next <= total {
                    let last = (next + per_page - 1).min(total);
                    let records: Vec<Value> = (next..=last).map(|n| json!({ "n": n })).collect();
                    yield StreamPage { records, bookmark: Some(json!({ "pos": last })) };
                    next = last + 1;
                }
            })
        }
        async fn apply_start_bookmark(&self, bm: Value) -> Result<(), FaucetError> {
            *self.start.lock().unwrap() = bm["pos"].as_u64().unwrap_or(0);
            *self.applied.lock().unwrap() = Some(bm);
            Ok(())
        }
        fn replay_guarantee(&self) -> crate::idempotency::ReplayGuarantee {
            crate::idempotency::ReplayGuarantee::Deterministic
        }
        fn supports_exactly_once(&self) -> bool {
            true
        }
        fn position_le(&self, a: &Value, b: &Value) -> Option<bool> {
            if !self.ordered {
                return None;
            }
            Some(a["pos"].as_u64()? <= b["pos"].as_u64()?)
        }
    }

    /// An idempotent sink keeping its rows and one commit token per scope.
    #[derive(Clone)]
    struct TokenSink {
        rows: Arc<Mutex<Vec<Value>>>,
        tokens: Arc<Mutex<HashMap<String, String>>>,
    }
    impl TokenSink {
        fn new() -> Self {
            Self {
                rows: Arc::new(Mutex::new(Vec::new())),
                tokens: Arc::new(Mutex::new(HashMap::new())),
            }
        }
        fn committed(self, scope: &str, seq: u64, pos: u64) -> Self {
            self.tokens.lock().unwrap().insert(
                scope.into(),
                format_token_with_bookmark(seq, Some(&json!({ "pos": pos }))),
            );
            self
        }
        fn ns(&self) -> Vec<u64> {
            self.rows
                .lock()
                .unwrap()
                .iter()
                .filter_map(|r| r["n"].as_u64())
                .collect()
        }
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

    async fn seed(store: &MemoryStateStore, key: &str, pos: u64, seq: u64) {
        store
            .put(key, &wrap_state(Some(&json!({ "pos": pos })), seq))
            .await
            .unwrap();
    }

    async fn stored_seq(store: &MemoryStateStore, key: &str) -> u64 {
        let v = store.get(key).await.unwrap().unwrap();
        crate::idempotency::unwrap_state(&crate::state_version::peel_versioned(&v)).1
    }

    fn eo() -> TopologyGovernance {
        let mut g = TopologyGovernance::new();
        g.delivery = DeliveryMode::ExactlyOnce;
        g
    }

    fn two_sink_graph(source: LogSource, ahead: TokenSink, behind: TokenSink) -> Topology {
        Topology::builder()
            .source("s", Box::new(source))
            .tee("t", 4, Some(2))
            .sink("ahead", Box::new(ahead))
            .sink("behind", Box::new(behind))
            .edge("s", "t")
            .edge("t", "ahead")
            .edge("t", "behind")
            .build()
            .unwrap()
    }

    /// CORE-01: with diverged sinks and a source that cannot order positions,
    /// the replay starts at the laggard and every sink starts at the laggard's
    /// sequence, so the sink that is ahead skips what it committed instead of
    /// writing it twice — and both end on the same, comparable sequence.
    #[tokio::test]
    async fn eo_diverged_sinks_never_rewrite_committed_pages() {
        let store = Arc::new(MemoryStateStore::new());
        seed(&store, "p::ahead", 9, 9).await;
        seed(&store, "p::behind", 5, 5).await;
        let ahead = TokenSink::new().committed("p::ahead", 9, 9);
        let behind = TokenSink::new().committed("p::behind", 5, 5);
        let (source, applied) = LogSource::new(10, 1, false);
        let topo = two_sink_graph(source, ahead.clone(), behind.clone());

        topo.run_with(
            TopologyOptions::new("p").with_state_store(store.clone()),
            eo(),
        )
        .await
        .unwrap();

        assert_eq!(*applied.lock().unwrap(), Some(json!({ "pos": 5 })));
        assert_eq!(ahead.ns(), vec![10], "pages 6..=9 were already committed");
        assert_eq!(behind.ns(), vec![6, 7, 8, 9, 10]);
        assert_eq!(stored_seq(&store, "p::ahead").await, 10);
        assert_eq!(stored_seq(&store, "p::behind").await, 10);
    }

    /// CORE-01 with a source that orders its positions: replayed page boundaries
    /// may differ from the original ones, and the sink ahead still skips exactly
    /// what it committed, by position rather than by page count.
    #[tokio::test]
    async fn eo_diverged_sinks_skip_by_position_when_the_source_orders_them() {
        let store = Arc::new(MemoryStateStore::new());
        seed(&store, "p::ahead", 9, 9).await;
        seed(&store, "p::behind", 5, 5).await;
        let ahead = TokenSink::new().committed("p::ahead", 9, 9);
        let behind = TokenSink::new().committed("p::behind", 5, 5);
        let (source, applied) = LogSource::new(10, 2, true);
        let topo = two_sink_graph(source, ahead.clone(), behind.clone());

        topo.run_with(
            TopologyOptions::new("p").with_state_store(store.clone()),
            eo(),
        )
        .await
        .unwrap();

        assert_eq!(*applied.lock().unwrap(), Some(json!({ "pos": 5 })));
        assert_eq!(ahead.ns(), vec![10]);
        assert_eq!(behind.ns(), vec![6, 7, 8, 9, 10]);
        assert_eq!(stored_seq(&store, "p::ahead").await, 10);
    }

    /// CORE-11: the sink committed a page the state store never recorded. The
    /// source is re-anchored at the position embedded in the sink's token, so
    /// nothing is re-written and nothing is skipped however the replay is paged.
    #[tokio::test]
    async fn eo_resume_reanchors_from_the_sinks_committed_token() {
        let store = Arc::new(MemoryStateStore::new());
        seed(&store, "p::k", 4, 4).await;
        let sink = TokenSink::new().committed("p::k", 5, 7);
        let (source, applied) = LogSource::new(9, 1, false);
        let topo = Topology::builder()
            .source("s", Box::new(source))
            .sink("k", Box::new(sink.clone()))
            .edge("s", "k")
            .build()
            .unwrap();

        topo.run_with(
            TopologyOptions::new("p").with_state_store(store.clone()),
            eo(),
        )
        .await
        .unwrap();

        assert_eq!(*applied.lock().unwrap(), Some(json!({ "pos": 7 })));
        assert_eq!(sink.ns(), vec![8, 9]);
        assert_eq!(stored_seq(&store, "p::k").await, 7);
    }

    #[test]
    fn eo_plan_without_a_committed_position_replays_from_zero() {
        let (src, _) = LogSource::new(1, 1, false);
        let plan = plan_exactly_once(
            &[
                ("a".into(), Some(json!({ "pos": 3 })), 3),
                ("b".into(), None, 0),
            ],
            1,
            Some(&src),
        );
        assert_eq!(plan.start, None);
        assert!(plan.sinks.values().all(|r| r.start_seq == 0));

        let (ordered, _) = LogSource::new(1, 1, true);
        let plan = plan_exactly_once(
            &[
                ("a".into(), Some(json!({ "pos": 3 })), 3),
                ("b".into(), None, 0),
            ],
            1,
            Some(&ordered),
        );
        assert_eq!(plan.start, None);
        assert_eq!(plan.sinks["a"].skip_through, Some(json!({ "pos": 3 })));
        assert_eq!(plan.sinks["a"].start_seq, 3);
        assert_eq!(plan.sinks["b"].skip_through, None);
    }

    #[test]
    fn eo_plan_for_several_sources_keeps_each_sinks_sequence() {
        let plan = plan_exactly_once(
            &[
                ("a".into(), Some(json!({ "pos": 3 })), 3),
                ("b".into(), Some(json!({ "pos": 4 })), 4),
            ],
            2,
            None,
        );
        assert_eq!(plan.start, None);
        assert_eq!(plan.sinks["a"].start_seq, 3);
        assert_eq!(plan.sinks["b"].start_seq, 4);
    }

    #[test]
    fn eo_plan_agreeing_sinks_resume_without_skipping() {
        let plan = plan_exactly_once(
            &[
                ("a".into(), Some(json!({ "pos": 3 })), 3),
                ("b".into(), Some(json!({ "pos": 3 })), 6),
            ],
            1,
            None,
        );
        assert_eq!(plan.start, Some(json!({ "pos": 3 })));
        assert_eq!(plan.sinks["b"].start_seq, 6);
        assert!(plan.sinks.values().all(|r| r.skip_through.is_none()));
    }

    // ── CORE-13 ───────────────────────────────────────────────────────────────

    #[test]
    fn validate_rejects_a_join_fed_by_one_tee_on_both_sides() {
        let jn = JoinNode {
            config: JoinConfig {
                mode: JoinMode::Inner,
                build_key: "i".into(),
                probe_key: "i".into(),
                ..Default::default()
            },
            build_edge: "b".into(),
            probe_edge: "p".into(),
        };
        let (sink, _) = CollectSink::new();
        let err = Topology::builder()
            .source("s", VecSource::boxed(recs(1)))
            .tee("t", 4, Some(2))
            .join("j", jn)
            .sink("k", Box::new(sink))
            .edge("s", "t")
            .labelled_edge("t", "j", "b")
            .labelled_edge("t", "j", "p")
            .edge("j", "k")
            .build()
            .unwrap_err()
            .to_string();
        assert!(err.contains("join 'j'"), "{err}");
        assert!(err.contains("'s'") || err.contains("'t'"), "{err}");
    }

    #[tokio::test]
    async fn dropping_the_run_future_aborts_its_node_tasks() {
        struct Wedged {
            alive: Arc<()>,
            entered: Arc<tokio::sync::Notify>,
        }
        #[async_trait::async_trait]
        impl Sink for Wedged {
            async fn write_batch(&self, _: &[Value]) -> Result<usize, FaucetError> {
                let _hold = Arc::clone(&self.alive);
                self.entered.notify_one();
                std::future::pending().await
            }
        }
        let alive = Arc::new(());
        let entered = Arc::new(tokio::sync::Notify::new());
        let topology = Topology::builder()
            .source("s", VecSource::boxed(recs(2)))
            .sink(
                "k",
                Box::new(Wedged {
                    alive: Arc::clone(&alive),
                    entered: Arc::clone(&entered),
                }),
            )
            .edge("s", "k")
            .build()
            .unwrap();
        let run = tokio::spawn(topology.run(TopologyOptions::default()));
        entered.notified().await;
        run.abort();
        let _ = run.await;
        tokio::time::timeout(Duration::from_secs(5), async {
            while Arc::strong_count(&alive) > 1 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the wedged sink node must be aborted with the run");
    }

    #[tokio::test]
    async fn broadcast_gives_up_on_a_full_channel_when_cancelled() {
        let (tx, mut rx) = mpsc::channel::<StreamPage>(1);
        tx.send(StreamPage {
            records: Vec::new(),
            bookmark: None,
        })
        .await
        .unwrap();
        let cancel = CancellationToken::new();
        let trip = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            trip.cancel();
        });
        let mut outs = vec![tx.clone(), tx];
        let sent = tokio::time::timeout(
            Duration::from_secs(5),
            broadcast(
                StreamPage {
                    records: recs(1),
                    bookmark: None,
                },
                &mut outs,
                &cancel,
            ),
        )
        .await
        .expect("a cancelled broadcast must not wait on a full channel");
        assert!(!sent);
        assert!(rx.recv().await.is_some());
    }

    #[tokio::test]
    async fn broadcast_drops_closed_outputs() {
        let (live_tx, mut live_rx) = mpsc::channel::<StreamPage>(4);
        let (dead_tx, dead_rx) = mpsc::channel::<StreamPage>(4);
        drop(dead_rx);
        let mut outs = vec![dead_tx, live_tx];
        let cancel = CancellationToken::new();
        let page = StreamPage {
            records: recs(2),
            bookmark: None,
        };
        assert!(broadcast(page, &mut outs, &cancel).await);
        assert_eq!(outs.len(), 1);
        assert_eq!(live_rx.recv().await.unwrap().records.len(), 2);
        let mut none: Vec<mpsc::Sender<StreamPage>> = Vec::new();
        let page = StreamPage {
            records: Vec::new(),
            bookmark: None,
        };
        assert!(!broadcast(page, &mut none, &cancel).await);
    }

    #[tokio::test]
    async fn a_cancelled_receive_returns_none() {
        let (_tx, mut rx) = mpsc::channel::<StreamPage>(1);
        let cancel = CancellationToken::new();
        cancel.cancel();
        assert!(recv_or_cancel(&mut rx, &cancel).await.is_none());
    }

    // ── CORE-12 ───────────────────────────────────────────────────────────────

    #[test]
    fn merge_mode_follows_the_inputs_origins() {
        let set = |ids: &[&str]| ids.iter().map(|s| s.to_string()).collect::<HashSet<_>>();
        assert_eq!(
            merge_mode(&[set(&["a"]), set(&["b"])]),
            MergeMode::Passthrough
        );
        assert_eq!(merge_mode(&[set(&["a"]), set(&["a"])]), MergeMode::Gated);
        assert_eq!(
            merge_mode(&[set(&["a", "b"]), set(&["a"])]),
            MergeMode::Strip
        );
    }

    #[test]
    fn bookmark_gate_releases_a_position_only_once_every_input_delivered_it() {
        let mut gate = BookmarkGate::new(2);
        assert_eq!(gate.arrive(0, json!(1)), None);
        assert_eq!(gate.arrive(0, json!(2)), None);
        assert_eq!(gate.arrive(1, json!(1)), Some(json!(1)));
        assert_eq!(gate.arrive(1, json!(2)), Some(json!(2)));
        assert_eq!(gate.arrive(1, json!(3)), None);
        // The lagging input closes: everything the other one delivered is safe.
        assert_eq!(gate.close(0), Some(json!(3)));
        assert_eq!(gate.close(1), None);
    }

    #[test]
    fn graph_facts_gate_a_merge_that_rejoins_one_source() {
        let (k, _) = CollectSink::new();
        let topo = Topology::builder()
            .source("s", VecSource::boxed(recs(1)))
            .tee("t", 4, Some(2))
            .transform("x", Vec::new())
            .transform("y", Vec::new())
            .merge("m")
            .sink("k", Box::new(k))
            .edge("s", "t")
            .edge("t", "x")
            .edge("t", "y")
            .edge("x", "m")
            .edge("y", "m")
            .edge("m", "k")
            .build()
            .unwrap();
        let facts = GraphFacts::of(topo.nodes(), topo.edges());
        assert_eq!(facts.merge_modes["m"], MergeMode::Gated);
        assert!(facts.ancestors["k"].contains("s"));
        assert!(facts.ancestors["k"].contains("x"));
    }

    /// A state store that logs every put into the same log a sink writes to, so
    /// a test can check the order of "row written" and "position persisted".
    struct LoggingStore {
        inner: MemoryStateStore,
        log: Arc<Mutex<Vec<String>>>,
    }
    #[async_trait]
    impl StateStore for LoggingStore {
        async fn get(&self, key: &str) -> Result<Option<Value>, FaucetError> {
            self.inner.get(key).await
        }
        async fn put(&self, key: &str, value: &Value) -> Result<(), FaucetError> {
            let data = crate::state_version::peel_versioned(value);
            self.log
                .lock()
                .unwrap()
                .push(format!("put {}", data["pos"].as_u64().unwrap_or(0)));
            self.inner.put(key, value).await
        }
        async fn delete(&self, key: &str) -> Result<(), FaucetError> {
            self.inner.delete(key).await
        }
    }

    struct LoggingSink {
        log: Arc<Mutex<Vec<String>>>,
    }
    #[async_trait]
    impl Sink for LoggingSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            let mut log = self.log.lock().unwrap();
            for r in records {
                log.push(format!("row {}", r["n"].as_u64().unwrap_or(0)));
            }
            Ok(records.len())
        }
    }

    /// CORE-12: in a tee → … → merge diamond, a position is persisted only after
    /// both branches' copies of its page were written.
    #[tokio::test]
    async fn a_diamond_persists_a_position_only_after_both_copies_landed() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let store = Arc::new(LoggingStore {
            inner: MemoryStateStore::new(),
            log: log.clone(),
        });
        let (source, _) = LogSource::new(6, 1, false);
        let topo = Topology::builder()
            .source("s", Box::new(source))
            .tee("t", 1, Some(2))
            .transform("x", Vec::new())
            .transform("y", Vec::new())
            .merge("m")
            .sink("k", Box::new(LoggingSink { log: log.clone() }))
            .edge("s", "t")
            .edge("t", "x")
            .edge("t", "y")
            .edge("x", "m")
            .edge("y", "m")
            .edge("m", "k")
            .build()
            .unwrap();
        topo.run(TopologyOptions::new("p").with_state_store(store))
            .await
            .unwrap();

        let log = log.lock().unwrap().clone();
        for (i, entry) in log.iter().enumerate() {
            let Some(pos) = entry.strip_prefix("put ") else {
                continue;
            };
            let copies = log[..i]
                .iter()
                .filter(|e| **e == format!("row {pos}"))
                .count();
            assert_eq!(
                copies, 2,
                "position {pos} persisted before both copies: {log:?}"
            );
        }
        assert_eq!(log.iter().filter(|e| e.starts_with("row")).count(), 12);
        assert!(log.iter().any(|e| e == "put 6"), "{log:?}");
    }

    // ── CORE-14 ───────────────────────────────────────────────────────────────

    struct OverwriteSink {
        log: Arc<Mutex<Vec<String>>>,
        fail_commit: bool,
    }
    #[async_trait]
    impl Sink for OverwriteSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            self.log.lock().unwrap().push("write".into());
            Ok(records.len())
        }
        fn is_overwrite(&self) -> bool {
            true
        }
        async fn begin_overwrite(&self) -> Result<(), FaucetError> {
            self.log.lock().unwrap().push("begin".into());
            Ok(())
        }
        async fn commit_overwrite(&self) -> Result<(), FaucetError> {
            if self.fail_commit {
                return Err(FaucetError::Sink("swap failed".into()));
            }
            self.log.lock().unwrap().push("commit".into());
            Ok(())
        }
        async fn abort_overwrite(&self) -> Result<(), FaucetError> {
            self.log.lock().unwrap().push("abort".into());
            Ok(())
        }
        async fn complete_run(&self) -> Result<(), FaucetError> {
            self.log.lock().unwrap().push("complete".into());
            Ok(())
        }
    }

    #[tokio::test]
    async fn an_overwrite_sink_node_commits_then_persists_its_position() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let store = Arc::new(LoggingStore {
            inner: MemoryStateStore::new(),
            log: log.clone(),
        });
        let (source, _) = LogSource::new(3, 1, false);
        let topo = Topology::builder()
            .source("s", Box::new(source))
            .sink(
                "k",
                Box::new(OverwriteSink {
                    log: log.clone(),
                    fail_commit: false,
                }),
            )
            .edge("s", "k")
            .build()
            .unwrap();
        topo.run(TopologyOptions::new("p").with_state_store(store))
            .await
            .unwrap();
        let log = log.lock().unwrap().clone();
        assert_eq!(
            log,
            vec![
                "begin", "write", "write", "write", "commit", "put 3", "complete"
            ],
            "staged, swapped, then the position — never before the swap"
        );
    }

    #[tokio::test]
    async fn an_overwrite_sink_node_discards_staging_when_another_node_fails() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let topo = Topology::builder()
            .source("s", VecSource::boxed(recs(3)))
            .tee("t", 4, Some(2))
            .sink(
                "k",
                Box::new(OverwriteSink {
                    log: log.clone(),
                    fail_commit: false,
                }),
            )
            .sink("bad", Box::new(FailingSink))
            .edge("s", "t")
            .edge("t", "k")
            .edge("t", "bad")
            .build()
            .unwrap();
        let run = topo
            .run_reported(
                TopologyOptions::new("p").with_on_error(TopologyOnError::Continue),
                TopologyGovernance::new(),
            )
            .await
            .unwrap();
        assert!(!run.result.errors.is_empty());
        let log = log.lock().unwrap().clone();
        assert!(log.contains(&"abort".to_string()), "{log:?}");
        assert!(!log.contains(&"commit".to_string()), "{log:?}");
    }

    #[tokio::test]
    async fn an_overwrite_sink_node_discards_staging_on_cancel() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let cancel = CancellationToken::new();
        cancel.cancel();
        let topo = Topology::builder()
            .source("s", VecSource::boxed(recs(3)))
            .sink(
                "k",
                Box::new(OverwriteSink {
                    log: log.clone(),
                    fail_commit: false,
                }),
            )
            .edge("s", "k")
            .build()
            .unwrap();
        topo.run(TopologyOptions::new("p").with_cancel(cancel))
            .await
            .unwrap();
        let log = log.lock().unwrap().clone();
        assert_eq!(log.first().map(String::as_str), Some("begin"));
        assert!(log.contains(&"abort".to_string()), "{log:?}");
        assert!(!log.contains(&"commit".to_string()), "{log:?}");
    }

    #[tokio::test]
    async fn a_failed_swap_fails_the_run_and_discards_the_other_staging() {
        let first = Arc::new(Mutex::new(Vec::new()));
        let second = Arc::new(Mutex::new(Vec::new()));
        let topo = Topology::builder()
            .source("s", VecSource::boxed(recs(2)))
            .tee("t", 4, Some(2))
            .sink(
                "a",
                Box::new(OverwriteSink {
                    log: first.clone(),
                    fail_commit: true,
                }),
            )
            .sink(
                "b",
                Box::new(OverwriteSink {
                    log: second.clone(),
                    fail_commit: false,
                }),
            )
            .edge("s", "t")
            .edge("t", "a")
            .edge("t", "b")
            .build()
            .unwrap();
        let err = topo
            .run(TopologyOptions::new("p"))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("swap failed"), "{err}");
        let second = second.lock().unwrap().clone();
        assert!(second.contains(&"abort".to_string()), "{second:?}");
        assert!(!second.contains(&"commit".to_string()), "{second:?}");
    }

    // ── CLI-18 (core side) ────────────────────────────────────────────────────

    struct BrokenSource;
    #[async_trait]
    impl Source for BrokenSource {
        async fn fetch_with_context(
            &self,
            _c: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Err(FaucetError::Source("source down".into()))
        }
    }

    #[tokio::test]
    async fn a_sink_fed_by_a_failed_source_is_reported_failed() {
        let (sink, _) = CollectSink::new();
        let topo = Topology::builder()
            .source("src", Box::new(BrokenSource))
            .transform("x", Vec::new())
            .sink("k", Box::new(sink))
            .edge("src", "x")
            .edge("x", "k")
            .build()
            .unwrap();
        let run = topo
            .run_reported(
                TopologyOptions::new("p").with_on_error(TopologyOnError::Continue),
                TopologyGovernance::new(),
            )
            .await
            .unwrap();
        let k = run.nodes.iter().find(|n| n.node_id == "k").unwrap();
        let err = k.error.as_deref().unwrap_or_default();
        assert!(err.contains("upstream node 'src' failed"), "{err}");
        assert_eq!(k.error_kind, Some(NodeErrorKind::Other));
    }

    #[tokio::test]
    async fn run_attributed_keeps_the_per_node_report_on_failure() {
        let (sink, _) = CollectSink::new();
        let topo = Topology::builder()
            .source("src", Box::new(BrokenSource))
            .sink("k", Box::new(sink))
            .edge("src", "k")
            .build()
            .unwrap();
        let (run, err) = topo
            .run_attributed(TopologyOptions::new("p"), TopologyGovernance::new())
            .await;
        assert!(err.is_some(), "Propagate surfaces the failure");
        let k = run.nodes.iter().find(|n| n.node_id == "k").unwrap();
        assert!(k.error.is_some(), "the sink is reported, not dropped");
        let src = run.nodes.iter().find(|n| n.node_id == "src").unwrap();
        assert!(
            src.error
                .as_deref()
                .unwrap_or_default()
                .contains("source down")
        );
    }

    #[tokio::test]
    async fn run_attributed_reports_an_invalid_graph() {
        let (run, err) = Topology {
            nodes: Vec::new(),
            edges: Vec::new(),
        }
        .run_attributed(TopologyOptions::new("p"), TopologyGovernance::new())
        .await;
        assert!(err.is_some());
        assert!(run.nodes.is_empty());
    }
}
