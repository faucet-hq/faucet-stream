//! `faucet verify` — prove a destination matches its source **by content**
//! (#701), and repair only what differs.
//!
//! The comparison is keyed. With a single integer key on two range-readable
//! SQL sources the verifier plans key ranges, compares a digest per range
//! (computed **inside** each backend when both report the same algorithm, so
//! a matching range ships no rows; streamed and hashed client-side otherwise),
//! bisects the ranges that disagree down to `leaf_rows`, and only then fetches
//! rows and diffs them per key. Any other key, or a source that cannot read a
//! key range, compares the whole dataset row by row.
//!
//! What is compared is what the pipeline would have written: the source side
//! runs through the row's transforms and masking first, so a deterministic
//! mask (`hash`, `tokenize`, `redact`) matches on both sides rather than being
//! reported as a difference.
//!
//! `--repair` re-syncs the differing keys through the row's own sink with
//! `write_mode: upsert` (deletes only with `--allow-delete`), so quality and
//! contract checks — and the DLQ — still apply to the repaired rows.

pub mod metrics;
pub mod spec;

pub use spec::VerifySpec;

use crate::auth_catalog::AuthCatalog;
use crate::config::{ConnectorSpec, ExecutionSpec, PipelineConfig};
use crate::dlq_replay::reader::SourceOverride;
use crate::error::{CliError, CliResult};
use crate::executor::ExecuteOptions;
use crate::expand::{ExpandedNode, NodeRole, expand};
use crate::registry::{build_sink, build_source};
use async_trait::async_trait;
use chrono::{DateTime, FixedOffset};
use faucet_core::diff::{
    Difference, DigestAccumulator, KeyRange, Normalizer, ServerDigest, VerifyReport, diff_rows,
    key_text, row_hash,
};
use faucet_core::observability::Labels;
use faucet_core::shard::PkShardBounds;
use faucet_core::{DeliveryMode, FaucetError, Source};
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;

/// Whether a source configured this way reads one integer-key range at a
/// time through `apply_shard` — its declared `range_read` capability.
pub fn source_reads_ranges(kind: &str, config: &Value) -> bool {
    crate::registry::source_capabilities(kind, config).range_read()
}

/// Per-invocation inputs for [`verify`].
pub struct VerifyInputs {
    /// Which root row to verify (`None` = the first root).
    pub row: Option<String>,
    /// Re-sync differing keys through the sink (overrides `verify.repair`).
    pub repair: bool,
    /// Let the repair delete destination-only rows (overrides
    /// `verify.allow_delete`).
    pub allow_delete: bool,
    /// Plan the repair without writing.
    pub dry_run: bool,
    pub pipeline_name: String,
    pub execution: Option<ExecutionSpec>,
    pub auth: AuthCatalog,
    pub clock: DateTime<FixedOffset>,
}

/// The guarantees a repair runs under (#871): the budget, cancellation token
/// and resilience policy of whatever started it. `faucet verify` takes them
/// from the config; a post-run verify takes the run's (its remaining budget,
/// its token).
#[derive(Clone, Default)]
pub struct RunGuards {
    pub budget: Option<faucet_core::BudgetSpec>,
    pub cancel: Option<tokio_util::sync::CancellationToken>,
    pub resilience: Option<faucet_core::ResiliencePolicy>,
}

impl RunGuards {
    /// The config's own budget and resilience policy.
    pub fn from_config(cfg: &PipelineConfig) -> CliResult<Self> {
        Ok(Self {
            budget: crate::budget::effective_budget_with_kinds(
                cfg.budget.as_ref(),
                None,
                &crate::budget::sink_template_kinds(cfg),
            )
            .map_err(CliError::Config)?,
            cancel: None,
            resilience: match &cfg.resilience {
                Some(spec) => Some(spec.to_policy()?),
                None => None,
            },
        })
    }
}

/// The result of one `faucet verify`: the row it compared, the two datasets,
/// and the [`VerifyReport`].
#[derive(Debug, Clone, Serialize)]
pub struct VerifyOutcome {
    pub row: String,
    pub source: String,
    pub destination: String,
    pub key: Vec<String>,
    /// `range` (digest + bisection) or `full` (one row-by-row pass).
    pub strategy: &'static str,
    #[serde(flatten)]
    pub report: VerifyReport,
    pub dry_run: bool,
}

impl VerifyOutcome {
    /// Differing keys (the exit code of `faucet verify`).
    pub fn differing_keys(&self) -> usize {
        self.report.differences.len()
    }
}

/// Verify one root row of `cfg`.
pub async fn verify(
    cfg: &PipelineConfig,
    spec: &VerifySpec,
    inputs: VerifyInputs,
) -> CliResult<VerifyOutcome> {
    spec.validate()?;
    let nodes = expand(&crate::partition::resolve_runtime(cfg).await?)?;
    let all = nodes.clone();
    let node = crate::dlq_replay::plan::select_replay_node(nodes, inputs.row.as_deref())?;
    crate::destination::check_owns_destination(&node, &all)?;
    crate::destination::check_owns_destination_resolved(&node, &all, &inputs.auth).await?;
    let guards = RunGuards::from_config(cfg)?;
    verify_node_guarded(&node, spec, &inputs, &guards).await
}

/// [`verify_node`] behind a heap allocation, built in its own never-inlined
/// frame — what the executor awaits from inside `run_one_invocation`, so the
/// verifier's (large) state machine never lands in that future's frame.
#[inline(never)]
pub fn verify_node_boxed<'a>(
    node: &'a ExpandedNode,
    spec: &'a VerifySpec,
    inputs: &'a VerifyInputs,
    guards: &'a RunGuards,
) -> futures::future::BoxFuture<'a, CliResult<VerifyOutcome>> {
    Box::pin(verify_node_guarded(node, spec, inputs, guards))
}

/// Verify an already-expanded root node. Shared by the command and the
/// executor's post-run pass.
pub async fn verify_node(
    node: &ExpandedNode,
    spec: &VerifySpec,
    inputs: &VerifyInputs,
) -> CliResult<VerifyOutcome> {
    verify_node_guarded(node, spec, inputs, &RunGuards::default()).await
}

/// [`verify_node`] whose repair runs under `guards` (#871).
pub async fn verify_node_guarded(
    node: &ExpandedNode,
    spec: &VerifySpec,
    inputs: &VerifyInputs,
    guards: &RunGuards,
) -> CliResult<VerifyOutcome> {
    if !matches!(node.role, NodeRole::Root) {
        return Err(CliError::Config(format!(
            "verify: row '{}' is a child node; only root pipelines can be verified",
            node.id
        )));
    }
    let started = std::time::Instant::now();
    let mut source_cfg = node.source.config.clone();
    let mut sink_cfg = node.sink.config.clone();
    crate::executor::resolve_now_inplace(&mut source_cfg, inputs.clock)?;
    crate::executor::resolve_now_inplace(&mut sink_cfg, inputs.clock)?;

    // The key: `verify.key`, else the sink's upsert key.
    let key: Vec<String> = if !spec.key.is_empty() {
        spec.key.clone()
    } else {
        sink_cfg
            .get("key")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default()
    };
    if key.is_empty() {
        return Err(CliError::Config(format!(
            "verify: row '{}' has no key to match rows on — set `verify.key` (or give the sink a \
             `key` with `write_mode: upsert`). A keyless table cannot be verified: there is no \
             stable identity to bisect on.",
            node.id
        )));
    }

    // Destination read-back: an explicit `verify.destination`, else what the
    // sink itself describes.
    let sink = build_sink(&node.sink.kind, sink_cfg.clone(), &inputs.auth).await?;
    let sink_uri = sink.dataset_uri();
    let (dest_kind, dest_cfg) = match &spec.destination {
        Some(d) => {
            let mut c = d.config.clone();
            crate::executor::resolve_now_inplace(&mut c, inputs.clock)?;
            (d.kind.clone(), c)
        }
        None => sink.readback_source().ok_or_else(|| {
            CliError::Config(format!(
                "verify: sink '{}' cannot describe how to read its destination back — set \
                 `verify.destination: {{ type: <source>, config: {{ … }} }}` to a source that \
                 returns the destination rows",
                node.sink.kind
            ))
        })?,
    };

    // Range mode needs one integer key and two range-readable sources. The
    // key's type is only known to the backend: planning the ranges casts its
    // bounds to an integer, so a text / uuid key fails there and the row is
    // compared in full instead.
    let mut range_mode = key.len() == 1
        && source_reads_ranges(&node.source.kind, &source_cfg)
        && source_reads_ranges(&dest_kind, &dest_cfg);
    if range_mode
        && let Ok(Some(schema)) = sink.current_schema().await
        && !key_is_integer(&schema, &key[0])
    {
        tracing::info!(row = %node.id, key = %key[0], "verify: the key is not an integer column; comparing the whole dataset");
        range_mode = false;
    }
    let mut source_cfg = source_cfg;
    let mut dest_cfg = dest_cfg;
    let mut planned = Vec::new();
    if range_mode {
        let mut sharded = source_cfg.clone();
        inject_shard_key(&mut sharded, &key[0]);
        let probe = build_source(&node.source.kind, sharded.clone(), &inputs.auth, None).await?;
        match probe.enumerate_shards(spec.ranges).await {
            Ok(shards) => {
                planned = shards;
                source_cfg = sharded;
                inject_shard_key(&mut dest_cfg, &key[0]);
            }
            Err(e) => {
                tracing::warn!(
                    row = %node.id,
                    key = %key[0],
                    error = %e,
                    "verify: key ranges cannot be planned (not an integer key?); comparing the \
                     whole dataset instead"
                );
                range_mode = false;
            }
        }
    }

    // Server-side digests need an explicit column list on both sides. Take
    // `verify.columns`, else the destination's live columns (minus the key and
    // the excluded ones); a schemaless destination digests client-side.
    let server_columns: Option<Vec<String>> = match &spec.columns {
        Some(cols) => Some(cols.clone()),
        None if range_mode => match sink.current_schema().await {
            Ok(Some(schema)) => Some(schema_columns(&schema, &key, &spec.exclude)),
            Ok(None) => None,
            Err(e) => {
                tracing::debug!(error = %e, "destination schema unavailable; digesting client-side");
                None
            }
        },
        None => None,
    };

    let source = build_source(&node.source.kind, source_cfg, &inputs.auth, None).await?;
    let source_uri = source.dataset_uri();
    let source = shape_source(source, node, inputs)?;
    let dest = build_source(&dest_kind, dest_cfg, &inputs.auth, None).await?;
    #[cfg(feature = "masking")]
    let masking = match &node.masking {
        Some(m) => Some(
            faucet_core::CompiledMasking::compile_for_sink(m, &[&node.sink_ref, &node.sink.kind])
                .map_err(|e| CliError::Config(format!("masking: {e}")))?,
        ),
        None => None,
    };
    #[cfg(not(feature = "masking"))]
    let masking: Option<()> = None;
    // A backend digests the raw source rows, but the destination holds them
    // masked, so a masked row's server digests could never agree: every range
    // would bisect to its leaves. Digest client-side, over masked rows.
    #[cfg(feature = "masking")]
    let server_columns = match &masking {
        Some(m) if !m.is_empty() => {
            tracing::info!(row = %node.id, "verify: masking rewrites this row; digesting client-side");
            None
        }
        _ => server_columns,
    };

    let mut session = Session {
        source: source.as_ref(),
        dest: dest.as_ref(),
        key: &key,
        columns: spec.columns.as_deref(),
        exclude: &spec.exclude,
        norm: &spec.normalize,
        report: VerifyReport::default(),
        source_rows: HashMap::new(),
        dest_keys: HashMap::new(),
        max_rows: spec.max_rows_scanned,
        server_columns,
        #[cfg(feature = "masking")]
        masking: masking.as_ref(),
        #[cfg(not(feature = "masking"))]
        masking: masking.as_ref(),
    };

    let strategy = if range_mode {
        session.verify_ranges(spec, planned).await?;
        "range"
    } else {
        session.verify_full().await?;
        "full"
    };
    let total = session.report.differences.len();
    let mut report = session.report;
    let source_rows = session.source_rows;

    // The repair covers every difference found; `max_differences` caps only
    // what is reported, below.
    let repair = inputs.repair || spec.repair;
    if repair && !report.differences.is_empty() {
        let (ups, dels) = repair_node(
            node,
            &key,
            &report.differences,
            &source_rows,
            spec,
            inputs,
            guards,
        )
        .await?;
        report.repaired_upserts = Some(ups);
        report.repaired_deletes = Some(dels);
    }
    if total > spec.max_differences {
        report.differences.truncate(spec.max_differences);
        report.truncated = true;
    }

    metrics::record(
        &inputs.pipeline_name,
        &node.id,
        &report,
        started.elapsed().as_secs_f64(),
    );
    tracing::info!(
        row = %node.id,
        strategy,
        ranges = report.ranges_compared,
        differing_ranges = report.ranges_differing,
        differences = total,
        "verification complete"
    );
    Ok(VerifyOutcome {
        row: node.id.clone(),
        source: source_uri,
        destination: sink_uri,
        key,
        strategy,
        report,
        dry_run: inputs.dry_run,
    })
}

/// Whether the destination schema types `key` as an integer (a column it does
/// not list is given the benefit of the doubt; range planning then decides).
fn key_is_integer(schema: &Value, key: &str) -> bool {
    let Some(prop) = schema.get("properties").and_then(|p| p.get(key)) else {
        return true;
    };
    match prop.get("type") {
        Some(Value::String(t)) => t == "integer",
        Some(Value::Array(ts)) => ts
            .iter()
            .filter_map(Value::as_str)
            .filter(|t| *t != "null")
            .all(|t| t == "integer"),
        _ => true,
    }
}

/// Put `shard: { key }` on a range-readable source config (unless one is
/// already set) so `enumerate_shards` can plan the key ranges.
fn inject_shard_key(cfg: &mut Value, key: &str) {
    if let Value::Object(m) = cfg
        && !m.contains_key("shard")
    {
        m.insert("shard".into(), serde_json::json!({ "key": key }));
    }
}

/// Wrap the raw source in the row's transform chain, so the compared rows are
/// what the pipeline writes.
fn shape_source(
    source: Box<dyn Source>,
    node: &ExpandedNode,
    inputs: &VerifyInputs,
) -> CliResult<Box<dyn Source>> {
    let stages = crate::transforms::compile_transforms(&node.transforms)?;
    if stages.is_empty() {
        return Ok(source);
    }
    let labels = Labels::new(
        inputs.pipeline_name.clone(),
        node.id.clone(),
        format!("verify-{}", uuid::Uuid::now_v7()),
    );
    Ok(Box::new(faucet_core::TransformingSource::new(
        source, stages, labels,
    )?))
}

/// One verification pass over a source/destination pair.
struct Session<'a> {
    source: &'a dyn Source,
    dest: &'a dyn Source,
    key: &'a [String],
    columns: Option<&'a [String]>,
    exclude: &'a [String],
    norm: &'a Normalizer,
    report: VerifyReport,
    /// Source rows by key text, kept for the repair (only for keys that
    /// differed — the leaf ranges).
    source_rows: HashMap<String, Value>,
    /// Destination key objects by key text (for deletes).
    dest_keys: HashMap<String, Value>,
    max_rows: Option<u64>,
    /// The column list server digests hash (`None` = digest client-side).
    server_columns: Option<Vec<String>>,
    #[cfg(feature = "masking")]
    masking: Option<&'a faucet_core::CompiledMasking>,
    #[cfg(not(feature = "masking"))]
    masking: Option<&'a ()>,
}

impl Session<'_> {
    fn over_budget(&self) -> bool {
        self.max_rows.is_some_and(|cap| {
            self.report.rows_fetched_source + self.report.rows_fetched_dest >= cap
        })
    }

    /// Rows either side may still fetch under `max_rows_scanned`.
    fn budget_left(&self) -> Option<u64> {
        self.max_rows.map(|cap| {
            cap.saturating_sub(self.report.rows_fetched_source + self.report.rows_fetched_dest)
        })
    }

    /// Fetch the source side of `range` (whole dataset for `ALL`), shaped as
    /// the pipeline would write it. `None` = the scan budget ran out reading
    /// it (the report is marked truncated).
    async fn fetch_source(&mut self, range: &KeyRange) -> CliResult<Option<Vec<Value>>> {
        let left = self.budget_left();
        let (rows, complete) = fetch_range(self.source, range, self.key, "source", left).await?;
        self.report.rows_fetched_source += rows.len() as u64;
        if !complete {
            self.report.truncated = true;
            return Ok(None);
        }
        #[cfg(feature = "masking")]
        let rows = match self.masking {
            Some(m) => faucet_core::apply_masking(rows, m).records,
            None => rows,
        };
        Ok(Some(rows))
    }

    async fn fetch_dest(&mut self, range: &KeyRange) -> CliResult<Option<Vec<Value>>> {
        let left = self.budget_left();
        let (rows, complete) = fetch_range(self.dest, range, self.key, "destination", left).await?;
        self.report.rows_fetched_dest += rows.len() as u64;
        if !complete {
            self.report.truncated = true;
            return Ok(None);
        }
        Ok(Some(rows))
    }

    /// Whole-dataset comparison: one fetch of each side, one keyed diff.
    async fn verify_full(&mut self) -> CliResult<()> {
        self.report.ranges_compared = 1;
        self.diff_leaf(&KeyRange::ALL).await
    }

    /// Fetch both sides of a leaf range and record the keyed differences. A
    /// side cut short by `max_rows_scanned` is not diffed: a partial set would
    /// report the unread rows as missing.
    async fn diff_leaf(&mut self, range: &KeyRange) -> CliResult<()> {
        let Some(src) = self.fetch_source(range).await? else {
            return Ok(());
        };
        let Some(dst) = self.fetch_dest(range).await? else {
            return Ok(());
        };
        let diffs = diff_rows(&src, &dst, self.key, self.columns, self.exclude, self.norm);
        if diffs.is_empty() {
            return Ok(());
        }
        self.report.ranges_differing += 1;
        // Keep exactly the rows a repair needs: source rows for keys that
        // must be upserted, destination keys for keys that must be deleted.
        let differing: std::collections::HashSet<String> = diffs
            .iter()
            .map(|d| key_text(&d.key, self.key, self.norm))
            .collect();
        for r in src {
            let kt = key_text(&r, self.key, self.norm);
            if differing.contains(&kt) {
                self.source_rows.insert(kt, r);
            }
        }
        for r in dst {
            let kt = key_text(&r, self.key, self.norm);
            if differing.contains(&kt) {
                self.dest_keys.insert(kt, key_object(&r, self.key));
            }
        }
        self.report.differences.extend(diffs);
        Ok(())
    }

    /// Range mode: plan → digest → bisect → leaf diff.
    async fn verify_ranges(
        &mut self,
        spec: &VerifySpec,
        shards: Vec<faucet_core::ShardSpec>,
    ) -> CliResult<()> {
        let key = &self.key[0];
        let mut queue: Vec<KeyRange> = shards.iter().map(range_of_shard).collect();
        if queue.is_empty() {
            queue.push(KeyRange::ALL);
        }
        // Decide once whether both backends digest with the same algorithm.
        // The whole-dataset digests already answer the question when they
        // agree; when they don't, the planned ranges narrow it down.
        let columns: Vec<String> = self.server_columns.clone().unwrap_or_default();
        let server = if self.server_columns.is_some() {
            match (
                self.source
                    .range_digest(&KeyRange::ALL, key, &columns)
                    .await?,
                self.dest
                    .range_digest(&KeyRange::ALL, key, &columns)
                    .await?,
            ) {
                (Some(a), Some(b)) if a.comparable(&b) => {
                    self.report.ranges_compared += 1;
                    if a.same(&b) {
                        self.report.server_digests = true;
                        return Ok(());
                    }
                    self.report.ranges_differing += 1;
                    true
                }
                _ => false,
            }
        } else {
            false
        };
        self.report.server_digests = server;
        while let Some(range) = queue.pop() {
            if self.over_budget() {
                self.report.truncated = true;
                break;
            }
            self.report.ranges_compared += 1;
            let (same, rows, bounds) = if server {
                let a = self
                    .source
                    .range_digest(&range, key, &columns)
                    .await?
                    .ok_or_else(|| FaucetError::Source("range digest vanished".into()))?;
                let b = self
                    .dest
                    .range_digest(&range, key, &columns)
                    .await?
                    .ok_or_else(|| FaucetError::Source("range digest vanished".into()))?;
                (a.same(&b), a.rows.max(b.rows), a.bounds_union(&b))
            } else {
                let (Some(a), Some(b)) = (
                    self.client_digest(&range, true).await?,
                    self.client_digest(&range, false).await?,
                ) else {
                    break;
                };
                (a.same(&b), a.rows.max(b.rows), a.bounds_union(&b))
            };
            if same {
                continue;
            }
            if rows > spec.leaf_rows
                && let Some((lo, hi)) = range.bisect(bounds)
            {
                queue.push(hi);
                queue.push(lo);
                continue;
            }
            self.diff_leaf(&range).await?;
        }
        Ok(())
    }

    /// Stream one side of a range and fold it into a client-side digest.
    async fn client_digest(
        &mut self,
        range: &KeyRange,
        is_source: bool,
    ) -> CliResult<Option<faucet_core::diff::ContentDigest>> {
        let rows = if is_source {
            self.fetch_source(range).await?
        } else {
            self.fetch_dest(range).await?
        };
        let Some(rows) = rows else {
            return Ok(None);
        };
        let mut acc = DigestAccumulator::new();
        for r in &rows {
            acc.add(
                row_hash(r, self.key, self.columns, self.exclude, self.norm),
                faucet_core::diff::key_int(r, self.key),
            );
        }
        Ok(Some(acc.finish()))
    }
}

/// Read one key range from a source. `ALL` clears any applied range. With a
/// `limit` the read is streamed and stops past it; the flag says whether the
/// range was read completely.
async fn fetch_range(
    src: &dyn Source,
    range: &KeyRange,
    key: &[String],
    side: &str,
    limit: Option<u64>,
) -> CliResult<(Vec<Value>, bool)> {
    use futures::StreamExt as _;
    if key.len() == 1 && (range.lo.is_some() || range.hi.is_some()) {
        src.apply_shard(&range.to_shard(&key[0]))
            .await
            .map_err(|e| {
                CliError::Config(format!("verify: narrowing the {side} to {range}: {e}"))
            })?;
    }
    let read_err = |e: FaucetError| {
        CliError::Faucet(FaucetError::Source(format!(
            "verify: reading the {side}: {e}"
        )))
    };
    let Some(limit) = limit else {
        return Ok((src.fetch_all().await.map_err(read_err)?, true));
    };
    let context = HashMap::new();
    let batch = usize::try_from(limit.clamp(1, 10_000)).unwrap_or(10_000);
    let mut pages = src.stream_pages(&context, batch);
    let mut rows = Vec::new();
    while let Some(page) = pages.next().await {
        rows.extend(page.map_err(read_err)?.records);
        if rows.len() as u64 > limit {
            rows.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
            return Ok((rows, false));
        }
    }
    Ok((rows, true))
}

/// The key range a PK shard descriptor covers (the whole space for a
/// non-PK or whole-dataset shard).
fn range_of_shard(spec: &faucet_core::ShardSpec) -> KeyRange {
    match PkShardBounds::from_spec(spec) {
        Some(b) if !spec.is_whole() => KeyRange {
            lo: (!b.lo_unbounded).then_some(b.lo),
            hi: (!b.hi_unbounded).then_some(b.hi),
        },
        _ => KeyRange::ALL,
    }
}

/// Column names of an `infer_schema`-shaped `{"type":"object","properties":{…}}`
/// schema, minus the key and the excluded ones, sorted — the list both sides
/// digest server-side.
pub fn schema_columns(schema: &Value, key: &[String], exclude: &[String]) -> Vec<String> {
    let mut cols: Vec<String> = schema
        .get("properties")
        .and_then(Value::as_object)
        .map(|p| p.keys().cloned().collect())
        .unwrap_or_default();
    cols.retain(|c| !key.contains(c) && !faucet_core::diff::is_excluded(c, exclude));
    cols.sort();
    cols
}

/// `{ k1: v1, … }` — a record's key columns only.
fn key_object(record: &Value, key: &[String]) -> Value {
    let mut m = serde_json::Map::new();
    for k in key {
        m.insert(k.clone(), record.get(k).cloned().unwrap_or(Value::Null));
    }
    Value::Object(m)
}

/// An in-memory source over a fixed record set — what the repair feeds
/// through the row's sink.
struct VecSource {
    records: std::sync::Mutex<Option<Vec<Value>>>,
}

impl VecSource {
    fn new(records: Vec<Value>) -> Self {
        Self {
            records: std::sync::Mutex::new(Some(records)),
        }
    }
}

#[async_trait]
impl Source for VecSource {
    async fn fetch_with_context(
        &self,
        _context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Ok(self
            .records
            .lock()
            .map_err(|_| FaucetError::Source("verify repair source poisoned".into()))?
            .take()
            .unwrap_or_default())
    }

    fn connector_name(&self) -> &'static str {
        "verify-repair"
    }

    fn config_schema(&self) -> Value {
        serde_json::json!({ "type": "object" })
    }
}

/// Re-sync the differing keys through the row's sink: upsert the source rows
/// for missing/changed keys; delete destination-only keys when allowed.
/// Returns `(upserts, deletes)` written.
async fn repair_node(
    node: &ExpandedNode,
    key: &[String],
    differences: &[Difference],
    source_rows: &HashMap<String, Value>,
    spec: &VerifySpec,
    inputs: &VerifyInputs,
    guards: &RunGuards,
) -> CliResult<(u64, u64)> {
    let allow_delete = inputs.allow_delete || spec.allow_delete;
    let norm = &spec.normalize;
    if !crate::registry::sink_capabilities(&node.sink.kind, &node.sink.config)
        .supports_write_mode(faucet_core::WriteMode::Upsert)
    {
        return Err(CliError::Config(format!(
            "verify --repair: sink '{}' does not support keyed writes (write_mode: upsert), so \
             differing keys cannot be re-synced through it",
            node.sink.kind
        )));
    }
    let mut upserts: Vec<Value> = Vec::new();
    let mut deletes: Vec<Value> = Vec::new();
    for d in differences {
        // The same normalizer the session keyed `source_rows` with.
        let kt = key_text(&d.key, key, norm);
        if d.needs_upsert() {
            let row = source_rows.get(&kt).ok_or_else(|| {
                CliError::Config(format!(
                    "verify --repair: no source row was kept for differing key {kt}; nothing \
                     was written"
                ))
            })?;
            upserts.push(row.clone());
        } else if d.needs_delete() && allow_delete {
            deletes.push(d.key.clone());
        }
    }
    let mut written = 0u64;
    let mut deleted = 0u64;
    if !upserts.is_empty() {
        written = run_repair(node, key, "upsert", upserts, inputs, guards).await?;
    }
    if !deletes.is_empty() {
        deleted = run_repair(node, key, "delete", deletes, inputs, guards).await?;
    }
    Ok((written, deleted))
}

/// The node a repair pass runs: `node` reading `records`, its sink forced to
/// keyed `write_mode`, masking and profiling opted out by name (#871, #875).
fn repair_target(
    node: &ExpandedNode,
    key: &[String],
    write_mode: &str,
    records: Vec<Value>,
) -> ExpandedNode {
    let mut repair = node.clone();
    repair.source = ConnectorSpec {
        kind: "verify-repair".into(),
        ..node.source.clone()
    };
    repair.source_override = Some(SourceOverride::new(Box::new(VecSource::new(records))));
    repair.transforms.clear();
    repair.opt_outs.push(faucet_core::OptOut {
        pass: faucet_core::Pass::Masking,
        reason: "verify masked the rows it read from the source, and masking is not idempotent"
            .into(),
    });
    repair.opt_outs.push(faucet_core::OptOut {
        pass: faucet_core::Pass::Profiling,
        reason: "repair writes correct earlier rows; they must not shift the profile baseline"
            .into(),
    });
    repair.state = None;
    repair.delivery = DeliveryMode::AtLeastOnce;
    repair.cleanup_scope = None;
    if let Value::Object(m) = &mut repair.sink.config {
        m.insert("write_mode".into(), Value::String(write_mode.into()));
        m.insert(
            "key".into(),
            Value::Array(key.iter().map(|k| Value::String(k.clone())).collect()),
        );
        m.remove("delete_marker");
    }
    repair
}

/// Run one repair pass: a copy of the row whose source is the given records
/// and whose sink is forced to `write_mode`. Transforms are cleared and
/// masking and profiling opted out (the rows are already shaped and masked,
/// and corrections must not shift the profile baseline), state is dropped (no
/// bookmark to advance), delivery is at-least-once (keyed writes are
/// idempotent). The budget, cancellation token and resilience policy come from
/// `guards` (#871).
async fn run_repair(
    node: &ExpandedNode,
    key: &[String],
    write_mode: &str,
    records: Vec<Value>,
    inputs: &VerifyInputs,
    guards: &RunGuards,
) -> CliResult<u64> {
    let repair = repair_target(node, key, write_mode, records);
    // The repair runs the executor from inside the executor's own post-run
    // pass; `run_boxed` breaks the recursive future type.
    let summary = crate::runtime::run_boxed(
        crate::runtime::Principal::local(),
        vec![repair],
        ExecuteOptions {
            legacy_state_writes: false,
            force_lease: false,
            pipeline_name: format!("{}-verify-repair", inputs.pipeline_name),
            run_id: None,
            execution: inputs.execution.clone(),
            concurrency: None,
            dry_run: inputs.dry_run,
            limit: None,
            state_path_override: None,
            state_scope: Default::default(),
            shard: None,
            auth: inputs.auth.clone(),
            clock: inputs.clock,
            cancel: guards.cancel.clone(),
            resilience: guards.resilience.clone(),
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
            usage: Default::default(),
            budget: guards.budget.clone(),
        },
    )
    .await?;
    if let Some(err) = summary.invocations.iter().find_map(|i| i.error.clone()) {
        return Err(CliError::Config(format!(
            "verify --repair ({write_mode}) failed: {err}"
        )));
    }
    Ok(summary
        .invocations
        .iter()
        .map(|i| i.records_written as u64)
        .sum())
}

/// The `ServerDigest` two sides must share to be compared server-side —
/// exposed for tests and the doctor probe.
pub fn digests_comparable(a: &ServerDigest, b: &ServerDigest) -> bool {
    a.comparable(b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// #871: a repair runs under its caller's budget and never feeds the
    /// profile baseline (profiling and masking are opted out by name).
    #[cfg(all(feature = "source-sqlite", feature = "sink-sqlite"))]
    #[tokio::test]
    async fn a_repair_runs_under_the_callers_budget_and_never_profiles() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("mirror.db");
        std::fs::File::create(&db).unwrap();
        let yaml = format!(
            "version: 1\nname: m\npipeline:\n  source: {{ type: sqlite, config: {{ database_url: \"sqlite:{db}\", query: \"SELECT 1 AS id\" }} }}\n  sink: {{ type: sqlite, config: {{ database_url: \"sqlite:{db}\", table_name: t, column_mapping: auto_map, create_table: true, write_mode: upsert, key: [id] }} }}\n  state: {{ type: memory }}\nprofiling: {{}}\n",
            db = db.display()
        );
        let cfg = PipelineConfig::from_text(&yaml, std::path::Path::new("m.yaml")).unwrap();
        let node = expand(&cfg).unwrap().remove(0);
        assert!(node.profiling.is_some());
        let key = vec!["id".to_string()];
        let rows = || vec![json!({"id": 1}), json!({"id": 2})];

        let target = repair_target(&node, &key, "upsert", rows());
        let opts = ExecuteOptions::for_tests("m-verify-repair");
        let built = crate::governance::governance_for(crate::governance::GovernanceRequest {
            node: &target,
            opts: &opts,
            pipeline: "m",
            row: &target.id,
            run_id: "r",
            cancel: &tokio_util::sync::CancellationToken::new(),
            meter: None,
        })
        .await
        .unwrap();
        assert!(built.profiler.is_none());
        assert!(
            built
                .governance
                .opted_out(faucet_core::Pass::Profiling)
                .is_some()
        );
        assert!(
            built
                .governance
                .opted_out(faucet_core::Pass::Masking)
                .is_some()
        );

        let inputs = VerifyInputs {
            row: None,
            repair: true,
            allow_delete: false,
            dry_run: false,
            pipeline_name: "m".into(),
            execution: None,
            auth: Default::default(),
            clock: chrono::Utc::now().fixed_offset(),
        };
        let tight = RunGuards {
            budget: Some(faucet_core::BudgetSpec {
                max_records: Some(1),
                ..Default::default()
            }),
            ..Default::default()
        };
        let err = run_repair(&node, &key, "upsert", rows(), &inputs, &tight)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("max_records"), "{err}");
        let written = run_repair(
            &node,
            &key,
            "upsert",
            rows(),
            &inputs,
            &RunGuards::default(),
        )
        .await
        .unwrap();
        assert_eq!(written, 2);

        // The unguarded entry point compares the same node: the source holds
        // only id 1, so the destination's id 2 is a difference.
        let plan = VerifyInputs {
            repair: false,
            ..inputs
        };
        let outcome = verify_node(&node, &VerifySpec::default(), &plan)
            .await
            .unwrap();
        assert_eq!(outcome.key, key);
        assert!(!outcome.report.differences.is_empty(), "{outcome:?}");
    }

    /// A SQLite database holding `src` (the source table) and an empty `t`
    /// (the destination), and the root node mirroring one into the other.
    #[cfg(all(feature = "source-sqlite", feature = "sink-sqlite"))]
    async fn sqlite_mirror(
        dir: &std::path::Path,
        key: &[&str],
        rows: &[(i64, &str, &str)],
        extra: &str,
    ) -> ExpandedNode {
        let db = dir.join("m.db");
        let url = format!("sqlite:{}?mode=rwc", db.display());
        let pool = sqlx::SqlitePool::connect(&url).await.unwrap();
        let pk = key.join(", ");
        for table in ["src", "t"] {
            sqlx::query(&format!(
                "CREATE TABLE {table} (id INTEGER, ts TEXT, v TEXT, PRIMARY KEY ({pk}))"
            ))
            .execute(&pool)
            .await
            .unwrap();
        }
        for (id, ts, v) in rows {
            sqlx::query("INSERT INTO src (id, ts, v) VALUES (?, ?, ?)")
                .bind(id)
                .bind(ts)
                .bind(v)
                .execute(&pool)
                .await
                .unwrap();
        }
        pool.close().await;
        let yaml = format!(
            "version: 1\nname: m\npipeline:\n  source: {{ type: sqlite, config: {{ database_url: \"sqlite:{db}\", query: \"SELECT id, ts, v FROM src\" }} }}\n  sink: {{ type: sqlite, config: {{ database_url: \"sqlite:{db}\", table_name: t, column_mapping: auto_map, write_mode: upsert, key: [{pk}] }} }}\n{extra}",
            db = db.display()
        );
        let cfg = PipelineConfig::from_text(&yaml, std::path::Path::new("m.yaml")).unwrap();
        expand(&cfg).unwrap().remove(0)
    }

    #[cfg(all(feature = "source-sqlite", feature = "sink-sqlite"))]
    fn repair_inputs() -> VerifyInputs {
        VerifyInputs {
            row: None,
            repair: true,
            allow_delete: false,
            dry_run: false,
            pipeline_name: "m".into(),
            execution: None,
            auth: Default::default(),
            clock: chrono::Utc::now().fixed_offset(),
        }
    }

    /// `max_differences` caps what is reported, never what is repaired.
    #[cfg(all(feature = "source-sqlite", feature = "sink-sqlite"))]
    #[tokio::test]
    async fn repair_covers_differences_beyond_the_reporting_cap() {
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<(i64, &str, &str)> = (1..=5).map(|i| (i, "x", "v")).collect();
        let node = sqlite_mirror(dir.path(), &["id"], &rows, "").await;
        let spec = VerifySpec {
            max_differences: 2,
            ..Default::default()
        };
        let out = verify_node(&node, &spec, &repair_inputs()).await.unwrap();
        assert_eq!(out.report.differences.len(), 2);
        assert!(out.report.truncated);
        assert_eq!(out.report.repaired_upserts, Some(5), "{out:?}");

        let check = VerifyInputs {
            repair: false,
            ..repair_inputs()
        };
        let again = verify_node(&node, &spec, &check).await.unwrap();
        assert!(again.report.differences.is_empty(), "{again:?}");
    }

    /// The repair looks rows up under the spec's normalizer, so a non-default
    /// `normalize` still re-syncs a timestamp-keyed row.
    #[cfg(all(feature = "source-sqlite", feature = "sink-sqlite"))]
    #[tokio::test]
    async fn repair_uses_the_specs_normalizer_for_key_lookup() {
        let dir = tempfile::tempdir().unwrap();
        let rows = [(7, "2026-01-01 00:00:00", "a")];
        let node = sqlite_mirror(dir.path(), &["id", "ts"], &rows, "").await;
        let spec = VerifySpec {
            key: vec!["id".into(), "ts".into()],
            normalize: Normalizer {
                timestamps: false,
                ..Default::default()
            },
            ..Default::default()
        };
        let out = verify_node(&node, &spec, &repair_inputs()).await.unwrap();
        assert_eq!(out.report.repaired_upserts, Some(1), "{out:?}");
    }

    /// `max_rows_scanned` bounds a whole-dataset comparison too: the read
    /// stops past the cap, the report is truncated, and a partial set is not
    /// diffed into false "missing" rows.
    #[cfg(all(feature = "source-sqlite", feature = "sink-sqlite"))]
    #[tokio::test]
    async fn the_scan_cap_bounds_a_full_comparison() {
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<(i64, &str, &str)> = (1..=5).map(|i| (i, "t", "v")).collect();
        let node = sqlite_mirror(dir.path(), &["id", "ts"], &rows, "").await;
        let spec = VerifySpec {
            key: vec!["id".into(), "ts".into()],
            max_rows_scanned: Some(3),
            ..Default::default()
        };
        let check = VerifyInputs {
            repair: false,
            ..repair_inputs()
        };
        let out = verify_node(&node, &spec, &check).await.unwrap();
        assert_eq!(out.strategy, "full");
        assert!(out.report.truncated, "{out:?}");
        assert!(out.report.rows_fetched_source <= 3, "{out:?}");
        assert!(out.report.differences.is_empty(), "{out:?}");
        let uncapped = verify_node(
            &node,
            &VerifySpec {
                max_rows_scanned: None,
                ..spec
            },
            &check,
        )
        .await
        .unwrap();
        assert_eq!(uncapped.report.differences.len(), 5);
    }

    #[test]
    fn guards_from_a_config_carry_its_budget_and_resilience() {
        let cfg = PipelineConfig::from_text(
            "version: 1\nbudget: { max_records: 5 }\nresilience: { retry: { max_attempts: 2 } }\npipeline:\n  source: { type: rest, config: { url: \"http://x\" } }\n  sink: { type: stdout, config: {} }\n",
            std::path::Path::new("g.yaml"),
        )
        .unwrap();
        let g = RunGuards::from_config(&cfg).unwrap();
        assert_eq!(g.budget.unwrap().max_records, Some(5));
        assert_eq!(g.resilience.unwrap().retry.max_attempts, 2);
        assert!(g.cancel.is_none());
    }

    #[test]
    fn shard_key_is_injected_once() {
        let mut cfg = json!({"connection_url": "x", "query": "select 1"});
        inject_shard_key(&mut cfg, "id");
        assert_eq!(cfg["shard"], json!({"key": "id"}));
        let mut cfg = json!({"shard": {"key": "other"}});
        inject_shard_key(&mut cfg, "id");
        assert_eq!(cfg["shard"]["key"], "other", "an explicit shard wins");
        let mut scalar = json!("nope");
        inject_shard_key(&mut scalar, "id");
        assert_eq!(scalar, json!("nope"));
    }

    #[test]
    fn shard_descriptors_map_to_ranges() {
        let all = range_of_shard(&faucet_core::ShardSpec::whole());
        assert_eq!(all, KeyRange::ALL);
        let shards = faucet_core::shard::plan_pk_shards("id", 0, 99, 2);
        let first = range_of_shard(&shards[0]);
        assert_eq!(first.lo, None, "first shard is open below");
        assert!(first.hi.is_some());
        let last = range_of_shard(&shards[shards.len() - 1]);
        assert_eq!(last.hi, None, "last shard is open above");
        let hash = faucet_core::ShardSpec::new("h", json!({"shards": 2, "index": 0}));
        assert_eq!(range_of_shard(&hash), KeyRange::ALL);
    }

    #[test]
    fn schema_columns_drop_key_and_excluded() {
        let schema = json!({"type": "object", "properties": {"id": {}, "_faucet_run_id": {}, "b": {}, "a": {}}});
        assert_eq!(
            schema_columns(&schema, &["id".into()], &["_faucet_*".into()]),
            vec!["a".to_string(), "b".to_string()]
        );
        assert!(schema_columns(&json!({}), &[], &[]).is_empty());
    }

    #[test]
    fn key_object_keeps_only_key_columns() {
        let rec = json!({"id": 1, "v": "x", "extra": true});
        assert_eq!(
            key_object(&rec, &["id".into(), "missing".into()]),
            json!({"id": 1, "missing": null})
        );
    }

    #[tokio::test]
    async fn vec_source_yields_once() {
        let s = VecSource::new(vec![json!({"a": 1})]);
        assert_eq!(s.fetch_all().await.unwrap().len(), 1);
        assert!(s.fetch_all().await.unwrap().is_empty());
        assert_eq!(s.connector_name(), "verify-repair");
        assert!(s.config_schema().is_object());
    }

    #[test]
    fn range_read_is_a_declared_capability() {
        #[cfg(feature = "source-sqlite")]
        assert!(source_reads_ranges("sqlite", &json!({})));
        assert!(!source_reads_ranges("no-such-source", &json!({})));
        let a = ServerDigest {
            algorithm: "x".into(),
            rows: 1,
            digest: "1".into(),
            key_min: None,
            key_max: None,
        };
        let mut b = a.clone();
        b.algorithm = "y".into();
        assert!(digests_comparable(&a, &a));
        assert!(!digests_comparable(&a, &b));
    }
}
