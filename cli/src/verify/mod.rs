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

/// Source kinds whose `apply_shard` narrows a query to a PK range, so the
/// verifier can read one key range at a time. Mirrors the PK-range shardable
/// sources (#230/#262).
pub const RANGE_READ_SOURCE_KINDS: &[&str] = &["postgres", "mysql", "sqlite", "mssql"];

/// Whether a source kind can read one integer-key range at a time.
pub fn source_reads_ranges(kind: &str) -> bool {
    RANGE_READ_SOURCE_KINDS.contains(&kind)
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
    let nodes = expand(cfg)?;
    let node = crate::dlq_replay::plan::select_replay_node(nodes, inputs.row.as_deref())?;
    verify_node(&node, spec, &inputs).await
}

/// [`verify_node`] behind a heap allocation, built in its own never-inlined
/// frame — what the executor awaits from inside `run_one_invocation`, so the
/// verifier's (large) state machine never lands in that future's frame.
#[inline(never)]
pub fn verify_node_boxed<'a>(
    node: &'a ExpandedNode,
    spec: &'a VerifySpec,
    inputs: &'a VerifyInputs,
) -> futures::future::BoxFuture<'a, CliResult<VerifyOutcome>> {
    Box::pin(verify_node(node, spec, inputs))
}

/// Verify an already-expanded root node. Shared by the command and the
/// executor's post-run pass.
pub async fn verify_node(
    node: &ExpandedNode,
    spec: &VerifySpec,
    inputs: &VerifyInputs,
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

    // Range mode needs one integer key and two range-readable sources.
    let range_mode =
        key.len() == 1 && source_reads_ranges(&node.source.kind) && source_reads_ranges(&dest_kind);
    let mut source_cfg = source_cfg;
    let mut dest_cfg = dest_cfg;
    if range_mode {
        inject_shard_key(&mut source_cfg, &key[0]);
        inject_shard_key(&mut dest_cfg, &key[0]);
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
        session.verify_ranges(spec).await?;
        "range"
    } else {
        session.verify_full().await?;
        "full"
    };
    // Cap what is reported; the tally still counts everything found.
    let total = session.report.differences.len();
    if total > spec.max_differences {
        session.report.differences.truncate(spec.max_differences);
        session.report.truncated = true;
    }
    let mut report = session.report;
    let source_rows = session.source_rows;

    let repair = inputs.repair || spec.repair;
    if repair && !report.differences.is_empty() {
        let allow_delete = inputs.allow_delete || spec.allow_delete;
        let (ups, dels) = repair_node(
            node,
            &key,
            &report.differences,
            &source_rows,
            allow_delete,
            inputs,
        )
        .await?;
        report.repaired_upserts = Some(ups);
        report.repaired_deletes = Some(dels);
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

    /// Fetch the source side of `range` (whole dataset for `ALL`), shaped as
    /// the pipeline would write it.
    async fn fetch_source(&mut self, range: &KeyRange) -> CliResult<Vec<Value>> {
        let rows = fetch_range(self.source, range, self.key, "source").await?;
        self.report.rows_fetched_source += rows.len() as u64;
        #[cfg(feature = "masking")]
        let rows = match self.masking {
            Some(m) => faucet_core::apply_masking(rows, m).records,
            None => rows,
        };
        Ok(rows)
    }

    async fn fetch_dest(&mut self, range: &KeyRange) -> CliResult<Vec<Value>> {
        let rows = fetch_range(self.dest, range, self.key, "destination").await?;
        self.report.rows_fetched_dest += rows.len() as u64;
        Ok(rows)
    }

    /// Whole-dataset comparison: one fetch of each side, one keyed diff.
    async fn verify_full(&mut self) -> CliResult<()> {
        self.report.ranges_compared = 1;
        self.diff_leaf(&KeyRange::ALL).await
    }

    /// Fetch both sides of a leaf range and record the keyed differences.
    async fn diff_leaf(&mut self, range: &KeyRange) -> CliResult<()> {
        let src = self.fetch_source(range).await?;
        let dst = self.fetch_dest(range).await?;
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
    async fn verify_ranges(&mut self, spec: &VerifySpec) -> CliResult<()> {
        let key = &self.key[0];
        let shards = self
            .source
            .enumerate_shards(spec.ranges)
            .await
            .map_err(|e| CliError::Config(format!("verify: planning key ranges: {e}")))?;
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
                let a = self.client_digest(&range, true).await?;
                let b = self.client_digest(&range, false).await?;
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
    ) -> CliResult<faucet_core::diff::ContentDigest> {
        let rows = if is_source {
            self.fetch_source(range).await?
        } else {
            self.fetch_dest(range).await?
        };
        let mut acc = DigestAccumulator::new();
        for r in &rows {
            acc.add(
                row_hash(r, self.key, self.columns, self.exclude, self.norm),
                faucet_core::diff::key_int(r, self.key),
            );
        }
        Ok(acc.finish())
    }
}

/// Read one key range from a source. `ALL` clears any applied range.
async fn fetch_range(
    src: &dyn Source,
    range: &KeyRange,
    key: &[String],
    side: &str,
) -> CliResult<Vec<Value>> {
    if key.len() == 1 {
        src.apply_shard(&range.to_shard(&key[0]))
            .await
            .map_err(|e| {
                CliError::Config(format!("verify: narrowing the {side} to {range}: {e}"))
            })?;
    }
    src.fetch_all().await.map_err(|e| {
        CliError::Faucet(FaucetError::Source(format!(
            "verify: reading the {side}: {e}"
        )))
    })
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
    allow_delete: bool,
    inputs: &VerifyInputs,
) -> CliResult<(u64, u64)> {
    if !crate::registry::sink_supported_write_modes(&node.sink.kind)
        .contains(&faucet_core::WriteMode::Upsert)
    {
        return Err(CliError::Config(format!(
            "verify --repair: sink '{}' does not support keyed writes (write_mode: upsert), so \
             differing keys cannot be re-synced through it",
            node.sink.kind
        )));
    }
    let norm = Normalizer::default();
    let mut upserts: Vec<Value> = Vec::new();
    let mut deletes: Vec<Value> = Vec::new();
    for d in differences {
        let kt = key_text(&d.key, key, &norm);
        if d.needs_upsert() {
            if let Some(row) = source_rows.get(&kt) {
                upserts.push(row.clone());
            }
        } else if d.needs_delete() && allow_delete {
            deletes.push(d.key.clone());
        }
    }
    let mut written = 0u64;
    let mut deleted = 0u64;
    if !upserts.is_empty() {
        written = run_repair(node, key, "upsert", upserts, inputs).await?;
    }
    if !deletes.is_empty() {
        deleted = run_repair(node, key, "delete", deletes, inputs).await?;
    }
    Ok((written, deleted))
}

/// Run one repair pass: a copy of the row whose source is the given records
/// and whose sink is forced to `write_mode`. Transforms and masking are
/// cleared (the rows are already shaped), state is dropped (no bookmark to
/// advance), delivery is at-least-once (keyed writes are idempotent).
async fn run_repair(
    node: &ExpandedNode,
    key: &[String],
    write_mode: &str,
    records: Vec<Value>,
    inputs: &VerifyInputs,
) -> CliResult<u64> {
    let mut repair = node.clone();
    repair.source = ConnectorSpec {
        kind: "verify-repair".into(),
        ..node.source.clone()
    };
    repair.source_override = Some(SourceOverride::new(Box::new(VecSource::new(records))));
    repair.transforms.clear();
    #[cfg(feature = "masking")]
    {
        repair.masking = None;
    }
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
    // The repair runs the executor from inside the executor's own post-run
    // pass; `run_expanded_boxed` breaks the recursive future type.
    let summary = crate::executor::run_expanded_boxed(
        vec![repair],
        ExecuteOptions {
            pipeline_name: format!("{}-verify-repair", inputs.pipeline_name),
            run_id: None,
            execution: inputs.execution.clone(),
            concurrency: None,
            dry_run: inputs.dry_run,
            limit: None,
            state_path_override: None,
            shard: None,
            auth: inputs.auth.clone(),
            clock: inputs.clock,
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
    fn range_read_allowlist() {
        assert!(source_reads_ranges("postgres"));
        assert!(source_reads_ranges("sqlite"));
        assert!(!source_reads_ranges("csv"));
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
