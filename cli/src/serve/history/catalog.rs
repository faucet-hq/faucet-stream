//! Data Movement Catalog storage types + pure merge logic (#279).
//!
//! The catalog is the accumulating, cross-run picture of every dataset a
//! pipeline touches: identity (a canonical, credential-redacted dataset URI),
//! a deduplicated schema timeline, per-run volume/freshness stats, and the
//! lineage edges between datasets. It rides the run-history backends — the
//! in-memory store and the shared SQL machinery both implement the
//! `catalog_*` methods on [`RunHistory`](super::RunHistory) — so persistence
//! reuses the existing `--history` / `serve-history-*` plumbing and the
//! `FallbackHistory` degradation contract (a catalog write never fails a run).
//!
//! Everything in this module is **pure**: the merge of one run's observation
//! into a dataset record ([`apply_observation`]), schema hashing/dedup
//! ([`schema_hash`]), the list filter ([`filter_datasets`]), and the
//! depth-bounded lineage BFS ([`lineage_slice`]) are shared by both backends
//! so they can never drift apart. Backends only do I/O.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// How many volume points a dataset keeps (older ones are pruned on write).
pub const STATS_RETAIN: usize = 500;

/// How many of the most recent volume points a detail read returns.
pub const STATS_DETAIL_LIMIT: usize = 50;

/// Default depth bound for the lineage graph read.
pub const LINEAGE_DEFAULT_DEPTH: u32 = 5;

/// Which side of a pipeline a dataset was observed on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DatasetRole {
    Source,
    Sink,
}

impl DatasetRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Sink => "sink",
        }
    }
}

/// One observation of a dataset from a finished, successful root invocation.
#[derive(Debug, Clone)]
pub struct DatasetObservation {
    /// Canonical dataset URI — credential-redacted and `${now.*}`-templated
    /// segments folded back to their tokens, so dated paths converge on one
    /// dataset instead of one per day.
    pub uri: String,
    /// Connector kind (`"csv"`, `"postgres"`, …).
    pub kind: String,
    pub role: DatasetRole,
    /// Observed record schema (`infer_schema`-shaped
    /// `{"type":"object","properties":{…}}`), `None` when nothing was sampled.
    pub schema: Option<Value>,
    /// Records read from / written to this dataset in the run.
    pub records: u64,
}

/// The composite catalog write for one run: both dataset observations plus the
/// source→sink lineage edge. One call so backends can keep the write paths
/// together (and a partial failure degrades the whole update, not half of it).
#[derive(Debug, Clone)]
pub struct CatalogUpdate {
    /// Provenance: the serve run id, or the invocation run id for CLI runs.
    pub run_id: String,
    pub pipeline: String,
    /// Matrix row id (`"default"` for non-matrix runs).
    pub row: String,
    pub recorded_at: DateTime<Utc>,
    /// Input datasets. A single-source pipeline has one; a topology sink fed by a
    /// merge or join has one per source that reaches it (#459). Each carries its
    /// own record count, so per-dataset volume stays accurate instead of the sink
    /// total being repeated across edges.
    pub sources: Vec<DatasetObservation>,
    pub sink: DatasetObservation,
    /// Column-lineage facet derived by `faucet-lineage` for the edge, when the
    /// transform chain is expressible (`None` when opaque).
    pub column_lineage: Option<Value>,
}

// ── Config snapshots (#374) ──────────────────────────────────────────────────
//
// A `faucet plan --diff`-able record of the *resolved + expanded* config as it
// last ran. One snapshot per pipeline (latest wins); each carries a per-row,
// secret-redacted view so the diff reflects real data-movement effects, never a
// misleading raw-YAML text diff. Stored types only — the build-from-`ExpandedNode`
// and diff/render logic lives in `cli/src/catalog/snapshot.rs` (a higher layer
// that may depend on the CLI's expand types; this module must not).

/// A redacted, resolved+expanded config snapshot recorded on a successful run.
/// Diffed by `faucet plan --diff` against the current config.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigSnapshot {
    pub pipeline: String,
    pub recorded_at: DateTime<Utc>,
    /// `faucet` version that recorded the snapshot (informational).
    pub faucet_version: String,
    /// Expanded row id → row snapshot. `BTreeMap` so serialization and diffs are
    /// deterministic regardless of expansion order.
    pub rows: std::collections::BTreeMap<String, RowSnapshot>,
}

/// One expanded row (a single source→sink movement) as it last ran.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RowSnapshot {
    pub source: ConnectorSnapshot,
    pub sink: ConnectorSnapshot,
    pub transforms: Vec<TransformSnapshot>,
    /// Durable state key for this row, when a state store is configured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_key: Option<String>,
    /// End-to-end delivery guarantee (`Debug` of `DeliveryGuarantee`).
    pub delivery_guarantee: String,
    /// Pipeline-level `execution.on_error` (`"stop"` / `"continue"`).
    pub on_error: String,
    /// Whether a DLQ sink is attached to this row.
    pub dlq: bool,
}

/// A connector (source or sink) with its **secret-redacted** resolved config.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConnectorSnapshot {
    pub kind: String,
    /// Resolved config with every secret-sourced value replaced by a stable
    /// `<secret:sha256:…>` token — no secret material is ever persisted.
    pub config: Value,
}

/// A transform stage with its resolved (redacted) config.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransformSnapshot {
    pub kind: String,
    pub config: Value,
}

/// One catalogued dataset — the list element and the head of the detail view.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogDataset {
    /// Stable id: 16 hex chars of sha256(uri). Used in URLs and edge keys.
    pub id: String,
    pub uri: String,
    pub kind: String,
    /// Roles this dataset has been seen in (`"source"` / `"sink"`), sorted.
    pub roles: Vec<String>,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    /// Last successful run that touched this dataset (freshness).
    pub last_success: DateTime<Utc>,
    pub last_run_id: String,
    /// Pipeline name of the most recent run that touched this dataset.
    pub pipeline: String,
    /// Records moved in the most recent run.
    pub last_records: u64,
    /// Records moved across all recorded runs.
    pub total_records: u64,
    /// Number of recorded runs that touched this dataset.
    pub runs: u64,
    /// Number of schema-timeline entries (0 when never sampled).
    pub schema_versions: u32,
    /// Latest observed schema (`None` when never sampled).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_schema: Option<Value>,
    /// Content hash of `current_schema` — the dedupe key for the timeline.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_schema_hash: Option<String>,
}

/// One schema-timeline entry. Appended only when the observed schema's content
/// hash differs from the previous entry, so the timeline stays compact.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogSchemaVersion {
    pub dataset_id: String,
    /// 1-based, monotonically increasing per dataset.
    pub version: u32,
    pub recorded_at: DateTime<Utc>,
    pub run_id: String,
    pub schema: Value,
    pub schema_hash: String,
    /// Diff against the previous version (computed via
    /// `faucet_core::drift::diff_schema`); `None` for the first version.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diff: Option<Value>,
}

/// One per-run volume point for a dataset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogStatsPoint {
    pub recorded_at: DateTime<Utc>,
    pub run_id: String,
    pub records: u64,
}

/// One source→sink lineage edge, keyed by `(src_id, dst_id)`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogLineageEdge {
    pub src_id: String,
    pub dst_id: String,
    pub src_uri: String,
    pub dst_uri: String,
    pub pipeline: String,
    pub row: String,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub last_run_id: String,
    /// Recorded runs that traversed this edge.
    pub runs: u64,
    /// Records moved along this edge in the most recent run.
    pub last_records: u64,
    /// Column-lineage facet from the most recent run (`None` when opaque).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column_lineage: Option<Value>,
}

/// Filter + keyset pagination for the dataset list.
#[derive(Debug, Default, Clone)]
pub struct CatalogListFilter {
    /// Exact connector-kind match.
    pub kind: Option<String>,
    /// Case-insensitive substring match on the dataset URI.
    pub q: Option<String>,
    pub limit: usize,
    /// Dataset id of the last element of the previous page.
    pub cursor: Option<String>,
}

/// One page of the dataset list, ordered `(last_seen DESC, id DESC)`.
#[derive(Debug, Serialize)]
pub struct CatalogDatasetPage {
    pub datasets: Vec<CatalogDataset>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// The full detail view for one dataset.
#[derive(Debug, Serialize)]
pub struct CatalogDatasetDetail {
    #[serde(flatten)]
    pub dataset: CatalogDataset,
    /// Schema timeline, oldest first.
    pub schema_timeline: Vec<CatalogSchemaVersion>,
    /// Most recent volume points, newest first (bounded by
    /// [`STATS_DETAIL_LIMIT`]).
    pub stats: Vec<CatalogStatsPoint>,
    /// Edges whose destination is this dataset.
    pub upstream: Vec<CatalogLineageEdge>,
    /// Edges whose source is this dataset.
    pub downstream: Vec<CatalogLineageEdge>,
}

/// Stable dataset id: the first 16 hex chars of sha256(uri). Short enough for
/// URLs, long enough that collisions are out of reach at catalog scale.
pub fn dataset_id(uri: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(uri.as_bytes());
    hex_prefix(&digest, 16)
}

/// Content hash of a schema value over a canonical (recursively key-sorted)
/// rendering, so the hash is independent of `serde_json`'s map ordering
/// (`preserve_order` flips between builds).
pub fn schema_hash(schema: &Value) -> String {
    use sha2::{Digest, Sha256};
    let mut canonical = String::new();
    canonical_json(schema, &mut canonical);
    let digest = Sha256::digest(canonical.as_bytes());
    hex_prefix(&digest, 16)
}

fn hex_prefix(bytes: &[u8], chars: usize) -> String {
    let mut out = String::with_capacity(chars);
    for b in bytes {
        use std::fmt::Write as _;
        let _ = write!(out, "{b:02x}");
        if out.len() >= chars {
            break;
        }
    }
    out.truncate(chars);
    out
}

/// Render `v` with object keys sorted recursively (arrays keep order).
fn canonical_json(v: &Value, out: &mut String) {
    match v {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push('{');
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&Value::String((*k).clone()).to_string());
                out.push(':');
                canonical_json(&map[*k], out);
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                canonical_json(item, out);
            }
            out.push(']');
        }
        scalar => out.push_str(&scalar.to_string()),
    }
}

/// Serialize a `faucet_core::drift::SchemaDiff` (not `Serialize` itself) into
/// a stable JSON shape for the schema-timeline `diff` field.
fn diff_to_value(diff: &faucet_core::SchemaDiff) -> Value {
    let change = |c: &faucet_core::ColumnChange| -> Value {
        json!({ "column": c.name, "from": c.from, "to": c.to })
    };
    json!({
        "added": diff.additions.iter().map(change).collect::<Vec<_>>(),
        "widened": diff.widenings.iter().map(change).collect::<Vec<_>>(),
        "changed": diff.incompatible.iter().map(change).collect::<Vec<_>>(),
        "removed": diff.droppable_required.clone(),
    })
}

/// Whether a diff value carries any actual change (an all-empty diff is
/// omitted from the timeline entry).
fn diff_is_empty(diff: &Value) -> bool {
    ["added", "widened", "changed", "removed"].iter().all(|k| {
        diff.get(k)
            .and_then(Value::as_array)
            .is_none_or(Vec::is_empty)
    })
}

/// Fold one run's observation into the (possibly absent) existing dataset
/// record. Returns the updated record plus a new schema-timeline entry when —
/// and only when — the observed schema's content hash differs from the
/// current one.
pub fn apply_observation(
    existing: Option<&CatalogDataset>,
    obs: &DatasetObservation,
    run_id: &str,
    pipeline: &str,
    row: &str,
    now: DateTime<Utc>,
) -> (CatalogDataset, Option<CatalogSchemaVersion>) {
    let _ = row; // provenance detail carried on the edge, not the dataset
    let id = dataset_id(&obs.uri);
    let mut ds = match existing {
        Some(prev) => prev.clone(),
        None => CatalogDataset {
            id: id.clone(),
            uri: obs.uri.clone(),
            kind: obs.kind.clone(),
            roles: Vec::new(),
            first_seen: now,
            last_seen: now,
            last_success: now,
            last_run_id: run_id.to_string(),
            pipeline: pipeline.to_string(),
            last_records: 0,
            total_records: 0,
            runs: 0,
            schema_versions: 0,
            current_schema: None,
            current_schema_hash: None,
        },
    };
    let role = obs.role.as_str().to_string();
    if !ds.roles.contains(&role) {
        ds.roles.push(role);
        ds.roles.sort();
    }
    ds.kind = obs.kind.clone();
    ds.last_seen = now;
    ds.last_success = now;
    ds.last_run_id = run_id.to_string();
    ds.pipeline = pipeline.to_string();
    ds.last_records = obs.records;
    ds.total_records = ds.total_records.saturating_add(obs.records);
    ds.runs = ds.runs.saturating_add(1);

    let new_version = match &obs.schema {
        Some(schema) => {
            let hash = schema_hash(schema);
            if ds.current_schema_hash.as_deref() == Some(hash.as_str()) {
                None
            } else {
                let diff = ds.current_schema.as_ref().map(|prev| {
                    diff_to_value(&faucet_core::drift::diff_schema(prev, schema, true))
                });
                let diff = diff.filter(|d| !diff_is_empty(d));
                ds.schema_versions += 1;
                ds.current_schema = Some(schema.clone());
                ds.current_schema_hash = Some(hash.clone());
                Some(CatalogSchemaVersion {
                    dataset_id: id,
                    version: ds.schema_versions,
                    recorded_at: now,
                    run_id: run_id.to_string(),
                    schema: schema.clone(),
                    schema_hash: hash,
                    diff,
                })
            }
        }
        None => None,
    };
    (ds, new_version)
}

/// Fold one run's traversal into the (possibly absent) existing lineage edge.
pub fn apply_edge(
    existing: Option<&CatalogLineageEdge>,
    update: &CatalogUpdate,
    source: &DatasetObservation,
) -> CatalogLineageEdge {
    let mut edge = match existing {
        Some(prev) => prev.clone(),
        None => CatalogLineageEdge {
            src_id: dataset_id(&source.uri),
            dst_id: dataset_id(&update.sink.uri),
            src_uri: source.uri.clone(),
            dst_uri: update.sink.uri.clone(),
            pipeline: update.pipeline.clone(),
            row: update.row.clone(),
            first_seen: update.recorded_at,
            last_seen: update.recorded_at,
            last_run_id: update.run_id.clone(),
            runs: 0,
            last_records: 0,
            column_lineage: None,
        },
    };
    edge.pipeline = update.pipeline.clone();
    edge.row = update.row.clone();
    edge.last_seen = update.recorded_at;
    edge.last_run_id = update.run_id.clone();
    edge.runs = edge.runs.saturating_add(1);
    // The records this edge carried. For a single-source pipeline the source read
    // count and the sink write count coincide; for a merge, attributing the sink
    // total to every edge would over-count, so each edge reports its own source's
    // contribution (#459).
    edge.last_records = if update.sources.len() == 1 {
        update.sink.records
    } else {
        source.records
    };
    if update.column_lineage.is_some() {
        edge.column_lineage = update.column_lineage.clone();
    }
    edge
}

/// Filter, order (`last_seen DESC, id DESC`), and keyset-paginate the dataset
/// list. Shared by the memory and SQL backends (which fetch all rows and
/// delegate here) so the two can never disagree on filter semantics.
pub fn filter_datasets(
    mut all: Vec<CatalogDataset>,
    filter: &CatalogListFilter,
) -> CatalogDatasetPage {
    all.retain(|d| filter.kind.as_deref().is_none_or(|k| d.kind == k));
    if let Some(q) = filter.q.as_deref() {
        let q = q.to_lowercase();
        all.retain(|d| d.uri.to_lowercase().contains(&q));
    }
    all.sort_by(|a, b| b.last_seen.cmp(&a.last_seen).then_with(|| b.id.cmp(&a.id)));
    if let Some(cursor) = &filter.cursor
        && let Some(pos) = all.iter().position(|d| &d.id == cursor)
    {
        all.drain(..=pos);
    }
    let limit = filter.limit.max(1);
    let next_cursor = if all.len() > limit {
        Some(all[limit - 1].id.clone())
    } else {
        None
    };
    all.truncate(limit);
    CatalogDatasetPage {
        datasets: all,
        next_cursor,
    }
}

/// Slice the edge graph for the lineage read: with no root, return everything;
/// with a root, BFS outward (both directions) up to `depth` hops. Shared by
/// both backends.
pub fn lineage_slice(
    edges: Vec<CatalogLineageEdge>,
    root: Option<&str>,
    depth: u32,
) -> Vec<CatalogLineageEdge> {
    let Some(root) = root else {
        return edges;
    };
    let mut frontier: std::collections::HashSet<String> =
        std::collections::HashSet::from([root.to_string()]);
    let mut reached = frontier.clone();
    let mut kept: Vec<usize> = Vec::new();
    let mut kept_set: std::collections::HashSet<usize> = std::collections::HashSet::new();
    for _ in 0..depth.max(1) {
        let mut next: std::collections::HashSet<String> = std::collections::HashSet::new();
        for (i, e) in edges.iter().enumerate() {
            if kept_set.contains(&i) {
                continue;
            }
            if frontier.contains(&e.src_id) || frontier.contains(&e.dst_id) {
                kept.push(i);
                kept_set.insert(i);
                for id in [&e.src_id, &e.dst_id] {
                    if reached.insert(id.clone()) {
                        next.insert(id.clone());
                    }
                }
            }
        }
        if next.is_empty() {
            break;
        }
        frontier = next;
    }
    kept.sort_unstable();
    let mut kept_edges = Vec::with_capacity(kept.len());
    let mut edges = edges;
    // Drain in reverse so earlier indices stay valid.
    for i in kept.iter().rev() {
        kept_edges.push(edges.swap_remove(*i));
    }
    kept_edges.reverse();
    kept_edges
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn obs(
        uri: &str,
        role: DatasetRole,
        schema: Option<Value>,
        records: u64,
    ) -> DatasetObservation {
        DatasetObservation {
            uri: uri.into(),
            kind: "csv".into(),
            role,
            schema,
            records,
        }
    }

    fn schema_a() -> Value {
        json!({"type": "object", "properties": {"id": {"type": "integer"}, "name": {"type": "string"}}})
    }

    fn schema_b() -> Value {
        json!({"type": "object", "properties": {"id": {"type": "integer"}, "name": {"type": "string"}, "email": {"type": "string"}}})
    }

    #[test]
    fn dataset_id_is_stable_and_short() {
        let a = dataset_id("csv://./in.csv");
        assert_eq!(a.len(), 16);
        assert_eq!(a, dataset_id("csv://./in.csv"));
        assert_ne!(a, dataset_id("csv://./other.csv"));
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn schema_hash_is_key_order_independent() {
        let a = json!({"properties": {"a": {"type": "string"}, "b": {"type": "integer"}}});
        let b = json!({"properties": {"b": {"type": "integer"}, "a": {"type": "string"}}});
        assert_eq!(schema_hash(&a), schema_hash(&b));
        assert_ne!(
            schema_hash(&a),
            schema_hash(&json!({"properties": {"a": {"type": "integer"}}}))
        );
    }

    #[test]
    fn schema_hash_covers_arrays_and_preserves_their_order() {
        // Nullable columns infer as `"type": ["string", "null"]` — the
        // canonical rendering must keep array ORDER significant while still
        // sorting object keys.
        let a = json!({"properties": {"a": {"type": ["string", "null"]}}});
        let b = json!({"properties": {"a": {"type": ["null", "string"]}}});
        assert_ne!(schema_hash(&a), schema_hash(&b), "array order is meaning");
        assert_eq!(schema_hash(&a), schema_hash(&a.clone()));
    }

    #[test]
    fn first_observation_creates_dataset_and_version_one() {
        let now = Utc::now();
        let (ds, v) = apply_observation(
            None,
            &obs("csv://./in.csv", DatasetRole::Source, Some(schema_a()), 10),
            "r1",
            "p",
            "default",
            now,
        );
        assert_eq!(ds.id, dataset_id("csv://./in.csv"));
        assert_eq!(ds.roles, vec!["source"]);
        assert_eq!(ds.runs, 1);
        assert_eq!(ds.total_records, 10);
        assert_eq!(ds.schema_versions, 1);
        let v = v.expect("first schema observation appends version 1");
        assert_eq!(v.version, 1);
        assert!(v.diff.is_none(), "no previous schema, no diff");
    }

    #[test]
    fn unchanged_schema_does_not_append_a_version() {
        let now = Utc::now();
        let (ds, _) = apply_observation(
            None,
            &obs("csv://./in.csv", DatasetRole::Source, Some(schema_a()), 10),
            "r1",
            "p",
            "default",
            now,
        );
        let (ds2, v2) = apply_observation(
            Some(&ds),
            &obs("csv://./in.csv", DatasetRole::Source, Some(schema_a()), 7),
            "r2",
            "p",
            "default",
            now,
        );
        assert!(v2.is_none(), "identical schema must dedupe");
        assert_eq!(ds2.schema_versions, 1);
        assert_eq!(ds2.runs, 2);
        assert_eq!(ds2.total_records, 17);
        assert_eq!(ds2.last_records, 7);
        assert_eq!(ds2.last_run_id, "r2");
    }

    #[test]
    fn changed_schema_appends_a_version_with_a_diff() {
        let now = Utc::now();
        let (ds, _) = apply_observation(
            None,
            &obs("csv://./in.csv", DatasetRole::Source, Some(schema_a()), 10),
            "r1",
            "p",
            "default",
            now,
        );
        let (ds2, v2) = apply_observation(
            Some(&ds),
            &obs("csv://./in.csv", DatasetRole::Source, Some(schema_b()), 10),
            "r2",
            "p",
            "default",
            now,
        );
        assert_eq!(ds2.schema_versions, 2);
        let v2 = v2.expect("schema change appends version 2");
        assert_eq!(v2.version, 2);
        let diff = v2.diff.expect("second version diffs against the first");
        let added = diff["added"].as_array().unwrap();
        assert_eq!(added.len(), 1);
        assert_eq!(added[0]["column"], "email");
    }

    #[test]
    fn roles_accumulate_and_sort() {
        let now = Utc::now();
        let (ds, _) = apply_observation(
            None,
            &obs("x://d", DatasetRole::Sink, None, 1),
            "r1",
            "p",
            "default",
            now,
        );
        let (ds2, _) = apply_observation(
            Some(&ds),
            &obs("x://d", DatasetRole::Source, None, 1),
            "r2",
            "p",
            "default",
            now,
        );
        assert_eq!(ds2.roles, vec!["sink", "source"]);
        assert!(ds2.current_schema.is_none());
        assert_eq!(ds2.schema_versions, 0);
    }

    fn update(src: &str, dst: &str, records: u64) -> CatalogUpdate {
        CatalogUpdate {
            run_id: "r1".into(),
            pipeline: "p".into(),
            row: "default".into(),
            recorded_at: Utc::now(),
            sources: vec![obs(src, DatasetRole::Source, None, records)],
            sink: obs(dst, DatasetRole::Sink, None, records),
            column_lineage: None,
        }
    }

    #[test]
    fn edge_accumulates_and_keeps_last_column_lineage() {
        let mut u = update("a://1", "b://2", 5);
        u.column_lineage = Some(json!({"fields": {"x": {}}}));
        let e = apply_edge(None, &u, &u.sources[0]);
        assert_eq!(e.runs, 1);
        assert_eq!(e.last_records, 5);
        assert!(e.column_lineage.is_some());

        // A later opaque run keeps the previous column lineage.
        let mut u2 = update("a://1", "b://2", 9);
        u2.run_id = "r2".into();
        let e2 = apply_edge(Some(&e), &u2, &u2.sources[0]);
        assert_eq!(e2.runs, 2);
        assert_eq!(e2.last_records, 9);
        assert_eq!(e2.last_run_id, "r2");
        assert!(e2.column_lineage.is_some(), "opaque run keeps prior facet");
    }

    fn ds(id_uri: &str, kind: &str, last_seen: DateTime<Utc>) -> CatalogDataset {
        CatalogDataset {
            id: dataset_id(id_uri),
            uri: id_uri.into(),
            kind: kind.into(),
            roles: vec!["source".into()],
            first_seen: last_seen,
            last_seen,
            last_success: last_seen,
            last_run_id: "r".into(),
            pipeline: "p".into(),
            last_records: 0,
            total_records: 0,
            runs: 1,
            schema_versions: 0,
            current_schema: None,
            current_schema_hash: None,
        }
    }

    #[test]
    fn filter_datasets_filters_orders_and_paginates() {
        let t0 = Utc::now();
        let all = vec![
            ds("csv://a", "csv", t0),
            ds("csv://b", "csv", t0 + chrono::Duration::seconds(1)),
            ds(
                "postgres://h/db",
                "postgres",
                t0 + chrono::Duration::seconds(2),
            ),
        ];
        // Kind filter.
        let page = filter_datasets(
            all.clone(),
            &CatalogListFilter {
                kind: Some("postgres".into()),
                limit: 10,
                ..Default::default()
            },
        );
        assert_eq!(page.datasets.len(), 1);
        assert_eq!(page.datasets[0].kind, "postgres");
        // Substring filter, case-insensitive.
        let page = filter_datasets(
            all.clone(),
            &CatalogListFilter {
                q: Some("CSV://".into()),
                limit: 10,
                ..Default::default()
            },
        );
        assert_eq!(page.datasets.len(), 2);
        // Newest-first + pagination.
        let page = filter_datasets(
            all.clone(),
            &CatalogListFilter {
                limit: 2,
                ..Default::default()
            },
        );
        assert_eq!(page.datasets[0].kind, "postgres");
        let cursor = page.next_cursor.expect("3 rows, page of 2");
        let page2 = filter_datasets(
            all,
            &CatalogListFilter {
                limit: 2,
                cursor: Some(cursor),
                ..Default::default()
            },
        );
        assert_eq!(page2.datasets.len(), 1);
        assert!(page2.next_cursor.is_none());
    }

    fn edge(src: &str, dst: &str) -> CatalogLineageEdge {
        {
            let u = update(src, dst, 1);
            let src_obs = u.sources[0].clone();
            apply_edge(None, &u, &src_obs)
        }
    }

    #[test]
    fn lineage_slice_respects_root_and_depth() {
        // a → b → c → d, plus x → y off to the side.
        let edges = vec![
            edge("a", "b"),
            edge("b", "c"),
            edge("c", "d"),
            edge("x", "y"),
        ];
        let all = lineage_slice(edges.clone(), None, 5);
        assert_eq!(all.len(), 4, "no root returns everything");

        let b = dataset_id("b");
        // Depth 1 from b: edges touching b only.
        let d1 = lineage_slice(edges.clone(), Some(&b), 1);
        assert_eq!(d1.len(), 2);
        // Depth 2 from b: reaches c→d too, never x→y.
        let d2 = lineage_slice(edges.clone(), Some(&b), 2);
        assert_eq!(d2.len(), 3);
        assert!(d2.iter().all(|e| e.src_uri != "x"));
        // Unknown root: nothing.
        assert!(lineage_slice(edges, Some("nope"), 3).is_empty());
    }
}

#[cfg(test)]
mod multi_source_tests {
    use super::*;

    fn obs2(uri: &str, role: DatasetRole, records: u64) -> DatasetObservation {
        DatasetObservation {
            uri: uri.into(),
            kind: "csv".into(),
            role,
            schema: None,
            records,
        }
    }

    /// #459: a topology sink fed by a merge has several inputs. Each gets its own
    /// edge, and each edge reports **its own** source's contribution — repeating
    /// the sink total across edges would over-count the volume.
    #[test]
    fn a_merge_sink_yields_one_edge_per_input_with_its_own_volume() {
        let update = CatalogUpdate {
            run_id: "r1".into(),
            pipeline: "p".into(),
            row: "w".into(),
            recorded_at: Utc::now(),
            sources: vec![
                obs2("csv://a.csv", DatasetRole::Source, 4),
                obs2("csv://b.csv", DatasetRole::Source, 3),
            ],
            sink: obs2("jsonl://out.jsonl", DatasetRole::Sink, 7),
            column_lineage: None,
        };

        let a = apply_edge(None, &update, &update.sources[0]);
        let b = apply_edge(None, &update, &update.sources[1]);
        assert_eq!(a.src_uri, "csv://a.csv");
        assert_eq!(b.src_uri, "csv://b.csv");
        assert_eq!(a.dst_uri, "jsonl://out.jsonl");
        assert_eq!(a.dst_id, b.dst_id, "both edges land on the same sink");
        // Per-source volume, so a.last + b.last == the sink's 7 rather than 7 each.
        assert_eq!((a.last_records, b.last_records), (4, 3));
        assert_eq!(a.last_records + b.last_records, update.sink.records);
    }

    /// The single-source case is unchanged: the edge reports the sink's count,
    /// which is what every matrix pipeline records.
    #[test]
    fn a_single_source_edge_still_reports_the_sink_count() {
        let update = CatalogUpdate {
            run_id: "r1".into(),
            pipeline: "p".into(),
            row: "default".into(),
            recorded_at: Utc::now(),
            sources: vec![obs2("csv://a.csv", DatasetRole::Source, 9)],
            sink: obs2("jsonl://out.jsonl", DatasetRole::Sink, 9),
            column_lineage: None,
        };
        let e = apply_edge(None, &update, &update.sources[0]);
        assert_eq!(e.last_records, 9);
    }
}
