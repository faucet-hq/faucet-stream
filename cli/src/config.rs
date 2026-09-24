//! Parsed `pipeline.yaml` / `pipeline.json` schema (matrix-aware).
//!
//! Top-level shape:
//!
//! ```yaml
//! version: 1
//! name: optional-human-name
//! pipeline:               # required — full base config
//!   source: { type, config }
//!   sink:   { type, config }
//!   transforms: [...]
//!   state:  { type, config }
//! matrix:                 # optional — omitted or empty == one anonymous row
//!   - id: <string>
//!     parent: <id>
//!     parent_key: <jsonpath>   # default "id"
//!     source: { ... }     # partial override, deep-merged into pipeline.source
//!     sink:   { ... }
//!     transforms: [...]   # row-level transforms, appended after pipeline + source layers
//!     state:  { ... }     # if Some, replaces pipeline.state wholesale
//! execution:              # optional
//!   max_concurrent: <usize>
//!   on_error: continue|stop
//! ```
//!
//! The wire format is intentionally loose: every connector keeps its own
//! config schema, and the CLI threads a `serde_json::Value` through to the
//! connector's `serde::Deserialize` impl. That keeps this struct stable as
//! new fields are added to individual connectors without needing CLI work.

use crate::error::{CliError, CliResult};
use crate::params::ParamsSpec;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// The only `kind:` a complete pipeline config may carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum ConfigKind {
    Pipeline,
}

/// Top-level pipeline definition.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PipelineConfig {
    /// Document kind. A complete pipeline config may declare `kind: pipeline`
    /// (the registry's explicit kind for a hand-written pipeline template);
    /// `source-template` / `sink-template` documents are not pipeline configs
    /// and are composed with `faucet run --source X --sink Y` instead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<ConfigKind>,

    /// Config-format version. Currently always `1`.
    #[serde(default = "default_version")]
    pub version: u32,

    /// Optional human-readable name (used in logs and error messages).
    #[serde(default)]
    pub name: Option<String>,

    /// Optional shared constants. Resolvable as `${vars.key}` anywhere in
    /// the config (including inside named templates). Resolved at load time,
    /// after env/file/secret substitution.
    #[serde(default)]
    pub vars: Option<HashMap<String, Value>>,

    /// Optional typed run parameters (#444) — the config's trigger-time
    /// override surface. Each entry declares a name, scalar `type`,
    /// `required`/`default`, a `secret` flag, and a `description`; values are
    /// referenced as `${param.NAME}` and bound before parsing by
    /// [`crate::params::bind`] (from `--param`, `faucet template run`, or
    /// `POST /v1/templates/{id}/runs`). Because binding happens pre-parse, no
    /// `${param.*}` token ever reaches a connector. See `faucet schema params`.
    #[serde(default, skip_serializing_if = "ParamsSpec::is_empty")]
    pub params: ParamsSpec,

    /// Optional named auth providers. Each entry is a `{ type, config }` spec
    /// (the same shape as inline auth) built once and shared across every
    /// connector that references it via `auth: { ref: <name> }`. Values are kept
    /// as raw JSON so `faucet-auth` owns the per-type schema.
    #[serde(default)]
    pub auth: Option<HashMap<String, Value>>,

    /// Base pipeline — every matrix row is deep-merged into this.
    pub pipeline: PipelineSpec,

    /// Matrix of per-row overrides. Empty or omitted means "one anonymous row"
    /// (full pipeline runs once with no merge).
    #[serde(default)]
    pub matrix: Vec<MatrixRow>,

    /// Optional execution controls (concurrency, on-error policy).
    #[serde(default)]
    pub execution: Option<ExecutionSpec>,

    /// Optional matrix-row selection policy (#377). Currently carries only
    /// `include_parents` — how a selected row's `parent:` / `depends_on:`
    /// ancestors are resolved when they are not independently in the run set.
    /// Absent = the built-in default (`include_parents: off`, strict).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selection: Option<SelectionSpec>,

    /// Optional observability configuration (Prometheus + tracing).
    #[serde(default)]
    pub observability: Option<ObservabilitySpec>,

    /// Delivery guarantee for every row (overridable per matrix row). Default
    /// `at_least_once` — no behaviour change for existing configs. `exactly_once`
    /// requires an idempotent sink, a deterministic-replay source, and a state
    /// store (enforced at expand time).
    #[serde(default)]
    pub delivery: faucet_core::DeliveryMode,

    /// Optional resilience policy (retry / backoff / circuit-breaker /
    /// poison-pill). Top-level in v1 (not per-matrix-row). Absent = no behaviour
    /// change. Consumed by `faucet run`/`schedule`/`replicate`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resilience: Option<ResilienceSpec>,

    /// Optional data-freshness & volume SLA (#202). A matrix row's own `sla:`
    /// replaces it for that row (#679). Evaluated after every root
    /// invocation by `faucet run`/`schedule`/`serve`/`replicate`; violations
    /// emit metrics + warnings and never fail the run. Staleness/volume checks
    /// require a `state:` block (enforced at expand time).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sla: Option<crate::sla::SlaSpec>,

    /// Optional completeness reconciliation (#502): after a successful root run,
    /// compare rows written against an authoritative count probe and **fail the
    /// run** on a shortfall beyond tolerance — a silent-truncation guard,
    /// especially for `write_mode: overwrite`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconcile: Option<crate::reconcile::ReconcileSpec>,

    /// Optional source-shard distribution for clustered (Mode B) execution.
    /// Only consumed by `faucet serve --cluster`: a run whose source
    /// [is shardable](faucet_core::Source::is_shardable) is split into
    /// `shard.count` shards processed concurrently across cluster workers.
    /// Ignored by `faucet run` and by a non-cluster `serve`, so it is fully
    /// backward compatible.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard: Option<ShardingSpec>,

    /// `mirror:` — the snapshot→CDC handoff block. Consumed only by
    /// `faucet mirror`; ignored by `faucet run` (like `schedule:`). The
    /// pre-#670 spelling `replication:` is still accepted.
    #[serde(
        default,
        rename = "mirror",
        alias = "replication",
        skip_serializing_if = "Option::is_none"
    )]
    pub replication: Option<crate::replication::spec::ReplicationSpec>,

    /// Optional backfill defaults (window/concurrency/timezone, #282).
    /// Consumed only by `faucet backfill`; ignored by `faucet run` (like
    /// `schedule:` / `replication:`). See `faucet schema backfill`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backfill: Option<crate::backfill::BackfillSpec>,

    /// Default range partitioning applied to every root row that does not
    /// declare its own (#479). Unlike `backfill:` / `schedule:`, this IS
    /// consumed by `faucet run` — a partitioned row fans out on every run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<crate::partition::PartitionSpec>,

    /// Optional cron schedule. Only consumed by `faucet schedule`; ignored by
    /// `faucet run`. Presence makes the config runnable on a schedule.
    #[cfg(feature = "schedule")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<crate::schedule::spec::ScheduleSpec>,

    /// Optional OpenLineage emission. Consumed by `faucet run`/`schedule`/`serve`.
    #[cfg(feature = "lineage")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage: Option<faucet_lineage::LineageConfig>,

    /// Optional Data Movement Catalog store (#279): where `faucet run` /
    /// `schedule` / `replicate` accumulate the cross-run dataset catalog
    /// (identity, schema timeline, volume/freshness, lineage edges). `faucet
    /// serve` ignores this block and records into its `--history` backend.
    /// Recording never fails a run. See `faucet schema catalog`.
    #[cfg(feature = "catalog")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog: Option<crate::catalog::CatalogSpec>,

    /// Optional notification / incident-routing rules (#280). Fan pipeline
    /// lifecycle and health events (run failure/success, SLA breach, circuit
    /// open, contract abort, DLQ threshold, scheduler stuck) out to Slack /
    /// PagerDuty / a signed webhook. Consumed by every runtime
    /// (`run`/`schedule`/`serve`/`replicate`); delivery never fails a run.
    #[cfg(feature = "notify")]
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notifications: Vec<crate::notify::NotificationSpec>,

    /// Optional `_faucet_*` run/lineage metadata columns (#510) stamped onto
    /// every row by a connector-agnostic sink decorator (works for any sink).
    /// Off unless present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_columns: Option<faucet_core::MetadataColumnsSpec>,

    /// Optional retention policy for the **local files** this pipeline's sinks
    /// write (#587): how long they are kept before the GC reclaims them, and
    /// whether faucet records them at all. Without the block, outputs are
    /// tracked and inherit the runtime's default window (7 days). Recording
    /// needs a store — `faucet serve`'s `--history`, or the `catalog:` block for
    /// the one-shot runtimes — and is inert (with a warning) without one.
    /// See `faucet schema local-outputs`.
    #[cfg(feature = "catalog")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_outputs: Option<crate::local_outputs::LocalOutputsSpec>,
}

/// The base pipeline definition. Each matrix row is resolved against the
/// template catalogs below; the singular `source` / `sink` fields are the
/// legacy way to declare a single template (internally named `default`).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PipelineSpec {
    /// Legacy singular source — registers as a template named `default`.
    /// Defining both `source` and `sources.default` is an error at expand time.
    #[serde(default)]
    pub source: Option<ConnectorSpec>,

    /// Legacy singular sink — registers as a template named `default`.
    #[serde(default)]
    pub sink: Option<ConnectorSpec>,

    /// Named source templates. A matrix row picks one via `source.ref: NAME`.
    #[serde(default)]
    pub sources: HashMap<String, ConnectorSpec>,

    /// Named sink templates. A matrix row picks one via `sink.ref: NAME`.
    #[serde(default)]
    pub sinks: HashMap<String, ConnectorSpec>,

    #[serde(default)]
    pub transforms: Vec<TransformSpec>,
    #[serde(default)]
    pub state: Option<StateStoreSpec>,
    #[serde(default)]
    pub dlq: Option<DlqSpec>,

    /// Data-quality checks (pipeline-level; no matrix-row override in v1).
    #[cfg(feature = "quality")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quality: Option<faucet_core::QualitySpec>,

    /// Data contract (pipeline-level; no matrix-row override in v1): a
    /// versioned output schema/constraint promise enforced per page after
    /// transforms and quality checks (#204). See `faucet schema contract`.
    #[cfg(feature = "contract")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contract: Option<faucet_core::ContractSpec>,

    /// PII detection + column-level masking policy (pipeline-level; no
    /// matrix-row override in v1): classify sensitive fields (by name pattern,
    /// value detector, or explicit list) and redact/hash/tokenize/partial-mask
    /// them per page — before every sink write, the DLQ, and lineage sampling
    /// (#206). Rules can be scoped per destination sink via `applies_to`. See
    /// `faucet schema masking` and `faucet masking`.
    #[cfg(feature = "masking")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub masking: Option<faucet_core::MaskingSpec>,

    /// Schema-drift handling policy (pipeline-level; no matrix-row override in v1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<faucet_core::SchemaDriftSpec>,

    /// Topology-mode nodes (issue #71): an explicit graph of typed nodes
    /// (`source` / `transform` / `tee` / `merge` / `join` / `sink`) keyed by
    /// node id. When non-empty this pipeline runs in **topology mode** and the
    /// top-level `matrix:` block must be empty (they are mutually exclusive).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub nodes: HashMap<String, NodeSpec>,

    /// Topology-mode edges connecting `nodes` (issue #71). Each edge names a
    /// producer (`from`) and consumer (`to`); a join's incoming edges also
    /// carry an `as:` label matching the join's `build`/`probe` edge names.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub edges: Vec<EdgeSpec>,
}

/// A single topology node (issue #71). The `kind` discriminator selects the
/// node type; the remaining fields are kind-specific.
///
/// `source` / `sink` nodes pick a template with `ref:` (defaulting to
/// `default`) and may override `type` / `config` inline — the same shape as a
/// matrix row's [`PartialConnector`]. `transform` carries a `transforms:`
/// list. `tee` carries `channel_capacity` + optional `fanout`. `merge` has no
/// extra fields. `join` carries the hash-join configuration.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum NodeSpec {
    /// A data source (0 in, 1 out).
    Source {
        /// Template name in `pipeline.sources` (defaults to `default`).
        #[serde(rename = "ref", default, skip_serializing_if = "Option::is_none")]
        template: Option<String>,
        /// Inline connector-type override.
        #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
        kind: Option<String>,
        /// Inline config override (deep-merged onto the resolved template).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        config: Option<Value>,
    },
    /// A data sink (1 in, 0 out).
    Sink {
        /// Template name in `pipeline.sinks` (defaults to `default`).
        #[serde(rename = "ref", default, skip_serializing_if = "Option::is_none")]
        template: Option<String>,
        /// Inline connector-type override.
        #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
        kind: Option<String>,
        /// Inline config override (deep-merged onto the resolved template).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        config: Option<Value>,
    },
    /// Transform stages applied per page (1 in, 1 out).
    Transform {
        /// Transform stages, in order.
        #[serde(default)]
        transforms: Vec<TransformSpec>,
    },
    /// Fan-out: clone each page to every downstream edge (1 in, N out).
    Tee {
        /// Bounded-channel capacity for each outgoing edge.
        #[serde(default = "default_channel_capacity")]
        channel_capacity: usize,
        /// Optional expected fan-out (outgoing edge count) sanity check.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        fanout: Option<usize>,
    },
    /// Fan-in: forward pages from all inputs in arrival order (N in, 1 out).
    Merge,
    /// Hash-join two upstreams by key (2 in, 1 out).
    Join(JoinSpec),
}

/// The `join:` node configuration (issue #72).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JoinSpec {
    /// `inner` (drop non-matches) or `left` (keep non-matches).
    #[serde(default)]
    pub mode: faucet_core::JoinMode,
    /// The build (right) side — fully buffered as a lookup index.
    pub build: JoinSide,
    /// The probe (left) side — streamed and enriched.
    pub probe: JoinSide,
    /// Fields to copy from the matched build record onto the probe record.
    #[serde(default)]
    pub project: Vec<faucet_core::Projection>,
    /// Value used to fill projected fields on a `left`-mode non-match.
    #[serde(default)]
    pub on_missing: Value,
    /// Multi-match policy: `first` or `cartesian`.
    #[serde(default)]
    pub on_duplicate: faucet_core::OnDuplicate,
    /// Projection-collision policy: `overwrite` / `skip` / `error`.
    #[serde(default)]
    pub on_collision: faucet_core::OnCollision,
    /// Key normalization: `preserve` (no coercion) or `stringify`.
    #[serde(default)]
    pub key_normalize: faucet_core::KeyNormalize,
    /// Safety cap on build-side records.
    #[serde(default = "default_max_build_records")]
    pub max_build_records: usize,
}

/// One side of a [`JoinSpec`]: the incoming edge label plus the dotted key path.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JoinSide {
    /// Name of the incoming edge (matches an `edges[].as` label).
    pub edge: String,
    /// Dotted path to the join key inside each record on this side.
    pub key: String,
}

/// A topology edge (issue #71).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EdgeSpec {
    /// Producer node id.
    pub from: String,
    /// Consumer node id.
    pub to: String,
    /// Optional edge label (required on a join's two incoming edges).
    #[serde(rename = "as", default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

fn default_channel_capacity() -> usize {
    faucet_core::topology::DEFAULT_CHANNEL_CAPACITY
}

fn default_max_build_records() -> usize {
    faucet_core::join::DEFAULT_MAX_BUILD_RECORDS
}

/// A `{ type, config }` block, the universal shape for both sources and sinks.
///
/// Source templates may additionally carry `transforms:` and
/// `inherit_transforms:`. Both are rejected on sink templates at expand time.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ConnectorSpec {
    /// Connector type — matches the suffix of the underlying crate
    /// (e.g. `rest` for `faucet-source-rest`).
    #[serde(rename = "type")]
    pub kind: String,

    /// Connector-specific config object. Passed through verbatim to the
    /// connector's `serde::Deserialize` impl.
    #[serde(default = "empty_object")]
    pub config: Value,

    /// Transforms bound to this source template. Applied after `T_pipeline`
    /// and before `T_row` for every matrix row that resolves to this template.
    /// Rejected at expand time when this `ConnectorSpec` is used as a sink.
    #[serde(default)]
    pub transforms: Option<Vec<TransformSpec>>,

    /// When `false`, drops upstream `T_pipeline` transforms for every matrix
    /// row that resolves to this source template. Default `true`. Rejected
    /// at expand time on sinks.
    #[serde(default = "default_true")]
    pub inherit_transforms: bool,

    /// Readiness ladder for the *source* side of a row (#371). Governs whether
    /// a row runs by default. Deep-merges from template → row `source` override
    /// like any scalar. `None` resolves to the built-in default (`active`).
    /// Meaningful only on source templates; ignored on sinks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<SourceStatus>,

    /// Free-form classification tags for the *source* template (#376).
    /// Union-merged (not replaced) with a matrix row's own `tags:` to form the
    /// row's effective tag set. Meaningful only on source templates; ignored on
    /// sinks. Validated at expand time (charset `^[a-z0-9][a-z0-9_-]*$`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,

    /// **Completeness claim** for scoped cleanup (#478). Meaningful only on
    /// source templates; rejected at expand time on sinks — only a source knows
    /// whether a fetch returned every record for a scope.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub complete_for: Option<CompletenessClaim>,
}

/// "For these column values, this fetch returns *all* the records" (#478).
///
/// Declared on the source because a sink sees a page and cannot tell a complete
/// set from page 1 of 3. The claim alone never deletes anything: `on_missing`
/// defaults to `ignore`, so acting on it is a separate, explicit opt-in.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CompletenessClaim {
    /// The scope this fetch is authoritative for, keyed by **destination**
    /// column names — the `DELETE` runs against destination columns, and a
    /// transform chain may rename fields between source and sink. Values may
    /// carry `${parent.path}` / `${now.*}` tokens, resolved per invocation like
    /// any other config value.
    pub scope: std::collections::BTreeMap<String, Value>,

    /// What to do about destination rows inside `scope` that this run did not
    /// write. Defaults to `ignore`, so adding a claim can never start deleting
    /// data on its own.
    #[serde(default)]
    pub on_missing: OnMissing,
}

/// Action for destination rows inside a completeness scope that a run did not
/// write (#478).
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OnMissing {
    /// Leave them alone. The claim is recorded but inert (default).
    #[default]
    Ignore,
    /// Delete them — the only way an incremental sync can remove records that
    /// were deleted at the source.
    Delete,
}

/// A partial connector override carried by a matrix row. Both `type` and
/// `config` are optional so rows can swap the kind, override only the inner
/// config, or both. `ref:` (optional) picks which named template under
/// `pipeline.sources` / `pipeline.sinks` this row instantiates; when absent,
/// the row inherits the legacy singular `pipeline.source` / `pipeline.sink`
/// (registered internally as a template named `default`).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PartialConnector {
    /// Name of the template under `pipeline.sources` / `pipeline.sinks` to
    /// instantiate. `None` falls back to the `default` template.
    #[serde(default)]
    pub r#ref: Option<String>,
    /// Override the connector kind (otherwise inherits from the template).
    #[serde(rename = "type", default)]
    pub kind: Option<String>,
    /// Partial config object — deep-merged into the resolved template's config.
    #[serde(default)]
    pub config: Option<Value>,
    /// Per-row readiness override for the source (#371). When `Some`, replaces
    /// the template's `status` (scalar deep-merge). Only meaningful on a row's
    /// `source:` override; a `sink:` override's `status` is ignored.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<SourceStatus>,
}

/// A single transform declaration.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TransformSpec {
    /// Built-in transform identifier. One of: `flatten`, `rename_keys`,
    /// `snake_case`, `select`, `drop`, `set`, `rename_field`, `cast`, `redact`,
    /// `value_case`. See the docs-site cookbook page on transforms for
    /// per-transform config schemas.
    #[serde(rename = "type")]
    pub kind: String,

    /// Transform-specific config object (e.g. `{ separator: "__" }` for flatten).
    #[serde(default = "empty_object")]
    pub config: Value,
}

/// State-store backend selector.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StateStoreSpec {
    /// Store type: `file`, `memory`, `redis`, or `postgres`.
    #[serde(rename = "type")]
    pub kind: String,

    /// Store-specific config.
    #[serde(default = "empty_object")]
    pub config: Value,
}

/// One row of the `matrix:` block.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MatrixRow {
    /// Row identifier. Required for parent/child references and runtime
    /// `${id.path}` interpolation. Anonymous rows get a synthetic `row-N` id.
    #[serde(default)]
    pub id: Option<String>,

    /// If set, this row runs once per record produced by the named parent row.
    #[serde(default)]
    pub parent: Option<String>,

    /// Relative cost hint used by `execution.schedule: lpt` (#644) — higher is
    /// dispatched earlier. Ignored under the default `declared` order.
    ///
    /// The useful proxy is **bytes moved**, not rows: a 970k-row × 3-column
    /// table is lighter than a 122k-row × 100-column one, so ranking on row
    /// count alone mis-orders wide-but-short objects. `faucet discover` fills
    /// this in from each dataset's estimated rows and column widths; set it by
    /// hand when you know better.
    #[serde(default)]
    pub weight: Option<f64>,

    /// Row ids this row waits for. The row starts only after every listed
    /// row (all of its invocations) finishes successfully; a failed or
    /// skipped dependency skips this row. Pure completion-ordering — unlike
    /// `parent:`, no records are consumed and no per-record fan-out occurs.
    #[serde(default)]
    pub depends_on: Vec<String>,

    /// Dotted field path inside each parent record that uniquely identifies
    /// the record. Used as the state-key suffix. Default: `id`.
    #[serde(default = "default_parent_key")]
    pub parent_key: String,

    /// Partial override of `pipeline.source` (deep-merged).
    #[serde(default)]
    pub source: Option<PartialConnector>,

    /// Partial override of `pipeline.sink` (deep-merged).
    #[serde(default)]
    pub sink: Option<PartialConnector>,

    /// Row-level transforms. Appended after `T_pipeline` and `T_source`
    /// (unless `inherit_transforms` is `false` here, in which case both
    /// upstream layers are dropped). `None` or empty list contributes
    /// nothing.
    #[serde(default)]
    pub transforms: Option<Vec<TransformSpec>>,

    /// When `false`, drops upstream `T_pipeline` and `T_source` transforms
    /// for this row. Default `true`.
    #[serde(default = "default_true")]
    pub inherit_transforms: bool,

    /// If `Some`, replaces `pipeline.state` wholesale.
    #[serde(default)]
    pub state: Option<StateStoreSpec>,

    /// Matrix-row override semantics:
    /// - field absent  → `None`     — inherit from `pipeline.dlq`
    /// - `dlq: null`   → `Some(None)` — disable DLQ for this row
    /// - `dlq: { ... }` → `Some(Some(spec))` — replace base DLQ wholesale
    #[serde(default, deserialize_with = "deserialize_dlq_override")]
    pub dlq: Option<Option<DlqSpec>>,

    /// Per-row delivery override. `None` inherits the top-level `delivery`.
    #[serde(default)]
    pub delivery: Option<faucet_core::DeliveryMode>,

    /// Per-row SLA override (#679). Replaces the top-level `sla:` for this
    /// row's invocations; `None` inherits it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sla: Option<crate::sla::SlaSpec>,

    /// Free-form classification tags for this row (#376). Orthogonal to the
    /// source's `status:` readiness ladder — `status` gates whether a row is
    /// eligible; `tags` narrow *within* the eligible set at runtime via
    /// `--tag`. Union-merged with the source template's `tags:` to form the
    /// effective tag set. Validated at expand time (charset
    /// `^[a-z0-9][a-z0-9_-]*$`, deduped).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,

    /// Split this row into N independent invocations over a chunked range
    /// (#479), each scoped by `${partition.*}` tokens substituted into the
    /// connector configs. Inherits the top-level `partition:` block when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<crate::partition::PartitionSpec>,

    /// **Fan-out axis** (#501), spelled `fan_out:` on the wire. When set, this
    /// row enumerates a value-set at runtime: it builds `fan_out.source`,
    /// drains it, projects `select` (JSONPath) from each record, and dedups
    /// (first-seen order). The row has no sink and writes nothing — its
    /// value-set is exposed to `for_each` rows as `${<id>.<as>}`. Runs once
    /// per pipeline run, cached across dependents.
    ///
    /// The historical spelling `discover:` is accepted as an alias (#654 M21).
    /// It collided with the other, unrelated `discover` in the vocabulary —
    /// `Source::discover`, which enumerates a connection's **datasets** — and
    /// the canonical verb for "turn a listed set into one matrix row per
    /// member" is `fan_out` (see the Vocabulary table in
    /// `.claude/rules/architecture.md`).
    #[serde(
        default,
        rename = "fan_out",
        alias = "discover",
        skip_serializing_if = "Option::is_none"
    )]
    pub discover: Option<DiscoverSpec>,

    /// Fan this row out over the **cartesian product** of the named discovery
    /// dimensions (#501) — one invocation per tuple, with `${<dim>.<as>}`
    /// resolved to that tuple's value in the source/sink config. Distinct from
    /// `parent:` (fan-out over one real pipeline's emitted records — a tree, not
    /// a product) and `depends_on:` (completion ordering only). Each id must name
    /// a `discover:` row; the product is bounded by `MAX_MATRIX_PRODUCT`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub for_each: Vec<String>,
}

/// A discovery dimension's source + projection (#501). See [`MatrixRow::discover`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DiscoverSpec {
    /// Source to enumerate. Either a `{ ref: <name> }` pointing at a
    /// `pipeline.sources` template (recommended — reuses a complete connector
    /// config), or a standalone `{ type, config }`. It is **not** merged over
    /// the `default` template — a discovery step is independent of the
    /// pipeline's data source (which typically carries `${dim}` tokens meant
    /// for the fan-out rows, not the enumeration).
    pub source: PartialConnector,

    /// JSONPath projecting the dimension value from each discovered record
    /// (e.g. `$.id`). `$` selects the whole record. Null / missing values are
    /// skipped with a warning.
    pub select: String,

    /// Alias the projected value is exposed under: a `for_each` row references
    /// it as `${<row_id>.<as>}`. Must match `^[a-z0-9][a-z0-9_-]*$`.
    #[serde(rename = "as")]
    pub as_alias: String,

    /// **Collected (list-valued) dimension** (#531). When `true`, the whole
    /// deduped value-set is published as a single list per upstream tuple rather
    /// than as an independent cartesian axis — so a consuming row injects the
    /// entire list into one request (`${<id>.<as>}` renders it comma-joined,
    /// e.g. HubSpot's `?properties=a,b,c`). Required when a `discover:` row also
    /// declares `for_each:` (chained / two-level discovery), and only meaningful
    /// on a discovery row.
    #[serde(default)]
    pub collect: bool,
}

/// Readiness ladder for a matrix row's source (#371). A single ordered axis
/// answering "when does this row run?". Default (when absent) is [`Active`].
///
/// [`Active`]: SourceStatus::Active
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum SourceStatus {
    /// Always runs; cannot be narrowed out except by an explicit `--skip <id>`.
    Mandatory,
    /// Runs by default (bare `faucet run`). The absent-default.
    #[default]
    Active,
    /// Ready, but opt-in: runs only when requested via `--status available`.
    Available,
    /// Work-in-progress: runs only under `--status draft`.
    Draft,
    /// Retired, kept for history: runs only under `--status archived`.
    Archived,
}

impl SourceStatus {
    /// Every variant, in ladder order — used to render error listings.
    pub const ALL: [SourceStatus; 5] = [
        SourceStatus::Mandatory,
        SourceStatus::Active,
        SourceStatus::Available,
        SourceStatus::Draft,
        SourceStatus::Archived,
    ];

    /// The snake_case wire name (matches the serde discriminator).
    pub fn as_str(self) -> &'static str {
        match self {
            SourceStatus::Mandatory => "mandatory",
            SourceStatus::Active => "active",
            SourceStatus::Available => "available",
            SourceStatus::Draft => "draft",
            SourceStatus::Archived => "archived",
        }
    }

    /// Parse a `--status` token (or a `status:` value). `None` on an unknown
    /// value; callers surface a typed error listing [`SourceStatus::ALL`].
    pub fn parse(s: &str) -> Option<Self> {
        SourceStatus::ALL.into_iter().find(|v| v.as_str() == s)
    }

    /// Whether this tier is in the default (bare-`run`) eligible set.
    pub fn default_eligible(self) -> bool {
        matches!(self, SourceStatus::Mandatory | SourceStatus::Active)
    }
}

/// Policy for pulling a selected row's `parent:` / `depends_on:` ancestors into
/// the run set when they are not independently selected (#377). The **only**
/// mechanism that decides ancestor inclusion. Default [`Off`] (strict).
///
/// [`Off`]: IncludeParents::Off
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum IncludeParents {
    /// Include nothing automatically; a required ancestor missing from the run
    /// set is a hard, fail-fast error.
    #[default]
    Off,
    /// Include required ancestors whose resolved status is eligible; error if a
    /// required ancestor is parked (`available`/`draft`/`archived`).
    Eligible,
    /// Include every required ancestor regardless of status (parked ancestors
    /// pulled in too, with a warning); never errors on ancestors.
    All,
}

impl IncludeParents {
    /// The snake_case wire name.
    pub fn as_str(self) -> &'static str {
        match self {
            IncludeParents::Off => "off",
            IncludeParents::Eligible => "eligible",
            IncludeParents::All => "all",
        }
    }

    /// Parse a `--include-parents` token. `None` on an unknown value.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "off" => Some(IncludeParents::Off),
            "eligible" => Some(IncludeParents::Eligible),
            "all" => Some(IncludeParents::All),
            _ => None,
        }
    }
}

/// Matrix-row selection policy (#377). Currently a single knob:
/// `include_parents`. Extensible for future selection-model settings.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SelectionSpec {
    /// How parent/dependency ancestors are resolved for a narrowed run set.
    #[serde(default)]
    pub include_parents: IncludeParents,
}

/// Execution-time controls.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExecutionSpec {
    /// Maximum concurrent pipeline invocations (root + per-parent-record
    /// child invocations all share this budget). Defaults to
    /// `num_cpus::get().min(4)` at runtime when `None`. `0` = **no limit**
    /// (the house `0`-sentinel, like `batch_size: 0`) — every invocation runs
    /// in parallel; before 1.7 a `0` here clamped to fully-serial instead, so
    /// audit any config that relied on that.
    #[serde(default)]
    pub max_concurrent: Option<usize>,

    /// What to do when a pipeline invocation fails.
    #[serde(default)]
    pub on_error: OnError,

    /// Adaptive batch-size controller (opt-in). See `faucet_core::AdaptiveBatchConfig`.
    #[serde(default)]
    pub adaptive_batch_size: Option<faucet_core::AdaptiveBatchConfig>,

    /// Order in which ready sibling rows queue for a concurrency permit
    /// (#644). Defaults to `declared` — today's behaviour, byte for byte.
    #[serde(default)]
    pub schedule: DispatchOrder,
}

/// How the executor orders ready sibling rows competing for permits (#644).
///
/// Only the *enqueue order* changes; the permit budget, the `JoinSet`, and the
/// `on_error` cancellation are untouched.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DispatchOrder {
    /// Matrix-declaration order (BFS). The default, and fully deterministic.
    #[default]
    Declared,
    /// Longest-processing-time-first: heaviest rows claim permits first.
    ///
    /// With heterogeneous row durations, makespan otherwise depends on the
    /// order the user happened to list rows in — a large object listed late
    /// becomes an idle tail (measured: two big Salesforce objects finishing
    /// ~9 min after the other 19, with 6 of 8 slots idle). LPT is provably
    /// within 4/3 of optimal for `P || Cmax`, and largest-*last* — which plain
    /// declaration order can accidentally produce — is the worst case.
    ///
    /// Rows are ranked by [`MatrixRow::weight`]; ties and missing weights fall
    /// back to declaration order, so dispatch is never nondeterministic.
    Lpt,
}

/// Source-shard distribution settings for clustered (Mode B) execution.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ShardingSpec {
    /// Target number of shards to split the source into. Must be `>= 2` (a
    /// count of 1 means "don't shard" — omit the block instead). The actual
    /// shard count may be smaller when the source has fewer natural partitions
    /// (e.g. a key range narrower than `count`).
    pub count: usize,
}

/// Failure-handling policy across the matrix.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum OnError {
    /// Skip the failed invocation's subtree but keep running siblings (default).
    #[default]
    Continue,
    /// Cancel every pending and in-flight invocation on first failure.
    Stop,
}

/// Top-level observability block: Prometheus scrape endpoint and tracing level.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ObservabilitySpec {
    /// Prometheus metrics scrape endpoint configuration.
    #[serde(default)]
    pub prometheus: Option<PrometheusSpec>,

    /// Tracing / logging configuration.
    #[serde(default)]
    pub tracing: Option<TracingSpec>,

    /// OTLP (OpenTelemetry) export configuration (#201).
    #[serde(default)]
    pub otel: Option<OtelSpec>,
}

/// Configuration for the Prometheus metrics HTTP endpoint.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PrometheusSpec {
    /// Socket address to bind the scrape endpoint on (e.g. `"127.0.0.1:9464"`).
    pub listen: String,

    /// Custom histogram bucket boundaries. Falls back to the Prometheus default
    /// buckets when `None`.
    #[serde(default)]
    pub buckets: Option<Vec<f64>>,
}

/// Tracing / log-level configuration.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TracingSpec {
    /// `tracing-subscriber` filter directive (e.g. `"info"`, `"debug"`,
    /// `"faucet=trace"`). Defaults to the value of `RUST_LOG` when `None`.
    #[serde(default)]
    pub level: Option<String>,
}

/// OTLP export block under `observability.otel:`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OtelSpec {
    /// Collector endpoint URL. When empty, defaults to the protocol-specific
    /// localhost address (`http://localhost:4317` for gRPC, `:4318` for HTTP).
    #[serde(default)]
    pub endpoint: String,
    /// OTLP transport protocol (`grpc` or `http`). Default: `grpc`.
    #[serde(default)]
    pub protocol: faucet_core::OtelProtocol,
    /// Extra headers sent on every export request (e.g. backend auth tokens).
    #[serde(default)]
    pub headers: std::collections::HashMap<String, String>,
    /// Head-based trace sampling ratio, 0.0..=1.0. Default: 1.0 (sample all).
    #[serde(default = "default_otel_ratio")]
    pub sample_ratio: f64,
    /// Which signals to export. Default: `[traces, metrics]`.
    #[serde(default = "default_otel_export")]
    pub export: Vec<faucet_core::OtelSignal>,
    /// OTel resource `service.name`. Default: `"faucet"`.
    #[serde(default = "default_otel_service")]
    pub service_name: String,
    /// Per-export timeout in seconds. Default: 10.
    #[serde(default = "default_otel_timeout")]
    pub timeout_secs: u64,
    /// Metric push interval in seconds. Default: 60.
    #[serde(default = "default_otel_interval")]
    pub metric_interval_secs: u64,
}

fn default_otel_ratio() -> f64 {
    1.0
}
fn default_otel_export() -> Vec<faucet_core::OtelSignal> {
    vec![
        faucet_core::OtelSignal::Traces,
        faucet_core::OtelSignal::Metrics,
    ]
}
fn default_otel_service() -> String {
    "faucet".to_string()
}
fn default_otel_timeout() -> u64 {
    10
}
fn default_otel_interval() -> u64 {
    60
}

impl OtelSpec {
    /// Convert to the core config and validate ranges/URL.
    pub fn to_core(&self) -> Result<faucet_core::OtelConfig, String> {
        let cfg = faucet_core::OtelConfig {
            endpoint: self.endpoint.clone(),
            protocol: self.protocol,
            headers: self.headers.clone(),
            sample_ratio: self.sample_ratio,
            export: self.export.clone(),
            service_name: self.service_name.clone(),
            timeout_secs: self.timeout_secs,
            metric_interval_secs: self.metric_interval_secs,
        };
        cfg.validate()?;
        Ok(cfg)
    }
}

/// Mirrors `faucet_core::OnBatchError` but with `JsonSchema` derived and
/// `Deserialize` accepting the YAML/JSON shape. Converted to the core
/// type during `executor::build_dlq_config`.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OnBatchErrorSpec {
    #[default]
    Propagate,
    DlqAll,
}

/// DLQ configuration block under `pipeline.dlq:`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DlqSpec {
    pub sink: ConnectorSpec,
    #[serde(default)]
    pub on_batch_error: OnBatchErrorSpec,
    #[serde(default)]
    pub max_failures_per_page: Option<usize>,
    #[serde(default)]
    pub max_failures_total: Option<usize>,
    #[serde(default = "default_true")]
    pub include_original_payload: bool,
}

/// User-facing `resilience:` config. Maps to `faucet_core::ResiliencePolicy`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResilienceSpec {
    /// Retry/backoff applied to sink-write, flush, and state-store I/O (and
    /// injected into rest/xml/graphql sources).
    #[serde(default)]
    pub retry: RetrySpec,
    /// Which error classes are retried. Omitted = all four transient classes.
    #[serde(default)]
    pub retry_on: Option<Vec<faucet_core::RetryClass>>,
    /// Optional circuit breaker.
    #[serde(default)]
    pub circuit_breaker: Option<CircuitBreakerSpec>,
    /// Optional poison-pill (per-row) handling (DLQ path only).
    #[serde(default)]
    pub poison: Option<PoisonSpec>,
}

/// Retry/backoff tuning.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RetrySpec {
    /// Total attempts including the first (1 = no retry).
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    /// Backoff growth shape.
    #[serde(default)]
    pub backoff: BackoffSpec,
    /// Base delay in milliseconds.
    #[serde(default = "default_base_ms")]
    pub base_ms: u64,
    /// Per-sleep cap in milliseconds (pre-jitter).
    #[serde(default = "default_max_ms")]
    pub max_ms: u64,
    /// Whether to apply `[0.5, 1.5)` jitter.
    #[serde(default = "default_true")]
    pub jitter: bool,
}

impl Default for RetrySpec {
    fn default() -> Self {
        Self {
            max_attempts: default_max_attempts(),
            backoff: BackoffSpec::default(),
            base_ms: default_base_ms(),
            max_ms: default_max_ms(),
            jitter: true,
        }
    }
}

/// Backoff growth shape (config spelling of `faucet_core::BackoffKind`).
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum BackoffSpec {
    /// No delay between attempts.
    None,
    /// Constant `base_ms` delay.
    Fixed,
    /// `base_ms * 2^attempt`, capped at `max_ms`.
    #[default]
    Exponential,
}

/// Circuit-breaker tuning.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CircuitBreakerSpec {
    /// Consecutive exhausted-retry page failures before the circuit opens.
    pub consecutive_failures: u32,
    /// Re-entry cooldown in seconds (honored by the orchestration layer).
    pub cooldown_secs: u64,
}

/// Poison-pill (per-row) policy.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PoisonSpec {
    /// Per-row write attempts before applying `action`.
    pub max_row_attempts: u32,
    /// Terminal action for a persistently failing row.
    #[serde(default)]
    pub action: PoisonActionSpec,
}

/// Terminal action for a poison row (config spelling of
/// `faucet_core::PoisonAction`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PoisonActionSpec {
    /// Route to the DLQ (requires a `dlq:` block).
    #[default]
    Dlq,
    /// Discard the row.
    Drop,
    /// Propagate the row error and abort the run.
    Fail,
}

fn default_max_attempts() -> u32 {
    5
}
fn default_base_ms() -> u64 {
    200
}
fn default_max_ms() -> u64 {
    30_000
}

impl ResilienceSpec {
    /// Validate and build the core policy. Fail-fast on bad config.
    pub fn to_policy(&self) -> Result<faucet_core::ResiliencePolicy, crate::error::CliError> {
        use crate::error::CliError;
        if self.retry.max_attempts < 1 {
            return Err(CliError::Config(
                "resilience.retry.max_attempts must be >= 1".into(),
            ));
        }
        if self.retry.base_ms > self.retry.max_ms {
            return Err(CliError::Config(
                "resilience.retry.base_ms must be <= max_ms".into(),
            ));
        }
        let retry_on = match &self.retry_on {
            Some(v) if v.is_empty() => {
                return Err(CliError::Config(
                    "resilience.retry_on must not be empty".into(),
                ));
            }
            Some(v) => faucet_core::RetryClassSet::from_iter(v.iter().copied()),
            None => faucet_core::RetryClassSet::default(),
        };
        let backoff = match self.retry.backoff {
            BackoffSpec::None => faucet_core::BackoffKind::None,
            BackoffSpec::Fixed => faucet_core::BackoffKind::Fixed,
            BackoffSpec::Exponential => faucet_core::BackoffKind::Exponential,
        };
        let circuit_breaker = match self.circuit_breaker {
            Some(cb) if cb.consecutive_failures < 1 => {
                return Err(CliError::Config(
                    "resilience.circuit_breaker.consecutive_failures must be >= 1".into(),
                ));
            }
            Some(cb) => Some(faucet_core::CircuitBreakerConfig {
                consecutive_failures: cb.consecutive_failures,
                cooldown: std::time::Duration::from_secs(cb.cooldown_secs),
            }),
            None => None,
        };
        let poison = match self.poison {
            Some(p) if p.max_row_attempts < 1 => {
                return Err(CliError::Config(
                    "resilience.poison.max_row_attempts must be >= 1".into(),
                ));
            }
            Some(p) => Some(faucet_core::PoisonPolicy {
                max_row_attempts: p.max_row_attempts,
                action: match p.action {
                    PoisonActionSpec::Dlq => faucet_core::PoisonAction::Dlq,
                    PoisonActionSpec::Drop => faucet_core::PoisonAction::Drop,
                    PoisonActionSpec::Fail => faucet_core::PoisonAction::Fail,
                },
            }),
            None => None,
        };
        Ok(faucet_core::ResiliencePolicy {
            retry: faucet_core::RetryPolicy {
                max_attempts: self.retry.max_attempts,
                backoff,
                base: std::time::Duration::from_millis(self.retry.base_ms),
                max: std::time::Duration::from_millis(self.retry.max_ms),
                jitter: self.retry.jitter,
                retry_on,
            },
            circuit_breaker,
            poison,
        })
    }
}

fn default_true() -> bool {
    true
}

fn default_version() -> u32 {
    1
}
fn default_parent_key() -> String {
    "id".to_owned()
}
fn empty_object() -> Value {
    Value::Object(Default::default())
}

fn deserialize_dlq_override<'de, D>(deserializer: D) -> Result<Option<Option<DlqSpec>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<DlqSpec>::deserialize(deserializer).map(Some)
}

/// Resolve load-time `${env:}` / `${file:}` / `${secret:}` directives in a
/// composed config document **after parsing** rather than on the raw text.
///
/// The document is parsed into an untyped value (by file extension), each string
/// scalar is interpolated in place via
/// [`interpolate_value_with_env`](crate::interpolate::interpolate_value_with_env)
/// (and `${param.*}` bound by [`crate::params::bind_document`]), and the tree is
/// re-serialised back into the same format for the typed parse that follows.
/// Resolving post-parse means a resolved value can never inject or break the
/// document's structure (F43) — an env/file value containing `:`, a newline, or
/// `-` stays the single scalar it was parsed as. Re-serialising and letting
/// [`PipelineConfig::from_text`] re-parse keeps the typed-deserialise error
/// messages (unknown field, type mismatch, version gate) identical to the old
/// path. A syntax error in the document surfaces here, mapped exactly as
/// `from_text` would map it.
/// Caller-supplied inputs for one load: `params:` values and an `${env:}`
/// overlay (#444). Both are empty by default, which is exactly the pre-#444
/// behaviour — a config with no `params:` block, or one whose params all carry
/// defaults, loads unchanged.
#[derive(Debug, Clone)]
pub struct RunInputs {
    /// Values for the config's declared `params:` (from `--param`, an HTTP
    /// `params` object, or a template trigger).
    pub params: crate::params::SuppliedParams,
    /// Values that win over the process environment for `${env:VAR}` /
    /// `${secret:VAR}` during this load only.
    pub env: crate::interpolate::EnvOverlay,
    /// What to do with a `required` param the caller did not supply.
    pub mode: crate::params::BindMode,
}

impl Default for RunInputs {
    fn default() -> Self {
        Self {
            params: Default::default(),
            env: Default::default(),
            mode: crate::params::BindMode::Strict,
        }
    }
}

impl RunInputs {
    /// Inputs that fill unsupplied required params with type-shaped
    /// placeholders — for structural validation of a parameterized config.
    pub fn placeholders() -> Self {
        Self {
            mode: crate::params::BindMode::Placeholder,
            ..Self::default()
        }
    }

    /// Inputs carrying just a supplied-param map.
    pub fn with_params(params: crate::params::SuppliedParams) -> Self {
        Self {
            params,
            ..Self::default()
        }
    }
}

fn resolve_document(text: &str, path: &Path, inputs: &RunInputs) -> CliResult<String> {
    use crate::interpolate::interpolate_value_with_env;
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(str::to_ascii_lowercase);
    // Shared per-scalar resolution: env/file/secret first (so a param `default:
    // "${env:X}"` resolves), then `${param.*}` binding. Deliberately in that
    // order — a *supplied* param value must never be re-scanned for directives
    // (see `params::bind`).
    let resolve = |value: &mut serde_json::Value| -> CliResult<()> {
        interpolate_value_with_env(value, &inputs.env)?;
        crate::params::bind_document(value, &inputs.params, inputs.mode)?;
        Ok(())
    };
    match ext.as_deref() {
        Some("yaml" | "yml") => {
            let mut value: serde_json::Value =
                serde_yaml::from_str(text).map_err(|e| CliError::ParseConfig {
                    path: path.to_path_buf(),
                    message: friendly_parse_error(&e.to_string()),
                })?;
            resolve(&mut value)?;
            serde_yaml::to_string(&value).map_err(|e| CliError::ParseConfig {
                path: path.to_path_buf(),
                message: e.to_string(),
            })
        }
        Some("json") => {
            let mut value: serde_json::Value =
                serde_json::from_str(text).map_err(|e| CliError::ParseConfig {
                    path: path.to_path_buf(),
                    message: friendly_parse_error(&e.to_string()),
                })?;
            resolve(&mut value)?;
            serde_json::to_string(&value).map_err(|e| CliError::ParseConfig {
                path: path.to_path_buf(),
                message: e.to_string(),
            })
        }
        _ => Err(CliError::UnknownExtension {
            path: path.to_path_buf(),
        }),
    }
}

impl PipelineConfig {
    /// Load a pipeline config from disk. The file extension determines the
    /// parser: `.yaml` / `.yml` → YAML, `.json` → JSON. Other extensions are
    /// rejected.
    ///
    /// Composition runs first via [`crate::compose::compose`]: `extends` (base
    /// inheritance), `profiles` (the named overlay selected by `profile`), and
    /// `!include` (YAML fragment substitution) are resolved into a single
    /// merged document before `${...}` interpolation and parsing.
    ///
    /// Secret directives (`${vault:…}`, `${aws-sm:…}`, etc.) are **not**
    /// resolved by this path. If any are present the call returns
    /// `CliError::SecretsRequireAsyncLoad` — use [`Self::from_path_async`] instead.
    pub fn from_path(path: impl AsRef<Path>, profile: Option<&str>) -> CliResult<Self> {
        Self::from_path_with(path, profile, &RunInputs::default())
    }

    /// [`Self::from_path`] with caller-supplied [`RunInputs`] (`params:` values
    /// and an `${env:}` overlay, #444).
    pub fn from_path_with(
        path: impl AsRef<Path>,
        profile: Option<&str>,
        inputs: &RunInputs,
    ) -> CliResult<Self> {
        let path = path.as_ref();
        let composed = crate::compose::compose(path, profile)?;
        let interpolated = resolve_document(&composed, path, inputs)?;
        let cfg = Self::from_text(&interpolated, path)?;
        // Secret directives need the async resolver path; never let them survive
        // into a connector config as literal `${vault:…}` text.
        crate::secrets::ensure_no_secret_directives(&cfg)?;
        Ok(cfg)
    }

    /// Like [`Self::from_path`] but does not reject secret directives — they are
    /// left unresolved. Used by `validate --no-secrets`. Composition
    /// (extends/profiles/`!include`) runs first via [`crate::compose::compose`].
    pub fn from_path_tolerating_secrets(
        path: impl AsRef<Path>,
        profile: Option<&str>,
    ) -> CliResult<Self> {
        Self::from_path_tolerating_secrets_with(path, profile, &RunInputs::default())
    }

    /// [`Self::from_path_tolerating_secrets`] with caller-supplied [`RunInputs`].
    pub fn from_path_tolerating_secrets_with(
        path: impl AsRef<Path>,
        profile: Option<&str>,
        inputs: &RunInputs,
    ) -> CliResult<Self> {
        let path = path.as_ref();
        let composed = crate::compose::compose(path, profile)?;
        let interpolated = resolve_document(&composed, path, inputs)?;
        Self::from_text(&interpolated, path)
    }

    /// Async load path: like [`Self::from_path`] but resolves secret-manager
    /// directives (`${vault:…}`, `${aws-sm:…}`, …) as a final stage. Composition
    /// (extends/profiles/`!include`) runs first via [`crate::compose::compose`].
    pub async fn from_path_async(path: impl AsRef<Path>, profile: Option<&str>) -> CliResult<Self> {
        Self::from_path_async_with(path, profile, &RunInputs::default()).await
    }

    /// [`Self::from_path_async`] with caller-supplied [`RunInputs`] (`params:`
    /// values and an `${env:}` overlay, #444).
    pub async fn from_path_async_with(
        path: impl AsRef<Path>,
        profile: Option<&str>,
        inputs: &RunInputs,
    ) -> CliResult<Self> {
        let path = path.as_ref();
        let composed = crate::compose::compose(path, profile)?;
        let interpolated = resolve_document(&composed, path, inputs)?;
        let mut cfg = Self::from_text(&interpolated, path)?;
        crate::secrets::resolve_secrets(&mut cfg).await?;
        Ok(cfg)
    }

    /// Load a config from **text** the way [`Self::from_path_with`] loads a
    /// file — env/file interpolation + params binding, then the typed parse,
    /// rejecting unresolved secret directives. For documents that never lived
    /// on disk (a hub composition); `path` picks the parser and labels errors.
    pub fn from_text_with(text: &str, path: &Path, inputs: &RunInputs) -> CliResult<Self> {
        let interpolated = resolve_document(text, path, inputs)?;
        let cfg = Self::from_text(&interpolated, path)?;
        crate::secrets::ensure_no_secret_directives(&cfg)?;
        Ok(cfg)
    }

    /// [`Self::from_text_with`] with async secret resolution — the text
    /// counterpart of [`Self::from_path_async_with`].
    pub async fn from_text_async_with(
        text: &str,
        path: &Path,
        inputs: &RunInputs,
    ) -> CliResult<Self> {
        let interpolated = resolve_document(text, path, inputs)?;
        let mut cfg = Self::from_text(&interpolated, path)?;
        crate::secrets::resolve_secrets(&mut cfg).await?;
        Ok(cfg)
    }

    /// Parse an already-interpolated config string. `path` is only used for
    /// error messages and to pick the parser by file extension.
    pub fn from_text(text: &str, path: &Path) -> CliResult<Self> {
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .map(str::to_ascii_lowercase);
        // Deprecated spellings are visible only in the raw document (#670).
        let raw: Option<serde_json::Value> = match ext.as_deref() {
            Some("yaml" | "yml") => serde_yaml::from_str(text).ok(),
            Some("json") => serde_json::from_str(text).ok(),
            _ => None,
        };
        if let Some(raw) = &raw {
            crate::vocabulary::warn_deprecated(raw);
        }
        let cfg: PipelineConfig = match ext.as_deref() {
            Some("yaml" | "yml") => {
                serde_yaml::from_str(text).map_err(|e| CliError::ParseConfig {
                    path: path.to_path_buf(),
                    message: friendly_parse_error(&e.to_string()),
                })?
            }
            Some("json") => serde_json::from_str(text).map_err(|e| CliError::ParseConfig {
                path: path.to_path_buf(),
                message: friendly_parse_error(&e.to_string()),
            })?,
            _ => {
                return Err(CliError::UnknownExtension {
                    path: path.to_path_buf(),
                });
            }
        };
        Self::finish(cfg, path)
    }

    /// Build a config from an already-parsed JSON value (used by `faucet serve`,
    /// which merges a submitted body onto a `--default-config` base). Runs the
    /// same version check + structural `${...}` ref resolution as [`Self::from_text`].
    ///
    /// **Note:** load-time `${env:VAR}` / `${file:PATH}` / `${secret:VAR}` directives
    /// are **not** resolved here — the caller must pre-resolve them (e.g. by running
    /// `interpolate` on the source text) before building the `Value`.
    pub fn from_value(value: serde_json::Value) -> CliResult<Self> {
        let synthetic = Path::new("<submitted>");
        crate::vocabulary::warn_deprecated(&value);
        let cfg: PipelineConfig =
            serde_json::from_value(value).map_err(|e| CliError::ParseConfig {
                path: synthetic.to_path_buf(),
                message: friendly_parse_error(&e.to_string()),
            })?;
        Self::finish(cfg, synthetic)
    }

    /// Shared post-parse tail: version gate + structural `${...}` ref resolution.
    fn finish(mut cfg: PipelineConfig, path: &Path) -> CliResult<Self> {
        if cfg.version != 1 {
            return Err(CliError::ParseConfig {
                path: path.to_path_buf(),
                message: format!(
                    "unsupported pipeline version {}, only version 1 is recognised",
                    cfg.version
                ),
            });
        }
        crate::interpolate::resolve_config_refs(&mut cfg)?;
        if let Some(obs) = cfg.observability.as_ref()
            && let Some(otel) = obs.otel.as_ref()
        {
            otel.to_core().map_err(CliError::Config)?;
        }
        Ok(cfg)
    }
}

/// Translate the typical serde "missing field" message into a hint when the
/// caller appears to be using the pre-#54 top-level shape.
fn friendly_parse_error(raw: &str) -> String {
    let lower = raw.to_ascii_lowercase();
    if lower.contains("missing field `pipeline`") {
        return format!(
            "{raw}\n\nhint: top-level `source:` / `sink:` is no longer supported. Wrap them in a `pipeline:` block — see `faucet init` for the new shape."
        );
    }
    if lower.contains("unknown field `extends`") || lower.contains("unknown field `profiles`") {
        return format!(
            "{raw}\n\nhint: config composition (`extends` / `profiles` / `!include`) is resolved only for file-based loads, not for configs submitted to `faucet serve` — resolve composition before submitting."
        );
    }
    raw.to_owned()
}

/// Convenience: parse a config from text using a synthetic path so the right
/// parser is selected. Used by tests and the `validate --stdin` flow.
pub fn parse_with_extension(text: &str, ext: &str) -> CliResult<PipelineConfig> {
    let synthetic = PathBuf::from(format!("pipeline.{ext}"));
    PipelineConfig::from_text(text, &synthetic)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_minimal_pipeline_yaml() {
        let yaml = r#"
version: 1
pipeline:
  source:
    type: rest
    config:
      base_url: https://api.example.com
  sink:
    type: jsonl
    config:
      path: ./out.jsonl
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        assert_eq!(cfg.pipeline.source.as_ref().unwrap().kind, "rest");
        assert_eq!(cfg.pipeline.sink.as_ref().unwrap().kind, "jsonl");
        assert!(cfg.matrix.is_empty());
        assert!(cfg.execution.is_none());
        assert!(cfg.pipeline.transforms.is_empty());
        assert!(cfg.pipeline.state.is_none());
    }

    #[test]
    fn parses_replication_block() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: postgres-cdc, config: { connection_url: "postgres://x", slot_name: s, publication_name: p } }
  sink:   { type: postgres, config: { connection_url: "postgres://y", table_name: t, column_mapping: auto_map, write_mode: upsert, key: [id] } }
  state:  { type: file, config: { path: ./st } }
replication:
  mode: snapshot_then_cdc
  snapshot:
    source: { type: postgres, config: { connection_url: "postgres://x", query: "SELECT * FROM t" } }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let r = cfg.replication.expect("replication parsed");
        assert_eq!(r.snapshot.source.kind, "postgres");
    }

    #[test]
    fn pipeline_spec_parses_schema_block() {
        let yaml = r#"
version: 1
pipeline:
  source:
    type: rest
    config:
      base_url: https://api.example.com
  sink:
    type: jsonl
    config:
      path: ./out.jsonl
  schema:
    on_drift: evolve
    allow_type_widening: false
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let schema = cfg.pipeline.schema.expect("schema block parsed");
        assert_eq!(schema.on_drift, faucet_core::OnDrift::Evolve);
        assert!(!schema.allow_type_widening);
    }

    #[test]
    fn parses_minimal_json() {
        let raw = r#"{
            "version": 1,
            "pipeline": {
                "source": {"type": "rest", "config": {}},
                "sink":   {"type": "jsonl", "config": {"path": "./out.jsonl"}}
            }
        }"#;
        let cfg = parse_with_extension(raw, "json").unwrap();
        assert_eq!(cfg.pipeline.source.as_ref().unwrap().kind, "rest");
    }

    #[test]
    fn parses_matrix_rows_with_partial_overrides() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://api.example.com } }
  sink:   { type: jsonl, config: { path: ./out.jsonl } }
matrix:
  - id: users
    source: { config: { path: /v1/users } }
    sink:   { config: { path: ./users.jsonl } }
  - id: posts
    parent: users
    parent_key: user_id
    source: { config: { path: "/v1/users/${users.id}/posts" } }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        assert_eq!(cfg.matrix.len(), 2);
        assert_eq!(cfg.matrix[0].id.as_deref(), Some("users"));
        assert!(cfg.matrix[0].parent.is_none());
        let users_src = cfg.matrix[0].source.as_ref().unwrap();
        assert_eq!(users_src.config.as_ref().unwrap()["path"], "/v1/users");

        assert_eq!(cfg.matrix[1].parent.as_deref(), Some("users"));
        assert_eq!(cfg.matrix[1].parent_key, "user_id");
    }

    #[test]
    fn parent_key_defaults_to_id() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
matrix:
  - { id: users }
  - { id: posts, parent: users }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        assert_eq!(cfg.matrix[1].parent_key, "id");
    }

    #[test]
    fn parses_execution_block() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
execution:
  max_concurrent: 8
  on_error: stop
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let exec = cfg.execution.unwrap();
        assert_eq!(exec.max_concurrent, Some(8));
        assert_eq!(exec.on_error, OnError::Stop);
    }

    #[test]
    fn on_error_defaults_to_continue() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
execution: { max_concurrent: 2 }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        assert_eq!(cfg.execution.unwrap().on_error, OnError::Continue);
    }

    #[test]
    fn rejects_old_top_level_source_sink_with_hint() {
        // Pre-#54 shape: `source:` and `sink:` at the top level.
        let yaml = r#"
version: 1
source: { type: rest, config: {} }
sink:   { type: jsonl, config: { path: ./o.jsonl } }
"#;
        let err = parse_with_extension(yaml, "yaml").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("pipeline"),
            "expected a hint about wrapping in `pipeline:`, got: {msg}"
        );
    }

    #[test]
    fn rejects_unknown_extension() {
        let text = "version: 1\n";
        let err = PipelineConfig::from_text(text, Path::new("pipeline.toml")).unwrap_err();
        assert!(matches!(err, CliError::UnknownExtension { .. }));
    }

    #[test]
    fn rejects_future_version() {
        let yaml = r#"
version: 99
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./x } }
"#;
        let err = parse_with_extension(yaml, "yaml").unwrap_err();
        match err {
            CliError::ParseConfig { message, .. } => assert!(message.contains("version 99")),
            other => panic!("expected ParseConfig, got {other:?}"),
        }
    }

    #[test]
    fn transforms_and_state_round_trip() {
        let yaml = r#"
version: 1
pipeline:
  source:
    type: rest
    config: {}
  transforms:
    - type: snake_case
    - type: flatten
      config: { separator: "__" }
  sink:
    type: jsonl
    config: { path: "./out.jsonl" }
  state:
    type: file
    config: { path: "./.faucet-state" }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        assert_eq!(cfg.pipeline.transforms.len(), 2);
        assert_eq!(cfg.pipeline.transforms[0].kind, "snake_case");
        assert_eq!(cfg.pipeline.transforms[1].kind, "flatten");
        assert_eq!(
            cfg.pipeline.transforms[1].config,
            json!({"separator": "__"})
        );
        let state = cfg.pipeline.state.unwrap();
        assert_eq!(state.kind, "file");
    }

    #[test]
    fn from_path_interpolates_env_var() {
        unsafe { std::env::set_var("FAUCET_CFG_URL", "https://x.example") };
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pipeline.yaml");
        std::fs::write(
            &path,
            r#"
version: 1
pipeline:
  source:
    type: rest
    config:
      base_url: ${env:FAUCET_CFG_URL}
  sink:
    type: jsonl
    config:
      path: ./out.jsonl
"#,
        )
        .unwrap();
        let cfg = PipelineConfig::from_path(&path, None).unwrap();
        assert_eq!(
            cfg.pipeline.source.as_ref().unwrap().config["base_url"],
            "https://x.example"
        );
        unsafe { std::env::remove_var("FAUCET_CFG_URL") };
    }

    #[test]
    fn observability_block_parses() {
        let y = r#"
version: 1
name: x
observability:
  prometheus:
    listen: "127.0.0.1:9464"
    buckets: [0.01, 0.1, 1.0]
  tracing:
    level: "info"
pipeline:
  source:
    type: rest
    config:
      base_url: "https://example.com"
      path: "/data"
  sink:
    type: jsonl
    config:
      path: "/tmp/faucet-test.jsonl"
"#;
        let cfg: PipelineConfig = serde_yaml::from_str(y).unwrap();
        let obs = cfg.observability.expect("observability block parsed");
        let p = obs.prometheus.expect("prometheus parsed");
        assert_eq!(p.listen, "127.0.0.1:9464");
        assert_eq!(p.buckets.unwrap().len(), 3);
        assert_eq!(obs.tracing.unwrap().level.unwrap(), "info");
    }

    #[test]
    fn from_path_leaves_id_path_tokens_unresolved_at_load_time() {
        // `${users.id}` must survive load-time interpolation so the matrix
        // expander / record-time resolver can handle it later.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pipeline.yaml");
        std::fs::write(
            &path,
            r#"
version: 1
pipeline:
  source: { type: rest, config: { path: "/v1/users/${users.id}/posts" } }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
"#,
        )
        .unwrap();
        let cfg = PipelineConfig::from_path(&path, None).unwrap();
        assert_eq!(
            cfg.pipeline.source.as_ref().unwrap().config["path"],
            "/v1/users/${users.id}/posts"
        );
    }

    #[cfg(feature = "schedule")]
    #[test]
    fn parses_schedule_block() {
        let yaml = r#"
version: 1
schedule:
  cron: "0 2 * * *"
  timezone: "America/Los_Angeles"
  overlap_policy: skip
  max_consecutive_failures: 5
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let s = cfg.schedule.expect("schedule parsed");
        assert_eq!(s.cron, "0 2 * * *");
        assert_eq!(s.timezone, "America/Los_Angeles");
        assert_eq!(s.max_consecutive_failures, Some(5));
    }

    #[test]
    fn execution_spec_parses_adaptive_block() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://api.example.com } }
  sink:   { type: jsonl, config: { path: ./out.jsonl } }
execution:
  adaptive_batch_size:
    enabled: true
    min: 200
    max: 4000
    target_latency_ms: 800
"#;
        let cfg = crate::config::parse_with_extension(yaml, "yaml").unwrap();
        let ab = cfg.execution.unwrap().adaptive_batch_size.unwrap();
        assert!(ab.enabled);
        assert_eq!(ab.min, 200);
        assert_eq!(ab.target_latency_ms, Some(800));
        ab.validate().unwrap();
    }

    #[cfg(feature = "quality")]
    #[test]
    fn parses_quality_block() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: { url: "https://x" } }
  quality:
    record:
      - { type: not_null, field: id, on_failure: abort }
  sink: { type: stdout, config: {} }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let q = cfg.pipeline.quality.expect("quality parsed");
        assert_eq!(q.record.len(), 1);
    }

    #[cfg(feature = "contract")]
    #[test]
    fn parses_contract_block() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: { url: "https://x" } }
  contract:
    version: "1.0.0"
    on_breach: warn
    fields:
      - { name: id, type: integer }
      - { name: status, type: string, enum: [open, closed] }
  sink: { type: stdout, config: {} }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let c = cfg.pipeline.contract.expect("contract parsed");
        assert_eq!(c.version, "1.0.0");
        assert_eq!(c.on_breach, faucet_core::OnBreach::Warn);
        assert_eq!(c.fields.len(), 2);
    }

    #[test]
    fn parses_dlq_block_with_defaults() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
  dlq:
    sink: { type: jsonl, config: { path: ./dlq.jsonl } }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let dlq = cfg.pipeline.dlq.expect("dlq parsed");
        assert_eq!(dlq.sink.kind, "jsonl");
        assert_eq!(dlq.on_batch_error, OnBatchErrorSpec::Propagate);
        assert!(dlq.max_failures_per_page.is_none());
        assert!(dlq.max_failures_total.is_none());
        assert!(dlq.include_original_payload);
    }

    #[test]
    fn parses_dlq_block_with_dlq_all_and_budgets() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
  dlq:
    sink: { type: kafka, config: { brokers: ["b:9092"], topic: dlq } }
    on_batch_error: dlq_all
    max_failures_per_page: 100
    max_failures_total: 10000
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let dlq = cfg.pipeline.dlq.unwrap();
        assert_eq!(dlq.sink.kind, "kafka");
        assert_eq!(dlq.on_batch_error, OnBatchErrorSpec::DlqAll);
        assert_eq!(dlq.max_failures_per_page, Some(100));
        assert_eq!(dlq.max_failures_total, Some(10000));
    }

    #[test]
    fn matrix_row_dlq_null_disables_inherited_dlq() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
  dlq:
    sink: { type: jsonl, config: { path: ./dlq.jsonl } }
matrix:
  - id: a
  - id: b
    dlq: null
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        assert!(cfg.matrix[0].dlq.is_none());
        assert_eq!(cfg.matrix[1].dlq, Some(None));
    }

    #[test]
    fn matrix_row_dlq_object_replaces_inherited_dlq() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
  dlq:
    sink: { type: jsonl, config: { path: ./base.jsonl } }
matrix:
  - id: a
    dlq:
      sink: { type: jsonl, config: { path: ./a.jsonl } }
      on_batch_error: dlq_all
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let row_dlq = cfg.matrix[0].dlq.clone().unwrap().unwrap();
        assert_eq!(row_dlq.on_batch_error, OnBatchErrorSpec::DlqAll);
        let sink_path = row_dlq.sink.config.get("path").unwrap();
        assert_eq!(sink_path, "./a.jsonl");
    }

    #[test]
    fn parses_named_sources_and_sinks() {
        let yaml = r#"
version: 1
pipeline:
  sources:
    users_api:
      type: rest
      config: { base_url: https://api.example.com }
    posts_api:
      type: rest
      config: { base_url: https://api.example.com }
  sinks:
    warehouse:
      type: postgres
      config: { connection_url: "postgres://x" }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        assert!(cfg.pipeline.source.is_none());
        assert!(cfg.pipeline.sink.is_none());
        assert_eq!(cfg.pipeline.sources.len(), 2);
        assert_eq!(cfg.pipeline.sources["users_api"].kind, "rest");
        assert_eq!(cfg.pipeline.sinks["warehouse"].kind, "postgres");
    }

    #[test]
    fn legacy_singular_source_still_parses() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        assert!(cfg.pipeline.source.is_some());
        assert!(cfg.pipeline.sink.is_some());
        assert!(cfg.pipeline.sources.is_empty());
        assert!(cfg.pipeline.sinks.is_empty());
    }

    #[test]
    fn parses_matrix_row_with_ref_field() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
matrix:
  - id: load_users
    source:
      ref: users_api
      config: { path: /v1/users }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let src = cfg.matrix[0].source.as_ref().unwrap();
        assert_eq!(src.r#ref.as_deref(), Some("users_api"));
        assert_eq!(src.kind, None);
        assert_eq!(src.config.as_ref().unwrap()["path"], "/v1/users");
    }

    #[test]
    fn parses_top_level_vars_block() {
        let yaml = r#"
version: 1
vars:
  api_base: https://api.example.com
  api_token_env: API_TOKEN
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let vars = cfg.vars.as_ref().unwrap();
        assert_eq!(vars["api_base"], "https://api.example.com");
        assert_eq!(vars["api_token_env"], "API_TOKEN");
    }

    #[test]
    fn vars_block_is_optional() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        assert!(cfg.vars.is_none());
    }

    #[test]
    fn from_path_resolves_vars_at_load() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pipeline.yaml");
        std::fs::write(
            &path,
            r#"
version: 1
vars:
  base: https://api.example.com
pipeline:
  source: { type: rest, config: { url: "${vars.base}/v1" } }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
"#,
        )
        .unwrap();
        let cfg = PipelineConfig::from_path(&path, None).unwrap();
        assert_eq!(
            cfg.pipeline.source.as_ref().unwrap().config["url"],
            "https://api.example.com/v1"
        );
    }

    #[test]
    fn sync_from_path_errors_on_secret_directive() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("p.yaml");
        std::fs::write(
            &path,
            r#"
version: 1
pipeline:
  source: { type: rest, config: { url: "${vault:secret/x}" } }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
"#,
        )
        .unwrap();
        match PipelineConfig::from_path(&path, None).unwrap_err() {
            CliError::SecretsRequireAsyncLoad => {}
            other => panic!("expected SecretsRequireAsyncLoad, got {other:?}"),
        }
    }

    #[test]
    fn from_value_accepts_v1_and_resolves_refs() {
        let v = serde_json::json!({
            "version": 1,
            "vars": { "out": "resolved.jsonl" },
            "pipeline": {
                "source": { "type": "csv",  "config": { "path": "x.csv" } },
                "sink":   { "type": "jsonl", "config": { "path": "${vars.out}" } }
            }
        });
        let cfg = PipelineConfig::from_value(v).unwrap();
        assert_eq!(cfg.version, 1);
        // structural ${vars.*} refs are resolved by from_value (via finish → resolve_config_refs)
        assert_eq!(cfg.pipeline.sink.unwrap().config["path"], "resolved.jsonl");
    }

    #[test]
    fn from_value_rejects_non_v1() {
        // structurally valid config; only the version is wrong
        let v = serde_json::json!({ "version": 99, "pipeline": {} });
        let err = PipelineConfig::from_value(v).unwrap_err();
        match err {
            CliError::ParseConfig { message, .. } => assert!(message.contains("version 99")),
            other => panic!("expected ParseConfig, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn async_from_path_loads_without_secrets() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("p.yaml");
        std::fs::write(
            &path,
            r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
"#,
        )
        .unwrap();
        let cfg = PipelineConfig::from_path_async(&path, None).await.unwrap();
        assert_eq!(cfg.version, 1);
    }

    #[cfg(feature = "lineage")]
    #[test]
    fn parses_lineage_block() {
        let yaml = r#"
version: 1
lineage:
  namespace: prod
  transport: { type: file, config: { path: /tmp/ol.jsonl } }
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let l = cfg.lineage.expect("lineage parsed");
        assert_eq!(l.namespace, "prod");
    }

    #[test]
    fn from_path_resolves_extends_and_profile() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("base.yaml"),
            "version: 1\npipeline:\n  source: { type: csv, config: { path: x.csv } }\n  sink: { type: jsonl, config: { path: base.jsonl } }\nprofiles:\n  prod:\n    pipeline:\n      sink: { config: { path: prod.jsonl } }\n",
        )
        .unwrap();
        let app = dir.path().join("app.yaml");
        std::fs::write(&app, "extends: ./base.yaml\n").unwrap();

        // No profile → base sink path.
        let cfg = PipelineConfig::from_path(&app, None).unwrap();
        assert_eq!(
            cfg.pipeline.sink.as_ref().unwrap().config["path"],
            "base.jsonl"
        );

        // --profile prod → overridden sink path.
        let cfg = PipelineConfig::from_path(&app, Some("prod")).unwrap();
        assert_eq!(
            cfg.pipeline.sink.as_ref().unwrap().config["path"],
            "prod.jsonl"
        );
    }

    #[test]
    fn from_value_rejects_extends_with_composition_hint() {
        // A submitted body (serve path) must not silently accept `extends`.
        let v = serde_json::json!({
            "version": 1,
            "extends": "base.yaml",
            "pipeline": { "source": { "type": "csv", "config": {} }, "sink": { "type": "jsonl", "config": {} } }
        });
        let err = PipelineConfig::from_value(v).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("composition"),
            "expected composition hint, got: {msg}"
        );
    }

    #[test]
    fn delivery_defaults_to_at_least_once_and_parses_exactly_once() {
        // Default when omitted: top-level delivery should be AtLeastOnce.
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        assert_eq!(cfg.delivery, faucet_core::DeliveryMode::AtLeastOnce);

        // Explicit exactly_once at top level.
        let yaml2 = r#"
version: 1
delivery: exactly_once
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
"#;
        let cfg2 = parse_with_extension(yaml2, "yaml").unwrap();
        assert_eq!(cfg2.delivery, faucet_core::DeliveryMode::ExactlyOnce);

        // Matrix row: absent delivery inherits (None), explicit overrides.
        let yaml3 = r#"
version: 1
delivery: at_least_once
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
matrix:
  - id: a
  - id: b
    delivery: exactly_once
"#;
        let cfg3 = parse_with_extension(yaml3, "yaml").unwrap();
        assert_eq!(cfg3.matrix[0].delivery, None);
        assert_eq!(
            cfg3.matrix[1].delivery,
            Some(faucet_core::DeliveryMode::ExactlyOnce)
        );
    }

    #[test]
    fn resilience_spec_parses_and_builds_policy() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: "https://x" } }
  sink: { type: stdout, config: {} }
resilience:
  retry: { max_attempts: 4, backoff: exponential, base_ms: 100, max_ms: 5000, jitter: true }
  retry_on: [http_5xx, timeout]
  circuit_breaker: { consecutive_failures: 3, cooldown_secs: 30 }
  poison: { max_row_attempts: 2, action: dlq }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let spec = cfg.resilience.unwrap();
        let policy = spec.to_policy().unwrap();
        assert_eq!(policy.retry.max_attempts, 4);
        assert_eq!(policy.circuit_breaker.unwrap().consecutive_failures, 3);
        assert_eq!(policy.poison.unwrap().max_row_attempts, 2);
    }

    #[test]
    fn resilience_rejects_zero_max_attempts() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: "https://x" } }
  sink: { type: stdout, config: {} }
resilience: { retry: { max_attempts: 0 } }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let err = cfg.resilience.unwrap().to_policy().unwrap_err();
        assert!(err.to_string().contains("max_attempts"));
    }

    #[test]
    fn observability_parses_otel_block() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: "http://x" } }
  sink: { type: stdout, config: {} }
observability:
  otel:
    endpoint: http://collector:4317
    protocol: grpc
    export: [traces, metrics]
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let otel = cfg.observability.unwrap().otel.unwrap();
        assert_eq!(otel.endpoint, "http://collector:4317");
    }

    #[test]
    fn otel_validation_rejects_bad_ratio() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: "http://x" } }
  sink: { type: stdout, config: {} }
observability:
  otel:
    sample_ratio: 9.0
"#;
        let err = parse_with_extension(yaml, "yaml").unwrap_err();
        assert!(format!("{err}").contains("sample_ratio"));
    }
}
