//! Template Hub document kinds (#571).
//!
//! A hub template is **half** a pipeline. A `source-template` owns the hard
//! part — how to talk to one system, how its records are shaped, and which
//! **streams** (tables) it produces with what write semantics. A
//! `sink-template` is thin: where records land, and which sink config key
//! receives the stream name. Any source × any sink composes into an ordinary
//! `PipelineConfig` at run time ([`super::compose::compose`]), so a template written
//! once serves every destination.
//!
//! Both kinds are plain data + validation; no I/O here.

use std::collections::{BTreeMap, HashMap, HashSet};

use faucet_core::WriteMode;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::{ConnectorSpec, TransformSpec};
use crate::error::{CliError, CliResult};
use crate::params::ParamsSpec;

/// The `kind:` discriminator at the top of a template document.
///
/// `source-template` / `sink-template` are the hub kinds composed at run time;
/// `pipeline` is a complete, hand-written config registered as-is (the
/// pre-#571 template model, kept as an explicit kind for graphs that are not
/// "streams of one source" — topology mode, multi-source DAGs).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum TemplateKind {
    SourceTemplate,
    SinkTemplate,
    Pipeline,
    /// The operational half of a composed run (#679): state, DLQ,
    /// notifications, SLA and delivery policy, applied over a source × sink
    /// composition. Never runnable on its own.
    Deployment,
}

impl TemplateKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SourceTemplate => "source-template",
            Self::SinkTemplate => "sink-template",
            Self::Pipeline => "pipeline",
            Self::Deployment => "deployment",
        }
    }

    /// Parse a `kind:` value.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "source-template" => Some(Self::SourceTemplate),
            "sink-template" => Some(Self::SinkTemplate),
            "pipeline" => Some(Self::Pipeline),
            "deployment" => Some(Self::Deployment),
            _ => None,
        }
    }

    /// The registry's default for a record written before kinds existed.
    pub const fn pipeline() -> Self {
        Self::Pipeline
    }

    /// A hub kind (composed at run time) rather than a complete pipeline.
    pub fn is_hub(self) -> bool {
        matches!(self, Self::SourceTemplate | Self::SinkTemplate)
    }
}

impl std::fmt::Display for TemplateKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

fn default_version() -> u32 {
    1
}

/// Slug rule shared by template names and stream names: lowercase, digits,
/// `-`/`_`, first character alphanumeric. Stream names additionally become
/// table names, so `-` is rejected there (see [`Stream::validate`]).
/// The namespace the hub's maintained templates live under. An unqualified
/// locator (`--source netsuite`) resolves here when no top-level file matches,
/// so the official set is addressable by short name while still being owned
/// by the org like any other namespace (#682).
pub const OFFICIAL_OWNER: &str = "faucet-hq";

/// The hub id of a template: `owner/name`, or `name` for an unscoped one.
pub fn hub_id(owner: Option<&str>, name: &str) -> String {
    match owner {
        Some(o) => format!("{o}/{name}"),
        None => name.to_string(),
    }
}

/// Split a hub id into `(owner, name)`.
pub fn split_hub_id(id: &str) -> (Option<&str>, &str) {
    match id.split_once('/') {
        Some((o, n)) => (Some(o), n),
        None => (None, id),
    }
}

fn check_slug(what: &str, raw: &str, allow_dash: bool) -> CliResult<()> {
    let ok = !raw.is_empty()
        && raw.len() <= 64
        && raw
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        && raw.chars().all(|c| {
            c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || (allow_dash && c == '-')
        });
    if ok {
        Ok(())
    } else {
        Err(CliError::Config(format!(
            "{what} '{raw}' must match ^[a-z0-9][a-z0-9_{}]*$ (max 64 chars)",
            if allow_dash { "-" } else { "" }
        )))
    }
}

/// One or more write modes, in **preference order**. Accepts a bare string
/// (`write: overwrite`) or a list (`write: [overwrite, upsert]`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum WriteChoice {
    One(WriteMode),
    Many(Vec<WriteMode>),
}

impl Default for WriteChoice {
    fn default() -> Self {
        Self::One(WriteMode::Append)
    }
}

impl WriteChoice {
    /// The ordered candidate list.
    pub fn candidates(&self) -> Vec<WriteMode> {
        match self {
            Self::One(m) => vec![*m],
            Self::Many(v) => v.clone(),
        }
    }
}

/// A source-side override for one stream: the connector config fragment
/// deep-merged onto the chosen source's `config` (a REST `path`, a SQL
/// `query`, a per-stream `records_path`, …), and optionally which named
/// source it reads from.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StreamSource {
    /// A source declared under the template's `sources:` map. Omitted = the
    /// template's main `source`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub r#ref: Option<String>,
    #[serde(default = "empty_object")]
    pub config: Value,
}

impl Default for StreamSource {
    fn default() -> Self {
        Self {
            r#ref: None,
            config: empty_object(),
        }
    }
}

/// The reserved name of the template's main `source` inside the composed
/// `pipeline.sources` map.
pub const DEFAULT_SOURCE: &str = "default";

fn empty_object() -> Value {
    Value::Object(Default::default())
}

/// One table a source template produces.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Stream {
    /// Stream name — becomes the matrix row id, the state-key suffix, and the
    /// value of `${stream}` in the sink template's `per_stream` block (so it is
    /// the destination table name). `^[a-z0-9][a-z0-9_]*$`.
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Source-config override for this stream.
    #[serde(default)]
    pub source: StreamSource,
    /// Transforms run **after** the template's shared `transforms`, for this
    /// stream only (the matrix-row layer).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transforms: Vec<TransformSpec>,
    /// Destination key columns. Required by `upsert` / `delete`; becomes the
    /// sink's `key`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub primary_keys: Vec<String>,
    /// Acceptable write modes in preference order. The composer picks the first
    /// one the chosen sink supports and fails the stream if none is. Default
    /// `append`.
    #[serde(default)]
    pub write: WriteChoice,
    /// Run this stream once per record of the named parent stream, with
    /// `${<parent>.<field>}` tokens resolved per record (matrix parent/child
    /// fan-out).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    /// The parent record field that keys each child invocation's state.
    /// Default `id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_key: Option<String>,
    /// Set `false` to drop the template's shared `transforms` for this stream
    /// (the matrix-row `inherit_transforms` switch).
    #[serde(default = "default_true", skip_serializing_if = "is_true")]
    pub inherit_transforms: bool,
}

fn default_true() -> bool {
    true
}

fn is_true(b: &bool) -> bool {
    *b
}

impl Stream {
    pub fn validate(&self) -> CliResult<()> {
        check_slug("stream name", &self.name, false)?;
        if !self.source.config.is_object() {
            return Err(CliError::Config(format!(
                "stream '{}': `source.config` must be a mapping",
                self.name
            )));
        }
        let modes = self.write.candidates();
        if modes.is_empty() {
            return Err(CliError::Config(format!(
                "stream '{}': `write` must list at least one mode",
                self.name
            )));
        }
        let mut seen = HashSet::new();
        for m in &modes {
            if !seen.insert(m.as_str()) {
                return Err(CliError::Config(format!(
                    "stream '{}': `write` lists '{}' twice",
                    self.name,
                    m.as_str()
                )));
            }
        }
        for k in &self.primary_keys {
            if k.trim().is_empty() {
                return Err(CliError::Config(format!(
                    "stream '{}': `primary_keys` contains an empty name",
                    self.name
                )));
            }
        }
        if self.parent.as_deref() == Some(self.name.as_str()) {
            return Err(CliError::Config(format!(
                "stream '{}': cannot be its own parent",
                self.name
            )));
        }
        if self.parent.is_none() && self.parent_key.is_some() {
            return Err(CliError::Config(format!(
                "stream '{}': `parent_key` needs a `parent`",
                self.name
            )));
        }
        Ok(())
    }
}

/// `kind: source-template` — one system, its shaping, and its streams.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SourceTemplate {
    /// Must be `source-template`.
    pub kind: TemplateKind,
    /// Document version; must be `1`.
    #[serde(default = "default_version")]
    pub version: u32,
    /// Short name (`netsuite`). With `owner`, the hub id is `owner/name`; an
    /// official template (no owner) is addressed by `name` alone. The id is the
    /// composed pipeline's `name:`, so per-stream state keys
    /// (`{id}::{stream}`) stay stable no matter which sink is composed in.
    pub name: String,
    /// Publisher namespace — a GitHub user or org login (#682), equal to the
    /// directory the file lives in (`source-templates/<owner>/<name>.yaml`).
    /// The hub's maintained set is the `faucet-hq` namespace; a top-level file
    /// with no owner is an unscoped template (a private hub's shortcut).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Free-form discovery tags (`finance`, `hr`, `saas`, …).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    /// Link to the upstream API docs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub docs: Option<String>,
    /// Trigger-time parameters (`${param.NAME}`), same grammar as a pipeline's
    /// `params:` block. Merged with the sink template's params at compose time;
    /// a name declared by both with different specs is an error.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub params: ParamsSpec,
    /// Shared auth providers (`auth: { ref }` targets), same as a pipeline's
    /// top-level `auth:` catalog.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<HashMap<String, Value>>,
    /// The source connector every stream shares (unless a stream names one of
    /// `sources` via `source.ref`).
    pub source: ConnectorSpec,
    /// Additional named sources for streams that read a second endpoint
    /// family (a reports API beside the entity API, an older version, …).
    /// `default` is reserved for `source`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub sources: BTreeMap<String, ConnectorSpec>,
    /// Shaping that travels with the source — runs before **every** sink, so
    /// each destination receives the same record shape.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transforms: Vec<TransformSpec>,
    /// Optional data contract, passed through as `pipeline.contract`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contract: Option<Value>,
    /// The tables this source produces; one matrix row each.
    pub streams: Vec<Stream>,
}

impl SourceTemplate {
    /// `owner/name`, or `name` for an unscoped template.
    pub fn id(&self) -> String {
        hub_id(self.owner.as_deref(), &self.name)
    }

    /// Maintained by the hub itself (the [`OFFICIAL_OWNER`] namespace).
    pub fn is_official(&self) -> bool {
        self.owner.as_deref() == Some(OFFICIAL_OWNER)
    }

    pub fn validate(&self) -> CliResult<()> {
        if self.kind != TemplateKind::SourceTemplate {
            return Err(CliError::Config(format!(
                "'{}' is a {}, not a source-template",
                self.name,
                self.kind.as_str()
            )));
        }
        if self.version != 1 {
            return Err(CliError::Config(format!(
                "source-template '{}': unsupported version {} (expected 1)",
                self.name, self.version
            )));
        }
        check_slug("source-template name", &self.name, true)?;
        if let Some(o) = &self.owner {
            check_slug("source-template owner", o, true)?;
        }
        crate::params::spec::validate(&self.params)?;
        if self.streams.is_empty() {
            return Err(CliError::Config(format!(
                "source-template '{}': `streams` is empty — a source template must produce at least one table",
                self.name
            )));
        }
        let mut seen = HashSet::new();
        for s in &self.streams {
            s.validate()
                .map_err(|e| CliError::Config(format!("source-template '{}': {e}", self.name)))?;
            if !seen.insert(s.name.as_str()) {
                return Err(CliError::Config(format!(
                    "source-template '{}': stream '{}' is declared twice",
                    self.name, s.name
                )));
            }
        }
        for s in &self.streams {
            if let Some(p) = &s.parent
                && !seen.contains(p.as_str())
            {
                return Err(CliError::Config(format!(
                    "source-template '{}': stream '{}' names unknown parent stream '{p}'",
                    self.name, s.name
                )));
            }
            if let Some(r) = &s.source.r#ref
                && r != DEFAULT_SOURCE
                && !self.sources.contains_key(r)
            {
                return Err(CliError::Config(format!(
                    "source-template '{}': stream '{}' reads source '{r}', which is not declared under `sources`",
                    self.name, s.name
                )));
            }
        }
        if self.sources.contains_key(DEFAULT_SOURCE) {
            return Err(CliError::Config(format!(
                "source-template '{}': `sources.default` is reserved for the main `source`",
                self.name
            )));
        }
        if self.source.transforms.is_some() {
            return Err(CliError::Config(format!(
                "source-template '{}': put shared transforms in the top-level `transforms:`, not under `source`",
                self.name
            )));
        }
        check_param_refs(
            &self.name,
            &self.params,
            &[
                serde_json::to_value(&self.source).unwrap_or(Value::Null),
                serde_json::to_value(&self.sources).unwrap_or(Value::Null),
                serde_json::to_value(&self.transforms).unwrap_or(Value::Null),
                serde_json::to_value(&self.auth).unwrap_or(Value::Null),
                serde_json::to_value(&self.streams).unwrap_or(Value::Null),
                self.contract.clone().unwrap_or(Value::Null),
            ],
        )?;
        Ok(())
    }

    /// Stream names in declaration order.
    pub fn stream_names(&self) -> Vec<&str> {
        self.streams.iter().map(|s| s.name.as_str()).collect()
    }
}

/// `kind: sink-template` — a destination, and how each stream is addressed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SinkTemplate {
    /// Must be `sink-template`.
    pub kind: TemplateKind,
    #[serde(default = "default_version")]
    pub version: u32,
    /// Hub id.
    pub name: String,
    /// Publisher namespace — a GitHub user or org login (#682), equal to the
    /// directory the file lives in; `faucet-hq` for the hub's maintained set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub docs: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub params: ParamsSpec,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<HashMap<String, Value>>,
    /// The sink connector. `write_mode` / `key` must **not** be set here — the
    /// composer injects them per stream.
    pub sink: ConnectorSpec,
    /// Sink config keys rendered once per stream, with `${stream}` replaced by
    /// the stream name and `${source}` by the source template's name — e.g.
    /// `{ table_id: "${stream}" }` for a warehouse, or
    /// `{ path: "./out/${source}/${stream}.jsonl" }` for a file sink. This is
    /// what makes one sink template serve every stream of every source.
    pub per_stream: BTreeMap<String, Value>,
    /// Write modes this template **satisfies by construction** with a mode
    /// the connector natively supports: `{ overwrite: append }` on a file sink
    /// configured with `append: false` says "a stream that wants a full
    /// refresh gets one — the file is rewritten every run — so run it as
    /// append". The composer records the substitution; the connector's real
    /// capabilities are never overstated for a mode that needs keys (`upsert`
    /// / `delete` cannot be aliased).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub write_mode_aliases: BTreeMap<String, WriteMode>,
}

impl SinkTemplate {
    /// `owner/name`, or `name` for an unscoped template.
    pub fn id(&self) -> String {
        hub_id(self.owner.as_deref(), &self.name)
    }

    /// Maintained by the hub itself (the [`OFFICIAL_OWNER`] namespace).
    pub fn is_official(&self) -> bool {
        self.owner.as_deref() == Some(OFFICIAL_OWNER)
    }

    pub fn validate(&self) -> CliResult<()> {
        if self.kind != TemplateKind::SinkTemplate {
            return Err(CliError::Config(format!(
                "'{}' is a {}, not a sink-template",
                self.name,
                self.kind.as_str()
            )));
        }
        if self.version != 1 {
            return Err(CliError::Config(format!(
                "sink-template '{}': unsupported version {} (expected 1)",
                self.name, self.version
            )));
        }
        check_slug("sink-template name", &self.name, true)?;
        if let Some(o) = &self.owner {
            check_slug("sink-template owner", o, true)?;
        }
        crate::params::spec::validate(&self.params)?;
        if self.sink.transforms.is_some() || !self.sink.inherit_transforms {
            return Err(CliError::Config(format!(
                "sink-template '{}': sinks carry no transforms — shaping belongs to the source template",
                self.name
            )));
        }
        if let Some(obj) = self.sink.config.as_object() {
            for k in ["write_mode", "key"] {
                if obj.contains_key(k) {
                    return Err(CliError::Config(format!(
                        "sink-template '{}': `sink.config.{k}` is injected per stream by the composer — remove it",
                        self.name
                    )));
                }
            }
        } else {
            return Err(CliError::Config(format!(
                "sink-template '{}': `sink.config` must be a mapping",
                self.name
            )));
        }
        if self.per_stream.is_empty() {
            return Err(CliError::Config(format!(
                "sink-template '{}': `per_stream` is empty — every stream would land in the same place; \
                 name the config key that takes `${{stream}}` (e.g. `table_id: \"${{stream}}\"`)",
                self.name
            )));
        }
        let mentions_stream = self
            .per_stream
            .values()
            .any(|v| value_mentions(v, "${stream}"));
        if !mentions_stream {
            return Err(CliError::Config(format!(
                "sink-template '{}': no `per_stream` value uses `${{stream}}`, so streams would collide",
                self.name
            )));
        }
        for k in ["write_mode", "key"] {
            if self.per_stream.contains_key(k) {
                return Err(CliError::Config(format!(
                    "sink-template '{}': `per_stream.{k}` is injected by the composer — remove it",
                    self.name
                )));
            }
        }
        for (from, to) in &self.write_mode_aliases {
            let from_mode = parse_mode(from).ok_or_else(|| {
                CliError::Config(format!(
                    "sink-template '{}': `write_mode_aliases` key '{from}' is not a write mode (append|upsert|delete|overwrite)",
                    self.name
                ))
            })?;
            if matches!(from_mode, WriteMode::Upsert | WriteMode::Delete) {
                return Err(CliError::Config(format!(
                    "sink-template '{}': `{from}` cannot be aliased — a keyed mode is only satisfied by a sink that dedups by key",
                    self.name
                )));
            }
            if from_mode == *to {
                return Err(CliError::Config(format!(
                    "sink-template '{}': `write_mode_aliases.{from}` maps a mode to itself",
                    self.name
                )));
            }
        }
        check_param_refs(
            &self.name,
            &self.params,
            &[
                serde_json::to_value(&self.sink).unwrap_or(Value::Null),
                serde_json::to_value(&self.auth).unwrap_or(Value::Null),
                serde_json::to_value(&self.per_stream).unwrap_or(Value::Null),
            ],
        )?;
        Ok(())
    }
}

/// Parse a write-mode name (`append`, `upsert`, `delete`, `overwrite`).
pub fn parse_mode(name: &str) -> Option<WriteMode> {
    serde_json::from_value(Value::String(name.to_string())).ok()
}

/// The keys a deployment overlay may set (#679), besides its own metadata.
/// Everything here is operational: none of it changes which connectors run or
/// what the streams produce, so a composed run's shape is fixed by its two
/// templates and the overlay only decides how it is operated.
pub const DEPLOYMENT_BLOCKS: &[&str] = &[
    "state",
    "dlq",
    "notifications",
    "sla",
    "profiling",
    "policy",
    "resilience",
    "execution",
    "delivery",
    "schedule",
];

/// Per-stream operational overrides in a deployment overlay.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StreamOverlay {
    /// Replaces the deployment's `sla:` for this stream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sla: Option<Value>,
    /// Replaces the deployment's `dlq:` for this stream; `null` turns it off.
    #[serde(
        default,
        deserialize_with = "present_or_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub dlq: Option<Option<Value>>,
    /// Replaces the deployment's `delivery:` for this stream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery: Option<Value>,
}

/// Keep an explicit `null` distinct from an absent key: `Some(None)` vs `None`.
fn present_or_null<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Option<Option<Value>>, D::Error> {
    Ok(Some(Option::<Value>::deserialize(d)?))
}

impl StreamOverlay {
    fn is_empty(&self) -> bool {
        self.sla.is_none() && self.dlq.is_none() && self.delivery.is_none()
    }
}

/// A `kind: deployment` overlay (#679): the blocks that belong to neither the
/// source template (published for everyone, so it cannot name *your* state
/// store) nor the sink template (a destination, not an operations policy),
/// applied last over a composition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeploymentTemplate {
    /// Must be `deployment`.
    pub kind: TemplateKind,
    #[serde(default = "default_version")]
    pub version: u32,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    /// Parameters the operational blocks reference (a state-store DSN, a
    /// webhook URL). Merged with the templates' parameters; a name declared on
    /// both sides must be declared identically.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub params: ParamsSpec,
    /// → `pipeline.state` of the composed run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<Value>,
    /// → `pipeline.dlq`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dlq: Option<Value>,
    /// → top-level `notifications`.
    #[serde(default, alias = "notify", skip_serializing_if = "Option::is_none")]
    pub notifications: Option<Value>,
    /// → top-level `sla`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sla: Option<Value>,
    /// → top-level `profiling` (#708).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profiling: Option<Value>,
    /// → top-level `policy` (#702).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<Value>,
    /// → top-level `resilience`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resilience: Option<Value>,
    /// → top-level `execution`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution: Option<Value>,
    /// → top-level `delivery`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery: Option<Value>,
    /// → top-level `schedule` (read by `faucet schedule`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<Value>,
    /// Per-stream overrides, keyed by the source template's stream name.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub streams: BTreeMap<String, StreamOverlay>,
}

impl DeploymentTemplate {
    /// `owner/name`, or `name` for an unscoped overlay.
    pub fn id(&self) -> String {
        hub_id(self.owner.as_deref(), &self.name)
    }

    /// Parse an untyped document, explaining a refused key in terms of what an
    /// overlay may set rather than serde's bare "unknown field".
    pub fn from_value(value: Value) -> CliResult<Self> {
        if let Some(obj) = value.as_object() {
            const META: &[&str] = &[
                "kind",
                "version",
                "name",
                "owner",
                "description",
                "tags",
                "params",
                "streams",
                "notify",
            ];
            let refused: Vec<&str> = obj
                .keys()
                .map(String::as_str)
                .filter(|k| !META.contains(k) && !DEPLOYMENT_BLOCKS.contains(k))
                .collect();
            if !refused.is_empty() {
                return Err(CliError::Config(format!(
                    "deployment overlay: `{}` cannot be set here — an overlay decides how a composed run is operated, never which connectors run or what the streams produce. It may set: {} (and per-stream `sla` / `dlq` / `delivery` under `streams:`)",
                    refused.join("`, `"),
                    DEPLOYMENT_BLOCKS.join(", ")
                )));
            }
        }
        let t: Self = serde_json::from_value(value)
            .map_err(|e| CliError::Config(format!("deployment overlay: {e}")))?;
        t.validate()?;
        Ok(t)
    }

    pub fn validate(&self) -> CliResult<()> {
        if self.kind != TemplateKind::Deployment {
            return Err(CliError::Config(format!(
                "'{}' is a {}, not a deployment",
                self.name,
                self.kind.as_str()
            )));
        }
        if self.version != 1 {
            return Err(CliError::Config(format!(
                "deployment '{}': unsupported version {} (expected 1)",
                self.name, self.version
            )));
        }
        check_slug("deployment name", &self.name, true)?;
        if let Some(o) = &self.owner {
            check_slug("deployment owner", o, true)?;
        }
        crate::params::spec::validate(&self.params)?;
        for (name, o) in &self.streams {
            check_slug("deployment stream", name, false)?;
            if o.is_empty() {
                return Err(CliError::Config(format!(
                    "deployment '{}': `streams.{name}` overrides nothing — set `sla`, `dlq`, or `delivery`, or drop the entry",
                    self.name
                )));
            }
        }
        let mut refs = Vec::new();
        for (_, v) in self.blocks() {
            param_refs(v, &mut refs);
        }
        for o in self.streams.values() {
            for v in [
                o.sla.as_ref(),
                o.dlq.as_ref().and_then(Option::as_ref),
                o.delivery.as_ref(),
            ]
            .into_iter()
            .flatten()
            {
                param_refs(v, &mut refs);
            }
        }
        for r in refs {
            if !self.params.contains_key(&r) {
                return Err(CliError::Config(format!(
                    "deployment '{}': `${{param.{r}}}` is referenced but not declared under `params:`",
                    self.name
                )));
            }
        }
        Ok(())
    }

    /// The top-level operational blocks this overlay sets, by config key.
    pub fn blocks(&self) -> Vec<(&'static str, &Value)> {
        [
            ("state", &self.state),
            ("dlq", &self.dlq),
            ("notifications", &self.notifications),
            ("sla", &self.sla),
            ("profiling", &self.profiling),
            ("policy", &self.policy),
            ("resilience", &self.resilience),
            ("execution", &self.execution),
            ("delivery", &self.delivery),
            ("schedule", &self.schedule),
        ]
        .into_iter()
        .filter_map(|(k, v)| v.as_ref().map(|v| (k, v)))
        .collect()
    }
}

impl SinkTemplate {
    /// The alias table with parsed keys (validated by [`Self::validate`]).
    pub fn aliases(&self) -> Vec<(WriteMode, WriteMode)> {
        self.write_mode_aliases
            .iter()
            .filter_map(|(k, v)| parse_mode(k).map(|m| (m, *v)))
            .collect()
    }
}

fn value_mentions(v: &Value, needle: &str) -> bool {
    match v {
        Value::String(s) => s.contains(needle),
        Value::Array(a) => a.iter().any(|x| value_mentions(x, needle)),
        Value::Object(o) => o.values().any(|x| value_mentions(x, needle)),
        _ => false,
    }
}

/// Collect every `${param.NAME}` reference in a value tree.
pub fn param_refs(v: &Value, out: &mut Vec<String>) {
    match v {
        Value::String(s) => {
            let mut rest = s.as_str();
            while let Some(i) = rest.find("${param.") {
                let after = &rest[i + "${param.".len()..];
                match after.find('}') {
                    Some(j) => {
                        out.push(after[..j].to_string());
                        rest = &after[j + 1..];
                    }
                    None => break,
                }
            }
        }
        Value::Array(a) => a.iter().for_each(|x| param_refs(x, out)),
        Value::Object(o) => o.values().for_each(|x| param_refs(x, out)),
        _ => {}
    }
}

fn check_param_refs(name: &str, declared: &ParamsSpec, trees: &[Value]) -> CliResult<()> {
    let mut refs = Vec::new();
    for t in trees {
        param_refs(t, &mut refs);
    }
    refs.sort();
    refs.dedup();
    let undeclared: Vec<&str> = refs
        .iter()
        .map(String::as_str)
        .filter(|r| !declared.contains_key(*r))
        .collect();
    if undeclared.is_empty() {
        Ok(())
    } else {
        Err(CliError::Config(format!(
            "'{name}': `${{param.*}}` references undeclared params: {} — add them to `params:`",
            undeclared.join(", ")
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn src_yaml() -> &'static str {
        r#"
kind: source-template
name: ramp
description: Ramp spend platform
tags: [finance]
params:
  client_id: { type: string, required: true, secret: true }
source:
  type: rest
  config:
    base_url: https://api.example.com/v1
    path: /
    auth: { type: bearer, config: { token: "${param.client_id}" } }
transforms:
  - { type: keys_case, config: { mode: snake } }
streams:
  - name: bills
    source: { config: { path: /bills } }
    primary_keys: [id]
    write: [overwrite, upsert]
  - name: users
    source: { config: { path: /users } }
    transforms: [{ type: json_encode, config: { fields: [custom_fields] } }]
"#
    }

    fn sink_yaml() -> &'static str {
        r#"
kind: sink-template
name: bigquery
params:
  project: { type: string, required: true }
sink:
  type: bigquery
  config: { project_id: "${param.project}", dataset_id: raw }
per_stream:
  table_id: "${stream}"
"#
    }

    #[test]
    fn parses_and_validates_the_reference_shapes() {
        let s: SourceTemplate = serde_yaml::from_str(src_yaml()).unwrap();
        s.validate().unwrap();
        assert_eq!(s.stream_names(), vec!["bills", "users"]);
        assert_eq!(
            s.streams[0].write.candidates(),
            vec![WriteMode::Overwrite, WriteMode::Upsert]
        );
        assert_eq!(s.streams[1].write.candidates(), vec![WriteMode::Append]);
        let k: SinkTemplate = serde_yaml::from_str(sink_yaml()).unwrap();
        k.validate().unwrap();
        assert_eq!(k.kind.as_str(), "sink-template");
    }

    #[test]
    fn source_validation_names_every_defect() {
        let base: SourceTemplate = serde_yaml::from_str(src_yaml()).unwrap();
        let err = |mutate: &dyn Fn(&mut SourceTemplate)| {
            let mut t = base.clone();
            mutate(&mut t);
            t.validate().unwrap_err().to_string()
        };
        assert!(err(&|t| t.kind = TemplateKind::SinkTemplate).contains("not a source-template"));
        assert!(err(&|t| t.version = 2).contains("version 2"));
        assert!(err(&|t| t.name = "Ramp!".into()).contains("must match"));
        assert!(err(&|t| t.streams.clear()).contains("`streams` is empty"));
        assert!(err(&|t| t.streams[1].name = "bills".into()).contains("declared twice"));
        assert!(err(&|t| t.streams[0].name = "with-dash".into()).contains("must match"));
        assert!(
            err(&|t| t.streams[0].write =
                WriteChoice::Many(vec![WriteMode::Append, WriteMode::Append]))
            .contains("twice")
        );
        assert!(err(&|t| t.streams[0].write = WriteChoice::Many(vec![])).contains("at least one"));
        assert!(err(&|t| t.streams[0].primary_keys = vec![" ".into()]).contains("empty name"));
        assert!(err(&|t| t.streams[0].source.config = Value::Null).contains("must be a mapping"));
        assert!(err(&|t| t.source.transforms = Some(vec![])).contains("top-level `transforms:`"));
        assert!(err(&|t| t.params.clear()).contains("undeclared params: client_id"));
        assert!(
            err(&|t| t.streams[1].parent = Some("nope".into()))
                .contains("unknown parent stream 'nope'")
        );
        assert!(err(&|t| t.streams[1].parent = Some("users".into())).contains("its own parent"));
        assert!(
            err(&|t| t.streams[1].parent_key = Some("id".into()))
                .contains("`parent_key` needs a `parent`")
        );
        assert!(
            err(&|t| t.streams[1].source.r#ref = Some("reports".into()))
                .contains("not declared under `sources`")
        );
        assert!(
            err(&|t| {
                t.sources.insert("default".into(), t.source.clone());
            })
            .contains("`sources.default` is reserved")
        );
        let mut ok = base.clone();
        ok.sources.insert("reports".into(), base.source.clone());
        ok.streams[1].source.r#ref = Some("reports".into());
        ok.streams[1].parent = Some("bills".into());
        ok.streams[1].parent_key = Some("id".into());
        ok.validate().unwrap();
    }

    #[test]
    fn sink_validation_names_every_defect() {
        let base: SinkTemplate = serde_yaml::from_str(sink_yaml()).unwrap();
        let err = |mutate: &dyn Fn(&mut SinkTemplate)| {
            let mut t = base.clone();
            mutate(&mut t);
            t.validate().unwrap_err().to_string()
        };
        assert!(err(&|t| t.kind = TemplateKind::SourceTemplate).contains("not a sink-template"));
        assert!(err(&|t| t.version = 0).contains("version 0"));
        assert!(err(&|t| t.per_stream.clear()).contains("`per_stream` is empty"));
        assert!(
            err(&|t| {
                t.per_stream.clear();
                t.per_stream.insert("table_id".into(), "fixed".into());
            })
            .contains("no `per_stream` value uses")
        );
        assert!(
            err(&|t| {
                t.per_stream.insert("write_mode".into(), "append".into());
            })
            .contains("per_stream.write_mode")
        );
        assert!(
            err(&|t| {
                t.sink.config["write_mode"] = "append".into();
            })
            .contains("sink.config.write_mode")
        );
        assert!(err(&|t| t.sink.config = Value::Null).contains("must be a mapping"));
        assert!(err(&|t| t.sink.transforms = Some(vec![])).contains("carry no transforms"));
        assert!(err(&|t| t.params.clear()).contains("undeclared params: project"));
        assert!(
            err(&|t| {
                t.write_mode_aliases
                    .insert("truncate".into(), WriteMode::Append);
            })
            .contains("not a write mode")
        );
        assert!(
            err(&|t| {
                t.write_mode_aliases
                    .insert("upsert".into(), WriteMode::Append);
            })
            .contains("cannot be aliased")
        );
        assert!(
            err(&|t| {
                t.write_mode_aliases
                    .insert("append".into(), WriteMode::Append);
            })
            .contains("maps a mode to itself")
        );
        let mut ok = base.clone();
        ok.write_mode_aliases
            .insert("overwrite".into(), WriteMode::Append);
        ok.validate().unwrap();
        assert_eq!(
            ok.aliases(),
            vec![(WriteMode::Overwrite, WriteMode::Append)]
        );
        assert_eq!(parse_mode("overwrite"), Some(WriteMode::Overwrite));
        assert_eq!(parse_mode("nope"), None);
    }

    #[test]
    fn param_ref_scanner_handles_nesting_and_junk() {
        let v = serde_json::json!({
            "a": "${param.x}/${param.y}",
            "b": ["${param.z}", 1, {"c": "${param.x}"}],
            "d": "${param.unterminated",
            "e": "${env:NOT_A_PARAM}"
        });
        let mut refs = Vec::new();
        param_refs(&v, &mut refs);
        refs.sort();
        assert_eq!(refs, vec!["x", "x", "y", "z"]);
    }

    #[test]
    fn write_choice_round_trips_both_forms() {
        let one: WriteChoice = serde_yaml::from_str("overwrite").unwrap();
        assert_eq!(one, WriteChoice::One(WriteMode::Overwrite));
        let many: WriteChoice = serde_yaml::from_str("[upsert, append]").unwrap();
        assert_eq!(
            many.candidates(),
            vec![WriteMode::Upsert, WriteMode::Append]
        );
        assert_eq!(WriteChoice::default().candidates(), vec![WriteMode::Append]);
        assert!(serde_yaml::from_str::<WriteChoice>("truncate").is_err());
    }

    #[test]
    fn a_deployment_overlay_parses_and_refuses_shape_changing_keys() {
        let v: Value = serde_yaml::from_str(
            "kind: deployment\nname: prod\nstate: { type: memory }\nnotify: []\n",
        )
        .unwrap();
        let d = DeploymentTemplate::from_value(v).unwrap();
        assert_eq!(d.id(), "prod");
        assert_eq!(
            d.blocks().iter().map(|(k, _)| *k).collect::<Vec<_>>(),
            vec!["state", "notifications"]
        );
        for bad in [
            "pipeline: {}",
            "matrix: []",
            "source: {}",
            "sink: {}",
            "transforms: []",
        ] {
            let v: Value =
                serde_yaml::from_str(&format!("kind: deployment\nname: x\n{bad}\n")).unwrap();
            let err = DeploymentTemplate::from_value(v).unwrap_err().to_string();
            assert!(
                err.contains("cannot be set here") && err.contains("state, dlq"),
                "{err}"
            );
        }
        let owned: Value =
            serde_yaml::from_str("kind: deployment\nname: x\nowner: acme\nsla: {}\n").unwrap();
        assert_eq!(
            DeploymentTemplate::from_value(owned).unwrap().id(),
            "acme/x"
        );
    }

    #[test]
    fn deployment_validation_rules() {
        let parse = |y: &str| {
            DeploymentTemplate::from_value(serde_yaml::from_str::<Value>(y).unwrap())
                .map(|_| ())
                .unwrap_err()
                .to_string()
        };
        assert!(parse("kind: source-template\nname: x\n").contains("not a deployment"));
        assert!(parse("kind: deployment\nversion: 2\nname: x\n").contains("unsupported version"));
        assert!(parse("kind: deployment\nname: Bad\n").contains("deployment name"));
        assert!(parse("kind: deployment\nname: x\nowner: Bad\n").contains("deployment owner"));
        assert!(
            parse("kind: deployment\nname: x\nstreams:\n  s: {}\n").contains("overrides nothing")
        );
        assert!(
            parse("kind: deployment\nname: x\nstreams:\n  Bad: { delivery: at_least_once }\n")
                .contains("deployment stream")
        );
        assert!(
            parse("kind: deployment\nname: x\nstreams:\n  s: { sla: { x: \"${param.p}\" } }\n")
                .contains("`${param.p}` is referenced but not declared")
        );
        assert!(
            parse("kind: deployment\nname: x\nstate: { url: \"${param.dsn}\" }\n")
                .contains("param.dsn")
        );
        assert!(
            parse("kind: deployment\nname: x\nstreams:\n  s: { bogus: 1 }\n").contains("bogus")
        );
        assert_eq!(
            TemplateKind::parse("deployment"),
            Some(TemplateKind::Deployment)
        );
        assert_eq!(TemplateKind::Deployment.to_string(), "deployment");
        assert!(!TemplateKind::Deployment.is_hub());
    }
}
