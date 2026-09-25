//! Pure composition: `source-template × sink-template → PipelineConfig`
//! document (#571).
//!
//! The output is an ordinary config document — `pipeline.sources.default`,
//! `pipeline.sinks.default`, the source's shared `transforms`, and one matrix
//! row per stream carrying the stream's source override, its own transforms,
//! and the **resolved write mode** for the chosen sink. Everything downstream
//! (params binding, `expand`, the write-mode × sink gate, the run) is the
//! existing path, so a composed pipeline inherits every guarantee a
//! hand-written one has.
//!
//! Write-mode resolution is the load-bearing step: each stream lists the
//! modes it accepts in preference order; the first the sink supports wins.
//! `upsert` / `delete` additionally need `primary_keys`, which become the
//! sink's `key`. A stream with no viable mode is a **per-stream** error naming
//! both sides — the matrix cell is "incompatible", not "silently append".

use std::collections::BTreeMap;

use faucet_core::WriteMode;
use serde::Serialize;
use serde_json::{Map, Value, json};

use super::spec::{DEFAULT_SOURCE, DeploymentTemplate, SinkTemplate, SourceTemplate, Stream};
use crate::error::{CliError, CliResult};

/// The write mode resolved for one stream against one sink.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StreamPlan {
    pub stream: String,
    /// The stream's declared preference list.
    pub requested: Vec<WriteMode>,
    /// The mode the sink will run with.
    pub chosen: WriteMode,
    /// When the sink template satisfies the stream's mode by construction
    /// (`write_mode_aliases`), the stream mode `chosen` stands in for.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub satisfies: Option<WriteMode>,
    /// The sink `key` (only for `upsert` / `delete`).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub key: Vec<String>,
}

impl StreamPlan {
    /// `overwrite` or `overwrite→append`, for reports.
    pub fn describe(&self) -> String {
        match self.satisfies {
            Some(s) => format!("{}→{}", s.as_str(), self.chosen.as_str()),
            None => self.chosen.as_str().to_string(),
        }
    }
}

/// One composed pipeline plus the per-stream decisions behind it.
#[derive(Debug, Clone, Serialize)]
pub struct Composition {
    /// Pipeline `name:` — the source template's name, so state keys are
    /// `{source}::{stream}` regardless of sink.
    pub name: String,
    pub source: String,
    pub sink: String,
    pub sink_kind: String,
    pub streams: Vec<StreamPlan>,
    /// The deployment overlay applied over the pairing (#679), by id.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub overlay: Option<String>,
    /// What the overlay set, as config paths (`pipeline.state`,
    /// `matrix.orders.dlq`) — the plan output shows it.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub overlay_contributes: Vec<String>,
    /// Operator-facing warnings about the composed run.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    /// Whether any stream reads incrementally ([`has_incremental_stream`]).
    #[serde(skip)]
    pub incremental: bool,
    /// Which hub each side came from, when composed from hubs (#696).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_hub: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sink_hub: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub overlay_hub: Option<String>,
    /// The composed `PipelineConfig` document (JSON value; serialize as YAML
    /// for humans).
    pub document: Value,
}

impl Composition {
    /// The document as YAML text (what `faucet hub compose` prints), headed by
    /// a comment naming the hub each side came from.
    pub fn to_yaml(&self) -> CliResult<String> {
        let body = serde_yaml::to_string(&self.document)
            .map_err(|e| CliError::Internal(format!("hub compose: rendering YAML: {e}")))?;
        let mut head = String::new();
        for (what, id, hub) in [
            ("source", Some(&self.source), &self.source_hub),
            ("sink", Some(&self.sink), &self.sink_hub),
            ("overlay", self.overlay.as_ref(), &self.overlay_hub),
        ] {
            if let (Some(id), Some(hub)) = (id, hub) {
                head.push_str(&format!("# {what}: {id} from {hub}\n"));
            }
        }
        Ok(head + &body)
    }
}

/// Why a stream cannot run against a sink.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StreamIncompatibility {
    pub stream: String,
    pub reason: String,
}

/// The per-stream failures of one source × sink pairing, aggregated so an
/// operator sees every problem at once rather than one per run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Incompatible {
    pub source: String,
    pub sink: String,
    pub streams: Vec<StreamIncompatibility>,
}

impl Incompatible {
    pub fn render(&self) -> String {
        let mut s = format!(
            "source-template '{}' cannot compose with sink-template '{}' — {} stream(s) have no viable write mode:",
            self.source,
            self.sink,
            self.streams.len()
        );
        for i in &self.streams {
            s.push_str(&format!("\n  - {}: {}", i.stream, i.reason));
        }
        s
    }
}

fn mode_list(modes: &[WriteMode]) -> String {
    modes
        .iter()
        .map(|m| m.as_str())
        .collect::<Vec<_>>()
        .join("|")
}

/// Pick the first requested mode the sink supports — natively, or through
/// one of the sink template's `write_mode_aliases`. `upsert` / `delete`
/// require `primary_keys`; listing one without keys is an error rather than
/// a silent fall-through, because it is almost always a template mistake.
pub fn resolve_mode(
    stream: &Stream,
    sink_kind: &str,
    supported: &[WriteMode],
    aliases: &[(WriteMode, WriteMode)],
) -> Result<StreamPlan, StreamIncompatibility> {
    let requested = stream.write.candidates();
    for m in &requested {
        if matches!(m, WriteMode::Upsert | WriteMode::Delete) && stream.primary_keys.is_empty() {
            return Err(StreamIncompatibility {
                stream: stream.name.clone(),
                reason: format!(
                    "`write` lists {} but the stream declares no `primary_keys`",
                    m.as_str()
                ),
            });
        }
    }
    let mut resolved: Option<(WriteMode, Option<WriteMode>)> = None;
    for m in &requested {
        if supported.contains(m) {
            resolved = Some((*m, None));
            break;
        }
        if let Some((_, to)) = aliases
            .iter()
            .find(|(from, to)| from == m && supported.contains(to))
        {
            resolved = Some((*to, Some(*m)));
            break;
        }
    }
    match resolved {
        Some((chosen, satisfies)) => Ok(StreamPlan {
            stream: stream.name.clone(),
            requested,
            chosen,
            satisfies,
            key: if matches!(chosen, WriteMode::Upsert | WriteMode::Delete) {
                stream.primary_keys.clone()
            } else {
                Vec::new()
            },
        }),
        None => Err(StreamIncompatibility {
            stream: stream.name.clone(),
            reason: format!(
                "needs {}; sink '{sink_kind}' supports only {}{}",
                mode_list(&requested),
                mode_list(supported),
                if aliases.is_empty() {
                    String::new()
                } else {
                    format!(
                        " (aliases: {})",
                        aliases
                            .iter()
                            .map(|(f, t)| format!("{}→{}", f.as_str(), t.as_str()))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                }
            ),
        }),
    }
}

/// Replace `${stream}` / `${source}` in every string of a value tree.
pub fn render_per_stream(v: &Value, stream: &str, source: &str) -> Value {
    render_per_stream_owned(v, stream, source, "")
}

/// [`render_per_stream`] with the source template's `owner` for `${owner}`
/// (empty for an unscoped template). `${source}` stays the short name so a
/// destination table name never receives a `/`.
pub fn render_per_stream_owned(v: &Value, stream: &str, source: &str, owner: &str) -> Value {
    match v {
        Value::String(s) => Value::String(
            s.replace("${stream}", stream)
                .replace("${source}", source)
                .replace("${owner}", owner),
        ),
        Value::Array(a) => Value::Array(
            a.iter()
                .map(|x| render_per_stream_owned(x, stream, source, owner))
                .collect(),
        ),
        Value::Object(o) => Value::Object(
            o.iter()
                .map(|(k, x)| (k.clone(), render_per_stream_owned(x, stream, source, owner)))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// Merge two `params:` (or `auth:`) maps. The same name declared by both
/// sides must have an identical spec — otherwise the two templates disagree
/// about what the operator is supplying, and guessing would bind one side
/// wrong.
fn merge_named<T: Serialize + Clone>(
    what: &str,
    a: &BTreeMap<String, T>,
    b: &BTreeMap<String, T>,
    a_owner: &str,
    b_owner: &str,
) -> CliResult<BTreeMap<String, T>> {
    let mut out = a.clone();
    for (k, v) in b {
        match out.get(k) {
            None => {
                out.insert(k.clone(), v.clone());
            }
            Some(existing) => {
                let same = serde_json::to_value(existing).ok() == serde_json::to_value(v).ok();
                if !same {
                    return Err(CliError::Config(format!(
                        "{what} '{k}' is declared by both '{a_owner}' and '{b_owner}' with different specs — \
                         rename one (e.g. prefix the sink's with `sink_`)"
                    )));
                }
            }
        }
    }
    Ok(out)
}

fn merge_auth(
    a: &Option<std::collections::HashMap<String, Value>>,
    b: &Option<std::collections::HashMap<String, Value>>,
    a_owner: &str,
    b_owner: &str,
) -> CliResult<Option<Map<String, Value>>> {
    let to_btree =
        |m: &Option<std::collections::HashMap<String, Value>>| -> BTreeMap<String, Value> {
            m.as_ref()
                .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                .unwrap_or_default()
        };
    let merged = merge_named(
        "auth provider",
        &to_btree(a),
        &to_btree(b),
        a_owner,
        b_owner,
    )?;
    if merged.is_empty() {
        Ok(None)
    } else {
        Ok(Some(merged.into_iter().collect()))
    }
}

/// Compose with the sink's write-mode capabilities looked up from the
/// connector registry.
pub fn compose(source: &SourceTemplate, sink: &SinkTemplate) -> CliResult<Composition> {
    compose_with(
        source,
        sink,
        crate::registry::sink_supported_write_modes(&sink.sink.kind),
    )
}

/// [`compose`] with an explicit capability list (the registry's, or a test's).
pub fn compose_with(
    source: &SourceTemplate,
    sink: &SinkTemplate,
    supported: &[WriteMode],
) -> CliResult<Composition> {
    source.validate()?;
    sink.validate()?;

    // Resolve every stream first so the error lists all of them at once.
    let aliases = sink.aliases();
    for (from, to) in &aliases {
        if supported.contains(from) {
            return Err(CliError::Config(format!(
                "sink-template '{}': `write_mode_aliases.{}` is redundant — sink '{}' supports it natively",
                sink.name,
                from.as_str(),
                sink.sink.kind
            )));
        }
        if !supported.contains(to) {
            return Err(CliError::Config(format!(
                "sink-template '{}': `write_mode_aliases.{}` maps to `{}`, which sink '{}' does not support (only {})",
                sink.name,
                from.as_str(),
                to.as_str(),
                sink.sink.kind,
                mode_list(supported)
            )));
        }
    }
    let mut plans = Vec::with_capacity(source.streams.len());
    let mut failures = Vec::new();
    for s in &source.streams {
        match resolve_mode(s, &sink.sink.kind, supported, &aliases) {
            Ok(p) => plans.push(p),
            Err(e) => failures.push(e),
        }
    }
    if !failures.is_empty() {
        return Err(CliError::Config(
            Incompatible {
                source: source.id(),
                sink: sink.id(),
                streams: failures,
            }
            .render(),
        ));
    }

    let params = merge_named(
        "param",
        &source.params,
        &sink.params,
        &source.name,
        &sink.name,
    )?;
    let auth = merge_auth(&source.auth, &sink.auth, &source.name, &sink.name)?;

    // A sink without a `WriteSpec` (jsonl, csv, stdout, …) rejects an unknown
    // `write_mode` key under strict config validation, so the key is only
    // written for sinks that understand it.
    let sink_takes_write_mode = supported.len() > 1;

    let mut rows = Vec::with_capacity(source.streams.len());
    for (s, plan) in source.streams.iter().zip(&plans) {
        let mut sink_cfg = Map::new();
        for (k, v) in &sink.per_stream {
            sink_cfg.insert(
                k.clone(),
                render_per_stream_owned(
                    v,
                    &s.name,
                    &source.name,
                    source.owner.as_deref().unwrap_or(""),
                ),
            );
        }
        if sink_takes_write_mode {
            sink_cfg.insert(
                "write_mode".into(),
                Value::String(plan.chosen.as_str().into()),
            );
        }
        if !plan.key.is_empty() {
            sink_cfg.insert("key".into(), json!(plan.key));
        }
        let mut row = Map::new();
        row.insert("id".into(), Value::String(s.name.clone()));
        if let Some(p) = &s.parent {
            row.insert("parent".into(), Value::String(p.clone()));
        }
        if let Some(k) = &s.parent_key {
            row.insert("parent_key".into(), Value::String(k.clone()));
        }
        if !s.inherit_transforms {
            row.insert("inherit_transforms".into(), Value::Bool(false));
        }
        let mut src_ref = Map::new();
        src_ref.insert(
            "ref".into(),
            Value::String(
                s.source
                    .r#ref
                    .clone()
                    .unwrap_or_else(|| DEFAULT_SOURCE.to_string()),
            ),
        );
        if s.source.config.as_object().is_some_and(|o| !o.is_empty()) {
            src_ref.insert("config".into(), s.source.config.clone());
        }
        row.insert("source".into(), Value::Object(src_ref));
        row.insert(
            "sink".into(),
            json!({ "ref": "default", "config": Value::Object(sink_cfg) }),
        );
        if !s.transforms.is_empty() {
            row.insert(
                "transforms".into(),
                serde_json::to_value(&s.transforms)
                    .map_err(|e| CliError::Internal(format!("hub compose: transforms: {e}")))?,
            );
        }
        rows.push(Value::Object(row));
    }

    let mut pipeline = Map::new();
    let mut sources = Map::new();
    sources.insert(
        DEFAULT_SOURCE.into(),
        serde_json::to_value(&source.source).map_err(internal)?,
    );
    for (name, spec) in &source.sources {
        sources.insert(name.clone(), serde_json::to_value(spec).map_err(internal)?);
    }
    pipeline.insert("sources".into(), Value::Object(sources));
    pipeline.insert(
        "sinks".into(),
        json!({ "default": serde_json::to_value(&sink.sink).map_err(internal)? }),
    );
    if !source.transforms.is_empty() {
        pipeline.insert(
            "transforms".into(),
            serde_json::to_value(&source.transforms).map_err(internal)?,
        );
    }
    if let Some(c) = &source.contract {
        pipeline.insert("contract".into(), c.clone());
    }

    let mut doc = Map::new();
    doc.insert("version".into(), json!(1));
    doc.insert("name".into(), Value::String(source.id()));
    if !params.is_empty() {
        doc.insert(
            "params".into(),
            serde_json::to_value(&params).map_err(internal)?,
        );
    }
    if let Some(a) = auth {
        doc.insert("auth".into(), Value::Object(a));
    }
    doc.insert("pipeline".into(), Value::Object(pipeline));
    doc.insert("matrix".into(), Value::Array(rows));

    let incremental = has_incremental_stream(source);
    let warnings = if incremental {
        vec![format!(
            "'{}' has incremental streams but the composed run has no `state:` block, so they re-read everything each run — apply a deployment overlay (`kind: deployment`) that sets `state:`",
            source.id()
        )]
    } else {
        Vec::new()
    };

    Ok(Composition {
        name: source.id(),
        source: source.id(),
        sink: sink.id(),
        sink_kind: sink.sink.kind.clone(),
        streams: plans,
        overlay: None,
        overlay_contributes: Vec::new(),
        warnings,
        incremental,
        source_hub: None,
        sink_hub: None,
        overlay_hub: None,
        document: Value::Object(doc),
    })
}

/// Whether any stream reads incrementally (`replication_method: Incremental`
/// in the shared source config or a stream override) — such a stream only
/// bookmarks across runs when the run has a state store.
pub fn has_incremental_stream(source: &SourceTemplate) -> bool {
    fn incremental(v: &Value) -> bool {
        match v.get("replication_method") {
            Some(Value::String(s)) => s.eq_ignore_ascii_case("incremental"),
            Some(Value::Object(o)) => o
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|t| t.eq_ignore_ascii_case("incremental")),
            _ => false,
        }
    }
    incremental(&source.source.config)
        || source.sources.values().any(|s| incremental(&s.config))
        || source.streams.iter().any(|s| incremental(&s.source.config))
}

impl Composition {
    /// Apply a deployment overlay (#679) last: its operational blocks replace
    /// whatever the composition carried, and its per-stream overrides land on
    /// the matching matrix rows. Connector selection and stream shape are out
    /// of its reach by construction ([`DeploymentTemplate::from_value`]).
    pub fn apply_overlay(mut self, overlay: &DeploymentTemplate) -> CliResult<Self> {
        overlay.validate()?;
        let doc = self
            .document
            .as_object_mut()
            .ok_or_else(|| CliError::Internal("hub compose: document is not an object".into()))?;

        let theirs: BTreeMap<String, crate::params::ParamSpec> = match doc.get("params") {
            Some(v) => serde_json::from_value(v.clone()).map_err(internal)?,
            None => BTreeMap::new(),
        };
        let params = merge_named(
            "param",
            &theirs,
            &overlay.params,
            &format!("{} × {}", self.source, self.sink),
            &overlay.id(),
        )?;
        if !params.is_empty() {
            doc.insert(
                "params".into(),
                serde_json::to_value(&params).map_err(internal)?,
            );
        }

        let mut contributes = Vec::new();
        for (key, value) in overlay.blocks() {
            match key {
                "state" | "dlq" => {
                    let pipeline = doc
                        .get_mut("pipeline")
                        .and_then(Value::as_object_mut)
                        .ok_or_else(|| {
                            CliError::Internal("hub compose: document has no pipeline".into())
                        })?;
                    pipeline.insert(key.into(), value.clone());
                    contributes.push(format!("pipeline.{key}"));
                }
                _ => {
                    doc.insert(key.into(), value.clone());
                    contributes.push(key.to_string());
                }
            }
        }

        let known: Vec<String> = self.streams.iter().map(|p| p.stream.clone()).collect();
        let rows = doc
            .get_mut("matrix")
            .and_then(Value::as_array_mut)
            .ok_or_else(|| CliError::Internal("hub compose: document has no matrix".into()))?;
        for (stream, o) in &overlay.streams {
            let row = rows
                .iter_mut()
                .find(|r| r.get("id").and_then(Value::as_str) == Some(stream.as_str()))
                .and_then(Value::as_object_mut)
                .ok_or_else(|| {
                    CliError::Config(format!(
                        "deployment '{}': `streams.{stream}` names no stream of '{}' (streams: {})",
                        overlay.id(),
                        self.source,
                        known.join(", ")
                    ))
                })?;
            let dlq = o.dlq.as_ref().map(|v| v.clone().unwrap_or(Value::Null));
            for (key, value) in [
                ("sla", o.sla.clone()),
                ("dlq", dlq),
                ("delivery", o.delivery.clone()),
            ] {
                if let Some(v) = value {
                    row.insert(key.into(), v);
                    contributes.push(format!("matrix.{stream}.{key}"));
                }
            }
        }

        if let Some(state) = &overlay.state {
            self.warnings.retain(|w| !w.contains("no `state:` block"));
            let memory = state.get("type").and_then(Value::as_str) == Some("memory");
            if memory && self.incremental {
                self.warnings.push(format!(
                    "deployment '{}' uses a `memory` state store: bookmarks for '{}''s incremental streams reset when the process exits — use `file`, `redis`, or `postgres` for runs that must resume",
                    overlay.id(),
                    self.source
                ));
            }
        }
        self.overlay = Some(overlay.id());
        self.overlay_contributes = contributes;
        Ok(self)
    }
}

fn internal(e: serde_json::Error) -> CliError {
    CliError::Internal(format!("hub compose: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hub::spec::{StreamSource, TemplateKind, WriteChoice};

    const SRC: &str = r#"
kind: source-template
name: ramp
params:
  token: { type: string, required: true, secret: true }
  shared: { type: string, default: a }
auth:
  ramp_oauth: { type: static, config: { token: "${param.token}" } }
source:
  type: rest
  config:
    base_url: https://api.example.com/v1
    path: /
    auth: { ref: ramp_oauth }
transforms:
  - { type: keys_case, config: { mode: snake } }
contract: { version: "1", fields: [] }
streams:
  - name: bills
    source: { config: { path: /bills } }
    primary_keys: [id]
    write: [overwrite, upsert]
    transforms: [{ type: json_encode, config: { fields: [line_items] } }]
  - name: users
    source: { config: { path: /users } }
"#;

    const BQ: &str = r#"
kind: sink-template
name: bigquery
params:
  project: { type: string, required: true }
  shared: { type: string, default: a }
sink:
  type: bigquery
  config: { project_id: "${param.project}", dataset_id: raw }
per_stream:
  table_id: "${stream}"
"#;

    const JSONL: &str = r#"
kind: sink-template
name: jsonl
params:
  out_dir: { type: string, default: ./out }
sink:
  type: jsonl
  config: { append: false }
per_stream:
  path: "${param.out_dir}/${source}/${stream}.jsonl"
"#;

    fn src() -> SourceTemplate {
        serde_yaml::from_str(SRC).unwrap()
    }

    const ALL: &[WriteMode] = &[
        WriteMode::Append,
        WriteMode::Upsert,
        WriteMode::Delete,
        WriteMode::Overwrite,
    ];

    #[test]
    fn composes_a_full_pipeline_document_for_a_capable_sink() {
        let sink: SinkTemplate = serde_yaml::from_str(BQ).unwrap();
        let c = compose_with(&src(), &sink, ALL).unwrap();
        assert_eq!(
            c.name, "ramp",
            "pipeline name is the source's, so state keys survive a sink swap"
        );
        assert_eq!(c.sink_kind, "bigquery");
        assert_eq!(c.streams[0].chosen, WriteMode::Overwrite);
        assert!(c.streams[0].key.is_empty(), "overwrite is not keyed");
        assert_eq!(c.streams[1].chosen, WriteMode::Append);

        let d = &c.document;
        assert_eq!(d["version"], 1);
        assert_eq!(d["pipeline"]["sources"]["default"]["type"], "rest");
        assert_eq!(
            d["pipeline"]["sinks"]["default"]["config"]["project_id"],
            "${param.project}"
        );
        assert_eq!(d["pipeline"]["transforms"][0]["type"], "keys_case");
        assert_eq!(d["pipeline"]["contract"]["version"], "1");
        assert!(d["auth"]["ramp_oauth"].is_object());
        // Params merged: identical `shared` is fine.
        assert!(d["params"]["token"].is_object() && d["params"]["project"].is_object());
        let rows = d["matrix"].as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["id"], "bills");
        assert_eq!(rows[0]["source"]["ref"], "default");
        assert_eq!(rows[0]["source"]["config"]["path"], "/bills");
        assert_eq!(rows[0]["sink"]["config"]["table_id"], "bills");
        assert_eq!(rows[0]["sink"]["config"]["write_mode"], "overwrite");
        assert!(rows[0]["sink"]["config"].get("key").is_none());
        assert_eq!(rows[0]["transforms"][0]["type"], "json_encode");
        assert_eq!(rows[1]["sink"]["config"]["write_mode"], "append");
        assert!(rows[1].get("transforms").is_none());

        // It is a real PipelineConfig.
        let cfg = crate::config::PipelineConfig::from_value(c.document.clone()).unwrap();
        assert_eq!(cfg.matrix.len(), 2);
        assert!(c.to_yaml().unwrap().contains("table_id: bills"));
    }

    #[test]
    fn parent_ref_and_inherit_flow_into_the_matrix_row() {
        let sink: SinkTemplate = serde_yaml::from_str(BQ).unwrap();
        let mut s = src();
        s.sources.insert("reports".into(), s.source.clone());
        s.streams.push(Stream {
            name: "bill_lines".into(),
            description: None,
            source: StreamSource {
                r#ref: Some("reports".into()),
                config: json!({ "path": "/bills/${bills.id}/lines" }),
            },
            transforms: vec![],
            primary_keys: vec![],
            write: WriteChoice::One(WriteMode::Append),
            parent: Some("bills".into()),
            parent_key: Some("id".into()),
            inherit_transforms: false,
        });
        let c = compose_with(&s, &sink, ALL).unwrap();
        let d = &c.document;
        assert!(d["pipeline"]["sources"]["reports"].is_object());
        let row = &d["matrix"][2];
        assert_eq!(row["id"], "bill_lines");
        assert_eq!(row["parent"], "bills");
        assert_eq!(row["parent_key"], "id");
        assert_eq!(row["inherit_transforms"], false);
        assert_eq!(row["source"]["ref"], "reports");
        assert!(
            d["matrix"][0].get("parent").is_none()
                && d["matrix"][0].get("inherit_transforms").is_none()
        );
        crate::config::PipelineConfig::from_value(c.document.clone()).unwrap();
    }

    /// A file sink rewritten on every run *is* a full refresh: the template
    /// says so with `write_mode_aliases: { overwrite: append }`, and the
    /// composer runs the stream as append while recording what it satisfies.
    #[test]
    fn aliases_satisfy_a_mode_the_connector_lacks_and_are_validated_against_the_registry() {
        let mut sink: SinkTemplate = serde_yaml::from_str(JSONL).unwrap();
        sink.write_mode_aliases
            .insert("overwrite".into(), WriteMode::Append);
        let c = compose_with(&src(), &sink, &[WriteMode::Append]).unwrap();
        assert_eq!(c.streams[0].chosen, WriteMode::Append);
        assert_eq!(c.streams[0].satisfies, Some(WriteMode::Overwrite));
        assert_eq!(c.streams[0].describe(), "overwrite→append");
        assert_eq!(c.streams[1].describe(), "append");
        assert!(
            c.document["matrix"][0]["sink"]["config"]
                .get("write_mode")
                .is_none()
        );
        let bq: SinkTemplate = serde_yaml::from_str(BQ).unwrap();
        let mut bq2 = bq.clone();
        bq2.write_mode_aliases
            .insert("overwrite".into(), WriteMode::Append);
        assert!(
            compose_with(&src(), &bq2, ALL)
                .unwrap_err()
                .to_string()
                .contains("redundant")
        );
        let mut bad: SinkTemplate = serde_yaml::from_str(JSONL).unwrap();
        bad.write_mode_aliases
            .insert("overwrite".into(), WriteMode::Delete);
        let err = compose_with(&src(), &bad, &[WriteMode::Append])
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("maps to `delete`, which sink 'jsonl' does not support"),
            "{err}"
        );
        let mut s = src();
        s.streams[0].write = WriteChoice::One(WriteMode::Upsert);
        let err = compose_with(&s, &sink, &[WriteMode::Append])
            .unwrap_err()
            .to_string();
        assert!(err.contains("(aliases: overwrite→append)"), "{err}");
    }

    #[test]
    fn upsert_fallback_carries_the_key() {
        let sink: SinkTemplate = serde_yaml::from_str(BQ).unwrap();
        let c = compose_with(&src(), &sink, &[WriteMode::Append, WriteMode::Upsert]).unwrap();
        assert_eq!(c.streams[0].chosen, WriteMode::Upsert);
        assert_eq!(c.streams[0].key, vec!["id"]);
        assert_eq!(
            c.document["matrix"][0]["sink"]["config"]["key"],
            json!(["id"])
        );
    }

    /// The matrix cell: an append-only sink cannot take a stream that needs
    /// overwrite|upsert — and the error names both sides, per stream.
    #[test]
    fn incompatible_streams_fail_together_with_both_sides_named() {
        let sink: SinkTemplate = serde_yaml::from_str(JSONL).unwrap();
        let mut s = src();
        s.streams.push(Stream {
            name: "audit".into(),
            description: None,
            source: StreamSource::default(),
            transforms: vec![],
            primary_keys: vec![],
            write: WriteChoice::One(WriteMode::Overwrite),
            parent: None,
            parent_key: None,
            inherit_transforms: true,
        });
        let err = compose_with(&s, &sink, &[WriteMode::Append])
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("cannot compose with sink-template 'jsonl'"),
            "{err}"
        );
        assert!(err.contains("2 stream(s)"), "{err}");
        assert!(
            err.contains("- bills: needs overwrite|upsert; sink 'jsonl' supports only append"),
            "{err}"
        );
        assert!(err.contains("- audit: needs overwrite"), "{err}");
        assert!(
            !err.contains("users"),
            "the compatible stream is not listed"
        );
    }

    #[test]
    fn an_append_only_sink_gets_no_write_mode_key_and_renders_source_and_stream() {
        let sink: SinkTemplate = serde_yaml::from_str(JSONL).unwrap();
        let mut s = src();
        s.streams[0].write = WriteChoice::One(WriteMode::Append);
        let c = compose_with(&s, &sink, &[WriteMode::Append]).unwrap();
        let cfg = &c.document["matrix"][0]["sink"]["config"];
        assert_eq!(cfg["path"], "${param.out_dir}/ramp/bills.jsonl");
        assert!(
            cfg.get("write_mode").is_none(),
            "jsonl has no WriteSpec; the key would be rejected"
        );
        assert_eq!(c.streams[0].chosen, WriteMode::Append);
    }

    #[test]
    fn upsert_without_primary_keys_is_an_error_not_a_fallthrough() {
        let sink: SinkTemplate = serde_yaml::from_str(BQ).unwrap();
        let mut s = src();
        s.streams[0].primary_keys.clear();
        s.streams[0].write = WriteChoice::Many(vec![WriteMode::Upsert, WriteMode::Append]);
        let err = compose_with(&s, &sink, ALL).unwrap_err().to_string();
        assert!(
            err.contains("bills: `write` lists upsert but the stream declares no `primary_keys`"),
            "{err}"
        );
    }

    #[test]
    fn conflicting_params_or_auth_are_refused_identical_ones_merge() {
        let mut sink: SinkTemplate = serde_yaml::from_str(BQ).unwrap();
        sink.params.get_mut("shared").unwrap().default = Some(json!("b"));
        let err = compose_with(&src(), &sink, ALL).unwrap_err().to_string();
        assert!(
            err.contains("param 'shared' is declared by both 'ramp' and 'bigquery'"),
            "{err}"
        );

        let mut sink: SinkTemplate = serde_yaml::from_str(BQ).unwrap();
        sink.auth = Some(
            [(
                "ramp_oauth".to_string(),
                json!({"type": "static", "config": {"token": "other"}}),
            )]
            .into_iter()
            .collect(),
        );
        let err = compose_with(&src(), &sink, ALL).unwrap_err().to_string();
        assert!(err.contains("auth provider 'ramp_oauth'"), "{err}");

        let mut sink: SinkTemplate = serde_yaml::from_str(BQ).unwrap();
        sink.auth = Some(
            [(
                "bq_sa".to_string(),
                json!({"type": "static", "config": {"token": "x"}}),
            )]
            .into_iter()
            .collect(),
        );
        let c = compose_with(&src(), &sink, ALL).unwrap();
        assert!(
            c.document["auth"]["bq_sa"].is_object() && c.document["auth"]["ramp_oauth"].is_object()
        );
    }

    #[test]
    fn invalid_templates_are_rejected_before_composition() {
        let mut sink: SinkTemplate = serde_yaml::from_str(BQ).unwrap();
        sink.kind = TemplateKind::SourceTemplate;
        assert!(compose_with(&src(), &sink, ALL).is_err());
        let mut s = src();
        s.streams.clear();
        let sink: SinkTemplate = serde_yaml::from_str(BQ).unwrap();
        assert!(compose_with(&s, &sink, ALL).is_err());
    }

    #[test]
    fn registry_backed_compose_uses_real_capabilities() {
        // jsonl is append-only in the registry; bills needs overwrite|upsert.
        let sink: SinkTemplate = serde_yaml::from_str(JSONL).unwrap();
        assert!(compose(&src(), &sink).is_err());
        let mut s = src();
        s.streams[0].write = WriteChoice::One(WriteMode::Append);
        assert!(compose(&s, &sink).is_ok());
    }

    #[test]
    fn render_per_stream_touches_only_strings() {
        let v = json!({"a": "${stream}-${source}", "b": ["${stream}", 3], "c": true});
        assert_eq!(
            render_per_stream(&v, "bills", "ramp"),
            json!({"a": "bills-ramp", "b": ["bills", 3], "c": true})
        );
    }

    fn overlay(yaml: &str) -> DeploymentTemplate {
        DeploymentTemplate::from_value(serde_yaml::from_str(yaml).unwrap()).unwrap()
    }

    fn jsonl_pair() -> Composition {
        let k: SinkTemplate = serde_yaml::from_str(BQ).unwrap();
        compose_with(&src(), &k, ALL).unwrap()
    }

    #[test]
    fn an_overlay_places_operational_blocks_and_per_stream_overrides() {
        let o = overlay(
            r#"
kind: deployment
name: prod
params:
  state_dir: { type: string, default: ./state }
state: { type: file, config: { path: "${param.state_dir}" } }
dlq: { sink: { type: jsonl, config: { path: ./dlq.jsonl } } }
notify: [{ name: ops, channel: { type: webhook, config: { url: https://hooks.example/x } } }]
sla: { max_staleness_secs: 3600 }
delivery: at_least_once
execution: { max_concurrent: 2 }
streams:
  bills: { sla: { min_rows_per_run: 1 }, dlq: null }
"#,
        );
        let c = jsonl_pair().apply_overlay(&o).unwrap();
        let d = &c.document;
        assert_eq!(d["pipeline"]["state"]["type"], json!("file"));
        assert_eq!(d["pipeline"]["dlq"]["sink"]["type"], json!("jsonl"));
        assert_eq!(
            d["notifications"][0]["name"],
            json!("ops"),
            "`notify` is an alias"
        );
        assert_eq!(d["sla"]["max_staleness_secs"], json!(3600));
        assert_eq!(d["delivery"], json!("at_least_once"));
        assert_eq!(d["execution"]["max_concurrent"], json!(2));
        assert!(
            d["params"]["state_dir"].is_object(),
            "overlay params merge in"
        );
        assert!(d["params"]["token"].is_object(), "template params survive");
        let bills = d["matrix"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["id"] == "bills")
            .unwrap();
        assert_eq!(bills["sla"]["min_rows_per_run"], json!(1));
        assert!(bills["dlq"].is_null() && bills.as_object().unwrap().contains_key("dlq"));
        assert_eq!(c.overlay.as_deref(), Some("prod"));
        assert_eq!(
            c.overlay_contributes,
            vec![
                "pipeline.state",
                "pipeline.dlq",
                "notifications",
                "sla",
                "execution",
                "delivery",
                "matrix.bills.sla",
                "matrix.bills.dlq",
            ]
        );
        // Connector selection and stream shape are untouched.
        assert_eq!(
            d["pipeline"]["sinks"],
            jsonl_pair().document["pipeline"]["sinks"]
        );
    }

    #[test]
    fn an_overlay_naming_an_unknown_stream_or_clashing_param_is_refused() {
        let err = jsonl_pair()
            .apply_overlay(&overlay(
                "kind: deployment\nname: x\nstreams:\n  nope: { delivery: at_least_once }\n",
            ))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("names no stream") && err.contains("bills"),
            "{err}"
        );
        let err = jsonl_pair()
            .apply_overlay(&overlay(
                "kind: deployment\nname: x\nparams:\n  token: { type: int }\nstate: { type: memory }\n",
            ))
            .unwrap_err()
            .to_string();
        assert!(err.contains("param 'token'"), "{err}");
    }

    #[test]
    fn incremental_streams_warn_without_state_and_with_memory_state() {
        let mut s = src();
        s.streams[0].source.config =
            json!({ "path": "/bills", "replication_method": { "type": "Incremental" } });
        let k: SinkTemplate = serde_yaml::from_str(BQ).unwrap();
        let c = compose_with(&s, &k, ALL).unwrap();
        assert!(c.incremental);
        assert!(
            c.warnings[0].contains("no `state:` block"),
            "{:?}",
            c.warnings
        );
        let with_state = c
            .clone()
            .apply_overlay(&overlay(
                "kind: deployment\nname: x\nstate: { type: file, config: { path: ./s } }\n",
            ))
            .unwrap();
        assert!(with_state.warnings.is_empty(), "{:?}", with_state.warnings);
        let memory = c
            .apply_overlay(&overlay(
                "kind: deployment\nname: x\nstate: { type: memory }\n",
            ))
            .unwrap();
        assert_eq!(memory.warnings.len(), 1);
        assert!(memory.warnings[0].contains("`memory` state store"));
        // No incremental streams: no warning either way.
        let plain = compose_with(&src(), &k, ALL).unwrap();
        assert!(!plain.incremental && plain.warnings.is_empty());
    }

    #[test]
    fn incremental_detection_reads_every_source_config_shape() {
        let mut s = src();
        assert!(!has_incremental_stream(&s));
        s.source.config = json!({ "replication_method": "incremental" });
        assert!(has_incremental_stream(&s));
        let mut s = src();
        s.sources.insert(
            "other".into(),
            serde_json::from_value(json!({ "type": "rest", "config": { "replication_method": { "type": "INCREMENTAL" } } })).unwrap(),
        );
        assert!(has_incremental_stream(&s));
        let mut s = src();
        s.source.config = json!({ "replication_method": { "type": "FullTable" } });
        assert!(!has_incremental_stream(&s));
    }
}
