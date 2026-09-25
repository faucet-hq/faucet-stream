//! The hub **catalog**: a directory of `source-templates/*.yaml` +
//! `sink-templates/*.yaml`, the source × sink compatibility matrix computed
//! from it, the lint every published template must pass, and the renderers
//! that turn the matrix into the docs page and `index.json`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::{Value, json};

use super::compose::{StreamIncompatibility, StreamPlan, compose, resolve_mode};
use super::spec::{OFFICIAL_OWNER, SinkTemplate, SourceTemplate, hub_id};
use crate::error::{CliError, CliResult};

pub const SOURCE_DIR: &str = "source-templates";
pub const SINK_DIR: &str = "sink-templates";

/// A loaded catalog.
#[derive(Debug, Default, Clone)]
pub struct Catalog {
    pub root: PathBuf,
    pub sources: Vec<(PathBuf, SourceTemplate)>,
    pub sinks: Vec<(PathBuf, SinkTemplate)>,
}

fn is_template_file(p: &Path) -> bool {
    p.extension()
        .and_then(|e| e.to_str())
        .map(str::to_ascii_lowercase)
        .is_some_and(|e| matches!(e.as_str(), "yaml" | "yml" | "json"))
        && !p
            .file_name()
            .and_then(|n| n.to_str())
            // Dotfiles, and `<name>.faucet.yaml` sidecars (stable / deprecated
            // versions, #682 / #691), are not templates.
            .is_some_and(|n| n.starts_with('.') || n.contains(".faucet."))
}

/// Template files directly in `dir` (unscoped templates) plus one level of
/// owner subdirectories (`<dir>/<owner>/<name>.yaml`, #682). Returns
/// `(path, owner)`.
fn list_dir(dir: &Path) -> CliResult<Vec<(PathBuf, Option<String>)>> {
    if !dir.is_dir() {
        return Ok(Vec::new());
    }
    let read = |d: &Path| -> CliResult<Vec<PathBuf>> {
        let mut v: Vec<PathBuf> = std::fs::read_dir(d)
            .map_err(|e| CliError::Config(format!("reading {}: {e}", d.display())))?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .collect();
        v.sort();
        Ok(v)
    };
    let entries = read(dir)?;
    let mut out: Vec<(PathBuf, Option<String>)> = entries
        .iter()
        .filter(|p| p.is_file() && is_template_file(p))
        .map(|p| (p.clone(), None))
        .collect();
    for p in &entries {
        if p.is_dir()
            && let Some(owner) = p.file_name().and_then(|n| n.to_str())
            && !owner.starts_with('.')
        {
            for f in read(p)? {
                if f.is_file() && is_template_file(&f) {
                    out.push((f, Some(owner.to_string())));
                }
            }
        }
    }
    Ok(out)
}

/// A bare name also addresses the hub's official namespace (`faucet-hq/<name>`).
fn official_alias(id: &str) -> Option<String> {
    (!id.contains('/')).then(|| hub_id(Some(OFFICIAL_OWNER), id))
}

impl Catalog {
    /// Load every template under `root/source-templates` and
    /// `root/sink-templates`. Each file is parsed **and validated**; the first
    /// broken file fails the load with its path, because a catalog with one
    /// bad template must not be published.
    pub fn load(root: &Path) -> CliResult<Self> {
        let mut cat = Self {
            root: root.to_path_buf(),
            ..Default::default()
        };
        for (p, owner) in list_dir(&root.join(SOURCE_DIR))? {
            let t = super::parse_source_file(&p)?;
            check_owner(&p, t.owner.as_deref(), owner.as_deref())?;
            cat.sources.push((p, t));
        }
        for (p, owner) in list_dir(&root.join(SINK_DIR))? {
            let t = super::parse_sink_file(&p)?;
            check_owner(&p, t.owner.as_deref(), owner.as_deref())?;
            cat.sinks.push((p, t));
        }
        if cat.sources.is_empty() && cat.sinks.is_empty() {
            return Err(CliError::Config(format!(
                "no hub templates under {} (expected {SOURCE_DIR}/ and {SINK_DIR}/ with *.yaml files)",
                root.display()
            )));
        }
        // Ids must be unique and match the file stem, so `--source <id>`
        // resolves unambiguously to one file.
        let mut seen = BTreeMap::new();
        for (p, t) in &cat.sources {
            check_stem(p, &t.name, &t.id(), &mut seen)?;
        }
        seen.clear();
        for (p, t) in &cat.sinks {
            check_stem(p, &t.name, &t.id(), &mut seen)?;
        }
        Ok(cat)
    }

    /// Look a source template up by hub id (`owner/name`, or `name`).
    pub fn source(&self, id: &str) -> Option<&SourceTemplate> {
        let find = |want: &str| self.sources.iter().find(|(_, t)| t.id() == want);
        find(id)
            .or_else(|| official_alias(id).and_then(|a| find(&a)))
            .map(|(_, t)| t)
    }

    /// Look a sink template up by hub id (`owner/name`, or `name`).
    pub fn sink(&self, id: &str) -> Option<&SinkTemplate> {
        let find = |want: &str| self.sinks.iter().find(|(_, t)| t.id() == want);
        find(id)
            .or_else(|| official_alias(id).and_then(|a| find(&a)))
            .map(|(_, t)| t)
    }

    /// Compatibility of every source against every sink, in catalog order.
    pub fn matrix(&self) -> Vec<Cell> {
        let mut cells = Vec::with_capacity(self.sources.len() * self.sinks.len());
        for (_, s) in &self.sources {
            for (_, k) in &self.sinks {
                cells.push(cell(s, k));
            }
        }
        cells
    }
}

fn check_stem(
    p: &Path,
    name: &str,
    id: &str,
    seen: &mut BTreeMap<String, PathBuf>,
) -> CliResult<()> {
    let stem = p.file_stem().and_then(|s| s.to_str()).unwrap_or_default();
    if stem != name {
        return Err(CliError::Config(format!(
            "{}: file stem '{stem}' must equal the template's `name: {name}` so `--source/--sink {id}` finds it",
            p.display()
        )));
    }
    if let Some(prev) = seen.insert(id.to_string(), p.to_path_buf()) {
        return Err(CliError::Config(format!(
            "template '{id}' is defined twice: {} and {}",
            prev.display(),
            p.display()
        )));
    }
    Ok(())
}

/// The `owner:` field must agree with the directory a template lives in (#682):
/// a file under `<owner>/` carries `owner: <owner>`; a top-level (unscoped)
/// file carries none. The field is what makes a template self-describing once
/// it leaves the catalog (registry, sync, remote fetch).
fn check_owner(p: &Path, declared: Option<&str>, dir: Option<&str>) -> CliResult<()> {
    match (declared, dir) {
        (None, None) => Ok(()),
        (Some(d), Some(dir)) if d == dir => Ok(()),
        (Some(d), Some(dir)) => Err(CliError::Config(format!(
            "{}: `owner: {d}` but the file lives under '{dir}/' — the two must match",
            p.display()
        ))),
        (Some(d), None) => Err(CliError::Config(format!(
            "{}: declares `owner: {d}` but lives at the top level — move it to '{d}/{}'",
            p.display(),
            p.file_name().and_then(|n| n.to_str()).unwrap_or_default()
        ))),
        (None, Some(dir)) => Err(CliError::Config(format!(
            "{}: lives under '{dir}/' but has no `owner:` — add `owner: {dir}` so the template stays owned once it leaves the catalog",
            p.display()
        ))),
    }
}

/// One source × sink cell of the matrix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Cell {
    pub source: String,
    pub sink: String,
    pub sink_kind: String,
    /// Every stream resolved to a mode the sink supports.
    pub compatible: bool,
    /// Per-stream resolution (the compatible streams).
    pub streams: Vec<StreamPlan>,
    /// Streams with no viable mode.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub incompatible: Vec<StreamIncompatibility>,
}

/// Compute one cell without building the document — the matrix over a large
/// catalog stays cheap, and a partially compatible pairing still reports
/// which streams would work.
pub fn cell(source: &SourceTemplate, sink: &SinkTemplate) -> Cell {
    let supported = crate::registry::sink_supported_write_modes(&sink.sink.kind);
    let aliases = sink.aliases();
    let mut streams = Vec::new();
    let mut incompatible = Vec::new();
    for s in &source.streams {
        match resolve_mode(s, &sink.sink.kind, supported, &aliases) {
            Ok(p) => streams.push(p),
            Err(e) => incompatible.push(e),
        }
    }
    Cell {
        source: source.id(),
        sink: sink.id(),
        sink_kind: sink.sink.kind.clone(),
        compatible: incompatible.is_empty(),
        streams,
        incompatible,
    }
}

/// Fully compose every compatible pair (what the catalog test does to prove
/// each published pairing parses as a `PipelineConfig`).
pub fn compose_all(cat: &Catalog) -> Vec<(String, String, CliResult<super::compose::Composition>)> {
    let mut out = Vec::new();
    for (_, s) in &cat.sources {
        for (_, k) in &cat.sinks {
            out.push((s.id(), k.id(), compose(s, k)));
        }
    }
    out
}

// ── lint ────────────────────────────────────────────────────────────────────

/// Config keys whose literal value would be a leaked credential.
const SECRET_KEYS: &[&str] = &[
    "token",
    "secret",
    "password",
    "passwd",
    "api_key",
    "apikey",
    "private_key",
    "client_secret",
    "access_key",
    "secret_key",
    "consumer_secret",
    "token_secret",
    "json",
    "credentials",
];

/// A value that is not a credential even though it sits under a secret-ish
/// key: empty, a `${…}` reference, a JSONPath capture (`$.access_token` in a
/// login-flow `capture:` block says *where to read* a token), or a one-/two-
/// character constant (BambooHR's documented `password: x`), which cannot be
/// a real secret.
fn looks_like_reference(s: &str) -> bool {
    s.is_empty()
        || (s.starts_with("${") && s.ends_with('}'))
        || s.starts_with("$.")
        || s.starts_with("$[")
        || s.chars().count() <= 2
}

fn walk_secrets(path: &str, v: &Value, findings: &mut Vec<String>) {
    match v {
        Value::Object(o) => {
            for (k, x) in o {
                let child = if path.is_empty() {
                    k.clone()
                } else {
                    format!("{path}.{k}")
                };
                let lk = k.to_ascii_lowercase();
                if SECRET_KEYS
                    .iter()
                    .any(|s| lk == *s || lk.ends_with(&format!("_{s}")))
                    && let Value::String(val) = x
                    && !looks_like_reference(val)
                {
                    findings.push(format!(
                        "`{child}` holds a literal value — credentials must be `${{param.NAME}}` / `${{env:NAME}}` / `${{secret:NAME}}`"
                    ));
                }
                walk_secrets(&child, x, findings);
            }
        }
        Value::Array(a) => {
            for (i, x) in a.iter().enumerate() {
                walk_secrets(&format!("{path}[{i}]"), x, findings);
            }
        }
        _ => {}
    }
}

/// Markers of private infrastructure that must never ship in a public
/// template: hostnames of managed databases, private-registry paths, and
/// the like. A template is config for *someone else's* deployment.
const PRIVATE_MARKERS: &[&str] = &[
    ".rds.amazonaws.com",
    ".internal",
    "localhost:",
    "127.0.0.1",
    "REPLACE_ME",
];

fn walk_markers(path: &str, v: &Value, findings: &mut Vec<String>) {
    match v {
        Value::String(s) => {
            for m in PRIVATE_MARKERS {
                if s.contains(m) {
                    findings.push(format!("`{path}` contains '{m}' — a public template must not point at private infrastructure or placeholders"));
                }
            }
        }
        Value::Object(o) => {
            for (k, x) in o {
                let child = if path.is_empty() {
                    k.clone()
                } else {
                    format!("{path}.{k}")
                };
                walk_markers(&child, x, findings);
            }
        }
        Value::Array(a) => {
            for (i, x) in a.iter().enumerate() {
                walk_markers(&format!("{path}[{i}]"), x, findings);
            }
        }
        _ => {}
    }
}

/// Publishability lint shared by both kinds. Structural validity is
/// [`SourceTemplate::validate`] / [`SinkTemplate::validate`]; this is the
/// policy layer: no literal credentials, no private infrastructure, a
/// description, and every `secret: true` param actually marked.
pub fn lint_source(t: &SourceTemplate) -> Vec<String> {
    let mut f = Vec::new();
    if t.description
        .as_deref()
        .map(str::trim)
        .unwrap_or_default()
        .is_empty()
    {
        f.push("missing `description`".into());
    }
    let v = serde_json::to_value(t).unwrap_or(Value::Null);
    walk_secrets("source", &v["source"], &mut f);
    walk_secrets("auth", &v["auth"], &mut f);
    walk_markers("source", &v["source"], &mut f);
    walk_markers("auth", &v["auth"], &mut f);
    walk_markers("streams", &v["streams"], &mut f);
    lint_params(&t.params, &mut f);
    f
}

pub fn lint_sink(t: &SinkTemplate) -> Vec<String> {
    let mut f = Vec::new();
    if t.description
        .as_deref()
        .map(str::trim)
        .unwrap_or_default()
        .is_empty()
    {
        f.push("missing `description`".into());
    }
    let v = serde_json::to_value(t).unwrap_or(Value::Null);
    walk_secrets("sink", &v["sink"], &mut f);
    walk_secrets("auth", &v["auth"], &mut f);
    walk_markers("sink", &v["sink"], &mut f);
    walk_markers("auth", &v["auth"], &mut f);
    walk_markers("per_stream", &v["per_stream"], &mut f);
    lint_params(&t.params, &mut f);
    f
}

/// Lint a deployment overlay (#679). An overlay names private infrastructure by
/// design (your state store, your webhook), so only credentials are checked:
/// they must arrive as `${param.*}` / `${env:}` / `${secret:}`, never as
/// literals — including a password embedded in a connection URL.
pub fn lint_deployment(t: &crate::hub::DeploymentTemplate) -> Vec<String> {
    let mut f = Vec::new();
    if t.description
        .as_deref()
        .map(str::trim)
        .unwrap_or_default()
        .is_empty()
    {
        f.push("missing `description`".into());
    }
    let v = serde_json::to_value(t).unwrap_or(Value::Null);
    for key in crate::hub::spec::DEPLOYMENT_BLOCKS
        .iter()
        .chain(["streams"].iter())
    {
        walk_secrets(key, &v[*key], &mut f);
        walk_url_passwords(key, &v[*key], &mut f);
    }
    lint_params(&t.params, &mut f);
    f
}

/// `scheme://user:password@host` with a literal password.
fn walk_url_passwords(path: &str, v: &Value, findings: &mut Vec<String>) {
    match v {
        Value::String(s) => {
            if let Some((_, rest)) = s.split_once("://")
                && let Some((userinfo, _)) = rest.split_once('@')
                && let Some((_, pass)) = userinfo.split_once(':')
                && !pass.is_empty()
                && !pass.contains("${")
            {
                findings.push(format!(
                    "`{path}` embeds a literal password in a URL — use `${{param.NAME}}` / `${{env:NAME}}` / `${{secret:NAME}}`"
                ));
            }
        }
        Value::Object(o) => {
            for (k, x) in o {
                walk_url_passwords(&format!("{path}.{k}"), x, findings);
            }
        }
        Value::Array(a) => {
            for (i, x) in a.iter().enumerate() {
                walk_url_passwords(&format!("{path}[{i}]"), x, findings);
            }
        }
        _ => {}
    }
}

fn lint_params(params: &crate::params::ParamsSpec, f: &mut Vec<String>) {
    for (name, p) in params {
        let ln = name.to_ascii_lowercase();
        let secret_ish = SECRET_KEYS
            .iter()
            .any(|s| ln == *s || ln.ends_with(&format!("_{s}")));
        if secret_ish && !p.secret {
            f.push(format!(
                "param `{name}` looks like a credential but is not `secret: true`"
            ));
        }
        if p.secret
            && p.default
                .as_ref()
                .is_some_and(|d| !d.as_str().is_some_and(str::is_empty))
        {
            f.push(format!(
                "secret param `{name}` has a non-empty default — a baked-in credential"
            ));
        }
    }
}

/// Lint the whole catalog; returns `(template, findings)` for every template
/// with at least one finding.
pub fn lint_catalog(cat: &Catalog) -> Vec<(String, Vec<String>)> {
    let mut out = Vec::new();
    for (_, s) in &cat.sources {
        let f = lint_source(s);
        if !f.is_empty() {
            out.push((format!("source-template {}", s.id()), f));
        }
    }
    for (_, k) in &cat.sinks {
        let f = lint_sink(k);
        if !f.is_empty() {
            out.push((format!("sink-template {}", k.id()), f));
        }
    }
    out
}

// ── renderers ───────────────────────────────────────────────────────────────

/// The copy-paste command for one pairing: every required param listed with
/// a `<placeholder>`, secrets pointed at an environment variable.
pub fn run_command(source: &SourceTemplate, sink: &SinkTemplate) -> String {
    let mut cmd = format!("faucet run --source {} --sink {}", source.id(), sink.id());
    let mut params: Vec<(&String, &crate::params::ParamSpec)> = source.params.iter().collect();
    params.extend(sink.params.iter());
    for (name, p) in params {
        if !p.required {
            continue;
        }
        if p.secret {
            cmd.push_str(&format!(
                " \\\n  --param {name}=\"${}\"",
                name.to_ascii_uppercase()
            ));
        } else {
            cmd.push_str(&format!(" \\\n  --param {name}=<{name}>"));
        }
    }
    cmd
}

/// Rebuild every object in `v` with its keys in sorted order, so the emitted
/// JSON is byte-identical whether `serde_json` was compiled with
/// `preserve_order` (feature-unified in by other crates under
/// `--all-features`) or not. The committed `index.json` depends on it.
pub fn sort_keys(v: Value) -> Value {
    match v {
        Value::Object(o) => {
            let mut entries: Vec<(String, Value)> = o.into_iter().collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            let mut out = serde_json::Map::new();
            for (k, x) in entries {
                out.insert(k, sort_keys(x));
            }
            Value::Object(out)
        }
        Value::Array(a) => Value::Array(a.into_iter().map(sort_keys).collect()),
        other => other,
    }
}

/// Machine-readable index of the catalog (what a website consumes). Keys are
/// sorted at every level (see [`sort_keys`]).
pub fn index_json(cat: &Catalog) -> Value {
    index_json_with(cat, run_command)
}

/// `index_json` with a caller-chosen per-pairing command renderer — the
/// registry uses `faucet template run … --sink …` where a directory hub uses
/// `faucet run --source … --sink …`.
pub fn index_json_with(
    cat: &Catalog,
    command: fn(&SourceTemplate, &SinkTemplate) -> String,
) -> Value {
    sort_keys(index_json_unsorted(cat, command))
}

/// The copy-paste command for a pairing held in a template registry.
pub fn registry_run_command(source: &SourceTemplate, sink: &SinkTemplate) -> String {
    run_command(source, sink).replacen(
        &format!("faucet run --source {} --sink {}", source.id(), sink.id()),
        &format!("faucet template run {} --sink {}", source.id(), sink.id()),
        1,
    )
}

fn index_json_unsorted(
    cat: &Catalog,
    command: fn(&SourceTemplate, &SinkTemplate) -> String,
) -> Value {
    let cells = cat.matrix();
    json!({
        "version": 1,
        "sources": cat.sources.iter().map(|(p, s)| json!({
            "id": s.id(),
            "owner": s.owner,
            "official": s.is_official(),
            "name": s.name,
            "description": s.description,
            "tags": s.tags,
            "docs": s.docs,
            "source_type": s.source.kind,
            "file": rel(&cat.root, p),
            "streams": s.streams.iter().map(|st| json!({
                "name": st.name,
                "description": st.description,
                "write": st.write.candidates().iter().map(|m| m.as_str()).collect::<Vec<_>>(),
                "primary_keys": st.primary_keys,
            })).collect::<Vec<_>>(),
            "params": s.params.iter().map(|(n, p)| json!({
                "name": n, "type": p.kind, "required": p.required, "secret": p.secret,
                "description": p.description, "default": p.default,
            })).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
        "sinks": cat.sinks.iter().map(|(p, k)| json!({
            "id": k.id(),
            "owner": k.owner,
            "official": k.is_official(),
            "name": k.name,
            "description": k.description,
            "tags": k.tags,
            "docs": k.docs,
            "sink_type": k.sink.kind,
            "file": rel(&cat.root, p),
            "write_modes": crate::registry::sink_supported_write_modes(&k.sink.kind).iter().map(|m| m.as_str()).collect::<Vec<_>>(),
            "params": k.params.iter().map(|(n, p)| json!({
                "name": n, "type": p.kind, "required": p.required, "secret": p.secret,
                "description": p.description, "default": p.default,
            })).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
        "matrix": cells.iter().map(|c| json!({
            "source": c.source,
            "sink": c.sink,
            "compatible": c.compatible,
            "streams": c.streams.iter().map(|p| json!({"stream": p.stream, "write_mode": p.chosen.as_str(), "satisfies": p.satisfies.map(|m| m.as_str())})).collect::<Vec<_>>(),
            "incompatible": c.incompatible,
            "command": cat.source(&c.source).zip(cat.sink(&c.sink)).map(|(s, k)| command(s, k)),
        })).collect::<Vec<_>>(),
    })
}

fn rel(root: &Path, p: &Path) -> String {
    p.strip_prefix(root)
        .unwrap_or(p)
        .to_string_lossy()
        .replace('\\', "/")
}

/// The docs-site page: the matrix as a table, then one section per source
/// with its streams and a copy-paste command per compatible sink.
/// Markdown heading anchor for a hub id (`acme/netsuite` → `acme-netsuite`).
fn anchor(id: &str) -> String {
    id.replace('/', "-")
}

pub fn render_markdown(cat: &Catalog) -> String {
    let cells = cat.matrix();
    let mut md = String::new();
    md.push_str("# Template Hub — source × sink matrix\n\n");
    md.push_str(
        "<!-- GENERATED from hub/ by `cargo test -p faucet-cli --test hub_catalog -- --ignored regenerate`; do not edit by hand. -->\n\n",
    );
    md.push_str(&format!(
        "{} source templates × {} sink templates. ✓ = every stream has a write mode the sink supports; \
         ◐ = some streams do; — = none. Each source section below carries the copy-paste command for every compatible sink.\n\n",
        cat.sources.len(),
        cat.sinks.len()
    ));
    // Table.
    md.push_str("| source \\ sink |");
    for (_, k) in &cat.sinks {
        md.push_str(&format!(" [{}](#sink-{}) |", k.id(), anchor(&k.id())));
    }
    md.push_str("\n|---|");
    for _ in &cat.sinks {
        md.push_str(":---:|");
    }
    md.push('\n');
    for (_, s) in &cat.sources {
        md.push_str(&format!("| [{}](#{}) |", s.id(), anchor(&s.id())));
        for (_, k) in &cat.sinks {
            let c = cells
                .iter()
                .find(|c| c.source == s.id() && c.sink == k.id())
                .expect("cell");
            let mark = if c.compatible {
                "✓"
            } else if c.streams.is_empty() {
                "—"
            } else {
                "◐"
            };
            md.push_str(&format!(" {mark} |"));
        }
        md.push('\n');
    }
    md.push('\n');

    md.push_str("## Sinks\n\n");
    for (_, k) in &cat.sinks {
        md.push_str(&format!(
            "### sink: {}\n\n<a id=\"sink-{}\"></a>{}\n\n- connector: `{}` · write modes: {}\n",
            k.id(),
            anchor(&k.id()),
            k.description.as_deref().unwrap_or(""),
            k.sink.kind,
            crate::registry::sink_supported_write_modes(&k.sink.kind)
                .iter()
                .map(|m| format!("`{}`", m.as_str()))
                .collect::<Vec<_>>()
                .join(", ")
        ));
        if !k.write_mode_aliases.is_empty() {
            md.push_str(&format!(
                "- satisfies by construction: {}\n",
                k.aliases()
                    .iter()
                    .map(|(f, t)| format!("`{}`→`{}`", f.as_str(), t.as_str()))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        render_params(&mut md, &k.params);
        md.push('\n');
    }

    md.push_str("## Sources\n\n");
    for (_, s) in &cat.sources {
        md.push_str(&format!(
            "### {}\n\n<a id=\"{}\"></a>{}\n\n",
            s.id(),
            anchor(&s.id()),
            s.description.as_deref().unwrap_or("")
        ));
        if !s.tags.is_empty() {
            md.push_str(&format!(
                "- tags: {}\n",
                s.tags
                    .iter()
                    .map(|t| format!("`{t}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        md.push_str(&format!(
            "- connector: `{}` · {} stream(s)\n",
            s.source.kind,
            s.streams.len()
        ));
        if let Some(d) = &s.docs {
            md.push_str(&format!("- upstream docs: <{d}>\n"));
        }
        render_params(&mut md, &s.params);
        md.push_str("\n| stream | write (preference) | primary keys |\n|---|---|---|\n");
        for st in &s.streams {
            md.push_str(&format!(
                "| `{}` | {} | {} |\n",
                st.name,
                st.write
                    .candidates()
                    .iter()
                    .map(|m| format!("`{}`", m.as_str()))
                    .collect::<Vec<_>>()
                    .join(" → "),
                if st.primary_keys.is_empty() {
                    "—".to_string()
                } else {
                    st.primary_keys
                        .iter()
                        .map(|k| format!("`{k}`"))
                        .collect::<Vec<_>>()
                        .join(", ")
                }
            ));
        }
        md.push('\n');
        for (_, k) in &cat.sinks {
            let c = cells
                .iter()
                .find(|c| c.source == s.id() && c.sink == k.id())
                .expect("cell");
            if c.compatible {
                let aliased: Vec<String> = c
                    .streams
                    .iter()
                    .filter(|p| p.satisfies.is_some())
                    .map(|p| format!("`{}` {}", p.stream, p.describe()))
                    .collect();
                md.push_str(&format!(
                    "**→ {}**{}\n\n```bash\n{}\n```\n\n",
                    k.id(),
                    if aliased.is_empty() {
                        String::new()
                    } else {
                        format!(
                            " — {} stream(s) run through an alias: {}",
                            aliased.len(),
                            aliased.join(", ")
                        )
                    },
                    run_command(s, k)
                ));
            } else {
                md.push_str(&format!("**→ {}** — incompatible:\n\n", k.id()));
                for i in &c.incompatible {
                    md.push_str(&format!("- `{}`: {}\n", i.stream, i.reason));
                }
                md.push('\n');
            }
        }
    }
    md
}

fn render_params(md: &mut String, params: &crate::params::ParamsSpec) {
    if params.is_empty() {
        return;
    }
    md.push_str("- params:\n");
    for (n, p) in params {
        let mut bits = Vec::new();
        if p.required {
            bits.push("required".to_string());
        }
        if p.secret {
            bits.push("secret".to_string());
        }
        if let Some(d) = &p.default {
            bits.push(format!("default `{d}`"));
        }
        md.push_str(&format!(
            "  - `{n}`{}{}\n",
            if bits.is_empty() {
                String::new()
            } else {
                format!(" ({})", bits.join(", "))
            },
            p.description
                .as_deref()
                .map(|d| format!(" — {d}"))
                .unwrap_or_default()
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SRC: &str = r#"
kind: source-template
name: acme
description: Acme billing API
tags: [finance]
params:
  api_token: { type: string, required: true, secret: true }
source:
  type: rest
  config: { base_url: https://api.acme.example/v1, path: /, auth: { type: bearer, config: { token: "${param.api_token}" } } }
streams:
  - name: invoices
    source: { config: { path: /invoices } }
    primary_keys: [id]
    write: [overwrite, upsert]
  - name: events
    source: { config: { path: /events } }
"#;
    const BQ: &str = r#"
kind: sink-template
name: bigquery
description: Google BigQuery
params:
  bq_project: { type: string, required: true }
sink:
  type: bigquery
  config: { project_id: "${param.bq_project}", dataset_id: raw }
per_stream:
  table_id: "${stream}"
"#;
    const JSONL: &str = r#"
kind: sink-template
name: jsonl
description: Local JSON Lines files
params:
  out_dir: { type: string, default: ./out }
sink:
  type: jsonl
  config: {}
per_stream:
  path: "${param.out_dir}/${source}/${stream}.jsonl"
"#;

    fn write_catalog(dir: &Path) {
        std::fs::create_dir_all(dir.join(SOURCE_DIR)).unwrap();
        std::fs::create_dir_all(dir.join(SINK_DIR)).unwrap();
        std::fs::write(dir.join(SOURCE_DIR).join("acme.yaml"), SRC).unwrap();
        std::fs::write(dir.join(SINK_DIR).join("bigquery.yaml"), BQ).unwrap();
        std::fs::write(dir.join(SINK_DIR).join("jsonl.yaml"), JSONL).unwrap();
        std::fs::write(dir.join(SINK_DIR).join("README.md"), "ignored").unwrap();
        std::fs::write(dir.join(SINK_DIR).join(".hidden.yaml"), "ignored: true").unwrap();
    }

    #[test]
    fn loads_matrix_and_renders() {
        let dir = tempfile::tempdir().unwrap();
        write_catalog(dir.path());
        let cat = Catalog::load(dir.path()).unwrap();
        assert_eq!(cat.sources.len(), 1);
        assert_eq!(cat.sinks.len(), 2);
        assert!(
            cat.source("acme").is_some()
                && cat.sink("jsonl").is_some()
                && cat.sink("nope").is_none()
        );

        let cells = cat.matrix();
        assert_eq!(cells.len(), 2);
        let bq = cells.iter().find(|c| c.sink == "bigquery").unwrap();
        assert!(bq.compatible);
        assert_eq!(bq.streams.len(), 2);
        let jl = cells.iter().find(|c| c.sink == "jsonl").unwrap();
        assert!(!jl.compatible, "invoices needs overwrite|upsert");
        assert_eq!(jl.streams.len(), 1, "events still resolves");
        assert_eq!(jl.incompatible[0].stream, "invoices");

        let all = compose_all(&cat);
        assert_eq!(all.len(), 2);
        assert!(
            all.iter()
                .find(|(_, k, _)| k == "bigquery")
                .unwrap()
                .2
                .is_ok()
        );
        assert!(
            all.iter()
                .find(|(_, k, _)| k == "jsonl")
                .unwrap()
                .2
                .is_err()
        );

        let cmd = run_command(cat.source("acme").unwrap(), cat.sink("bigquery").unwrap());
        assert!(
            cmd.starts_with("faucet run --source acme --sink bigquery"),
            "{cmd}"
        );
        assert!(cmd.contains("--param api_token=\"$API_TOKEN\""), "{cmd}");
        assert!(cmd.contains("--param bq_project=<bq_project>"), "{cmd}");
        assert!(
            !cmd.contains("out_dir"),
            "optional params are not in the command"
        );

        let md = render_markdown(&cat);
        assert!(md.contains("| [acme](#acme) | ✓ | ◐ |"), "{md}");
        assert!(md.contains("### sink: bigquery"));
        assert!(md.contains("**→ jsonl** — incompatible:"));
        assert!(
            md.contains("- `invoices`: needs overwrite|upsert; sink 'jsonl' supports only append")
        );
        assert!(md.contains("| `invoices` | `overwrite` → `upsert` | `id` |"));
        assert!(md.contains("`api_token` (required, secret)"));

        let idx = index_json(&cat);
        assert_eq!(idx["version"], 1);
        assert_eq!(idx["sources"][0]["file"], "source-templates/acme.yaml");
        assert_eq!(
            idx["sources"][0]["streams"][0]["write"],
            json!(["overwrite", "upsert"])
        );
        assert_eq!(
            idx["sinks"][0]["write_modes"],
            json!(["append", "upsert", "delete", "overwrite"])
        );
        let m = idx["matrix"].as_array().unwrap();
        assert_eq!(m.len(), 2);
        assert_eq!(m[0]["compatible"], true);
        assert!(
            m[0]["command"]
                .as_str()
                .unwrap()
                .contains("--sink bigquery")
        );
        assert_eq!(m[1]["incompatible"][0]["stream"], "invoices");
    }

    #[test]
    fn index_json_is_key_order_canonical() {
        let dir = tempfile::tempdir().unwrap();
        write_catalog(dir.path());
        let cat = Catalog::load(dir.path()).unwrap();
        let idx = index_json(&cat);
        fn assert_sorted(v: &Value, path: &str) {
            match v {
                Value::Object(o) => {
                    let keys: Vec<&String> = o.keys().collect();
                    let mut sorted = keys.clone();
                    sorted.sort();
                    assert_eq!(keys, sorted, "keys out of order at {path}");
                    for (k, x) in o {
                        assert_sorted(x, &format!("{path}.{k}"));
                    }
                }
                Value::Array(a) => a
                    .iter()
                    .enumerate()
                    .for_each(|(i, x)| assert_sorted(x, &format!("{path}[{i}]"))),
                _ => {}
            }
        }
        assert_sorted(&idx, "$");
        assert_eq!(
            sort_keys(json!({"b": 1, "a": {"z": [ {"y": 1, "x": 2} ], "c": 3}})).to_string(),
            r#"{"a":{"c":3,"z":[{"x":2,"y":1}]},"b":1}"#
        );
    }

    #[test]
    fn bare_names_alias_the_official_namespace() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path();
        std::fs::create_dir_all(root.join("source-templates/faucet-hq")).unwrap();
        std::fs::create_dir_all(root.join("sink-templates/faucet-hq")).unwrap();
        std::fs::write(
            root.join("source-templates/faucet-hq/shop.yaml"),
            "kind: source-template\nname: shop\nowner: faucet-hq\ndescription: d\nsource: {type: csv, config: {path: ./x.csv}}\nstreams: [{name: t}]\n",
        )
        .unwrap();
        std::fs::write(
            root.join("sink-templates/faucet-hq/lake.yaml"),
            "kind: sink-template\nname: lake\nowner: faucet-hq\ndescription: d\nsink: {type: jsonl, config: {}}\nper_stream: {path: \"./lake/${stream}.jsonl\"}\n",
        )
        .unwrap();
        let cat = Catalog::load(root).unwrap();
        assert!(cat.source("shop").is_some_and(|s| s.is_official()));
        assert!(cat.sink("lake").is_some_and(|k| k.is_official()));
        assert!(cat.sink("faucet-hq/lake").is_some());
        assert!(cat.source("missing").is_none() && cat.sink("octo/lake").is_none());
        let idx = index_json(&cat);
        assert_eq!(idx["sources"][0]["official"], serde_json::json!(true));
        assert_eq!(idx["sinks"][0]["official"], serde_json::json!(true));
        assert_eq!(idx["sinks"][0]["id"], serde_json::json!("faucet-hq/lake"));
    }

    #[test]
    fn owner_directories_load_and_owner_must_match_the_directory() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path();
        std::fs::create_dir_all(root.join("source-templates/octo")).unwrap();
        std::fs::create_dir_all(root.join("sink-templates")).unwrap();
        let src = |owner: &str| {
            format!(
                "kind: source-template\nname: shop\n{owner}description: d\nsource: {{type: csv, config: {{path: ./x.csv}}}}\nstreams: [{{name: t}}]\n"
            )
        };
        std::fs::write(root.join("source-templates/shop.yaml"), src("")).unwrap();
        std::fs::write(
            root.join("source-templates/octo/shop.yaml"),
            src("owner: octo\n"),
        )
        .unwrap();
        std::fs::write(
            root.join("sink-templates/files.yaml"),
            "kind: sink-template\nname: files\ndescription: d\nsink: {type: jsonl, config: {}}\nper_stream: {path: \"./out/${owner}/${source}/${stream}.jsonl\"}\n",
        )
        .unwrap();
        let cat = Catalog::load(root).expect("two namespaces, same short name");
        let ids: Vec<String> = cat.sources.iter().map(|(_, s)| s.id()).collect();
        assert_eq!(ids, ["shop", "octo/shop"]);
        assert!(cat.source("octo/shop").unwrap().owner.as_deref() == Some("octo"));
        let idx = index_json(&cat);
        assert_eq!(idx["sources"][0]["official"], serde_json::json!(false));
        assert_eq!(idx["sources"][1]["official"], serde_json::json!(false));
        assert_eq!(idx["sources"][1]["id"], serde_json::json!("octo/shop"));
        assert_eq!(idx["sources"][1]["owner"], serde_json::json!("octo"));
        assert_eq!(idx["matrix"][1]["source"], serde_json::json!("octo/shop"));
        assert!(
            idx["matrix"][1]["command"]
                .as_str()
                .unwrap()
                .starts_with("faucet run --source octo/shop --sink files")
        );
        let md = render_markdown(&cat);
        assert!(md.contains("[octo/shop](#octo-shop)"), "{md}");
        // `${owner}` renders in per-stream addressing; `${source}` stays the short name.
        let c = compose(cat.source("octo/shop").unwrap(), cat.sink("files").unwrap()).unwrap();
        assert_eq!(
            c.document["matrix"][0]["sink"]["config"]["path"],
            serde_json::json!("./out/octo/shop/t.jsonl")
        );
        let c = compose(cat.source("shop").unwrap(), cat.sink("files").unwrap()).unwrap();
        assert_eq!(
            c.document["matrix"][0]["sink"]["config"]["path"],
            serde_json::json!("./out//shop/t.jsonl")
        );

        // Owner declared but at the top level.
        std::fs::write(
            root.join("source-templates/stray.yaml"),
            src("owner: octo\n").replace("name: shop", "name: stray"),
        )
        .unwrap();
        let err = Catalog::load(root).unwrap_err().to_string();
        assert!(
            err.contains("lives at the top level") && err.contains("octo/stray.yaml"),
            "{err}"
        );
        std::fs::remove_file(root.join("source-templates/stray.yaml")).unwrap();
        // Under an owner directory without the field.
        std::fs::write(
            root.join("source-templates/octo/bare.yaml"),
            src("").replace("name: shop", "name: bare"),
        )
        .unwrap();
        let err = Catalog::load(root).unwrap_err().to_string();
        assert!(
            err.contains("has no `owner:`") && err.contains("add `owner: octo`"),
            "{err}"
        );
        std::fs::remove_file(root.join("source-templates/octo/bare.yaml")).unwrap();
        // Owner disagreeing with the directory.
        std::fs::write(
            root.join("source-templates/octo/other.yaml"),
            src("owner: someone\n").replace("name: shop", "name: other"),
        )
        .unwrap();
        let err = Catalog::load(root).unwrap_err().to_string();
        assert!(
            err.contains("`owner: someone` but the file lives under 'octo/'"),
            "{err}"
        );
    }

    #[test]
    fn sidecars_beside_templates_are_not_loaded_as_templates() {
        assert!(!is_template_file(Path::new(
            "sink-templates/acme/x.faucet.yaml"
        )));
        assert!(!is_template_file(Path::new(
            "sink-templates/acme/.hidden.yaml"
        )));
        assert!(is_template_file(Path::new("sink-templates/acme/x.yaml")));
        assert!(!is_template_file(Path::new("sink-templates/acme/OWNERS")));
    }

    #[test]
    fn load_rejects_stem_mismatch_duplicates_and_empty_dirs() {
        let dir = tempfile::tempdir().unwrap();
        assert!(
            Catalog::load(dir.path())
                .unwrap_err()
                .to_string()
                .contains("no hub templates")
        );
        write_catalog(dir.path());
        std::fs::write(dir.path().join(SOURCE_DIR).join("other.yaml"), SRC).unwrap();
        let err = Catalog::load(dir.path()).unwrap_err().to_string();
        assert!(err.contains("file stem 'other' must equal"), "{err}");
        std::fs::remove_file(dir.path().join(SOURCE_DIR).join("other.yaml")).unwrap();
        std::fs::write(dir.path().join(SOURCE_DIR).join("acme.yml"), SRC).unwrap();
        let err = Catalog::load(dir.path()).unwrap_err().to_string();
        assert!(err.contains("defined twice"), "{err}");
        std::fs::write(
            dir.path().join(SOURCE_DIR).join("acme.yml"),
            "kind: source-template\nname: acme\n",
        )
        .unwrap();
        assert!(
            Catalog::load(dir.path()).is_err(),
            "a broken file fails the load"
        );
    }

    #[test]
    fn lint_catches_literal_credentials_markers_and_unmarked_secret_params() {
        let dir = tempfile::tempdir().unwrap();
        write_catalog(dir.path());
        let cat = Catalog::load(dir.path()).unwrap();
        assert!(lint_catalog(&cat).is_empty(), "{:?}", lint_catalog(&cat));

        let mut s = cat.source("acme").unwrap().clone();
        s.description = None;
        s.source.config["auth"]["config"]["token"] = json!("sk-live-123");
        s.source.config["base_url"] = json!("https://db.internal/x");
        s.params.insert(
            "db_password".into(),
            crate::params::ParamSpec {
                kind: Default::default(),
                required: true,
                default: None,
                secret: false,
                description: None,
                values: vec![],
                computed: None,
            },
        );
        s.params.get_mut("api_token").unwrap().default = Some(json!("baked"));
        let f = lint_source(&s);
        let joined = f.join("\n");
        assert!(joined.contains("missing `description`"), "{joined}");
        assert!(
            joined.contains("`source.config.auth.config.token` holds a literal value"),
            "{joined}"
        );
        assert!(joined.contains("contains '.internal'"), "{joined}");
        assert!(
            joined.contains("param `db_password` looks like a credential"),
            "{joined}"
        );
        assert!(
            joined.contains("secret param `api_token` has a non-empty default"),
            "{joined}"
        );

        let mut k = cat.sink("bigquery").unwrap().clone();
        k.sink.config["auth"] = json!({"type": "service_account_key", "config": {"json": "{\"private_key\": \"...\"}"}});
        k.per_stream
            .insert("dataset_id".into(), json!("REPLACE_ME"));
        let f = lint_sink(&k).join("\n");
        assert!(
            f.contains("`sink.config.auth.config.json` holds a literal value"),
            "{f}"
        );
        assert!(f.contains("REPLACE_ME"), "{f}");
        // Empty strings, references, JSONPath captures, and tiny constants are fine.
        k.sink.config["auth"]["config"]["json"] = json!("${param.sa_key}");
        k.per_stream.remove("dataset_id");
        assert!(lint_sink(&k).is_empty(), "{:?}", lint_sink(&k));
        let mut s = cat.source("acme").unwrap().clone();
        s.auth = Some(
            [("flow".to_string(), json!({"type": "login_flow", "config": {"steps": [{"capture": {"access_token": "$.access_token"}}]}}))]
                .into_iter()
                .collect(),
        );
        s.source.config["auth"]["config"]["password"] = json!("x");
        assert!(lint_source(&s).is_empty(), "{:?}", lint_source(&s));
    }

    #[test]
    fn a_deployment_lint_flags_literal_credentials_only() {
        let d = |y: &str| {
            crate::hub::DeploymentTemplate::from_value(serde_yaml::from_str(y).unwrap()).unwrap()
        };
        let ok = d(r#"
kind: deployment
name: prod
description: Production operations
params: { dsn: { type: string, secret: true } }
state: { type: postgres, config: { url: "${param.dsn}" } }
notifications: [{ name: ops, channel: { type: webhook, config: { url: "https://hooks.internal.example/x" } } }]
"#);
        assert!(
            lint_deployment(&ok).is_empty(),
            "{:?}",
            lint_deployment(&ok)
        );
        let bad = d(r#"
kind: deployment
name: prod
params: { password: { type: string } }
state: { type: postgres, config: { url: "postgres://app:hunter2@db:5432/x" } }
dlq: { sink: { type: http, config: { token: "abc123" } } }
streams: { s: { dlq: { sink: { type: http, config: { url: "https://u:p@h/x" } } } } }
"#);
        let f = lint_deployment(&bad);
        assert!(
            f.iter().any(|x| x.contains("missing `description`")),
            "{f:?}"
        );
        assert!(
            f.iter()
                .any(|x| x.contains("state.config.url") && x.contains("literal password")),
            "{f:?}"
        );
        assert!(
            f.iter().any(|x| x.contains("dlq.sink.config.token")),
            "{f:?}"
        );
        assert!(f.iter().any(|x| x.contains("streams.s.dlq")), "{f:?}");
        assert!(f.iter().any(|x| x.contains("param `password`")), "{f:?}");
    }
}
