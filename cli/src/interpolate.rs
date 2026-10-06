//! Two-stage interpolation for pipeline configs.
//!
//! **Load-time** ([`interpolate`]) resolves directives that are knowable
//! before any pipeline has run — environment variables, file contents,
//! secrets. Tokens that don't match one of these prefixes are left literal
//! so the matrix expander can later treat them as `${row_id.field.path}`
//! deferred references.
//!
//! **Record-time** ([`interpolate_record`]) resolves the remaining
//! `${row_id.dotted.path}` tokens against a context map of parent records,
//! producing a string ready to feed into a connector's `Deserialize` impl.
//!
//! Supported load-time directives:
//!
//! | Form               | Resolves to |
//! |--------------------|-------------|
//! | `${env:VAR}`       | the value of environment variable `VAR` |
//! | `${file:PATH}`     | the contents of the file at `PATH` (trimmed) |
//! | `${secret:VAR}`    | alias for `${env:VAR}` (reserved for a future secrets backend) |
//!
//! Anything else (including `${users.id}`, `${posts.author.name}`) is
//! deferred to record-time. A literal `${` is written `$${`.

use crate::error::{CliError, CliResult};
use chrono::{DateTime, FixedOffset};
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;

/// An overlay consulted before the process environment when resolving
/// `${env:VAR}` / `${secret:VAR}`. Populated by a caller-supplied `env:` map
/// (`faucet run --param-env`, `POST /v1/templates/{id}/runs`) so a run can
/// override a variable without mutating the shared process environment — which
/// would be racy in a concurrent server (#444).
pub type EnvOverlay = HashMap<String, String>;

/// Resolve every load-time directive in `input`. Unknown prefixes (and
/// tokens with no `:` at all — i.e. `${row_id.field}` references) survive
/// verbatim for record-time resolution.
pub fn interpolate(input: &str) -> CliResult<String> {
    interpolate_with_env(input, &EnvOverlay::new())
}

/// [`interpolate`] with an [`EnvOverlay`] taking precedence over the process
/// environment for `${env:}` / `${secret:}` lookups.
pub fn interpolate_with_env(input: &str, overlay: &EnvOverlay) -> CliResult<String> {
    rewrite(input, |body| match classify_directive(body) {
        Directive::LoadTime { prefix, body } => match prefix {
            "env" | "secret" => {
                let value = match overlay.get(body) {
                    Some(v) => v.clone(),
                    None => std::env::var(body).map_err(|_| CliError::MissingEnvVar {
                        var: body.to_owned(),
                        location: format!("${{{prefix}:{body}}}"),
                    })?,
                };
                // Register the resolved value for redaction, exactly as the
                // secrets-manager pass does for `${vault:…}` etc. — otherwise a
                // credential supplied via the very common `${env:TOKEN}` /
                // `${secret:VAR}` form leaks into tracing/log/error output while
                // `${vault:…}` ones are scrubbed (an inconsistent boundary,
                // #146 M3). `register` no-ops for values below the min length.
                crate::secrets::registry::register(&value);
                Ok(Some(value))
            }
            "file" => {
                let value = read_file_trimmed(body)?;
                crate::secrets::registry::register(&value);
                Ok(Some(value))
            }
            // Any other ${prefix:body} that isn't env/file/secret — leave it
            // literal so a downstream validator can flag truly bogus prefixes.
            _ => Ok(None),
        },
        Directive::Deferred { .. } => Ok(None),
    })
}

/// Resolve load-time directives (`${env:}` / `${file:}` / `${secret:}`) inside
/// an already-parsed config tree by running [`interpolate`] on every string
/// scalar — object keys and string values, recursively.
///
/// This is the **structure-safe** counterpart to running [`interpolate`] on the
/// raw document text: because substitution happens per-scalar, a resolved
/// value containing markup-significant characters (`:`, newlines, `-`) can never
/// inject a key, an array element, or otherwise alter the document's structure
/// (F43) — it stays the single scalar it was parsed as, exactly like the
/// secrets-manager pass. Deferred `${id.path}` / `${now.*}` tokens survive
/// verbatim for their later resolution stages.
pub fn interpolate_value(value: &mut Value) -> CliResult<()> {
    interpolate_value_with_env(value, &EnvOverlay::new())
}

/// [`interpolate_value`] with an [`EnvOverlay`] taking precedence over the
/// process environment.
pub fn interpolate_value_with_env(value: &mut Value, overlay: &EnvOverlay) -> CliResult<()> {
    match value {
        Value::String(s) => {
            *s = interpolate_with_env(s, overlay)?;
        }
        Value::Array(items) => {
            for item in items {
                interpolate_value_with_env(item, overlay)?;
            }
        }
        Value::Object(map) => {
            // Keys may also carry directives (rare, but supported by the old
            // raw-text pass). Rebuild the map so an interpolated key is honoured.
            let entries: Vec<(String, Value)> = std::mem::take(map).into_iter().collect();
            for (key, mut val) in entries {
                interpolate_value_with_env(&mut val, overlay)?;
                let resolved_key = interpolate_with_env(&key, overlay)?;
                map.insert(resolved_key, val);
            }
        }
        _ => {}
    }
    Ok(())
}

/// Resolve `${id.dotted.path}` tokens against `ctx`. Tokens that look like
/// load-time directives (`${env:...}`, `${file:...}`, `${secret:...}`) are
/// left untouched — they should already have been resolved by [`interpolate`].
///
/// Errors when an `id` is unknown or a dotted path does not resolve.
pub fn interpolate_record(input: &str, ctx: &HashMap<String, Value>) -> CliResult<String> {
    rewrite(input, |body| match classify_directive(body) {
        Directive::LoadTime { .. } => Ok(None),
        Directive::Deferred { id, path } => {
            let record = ctx
                .get(id)
                .ok_or_else(|| CliError::UnknownInterpolationId {
                    id: id.to_owned(),
                    token: format!("${{{body}}}"),
                })?;
            let resolved =
                resolve_dotted(record, path).ok_or_else(|| CliError::MissingRecordField {
                    id: id.to_owned(),
                    path: path.to_owned(),
                })?;
            Ok(Some(value_to_string_record(&resolved)))
        }
    })
}

/// Render a resolved runtime value for substitution into a config field.
///
/// Like [`value_to_string`], but a JSON **array** renders as its scalar elements
/// **comma-joined** (`["a","b","c"]` → `a,b,c`) rather than as JSON. This is the
/// collected-dimension form (#531): a `collect: true` discovery publishes a list
/// per tuple, and `${id.alias}` injects it as one param (e.g. a
/// `?properties=a,b,c` field list). Non-scalar array elements fall back to their JSON form.
pub(crate) fn value_to_string_record(v: &Value) -> String {
    match v {
        Value::Array(items) => items
            .iter()
            .map(value_to_string)
            .collect::<Vec<_>>()
            .join(","),
        other => value_to_string(other),
    }
}

/// Resolve `${now.<token>}` references in `input` against the run clock. Every
/// other `${...}` token (env/file/secret/vars/sources/sinks/row-id) is left
/// verbatim. `now` is a reserved built-in id (see `expand.rs`). An unknown
/// `${now.<bad>}` token is a config error (never silently passed through).
pub fn resolve_now(input: &str, clock: DateTime<FixedOffset>) -> CliResult<String> {
    rewrite(input, |body| {
        if let Directive::Deferred { id: "now", path } = classify_directive(body) {
            return Ok(Some(now_token(path, clock)?));
        }
        Ok(None)
    })
}

/// Render one `${now.<path>}` token. `path` is the text after `now.`.
fn now_token(path: &str, clock: DateTime<FixedOffset>) -> CliResult<String> {
    // Arbitrary chrono strftime via the dot-form `${now.strftime.<fmt>}`.
    if let Some(fmt) = path.strip_prefix("strftime.") {
        // Pre-validate: a bad specifier yields `Item::Error`, and rendering it
        // would panic in `to_string()`. Reject it as a config error instead.
        use chrono::format::{Item, StrftimeItems};
        let items: Vec<Item> = StrftimeItems::new(fmt).collect();
        if items.iter().any(|i| matches!(i, Item::Error)) {
            return Err(CliError::Config(format!(
                "invalid strftime format in `${{now.strftime.{fmt}}}`"
            )));
        }
        return Ok(clock.format_with_items(items.iter()).to_string());
    }
    let rendered = match path {
        "date" => clock.format("%Y-%m-%d").to_string(),
        "datetime" | "iso" => clock.to_rfc3339(),
        "year" => clock.format("%Y").to_string(),
        "month" => clock.format("%m").to_string(),
        "day" => clock.format("%d").to_string(),
        "hour" => clock.format("%H").to_string(),
        "minute" => clock.format("%M").to_string(),
        "second" => clock.format("%S").to_string(),
        "unix" => clock.timestamp().to_string(),
        other => {
            return Err(CliError::Config(format!(
                "unknown `${{now.{other}}}` token — valid: date, datetime, iso, year, month, day, hour, minute, second, unix, strftime.<fmt>"
            )));
        }
    };
    Ok(rendered)
}

/// Walk `input` byte-by-byte, calling `resolve` on each `${...}` body.
/// `resolve` returns `Some(s)` to substitute, or `None` to keep verbatim.
pub(crate) fn rewrite<F>(input: &str, mut resolve: F) -> CliResult<String>
where
    F: FnMut(&str) -> CliResult<Option<String>>,
{
    let mut out = String::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // Escape: `$${` → literal `${`.
        if bytes[i] == b'$' && i + 2 < bytes.len() && bytes[i + 1] == b'$' && bytes[i + 2] == b'{' {
            out.push('$');
            i += 2;
            continue;
        }
        if bytes[i] == b'$' && i + 1 < bytes.len() && bytes[i + 1] == b'{' {
            let start = i + 2;
            let Some(rel_end) = input[start..].find('}') else {
                // Unclosed directive — copy the rest verbatim and stop scanning.
                out.push_str(&input[i..]);
                break;
            };
            let end = start + rel_end;
            let body = &input[start..end];
            match resolve(body)? {
                Some(s) => out.push_str(&s),
                None => out.push_str(&input[i..=end]),
            }
            i = end + 1;
            continue;
        }
        let ch = input[i..].chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }
    Ok(out)
}

/// Classification of a `${...}` directive body. This is the **single** rule
/// shared by load-time interpolation, record-time interpolation, and matrix
/// validation (`expand.rs`) so they can never disagree about what a token
/// means (#78/#39).
///
/// A load-time directive uses a colon (`${env:VAR}`, `${file:./p}`); a
/// deferred reference uses a dot or nothing (`${users.id}`, `${row}`). The
/// colon is checked first, so `${env.foo}` (no colon) is a *deferred*
/// reference to id `env`, not a malformed load-time `env` directive — both
/// the validator and the runtime now treat it identically.
pub enum Directive<'a> {
    /// Prefixed directive like `${env:VAR}` or `${file:./p}` — split on `:`.
    LoadTime { prefix: &'a str, body: &'a str },
    /// No `:` — a `${id.dotted.path}` reference deferred to runtime. `id` is
    /// the text before the first `.`; `path` is the (possibly empty) rest.
    Deferred { id: &'a str, path: &'a str },
}

pub fn classify_directive(body: &str) -> Directive<'_> {
    match body.split_once(':') {
        Some((prefix, rest)) => Directive::LoadTime { prefix, body: rest },
        None => {
            let (id, path) = body.split_once('.').unwrap_or((body, ""));
            Directive::Deferred { id, path }
        }
    }
}

/// Iterate every `${...}` directive in `s`, yielding the full token text
/// (including `${` and `}`) and its [`Directive`] classification. `$${` is an
/// escape and yields nothing; an unterminated `${` ends iteration. This is
/// the shared tokenizer used by `expand.rs` validation so it scans and
/// classifies tokens exactly as `rewrite` does during substitution.
pub fn iter_directives(s: &str) -> impl Iterator<Item = (&str, Directive<'_>)> {
    let bytes = s.as_bytes();
    let mut i = 0;
    std::iter::from_fn(move || {
        while i < bytes.len() {
            // `$${` escape — consume the `$$` (mirrors `rewrite`) and continue.
            if bytes[i] == b'$'
                && i + 2 < bytes.len()
                && bytes[i + 1] == b'$'
                && bytes[i + 2] == b'{'
            {
                i += 2;
                continue;
            }
            if bytes[i] == b'$' && i + 1 < bytes.len() && bytes[i + 1] == b'{' {
                let start = i;
                let body_start = i + 2;
                let rel_end = s[body_start..].find('}')?;
                let end = body_start + rel_end;
                i = end + 1;
                let body = &s[body_start..end];
                return Some((&s[start..=end], classify_directive(body)));
            }
            i += 1;
        }
        None
    })
}

/// Upper bound on a `${file:...}` read. The directive injects small token /
/// secret / cert files into a config field; anything larger is almost
/// certainly a misconfiguration (a data file, or `/dev/zero`, which would
/// OOM an unbounded `fs::read`). Configs are trusted input, but a stray
/// path shouldn't be able to exhaust memory (#78/#37).
const MAX_INTERPOLATED_FILE_BYTES: u64 = 1024 * 1024; // 1 MiB

fn read_file_trimmed(path_str: &str) -> CliResult<String> {
    use std::io::Read as _;
    let path = PathBuf::from(path_str);
    let file = std::fs::File::open(&path).map_err(|source| CliError::ReadInterpolatedFile {
        path: path.clone(),
        source,
    })?;
    // Read at most MAX+1 bytes so we can detect (rather than truncate) an
    // oversized file without ever allocating more than the cap.
    let mut buf = Vec::new();
    file.take(MAX_INTERPOLATED_FILE_BYTES + 1)
        .read_to_end(&mut buf)
        .map_err(|source| CliError::ReadInterpolatedFile {
            path: path.clone(),
            source,
        })?;
    if buf.len() as u64 > MAX_INTERPOLATED_FILE_BYTES {
        return Err(CliError::InterpolatedFileTooLarge {
            path,
            max_bytes: MAX_INTERPOLATED_FILE_BYTES,
        });
    }
    Ok(String::from_utf8_lossy(&buf).trim_end().to_owned())
}

/// Walk a dotted path through a JSON value. Returns `None` if any segment
/// is missing or addresses through a non-object/array node.
pub(crate) fn resolve_dotted(root: &Value, path: &str) -> Option<Value> {
    if path.is_empty() {
        return Some(root.clone());
    }
    let mut cur = root;
    for segment in path.split('.') {
        cur = match cur {
            Value::Object(map) => map.get(segment)?,
            Value::Array(arr) => {
                let idx: usize = segment.parse().ok()?;
                arr.get(idx)?
            }
            _ => return None,
        };
    }
    Some(cur.clone())
}

/// Render a JSON value as a plain string suitable for substitution into a
/// config field. Strings come through unquoted; everything else uses
/// `to_string()` (numbers / bools / null / nested JSON).
pub(crate) fn value_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

// ── Post-parse load-time ref resolution ─────────────────────────────────────

/// Resolve every `${vars.X}`, `${sources.X.PATH}`, `${sinks.X.PATH}` token
/// found in a parsed [`PipelineConfig`](crate::config::PipelineConfig).
///
/// Order:
/// 1. Resolve `${vars.X}` references *inside the vars block itself* (with
///    cycle detection), so vars-referencing-vars works.
/// 2. Substitute `${vars.X}` inside `pipeline.sources.*` / `pipeline.sinks.*`
///    template bodies, plus the legacy singular `pipeline.source` /
///    `pipeline.sink` configs. This snapshot becomes the basis for
///    `${sources.X.PATH}` / `${sinks.X.PATH}` lookups.
/// 3. Walk every other string location in the config and resolve both
///    `${vars.X}` and `${sources/sinks.X.PATH}`. `${row_id.path}` tokens
///    are passed through verbatim for runtime resolution.
///
/// The legacy singular `pipeline.source` / `pipeline.sink` are visible under
/// the template name `default` for `${sources.default.config.X}` /
/// `${sinks.default.config.X}` lookups.
pub fn resolve_config_refs(cfg: &mut crate::config::PipelineConfig) -> CliResult<()> {
    // Phase 1: fully resolve the vars block (vars may reference other vars).
    if let Some(vars) = cfg.vars.clone() {
        let resolved = resolve_vars_block(&vars)?;
        cfg.vars = Some(resolved);
    }

    let empty_vars: HashMap<String, Value> = HashMap::new();
    let vars_ref: &HashMap<String, Value> = cfg.vars.as_ref().unwrap_or(&empty_vars);

    // Phase 2: substitute vars inside template bodies so the snapshot taken
    // next sees fully-resolved values.
    for (_name, spec) in cfg.pipeline.sources.iter_mut() {
        resolve_vars_only(&mut spec.config, vars_ref)?;
    }
    for (_name, spec) in cfg.pipeline.sinks.iter_mut() {
        resolve_vars_only(&mut spec.config, vars_ref)?;
    }
    if let Some(spec) = cfg.pipeline.source.as_mut() {
        resolve_vars_only(&mut spec.config, vars_ref)?;
    }
    if let Some(spec) = cfg.pipeline.sink.as_mut() {
        resolve_vars_only(&mut spec.config, vars_ref)?;
    }

    // Snapshot the templates *after* vars substitution.
    let snapshot = TemplateSnapshot::capture(&cfg.pipeline);

    // Phase 3: walk everything — including the template bodies again for
    // ${sources/sinks.X.PATH} refs (Phase 2 only resolved ${vars.X} there).
    for (_name, spec) in cfg.pipeline.sources.iter_mut() {
        resolve_value_full(&mut spec.config, vars_ref, &snapshot)?;
    }
    for (_name, spec) in cfg.pipeline.sinks.iter_mut() {
        resolve_value_full(&mut spec.config, vars_ref, &snapshot)?;
    }
    if let Some(spec) = cfg.pipeline.source.as_mut() {
        resolve_value_full(&mut spec.config, vars_ref, &snapshot)?;
    }
    if let Some(spec) = cfg.pipeline.sink.as_mut() {
        resolve_value_full(&mut spec.config, vars_ref, &snapshot)?;
    }
    for t in cfg.pipeline.transforms.iter_mut() {
        resolve_value_full(&mut t.config, vars_ref, &snapshot)?;
    }
    if let Some(s) = cfg.pipeline.state.as_mut() {
        resolve_value_full(&mut s.config, vars_ref, &snapshot)?;
    }
    if let Some(d) = cfg.pipeline.dlq.as_mut() {
        resolve_value_full(&mut d.sink.config, vars_ref, &snapshot)?;
    }
    // The shared `auth:` catalog is a first-class config location: provider
    // specs may reference `${vars.X}` / `${sources.X.PATH}` like any other (#134).
    if let Some(auth) = cfg.auth.as_mut() {
        for (_name, spec) in auth.iter_mut() {
            resolve_value_full(spec, vars_ref, &snapshot)?;
        }
    }
    // The replication snapshot source config is a first-class connector config
    // (like `pipeline.source.config`); `${vars.X}` / `${sources.X.PATH}` refs
    // there must resolve too.
    if let Some(r) = cfg.replication.as_mut() {
        resolve_value_full(&mut r.snapshot.source.config, vars_ref, &snapshot)?;
    }
    for (i, row) in cfg.matrix.iter_mut().enumerate() {
        let _row_owner = row.id.clone().unwrap_or_else(|| format!("row-{i}"));
        if let Some(p) = row.source.as_mut()
            && let Some(c) = p.config.as_mut()
        {
            resolve_value_full(c, vars_ref, &snapshot)?;
        }
        if let Some(p) = row.sink.as_mut()
            && let Some(c) = p.config.as_mut()
        {
            resolve_value_full(c, vars_ref, &snapshot)?;
        }
        if let Some(ts) = row.transforms.as_mut() {
            for t in ts.iter_mut() {
                resolve_value_full(&mut t.config, vars_ref, &snapshot)?;
            }
        }
        if let Some(s) = row.state.as_mut() {
            resolve_value_full(&mut s.config, vars_ref, &snapshot)?;
        }
        if let Some(Some(d)) = row.dlq.as_mut() {
            resolve_value_full(&mut d.sink.config, vars_ref, &snapshot)?;
        }
    }
    Ok(())
}

/// Snapshot of the resolved templates (vars already substituted) so that
/// `${sources.X.PATH}` lookups can find values without re-walking the live config.
struct TemplateSnapshot {
    sources: HashMap<String, Value>,
    sinks: HashMap<String, Value>,
}

impl TemplateSnapshot {
    fn capture(spec: &crate::config::PipelineSpec) -> Self {
        let mut sources: HashMap<String, Value> = spec
            .sources
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    serde_json::to_value(v)
                        .expect("ConnectorSpec derives Serialize and cannot fail"),
                )
            })
            .collect();
        if let Some(s) = &spec.source {
            sources.entry("default".into()).or_insert_with(|| {
                serde_json::to_value(s).expect("ConnectorSpec derives Serialize and cannot fail")
            });
        }
        let mut sinks: HashMap<String, Value> = spec
            .sinks
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    serde_json::to_value(v)
                        .expect("ConnectorSpec derives Serialize and cannot fail"),
                )
            })
            .collect();
        if let Some(s) = &spec.sink {
            sinks.entry("default".into()).or_insert_with(|| {
                serde_json::to_value(s).expect("ConnectorSpec derives Serialize and cannot fail")
            });
        }
        Self { sources, sinks }
    }
}

/// Resolve the vars block in topological order, returning the fully-substituted
/// map. Cycles surface as [`CliError::InterpolationCycle`].
fn resolve_vars_block(input: &HashMap<String, Value>) -> CliResult<HashMap<String, Value>> {
    let mut resolved: HashMap<String, Value> = HashMap::new();
    let mut visiting: Vec<String> = Vec::new();
    for key in input.keys() {
        resolve_one_var(key, input, &mut resolved, &mut visiting)?;
    }
    Ok(resolved)
}

fn resolve_one_var(
    key: &str,
    input: &HashMap<String, Value>,
    resolved: &mut HashMap<String, Value>,
    visiting: &mut Vec<String>,
) -> CliResult<()> {
    if resolved.contains_key(key) {
        return Ok(());
    }
    if let Some(start) = visiting.iter().position(|k| k == key) {
        // Already on the DFS stack — cycle detected. Build the chain in
        // traversal order: the nodes from `start` to the end of the stack,
        // plus the back-edge closing the cycle (key itself again).
        let chain: Vec<String> = visiting[start..]
            .iter()
            .map(|k| format!("vars.{k}"))
            .chain(std::iter::once(format!("vars.{key}")))
            .collect();
        return Err(CliError::InterpolationCycle { chain });
    }
    visiting.push(key.to_string());
    let mut value = input
        .get(key)
        .expect("key was taken from input map")
        .clone();
    resolve_vars_recursive(&mut value, input, resolved, visiting)?;
    visiting.pop();
    resolved.insert(key.to_string(), value);
    Ok(())
}

/// Phase 1 — vars may reference other vars. Resolves `${vars.X}` tokens in
/// `v` and recursively resolves any vars that haven't been resolved yet.
fn resolve_vars_recursive(
    v: &mut Value,
    input: &HashMap<String, Value>,
    resolved: &mut HashMap<String, Value>,
    visiting: &mut Vec<String>,
) -> CliResult<()> {
    match v {
        Value::String(s) => {
            let new_s = rewrite(s, |body| {
                let Some(name) = body.strip_prefix("vars.") else {
                    return Ok(None); // not a vars ref — leave verbatim
                };
                if !resolved.contains_key(name) {
                    if !input.contains_key(name) {
                        return Err(CliError::UnknownVarsRef {
                            name: name.to_string(),
                            token: format!("${{{body}}}"),
                        });
                    }
                    resolve_one_var(name, input, resolved, visiting)?;
                }
                Ok(Some(value_to_string(&resolved[name])))
            })?;
            *s = new_s;
        }
        Value::Array(a) => {
            for item in a.iter_mut() {
                resolve_vars_recursive(item, input, resolved, visiting)?;
            }
        }
        Value::Object(m) => {
            for item in m.values_mut() {
                resolve_vars_recursive(item, input, resolved, visiting)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Phase 2 — vars map already fully resolved. Resolves only `${vars.X}`
/// tokens in `v` against the pre-resolved vars map; errors if a var is unknown.
fn resolve_vars_only(v: &mut Value, vars: &HashMap<String, Value>) -> CliResult<()> {
    match v {
        Value::String(s) => {
            let new_s = rewrite(s, |body| {
                let Some(name) = body.strip_prefix("vars.") else {
                    return Ok(None);
                };
                let val = vars.get(name).ok_or_else(|| CliError::UnknownVarsRef {
                    name: name.to_string(),
                    token: format!("${{{body}}}"),
                })?;
                Ok(Some(value_to_string(val)))
            })?;
            *s = new_s;
        }
        Value::Array(a) => {
            for item in a.iter_mut() {
                resolve_vars_only(item, vars)?;
            }
        }
        Value::Object(m) => {
            for item in m.values_mut() {
                resolve_vars_only(item, vars)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Phase 3 — vars and template refs. Resolves both `${vars.X}` and
/// `${sources/sinks.X.PATH}` tokens in `v`; deferred row-id tokens pass through.
fn resolve_value_full(
    v: &mut Value,
    vars: &HashMap<String, Value>,
    templates: &TemplateSnapshot,
) -> CliResult<()> {
    match v {
        Value::String(s) => {
            let new_s = rewrite(s, |body| {
                if let Some(name) = body.strip_prefix("vars.") {
                    let val = vars.get(name).ok_or_else(|| CliError::UnknownVarsRef {
                        name: name.to_string(),
                        token: format!("${{{body}}}"),
                    })?;
                    return Ok(Some(value_to_string(val)));
                }
                if let Some(rest) = body.strip_prefix("sources.") {
                    let mut visiting = Vec::new();
                    return Ok(Some(lookup_template_path(
                        &templates.sources,
                        &templates.sinks,
                        "sources",
                        rest,
                        &mut visiting,
                    )?));
                }
                if let Some(rest) = body.strip_prefix("sinks.") {
                    let mut visiting = Vec::new();
                    return Ok(Some(lookup_template_path(
                        &templates.sources,
                        &templates.sinks,
                        "sinks",
                        rest,
                        &mut visiting,
                    )?));
                }
                // Any other prefix (e.g. `${users.id}`) is a deferred row-id token —
                // leave verbatim for runtime resolution.
                Ok(None)
            })?;
            *s = new_s;
        }
        Value::Array(a) => {
            for item in a.iter_mut() {
                resolve_value_full(item, vars, templates)?;
            }
        }
        Value::Object(m) => {
            for item in m.values_mut() {
                resolve_value_full(item, vars, templates)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Resolve a `${sources.X.PATH}` / `${sinks.X.PATH}` reference, following
/// template-to-template chains to their terminal literal.
///
/// `visiting` is the DFS stack of `{kind}.{name}` keys currently being
/// expanded; re-encountering a key on the stack is a cycle (e.g. `a → b → a`)
/// and surfaces as [`CliError::InterpolationCycle`] rather than silently
/// leaving each template holding the other's token text (#78/#10).
fn lookup_template_path(
    sources: &HashMap<String, Value>,
    sinks: &HashMap<String, Value>,
    kind: &str,
    rest: &str,
    visiting: &mut Vec<String>,
) -> CliResult<String> {
    // `rest` is `<name>` or `<name>.<dotted.path>`
    let (name, path) = rest.split_once('.').unwrap_or((rest, ""));
    let key = format!("{kind}.{name}");
    if let Some(start) = visiting.iter().position(|k| *k == key) {
        let chain: Vec<String> = visiting[start..]
            .iter()
            .cloned()
            .chain(std::iter::once(key))
            .collect();
        return Err(CliError::InterpolationCycle { chain });
    }
    let catalog = if kind == "sources" { sources } else { sinks };
    let template = catalog
        .get(name)
        .ok_or_else(|| CliError::UnknownTemplateRef {
            token: format!("${{{kind}.{rest}}}"),
            reason: format!("no {kind} template named '{name}'"),
        })?;
    let resolved = resolve_dotted(template, path).ok_or_else(|| CliError::UnknownTemplateRef {
        token: format!("${{{kind}.{rest}}}"),
        reason: format!("path '{path}' does not resolve inside {kind} template '{name}'"),
    })?;
    // The looked-up value may itself contain `${sources/sinks.X}` tokens (a
    // template referencing another template). Resolve them, following the
    // chain, with `key` pushed so a cycle is detected. `${vars.X}` are already
    // substituted (Phase 2); other prefixes are deferred row-id tokens and
    // pass through verbatim.
    let resolved_str = value_to_string(&resolved);
    visiting.push(key);
    let out = rewrite(&resolved_str, |body| {
        if let Some(rest) = body.strip_prefix("sources.") {
            return Ok(Some(lookup_template_path(
                sources, sinks, "sources", rest, visiting,
            )?));
        }
        if let Some(rest) = body.strip_prefix("sinks.") {
            return Ok(Some(lookup_template_path(
                sources, sinks, "sinks", rest, visiting,
            )?));
        }
        Ok(None)
    });
    visiting.pop();
    out
}

/// Resolve `${name}` and `${row_id}` in a lineage job-name template. `${now.*}`
/// tokens are resolved earlier by the run-clock pass over the config.
pub fn resolve_lineage_job_name(template: &str, name: &str, row_id: &str) -> String {
    template
        .replace("${name}", name)
        .replace("${row_id}", row_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn passes_through_text_with_no_directives() {
        let out = interpolate("just a string").unwrap();
        assert_eq!(out, "just a string");
    }

    #[test]
    fn interpolate_record_comma_joins_collected_lists() {
        // #531: a collected discovery dim resolves to a JSON array; the record
        // interpolation renders it comma-joined so `?properties=a,b,c` works.
        let ctx: HashMap<String, Value> = [(
            "props".to_string(),
            json!({"name": ["amount", "stage", "name"]}),
        )]
        .into();
        let out = interpolate_record("${props.name}", &ctx).unwrap();
        assert_eq!(out, "amount,stage,name");
        // Embedded in a larger string, too.
        let out2 = interpolate_record("f=${props.name}&x=1", &ctx).unwrap();
        assert_eq!(out2, "f=amount,stage,name&x=1");
    }

    #[test]
    fn interpolate_record_scalar_unchanged() {
        let ctx: HashMap<String, Value> = [("types".to_string(), json!({"name": "deal"}))].into();
        assert_eq!(interpolate_record("${types.name}", &ctx).unwrap(), "deal");
    }

    #[test]
    fn substitutes_env_var() {
        unsafe { std::env::set_var("FAUCET_TEST_VAR", "hello") };
        let out = interpolate("token=${env:FAUCET_TEST_VAR}").unwrap();
        assert_eq!(out, "token=hello");
        unsafe { std::env::remove_var("FAUCET_TEST_VAR") };
    }

    #[test]
    fn missing_env_var_is_an_error() {
        unsafe { std::env::remove_var("FAUCET_TEST_MISSING") };
        let err = interpolate("token=${env:FAUCET_TEST_MISSING}").unwrap_err();
        match err {
            CliError::MissingEnvVar { var, .. } => assert_eq!(var, "FAUCET_TEST_MISSING"),
            other => panic!("expected MissingEnvVar, got {other:?}"),
        }
    }

    #[test]
    fn interpolate_value_resolves_scalars_in_strings_keys_and_arrays() {
        unsafe { std::env::set_var("FAUCET_F43_TOKEN", "sekret") };
        let mut v = json!({
            "${env:FAUCET_F43_KEY}": "kv",
            "auth": {"token": "${env:FAUCET_F43_TOKEN}"},
            "list": ["${env:FAUCET_F43_TOKEN}", 42, true],
            "deferred": "${users.id}",
            "num": 5,
        });
        unsafe { std::env::set_var("FAUCET_F43_KEY", "ckey") };
        interpolate_value(&mut v).unwrap();
        assert_eq!(v["auth"]["token"], "sekret");
        assert_eq!(v["list"][0], "sekret");
        assert_eq!(v["list"][1], 42); // non-strings untouched
        assert_eq!(v["list"][2], true);
        assert_eq!(v["deferred"], "${users.id}"); // deferred token survives
        assert_eq!(v["num"], 5);
        assert_eq!(v["ckey"], "kv"); // interpolated key
        unsafe { std::env::remove_var("FAUCET_F43_TOKEN") };
        unsafe { std::env::remove_var("FAUCET_F43_KEY") };
    }

    #[test]
    fn interpolate_value_keeps_resolved_value_as_a_single_scalar() {
        // F43: a resolved value containing markup-significant characters must
        // NOT alter the document's structure — it stays one scalar string,
        // unlike the old raw-text substitution which could inject a sibling key.
        unsafe { std::env::set_var("FAUCET_F43_INJECT", "real\ninjected_key: pwned\nmore: x") };
        let mut v = json!({ "name": "${env:FAUCET_F43_INJECT}" });
        interpolate_value(&mut v).unwrap();
        assert_eq!(v["name"], "real\ninjected_key: pwned\nmore: x");
        // The object still has exactly one key — nothing was injected.
        assert_eq!(v.as_object().unwrap().len(), 1);
        assert!(v.get("injected_key").is_none());
        unsafe { std::env::remove_var("FAUCET_F43_INJECT") };
    }

    #[test]
    fn secret_prefix_is_env_alias_for_now() {
        unsafe { std::env::set_var("FAUCET_SECRET_VAR", "shh") };
        let out = interpolate("${secret:FAUCET_SECRET_VAR}").unwrap();
        assert_eq!(out, "shh");
        unsafe { std::env::remove_var("FAUCET_SECRET_VAR") };
    }

    #[test]
    fn resolved_env_and_secret_values_are_registered_for_redaction() {
        // M3 (#146): credentials supplied via ${env:}/${secret:} must be
        // scrubbed from faucet's tracing/log/error output, just like
        // ${vault:…} values — not left to leak.
        let secret = "super-secret-token-abcdef-1234567890"; // >= MIN_REDACT_LEN
        unsafe { std::env::set_var("FAUCET_M3_REDACT_TOKEN", secret) };
        let out = interpolate("Authorization: Bearer ${env:FAUCET_M3_REDACT_TOKEN}").unwrap();
        assert!(out.contains(secret));
        let redacted = crate::secrets::registry::redact(&out);
        assert!(
            !redacted.contains(secret),
            "resolved ${{env:}} value must be registered for redaction"
        );
        assert!(redacted.contains("***"));
        unsafe { std::env::remove_var("FAUCET_M3_REDACT_TOKEN") };
    }

    #[test]
    fn reads_file_directive_and_trims_trailing_newline() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("token.txt");
        std::fs::write(&path, "abcdef\n").unwrap();
        let raw = format!("token=${{file:{}}}", path.display());
        let out = interpolate(&raw).unwrap();
        assert_eq!(out, "token=abcdef");
    }

    #[test]
    fn file_directive_rejects_oversized_file() {
        // Regression for #78/#37: a file larger than the cap errors instead of
        // being read unboundedly into memory.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("big.bin");
        let big = vec![b'x'; (MAX_INTERPOLATED_FILE_BYTES + 10) as usize];
        std::fs::write(&path, &big).unwrap();
        let raw = format!("${{file:{}}}", path.display());
        match interpolate(&raw).unwrap_err() {
            CliError::InterpolatedFileTooLarge { max_bytes, .. } => {
                assert_eq!(max_bytes, MAX_INTERPOLATED_FILE_BYTES);
            }
            other => panic!("expected InterpolatedFileTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn file_directive_reads_file_at_the_limit() {
        // Exactly at the cap is allowed.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ok.bin");
        std::fs::write(&path, vec![b'a'; MAX_INTERPOLATED_FILE_BYTES as usize]).unwrap();
        let raw = format!("${{file:{}}}", path.display());
        assert!(interpolate(&raw).is_ok());
    }

    #[test]
    fn load_time_leaves_id_path_tokens_alone() {
        unsafe { std::env::set_var("FAUCET_T", "v") };
        let out = interpolate("a=${env:FAUCET_T} b=${users.id}").unwrap();
        assert_eq!(out, "a=v b=${users.id}");
        unsafe { std::env::remove_var("FAUCET_T") };
    }

    #[test]
    fn load_time_passes_unknown_prefix_through() {
        // Unknown prefixes are deferred — the matrix expander decides if
        // they reference a real row id later. (Pre-#54 behaviour was to error
        // here; that responsibility moves to expand.rs.)
        let out = interpolate("${weird:thing}").unwrap();
        assert_eq!(out, "${weird:thing}");
    }

    #[test]
    fn classify_colon_is_load_time_dot_is_deferred() {
        // The single classification rule shared by interpolate + expand (#78/#39).
        assert!(matches!(
            classify_directive("env:VAR"),
            Directive::LoadTime {
                prefix: "env",
                body: "VAR"
            }
        ));
        // No colon → deferred, even for a reserved-looking prefix like `env`.
        assert!(matches!(
            classify_directive("env.foo"),
            Directive::Deferred {
                id: "env",
                path: "foo"
            }
        ));
        assert!(matches!(
            classify_directive("users.addr.city"),
            Directive::Deferred {
                id: "users",
                path: "addr.city"
            }
        ));
        assert!(matches!(
            classify_directive("row"),
            Directive::Deferred {
                id: "row",
                path: ""
            }
        ));
    }

    #[test]
    fn iter_directives_finds_tokens_and_skips_escapes() {
        let toks: Vec<_> = iter_directives("a=${env:V} b=${users.id} c=$${lit}").collect();
        // The escaped $${lit} is not a token.
        assert_eq!(toks.len(), 2);
        assert_eq!(toks[0].0, "${env:V}");
        assert!(matches!(
            toks[0].1,
            Directive::LoadTime { prefix: "env", .. }
        ));
        assert_eq!(toks[1].0, "${users.id}");
        assert!(matches!(toks[1].1, Directive::Deferred { id: "users", .. }));
    }

    #[test]
    fn dollar_dollar_brace_is_escaped() {
        let out = interpolate("path=$${env:VAR}").unwrap();
        assert_eq!(out, "path=${env:VAR}");
    }

    #[test]
    fn unclosed_directive_is_left_literal() {
        let out = interpolate("hello ${env:NOPE").unwrap();
        assert_eq!(out, "hello ${env:NOPE");
    }

    #[test]
    fn multiple_directives_resolve_in_order() {
        unsafe { std::env::set_var("FAUCET_A", "one") };
        unsafe { std::env::set_var("FAUCET_B", "two") };
        let out = interpolate("${env:FAUCET_A}-${env:FAUCET_B}").unwrap();
        assert_eq!(out, "one-two");
        unsafe { std::env::remove_var("FAUCET_A") };
        unsafe { std::env::remove_var("FAUCET_B") };
    }

    // ── record-time tests ───────────────────────────────────────────────

    fn ctx_with(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).into(), v.clone()))
            .collect()
    }

    #[test]
    fn record_resolves_simple_dotted_path() {
        let ctx = ctx_with(&[("users", json!({"id": 42, "name": "alice"}))]);
        let out = interpolate_record("/v1/users/${users.id}", &ctx).unwrap();
        assert_eq!(out, "/v1/users/42");
    }

    #[test]
    fn record_resolves_nested_dotted_path() {
        let ctx = ctx_with(&[(
            "users",
            json!({"id": 1, "addr": {"city": "NYC", "zip": "10001"}}),
        )]);
        let out = interpolate_record("/${users.addr.city}/${users.addr.zip}", &ctx).unwrap();
        assert_eq!(out, "/NYC/10001");
    }

    #[test]
    fn record_resolves_array_index() {
        let ctx = ctx_with(&[("users", json!({"tags": ["a", "b", "c"]}))]);
        let out = interpolate_record("first=${users.tags.0}", &ctx).unwrap();
        assert_eq!(out, "first=a");
    }

    #[test]
    fn record_renders_numbers_and_booleans_as_strings() {
        let ctx = ctx_with(&[("users", json!({"id": 7, "active": true}))]);
        let out = interpolate_record("id=${users.id} active=${users.active}", &ctx).unwrap();
        assert_eq!(out, "id=7 active=true");
    }

    #[test]
    fn record_unknown_id_errors() {
        let ctx = ctx_with(&[("users", json!({"id": 1}))]);
        let err = interpolate_record("${nobody.x}", &ctx).unwrap_err();
        assert!(matches!(err, CliError::UnknownInterpolationId { .. }));
    }

    #[test]
    fn record_missing_field_errors() {
        let ctx = ctx_with(&[("users", json!({"id": 1}))]);
        let err = interpolate_record("${users.missing}", &ctx).unwrap_err();
        match err {
            CliError::MissingRecordField { id, path } => {
                assert_eq!(id, "users");
                assert_eq!(path, "missing");
            }
            other => panic!("expected MissingRecordField, got {other:?}"),
        }
    }

    #[test]
    fn record_leaves_load_time_directives_alone() {
        let ctx = HashMap::new();
        let out = interpolate_record("a=${env:NOPE} b=${file:./x}", &ctx).unwrap();
        assert_eq!(out, "a=${env:NOPE} b=${file:./x}");
    }

    // ── post-parse load-time tests ───────────────────────────────────────

    use crate::config::{PipelineConfig, parse_with_extension};
    use crate::interpolate::resolve_config_refs;

    fn load(yaml: &str) -> PipelineConfig {
        let mut cfg = parse_with_extension(yaml, "yaml").unwrap();
        resolve_config_refs(&mut cfg).unwrap();
        cfg
    }

    #[test]
    fn resolves_vars_in_source_config() {
        let cfg = load(
            r#"
version: 1
vars:
  base: https://api.example.com
pipeline:
  source: { type: rest, config: { base_url: "${vars.base}" } }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
"#,
        );
        assert_eq!(
            cfg.pipeline.source.as_ref().unwrap().config["base_url"],
            "https://api.example.com"
        );
    }

    #[test]
    fn resolves_vars_in_replication_snapshot_source_config() {
        // `${vars.X}` in the replication snapshot source config must resolve at
        // load time, just like in `pipeline.source.config`.
        let cfg = load(
            r#"
version: 1
vars:
  base: postgres://db/orders
pipeline:
  source: { type: postgres-cdc, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
replication:
  mode: snapshot_then_cdc
  snapshot:
    source:
      type: postgres
      config: { connection_url: "${vars.base}", query: "SELECT 1" }
"#,
        );
        assert_eq!(
            cfg.replication.as_ref().unwrap().snapshot.source.config["connection_url"],
            "postgres://db/orders"
        );
    }

    #[test]
    fn resolves_vars_referencing_other_vars() {
        let cfg = load(
            r#"
version: 1
vars:
  base: https://api.example.com
  users_url: "${vars.base}/v1/users"
pipeline:
  source: { type: rest, config: { url: "${vars.users_url}" } }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
"#,
        );
        assert_eq!(
            cfg.pipeline.source.as_ref().unwrap().config["url"],
            "https://api.example.com/v1/users"
        );
    }

    #[test]
    fn resolves_template_ref_from_matrix_row() {
        let cfg = load(
            r#"
version: 1
pipeline:
  sources:
    users_api:
      type: rest
      config: { base_url: https://api.example.com }
  sinks:
    archive: { type: jsonl, config: { path: ./out.jsonl } }
matrix:
  - id: load_users
    source:
      ref: users_api
      config: { audit_url: "${sources.users_api.config.base_url}/audit" }
"#,
        );
        let row_src = cfg.matrix[0].source.as_ref().unwrap();
        assert_eq!(
            row_src.config.as_ref().unwrap()["audit_url"],
            "https://api.example.com/audit"
        );
    }

    #[test]
    fn detects_vars_cycle() {
        let yaml = r#"
version: 1
vars:
  a: "${vars.b}"
  b: "${vars.c}"
  c: "${vars.a}"
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
"#;
        // resolve_config_refs now runs inside from_text; the error surfaces there.
        let err = parse_with_extension(yaml, "yaml").unwrap_err();
        match err {
            CliError::InterpolationCycle { chain } => {
                // 3-node cycle a → b → c → a: chain should have 4 entries with
                // the first equal to the last (the closing back-edge).
                assert_eq!(chain.len(), 4, "chain: {chain:?}");
                assert_eq!(chain.first(), chain.last(), "chain: {chain:?}");
                // Every node appears exactly once interior, plus the closing edge.
                let mut sorted_interior: Vec<_> = chain[..3].to_vec();
                sorted_interior.sort();
                assert_eq!(sorted_interior, vec!["vars.a", "vars.b", "vars.c"]);
            }
            other => panic!("expected InterpolationCycle, got {other:?}"),
        }
    }

    #[test]
    fn resolves_cross_template_reference() {
        // sources.b references sources.a via ${sources.a.config.host}.
        let cfg = load(
            r#"
version: 1
pipeline:
  sources:
    a: { type: rest, config: { host: api.example.com } }
    b: { type: rest, config: { host: "${sources.a.config.host}" } }
  sinks:
    out: { type: jsonl, config: { path: ./o.jsonl } }
"#,
        );
        assert_eq!(cfg.pipeline.sources["b"].config["host"], "api.example.com");
    }

    #[test]
    fn resolves_chained_cross_template_reference() {
        // a → b → c, where c holds the literal. A single resolution pass (as
        // production runs via `from_text`) must follow the chain all the way
        // to the literal, not stop at b's token text (#78/#10). NB: we use
        // `parse_with_extension` directly (one pass) rather than the `load`
        // helper, which resolves twice and would mask a single-pass gap.
        let cfg = parse_with_extension(
            r#"
version: 1
pipeline:
  sources:
    a: { type: rest, config: { host: "${sources.b.config.host}" } }
    b: { type: rest, config: { host: "${sources.c.config.host}" } }
    c: { type: rest, config: { host: db.example.com } }
  sinks:
    out: { type: jsonl, config: { path: ./o.jsonl } }
"#,
            "yaml",
        )
        .unwrap();
        assert_eq!(cfg.pipeline.sources["a"].config["host"], "db.example.com");
        assert_eq!(cfg.pipeline.sources["b"].config["host"], "db.example.com");
    }

    #[test]
    fn resolves_cross_template_reference_across_kinds() {
        // A sink template referencing a source template (cross-namespace).
        let cfg = load(
            r#"
version: 1
pipeline:
  sources:
    api: { type: rest, config: { host: api.example.com } }
  sinks:
    mirror: { type: http, config: { url: "${sources.api.config.host}" } }
"#,
        );
        assert_eq!(
            cfg.pipeline.sinks["mirror"].config["url"],
            "api.example.com"
        );
    }

    #[test]
    fn detects_cross_template_cycle() {
        // a → b → a is a mutual cycle and must error, not silently leave each
        // template holding the other's literal token text (#78/#10).
        let yaml = r#"
version: 1
pipeline:
  sources:
    a: { type: rest, config: { host: "${sources.b.config.host}" } }
    b: { type: rest, config: { host: "${sources.a.config.host}" } }
  sinks:
    out: { type: jsonl, config: { path: ./o.jsonl } }
"#;
        let err = parse_with_extension(yaml, "yaml").unwrap_err();
        match err {
            CliError::InterpolationCycle { chain } => {
                assert!(chain.first() == chain.last(), "chain: {chain:?}");
                assert!(
                    chain.iter().any(|c| c == "sources.a")
                        && chain.iter().any(|c| c == "sources.b"),
                    "chain must name both templates: {chain:?}"
                );
            }
            other => panic!("expected InterpolationCycle, got {other:?}"),
        }
    }

    #[test]
    fn unknown_template_path_errors() {
        // sources.a exists, but its config has no `missing_field` path.
        let yaml = r#"
version: 1
pipeline:
  sources:
    a: { type: rest, config: { host: x } }
  source: { type: rest, config: { x: "${sources.a.config.missing_field}" } }
  sink: { type: jsonl, config: { path: ./o.jsonl } }
"#;
        // resolve_config_refs now runs inside from_text; the error surfaces there.
        let err = parse_with_extension(yaml, "yaml").unwrap_err();
        match err {
            CliError::UnknownTemplateRef { reason, .. } => {
                assert!(reason.contains("missing_field"));
            }
            other => panic!("expected UnknownTemplateRef, got {other:?}"),
        }
    }

    #[test]
    fn unknown_var_errors() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: { url: "${vars.nope}" } }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
"#;
        // resolve_config_refs now runs inside from_text; the error surfaces there.
        let err = parse_with_extension(yaml, "yaml").unwrap_err();
        match err {
            CliError::UnknownVarsRef { name, .. } => assert_eq!(name, "nope"),
            other => panic!("expected UnknownVarsRef, got {other:?}"),
        }
    }

    #[test]
    fn unknown_template_ref_errors() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: { x: "${sources.nope.config.foo}" } }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
"#;
        // resolve_config_refs now runs inside from_text; the error surfaces there.
        let err = parse_with_extension(yaml, "yaml").unwrap_err();
        match err {
            CliError::UnknownTemplateRef { reason, .. } => {
                assert!(reason.to_ascii_lowercase().contains("nope"));
            }
            other => panic!("expected UnknownTemplateRef, got {other:?}"),
        }
    }

    #[test]
    fn leaves_row_id_tokens_for_runtime() {
        let cfg = load(
            r#"
version: 1
pipeline:
  source: { type: rest, config: { path: "/v1/users/${users.id}/posts" } }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
"#,
        );
        // ${users.id} must survive — it's a deferred row-id reference.
        assert_eq!(
            cfg.pipeline.source.as_ref().unwrap().config["path"],
            "/v1/users/${users.id}/posts"
        );
    }

    #[test]
    fn resolves_vars_inside_auth_catalog() {
        // The shared `auth:` catalog must be a first-class config location:
        // ${vars.X} (and ${sources/sinks.X.PATH}) resolve there too (#134).
        let cfg = load(
            r#"
version: 1
vars:
  idp_token: topsecret
auth:
  idp: { type: static, config: { token: "Bearer ${vars.idp_token}" } }
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
"#,
        );
        assert_eq!(
            cfg.auth.as_ref().unwrap()["idp"]["config"]["token"],
            "Bearer topsecret"
        );
    }

    // ── resolve_now tests ────────────────────────────────────────────────────

    fn fixed_clock() -> chrono::DateTime<chrono::FixedOffset> {
        use chrono::TimeZone;
        // 2026-03-08 14:05:09 +00:00
        chrono::FixedOffset::east_opt(0)
            .unwrap()
            .with_ymd_and_hms(2026, 3, 8, 14, 5, 9)
            .unwrap()
    }

    #[test]
    fn now_named_tokens_render() {
        let c = fixed_clock();
        assert_eq!(resolve_now("${now.date}", c).unwrap(), "2026-03-08");
        assert_eq!(resolve_now("${now.year}", c).unwrap(), "2026");
        assert_eq!(resolve_now("${now.month}", c).unwrap(), "03");
        assert_eq!(resolve_now("${now.day}", c).unwrap(), "08");
        assert_eq!(resolve_now("${now.hour}", c).unwrap(), "14");
        assert_eq!(resolve_now("${now.minute}", c).unwrap(), "05");
        assert_eq!(resolve_now("${now.second}", c).unwrap(), "09");
        assert_eq!(
            resolve_now("${now.unix}", c).unwrap(),
            c.timestamp().to_string()
        );
        assert!(
            resolve_now("${now.iso}", c)
                .unwrap()
                .starts_with("2026-03-08T14:05:09")
        );
        assert_eq!(
            resolve_now("${now.datetime}", c).unwrap(),
            resolve_now("${now.iso}", c).unwrap()
        );
    }

    #[test]
    fn now_in_a_path_template() {
        let c = fixed_clock();
        assert_eq!(
            resolve_now("s3://bucket/dt=${now.date}/part.jsonl", c).unwrap(),
            "s3://bucket/dt=2026-03-08/part.jsonl"
        );
    }

    #[test]
    fn now_strftime_renders_and_rejects_bad_format() {
        let c = fixed_clock();
        assert_eq!(
            resolve_now("${now.strftime.%Y/%m/%d}", c).unwrap(),
            "2026/03/08"
        );
        // A bogus specifier must be a clean config error, NOT a panic.
        // `%Q` is not a valid strftime specifier in chrono and produces Item::Error.
        let err = resolve_now("${now.strftime.%Q}", c).unwrap_err();
        assert!(err.to_string().contains("strftime"));
    }

    #[test]
    fn now_unknown_token_errors() {
        let c = fixed_clock();
        let err = resolve_now("${now.bogus}", c).unwrap_err();
        assert!(err.to_string().contains("now.bogus"));
    }

    #[test]
    fn now_leaves_other_tokens_verbatim() {
        let c = fixed_clock();
        // env/row-id/vars tokens must survive the now-pass untouched.
        assert_eq!(
            resolve_now("${env:VAR}/${users.id}/${now.date}", c).unwrap(),
            "${env:VAR}/${users.id}/2026-03-08"
        );
    }

    #[test]
    fn resolves_lineage_job_name_tokens() {
        assert_eq!(
            resolve_lineage_job_name("${name}::${row_id}", "orders", "users"),
            "orders::users"
        );
        assert_eq!(
            resolve_lineage_job_name("static", "orders", "users"),
            "static"
        );
    }
}
