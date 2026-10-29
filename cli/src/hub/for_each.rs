//! `for_each:` streams (#952): one source-template stream repeated per item
//! of a `type: list` param, expanded into ordinary streams before the
//! template composes.
//!
//! ```yaml
//! params:
//!   tables: { type: list, items: { type: string }, max_items: 500 }
//! streams:
//!   - for_each: ${param.tables}
//!     as: t
//!     name: "orders_${t}"
//!     source: { config: { query: 'SELECT * FROM "${t}"' } }
//! ```
//!
//! Template params come from the authenticated trigger caller, so looping
//! over one adds no trust: an item is held to the same rules as a
//! `${param.*}` spliced into the same place (quoting in SQL text, no query or
//! `..` in a URL, never assembling a directive). Items are coerced against
//! the list's `items:` before anything expands; each rendered name must be a
//! stream name (`^[a-z0-9][a-z0-9_]*$`, or normalised with
//! `name_case: snake`) and unique across the template.
//!
//! Expansion needs the param values, so every path that composes a template
//! expands it first: a trigger and `faucet run` with the caller's params, a
//! structural check (`hub check`, registration, `faucet validate`) with
//! distinct placeholder items.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use super::spec::{NameCase, SourceTemplate, Stream, check_slug, value_mentions};
use crate::error::{CliError, CliResult};
use crate::interpolate::{Directive, EnvOverlay, iter_directives, rewrite, value_to_string};
use crate::params::{BindMode, BoundParams, ParamSpec, ParamType, ParamsSpec, SuppliedParams};

/// Loop-variable names that would shadow another interpolation namespace.
const RESERVED_VARS: &[&str] = &[
    "param",
    "stream",
    "source",
    "owner",
    "env",
    "file",
    "secret",
    "now",
    "vars",
    "sources",
    "sinks",
    "map",
    "backfill",
    "partition",
    "trigger",
    "tenant",
    "parent",
];

/// Whether any stream of `src` repeats over a list param.
pub fn has_for_each(src: &SourceTemplate) -> bool {
    src.streams.iter().any(|s| s.for_each.is_some())
}

/// The param a `for_each:` names — the whole value must be one
/// `${param.NAME}`.
pub fn list_param(stream: &Stream) -> CliResult<&str> {
    let raw = stream.for_each.as_deref().unwrap_or_default();
    let mut tokens = iter_directives(raw);
    if let (Some((token, Directive::Deferred { id, path })), None) = (tokens.next(), tokens.next())
        && token == raw.trim()
        && id == crate::params::PARAM_ID
        && !path.is_empty()
        && !path.contains('.')
    {
        return Ok(path);
    }
    Err(CliError::Config(format!(
        "stream '{}': `for_each: {raw}` must be exactly `${{param.NAME}}` — a stream repeats \
         only over a `type: list` param, never over upstream data or the environment",
        stream.name
    )))
}

fn var(stream: &Stream) -> &str {
    stream.r#as.as_deref().unwrap_or_default()
}

/// The `${<as>…}` references in `s`: `None` for the whole item, `Some(field)`.
fn var_refs<'a>(s: &'a str, var: &str) -> Vec<(&'a str, Option<&'a str>)> {
    iter_directives(s)
        .filter_map(|(token, d)| match d {
            Directive::Deferred { id, path } if id == var => {
                Some((token, (!path.is_empty()).then_some(path)))
            }
            _ => None,
        })
        .collect()
}

fn walk_strings<'a>(v: &'a Value, out: &mut Vec<&'a str>) {
    match v {
        Value::String(s) => out.push(s),
        Value::Array(a) => a.iter().for_each(|x| walk_strings(x, out)),
        Value::Object(o) => {
            for (k, x) in o {
                out.push(k);
                walk_strings(x, out);
            }
        }
        _ => {}
    }
}

/// The parts of a stream an item renders into, as one value.
fn rendered_parts(stream: &Stream) -> Value {
    serde_json::json!({
        "source": stream.source,
        "transforms": stream.transforms,
        "primary_keys": stream.primary_keys,
        "write": stream.write,
    })
}

/// The shape checks of one `for_each` stream that need no params.
pub fn validate_stream(stream: &Stream) -> CliResult<()> {
    let pattern = &stream.name;
    list_param(stream)?;
    let v = var(stream);
    if v.is_empty() {
        return Err(CliError::Config(format!(
            "stream '{pattern}': `for_each:` needs `as:` — the name an item is referenced by \
             (`as: t`, then `${{t}}` or `${{t.field}}`)"
        )));
    }
    let ident = v
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && v.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if !ident || RESERVED_VARS.contains(&v) {
        return Err(CliError::Config(format!(
            "stream '{pattern}': `as: {v}` must be a lower-case identifier \
             (^[a-z_][a-z0-9_]*$) that is not a reserved name ({})",
            RESERVED_VARS.join(", ")
        )));
    }
    if var_refs(pattern, v).is_empty() {
        return Err(CliError::Config(format!(
            "stream '{pattern}': a `for_each` name must use `${{{v}}}` (or `${{{v}.field}}`), \
             or every item would get the same name"
        )));
    }
    if stream.parent.is_some() {
        return Err(CliError::Config(format!(
            "stream '{pattern}': a `for_each` stream cannot have a `parent` — run it per item \
             of the list, or per parent record, not both"
        )));
    }
    for (token, d) in iter_directives(pattern) {
        match d {
            Directive::Deferred { id, .. } if id == v || id == crate::params::PARAM_ID => {}
            _ => {
                return Err(CliError::Config(format!(
                    "stream '{pattern}': a `for_each` name may use only `${{{v}…}}` and \
                     `${{param.*}}`, not `{token}`"
                )));
            }
        }
    }
    Ok(())
}

/// The checks of a template's `for_each` streams against its `params:`.
pub fn validate_template(src: &SourceTemplate) -> CliResult<()> {
    let repeated: BTreeSet<&str> = src
        .streams
        .iter()
        .filter(|s| s.for_each.is_some())
        .map(|s| s.name.as_str())
        .collect();
    for s in &src.streams {
        if let Some(p) = &s.parent
            && repeated.contains(p.as_str())
        {
            return Err(CliError::Config(format!(
                "source-template '{}': stream '{}' names the `for_each` stream '{p}' as its \
                 parent — a repeated stream cannot have children",
                src.name, s.name
            )));
        }
        if s.for_each.is_none() {
            continue;
        }
        let what = format!("source-template '{}': stream '{}'", src.name, s.name);
        let pname = list_param(s)?;
        let Some(p) = src.params.get(pname) else {
            return Err(CliError::Config(format!(
                "{what}: `for_each` names param '{pname}', which `params:` does not declare"
            )));
        };
        if p.kind != ParamType::List {
            return Err(CliError::Config(format!(
                "{what}: `for_each` needs a `type: list` param, but '{pname}' is a {}",
                p.kind.as_str()
            )));
        }
        let items = p.items.as_deref().cloned().unwrap_or_default();
        let v = var(s);
        let parts = rendered_parts(s);
        let mut strings = vec![s.name.as_str()];
        walk_strings(&parts, &mut strings);
        for text in strings {
            for (token, field) in var_refs(text, v) {
                match (items.kind, field) {
                    (ParamType::Object, None) if text == s.name => {
                        return Err(CliError::Config(format!(
                            "{what}: `{token}` is a whole object item — name a field \
                             (`${{{v}.{}}}`)",
                            items
                                .properties
                                .keys()
                                .next()
                                .map_or("field", String::as_str)
                        )));
                    }
                    (ParamType::Object, Some(f)) if !items.properties.contains_key(f) => {
                        return Err(CliError::Config(format!(
                            "{what}: `{token}` names no field of the items of '{pname}' \
                             (fields: {})",
                            items
                                .properties
                                .keys()
                                .cloned()
                                .collect::<Vec<_>>()
                                .join(", ")
                        )));
                    }
                    (ParamType::Object, _) => {}
                    (_, Some(f)) => {
                        return Err(CliError::Config(format!(
                            "{what}: `{token}` — the items of '{pname}' are {}s with no field \
                             '{f}'; use `${{{v}}}`",
                            items.kind.as_str()
                        )));
                    }
                    (_, None) => {}
                }
            }
        }
        // A secret item may not become a row id (ids reach logs and metrics).
        for (token, field) in var_refs(&s.name, v) {
            let secret = p.is_secret()
                || field.is_some_and(|f| items.properties.get(f).is_some_and(|fp| fp.secret));
            if secret {
                return Err(CliError::Config(format!(
                    "{what}: the name uses `{token}`, which is secret — a row name is shown in \
                     run records and logs"
                )));
            }
        }
        for (token, d) in iter_directives(&s.name) {
            if let Directive::Deferred { id, path } = d
                && id == crate::params::PARAM_ID
                && src.params.get(path).is_some_and(ParamSpec::is_secret)
            {
                return Err(CliError::Config(format!(
                    "{what}: the name uses `{token}`, which is secret — a row name is shown in \
                     run records and logs"
                )));
            }
        }
    }
    Ok(())
}

/// Whether a `for_each` stream's destination differs per item: its rendered
/// `per_stream` block refers to the loop variable (through `${stream}`).
pub fn destination_varies(rendered: &Value, stream: &Stream) -> bool {
    let v = var(stream);
    value_mentions(rendered, &format!("${{{v}}}")) || value_mentions(rendered, &format!("${{{v}."))
}

/// Normalise a rendered name to snake case.
pub fn snake(raw: &str) -> String {
    let mut out = String::new();
    let mut prev: Option<char> = None;
    for c in raw.chars() {
        if c.is_ascii_alphanumeric() {
            if c.is_ascii_uppercase()
                && prev.is_some_and(|p| p.is_ascii_lowercase() || p.is_ascii_digit())
            {
                out.push('_');
            }
            out.push(c.to_ascii_lowercase());
        } else if !out.is_empty() && !out.ends_with('_') {
            out.push('_');
        }
        prev = Some(c);
    }
    out.trim_end_matches('_').to_string()
}

/// One item of a list, as a loop variable.
struct Item<'a> {
    var: &'a str,
    value: &'a Value,
    label: String,
    /// Caller-supplied free text, held to the `${param.*}` splice rules.
    guarded: bool,
    items: &'a ParamSpec,
    /// The caller's free text is recorded where it lands (#989): the list
    /// was supplied and is not `trusted`.
    traced: bool,
}

impl Item<'_> {
    /// The value `${<var>}` / `${<var>.<field>}` stands for.
    fn get(&self, token: &str, field: Option<&str>) -> CliResult<&Value> {
        match field {
            None => Ok(self.value),
            Some(f) => self.value.get(f).ok_or_else(|| {
                CliError::Config(format!(
                    "{}: `{token}` — the item has no field '{f}' (it is optional and was not \
                     given)",
                    self.label
                ))
            }),
        }
    }

    /// Whether `field` of this item comes from a closed `values:` set.
    fn closed(&self, field: Option<&str>) -> bool {
        match field {
            None => !self.items.values.is_empty(),
            Some(f) => self
                .items
                .properties
                .get(f)
                .is_some_and(|p| !p.values.is_empty()),
        }
    }

    /// The text of a reference spliced into a longer string.
    fn text(&self, token: &str, field: Option<&str>) -> CliResult<String> {
        let v = self.get(token, field)?;
        if v.is_object() || v.is_array() {
            return Err(CliError::Config(format!(
                "{}: `{token}` is a whole object item and cannot be part of a longer string — \
                 name a field",
                self.label
            )));
        }
        Ok(value_to_string(v))
    }
}

/// Render every `${<var>…}` reference in one string, holding a spliced item
/// to the `${param.*}` rules.
fn render_string(s: &str, item: &Item<'_>, field: Option<&str>) -> CliResult<Value> {
    let refs = var_refs(s, item.var);
    if refs.is_empty() {
        return Ok(Value::String(s.to_string()));
    }
    if let [(token, f)] = refs.as_slice()
        && *token == s
    {
        return Ok(item.get(token, *f)?.clone());
    }
    if item.guarded {
        for (token, f) in &refs {
            if item.closed(*f) {
                continue;
            }
            let v = item.get(token, *f)?;
            if let Some(why) = crate::params::bind::splice_refusal(v, s, token, field) {
                return Err(crate::params::bind::splice_error(
                    &format!("param '{}' (`{token}`)", item.label),
                    &why,
                ));
            }
        }
    }
    let render = |inert: bool| {
        rewrite(s, |body| {
            match crate::interpolate::classify_directive(body) {
                Directive::Deferred { id, path } if id == item.var => {
                    let token = format!("${{{body}}}");
                    let f = (!path.is_empty()).then_some(path);
                    if inert {
                        item.text(&token, f).map(|_| Some("_".to_string()))
                    } else {
                        item.text(&token, f).map(Some)
                    }
                }
                _ => Ok(None),
            }
        })
    };
    let out = render(false)?;
    // An item may not assemble a directive with the text around it.
    let baseline = render(true)?;
    let mut allowed: Vec<&str> = iter_directives(&baseline).map(|(t, _)| t).collect();
    for (token, _) in iter_directives(&out) {
        match allowed.iter().position(|a| *a == token) {
            Some(i) => {
                allowed.swap_remove(i);
            }
            None => {
                return Err(CliError::Config(format!(
                    "{}: the item combines with the text around it into an interpolation \
                     directive (`${{…}}`) — items are literal data",
                    item.label
                )));
            }
        }
    }
    Ok(Value::String(out))
}

/// Whether `s` splices caller free text of `item` (#989).
fn splices_free_text(s: &str, item: &Item<'_>) -> bool {
    item.traced
        && var_refs(s, item.var)
            .iter()
            .any(|(_, f)| crate::params::caller::item_field_text(item.items, item.value, *f))
}

/// Render every reference to `item` in `v` (at JSON pointer `ptr`),
/// recording in `traced` where caller free text lands.
fn render_tree(
    v: &mut Value,
    item: &Item<'_>,
    field: Option<&str>,
    ptr: &str,
    traced: &mut Vec<String>,
) -> CliResult<()> {
    match v {
        Value::String(s) => {
            if splices_free_text(s, item) {
                traced.push(ptr.to_string());
            }
            *v = render_string(s, item, field)?;
        }
        Value::Array(a) => {
            for (i, x) in a.iter_mut().enumerate() {
                render_tree(x, item, field, &format!("{ptr}/{i}"), traced)?;
            }
        }
        Value::Object(o) => {
            let entries: Vec<(String, Value)> = std::mem::take(o).into_iter().collect();
            for (key, mut val) in entries {
                let rendered = render_string(&key, item, None)
                    .map(|k| match k {
                        Value::String(k) => k,
                        other => value_to_string(&other),
                    })
                    .unwrap_or_else(|_| key.clone());
                let at = crate::params::caller::child(ptr, &rendered);
                render_tree(&mut val, item, Some(&key), &at, traced)?;
                let key = match render_string(&key, item, None)? {
                    Value::String(k) => k,
                    other => value_to_string(&other),
                };
                if splices_free_text(&key, item) {
                    traced.push(at);
                }
                o.insert(key, val);
            }
        }
        _ => {}
    }
    Ok(())
}

/// Render a `for_each` stream's name for one item: its `${<var>…}` and
/// `${param.*}` references, then `name_case`, then the stream-name rule.
fn render_name(
    stream: &Stream,
    item: &Item<'_>,
    bound: &BoundParams,
    what: &str,
) -> CliResult<String> {
    let with_item = rewrite(
        &stream.name,
        |body| match crate::interpolate::classify_directive(body) {
            Directive::Deferred { id, path } if id == item.var => {
                let token = format!("${{{body}}}");
                item.text(&token, (!path.is_empty()).then_some(path))
                    .map(Some)
            }
            Directive::Deferred { id, path } if id == crate::params::PARAM_ID => {
                if bound.placeholders.contains(path) {
                    return Ok(Some("param".to_string()));
                }
                match bound.values.get(path) {
                    Some(v) if v.is_array() || v.is_object() => Err(CliError::Config(format!(
                        "{what}: the name uses `${{{body}}}`, a list — a name takes scalars"
                    ))),
                    Some(v) => Ok(Some(value_to_string(v))),
                    None => Err(CliError::UnknownParamRef {
                        name: path.to_string(),
                        token: format!("${{{body}}}"),
                    }),
                }
            }
            _ => Ok(None),
        },
    )?;
    let name = match stream.name_case {
        Some(NameCase::Snake) => snake(&with_item),
        None => with_item.clone(),
    };
    check_slug("stream name", &name, false).map_err(|_| {
        CliError::Config(format!(
            "{what}: item {} renders the name '{with_item}'{}, which is not a stream name \
             (^[a-z0-9][a-z0-9_]*$, max 64 chars){}",
            item.label,
            if name == with_item {
                String::new()
            } else {
                format!(" ('{name}' after `name_case`)")
            },
            if stream.name_case.is_none() {
                " — set `name_case: snake` to normalise mixed-case or dashed names"
            } else {
                ""
            }
        ))
    })?;
    Ok(name)
}

/// Expand every `for_each` stream of `src` against `supplied` (the caller's
/// params; `mode` as for binding — `Placeholder` gives each list two distinct
/// placeholder items). `env` resolves `${env:…}` in the param defaults first,
/// as a local materialization does. A template without `for_each` streams is
/// returned unchanged.
pub fn expand(
    src: &SourceTemplate,
    supplied: &SuppliedParams,
    mode: BindMode,
    env: Option<&EnvOverlay>,
) -> CliResult<SourceTemplate> {
    expand_traced(src, supplied, mode, env).map(|e| e.template)
}

/// A template with its `for_each` streams expanded, and where the caller's
/// list items landed.
#[derive(Debug, Clone)]
pub struct Expanded {
    pub template: SourceTemplate,
    /// Caller free text an item wrote (#989), by stream: the stream's name,
    /// a JSON pointer into the stream (`/source/config/…`,
    /// `/transforms/0/config/…`) and the list param.
    pub caller_fields: Vec<(String, String, String)>,
}

impl Expanded {
    /// [`Self::caller_fields`] as pointers into a composed document, whose
    /// matrix row `id` is the stream name (rows a selection dropped are
    /// skipped).
    pub fn composed_fields(&self, document: &Value) -> crate::params::CallerFields {
        let mut out = crate::params::CallerFields::default();
        let rows = document
            .get("matrix")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for (stream, ptr, param) in &self.caller_fields {
            if let Some(i) = rows
                .iter()
                .position(|r| r.get("id").and_then(Value::as_str) == Some(stream.as_str()))
            {
                out.insert(format!("/matrix/{i}{ptr}"), param.clone());
            }
        }
        out
    }
}

/// [`expand`], recording where caller free text landed.
pub fn expand_traced(
    src: &SourceTemplate,
    supplied: &SuppliedParams,
    mode: BindMode,
    env: Option<&EnvOverlay>,
) -> CliResult<Expanded> {
    if !has_for_each(src) {
        return Ok(Expanded {
            template: src.clone(),
            caller_fields: Vec::new(),
        });
    }
    src.validate()?;
    let spec: ParamsSpec = match env {
        Some(env) => {
            let mut v = serde_json::to_value(&src.params)
                .map_err(|e| CliError::Internal(format!("for_each: params: {e}")))?;
            crate::interpolate::interpolate_value_with_env(&mut v, env)?;
            serde_json::from_value(v)
                .map_err(|e| CliError::Config(format!("invalid `params:` block: {e}")))?
        }
        None => unresolved_list_defaults(src, src.params.clone(), supplied, mode)?,
    };
    let own: SuppliedParams = supplied
        .iter()
        .filter(|(k, _)| spec.contains_key(*k))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let bound = crate::params::resolve(&spec, &own, mode)?;

    let mut streams = Vec::with_capacity(src.streams.len());
    let mut caller_fields = Vec::new();
    // Rendered name → where it came from, for the duplicate error.
    let mut origin: BTreeMap<String, String> = src
        .streams
        .iter()
        .filter(|s| s.for_each.is_none())
        .map(|s| (s.name.clone(), format!("stream '{}'", s.name)))
        .collect();
    for s in &src.streams {
        if s.for_each.is_none() {
            streams.push(s.clone());
            continue;
        }
        let what = format!("source-template '{}': stream '{}'", src.name, s.name);
        let pname = list_param(s)?;
        let p = &spec[pname];
        let items_spec = p.items.as_deref().cloned().unwrap_or_default();
        let empty = Vec::new();
        let elems = bound
            .values
            .get(pname)
            .and_then(Value::as_array)
            .unwrap_or(&empty);
        for (i, el) in elems.iter().enumerate() {
            let item = Item {
                var: var(s),
                value: el,
                label: format!("{pname}[{i}]"),
                guarded: bound.caller_supplied.contains(pname),
                items: &items_spec,
                traced: bound.caller_supplied.contains(pname) && !p.trusted,
            };
            let name = render_name(s, &item, &bound, &what)?;
            if let Some(prev) = origin.insert(name.clone(), format!("item {}", item.label)) {
                return Err(CliError::Config(format!(
                    "{what}: item {} renders the name '{name}', which {prev} already has — \
                     every row needs its own name",
                    item.label
                )));
            }
            let mut parts = rendered_parts(s);
            let mut traced = Vec::new();
            render_tree(&mut parts, &item, None, "", &mut traced).map_err(|e| match e {
                CliError::Config(m) => CliError::Config(format!("{what}: {m}")),
                other => other,
            })?;
            // A row's source config and transforms reach connectors; its
            // keys, write modes and name are checked as stream data.
            caller_fields.extend(
                traced
                    .into_iter()
                    .filter(|p| p.starts_with("/source/config") || p.starts_with("/transforms/"))
                    .map(|p| (name.clone(), p, pname.to_string())),
            );
            let mut out = s.clone();
            out.name = name;
            out.for_each = None;
            out.r#as = None;
            out.name_case = None;
            out.source = serde_json::from_value(parts["source"].take())
                .map_err(|e| CliError::Config(format!("{what}: `source`: {e}")))?;
            out.transforms = serde_json::from_value(parts["transforms"].take())
                .map_err(|e| CliError::Config(format!("{what}: `transforms`: {e}")))?;
            out.primary_keys = serde_json::from_value(parts["primary_keys"].take())
                .map_err(|e| CliError::Config(format!("{what}: `primary_keys`: {e}")))?;
            out.write = serde_json::from_value(parts["write"].take())
                .map_err(|e| CliError::Config(format!("{what}: `write`: {e}")))?;
            streams.push(out);
        }
    }
    let mut expanded = src.clone();
    expanded.streams = streams;
    expanded.validate()?;
    Ok(Expanded {
        template: expanded,
        caller_fields,
    })
}

/// [`expand`] with placeholder items — the structural view of a template
/// (`hub check`, registration, a row listing without params).
/// Without an env pass a list default that is still an `${…}` directive cannot
/// be expanded: a placeholder check uses placeholder items, a real run refuses.
fn unresolved_list_defaults(
    src: &SourceTemplate,
    mut spec: ParamsSpec,
    supplied: &SuppliedParams,
    mode: BindMode,
) -> CliResult<ParamsSpec> {
    for s in src.streams.iter().filter(|s| s.for_each.is_some()) {
        let pname = list_param(s)?;
        if supplied.contains_key(pname) {
            continue;
        }
        let Some(p) = spec.get_mut(pname) else {
            continue;
        };
        let directive = p
            .default
            .as_ref()
            .is_some_and(|d| d.to_string().contains("${"));
        if !directive {
            continue;
        }
        if mode == BindMode::Placeholder {
            p.default = None;
        } else {
            return Err(CliError::Config(format!(
                "source-template '{}': list param '{pname}' defaults to an `${{…}}` directive, \
                 which this run cannot resolve before its streams are expanded (a clustered \
                 server persists the expanded run); pass '{pname}' as a param",
                src.name
            )));
        }
    }
    Ok(spec)
}

pub fn expand_placeholders(src: &SourceTemplate) -> CliResult<SourceTemplate> {
    expand(src, &SuppliedParams::new(), BindMode::Placeholder, None)
}

/// The list params a template's `for_each` streams repeat over that neither
/// `supplied` nor a default provides.
pub fn unsupplied_lists(src: &SourceTemplate, supplied: &SuppliedParams) -> Vec<String> {
    let mut out: Vec<String> = src
        .streams
        .iter()
        .filter_map(|s| list_param(s).ok().filter(|_| s.for_each.is_some()))
        .filter(|p| {
            !supplied.contains_key(*p) && src.params.get(*p).is_none_or(|s| s.default.is_none())
        })
        .map(str::to_string)
        .collect();
    out.sort();
    out.dedup();
    out
}

/// The scalar params a `for_each` stream's `name` renders that neither
/// `supplied` nor a default provides; the rendered names are row ids.
pub fn unsupplied_name_params(src: &SourceTemplate, supplied: &SuppliedParams) -> Vec<String> {
    let mut out = Vec::new();
    for s in src.streams.iter().filter(|s| s.for_each.is_some()) {
        let mut rest = s.name.as_str();
        while let Some(i) = rest.find("${param.") {
            rest = &rest[i + "${param.".len()..];
            let Some(end) = rest.find('}') else { break };
            let name = &rest[..end];
            rest = &rest[end..];
            if !supplied.contains_key(name)
                && src.params.get(name).is_none_or(|p| p.default.is_none())
            {
                out.push(name.to_string());
            }
        }
    }
    out.sort();
    out.dedup();
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn src(yaml: &str) -> SourceTemplate {
        crate::yaml::from_str(yaml).expect("source template")
    }

    const TABLES: &str = r#"
kind: source-template
name: pg
params:
  pg_schema: { type: string, default: public }
  tables:
    type: list
    max_items: 5
    items:
      type: object
      properties:
        schema: { type: string, required: true }
        table: { type: string, required: true }
source:
  type: postgres
  config: { url: "postgres://localhost/db" }
streams:
  - name: audit
    source: { config: { query: "SELECT * FROM audit" } }
  - for_each: ${param.tables}
    as: t
    name: "${t.schema}_${t.table}"
    source: { config: { query: 'SELECT * FROM "${t.schema}"."${t.table}"' } }
    primary_keys: ["${t.table}_id"]
    write: overwrite
"#;

    const SCALARS: &str = r#"
kind: source-template
name: pg
params:
  pg_schema: { type: string, default: public }
  tables: { type: list, items: { type: string }, max_items: 500 }
  shards: { type: list, items: { type: int } }
source:
  type: postgres
  config: { url: "postgres://localhost/db" }
streams:
  - for_each: ${param.tables}
    as: t
    name: "${param.pg_schema}_${t}"
    source: { config: { query: 'SELECT * FROM "${param.pg_schema}"."${t}"' } }
  - for_each: ${param.shards}
    as: n
    name: "events_shard_${n}"
    source: { config: { query: "SELECT * FROM events WHERE shard_id = ${n}" } }
"#;

    fn params(v: Value) -> SuppliedParams {
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn unsupplied_name_params_are_the_ones_a_name_needs() {
        let t = src(&SCALARS.replace(
            "pg_schema: { type: string, default: public }",
            "pg_schema: { type: string }\n  region: { type: string }",
        ));
        assert_eq!(
            unsupplied_name_params(&t, &SuppliedParams::new()),
            ["pg_schema"]
        );
        assert!(unsupplied_name_params(&t, &params(json!({"pg_schema": "s"}))).is_empty());
        assert!(unsupplied_name_params(&src(SCALARS), &SuppliedParams::new()).is_empty());
        let mut broken = src(SCALARS);
        broken.streams[0].name = "x_${param.open".into();
        assert!(unsupplied_name_params(&broken, &SuppliedParams::new()).is_empty());
    }

    #[test]
    fn a_directive_list_default_resolves_with_env_and_is_refused_without() {
        let t = src(&SCALARS.replace(
            "tables: { type: list, items: { type: string }, max_items: 500 }",
            "tables: { type: list, items: { type: string }, default: \"${env:FE_TABLES}\" }",
        ));
        let shards = params(json!({"shards": [1]}));
        let env: EnvOverlay = [("FE_TABLES".to_string(), "orders,users".to_string())].into();
        let names =
            |t: &SourceTemplate| t.streams.iter().map(|s| s.name.clone()).collect::<Vec<_>>();
        let local = expand(&t, &shards, BindMode::Strict, Some(&env)).unwrap();
        assert_eq!(
            names(&local),
            ["public_orders", "public_users", "events_shard_1"]
        );

        let err = expand(&t, &shards, BindMode::Strict, None)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("list param 'tables' defaults to an `${…}` directive"),
            "{err}"
        );
        let supplied = params(json!({"shards": [1], "tables": ["a"]}));
        assert_eq!(
            names(&expand(&t, &supplied, BindMode::Strict, None).unwrap()),
            ["public_a", "events_shard_1"]
        );

        let checked = expand_placeholders(&t).unwrap();
        assert!(
            names(&checked).iter().all(|n| !n.contains("env")),
            "{:?}",
            names(&checked)
        );
        assert!(names(&checked).len() >= 3);
    }

    #[test]
    fn object_items_expand_into_one_stream_each() {
        let t = src(TABLES);
        t.validate().unwrap();
        assert!(has_for_each(&t));
        let e = expand(
            &t,
            &params(json!({"tables": [
                {"schema": "public", "table": "orders"},
                {"schema": "sales", "table": "customers"},
            ]})),
            BindMode::Strict,
            None,
        )
        .unwrap();
        assert!(!has_for_each(&e));
        assert_eq!(
            e.stream_names(),
            vec!["audit", "public_orders", "sales_customers"]
        );
        assert_eq!(
            e.streams[2].source.config["query"],
            r#"SELECT * FROM "sales"."customers""#
        );
        assert_eq!(e.streams[2].primary_keys, vec!["customers_id"]);
        assert!(e.streams[1].for_each.is_none() && e.streams[1].r#as.is_none());
    }

    #[test]
    fn scalar_items_render_names_and_typed_values_exactly() {
        let t = src(SCALARS);
        let e = expand(
            &t,
            &params(json!({"tables": "orders,customers", "shards": "[1, 9007199254740993]"})),
            BindMode::Strict,
            None,
        )
        .unwrap();
        assert_eq!(
            e.stream_names(),
            vec![
                "public_orders",
                "public_customers",
                "events_shard_1",
                "events_shard_9007199254740993"
            ]
        );
        assert_eq!(
            e.streams[1].source.config["query"],
            r#"SELECT * FROM "${param.pg_schema}"."customers""#,
            "`${{param.*}}` outside the name binds later, with the rest of the config"
        );
        assert_eq!(
            e.streams[3].source.config["query"],
            "SELECT * FROM events WHERE shard_id = 9007199254740993"
        );
    }

    #[test]
    fn a_whole_value_reference_keeps_the_item_type() {
        let t = src(&SCALARS.replace(
            "    source: { config: { query: \"SELECT * FROM events WHERE shard_id = ${n}\" } }",
            "    source: { config: { query: \"SELECT 1\", shard: \"${n}\" } }",
        ));
        let e = expand(
            &t,
            &params(json!({"tables": ["a"], "shards": [7]})),
            BindMode::Strict,
            None,
        )
        .unwrap();
        assert_eq!(e.streams[1].source.config["shard"], json!(7));
    }

    #[test]
    fn duplicate_or_invalid_names_are_refused_naming_the_items() {
        let t = src(SCALARS);
        let err = expand(
            &t,
            &params(json!({"tables": ["a", "a"], "shards": [1]})),
            BindMode::Strict,
            None,
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains(
                "item tables[1] renders the name 'public_a', which item tables[0] already has"
            ),
            "{err}"
        );
        // `1` and `"1"` coerce to the same int.
        let err = expand(
            &t,
            &params(json!({"tables": ["a"], "shards": [1, "1"]})),
            BindMode::Strict,
            None,
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("shards[1]") && err.contains("shards[0]"),
            "{err}"
        );
        let err = expand(
            &t,
            &params(json!({"tables": ["Sales-Orders"], "shards": [1]})),
            BindMode::Strict,
            None,
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("item tables[0] renders the name 'public_Sales-Orders'")
                && err.contains("name_case: snake"),
            "{err}"
        );
        let mut snake_t = t.clone();
        snake_t.streams[0].name_case = Some(NameCase::Snake);
        let e = expand(
            &snake_t,
            &params(json!({"tables": ["Sales-Orders", "lineItems"], "shards": [1]})),
            BindMode::Strict,
            None,
        )
        .unwrap();
        assert_eq!(e.streams[0].name, "public_sales_orders");
        assert_eq!(e.streams[1].name, "public_line_items");
        snake_t.streams[0].name = "${t}".into();
        let err = expand(
            &snake_t,
            &params(json!({"tables": ["--"], "shards": [1]})),
            BindMode::Strict,
            None,
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("after `name_case`") && !err.contains("set `name_case"),
            "{err}"
        );
        // An item may also collide with a declared stream.
        let mut static_too = src(TABLES);
        static_too.streams[0].name = "public_orders".into();
        let err = expand(
            &static_too,
            &params(json!({"tables": [{"schema": "public", "table": "orders"}]})),
            BindMode::Strict,
            None,
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("which stream 'public_orders' already has"),
            "{err}"
        );
    }

    #[test]
    fn items_are_held_to_the_param_splice_rules() {
        let t = src(SCALARS);
        let mut snake_t = t.clone();
        snake_t.streams[0].name_case = Some(NameCase::Snake);
        let err = expand(
            &snake_t,
            &params(json!({"tables": ["a\"; DROP TABLE x; --"], "shards": [1]})),
            BindMode::Strict,
            None,
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("tables[0]") && err.contains("could change the meaning"),
            "{err}"
        );
        // A placeholder (not caller text) is not screened.
        let e = expand_placeholders(&t).unwrap();
        assert_eq!(
            e.stream_names(),
            vec!["public_p1", "public_p2", "events_shard_1", "events_shard_2"]
        );
        // An unquoted text item in SQL is refused like an unquoted param.
        let unquoted = src(&SCALARS.replace(
            r#"'SELECT * FROM "${param.pg_schema}"."${t}"'"#,
            "'SELECT * FROM ${t}'",
        ));
        let err = expand(
            &unquoted,
            &params(json!({"tables": ["orders"], "shards": [1]})),
            BindMode::Strict,
            None,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("unquoted in SQL text"), "{err}");
        // A closed set lets it through.
        let mut closed = unquoted.clone();
        closed
            .params
            .get_mut("tables")
            .unwrap()
            .items
            .as_mut()
            .unwrap()
            .values = vec![json!("orders")];
        expand(
            &closed,
            &params(json!({"tables": ["orders"], "shards": [1]})),
            BindMode::Strict,
            None,
        )
        .unwrap();
    }

    #[test]
    fn an_item_cannot_assemble_a_directive() {
        let item_spec = ParamSpec::default();
        let item = Item {
            var: "t",
            value: &json!("$"),
            label: "tables[0]".into(),
            guarded: false,
            items: &item_spec,
            traced: false,
        };
        let err = render_string("${t}{env:HOME}", &item, Some("query"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("interpolation directive"), "{err}");
    }

    #[test]
    fn shape_errors_are_reported_at_validation() {
        for (from, to, want) in [
            (
                "for_each: ${param.tables}\n    as: t",
                "for_each: tables\n    as: t",
                "exactly `${param.NAME}`",
            ),
            (
                "for_each: ${param.tables}\n    as: t",
                "for_each: ${param.tables}",
                "needs `as:`",
            ),
            ("as: t\n", "as: param\n", "reserved name"),
            ("as: t\n", "as: T\n", "lower-case identifier"),
            (
                "name: \"${t.schema}_${t.table}\"",
                "name: fixed",
                "must use `${t}`",
            ),
            (
                "name: \"${t.schema}_${t.table}\"",
                "name: \"${t.schema}_${stream}\"",
                "not `${stream}`",
            ),
            (
                "name: \"${t.schema}_${t.table}\"",
                "name: \"${t}\"",
                "whole object item",
            ),
            (
                "name: \"${t.schema}_${t.table}\"",
                "name: \"${t.nope}\"",
                "names no field",
            ),
            (
                "for_each: ${param.tables}",
                "for_each: ${param.pg_schema}",
                "needs a `type: list` param",
            ),
            (
                "for_each: ${param.tables}",
                "for_each: ${param.nope}",
                "does not declare",
            ),
            (
                "    write: overwrite\n",
                "    write: overwrite\n    parent: audit\n",
                "cannot have a `parent`",
            ),
        ] {
            let yaml = TABLES.replacen(from, to, 1);
            assert_ne!(yaml, TABLES, "{from}");
            let err = src(&yaml).validate().unwrap_err().to_string();
            assert!(err.contains(want), "{to} → {err}");
        }
        let err = src(&format!(
            "{TABLES}  - name: child\n    parent: \"${{t.schema}}_${{t.table}}\"\n"
        ))
        .validate()
        .unwrap_err()
        .to_string();
        assert!(err.contains("cannot have children"), "{err}");
        let err = src(&SCALARS.replace("name: \"events_shard_${n}\"", "name: \"events_${n.id}\""))
            .validate()
            .unwrap_err()
            .to_string();
        assert!(err.contains("no field 'id'"), "{err}");
        let mut static_as = src(TABLES);
        static_as.streams[0].r#as = Some("x".into());
        assert!(
            static_as
                .validate()
                .unwrap_err()
                .to_string()
                .contains("needs `for_each:`")
        );
    }

    #[test]
    fn secret_items_may_not_name_a_row() {
        let secret = TABLES.replace(
            "table: { type: string, required: true }",
            "table: { type: string, required: true, secret: true }",
        );
        let err = src(&secret).validate().unwrap_err().to_string();
        assert!(err.contains("which is secret"), "{err}");
        let whole = SCALARS.replace(
            "tables: { type: list, items: { type: string }, max_items: 500 }",
            "tables: { type: list, secret: true, items: { type: string } }",
        );
        assert!(
            src(&whole)
                .validate()
                .unwrap_err()
                .to_string()
                .contains("which is secret")
        );
        let param = SCALARS.replace(
            "pg_schema: { type: string, default: public }",
            "pg_schema: { type: string, default: public, secret: true }",
        );
        assert!(
            src(&param)
                .validate()
                .unwrap_err()
                .to_string()
                .contains("which is secret")
        );
    }

    #[test]
    fn a_missing_optional_field_or_a_list_in_a_name_is_refused() {
        let t = src(&TABLES.replace(
            "schema: { type: string, required: true }",
            "schema: { type: string }",
        ));
        let err = expand(
            &t,
            &params(json!({"tables": [{"table": "x"}]})),
            BindMode::Strict,
            None,
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("tables[0]") && err.contains("no field 'schema'"),
            "{err}"
        );
        let lists = src(&SCALARS.replace(
            "name: \"events_shard_${n}\"",
            "name: \"e_${n}_${param.tables}\"",
        ));
        let err = expand(
            &lists,
            &params(json!({"tables": ["a"], "shards": [1]})),
            BindMode::Strict,
            None,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("a list — a name takes scalars"), "{err}");
    }

    #[test]
    fn env_defaults_resolve_before_names_render() {
        let t = src(&SCALARS.replace(
            "pg_schema: { type: string, default: public }",
            "pg_schema: { type: string, default: \"${env:FOREACH_SCHEMA}\" }",
        ));
        let env: EnvOverlay = [("FOREACH_SCHEMA".to_string(), "sales".to_string())]
            .into_iter()
            .collect();
        let e = expand(
            &t,
            &params(json!({"tables": ["a"], "shards": [1]})),
            BindMode::Strict,
            Some(&env),
        )
        .unwrap();
        assert_eq!(e.streams[0].name, "sales_a");
    }

    #[test]
    fn unsupplied_lists_name_what_a_caller_must_pass() {
        let t = src(SCALARS);
        assert_eq!(
            unsupplied_lists(&t, &SuppliedParams::new()),
            vec!["shards", "tables"]
        );
        assert_eq!(
            unsupplied_lists(&t, &params(json!({"tables": ["a"]}))),
            vec!["shards"]
        );
    }

    #[test]
    fn snake_case_normalisation() {
        assert_eq!(snake("Sales-Orders"), "sales_orders");
        assert_eq!(snake("lineItems2"), "line_items2");
        assert_eq!(snake("__a  b__"), "a_b");
        assert_eq!(snake("ABC"), "abc");
    }

    #[test]
    fn destinations_vary_only_through_the_loop_variable() {
        let t = src(TABLES);
        let s = &t.streams[1];
        assert!(destination_varies(
            &json!({"table_id": "${t.schema}_${t.table}"}),
            s
        ));
        assert!(!destination_varies(
            &json!({"table_id": "${param.table}"}),
            s
        ));
    }

    /// #989: an item a caller supplied is recorded where it lands in a
    /// stream's source config and transforms, and maps onto the composed
    /// document's matrix row of that stream; an int item, a closed field and
    /// a `trusted` list are not recorded.
    #[test]
    fn caller_items_are_traced_into_the_composed_rows() {
        let t = src(r#"
kind: source-template
name: files
params:
  dirs:
    type: list
    items:
      type: object
      properties:
        name: { type: string, required: true }
        dir: { type: string, required: true }
        fmt: { type: string, values: [csv, jsonl] }
  ids: { type: list, items: { type: int } }
  ok: { type: list, items: { type: string }, trusted: true }
source:
  type: file
  config: { path: "/data" }
streams:
  - for_each: ${param.dirs}
    as: d
    name: "f_${d.name}"
    source: { config: { path: "${d.dir}/*.${d.fmt}", format: "${d.fmt}" } }
  - for_each: ${param.ids}
    as: n
    name: "id_${n}"
    source: { config: { path: "/data/${n}.csv" } }
  - for_each: ${param.ok}
    as: o
    name: "ok_${o}"
    source: { config: { path: "${o}" } }
"#);
        let e = expand_traced(
            &t,
            &params(json!({
                "dirs": [{ "name": "a", "dir": "/etc", "fmt": "csv" }],
                "ids": [7],
                "ok": ["x"],
            })),
            BindMode::Strict,
            None,
        )
        .unwrap();
        assert_eq!(
            e.caller_fields,
            vec![(
                "f_a".to_string(),
                "/source/config/path".to_string(),
                "dirs".to_string()
            )]
        );
        let doc = json!({ "matrix": [{ "id": "id_7" }, { "id": "f_a" }] });
        let fields = e.composed_fields(&doc);
        assert!(fields.covers("/matrix/1/source/config/path"));
        assert!(!fields.covers("/matrix/0"));
        assert!(e.composed_fields(&json!({})).is_empty());
        // Defaults bound by the author are never traced.
        let d = src(r#"
kind: source-template
name: files
params:
  dirs: { type: list, items: { type: string }, default: ["etc"] }
source: { type: file, config: { path: "/data" } }
streams:
  - for_each: ${param.dirs}
    as: d
    name: "f_${d}"
    source: { config: { path: "/data/${d}" } }
"#);
        let traced = expand_traced(&d, &params(json!({})), BindMode::Strict, None).unwrap();
        assert!(traced.caller_fields.is_empty());
    }
}
