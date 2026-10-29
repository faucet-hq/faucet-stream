//! Where a bound config holds text a caller chose (#989).
//!
//! A registered template is the operator's, so `faucet serve` loads it
//! trusted. The values a caller supplies for its params are not: a binding
//! records the JSON pointer of every config value a caller-supplied param
//! wrote, whole or spliced into longer text, so the server can hold exactly
//! those values to the caller reference policy after the trusted load.
//!
//! A param is recorded only when its caller value is free text: a `trusted:
//! true` param, a closed `values:` set, an `int` / `float` / `bool` and every
//! default are the author's. A computed param is recorded when its expression
//! splices a recorded param's text (`${param.x}`), not when it only switches
//! on one (`${map:x|…}`, whose results the author wrote).

use super::spec::{ParamSpec, ParamType, ParamsSpec};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

/// JSON pointers (RFC 6901) of the config values that hold caller-chosen
/// text, each with the params that wrote it. A pointer covers its whole
/// subtree (a list param substituted whole, an object key).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(transparent)]
pub struct CallerFields(BTreeMap<String, BTreeSet<String>>);

impl CallerFields {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Record that `param` wrote the value at `pointer`.
    pub fn insert(&mut self, pointer: impl Into<String>, param: impl Into<String>) {
        self.0
            .entry(pointer.into())
            .or_default()
            .insert(param.into());
    }

    /// Record every param of `params` at `pointer`.
    pub fn insert_all(&mut self, pointer: &str, params: &BTreeSet<String>) {
        for p in params {
            self.insert(pointer, p.clone());
        }
    }

    pub fn extend(&mut self, other: &CallerFields) {
        for (ptr, params) in &other.0 {
            self.insert_all(ptr, params);
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &BTreeSet<String>)> {
        self.0.iter()
    }

    /// Whether any recorded value overlaps `pointer` (is it, inside it, or
    /// holds it).
    pub fn covers(&self, pointer: &str) -> bool {
        self.0.keys().any(|p| overlaps(p, pointer))
    }

    /// The params whose values overlap `pointer`.
    pub fn params_at(&self, pointer: &str) -> BTreeSet<String> {
        self.0
            .iter()
            .filter(|(p, _)| overlaps(p, pointer))
            .flat_map(|(_, names)| names.iter().cloned())
            .collect()
    }

    /// The entries inside (or holding) `base`, relative to it: an entry that
    /// holds `base` becomes the whole of it (`""`).
    pub fn under(&self, base: &str) -> CallerFields {
        let mut out = CallerFields::default();
        for (p, params) in &self.0 {
            if is_within(base, p) {
                out.insert_all("", params);
            } else if let Some(rest) = p.strip_prefix(base)
                && rest.starts_with('/')
            {
                out.insert_all(rest, params);
            }
        }
        out
    }

    /// Every entry with `base` prepended.
    pub fn rebased(&self, base: &str) -> CallerFields {
        let mut out = CallerFields::default();
        for (p, params) in &self.0 {
            out.insert_all(&format!("{base}{p}"), params);
        }
        out
    }

    /// Every entry whose pointer starts with the segment `from` at depth
    /// `depth` (0 = the first segment) also recorded with `to` there, for a
    /// key the config reads under another name (an alias).
    pub fn with_alias(mut self, depth: usize, from: &str, to: &str) -> CallerFields {
        let mut extra = Vec::new();
        for (p, params) in &self.0 {
            let mut segs: Vec<&str> = p.split('/').skip(1).collect();
            if segs.get(depth) == Some(&from) {
                segs[depth] = to;
                extra.push((format!("/{}", segs.join("/")), params.clone()));
            }
        }
        for (p, params) in extra {
            self.insert_all(&p, &params);
        }
        self
    }
}

/// Escape one key for a JSON pointer.
pub fn escape(segment: &str) -> String {
    segment.replace('~', "~0").replace('/', "~1")
}

/// `base` + `/segment`.
pub fn child(base: &str, segment: &str) -> String {
    format!("{base}/{}", escape(segment))
}

/// Whether `inner` is `outer` or inside it.
fn is_within(inner: &str, outer: &str) -> bool {
    inner == outer
        || inner
            .strip_prefix(outer)
            .is_some_and(|rest| rest.starts_with('/'))
}

fn overlaps(a: &str, b: &str) -> bool {
    is_within(a, b) || is_within(b, a)
}

/// A JSON pointer as a dotted field name for a message (`/pipeline/dlq/x` →
/// `pipeline.dlq.x`).
pub fn display(pointer: &str) -> String {
    if pointer.is_empty() {
        return "(the whole config)".to_string();
    }
    pointer
        .split('/')
        .skip(1)
        .map(|s| s.replace("~1", "/").replace("~0", "~"))
        .collect::<Vec<_>>()
        .join(".")
}

/// Whether a caller value of `p` carries free text the author did not write.
pub(crate) fn has_free_text(p: &ParamSpec, v: &Value) -> bool {
    if p.trusted || !p.values.is_empty() {
        return false;
    }
    match (p.kind, v) {
        (ParamType::String, Value::String(_)) => true,
        (ParamType::List, Value::Array(items)) => {
            let item_spec = p.items.as_deref().cloned().unwrap_or_default();
            items.iter().any(|i| item_text(&item_spec, i))
        }
        _ => false,
    }
}

fn item_text(spec: &ParamSpec, v: &Value) -> bool {
    match v {
        Value::String(_) => spec.values.is_empty(),
        Value::Object(o) => o.iter().any(|(k, x)| {
            x.is_string()
                && spec
                    .properties
                    .get(k)
                    .is_none_or(|f| f.values.is_empty() && f.kind == ParamType::String)
        }),
        _ => false,
    }
}

/// Whether field `field` (`None` = the item itself) of a list item held to
/// `spec` (the list's `items`) is free text.
pub(crate) fn item_field_text(spec: &ParamSpec, item: &Value, field: Option<&str>) -> bool {
    match field {
        None => item_text(spec, item),
        Some(f) => {
            item.get(f).is_some_and(Value::is_string)
                && spec
                    .properties
                    .get(f)
                    .is_none_or(|s| s.values.is_empty() && s.kind == ParamType::String)
        }
    }
}

/// The params whose bound value is caller free text: supplied ones per
/// [`has_free_text`], and computed ones whose expression splices one.
pub(crate) fn free_text_params(
    spec: &ParamsSpec,
    values: &BTreeMap<String, Value>,
    supplied: &BTreeSet<String>,
) -> BTreeSet<String> {
    let mut out: BTreeSet<String> = supplied
        .iter()
        .filter(|n| {
            spec.get(*n)
                .zip(values.get(*n))
                .is_some_and(|(p, v)| p.computed.is_none() && has_free_text(p, v))
        })
        .cloned()
        .collect();
    loop {
        let before = out.len();
        for (name, p) in spec {
            let Some(expr) = &p.computed else { continue };
            if out.contains(name) {
                continue;
            }
            if spliced_params(expr).iter().any(|r| out.contains(r)) {
                out.insert(name.clone());
            }
        }
        if out.len() == before {
            return out;
        }
    }
}

/// The params a computed expression splices as text (`${param.NAME}`) — not
/// the switch of a `${map:NAME|…}`, whose results the author wrote.
fn spliced_params(expr: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = expr;
    while let Some(start) = rest.find("${") {
        let after = &rest[start + 2..];
        let Some(end) = after.find('}') else { break };
        if let Some(name) = after[..end].strip_prefix("param.") {
            out.push(name.trim().to_string());
        }
        rest = &after[end + 1..];
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fields(entries: &[(&str, &str)]) -> CallerFields {
        let mut f = CallerFields::default();
        for (p, n) in entries {
            f.insert(*p, *n);
        }
        f
    }

    #[test]
    fn pointers_overlap_by_whole_segments() {
        let f = fields(&[("/a/b", "x"), ("/list", "y")]);
        assert!(f.covers("/a/b"));
        assert!(f.covers("/a/b/c"));
        assert!(f.covers("/a"));
        assert!(!f.covers("/a/bc"));
        assert!(f.covers("/list/0"));
        assert_eq!(f.params_at("/a"), BTreeSet::from(["x".to_string()]));
        assert!(f.params_at("/z").is_empty());
        assert_eq!(f.len(), 2);
    }

    #[test]
    fn entries_move_between_bases() {
        let f = fields(&[("/m/0/config/path", "x"), ("/m/1", "y")]);
        let u = f.under("/m/0/config");
        assert_eq!(u, fields(&[("/path", "x")]));
        assert_eq!(f.under("/m/1/config"), fields(&[("", "y")]));
        assert_eq!(u.rebased("/z"), fields(&[("/z/path", "x")]));
        let a = fields(&[("/replication/snapshot", "x")]).with_alias(0, "replication", "mirror");
        assert!(a.covers("/mirror/snapshot/source"));
        assert!(a.covers("/replication/snapshot"));
        let mut e = CallerFields::default();
        e.extend(&f);
        assert_eq!(e, f);
        assert_eq!(e.iter().count(), 2);
    }

    #[test]
    fn keys_escape_and_display() {
        assert_eq!(child("/a", "b/c~d"), "/a/b~1c~0d");
        assert_eq!(display("/pipeline/dlq/a~1b"), "pipeline.dlq.a/b");
        assert_eq!(display(""), "(the whole config)");
    }

    #[test]
    fn only_free_text_a_caller_chose_is_recorded() {
        let spec: ParamsSpec = serde_json::from_value(serde_json::json!({
            "dir": {},
            "n": { "type": "int" },
            "pick": { "values": ["a", "b"] },
            "ok": { "trusted": true },
            "files": { "type": "list", "items": { "type": "string" } },
            "nums": { "type": "list", "items": { "type": "int" } },
            "objs": { "type": "list", "items": { "type": "object",
                "properties": { "k": { "values": ["x"] }, "p": {} } } },
            "closed": { "type": "list", "items": { "type": "object",
                "properties": { "k": { "values": ["x"] } } } },
            "joined": { "computed": "/data/${param.dir}" },
            "mapped": { "computed": "${map:pick|a=/one|*=/two}" },
            "chained": { "computed": "${param.joined}/x" },
        }))
        .unwrap();
        let values: BTreeMap<String, Value> = serde_json::from_value(serde_json::json!({
            "dir": "/etc", "n": 3, "pick": "a", "ok": "/etc", "files": ["a"], "nums": [1],
            "objs": [{ "k": "x", "p": "q" }], "closed": [{ "k": "x" }],
            "joined": "/data//etc", "mapped": "/one", "chained": "/data//etc/x",
        }))
        .unwrap();
        let supplied: BTreeSet<String> =
            ["dir", "n", "pick", "ok", "files", "nums", "objs", "closed"]
                .into_iter()
                .map(String::from)
                .collect();
        let got = free_text_params(&spec, &values, &supplied);
        let want: BTreeSet<String> = ["dir", "files", "objs", "joined", "chained"]
            .into_iter()
            .map(String::from)
            .collect();
        assert_eq!(got, want);
        let objs = spec["objs"].items.as_deref().unwrap();
        let item = serde_json::json!({ "k": "x", "p": "q" });
        assert!(item_field_text(objs, &item, Some("p")));
        assert!(!item_field_text(objs, &item, Some("k")));
        assert!(item_field_text(objs, &item, None));
        let closed = ParamSpec {
            values: vec![Value::String("x".into())],
            ..ParamSpec::default()
        };
        assert!(!item_field_text(&closed, &Value::String("x".into()), None));
    }
}
