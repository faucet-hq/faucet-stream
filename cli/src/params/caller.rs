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

/// How a caller's text fills the value a [`CallerFields`] entry points at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Wrote {
    /// The whole value is the caller's text (a whole `${param.x}`).
    Whole,
    /// The caller's text is part of the value, beside the author's (or the
    /// value is secret): only the caller's own text may be shown.
    Part,
}

/// JSON pointers (RFC 6901) of the config values that hold caller-chosen
/// text, each with the params (or `env:NAME` overrides) that wrote it. A
/// pointer covers its whole subtree (a list param substituted whole, an
/// object key). A pointer never holds secret text: under a key that does,
/// the entry stands at the key's object instead.
///
/// What a refusal may show travels with it in memory only — which entries
/// the caller wrote whole, and the caller's own texts — and is never
/// persisted: a stored set shows no values.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(transparent)]
pub struct CallerFields {
    entries: BTreeMap<String, BTreeSet<String>>,
    #[serde(skip)]
    #[schemars(skip)]
    whole: BTreeSet<(String, String)>,
    #[serde(skip)]
    #[schemars(skip)]
    texts: BTreeMap<String, BTreeSet<String>>,
}

/// The name an `env` override is recorded under.
pub fn env_name(var: &str) -> String {
    format!("env:{var}")
}

/// How a recorded name reads in a message: `param 'x'` / `env override 'X'`.
pub fn describe(name: &str) -> String {
    match name.strip_prefix("env:") {
        Some(var) => format!("env override '{var}'"),
        None => format!("param '{name}'"),
    }
}

impl CallerFields {
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// An empty set carrying this one's texts.
    pub fn empty_like(&self) -> CallerFields {
        CallerFields {
            texts: self.texts.clone(),
            ..Default::default()
        }
    }

    /// Record that `param`'s text is the whole value at `pointer`.
    pub fn insert(&mut self, pointer: impl Into<String>, param: impl Into<String>) {
        self.insert_as(pointer, param, Wrote::Whole);
    }

    /// Record that `param` wrote (`how`) the value at `pointer`. A name
    /// recorded both ways at one pointer counts as [`Wrote::Part`].
    pub fn insert_as(&mut self, pointer: impl Into<String>, param: impl Into<String>, how: Wrote) {
        let (pointer, param) = (pointer.into(), param.into());
        let fresh = self
            .entries
            .entry(pointer.clone())
            .or_default()
            .insert(param.clone());
        let key = (pointer, param);
        if how == Wrote::Whole && fresh {
            self.whole.insert(key);
        } else if how == Wrote::Part {
            self.whole.remove(&key);
        }
    }

    /// Record every param of `params` at `pointer`, as part of its value.
    pub fn insert_all(&mut self, pointer: &str, params: &BTreeSet<String>) {
        for p in params {
            self.insert_as(pointer, p.clone(), Wrote::Part);
        }
    }

    /// Record every name `other` has at `from` at `pointer`, as it wrote it.
    pub fn copy_entry(&mut self, other: &CallerFields, from: &str, pointer: &str) {
        for n in other.entries.get(from).into_iter().flatten() {
            let how = if other.whole.contains(&(from.to_string(), n.clone())) {
                Wrote::Whole
            } else {
                Wrote::Part
            };
            self.insert_as(pointer, n.clone(), how);
        }
    }

    /// Record a text `name`'s caller sent (shown in a refusal).
    pub fn add_text(&mut self, name: impl Into<String>, text: impl Into<String>) {
        let text = text.into();
        if !text.is_empty() {
            self.texts.entry(name.into()).or_default().insert(text);
        }
    }

    pub fn extend(&mut self, other: &CallerFields) {
        for ptr in other.entries.keys() {
            self.copy_entry(other, ptr, ptr);
        }
        for (n, t) in &other.texts {
            self.texts
                .entry(n.clone())
                .or_default()
                .extend(t.iter().cloned());
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &BTreeSet<String>)> {
        self.entries.iter()
    }

    /// Whether any recorded value overlaps `pointer` (is it, inside it, or
    /// holds it).
    pub fn covers(&self, pointer: &str) -> bool {
        self.entries.keys().any(|p| overlaps(p, pointer))
    }

    /// The params whose values overlap `pointer`.
    pub fn params_at(&self, pointer: &str) -> BTreeSet<String> {
        self.entries
            .iter()
            .filter(|(p, _)| overlaps(p, pointer))
            .flat_map(|(_, names)| names.iter().cloned())
            .collect()
    }

    /// Whether every name overlapping `pointer` wrote all of it: an entry at
    /// or above it written whole (an entry inside it wrote only part).
    fn all_whole(&self, pointer: &str) -> bool {
        let mut any = false;
        for (p, names) in &self.entries {
            if !overlaps(p, pointer) {
                continue;
            }
            for n in names {
                if !(is_within(pointer, p) && self.whole.contains(&(p.clone(), n.clone()))) {
                    return false;
                }
                any = true;
            }
        }
        any
    }

    fn texts_at(&self, pointer: &str) -> Vec<&str> {
        self.params_at(pointer)
            .iter()
            .filter_map(|n| self.texts.get(n))
            .flatten()
            .map(String::as_str)
            .collect()
    }

    /// `value` (the value at `pointer`) as a refusal may show it: whole when
    /// the caller wrote all of it, else only the caller's own texts with the
    /// rest elided (`…`) — `None` when nothing of it is known to be theirs.
    pub fn shown(&self, pointer: &str, value: &str) -> Option<String> {
        if self.all_whole(pointer) {
            return Some(value.to_string());
        }
        let out = elide(value, &self.texts_at(pointer));
        (out != ELIDED).then_some(out)
    }

    /// `text` (a refusal reason about the value at `pointer`) with every
    /// backquoted span that is not the caller's own text elided.
    pub fn shown_reason(&self, pointer: &str, text: &str) -> String {
        if self.all_whole(pointer) {
            return text.to_string();
        }
        let texts = self.texts_at(pointer);
        let mut out = String::with_capacity(text.len());
        let mut rest = text;
        while let Some(open) = rest.find('`') {
            let Some(close) = rest[open + 1..].find('`') else {
                break;
            };
            out.push_str(&rest[..=open]);
            out.push_str(&elide(&rest[open + 1..open + 1 + close], &texts));
            out.push('`');
            rest = &rest[open + close + 2..];
        }
        out.push_str(rest);
        out
    }

    /// The entries inside (or holding) `base`, relative to it: an entry that
    /// holds `base` becomes the whole of it (`""`).
    pub fn under(&self, base: &str) -> CallerFields {
        let mut out = self.empty_like();
        for p in self.entries.keys() {
            if is_within(base, p) {
                out.copy_entry(self, p, "");
            } else if let Some(rest) = p.strip_prefix(base)
                && rest.starts_with('/')
            {
                out.copy_entry(self, p, rest);
            }
        }
        out
    }

    /// Every entry with `base` prepended.
    pub fn rebased(&self, base: &str) -> CallerFields {
        let mut out = self.empty_like();
        for p in self.entries.keys() {
            out.copy_entry(self, p, &format!("{base}{p}"));
        }
        out
    }

    /// Every entry whose pointer starts with the segment `from` at depth
    /// `depth` (0 = the first segment) also recorded with `to` there, for a
    /// key the config reads under another name (an alias).
    pub fn with_alias(mut self, depth: usize, from: &str, to: &str) -> CallerFields {
        let mut extra = Vec::new();
        for p in self.entries.keys() {
            let mut segs: Vec<&str> = p.split('/').skip(1).collect();
            if segs.get(depth) == Some(&from) {
                segs[depth] = to;
                extra.push((p.clone(), format!("/{}", segs.join("/"))));
            }
        }
        let src = self.clone();
        for (p, alias) in extra {
            self.copy_entry(&src, &p, &alias);
        }
        self
    }
}

/// What [`elide`] writes for text that is not the caller's.
const ELIDED: &str = "…";

/// `value` with every run of text that is not one of `texts` replaced by `…`.
fn elide(value: &str, texts: &[&str]) -> String {
    let mut texts: Vec<&str> = texts.iter().copied().filter(|t| !t.is_empty()).collect();
    texts.sort_by_key(|t| std::cmp::Reverse(t.len()));
    let mut out = String::new();
    let mut rest = value;
    let mut gap = false;
    'scan: while let Some(c) = rest.chars().next() {
        for t in &texts {
            if rest.starts_with(t) {
                if gap {
                    out.push_str(ELIDED);
                    gap = false;
                }
                out.push_str(t);
                rest = &rest[t.len()..];
                continue 'scan;
            }
        }
        gap = true;
        rest = &rest[c.len_utf8()..];
    }
    if gap {
        out.push_str(ELIDED);
    }
    out
}

/// Where a binding walk is in the document it rewrites: the pointer of the
/// node as written (`from`) and as it will read (`at`). Under a key whose
/// text is secret, `at` stops at that key's object, so no recorded pointer
/// holds the secret.
#[derive(Debug, Clone, Default)]
pub struct Cursor {
    pub from: String,
    pub at: String,
    hidden: bool,
}

impl Cursor {
    pub fn index(&self, i: usize) -> Cursor {
        Cursor {
            from: format!("{}/{i}", self.from),
            at: if self.hidden {
                self.at.clone()
            } else {
                format!("{}/{i}", self.at)
            },
            hidden: self.hidden,
        }
    }

    /// The child under `key`, which reads as `new_key`; `secret` hides it.
    pub fn key(&self, key: &str, new_key: &str, secret: bool) -> Cursor {
        let hidden = self.hidden || secret;
        Cursor {
            from: child(&self.from, key),
            at: if hidden {
                self.at.clone()
            } else {
                child(&self.at, new_key)
            },
            hidden,
        }
    }

    /// Move what `carried` recorded at this node (as written) to where it
    /// reads.
    pub fn carry(&self, carried: &CallerFields, out: &mut CallerFields) {
        out.copy_entry(carried, &self.from, &self.at);
    }
}

/// Whether a rewritten key holds text registered as secret (a `secret: true`
/// param, an `${env:}` / `${file:}` value).
pub fn key_is_secret(new_key: &str) -> bool {
    crate::secrets::registry::redact(new_key) != new_key
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
