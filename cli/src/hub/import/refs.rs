//! The documents an imported spec is made of, and `$ref` resolution across
//! them.
//!
//! A spec is one root document plus every document its `$ref`s name. The
//! loader ([`super::load_spec`]) fetches them; everything here is pure. A
//! reference may point inside its own document (`#/components/schemas/X`) or
//! at a relative file (`common.yaml#/X`). An absolute URL (`https://…`,
//! `file:///…`) or a network-path reference (`//host/…`) is refused unless
//! the caller allowed remote references.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;
use url::Url;

use crate::error::{CliError, CliResult};

/// Longest `$ref` → `$ref` chain followed before giving up.
const MAX_REF_HOPS: usize = 64;

/// The root document and every document it references, keyed by URL.
#[derive(Debug, Clone)]
pub struct SpecSet {
    root: Url,
    docs: BTreeMap<String, Value>,
}

impl SpecSet {
    /// A spec made of one document.
    pub fn single(root: Url, doc: Value) -> Self {
        let mut docs = BTreeMap::new();
        docs.insert(doc_key(&root), doc);
        Self { root, docs }
    }

    /// Add a referenced document.
    pub fn insert(&mut self, url: &Url, doc: Value) {
        self.docs.insert(doc_key(url), doc);
    }

    pub fn contains(&self, url: &Url) -> bool {
        self.docs.contains_key(&doc_key(url))
    }

    pub fn get(&self, url: &Url) -> Option<&Value> {
        self.docs.get(&doc_key(url))
    }

    pub fn len(&self) -> usize {
        self.docs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.docs.is_empty()
    }

    pub fn root_url(&self) -> &Url {
        &self.root
    }

    pub fn root_key(&self) -> String {
        doc_key(&self.root)
    }

    pub fn root(&self) -> &Value {
        self.docs.get(&self.root_key()).unwrap_or(&Value::Null)
    }

    /// Follow `v`'s `$ref` chain (`v` lives in document `base`) to a value
    /// that is not a reference, returning the document it lives in.
    pub fn deref(&self, base: &str, v: &Value) -> CliResult<(String, Value)> {
        let mut base = base.to_string();
        let mut cur = v.clone();
        let mut seen = BTreeSet::new();
        for _ in 0..MAX_REF_HOPS {
            let Some(r) = cur.get("$ref").and_then(Value::as_str) else {
                return Ok((base, cur));
            };
            let (doc_part, fragment) = split_ref(r);
            let target = if doc_part.is_empty() {
                base.clone()
            } else {
                let b = Url::parse(&base)
                    .map_err(|e| CliError::Config(format!("spec: bad document URL {base}: {e}")))?;
                doc_key(&b.join(doc_part).map_err(|e| {
                    CliError::Config(format!("spec: `$ref: {r}` does not resolve: {e}"))
                })?)
            };
            if !seen.insert(format!("{target}#{fragment}")) {
                return Err(CliError::Config(format!(
                    "spec: `$ref: {r}` is part of a reference cycle"
                )));
            }
            let doc = self.docs.get(&target).ok_or_else(|| {
                CliError::Config(format!(
                    "spec: `$ref: {r}` names a document that was not loaded"
                ))
            })?;
            cur = pointer(doc, fragment)
                .ok_or_else(|| CliError::Config(format!("spec: `$ref: {r}` points at nothing")))?
                .clone();
            base = target;
        }
        Err(CliError::Config(format!(
            "spec: more than {MAX_REF_HOPS} chained `$ref`s"
        )))
    }
}

/// The key a document is stored under: its URL without a fragment.
pub fn doc_key(url: &Url) -> String {
    let mut u = url.clone();
    u.set_fragment(None);
    u.to_string()
}

/// `common.yaml#/a/b` → (`common.yaml`, `/a/b`).
pub fn split_ref(r: &str) -> (&str, &str) {
    match r.split_once('#') {
        Some((d, f)) => (d, f),
        None => (r, ""),
    }
}

/// Resolve a JSON Pointer fragment (`/a/b~1c`, percent-encoded or not).
pub fn pointer<'a>(doc: &'a Value, fragment: &str) -> Option<&'a Value> {
    if fragment.is_empty() {
        return Some(doc);
    }
    let decoded = percent_decode(fragment);
    let mut cur = doc;
    for raw in decoded.strip_prefix('/')?.split('/') {
        let seg = raw.replace("~1", "/").replace("~0", "~");
        cur = match cur {
            Value::Object(m) => m.get(&seg)?,
            Value::Array(a) => a.get(seg.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(cur)
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && let Some(hex) = s.get(i + 1..i + 3)
            && let Ok(b) = u8::from_str_radix(hex, 16)
        {
            out.push(b);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Keys whose value is a map of names (each entry an object of its own kind),
/// so a member called `example` or `default` is a name, not a keyword.
const NAME_MAPS: &[&str] = &[
    "properties",
    "patternProperties",
    "$defs",
    "definitions",
    "schemas",
    "parameters",
    "responses",
    "headers",
    "securitySchemes",
    "securityDefinitions",
    "requestBodies",
    "links",
    "callbacks",
    "pathItems",
    "paths",
    "content",
    "variables",
    "encoding",
    "dependentSchemas",
];

/// Keys whose value is literal data in OpenAPI, where a `$ref` key is not a
/// reference and is never followed.
fn literal_key(k: &str) -> bool {
    matches!(
        k,
        "example" | "default" | "enum" | "const" | "externalValue"
    ) || k.starts_with("x-")
}

/// Every `$ref` string in a document, outside literal data (`example`,
/// `default`, `enum`, `x-*` extensions, an Example Object's `value`).
pub fn collect_refs(v: &Value, out: &mut BTreeSet<String>) {
    walk_refs(v, Ctx::Keyword, out);
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Ctx {
    Keyword,
    Names,
    Examples,
    Example,
}

fn walk_refs(v: &Value, ctx: Ctx, out: &mut BTreeSet<String>) {
    match v {
        Value::Object(m) => {
            for (k, x) in m {
                let child = match ctx {
                    Ctx::Names => Ctx::Keyword,
                    Ctx::Examples => Ctx::Example,
                    Ctx::Example if k == "value" => continue,
                    _ if k == "$ref" => {
                        if let Some(s) = x.as_str() {
                            out.insert(s.to_string());
                        }
                        continue;
                    }
                    _ if literal_key(k) => continue,
                    _ if k == "examples" => {
                        if x.is_array() {
                            continue;
                        }
                        Ctx::Examples
                    }
                    _ if NAME_MAPS.contains(&k.as_str()) => Ctx::Names,
                    _ => Ctx::Keyword,
                };
                walk_refs(x, child, out);
            }
        }
        Value::Array(a) => a.iter().for_each(|x| walk_refs(x, Ctx::Keyword, out)),
        _ => {}
    }
}

/// The document a reference found in `base` needs loaded, or `None` for a
/// same-document reference. Without `allow_remote` only relative references
/// are followed, and only to the root spec's origin (scheme, host and port)
/// and, for a local spec, inside the root spec's directory. A `file:`
/// reference is never followed from a remote document.
pub fn ref_target(root: &Url, base: &Url, r: &str, allow_remote: bool) -> CliResult<Option<Url>> {
    let (doc_part, _) = split_ref(r);
    if doc_part.is_empty() {
        return Ok(None);
    }
    let refuse = |why: &str| {
        Err(CliError::Config(format!(
            "spec: `$ref: {r}` {why}; only same-document and relative references inside the \
             spec's location are followed (pass --allow-remote-refs to follow other locations)"
        )))
    };
    if doc_part.trim() != doc_part
        || doc_part.contains('\\')
        || doc_part.chars().any(char::is_control)
    {
        return Err(CliError::Config(format!(
            "spec: `$ref: {r}` is malformed (whitespace or a backslash in the location)"
        )));
    }
    let target = base
        .join(doc_part)
        .map_err(|e| CliError::Config(format!("spec: `$ref: {r}` does not resolve: {e}")))?;
    if base.scheme() != "file" && target.scheme() == "file" {
        return Err(CliError::Config(format!(
            "spec: `$ref: {r}` points a remote document at a local file, which is never followed"
        )));
    }
    if allow_remote {
        return Ok(Some(target));
    }
    if Url::parse(doc_part).is_ok() || doc_part.starts_with("//") {
        return refuse("names another location");
    }
    if target.scheme() != root.scheme()
        || target.host_str() != root.host_str()
        || target.port_or_known_default() != root.port_or_known_default()
    {
        return refuse("leaves the spec's origin");
    }
    if target.scheme() == "file" && !target.path().starts_with(&root_dir(root)) {
        return refuse("leaves the spec's directory");
    }
    Ok(Some(target))
}

/// The directory part of a URL's path, with its trailing `/`.
pub fn root_dir(root: &Url) -> String {
    let p = root.path();
    match p.rfind('/') {
        Some(i) => p[..=i].to_string(),
        None => "/".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn url(s: &str) -> Url {
        Url::parse(s).unwrap()
    }

    #[test]
    fn pointers_decode_escapes() {
        let doc = json!({"a": {"b/c": [1, {"~d": 2}]}, "p q": 3});
        assert_eq!(pointer(&doc, ""), Some(&doc));
        assert_eq!(pointer(&doc, "/a/b~1c/0"), Some(&json!(1)));
        assert_eq!(pointer(&doc, "/a/b~1c/1/~0d"), Some(&json!(2)));
        assert_eq!(pointer(&doc, "/p%20q"), Some(&json!(3)));
        assert_eq!(pointer(&doc, "/a/missing"), None);
        assert_eq!(pointer(&doc, "/a/b~1c/x"), None);
        assert_eq!(pointer(&json!(1), "/a"), None);
        assert_eq!(pointer(&doc, "a"), None);
        assert_eq!(percent_decode("%zz%41"), "%zzA");
    }

    #[test]
    fn refs_resolve_within_and_across_documents() {
        let root = url("file:///spec/api.yaml");
        let mut set = SpecSet::single(
            root.clone(),
            json!({"a": {"$ref": "#/b"}, "b": {"$ref": "common.yaml#/c"}}),
        );
        set.insert(
            &url("file:///spec/common.yaml"),
            json!({"c": {"type": "string"}}),
        );
        assert!(set.contains(&url("file:///spec/common.yaml#/c")));
        assert_eq!(set.root_url(), &root);
        let (base, v) = set.deref(&set.root_key(), &json!({"$ref": "#/a"})).unwrap();
        assert_eq!(base, "file:///spec/common.yaml");
        assert_eq!(v, json!({"type": "string"}));
    }

    #[test]
    fn ref_errors_are_typed() {
        let set = SpecSet::single(
            url("file:///spec/api.yaml"),
            json!({"x": {"$ref": "#/y"}, "y": {"$ref": "#/x"}, "z": {"$ref": "#/nope"}}),
        );
        let k = set.root_key();
        let cycle = set.deref(&k, &json!({"$ref": "#/x"})).unwrap_err();
        assert!(cycle.to_string().contains("cycle"), "{cycle}");
        let missing = set.deref(&k, &json!({"$ref": "#/z"})).unwrap_err();
        assert!(
            missing.to_string().contains("points at nothing"),
            "{missing}"
        );
        let unloaded = set
            .deref(&k, &json!({"$ref": "other.yaml#/a"}))
            .unwrap_err();
        assert!(unloaded.to_string().contains("not loaded"), "{unloaded}");
        let bad_base = set
            .deref("not a url", &json!({"$ref": "o.yaml#/a"}))
            .unwrap_err();
        assert!(
            bad_base.to_string().contains("bad document URL"),
            "{bad_base}"
        );
        assert_eq!(
            SpecSet::single(url("file:///x"), json!(1)).root(),
            &json!(1)
        );
    }

    #[test]
    fn long_chains_stop() {
        let mut doc = serde_json::Map::new();
        for i in 0..=MAX_REF_HOPS {
            doc.insert(format!("n{i}"), json!({"$ref": format!("#/n{}", i + 1)}));
        }
        let set = SpecSet::single(url("file:///s.json"), Value::Object(doc));
        let e = set
            .deref(&set.root_key(), &json!({"$ref": "#/n0"}))
            .unwrap_err();
        assert!(e.to_string().contains("chained"), "{e}");
    }

    #[test]
    fn collects_every_ref() {
        let mut out = BTreeSet::new();
        collect_refs(
            &json!({"a": [{"$ref": "x.yaml"}], "b": {"$ref": "#/a"}, "c": {"$ref": 3}}),
            &mut out,
        );
        assert_eq!(
            out.into_iter().collect::<Vec<_>>(),
            vec!["#/a".to_string(), "x.yaml".to_string()]
        );
    }

    #[test]
    fn refs_in_literal_data_are_not_followed() {
        let mut out = BTreeSet::new();
        collect_refs(
            &json!({
                "example": {"$ref": "/etc/passwd"},
                "x-vendor": {"$ref": "secret.yaml"},
                "default": {"$ref": "d.yaml"},
                "enum": [{"$ref": "e.yaml"}],
                "content": {"application/json": {"examples": {
                    "one": {"$ref": "#/components/examples/A", "value": {"$ref": "v.yaml"}},
                }}},
                "schema": {"examples": [{"$ref": "arr.yaml"}], "properties": {
                    "example": {"$ref": "prop.yaml"},
                    "default": {"$ref": "#/d"},
                }},
            }),
            &mut out,
        );
        assert_eq!(
            out.into_iter().collect::<Vec<_>>(),
            vec![
                "#/components/examples/A".to_string(),
                "#/d".to_string(),
                "prop.yaml".to_string()
            ]
        );
    }

    fn refused(root: &str, base: &str, r: &str, remote: bool) -> String {
        ref_target(&url(root), &url(base), r, remote)
            .unwrap_err()
            .to_string()
    }

    #[test]
    fn remote_references_need_an_opt_in() {
        let base = url("file:///spec/api.yaml");
        assert_eq!(ref_target(&base, &base, "#/a", false).unwrap(), None);
        assert_eq!(
            ref_target(&base, &base, "parts/c.yaml#/x", false).unwrap(),
            Some(url("file:///spec/parts/c.yaml"))
        );
        for r in [
            "https://example.com/s.yaml",
            "//example.com/s.yaml",
            "file:///etc/x",
        ] {
            let e = refused("file:///spec/api.yaml", "file:///spec/api.yaml", r, false);
            assert!(e.contains("--allow-remote-refs"), "{e}");
        }
        assert_eq!(
            ref_target(&base, &base, "https://example.com/s.yaml", true).unwrap(),
            Some(url("https://example.com/s.yaml"))
        );
        let web = url("https://example.com/v1/openapi.yaml");
        assert_eq!(
            ref_target(&web, &web, "schemas.yaml", false).unwrap(),
            Some(url("https://example.com/v1/schemas.yaml"))
        );
        let opaque = url("data:text/plain,x");
        assert!(ref_target(&opaque, &opaque, "rel.yaml", false).is_err());
        assert_eq!(root_dir(&url("data:x")), "/");
    }

    #[test]
    fn escapes_are_refused() {
        let web = "https://api.example.com/v1/openapi.yaml";
        for r in [
            "\\\\evil.example\\x.yaml",
            "/\\evil.example/x",
            " //evil.example/x",
            "x.yaml\n",
        ] {
            assert!(refused(web, web, r, false).contains("malformed"), "{r:?}");
        }
        assert!(
            refused(web, web, "https://api.example.com:8443/x.yaml", false)
                .contains("another location")
        );
        let local = "file:///home/u/spec/api.yaml";
        assert!(refused(local, local, "/etc/hosts", false).contains("directory"));
        assert!(refused(local, local, "../../../../etc/x.yaml", false).contains("directory"));
        assert!(
            refused(
                local,
                "file:///home/u/spec/parts/a.yaml",
                "../../x.yaml",
                false
            )
            .contains("directory")
        );
        assert!(
            ref_target(
                &url(local),
                &url("file:///home/u/spec/parts/a.yaml"),
                "../b.yaml",
                false
            )
            .unwrap()
            .is_some()
        );
        for remote in [false, true] {
            assert!(refused(web, web, "file:///etc/passwd", remote).contains("never followed"));
        }
        assert!(
            refused(web, "https://other.example.com/x.yaml", "y.yaml", false).contains("origin")
        );
    }
}
