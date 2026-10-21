//! Every connector config field that looks like it names an external
//! resource — a path, a URL, a command — carries the `x-faucet-resource`
//! schema annotation (#881), or is listed with a reason in
//! `scripts/resource-annotation-allow.txt`. `faucet serve` checks annotated
//! fields against its reference policy generically, so an un-annotated field
//! would be open to a caller-supplied config. Runs over every connector this
//! build compiles in (all of them in the CI `--all-features` jobs).

use faucet_cli::registry::{sink_kinds, sink_schema, source_kinds, source_schema};
use faucet_core::resolve::RESOURCE_ANNOTATION;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

const EXACT: &[&str] = &[
    "path",
    "paths",
    "file",
    "files",
    "dir",
    "directory",
    "url",
    "urls",
    "uri",
    "endpoint",
    "command",
    "executable",
    "program",
    "connection_string",
    "warehouse",
    "location",
    "database",
    "socket",
    "host",
    "hosts",
    "brokers",
    "servers",
    "bootstrap_servers",
    "connect_string",
];

const SUFFIXES: &[&str] = &[
    "_path",
    "_paths",
    "_file",
    "_files",
    "_dir",
    "_url",
    "_urls",
    "_uri",
    "_endpoint",
    "_command",
    "_cert",
    "_pkcs12",
    "_location",
    "_executable",
    "_host",
    "_hosts",
    "_servers",
    "_brokers",
];

fn looks_like_resource(name: &str) -> bool {
    EXACT.contains(&name) || SUFFIXES.iter().any(|s| name.ends_with(s))
}

fn deref<'a>(root: &'a Value, mut node: &'a Value) -> &'a Value {
    for _ in 0..64 {
        match node
            .get("$ref")
            .and_then(Value::as_str)
            .and_then(|r| r.strip_prefix('#'))
            .and_then(|p| root.pointer(p))
        {
            Some(next) => node = next,
            None => return node,
        }
    }
    node
}

fn alternatives<'a>(root: &'a Value, node: &'a Value, out: &mut Vec<&'a Value>, depth: usize) {
    if depth > 16 {
        return;
    }
    out.push(node);
    let r = deref(root, node);
    out.push(r);
    for key in ["oneOf", "anyOf", "allOf"] {
        if let Some(Value::Array(bs)) = r.get(key) {
            for b in bs {
                alternatives(root, b, out, depth + 1);
            }
        }
    }
}

fn is_string(root: &Value, node: &Value) -> bool {
    let mut alts = Vec::new();
    alternatives(root, node, &mut alts, 0);
    alts.iter().any(|a| {
        let types: Vec<&str> = match a.get("type") {
            Some(Value::String(t)) => vec![t.as_str()],
            Some(Value::Array(ts)) => ts.iter().filter_map(Value::as_str).collect(),
            _ => Vec::new(),
        };
        types.contains(&"string")
            || (types.contains(&"array")
                && a.get("items")
                    .filter(|i| i.is_object())
                    .is_some_and(|i| is_string(root, i)))
    })
}

fn is_annotated(root: &Value, node: &Value) -> bool {
    let mut alts = Vec::new();
    alternatives(root, node, &mut alts, 0);
    alts.iter().any(|a| a.get(RESOURCE_ANNOTATION).is_some())
}

/// `(dotted path, annotated)` for every resource-looking string field.
fn resource_like_fields(
    root: &Value,
    node: &Value,
    prefix: &str,
    out: &mut BTreeMap<String, bool>,
    depth: usize,
) {
    if depth > 12 {
        return;
    }
    let mut alts = Vec::new();
    alternatives(root, node, &mut alts, 0);
    for a in alts {
        if let Some(Value::Object(props)) = a.get("properties") {
            for (k, v) in props {
                let p = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{prefix}.{k}")
                };
                if looks_like_resource(k) && is_string(root, v) {
                    let annotated = is_annotated(root, v);
                    let e = out.entry(p.clone()).or_insert(annotated);
                    *e |= annotated;
                }
                resource_like_fields(root, v, &p, out, depth + 1);
            }
        }
        if let Some(items) = a.get("items").filter(|i| i.is_object()) {
            resource_like_fields(root, items, &format!("{prefix}[]"), out, depth + 1);
        }
    }
}

fn allow_list() -> BTreeSet<(String, String)> {
    let text = include_str!("../../scripts/resource-annotation-allow.txt");
    let mut out = BTreeSet::new();
    for (i, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (entry, reason) = line
            .split_once('#')
            .unwrap_or_else(|| panic!("line {}: an entry needs a `# reason`", i + 1));
        assert!(
            !reason.trim().is_empty(),
            "line {}: an entry needs a reason",
            i + 1
        );
        let mut parts = entry.split_whitespace();
        let (Some(connector), Some(field), None) = (parts.next(), parts.next(), parts.next())
        else {
            panic!("line {}: expected `<side>:<kind> <field>`", i + 1);
        };
        out.insert((connector.to_string(), field.to_string()));
    }
    out
}

fn connectors() -> Vec<(String, Value)> {
    let mut out = Vec::new();
    for kind in source_kinds() {
        out.push((format!("source:{kind}"), source_schema(kind).unwrap()));
    }
    for kind in sink_kinds() {
        out.push((format!("sink:{kind}"), sink_schema(kind).unwrap()));
    }
    out
}

#[test]
fn every_resource_field_is_annotated_or_allow_listed() {
    let allowed = allow_list();
    let mut missing = Vec::new();
    let mut used = BTreeSet::new();
    let mut compiled = BTreeSet::new();
    for (connector, schema) in connectors() {
        compiled.insert(connector.clone());
        let mut fields = BTreeMap::new();
        resource_like_fields(&schema, &schema, "", &mut fields, 0);
        for (field, annotated) in fields {
            let key = (connector.clone(), field.clone());
            if allowed.contains(&key) {
                used.insert(key);
            } else if !annotated {
                missing.push(format!("{connector} {field}"));
            }
        }
    }
    assert!(
        missing.is_empty(),
        "connector config fields that look like a path, URL or command but carry no \
         `x-faucet-resource` annotation (add `#[schemars(extend(\"x-faucet-resource\" = \
         \"path\"|\"url\"|\"command\"))]`, or list them with a reason in \
         scripts/resource-annotation-allow.txt):\n  {}",
        missing.join("\n  ")
    );
    let stale: Vec<String> = allowed
        .iter()
        .filter(|k| compiled.contains(&k.0) && !used.contains(*k))
        .map(|(c, f)| format!("{c} {f}"))
        .collect();
    assert!(
        stale.is_empty(),
        "scripts/resource-annotation-allow.txt lists fields that no longer exist or no \
         longer look like a resource:\n  {}",
        stale.join("\n  ")
    );
}

#[test]
fn the_heuristic_recognises_resource_names() {
    for name in [
        "path",
        "ca_path",
        "token_url",
        "connection_uri",
        "tls_cert",
        "executable",
        "host",
        "storage_host",
        "emulator_host",
        "brokers",
        "servers",
    ] {
        assert!(looks_like_resource(name), "{name}");
    }
    for name in [
        "batch_size",
        "pathological",
        "urlish",
        "file_format",
        "hostname_case",
    ] {
        assert!(!looks_like_resource(name), "{name}");
    }
}
