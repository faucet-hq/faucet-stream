//! The external resources a connector config names, found through its
//! schema's `x-faucet-resource` annotations ([`faucet_core::resolve`]), and the
//! rule a community template is held to: it may not read the machine it runs
//! on — no local path, no local or private network address, no `file:` URL,
//! no program, no listener, no setting that turns off verification, and no
//! authentication with the machine's own identity.

use faucet_core::resolve::{ResourceKind, ResourceRef, annotated_resources};
use serde_json::Value;
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, OnceLock};

type SchemaCache = Mutex<HashMap<String, Arc<Vec<Value>>>>;

fn schema_cache() -> &'static SchemaCache {
    static CACHE: OnceLock<SchemaCache> = OnceLock::new();
    CACHE.get_or_init(Default::default)
}

/// The config schemas of connector `kind` (its source and its sink, as
/// registered — built-in or plugin) and of a transform of that name, whose
/// config can name resources too (a `sql` transform's reference relations).
pub(crate) fn schemas(kind: &str) -> Arc<Vec<Value>> {
    if let Ok(cache) = schema_cache().lock()
        && let Some(hit) = cache.get(kind)
    {
        return hit.clone();
    }
    let found: Vec<Value> = [
        crate::registry::source_schema(kind).ok(),
        crate::registry::sink_schema(kind).ok(),
        crate::transforms::transform_schema(kind).ok(),
    ]
    .into_iter()
    .flatten()
    .collect();
    let found = Arc::new(found);
    if let Ok(mut cache) = schema_cache().lock() {
        cache.insert(kind.to_string(), found.clone());
    }
    found
}

/// The resources a `kind` connector's `config` names through annotated
/// schema fields.
pub fn connector_resources(kind: &str, config: &Value) -> Vec<ResourceRef> {
    let mut seen = BTreeSet::new();
    schemas(kind)
        .iter()
        .flat_map(|s| annotated_resources(s, config))
        .filter(|r| seen.insert((r.pointer.clone(), r.kind, r.value.clone())))
        .collect()
}

/// The resources a `kind` state store's `config` names through its own
/// schema's annotated fields ([`crate::state::state_store_schema`]),
/// independent of the connectors the build compiles.
pub fn state_resources(kind: &str, config: &Value) -> Vec<ResourceRef> {
    static CACHE: OnceLock<SchemaCache> = OnceLock::new();
    let cache = CACHE.get_or_init(Default::default);
    let schemas = match cache.lock().ok().and_then(|c| c.get(kind).cloned()) {
        Some(hit) => hit,
        None => {
            let found = Arc::new(
                crate::state::state_store_schema(kind)
                    .into_iter()
                    .collect::<Vec<_>>(),
            );
            if let Ok(mut c) = cache.lock() {
                c.insert(kind.to_string(), found.clone());
            }
            found
        }
    };
    schemas
        .iter()
        .flat_map(|s| annotated_resources(s, config))
        .collect()
}

/// Whether connector `kind` runs a program (its schema declares a
/// `command` field).
pub fn runs_program(kind: &str) -> bool {
    schemas(kind)
        .iter()
        .any(|s| faucet_core::resolve::declares(s, ResourceKind::Command))
}

/// What a community template's resource reaches on this machine, phrased by
/// class (never the value), or `None` when it reaches nothing local.
pub fn community_refusal(r: &ResourceRef) -> Option<&'static str> {
    match r.kind {
        ResourceKind::Path => faucet_core::resolve::local_path(&r.value)
            .is_some()
            .then_some("names a local path, which reads this machine"),
        ResourceKind::Url => {
            let scheme = r
                .value
                .split_once(':')
                .map(|(s, _)| s.trim().to_ascii_lowercase());
            if scheme.as_deref() == Some("file") {
                return Some("uses a `file:` URL, which reads this machine");
            }
            faucet_core::resolve::url_hosts_as(&r.value, r.syntax)
                .iter()
                .any(|h| faucet_core::resolve::is_local_host(h))
                .then_some("reaches a local or private network address")
        }
        ResourceKind::Command => Some("runs a program"),
        ResourceKind::Listen => Some("opens a listener on this machine"),
        ResourceKind::Unverified => Some("turns off certificate or host-key verification"),
        ResourceKind::HostCredentials => Some("authenticates with this machine's own credentials"),
        _ => None,
    }
}

/// A placeholder for a `${…}` token inside a value: a remote name, so a path
/// stays a path and a URL keeps its literal host.
const TOKEN_PLACEHOLDER: &str = "faucet-param.invalid";

/// The values a resource stands for, author-controlled ones only: a value
/// that is wholly `${param.NAME}` stands for that param's `default` and its
/// closed `values` (none when the operator supplies it); any other `${…}` in
/// a value is replaced by a remote placeholder.
pub fn author_values(value: &str, params: Option<&Value>) -> Vec<String> {
    let whole = value
        .trim()
        .strip_prefix("${param.")
        .and_then(|r| r.strip_suffix('}'))
        .filter(|n| !n.contains(['{', '}']));
    if let Some(name) = whole {
        let spec = params.and_then(|p| p.get(name));
        let mut out = Vec::new();
        if let Some(d) = spec.and_then(|s| s.get("default")).and_then(Value::as_str) {
            out.push(d.to_string());
        }
        if let Some(vals) = spec.and_then(|s| s.get("values")).and_then(Value::as_array) {
            out.extend(vals.iter().filter_map(Value::as_str).map(str::to_string));
        }
        return out;
    }
    // A value that starts with a param the operator supplies (no default)
    // lives under a root the operator chose: the author's suffix counts only
    // if it could climb out of it.
    if let Some((name, suffix)) = value
        .trim()
        .strip_prefix("${param.")
        .and_then(|r| r.split_once('}'))
        && !name.contains('{')
        && params
            .and_then(|p| p.get(name))
            .is_none_or(|spec| spec.get("default").is_none())
        && !suffix.split(['/', '\\']).any(|seg| seg.trim() == "..")
    {
        return Vec::new();
    }
    let mut out = String::new();
    let mut rest = value;
    while let Some(i) = rest.find("${") {
        out.push_str(&rest[..i]);
        match rest[i..].find('}') {
            Some(j) => {
                out.push_str(TOKEN_PLACEHOLDER);
                rest = &rest[i + j + 1..];
            }
            None => {
                rest = &rest[i..];
                break;
            }
        }
    }
    out.push_str(rest);
    vec![out]
}

/// The first resource in `doc` (a hub template or pipeline config) that a
/// community template may not name, as ``"<what> in `<where>`"``.
pub fn community_resource_use(doc: &Value) -> Option<String> {
    let params = doc.get("params");
    let mut found = None;
    // A sink template's `per_stream` keys are merged into its sink config.
    let merged_sink = doc
        .get("per_stream")
        .and_then(Value::as_object)
        .and_then(|per| {
            let sink = doc.get("sink")?;
            let mut config = sink
                .get("config")
                .cloned()
                .unwrap_or_else(|| Value::Object(Default::default()));
            let obj = config.as_object_mut()?;
            for (k, v) in per {
                obj.insert(k.clone(), v.clone());
            }
            Some(serde_json::json!({"sink": {"type": sink.get("type")?.clone(), "config": config}}))
        });
    let docs: Vec<&Value> = std::iter::once(doc).chain(merged_sink.as_ref()).collect();
    for d in docs {
        walk_connectors(d, d, &mut Vec::new(), &mut |at, kind, config, role| {
            if found.is_some() {
                return;
            }
            let mut refs = connector_resources(kind, config);
            refs.extend(state_resources(kind, config));
            for r in refs {
                for value in author_values(&r.value, params) {
                    let candidate =
                        ResourceRef::new(r.pointer.clone(), r.kind, value).with_syntax(r.syntax);
                    if let Some(why) = community_refusal(&candidate) {
                        found = Some(format!(
                            "{why} in `{at}.config{}`",
                            r.pointer.replace('/', ".")
                        ));
                        return;
                    }
                }
            }
            if let Some(role) = role {
                let identity = match role {
                    Role::Source => crate::registry::source_host_identity(kind, config),
                    Role::Sink => crate::registry::sink_host_identity(kind, config),
                };
                if identity.uses_host_identity() {
                    let fields = identity.fields().join("`, `");
                    found = Some(if fields.is_empty() {
                        format!("may authenticate with this machine's own credentials in `{at}`")
                    } else {
                        format!(
                            "authenticates with this machine's own credentials (`{fields}`) in `{at}`"
                        )
                    });
                }
            }
        });
    }
    found
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Source,
    Sink,
}

/// Keys whose `{type, config}` objects are not connectors (state stores,
/// lineage transports, auth providers, sync origins).
const NOT_CONNECTORS: &[&str] = &["state", "lineage", "transport", "auth", "catalog"];

/// Visit every `{type, config}` block in `v` (and every `{ref, config}`
/// override, typed from the block it names), with its location, kind, config
/// and connector role (`None` for state and other non-connector blocks).
fn walk_connectors(
    root: &Value,
    v: &Value,
    path: &mut Vec<String>,
    f: &mut impl FnMut(&str, &str, &Value, Option<Role>),
) {
    match v {
        Value::Object(map) => {
            let config = map.get("config").filter(|c| c.is_object());
            let kind = map
                .get("type")
                .or_else(|| map.get("kind"))
                .and_then(Value::as_str)
                .map(str::to_string)
                .or_else(|| config.and_then(|_| referenced_kind(root, path, map)));
            if let (Some(kind), Some(config)) = (kind, config) {
                let at = path.join(".");
                let in_any = |keys: &[&str]| path.iter().any(|p| keys.contains(&p.as_str()));
                let role = if in_any(NOT_CONNECTORS) {
                    None
                } else if in_any(&["sink", "sinks", "dlq"]) {
                    Some(Role::Sink)
                } else {
                    Some(Role::Source)
                };
                f(&at, &kind, config, role);
            }
            for (k, child) in map {
                path.push(k.clone());
                walk_connectors(root, child, path, f);
                path.pop();
            }
        }
        Value::Array(items) => {
            for (i, child) in items.iter().enumerate() {
                path.push(i.to_string());
                walk_connectors(root, child, path, f);
                path.pop();
            }
        }
        _ => {}
    }
}

/// The kind of the block a `{ref, config}` override (a stream's or a matrix
/// row's `source` / `sink`) names: the named template's, else the default one.
fn referenced_kind(
    root: &Value,
    path: &[String],
    map: &serde_json::Map<String, Value>,
) -> Option<String> {
    let side = path.last()?.as_str();
    let (one, many) = match side {
        "source" => ("source", "sources"),
        "sink" => ("sink", "sinks"),
        _ => return None,
    };
    let kind_of = |v: &Value| {
        v.get("type")
            .or_else(|| v.get("kind"))
            .and_then(Value::as_str)
            .map(str::to_string)
    };
    let scopes = [root, root.get("pipeline").unwrap_or(&Value::Null)];
    if let Some(r) = map.get("ref").and_then(Value::as_str) {
        return scopes
            .iter()
            .find_map(|s| s.get(many).and_then(|m| m.get(r)).and_then(kind_of));
    }
    scopes.iter().find_map(|s| s.get(one).and_then(kind_of))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn param_values_screen_only_what_the_author_wrote() {
        let params = json!({"p": {"default": "/home/x", "values": ["/a", "/b"]}, "q": {}});
        assert_eq!(
            author_values("${param.p}", Some(&params)),
            ["/home/x", "/a", "/b"]
        );
        assert!(author_values("${param.q}", Some(&params)).is_empty());
        assert!(author_values("${param.none}", None).is_empty());
        assert_eq!(
            author_values("https://${param.host}/x", None),
            ["https://faucet-param.invalid/x"]
        );
        assert_eq!(
            author_values("/data/${stream}.csv", None),
            ["/data/faucet-param.invalid.csv"]
        );
        assert!(author_values("${param.q}/${stream}.jsonl", Some(&params)).is_empty());
        assert_eq!(
            author_values("${param.q}/../etc", Some(&params)),
            ["faucet-param.invalid/../etc"],
            "a suffix that climbs out of the operator's root counts"
        );
        assert_eq!(
            author_values("plain ${unterminated", None),
            ["plain ${unterminated"]
        );
    }

    #[cfg(feature = "sink-jsonl")]
    #[test]
    fn a_sink_templates_per_stream_keys_are_screened() {
        let doc = json!({
            "sink": {"type": "jsonl", "config": {}},
            "per_stream": {"path": "/var/out/${stream}.jsonl"}
        });
        let found = community_resource_use(&doc).unwrap();
        assert!(found.contains("local path"), "{found}");
        let remote = json!({
            "sink": {"type": "jsonl", "config": {}},
            "per_stream": {"path": "${param.out}/${stream}.jsonl"},
            "params": {"out": {}}
        });
        assert!(community_resource_use(&remote).is_none());
    }

    #[test]
    fn refusals_are_by_class() {
        let r = |kind, v: &str| community_refusal(&ResourceRef::new("/x", kind, v));
        assert!(r(ResourceKind::Path, "/etc/passwd").is_some());
        assert!(r(ResourceKind::Path, "s3://bucket/key").is_none());
        assert!(r(ResourceKind::Url, "file:///etc").is_some());
        assert!(r(ResourceKind::Url, "http://127.0.0.1:8080/").is_some());
        assert!(r(ResourceKind::Url, "https://10.0.0.5/api").is_some());
        assert!(r(ResourceKind::Url, "https://api.example.com/v1").is_none());
        assert!(r(ResourceKind::Command, "tap").is_some());
        assert!(r(ResourceKind::Listen, "0.0.0.0:80").is_some());
        assert!(r(ResourceKind::Key, "dir/file").is_none());
        assert!(r(ResourceKind::Unverified, "true").is_some());
        assert!(r(ResourceKind::HostCredentials, "default").is_some());
    }

    #[cfg(feature = "sink-s3")]
    #[test]
    fn host_credentials_are_found_through_template_references() {
        let direct = json!({"sink": {"type": "s3", "config": {"bucket": "b"}}});
        let found = community_resource_use(&direct).unwrap();
        assert!(found.contains("own credentials"), "{found}");
        let by_ref = json!({
            "pipeline": {"sinks": {"t": {"type": "s3", "config": {"bucket": "b"}}}},
            "matrix": [{"id": "r", "sink": {"ref": "t", "config": {"bucket": "c"}}}]
        });
        assert!(community_resource_use(&by_ref).is_some());
        let by_default = json!({
            "pipeline": {"sink": {"type": "s3", "config": {"bucket": "b"}}},
            "matrix": [{"id": "r", "sink": {"config": {"bucket": "c"}}}]
        });
        assert!(community_resource_use(&by_default).is_some());
        let unnamed = json!({"pipeline": {"nodes": {"n": {"config": {"bucket": "c"}}}}});
        assert!(community_resource_use(&unnamed).is_none());
    }
}
