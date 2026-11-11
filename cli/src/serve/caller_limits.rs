//! Memory bounds a caller's config may not lift: a source's `batch_size: 0`
//! (one page holding the whole result) and every field a connector's schema
//! marks `x-faucet-caller-max` ([`faucet_core::resolve::CALLER_MAX_ANNOTATION`]).

use crate::config::PipelineConfig;
use crate::expand::ExpandedNode;
use crate::serve::error::ServeError;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

fn ceilings(is_source: bool, kind: &str) -> Arc<Vec<(String, u64)>> {
    type Cache = RwLock<HashMap<(bool, String), Arc<Vec<(String, u64)>>>>;
    static CACHE: OnceLock<Cache> = OnceLock::new();
    let cache = CACHE.get_or_init(Default::default);
    let key = (is_source, kind.to_string());
    if let Some(hit) = cache.read().ok().and_then(|c| c.get(&key).cloned()) {
        return hit;
    }
    let schema = if is_source {
        crate::registry::source_schema(kind).ok()
    } else {
        crate::registry::sink_schema(kind).ok()
    };
    let out = Arc::new(
        schema
            .map(|s| faucet_core::resolve::caller_max_fields(&s))
            .unwrap_or_default(),
    );
    if let Ok(mut c) = cache.write() {
        c.insert(key, Arc::clone(&out));
    }
    out
}

/// Why one connector's config lifts a bound a caller may not, if it does.
fn refusal(is_source: bool, kind: &str, config: &Value) -> Option<String> {
    if is_source && config.get("batch_size").and_then(Value::as_u64) == Some(0) {
        return Some(format!(
            "source `{kind}` `batch_size: 0` holds the whole result in one page; set a page size"
        ));
    }
    for (field, max) in ceilings(is_source, kind).iter() {
        let lifted = match config.get(field) {
            None => continue,
            Some(Value::Null) => Some("`null` (no bound)".to_string()),
            Some(v) => match v.as_u64() {
                Some(0) => Some("`0` (no bound)".to_string()),
                Some(n) if n > *max => Some(n.to_string()),
                _ => None,
            },
        };
        if let Some(what) = lifted {
            return Some(format!(
                "`{kind}` `{field}` is {what}; a submitted config may set 1 to {max}"
            ));
        }
    }
    None
}

/// Refuse a caller's config whose rows (or, for a topology, whose source,
/// sink and node configs) lift a memory bound.
pub fn screen(cfg: &PipelineConfig, nodes: &[ExpandedNode]) -> Result<(), ServeError> {
    let mut found = Vec::new();
    let mut check = |is_source: bool, kind: &str, config: &Value| {
        if let Some(why) = refusal(is_source, kind, config)
            && !found.contains(&why)
        {
            found.push(why);
        }
    };
    for n in nodes {
        check(true, &n.source.kind, &n.source.config);
        check(false, &n.sink.kind, &n.sink.config);
        if let Some(d) = &n.dlq {
            check(false, &d.sink.kind, &d.sink.config);
        }
    }
    if nodes.is_empty() {
        let doc = serde_json::to_value(cfg).unwrap_or_default();
        let pipeline = &doc["pipeline"];
        let template = |side: &str, r: Option<&str>| -> Option<String> {
            match r {
                Some(r) => pipeline[format!("{side}s")][r]["type"].as_str(),
                None => pipeline[side]["type"]
                    .as_str()
                    .or_else(|| pipeline[format!("{side}s")]["default"]["type"].as_str()),
            }
            .map(str::to_string)
        };
        for (side, is_source) in [("source", true), ("sink", false)] {
            let single = pipeline.get(side).into_iter();
            let named = pipeline
                .get(format!("{side}s"))
                .and_then(Value::as_object)
                .into_iter()
                .flat_map(|m| m.values());
            for spec in single.chain(named) {
                if let Some(kind) = spec["type"].as_str() {
                    check(is_source, kind, &spec["config"]);
                }
            }
        }
        for node in pipeline["nodes"]
            .as_object()
            .into_iter()
            .flat_map(|m| m.values())
        {
            let is_source = match node["kind"].as_str() {
                Some("source") => true,
                Some("sink") => false,
                _ => continue,
            };
            let side = if is_source { "source" } else { "sink" };
            let kind = node["type"]
                .as_str()
                .map(str::to_string)
                .or_else(|| template(side, node["ref"].as_str()));
            if let (Some(kind), Some(config)) = (kind, node.get("config")) {
                check(is_source, &kind, config);
            }
        }
    }
    if found.is_empty() {
        Ok(())
    } else {
        Err(ServeError::Forbidden(format!(
            "the config lifts a memory bound a submitted config may not: {}",
            found.join("; ")
        )))
    }
}

#[cfg(all(test, feature = "source-webhook", feature = "sink-jsonl"))]
mod tests {
    use super::*;

    fn load(doc: Value) -> (PipelineConfig, Vec<ExpandedNode>) {
        let cfg = PipelineConfig::from_value(doc).unwrap();
        let nodes = if crate::topology::is_topology(&cfg) {
            Vec::new()
        } else {
            crate::expand::expand(&cfg).unwrap()
        };
        (cfg, nodes)
    }

    fn webhook(config: Value) -> Value {
        serde_json::json!({ "version": 1, "pipeline": {
            "source": { "type": "webhook", "config": config },
            "sink": { "type": "jsonl", "config": { "path": "o.jsonl" } } } })
    }

    fn refused(doc: Value, needle: &str) {
        let (cfg, nodes) = load(doc.clone());
        let err = screen(&cfg, &nodes).unwrap_err();
        assert!(
            matches!(&err, ServeError::Forbidden(m) if m.contains(needle)),
            "{doc}: {err:?}"
        );
    }

    #[test]
    fn caller_buffer_settings_are_capped() {
        let (cfg, nodes) = load(webhook(serde_json::json!({
            "batch_size": 100, "timeout_secs": 3600, "max_payloads": 10,
            "max_buffered_bytes": 1024 })));
        screen(&cfg, &nodes).unwrap();
        let (cfg, nodes) = load(webhook(serde_json::json!({})));
        screen(&cfg, &nodes).expect("defaults keep their bounds");
        refused(
            webhook(serde_json::json!({ "batch_size": 0 })),
            "batch_size: 0",
        );
        refused(
            webhook(serde_json::json!({ "max_buffered_bytes": 0 })),
            "`max_buffered_bytes` is `0`",
        );
        refused(
            webhook(serde_json::json!({ "timeout_secs": 3601 })),
            "may set 1 to 3600",
        );
        refused(
            webhook(serde_json::json!({ "max_payloads": null })),
            "`null` (no bound)",
        );
        let topology = serde_json::json!({ "version": 1, "pipeline": {
            "sources": { "hook": { "type": "webhook", "config": {} } },
            "sinks": { "out": { "type": "jsonl", "config": { "path": "o.jsonl" } } },
            "nodes": {
                "in": { "kind": "source", "ref": "hook", "config": { "batch_size": 0 } },
                "w": { "kind": "sink", "ref": "out" }
            },
            "edges": [ { "from": "in", "to": "w" } ] } });
        refused(topology, "batch_size: 0");
    }

    #[cfg(feature = "source-postgres-cdc")]
    #[test]
    fn cdc_staging_needs_a_bound() {
        let doc = |v: Value| {
            serde_json::json!({ "version": 1, "pipeline": {
                "source": { "type": "postgres-cdc", "config": {
                    "connection_url": "postgres://h/db", "slot_name": "s",
                    "publication_name": "p", "max_staged_records": v } },
                "sink": { "type": "jsonl", "config": { "path": "o.jsonl" } },
                "state": { "type": "file", "config": { "path": "state" } } } })
        };
        refused(doc(Value::Null), "max_staged_records");
        refused(doc(serde_json::json!(1_000_001)), "1 to 1000000");
        let (cfg, nodes) = load(doc(serde_json::json!(5000)));
        screen(&cfg, &nodes).unwrap();
    }
}
