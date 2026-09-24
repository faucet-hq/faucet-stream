//! Deprecated config spellings (#670, RFC 0009).
//!
//! Renamed keys stay accepted through serde aliases, which erase which
//! spelling was used, so this pass reads the raw document before the typed
//! parse and says what to write instead. It never rejects anything.

use std::collections::BTreeSet;

use serde_json::Value;

/// REST source keys renamed in #670: `(old, new)`. Scoped to `type: rest`,
/// because Snowflake's `partition_concurrency` names Snowflake's own result
/// partitions and is not deprecated.
const REST_RENAMES: &[(&str, &str)] = &[
    ("partitions", "requests"),
    ("partition_concurrency", "request_concurrency"),
];

/// Every deprecated spelling in `doc`, as operator-facing messages (deduplicated,
/// in a stable order).
pub fn deprecated_spellings(doc: &Value) -> Vec<String> {
    let mut out = BTreeSet::new();
    if doc.get("replication").is_some() {
        out.insert("`replication:` is now `mirror:` (the old key still works)".to_string());
    }

    let pipeline = doc.get("pipeline");
    let templates = |name: &str| -> Option<&Value> {
        let p = pipeline?;
        if name == "default"
            && let Some(s) = p.get("source")
        {
            return Some(s);
        }
        p.get("sources")?.get(name)
    };
    let kind_of = |connector: &Value| -> Option<String> {
        if let Some(t) = connector.get("type").and_then(Value::as_str) {
            return Some(t.to_string());
        }
        let r = connector
            .get("ref")
            .and_then(Value::as_str)
            .unwrap_or("default");
        templates(r)?
            .get("type")
            .and_then(Value::as_str)
            .map(str::to_string)
    };

    let mut connectors: Vec<&Value> = Vec::new();
    if let Some(p) = pipeline {
        connectors.extend(p.get("source"));
        if let Some(m) = p.get("sources").and_then(Value::as_object) {
            connectors.extend(m.values());
        }
    }
    if let Some(rows) = doc.get("matrix").and_then(Value::as_array) {
        connectors.extend(rows.iter().filter_map(|r| r.get("source")));
    }
    for block in ["mirror", "replication"] {
        connectors.extend(
            doc.get(block)
                .and_then(|b| b.get("snapshot"))
                .and_then(|s| s.get("source")),
        );
    }

    for c in connectors {
        if kind_of(c).as_deref() != Some("rest") {
            continue;
        }
        let Some(cfg) = c.get("config").and_then(Value::as_object) else {
            continue;
        };
        for (old, new) in REST_RENAMES {
            if cfg.contains_key(*old) {
                out.insert(format!(
                    "rest source: `{old}` is now `{new}` (the old key still works)"
                ));
            }
        }
        if cfg
            .get("odata")
            .and_then(Value::as_object)
            .is_some_and(|o| o.contains_key("partition"))
        {
            out.insert(
                "rest source: `odata.partition` is now `odata.key_ranges` (the old key still works)"
                    .to_string(),
            );
        }
    }
    out.into_iter().collect()
}

/// Log each deprecated spelling once per load.
pub fn warn_deprecated(doc: &Value) {
    for m in deprecated_spellings(doc) {
        tracing::warn!("{m}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn finds_every_renamed_key_and_only_on_rest() {
        let doc = json!({
            "replication": {
                "snapshot": { "source": { "type": "rest", "config": { "partitions": [] } } }
            },
            "pipeline": {
                "source": { "type": "rest", "config": { "partition_concurrency": 2, "odata": { "partition": {} } } },
                "sources": {
                    "wh": { "type": "snowflake", "config": { "partition_concurrency": 4 } }
                }
            },
            "matrix": [
                { "id": "a", "source": { "config": { "partitions": [] } } },
                { "id": "b", "source": { "ref": "wh", "config": { "partition_concurrency": 1 } } }
            ]
        });
        assert_eq!(
            deprecated_spellings(&doc),
            vec![
                "`replication:` is now `mirror:` (the old key still works)",
                "rest source: `odata.partition` is now `odata.key_ranges` (the old key still works)",
                "rest source: `partition_concurrency` is now `request_concurrency` (the old key still works)",
                "rest source: `partitions` is now `requests` (the old key still works)",
            ]
        );
        warn_deprecated(&doc);
    }

    #[test]
    fn the_new_spellings_are_silent() {
        let doc = json!({
            "mirror": { "snapshot": { "source": { "type": "postgres", "config": {} } } },
            "pipeline": { "source": { "type": "rest", "config": { "requests": [], "request_concurrency": 2, "odata": { "key_ranges": {} } } } },
            "matrix": [ { "source": { "ref": "missing", "config": { "partitions": [] } } }, { "id": "x" } ]
        });
        assert!(deprecated_spellings(&doc).is_empty());
        assert!(deprecated_spellings(&json!("not an object")).is_empty());
    }
}
