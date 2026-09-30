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
    file_sink_spellings(doc, &mut out);
    deprecated_kinds(doc, &mut out);
    out.into_iter().collect()
}

/// Sink kinds on the shared file writer, whose `mode` became `if_exists`.
const FILE_WRITER_SINK_KINDS: &[&str] = &["file", "s3", "gcs", "azure-blob", "sftp"];

/// `if_exists` values renamed with it: `(old, new)`.
const IF_EXISTS_RENAMES: &[(&str, &str)] =
    &[("overwrite", "replace"), ("error_if_exists", "error")];

/// `mode:` (now `if_exists:`) and its old values on file-writing sinks.
fn file_sink_spellings(doc: &Value, out: &mut BTreeSet<String>) {
    let pipeline = doc.get("pipeline");
    let template = |name: &str| -> Option<&Value> {
        let p = pipeline?;
        if name == "default"
            && let Some(s) = p.get("sink")
        {
            return Some(s);
        }
        p.get("sinks")?.get(name)
    };
    let kind_of = |sink: &Value| -> Option<String> {
        if let Some(t) = sink.get("type").and_then(Value::as_str) {
            return Some(t.to_string());
        }
        let r = sink.get("ref").and_then(Value::as_str).unwrap_or("default");
        template(r)?
            .get("type")
            .and_then(Value::as_str)
            .map(str::to_string)
    };

    let mut sinks: Vec<&Value> = Vec::new();
    let mut dlq_sinks: Vec<&Value> = Vec::new();
    if let Some(p) = pipeline {
        sinks.extend(p.get("sink"));
        if let Some(m) = p.get("sinks").and_then(Value::as_object) {
            sinks.extend(m.values());
        }
        dlq_sinks.extend(p.get("dlq").and_then(|d| d.get("sink")));
    }
    if let Some(rows) = doc.get("matrix").and_then(Value::as_array) {
        sinks.extend(rows.iter().filter_map(|r| r.get("sink")));
        dlq_sinks.extend(
            rows.iter()
                .filter_map(|r| r.get("dlq").and_then(|d| d.get("sink"))),
        );
    }

    let kinds = sinks
        .into_iter()
        .map(|s| (s, kind_of(s)))
        .chain(dlq_sinks.into_iter().map(|s| {
            let kind = s.get("type").and_then(Value::as_str).map(str::to_string);
            (s, kind)
        }));
    for (sink, kind) in kinds {
        let Some(kind) = kind.filter(|k| FILE_WRITER_SINK_KINDS.contains(&k.as_str())) else {
            continue;
        };
        let Some(cfg) = sink.get("config").and_then(Value::as_object) else {
            continue;
        };
        if cfg.contains_key("mode") {
            out.insert(format!(
                "{kind} sink: `mode` is now `if_exists` (the old key still works)"
            ));
        }
        let value = cfg
            .get("if_exists")
            .or_else(|| cfg.get("mode"))
            .and_then(Value::as_str);
        if let Some((old, new)) = IF_EXISTS_RENAMES.iter().find(|(o, _)| Some(*o) == value) {
            out.insert(format!(
                "{kind} sink: `if_exists: {old}` is now `if_exists: {new}` (the old value still works)"
            ));
        }
    }
}

/// Connector kinds replaced by `type: file` (#779).
pub const DEPRECATED_FILE_KINDS: &[&str] = &["csv", "jsonl", "parquet"];

/// Per deprecated kind: what it reads or writes, and what replaces it. The one
/// source of the deprecation wording on every CLI surface.
const FILE_KIND_REPLACEMENTS: &[(&str, &str, &str)] = &[
    (
        "csv",
        "CSV file",
        "`type: file` with `format: csv` (dialect options under `csv:`)",
    ),
    (
        "jsonl",
        "JSON Lines file",
        "`type: file` with a `.jsonl` path (or `format: json_lines`)",
    ),
    (
        "parquet",
        "Parquet file",
        "`type: file` with `format: parquet`, or `type: s3` for an S3 location",
    ),
];

/// What replaces a deprecated file kind, or `None` for any other kind.
pub fn file_kind_replacement(kind: &str) -> Option<&'static str> {
    FILE_KIND_REPLACEMENTS
        .iter()
        .find(|(k, _, _)| *k == kind)
        .map(|(_, _, r)| *r)
}

/// The load-time warning for a deprecated file kind.
pub fn deprecated_kind_warning(kind: &str) -> Option<String> {
    file_kind_replacement(kind).map(|r| {
        format!(
            "connector kind `{kind}` is deprecated: use {r}. It keeps working on its own \
             crate, unchanged, until the next major release"
        )
    })
}

/// The `faucet list` / schema-catalog description of a deprecated file kind.
pub fn deprecated_kind_description(kind: &str) -> Option<&'static str> {
    static LABELS: std::sync::LazyLock<Vec<(&'static str, String)>> =
        std::sync::LazyLock::new(|| {
            FILE_KIND_REPLACEMENTS
                .iter()
                .map(|(k, what, r)| (*k, format!("Deprecated {what} connector: use {r}.")))
                .collect()
        });
    LABELS
        .iter()
        .find(|(k, _)| *k == kind)
        .map(|(_, l)| l.as_str())
}

/// Connector blocks anywhere in the document that use a deprecated kind: an
/// object with a `type` and a `config`, or a bare `{ type }` override under a
/// `source` / `sink` key (a matrix row switching kind without new config).
fn deprecated_kinds(v: &Value, out: &mut BTreeSet<String>) {
    walk_kinds(v, None, out);
}

fn walk_kinds(v: &Value, parent: Option<&str>, out: &mut BTreeSet<String>) {
    match v {
        Value::Object(m) => {
            let connector = m.contains_key("config") || matches!(parent, Some("source" | "sink"));
            if connector
                && let Some(kind) = m.get("type").and_then(Value::as_str)
                && let Some(w) = deprecated_kind_warning(kind)
            {
                out.insert(w);
            }
            m.iter()
                .for_each(|(k, c)| walk_kinds(c, Some(k.as_str()), out));
        }
        Value::Array(a) => a.iter().for_each(|c| walk_kinds(c, None, out)),
        _ => {}
    }
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
    fn names_each_deprecated_file_kind_once() {
        let doc = json!({
            "pipeline": {
                "source": { "type": "csv", "config": { "path": "a.csv" } },
                "sinks": {
                    "a": { "type": "jsonl", "config": { "path": "a.jsonl" } },
                    "b": { "type": "parquet", "config": { "destination": { "type": "local_path", "path": "o/" } } }
                },
                "dlq": { "sink": { "type": "jsonl", "config": { "path": "d.jsonl" } } }
            },
            "matrix": [ { "id": "x", "source": { "type": "file", "config": { "path": "b.csv" } } } ]
        });
        let notes = deprecated_spellings(&doc);
        assert_eq!(notes.len(), 3, "{notes:?}");
        for kind in DEPRECATED_FILE_KINDS {
            assert!(
                notes
                    .iter()
                    .any(|n| n.starts_with(&format!("connector kind `{kind}`"))),
                "{notes:?}"
            );
        }
        assert!(deprecated_spellings(&json!({"a": {"type": "csv"}})).is_empty());
    }

    #[test]
    fn the_file_sinks_old_mode_spellings_warn() {
        let doc = json!({
            "pipeline": {
                "sink": { "type": "file", "config": { "path": "a.jsonl", "mode": "append" } },
                "sinks": {
                    "lake": { "type": "s3", "config": { "if_exists": "overwrite" } },
                    "db": { "type": "postgres", "config": { "mode": "overwrite" } }
                },
                "dlq": { "sink": { "type": "sftp", "config": { "mode": "error_if_exists" } } }
            },
            "matrix": [
                { "id": "a", "sink": { "config": { "if_exists": "replace" } } },
                { "id": "b", "sink": { "ref": "lake", "config": { "mode": "append" } } },
                { "id": "c", "dlq": { "sink": { "type": "gcs", "config": { "if_exists": "error" } } } }
            ]
        });
        assert_eq!(
            deprecated_spellings(&doc),
            vec![
                "file sink: `mode` is now `if_exists` (the old key still works)",
                "s3 sink: `if_exists: overwrite` is now `if_exists: replace` (the old value still works)",
                "s3 sink: `mode` is now `if_exists` (the old key still works)",
                "sftp sink: `if_exists: error_if_exists` is now `if_exists: error` (the old value still works)",
                "sftp sink: `mode` is now `if_exists` (the old key still works)",
            ]
        );
        let current = json!({
            "pipeline": { "sink": { "type": "file", "config": { "if_exists": "append" } } }
        });
        assert!(deprecated_spellings(&current).is_empty());
    }

    #[test]
    fn a_bare_kind_override_on_a_matrix_row_warns() {
        let doc = json!({
            "matrix": [
                { "id": "x", "sink": { "type": "csv" } },
                { "id": "y", "source": { "type": "parquet", "ref": "t" } },
                { "id": "z", "sink": { "type": "file" } }
            ]
        });
        let notes = deprecated_spellings(&doc);
        assert_eq!(notes.len(), 2, "{notes:?}");
        assert!(notes[0].starts_with("connector kind `csv`"), "{notes:?}");
        assert!(
            notes[1].starts_with("connector kind `parquet`"),
            "{notes:?}"
        );
    }

    #[test]
    fn every_surface_shares_one_replacement_per_kind() {
        for kind in DEPRECATED_FILE_KINDS {
            let r = file_kind_replacement(kind).unwrap();
            assert!(deprecated_kind_warning(kind).unwrap().contains(r));
            assert!(deprecated_kind_description(kind).unwrap().contains(r));
        }
        assert!(
            deprecated_kind_description("parquet")
                .unwrap()
                .contains("`type: s3`")
        );
        assert!(file_kind_replacement("file").is_none());
        assert!(deprecated_kind_warning("file").is_none());
        assert!(deprecated_kind_description("file").is_none());
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
