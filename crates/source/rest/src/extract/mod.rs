//! Record extraction from API responses.

pub mod jsonpath;

pub use jsonpath::extract_records;

use crate::config::RecordsMultiSpec;
use faucet_core::FaucetError;
use jsonpath_rust::JsonPath;
use serde_json::Value;
use std::collections::HashMap;

/// Central record-extraction entry point used by both the paginated stream loop
/// and the async-job fetch path. Routes to the right extraction mode:
///
/// - `records_multi` (#548) — several op-stamped arrays in one page.
/// - `record_ancestors` (#549) — nested `records_path` with enclosing ancestor
///   fields lifted onto each record.
/// - otherwise — the classic single `records_path` (or whole-body) extraction.
pub fn extract_configured(
    body: &Value,
    records_path: Option<&str>,
    record_ancestors: Option<&HashMap<String, String>>,
    records_multi: &[RecordsMultiSpec],
    op_field: &str,
) -> Result<Vec<Value>, FaucetError> {
    if !records_multi.is_empty() {
        return extract_multi(body, records_multi, op_field);
    }
    match record_ancestors {
        Some(anc) if !anc.is_empty() => {
            let rp = records_path.ok_or_else(|| {
                FaucetError::Config("rest: `record_ancestors` requires `records_path`".into())
            })?;
            extract_with_ancestors(body, rp, anc)
        }
        _ => extract_records(body, records_path),
    }
}

/// Multi-array fan-out (#548): extract every configured array and stamp each
/// record with its spec's op value under `op_field`. All records from all specs
/// are returned in one flat vec (declared order), for emission in one page.
pub fn extract_multi(
    body: &Value,
    specs: &[RecordsMultiSpec],
    op_field: &str,
) -> Result<Vec<Value>, FaucetError> {
    let mut out = Vec::new();
    for spec in specs {
        let records = extract_records(body, Some(&spec.path))?;
        for mut rec in records {
            if let Some(map) = rec.as_object_mut() {
                map.insert(op_field.to_owned(), Value::String(spec.op.clone()));
            }
            out.push(rec);
        }
    }
    Ok(out)
}

/// Split a nested `records_path` into `(ancestor_path, remainder)` at the FIRST
/// array wildcard `[*]`. For `"$.data[*].data.object"` this yields
/// `("$.data[*]", ".data.object")`. Returns `None` when there is no `[*]`.
fn split_ancestor_path(records_path: &str) -> Option<(String, String)> {
    let idx = records_path.find("[*]")?;
    let ancestor = format!("{}[*]", &records_path[..idx]);
    let remainder = records_path[idx + "[*]".len()..].to_owned();
    Some((ancestor, remainder))
}

/// Run a dot/bracket path *relative to* a node, returning the matched values.
/// An empty relative path yields the node itself.
fn query_relative(node: &Value, rel: &str) -> Result<Vec<Value>, FaucetError> {
    let rel = rel.trim();
    if rel.is_empty() {
        return Ok(vec![node.clone()]);
    }
    let query = if rel.starts_with('$') {
        rel.to_owned()
    } else if rel.starts_with('.') || rel.starts_with('[') {
        format!("${rel}")
    } else {
        format!("$.{rel}")
    };
    let results = node
        .query(&query)
        .map_err(|e| FaucetError::JsonPath(format!("{e}")))?;
    Ok(results.into_iter().cloned().collect())
}

/// Envelope-ancestor lifting (#549): select each `[*]` array-element ancestor,
/// resolve the leaf record(s) beneath it, and copy the named ancestor-relative
/// paths onto every emitted record.
pub fn extract_with_ancestors(
    body: &Value,
    records_path: &str,
    ancestors: &HashMap<String, String>,
) -> Result<Vec<Value>, FaucetError> {
    let (ancestor_path, remainder) = split_ancestor_path(records_path).ok_or_else(|| {
        FaucetError::Config(format!(
            "rest: `record_ancestors` requires a `records_path` containing an array wildcard `[*]` \
             (got '{records_path}')"
        ))
    })?;

    let ancestor_nodes = body
        .query(&ancestor_path)
        .map_err(|e| FaucetError::JsonPath(format!("{e}")))?;

    let mut out = Vec::new();
    for anc in ancestor_nodes {
        // Resolve the ancestor field values once per ancestor element.
        let mut lifted: Vec<(String, Value)> = Vec::with_capacity(ancestors.len());
        for (dest, rel) in ancestors {
            if let Some(v) = query_relative(anc, rel)?.into_iter().next() {
                lifted.push((dest.clone(), v));
            }
        }
        // Resolve the leaf record(s) beneath this ancestor and stamp them.
        for mut leaf in query_relative(anc, &remainder)? {
            if let Some(map) = leaf.as_object_mut() {
                for (dest, v) in &lifted {
                    map.insert(dest.clone(), v.clone());
                }
            }
            out.push(leaf);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn event_feed() -> Value {
        // Event-feed-shaped body: an outer `data[*]` envelope carrying the
        // event id/created, with the real object nested at `.data.object`.
        json!({
            "data": [
                {
                    "id": "evt_1",
                    "created": 1000,
                    "type": "invoice.paid",
                    "data": { "object": { "id": "in_1", "amount": 500 } }
                },
                {
                    "id": "evt_2",
                    "created": 2000,
                    "type": "invoice.paid",
                    "data": { "object": { "id": "in_2", "amount": 700 } }
                }
            ]
        })
    }

    #[test]
    fn record_ancestors_lifts_envelope_fields() {
        let anc = HashMap::from([
            ("event_id".to_owned(), "id".to_owned()),
            ("event_created".to_owned(), "created".to_owned()),
        ]);
        let out = extract_with_ancestors(&event_feed(), "$.data[*].data.object", &anc).unwrap();
        assert_eq!(out.len(), 2);

        assert_eq!(out[0]["id"], "in_1");
        assert_eq!(out[0]["amount"], 500);
        assert_eq!(out[0]["event_id"], "evt_1");
        assert_eq!(out[0]["event_created"], 1000);

        assert_eq!(out[1]["id"], "in_2");
        assert_eq!(out[1]["event_id"], "evt_2");
        assert_eq!(out[1]["event_created"], 2000);
    }

    #[test]
    fn record_ancestors_skips_absent_ancestor_fields() {
        // An ancestor-relative path that matches nothing is simply not copied.
        let anc = HashMap::from([("missing".to_owned(), "nope".to_owned())]);
        let out = extract_with_ancestors(&event_feed(), "$.data[*].data.object", &anc).unwrap();
        assert_eq!(out.len(), 2);
        assert!(out[0].get("missing").is_none());
        assert_eq!(out[0]["id"], "in_1");
    }

    #[test]
    fn split_ancestor_path_splits_at_first_wildcard() {
        assert_eq!(
            split_ancestor_path("$.data[*].data.object"),
            Some(("$.data[*]".to_owned(), ".data.object".to_owned()))
        );
        assert_eq!(
            split_ancestor_path("$.items[*]"),
            Some(("$.items[*]".to_owned(), String::new()))
        );
        assert_eq!(split_ancestor_path("$.data"), None);
    }

    #[test]
    fn extract_multi_stamps_op_marker() {
        let body = json!({
            "added":    [{"id": 1}, {"id": 2}],
            "modified": [{"id": 3}],
            "removed":  [{"id": 4}]
        });
        let specs = vec![
            RecordsMultiSpec {
                path: "$.added[*]".into(),
                op: "upsert".into(),
            },
            RecordsMultiSpec {
                path: "$.modified[*]".into(),
                op: "upsert".into(),
            },
            RecordsMultiSpec {
                path: "$.removed[*]".into(),
                op: "delete".into(),
            },
        ];
        let out = extract_multi(&body, &specs, "_op").unwrap();
        assert_eq!(out.len(), 4);
        assert_eq!(out[0]["id"], 1);
        assert_eq!(out[0]["_op"], "upsert");
        assert_eq!(out[2]["id"], 3);
        assert_eq!(out[2]["_op"], "upsert");
        assert_eq!(out[3]["id"], 4);
        assert_eq!(out[3]["_op"], "delete");
    }

    #[test]
    fn extract_multi_handles_missing_and_empty_arrays() {
        let body = json!({ "added": [{"id": 1}], "removed": [] });
        let specs = vec![
            RecordsMultiSpec {
                path: "$.added[*]".into(),
                op: "u".into(),
            },
            RecordsMultiSpec {
                path: "$.modified[*]".into(), // absent
                op: "u".into(),
            },
            RecordsMultiSpec {
                path: "$.removed[*]".into(), // empty
                op: "d".into(),
            },
        ];
        let out = extract_multi(&body, &specs, "_op").unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["_op"], "u");
    }

    #[test]
    fn extract_configured_routes_by_mode() {
        // Multi mode.
        let body = json!({ "added": [{"id": 1}], "removed": [{"id": 2}] });
        let specs = vec![
            RecordsMultiSpec {
                path: "$.added[*]".into(),
                op: "upsert".into(),
            },
            RecordsMultiSpec {
                path: "$.removed[*]".into(),
                op: "delete".into(),
            },
        ];
        let out = extract_configured(&body, None, None, &specs, "_op").unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[1]["_op"], "delete");

        // Ancestor mode.
        let anc = HashMap::from([("event_id".to_owned(), "id".to_owned())]);
        let out = extract_configured(
            &event_feed(),
            Some("$.data[*].data.object"),
            Some(&anc),
            &[],
            "_op",
        )
        .unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0]["event_id"], "evt_1");

        // Classic mode.
        let body = json!({ "items": [{"id": 9}] });
        let out = extract_configured(&body, Some("$.items[*]"), None, &[], "_op").unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["id"], 9);
    }
}
