//! Page → write operations. Pure.
//!
//! Every mode dedups within the page by the table key, last write wins —
//! `BatchWriteItem` rejects a request naming one key twice, and sequential
//! `PutItem`s of one key converge to the last anyway. Each operation keeps
//! the input rows it stands for, so outcomes map back to rows.

use crate::config::{MAX_ITEM_BYTES, MAX_REQUEST_BYTES};
use aws_sdk_dynamodb::types::{AttributeValue, DeleteRequest, PutRequest, WriteRequest};
use faucet_common_dynamodb::{item_size, item_to_typed_json, json_to_attribute, json_to_item};
use faucet_core::{FaucetError, WriteMode, WriteSpec};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};

/// Item attribute map.
pub(crate) type Item = HashMap<String, AttributeValue>;

/// A put or a delete.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum OpKind {
    Put(Item),
    Delete(Item),
}

/// One write, standing for one or more input rows.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Op {
    pub rows: Vec<usize>,
    pub kind: OpKind,
    pub canon: String,
}

impl Op {
    /// Bytes this op contributes to a request.
    pub fn size(&self) -> usize {
        match &self.kind {
            OpKind::Put(i) | OpKind::Delete(i) => item_size(i),
        }
    }

    /// The `WriteRequest` for `BatchWriteItem`.
    pub fn write_request(&self) -> WriteRequest {
        match &self.kind {
            OpKind::Put(item) => WriteRequest::builder()
                .put_request(
                    PutRequest::builder()
                        .set_item(Some(item.clone()))
                        .build()
                        .expect("item is set"),
                )
                .build(),
            OpKind::Delete(key) => WriteRequest::builder()
                .delete_request(
                    DeleteRequest::builder()
                        .set_key(Some(key.clone()))
                        .build()
                        .expect("key is set"),
                )
                .build(),
        }
    }
}

/// The planned page.
#[derive(Debug, Default)]
pub(crate) struct Planned {
    pub ops: Vec<Op>,
    pub failures: BTreeMap<usize, FaucetError>,
}

/// Stable identity of an item's key: the typed key attributes in key order.
pub(crate) fn key_canon(item: &Item, keys: &[String]) -> String {
    keys.iter()
        .map(|k| {
            let one: Item = item
                .get(k)
                .map(|v| (k.clone(), v.clone()))
                .into_iter()
                .collect();
            item_to_typed_json(&one).to_string()
        })
        .collect::<Vec<_>>()
        .join("\u{1f}")
}

/// Key identity of an unprocessed `WriteRequest` returned by DynamoDB.
pub(crate) fn request_canon(req: &WriteRequest, keys: &[String]) -> Option<String> {
    if let Some(p) = req.put_request() {
        return Some(key_canon(p.item(), keys));
    }
    req.delete_request().map(|d| key_canon(d.key(), keys))
}

fn is_delete_marked(rec: &Value, spec: &WriteSpec) -> bool {
    let Some(dm) = &spec.delete_marker else {
        return false;
    };
    rec.get(&dm.field)
        .and_then(Value::as_str)
        .is_some_and(|s| dm.values.iter().any(|m| m == s))
}

fn row_error(mode: WriteMode, msg: String) -> FaucetError {
    FaucetError::Sink(format!("dynamodb {}: {msg}", mode.as_str()))
}

/// Plan a page. `keys` is the table's key schema (partition key first).
pub(crate) fn plan(records: &[Value], spec: &WriteSpec, keys: &[String]) -> Planned {
    let mode = spec.write_mode;
    let mut planned = Planned::default();
    let mut slots: HashMap<String, usize> = HashMap::new();
    for (i, rec) in records.iter().enumerate() {
        let Some(obj) = rec.as_object() else {
            planned
                .failures
                .insert(i, row_error(mode, "record is not a JSON object".into()));
            continue;
        };
        let mut key_item = Item::new();
        let mut missing = None;
        for k in keys {
            match obj.get(k) {
                None => missing = Some(format!("missing key attribute '{k}'")),
                Some(Value::Null) => missing = Some(format!("null value for key attribute '{k}'")),
                Some(v) => {
                    key_item.insert(k.clone(), json_to_attribute(v));
                    continue;
                }
            }
            break;
        }
        if let Some(msg) = missing {
            planned.failures.insert(i, row_error(mode, msg));
            continue;
        }
        let is_delete = match mode {
            WriteMode::Delete => true,
            WriteMode::Upsert => is_delete_marked(rec, spec),
            _ => false,
        };
        let kind = if is_delete {
            OpKind::Delete(key_item)
        } else {
            let mut body = rec.clone();
            if mode == WriteMode::Upsert
                && let (Some(dm), Value::Object(map)) = (&spec.delete_marker, &mut body)
            {
                map.remove(&dm.field);
            }
            match json_to_item(&body) {
                Ok(item) => OpKind::Put(item),
                Err(e) => {
                    planned.failures.insert(i, row_error(mode, e.to_string()));
                    continue;
                }
            }
        };
        let canon = match &kind {
            OpKind::Put(item) | OpKind::Delete(item) => key_canon(item, keys),
        };
        match slots.get(&canon) {
            Some(&slot) => {
                let op = &mut planned.ops[slot];
                op.kind = kind;
                op.rows.push(i);
            }
            None => {
                slots.insert(canon.clone(), planned.ops.len());
                planned.ops.push(Op {
                    rows: vec![i],
                    kind,
                    canon,
                });
            }
        }
    }
    let (ok, oversized): (Vec<Op>, Vec<Op>) = std::mem::take(&mut planned.ops)
        .into_iter()
        .partition(|op| op.size() <= MAX_ITEM_BYTES);
    for op in oversized {
        let size = op.size();
        for row in op.rows {
            planned.failures.insert(
                row,
                row_error(
                    mode,
                    format!("item is ~{size} bytes, above DynamoDB's {MAX_ITEM_BYTES}-byte limit"),
                ),
            );
        }
    }
    planned.ops = ok;
    planned
}

/// Split ops into `BatchWriteItem` requests (≤ `max_items`, ≤ 16 MB).
pub(crate) fn chunk_ops(ops: Vec<Op>, max_items: usize) -> Vec<Vec<Op>> {
    let mut out = Vec::new();
    let mut current: Vec<Op> = Vec::new();
    let mut bytes = 0usize;
    for op in ops {
        let size = op.size();
        if !current.is_empty() && (current.len() >= max_items || bytes + size > MAX_REQUEST_BYTES) {
            out.push(std::mem::take(&mut current));
            bytes = 0;
        }
        bytes += size;
        current.push(op);
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::DeleteMarker;
    use serde_json::json;

    fn keys() -> Vec<String> {
        vec!["pk".into(), "sk".into()]
    }

    fn spec(mode: WriteMode) -> WriteSpec {
        WriteSpec {
            write_mode: mode,
            key: keys(),
            ..Default::default()
        }
    }

    #[test]
    fn append_dedups_by_table_key_last_wins() {
        let rows = vec![
            json!({"pk": "a", "sk": 1, "v": 1}),
            json!({"pk": "a", "sk": 2, "v": 2}),
            json!({"pk": "a", "sk": 1, "v": 3}),
            json!({"pk": "a", "v": 4}),
            json!({"pk": "a", "sk": null}),
            json!([1]),
        ];
        let p = plan(&rows, &WriteSpec::default(), &keys());
        assert_eq!(p.ops.len(), 2);
        assert_eq!(p.ops[0].rows, vec![0, 2]);
        assert!(
            matches!(&p.ops[0].kind, OpKind::Put(i) if i["v"] == AttributeValue::N("3".into()))
        );
        assert!(
            p.failures[&3]
                .to_string()
                .contains("missing key attribute 'sk'")
        );
        assert!(p.failures[&4].to_string().contains("null value"));
        assert!(p.failures[&5].to_string().contains("not a JSON object"));
        assert!(p.failures[&3].to_string().contains("dynamodb append"));
    }

    #[test]
    fn upsert_marker_turns_rows_into_deletes_and_strips_the_marker() {
        let mut s = spec(WriteMode::Upsert);
        s.delete_marker = Some(DeleteMarker {
            field: "__op".into(),
            values: vec!["d".into()],
        });
        let rows = vec![
            json!({"pk": "a", "sk": 1, "v": 1, "__op": "u"}),
            json!({"pk": "b", "sk": 1, "__op": "d"}),
            json!({"pk": "a", "sk": 1, "__op": "d"}),
            json!({"pk": "c", "sk": 1, "__op": 5}),
        ];
        let p = plan(&rows, &s, &keys());
        assert_eq!(p.ops.len(), 3);
        assert_eq!(p.ops[0].rows, vec![0, 2]);
        assert!(matches!(&p.ops[0].kind, OpKind::Delete(k) if k.len() == 2));
        assert!(matches!(&p.ops[1].kind, OpKind::Delete(_)));
        match &p.ops[2].kind {
            OpKind::Put(item) => assert!(!item.contains_key("__op")),
            other => panic!("{other:?}"),
        }
        let no_marker = plan(&rows[..1], &spec(WriteMode::Upsert), &keys());
        assert!(matches!(&no_marker.ops[0].kind, OpKind::Put(i) if i.contains_key("__op")));
    }

    #[test]
    fn delete_mode_keeps_only_key_attributes() {
        let p = plan(
            &[json!({"pk": "a", "sk": 1, "big": "x"})],
            &spec(WriteMode::Delete),
            &keys(),
        );
        match &p.ops[0].kind {
            OpKind::Delete(k) => assert_eq!(k.len(), 2),
            other => panic!("{other:?}"),
        }
        assert!(p.ops[0].write_request().delete_request().is_some());
    }

    #[test]
    fn oversized_items_fail_every_row_they_stand_for() {
        let big = "x".repeat(MAX_ITEM_BYTES + 1);
        let rows = vec![
            json!({"pk": "a", "sk": 1, "blob": big}),
            json!({"pk": "a", "sk": 1, "blob": big}),
            json!({"pk": "b", "sk": 1}),
        ];
        let p = plan(&rows, &WriteSpec::default(), &keys());
        assert_eq!(p.ops.len(), 1);
        assert_eq!(p.failures.len(), 2);
        assert!(p.failures[&0].to_string().contains("byte limit"));
    }

    #[test]
    fn canon_matches_unprocessed_requests() {
        let p = plan(
            &[json!({"pk": "a", "sk": 1, "v": 1})],
            &WriteSpec::default(),
            &keys(),
        );
        let req = p.ops[0].write_request();
        assert_eq!(request_canon(&req, &keys()).unwrap(), p.ops[0].canon);
        let d = plan(
            &[json!({"pk": "a", "sk": 1})],
            &spec(WriteMode::Delete),
            &keys(),
        );
        assert_eq!(
            request_canon(&d.ops[0].write_request(), &keys()).unwrap(),
            p.ops[0].canon
        );
        assert!(request_canon(&WriteRequest::builder().build(), &keys()).is_none());
        let seven = plan(&[json!({"pk": 7, "sk": 1})], &WriteSpec::default(), &keys());
        let seven_s = plan(
            &[json!({"pk": "7", "sk": 1})],
            &WriteSpec::default(),
            &keys(),
        );
        assert_ne!(seven.ops[0].canon, seven_s.ops[0].canon);
    }

    #[test]
    fn chunking_respects_item_and_byte_ceilings() {
        let rows: Vec<Value> = (0..60)
            .map(|i| json!({"pk": format!("k{i}"), "sk": 1}))
            .collect();
        let p = plan(&rows, &WriteSpec::default(), &keys());
        let chunks = chunk_ops(p.ops, 25);
        assert_eq!(
            chunks.iter().map(Vec::len).collect::<Vec<_>>(),
            [25, 25, 10]
        );

        let blob = "x".repeat(300 * 1024);
        let rows: Vec<Value> = (0..120)
            .map(|i| json!({"pk": format!("k{i}"), "sk": 1, "b": blob}))
            .collect();
        let p = plan(&rows, &WriteSpec::default(), &keys());
        let chunks = chunk_ops(p.ops, 1000);
        assert_eq!(
            chunks.len(),
            3,
            "the 16 MB ceiling binds before the item count"
        );
        for c in &chunks {
            assert!(c.iter().map(Op::size).sum::<usize>() <= MAX_REQUEST_BYTES);
        }
        assert!(chunk_ops(Vec::new(), 25).is_empty());
    }
}
