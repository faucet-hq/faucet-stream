//! Page → write operations. Pure.
//!
//! Every mode dedups within the page by the table key, last write wins —
//! `BatchWriteItem` rejects a request naming one key twice, and sequential
//! `PutItem`s of one key converge to the last anyway. Each operation keeps
//! the input rows it stands for, so outcomes map back to rows.

use crate::config::{MAX_ITEM_BYTES, MAX_REQUEST_BYTES};
use aws_sdk_dynamodb::types::{AttributeValue, DeleteRequest, PutRequest, WriteRequest};
use faucet_common_dynamodb::{
    KeyAttribute, ScalarType, ValueFormat, item_size, item_to_typed_json, json_to_item,
    json_to_key_attribute, typed_json_to_attribute, typed_json_to_item,
};
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
///
/// A number key is identified by its value, not its text: DynamoDB treats
/// `1`, `1.0`, `01` and `1e0` as one key, so the in-page dedup and the
/// matching of unprocessed requests must too.
pub(crate) fn key_canon(item: &Item, keys: &[String]) -> String {
    keys.iter()
        .map(|k| {
            let one: Item = item
                .get(k)
                .map(|v| match v {
                    AttributeValue::N(n) => (
                        k.clone(),
                        AttributeValue::N(canonical_number(n).unwrap_or_else(|| n.clone())),
                    ),
                    other => (k.clone(), other.clone()),
                })
                .into_iter()
                .collect();
            item_to_typed_json(&one).to_string()
        })
        .collect::<Vec<_>>()
        .join("\u{1f}")
}

/// A DynamoDB number's value as `[-]<digits>e<exp>` with no leading or
/// trailing zeros (`0` for zero), so equal numbers render equally. `None`
/// for text that is not a decimal.
fn canonical_number(raw: &str) -> Option<String> {
    let s = raw.trim();
    let (negative, s) = match s.as_bytes().first()? {
        b'-' => (true, &s[1..]),
        b'+' => (false, &s[1..]),
        _ => (false, s),
    };
    let (mantissa, exp_part) = match s.find(['e', 'E']) {
        Some(i) => (&s[..i], Some(&s[i + 1..])),
        None => (s, None),
    };
    let (int_part, frac_part) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if (int_part.is_empty() && frac_part.is_empty())
        || !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let exp: i64 = match exp_part {
        Some(e) => e.parse().ok()?,
        None => 0,
    };
    let joined = format!("{int_part}{frac_part}");
    let leading = joined.trim_start_matches('0');
    let digits = leading.trim_end_matches('0');
    if digits.is_empty() {
        return Some("0".into());
    }
    let exp = exp
        .checked_sub(frac_part.len() as i64)?
        .checked_add((leading.len() - digits.len()) as i64)?;
    Some(format!("{}{digits}e{exp}", if negative { "-" } else { "" }))
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
        .and_then(|v| v.as_str().or_else(|| v.get("S").and_then(Value::as_str)))
        .is_some_and(|s| dm.values.iter().any(|m| m == s))
}

/// A typed key value (`{"S": "a"}`) as the table's declared key type.
fn typed_key_attribute(
    name: &str,
    v: &Value,
    scalar: ScalarType,
) -> Result<AttributeValue, String> {
    let av = typed_json_to_attribute(v)
        .map_err(|_| format!("key attribute '{name}' is not a typed attribute value: {v}"))?;
    match (scalar, &av) {
        (ScalarType::String, AttributeValue::S(_))
        | (ScalarType::Number, AttributeValue::N(_))
        | (ScalarType::Binary, AttributeValue::B(_)) => Ok(av),
        (_, AttributeValue::Null(_)) => Err(format!("null value for key attribute '{name}'")),
        _ => Err(format!(
            "key attribute '{name}' must be a typed {} value for the table's key, got {v}",
            match scalar {
                ScalarType::String => "S",
                ScalarType::Number => "N",
                ScalarType::Binary => "B",
            }
        )),
    }
}

fn row_error(mode: WriteMode, msg: String) -> FaucetError {
    FaucetError::Sink(format!("dynamodb {}: {msg}", mode.as_str()))
}

/// [`plan_as`] for plain-JSON records.
#[cfg(test)]
pub(crate) fn plan(records: &[Value], spec: &WriteSpec, keys: &[KeyAttribute]) -> Planned {
    plan_as(records, spec, keys, ValueFormat::Plain)
}

/// Plan a page. `keys` is the table's key schema (partition key first); key
/// attributes are written as the table's declared type. `format` says how the
/// records carry attribute values.
pub(crate) fn plan_as(
    records: &[Value],
    spec: &WriteSpec,
    keys: &[KeyAttribute],
    format: ValueFormat,
) -> Planned {
    let names: Vec<String> = keys.iter().map(|k| k.name.clone()).collect();
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
            let name = &k.name;
            match obj.get(name) {
                None => missing = Some(format!("missing key attribute '{name}'")),
                Some(Value::Null) => {
                    missing = Some(format!("null value for key attribute '{name}'"))
                }
                Some(v) => match match format {
                    ValueFormat::Plain => json_to_key_attribute(name, v, k.scalar),
                    ValueFormat::Typed => typed_key_attribute(name, v, k.scalar),
                } {
                    Ok(av) => {
                        key_item.insert(name.clone(), av);
                        continue;
                    }
                    Err(e) => missing = Some(e),
                },
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
            let item = match format {
                ValueFormat::Plain => json_to_item(&body),
                ValueFormat::Typed => typed_json_to_item(&body).map_err(|e| match e {
                    FaucetError::Source(m) => FaucetError::Sink(m),
                    other => other,
                }),
            };
            match item {
                Ok(mut item) => {
                    item.extend(key_item);
                    OpKind::Put(item)
                }
                Err(e) => {
                    planned.failures.insert(i, row_error(mode, e.to_string()));
                    continue;
                }
            }
        };
        let canon = match &kind {
            OpKind::Put(item) | OpKind::Delete(item) => key_canon(item, &names),
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

    fn keys() -> Vec<KeyAttribute> {
        use faucet_common_dynamodb::{KeyRole, ScalarType};
        vec![
            KeyAttribute {
                name: "pk".into(),
                role: KeyRole::Hash,
                scalar: ScalarType::String,
            },
            KeyAttribute {
                name: "sk".into(),
                role: KeyRole::Range,
                scalar: ScalarType::Number,
            },
        ]
    }

    fn names() -> Vec<String> {
        vec!["pk".into(), "sk".into()]
    }

    fn spec(mode: WriteMode) -> WriteSpec {
        WriteSpec {
            write_mode: mode,
            key: names(),
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
    fn number_keys_are_identified_by_value() {
        assert_eq!(canonical_number("1").as_deref(), Some("1e0"));
        assert_eq!(canonical_number("001.50").as_deref(), Some("15e-1"));
        assert_eq!(canonical_number("1.5").as_deref(), Some("15e-1"));
        assert_eq!(canonical_number("1e2").as_deref(), Some("1e2"));
        assert_eq!(canonical_number("100").as_deref(), Some("1e2"));
        assert_eq!(canonical_number("-0.0").as_deref(), Some("0"));
        assert_eq!(canonical_number("-2.").as_deref(), Some("-2e0"));
        assert_eq!(canonical_number("+2.50").as_deref(), Some("25e-1"));
        assert!(canonical_number("x").is_none());
        assert!(canonical_number(".").is_none());
        // A request DynamoDB hands back with the number re-rendered still
        // matches the op that sent it.
        let sent = plan(
            &[json!({"pk": "a", "sk": "001"})],
            &WriteSpec::default(),
            &keys(),
        );
        let echoed: Item = [
            ("pk".to_string(), AttributeValue::S("a".into())),
            ("sk".to_string(), AttributeValue::N("1".into())),
        ]
        .into_iter()
        .collect();
        assert_eq!(key_canon(&echoed, &names()), sent.ops[0].canon);
        // `1` and `1.0` are one key: the page keeps the last write.
        let dup = plan(
            &[
                json!({"pk": "a", "sk": 1, "v": 1}),
                json!({"pk": "a", "sk": 1.0, "v": 2}),
            ],
            &WriteSpec::default(),
            &keys(),
        );
        assert_eq!(dup.ops.len(), 1);
    }

    #[test]
    fn canon_matches_unprocessed_requests() {
        let p = plan(
            &[json!({"pk": "a", "sk": 1, "v": 1})],
            &WriteSpec::default(),
            &keys(),
        );
        let req = p.ops[0].write_request();
        assert_eq!(request_canon(&req, &names()).unwrap(), p.ops[0].canon);
        let d = plan(
            &[json!({"pk": "a", "sk": 1})],
            &spec(WriteMode::Delete),
            &keys(),
        );
        assert_eq!(
            request_canon(&d.ops[0].write_request(), &names()).unwrap(),
            p.ops[0].canon
        );
        assert!(request_canon(&WriteRequest::builder().build(), &names()).is_none());
        let seven = plan(
            &[json!({"pk": "7", "sk": 7})],
            &WriteSpec::default(),
            &keys(),
        );
        let seven_s = plan(
            &[json!({"pk": "7", "sk": "7"})],
            &WriteSpec::default(),
            &keys(),
        );
        assert_eq!(
            seven.ops[0].canon, seven_s.ops[0].canon,
            "an N key arrives as N whether the record carried 7 or \"7\""
        );
    }

    #[test]
    fn key_attributes_take_the_table_declared_type() {
        use faucet_common_dynamodb::{KeyRole, ScalarType};
        let bin_keys = vec![KeyAttribute {
            name: "id".into(),
            role: KeyRole::Hash,
            scalar: ScalarType::Binary,
        }];
        let p = plan(
            &[json!({"id": "AAEC", "v": "AAEC"})],
            &WriteSpec::default(),
            &bin_keys,
        );
        match &p.ops[0].kind {
            OpKind::Put(item) => {
                assert_eq!(
                    item["id"],
                    AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new(vec![0u8, 1, 2]))
                );
                assert_eq!(item["v"], AttributeValue::S("AAEC".into()));
            }
            other => panic!("{other:?}"),
        }
        let big = "123456789012345678901234567890.5";
        let p = plan(
            &[json!({"pk": "a", "sk": big})],
            &WriteSpec::default(),
            &keys(),
        );
        match &p.ops[0].kind {
            OpKind::Put(item) => assert_eq!(item["sk"], AttributeValue::N(big.into())),
            other => panic!("{other:?}"),
        }
        let p = plan(&[json!({"pk": 7, "sk": 1})], &WriteSpec::default(), &keys());
        assert!(p.ops.is_empty());
        assert!(
            p.failures[&0].to_string().contains("table's S key"),
            "{}",
            p.failures[&0]
        );
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

    #[test]
    fn typed_records_are_written_exactly() {
        let rows = vec![json!({
            "pk": {"S": "a"},
            "sk": {"N": "1"},
            "tags": {"SS": ["x", "y"]},
            "blob": {"B": "AQ=="},
            "big": {"N": "12345678901234567890.123456789"},
            "nested": {"M": {"bs": {"BS": ["Ag=="]}}}
        })];
        let p = plan_as(&rows, &WriteSpec::default(), &keys(), ValueFormat::Typed);
        assert!(p.failures.is_empty(), "{:?}", p.failures);
        let OpKind::Put(item) = &p.ops[0].kind else {
            panic!("expected a put")
        };
        assert_eq!(
            item["tags"],
            AttributeValue::Ss(vec!["x".into(), "y".into()])
        );
        assert_eq!(
            item["blob"],
            AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new(vec![1]))
        );
        assert_eq!(
            item["big"],
            AttributeValue::N("12345678901234567890.123456789".into())
        );
        assert_eq!(item["pk"], AttributeValue::S("a".into()));
        assert_eq!(item_to_typed_json(item), rows[0]);
    }

    #[test]
    fn typed_keys_must_match_the_declared_type() {
        let cases = [
            (
                json!({"pk": {"N": "1"}, "sk": {"N": "1"}}),
                "must be a typed S value",
            ),
            (
                json!({"pk": "a", "sk": {"N": "1"}}),
                "not a typed attribute value",
            ),
            (
                json!({"pk": {"NULL": true}, "sk": {"N": "1"}}),
                "null value for key attribute 'pk'",
            ),
            (
                json!({"pk": {"S": "a"}, "sk": {"N": "1"}, "v": 3}),
                "malformed typed attribute",
            ),
        ];
        for (row, want) in cases {
            let p = plan_as(
                std::slice::from_ref(&row),
                &WriteSpec::default(),
                &keys(),
                ValueFormat::Typed,
            );
            let err = p.failures[&0].to_string();
            assert!(err.contains(want), "{row}: {err}");
            assert!(!err.contains("Source error"), "{err}");
        }
    }

    #[test]
    fn typed_delete_markers_and_deletes() {
        let mut s = spec(WriteMode::Upsert);
        s.delete_marker = Some(DeleteMarker {
            field: "op".into(),
            values: vec!["d".into()],
        });
        let rows = vec![
            json!({"pk": {"S": "a"}, "sk": {"N": "1"}, "op": {"S": "d"}}),
            json!({"pk": {"S": "b"}, "sk": {"N": "2"}, "op": {"S": "u"}, "v": {"BOOL": true}}),
            json!({"pk": {"S": "c"}, "sk": {"N": "3"}, "op": "d"}),
        ];
        let p = plan_as(&rows, &s, &keys(), ValueFormat::Typed);
        assert!(p.failures.is_empty(), "{:?}", p.failures);
        assert!(matches!(p.ops[0].kind, OpKind::Delete(_)));
        let OpKind::Put(item) = &p.ops[1].kind else {
            panic!("expected a put")
        };
        assert!(!item.contains_key("op"));
        assert_eq!(item["v"], AttributeValue::Bool(true));
        assert!(matches!(p.ops[2].kind, OpKind::Delete(_)));
        let d = plan_as(
            &[json!({"pk": {"S": "a"}, "sk": {"N": "9"}})],
            &spec(WriteMode::Delete),
            &keys(),
            ValueFormat::Typed,
        );
        let OpKind::Delete(key) = &d.ops[0].kind else {
            panic!("expected a delete")
        };
        assert_eq!(key["sk"], AttributeValue::N("9".into()));
    }
}
