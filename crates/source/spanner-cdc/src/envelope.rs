//! Convert a [`DataChange`] into faucet CDC envelopes (one per modified row).
//!
//! The envelope follows the shared `cdc_unwrap` contract (`op`, `before`,
//! `after`, `document_key`):
//!
//! ```json
//! {
//!   "op": "u", "ts_ms": 1760256315459, "table": "orders",
//!   "document_key": {"id": 1},
//!   "before": {"id": 1, "status": "new"},
//!   "after":  {"id": 1, "status": "paid"},
//!   "commit_timestamp": "2026-10-12T08:05:15.459516Z",
//!   "server_transaction_id": "1791792315429723",
//!   "record_sequence": "00000000",
//!   "transaction_tag": "", "value_capture_type": "OLD_AND_NEW_VALUES",
//!   "is_system_transaction": false
//! }
//! ```
//!
//! Spanner delivers mod values in wire format inside JSON (an `INT64` arrives
//! as the string `"1"`, a `JSON` column as JSON text). They are re-typed here
//! using the record's `column_types`, so a downstream sink sees `1` and an
//! object, as it would from a query.

use serde_json::{Map, Value, json};

use crate::record::{DataChange, ModType};

/// Re-type one wire-format value using its Spanner type JSON
/// (`{"code": "INT64"}`, `{"code": "ARRAY", "array_element_type": {...}}`).
pub(crate) fn retype(value: &Value, ty: Option<&Value>) -> Value {
    if value.is_null() {
        return Value::Null;
    }
    let code = ty
        .and_then(|t| t.get("code"))
        .and_then(Value::as_str)
        .unwrap_or("");
    match (code, value) {
        ("INT64" | "ENUM", Value::String(s)) => s
            .parse::<i64>()
            .map(Value::from)
            .or_else(|_| s.parse::<u64>().map(Value::from))
            .unwrap_or_else(|_| value.clone()),
        // JSON columns travel as JSON text (PostgreSQL JSONB included).
        ("JSON", Value::String(s)) => serde_json::from_str(s).unwrap_or_else(|_| value.clone()),
        ("ARRAY", Value::Array(items)) => {
            let elem = ty.and_then(|t| t.get("array_element_type"));
            Value::Array(items.iter().map(|v| retype(v, elem)).collect())
        }
        // NUMERIC stays a string (precision), BYTES stays base64, timestamps
        // and dates stay strings, FLOAT64 is already a number (or "NaN").
        _ => value.clone(),
    }
}

fn retype_map(src: &Map<String, Value>, types: &Map<String, Value>) -> Map<String, Value> {
    src.iter()
        .map(|(k, v)| (k.clone(), retype(v, types.get(k))))
        .collect()
}

/// `keys` overlaid by `values` (values win; they never repeat key columns).
fn merged(keys: &Map<String, Value>, values: &Map<String, Value>) -> Value {
    let mut out = keys.clone();
    for (k, v) in values {
        out.insert(k.clone(), v.clone());
    }
    Value::Object(out)
}

/// Build the envelopes for one data change record, one per entry in `mods`.
pub(crate) fn to_envelopes(change: &DataChange) -> Vec<Value> {
    let commit = change
        .commit_timestamp
        .to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
    let ts_ms = change.commit_timestamp.timestamp_millis();
    change
        .mods
        .iter()
        .map(|m| {
            let keys = retype_map(&m.keys, &change.column_types);
            let new_values = retype_map(&m.new_values, &change.column_types);
            let old_values = retype_map(&m.old_values, &change.column_types);
            let (before, after) = match change.mod_type {
                ModType::Insert => (Value::Null, merged(&keys, &new_values)),
                ModType::Update => (merged(&keys, &old_values), merged(&keys, &new_values)),
                ModType::Delete => (merged(&keys, &old_values), Value::Null),
            };
            json!({
                "op": change.mod_type.op(),
                "ts_ms": ts_ms,
                "table": change.table,
                "document_key": Value::Object(keys),
                "before": before,
                "after": after,
                "commit_timestamp": commit,
                "server_transaction_id": change.server_transaction_id,
                "record_sequence": change.record_sequence,
                "transaction_tag": change.transaction_tag,
                "value_capture_type": change.value_capture_type,
                "is_system_transaction": change.is_system_transaction,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{Mod, parse_data_change};

    fn change(mod_type: ModType, keys: Value, new: Value, old: Value) -> DataChange {
        let mut c = parse_data_change(&json!({
            "table_name": "orders",
            "commit_timestamp": "2026-10-12T08:05:15.459516Z",
            "server_transaction_id": "tx",
            "record_sequence": "00000000",
            "is_last_record_in_transaction_in_partition": true,
            "mod_type": match mod_type {
                ModType::Insert => "INSERT",
                ModType::Update => "UPDATE",
                ModType::Delete => "DELETE",
            },
            "value_capture_type": "OLD_AND_NEW_VALUES",
            "column_types": [
                {"name": "id", "type": {"code": "INT64"}},
                {"name": "name", "type": {"code": "STRING"}},
                {"name": "amt", "type": {"code": "NUMERIC"}},
                {"name": "doc", "type": {"code": "JSON"}},
                {"name": "tags", "type": {"code": "ARRAY", "array_element_type": {"code": "INT64"}}},
                {"name": "f", "type": {"code": "FLOAT64"}},
                {"name": "b", "type": {"code": "BYTES"}}
            ],
            "mods": []
        }))
        .unwrap();
        c.mods.push(Mod {
            keys: keys.as_object().unwrap().clone(),
            new_values: new.as_object().unwrap().clone(),
            old_values: old.as_object().unwrap().clone(),
        });
        c
    }

    #[test]
    fn insert_has_after_only() {
        let e = to_envelopes(&change(
            ModType::Insert,
            json!({"id": "1"}),
            json!({"name": "a"}),
            json!({}),
        ));
        assert_eq!(e.len(), 1);
        assert_eq!(e[0]["op"], "c");
        assert_eq!(e[0]["before"], Value::Null);
        assert_eq!(e[0]["after"], json!({"id": 1, "name": "a"}));
        assert_eq!(e[0]["document_key"], json!({"id": 1}));
        assert_eq!(e[0]["table"], "orders");
        assert_eq!(e[0]["commit_timestamp"], "2026-10-12T08:05:15.459516Z");
        assert_eq!(e[0]["ts_ms"], 1_791_792_315_459_i64);
    }

    #[test]
    fn update_has_both_images() {
        let e = to_envelopes(&change(
            ModType::Update,
            json!({"id": "1"}),
            json!({"name": "b"}),
            json!({"name": "a"}),
        ));
        assert_eq!(e[0]["op"], "u");
        assert_eq!(e[0]["before"], json!({"id": 1, "name": "a"}));
        assert_eq!(e[0]["after"], json!({"id": 1, "name": "b"}));
    }

    #[test]
    fn delete_has_before_only_and_the_key() {
        let e = to_envelopes(&change(
            ModType::Delete,
            json!({"id": "1"}),
            json!({}),
            json!({"name": "b"}),
        ));
        assert_eq!(e[0]["op"], "d");
        assert_eq!(e[0]["after"], Value::Null);
        assert_eq!(e[0]["before"], json!({"id": 1, "name": "b"}));
        assert_eq!(e[0]["document_key"], json!({"id": 1}));
    }

    #[test]
    fn wire_values_are_retyped_by_column_type() {
        let e = to_envelopes(&change(
            ModType::Insert,
            json!({"id": "9223372036854775807"}),
            json!({
                "amt": "1.50", "doc": "{\"k\": [1, 2]}", "tags": ["1", "2"],
                "f": 1.25, "b": "YWJj", "name": null
            }),
            json!({}),
        ));
        let after = &e[0]["after"];
        assert_eq!(after["id"], json!(9_223_372_036_854_775_807_i64));
        assert_eq!(after["amt"], json!("1.50"));
        assert_eq!(after["doc"], json!({"k": [1, 2]}));
        assert_eq!(after["tags"], json!([1, 2]));
        assert_eq!(after["f"], json!(1.25));
        assert_eq!(after["b"], json!("YWJj"));
        assert_eq!(after["name"], Value::Null);
    }

    #[test]
    fn invalid_json_text_and_unparseable_ints_pass_through() {
        assert_eq!(
            retype(&json!("not json"), Some(&json!({"code": "JSON"}))),
            json!("not json")
        );
        assert_eq!(
            retype(&json!("abc"), Some(&json!({"code": "INT64"}))),
            json!("abc")
        );
        // Unknown / absent types never alter the value.
        assert_eq!(retype(&json!("7"), None), json!("7"));
    }

    #[test]
    fn one_envelope_per_mod() {
        let mut c = change(
            ModType::Insert,
            json!({"id": "1"}),
            json!({"name": "a"}),
            json!({}),
        );
        c.mods.push(Mod {
            keys: json!({"id": "2"}).as_object().unwrap().clone(),
            new_values: json!({"name": "b"}).as_object().unwrap().clone(),
            old_values: Map::new(),
        });
        let e = to_envelopes(&c);
        assert_eq!(e.len(), 2);
        assert_eq!(e[1]["document_key"], json!({"id": 2}));
    }
}
