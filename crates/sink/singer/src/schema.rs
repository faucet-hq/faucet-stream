//! Cumulative stream schema for records whose schema is inferred.
//!
//! Pure logic: the first page's inferred schema is sent as-is; each later page
//! is folded in, and a new `SCHEMA` message is needed only when the fold
//! widened something (a new field, a new type, a field turning nullable).

use faucet_core::Value;
use serde_json::{Map, json};

/// Fold `page` (an `infer_schema` object) into `current`. Returns whether
/// `current` changed.
pub fn widen(current: &mut Value, page: &Value) -> bool {
    let empty = Map::new();
    let page_props = page
        .get("properties")
        .and_then(Value::as_object)
        .unwrap_or(&empty);
    if !current.get("properties").is_some_and(Value::is_object) {
        *current = json!({"type": "object", "properties": {}});
    }
    let props = current
        .get_mut("properties")
        .and_then(Value::as_object_mut)
        .expect("properties object ensured above");
    let mut changed = false;
    for (key, schema) in page_props {
        match props.get_mut(key) {
            None => {
                let mut schema = schema.clone();
                add_type(&mut schema, "null");
                props.insert(key.clone(), schema);
                changed = true;
            }
            Some(existing) => {
                for t in types(schema) {
                    changed |= add_type(existing, &t);
                }
            }
        }
    }
    for (key, existing) in props.iter_mut() {
        if !page_props.contains_key(key) {
            changed |= add_type(existing, "null");
        }
    }
    changed
}

fn types(schema: &Value) -> Vec<String> {
    match schema.get("type") {
        Some(Value::String(t)) => vec![t.clone()],
        Some(Value::Array(a)) => a
            .iter()
            .filter_map(|t| t.as_str().map(str::to_string))
            .collect(),
        _ => Vec::new(),
    }
}

/// Add `t` to `schema.type`; returns whether it was missing. A schema without
/// a `type` accepts everything and is left alone.
fn add_type(schema: &mut Value, t: &str) -> bool {
    let Some(obj) = schema.as_object_mut() else {
        return false;
    };
    let current = match obj.get("type") {
        Some(Value::String(s)) => vec![s.clone()],
        Some(Value::Array(a)) => a
            .iter()
            .filter_map(|x| x.as_str().map(str::to_string))
            .collect(),
        _ => return false,
    };
    if current.iter().any(|c| c == t) {
        return false;
    }
    let mut all = current;
    all.push(t.to_string());
    obj.insert(
        "type".into(),
        Value::Array(all.into_iter().map(Value::String).collect()),
    );
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_page_initializes() {
        let mut cur = Value::Null;
        let page = json!({"type": "object", "properties": {"id": {"type": "integer"}}});
        assert!(widen(&mut cur, &page));
        assert_eq!(cur["properties"]["id"]["type"], json!(["integer", "null"]));
    }

    #[test]
    fn identical_page_is_no_change() {
        let page = json!({"type": "object", "properties": {"id": {"type": "integer"}}});
        let mut cur = json!({"type": "object", "properties": {"id": {"type": "integer"}}});
        assert!(!widen(&mut cur, &page));
        assert_eq!(cur, page);
    }

    #[test]
    fn widens_types_new_fields_and_missing_fields() {
        let mut cur = json!({"type": "object", "properties": {
            "id": {"type": "integer"},
            "gone": {"type": "string"},
            "any": {}
        }});
        let page = json!({"type": "object", "properties": {
            "id": {"type": ["number", "null"]},
            "new": {"type": "boolean"},
            "any": {"type": "string"}
        }});
        assert!(widen(&mut cur, &page));
        assert_eq!(
            cur["properties"]["id"]["type"],
            json!(["integer", "number", "null"])
        );
        assert_eq!(cur["properties"]["new"]["type"], json!(["boolean", "null"]));
        assert_eq!(cur["properties"]["gone"]["type"], json!(["string", "null"]));
        assert_eq!(cur["properties"]["any"], json!({}));
        assert!(!widen(&mut cur, &page));
    }

    #[test]
    fn non_object_property_schemas_are_left_alone() {
        let mut cur = json!({"type": "object", "properties": {"x": true}});
        let page = json!({"type": "object", "properties": {"x": {"type": "string"}}});
        assert!(!widen(&mut cur, &page));
        let mut cur = json!({"type": "object", "properties": {"x": true}});
        assert!(!widen(
            &mut cur,
            &json!({"type": "object", "properties": {}})
        ));
    }
}
