//! Bookmarks of a source with several `requests:` entries.
//!
//! Each entry is its own feed with its own sequence, so an incremental (or
//! windowed) source with more than one entry keeps one bookmark per entry,
//! stored as `{"requests": {"<entry id>": <bookmark>, …}}` inside the
//! versioned state envelope (schema [`STATE_SCHEMA`]). Any other stored value
//! is a bookmark shared by every entry, as earlier releases wrote it.

use faucet_core::FaucetError;
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashMap};

/// The REST bookmark shape version (#736). Schema 1 adds the per-entry
/// `requests:` form; a schema-0 value is a bookmark shared by every entry.
pub const STATE_SCHEMA: u32 = 1;

const REQUESTS_KEY: &str = "requests";

/// Bring a REST bookmark stored at shape version `from` up to
/// [`STATE_SCHEMA`]. A schema-0 bookmark keeps its meaning: the start every
/// entry resumes from.
pub fn migrate_state(from: u32, data: Value) -> Result<Value, FaucetError> {
    match from {
        0 | STATE_SCHEMA => Ok(data),
        other => Err(FaucetError::State(format!(
            "rest has no bookmark schema {other} (this release reads up to {STATE_SCHEMA})"
        ))),
    }
}

/// A stored REST bookmark, read.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Stored {
    /// One bookmark for every entry (or a source without `requests:`).
    Shared(Value),
    /// A bookmark per entry id.
    PerEntry(BTreeMap<String, Value>),
}

impl Stored {
    pub(crate) fn parse(value: &Value) -> Self {
        if let Value::Object(o) = value
            && o.len() == 1
            && let Some(Value::Object(entries)) = o.get(REQUESTS_KEY)
        {
            return Stored::PerEntry(
                entries
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            );
        }
        Stored::Shared(value.clone())
    }
}

/// The stored form of per-entry bookmarks.
pub(crate) fn render(entries: BTreeMap<String, Value>) -> Value {
    let mut inner = Map::new();
    inner.extend(entries);
    let mut outer = Map::new();
    outer.insert(REQUESTS_KEY.into(), Value::Object(inner));
    Value::Object(outer)
}

/// A stable id for a `requests:` entry: an FNV hash of its canonical JSON
/// (object keys sorted at every level), so key order never changes it and no
/// entry value lands in state.
pub(crate) fn entry_id(entry: &HashMap<String, Value>) -> String {
    let sorted: BTreeMap<&String, &Value> = entry.iter().collect();
    let mut text = String::from("{");
    for (i, (k, v)) in sorted.into_iter().enumerate() {
        if i > 0 {
            text.push(',');
        }
        text.push_str(&Value::String(k.clone()).to_string());
        text.push(':');
        canonical(v, &mut text);
    }
    text.push('}');
    format!("{:016x}", faucet_core::shard::shard_hash(&text))
}

fn canonical(value: &Value, out: &mut String) {
    match value {
        Value::Object(o) => {
            let sorted: BTreeMap<&String, &Value> = o.iter().collect();
            out.push('{');
            for (i, (k, v)) in sorted.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&Value::String(k.clone()).to_string());
                out.push(':');
                canonical(v, out);
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (i, v) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                canonical(v, out);
            }
            out.push(']');
        }
        other => out.push_str(&other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn entry_ids_ignore_key_order_at_every_level() {
        let a = HashMap::from([
            ("region".to_string(), json!("eu")),
            (
                "filter".to_string(),
                json!({"b": 1, "a": [1, {"y": 2, "x": 1}]}),
            ),
        ]);
        let b = HashMap::from([
            (
                "filter".to_string(),
                json!({"a": [1, {"x": 1, "y": 2}], "b": 1}),
            ),
            ("region".to_string(), json!("eu")),
        ]);
        assert_eq!(entry_id(&a), entry_id(&b));
        assert_eq!(entry_id(&a).len(), 16);
        let c = HashMap::from([("region".to_string(), json!("us"))]);
        assert_ne!(entry_id(&a), entry_id(&c));
    }

    #[test]
    fn stored_values_round_trip_and_legacy_values_are_shared() {
        let map = BTreeMap::from([("a".to_string(), json!(100)), ("b".to_string(), json!(50))]);
        let stored = render(map.clone());
        assert_eq!(stored, json!({"requests": {"a": 100, "b": 50}}));
        assert_eq!(Stored::parse(&stored), Stored::PerEntry(map));
        assert_eq!(Stored::parse(&json!(7)), Stored::Shared(json!(7)));
        let other = json!({"requests": {"a": 1}, "x": 2});
        assert_eq!(Stored::parse(&other), Stored::Shared(other.clone()));
        assert_eq!(migrate_state(0, json!(5)).unwrap(), json!(5));
        assert_eq!(migrate_state(1, stored.clone()).unwrap(), stored);
        assert!(migrate_state(2, json!(5)).is_err());
    }
}
