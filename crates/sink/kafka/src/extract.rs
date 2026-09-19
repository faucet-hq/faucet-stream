//! Extract key / partition / topic / headers from records via JSONPath.

use faucet_core::FaucetError;
use jsonpath_rust::JsonPath;
use serde_json::{Map, Value};

/// Resolve a single JSONPath against `record`. Returns the first match (or
/// `None` if the path doesn't match anything).
fn first_match(record: &Value, path: &str) -> Result<Option<Value>, FaucetError> {
    let hits = record
        .query(path)
        .map_err(|e| FaucetError::Config(format!("invalid JSONPath '{path}': {e}")))?;
    Ok(hits.into_iter().next().cloned())
}

/// Extract a string value via JSONPath. Numbers are stringified; booleans
/// become "true"/"false". Returns `None` if the path doesn't resolve.
pub fn string_at(record: &Value, path: &str) -> Result<Option<String>, FaucetError> {
    let Some(v) = first_match(record, path)? else {
        return Ok(None);
    };
    Ok(Some(match v {
        Value::String(s) => s,
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        other => other.to_string(),
    }))
}

/// Extract an i32 partition. Errors on non-integer matches or out-of-range values.
pub fn partition_at(record: &Value, path: &str) -> Result<Option<i32>, FaucetError> {
    let Some(v) = first_match(record, path)? else {
        return Ok(None);
    };
    match v.as_i64() {
        Some(n) if n >= 0 && n <= i32::MAX as i64 => Ok(Some(n as i32)),
        Some(n) => Err(FaucetError::Sink(format!(
            "partition_path '{path}' resolved to out-of-range {n}"
        ))),
        None => Err(FaucetError::Sink(format!(
            "partition_path '{path}' did not resolve to an integer"
        ))),
    }
}

/// Extract a headers map. The path must resolve to a JSON object; non-string
/// values are stringified, **except `null`, which is preserved**.
///
/// A Kafka header may legitimately have *no* value, and that is the honest
/// rendering of JSON `null`. Stringifying it to `"null"` would be
/// indistinguishable, to a consumer, from the four-character string — so the
/// null survives here and the sink turns it into a valueless header (#657).
pub fn headers_at(record: &Value, path: &str) -> Result<Option<Map<String, Value>>, FaucetError> {
    let Some(v) = first_match(record, path)? else {
        return Ok(None);
    };
    let obj = v.as_object().ok_or_else(|| {
        FaucetError::Sink(format!(
            "headers_path '{path}' did not resolve to a JSON object"
        ))
    })?;
    let mut out = Map::new();
    for (k, val) in obj {
        let rendered = match val {
            Value::Null => Value::Null,
            Value::String(s) => Value::String(s.clone()),
            other => Value::String(other.to_string()),
        };
        out.insert(k.clone(), rendered);
    }
    Ok(Some(out))
}

/// Resolve a JSONPath against `record` and return the first matching JSON value.
pub fn value_at(record: &Value, path: &str) -> Result<Option<Value>, FaucetError> {
    first_match(record, path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn string_at_extracts_top_level_field() {
        let r = json!({"user_id": "alice"});
        assert_eq!(
            string_at(&r, "$.user_id").unwrap().as_deref(),
            Some("alice")
        );
    }

    #[test]
    fn string_at_returns_none_for_missing_path() {
        let r = json!({"a": 1});
        assert!(string_at(&r, "$.b").unwrap().is_none());
    }

    #[test]
    fn string_at_stringifies_numbers() {
        let r = json!({"id": 42});
        assert_eq!(string_at(&r, "$.id").unwrap().as_deref(), Some("42"));
    }

    #[test]
    fn string_at_stringifies_booleans() {
        let r = json!({"flag": true});
        assert_eq!(string_at(&r, "$.flag").unwrap().as_deref(), Some("true"));
    }

    #[test]
    fn partition_at_returns_i32() {
        let r = json!({"p": 3});
        assert_eq!(partition_at(&r, "$.p").unwrap(), Some(3));
    }

    #[test]
    fn partition_at_returns_none_for_missing() {
        let r = json!({"x": 1});
        assert_eq!(partition_at(&r, "$.p").unwrap(), None);
    }

    #[test]
    fn partition_at_rejects_negative() {
        let r = json!({"p": -1});
        assert!(partition_at(&r, "$.p").is_err());
    }

    #[test]
    fn partition_at_rejects_out_of_range() {
        let r = json!({"p": i64::from(i32::MAX) + 1});
        assert!(partition_at(&r, "$.p").is_err());
    }

    #[test]
    fn partition_at_rejects_non_integer() {
        let r = json!({"p": "not a number"});
        assert!(partition_at(&r, "$.p").is_err());
    }

    #[test]
    fn headers_at_extracts_object_into_map() {
        let r = json!({"h": {"x": "y", "n": 1}});
        let h = headers_at(&r, "$.h").unwrap().unwrap();
        assert_eq!(h.get("x").and_then(|v| v.as_str()), Some("y"));
        assert_eq!(h.get("n").and_then(|v| v.as_str()), Some("1"));
    }

    #[test]
    fn headers_at_preserves_null_so_the_sink_can_emit_a_valueless_header() {
        // Kafka headers permit no value at all, which is what `null` means. A
        // stringified "null" would be indistinguishable from the literal text.
        let r = json!({"h": {"present": "x", "absent": null, "number": 1}});
        let h = headers_at(&r, "$.h").unwrap().unwrap();
        assert_eq!(h.get("present").and_then(|v| v.as_str()), Some("x"));
        assert!(
            h.get("absent").expect("key present").is_null(),
            "null must survive rather than becoming the string \"null\""
        );
        // Everything else still stringifies.
        assert_eq!(h.get("number").and_then(|v| v.as_str()), Some("1"));
    }

    #[test]
    fn headers_at_returns_none_for_missing() {
        let r = json!({});
        assert!(headers_at(&r, "$.h").unwrap().is_none());
    }

    #[test]
    fn headers_at_rejects_non_object() {
        let r = json!({"h": "string"});
        assert!(headers_at(&r, "$.h").is_err());
    }

    #[test]
    fn invalid_path_is_a_config_error() {
        let r = json!({});
        let err = string_at(&r, "not a path").unwrap_err();
        assert!(format!("{err}").to_lowercase().contains("jsonpath"));
    }

    #[test]
    fn value_at_returns_subobject() {
        let r = json!({"nested": {"a": 1}});
        let v = value_at(&r, "$.nested").unwrap().unwrap();
        assert_eq!(v["a"], 1);
    }
}
