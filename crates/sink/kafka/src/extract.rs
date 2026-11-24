//! Extract key / partition / topic / headers from records via JSONPath.

use faucet_core::FaucetError;
use jsonpath_rust::parser::model::JpQuery;
use jsonpath_rust::parser::parse_json_path;
use jsonpath_rust::query::js_path_process;
use serde_json::{Map, Value};

/// A JSONPath parsed once and evaluated per record.
pub struct CompiledPath {
    path: String,
    query: Result<JpQuery, String>,
}

impl CompiledPath {
    /// Parse `path`. A path that does not parse fails each lookup with the
    /// error a per-record parse gives.
    pub fn new(path: &str) -> Self {
        Self {
            path: path.to_owned(),
            query: parse_json_path(path).map_err(|e| e.to_string()),
        }
    }

    /// The path as configured.
    pub fn path(&self) -> &str {
        &self.path
    }

    fn first<'a>(&self, record: &'a Value) -> Result<Option<&'a Value>, FaucetError> {
        let invalid = |e: &dyn std::fmt::Display| {
            FaucetError::Config(format!("invalid JSONPath '{}': {e}", self.path))
        };
        let query = self.query.as_ref().map_err(|e| invalid(e))?;
        let hits = js_path_process(query, record).map_err(|e| invalid(&e))?;
        Ok(hits.into_iter().next().map(|hit| hit.val()))
    }

    /// [`string_at`] for a parsed path.
    pub fn string(&self, record: &Value) -> Result<Option<String>, FaucetError> {
        Ok(match self.first(record)? {
            None | Some(Value::Null) => None,
            Some(Value::String(s)) => Some(s.clone()),
            Some(Value::Number(n)) => Some(n.to_string()),
            Some(Value::Bool(b)) => Some(b.to_string()),
            Some(other) => Some(other.to_string()),
        })
    }

    /// [`partition_at`] for a parsed path.
    pub fn partition(&self, record: &Value) -> Result<Option<i32>, FaucetError> {
        let Some(v) = self.first(record)? else {
            return Ok(None);
        };
        match v.as_i64() {
            Some(n) if n >= 0 && n <= i32::MAX as i64 => Ok(Some(n as i32)),
            Some(n) => Err(FaucetError::Sink(format!(
                "partition_path '{}' resolved to out-of-range {n}",
                self.path
            ))),
            None => Err(FaucetError::Sink(format!(
                "partition_path '{}' did not resolve to an integer",
                self.path
            ))),
        }
    }

    /// [`headers_at`] for a parsed path.
    pub fn headers(&self, record: &Value) -> Result<Option<Map<String, Value>>, FaucetError> {
        let Some(v) = self.first(record)? else {
            return Ok(None);
        };
        let obj = v.as_object().ok_or_else(|| {
            FaucetError::Sink(format!(
                "headers_path '{}' did not resolve to a JSON object",
                self.path
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

    /// [`value_at`] for a parsed path.
    pub fn value(&self, record: &Value) -> Result<Option<Value>, FaucetError> {
        Ok(self.first(record)?.cloned())
    }
}

/// Extract a string value via JSONPath. Numbers are stringified; booleans
/// become "true"/"false". Returns `None` if the path doesn't resolve **or
/// resolves to JSON `null`** — a null key must reach `on_key_error` rather
/// than collapse every such record onto the literal key `"null"`.
pub fn string_at(record: &Value, path: &str) -> Result<Option<String>, FaucetError> {
    CompiledPath::new(path).string(record)
}

/// Extract an i32 partition. Errors on non-integer matches or out-of-range values.
pub fn partition_at(record: &Value, path: &str) -> Result<Option<i32>, FaucetError> {
    CompiledPath::new(path).partition(record)
}

/// Extract a headers map. The path must resolve to a JSON object; non-string
/// values are stringified, **except `null`, which is preserved**.
///
/// A Kafka header may legitimately have *no* value, and that is the honest
/// rendering of JSON `null`. Stringifying it to `"null"` would be
/// indistinguishable, to a consumer, from the four-character string — so the
/// null survives here and the sink turns it into a valueless header (#657).
pub fn headers_at(record: &Value, path: &str) -> Result<Option<Map<String, Value>>, FaucetError> {
    CompiledPath::new(path).headers(record)
}

/// Resolve a JSONPath against `record` and return the first matching JSON value.
pub fn value_at(record: &Value, path: &str) -> Result<Option<Value>, FaucetError> {
    CompiledPath::new(path).value(record)
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
    fn string_at_treats_null_as_unresolved() {
        let r = json!({"id": null});
        assert!(string_at(&r, "$.id").unwrap().is_none());
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

    /// The per-record parse this module used before paths were compiled.
    fn first_match_reference(record: &Value, path: &str) -> Result<Option<Value>, FaucetError> {
        use jsonpath_rust::JsonPath;
        let hits = record
            .query(path)
            .map_err(|e| FaucetError::Config(format!("invalid JSONPath '{path}': {e}")))?;
        Ok(hits.into_iter().next().cloned())
    }

    #[test]
    fn a_compiled_path_resolves_like_a_path_parsed_per_record() {
        let records = [
            json!({"id": 7, "user": {"name": "a", "tags": ["x", "y"]}, "h": {"k": 1, "n": null}}),
            json!({"id": null, "user": {"tags": []}}),
            json!({"id": "s", "p": 3, "list": [{"id": 1}, {"id": 2}]}),
            json!([1, 2, 3]),
            json!("text"),
            json!({}),
        ];
        let paths = [
            "$.id",
            "$.user.name",
            "$.user.tags[0]",
            "$.user.tags[*]",
            "$..id",
            "$.h",
            "$.p",
            "$.list[?(@.id > 1)].id",
            "$[1]",
            "$",
            "not a path",
            "$[",
            "id",
            "",
        ];
        for path in paths {
            let compiled = CompiledPath::new(path);
            assert_eq!(compiled.path(), path);
            for record in &records {
                let expected = first_match_reference(record, path).map_err(|e| format!("{e:?}"));
                let got = compiled.value(record).map_err(|e| format!("{e:?}"));
                assert_eq!(got, expected, "{path:?} over {record}");
                assert_eq!(
                    compiled.string(record).map_err(|e| format!("{e:?}")),
                    string_at(record, path).map_err(|e| format!("{e:?}"))
                );
                assert_eq!(
                    compiled.partition(record).map_err(|e| format!("{e:?}")),
                    partition_at(record, path).map_err(|e| format!("{e:?}"))
                );
                assert_eq!(
                    compiled.headers(record).map_err(|e| format!("{e:?}")),
                    headers_at(record, path).map_err(|e| format!("{e:?}"))
                );
            }
        }
    }
}
