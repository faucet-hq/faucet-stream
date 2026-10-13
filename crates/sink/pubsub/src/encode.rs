//! Pure ordering-key extraction, attribute building, and payload encoding.
//! No SDK types here — `sink.rs` turns a [`Prepared`] into a `PubsubMessage`.

use crate::config::{OrderingKey, ValueFormat};
use faucet_core::FaucetError;
use serde_json::Value;
use std::collections::HashMap;

/// A record encoded and ready to become a `PubsubMessage`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Prepared {
    pub data: Vec<u8>,
    pub attributes: HashMap<String, String>,
    /// Empty when no ordering key applies.
    pub ordering_key: String,
}

/// Resolve a dot path (`a.b.c`, object keys only) into a record.
fn resolve_path<'a>(record: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cur = record;
    for seg in path.split('.') {
        cur = cur.get(seg)?;
    }
    Some(cur)
}

fn type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Stringify a scalar. Containers and null are per-record errors.
fn scalar_to_string(v: &Value, what: &str) -> Result<String, FaucetError> {
    match v {
        Value::String(s) => Ok(s.clone()),
        Value::Number(n) => Ok(n.to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        Value::Null => Err(FaucetError::Sink(format!(
            "pubsub: {what} resolved to null"
        ))),
        Value::Array(_) | Value::Object(_) => Err(FaucetError::Sink(format!(
            "pubsub: {what} resolved to a {} — must be a scalar",
            type_name(v)
        ))),
    }
}

/// Derive the ordering key for one record. `Ok(None)` = no ordering key.
pub(crate) fn derive_ordering_key(
    record: &Value,
    strategy: &OrderingKey,
) -> Result<Option<String>, FaucetError> {
    match strategy {
        OrderingKey::None => Ok(None),
        OrderingKey::Field { name } => {
            let v = record.get(name).ok_or_else(|| {
                FaucetError::Sink(format!(
                    "pubsub: record has no top-level field '{name}' for the ordering key"
                ))
            })?;
            Ok(Some(scalar_to_string(v, &format!("field '{name}'"))?))
        }
        OrderingKey::Jsonpath { path } => {
            let v = resolve_path(record, path).ok_or_else(|| {
                FaucetError::Sink(format!(
                    "pubsub: path '{path}' matched nothing for the ordering key"
                ))
            })?;
            Ok(Some(scalar_to_string(v, &format!("path '{path}'"))?))
        }
    }
}

/// Encode one record's payload bytes per the configured format. Pure.
pub(crate) fn encode_payload(record: &Value, format: ValueFormat) -> Result<Vec<u8>, FaucetError> {
    match format {
        ValueFormat::Json => serde_json::to_vec(record)
            .map_err(|e| FaucetError::Sink(format!("pubsub: record serialization failed: {e}"))),
        ValueFormat::String => match record {
            Value::String(s) => Ok(s.clone().into_bytes()),
            other => Err(FaucetError::Sink(format!(
                "pubsub: value_format 'string' requires string records (got {})",
                type_name(other)
            ))),
        },
        ValueFormat::Bytes => match record {
            Value::String(s) => {
                use base64::Engine as _;
                base64::engine::general_purpose::STANDARD
                    .decode(s.as_bytes())
                    .map_err(|e| {
                        FaucetError::Sink(format!(
                            "pubsub: value_format 'bytes' requires base64 strings: {e}"
                        ))
                    })
            }
            other => Err(FaucetError::Sink(format!(
                "pubsub: value_format 'bytes' requires base64 string records (got {})",
                type_name(other)
            ))),
        },
    }
}

/// Pull a message-attribute map out of `attributes_field` (a JSON object of
/// scalars). Returns the attributes and the payload record with that field
/// removed. Absent field → empty attributes + the record unchanged.
fn extract_attributes<'a>(
    record: &'a Value,
    attributes_field: Option<&str>,
) -> Result<(HashMap<String, String>, std::borrow::Cow<'a, Value>), FaucetError> {
    let Some(field) = attributes_field else {
        return Ok((HashMap::new(), std::borrow::Cow::Borrowed(record)));
    };
    let Some(raw) = record.get(field) else {
        return Ok((HashMap::new(), std::borrow::Cow::Borrowed(record)));
    };
    let obj = raw.as_object().ok_or_else(|| {
        FaucetError::Sink(format!(
            "pubsub: attributes_field '{field}' must be a JSON object (got {})",
            type_name(raw)
        ))
    })?;
    let mut attributes = HashMap::with_capacity(obj.len());
    for (k, v) in obj {
        attributes.insert(k.clone(), scalar_to_string(v, &format!("attribute '{k}'"))?);
    }
    // Strip the attributes field from the payload.
    let mut stripped = record.clone();
    if let Some(map) = stripped.as_object_mut() {
        map.remove(field);
    }
    Ok((attributes, std::borrow::Cow::Owned(stripped)))
}

/// Build the full [`Prepared`] message for one record. Pure — the single
/// per-record entry point `sink.rs` calls.
pub(crate) fn prepare(
    record: &Value,
    value_format: ValueFormat,
    ordering_key: &OrderingKey,
    attributes_field: Option<&str>,
) -> Result<Prepared, FaucetError> {
    let ordering = derive_ordering_key(record, ordering_key)?.unwrap_or_default();
    let (attributes, payload) = extract_attributes(record, attributes_field)?;
    let data = encode_payload(payload.as_ref(), value_format)?;
    Ok(Prepared {
        data,
        attributes,
        ordering_key: ordering,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    /// The value-fidelity corpus (#885): every record this sink encodes reads
    /// back unchanged.
    #[test]
    fn the_fidelity_corpus_encodes_unchanged() {
        use faucet_conformance::fidelity;
        let sent = fidelity::corpus();
        let mut landed = Vec::new();
        for record in &sent {
            let bytes: Vec<u8> = encode_payload(record, ValueFormat::Json).unwrap();
            landed.push(faucet_core::fidelity::from_slice(&bytes).unwrap());
        }
        fidelity::assert_round_trip(&sent, &landed, fidelity::Tolerance::exact());
    }

    use serde_json::json;

    #[test]
    fn ordering_key_strategies() {
        let r = json!({"customer_id": 42, "nested": {"id": "abc"}, "flag": true});
        assert_eq!(derive_ordering_key(&r, &OrderingKey::None).unwrap(), None);
        assert_eq!(
            derive_ordering_key(
                &r,
                &OrderingKey::Field {
                    name: "customer_id".into()
                }
            )
            .unwrap()
            .as_deref(),
            Some("42")
        );
        assert_eq!(
            derive_ordering_key(
                &r,
                &OrderingKey::Jsonpath {
                    path: "nested.id".into()
                }
            )
            .unwrap()
            .as_deref(),
            Some("abc")
        );
        assert_eq!(
            derive_ordering_key(
                &r,
                &OrderingKey::Field {
                    name: "flag".into()
                }
            )
            .unwrap()
            .as_deref(),
            Some("true")
        );
        // Missing / null / container fail per-record.
        assert!(
            derive_ordering_key(
                &r,
                &OrderingKey::Field {
                    name: "nope".into()
                }
            )
            .is_err()
        );
        let bad = json!({"k": null, "obj": {"a": 1}});
        assert!(derive_ordering_key(&bad, &OrderingKey::Field { name: "k".into() }).is_err());
        assert!(derive_ordering_key(&bad, &OrderingKey::Field { name: "obj".into() }).is_err());
        assert!(
            derive_ordering_key(
                &r,
                &OrderingKey::Jsonpath {
                    path: "no.pe".into()
                }
            )
            .is_err()
        );
    }

    #[test]
    fn payload_encoding_per_format() {
        let obj = json!({"a": 1});
        assert_eq!(
            encode_payload(&obj, ValueFormat::Json).unwrap(),
            br#"{"a":1}"#.to_vec()
        );

        let s = json!("plain text");
        assert_eq!(
            encode_payload(&s, ValueFormat::String).unwrap(),
            b"plain text".to_vec()
        );
        assert!(encode_payload(&obj, ValueFormat::String).is_err());

        assert_eq!(
            encode_payload(&json!("AQID"), ValueFormat::Bytes).unwrap(),
            vec![1, 2, 3]
        );
        assert!(encode_payload(&json!("!!!"), ValueFormat::Bytes).is_err());
        assert!(encode_payload(&obj, ValueFormat::Bytes).is_err());
    }

    #[test]
    fn attributes_extracted_and_stripped() {
        let r = json!({"a": 1, "__attributes": {"origin": "eu", "n": 7, "ok": true}});
        let (attrs, payload) = extract_attributes(&r, Some("__attributes")).unwrap();
        assert_eq!(attrs.get("origin").map(String::as_str), Some("eu"));
        assert_eq!(attrs.get("n").map(String::as_str), Some("7"));
        assert_eq!(attrs.get("ok").map(String::as_str), Some("true"));
        // The field is removed from the payload.
        assert!(payload.get("__attributes").is_none());
        assert_eq!(payload["a"], 1);
    }

    #[test]
    fn attributes_absent_field_is_no_op() {
        let r = json!({"a": 1});
        let (attrs, payload) = extract_attributes(&r, Some("__attributes")).unwrap();
        assert!(attrs.is_empty());
        assert_eq!(payload.as_ref(), &r);

        let (attrs, payload) = extract_attributes(&r, None).unwrap();
        assert!(attrs.is_empty());
        assert_eq!(payload.as_ref(), &r);
    }

    #[test]
    fn attributes_field_must_be_object_of_scalars() {
        let r = json!({"__attributes": "not an object"});
        assert!(extract_attributes(&r, Some("__attributes")).is_err());
        let r = json!({"__attributes": {"nested": {"x": 1}}});
        assert!(extract_attributes(&r, Some("__attributes")).is_err());
    }

    #[test]
    fn prepare_end_to_end() {
        let r = json!({"id": "o-1", "amount": 5, "__attributes": {"src": "web"}});
        let p = prepare(
            &r,
            ValueFormat::Json,
            &OrderingKey::Field { name: "id".into() },
            Some("__attributes"),
        )
        .unwrap();
        assert_eq!(p.ordering_key, "o-1");
        assert_eq!(p.attributes.get("src").map(String::as_str), Some("web"));
        // Payload is the record minus __attributes.
        let decoded: Value = serde_json::from_slice(&p.data).unwrap();
        assert_eq!(decoded["id"], "o-1");
        assert_eq!(decoded["amount"], 5);
        assert!(decoded.get("__attributes").is_none());
    }

    #[test]
    fn prepare_no_ordering_no_attributes() {
        let r = json!({"id": "o-2"});
        let p = prepare(&r, ValueFormat::Json, &OrderingKey::None, None).unwrap();
        assert!(p.ordering_key.is_empty());
        assert!(p.attributes.is_empty());
        assert_eq!(serde_json::from_slice::<Value>(&p.data).unwrap(), r);
    }

    #[test]
    fn prepare_propagates_ordering_error() {
        let r = json!({"id": "o-3"});
        let err = prepare(
            &r,
            ValueFormat::Json,
            &OrderingKey::Field {
                name: "missing".into(),
            },
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("missing"), "{err}");
    }
}
