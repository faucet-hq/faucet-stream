//! Pure partition-key / hash-key derivation and payload encoding.

use crate::config::{ExplicitHashKey, PartitionKey, ValueFormat};
use faucet_core::FaucetError;
use serde_json::Value;

/// Kinesis partition keys must be 1..=256 unicode characters.
const MAX_KEY_CHARS: usize = 256;

/// Resolve a dot path (`a.b.c`, object keys only) into a record.
fn resolve_path<'a>(record: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cur = record;
    for seg in path.split('.') {
        cur = cur.get(seg)?;
    }
    Some(cur)
}

/// Stringify a scalar for use as a key. Containers and null are per-record
/// errors — silently serializing an object would spread a hot key across
/// shards unpredictably and hide config mistakes.
fn scalar_to_key(v: &Value, what: &str) -> Result<String, FaucetError> {
    match v {
        Value::String(s) => Ok(s.clone()),
        Value::Number(n) => Ok(n.to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        Value::Null => Err(FaucetError::Sink(format!(
            "kinesis: {what} resolved to null — cannot derive a partition key"
        ))),
        Value::Array(_) | Value::Object(_) => Err(FaucetError::Sink(format!(
            "kinesis: {what} resolved to a container — partition keys must be scalars"
        ))),
    }
}

/// Validate the final key length (Kinesis limit: 1..=256 chars).
fn check_key(key: String, what: &str) -> Result<String, FaucetError> {
    if key.is_empty() {
        return Err(FaucetError::Sink(format!(
            "kinesis: {what} resolved to an empty partition key"
        )));
    }
    if key.chars().count() > MAX_KEY_CHARS {
        return Err(FaucetError::Sink(format!(
            "kinesis: {what} resolved to a partition key longer than {MAX_KEY_CHARS} characters"
        )));
    }
    Ok(key)
}

/// Derive the partition key for one record.
pub(crate) fn derive_partition_key(
    record: &Value,
    strategy: &PartitionKey,
) -> Result<String, FaucetError> {
    match strategy {
        PartitionKey::Random => Ok(uuid::Uuid::new_v4().to_string()),
        PartitionKey::Static { value } => Ok(value.clone()),
        PartitionKey::Field { name } => {
            let v = record.get(name).ok_or_else(|| {
                FaucetError::Sink(format!(
                    "kinesis: record has no top-level field '{name}' for the partition key"
                ))
            })?;
            check_key(
                scalar_to_key(v, &format!("field '{name}'"))?,
                &format!("field '{name}'"),
            )
        }
        PartitionKey::Jsonpath { path } => {
            let v = resolve_path(record, path).ok_or_else(|| {
                FaucetError::Sink(format!(
                    "kinesis: path '{path}' matched nothing for the partition key"
                ))
            })?;
            check_key(
                scalar_to_key(v, &format!("path '{path}'"))?,
                &format!("path '{path}'"),
            )
        }
        PartitionKey::Hash { path } => {
            let v = resolve_path(record, path).ok_or_else(|| {
                FaucetError::Sink(format!(
                    "kinesis: path '{path}' matched nothing for the hash partition key"
                ))
            })?;
            let raw = scalar_to_key(v, &format!("path '{path}'"))?;
            use md5::Digest as _;
            let digest = md5::Md5::digest(raw.as_bytes());
            Ok(format!("{digest:x}"))
        }
    }
}

/// Derive the optional explicit hash key for one record.
pub(crate) fn derive_explicit_hash_key(
    record: &Value,
    strategy: &ExplicitHashKey,
) -> Result<Option<String>, FaucetError> {
    let v = match strategy {
        ExplicitHashKey::None => return Ok(None),
        ExplicitHashKey::Field { name } => record.get(name).ok_or_else(|| {
            FaucetError::Sink(format!(
                "kinesis: record has no top-level field '{name}' for the explicit hash key"
            ))
        })?,
        ExplicitHashKey::Jsonpath { path } => resolve_path(record, path).ok_or_else(|| {
            FaucetError::Sink(format!(
                "kinesis: path '{path}' matched nothing for the explicit hash key"
            ))
        })?,
    };
    let key = scalar_to_key(v, "explicit hash key")?;
    // Kinesis requires a decimal integer in [0, 2^128); a 39-digit value can
    // still be 2^128 or more, so parse rather than count digits (#789 MSG-91).
    if key.is_empty() || !key.bytes().all(|b| b.is_ascii_digit()) || key.parse::<u128>().is_err() {
        return Err(FaucetError::Sink(
            "kinesis: explicit hash key must be a decimal integer in [0, 2^128)".into(),
        ));
    }
    Ok(Some(key))
}

/// Encode one record's payload bytes per the configured format.
pub(crate) fn encode_payload(record: &Value, format: ValueFormat) -> Result<Vec<u8>, FaucetError> {
    match format {
        ValueFormat::Json => serde_json::to_vec(record)
            .map_err(|e| FaucetError::Sink(format!("kinesis: record serialization failed: {e}"))),
        ValueFormat::String => match record {
            Value::String(s) => Ok(s.clone().into_bytes()),
            other => Err(FaucetError::Sink(format!(
                "kinesis: value_format 'string' requires string records (got {})",
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
                            "kinesis: value_format 'bytes' requires base64 strings: {e}"
                        ))
                    })
            }
            other => Err(FaucetError::Sink(format!(
                "kinesis: value_format 'bytes' requires base64 string records (got {})",
                type_name(other)
            ))),
        },
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn random_and_static_strategies() {
        let r = json!({});
        let a = derive_partition_key(&r, &PartitionKey::Random).unwrap();
        let b = derive_partition_key(&r, &PartitionKey::Random).unwrap();
        assert_ne!(a, b, "random keys differ");
        assert_eq!(
            derive_partition_key(
                &r,
                &PartitionKey::Static {
                    value: "const".into()
                }
            )
            .unwrap(),
            "const"
        );
    }

    #[test]
    fn field_and_jsonpath_strategies() {
        let r = json!({"user_id": 42, "nested": {"id": "abc"}, "flag": true});
        assert_eq!(
            derive_partition_key(
                &r,
                &PartitionKey::Field {
                    name: "user_id".into()
                }
            )
            .unwrap(),
            "42"
        );
        assert_eq!(
            derive_partition_key(
                &r,
                &PartitionKey::Jsonpath {
                    path: "nested.id".into()
                }
            )
            .unwrap(),
            "abc"
        );
        assert_eq!(
            derive_partition_key(
                &r,
                &PartitionKey::Field {
                    name: "flag".into()
                }
            )
            .unwrap(),
            "true"
        );

        // Missing / null / container values are per-record errors.
        assert!(
            derive_partition_key(
                &r,
                &PartitionKey::Field {
                    name: "nope".into()
                }
            )
            .is_err()
        );
        let with_null = json!({"k": null, "obj": {"a": 1}});
        assert!(
            derive_partition_key(&with_null, &PartitionKey::Field { name: "k".into() }).is_err()
        );
        assert!(
            derive_partition_key(&with_null, &PartitionKey::Field { name: "obj".into() }).is_err()
        );
    }

    #[test]
    fn hash_strategy_is_stable_hex() {
        let r = json!({"event": {"id": "hot-key"}});
        let a = derive_partition_key(
            &r,
            &PartitionKey::Hash {
                path: "event.id".into(),
            },
        )
        .unwrap();
        assert_eq!(a.len(), 32, "md5 hex");
        let b = derive_partition_key(
            &r,
            &PartitionKey::Hash {
                path: "event.id".into(),
            },
        )
        .unwrap();
        assert_eq!(a, b, "deterministic");
    }

    #[test]
    fn key_length_limit_enforced() {
        let r = json!({"k": "x".repeat(257)});
        assert!(derive_partition_key(&r, &PartitionKey::Field { name: "k".into() }).is_err());
        let ok = json!({"k": "x".repeat(256)});
        assert!(derive_partition_key(&ok, &PartitionKey::Field { name: "k".into() }).is_ok());
    }

    #[test]
    fn explicit_hash_key_rules() {
        let r = json!({
            "h": "123456",
            "bad": "0x12",
            "big": "1".repeat(40),
            "max": u128::MAX.to_string(),
            "over": "340282366920938463463374607431768211456"
        });
        assert!(
            derive_explicit_hash_key(&r, &ExplicitHashKey::Field { name: "max".into() }).is_ok()
        );
        assert!(
            derive_explicit_hash_key(
                &r,
                &ExplicitHashKey::Field {
                    name: "over".into()
                }
            )
            .is_err(),
            "2^128 has 39 digits and is still out of range"
        );
        assert_eq!(
            derive_explicit_hash_key(&r, &ExplicitHashKey::None).unwrap(),
            None
        );
        assert_eq!(
            derive_explicit_hash_key(&r, &ExplicitHashKey::Field { name: "h".into() })
                .unwrap()
                .as_deref(),
            Some("123456")
        );
        assert!(
            derive_explicit_hash_key(&r, &ExplicitHashKey::Field { name: "bad".into() }).is_err(),
            "non-decimal rejected"
        );
        assert!(
            derive_explicit_hash_key(&r, &ExplicitHashKey::Field { name: "big".into() }).is_err(),
            "over 2^128 rejected"
        );
        assert!(
            derive_explicit_hash_key(
                &r,
                &ExplicitHashKey::Jsonpath {
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

        let b64 = json!("AQID");
        assert_eq!(
            encode_payload(&b64, ValueFormat::Bytes).unwrap(),
            vec![1, 2, 3]
        );
        assert!(encode_payload(&json!("!!!"), ValueFormat::Bytes).is_err());
        assert!(encode_payload(&obj, ValueFormat::Bytes).is_err());
    }
}
