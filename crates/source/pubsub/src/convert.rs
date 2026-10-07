//! Pure payload decoding and message → record assembly. No SDK types leak in
//! here beyond the plain fields (`data` / `attributes` / `message_id` / …)
//! read off a message, so this module unit-tests fully offline.

use crate::config::{OnDecodeError, ValueFormat};
use faucet_core::FaucetError;
use serde_json::{Map, Value, json};
use std::collections::HashMap;

/// Decode one message payload per the configured format. Pure.
pub(crate) fn decode_payload(
    data: &[u8],
    format: ValueFormat,
    message_id: &str,
) -> Result<Value, FaucetError> {
    match format {
        ValueFormat::Json => serde_json::from_slice(data).map_err(|e| {
            FaucetError::Source(format!(
                "pubsub: message {message_id} payload is not valid JSON: {e}"
            ))
        }),
        ValueFormat::String => match std::str::from_utf8(data) {
            Ok(s) => Ok(Value::String(s.to_string())),
            Err(e) => Err(FaucetError::Source(format!(
                "pubsub: message {message_id} payload is not valid UTF-8: {e}"
            ))),
        },
        ValueFormat::Bytes => {
            use base64::Engine as _;
            Ok(Value::String(
                base64::engine::general_purpose::STANDARD.encode(data),
            ))
        }
    }
}

/// Convert a message's attribute map into a JSON object of strings. Pure.
pub(crate) fn attributes_to_value(attributes: &HashMap<String, String>) -> Value {
    let mut map = Map::with_capacity(attributes.len());
    for (k, v) in attributes {
        map.insert(k.clone(), Value::String(v.clone()));
    }
    Value::Object(map)
}

/// Assemble the emitted record from a decoded payload + Pub/Sub metadata.
/// Pure — `publish_time_millis`/`ordering_key` are omitted when absent/empty.
#[allow(clippy::too_many_arguments)]
pub(crate) fn message_to_record(
    data: &[u8],
    attributes: &HashMap<String, String>,
    message_id: &str,
    ordering_key: &str,
    publish_time_millis: Option<i64>,
    format: ValueFormat,
    attributes_key: &str,
) -> Result<Value, FaucetError> {
    let payload = decode_payload(data, format, message_id)?;
    let mut record = Map::new();
    record.insert("data".to_string(), payload);
    record.insert(attributes_key.to_string(), attributes_to_value(attributes));
    record.insert("message_id".to_string(), json!(message_id));
    if !ordering_key.is_empty() {
        record.insert("ordering_key".to_string(), json!(ordering_key));
    }
    if let Some(ms) = publish_time_millis {
        record.insert("publish_time_millis".to_string(), json!(ms));
    }
    Ok(Value::Object(record))
}

/// The message's raw parts, as read off a received message.
pub(crate) struct MessageParts<'a> {
    pub data: &'a [u8],
    pub attributes: &'a HashMap<String, String>,
    pub message_id: &'a str,
    pub ordering_key: &'a str,
    pub publish_time_millis: Option<i64>,
}

/// [`message_to_record`] under the `on_decode_error` policy: `Ok(None)` means
/// the message is skipped (acked with its page, no record). Pure.
pub(crate) fn record_with_policy(
    m: &MessageParts<'_>,
    format: ValueFormat,
    attributes_key: &str,
    on_error: OnDecodeError,
) -> Result<Option<Value>, FaucetError> {
    let decoded = message_to_record(
        m.data,
        m.attributes,
        m.message_id,
        m.ordering_key,
        m.publish_time_millis,
        format,
        attributes_key,
    );
    match (decoded, on_error) {
        (Ok(record), _) => Ok(Some(record)),
        (Err(e), OnDecodeError::Fail) => Err(e),
        (Err(e), OnDecodeError::Skip) => {
            tracing::warn!(error = %e, "pubsub: undecodable message skipped");
            Ok(None)
        }
        (Err(e), OnDecodeError::Raw) => {
            let mut record = message_to_record(
                m.data,
                m.attributes,
                m.message_id,
                m.ordering_key,
                m.publish_time_millis,
                ValueFormat::Bytes,
                attributes_key,
            )?;
            if let Value::Object(map) = &mut record {
                map.insert("decode_error".to_string(), json!(e.to_string()));
            }
            Ok(Some(record))
        }
    }
}

/// Convert a protobuf `Timestamp` (seconds + nanos) to epoch milliseconds.
/// Pure; saturating so an absurd timestamp never panics.
pub(crate) fn timestamp_millis(seconds: i64, nanos: i32) -> i64 {
    seconds
        .saturating_mul(1000)
        .saturating_add(i64::from(nanos) / 1_000_000)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attrs(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn payload_decodes_per_format() {
        let j = decode_payload(br#"{"a":1}"#, ValueFormat::Json, "m1").unwrap();
        assert_eq!(j["a"], 1);
        let err = decode_payload(b"not json", ValueFormat::Json, "m-bad")
            .unwrap_err()
            .to_string();
        assert!(err.contains("m-bad"), "{err}");

        let s = decode_payload("héllo".as_bytes(), ValueFormat::String, "m1").unwrap();
        assert_eq!(s, Value::String("héllo".into()));
        assert!(decode_payload(&[0xff, 0xfe], ValueFormat::String, "m1").is_err());

        let b = decode_payload(&[1, 2, 3], ValueFormat::Bytes, "m1").unwrap();
        assert_eq!(b, Value::String("AQID".into()));
    }

    #[test]
    fn attributes_map_to_string_object() {
        let v = attributes_to_value(&attrs(&[("k1", "v1"), ("k2", "v2")]));
        assert_eq!(v["k1"], "v1");
        assert_eq!(v["k2"], "v2");
        assert_eq!(attributes_to_value(&HashMap::new()), json!({}));
    }

    #[test]
    fn record_shape_full() {
        let r = message_to_record(
            br#"{"x":1}"#,
            &attrs(&[("origin", "eu")]),
            "msg-7",
            "order-42",
            Some(1_716_700_000_123),
            ValueFormat::Json,
            "__attributes",
        )
        .unwrap();
        assert_eq!(r["data"]["x"], 1);
        assert_eq!(r["__attributes"]["origin"], "eu");
        assert_eq!(r["message_id"], "msg-7");
        assert_eq!(r["ordering_key"], "order-42");
        assert_eq!(r["publish_time_millis"], 1_716_700_000_123i64);
    }

    #[test]
    fn record_omits_absent_optionals_and_honours_attributes_key() {
        let r = message_to_record(
            b"raw text",
            &HashMap::new(),
            "msg-8",
            "", // empty ordering key → omitted
            None,
            ValueFormat::String,
            "attrs",
        )
        .unwrap();
        assert_eq!(r["data"], "raw text");
        assert!(r.get("attrs").is_some());
        assert!(r.get("__attributes").is_none());
        assert!(r.get("ordering_key").is_none());
        assert!(r.get("publish_time_millis").is_none());
    }

    #[test]
    fn record_propagates_decode_error() {
        let err = message_to_record(
            b"not json",
            &HashMap::new(),
            "msg-9",
            "",
            None,
            ValueFormat::Json,
            "__attributes",
        )
        .unwrap_err();
        assert!(err.to_string().contains("msg-9"), "{err}");
    }

    #[test]
    fn timestamp_millis_math() {
        assert_eq!(
            timestamp_millis(1_716_700_000, 123_000_000),
            1_716_700_000_123
        );
        assert_eq!(timestamp_millis(0, 0), 0);
        // Saturating on overflow rather than panicking.
        assert_eq!(timestamp_millis(i64::MAX, 0), i64::MAX);
    }

    #[test]
    fn on_decode_error_fails_skips_or_keeps_the_raw_payload() {
        let a = attrs(&[]);
        let m = MessageParts {
            data: b"not json",
            attributes: &a,
            message_id: "m1",
            ordering_key: "",
            publish_time_millis: None,
        };
        let fail = record_with_policy(&m, ValueFormat::Json, "__attributes", OnDecodeError::Fail);
        assert!(fail.unwrap_err().to_string().contains("m1"));
        assert_eq!(
            record_with_policy(&m, ValueFormat::Json, "__attributes", OnDecodeError::Skip).unwrap(),
            None
        );
        let raw = record_with_policy(&m, ValueFormat::Json, "__attributes", OnDecodeError::Raw)
            .unwrap()
            .unwrap();
        assert_eq!(raw["data"], "bm90IGpzb24=");
        assert!(
            raw["decode_error"]
                .as_str()
                .unwrap()
                .contains("not valid JSON")
        );
        let ok = MessageParts {
            data: br#"{"a":1}"#,
            ..m
        };
        assert_eq!(
            record_with_policy(&ok, ValueFormat::Json, "__attributes", OnDecodeError::Skip)
                .unwrap()
                .unwrap()["data"]["a"],
            1
        );
    }
}
