//! Stored events as pipeline records.

use crate::error::FeedSourceError;
use faucet_common_feed::{FeedUid, StoredEvent};
use faucet_core::fidelity::{self, TimePrecision};
use serde_json::{Map, Value};

/// The record key the event's metadata is stored under.
pub const METADATA_KEY: &str = "_feed";

/// The key a payload that is not a JSON object is stored under.
pub const VALUE_KEY: &str = "value";

enum Kind {
    Json,
    Form,
    Text,
    Bytes,
}

fn kind_of(content_type: &str) -> Kind {
    let media = content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if media == "application/json" || media.ends_with("+json") {
        Kind::Json
    } else if media == "application/x-www-form-urlencoded" {
        Kind::Form
    } else if media.starts_with("text/") {
        Kind::Text
    } else {
        Kind::Bytes
    }
}

fn wrapped(value: Value) -> Map<String, Value> {
    let mut record = Map::new();
    record.insert(VALUE_KEY.to_string(), value);
    record
}

/// A URL-encoded form as an object: a key sent once maps to its string, a
/// key sent more than once to an array of its strings in arrival order.
/// Names are literal (`tag[]` is the key `tag[]`).
pub(crate) fn form_record(payload: &[u8]) -> Map<String, Value> {
    let mut record = Map::new();
    for (k, v) in url::form_urlencoded::parse(payload) {
        let v = Value::String(v.into_owned());
        match record.get_mut(k.as_ref()) {
            None => {
                record.insert(k.into_owned(), v);
            }
            Some(Value::Array(values)) => values.push(v),
            Some(first) => {
                let first = std::mem::take(first);
                record.insert(k.into_owned(), Value::Array(vec![first, v]));
            }
        }
    }
    record
}

/// One event's `payload` (already opened when it was sealed) as a record:
/// a JSON object as it is; any other JSON value, text and bytes (base64)
/// under `value`; a URL-encoded form as an object of strings (an array of
/// strings for a repeated key). A payload holding a top-level `_feed` field
/// is refused: that key is reserved for the feed's metadata.
pub(crate) fn record(
    feed: &FeedUid,
    event: &StoredEvent,
    payload: &[u8],
    include_metadata: bool,
) -> Result<Value, FeedSourceError> {
    let undecodable = |reason: String| FeedSourceError::Undecodable {
        feed: feed.to_string(),
        seq: event.seq.get(),
        id: event.id.clone(),
        content_type: event.content_type.clone(),
        reason,
    };
    let mut record = match kind_of(&event.content_type) {
        Kind::Json => {
            match fidelity::from_slice(payload).map_err(|e| undecodable(e.to_string()))? {
                Value::Object(object) => object,
                other => wrapped(other),
            }
        }
        Kind::Form => form_record(payload),
        Kind::Text => match std::str::from_utf8(payload) {
            Ok(text) => wrapped(Value::String(text.to_string())),
            Err(e) => return Err(undecodable(e.to_string())),
        },
        Kind::Bytes => wrapped(fidelity::binary(payload)),
    };
    if record.contains_key(METADATA_KEY) {
        return Err(undecodable(format!(
            "the payload has a field named \"{METADATA_KEY}\", which is reserved for the feed's metadata"
        )));
    }
    if include_metadata {
        record.insert(METADATA_KEY.to_string(), metadata(event));
    }
    Ok(Value::Object(record))
}

fn metadata(event: &StoredEvent) -> Value {
    let headers: Map<String, Value> = event
        .headers
        .iter()
        .map(|(k, v)| (k.clone(), Value::String(v.clone())))
        .collect();
    let mut meta = Map::new();
    meta.insert("seq".into(), Value::from(event.seq.get()));
    meta.insert("id".into(), Value::String(event.id.clone()));
    meta.insert(
        "received_at".into(),
        fidelity::timestamp(&event.received_at.to_chrono(), TimePrecision::Micros),
    );
    meta.insert("headers".into(), Value::Object(headers));
    Value::Object(meta)
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_common_feed::Seq;
    use serde_json::json;
    use std::collections::BTreeMap;

    fn event(content_type: &str, payload: &[u8]) -> StoredEvent {
        let at = "2026-11-16T10:20:30.123456Z".parse().unwrap();
        StoredEvent {
            seq: Seq(7),
            id: "0193-e7".into(),
            received_at: at,
            stored_at: at,
            content_type: content_type.into(),
            headers: BTreeMap::from([("x-request-id".to_string(), "r-1".to_string())]),
            payload: payload.to_vec(),
            size: payload.len() as u64,
            sealed: false,
            dedupe_key: None,
        }
    }

    fn decode(content_type: &str, payload: &[u8]) -> Result<Value, FeedSourceError> {
        let e = event(content_type, payload);
        record(&FeedUid::new("orders").unwrap(), &e, &e.payload, false)
    }

    #[test]
    fn a_json_object_is_the_record() {
        assert_eq!(
            decode("application/json", br#"{"id": 1, "nested": {"a": [1, 2]}}"#).unwrap(),
            json!({"id": 1, "nested": {"a": [1, 2]}})
        );
        assert_eq!(
            decode("Application/Vnd.Api+JSON; charset=utf-8", br#"{"id": 1}"#).unwrap(),
            json!({"id": 1})
        );
    }

    #[test]
    fn other_json_values_go_under_value() {
        assert_eq!(
            decode("application/json", b"[1, 2]").unwrap(),
            json!({"value": [1, 2]})
        );
        assert_eq!(
            decode("application/json", b"\"hi\"").unwrap(),
            json!({"value": "hi"})
        );
        assert_eq!(
            decode("application/json", b"null").unwrap(),
            json!({"value": null})
        );
        assert_eq!(
            decode("application/json", b"12").unwrap(),
            json!({"value": 12})
        );
    }

    #[test]
    fn json_numbers_keep_their_exact_value() {
        let out = decode(
            "application/json",
            br#"{"big": 123456789012345678901234567890, "money": 0.1, "n": 42}"#,
        )
        .unwrap();
        assert_eq!(out["big"], json!("123456789012345678901234567890"));
        assert_eq!(out["n"], json!(42));
        assert_eq!(out["money"], json!(0.1));
    }

    #[test]
    fn text_is_a_string_and_bytes_are_base64() {
        assert_eq!(
            decode("text/plain; charset=utf-8", "héllo".as_bytes()).unwrap(),
            json!({"value": "héllo"})
        );
        assert_eq!(
            decode("text/csv", b"a,b\n1,2\n").unwrap(),
            json!({"value": "a,b\n1,2\n"})
        );
        assert_eq!(
            decode("application/octet-stream", &[0, 159, 255]).unwrap(),
            json!({"value": "AJ//"})
        );
        assert_eq!(decode("", b"raw").unwrap(), json!({"value": "cmF3"}));
    }

    #[test]
    fn a_form_is_an_object_of_strings() {
        assert_eq!(
            decode(
                "application/x-www-form-urlencoded",
                b"name=Ada+Lovelace&n=7&note=a%26b&empty="
            )
            .unwrap(),
            json!({"name": "Ada Lovelace", "n": "7", "note": "a&b", "empty": ""})
        );
    }

    #[test]
    fn a_repeated_form_key_keeps_every_value_in_order() {
        let form = "application/x-www-form-urlencoded";
        assert_eq!(
            decode(
                form,
                b"tag=a&x=1&tag=b+c&tag=%26&x=&solo=s&tag[]=1&a[b]=2&bare"
            )
            .unwrap(),
            json!({
                "tag": ["a", "b c", "&"],
                "x": ["1", ""],
                "solo": "s",
                "tag[]": "1",
                "a[b]": "2",
                "bare": ""
            })
        );
        assert_eq!(decode(form, b"a=&a=x").unwrap(), json!({"a": ["", "x"]}));
        assert_eq!(decode(form, b"k=1").unwrap(), json!({"k": "1"}));
    }

    #[test]
    fn a_stored_payload_holding_the_metadata_key_is_refused() {
        for (content_type, payload) in [
            ("application/json", &br#"{"_feed": 1, "id": 2}"#[..]),
            ("application/json", br#"{"\u005ffeed": 1}"#),
            ("application/x-www-form-urlencoded", b"a=1&%5Ffeed=x"),
        ] {
            for include_metadata in [false, true] {
                let e = event(content_type, payload);
                let err = record(
                    &FeedUid::new("orders").unwrap(),
                    &e,
                    &e.payload,
                    include_metadata,
                )
                .unwrap_err();
                assert!(
                    matches!(&err, FeedSourceError::Undecodable { reason, .. } if reason.contains("reserved")),
                    "{content_type}: {err}"
                );
            }
        }
        assert_eq!(
            decode("application/json", br#"{"a": {"_feed": 1}}"#).unwrap(),
            json!({"a": {"_feed": 1}})
        );
    }

    #[test]
    fn a_payload_that_contradicts_its_type_is_a_typed_error() {
        let err = decode("application/json", b"{not json").unwrap_err();
        assert!(
            matches!(&err, FeedSourceError::Undecodable { seq: 7, content_type, .. } if content_type == "application/json"),
            "{err}"
        );
        let err = decode("text/plain", &[0xff, 0xfe]).unwrap_err();
        assert!(matches!(err, FeedSourceError::Undecodable { .. }), "{err}");
    }

    #[test]
    fn metadata_is_added_under_its_key() {
        let e = event("application/json", br#"{"id": 1}"#);
        let out = record(&FeedUid::new("orders").unwrap(), &e, &e.payload, true).unwrap();
        assert_eq!(
            out,
            json!({
                "id": 1,
                "_feed": {
                    "seq": 7,
                    "id": "0193-e7",
                    "received_at": "2026-11-16T10:20:30.123456Z",
                    "headers": {"x-request-id": "r-1"}
                }
            })
        );
        let e = event("text/plain", b"hi");
        let out = record(&FeedUid::new("orders").unwrap(), &e, &e.payload, true).unwrap();
        assert_eq!(out["value"], json!("hi"));
        assert_eq!(out["_feed"]["seq"], json!(7));
    }
}
