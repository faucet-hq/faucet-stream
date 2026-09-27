//! DynamoDB Streams record → faucet CDC envelope. Pure.
//!
//! Envelope shape (compatible with the `cdc_unwrap` stage):
//!
//! ```json
//! { "op": "c" | "u" | "d" | "r", "before": {…} | null, "after": {…} | null,
//!   "key": {…}, "document_key": {…}, "table": "orders", "event_id": "…",
//!   "event_name": "INSERT", "sequence_number": "…", "shard_id": "…",
//!   "ts_ms": 1716700000000, "size_bytes": 42, "stream_view_type": "NEW_AND_OLD_IMAGES",
//!   "user_identity": null }
//! ```
//!
//! `document_key` duplicates `key` because `cdc_unwrap` falls back to it for a
//! delete when the stream carries no old image (`KEYS_ONLY` / `NEW_IMAGE`).
//! TTL expirations arrive as `op: "d"` with `user_identity.principal_id =
//! "dynamodb.amazonaws.com"`.

use aws_sdk_dynamodbstreams::types::Record;
use faucet_common_dynamodb::streams_item_to_json;
use faucet_core::FaucetError;
use serde_json::{Value, json};

/// Map a Streams `eventName` to the envelope `op`.
pub fn op_for_event(event_name: &str) -> String {
    match event_name {
        "INSERT" => "c".into(),
        "MODIFY" => "u".into(),
        "REMOVE" => "d".into(),
        other => other.to_ascii_lowercase(),
    }
}

/// Everything the envelope needs, already converted to JSON.
#[derive(Debug, Default)]
pub struct EnvelopeParts<'a> {
    /// `INSERT` / `MODIFY` / `REMOVE`.
    pub event_name: &'a str,
    /// Primary key attributes.
    pub key: Value,
    /// Old image, if the stream view carries it.
    pub before: Value,
    /// New image, if the stream view carries it.
    pub after: Value,
    /// Table name.
    pub table: &'a str,
    /// Stream event id.
    pub event_id: Option<&'a str>,
    /// Sequence number.
    pub sequence: &'a str,
    /// Shard id.
    pub shard_id: &'a str,
    /// Approximate creation time in epoch milliseconds.
    pub ts_ms: Option<i64>,
    /// Record size in bytes.
    pub size_bytes: Option<i64>,
    /// Stream view type.
    pub view_type: Option<&'a str>,
    /// `userIdentity` (TTL deletes).
    pub user_identity: Value,
}

/// Assemble the envelope object.
pub fn build_envelope(p: EnvelopeParts<'_>) -> Value {
    json!({
        "op": op_for_event(p.event_name),
        "before": p.before,
        "after": p.after,
        "key": p.key.clone(),
        "document_key": p.key,
        "table": p.table,
        "event_id": p.event_id,
        "event_name": p.event_name,
        "sequence_number": p.sequence,
        "shard_id": p.shard_id,
        "ts_ms": p.ts_ms,
        "size_bytes": p.size_bytes,
        "stream_view_type": p.view_type,
        "user_identity": p.user_identity,
    })
}

/// Snapshot-read envelope (`op: "r"`) for an item read by a resnapshot scan.
pub fn snapshot_envelope(item: Value, key: Value, table: &str) -> Value {
    json!({
        "op": "r",
        "before": null,
        "after": item,
        "key": key.clone(),
        "document_key": key,
        "table": table,
        "event_id": null,
        "event_name": "SNAPSHOT",
        "sequence_number": null,
        "shard_id": null,
        "ts_ms": null,
        "size_bytes": null,
        "stream_view_type": null,
        "user_identity": null,
    })
}

/// Convert one Streams record to `(sequence_number, envelope)`.
pub fn record_to_envelope(
    record: &Record,
    shard_id: &str,
    table: &str,
) -> Result<(String, Value), FaucetError> {
    let sr = record.dynamodb().ok_or_else(|| {
        FaucetError::Source(format!(
            "dynamodb streams: record {} in {shard_id} has no stream record",
            record.event_id().unwrap_or("?")
        ))
    })?;
    let sequence = sr.sequence_number().ok_or_else(|| {
        FaucetError::Source(format!(
            "dynamodb streams: record {} in {shard_id} has no sequence number",
            record.event_id().unwrap_or("?")
        ))
    })?;
    let image = |m: Option<&std::collections::HashMap<String, _>>| -> Result<Value, FaucetError> {
        m.map(streams_item_to_json)
            .transpose()
            .map(|v| v.unwrap_or(Value::Null))
    };
    let event_name = record.event_name().map(|e| e.as_str()).unwrap_or("UNKNOWN");
    let user_identity = record
        .user_identity()
        .map(|u| json!({"principal_id": u.principal_id(), "type": u.r#type()}))
        .unwrap_or(Value::Null);
    let envelope = build_envelope(EnvelopeParts {
        event_name,
        key: image(sr.keys.as_ref())?,
        before: image(sr.old_image.as_ref())?,
        after: image(sr.new_image.as_ref())?,
        table,
        event_id: record.event_id(),
        sequence,
        shard_id,
        ts_ms: sr
            .approximate_creation_date_time()
            .and_then(|t| t.to_millis().ok()),
        size_bytes: sr.size_bytes(),
        view_type: sr.stream_view_type().map(|v| v.as_str()),
        user_identity,
    });
    Ok((sequence.to_string(), envelope))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodbstreams::primitives::DateTime;
    use aws_sdk_dynamodbstreams::types::{
        AttributeValue, Identity, OperationType, StreamRecord, StreamViewType,
    };
    use std::collections::HashMap;

    fn attrs(pairs: &[(&str, &str)]) -> HashMap<String, AttributeValue> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), AttributeValue::S(v.to_string())))
            .collect()
    }

    fn record(op: OperationType, old: bool, new: bool) -> Record {
        let mut sr = StreamRecord::builder()
            .set_keys(Some(attrs(&[("pk", "a")])))
            .sequence_number("100")
            .size_bytes(42)
            .stream_view_type(StreamViewType::NewAndOldImages)
            .approximate_creation_date_time(DateTime::from_secs(1_716_700_000));
        if old {
            sr = sr.set_old_image(Some(attrs(&[("pk", "a"), ("v", "old")])));
        }
        if new {
            sr = sr.set_new_image(Some(attrs(&[("pk", "a"), ("v", "new")])));
        }
        Record::builder()
            .event_id("e1")
            .event_name(op)
            .dynamodb(sr.build())
            .build()
    }

    #[test]
    fn op_mapping() {
        assert_eq!(op_for_event("INSERT"), "c");
        assert_eq!(op_for_event("MODIFY"), "u");
        assert_eq!(op_for_event("REMOVE"), "d");
        assert_eq!(op_for_event("FUTURE"), "future");
    }

    #[test]
    fn insert_modify_remove_envelopes() {
        let (seq, env) =
            record_to_envelope(&record(OperationType::Insert, false, true), "s1", "t").unwrap();
        assert_eq!(seq, "100");
        assert_eq!(env["op"], "c");
        assert_eq!(env["before"], Value::Null);
        assert_eq!(env["after"]["v"], "new");
        assert_eq!(env["key"], json!({"pk": "a"}));
        assert_eq!(env["document_key"], json!({"pk": "a"}));
        assert_eq!(env["ts_ms"], 1_716_700_000_000i64);
        assert_eq!(env["size_bytes"], 42);
        assert_eq!(env["stream_view_type"], "NEW_AND_OLD_IMAGES");
        assert_eq!(env["shard_id"], "s1");
        assert_eq!(env["table"], "t");
        assert_eq!(env["event_id"], "e1");
        assert_eq!(env["user_identity"], Value::Null);

        let (_, env) =
            record_to_envelope(&record(OperationType::Modify, true, true), "s1", "t").unwrap();
        assert_eq!(env["op"], "u");
        assert_eq!(env["before"]["v"], "old");

        let mut rec = record(OperationType::Remove, true, false);
        rec.user_identity = Some(
            Identity::builder()
                .principal_id("dynamodb.amazonaws.com")
                .r#type("Service")
                .build(),
        );
        let (_, env) = record_to_envelope(&rec, "s1", "t").unwrap();
        assert_eq!(env["op"], "d");
        assert_eq!(env["after"], Value::Null);
        assert_eq!(
            env["user_identity"]["principal_id"],
            "dynamodb.amazonaws.com"
        );
    }

    #[test]
    fn malformed_records_error() {
        let no_sr = Record::builder().event_id("x").build();
        assert!(
            record_to_envelope(&no_sr, "s", "t")
                .unwrap_err()
                .to_string()
                .contains("no stream record")
        );
        let no_seq = Record::builder()
            .dynamodb(StreamRecord::builder().build())
            .build();
        assert!(
            record_to_envelope(&no_seq, "s", "t")
                .unwrap_err()
                .to_string()
                .contains("no sequence number")
        );
        let no_name = Record::builder()
            .dynamodb(StreamRecord::builder().sequence_number("1").build())
            .build();
        let (_, env) = record_to_envelope(&no_name, "s", "t").unwrap();
        assert_eq!(env["event_name"], "UNKNOWN");
        assert_eq!(env["key"], Value::Null);
    }

    #[test]
    fn snapshot_envelope_shape() {
        let env = snapshot_envelope(json!({"pk": "a", "v": 1}), json!({"pk": "a"}), "t");
        assert_eq!(env["op"], "r");
        assert_eq!(env["after"]["v"], 1);
        assert_eq!(env["document_key"], json!({"pk": "a"}));
        assert_eq!(env["event_name"], "SNAPSHOT");
    }
}
