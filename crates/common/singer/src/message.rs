//! Singer message model, line parser, and line encoders.
//!
//! Pure logic: [`parse_line`] turns one line of a tap's (or target's) stdout
//! into a [`SingerMessage`]; the `write_*` encoders append one newline-terminated
//! Singer message to a byte buffer for a target's stdin.
//!
//! Singer message reference: <https://hub.meltano.com/singer/spec/>.

use faucet_core::Value;
use serde::Serialize;

/// A parsed Singer protocol message.
///
/// v0 models the three messages the bridge acts on plus a catch-all
/// [`Other`](SingerMessage::Other) arm for messages that are logged and
/// skipped (`ACTIVATE_VERSION`, `BATCH`, …).
#[derive(Debug, Clone, PartialEq)]
pub enum SingerMessage {
    /// A data record for `stream`.
    Record { stream: String, record: Value },
    /// A JSON-Schema declaration for `stream`.
    Schema {
        stream: String,
        schema: Value,
        key_properties: Option<Vec<String>>,
        bookmark_properties: Option<Vec<String>>,
    },
    /// A resume-checkpoint blob. `value` is opaque to the bridge — it is passed
    /// back to the tap verbatim on resume via `--state`.
    State { value: Value },
    /// Any other message type (e.g. `ACTIVATE_VERSION`, `BATCH`): recognized,
    /// logged, and skipped in v0.
    Other { message_type: String },
}

/// Parse one line of tap stdout into a [`SingerMessage`].
///
/// Returns `Err(reason)` for a blank line, non-JSON, a non-object payload, a
/// missing/`non-string `type`, or a well-typed message missing a required field
/// (e.g. a RECORD with no `record`). The caller decides whether that aborts the
/// run or is skipped, per the malformed policy.
pub fn parse_line(line: &str) -> Result<SingerMessage, String> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Err("blank line".to_string());
    }
    let value: Value = serde_json::from_str(trimmed).map_err(|e| format!("not valid JSON: {e}"))?;
    let obj = value
        .as_object()
        .ok_or_else(|| "message is not a JSON object".to_string())?;
    let msg_type = obj
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| "missing or non-string `type` field".to_string())?;

    match msg_type {
        "RECORD" => {
            let stream = obj
                .get("stream")
                .and_then(Value::as_str)
                .ok_or_else(|| "RECORD missing `stream`".to_string())?
                .to_string();
            let record = obj
                .get("record")
                .ok_or_else(|| "RECORD missing `record`".to_string())?
                .clone();
            if !record.is_object() {
                return Err("RECORD `record` is not an object".to_string());
            }
            Ok(SingerMessage::Record { stream, record })
        }
        "SCHEMA" => {
            let stream = obj
                .get("stream")
                .and_then(Value::as_str)
                .ok_or_else(|| "SCHEMA missing `stream`".to_string())?
                .to_string();
            let schema = obj
                .get("schema")
                .ok_or_else(|| "SCHEMA missing `schema`".to_string())?
                .clone();
            Ok(SingerMessage::Schema {
                stream,
                schema,
                key_properties: string_array(obj.get("key_properties")),
                bookmark_properties: string_array(obj.get("bookmark_properties")),
            })
        }
        "STATE" => {
            let value = obj
                .get("value")
                .ok_or_else(|| "STATE missing `value`".to_string())?
                .clone();
            Ok(SingerMessage::State { value })
        }
        other => Ok(SingerMessage::Other {
            message_type: other.to_string(),
        }),
    }
}

#[derive(Serialize)]
struct SchemaLine<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    stream: &'a str,
    schema: &'a Value,
    key_properties: &'a [String],
}

#[derive(Serialize)]
struct RecordLine<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    stream: &'a str,
    record: &'a Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<i64>,
}

#[derive(Serialize)]
struct StateLine<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    value: &'a Value,
}

#[derive(Serialize)]
struct ActivateVersionLine<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    stream: &'a str,
    version: i64,
}

fn push_line<T: Serialize>(buf: &mut Vec<u8>, line: &T) {
    serde_json::to_writer(&mut *buf, line).expect("Singer message serialization is infallible");
    buf.push(b'\n');
}

/// Append a `SCHEMA` message for `stream` to `buf`.
pub fn write_schema(buf: &mut Vec<u8>, stream: &str, schema: &Value, key_properties: &[String]) {
    push_line(
        buf,
        &SchemaLine {
            kind: "SCHEMA",
            stream,
            schema,
            key_properties,
        },
    );
}

/// Append a `RECORD` message to `buf`. `version` tags the record with a table
/// version for `ACTIVATE_VERSION`-driven full refreshes.
pub fn write_record(buf: &mut Vec<u8>, stream: &str, record: &Value, version: Option<i64>) {
    push_line(
        buf,
        &RecordLine {
            kind: "RECORD",
            stream,
            record,
            version,
        },
    );
}

/// Append a `STATE` message carrying `value` to `buf`.
pub fn write_state(buf: &mut Vec<u8>, value: &Value) {
    push_line(
        buf,
        &StateLine {
            kind: "STATE",
            value,
        },
    );
}

/// Append an `ACTIVATE_VERSION` message to `buf`: the target makes `version`
/// the live version of `stream` and discards rows from older versions.
pub fn write_activate_version(buf: &mut Vec<u8>, stream: &str, version: i64) {
    push_line(
        buf,
        &ActivateVersionLine {
            kind: "ACTIVATE_VERSION",
            stream,
            version,
        },
    );
}

/// Extract an optional `["a","b"]` string array from a JSON field.
fn string_array(v: Option<&Value>) -> Option<Vec<String>> {
    v.and_then(Value::as_array).map(|arr| {
        arr.iter()
            .filter_map(|x| x.as_str().map(str::to_string))
            .collect()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_record() {
        let m = parse_line(r#"{"type":"RECORD","stream":"s","record":{"id":1}}"#).unwrap();
        assert_eq!(
            m,
            SingerMessage::Record {
                stream: "s".into(),
                record: json!({"id": 1})
            }
        );
    }

    #[test]
    fn parses_schema_with_key_props() {
        let m = parse_line(
            r#"{"type":"SCHEMA","stream":"s","schema":{"type":"object"},"key_properties":["id"],"bookmark_properties":["updated_at"]}"#,
        )
        .unwrap();
        match m {
            SingerMessage::Schema {
                stream,
                key_properties,
                bookmark_properties,
                ..
            } => {
                assert_eq!(stream, "s");
                assert_eq!(key_properties, Some(vec!["id".to_string()]));
                assert_eq!(bookmark_properties, Some(vec!["updated_at".to_string()]));
            }
            _ => panic!("expected SCHEMA"),
        }
    }

    #[test]
    fn parses_state() {
        let m = parse_line(r#"{"type":"STATE","value":{"bookmarks":{"s":{"v":5}}}}"#).unwrap();
        assert_eq!(
            m,
            SingerMessage::State {
                value: json!({"bookmarks":{"s":{"v":5}}})
            }
        );
    }

    #[test]
    fn activate_version_and_batch_are_other() {
        assert_eq!(
            parse_line(r#"{"type":"ACTIVATE_VERSION","stream":"s","version":1}"#).unwrap(),
            SingerMessage::Other {
                message_type: "ACTIVATE_VERSION".into()
            }
        );
        assert_eq!(
            parse_line(r#"{"type":"BATCH","stream":"s"}"#).unwrap(),
            SingerMessage::Other {
                message_type: "BATCH".into()
            }
        );
    }

    #[test]
    fn malformed_variants_error() {
        assert!(parse_line("").is_err()); // blank
        assert!(parse_line("not json").is_err()); // non-json
        assert!(parse_line("[1,2,3]").is_err()); // not an object
        assert!(parse_line(r#"{"stream":"s"}"#).is_err()); // no type
        assert!(parse_line(r#"{"type":"RECORD","stream":"s"}"#).is_err()); // no record
        assert!(parse_line(r#"{"type":"RECORD","record":{"id":1}}"#).is_err()); // no stream
        assert!(parse_line(r#"{"type":"RECORD","stream":"s","record":5}"#).is_err()); // record not obj
        assert!(parse_line(r#"{"type":"STATE"}"#).is_err()); // no value
        assert!(parse_line(r#"{"type":"SCHEMA","stream":"s"}"#).is_err()); // no schema
    }
    #[test]
    fn encoders_round_trip_through_the_parser() {
        let mut buf = Vec::new();
        write_schema(
            &mut buf,
            "s",
            &json!({"type": "object"}),
            &["id".to_string()],
        );
        write_record(&mut buf, "s", &json!({"id": 1}), None);
        write_record(&mut buf, "s", &json!({"id": 2}), Some(7));
        write_state(&mut buf, &json!({"k": 1}));
        write_activate_version(&mut buf, "s", 7);
        let text = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 5);
        assert!(text.ends_with('\n'));
        assert_eq!(
            parse_line(lines[0]).unwrap(),
            SingerMessage::Schema {
                stream: "s".into(),
                schema: json!({"type": "object"}),
                key_properties: Some(vec!["id".into()]),
                bookmark_properties: None,
            }
        );
        assert_eq!(
            parse_line(lines[1]).unwrap(),
            SingerMessage::Record {
                stream: "s".into(),
                record: json!({"id": 1})
            }
        );
        assert!(!lines[1].contains("version"));
        let v: Value = serde_json::from_str(lines[2]).unwrap();
        assert_eq!(v["version"], json!(7));
        assert_eq!(
            parse_line(lines[3]).unwrap(),
            SingerMessage::State {
                value: json!({"k": 1})
            }
        );
        let v: Value = serde_json::from_str(lines[4]).unwrap();
        assert_eq!(
            v,
            json!({"type": "ACTIVATE_VERSION", "stream": "s", "version": 7})
        );
    }
}
