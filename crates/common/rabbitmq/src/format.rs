//! Message value formats and the pure payload codecs shared by the source and
//! the sink.

use base64::Engine as _;
use faucet_core::FaucetError;
use lapin::types::{AMQPValue, FieldTable};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// How a message body maps to and from a JSON record.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RabbitMqValueFormat {
    /// The body is a JSON document. The source fails (or skips, per
    /// `on_decode_error`) a body that is not valid JSON; the sink serializes
    /// each record as JSON.
    #[default]
    Json,
    /// The body is UTF-8 text, emitted as a JSON string. The sink requires
    /// string records.
    String,
    /// The body is opaque bytes, emitted as a standard base64 JSON string. The
    /// sink requires base64 string records.
    Bytes,
}

impl RabbitMqValueFormat {
    /// The `content_type` property the sink stamps on published messages.
    pub fn content_type(self) -> &'static str {
        match self {
            RabbitMqValueFormat::Json => "application/json",
            RabbitMqValueFormat::String => "text/plain",
            RabbitMqValueFormat::Bytes => "application/octet-stream",
        }
    }
}

/// Decode a message body into a JSON value. Never panics: non-UTF-8 bodies
/// are errors under `json`/`string` and base64 under `bytes`.
pub fn decode_payload(body: &[u8], format: RabbitMqValueFormat) -> Result<Value, FaucetError> {
    match format {
        RabbitMqValueFormat::Json => serde_json::from_slice(body).map_err(|e| {
            FaucetError::Source(format!("rabbitmq: message body is not valid JSON: {e}"))
        }),
        RabbitMqValueFormat::String => std::str::from_utf8(body)
            .map(|s| Value::String(s.to_string()))
            .map_err(|e| {
                FaucetError::Source(format!("rabbitmq: message body is not valid UTF-8: {e}"))
            }),
        RabbitMqValueFormat::Bytes => Ok(Value::String(
            base64::engine::general_purpose::STANDARD.encode(body),
        )),
    }
}

/// Encode a record into a message body.
pub fn encode_payload(record: &Value, format: RabbitMqValueFormat) -> Result<Vec<u8>, FaucetError> {
    match format {
        RabbitMqValueFormat::Json => serde_json::to_vec(record)
            .map_err(|e| FaucetError::Sink(format!("rabbitmq: record serialization failed: {e}"))),
        RabbitMqValueFormat::String => match record {
            Value::String(s) => Ok(s.as_bytes().to_vec()),
            other => Err(FaucetError::Sink(format!(
                "rabbitmq: value_format 'string' requires string records (got {})",
                type_name(other)
            ))),
        },
        RabbitMqValueFormat::Bytes => match record {
            Value::String(s) => base64::engine::general_purpose::STANDARD
                .decode(s.as_bytes())
                .map_err(|e| {
                    FaucetError::Sink(format!(
                        "rabbitmq: value_format 'bytes' requires base64 string records: {e}"
                    ))
                }),
            other => Err(FaucetError::Sink(format!(
                "rabbitmq: value_format 'bytes' requires base64 string records (got {})",
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

/// Render an AMQP header table as a JSON object. Byte arrays and non-UTF-8
/// long strings become base64 strings; non-finite floats become `null`.
pub fn field_table_to_json(table: &FieldTable) -> Value {
    let mut out = Map::new();
    for (k, v) in table {
        out.insert(k.as_str().to_string(), amqp_value_to_json(v));
    }
    Value::Object(out)
}

fn amqp_value_to_json(v: &AMQPValue) -> Value {
    match v {
        AMQPValue::Boolean(b) => Value::Bool(*b),
        AMQPValue::ShortShortInt(n) => Value::from(*n),
        AMQPValue::ShortShortUInt(n) => Value::from(*n),
        AMQPValue::ShortInt(n) => Value::from(*n),
        AMQPValue::ShortUInt(n) => Value::from(*n),
        AMQPValue::LongInt(n) => Value::from(*n),
        AMQPValue::LongUInt(n) => Value::from(*n),
        AMQPValue::LongLongInt(n) => Value::from(*n),
        AMQPValue::Timestamp(n) => Value::from(*n),
        AMQPValue::Float(f) => serde_json::Number::from_f64(f64::from(*f))
            .map(Value::Number)
            .unwrap_or(Value::Null),
        AMQPValue::Double(f) => serde_json::Number::from_f64(*f)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        AMQPValue::DecimalValue(d) => {
            let scaled = f64::from(d.value) / 10f64.powi(i32::from(d.scale));
            serde_json::Number::from_f64(scaled)
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        AMQPValue::ShortString(s) => Value::String(s.as_str().to_string()),
        AMQPValue::LongString(s) => match std::str::from_utf8(s.as_bytes()) {
            Ok(text) => Value::String(text.to_string()),
            Err(_) => Value::String(base64::engine::general_purpose::STANDARD.encode(s.as_bytes())),
        },
        AMQPValue::FieldArray(arr) => {
            Value::Array(arr.as_slice().iter().map(amqp_value_to_json).collect())
        }
        AMQPValue::FieldTable(t) => field_table_to_json(t),
        AMQPValue::ByteArray(b) => {
            Value::String(base64::engine::general_purpose::STANDARD.encode(b.as_slice()))
        }
        AMQPValue::Void => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lapin::types::{ByteArray, DecimalValue, FieldArray, LongString, ShortString};
    use serde_json::json;

    #[test]
    fn default_is_json() {
        assert_eq!(RabbitMqValueFormat::default(), RabbitMqValueFormat::Json);
        let v: RabbitMqValueFormat = serde_json::from_value(json!("bytes")).unwrap();
        assert_eq!(v, RabbitMqValueFormat::Bytes);
    }

    #[test]
    fn content_types() {
        assert_eq!(RabbitMqValueFormat::Json.content_type(), "application/json");
        assert_eq!(RabbitMqValueFormat::String.content_type(), "text/plain");
        assert_eq!(
            RabbitMqValueFormat::Bytes.content_type(),
            "application/octet-stream"
        );
    }

    #[test]
    fn decode_json_string_bytes() {
        let v = decode_payload(br#"{"a":1}"#, RabbitMqValueFormat::Json).unwrap();
        assert_eq!(v, json!({"a": 1}));
        assert!(decode_payload(b"not json", RabbitMqValueFormat::Json).is_err());
        let s = decode_payload("héllo".as_bytes(), RabbitMqValueFormat::String).unwrap();
        assert_eq!(s, json!("héllo"));
        assert!(decode_payload(&[0xff, 0xfe], RabbitMqValueFormat::String).is_err());
        assert!(decode_payload(&[0xff, 0xfe], RabbitMqValueFormat::Json).is_err());
        let b = decode_payload(&[0xff, 0x00, 0x01], RabbitMqValueFormat::Bytes).unwrap();
        assert_eq!(b, json!("/wAB"));
    }

    #[test]
    fn encode_round_trips() {
        let rec = json!({"id": 7});
        let body = encode_payload(&rec, RabbitMqValueFormat::Json).unwrap();
        assert_eq!(
            decode_payload(&body, RabbitMqValueFormat::Json).unwrap(),
            rec
        );

        let body = encode_payload(&json!("text"), RabbitMqValueFormat::String).unwrap();
        assert_eq!(body, b"text");
        let err = encode_payload(&json!(1), RabbitMqValueFormat::String).unwrap_err();
        assert!(err.to_string().contains("number"));

        let body = encode_payload(&json!("/wAB"), RabbitMqValueFormat::Bytes).unwrap();
        assert_eq!(body, vec![0xff, 0x00, 0x01]);
        assert!(encode_payload(&json!("!!notb64"), RabbitMqValueFormat::Bytes).is_err());
        let err = encode_payload(&json!({"a": 1}), RabbitMqValueFormat::Bytes).unwrap_err();
        assert!(err.to_string().contains("object"));
    }

    #[test]
    fn type_names_cover_all() {
        assert_eq!(type_name(&Value::Null), "null");
        assert_eq!(type_name(&json!(true)), "bool");
        assert_eq!(type_name(&json!([1])), "array");
        assert_eq!(type_name(&json!("s")), "string");
    }

    #[test]
    fn headers_render_every_variant() {
        let mut nested = FieldTable::default();
        nested.insert(ShortString::from("inner"), AMQPValue::Boolean(false));
        let mut arr = FieldArray::default();
        arr.push(AMQPValue::LongInt(3));
        let mut t = FieldTable::default();
        t.insert("b".into(), AMQPValue::Boolean(true));
        t.insert("i8".into(), AMQPValue::ShortShortInt(-1));
        t.insert("u8".into(), AMQPValue::ShortShortUInt(1));
        t.insert("i16".into(), AMQPValue::ShortInt(-2));
        t.insert("u16".into(), AMQPValue::ShortUInt(2));
        t.insert("i32".into(), AMQPValue::LongInt(-3));
        t.insert("u32".into(), AMQPValue::LongUInt(3));
        t.insert("i64".into(), AMQPValue::LongLongInt(-4));
        t.insert("ts".into(), AMQPValue::Timestamp(1_700_000_000));
        t.insert("f32".into(), AMQPValue::Float(1.5));
        t.insert("f64".into(), AMQPValue::Double(2.5));
        t.insert("nan".into(), AMQPValue::Double(f64::NAN));
        t.insert("fnan".into(), AMQPValue::Float(f32::NAN));
        t.insert(
            "dec".into(),
            AMQPValue::DecimalValue(DecimalValue {
                scale: 2,
                value: 1234,
            }),
        );
        t.insert("ss".into(), AMQPValue::ShortString("short".into()));
        t.insert("ls".into(), AMQPValue::LongString(LongString::from("long")));
        t.insert(
            "lsbin".into(),
            AMQPValue::LongString(LongString::from(vec![0xffu8, 0xfe])),
        );
        t.insert("arr".into(), AMQPValue::FieldArray(arr));
        t.insert("tbl".into(), AMQPValue::FieldTable(nested));
        t.insert(
            "bytes".into(),
            AMQPValue::ByteArray(ByteArray::from(vec![1u8, 2, 3])),
        );
        t.insert("void".into(), AMQPValue::Void);

        let v = field_table_to_json(&t);
        assert_eq!(v["b"], json!(true));
        assert_eq!(v["i8"], json!(-1));
        assert_eq!(v["u8"], json!(1));
        assert_eq!(v["i16"], json!(-2));
        assert_eq!(v["u16"], json!(2));
        assert_eq!(v["i32"], json!(-3));
        assert_eq!(v["u32"], json!(3));
        assert_eq!(v["i64"], json!(-4));
        assert_eq!(v["ts"], json!(1_700_000_000u64));
        assert_eq!(v["f32"], json!(1.5));
        assert_eq!(v["f64"], json!(2.5));
        assert_eq!(v["nan"], Value::Null);
        assert_eq!(v["fnan"], Value::Null);
        assert_eq!(v["dec"], json!(12.34));
        assert_eq!(v["ss"], json!("short"));
        assert_eq!(v["ls"], json!("long"));
        assert_eq!(v["lsbin"], json!("//4="));
        assert_eq!(v["arr"], json!([3]));
        assert_eq!(v["tbl"], json!({"inner": false}));
        assert_eq!(v["bytes"], json!("AQID"));
        assert_eq!(v["void"], Value::Null);
    }
}
