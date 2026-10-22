//! Avro encode/decode wrapped in the Confluent wire envelope.

use crate::schema_registry::{client::SchemaRegistryClient, envelope};
use apache_avro::reader::datum::GenericDatumReader;
use apache_avro::writer::datum::GenericDatumWriter;
use apache_avro::{Schema, types::Value as AvroValue};
use faucet_core::FaucetError;
use faucet_core::file_format::avro::{AvroDatumCodec, BytesText};
use serde_json::Value;

/// The parsed schema with its named types indexed, built once per schema text.
fn parse(
    client: &SchemaRegistryClient,
    text: &str,
) -> Result<std::sync::Arc<AvroDatumCodec>, FaucetError> {
    client.parsed("avro", text, |t| {
        Schema::parse_str(t)
            .map(AvroDatumCodec::new)
            .map_err(|e| FaucetError::Config(format!("avro schema parse: {e}")))
    })
}

/// Decode `bytes` (with Confluent envelope) into a JSON value, using the
/// writer schema fetched from the registry by ID.
pub async fn decode(client: &SchemaRegistryClient, bytes: &[u8]) -> Result<Value, FaucetError> {
    let (schema_id, body) = envelope::decode(bytes)?;
    let registered = client.get_schema(schema_id).await?;
    registered.expect_type("AVRO")?;
    let codec = if registered.references.is_empty() {
        parse(client, &registered.schema)?
    } else {
        let deps = client.references(&registered).await?;
        let texts: Vec<&str> = deps.iter().map(|(_, s)| s.schema.as_str()).collect();
        parse(client, &inline_references(&registered.schema, &texts)?)?
    };
    let mut cursor = std::io::Cursor::new(body);
    let avro_value = GenericDatumReader::builder(codec.schema())
        .build()
        .and_then(|r| r.read_value(&mut cursor))
        .map_err(|e| FaucetError::Source(format!("avro decode: {e}")))?;
    avro_to_json(&avro_value, &codec)
}

/// `schema` with every named type it uses from `references` defined inline at
/// its first use, so it parses on its own.
fn inline_references(schema: &str, references: &[&str]) -> Result<String, FaucetError> {
    use serde_json::Map;
    fn parse_json(text: &str) -> Result<Value, FaucetError> {
        serde_json::from_str(text)
            .map_err(|e| FaucetError::Source(format!("avro schema parse: {e}")))
    }
    fn fullname(obj: &Map<String, Value>, enclosing: Option<&str>) -> Option<String> {
        let name = obj.get("name")?.as_str()?;
        if name.contains('.') {
            return Some(name.to_string());
        }
        match obj.get("namespace").and_then(Value::as_str).or(enclosing) {
            Some(ns) if !ns.is_empty() => Some(format!("{ns}.{name}")),
            _ => Some(name.to_string()),
        }
    }
    fn namespace_of(full: &str) -> Option<String> {
        full.rsplit_once('.').map(|(ns, _)| ns.to_string())
    }
    struct Inliner {
        named: std::collections::HashMap<String, Value>,
        defined: std::collections::HashSet<String>,
    }
    impl Inliner {
        fn walk(&mut self, v: &Value, ns: Option<&str>) -> Value {
            match v {
                Value::String(s) => {
                    let full = match ns {
                        Some(ns) if !s.contains('.') && !ns.is_empty() => format!("{ns}.{s}"),
                        _ => s.clone(),
                    };
                    for key in [&full, s] {
                        if !self.defined.contains(key)
                            && let Some(def) = self.named.get(key).cloned()
                        {
                            self.defined.insert(key.clone());
                            let inner_ns = namespace_of(key);
                            return self.walk(&def, inner_ns.as_deref());
                        }
                    }
                    v.clone()
                }
                Value::Array(items) => {
                    Value::Array(items.iter().map(|i| self.walk(i, ns)).collect())
                }
                Value::Object(obj) => {
                    let full = fullname(obj, ns);
                    if let Some(full) = &full
                        && obj.contains_key("type")
                    {
                        self.defined.insert(full.clone());
                    }
                    let inner_ns = full.as_deref().and_then(namespace_of);
                    let inner = inner_ns.as_deref().or(ns);
                    let mut out = Map::new();
                    for (k, val) in obj {
                        let walked = match k.as_str() {
                            "type" | "items" | "values" => self.walk(val, inner),
                            "fields" => match val {
                                Value::Array(fields) => Value::Array(
                                    fields
                                        .iter()
                                        .map(|f| match f {
                                            Value::Object(fo) => {
                                                let mut fo = fo.clone();
                                                if let Some(t) = fo.get("type").cloned() {
                                                    fo.insert("type".into(), self.walk(&t, inner));
                                                }
                                                Value::Object(fo)
                                            }
                                            other => other.clone(),
                                        })
                                        .collect(),
                                ),
                                other => other.clone(),
                            },
                            _ => val.clone(),
                        };
                        out.insert(k.clone(), walked);
                    }
                    Value::Object(out)
                }
                other => other.clone(),
            }
        }
    }
    let mut named = std::collections::HashMap::new();
    for text in references {
        let def = parse_json(text)?;
        if let Value::Object(obj) = &def
            && let Some(full) = fullname(obj, None)
        {
            named.insert(full, def);
        }
    }
    let mut inliner = Inliner {
        named,
        defined: Default::default(),
    };
    let main = parse_json(schema)?;
    Ok(inliner.walk(&main, None).to_string())
}

/// Encode a JSON value as Avro under the named subject, registering or
/// reusing the schema. Returns the wire envelope bytes.
///
/// `subject` is typically `{topic}-value` (TopicNameStrategy). `schema_text`
/// is the writer schema as JSON; on first use it is registered with the
/// registry and the returned ID is cached for subsequent calls.
pub async fn encode(
    client: &SchemaRegistryClient,
    subject: &str,
    schema_text: &str,
    value: &Value,
) -> Result<Vec<u8>, FaucetError> {
    encode_with(client, subject, schema_text, value, BytesText::default()).await
}

/// [`encode`] with an explicit reading of JSON strings for `bytes` / `fixed`
/// fields.
pub async fn encode_with(
    client: &SchemaRegistryClient,
    subject: &str,
    schema_text: &str,
    value: &Value,
    bytes: BytesText,
) -> Result<Vec<u8>, FaucetError> {
    let codec = parse(client, schema_text)?;
    let id = client.register_schema(subject, "AVRO", schema_text).await?;
    let avro_value = json_to_avro(value, &codec, bytes)?;
    let payload = GenericDatumWriter::builder(codec.schema())
        .build()
        .and_then(|w| w.write_value_to_vec(avro_value))
        .map_err(|e| FaucetError::Sink(format!("avro encode: {e}")))?;
    Ok(envelope::encode(id, &payload))
}

/// Convert an `AvroValue` to JSON against its writer schema, with the same
/// logical-type mapping as the Avro file format (#789 MSG-27): `decimal` as an
/// exact decimal string, `bytes`/`fixed` as base64, `date`/`time`/`timestamp` as
/// ISO 8601 strings. apache-avro's generic conversion turned these into int
/// arrays and day numbers.
fn avro_to_json(v: &AvroValue, codec: &AvroDatumCodec) -> Result<Value, FaucetError> {
    codec.to_json(v)
}

/// Convert a `serde_json::Value` to an `AvroValue` against the writer schema,
/// accepting the shapes [`avro_to_json`] produces, so a decoded record encodes
/// back unchanged.
fn json_to_avro(
    v: &Value,
    codec: &AvroDatumCodec,
    bytes: BytesText,
) -> Result<AvroValue, FaucetError> {
    codec
        .from_json(v, bytes)
        .map_err(|e| FaucetError::Sink(format!("json->avro: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SchemaRegistryConfig;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn json_without_an_avro_value_is_a_sink_error() {
        let err = json_to_avro(
            &serde_json::json!(u64::MAX),
            &AvroDatumCodec::new(Schema::Long),
            BytesText::Hex,
        )
        .expect_err("u64");
        assert!(err.to_string().contains("json->avro"), "{err}");
    }

    #[test]
    fn references_are_inlined_at_first_use() {
        let address = r#"{"type":"record","name":"Address","namespace":"com.acme",
            "fields":[{"name":"city","type":"string"},{"name":"zip","type":"Zip"}]}"#;
        let zip = r#"{"type":"fixed","name":"Zip","namespace":"com.acme","size":2}"#;
        let main = r#"{"type":"record","name":"User","namespace":"com.acme","fields":[
            {"name":"home","type":"com.acme.Address"},
            {"name":"work","type":["null","Address"]}]}"#;
        let text = inline_references(main, &[zip, address]).unwrap();
        let schema = Schema::parse_str(&text).unwrap();
        let codec = AvroDatumCodec::new(schema);
        let datum = codec
            .from_json(
                &serde_json::json!({"home": {"city": "x", "zip": "AQI="}, "work": null}),
                BytesText::Base64,
            )
            .unwrap();
        assert_eq!(
            codec.to_json(&datum).unwrap(),
            serde_json::json!({"home": {"city": "x", "zip": "AQI="}, "work": null})
        );
        assert!(inline_references("{", &[]).is_err());
    }

    #[tokio::test]
    async fn a_writer_schema_with_references_decodes() {
        let server = MockServer::start().await;
        let address = r#"{"type":"record","name":"Address","namespace":"com.acme","fields":[{"name":"city","type":"string"}]}"#;
        let main = r#"{"type":"record","name":"User","namespace":"com.acme","fields":[{"name":"home","type":"com.acme.Address"}]}"#;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/5"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "schema": main,
                "references": [{"name": "com.acme.Address", "subject": "address", "version": 2}]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/subjects/address/versions/2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "subject": "address", "version": 2, "id": 4, "schema": address
            })))
            .expect(1)
            .mount(&server)
            .await;
        let client = SchemaRegistryClient::new(&SchemaRegistryConfig::new(server.uri())).unwrap();
        let (writer, _) = Schema::parse_str_with_list(main, [address]).unwrap();
        let codec = AvroDatumCodec::new(
            Schema::parse_str(&inline_references(main, &[address]).unwrap()).unwrap(),
        );
        let value = serde_json::json!({"home": {"city": "Lyon"}});
        let datum = codec.from_json(&value, BytesText::Base64).unwrap();
        drop(writer);
        let payload = GenericDatumWriter::builder(codec.schema())
            .build()
            .and_then(|w| w.write_value_to_vec(datum))
            .unwrap();
        let bytes = envelope::encode(5, &payload);
        assert_eq!(decode(&client, &bytes).await.unwrap(), value);
        assert_eq!(decode(&client, &bytes).await.unwrap(), value);
    }

    #[tokio::test]
    async fn a_schema_of_another_type_is_refused() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/6"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "schema": "syntax = \"proto3\";", "schemaType": "PROTOBUF"
            })))
            .mount(&server)
            .await;
        let client = SchemaRegistryClient::new(&SchemaRegistryConfig::new(server.uri())).unwrap();
        let err = decode(&client, &envelope::encode(6, &[0]))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("registered as PROTOBUF"), "{err}");
    }

    #[tokio::test]
    async fn logical_types_decode_like_the_avro_file_format_and_round_trip() {
        let server = MockServer::start().await;
        let schema_text = r#"{"type":"record","name":"Pay","fields":[
            {"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}},
            {"name":"raw","type":"bytes"},
            {"name":"tag","type":{"type":"fixed","name":"tag_t","size":2}},
            {"name":"day","type":{"type":"int","logicalType":"date"}}
        ]}"#;
        Mock::given(method("POST"))
            .and(path("/subjects/pay-value/versions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"id": 3})))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/3"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "schema": schema_text,
                "schemaType": "AVRO",
            })))
            .mount(&server)
            .await;
        let client = SchemaRegistryClient::new(&SchemaRegistryConfig::new(server.uri())).unwrap();
        let record = serde_json::json!({"amount": "12.34", "raw": "AP8=", "tag": "q80=", "day": "2026-10-07"});
        for _ in 0..2 {
            let bytes = encode(&client, "pay-value", schema_text, &record)
                .await
                .unwrap();
            assert_eq!(decode(&client, &bytes).await.unwrap(), record);
        }
        let a = parse(&client, schema_text).unwrap();
        let b = parse(&client, schema_text).unwrap();
        assert!(
            std::sync::Arc::ptr_eq(&a, &b),
            "the parsed schema is cached"
        );
    }

    #[tokio::test]
    async fn utf8_bytes_encoding_writes_a_string_as_its_bytes() {
        let server = MockServer::start().await;
        let schema_text =
            r#"{"type":"record","name":"R","fields":[{"name":"raw","type":"bytes"}]}"#;
        Mock::given(method("POST"))
            .and(path("/subjects/r-value/versions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"id": 4})))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/4"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "schema": schema_text,
                "schemaType": "AVRO",
            })))
            .mount(&server)
            .await;
        let client = SchemaRegistryClient::new(&SchemaRegistryConfig::new(server.uri())).unwrap();
        let record = serde_json::json!({"raw": "cafe"});
        let utf8 = encode_with(&client, "r-value", schema_text, &record, BytesText::Utf8)
            .await
            .unwrap();
        assert_eq!(
            decode(&client, &utf8).await.unwrap(),
            serde_json::json!({"raw": "Y2FmZQ=="})
        );
        let hex = encode_with(&client, "r-value", schema_text, &record, BytesText::Hex)
            .await
            .unwrap();
        assert_eq!(
            decode(&client, &hex).await.unwrap(),
            serde_json::json!({"raw": "yv4="})
        );
        let b64 = serde_json::json!({"raw": "yv4="});
        let bytes = encode(&client, "r-value", schema_text, &b64).await.unwrap();
        assert_eq!(decode(&client, &bytes).await.unwrap(), b64);
        let err = encode(
            &client,
            "r-value",
            schema_text,
            &serde_json::json!({"raw": "hello!"}),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("not base64"), "{err}");
    }

    #[tokio::test]
    async fn avro_round_trip_through_mock_registry() {
        let server = MockServer::start().await;
        let schema_text = r#"{"type":"record","name":"User","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}"#;

        Mock::given(method("POST"))
            .and(path("/subjects/users-value/versions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"id": 1})))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "schema": schema_text,
                "schemaType": "AVRO",
            })))
            .mount(&server)
            .await;

        let client = SchemaRegistryClient::new(&SchemaRegistryConfig::new(server.uri())).unwrap();
        let record = serde_json::json!({"id": 42, "name": "alice"});
        let bytes = encode(&client, "users-value", schema_text, &record)
            .await
            .unwrap();
        let decoded = decode(&client, &bytes).await.unwrap();
        assert_eq!(decoded["id"], 42);
        assert_eq!(decoded["name"], "alice");
    }
}
