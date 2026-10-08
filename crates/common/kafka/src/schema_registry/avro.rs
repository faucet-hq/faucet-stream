//! Avro encode/decode wrapped in the Confluent wire envelope.

use crate::schema_registry::{client::SchemaRegistryClient, envelope};
use apache_avro::reader::datum::GenericDatumReader;
use apache_avro::writer::datum::GenericDatumWriter;
use apache_avro::{Schema, types::Value as AvroValue};
use faucet_core::FaucetError;
use faucet_core::file_format::avro::{datum_from_json, datum_to_json};
use serde_json::Value;

fn parse(client: &SchemaRegistryClient, text: &str) -> Result<std::sync::Arc<Schema>, FaucetError> {
    client.parsed("avro", text, |t| {
        Schema::parse_str(t).map_err(|e| FaucetError::Config(format!("avro schema parse: {e}")))
    })
}

/// Decode `bytes` (with Confluent envelope) into a JSON value, using the
/// writer schema fetched from the registry by ID.
pub async fn decode(client: &SchemaRegistryClient, bytes: &[u8]) -> Result<Value, FaucetError> {
    let (schema_id, body) = envelope::decode(bytes)?;
    let registered = client.get_schema(schema_id).await?;
    let schema = parse(client, &registered.schema)?;
    let mut cursor = std::io::Cursor::new(body);
    let avro_value = GenericDatumReader::builder(&schema)
        .build()
        .and_then(|r| r.read_value(&mut cursor))
        .map_err(|e| FaucetError::Source(format!("avro decode: {e}")))?;
    avro_to_json(&avro_value, &schema)
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
    let schema = parse(client, schema_text)?;
    let id = client.register_schema(subject, "AVRO", schema_text).await?;
    let avro_value = json_to_avro(value, &schema)?;
    let payload = GenericDatumWriter::builder(&schema)
        .build()
        .and_then(|w| w.write_value_to_vec(avro_value))
        .map_err(|e| FaucetError::Sink(format!("avro encode: {e}")))?;
    Ok(envelope::encode(id, &payload))
}

/// Convert an `AvroValue` to JSON against its writer schema, with the same
/// logical-type mapping as the Avro file format (#789 MSG-27): `decimal` as an
/// exact decimal string, `bytes`/`fixed` as hex, `date`/`time`/`timestamp` as
/// ISO 8601 strings. apache-avro's generic conversion turned these into int
/// arrays and day numbers.
fn avro_to_json(v: &AvroValue, schema: &Schema) -> Result<Value, FaucetError> {
    datum_to_json(v, schema)
}

/// Convert a `serde_json::Value` to an `AvroValue` against the writer schema,
/// accepting the shapes [`avro_to_json`] produces, so a decoded record encodes
/// back unchanged.
fn json_to_avro(v: &Value, schema: &Schema) -> Result<AvroValue, FaucetError> {
    datum_from_json(v, schema).map_err(|e| FaucetError::Sink(format!("json->avro: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SchemaRegistryConfig;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn json_without_an_avro_value_is_a_sink_error() {
        let err = json_to_avro(&serde_json::json!(u64::MAX), &Schema::Long).expect_err("u64");
        assert!(err.to_string().contains("json->avro"), "{err}");
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
        let record = serde_json::json!({"amount": "12.34", "raw": "00ff", "tag": "abcd", "day": "2026-10-07"});
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
