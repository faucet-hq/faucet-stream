//! Avro encode/decode wrapped in the Confluent wire envelope.

use crate::schema_registry::{client::SchemaRegistryClient, envelope};
use apache_avro::reader::datum::GenericDatumReader;
use apache_avro::writer::datum::GenericDatumWriter;
use apache_avro::{Schema, types::Value as AvroValue};
use faucet_core::FaucetError;
use serde_json::Value;

/// Decode `bytes` (with Confluent envelope) into a JSON value, using the
/// writer schema fetched from the registry by ID.
pub async fn decode(client: &SchemaRegistryClient, bytes: &[u8]) -> Result<Value, FaucetError> {
    let (schema_id, body) = envelope::decode(bytes)?;
    let registered = client.get_schema(schema_id).await?;
    let schema = Schema::parse_str(&registered.schema)
        .map_err(|e| FaucetError::Source(format!("avro schema parse: {e}")))?;
    let mut cursor = std::io::Cursor::new(body);
    let avro_value = GenericDatumReader::builder(&schema)
        .build()
        .and_then(|r| r.read_value(&mut cursor))
        .map_err(|e| FaucetError::Source(format!("avro decode: {e}")))?;
    avro_to_json(avro_value)
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
    let schema = Schema::parse_str(schema_text)
        .map_err(|e| FaucetError::Config(format!("avro schema parse: {e}")))?;
    let id = client.register_schema(subject, "AVRO", schema_text).await?;
    let avro_value = json_to_avro(value, &schema)?;
    let payload = GenericDatumWriter::builder(&schema)
        .build()
        .and_then(|w| w.write_value_to_vec(avro_value))
        .map_err(|e| FaucetError::Sink(format!("avro encode: {e}")))?;
    Ok(envelope::encode(id, &payload))
}

/// Convert an `AvroValue` to a `serde_json::Value`.
///
/// Uses apache-avro's `TryFrom<AvroValue> for serde_json::Value`.
fn avro_to_json(v: AvroValue) -> Result<Value, FaucetError> {
    v.try_into()
        .map_err(|e: apache_avro::Error| FaucetError::Source(format!("avro->json: {e}")))
}

/// Convert a `serde_json::Value` to an `AvroValue` and resolve it against
/// the writer schema.
///
/// Both steps are fallible: a JSON number outside every Avro numeric range
/// has no Avro value, and schema resolution fails when the shape does not
/// match the writer schema.
fn json_to_avro(v: &Value, schema: &Schema) -> Result<AvroValue, FaucetError> {
    let avro: AvroValue = AvroValue::try_from(v.clone())
        .map_err(|e| FaucetError::Sink(format!("json->avro: {e}")))?
        .resolve(schema)
        .map_err(|e| FaucetError::Sink(format!("json->avro resolve: {e}")))?;
    Ok(avro)
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
