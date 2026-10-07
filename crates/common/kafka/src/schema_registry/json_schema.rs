//! Confluent JSON Schema: payload is plain JSON, wrapped in the Confluent
//! wire envelope and optionally validated against the registered schema.

use crate::schema_registry::{client::SchemaRegistryClient, envelope};
use faucet_core::FaucetError;
use serde_json::Value;

/// Decode `bytes` into a `serde_json::Value`. If `validate` is true, the
/// decoded JSON is validated against the writer schema from the registry.
pub async fn decode(
    client: &SchemaRegistryClient,
    bytes: &[u8],
    validate: bool,
) -> Result<Value, FaucetError> {
    let (schema_id, body) = envelope::decode(bytes)?;
    let json: Value = serde_json::from_slice(body)
        .map_err(|e| FaucetError::Source(format!("json-schema decode: {e}")))?;
    if validate {
        let registered = client.get_schema(schema_id).await?;
        let validator = client.parsed("json-schema", &registered.schema, |text| {
            let schema_value: Value = serde_json::from_str(text)
                .map_err(|e| FaucetError::Source(format!("json-schema parse: {e}")))?;
            jsonschema::validator_for(&schema_value)
                .map_err(|e| FaucetError::Source(format!("json-schema compile: {e}")))
        })?;
        let messages: Vec<String> = validator
            .iter_errors(&json)
            .map(|e| e.to_string())
            .collect();
        if !messages.is_empty() {
            return Err(FaucetError::Source(format!(
                "json-schema validation failed: {}",
                messages.join("; ")
            )));
        }
    }
    Ok(json)
}

/// Encode `value` as JSON under `subject`, registering the schema first.
pub async fn encode(
    client: &SchemaRegistryClient,
    subject: &str,
    schema_text: &str,
    value: &Value,
) -> Result<Vec<u8>, FaucetError> {
    let id = client.register_schema(subject, "JSON", schema_text).await?;
    let payload =
        serde_json::to_vec(value).map_err(|e| FaucetError::Source(format!("json encode: {e}")))?;
    Ok(envelope::encode(id, &payload))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SchemaRegistryConfig;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn json_schema_round_trip_without_validation() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/subjects/orders-value/versions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"id": 7})))
            .mount(&server)
            .await;
        let client = SchemaRegistryClient::new(&SchemaRegistryConfig::new(server.uri())).unwrap();
        let value = serde_json::json!({"order_id": "abc", "qty": 3});
        let bytes = encode(&client, "orders-value", r#"{"type":"object"}"#, &value)
            .await
            .unwrap();
        let decoded = decode(&client, &bytes, false).await.unwrap();
        assert_eq!(decoded, value);
    }

    #[tokio::test]
    async fn json_schema_validation_rejects_bad_payload() {
        let server = MockServer::start().await;
        let schema_text =
            r#"{"type":"object","required":["qty"],"properties":{"qty":{"type":"integer"}}}"#;
        Mock::given(method("POST"))
            .and(path("/subjects/orders-value/versions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"id": 9})))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/9"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "schema": schema_text,
                "schemaType": "JSON",
            })))
            .mount(&server)
            .await;
        let client = SchemaRegistryClient::new(&SchemaRegistryConfig::new(server.uri())).unwrap();
        let value = serde_json::json!({"name": "no qty here"});
        let bytes = encode(&client, "orders-value", schema_text, &value)
            .await
            .unwrap();
        let err = decode(&client, &bytes, true).await.unwrap_err();
        assert!(format!("{err}").contains("validation failed"));
    }

    #[tokio::test]
    async fn json_schema_validation_accepts_valid_payload() {
        let server = MockServer::start().await;
        let schema_text =
            r#"{"type":"object","required":["qty"],"properties":{"qty":{"type":"integer"}}}"#;
        Mock::given(method("POST"))
            .and(path("/subjects/orders-value/versions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"id": 9})))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/9"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "schema": schema_text,
                "schemaType": "JSON",
            })))
            .mount(&server)
            .await;
        let client = SchemaRegistryClient::new(&SchemaRegistryConfig::new(server.uri())).unwrap();
        let value = serde_json::json!({"qty": 5});
        let bytes = encode(&client, "orders-value", schema_text, &value)
            .await
            .unwrap();
        let decoded = decode(&client, &bytes, true).await.unwrap();
        assert_eq!(decoded["qty"], 5);
    }
}
