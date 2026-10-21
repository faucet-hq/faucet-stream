//! JSON Schema decode through the Confluent wire envelope against a fake
//! Schema Registry (wiremock): numbers the payload spells exactly stay exact.
#![cfg(feature = "schema-registry")]

use faucet_common_kafka::SchemaRegistryConfig;
use faucet_common_kafka::schema_registry::{client::SchemaRegistryClient, envelope, json_schema};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const SCHEMA: &str = r#"{"type":"object","properties":{"id":{"type":"integer"},"amount":{"type":"number"},"small":{"type":"integer"}},"required":["id"]}"#;

async fn registry() -> (MockServer, SchemaRegistryClient) {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/schemas/ids/7"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"schema": SCHEMA})))
        .mount(&server)
        .await;
    let client = SchemaRegistryClient::new(&SchemaRegistryConfig::new(server.uri())).unwrap();
    (server, client)
}

#[tokio::test]
async fn big_integers_and_long_decimals_decode_exactly() {
    let (_server, client) = registry().await;
    let payload = br#"{"id":12345678901234567890123,"amount":1234567890.123456789012345,"ratio":0.1,"small":42}"#;
    let bytes = envelope::encode(7, payload);
    let decoded = json_schema::decode(&client, &bytes, true).await.unwrap();
    assert_eq!(decoded["id"], json!("12345678901234567890123"));
    assert_eq!(decoded["amount"], json!("1234567890.123456789012345"));
    assert_eq!(decoded["ratio"], json!(0.1));
    assert_eq!(decoded["small"], json!(42));
}

#[tokio::test]
async fn a_payload_failing_validation_is_refused_and_bad_json_fails_to_decode() {
    let (_server, client) = registry().await;
    let err = json_schema::decode(&client, &envelope::encode(7, br#"{"other":1}"#), true)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("validation failed"), "{err}");
    let err = json_schema::decode(&client, &envelope::encode(7, b"{not json"), false)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("json-schema decode"), "{err}");
}
