//! Avro encode/decode through the Confluent wire envelope against a fake
//! Schema Registry (wiremock).
#![cfg(feature = "schema-registry")]

use faucet_common_kafka::SchemaRegistryConfig;
use faucet_common_kafka::schema_registry::{avro, client::SchemaRegistryClient};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const SCHEMA: &str = r#"{"type":"record","name":"Order","fields":[
    {"name":"id","type":"long"},
    {"name":"note","type":["null","string"],"default":null}
]}"#;

async fn registry() -> (MockServer, SchemaRegistryClient) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/subjects/orders-value/versions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": 3})))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/schemas/ids/3"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"schema": SCHEMA})))
        .mount(&server)
        .await;
    let client = SchemaRegistryClient::new(&SchemaRegistryConfig::new(server.uri())).unwrap();
    (server, client)
}

#[tokio::test]
async fn a_record_round_trips_through_the_registry() {
    let (_server, client) = registry().await;
    let record = json!({"id": 42, "note": "rush"});
    let bytes = avro::encode(&client, "orders-value", SCHEMA, &record)
        .await
        .unwrap();
    assert_eq!(bytes[0], 0, "Confluent magic byte");
    assert_eq!(&bytes[1..5], &3u32.to_be_bytes(), "registered schema id");
    let decoded = avro::decode(&client, &bytes).await.unwrap();
    assert_eq!(decoded["id"], json!(42));
    assert_eq!(decoded["note"], json!("rush"));
}

#[tokio::test]
async fn a_record_that_does_not_match_the_schema_is_refused() {
    let (_server, client) = registry().await;
    let err = avro::encode(
        &client,
        "orders-value",
        SCHEMA,
        &json!({"id": "not-a-number"}),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("avro"), "{err}");
}

#[tokio::test]
async fn a_corrupt_body_fails_to_decode() {
    let (_server, client) = registry().await;
    let mut bytes = vec![0u8];
    bytes.extend_from_slice(&3u32.to_be_bytes());
    bytes.push(0xff);
    let err = avro::decode(&client, &bytes).await.unwrap_err();
    assert!(err.to_string().contains("avro decode"), "{err}");
}
