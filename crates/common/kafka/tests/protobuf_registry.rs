//! Protobuf encode/decode through the Confluent wire envelope against a fake
//! Schema Registry (wiremock): the compiled schema is reused per schema id.
#![cfg(feature = "schema-registry")]

use faucet_common_kafka::SchemaRegistryConfig;
use faucet_common_kafka::schema_registry::{client::SchemaRegistryClient, protobuf};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const PROTO: &str =
    "syntax = \"proto3\";\npackage example;\nmessage Order { int64 id = 1; string note = 2; }\n";

#[tokio::test]
async fn a_message_round_trips_and_the_schema_is_fetched_once() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/subjects/orders-value/versions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": 9})))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/schemas/ids/9"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"schema": PROTO, "schemaType": "PROTOBUF"})),
        )
        .expect(1)
        .mount(&server)
        .await;
    let client = SchemaRegistryClient::new(&SchemaRegistryConfig::new(server.uri())).unwrap();
    let value = json!({"id": "42", "note": "rush"});
    let bytes = protobuf::encode(&client, "orders-value", PROTO, &value)
        .await
        .unwrap();
    for _ in 0..3 {
        let decoded = protobuf::decode(&client, &bytes).await.unwrap();
        assert_eq!(decoded["note"], json!("rush"));
        assert_eq!(decoded["id"], json!("42"));
    }
}
