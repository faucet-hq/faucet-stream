//! DynamoDB Local (Docker) helpers shared by the integration tests.
#![allow(dead_code)]

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType, PutRequest,
    ScalarAttributeType, StreamSpecification, StreamViewType, WriteRequest,
};
use faucet_conformance::containers::{self, Endpoint, ReadyProbe, StartOptions};
use faucet_source_dynamodb::{DynamoDbCredentials, DynamoDbSourceConfig};
use std::collections::HashMap;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::{ContainerAsync, GenericImage};

pub type Container = ContainerAsync<GenericImage>;

/// Start DynamoDB Local once it answers `ListTables`; `None` (test skipped)
/// when Docker is unavailable.
pub async fn start() -> Option<(Container, String, Client)> {
    let opts = StartOptions::default().ready(ReadyProbe::custom(8000, |ep: Endpoint| async move {
        let endpoint = format!("http://127.0.0.1:{}", ep.port);
        let client =
            faucet_source_dynamodb::build_client(Some("us-east-1"), Some(&endpoint), &creds())
                .await
                .map_err(|e| e.to_string())?;
        client
            .list_tables()
            .send()
            .await
            .map(drop)
            .map_err(|e| e.to_string())
    }));
    let container = containers::start_or_skip(
        || {
            GenericImage::new("amazon/dynamodb-local", "latest")
                .with_exposed_port(8000.tcp())
                .with_wait_for(WaitFor::message_on_stdout("CorsParams"))
        },
        &opts,
    )
    .await?;
    let port = container.get_host_port_ipv4(8000).await.ok()?;
    let endpoint = format!("http://127.0.0.1:{port}");
    let client = faucet_source_dynamodb::build_client(Some("us-east-1"), Some(&endpoint), &creds())
        .await
        .expect("client");
    Some((container, endpoint, client))
}

pub fn creds() -> DynamoDbCredentials {
    DynamoDbCredentials::AccessKey {
        access_key_id: "test".into(),
        secret_access_key: "test".into(),
        session_token: None,
    }
}

pub fn config(endpoint: &str, table: &str) -> DynamoDbSourceConfig {
    let mut c = DynamoDbSourceConfig::new(table);
    c.region = Some("us-east-1".into());
    c.endpoint_url = Some(endpoint.into());
    c.credentials = creds();
    c.poll_interval_ms = 200;
    c
}

/// Create a table (`pk` partition key, optional numeric `sk`), optionally
/// with a `NEW_AND_OLD_IMAGES` stream.
pub async fn create_table(
    client: &Client,
    name: &str,
    pk: ScalarAttributeType,
    sk: bool,
    stream: bool,
) {
    let mut b = client
        .create_table()
        .table_name(name)
        .billing_mode(BillingMode::PayPerRequest)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(pk)
                .build()
                .unwrap(),
        );
    if sk {
        b = b
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("sk")
                    .key_type(KeyType::Range)
                    .build()
                    .unwrap(),
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("sk")
                    .attribute_type(ScalarAttributeType::N)
                    .build()
                    .unwrap(),
            );
    }
    if stream {
        b = b.stream_specification(
            StreamSpecification::builder()
                .stream_enabled(true)
                .stream_view_type(StreamViewType::NewAndOldImages)
                .build()
                .unwrap(),
        );
    }
    b.send().await.expect("create table");
}

/// Batch-put items (plain attribute maps).
pub async fn put_items(client: &Client, table: &str, items: Vec<HashMap<String, AttributeValue>>) {
    for chunk in items.chunks(25) {
        let mut reqs: Vec<WriteRequest> = chunk
            .iter()
            .map(|i| {
                WriteRequest::builder()
                    .put_request(
                        PutRequest::builder()
                            .set_item(Some(i.clone()))
                            .build()
                            .unwrap(),
                    )
                    .build()
            })
            .collect();
        while !reqs.is_empty() {
            let out = client
                .batch_write_item()
                .request_items(table, reqs)
                .send()
                .await
                .expect("batch write");
            reqs = out
                .unprocessed_items()
                .and_then(|m| m.get(table).cloned())
                .unwrap_or_default();
        }
    }
}

pub fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.into())
}

pub fn n(v: impl ToString) -> AttributeValue {
    AttributeValue::N(v.to_string())
}

pub fn item(pairs: Vec<(&str, AttributeValue)>) -> HashMap<String, AttributeValue> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}
