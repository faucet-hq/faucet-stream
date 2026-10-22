//! A conditional write is retried by the sink's own loop only: the SDK's
//! built-in retries are off for it, so an unknown outcome is visible to the
//! sink (which reads the item back when the retried condition then fails).

mod common;

use faucet_core::Sink;
use faucet_sink_dynamodb::DynamoDbSink;
use serde_json::json;
use wiremock::matchers::{header, method};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn internal_error() -> ResponseTemplate {
    ResponseTemplate::new(500)
        .insert_header("content-type", "application/x-amz-json-1.0")
        .set_body_json(json!({
            "__type": "com.amazonaws.dynamodb.v20120810#InternalServerError",
            "message": "try again"
        }))
}

async fn put_attempts(
    condition: Option<&str>,
    target: &str,
) -> (usize, Vec<faucet_core::RowOutcome>) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(header("x-amz-target", "DynamoDB_20120810.DescribeTable"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/x-amz-json-1.0")
                .set_body_json(json!({"Table": {
                    "TableName": "orders",
                    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
                }})),
        )
        .mount(&server)
        .await;
    for op in ["PutItem", "BatchWriteItem"] {
        Mock::given(method("POST"))
            .and(header(
                "x-amz-target",
                format!("DynamoDB_20120810.{op}").as_str(),
            ))
            .respond_with(internal_error())
            .mount(&server)
            .await;
    }
    let mut cfg = common::config(&server.uri(), "orders");
    cfg.retry.max_retries = 2;
    cfg.retry.initial_backoff_ms = 1;
    cfg.condition_expression = condition.map(str::to_string);
    if condition.is_some() {
        cfg.expression_attribute_names
            .insert("#p".into(), "pk".into());
    }
    let sink = DynamoDbSink::new(cfg).await.unwrap();
    let out = sink
        .write_batch_partial(&[json!({"pk": "a"})])
        .await
        .unwrap_or_else(|e| vec![Err(e)]);
    let puts = server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|r| {
            r.headers
                .get("x-amz-target")
                .is_some_and(|v| v == format!("DynamoDB_20120810.{target}").as_str())
        })
        .count();
    (puts, out)
}

#[tokio::test]
async fn a_conditional_put_is_retried_by_the_sink_only() {
    let (puts, out) = put_attempts(Some("attribute_not_exists(#p)"), "PutItem").await;
    // One first attempt plus `max_retries` sink retries; no hidden SDK retries.
    assert_eq!(puts, 3);
    assert!(out[0].is_err());
}

#[tokio::test]
async fn an_unconditional_write_is_retried() {
    let (puts, out) = put_attempts(None, "BatchWriteItem").await;
    assert!(puts > 1, "{puts}");
    assert!(out[0].is_err());
}
