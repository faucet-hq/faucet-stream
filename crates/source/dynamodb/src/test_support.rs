//! Mock-server clients for unit tests (SDK retries disabled so each mocked
//! response reaches the connector's own retry logic).

use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region};
use serde_json::Value;
use wiremock::matchers::{header, method};
use wiremock::{Mock, MockServer, ResponseTemplate};

pub(crate) fn dynamo(uri: &str) -> aws_sdk_dynamodb::Client {
    let conf = aws_sdk_dynamodb::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new("a", "b", None, None, "test"))
        .endpoint_url(uri)
        .retry_config(aws_sdk_dynamodb::config::retry::RetryConfig::disabled())
        .build();
    aws_sdk_dynamodb::Client::from_conf(conf)
}

pub(crate) fn streams(uri: &str) -> aws_sdk_dynamodbstreams::Client {
    let conf = aws_sdk_dynamodbstreams::Config::builder()
        .behavior_version(aws_sdk_dynamodbstreams::config::BehaviorVersion::latest())
        .region(aws_sdk_dynamodbstreams::config::Region::new("us-east-1"))
        .credentials_provider(aws_sdk_dynamodbstreams::config::Credentials::new(
            "a", "b", None, None, "test",
        ))
        .endpoint_url(uri)
        .retry_config(aws_sdk_dynamodbstreams::config::retry::RetryConfig::disabled())
        .build();
    aws_sdk_dynamodbstreams::Client::from_conf(conf)
}

pub(crate) fn ok(body: Value) -> ResponseTemplate {
    ResponseTemplate::new(200)
        .insert_header("content-type", "application/x-amz-json-1.0")
        .set_body_string(body.to_string())
}

pub(crate) fn err(status: u16, code: &str) -> ResponseTemplate {
    ResponseTemplate::new(status)
        .insert_header("content-type", "application/x-amz-json-1.0")
        .set_body_string(
            serde_json::json!({"__type": format!("com.amazonaws.dynamodb.v20120810#{code}"), "message": code})
                .to_string(),
        )
}

/// Mount a response for one target, served `times` times (in mount order).
pub(crate) async fn on(server: &MockServer, target: &str, resp: ResponseTemplate, times: u64) {
    Mock::given(method("POST"))
        .and(header("x-amz-target", target))
        .respond_with(resp)
        .up_to_n_times(times)
        .mount(server)
        .await;
}
