//! A stalled server fails the request instead of hanging the run (#789 API-05).

use std::time::Duration;

use faucet_source_graphql::{GraphqlStream, GraphqlStreamConfig};
use serde_json::json;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn a_stalled_server_times_out_the_request() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_secs(30))
                .set_body_json(json!({"data": {"items": []}})),
        )
        .mount(&server)
        .await;
    let stream = GraphqlStream::new(
        GraphqlStreamConfig::new(
            format!("{}/graphql", server.uri()),
            "query { items { id } }",
        )
        .records_path("$.data.items[*]")
        .timeout(Some(Duration::from_millis(100))),
    );

    let outcome = tokio::time::timeout(Duration::from_secs(25), stream.fetch_all())
        .await
        .expect("the request timeout must end the call, not the test guard");
    assert!(outcome.is_err(), "a timed-out request is an error");
}

#[test]
fn timeouts_default_to_thirty_and_ten_seconds() {
    let mut v = serde_json::to_value(GraphqlStreamConfig::new(
        "https://api/graphql",
        "query { items { id } }",
    ))
    .unwrap();
    let obj = v.as_object_mut().unwrap();
    obj.remove("timeout");
    obj.remove("connect_timeout");
    let cfg: GraphqlStreamConfig = serde_json::from_value(v.clone()).unwrap();
    assert_eq!(cfg.timeout, Some(Duration::from_secs(30)));
    assert_eq!(cfg.connect_timeout, Some(Duration::from_secs(10)));

    let obj = v.as_object_mut().unwrap();
    obj.insert("timeout".into(), serde_json::Value::Null);
    obj.insert("connect_timeout".into(), json!(3));
    let cfg: GraphqlStreamConfig = serde_json::from_value(v).unwrap();
    assert_eq!(cfg.timeout, None);
    assert_eq!(cfg.connect_timeout, Some(Duration::from_secs(3)));
}

#[test]
fn the_connect_timeout_builder_sets_the_field() {
    let cfg = GraphqlStreamConfig::new("https://api/graphql", "query { a }")
        .connect_timeout(Some(Duration::from_secs(4)));
    assert_eq!(cfg.connect_timeout, Some(Duration::from_secs(4)));
}

#[tokio::test]
async fn an_invalid_custom_header_value_is_an_auth_error() {
    let stream = GraphqlStream::new(
        GraphqlStreamConfig::new("http://127.0.0.1:1/graphql", "query { a }").auth(
            faucet_source_graphql::GraphqlAuth::Custom {
                headers: [("x-key".to_string(), "bad\nvalue".to_string())].into(),
            },
        ),
    );
    let err = stream.fetch_all().await.unwrap_err();
    assert!(
        err.to_string().contains("invalid custom header value"),
        "{err}"
    );
}
