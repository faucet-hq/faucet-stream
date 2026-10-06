//! A stalled server fails the request instead of hanging the run (#789 API-05).

use std::time::Duration;

use faucet_source_xml::{XmlStream, XmlStreamConfig};
use serde_json::json;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn a_stalled_server_times_out_the_request() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_secs(30))
                .set_body_string("<root><item><id>1</id></item></root>"),
        )
        .mount(&server)
        .await;
    let stream = XmlStream::new(
        XmlStreamConfig::new(server.uri(), "/feed.xml")
            .records_element_path("root.item")
            .timeout(Some(Duration::from_millis(100))),
    );

    let outcome = tokio::time::timeout(Duration::from_secs(25), stream.fetch_all())
        .await
        .expect("the request timeout must end the call, not the test guard");
    assert!(outcome.is_err(), "a timed-out request is an error");
}

#[test]
fn timeouts_default_to_thirty_and_ten_seconds() {
    let mut v = serde_json::to_value(XmlStreamConfig::new("https://api", "/feed.xml")).unwrap();
    let obj = v.as_object_mut().unwrap();
    obj.remove("timeout");
    obj.remove("connect_timeout");
    let cfg: XmlStreamConfig = serde_json::from_value(v.clone()).unwrap();
    assert_eq!(cfg.timeout, Some(Duration::from_secs(30)));
    assert_eq!(cfg.connect_timeout, Some(Duration::from_secs(10)));

    let obj = v.as_object_mut().unwrap();
    obj.insert("timeout".into(), serde_json::Value::Null);
    obj.insert("connect_timeout".into(), json!(3));
    let cfg: XmlStreamConfig = serde_json::from_value(v).unwrap();
    assert_eq!(cfg.timeout, None);
    assert_eq!(cfg.connect_timeout, Some(Duration::from_secs(3)));
}
