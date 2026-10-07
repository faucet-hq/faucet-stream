//! An XML response decoded through the HTTP path keeps every attribute.

use faucet_source_rest::{DecodeStep, ParseFormat, ParseSpec, RestStream, RestStreamConfig};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn xml_attributes_survive_the_decode_step() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/rows"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"<rows><row id="1" kind="a"/><row id="2" kind="b"/></rows>"#),
        )
        .mount(&server)
        .await;
    let cfg = RestStreamConfig::new(&server.uri(), "/rows").decode(vec![DecodeStep::Parse {
        parse: ParseSpec {
            format: ParseFormat::Xml,
            records_path: None,
            delimiter: None,
            has_headers: true,
            sheet: None,
            header_row: 0,
        },
    }]);
    let records = RestStream::new(cfg).unwrap().fetch_all().await.unwrap();
    let text = serde_json::to_string(&records).unwrap();
    for needle in ["\"@id\":\"1\"", "\"@kind\":\"b\""] {
        assert!(text.contains(needle), "{needle} in {text}");
    }
}
